//! Q2 Pod's Spotify browser on `--browse-socket`: the Q2SPOT/1 protocol, one request and one
//! response per connection, described in q2-pod's docs/librespot.md. It shares the main loop's
//! session and Spirc, and keeps nothing between requests: cursors carry the paging state.

use librespot::{
    connect::{LoadRequest, LoadRequestOptions, PlayingTrack, QueueState, Spirc},
    core::{Error, Session, SpotifyUri, error::ErrorKind, http_client::HttpClientError},
    metadata::{Album, Artist, Metadata, Track, artist::Artists},
    protocol::{
        collection2v2::PageResponse,
        extended_metadata::{
            BatchedEntityRequest, BatchedExtensionResponse, EntityRequest, ExtensionQuery,
        },
        extension_kind::ExtensionKind,
        playlist4_external::SelectedListContent,
    },
};
use log::{error, warn};
use protobuf::Message;
use std::{
    collections::HashMap,
    ops::Range,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::{mpsc, oneshot, watch},
};

/// This backend's version, answered by `caps`.
pub const VERSION: &str = "q2-v1.3";
const MAGIC: &str = "Q2SPOT/1";
const MAX_REQUEST: usize = 8 * 1024;
const MAX_RESPONSE: usize = 64 * 1024;
const MAX_CURSOR: usize = 1024;
const MAX_TEXT: usize = 255;
const MAX_MESSAGE: usize = 120;
const PAGE: usize = 50;
/// Answer inside the client's 15 s.
const DEADLINE: Duration = Duration::from_secs(14);
/// Liked Songs and saved albums stop paging past other items here and leave a cursor.
const PAGING_BUDGET: Duration = Duration::from_secs(10);
/// Throttled without a Retry-After: hold off this long.
const THROTTLE_DEFAULT: Duration = Duration::from_secs(30);
const THROTTLE_MAX: Duration = Duration::from_secs(3600);

/// Ops with their minimum and maximum argument counts.
const OPS: [(&str, usize, usize); 18] = [
    ("caps", 0, 0),
    ("account", 0, 0),
    ("playlists", 0, 1),
    ("playlist", 1, 2),
    ("liked", 0, 1),
    ("albums", 0, 1),
    ("artists", 0, 1),
    ("album", 1, 2),
    ("artist", 1, 1),
    ("search", 2, 3),
    ("play", 1, 3),
    ("queue", 0, 0),
    ("skip", 2, 2),
    ("enqueue", 1, 1),
    ("settings", 0, 0),
    ("shuffle", 1, 1),
    ("repeat", 1, 1),
    ("signout", 0, 0),
];
/// Ops a newer request never aborts.
const MUTATING: [&str; 6] = ["play", "skip", "enqueue", "shuffle", "repeat", "signout"];

/// Throttle deadlines: spclient, and Pathfinder (search) on its own host.
static THROTTLED: Mutex<[Option<Instant>; 2]> = Mutex::new([None; 2]);
const SPCLIENT: usize = 0;
const PATHFINDER: usize = 1;

/// Forgets throttling, as for another account.
pub fn forget_throttling() {
    *THROTTLED.lock().unwrap() = [None; 2];
}

/// What the browser can reach, kept current by the main loop.
#[derive(Clone)]
pub enum Link {
    /// No saved login: pair from the phone.
    Login,
    Connecting,
    Ready(Session, Spirc),
}

/// Asks the main loop to sign out: it answers on the first channel once the login is gone,
/// then exits when the second is dropped, after the reply went out.
pub type Signout = mpsc::UnboundedSender<(oneshot::Sender<()>, oneshot::Receiver<()>)>;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Status {
    Ok,
    Login,
    Offline,
    Premium,
    Refused,
    Unavailable,
    Stale,
    Throttled,
    Unsupported,
    Bad,
    Error,
}

impl Status {
    /// The wire name and the default message.
    fn text(self) -> (&'static str, &'static str) {
        match self {
            Status::Ok => ("ok", ""),
            Status::Login => ("login", "Open Spotify on your phone and choose Q2"),
            Status::Offline => ("offline", "Spotify can't be reached"),
            Status::Premium => ("premium", "Spotify Premium is required"),
            Status::Refused => ("refused", "Spotify refused the login"),
            Status::Unavailable => ("unavailable", "Not available"),
            Status::Stale => ("stale", "Search needs an add-on update"),
            Status::Throttled => ("throttled", "Spotify is busy, try again later"),
            Status::Unsupported => ("unsupported", "Not supported by this add-on"),
            Status::Bad => ("bad", "Bad request"),
            Status::Error => ("error", "Spotify request failed"),
        }
    }
}

#[derive(Debug)]
struct Fail(Status, String);

fn fail(status: Status) -> Fail {
    Fail(status, status.text().1.into())
}

/// The status for a failed Spotify request, and how long a 429 asked us to wait.
fn classify(e: &Error) -> (Status, Option<Duration>) {
    match e.error.downcast_ref::<HttpClientError>() {
        Some(HttpClientError::RetryAfter(wait)) => (Status::Throttled, Some(*wait)),
        Some(HttpClientError::StatusCode(code)) => (
            match code.as_u16() {
                401 | 403 => Status::Refused,
                404 | 410 | 451 => Status::Unavailable,
                412 => Status::Stale,
                429 => Status::Throttled,
                _ => Status::Error,
            },
            None,
        ),
        None => (
            match e.kind {
                ErrorKind::Unauthenticated | ErrorKind::PermissionDenied => Status::Refused,
                ErrorKind::NotFound => Status::Unavailable,
                // a failed connect, DNS lookup or TLS handshake, or a timeout
                ErrorKind::Unavailable | ErrorKind::DeadlineExceeded => Status::Offline,
                ErrorKind::ResourceExhausted => Status::Throttled,
                _ => Status::Error,
            },
            None,
        ),
    }
}

/// The answer for a failed request to the `host` throttle bucket; a 429 holds that host off.
fn failed(e: Error, host: usize) -> Fail {
    // Error's text holds no tokens: a kind and a status code or I/O cause
    warn!("Browse request failed: {e}");
    let (status, wait) = classify(&e);
    if status != Status::Throttled {
        return fail(status);
    }
    let wait = wait.unwrap_or(THROTTLE_DEFAULT).min(THROTTLE_MAX);
    THROTTLED.lock().unwrap()[host] = Some(Instant::now() + wait);
    throttled(wait)
}

impl From<Error> for Fail {
    fn from(e: Error) -> Self {
        failed(e, SPCLIENT)
    }
}

fn throttled(wait: Duration) -> Fail {
    let message = format!("Spotify is busy, try again in {} s", wait.as_secs().max(1));
    Fail(Status::Throttled, message)
}

#[derive(Clone, Debug, Default, PartialEq)]
struct Row {
    kind: &'static str,
    uri: String,
    title: String,
    subtitle: String,
    context: String,
    position: Option<usize>,
    /// The cursor that continues after this row, for a page cut short.
    after: String,
}

#[derive(Debug, Default)]
struct Page {
    rows: Vec<Row>,
    cursor: String,
    message: String,
}

// Framing

fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '%' => out.push_str("%25"),
            '\t' => out.push_str("%09"),
            '\n' => out.push_str("%0A"),
            '\r' => out.push_str("%0D"),
            c => out.push(c),
        }
    }
    out
}

fn unescape(field: &[u8]) -> Option<String> {
    let hex = |b: Option<&u8>| (*b? as char).to_digit(16);
    let mut out = Vec::with_capacity(field.len());
    let mut bytes = field.iter();
    while let Some(&b) = bytes.next() {
        if b == b'%' {
            out.push((hex(bytes.next())? * 16 + hex(bytes.next())?) as u8);
        } else {
            out.push(b);
        }
    }
    String::from_utf8(out).ok()
}

/// `s` cut to at most `max` bytes on a character boundary.
fn cut(s: &str, max: usize) -> &str {
    let mut end = s.len().min(max);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[derive(Debug, PartialEq)]
struct Request {
    id: String,
    op: String,
    args: Vec<String>,
}

/// The request, or the id to answer `bad` with.
fn parse(buf: &[u8]) -> Result<Request, String> {
    let bad = || "0".to_string();
    if buf.len() > MAX_REQUEST {
        return Err(bad());
    }
    let rest = buf
        .strip_prefix(MAGIC.as_bytes())
        .and_then(|r| r.strip_prefix(b"\n"))
        .ok_or_else(bad)?;
    let line = rest.strip_suffix(b"\n").unwrap_or(rest);
    let mut raw = line.split(|&b| b == b'\t');
    let id = raw.next().unwrap_or_default();
    if id.is_empty() || id.len() > 20 || !id.iter().all(u8::is_ascii_digit) {
        return Err(bad());
    }
    let id = String::from_utf8_lossy(id).into_owned();
    if line.contains(&b'\n') {
        return Err(id);
    }
    let mut fields = raw
        .map(unescape)
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| id.clone())?;
    if fields.is_empty() || fields[0].is_empty() {
        return Err(id);
    }
    let op = fields.remove(0);
    Ok(Request {
        id,
        op,
        args: fields,
    })
}

/// The response, with as many rows as fit in 64 KiB; a page cut short continues after its last row.
fn encode(id: &str, result: Result<Page, Fail>) -> Vec<u8> {
    let (status, page) = match result {
        Ok(page) => (Status::Ok, page),
        Err(Fail(status, message)) => (
            status,
            Page {
                message,
                ..Default::default()
            },
        ),
    };
    let mut rows = String::new();
    let mut cursor = page.cursor.as_str();
    // room for the header with the longest cursor
    let budget = MAX_RESPONSE - (MAGIC.len() + id.len() + 3 * (MAX_MESSAGE + MAX_CURSOR) + 32);
    for (i, row) in page.rows.iter().enumerate() {
        let position = row.position.map(|p| p.to_string()).unwrap_or_default();
        let fields = [
            row.kind,
            &row.uri,
            cut(&row.title, MAX_TEXT),
            cut(&row.subtitle, MAX_TEXT),
            &row.context,
            &position,
        ];
        let line = fields.map(escape).join("\t") + "\n";
        if rows.len() + line.len() > budget {
            match page.rows[..i].last() {
                Some(row) if !row.after.is_empty() => cursor = &row.after,
                _ => return encode(id, Err(Fail(Status::Error, "Page too large".into()))),
            }
            break;
        }
        rows.push_str(&line);
    }
    if cursor.len() > MAX_CURSOR {
        return encode(id, Err(Fail(Status::Error, "Cursor too long".into())));
    }
    format!(
        "{MAGIC}\n{id}\t{}\t{}\t{}\n{rows}",
        status.text().0,
        escape(cut(&page.message, MAX_MESSAGE)),
        escape(cursor)
    )
    .into_bytes()
}

// Serving

/// Binds `path` and answers each connection in its own task. A new connection aborts the
/// previous read-only request: the client has given up on it.
pub fn serve(path: &Path, link: watch::Receiver<Link>, signout: Signout) {
    let _ = std::fs::remove_file(path);
    let listener = match UnixListener::bind(path) {
        Ok(listener) => listener,
        Err(e) => return error!("Could not bind {}: {e}", path.display()),
    };
    tokio::spawn(async move {
        let mut last: Option<(tokio::task::AbortHandle, Arc<AtomicBool>)> = None;
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    if let Some((task, abortable)) = last.take() {
                        if abortable.load(Ordering::Relaxed) {
                            task.abort();
                        }
                    }
                    let abortable = Arc::new(AtomicBool::new(false));
                    let task = tokio::spawn(answer(
                        stream,
                        link.clone(),
                        signout.clone(),
                        abortable.clone(),
                    ));
                    last = Some((task.abort_handle(), abortable));
                }
                Err(e) => {
                    warn!("Browse socket: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });
}

async fn answer(
    mut stream: UnixStream,
    mut link: watch::Receiver<Link>,
    signout: Signout,
    abortable: Arc<AtomicBool>,
) {
    let deadline = tokio::time::Instant::now() + DEADLINE;
    let mut buf = Vec::new();
    // the client shuts down its write side after the request; one byte past the limit is `bad`
    let mut limited = (&mut stream).take(MAX_REQUEST as u64 + 1);
    let read = limited.read_to_end(&mut buf);
    if !matches!(tokio::time::timeout_at(deadline, read).await, Ok(Ok(_))) {
        return;
    }
    // held until the reply is out: dropping it lets a sign out exit
    let mut flushed = None;
    let (id, result) = match parse(&buf) {
        Err(id) => (id, Err(fail(Status::Bad))),
        Ok(request) => {
            abortable.store(!MUTATING.contains(&request.op.as_str()), Ordering::Relaxed);
            let run = run(&request, &mut link, &signout, &mut flushed);
            let result = tokio::time::timeout_at(deadline, run)
                .await
                .unwrap_or_else(|_| {
                    Err(Fail(
                        Status::Offline,
                        "Spotify didn't answer in time".into(),
                    ))
                });
            (request.id, result)
        }
    };
    let _ = stream.write_all(&encode(&id, result)).await;
    let _ = stream.shutdown().await;
    drop(flushed);
}

async fn run(
    request: &Request,
    link: &mut watch::Receiver<Link>,
    signout: &Signout,
    flushed: &mut Option<oneshot::Sender<()>>,
) -> Result<Page, Fail> {
    let Request { op, args, .. } = request;
    let Some(&(_, min, max)) = OPS.iter().find(|(name, ..)| name == op) else {
        return Err(fail(Status::Unsupported));
    };
    if args.len() < min || args.len() > max {
        return Err(fail(Status::Bad));
    }
    let arg = |i: usize| args.get(i).map(String::as_str).unwrap_or_default();
    // the optional cursor or position follows the required arguments
    let last = arg(min);
    match op.as_str() {
        "caps" => return Ok(caps()),
        "signout" => {
            let (done, signed_out) = oneshot::channel();
            let (flushed_tx, flushed_rx) = oneshot::channel();
            *flushed = Some(flushed_tx);
            signout
                .send((done, flushed_rx))
                .map_err(|_| fail(Status::Error))?;
            signed_out.await.map_err(|_| fail(Status::Error))?;
            return Ok(Page::default());
        }
        _ => {}
    }
    let (session, spirc) = ready(link).await?;
    // only ops that ask Spotify's web APIs wait out a throttle; Connect commands never do
    let host = match op.as_str() {
        "search" => Some(PATHFINDER),
        "account" | "playlists" | "playlist" | "liked" | "albums" | "artists" | "album"
        | "artist" => Some(SPCLIENT),
        _ => None,
    };
    if let Some(until) = host.and_then(|host| THROTTLED.lock().unwrap()[host]) {
        let now = Instant::now();
        if until > now {
            return Err(throttled(until - now));
        }
    }
    match op.as_str() {
        "account" => account(&session).await,
        "playlists" => playlists(&session, offset(last)?).await,
        "playlist" => playlist(&session, arg(0), offset(last)?).await,
        "liked" => collection(&session, Walk::new(last, "spotify:track:")?).await,
        "albums" => collection(&session, Walk::new(last, "spotify:album:")?).await,
        "artists" => collection(&session, Walk::new(last, "spotify:artist:")?).await,
        "album" => album(&session, arg(0), offset(last)?).await,
        "artist" => artist(&session, arg(0)).await,
        "search" => search(&session, arg(0), arg(1), offset(last)?).await,
        "play" => play(&spirc, arg(0), last, arg(2)).await,
        "queue" => queue(&session, &spirc).await,
        "skip" => skip(&spirc, arg(0), arg(1)).await,
        "enqueue" => enqueue(&spirc, arg(0)).await,
        "settings" => settings(&spirc).await,
        "shuffle" => shuffle(&spirc, arg(0)).await,
        "repeat" => repeat(&spirc, arg(0)).await,
        _ => Err(fail(Status::Unsupported)),
    }
}

/// The current session and Spirc, waiting while Connect logs in.
async fn ready(link: &mut watch::Receiver<Link>) -> Result<(Session, Spirc), Fail> {
    let state = link
        .wait_for(|l| !matches!(l, Link::Connecting))
        .await
        .map_err(|_| fail(Status::Error))?
        .clone();
    match state {
        Link::Ready(session, spirc) => Ok((session, spirc)),
        _ => Err(fail(Status::Login)),
    }
}

fn offset(cursor: &str) -> Result<usize, Fail> {
    match cursor {
        "" => Ok(0),
        _ => cursor.parse().map_err(|_| fail(Status::Bad)),
    }
}

/// The base62 id of a `spotify:<kind>:<id>` URI.
fn uri_of<'a>(uri: &'a str, kind: &str) -> Result<&'a str, Fail> {
    uri.strip_prefix("spotify:")
        .and_then(|r| r.strip_prefix(kind)?.strip_prefix(':'))
        .filter(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_alphanumeric()))
        .ok_or_else(|| fail(Status::Bad))
}

fn names(artists: &Artists) -> String {
    artists
        .iter()
        .map(|a| a.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The items in one batched extended-metadata response, by URI. An item that doesn't parse
/// is left out and logged.
fn parse_items<T: Metadata>(response: BatchedExtensionResponse) -> HashMap<String, T> {
    let mut items = HashMap::new();
    for data in response
        .extended_metadata
        .into_iter()
        .flat_map(|a| a.extension_data)
    {
        let item = SpotifyUri::from_uri(&data.entity_uri).and_then(|id| {
            T::parse(
                &T::Message::parse_from_bytes(&data.extension_data.value)?,
                &id,
            )
        });
        match item {
            Ok(item) => drop(items.insert(data.entity_uri, item)),
            Err(e) => warn!("Browse: leaving out {}: {e}", data.entity_uri),
        }
    }
    items
}

/// Titles and subtitles for the rows from one metadata request; rows whose item is gone drop out.
async fn fill<T: Metadata>(
    session: &Session,
    kind: ExtensionKind,
    rows: Vec<Row>,
    show: impl Fn(&T) -> (String, String),
) -> Result<Vec<Row>, Fail> {
    if rows.is_empty() {
        return Ok(rows);
    }
    let mut uris: Vec<_> = rows.iter().map(|row| row.uri.clone()).collect();
    uris.sort();
    uris.dedup();
    let request = BatchedEntityRequest {
        entity_request: uris
            .into_iter()
            .map(|entity_uri| EntityRequest {
                entity_uri,
                query: vec![ExtensionQuery {
                    extension_kind: kind.into(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let response = session.spclient().get_extended_metadata(request).await?;
    let items = parse_items::<T>(response);
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let (title, subtitle) = show(items.get(&row.uri)?);
            Some(Row {
                title,
                subtitle,
                ..row
            })
        })
        .collect())
}

async fn tracks(session: &Session, rows: Vec<Row>) -> Result<Vec<Row>, Fail> {
    fill(session, ExtensionKind::TRACK_V4, rows, |t: &Track| {
        (t.name.clone(), names(&t.artists))
    })
    .await
}

async fn albums(session: &Session, rows: Vec<Row>) -> Result<Vec<Row>, Fail> {
    fill(session, ExtensionKind::ALBUM_V4, rows, |a: &Album| {
        (a.name.clone(), names(&a.artists))
    })
    .await
}

fn row(kind: &'static str, uri: String, context: &str, position: Option<usize>) -> Row {
    Row {
        kind,
        uri,
        context: context.into(),
        position,
        after: position.map(|p| (p + 1).to_string()).unwrap_or_default(),
        ..Default::default()
    }
}

// Ops

fn caps() -> Page {
    Page {
        rows: OPS
            .iter()
            .map(|(name, ..)| row("op", name.to_string(), "", None))
            .collect(),
        message: VERSION.into(),
        ..Default::default()
    }
}

async fn account(session: &Session) -> Result<Page, Fail> {
    // the account's attributes follow the login
    let mut product = session.get_user_attribute("type");
    for _ in 0..50 {
        if product.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        product = session.get_user_attribute("type");
    }
    match product.as_deref() {
        Some("premium") => {}
        Some(_) => return Err(fail(Status::Premium)),
        None => {
            return Err(Fail(
                Status::Error,
                "No account details from Spotify".into(),
            ));
        }
    }
    let username = session.username();
    let profile = session
        .spclient()
        .get_user_profile(&username, None, None)
        .await;
    let name = profile
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .and_then(|json| json["name"].as_str().map(str::to_owned))
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| username.clone());
    Ok(Page {
        rows: vec![Row {
            title: name,
            subtitle: "Premium".into(),
            ..row("account", format!("spotify:user:{username}"), "", None)
        }],
        ..Default::default()
    })
}

/// An offset cursor, or none at the end.
fn cursor_at(next: usize, more: bool) -> String {
    if more {
        next.to_string()
    } else {
        String::new()
    }
}

/// Where a playlist or rootlist response begins in the list.
fn base(list: &SelectedListContent) -> usize {
    list.contents.pos().max(0) as usize
}

/// The response's items from `from`, whether or not the server began there, and the cursor.
fn window(list: &SelectedListContent, from: usize) -> (Range<usize>, String) {
    let items = list.contents.items.len();
    let start = from.saturating_sub(base(list)).min(items);
    let end = start.saturating_add(PAGE).min(items);
    let next = base(list) + end;
    let more = end > start && next < list.length().max(0) as usize;
    (start..end, cursor_at(next, more))
}

fn playlist_rows(list: &SelectedListContent, from: usize) -> Page {
    let (taken, cursor) = window(list, from);
    let contents = &list.contents;
    let rows = taken
        .filter(|&i| contents.items[i].uri().starts_with("spotify:playlist:"))
        .map(|i| {
            let meta = contents.meta_items.get(i);
            Row {
                title: meta.map(|m| m.attributes.name().into()).unwrap_or_default(),
                subtitle: meta.map(|m| m.owner_username().into()).unwrap_or_default(),
                after: (base(list) + i + 1).to_string(),
                ..row("playlist", contents.items[i].uri().into(), "", None)
            }
        })
        .collect();
    Page {
        rows,
        cursor,
        ..Default::default()
    }
}

async fn playlists(session: &Session, from: usize) -> Result<Page, Fail> {
    let bytes = session.spclient().get_rootlist(from, Some(PAGE)).await?;
    let list = SelectedListContent::parse_from_bytes(&bytes).map_err(|_| fail(Status::Error))?;
    Ok(playlist_rows(&list, from))
}

/// A playlist's track rows from `from`, positioned by their index in the playlist.
fn playlist_tracks(list: &SelectedListContent, from: usize, context: &str) -> Page {
    let (taken, cursor) = window(list, from);
    let rows = taken
        .map(|i| (list.contents.items[i].uri(), base(list) + i))
        .filter(|(uri, _)| uri.starts_with("spotify:track:"))
        .map(|(uri, index)| row("track", uri.into(), context, Some(index)))
        .collect();
    Page {
        rows,
        cursor,
        ..Default::default()
    }
}

async fn playlist(session: &Session, uri: &str, from: usize) -> Result<Page, Fail> {
    let id = uri_of(uri, "playlist")?;
    let endpoint = format!("/playlist/v2/playlist/{id}?from={from}&length={PAGE}");
    let bytes = session
        .spclient()
        .request(&http::Method::GET, &endpoint, None, None)
        .await?;
    let list = SelectedListContent::parse_from_bytes(&bytes).map_err(|_| fail(Status::Error))?;
    let page = playlist_tracks(&list, from, uri);
    Ok(Page {
        rows: tracks(session, page.rows).await?,
        ..page
    })
}

/// A walk through a collection v2 set. Its cursor is `position.skip.token`: the next row's
/// position, the matching items of the token's page already sent, and that page's token.
#[derive(Debug, Default)]
struct Walk {
    prefix: &'static str,
    position: usize,
    skip: usize,
    token: String,
    /// Rows with their position and `after` set.
    found: Vec<Row>,
}

impl Walk {
    fn new(cursor: &str, prefix: &'static str) -> Result<Walk, Fail> {
        let mut walk = Walk {
            prefix,
            ..Default::default()
        };
        if !cursor.is_empty() {
            let mut parts = cursor.splitn(3, '.');
            let mut number = || parts.next().and_then(|n| n.parse().ok());
            walk.position = number().ok_or_else(|| fail(Status::Bad))?;
            walk.skip = number().ok_or_else(|| fail(Status::Bad))?;
            walk.token = parts.next().ok_or_else(|| fail(Status::Bad))?.into();
        }
        Ok(walk)
    }

    fn cursor(&self, skip: usize) -> String {
        format!("{}.{skip}.{}", self.position, self.token)
    }

    /// Takes the matching items of the page fetched with `self.token`. Returns the final
    /// cursor once the walk ends, or `None` to fetch the next page.
    fn take(&mut self, page: &PageResponse) -> Option<String> {
        // the `collection` set lists saved albums first, then liked tracks
        let albums_done = self.prefix == "spotify:album:";
        let mut matched = 0;
        for item in page.items.iter().filter(|i| !i.is_removed) {
            if albums_done && item.uri.starts_with("spotify:track:") {
                return Some(String::new());
            }
            if !item.uri.starts_with(self.prefix) {
                continue;
            }
            matched += 1;
            if matched <= self.skip {
                continue;
            }
            if self.found.len() == PAGE {
                return Some(self.cursor(matched - 1));
            }
            let position = self.position;
            self.position = position.saturating_add(1);
            self.found.push(Row {
                uri: item.uri.clone(),
                position: Some(position),
                after: self.cursor(matched),
                ..Default::default()
            });
        }
        self.skip = 0;
        self.token = page.next_page_token.clone();
        if self.token.is_empty() {
            Some(String::new())
        } else if self.found.len() == PAGE {
            Some(self.cursor(0))
        } else {
            None
        }
    }
}

async fn collection(session: &Session, mut walk: Walk) -> Result<Page, Fail> {
    let set = if walk.prefix == "spotify:artist:" {
        "artist"
    } else {
        "collection"
    };
    let start = Instant::now();
    let cursor = loop {
        let page = session
            .spclient()
            .get_collection_page(set, &walk.token, PAGE as i32)
            .await?;
        if let Some(cursor) = walk.take(&page) {
            break cursor;
        }
        if start.elapsed() > PAGING_BUDGET {
            break walk.cursor(0);
        }
    };
    let rows = match walk.prefix {
        "spotify:track:" => {
            let context = format!("spotify:user:{}:collection", session.username());
            let found = walk.found.into_iter();
            let rows = found.map(|r| Row {
                kind: "track",
                context: context.clone(),
                ..r
            });
            tracks(session, rows.collect()).await?
        }
        "spotify:album:" => {
            let found = walk.found.into_iter();
            let rows = found.map(|r| Row {
                kind: "album",
                position: None,
                ..r
            });
            albums(session, rows.collect()).await?
        }
        _ => {
            let found = walk.found.into_iter();
            let rows = found.map(|r| Row {
                kind: "artist",
                position: None,
                ..r
            });
            fill(
                session,
                ExtensionKind::ARTIST_V4,
                rows.collect(),
                |a: &Artist| (a.name.clone(), String::new()),
            )
            .await?
        }
    };
    Ok(Page {
        rows,
        cursor,
        ..Default::default()
    })
}

async fn album(session: &Session, uri: &str, from: usize) -> Result<Page, Fail> {
    uri_of(uri, "album")?;
    let album = Album::get(session, &SpotifyUri::from_uri(uri)?).await?;
    let ids: Vec<_> = album.tracks().collect();
    let end = from.saturating_add(PAGE).min(ids.len());
    let mut rows = Vec::new();
    for (index, id) in ids.iter().enumerate().take(end).skip(from) {
        rows.push(row("track", id.to_uri()?, uri, Some(index)));
    }
    Ok(Page {
        rows: tracks(session, rows).await?,
        cursor: cursor_at(end, end < ids.len()),
        ..Default::default()
    })
}

async fn artist(session: &Session, uri: &str) -> Result<Page, Fail> {
    uri_of(uri, "artist")?;
    let artist = Artist::get(session, &SpotifyUri::from_uri(uri)?).await?;
    // ponytail: top tracks play on their own; an artist context is unverified on Connect
    let mut top = Vec::new();
    for track in artist.top_tracks.for_country(&session.country()).iter() {
        top.push(row("track", track.to_uri()?, "", None));
    }
    let mut releases = Vec::new();
    for album in artist.albums_current().chain(artist.singles_current()) {
        releases.push(row("album", album.to_uri()?, "", None));
    }
    releases.truncate(PAGE.saturating_sub(top.len()));
    let mut rows = tracks(session, top).await?;
    rows.extend(albums(session, releases).await?);
    Ok(Page {
        rows,
        ..Default::default()
    })
}

/// searchDesktop's hash, from spotatui v0.45.0; a retired one gives 412 (`stale`).
const SEARCH_HASH: &str = "eef7cc54888d91bdd6802623477873caa3948ae173a0c34fd86827b267e94c03";

async fn search(session: &Session, category: &str, term: &str, from: usize) -> Result<Page, Fail> {
    if !matches!(category, "tracks" | "albums" | "artists" | "playlists") || term.is_empty() {
        return Err(fail(Status::Bad));
    }
    let variables = serde_json::json!({
        "searchTerm": term, "offset": from, "limit": PAGE, "numberOfTopResults": 5,
        "includeAudiobooks": false, "includeArtistHasConcertsField": false,
        "includePreReleases": false, "includeLocalConcertsField": false,
        "includeAuthors": false,
    });
    let answer = session
        .spclient()
        .pathfinder_query("searchDesktop", SEARCH_HASH, variables)
        .await
        .map_err(|e| failed(e, PATHFINDER))?;
    search_rows(&answer, category, from)
}

fn search_rows(answer: &serde_json::Value, category: &str, from: usize) -> Result<Page, Fail> {
    let (key, kind) = match category {
        "tracks" => ("tracksV2", "track"),
        "albums" => ("albumsV2", "album"),
        "artists" => ("artists", "artist"),
        _ => ("playlists", "playlist"),
    };
    let section = &answer["data"]["searchV2"][key];
    let Some(items) = section["items"].as_array() else {
        return Err(fail(Status::Error));
    };
    let text = |v: &serde_json::Value| v.as_str().unwrap_or_default().to_owned();
    let artists = |data: &serde_json::Value| {
        let names: Vec<_> = data["artists"]["items"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|a| a["profile"]["name"].as_str())
            .collect();
        names.join(", ")
    };
    let rows = items
        .iter()
        .enumerate()
        .filter_map(|(i, item)| {
            let data = item.pointer("/item/data").unwrap_or(&item["data"]);
            let uri = data["uri"].as_str().filter(|u| !u.is_empty())?;
            let (title, subtitle) = match kind {
                "track" | "album" => (text(&data["name"]), artists(data)),
                "artist" => (text(&data["profile"]["name"]), String::new()),
                _ => (text(&data["name"]), text(&data["ownerV2"]["data"]["name"])),
            };
            Some(Row {
                title,
                subtitle,
                after: from.saturating_add(i + 1).to_string(),
                ..row(kind, uri.into(), "", None)
            })
        })
        .collect();
    let next = from.saturating_add(items.len());
    let total = section["totalCount"].as_u64().unwrap_or_default() as usize;
    let more = !items.is_empty() && next < total;
    Ok(Page {
        rows,
        cursor: cursor_at(next, more),
        ..Default::default()
    })
}

/// `play context [position [uri]]`: `uri` is the row's track, which Liked Songs start by.
async fn play(spirc: &Spirc, context: &str, position: &str, uri: &str) -> Result<Page, Fail> {
    if !context.starts_with("spotify:") || (!uri.is_empty() && uri_of(uri, "track").is_err()) {
        return Err(fail(Status::Bad));
    }
    let playing_track = match position {
        "" => None,
        // Connect may order the collection unlike collection v2, and it holds no duplicates
        _ if !uri.is_empty() && context.ends_with(":collection") => {
            Some(PlayingTrack::Uri(uri.into()))
        }
        _ => Some(PlayingTrack::Index(
            position.parse().map_err(|_| fail(Status::Bad))?,
        )),
    };
    let options = LoadRequestOptions {
        start_playing: true,
        playing_track,
        ..Default::default()
    };
    let request = if position.is_empty() && context.starts_with("spotify:track:") {
        LoadRequest::from_tracks(vec![context.into()], options)
    } else {
        LoadRequest::from_context_uri(context.into(), options)
    };
    spirc.activate()?;
    match spirc.load_checked(request).await? {
        true => Ok(Page::default()),
        false => Err(Fail(
            Status::Unavailable,
            "Spotify couldn't start that".into(),
        )),
    }
}

async fn queue(session: &Session, spirc: &Spirc) -> Result<Page, Fail> {
    let Some(state) = spirc.queue().await? else {
        return Ok(Page::default());
    };
    let next = state.next.into_iter().take(PAGE).enumerate();
    let rows = next.map(|(i, uri)| Row {
        after: String::new(),
        ..row("track", uri, "", Some(i))
    });
    Ok(Page {
        rows: tracks(session, rows.collect()).await?,
        ..Default::default()
    })
}

async fn skip(spirc: &Spirc, position: &str, uri: &str) -> Result<Page, Fail> {
    let position = position.parse().map_err(|_| fail(Status::Bad))?;
    match spirc.skip_to(position, uri.into()).await? {
        true => Ok(Page::default()),
        false => Err(Fail(Status::Unavailable, "The queue has changed".into())),
    }
}

fn not_playing() -> Fail {
    Fail(Status::Unavailable, "Nothing is playing on Q2".into())
}

/// The play options, or `unavailable` while Q2 isn't playing (Connect then ignores them).
async fn active(spirc: &Spirc) -> Result<QueueState, Fail> {
    spirc.queue().await?.ok_or_else(not_playing)
}

async fn enqueue(spirc: &Spirc, uri: &str) -> Result<Page, Fail> {
    uri_of(uri, "track")?;
    match spirc.add_to_queue(uri.into()).await? {
        true => Ok(Page::default()),
        false => Err(not_playing()),
    }
}

async fn settings(spirc: &Spirc) -> Result<Page, Fail> {
    let state = active(spirc).await?;
    let setting = |name: &str, value: &str| Row {
        title: value.into(),
        ..row("setting", name.into(), "", None)
    };
    let repeat = match (state.repeat_track, state.repeat_context) {
        (true, _) => "track",
        (_, true) => "context",
        _ => "off",
    };
    Ok(Page {
        rows: vec![
            setting("shuffle", if state.shuffle { "on" } else { "off" }),
            setting("repeat", repeat),
        ],
        ..Default::default()
    })
}

async fn shuffle(spirc: &Spirc, value: &str) -> Result<Page, Fail> {
    let on = match value {
        "on" => true,
        "off" => false,
        _ => return Err(fail(Status::Bad)),
    };
    active(spirc).await?;
    spirc.shuffle(on)?;
    Ok(Page::default())
}

async fn repeat(spirc: &Spirc, value: &str) -> Result<Page, Fail> {
    if !matches!(value, "off" | "context" | "track") {
        return Err(fail(Status::Bad));
    }
    active(spirc).await?;
    spirc.repeat_track(value == "track")?;
    if value != "track" {
        spirc.repeat(value == "context")?;
    }
    Ok(Page::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use librespot::protocol::{
        collection2v2::CollectionItem,
        entity_extension_data::EntityExtensionData,
        extended_metadata::EntityExtensionDataArray,
        metadata,
        playlist4_external::{Item, ListAttributes, ListItems, MetaItem},
    };

    fn request(line: &str) -> Vec<u8> {
        format!("Q2SPOT/1\n{line}\n").into_bytes()
    }

    fn lines(response: &[u8]) -> Vec<String> {
        String::from_utf8(response.to_vec())
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn track(uri: &str) -> Row {
        row("track", uri.into(), "", None)
    }

    #[test]
    fn escaping_round_trips() {
        let text = "50% tab\there\nline\rcr – ünïcødé 🎧";
        let escaped = escape(text);
        assert_eq!(escaped, "50%25 tab%09here%0Aline%0Dcr – ünïcødé 🎧");
        assert_eq!(unescape(escaped.as_bytes()).as_deref(), Some(text));
        assert_eq!(unescape(b"%41%e2%80%93").as_deref(), Some("A–"));
    }

    #[test]
    fn malformed_escapes_are_rejected() {
        for field in [&b"%"[..], b"%4", b"%zz", b"a%g1", b"%+f", b"%ff%fe"] {
            assert_eq!(unescape(field), None, "{field:?}");
        }
    }

    #[test]
    fn cut_keeps_whole_characters() {
        assert_eq!(cut("abc", 255), "abc");
        assert_eq!(cut("aé", 2), "a");
        assert_eq!(cut(&"ü".repeat(200), MAX_TEXT).len(), 254);
    }

    #[test]
    fn requests_parse() {
        assert_eq!(
            parse(&request("7\tsearch\ttracks\tAC%09DC\t50")),
            Ok(Request {
                id: "7".into(),
                op: "search".into(),
                args: vec!["tracks".into(), "AC\tDC".into(), "50".into()],
            })
        );
        // EOF without a newline is fine too
        assert_eq!(parse(b"Q2SPOT/1\n8\tliked").unwrap().op, "liked");
    }

    #[test]
    fn bad_requests_are_rejected() {
        assert_eq!(parse(b"7\tliked\n"), Err("0".into()));
        assert_eq!(parse(b"Q2SPOT/2\n7\tliked\n"), Err("0".into()));
        assert_eq!(parse(&request("x7\tliked")), Err("0".into()));
        assert_eq!(parse(&request("7")), Err("7".into()));
        assert_eq!(parse(&request("7\tplay\t%zz")), Err("7".into()));
        assert_eq!(parse(&request("7\tliked\nextra")), Err("7".into()));
        let big = format!("7\tsearch\ttracks\t{}", "a".repeat(MAX_REQUEST));
        assert_eq!(parse(&request(&big)), Err("0".into()));
    }

    #[tokio::test]
    async fn ops_are_checked() {
        let (_tx, mut link) = watch::channel(Link::Login);
        let (signout, _rx) = mpsc::unbounded_channel();
        let mut flushed = None;
        for (line, status) in [
            ("1\tdance", Status::Unsupported),
            ("2\tplaylist", Status::Bad),
            ("3\tskip\t1", Status::Bad),
            ("4\tliked", Status::Login),
            ("5\tplay\tspotify:x\t1\turi\textra", Status::Bad),
        ] {
            let request = parse(&request(line)).unwrap();
            let result = run(&request, &mut link, &signout, &mut flushed).await;
            assert_eq!(result.unwrap_err().0, status, "{line}");
        }
        let request = parse(&request("6\tcaps")).unwrap();
        let caps = run(&request, &mut link, &signout, &mut flushed)
            .await
            .unwrap();
        assert_eq!(caps.message, VERSION);
        assert_eq!(caps.rows.len(), OPS.len());
    }

    #[test]
    fn responses_encode() {
        let page = Page {
            rows: vec![Row {
                title: "Song\tTitle".into(),
                subtitle: "Band".into(),
                ..row(
                    "track",
                    "spotify:track:1".into(),
                    "spotify:user:alice:collection",
                    Some(0),
                )
            }],
            cursor: "1.1.tok".into(),
            ..Default::default()
        };
        assert_eq!(
            lines(&encode("7", Ok(page))),
            [
                "Q2SPOT/1",
                "7\tok\t\t1.1.tok",
                "track\tspotify:track:1\tSong%09Title\tBand\tspotify:user:alice:collection\t0"
            ]
        );
        let error = encode("8", Err(fail(Status::Offline)));
        assert_eq!(lines(&error)[1], "8\toffline\tSpotify can't be reached\t");
    }

    fn heavy_rows(after: bool) -> Vec<Row> {
        // escaping triples these titles: 50 rows would pass 64 KiB
        (0..PAGE)
            .map(|i| Row {
                title: "%".repeat(400),
                subtitle: "\t".repeat(400),
                after: if after {
                    (i + 1).to_string()
                } else {
                    String::new()
                },
                ..track("spotify:track:x")
            })
            .collect()
    }

    #[test]
    fn long_pages_are_cut_with_a_cursor() {
        let page = Page {
            rows: heavy_rows(true),
            cursor: "50".into(),
            ..Default::default()
        };
        let response = encode("1", Ok(page));
        assert!(response.len() <= MAX_RESPONSE);
        let lines = lines(&response);
        let sent = lines.len() - 2;
        assert!(sent < PAGE && sent > 0);
        assert_eq!(lines[1], format!("1\tok\t\t{sent}"));
        assert!(lines[2].contains(&"%25".repeat(MAX_TEXT)));
        assert!(!lines[2].contains(&"%25".repeat(MAX_TEXT + 1)));
    }

    #[test]
    fn pages_that_cannot_continue_are_errors() {
        let page = Page {
            rows: heavy_rows(false),
            ..Default::default()
        };
        assert_eq!(
            lines(&encode("1", Ok(page)))[1],
            "1\terror\tPage too large\t"
        );
        let page = Page {
            cursor: "x".repeat(MAX_CURSOR + 1),
            ..Default::default()
        };
        assert_eq!(
            lines(&encode("1", Ok(page)))[1],
            "1\terror\tCursor too long\t"
        );
    }

    fn collection_page(uris: &[&str], next: &str) -> PageResponse {
        let page = PageResponse {
            items: uris
                .iter()
                .map(|uri| CollectionItem {
                    uri: uri.to_string(),
                    ..Default::default()
                })
                .collect(),
            next_page_token: next.into(),
            ..Default::default()
        };
        // round trip through the wire format, as the server sends it
        PageResponse::parse_from_bytes(&page.write_to_bytes().unwrap()).unwrap()
    }

    fn walk_to_end(cursor: &str, prefix: &'static str, pages: &[PageResponse]) -> (Walk, String) {
        let mut walk = Walk::new(cursor, prefix).unwrap();
        let tokens = ["", "p2", "p3"];
        loop {
            let page = tokens.iter().position(|t| *t == walk.token).unwrap();
            if let Some(cursor) = walk.take(&pages[page]) {
                return (walk, cursor);
            }
        }
    }

    #[test]
    fn liked_skips_albums_and_counts_across_pages() {
        let mut first = vec!["spotify:album:a"; 30];
        first.extend(["spotify:track:t"; 20]);
        let pages = [
            collection_page(&first, "p2"),
            collection_page(&["spotify:track:t"; 50], "p3"),
            collection_page(&["spotify:track:t"; 10], ""),
        ];
        let (walk, cursor) = walk_to_end("", "spotify:track:", &pages);
        assert_eq!(walk.found.len(), PAGE);
        assert_eq!(walk.found[0].position, Some(0));
        assert_eq!(walk.found[49].position, Some(49));
        // the page filled 30 rows into p2: the next starts there
        assert_eq!(cursor, "50.30.p2");
        assert_eq!(walk.found[49].after, "50.30.p2");
        assert_eq!(walk.found[19].after, "20.20.");

        let (walk, cursor) = walk_to_end(&cursor, "spotify:track:", &pages);
        assert_eq!(walk.found.len(), 30);
        assert_eq!(walk.found[0].position, Some(50));
        assert_eq!(walk.found[29].position, Some(79));
        assert_eq!(cursor, "");
    }

    #[test]
    fn albums_stop_at_the_first_liked_track() {
        let mut first = vec!["spotify:album:a"; 3];
        first.push("spotify:track:t");
        let (walk, cursor) = walk_to_end("", "spotify:album:", &[collection_page(&first, "p2")]);
        assert_eq!(walk.found.len(), 3);
        assert_eq!(cursor, "");
    }

    #[test]
    fn cursors_round_trip() {
        let walk = Walk::new("12.3.tok.with.dots", "spotify:track:").unwrap();
        assert_eq!((walk.position, walk.skip), (12, 3));
        assert_eq!(walk.cursor(3), "12.3.tok.with.dots");
        assert!(Walk::new("12", "spotify:track:").is_err());
        assert!(Walk::new("a.b.c", "spotify:track:").is_err());
        assert_eq!(offset("").ok(), Some(0));
        assert_eq!(offset("51").ok(), Some(51));
        assert!(offset("-1").is_err());
        // huge client numbers saturate instead of overflowing
        let mut walk = Walk::new(&format!("{}.0.", usize::MAX), "spotify:track:").unwrap();
        assert_eq!(
            walk.take(&collection_page(&["spotify:track:t"], "")),
            Some(String::new())
        );
        let page = search_rows(&serde_json::json!({}), "tracks", usize::MAX);
        assert!(page.is_err());
        assert_eq!(
            window(&list(&["spotify:track:a"], 0, 1, false), usize::MAX).0,
            1..1
        );
    }

    fn list(uris: &[&str], pos: i32, length: i32, names: bool) -> SelectedListContent {
        let mut list = SelectedListContent::new();
        list.set_length(length);
        let mut contents = ListItems::new();
        contents.set_pos(pos);
        contents.set_truncated(false);
        for (i, uri) in uris.iter().enumerate() {
            let mut item = Item::new();
            item.set_uri(uri.to_string());
            contents.items.push(item);
            if names {
                let mut meta = MetaItem::new();
                let mut attributes = ListAttributes::new();
                attributes.set_name(format!("Mix {i}"));
                meta.attributes = Some(attributes).into();
                meta.set_owner_username("bob".into());
                contents.meta_items.push(meta);
            }
        }
        list.contents = Some(contents).into();
        SelectedListContent::parse_from_bytes(&list.write_to_bytes().unwrap()).unwrap()
    }

    #[test]
    fn playlist_entries_keep_their_positions() {
        let uris = [
            "spotify:track:a",
            "spotify:track:b",
            "spotify:track:a",
            "spotify:local:x:y:z:1",
            "spotify:track:c",
        ];
        // the server began at 50 as asked
        let page = playlist_tracks(&list(&uris, 50, 120, false), 50, "spotify:playlist:p");
        let positions: Vec<_> = page
            .rows
            .iter()
            .map(|r| (r.uri.as_str(), r.position))
            .collect();
        assert_eq!(
            positions,
            [
                ("spotify:track:a", Some(50)),
                ("spotify:track:b", Some(51)),
                ("spotify:track:a", Some(52)),
                ("spotify:track:c", Some(54)),
            ]
        );
        assert_eq!(page.rows[2].after, "53");
        assert_eq!(page.rows[2].context, "spotify:playlist:p");
        assert_eq!(page.cursor, "55");
        // the server sent the whole list from 0
        let page = playlist_tracks(&list(&uris, 0, 5, false), 2, "");
        assert_eq!(page.rows[0].position, Some(2));
        assert_eq!(page.cursor, "");
    }

    #[test]
    fn rootlist_rows_skip_folders() {
        let uris = [
            "spotify:start-group:f:Folder",
            "spotify:playlist:p1",
            "spotify:end-group:f",
            "spotify:playlist:p2",
        ];
        let page = playlist_rows(&list(&uris, 0, 4, true), 0);
        assert_eq!(page.rows.len(), 2);
        assert_eq!(page.rows[0].uri, "spotify:playlist:p1");
        assert_eq!(page.rows[0].title, "Mix 1");
        assert_eq!(page.rows[1].subtitle, "bob");
        assert_eq!(page.rows[1].after, "4");
        assert_eq!(page.cursor, "");
    }

    #[test]
    fn uris_are_checked() {
        assert_eq!(
            uri_of("spotify:playlist:37i9dQZF1DX", "playlist").ok(),
            Some("37i9dQZF1DX")
        );
        assert!(uri_of("spotify:playlist:", "playlist").is_err());
        assert!(uri_of("spotify:playlists:x", "playlist").is_err());
        assert!(uri_of("spotify:playlist:a/../b", "playlist").is_err());
        assert!(uri_of("spotify:album:x", "playlist").is_err());
    }

    #[test]
    fn batched_metadata_leaves_out_broken_items() {
        let gid = |b: u8| vec![b; 16];
        let mut artist = metadata::Artist::new();
        artist.set_gid(gid(3));
        artist.set_name("The Fireflies".into());
        let mut album = metadata::Album::new();
        album.set_gid(gid(2));
        let mut good = metadata::Track::new();
        good.set_gid(gid(1));
        good.set_name("Lantern Song".into());
        good.album = Some(album).into();
        good.artist.push(artist);
        let uri = SpotifyUri::try_from(&good).unwrap().to_uri().unwrap();
        let entry = |uri: &str, bytes: Vec<u8>| {
            let mut data = EntityExtensionData::new();
            data.entity_uri = uri.into();
            data.extension_data.mut_or_insert_default().value = bytes;
            data
        };
        let mut array = EntityExtensionDataArray::new();
        array
            .extension_data
            .push(entry(&uri, good.write_to_bytes().unwrap()));
        array
            .extension_data
            .push(entry("spotify:track:broken", vec![0xff, 0xff]));
        let mut response = BatchedExtensionResponse::new();
        response.extended_metadata.push(array);
        let items = parse_items::<Track>(response);
        assert_eq!(items.len(), 1);
        assert_eq!(names(&items[&uri].artists), "The Fireflies");
    }

    #[test]
    fn search_rows_from_pathfinder() {
        let answer = serde_json::json!({"data": {"searchV2": {
            "tracksV2": {"totalCount": 120, "items": [
                {"item": {"data": {"uri": "spotify:track:t1", "name": "Lantern Song",
                    "artists": {"items": [{"profile": {"name": "The Fireflies"}},
                                          {"profile": {"name": "Moth"}}]}}}},
                {"item": {"data": {"__typename": "NotFound"}}}
            ]},
            "albumsV2": {"totalCount": 1, "items": [
                {"data": {"uri": "spotify:album:a1", "name": "Night Garden",
                    "artists": {"items": [{"profile": {"name": "The Fireflies"}}]}}}
            ]},
            "artists": {"totalCount": 1, "items": [
                {"data": {"uri": "spotify:artist:r1", "profile": {"name": "The Fireflies"}}}
            ]},
            "playlists": {"totalCount": 1, "items": [
                {"data": {"uri": "spotify:playlist:p1", "name": "Glow",
                    "ownerV2": {"data": {"name": "Carol"}}}}
            ]}
        }}});
        let tracks = search_rows(&answer, "tracks", 50).unwrap();
        assert_eq!(tracks.rows.len(), 1);
        assert_eq!(tracks.rows[0].subtitle, "The Fireflies, Moth");
        assert_eq!(tracks.rows[0].context, "");
        assert_eq!(tracks.rows[0].position, None);
        assert_eq!(tracks.rows[0].after, "51");
        assert_eq!(tracks.cursor, "52");
        let albums = search_rows(&answer, "albums", 0).unwrap();
        assert_eq!((albums.rows[0].kind, albums.cursor.as_str()), ("album", ""));
        assert_eq!(
            search_rows(&answer, "artists", 0).unwrap().rows[0].title,
            "The Fireflies"
        );
        assert_eq!(
            search_rows(&answer, "playlists", 0).unwrap().rows[0].subtitle,
            "Carol"
        );
        let errors = serde_json::json!({"errors": [{"message": "x"}]});
        assert!(search_rows(&errors, "tracks", 0).is_err());
    }

    #[test]
    fn errors_are_classified() {
        use http::StatusCode;
        let status = |code: StatusCode| classify(&HttpClientError::StatusCode(code).into()).0;
        assert_eq!(status(StatusCode::UNAUTHORIZED), Status::Refused);
        assert_eq!(status(StatusCode::FORBIDDEN), Status::Refused);
        assert_eq!(status(StatusCode::NOT_FOUND), Status::Unavailable);
        assert_eq!(status(StatusCode::PRECONDITION_FAILED), Status::Stale);
        assert_eq!(status(StatusCode::TOO_MANY_REQUESTS), Status::Throttled);
        assert_eq!(status(StatusCode::INTERNAL_SERVER_ERROR), Status::Error);
        let wait = Duration::from_secs(90);
        assert_eq!(
            classify(&HttpClientError::RetryAfter(wait).into()),
            (Status::Throttled, Some(wait))
        );
        let refused = std::io::Error::from(std::io::ErrorKind::ConnectionRefused);
        assert_eq!(classify(&refused.into()).0, Status::Offline);
        assert_eq!(
            classify(&Error::deadline_exceeded("slow")).0,
            Status::Offline
        );
        assert_eq!(
            classify(&Error::unauthenticated("login5")).0,
            Status::Refused
        );
        // a JSON parse error is FailedPrecondition, but not a stale search
        let json = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        assert_eq!(classify(&json.into()).0, Status::Error);
    }

    #[test]
    fn throttling_holds_off_one_host_for_at_most_an_hour() {
        let fail = failed(
            HttpClientError::RetryAfter(Duration::from_secs(90)).into(),
            PATHFINDER,
        );
        assert_eq!(fail.0, Status::Throttled);
        assert!(fail.1.contains("90 s"));
        let huge = Duration::from_secs(u64::MAX / 4);
        failed(HttpClientError::RetryAfter(huge).into(), SPCLIENT);
        let [spclient, pathfinder] = *THROTTLED.lock().unwrap();
        assert!(spclient.unwrap() <= Instant::now() + THROTTLE_MAX);
        assert!(pathfinder.unwrap() > Instant::now() + Duration::from_secs(80));
        forget_throttling();
        assert_eq!(*THROTTLED.lock().unwrap(), [None, None]);
    }
}
