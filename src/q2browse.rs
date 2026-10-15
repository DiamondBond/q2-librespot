//! Q2 Pod's Spotify browser on `--browse-socket`: the Q2SPOT/1 protocol, one request and one
//! response per connection, described in q2-pod's docs/librespot.md. It shares the main loop's
//! session and Spirc, and keeps nothing between requests: cursors carry the paging state.

use futures_util::{StreamExt, stream};
use librespot::{
    connect::{LoadRequest, LoadRequestOptions, PlayingTrack, Spirc},
    core::{Error, Session, SpotifyId, SpotifyUri, error::ErrorKind, http_client::HttpClientError},
    metadata::{Album, Artist, Metadata, Track, artist::Artists},
    protocol::{collection2v2::PageResponse, playlist4_external::SelectedListContent},
};
use log::{error, warn};
use protobuf::Message;
use std::{
    path::Path,
    sync::Mutex,
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
/// Metadata lookups in flight per request.
const LOOKUPS: usize = 8;

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
    ("play", 1, 2),
    ("queue", 0, 0),
    ("skip", 2, 2),
    ("enqueue", 1, 1),
    ("settings", 0, 0),
    ("shuffle", 1, 1),
    ("repeat", 1, 1),
    ("signout", 0, 0),
];

static THROTTLED_UNTIL: Mutex<Option<Instant>> = Mutex::new(None);

/// What the browser can reach, kept current by the main loop.
#[derive(Clone)]
pub enum Link {
    /// No saved login: pair from the phone.
    Login,
    Connecting,
    Ready(Session, Spirc),
}

/// Asks the main loop to sign out; it answers once the login is gone.
pub type Signout = mpsc::UnboundedSender<oneshot::Sender<()>>;

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
    fn name(self) -> &'static str {
        match self {
            Status::Ok => "ok",
            Status::Login => "login",
            Status::Offline => "offline",
            Status::Premium => "premium",
            Status::Refused => "refused",
            Status::Unavailable => "unavailable",
            Status::Stale => "stale",
            Status::Throttled => "throttled",
            Status::Unsupported => "unsupported",
            Status::Bad => "bad",
            Status::Error => "error",
        }
    }

    fn message(self) -> &'static str {
        match self {
            Status::Ok => "",
            Status::Login => "Open Spotify on your phone and choose Q2",
            Status::Offline => "Spotify can't be reached",
            Status::Premium => "Spotify Premium is required",
            Status::Refused => "Spotify refused the login",
            Status::Unavailable => "Not available",
            Status::Stale => "Search needs an add-on update",
            Status::Throttled => "Spotify is busy, try again later",
            Status::Unsupported => "Not supported by this add-on",
            Status::Bad => "Bad request",
            Status::Error => "Spotify request failed",
        }
    }
}

#[derive(Debug)]
struct Fail(Status, String);

fn fail(status: Status) -> Fail {
    Fail(status, status.message().into())
}

fn fail_with(status: Status, message: &str) -> Fail {
    Fail(status, message.into())
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

impl From<Error> for Fail {
    fn from(e: Error) -> Self {
        // Error's text holds no tokens: a kind and a status code or I/O cause
        warn!("Browse request failed: {e}");
        let (status, wait) = classify(&e);
        if status == Status::Throttled {
            let wait = wait.unwrap_or(THROTTLE_DEFAULT);
            *THROTTLED_UNTIL.lock().unwrap() = Some(Instant::now() + wait);
            return throttled(wait);
        }
        fail(status)
    }
}

fn throttled(wait: Duration) -> Fail {
    let message = format!("Spotify is busy, try again in {} s", wait.as_secs().max(1));
    Fail(Status::Throttled, message)
}

#[derive(Debug, Default, PartialEq)]
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

fn escape(s: &str, out: &mut String) {
    for c in s.chars() {
        match c {
            '%' => out.push_str("%25"),
            '\t' => out.push_str("%09"),
            '\n' => out.push_str("%0A"),
            '\r' => out.push_str("%0D"),
            c => out.push(c),
        }
    }
}

fn unescape(field: &[u8]) -> Option<String> {
    let mut out = Vec::with_capacity(field.len());
    let mut bytes = field.iter();
    while let Some(&b) = bytes.next() {
        if b == b'%' {
            let hex = [*bytes.next()?, *bytes.next()?];
            if !hex.iter().all(u8::is_ascii_hexdigit) {
                return None;
            }
            out.push(u8::from_str_radix(std::str::from_utf8(&hex).ok()?, 16).ok()?);
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
fn encode(id: &str, status: Status, message: &str, page: &Page) -> Vec<u8> {
    let mut rows = String::new();
    let mut cursor = page.cursor.as_str();
    // room for the header with the longest cursor
    let budget = MAX_RESPONSE - (MAGIC.len() + id.len() + 3 * (MAX_MESSAGE + MAX_CURSOR) + 32);
    for (i, row) in page.rows.iter().enumerate() {
        let mut line = String::new();
        let position = row.position.map(|p| p.to_string()).unwrap_or_default();
        let fields = [
            row.kind,
            &row.uri,
            cut(&row.title, MAX_TEXT),
            cut(&row.subtitle, MAX_TEXT),
            &row.context,
            &position,
        ];
        for (j, field) in fields.iter().enumerate() {
            if j > 0 {
                line.push('\t');
            }
            escape(field, &mut line);
        }
        line.push('\n');
        if rows.len() + line.len() > budget {
            cursor = page.rows[..i].last().map_or("", |row| row.after.as_str());
            break;
        }
        rows.push_str(&line);
    }
    if cursor.len() > MAX_CURSOR {
        return encode(id, Status::Error, "Cursor too long", &Page::default());
    }
    let mut out = format!("{MAGIC}\n{id}\t{}\t", status.name());
    escape(cut(message, MAX_MESSAGE), &mut out);
    out.push('\t');
    escape(cursor, &mut out);
    out.push('\n');
    out.push_str(&rows);
    out.into_bytes()
}

// Serving

/// Binds `path` and answers each connection in its own task.
pub fn serve(path: &Path, link: watch::Receiver<Link>, signout: Signout) {
    let _ = std::fs::remove_file(path);
    let listener = match UnixListener::bind(path) {
        Ok(listener) => listener,
        Err(e) => return error!("Could not bind {}: {e}", path.display()),
    };
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    tokio::spawn(answer(stream, link.clone(), signout.clone()));
                }
                Err(e) => {
                    warn!("Browse socket: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });
}

async fn answer(mut stream: UnixStream, mut link: watch::Receiver<Link>, signout: Signout) {
    let deadline = tokio::time::Instant::now() + DEADLINE;
    let mut buf = Vec::new();
    let read = async {
        let mut chunk = [0u8; 1024];
        // to EOF, the request line's newline, or one byte past the limit
        while buf.len() <= MAX_REQUEST && buf.iter().filter(|&&b| b == b'\n').count() < 2 {
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
    };
    if tokio::time::timeout_at(deadline, read).await.is_err() {
        return;
    }
    let response = match parse(&buf) {
        Err(id) => encode(&id, Status::Bad, Status::Bad.message(), &Page::default()),
        Ok(request) => {
            let result = tokio::time::timeout_at(deadline, run(&request, &mut link, &signout))
                .await
                .unwrap_or_else(|_| {
                    Err(fail_with(Status::Offline, "Spotify didn't answer in time"))
                });
            match result {
                Ok(page) => encode(&request.id, Status::Ok, &page.message, &page),
                Err(Fail(status, message)) => {
                    encode(&request.id, status, &message, &Page::default())
                }
            }
        }
    };
    let _ = stream.write_all(&response).await;
    let _ = stream.shutdown().await;
}

async fn run(
    request: &Request,
    link: &mut watch::Receiver<Link>,
    signout: &Signout,
) -> Result<Page, Fail> {
    let Request { op, args, .. } = request;
    let Some(&(_, min, max)) = OPS.iter().find(|(name, ..)| name == op) else {
        return Err(fail(Status::Unsupported));
    };
    if args.len() < min || args.len() > max {
        return Err(fail(Status::Bad));
    }
    let arg = |i: usize| args.get(i).map(String::as_str).unwrap_or_default();
    // the optional cursor or position is the last argument
    let last = args.get(min).map(String::as_str).unwrap_or_default();
    match op.as_str() {
        "caps" => return Ok(caps()),
        "signout" => {
            let (done, signed_out) = oneshot::channel();
            signout.send(done).map_err(|_| fail(Status::Error))?;
            signed_out.await.map_err(|_| fail(Status::Error))?;
            return Ok(Page::default());
        }
        _ => {}
    }
    let (session, spirc) = ready(link).await?;
    if let Some(until) = *THROTTLED_UNTIL.lock().unwrap() {
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
        "play" => play(&spirc, arg(0), last),
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

fn uri_of<'a>(uri: &'a str, kind: &str) -> Result<&'a str, Fail> {
    match uri
        .strip_prefix("spotify:")
        .and_then(|r| r.strip_prefix(kind))
    {
        Some(id) if id.starts_with(':') => Ok(uri),
        _ => Err(fail(Status::Bad)),
    }
}

fn names(artists: &Artists) -> String {
    let names: Vec<_> = artists.iter().map(|a| a.name.as_str()).collect();
    names.join(", ")
}

/// Metadata for each URI, in order; `None` for an item that is gone.
async fn lookup<T: Metadata>(session: &Session, uris: &[String]) -> Result<Vec<Option<T>>, Fail> {
    // owned values: borrowed ones here make the task's future not Send
    let results: Vec<Result<T, Error>> = stream::iter(uris.to_vec())
        .map(|uri| {
            let session = session.clone();
            async move { T::get(&session, &SpotifyUri::from_uri(&uri)?).await }
        })
        .buffered(LOOKUPS)
        .collect()
        .await;
    results
        .into_iter()
        .map(|result| match result {
            Ok(item) => Ok(Some(item)),
            Err(e) if classify(&e).0 == Status::Unavailable => Ok(None),
            Err(e) => Err(e.into()),
        })
        .collect()
}

/// Track rows for (uri, position, after) entries, all in `context`.
async fn track_rows(
    session: &Session,
    entries: Vec<(String, Option<usize>, String)>,
    context: &str,
) -> Result<Vec<Row>, Fail> {
    let uris: Vec<_> = entries.iter().map(|e| e.0.clone()).collect();
    let tracks = lookup::<Track>(session, &uris).await?;
    Ok(entries
        .into_iter()
        .zip(tracks)
        .filter_map(|((uri, position, after), track)| {
            let track = track?;
            Some(Row {
                kind: "track",
                uri,
                title: track.name,
                subtitle: names(&track.artists),
                context: context.into(),
                position,
                after,
            })
        })
        .collect())
}

async fn album_rows(session: &Session, entries: Vec<(String, String)>) -> Result<Vec<Row>, Fail> {
    let uris: Vec<_> = entries.iter().map(|e| e.0.clone()).collect();
    let albums = lookup::<Album>(session, &uris).await?;
    Ok(entries
        .into_iter()
        .zip(albums)
        .filter_map(|((uri, after), album)| {
            let album = album?;
            Some(Row {
                kind: "album",
                uri,
                title: album.name,
                subtitle: names(&album.artists),
                after,
                ..Default::default()
            })
        })
        .collect())
}

// Ops

fn caps() -> Page {
    Page {
        rows: OPS
            .iter()
            .map(|(name, ..)| Row {
                kind: "op",
                uri: name.to_string(),
                ..Default::default()
            })
            .collect(),
        cursor: String::new(),
        message: VERSION.into(),
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
        None => return Err(fail_with(Status::Error, "No account details from Spotify")),
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
            kind: "account",
            uri: format!("spotify:user:{username}"),
            title: name,
            subtitle: "Premium".into(),
            ..Default::default()
        }],
        ..Default::default()
    })
}

/// The items from `from` of a playlist or the rootlist, whether or not the server began there.
fn window(list: &SelectedListContent, from: usize) -> (Vec<(usize, usize)>, String) {
    let base = list.contents.pos() as usize;
    let items = list.contents.items.len();
    let start = from.saturating_sub(base).min(items);
    let end = (start + PAGE).min(items);
    // (index in this response, index in the list)
    let taken: Vec<_> = (start..end).map(|i| (i, base + i)).collect();
    let next = base + end;
    let more = end > start && next < list.length() as usize;
    (
        taken,
        if more {
            next.to_string()
        } else {
            String::new()
        },
    )
}

fn playlist_rows(list: &SelectedListContent, from: usize) -> Page {
    let (taken, cursor) = window(list, from);
    let contents = &list.contents;
    let rows = taken
        .into_iter()
        .filter(|&(i, _)| contents.items[i].uri().starts_with("spotify:playlist:"))
        .map(|(i, index)| {
            let meta = contents.meta_items.get(i);
            Row {
                kind: "playlist",
                uri: contents.items[i].uri().into(),
                title: meta.map(|m| m.attributes.name().into()).unwrap_or_default(),
                subtitle: meta.map(|m| m.owner_username().into()).unwrap_or_default(),
                after: (index + 1).to_string(),
                ..Default::default()
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

/// A playlist's playable entries from `from`: (uri, index in the playlist, after).
fn playlist_tracks(
    list: &SelectedListContent,
    from: usize,
) -> (Vec<(String, Option<usize>, String)>, String) {
    let (taken, cursor) = window(list, from);
    let entries = taken
        .into_iter()
        .map(|(i, index)| (list.contents.items[i].uri().to_owned(), index))
        .filter(|(uri, _)| uri.starts_with("spotify:track:"))
        .map(|(uri, index)| (uri, Some(index), (index + 1).to_string()))
        .collect();
    (entries, cursor)
}

async fn playlist(session: &Session, uri: &str, from: usize) -> Result<Page, Fail> {
    let id = uri_of(uri, "playlist")?
        .rsplit(':')
        .next()
        .unwrap_or_default();
    let id = SpotifyId::from_base62(id).map_err(|_| fail(Status::Bad))?;
    let endpoint = format!(
        "/playlist/v2/playlist/{}?from={from}&length={PAGE}",
        id.to_base62()?
    );
    let bytes = session
        .spclient()
        .request(&http::Method::GET, &endpoint, None, None)
        .await?;
    let list = SelectedListContent::parse_from_bytes(&bytes).map_err(|_| fail(Status::Error))?;
    let (entries, cursor) = playlist_tracks(&list, from);
    Ok(Page {
        rows: track_rows(session, entries, uri).await?,
        cursor,
        ..Default::default()
    })
}

/// A walk through a collection v2 set. Its cursor is `position.skip.token`: the next row's
/// position, the matching items of the token's page already sent, and that page's token.
#[derive(Debug, PartialEq)]
struct Walk {
    prefix: &'static str,
    position: usize,
    skip: usize,
    token: String,
    /// (uri, position, after)
    found: Vec<(String, usize, String)>,
}

impl Walk {
    fn new(cursor: &str, prefix: &'static str) -> Result<Walk, Fail> {
        let mut walk = Walk {
            prefix,
            position: 0,
            skip: 0,
            token: String::new(),
            found: Vec::new(),
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
            self.position += 1;
            let after = self.cursor(matched);
            self.found
                .push((item.uri.clone(), self.position - 1, after));
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
            let entries = walk
                .found
                .into_iter()
                .map(|(uri, position, after)| (uri, Some(position), after))
                .collect();
            track_rows(session, entries, &context).await?
        }
        "spotify:album:" => {
            let entries = walk.found.into_iter().map(|(uri, _, after)| (uri, after));
            album_rows(session, entries.collect()).await?
        }
        _ => {
            let uris: Vec<_> = walk.found.iter().map(|f| f.0.clone()).collect();
            let artists = lookup::<Artist>(session, &uris).await?;
            walk.found
                .into_iter()
                .zip(artists)
                .filter_map(|((uri, _, after), artist)| {
                    Some(Row {
                        kind: "artist",
                        uri,
                        title: artist?.name,
                        after,
                        ..Default::default()
                    })
                })
                .collect()
        }
    };
    Ok(Page {
        rows,
        cursor,
        ..Default::default()
    })
}

async fn album(session: &Session, uri: &str, from: usize) -> Result<Page, Fail> {
    let album = Album::get(session, &SpotifyUri::from_uri(uri_of(uri, "album")?)?).await?;
    let tracks: Vec<_> = album.tracks().collect();
    let end = (from + PAGE).min(tracks.len());
    let mut entries = Vec::new();
    for (index, track) in tracks.iter().enumerate().take(end).skip(from) {
        entries.push((track.to_uri()?, Some(index), (index + 1).to_string()));
    }
    let cursor = if end < tracks.len() {
        end.to_string()
    } else {
        String::new()
    };
    Ok(Page {
        rows: track_rows(session, entries, uri).await?,
        cursor,
        ..Default::default()
    })
}

async fn artist(session: &Session, uri: &str) -> Result<Page, Fail> {
    let artist = Artist::get(session, &SpotifyUri::from_uri(uri_of(uri, "artist")?)?).await?;
    // ponytail: top tracks play on their own; an artist context is unverified on Connect
    let mut tracks = Vec::new();
    for track in artist.top_tracks.for_country(&session.country()).iter() {
        tracks.push((track.to_uri()?, None, String::new()));
    }
    let mut albums = Vec::new();
    for album in artist.albums_current().chain(artist.singles_current()) {
        albums.push((album.to_uri()?, String::new()));
    }
    albums.truncate(PAGE.saturating_sub(tracks.len()));
    let mut rows = track_rows(session, tracks, "").await?;
    rows.extend(album_rows(session, albums).await?);
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
        .await?;
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
                kind,
                uri: uri.into(),
                title,
                subtitle,
                after: (from + i + 1).to_string(),
                ..Default::default()
            })
        })
        .collect();
    let next = from + items.len();
    let total = section["totalCount"].as_u64().unwrap_or_default() as usize;
    Ok(Page {
        rows,
        cursor: if !items.is_empty() && next < total {
            next.to_string()
        } else {
            String::new()
        },
        ..Default::default()
    })
}

fn play(spirc: &Spirc, context: &str, position: &str) -> Result<Page, Fail> {
    if !context.starts_with("spotify:") {
        return Err(fail(Status::Bad));
    }
    let mut options = LoadRequestOptions {
        start_playing: true,
        ..Default::default()
    };
    let request = if !position.is_empty() {
        let index = position.parse().map_err(|_| fail(Status::Bad))?;
        options.playing_track = Some(PlayingTrack::Index(index));
        LoadRequest::from_context_uri(context.into(), options)
    } else if context.starts_with("spotify:track:") {
        LoadRequest::from_tracks(vec![context.into()], options)
    } else {
        LoadRequest::from_context_uri(context.into(), options)
    };
    spirc.activate()?;
    spirc.load(request)?;
    Ok(Page::default())
}

async fn queue(session: &Session, spirc: &Spirc) -> Result<Page, Fail> {
    let Some(state) = spirc.queue().await? else {
        return Ok(Page::default());
    };
    let entries = state
        .next
        .into_iter()
        .take(PAGE)
        .enumerate()
        .map(|(i, uri)| (uri, Some(i), String::new()))
        .collect();
    Ok(Page {
        rows: track_rows(session, entries, "").await?,
        ..Default::default()
    })
}

async fn skip(spirc: &Spirc, position: &str, uri: &str) -> Result<Page, Fail> {
    let position = position.parse().map_err(|_| fail(Status::Bad))?;
    match spirc.skip_to(position, uri.into()).await? {
        true => Ok(Page::default()),
        false => Err(fail_with(Status::Unavailable, "The queue has changed")),
    }
}

/// The play options, or `unavailable` while Q2 isn't playing (Connect then ignores them).
async fn active(spirc: &Spirc) -> Result<librespot::connect::QueueState, Fail> {
    spirc
        .queue()
        .await?
        .ok_or_else(|| fail_with(Status::Unavailable, "Nothing is playing on Q2"))
}

async fn enqueue(spirc: &Spirc, uri: &str) -> Result<Page, Fail> {
    uri_of(uri, "track")?;
    active(spirc).await?;
    spirc.add_to_queue(uri.into())?;
    Ok(Page::default())
}

async fn settings(spirc: &Spirc) -> Result<Page, Fail> {
    let state = active(spirc).await?;
    let setting = |name: &str, value: &str| Row {
        kind: "setting",
        uri: name.into(),
        title: value.into(),
        ..Default::default()
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
        Row {
            kind: "track",
            uri: uri.into(),
            ..Default::default()
        }
    }

    #[test]
    fn escaping_round_trips() {
        let text = "50% tab\there\nline\rcr – ünïcødé 🎧";
        let mut escaped = String::new();
        escape(text, &mut escaped);
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
        let unterminated = b"Q2SPOT/1\n8\tliked";
        assert_eq!(parse(unterminated).unwrap().op, "liked");
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
        for (line, status) in [
            ("1\tdance", Status::Unsupported),
            ("2\tplaylist", Status::Bad),
            ("3\tskip\t1", Status::Bad),
            ("4\tliked", Status::Login),
        ] {
            let request = parse(&request(line)).unwrap();
            let result = run(&request, &mut link, &signout).await;
            assert_eq!(result.unwrap_err().0, status, "{line}");
        }
        let request = parse(&request("5\tcaps")).unwrap();
        let caps = run(&request, &mut link, &signout).await.unwrap();
        assert_eq!(caps.message, VERSION);
        assert_eq!(caps.rows.len(), OPS.len());
    }

    #[test]
    fn responses_encode() {
        let page = Page {
            rows: vec![Row {
                context: "spotify:user:alice:collection".into(),
                position: Some(0),
                title: "Song\tTitle".into(),
                subtitle: "Band".into(),
                ..track("spotify:track:1")
            }],
            cursor: "1.1.tok".into(),
            ..Default::default()
        };
        assert_eq!(
            lines(&encode("7", Status::Ok, "", &page)),
            [
                "Q2SPOT/1",
                "7\tok\t\t1.1.tok",
                "track\tspotify:track:1\tSong%09Title\tBand\tspotify:user:alice:collection\t0"
            ]
        );
        let error = encode(
            "8",
            Status::Offline,
            Status::Offline.message(),
            &Page::default(),
        );
        assert_eq!(lines(&error)[1], "8\toffline\tSpotify can't be reached\t");
    }

    #[test]
    fn long_pages_are_cut_with_a_cursor() {
        // escaping triples these titles: 50 rows would pass 64 KiB
        let rows = (0..PAGE)
            .map(|i| Row {
                title: "%".repeat(400),
                subtitle: "\t".repeat(400),
                position: Some(i),
                after: format!("{}", i + 1),
                ..track("spotify:track:x")
            })
            .collect();
        let page = Page {
            rows,
            cursor: "50".into(),
            ..Default::default()
        };
        let response = encode("1", Status::Ok, "", &page);
        assert!(response.len() <= MAX_RESPONSE);
        let lines = lines(&response);
        let sent = lines.len() - 2;
        assert!(sent < PAGE && sent > 0);
        assert_eq!(lines[1], format!("1\tok\t\t{sent}"));
        assert!(lines[2].contains(&"%25".repeat(MAX_TEXT)));
        assert!(!lines[2].contains(&"%25".repeat(MAX_TEXT + 1)));
    }

    #[test]
    fn oversized_cursors_are_errors() {
        let page = Page {
            cursor: "x".repeat(MAX_CURSOR + 1),
            ..Default::default()
        };
        assert_eq!(
            lines(&encode("1", Status::Ok, "", &page))[1],
            "1\terror\tCursor too long\t"
        );
    }

    fn collection_page(uris: &[&str], next: &str) -> PageResponse {
        PageResponse {
            items: uris
                .iter()
                .map(|uri| CollectionItem {
                    uri: uri.to_string(),
                    ..Default::default()
                })
                .collect(),
            next_page_token: next.into(),
            ..Default::default()
        }
        // round trip through the wire format, as the server sends it
        .write_to_bytes()
        .map(|bytes| PageResponse::parse_from_bytes(&bytes).unwrap())
        .unwrap()
    }

    fn walk_to_end(cursor: &str, prefix: &'static str, pages: &[PageResponse]) -> (Walk, String) {
        let mut walk = Walk::new(cursor, prefix).unwrap();
        let tokens = ["", "p2", "p3"];
        let mut page = tokens.iter().position(|t| *t == walk.token).unwrap();
        loop {
            if let Some(cursor) = walk.take(&pages[page]) {
                return (walk, cursor);
            }
            page = tokens.iter().position(|t| *t == walk.token).unwrap();
        }
    }

    #[test]
    fn liked_skips_albums_and_counts_across_pages() {
        let album = "spotify:album:a";
        let mut first: Vec<&str> = vec![album; 30];
        first.extend(["spotify:track:t"; 20]);
        let pages = [
            collection_page(&first, "p2"),
            collection_page(&["spotify:track:t"; 50], "p3"),
            collection_page(&["spotify:track:t"; 10], ""),
        ];
        let (walk, cursor) = walk_to_end("", "spotify:track:", &pages);
        assert_eq!(walk.found.len(), PAGE);
        assert_eq!(walk.found[0].1, 0);
        assert_eq!(walk.found[49].1, 49);
        // the page filled 30 rows into p2: the next starts there
        assert_eq!(cursor, "50.30.p2");
        assert_eq!(walk.found[49].2, "50.30.p2");
        assert_eq!(walk.found[19].2, "20.20.");

        let (walk, cursor) = walk_to_end(&cursor, "spotify:track:", &pages);
        assert_eq!(walk.found.len(), 30);
        assert_eq!(walk.found[0].1, 50);
        assert_eq!(walk.found[29].1, 79);
        assert_eq!(cursor, "");
    }

    #[test]
    fn albums_stop_at_the_first_liked_track() {
        let mut first: Vec<&str> = vec!["spotify:album:a"; 3];
        first.push("spotify:track:t");
        let pages = [collection_page(&first, "p2")];
        let (walk, cursor) = walk_to_end("", "spotify:album:", &pages);
        assert_eq!(walk.found.len(), 3);
        assert_eq!(cursor, "");
    }

    #[test]
    fn walk_cursors_round_trip() {
        let walk = Walk::new("12.3.tok.with.dots", "spotify:track:").unwrap();
        assert_eq!((walk.position, walk.skip), (12, 3));
        assert_eq!(walk.cursor(3), "12.3.tok.with.dots");
        assert!(Walk::new("12", "spotify:track:").is_err());
        assert!(Walk::new("a.b.c", "spotify:track:").is_err());
        assert_eq!(offset("").ok(), Some(0));
        assert_eq!(offset("51").ok(), Some(51));
        assert!(offset("-1").is_err());
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
        let bytes = list.write_to_bytes().unwrap();
        SelectedListContent::parse_from_bytes(&bytes).unwrap()
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
        let (entries, cursor) = playlist_tracks(&list(&uris, 50, 120, false), 50);
        let positions: Vec<_> = entries.iter().map(|e| (e.0.as_str(), e.1)).collect();
        assert_eq!(
            positions,
            [
                ("spotify:track:a", Some(50)),
                ("spotify:track:b", Some(51)),
                ("spotify:track:a", Some(52)),
                ("spotify:track:c", Some(54)),
            ]
        );
        assert_eq!(entries[2].2, "53");
        assert_eq!(cursor, "55");
        // the server sent the whole list from 0
        let (entries, cursor) = playlist_tracks(&list(&uris, 0, 5, false), 2);
        assert_eq!(entries[0].1, Some(2));
        assert_eq!(cursor, "");
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
    fn throttling_is_answered_locally() {
        let fail = Fail::from(Error::from(HttpClientError::RetryAfter(
            Duration::from_secs(90),
        )));
        assert_eq!(fail.0, Status::Throttled);
        assert!(fail.1.contains("90 s"));
        let until = THROTTLED_UNTIL.lock().unwrap().take().unwrap();
        assert!(until > Instant::now() + Duration::from_secs(80));
    }
}
