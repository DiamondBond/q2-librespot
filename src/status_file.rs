use bytes::Bytes;
use http::Request;
use librespot::{
    core::http_client::HttpClient,
    metadata::audio::UniqueFields,
    playback::player::{PlayerEvent, PlayerEventChannel},
};
use log::warn;
use std::{
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use url::Url;

// The smallest cover at least this wide is fetched (Spotify has 64, 300 and 640 px).
const COVER_MIN_WIDTH: i32 = 160;

#[derive(Default)]
struct Status {
    state: &'static str,
    track: String,
    title: String,
    artist: String,
    album: String,
    duration_ms: u32,
    position_ms: u32,
    at_ms: u64,
    cover: bool,
}

/// Milliseconds on CLOCK_MONOTONIC, the clock a reader on the same machine can compare
/// `at` against to move the position on while playing.
fn now_ms() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: ts is a valid timespec for clock_gettime to fill.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1000 + ts.tv_nsec as u64 / 1_000_000
}

fn one_line(s: &str) -> String {
    s.replace(['\n', '\r'], " ")
}

/// Writes path whole, through a temporary file and a rename, so a reader never sees half of it.
fn write_atomic(path: &Path, data: &[u8]) {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    if let Err(e) = fs::write(&tmp, data).and_then(|_| fs::rename(&tmp, path)) {
        warn!("Could not write {}: {e}", path.display());
    }
}

fn write_status(path: &Path, s: &Status) {
    let mut out = String::new();
    let _ = write!(
        out,
        "state={}\ntrack={}\ntitle={}\nartist={}\nalbum={}\nduration={}\nposition={}\nat={}\ncover={}\n",
        s.state,
        s.track,
        s.title,
        s.artist,
        s.album,
        s.duration_ms,
        s.position_ms,
        s.at_ms,
        s.cover as u8
    );
    write_atomic(path, out.as_bytes());
}

fn cover_path(path: &Path) -> PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(".jpg");
    p.into()
}

async fn fetch_cover(client: &HttpClient, url: &str) -> Option<Bytes> {
    let req = Request::get(url).body(Bytes::new()).ok()?;
    match client.request_body(req).await {
        Ok(body) => Some(body),
        Err(e) => {
            warn!("Could not fetch cover {url}: {e}");
            None
        }
    }
}

/// Keeps `path` up to date with the player's state for another program to show: the Connect
/// session (`state=none` without one), playing, paused or stopped, the track's metadata and its
/// position at `at` (CLOCK_MONOTONIC ms). Each track's cover is fetched to `path.jpg`, and
/// `cover=1` once that file holds the current track's.
pub fn run(mut events: PlayerEventChannel, path: PathBuf, proxy: Option<Url>) {
    let status = Arc::new(Mutex::new(Status {
        state: "none",
        ..Default::default()
    }));
    write_status(&path, &status.lock().unwrap());
    let client = Arc::new(HttpClient::new(proxy.as_ref()));

    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            let mut s = status.lock().unwrap();
            match event {
                PlayerEvent::SessionConnected { .. } => {
                    *s = Status {
                        state: "stopped",
                        ..Default::default()
                    }
                }
                PlayerEvent::SessionDisconnected { .. } => {
                    *s = Status {
                        state: "none",
                        ..Default::default()
                    }
                }
                PlayerEvent::TrackChanged { audio_item } => {
                    s.track = audio_item.track_id.to_id().unwrap_or_default();
                    s.title = one_line(&audio_item.name);
                    s.duration_ms = audio_item.duration_ms;
                    (s.artist, s.album) = match audio_item.unique_fields {
                        UniqueFields::Track { artists, album, .. } => (
                            artists
                                .0
                                .iter()
                                .map(|a| a.name.as_str())
                                .collect::<Vec<_>>()
                                .join(", "),
                            album,
                        ),
                        UniqueFields::Local { artists, album, .. } => {
                            (artists.unwrap_or_default(), album.unwrap_or_default())
                        }
                        UniqueFields::Episode { show_name, .. } => (show_name, String::new()),
                    };
                    s.artist = one_line(&s.artist);
                    s.album = one_line(&s.album);
                    s.cover = false;

                    let mut covers = audio_item.covers;
                    covers.sort_by_key(|c| c.width);
                    let cover = covers
                        .iter()
                        .find(|c| c.width >= COVER_MIN_WIDTH)
                        .or(covers.last())
                        .map(|c| c.url.clone());
                    if let Some(url) = cover {
                        let (status, client, path) = (status.clone(), client.clone(), path.clone());
                        let track = s.track.clone();
                        tokio::spawn(async move {
                            let Some(jpeg) = fetch_cover(&client, &url).await else {
                                return;
                            };
                            let mut s = status.lock().unwrap();
                            if s.track == track {
                                write_atomic(&cover_path(&path), &jpeg);
                                s.cover = true;
                                write_status(&path, &s);
                            }
                        });
                    }
                }
                PlayerEvent::Playing { position_ms, .. } => {
                    (s.state, s.position_ms, s.at_ms) = ("playing", position_ms, now_ms())
                }
                PlayerEvent::Paused { position_ms, .. } => {
                    (s.state, s.position_ms, s.at_ms) = ("paused", position_ms, now_ms())
                }
                PlayerEvent::Seeked { position_ms, .. }
                | PlayerEvent::PositionCorrection { position_ms, .. } => {
                    (s.position_ms, s.at_ms) = (position_ms, now_ms())
                }
                PlayerEvent::Stopped { .. } => (s.state, s.position_ms) = ("stopped", 0),
                _ => continue,
            }
            write_status(&path, &s);
        }
    });
}
