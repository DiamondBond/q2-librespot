//! Q2 browse probe: checks that a login saved by a Spotify Connect handoff can
//! reach the internal APIs a device-side browser needs, then optionally starts
//! a context on this device. Contacts Spotify; never run by `cargo test`.
//!
//!   cargo run --example q2_probe -- CACHE_DIR [CONTEXT_URI [INDEX]]
//!
//! CACHE_DIR holds the credentials.json librespot saved (`--cache`). Prints
//! counts and URIs, never credentials or tokens. Logs at info only: librespot's
//! trace logs include bearer tokens, so RUST_LOG is deliberately not read.

use std::{env, process::exit, time::Duration};

use librespot::{
    connect::{ConnectConfig, LoadRequest, LoadRequestOptions, PlayingTrack, Spirc},
    core::{Error, cache::Cache, config::SessionConfig, session::Session, spotify_uri::SpotifyUri},
    metadata::{Album, Artist, Metadata},
    playback::{
        audio_backend,
        config::{AudioFormat, PlayerConfig},
        mixer::{self, MixerConfig},
        player::{Player, PlayerEvent},
    },
    protocol::playlist4_external::SelectedListContent,
};
use protobuf::Message;

const PAGE: i32 = 50;
/// spotatui v0.45.0's searchDesktop hashes, newest first; a retired one gives 412.
const SEARCH_HASH: &str = "eef7cc54888d91bdd6802623477873caa3948ae173a0c34fd86827b267e94c03";

fn report<T>(name: &str, result: Result<T, Error>, show: impl FnOnce(&T) -> String) -> Option<T> {
    match result {
        Ok(value) => {
            println!("ok   {name}: {}", show(&value));
            Some(value)
        }
        Err(e) => {
            println!("FAIL {name}: {e}");
            None
        }
    }
}

/// Up to `pages` pages of a collection set, following next_page_token.
async fn collection(session: &Session, set: &str, pages: usize) -> Result<Vec<String>, Error> {
    let mut uris = Vec::new();
    let mut token = String::new();
    for _ in 0..pages {
        let page = session
            .spclient()
            .get_collection_page(set, &token, PAGE)
            .await?;
        uris.extend(
            page.items
                .into_iter()
                .filter(|item| !item.is_removed)
                .map(|item| item.uri),
        );
        token = page.next_page_token;
        if token.is_empty() {
            break;
        }
    }
    Ok(uris)
}

async fn search(session: &Session, term: &str) -> Result<serde_json::Value, Error> {
    let variables = serde_json::json!({
        "searchTerm": term, "offset": 0, "limit": 10, "numberOfTopResults": 5,
        "includeAudiobooks": false, "includeArtistHasConcertsField": false,
        "includePreReleases": false, "includeLocalConcertsField": false,
        "includeAuthors": false,
    });
    // ponytail: one hash; a 412 means it has rotated, so bump SEARCH_HASH by hand
    let answer = session
        .spclient()
        .pathfinder_query("searchDesktop", SEARCH_HASH, variables)
        .await?;
    if answer["data"]["searchV2"].is_object() {
        Ok(answer["data"]["searchV2"].clone())
    } else {
        Err(Error::unavailable(answer["errors"].to_string()))
    }
}

fn count(section: &serde_json::Value) -> String {
    format!(
        "{} (first {})",
        section["totalCount"],
        section["items"][0]
            .pointer("/item/data/uri")
            .or_else(|| section["items"][0].pointer("/data/uri"))
            .unwrap_or(&serde_json::Value::Null)
    )
}

#[tokio::main]
async fn main() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();
    let args: Vec<_> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} CACHE_DIR [CONTEXT_URI [INDEX]]", args[0]);
        exit(2);
    }
    let cache = Cache::new(Some(&args[1]), None, None, None).unwrap();
    let Some(credentials) = cache.credentials() else {
        eprintln!("No credentials.json in {}", args[1]);
        exit(1);
    };

    let session = Session::new(SessionConfig::default(), Some(cache));
    let mixer = mixer::find(None).unwrap()(MixerConfig::default()).unwrap();
    let player = Player::new(
        PlayerConfig::default(),
        session.clone(),
        mixer.get_soft_volume(),
        move || audio_backend::find(None).unwrap()(None, AudioFormat::default()),
    );
    let config = ConnectConfig {
        name: "Q2 probe".into(),
        ..Default::default()
    };
    let (spirc, spirc_task) =
        match Spirc::new(config, session.clone(), credentials, player.clone(), mixer).await {
            Ok(spirc) => spirc,
            Err(e) => {
                println!("FAIL connect: {e}");
                exit(1);
            }
        };
    let spirc_task = tokio::spawn(spirc_task);
    tokio::time::sleep(Duration::from_secs(2)).await; // the account type follows the login
    println!(
        "ok   connect: account {}, country {}",
        session.get_user_attribute("type").unwrap_or_default(),
        session.country()
    );

    report("login5 token", session.login5().auth_token().await, |t| {
        format!("{} expires in {:?}", t.token_type, t.expires_in)
    });
    let rootlist = session
        .spclient()
        .get_rootlist(0, Some(PAGE as usize))
        .await
        .and_then(|bytes| Ok(SelectedListContent::parse_from_bytes(&bytes)?));
    let rootlist = report("playlists", rootlist, |list| {
        let contents = &list.contents;
        let first = contents
            .items
            .iter()
            .position(|i| i.uri().starts_with("spotify:playlist:"));
        let meta = first.and_then(|i| contents.meta_items.get(i));
        format!(
            "{} rows, {} meta (first {} \"{}\" by {})",
            contents.items.len(),
            contents.meta_items.len(),
            first.map_or("none", |i| contents.items[i].uri()),
            meta.map_or("", |m| m.attributes.name()),
            meta.map_or("", |m| m.owner_username()),
        )
    });
    // q2browse pages a playlist with from/length; pos shows whether the server began there
    let first_playlist = rootlist.as_ref().and_then(|list| {
        list.contents
            .items
            .iter()
            .map(|i| i.uri())
            .find(|uri| uri.starts_with("spotify:playlist:"))
            .map(str::to_owned)
    });
    if let Some(uri) = first_playlist {
        let id = uri.rsplit(':').next().unwrap_or_default();
        let page = session
            .spclient()
            .request(
                &http::Method::GET,
                &format!("/playlist/v2/playlist/{id}?from={PAGE}&length={PAGE}"),
                None,
                None,
            )
            .await
            .and_then(|bytes| Ok(SelectedListContent::parse_from_bytes(&bytes)?));
        report("playlist page two", page, |list| {
            format!(
                "{uri}: pos {}, {} rows of {}",
                list.contents.pos(),
                list.contents.items.len(),
                list.length()
            )
        });
    }

    let saved = report(
        "collection",
        collection(&session, "collection", 40).await,
        |uris| {
            let tracks = uris
                .iter()
                .filter(|u| u.starts_with("spotify:track:"))
                .count();
            format!(
                "{tracks} liked tracks, {} albums (up to 40 pages)",
                uris.len() - tracks
            )
        },
    )
    .unwrap_or_default();
    let artists = report(
        "followed artists",
        collection(&session, "artist", 1).await,
        |uris| format!("{} (first page)", uris.len()),
    )
    .unwrap_or_default();

    if let Some(uri) = saved.iter().find(|u| u.starts_with("spotify:album:")) {
        let album = match SpotifyUri::from_uri(uri) {
            Ok(id) => Album::get(&session, &id).await,
            Err(e) => Err(e),
        };
        report("album", album, |a| {
            format!("{uri}: {} tracks", a.tracks().count())
        });
    }
    if let Some(uri) = artists.first() {
        let artist = match SpotifyUri::from_uri(uri) {
            Ok(id) => Artist::get(&session, &id).await,
            Err(e) => Err(e),
        };
        report("artist", artist, |a| {
            format!(
                "{uri}: {} top tracks, {} albums",
                a.top_tracks.for_country(&session.country()).len(),
                a.albums_current().count()
            )
        });
    }
    report("search", search(&session, "radiohead").await, |s| {
        format!(
            "tracks {}, albums {}, artists {}, playlists {}",
            count(&s["tracksV2"]),
            count(&s["albumsV2"]),
            count(&s["artists"]),
            count(&s["playlists"])
        )
    });

    let Some(context) = args.get(2) else {
        spirc.shutdown().ok();
        return;
    };
    let index = args.get(3).and_then(|i| i.parse().ok()).unwrap_or(0);
    let mut events = player.get_player_event_channel();
    spirc.activate().unwrap();
    spirc
        .load(LoadRequest::from_context_uri(
            context.clone(),
            LoadRequestOptions {
                start_playing: true,
                playing_track: Some(PlayingTrack::Index(index)),
                ..Default::default()
            },
        ))
        .unwrap();
    println!("     playing {context} from index {index}; Ctrl-C stops");
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            match event {
                PlayerEvent::Playing { track_id, .. } => println!("     playing {track_id}"),
                PlayerEvent::EndOfTrack { track_id, .. } => println!("     ended {track_id}"),
                PlayerEvent::Unavailable { track_id, .. } => {
                    println!("     unavailable {track_id}")
                }
                _ => {}
            }
        }
    });
    tokio::signal::ctrl_c().await.ok();
    spirc.shutdown().ok();
    spirc_task.await.ok();
}
