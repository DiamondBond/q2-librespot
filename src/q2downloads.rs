//! Persistent saved collections; mutations stop and join the worker before deleting audio.
use super::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Clone, Default, Serialize, Deserialize)]
pub(super) struct Library {
    pub collections: Vec<Collection>,
    pub singles: Vec<String>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Collection {
    pub source: String,
    pub title: String,
    pub entries: Vec<Entry>,
    pub state: String,
    pub listing: bool,
    pub message: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Entry {
    pub uri: String,
    pub title: String,
    pub error: String,
}
impl Library {
    fn queue(&mut self, source: &str) -> bool {
        if self.collections.iter().any(|c| c.source == source) {
            return false;
        }
        if source.starts_with("spotify:track:") {
            self.singles.push(source.into());
        }
        self.collections.push(Collection {
            source: source.into(),
            title: if source == "liked" {
                "Liked Songs".into()
            } else {
                source.into()
            },
            entries: vec![],
            state: "queued".into(),
            listing: true,
            message: "Waiting to download".into(),
        });
        true
    }
}

impl OfflinePlayback {
    fn account_name(&self) -> String {
        self.cache
            .as_ref()
            .and_then(Cache::credentials)
            .and_then(|c| c.username)
            .unwrap_or_default()
    }
    fn library(&self) -> Result<Library, Fail> {
        Ok(self
            .cache
            .as_ref()
            .ok_or_else(|| fail(Status::Unavailable))?
            .download_records(&self.account_name())?)
    }
    fn save_library(&self, library: &Library) -> Result<(), Fail> {
        Ok(self
            .cache
            .as_ref()
            .ok_or_else(|| fail(Status::Unavailable))?
            .save_download_records(&self.account_name(), library)?)
    }
    pub(super) fn recover_downloads(&self) {
        let _records = self.library_write.lock().unwrap();
        if self.account_name().is_empty() {
            return;
        }
        let result = (|| {
            let mut lib = self.library()?;
            for c in &mut lib.collections {
                if matches!(
                    c.state.as_str(),
                    "queued" | "listing" | "downloading" | "waiting"
                ) {
                    c.state = "paused".into();
                    c.message = "Interrupted. Choose Resume".into();
                }
            }
            self.save_library(&lib)
        })();
        if let Err(e) = result {
            self.download.lock().unwrap().message = e.1;
        }
    }
    fn complete(&self) -> HashSet<String> {
        self.tracks()
            .into_iter()
            .filter(|t| {
                self.cache
                    .as_ref()
                    .is_some_and(|c| c.protected(librespot::core::FileId(t.file)))
            })
            .map(|t| t.uri)
            .collect()
    }

    pub(super) fn collections(&self, cursor: &str) -> Result<Page, Fail> {
        let lib = self.library()?;
        let complete = self.complete();
        let collections: Vec<_> = lib
            .collections
            .iter()
            .filter(|c| !c.source.starts_with("spotify:track:"))
            .collect();
        let (range, cursor) = page_of(collections.len(), offset(cursor)?);
        Ok(Page {
            rows: range
                .map(|i| {
                    let c = collections[i];
                    let done = c
                        .entries
                        .iter()
                        .filter(|e| complete.contains(&e.uri))
                        .count();
                    Row {
                        title: c.title.clone(),
                        subtitle: format!("{done}/{} · {}", c.entries.len(), c.state),
                        after: (i + 1).to_string(),
                        ..row("collection", c.source.clone(), "", None)
                    }
                })
                .collect(),
            cursor,
            ..Default::default()
        })
    }
    pub(super) fn collection(&self, source: &str, cursor: &str) -> Result<Page, Fail> {
        let lib = self.library()?;
        let c = lib
            .collections
            .iter()
            .find(|c| c.source == source)
            .ok_or_else(|| fail(Status::Unavailable))?;
        let tracks: HashMap<_, _> = self
            .tracks()
            .into_iter()
            .filter(|t| {
                self.cache
                    .as_ref()
                    .is_some_and(|c| c.protected(librespot::core::FileId(t.file)))
            })
            .map(|t| (t.uri.clone(), t))
            .collect();
        let complete: HashSet<_> = tracks.keys().cloned().collect();
        let (range, cursor) = page_of(c.entries.len(), offset(cursor)?);
        Ok(Page {
            rows: range
                .map(|i| {
                    let e = &c.entries[i];
                    Row {
                        title: tracks
                            .get(&e.uri)
                            .map(|t| t.title.clone())
                            .unwrap_or_else(|| {
                                if e.title.is_empty() {
                                    e.uri.clone()
                                } else {
                                    e.title.clone()
                                }
                            }),
                        subtitle: if complete.contains(&e.uri) {
                            "Downloaded".into()
                        } else if e.error.is_empty() {
                            "Not downloaded".into()
                        } else {
                            e.error.clone()
                        },
                        after: (i + 1).to_string(),
                        ..row(
                            if complete.contains(&e.uri) {
                                "track"
                            } else {
                                "missing"
                            },
                            e.uri.clone(),
                            source,
                            Some(i),
                        )
                    }
                })
                .collect(),
            cursor,
            message: c.message.clone(),
        })
    }
    pub(super) fn play_collection(&self, source: &str, position: &str) -> Result<Page, Fail> {
        let lib = self.library()?;
        let c = lib
            .collections
            .iter()
            .find(|c| c.source == source)
            .ok_or_else(|| fail(Status::Unavailable))?;
        let mut at = offset(position)?;
        let complete = self.complete();
        while at < c.entries.len() && !complete.contains(&c.entries[at].uri) {
            at += 1;
        }
        let entry = c.entries.get(at).ok_or_else(|| fail(Status::Unavailable))?;
        let id = SpotifyUri::from_uri(&entry.uri).map_err(|_| fail(Status::Unavailable))?;
        *self.current.lock().unwrap() = Some(OfflineQueue {
            protected_only: true,
            uris: c.entries.iter().map(|e| e.uri.clone()).collect(),
            at,
            playing: true,
            id: None,
            loading: true,
        });
        self.player.load_cached(id, true, 0);
        Ok(Page::default())
    }
    pub(super) fn jobs(&self, cursor: &str) -> Result<Page, Fail> {
        let lib = self.library()?;
        let complete = self.complete();
        let (range, cursor) = page_of(lib.collections.len(), offset(cursor)?);
        Ok(Page {
            rows: range
                .map(|i| {
                    let c = &lib.collections[i];
                    let done = c
                        .entries
                        .iter()
                        .filter(|e| complete.contains(&e.uri))
                        .count();
                    Row {
                        title: c.state.clone(),
                        subtitle: format!("{done}/{} · {}", c.entries.len(), c.message),
                        after: (i + 1).to_string(),
                        ..row("job", c.source.clone(), &c.title, Some(done))
                    }
                })
                .collect(),
            cursor,
            ..Default::default()
        })
    }
    pub(super) fn job_status(&self, source: &str) -> Result<Page, Fail> {
        let lib = self.library()?;
        let c = lib
            .collections
            .iter()
            .find(|c| c.source == source)
            .ok_or_else(|| fail(Status::Unavailable))?;
        let complete = self.complete();
        let done = c
            .entries
            .iter()
            .filter(|e| complete.contains(&e.uri))
            .count();
        let failed = c
            .entries
            .iter()
            .filter(|e| !e.error.is_empty() && !complete.contains(&e.uri))
            .count();
        let mut rows = Vec::new();
        for (key, value) in [
            ("state", c.state.clone()),
            ("completed", done.to_string()),
            ("total", c.entries.len().to_string()),
            ("failed", failed.to_string()),
            ("listing_pending", c.listing.to_string()),
        ] {
            rows.push(Row {
                title: value,
                ..row("value", key.into(), source, None)
            });
        }
        Ok(Page {
            rows,
            message: c.message.clone(),
            ..Default::default()
        })
    }
    async fn stop_worker(&self) {
        let task = self.download.lock().unwrap().task.take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        download_state(false, "Download paused");
    }
    fn start_worker(self: &Arc<Self>, session: Session) {
        session.http_client().report_rate_limits();
        let mut download = self.download.lock().unwrap();
        download.session = Some(session);
        if download.task.as_ref().is_some_and(|t| !t.is_finished()) {
            self.download_wake.notify_one();
            return;
        }
        let this = self.clone();
        download.task = Some(tokio::spawn(async move {
            loop {
                let wake = this.download_wake.notified();
                let session = this.download.lock().unwrap().session.clone().unwrap();
                if let Err(e) = this.work(&session).await {
                    this.recover_downloads();
                    this.download.lock().unwrap().message = e.1.clone();
                    download_state(false, &e.1);
                    break;
                }
                // Keep one sleeping worker: queueing at the idle boundary cannot strand a job.
                wake.await;
            }
        }));
    }
    fn queue_source(&self, source: &str) -> Result<(), Fail> {
        let _records = self.library_write.lock().unwrap();
        let mut lib = self.library()?;
        if lib.queue(source) {
            self.save_library(&lib)?;
        }
        Ok(())
    }
    pub(super) async fn save_collection(
        self: &Arc<Self>,
        source: &str,
        link: &mut watch::Receiver<Link>,
    ) -> Result<Page, Fail> {
        if source != "liked" && uri_of(source, "playlist").is_err() {
            uri_of(source, "track")?;
        }
        let _control = self.download_control.lock().await;
        let (session, _) = ready(link).await?;
        self.queue_source(source)?;
        self.start_worker(session);
        Ok(Page {
            message: "Download saved. See Downloads for progress or Resume".into(),
            ..Default::default()
        })
    }
    pub(super) async fn job_control(
        self: &Arc<Self>,
        source: &str,
        action: &str,
        link: &mut watch::Receiver<Link>,
    ) -> Result<Page, Fail> {
        if !matches!(action, "pause" | "resume" | "cancel" | "retry" | "update") {
            return Err(fail(Status::Bad));
        }
        let _control = self.download_control.lock().await;
        let session = if matches!(action, "resume" | "retry" | "update") {
            Some(ready(link).await?.0)
        } else {
            match &*link.borrow() {
                Link::Ready(session, _) => Some(session.clone()),
                _ => None,
            }
        };
        self.stop_worker().await;
        let _records = self.library_write.lock().unwrap();
        let mut lib = self.library()?;
        // Any other interrupted transfer is queued only within this running service.
        for c in &mut lib.collections {
            if matches!(c.state.as_str(), "listing" | "downloading" | "waiting") {
                c.state = "queued".into();
            }
        }
        let c = lib
            .collections
            .iter_mut()
            .find(|c| c.source == source)
            .ok_or_else(|| fail(Status::Unavailable))?;
        c.state = match action {
            "pause" => "paused",
            "cancel" => "canceled",
            _ => "queued",
        }
        .into();
        if action == "update" {
            c.listing = true;
        }
        if matches!(action, "retry" | "resume" | "update") {
            for e in &mut c.entries {
                e.error.clear();
            }
        }
        c.message = action.into();
        self.save_library(&lib)?;
        if let Some(session) = session {
            self.start_worker(session);
        }
        Ok(Page {
            message: format!("Download: {action}"),
            ..Default::default()
        })
    }
    fn progress(&self, source: &str, state: &str, message: &str) -> Result<(), Fail> {
        let _records = self.library_write.lock().unwrap();
        let mut lib = self.library()?;
        let c = lib
            .collections
            .iter_mut()
            .find(|c| c.source == source)
            .ok_or_else(|| fail(Status::Unavailable))?;
        let changed = c.state != state;
        c.state = state.into();
        c.message = message.into();
        // Audio records already make successful entries recoverable; don't rewrite a large manifest per track.
        if changed || state != "downloading" {
            self.save_library(&lib)?;
        }
        self.download.lock().unwrap().message = message.into();
        download_state(matches!(state, "listing" | "downloading"), message);
        Ok(())
    }
    async fn work(&self, session: &Session) -> Result<(), Fail> {
        loop {
            let Some(c) = self
                .library()?
                .collections
                .into_iter()
                .find(|c| c.state == "queued")
            else {
                download_state(false, "Downloads idle");
                return Ok(());
            };
            let result = self.transfer(session, &c).await;
            match result {
                Ok(()) => {
                    if self
                        .library()?
                        .collections
                        .iter()
                        .find(|x| x.source == c.source)
                        .is_some_and(|x| x.state != "partial")
                    {
                        self.progress(&c.source, "complete", "Download complete")?;
                    }
                }
                Err(e) => {
                    self.progress(
                        &c.source,
                        "paused",
                        &format!("{}; choose Retry Failed or Resume", e.1),
                    )?;
                    // Storage/auth/network trouble needs intervention; don't burn battery on the queue.
                    download_state(false, &e.1);
                    return Ok(());
                }
            }
        }
    }
    async fn cooldown(&self, source: &str) -> Result<(), Fail> {
        let until = THROTTLED.lock().unwrap()[SPCLIENT];
        if let Some(until) = until.filter(|t| *t > Instant::now()) {
            let wait = until.saturating_duration_since(Instant::now());
            self.progress(
                source,
                "waiting",
                &format!("Spotify cooldown: {} s", wait.as_secs() + 1),
            )?;
            tokio::time::sleep(wait).await;
        }
        Ok(())
    }
    async fn retry<T, F, Fut>(
        &self,
        source: &str,
        state: &str,
        message: &str,
        mut operation: F,
    ) -> Result<T, Fail>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, Fail>>,
    {
        let mut attempt = 0;
        loop {
            self.cooldown(source).await?;
            self.progress(source, state, message)?;
            match operation().await {
                Ok(value) => return Ok(value),
                Err(e) if e.0 == Status::Throttled => continue,
                Err(e) if matches!(e.0, Status::Offline | Status::Error) && attempt < 3 => {
                    attempt += 1;
                    self.progress(
                        source,
                        "waiting",
                        &format!("{}; retrying in {} s", e.1, 1 << attempt),
                    )?;
                    tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
    async fn transfer(&self, session: &Session, original: &Collection) -> Result<(), Fail> {
        let source = &original.source;
        let mut c = original.clone();
        if c.listing {
            let (title, entries) = self
                .retry(source, "listing", "Reading collection", || async {
                    tokio::time::timeout(Duration::from_secs(120), source_entries(session, source))
                        .await
                        .unwrap_or_else(|_| Err(fail(Status::Offline)))
                })
                .await?;
            // The old manifest survives until EVERY source page has succeeded.
            c.title = title;
            c.entries = entries;
            c.listing = false;
            let _records = self.library_write.lock().unwrap();
            let mut lib = self.library()?;
            *lib.collections
                .iter_mut()
                .find(|x| x.source == *source)
                .ok_or_else(|| fail(Status::Unavailable))? = c.clone();
            self.save_library(&lib)?;
        }
        let mut any_failed = false;
        for i in 0..c.entries.len() {
            if !c.entries[i].error.is_empty() {
                any_failed = true;
                continue;
            }
            let result = self
                .retry(
                    source,
                    "downloading",
                    &format!("Saving {} of {}", i + 1, c.entries.len()),
                    || async {
                        if uri_of(&c.entries[i].uri, "track").is_err() {
                            Err(fail(Status::Unavailable))
                        } else {
                            download_track(session, &c.entries[i].uri, self.bitrate).await
                        }
                    },
                )
                .await;
            match result {
                Ok(()) => {}
                Err(e) if e.0 == Status::Unavailable => {
                    c.entries[i].error = e.1;
                    any_failed = true;
                }
                Err(e) => return Err(e),
            }
            if let Some(t) = self.tracks().iter().find(|t| t.uri == c.entries[i].uri) {
                c.entries[i].title = t.title.clone();
            }
            if !c.entries[i].error.is_empty() {
                let _records = self.library_write.lock().unwrap();
                let mut lib = self.library()?;
                lib.collections
                    .iter_mut()
                    .find(|x| x.source == *source)
                    .ok_or_else(|| fail(Status::Unavailable))?
                    .entries[i] = c.entries[i].clone();
                self.save_library(&lib)?;
            }
        }
        {
            let _records = self.library_write.lock().unwrap();
            let mut lib = self.library()?;
            lib.collections
                .iter_mut()
                .find(|x| x.source == *source)
                .ok_or_else(|| fail(Status::Unavailable))?
                .entries = c.entries;
            self.save_library(&lib)?;
        }
        if any_failed {
            self.progress(source, "partial", "Unavailable tracks; choose Retry Failed")?;
        }
        // Keep partial state rather than overwriting it with complete.
        Ok(())
    }
    pub(super) async fn remove_downloads(
        &self,
        kind: &str,
        ids: &str,
        confirmed: bool,
    ) -> Result<Page, Fail> {
        if !matches!(kind, "collections" | "tracks" | "all") {
            return Err(fail(Status::Bad));
        }
        let _control = self.download_control.lock().await;
        if confirmed {
            self.stop_worker().await;
        }
        let _records = self.library_write.lock().unwrap();
        let mut lib = self.library()?;
        let selected: HashSet<&str> = ids.split(',').collect();
        let tracks = self.tracks();
        let removed_collections: HashSet<String> = lib
            .collections
            .iter()
            .filter(|c| {
                kind == "all" || kind == "collections" && selected.contains(c.source.as_str())
            })
            .map(|c| c.source.clone())
            .collect();
        let removed_tracks: HashSet<String> = tracks
            .iter()
            .filter(|t| kind == "all" || kind == "tracks" && selected.contains(t.uri.as_str()))
            .map(|t| t.uri.clone())
            .collect();
        let affected = lib
            .collections
            .iter()
            .filter(|c| c.entries.iter().any(|e| removed_tracks.contains(&e.uri)))
            .count();
        let message = format!(
            "Remove {} playlists, {} tracks? {affected} playlists may become incomplete",
            removed_collections.len(),
            removed_tracks.len()
        );
        if !confirmed {
            return Ok(Page {
                message,
                ..Default::default()
            });
        }
        lib.collections.retain(|c| {
            !removed_collections.contains(&c.source) && !removed_tracks.contains(&c.source)
        });
        lib.singles.retain(|u| !removed_tracks.contains(u));
        for c in &mut lib.collections {
            if matches!(
                c.state.as_str(),
                "queued" | "listing" | "downloading" | "waiting"
            ) {
                c.state = "paused".into();
            }
            for e in &mut c.entries {
                if removed_tracks.contains(&e.uri) {
                    e.error = "Removed. Choose Retry Failed to download again".into();
                    c.state = "partial".into();
                }
            }
        }
        // Save references first: interruption may leave an orphan, never a false completed entry.
        self.save_library(&lib)?;
        let needed: HashSet<&str> = lib
            .collections
            .iter()
            .flat_map(|c| c.entries.iter().map(|e| e.uri.as_str()))
            .chain(lib.singles.iter().map(String::as_str))
            .collect();
        let cache = self
            .cache
            .as_ref()
            .ok_or_else(|| fail(Status::Unavailable))?;
        for t in tracks {
            if removed_tracks.contains(&t.uri)
                || kind == "collections"
                    && cache.protected(librespot::core::FileId(t.file))
                    && !needed.contains(t.uri.as_str())
            {
                cache.remove_download_track(&t)?;
            }
        }
        if kind == "all" {
            cache.clear_downloads(&self.account_name())?;
        }
        Ok(Page {
            message: "Downloads removed. Login and settings kept".into(),
            ..Default::default()
        })
    }
}

async fn source_entries(session: &Session, source: &str) -> Result<(String, Vec<Entry>), Fail> {
    if source.starts_with("spotify:track:") {
        uri_of(source, "track")?;
        return Ok((
            source.into(),
            vec![Entry {
                uri: source.into(),
                title: String::new(),
                error: String::new(),
            }],
        ));
    }
    if source == "liked" {
        let context = format!("spotify:user:{}:collection", session.username());
        let ctx = session.spclient().get_context(&context).await?;
        let mut entries = Vec::new();
        let mut pages: std::collections::VecDeque<_> = ctx.pages.into();
        let mut seen = HashSet::new();
        while let Some(page) = pages.pop_front() {
            if page.tracks.is_empty() && !page.page_url().is_empty() {
                let url = page.page_url();
                if !seen.insert(url.to_string()) {
                    return Err(fail(Status::Error));
                }
                pages.push_front(context_page(session, url).await?);
                continue;
            }
            entries.extend(page.tracks.iter().map(|t| Entry {
                uri: t.uri().into(),
                title: String::new(),
                error: String::new(),
            }));
            if !page.next_page_url().is_empty() {
                let url = page.next_page_url();
                if !seen.insert(url.to_string()) {
                    return Err(fail(Status::Error));
                }
                pages.push_back(context_page(session, url).await?);
            }
        }
        return Ok(("Liked Songs".into(), entries));
    }
    let id = uri_of(source, "playlist")?;
    let mut from = 0;
    let mut entries = Vec::new();
    let mut title = String::new();
    let mut revision = None;
    loop {
        let endpoint = format!("/playlist/v2/playlist/{id}?from={from}&length={PAGE}");
        let bytes = session
            .spclient()
            .request(&http::Method::GET, &endpoint, None, None)
            .await?;
        let list =
            SelectedListContent::parse_from_bytes(&bytes).map_err(|_| fail(Status::Error))?;
        if from == 0 {
            title = list.attributes.name().into();
            revision = list.revision.clone();
        } else if list.revision != revision {
            return Err(Fail(
                Status::Error,
                "Playlist changed while listing; retry".into(),
            ));
        }
        if base(&list) > from {
            return Err(fail(Status::Error));
        }
        let (range, cursor) = window(&list, from);
        if range.is_empty() && from < list.length().max(0) as usize {
            return Err(fail(Status::Error));
        }
        entries.extend(range.map(|i| Entry {
            uri: list.contents.items[i].uri().into(),
            title: String::new(),
            error: String::new(),
        }));
        if cursor.is_empty() {
            break;
        }
        let next = offset(&cursor)?;
        if next <= from {
            return Err(fail(Status::Error));
        }
        from = next;
    }
    Ok((title, entries))
}
async fn context_page(
    session: &Session,
    url: &str,
) -> Result<librespot::protocol::context_page::ContextPage, Fail> {
    let path = url
        .strip_prefix("hm://")
        .map(|s| format!("/{s}"))
        .or_else(|| url.starts_with('/').then(|| url.to_string()))
        .ok_or_else(|| fail(Status::Bad))?;
    let bytes = session
        .spclient()
        .request(&http::Method::GET, &path, None, None)
        .await?;
    let mut page: librespot::protocol::context_page::ContextPage =
        protobuf_json_mapping::parse_from_str(
            std::str::from_utf8(&bytes).map_err(|_| fail(Status::Error))?,
        )
        .map_err(|_| fail(Status::Error))?;
    page.page_url = None; // An empty fetched page is terminal, not another lazy page.
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use librespot::core::{FileId, authentication::Credentials};
    fn fixture(
        name: &str,
    ) -> (
        std::path::PathBuf,
        Arc<OfflinePlayback>,
        Session,
        std::sync::MutexGuard<'static, ()>,
    ) {
        let guard = THROTTLE_TEST.lock().unwrap();
        let root =
            std::env::temp_dir().join(format!("q2-collections-{name}-{}", std::process::id()));
        let cache = Cache::new(
            Some(&root),
            Some(&root),
            Some(&root.join("audio")),
            Some(40),
        )
        .unwrap();
        cache.save_credentials(&Credentials::with_password("alice", "test"));
        let session = Session::new(Default::default(), Some(cache.clone()));
        session.set_username("alice");
        let player = Player::new(
            Default::default(),
            session.clone(),
            Box::new(librespot::playback::mixer::NoOpVolume),
            || {
                librespot::playback::audio_backend::find(Some("pipe".into())).unwrap()(
                    Some("/dev/null".into()),
                    Default::default(),
                )
            },
        );
        (
            root,
            OfflinePlayback::new(Some(cache), player, Bitrate::Bitrate160),
            session,
            guard,
        )
    }
    fn track(n: u8) -> OfflineTrack {
        OfflineTrack {
            account: "alice".into(),
            uri: format!("spotify:track:{n:022}"),
            file: [n; 20],
            format: AudioFileFormat::OGG_VORBIS_160 as i32,
            key: Some([1; 16]),
            title: format!("Track {n}"),
            artist: "Artist".into(),
            album: "Album".into(),
            duration_ms: 1000,
            is_explicit: false,
            played_at: unix_ms(),
        }
    }
    fn collection(source: &str, entries: &[String]) -> Collection {
        Collection {
            source: source.into(),
            title: "Named Mix".into(),
            entries: entries
                .iter()
                .map(|uri| Entry {
                    uri: uri.clone(),
                    title: "Title".into(),
                    error: String::new(),
                })
                .collect(),
            state: "queued".into(),
            listing: false,
            message: String::new(),
        }
    }
    fn save(offline: &OfflinePlayback, n: u8) -> OfflineTrack {
        let t = track(n);
        let cache = offline.cache.as_ref().unwrap();
        cache
            .save_file(FileId(t.file), &mut &b"complete audio"[..])
            .unwrap();
        cache.save_offline_track(&t).unwrap();
        cache.protect(&t).unwrap();
        t
    }
    #[test]
    fn repeated_requests_preserve_the_original_job_and_queue_order() {
        let mut lib = Library::default();
        assert!(lib.queue("liked"));
        assert!(lib.queue("spotify:playlist:0000000000000000000001"));
        lib.collections[0].title = "Previously saved title".into();
        lib.collections[0].state = "paused".into();
        lib.collections[0].entries.push(Entry {
            uri: track(1).uri,
            title: "One".into(),
            error: String::new(),
        });
        assert!(!lib.queue("liked"));
        assert_eq!(lib.collections.len(), 2);
        assert_eq!(lib.collections[0].title, "Previously saved title");
        assert_eq!(lib.collections[0].state, "paused");
        assert_eq!(lib.collections[0].entries.len(), 1);
        assert!(lib.queue(&track(2).uri));
        assert!(!lib.queue(&track(2).uri));
        assert_eq!(lib.singles, [track(2).uri]);
    }
    #[tokio::test]
    async fn ordered_pages_duplicates_missing_entries_and_offline_keys() {
        let (root, offline, _, _guard) = fixture("order");
        let a = save(&offline, 1);
        let b = save(&offline, 2);
        let entries: Vec<_> = (0..105)
            .map(|i| {
                if i == 51 {
                    "unavailable:local".into()
                } else if i % 2 == 0 {
                    a.uri.clone()
                } else {
                    b.uri.clone()
                }
            })
            .collect();
        offline
            .save_library(&Library {
                collections: vec![collection("liked", &entries)],
                singles: vec![],
            })
            .unwrap();
        let page = offline.collection("liked", "50").unwrap();
        assert_eq!(page.rows.len(), 50);
        assert_eq!(page.rows[0].position, Some(50));
        assert_eq!(page.rows[1].kind, "missing");
        assert_eq!(page.rows[2].position, Some(52));
        assert_eq!(page.cursor, "100");
        assert_eq!(offline.collection("liked", "100").unwrap().rows.len(), 5);
        offline.play_collection("liked", "50").unwrap();
        assert_eq!(offline.current.lock().unwrap().as_ref().unwrap().at, 50);
        assert!(offline.control(b"n"));
        assert_eq!(offline.current.lock().unwrap().as_ref().unwrap().at, 52);
        assert!(offline.control(b"b"));
        assert_eq!(offline.current.lock().unwrap().as_ref().unwrap().at, 50);
        offline.end();
        drop(offline);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn shared_audio_bulk_removal_clear_and_account_isolation() {
        let (root, offline, _, _guard) = fixture("removal");
        let a = save(&offline, 1);
        let b = save(&offline, 2);
        let single = save(&offline, 3);
        offline
            .save_library(&Library {
                collections: vec![
                    collection("one", &[a.uri.clone(), b.uri.clone(), single.uri.clone()]),
                    collection("two", &[a.uri.clone()]),
                ],
                singles: vec![single.uri.clone()],
            })
            .unwrap();
        offline
            .remove_downloads("collections", "one", false)
            .await
            .unwrap();
        assert_eq!(offline.tracks().len(), 3);
        offline
            .remove_downloads("collections", "one", true)
            .await
            .unwrap();
        assert_eq!(offline.tracks().len(), 2); // shared A and independently saved track survive
        let preview = offline
            .remove_downloads("tracks", &a.uri, false)
            .await
            .unwrap();
        assert!(
            preview
                .message
                .contains("1 playlists may become incomplete")
        );
        offline
            .remove_downloads("tracks", &a.uri, true)
            .await
            .unwrap();
        assert_eq!(offline.library().unwrap().collections[0].state, "partial");
        assert_eq!(
            offline.collection("two", "").unwrap().rows[0].kind,
            "missing"
        );
        let bob = Library {
            collections: vec![collection("bob-only", &[])],
            singles: vec![],
        };
        offline
            .cache
            .as_ref()
            .unwrap()
            .save_download_records("bob", &bob)
            .unwrap();
        offline.remove_downloads("all", "", true).await.unwrap();
        assert!(offline.tracks().is_empty());
        assert!(offline.library().unwrap().collections.is_empty());
        let cache = offline.cache.as_ref().unwrap();
        assert!(cache.credentials().is_some());
        assert_eq!(
            cache
                .download_records::<Library>("bob")
                .unwrap()
                .collections
                .len(),
            1
        );
        drop(offline);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn worker_reuses_cached_audio_survives_eviction_and_restarts_paused() {
        let (root, offline, session, _guard) = fixture("worker");
        let a = save(&offline, 1);
        offline
            .save_library(&Library {
                collections: vec![
                    collection("liked", &[a.uri.clone(), a.uri.clone()]),
                    collection("other", &["local:missing".into()]),
                ],
                singles: vec![],
            })
            .unwrap();
        offline.work(&session).await.unwrap();
        let lib = offline.library().unwrap();
        assert_eq!(lib.collections[0].state, "complete");
        assert_eq!(lib.collections[1].state, "partial");
        let cache = offline.cache.as_ref().unwrap();
        cache
            .save_file(
                FileId([9; 20]),
                &mut &b"large enough to evict every temporary cached track"[..],
            )
            .unwrap();
        assert_eq!(offline.tracks().len(), 1);
        assert!(cache.protected(FileId(a.file)));
        let mut lib = lib;
        lib.collections[0].state = "downloading".into();
        offline.save_library(&lib).unwrap();
        offline.recover_downloads();
        assert_eq!(offline.library().unwrap().collections[0].state, "paused");
        assert_eq!(offline.library().unwrap().collections[0].entries.len(), 2);
        assert!(offline.download.lock().unwrap().task.is_none());
        drop(offline);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn cancellation_is_joined_before_deletion_and_no_worker_recreates_audio() {
        let (root, offline, _, _guard) = fixture("cancel");
        let t = save(&offline, 1);
        offline
            .save_library(&Library {
                collections: vec![collection("liked", &[t.uri.clone()])],
                singles: vec![],
            })
            .unwrap();
        let task = tokio::spawn(std::future::pending::<()>());
        let handle = task.abort_handle();
        offline.download.lock().unwrap().task = Some(task);
        offline.remove_downloads("all", "", true).await.unwrap();
        assert!(handle.is_finished());
        assert!(offline.download.lock().unwrap().task.is_none());
        assert!(offline.tracks().is_empty());
        assert!(offline.library().unwrap().collections.is_empty());
        drop(offline);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn interrupted_manifest_and_failed_listing_keep_previous_order_and_title() {
        let (root, offline, session, _guard) = fixture("atomic");
        let t = save(&offline, 1);
        let mut c = collection("bad-source", &[t.uri.clone(), t.uri.clone()]);
        c.listing = true;
        offline
            .save_library(&Library {
                collections: vec![c.clone()],
                singles: vec![],
            })
            .unwrap();
        // A power loss leaves the temporary file; the committed manifest is still authoritative.
        let dir = root.join("offline/collections");
        let path = std::fs::read_dir(&dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::write(path.with_extension("tmp"), b"{\"collections\":[").unwrap();
        assert!(offline.transfer(&session, &c).await.is_err());
        let got = offline.library().unwrap().collections.remove(0);
        assert_eq!(got.title, "Named Mix");
        assert_eq!(got.entries.len(), 2);
        assert_eq!(got.entries[1].uri, t.uri);
        drop(offline);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn cooldown_is_shared_cancelable_and_network_retry_is_bounded() {
        let (root, offline, _, _guard) = fixture("retry");
        offline
            .save_library(&Library {
                collections: vec![collection("liked", &[])],
                singles: vec![],
            })
            .unwrap();
        let e = failed(
            HttpClientError::StatusCode(http::StatusCode::TOO_MANY_REQUESTS).into(),
            SPCLIENT,
        );
        assert_eq!(e.0, Status::Throttled);
        assert!(
            THROTTLED.lock().unwrap()[SPCLIENT].unwrap() > Instant::now() + Duration::from_secs(29)
        );
        // Waiting is not a failed entry and does not hold the Q2 awake; dropping the future cancels it.
        assert!(
            tokio::time::timeout(Duration::from_millis(5), offline.cooldown("liked"))
                .await
                .is_err()
        );
        assert_eq!(offline.job_status("liked").unwrap().rows[3].title, "0");
        assert_eq!(offline.library().unwrap().collections[0].state, "waiting");
        assert!(
            std::fs::read_to_string(download_file())
                .unwrap()
                .starts_with("downloading=0")
        );
        forget_throttling();
        let count = std::cell::Cell::new(0);
        let result: Result<(), Fail> = offline
            .retry("liked", "downloading", "Test interruption", || {
                count.set(count.get() + 1);
                std::future::ready(Err(fail(Status::Offline)))
            })
            .await;
        assert_eq!(result.unwrap_err().0, Status::Offline);
        assert_eq!(count.get(), 4); // initial attempt plus three bounded retries
        let count = std::cell::Cell::new(0);
        let result: Result<(), Fail> = offline
            .retry("liked", "downloading", "Unavailable", || {
                count.set(count.get() + 1);
                std::future::ready(Err(fail(Status::Unavailable)))
            })
            .await;
        assert_eq!(result.unwrap_err().0, Status::Unavailable);
        assert_eq!(count.get(), 1);
        drop(offline);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn queue_updates_and_progress_are_serialized_and_idle_worker_wakes() {
        let (root, offline, session, _guard) = fixture("concurrent");
        offline.queue_source("liked").unwrap();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for i in 0..50 {
                    offline
                        .queue_source(&format!("spotify:playlist:{i:022}"))
                        .unwrap();
                }
            });
            scope.spawn(|| {
                for _ in 0..50 {
                    offline.progress("liked", "listing", "Reading").unwrap();
                }
            });
        });
        assert_eq!(offline.library().unwrap().collections.len(), 51);
        let t = save(&offline, 1);
        offline
            .save_library(&Library {
                collections: vec![collection("liked", &[t.uri.clone()])],
                singles: vec![],
            })
            .unwrap();
        offline.start_worker(session.clone());
        for _ in 0..1000 {
            if offline.library().unwrap().collections[0].state == "complete" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(offline.library().unwrap().collections[0].state, "complete");
        {
            let _records = offline.library_write.lock().unwrap();
            let mut lib = offline.library().unwrap();
            lib.collections.push(collection("second", &[t.uri.clone()]));
            offline.save_library(&lib).unwrap();
        }
        offline.start_worker(session);
        for _ in 0..1000 {
            if offline.library().unwrap().collections[1].state == "complete" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(offline.library().unwrap().collections[1].state, "complete");
        offline.stop_worker().await;
        drop(offline);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn protected_audio_requires_a_complete_commit_and_clear_is_account_scoped() {
        let (root, offline, _, _guard) = fixture("integrity");
        let a = save(&offline, 1);
        let cache = offline.cache.as_ref().unwrap();
        cache.remove_file(FileId(a.file)).unwrap();
        assert_eq!(offline.tracks().len(), 1);
        let protected = root
            .join("offline/audio/616c696365")
            .join(FileId(a.file).to_base16().unwrap());
        std::fs::write(&protected, b"cut").unwrap();
        assert!(offline.tracks().is_empty());
        cache
            .protect_contents(&a, &mut &b"complete audio"[..])
            .unwrap();
        assert_eq!(offline.tracks().len(), 1);
        // A different account's protected file and metadata are outside Clear All's scope.
        let mut bob = track(2);
        bob.account = "bob".into();
        cache
            .protect_contents(&bob, &mut &b"bob audio"[..])
            .unwrap();
        cache.save_offline_track(&bob).unwrap();
        let orphan = protected.with_extension("tmp");
        std::fs::write(&orphan, b"interrupted protected copy").unwrap();
        offline.remove_downloads("all", "", true).await.unwrap();
        assert!(!protected.exists() && !orphan.exists());
        assert_eq!(cache.offline_tracks("bob").len(), 1);
        assert!(cache.credentials().is_some());
        cache.clear_offline().unwrap();
        assert!(cache.offline_tracks("bob").is_empty());
        drop(offline);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn storage_errors_keep_io_causes_and_never_claim_completion() {
        let (root, offline, _, _guard) = fixture("storage");
        let cache = offline.cache.as_ref().unwrap();
        let audio_dir = root.join("offline/audio");
        std::fs::create_dir_all(audio_dir.parent().unwrap()).unwrap();
        std::fs::write(&audio_dir, b"not a directory").unwrap();
        let err: Fail = cache
            .protect_contents(&track(1), &mut &b"audio"[..])
            .unwrap_err()
            .into();
        assert_eq!(err.0, Status::Storage);
        assert!(err.1.contains("Card:"));
        assert!(offline.tracks().is_empty());
        // ENOSPC and EROFS remain storage failures, not authentication or rate limits.
        for code in [28, 30, 13] {
            let e: Fail = Error::from(std::io::Error::from_raw_os_error(code)).into();
            assert_eq!(e.0, Status::Storage);
            assert!(e.1.contains(&std::io::Error::from_raw_os_error(code).to_string()));
        }
        drop(offline);
        std::fs::remove_dir_all(root).unwrap();
    }
}
