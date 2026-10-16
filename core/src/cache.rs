use std::{
    cmp::Reverse,
    collections::HashMap,
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::SystemTime,
};

use priority_queue::PriorityQueue;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{Error, FileId, authentication::Credentials, error::ErrorKind};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OfflineTrack {
    pub account: String,
    pub uri: String,
    pub file: [u8; 20],
    pub format: i32,
    pub key: Option<[u8; 16]>,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub duration_ms: u32,
    pub is_explicit: bool,
    pub played_at: u64,
}

const CACHE_LIMITER_POISON_MSG: &str = "cache limiter mutex should not be poisoned";

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("audio cache location is not configured")]
    Path,
}

impl From<CacheError> for Error {
    fn from(err: CacheError) -> Self {
        Error::failed_precondition(err)
    }
}

/// Some kind of data structure that holds some paths, the size of these files and a timestamp.
/// It keeps track of the file sizes and is able to pop the path with the oldest timestamp if
/// a given limit is exceeded.
struct SizeLimiter {
    queue: PriorityQueue<PathBuf, Reverse<SystemTime>>,
    sizes: HashMap<PathBuf, u64>,
    size_limit: u64,
    in_use: u64,
}

impl SizeLimiter {
    /// Creates a new instance with the given size limit.
    fn new(limit: u64) -> Self {
        Self {
            queue: PriorityQueue::new(),
            sizes: HashMap::new(),
            size_limit: limit,
            in_use: 0,
        }
    }

    /// Adds an entry to this data structure.
    ///
    /// If this file is already contained, it will be updated accordingly.
    fn add(&mut self, file: &Path, size: u64, accessed: SystemTime) {
        self.in_use += size;
        self.queue.push(file.to_owned(), Reverse(accessed));
        if let Some(old_size) = self.sizes.insert(file.to_owned(), size) {
            // It's important that decreasing happens after
            // increasing the size, to prevent an overflow.
            self.in_use -= old_size;
        }
    }

    /// Returns true if the limit is exceeded.
    fn exceeds_limit(&self) -> bool {
        self.in_use > self.size_limit
    }

    /// Returns the least recently accessed file if the size of the cache exceeds
    /// the limit.
    ///
    /// The entry is removed from the data structure, but the caller is responsible
    /// to delete the file in the file system.
    fn pop(&mut self) -> Option<PathBuf> {
        if self.exceeds_limit() {
            if let Some((next, _)) = self.queue.pop() {
                if let Some(size) = self.sizes.remove(&next) {
                    self.in_use -= size;
                } else {
                    error!("`queue` and `sizes` should have the same keys.");
                }
                Some(next)
            } else {
                error!("in_use was > 0, so the queue should have contained an item.");
                None
            }
        } else {
            None
        }
    }

    /// Updates the timestamp of an existing element. Returns `true` if the item did exist.
    fn update(&mut self, file: &Path, access_time: SystemTime) -> bool {
        self.queue
            .change_priority(file, Reverse(access_time))
            .is_some()
    }

    /// Removes an element with the specified path. Returns `true` if the item did exist.
    fn remove(&mut self, file: &Path) -> bool {
        if self.queue.remove(file).is_none() {
            return false;
        }

        if let Some(size) = self.sizes.remove(file) {
            self.in_use -= size;
        } else {
            error!("`queue` and `sizes` should have the same keys.");
        }

        true
    }
}

struct FsSizeLimiter {
    limiter: Mutex<SizeLimiter>,
}

impl FsSizeLimiter {
    /// Returns access time and file size of a given path.
    fn get_metadata(file: &Path) -> io::Result<(SystemTime, u64)> {
        let metadata = file.metadata()?;

        // The first of the following timestamps which is available will be chosen as access time:
        // 1. Access time
        // 2. Modification time
        // 3. Creation time
        // 4. Current time
        let access_time = metadata
            .accessed()
            .or_else(|_| metadata.modified())
            .or_else(|_| metadata.created())
            .unwrap_or_else(|_| SystemTime::now());

        let size = metadata.len();

        Ok((access_time, size))
    }

    /// Recursively search a directory for files and add them to the `limiter` struct.
    fn init_dir(limiter: &mut SizeLimiter, path: &Path) {
        let list_dir = match fs::read_dir(path) {
            Ok(list_dir) => list_dir,
            Err(e) => {
                warn!("Could not read directory {path:?} in cache dir: {e}");
                return;
            }
        };

        for entry in list_dir {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    warn!("Could not directory {path:?} in cache dir: {e}");
                    return;
                }
            };

            match entry.file_type() {
                Ok(file_type) if file_type.is_dir() || file_type.is_symlink() => {
                    Self::init_dir(limiter, &entry.path())
                }
                Ok(file_type) if file_type.is_file() => {
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == "tmp") {
                        let _ = fs::remove_file(path);
                        continue;
                    }
                    match Self::get_metadata(&path) {
                        Ok((access_time, size)) => {
                            limiter.add(&path, size, access_time);
                        }
                        Err(e) => {
                            warn!("Could not read file {path:?} in cache dir: {e}")
                        }
                    }
                }
                Ok(ft) => {
                    warn!(
                        "File {:?} in cache dir has unsupported type {:?}",
                        entry.path(),
                        ft
                    )
                }
                Err(e) => {
                    warn!(
                        "Could not get type of file {:?} in cache dir: {}",
                        entry.path(),
                        e
                    )
                }
            };
        }
    }

    fn add(&self, file: &Path, size: u64) {
        self.limiter
            .lock()
            .expect(CACHE_LIMITER_POISON_MSG)
            .add(file, size, SystemTime::now())
    }

    fn touch(&self, file: &Path) -> bool {
        self.limiter
            .lock()
            .expect(CACHE_LIMITER_POISON_MSG)
            .update(file, SystemTime::now())
    }

    fn remove(&self, file: &Path) -> bool {
        self.limiter
            .lock()
            .expect(CACHE_LIMITER_POISON_MSG)
            .remove(file)
    }

    fn prune_internal<F: FnMut() -> Option<PathBuf>>(mut pop: F) -> Result<(), Error> {
        let mut first = true;
        let mut count = 0;
        let mut last_error = None;

        while let Some(file) = pop() {
            if first {
                debug!("Cache dir exceeds limit, removing least recently used files.");
                first = false;
            }

            let res = fs::remove_file(&file);
            if let Err(e) = res {
                warn!("Could not remove file {file:?} from cache dir: {e}");
                last_error = Some(e);
            } else {
                count += 1;
            }
        }

        if count > 0 {
            info!("Removed {count} cache files.");
        }

        if let Some(err) = last_error {
            Err(err.into())
        } else {
            Ok(())
        }
    }

    fn prune(&self) -> Result<(), Error> {
        Self::prune_internal(|| self.limiter.lock().expect(CACHE_LIMITER_POISON_MSG).pop())
    }

    fn new(path: &Path, limit: u64) -> Result<Self, Error> {
        let mut limiter = SizeLimiter::new(limit);

        Self::init_dir(&mut limiter, path);
        Self::prune_internal(|| limiter.pop())?;

        Ok(Self {
            limiter: Mutex::new(limiter),
        })
    }
}

/// A cache for volume, credentials and audio files.
#[derive(Clone)]
pub struct Cache {
    credentials_location: Option<PathBuf>,
    volume_location: Option<PathBuf>,
    audio_location: Option<PathBuf>,
    offline_location: Option<PathBuf>,
    size_limiter: Option<Arc<FsSizeLimiter>>,
}

impl Cache {
    pub fn new<P: AsRef<Path>>(
        credentials_path: Option<P>,
        volume_path: Option<P>,
        audio_path: Option<P>,
        size_limit: Option<u64>,
    ) -> Result<Self, Error> {
        let mut size_limiter = None;

        if let Some(location) = &credentials_path {
            fs::create_dir_all(location)?;
        }

        let credentials_location = credentials_path
            .as_ref()
            .map(|p| p.as_ref().join("credentials.json"));

        if let Some(location) = &volume_path {
            fs::create_dir_all(location)?;
        }

        let volume_location = volume_path.as_ref().map(|p| p.as_ref().join("volume"));

        if let Some(location) = &audio_path {
            fs::create_dir_all(location)?;

            if let Some(limit) = size_limit {
                let limiter = FsSizeLimiter::new(location.as_ref(), limit)?;
                size_limiter = Some(Arc::new(limiter));
            }
        }

        let audio_location = audio_path.map(|p| p.as_ref().to_owned());

        let cache = Cache {
            credentials_location,
            volume_location,
            audio_location,
            offline_location: credentials_path
                .as_ref()
                .map(|p| p.as_ref().join("offline")),
            size_limiter,
        };

        Ok(cache)
    }

    pub fn credentials(&self) -> Option<Credentials> {
        let location = self.credentials_location.as_ref()?;

        // This closure is just convencience to enable the question mark operator
        let read = || -> Result<Credentials, Error> {
            let mut file = File::open(location)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            Ok(serde_json::from_str(&contents)?)
        };

        match read() {
            Ok(c) => Some(c),
            Err(e) => {
                // If the file did not exist, the file was probably not written
                // before. Otherwise, log the error.
                if e.kind != ErrorKind::NotFound {
                    warn!("Error reading credentials from cache: {e}");
                }
                None
            }
        }
    }

    pub fn save_credentials(&self, cred: &Credentials) {
        if let Some(location) = &self.credentials_location {
            let result = File::create(location).and_then(|mut file| {
                let data = serde_json::to_string(cred)?;
                write!(file, "{data}")
            });

            if let Err(e) = result {
                warn!("Cannot save credentials to cache: {e}")
            }
        }
    }

    /// Deletes the saved credentials, if any.
    pub fn remove_credentials(&self) -> Result<(), Error> {
        match self.credentials_location.as_ref().map(fs::remove_file) {
            Some(Err(e)) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
            _ => Ok(()),
        }
    }

    pub fn volume(&self) -> Option<u16> {
        let location = self.volume_location.as_ref()?;

        let read = || -> Result<u16, Error> {
            let mut file = File::open(location)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            Ok(contents.parse()?)
        };

        match read() {
            Ok(v) => Some(v),
            Err(e) => {
                if e.kind != ErrorKind::NotFound {
                    warn!("Error reading volume from cache: {e}");
                }
                None
            }
        }
    }

    pub fn save_volume(&self, volume: u16) {
        if let Some(ref location) = self.volume_location {
            let result = File::create(location).and_then(|mut file| write!(file, "{volume}"));
            if let Err(e) = result {
                warn!("Cannot save volume to cache: {e}");
            }
        }
    }

    pub fn file_path(&self, file: FileId) -> Option<PathBuf> {
        match file.to_base16() {
            Ok(name) => self.audio_location.as_ref().map(|location| {
                let mut path = location.join(&name[0..2]);
                path.push(&name[2..]);
                path
            }),
            Err(e) => {
                warn!("Invalid FileId: {e}");
                None
            }
        }
    }

    pub fn file(&self, file: FileId) -> Option<File> {
        let path = self.file_path(file)?;
        match File::open(&path) {
            Ok(file) => {
                if let Some(limiter) = self.size_limiter.as_deref() {
                    if !limiter.touch(&path) {
                        error!("limiter could not touch {path:?}");
                    }
                }
                Some(file)
            }
            Err(e) => {
                if e.kind() != io::ErrorKind::NotFound {
                    warn!("Error reading file from cache: {e}")
                }
                None
            }
        }
    }

    pub fn save_file<F: Read>(&self, file: FileId, contents: &mut F) -> Result<PathBuf, Error> {
        if let Some(path) = self.file_path(file) {
            if let Some(parent) = path.parent() {
                let tmp = path.with_extension("tmp");
                let result = fs::create_dir_all(parent)
                    .and_then(|_| File::create(&tmp))
                    .and_then(|mut out| {
                        let size = io::copy(contents, &mut out)?;
                        out.sync_all()?;
                        fs::rename(&tmp, &path)?;
                        Ok(size)
                    });
                if let Ok(size) = result {
                    if let Some(limiter) = self.size_limiter.as_deref() {
                        limiter.add(&path, size);
                        limiter.prune()?;
                    }
                    return Ok(path);
                }
                let _ = fs::remove_file(tmp);
            }
        }
        Err(CacheError::Path.into())
    }

    pub fn save_offline_track(&self, track: &OfflineTrack) -> Result<(), Error> {
        let dir = self.offline_location.as_ref().ok_or(CacheError::Path)?;
        fs::create_dir_all(dir)?;
        let name = FileId(track.file).to_base16()?;
        let path = dir.join(name);
        let tmp = path.with_extension("tmp");
        let data = serde_json::to_vec(track)?;
        File::create(&tmp)?.write_all(&data)?;
        fs::rename(tmp, path)?;
        Ok(())
    }

    pub fn offline_tracks(&self, account: &str) -> Vec<OfflineTrack> {
        let Some(dir) = &self.offline_location else {
            return Vec::new();
        };
        let Ok(entries) = fs::read_dir(dir) else {
            return Vec::new();
        };
        let mut tracks = Vec::new();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        for entry in entries.flatten() {
            if !entry.file_type().is_ok_and(|t| t.is_file()) {
                continue;
            }
            let Ok(data) = fs::read(entry.path()) else {
                continue;
            };
            let Ok(track) = serde_json::from_slice::<OfflineTrack>(&data) else {
                continue;
            };
            if track.account != account || track.account.is_empty() {
                continue;
            }
            let Some(path) = self.file_path(FileId(track.file)) else {
                continue;
            };
            if path.metadata().is_ok_and(|m| m.is_file() && m.len() > 0) {
                tracks.push(track);
            } else if now.saturating_sub(track.played_at) > 86_400 {
                // Evicted audio: its record and art go too, so they stay within the audio's count.
                // A day's grace, so a download still running keeps the key it will need.
                let _ = fs::remove_file(entry.path());
                if let Some(art) = track
                    .uri
                    .rsplit(':')
                    .next()
                    .and_then(|id| self.offline_cover_path(id))
                {
                    let _ = fs::remove_file(art);
                }
            }
        }
        tracks.sort_by(|a, b| b.played_at.cmp(&a.played_at));
        tracks
    }

    fn offline_cover_path(&self, id: &str) -> Option<PathBuf> {
        if id.len() != 22 || !id.bytes().all(|c| c.is_ascii_alphanumeric()) {
            return None;
        }
        Some(
            self.offline_location
                .as_ref()?
                .join("art")
                .join(format!("{id}.jpg")),
        )
    }

    pub fn offline_cover(&self, id: &str) -> Option<Vec<u8>> {
        fs::read(self.offline_cover_path(id)?).ok()
    }

    pub fn save_offline_cover(&self, id: &str, jpeg: &[u8]) -> Result<(), Error> {
        let path = self.offline_cover_path(id).ok_or(CacheError::Path)?;
        fs::create_dir_all(path.parent().ok_or(CacheError::Path)?)?;
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, jpeg)?;
        fs::rename(tmp, path)?;
        Ok(())
    }

    pub fn clear_offline(&self) -> Result<(), Error> {
        for dir in [&self.audio_location, &self.offline_location]
            .into_iter()
            .flatten()
        {
            match fs::remove_dir_all(dir) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    pub fn remove_file(&self, file: FileId) -> Result<(), Error> {
        let path = self.file_path(file).ok_or(CacheError::Path)?;

        fs::remove_file(&path)?;
        if let Some(limiter) = self.size_limiter.as_deref() {
            limiter.remove(&path);
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

    fn ordered_time(v: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(v)
    }

    #[test]
    fn test_size_limiter() {
        let mut limiter = SizeLimiter::new(1000);

        limiter.add(Path::new("a"), 500, ordered_time(2));
        limiter.add(Path::new("b"), 500, ordered_time(1));

        // b (500) -> a (500)  => sum: 1000 <= 1000
        assert!(!limiter.exceeds_limit());
        assert_eq!(limiter.pop(), None);

        limiter.add(Path::new("c"), 1000, ordered_time(3));

        // b (500) -> a (500) -> c (1000)  => sum: 2000 > 1000
        assert!(limiter.exceeds_limit());
        assert_eq!(limiter.pop().as_deref(), Some(Path::new("b")));
        // a (500) -> c (1000)  => sum: 1500 > 1000
        assert_eq!(limiter.pop().as_deref(), Some(Path::new("a")));
        // c (1000)   => sum: 1000 <= 1000
        assert_eq!(limiter.pop().as_deref(), None);

        limiter.add(Path::new("d"), 5, ordered_time(2));
        // d (5) -> c (1000) => sum: 1005 > 1000
        assert_eq!(limiter.pop().as_deref(), Some(Path::new("d")));
        // c (1000)   => sum: 1000 <= 1000
        assert_eq!(limiter.pop().as_deref(), None);

        // Test updating

        limiter.add(Path::new("e"), 500, ordered_time(3));
        //  c (1000) -> e (500)  => sum: 1500 > 1000
        assert!(limiter.update(Path::new("c"), ordered_time(4)));
        // e (500) -> c (1000)  => sum: 1500 > 1000
        assert_eq!(limiter.pop().as_deref(), Some(Path::new("e")));
        // c (1000)  => sum: 1000 <= 1000

        // Test removing
        limiter.add(Path::new("f"), 500, ordered_time(2));
        assert!(limiter.remove(Path::new("c")));
        assert!(!limiter.exceeds_limit());
    }

    #[test]
    fn offline_records_require_complete_audio_and_disappear_on_signout() {
        let root = std::env::temp_dir().join(format!("q2-cache-test-{}", std::process::id()));
        let audio = root.join("files");
        let cache = Cache::new(Some(&root), Some(&root), Some(&audio), Some(12)).unwrap();
        let record = |n: u8, played_at| OfflineTrack {
            account: "alice".into(),
            uri: format!("spotify:track:{n:022}"),
            file: [n; 20],
            format: 1,
            key: Some([n; 16]),
            title: format!("Track {n}"),
            artist: "Artist".into(),
            album: "Album".into(),
            duration_ms: 1000,
            is_explicit: false,
            played_at,
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // A day-old record without its audio is pruned: audio arriving later doesn't bring it back.
        let stale = record(3, now - 2 * 86_400);
        cache.save_offline_track(&stale).unwrap();
        assert!(cache.offline_tracks("alice").is_empty());
        cache.save_file(FileId(stale.file), &mut &b"1"[..]).unwrap();
        assert!(cache.offline_tracks("alice").is_empty());
        cache.remove_file(FileId(stale.file)).unwrap();
        let a = record(1, now);
        cache.save_offline_track(&a).unwrap();
        assert!(cache.offline_tracks("alice").is_empty());
        let path = cache.file_path(FileId(a.file)).unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path.with_extension("tmp"), b"partial").unwrap();
        assert!(cache.offline_tracks("alice").is_empty());
        cache
            .save_file(FileId(a.file), &mut &b"12345678"[..])
            .unwrap();
        assert_eq!(cache.offline_tracks("alice").len(), 1);
        assert!(cache.offline_tracks("bob").is_empty());
        let b = record(2, now + 1);
        cache.save_offline_track(&b).unwrap();
        cache
            .save_file(FileId(b.file), &mut &b"abcdefgh"[..])
            .unwrap();
        assert_eq!(cache.offline_tracks("alice")[0].uri, b.uri);
        assert_eq!(cache.offline_tracks("alice").len(), 1); // oldest file evicted
        let recovered = Cache::new(Some(&root), Some(&root), Some(&audio), Some(12)).unwrap();
        assert_eq!(recovered.offline_tracks("alice").len(), 1);
        recovered
            .save_offline_cover("abcdefghijklmnopqrstuv", b"jpeg")
            .unwrap();
        assert_eq!(
            recovered.offline_cover("abcdefghijklmnopqrstuv"),
            Some(b"jpeg".to_vec())
        );
        recovered.clear_offline().unwrap();
        assert!(recovered.offline_tracks("alice").is_empty());
        assert!(recovered.offline_cover("abcdefghijklmnopqrstuv").is_none());
        let _ = fs::remove_dir_all(root);
    }
}
