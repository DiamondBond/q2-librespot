mod receive;

use std::{
    cmp::min,
    fs,
    io::{self, Read, Seek, SeekFrom},
    path::Path,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    sync::{Condvar, Mutex},
    time::Duration,
};

use futures_util::{StreamExt, TryFutureExt, future::IntoStream};
use hyper::{Response, StatusCode, body::Incoming, header::CONTENT_RANGE};
use hyper_util::client::legacy::ResponseFuture;

use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::sync::{Semaphore, mpsc, oneshot};

use librespot_core::{Error, FileId, Session, cdn_url::CdnUrl};

use self::receive::audio_file_fetch;

use crate::range_set::{Range, RangeSet};

pub type AudioFileResult = Result<(), librespot_core::Error>;

const DOWNLOAD_STATUS_POISON_MSG: &str = "audio download status mutex should not be poisoned";

#[derive(Error, Debug)]
pub enum AudioFileError {
    #[error("other end of channel disconnected")]
    Channel,
    #[error("required header not found")]
    Header,
    #[error("streamer received no data")]
    NoData,
    #[error("no output available")]
    Output,
    #[error("invalid status code {0}")]
    StatusCode(StatusCode),
    #[error("wait timeout exceeded")]
    WaitTimeout,
}

impl From<AudioFileError> for Error {
    fn from(err: AudioFileError) -> Self {
        match err {
            AudioFileError::Channel => Error::aborted(err),
            AudioFileError::Header => Error::unavailable(err),
            AudioFileError::NoData => Error::unavailable(err),
            AudioFileError::Output => Error::aborted(err),
            AudioFileError::StatusCode(_) => Error::failed_precondition(err),
            AudioFileError::WaitTimeout => Error::deadline_exceeded(err),
        }
    }
}

#[derive(Clone)]
pub struct AudioFetchParams {
    /// The minimum size of a block that is requested from the Spotify servers in one request.
    /// This is the block size that is typically requested while doing a `seek()` on a file.
    /// The Symphonia decoder requires this to be a power of 2 and > 32 kB.
    /// Note: smaller requests can happen if part of the block is downloaded already.
    pub minimum_download_size: usize,

    /// The minimum network throughput that we expect. Together with the minimum download size,
    /// this will determine the time we will wait for a response.
    pub minimum_throughput: usize,

    /// The ping time that is used for calculations before a ping time was actually measured.
    pub initial_ping_time_estimate: Duration,

    /// If the measured ping time to the Spotify server is larger than this value, it is capped
    /// to avoid run-away block sizes and pre-fetching.
    pub maximum_assumed_ping_time: Duration,

    /// Before playback starts, this many seconds of data must be present.
    /// Note: the calculations are done using the nominal bitrate of the file. The actual amount
    /// of audio data may be larger or smaller.
    pub read_ahead_before_playback: Duration,

    /// While playing back, this many seconds of data ahead of the current read position are
    /// requested.
    /// Note: the calculations are done using the nominal bitrate of the file. The actual amount
    /// of audio data may be larger or smaller.
    pub read_ahead_during_playback: Duration,

    /// If the amount of data that is pending (requested but not received) is less than a certain amount,
    /// data is pre-fetched in addition to the read ahead settings above. The threshold for requesting more
    /// data is calculated as `<pending bytes> < PREFETCH_THRESHOLD_FACTOR * <ping time> * <nominal data rate>`
    pub prefetch_threshold_factor: f32,

    /// The time we will wait to obtain status updates on downloading.
    pub download_timeout: Duration,
}

impl Default for AudioFetchParams {
    fn default() -> Self {
        let minimum_download_size = 64 * 1024;
        let minimum_throughput = 8 * 1024;
        Self {
            minimum_download_size,
            minimum_throughput,
            initial_ping_time_estimate: Duration::from_millis(500),
            maximum_assumed_ping_time: Duration::from_millis(1500),
            read_ahead_before_playback: Duration::from_secs(1),
            read_ahead_during_playback: Duration::from_secs(5),
            prefetch_threshold_factor: 4.0,
            download_timeout: Duration::from_secs(
                (minimum_download_size / minimum_throughput) as u64,
            ),
        }
    }
}

static AUDIO_FETCH_PARAMS: OnceLock<AudioFetchParams> = OnceLock::new();

impl AudioFetchParams {
    pub fn set(params: AudioFetchParams) -> Result<(), AudioFetchParams> {
        AUDIO_FETCH_PARAMS.set(params)
    }

    pub fn get() -> &'static AudioFetchParams {
        AUDIO_FETCH_PARAMS.get_or_init(AudioFetchParams::default)
    }
}

pub enum AudioFile {
    Cached(fs::File),
    Streaming(AudioFileStreaming),
}

#[derive(Debug)]
pub struct StreamingRequest {
    streamer: IntoStream<ResponseFuture>,
    initial_response: Option<Response<Incoming>>,
    offset: usize,
    length: usize,
}

#[derive(Debug)]
pub enum StreamLoaderCommand {
    Fetch(Range), // signal the stream loader to fetch a range of the file
    Close,        // terminate and don't load any more data
}

#[derive(Clone)]
pub struct StreamLoaderController {
    channel_tx: Option<mpsc::UnboundedSender<StreamLoaderCommand>>,
    stream_shared: Option<Arc<AudioFileShared>>,
    file_size: usize,
}

impl StreamLoaderController {
    /// Transfer causes for a background downloader, before a generic wait timeout hides them.
    pub fn take_error(&self) -> Option<Error> {
        self.stream_shared
            .as_ref()?
            .transfer_error
            .lock()
            .unwrap()
            .take()
    }

    pub fn len(&self) -> usize {
        self.file_size
    }

    pub fn is_empty(&self) -> bool {
        self.file_size == 0
    }

    pub fn range_available(&self, range: Range) -> bool {
        if let Some(ref shared) = self.stream_shared {
            let download_status = shared
                .download_status
                .lock()
                .expect(DOWNLOAD_STATUS_POISON_MSG);

            range.length
                <= download_status
                    .downloaded
                    .contained_length_from_value(range.start)
        } else {
            range.length <= self.len() - range.start
        }
    }

    pub fn range_to_end_available(&self) -> bool {
        match self.stream_shared {
            Some(ref shared) => {
                let read_position = shared.read_position();
                self.range_available(Range::new(read_position, self.len() - read_position))
            }
            None => true,
        }
    }

    pub fn ping_time(&self) -> Option<Duration> {
        self.stream_shared.as_ref().map(|shared| shared.ping_time())
    }

    fn send_stream_loader_command(&self, command: StreamLoaderCommand) {
        if let Some(ref channel) = self.channel_tx {
            // Ignore the error in case the channel has been closed already.
            // This means that the file was completely downloaded.
            let _ = channel.send(command);
        }
    }

    pub fn fetch(&self, range: Range) {
        // signal the stream loader to fetch a range of the file
        self.send_stream_loader_command(StreamLoaderCommand::Fetch(range));
    }

    pub fn fetch_blocking(&self, mut range: Range) -> AudioFileResult {
        // signal the stream loader to tech a range of the file and block until it is loaded.

        // ensure the range is within the file's bounds.
        if range.start >= self.len() {
            range.length = 0;
        } else if range.end() > self.len() {
            range.length = self.len() - range.start;
        }

        self.fetch(range);

        if let Some(ref shared) = self.stream_shared {
            let mut download_status = shared
                .download_status
                .lock()
                .expect(DOWNLOAD_STATUS_POISON_MSG);
            let download_timeout = AudioFetchParams::get().download_timeout;

            while range.length
                > download_status
                    .downloaded
                    .contained_length_from_value(range.start)
            {
                let (new_download_status, wait_result) = shared
                    .cond
                    .wait_timeout(download_status, download_timeout)
                    .expect(DOWNLOAD_STATUS_POISON_MSG);

                download_status = new_download_status;
                if wait_result.timed_out() {
                    return Err(AudioFileError::WaitTimeout.into());
                }

                if range.length
                    > (download_status
                        .downloaded
                        .union(&download_status.requested)
                        .contained_length_from_value(range.start))
                {
                    // For some reason, the requested range is neither downloaded nor requested.
                    // This could be due to a network error. Request it again.
                    self.fetch(range);
                }
            }
        }

        Ok(())
    }

    pub fn fetch_next_and_wait(
        &self,
        request_length: usize,
        wait_length: usize,
    ) -> AudioFileResult {
        match self.stream_shared {
            Some(ref shared) => {
                let start = shared.read_position();

                let request_range = Range {
                    start,
                    length: request_length,
                };
                self.fetch(request_range);

                let wait_range = Range {
                    start,
                    length: wait_length,
                };
                self.fetch_blocking(wait_range)
            }
            None => Ok(()),
        }
    }

    pub fn set_random_access_mode(&self) {
        // optimise download strategy for random access
        if let Some(ref shared) = self.stream_shared {
            shared.set_download_streaming(false)
        }
    }

    pub fn set_stream_mode(&self) {
        // optimise download strategy for streaming
        if let Some(ref shared) = self.stream_shared {
            shared.set_download_streaming(true)
        }
    }

    pub fn close(&self) {
        // terminate stream loading and don't load any more data for this file.
        self.send_stream_loader_command(StreamLoaderCommand::Close);
    }

    pub fn from_local_file(file_size: u64) -> Self {
        Self {
            channel_tx: None,
            stream_shared: None,
            file_size: file_size as usize,
        }
    }
}

pub struct AudioFileStreaming {
    read_file: fs::File,
    position: u64,
    stream_loader_command_tx: mpsc::UnboundedSender<StreamLoaderCommand>,
    shared: Arc<AudioFileShared>,
}

struct AudioFileDownloadStatus {
    requested: RangeSet,
    downloaded: RangeSet,
}

struct AudioFileShared {
    transfer_error: Mutex<Option<Error>>,
    cdn_url: String,
    file_size: usize,
    bytes_per_second: usize,
    cond: Condvar,
    download_status: Mutex<AudioFileDownloadStatus>,
    download_streaming: AtomicBool,
    download_slots: Semaphore,
    ping_time_ms: AtomicUsize,
    read_position: AtomicUsize,
    throughput: AtomicUsize,
}

impl AudioFileShared {
    fn is_download_streaming(&self) -> bool {
        self.download_streaming.load(Ordering::Acquire)
    }

    fn set_download_streaming(&self, streaming: bool) {
        self.download_streaming.store(streaming, Ordering::Release)
    }

    fn ping_time(&self) -> Duration {
        let ping_time_ms = self.ping_time_ms.load(Ordering::Acquire);
        if ping_time_ms > 0 {
            Duration::from_millis(ping_time_ms as u64)
        } else {
            AudioFetchParams::get().initial_ping_time_estimate
        }
    }

    fn set_ping_time(&self, duration: Duration) {
        self.ping_time_ms
            .store(duration.as_millis() as usize, Ordering::Release)
    }

    fn throughput(&self) -> usize {
        self.throughput.load(Ordering::Acquire)
    }

    fn set_throughput(&self, throughput: usize) {
        self.throughput.store(throughput, Ordering::Release)
    }

    fn read_position(&self) -> usize {
        self.read_position.load(Ordering::Acquire)
    }

    fn set_read_position(&self, position: u64) {
        self.read_position
            .store(position as usize, Ordering::Release)
    }
}

impl AudioFile {
    pub async fn open(
        session: &Session,
        file_id: FileId,
        bytes_per_second: usize,
    ) -> Result<AudioFile, Error> {
        if let Some(file) = session.cache().and_then(|cache| cache.file(file_id)) {
            debug!("File {file_id} already in cache");
            return Ok(AudioFile::Cached(file));
        }

        debug!("Downloading file {file_id}");

        let (complete_tx, complete_rx) = oneshot::channel();

        let streaming =
            AudioFileStreaming::open(session.clone(), file_id, complete_tx, bytes_per_second);

        let session_ = session.clone();
        session.spawn(complete_rx.map_ok(move |mut file| {
            debug!("Downloading file {file_id} complete");

            if let Some(cache) = session_.cache() {
                if let Some(cache_id) = cache.file_path(file_id) {
                    if let Err(e) = cache.save_file(file_id, &mut file) {
                        error!("Error caching file {file_id} to {cache_id:?}: {e}");
                    } else {
                        debug!("File {file_id} cached to {cache_id:?}");
                    }
                }
            }
        }));

        Ok(AudioFile::Streaming(streaming.await?))
    }

    pub fn get_stream_loader_controller(&self) -> Result<StreamLoaderController, Error> {
        let controller = match self {
            AudioFile::Streaming(stream) => StreamLoaderController {
                channel_tx: Some(stream.stream_loader_command_tx.clone()),
                stream_shared: Some(stream.shared.clone()),
                file_size: stream.shared.file_size,
            },
            AudioFile::Cached(file) => StreamLoaderController {
                channel_tx: None,
                stream_shared: None,
                file_size: file.metadata()?.len() as usize,
            },
        };

        Ok(controller)
    }

    pub fn is_cached(&self) -> bool {
        matches!(self, AudioFile::Cached { .. })
    }
}

impl AudioFileStreaming {
    pub async fn open(
        session: Session,
        file_id: FileId,
        complete_tx: oneshot::Sender<NamedTempFile>,
        bytes_per_second: usize,
    ) -> Result<AudioFileStreaming, Error> {
        let cdn_url = CdnUrl::new(file_id).resolve_audio(&session).await?;

        let minimum_download_size = AudioFetchParams::get().minimum_download_size;

        let mut response_streamer_url = None;
        let mut last_error = None;
        let urls = cdn_url.try_get_urls()?;
        for url in &urls {
            // When the audio file is really small, this `download_size` may turn out to be
            // larger than the audio file we're going to stream later on. This is OK; requesting
            // `Content-Range` > `Content-Length` will return the complete file with status code
            // 206 Partial Content.
            let mut streamer =
                session
                    .spclient()
                    .stream_from_cdn(*url, 0, minimum_download_size)?;

            // Get the first chunk with the headers to get the file size.
            // The remainder of that chunk with possibly also a response body is then
            // further processed in `audio_file_fetch`.
            let streamer_result = tokio::time::timeout(Duration::from_secs(10), streamer.next())
                .await
                .map_err(|_| AudioFileError::WaitTimeout.into())
                .and_then(|x| x.ok_or_else(|| AudioFileError::NoData.into()))
                .and_then(|x| x.map_err(Error::from));

            match streamer_result {
                Ok(r) => {
                    response_streamer_url = Some((r, streamer, url));
                    break;
                }
                Err(e) => {
                    warn!("Fetching {url} failed with error {e:?}, trying next");
                    last_error = Some(e);
                }
            }
        }

        let Some((response, streamer, url)) = response_streamer_url else {
            return Err(last_error.unwrap_or_else(|| {
                Error::unavailable(format!("{} URLs failed, none left to try", urls.len()))
            }));
        };

        trace!("Streaming from {url}");

        let code = response.status();
        if code != StatusCode::PARTIAL_CONTENT {
            debug!("Opening audio file expected partial content but got: {code}");
            use librespot_core::http_client::HttpClientError;
            if code == StatusCode::TOO_MANY_REQUESTS {
                if let Some(wait) =
                    librespot_core::http_client::HttpClient::retry_after(response.headers())
                {
                    return Err(HttpClientError::RetryAfter(wait).into());
                }
            }
            return Err(HttpClientError::StatusCode(code).into());
        }

        let header_value = response
            .headers()
            .get(CONTENT_RANGE)
            .ok_or(AudioFileError::Header)?;
        let str_value = header_value.to_str()?;
        let hyphen_index = str_value.find('-').unwrap_or_default();
        let slash_index = str_value.find('/').unwrap_or_default();
        let upper_bound: usize = str_value[hyphen_index + 1..slash_index].parse()?;
        let file_size = str_value[slash_index + 1..].parse()?;

        let initial_request = StreamingRequest {
            streamer,
            initial_response: Some(response),
            offset: 0,
            length: upper_bound + 1,
        };

        let shared = Arc::new(AudioFileShared {
            transfer_error: Mutex::new(None),
            cdn_url: url.to_string(),
            file_size,
            bytes_per_second,
            cond: Condvar::new(),
            download_status: Mutex::new(AudioFileDownloadStatus {
                requested: RangeSet::new(),
                downloaded: RangeSet::new(),
            }),
            download_streaming: AtomicBool::new(false),
            download_slots: Semaphore::new(1),
            ping_time_ms: AtomicUsize::new(0),
            read_position: AtomicUsize::new(0),
            throughput: AtomicUsize::new(0),
        });

        let write_file = match session.config().tmp_fallback_dir.clone() {
            Some(fallback) => {
                let tmp_dir = session.config().tmp_dir.clone();
                let size = file_size as u64;
                tokio::task::spawn_blocking(move || {
                    temp_file(&tmp_dir, &fallback, size, mem_available())
                })
                .await
                .map_err(Error::internal)??
            }
            None => {
                let write_file = NamedTempFile::new_in(session.config().tmp_dir.clone())?;
                write_file.as_file().set_len(file_size as u64)?;
                write_file
            }
        };

        let read_file = write_file.reopen()?;

        let (stream_loader_command_tx, stream_loader_command_rx) =
            mpsc::unbounded_channel::<StreamLoaderCommand>();

        let error_shared = shared.clone();
        let fetch_session = session.clone();
        let fetch_shared = shared.clone();
        session.spawn(async move {
            if let Err(e) = audio_file_fetch(
                fetch_session,
                fetch_shared,
                initial_request,
                write_file,
                stream_loader_command_rx,
                complete_tx,
            )
            .await
            {
                error!("Audio fetch failed: {e}");
                *error_shared.transfer_error.lock().unwrap() = Some(e);
            }
            Ok::<(), Error>(())
        });

        Ok(AudioFileStreaming {
            read_file,
            position: 0,
            stream_loader_command_tx,
            shared,
        })
    }
}

/// RAM a tmpfs download must leave: without swap its pages cannot be reclaimed, and when RAM runs
/// out the kernel kills a process rather than report the tmpfs (half the RAM) full.
const RAM_RESERVE: u64 = 24 << 20;

/// The download's temporary file in `tmp_dir`, a tmpfs, with its space reserved there, so a full
/// one fails here rather than at a later write (a sparse `set_len` always succeeds, and two files
/// opened together could both seem to fit). One that does not fit, or would leave less than
/// `RAM_RESERVE` of the `ram` available, goes to `fallback`, sized there too: on a FAT card, which
/// has no sparse files, that writes it whole, as the decoder's first read near its end would
/// anyway, and this runs in a blocking task before playback waits on the file.
fn temp_file(
    tmp_dir: &Path,
    fallback: &Path,
    file_size: u64,
    ram: Option<u64>,
) -> io::Result<NamedTempFile> {
    let mut file = NamedTempFile::new_in(tmp_dir)?;
    let reserved = match ram {
        Some(ram) if ram < file_size.saturating_add(RAM_RESERVE) => Err(io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("{} MiB of RAM available", ram >> 20),
        )),
        _ => reserve(file.as_file(), file_size),
    };
    match reserved {
        Ok(()) => return Ok(file),
        Err(e) if needs_fallback(&e) => {
            file = NamedTempFile::new_in(fallback)?;
            info!(
                "Downloading {file_size} bytes to {} instead of {}: {e}",
                file.path().display(),
                tmp_dir.display()
            );
        }
        Err(_) => {} // cannot reserve there: sized as without a fallback
    }
    file.as_file().set_len(file_size)?;
    Ok(file)
}

/// Whether a file whose space could not be reserved belongs in the fallback dir: its file system
/// is full (ENOSPC), cannot hold a file that large (EFBIG) or is out of RAM (ENOMEM).
fn needs_fallback(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::StorageFull | io::ErrorKind::FileTooLarge | io::ErrorKind::OutOfMemory
    )
}

/// Allocates the file's `len` bytes now: fallocate fails where the file system cannot allocate
/// ahead, where glibc's posix_fallocate would write the whole file instead.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn reserve(file: &fs::File, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let len =
        libc::off_t::try_from(len).map_err(|_| io::Error::from(io::ErrorKind::FileTooLarge))?;
    let allocate = || {
        // SAFETY: the descriptor is open for as long as `file` is borrowed.
        match unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, len) } {
            0 => Ok(()),
            _ => Err(io::Error::last_os_error()),
        }
    };
    // A signal during a large allocation interrupts it (EINTR), undone: once more.
    match allocate() {
        Err(e) if e.kind() == io::ErrorKind::Interrupted => allocate(),
        reserved => reserved,
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn reserve(file: &fs::File, len: u64) -> io::Result<()> {
    file.set_len(len)
}

/// The RAM the kernel can still give (MemAvailable).
#[cfg(any(target_os = "linux", target_os = "android"))]
fn mem_available() -> Option<u64> {
    let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
    let kib = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemAvailable:"))?;
    let kib: u64 = kib.trim().strip_suffix("kB")?.trim().parse().ok()?;
    Some(kib << 10)
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn mem_available() -> Option<u64> {
    None
}

impl Read for AudioFileStreaming {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let offset = self.position as usize;

        if offset >= self.shared.file_size {
            return Ok(0);
        }

        let length = min(output.len(), self.shared.file_size - offset);
        if length == 0 {
            return Ok(0);
        }

        let read_ahead_during_playback = AudioFetchParams::get().read_ahead_during_playback;
        let length_to_request = if self.shared.is_download_streaming() {
            let length_to_request = length
                + (read_ahead_during_playback.as_secs_f32() * self.shared.bytes_per_second as f32)
                    as usize;

            // Due to the read-ahead stuff, we potentially request more than the actual request demanded.
            min(length_to_request, self.shared.file_size - offset)
        } else {
            length
        };

        let mut ranges_to_request = RangeSet::new();
        ranges_to_request.add_range(&Range::new(offset, length_to_request));

        let mut download_status = self
            .shared
            .download_status
            .lock()
            .expect(DOWNLOAD_STATUS_POISON_MSG);

        ranges_to_request.subtract_range_set(&download_status.downloaded);
        ranges_to_request.subtract_range_set(&download_status.requested);

        for &range in ranges_to_request.iter() {
            self.stream_loader_command_tx
                .send(StreamLoaderCommand::Fetch(range))
                .map_err(|err| io::Error::new(io::ErrorKind::BrokenPipe, err))?;
        }

        let download_timeout = AudioFetchParams::get().download_timeout;
        while !download_status.downloaded.contains(offset) {
            let (new_download_status, wait_result) = self
                .shared
                .cond
                .wait_timeout(download_status, download_timeout)
                .expect(DOWNLOAD_STATUS_POISON_MSG);

            download_status = new_download_status;
            if wait_result.timed_out() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    Error::deadline_exceeded(AudioFileError::WaitTimeout),
                ));
            }
        }
        let available_length = download_status
            .downloaded
            .contained_length_from_value(offset);

        drop(download_status);

        self.position = self.read_file.seek(SeekFrom::Start(offset as u64))?;
        let read_len = min(length, available_length);
        let read_len = self.read_file.read(&mut output[..read_len])?;

        self.position += read_len as u64;
        self.shared.set_read_position(self.position);

        Ok(read_len)
    }
}

impl Seek for AudioFileStreaming {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        // If we are already at this position, we don't need to switch download mode.
        // These checks and locks are less expensive than interrupting streaming.
        let current_position = self.position as i64;
        let requested_pos = match pos {
            SeekFrom::Start(pos) => pos as i64,
            SeekFrom::End(pos) => self.shared.file_size as i64 - pos - 1,
            SeekFrom::Current(pos) => current_position + pos,
        };
        if requested_pos == current_position {
            return Ok(current_position as u64);
        }

        // Again if we have already downloaded this part.
        let available = self
            .shared
            .download_status
            .lock()
            .expect(DOWNLOAD_STATUS_POISON_MSG)
            .downloaded
            .contains(requested_pos as usize);

        let mut was_streaming = false;
        if !available {
            // Ensure random access mode if we need to download this part.
            // Checking whether we are streaming now is a micro-optimization
            // to save an atomic load.
            was_streaming = self.shared.is_download_streaming();
            if was_streaming {
                self.shared.set_download_streaming(false);
            }
        }

        self.position = self.read_file.seek(pos)?;
        self.shared.set_read_position(self.position);

        if !available && was_streaming {
            self.shared.set_download_streaming(true);
        }

        Ok(self.position)
    }
}

impl Read for AudioFile {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        match *self {
            AudioFile::Cached(ref mut file) => file.read(output),
            AudioFile::Streaming(ref mut file) => file.read(output),
        }
    }
}

impl Seek for AudioFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match *self {
            AudioFile::Cached(ref mut file) => file.seek(pos),
            AudioFile::Streaming(ref mut file) => file.seek(pos),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_full_file_system_or_ram_falls_back() {
        use io::ErrorKind::*;
        for kind in [StorageFull, FileTooLarge, OutOfMemory] {
            assert!(needs_fallback(&kind.into()));
        }
        for kind in [Interrupted, PermissionDenied, Unsupported] {
            assert!(!needs_fallback(&kind.into()));
        }
        #[cfg(target_os = "linux")]
        for (errno, falls_back) in [
            (libc::ENOSPC, true),
            (libc::EFBIG, true),
            (libc::ENOMEM, true),
            (libc::EINTR, false),
            (libc::EOPNOTSUPP, false),
        ] {
            assert_eq!(
                needs_fallback(&io::Error::from_raw_os_error(errno)),
                falls_back
            );
        }
    }

    #[test]
    fn downloads_that_would_leave_too_little_ram_go_sized_to_the_fallback() {
        let (tmp, fallback) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let len = |file: &NamedTempFile| file.as_file().metadata().unwrap().len();
        let count = |dir: &tempfile::TempDir| fs::read_dir(dir.path()).unwrap().count();
        let size = 1 << 16;
        for ram in [None, Some(size + RAM_RESERVE)] {
            let file = temp_file(tmp.path(), fallback.path(), size, ram).unwrap();
            assert!(file.path().starts_with(tmp.path()));
            assert_eq!(len(&file), size);
        }
        let ram = Some(size + RAM_RESERVE - 1);
        let file = temp_file(tmp.path(), fallback.path(), size, ram).unwrap();
        assert!(file.path().starts_with(fallback.path()));
        assert_eq!(len(&file), size);
        assert_eq!((count(&tmp), count(&fallback)), (0, 1));
    }
}
