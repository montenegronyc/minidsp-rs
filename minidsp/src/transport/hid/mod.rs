//! HID transport for local USB devices
use std::{
    cell::Cell,
    collections::HashMap,
    panic::{self, AssertUnwindSafe},
    sync::{mpsc, Arc, Mutex, OnceLock},
    thread,
};

use anyhow::Result;

use futures::{SinkExt, TryStreamExt};
pub use hidapi::HidError;
use hidapi::{HidApi, HidDevice, HidResult};
use stream::HidStream;
use url2::Url2;

mod discover;
mod stream;
mod wrapper;
pub use discover::*;

use super::IntoTransport;

pub const VID_MINIDSP: u16 = 0x2752;
pub const OLD_MINIDSP_PID: (u16, u16) = (0x04d8, 0x003f);

static HIDAPI_INSTANCE: Mutex<Option<Arc<Mutex<HidApi>>>> = Mutex::new(None);

type HidJob = Box<dyn FnOnce() + Send>;

thread_local! {
    static ON_HID_THREAD: Cell<bool> = const { Cell::new(false) };
}

/// Runs `f` on the dedicated, long-lived hidapi thread and waits for its result.
///
/// hidapi's macOS backend schedules its global `IOHIDManager` on the run loop of whichever thread
/// first calls `hid_init`, and every later enumeration schedules the matched devices on that same
/// run loop. If that thread exits, its run loop is freed and the next enumeration crashes in
/// `CFRunLoopAddSource` (EXC_BREAKPOINT in `__CFCheckCFInfoPACSignature`). Under tokio this happens
/// whenever the initializing worker goes through `block_in_place` (as HID writes do) and is later
/// retired from the blocking pool. Funnelling every `HidApi` call through one thread that never
/// exits keeps that run loop alive, and matches hidapi's single-thread expectation on all platforms.
fn run_on_hid_thread<R, F>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    if ON_HID_THREAD.with(Cell::get) {
        return f();
    }

    static JOBS: OnceLock<mpsc::Sender<HidJob>> = OnceLock::new();
    let jobs = JOBS.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<HidJob>();
        thread::Builder::new()
            .name("hidapi".into())
            .spawn(move || {
                ON_HID_THREAD.with(|flag| flag.set(true));
                for job in rx {
                    // A panicking job must not take the thread (and its run loop) down with it.
                    let _ = panic::catch_unwind(AssertUnwindSafe(job));
                }
            })
            .expect("failed to spawn hidapi thread");
        tx
    });

    let (result_tx, result_rx) = mpsc::sync_channel(1);
    jobs.send(Box::new(move || {
        let _ = result_tx.send(f());
    }))
    .expect("hidapi thread is gone");
    result_rx.recv().expect("hidapi call panicked")
}

/// Initializes a global instance of HidApi, created on the dedicated hidapi thread.
///
/// Prefer [`with_api`], which also runs the call itself on that thread.
pub fn initialize_api() -> HidResult<Arc<Mutex<HidApi>>> {
    let mut instance = HIDAPI_INSTANCE.lock().unwrap();
    if let Some(api) = instance.as_ref() {
        return Ok(api.clone());
    }

    let api = Arc::new(Mutex::new(run_on_hid_thread(HidApi::new)?));
    instance.replace(api.clone());
    Ok(api)
}

/// Runs `f` with the global HidApi on the dedicated hidapi thread (see [`run_on_hid_thread`]).
pub fn with_api<R, F>(f: F) -> HidResult<R>
where
    F: FnOnce(&mut HidApi) -> R + Send + 'static,
    R: Send + 'static,
{
    let api = initialize_api()?;
    Ok(run_on_hid_thread(move || f(&mut api.lock().unwrap())))
}

pub struct HidTransport {
    stream: HidStream,
}

impl HidTransport {
    pub fn new(device: HidDevice) -> HidTransport {
        HidTransport {
            stream: HidStream::new(device),
        }
    }

    pub fn with_url(hid: &HidApi, url: &Url2) -> Result<HidTransport, HidError> {
        // If we have a path, decode it.
        let path = url.path();
        if !path.is_empty() {
            let path = urlencoding::decode(path).map_err(|_| HidError::HidApiErrorEmpty)?;
            return HidTransport::with_path(hid, path.to_string());
        }

        // If it's empty, try to get the vid and pid from the query string
        let query: HashMap<_, _> = url.query_pairs().collect();
        let vid = query.get("vid");
        let pid = query.get("pid");

        if let (Some(vid), Some(pid)) = (vid, pid) {
            let vid = u16::from_str_radix(vid, 16).map_err(|_| HidError::HidApiError {
                message: "couldn't parse vendor id".to_string(),
            })?;
            let pid = u16::from_str_radix(pid, 16).map_err(|_| HidError::HidApiError {
                message: "couldn't parse product id".to_string(),
            })?;
            return HidTransport::with_product_id(hid, vid, pid);
        }

        Err(HidError::HidApiError {
            message: "malformed url".to_string(),
        })
    }

    pub fn with_path(hid: &HidApi, path: String) -> Result<HidTransport, HidError> {
        let path = std::ffi::CString::new(path.into_bytes()).unwrap();
        Ok(HidTransport::new(hid.open_path(&path)?))
    }

    pub fn with_product_id(hid: &HidApi, vid: u16, pid: u16) -> Result<HidTransport, HidError> {
        let hid_device = hid.open(vid, pid)?;
        Ok(HidTransport::new(hid_device))
    }

    pub fn into_inner(self) -> HidStream {
        self.stream
    }
}

impl IntoTransport for HidTransport {
    fn into_transport(self) -> super::Transport {
        Box::pin(self.into_inner().sink_err_into().err_into())
    }
}
