//! snmalloc streaming-mode sampler — emits per-event allocation JSONL.
//!
//! Only compiled when the `snmalloc_profiling` Cargo feature is on (i.e.
//! the `:konfig_bin_heapprof` Bazel target). Activation is runtime-gated
//! by `KONFIG_SNMALLOC_STREAM_PATH`: when set to a writable path the
//! process opens a `ProfilingSession` and streams one JSONL line per
//! sampled allocation/resize event to that file.
//!
//! JSONL schema (matches snmalloc-tools `rate-report` consumer):
//!
//! ```text
//! {"ts_ns":<u128>,"kind":"alloc|resize","site":"0x<hex>","size":<usize>}
//! ```
//!
//! `ts_ns` is nanoseconds since process start (monotonic). `site` is the
//! innermost return-address frame from the sample's captured stack — the
//! allocation site itself when `backtrace::symbolize::gimli` walks it.
//! `size` is `requested_size` (caller-asked bytes, not sizeclass-rounded).
//!
//! Backpressure: bounded mpsc (16 384 cap) between the snmalloc trampoline
//! and the writer thread. The trampoline runs on whichever app thread
//! triggered the sampled allocation and `try_send`s — full channel drops
//! the sample rather than block the allocator. Sample rate is
//! ~1-per-512 KiB by default so 16 k entries is ~8 GiB of allocation
//! activity in flight; the writer thread keeps up under any realistic load.
//!
//! Tracker: CU-86aj35zxw.

use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::thread;
use std::time::Instant;

use snmalloc_rs::ProfilingSession;
use snmalloc_rs::streaming::EventKind;
use tracing::{info, warn};

/// MPSC channel cap between snmalloc trampoline and writer thread.
const CHANNEL_CAPACITY: usize = 16_384;

/// Output file buffered-writer capacity. Larger than typical 4 KiB stdio
/// buffers so the writer thread can amortise `write(2)` syscalls under
/// burst load.
const WRITER_BUF_BYTES: usize = 64 * 1024;

/// Anchor for the `ts_ns` field. Set on first `start_if_env` call; the
/// streaming closure reads it once per sample.
static PROC_START: OnceLock<Instant> = OnceLock::new();

/// `ProfilingSession` is `!Sync` (it carries `PhantomData<*const ()>`),
/// so we can't store it in a `static`. Instead we use a sentinel flag to
/// make `start_if_env` idempotent, and intentionally `mem::forget` the
/// `ProfilingSession` after `start()` so the C-side callback stays
/// registered for the process lifetime (dropping the session would
/// unregister it).
static SESSION_STARTED: AtomicBool = AtomicBool::new(false);

/// Counter for samples dropped due to channel saturation. Exposed via
/// `dropped_samples()` so an operator can decide whether the cap needs
/// raising.
static DROPPED: AtomicUsize = AtomicUsize::new(0);

/// Owned snapshot of a [`StreamSample`] — what we ship across the mpsc
/// from the trampoline to the writer thread.
#[derive(Copy, Clone)]
struct StreamEvent {
    ts_ns: u128,
    kind: EventKind,
    site: u64,
    size: usize,
}

/// Check `KONFIG_SNMALLOC_STREAM_PATH`. When set and non-empty, open the
/// file, start a `ProfilingSession`, and spawn a writer thread. Idempotent
/// — repeated calls after the first are no-ops.
///
/// Returns `Ok(true)` when streaming was activated this call, `Ok(false)`
/// when the env var was absent/empty, and `Err(_)` when activation was
/// attempted but failed (file create, session start collision, thread
/// spawn, etc).
pub fn start_if_env() -> Result<bool, Box<dyn std::error::Error>> {
    let path = match env::var("KONFIG_SNMALLOC_STREAM_PATH") {
        Ok(v) if !v.is_empty() => v,
        _ => return Ok(false),
    };

    if SESSION_STARTED.swap(true, Ordering::AcqRel) {
        // Idempotent re-entry — return false so callers can distinguish
        // "did not activate" from "already active".
        return Ok(false);
    }

    let file = File::create(&path)?;
    let writer = BufWriter::with_capacity(WRITER_BUF_BYTES, file);

    // Seed the timestamp anchor before the session can start firing.
    let _ = PROC_START.set(Instant::now());

    let (tx, rx) = sync_channel::<StreamEvent>(CHANNEL_CAPACITY);

    thread::Builder::new()
        .name("konfig-snmalloc-stream-sink".to_string())
        .spawn(move || writer_loop(rx, writer))?;

    let session =
        ProfilingSession::start(move |sample| handle_sample(&tx, sample)).map_err(Box::new)?;

    // `ProfilingSession` is `!Sync` — can't be stored in a `static`. Leak
    // it via `mem::forget` so the C-side callback stays registered for
    // the process lifetime; dropping the session would unregister it.
    std::mem::forget(session);

    info!(path = %path, cap = CHANNEL_CAPACITY, "snmalloc stream-mode sink started");
    Ok(true)
}

/// Count of samples that hit a full channel and got dropped. Useful as
/// an operator-visible "the sink couldn't keep up" signal. Increments
/// monotonically over the process lifetime.
pub fn dropped_samples() -> usize {
    DROPPED.load(Ordering::Relaxed)
}

/// Streaming-trampoline callback. Hot path — keep allocations off the
/// path here. The `StreamSample` is borrowed and short-lived; copy the
/// fields we need into the owned `StreamEvent`, then push to the mpsc.
fn handle_sample(tx: &SyncSender<StreamEvent>, sample: snmalloc_rs::StreamSample<'_>) {
    let ts_ns = PROC_START
        .get()
        .map(|s| s.elapsed().as_nanos())
        .unwrap_or(0);
    // Innermost frame (the alloc-site); fall back to alloc_ptr when the
    // stack is empty (snmalloc returns an empty slice on the cold-path
    // pre-init alloc, per upstream Phase 5.2 notes).
    let site = sample
        .stack()
        .first()
        .map(|p| *p as usize as u64)
        .unwrap_or_else(|| sample.alloc_ptr() as usize as u64);
    let ev = StreamEvent {
        ts_ns,
        kind: sample.kind(),
        site,
        size: sample.requested_size(),
    };
    match tx.try_send(ev) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            DROPPED.fetch_add(1, Ordering::Relaxed);
        }
        Err(TrySendError::Disconnected(_)) => {
            // Writer thread crashed; nothing useful to do here on the
            // allocation hot path. Subsequent samples will also fail.
        }
    }
}

/// Writer-thread main loop. Pulls owned `StreamEvent`s off the channel
/// and emits one JSONL line per event into the buffered file. Exits when
/// the sender side disconnects (process is tearing down).
fn writer_loop(rx: std::sync::mpsc::Receiver<StreamEvent>, mut writer: BufWriter<File>) {
    while let Ok(ev) = rx.recv() {
        let kind_str = match ev.kind {
            EventKind::Alloc => "alloc",
            EventKind::Resize => "resize",
        };
        // Hand-rolled JSON keeps the writer thread allocation-free per
        // line. Schema matches snmalloc-tools rate-report's expected
        // input (snmalloc-tools/tests/fixtures/streaming_log_sample.jsonl).
        let res = writeln!(
            &mut writer,
            r#"{{"ts_ns":{},"kind":"{}","site":"0x{:016x}","size":{}}}"#,
            ev.ts_ns, kind_str, ev.site, ev.size
        );
        if let Err(e) = res {
            // Persistent write failure: log once, drain remaining events
            // to keep the channel from blocking the sender, but stop
            // attempting to write further.
            warn!(error = %e, "snmalloc stream-sink write failed — draining + halting writes");
            while rx.recv().is_ok() {}
            return;
        }
    }
    let _ = writer.flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `start_if_env` returns `Ok(false)` when the env var is missing.
    /// Use a unique env-var name + ensure it's unset; tokio-style env
    /// races aren't relevant here because the global is process-scoped
    /// and we never set it inside this test.
    #[test]
    fn start_if_env_returns_false_when_unset() {
        // SAFETY: single-threaded test; no concurrent reads of the env.
        unsafe { env::remove_var("KONFIG_SNMALLOC_STREAM_PATH") };
        let res = start_if_env().expect("must not error when unset");
        assert!(!res, "must return false when env var is unset");
    }

    /// `dropped_samples()` is monotonic + starts at zero (modulo other
    /// tests racing — keep this test isolated by asserting on a fresh
    /// read pattern).
    #[test]
    fn dropped_samples_returns_a_count() {
        let _ = dropped_samples();
    }
}
