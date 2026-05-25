#!/usr/bin/env -S nix shell --quiet nixpkgs#uv nixpkgs#aria2 nixpkgs#curl nixpkgs#gnutar nixpkgs#restic nixpkgs#rclone nixpkgs#backblaze-b2 nixpkgs#backrest --command uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "beautifulsoup4>=4.12.0",
#   "google-api-python-client>=2.0.0",
#   "google-auth-oauthlib>=1.0.0",
#   "websocket-client>=1.8.0",
# ]
# ///
#
# Examples:
#   ./autotakeout.py
#   ./autotakeout.py --poll 300 --timeout 0
#   ./autotakeout.py --b2-bucket my-unique-bucket-name
#   ./autotakeout.py verify
#   ./autotakeout.py backrest
#   ./autotakeout.py --google-password 'your-google-password'
#
# Debug escape hatches:
#   ./autotakeout.py login --credentials ~/Downloads/client_secret_*.json --browser "$(command -v brave)"
#   ./autotakeout.py links --credentials ~/Downloads/client_secret_*.json --show
#   ./autotakeout.py download --browser "$(command -v brave)" --raw data/raw
#   ./autotakeout.py extract --raw data/raw --merged data/merged
from __future__ import annotations

import argparse
import base64
import getpass
import hashlib
import html
import json
import os
import queue
import re
import secrets
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from pathlib import Path
from urllib.parse import parse_qs, quote, urlsplit
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from websocket import WebSocketTimeoutException, create_connection

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
TAKEOUT_QUERY = (
    '("Google data is ready to download" OR "Your Google data is ready" '
    'OR "Your Google Takeout export is ready" OR "Takeout") newer_than:8d'
)
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125 Safari/537.36"
URL_RE = re.compile(r"https?://[^\s\"'<>]+")
STATE = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state")) / "autotakeout"
CONFIG = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "autotakeout" / "config.json"
SECRETS = STATE / "secrets.json"
PENDING_EXPORT = STATE / "pending-export.json"
BACKREST_DIR = STATE / "backrest"
BACKREST_CONFIG = BACKREST_DIR / "config.json"
BACKREST_DATA = BACKREST_DIR / "data"
BROWSER_DOWNLOAD_RETRIES = 5
PENDING_EXPORT_MAX_AGE_SECONDS = 7 * 24 * 60 * 60
RAW_EXPORT_MARKER = ".autotakeout-export.json"
RAW_DOWNLOADS_MARKER = ".autotakeout-downloads.json"
RESTIC_RESTORE_MARKER = ".autotakeout-restore-marker.json"
RESTIC_VALIDATION_MANIFEST = ".autotakeout-validation.json"
VALIDATION_METADATA_NAMES = {
    RAW_EXPORT_MARKER,
    RAW_DOWNLOADS_MARKER,
    RESTIC_RESTORE_MARKER,
    RESTIC_VALIDATION_MANIFEST,
}

CDP_PENDING: dict[int, list[dict]] = {}

TAKEOUT_CREATE_EXPORT_JS = r"""
(async () => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
  const visible = (el) => !!(el && (el.offsetWidth || el.offsetHeight || el.getClientRects().length));
  const textOf = (el) => norm([
    el.innerText,
    el.textContent,
    el.getAttribute && el.getAttribute('aria-label'),
    el.getAttribute && el.getAttribute('title'),
    el.value,
  ].filter(Boolean).join(' '));
  const clickable = (el) => el && (
    el.closest('button,a,[role="button"],[role="option"],[role="menuitem"],[role="checkbox"],[role="combobox"],mat-select') || el
  );
  const controls = () => Array.from(document.querySelectorAll(
    'button,a,[role="button"],[role="option"],[role="menuitem"],[role="checkbox"],[role="combobox"],mat-select,input[type="checkbox"]'
  )).filter(visible);
  const click = async (el) => {
    el = clickable(el);
    if (!el || !visible(el)) return false;
    el.scrollIntoView({block: 'center', inline: 'center'});
    await sleep(250);
    el.click();
    await sleep(1200);
    return true;
  };
  const clickControl = async (regex, label) => {
    for (const el of controls()) {
      if (regex.test(textOf(el))) {
        await click(el);
        return true;
      }
    }
    return false;
  };
  const waitFor = async (fn, label, ms = 45000) => {
    const deadline = Date.now() + ms;
    while (Date.now() < deadline) {
      const value = await fn();
      if (value) return value;
      await sleep(500);
    }
    throw new Error(`Timed out waiting for ${label}`);
  };
  const checked = (el) => {
    if (!el) return false;
    if (el.matches && el.matches('input[type="checkbox"]')) return !!el.checked;
    return el.getAttribute('aria-checked') === 'true' || el.getAttribute('data-checked') === 'true';
  };
  const setGooglePhotosChecked = async () => {
    const labels = Array.from(document.querySelectorAll('div,span,label'))
      .filter(visible)
      .filter((el) => /Google Photos/i.test(textOf(el)));
    for (const label of labels) {
      let parent = label;
      for (let i = 0; i < 9 && parent; i++, parent = parent.parentElement) {
        const box = Array.from(parent.querySelectorAll('[role="checkbox"],input[type="checkbox"]')).filter(visible)[0];
        if (box) {
          if (!checked(box)) await click(box);
          return true;
        }
      }
    }
    return false;
  };
  const choose = async (current, desired, label) => {
    if (await clickControl(desired, label)) return true;
    if (!(await clickControl(current, label))) return false;
    return await waitFor(() => clickControl(desired, label), label, 15000);
  };

  try {
    await waitFor(() => /takeout\.google\.com/.test(location.hostname), 'Takeout page');
    await sleep(3000);

    if (!(await clickControl(/Deselect all|Unselect all/i, 'Deselect all'))) {
      return {ok: false, step: 'deselect all', detail: 'Could not find the Deselect all control.'};
    }

    if (!(await waitFor(setGooglePhotosChecked, 'Google Photos checkbox'))) {
      return {ok: false, step: 'select Google Photos', detail: 'Could not find the Google Photos checkbox.'};
    }

    if (!(await waitFor(() => clickControl(/Next step/i, 'Next step'), 'Next step button'))) {
      return {ok: false, step: 'next step'};
    }

    await sleep(2500);
    if (!(await choose(/\.?zip\b/i, /\.?tgz\b/i, 'file type .tgz'))) {
      return {ok: false, step: 'file type', detail: 'Could not switch archive type to .tgz.'};
    }

    if (!(await choose(/\b2\s*GB\b/i, /\b50\s*GB\b/i, 'file size 50 GB'))) {
      return {ok: false, step: 'file size', detail: 'Could not switch archive size to 50 GB.'};
    }

    if (!(await waitFor(() => clickControl(/Create export/i, 'Create export'), 'Create export button'))) {
      return {ok: false, step: 'create export'};
    }

    await sleep(3000);
    return {ok: true, url: location.href, title: document.title};
  } catch (e) {
    return {
      ok: false,
      step: 'exception',
      detail: String(e && e.message || e),
      url: location.href,
      title: document.title,
    };
  }
})()
"""

TAKEOUT_FIND_DOWNLOAD_LINKS_JS = r"""
(async () => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
  const collect = () => {
    const urls = [];
    for (const a of Array.from(document.querySelectorAll('a[href]'))) {
      const href = a.href || '';
      const text = norm(a.innerText || a.textContent || a.getAttribute('aria-label') || '');
      const low = href.toLowerCase();
      if (low.includes('takeout.google.com') && low.includes('download')) urls.push(href);
      if (low.includes('takeout.google.com') && /download/i.test(text)) urls.push(href);
    }
    return Array.from(new Set(urls));
  };
  const deadline = Date.now() + 60000;
  while (Date.now() < deadline) {
    const urls = collect();
    if (urls.length) return {ok: true, urls, url: location.href, title: document.title};
    await sleep(1000);
  }
  return {
    ok: false,
    urls: [],
    url: location.href,
    title: document.title,
    detail: norm(document.body && document.body.innerText || '').slice(0, 1000),
  };
})()
"""

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def same(a: Path, b: Path) -> bool:
    return a.stat().st_size == b.stat().st_size and sha256(a) == sha256(b)

def collision_name(dst: Path, src: Path) -> Path:
    h = sha256(src)[:12]
    candidate = dst.with_name(f"{dst.stem}.collision-{h}{dst.suffix}")
    n = 2
    while candidate.exists():
        if same(candidate, src):
            return candidate
        candidate = dst.with_name(f"{dst.stem}.collision-{h}-{n}{dst.suffix}")
        n += 1
    return candidate

def is_takeout_report_archive(archive: Path) -> bool:
    try:
        with tarfile.open(archive, "r:gz") as tar:
            first = tar.next()
            if first is None or first.name != "Takeout/archive_browser.html":
                return False
            return tar.next() is None
    except tarfile.TarError:
        return False

def extract_archive(archive: Path, merged: Path) -> dict:
    manifest = merged / "autotakeout-merge-manifest.jsonl"
    stats = {"archives": 0, "reports": 0, "moved": 0, "duplicates": 0, "collisions": 0}
    with manifest.open("a") as log:
        if is_takeout_report_archive(archive):
            print(f"keeping report archive without merging: {archive}")
            log.write(json.dumps({"report": str(archive)}) + "\n")
            stats["reports"] = 1
            return stats

        temp = merged / ".extracting"
        work = temp / archive.name
        shutil.rmtree(work, ignore_errors=True)
        work.mkdir(parents=True)
        print(f"extracting {archive}")
        try:
            subprocess.run(["tar", "-xzf", str(archive), "-C", str(work)], check=True)
            for src in sorted(p for p in work.rglob("*") if p.is_file()):
                rel = src.relative_to(work)
                dst = merged / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                if not dst.exists():
                    shutil.move(str(src), str(dst))
                    stats["moved"] += 1
                elif same(src, dst):
                    src.unlink()
                    stats["duplicates"] += 1
                else:
                    other = collision_name(dst, src)
                    if other.exists() and same(src, other):
                        src.unlink()
                        stats["duplicates"] += 1
                    else:
                        shutil.move(str(src), str(other))
                        log.write(json.dumps({"collision": str(rel), "wrote": str(other.relative_to(merged))}) + "\n")
                        stats["collisions"] += 1
        finally:
            shutil.rmtree(work, ignore_errors=True)
    stats["archives"] = 1
    print(
        f"extracted {archive.name}; moved={stats['moved']} "
        f"duplicates={stats['duplicates']} collisions={stats['collisions']}"
    )
    return stats

def extraction_summary(results: list[dict]) -> str:
    archives = sum(item.get("archives", 0) for item in results)
    reports = sum(item.get("reports", 0) for item in results)
    moved = sum(item.get("moved", 0) for item in results)
    duplicates = sum(item.get("duplicates", 0) for item in results)
    collisions = sum(item.get("collisions", 0) for item in results)
    return (
        f"extracted {archives} archives; "
        f"reports={reports} moved={moved} duplicates={duplicates} collisions={collisions}"
    )

def archive_integrity_ok(path: Path) -> bool:
    result = subprocess.run(
        ["tar", "-tzf", str(path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0

def load_completed_download_marker(raw: Path) -> dict:
    path = raw / RAW_DOWNLOADS_MARKER
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {"version": 1, "files": {}}

def load_completed_downloads(raw: Path) -> dict:
    return load_completed_download_marker(raw).get("files", {})

def write_private_json_if_changed(path: Path, data: dict) -> None:
    content = json.dumps(data, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == content:
        path.chmod(0o600)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    path.chmod(0o600)

def first_existing_path(paths: list[Path], *, default: Path) -> Path:
    for path in paths:
        if path.exists():
            return path
    return default

def record_completed_download(raw: Path, filename: str, params: dict) -> None:
    if not filename:
        return
    path = raw / filename
    size = int(params.get("totalBytes") or 0)
    if path.exists():
        size = path.stat().st_size
    if size <= 0:
        return

    data = load_completed_download_marker(raw)
    files = data.setdefault("files", {})
    files[filename] = {
        "size": size,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    write_private_json_if_changed(raw / RAW_DOWNLOADS_MARKER, data)

def existing_download_status(raw: Path, filename: str) -> str:
    if not filename:
        return "missing"
    path = raw / filename
    if not path.exists() or not path.is_file():
        return "missing"
    if path.name.endswith(".crdownload"):
        return "invalid"
    size = path.stat().st_size
    if size <= 0:
        return "invalid"

    recorded = load_completed_downloads(raw).get(filename)
    if recorded:
        return "complete" if int(recorded.get("size") or -1) == size else "invalid"

    if archive_integrity_ok(path):
        record_completed_download(raw, filename, {"totalBytes": size})
        return "complete"
    return "invalid"

def completed_archive_files(raw: Path) -> list[Path]:
    if not raw.exists():
        return []
    return [
        path
        for path in sorted(raw.iterdir())
        if path.is_file()
        and path.name.endswith((".tgz", ".tar.gz"))
        and existing_download_status(raw, path.name) == "complete"
    ]

def notify_completed_archives(raw: Path, on_complete) -> None:
    if not on_complete:
        return
    for archive in completed_archive_files(raw):
        on_complete(archive)

class ExtractionPipeline:
    def __init__(self, raw: Path, merged: Path) -> None:
        self.raw = raw
        self.merged = merged
        self.queue = queue.Queue()
        self.lock = threading.Lock()
        self.enqueued: set[str] = set()
        self.errors: list[BaseException] = []
        self.results: list[dict] = []
        self.closed = False
        self.thread = threading.Thread(target=self._run, name="autotakeout-extractor", daemon=True)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close(wait=exc_type is None)
        return False

    def start(self) -> None:
        self.merged.mkdir(parents=True, exist_ok=True)
        self.thread.start()
        notify_completed_archives(self.raw, self.enqueue)

    def enqueue(self, archive: Path) -> None:
        archive = Path(archive)
        if not archive.name.endswith((".tgz", ".tar.gz")) or not archive.exists():
            return
        key = str(archive.resolve())
        with self.lock:
            if key in self.enqueued:
                return
            self.enqueued.add(key)
        self.queue.put(archive)

    def wait(self) -> None:
        self.queue.join()
        self.raise_errors()
        self.print_summary()

    def close(self, *, wait: bool = True) -> None:
        if self.closed:
            return
        self.closed = True
        error = None
        if wait:
            try:
                self.wait()
            except BaseException as e:
                error = e
        self.queue.put(None)
        if wait:
            self.thread.join()
            shutil.rmtree(self.merged / ".extracting", ignore_errors=True)
        if error:
            raise error

    def _run(self) -> None:
        while True:
            archive = self.queue.get()
            try:
                if archive is None:
                    return
                result = extract_archive(archive, self.merged)
                with self.lock:
                    self.results.append(result)
            except BaseException as e:
                with self.lock:
                    self.errors.append(e)
            finally:
                self.queue.task_done()

    def raise_errors(self) -> None:
        with self.lock:
            errors = list(self.errors)
        if errors:
            raise RuntimeError(f"Background extraction failed: {errors[0]}") from errors[0]

    def print_summary(self) -> None:
        with self.lock:
            results = list(self.results)
        if not results:
            print("no completed archives available to extract yet")
            return
        print(extraction_summary(results))

def clear_browser_restore_state(profile: Path) -> None:
    for sessions_dir in profile.glob("*/Sessions"):
        if not sessions_dir.is_dir():
            continue
        for path in sessions_dir.iterdir():
            try:
                if path.is_file():
                    path.unlink()
            except FileNotFoundError:
                pass

def stop_existing_browser_profile(profile: Path) -> None:
    marker = f"--user-data-dir={profile}".lower()
    pids: list[int] = []
    for proc_dir in Path("/proc").iterdir():
        if not proc_dir.name.isdecimal():
            continue
        pid = int(proc_dir.name)
        if pid == os.getpid():
            continue
        try:
            args = (proc_dir / "cmdline").read_bytes().decode("utf-8", "ignore").split("\0")
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        command = " ".join(args).lower()
        if marker not in command:
            continue
        if not any(name in command for name in ("brave", "chrome", "chromium")):
            continue
        try:
            os.kill(pid, signal.SIGTERM)
            pids.append(pid)
        except ProcessLookupError:
            pass

    if not pids:
        return

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if all(not Path(f"/proc/{pid}").exists() for pid in pids):
            return
        time.sleep(0.1)

    for pid in pids:
        if not Path(f"/proc/{pid}").exists():
            continue
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

def launch_browser(
    profile: Path,
    browser: Path | None,
    url: str,
    *,
    remote_debugging_port: int | None = None,
) -> subprocess.Popen:
    if browser is None:
        raise SystemExit("Could not find Brave, Chrome, or Chromium. Pass --browser /path/to/browser.")

    if remote_debugging_port is not None:
        stop_existing_browser_profile(profile)
        clear_browser_restore_state(profile)

    profile.mkdir(parents=True, exist_ok=True)
    command = [
        str(browser),
        f"--user-data-dir={profile}",
        "--no-first-run",
        "--no-default-browser-check",
    ]
    if remote_debugging_port is not None:
        command.extend(
            [
                "--remote-debugging-address=127.0.0.1",
                f"--remote-debugging-port={remote_debugging_port}",
                "--remote-allow-origins=*",
            ]
        )
    command.append(url)
    return subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)

def stop_browser(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()

def browser_login(profile: Path, browser: Path | None) -> None:
    profile.mkdir(parents=True, exist_ok=True)
    proc = launch_browser(profile, browser, "https://takeout.google.com/?pli=1")
    input("Log into Google in the normal browser window, then close it and press Enter here. ")
    stop_browser(proc)

class BrowserDownloadCanceled(RuntimeError):
    pass

def cdp_pending(ws) -> list[dict]:
    return CDP_PENDING.setdefault(id(ws), [])

def cdp_call(ws, method: str, params: dict | None = None) -> dict:
    cdp_call.counter += 1
    call_id = cdp_call.counter
    ws.send(json.dumps({"id": call_id, "method": method, "params": params or {}}))
    while True:
        message = json.loads(ws.recv())
        if message.get("id") != call_id:
            if "method" in message:
                cdp_pending(ws).append(message)
            continue
        if "error" in message:
            raise RuntimeError(f"CDP {method} failed: {message['error']}")
        return message.get("result", {})
cdp_call.counter = 0

def enable_browser_downloads(ws, raw: Path) -> None:
    params = {"behavior": "allow", "downloadPath": str(raw.resolve()), "eventsEnabled": True}
    try:
        cdp_call(ws, "Browser.setDownloadBehavior", params)
    except RuntimeError:
        cdp_call(ws, "Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": str(raw.resolve())})

def devtools_json(port: int, path: str):
    with urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))

def devtools_page_target(port: int) -> dict:
    targets = devtools_json(port, "/json/list")
    pages = [target for target in targets if target.get("type") == "page" and target.get("webSocketDebuggerUrl")]
    if pages:
        return pages[0]

    new_url = f"/json/new?{quote('https://takeout.google.com/settings/takeout', safe='')}"
    request = Request(f"http://127.0.0.1:{port}{new_url}", method="PUT")
    with urlopen(request, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))

def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])

def wait_for_devtools(port: int, timeout: int = 120) -> None:
    deadline = time.monotonic() + timeout
    url = f"http://127.0.0.1:{port}/json/version"
    while time.monotonic() < deadline:
        try:
            with urlopen(url, timeout=1) as response:
                json.loads(response.read().decode("utf-8"))
            return
        except Exception:
            time.sleep(0.25)
    raise RuntimeError("Timed out waiting for browser DevTools endpoint")

def open_browser_with_devtools(profile: Path, browser: Path | None, url: str):
    port = free_port()
    proc = launch_browser(profile, browser, url, remote_debugging_port=port)
    try:
        wait_for_devtools(port)
        target = devtools_page_target(port)
        ws = create_connection(target["webSocketDebuggerUrl"], timeout=30)
        cdp_call(ws, "Page.enable")
        cdp_call(ws, "Runtime.enable")
        cdp_call(ws, "Page.bringToFront")
        return proc, ws
    except Exception:
        stop_browser(proc)
        raise

def cdp_eval(ws, expression: str, *, await_promise: bool = False, timeout: int = 90):
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = cdp_call(
                ws,
                "Runtime.evaluate",
                {
                    "expression": expression,
                    "awaitPromise": await_promise,
                    "returnByValue": True,
                    "userGesture": True,
                },
            )
            if "exceptionDetails" in result:
                raise RuntimeError(result["exceptionDetails"])
            remote = result.get("result", {})
            if "value" in remote:
                return remote["value"]
            return remote.get("description")
        except RuntimeError as e:
            last_error = e
            message = str(e)
            transient = (
                "Cannot find default execution context" in message
                or "Execution context was destroyed" in message
                or "Inspected target navigated or closed" in message
            )
            if not transient:
                raise
            time.sleep(0.5)
    raise RuntimeError(f"Timed out waiting for page execution context: {last_error}")

def browser_page_summary(ws) -> dict:
    detail = cdp_eval(
        ws,
        "({url: location.href, title: document.title, text: (document.body && document.body.innerText || '').slice(0, 500)})",
        timeout=5,
    )
    return detail if isinstance(detail, dict) else {"detail": detail}

def cdp_next_event(ws) -> dict:
    pending = cdp_pending(ws)
    if pending:
        return pending.pop(0)
    while True:
        message = json.loads(ws.recv())
        if "method" in message:
            return message

def finish_progress_line(previous_width: int) -> None:
    if previous_width:
        sys.stdout.write("\r" + " " * previous_width + "\r")
        sys.stdout.flush()

def google_password_prompt_visible(ws) -> bool:
    expression = """
(() => {
  const visible = (el) => !!(el && (el.offsetWidth || el.offsetHeight || el.getClientRects().length));
  return Array.from(document.querySelectorAll('input[type="password"]')).some(visible);
})()
"""
    try:
        return bool(cdp_eval(ws, expression, timeout=5))
    except Exception:
        return False

def print_download_progress(name: str, params: dict, previous_width: int, *, final: bool = False) -> int:
    received = int(params.get("receivedBytes") or 0)
    total_bytes = int(params.get("totalBytes") or 0)
    if final and total_bytes:
        received = total_bytes
    if total_bytes:
        pct = min(100.0, received * 100 / total_bytes)
        text = f"{name}: {received / 1024**3:.2f}/{total_bytes / 1024**3:.2f} GiB ({pct:.1f}%)"
    else:
        text = f"{name}: {received / 1024**3:.2f} GiB"
    width = max(previous_width, len(text))
    sys.stdout.write("\r" + text.ljust(width))
    if final:
        sys.stdout.write("\n")
        width = 0
    sys.stdout.flush()
    return width

def remove_incomplete_download(raw: Path, filename: str) -> None:
    for path in (raw / filename, raw / f"{filename}.crdownload"):
        path.unlink(missing_ok=True)

def submit_google_password(ws, password: str) -> bool:
    expression = f"""
(() => {{
  const password = {json.dumps(password)};
  const visible = (el) => !!(el && (el.offsetWidth || el.offsetHeight || el.getClientRects().length));
  const input = Array.from(document.querySelectorAll('input[type="password"]')).find(visible);
  if (!input) return false;
  input.focus();
  input.value = password;
  input.dispatchEvent(new Event('input', {{bubbles: true}}));
  input.dispatchEvent(new Event('change', {{bubbles: true}}));
  const buttons = Array.from(document.querySelectorAll('button, [role="button"], input[type="submit"]')).filter(visible);
  const next = buttons.find((el) => /next/i.test(el.innerText || el.textContent || el.value || el.getAttribute('aria-label') || '')) || buttons[0];
  if (next) {{
    next.click();
  }} else {{
    input.form && input.form.submit();
  }}
  return true;
}})()
"""
    try:
        return bool(cdp_eval(ws, expression, timeout=5))
    except Exception:
        return False

def wait_for_browser_download(
    ws,
    raw: Path,
    index: int,
    total: int,
    url: str,
    google_password: str | None,
) -> str | None:
    start_deadline = time.monotonic() + 120
    active_guid = ""
    filename = ""
    last_report = 0.0
    last_progress_width = 0
    waiting_for_auth = False
    password_submitted = False
    password_prompted = False
    while True:
        try:
            event = cdp_next_event(ws)
        except WebSocketTimeoutException:
            if active_guid:
                continue
            detail = browser_page_summary(ws)
            current_url = str(detail.get("url", ""))
            if "accounts.google.com/" in current_url:
                if google_password and not password_submitted and submit_google_password(ws, google_password):
                    print("Submitted Google password challenge.")
                    password_submitted = True
                    waiting_for_auth = True
                    continue
                if not google_password and not password_prompted and google_password_prompt_visible(ws):
                    password_prompted = True
                    google_password = getpass.getpass("Google password for Takeout reauth (not stored): ")
                    if google_password and submit_google_password(ws, google_password):
                        print("Submitted Google password challenge.")
                        password_submitted = True
                        waiting_for_auth = True
                        continue
                if not waiting_for_auth:
                    print("Google is asking for interactive verification before downloading.")
                    print("Finish the Google verification in the browser; the script will continue automatically.")
                    waiting_for_auth = True
                continue
            if waiting_for_auth:
                print("Google verification finished; retrying the archive download.")
                cdp_pending(ws).clear()
                waiting_for_auth = False
                start_deadline = time.monotonic() + 120
                cdp_call(ws, "Page.navigate", {"url": url})
                continue
            if time.monotonic() > start_deadline:
                raise RuntimeError(f"No browser download started for item {index}/{total}: {detail}")
            continue

        method = event.get("method")
        params = event.get("params", {})
        if method == "Browser.downloadWillBegin":
            active_guid = params.get("guid", "")
            filename = params.get("suggestedFilename", "") or active_guid
            if filename:
                status = existing_download_status(raw, filename)
                if status == "complete":
                    if active_guid:
                        try:
                            cdp_call(ws, "Browser.cancelDownload", {"guid": active_guid})
                        except RuntimeError:
                            pass
                    print(f"already downloaded {filename}")
                    return filename
                if status == "invalid":
                    if active_guid:
                        try:
                            cdp_call(ws, "Browser.cancelDownload", {"guid": active_guid})
                        except RuntimeError:
                            pass
                    remove_incomplete_download(raw, filename)
                    raise BrowserDownloadCanceled(f"Removed incomplete existing download {filename}")
            print(f"downloading {filename}")
            continue
        if method != "Browser.downloadProgress":
            continue

        guid = params.get("guid", "")
        if active_guid and guid != active_guid:
            continue
        if not active_guid:
            active_guid = guid
        state = params.get("state")
        if state == "completed":
            last_progress_width = print_download_progress(filename or active_guid, params, last_progress_width, final=True)
            record_completed_download(raw, filename or active_guid, params)
            print(f"downloaded {filename or active_guid}")
            return filename or active_guid
        if state == "canceled":
            if filename and existing_download_status(raw, filename) == "complete":
                finish_progress_line(last_progress_width)
                print(f"downloaded {filename} before browser reported cancellation")
                return filename
            finish_progress_line(last_progress_width)
            raise BrowserDownloadCanceled(f"Browser canceled download {filename or active_guid}")

        now = time.monotonic()
        if now - last_report >= 30:
            last_report = now
            last_progress_width = print_download_progress(filename or active_guid, params, last_progress_width)

def browser_download(
    links: list[str],
    raw: Path,
    profile: Path,
    browser: Path | None,
    google_password: str | None,
    *,
    on_complete=None,
) -> None:
    if browser is None:
        raise SystemExit("A Chrome/Brave/Chromium executable is required for browser downloads")

    raw.mkdir(parents=True, exist_ok=True)
    proc, ws = open_browser_with_devtools(profile, browser, "about:blank")
    try:
        ws.settimeout(5)
        enable_browser_downloads(ws, raw)
        for index, url in enumerate(links, 1):
            for attempt in range(1, BROWSER_DOWNLOAD_RETRIES + 1):
                cdp_pending(ws).clear()
                suffix = f" attempt {attempt}/{BROWSER_DOWNLOAD_RETRIES}" if attempt > 1 else ""
                print(f"browser download {index}/{len(links)}{suffix}")
                cdp_call(ws, "Page.navigate", {"url": url})
                try:
                    filename = wait_for_browser_download(ws, raw, index, len(links), url, google_password)
                    if filename and on_complete:
                        on_complete(raw / filename)
                    break
                except BrowserDownloadCanceled as e:
                    if attempt == BROWSER_DOWNLOAD_RETRIES:
                        raise RuntimeError(str(e)) from e
                    print(f"{e}; retrying")
                    time.sleep(min(30, attempt * 5))
    finally:
        ws.close()
        stop_browser(proc)

def choose_downloader(which: str, browser: Path | None) -> str:
    if which != "auto":
        return which
    if browser is not None:
        return "browser"
    for tool in ("aria2c", "curl"):
        if shutil.which(tool):
            return tool
    raise SystemExit("Install aria2c or curl")

def read_cookies_from_devtools(port: int) -> list[dict]:
    target = devtools_page_target(port)
    ws = create_connection(target["webSocketDebuggerUrl"], timeout=15)
    try:
        cdp_call(ws, "Network.enable")
        result = cdp_call(ws, "Network.getAllCookies")
    finally:
        ws.close()
    return result.get("cookies", [])

def export_cookies(profile: Path, browser: Path | None) -> Path:
    if browser is None:
        raise SystemExit("A Chrome/Brave/Chromium executable is required to export logged-in cookies")

    cookie_file = STATE / "cookies.txt"
    port = free_port()
    proc = launch_browser(
        profile,
        browser,
        "https://takeout.google.com/settings/takeout",
        remote_debugging_port=port,
    )
    try:
        wait_for_devtools(port)
        cookies = read_cookies_from_devtools(port)
    finally:
        stop_browser(proc)

    lines = ["# Netscape HTTP Cookie File", ""]
    for c in cookies:
        domain = c.get("domain", "")
        if not domain:
            continue
        lines.append(
            "\t".join(
                [
                    f"#HttpOnly_{domain}" if c.get("httpOnly") else domain,
                    "TRUE" if domain.startswith(".") else "FALSE",
                    c.get("path", "/"),
                    "TRUE" if c.get("secure") else "FALSE",
                    str(max(0, int(c.get("expires") or 0))),
                    str(c.get("name", "")).replace("\t", " "),
                    str(c.get("value", "")).replace("\t", " "),
                ]
            )
        )
    cookie_file.write_text("\n".join(lines) + "\n")
    cookie_file.chmod(0o600)
    print(f"exported {len(cookies)} cookies to {cookie_file}")
    return cookie_file

def download(
    links: list[str],
    raw: Path,
    profile: Path,
    browser: Path | None,
    which: str,
    google_password: str | None = None,
    on_complete=None,
) -> None:
    raw.mkdir(parents=True, exist_ok=True)
    tool = choose_downloader(which, browser)
    if tool == "browser":
        browser_download(links, raw, profile, browser, google_password, on_complete=on_complete)
        return

    cookie_file = export_cookies(profile, browser)
    if tool == "aria2c":
        fd, name = tempfile.mkstemp(prefix="autotakeout-links-", suffix=".txt")
        os.close(fd)
        list_file = Path(name)
        try:
            list_file.chmod(0o600)
            list_file.write_text("".join(f"{u}\n  dir={raw}\n" for u in links))
            subprocess.run(
                [
                    "aria2c",
                    "--load-cookies", str(cookie_file),
                    "--continue=true",
                    "--content-disposition=true",
                    "--auto-file-renaming=false",
                    "--max-connection-per-server=1",
                    "--split=1",
                    "--file-allocation=none",
                    "--max-tries=10",
                    "--retry-wait=30",
                    "--user-agent", UA,
                    "--input-file", str(list_file),
                ],
                check=True,
            )
            notify_completed_archives(raw, on_complete)
        finally:
            list_file.unlink(missing_ok=True)
    else:
        for u in links:
            subprocess.run(
                [
                    "curl",
                    "--fail", "--location", "--continue-at", "-", "--remote-name",
                    "--remote-header-name", "--cookie", str(cookie_file), "--user-agent", UA, u,
                ],
                cwd=raw,
                check=True,
            )
            notify_completed_archives(raw, on_complete)

def extract_all(raw: Path, merged: Path) -> None:
    archives = sorted([p for p in raw.iterdir() if p.name.endswith((".tgz", ".tar.gz"))])
    if not archives:
        raise SystemExit(f"No .tgz archives found in {raw}")
    merged.mkdir(parents=True, exist_ok=True)
    results = [extract_archive(archive, merged) for archive in archives]
    shutil.rmtree(merged / ".extracting", ignore_errors=True)
    print(extraction_summary(results))

def decode(data: str) -> str:
    return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4)).decode("utf-8", "replace")

def walk(part: dict):
    yield part
    for child in part.get("parts", []) or []:
        yield from walk(child)

def extract_links(payload: dict) -> list[dict]:
    bodies = []
    for part in walk(payload):
        data = part.get("body", {}).get("data")
        if data:
            bodies.append((part.get("mimeType"), decode(data)))

    candidates = []
    for mime, body in bodies:
        if mime == "text/html":
            soup = BeautifulSoup(body, "html.parser")
            for a in soup.find_all("a"):
                href = html.unescape(a.get("href") or "")
                text = a.get_text(" ", strip=True)
                if href:
                    candidates.append((href, text))
        else:
            candidates.extend((html.unescape(m.group(0)), "") for m in URL_RE.finditer(body))

    out, seen = [], set()
    for url, text in candidates:
        url = url.rstrip(").,;]")
        low, t = url.lower(), text.lower()
        keep = (
            "takeout.google.com" in low
            and ("download" in low or "/settings/takeout" in low or "/manage/archive" in low)
        ) or (
            "accounts.google.com" in low
            and "takeout.google.com" in low
            and any(w in t for w in ("download", "archive", "takeout", "manage"))
        ) or (
            "notifications.google.com" in low and any(w in t for w in ("download", "archive", "files"))
        )
        if keep and url not in seen:
            seen.add(url)
            out.append({"url": url, "text": text})
    return out

def headers(payload: dict) -> dict:
    return {h.get("name", "").lower(): h.get("value", "") for h in payload.get("headers", [])}

def find_takeout_links_from_service(service, query: str, max_emails: int) -> dict | None:
    res = service.users().messages().list(userId="me", q=query, maxResults=max_emails).execute()
    messages = res.get("messages", [])
    if not messages:
        return None

    best = None
    for msg in messages:
        full = service.users().messages().get(userId="me", id=msg["id"], format="full").execute()
        links = extract_links(full.get("payload", {}))
        if links and (best is None or int(full["internalDate"]) > int(best["internalDate"])):
            best = {
                "id": full["id"],
                "internalDate": full["internalDate"],
                "headers": headers(full.get("payload", {})),
                "links": links,
            }
    return best

def gmail_service(credentials: Path, token: Path):
    creds = Credentials.from_authorized_user_file(token, SCOPES) if token.exists() else None
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(GoogleAuthRequest())
        else:
            creds = InstalledAppFlow.from_client_secrets_file(str(credentials), SCOPES).run_local_server(port=0)
        token.parent.mkdir(parents=True, exist_ok=True)
        token.write_text(creds.to_json())
        token.chmod(0o600)
    return build("gmail", "v1", credentials=creds)

def find_takeout_links(credentials: Path, token: Path, query: str, max_emails: int) -> dict:
    service = gmail_service(credentials, token)
    found = find_takeout_links_from_service(service, query, max_emails)
    if found:
        return found
    raise SystemExit("No Takeout email with download links was found")

def chromium_cookie_dbs(profile: Path) -> list[Path]:
    paths = [
        profile / "Default" / "Network" / "Cookies",
        profile / "Default" / "Cookies",
    ]
    paths.extend(profile.glob("*/Network/Cookies"))
    paths.extend(profile.glob("*/Cookies"))
    return sorted({path for path in paths if path.exists() and path.is_file()})

def profile_has_google_login_cookie(profile: Path) -> bool:
    for db in chromium_cookie_dbs(profile):
        fd, name = tempfile.mkstemp(prefix="autotakeout-cookies-", suffix=".sqlite")
        os.close(fd)
        tmp = Path(name)
        try:
            shutil.copy2(db, tmp)
            with sqlite3.connect(tmp) as conn:
                rows = conn.execute(
                    """
                    select name from cookies
                    where host_key like '%google.com'
                      and name in ('SID', '__Secure-1PSID', '__Secure-3PSID')
                    """
                ).fetchall()
            if rows:
                return True
        except Exception:
            continue
        finally:
            tmp.unlink(missing_ok=True)
    return False

def browser_profile_ready(profile: Path, browser: Path | None) -> bool:
    return profile_has_google_login_cookie(profile)

def clear_pending_takeout_export() -> None:
    PENDING_EXPORT.unlink(missing_ok=True)

def open_takeout(profile: Path, browser: Path | None) -> None:
    proc = launch_browser(profile, browser, "https://takeout.google.com/?pli=1")
    input("Create the Google Photos Takeout export in the normal browser window, then close it and press Enter here. ")
    stop_browser(proc)

def create_takeout_export(profile: Path, browser: Path | None) -> None:
    print("Automating Takeout: deselect all, select Google Photos, choose .tgz and 50 GB, create export.")
    try:
        proc, ws = open_browser_with_devtools(profile, browser, "https://takeout.google.com/?pli=1")
        try:
            ws.settimeout(180)
            result = cdp_eval(ws, TAKEOUT_CREATE_EXPORT_JS, await_promise=True)
        finally:
            ws.close()
            stop_browser(proc)
    except Exception as e:
        print(f"Takeout automation failed: {e}")
        print("Opening Takeout for manual fallback.")
        open_takeout(profile, browser)
        return

    if not result.get("ok"):
        print(f"Takeout automation stopped at: {result.get('step', 'unknown step')}")
        if result.get("detail"):
            print(result["detail"])
        print("Opening Takeout for manual fallback.")
        open_takeout(profile, browser)
        return

    print("Takeout export request created.")

def pending_takeout_export_active() -> bool:
    try:
        data = json.loads(PENDING_EXPORT.read_text(encoding="utf-8"))
        requested_at = float(data.get("requested_at_unix") or 0)
    except (FileNotFoundError, json.JSONDecodeError, OSError, TypeError, ValueError):
        return False

    age = time.time() - requested_at
    if age <= PENDING_EXPORT_MAX_AGE_SECONDS:
        return True

    PENDING_EXPORT.unlink(missing_ok=True)
    return False

def existing_takeout_downloads(raw: Path) -> list[Path]:
    if not raw.exists():
        return []
    return sorted(
        path
        for path in raw.iterdir()
        if path.is_file()
        and path.name.startswith("takeout-")
        and path.name.endswith((".tgz", ".tar.gz", ".tgz.crdownload", ".tar.gz.crdownload"))
    )

def prompt_yes_no(prompt: str, *, default: bool = False) -> bool:
    if not sys.stdin.isatty():
        return default
    suffix = " [Y/n] " if default else " [y/N] "
    while True:
        answer = input(prompt + suffix).strip().lower()
        if not answer:
            return default
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Please answer yes or no.")

def read_raw_export_id(marker: Path) -> str | None:
    try:
        return json.loads(marker.read_text()).get("export_id")
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None

def takeout_export_id_from_url(url: str) -> str | None:
    parsed = urlsplit(url)
    query = parse_qs(parsed.query)
    if value := query.get("j", [None])[0]:
        return value
    if match := re.search(r"/manage/archive/([^/?#]+)", parsed.path):
        return match.group(1)
    for key in ("continue", "followup"):
        for nested in query.get(key, []):
            if value := takeout_export_id_from_url(nested):
                return value
    return None

def takeout_export_id(urls: list[str]) -> str | None:
    for url in urls:
        export_id = takeout_export_id_from_url(url)
        if export_id:
            return export_id
    return None

def write_raw_export_marker(marker: Path, export_id: str, archive_links: list[str]) -> None:
    write_private_json_if_changed(
        marker,
        {
            "export_id": export_id,
            "link_count": len(archive_links),
            "version": 1,
        },
    )

def prepare_raw_directory(raw: Path, archive_links: list[str], *, force: bool = False) -> None:
    export_id = takeout_export_id(archive_links)
    if not export_id:
        return

    raw.mkdir(parents=True, exist_ok=True)
    marker = raw / RAW_EXPORT_MARKER
    previous_id = read_raw_export_id(marker)
    existing = existing_takeout_downloads(raw)

    if existing and previous_id != export_id:
        label = previous_id or "unknown export"
        print(f"{raw} contains {len(existing)} existing Takeout download file(s) from {label}.")
        for path in existing[:10]:
            print(f"  {path.name}")
        if len(existing) > 10:
            print(f"  ... {len(existing) - 10} more")
        if force:
            delete_old = True
            print("--force set; deleting old Takeout files without prompting")
        else:
            delete_old = prompt_yes_no("Delete those old Takeout files before downloading this export?", default=False)
        if delete_old:
            for path in existing:
                path.unlink(missing_ok=True)
            print(f"deleted {len(existing)} old Takeout file(s)")
        else:
            print("leaving existing Takeout files in place")

    write_raw_export_marker(marker, export_id, archive_links)

def redact(url: str) -> str:
    p = urlsplit(url)
    return f"{p.scheme}://{p.netloc}{p.path}?..."

def print_links(found: dict, show: bool) -> None:
    h = found["headers"]
    print(f"matched: {h.get('subject', '')}")
    print(f"from: {h.get('from', '')}")
    print(f"date: {h.get('date', '')}")
    for i, item in enumerate(found["links"], 1):
        print(f"{i}: {item['url'] if show else redact(item['url'])}")

def is_archive_download_url(url: str) -> bool:
    low = url.lower()
    return "takeout.google.com" in low and "download" in low

def resolve_archive_links_from_page(profile: Path, browser: Path | None, url: str) -> list[str]:
    proc, ws = open_browser_with_devtools(profile, browser, url)
    try:
        ws.settimeout(90)
        result = cdp_eval(ws, TAKEOUT_FIND_DOWNLOAD_LINKS_JS, await_promise=True)
    finally:
        ws.close()
        stop_browser(proc)

    if not result.get("ok"):
        print(f"Could not resolve links from {redact(url)}")
        print(result.get("detail", ""))
        return []
    return result.get("urls", [])

def resolve_archive_download_links(links: list[dict], profile: Path, browser: Path | None) -> list[str]:
    urls = [item["url"] if isinstance(item, dict) else item for item in links]
    urls = list(dict.fromkeys(urls))
    if urls and all(is_archive_download_url(url) for url in urls):
        return urls

    print("Resolving Takeout archive page into actual download links.")
    resolved: list[str] = []
    for url in urls:
        if is_archive_download_url(url):
            resolved.append(url)
            continue
        resolved.extend(resolve_archive_links_from_page(profile, browser, url))

    resolved = list(dict.fromkeys(resolved))
    if not resolved:
        raise RuntimeError("Could not find archive download links on the Takeout archive page")
    print(f"Resolved {len(resolved)} archive download link(s).")
    return resolved

def restic_base_command(plan: dict) -> list[str]:
    return ["restic", "-r", plan["repo"], "--password-file", str(plan["password_file"])]

def run_restic_backup(plan: dict, paths: list[Path]) -> None:
    subprocess.run(
        restic_base_command(plan)
        + [
            "backup",
            "--skip-if-unchanged",
            "--ignore-inode",
            "--tag",
            "autotakeout",
            *map(str, paths),
        ],
        env=plan["env"],
        check=True,
    )

def restic_dump_command(plan: dict, path: Path, *, path_selector: Path | None = None) -> list[str]:
    command = restic_base_command(plan) + ["dump", "--tag", "autotakeout"]
    if path_selector is not None:
        command.extend(["--path", str(path_selector.resolve())])
    command.extend(["latest", str(path.resolve())])
    return command

def restic_dump_bytes(
    plan: dict,
    path: Path,
    *,
    path_selector: Path | None = None,
    required: bool = True,
) -> bytes | None:
    command = restic_dump_command(plan, path, path_selector=path_selector)
    proc = subprocess.run(command, env=plan["env"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        if required:
            sys.stderr.write(proc.stderr.decode("utf-8", "replace"))
            raise subprocess.CalledProcessError(proc.returncode, command)
        return None
    return proc.stdout

def load_restic_validation_manifest(plan: dict, manifest_path: Path | None) -> dict | None:
    if not manifest_path:
        return None

    manifest = manifest_path.resolve()
    local_bytes = manifest.read_bytes() if manifest.exists() else None
    restored = restic_dump_bytes(plan, manifest, path_selector=manifest.parent, required=local_bytes is not None)
    if restored is None:
        return None
    if local_bytes is not None and restored != local_bytes:
        raise RuntimeError(f"Restic validation manifest mismatch for {manifest}")
    print(f"restic validation manifest restore verified: {manifest}")
    return json.loads(restored.decode("utf-8"))

def verify_restic_marker(plan: dict, marker_path: Path) -> None:
    expected = marker_path.read_bytes()
    marker = marker_path.resolve()
    restored = restic_dump_bytes(plan, marker, path_selector=marker.parent)
    if restored != expected:
        raise RuntimeError(f"Restic restore marker mismatch for {marker}")
    print(f"restic marker restore verified: {marker}")

def compare_summary(label: str, expected: dict, actual: dict) -> None:
    fields = ("file_count", "total_bytes")
    mismatches = [field for field in fields if expected[field] != actual[field]]
    if mismatches:
        details = ", ".join(f"{field}: {actual[field]} != {expected[field]}" for field in mismatches)
        raise RuntimeError(f"{label} summary mismatch for {expected['path']}: {details}")

def is_validation_metadata(path: Path) -> bool:
    return path.name in VALIDATION_METADATA_NAMES

def iter_regular_files(roots: list[Path]):
    for root in roots:
        root = root.resolve()
        for directory, dirnames, filenames in os.walk(root):
            dirpath = Path(directory)
            dirnames[:] = [name for name in dirnames if not (dirpath / name).is_symlink()]
            for name in filenames:
                candidate = dirpath / name
                if candidate.is_symlink() or is_validation_metadata(candidate):
                    continue
                path = candidate.resolve()
                try:
                    stat = path.stat()
                except FileNotFoundError:
                    continue
                if not path.is_file():
                    continue
                yield root, path, stat.st_size

def summarize_local_root(root: Path) -> dict:
    root = root.resolve()
    if not root.exists():
        raise RuntimeError(f"Validation path does not exist: {root}")

    file_count = 0
    total_bytes = 0
    for _, path, size in iter_regular_files([root]):
        file_count += 1
        total_bytes += size
    return {"path": str(root), "file_count": file_count, "total_bytes": total_bytes}

def verify_local_counts_and_sizes(manifest: dict) -> None:
    checked = 0
    missing = []
    for expected in manifest.get("paths", []):
        root = Path(expected["path"])
        if not root.exists():
            missing.append(str(root))
            continue
        actual = summarize_local_root(root)
        compare_summary("local filesystem", expected, actual)
        checked += 1

    if checked:
        print("local file counts and sizes match validation manifest")
    if missing:
        print("local path(s) missing; skipped local comparison for: " + ", ".join(missing))

def summarize_restic_root(plan: dict, root: Path) -> dict:
    root = root.resolve()
    command = restic_base_command(plan) + [
        "ls",
        "latest",
        "--tag",
        "autotakeout",
        "--path",
        str(root),
        "--recursive",
        "--json",
        str(root),
    ]
    listing = subprocess.run(command, env=plan["env"], check=True, stdout=subprocess.PIPE, text=True).stdout
    file_count = 0
    total_bytes = 0
    for line in listing.splitlines():
        item = json.loads(line)
        if item.get("message_type") != "node" or item.get("type") != "file":
            continue
        if is_validation_metadata(Path(item["path"])):
            continue
        file_count += 1
        total_bytes += int(item.get("size") or 0)
    return {"path": str(root), "file_count": file_count, "total_bytes": total_bytes}

def verify_restic_counts_and_sizes(plan: dict, manifest: dict) -> None:
    for expected in manifest.get("paths", []):
        actual = summarize_restic_root(plan, Path(expected["path"]))
        compare_summary("restic snapshot", expected, actual)
    print("restic file counts and sizes match validation manifest")

def format_bytes(size: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}" if unit != "B" else f"{size} B"
        value /= 1024
    return f"{size} B"

def restic_dump_sha256(plan: dict, path: Path, *, path_selector: Path | None = None) -> tuple[str, int]:
    command = restic_dump_command(plan, path, path_selector=path_selector)
    proc = subprocess.Popen(command, env=plan["env"], stdout=subprocess.PIPE)
    if proc.stdout is None:
        raise RuntimeError("restic dump did not provide stdout")

    digest = hashlib.sha256()
    size = 0
    for chunk in iter(lambda: proc.stdout.read(1024 * 1024), b""):
        size += len(chunk)
        digest.update(chunk)

    rc = proc.wait()
    if rc != 0:
        raise subprocess.CalledProcessError(rc, command)
    return digest.hexdigest(), size

def verify_restic_samples(plan: dict, manifest: dict) -> None:
    samples = manifest.get("samples", [])
    if not samples:
        print("No sample files in validation manifest.")
        return

    for i, sample in enumerate(samples, 1):
        digest, size = restic_dump_sha256(
            plan,
            Path(sample["path"]),
            path_selector=Path(sample["root"]),
        )
        if size != sample["size"]:
            raise RuntimeError(f"Restored sample size mismatch for {sample['path']}: {size} != {sample['size']}")
        if digest != sample["sha256"]:
            raise RuntimeError(f"Restored sample sha256 mismatch for {sample['path']}")
        print(f"restic sample {i}/{len(samples)} verified: {sample['relative_path']} ({format_bytes(size)})")

def verify_validation_manifest(plan: dict, manifest: dict) -> None:
    verify_restic_counts_and_sizes(plan, manifest)
    verify_local_counts_and_sizes(manifest)
    verify_restic_samples(plan, manifest)

def verify_restic_backup(
    plan: dict,
    *,
    marker_path: Path | None = None,
    manifest_path: Path | None = None,
    subset: str | None = "1%",
    full: bool = False,
) -> None:
    if marker_path and marker_path.exists():
        verify_restic_marker(plan, marker_path)
    else:
        print("No restore marker found; skipping marker dump verification.")

    manifest = load_restic_validation_manifest(plan, manifest_path) if manifest_path else None
    if manifest:
        verify_validation_manifest(plan, manifest)
    else:
        print("No validation manifest found; skipping count, size, and sample-file verification.")

    command = restic_base_command(plan) + ["check"]
    if full:
        command.append("--read-data")
    elif subset:
        command.extend(["--read-data-subset", subset])
    print("Running " + " ".join(command[:1] + command[5:]))
    subprocess.run(command, env=plan["env"], check=True)
    print("restic verification completed")

def write_restic_restore_marker(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    marker = root / RESTIC_RESTORE_MARKER
    write_private_json_if_changed(
        marker,
        {
            "purpose": "autotakeout restic restore verification",
            "root": str(root),
            "version": 1,
        },
    )
    return marker

def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def choose_validation_samples(roots: list[Path], count: int, max_bytes: int | None) -> list[dict]:
    if count <= 0:
        return []

    candidates = []
    for root, path, size in iter_regular_files(roots):
        if max_bytes is not None and size > max_bytes:
            continue
        candidates.append((hashlib.sha256(str(path).encode("utf-8")).hexdigest(), root, path, size))

    samples = []
    for _, root, path, size in sorted(candidates)[:count]:
        samples.append(
            {
                "root": str(root),
                "path": str(path),
                "relative_path": path.relative_to(root).as_posix(),
                "size": size,
                "sha256": sha256_file(path),
            }
        )
    return samples

def write_restic_validation_manifest(
    root: Path,
    paths: list[Path],
    *,
    sample_count: int,
    sample_max_mib: int,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    roots = [path.resolve() for path in paths]
    sample_max_bytes = None if sample_max_mib <= 0 else sample_max_mib * 1024**2
    manifest = {
        "version": 1,
        "paths": [summarize_local_root(root) for root in roots],
        "sample_max_bytes": sample_max_bytes,
        "samples": choose_validation_samples(roots, sample_count, sample_max_bytes),
    }

    path = root / RESTIC_VALIDATION_MANIFEST
    write_private_json_if_changed(path, manifest)
    print(
        "validation manifest: "
        f"{sum(item['file_count'] for item in manifest['paths'])} files, "
        f"{format_bytes(sum(item['total_bytes'] for item in manifest['paths']))}, "
        f"{len(manifest['samples'])} sample(s)"
    )
    return path

def run_backup_flow(a: argparse.Namespace, restic_plan: dict, *, paths: list[Path]) -> None:
    existing = [path for path in paths if path.exists()]
    missing = [path for path in paths if not path.exists()]
    if missing:
        raise SystemExit("Backup path does not exist: " + ", ".join(str(path) for path in missing))
    if not existing:
        raise SystemExit("No backup paths were provided")

    marker_root = existing[0]
    marker = write_restic_restore_marker(marker_root)
    manifest = write_restic_validation_manifest(
        marker_root,
        existing,
        sample_count=a.restic_sample_count,
        sample_max_mib=a.restic_sample_max_mib,
    )
    run_restic_backup(restic_plan, existing)
    if a.verify_restic:
        print("Verifying restic backup.")
        verify_restic_backup(
            restic_plan,
            marker_path=marker,
            manifest_path=manifest,
            subset=a.restic_check_subset,
            full=a.restic_full_check,
        )

def save_links(found: dict) -> None:
    path = STATE / "links.json"
    path.write_text(json.dumps(found, indent=2) + "\n")
    path.chmod(0o600)
    print(f"cached sensitive links in {path}")

def load_secrets() -> dict:
    if not SECRETS.exists():
        return {}
    SECRETS.chmod(0o600)
    try:
        return json.loads(SECRETS.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise SystemExit(f"Invalid secrets file {SECRETS}: {e}") from e

def save_secrets(data: dict) -> None:
    SECRETS.parent.mkdir(parents=True, exist_ok=True)
    write_private_json_if_changed(SECRETS, data)

def save_backblaze_b2_secrets(key_id: str, key: str) -> None:
    data = load_secrets()
    current = data.get("backblaze_b2") or {}
    updated = {"key_id": key_id, "key": key}
    if current == updated:
        return

    data["backblaze_b2"] = updated
    save_secrets(data)
    print(f"stored Backblaze B2 credentials: {SECRETS}")

def b2_env(a: argparse.Namespace) -> dict:
    env = os.environ.copy()
    stored = load_secrets().get("backblaze_b2", {})
    key_id = (
        a.b2_key_id
        or os.environ.get("B2_ACCOUNT_ID")
        or os.environ.get("B2_APPLICATION_KEY_ID")
        or stored.get("key_id")
    )
    key = (
        a.b2_key
        or os.environ.get("B2_ACCOUNT_KEY")
        or os.environ.get("B2_APPLICATION_KEY")
        or stored.get("key")
    )

    if not key_id:
        key_id = input("Backblaze application key ID: ").strip()
    if not key:
        key = getpass.getpass("Backblaze application key: ")
    if not key_id or not key:
        raise SystemExit("Backblaze B2 credentials are required for --restic")

    save_backblaze_b2_secrets(key_id, key)

    env["B2_ACCOUNT_ID"] = key_id
    env["B2_ACCOUNT_KEY"] = key
    env["B2_APPLICATION_KEY_ID"] = key_id
    env["B2_APPLICATION_KEY"] = key
    env.setdefault("B2_ACCOUNT_INFO", str(STATE / "b2-account-info.sqlite"))
    return env

def bucket_from_restic_repo(repo: str | None) -> str | None:
    if not repo or not repo.startswith("b2:"):
        return None
    parts = repo.split(":", 2)
    return parts[1] if len(parts) > 1 and parts[1] else None

def authorize_b2(env: dict, b2: str) -> None:
    probe = subprocess.run(
        [b2, "bucket", "list"],
        env=env,
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    if probe.returncode == 0:
        return

    print("Authorizing Backblaze B2 CLI.")
    subprocess.run([b2, "account", "authorize"], env=env, check=True)

def b2_cli() -> str:
    for name in ("b2", "backblaze-b2", "b2v4", "b2v3"):
        if shutil.which(name):
            return name
    raise SystemExit("Backblaze B2 CLI is not on PATH")

def ensure_b2_bucket(bucket: str, env: dict) -> None:
    b2 = b2_cli()
    authorize_b2(env, b2)
    listing = subprocess.run(
        [b2, "bucket", "list"],
        env=env,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout
    if any(bucket in line.split() for line in listing.splitlines()):
        print(f"B2 bucket exists: {bucket}")
        return

    print(f"Creating private B2 bucket: {bucket}")
    subprocess.run([b2, "bucket", "create", bucket, "allPrivate"], env=env, check=True)

def ensure_restic_password_file(path: Path | None, *, create: bool = True) -> Path:
    path = path or (STATE / "restic-password")
    if path.exists():
        return path
    if not create:
        raise SystemExit(f"Restic password file does not exist: {path}")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(secrets.token_urlsafe(48) + "\n")
    path.chmod(0o600)
    print(f"created new restic password file: {path}")
    print("Keep a separate copy of that password file; it is required for restores.")
    return path

def ensure_restic_repo(repo: str, password_file: Path, env: dict) -> None:
    probe = subprocess.run(
        restic_base_command({"repo": repo, "password_file": password_file}) + ["snapshots", "--json"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    if probe.returncode == 0:
        print("restic repo is already initialized.")
        return

    print("Initializing restic repo.")
    subprocess.run(
        restic_base_command({"repo": repo, "password_file": password_file}) + ["init"],
        env=env,
        check=True,
    )

def load_config() -> dict:
    if not CONFIG.exists():
        return {}
    try:
        return json.loads(CONFIG.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise SystemExit(f"Invalid config file {CONFIG}: {e}") from e

def save_config(config: dict) -> None:
    CONFIG.parent.mkdir(parents=True, exist_ok=True)
    CONFIG.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    CONFIG.chmod(0o600)

def setup_restic(a: argparse.Namespace, *, initialize: bool = True, save: bool = True) -> dict:
    repo = a.restic_repo
    bucket = bucket_from_restic_repo(repo) or a.b2_bucket
    if not repo:
        if not bucket:
            bucket = input("Backblaze B2 bucket name for restic: ").strip()
        if not bucket:
            raise SystemExit("--b2-bucket is required when --restic-repo is not set")
        repo = f"b2:{bucket}:{a.b2_prefix}"

    password_file = ensure_restic_password_file(a.restic_password_file, create=initialize)
    env = os.environ.copy()

    if repo.startswith("b2:"):
        env = b2_env(a)
        bucket = bucket_from_restic_repo(repo)
    if initialize and bucket and repo.startswith("b2:"):
        ensure_b2_bucket(bucket, env)
    if initialize:
        ensure_restic_repo(repo, password_file, env)

    if save:
        config = load_config()
        config["restic_repo"] = repo
        config["restic_password_file"] = str(password_file)
        if bucket:
            config["b2_bucket"] = bucket
        config["b2_prefix"] = a.b2_prefix
        save_config(config)

    print(f"restic repo: {repo}")
    print(f"restic password file: {password_file}")
    return {"repo": repo, "password_file": password_file, "env": env}

def restic_repo_guid(plan: dict) -> str:
    command = restic_base_command(plan) + ["cat", "config", "--json"]
    proc = subprocess.run(command, env=plan["env"], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise subprocess.CalledProcessError(proc.returncode, command)

    try:
        config = json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError("Could not parse restic repo config JSON") from e

    guid = config.get("id", "")
    if not re.fullmatch(r"[0-9a-f]{64}", guid):
        raise RuntimeError(f"Restic repo id is not a 64-character hex value: {guid!r}")
    return guid

def load_backrest_config(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise SystemExit(f"Invalid Backrest config file {path}: {e}") from e

def upsert_backrest_item(items: list[dict], item: dict) -> None:
    for index, existing in enumerate(items):
        if existing.get("id") == item["id"]:
            items[index] = {**existing, **item}
            return
    items.append(item)

def write_backrest_config(plan: dict, merged: Path, config_path: Path) -> Path:
    repo_id = "autotakeout-restic"
    plan_id = "google-photos-merged"
    password_file = plan["password_file"].expanduser().resolve()

    config = load_backrest_config(config_path)
    config.setdefault("version", 6)
    config.setdefault("instance", "autotakeout")
    config.setdefault("auth", {"disabled": True})
    repos = config.setdefault("repos", [])
    plans = config.setdefault("plans", [])
    if not isinstance(repos, list) or not isinstance(plans, list):
        raise SystemExit(f"Invalid Backrest config file {config_path}: repos and plans must be lists")

    upsert_backrest_item(
        repos,
        {
            "id": repo_id,
            "uri": plan["repo"],
            "guid": restic_repo_guid(plan),
            "env": [f"RESTIC_PASSWORD_FILE={password_file}"],
            "autoUnlock": True,
            "autoInitialize": False,
            "prunePolicy": {"schedule": {"disabled": True}},
            "checkPolicy": {"schedule": {"disabled": True}},
        },
    )
    upsert_backrest_item(
        plans,
        {
            "id": plan_id,
            "repo": repo_id,
            "paths": [str(merged.expanduser().resolve())],
            "schedule": {"disabled": True},
            "retention": {"policyKeepAll": True},
            "backup_flags": ["--tag autotakeout", "--ignore-inode"],
            "skipIfUnchanged": True,
        },
    )

    write_private_json_if_changed(config_path, config)
    return config_path

def backrest_display_url(bind_address: str) -> str:
    if bind_address.startswith(":"):
        return f"http://127.0.0.1{bind_address}"
    if bind_address.startswith("0.0.0.0:"):
        return "http://127.0.0.1:" + bind_address.rsplit(":", 1)[1]
    return f"http://{bind_address}"

def backrest_socket_target(bind_address: str) -> tuple[str, int] | None:
    if bind_address.startswith(":"):
        return "127.0.0.1", int(bind_address[1:])
    if bind_address.startswith("[") and "]:" in bind_address:
        host, port = bind_address.rsplit("]:", 1)
        return host[1:], int(port)
    if ":" not in bind_address:
        return None

    host, port = bind_address.rsplit(":", 1)
    if host in ("", "0.0.0.0", "::"):
        host = "127.0.0.1"
    return host, int(port)

def wait_for_backrest_listener(bind_address: str, timeout_seconds: int = 30) -> bool:
    target = backrest_socket_target(bind_address)
    if target is None:
        time.sleep(2)
        return True

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(target, timeout=0.5):
                return True
        except OSError:
            time.sleep(0.25)
    return False

def open_url(url: str) -> bool:
    if sys.platform == "darwin":
        command = ["open", url]
    elif os.name == "nt":
        command = ["cmd", "/c", "start", "", url]
    else:
        opener = shutil.which("xdg-open")
        if not opener:
            return False
        command = [opener, url]

    try:
        subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except OSError:
        return False

def request_backrest_snapshot_index(url: str) -> bool:
    endpoint = url.rstrip("/") + "/v1.Backrest/DoRepoTask"
    payload = json.dumps(
        {
            "repoId": "autotakeout-restic",
            "task": "TASK_INDEX_SNAPSHOTS",
        }
    ).encode("utf-8")
    request = Request(endpoint, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(request, timeout=15) as response:
            response.read()
        print("Requested Backrest snapshot index.", flush=True)
        return True
    except Exception as e:
        print(f"Could not request Backrest snapshot index automatically: {e}", flush=True)
        return False

def run_backrest_startup_actions(bind_address: str, url: str, *, index_snapshots: bool, open_browser: bool) -> None:
    def worker() -> None:
        if not wait_for_backrest_listener(bind_address):
            print(f"Backrest did not become reachable yet; open this URL after it starts: {url}", flush=True)
            return
        if index_snapshots:
            request_backrest_snapshot_index(url)
        if open_browser and not open_url(url):
            print(f"Could not open a browser automatically; open this URL: {url}", flush=True)

    threading.Thread(target=worker, daemon=True).start()

def start_backrest(a: argparse.Namespace) -> None:
    plan = setup_restic(a, initialize=False, save=False)
    config_path = (a.backrest_config or BACKREST_CONFIG).expanduser()
    data_dir = (a.backrest_data or BACKREST_DATA).expanduser()
    data_dir.mkdir(parents=True, exist_ok=True)

    write_backrest_config(plan, a.merged, config_path)
    env = plan["env"].copy()
    env["RESTIC_PASSWORD_FILE"] = str(plan["password_file"].expanduser().resolve())

    restic_cmd = shutil.which("restic") or "restic"
    command = [
        "backrest",
        "-bind-address",
        a.bind_address,
        "-config-file",
        str(config_path),
        "-data-dir",
        str(data_dir),
        "-restic-cmd",
        restic_cmd,
    ]
    url = backrest_display_url(a.bind_address)
    print(f"Backrest config: {config_path}", flush=True)
    print(f"Backrest data: {data_dir}", flush=True)
    print(f"Backrest URL: {url}", flush=True)
    if a.index_snapshots:
        print("Indexing Backrest snapshots after the web server starts.", flush=True)
    if a.open_browser:
        print(f"Opening Backrest in your browser. If it does not open, visit: {url}", flush=True)
    if a.index_snapshots or a.open_browser:
        run_backrest_startup_actions(
            a.bind_address,
            url,
            index_snapshots=a.index_snapshots,
            open_browser=a.open_browser,
        )
    print("Press Ctrl-C to stop Backrest.", flush=True)
    subprocess.run(command, env=env, check=True)

def wait_for_takeout_links(
    service,
    *,
    query: str,
    max_emails: int,
    poll_seconds: int,
    timeout_seconds: int,
) -> dict:
    started = time.monotonic()
    attempt = 1
    while True:
        found = find_takeout_links_from_service(service, query, max_emails)
        if found:
            return found

        elapsed = int(time.monotonic() - started)
        if timeout_seconds and elapsed >= timeout_seconds:
            raise SystemExit(f"No Takeout email with download links after {elapsed} seconds")

        if attempt == 1:
            print("No Takeout download email yet.")
            print("If you have not created the export yet, create it at https://takeout.google.com/ now.")

        sleep_for = poll_seconds
        if timeout_seconds:
            sleep_for = min(sleep_for, max(1, timeout_seconds - elapsed))
        print(f"Waiting {sleep_for}s before checking Gmail again...")
        time.sleep(sleep_for)
        attempt += 1

def write_pending_takeout_export() -> None:
    PENDING_EXPORT.parent.mkdir(parents=True, exist_ok=True)
    PENDING_EXPORT.write_text(
        json.dumps(
            {
                "product": "Google Photos",
                "requested_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "requested_at_unix": time.time(),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    PENDING_EXPORT.chmod(0o600)

def guided(a: argparse.Namespace) -> None:
    print(f"state: {STATE}")
    print(f"raw archives: {a.raw}")
    print(f"merged output: {a.merged}")
    print("")

    restic_plan = None
    if a.restic:
        print("0. Checking Backblaze B2 and restic.")
        restic_plan = setup_restic(a)
        print("")

    print("1. Checking Gmail OAuth.")
    service = gmail_service(a.credentials, a.token)

    print("2. Checking browser login.")
    if a.force_login or not browser_profile_ready(a.profile, a.browser):
        print("Opening browser. Log into Google, then press Enter here.")
        browser_login(a.profile, a.browser)
    else:
        print("Browser profile looks logged in.")

    print("3. Looking for Takeout email.")
    found = find_takeout_links_from_service(service, a.query, a.max_emails)
    if not found:
        print("No ready Takeout email found.")
        if a.create_export:
            if pending_takeout_export_active():
                print("Takeout export was already requested; waiting on Gmail instead of creating another one.")
            else:
                print("Creating a Google Photos Takeout export automatically.")
                create_takeout_export(profile=a.profile, browser=a.browser)
                write_pending_takeout_export()
        else:
            print("Opening Takeout for manual export creation.")
            open_takeout(profile=a.profile, browser=a.browser)
            write_pending_takeout_export()
        found = wait_for_takeout_links(
            service,
            query=a.query,
            max_emails=a.max_emails,
            poll_seconds=a.poll,
            timeout_seconds=a.timeout,
        )
    clear_pending_takeout_export()
    save_links(found)
    print_links(found, show=False)

    archive_links = resolve_archive_download_links(found["links"], a.profile, a.browser)
    if archive_links != [link["url"] for link in found["links"]]:
        found["links"] = [{"url": url, "text": "resolved archive download"} for url in archive_links]
        save_links(found)
    prepare_raw_directory(a.raw, archive_links, force=a.force)

    print("4. Downloading archives.")
    if a.skip_extract:
        download(archive_links, a.raw, a.profile, a.browser, a.downloader, a.google_password)
        print("5. Skipping extract.")
    else:
        print("5. Extracting and merging archives while downloads continue.")
        with ExtractionPipeline(a.raw, a.merged) as extractor:
            download(
                archive_links,
                a.raw,
                a.profile,
                a.browser,
                a.downloader,
                a.google_password,
                on_complete=extractor.enqueue,
            )

    if a.restic:
        print("6. Running restic backup.")
        if a.skip_extract and not a.merged.exists():
            print("Skipping restic backup because --skip-extract left no merged output to back up.")
        else:
            run_backup_flow(a, restic_plan, paths=[a.merged])

    if a.rclone_remote:
        print("6. Running rclone backup.")
        subprocess.run(
            [
                "rclone",
                "sync" if a.rclone_sync else "copy",
                str(a.merged),
                a.rclone_remote,
                "--progress",
            ],
            check=True,
        )

def load_links() -> list[str]:
    return [x["url"] for x in json.loads((STATE / "links.json").read_text())["links"]]

def config_path(config: dict, key: str) -> Path | None:
    value = config.get(key)
    return Path(value).expanduser() if value else None

def credential_search_dirs() -> tuple[Path, ...]:
    return (Path.home() / "Downloads", CONFIG.parent)

def discover_credentials() -> Path | None:
    candidates: list[Path] = []
    for directory in credential_search_dirs():
        for pattern in ("client_secret*.json", "credentials*.json", "google_oauth*.json", "oauth*.json"):
            candidates.extend(sorted(directory.glob(pattern)))
    candidates = [path for path in candidates if path.is_file()]

    if len(candidates) == 1:
        print(f"Found Google OAuth client JSON: {candidates[0]}")
        return candidates[0]
    if not candidates:
        return None

    print("Found multiple possible Google OAuth client JSON files:")
    for i, path in enumerate(candidates, 1):
        print(f"{i}. {path}")
    if not sys.stdin.isatty():
        raise SystemExit("Pass --credentials because multiple OAuth JSON files were found")

    while True:
        choice = input("Use which file? ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(candidates):
            return candidates[int(choice) - 1]
        print("Enter one of the listed numbers.")

def explain_missing_credentials() -> None:
    print("")
    print("I need one Gmail API setup file so I can monitor your inbox for the Takeout email.")
    print("This is not your Google password and it is not a Takeout archive.")
    print("It is the OAuth client JSON downloaded from Google Cloud, usually named like:")
    print("  client_secret_1234567890-abc.apps.googleusercontent.com.json")
    print("")
    print("Step-by-step:")
    print("  1. Open https://console.cloud.google.com/apis/library/gmail.googleapis.com")
    print("  2. Select or create a Google Cloud project.")
    print("  3. Click Enable for the Gmail API.")
    print("  4. Open https://console.cloud.google.com/auth/overview")
    print("  5. If Google Auth Platform is not configured, click Get started.")
    print("  6. App name: autotakeout. User support email: your email.")
    print("  7. Audience: choose Internal if this is a Workspace account and it is offered;")
    print("     otherwise choose External/Testing and add your own Gmail as a test user if asked.")
    print("  8. Contact email: your email. Accept the user data policy and finish.")
    print("  9. Open https://console.cloud.google.com/auth/clients")
    print(" 10. Click Create client, choose Application type: Desktop app.")
    print(" 11. Name it autotakeout, click Create, then download the JSON file.")
    print(" 12. Put that JSON in this repo, ~/Downloads, or ~/.config/autotakeout.")
    print("")
    print("I looked in:")
    for directory in credential_search_dirs():
        print(f"  {directory}")
    print("")
    print("Paste the path to the downloaded JSON below, or press Ctrl-C and rerun after moving it.")

def find_browser() -> Path | None:
    for name in ("brave", "brave-browser", "google-chrome", "chromium", "chromium-browser"):
        if found := shutil.which(name):
            return Path(found)
    return None

def prompt_path(
    label: str,
    *,
    default: Path | None = None,
    must_exist: bool = False,
    allow_blank: bool = False,
    blank_message: str | None = None,
) -> Path | None:
    if not sys.stdin.isatty():
        raise SystemExit(f"{label} is required")

    suffix = f" [{default}]" if default else ""
    while True:
        raw = input(f"{label}{suffix}: ").strip()
        if not raw and default:
            raw = str(default)
        if not raw and allow_blank:
            if blank_message:
                print(blank_message)
            return None
        if not raw:
            print("A value is required.")
            continue
        path = Path(raw).expanduser()
        if must_exist and not path.exists():
            print(f"Path does not exist: {path}")
            continue
        return path

def usable_config_path(config: dict, key: str) -> Path | None:
    value = config_path(config, key)
    return value if value and value.exists() else None

def resolve_preferences(
    a: argparse.Namespace,
    *,
    credentials: bool = False,
    browser: bool = False,
    raw: bool = False,
    merged: bool = False,
    restic: bool = False,
    save: bool = True,
) -> None:
    config = load_config()
    changed = False

    if getattr(a, "token", None) is None:
        a.token = config_path(config, "token") or (STATE / "gmail-token.json")

    if credentials:
        value = a.credentials or usable_config_path(config, "credentials") or discover_credentials()
        if value is None:
            explain_missing_credentials()
            value = prompt_path("Path to downloaded client_secret_*.json file", must_exist=True)
        a.credentials = value
        config["credentials"] = str(value)
        changed = True
    elif getattr(a, "credentials", None) is None and config.get("credentials"):
        a.credentials = config_path(config, "credentials")

    if browser:
        value = a.browser or usable_config_path(config, "browser") or find_browser()
        if value is None:
            value = prompt_path(
                "Browser executable path",
                must_exist=True,
            )
        a.browser = value
        if value:
            config["browser"] = str(value)
            changed = True

        if getattr(a, "profile", None) is None:
            a.profile = config_path(config, "profile") or (STATE / "browser-profile")
        config["profile"] = str(a.profile)
        changed = True

    if raw:
        if getattr(a, "raw", None) is None:
            a.raw = config_path(config, "raw") or Path("data/raw")
        config["raw"] = str(a.raw)
        changed = True

    if merged:
        if getattr(a, "merged", None) is None:
            a.merged = config_path(config, "merged") or Path("data/merged")
        config["merged"] = str(a.merged)
        changed = True

    if restic:
        explicit_bucket = getattr(a, "b2_bucket", None)
        if getattr(a, "restic_repo", None) is None and explicit_bucket is None:
            a.restic_repo = os.environ.get("RESTIC_REPOSITORY") or config.get("restic_repo")
        if getattr(a, "restic_password_file", None) is None:
            env_password = os.environ.get("RESTIC_PASSWORD_FILE")
            a.restic_password_file = Path(env_password) if env_password else config_path(config, "restic_password_file")
        if explicit_bucket is None:
            a.b2_bucket = config.get("b2_bucket")
        if getattr(a, "b2_prefix", None) is None:
            a.b2_prefix = config.get("b2_prefix") or "autotakeout-restic"
        config["b2_prefix"] = a.b2_prefix
        changed = True

    if changed and save:
        save_config(config)
        print(f"preferences: {CONFIG}")

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--credentials", type=Path, help="Downloaded Gmail API client_secret_*.json file")
    p.add_argument("--token", type=Path)
    p.add_argument("--browser", type=Path)
    p.add_argument("--profile", type=Path)
    p.add_argument("--raw", type=Path)
    p.add_argument("--merged", type=Path)
    p.add_argument("--query", default=TAKEOUT_QUERY)
    p.add_argument("--max-emails", type=int, default=50)
    p.add_argument("--poll", type=int, default=300, help="Seconds between Gmail checks")
    p.add_argument("--timeout", type=int, default=0, help="Seconds to wait for email; 0 waits forever")
    p.add_argument(
        "--create-export",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Automatically create the Google Photos Takeout export if no ready email exists.",
    )
    p.add_argument("--skip-extract", action="store_true")
    p.add_argument("--downloader", choices=["auto", "browser", "aria2c", "curl"], default="auto")
    p.add_argument("--google-password", help="Google password for Takeout reauth; never stored")
    p.add_argument(
        "--restic",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run restic backup after extraction; use --no-restic to skip.",
    )
    p.add_argument("--restic-repo")
    p.add_argument(
        "--restic-password-file",
        type=Path,
    )
    p.add_argument("--b2-bucket", help="Backblaze B2 bucket to create/use for restic")
    p.add_argument("--b2-prefix", help="Path inside the B2 bucket for the restic repo")
    p.add_argument("--b2-key-id")
    p.add_argument("--b2-key")
    p.add_argument(
        "--verify-restic",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Verify restic after backup; use --no-verify-restic to skip.",
    )
    p.add_argument("--restic-check-subset", default="1%", help="Subset for restic check --read-data-subset")
    p.add_argument("--restic-full-check", action="store_true", help="Run restic check --read-data")
    p.add_argument("--restic-sample-count", type=int, default=5, help="Number of files to restore and hash-check")
    p.add_argument(
        "--restic-sample-max-mib",
        type=int,
        default=512,
        help="Maximum sample file size in MiB; 0 disables the limit",
    )
    p.add_argument("--rclone-remote", help="Run rclone copy after extraction, e.g. b2remote:google-photos")
    p.add_argument("--rclone-sync", action="store_true")
    p.add_argument("--force-login", action="store_true", help="Always open the browser login step first")
    p.add_argument("--force", action="store_true", help="Delete old raw Takeout files without prompting")

    sub = p.add_subparsers(dest="cmd")

    login = sub.add_parser("login", help="Log into Google in a dedicated browser profile")
    login.add_argument("--browser", type=Path)
    login.add_argument("--profile", type=Path)
    login.add_argument("--credentials", type=Path, help="Also create Gmail token")

    run = sub.add_parser("run", help="Find Takeout email, download archives, extract them")
    run.add_argument("--credentials", type=Path)
    run.add_argument("--token", type=Path)
    run.add_argument("--browser", type=Path)
    run.add_argument("--profile", type=Path)
    run.add_argument("--raw", type=Path)
    run.add_argument("--merged", type=Path)
    run.add_argument("--query", default=TAKEOUT_QUERY)
    run.add_argument("--max-emails", type=int, default=50)
    run.add_argument("--skip-extract", action="store_true")
    run.add_argument("--downloader", choices=["auto", "browser", "aria2c", "curl"], default="auto")
    run.add_argument("--google-password")
    run.add_argument("--force", action="store_true", help="Delete old raw Takeout files without prompting")
    run.add_argument(
        "--restic",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run restic backup after extraction; use --no-restic to skip.",
    )
    run.add_argument("--restic-repo")
    run.add_argument("--restic-password-file", type=Path)
    run.add_argument("--b2-bucket", help="Backblaze B2 bucket to create/use for restic")
    run.add_argument("--b2-prefix", help="Path inside the B2 bucket for the restic repo")
    run.add_argument("--b2-key-id")
    run.add_argument("--b2-key")
    run.add_argument(
        "--verify-restic",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Verify restic after backup; use --no-verify-restic to skip.",
    )
    run.add_argument("--restic-check-subset", default="1%", help="Subset for restic check --read-data-subset")
    run.add_argument("--restic-full-check", action="store_true", help="Run restic check --read-data")
    run.add_argument("--restic-sample-count", type=int, default=5, help="Number of files to restore and hash-check")
    run.add_argument(
        "--restic-sample-max-mib",
        type=int,
        default=512,
        help="Maximum sample file size in MiB; 0 disables the limit",
    )

    links = sub.add_parser("links", help="Print and cache links from the newest Takeout email")
    links.add_argument("--credentials", type=Path)
    links.add_argument("--token", type=Path)
    links.add_argument("--query", default=TAKEOUT_QUERY)
    links.add_argument("--max-emails", type=int, default=50)
    links.add_argument("--show", action="store_true")

    dl = sub.add_parser("download", help="Download cached links")
    dl.add_argument("--browser", type=Path)
    dl.add_argument("--profile", type=Path)
    dl.add_argument("--raw", type=Path)
    dl.add_argument("--downloader", choices=["auto", "browser", "aria2c", "curl"], default="auto")
    dl.add_argument("--google-password")
    dl.add_argument("--force", action="store_true", help="Delete old raw Takeout files without prompting")

    ex = sub.add_parser("extract", help="Extract downloaded .tgz archives")
    ex.add_argument("--raw", type=Path)
    ex.add_argument("--merged", type=Path)

    backup = sub.add_parser("backup", help="Back up existing merged output with restic")
    backup.add_argument("--merged", type=Path)
    backup.add_argument("--restic-repo")
    backup.add_argument("--restic-password-file", type=Path)
    backup.add_argument("--b2-bucket", help="Backblaze B2 bucket to create/use for restic")
    backup.add_argument("--b2-prefix", help="Path inside the B2 bucket for the restic repo")
    backup.add_argument("--b2-key-id")
    backup.add_argument("--b2-key")
    backup.add_argument(
        "--verify-restic",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Verify restic after backup; use --no-verify-restic to skip.",
    )
    backup.add_argument("--restic-check-subset", default="1%", help="Subset for restic check --read-data-subset")
    backup.add_argument("--restic-full-check", action="store_true", help="Run restic check --read-data")
    backup.add_argument("--restic-sample-count", type=int, default=5, help="Number of files to restore and hash-check")
    backup.add_argument(
        "--restic-sample-max-mib",
        type=int,
        default=512,
        help="Maximum sample file size in MiB; 0 disables the limit",
    )

    restic = sub.add_parser("restic", help="Run restic backup")
    restic.add_argument("paths", nargs="+", type=Path)
    restic.add_argument("--repo", default=os.environ.get("RESTIC_REPOSITORY"))

    verify = sub.add_parser("verify", help="Verify the restic backup")
    verify.add_argument("--raw", type=Path)
    verify.add_argument("--merged", type=Path)
    verify.add_argument("--restic-repo")
    verify.add_argument("--restic-password-file", type=Path)
    verify.add_argument("--b2-bucket", help="Backblaze B2 bucket used for restic")
    verify.add_argument("--b2-prefix", help="Path inside the B2 bucket for the restic repo")
    verify.add_argument("--b2-key-id")
    verify.add_argument("--b2-key")
    verify.add_argument("--restic-check-subset", default="1%", help="Subset for restic check --read-data-subset")
    verify.add_argument("--restic-full-check", action="store_true", help="Run restic check --read-data")

    backrest = sub.add_parser("backrest", help="Start Backrest connected to the configured restic repo")
    backrest.add_argument("--merged", type=Path)
    backrest.add_argument("--restic-repo")
    backrest.add_argument("--restic-password-file", type=Path)
    backrest.add_argument("--b2-bucket", help="Backblaze B2 bucket used for restic")
    backrest.add_argument("--b2-prefix", help="Path inside the B2 bucket for the restic repo")
    backrest.add_argument("--b2-key-id")
    backrest.add_argument("--b2-key")
    backrest.add_argument("--bind-address", default="127.0.0.1:9898", help="Backrest bind address")
    backrest.add_argument("--backrest-config", type=Path, help="Backrest config path")
    backrest.add_argument("--backrest-data", type=Path, help="Backrest data directory")
    backrest.add_argument(
        "--open-browser",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Open Backrest in the default browser after the web server starts.",
    )
    backrest.add_argument(
        "--index-snapshots",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Ask Backrest to index restic snapshots after the web server starts.",
    )

    rclone = sub.add_parser("rclone", help="Run rclone copy/sync")
    rclone.add_argument("source", type=Path)
    rclone.add_argument("remote")
    rclone.add_argument("--sync", action="store_true")

    a = p.parse_args()
    STATE.mkdir(parents=True, exist_ok=True)

    if a.cmd is None:
        resolve_preferences(a, credentials=True, browser=True, raw=True, merged=True, restic=a.restic)
        guided(a)
    elif a.cmd == "login":
        resolve_preferences(a, credentials=bool(a.credentials), browser=True)
        if a.credentials:
            gmail_service(a.credentials, STATE / "gmail-token.json")
        browser_login(a.profile, a.browser)
    elif a.cmd == "links":
        resolve_preferences(a, credentials=True, save=False)
        found = find_takeout_links(a.credentials, a.token, a.query, a.max_emails)
        save_links(found)
        print_links(found, show=a.show)
    elif a.cmd == "download":
        resolve_preferences(a, browser=True, raw=True, save=False)
        archive_links = resolve_archive_download_links(
            [{"url": url, "text": "cached"} for url in load_links()],
            a.profile,
            a.browser,
        )
        prepare_raw_directory(a.raw, archive_links, force=a.force)
        download(archive_links, a.raw, a.profile, a.browser, a.downloader, a.google_password)
    elif a.cmd == "extract":
        resolve_preferences(a, raw=True, merged=True, save=False)
        extract_all(a.raw, a.merged)
    elif a.cmd == "run":
        resolve_preferences(a, credentials=True, browser=True, raw=True, merged=True, restic=a.restic)
        restic_plan = setup_restic(a) if a.restic else None
        found = find_takeout_links(a.credentials, a.token, a.query, a.max_emails)
        save_links(found)
        print_links(found, show=False)
        archive_links = resolve_archive_download_links(found["links"], a.profile, a.browser)
        prepare_raw_directory(a.raw, archive_links, force=a.force)
        if not a.skip_extract:
            with ExtractionPipeline(a.raw, a.merged) as extractor:
                download(
                    archive_links,
                    a.raw,
                    a.profile,
                    a.browser,
                    a.downloader,
                    a.google_password,
                    on_complete=extractor.enqueue,
                )
        else:
            download(archive_links, a.raw, a.profile, a.browser, a.downloader, a.google_password)
        if a.restic:
            if a.skip_extract and not a.merged.exists():
                print("Skipping restic backup because --skip-extract left no merged output to back up.")
            else:
                run_backup_flow(a, restic_plan, paths=[a.merged])
    elif a.cmd == "backup":
        resolve_preferences(a, merged=True, restic=True)
        run_backup_flow(a, setup_restic(a), paths=[a.merged])
    elif a.cmd == "restic":
        if not a.repo:
            raise SystemExit("--repo or RESTIC_REPOSITORY is required")
        subprocess.run(["restic", "-r", a.repo, "backup", *map(str, a.paths)], check=True)
    elif a.cmd == "verify":
        resolve_preferences(a, raw=True, merged=True, restic=True, save=False)
        marker_path = first_existing_path(
            [a.merged / RESTIC_RESTORE_MARKER, a.raw / RESTIC_RESTORE_MARKER],
            default=a.merged / RESTIC_RESTORE_MARKER,
        )
        manifest_path = first_existing_path(
            [a.merged / RESTIC_VALIDATION_MANIFEST, a.raw / RESTIC_VALIDATION_MANIFEST],
            default=a.merged / RESTIC_VALIDATION_MANIFEST,
        )
        verify_restic_backup(
            setup_restic(a, initialize=False, save=False),
            marker_path=marker_path,
            manifest_path=manifest_path,
            subset=a.restic_check_subset,
            full=a.restic_full_check,
        )
    elif a.cmd == "backrest":
        resolve_preferences(a, merged=True, restic=True, save=False)
        start_backrest(a)
    elif a.cmd == "rclone":
        subprocess.run(["rclone", "sync" if a.sync else "copy", str(a.source), a.remote, "--progress"], check=True)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
