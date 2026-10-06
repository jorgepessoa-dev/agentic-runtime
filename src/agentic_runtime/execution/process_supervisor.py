from __future__ import annotations

import os
import signal
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence


@dataclass
class ManagedProcess:
    execution_id: str
    process: subprocess.Popen[bytes]
    stdout_file: object
    stderr_file: object
    started_monotonic: float
    deadline_monotonic: float
    last_heartbeat: float
    peak_rss_kb: int = 0
    state: str = "RUNNING"
    exit_code: int | None = None
    reason: str | None = None
    result_committed: bool = False
    state_lock: threading.RLock = field(default_factory=threading.RLock)


class ProcessSupervisor:
    """Owns a POSIX process session and signals its entire process group."""

    def __init__(self, terminate_grace_seconds: float = 0.4, poll_seconds: float = 0.05) -> None:
        self.terminate_grace_seconds = terminate_grace_seconds
        self.poll_seconds = poll_seconds
        self._processes: dict[str, ManagedProcess] = {}

    def start(self, execution_id: str, argv: Sequence[str], cwd: Path,
              env: Mapping[str, str], timeout_seconds: float) -> ManagedProcess:
        if not argv or timeout_seconds <= 0:
            raise ValueError("argv and positive timeout_seconds are required")
        if execution_id in self._processes:
            raise ValueError(f"execution already started: {execution_id}")
        out, err = tempfile.TemporaryFile(), tempfile.TemporaryFile()
        now = time.monotonic()
        try:
            process = subprocess.Popen(list(argv), cwd=cwd, env=dict(env), stdin=subprocess.DEVNULL,
                stdout=out, stderr=err, start_new_session=True, close_fds=True)
        except BaseException:
            out.close(); err.close()
            raise
        managed = ManagedProcess(execution_id, process, out, err, now, now + timeout_seconds, now)
        self._processes[execution_id] = managed
        return managed

    @staticmethod
    def _sample_rss(process: subprocess.Popen[bytes]) -> int:
        try:
            for line in Path(f"/proc/{process.pid}/status").read_text().splitlines():
                if line.startswith("VmHWM:"):
                    return int(line.split()[1])
        except (OSError, ValueError, IndexError):
            pass
        return 0

    @staticmethod
    def _sample_group_rss_kb(pgid: int) -> int:
        total = 0
        try:
            for entry in Path("/proc").iterdir():
                if not entry.name.isdigit():
                    continue
                try:
                    stat = (entry / "stat").read_text()
                    fields = stat[stat.rfind(")") + 2:].split()
                    if fields[2] == str(pgid) and fields[0] not in {"Z", "X"}:
                        for line in (entry / "status").read_text().splitlines():
                            if line.startswith("VmRSS:"):
                                total += int(line.split()[1])
                                break
                except (OSError, IndexError, ValueError):
                    continue
        except OSError:
            pass
        return total

    @staticmethod
    def _group_has_live_process(pgid: int) -> bool:
        try:
            for entry in Path("/proc").iterdir():
                if not entry.name.isdigit():
                    continue
                try:
                    stat = (entry / "stat").read_text()
                    fields = stat[stat.rfind(")") + 2:].split()  # fields start at proc field 3
                    if fields[2] == str(pgid) and fields[0] not in {"Z", "X"}:
                        return True
                except (OSError, IndexError):
                    continue
        except OSError:
            return False
        return False

    @staticmethod
    def _signal_group(managed: ManagedProcess, sig: int) -> None:
        try:
            os.killpg(managed.process.pid, sig)
        except ProcessLookupError:
            pass

    def _stop_group(self, managed: ManagedProcess, reason: str) -> None:
        managed.reason = reason
        self._signal_group(managed, signal.SIGTERM)
        end = time.monotonic() + self.terminate_grace_seconds
        while time.monotonic() < end and self._group_has_live_process(managed.process.pid):
            managed.peak_rss_kb = max(managed.peak_rss_kb, self._sample_group_rss_kb(managed.process.pid))
            time.sleep(self.poll_seconds)
        if self._group_has_live_process(managed.process.pid):
            self._signal_group(managed, signal.SIGKILL)
        try:
            managed.process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            self._signal_group(managed, signal.SIGKILL)
            managed.process.wait()

    def wait(self, execution_id: str) -> ManagedProcess:
        managed = self._processes[execution_id]
        while True:
            with managed.state_lock:
                if managed.state != "RUNNING":
                    return managed
                code = managed.process.poll()
                now = time.monotonic()
                managed.last_heartbeat = now
                managed.peak_rss_kb = max(managed.peak_rss_kb, self._sample_group_rss_kb(managed.process.pid))
                if code is not None:
                    managed.exit_code = code
                    if self._group_has_live_process(managed.process.pid):
                        self._stop_group(managed, "worker exited with child processes still alive")
                        managed.state = "FAILED"
                    else:
                        managed.state = "SUCCEEDED" if code == 0 else "FAILED"
                    return managed
                if now >= managed.deadline_monotonic:
                    self._stop_group(managed, "timeout")
                    managed.exit_code = managed.process.returncode
                    managed.state = "TIMED_OUT"
                    return managed
            time.sleep(min(self.poll_seconds, max(0.001, managed.deadline_monotonic - time.monotonic())))

    def cancel(self, execution_id: str) -> ManagedProcess:
        managed = self._processes[execution_id]
        with managed.state_lock:
            if managed.state == "RUNNING":
                self._stop_group(managed, "cancelled")
                managed.exit_code = managed.process.returncode
                managed.state = "CANCELLED"
            return managed

    def status(self, execution_id: str) -> Mapping[str, object]:
        managed = self._processes[execution_id]
        with managed.state_lock:
            return {"execution_id": execution_id, "started": True,
                "process_alive": managed.process.poll() is None,
                "last_heartbeat_monotonic": managed.last_heartbeat, "state": managed.state,
                "result_committed": managed.result_committed, "pid": managed.process.pid,
                "peak_rss_kb": managed.peak_rss_kb}

    def output(self, execution_id: str) -> tuple[bytes, bytes]:
        managed = self._processes[execution_id]
        out, err = managed.stdout_file, managed.stderr_file
        out.flush(); err.flush(); out.seek(0); err.seek(0)
        return out.read(), err.read()

    def forget(self, execution_id: str) -> None:
        managed = self._processes.pop(execution_id)
        managed.stdout_file.close(); managed.stderr_file.close()
