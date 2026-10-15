"""Durable job records, host admission blocks, the dispatch gate, the host
identity and the per-host event log.

All state lives under the configured ``state_dir`` on local storage with
user-private permissions.  It is metadata only: the guard's lock (the
configured guard home's ``lock``) is never created, moved or unlinked here.

The event log (``<state>/events/<host>.jsonl``, campaign G decision D11) is
derived data: one JSON line per lifecycle transition, appended beside the
``state.json`` update that records it.  ``state.json`` stays authoritative;
nothing reads the log for a decision, and a failure to append to it never
changes one.
"""

from __future__ import annotations

import contextlib
import datetime as _dt
import fcntl
import json
import os
import secrets
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

from .fsprobe import is_dir_strict, listdir_strict

MANIFEST_SCHEMA = "agent-compute-manifest/1"
STATE_SCHEMA = "agent-compute-state/1"
EVENT_SCHEMA = "agent-compute-event/1"
HOST_ID_SCHEMA = "agent-compute-host-id/1"
HOST_ID_FILE = "host-id.json"

# Phases before a terminal outcome.
# `launched` (G2a): the guard has begun launching the workload, whose
# identity is not captured yet; `executing` once it is.
PHASES = ("accepted", "queued", "dispatched", "dispatch-held",
          "admission-wait", "launched", "executing", "cleanup")
# Terminal outcomes.  Only `unknown` leaves the host blocked.
OUTCOMES = ("succeeded", "failed", "launch-failed", "refused",
            "admission-timeout", "held-inputs-changed", "cancelled",
            "timed-out", "killed-resource", "interrupted", "unknown")
# Outcomes that guarantee the workload never started (safe to resubmit).
NEVER_STARTED = ("refused", "admission-timeout", "held-inputs-changed",
                 "launch-failed")


def utc_now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_job_id() -> str:
    stamp = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"acs-{stamp}-{secrets.token_hex(3)}"


def private_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)
    return path


def history_item(actor: str, **fields) -> dict:
    """One ``history[]`` entry: what happened (``phase``, ``outcome`` or
    ``event``), when (UTC and this host's monotonic clock) and who wrote
    it (``runner:<pid>``, ``cli:<pid>``, ``reconcile:<pid>``)."""
    return {"utc": utc_now(), "monotonic_s": time.monotonic(),
            "actor": f"{actor}:{os.getpid()}", **fields}


_BOOT_ID: list = []


def boot_id() -> str | None:
    """This boot's identity (Linux: the kernel's boot_id; macOS: the boot
    session uuid), read once per process; None when it cannot be read."""
    if not _BOOT_ID:
        value = None
        try:
            if sys.platform.startswith("linux"):
                value = Path("/proc/sys/kernel/random/boot_id").read_text(
                ).strip() or None
            elif sys.platform == "darwin":
                out = subprocess.run(["sysctl", "-n", "kern.bootsessionuuid"],
                                     capture_output=True, text=True,
                                     timeout=10)
                value = (out.stdout.strip() or None) \
                    if out.returncode == 0 else None
        except (OSError, subprocess.SubprocessError):
            value = None
        _BOOT_ID.append(value)
    return _BOOT_ID[0]


def read_host_id(state_dir: Path) -> "dict | Unreadable | None":
    """The host identity record; None when not created yet; an
    ``Unreadable`` when it exists but cannot be used."""
    path = Path(state_dir) / HOST_ID_FILE
    record = read_json(path)
    if isinstance(record, dict) and (
            not isinstance(record.get("host_id"), str) or
            not record["host_id"]):
        return Unreadable(path, "wrong shape: no host_id")
    return record


def ensure_host_id(state_dir: Path, host: str) -> "dict | Unreadable":
    """This installation's persistent host id, created on first use (a
    random uuid, ``O_EXCL``, mode 0600) and never replaced: an unreadable
    record is returned as such (C4), not overwritten."""
    state_dir = Path(state_dir)
    record = read_host_id(state_dir)
    if record is not None:
        return record
    private_dir(state_dir)
    path = state_dir / HOST_ID_FILE
    data = {"schema": HOST_ID_SCHEMA, "host_id": uuid.uuid4().hex,
            "created_utc": utc_now(), "created_host": host}
    # a temp file of its own (concurrent first uses, threads of one process
    # included), then a hard link: never replaces an existing record
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp",
                                dir=state_dir)
    temp = Path(name)
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(json.dumps(data, indent=1, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp, 0o600)
        os.link(temp, path)
    except FileExistsError:
        pass                            # another first use won the race
    finally:
        temp.unlink(missing_ok=True)
    found = read_host_id(state_dir)
    return found if found is not None else Unreadable(path, "vanished")


def host_id_of(state_dir: Path) -> str | None:
    record = read_host_id(state_dir)
    return record.get("host_id") if isinstance(record, dict) else None


def warm_event_identity() -> None:
    """Read (once per process) what every event line carries and costs a
    subprocess, so that ``log_event`` never runs one under the lock."""
    try:
        boot_id()
        from .identity import identity
        identity()
    except Exception:  # noqa: BLE001 - derived data never decides
        pass


def events_path(state_dir: Path, host: str) -> Path:
    return Path(state_dir) / "events" / f"{_safe(host)}.jsonl"


def append_event(state_dir: Path, host: str, record: dict) -> bool:
    """Append one line to the host's event log.  Derived data: any failure
    is swallowed (False) and changes nothing else."""
    try:
        line = json.dumps(record, sort_keys=True, default=json_default) + "\n"
        path = events_path(state_dir, host)
        private_dir(path.parent)
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            # a damaged last line (no newline) never swallows this one
            size = os.fstat(fd).st_size
            if size and os.pread(fd, 1, size - 1) != b"\n":
                line = "\n" + line
            os.write(fd, line.encode())
        finally:
            os.close(fd)
        return True
    except (OSError, ValueError, TypeError):
        return False


class Unreadable:
    """A record that exists (or cannot be shown not to) but cannot be used:
    an I/O or permission error, a directory at the path, bytes that are not
    UTF-8 JSON (a truncated or an empty file), or JSON of the wrong shape.

    C4: never read as absent.  It is not a dict, so code that needs the
    contents must check for it and fail closed with ``reason``; and it is
    always true, so ``if record:`` treats it as present (a block or a
    dispatch gate that cannot be read restricts like one that can)."""

    __slots__ = ("path", "why")

    def __init__(self, path, why: str):
        self.path = Path(path)
        self.why = why

    def __bool__(self) -> bool:
        return True

    def __repr__(self) -> str:
        return f"Unreadable({str(self.path)!r}, {self.why!r})"

    @property
    def reason(self) -> str:
        return f"{self.path} is unreadable ({self.why})"

    def view(self) -> dict:
        return {"unreadable": self.why, "path": str(self.path)}


class RecordUnreadable(Exception):
    """A write that would replace an unreadable record, and so destroy what
    it may hold, was refused (C4)."""

    def __init__(self, record: Unreadable):
        super().__init__(record.reason)
        self.record = record


def json_default(value):
    """``json.dumps`` hook: an ``Unreadable`` is written as its view."""
    if isinstance(value, Unreadable):
        return value.view()
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


def write_json(path: Path, data: dict) -> None:
    """Atomic, user-private JSON write."""
    private_dir(path.parent)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write(json.dumps(data, indent=1, sort_keys=True,
                                default=json_default) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)


def read_json(path: Path) -> dict | Unreadable | None:
    """The JSON object at ``path``; ``None`` only when the file does not
    exist; an ``Unreadable`` for anything else that is not a JSON object
    (C4: an unreadable record is never read as an absent one)."""
    try:
        raw = Path(path).read_bytes()
    except FileNotFoundError:
        return None
    except OSError as error:
        return Unreadable(path, f"{type(error).__name__}: "
                                f"{error.strerror or error}")
    if not raw.strip():
        return Unreadable(path, "empty file")
    try:
        data = json.loads(raw.decode("utf-8"))
    except UnicodeDecodeError:
        return Unreadable(path, "not UTF-8")
    except ValueError as error:
        return Unreadable(path, f"not valid JSON ({error})")
    if not isinstance(data, dict):
        return Unreadable(path, f"wrong shape: a JSON {type(data).__name__}, "
                                "not an object")
    return data


@contextlib.contextmanager
def state_lock(state_dir: Path):
    """Serialise mutations of ACS metadata (never the compute lock)."""
    private_dir(state_dir)
    fd = os.open(state_dir / "state.lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


class JobStore:
    def __init__(self, state_dir: Path, actor: str = "cli"):
        # the per-process identity of the event lines (the boot id: sysctl
        # on macOS; a checkout's source sha: git), read here, never under
        # the state lock (review round 1)
        warm_event_identity()
        self.state_dir = state_dir
        self.jobs = state_dir / "jobs"
        self.requests = state_dir / "requests"
        self.blocks = state_dir / "blocks"
        # who writes the transitions (history entries, event lines)
        self.actor = actor

    def history(self, **fields) -> dict:
        return history_item(self.actor, **fields)

    def log_event(self, job_id: str, state, item: dict) -> None:
        """Append the event line for one history entry just written to the
        job's ``state.json`` (D11).  Never raises; never read back."""
        try:
            state = state if isinstance(state, dict) else {}
            man = self.manifest(job_id)
            man = man if isinstance(man, dict) else {}
            host = man.get("host") or state.get("host")
            if not isinstance(host, str) or not host:
                return
            from .identity import identity
            ident = identity()
            event = item.get("event") or (
                "phase" if "phase" in item else
                "outcome" if "outcome" in item else "update")
            append_event(self.state_dir, host, {
                "schema": EVENT_SCHEMA, "utc": item.get("utc") or utc_now(),
                "monotonic_s": item.get("monotonic_s", time.monotonic()),
                "boot_id": boot_id(), "job_id": job_id,
                "request_id": man.get("request_id",
                                      state.get("request_id")),
                "host": host, "host_id": host_id_of(self.state_dir),
                "version": ident["version"],
                "source_sha": ident["source_sha"], "event": event,
                "phase": item.get("phase", state.get("phase")),
                "outcome": item.get("outcome", state.get("outcome")),
                "reason": item.get("reason", state.get("reason")),
                "actor": item.get("actor") or
                f"{self.actor}:{os.getpid()}"})
        except Exception:  # noqa: BLE001 - derived data never decides
            pass

    def job_dir(self, job_id: str) -> Path:
        if not job_id.startswith("acs-") or "/" in job_id or ".." in job_id:
            raise ValueError(f"not an agent-compute job id: {job_id!r}")
        return self.jobs / job_id

    def manifest(self, job_id: str) -> dict | Unreadable | None:
        return read_json(self.job_dir(job_id) / "manifest.json")

    def state(self, job_id: str) -> dict | Unreadable | None:
        return read_json(self.job_dir(job_id) / "state.json")

    def update_state(self, job_id: str, **changes) -> dict:
        """Merge ``changes`` into the job's state.  An unreadable state is
        never replaced (``RecordUnreadable``): it may hold a runner claim or
        an outcome."""
        with state_lock(self.state_dir):
            path = self.job_dir(job_id) / "state.json"
            state = read_json(path)
            if isinstance(state, Unreadable):
                raise RecordUnreadable(state)
            state = state or {"schema": STATE_SCHEMA, "job_id": job_id,
                              "history": []}
            phase = changes.get("phase")
            outcome = changes.get("outcome")
            items = []
            if phase and phase != state.get("phase"):
                items.append(self.history(phase=phase,
                                          reason=changes.get("reason")))
            if outcome and outcome != state.get("outcome"):
                items.append(self.history(outcome=outcome,
                                          reason=changes.get("reason")))
            state.setdefault("history", []).extend(items)
            state.update(changes)
            state["updated_utc"] = utc_now()
            write_json(path, state)
            for item in items:
                self.log_event(job_id, state, item)
            return state

    def all_ids(self) -> list[str]:
        """Every job id on disk; ``[]`` only when the jobs dir does not
        exist.  A jobs dir that cannot be examined or listed raises
        ``OSError`` (A49: never "no jobs": every unfinished-job scan, the
        one-heavy-job reservation and the host-label checks start here)."""
        if not is_dir_strict(self.jobs):
            return []
        return [name for name in listdir_strict(self.jobs)
                if name.startswith("acs-")]

    # -- request ids (resubmission after an interrupted session) -----------
    def request_job(self, request_id: str) -> str | Unreadable | None:
        """The job a request id is bound to; None when unbound; an
        ``Unreadable`` when the binding exists but cannot be read (C4: the
        id is then refused, never bound again)."""
        path = self.requests / f"{_safe(request_id)}.json"
        record = read_json(path)
        if not isinstance(record, dict):
            return record
        job = record.get("job_id")
        if not isinstance(job, str) or not job:
            return Unreadable(path, "wrong shape: no job id")
        recorded = record.get("request_id")
        if recorded != request_id:
            # A32: a binding of another id sharing the file name (written
            # before request ids were validated, e.g. `a b` for `a_b`) never
            # answers this id: refused, never bound again
            return Unreadable(path, f"it binds request id {recorded!r}, "
                                    f"not {request_id!r} (another id with "
                                    "the same file name)")
        return job

    def bind_request(self, request_id: str, job_id: str) -> None:
        write_json(self.requests / f"{_safe(request_id)}.json",
                   {"request_id": request_id, "job_id": job_id,
                    "utc": utc_now()})

    def unbind_request(self, request_id: str) -> None:
        """Caller holds the state lock and has checked the binding."""
        (self.requests / f"{_safe(request_id)}.json").unlink(missing_ok=True)

    # -- host admission block -----------------------------------------------
    def block_path(self, host: str) -> Path:
        return self.blocks / f"{_safe(host)}.json"

    def block(self, host: str) -> dict | Unreadable | None:
        """The host's admission block; None when there is none; an
        ``Unreadable`` (true: the host counts as blocked) when the file
        exists but cannot be read or has no ``jobs`` object (C4)."""
        path = self.block_path(host)
        record = read_json(path)
        if isinstance(record, dict) and \
                not isinstance(record.get("jobs"), dict):
            return Unreadable(path, "wrong shape: no 'jobs' object")
        return record

    def set_block(self, host: str, job_id: str, reason: str,
                  evidence: dict) -> dict:
        """Add ``job_id`` to the host block.  ``RecordUnreadable`` when the
        block file exists but cannot be read: it is never replaced, and the
        host stays blocked by it."""
        with state_lock(self.state_dir):
            return self._add_block(host, job_id, reason, evidence)

    def block_if_unfinished(self, host: str, job_id: str, runner: dict,
                            reason: str, evidence: dict) -> bool:
        """Block the host for a lost job only if, re-read under the state
        lock, the job is still unfinished and still claimed by ``runner``
        (the identity found gone).  A runner that wrote its outcome, or was
        replaced, between the caller's read and its liveness check is not
        lost.  Only that positive evidence skips the block: an unreadable
        state still blocks.  An unreadable block file is never replaced; the
        host is blocked by it already, so this returns True."""
        with state_lock(self.state_dir):
            state = read_json(self.job_dir(job_id) / "state.json")
            if isinstance(state, dict) and (
                    state.get("outcome") or state.get("runner") != runner):
                return False
            try:
                self._add_block(host, job_id, reason, evidence)
            except RecordUnreadable:
                pass
            return True

    def block_if_unreadable(self, host: str, job_id: str, reason: str,
                            evidence: dict) -> bool:
        """C4: block the host for a job whose state cannot be read (it may
        hide a live runner, or an end that was never verified), if, re-read
        under the state lock, it still cannot be read.  An unreadable block
        file is never replaced; it already blocks (True)."""
        with state_lock(self.state_dir):
            state = read_json(self.job_dir(job_id) / "state.json")
            if not isinstance(state, Unreadable):
                return False
            try:
                self._add_block(host, job_id, reason,
                                {**evidence, "state": state.view()})
            except RecordUnreadable:
                pass
            return True

    def _add_block(self, host: str, job_id: str, reason: str,
                   evidence: dict) -> dict:
        """Caller holds the state lock.  Never replaces an unreadable block
        file (``RecordUnreadable``): that would drop every job it holds."""
        current = self.block(host)
        if isinstance(current, Unreadable):
            raise RecordUnreadable(current)
        current = current or {"host": host, "created_utc": utc_now(),
                              "jobs": {}}
        current["jobs"][job_id] = {"reason": reason, "evidence": evidence,
                                   "utc": utc_now()}
        current["updated_utc"] = utc_now()
        write_json(self.block_path(host), current)
        return current

    def clear_block_job(self, host: str, job_id: str
                        ) -> dict | Unreadable | None:
        """Remove one job from the host block.  An unreadable block file is
        left as it is (and returned): only an operator repairs it."""
        with state_lock(self.state_dir):
            current = self.block(host)
            if isinstance(current, Unreadable):
                return current
            if not current:
                return None
            current["jobs"].pop(job_id, None)
            if current["jobs"]:
                current["updated_utc"] = utc_now()
                write_json(self.block_path(host), current)
                return current
            self.block_path(host).unlink()
            return None

    # -- dispatch gate (no automatic start after a daemon restart) ---------
    @property
    def gate_path(self) -> Path:
        return self.state_dir / "dispatch-gate.json"

    def gate(self) -> dict | Unreadable | None:
        """The dispatch gate record; None when the gate is open; an
        ``Unreadable`` (true: dispatch is held) when it exists but cannot be
        read (C4)."""
        return read_json(self.gate_path)

    def close_gate(self, reason: str) -> None:
        write_json(self.gate_path, {"closed_utc": utc_now(), "reason": reason})

    def open_gate(self) -> None:
        with contextlib.suppress(FileNotFoundError):
            self.gate_path.unlink()


def _safe(name: str) -> str:
    keep = "".join(c if c.isalnum() or c in "-_.:" else "_" for c in name)
    if not keep or keep.startswith("."):
        raise ValueError(f"unusable identifier {name!r}")
    return keep
