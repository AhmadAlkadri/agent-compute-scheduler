"""Process identity and cleanup evidence.

A pid alone is not an identity: every liveness decision pairs it with a
start-time discriminator, so a recycled pid is never mistaken for the
recorded process.  The exact discriminators are independent of the
checker's time zone and locale:

* Linux: ``start = "ticks:N"``, the start time in clock ticks since boot
  (``/proc/<pid>/stat`` field 22), plus ``boot_id``;
* macOS: ``start_utc_s``, the kernel start time in whole UTC seconds, read
  with ``ps -o lstart`` under ``TZ=UTC0 LC_ALL=C``.  ``start`` is left
  empty on purpose, so a pre-fix reader (which compares ``start`` strings
  rendered in its own TZ) falls back to the TZ-independent ``start_epoch``;
* legacy macOS (recorded by 0.1.0-eb369eb937 and earlier): ``start =
  "lstart:<local wall time>"`` rendered in the recorder's (unknown) zone,
  and ``start_epoch`` = ``mktime`` of it, which is an hour off in the DST
  fall-back hour.

``match`` is four-valued: ``alive``; ``gone`` (no live process at the pid);
``recycled`` (a live process holds the pid and provably started AFTER the
recorded one, beyond tolerance, against an exact recorded start); and
``unknown`` (anything else: a failed ``ps``, an epoch-only identity that
does not match, a mismatch in the wrong direction or within the zone
uncertainty of a legacy string).  ``liveness`` folds ``recycled`` into
``gone``.  A failed ``ps`` is ``unknown``, never ``gone``.

Host venue: the workload's process group (and, on Linux, its session) is
the tracked unit; the guard launches it with ``start_new_session=True``, so
its pid is the group and session leader.  Limit: a descendant that calls
``setsid()`` leaves the tracked group.

Container venue: see ``container_job_evidence`` -- the guard's job
marker (``ACS_JOB_MARKER``, the legacy ``SMC_GUARD_JOB`` and any alias the
job's manifest records) in each process's environ, plus the sessions and
groups of the marker holders and of the recorded leader; any unmarked
process (not provably older than the job) in a session or group whose
leader is gone makes the evidence unverifiable.  Limit: a descendant that
both drops the marker (``env -i``, a fresh-environment exec, an overwritten
environ area) and calls ``setsid()`` leads a live, unmarked session of its
own and escapes.

Either way this is sampled evidence, not containment (no cgroup).
"""

from __future__ import annotations

import calendar
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from .fsprobe import exists_strict

DOCKER_TIMEOUT_S = 30
# A20: every ACS in-container helper runs from "/" (never the container's
# WorkingDir, which may be a hung bind mount) as `python3 -I -S` (no cwd or
# PYTHONPATH on sys.path, no site), and its first statement arms an alarm
# below the host-side docker timeout.  The alarm ends killable waits; a
# true D-state wait (or a hang before python starts) is bounded only by
# the host timeout, and the evidence then fails closed.  Same values as
# the internal guard's (guard/engine.py, which is pinned and not shared).
CONTAINER_EXEC_WORKDIR = "/"
SCRIPT_ALARM_S = 20
# docker exec's exit status for a helper killed by its SIGALRM (14 on Linux)
ALARM_EXIT = 128 + 14
EPOCH_TOLERANCE_S = 3.0
# A legacy ``lstart:`` string is local wall time in an unknown zone.  Every
# real UTC offset lies in [-12 h, +14 h], so the true start is at most 12 h
# after the string read as UTC (and at most 14 h before it).
LEGACY_LOCAL_BEHIND_S = 12 * 3600
LEGACY_LOCAL_AHEAD_S = 14 * 3600
ALIVE, GONE, UNKNOWN, RECYCLED = "alive", "gone", "unknown", "recycled"
LSTART_FORMAT = "%a %b %d %H:%M:%S %Y"


class ProcCheckError(Exception):
    """The process table could not be read."""


def _ps_env(local: bool = False) -> dict[str, str]:
    """A controlled environment for ``ps``: C locale, and UTC unless
    ``local`` (the system zone, as a pre-fix runner rendered ``lstart``)."""
    env = {"PATH": os.environ.get("PATH") or "/usr/bin:/bin", "LC_ALL": "C"}
    if not local:
        env["TZ"] = "UTC0"
    return env


def _ps(args: list[str], local: bool = False) -> str:
    try:
        out = subprocess.run(["ps", *args], capture_output=True, text=True,
                             timeout=30, env=_ps_env(local))
    except (OSError, subprocess.SubprocessError) as error:
        raise ProcCheckError(f"ps failed: {error}") from error
    if out.returncode not in (0, 1):
        raise ProcCheckError(f"ps exited {out.returncode}")
    return out.stdout


def _linux_boot_epoch() -> float:
    for line in Path("/proc/stat").read_text().splitlines():
        if line.startswith("btime "):
            return float(line.split()[1])
    raise ProcCheckError("no btime in /proc/stat")


def start_info(pid: int | None) -> tuple[str, float] | None:
    """(exact start discriminator, start epoch) or None if no such live
    process.  Raises ProcCheckError when the table cannot be read."""
    if not pid:
        return None
    pid = int(pid)
    if sys.platform.startswith("linux"):
        try:
            text = Path(f"/proc/{pid}/stat").read_text()
        except (FileNotFoundError, ProcessLookupError):
            return None
        except OSError as error:
            try:
                if not exists_strict(f"/proc/{pid}"):
                    return None
            except OSError as probe:
                # A49: not "no such process" when /proc/<pid> itself
                # cannot be examined
                raise ProcCheckError(str(probe)) from probe
            raise ProcCheckError(str(error)) from error
        fields = text.rsplit(")", 1)[1].split()
        if fields[0] in ("Z", "X") and not _live_task(pid):
            return None     # a zombie is not live, unless (main thread
            # exited via pthread_exit) another of its threads still runs
        ticks = int(fields[19])
        epoch = _linux_boot_epoch() + ticks / os.sysconf("SC_CLK_TCK")
        return f"ticks:{ticks}", epoch
    lstart = _lstart(pid)
    if lstart is None:
        return None
    try:
        epoch = calendar.timegm(time.strptime(lstart, LSTART_FORMAT))
    except ValueError as error:
        raise ProcCheckError(f"unparsable lstart {lstart!r}") from error
    return f"utc:{epoch}", float(epoch)


def _lstart(pid: int, local: bool = False) -> str | None:
    """macOS: the ``ps lstart`` of a live pid, whitespace-normalised,
    rendered in UTC (or, if ``local``, in the system zone)."""
    out = _ps(["-o", "stat=,lstart=", "-p", str(pid)], local=local).strip()
    if not out:
        return None
    stat, _, lstart = out.partition(" ")
    if stat.startswith("Z"):
        return None
    return " ".join(lstart.split())


def _boot_id() -> str | None:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        return None


def _live_task(pid: int) -> bool:
    """Linux: does any thread of ``pid`` other than a zombie main thread
    still run?"""
    try:
        tids = os.listdir(f"/proc/{pid}/task")
    except OSError:
        return False
    for tid in tids:
        try:
            text = Path(f"/proc/{pid}/task/{tid}/stat").read_text()
        except OSError:
            continue
        if text.rsplit(")", 1)[1].split()[0] not in ("Z", "X"):
            return True
    return False


def start_time(pid: int | None) -> str | None:
    info = start_info(pid)
    return info[0] if info else None


def identity(pid: int | None) -> dict | None:
    try:
        info = start_info(pid)
    except ProcCheckError:
        return {"pid": int(pid), "start": None, "start_epoch": None,
                "note": "start time unavailable"} if pid else None
    if info is None:
        return None
    try:
        pgid = os.getpgid(int(pid))
        sid = os.getsid(int(pid))
    except OSError:
        pgid = sid = None
    ident = {"pid": int(pid), "start": info[0], "start_epoch": info[1],
             "pgid": pgid, "sid": sid}
    if info[0].startswith("utc:"):
        # see the module docstring: ``start`` stays empty for old readers
        ident["start"] = None
        ident["start_utc_s"] = int(info[0][4:])
    elif info[0].startswith("ticks:"):
        ident["boot_id"] = _boot_id()
    return ident


def _int(value) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _number(value) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


# A recorded start_utc_s is trusted only if it agrees with start_epoch
# (both are written from the same reading): anything else is not exact.
UTC_S_PLAUSIBLE_S = 13 * 3600


def _utc_s(ident: dict) -> int | None:
    """The recorded macOS exact start, or None if absent or implausible
    (<= 0, no start_epoch, or more than 13 h from start_epoch)."""
    value = _int(ident.get("start_utc_s"))
    epoch = _number(ident.get("start_epoch"))
    if value is None or value <= 0 or epoch is None or \
            abs(value - epoch) > UTC_S_PLAUSIBLE_S:
        return None
    return value


def has_exact_start(ident: dict | None) -> bool:
    """Does the identity carry an exact start discriminator (not merely an
    epoch taken from the guard's ``started_utc``)?"""
    if not isinstance(ident, dict):
        return False
    start = ident.get("start")
    if isinstance(start, str) and start.startswith(("ticks:", "lstart:")):
        return True
    return _utc_s(ident) is not None


def match(ident: dict | None) -> str:
    """ALIVE / GONE / RECYCLED / UNKNOWN for a recorded process identity
    (see the module docstring)."""
    if not isinstance(ident, dict) or not ident.get("pid"):
        return GONE
    try:
        info = start_info(ident["pid"])
    except ProcCheckError:
        return UNKNOWN
    if info is None:
        return GONE
    return _compare(ident, info)


def liveness(ident: dict | None) -> str:
    """ALIVE / GONE / UNKNOWN: whether the recorded process runs.  A pid now
    held by a provably later process is GONE; any other mismatch is
    UNKNOWN."""
    state = match(ident)
    return GONE if state == RECYCLED else state


def _compare(ident: dict, info: tuple[str, float]) -> str:
    key, epoch_now = info
    start = ident.get("start") if isinstance(ident.get("start"), str) \
        else None
    if start and start.startswith("ticks:"):
        return _compare_ticks(ident, start, key)
    utc_s = _utc_s(ident)
    if utc_s is not None:
        if not key.startswith("utc:"):
            return UNKNOWN
        now = int(key[4:])
        if now == utc_s:
            return ALIVE
        if now > utc_s + EPOCH_TOLERANCE_S:
            return RECYCLED
        return UNKNOWN
    if start and start.startswith("lstart:"):
        return _compare_legacy(ident, start, key)
    if start:
        return UNKNOWN      # an unrecognised discriminator
    epoch = _number(ident.get("start_epoch"))
    if epoch is not None and abs(epoch_now - epoch) <= EPOCH_TOLERANCE_S:
        return ALIVE        # inexact: see has_exact_start
    # an epoch-only identity (no exact start) never proves a recycle
    return UNKNOWN


def _compare_ticks(ident: dict, start: str, key: str) -> str:
    if not key.startswith("ticks:"):
        return UNKNOWN
    try:
        recorded, now = int(start[6:]), int(key[6:])
    except ValueError:
        return UNKNOWN
    if now == recorded:
        # C2: ticks restart at every boot, so equal ticks prove the same
        # process only on the same boot.  A different boot id is a later
        # process (the recorded one cannot survive a reboot); an unreadable
        # current boot id, or a record without one (legacy, or unreadable
        # at capture), cannot rule out a later boot
        boot = ident.get("boot_id")
        if not (isinstance(boot, str) and boot):
            return UNKNOWN
        current = _boot_id()
        if current is None:
            return UNKNOWN
        return ALIVE if current == boot else RECYCLED
    if now > recorded + EPOCH_TOLERANCE_S * os.sysconf("SC_CLK_TCK"):
        return RECYCLED                         # same boot, later process
    # ticks restart at boot: a smaller value is a later boot only if the
    # record proves it predates this boot
    boot = ident.get("boot_id")
    if isinstance(boot, str) and boot:
        current = _boot_id()
        # a later boot only if the current boot_id is READ and differs;
        # unreadable -> unknown, never recycled
        return RECYCLED if current is not None and current != boot \
            else UNKNOWN
    recorded_epoch = _number(ident.get("start_epoch"))      # pre-boot_id
    if now < recorded and recorded_epoch is not None:
        try:
            if recorded_epoch < _linux_boot_epoch() - EPOCH_TOLERANCE_S:
                return RECYCLED
        except (OSError, ValueError, ProcCheckError):
            pass
    return UNKNOWN


def _compare_legacy(ident: dict, start: str, key: str) -> str:
    """A pre-fix macOS record: local wall time in an unknown zone, and an
    epoch that may be an hour off (DST fall-back).  Never RECYCLED unless
    the process at the pid started after the latest instant the string can
    denote in any zone."""
    if not key.startswith("utc:"):
        return UNKNOWN
    now = int(key[4:])
    recorded = _number(ident.get("start_epoch"))
    if recorded is not None and now == round(recorded):
        return ALIVE
    try:
        if _lstart(int(ident["pid"]), local=True) == start[7:]:
            return ALIVE        # the pre-fix comparison, in the system zone
    except ProcCheckError:
        pass
    try:
        naive = calendar.timegm(time.strptime(start[7:], LSTART_FORMAT))
    except ValueError:
        return UNKNOWN
    if now > naive + LEGACY_LOCAL_BEHIND_S + EPOCH_TOLERANCE_S:
        return RECYCLED
    return UNKNOWN


def same_process(ident: dict | None) -> bool:
    return liveness(ident) == ALIVE


def group_members(leader: int, min_epoch: float | None = None) -> list[int]:
    """Live pids whose process group (or Linux session) is ``leader``,
    optionally only those started at or after ``min_epoch``."""
    linux = sys.platform.startswith("linux")
    columns = "stat=,pid=,pgid=,sid=" if linux else "stat=,pid=,pgid="
    members = []
    for line in _ps(["-A", "-o", columns]).splitlines():
        parts = line.split()
        if len(parts) < 3 or not parts[1].isdigit():
            continue
        if parts[0].startswith("Z") and not (
                linux and _live_task(int(parts[1]))):
            continue
        pid, rest = int(parts[1]), [int(p) for p in parts[2:] if p.isdigit()]
        if leader not in rest:
            continue
        if min_epoch is not None:
            try:
                info = start_info(pid)
            except ProcCheckError:
                info = ("?", float("inf"))
            if info is None or info[1] < min_epoch - EPOCH_TOLERANCE_S:
                continue
        members.append(pid)
    return members


def host_survivors(workload: dict | None) -> dict:
    """Evidence about a host workload after its supervisor finished.

    ``clean`` is True only when the leader is provably gone and no process
    remains in its group.  ``signalable`` says whether a targeted signal
    to that group is justified by the evidence."""
    if not isinstance(workload, dict) or not valid_pid(workload.get("pid")):
        return {"verified": False, "clean": False,
                "reason": "no valid workload identity recorded"}
    leader = workload["pid"]
    state = match(workload)
    if state == RECYCLED:
        # A process that provably started after the recorded leader holds
        # its pid.  POSIX never reuses a pid while a process group (or
        # session) with that id exists, so the recorded group is empty; a
        # group with that id now is the new process's, not ours.
        return {"verified": True, "clean": True, "members": [],
                "leader_alive": False,
                "note": "leader pid recycled: the process now at it "
                        "started after the recorded leader"}
    try:
        # no start-time filter: a member cannot predate its setsid leader,
        # and a clock step after the capture must not hide live members
        members = group_members(leader)
    except ProcCheckError as error:
        return {"verified": False, "clean": False, "reason": str(error)}
    if state == UNKNOWN:
        return {"verified": False, "clean": False, "members": members,
                "reason": "leader pid exists but cannot be matched to the "
                          "recorded start time (or the process table "
                          "cannot be read)"}
    exact = has_exact_start(workload)
    evidence = {"verified": True, "leader_alive": state == ALIVE,
                "members": members,
                "clean": state == GONE and not members,
                # a signal needs an exact-start live leader, or a group
                # whose leader pid is free (so the group can only be ours)
                "signalable": (state == ALIVE and exact) or
                              (state == GONE and bool(members))}
    if state == ALIVE and not exact:
        evidence["reason"] = ("leader matched only by the guard's start "
                              "epoch (no exact start recorded): not "
                              "signalled; inspect it by hand")
    return evidence


def signal_host_group(workload: dict, sig: int,
                      seen: dict | None = None) -> bool:
    """Signal the identified workload; never a name match, never an
    unproven pid.

    * Exact-start live leader: ``killpg`` its group.
    * Leader pid free, members left: never ``killpg`` (had our group
      emptied and the pid been reused by a process that double-forked, its
      child would sit in a group with our id).  ``seen`` is the runner's
      last proof that the exact-start leader was alive
      (``workload_seen_utc``, and on Linux ``workload_seen_ticks`` /
      ``workload_seen_boot_id``).  At that moment group P was ours and its
      id could not be reused.  A member that started strictly before that
      proof (minus 1 s: the macOS start resolution) and at or after the
      recorded leader start, and that is still in group P with the same
      start, has kept the id occupied ever since, so it is ours.  Such
      members are signalled one by one, each re-proven right before
      ``os.kill``.  Without a proof, nothing is signalled.  A host block's
      time proves nothing: a lost-supervision block can be written long
      after the group emptied and its id was reused."""
    evidence = host_survivors(workload)
    if not evidence.get("verified") or not evidence.get("signalable"):
        return False
    leader = int(workload["pid"])
    if evidence.get("leader_alive"):
        try:
            os.killpg(leader, sig)
            return True
        except (ProcessLookupError, PermissionError):
            return False
    return _signal_members(workload, evidence.get("members") or [], sig,
                           seen) > 0


def uptime_ticks() -> int | None:
    """Linux: clock ticks since boot now (monotonic, immune to wall-clock
    steps), for comparison with process start ticks; else None."""
    if not sys.platform.startswith("linux"):
        return None
    try:
        seconds = float(Path("/proc/uptime").read_text().split()[0])
    except (OSError, ValueError, IndexError):
        return None
    return int(seconds * os.sysconf("SC_CLK_TCK"))


def seen_mark() -> dict:
    """Taken BEFORE a liveness proof, so the recorded moment is never
    later than the proof itself."""
    return {"workload_seen_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                               time.gmtime()),
            "workload_seen_ticks": uptime_ticks(),
            "workload_seen_boot_id": _boot_id()
            if sys.platform.startswith("linux") else None}


def member_signal_refusal(workload: dict, seen: dict | None) -> str | None:
    """Why the members of a group whose leader pid is free cannot be
    signalled at all (None: they can be, subject to the per-member proof)."""
    start = workload.get("start")
    if isinstance(start, str) and start.startswith("ticks:") and (
            _same_boot(workload.get("boot_id")) is False or
            (isinstance(seen, dict) and
             _same_boot(seen.get("workload_seen_boot_id")) is False)):
        return ("the leader pid is free and the recorded boot_id does not "
                "match the current one, or the current boot_id cannot be "
                "read: the recorded start ticks cannot be compared, so no "
                "member is signalled; inspect them by hand")
    if _leader_bound(workload) is None:
        return ("the leader pid is free and its recorded start is not exact "
                "(epoch-only or legacy record): members are not signalled; "
                "inspect them by hand")
    if _seen_bound(workload, seen) is None:
        return ("the leader pid is free and there is no usable "
                "workload_seen_utc (the runner's last proof that the leader "
                "was alive): members cannot be proven to predate a reuse of "
                "the group id, so none is signalled; inspect them by hand")
    return None


def _same_boot(recorded) -> bool | None:
    """True/False if a recorded boot_id matches the current one; None if
    nothing was recorded.  An unreadable current boot_id is never 'the
    same boot' (False)."""
    if not (isinstance(recorded, str) and recorded):
        return None
    current = _boot_id()
    return current is not None and current == recorded


def _leader_bound(workload: dict) -> tuple[str, int] | None:
    start = workload.get("start")
    if isinstance(start, str) and start.startswith("ticks:"):
        if _same_boot(workload.get("boot_id")) is False:
            return None     # another boot, or the current boot unreadable
        try:
            return "ticks", int(start[6:])
        except ValueError:
            return None
    utc_s = _utc_s(workload)
    return ("utc", utc_s) if utc_s is not None else None


def _seen_bound(workload: dict, seen: dict | None) -> tuple[str, float] | None:
    """The exclusive upper bound on a member's start: ("ticks", t) or
    ("epoch", e), one second before the last proof."""
    leader = _leader_bound(workload)
    if leader is None or not isinstance(seen, dict):
        return None
    if leader[0] == "ticks":
        ticks = _int(seen.get("workload_seen_ticks"))
        boot = seen.get("workload_seen_boot_id")
        same = _same_boot(boot)
        if same is False:
            return None     # proof from another boot, or boot unreadable
        if ticks is not None and same and \
                boot == (workload.get("boot_id") or boot):
            return "ticks", ticks - os.sysconf("SC_CLK_TCK")
    epoch = utc_to_epoch(seen.get("workload_seen_utc"))
    if epoch is None:
        return None
    return "epoch", epoch - 1.0


def _member_in_window(info, leader: tuple[str, int],
                      upper: tuple[str, float]) -> bool:
    if info is None:
        return False
    key, epoch = info
    kind, leader_start = leader
    if kind == "ticks":
        if not key.startswith("ticks:"):
            return False
        ticks = int(key[6:])
        if ticks < leader_start:
            return False
        if upper[0] == "ticks":
            return ticks < upper[1]
        return epoch < upper[1]        # wall clock: see the docs' limit
    if not key.startswith("utc:"):
        return False
    start = int(key[4:])
    return leader_start <= start and start < upper[1]


def _signal_members(workload: dict, members: list, sig: int,
                    seen: dict | None) -> int:
    if member_signal_refusal(workload, seen):
        return 0
    leader_bound = _leader_bound(workload)
    upper = _seen_bound(workload, seen)
    leader = int(workload["pid"])
    sent = 0
    for pid in members:
        try:
            info = start_info(pid)
            if not _member_in_window(info, leader_bound, upper):
                continue
            # re-prove immediately before the signal
            if start_info(pid) != info or os.getpgid(pid) != leader:
                continue
            os.kill(pid, sig)
            sent += 1
        except (ProcCheckError, OSError, ValueError):
            continue
    return sent


# --- containers -----------------------------------------------------------------
#
# Evidence for a container-venue job never comes from the guard record
# alone.  The guard runs ``docker exec -e <marker>=<job_id> C setsid -w
# bash -c '...exec CMD'``; under ``docker exec`` that setsid forks, and
# ``-w`` waits only for its child (the new session leader, the pidfile's
# ``$$``, recorded as ``container_pid`` when the guard manages to read it).
# So "the docker client exited" says nothing about the leader's
# descendants.  What does survive fork, exec and setsid() is the job marker
# ``<marker>=<job_id>`` in every descendant's environ (G2a: a process is
# marked when it carries any of the job's marker names, which the manifest
# records, together with every name a version of ACS ever exported:
# ``KNOWN_MARKERS``; never fewer names, so never a false clean).  One in-container
# scan reads every live process; the job's set is the marker holders plus,
# to a fixpoint, every process sharing a member's session or process group
# or whose parent is a member (plus the recorded leader's session/group).
# A marker holder with an unmarked descendant is never signalled: that
# would erase the only link to the descendant.  An unmarked process that may postdate the job
# in a session or group whose leader is gone (every docker-exec'd process
# leads its own session, so a scrubbed descendant left behind by a finished
# leader looks exactly like this) makes the evidence unverifiable.  Limits:
# a descendant that both scrubs or overwrites its environment AND calls
# setsid() (so it leads a live session of its own) escapes; unrelated
# orphaned sessions started after the job block (fail-closed); there is no
# cgroup containment.

# Every job-marker name any version exported: the internal guard's primary
# name and the legacy one (an external guard's, and the operator's alias).
KNOWN_MARKERS = ("ACS_JOB_MARKER", "SMC_GUARD_JOB")
HAS_MARKER, NO_MARKER, UNREADABLE = "has-marker", "no-marker", "unreadable"


def job_markers(manifest) -> tuple[str, ...]:
    """The marker names a job was launched with, from its manifest
    (``marker`` and ``marker_aliases``, recorded since 0.2.0).  A manifest
    of an earlier version records none: its guard exported
    ``SMC_GUARD_JOB``."""
    man = manifest if isinstance(manifest, dict) else {}
    marker = man.get("marker")
    if not isinstance(marker, str) or not marker:
        return ("SMC_GUARD_JOB",)
    aliases = man.get("marker_aliases")
    aliases = [a for a in aliases if isinstance(a, str) and a] \
        if isinstance(aliases, list) else []
    return (marker, *[a for a in aliases if a != marker])


def scan_markers(manifest) -> tuple[str, ...]:
    """The names the evidence looks for: the job's own (``job_markers``)
    and every name a version of ACS exported (``KNOWN_MARKERS``).  More
    names can only find more of the job's processes."""
    names = list(job_markers(manifest))
    names += [n for n in KNOWN_MARKERS if n not in names]
    return tuple(names)


def _marker_names(markers) -> list[str]:
    names = [m for m in (markers or ()) if isinstance(m, str) and m]
    names += [n for n in KNOWN_MARKERS if n not in names]
    return names


def bounded(body: str, alarm_s: int = SCRIPT_ALARM_S) -> str:
    """An in-container helper script whose first statement arms its alarm
    (default disposition: the kernel kills it, whatever python is doing)."""
    return ("import signal\n"
            f"signal.alarm({int(alarm_s)})\n"
            "signal.signal(signal.SIGALRM, signal.SIG_DFL)\n" + body)


# Used only to capture the running leader's exact start ticks.
_CONTAINER_SCAN = bounded(r"""
import json, os, sys
target = int(sys.argv[1])
tck = os.sysconf('SC_CLK_TCK')
btime = None
for line in open('/proc/stat'):
    if line.startswith('btime '):
        btime = float(line.split()[1])
start, epoch = None, None
try:
    fields = open('/proc/%d/stat' % target).read().rsplit(')', 1)[1].split()
    if fields[0] not in ('Z', 'X'):
        start = 'ticks:' + fields[19]
        epoch = btime + int(fields[19]) / tck if btime else None
except OSError:
    pass
print(json.dumps({'start': start, 'epoch': epoch}))
""")

# Shared by the in-container helpers.  A tgid whose main thread called
# pthread_exit shows Z/X in /proc/<tgid>/stat while its other threads run:
# it is live if any task is not Z/X, and its environ is read through that
# task (os.kill/killpg on the tgid still reach it).
_LIVE_STAT = r"""
def stat(pid):
    try:
        f = open('/proc/%d/stat' % pid).read().rsplit(')', 1)[1].split()
    except OSError:
        return None, None
    if f[0] not in ('Z', 'X'):
        return f, '/proc/%d' % pid
    try:
        tids = os.listdir('/proc/%d/task' % pid)
    except OSError:
        return None, None
    for tid in tids:
        if tid == str(pid):
            continue
        try:
            t = open('/proc/%d/task/%s/stat' % (pid, tid)).read()
        except OSError:
            continue
        if t.rsplit(')', 1)[1].split()[0] not in ('Z', 'X'):
            return f, '/proc/%d/task/%s' % (pid, tid)
    return None, None
"""

# Every live process with its ids, start ticks and marker status.  A process
# is reported only if its stat reads the same before and after its environ,
# so one that exited (or was replaced) mid-read is not misreported.  /proc
# is re-listed until no new pid appears, so a process that forks and exits
# during the walk cannot hide its child; if it never settles the evidence
# is unverifiable.
_JOB_SCAN = bounded(r"""
import json, os, sys, time
names = json.loads(sys.argv[2]) if len(sys.argv) > 2 else \
    ['ACS_JOB_MARKER', 'SMC_GUARD_JOB']
markers = [(n + '=' + sys.argv[1]).encode() for n in names] \
    if sys.argv[1] else []
me = os.getpid()
btime = None
for line in open('/proc/stat'):
    if line.startswith('btime '):
        btime = float(line.split()[1])
""" + _LIVE_STAT + r"""
procs = []
def record(pid):
    first, path = stat(pid)
    if first is None:
        return
    try:
        environ = open(path + '/environ', 'rb').read().split(b'\0')
        env = 'has-marker' if any(m in environ for m in markers) \
            else 'no-marker'
    except OSError:
        env = 'unreadable'
    again, _ = stat(pid)
    if again is None or again[19] != first[19]:
        return
    procs.append({'pid': pid, 'ppid': int(first[1]), 'pgrp': int(first[2]),
                  'sid': int(first[3]), 'ticks': int(first[19]), 'env': env})
seen, settled = set(), False
for _ in range(20):
    fresh = [int(n) for n in os.listdir('/proc')
             if n.isdigit() and int(n) != me and int(n) not in seen]
    if not fresh:
        settled = True
        break
    seen.update(fresh)
    for pid in fresh:
        record(pid)
print(json.dumps({'now': time.time(), 'btime': btime, 'settled': settled,
                  'tck': os.sysconf('SC_CLK_TCK'), 'procs': procs}))
""")

# Signals only processes proven, immediately before os.kill, to carry the
# job marker with the scanned start ticks; a group is signalled only
# through its proven leader (pid == pgid).
_JOB_SIGNAL = bounded(r"""
import json, os, sys
names = json.loads(sys.argv[4]) if len(sys.argv) > 4 else \
    ['ACS_JOB_MARKER', 'SMC_GUARD_JOB']
markers = [(n + '=' + sys.argv[1]).encode() for n in names]
sig = int(sys.argv[2])
plan = json.loads(sys.argv[3])
""" + _LIVE_STAT + r"""
def unmarked_child(pid):
    # a live direct child without the marker (or unreadable) at kill time
    for name in os.listdir('/proc'):
        if not name.isdigit():
            continue
        f, path = stat(int(name))
        if f is None or int(f[1]) != pid:
            continue
        try:
            environ = open(path + '/environ', 'rb').read().split(b'\0')
        except OSError:
            return True
        if not any(m in environ for m in markers):
            return True
    return False
def proven(pid, ticks):
    first, path = stat(pid)
    if first is None or int(first[19]) != ticks:
        return None
    try:
        environ = open(path + '/environ', 'rb').read().split(b'\0')
    except OSError:
        return None
    if not any(m in environ for m in markers):
        return None
    again, _ = stat(pid)
    return again if again is not None and int(again[19]) == ticks else None
done = {'groups': [], 'pids': [], 'skipped': []}
for pgid, ticks in plan['groups']:
    leader = proven(pgid, ticks)
    if leader is None or int(leader[2]) != pgid:
        done['skipped'].append([pgid, 'group leader not proven'])
        continue
    if unmarked_child(pgid):
        done['skipped'].append([pgid, 'unmarked child; handle manually'])
        continue
    try:
        os.killpg(pgid, sig)
        done['groups'].append(pgid)
    except OSError as error:
        done['skipped'].append([pgid, str(error)])
for pid, ticks in plan['pids']:
    if proven(pid, ticks) is None:
        done['skipped'].append([pid, 'not proven'])
        continue
    if unmarked_child(pid):
        done['skipped'].append([pid, 'unmarked child; handle manually'])
        continue
    try:
        os.kill(pid, sig)
        done['pids'].append(pid)
    except OSError as error:
        done['skipped'].append([pid, str(error)])
print(json.dumps(done))
""")


def valid_pid(value) -> int | None:
    """A positive int pid, or None for anything else (bool, str, float...)."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


# guard.json schemas ACS reads: the internal guard's (0.2.0 on) and the
# legacy one (earlier internal guards and the external guard script)
GUARD_RECORD_SCHEMAS = ("agent-compute-guard-record/1", "smc-guard-record/1")


def record_problem(record) -> str | None:
    """Why a guard record cannot be used as evidence (None: usable or
    absent).  A malformed record never resolves a job."""
    if record is None:
        return None
    if not isinstance(record, dict):
        # C4: an unreadable guard.json (state.Unreadable) is never absent
        reason = getattr(record, "reason", None)
        return f"guard record {reason}" if isinstance(reason, str) else \
            f"guard record is a {type(record).__name__}, not an object"
    schema = record.get("schema")
    if schema is not None and schema not in GUARD_RECORD_SCHEMAS:
        return (f"guard record schema {schema!r} is not one of "
                f"{list(GUARD_RECORD_SCHEMAS)}")
    wrong = [key for key, ok in (
        ("pid", record.get("pid") is None or valid_pid(record.get("pid"))),
        ("container_pid", record.get("container_pid") is None or
         valid_pid(record.get("container_pid"))),
        ("outcome", record.get("outcome") is None or
         isinstance(record.get("outcome"), str)),
        ("job_id", record.get("job_id") is None or
         isinstance(record.get("job_id"), str)),
        ("started_utc", record.get("started_utc") is None or
         isinstance(record.get("started_utc"), str))) if not ok]
    if wrong:
        return "guard record field(s) of the wrong type: " + ", ".join(wrong)
    return None


def _text(value) -> str | None:
    return value if isinstance(value, str) and value else None


def docker(*args: str, timeout: float = DOCKER_TIMEOUT_S):
    return subprocess.run(["docker", *args], capture_output=True, text=True,
                          timeout=timeout)


def docker_daemon_id() -> str | None:
    """The answering docker daemon's ID (``docker info``), or None."""
    try:
        out = docker("info", "--format", "{{.ID}}")
    except (OSError, subprocess.SubprocessError):
        return None
    ident = out.stdout.strip()
    return ident if out.returncode == 0 and ident and \
        ident != "<no value>" else None


def container_id(name: str) -> str | None:
    try:
        out = docker("inspect", "--format", "{{.Id}}", name)
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() if out.returncode == 0 else None


# Containers in which an ACS helper outlived the host-side docker timeout
# in THIS process.  Its in-container python may still be stuck there (the
# docker client is killed, the exec'd process is not), and another helper
# would only pile up behind it (A20: 9 after one `reconcile --terminate`).
# So no further helper is spawned there by this process: each later call
# fails closed at once.  This can only turn a would-be answer into a
# failure, never a failure into evidence.  A new process (the next
# reconcile, runner or doctor) tries again.
_STUCK: set[str] = set()


def container_python(container: str, script: str, *args: str,
                     timeout: float = DOCKER_TIMEOUT_S
                     ) -> tuple[dict | None, str | None]:
    """Run one ACS helper (a ``bounded`` script) in ``container``: ``docker
    exec -w / <container> python3 -I -S -c SCRIPT ARGS``.  Returns ``(the
    JSON object on its last stdout line, None)`` or ``(None, why)``, where
    ``why`` distinguishes a host-side timeout, the in-container alarm, a
    failed exec and unparseable output.  Every failure is just a failure
    to the callers (unverified evidence, nothing signalled)."""
    if container in _STUCK:
        return None, ("skipped: an earlier ACS helper in this container "
                      "timed out on the host side and may still be stuck "
                      "there; not spawning another")
    try:
        out = docker("exec", "-w", CONTAINER_EXEC_WORKDIR, container,
                     "python3", "-I", "-S", "-c", script, *args,
                     timeout=timeout)
    except subprocess.TimeoutExpired:
        _STUCK.add(container)
        return None, f"timed out (host {timeout:g} s)"
    except (OSError, subprocess.SubprocessError) as error:
        return None, f"docker exec failed: {error}"
    if out.returncode == ALARM_EXIT:
        return None, f"timed out (in-container alarm, {SCRIPT_ALARM_S} s)"
    if out.returncode != 0:
        return None, (f"failed (rc {out.returncode}, stderr "
                      f"{(out.stderr or '').strip()[-200:]!r})")
    try:
        answer = json.loads(out.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError, AttributeError):
        answer = None
    if not isinstance(answer, dict):
        return None, ("unparseable output: "
                      f"{(out.stdout or '').strip()[-120:]!r}")
    return answer, None


def container_scan(container: str, pid: int) -> dict | None:
    """Exact start ticks of one in-container pid (the captured leader)."""
    answer, _ = container_python(container, _CONTAINER_SCAN, str(int(pid)))
    return answer


def _job_facts(container, record, workload, markers=None) -> dict:
    """What identifies the job, from the manifest's container, the guard
    record, a captured workload and the manifest's marker names; every
    field type-checked."""
    cont = container if isinstance(container, dict) else {}
    rec = record if isinstance(record, dict) else {}
    work = workload if isinstance(workload, dict) else {}
    started = _text(rec.get("started_utc")) or _text(work.get("started_utc"))
    exact = _text(work.get("container_start"))
    return {"container_id": _text(cont.get("id")) or
            _text(work.get("container_id")),
            "name": _text(cont.get("name")) or _text(work.get("container")),
            "daemon_id": _text(cont.get("daemon_id")),
            "job_id": _text(rec.get("job_id")) or
            _text(work.get("guard_job_id")),
            "container_pid": valid_pid(rec.get("container_pid")) or
            valid_pid(work.get("container_pid")),
            "start_epoch": utc_to_epoch(started),
            "exact_start": exact if exact and exact.startswith("ticks:")
            else None,
            "markers": _marker_names(markers)}


def _daemon_problem(facts: dict) -> str | None:
    """Why the answering docker daemon cannot be trusted for this job (None:
    it is the recorded one, or the manifest predates daemon ids).  Shared by
    the evidence and the signal path (C5): "No such container", every scan
    and every signal are trusted only from the daemon the job was submitted
    to."""
    if not facts["daemon_id"]:
        return None
    current = docker_daemon_id()
    if current is None:
        return "the docker daemon's identity cannot be read"
    if current != facts["daemon_id"]:
        return (f"a different docker daemon answers ({current}, recorded "
                f"{facts['daemon_id']})")
    return None


def _unverified(reason: str, **extra) -> dict:
    return {"verified": False, "clean": False, "reason": reason, **extra}


def _locate(cid: str) -> dict:
    """``{"state": "running"|"stopped", "id": <full id>}``, ``{"state":
    "gone"}`` ("No such"), or an unverified result, for an id or a name."""
    try:
        out = docker("inspect", "--type", "container", "--format",
                     "{{.Id}} {{.State.Running}}", cid)
    except (OSError, subprocess.SubprocessError) as error:
        return _unverified(f"docker unreachable: {error}")
    if out.returncode != 0:
        if "No such container" in out.stderr or "No such object" in \
                out.stderr:
            return {"state": "gone"}
        return _unverified("docker inspect failed: " +
                           (out.stderr.strip()[-200:] or
                            f"exit {out.returncode}"))
    fields = out.stdout.split()
    if len(fields) != 2:
        return _unverified(f"docker inspect of {cid[:64]} returned "
                           f"{out.stdout.strip()[:120]!r}")
    return {"state": "running" if fields[1] == "true" else "stopped",
            "id": fields[0]}


def _where(facts: dict) -> dict:
    """Where the job's processes may be.  The recorded id is located by id
    (never by name: a renamed original must not read as replaced).  The
    guard, however, execs by NAME, so if the name is now bound to a
    different running container, that one must be scanned too:
    ``{"recorded": <_locate result>, "rebound": <id or None>}`` or an
    unverified result."""
    cid = facts["container_id"]
    recorded = _locate(cid)
    if "verified" in recorded:
        return recorded
    if recorded["state"] != "gone" and recorded.get("id", cid) != cid:
        return _unverified(f"docker inspect of {cid[:12]} returned another "
                           f"container ({recorded['id'][:12]})")
    rebound = None
    if facts["name"]:
        by_name = _locate(facts["name"])
        if "verified" in by_name:
            return _unverified("container name lookup failed: " +
                               by_name["reason"])
        if by_name["state"] == "running" and \
                by_name.get("id", cid) != cid:
            rebound = by_name["id"]
    return {"recorded": recorded, "rebound": rebound}


def _scan_job(cid: str, job_id: str | None, markers=None
              ) -> tuple[dict | None, str | None]:
    """``(scan, None)`` or ``(None, why it failed)``."""
    before = time.time()
    scan, why = container_python(cid, _JOB_SCAN, job_id or "",
                                 json.dumps(_marker_names(markers)))
    after = time.time()
    if scan is None:
        return None, why
    try:
        scan["host_before"], scan["host_after"] = before, after
        for proc in scan["procs"]:
            proc["start"] = f"ticks:{proc['ticks']}"
        return scan, None
    except (KeyError, TypeError):
        return None, "unparseable scan (malformed process list)"


def _started_before(proc: dict, scan: dict, start_epoch: float | None) -> bool:
    """True only if ``proc`` provably started before the job did.  The
    container clock may be offset from the host's (a VM), so the offset is
    bounded by the scan's own ``now`` and the host times around the exec."""
    if start_epoch is None or not isinstance(scan.get("btime"), (int, float)) \
            or not scan.get("tck") or \
            not isinstance(scan.get("now"), (int, float)):
        return False
    proc_c = scan["btime"] + proc["ticks"] / scan["tck"]
    latest_host = proc_c - (scan["now"] - scan["host_after"])
    return latest_host < start_epoch - EPOCH_TOLERANCE_S


def _public(proc: dict) -> dict:
    return {k: proc[k] for k in ("pid", "ppid", "pgrp", "sid", "start",
                                 "env")}


def container_job_evidence(container, record, workload=None,
                           scan: dict | None = None, *,
                           markers=None) -> dict:
    """The ONE cleanup-evidence function for a container-venue job, shared
    by ``Runner.verify_cleanup`` and ``agent-compute reconcile``.

    ``clean`` is True only when no process of the job is found in the
    recorded container (gone, stopped, or scanned: no marker holder, no
    member of a marker holder's or the recorded leader's session or process
    group, no unreadable process that may have started after the job) AND
    none in a different running container now bound to the recorded name
    (the guard execs by name).  Anything unobservable is ``verified:
    False``.  ``scan`` injects the recorded container's scan (tests).
    ``markers``: the job's marker names (``scan_markers(manifest)``);
    ``KNOWN_MARKERS`` are always looked for as well."""
    facts = _job_facts(container, record, workload, markers)
    base = {"container_id": facts["container_id"],
            "guard_job_id": facts["job_id"],
            "container_pid": facts["container_pid"]}
    if not facts["container_id"]:
        return _unverified("no container id recorded", **base)
    if not facts["job_id"] and not facts["container_pid"]:
        return _unverified("the guard record has neither a job id (the "
                           "job marker's value) nor an in-container pid; "
                           "nothing identifies the job's processes", **base)
    base["daemon_verified"] = bool(facts["daemon_id"])
    if facts["daemon_id"]:
        # "No such container" and every scan are trusted only from the
        # daemon the job was submitted to
        problem = _daemon_problem(facts)
        if problem:
            return _unverified(problem, **base)
    else:
        base["daemon_note"] = ("legacy manifest without a daemon id: the "
                               "answering docker daemon was not verified")
    where = _where(facts)
    if "verified" in where:
        return {**where, **base}
    rebound = where["rebound"]
    extra = None
    if rebound:
        extra = _rebound_evidence(facts, rebound)
        if not extra.get("verified"):
            return {**extra, **base, "rebound_container": rebound}
    evidence = _recorded_evidence(facts, where["recorded"], scan, base)
    if extra is None or not evidence.get("verified"):
        return evidence
    members = extra["members"]
    return {**evidence, "rebound_container": rebound,
            "method": f"{evidence.get('method')} + name-rebound marker-scan",
            "rebound_members": members,
            "members": evidence.get("members", []) + members,
            "signalable": bool(evidence.get("signalable") or
                               extra.get("signalable")),
            "clean": evidence["clean"] is True and not members}


def _rebound_evidence(facts: dict, cid: str) -> dict:
    """Marker evidence in a container now bound to the recorded name."""
    if not facts["job_id"]:
        return _unverified(f"the container name now resolves to another "
                           f"running container ({cid[:12]}) and there is no "
                           "job marker to scan it for")
    scan, why = _scan_job(cid, facts["job_id"], facts["markers"])
    if scan is None:
        return _unverified(f"scan of the name-bound container {cid[:12]} "
                           f"failed: {why}")
    if scan.get("settled") is False:
        return _unverified(f"the process table of the name-bound container "
                           f"{cid[:12]} kept changing during the scan")
    # the recorded leader pid belongs to neither container for certain,
    # so only the marker (and its sessions/groups) decides here
    evidence = _marker_evidence({**facts, "container_pid": None,
                                 "exact_start": None}, scan,
                                {"container_id": cid})
    for member in evidence.get("members", []):
        member["container_id"] = cid
    return evidence


def _recorded_evidence(facts: dict, located: dict, scan: dict | None,
                       base: dict) -> dict:
    if located["state"] == "gone":
        return {"verified": True, "clean": True, "members": [], **base,
                "method": "container-gone",
                "note": "the recorded container no longer exists"}
    if located["state"] == "stopped":
        return {"verified": True, "clean": True, "members": [], **base,
                "method": "container-stopped",
                "note": "the recorded container is stopped: no process of "
                        "it is running"}
    if scan is None:
        scan, why = _scan_job(facts["container_id"], facts["job_id"],
                              facts["markers"])
        if scan is None:
            return _unverified(f"in-container scan failed: {why}", **base)
    if scan.get("settled") is False:
        return _unverified("the container's process table kept changing "
                           "during the scan", **base)
    if facts["job_id"]:
        return _marker_evidence(facts, scan, base)
    return _legacy_evidence(facts, scan, base)


def _leader_state(facts: dict, scan: dict, by_pid: dict,
                  marker: bool) -> tuple[str | None, str | None]:
    cpid = facts["container_pid"]
    if cpid is None:
        return None, None
    proc = by_pid.get(cpid)
    if proc is None:
        return GONE, None
    if facts["exact_start"]:
        if proc["start"] == facts["exact_start"]:
            return ALIVE, None
        if marker and proc["env"] == HAS_MARKER:
            return ALIVE, "the recorded leader pid now holds another " \
                          "process of the same job"
        return RECYCLED, "exact start ticks differ from the captured leader"
    if not marker:
        return UNKNOWN, ("a process holds the recorded leader pid; with no "
                         "job marker and no captured start ticks nothing "
                         "proves whose it is")
    if proc["env"] == HAS_MARKER:
        return ALIVE, None
    if _started_before(proc, scan, facts["start_epoch"]):
        return RECYCLED, "the process at the leader pid predates the job"
    # readable-without-marker could be a recycled pid OR this job's leader
    # after an environment-scrubbing exec (e.g. `env -i CMD`); time alone
    # must not decide it
    return UNKNOWN, ("a process at the recorded leader pid started after "
                     "the job and cannot be proven unrelated "
                     f"(environ {proc['env']})")


ORPHANED_REASON = ("process(es) that may have started after the job sit in "
                   "a session or process group whose leader is gone; they "
                   "carry no job marker, so they may be this job's with a "
                   "scrubbed environment.  ACS never signals unmarked "
                   "processes: inspect them and stop them by hand if they "
                   "are this job's, then reconcile")


def _orphaned(procs: list, scan: dict, start_epoch: float | None,
              members: list) -> list:
    """Non-member processes, not provably older than the job, whose session
    or process group names no live process.  Every ``docker exec``'d
    process leads its own session, so a job descendant that dropped the
    marker but stayed in the job's session is left in exactly such an
    orphaned session once the job's leader exits.  Fail-closed: unrelated
    orphaned sessions started after the job also block."""
    live = {p["pid"] for p in procs}
    taken = {p["pid"] for p in members}
    return [p for p in procs if p["pid"] not in taken and
            (p["sid"] not in live or p["pgrp"] not in live) and
            not _started_before(p, scan, start_epoch)]


def _judge(evidence: dict, problems: list, orphaned: list,
           members: list) -> dict:
    if orphaned:
        evidence["orphaned"] = [_public(p) for p in orphaned]
        problems.append(f"{len(orphaned)} {ORPHANED_REASON}")
    if problems:
        return {**evidence, "verified": False, "clean": False,
                "reason": "; ".join(problems)}
    return {**evidence, "verified": True, "clean": not members}


def _job_closure(procs: list, holders: list, seeds: set) -> list:
    """The job's processes: the marker holders, then (to a fixpoint) every
    process sharing a member's session or process group, or whose parent is
    a member.  The lineage step matters when a process drops the marker AND
    leads its own session -- e.g. the guard's leader after ``exec env -i
    CMD`` -- while its (marked) parent lives: it is the job's even though it
    shares nothing else with a marker holder."""
    member_pids = {p["pid"] for p in holders}
    sessions = {p["sid"] for p in holders} | seeds
    groups = {p["pgrp"] for p in holders} | seeds
    while True:
        sessions.discard(0)
        groups.discard(0)
        grown = [p for p in procs if p["pid"] not in member_pids and
                 (p["sid"] in sessions or p["pgrp"] in groups or
                  p["ppid"] in member_pids)]
        if not grown:
            break
        for p in grown:
            member_pids.add(p["pid"])
            sessions.add(p["sid"])
            groups.add(p["pgrp"])
    return [p for p in procs if p["pid"] in member_pids]


def _signal_plan(scan: dict) -> tuple[dict, list]:
    """Marker holders to signal (and the groups they lead), except any
    holder with a live unmarked descendant in this scan: signalling it
    would erase the only link from the job to that descendant (D4), so it
    is left, keeping the evidence unclean, for a human."""
    procs = scan["procs"]
    children: dict = {}
    for p in procs:
        children.setdefault(p["ppid"], []).append(p)

    def unmarked_below(pid: int) -> bool:
        stack, seen = list(children.get(pid, [])), set()
        while stack:
            child = stack.pop()
            if child["pid"] in seen:
                continue
            seen.add(child["pid"])
            if child["env"] != HAS_MARKER:
                return True
            stack.extend(children.get(child["pid"], []))
        return False

    plan, skipped = {"groups": [], "pids": []}, []
    for p in procs:
        if p["env"] != HAS_MARKER:
            continue
        if unmarked_below(p["pid"]):
            skipped.append([p["pid"], "unmarked child; handle manually"])
            continue
        plan["pids"].append([p["pid"], p["ticks"]])
        if p["pid"] == p["pgrp"]:
            plan["groups"].append([p["pid"], p["ticks"]])
    return plan, skipped


def _marker_evidence(facts: dict, scan: dict, base: dict) -> dict:
    procs = scan["procs"]
    by_pid = {p["pid"]: p for p in procs}
    holders = [p for p in procs if p["env"] == HAS_MARKER]
    unreadable = [p for p in procs if p["env"] == UNREADABLE and
                  not _started_before(p, scan, facts["start_epoch"])]
    leader, note = _leader_state(facts, scan, by_pid, marker=True)
    seeds = set()
    if facts["container_pid"] and leader in (ALIVE, GONE):
        # a GONE leader's pid cannot be reused while its session/group has
        # members, so processes still carrying that sid/pgrp are the job's
        seeds.add(facts["container_pid"])
    members = _job_closure(procs, holders, seeds)
    unmarked = [p for p in members if p["env"] != HAS_MARKER]
    evidence = {**base, "method": "marker-scan", "leader": leader,
                "leader_alive": leader == ALIVE,
                "members": [_public(p) for p in members],
                "unmarked_members": [_public(p) for p in unmarked],
                "signalable": bool(holders)}
    if note:
        evidence["leader_note"] = note
    problems = []
    if unreadable:
        evidence["unreadable"] = [_public(p) for p in unreadable]
        problems.append(f"{len(unreadable)} process(es) whose environ cannot "
                        "be read may have started after the job")
    if leader == UNKNOWN:
        problems.append(note)
    return _judge(evidence, problems,
                  _orphaned(procs, scan, facts["start_epoch"], members),
                  members)


def _legacy_evidence(facts: dict, scan: dict, base: dict) -> dict:
    """No job marker (a legacy or malformed guard record): only the recorded
    leader's session/group can be checked, and time never decides."""
    procs = scan["procs"]
    leader, note = _leader_state(facts, scan, {p["pid"]: p for p in procs},
                                 marker=False)
    cpid = facts["container_pid"]
    members = [] if leader == RECYCLED else \
        [p for p in procs if cpid in (p["sid"], p["pgrp"])]
    evidence = {**base, "method": "leader-session (no job marker)",
                "leader": leader, "leader_alive": leader == ALIVE,
                "members": [_public(p) for p in members],
                "signalable": False,
                "note": "no job marker: a descendant that left the session "
                        "cannot be detected"}
    if note:
        evidence["leader_note"] = note
    return _judge(evidence, [note] if leader == UNKNOWN else [],
                  _orphaned(procs, scan, facts["start_epoch"], members),
                  members)


def container_survivors(workload: dict | None) -> dict:
    """Evidence from a captured workload alone (no guard record)."""
    work = workload if isinstance(workload, dict) else {}
    return container_job_evidence({"id": work.get("container_id")}, None,
                                  work)


def signal_container_job(container, record, workload, sig: int, *,
                         markers=None) -> dict:
    """Signal the job's processes from a FRESH scan of each container that
    may hold them (the recorded one if running, and a different running
    container now bound to the recorded name).  In each, one python3 helper
    re-proves each target (marker + scanned start ticks) right before
    ``os.kill``, and signals a process group only through its proven
    leader.  Never a name match; never an unproven pid; nothing at all
    without a job marker, and nothing (no inspect, no exec) when the
    recorded docker daemon is not the one answering (C5; a legacy manifest
    without a daemon id is not checked, as for the evidence)."""
    facts = _job_facts(container, record, workload, markers)
    if not facts["container_id"] or not facts["job_id"]:
        return {"signalled": False, "reason": "no container id or job marker"}
    problem = _daemon_problem(facts)
    if problem:
        return {"signalled": False, "reason": f"{problem}; nothing signalled"}
    where = _where(facts)
    if "verified" in where:
        return {"signalled": False, "reason": where["reason"]}
    targets = [cid for cid in (facts["container_id"]
                               if where["recorded"]["state"] == "running"
                               else None, where["rebound"]) if cid]
    results = {cid: _signal_in(cid, facts["job_id"], sig, facts["markers"])
               for cid in targets}
    return {"signalled": any(r.get("signalled") for r in results.values()),
            "containers": results}


def _signal_in(cid: str, job_id: str, sig: int, markers=None) -> dict:
    scan, why = _scan_job(cid, job_id, markers)
    if scan is None:
        return {"signalled": False, "reason": f"in-container scan failed: "
                f"{why}"}
    plan, skipped = _signal_plan(scan)
    if not plan["pids"]:
        return {"signalled": False, "skipped": skipped,
                "reason": "no marker holder that can be signalled"}
    done, why = container_python(cid, _JOB_SIGNAL, job_id, str(int(sig)),
                                 json.dumps(plan),
                                 json.dumps(_marker_names(markers)))
    if done is None:
        # it may have signalled some targets before failing; the caller
        # re-scans for evidence either way
        return {"signalled": False, "skipped": skipped,
                "reason": f"signal helper failed: {why}"}
    groups, pids = done.get("groups"), done.get("pids")
    theirs = done.get("skipped")
    done["skipped"] = skipped + (theirs if isinstance(theirs, list) else [])
    done["signalled"] = bool(groups or pids)
    return done


def signal_workload(workload: dict, sig: int,
                    daemon_id: str | None = None, *, markers=None) -> bool:
    """The runner's cancel path passes no proof time, so a host group
    whose leader pid is free is not signalled (see signal_host_group).  A
    container job is signalled only under the docker daemon recorded at
    submission (``daemon_id``, from the manifest; C5)."""
    if workload.get("venue") == "container":
        return signal_container_job({"id": workload.get("container_id"),
                                     "daemon_id": daemon_id},
                                    None, workload, sig,
                                    markers=markers)["signalled"]
    return signal_host_group(workload, sig)


# --- the compute lock, observed read-only -------------------------------------------

def lock_file_users(lock: Path) -> list[int] | None:
    """Pids that have the guard's lock file open, observed without opening
    or locking it (None: cannot tell).  A holder, or a legacy guard waiting
    with --wait, has it open."""
    try:
        if not exists_strict(lock):
            return []
    except OSError:
        return None                     # A49: cannot tell
    if sys.platform.startswith("linux"):
        target = os.path.realpath(lock)
        users = []
        for name in os.listdir("/proc"):
            if not name.isdigit():
                continue
            fd_dir = f"/proc/{name}/fd"
            try:
                for fd in os.listdir(fd_dir):
                    try:
                        if os.readlink(f"{fd_dir}/{fd}") == target:
                            users.append(int(name))
                            break
                    except OSError:
                        continue
            except OSError:
                continue
        return users
    try:
        out = subprocess.run(["lsof", "-t", "--", str(lock)],
                             capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode not in (0, 1):
        return None
    return [int(p) for p in out.stdout.split() if p.isdigit()]


def utc_to_epoch(text: str | None) -> float | None:
    if not text or not isinstance(text, str):
        return None
    import calendar
    try:
        return float(calendar.timegm(time.strptime(text,
                                                   "%Y-%m-%dT%H:%M:%SZ")))
    except ValueError:
        return None
