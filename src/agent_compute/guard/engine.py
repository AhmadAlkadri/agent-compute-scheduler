#!/usr/bin/env python3
"""Resource guard: the ACS-internal guard (backend ``acs-guard``).

The ACS runner executes this file, from the installed version directory
the job was submitted with, as its own process; ``backend.sha256`` pins it.
ACS admission (cli, runner, queue, reconcile, doctor) never imports it and
never opens the lock; only this guard, run as its own process, takes the
lock in its guard home.

**Guard home.**  ``$ACS_GUARD_HOME`` (ACS always sets it from the machine
config's ``backend.guard_home``, default ``<state_dir>/guard``; the legacy
name ``SMC_GUARD_HOME`` is read too, and both naming different homes is
refused).  It holds ``lock``, ``current.json`` and ``inflight.json``.
There is no built-in default: ``run``/``probe`` refuse without a home.

**No direct route.**  ``run`` and ``probe`` refuse (exit 7) unless invoked
by the ACS runner of a claimed job: ``--acs-job ID --acs-state DIR`` must
name a job whose ``state.json`` records this process's parent as its
runner and that has no outcome yet.  This is cooperative, not a security
boundary (a same-user process can forge the files); it keeps agents on the
admitted route.  Sandbox tests bypass it only with
``ACS_GUARD_TEST_UNCLAIMED=1`` **and** a sandbox guard home: one that holds
the test harness's sentinel file ``ACS_TEST_SANDBOX`` (written by
``tests/conftest.py``; no ACS code creates it, so a production guard home,
whatever its configured path, never has it) and that is not the legacy
home, the default ACS one or a guard home beside an ACS state dir.  So the
bypass never reaches a real lock.  ``status`` and ``baseline`` run no workload and
need no claim.

**Job marker and names.**  Every process of a job carries
``ACS_JOB_MARKER=<guard job id>``, and the same value under each
``--marker-alias NAME`` (the machine config's ``backend.marker_aliases``,
for workloads that check a legacy name).  The workload also gets
``ACS_CASE``, ``ACS_WORKERS`` and, with ``--dense-threads``,
``ACS_DENSE_THREADS``; ``--legacy-workload-env`` adds the legacy names of
these (``SMC_GUARD_CASE``, ``SMC_GUARD_WORKERS``,
``SMC_DENSE_BLAS_THREADS``) and makes ``SMC_SLICE`` a slice fallback
beside ``ACS_SLICE``.

**Nested pass-through.**  A guard started inside a marked job
(``ACS_JOB_MARKER`` or the legacy ``SMC_GUARD_JOB`` set) runs the command
straight through, without the lock, only when the enclosing ACS job is
proven: ``ACS_JOB_ID`` names a claimed, unfinished job whose guard's
in-flight marker in this guard home carries the same marker value.
Otherwise it refuses (exit 7): a variable alone proves nothing.  A
container launch from inside a guarded job is always refused.

A job is any command that imports numpy, launches or execs into a container,
or runs longer than a few seconds.  The guard

- takes the single machine-wide job lock (``<guard home>/lock``,
  ``flock(LOCK_EX|LOCK_NB)``; a second launch is refused, or waits with
  ``--wait``); ``--light`` host jobs (measured peak <= the light budget,
  one thread) skip the lock and may run beside the one heavy job, capped
  at 1.5 x the light budget;
- pins BLAS/OpenMP threads and runs the job under ``nice``;
- pre-flights against the measured peak of the job's case key in the
  project's baseline table (``--baseline``): refused when host available
  memory is below floor + estimate, or when the case has no measurement and
  no declared reserve (``probe`` first, or ``--reserve-gb``); a container
  job on macOS is credited a heuristic lower bound of the Docker VM memory
  that is resident on the host and free in the guest (up to its estimate;
  the run-time floor pause, which takes no credit, stays the backstop),
  refused outright when it is larger than the guest's MemTotal and made
  to wait while it is larger than the guest's MemAvailable (``vm_credit``;
  0 credit and no guest check whenever that cannot be measured or the
  guest reading is not the VM's own);
- samples the job's process-group RSS (host) or the container cgroup memory
  (container venue) and the host available memory every second;
- pauses the job on a host floor breach (SIGSTOP / ``docker pause``), resumes
  when memory has stayed above floor + 2 GB for 60 s, kills after 30 min
  paused; kills a job whose own memory exceeds its budget (SIGTERM, SIGKILL
  after 10 s) or its wall-time ceiling;
- writes ``guard.json`` (``agent-compute-guard-record/1``, with the marker
  names, this file's sha256 and, for a refused or lock-busy attempt, the
  ``refusal``) + ``samples.csv`` under ``--outputs`` (or
  ``--outputs-root/<case>/<stamp>``), and to ``--runlog`` lines only for
  an admitted attempt: its pre-launch notes (written at the admission),
  its actions and its end (since 0.2.0, campaign G decision D3: a refusal
  or a busy lock writes no runlog line, so a waiting job's retries leave
  the project's runlog alone); a new baseline table is
  ``agent-compute-baseline/1``;
  the legacy record and table schemas (``smc-guard-record/1``,
  ``smc-perf-baseline/1``) are read and an existing table keeps its schema;
- for container jobs computes the per-job memory ceiling (1.5 x measured
  peak, rounded up to 1 GB, never above VM size minus 2 GB) and kills the
  job above it by sampling; only with ``--container-limits`` does it also
  apply that ceiling and the profile's CPU quota to the container
  (``docker update --memory/--memory-swap/--cpus``);
- verifies that the job's processes are gone before it reports the end,
  and keeps a durable in-flight marker until they are (below).

Host available memory on macOS is total minus wired, anonymous and
compressor-occupied pages plus purgeable pages (``vm_stat``); on Linux it is
``MemAvailable``.

Usage::

    python3 -m agent_compute.guard run   --profile-file P --case K \
        --cwd DIR --runlog F --baseline F --outputs DIR [options] -- CMD ...
    python3 -m agent_compute.guard probe ...  (same options; 60 s probe)
    python3 -m agent_compute.guard status [--profile-file P] [--baseline F]
    python3 -m agent_compute.guard baseline --baseline F \
        [--reconcile --records DIR [--case K ...] [--apply]]

The file also runs by path (``python3 .../agent_compute/guard/engine.py``).
Standard library only; Python 3.10+.

Parameters
----------

- **Machine profile** (budget, floor, container CPUs, runlog tag, and the
  optional disk floor, light budget, VM reserve and container memory
  factor): read from ``--profile-file`` (JSON, see ``load_profile``); there
  are no built-in profiles and no auto-detection.  The optional keys have
  documented defaults (``DISK_FLOOR_GB`` etc.).  ``--budget-gb`` and
  ``--floor-gb`` override the profile, as ACS does for a dedicated budget.
- **Project settings**: ``--runlog`` and ``--baseline`` are required; the
  outputs directory is ``--outputs`` or ``--outputs-root/<case>/<stamp>``
  (one is required); ``--cwd`` defaults to ``--project-root`` (one is
  required); ``--container-workdir`` is required for the container venue;
  ``baseline --reconcile`` requires ``--records``.  ``--project-root`` is
  also the last ladder-config search base, the disk check's fallback
  directory (else ``--cwd``) and the base the record's ``baseline`` path is
  made relative to (else the path as given).
- **Reserve detection** runs only with ``--reserve-detector smc-ladder`` (a
  project-specific detector, shipped as the example: a JSON config argument
  with a ``ladder`` list declares its heaviest rung's reserve); without it
  no command is a ladder job (record ``"ladder": null``).
- **Texts**: a new runlog gets a generic header (or ``--runlog-header-file``
  verbatim).  No abbreviated options (``allow_abbrev=False``).

Lifecycle
---------

- **Group kill, verified.**  A kill (budget, wall time, pause ceiling,
  SIGINT/SIGTERM, probe stop) signals the job's whole process group (host:
  and, on Linux, its session) or in-container group (``os.killpg`` inside
  the container, since dash rejects ``kill -- -PGID``), waits until no
  member is left, sends SIGKILL to whatever outlives the grace, and records
  in ``guard.json`` ``cleanup`` whether the group is provably empty.  The
  host leader is not reaped before that check, so its pid cannot name
  another process's group.
- **Container lineage, D4 (campaign G, C3).**  The container venue's
  members also include, to a fixpoint, every process sharing a member's
  session or group or whose parent is a member, as ACS's own evidence does.
  A member that is neither marked nor in the recorded group or session (the
  leader after ``exec env -i CMD`` when the pid file was never read) is
  never signalled, nor is any process with such a descendant (killing it
  would erase the only link); they stay members, so such a job ends
  ``cleanup-unverified`` with ``unmarked_survivors`` and the in-flight
  marker kept.  Every target is re-proven right before the signal.
- **No clean end without evidence.**  If members of the job remain, or its
  processes cannot be listed, after a kill or after the leader ended by
  itself, the outcome is ``cleanup-unverified`` (exit 5; ``cleanup`` keeps
  the survivors and the job's own outcome).  Survivors of a normal end are
  recorded, not killed.
- **Signals from before the launch.**  SIGINT/SIGTERM handlers are
  installed before the workload starts (host ``Popen``; the container
  venue's pid discovery, up to 30 s), so such a signal stops the job
  through the kill path.  After the job, further signals are ignored.
- **In-flight marker.**  ``<guard home>/inflight.json``
  (``agent-compute-guard-inflight/1``; it records the ACS job, its state
  dir and the marker names) is written before a heavy launch and removed
  only after the job's processes are verified gone.  A guard that finds one
  (its writer was SIGKILLed or crashed, or could not verify its end)
  refuses with exit 4 (``guard:lock-busy``), or waits with ``--wait``,
  until the recorded workload is proven gone from evidence, then clears it
  (action ``stale-inflight``).  Unprovable (the writer died before it
  recorded the workload pid) means an operator checks and removes it.
  ``--light`` jobs neither write nor honor it.
- **A20.**  Every ``docker exec`` runs with ``-w /``; in-container helpers
  run ``python3 -I -S`` under an alarm.
- **Limits.**  Tracking is cooperative: a descendant that calls setsid()
  (or setpgid() on macOS) leaves the tracked group and is not seen, unless
  (container venue) it keeps one of the job's marker variables.  There is
  no cgroup containment.

History: this guard was extracted from a resource guard that a scientific
project kept in its own repository (``guard.py``), ported faithfully and
then parameterized (campaign slices S2-S4) and generalized (campaign G);
the provenance, the port's differences and the decisions are recorded in
``docs/campaign-extraction.md`` (and ``docs/campaign-generalization.md``).
Comments below describe the mechanism; the defects that motivated it are
in the ACS findings register.
"""

from __future__ import annotations

import argparse
import csv
import datetime as _dt
import fcntl
import json
import math
import os
import platform
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import TypeGuard

GB = 1024.0 ** 3

# --- guard mechanism constants ---------------------------------------------------
# The machine-specific values (host budget, floor, container CPUs, runlog
# tag) are NOT here: they come from the profile file.  LIGHT_BUDGET_GB,
# DISK_FLOOR_GB, VM_RESERVE_GB and CONTAINER_MEMORY_FACTOR are the defaults
# of the profile's optional keys.
LIGHT_BUDGET_GB = 1.0          # --light: may run beside the one heavy job
RESUME_MARGIN_GB = 2.0         # resume when available > floor + margin ...
RESUME_HOLD_S = 60.0           # ... for this long
MAX_PAUSE_S = 1800.0           # kill after this long paused
KILL_GRACE_S = 10.0            # SIGTERM -> SIGKILL
PROBE_S = 60.0                 # probe duration
DISK_FLOOR_GB = 20.0           # refuse when free disk is below this
SAMPLER_STALE_S = 30.0         # log `sampler-stale` after this long blind
SAMPLER_PAUSE_S = 300.0        # pause the job after this long blind
CONTAINER_MEMORY_FACTOR = 1.5
VM_RESERVE_GB = 2.0
NICE = 10
SAMPLE_INTERVAL_S = 1.0
DOCKER_TIMEOUT_S = 30.0
# Docker VM credit (macOS container venue; see `vm_credit`)
VM_PROCESS_NAME = "com.apple.Virtualization.VirtualMachine"
DOCKER_VZ_HELPER = "com.docker.virtualization"
VM_PROBE_TIMEOUT_S = 10.0
VM_CREDIT_OVERHEAD_GB = 0.25       # VMM overhead counted in the footprint
VM_CREDIT_OVERHEAD_FRACTION = 0.03  # guest RAM outside MemTotal (kernel, memmap)

# --- machine profile -----------------------------------------------------------------
# One machine, one profile, supplied by the caller as a JSON file:
#
#   {"schema": "agent-compute-guard-profile/1", "name": "mac",
#    "budget_gb": 12.0, "floor_gb": 8.0, "container_cpus": 10,
#    "runlog_tag": null}
#
# plus the optional keys below.  There is no built-in profile and no
# auto-detection: the numbers belong to the machine config (slice S4).  The
# lock is per guard home (one per machine), so each host runs exactly one
# heavy job of its own, whatever the profile.
PROFILE_SCHEMA = "agent-compute-guard-profile/1"
PROFILE_REQUIRED = ("name", "budget_gb", "floor_gb", "container_cpus",
                    "runlog_tag")
PROFILE_OPTIONAL = {"disk_floor_gb": DISK_FLOOR_GB,
                    "light_budget_gb": LIGHT_BUDGET_GB,
                    "vm_reserve_gb": VM_RESERVE_GB,
                    "container_memory_factor": CONTAINER_MEMORY_FACTOR}


class ProfileError(ValueError):
    pass


def _positive_number(key: str, value) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or \
            not math.isfinite(value) or value <= 0:
        raise ProfileError(f"'{key}' must be a positive number, got {value!r}")
    return float(value)


def parse_profile(data) -> dict:
    """Validate a machine profile; return it with the optional keys filled
    in (their documented defaults).  Unknown keys are refused, so a typo can
    never silently fall back to a default."""
    if not isinstance(data, dict):
        raise ProfileError("the profile must be a JSON object")
    if data.get("schema") != PROFILE_SCHEMA:
        raise ProfileError(f"'schema' must be '{PROFILE_SCHEMA}'")
    known = {"schema", *PROFILE_REQUIRED, *PROFILE_OPTIONAL}
    unknown = sorted(set(data) - known)
    if unknown:
        raise ProfileError(f"unknown keys: {', '.join(unknown)}")
    missing = [k for k in PROFILE_REQUIRED if k not in data]
    if missing:
        raise ProfileError(f"missing keys: {', '.join(missing)}")
    name = data["name"]
    if not isinstance(name, str) or not name.strip():
        raise ProfileError("'name' must be a non-empty string")
    cpus = data["container_cpus"]
    if isinstance(cpus, bool) or not isinstance(cpus, int) or cpus < 1:
        raise ProfileError(f"'container_cpus' must be an integer >= 1, "
                           f"got {cpus!r}")
    tag = data["runlog_tag"]
    if tag is not None and (not isinstance(tag, str) or not tag.strip()):
        raise ProfileError("'runlog_tag' must be null or a non-empty string")
    profile = {"name": name, "container_cpus": cpus, "runlog_tag": tag,
               "budget_gb": _positive_number("budget_gb", data["budget_gb"]),
               "floor_gb": _positive_number("floor_gb", data["floor_gb"])}
    for key, default in PROFILE_OPTIONAL.items():
        profile[key] = _positive_number(key, data.get(key, default))
    return profile


def load_profile(path) -> dict:
    try:
        data = json.loads(Path(path).read_text())
    except (OSError, ValueError) as error:
        raise ProfileError(f"cannot read {path}: {error}") from error
    try:
        return parse_profile(data)
    except ProfileError as error:
        raise ProfileError(f"{path}: {error}") from error


THREAD_VARS = ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS",
               "VECLIB_MAXIMUM_THREADS", "MKL_NUM_THREADS",
               "NUMEXPR_NUM_THREADS")
# The job marker: every process of a job carries JOB_MARKER=<guard job id>
# (and the same value under each --marker-alias).  LEGACY_JOB_MARKER is the
# name earlier versions exported (and an external guard script still does):
# the nested pass-through and the container scans recognise both.
JOB_MARKER = "ACS_JOB_MARKER"
LEGACY_JOB_MARKER = "SMC_GUARD_JOB"
KNOWN_MARKERS = (JOB_MARKER, LEGACY_JOB_MARKER)
# Workload variables, and their legacy names (--legacy-workload-env)
ENV_CASE = "ACS_CASE"
ENV_WORKERS = "ACS_WORKERS"
ENV_DENSE_THREADS = "ACS_DENSE_THREADS"     # the dense-phase BLAS count
LEGACY_ENV_CASE = "SMC_GUARD_CASE"
LEGACY_ENV_WORKERS = "SMC_GUARD_WORKERS"
LEGACY_ENV_DENSE_THREADS = "SMC_DENSE_BLAS_THREADS"
# the slice id when --slice is not given (the legacy name only with
# --legacy-workload-env)
ENV_SLICE = "ACS_SLICE"
LEGACY_ENV_SLICE = "SMC_SLICE"
# the ACS job id the runner exports (the nested pass-through's claim)
ENV_JOB_ID = "ACS_JOB_ID"
# The guard home (lock, current.json, inflight.json).  ACS always sets it;
# the legacy name is read too (an operator's direct runs, the tests' own
# sandboxes); both set to different homes is refused.
ENV_HOME = "ACS_GUARD_HOME"
LEGACY_ENV_HOME = "SMC_GUARD_HOME"
ENV_FREE_FILE = "SMC_GUARD_FREE_MEM_FILE"   # test hook: fake available bytes
ENV_SAMPLER_FAIL_FILE = "SMC_GUARD_SAMPLER_FAIL_FILE"  # test hook: blind sampler
ENV_VM_FOOTPRINT_FILE = "SMC_GUARD_VM_FOOTPRINT_FILE"  # test hook: VM resident bytes

# Record schemas: the ones written, then the legacy ones still read
GUARD_SCHEMA = "agent-compute-guard-record/1"
BASELINE_SCHEMA = "agent-compute-baseline/1"
GUARD_RECORD_SCHEMAS = (GUARD_SCHEMA, "smc-guard-record/1")
BASELINE_SCHEMAS = (BASELINE_SCHEMA, "smc-perf-baseline/1")
# A marker alias: an upper-case variable name that is none of the guard's
# own variables
_ALIAS = re.compile(r"[A-Z][A-Z0-9_]{0,63}")
RESERVED_NAMES = (JOB_MARKER, ENV_JOB_ID, ENV_CASE, ENV_WORKERS,
                  ENV_DENSE_THREADS, ENV_SLICE, ENV_HOME, "PATH", "HOME",
                  "LANG", "LC_ALL", "USER", "LOGNAME", "SHELL", "TMPDIR")

EXIT_REFUSED = 3
EXIT_LOCKED = 4
EXIT_KILLED = 5
EXIT_VENUE = 6
EXIT_USAGE = 7

# S3 (guard lifecycle).  The outcome of a job whose processes could not be
# proven gone when the guard finished (survivors, or no evidence).  It is
# a new outcome word: the ACS runner maps any outcome it does not know
# to "unknown" (runner.GUARD_OUTCOME), and its own cleanup evidence blocks
# the host when processes remain.  The guard exits EXIT_KILLED with it.
OUTCOME_UNCLEAN = "cleanup-unverified"
# The durable "a workload may be alive" marker, next to current.json in the
# guard home (never the lock file itself).  Written before a heavy launch,
# removed only after the job's processes are verified gone.  A new guard
# that finds one refuses (EXIT_LOCKED, or waits with --wait) until the
# recorded workload is proven gone from evidence, then clears it.
INFLIGHT_FILE = "inflight.json"
INFLIGHT_SCHEMA = "agent-compute-guard-inflight/1"
# S4: the ACS claim that run/probe require (see acs_claim_problem).
ENV_TEST_UNCLAIMED = "ACS_GUARD_TEST_UNCLAIMED"
# The test bypass works only in a guard home holding this regular file,
# which only the test harness writes (tests/conftest.py mark_sandbox).
SANDBOX_SENTINEL = "ACS_TEST_SANDBOX"
_ACS_JOB_ID = r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}"
# A20: every `docker exec` runs from `/`, never the container's WorkingDir
# (a hung bind mount there hung every exec, 2026-09-28).  In-container
# helpers run `python3 -I -S` and bound themselves with an alarm below the
# host-side docker timeout.
CONTAINER_EXEC_WORKDIR = "/"
SCRIPT_ALARM_S = 20


_ENGINE_SHA: list = []


def engine_sha256() -> str | None:
    """sha256 of this file (recorded in every guard record)."""
    if not _ENGINE_SHA:
        import hashlib
        try:
            _ENGINE_SHA.append(hashlib.sha256(
                Path(__file__).read_bytes()).hexdigest())
        except OSError:
            _ENGINE_SHA.append(None)
    return _ENGINE_SHA[0]


def utc_now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def log(message: str) -> None:
    sys.stderr.write(f"guard: {message}\n")
    sys.stderr.flush()


# --- host memory --------------------------------------------------------------

_MEMORY_TOTAL: int | None = None


def host_memory_total() -> int:
    global _MEMORY_TOTAL
    if _MEMORY_TOTAL is not None:
        return _MEMORY_TOTAL
    if sys.platform == "darwin":
        _MEMORY_TOTAL = int(subprocess.run(["sysctl", "-n", "hw.memsize"],
                                           capture_output=True, text=True,
                                           check=True).stdout.strip())
    else:
        _MEMORY_TOTAL = _proc_meminfo().get("MemTotal", 0)
    return _MEMORY_TOTAL


def _proc_meminfo() -> dict[str, int]:
    result: dict[str, int] = {}
    try:
        text = Path("/proc/meminfo").read_text()
    except OSError:
        return result
    for line in text.splitlines():
        key, _, rest = line.partition(":")
        fields = rest.split()
        if not fields:
            continue
        try:
            value = int(fields[0])
        except ValueError:
            continue
        if len(fields) > 1 and fields[1].lower() == "kb":
            value *= 1024
        result[key.strip()] = value
    return result


def _vm_stat() -> dict[str, int]:
    out = subprocess.run(["vm_stat"], capture_output=True, text=True,
                         check=True).stdout
    pages: dict[str, int] = {}
    page_size = 16384
    for line in out.splitlines():
        if line.startswith("Mach Virtual Memory Statistics"):
            digits = "".join(ch for ch in line.split("page size of")[-1]
                             if ch.isdigit())
            if digits:
                page_size = int(digits)
            continue
        key, _, value = line.partition(":")
        value = value.strip().rstrip(".")
        if value.isdigit():
            pages[key.strip().strip('"')] = int(value)
    pages["_page_size"] = page_size
    return pages


def host_memory_sample() -> dict:
    """Return available bytes plus the raw components used to derive them."""
    fake = os.environ.get(ENV_FREE_FILE)
    if fake:
        try:
            value = float(Path(fake).read_text().strip())
        except (OSError, ValueError):
            value = float("nan")
        return {"available": value, "source": "fake-file"}
    if sys.platform == "darwin":
        pages = _vm_stat()
        page = pages["_page_size"]
        total = host_memory_total()
        wired = pages.get("Pages wired down", 0)
        anonymous = pages.get("Anonymous pages", 0)
        compressor = pages.get("Pages occupied by compressor", 0)
        purgeable = pages.get("Pages purgeable", 0)
        free = pages.get("Pages free", 0)
        used = (wired + anonymous + compressor - purgeable) * page
        return {"available": float(total - used), "source": "vm_stat",
                "free": float(free * page), "wired": float(wired * page),
                "anonymous": float(anonymous * page),
                "compressor": float(compressor * page),
                "purgeable": float(purgeable * page)}
    meminfo = _proc_meminfo()
    return {"available": float(meminfo.get("MemAvailable", 0)),
            "source": "/proc/meminfo"}


# --- process-group RSS --------------------------------------------------------

def process_group_rss(pgid: int) -> tuple[int | None, int]:
    """Sum RSS (bytes) of every process in the group; also return the count.

    Returns ``(None, 0)`` when the sample itself could not be taken (``ps``
    failed or timed out), which is NOT the same as an empty process group:
    the caller counts it as a sampler failure instead of reading it as
    0 bytes.
    """
    try:
        out = subprocess.run(["ps", "-A", "-o", "pid=,pgid=,rss="],
                             capture_output=True, text=True,
                             timeout=10).stdout
    except (subprocess.SubprocessError, OSError):
        return None, 0
    total = 0
    count = 0
    for line in out.splitlines():
        fields = line.split()
        if len(fields) != 3:
            continue
        try:
            _, group, rss_kb = (int(f) for f in fields)
        except ValueError:
            continue
        if group == pgid:
            total += rss_kb * 1024
            count += 1
    return total, count


# --- baseline table -----------------------------------------------------------

def load_baseline(path: Path, profile_name: str | None = None) -> dict:
    if not path.is_file():
        return {"schema": BASELINE_SCHEMA,
                "host": host_description(profile_name), "cases": {}}
    data = json.loads(path.read_text())
    data.setdefault("cases", {})
    return data


def host_description(profile_name: str | None = None) -> dict:
    description = {"platform": platform.platform(),
                   "machine": platform.machine(),
                   "cores": os.cpu_count()}
    try:
        description["memory_gb"] = round(host_memory_total() / GB, 1)
    except Exception:  # noqa: BLE001 - descriptive only
        pass
    if sys.platform == "darwin":
        try:
            description["model"] = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True, text=True, check=True).stdout.strip()
        except (subprocess.SubprocessError, OSError):
            pass
    elif sys.platform.startswith("linux"):
        try:
            for line in Path("/proc/cpuinfo").read_text().splitlines():
                if line.startswith("model name"):
                    description["model"] = line.split(":", 1)[1].strip()
                    break
        except OSError:
            pass
    description["profile"] = profile_name
    return description


def save_baseline(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data["cases"] = dict(sorted(data["cases"].items()))
    path.write_text(json.dumps(data, indent=1, sort_keys=True) + "\n")


def docker_memory_cap_gb(peak_gb: float, vm_gb: float,
                         factor: float = CONTAINER_MEMORY_FACTOR,
                         vm_reserve_gb: float = VM_RESERVE_GB) -> float:
    cap = math.ceil(max(peak_gb, 0.0) * factor)
    cap = max(cap, 1)
    return float(min(cap, max(vm_gb - vm_reserve_gb, 1.0)))


def allowed_workers(single_peak_gb: float, budget_gb: float,
                    cores: int) -> int:
    if single_peak_gb <= 0:
        return 1
    return max(1, min(cores - 2, int(budget_gb // single_peak_gb)))


def pid_alive(pid) -> bool:
    """True when `pid` names a live process (a lock holder, say)."""
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except (TypeError, ValueError, OSError):
        return False
    return True


# --- ladder jobs (the "smc-ladder" reserve detector) --------------------------------
# A convergence-ladder job climbs rungs (e.g. a fixed step -> an adaptive step
# -> a line search -> stabilisation -> a finer mesh), and the LAST rungs are
# the expensive ones.  A case key's measured peak therefore bounds only the
# rungs the measured run happened to use: a reserve of 1.5 x a short
# probe that stayed on the first rung is too small for the heaviest rung.  So
# a ladder declares the reserve of its heaviest rung, in its own config
# (`"reserve_gb"` on the rung, or at the top level) or at launch
# (`--reserve-gb`), and admission takes the max of that and the measurement.
#
# These are one project's semantics, shipped as the example of a
# project-specific detector: it runs only for a project that opts in with
# `--reserve-detector smc-ladder` (RESERVE_DETECTORS).  The last search
# base for a relative config path is the caller's `--project-root`.

LADDER_CONFIG_MAX_BYTES = 4 * 1024 * 1024
RESERVE_DETECTORS = ("smc-ladder",)

def ladder_config_in(command: list[str], cwd=None, project_root=None):
    """Return ``(path, config)`` of the first ladder config the command names.

    A ladder config is any JSON file argument carrying a non-empty ``ladder``
    list, whatever program takes it (a driver and the scripts that wrap it
    alike), so the rule does not depend on the launcher's name.
    """
    bases = [Path(cwd)] if cwd else []
    bases += [Path.cwd()]
    if project_root:
        bases.append(Path(project_root))
    for token in command:
        if not token.endswith(".json"):
            continue
        candidates = [Path(token)] if Path(token).is_absolute() else \
            [base / token for base in bases]
        for path in candidates:
            if not path.is_file():
                continue
            try:
                # a ladder config is a small declaration; never pull a
                # multi-megabyte observables/trajectory file into pre-flight
                if path.stat().st_size > LADDER_CONFIG_MAX_BYTES:
                    break
                data = json.loads(path.read_text())
            except (OSError, ValueError):
                break
            if isinstance(data, dict) and isinstance(data.get("ladder"), list) \
                    and data["ladder"]:
                return path, data
            break
    return None


def ladder_reserve(config: dict) -> tuple[str, float | None]:
    """The heaviest rung of a ladder config and its declared reserve (GB).

    The heaviest rung is the one that declares the largest ``reserve_gb``;
    with nothing declared per rung it is the last rung, since the ladder is
    ordered by escalation and a ``mesh_next`` rung refines the mesh.
    """
    rungs = [r for r in config.get("ladder") or [] if isinstance(r, dict)]
    names = [r.get("name") or f"rung{i}" for i, r in enumerate(rungs)]
    declared = [(name, float(rung["reserve_gb"]))
                for name, rung in zip(names, rungs)
                if isinstance(rung.get("reserve_gb"), (int, float))]
    last = names[-1] if names else "-"
    top = config.get("reserve_gb")
    top_value = float(top) if isinstance(top, (int, float)) else None
    if declared:
        name, value = max(declared, key=lambda pair: pair[1])
        if top_value is not None and top_value > value:
            return last, top_value
        return name, value
    return last, top_value


# --- docker helpers -----------------------------------------------------------

def docker(*args: str, timeout: float = DOCKER_TIMEOUT_S,
           check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], capture_output=True, text=True,
                          timeout=timeout, check=check)


def docker_vm_info() -> dict:
    result = docker("info", "--format", "{{.MemTotal}} {{.NCPU}} {{.OSType}}")
    mem, ncpu, ostype = result.stdout.split()
    return {"vm_memory_gb": int(mem) / GB, "vm_cpus": int(ncpu),
            "os": ostype}


def container_running(name: str) -> bool:
    result = docker("inspect", "--format", "{{.State.Running}}", name,
                    check=False)
    return result.returncode == 0 and result.stdout.strip() == "true"


def docker_exec(container: str, *args: str, env_flags: tuple = (),
                timeout: float = DOCKER_TIMEOUT_S,
                check: bool = False) -> subprocess.CompletedProcess:
    """``docker exec`` from ``/`` (A20), never the container's WorkingDir."""
    return docker("exec", "-w", CONTAINER_EXEC_WORKDIR, *env_flags, container,
                  *args, timeout=timeout, check=check)


# --- Docker VM credit (macOS container venue) -----------------------------------
#
# The defect (observed 2026-10-01).  On macOS, host "available" is total
# minus wired, anonymous and compressor pages plus purgeable (vm_stat).
# Docker Desktop's VM runs as `com.apple.Virtualization.VirtualMachine`,
# whose guest RAM is anonymous host memory, and the VM keeps it after the
# container work that touched it has ended.  Memory that the VM already
# holds on the host, and that is free inside the guest, was therefore
# subtracted from "available", and a container job (which runs inside that
# memory) was refused as if it needed fresh host pages: 11-13 GB available
# on a 64 GB Mac with the VM bloated, a 13 GB container job waiting ~20 h.
#
# The credit is a HEURISTIC lower bound on the POOL of guest pages that are
# both free in the guest and resident on the host:
#
#     credit = max(0, min(R, MemTotal) - (MemTotal - MemAvailable) - O)
#
# It bounds the size of that reusable pool, not the pages the guest kernel
# will actually hand the job: the guest may allocate the job pages that the
# host does not hold yet (and then the host's used memory grows).  It is
# not a guarantee.  What protects the host is unchanged: the run-time floor
# pause in `monitor()` reads host available WITHOUT any credit, so if the
# credit was too generous the job is paused (and killed after the pause
# ceiling) before the host goes below its floor.  The cost of a wrong
# credit is a paused or killed run, not host memory.
#
# - R, the VM's resident host memory: its `phys_footprint` (never above the
#   sum of its dirty pages) minus the part of it that is compressed or
#   swapped (`footprint --swapped`).  Measured on the Mac 2026-10-01 (24 GiB
#   VM, guest using 2.0 GB): phys_footprint 6.12 GB, of which 1.84 GB
#   compressed/swapped, so R = 4.29 GB, matching vmmap's 4.0 GiB DIRTY for
#   the guest-RAM region ("App-Specific Tag 1"; 1.7 GiB SWAPPED).  Not RSS
#   (`ps`: 5.9 GiB), which also counts resident pages that are not dirty and
#   so may not be counted as used.  Not the whole footprint: a compressed
#   guest page must be decompressed into a fresh host page when the job
#   touches it, and the host already counts it only at its compressed size.
# - MemTotal - MemAvailable, the guest's used memory, read from the target
#   container's /proc/meminfo.  That is the guest kernel's view only when
#   no lxcfs/sysbox (or a replaced `cat`) virtualizes it, so it is trusted
#   only when its MemTotal matches `docker info`'s MemTotal (the daemon's
#   own read of the same guest kernel counter) within VM_MEMTOTAL_TOLERANCE.
#   Of R, at most that much is in use in the guest, so at least R - used is
#   free there.
# - O, the part of R that is not MemTotal guest RAM: guest memory outside
#   MemTotal (kernel image, struct page array; measured 0.57 GiB on the
#   24 GiB VM, ~1.6 % + 0.2 GB) and the VMM's own resident memory (tens of
#   MB).  O = 0.25 GB + 3 % of MemTotal (0.97 GB at 24 GB) covers both
#   with about a 2x margin on the struct page array.
#
# Admission is then host available + min(credit, estimate) >= floor +
# estimate: the credit only covers the job's own estimate, never the floor.
# The job must also fit in the VM: guest MemTotal >= estimate (else a
# final refusal) and guest MemAvailable >= estimate (else a retryable
# wait).  No extra margin: MemAvailable already excludes the guest
# kernel's reserves (its watermarks).
# If anything is missing, ambiguous (two VMs, no Docker VZ helper), times
# out, is undecodable or is not finite, the credit is 0 (today's decision)
# and the reason is recorded; no exception other than KeyboardInterrupt and
# SystemExit leaves `vm_credit`.  Linux (and WSL) is unchanged: containers
# share the kernel, so /proc/meminfo already accounts for them.

# docker info's MemTotal and the container's /proc/meminfo MemTotal are the
# same guest kernel counter read seconds apart (equal to the byte on the Mac,
# 2026-10-01: 25159561216); 1 % allows rounding and a hot-plug/balloon
# change between the reads, and rejects a virtualized view (lxcfs/sysbox
# show the container's limit, which is unlikely to be within 1 % of the VM).
VM_MEMTOTAL_TOLERANCE = 0.01


def docker_vm_separate() -> bool:
    """True where containers run in a VM whose guest RAM the host counts as
    used memory of one VM process (Docker Desktop on macOS)."""
    return sys.platform == "darwin"


def _finite_nonnegative(value: object) -> TypeGuard[int | float]:
    return isinstance(value, (int, float)) and not isinstance(value, bool) \
        and math.isfinite(value) and value >= 0


def _ascii_int(text: str) -> int | None:
    """``int(text)`` for plain ASCII digits only (never '²' or '٣')."""
    return int(text) if text.isascii() and text.isdigit() else None


def vm_credit_gb(resident_gb: float, guest_total_gb: float,
                 guest_available_gb: float,
                 estimate_gb: float) -> tuple[float, float]:
    """``(raw credit, applied credit)`` in GB: the applied credit is never
    negative and never above the estimate; a non-finite or negative input
    gives ``(0, 0)``."""
    if not (_finite_nonnegative(resident_gb)
            and _finite_nonnegative(guest_total_gb)
            and _finite_nonnegative(guest_available_gb)
            and _finite_nonnegative(estimate_gb)) \
            or guest_available_gb > guest_total_gb:
        return 0.0, 0.0
    used = guest_total_gb - guest_available_gb
    # assumes 4 KiB guest pages (struct page ~1.56 % of RAM); a 16 KiB-page
    # guest kernel would need less, so this stays conservative there
    overhead = VM_CREDIT_OVERHEAD_GB \
        + VM_CREDIT_OVERHEAD_FRACTION * guest_total_gb
    raw = max(0.0, min(resident_gb, guest_total_gb) - used - overhead)
    return raw, min(raw, estimate_gb)


def guest_meminfo(container: str) -> tuple[dict | None, str | None]:
    """The guest's ``MemTotal``/``MemAvailable`` (bytes), read from
    ``/proc/meminfo`` inside the target container; or ``(None, reason)``."""
    try:
        result = docker_exec(container, "cat", "/proc/meminfo",
                             timeout=VM_PROBE_TIMEOUT_S)
    except (subprocess.SubprocessError, OSError, ValueError) as error:
        # ValueError: UnicodeDecodeError of non-UTF-8 output
        return None, f"guest /proc/meminfo: docker exec failed: {error!r}"
    if result.returncode != 0:
        return None, (f"guest /proc/meminfo: docker exec exit "
                      f"{result.returncode}: {result.stderr.strip()[:200]}")
    values: dict[str, int] = {}
    for line in result.stdout.splitlines():
        key, _, rest = line.partition(":")
        key = key.strip()
        fields = rest.split()
        if key not in ("MemTotal", "MemAvailable"):
            continue
        number = _ascii_int(fields[0]) if len(fields) == 2 else None
        if number is None or fields[1] != "kB":
            return None, f"guest /proc/meminfo: unreadable {key} line"
        if key in values:
            return None, f"guest /proc/meminfo: {key} twice"
        values[key] = number * 1024
    if set(values) != {"MemTotal", "MemAvailable"}:
        return None, "guest /proc/meminfo: no MemTotal/MemAvailable in kB"
    if values["MemTotal"] <= 0 or values["MemAvailable"] > values["MemTotal"]:
        return None, (f"guest /proc/meminfo: MemAvailable "
                      f"{values['MemAvailable']} > MemTotal "
                      f"{values['MemTotal']} (or MemTotal 0)")
    return values, None


def _footprint_resident(report: object, pid: int) -> tuple[float | None,
                                                            str, dict]:
    """Read ``footprint -j`` output: ``(resident bytes, "", extra)`` or
    ``(None, reason, {})``."""
    if not isinstance(report, dict):
        return None, "footprint report is not an object", {}
    processes = report.get("processes")
    if not isinstance(processes, list):
        return None, "footprint report has no process list", {}
    procs = [p for p in processes
             if isinstance(p, dict) and p.get("pid") == pid
             and p.get("name") == VM_PROCESS_NAME]
    if len(procs) != 1:
        return None, (f"footprint reports no single {VM_PROCESS_NAME} at "
                      f"pid {pid}"), {}
    proc = procs[0]
    auxiliary = proc.get("auxiliary")
    phys = auxiliary.get("phys_footprint") \
        if isinstance(auxiliary, dict) else None
    if not _finite_nonnegative(phys):
        return None, f"phys_footprint is {phys!r}", {}
    categories = proc.get("categories")
    if not isinstance(categories, dict) or not categories:
        return None, "footprint reports no categories", {}
    dirty = 0.0
    swapped = 0.0
    for name, category in categories.items():
        if not isinstance(category, dict):
            return None, f"category {name!r} is not an object", {}
        d, s = category.get("dirty"), category.get("swapped")
        if not (_finite_nonnegative(d) and _finite_nonnegative(s)):
            return None, (f"category {name!r}: dirty {d!r}, swapped "
                          f"{s!r}"), {}
        dirty += d
        swapped += s
    footprint = min(float(phys), dirty)
    if not math.isfinite(footprint) or not math.isfinite(swapped):
        return None, "non-finite footprint totals", {}
    resident = footprint - swapped
    if resident < 0:
        return None, (f"swapped {swapped:.0f} exceeds the footprint "
                      f"{footprint:.0f}"), {}
    return resident, "", {"phys_footprint_gb": round(float(phys) / GB, 3),
                          "swapped_gb": round(swapped / GB, 3)}


def vm_host_footprint() -> tuple[float | None, dict]:
    """The Docker VM's resident host memory in bytes (see the comment
    above), and ``{"source", "pid", "reason", ...}``; ``None`` with a
    reason when it cannot be established unambiguously."""
    info: dict = {"source": None, "pid": None, "reason": None}
    fake = os.environ.get(ENV_VM_FOOTPRINT_FILE)
    if fake:
        info["source"] = "fake-file"
        try:
            value = float(Path(fake).read_text().strip())
        except (OSError, ValueError):
            value = float("nan")
        if not _finite_nonnegative(value):
            info["reason"] = f"VM host footprint: test hook reads {value!r}"
            return None, info
        return value, info
    try:
        ps = subprocess.run(["ps", "-A", "-o", "pid=,comm="],
                            capture_output=True, text=True,
                            timeout=VM_PROBE_TIMEOUT_S)
    except (subprocess.SubprocessError, OSError, ValueError) as error:
        # ValueError: UnicodeDecodeError (a process path that is not UTF-8)
        info["reason"] = f"VM host footprint: ps failed: {error!r}"
        return None, info
    if ps.returncode != 0:
        info["reason"] = f"VM host footprint: ps exit {ps.returncode}"
        return None, info
    vms: list[int] = []
    helpers: list[int] = []
    for line in ps.stdout.splitlines():
        field, _, comm = line.strip().partition(" ")
        pid = _ascii_int(field)
        if pid is None:
            continue
        name = comm.strip().rsplit("/", 1)[-1]
        if name == VM_PROCESS_NAME:
            vms.append(pid)
        elif name == DOCKER_VZ_HELPER:
            helpers.append(pid)
    # identity: exactly one Virtualization.framework VM, and Docker Desktop
    # running its VM under that framework (its VZ helper); with two VMs, or
    # with Docker on another VMM, the VM process may not be Docker's
    if not vms:
        info["reason"] = f"VM host footprint: no {VM_PROCESS_NAME} process"
        return None, info
    if len(vms) > 1:
        info["reason"] = (f"VM host footprint: ambiguous, {len(vms)} "
                          f"{VM_PROCESS_NAME} processes {vms}")
        return None, info
    if len(helpers) != 1:
        info["reason"] = (f"VM host footprint: "
                          + (f"no {DOCKER_VZ_HELPER} process" if not helpers
                             else f"{len(helpers)} {DOCKER_VZ_HELPER} "
                                  "processes")
                          + " (Docker's VM is not identified)")
        return None, info
    pid = vms[0]
    info["pid"] = pid
    try:
        with tempfile.TemporaryDirectory(prefix="acs-guard-vm-") as tmp:
            out = Path(tmp) / "footprint.json"
            result = subprocess.run(
                ["footprint", "--swapped", "-f", "bytes", "-j", str(out),
                 str(pid)], capture_output=True, text=True,
                timeout=VM_PROBE_TIMEOUT_S)
            report = json.loads(out.read_text()) if out.is_file() else None
    except (subprocess.SubprocessError, OSError, ValueError) as error:
        info["reason"] = f"VM host footprint: footprint failed: {error!r}"
        return None, info
    if result.returncode != 0 or report is None:
        info["reason"] = (f"VM host footprint: footprint exit "
                          f"{result.returncode}, "
                          f"{'no report' if report is None else 'report'}")
        return None, info
    resident, reason, extra = _footprint_resident(report, pid)
    if resident is None:
        info["reason"] = f"VM host footprint: {reason}"
        return None, info
    info.update(source="footprint", **extra)
    return resident, info


def vm_credit(container: str, estimate_gb: float,
              docker_mem_total: float | None) -> dict:
    """The Docker VM credit for one container job, with its evidence.  The
    credit is 0 with a ``reason`` whenever a measurement fails; the guest
    readings are used (``guest_verified``) only when the guest MemTotal
    matches ``docker info``'s (``docker_mem_total``, bytes)."""
    record: dict = {"credit_gb": 0.0, "raw_credit_gb": 0.0,
                    "vm_resident_gb": None, "vm_source": None,
                    "vm_pid": None, "docker_mem_total_gb": None,
                    "guest_mem_total_gb": None,
                    "guest_mem_available_gb": None, "guest_used_gb": None,
                    "guest_verified": False, "overhead_gb": None,
                    "reason": None}
    try:
        _vm_credit_measure(record, container, estimate_gb, docker_mem_total)
    except Exception as error:      # never KeyboardInterrupt/SystemExit
        record.update(credit_gb=0.0, raw_credit_gb=0.0, guest_verified=False,
                      reason=f"unexpected {type(error).__name__}: {error}")
    return record


def _vm_credit_measure(record: dict, container: str, estimate_gb: float,
                       docker_mem_total: float | None) -> None:
    if not _finite_nonnegative(docker_mem_total) or docker_mem_total <= 0:
        record["reason"] = (f"docker info MemTotal unknown "
                            f"({docker_mem_total!r})")
        return
    record["docker_mem_total_gb"] = round(docker_mem_total / GB, 3)
    guest, reason = guest_meminfo(container)
    if guest is None:
        record["reason"] = reason
        return
    total_gb = guest["MemTotal"] / GB
    available_gb = guest["MemAvailable"] / GB
    record.update(guest_mem_total_gb=round(total_gb, 3),
                  guest_mem_available_gb=round(available_gb, 3),
                  guest_used_gb=round(total_gb - available_gb, 3))
    if abs(guest["MemTotal"] - docker_mem_total) \
            > VM_MEMTOTAL_TOLERANCE * docker_mem_total:
        record["reason"] = (f"guest /proc/meminfo MemTotal {total_gb:.3f} GB "
                            f"differs from docker info MemTotal "
                            f"{docker_mem_total / GB:.3f} GB by more than "
                            f"{VM_MEMTOTAL_TOLERANCE:.0%} (a virtualized "
                            "view, not the VM's)")
        return
    record["guest_verified"] = True
    resident, info = vm_host_footprint()
    record.update(vm_source=info.get("source"), vm_pid=info.get("pid"))
    for key in ("phys_footprint_gb", "swapped_gb"):
        if key in info:
            record[f"vm_{key}"] = info[key]
    if resident is None:
        record["reason"] = info.get("reason") or "VM host footprint: unknown"
        return
    record["vm_resident_gb"] = round(resident / GB, 3)
    raw, applied = vm_credit_gb(resident / GB, total_gb, available_gb,
                                estimate_gb)
    # credit_gb unrounded: it is the value admission adds (<= estimate)
    record.update(raw_credit_gb=round(raw, 3), credit_gb=applied,
                  overhead_gb=round(VM_CREDIT_OVERHEAD_GB
                                    + VM_CREDIT_OVERHEAD_FRACTION * total_gb,
                                    3))


# --- process-group evidence (S3) ----------------------------------------------------
#
# Host venue: the workload leads its own session (start_new_session), so its
# pid is also its process group id and (Linux) session id.  While the guard
# has not reaped the leader, that id cannot be reused by the kernel, so every
# process in the group (or session) is the job's.  The kill path therefore
# signals and verifies the group BEFORE reaping the leader.  A descendant
# that calls setsid() (or setpgid() on macOS, which has no session column
# in ps) leaves the group and is NOT tracked: this is cooperative tracking,
# not containment.

def _linux_live_zombie(pid: int) -> bool:
    """A Linux tgid whose main thread exited shows Z while other threads
    still run: it is live."""
    try:
        tids = os.listdir(f"/proc/{pid}/task")
    except OSError:
        return False
    for tid in tids:
        if tid == str(pid):
            continue
        try:
            text = Path(f"/proc/{pid}/task/{tid}/stat").read_text()
        except OSError:
            continue
        if text.rsplit(")", 1)[-1].split()[:1] not in (["Z"], ["X"]):
            return True
    return False


def host_group_members(leader: int) -> list[int] | None:
    """Live pids whose process group (or, on Linux, session) is ``leader``;
    zombies excluded.  ``None`` when the process table cannot be read, which
    is never read as "empty"."""
    linux = sys.platform.startswith("linux")
    columns = "pid=,pgid=,sid=,stat=" if linux else "pid=,pgid=,stat="
    try:
        result = subprocess.run(["ps", "-A", "-o", columns],
                                capture_output=True, text=True, timeout=10)
    except (subprocess.SubprocessError, OSError):
        return None
    if result.returncode != 0 or not result.stdout.strip():
        return None
    members = []
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) != (4 if linux else 3):
            continue
        try:
            ids = [int(f) for f in fields[:-1]]
        except ValueError:
            continue
        if leader not in ids[1:]:
            continue
        if fields[-1].startswith(("Z", "X")) and not (
                linux and _linux_live_zombie(ids[0])):
            continue
        members.append(ids[0])
    return members


def process_start(pid) -> str | None:
    """A start-time token for ``pid`` (Linux: exact start ticks; macOS:
    ``ps lstart`` in UTC, one-second resolution), or None if there is no
    such process or it cannot be read."""
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return None
    if pid <= 0:
        return None
    if sys.platform.startswith("linux"):
        try:
            fields = Path(f"/proc/{pid}/stat").read_text() \
                .rsplit(")", 1)[1].split()
            return f"ticks:{fields[19]}"
        except (OSError, IndexError):
            return None
    try:
        result = subprocess.run(["ps", "-o", "lstart=", "-p", str(pid)],
                                capture_output=True, text=True, timeout=10,
                                env={**os.environ, "LC_ALL": "C",
                                     "TZ": "UTC0"})
    except (subprocess.SubprocessError, OSError):
        return None
    text = " ".join(result.stdout.split())
    return f"lstart:{text}" if result.returncode == 0 and text else None


# Container venue: one in-container helper lists (and optionally signals)
# the job's live processes.  Members: the group or session led by the
# recorded in-container pid (unless that pid now belongs to a process with
# other start ticks: a recycled pid, the job's group is then provably gone),
# every process whose environment carries this job's SMC_GUARD_JOB marker
# (it finds the job before its pid is known, and a descendant that left the
# group but kept the marker), and then, to a fixpoint, every process sharing
# a member's session or group or whose parent is a member (C3: the lineage
# step of ACS's evidence, procs._job_closure).  A member that is neither
# marked nor in the recorded group or session is identifiable only through
# that lineage ("unmarked", e.g. the leader after `exec env -i CMD` when the
# pid file was never read): it is never signalled, and no process with such
# a descendant is signalled either (D4, as in ACS's procs._signal_plan),
# since killing it would erase the only link to the descendant; they stay
# members, so the end is never clean while they live.  Every target is
# re-proven (a fresh scan, then its start ticks and marker or group right
# before the signal).  A group is signalled with os.killpg, never with
# `kill -- -PGID`, which dash (Debian/Ubuntu /bin/sh) rejects.
_CONTAINER_GROUP = r"""
import json, os, signal, sys
signal.alarm(%d)
job, pgid, ticks, sig = sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4])
names = json.loads(sys.argv[5]) if len(sys.argv) > 5 else ['ACS_JOB_MARKER', 'SMC_GUARD_JOB']
markers = [(n + '=' + job).encode() for n in names] if job else []
me = os.getpid()
def stat(pid):
    try:
        f = open('/proc/%%d/stat' %% pid).read().rsplit(')', 1)[1].split()
    except (OSError, IndexError):
        return None
    if f[0] not in ('Z', 'X'):
        return f
    try:
        tids = os.listdir('/proc/%%d/task' %% pid)
    except OSError:
        return None
    for tid in tids:
        try:
            t = open('/proc/%%d/task/%%s/stat' %% (pid, tid)).read()
        except OSError:
            continue
        if t.rsplit(')', 1)[1].split()[0] not in ('Z', 'X'):
            return f
    return None
def marked(pid):
    if not markers:
        return False
    try:
        environ = open('/proc/%%d/environ' %% pid, 'rb').read().split(b'\0')
    except OSError:
        return False
    return any(m in environ for m in markers)
def leader():
    if pgid <= 0:
        return None, False
    f = stat(pgid)
    if f is None:
        return None, False
    return f[19], bool(ticks) and ticks != f[19]
def scan():
    lt, strange = leader()
    grouped = pgid > 0 and not strange
    table = {}
    for name in os.listdir('/proc'):
        if not name.isdigit() or int(name) == me:
            continue
        pid = int(name)
        f = stat(pid)
        if f is None:
            continue
        table[pid] = {'pid': pid, 'ppid': int(f[1]), 'pgrp': int(f[2]),
                      'sid': int(f[3]), 'ticks': f[19],
                      'marked': marked(pid),
                      'covered': grouped and pgid in (int(f[2]), int(f[3]))}
    member = {pid for pid, p in table.items() if p['covered'] or p['marked']}
    sessions = {table[pid]['sid'] for pid in member}
    groups = {table[pid]['pgrp'] for pid in member}
    if grouped:
        sessions.add(pgid)
        groups.add(pgid)
    while True:
        sessions.discard(0)
        groups.discard(0)
        grown = [pid for pid, p in table.items() if pid not in member and
                 (p['sid'] in sessions or p['pgrp'] in groups or
                  p['ppid'] in member)]
        if not grown:
            break
        for pid in grown:
            member.add(pid)
            sessions.add(table[pid]['sid'])
            groups.add(table[pid]['pgrp'])
    return lt, strange, grouped, table, member
def lineage_only(p):
    return not p['marked'] and not p['covered']
def guarded_below(table, pid):
    # a live descendant identifiable only through its ancestors (D4)
    children = {}
    for p in table.values():
        children.setdefault(p['ppid'], []).append(p['pid'])
    stack, seen = list(children.get(pid, [])), set()
    while stack:
        c = stack.pop()
        if c in seen or c not in table:
            continue
        seen.add(c)
        if lineage_only(table[c]):
            return True
        stack.extend(children.get(c, []))
    return False
def proven(p):
    # right before the signal: the same start ticks, and still marked (a
    # marker target) or still in the recorded group or session
    f = stat(p['pid'])
    if f is None or f[19] != p['ticks']:
        return False
    if p['marked']:
        return marked(p['pid']) and (stat(p['pid']) or [None] * 20)[19] == p['ticks']
    return pgid in (int(f[2]), int(f[3]))
leader_ticks, stranger, grouped, table, member = scan()
members = [{'pid': pid, 'ppid': table[pid]['ppid'], 'pgrp': table[pid]['pgrp'],
            'sid': table[pid]['sid']} for pid in sorted(member)]
unmarked = [m for m in members if lineage_only(table[m['pid']])]
signalled, skipped, errors = [], [], []
if sig:
    # re-proof: decide on a fresh scan; a target must be the same process
    # (same start ticks, same marker/group status) in both
    _, stranger2, grouped2, fresh, member2 = scan()
    targets = []
    for pid in sorted(member & member2):
        a, b = table[pid], fresh[pid]
        if lineage_only(b):
            continue                    # never signalled
        if a['ticks'] != b['ticks'] or a['marked'] != b['marked'] or \
                a['covered'] != b['covered']:
            skipped.append([pid, 'changed between scans; not proven'])
            continue
        if guarded_below(fresh, pid):
            skipped.append([pid, 'unmarked child; handle manually'])
            continue
        targets.append(b)
    held = {s[0] for s in skipped}
    group = [p for p in fresh.values() if grouped2 and p['pgrp'] == pgid]
    by_group = False
    if group and not any(p['pid'] in held or guarded_below(fresh, p['pid'])
                         for p in group):
        lt, strange = leader()
        if lt == leader_ticks and not strange:
            try:
                os.killpg(pgid, sig)
                signalled.append(-pgid)
                by_group = True
            except OSError as error:
                errors.append(str(error))
    for p in targets:
        if by_group and p['pgrp'] == pgid:
            continue
        if not proven(p):
            skipped.append([p['pid'], 'not proven right before the signal'])
            continue
        try:
            os.kill(p['pid'], sig)
            signalled.append(p['pid'])
        except OSError as error:
            errors.append(str(error))
print(json.dumps({'leader_ticks': leader_ticks, 'stranger': stranger,
                  'members': members, 'unmarked': unmarked,
                  'signalled': signalled, 'skipped': skipped,
                  'errors': errors}))
""" % SCRIPT_ALARM_S


def marker_names(markers=None) -> list[str]:
    """The names a container scan looks for: the job's own, then every
    name a version exported (more names only find more of the job)."""
    names = [m for m in (markers or ()) if isinstance(m, str) and m]
    return names + [n for n in KNOWN_MARKERS if n not in names]


def container_group(container: str, job_id: str | None, pgid: int | None,
                    ticks: str | None, sig: int = 0,
                    markers=None) -> dict | None:
    """Run the in-container helper; its parsed answer, or None when it could
    not run (no python3, docker failure or timeout, unparseable output)."""
    try:
        result = docker_exec(container, "python3", "-I", "-S", "-c",
                             _CONTAINER_GROUP, job_id or "",
                             str(int(pgid or 0)), ticks or "", str(int(sig)),
                             json.dumps(marker_names(markers)))
    except (subprocess.SubprocessError, OSError):
        return None
    if result.returncode != 0:
        return None
    try:
        answer = json.loads(result.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return None
    return answer if isinstance(answer, dict) and \
        isinstance(answer.get("members"), list) else None


# --- the in-flight marker (S3) ------------------------------------------------------

def guard_home() -> Path | None:
    """The guard home ACS gave this guard (``ACS_GUARD_HOME``, or the
    legacy ``SMC_GUARD_HOME``); None when neither is set or they differ
    (``guard_home_problem`` says which).  There is no built-in default:
    ACS always passes the configured home."""
    return None if guard_home_problem() else \
        Path(os.environ.get(ENV_HOME) or os.environ[LEGACY_ENV_HOME])


def guard_home_problem() -> str | None:
    home = os.environ.get(ENV_HOME)
    legacy = os.environ.get(LEGACY_ENV_HOME)
    if not home and not legacy:
        return (f"no guard home: {ENV_HOME} is not set (ACS sets it from "
                "the machine config)")
    if home and legacy and os.path.realpath(home) != \
            os.path.realpath(legacy):
        return (f"{ENV_HOME} ({home}) and {LEGACY_ENV_HOME} ({legacy}) name "
                "different guard homes")
    return None


def read_inflight(home: Path) -> dict | None:
    """The marker, ``None`` when there is none; an unreadable or malformed
    marker is returned as ``{"malformed": ...}`` (never read as absent)."""
    path = home / INFLIGHT_FILE
    try:
        text = path.read_text()
    except FileNotFoundError:
        return None
    except OSError as error:
        return {"malformed": f"unreadable: {error}"}
    try:
        data = json.loads(text)
    except ValueError:
        return {"malformed": text[:200]}
    return data if isinstance(data, dict) else {"malformed": text[:200]}


def write_inflight(home: Path, data: dict) -> None:
    home.mkdir(parents=True, exist_ok=True)
    target = home / INFLIGHT_FILE
    temp = home / f".{INFLIGHT_FILE}.{os.getpid()}.tmp"
    temp.write_text(json.dumps(data, sort_keys=True) + "\n")
    os.replace(temp, target)


def inflight_gone(marker: dict) -> tuple[bool, str]:
    """Is the workload a marker names proven gone?  ``(True, how)`` only on
    evidence; anything unknown is ``(False, why)``."""
    if "malformed" in marker or marker.get("schema") != INFLIGHT_SCHEMA:
        return False, "the in-flight marker is unreadable or malformed"
    job = marker.get("job_id")
    if marker.get("venue") == "container":
        target = marker.get("container_id") or marker.get("container")
        if not target:
            return False, "the marker names no container"
        try:
            probe = docker("inspect", "--format", "{{.State.Running}}",
                           str(target), check=False, timeout=15)
        except (subprocess.SubprocessError, OSError) as error:
            return False, f"docker unreachable ({type(error).__name__})"
        if probe.returncode != 0:
            if "no such" in (probe.stderr or "").lower():
                return True, f"container {target} no longer exists"
            return False, f"docker inspect failed: {probe.stderr.strip()}"
        if probe.stdout.strip() == "false":
            return True, f"container {target} is not running"
        scan = container_group(str(target), job, marker.get("container_pid"),
                               marker.get("container_ticks"),
                               markers=marker.get("markers"))
        if scan is None:
            return False, "the in-container process scan failed"
        if scan["members"]:
            pids = [m.get("pid") for m in scan["members"]]
            return False, f"in-container processes remain: {pids}"
        return True, "no in-container process of the job remains"
    pid = marker.get("pid")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return False, ("the guard died before it recorded the workload pid; "
                       "an operator must check and clear the marker")
    start = process_start(pid)
    members = host_group_members(pid)
    if members is None:
        return False, "the process table could not be read"
    note = ""
    if pid in members:
        if not (start is not None and marker.get("start")
                and start != marker.get("start")):
            return False, f"the workload leader {pid} is alive"
        # another process holds the leader's pid: set only that pid aside;
        # every other member of the group still counts (no assumption
        # about pid reuse, so at worst this stays unproven)
        members = [m for m in members if m != pid]
        note = " (its pid now belongs to another process)"
    if members:
        return False, f"processes remain in the job's group: {members}"
    return True, f"the leader and its process group are gone{note}"


# --- runlog -------------------------------------------------------------------

def runlog_line(path: Path, slice_id: str, job_id: str, kind: str,
                detail: str, header: str | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(RUNLOG_HEADER if header is None else header)
    with path.open("a") as handle:
        handle.write(f"- {utc_now()} | {slice_id} | {job_id} | {kind} | "
                     f"{detail}\n")


RUNLOG_HEADER = """# Run log

Append-only; one line per admitted run (its end) and one per guard action
of an admitted run (the resource guard, `agent_compute.guard`); refusals
and a busy lock are recorded in the attempt's guard.json, not here. Format:

    - <utc> | <slice> | <case>/<stamp> | <outcome> | steps=<n> | s/step=<x> | peak_rss=<gb> GB | wall=<s> s
    - <utc> | <slice> | <case>/<stamp> | guard:<pause|resume|kill|...> | <reason>
    - <utc> | <slice> | operator | dedicated | <window and caps>

Outcomes: completed, failed (nonzero exit), killed-budget, killed-walltime,
killed-paused, killed-interrupted, probe-stopped (60 s probe hit its limit).
"""


# --- the job ------------------------------------------------------------------

class Job:
    def __init__(self, args, command: list[str], profile: dict):
        self.args = args
        self.command = command
        self.profile = profile
        self.profile_name = profile["name"]
        self.container_cpus = int(self.profile["container_cpus"])
        self.runlog_tag = self.profile["runlog_tag"]
        self.light_budget_gb = float(profile["light_budget_gb"])
        self.disk_floor_gb = float(profile["disk_floor_gb"])
        self.vm_reserve_gb = float(profile["vm_reserve_gb"])
        self.memory_factor = float(profile["container_memory_factor"])
        self.project_root = (Path(args.project_root)
                             if getattr(args, "project_root", None) else None)
        self.reserve_detector = getattr(args, "reserve_detector", None)
        self.runlog_header = None
        if getattr(args, "runlog_header_file", None):
            self.runlog_header = Path(args.runlog_header_file).read_text()
        self.case = args.case
        self.stamp = stamp()
        self.job_id = f"{self.case}/{self.stamp}"
        self.venue = args.venue
        self.container = args.container
        self.threads = args.threads
        self.workers = args.workers
        # The dense-phase BLAS count is a SEPARATE knob from --threads.
        # --threads stays the process-wide pin the job is launched with;
        # this one is exported for the workload to read at runtime (for a
        # dense phase it runs with more threads).  Unset means the variable
        # is not exported at all, so nothing changes.
        self.dense_threads = getattr(args, "dense_threads", None)
        self.probe = bool(args.probe)
        self.light = bool(getattr(args, "light", False))
        self.baseline_path = Path(args.baseline)
        self.runlog_path = Path(args.runlog)
        self.out_dir = (Path(args.outputs) if args.outputs
                        else Path(args.outputs_root) / self.case / self.stamp)
        # the names this job exports (JOB_MARKER first, then the aliases)
        self.markers = [JOB_MARKER, *(getattr(args, "marker_alias", None)
                                      or [])]
        self.legacy_env = bool(getattr(args, "legacy_workload_env", False))
        self.slice_id = args.slice or os.environ.get(ENV_SLICE) or (
            os.environ.get(LEGACY_ENV_SLICE) if self.legacy_env else None) \
            or "-"
        self.budget_gb = float(args.budget_gb if args.budget_gb is not None
                               else self.profile["budget_gb"])
        self.floor_gb = float(args.floor_gb if args.floor_gb is not None
                              else self.profile["floor_gb"])
        self.interval = float(args.interval_s)
        self.resume_hold = float(args.resume_hold_s)
        self.max_pause = float(args.max_pause_s)
        self.grace = float(args.grace_s)
        self.timeout_s = (float(args.timeout_min) * 60.0
                          if args.timeout_min else None)
        self.probe_s = float(args.probe_s)
        self.sampler_stale_s = float(getattr(args, "sampler_stale_s",
                                             SAMPLER_STALE_S))
        self.sampler_pause_s = float(getattr(args, "sampler_pause_s",
                                             SAMPLER_PAUSE_S))
        self.sampler_failures = 0
        self.sampler_failed_since: float | None = None
        self.sampler_stale_logged = False
        self.pause_cause: str | None = None
        self.ladder: dict | None = None
        self.declared_reserve: float | None = None
        self.declared_only = False
        self.actions: list[dict] = []
        self.samples: list[tuple[float, float, float]] = []
        self.peak_rss = 0.0
        self.min_available = float("inf")
        self.outcome = "not-started"
        self.exit_code: int | None = None
        self.start_time = 0.0
        self.end_time = 0.0
        self.paused_since: float | None = None
        self.above_since: float | None = None
        self.paused_total = 0.0
        self.estimate_gb = 0.0
        self.row: dict | None = None
        self.docker_memory_gb: float | None = None
        self.vm: dict | None = None
        # the pre-flight memory decision and, on macOS container jobs, the
        # Docker VM credit with its evidence (None: not measured)
        self.vm_credit: dict | None = None
        self.admission_memory: dict | None = None
        self.proc: subprocess.Popen | None = None
        self.container_pid: int | None = None
        self.pid_file = f"/tmp/acs-guard-{self.stamp}.pid"
        # S3: docker update only when the caller opts in
        self.container_limits = bool(getattr(args, "container_limits", False))
        self.limits_applied = False
        self.container_id: str | None = None
        self.container_ticks: str | None = None
        self.leader_start: str | None = None
        # S3: SIGTERM/SIGINT are caught from before the launch (run())
        self.interrupted = False
        self.handlers_installed = False
        # S3: the verified end of the job's processes (None: not checked)
        self.cleanup: dict | None = None
        # C3: the container helper's last unmarked members, and what it
        # would not signal (D4)
        self.unmarked: list[int] = []
        self.not_signalled: list | None = None
        self.inflight: dict | None = None
        self.env = dict(os.environ)
        self.lock_handle = None
        self.sample_writer = None
        self.sample_file = None
        # D3: runlog lines only for an admitted attempt; what is noted
        # before the admission (stale lock, ladder) waits here, and is
        # dropped with the attempt if it is refused (guard.json keeps it)
        self.admitted = False
        self.runlog_pending: list[tuple[str, str]] = []
        # why this attempt was refused or found the lock busy (guard.json)
        self.refusal: str | None = None

    # -- bookkeeping ----------------------------------------------------------

    def runlog(self, kind: str, detail: str) -> None:
        """One runlog line; non-default profiles carry their host tag.
        Before the admission the line is held (``admit`` writes it)."""
        if not self.admitted:
            self.runlog_pending.append((kind, detail))
            return
        if self.runlog_tag:
            detail = f"{detail} | {self.runlog_tag}"
        runlog_line(path=self.runlog_path, slice_id=self.slice_id,
                    job_id=self.job_id, kind=kind, detail=detail,
                    header=self.runlog_header)

    def admit(self) -> None:
        """The attempt passed the lock and the pre-flight and is about to
        launch: from now on it writes runlog lines, starting with the notes
        held since it began."""
        self.admitted = True
        pending, self.runlog_pending = self.runlog_pending, []
        for kind, detail in pending:
            self.runlog(kind, detail)

    def action(self, kind: str, reason: str) -> None:
        record = {"utc": utc_now(), "t": round(time.time() - self.start_time, 1)
                  if self.start_time else 0.0, "action": kind, "reason": reason}
        self.actions.append(record)
        log(f"{kind}: {reason}")
        self.runlog(f"guard:{kind}", reason)
        self.write_record()

    def write_record(self) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        record = {
            "schema": GUARD_SCHEMA,
            "job_id": self.job_id, "case": self.case, "slice": self.slice_id,
            # the marker names this job's processes carry, and this
            # engine's own sha256
            "markers": self.markers, "engine_sha256": engine_sha256(),
            "profile": self.profile_name,
            "venue": self.venue, "container": self.container,
            "command": self.command, "probe": self.probe,
            "pid": self.proc.pid if self.proc is not None else None,
            "container_pid": self.container_pid,
            "started_utc": getattr(self, "started_utc", None),
            "ended_utc": getattr(self, "ended_utc", None),
            "outcome": self.outcome, "exit_code": self.exit_code,
            "wall_s": round((self.end_time or time.time()) - self.start_time, 1)
            if self.start_time else None,
            "paused_s": round(self.paused_total, 1),
            "peak_rss_gb": round(self.peak_rss / GB, 3),
            "min_host_available_gb": (round(self.min_available / GB, 2)
                                      if self.min_available != float("inf")
                                      else None),
            "estimate_gb": self.estimate_gb,
            "declared_reserve_gb": self.declared_reserve,
            "ladder": self.ladder,
            "thresholds": {"budget_gb": self.budget_gb,
                           "job_budget_gb": self.job_budget_gb(),
                           "floor_gb": self.floor_gb,
                           "resume_margin_gb": RESUME_MARGIN_GB,
                           "resume_hold_s": self.resume_hold,
                           "max_pause_s": self.max_pause,
                           "kill_grace_s": self.grace,
                           "timeout_s": self.timeout_s,
                           "sampler_stale_s": self.sampler_stale_s,
                           "sampler_pause_s": self.sampler_pause_s,
                           "probe_s": self.probe_s if self.probe else None},
            "threads": self.threads, "workers": self.workers, "nice": NICE,
            "dense_threads": self.dense_threads,
            "light": self.light,
            # D3 (additive): why the attempt was refused or found the lock
            # busy (no runlog line is written for it)
            "refusal": self.refusal,
            "docker": ({"memory_gb": self.docker_memory_gb,
                        "cpus": self.container_cpus, **(self.vm or {}),
                        # S3 (additive): whether `docker update` applied
                        # memory_gb/cpus to the container (--container-limits)
                        "limits_applied": self.limits_applied}
                       if self.venue == "container" else None),
            # additive: the pre-flight memory decision (host available,
            # the raw host sample, and the macOS Docker VM credit)
            "admission_memory": self.admission_memory,
            "sample_count": len(self.samples),
            "actions": self.actions,
            # S3 (additive): did the job's processes provably end?
            "cleanup": self.cleanup,
            "baseline": str(self.baseline_path.relative_to(self.project_root))
            if self.project_root is not None
            and self.baseline_path.is_relative_to(self.project_root)
            else str(self.baseline_path),
        }
        # Atomic: a reader (a test, a `tail`, the next session's triage) must
        # never see a half-written record, and the final write must not be
        # able to land as a truncated or lost update (once observed: a run
        # that exited 0 with its runlog line and its complete samples.csv,
        # but `guard.json` frozen at the last periodic snapshot, outcome
        # "running" -- a job that reads as unmanaged forever; mechanism not
        # established).  The write is a temp file plus os.replace, and `run`
        # repeats it in a finally.
        target = self.out_dir / "guard.json"
        temp = target.with_name(f".guard.json.{os.getpid()}.tmp")
        temp.write_text(json.dumps(record, indent=1, sort_keys=True) + "\n")
        os.replace(temp, target)

    # -- lock -----------------------------------------------------------------

    def take_lock(self) -> bool:
        home = guard_home()
        home.mkdir(parents=True, exist_ok=True)
        if self.light:
            # a light host job (tests, analysis, plots; measured peak <=
            # the light budget, one thread) may run
            # beside the one heavy job; it takes no lock and is capped at
            # 1.5 x LIGHT_BUDGET_GB instead of the host budget
            return True
        lock_path = home / "lock"
        handle = open(lock_path, "a+")
        deadline = time.time() + float(self.args.wait) if self.args.wait else None
        cleared: tuple[dict, str] | None = None
        waiting_logged = False
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                holder = ""
                try:
                    holder = (home / "current.json").read_text().strip()
                except OSError:
                    pass
                if deadline is None or time.time() > deadline:
                    handle.close()
                    self.refusal = (f"another job holds the lock: "
                                    f"{holder[:200]}")
                    log(f"refused: another job holds the lock {holder[:200]}")
                    return False
                time.sleep(min(5.0, self.interval))
                continue
            # S3: the flock is free, but a guard that died (or could not
            # verify its job's end) may have left its workload running: the
            # kernel released that guard's flock, not its processes.  Its
            # in-flight marker keeps the machine's one heavy slot taken
            # until the workload is proven gone from evidence.
            marker = read_inflight(home)
            if marker is None:
                break
            gone, why = inflight_gone(marker)
            if gone:
                try:
                    (home / INFLIGHT_FILE).unlink()
                except FileNotFoundError:
                    pass
                cleared = (marker, why)
                break
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            reason = (f"the in-flight workload of "
                      f"{marker.get('job_id', '<unknown job>')} is not proven "
                      f"gone ({why}); marker {home / INFLIGHT_FILE}")
            if deadline is None or time.time() > deadline:
                handle.close()
                self.refusal = reason
                log(f"refused: {reason}")
                return False
            if not waiting_logged:
                log(f"waiting: {reason}")
                waiting_logged = True
            time.sleep(min(5.0, self.interval))
        self.lock_handle = handle
        # The flock itself is released by the kernel when its holder dies, so
        # the lock never leaks; what can survive a SIGKILL is the descriptive
        # `current.json`.  Name that stale holder in the runlog instead of
        # silently overwriting it.
        stale_holder = None
        try:
            previous = json.loads((home / "current.json").read_text())
        except (OSError, ValueError):
            previous = None
        if isinstance(previous, dict) and not pid_alive(previous.get("pid")):
            stale_holder = previous.get("job_id")
        handle.seek(0)
        handle.truncate()
        handle.write(self.job_id + "\n")
        handle.flush()
        (home / "current.json").write_text(json.dumps({
            "job_id": self.job_id, "pid": os.getpid(), "venue": self.venue,
            "command": self.command, "started_utc": utc_now()}) + "\n")
        if stale_holder:
            self.action("stale-lock", f"reclaimed the lock from the dead "
                                      f"holder {stale_holder}")
        if cleared is not None:
            self.action("stale-inflight",
                        f"cleared the in-flight marker of "
                        f"{cleared[0].get('job_id')}: {cleared[1]}")
        return True

    def release_lock(self) -> None:
        if self.lock_handle is None:
            return
        home = guard_home()
        try:
            (home / "current.json").unlink()
        except OSError:
            pass
        try:
            fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_UN)
            self.lock_handle.close()
        except OSError:
            pass
        self.lock_handle = None

    # -- pre-flight -----------------------------------------------------------

    def refuse(self, reason: str) -> int:
        """A pre-flight refusal: recorded in guard.json (``refusal``) and on
        stderr (the ACS runner reads the reason there), never in the
        runlog (D3: a retried refusal would add a line per attempt)."""
        self.outcome = "refused"
        self.refusal = reason
        log(f"REFUSED: {reason}")
        self.write_record()
        return EXIT_REFUSED

    def job_budget_gb(self) -> float:
        """The memory ceiling this job is killed at, on the host venue.

        The profile budget, except when the reserve is a DECLARATION rather
        than a measurement -- a probe, or a run admitted on ``--reserve-gb``
        or a ladder's declared rung reserve with no measured row.  Those are
        held to 1.5 x what they declared, so a job that outgrows its own
        declaration is killed instead of the host.
        """
        if self.venue == "container":
            return float(self.docker_memory_gb or self.budget_gb)
        if (self.probe or self.declared_only) and self.estimate_gb > 0:
            return min(self.budget_gb,
                       float(math.ceil(self.estimate_gb * self.memory_factor)))
        return self.budget_gb

    def resolve_ladder(self) -> None:
        """Record whether this job drives a convergence ladder, and its
        heaviest rung's declared reserve.  Only with
        ``--reserve-detector smc-ladder``: otherwise no job is a ladder."""
        if self.reserve_detector != "smc-ladder":
            self.ladder = None
            return
        found = ladder_config_in(self.command, self.args.cwd,
                                 self.project_root)
        if found is None:
            self.ladder = None
            return
        path, config = found
        rung, reserve = ladder_reserve(config)
        self.ladder = {"config": str(path), "heaviest_rung": rung,
                       "declared_reserve_gb": reserve,
                       "rungs": [r.get("name") for r in config["ladder"]
                                 if isinstance(r, dict)]}

    def declared_reserve_gb(self) -> float | None:
        """The job's declared fallback reserve: ``--reserve-gb`` (or the
        legacy ``--estimate-gb``), else the ladder config's heaviest rung."""
        for value in (getattr(self.args, "reserve_gb", None),
                      self.args.estimate_gb):
            if value is not None:
                return float(value)
        if self.ladder is not None:
            return self.ladder["declared_reserve_gb"]
        return None

    def _vm_credit_text(self) -> str:
        """The VM credit's evidence, for a refusal message."""
        c = self.vm_credit or {}
        if c.get("reason"):
            return f"no Docker VM credit: {c['reason']}"
        return (f"VM resident {c['vm_resident_gb']:.1f} GB "
                f"({c['vm_source']}), guest MemTotal "
                f"{c['guest_mem_total_gb']:.1f} GB, MemAvailable "
                f"{c['guest_mem_available_gb']:.1f} GB, overhead "
                f"{c['overhead_gb']:.2f} GB: credit {c['raw_credit_gb']:.2f} "
                f"GB, applied up to the estimate")

    def preflight(self) -> int | None:
        baseline = load_baseline(self.baseline_path, self.profile_name)
        self.row = baseline["cases"].get(self.case)
        self.resolve_ladder()
        self.declared_reserve = self.declared_reserve_gb()
        measured_gb = (float(self.row["peak_rss_gb"]) if self.row is not None
                       else None)
        if self.probe:
            # A probe reserves what it declares (--estimate-gb), else the
            # case's last measurement, else the whole budget; the container
            # cap (1.5x) and the host kill threshold follow that reserve, so a
            # probe that outgrows its declaration is stopped, not the host.
            if self.declared_reserve is not None:
                self.estimate_gb = float(self.declared_reserve)
            elif measured_gb is not None:
                self.estimate_gb = measured_gb
            else:
                self.estimate_gb = self.budget_gb
            self.estimate_gb = min(max(self.estimate_gb, 0.25), self.budget_gb)
        elif measured_gb is None and self.declared_reserve is None:
            if self.ladder is not None:
                return self.refuse(
                    f"ladder job: '{self.ladder['config']}' declares no "
                    f"reserve_gb for its heaviest rung "
                    f"'{self.ladder['heaviest_rung']}' and case "
                    f"'{self.case}' has no measured peak in "
                    f"{self.baseline_path.name}. A ladder climbs to rungs a "
                    "short probe never reaches (1.5 x a probe that stayed "
                    "on the first rung is too small for the heaviest "
                    "one), so declare the heaviest rung's "
                    'reserve in the config ("reserve_gb": <GB> on that rung '
                    "or at the top level) or at launch (--reserve-gb).")
            probe_cmd = " ".join(shlex.quote(c) for c in self.command)
            return self.refuse(
                f"no measured peak for case '{self.case}' in "
                f"{self.baseline_path.name}; run first: guard "
                f"probe --case {self.case}"
                f"{' --venue container --container ' + self.container if self.venue == 'container' else ''}"
                f" -- {probe_cmd}  (or declare the reserve with --reserve-gb)")
        else:
            # admission is max(measured peak, declared reserve)
            self.estimate_gb = max(measured_gb or 0.0,
                                   self.declared_reserve or 0.0)
            self.declared_only = measured_gb is None
            if self.venue == "host" and self.estimate_gb > self.budget_gb:
                return self.refuse(
                    f"reserve {self.estimate_gb:.2f} GB exceeds the "
                    f"host budget {self.budget_gb:.1f} GB")
            if self.ladder is not None and self.declared_reserve is None:
                self.action(
                    "ladder-undeclared",
                    f"'{self.ladder['config']}' declares no reserve_gb for "
                    f"its heaviest rung '{self.ladder['heaviest_rung']}'; "
                    f"admitted on the case measurement "
                    f"{measured_gb:.2f} GB, which bounds only the rungs that "
                    "run used")
        if self.light:
            if self.venue != "host" or self.threads != 1 or self.workers != 1:
                return self.refuse("--light is for single-thread host jobs "
                                   "only (tests, analysis, plots)")
            if self.estimate_gb > self.light_budget_gb:
                return self.refuse(
                    f"--light needs a reserve <= {self.light_budget_gb:g} GB "
                    f"(estimate {self.estimate_gb:.2f} GB); run it as a "
                    "normal job")
            self.budget_gb = min(self.budget_gb, 1.5 * self.light_budget_gb)
        if self.workers > 1:
            if self.row is None or self.row.get("workers", 1) != 1:
                return self.refuse(
                    "worker fan-out needs a recorded single-worker peak")
            cores = os.cpu_count() or 2
            limit = allowed_workers(self.estimate_gb, self.budget_gb, cores)
            if self.workers > limit:
                return self.refuse(
                    f"{self.workers} workers exceed the allowed "
                    f"min(cores-2, budget/peak) = {limit}")
            self.estimate_gb = self.estimate_gb * self.workers
        memory = host_memory_sample()
        available_gb = memory["available"] / GB
        needed_gb = self.floor_gb + self.estimate_gb
        credit_gb = 0.0
        self.vm_credit = None
        if self.venue == "container" and docker_vm_separate():
            # macOS: memory the Docker VM already holds and that is free in
            # the guest is not new host memory for a container job (see
            # `vm_credit`); 0 whenever it cannot be measured
            # docker info's MemTotal checks that the container's
            # /proc/meminfo is the VM's own (VM_MEMTOTAL_TOLERANCE); a
            # failure here gives credit 0, and the container checks below
            # read docker info again and refuse as before
            docker_total = None
            try:
                self.vm = docker_vm_info()
                docker_total = self.vm["vm_memory_gb"] * GB
            except (subprocess.SubprocessError, OSError, ValueError,
                    KeyError):
                self.vm = None
            self.vm_credit = vm_credit(self.container, self.estimate_gb,
                                       docker_total)
            credit_gb = float(self.vm_credit["credit_gb"])
        self.admission_memory = {
            "host_available_gb": round(available_gb, 3)
            if math.isfinite(available_gb) else None,
            "floor_gb": self.floor_gb, "estimate_gb": self.estimate_gb,
            "needed_gb": round(needed_gb, 3), "vm_credit_gb": credit_gb,
            "sample": {key: (round(value / GB, 3)
                             if isinstance(value, float)
                             and math.isfinite(value) else value)
                       for key, value in memory.items()},
            "vm_credit": self.vm_credit}
        if self.vm_credit is not None:
            # the job must fit in the VM.  Larger than the guest's MemTotal:
            # it never fits (a final refusal).  Larger than its MemAvailable
            # only: the guest is busy, and the ACS runner waits and retries
            # (runner.RETRYABLE_REFUSALS has "Docker VM guest memory")
            # only on guest readings checked against docker info
            verified = self.vm_credit["guest_verified"] is True
            guest_total = self.vm_credit["guest_mem_total_gb"]
            guest_available = self.vm_credit["guest_mem_available_gb"]
            if verified and guest_total is not None \
                    and guest_total < self.estimate_gb:
                return self.refuse(
                    f"the job does not fit in the Docker VM: guest MemTotal "
                    f"{guest_total:.1f} GB < estimate "
                    f"{self.estimate_gb:.2f} GB")
            if verified and guest_available is not None \
                    and guest_available < self.estimate_gb:
                return self.refuse(
                    f"Docker VM guest memory: the job does not fit in the "
                    f"Docker VM now: guest MemAvailable "
                    f"{guest_available:.1f} GB < estimate "
                    f"{self.estimate_gb:.2f} GB (guest MemTotal "
                    f"{guest_total:.1f} GB)")
        if not (available_gb + credit_gb >= needed_gb):
            if self.vm_credit is None:
                return self.refuse(
                    f"host available memory {available_gb:.1f} GB < floor "
                    f"{self.floor_gb:.1f} GB + estimate "
                    f"{self.estimate_gb:.2f} GB")
            return self.refuse(
                f"host available memory {available_gb:.1f} GB + Docker VM "
                f"credit {credit_gb:.2f} GB < floor {self.floor_gb:.1f} GB "
                f"+ estimate {self.estimate_gb:.2f} GB; "
                + self._vm_credit_text())
        try:
            free_disk_gb = shutil.disk_usage(
                self.out_dir.parent if self.out_dir.parent.exists()
                else (self.project_root or Path(self.args.cwd))).free / GB
        except OSError:
            free_disk_gb = float("inf")
        if free_disk_gb < self.disk_floor_gb:
            return self.refuse(f"free disk {free_disk_gb:.1f} GB < "
                               f"{self.disk_floor_gb:.0f} GB")
        if self.venue == "container":
            try:
                # read again: the darwin credit path may already have it
                # (or have failed to read it)
                self.vm = docker_vm_info()
            except (subprocess.SubprocessError, OSError, ValueError) as error:
                return self.refuse(f"docker unreachable: {error}")
            if not container_running(self.container):
                return self.refuse(f"container '{self.container}' is not "
                                   "running")
            try:
                self.container_id = docker(
                    "inspect", "--format", "{{.Id}}", self.container,
                    check=False).stdout.strip() or None
            except (subprocess.SubprocessError, OSError):
                self.container_id = None
            # the per-job memory ceiling: the sampler kills the job above it
            # (job_budget_gb) whether or not it is applied to the container
            self.docker_memory_gb = docker_memory_cap_gb(
                self.estimate_gb, self.vm["vm_memory_gb"],
                self.memory_factor, self.vm_reserve_gb)
            if self.container_limits:
                # S3: opt-in only (--container-limits).  Without it the
                # guard never changes a container's limits: in a shared
                # container they belong to its owner, and the job is held
                # to the same ceiling by sampling alone
                cap = f"{int(self.docker_memory_gb)}g"
                try:
                    docker("update", f"--memory={cap}",
                           f"--memory-swap={cap}",
                           f"--cpus={self.container_cpus}", self.container)
                except subprocess.CalledProcessError as error:
                    return self.refuse(
                        f"docker update failed: {error.stderr.strip()}")
                self.limits_applied = True
        return None

    # -- launch ---------------------------------------------------------------

    def exported(self) -> list[str]:
        """The variables this guard sets for the workload (a container
        workload gets exactly these)."""
        names = [*THREAD_VARS, *self.markers, ENV_CASE, ENV_WORKERS]
        if self.dense_threads is not None:
            names.append(ENV_DENSE_THREADS)
        if self.legacy_env:
            names += [LEGACY_ENV_CASE, LEGACY_ENV_WORKERS]
            if self.dense_threads is not None:
                names.append(LEGACY_ENV_DENSE_THREADS)
        return names

    def job_env(self) -> dict[str, str]:
        env = dict(os.environ)
        for var in THREAD_VARS:
            env[var] = str(self.threads)
        for marker in self.markers:
            env[marker] = self.job_id
        values = {ENV_CASE: self.case, ENV_WORKERS: str(self.workers),
                  LEGACY_ENV_CASE: self.case,
                  LEGACY_ENV_WORKERS: str(self.workers)}
        if self.dense_threads is not None:
            values[ENV_DENSE_THREADS] = str(self.dense_threads)
            values[LEGACY_ENV_DENSE_THREADS] = str(self.dense_threads)
        for name in self.exported():
            if name in values:
                env[name] = values[name]
        return env

    # -- the in-flight marker (S3) ----------------------------------------------

    def mark_inflight(self, **fields) -> None:
        """Write (or update) this job's in-flight marker.  The first write,
        before anything is launched, must succeed (an OSError aborts the
        launch); later updates are best effort, since the first write
        already fails closed."""
        if self.light:
            return
        if self.inflight is None:
            self.inflight = {
                "schema": INFLIGHT_SCHEMA, "job_id": self.job_id,
                "venue": self.venue, "guard_pid": os.getpid(),
                "container": self.container,
                "container_id": self.container_id,
                "pid_file": (self.pid_file if self.venue == "container"
                             else None),
                "outputs": str(self.out_dir), "command": self.command,
                "phase": "launching", "pid": None, "start": None,
                "container_pid": None, "container_ticks": None,
                # S4: the ACS job this guard runs for (reconcile maps the
                # marker to that job's evidence); None on a test bypass.
                # G2a: its state dir (the nested pass-through checks the
                # enclosing job's claim there) and the marker names
                "acs_job": getattr(self.args, "acs_job", None),
                "acs_state": getattr(self.args, "acs_state", None),
                "markers": self.markers}
        self.inflight.update(fields)
        self.inflight["written_utc"] = utc_now()
        write_inflight(guard_home(), self.inflight)

    def update_inflight(self, **fields) -> None:
        try:
            self.mark_inflight(**fields)
        except OSError as error:
            log(f"could not update the in-flight marker: {error}")

    def clear_inflight(self) -> None:
        """Remove this job's marker: only once its processes are verified
        gone (or nothing was ever started).  The lock is still held."""
        if self.light or self.inflight is None:
            return
        try:
            (guard_home() / INFLIGHT_FILE).unlink()
        except FileNotFoundError:
            pass
        self.inflight = None

    def install_signal_handlers(self) -> None:
        """S3: SIGINT/SIGTERM are caught from BEFORE the launch, so a
        signal between ``Popen`` and the monitor (the container venue's pid
        discovery can take up to 30 s) stops the job through the normal
        kill path instead of killing the guard by the default disposition
        with the workload running.  Deliberately never restored, for the
        rest of this one-shot process's life (see ``monitor``'s
        ``finally``)."""
        def on_signal(signum, _frame):
            self.interrupted = True

        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, on_signal)
        self.handlers_installed = True

    def launch(self) -> None:
        env = self.job_env()
        self.mark_inflight(phase="launching")     # before anything starts
        self.start_time = time.time()
        self.started_utc = utc_now()
        if self.venue == "host":
            def preexec():
                try:
                    os.nice(NICE)
                except OSError:
                    pass
            self.proc = subprocess.Popen(
                self.command, env=env,
                cwd=str(self.args.cwd or self.project_root),
                start_new_session=True, preexec_fn=preexec)
            # the leader's start token is read after the first sample
            # (monitor), so the first sample is as early as possible
            self.update_inflight(phase="launched", pid=self.proc.pid)
            return
        env_flags: list[str] = []
        for var in self.exported():
            env_flags += ["-e", f"{var}={env[var]}"]
        workdir = self.args.container_workdir
        inner = " ".join(shlex.quote(c) for c in self.command)
        script = (f"echo $$ > {shlex.quote(self.pid_file)}; "
                  f"cd {shlex.quote(workdir)} && exec nice -n {NICE} {inner}")
        # A20: the exec starts in `/` and the script's own `cd` enters the
        # workdir, so the pid file is written even if that `cd` hangs
        self.proc = subprocess.Popen(
            ["docker", "exec", "-w", CONTAINER_EXEC_WORKDIR, *env_flags,
             self.container, "setsid", "-w", "bash", "-c", script])
        self.update_inflight(phase="launched", pid=self.proc.pid)
        deadline = time.time() + 30
        while time.time() < deadline and self.container_pid is None:
            if self.proc.poll() is not None:
                break
            try:
                result = docker_exec(self.container, "cat", self.pid_file,
                                     timeout=15)
            except (subprocess.SubprocessError, OSError):
                # S3: a hung or failed read is "not yet", never an exception
                # out of launch() with the workload started (an exception
                # here would release the lock over a live job)
                result = None
            if result is not None and result.returncode == 0 and \
                    result.stdout.strip().isdigit():
                self.container_pid = int(result.stdout.strip())
                break
            time.sleep(0.5)
        if self.container_pid is not None:
            scan = container_group(self.container, self.job_id,
                                   self.container_pid, None,
                                   markers=self.markers)
            if scan is not None and scan.get("leader_ticks"):
                self.container_ticks = str(scan["leader_ticks"])
            self.update_inflight(container_pid=self.container_pid,
                                 container_ticks=self.container_ticks)

    # -- sampling -------------------------------------------------------------

    def _sampler_failure(self, reason: str) -> float:
        """One failed sample: count it, start the stale clock, keep the last
        value.  A hiccup is not a breach; `monitor` decides when the
        streak has lasted long enough to log, pause and finally kill."""
        self.sampler_failures = getattr(self, "sampler_failures", 0) + 1
        if getattr(self, "sampler_failed_since", None) is None:
            self.sampler_failed_since = time.time()
        if self.sampler_failures == 1:
            self.action("sampler-unavailable", f"{reason}; keeping the last "
                                               "sample")
        return self.samples[-1][1] if self.samples else 0.0

    def _sampler_recovered(self) -> None:
        self.sampler_failures = 0
        self.sampler_failed_since = None
        self.sampler_stale_logged = False

    def sample_rss(self) -> float:
        fail_file = os.environ.get(ENV_SAMPLER_FAIL_FILE)
        if fail_file:
            try:
                blind = Path(fail_file).read_text().strip() not in ("", "0")
            except OSError:
                blind = False
            if blind:
                return Job._sampler_failure(
                    self, f"sampling disabled by {ENV_SAMPLER_FAIL_FILE}")
        if self.venue == "host":
            rss, _ = process_group_rss(self.proc.pid)
            if rss is None:
                return Job._sampler_failure(self, "ps failed or timed out")
            Job._sampler_recovered(self)
            return float(rss)
        if self.paused_since is not None:
            if getattr(self, "pause_cause", None) != "sampler":
                return self.samples[-1][1] if self.samples else 0.0
            # paused BECAUSE the sampler went blind: the container is
            # `docker pause`d, so `exec` cannot answer; the recovery test is
            # whether the daemon answers about it at all
            try:
                probe = docker("inspect", "--format", "{{.State.Running}}",
                               self.container, check=False, timeout=15)
            except (subprocess.TimeoutExpired, OSError) as exc:
                return Job._sampler_failure(
                    self, f"{type(exc).__name__} on docker inspect")
            if probe.returncode == 0:
                Job._sampler_recovered(self)
            return self.samples[-1][1] if self.samples else 0.0
        for path in ("/sys/fs/cgroup/memory.current",
                     "/sys/fs/cgroup/memory/memory.usage_in_bytes"):
            try:
                result = docker_exec(self.container, "cat", path, timeout=15)
            except (subprocess.TimeoutExpired, OSError) as exc:
                # a transient `docker exec` timeout in the sampler must not
                # raise out of monitor() and take the guard (and its job)
                # down with it.  A sampler hiccup is not a breach: keep the
                # last sample, count it, and only log.
                return Job._sampler_failure(
                    self, f"{type(exc).__name__} on docker exec")
            if result.returncode == 0 and result.stdout.strip().isdigit():
                Job._sampler_recovered(self)
                return float(result.stdout.strip())
        return self.samples[-1][1] if self.samples else 0.0

    def open_samples(self) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.sample_file = open(self.out_dir / "samples.csv", "w", newline="")
        self.sample_writer = csv.writer(self.sample_file)
        self.sample_writer.writerow(["t_s", "rss_gb", "host_available_gb",
                                     "paused"])

    def record_sample(self, t: float, rss: float, available: float) -> None:
        self.samples.append((t, rss, available))
        self.peak_rss = max(self.peak_rss, rss)
        if not math.isnan(available):
            self.min_available = min(self.min_available, available)
        self.sample_writer.writerow([f"{t:.1f}", f"{rss / GB:.3f}",
                                     f"{available / GB:.2f}",
                                     int(self.paused_since is not None)])
        self.sample_file.flush()

    # -- control --------------------------------------------------------------

    def pause(self, reason: str, cause: str = "floor") -> None:
        if self.paused_since is not None:
            return
        try:
            if self.venue == "host":
                os.killpg(self.proc.pid, signal.SIGSTOP)
            else:
                docker("pause", self.container)
        except (OSError, subprocess.SubprocessError) as error:
            self.action("pause-failed", f"{reason}; {error}")
            return
        self.paused_since = time.time()
        self.above_since = None
        self.pause_cause = cause
        self.action("pause", reason)

    def resume(self, reason: str) -> None:
        if self.paused_since is None:
            return
        try:
            if self.venue == "host":
                os.killpg(self.proc.pid, signal.SIGCONT)
            else:
                docker("unpause", self.container)
        except (OSError, subprocess.SubprocessError) as error:
            self.action("resume-failed", f"{reason}; {error}")
            return
        self.paused_total += time.time() - self.paused_since
        self.paused_since = None
        self.above_since = None
        self.pause_cause = None
        self.action("resume", reason)

    def _signal_job(self, sig: int) -> None:
        if self.venue == "host":
            # the leader is not reaped yet (the kill path never polls it
            # before the group is verified), so its pid still names this
            # job's group and session: nothing else can hold it
            try:
                os.killpg(self.proc.pid, sig)
            except ProcessLookupError:
                pass
            except OSError as error:
                log(f"killpg {self.proc.pid}: {error}")
            for pid in host_group_members(self.proc.pid) or []:
                # Linux: session members that changed their process group
                try:
                    os.kill(pid, sig)
                except OSError:
                    pass
            return
        if self.paused_since is not None:
            docker("unpause", self.container, check=False)
            self.paused_total += time.time() - self.paused_since
            self.paused_since = None
        # S3 (dash): os.killpg in the container, never `kill -- -PGID`
        scan = container_group(self.container, self.job_id,
                               self.container_pid, self.container_ticks,
                               sig=int(sig), markers=self.markers)
        if scan is not None:
            # C3: what the helper would not signal (a process with a live
            # unmarked descendant, or one not proven) is recorded
            if scan.get("skipped"):
                self.not_signalled = scan["skipped"]
                log(f"not signalled (handle by hand): {scan['skipped']}")
            return
        # no helper (no python3 in the image, or docker failed): bash's
        # builtin kill, which accepts `--` (dash's does not).  The group's
        # end then cannot be verified, so the job is not reported clean.
        name = {signal.SIGTERM: "TERM", signal.SIGKILL: "KILL"}[sig]
        if self.container_pid is not None:
            try:
                docker_exec(self.container, "bash", "-c",
                            f"kill -s {name} -- -{self.container_pid} "
                            f"2>/dev/null; kill -s {name} "
                            f"{self.container_pid} 2>/dev/null; true",
                            timeout=15)
            except (subprocess.SubprocessError, OSError):
                pass
        else:
            self.proc.send_signal(sig)

    def _live_members(self) -> list[int] | None:
        """The job's live processes now; None when that cannot be read."""
        if self.venue == "host":
            return host_group_members(self.proc.pid)
        scan = container_group(self.container, self.job_id,
                               self.container_pid, self.container_ticks,
                               markers=self.markers)
        if scan is None:
            return None
        # C3: members include descendants linked only by lineage
        self.unmarked = sorted(int(m["pid"]) for m in
                               scan.get("unmarked") or [])
        return sorted(int(m["pid"]) for m in scan["members"])

    def _cleanup_evidence(self, members: list[int] | None, when: str,
                          note: str | None = None) -> dict:
        method = ("host process group" +
                  (" and session" if sys.platform.startswith("linux") else "")
                  if self.venue == "host" else
                  "in-container group, session and SMC_GUARD_JOB marker")
        if members is None:
            return {"verified": False, "clean": False, "survivors": None,
                    "method": method, "checked": when,
                    "reason": "the job's processes could not be listed"}
        evidence = {"verified": True, "clean": not members,
                    "survivors": members, "method": method, "checked": when,
                    "reason": (None if not members else
                               f"processes of the job remain {when}")}
        if note:
            evidence["note"] = note
        unmarked = [pid for pid in getattr(self, "unmarked", None) or []
                    if pid in members]
        if unmarked:
            # C3: the guard never signals these, nor a marked parent of one
            evidence["unmarked_survivors"] = unmarked
            evidence["reason"] += (": some without the job marker, linked "
                                   "only through a parent (not signalled; "
                                   "handle by hand)")
        if getattr(self, "not_signalled", None) and members:
            evidence["not_signalled"] = self.not_signalled
        return evidence

    def _wait_until_gone(self, timeout: float) -> list[int] | None:
        """Poll the job's processes until none is left (and, in the
        container venue, the `docker exec` client has exited) or
        ``timeout``; return the last reading."""
        deadline = time.time() + timeout
        step = 0.2 if self.venue == "host" else 0.5
        while True:
            members = self._live_members()
            done = members == [] and (self.venue == "host" or
                                      self.proc.poll() is not None)
            if done or time.time() >= deadline:
                return members
            time.sleep(step)

    def _stop_job(self) -> None:
        """S3: TERM the job's processes, KILL whatever outlives the grace,
        and record whether they are provably gone (``self.cleanup``).
        Waiting only for the LEADER would leave members that outlive it
        unkilled and unchecked."""
        self._signal_job(signal.SIGTERM)
        members = self._wait_until_gone(self.grace)
        if members != [] or (self.venue == "container" and
                             self.proc.poll() is None):
            self._signal_job(signal.SIGKILL)
            members = self._wait_until_gone(self.grace)
        self.cleanup = self._cleanup_evidence(members, "after the kill")
        try:
            self.proc.wait(timeout=self.grace)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            try:
                self.proc.wait(timeout=self.grace)
            except subprocess.TimeoutExpired:
                pass

    def _verify_end(self) -> dict:
        """The job's leader ended by itself (and is reaped): is anything of
        the job left?  Survivors are recorded, not killed (never left
        running unreported)."""
        members = self._live_members()
        if self.venue == "host" and members and self.proc.pid in members:
            # A live process at the reaped leader's pid is not the leader.
            # Only that pid is set aside: every OTHER member of the group
            # still counts, so this needs no assumption about pid reuse (a
            # stranger's own group-mates can only make it unclean).
            others = [pid for pid in members if pid != self.proc.pid]
            return self._cleanup_evidence(
                others, "after the leader ended",
                note=f"pid {self.proc.pid} now belongs to another process")
        return self._cleanup_evidence(members, "after the leader ended")

    def kill(self, outcome: str, reason: str) -> None:
        self.outcome = outcome
        self.action("kill", reason)
        self._stop_job()

    def stop_probe(self) -> None:
        self.outcome = "probe-stopped"
        log(f"probe limit {self.probe_s:.0f} s reached; stopping the job")
        self._stop_job()

    # -- main loop ------------------------------------------------------------

    def monitor(self) -> None:
        # The SIGINT/SIGTERM handlers were installed by run() before the
        # launch (S3); they set self.interrupted.
        job_budget = self.job_budget_gb()
        floor = self.floor_gb * GB
        resume_level = (self.floor_gb + RESUME_MARGIN_GB) * GB
        next_sample = self.start_time
        try:
            while True:
                if self.proc.poll() is not None and self.outcome in (
                        "not-started", "running"):
                    self.outcome = "completed" if self.proc.returncode == 0 \
                        else "failed"
                    break
                if self.proc.poll() is not None:
                    break
                now = time.time()
                if now < next_sample:
                    time.sleep(min(0.1, next_sample - now))
                    continue
                next_sample = now + self.interval
                t = now - self.start_time
                rss = self.sample_rss()
                available = host_memory_sample()["available"]
                self.record_sample(t, rss, available)
                if self.venue == "host" and self.leader_start is None and \
                        len(self.samples) == 1:
                    # the leader is not reaped before this loop sees it end,
                    # so the start read here is the leader's own (a marker
                    # without it only makes a later check more cautious)
                    self.leader_start = process_start(self.proc.pid)
                    self.update_inflight(start=self.leader_start)
                if len(self.samples) % 30 == 0:
                    self.write_record()
                if self.interrupted:
                    self.kill("killed-interrupted", "guard received SIGINT/"
                              "SIGTERM")
                    break
                if rss > job_budget * GB:
                    self.kill("killed-budget",
                              f"job memory {rss / GB:.2f} GB > budget "
                              f"{job_budget:.2f} GB")
                    break
                if self.timeout_s is not None and \
                        t - self.paused_total > self.timeout_s:
                    self.kill("killed-walltime",
                              f"wall time {t - self.paused_total:.0f} s > "
                              f"ceiling {self.timeout_s:.0f} s (class: wall "
                              "time)")
                    break
                if self.probe and t >= self.probe_s:
                    self.stop_probe()
                    break
                # a stale sample is not a sample.  After the
                # declared stale window the streak is logged; after the
                # longer window the job is PAUSED rather than run blind; it
                # resumes if sampling recovers and is killed by the existing
                # paused ceiling below if it never does.  Never unmanaged,
                # never trusted indefinitely.
                if self.sampler_failed_since is None:
                    if self.pause_cause == "sampler":
                        self.resume("sampling recovered")
                else:
                    stale_for = now - self.sampler_failed_since
                    if stale_for >= self.sampler_stale_s and \
                            not self.sampler_stale_logged:
                        self.sampler_stale_logged = True
                        self.action("sampler-stale",
                                    f"no sample for {stale_for:.0f} s "
                                    f"({self.sampler_failures} consecutive "
                                    f"failures) > "
                                    f"{self.sampler_stale_s:.0f} s")
                    if stale_for >= self.sampler_pause_s and \
                            self.paused_since is None:
                        self.pause(f"no sample for {stale_for:.0f} s > "
                                   f"{self.sampler_pause_s:.0f} s: the job "
                                   "is unobservable", cause="sampler")
                if self.paused_since is None:
                    if not math.isnan(available) and available < floor:
                        self.pause(f"host available {available / GB:.1f} GB "
                                   f"< floor {self.floor_gb:.1f} GB")
                else:
                    paused_for = now - self.paused_since
                    if paused_for > self.max_pause:
                        self.kill("killed-paused",
                                  f"paused {paused_for:.0f} s > "
                                  f"{self.max_pause:.0f} s")
                        break
                    if self.pause_cause == "sampler":
                        pass          # only recovered sampling resumes it
                    elif not math.isnan(available) and available > resume_level:
                        if self.above_since is None:
                            self.above_since = now
                        elif now - self.above_since >= self.resume_hold:
                            self.resume(f"host available {available / GB:.1f}"
                                        f" GB > floor + {RESUME_MARGIN_GB:.0f}"
                                        f" GB for {self.resume_hold:.0f} s")
                    else:
                        self.above_since = None
        finally:
            # The pre-guard SIGINT/SIGTERM handlers are deliberately NOT
            # restored here.  `run()`'s post-monitor bookkeeping
            # (record_baseline, final_runlog, write_record, release_lock,
            # and for a container the pid-file cleanup) takes real time,
            # and a supervisor that re-sends SIGTERM to a cancelled guard on
            # every poll tick until it exits would otherwise land a second
            # SIGTERM in that window and kill the guard by the default
            # disposition (exit -15) instead of its own graceful path
            # (`EXIT_KILLED` = 5); observed as a flaky test under CPU
            # contention.  Keeping the handler absorbs every further signal
            # harmlessly (it only sets an already-consumed flag), whatever
            # the window's size or cause.
            if self.cleanup is None:
                if self.proc.poll() is None:
                    try:
                        self.proc.wait(timeout=self.grace)
                    except subprocess.TimeoutExpired:
                        self._stop_job()
                if self.cleanup is None:
                    self.cleanup = self._verify_end()
            self.exit_code = self.proc.returncode
            self.end_time = time.time()
            self.ended_utc = utc_now()
            if self.venue == "container":
                try:
                    docker_exec(self.container, "rm", "-f", self.pid_file,
                                timeout=15)
                except (subprocess.SubprocessError, OSError):
                    pass

    # -- records --------------------------------------------------------------

    def record_baseline(self) -> None:
        wall = self.end_time - self.start_time - self.paused_total
        completed = self.outcome == "completed"
        if not completed and not self.probe:
            return
        baseline = load_baseline(self.baseline_path, self.profile_name)
        old = baseline["cases"].get(self.case) or {}
        this_peak_gb = round(self.peak_rss / GB, 3)
        peak_gb = this_peak_gb
        kind = "measured" if completed else "probe"
        if old.get("kind") == "measured" and kind == "probe":
            # a probe never overwrites a completed measurement
            return
        # The reserve a case key stands for is the LARGEST thing that ever
        # ran under it, so `peak_rss_gb` must never DECREASE when a cheaper
        # command reuses the key -- whatever the previous row's kind was
        # (otherwise a heavy probe followed by a cheap completed run under
        # the same key would lower the reserve, and the next heavy job would
        # be pre-flighted against the cheap one and given too small a
        # container cap).  `last_peak_rss_gb` keeps this run's own number
        # so the history is not lost.
        if self.workers == old.get("workers", 1):
            peak_gb = max(peak_gb, float(old.get("peak_rss_gb", 0.0)))
        row = {"venue": self.venue, "kind": kind, "peak_rss_gb": peak_gb,
               "wall_s": round(wall, 1), "threads": self.threads,
               "workers": self.workers, "measured_utc": utc_now(),
               "job_id": self.job_id, "completed": completed,
               "command": " ".join(shlex.quote(c) for c in self.command)}
        if peak_gb != this_peak_gb:
            row["last_peak_rss_gb"] = this_peak_gb
        if self.docker_memory_gb is not None:
            row["docker_memory_gb"] = self.docker_memory_gb
        if self.args.units:
            name, _, count = self.args.units.partition("=")
            try:
                count_value = float(count)
            except ValueError:
                count_value = 0.0
            if count_value > 0 and completed:
                row["units"] = {"name": name, "count": count_value,
                                f"s_per_{name.rstrip('s')}": round(
                                    wall / count_value, 3)}
        if self.args.note:
            row["note"] = self.args.note
        baseline["cases"][self.case] = row
        save_baseline(self.baseline_path, baseline)
        log(f"baseline row '{self.case}' = {kind} peak {peak_gb:.3f} GB, "
            f"wall {wall:.0f} s")

    def final_runlog(self) -> None:
        wall = self.end_time - self.start_time
        steps = "-"
        s_per_step = "-"
        if self.args.units:
            name, _, count = self.args.units.partition("=")
            try:
                count_value = float(count)
            except ValueError:
                count_value = None
            if count_value is not None and self.outcome == "completed":
                steps = f"{count_value:g} {name}"
                if count_value > 0:
                    s_per_step = f"{(wall - self.paused_total) / count_value:.2f}"
            elif count_value is not None:
                # a killed/refused/probe-stopped job never reached the
                # DECLARED unit count, so dividing wall time by it understates
                # the real per-unit cost (a cancelled solve would log a rate
                # for steps it never took).  The guard has no protocol for
                # how many units a partial job actually reached (that is
                # job-specific progress, not something `--units` declares),
                # so a non-completed outcome reports the declared target
                # labelled as such and OMITS the rate rather than mislead.
                steps = f"{count_value:g} {name} (declared)"
        detail = (f"steps={steps} | s/step={s_per_step} | "
                  f"peak_rss={self.peak_rss / GB:.2f} GB | wall={wall:.0f} s")
        if self.venue == "container":
            detail += f" | docker_memory={self.docker_memory_gb:g}g"
        if self.probe:
            detail += " | probe"
        if self.declared_reserve is not None:
            detail += f" | reserve={self.declared_reserve:g} GB"
        if self.light:
            detail += " | light"
        if self.dense_threads is not None:
            detail += f" | dense_threads={self.dense_threads}"
        if self.exit_code not in (0, None):
            detail += f" | exit={self.exit_code}"
        self.runlog(self.outcome, detail)

    def run(self) -> int:
        self.outcome = "running"
        try:
            if not self.take_lock():
                self.outcome = "lock-busy"
                return EXIT_LOCKED
            code = self.preflight()
            if code is not None:
                return code
            self.install_signal_handlers()
            if self.interrupted:
                # S3: a signal during pre-flight; nothing has been started
                self.outcome = "killed-interrupted"
                self.action("kill", "guard received SIGINT/SIGTERM before "
                                    "the launch; nothing was started")
                return EXIT_KILLED
            self.admit()
            self.open_samples()
            try:
                self.launch()
            except (OSError, subprocess.SubprocessError) as error:
                self.outcome = "launch-failed"
                self.end_time = time.time()
                self.runlog("launch-failed", str(error))
                if self.proc is None:
                    self.clear_inflight()     # nothing was started
                else:
                    # S3: started, then failed: stop and verify it
                    self._stop_job()
                    if self.cleanup.get("clean") is True:
                        self.clear_inflight()
                self.write_record()
                return EXIT_VENUE
            self.write_record()
            log(f"job {self.job_id} started ({self.venue}"
                f"{', ' + self.container if self.venue == 'container' else ''}"
                f"; estimate {self.estimate_gb:.2f} GB; threads "
                f"{self.threads}; {'probe ' if self.probe else ''}"
                f"outputs {self.out_dir})")
            self.monitor()
            if (self.cleanup or {}).get("clean") is True:
                self.clear_inflight()
            else:
                # S3: never a clean end without evidence.  The in-flight
                # marker stays, so the next guard waits until the workload
                # is proven gone.
                evidence = self.cleanup or self._cleanup_evidence(None, "")
                evidence["outcome"] = self.outcome
                self.cleanup = evidence
                self.outcome = OUTCOME_UNCLEAN
                self.action("cleanup-unverified",
                            f"{evidence.get('reason')}: survivors "
                            f"{evidence.get('survivors')} (job outcome "
                            f"{evidence['outcome']}); the in-flight marker "
                            "stays until they are proven gone")
                self.update_inflight(phase="unclean",
                                     survivors=evidence.get("survivors"))
            self.record_baseline()
            self.final_runlog()
            self.write_record()
            log(f"job {self.job_id} {self.outcome}: peak "
                f"{self.peak_rss / GB:.2f} GB, wall "
                f"{self.end_time - self.start_time:.0f} s, exit "
                f"{self.exit_code}")
            if self.outcome.startswith("killed") or \
                    self.outcome == OUTCOME_UNCLEAN:
                return EXIT_KILLED
            if self.outcome == "probe-stopped":
                return 0
            return int(self.exit_code or 0)
        finally:
            if self.sample_file is not None:
                self.sample_file.close()
            # The record is the job's evidence: write it on EVERY exit path,
            # including one that raised, and never fail the job because the
            # write failed -- say so instead.
            if self.outcome != "not-started":
                try:
                    self.write_record()
                except OSError as error:
                    log(f"could not write {self.out_dir / 'guard.json'}: "
                        f"{error}")
            self.release_lock()
            if self.handlers_installed:
                # S3: interpreter finalization resets Python-level handlers
                # to SIG_DFL (but leaves SIG_IGN alone), so a repeated
                # cancel landing while this process exits would still kill
                # it by default disposition (-15 instead of 5).  The job is
                # over; ignore further signals for the last milliseconds.
                for sig in (signal.SIGINT, signal.SIGTERM):
                    signal.signal(sig, signal.SIG_IGN)


# --- the ACS claim (S4) -------------------------------------------------------------

def sandbox_home_problem() -> str | None:
    """Why the guard home is not a sandbox (None: it is): the test bypass
    never reaches a real lock, so the home must be set, must hold the test
    harness's sentinel file (``SANDBOX_SENTINEL``, a regular file, not a
    link: no production home has it, whatever its path) and must not be
    the legacy home, the default ACS one, or the guard home inside any ACS
    state dir (a sibling ``admission.lock`` or ``jobs``)."""
    home = guard_home()
    if home is None:
        return f"it needs a sandbox guard home ({guard_home_problem()})"
    sentinel = Path(home) / SANDBOX_SENTINEL
    if sentinel.is_symlink() or not sentinel.is_file():
        return (f"it needs a sandbox guard home holding the test harness's "
                f"{SANDBOX_SENTINEL} file; {home} has none")
    real = [Path.home() / ".smc-guard",
            Path.home() / ".local" / "state" / "agent-compute" / "guard"]
    resolved = os.path.realpath(home)
    parent = Path(resolved).parent
    if any(resolved == os.path.realpath(r) for r in real) or \
            (parent / "admission.lock").exists() or \
            (parent / "jobs").is_dir():
        return (f"it needs a guard home other than a real one ({home} is "
                f"{', '.join(map(str, real))} or an ACS state dir's)")
    return None


def nested_claim_problem(value: str) -> str | None:
    """Why a guard started inside a marked job may not pass straight
    through (None: it may).  The marker alone proves nothing (any process
    can set a variable): the enclosing ACS job must be claimed and
    unfinished, and the guard home's in-flight marker must be that job's
    guard with this marker value.  Cooperative, like the run claim."""
    acs_job = os.environ.get(ENV_JOB_ID)
    if not acs_job:
        return (f"no enclosing ACS job ({ENV_JOB_ID} is not set): a job "
                "marker alone proves nothing")
    if not re.fullmatch(_ACS_JOB_ID, acs_job) or ".." in acs_job:
        return f"{ENV_JOB_ID} {acs_job!r} is not an ACS job id"
    home = guard_home()
    if home is None:
        return guard_home_problem()
    marker = read_inflight(home)
    if not isinstance(marker, dict) or "malformed" in marker:
        return (f"the guard home {home} holds no in-flight marker of a "
                "running job")
    if marker.get("acs_job") != acs_job:
        return (f"the in-flight marker belongs to ACS job "
                f"{marker.get('acs_job')}, not {acs_job}")
    if marker.get("job_id") != value:
        return (f"the marker value {value!r} is not the enclosing guard's "
                f"job ({marker.get('job_id')})")
    state_dir = marker.get("acs_state")
    if not isinstance(state_dir, str) or not state_dir:
        return "the in-flight marker names no ACS state dir"
    path = Path(state_dir) / "jobs" / acs_job / "state.json"
    try:
        state = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        return f"cannot read the ACS job state {path}: {error}"
    if not isinstance(state, dict):
        return f"the ACS job state {path} is malformed"
    if state.get("outcome"):
        return f"ACS job {acs_job} already has an outcome ({state['outcome']})"
    runner = state.get("runner")
    pid = runner.get("pid") if isinstance(runner, dict) else None
    if not isinstance(pid, int) or isinstance(pid, bool) or \
            not pid_alive(pid):
        return f"ACS job {acs_job} has no live claimed runner"
    return None


def acs_claim_problem(args) -> str | None:
    """Why this ``run``/``probe`` may not start (None: it may).

    The internal guard is ACS's backend, not a public route: it runs only
    for the ACS runner of a claimed job.  ``--acs-state DIR`` and
    ``--acs-job ID`` must name ``DIR/jobs/ID/state.json`` whose ``runner``
    pid is this process's parent and which has no outcome.  Cooperative
    only (same-user files can be forged); not a security boundary."""
    if os.environ.get(ENV_TEST_UNCLAIMED) == "1":
        problem = sandbox_home_problem()
        if problem is None:
            return None
        return (f"{ENV_TEST_UNCLAIMED} is for sandbox tests only: {problem}")
    job, state_dir = args.acs_job, args.acs_state
    if not job or not state_dir:
        return ("the internal guard runs only for an ACS job: submit the "
                "workload with `agent-compute submit` (or `agent-compute "
                "run`); direct `run`/`probe` calls are refused")
    if not re.fullmatch(_ACS_JOB_ID, job) or ".." in job:
        return f"--acs-job {job!r} is not an ACS job id"
    path = Path(state_dir) / "jobs" / job / "state.json"
    try:
        state = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        return f"cannot read the ACS job state {path}: {error}"
    if not isinstance(state, dict):
        return f"the ACS job state {path} is malformed"
    if state.get("outcome"):
        return f"ACS job {job} already has an outcome ({state['outcome']})"
    runner = state.get("runner")
    pid = runner.get("pid") if isinstance(runner, dict) else None
    if not isinstance(pid, int) or isinstance(pid, bool) or \
            pid != os.getppid():
        return (f"ACS job {job} is not claimed by this guard's parent "
                f"(runner pid {pid}, parent {os.getppid()}): only the job's "
                "ACS runner may run it")
    return None


# --- CLI ------------------------------------------------------------------------

def add_run_arguments(parser: argparse.ArgumentParser) -> None:
    # The project and machine arguments below have no argparse `required`:
    # main() checks them only AFTER the nested pass-through, so a guard
    # invoked inside a (claimed) guarded job passes through without them.
    parser.add_argument("--case", required=True,
                        help="case key in the project's baseline table")
    parser.add_argument("--profile-file", default=None,
                        help="machine profile JSON (budget, floor, CPU quota, "
                             "runlog tag; see load_profile).  Required.")
    parser.add_argument("--acs-job", default=None,
                        help="the ACS job this guard runs for (S4: required "
                             "with --acs-state; set by the ACS runner)")
    parser.add_argument("--acs-state", default=None,
                        help="the ACS state directory holding that job's "
                             "claim")
    parser.add_argument("--marker-alias", action="append", default=[],
                        metavar="NAME",
                        help="also export the job marker under NAME (the "
                             "machine config's backend.marker_aliases; "
                             "repeatable)")
    parser.add_argument("--legacy-workload-env", action="store_true",
                        help="also export the legacy names of the workload "
                             "variables and read the legacy slice variable "
                             "(the machine config's "
                             "backend.legacy_workload_env)")
    parser.add_argument("--project-root", default=None,
                        help="project root: the default --cwd, the last "
                             "ladder-config search base, the disk check's "
                             "fallback and the base of the record's "
                             "baseline path")
    parser.add_argument("--venue", choices=("host", "container"),
                        default="host")
    parser.add_argument("--container", default=None,
                        help="running container name (container venue)")
    parser.add_argument("--container-workdir", default=None,
                        help="in-container working directory (required for "
                             "the container venue)")
    parser.add_argument("--container-limits", action="store_true",
                        help="container venue: apply the per-job memory cap "
                             "and the profile's CPU quota to the container "
                             "with `docker update` (opt-in; without it the "
                             "container's limits are never changed and the "
                             "cap is enforced by sampling only)")
    parser.add_argument("--dense-threads", type=int, default=None,
                        help="OpenBLAS threads for the DENSE bulk phase only "
                             f"(exports {ENV_DENSE_THREADS}; leaves --threads, "
                             "the process-wide pin, untouched)")
    parser.add_argument("--threads", type=int, default=1,
                        help="BLAS/OpenMP threads (1 for post-processing)")
    parser.add_argument("--workers", type=int, default=1,
                        help="worker fan-out; needs a single-worker peak")
    parser.add_argument("--slice", default=None, help="slice id for the runlog")
    parser.add_argument("--units", default=None,
                        help="NAME=COUNT (e.g. steps=100) for s/unit in the "
                             "runlog and baseline")
    parser.add_argument("--note", default=None, help="baseline row note")
    parser.add_argument("--outputs", default=None,
                        help="directory for guard.json/samples.csv (this or "
                             "--outputs-root is required)")
    parser.add_argument("--outputs-root", default=None,
                        help="records root: outputs go to "
                             "<root>/<case>/<stamp>")
    parser.add_argument("--cwd", default=None,
                        help="host job working dir (default: --project-root)")
    parser.add_argument("--timeout-min", type=float, default=None,
                        help="wall-time ceiling in minutes")
    parser.add_argument("--wait", type=float, default=None,
                        help="seconds to wait for the lock instead of refusing")
    parser.add_argument("--light", action="store_true",
                        help="light host job (tests, analysis, plots; peak <= "
                             "the profile's light budget, default "
                             f"{LIGHT_BUDGET_GB:g} GB, one thread): runs beside "
                             "the heavy job without the lock, still floor-checked "
                             "and logged")
    parser.add_argument("--budget-gb", type=float, default=None,
                        help="host process-group budget (default: profile)")
    parser.add_argument("--floor-gb", type=float, default=None,
                        help="host available-memory floor (default: profile)")
    parser.add_argument("--interval-s", type=float, default=SAMPLE_INTERVAL_S)
    parser.add_argument("--resume-hold-s", type=float, default=RESUME_HOLD_S)
    parser.add_argument("--max-pause-s", type=float, default=MAX_PAUSE_S)
    parser.add_argument("--grace-s", type=float, default=KILL_GRACE_S)
    parser.add_argument("--probe-s", type=float, default=PROBE_S)
    parser.add_argument("--sampler-stale-s", type=float,
                        default=SAMPLER_STALE_S,
                        help="log `sampler-stale` after this long without a "
                             "sample")
    parser.add_argument("--sampler-pause-s", type=float,
                        default=SAMPLER_PAUSE_S,
                        help="pause the job after this long without a sample; "
                             "it resumes if sampling recovers and is killed by "
                             "--max-pause-s if it does not")
    parser.add_argument("--reserve-gb", type=float, default=None,
                        help="declared fallback reserve (GB): admission takes "
                             "max(measured peak, this).  With no measurement "
                             "the job is also HELD to 1.5 x it, like a probe.")
    parser.add_argument("--estimate-gb", type=float, default=None,
                        help="probe reserve (pre-flight, cap, kill threshold); "
                             "default: the case's measured peak, else the "
                             "budget.  On a run it is a synonym of "
                             "--reserve-gb.")
    parser.add_argument("--reserve-detector", choices=RESERVE_DETECTORS,
                        default=None,
                        help="opt-in project reserve detection: smc-ladder "
                             "= a JSON config argument with a `ladder` list "
                             "declares its heaviest rung's reserve")
    parser.add_argument("--baseline", default=None,
                        help="the project's measured table (required)")
    parser.add_argument("--runlog", default=None,
                        help="the project's run log (required)")
    parser.add_argument("--runlog-header-file", default=None,
                        help="text written at the top of a NEW run log "
                             "(default: a generic header)")


def cmd_status(args) -> int:
    profile = None
    if args.profile_file:
        try:
            profile = load_profile(args.profile_file)
        except ProfileError as error:
            log(f"bad profile: {error}")
            return EXIT_USAGE
    home = guard_home()
    current = home / "current.json" if home is not None else None
    holder = None
    if current is not None and current.is_file():
        try:
            holder = json.loads(current.read_text())
        except ValueError:
            holder = {"raw": current.read_text()}
        if isinstance(holder, dict):
            # a record left behind by a SIGKILLed guard is not
            # a running job.  The flock is already gone; say so here too.
            alive = pid_alive(holder.get("pid"))
            holder["alive"] = alive
            holder["stale"] = not alive
    memory = host_memory_sample()
    print(json.dumps({
        "lock_holder": holder,
        "host_available_gb": round(memory["available"] / GB, 2),
        "host_memory": {k: round(v / GB, 2) for k, v in memory.items()
                        if isinstance(v, float)},
        "profile": profile["name"] if profile else None,
        "budget_gb": profile["budget_gb"] if profile else None,
        "floor_gb": profile["floor_gb"] if profile else None,
        "container_cpus": profile["container_cpus"] if profile else None,
        "baseline": args.baseline,
        # S3 (additive): a workload a previous guard may have left running
        "inflight": read_inflight(home) if home is not None else None,
        "guard_home": str(home) if home is not None else None,
        # S4 (additive): the profile with the optional keys filled in
        "effective_profile": profile,
    }, indent=1, sort_keys=True))
    return 0


def cmd_baseline(args) -> int:
    baseline_path = Path(args.baseline)
    if getattr(args, "reconcile", False):
        return reconcile_baseline(baseline_path, Path(args.records),
                                  cases=args.case, apply=args.apply)
    baseline = load_baseline(baseline_path)
    rows = baseline["cases"]
    if not rows:
        print("(no cases measured)")
        return 0
    width = max(len(k) for k in rows)
    print(f"{'case':<{width}}  venue      kind      peak GB  wall s  threads")
    for key, row in rows.items():
        print(f"{key:<{width}}  {row.get('venue', '-'):<9}  "
              f"{row.get('kind', '-'):<8}  {row.get('peak_rss_gb', 0):7.3f}  "
              f"{row.get('wall_s', 0):6.0f}  {row.get('threads', '-')}")
    return 0


def reconcile_baseline(baseline_path: Path, records_root: Path,
                       cases=None, apply=False) -> int:
    """Raise every baseline row to the largest peak its own guard records show.

    The guard writes `outputs/guard/<case>/<stamp>/guard.json` for every job
    it ran, including the ones whose baseline row a later, cheaper job under
    the same key overwrote.  This replays those records through the SAME
    never-decrease rule `record_baseline` now applies, so a row is restored
    from the guard's own measurement rather than edited by hand.  Read-only
    unless `--apply`.
    """
    baseline = load_baseline(baseline_path)
    rows = baseline["cases"]
    wanted = set(cases or [])
    if apply and not wanted:
        log("--apply needs at least one --case: a record whose COMMAND has "
            "since changed (a peak that belongs to a removed program) must "
            "not be reinstated wholesale")
        return EXIT_USAGE
    changes = []
    for case_dir in sorted(p for p in records_root.glob("*") if p.is_dir()):
        case = case_dir.name
        if wanted and case not in wanted:
            continue
        row = rows.get(case)
        if row is None:
            continue
        best = 0.0
        best_job = None
        for record_path in sorted(case_dir.glob("*/guard.json")):
            try:
                record = json.loads(record_path.read_text())
            except ValueError:
                continue
            if not isinstance(record, dict) or \
                    record.get("schema", GUARD_SCHEMA) not in \
                    GUARD_RECORD_SCHEMAS:
                continue            # not a guard record (either schema)
            if record.get("workers", 1) != row.get("workers", 1):
                continue
            peak = float(record.get("peak_rss_gb", 0.0))
            if peak > best:
                best, best_job = peak, record.get("job_id", record_path.name)
        current = float(row.get("peak_rss_gb", 0.0))
        if best > current:
            changes.append((case, current, best, best_job))
            if apply:
                row["last_peak_rss_gb"] = current
                row["peak_rss_gb"] = best
                row["peak_source_job_id"] = best_job
    for case, current, best, job in changes:
        print(f"{case}: {current:.3f} -> {best:.3f} GB (from {job})")
    if not changes:
        print("(every baseline row already holds its largest recorded peak)")
    elif apply:
        save_baseline(baseline_path, baseline)
        print(f"wrote {baseline_path}")
    else:
        print("(dry run; pass --apply to write)")
    return 0


def main(argv: list[str] | None = None) -> int:
    # R2 (S2 review): no abbreviated options, so `--profile mac` (an
    # external guard's option) is refused instead of being read as
    # `--profile-file mac`
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     allow_abbrev=False)
    sub = parser.add_subparsers(dest="mode", required=True)
    run = sub.add_parser("run", help="run one guarded job",
                         allow_abbrev=False)
    add_run_arguments(run)
    run.add_argument("--probe", action="store_true",
                     help="60 s probe that records the case's peak")
    probe = sub.add_parser("probe", help="run a 60 s probe of a case",
                           allow_abbrev=False)
    add_run_arguments(probe)
    status = sub.add_parser("status", help="lock holder and host memory",
                            allow_abbrev=False)
    status.add_argument("--profile-file", default=None,
                        help="machine profile JSON to report (optional)")
    status.add_argument("--baseline", default=None,
                        help="the project's measured table to report")
    baseline = sub.add_parser("baseline", help="print the measured table",
                              allow_abbrev=False)
    baseline.add_argument("--baseline", required=True,
                          help="the project's measured table")
    baseline.add_argument("--reconcile", action="store_true",
                          help="raise each row to the largest peak its own "
                               "outputs/guard records show")
    baseline.add_argument("--records", default=None,
                          help="guard record root for --reconcile (required "
                               "with it)")
    baseline.add_argument("--case", action="append", default=[],
                          help="restrict --reconcile to these case keys")
    baseline.add_argument("--apply", action="store_true",
                          help="write the reconciled rows (default: dry run)")
    argv = list(sys.argv[1:] if argv is None else argv)
    # Split at the first "--" ourselves: argparse.REMAINDER swallows trailing
    # options such as --wait on Python 3.10 (the container interpreter).
    command: list[str] = []
    if "--" in argv:
        cut = argv.index("--")
        argv, command = argv[:cut], argv[cut + 1:]
    args = parser.parse_args(argv)
    if args.mode == "status":
        return cmd_status(args)
    if args.mode == "baseline":
        if args.reconcile and not args.records:
            parser.error("baseline --reconcile needs --records DIR")
        return cmd_baseline(args)
    if args.mode == "probe":
        args.probe = True
    if not command:
        parser.error("no command given after --")
    if args.venue == "container" and not args.container:
        parser.error("--venue container needs --container NAME")
    profile = None
    profile_error = None
    if args.profile_file:
        try:
            profile = load_profile(args.profile_file)
        except ProfileError as error:
            profile_error = error
    if args.dense_threads is not None:
        # the profile is resolved here only for --dense-threads (a bad
        # profile exits before the pass-through); otherwise after it
        if profile_error is not None:
            log(f"bad profile: {profile_error}")
            return EXIT_USAGE
        # without a profile (only possible on the nested pass-through, which
        # exports nothing) there is no CPU quota to check against
        cpus = profile["container_cpus"] if profile else None
        if args.dense_threads < 1:
            parser.error("--dense-threads must be >= 1")
        if cpus is not None and args.dense_threads > cpus:
            # the container's CPU quota is the profile's CPU count
            parser.error(f"--dense-threads must be <= {cpus} "
                         "(the CPU quota)")
    nested = next((os.environ[name] for name in KNOWN_MARKERS
                   if os.environ.get(name)), None)
    if nested:
        # nested under an already-guarded job: pass straight through, but
        # only inside the enclosing claimed ACS job (G2a)
        if args.venue == "container":
            log("a container job cannot be launched from inside a guarded "
                "job; run it from the host guard")
            return EXIT_USAGE
        problem = nested_claim_problem(nested)
        if problem is not None:
            log(f"refused: nested under job {nested}, but {problem}")
            return EXIT_USAGE
        log(f"nested under job {nested}; pass-through")
        env = dict(os.environ)
        try:
            os.execvpe(command[0], command, env)
        except OSError as error:
            log(f"exec failed: {error}")
            return EXIT_VENUE
    claim = acs_claim_problem(args)
    if claim is not None:
        log(f"refused: {claim}")
        return EXIT_USAGE
    problem = guard_home_problem()
    if problem is not None:
        log(f"refused: {problem}")
        return EXIT_USAGE
    seen: list[str] = []
    for alias in args.marker_alias:
        if not _ALIAS.fullmatch(alias) or alias in RESERVED_NAMES or \
                alias.startswith("ACS_GUARD_") or alias in seen:
            log(f"refused: --marker-alias {alias!r} is not an upper-case "
                "variable name, or is reserved, or is given twice")
            return EXIT_USAGE
        seen.append(alias)
    if profile_error is not None:
        log(f"bad profile: {profile_error}")
        return EXIT_USAGE
    # no defaults from a checkout: the caller supplies them
    missing = []
    if profile is None:
        missing.append("--profile-file")
    if not args.runlog:
        missing.append("--runlog")
    if not args.baseline:
        missing.append("--baseline")
    if not args.cwd and not args.project_root:
        missing.append("--cwd or --project-root")
    if bool(args.outputs) == bool(args.outputs_root):
        missing.append("exactly one of --outputs, --outputs-root")
    if args.venue == "container" and not args.container_workdir:
        missing.append("--container-workdir (container venue)")
    if missing:
        parser.error("missing: " + "; ".join(missing))
    try:
        job = Job(args, command, profile)
    except OSError as error:
        log(f"cannot read --runlog-header-file: {error}")
        return EXIT_USAGE
    return job.run()


if __name__ == "__main__":
    sys.exit(main())
