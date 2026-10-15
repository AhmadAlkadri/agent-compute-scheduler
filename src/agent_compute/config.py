"""Machine-local configuration (schema ``agent-compute-config/2``).

The configuration lives outside every project repository
(``~/.config/agent-compute/config.toml``, or ``$AGENT_COMPUTE_CONFIG``).  It
names the execution host, the backend guard and its verified fingerprint,
the guard home and job-marker names, the queue, remote routes and
per-project policy.  Nothing here is imported by project code.

Validation is strict: an unknown key anywhere, or a value of the wrong
type, is a ``ConfigError``, so a typo never falls back to a default.  A v1
config (``agent-compute-config/1``) is refused with the instruction to
migrate it (``agent-compute config migrate``, operator-authorized), which
rewrites it with the same effective values (``migrate_v1_text``,
``v1_effective_values``, ``effective_values``).

The backend (``[backend].kind``) has one value, ``acs-guard``: the
ACS-internal guard.  Its path is NOT configured: it is
``agent_compute/guard/engine.py`` inside the installed version directory
the job was submitted with (the runner runs from that version).
``backend.sha256`` pins that file.  The machine profile is the
``[backend.profile]`` table (required; no defaults here: the engine's
documented defaults for its optional keys are the only ones; ``agent-compute
config init`` prints host-derived values for a fresh config).  A project's
``reserve_detector`` is optional (default ``"none"``; ``"smc-ladder"`` is
the shipped example of a project-specific detector), and a project with
containers must declare ``container_limits`` (the containers whose limits
the guard may ``docker update``; may be empty): no silent default may change
a shared container's limits.  The external ``guard.py`` backend
(``smc-guard``) was retired in 0.2.0 (campaign G decision D1): a config
naming it, or a guard script path, fails to load with that reason.

Hosts and placement (campaign G slice G3a, decisions D5, D6).  ``[hosts.*]``
names the remote routes (``transport``, ``ssh``, ``distro``,
``agent_compute``, the ``connect_timeout_s``/``call_timeout_s`` every
remote call uses, and, G3b, ``reply_grace_s``: how long after a send a
lost reply that the destination does not know may be resolved "not
accepted", at least 900 s); no entry may be named like this host (it would be
shadowed by it) and no two may share one route (two names for one
installation would count it twice).  A project declares where it may run:
``projects.<p>.hosts`` (ordered; default ``[host]``: here only, as
before) and, for a remote, ``projects.<p>.on.<host>`` (``cwd``,
``container``, ``container_workdir`` there).  Admission stays on each
host; an unpinned submit of a project that declares more than one host
is placed by the client-side policy (G3b, ``placement.evaluate``), a
pinned one (``--host``) goes where it is pinned.

Budgets (D4): ``backend.profile.budget_gb`` and
``backend.dedicated_budget_gb`` are capped by this host's RAM, read at every
load (``host_resources``); there is no fixed cap.  Records (D3): a project's
runlog and baseline default to ``{state}/projects/{project}/...``; a path
inside the project's repository tree needs ``records_in_tree = true``.

Names (campaign G decision D2).  The guard home (lock, ``current.json``,
``inflight.json``) is ``backend.guard_home``, default ``<state_dir>/guard``;
the internal guard marks a job's processes with ``ACS_JOB_MARKER`` and also
with each name in ``backend.marker_aliases`` (default none), and exports
``ACS_CASE``/``ACS_WORKERS``/``ACS_DENSE_THREADS`` (plus the legacy names
with ``backend.legacy_workload_env = true``).  A host that shares its lock
with direct callers of an external guard keeps the legacy values by
configuration (``config migrate`` writes them).
"""

from __future__ import annotations

import dataclasses
import errno
import fnmatch
import math
import os
import re
import subprocess
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

CONFIG_SCHEMA = "agent-compute-config/2"
LEGACY_CONFIG_SCHEMA = "agent-compute-config/1"
ENV_CONFIG = "AGENT_COMPUTE_CONFIG"
# where `install.py` installs, when it is not the real home (a throwaway or
# second installation); `config init` derives the state dir from it (K3)
ENV_INSTALL_HOME = "ACS_INSTALL_HOME"
DEFAULT_CONFIG = Path("~/.config/agent-compute/config.toml")
DEFAULT_STATE_DIR = "~/.local/state/agent-compute"
MIGRATE_HINT = ("the operator migrates it, at an idle checkpoint (no job "
                "unfinished), with `agent-compute config migrate "
                "--authorized-by \"<who, when, what>\"`: same effective "
                "values, a backup and a diff")

# Environment names that must never reach a job record or queue command.
SECRET_MARKERS = ("TOKEN", "SECRET", "PASSWORD", "PASSWD", "CREDENTIAL",
                  "API_KEY", "APIKEY", "PRIVATE_KEY", "ACCESS_KEY", "AUTH",
                  "_PAT", "COOKIE", "SESSION_KEY")
# The only guard variables a test-mode config may set (fake readings).
GUARD_TEST_HOOKS = ("SMC_GUARD_FREE_MEM_FILE", "SMC_GUARD_SAMPLER_FAIL_FILE")
# Guard control variables: stale nesting state and test hooks.  Stripped from
# every job environment; only a test-mode config may set the hooks.
GUARD_ENV_PREFIX = "SMC_GUARD_"
ACS_GUARD_PREFIX = "ACS_GUARD_"
# The job marker the internal guard exports to every process of a job, and
# the legacy name (an external guard's, and an alias by configuration).
JOB_MARKER = "ACS_JOB_MARKER"
LEGACY_JOB_MARKER = "SMC_GUARD_JOB"
# The ACS job id the runner exports (S5a), and the guard home variable the
# runner sets for the internal guard (the external guard reads the legacy
# one).
JOB_ID_ENV = "ACS_JOB_ID"
GUARD_HOME_ENV = "ACS_GUARD_HOME"
LEGACY_GUARD_HOME_ENV = "SMC_GUARD_HOME"
# Workload variables the internal guard exports, and their legacy names
# (exported beside them with backend.legacy_workload_env).
WORKLOAD_ENV = ("ACS_CASE", "ACS_WORKERS", "ACS_DENSE_THREADS")
LEGACY_WORKLOAD_ENV = ("SMC_GUARD_CASE", "SMC_GUARD_WORKERS",
                       "SMC_DENSE_BLAS_THREADS")
# The slice id the guard falls back to when --slice is not given.
SLICE_ENV = "ACS_SLICE"
LEGACY_SLICE_ENV = "SMC_SLICE"
THREAD_VARS = ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS",
               "VECLIB_MAXIMUM_THREADS", "MKL_NUM_THREADS",
               "NUMEXPR_NUM_THREADS")
# The variables of the controlled job environment (cli.BASE_ENV)
BASE_ENV_NAMES = ("PATH", "HOME", "LANG", "LC_ALL", "USER", "LOGNAME",
                  "SHELL", "TMPDIR")
ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
ALIAS_NAME = re.compile(r"[A-Z][A-Z0-9_]{0,63}")
# A host label names files (the block, the event log): no separators, no
# leading dot, no whitespace
HOST_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
GROUP_NAME = re.compile(r"[A-Za-z0-9_-]{1,64}")


class ConfigError(Exception):
    pass


# --- the backend ----------------------------------------------------------------------
BACKEND_ACS = "acs-guard"
BACKEND_KINDS = (BACKEND_ACS,)
# the external guard.py backend, retired in 0.2.0 (D1): named only to refuse
# it with the reason
RETIRED_BACKEND = "smc-guard"
RETIRED_TEXT = (f"backend.kind '{RETIRED_BACKEND}' (an external guard.py run "
                "in place) was retired in 0.2.0 (campaign G decision D1): "
                f"the ACS-internal guard, kind '{BACKEND_ACS}', is the only "
                "backend; write kind = \"acs-guard\" with the machine "
                "profile table [backend.profile] and pin its engine "
                "(operator: `agent-compute pin-backend --evidence <committed "
                "suite output> --authorized-by \"<who, when, what>\"`)")
# The machine profile of the ACS-internal guard (its --profile-file).  Mirror
# of the engine's schema (agent_compute/guard/engine.py parse_profile); the
# admission layer never imports the engine, and a test checks that both
# accept and refuse the same profiles.
PROFILE_SCHEMA = "agent-compute-guard-profile/1"
PROFILE_REQUIRED = ("name", "budget_gb", "floor_gb", "container_cpus",
                    "runlog_tag")
PROFILE_OPTIONAL = ("disk_floor_gb", "light_budget_gb", "vm_reserve_gb",
                    "container_memory_factor")
# Project reserve detectors the engine knows (engine.RESERVE_DETECTORS),
# plus the opt-out, which is the default (D4).  "smc-ladder" is the shipped
# example of a project-specific detector (the Stokes ladder configs).
RESERVE_DETECTORS = ("smc-ladder",)
RESERVE_DETECTOR_NONE = "none"
GB = 1024 ** 3
# Test hooks for the host's RAM and cores, honoured only by code running
# from a checkout (the test suite), never by an installed version
# (``safety.installation``): they only change which operator budgets a
# config may declare, and a runner (scrubbed environment) never sees them.
TEST_MEMORY_ENV = "ACS_TEST_HOST_MEMORY_GB"
TEST_CORES_ENV = "ACS_TEST_HOST_CORES"


def _from_checkout() -> bool:
    from .safety import installation
    return installation(__file__) is None


def host_resources() -> dict:
    """This host's RAM in GB (2**30 bytes; None when it cannot be read) and
    logical cores (D4): the budget cap of every config load, and the basis
    of the defaults ``config init`` prints and ``doctor`` shows."""
    try:
        memory = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") / GB
    except (OSError, ValueError, AttributeError, TypeError):
        memory = None
    if memory is not None and not (math.isfinite(memory) and memory > 0):
        memory = None
    found = {"memory_gb": round(memory, 3) if memory else None,
             "cores": os.cpu_count(), "source": "sysconf"}
    if _from_checkout() and (os.environ.get(TEST_MEMORY_ENV) or
                             os.environ.get(TEST_CORES_ENV)):
        try:
            if os.environ.get(TEST_MEMORY_ENV):
                found["memory_gb"] = float(os.environ[TEST_MEMORY_ENV])
            if os.environ.get(TEST_CORES_ENV):
                found["cores"] = int(os.environ[TEST_CORES_ENV])
        except ValueError:
            raise ConfigError(f"{TEST_MEMORY_ENV}/{TEST_CORES_ENV}: not a "
                              "number") from None
        found["source"] = "test hook (checkout only)"
    return found


def derived_defaults(memory_gb: float, cores: int | None) -> dict:
    """The profile defaults of a fresh config (D4): ``floor_gb = max(2,
    0.125 x RAM)`` (rounded up to 0.1 GB), ``budget_gb = 0.25 x RAM``
    (rounded down to 0.1 GB), ``container_cpus = cores - 2`` (at least 1).
    Printed by ``config init`` and ``doctor``; never applied to a config
    that states its own numbers."""
    floor = max(2.0, math.ceil(round(0.125 * memory_gb * 10, 6)) / 10)
    budget = max(0.1, math.floor(round(0.25 * memory_gb * 10, 6)) / 10)
    return {"floor_gb": floor, "budget_gb": budget,
            "container_cpus": max(1, (cores or 1) - 2)}


def _positive(key: str, value) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or \
            not math.isfinite(value) or value <= 0:
        raise ConfigError(f"backend.profile.{key} must be a positive number, "
                          f"got {value!r}")
    return float(value)


def backend_profile(value, cap_gb: float) -> dict:
    """Validate ``[backend.profile]``; return it as the engine's profile
    JSON (``agent-compute-guard-profile/1``) with ONLY the keys the operator
    wrote.  Unknown keys are refused (a typo never falls back to a
    default).  ``budget_gb`` is capped like a dedicated budget, by
    ``cap_gb`` (this host's RAM; 48 GB for the v1 format)."""
    if not isinstance(value, dict):
        raise ConfigError("[backend.profile] must be a table (name, "
                          "budget_gb, floor_gb, container_cpus, runlog_tag)")
    known = {*PROFILE_REQUIRED, *PROFILE_OPTIONAL}
    unknown = sorted(set(value) - known)
    if unknown:
        raise ConfigError(f"backend.profile: unknown keys {unknown}")
    missing = [k for k in PROFILE_REQUIRED if k not in value]
    if missing:
        raise ConfigError(f"backend.profile: missing keys {missing} (no "
                          "defaults: write the machine's values)")
    name = value["name"]
    if not isinstance(name, str) or not name.strip():
        raise ConfigError("backend.profile.name must be a non-empty string")
    cpus = value["container_cpus"]
    if isinstance(cpus, bool) or not isinstance(cpus, int) or cpus < 1:
        raise ConfigError("backend.profile.container_cpus must be an integer "
                          f">= 1, got {cpus!r}")
    tag = value["runlog_tag"]
    # TOML has no null: the operator writes runlog_tag = "" for none
    if tag is not None and not isinstance(tag, str):
        raise ConfigError("backend.profile.runlog_tag must be a string "
                          "(\"\" for none)")
    tag = tag.strip() if isinstance(tag, str) else None
    profile = {"schema": PROFILE_SCHEMA, "name": name,
               "budget_gb": _positive("budget_gb", value["budget_gb"]),
               "floor_gb": _positive("floor_gb", value["floor_gb"]),
               "container_cpus": cpus, "runlog_tag": tag or None}
    if profile["budget_gb"] > cap_gb:
        raise ConfigError(f"backend.profile.budget_gb must be <= this "
                          f"host's RAM ({cap_gb:g} GB), got "
                          f"{profile['budget_gb']:g}")
    for key in PROFILE_OPTIONAL:
        if key in value:
            profile[key] = _positive(key, value[key])
    return profile


def expand(path: str | os.PathLike) -> Path:
    return Path(os.path.expandvars(os.path.expanduser(str(path))))


def is_secret_name(name: str) -> bool:
    upper = name.upper()
    return any(marker in upper for marker in SECRET_MARKERS)


# Operator declaration that the host is dedicated compute (machine config
# `backend.dedicated_budget_gb`): the guard's host budget is raised from its
# profile default to the declared value, for every project, never above the
# host's RAM (D4; before 0.2.0 a fixed 48 GB, which the v1 format keeps).
# The guard's own host-available-memory floor still applies: the declared
# budget is a per-job ceiling, never an admission test.
V1_BUDGET_MAX_GB = 48.0


def dedicated_budget(value, cap_gb: float) -> float | None:
    """Validate `backend.dedicated_budget_gb`; None when not declared.

    Fails closed: anything but a finite number in (0, ``cap_gb``] (this
    host's RAM) is a ConfigError, so no command runs on it: submit and
    doctor refuse, and a runner started on it exits before claiming the job
    (it never starts; once the config is fixed the job settles as
    launch-failed/refused, never started).
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError("backend.dedicated_budget_gb must be a number of GB, "
                          f"got {value!r}")
    value = float(value)
    if not (math.isfinite(value) and 0 < value <= cap_gb):
        raise ConfigError("backend.dedicated_budget_gb must be in (0, "
                          f"{cap_gb:g}] GB (this host's RAM), got {value!r}")
    return value


@dataclass
class Project:
    name: str
    roots: list[str]
    case_prefix: str = ""
    runlog: str = "{state}/projects/{project}/runlog-{host}.md"
    baseline: str = "{state}/projects/{project}/baseline-{host}.json"
    inputs: list[str] = field(default_factory=lambda: ["."])
    env_passthrough: list[str] = field(default_factory=list)
    max_timeout_min: float = 60.0
    max_threads: int = 1
    container_workdir: str | None = None
    container_provenance: str | None = None
    containers: list[str] = field(default_factory=list)
    # the engine's --reserve-detector ("none", the default since 0.2.0: no
    # detection; "smc-ladder": the shipped example detector) and the
    # containers it may `docker update` (--container-limits; required for a
    # project with containers, see load)
    reserve_detector: str | None = RESERVE_DETECTOR_NONE
    container_limits: list[str] | None = None
    # D3: the explicit opt-in to a runlog/baseline inside the project's
    # repository tree (a relative path, {worktree}, or a path under a root)
    records_in_tree: bool = False
    # G3a (D5): the hosts this project may run on, in order (None in the
    # dataclass; ``load`` sets the default [host]), and per remote host its
    # mapping there: {"cwd", "container", "container_workdir"}
    hosts: list[str] | None = None
    on: dict = field(default_factory=dict)

    @property
    def detector(self) -> str | None:
        """The --reserve-detector to pass (None: none)."""
        return None if self.reserve_detector in (None, RESERVE_DETECTOR_NONE) \
            else self.reserve_detector

    def matches(self, worktree: Path) -> bool:
        text = os.path.realpath(worktree)
        return any(fnmatch.fnmatch(text, os.path.realpath(expand(root)))
                   for root in self.roots)


PROJECT_KEYS = tuple(f.name for f in dataclasses.fields(Project)
                     if f.name != "name")
# G3a: where a project may run (not in the v1 format; effective values
# only when they differ from the default "here only")
PLACEMENT_PROJECT_KEYS = ("hosts", "on")
ON_KEYS = ("cwd", "container", "container_workdir")
# the project keys of the v1 format (records_in_tree is new in 0.2.0)
V1_PROJECT_KEYS = tuple(k for k in PROJECT_KEYS
                        if k not in ("records_in_tree",
                                     *PLACEMENT_PROJECT_KEYS))
# the keys every effective-values table carries for a project
EFFECTIVE_PROJECT_KEYS = tuple(k for k in PROJECT_KEYS
                               if k not in PLACEMENT_PROJECT_KEYS)
RECORD_KEYS = ("runlog", "baseline")
# what {worktree} stands for while a template is classified at load time
_WORKTREE_SENTINEL = "/__acs_worktree__"


@dataclass
class RemoteHost:
    name: str
    transport: str            # "ssh-wsl" or "ssh"
    ssh: str
    agent_compute: str
    distro: str | None = None
    # campaign G (G2a schema; used by every remote call since G3a,
    # remote.py): seconds; None: the defaults of remote.py
    connect_timeout_s: float | None = None
    call_timeout_s: float | None = None
    # G3b (D6): seconds after a send before a lost reply that the
    # destination does not know may be resolved "not accepted" (None:
    # REPLY_GRACE_MIN_S; never less)
    reply_grace_s: float | None = None

    @property
    def route(self) -> tuple:
        """What reaches the installation: two entries with one route are
        one machine's one ACS under two names."""
        return (self.transport, self.ssh, self.distro)

    def view(self) -> dict:
        return {"transport": self.transport, "ssh": self.ssh,
                "distro": self.distro, "agent_compute": self.agent_compute}


TRANSPORTS = ("ssh-wsl", "ssh")
TOP_KEYS = ("schema", "host", "state_dir", "python", "job_path", "test_mode",
            "backend", "queue", "hosts", "projects")
BACKEND_KEYS = ("kind", "sha256", "profile", "dedicated_budget_gb",
                "guard_home", "marker_aliases", "legacy_workload_env",
                "extra_env", "lock_wait_max_s", "admission_retry_s",
                "admission_retry_max_s")
QUEUE_KEYS = ("enabled", "pueue", "pueued", "group")
HOST_KEYS = ("transport", "ssh", "agent_compute", "distro",
             "connect_timeout_s", "call_timeout_s", "reply_grace_s")
# G3b: the least reply grace (the destination's own unclaimed grace, the
# slowest legitimate submit: cli.UNCLAIMED_GRACE_S)
REPLY_GRACE_MIN_S = 900.0


@dataclass
class Config:
    path: Path
    host: str
    state_dir: Path
    python: str
    backend_kind: str                # acs-guard (the only kind)
    guard_sha256: str                # the pinned sha256 of the engine
    profile: dict                    # [backend.profile], engine JSON form
    dedicated_budget_gb: float | None  # None: the guard profile's budget
    guard_home: Path | None          # None: the default <state_dir>/guard
    guard_extra_env: dict[str, str]
    lock_wait_max_s: float
    admission_retry_s: float
    admission_retry_max_s: float
    queue_enabled: bool
    pueue: str | None
    pueued: str | None
    queue_group: str
    job_path: str | None
    test_mode: bool
    projects: dict[str, Project]
    remotes: dict[str, RemoteHost]
    raw: dict
    marker_aliases: list[str] = field(default_factory=list)
    legacy_workload_env: bool = False

    # -- derived locations ------------------------------------------------
    @property
    def jobs_dir(self) -> Path:
        return self.state_dir / "jobs"

    @property
    def blocks_dir(self) -> Path:
        return self.state_dir / "blocks"

    @property
    def requests_dir(self) -> Path:
        return self.state_dir / "requests"

    @property
    def pueue_dir(self) -> Path:
        return self.state_dir / "pueue"

    @property
    def effective_guard_home(self) -> Path:
        """The guard home every guard of this host uses: the configured
        one, else ``<state_dir>/guard``.  ACS passes it to the guard
        explicitly; it never relies on a guard's own default."""
        return self.guard_home or self.state_dir / "guard"

    @property
    def markers(self) -> list[str]:
        """The job-marker names a job of this config carries: the internal
        guard's primary marker and the configured aliases."""
        return [JOB_MARKER, *self.marker_aliases]

    @property
    def workload_env(self) -> list[str]:
        """The workload variables (beside the thread pins) the guard
        exports."""
        names = list(WORKLOAD_ENV)
        if self.legacy_workload_env:
            names += LEGACY_WORKLOAD_ENV
        return sorted(names)

    @property
    def slice_env(self) -> list[str]:
        """The variables the guard reads the slice id from."""
        return [SLICE_ENV] + ([LEGACY_SLICE_ENV]
                              if self.legacy_workload_env else [])

    @property
    def effective_budget_gb(self) -> float:
        """The per-job host budget (a ceiling, never an admission test) the
        guard applies: the dedicated declaration, else the profile's."""
        if self.dedicated_budget_gb is not None:
            return self.dedicated_budget_gb
        return self.profile["budget_gb"]

    def fill(self, template: str, project: str, worktree: Path) -> Path:
        text = template.format(state=self.state_dir, project=project,
                               host=self.host, worktree=worktree)
        path = expand(text)
        return path if path.is_absolute() else worktree / path

    def resolve_project(self, worktree: Path,
                        name: str | None = None) -> Project:
        if name:
            project = self.projects.get(name)
            if project is None:
                raise ConfigError(f"unknown project '{name}' (configured: "
                                  f"{', '.join(sorted(self.projects))})")
            if not project.matches(worktree):
                raise ConfigError(f"{worktree} is not a configured root of "
                                  f"project '{name}'")
            return project
        found = [p for p in self.projects.values() if p.matches(worktree)]
        if len(found) != 1:
            raise ConfigError(
                f"{worktree} matches {len(found)} configured projects; "
                "pass --project or fix the machine config")
        return found[0]


def worktree_of(path: Path) -> Path:
    """The git worktree containing ``path`` (resolved), else the path."""
    try:
        out = subprocess.run(["git", "-C", str(path), "rev-parse",
                              "--show-toplevel"], capture_output=True,
                             text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return path.resolve()
    if out.returncode == 0 and out.stdout.strip():
        return Path(out.stdout.strip()).resolve()
    return path.resolve()


# --- environment names ------------------------------------------------------------

def control_variable(name: str, aliases=()) -> bool:
    """A variable that controls a guard or names a job (never set by an
    agent or a config): the guard prefixes, the ACS job id and marker, the
    neutral workload variables and the configured marker aliases."""
    return name.startswith((GUARD_ENV_PREFIX, ACS_GUARD_PREFIX)) or \
        name in (JOB_ID_ENV, JOB_MARKER, *WORKLOAD_ENV) or name in aliases


def env_problem(name, aliases=()) -> str | None:
    """Why ``name`` may not be passed to a job (``--env``,
    ``env_passthrough``, ``backend.extra_env``); None when it may."""
    if not isinstance(name, str) or not ENV_NAME.fullmatch(name):
        return f"{name!r} is not an environment variable name"
    if is_secret_name(name):
        return f"{name}: credentials never enter job records"
    if control_variable(name, aliases):
        return f"{name}: guard control variables are never passed to a job"
    return None


def alias_problem(name, seen=()) -> str | None:
    """Why ``name`` cannot be a job-marker alias (None: it can)."""
    if not isinstance(name, str) or not ALIAS_NAME.fullmatch(name):
        return f"{name!r} is not an upper-case variable name"
    if name in (JOB_MARKER, JOB_ID_ENV, *WORKLOAD_ENV, SLICE_ENV,
                *THREAD_VARS, *BASE_ENV_NAMES) or \
            name.startswith(ACS_GUARD_PREFIX) or name in seen:
        return f"{name} is reserved or listed twice"
    if is_secret_name(name):
        return f"{name} looks like a credential name"
    return None


# --- typed readers (strict) -----------------------------------------------------------

def _table(value, where: str) -> dict:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"{where} must be a table")
    return value


def _unknown(table: dict, known, where: str) -> None:
    extra = sorted(set(table) - set(known))
    if extra:
        raise ConfigError(f"{where}: unknown keys {extra} (a typo never "
                          "falls back to a default)")


def _string(table: dict, key: str, where: str, default=None,
            required: bool = False):
    if key not in table:
        if required:
            raise ConfigError(f"{where}.{key} is required")
        return default
    value = table[key]
    if not isinstance(value, str):
        raise ConfigError(f"{where}.{key} must be a string, got {value!r}")
    return value


def _flag(table: dict, key: str, where: str, default: bool) -> bool:
    value = table.get(key, default)
    if not isinstance(value, bool):
        raise ConfigError(f"{where}.{key} must be true or false, got "
                          f"{value!r}")
    return value


def _seconds(table: dict, key: str, where: str, default=None):
    if key not in table:
        return default
    value = table[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)) or \
            not math.isfinite(value) or value <= 0:
        raise ConfigError(f"{where}.{key} must be a positive number, got "
                          f"{value!r}")
    return float(value)


def _strings(table: dict, key: str, where: str, default=None):
    if key not in table:
        return default
    value = table[key]
    if not isinstance(value, list) or \
            not all(isinstance(v, str) for v in value):
        raise ConfigError(f"{where}.{key} must be a list of strings")
    return list(value)


def host_problem(host) -> str | None:
    if not isinstance(host, str) or not HOST_NAME.fullmatch(host):
        return (f"host must be a label of letters, digits, '.', '_' or '-' "
                f"(at most 64, not starting with '.' or '-'), got {host!r}")
    return None


def _project(name: str, body, aliases, state_dir: Path, host: str,
             remotes: dict | None = None) -> Project:
    where = f"projects.{name}"
    body = _table(body, where)
    _unknown(body, PROJECT_KEYS, where)
    roots = _strings(body, "roots", where)
    if not roots:
        raise ConfigError(f"{where}.roots is required (a non-empty list)")
    values = {"roots": roots}
    for key in ("case_prefix", "runlog", "baseline", "container_workdir",
                "container_provenance", "reserve_detector"):
        if key in body:
            values[key] = _string(body, key, where)
    for key in ("inputs", "env_passthrough", "containers",
                "container_limits"):
        if key in body:
            values[key] = _strings(body, key, where)
    if "max_timeout_min" in body:
        values["max_timeout_min"] = _seconds(body, "max_timeout_min", where)
    if "max_threads" in body:
        threads = body["max_threads"]
        if isinstance(threads, bool) or not isinstance(threads, int) or \
                threads < 1:
            raise ConfigError(f"{where}.max_threads must be an integer >= 1,"
                              f" got {threads!r}")
        values["max_threads"] = threads
    if "records_in_tree" in body:
        values["records_in_tree"] = _flag(body, "records_in_tree", where,
                                          False)
    values["hosts"], values["on"] = _placement(body, where, host,
                                               remotes or {})
    project = Project(name=name, **values)
    _check_project_keys(project)
    _check_records(project, state_dir, host)
    for var in project.env_passthrough:
        problem = env_problem(var, aliases)
        if problem:
            raise ConfigError(f"{where}.env_passthrough may not name {var} "
                              f"({problem})")
    return project


def _remote(name: str, body) -> RemoteHost:
    where = f"hosts.{name}"
    problem = host_problem(name)
    if problem:
        raise ConfigError(f"[{where}]: {problem}")
    body = _table(body, where)
    _unknown(body, HOST_KEYS, where)
    transport = _string(body, "transport", where, required=True)
    if transport not in TRANSPORTS:
        raise ConfigError(f"{where}.transport must be one of "
                          f"{list(TRANSPORTS)}, got {transport!r}")
    distro = _string(body, "distro", where)
    if transport == "ssh-wsl" and not distro:
        raise ConfigError(f"{where}.distro is required with transport "
                          "'ssh-wsl'")
    return RemoteHost(
        name=name, transport=transport,
        ssh=_string(body, "ssh", where, required=True),
        agent_compute=_string(body, "agent_compute", where, required=True),
        distro=distro,
        connect_timeout_s=_seconds(body, "connect_timeout_s", where),
        call_timeout_s=_seconds(body, "call_timeout_s", where),
        reply_grace_s=_reply_grace(body, where))


def _reply_grace(body: dict, where: str) -> float | None:
    value = _seconds(body, "reply_grace_s", where)
    if value is not None and value < REPLY_GRACE_MIN_S:
        raise ConfigError(f"{where}.reply_grace_s must be at least "
                          f"{REPLY_GRACE_MIN_S:g} s (a lost reply is "
                          "resolved \"not accepted\" only after the "
                          f"destination's own grace), got {value:g}")
    return value


def _placement(body: dict, where: str, host: str,
               remotes: dict) -> tuple[list[str], dict]:
    """G3a: ``hosts`` (ordered, each this host or a ``[hosts]`` entry, no
    repeats; default ``[host]``) and ``on.<host>`` (a remote's mapping:
    ``cwd`` absolute, ``container``, ``container_workdir``)."""
    known = [host, *remotes]
    if "hosts" in body:
        hosts = body["hosts"]
        if not isinstance(hosts, list) or \
                not all(isinstance(h, str) for h in hosts):
            raise ConfigError(f"{where}.hosts must be a list of host names "
                              f"(this host {host!r} or [hosts] entries)")
        if not hosts:
            raise ConfigError(f"{where}.hosts must be a non-empty list "
                              "(omit it for this host only)")
        for name in hosts:
            if name not in known:
                raise ConfigError(f"{where}.hosts names {name!r}, which is "
                                  f"neither this host ({host!r}) nor a "
                                  f"[hosts] entry ({sorted(remotes)})")
            if hosts.count(name) > 1:
                raise ConfigError(f"{where}.hosts lists {name!r} twice")
        hosts = list(hosts)
    else:
        hosts = [host]
    on = _table(body.get("on"), f"{where}.on")
    mapping = {}
    for name, table in on.items():
        here = f"{where}.on.{name}"
        if name == host:
            raise ConfigError(f"{here}: {name!r} is this host; its project "
                              "settings are the project itself (on.<host> "
                              "maps a remote host)")
        if name not in hosts:
            raise ConfigError(f"{here}: {name!r} is not one of the "
                              f"project's hosts {hosts}")
        table = _table(table, here)
        _unknown(table, ON_KEYS, here)
        entry = {key: _string(table, key, here) for key in ON_KEYS}
        for key in ("cwd", "container_workdir"):
            if entry[key] is not None and not entry[key].startswith("/"):
                raise ConfigError(f"{here}.{key} must be an absolute path "
                                  f"on {name}, got {entry[key]!r}")
        mapping[name] = entry
    return hosts, mapping


def _check_routes(host: str, remotes: dict) -> None:
    """G3a: no [hosts] entry named like this host (``--host`` would run it
    here, silently), and no two entries on one route (one installation
    under two names would be counted twice)."""
    if host in remotes:
        raise ConfigError(f"[hosts.{host}] is named like this host "
                          f"({host!r}): `--host {host}` runs here, so the "
                          "entry would be shadowed; remove it or rename it")
    seen: dict = {}
    for name, entry in remotes.items():
        other = seen.get(entry.route)
        if other is not None:
            raise ConfigError(f"[hosts.{other}] and [hosts.{name}] use the "
                              f"same route {list(entry.route)}: one "
                              "installation under two names; keep one")
        seen[entry.route] = name


def _guard_home(value, state_dir: Path) -> Path:
    text = value.replace("{state}", str(state_dir))
    home = expand(text)
    if not home.is_absolute():
        raise ConfigError(f"backend.guard_home must be an absolute path (or "
                          f"start with ~ or {{state}}), got {value!r}")
    return home


def _extra_env(backend: dict, aliases, test_mode: bool) -> dict[str, str]:
    table = _table(backend.get("extra_env"), "backend.extra_env")
    env = {}
    for name, value in table.items():
        if isinstance(value, bool) or \
                not isinstance(value, (str, int, float)):
            raise ConfigError(f"backend.extra_env.{name} must be a string "
                              "or a number")
        if test_mode and name in GUARD_TEST_HOOKS:
            env[name] = str(value)
            continue
        problem = env_problem(name, aliases)
        if problem:
            hint = (" (test hooks only in a test_mode config)"
                    if name in GUARD_TEST_HOOKS else "")
            raise ConfigError(f"backend.extra_env may not set {name}: "
                              f"{problem}{hint}")
        env[name] = str(value)
    return env


def load(path: str | os.PathLike | None = None) -> Config:
    # absolute: runners start in `/` (queue task, detached runner), so a
    # relative --config must be resolved against the submitter's cwd here
    path = expand(path or os.environ.get(ENV_CONFIG) or DEFAULT_CONFIG
                  ).absolute()
    if not path.is_file():
        raise ConfigError(f"no agent-compute config at {path}")
    try:
        raw = tomllib.loads(path.read_text())
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(f"{path}: not valid TOML: {error}") from None
    return from_raw(raw, path)


def from_raw(raw: dict, path: Path) -> Config:
    schema = raw.get("schema")
    if schema == LEGACY_CONFIG_SCHEMA:
        raise ConfigError(f"{path}: schema '{LEGACY_CONFIG_SCHEMA}' is the "
                          f"format before 0.2.0; {MIGRATE_HINT}")
    if schema != CONFIG_SCHEMA:
        raise ConfigError(f"{path}: schema must be '{CONFIG_SCHEMA}', got "
                          f"{schema!r}")
    _unknown(raw, TOP_KEYS, "top level")
    host = raw.get("host")
    problem = host_problem(host)
    if problem:
        raise ConfigError(f"host is required: {problem}")
    test_mode = _flag(raw, "test_mode", "top level", False)
    state_dir = expand(_string(raw, "state_dir", "top level",
                               DEFAULT_STATE_DIR))
    backend = _table(raw.get("backend"), "[backend]")
    queue = _table(raw.get("queue"), "[queue]")
    kind = backend.get("kind")
    # D1: the retired external backend, named with the reason (before the
    # unknown-key check, which would only say "unknown key guard")
    if kind == RETIRED_BACKEND:
        raise ConfigError(RETIRED_TEXT)
    if "guard" in backend:
        raise ConfigError("backend.guard names an external guard script, "
                          f"which only the retired '{RETIRED_BACKEND}' "
                          "backend ran (0.2.0, campaign G decision D1); the "
                          "internal guard is not configured by path (it is "
                          "the engine of the installed version, pinned by "
                          "backend.sha256): remove the line")
    _unknown(backend, BACKEND_KEYS, "[backend]")
    _unknown(queue, QUEUE_KEYS, "[queue]")
    if kind not in BACKEND_KINDS:
        raise ConfigError(f"backend.kind must be {BACKEND_ACS!r}, got "
                          f"{kind!r}")
    raw_profile = backend.get("profile")
    if isinstance(raw_profile, str):
        raise ConfigError("backend.profile must be the machine profile table "
                          "[backend.profile]; a profile name was the form of "
                          f"the retired '{RETIRED_BACKEND}' backend")
    if raw_profile is None:
        raise ConfigError("backend.kind 'acs-guard' needs the machine "
                          "profile table [backend.profile] (name, "
                          "budget_gb, floor_gb, container_cpus, "
                          "runlog_tag; `agent-compute config init` prints "
                          "values derived from this host)")
    resources = host_resources()
    cap = resources.get("memory_gb")
    if not cap:
        raise ConfigError("this host's RAM cannot be read, so the budget cap "
                          "(D4: the host's RAM) cannot be checked")
    profile = backend_profile(raw_profile, cap)
    sha = _string(backend, "sha256", "backend")
    if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{64}", sha):
        raise ConfigError("backend.kind 'acs-guard' needs backend.sha256: "
                          "the pinned sha256 of the internal guard "
                          "engine (operator: `agent-compute pin-backend "
                          "--evidence <file> --authorized-by \"<who, when, "
                          "what>\"`)")
    aliases_raw = backend.get("marker_aliases", [])
    if not isinstance(aliases_raw, list):
        raise ConfigError("backend.marker_aliases must be a list of "
                          "variable names")
    aliases: list[str] = []
    for name in aliases_raw:
        problem = alias_problem(name, aliases)
        if problem:
            raise ConfigError(f"backend.marker_aliases: {problem}")
        aliases.append(name)
    legacy_env = _flag(backend, "legacy_workload_env", "backend", False)
    extra_env = _extra_env(backend, aliases, test_mode)
    home = _string(backend, "guard_home", "backend")
    guard_home = _guard_home(home, state_dir) if home else None
    remotes = {name: _remote(name, body)
               for name, body in _table(raw.get("hosts"), "[hosts]").items()}
    _check_routes(host, remotes)
    projects = {name: _project(name, body, aliases, state_dir, host,
                               remotes)
                for name, body in _table(raw.get("projects"),
                                         "[projects]").items()}
    group = _string(queue, "group", "queue", "acs-heavy")
    if not GROUP_NAME.fullmatch(group):
        raise ConfigError(f"queue.group must be a plain name, got {group!r}")
    return Config(
        path=path,
        host=host,
        state_dir=state_dir,
        python=_string(raw, "python", "top level", "python3") or "python3",
        backend_kind=kind,
        guard_sha256=sha,
        profile=profile,
        dedicated_budget_gb=dedicated_budget(
            backend.get("dedicated_budget_gb"), cap),
        guard_home=guard_home,
        guard_extra_env=extra_env,
        lock_wait_max_s=_seconds(backend, "lock_wait_max_s", "backend",
                                 12 * 3600.0),
        admission_retry_s=_seconds(backend, "admission_retry_s", "backend",
                                   120.0),
        admission_retry_max_s=_seconds(backend, "admission_retry_max_s",
                                       "backend", 900.0),
        queue_enabled=_flag(queue, "enabled", "queue", False),
        pueue=_string(queue, "pueue", "queue"),
        pueued=_string(queue, "pueued", "queue"),
        queue_group=group,
        job_path=_string(raw, "job_path", "top level"),
        test_mode=test_mode,
        projects=projects,
        remotes=remotes,
        raw=raw,
        marker_aliases=aliases,
        legacy_workload_env=legacy_env,
    )


def _check_project_keys(project: Project) -> None:
    """The per-project guard keys: ``reserve_detector`` (optional, default
    "none" since 0.2.0, D4) and ``container_limits`` (R4: every `docker
    update` of a listed container is an explicit choice)."""
    name = project.name
    detector = project.reserve_detector
    if detector not in (*RESERVE_DETECTORS, RESERVE_DETECTOR_NONE):
        raise ConfigError(f"projects.{name}.reserve_detector must be one of "
                          f"{[*RESERVE_DETECTORS, RESERVE_DETECTOR_NONE]}, "
                          f"got {detector!r}")
    limits = project.container_limits
    if limits is not None:
        if not isinstance(limits, list) or \
                not all(isinstance(c, str) for c in limits):
            raise ConfigError(f"projects.{name}.container_limits must be a "
                              "list of container names")
        extra = [c for c in limits if c not in project.containers]
        if extra:
            raise ConfigError(f"projects.{name}.container_limits names "
                              f"{extra}, which are not in its containers")
    if project.containers and limits is None:
        raise ConfigError(
            f"projects.{name}.container_limits is required for a project "
            "with containers: list the containers whose memory/CPU limits "
            "the guard applies with `docker update`, or [] for none")


def record_tree_path(template: str, project: Project, state_dir: Path,
                     host: str) -> tuple[bool, str | None]:
    """Whether a runlog/baseline template writes into the project's
    repository tree (it names ``{worktree}``, is relative, which ``fill``
    resolves against the job's worktree, or lies under one of the project's
    roots), and its path relative to the tree when that is known."""
    try:
        text = template.format(state=state_dir, project=project.name,
                               host=host, worktree=_WORKTREE_SENTINEL)
    except (KeyError, IndexError, ValueError) as error:
        raise ConfigError(f"projects.{project.name}: the template "
                          f"{template!r} has an unknown placeholder "
                          f"({error}); use {{state}}, {{project}}, {{host}} "
                          "or {worktree}") from None
    path = expand(text)
    if "{worktree}" in template or not path.is_absolute():
        rel = os.path.normpath(os.path.relpath(path, _WORKTREE_SENTINEL)
                               if path.is_absolute() else path)
        return True, None if rel.startswith("..") else rel
    real = Path(os.path.realpath(path))
    roots = [os.path.realpath(expand(root)) for root in project.roots]
    for candidate in (real, *real.parents):
        if any(fnmatch.fnmatch(str(candidate), root) for root in roots):
            return True, os.path.relpath(real, candidate) \
                if candidate != real else None
    return False, None


def record_in_tree(template: str, project: Project, state_dir: Path,
                   host: str) -> bool:
    return record_tree_path(template, project, state_dir, host)[0]


def _check_records(project: Project, state_dir: Path, host: str) -> None:
    """D3: a runlog or baseline inside the repository tree is an explicit
    per-project opt-in (``records_in_tree = true``)."""
    if project.records_in_tree:
        return
    for key in RECORD_KEYS:
        template = getattr(project, key)
        if record_in_tree(template, project, state_dir, host):
            raise ConfigError(
                f"projects.{project.name}.{key} = {template!r} writes into "
                "the project's repository tree; records default to the state "
                "dir ({state}/projects/{project}/...), and writing into the "
                "tree is an explicit per-project opt-in: set "
                f"projects.{project.name}.records_in_tree = true (and keep "
                "the path out of the project's inputs), or use a {state} path")


# --- effective values and the v1 -> v2 migration -----------------------------------------

def effective_values(config: Config) -> dict:
    """Every value that decides what a job of this host does, flat and
    JSON-able (the side-by-side comparison of ``config migrate`` and
    ``doctor``)."""
    out = {"host": config.host, "state_dir": str(config.state_dir),
           "python": config.python, "job_path": config.job_path,
           "test_mode": config.test_mode,
           "backend.kind": config.backend_kind,
           "backend.sha256": config.guard_sha256,
           "backend.profile": config.profile,
           "backend.dedicated_budget_gb": config.dedicated_budget_gb,
           "backend.guard_home": str(config.effective_guard_home),
           "backend.extra_env": dict(config.guard_extra_env),
           "backend.lock_wait_max_s": config.lock_wait_max_s,
           "backend.admission_retry_s": config.admission_retry_s,
           "backend.admission_retry_max_s": config.admission_retry_max_s,
           "markers": list(config.markers),
           "workload_env": config.workload_env,
           "slice_env": config.slice_env,
           "queue.enabled": config.queue_enabled, "queue.pueue": config.pueue,
           "queue.pueued": config.pueued, "queue.group": config.queue_group}
    for name, remote in config.remotes.items():
        for key in HOST_KEYS:
            out[f"hosts.{name}.{key}"] = getattr(remote, key)
    for name, project in config.projects.items():
        for key in EFFECTIVE_PROJECT_KEYS:
            out[f"projects.{name}.{key}"] = getattr(project, key)
        # G3a: only when the project may run elsewhere (the default, "this
        # host only", is what every v1 config meant)
        if project.hosts != [config.host] or project.on:
            out[f"projects.{name}.hosts"] = list(project.hosts or [])
            out[f"projects.{name}.on"] = dict(project.on)
    return out


V1_RETIRED_TEXT = (f"backend.kind '{RETIRED_BACKEND}': this v1 config runs "
                   "the external guard.py, a backend retired in 0.2.0 "
                   "(campaign G decision D1), so it cannot be migrated with "
                   "the same effective values: switch the host to "
                   f"'{BACKEND_ACS}' under its current version first "
                   "(operator: `agent-compute pin-backend --backend "
                   "acs-guard --evidence ...`), then migrate")


def v1_effective_values(raw: dict) -> dict:
    """The effective values of a v1 config as the versions before 0.2.0
    applied them (the guard home ``~/.smc-guard`` outside test mode, the
    legacy job marker and workload names, the defaults of the time,
    ``records_in_tree`` true where the records were inside the tree).
    Raises ConfigError where those versions refused to load it, and for a
    ``smc-guard`` config (its backend is retired).  ``backend.guard`` (and
    the smc-guard profile name) had no effect under ``acs-guard`` and are
    not effective values."""
    if raw.get("schema") != LEGACY_CONFIG_SCHEMA:
        raise ConfigError(f"schema must be '{LEGACY_CONFIG_SCHEMA}'")
    backend = raw.get("backend") or {}
    queue = raw.get("queue") or {}
    kind = backend.get("kind")
    if kind == RETIRED_BACKEND:
        raise ConfigError(V1_RETIRED_TEXT)
    if kind != BACKEND_ACS:
        raise ConfigError(f"backend.kind must be one of "
                          f"{[RETIRED_BACKEND, BACKEND_ACS]}, got {kind!r}")
    raw_profile = backend.get("profile")
    sha = backend.get("sha256")
    if not isinstance(raw_profile, dict) or not isinstance(sha, str) or \
            not re.fullmatch(r"[0-9a-f]{64}", sha):
        raise ConfigError("backend.kind 'acs-guard' needs "
                          "[backend.profile] and backend.sha256")
    profile = backend_profile(raw_profile, V1_BUDGET_MAX_GB)
    test_mode = bool(raw.get("test_mode", False))
    extra_env = {str(k): str(v) for k, v in
                 (backend.get("extra_env") or {}).items()}
    home = backend.get("guard_home")
    bad = [k for k in extra_env if k.startswith(GUARD_ENV_PREFIX) and
           (k not in GUARD_TEST_HOOKS or not test_mode)]
    if bad or (home and not test_mode):
        raise ConfigError("backend.extra_env/guard_home: only test hooks, "
                          "and only in a test_mode config")
    state_dir = expand(raw.get("state_dir", DEFAULT_STATE_DIR))
    host = str(raw.get("host") or "")
    out = {"host": host,
           "state_dir": str(state_dir),
           "python": str(raw.get("python") or "python3"),
           "job_path": raw.get("job_path"), "test_mode": test_mode,
           "backend.kind": kind,
           "backend.sha256": backend.get("sha256"),
           "backend.profile": profile,
           "backend.dedicated_budget_gb":
               dedicated_budget(backend.get("dedicated_budget_gb"),
                                V1_BUDGET_MAX_GB),
           "backend.guard_home": str(expand(home)) if home
           else str(Path.home() / ".smc-guard"),
           "backend.extra_env": extra_env,
           "backend.lock_wait_max_s":
               float(backend.get("lock_wait_max_s", 12 * 3600)),
           "backend.admission_retry_s":
               float(backend.get("admission_retry_s", 120)),
           "backend.admission_retry_max_s":
               float(backend.get("admission_retry_max_s", 900)),
           "markers": [LEGACY_JOB_MARKER],
           "workload_env": sorted(LEGACY_WORKLOAD_ENV),
           "slice_env": [LEGACY_SLICE_ENV],
           "queue.enabled": bool(queue.get("enabled", False)),
           "queue.pueue": queue.get("pueue"),
           "queue.pueued": queue.get("pueued"),
           "queue.group": str(queue.get("group", "acs-heavy"))}
    for name, body in (raw.get("hosts") or {}).items():
        unknown = sorted(set(body) - {"transport", "ssh", "agent_compute",
                                      "distro"})
        if unknown:
            raise ConfigError(f"hosts.{name}: unknown keys {unknown}")
        for key in ("transport", "ssh", "agent_compute", "distro"):
            out[f"hosts.{name}.{key}"] = body.get(key)
    for name, body in (raw.get("projects") or {}).items():
        body = dict(body)
        if not body.get("roots"):
            raise ConfigError(f"projects.{name}.roots is required")
        unknown = sorted(set(body) - set(V1_PROJECT_KEYS))
        if unknown:
            # v1's Project(**body) raised on them
            raise ConfigError(f"projects.{name}: unknown keys {unknown}")
        if "reserve_detector" not in body:
            # v1 (acs-guard) required an explicit detector
            raise ConfigError(f"projects.{name}.reserve_detector is "
                              "required with backend.kind 'acs-guard'")
        project = Project(name=name, **body)
        _check_project_keys(project)
        # v1 wrote the records wherever the paths said: an in-tree path
        # was that project's effective choice, now an explicit opt-in
        project.records_in_tree = any(
            record_in_tree(getattr(project, key), project, state_dir, host)
            for key in RECORD_KEYS)
        for var in project.env_passthrough:
            if is_secret_name(var) or var.startswith(GUARD_ENV_PREFIX):
                raise ConfigError(f"projects.{name}.env_passthrough may not "
                                  f"name {var}")
        if not project.max_timeout_min > 0:
            raise ConfigError(f"projects.{name}.max_timeout_min must be "
                              "finite and positive")
        for key in EFFECTIVE_PROJECT_KEYS:
            out[f"projects.{name}.{key}"] = getattr(project, key)
    return out


def compare_effective(before: dict, after: dict) -> list[dict]:
    """One row per key: ``same``; ``superset`` (a list of names that only
    grew: the neutral names beside the kept legacy ones); ``added`` (a key
    the old format did not have, unset); ``changed``; ``removed``."""
    rows = []
    for key in sorted(set(before) | set(after)):
        if key not in after:
            verdict = "removed"
        elif key not in before:
            verdict = "added" if after[key] is None else "changed"
        elif before[key] == after[key]:
            verdict = "same"
        elif isinstance(before[key], list) and \
                isinstance(after[key], list) and \
                set(before[key]) < set(after[key]):
            verdict = "superset"
        else:
            verdict = "changed"
        rows.append({"key": key, "before": before.get(key),
                     "after": after.get(key), "verdict": verdict})
    return rows


def comparison_problems(rows: list[dict]) -> list[str]:
    return [f"{r['key']}: {r['before']!r} -> {r['after']!r}" for r in rows
            if r["verdict"] in ("changed", "removed")]


# v1 keys that were silently ignored, or crashed the load: the migration
# refuses to carry them (they had no effect; the operator removes or fixes
# them by hand first)
def v1_unknown_keys(raw: dict) -> list[str]:
    found = [k for k in raw if k not in TOP_KEYS]
    found += [f"backend.{k}" for k in (raw.get("backend") or {})
              if k not in ("kind", "guard", "sha256", "profile",
                           "dedicated_budget_gb", "guard_home", "extra_env",
                           "lock_wait_max_s", "admission_retry_s",
                           "admission_retry_max_s")]
    found += [f"queue.{k}" for k in (raw.get("queue") or {})
              if k not in QUEUE_KEYS]
    for name, body in (raw.get("hosts") or {}).items():
        found += [f"hosts.{name}.{k}" for k in body
                  if k not in ("transport", "ssh", "agent_compute",
                               "distro")]
    return found


_HEADER = re.compile(r"^\s*\[\s*(?P<name>[^\[\]]+?)\s*\]\s*(#.*)?$")
_SCHEMA_LINE = re.compile(r'^(?P<pad>\s*)schema\s*=\s*"agent-compute-config/1"'
                          r'(?P<rest>.*)$')
_GUARD_LINE = re.compile(r'^\s*guard\s*=\s*"[^"\n]*"\s*(#.*)?$')


def migrate_v1_text(text: str, stamp: str) -> tuple[str, dict]:
    """``text`` (a v1 config) rewritten to v2 with the same effective
    values, and the summary of what changed.  Lines are inserted (the
    values v1 applied implicitly, made explicit: the legacy guard home and
    names, the default record paths, ``records_in_tree = true`` for a
    project whose v1 records were inside its tree), the schema line is
    changed and the ``[backend]`` ``guard`` line (the retired backend's
    script path, without effect under acs-guard) is commented out; every
    other line and comment is kept.  The result is checked by parsing
    both: anything else is refused (edit by hand)."""
    try:
        raw = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(f"the config does not parse: {error}") from None
    if raw.get("schema") != LEGACY_CONFIG_SCHEMA:
        raise ConfigError(f"not a '{LEGACY_CONFIG_SCHEMA}' config")
    unknown = v1_unknown_keys(raw)
    if unknown:
        raise ConfigError(f"keys the v1 format ignored (or could not load): "
                          f"{unknown}; they had no effect: remove or fix "
                          "them by hand first, then migrate")
    effective = v1_effective_values(raw)  # it loaded under v1, or refuse
    backend = raw.get("backend") or {}
    added: dict = {}
    removed: dict = {}
    if "guard" in backend:
        removed["backend.guard"] = backend["guard"]
    backend_lines = [f"# agent-compute config migrate ({stamp}): v1 -> v2;",
                     "# the values v1 applied implicitly, made explicit"]
    if not (raw.get("test_mode") and backend.get("guard_home")):
        added["backend.guard_home"] = "~/.smc-guard"
        backend_lines.append('guard_home = "~/.smc-guard"')
    if "marker_aliases" not in backend:
        added["backend.marker_aliases"] = [LEGACY_JOB_MARKER]
        backend_lines.append(f'marker_aliases = ["{LEGACY_JOB_MARKER}"]')
    if "legacy_workload_env" not in backend:
        added["backend.legacy_workload_env"] = True
        backend_lines.append("legacy_workload_env = true")
    defaults = {"runlog": Project.runlog, "baseline": Project.baseline}
    project_lines: dict[str, list[str]] = {}
    for name, body in (raw.get("projects") or {}).items():
        missing = [k for k in ("runlog", "baseline") if k not in body]
        if missing:
            project_lines[name] = [
                f"# agent-compute config migrate ({stamp}): the v1 default"
                + ("s" if len(missing) > 1 else "") + ", made explicit"]
            for key in missing:
                added[f"projects.{name}.{key}"] = defaults[key]
                project_lines[name].append(f'{key} = "{defaults[key]}"')
        if effective[f"projects.{name}.records_in_tree"]:
            # D3: v1 wrote this project's records into its tree; kept, now
            # as the explicit opt-in
            added[f"projects.{name}.records_in_tree"] = True
            project_lines.setdefault(name, []).extend([
                f"# agent-compute config migrate ({stamp}): v1 wrote this "
                "project's records into its tree;",
                "# kept, as the explicit opt-in of 0.2.0",
                "records_in_tree = true"])
    lines = text.splitlines(keepends=True)
    out: list[str] = []
    schema_done = backend_done = False
    table = None
    guard_lines = 0
    for line in lines:
        match = _SCHEMA_LINE.match(line)
        if match and not schema_done:
            out.append(f'{match.group("pad")}schema = "{CONFIG_SCHEMA}"'
                       f'{match.group("rest")}\n')
            schema_done = True
            continue
        if table == "backend" and _GUARD_LINE.match(line):
            out.append(f"# agent-compute config migrate ({stamp}): the "
                       f"retired {RETIRED_BACKEND} backend's script, without "
                       "effect under acs-guard:\n")
            out.append(f"# {line.rstrip()}\n")
            guard_lines += 1
            continue
        out.append(line if line.endswith("\n") else line + "\n")
        header = _HEADER.match(line)
        if not header:
            continue
        name = header.group("name")
        table = name
        if name == "backend" and not backend_done:
            out += [f"{entry}\n" for entry in backend_lines]
            backend_done = True
        elif name.startswith("projects."):
            project = name[len("projects."):].strip()
            if project.startswith('"') and project.endswith('"'):
                project = project[1:-1]
            out += [f"{entry}\n" for entry in project_lines.pop(project, [])]
    if not schema_done or not backend_done or project_lines or \
            guard_lines != len(removed):
        raise ConfigError("the config's layout is not one the migration can "
                          "rewrite line by line (schema line, [backend] and "
                          "[projects.*] headers, one `guard = \"...\"` line "
                          "in [backend]); migrate it by hand")
    result = "".join(out)
    expected = tomllib.loads(text)
    expected["schema"] = CONFIG_SCHEMA
    expected.get("backend", {}).pop("guard", None)
    for key, value in added.items():
        node = expected
        parts = key.split(".", 1) if key.startswith("backend.") else \
            ["projects", *key[len("projects."):].rsplit(".", 1)]
        if parts[0] == "backend":
            node.setdefault("backend", {})[parts[1]] = value
        else:
            node["projects"][parts[1]][parts[2]] = value
    if tomllib.loads(result) != expected:
        raise ConfigError("rewriting the config line by line would change "
                          "other settings; migrate it by hand")
    summary = {"changed": {"schema": [LEGACY_CONFIG_SCHEMA, CONFIG_SCHEMA]},
               "added": added, "removed": removed}
    return result, summary


# --- a fresh config (`agent-compute config init`, D4) ----------------------------------------

def init_text(label: str, resources: dict, python: str,
              pueue: str | None = None, pueued: str | None = None,
              state_dir: str | None = None,
              config_hint: str | None = None) -> str:
    """A fresh v2 config for this host, with neutral names, records in the
    state dir, no reserve detector, the zero placeholder pin (every submit
    is refused until the operator pins the engine with evidence) and the
    profile defaults derived from the host (``derived_defaults``).

    ``state_dir`` is the absolute state dir to print and ``config_hint``
    the path the comment tells the operator to save the file as (both from
    ``init_locations``); without them the text keeps the literal
    ``DEFAULT_STATE_DIR`` and the default config path."""
    memory, cores = resources["memory_gb"], resources.get("cores")
    defaults = derived_defaults(memory, cores)
    problem = host_problem(label)
    if problem:
        raise ConfigError(f"--label: {problem}")
    return f"""\
# agent-compute machine config (schema {CONFIG_SCHEMA}), printed by
# `agent-compute config init` for host {label!r}: {memory:g} GB RAM,
# {cores} cores ({resources.get('source')}).  Save it (mode 0600) as
# {config_hint or '~/.config/agent-compute/config.toml'}, review every value, then have the
# operator pin the engine.  Unknown keys are errors.
schema = "{CONFIG_SCHEMA}"
host = "{label}"
state_dir = {json_string(state_dir or DEFAULT_STATE_DIR)}      # local disk, not cloud-synced
python = {json_string(python)}

[backend]
kind = "{BACKEND_ACS}"
# The pinned sha256 of the internal guard engine.  Until the operator runs
# `agent-compute pin-backend --evidence <committed suite output>
# --authorized-by "<who, when, what>"`, every submit is refused.
sha256 = "{'0' * 64}"
# The guard home (lock, current.json, inflight.json) defaults to
# <state_dir>/guard; job processes carry ACS_JOB_MARKER only.
lock_wait_max_s = 43200        # a dispatched job waits at most 12 h for admission
admission_retry_s = 120        # first re-attempt of a memory/disk refusal
admission_retry_max_s = 900    # then doubling up to this

# Derived from this host (D4; docs/memory-model.md): floor_gb = max(2,
# 0.125 x RAM), budget_gb = 0.25 x RAM, container_cpus = cores - 2.
# Admission: measured available memory >= floor_gb + the job's reserve.
# budget_gb is a per-job ceiling (kill above it), never an admission test;
# at most the host's RAM, like backend.dedicated_budget_gb.
[backend.profile]
name = "{label}"
budget_gb = {defaults['budget_gb']:g}
floor_gb = {defaults['floor_gb']:g}
container_cpus = {defaults['container_cpus']}
runlog_tag = ""                # none

[queue]
enabled = true
pueue = {json_string(pueue or 'pueue')}
pueued = {json_string(pueued or 'pueued')}
group = "acs-heavy"            # the one shared heavy group; parallelism 1

# One table per project (uncomment and edit):
# [projects.my-project]
# roots = ["~/src/my-project", "~/src/my-project-*"]
# case_prefix = "mp."
# inputs = ["."]
# max_timeout_min = 120
# max_threads = 4
# containers = []
# records default to the state dir:
#   runlog = "{{state}}/projects/{{project}}/runlog-{{host}}.md"
#   baseline = "{{state}}/projects/{{project}}/baseline-{{host}}.json"
# (a path inside the repository needs records_in_tree = true, and must stay
# outside the project's inputs)
# reserve_detector defaults to "none"; "smc-ladder" is the shipped example
# of a project-specific detector (ladder configs that declare reserves)
"""


# --- where a fresh config belongs (K3; register A43, A44) --------------------------------

INIT_CONFIG_REL = ".config/agent-compute/config.toml"
INIT_STATE_REL = ".local/state/agent-compute"
_ABSENT = (errno.ENOENT, errno.ENOTDIR)


def init_locations(environ=None) -> tuple[Path, Path, Path]:
    """``(install_home, config_path, state_dir)`` of the installation a
    fresh config is for: ``$ACS_INSTALL_HOME`` when set (a throwaway or
    second installation), else the real home (``~``, as ``expand`` reads
    it).  All absolute.  For the real home the state dir is the one the
    default ``state_dir = "~/.local/state/agent-compute"`` names."""
    environ = os.environ if environ is None else environ
    home = environ.get(ENV_INSTALL_HOME) or os.path.expanduser("~")
    home = Path(os.path.abspath(os.path.expanduser(home)))
    return home, home / INIT_CONFIG_REL, home / INIT_STATE_REL


def _absence(path: Path) -> None:
    """``path`` gave ENOENT/ENOTDIR: that is absence unless a symlink on
    the way is dangling (an unmounted volume, a moved target), which leaves
    the question open: ``ConfigError``."""
    for part in (path, *path.parents):
        try:
            found = os.lstat(part)
        except OSError as inner:
            if inner.errno in _ABSENT:
                continue
            raise ConfigError(f"{part}: cannot be examined "
                              f"({inner.strerror})") from None
        if (found.st_mode & 0o170000) == 0o120000:
            try:
                os.stat(part)
            except OSError:
                raise ConfigError(f"{part}: a link whose target is absent "
                                  f"(on the way to {path})") from None
        return


def _state_dir_of(path: Path) -> Path | None:
    """The absolute state dir the config at ``path`` names, read raw (not
    loaded: any schema); None when the file is absent (ENOENT/ENOTDIR and
    no dangling link on the way).  ``ConfigError`` when it cannot be read
    or understood: then it cannot be ruled out that it uses a state dir."""
    try:
        with open(path, "rb") as handle:
            data = handle.read()
    except OSError as error:
        if error.errno not in _ABSENT:
            raise ConfigError(f"{path}: another installation's config "
                              f"cannot be read ({error.strerror}), so it "
                              "cannot be ruled out that it uses this state "
                              "dir") from None
        _absence(path)
        return None
    try:
        raw = tomllib.loads(data.decode("utf-8"))
    except (ValueError, RecursionError) as error:
        raise ConfigError(f"{path}: another installation's config cannot be "
                          f"parsed ({error}), so it cannot be ruled out "
                          "that it uses this state dir") from None
    value = raw.get("state_dir", DEFAULT_STATE_DIR)
    text = os.path.expandvars(os.path.expanduser(value)) \
        if isinstance(value, str) else ""
    if "\0" in text or not os.path.isabs(text):
        raise ConfigError(f"{path}: its state_dir {value!r} does not resolve "
                          "to an absolute path, so it cannot be ruled out "
                          "that it uses this state dir")
    return Path(text)


def _overlaps(first: Path, second: Path) -> bool:
    a, b = os.path.realpath(first), os.path.realpath(second)
    return a == b or a.startswith(b + os.sep) or b.startswith(a + os.sep)


def state_dir_conflict(state_dir: Path, own_config: Path,
                       environ=None) -> str | None:
    """Why a fresh config for the installation whose config is
    ``own_config`` must not print ``state_dir``: another installation's
    config (the default one at ``~/.config/agent-compute/config.toml``, or
    the one ``$AGENT_COMPUTE_CONFIG`` names) uses that state dir, one
    inside it or one containing it.  None when no other installation does.
    Read-only; a config that cannot be read or understood raises
    ``ConfigError`` (it cannot be ruled out)."""
    environ = os.environ if environ is None else environ
    others: list[tuple[Path, str]] = []
    if environ.get(ENV_CONFIG):
        others.append((expand(environ[ENV_CONFIG]).absolute(),
                       f"${ENV_CONFIG}"))
    others.append((Path(os.path.expanduser("~")) / INIT_CONFIG_REL,
                   "the default installation's config"))
    mine, seen = os.path.realpath(own_config), set()
    for path, why in others:
        key = os.path.realpath(path)
        if key == mine or key in seen:
            continue
        seen.add(key)
        theirs = _state_dir_of(path)
        if theirs is not None and _overlaps(theirs, state_dir):
            return (f"{why} ({path}) uses the state dir {theirs}, which "
                    f"overlaps {state_dir}: two installations would share "
                    "one state dir and one queue daemon")
    return None


def json_string(value: str) -> str:
    """A TOML basic string (JSON escapes are valid TOML ones)."""
    import json
    return json.dumps(value)
