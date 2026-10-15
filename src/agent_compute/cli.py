"""agent-compute: explicit, agent-invoked admission for heavy jobs.

    agent-compute doctor [--json]
    agent-compute submit  --case KEY --timeout-min M [options] -- CMD ...
    agent-compute run     --case KEY --timeout-min M [options] -- CMD ...
    agent-compute status  [JOB|REQUEST-ID] [--json] [--all] [--unfinished]
                          [--mine X] [--request-id R]
    agent-compute wait    JOB [--timeout S]
    agent-compute cancel  JOB
    agent-compute reconcile [--terminate JOB]
    agent-compute log     JOB [--tail N]
    agent-compute fetch   JOB DIR
    agent-compute hosts   [--json]
    agent-compute placements resolve [R] [--json]
    agent-compute _summary --json          # read-only host summary
    agent-compute _request R --json        # read-only request-id lookup
    agent-compute resume
    agent-compute daemon  {start,stop,status}
    agent-compute config migrate --authorized-by "<who, when, what>"
    agent-compute config init [--label NAME]
    agent-compute pin-backend --evidence FILE --authorized-by "<who, ...>"
    agent-compute --version
    agent-compute --host HOST <any of the above>   # runs on that host

A successful ``submit`` means *accepted*, never *succeeded*.

Several hosts (campaign G slice G3a, decisions D5, D6): ``hosts`` shows
every configured host (``_summary`` of each, read-only, in parallel);
``--host X submit`` writes a local placement record (``placement.py``)
before the remote call; ``status``/``wait``/``cancel``/``reconcile
--terminate``/``log``/``fetch`` given a job id or request id of such a
record go to the recorded host (that host's own gates apply there).
Automatic placement (G3b): an unpinned ``submit``/``run`` of a project
that declares more than one host is placed by the client-side policy
(``placement.evaluate``) and sent once, never retried elsewhere; a lost
reply is resolved by request id on the same host only (``placements
resolve``, ``status R``, ``status --resolve``, ``hosts``; plain ``status``
asks no other host); a placed or pinned submit from a checkout
of the project requires its commit at the destination (``--require-commit``,
unless ``--any-commit``); a call that arrived routed is never routed on
(A40).  ``run`` is
``submit`` + ``wait`` with a bounded admission wait; with the queue enabled
it takes its turn in the same shared queue.
"""

from __future__ import annotations

import argparse
import json
import os
import posixpath
import re
import signal
import subprocess
import sys
import time
import tomllib
from pathlib import Path

from . import authority, identity, procs, safety
from . import config as cfgmod
from . import placement as pl
from . import queue as q
from . import remote as remote_mod
from .config import (BACKEND_ACS, BACKEND_KINDS, CONFIG_SCHEMA, JOB_MARKER,
                     LEGACY_JOB_MARKER, Config, ConfigError,
                     control_variable, env_problem, expand, load,
                     worktree_of)
from .fingerprint import (checkout_state, code_problem, guard_identity,
                          inputs_fingerprint)
from .fsprobe import (exists_strict, glob_strict, is_dir_strict, lexists_strict,
                      is_file_strict, is_symlink_strict, walk_files_strict)
from .runner import DEFAULT_PROBE_S, JOB_ENV
from .state import (EVENT_SCHEMA, MANIFEST_SCHEMA, NEVER_STARTED,
                    STATE_SCHEMA, JobStore, RecordUnreadable, Unreadable,
                    _safe, ensure_host_id, events_path, json_default,
                    new_job_id, private_dir, read_host_id, read_json,
                    state_lock, utc_now, write_json)

BASE_ENV = ("PATH", "HOME", "LANG", "LC_ALL", "USER", "LOGNAME", "SHELL",
            "TMPDIR")
REQUIRED_GUARD_OPTIONS = ("--outputs", "--runlog", "--baseline", "--wait",
                          "--timeout-min", "--reserve-gb", "--cwd",
                          "--threads", "--case", "--venue", "--probe-s")
# what the runner passes the internal guard in addition
REQUIRED_ENGINE_OPTIONS = ("--acs-job", "--acs-state", "--profile-file",
                           "--project-root", "--reserve-detector",
                           "--container-limits", "--budget-gb")
REQUIRED_ENGINE_OPTIONS += ("--marker-alias", "--legacy-workload-env")
EXIT_WAIT_TIMEOUT = 75
EXIT_NOT_ACCEPTED = 2
# G2a: a request id names a binding file (state._safe keeps exactly these
# characters, so two ids never share one) and is shown in one line
REQUEST_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
ENV_SUBMITTER = "AGENT_COMPUTE_SUBMITTER"
GUARD_RECORD_SCHEMA = "agent-compute-guard-record/1"
LEGACY_GUARD_RECORD_SCHEMA = "smc-guard-record/1"
BASELINE_SCHEMA = "agent-compute-baseline/1"
LEGACY_BASELINE_SCHEMA = "smc-perf-baseline/1"
AUTHORIZATION_SCHEMA = "agent-compute-authorization/1"
HELD = "held-inputs-changed"
CLOUD_MARKERS = ("Mobile Documents", "CloudStorage", "Dropbox",
                 "Google Drive", "OneDrive", "iCloud")
# A12: how long a job may sit accepted with neither a queue task nor a
# runner before it is taken for stranded (its submitting client died between
# reserving the job and enqueueing it) and finished as never started.  It
# covers, with a wide margin, the longest the submit path can legitimately
# take between the two: the manifest (the guard hash, docker inspect/exec
# at 30 s each, git at 120 s per call) plus the queue daemon start and
# `pueue add` (60 s per call) -- about 7 min in the worst case.  Safety
# never depends on it: see settle_unclaimed.
UNCLAIMED_GRACE_S = 900.0


def out(obj, as_json: bool) -> None:
    if as_json:
        print(json.dumps(obj, indent=1, sort_keys=True,
                         default=json_default))
    else:
        print(human(obj))


def acs_identity(config: Config) -> dict:
    """What every diagnostic output carries (D10): the running version, its
    source sha, its safety level, the engine sha256, this host's label and
    id, and the time."""
    ident = identity.identity()
    return {"version": ident["version"], "source_sha": ident["source_sha"],
            "package_version": ident["package_version"],
            "safety_level": identity.running_safety_level(),
            "engine_sha256": identity.engine_sha256(),
            "host": config.host,
            "host_id": _host_id_quiet(config), "utc": utc_now()}


def routed_from() -> str | None:
    """Register A40 (G3b): the sender's label when this call arrived over a
    route (``remote.NO_REROUTE_ENV``, set in every remote script); such a
    call is never routed on (no ``--host`` forwarding, no routing by
    placement record, no automatic placement, no resolution queries, no
    remote probes)."""
    return os.environ.get(remote_mod.NO_REROUTE_ENV) or None


def _host_id_quiet(config: Config) -> str | None:
    record = read_json(config.state_dir / "host-id.json")
    return record.get("host_id") if isinstance(record, dict) else None


def held_line(view: dict) -> str | None:
    """G2a: the one line that says a job was held and why (printed by
    `wait`, human `status` and `run`)."""
    if view.get("outcome") != HELD:
        return None
    return (f"{view.get('job_id')}: {HELD} (never started): "
            f"{view.get('reason')}")


def record_view(record):
    """A record for output (C4: an ``Unreadable`` as its view)."""
    return record.view() if isinstance(record, Unreadable) else record


def _blocked_jobs(store: JobStore, host: str) -> dict | None:
    """The jobs in the host block; None when the block file cannot be read
    (C4: then no job can be shown NOT to be in it)."""
    block = store.block(host)
    if isinstance(block, Unreadable):
        return None
    return (block or {}).get("jobs") or {}


def _admission_records_unreadable(store: JobStore, host: str) -> list:
    """C4: the host block and the dispatch gate, where they exist but cannot
    be read."""
    return [record for record in (store.block(host), store.gate())
            if isinstance(record, Unreadable)]


def human(obj, indent: int = 0) -> str:
    pad = "  " * indent
    if isinstance(obj, dict):
        lines = []
        for key, value in obj.items():
            if isinstance(value, (dict, list)) and value:
                lines.append(f"{pad}{key}:")
                lines.append(human(value, indent + 1))
            else:
                lines.append(f"{pad}{key}: {value}")
        return "\n".join(lines)
    if isinstance(obj, list):
        return "\n".join(f"{pad}- {human(v, indent + 1).strip()}"
                         for v in obj)
    return f"{pad}{obj}"


# --- submission --------------------------------------------------------------------

def build_env(config: Config, project, extra: list[str]) -> dict[str, str]:
    env = {k: os.environ[k] for k in BASE_ENV if k in os.environ}
    if config.job_path:
        env["PATH"] = config.job_path
    for name in project.env_passthrough:
        if name in os.environ:
            env[name] = os.environ[name]
    for item in extra:
        name, sep, value = item.partition("=")
        if not sep or not name:
            raise ConfigError(f"--env expects NAME=VALUE, got {item!r}")
        problem = env_problem(name, config.marker_aliases)
        if problem:
            raise ConfigError(f"--env {problem}")
        env[name] = value
    # S5a: nor the job markers of a job this submission runs inside of (the
    # child's own runner and guard set its own)
    return {k: v for k, v in env.items()
            if not control_variable(k, config.marker_aliases)}


# --- inside a running job (S5a) ------------------------------------------------------

def parent_job() -> dict | None:
    """The job this process runs inside of, from its environment: the ACS
    job id the runner exports (``ACS_JOB_ID``) and the guard's job marker
    (``ACS_JOB_MARKER``, or the legacy ``SMC_GUARD_JOB``).  None outside
    any job.  Metadata only.  ``smc_guard_job`` (the legacy variable's
    value) is kept for readers of the v1 view."""
    acs_job = os.environ.get(JOB_ENV) or None
    legacy = os.environ.get(LEGACY_JOB_MARKER) or None
    guard_job = os.environ.get(JOB_MARKER) or legacy
    if not acs_job and not guard_job:
        return None
    return {"acs_job": acs_job, "guard_job": guard_job,
            "smc_guard_job": legacy}


def inside_job_refusal(config: Config, command: str,
                       target: str | None = None) -> str | None:
    """Why ``run`` (or ``wait`` on ``target``) must not block here: this
    process runs inside a job that holds this host's heavy slot, so waiting
    for another job of this host would wait on that slot until the current
    job's own ceiling.  None when it may proceed:

    * outside any job;
    * ``wait`` on a job that already has an outcome;
    * the marked ACS job is a job of this host that has ended (a process
      that outlived it, e.g. a detached supervisor, holds no slot);
    * an ``ACS_JOB_ID`` this host's state does not know, with no guard
      marker (another ACS installation's job).
    A guard marker without a known ACS job (an external guard's job, or an
    ACS job of a version before S5a) is refused: nothing proves the slot
    free."""
    parent = parent_job()
    if parent is None:
        return None
    store = JobStore(config.state_dir)
    if target is not None:
        st = store.state(target)
        if isinstance(st, dict) and st.get("outcome"):
            return None
    acs_job = parent["acs_job"]
    known = False
    if acs_job:
        try:
            man = store.manifest(acs_job)
            st = store.state(acs_job)
        except ValueError:
            man = st = None
        if isinstance(man, Unreadable):
            known = True            # C4: cannot prove it another host's
        if isinstance(man, dict) and man.get("host") == config.host:
            known = True
            if isinstance(st, dict) and st.get("outcome"):
                return None
    if not known and not parent["guard_job"]:
        return None
    holder = acs_job if known else f"guarded job {parent['guard_job']}"
    return (f"`{command}` from inside the running job {holder} would wait on "
            "the slot this job holds (one heavy job per host). Submit the "
            "next step with `agent-compute submit` and exit, or wait from "
            "outside the job (`agent-compute status JOB` is read-only)")


NESTED_BASENAMES = ("guard.py", "agent-compute")
_PYTHON = re.compile(r"python(\d+(\.\d+)*)?")
_PY_OPTS_WITH_ARG = ("-X", "-W")
_ENV_OPTS_WITH_ARG = ("-u", "--unset", "-C", "--chdir")


def nested_guard(argv: list[str]) -> str | None:
    """A14: what the command would nest (``guard.py``, ``agent-compute``
    or ``python -m agent_compute``), or None.  Looks through ``env
    [options] [NAME=VALUE ...]`` and a ``python`` interpreter's options to
    the program it runs."""
    args = [str(a) for a in argv]
    while args:
        base = os.path.basename(args[0])
        if base in NESTED_BASENAMES:
            return base
        if base == "env":
            i = 1
            while i < len(args) and (args[i].startswith("-") or
                                     "=" in args[i].lstrip("=")):
                i += 2 if args[i] in _ENV_OPTS_WITH_ARG else 1
            args = args[i:]
            continue
        if not _PYTHON.fullmatch(base):
            return None
        i = 1
        while i < len(args):
            arg = args[i]
            if arg == "-" or arg.startswith("-c") or arg == "--":
                return None if arg != "--" else (
                    _script(args[i + 1]) if i + 1 < len(args) else None)
            module = None
            if arg.startswith("-m"):
                module = arg[2:] or (args[i + 1] if i + 1 < len(args) else "")
            elif arg.startswith("-") and not arg.startswith("--") and \
                    arg.endswith("m") and "c" not in arg:
                module = args[i + 1] if i + 1 < len(args) else ""
            if module is not None:
                return "python -m agent_compute" if \
                    module.split(".")[0] == "agent_compute" else None
            if arg in _PY_OPTS_WITH_ARG:
                i += 2
                continue
            if arg.startswith("-"):
                i += 1
                continue
            return _script(arg)
        return None
    return None


def _script(path: str) -> str | None:
    base = os.path.basename(path)
    if base == "engine.py" and \
            Path(path).parent.name == "guard" and \
            Path(path).parent.parent.name == "agent_compute":
        return "the ACS-internal guard"
    return base if base in NESTED_BASENAMES else None


def build_manifest(config: Config, args, argv: list[str],
                   job_id: str | None = None) -> dict:
    cwd = expand(args.cwd).resolve()
    if not cwd.is_dir():
        raise ConfigError(f"--cwd {cwd} is not a directory")
    worktree = worktree_of(cwd)
    project = config.resolve_project(worktree, args.project)
    req_timeout = float(args.timeout_min)
    if not 0 < req_timeout <= project.max_timeout_min:
        raise ConfigError(f"--timeout-min must be in (0, "
                          f"{project.max_timeout_min:g}] for project "
                          f"{project.name}")
    if not 1 <= args.threads <= project.max_threads:
        raise ConfigError(f"--threads must be in [1, {project.max_threads}] "
                          f"for project {project.name}")
    if args.dense_threads is not None and \
            not 1 <= args.dense_threads <= project.max_threads:
        raise ConfigError(f"--dense-threads must be in [1, "
                          f"{project.max_threads}]")
    probe = bool(getattr(args, "probe", False))
    probe_s = getattr(args, "probe_s", None)
    if probe_s is not None and not probe:
        raise ConfigError("--probe-s needs --probe")
    if probe:
        # the guard stops a probe at --probe-s (its default 60 s); a longer
        # probe than the wall-time ceiling would end timed-out instead
        probe_s = float(DEFAULT_PROBE_S if probe_s is None else probe_s)
        if not 0 < probe_s <= req_timeout * 60:
            raise ConfigError(f"--probe-s must be in (0, --timeout-min x 60 "
                              f"= {req_timeout * 60:g}] s (got {probe_s:g})")
    if not argv:
        raise ConfigError("no command given after --")
    nested = nested_guard(argv)
    if nested:
        raise ConfigError(
            f"the command runs {nested} itself; ACS already runs the guard "
            "(a nested one only passes through, or refuses a container "
            "launch): pass the workload directly, e.g. `agent-compute "
            "submit --case ... [--container ...] -- <workload command>`")
    backend = backend_identity(config)
    if not backend.get("exists"):
        raise ConfigError(f"backend guard missing at {backend['path']}")
    if backend["sha256"] != config.guard_sha256:
        raise ConfigError(
            f"backend guard {backend['sha256'][:12]} differs from the "
            f"verified {str(config.guard_sha256)[:12]}; ACS will not follow "
            "an unverified guard.  Report this to the operator; do not try "
            "to accept the new guard yourself")
    container = None
    if args.container:
        if args.env:
            # A7: the guard's `docker exec` exports only its own variables
            raise ConfigError(
                "--env does not reach a container workload (the guard's "
                "`docker exec` passes only its own variables); put them in "
                "the command instead: `-- env NAME=VALUE <command ...>`")
        if args.container not in project.containers:
            raise ConfigError(f"container {args.container!r} is not listed "
                              f"for project {project.name}; the guard "
                              "applies per-job docker limits to it")
        cid = procs.container_id(args.container)
        if not cid:
            raise ConfigError(f"container {args.container!r} is not running")
        workdir = args.container_workdir or project.container_workdir
        if not workdir:
            raise ConfigError("--container-workdir is required")
        daemon = procs.docker_daemon_id()
        if not daemon:
            raise ConfigError("the docker daemon's identity cannot be read "
                              "(`docker info`)")
        container = {"name": args.container, "id": cid, "workdir": workdir,
                     "daemon_id": daemon}
        # A7: the project's env_passthrough does not reach the container
        # either.  Recorded (names only) and reported, not refused: it is
        # operator policy the agent did not ask for, and refusing on an
        # ambient session variable would make submits depend on it.
        unforwarded = sorted(n for n in project.env_passthrough
                             if n in os.environ)
        if unforwarded:
            container["env_not_forwarded"] = unforwarded
        if project.container_provenance:
            from .runner import container_provenance
            path = f"{workdir.rstrip('/')}/{project.container_provenance}"
            container["provenance_path"] = path
            # A13: by the id just resolved, never by the name again
            container["provenance_sha256"] = container_provenance(cid, path)
            if container["provenance_sha256"] is None:
                raise ConfigError(f"cannot read {path} in {args.container}; "
                                  "the container's code cannot be verified")
    wait_cap = config.lock_wait_max_s
    admission_wait = min(float(args.lock_wait or wait_cap), wait_cap)
    job_id = job_id or new_job_id()
    try:
        fingerprint = inputs_fingerprint(worktree, cwd, argv, project.inputs,
                                         args.input)
    except OSError as error:
        # A49: an input that cannot be examined is not "missing"
        raise ConfigError(f"the job's inputs cannot be fingerprinted "
                          f"({error}); nothing was accepted") from None
    # G3b: the code requirement of a placed or pinned submit (checked by
    # accept and again by the runner at dispatch)
    required = getattr(args, "require_commit", None)
    clean = bool(getattr(args, "require_clean", False))
    code = {"head": fingerprint.get("git_head"), "dirty": None,
            "required_commit": required, "require_clean": clean,
            "any_commit": bool(getattr(args, "any_commit", False))}
    if required or clean:
        code["head"], code["dirty"] = checkout_state(worktree)
    ident = identity.identity()
    submitter = getattr(args, "submitter", None) or \
        os.environ.get(ENV_SUBMITTER) or None
    return {
        "schema": MANIFEST_SCHEMA, "job_id": job_id,
        "request_id": args.request_id, "owner": args.owner,
        # D10: the installed version name and its source sha
        "acs_version": ident["version"], "acs_source_sha": ident["source_sha"],
        "host": config.host, "host_id": _host_id_quiet(config),
        # D2: the names this job is launched with (the runner refuses it if
        # the config changes them before dispatch; reconcile reads them)
        "guard_home": str(config.effective_guard_home),
        "marker": config.markers[0], "marker_aliases": config.markers[1:],
        "legacy_workload_env": config.legacy_workload_env,
        "schemas": {"manifest": MANIFEST_SCHEMA, "state": STATE_SCHEMA,
                    "config": CONFIG_SCHEMA,
                    "guard_record": GUARD_RECORD_SCHEMA,
                    "baseline": BASELINE_SCHEMA,
                    "event": EVENT_SCHEMA},
        "submitter": submitter,
        "project": project.name, "worktree": str(worktree), "cwd": str(cwd),
        "argv": list(argv), "case": args.case,
        "guard_case": f"{project.case_prefix}{args.case}",
        "slice": args.slice, "units": args.units,
        "resources": {"timeout_min": req_timeout, "threads": args.threads,
                      "reserve_gb": args.reserve_gb,
                      "dense_threads": args.dense_threads,
                      "class": "heavy",
                      # S5a: the guard's probe mode (None: a normal run)
                      "probe": probe,
                      "probe_s": probe_s if probe else None},
        "paths": {"runlog": str(config.fill(project.runlog, project.name,
                                            worktree)),
                  "baseline": str(config.fill(project.baseline, project.name,
                                              worktree))},
        "env": build_env(config, project, args.env),
        "inputs": {"pathspecs": project.inputs, "extra": args.input,
                   "fingerprint": fingerprint},
        "backend": {**backend, "kind": config.backend_kind,
                    "profile": config.profile,
                    # at submit; the runner applies the live value at dispatch
                    "dedicated_budget_gb": config.dedicated_budget_gb},
        "container": container, "admission_wait_s": admission_wait,
        # S5a: the job this submission was made from inside of (metadata)
        "parent_job": parent_job(),
        # G3b: the code requirement, and the placement that chose this
        # host (request id, placing host, policy snapshot; None unless
        # placed here by this host's own policy)
        "code": code,
        "placement": getattr(args, "placement", None),
        "submitted_utc": utc_now(),
    }


def backend_path(config: Config) -> Path:
    """The guard ACS runs: the internal engine of THIS installed version
    (the runner of a job submitted here runs from the same version dir).
    There is no other (the external guard.py backend is retired, D1)."""
    from .runner import engine_path
    return engine_path()


def backend_identity(config: Config) -> dict:
    ident = guard_identity(backend_path(config))
    return {**ident, "kind": config.backend_kind}


def accept(config: Config, args, argv: list[str]) -> tuple[int, dict]:
    floor = safety.running_floor_problems()
    if floor:
        # Fail closed before anything is written: new work must not start on
        # an installation that contains a known false-safe decision.  Jobs
        # already queued keep the version they were submitted with.
        return EXIT_NOT_ACCEPTED, {
            "accepted": False,
            "reason": "installation below the safety floor; operator must "
                      "roll forward: " + "; ".join(floor)}
    store = JobStore(config.state_dir)
    private_dir(config.state_dir)
    private_dir(config.jobs_dir)
    unreadable = _admission_records_unreadable(store, config.host)
    if unreadable:
        # C4: an admission record that cannot be read is never "no block"
        # or "gate open"; nothing is written
        return EXIT_NOT_ACCEPTED, {
            "accepted": False,
            "reason": "host admission state unreadable: " +
                      "; ".join(r.reason for r in unreadable) +
                      "; an operator inspects it (see `agent-compute "
                      "doctor`); never started"}
    if args.request_id is not None and \
            not REQUEST_ID.fullmatch(args.request_id):
        return EXIT_NOT_ACCEPTED, {
            "accepted": False,
            "reason": f"request id {args.request_id!r} is not valid: 1 to "
                      "128 characters of A-Z a-z 0-9 . _ : -, starting "
                      "with a letter or digit (no whitespace, no '/')"}
    problem = commit_options_problem(args)
    if problem:
        return EXIT_NOT_ACCEPTED, {"accepted": False,
                                   "reason": problem + "; never started"}
    submitter = getattr(args, "submitter", None)
    if submitter is None:
        submitter = os.environ.get(ENV_SUBMITTER) or None
    if submitter is not None and not REQUEST_ID.fullmatch(submitter):
        return EXIT_NOT_ACCEPTED, {
            "accepted": False,
            "reason": f"submitter {submitter!r} (--submitter or "
                      f"{ENV_SUBMITTER}) is not valid: like a request id, "
                      "1 to 128 characters of A-Z a-z 0-9 . _ : -, starting "
                      "with a letter or digit"}
    host_id = ensure_host_id(config.state_dir, config.host)
    if isinstance(host_id, Unreadable):
        return EXIT_NOT_ACCEPTED, {
            "accepted": False,
            "reason": f"the host identity record {host_id.reason}; an "
                      "operator inspects it; never started"}
    renamed = rename_problem(config, store)
    if renamed:
        return EXIT_NOT_ACCEPTED, {"accepted": False,
                                   "reason": renamed["reason"]}
    moved = guard_home_change_problem(config, store)
    if moved:
        return EXIT_NOT_ACCEPTED, {"accepted": False,
                                   "reason": moved["reason"]}
    record_guard_home(config)
    previous = None
    if args.request_id:
        existing = store.request_job(args.request_id)
        if isinstance(existing, str):
            previous = existing
            # a stale stranded job (A12) must not burn its request id
            settle_unclaimed(config, store, [existing])
    job_id, existing = _reserve(store, config.host, args.request_id)
    if isinstance(existing, BoundElsewhere):
        # a placement record binds it to another host (checked under the
        # state lock with the reservation: review round 1, note 8)
        return EXIT_NOT_ACCEPTED, {
            "accepted": False, "request_id": args.request_id,
            "reason": existing.reason(config.state_dir, args.request_id)}
    if isinstance(existing, Unreadable):
        return EXIT_NOT_ACCEPTED, {
            "accepted": False,
            "reason": f"request binding unusable: {existing.reason}; the "
                      f"request id {args.request_id!r} may map to a job "
                      "that ran; an operator inspects it; never started"}
    if existing:
        st = store.state(existing)
        st = st if isinstance(st, dict) else {
            "phase": "state unreadable" if st is not None else None}
        return EXIT_NOT_ACCEPTED, {
            "accepted": False, "job_id": existing,
            "reason": f"request id {args.request_id!r} already maps "
                      f"to {existing} ({st.get('outcome') or st.get('phase')}); "
                      "inspect it instead of resubmitting"}
    try:
        manifest = build_manifest(config, args, argv, job_id)
    except BaseException:
        _release(store, job_id, args.request_id)
        raise
    boundary = project_hosts_refusal(
        config, config.projects[manifest["project"]],
        # pinned here, or placed here by the policy (G3b)
        pinned_here=getattr(args, "host", None) == config.host or
        getattr(args, "placement", None) is not None or
        routed_from() is not None)
    if boundary:
        # the early check could not resolve the project (review round 1,
        # note 7): the reservation is undone, nothing was enqueued
        _release(store, job_id, args.request_id)
        return EXIT_NOT_ACCEPTED, boundary
    problem = code_problem(manifest.get("code"), manifest["worktree"],
                           f"on {config.host}")
    if problem:
        # G3b: refused at submit (the runner checks again at dispatch);
        # the reservation is undone, nothing was enqueued
        _release(store, job_id, args.request_id)
        return EXIT_NOT_ACCEPTED, {
            "accepted": False, "request_id": args.request_id,
            "host": config.host, "host_id": manifest.get("host_id"),
            "acs_version": manifest.get("acs_version"),
            "acs_source_sha": manifest.get("acs_source_sha"),
            "head": manifest["code"].get("head"), "code": manifest["code"],
            "reason": problem + "; never started"}
    job_dir = store.job_dir(job_id)
    write_json(job_dir / "manifest.json", manifest)
    notices = []
    try:
        if config.queue_enabled:
            notices += q.ensure_daemon(config)
            task_id = q.add_task(config, job_id)
            if not _mark_queued(store, job_id, task_id):
                # finished while it was being enqueued (cancelled, or
                # settled as stranded): the task must never run it.  (Were
                # it dispatched anyway, the runner's claim refuses a job
                # with an outcome.)
                q.remove_queued(config, task_id)
                st = store.state(job_id)
                st = st if isinstance(st, dict) else {}
                return EXIT_NOT_ACCEPTED, {
                    "accepted": False, "job_id": job_id,
                    "reason": "the job ended while it was being enqueued "
                              f"({st.get('outcome')}: {st.get('reason')})"}
        else:
            spawn_detached_runner(config, job_id)
    except (q.QueueError, OSError, subprocess.SubprocessError,
            ValueError, RecordUnreadable) as error:
        # The add may have reached the daemon before its reply was lost (a
        # client timeout, a nonzero exit, unparseable stdout, or a failed
        # _mark_queued after the add).  Only the compare-and-set proves the
        # job never started; when it refuses (a runner claimed the job, or
        # its state cannot be read) the job may be running: its request id
        # stays bound, so a resubmit returns it instead of a second job (C1)
        if _finish_if_unclaimed(store, job_id, "refused",
                                f"could not enqueue: {error}; never started"):
            _unbind(store, args.request_id, job_id)
            return EXIT_NOT_ACCEPTED, {"accepted": False, "job_id": job_id,
                                       "reason": f"could not enqueue: {error}"}
        return EXIT_NOT_ACCEPTED, {
            "accepted": "unknown", "job_id": job_id, "host": config.host,
            "evidence": evidence_paths(config, job_id),
            "reason": f"the enqueue reply was lost ({error}) after the job "
                      "was handed to the queue, and a runner has claimed it "
                      "(or its state cannot be read): the job may be "
                      f"running.  `agent-compute status {job_id}` or `wait` "
                      "it; do not resubmit (the same --request-id returns "
                      "this job)"}
    unforwarded = (manifest.get("container") or {}).get("env_not_forwarded")
    if unforwarded:
        notices.append(f"env_passthrough {', '.join(unforwarded)} is set "
                       "here but does not reach a container workload; if "
                       "it needs them, pass them in the command: `-- env "
                       "NAME=VALUE <command ...>`")
    if not config.queue_enabled:
        notices.append("queue disabled on this host: the job's runner was "
                       "started detached and waits for the compute lock")
    if store.block(config.host):
        notices.append(f"host {config.host} admission is BLOCKED; the job "
                       "will be refused at dispatch until `agent-compute "
                       "reconcile` clears it")
    if store.gate():
        notices.append("dispatch gate is closed; `agent-compute resume` "
                       "after reviewing status")
    if previous and previous != job_id:
        before = store.state(previous)
        if isinstance(before, dict) and before.get("outcome") == HELD:
            notices.append(f"request id {args.request_id} was bound to "
                           f"{previous}, which ended {HELD} (never "
                           f"started): {before.get('reason')}")
    fingerprint = (manifest.get("inputs") or {}).get("fingerprint") or {}
    return 0, {"accepted": True, "job_id": job_id, "host": config.host,
               "project": manifest["project"],
               "evidence": evidence_paths(config, job_id),
               "notices": notices,
               "meaning": "accepted for admission; not started, not succeeded",
               # G3a: what a placing host records (D11 placement record)
               "host_id": manifest.get("host_id"),
               "acs_version": manifest.get("acs_version"),
               "acs_source_sha": manifest.get("acs_source_sha"),
               "head": fingerprint.get("git_head"),
               # G3b: the code requirement as checked here
               "code": manifest.get("code")}


def rename_problem(config: Config, store: JobStore) -> dict | None:
    """J (coupling inventory): renaming ``host`` splits the state, since
    blocks, prior-runner and lost-supervision scans go by the label.  So
    while an unfinished job (or one whose records cannot be read) or a host
    block is recorded under another label, a different label is refused
    (``doctor`` ERROR, ``submit`` refusal).  None when there is none."""
    jobs, blocks = [], []
    for job_id in store.all_ids():
        try:
            st = store.state(job_id)
        except ValueError:
            continue
        if isinstance(st, dict) and st.get("outcome"):
            continue
        man = store.manifest(job_id)
        host = man.get("host") if isinstance(man, dict) else \
            st.get("host") if isinstance(st, dict) else None
        if host != config.host and (host is not None or
                                    isinstance(man, Unreadable) or
                                    isinstance(st, Unreadable)):
            jobs.append({"job_id": job_id, "host": host})
    try:
        names = sorted(p.stem for p in glob_strict(store.blocks, "*.json"))
    except OSError:
        names = ["<unreadable blocks dir>"]     # A49: never "no blocks"
    blocks = [name for name in names if name != _safe(config.host)]
    if not jobs and not blocks:
        return None
    labels = sorted({j["host"] or "<unreadable>" for j in jobs} |
                    set(blocks))
    return {"jobs": jobs, "blocks": blocks,
            "reason": f"host label {config.host!r} differs from "
                      f"{', '.join(map(repr, labels))}, under which "
                      f"unfinished jobs {[j['job_id'] for j in jobs]} or "
                      f"host blocks {blocks} are recorded: a rename now "
                      "would split this host's state (blocks and lost-"
                      "supervision checks go by the label).  The operator "
                      "restores the old label until they finish and are "
                      "reconciled; never started"}


GUARD_HOME_RECORD = "guard-home.json"


def _legacy_home() -> str:
    return str(Path.home() / ".smc-guard")


def guard_home_change_problem(config: Config, store: JobStore
                              ) -> dict | None:
    """D2: the guard home changes only at an idle checkpoint.  Refused
    (``doctor`` ERROR, ``submit`` refusal; queued jobs are refused at
    dispatch by ``revalidate``) while an unfinished job's manifest names
    another home, or while the previous home still holds an in-flight
    marker, a lock holder record or lock users.  The previous home is the
    one last recorded in ``<state>/guard-home.json``; before any record,
    the home every version before 0.2.0 used in production
    (``~/.smc-guard``; not assumed for a test-mode config).  A manifest
    before 0.2.0 names no home: such a job cannot run under this config
    (an older runner fails at its load, this version's refuses it at
    dispatch), so it is not compared; its guard home is the legacy one,
    checked as the previous home.  None when there is no problem."""
    home = str(config.effective_guard_home)
    jobs = []
    for job_id in store.all_ids():
        try:
            st = store.state(job_id)
        except ValueError:
            continue
        if isinstance(st, dict) and st.get("outcome"):
            continue
        man = store.manifest(job_id)
        if not isinstance(man, dict):
            continue                    # rename_problem / C4 cover these
        if man.get("host", config.host) != config.host:
            continue
        theirs = man.get("guard_home")
        if isinstance(theirs, str) and theirs != home:
            jobs.append({"job_id": job_id, "guard_home": theirs})
    old = None if config.test_mode else _legacy_home()
    record = read_json(config.state_dir / GUARD_HOME_RECORD)
    if isinstance(record, dict) and isinstance(record.get("guard_home"),
                                               str):
        old = record["guard_home"]
    holds = []
    if old and old != home:
        old_path = Path(old)
        for name in ("inflight.json", "current.json"):
            try:
                held = exists_strict(old_path / name)
            except OSError:
                held = True             # A49: cannot be examined: held
            if held:
                holds.append(str(old_path / name))
        users = procs.lock_file_users(old_path / "lock")
        if users is None or users:
            holds.append(f"{old_path / 'lock'} users "
                         f"{users if users is not None else 'unknown'}")
    if not jobs and not holds:
        return None
    return {"guard_home": home, "recorded": old, "jobs": jobs,
            "old_home_holds": holds,
            "reason": f"the guard home is {home}, but unfinished jobs "
                      f"{[j['job_id'] for j in jobs]} or the previous home "
                      f"{old} ({holds}) still reference another one: a "
                      "guard home changes only at an idle checkpoint "
                      "(never with a job unfinished or a marker or lock "
                      "holder left in the old home); the operator restores "
                      "the old value until then; never started"}


def record_guard_home(config: Config) -> None:
    """Remember the guard home in use (``<state>/guard-home.json``) so a
    later change of ``backend.guard_home`` can be checked against it."""
    path = config.state_dir / GUARD_HOME_RECORD
    record = read_json(path)
    home = str(config.effective_guard_home)
    if isinstance(record, dict) and record.get("guard_home") == home:
        return
    if isinstance(record, Unreadable):
        return                          # C4: never replaced
    try:
        write_json(path, {"guard_home": home, "utc": utc_now(),
                          "previous": record.get("guard_home")
                          if isinstance(record, dict) else None})
    except OSError:
        pass


# K2 (A24): what a job that began launching leaves in its state.  Any of
# these set (not None) means the workload may have run, whether or not the
# runner got to record its ``workload`` identity before the cancel.
LAUNCH_STATE_KEYS = ("workload", "guard_outcome", "guard_record", "exit_code",
                     "cleanup", "guard_cleanup", "admitted_utc",
                     "execution_wall_s", "cleanup_s", "wall_s",
                     "peak_rss_gb", "container_name_check",
                     "workload_seen_utc", "workload_seen_ticks",
                     "workload_seen_boot_id")
LAUNCH_PHASES = ("launched", "executing", "cleanup")
# The guard outcomes that only a run that never launched can record (its
# initial value, a pre-flight refusal, the lock held by another guard).  Any
# other outcome in a record (running, killed-*, completed, ...) is a launch.
NO_LAUNCH_GUARD_OUTCOMES = (None, "not-started", "refused", "lock-busy")


def _launch_not_excluded(store: JobStore, job_id: str, st: dict) -> str | None:
    """K2 (A24): why a cancelled job may have launched its workload, or None
    when every record positively shows the guard never began launching.
    Shares ``_never_launched``'s rule (no workload identity, no guard record
    pid, no samples.csv in any attempt) and adds what else a launched job
    leaves: launch fields in the state, a launched/executing/cleanup phase in
    the history, a container pid, a start time or an outcome in a guard
    record.  Every record is read errno-safely: anything unreadable, a
    non-directory where a directory belongs or any error other than "does
    not exist" counts as a launch, never as absence (fail closed)."""
    for key in LAUNCH_STATE_KEYS:
        if st.get(key) is not None:
            return f"the state records {key}"
    if st.get("phase") in LAUNCH_PHASES:
        return f"the state is in phase {st.get('phase')}"
    history = st.get("history", [])
    if not isinstance(history, list):
        return "the state history is malformed"
    for item in history:
        if not isinstance(item, dict):
            return "the state history is malformed"
        if item.get("phase") in LAUNCH_PHASES:
            return f"the history shows phase {item.get('phase')}"
    root = store.job_dir(job_id) / "guard"
    try:
        names = sorted(os.listdir(root))
    except FileNotFoundError:
        names = []                      # no attempt was ever made
    except OSError as error:
        return f"the guard directory cannot be read ({error})"
    records = []
    for name in names:
        if not name.startswith("attempt-"):
            continue
        attempt_dir = root / name
        record = read_json(attempt_dir / "guard.json")
        if isinstance(record, Unreadable):
            return record.reason
        if isinstance(record, dict):
            records.append(record)
            if record.get("pid") is not None or \
                    record.get("container_pid") is not None or \
                    record.get("started_utc") is not None or \
                    record.get("exit_code") is not None:
                return f"{name}/guard.json records a launch"
            if record.get("outcome") not in NO_LAUNCH_GUARD_OUTCOMES:
                return (f"{name}/guard.json records the guard outcome "
                        f"{record.get('outcome')!r}")
        try:
            os.lstat(attempt_dir / "samples.csv")
        except FileNotFoundError:
            pass                        # the only proof of absence
        except OSError as error:
            return f"{name}/samples.csv cannot be checked ({error})"
        else:
            return f"{name}/samples.csv exists"
    if _never_launched(store, job_id, records, st) is None:
        return "the shared never-launched evidence is not met"
    return None


def _request_reusable(store: JobStore, job_id: str) -> bool:
    """A request id may be bound again only when its job provably never
    started: a never-started outcome, or (K2, A24) a cancel with positive
    never-launched evidence.  A cancel after the guard began launching but
    before the runner captured the workload identity (a container's
    in-container pid not yet read; a host workload between the guard's
    launch and the first capture poll) leaves no ``workload``, yet the
    workload may have run: the id stays bound."""
    st = store.state(job_id)
    if not isinstance(st, dict):
        return False                # absent or unreadable: proves nothing
    outcome = st.get("outcome")
    if outcome in NEVER_STARTED:
        return True
    if outcome != "cancelled":
        return False
    return _launch_not_excluded(store, job_id, st) is None


def _reserve(store: JobStore, host: str,
             request_id: str | None) -> tuple[str, str | None]:
    """A12: in ONE state-lock critical section, check the request id,
    create the new job's state (phase ``accepted``, no manifest yet) and
    bind the request id to it, before the manifest is built.  Returns
    ``(job_id, None)``, or ``(job_id, existing)`` when the request id
    already maps to a job that may have run, or ``("", Unreadable)`` when
    its binding cannot be read (C4: never bound again), or ``("",
    BoundElsewhere)`` when a placement record binds it to another host.
    """
    with state_lock(store.state_dir):
        if request_id:
            # G3a (D6): a placement record binding it to another host (or
            # unreadable, C4) refuses it, under the same lock as the
            # remote submit's commit (review round 1, note 8)
            try:
                record = pl.read(store.state_dir, request_id)
            except ValueError:
                record = None
            if pl.bound_elsewhere(record, host):
                return "", BoundElsewhere(record)
            existing = store.request_job(request_id)
            if isinstance(existing, Unreadable):
                return "", existing
            if existing and not _request_reusable(store, existing):
                return "", existing
        while True:
            job_id = new_job_id()
            try:
                os.mkdir(store.job_dir(job_id), 0o700)
                break
            except FileExistsError:
                continue
        reason = "submission accepted"
        item = store.history(phase="accepted", reason=reason)
        now = item["utc"]
        state = {"schema": STATE_SCHEMA, "job_id": job_id, "host": host,
                 "request_id": request_id, "phase": "accepted",
                 "accepted_utc": now, "reason": reason,
                 "updated_utc": now, "history": [item]}
        write_json(store.job_dir(job_id) / "state.json", state)
        store.log_event(job_id, state, item)
        if request_id:
            store.bind_request(request_id, job_id)
    return job_id, None


def _release(store: JobStore, job_id: str, request_id: str | None) -> None:
    """Undo a reservation whose manifest could not be built: nothing was
    enqueued or spawned, so the job directory holds only its state."""
    import shutil
    with state_lock(store.state_dir):
        job_dir = store.job_dir(job_id)
        st = store.state(job_id)
        # C4: an unreadable state may hold a claim; the directory stays
        st = st if isinstance(st, dict) else \
            {"runner": "unreadable"} if st is not None else {}
        try:
            has_manifest = exists_strict(job_dir / "manifest.json")
        except OSError:
            has_manifest = True         # A49: it may have one: keep the dir
        if not has_manifest and not st.get("runner"):
            shutil.rmtree(job_dir, ignore_errors=True)
        if request_id and store.request_job(request_id) == job_id:
            store.unbind_request(request_id)


def _unbind(store: JobStore, request_id: str | None, job_id: str) -> None:
    if not request_id:
        return
    with state_lock(store.state_dir):
        if store.request_job(request_id) == job_id:
            store.unbind_request(request_id)


def _mark_queued(store: JobStore, job_id: str, task_id: int) -> bool:
    """Record the queue task, unless the job already has an outcome."""
    with state_lock(store.state_dir):
        path = store.job_dir(job_id) / "state.json"
        st = read_json(path)
        if isinstance(st, Unreadable):
            raise RecordUnreadable(st)      # C4: never overwritten
        st = st or {}
        if st.get("outcome"):
            return False
        now = utc_now()
        st["pueue_task"] = task_id
        item = None
        if st.get("phase") == "accepted":       # a fast runner may be ahead
            reason = "in the shared heavy queue"
            item = store.history(phase="queued", reason=reason)
            st.setdefault("history", []).append(item)
            st.update(phase="queued", reason=reason)
        st["updated_utc"] = now
        write_json(path, st)
        if item is not None:
            store.log_event(job_id, st, item)
        return True


def spawn_detached_runner(config: Config, job_id: str) -> None:
    entry = Path(__file__).resolve().parent.parent.parent / "bin" / \
        "agent-compute"
    subprocess.Popen([config.python, str(entry), "--config", str(config.path),
                      "_exec", job_id], start_new_session=True,
                     stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                     stderr=subprocess.DEVNULL, env=q.scrubbed_env(),
                     cwd="/")


def evidence_paths(config: Config, job_id: str) -> dict:
    job_dir = JobStore(config.state_dir).job_dir(job_id)
    return {"manifest": str(job_dir / "manifest.json"),
            "state": str(job_dir / "state.json"),
            "log": str(job_dir / "output.log"),
            "guard_records": str(job_dir / "guard")}


# --- status / wait / cancel -------------------------------------------------------

def settle_unclaimed(config: Config, store: JobStore,
                     job_ids: list[str] | None = None,
                     tasks: dict | None = None,
                     budget_s: float | None = None) -> list[str]:
    """A3/A12: finish, as never started, every unfinished job that no
    runner ever claimed and that nothing can dispatch any more:

    * its Pueue task has ended (``Done``: FailedToSpawn, Failed, Killed...)
      or is missing from the running daemon -> ``launch-failed``;
    * it has no queue task at all and was accepted more than
      UNCLAIMED_GRACE_S ago (its submitting client died between reserving
      the job and enqueueing it, or its detached runner never started) ->
      ``refused``.

    Sound because a runner's claim (``Runner.claim``, under the state lock)
    is the only way to the guard, precedes every guard call, and refuses a
    job that has an outcome; this finish is a compare-and-set under the
    same lock that refuses a claimed job.  So either the claim came first
    (the job is left alone) or the finish did (no runner can ever start
    it).  The task-state and grace conditions only avoid finishing a job
    that could still legitimately be dispatched; they are not what makes
    it safe.  ``tasks`` injects the Pueue task table (tests); otherwise it
    is read once, only when needed, and only from a running daemon, within
    ``budget_s`` seconds when given (``wait --timeout``)."""
    call_s = None if budget_s is None else max(0.5, min(30.0, budget_s))
    settled = []
    for job_id in (store.all_ids() if job_ids is None else job_ids):
        try:
            st = store.state(job_id)
            man = store.manifest(job_id)
        except ValueError:
            continue
        if not isinstance(st, dict) or st.get("outcome") or st.get("runner"):
            continue
        host = man.get("host") if isinstance(man, dict) else st.get("host")
        if host is not None and host != config.host:
            continue
        task = st.get("pueue_task")
        if task is None:
            try:
                age = _seconds_between(st["accepted_utc"], utc_now())
            except (KeyError, TypeError, ValueError):
                continue
            if age < UNCLAIMED_GRACE_S:
                continue
            outcome = "refused"
            reason = (f"accepted {age:.0f} s ago but never enqueued nor "
                      "claimed by a runner (the submitting client stopped "
                      "before enqueueing it, or its runner never started); "
                      "never started")
        else:
            if not config.queue_enabled:
                continue
            if tasks is None:
                try:
                    if not q.daemon_running(config, timeout=call_s):
                        continue
                    tasks = q.status_json(config, timeout=call_s).get(
                        "tasks", {})
                except (q.QueueError, OSError, subprocess.SubprocessError,
                        ValueError):
                    continue
            raw = tasks.get(str(task))
            name = "Missing" if raw is None else \
                q._status_name(raw.get("status"))
            if name != "Missing" and name not in q.TERMINAL_TASK_STATES:
                continue
            detail = name
            if raw is not None:
                result = (raw.get("status") or {}).get("Done", {}) \
                    if isinstance(raw.get("status"), dict) else {}
                result = result.get("result") if isinstance(result, dict) \
                    else None
                detail = f"{name}: {json.dumps(result)[:200]}"
            outcome = "launch-failed"
            reason = (f"its queue task {task} ended ({detail}) before any "
                      "runner claimed the job; never started")
        if _finish_if_unclaimed(store, job_id, outcome, reason):
            settled.append(job_id)
    return settled


def detect_lost(config: Config) -> list[str]:
    """Non-terminal jobs whose runner is gone -> durable block.  Also
    settles never-claimed jobs that nothing can dispatch (A3/A12)."""
    store = JobStore(config.state_dir)
    settle_unclaimed(config, store)
    lost = []
    for job_id in store.all_ids():
        st = store.state(job_id)
        man = store.manifest(job_id)
        if isinstance(st, Unreadable):
            # C4: an unreadable state may hide a lost runner: reported and
            # blocked (re-read under the state lock), never skipped
            if isinstance(man, dict) and man.get("host") != config.host:
                continue
            blocked = _blocked_jobs(store, config.host)
            if (blocked is not None and job_id in blocked) or \
                    store.block_if_unreadable(
                        config.host, job_id,
                        f"the job's state cannot be read ({st.reason}): it "
                        "may hide a live or lost runner; an operator "
                        "inspects it", {"detected_by": "detect_lost"}):
                lost.append(job_id)
                q.pause_group_quietly(config)
            continue
        st = st or {}
        # an unreadable manifest cannot prove the job another host's
        host = (st.get("host") or config.host) \
            if isinstance(man, Unreadable) else (man or {}).get("host")
        if st.get("outcome") or host != config.host:
            continue
        runner = st.get("runner")
        if runner and procs.liveness(runner) == procs.GONE:
            blocked = _blocked_jobs(store, config.host)
            if blocked is None or job_id in blocked:
                # in the block, or the block cannot be read (and is never
                # replaced): the host is blocked either way
                lost.append(job_id)
            elif store.block_if_unfinished(
                    config.host, job_id, runner,
                    "runner gone while the job was unfinished "
                    "(lost supervision)",
                    {"runner": runner, "phase": st.get("phase"),
                     "workload": st.get("workload"),
                     "workload_seen": _workload_seen(st, {})}):
                # re-read under the state lock: a runner that finished (or
                # was replaced) during the check is not lost (A9)
                lost.append(job_id)
                q.pause_group_quietly(config)
    return lost


def queue_snapshot(config: Config, store: JobStore) -> list[dict]:
    """The unfinished jobs of this host, for queue positions: claimed ones
    (dispatched, waiting for admission, running) and queued ones with
    their queue task number (Pueue numbers tasks in submission order)."""
    jobs = []
    for job_id in store.all_ids():
        try:
            st = store.state(job_id)
        except ValueError:
            continue
        if not isinstance(st, dict) or st.get("outcome"):
            continue
        man = store.manifest(job_id)
        man = man if isinstance(man, dict) else {}
        if man.get("host", config.host) != config.host:
            continue
        jobs.append({"id": job_id, "project": man.get("project"),
                     "phase": st.get("phase"),
                     "timeout_min": (man.get("resources") or {}).get(
                         "timeout_min"),
                     "task": st.get("pueue_task"),
                     "claimed": bool(st.get("runner")),
                     "accepted_utc": st.get("accepted_utc"),
                     "dispatched_utc": st.get("dispatched_utc")})
    return jobs


def queue_place(job_id: str, snapshot: list[dict]) -> tuple[int | None,
                                                            dict | None]:
    """A queued job's position among the queued jobs (1: dispatched next)
    and the job ahead of it: the queued job just before it, else the job
    holding the slot.  The job ahead is named under ``id`` (never a second
    ``job_id`` key in a view)."""
    me = next((j for j in snapshot if j["id"] == job_id), None)
    if me is None or me["claimed"] or me["task"] is None:
        return None, None
    queued = sorted((j for j in snapshot if not j["claimed"] and
                     isinstance(j["task"], int)), key=lambda j: j["task"])
    before = [j for j in queued if j["task"] < me["task"]]
    position = len(before) + 1
    if before:
        ahead = before[-1]
    else:
        running = sorted((j for j in snapshot if j["claimed"]),
                         key=lambda j: j["dispatched_utc"] or "")
        ahead = running[0] if running else None
    if ahead is None:
        return position, None
    since = ahead["dispatched_utc"] if ahead["claimed"] else \
        ahead["accepted_utc"]
    elapsed = None
    if isinstance(since, str):
        try:
            elapsed = _seconds_between(since, utc_now())
        except ValueError:
            elapsed = None
    return position, {"id": ahead["id"], "project": ahead["project"],
                      "phase": ahead["phase"],
                      "timeout_min": ahead["timeout_min"],
                      "elapsed_s": elapsed}


def _elapsed(a, b):
    if not isinstance(a, str) or not isinstance(b, str):
        return None
    try:
        return _seconds_between(a, b)
    except ValueError:
        return None


def job_view(config: Config, job_id: str, pueue_tasks: dict | None,
             snapshot: list[dict] | None = None) -> dict:
    store = JobStore(config.state_dir)
    man = store.manifest(job_id)
    st = store.state(job_id)
    unreadable = [r.view() for r in (man, st) if isinstance(r, Unreadable)]
    man = man if isinstance(man, dict) else {}
    st = st if isinstance(st, dict) else {}
    view = {"job_id": job_id, "project": man.get("project"),
            "host": man.get("host"), "case": man.get("case"),
            "request_id": man.get("request_id"), "owner": man.get("owner"),
            "submitter": man.get("submitter"),
            "acs_version": man.get("acs_version"),
            "acs_source_sha": man.get("acs_source_sha"),
            "phase": st.get("phase"), "outcome": st.get("outcome"),
            "reason": st.get("reason"), "wait_reason": st.get("wait_reason"),
            "exit_code": st.get("exit_code"),
            # structured workload result (A19): consumers must not parse
            # `reason`; null when the guard never launched the workload
            "guard_outcome": st.get("guard_outcome"),
            "peak_rss_gb": st.get("peak_rss_gb"),
            "cleanup": _cleanup_summary(st.get("cleanup")),
            "accepted_utc": st.get("accepted_utc"),
            "dispatched_utc": st.get("dispatched_utc"),
            "admitted_utc": st.get("admitted_utc"),
            "ended_utc": st.get("ended_utc"),
            "execution_wall_s": st.get("wall_s"),
            # G2a: always present, null until computable (accepted ->
            # admitted, or -> ended for a job that never launched)
            "queue_wait_s": None,
            "dispatch_wait_s": _elapsed(st.get("accepted_utc"),
                                        st.get("dispatched_utc")),
            "admission_wait_s": _elapsed(st.get("dispatched_utc"),
                                         st.get("admitted_utc")),
            "queue_position": None, "job_ahead": None,
            # S5a: {probe_s} for a probe, else null; the job it was
            # submitted from inside of, else null
            "probe": ({"probe_s": (man.get("resources") or {}).get("probe_s")}
                      if (man.get("resources") or {}).get("probe") else None),
            "parent_job": man.get("parent_job"),
            # the recorded workload identity; null until captured
            "workload": st.get("workload") or None,
            # G3b: the code requirement and checkout HEAD at submit; the
            # placement that chose this host (its policy snapshot), when
            # this host's own policy placed it here
            "code": man.get("code"),
            "placement": man.get("placement"),
            "evidence": evidence_paths(config, job_id)}
    if st.get("accepted_utc") and (st.get("admitted_utc") or
                                   st.get("ended_utc")):
        view["queue_wait_s"] = _elapsed(
            st["accepted_utc"], st.get("admitted_utc") or st["ended_utc"])
    task = st.get("pueue_task")
    if task is not None and pueue_tasks is not None:
        raw = pueue_tasks.get(str(task), {}).get("status")
        view["queue_task"] = {"id": task, "status": (
            raw if isinstance(raw, str) else next(iter(raw or {"Missing": 0})))}
    if not st.get("outcome") and not st.get("runner") and task is not None:
        if snapshot is None:
            snapshot = queue_snapshot(config, store)
        view["queue_position"], view["job_ahead"] = queue_place(job_id,
                                                                snapshot)
    if st.get("guard_record"):
        view["evidence"]["guard_record"] = st["guard_record"]
    # G3a (executor corpus §4.2 item 9): the memory decision of the last
    # guard attempt that recorded one, so agents stop reading guard.json
    view["admission_memory"] = last_admission_memory(store, job_id)
    if unreadable:
        view["unreadable"] = unreadable         # C4: shown, never skipped
    return view


ADMISSION_MEMORY_KEYS = ("host_available_gb", "floor_gb", "estimate_gb",
                         "needed_gb", "vm_credit_gb")


def last_admission_memory(store: JobStore, job_id: str) -> dict | None:
    """``admission_memory`` of the newest attempt's ``guard.json`` that
    has one (with the attempt number, the path and the attempt's
    ``refusal``); None when no attempt recorded one.  Read-only."""
    root = store.job_dir(job_id) / "guard"
    try:
        attempts = sorted((p for p in root.glob("attempt-*")
                           if p.name.split("-")[1].isdigit()),
                          key=lambda p: int(p.name.split("-")[1]))
    except OSError:
        return None
    for attempt in reversed(attempts):
        record = read_json(attempt / "guard.json")
        if not isinstance(record, dict):
            continue
        memory = record.get("admission_memory")
        if isinstance(memory, dict):
            return {**{k: memory.get(k) for k in ADMISSION_MEMORY_KEYS},
                    "attempt": int(attempt.name.split("-")[1]),
                    "refusal": record.get("refusal"),
                    "path": str(attempt / "guard.json")}
    return None


def _cleanup_summary(cleanup) -> dict | None:
    """The cleanup verdict without the raw process evidence (that stays in
    state.json); null when no cleanup check ran."""
    if not isinstance(cleanup, dict):
        return None
    return {key: cleanup.get(key)
            for key in ("verified", "clean", "method", "reason")}


def _seconds_between(a: str, b: str) -> float:
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    import datetime as dt
    return (dt.datetime.strptime(b, fmt) -
            dt.datetime.strptime(a, fmt)).total_seconds()


def dispatch_state(config: Config, store: JobStore, groups: dict | None,
                   daemon: bool | None) -> dict:
    """Whether queued work can be dispatched now, and if not, why and what
    lifts it (G2a: the dispatch gate's reason, a paused queue group, a
    block; ``status`` and ``doctor`` both show this one answer)."""
    gate, block = store.gate(), store.block(config.host)
    if isinstance(gate, Unreadable):
        return {"held": True, "reason": f"the dispatch gate record "
                f"{gate.reason}; an operator inspects it"}
    if gate:
        return {"held": True, "reason": f"dispatch gate closed: "
                f"{gate.get('reason')}; `agent-compute resume` after "
                "reviewing `agent-compute status`"}
    if block:
        return {"held": True, "reason": "host admission is blocked: "
                "`agent-compute reconcile`, then `agent-compute resume`"}
    if not config.queue_enabled:
        return {"held": False, "reason": "queue disabled on this host: each "
                "job's detached runner waits for admission"}
    if daemon is None:
        return {"held": None, "reason": "the queue's state is not read here; "
                "`agent-compute status` reads it"}
    if not daemon:
        pending = q._pending_work(config)
        if pending:
            return {"held": True, "reason": f"the queue daemon is not "
                    f"running and {len(pending)} queued task(s) wait: "
                    "`agent-compute resume` (after reviewing status) "
                    "starts it and reopens dispatch"}
        return {"held": False, "reason": "queue daemon not running; nothing "
                "queued (started on demand by submit/resume)"}
    group = (groups or {}).get(config.queue_group) or {}
    if group.get("status") == "Paused":
        return {"held": True, "reason": f"queue group {config.queue_group} "
                "is paused: `agent-compute resume` needed (after reviewing "
                "`agent-compute status`)"}
    return {"held": False, "reason": "dispatching"}


def admission_view(config: Config, store: JobStore, lost: list,
                   groups: dict | None, daemon: bool | None) -> dict:
    holder = read_json(config.effective_guard_home / "current.json")
    holder = record_view(holder)
    if isinstance(holder, dict) and "unreadable" not in holder:
        holder = {k: holder.get(k) for k in ("job_id", "pid", "venue",
                                             "started_utc")}
        try:
            holder["alive"] = procs.start_info(holder.get("pid")) is not None
        except procs.ProcCheckError:
            holder["alive"] = None
    return {"blocked": record_view(store.block(config.host)),
            "dispatch_gate": record_view(store.gate()),
            # G2a: whether dispatch is held and why (gate, group, block)
            "dispatch": dispatch_state(config, store, groups, daemon),
            "compute_lock_holder": holder,
            # the internal guard's in-flight marker (read-only; S3/S4)
            "inflight_marker": inflight_view(config),
            "lost_supervision": lost}


def _queue_state(config: Config):
    tasks, groups, daemon = None, None, None
    if config.queue_enabled:
        daemon = q.daemon_running(config)
        if daemon:
            data = q.status_json(config)
            tasks, groups = data.get("tasks", {}), data.get("groups", {})
    return tasks, groups, daemon


def job_report(config: Config, job_id: str, lost: list | None = None,
               tasks=None, groups=None, daemon=None) -> dict:
    """G2a: the one shape of ``status JOB --json``, ``wait --json`` and
    ``cancel --json`` on a finished job: the job view at the top level,
    ``admission`` and the diagnostic identity (``acs``) under their own
    keys, and the view again under ``job`` (the v1 shape's key)."""
    store = JobStore(config.state_dir)
    view = job_view(config, job_id, tasks)
    return {**view, "job": view,
            "admission": admission_view(config, store, lost or [], groups,
                                        daemon),
            "acs": acs_identity(config)}


def _mine(view: dict, who: str) -> bool:
    return view.get("owner") == who or \
        (isinstance(view.get("request_id"), str) and
         view["request_id"].startswith(who))


def cmd_status(config: Config, args) -> int:
    request_id = getattr(args, "request_id", None)
    if request_id is not None:
        if args.job:
            print("agent-compute: give a job id or --request-id, not both",
                  file=sys.stderr)
            return EXIT_NOT_ACCEPTED
        if not REQUEST_ID.fullmatch(request_id):
            print(f"agent-compute: request id {request_id!r} is not valid",
                  file=sys.stderr)
            return EXIT_NOT_ACCEPTED
    store = JobStore(config.state_dir)
    lost = detect_lost(config)
    tasks, groups, daemon = _queue_state(config)
    if request_id is not None:
        out(request_report(config, request_id, lost, tasks, groups, daemon),
            args.json)
        return 0
    if args.job:
        report = job_report(config, args.job, lost, tasks, groups, daemon)
        if args.json:
            out(report, True)
        else:
            line = held_line(report)
            if line:
                print(line)
            out(report["job"], False)
        return 0
    snapshot = queue_snapshot(config, store)
    jobs = []
    for job_id in store.all_ids():
        view = job_view(config, job_id, tasks, snapshot)
        if getattr(args, "unfinished", False):
            keep = not view["outcome"]
        else:
            keep = args.all or not view["outcome"] or \
                view["outcome"] == "unknown"
        if keep and getattr(args, "mine", None) and \
                not _mine(view, args.mine):
            keep = False
        if keep:
            jobs.append(view)
    report = {
        "host": config.host,
        "acs": acs_identity(config),
        "admission": admission_view(config, store, lost, groups, daemon),
        "queue": ({"enabled": True, "daemon_running": daemon,
                   "group": config.queue_group,
                   "group_state": (groups or {}).get(config.queue_group)}
                  if config.queue_enabled else {"enabled": False}),
        "jobs": jobs,
        # G3a: this host's placement records (jobs sent to other hosts):
        # every one with --all, else the open ones
        "placements": placement_listing(config, bool(args.all)),
    }
    if not args.json:
        for view in jobs:
            line = held_line(view)
            if line:
                print(line)
    out(report, args.json)
    return 0


def inflight_view(config: Config) -> dict | None:
    """The internal guard's in-flight marker, read-only (None: absent)."""
    from .runner import read_inflight
    marker = read_inflight(config.effective_guard_home)
    if marker is None:
        return None
    keys = ("job_id", "acs_job", "phase", "venue", "pid", "container",
            "container_pid", "guard_pid", "written_utc", "survivors",
            "malformed")
    view = {k: marker.get(k) for k in keys if k in marker}
    view["path"] = str(config.effective_guard_home / "inflight.json")
    return view


def _print_job(config: Config, report: dict, as_json: bool) -> None:
    if as_json:
        out(report, True)
        return
    line = held_line(report)
    if line:
        print(line)
    view = dict(report["job"])
    if "note" in report:
        view["note"] = report["note"]
    out(view, False)


def cmd_wait(config: Config, args) -> int:
    # G2a: the deadline counts from the start and bounds every step (a
    # queue daemon that does not answer cannot stretch it)
    deadline = time.monotonic() + args.timeout if args.timeout else None

    def remaining() -> float | None:
        return None if deadline is None else deadline - time.monotonic()

    store = JobStore(config.state_dir)
    if store.manifest(args.job) is None:
        print(f"agent-compute: unknown job {args.job}", file=sys.stderr)
        return EXIT_NOT_ACCEPTED
    delay = 1.0
    last_settle = 0.0
    while True:
        st = store.state(args.job)
        if isinstance(st, Unreadable):
            # C4: no outcome can be read; the host is blocked for it
            detect_lost(config)
            report = job_report(config, args.job)
            report["note"] = (f"the job state {st.reason}: its end cannot be "
                              "read; `agent-compute reconcile` and an "
                              "operator resolve it")
            _print_job(config, report, args.json)
            return 18
        st = st or {}
        left = remaining()
        if not st.get("outcome") and not st.get("runner") and \
                time.monotonic() - last_settle >= 30.0 and \
                (left is None or left > 0):
            # A3/A12, at most every 30 s and within the wait's budget
            last_settle = time.monotonic()
            if settle_unclaimed(config, store, [args.job],
                                budget_s=left):
                st = store.state(args.job)
                st = st if isinstance(st, dict) else {}
        if st.get("outcome"):
            _print_job(config, job_report(config, args.job), args.json)
            from .runner import RC
            return RC.get(st["outcome"], 1)
        if st.get("runner") and \
                procs.liveness(st["runner"]) == procs.GONE and \
                not _outcome(store, args.job):      # re-read after (A9)
            detect_lost(config)
            blocked = _blocked_jobs(store, config.host)
            if blocked is None or args.job in blocked:
                report = job_report(config, args.job)
                report["note"] = ("runner gone before a terminal outcome: "
                                  "host blocked; run `agent-compute "
                                  "reconcile`")
                _print_job(config, report, args.json)
                return 18
            # not blocked: it finished or was re-claimed during the check
            # (A9); look again on the next poll
        left = remaining()
        if left is not None and left <= 0:
            report = job_report(config, args.job)
            report["note"] = "wait timed out; the job continues"
            _print_job(config, report, args.json)
            return EXIT_WAIT_TIMEOUT
        time.sleep(min(delay, max(0.1, left if left is not None else delay)))
        delay = min(delay * 1.5, 15.0)


def _outcome(store: JobStore, job_id: str) -> str | None:
    """The job's outcome; None when it has none or its state cannot be read
    (C4: then nothing proves it ended)."""
    st = store.state(job_id)
    return st.get("outcome") if isinstance(st, dict) else None


def _finish_if_unclaimed(store: JobStore, job_id: str, outcome: str,
                         reason: str) -> bool:
    """Compare-and-set: finish a job only if no runner ever claimed it.  An
    unreadable state proves nothing and is never overwritten (C4)."""
    with state_lock(store.state_dir):
        st = store.state(job_id)
        if isinstance(st, Unreadable):
            return False
        st = st or {}
        if st.get("runner") or st.get("outcome"):
            return False
        item = store.history(outcome=outcome, reason=reason)
        st.setdefault("history", []).append(item)
        st.update(phase="done", outcome=outcome, reason=reason,
                  ended_utc=utc_now(), updated_utc=utc_now(),
                  wait_reason=None)
        write_json(store.job_dir(job_id) / "state.json", st)
        store.log_event(job_id, st, item)
        return True


def cancel_job(config: Config, job_id: str, wait_s: float = 120,
               outcome: str = "cancelled",
               reason: str = "cancelled by request") -> dict:
    store = JobStore(config.state_dir)
    st = store.state(job_id)
    if st is None:
        return {"cancelled": False, "reason": f"unknown job {job_id}"}
    if isinstance(st, Unreadable):
        return {"cancelled": False, "job_id": job_id,
                "reason": f"the job state {st.reason}; nothing signalled; "
                          "an operator inspects it"}
    if st.get("outcome"):
        # G2a: the finished job's view, in the shape of `status JOB`
        return {**job_report(config, job_id), "cancelled": False,
                "note": f"already {st['outcome']}; nothing to cancel"}
    job_dir = store.job_dir(job_id)
    write_json(job_dir / "cancel-requested",
               {"utc": utc_now(), "outcome": outcome, "reason": reason})
    task = st.get("pueue_task")
    if config.queue_enabled and task is not None and not st.get("runner"):
        try:
            status, _ = q.task_status(config, task)
        except (q.QueueError, OSError, subprocess.SubprocessError, ValueError):
            status = "Unknown"
        # `pueue remove` also succeeds on finished tasks, so only a task
        # seen Queued is removed; the CAS then refuses to overwrite a runner
        # that claimed the job in between.
        if status in ("Queued", "Stashed") and q.remove_queued(config, task):
            if _finish_if_unclaimed(store, job_id, outcome,
                                    f"{reason}: removed from the queue before "
                                    "it started; never started"):
                return {"cancelled": True, "job_id": job_id,
                        "outcome": outcome,
                        "reason": "removed from the queue; never started"}
        elif status == "Missing" or status in q.TERMINAL_TASK_STATES:
            # A3: the task ended (e.g. FailedToSpawn) or vanished without a
            # runner claim; the CAS refuses a runner that claimed meanwhile
            if _finish_if_unclaimed(store, job_id, outcome,
                                    f"{reason}: its queue task had ended "
                                    f"({status}) without any runner claim; "
                                    "never started"):
                return {"cancelled": True, "job_id": job_id,
                        "outcome": outcome,
                        "reason": "its queue task had ended without a "
                                  "runner claim; never started"}
    elif config.queue_enabled and task is None and not st.get("runner"):
        # A12: accepted but not (yet) enqueued.  Finishing it now is safe
        # whatever the submitter does next: `accept` never enqueues a
        # finished job, and a runner's claim refuses one.
        if _finish_if_unclaimed(store, job_id, outcome,
                                f"{reason}: not yet enqueued; never "
                                "started"):
            return {"cancelled": True, "job_id": job_id, "outcome": outcome,
                    "reason": "not yet enqueued; never started"}
    deadline = time.time() + wait_s
    signalled = False
    while time.time() < deadline:
        st = store.state(job_id)
        if isinstance(st, Unreadable):
            time.sleep(0.5)             # C4: nothing is decided on it
            continue
        st = st or {}
        if st.get("outcome"):
            return {"cancelled": st["outcome"] in ("cancelled", outcome),
                    "job_id": job_id, "outcome": st["outcome"],
                    "reason": st.get("reason")}
        runner = st.get("runner")
        if runner and not signalled and \
                procs.liveness(runner) == procs.ALIVE:
            os.kill(int(runner["pid"]), signal.SIGTERM)
            signalled = True
        if not runner and not config.queue_enabled and \
                time.time() > deadline - wait_s + 10:
            # a detached runner that never claimed the job within 10 s
            if _finish_if_unclaimed(store, job_id, outcome,
                                    f"{reason}: no runner had claimed the "
                                    "job; never started"):
                continue
        time.sleep(0.5)
    return {"cancelled": False, "job_id": job_id,
            "reason": "cancellation requested; not finished yet — "
                      "`agent-compute wait` it"}


def cmd_cancel(config: Config, args) -> int:
    result = cancel_job(config, args.job)
    out(result, args.json)
    return 0 if result.get("cancelled") else 1


def cmd_run(config: Config, args, argv: list[str]) -> int:
    args.lock_wait = args.lock_wait or args.admission_timeout
    code, result = accept(config, args, argv)
    if code:
        out(result, args.json)
        return code
    job_id = result["job_id"]
    print(f"agent-compute: accepted {job_id}", file=sys.stderr)
    return _run_wait(config, args, job_id)


def _run_wait(config: Config, args, job_id: str) -> int:
    """``run`` after its acceptance: the bounded admission wait, then
    ``wait``."""
    store = JobStore(config.state_dir)
    deadline = time.time() + args.admission_timeout
    try:
        while True:
            st = store.state(job_id)
            if isinstance(st, Unreadable):
                break                   # cmd_wait reports it
            st = st or {}
            if st.get("outcome") or st.get("phase") in ("launched",
                                                         "executing",
                                                         "cleanup"):
                break
            if time.time() > deadline:
                cancel_job(config, job_id, outcome="admission-timeout",
                           reason=f"`run` admission wait of "
                                  f"{args.admission_timeout:g} s expired")
                break
            time.sleep(1.0)
        return cmd_wait(config, argparse.Namespace(job=job_id, timeout=None,
                                                   json=args.json))
    except KeyboardInterrupt:
        print(f"agent-compute: detached from {job_id}; it continues. "
              f"`agent-compute cancel {job_id}` to stop it.", file=sys.stderr)
        return 130


# --- reconcile / resume -----------------------------------------------------------

def cmd_reconcile(config: Config, args) -> int:
    store = JobStore(config.state_dir, actor="reconcile")
    detect_lost(config)
    block = store.block(config.host)
    report = {"host": config.host, "acs": acs_identity(config),
              "resolved": [], "unresolved": []}
    jobs = block.get("jobs") if isinstance(block, dict) else None
    if isinstance(block, Unreadable):
        # C4: never "host unblocked"; the file is left for the operator, who
        # may need the jobs it held: list the candidates
        report["unresolved"].append(
            {"job_id": None, "unreadable": block.view(),
             "blocker": f"the host block record {block.reason}; an operator "
                        f"inspects and repairs {block.path} by hand (it is "
                        "never replaced). Jobs of this host that ended "
                        "`unknown` without a reconcile record may have been "
                        "in it: " + (", ".join(_unreconciled_unknown(
                            store, config.host)) or "none")})
        jobs = {}
    for job_id, entry in dict(jobs or {}).items():
        entry = entry if isinstance(entry, dict) else {}
        facts, context = _reconcile_job(store, job_id, entry, args.terminate)
        if "blocker" in facts:
            report["unresolved"].append(facts)
            continue
        _record_resolution(store, job_id, entry, facts["cleanup"], context)
        store.clear_block_job(config.host, job_id)
        report["resolved"].append(facts)
    marker = reconcile_inflight(config, store)
    if marker is not None:
        report["inflight_marker"] = marker
    report["blocked"] = store.block(config.host) is not None or \
        bool(marker and not marker.get("resolved"))     # Unreadable: blocked
    if not report["blocked"]:
        report["next"] = ("host unblocked; `agent-compute resume` re-enables "
                          "dispatch" if config.queue_enabled else
                          "host unblocked")
    out(report, args.json)
    return 0 if not report["blocked"] else 1


def _unreconciled_unknown(store: JobStore, host: str) -> list[str]:
    """Jobs of ``host`` that ended ``unknown`` with no reconcile record."""
    found = []
    for job_id in store.all_ids():
        st, man = store.state(job_id), store.manifest(job_id)
        if isinstance(st, dict) and st.get("outcome") == "unknown" and \
                not st.get("reconciled") and \
                (man.get("host") if isinstance(man, dict) else host) == host:
            found.append(job_id)
    return found


def _marker_job(store: JobStore, config: Config, marker: dict) -> str | None:
    """The ACS job a marker was written for: its ``acs_job`` (S4 guards),
    else the job on this host whose recorded guard job id is the marker's."""
    acs_job = marker.get("acs_job")
    if isinstance(acs_job, str) and acs_job:
        return acs_job
    guard_job = marker.get("job_id")
    if not isinstance(guard_job, str) or not guard_job:
        return None
    found = []
    for job_id in store.all_ids():
        st = store.state(job_id)
        man = store.manifest(job_id)
        if isinstance(st, Unreadable) or isinstance(man, Unreadable):
            return None         # C4: the mapping cannot be proven unique
        st, man = st or {}, man or {}
        if man.get("host") != config.host:
            continue
        workload = st.get("workload") if isinstance(st.get("workload"),
                                                    dict) else {}
        try:
            records = _guard_records(store, job_id)
        except OSError:
            return None         # A49: the mapping cannot be proven unique
        if workload.get("guard_job_id") == guard_job or any(
                isinstance(r, dict) and r.get("job_id") == guard_job
                for r in records):
            found.append(job_id)
    return found[0] if len(found) == 1 else None


def reconcile_inflight(config: Config, store: JobStore) -> dict | None:
    """R3: resolve the internal guard's in-flight marker on ACS evidence.

    Removes ONLY ``<guard home>/inflight.json``, and only when
    * the marker maps to an ACS job of this host (``acs_job``, or a unique
      job whose guard job id is the marker's),
    * that job is not in the host block (resolved first, on evidence), has
      an outcome, and its runner and guard are gone,
    * a fresh check with ACS's own per-venue evidence (the one `reconcile`
      uses for blocks: ``procs.host_survivors`` /
      ``procs.container_job_evidence`` / never launched) proves its
      processes gone, and the job's last guard record is the marker's,
    * ACS's own admission lock is free (no ACS runner is between admission
      and outcome, so no guard can be writing a marker), held while the
      marker is re-read, compared and removed.
    The lock file (the guard home's ``lock``) is never opened or touched.
    Anything else leaves the marker (and so the host's one heavy slot)
    held, with the reason.  A durable history entry is written on the
    job's state."""
    import fcntl
    from .runner import read_inflight
    home = config.effective_guard_home
    marker = read_inflight(home)
    if marker is None:
        return None
    view = inflight_view(config)
    result = {"marker": view, "resolved": False}
    if "malformed" in marker:
        result["blocker"] = ("the in-flight marker is unreadable or malformed;"
                             " an operator inspects the host and removes "
                             f"{view['path']} by hand")
        return result
    job_id = _marker_job(store, config, marker)
    if job_id is None:
        result["blocker"] = (
            "the marker names no ACS job of this host (a direct internal-"
            "guard run, or an ambiguous guard job id): ACS has no evidence "
            "for it.  An operator checks that no process of guard job "
            f"{marker.get('job_id')} remains (host: its process group, or "
            f"processes with {' or '.join(config.markers)} in their "
            "environment; container: "
            f"the same inside {marker.get('container')}) and then removes "
            f"{view['path']} by hand")
        return result
    result["job_id"] = job_id
    blocked = _blocked_jobs(store, config.host)
    if blocked is None:
        result["blocker"] = ("the host block record cannot be read; resolve "
                             "it first (see `unresolved`)")
        return result
    if job_id in blocked:
        result["blocker"] = (f"its ACS job {job_id} is still blocked; resolve "
                             "it first (see `unresolved`)")
        return result
    try:
        st = store.state(job_id)
        man = store.manifest(job_id)
    except ValueError:
        st = man = None
    if isinstance(man, dict) and man.get("host") != config.host:
        result["blocker"] = (f"ACS job {job_id} is not a job of this host "
                             f"({man.get('host')})")
        return result
    if not isinstance(st, dict) or not st.get("outcome"):
        result["blocker"] = (f"its ACS job {job_id} has no outcome yet (it "
                             "may be running); wait for it")
        return result
    facts, context = _reconcile_job(store, job_id, {}, None)
    result["evidence"] = facts.get("cleanup")
    if "blocker" in facts:
        result["blocker"] = f"ACS job {job_id}: {facts['blocker']}"
        return result
    record = context.get("record")
    guard_job = marker.get("job_id")
    if isinstance(record, dict) and record.get("job_id") and \
            record.get("job_id") != guard_job:
        result["blocker"] = (f"the marker's guard job {guard_job} is not the "
                             f"last guard record of ACS job {job_id} "
                             f"({record.get('job_id')}); inspect by hand")
        return result
    if (facts.get("cleanup") or {}).get("clean") is not True:
        result["blocker"] = (f"ACS job {job_id}'s processes are not proven "
                             "gone")
        return result
    fd = os.open(config.state_dir / "admission.lock", os.O_RDWR | os.O_CREAT,
                 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            result["blocker"] = ("an ACS job is being admitted, running or "
                                 "cleaned up on this host; reconcile again "
                                 "after it")
            return result
        if read_inflight(home) != marker:
            result["blocker"] = "the marker changed meanwhile; reconcile again"
            return result
        _marker_history(store, job_id, st, {
            "event": "inflight-marker-cleared", "marker": marker,
            "evidence": facts["cleanup"], "path": view["path"]})
        try:
            (home / "inflight.json").unlink()
        except FileNotFoundError:
            pass
        except OSError as error:
            _marker_history(store, job_id, st, {
                "event": "inflight-marker-clear-failed",
                "error": str(error), "path": view["path"]})
            result["blocker"] = f"could not remove {view['path']}: {error}"
            return result
    finally:
        os.close(fd)
    result.update(resolved=True, evidence=facts["cleanup"])
    return result


def _marker_history(store: JobStore, job_id: str, fallback: dict,
                    item: dict) -> None:
    entry = store.history(**item)
    now = entry["utc"]
    with state_lock(store.state_dir):
        path = store.job_dir(job_id) / "state.json"
        current = read_json(path)
        if isinstance(current, Unreadable):
            return                      # C4: never overwritten
        current = current if isinstance(current, dict) else dict(fallback)
        history = current.get("history")
        history = history if isinstance(history, list) else []
        history.append(entry)
        current["history"] = history
        current["updated_utc"] = now
        write_json(path, current)
        store.log_event(job_id, current, entry)


def _reconcile_job(store: JobStore, job_id, entry: dict,
                   terminate_job: str | None) -> tuple[dict, dict]:
    """The facts for one blocked job; ``blocker`` is set unless the
    evidence proves the job's processes are gone."""
    facts: dict = {"job_id": job_id}
    try:
        job_dir = store.job_dir(job_id)
    except (ValueError, TypeError, AttributeError):
        facts["blocker"] = "not an agent-compute job id; inspect by hand"
        return facts, {}
    man = read_json(job_dir / "manifest.json")
    st = read_json(job_dir / "state.json")
    missing = [name for name, ok in (("job directory", job_dir.is_dir()),
                                     ("manifest", isinstance(man, dict)),
                                     ("state", isinstance(st, dict)))
               if not ok]
    if missing:
        facts["blocker"] = (f"no job evidence ({', '.join(missing)} missing "
                            "or unreadable); a missing record does not prove "
                            "that nothing was launched; inspect the host "
                            "(and the job's container) by hand")
        return facts, {}
    runner, guard = st.get("runner"), st.get("guard")
    bad = [k for k, v in (("runner", runner), ("guard", guard),
                          ("workload", st.get("workload")),
                          ("container", man.get("container")))
           if v is not None and not isinstance(v, dict)]
    if bad:
        facts["blocker"] = f"malformed job state ({', '.join(bad)})"
        return facts, {}
    facts["runner"] = procs.liveness(runner)
    facts["guard"] = procs.liveness(guard)
    if facts["runner"] != procs.GONE and not st.get("outcome"):
        facts["blocker"] = ("its runner is still supervising; wait for it"
                            if facts["runner"] == procs.ALIVE else
                            "cannot tell whether its runner is alive")
        return facts, {}
    try:
        records = _guard_records(store, job_id)
    except OSError as error:
        # A49: an unlistable guard dir is not "no guard record"
        facts["blocker"] = (f"its guard records cannot be read ({error}); "
                            "inspect by hand")
        return facts, {}
    record = records[-1] if records else None
    problems = [p for p in (procs.record_problem(r) for r in records) if p]
    if problems:
        facts["blocker"] = ("malformed guard record: " + "; ".join(problems)
                            + "; inspect by hand")
        return facts, {}
    workload = st.get("workload")
    if workload is None and isinstance(entry.get("evidence"), dict):
        workload = entry["evidence"].get("workload")
    if not isinstance(workload, dict):
        workload = None
    from .runner import workload_from_record
    if workload is None:
        workload = workload_from_record(man, record)
    container = man.get("container")
    # G2a: the marker names the job was launched with (a manifest written
    # before 0.2.0 has none: SMC_GUARD_JOB), plus every known name
    markers = procs.scan_markers(man)
    if terminate_job == job_id:
        facts["terminate"] = terminate(container, record, workload, guard,
                                       _workload_seen(st, entry),
                                       markers=markers)
    if procs.liveness(guard) != procs.GONE:
        facts["blocker"] = ("its guard process is alive or unverifiable "
                            "(it may hold the compute lock and supervise the "
                            f"workload); wait, or --terminate {job_id}")
        return facts, {}
    never = _never_launched(store, job_id, records, st)
    if never:
        evidence = {"verified": True, "clean": True,
                    "method": "never-launched", "note": never}
    elif container:
        evidence = procs.container_job_evidence(container, record, workload,
                                                markers=markers)
    elif workload is not None:
        evidence = procs.host_survivors(workload)
    else:
        evidence = {"verified": False, "clean": False,
                    "reason": "launch began but no workload pid was recorded"}
    facts["cleanup"] = evidence
    if evidence.get("orphaned"):
        facts["blocker"] = ("unmarked processes that may be this job's "
                            "remain (see cleanup.orphaned); `--terminate` "
                            "never signals unmarked processes, so they need "
                            "handling by hand: inspect them, stop them if "
                            "they are this job's, then reconcile")
    elif evidence.get("unmarked_members"):
        facts["blocker"] = ("this job's processes remain, some without the "
                            "job marker (see cleanup.unmarked_members); "
                            "`--terminate` never signals unmarked processes "
                            "nor a marked parent of one (that would erase "
                            "the only link to it), so they need handling by "
                            "hand: inspect them, stop them if they are this "
                            "job's, then reconcile")
    elif evidence.get("clean") is not True:
        facts["blocker"] = ("workload processes remain or cleanup cannot "
                            "be verified; inspect them, then "
                            f"`agent-compute reconcile --terminate {job_id}`"
                            " if they are this job's")
    return facts, {"record": record, "never_launched": bool(never)}


def _never_launched(store: JobStore, job_id: str, records: list,
                    st: dict) -> str | None:
    """Positive evidence that the guard never began launching: an existing
    job directory, no workload identity, no guard record with a pid and no
    samples.csv (the guard opens it before launching) in any attempt."""
    try:
        if st.get("workload") or _samples_exist(store, job_id):
            return None
    except OSError:
        return None             # A49: no positive evidence
    if any(isinstance(r, dict) and r.get("pid") for r in records):
        return None
    return ("job directory present; no workload identity, no guard record "
            "pid and no samples.csv in any attempt: the guard never began "
            "launching")


def _record_resolution(store: JobStore, job_id: str, entry: dict,
                       evidence: dict, context: dict) -> None:
    """A durable history entry for every resolution, with its evidence,
    whether or not the job already had an outcome."""
    from .runner import GUARD_OUTCOME
    from .state import state_lock
    record = context.get("record") or {}
    guard_outcome = record.get("outcome")
    with state_lock(store.state_dir):
        path = store.job_dir(job_id) / "state.json"
        st = read_json(path)
        if isinstance(st, Unreadable):
            raise RecordUnreadable(st)      # C4: never overwritten
        st = st if isinstance(st, dict) else {"job_id": job_id}
        history = st.get("history")
        history = history if isinstance(history, list) else []
        item = store.history(event="reconciled",
                             block_reason=entry.get("reason"),
                             evidence=evidence)
        now = item["utc"]
        if not st.get("outcome"):
            outcome = GUARD_OUTCOME.get(guard_outcome, "unknown") \
                if isinstance(guard_outcome, str) else "unknown"
            reason = ("reconciled after lost supervision (guard record "
                      f"outcome {guard_outcome}); never retried "
                      "automatically")
            st.update(phase="done", outcome=outcome, reason=reason,
                      exit_code=record.get("exit_code"), ended_utc=now,
                      cleanup=evidence)
            item.update(outcome=outcome, reason=reason)
        else:
            item["outcome_kept"] = st["outcome"]
        history.append(item)
        st["history"] = history
        st["reconciled"] = {"utc": now, "evidence": evidence}
        st["updated_utc"] = now
        write_json(path, st)
        store.log_event(job_id, st, item)


def _workload_seen(st: dict, entry: dict) -> dict | None:
    """The runner's last proof that the job's exact-start leader was alive:
    from the job state, else from the block evidence the runner wrote."""
    keys = ("workload_seen_utc", "workload_seen_ticks",
            "workload_seen_boot_id")
    if st.get("workload_seen_utc"):
        return {k: st.get(k) for k in keys}
    evidence = entry.get("evidence") if isinstance(entry, dict) else None
    seen = evidence.get("workload_seen") if isinstance(evidence, dict) \
        else None
    return seen if isinstance(seen, dict) else None


def terminate(container: dict | None, record: dict | None,
              workload: dict | None, guard: dict | None,
              seen: dict | None = None, *, markers=None) -> dict:
    """Stop an identified orphan: ask its guard first (its own kill path),
    then signal the job's proven processes directly if any remain.  For a
    host group whose leader pid is free, ``seen`` (the runner's last proof
    that the leader was alive) bounds which members are provably the
    job's."""
    done = {}
    if guard and procs.liveness(guard) == procs.ALIVE:
        os.kill(int(guard["pid"]), signal.SIGTERM)
        done["guard_term"] = True
        for _ in range(120):
            if procs.liveness(guard) == procs.GONE:
                break
            time.sleep(0.5)
    if container:
        def evidence():
            return procs.container_job_evidence(container, record, workload,
                                                markers=markers)

        def send(sig):
            return procs.signal_container_job(container, record, workload,
                                              sig, markers=markers)
    elif workload:
        def evidence():
            return procs.host_survivors(workload)

        def send(sig):
            return procs.signal_host_group(workload, sig, seen)

        ev = evidence()
        if ev.get("verified") and ev.get("members") and \
                not ev.get("leader_alive"):
            refusal = procs.member_signal_refusal(workload, seen)
            if refusal:
                done["not_signalled"] = refusal
    else:
        return done
    if evidence().get("clean") is not True:
        done["workload_term"] = send(signal.SIGTERM)
        for _ in range(20):
            if evidence().get("clean") is True:
                break
            time.sleep(0.5)
        else:
            done["workload_kill"] = send(signal.SIGKILL)
            time.sleep(1.0)
    return done


def _guard_records(store: JobStore, job_id: str) -> list:
    """Every attempt's guard.json, oldest first (None where absent)."""
    root = store.job_dir(job_id) / "guard"
    if not is_dir_strict(root):
        return []
    attempts = sorted((p for p in glob_strict(root, "attempt-*")
                       if p.name.split("-")[1].isdigit()),
                      key=lambda p: int(p.name.split("-")[1]))
    return [read_json(a / "guard.json") for a in attempts]


def _samples_exist(store: JobStore, job_id: str) -> bool:
    """Whether any attempt has a ``samples.csv``; raises ``OSError`` when
    the guard dir cannot be examined or listed (A49: never "no")."""
    root = store.job_dir(job_id) / "guard"
    if not is_dir_strict(root):
        return False
    return any(lexists_strict(attempt / "samples.csv")
               for attempt in glob_strict(root, "attempt-*"))


def cmd_resume(config: Config, args) -> int:
    store = JobStore(config.state_dir)
    detect_lost(config)
    if store.block(config.host):
        out({"resumed": False, "reason": "host admission is blocked; "
             "`agent-compute reconcile` first",
             "block": record_view(store.block(config.host))}, args.json)
        return 1
    gate = store.gate()
    if isinstance(gate, Unreadable):
        # C4: an unreadable gate is not opened (removed) by `resume`
        out({"resumed": False, "reason": f"the dispatch gate record "
             f"{gate.reason}; an operator inspects it and removes it by "
             "hand", "dispatch_gate": gate.view()}, args.json)
        return 1
    store.open_gate()
    if config.queue_enabled:
        q.ensure_daemon(config)
        store.open_gate()
        q.resume_group(config)
    out({"resumed": True}, args.json)
    return 0


# --- doctor -----------------------------------------------------------------------

def guard_help(config: Config) -> str:
    try:
        result = subprocess.run([config.python, str(backend_path(config)),
                                 "run", "--help"], capture_output=True,
                                text=True, timeout=60, env=q.scrubbed_env())
    except (OSError, subprocess.SubprocessError) as error:
        return f"ERROR {error}"
    return result.stdout


def engine_profile_check(config: Config) -> tuple[bool, dict]:
    """Ask the internal engine itself (``status --profile-file``, read-only:
    it runs no workload and takes no lock) to accept the configured profile
    and report it with its optional keys filled in."""
    import tempfile
    with tempfile.TemporaryDirectory(prefix="acs-doctor-") as tmp:
        path = Path(tmp) / "profile.json"
        path.write_text(json.dumps(config.profile))
        env = q.scrubbed_env()
        env[cfgmod.GUARD_HOME_ENV] = str(config.effective_guard_home)
        try:
            result = subprocess.run([config.python, str(backend_path(config)),
                                     "status", "--profile-file", str(path)],
                                    capture_output=True, text=True,
                                    timeout=60, env=env)
        except (OSError, subprocess.SubprocessError) as error:
            return False, {"error": str(error)}
    try:
        data = json.loads(result.stdout)
    except ValueError:
        return False, {"rc": result.returncode,
                       "stderr": result.stderr.strip()[-400:]}
    effective = data.get("effective_profile")
    return result.returncode == 0 and isinstance(effective, dict), \
        {"effective_profile": effective}


def host_resources_view(config: Config) -> dict:
    """D4: this host's RAM and cores (the budget cap), the profile defaults
    `config init` derives from them, and the configured numbers."""
    found = cfgmod.host_resources()
    view = {"memory_gb": found["memory_gb"], "cores": found["cores"],
            "source": found["source"], "budget_cap_gb": found["memory_gb"]}
    if found["memory_gb"]:
        view["derived_defaults"] = cfgmod.derived_defaults(
            found["memory_gb"], found["cores"])
    view["configured"] = {key: config.profile[key] for key in
                          ("floor_gb", "budget_gb", "container_cpus")}
    view["configured"]["dedicated_budget_gb"] = config.dedicated_budget_gb
    view["note"] = ("the derived defaults are what `config init` prints for "
                    "a fresh config; the configured numbers decide (the "
                    "migration kept the operator's)")
    return view


def pathspecs_match(rel: str, pathspecs: list[str]) -> bool | None:
    """Whether git's pathspecs ``pathspecs`` match the tree path ``rel``
    (git's own rules, in a throwaway repository); None when git cannot
    tell."""
    import tempfile
    with tempfile.TemporaryDirectory(prefix="acs-pathspec-") as tmp:
        target = Path(tmp) / rel
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.touch()
            subprocess.run(["git", "init", "-q", tmp], check=True,
                           capture_output=True, timeout=30)
            listed = subprocess.run(
                ["git", "-C", tmp, "ls-files", "--others", "-z", "--",
                 *pathspecs], capture_output=True, text=True, timeout=30)
        except (OSError, subprocess.SubprocessError):
            return None
        if listed.returncode != 0:
            return None
        return Path(rel).as_posix() in listed.stdout.split("\0")


def records_inside_inputs(config: Config, project) -> list[dict]:
    """D3: the project's runlog/baseline that lie inside its tree AND are
    matched by its ``inputs`` pathspecs (the guard's writes would change
    the input fingerprint of the project's queued jobs)."""
    found = []
    for key in cfgmod.RECORD_KEYS:
        template = getattr(project, key)
        try:
            inside, rel = cfgmod.record_tree_path(template, project,
                                                  config.state_dir,
                                                  config.host)
        except ConfigError as error:
            found.append({"key": key, "template": template,
                          "error": str(error)})
            continue
        if not inside:
            continue
        if rel is None:
            found.append({"key": key, "template": template,
                          "note": "inside the tree; its path in the tree "
                                  "cannot be determined, so the inputs "
                                  "were not checked"})
            continue
        matched = pathspecs_match(rel, project.inputs)
        if matched is not False:
            found.append({"key": key, "path": rel, "template": template,
                          "inputs": project.inputs,
                          **({} if matched else
                             {"note": "git could not check the pathspecs"})})
    return found


def pin_authorization_view(config: Config) -> dict:
    """D8: the last recorded ``pin-backend`` authorization of this config
    (``authorizations.jsonl``): who, when, the evidence (and whether that
    run had Docker), the engine sha256 it pinned."""
    last = None
    try:
        lines = (config.state_dir / "authorizations.jsonl").read_text(
            errors="replace").splitlines()
    except OSError:
        lines = []
    for line in lines:
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if isinstance(record, dict) and \
                record.get("action") == "pin-backend" and \
                record.get("config") == str(config.path):
            last = record
    if last is None:
        return {"recorded": False, "pinned": config.guard_sha256,
                "note": "no pin-backend authorization recorded for this "
                        "config (pinned before 0.2.0, or by hand)"}
    return {"recorded": True, "authorized_by": last.get("authorized_by"),
            "utc": last.get("utc"), "evidence": last.get("evidence"),
            "engine_sha256": last.get("engine_sha256"),
            "is_the_current_pin":
                last.get("engine_sha256") == config.guard_sha256}


def migration_check(config: Config) -> dict | None:
    """The last ``config migrate`` of this config (``authorizations.jsonl``)
    and, side by side, the effective values of its backup (v1) and of the
    config now; None when there is no record file or no such record.

    The check judges the migration, not what the operator changed after it
    (register A42): a value that differs from the backup is excused only
    when a later, successful authorization of this config (in the record
    file after the migration's record; ``pin-backend`` records the keys it
    changed in ``diff_summary.changed``) set that key to the value the
    config holds now.  Such a key is reported under ``changed_later`` with
    its verdict ``changed-later``; any other difference (a hand edit, a
    failed pin, a pin recorded before the migration or for another config)
    stays a problem.  A record file or backup that cannot be read fails
    the check (closed)."""
    path = config.state_dir / AUTH_FILE
    try:
        lines = path.read_text().splitlines()
    except FileNotFoundError:
        return None
    except OSError as error:
        return {"ok": False, "authorizations": str(path),
                "error": f"the authorization records cannot be read: "
                         f"{error}"}
    records = []
    for line in lines:
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if isinstance(record, dict) and \
                record.get("config") == str(config.path):
            records.append(record)
    at = next((i for i in range(len(records) - 1, -1, -1)
               if records[i].get("action") == "config-migrate"), None)
    if at is None:
        return None
    last = records[at]
    backup = Path(str(last.get("backup")))
    try:
        before = cfgmod.v1_effective_values(tomllib.loads(backup.read_text()))
    except (OSError, ValueError, ConfigError) as error:
        return {"ok": False, "backup": str(backup), "authorization": last,
                "error": f"the backup cannot be compared: {error}"}
    rows = cfgmod.compare_effective(before, cfgmod.effective_values(config))
    later = later_authorized_values(records[at + 1:])
    changed_later = []
    for row in rows:
        found = later.get(row["key"])
        # a removed key is never excused: only a value the config holds now
        if row["verdict"] != "changed" or found is None or \
                found["value"] != row["after"]:
            continue
        row["verdict"] = "changed-later"
        auth = found["authorization"]
        changed_later.append({
            "key": row["key"], "backup": row["before"], "now": row["after"],
            "action": auth.get("action"), "utc": auth.get("utc"),
            "authorized_by": auth.get("authorized_by"),
            "note": f"changed later by {auth.get('action')} at "
                    f"{auth.get('utc')} (authorized)"})
    problems = cfgmod.comparison_problems(rows)
    return {"ok": not problems, "backup": str(backup),
            "authorized_by": last.get("authorized_by"),
            "utc": last.get("utc"), "problems": problems,
            "changed_later": changed_later, "values": rows}


def later_authorized_values(records: list[dict]) -> dict:
    """``{key: {"value", "authorization"}}``: the value the last successful
    authorization among ``records`` (in file order) wrote for each key it
    recorded in ``diff_summary.changed`` (``{key: [before, after]}``).  An
    authorization followed by its failure record (``<action>-failed`` with
    its ``utc`` as ``authorization_utc``) changed nothing."""
    failed = {(str(r.get("action"))[:-len("-failed")],
               str(r.get("authorization_utc"))) for r in records
              if str(r.get("action")).endswith("-failed")}
    found: dict = {}
    for record in records:
        action = str(record.get("action"))
        if action.endswith("-failed") or action == "config-migrate" or \
                (action, str(record.get("utc"))) in failed:
            continue
        summary = record.get("diff_summary")
        changed = summary.get("changed") if isinstance(summary, dict) \
            else None
        if not isinstance(changed, dict):
            continue
        for key, pair in changed.items():
            if isinstance(pair, list) and len(pair) == 2:
                found[key] = {"value": pair[1], "authorization": record}
    return found


def event_log_check(config: Config) -> dict:
    path = events_path(config.state_dir, config.host)
    detail = {"path": str(path), "lines": 0, "last_utc": None,
              "note": "derived data (D11): never read for a decision; "
                      "state.json is authoritative"}
    try:
        lines = [line for line in path.read_text(errors="replace")
                 .splitlines() if line.strip()]
    except FileNotFoundError:
        return detail
    except OSError as error:
        detail["error"] = str(error)
        return detail
    detail["lines"] = len(lines)
    for line in reversed(lines):
        try:
            detail["last_utc"] = json.loads(line).get("utc")
            break
        except (ValueError, AttributeError):
            continue
    return detail


RESULT_LABEL = {"pass": "PASS", "fail": "FAIL", "warn": "WARN",
                "info": "INFO"}


def cmd_doctor(config: Config, args) -> int:
    checks: list[dict] = []

    def check(name, ok, detail=None, level="error"):
        result = "pass" if ok else {"error": "fail", "warning": "warn",
                                    "info": "info"}[level]
        checks.append({"check": name, "ok": bool(ok),
                       "level": "ok" if ok else level, "result": result,
                       "detail": detail})

    check("config", True, {"path": str(config.path),
                           "schema": CONFIG_SCHEMA})
    check("host identity set", bool(config.host), config.host)
    host_id = ensure_host_id(config.state_dir, config.host)
    check("host id", isinstance(host_id, dict),
          host_id if isinstance(host_id, dict) else host_id.view())
    renamed = rename_problem(config, JobStore(config.state_dir))
    check("host label unchanged under recorded work", not renamed,
          renamed or {"host": config.host})
    moved = guard_home_change_problem(config, JobStore(config.state_dir))
    check("guard home unchanged under recorded work", not moved,
          moved or {"guard_home": str(config.effective_guard_home)})
    migrated = migration_check(config)
    if migrated is not None:
        check("config migration kept the effective values", migrated["ok"],
              migrated)
    check("guard home and job markers", True,
          {"guard_home": str(config.effective_guard_home),
           "configured": config.guard_home is not None,
           "markers": config.markers,
           "workload_env": config.workload_env,
           "legacy_workload_env": config.legacy_workload_env},
          level="info")
    check("python >= 3.11", sys.version_info >= (3, 11), sys.version.split()[0])
    floor = safety.running_floor_problems()
    check("installation safety floor", not floor,
          floor if floor else
          "not an installed copy (a checkout); not checked" if floor is None
          else {"running": str(safety.installation()[1]),
                "min_activatable_level": safety.MIN_ACTIVATABLE_LEVEL})
    check("backend kind", True,
          {"kind": config.backend_kind,
           "meaning": "the ACS-internal guard (engine of the installed "
                      "version each job was submitted with); the only "
                      "backend since 0.2.0"})
    path = backend_path(config)
    ident = guard_identity(path)
    check("backend guard present", ident.get("exists"), str(path))
    if ident.get("exists"):
        check("backend guard matches verified fingerprint",
              config.guard_sha256 and ident["sha256"] == config.guard_sha256,
              {"current": ident["sha256"], "verified": config.guard_sha256,
               "path": str(path),
               "last_commit": ident.get("last_commit"),
               "dirty": ident.get("dirty")})
        text = guard_help(config)
        required = REQUIRED_GUARD_OPTIONS + REQUIRED_ENGINE_OPTIONS
        missing = [o for o in required if o not in text]
        check("backend guard options", not missing,
              {"missing": missing} if missing else "all required options")
    check("backend pin authorization", True, pin_authorization_view(config),
          level="info")
    ok_profile, detail = engine_profile_check(config) \
        if ident.get("exists") else (False, {"error": "no engine"})
    check("machine profile ([backend.profile])", ok_profile,
          {"configured": config.profile, **detail})
    check("host resources and profile defaults", True,
          host_resources_view(config), level="info")
    check("host memory budget", True,
          {"dedicated": True, "budget_gb": config.dedicated_budget_gb,
           "effective_budget_gb": config.dedicated_budget_gb,
           "source": "backend.dedicated_budget_gb (operator declaration; "
                     "applies to every project; a per-job ceiling, never "
                     "an admission test; guard floor still applies)"}
          if config.dedicated_budget_gb is not None else
          {"dedicated": False,
           "effective_budget_gb": config.effective_budget_gb,
           "budget_gb": f"backend.profile.budget_gb "
                        f"({config.profile['name']})"})
    lock = config.effective_guard_home / "lock"
    check("compute lock location", True,
          {"lock": str(lock), "exists": lock.exists(),
           "note": "read-only check; ACS admission never opens, moves or "
                   "unlinks it (only the guard process takes it)"})
    marker = inflight_view(config)
    marker_ok = True
    if marker is not None:
        job = marker.get("acs_job")
        try:
            st = JobStore(config.state_dir).state(job) \
                if isinstance(job, str) and job else None
        except ValueError:
            st = None
        running = isinstance(st, dict) and not st.get("outcome") and \
            procs.liveness(st.get("runner")) == procs.ALIVE
        marker_ok = running
        marker["note"] = ("written by the guard of a running ACS job"
                          if running else
                          "stale or unmapped: the internal guard refuses "
                          "heavy work until it is proven gone; run "
                          "`agent-compute reconcile`")
    check("no stale in-flight marker", marker_ok, marker)
    check("test mode off", not config.test_mode,
          "test_mode config: never use for production" if config.test_mode
          else None, level="warning")
    state = config.state_dir
    private_dir(state)
    mode = oct(state.stat().st_mode & 0o777)
    check("state dir private", mode == "0o700", f"{state} {mode}")
    check("state dir not cloud-synced",
          not any(m in str(state.resolve()) for m in CLOUD_MARKERS),
          str(state.resolve()))
    if config.queue_enabled:
        try:
            version = subprocess.run([config.pueue, "--version"],
                                     capture_output=True, text=True,
                                     timeout=15).stdout.strip()
        except (OSError, subprocess.SubprocessError, TypeError) as error:
            version = f"ERROR {error}"
        check("pueue available", version.startswith("pueue "), version)
        running = q.daemon_running(config)
        check("queue daemon running", running,
              "started on demand by submit/resume" if not running else
              str(q.socket_path(config)), level="info")
        if running:
            group = q.status_json(config).get("groups", {}).get(
                config.queue_group)
            check("shared heavy group has one slot",
                  group and group.get("parallel_tasks") == 1, group)
    else:
        check("queue", True, "disabled on this host: foreground/detached "
              "guarded execution only; durable submission unavailable",
              level="info")
    store = JobStore(config.state_dir)
    lost = detect_lost(config)
    check("no lost-supervision jobs", not lost, lost)
    check("host admission not blocked", not store.block(config.host),
          record_view(store.block(config.host)))
    unreadable = _admission_records_unreadable(store, config.host)
    check("admission records readable", not unreadable,
          [r.view() for r in unreadable] or None)
    groups = None
    daemon = q.daemon_running(config) if config.queue_enabled else None
    if daemon:
        try:
            groups = q.status_json(config).get("groups", {})
        except (q.QueueError, OSError, subprocess.SubprocessError,
                ValueError):
            groups = None
    dispatch = dispatch_state(config, store, groups, daemon)
    check("dispatch gate open", dispatch["held"] is False,
          {**dispatch, "gate": record_view(store.gate())}, level="warning")
    check("event log", True, event_log_check(config), level="info")
    for name, project in config.projects.items():
        roots = [str(expand(r)) for r in project.roots]
        check(f"project {name}", True,
              {"roots": roots, "max_timeout_min": project.max_timeout_min,
               "max_threads": project.max_threads,
               "containers": project.containers,
               "reserve_detector": project.reserve_detector,
               "container_limits": project.container_limits,
               "runlog": project.runlog, "baseline": project.baseline,
               "records_in_tree": project.records_in_tree})
        inside = records_inside_inputs(config, project)
        check(f"project {name} records outside its inputs", not inside,
              {"inside": inside,
               "why": "a runlog or baseline matched by the project's inputs "
                      "pathspecs changes the input fingerprint when the "
                      "guard writes to it, so the project's queued jobs end "
                      "held-inputs-changed; exclude it (for example "
                      "\":(exclude)docs\") or keep the records in the state "
                      "dir"} if inside else
              {"runlog": project.runlog, "baseline": project.baseline,
               "inputs": project.inputs}, level="warning")
    for name, remote in config.remotes.items():
        check(f"remote host {name}", True,
              {"transport": remote.transport, "ssh": remote.ssh,
               "note": "not contacted by doctor; use --host to query it"},
              level="info")
    # D13 (ADR-003): one admission authority on this host, read-only; a
    # report, never an admission input
    single = authority.single_authority(config)
    for key, label in (("guard_home", "guard home"),
                       ("state_dir", "state dir"),
                       ("queue_daemon", "queue daemon")):
        check(f"single authority: {label}", single[key]["ok"], single[key])
    ok = all(c["ok"] or c["level"] in ("warning", "info") for c in checks)
    ident = acs_identity(config)
    # D10: the identity flat (version = the installed version name) and
    # under "acs", as in every diagnostic output
    report = {**{k: ident[k] for k in ("version", "source_sha",
                                       "package_version", "safety_level",
                                       "engine_sha256", "host", "host_id",
                                       "utc")},
              "acs": ident, "ok": ok, "checks": checks,
              "single_authority": single}
    if args.json:
        out(report, True)
    else:
        print(f"agent-compute {ident['version']} ({ident['source_sha']}), "
              f"host {config.host} ({ident['host_id']})")
        for c in checks:
            mark = RESULT_LABEL[c["result"]]
            detail = c["detail"] if not isinstance(c["detail"], (dict, list)) \
                else json.dumps(c["detail"], default=json_default)
            print(f"[{mark}] {c['check']}" + (f": {detail}" if detail else ""))
        print("doctor:", "OK" if ok else "PROBLEMS FOUND")
    return 0 if ok else 1


def _maybe_running(store: JobStore, job_id: str) -> bool:
    """A job with a runner and no outcome, or whose state cannot be read
    (C4: it may be running)."""
    st = store.state(job_id)
    if isinstance(st, Unreadable):
        return True
    st = st or {}
    return not st.get("outcome") and bool(st.get("runner"))


def cmd_daemon(config: Config, args) -> int:
    if not config.queue_enabled:
        out({"queue": "disabled on this host"}, args.json)
        return 1
    if args.action == "start":
        out({"notices": q.ensure_daemon(config)}, args.json)
        return 0
    if args.action == "status":
        out({"running": q.daemon_running(config),
             "socket": str(q.socket_path(config))}, args.json)
        return 0
    # `pueue shutdown` SIGKILLs running tasks (verified on 4.0.4), which
    # would orphan a workload: pause dispatch first, then refuse if anything
    # is still running.
    q.pause_group_quietly(config)
    store = JobStore(config.state_dir)
    active = [j for j in store.all_ids() if _maybe_running(store, j)]
    for _ in range(30):         # a finished runner may still be exiting
        if active or not q.running_tasks(config):
            break
        time.sleep(0.5)
    if active or q.running_tasks(config):
        out({"stopped": False, "reason": f"jobs still running: {active}; "
             "dispatch stays paused (`agent-compute resume` to undo)"},
            args.json)
        return 1
    q.shutdown(config)
    out({"stopped": True}, args.json)
    return 0


def cmd_pin_backend(config: Config, args) -> int:
    """Operator-only (A4, D8): record the internal engine of THIS installed
    version as verified, bound to evidence, as a recorded authorization.

    ``--authorized-by "<who, when, what>"`` (one line) is required; agents
    never pin a guard (they report a mismatch to the operator).
    ``--evidence`` must be a suite output committed (and unmodified) in a
    git repository whose first line names this host, the internal engine
    (``profile standard``, ``profile legacy`` or, before 0.2.0, ``backend
    acs-guard``) and ``engine sha256 <sha>`` of the engine pinned, and
    whose pytest summary shows 0 failed, 0 errors; whether that run had
    Docker is read from the header and reported (not required).  Since
    campaign PA K4 (A47) it must be the full-suite file of
    ``tools/gate_run.py`` (its header token, a consistent skip line with
    no untagged skip, no deselected test, a collected count equal to the
    summary's, ``EXIT 0``; see ``_gate_file_problem``): a check of form,
    not of forgery (A4).  The
    authorization is appended to ``<state>/authorizations.jsonl`` (the
    record shape of ``config migrate``) FIRST; only then is the config
    rewritten atomically (0600), changing only the ``sha256`` line of the
    ``[backend]`` table (the result must load).  A record that cannot be
    appended refuses the pin, the config unchanged; a config write that
    fails after the record is recorded as ``pin-backend-failed`` (register
    A34).  ``--backend`` accepts only ``acs-guard`` (the external guard.py
    backend is retired, D1)."""
    authorized = _authorization_text(args.authorized_by)
    if authorized is None:
        print("agent-compute: pin-backend is an operator action: pass "
              "--authorized-by \"<who, when, what>\" (one line).  Agents "
              "never pin a guard: report the mismatch to the operator.",
              file=sys.stderr)
        return EXIT_NOT_ACCEPTED
    from .runner import engine_path
    path = engine_path()
    ident = guard_identity(path)
    if not ident.get("exists"):
        print(f"agent-compute: no guard at {path}", file=sys.stderr)
        return 1
    sha = ident["sha256"]
    problem, record = pin_evidence_problem(Path(args.evidence), sha,
                                           config.host)
    if problem:
        print(f"agent-compute: pin-backend refused: {problem}",
              file=sys.stderr)
        return 1
    try:
        text = set_backend_keys(config.path.read_text(), {"sha256": sha})
        _check_loads(config.path, text)
    except ConfigError as error:
        print(f"agent-compute: pin-backend refused: {error}", file=sys.stderr)
        return 1
    stamp = utc_now()
    ident_acs = identity.identity()
    host_id = _authorization_host_id(config.state_dir, config.host)
    authorization = {
        "schema": AUTHORIZATION_SCHEMA, "utc": stamp, "action": "pin-backend",
        "authorized_by": authorized, "config": str(config.path),
        "evidence": record, "engine": str(path), "engine_sha256": sha,
        "diff_summary": {"changed": {"backend.sha256":
                                     [config.guard_sha256, sha]}},
        "version": ident_acs["version"],
        "source_sha": ident_acs["source_sha"], "host": config.host,
        "host_id": host_id}
    report = {"pinned": ident, "kind": BACKEND_ACS,
              "previous": {"kind": config.backend_kind,
                           "sha256": config.guard_sha256},
              "evidence": record}
    # A34: the record first; no record, no pin
    try:
        report["authorization"] = str(append_authorization(config.state_dir,
                                                           authorization))
    except OSError as error:
        print(f"agent-compute: pin-backend refused: the authorization record "
              f"could not be appended to {config.state_dir / AUTH_FILE} "
              f"({error}); the config is unchanged", file=sys.stderr)
        return 1
    try:
        write_config_atomic(config.path, text)
    except OSError as error:
        _record_failure(config.state_dir, authorization,
                        "pin-backend-failed", error)
        print(f"agent-compute: pin-backend failed: the config could not be "
              f"written ({error}); it is unchanged; the authorization and "
              "this failure are recorded", file=sys.stderr)
        return 1
    out(report, args.json)
    return 0


def _check_loads(path: Path, text: str) -> Config:
    """Refuse a rewritten config that would not load (ConfigError);
    return what it loads as.  The check copy lives in a private temporary
    directory, not beside the config (A34: the config's directory is
    written only by the final atomic replace)."""
    import shutil
    import tempfile
    folder = Path(tempfile.mkdtemp(prefix="acs-config-check-"))
    try:
        temp = folder / path.name
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as handle:
            handle.write(text)
        try:
            return load(temp)
        except (ConfigError, tomllib.TOMLDecodeError) as error:
            raise ConfigError(f"the rewritten config would not load: "
                              f"{error}") from None
    finally:
        shutil.rmtree(folder, ignore_errors=True)


_SUMMARY = re.compile(r"^=+ (?P<body>.*\bin [0-9.]+s\b.*?) =+$")


# the header's statement that the suite ran the internal engine: the test
# profiles of 0.2.0, or the backend mode of the suites before it
_ENGINE_RUN = re.compile(r"\bprofile (standard|legacy)\b|"
                         r"\bbackend acs-guard\b")
_DOCKER_HIDDEN = re.compile(r"\bdocker hidden\b|\bwithout docker\b|"
                            r"\bno docker\b", re.I)
_DOCKER_WITH = re.compile(r"\bwith docker\b", re.I)


def evidence_docker(header: str) -> str:
    """Whether the run behind an evidence header had Docker: ``with``,
    ``hidden`` or ``not stated`` (reported, never required: a Docker-hidden
    run skips the container tests)."""
    if _DOCKER_HIDDEN.search(header):
        return "hidden"
    if _DOCKER_WITH.search(header):
        return "with"
    return "not stated"


def pin_evidence_problem(path: Path, sha: str,
                         host: str) -> tuple[str | None, dict]:
    """Why ``path`` is not evidence for pinning the engine ``sha`` on
    ``host`` (None if it is), and what was checked."""
    record: dict = {"path": str(path)}
    try:
        present = is_file_strict(path)
    except OSError as error:
        return (f"the evidence file {path} cannot be examined "
                f"({error.strerror or error})"), record
    if not present:
        return f"no evidence file at {path}", record
    path = path.resolve()
    record["path"] = str(path)

    def git(*a):
        return subprocess.run(["git", "-C", str(path.parent), *a],
                              capture_output=True, text=True, timeout=60)
    try:
        tracked = git("ls-files", "--error-unmatch", "--", path.name)
        status = git("status", "--porcelain", "--", path.name)
        commit = git("log", "-1", "--format=%H", "--", path.name)
    except (OSError, subprocess.SubprocessError) as error:
        return f"git cannot inspect {path}: {error}", record
    if tracked.returncode != 0 or not commit.stdout.strip():
        return f"{path} is not committed in a git repository", record
    if status.stdout.strip():
        return f"{path} differs from its committed version", record
    record["commit"] = commit.stdout.strip()
    lines = path.read_text(errors="replace").splitlines()
    header = next((line for line in lines if line.strip()), "")
    record["header"] = header[:300]
    record["docker"] = evidence_docker(header)
    if not _ENGINE_RUN.search(header):
        return ("its header does not say the suite ran the internal engine "
                "(`profile standard`, `profile legacy`, or before 0.2.0 "
                "`backend acs-guard`)"), record
    named = re.findall(r"engine sha256 ([0-9a-f]{16,64})\b", header)
    if not named or not all(sha.startswith(n) for n in named):
        return (f"its header does not name the new engine sha256 "
                f"{sha[:16]} (first line: {header[:160]!r})"), record
    if not re.search(rf"\bon {re.escape(host)}\b", header):
        return f"its header does not name this host ({host})", record
    summary = next((m for m in (_SUMMARY.match(line.strip())
                                for line in reversed(lines)) if m), None)
    if summary is None:
        return "no pytest summary line (a completed run)", record
    body = summary.group("body")
    record["summary"] = body
    counts = {kind: int(n) for n, kind in
              re.findall(r"(\d+) (failed|passed|errors?|xpassed)", body)}
    bad = counts.get("failed", 0) + counts.get("error", 0) + \
        counts.get("errors", 0)
    if bad or not counts.get("passed"):
        return f"the suite did not pass cleanly ({body})", record
    # PA K4 (A47): the file must be the tool's full-suite file, of form
    why = _gate_file_problem(lines, host, counts, body, record)
    if why:
        return why, record
    return None, record


# the header written by tools/gate_run.py (campaign PA K4; register A47)
GATE_FORMATS = (1,)
_GATE_TOKEN = re.compile(r"; written by tools/gate_run\.py (\d+);")
_GATE_SKIP = re.compile(
    r"^# skip-report: collected (?P<collected>\d+); .*?; skips by category: "
    r"(?P<categories>.*?); untagged (?P<untagged>\d+); "
    r"(?P<state>consistent|INCONSISTENT.*)$")
_COLLECTED = re.compile(
    r"^(?:collecting \.\.\. )?collected (\d+) items?\b")
# the pytest outcomes that are tests (warnings and the like are not)
_TEST_OUTCOMES = ("passed", "failed", "skipped", "error", "errors", "xfailed",
                  "xpassed")


_SKIP_SHORT = re.compile(
    r"^SKIPPED \[(\d+)\] \S+?(?::\d+)?: (?P<reason>.*)$")
_SKIP_VERBOSE = re.compile(
    r"^\S+?\.py(?:::.*?)? SKIPPED(?: \(.*\))?\s+\[\s*\d+%\]$")
_SKIP_TAG = re.compile(r"^\[([a-z]+)\]")
_SKIP_CATEGORIES = ("docker", "platform", "legacy", "pueue", "host", "clone",
                    "root")


def _gate_file_problem(lines: list[str], host: str, counts: dict,
                       summary: str, record: dict) -> str | None:
    """The conditions on the file itself beyond the host, engine and pass
    check (A47): a full suite run, written by ``tools/gate_run.py``.  The
    check is of form: it catches a smoke file, a filtered run, a missing
    skip accounting, a hand-edited header, a truncated or concatenated run;
    it does not detect forgery (A4), which the committed-and-unmodified rule
    and the operator's own authorization cover."""
    header = lines[0] if lines else ""
    first = next((line for line in lines if line.strip()), "")
    if first != header:
        return "the header must be the first line of the file"
    token = _GATE_TOKEN.search(header)
    if not token or int(token.group(1)) not in GATE_FORMATS:
        return ("its header does not say it was written by "
                "`tools/gate_run.py` (`written by tools/gate_run.py <n>`): "
                "write the evidence with that tool (README, \"Pin the "
                "engine\")")
    record["gate_tool"] = int(token.group(1))
    if not re.search(rf"^# \S+ full suite on {re.escape(host)}, ", header):
        return (f"its header does not say `full suite on {host}, ` (the "
                "tool's full-suite header for this host)")
    if not re.search(r"\bsource sha [0-9a-f]{40};", header):
        return "its header does not name a full source sha"
    if not re.search(r"\btracked changes: 0;", header):
        return "its header does not say `tracked changes: 0`"
    if not re.search(r"\bredactions: \d+;", header):
        return "its header does not record `redactions: <n>`"
    pueue = re.search(r"; pueue (\d+)\.\d+", header)
    if not pueue or pueue.group(1) != "4":
        return ("its header does not record `pueue 4.x` (the standard "
                "profile needs Pueue)")
    # each part exactly once: a concatenation of two outputs is not a run
    once = (("header", lambda x: x.startswith("# ") and
             " full suite on " in x or "written by tools/gate_run.py" in x),
            ("skip accounting", lambda x: x.startswith("# skip-report:")),
            ("pytest collected-count", lambda x: bool(_COLLECTED.match(x))),
            ("pytest summary", lambda x: bool(_SUMMARY.match(x.strip()))),
            ("EXIT", lambda x: bool(re.fullmatch(r"EXIT -?\d+", x.strip()))))
    for name, test in once:
        found = sum(1 for line in lines if test(line))
        if found != 1:
            return (f"the file has {found} {name} lines, not exactly one "
                    "(a concatenated or edited output)")
    if any(line.startswith("# gate_run: REFUSED") for line in lines):
        return "the tool marked this run REFUSED (the repository changed)"
    skip = _GATE_SKIP.match(lines[1]) if len(lines) > 1 else None
    if skip is None:
        return ("line 2 is not the skip accounting "
                "(`# skip-report: ...`, from tools/skip_report.py)")
    if skip.group("state") != "consistent":
        return f"the skip accounting is not consistent ({skip.group('state')})"
    if skip.group("untagged") != "0":
        return (f"the skip accounting has {skip.group('untagged')} untagged "
                "skip(s): an unclassified skip is unresolved evidence")
    if "deselected" in summary or any(
            _COLLECTED.match(line) and "deselected" in line
            for line in lines):
        return "the run deselected tests (a -k/-m filter): not a full suite"
    collected = next((int(m.group(1)) for m in
                      (_COLLECTED.match(line) for line in lines) if m), None)
    outcomes = {kind: int(n) for n, kind in re.findall(r"(\d+) (\w+)",
                                                       summary)}
    ran = sum(n for kind, n in outcomes.items() if kind in _TEST_OUTCOMES)
    record["collected"] = collected
    wanted = int(skip.group("collected"))
    if collected != wanted or ran != wanted:
        return (f"the collected count does not match: the skip line says "
                f"{wanted}, pytest collected {collected}, its summary "
                f"counts {ran} outcomes (a truncated or filtered run)")
    # the skip categories, recomputed from the body (line 2 is a copy)
    mine = {name: 0 for name in _SKIP_CATEGORIES}
    untagged = total = 0
    for line in lines:
        short = _SKIP_SHORT.match(line.rstrip())
        if short:
            n = int(short.group(1))
            total += n
            tag = _SKIP_TAG.match(short.group("reason"))
            if tag and tag.group(1) in mine:
                mine[tag.group(1)] += n
            else:
                untagged += n
    verbose = sum(1 for line in lines if _SKIP_VERBOSE.match(line.rstrip()))
    skipped = outcomes.get("skipped", 0)
    if untagged or total != skipped or verbose != skipped:
        return (f"the body's skips do not add up: {total} in the short "
                f"summary ({untagged} untagged), {verbose} in the -v lines, "
                f"{skipped} in the summary")
    stated = {name: int(n) for name, n in re.findall(
        r"([a-z]+) (\d+)", skip.group("categories"))}
    mine = {name: n for name, n in mine.items() if n}
    if stated != mine:
        return (f"the skip categories on line 2 ({stated}) are not the "
                f"body's ({mine})")
    if mine.get("pueue"):
        return (f"{mine['pueue']} `[pueue]` skips: the standard profile "
                "needs pueue and pueued")
    if record.get("docker") == "with" and mine.get("docker"):
        return (f"its header says WITH Docker but {mine['docker']} Docker "
                "tests skipped")
    last = next((line.strip() for line in reversed(lines) if line.strip()),
                "")
    if last != "EXIT 0":
        return f"the file does not end with `EXIT 0` (last line: {last!r})"
    return None


_TABLE = re.compile(r"^\s*\[")
_BACKEND = re.compile(r"^\s*\[\s*backend\s*\]\s*(#.*)?$")


def set_backend_sha(text: str, sha: str) -> str:
    """``text`` with the ``[backend]`` table's ``sha256`` set to ``sha``;
    every other line (another table's ``sha256`` included) unchanged."""
    return set_backend_keys(text, {"sha256": sha})


def set_backend_keys(text: str, values: dict[str, str]) -> str:
    """``text`` with these string keys of the ``[backend]`` table set (a
    missing one inserted after the header); every other line, comment and
    table (``[backend.profile]`` included) unchanged, which is checked by
    parsing both."""
    lines = text.splitlines(keepends=True)
    start = next((i for i, line in enumerate(lines)
                  if _BACKEND.match(line)), None)
    if start is None:
        raise ConfigError("the config has no [backend] table header")
    end = next((i for i in range(start + 1, len(lines))
                if _TABLE.match(lines[i])), len(lines))
    for key, value in values.items():
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
            raise ConfigError(f"refusing to write {key} = {value!r}")
        pattern = re.compile(rf"^\s*{re.escape(key)}\s*=")
        new_line = f'{key} = "{value}"\n'
        hits = [i for i in range(start + 1, end) if pattern.match(lines[i])]
        if len(hits) > 1:
            raise ConfigError(f"[backend] has more than one {key} line")
        if hits:
            lines[hits[0]] = new_line
        else:
            lines.insert(start + 1, new_line)
            end += 1
    result = "".join(lines)
    try:
        before, after = tomllib.loads(text), tomllib.loads(result)
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(f"the config does not parse: {error}") from None
    before.setdefault("backend", {}).update(values)
    if after != before:
        raise ConfigError(f"rewriting [backend] {sorted(values)} would change "
                          "other settings; edit the config by hand")
    return result


def _authorization_text(text) -> str | None:
    """D8: ``--authorized-by "<who, when, what>"``: one non-empty line."""
    if not isinstance(text, str) or not text.strip() or "\n" in text or \
            "\r" in text or len(text) > 500:
        return None
    return text.strip()


AUTH_FILE = "authorizations.jsonl"


def append_authorization(state_dir: Path, record: dict) -> Path:
    """Append one operator authorization (D8) to
    ``<state>/authorizations.jsonl`` (0600, fsynced).  An existing state
    dir is used as it is (never re-moded here, A34: a read-only one refuses
    the record, and so the operator action)."""
    state_dir = Path(state_dir)
    if not state_dir.is_dir():
        private_dir(state_dir)
    path = state_dir / AUTH_FILE
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, (json.dumps(record, sort_keys=True,
                                 default=json_default) + "\n").encode())
        os.fsync(fd)
    finally:
        os.close(fd)
    return path


def _authorization_host_id(state_dir: Path, host: str) -> str | None:
    """The host id for an authorization record: created on first use, but
    never by re-moding an existing state dir this process may not write
    (review round 1, note 11: the record's append then refuses the action
    cleanly)."""
    state_dir = Path(state_dir)
    if state_dir.is_dir() and not os.access(state_dir, os.W_OK):
        record = read_host_id(state_dir)
    else:
        try:
            record = ensure_host_id(state_dir, host)
        except OSError:
            record = None
    return record.get("host_id") if isinstance(record, dict) else None


def _record_failure(state_dir: Path, authorization: dict, action: str,
                    error: BaseException) -> None:
    """A34: after the authorization was recorded, the config write
    failed: record that too (best effort; the error is printed anyway)."""
    try:
        append_authorization(state_dir, {
            "schema": AUTHORIZATION_SCHEMA, "utc": utc_now(),
            "action": action, "authorized_by": authorization.get(
                "authorized_by"),
            "authorization_utc": authorization.get("utc"),
            "config": authorization.get("config"),
            "error": f"{type(error).__name__}: {error}",
            "host": authorization.get("host"),
            "host_id": authorization.get("host_id")})
    except OSError:
        pass


def _unfinished_in(state_dir: Path) -> list[str]:
    """Jobs of a state dir with no outcome, or whose state cannot be read
    (C4: they may be unfinished)."""
    store = JobStore(state_dir)
    busy = []
    for job_id in store.all_ids():
        try:
            st = store.state(job_id)
        except ValueError:
            continue
        if isinstance(st, Unreadable) or \
                (isinstance(st, dict) and not st.get("outcome")):
            busy.append(job_id)
    return busy


def cmd_config_migrate(args) -> int:
    """Operator-only (D8): rewrite a v1 machine config to v2 with the same
    effective values.  Refused while any job is unfinished; prints the
    diff and the side-by-side effective values; keeps a backup (0600)
    beside the config; appends the authorization record."""
    import difflib
    authorized = _authorization_text(args.authorized_by)
    if authorized is None:
        print("agent-compute: config migrate is an operator action: pass "
              "--authorized-by \"<who, when, what>\" (one line)",
              file=sys.stderr)
        return EXIT_NOT_ACCEPTED
    path = expand(args.config or os.environ.get(cfgmod.ENV_CONFIG) or
                  cfgmod.DEFAULT_CONFIG).absolute()
    try:
        text = path.read_text()
        raw = tomllib.loads(text)
    except (OSError, tomllib.TOMLDecodeError) as error:
        print(f"agent-compute: config migrate refused: cannot read {path}: "
              f"{error}", file=sys.stderr)
        return 1
    if raw.get("schema") == CONFIG_SCHEMA:
        out({"migrated": False, "config": str(path),
             "reason": f"already {CONFIG_SCHEMA}; nothing to do"}, args.json)
        return 0
    stamp = utc_now()
    try:
        new_text, summary = cfgmod.migrate_v1_text(text, stamp)
        before = cfgmod.v1_effective_values(raw)
    except ConfigError as error:
        print(f"agent-compute: config migrate refused: {error}",
              file=sys.stderr)
        return 1
    state_dir = Path(before["state_dir"])
    busy = _unfinished_in(state_dir)
    if busy:
        print(f"agent-compute: config migrate refused: unfinished jobs "
              f"{busy} (the migration needs an idle host: no job queued, "
              "waiting, running or unreadable)", file=sys.stderr)
        return 1
    try:
        migrated = _check_loads(path, new_text)
    except ConfigError as error:
        print(f"agent-compute: config migrate refused: {error}",
              file=sys.stderr)
        return 1
    after = cfgmod.effective_values(migrated)
    after["state_dir"] = str(migrated.state_dir)
    rows = cfgmod.compare_effective(before, after)
    problems = cfgmod.comparison_problems(rows)
    if problems:
        print("agent-compute: config migrate refused: effective values "
              f"would change: {problems}", file=sys.stderr)
        return 1
    diff = "".join(difflib.unified_diff(
        text.splitlines(keepends=True), new_text.splitlines(keepends=True),
        fromfile=f"{path} (v1)", tofile=f"{path} (v2)"))
    report = {"migrated": not args.dry_run, "config": str(path),
              "diff": diff, "diff_summary": summary, "effective": rows}
    if args.dry_run:
        report["backup"] = None
        _print_migration(report, args.json)
        return 0
    backup = path.with_name(f"{path.name}.pre-v2-"
                            f"{stamp.replace(':', '').replace('-', '')}")
    ident = identity.identity()
    host_id = _authorization_host_id(state_dir, migrated.host)
    record = {"schema": AUTHORIZATION_SCHEMA, "utc": stamp,
              "action": "config-migrate", "authorized_by": authorized,
              "config": str(path), "backup": str(backup),
              "from_schema": cfgmod.LEGACY_CONFIG_SCHEMA,
              "to_schema": CONFIG_SCHEMA, "diff_summary": summary,
              "effective": {"differ": [r for r in rows
                                       if r["verdict"] != "same"]},
              "version": ident["version"], "source_sha": ident["source_sha"],
              "host": migrated.host, "host_id": host_id}
    # A34: the record first (it names the backup about to be written); no
    # record, no migration
    try:
        report["authorization"] = str(append_authorization(state_dir,
                                                           record))
    except OSError as error:
        print(f"agent-compute: config migrate refused: the authorization "
              f"record could not be appended to {state_dir / AUTH_FILE} "
              f"({error}); the config is unchanged, no backup written",
              file=sys.stderr)
        return 1
    created = False
    try:
        fd = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        created = True
        with os.fdopen(fd, "w") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        write_config_atomic(path, new_text)
    except OSError as error:
        _record_failure(state_dir, record, "config-migrate-failed", error)
        print(f"agent-compute: config migrate failed: {error}; the config "
              "is unchanged; the authorization and this failure are "
              "recorded", file=sys.stderr)
        if created and path.read_text() == text:
            backup.unlink(missing_ok=True)      # ours, and not needed
        return 1
    report["backup"] = str(backup)
    report["record"] = record
    _print_migration(report, args.json)
    return 0


def cmd_config_init(args) -> int:
    """D4: print a fresh config for this host (neutral names, records in
    the state dir, detector none, host-derived profile defaults).  Writes
    nothing; K3 (A44): it reads, read-only, the other installations'
    configs (the default one, ``$AGENT_COMPUTE_CONFIG``) only to refuse a
    state dir they use."""
    import shutil
    import socket
    found = cfgmod.host_resources()
    if not found.get("memory_gb"):
        print("agent-compute: config init: this host's RAM cannot be read; "
              "write the profile numbers by hand", file=sys.stderr)
        return 1
    label = args.label or re.sub(r"[^A-Za-z0-9._-]", "-",
                                 socket.gethostname().split(".")[0]
                                 ).strip(".-")[:64] or "host"
    try:
        # K3 (A44): the state dir is absolute and belongs to the install
        # home this runs from (ACS_INSTALL_HOME, else the real home); one
        # that another installation's config already uses is refused
        _, own_config, state_dir = cfgmod.init_locations()
        clash = cfgmod.state_dir_conflict(state_dir, own_config)
        if clash:
            print(f"agent-compute: config init refused: {clash}",
                  file=sys.stderr)
            return EXIT_NOT_ACCEPTED
        default_home = Path(os.path.expanduser("~"))
        hint = ("~/" + cfgmod.INIT_CONFIG_REL
                if os.path.realpath(own_config) ==
                os.path.realpath(default_home / cfgmod.INIT_CONFIG_REL)
                else str(own_config))
        text = cfgmod.init_text(label, found, sys.executable,
                                shutil.which("pueue"), shutil.which("pueued"),
                                state_dir=str(state_dir), config_hint=hint)
    except ConfigError as error:
        print(f"agent-compute: config init: {error}", file=sys.stderr)
        return EXIT_NOT_ACCEPTED
    print(text, end="")
    return 0


def _print_migration(report: dict, as_json: bool) -> None:
    if as_json:
        out(report, True)
        return
    print(report["diff"], end="")
    print("effective values (v1 -> v2):")
    for row in report["effective"]:
        if row["verdict"] != "same":
            print(f"  {row['verdict']:9} {row['key']}: {row['before']!r} -> "
                  f"{row['after']!r}")
    same = sum(1 for r in report["effective"] if r["verdict"] == "same")
    print(f"  same      {same} other values")
    if report.get("backup"):
        print(f"migrated {report['config']}; backup {report['backup']}; "
              f"authorization recorded in {report.get('authorization')}")
    else:
        print("dry run: nothing written (no backup, no record)")


def write_config_atomic(path: Path, text: str) -> None:
    """Write beside ``path`` (0600), fsync, then rename over it."""
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp, 0o600)
        os.replace(temp, path)
    except BaseException:
        temp.unlink(missing_ok=True)
        raise


# --- hosts, placement and remote evidence (campaign G slice G3a) ------------------------

SUMMARY_SCHEMA = "agent-compute-summary/1"
HOSTS_SCHEMA = "agent-compute-hosts/1"
FETCH_SCHEMA = "agent-compute-fetch/1"
REQUEST_SCHEMA = "agent-compute-request/1"
CLAIMED_PHASES = ("dispatched", "dispatch-held", "admission-wait",
                  "launched", "executing", "cleanup")
RUNNING_PHASES = ("launched", "executing", "cleanup")
FETCH_MAX_BYTES = 256 * 1024 ** 2
GIT_READ_TIMEOUT_S = 20


def _git_read(path: Path, *args: str) -> subprocess.CompletedProcess | None:
    """A git query that writes nothing: no optional locks (no index
    refresh), no fsmonitor."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env["GIT_OPTIONAL_LOCKS"] = "0"
    try:
        return subprocess.run(["git", "--no-optional-locks", "-c",
                               "core.fsmonitor=false", "-C", str(path),
                               *args], capture_output=True, text=True,
                              timeout=GIT_READ_TIMEOUT_S, env=env,
                              stdin=subprocess.DEVNULL)
    except (OSError, subprocess.SubprocessError):
        return None


def checkout_view(root: str) -> dict:
    """A project root's checkout: HEAD and whether tracked files differ
    from it (untracked files are not checked).  A root pattern is not
    inspected (it names many worktrees)."""
    if any(c in root for c in "*?["):
        return {"root": root, "pattern": True, "inspected": False}
    path = expand(root)
    view = {"root": root, "path": str(path), "exists": path.is_dir()}
    if not view["exists"]:
        return view
    head = _git_read(path, "rev-parse", "HEAD")
    view["head"] = head.stdout.strip() if head is not None and \
        head.returncode == 0 else None
    status = _git_read(path, "status", "--porcelain", "--untracked-files=no")
    view["dirty"] = (bool(status.stdout.strip())
                     if status is not None and status.returncode == 0
                     else None)
    view["untracked"] = "not checked"
    return view


def host_summary(config: Config) -> dict:
    """``_summary``: this host as other hosts see it.  Strictly read-only:
    no lost-supervision detection or settling (what they would act on is
    reported as ``lost_suspected``), no host id creation, no event line,
    no state, queue or checkout write."""
    store = JobStore(config.state_dir)
    ident = acs_identity(config)
    floor = safety.running_floor_problems()
    block = store.block(config.host)
    gate = store.gate()
    daemon = {"enabled": config.queue_enabled, "running": None,
              "group": config.queue_group if config.queue_enabled else None,
              "group_status": None}
    groups = running_daemon = None
    if config.queue_enabled:
        try:
            running_daemon = q.daemon_running(config)
            if running_daemon:
                groups = q.status_json(config).get("groups", {})
        except (q.QueueError, OSError, subprocess.SubprocessError,
                ValueError) as error:
            daemon["error"] = str(error)
        daemon["running"] = running_daemon
        daemon["group_status"] = ((groups or {}).get(config.queue_group)
                                  or {}).get("status")
    by_phase: dict[str, int] = {}
    unreadable, suspected, claimed = [], [], []
    for job_id in store.all_ids():
        try:
            st = store.state(job_id)
            man = store.manifest(job_id)
        except ValueError:
            continue
        if isinstance(st, Unreadable):
            if isinstance(man, dict) and man.get("host") != config.host:
                continue
            unreadable.append(st.view())
            suspected.append({"job_id": job_id, "reason": f"the job state "
                              f"{st.reason}: it may hide a lost runner"})
            continue
        if not isinstance(st, dict) or st.get("outcome"):
            continue
        host = (st.get("host") or config.host) \
            if isinstance(man, Unreadable) else (man or {}).get("host")
        if host is not None and host != config.host:
            continue
        phase = st.get("phase") or "unknown"
        by_phase[phase] = by_phase.get(phase, 0) + 1
        runner = st.get("runner")
        if runner:
            if procs.liveness(runner) == procs.GONE:
                suspected.append({"job_id": job_id, "reason": "its runner is "
                                  "gone while the job is unfinished (lost "
                                  "supervision: the next status, wait or "
                                  "reconcile there blocks the host)"})
            else:
                claimed.append({"job_id": job_id,
                                "project": (man or {}).get("project")
                                if isinstance(man, dict) else None,
                                "phase": phase,
                                "dispatched_utc": st.get("dispatched_utc"),
                                "admitted_utc": st.get("admitted_utc")})
        elif st.get("pueue_task") is None and \
                _elapsed(st.get("accepted_utc"), utc_now()) is not None and \
                _elapsed(st.get("accepted_utc"), utc_now()) >= \
                UNCLAIMED_GRACE_S:
            suspected.append({"job_id": job_id, "reason": "accepted but "
                              "never enqueued nor claimed past the grace "
                              "(the next status there finishes it as never "
                              "started)"})
    running = next((c for c in claimed if c["phase"] in RUNNING_PHASES),
                   claimed[0] if claimed else None)
    engine = identity.engine_sha256()
    return {
        "schema": SUMMARY_SCHEMA, "read_only": True,
        "host": config.host, "host_id": ident["host_id"],
        "version": ident["version"], "source_sha": ident["source_sha"],
        "package_version": ident["package_version"],
        "safety_level": ident["safety_level"],
        "engine_sha256": engine, "utc": ident["utc"],
        "floor": {"ok": None if floor is None else not floor,
                  "problems": floor or [],
                  "note": "a checkout: not an installed copy"
                  if floor is None else None},
        "pin": {"ok": engine == config.guard_sha256,
                "pinned": config.guard_sha256, "engine_sha256": engine},
        "blocked": "unreadable" if isinstance(block, Unreadable)
        else bool(block),
        "block_jobs": sorted((block.get("jobs") or {}))
        if isinstance(block, dict) else None,
        "gate": "unreadable" if isinstance(gate, Unreadable)
        else "closed" if gate else "open",
        "gate_reason": gate.reason if isinstance(gate, Unreadable)
        else gate.get("reason") if isinstance(gate, dict) else None,
        "dispatch": dispatch_state(config, store, groups, running_daemon),
        "daemon": daemon,
        "inflight_marker": inflight_view(config),
        "jobs": {"unfinished": sum(by_phase.values()) + len(unreadable),
                 "by_phase": by_phase, "unreadable": unreadable},
        "running": running, "claimed": claimed,
        "lost_suspected": suspected,
        "budget": {"effective_budget_gb": config.effective_budget_gb,
                   "dedicated_budget_gb": config.dedicated_budget_gb,
                   "profile_budget_gb": config.profile.get("budget_gb"),
                   "floor_gb": config.profile.get("floor_gb"),
                   "container_cpus": config.profile.get("container_cpus")},
        "projects": {name: {"roots": list(project.roots),
                            "hosts": list(project.hosts or [config.host]),
                            "on": dict(project.on),
                            "containers": list(project.containers),
                            "checkouts": [checkout_view(root)
                                          for root in project.roots]}
                     for name, project in config.projects.items()},
        "remotes": sorted(config.remotes),
    }


def cmd_summary(config: Config, args) -> int:
    out(host_summary(config), args.json)
    return 0


def _summary_reply(name: str, reply) -> tuple[str, dict | None, list]:
    """(state, summary, reasons) of one host's ``_summary`` reply."""
    if reply.error is not None:
        return "error", None, [f"the route could not be used: {reply.error}"]
    if reply.timed_out:
        return "timeout", None, [f"no reply within {reply.timeout_s:g} s"]
    if reply.rc == remote_mod.EXIT_ROUTE:
        return "unreachable", None, ["ssh exited 255: unreachable, or the "
                                     "connection dropped"]
    if reply.rc not in (None, 0) and "invalid choice" in reply.stderr \
            and "'_summary'" in reply.stderr:
        return "old-remote", None, [
            "the remote ACS has no `_summary` (a version older than 0.2.0, "
            "campaign G slice G3a): ineligible until it runs 0.2.0"]
    if reply.rc != 0:
        last = (reply.stderr.strip().splitlines() or [""])[-1][:200]
        return "error", None, [f"`_summary` exited {reply.rc}: {last}"]
    try:
        data = json.loads(reply.stdout)
    except ValueError:
        data = None
    if not isinstance(data, dict) or data.get("schema") != SUMMARY_SCHEMA:
        return "unparseable", None, ["exit 0 without a summary object"]
    return "ok", data, []


def _summary_reasons(name: str, summary: dict) -> list[str]:
    """Why a host that answered cannot take work now (D5's candidate
    conditions that do not depend on a project; the placement policy,
    ``placement.evaluate``, adds the per-project ones)."""
    return [reason for _, reason in pl.summary_problems(name, summary)]


def cmd_hosts(config: Config, args) -> int:
    """Every configured host: this one in-process, the ``[hosts]`` entries
    over their routes in parallel, each within its call timeout.
    Read-only there; nothing is chosen.  Here, only the resolution of open
    placement records writes (G3b: by request id, on their own hosts)."""
    from concurrent.futures import ThreadPoolExecutor
    origin = routed_from()
    # A40: a call that arrived routed probes no other host
    names = [] if origin else sorted(config.remotes)
    with ThreadPoolExecutor(max_workers=max(1, len(names))) as pool:
        futures = {name: pool.submit(remote_mod.call, config, name,
                                     ["_summary", "--json"], None, True)
                   for name in names}
        local = host_summary(config)
        replies = {name: future.result() for name, future in futures.items()}
    entries = [{"name": config.host, "local": True, "route": None,
                "state": "ok", "reachable": True, "summary": local,
                "reasons": _summary_reasons(config.host, local),
                "raw": None}]
    for name in names:
        reply = replies[name]
        state, data, reasons = _summary_reply(name, reply)
        if data is not None:
            reasons += _summary_reasons(name, data)
        entries.append({"name": name, "local": False,
                        "route": config.remotes[name].view(),
                        "state": state,
                        "reachable": state in ("ok", "old-remote",
                                               "unparseable", "error")
                        and reply.rc not in (None, remote_mod.EXIT_ROUTE),
                        "summary": data, "reasons": reasons,
                        "raw": reply.as_record()})
    for name in sorted(config.remotes) if origin else []:
        entries.append({"name": name, "local": False,
                        "route": config.remotes[name].view(),
                        "state": "not-probed", "reachable": None,
                        "summary": None,
                        "reasons": [f"not probed: this call arrived routed "
                                    f"from {origin} (register A40)"],
                        "raw": None})
    problems = []
    by_id: dict[str, list[str]] = {}
    for item in entries:
        host_id = (item["summary"] or {}).get("host_id")
        if host_id:
            by_id.setdefault(host_id, []).append(item["name"])
    for host_id, group in by_id.items():
        if len(group) < 2:
            continue
        problems.append(f"{', '.join(group)} report the same host id "
                        f"{host_id}: one installation reached under "
                        f"{len(group)} names")
        for item in entries:
            if item["name"] in group:
                others = [g for g in group if g != item["name"]]
                item["reasons"].append(f"the same host id as "
                                       f"{', '.join(others)}")
    for item in entries:
        if item["summary"] and item["summary"].get("host") != item["name"]:
            problems.append(f"{item['name']} reports host "
                            f"{item['summary'].get('host')!r}")
        item["eligible"] = item["state"] == "ok" and not item["reasons"]
    # G3b: the open placement records are resolved by request id on their
    # own hosts (read-only there); a host this probe found unreachable is
    # not asked again
    resolutions = resolve_placements(
        config, None, {item["name"] for item in entries
                       if not item["local"] and item["state"] != "ok"})
    report = {"schema": HOSTS_SCHEMA, "acs": acs_identity(config),
              "hosts": entries, "identity_problems": problems,
              "resolutions": resolutions,
              "placements_open": [pl.view(r) for r in
                                  pl.records(config.state_dir)
                                  if isinstance(r, Unreadable) or
                                  r.get("phase") in pl.OPEN]}
    if args.json:
        out(report, True)
        return 0
    for item in entries:
        s = item["summary"] or {}
        jobs = (s.get("jobs") or {}).get("unfinished")
        running = (s.get("running") or {}).get("job_id")
        print(f"{item['name']}{' (this host)' if item['local'] else ''}: "
              f"{item['state']}, {'eligible' if item['eligible'] else 'not eligible'}"
              + (f"; id {s.get('host_id')}, {s.get('version')}, blocked "
                 f"{s.get('blocked')}, gate {s.get('gate')}, unfinished "
                 f"{jobs}, running {running}" if s else "")
              + (f"; {'; '.join(item['reasons'])}" if item["reasons"]
                 else ""))
    for problem in problems:
        print(f"identity: {problem}")
    for item in resolutions:
        print(f"resolution: {item['request_id']} -> {item['host']}: "
              f"{item['before']} -> {item['after']}: {item['reason']}")
    for record in report["placements_open"]:
        print(f"open placement: {record.get('request_id')} -> "
              f"{record.get('host')} ({record.get('phase')})")
    return 0


# -- status by request id, placement listing ---------------------------------------

def request_report(config: Config, request_id: str, lost=None, tasks=None,
                   groups=None, daemon=None) -> dict:
    """``status --request-id R``: the binding and every job of this host
    whose records name ``R`` (C1: the binding alone is not proof)."""
    store = JobStore(config.state_dir)
    binding = store.request_job(request_id)
    jobs = []
    for job_id in store.all_ids():
        try:
            man, st = store.manifest(job_id), store.state(job_id)
        except ValueError:
            continue
        named = {r.get("request_id") for r in (man, st)
                 if isinstance(r, dict)}
        if request_id in named or job_id == binding:
            jobs.append(job_view(config, job_id, tasks))
    record = pl.read(config.state_dir, request_id)
    return {"schema": REQUEST_SCHEMA, "request_id": request_id,
            "host": config.host,
            "binding": record_view(binding) if binding is not None
            else None,
            "found": bool(jobs) or isinstance(binding, str), "jobs": jobs,
            "placement": pl.view(record) if record is not None else None,
            "acs": acs_identity(config)}


def placement_listing(config: Config, everything: bool) -> list[dict]:
    return [pl.view(r) for r in pl.records(config.state_dir)
            if everything or isinstance(r, Unreadable) or
            r.get("phase") in pl.OPEN]


# -- lost-reply resolution by request id (G3b, D6) ----------------------------------------

LOOKUP_SCHEMA = "agent-compute-request-lookup/1"
RESOLVE_SCHEMA = "agent-compute-placements-resolve/1"


def request_lookup(config: Config, request_id: str) -> dict:
    """``_request R``: whether this host bound or recorded request id
    ``R``, strictly read-only (no lost-supervision detection, no queue
    call, no host id creation, no write): the binding and every job whose
    records name ``R`` (C1: the binding alone is not proof).  ``found``:
    True (a job, ``job_id``), ``"pending"`` (only a reservation in
    progress: a state without a manifest or an outcome), None (absence
    not provable: an unreadable binding, or an unreadable job record that
    may name it, C4), False (none)."""
    store = JobStore(config.state_dir)
    try:
        binding = store.request_job(request_id)
    except ValueError:
        binding = None
    jobs, unreadable = [], []
    if isinstance(binding, Unreadable):
        unreadable.append(binding.view())
    for job_id in store.all_ids():
        try:
            man, st = store.manifest(job_id), store.state(job_id)
        except ValueError:
            continue
        readable = [r for r in (man, st) if isinstance(r, dict)]
        named = [r.get("request_id") for r in readable if "request_id" in r]
        if request_id in named or job_id == binding:
            state = st if isinstance(st, dict) else {}
            jobs.append({"job_id": job_id, "phase": state.get("phase"),
                         "outcome": state.get("outcome"),
                         "accepted_utc": state.get("accepted_utc"),
                         "reserving": man is None and
                         not state.get("outcome")})
        elif not named and any(isinstance(r, Unreadable) for r in (man, st)):
            unreadable += [r.view() for r in (man, st)
                           if isinstance(r, Unreadable)]
    settled = [j for j in jobs if not j["reserving"]]
    if settled:
        found = True
        job = binding if isinstance(binding, str) and \
            any(j["job_id"] == binding for j in settled) else \
            settled[0]["job_id"]
    else:
        job = None
        found = "pending" if jobs else None if unreadable else False
    return {"schema": LOOKUP_SCHEMA, "read_only": True,
            "request_id": request_id, "host": config.host,
            "host_id": _host_id_quiet(config), "found": found,
            "job_id": job, "jobs": jobs,
            "binding": record_view(binding) if binding is not None
            else None,
            "unreadable": unreadable, "acs": acs_identity(config)}


def cmd_request(config: Config, args) -> int:
    if not REQUEST_ID.fullmatch(args.request_id):
        print(f"agent-compute: request id {args.request_id!r} is not valid",
              file=sys.stderr)
        return EXIT_NOT_ACCEPTED
    out(request_lookup(config, args.request_id), args.json)
    return 0


def _lookup_reply(reply) -> tuple[str, dict | None, str | None]:
    """(state, the lookup, why not) of a ``_request`` reply."""
    if reply.error is not None:
        return "unreachable", None, f"the route could not be used " \
                                    f"({reply.error})"
    if reply.timed_out:
        return "unreachable", None, f"no reply within {reply.timeout_s:g} s"
    if reply.rc == remote_mod.EXIT_ROUTE:
        return "unreachable", None, ("ssh exited 255 (unreachable, or the "
                                     "connection dropped)")
    if reply.rc != 0:
        last = (reply.stderr.strip().splitlines() or [""])[-1][:200]
        return "unanswered", None, (f"`_request` exited {reply.rc} (an ACS "
                                    f"without it is older than G3b): {last}")
    obj = _reply_object(reply.stdout)
    if obj is None or obj.get("schema") != LOOKUP_SCHEMA:
        return "unanswered", None, "exit 0 without a lookup object"
    return "ok", obj, None


def resolve_placements(config: Config, only=None,
                       unreachable=()) -> list[dict]:
    """G3b (D6): resolve the OPEN placement records (all of them, or those
    whose request id is in ``only``) by querying the SAME host by request
    id (``_request``, read-only there; this host's own records in-process):
    found -> ``accepted`` (with the job); not found, the host reachable and
    answering, at least the grace after the send -> ``resolved-not-accepted``
    (the agent may submit again; ACS never does); anything else leaves the
    record as it is.  Nothing is ever sent but the query.  ``unreachable``:
    hosts this run already found unreachable (not asked again)."""
    if not config.state_dir.is_dir() or routed_from():
        return []
    targets = [r for r in pl.records(config.state_dir)
               if isinstance(r, dict) and r.get("phase") in pl.OPEN and
               (only is None or r.get("request_id") in only)]
    if not targets:
        return []

    def query(record):
        host = record.get("host")
        if host == config.host:
            return "ok", request_lookup(config, record["request_id"]), \
                None, None
        if host not in config.remotes:
            return "no-route", None, (f"this config no longer routes "
                                      f"{host!r}"), None
        if host in unreachable:
            return "unreachable", None, ("its probe in this command found "
                                         "it unusable (not asked again)"), \
                None
        reply = remote_mod.call(config, host,
                                ["_request", record["request_id"], "--json"],
                                None, True)
        state, obj, why = _lookup_reply(reply)
        return state, obj, why, reply

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=min(8, len(targets))) as pool:
        answers = list(pool.map(query, targets))
    return [_settle(config, record, *answer)
            for record, answer in zip(targets, answers)]


def _settle(config: Config, record: dict, state: str, obj, why,
            reply) -> dict:
    request_id, host = record["request_id"], record.get("host")
    before = record.get("phase")
    sent = pl.sent_utc(record)
    route = config.remotes.get(host)
    grace = pl.grace_s(route.reply_grace_s if route else None, record,
                       remote_mod.CALL_TIMEOUT_S if host != config.host
                       else 0.0)
    # review round 1: the clock starts at the send (an unbounded send: at
    # its client's end)
    start = pl.grace_start(record)
    age = pl._age_s(start)
    raw = reply.as_record() if reply is not None else \
        {"in_process": True, "lookup": obj} if obj is not None else None
    result = {"request_id": request_id, "host": host, "before": before,
              "after": before, "found": None, "job_id": None,
              "sent_utc": sent, "age_s": age, "grace_s": grace,
              "reason": None, "raw": raw}
    if state != "ok":
        result["reason"] = f"{state}: {why}; it stays {before}"
        return result
    if obj.get("host") != host:
        result["reason"] = (f"the reply names host {obj.get('host')!r}, not "
                            f"{host!r}; it stays {before}")
        return result
    found = obj.get("found")
    result["found"] = found
    phase = fields = None
    if found is True and obj.get("job_id"):
        job = obj["job_id"]
        result["job_id"] = job
        phase = "accepted"
        reason = (f"{host} has request id {request_id} bound to {job} "
                  "(found by request id)")
        fields = {"remote_job_id": job}
    elif found is False:
        blocker = _time_blocker(config, record)
        if blocker:
            result["reason"] = (f"{host} does not know it, but {blocker}; "
                                f"it stays {before}")
            return result
        since = "its client ended" if pl.unbounded(record) else "the send"
        if age is None:
            result["reason"] = (f"{host} does not know it, but the time of "
                                f"{since} is unknown; it stays {before}")
            return result
        if age < grace:
            result["reason"] = (f"{host} does not know it yet, {age:.0f} s "
                                f"after {since}; the grace ({grace:g} s) "
                                f"ends in {grace - age:.0f} s; it stays "
                                f"{before}")
            return result
        phase = "resolved-not-accepted"
        reason = (f"{host} answered that it has no job for request id "
                  f"{request_id}, {age:.0f} s after {since} (grace "
                  f"{grace:g} s; the sender and its ssh client are gone): "
                  "never accepted there.  The agent may "
                  f"submit it again (to {host} with this id, or under a new "
                  "id); ACS never does")
        fields = {}
    elif found == "pending":
        result["reason"] = (f"{host} is reserving it right now (a job state "
                            f"without a manifest); it stays {before}")
        return result
    else:
        result["reason"] = (f"{host} cannot prove its absence (unreadable "
                            "records there: "
                            + "; ".join(u.get("reason") or "?" for u in
                                        obj.get("unreadable") or [])
                            + f"); it stays {before}")
        return result
    if not record.get("destination"):
        fields["destination"] = {
            "host": host, "host_id": obj.get("host_id"),
            "version": (obj.get("acs") or {}).get("version"),
            "source_sha": (obj.get("acs") or {}).get("source_sha"),
            "head": None, "code": None}
    try:
        written = pl.resolve(
            config.state_dir, request_id, phase, reason,
            raw if raw is not None else None,
            {"query": "_request", "host": host,
             "host_id": obj.get("host_id"), "found": found,
             "job_id": result["job_id"], "sent_utc": sent, "age_s": age,
             "grace_s": grace}, **fields)
    except (OSError, ValueError) as error:
        result["reason"] = (f"{reason}; but the record could not be written "
                            f"({error}); it stays {before}")
        return result
    if written is None:
        current = pl.read(config.state_dir, request_id)
        result["after"] = current.get("phase") if isinstance(current, dict) \
            else "unreadable"
        result["reason"] = f"{reason}; the record was settled meanwhile"
        return result
    result.update(after=phase, reason=reason)
    return result


def _time_blocker(config: Config, record: dict) -> str | None:
    """Review round 1: why "not found" may not resolve a record by time
    now: the placing process, or the send's ssh client, is not proven gone
    (their start-time-proven identities; a pid now held by a later process
    is gone, anything unprovable blocks), or an unbounded send is still
    recorded as sent.  None when nothing blocks."""
    placer = record.get("placer")
    if isinstance(placer, dict) and placer.get("pid") != os.getpid():
        state = procs.liveness(placer)
        if state != procs.GONE:
            return (f"the placing process (pid {placer.get('pid')}) is "
                    f"{state}: its own send may still be in progress")
    send = record.get("send") if isinstance(record.get("send"), dict) \
        else {}
    client = send.get("client")
    if isinstance(client, dict):
        state = procs.liveness(client)
        if state != procs.GONE:
            return (f"the send's ssh client (pid {client.get('pid')}) is "
                    f"{state}: the command may still reach "
                    f"{record.get('host')}")
    if pl.unbounded(record) and record.get("phase") == "sent":
        return ("its send is an unbounded `run` still recorded as sent: "
                "only an answer that finds the job resolves it")
    return None


def _report_resolutions(results: list[dict]) -> None:
    for item in results:
        if item["after"] != item["before"]:
            print(f"agent-compute: placement {item['request_id']} -> "
                  f"{item['host']}: {item['before']} -> {item['after']}"
                  + (f" ({item['job_id']})" if item.get("job_id") else "")
                  + f": {item['reason']}", file=sys.stderr)


def cmd_placements(config: Config, args) -> int:
    """``placements resolve [R]``: resolve the open placement records (or
    R's) by request id, on their own hosts (read-only there)."""
    if routed_from():
        print(f"agent-compute: this call arrived routed from "
              f"{routed_from()}: it asks no other host (register A40); "
              "nothing was asked", file=sys.stderr)
        return EXIT_NOT_ACCEPTED
    only = None
    if args.request_id:
        try:
            record = pl.read(config.state_dir, args.request_id)
        except ValueError:
            record = None
        if record is None:
            print(f"agent-compute: no placement record for request id "
                  f"{args.request_id!r} on this host", file=sys.stderr)
            return EXIT_NOT_ACCEPTED
        if isinstance(record, Unreadable):
            print(f"agent-compute: the placement record {record.reason}; an "
                  "operator inspects it; nothing was asked", file=sys.stderr)
            return EXIT_NOT_ACCEPTED
        only = {args.request_id}
    results = resolve_placements(config, only)
    report = {"schema": RESOLVE_SCHEMA, "resolutions": results,
              "placements": [pl.view(r) for r in pl.records(config.state_dir)
                             if only is None or (isinstance(r, dict) and
                                                 r.get("request_id") in only)],
              "acs": acs_identity(config)}
    if args.json:
        out(report, True)
        return 0
    for item in results:
        print(f"{item['request_id']} -> {item['host']}: {item['before']} -> "
              f"{item['after']}: {item['reason']}")
    if not results:
        print("no open placement record" + (f" for {args.request_id}"
                                            if only else ""))
    return 0


# -- remote evidence: log and fetch --------------------------------------------------

def cmd_log(config: Config, args) -> int:
    store = JobStore(config.state_dir)
    if store.manifest(args.job) is None and store.state(args.job) is None:
        print(f"agent-compute: unknown job {args.job}", file=sys.stderr)
        return EXIT_NOT_ACCEPTED
    path = store.job_dir(args.job) / "output.log"
    try:
        text = path.read_text(errors="replace")
    except FileNotFoundError:
        print(f"agent-compute: {args.job} has no output.log yet ({path})",
              file=sys.stderr)
        return 1
    except OSError as error:
        print(f"agent-compute: {path} is unreadable: {error}",
              file=sys.stderr)
        return 1
    if args.tail is not None:
        lines = text.splitlines(keepends=True)
        text = "".join(lines[-args.tail:]) if args.tail > 0 else ""
    sys.stdout.write(text)
    return 0


def _evidence_files(store: JobStore, job_id: str) -> list[Path]:
    """The job's evidence files (relative to its directory): manifest,
    state, output.log, and the guard records (``guard/**``); regular
    files only."""
    root = store.job_dir(job_id)
    found = [Path(name) for name in ("manifest.json", "state.json",
                                     "output.log")
             if is_file_strict(root / name)
             and not is_symlink_strict(root / name)]
    guard = root / "guard"
    if is_dir_strict(guard):
        # A49: a directory that cannot be listed raises (rglob skips it)
        for path in sorted(walk_files_strict(guard)):
            if is_file_strict(path) and not is_symlink_strict(path):
                found.append(path.relative_to(root))
    return found


def fetch_bundle(config: Config, job_id: str) -> dict:
    """``_fetch``: the job's evidence files as one gzip tar (base64), with
    each file's size and sha256.  Read-only."""
    import base64
    import hashlib
    import io
    import tarfile
    store = JobStore(config.state_dir)
    if store.manifest(job_id) is None and store.state(job_id) is None:
        raise ConfigError(f"unknown job {job_id}")
    root = store.job_dir(job_id)
    files, total = [], 0
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for rel in _evidence_files(store, job_id):
            data = (root / rel).read_bytes()
            total += len(data)
            if total > FETCH_MAX_BYTES:
                raise ConfigError(f"the evidence of {job_id} exceeds "
                                  f"{FETCH_MAX_BYTES // 1024 ** 2} MB; copy "
                                  "it with the route's own tools")
            info = tarfile.TarInfo(str(rel))
            info.size, info.mode = len(data), 0o600
            info.mtime = int((root / rel).stat().st_mtime)
            tar.addfile(info, io.BytesIO(data))
            files.append({"path": str(rel), "size": len(data),
                          "sha256": hashlib.sha256(data).hexdigest()})
    return {"schema": FETCH_SCHEMA, "job_id": job_id, "host": config.host,
            "source": str(root), "files": files,
            "archive_b64": base64.b64encode(buffer.getvalue()).decode()}


def cmd_fetch_bundle(config: Config, args) -> int:
    print(json.dumps(fetch_bundle(config, args.job), sort_keys=True))
    return 0


def unpack_bundle(bundle: dict, job_id: str, dest: Path) -> dict:
    """Write a fetched bundle under ``dest/<job>`` (never over an existing
    directory); every member is checked against its listed sha256."""
    import base64
    import hashlib
    import io
    import tarfile
    if bundle.get("schema") != FETCH_SCHEMA or \
            bundle.get("job_id") != job_id:
        raise ConfigError("the reply is not this job's evidence bundle")
    target = Path(dest).expanduser().absolute() / job_id
    if target.exists():
        raise ConfigError(f"{target} exists; fetch never overwrites")
    listed = {f["path"]: f for f in bundle.get("files") or []}
    data = base64.b64decode(bundle.get("archive_b64") or "")
    target.mkdir(parents=True, mode=0o700)
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
        for member in tar.getmembers():
            rel = Path(member.name)
            if not member.isfile() or rel.is_absolute() or \
                    ".." in rel.parts or member.name not in listed:
                raise ConfigError(f"refusing archive member {member.name!r}")
            content = tar.extractfile(member).read()
            if hashlib.sha256(content).hexdigest() != \
                    listed[member.name]["sha256"]:
                raise ConfigError(f"{member.name}: sha256 mismatch")
            path = target / rel
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as handle:
                handle.write(content)
    return {"fetched": True, "job_id": job_id, "host": bundle.get("host"),
            "source": bundle.get("source"), "dir": str(target),
            "files": bundle.get("files")}


def cmd_fetch(config: Config, args, host: str | None = None,
              route_view: dict | None = None) -> int:
    """``fetch JOB DIR``: the job's evidence into ``DIR/JOB``, from this
    host's state or, with a host, over its route (``_fetch`` there)."""
    # review round 1 (note 10): the job id names the directory written
    # here; checked before any call (ValueError: exit 2)
    JobStore(config.state_dir).job_dir(args.job)
    if host is None:
        bundle = fetch_bundle(config, args.job)
    else:
        reply = remote_mod.call(config, host, ["_fetch", args.job])
        if reply.rc != 0:
            sys.stderr.write(reply.stderr)
            remote_mod.explain(reply)
            return remote_mod.exit_code(reply) or 1
        try:
            bundle = json.loads(reply.stdout)
        except ValueError:
            print(f"agent-compute: {host} replied without an evidence "
                  "bundle; nothing written", file=sys.stderr)
            return 1
    result = unpack_bundle(bundle, args.job, Path(args.dir))
    if route_view is not None:
        result["route"] = route_view
    out(result, args.json)
    return 0


# -- --host X submit: the placement record (pinned) ------------------------------------

def _insert_request_id(passthrough: list[str], command: str,
                       request_id: str) -> list[str]:
    index = passthrough.index(command) + 1
    return passthrough[:index] + ["--request-id", request_id] + \
        passthrough[index:]


def _reply_object(text: str) -> dict | None:
    try:
        data = json.loads(text)
    except ValueError:
        return None
    return data if isinstance(data, dict) else None


def placement_outcome(reply, obj: dict | None,
                      command: str = "submit") -> tuple[str, str, str | None]:
    """(phase, reason, the destination's job id) of a remote submit (or
    run) reply (``placement.py``: what each phase means).  A ``run`` reply
    is the job's end (the ``wait`` view, any exit code) once the
    destination's own exit code arrived (the in-band trailer)."""
    if reply.rc == 0 and obj is not None and obj.get("job_id") and \
            obj.get("accepted") is True:
        return "accepted", "the destination accepted it", obj["job_id"]
    if command == "run" and reply.rc_source == "remote" and \
            obj is not None and obj.get("job_id") and \
            "accepted" not in obj:
        return ("accepted", f"the destination ran it ({obj.get('outcome')})",
                obj["job_id"])
    if reply.rc == EXIT_NOT_ACCEPTED and obj is not None:
        if obj.get("job_id"):
            return ("accepted", "the destination bound the request id to "
                    f"{obj['job_id']} ({obj.get('accepted')}: "
                    f"{obj.get('reason')})", obj["job_id"])
        if obj.get("accepted") is False:
            return "refused", f"refused there: {obj.get('reason')}", None
    if reply.rc == EXIT_NOT_ACCEPTED and obj is None and \
            not reply.stdout.strip() and not reply.timed_out and \
            reply.stderr.lstrip().startswith(("agent-compute:", "usage:")):
        return ("refused", "the destination's CLI refused it before "
                "acceptance (exit 2, no reply object)", None)
    if reply.timed_out:
        why = f"no reply within {reply.timeout_s:g} s"
    elif reply.error is not None:
        why = f"the route could not be used ({reply.error})"
    elif reply.rc == remote_mod.EXIT_ROUTE:
        why = "ssh exited 255 (unreachable, or the connection dropped)"
    else:
        why = f"exit {reply.rc} without a usable reply object"
    return "reply-lost", why + "; the destination may have accepted it", None


class BoundElsewhere:
    """``_reserve``'s answer when a placement record binds the request id
    to another host (or cannot be read)."""

    def __init__(self, record):
        self.record = record

    def reason(self, state_dir: Path, request_id: str) -> str:
        record = self.record
        where = record.reason if isinstance(record, Unreadable) else \
            f"host {record.get('host')!r} ({record.get('phase')})"
        return (f"request id {request_id!r} is bound to {where} by the "
                f"placement record {pl.record_path(state_dir, request_id)}; "
                "it is never submitted on another host (D6); never started")


def host_binding_refusal(config: Config, request_id: str) -> str | None:
    """Register A39 (review round 1): why a request id this host bound to
    a job may not be sent to another host; None when it may (unbound, or
    its job provably never started).  An unreadable binding refuses
    (C4)."""
    store = JobStore(config.state_dir)
    try:
        bound = store.request_job(request_id)
    except ValueError:
        return None
    if bound is None:
        return None
    if isinstance(bound, Unreadable):
        return (f"request id {request_id!r}: its binding on this host "
                f"{bound.reason}; it may name a job that ran; an operator "
                "inspects it; nothing was sent")
    if _request_reusable(store, bound):
        return None
    st = store.state(bound)
    what = st.get("outcome") or st.get("phase") if isinstance(st, dict) \
        else "state unreadable" if st is not None else "no state"
    return (f"request id {request_id!r} is bound on this host "
            f"({config.host}) to {bound} ({what}), which may have run: it is "
            "never submitted on another host (D6); inspect that job instead; "
            "nothing was sent")


def _refuse_remote(args, request_id: str, reason: str, **extra) -> int:
    out({"accepted": False, "request_id": request_id, "reason": reason,
         **extra}, args.json)
    print(f"agent-compute: {reason}", file=sys.stderr)
    return EXIT_NOT_ACCEPTED


def record_refusal(config: Config, request_id: str,
                   host: str | None, resolve: bool = True) -> dict | None:
    """G3b: what an existing placement record of ``request_id`` says about
    sending it now.  None when there is none, or when it allows the send
    (``refused`` without a job: proven never reserved; or
    ``resolved-not-accepted`` by ``host`` itself: the same-host re-send);
    else the refusal (``reason``; ``host`` and ``job_id`` when the record
    names them).  ``host`` None: an automatic placement, which never places
    a request id that a record binds.  ``resolve``: an open record is first
    resolved by request id on its host (False: only the reason)."""
    try:
        present = is_dir_strict(config.state_dir)
    except OSError as error:
        # A49: not "no record": a record binding this request id to
        # another host may be there
        return {"reason": f"request id {request_id!r}: the state dir "
                          f"{config.state_dir} cannot be examined "
                          f"({error.strerror or error}); a placement record "
                          "may bind it; nothing was sent"}
    if not present:
        return None
    try:
        record = pl.read(config.state_dir, request_id)
    except ValueError:
        return None
    if record is None or pl.frees(record):
        return None
    path = pl.record_path(config.state_dir, request_id)
    if isinstance(record, Unreadable):
        return {"reason": f"request id {request_id!r}: its placement record "
                          f"{record.reason}; it may name a job that ran: it "
                          "is not sent anywhere until an operator inspects "
                          "it; nothing was sent"}
    if resolve and record.get("phase") in pl.OPEN:
        # a lost reply is resolved by request id first (on its own host,
        # read-only there), never re-sent blind
        resolve_placements(config, {request_id})
        record = pl.read(config.state_dir, request_id)
        if not isinstance(record, dict):
            return {"reason": f"request id {request_id!r}: its placement "
                              "record cannot be read after its resolution; "
                              "an operator inspects it; nothing was sent"}
        if pl.frees(record):
            return None
    bound, phase = record.get("host"), record.get("phase")
    if phase == "resolved-not-accepted" and host is not None and \
            host == bound:
        return None
    if phase == "accepted":
        job = record.get("remote_job_id")
        return {"host": bound, "job_id": job,
                "reason": f"request id {request_id!r} already maps to {job} "
                          f"on {bound} (placement record {path}); inspect it "
                          "instead of resubmitting; nothing was sent"}
    if phase == "resolved-not-accepted":
        return {"host": bound,
                "reason": f"request id {request_id!r} was resolved not "
                          f"accepted by {bound} (placement record {path}): "
                          f"it may be sent again to {bound} only (`--host "
                          f"{bound}`), or submit it under a new request id; "
                          "nothing was sent"}
    return {"host": bound,
            "reason": f"request id {request_id!r} is bound to {bound} by its "
                      f"placement record {path} ({phase}: "
                      f"{record.get('reason')}): {bound} may have accepted "
                      f"it.  `agent-compute placements resolve {request_id}` "
                      f"asks {bound} by request id; until that resolves it, "
                      "it is not sent again, and never to another host; "
                      "nothing was sent"}


def local_binding_refusal(config: Config, request_id: str) -> dict | None:
    """A request id this host bound to a job that may have run (or whose
    binding cannot be read): an automatic placement is refused with that
    job, wherever it would go (A39 for another host; the local dedupe
    for this one)."""
    reason = host_binding_refusal(config, request_id)
    if not reason:
        return None
    bound = JobStore(config.state_dir).request_job(request_id)
    return {"reason": reason, "host": config.host,
            "job_id": bound if isinstance(bound, str) else None}


def _render_remote_reply(obj: dict, as_json: bool, note: dict,
                         command: str) -> None:
    if as_json:
        out({**obj, "placement": note}, True)
    elif command == "run" and isinstance(obj.get("job"), dict):
        line = held_line(obj)
        if line:
            print(line)
        out(obj["job"], False)
    else:
        out(obj, False)
        if command == "submit" and obj.get("job_id"):
            print(obj["job_id"])        # G2a: the job id is the last line


def _env_options_check(config: Config, args) -> None:
    for item in args.env:
        name, sep, _ = item.partition("=")
        problem = env_problem(name, config.marker_aliases) if sep and name \
            else f"--env expects NAME=VALUE, got {item!r}"
        if problem:
            raise ConfigError(f"--env {problem}")


def _set_option(tokens: list[str], verb: str, name: str, value,
                flag: bool = False) -> list[str]:
    """``tokens`` with every ``name V``/``name=V`` (``name`` for a flag)
    after the subcommand removed, and ``name value`` (``name``: a flag set)
    put right after the subcommand; ``value`` None or False: removed
    only."""
    index = tokens.index(verb) + 1
    kept, skip = [], False
    for token in tokens[index:]:
        if skip:
            skip = False
            continue
        if token == name:
            skip = not flag
            continue
        if token.startswith(name + "="):
            continue
        kept.append(token)
    added = [] if value is None or value is False else \
        [name] if flag else [name, str(value)]
    return tokens[:index] + added + kept


def _local_project(config: Config, args):
    """``(project, cwd, worktree)`` when ``--cwd`` names, on this host, a
    directory of a project configured here; else None."""
    try:
        cwd = expand(args.cwd).resolve()
        if not cwd.is_dir():
            return None
        worktree = worktree_of(cwd)
        project = config.resolve_project(worktree, args.project)
    except (ConfigError, OSError, ValueError):
        return None
    return project, cwd, worktree


def job_mapping(project, host: str, cwd: Path, worktree: Path,
                args) -> dict:
    """G3b: the job's values on ``host`` from ``projects.<p>.on.<host>``:
    ``cwd`` (the mapped checkout, plus the cwd's path inside its worktree
    here), and, when the job names a container, ``container`` and (when
    mapped) ``container_workdir``.  None where nothing is mapped."""
    entry = project.on.get(host) or {}
    try:
        inside = cwd.relative_to(worktree).as_posix()
    except ValueError:
        inside = "."
    base = entry.get("cwd")
    mapped = None if not base else base if inside in ("", ".") else \
        posixpath.join(base, inside)
    view = {"host": host, "cwd": mapped,
            "from": {"cwd": str(cwd), "container": args.container,
                     "container_workdir": args.container_workdir}}
    if args.container:
        view["container"] = entry.get("container")
        view["container_workdir"] = entry.get("container_workdir")
    return view


def _apply_mapping(tokens: list[str], verb: str, mapping: dict) -> list[str]:
    tokens = _set_option(tokens, verb, "--cwd", mapping["cwd"])
    if mapping.get("container"):
        tokens = _set_option(tokens, verb, "--container",
                             mapping["container"])
    if mapping.get("container_workdir"):
        tokens = _set_option(tokens, verb, "--container-workdir",
                             mapping["container_workdir"])
    return tokens


SHA = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")


def commit_options_problem(args) -> str | None:
    """``--require-commit``/``--require-clean``/``--any-commit`` (G3b)."""
    required = getattr(args, "require_commit", None)
    if getattr(args, "any_commit", False) and \
            (required or getattr(args, "require_clean", False)):
        return ("--any-commit excludes --require-commit and "
                "--require-clean")
    if required is not None and not SHA.fullmatch(required):
        return (f"--require-commit {required!r} is not a full commit sha "
                "(40 or 64 lowercase hex digits)")
    return None


def code_requirement(args, project, worktree: Path) -> dict:
    """G3b (code consistency): what the destination's checkout must be.
    When this host's cwd is a git worktree of the project, its HEAD is
    required there, and a clean tree when this one is clean (tracked files;
    untracked ones are not compared), unless ``--any-commit``; an explicit
    ``--require-commit`` (with or without ``--require-clean``) is kept."""
    view = {"required_commit": None, "require_clean": False,
            "any_commit": bool(getattr(args, "any_commit", False)),
            "local_worktree": str(worktree), "local_head": None,
            "local_dirty": None, "source": None}
    if project.matches(worktree):
        head = _git_read(worktree, "rev-parse", "HEAD")
        if head is not None and head.returncode == 0 and \
                head.stdout.strip():
            view["local_head"] = head.stdout.strip()
            status = _git_read(worktree, "status", "--porcelain",
                               "--untracked-files=no")
            view["local_dirty"] = (bool(status.stdout.strip())
                                   if status is not None and
                                   status.returncode == 0 else None)
    if getattr(args, "require_commit", None) or \
            getattr(args, "require_clean", False):
        view.update(required_commit=args.require_commit,
                    require_clean=bool(args.require_clean),
                    source="--require-commit/--require-clean as given")
    elif view["any_commit"]:
        view["source"] = "--any-commit: no requirement"
    elif view["local_head"]:
        view.update(required_commit=view["local_head"],
                    require_clean=view["local_dirty"] is False,
                    source=f"this host's checkout {worktree}")
    else:
        view["source"] = "the cwd is not a git worktree of the project here"
    return view


def _apply_code(tokens: list[str], verb: str, code: dict | None,
                args) -> list[str]:
    """The requirement as options, unless the caller gave them."""
    if not code or getattr(args, "require_commit", None) or \
            getattr(args, "require_clean", False) or code.get("any_commit"):
        return tokens
    if code.get("required_commit"):
        tokens = _set_option(tokens, verb, "--require-commit",
                             code["required_commit"])
    if code.get("require_clean"):
        tokens = _set_option(tokens, verb, "--require-clean", True,
                             flag=True)
    return tokens


def remote_submit(config: Config, args, argv: list[str],
                  command: list[str]) -> int:
    """``--host X submit`` (and ``run``, review round 1): refusals here
    first (never substitutes for the destination's admission), the
    ``on.<X>`` mapping and the code requirement when ``--cwd`` is a
    checkout of the project here (G3b), then the placement record
    (committed, then sent, fsynced) before the remote call, then the reply
    verbatim into it.  No retry, no other host."""
    host = args.host
    verb = args.cmd
    remote_mod.route(config, host)
    problem = commit_options_problem(args)
    if problem:
        raise ConfigError(problem)
    _env_options_check(config, args)
    local = _local_project(config, args)
    mapping = code = None
    if local is not None:
        project, cwd, worktree = local
        if project.on.get(host, {}).get("cwd"):
            mapping = job_mapping(project, host, cwd, worktree, args)
        code = code_requirement(args, project, worktree)
    if mapping is None and not os.path.isabs(args.cwd):
        raise ConfigError("remote submission needs an absolute --cwd on the "
                          "remote host (or a checkout of a project here "
                          f"whose projects.<p>.on.{host}.cwd maps it)")
    declared = None
    if local is not None:
        declared = local[0].hosts or [config.host]
        name = local[0].name
    elif args.project and args.project in config.projects:
        declared = config.projects[args.project].hosts or [config.host]
        name = args.project
    if declared is not None and host not in declared:
        raise ConfigError(f"project {name!r} declares hosts {declared} "
                          f"here; {host!r} is not one of them (nothing was "
                          "sent)")
    request_id = args.request_id or pl.new_request_id(config.host)
    if not REQUEST_ID.fullmatch(request_id):
        out({"accepted": False, "reason": f"request id {request_id!r} is "
             "not valid: 1 to 128 characters of A-Z a-z 0-9 . _ : -, "
             "starting with a letter or digit"}, args.json)
        return EXIT_NOT_ACCEPTED
    # the pin travels with the command (review round 2, R2-1): on X,
    # `--host X` names X itself, so X runs it as its own pinned submit (its
    # G3a boundary lets a pinned submit of a listed project through); a key
    # that is not X's own label is refused there as an unknown host
    # (fail-closed).  v0.1.x does the same with `--host <its own label>`.
    tokens = ["--host", host, *_strip_global(argv)]
    if mapping is not None:
        tokens = _apply_mapping(tokens, verb, mapping)
    tokens = _apply_code(tokens, verb, code, args)
    return send_remote(config, args, host, tokens, command, request_id,
                       pinned=True, project=args.project or
                       (local[0].name if local else None),
                       mapping=mapping, code=code)


def send_remote(config: Config, args, host: str, tokens: list[str],
                command: list[str], request_id: str, *, pinned: bool,
                project: str | None, policy: dict | None = None,
                mapping: dict | None = None, code: dict | None = None,
                note_extra: dict | None = None) -> int:
    """Send one submit (or run) to ``host``, once: the record committed and
    marked sent (fsynced) before the call, the reply verbatim into it, its
    phase from the reply.  Never retried, never sent to another host."""
    verb = args.cmd
    route_entry = remote_mod.route(config, host)
    tokens = _set_option(tokens, verb, "--request-id", request_id)
    if "--json" not in tokens:
        # the reply is parsed here, and shown in the caller's form below
        tokens.insert(tokens.index(verb) + 1, "--json")
    full = tokens + (["--", *command] if command else [])
    # read-only first (a refusal writes nothing, not even the state lock),
    # then again under the lock with the commit
    try:
        have_state = is_dir_strict(config.state_dir)
    except OSError as error:
        # A49: not "no records to check": nothing is sent
        return _refuse_remote(args, request_id, f"the state dir "
                              f"{config.state_dir} cannot be examined "
                              f"({error.strerror or error}); a placement "
                              "record or a binding may exist; nothing was "
                              "sent")
    if have_state:
        refusal = record_refusal(config, request_id, host)
        if refusal:
            return _refuse_remote(args, request_id, **refusal)
        reason = host_binding_refusal(config, request_id)
        if reason:
            return _refuse_remote(args, request_id, reason)
    try:
        record, existing = pl.commit(
            config.state_dir, request_id, host, route=route_entry.view(),
            pinned=pinned, project=project, argv=full,
            placing_host=config.host,
            placing_host_id=_host_id_quiet(config),
            version=identity.identity(), command=verb, policy=policy,
            code=code, mapping=mapping, placer=procs.identity(os.getpid()),
            guard=lambda: host_binding_refusal(config, request_id))
        if isinstance(existing, str):
            return _refuse_remote(args, request_id, existing)
        if existing is not None:
            refusal = record_refusal(config, request_id, host,
                                     resolve=False) or {
                "reason": BoundElsewhere(existing).reason(
                    config.state_dir, request_id).replace(
                        "never started", "nothing was sent")}
            return _refuse_remote(args, request_id, **refusal)
        # review round 1: the send's client call timeout (None: unbounded)
        # is on record before the call, and its ssh client's identity
        # before that client may run (the gate below)
        record = pl.advance(config.state_dir, record, "sent",
                            f"{verb} sent to {host}",
                            send={"command": verb, "client": None,
                                  "call_timeout_s": remote_mod.call_timeout(
                                      route_entry, full)})
    except (OSError, RecordUnreadable, ValueError) as error:
        print(f"agent-compute: the placement record could not be written "
              f"({error}); nothing was sent", file=sys.stderr)
        return EXIT_NOT_ACCEPTED

    def on_start(pid: int) -> bool:
        client = procs.identity(pid)
        pl.note_client(config.state_dir, request_id, client)
        return True

    reply = remote_mod.call(config, host, full, on_start=on_start)
    if not reply.sent:
        reason = f"{reply.error}; nothing was sent"
        try:
            pl.advance(config.state_dir, record, "refused", reason,
                       reply.as_record())
        except (OSError, ValueError) as error:
            reason += f" (the record could not be updated: {error})"
        remote_mod.explain(reply)
        return _refuse_remote(args, request_id, reason)
    obj = _reply_object(reply.stdout)
    phase, reason, job_id = placement_outcome(reply, obj, verb)
    destination = None
    if obj is not None:
        destination = {"host": obj.get("host"),
                       "host_id": obj.get("host_id"),
                       "version": obj.get("acs_version"),
                       "source_sha": obj.get("acs_source_sha"),
                       "head": obj.get("head"),
                       "code": obj.get("code")}
    fields = {"remote_job_id": job_id} if job_id else {}
    if destination is not None:
        fields["destination"] = destination
        if destination["host"] not in (None, host):
            reason += (f"; the destination reports host "
                       f"{destination['host']!r}, not {host!r}")
    try:
        record = pl.advance(config.state_dir, record, phase, reason,
                            reply.as_record(), **fields)
    except (OSError, ValueError) as error:
        print(f"agent-compute: the reply could not be recorded in the "
              f"placement record ({error}); it stays "
              f"{record.get('phase')}", file=sys.stderr)
    note = {"request_id": request_id, "phase": record.get("phase"),
            "record": str(pl.record_path(config.state_dir, request_id)),
            "pinned": pinned, "placing_host": config.host, "host": host,
            **(note_extra or {})}
    if obj is not None:
        _render_remote_reply(obj, args.json, note, verb)
    else:
        sys.stdout.write(reply.stdout)
    sys.stdout.flush()
    sys.stderr.write(reply.stderr)
    remote_mod.explain(reply)
    print(f"agent-compute: placement record {note['record']}: request id "
          f"{request_id} -> {host}, {record.get('phase')}"
          + (f" ({job_id})" if job_id else ""), file=sys.stderr)
    if phase == "reply-lost":
        print(f"agent-compute: the reply was lost: {reason}.  The record "
              f"binds request id {request_id} to {host}: `agent-compute "
              f"status {request_id}` or `agent-compute placements resolve "
              f"{request_id}` asks {host} by request id; do not submit it "
              "elsewhere", file=sys.stderr)
        code_ = remote_mod.exit_code(reply)
        return code_ if code_ not in (0, EXIT_NOT_ACCEPTED) else \
            remote_mod.EXIT_ROUTE
    return remote_mod.exit_code(reply)


# -- automatic placement (G3b, decision D5) -------------------------------------------------

def placed_project(config: Config, args):
    """``(project, cwd, worktree)`` of an unpinned ``submit``/``run`` when
    placement applies (the project, resolved here, declares more than one
    host); None otherwise (today's behaviour: here, or the boundary)."""
    if getattr(args, "host", None) or routed_from():
        return None
    local = _local_project(config, args)
    if local is None:
        return None
    return local if len(local[0].hosts or [config.host]) > 1 else None


def probe_hosts(config: Config, names: list[str]) -> dict:
    """Every host of ``names``: this one in-process, the others' read-only
    ``_summary`` in parallel, each within its probe timeout."""
    from concurrent.futures import ThreadPoolExecutor
    remotes = [n for n in names if n != config.host]
    probes = {}
    with ThreadPoolExecutor(max_workers=max(1, len(remotes))) as pool:
        futures = {name: pool.submit(remote_mod.call, config, name,
                                     ["_summary", "--json"], None, True)
                   for name in remotes}
        if config.host in names:
            local = host_summary(config)
            probes[config.host] = {"state": "ok", "summary": local,
                                   "reasons": [],
                                   "raw": {"in_process": True,
                                           "summary": local}}
        for name, future in futures.items():
            reply = future.result()
            state, data, reasons = _summary_reply(name, reply)
            probes[name] = {"state": state, "summary": data,
                            "reasons": reasons, "raw": reply.as_record()}
    return probes


def _placement_lines(decision: dict) -> list[str]:
    return [f"{c['host']}: {c['status']}" +
            (f" ({'; '.join(c['reasons'])})" if c["reasons"] else "")
            for c in decision["candidates"]]


def place_submit(config: Config, args, argv: list[str],
                 command: list[str], local) -> int:
    """G3b (D5, D6): an unpinned ``submit``/``run`` of a project that
    declares more than one host.  A request id (generated when absent),
    the read-only probes, the policy (``placement.evaluate``), the record
    committed here with the policy snapshot, then ONE submit to the chosen
    host (in-process when it is this one).  Nothing is written when no
    host is eligible; a refusal or a lost reply on the chosen host is
    never followed by a send to another."""
    verb = args.cmd
    problem = commit_options_problem(args)
    if problem:
        raise ConfigError(problem)
    request_id = args.request_id or pl.new_request_id(config.host)
    if not REQUEST_ID.fullmatch(request_id):
        out({"accepted": False, "reason": f"request id {request_id!r} is "
             "not valid: 1 to 128 characters of A-Z a-z 0-9 . _ : -, "
             "starting with a letter or digit"}, args.json)
        return EXIT_NOT_ACCEPTED
    _env_options_check(config, args)
    if verb == "run":
        refusal = inside_job_refusal(config, "run", None)
        if refusal:
            return _refuse_remote(args, request_id, refusal)
    project, cwd, worktree = local
    code = code_requirement(args, project, worktree)
    refusal = record_refusal(config, request_id, None) or \
        local_binding_refusal(config, request_id)
    if refusal:
        return _refuse_remote(args, request_id, **refusal)
    hosts = list(project.hosts)
    maps = {h: job_mapping(project, h, cwd, worktree, args)
            for h in hosts if h != config.host}
    probes = probe_hosts(config, hosts)
    decision = pl.evaluate(
        project.name, hosts, config.host, probes,
        reserve_gb=args.reserve_gb,
        cwd={h: m["cwd"] for h, m in maps.items()},
        container=({h: m.get("container") for h, m in maps.items()}
                   if args.container else None),
        required_commit=code["required_commit"],
        require_clean=code["require_clean"])
    decision.update(utc=utc_now(), placing_host=config.host,
                    request_id=request_id)
    chosen = decision["chosen"]
    if chosen is None:
        return _refuse_remote(
            args, request_id,
            f"no eligible host for project {project.name!r} among {hosts}: "
            + "; ".join(_placement_lines(decision)) +
            "; never started, nothing written", placement=decision)
    print(f"agent-compute: placed on {chosen} ({pl.RULE}): "
          + "; ".join(_placement_lines(decision)), file=sys.stderr)
    note = {"chosen": chosen, "rule": decision["rule"],
            "candidates": decision["candidates"], "code": code}
    if chosen == config.host:
        return place_here(config, args, argv, command, project, request_id,
                          decision, code, note)
    tokens = _apply_mapping(["--host", chosen, *_strip_global(argv)], verb,
                            maps[chosen])
    tokens = _apply_code(tokens, verb, code, args)
    return send_remote(config, args, chosen, tokens, command, request_id,
                       pinned=False, project=project.name, policy=decision,
                       mapping=maps[chosen], code=code, note_extra=note)


def place_here(config: Config, args, argv: list[str], command: list[str],
               project, request_id: str, decision: dict, code: dict,
               note: dict) -> int:
    """The placement chose this host: the record (committed, with the
    policy) first, then ``accept`` in-process, then the record's outcome
    (``accepted`` with the job the request id is bound to here, else
    ``refused``)."""
    verb = args.cmd
    full = _strip_global(argv) + (["--", *command] if command else [])
    try:
        record, existing = pl.commit(
            config.state_dir, request_id, config.host, route=None,
            pinned=False, project=project.name, argv=full,
            placing_host=config.host,
            placing_host_id=_host_id_quiet(config),
            version=identity.identity(), command=verb, policy=decision,
            code=code, placer=procs.identity(os.getpid()))
    except (OSError, RecordUnreadable, ValueError) as error:
        print(f"agent-compute: the placement record could not be written "
              f"({error}); nothing was started", file=sys.stderr)
        return EXIT_NOT_ACCEPTED
    if existing is not None:
        refusal = record_refusal(config, request_id, None,
                                 resolve=False) or {
            "reason": f"request id {request_id!r}: {existing}"
            if isinstance(existing, str) else
            BoundElsewhere(existing).reason(config.state_dir, request_id)}
        return _refuse_remote(args, request_id, **refusal)
    args.request_id = request_id
    if not getattr(args, "any_commit", False):
        args.require_commit = code["required_commit"]
        args.require_clean = code["require_clean"]
    args.placement = {"request_id": request_id, "placing_host": config.host,
                      "placing_host_id": _host_id_quiet(config),
                      "policy": pl.strip_raw(decision), "code": code}
    if verb == "run":
        args.lock_wait = args.lock_wait or args.admission_timeout
    store = JobStore(config.state_dir)
    try:
        rc, result = accept(config, args, command)
    except BaseException as error:
        try:
            pl.advance(config.state_dir, record, "refused",
                       f"refused here before acceptance: {error}")
        except (OSError, ValueError):
            pass
        raise
    try:
        bound = store.request_job(request_id)
    except ValueError:
        bound = None
    job = bound if isinstance(bound, str) else None
    if isinstance(bound, Unreadable):
        job = result.get("job_id")
    phase = "accepted" if job or isinstance(bound, Unreadable) else "refused"
    reason = ("accepted here" if result.get("accepted") is True else
              f"{result.get('accepted')}: {result.get('reason')}")
    fields = {"remote_job_id": job} if job else {}
    fields["destination"] = {"host": config.host,
                             "host_id": result.get("host_id"),
                             "version": result.get("acs_version"),
                             "source_sha": result.get("acs_source_sha"),
                             "head": result.get("head"),
                             "code": result.get("code")}
    try:
        record = pl.advance(config.state_dir, record, phase, reason, None,
                            **fields)
    except (OSError, ValueError) as error:
        print(f"agent-compute: the outcome could not be recorded in the "
              f"placement record ({error}); it stays committed",
              file=sys.stderr)
    note = {**note, "request_id": request_id, "phase": record.get("phase"),
            "record": str(pl.record_path(config.state_dir, request_id)),
            "pinned": False, "placing_host": config.host,
            "host": config.host}
    if verb == "run" and rc == 0:
        print(f"agent-compute: accepted {result['job_id']}", file=sys.stderr)
        return _run_wait(config, args, result["job_id"])
    if args.json:
        out({**result, "placement": note}, True)
    else:
        # the hosts and their reasons are on stderr ("placed on ...")
        out({**result, "placement": {k: note[k] for k in
                                     ("request_id", "phase", "record",
                                      "chosen")}}, False)
        if result.get("job_id") and verb == "submit":
            print(result["job_id"])
    return rc


# -- routing by placement record --------------------------------------------------------

ROUTED = ("status", "wait", "cancel", "reconcile", "log", "fetch")


def _route_target(args) -> str | None:
    if args.cmd == "reconcile":
        return args.terminate
    if args.cmd == "status":
        return getattr(args, "request_id", None) or args.job
    return getattr(args, "job", None)


def placement_route(config: Config, args) -> tuple[dict, str] | None:
    """The placement record for this command's job or request id, when it
    names another host and the id is not a job of this host; with the
    identifier to send there (the destination's job id, else the request
    id).  Unreadable records met on the way are reported (C4)."""
    if args.cmd not in ROUTED:
        return None
    ident = _route_target(args)
    if not ident:
        return None
    store = JobStore(config.state_dir)
    try:
        local = store.manifest(ident) is not None or \
            store.state(ident) is not None
    except ValueError:
        local = False
    if not local and args.cmd == "status" and \
            getattr(args, "request_id", None):
        try:
            local = store.request_job(ident) is not None
        except ValueError:
            local = False
    if local:
        return None
    if not config.state_dir.is_dir():
        return None
    record, unreadable = pl.find(config.state_dir, ident)
    for bad in unreadable:
        print(f"agent-compute: a placement record {bad.reason}; an "
              "operator inspects it", file=sys.stderr)
    if record is None or record.get("host") in (None, config.host):
        return None
    return record, record.get("remote_job_id") or ident


def routed_command(config: Config, args, argv: list[str],
                   record: dict, ident: str) -> int:
    """Run the command on the recorded host (its own gates apply there):
    the same command with the destination's job id (or, for ``status``
    without one, ``status --request-id``); the JSON reply carries
    ``route``.  Never falls back to this host."""
    host = record["host"]
    request_id = record.get("request_id")
    route_view = {"host": host, **(config.remotes[host].view()
                                   if host in config.remotes else {}),
                  "via": "placement record",
                  "record": str(pl.record_path(config.state_dir,
                                               request_id)),
                  "request_id": request_id,
                  "phase": record.get("phase")}
    if host not in config.remotes:
        print(f"agent-compute: the placement record {route_view['record']} "
              f"names host {host!r}, which this config no longer routes; "
              "nothing was run", file=sys.stderr)
        return EXIT_NOT_ACCEPTED
    job_known = bool(record.get("remote_job_id"))
    if args.cmd == "fetch":
        if not job_known:
            print(f"agent-compute: the job id on {host} is not known "
                  f"(placement {record.get('phase')}); `agent-compute status "
                  f"{request_id}` asks {host} by request id",
                  file=sys.stderr)
            return EXIT_NOT_ACCEPTED
        args.job = ident
        return cmd_fetch(config, args, host, route_view)
    if not job_known and args.cmd != "status":
        print(f"agent-compute: the job id on {host} is not known (placement "
              f"{record.get('phase')}); `agent-compute status {request_id}` "
              f"asks {host} by request id; nothing was run", file=sys.stderr)
        return EXIT_NOT_ACCEPTED
    passthrough = [a for a in _strip_global(argv)]
    original = _route_target(args)
    if args.cmd == "status" and getattr(args, "request_id", None):
        pass                            # `status --request-id R` there
    elif job_known:
        passthrough = [ident if a == original else a for a in passthrough]
    else:
        passthrough = ["--request-id" if a == original else a
                       for a in passthrough]
        passthrough.insert(passthrough.index("--request-id") + 1,
                           request_id)
    reply = remote_mod.call(config, host, passthrough)
    obj = _reply_object(reply.stdout)
    # G3b: the placement as this host recorded it (the policy snapshot of
    # an automatic placement: every host and why it was or was not chosen)
    placed = pl.policy_view(record)
    if getattr(args, "json", False) and obj is not None:
        merged = {**obj, "route": route_view, "placement": {
            **(obj.get("placement") or {}), **placed}}
        if isinstance(merged.get("job"), dict):
            merged["job"] = {**merged["job"],
                             "placement": merged["placement"]}
        out(merged, True)
    else:
        sys.stdout.write(reply.stdout)
    policy = placed.get("policy") if placed else None
    if policy:
        print(f"agent-compute: placed on {host} by {placed['placing_host']}"
              f"'s policy ({policy.get('rule')}): "
              + "; ".join(_placement_lines(policy)), file=sys.stderr)
    sys.stdout.flush()
    sys.stderr.write(reply.stderr)
    remote_mod.explain(reply)
    print(f"agent-compute: routed to {host} by the placement record of "
          f"request id {request_id}", file=sys.stderr)
    return remote_mod.exit_code(reply)


# -- the boundary of a submit that is not placed ----------------------------------------

def placement_refusal(config: Config, args,
                      pinned_here: bool = False) -> dict | None:
    """A local ``submit``/``run`` that is not placed automatically (G3b:
    ``placed_project``) of a project that declares hosts other than this
    one: refused with the candidates, unless it is pinned here (``--host``
    this host, or a call that arrived routed) and the project lists this
    host.  None when it runs here (the project's only host is this one, or
    the pin), or when the project cannot be resolved here (``accept``
    reports that)."""
    try:
        cwd = expand(args.cwd).resolve()
        project = config.resolve_project(worktree_of(cwd), args.project)
    except (ConfigError, OSError, ValueError):
        return None             # accept checks again after its resolution
    return project_hosts_refusal(config, project, pinned_here)


def project_hosts_refusal(config: Config, project,
                          pinned_here: bool) -> dict | None:
    """The G3a boundary for one resolved project (``placement_refusal``
    before anything is written; ``accept`` again after its own resolution,
    review round 1 note 7)."""
    hosts = list(project.hosts or [config.host])
    if hosts == [config.host] or (pinned_here and config.host in hosts):
        return None
    candidates = []
    for name in hosts:
        item = {"host": name, "local": name == config.host}
        if name != config.host:
            item.update({k: v for k, v in
                         (project.on.get(name) or {}).items()
                         if v is not None})
            route = config.remotes.get(name)
            item["route"] = route.view() if route else None
        candidates.append(item)
    here = config.host in hosts
    return {"accepted": False, "project": project.name,
            "reason": (f"project {project.name!r} declares hosts {hosts}: "
                       "choose one with --host (it was not placed "
                       "automatically: placement applies to an unpinned "
                       "submit of a project declaring several hosts); "
                       + (f"`--host {config.host}` runs it here; "
                          if here else
                          f"it may not run on this host ({config.host}); ")
                       + "for a remote host, its cwd and container are in "
                         "the candidates; never started, nothing written"),
            "candidates": candidates}


def request_bound_elsewhere(config: Config, request_id: str | None
                            ) -> dict | None:
    """A local submit whose request id a placement record binds to another
    host (D6): refused, nothing written."""
    if not request_id:
        return None
    try:
        present = is_dir_strict(config.state_dir)
    except OSError as error:
        return {"accepted": False, "request_id": request_id,
                "reason": f"request id {request_id!r}: the state dir "
                          f"{config.state_dir} cannot be examined "
                          f"({error.strerror or error}); a placement record "
                          "may bind it to another host; never started"}
    if not present:
        return None
    try:
        record = pl.read(config.state_dir, request_id)
    except ValueError:
        return None
    if not pl.bound_elsewhere(record, config.host):
        return None
    where = record.reason if isinstance(record, Unreadable) else \
        f"host {record.get('host')!r} ({record.get('phase')})"
    return {"accepted": False, "request_id": request_id,
            "reason": f"request id {request_id!r} is bound to {where} by "
                      f"the placement record "
                      f"{pl.record_path(config.state_dir, request_id)}; it "
                      "is never submitted on another host (D6); never "
                      "started"}


# --- argument parsing ---------------------------------------------------------------

def add_submit_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--case", required=True,
                        help="baseline case key (namespaced by the project's "
                             "case_prefix)")
    parser.add_argument("--timeout-min", type=float, required=True,
                        help="finite execution ceiling (guard wall time)")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--dense-threads", type=int, default=None)
    parser.add_argument("--reserve-gb", type=float, default=None,
                        help="declared memory reserve for an unmeasured case")
    parser.add_argument("--cwd", default=".")
    parser.add_argument("--project", default=None)
    parser.add_argument("--slice", default=None)
    parser.add_argument("--units", default=None)
    parser.add_argument("--env", action="append", default=[],
                        metavar="NAME=VALUE")
    parser.add_argument("--input", action="append", default=[],
                        metavar="PATH", help="extra input to fingerprint")
    parser.add_argument("--request-id", default=None,
                        help="stable id; a second submission with it is "
                             "refused while the first may have run")
    parser.add_argument("--owner", default=None,
                        help="free-text label for the submitting session")
    parser.add_argument("--submitter", default=None,
                        help="free label linking the job to a session "
                             f"(default: ${ENV_SUBMITTER}); recorded in "
                             "the manifest")
    parser.add_argument("--container", default=None,
                        help="container venue (must be listed for the project)")
    parser.add_argument("--container-workdir", default=None)
    parser.add_argument("--probe", action="store_true",
                        help="run the guard's probe mode: the same "
                             "admission, stopped at --probe-s and recorded "
                             "as a probe baseline row (an unmeasured case "
                             "needs no --reserve-gb)")
    parser.add_argument("--probe-s", type=float, default=None,
                        help=f"probe limit in seconds (default "
                             f"{DEFAULT_PROBE_S:g}; at most --timeout-min x "
                             "60)")
    parser.add_argument("--lock-wait", type=float, default=None,
                        help="seconds the job may wait for admission once "
                             "dispatched (capped by the host config)")
    # G3b: code consistency across hosts (the placing host passes them)
    parser.add_argument("--require-commit", default=None, metavar="SHA",
                        help="refuse (at submit and at dispatch, never "
                             "started) unless the job's checkout is at this "
                             "commit (a full sha); set by a placed or "
                             "pinned submit from a checkout of the project")
    parser.add_argument("--require-clean", action="store_true",
                        help="refuse (at submit and at dispatch) when the "
                             "job's checkout has uncommitted changes to "
                             "tracked files")
    parser.add_argument("--any-commit", action="store_true",
                        help="a placed or pinned submit: do not require "
                             "the placing checkout's commit at the "
                             "destination")
    parser.add_argument("--json", action="store_true")


def version_text() -> str:
    ident = identity.identity()
    return f"{ident['version']} {ident['source_sha'] or 'unknown'}"


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="agent-compute", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter,
                                allow_abbrev=False)
    p.add_argument("--config", default=None)
    p.add_argument("--host", default=None,
                   help="execution host (default: this host)")
    p.add_argument("--version", action="version", version=version_text())
    sub = p.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("doctor")
    d.add_argument("--json", action="store_true")
    s = sub.add_parser("submit")
    add_submit_options(s)
    r = sub.add_parser("run")
    add_submit_options(r)
    r.add_argument("--admission-timeout", type=float, default=900.0,
                   help="seconds to wait for admission before cancelling")
    st = sub.add_parser("status")
    st.add_argument("job", nargs="?")
    st.add_argument("--all", action="store_true")
    st.add_argument("--unfinished", action="store_true",
                    help="only jobs without an outcome")
    st.add_argument("--mine", default=None, metavar="OWNER|REQUEST-ID-PREFIX",
                    help="only jobs whose --owner is this, or whose "
                         "--request-id starts with it")
    st.add_argument("--request-id", default=None, metavar="R",
                    help="the job(s) this host bound or recorded under "
                         "request id R")
    st.add_argument("--resolve", action="store_true",
                    help="first resolve every open placement record by "
                         "request id on its own host (a remote call per "
                         "record, up to the probe timeout)")
    st.add_argument("--json", action="store_true")
    w = sub.add_parser("wait")
    w.add_argument("job")
    w.add_argument("--timeout", type=float, default=None)
    w.add_argument("--json", action="store_true")
    c = sub.add_parser("cancel")
    c.add_argument("job")
    c.add_argument("--json", action="store_true")
    rc = sub.add_parser("reconcile")
    rc.add_argument("--terminate", default=None, metavar="JOB")
    rc.add_argument("--json", action="store_true")
    rs = sub.add_parser("resume")
    rs.add_argument("--json", action="store_true")
    dm = sub.add_parser("daemon")
    dm.add_argument("action", choices=("start", "stop", "status"))
    dm.add_argument("--json", action="store_true")
    pb = sub.add_parser("pin-backend",
                        help="operator-only: pin the internal engine of "
                             "this installed version, with committed suite "
                             "evidence and a recorded authorization")
    pb.add_argument("--evidence", required=True, metavar="FILE",
                    help="committed full-suite output whose header names "
                         "this host, the internal engine and its sha256, "
                         "0 failed")
    pb.add_argument("--authorized-by", default=None, metavar="TEXT",
                    help="the operator authorization: who, when, what "
                         "(recorded in <state>/authorizations.jsonl)")
    pb.add_argument("--backend", choices=BACKEND_KINDS, default=BACKEND_ACS,
                    help="the backend to pin: acs-guard, the only one (the "
                         "internal engine of this installed version)")
    pb.add_argument("--json", action="store_true")
    cf = sub.add_parser("config", help="machine config maintenance")
    cfs = cf.add_subparsers(dest="config_cmd", required=True)
    mg = cfs.add_parser("migrate", help="operator-only: rewrite a v1 config "
                        "to v2 with the same effective values")
    mg.add_argument("--authorized-by", default=None, metavar="TEXT",
                    help="the operator authorization: who, when, what")
    mg.add_argument("--dry-run", action="store_true",
                    help="print the diff and the comparison; write nothing")
    mg.add_argument("--json", action="store_true")
    ci = cfs.add_parser("init", help="print a fresh config for this host "
                        "(host-derived profile defaults; writes nothing)")
    ci.add_argument("--label", default=None, metavar="NAME",
                    help="the host label (default: this host's short name)")
    lg = sub.add_parser("log", help="print a job's output.log")
    lg.add_argument("job")
    lg.add_argument("--tail", type=int, default=None, metavar="N",
                    help="only the last N lines")
    fe = sub.add_parser("fetch", help="copy a job's evidence (manifest, "
                        "state, output.log, guard records) into DIR/JOB")
    fe.add_argument("job")
    fe.add_argument("dir")
    fe.add_argument("--json", action="store_true")
    ho = sub.add_parser("hosts", help="every configured host's state "
                        "(read-only, in parallel), identity checks and "
                        "the open placement records")
    ho.add_argument("--json", action="store_true")
    sm = sub.add_parser("_summary", help="this host's read-only summary "
                        "(what `hosts` asks each host)")
    sm.add_argument("--json", action="store_true")
    pls = sub.add_parser("placements", help="placement records: "
                         "`resolve [R]` asks each open record's host by "
                         "request id (read-only there)")
    plsub = pls.add_subparsers(dest="placements_cmd", required=True)
    plr = plsub.add_parser("resolve", help="resolve the open placement "
                           "records (or R's) by request id")
    plr.add_argument("request_id", nargs="?", metavar="R")
    plr.add_argument("--json", action="store_true")
    rq = sub.add_parser("_request", help="whether this host bound or "
                        "recorded a request id (read-only; what `placements "
                        "resolve` asks)")
    rq.add_argument("request_id", metavar="R")
    rq.add_argument("--json", action="store_true")
    fb = sub.add_parser("_fetch", help="a job's evidence as one bundle "
                        "(what `fetch` asks a remote host)")
    fb.add_argument("job")
    ex = sub.add_parser("_exec")
    ex.add_argument("job")
    sub.add_parser("_argv", help="print received arguments (transport test)")
    # review round 1 (finding 4): no abbreviated options anywhere (`--en`
    # must not pass for `--env`, whose values placement records redact)
    for child in [*sub.choices.values(), *cfs.choices.values(),
                  *plsub.choices.values()]:
        child.allow_abbrev = False
    return p


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    command: list[str] = []
    if "--" in argv:
        cut = argv.index("--")
        argv, command = argv[:cut], argv[cut + 1:]
    args = parser().parse_args(argv)
    if args.cmd == "config":
        # before any load: a v1 config does not load (that is the point),
        # and `init` needs none
        if args.host:
            print(f"agent-compute: config {args.config_cmd} runs on the host "
                  "whose config it concerns (no --host)", file=sys.stderr)
            return EXIT_NOT_ACCEPTED
        if args.config_cmd == "init":
            return cmd_config_init(args)
        return cmd_config_migrate(args)
    try:
        config = load(args.config)
        if args.host and args.host != config.host:
            if routed_from():
                print(f"agent-compute: this call arrived routed from "
                      f"{routed_from()} ({remote_mod.NO_REROUTE_ENV}): it is "
                      f"never forwarded again (to {args.host}; register "
                      "A40); nothing was run", file=sys.stderr)
                return EXIT_NOT_ACCEPTED
            return remote_command(config, args, argv, command)
        if args.cmd == "_summary":
            return cmd_summary(config, args)
        if args.cmd == "hosts":
            return cmd_hosts(config, args)
        if args.cmd == "_fetch":
            return cmd_fetch_bundle(config, args)
        if args.cmd == "_request":
            return cmd_request(config, args)
        if args.cmd == "placements":
            return cmd_placements(config, args)
        if args.cmd == "status" and not routed_from():
            # G3b: `status R` resolves R's open record by request id on its
            # host first; plain `status` asks no other host (review round
            # 1), `--resolve` asks for every open record
            target = _route_target(args)
            if target or args.resolve:
                _report_resolutions(resolve_placements(
                    config, {target} if target else None))
        # A40: a call that arrived routed is answered here, never routed on
        routed = None if routed_from() else placement_route(config, args)
        if routed is not None:
            return routed_command(config, args, argv, *routed)
        if args.cmd in ("submit", "run"):
            local = placed_project(config, args)
            if local is not None:
                # G3b: more than one host declared, none pinned
                return place_submit(config, args, argv, command, local)
            refusal = request_bound_elsewhere(config, args.request_id) or \
                placement_refusal(config, args, pinned_here=bool(
                    args.host or routed_from()))
            if refusal:
                # nothing is written or started
                out(refusal, args.json)
                return EXIT_NOT_ACCEPTED
        if args.cmd == "log":
            return cmd_log(config, args)
        if args.cmd == "fetch":
            return cmd_fetch(config, args)
        if args.cmd == "_argv":
            print(json.dumps({"host": config.host, "argv": command}))
            return 0
        if args.cmd == "_exec":
            from .runner import main as run_job
            return run_job(config, args.job)
        if args.cmd == "doctor":
            return cmd_doctor(config, args)
        if args.cmd == "submit":
            code, result = accept(config, args, command)
            out(result, args.json)
            if not args.json and result.get("job_id"):
                # G2a: the job id is the last line of every human result
                print(result["job_id"])
            return code
        if args.cmd in ("run", "wait"):
            refusal = inside_job_refusal(
                config, args.cmd, args.job if args.cmd == "wait" else None)
            if refusal:
                # nothing is submitted or waited on
                out({"accepted": False, "reason": refusal}, args.json)
                return EXIT_NOT_ACCEPTED
        if args.cmd == "run":
            return cmd_run(config, args, command)
        if args.cmd == "status":
            return cmd_status(config, args)
        if args.cmd == "wait":
            return cmd_wait(config, args)
        if args.cmd == "cancel":
            return cmd_cancel(config, args)
        if args.cmd == "reconcile":
            return cmd_reconcile(config, args)
        if args.cmd == "resume":
            return cmd_resume(config, args)
        if args.cmd == "daemon":
            return cmd_daemon(config, args)
        if args.cmd == "pin-backend":
            return cmd_pin_backend(config, args)
    except (ConfigError, q.QueueError, ValueError) as error:
        print(f"agent-compute: {error}", file=sys.stderr)
        return EXIT_NOT_ACCEPTED
    return 2


def remote_command(config: Config, args, argv: list[str],
                   command: list[str]) -> int:
    """``--host X <command>``: run it on X (its config, state, queue and
    lock).  ``submit`` and ``run`` write the placement record first (a
    request id generated when absent, D6); ``fetch`` unpacks X's bundle
    here; everything else is forwarded as it is."""
    remote_mod.route(config, args.host)
    if args.cmd in ("submit", "run"):
        # review round 1: `run` is recorded like `submit`
        return remote_submit(config, args, argv, command)
    if args.cmd == "fetch":
        return cmd_fetch(config, args, host=args.host)
    passthrough = _strip_global(argv)
    return remote_mod.forward(config, args.host,
                              passthrough + (["--", *command] if command
                                             else []))


def _strip_global(argv: list[str]) -> list[str]:
    """argv without --config/--host (the remote uses its own config)."""
    result, skip = [], False
    for i, arg in enumerate(argv):
        if skip:
            skip = False
            continue
        if arg in ("--config", "--host"):
            skip = True
            continue
        if arg.startswith(("--config=", "--host=")):
            continue
        result.append(arg)
    return result
