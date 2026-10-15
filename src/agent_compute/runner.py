"""The job runner: one detached supervisor per accepted job.

The runner is what the queue executes (or what ``submit`` starts detached
when the queue is disabled).  It never takes the compute lock itself: it
invokes the guard once per admission attempt (the pinned ACS-internal
engine of this very version directory, as its own process, with a per-job
profile file, the job's ACS claim, its guard home and its marker names;
since 0.2.0 no other guard: the external ``guard.py`` backend is retired),
and the guard takes the real lock, pre-flights memory, pins threads,
monitors and enforces the wall-time ceiling.  The runner adds only:

* an atomic claim, so a job that left the queue once is never re-executed;
* the ACS *host admission lock* (``<state>/admission.lock``, ACS metadata,
  never the guard's lock): held from admission until the job's outcome and
  any host block are written, and released by the kernel if the runner
  dies.  So no second ACS job can reach the guard while an earlier ACS job's
  cleanup is unresolved, even though the guard's own flock is released the
  moment its guard process exits;
* the dispatch gate, the durable host block and backend/input revalidation,
  re-checked before *every* guard attempt;
* read-only observation of who holds the guard's lock file (``lsof`` or
  ``/proc``), so waiting does not start a guard attempt per poll;
* bounded re-attempts of memory/disk refusals, which the guard makes before
  launching anything;
* workload identity capture, cleanup verification after the guard exits, and
  a durable host block whenever cleanup cannot be established.
"""

from __future__ import annotations

import fcntl
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from . import procs
from .config import (ACS_GUARD_PREFIX, BACKEND_ACS, GUARD_ENV_PREFIX,
                     GUARD_HOME_ENV, Config)
from .fingerprint import (checkout_state, code_problem, guard_identity,
                          inputs_fingerprint)
from .fsprobe import exists_strict, lexists_strict
from .state import (JobStore, RecordUnreadable, Unreadable, read_json,
                    state_lock, utc_now, write_json)

# The ACS-internal guard of THIS version directory (the only backend).  The
# runner runs from the version dir the job was submitted with (the queue
# task and the detached runner call that version's bin/agent-compute), so
# this is the engine of the job's version.  Admission never imports it.
ENGINE = Path(__file__).resolve().parent / "guard" / "engine.py"
PROFILE_FILE = "guard-profile.json"
# The internal guard's in-flight marker (S3), next to current.json in the
# guard home.  ACS reads it; only `reconcile` removes it, on evidence.
INFLIGHT_FILE = "inflight.json"
# S5a: the ACS job id, exported to the guard and so inherited by a host
# workload.  It lets `agent-compute` tell that it runs inside that job
# (`submit` records it as `parent_job`; `run`/`wait` refuse while that job
# is unfinished).  Metadata, not a credential; never passed to a child job.
JOB_ENV = "ACS_JOB_ID"
# the guard's default probe limit (guard.py and the engine: PROBE_S)
DEFAULT_PROBE_S = 60.0


def engine_path() -> Path:
    return ENGINE

# the guard's exit codes (guard/engine.py; the same as guard.py's were)
EXIT_REFUSED, EXIT_LOCKED, EXIT_KILLED, EXIT_VENUE, EXIT_USAGE = 3, 4, 5, 6, 7

# runner exit codes (what the queue records; ACS status is authoritative)
RC = {"succeeded": 0, "failed": 1, "launch-failed": 10, "refused": 11,
      "admission-timeout": 12, "held-inputs-changed": 13, "cancelled": 14,
      "timed-out": 15, "killed-resource": 16, "interrupted": 17,
      "unknown": 18, "duplicate-dispatch": 19}

GUARD_OUTCOME = {"completed": "succeeded", "failed": "failed",
                 "killed-walltime": "timed-out",
                 "killed-budget": "killed-resource",
                 "killed-paused": "killed-resource",
                 "killed-interrupted": "interrupted",
                 "launch-failed": "launch-failed",
                 # S5a: the guard stopped the job at its probe limit (guard
                 # exit 0).  A probe's result, only for a job submitted as a
                 # probe (classify: `unknown` for any other job)
                 "probe-stopped": "succeeded",
                 # S3/S4: the internal guard could not prove the job's
                 # processes gone.  Never a result: an ambiguous end, and
                 # classify() blocks the host for it
                 "cleanup-unverified": "unknown"}
# guard outcomes that are an ambiguous end whatever ACS's own evidence says
AMBIGUOUS_GUARD_OUTCOMES = ("cleanup-unverified",)

# "Docker VM guest memory": the internal guard's macOS container pre-flight
# (A21) when the job fits in the Docker VM's guest but not in what the
# guest has available now; a job larger than the guest is a final refusal
RETRYABLE_REFUSALS = ("host available memory", "free disk",
                      "Docker VM guest memory")
POLL_S = 0.5
LOCK_POLL_S = 5.0
GUARD_WAIT_SLICE_S = 10.0
CANCEL_GRACE_S = 60.0
# the leader's start vs the guard's started_utc (taken just before Popen)
CAPTURE_TOLERANCE_S = 5.0
# how often the runner re-proves the exact-start leader alive and records
# workload_seen_utc (the bound for signalling orphaned members)
PROOF_INTERVAL_S = 10.0


def log(job_dir: Path, message: str) -> None:
    line = f"[agent-compute {utc_now()}] {message}\n"
    with open(job_dir / "output.log", "a") as handle:
        handle.write(line)


class Duplicate(Exception):
    pass


class Unclaimable(Exception):
    """C4: the job's own state cannot be read, so whether another runner
    claimed it, or it already ended, is unknown: it is not claimed."""


class Runner:
    # §6 clock readings and the launch note (class defaults as well, so a
    # Runner built without __init__, as some unit tests do, has them)
    t_claim: float | None = None
    t_launch: float | None = None
    launch_wall: float | None = None
    t_guard_exit: float | None = None
    launch_noted = False

    def __init__(self, config: Config, job_id: str):
        self.config = config
        self.store = JobStore(config.state_dir, actor="runner")
        self.job_id = job_id
        self.dir = self.store.job_dir(job_id)
        self.manifest = self.store.manifest(job_id)
        if isinstance(self.manifest, Unreadable):
            raise SystemExit(f"agent-compute: the manifest of {job_id} "
                             f"cannot be read: {self.manifest.reason}")
        if self.manifest is None:
            raise SystemExit(f"agent-compute: no manifest for {job_id}")
        self.host = self.manifest["host"]
        self.cancel = False
        self.guard_proc: subprocess.Popen | None = None
        self.workload: dict | None = None
        self.admission_fd: int | None = None
        self.seen: dict | None = None       # last proof the leader lived
        self.last_proof = 0.0
        self.launch_noted = False
        # §6: this runner's own clock readings (monotonic; the wall clock
        # only against the CLI's accepted_utc), stored at finish
        self.t_claim: float | None = None
        self.t_launch: float | None = None
        self.launch_wall: float | None = None
        self.t_guard_exit: float | None = None

    # -- helpers ---------------------------------------------------------------
    def update(self, **changes) -> dict:
        return self.store.update_state(self.job_id, **changes)

    def cancel_request(self) -> dict | None:
        """The recorded cancellation intent (outcome + reason), if any."""
        path = self.dir / "cancel-requested"
        try:
            requested = lexists_strict(path)
        except OSError:
            # A49: a request that cannot be examined is not "none": the
            # job is cancelled (like a malformed request, below)
            return {"outcome": "cancelled", "reason": "cancelled (the "
                    "cancellation request cannot be examined)"}
        if requested:
            request = read_json(path)
            return request if isinstance(request, dict) else {
                "outcome": "cancelled", "reason": "cancelled"}
        if self.cancel:
            return {"outcome": "cancelled", "reason": "cancelled by signal"}
        return None

    def never_started_cancel(self, where: str) -> int:
        """Finish a cancellation that provably happened before launch."""
        request = self.cancel_request() or {}
        return self.finish(request.get("outcome", "cancelled"),
                           f"{request.get('reason', 'cancelled')} ({where}); "
                           "never started")

    def elapsed(self) -> dict:
        """§6: the elapsed values this runner measured, stored at finish:
        ``queue_wait_s`` (accepted, by the CLI's ``accepted_utc``, to the
        launch this runner observed, or to now when nothing launched),
        ``admission_wait_s`` (its claim to that launch, or to now),
        ``execution_wall_s`` (that launch to the guard's exit) and
        ``cleanup_s`` (the guard's exit to now); the launch is observed at
        the runner's poll (POLL_S).  None where it does not apply."""
        now_m, now_w = time.monotonic(), time.time()
        state = self.store.state(self.job_id)
        accepted = state.get("accepted_utc") if isinstance(state, dict) \
            else None
        accepted = procs.utc_to_epoch(accepted) \
            if isinstance(accepted, str) else None
        launched = self.t_launch is not None

        def r(value):
            return None if value is None else round(max(0.0, value), 3)
        return {
            "elapsed_clock": "runner",
            "queue_wait_s": r((self.launch_wall if launched else now_w) -
                              accepted) if accepted is not None else None,
            "admission_wait_s": r((self.t_launch if launched else now_m) -
                                  self.t_claim)
            if self.t_claim is not None else None,
            "execution_wall_s": r(self.t_guard_exit - self.t_launch)
            if launched and self.t_guard_exit is not None else None,
            "cleanup_s": r(now_m - self.t_guard_exit)
            if launched and self.t_guard_exit is not None else None}

    def mark_launch(self) -> None:
        if self.t_launch is None:
            self.t_launch, self.launch_wall = time.monotonic(), time.time()

    def finish(self, outcome: str, reason: str, **extra) -> int:
        # the wait (if any) is over: wait_reason is cleared (G2a)
        try:
            measured = self.elapsed()
        except Exception:  # noqa: BLE001 - a measurement never blocks the end
            measured = {}
        self.update(phase="done", outcome=outcome, reason=reason,
                    ended_utc=utc_now(), wait_reason=None,
                    **{**measured, **extra})
        log(self.dir, f"outcome {outcome}: {reason}")
        return RC.get(outcome, 1)

    def block(self, reason: str, evidence: dict,
              job_id: str | None = None) -> None:
        if (job_id or self.job_id) == self.job_id and self.seen:
            evidence = {**evidence, "workload_seen": self.seen}
        try:
            self.store.set_block(self.host, job_id or self.job_id, reason,
                                 evidence)
        except RecordUnreadable as error:
            # C4: the block file is never replaced; the host stays blocked
            # by it, and this job's reason is in its own log and state
            log(self.dir, f"host block NOT recorded ({error}); the host "
                          "stays blocked by the unreadable block file")
        log(self.dir, f"HOST BLOCKED ({self.host}): {reason}")
        from .queue import pause_group_quietly
        pause_group_quietly(self.config)

    def block_lost(self, other: str, runner: dict) -> bool:
        """Block the host for a job whose runner was found gone, unless it
        finished (or was re-claimed) meanwhile (A9)."""
        if not self.store.block_if_unfinished(
                self.host, other, runner,
                "runner gone while the job was unfinished (lost supervision)",
                {"detected_by": self.job_id,
                 "state": _view(self.store.state(other))}):
            return False
        log(self.dir, f"HOST BLOCKED ({self.host}): lost supervision of "
                      f"{other}")
        from .queue import pause_group_quietly
        pause_group_quietly(self.config)
        return True

    def block_unverified(self, unverified: dict) -> list[str]:
        """At the admission deadline: a prior job whose runner stayed
        unknown gets a durable host block (re-checked under the state
        lock), so ``reconcile`` lists it and an operator can act (A2)."""
        blocked = []
        for other, runner in (unverified or {}).items():
            if _unreadable_mark(runner):
                continue        # blocked at once by the admission loop
            state = procs.liveness(runner)      # re-run right before
            if state == procs.GONE:
                if self.block_lost(other, runner):
                    blocked.append(other)
                continue
            if state != procs.UNKNOWN:
                continue                        # matched again: not blocked
            if self.store.block_if_unfinished(
                    self.host, other, runner,
                    "runner liveness unknown until the admission deadline of "
                    f"{self.job_id} (process check failed, or its pid is "
                    "held by a process that cannot be matched to the "
                    "recorded start); nothing is admitted beside it",
                    {"runner": runner, "liveness": procs.UNKNOWN,
                     "detected_by": self.job_id,
                     "state": _view(self.store.state(other))}):
                blocked.append(other)
        if blocked:
            log(self.dir, f"HOST BLOCKED ({self.host}): runner of "
                          f"{blocked} unverifiable at the deadline")
            from .queue import pause_group_quietly
            pause_group_quietly(self.config)
        return blocked

    def _on_signal(self, signum, _frame) -> None:
        self.cancel = True

    def claim(self) -> None:
        """Atomically record this runner as the job's only runner."""
        with state_lock(self.config.state_dir):
            state = self.store.state(self.job_id)
            if isinstance(state, Unreadable):
                raise Unclaimable(state.reason)
            state = state or {}
            if state.get("outcome"):
                raise Duplicate(f"dispatch of a finished job refused "
                                f"(outcome {state['outcome']})")
            if state.get("runner"):
                raise Duplicate(state["runner"])
            me = procs.identity(os.getpid())
            item = self.store.history(phase="dispatched",
                                      reason="runner started")
            state.setdefault("history", []).append(item)
            state.update(phase="dispatched", runner=me,
                         dispatched_utc=item["utc"], reason="runner started",
                         updated_utc=utc_now())
            from .state import write_json
            write_json(self.dir / "state.json", state)
            self.t_claim = time.monotonic()
            self.store.log_event(self.job_id, state, item)

    # -- pre-admission checks --------------------------------------------------
    def prior_runners(self) -> tuple[dict, dict]:
        """Unfinished ACS jobs on this host whose runner is provably gone
        (job id -> the runner identity found gone), and those whose runner
        cannot be verified (job id -> that runner identity) (liveness unknown: the process table could not
        be read, or the pid is held by a process that cannot be matched to
        the recorded start).  Neither may be admitted beside.

        C4: a job whose state cannot be read is unverified too (it may hide
        a live runner), as ``{"unreadable": <reason>}``; a manifest that
        cannot be read does not prove the job another host's."""
        dead, unverified = {}, {}
        for other in self.store.all_ids():
            if other == self.job_id:
                continue
            st = self.store.state(other)
            man = self.store.manifest(other)
            if isinstance(st, Unreadable):
                if isinstance(man, dict) and man.get("host") != self.host:
                    continue
                unverified[other] = {"unreadable": st.reason}
                continue
            st = st or {}
            # an unreadable manifest cannot prove the job another host's
            host = (st.get("host") or self.host) \
                if isinstance(man, Unreadable) else (man or {}).get("host")
            if host != self.host or st.get("outcome") or \
                    not st.get("runner"):
                continue
            state = procs.liveness(st["runner"])
            if state == procs.GONE:
                dead[other] = st["runner"]
            elif state == procs.UNKNOWN:
                unverified[other] = st["runner"]
        return dead, unverified

    def dead_jobs(self) -> list[str]:
        return list(self.prior_runners()[0])

    def backend_path(self) -> Path:
        return engine_path()

    def revalidate(self) -> str | None:
        man = self.manifest
        # a manifest without a kind predates the internal guard (it ran
        # guard.py); a retired kind is never run by this version
        submitted = (man.get("backend") or {}).get("kind")
        if submitted != BACKEND_ACS or \
                self.config.backend_kind != BACKEND_ACS:
            return (f"the job was submitted for backend {submitted!r}, which "
                    f"this version does not run (the backend is "
                    f"{self.config.backend_kind!r})")
        problem = self.names_changed()
        if problem:
            return problem
        path = self.backend_path()
        if str(path) != (man.get("backend") or {}).get("path"):
            return (f"the internal guard {path} is not the one recorded at "
                    "submission (another installed version)")
        ident = guard_identity(path)
        if not ident.get("exists"):
            return f"backend guard missing at {path}"
        if self.project() is None:
            return (f"project {man.get('project')!r} is no longer in the "
                    "machine config")
        problem = self.container_unlisted()
        if problem:
            return problem
        if ident["sha256"] != man["backend"]["sha256"]:
            return (f"backend guard changed since submission "
                    f"({man['backend']['sha256'][:12]} -> "
                    f"{ident['sha256'][:12]})")
        if ident["sha256"] != self.config.guard_sha256:
            return (f"backend guard {ident['sha256'][:12]} is not the "
                    f"verified {self.config.guard_sha256[:12]}")
        return self.baseline_problem() or self.code_changed()

    def baseline_problem(self) -> str | None:
        """A49: the guard's ``load_baseline`` reads a baseline table it
        cannot examine (Python 3.14: ``Path.is_file`` answers False for a
        ``PermissionError``) as an empty one, so a job with a declared
        reserve would be admitted without the case's recorded peak.  The
        engine is pinned (and unchanged), so the runner refuses before the
        guard starts: absence (ENOENT/ENOTDIR) is a legitimate first run."""
        path = (self.manifest.get("paths") or {}).get("baseline")
        if not isinstance(path, str) or not path:
            return None
        try:
            exists_strict(path)
        except OSError as error:
            return (f"the baseline table {path} cannot be examined "
                    f"({error.strerror or error}); the guard would read it "
                    "as empty")
        return None

    def code_changed(self) -> str | None:
        """G3b: a placed or pinned job's code requirement
        (``--require-commit``/``--require-clean``, recorded in the
        manifest), checked again at dispatch against the checkout as it is
        now; an unreadable checkout refuses."""
        code = self.manifest.get("code")
        if not isinstance(code, dict) or not (
                code.get("required_commit") or code.get("require_clean")):
            return None
        worktree = self.manifest.get("worktree")
        head, dirty = checkout_state(Path(worktree)) if worktree else \
            (None, None)
        return code_problem({**code, "head": head, "dirty": dirty}, worktree,
                            "at dispatch")

    def names_changed(self) -> str | None:
        """G2a: the job runs with the guard home and marker names recorded
        in its manifest at submission; a config that changed them since
        refuses the job (never started).  A manifest without them (written
        before 0.2.0) is never run by this version: refused too."""
        man = self.manifest
        if not isinstance(man.get("guard_home"), str) or \
                not isinstance(man.get("marker"), str):
            return ("the manifest records no guard home or marker names "
                    "(not written by this version)")
        now = {"guard home": str(self.config.effective_guard_home),
               "marker names": [self.config.markers[0],
                                *self.config.markers[1:]],
               "legacy workload names": self.config.legacy_workload_env}
        then = {"guard home": man["guard_home"],
                "marker names": [man["marker"],
                                 *(man.get("marker_aliases") or [])],
                "legacy workload names":
                    bool(man.get("legacy_workload_env"))}
        changed = [k for k in now if now[k] != then[k]]
        if changed:
            return (f"the {', '.join(changed)} changed since submission "
                    f"({', '.join(f'{then[k]!r} -> {now[k]!r}' for k in changed)})")
        return None

    def container_unlisted(self) -> str | None:
        """C6: the runner loads the live config at dispatch, so the
        project must still list the job's container (the submit-time
        allowlist check, repeated)."""
        container = self.manifest.get("container")
        if not isinstance(container, dict):
            return None
        project = self.project()
        name = container.get("name")
        if project is None:
            return (f"project {self.manifest.get('project')!r} is no longer "
                    "in the machine config")
        if name not in project.containers:
            return (f"container {name!r} is no longer listed for project "
                    f"{project.name} (containers: {project.containers})")
        return None

    def inputs_changed(self) -> str | None:
        man = self.manifest
        recorded = man["inputs"]["fingerprint"]
        if not Path(man["cwd"]).is_dir():
            # A3: the runner itself runs in `/`; the guard needs the cwd
            return f"the job's --cwd {man['cwd']} no longer exists"
        try:
            now = inputs_fingerprint(Path(man["worktree"]), Path(man["cwd"]),
                                     man["argv"], man["inputs"]["pathspecs"],
                                     man["inputs"]["extra"])
        except (OSError, RuntimeError) as error:
            return f"inputs cannot be re-fingerprinted: {error}"
        if now["sha256"] != recorded["sha256"]:
            changed = [k for k in ("git_head", "diff_sha256",
                                   "untracked_sha256", "operands",
                                   "extra_inputs")
                       if now.get(k) != recorded.get(k)]
            return ("inputs changed while queued "
                    f"({', '.join(changed) or 'operand content'})")
        container = man.get("container")
        if container:
            if container.get("daemon_id") and \
                    procs.docker_daemon_id() != container["daemon_id"]:
                return ("the docker daemon is not the one recorded at "
                        "submission (or cannot be identified)")
            cid = procs.container_id(container["name"])
            if cid != container.get("id"):
                return (f"container {container['name']} is not the one "
                        "recorded at submission")
            if container.get("provenance_path"):
                # A13: by the recorded id (just checked), not the name
                current = container_provenance(container["id"],
                                               container["provenance_path"])
                if current != container.get("provenance_sha256"):
                    return "container provenance file changed while queued"
        return None

    def lock_users(self) -> list[int] | None:
        return procs.lock_file_users(self.config.effective_guard_home / "lock")

    def lock_holder(self) -> str | None:
        current = read_json(self.config.effective_guard_home / "current.json")
        if not isinstance(current, dict):
            return None
        return f"{current.get('job_id')} (pid {current.get('pid')})"

    def inflight_marker(self) -> dict | None:
        return read_inflight(self.config.effective_guard_home)

    def try_admission_lock(self) -> bool:
        if self.admission_fd is None:
            self.admission_fd = os.open(self.config.state_dir /
                                        "admission.lock",
                                        os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(self.admission_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False

    # -- guard invocation ---------------------------------------------------------
    def profile_file(self) -> Path:
        """The per-job machine profile of the internal guard, written from
        the config this runner loaded (``agent-compute-guard-profile/1``)."""
        path = self.dir / PROFILE_FILE
        write_json(path, dict(self.config.profile))
        return path

    def project(self):
        return self.config.projects.get(self.manifest.get("project"))

    def guard_argv(self, attempt_dir: Path, wait_s: float) -> list[str]:
        man, req = self.manifest, self.manifest["resources"]
        # S5a: a probe runs the guard's own `probe` mode (the same
        # admission, lock and monitoring as `run`, stopped at --probe-s)
        mode = "probe" if req.get("probe") else "run"
        argv = [self.config.python, str(self.backend_path()), mode]
        # the engine refuses run/probe without the claim of the job it is
        # run for (cooperative: it checks that this runner is the job's
        # claimed runner, its parent)
        argv += ["--acs-job", self.job_id,
                 "--acs-state", str(self.config.state_dir),
                 "--profile-file", str(self.profile_file()),
                 "--project-root", man["worktree"]]
        argv += ["--case", man["guard_case"],
                 "--cwd", man["cwd"],
                 "--outputs", str(attempt_dir),
                 "--runlog", man["paths"]["runlog"],
                 "--baseline", man["paths"]["baseline"],
                 "--timeout-min", f"{req['timeout_min']:g}",
                 "--threads", str(req["threads"]),
                 "--wait", f"{max(1.0, wait_s):.0f}"]
        if req.get("probe"):
            probe_s = float(req.get("probe_s") or DEFAULT_PROBE_S)
            argv += ["--probe-s", f"{probe_s:g}"]
        # the config the runner loaded when it started decides: with the
        # queue that is at dispatch (a declaration made or removed while a job
        # is queued applies to it); without the queue the runner starts at
        # submit. The guard record's thresholds.budget_gb is the evidence.
        if self.config.dedicated_budget_gb is not None:
            argv += ["--budget-gb", f"{self.config.dedicated_budget_gb:g}"]
        if req.get("reserve_gb") is not None:
            argv += ["--reserve-gb", f"{req['reserve_gb']:g}"]
        if req.get("dense_threads") is not None:
            argv += ["--dense-threads", str(req["dense_threads"])]
        if man.get("slice"):
            argv += ["--slice", man["slice"]]
        if man.get("units"):
            argv += ["--units", man["units"]]
        if man.get("container"):
            argv += ["--venue", "container",
                     "--container", man["container"]["name"],
                     "--container-workdir", man["container"]["workdir"]]
        # the names recorded at submission (revalidate: still the config's)
        for alias in man.get("marker_aliases") or []:
            argv += ["--marker-alias", alias]
        if man.get("legacy_workload_env"):
            argv += ["--legacy-workload-env"]
        project = self.project()
        if project is None:
            raise RuntimeError(f"project {man.get('project')!r} is no "
                               "longer configured")
        if project.detector:
            argv += ["--reserve-detector", project.detector]
        if man.get("container") and \
                man["container"]["name"] in (project.container_limits or ()):
            argv += ["--container-limits"]
        return argv + ["--", *man["argv"]]

    def guard_env(self) -> dict[str, str]:
        # never the guard's control variables, nor the internal guard's
        # test-only claim bypass (ACS_GUARD_*): the runner gives the claim
        env = {k: v for k, v in self.manifest["env"].items()
               if not k.startswith((GUARD_ENV_PREFIX, ACS_GUARD_PREFIX))}
        env.update(self.config.guard_extra_env)
        env[JOB_ENV] = self.job_id
        # the guard home recorded at submission (revalidate: equal to the
        # config's), always explicit: no guard falls back to its own default
        home = self.manifest.get("guard_home") or \
            str(self.config.effective_guard_home)
        env[GUARD_HOME_ENV] = home
        return env

    def workload_from_record(self, record) -> dict | None:
        return workload_from_record(self.manifest, record)

    def capture_workload(self, attempt_dir: Path) -> None:
        """Record the workload identity from the guard record.  A host
        workload's exact start is retried on every poll until it is
        captured (a failed ``ps``, or a first read that did not match the
        guard's pid and ``started_utc``), never given up after one try."""
        man = self.manifest
        if self.workload and (man.get("container") or
                              procs.has_exact_start(self.workload)):
            return
        record = read_json(attempt_dir / "guard.json")
        if not isinstance(record, dict) or \
                not procs.valid_pid(record.get("pid")):
            return
        if self.workload:
            self._retry_exact_start(record)
            return
        if man.get("container"):
            if not procs.valid_pid(record.get("container_pid")):
                return      # the guard is still resolving the in-container pid
            workload = self.workload_from_record(record)
            scan = procs.container_scan(man["container"]["id"],
                                        record["container_pid"])
            if scan and scan.get("start"):
                workload["container_start"] = scan["start"]
                workload["container_start_epoch"] = scan.get("epoch")
            workload["client"] = procs.identity(record["pid"])
            self.check_container_name(man["container"])
        else:
            workload = self.workload_from_record(record)
            ident = self._exact_identity(record, workload)
            if ident:
                workload.update(ident)
        self.workload = workload
        self.mark_launch()
        self.update(phase="executing", workload=self.workload,
                    admitted_utc=record.get("started_utc") or utc_now(),
                    wait_reason=None,
                    reason="guard admitted the job and launched the workload")
        log(self.dir, f"executing: {self.workload}")

    def check_container_name(self, container: dict) -> None:
        """A13: the guard execs by NAME after ACS checked the id.  Once it
        has admitted, compare the recorded id with the container the name
        is bound to now; a mismatch is logged and recorded (the cleanup
        scan also scans a container rebound to the name)."""
        bound = procs.container_id(container["name"])
        check = {"utc": utc_now(), "name": container["name"],
                 "recorded_id": container.get("id"), "bound_id": bound,
                 "match": bound is not None and bound == container.get("id")}
        self.update(container_name_check=check)
        if not check["match"]:
            log(self.dir, f"WARNING: after admission the container name "
                          f"{container['name']} is bound to "
                          f"{(bound or 'nothing readable')[:12]}, not the "
                          f"recorded {str(container.get('id'))[:12]} (rebound "
                          "or unreadable); the guard execs by name, so "
                          "cleanup also scans the rebound container")

    def _exact_identity(self, record: dict, workload: dict) -> dict | None:
        """The live identity at the guard's pid, if it has an exact start
        that matches the guard's ``started_utc``."""
        ident = procs.identity(record["pid"])
        if not procs.has_exact_start(ident):
            return None
        guard_epoch = procs.utc_to_epoch(record.get("started_utc")) \
            if isinstance(record.get("started_utc"), str) else None
        if guard_epoch is not None and (
                ident.get("start_epoch") is None or
                abs(ident["start_epoch"] - guard_epoch) >
                CAPTURE_TOLERANCE_S):
            return None
        return ident

    def _retry_exact_start(self, record: dict) -> None:
        workload = self.workload
        if record.get("pid") != workload.get("pid"):
            return
        ident = self._exact_identity(record, workload)
        if not ident:
            return
        workload.update(ident)
        self.update(workload=workload)
        log(self.dir, f"exact workload start captured: {workload}")

    def prove_workload(self, force: bool = False) -> None:
        """Re-prove the host workload's exact-start leader alive and record
        the moment (taken before the proof) as ``workload_seen_utc`` (+
        uptime ticks on Linux): at most every PROOF_INTERVAL_S, or now if
        ``force``.  It bounds which orphaned group members ``reconcile
        --terminate`` may signal once the leader is gone."""
        workload = self.workload
        if not workload or workload.get("venue") != "host" or \
                not procs.has_exact_start(workload):
            return
        if not force and time.time() - self.last_proof < PROOF_INTERVAL_S:
            return
        mark = procs.seen_mark()
        self.last_proof = time.time()
        if procs.match(workload) != procs.ALIVE:
            return
        self.seen = mark
        self.update(**mark)

    def observe_launch(self, attempt_dir: Path) -> None:
        """G2a: once the guard has begun launching (a pid in its record, or
        its samples.csv) the job is no longer waiting for admission, even
        before its identity is captured (a container's in-container pid
        can take up to 30 s, or never come).  Shown once as phase
        ``launched`` with that wait reason; the capture moves it to
        ``executing`` (which still means: identity recorded) and clears
        the reason."""
        if self.workload is not None or self.launch_noted:
            return
        if not self.launch_began(attempt_dir):
            return
        self.mark_launch()
        self.launch_noted = True
        reason = ("the guard launched the workload; identity not yet "
                  "captured")
        self.update(phase="launched", wait_reason=reason, reason=reason)

    def launch_began(self, attempt_dir: Path) -> bool:
        record = read_json(attempt_dir / "guard.json")
        if record is not None and not isinstance(record, dict):
            return True                 # malformed: assume the worst
        try:
            sampled = lexists_strict(attempt_dir / "samples.csv")
        except OSError:
            return True                 # A49: cannot tell: assume the worst
        return bool(self.workload or (record or {}).get("pid") or sampled)

    def run_guard_attempt(self, attempt: int, wait_s: float) -> tuple[int, Path]:
        attempt_dir = self.dir / "guard" / f"attempt-{attempt}"
        attempt_dir.mkdir(parents=True, exist_ok=True)
        argv = self.guard_argv(attempt_dir, wait_s)
        log(self.dir, f"guard attempt {attempt}: {argv}")
        with open(self.dir / "output.log", "ab") as out:
            self.guard_proc = subprocess.Popen(
                argv, env=self.guard_env(), cwd=self.manifest["cwd"],
                stdin=subprocess.DEVNULL, stdout=out, stderr=out)
        self.update(guard=procs.identity(self.guard_proc.pid),
                    attempt=attempt)
        signalled = False
        cancel_at = None
        while self.guard_proc.poll() is None:
            self.capture_workload(attempt_dir)
            self.observe_launch(attempt_dir)
            self.prove_workload()
            request = self.cancel_request()
            if request and not signalled:
                if request.get("outcome") == "admission-timeout" and \
                        self.launch_began(attempt_dir):
                    pass        # too late for an admission timeout: let it run
                else:
                    log(self.dir, "cancel requested: SIGTERM to the guard")
                    self.guard_proc.send_signal(signal.SIGTERM)
                    signalled, cancel_at = True, time.time()
            if cancel_at and time.time() - cancel_at > CANCEL_GRACE_S:
                # The guard did not finish its own kill path: act on the
                # identified workload directly, then on the guard.
                log(self.dir, "guard did not stop in time; signalling the "
                              "identified workload and the guard")
                workload = self.workload or self.workload_from_record(
                    read_json(attempt_dir / "guard.json"))
                if workload:
                    procs.signal_workload(
                        workload, signal.SIGKILL,
                        (self.manifest.get("container") or {}).get(
                            "daemon_id"),
                        markers=procs.scan_markers(self.manifest))
                self.guard_proc.kill()
                cancel_at = None
            time.sleep(POLL_S)
        if self.launch_began(attempt_dir):
            self.mark_launch()          # launched between two polls
            self.t_guard_exit = time.monotonic()
        self.capture_workload(attempt_dir)
        self.prove_workload(force=True)     # the last proof before cleanup
        return self.guard_proc.returncode, attempt_dir

    # -- main --------------------------------------------------------------------
    def run(self) -> int:
        os.umask(0o077)
        try:
            self.claim()
        except Unclaimable as error:
            # C4: neither claimed nor finished (its state is not ours to
            # overwrite); the host is blocked for it on the same evidence
            log(self.dir, f"not claimed: the job state cannot be read "
                          f"({error}); the host is blocked for it")
            if self.store.block_if_unreadable(
                    self.host, self.job_id,
                    "the job's state cannot be read at dispatch (it may "
                    "hide another runner's claim or an outcome); an operator "
                    "inspects it", {"detected_by": self.job_id}):
                from .queue import pause_group_quietly
                pause_group_quietly(self.config)
            return RC["duplicate-dispatch"]
        except Duplicate as dup:
            prior = dup.args[0]
            if isinstance(prior, str):
                log(self.dir, prior)
                return RC["duplicate-dispatch"]
            if procs.liveness(prior) != procs.GONE:
                log(self.dir, "duplicate dispatch refused: the original "
                              "runner may still be supervising")
                return RC["duplicate-dispatch"]
            # A9: block only if, re-read under the state lock, the job is
            # still unfinished and still claimed by that prior runner
            if self.store.block_if_unfinished(
                    self.host, self.job_id, prior,
                    "runner lost while the job was unfinished; a second "
                    "dispatch was refused", {"prior_runner": prior}):
                log(self.dir, f"HOST BLOCKED ({self.host}): runner lost; "
                              "a second dispatch was refused")
                from .queue import pause_group_quietly
                pause_group_quietly(self.config)
            else:
                log(self.dir, "duplicate dispatch refused: the job finished "
                              "or was re-claimed meanwhile")
            return RC["duplicate-dispatch"]
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._on_signal)
        deadline = time.time() + float(self.manifest["admission_wait_s"])
        attempt = 0
        retry_s = self.config.admission_retry_s
        held = False
        last_reason = None
        pending_unknown: dict = {}

        def waiting(phase: str, reason: str) -> None:
            nonlocal last_reason
            if (phase, reason) != last_reason:
                self.update(phase=phase, wait_reason=reason, reason=reason)
                last_reason = (phase, reason)

        while True:
            if self.cancel_request():
                return self.never_started_cancel(
                    "while waiting for admission")
            gate = self.store.gate()
            if isinstance(gate, Unreadable):
                # C4: an unreadable gate holds dispatch like a closed one
                waiting("dispatch-held", "dispatch gate record "
                        f"{gate.reason}; an operator inspects it")
                self._sleep_cancellable(2.0)
                continue
            if gate is not None:
                waiting("dispatch-held",
                        f"dispatch gate closed: {gate.get('reason')}")
                self._sleep_cancellable(2.0)
                continue
            if time.time() > deadline:
                last = f" (last wait: {last_reason[1]})" if last_reason \
                    else ""
                self.block_unverified(pending_unknown)
                return self.finish("admission-timeout",
                                   "not admitted before the admission "
                                   f"deadline{last}; never started")
            if not held:
                if not self.try_admission_lock():
                    waiting("admission-wait", "another ACS job on this host "
                            "is being admitted, running or cleaned up")
                    self._sleep_cancellable(2.0)
                    continue
                held = True
            block = self.store.block(self.host)
            if isinstance(block, Unreadable):
                return self.finish("refused", f"host {self.host} admission "
                                   f"block record {block.reason}; blocked "
                                   "until an operator inspects it; never "
                                   "started")
            if block:
                return self.finish("refused", f"host {self.host} admission "
                                   "is blocked pending reconciliation "
                                   f"({', '.join(block['jobs'])}); never "
                                   "started")
            dead, unverified = self.prior_runners()
            unreadable = {other: mark["unreadable"]
                          for other, mark in unverified.items()
                          if _unreadable_mark(mark)}
            if unreadable:
                blocked = [other for other, why in unreadable.items()
                           if self.store.block_if_unreadable(
                               self.host, other,
                               f"the job's state cannot be read ({why}): it "
                               "may hide a live runner; nothing is admitted "
                               "beside it", {"detected_by": self.job_id})]
                if blocked:
                    from .queue import pause_group_quietly
                    pause_group_quietly(self.config)
                    log(self.dir, f"HOST BLOCKED ({self.host}): unreadable "
                                  f"job state(s) {blocked}")
                    return self.finish("refused", "unreadable job state(s) "
                                       f"{blocked}; host blocked; never "
                                       "started")
                self._sleep_cancellable(1.0)    # readable again: re-check
                continue
            pending_unknown = unverified
            lost = [other for other, runner in dead.items()
                    if self.block_lost(other, runner)]
            if lost:
                return self.finish("refused", f"lost-supervision jobs "
                                   f"{lost}; host blocked; never started")
            if unverified:
                # A2: never proceed on an unknown; wait (a failed ps
                # recovers) until the admission deadline, then refuse
                waiting("admission-wait", "cannot verify that the runner of "
                        f"unfinished job(s) {', '.join(unverified)} is gone "
                        "(process check failed or no matching recorded "
                        "start); not admitting beside it")
                self._sleep_cancellable(2.0)
                continue
            problem = self.revalidate()
            if problem:
                return self.finish("refused", problem + "; never started")
            problem = self.inputs_changed()
            if problem:
                return self.finish("held-inputs-changed",
                                   problem + "; resubmit against the new "
                                   "inputs; never started")
            users = self.lock_users()
            if users:
                holder = self.lock_holder()
                waiting("admission-wait", "compute lock held by "
                        f"{holder or 'pid ' + ','.join(map(str, users))}")
                self._sleep_cancellable(LOCK_POLL_S)
                continue
            attempt += 1
            waiting("admission-wait", "guard pre-flight")
            rc, attempt_dir = self.run_guard_attempt(
                attempt, min(GUARD_WAIT_SLICE_S, max(1.0,
                                                     deadline - time.time())))
            outcome = self.classify(rc, attempt_dir)
            if outcome == "retry-lock":
                marker = self.inflight_marker()
                if marker is not None:
                    # the internal guard refuses while a previous workload
                    # is not proven gone (S3 in-flight marker).  Back off
                    # like a resource refusal (each attempt is a guard
                    # process and a guard record; no runlog line since D3)
                    waiting("admission-wait", "the internal guard's in-flight "
                            f"marker ({marker.get('job_id')}, ACS job "
                            f"{marker.get('acs_job')}) is not proven gone; "
                            "`agent-compute status` shows it and "
                            "`agent-compute reconcile` resolves it on "
                            f"evidence; next attempt in {retry_s:.0f} s")
                    self._sleep_cancellable(retry_s)
                    retry_s = min(retry_s * 2,
                                  self.config.admission_retry_max_s)
                continue                         # lost a race for the lock
            if outcome == "retry-refusal":
                # each refused attempt is a guard process and a guard record
                # (attempt-N/guard.json, its `refusal`), so back off
                # (doubling, capped) instead of polling steadily
                if time.time() + retry_s < deadline:
                    waiting("admission-wait", "insufficient host resources "
                            "(guard pre-flight refusal: "
                            f"{self.refusal_reason()}); next attempt in "
                            f"{retry_s:.0f} s")
                    self._sleep_cancellable(retry_s)
                    retry_s = min(retry_s * 2, self.config.admission_retry_max_s)
                    continue
                return self.finish("admission-timeout",
                                   "resources stayed insufficient until the "
                                   "admission deadline; never started")
            return outcome

    def _sleep_cancellable(self, seconds: float) -> None:
        end = time.time() + seconds
        while time.time() < end and not self.cancel_request():
            time.sleep(POLL_S)

    def refusal_reason(self) -> str:
        try:
            lines = (self.dir / "output.log").read_text(
                errors="replace").splitlines()
        except OSError:
            return "guard refusal"
        for line in reversed(lines):
            if "REFUSED:" in line:
                return line.split("REFUSED:", 1)[1].strip()
        return "guard refusal"

    def classify(self, rc: int, attempt_dir: Path):
        raw = read_json(attempt_dir / "guard.json")
        record = raw if isinstance(raw, dict) else {}
        if not self.launch_began(attempt_dir):
            # The guard opens samples.csv before it launches anything, so no
            # pid and no samples.csv proves the workload never started.
            if rc == EXIT_REFUSED:
                reason = self.refusal_reason()
                if any(key in reason for key in RETRYABLE_REFUSALS):
                    return "retry-refusal"
                if "no measured peak" in reason:
                    # the guard names its own probe command; through ACS
                    # it is `--probe` (S5a)
                    reason += (" [through ACS: submit it with `--probe` "
                               "first, or declare `--reserve-gb`]")
                return self.finish("refused", f"guard refused: {reason}; "
                                              "never started")
            if rc == EXIT_LOCKED:
                return "retry-lock"
            if self.cancel_request():
                return self.never_started_cancel("during guard admission")
            if rc < 0:
                return self.finish("interrupted", f"guard ended by signal "
                                   f"{-rc} before launching; never started")
            return self.finish("refused", f"guard exited {rc} before "
                               "launching (see output.log); never started")
        # The guard records the launch client's pid as soon as Popen
        # returns, so only a launch failure WITHOUT a pid proves nothing
        # started.  With a pid (e.g. the in-container pid read timed out and
        # launch() raised) the workload may be running: verify it.
        if isinstance(raw, dict) and raw.get("pid") is None and (
                raw.get("outcome") == "launch-failed" or rc == EXIT_VENUE):
            return self.finish("launch-failed", "the workload could not be "
                               "launched (missing executable or venue); "
                               "never started", guard_record=str(attempt_dir))
        # the workload was (or may have been) launched: verify cleanup
        self.update(phase="cleanup", wait_reason=None,
                    reason="verifying workload cleanup")
        evidence = self.verify_cleanup(raw)
        guard_outcome = record.get("outcome")
        extra = {"guard_record": str(attempt_dir / "guard.json"),
                 "guard_outcome": guard_outcome,
                 "exit_code": record.get("exit_code"),
                 "wall_s": record.get("wall_s"),
                 "peak_rss_gb": record.get("peak_rss_gb"),
                 "cleanup": evidence}
        guard_cleanup = record.get("cleanup")
        if "cleanup" in record:
            extra["guard_cleanup"] = guard_cleanup
        guard_unclean = guard_outcome in AMBIGUOUS_GUARD_OUTCOMES or (
            isinstance(guard_cleanup, dict) and
            guard_cleanup.get("clean") is not True)
        if evidence.get("clean") is not True or guard_unclean:
            # reported apart from the scientific result: exit_code and
            # guard_outcome stay in the state; the outcome is unknown.  The
            # internal guard's own verdict counts too (S4): it can see
            # members ACS cannot (Linux session members, its kill path)
            reason = ("workload cleanup could not be established after the "
                      f"guard exited (guard rc {rc}, guard outcome "
                      f"{guard_outcome})")
            if guard_unclean and evidence.get("clean") is True:
                reason = ("the guard could not verify its workload's cleanup "
                          f"(guard outcome {guard_outcome}, guard cleanup "
                          f"{(guard_cleanup or {}).get('reason')}); ACS's "
                          "own check found nothing, but the end stays "
                          "ambiguous")
            self.block(reason, {"workload": self.workload, "cleanup": evidence,
                                "guard_cleanup": guard_cleanup,
                                "guard_record": str(attempt_dir)})
            return self.finish("unknown", reason, **extra)
        if rc < 0 or guard_outcome in (None, "running"):
            return self.finish("unknown", f"guard ended abnormally (rc {rc}); "
                               "workload verified gone; result unknown — do "
                               "not resubmit without checking its outputs",
                               **extra)
        outcome = GUARD_OUTCOME.get(guard_outcome, "unknown") \
            if isinstance(guard_outcome, str) else "unknown"
        if guard_outcome == "probe-stopped" and \
                not (self.manifest.get("resources") or {}).get("probe"):
            outcome = "unknown"         # a stop ACS did not ask for
        if outcome == "interrupted" and self.cancel_request():
            outcome = "cancelled"
        return self.finish(outcome, f"guard outcome {guard_outcome}, exit "
                                    f"{record.get('exit_code')}", **extra)

    def verify_cleanup(self, record: dict) -> dict:
        """Cleanup evidence after the guard exits.  A container job is
        always judged by ``procs.container_job_evidence`` (the same function
        ``reconcile`` uses), never from the guard record alone."""
        problem = procs.record_problem(record)
        if problem:
            return {"verified": False, "clean": False, "reason": problem}
        record = record if isinstance(record, dict) else {}
        if self.workload is None:
            self.workload = self.workload_from_record(record)
        container = (self.manifest or {}).get("container")
        if container:
            def check():
                return procs.container_job_evidence(
                    container, record, self.workload,
                    markers=procs.scan_markers(self.manifest))
        elif self.workload is None:
            return {"verified": False, "clean": False,
                    "reason": "the guard began launching (samples.csv exists) "
                              "but recorded no workload identity"}
        else:
            def check():
                return procs.host_survivors(self.workload)
        evidence = {}
        for _ in range(10):         # allow a short exit race to settle
            evidence = check()
            if evidence.get("clean") is True or not evidence.get("verified"):
                break
            time.sleep(0.5)
        evidence.setdefault("clean", False)
        return evidence


def _view(record):
    """A record for embedding in evidence (an ``Unreadable`` as its view)."""
    return record.view() if isinstance(record, Unreadable) else record


def _unreadable_mark(value) -> bool:
    """``prior_runners``' entry for a job whose state cannot be read."""
    return isinstance(value, dict) and set(value) == {"unreadable"}


def workload_from_record(manifest, record) -> dict | None:
    """The workload identity the guard recorded (no exact start time).  A
    container workload is returned even without ``container_pid``: its
    guard job id is the marker that identifies the job's processes."""
    if not isinstance(record, dict):
        return None
    man = manifest if isinstance(manifest, dict) else {}
    started = record.get("started_utc") \
        if isinstance(record.get("started_utc"), str) else None
    job = record.get("job_id") if isinstance(record.get("job_id"), str) \
        else None
    container = man.get("container")
    if isinstance(container, dict):
        if not procs.valid_pid(record.get("pid")) and not job:
            return None
        return {"venue": "container", "guard_job_id": job,
                "container": container.get("name"),
                "container_id": container.get("id"),
                "container_pid": procs.valid_pid(record.get("container_pid")),
                "started_utc": started,
                "container_start": None,
                "container_start_epoch": procs.utc_to_epoch(started)}
    if not procs.valid_pid(record.get("pid")):
        return None
    return {"venue": "host", "guard_job_id": job,
            "pid": record["pid"], "start": None,
            "start_epoch": procs.utc_to_epoch(started)}


def read_inflight(guard_home: Path) -> dict | None:
    """The internal guard's in-flight marker, read-only; None when absent.
    An unreadable or malformed one is returned as ``{"malformed": ...}``
    (never read as absent)."""
    path = Path(guard_home) / INFLIGHT_FILE
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


# A20: hashed by the same bounded helper as the scans (from "/", python3
# -I -S, alarm first), not by `sha256sum` in the container's WorkingDir.
# The digest is the one sha256sum printed, so recorded values still match.
_PROVENANCE = procs.bounded(r"""
import hashlib, json, sys
digest = hashlib.sha256()
with open(sys.argv[1], 'rb') as stream:
    for chunk in iter(lambda: stream.read(1 << 20), b''):
        digest.update(chunk)
print(json.dumps({'sha256': digest.hexdigest()}))
""")


def container_provenance(container: str, path: str) -> str | None:
    """sha256 of ``path`` inside ``container`` (pass the container ID), or
    None (fail-closed).  The helper runs from "/", so a relative path is
    refused rather than resolved against a directory nobody chose."""
    if not isinstance(path, str) or not path.startswith("/"):
        return None
    answer, _ = procs.container_python(container, _PROVENANCE, path)
    digest = answer.get("sha256") if answer else None
    if not isinstance(digest, str) or len(digest) != 64 or \
            digest.strip("0123456789abcdef"):
        return None
    return digest


def main(config: Config, job_id: str) -> int:
    runner = Runner(config, job_id)
    try:
        return runner.run()
    except Exception as error:  # noqa: BLE001 - the job must not vanish
        import traceback
        log(runner.dir, "runner crashed:\n" + traceback.format_exc())
        state = runner.store.state(job_id)
        if isinstance(state, Unreadable):
            # C4: whether it launched or ended is unknown and the state is
            # not overwritten: block the host and leave the rest to reconcile
            runner.block(f"runner crashed ({type(error).__name__}) and the "
                         f"job state {state.reason}",
                         {"workload": runner.workload,
                          "guard_invoked": runner.guard_proc is not None})
            raise
        state = state or {}
        if state.get("outcome"):
            raise
        if runner.guard_proc is not None or state.get("workload"):
            guard_alive = runner.guard_proc is not None and \
                runner.guard_proc.poll() is None
            runner.block(f"runner crashed ({type(error).__name__}) after "
                         "invoking the guard", {"workload": runner.workload,
                                                "guard_alive": guard_alive})
            return runner.finish("unknown", f"runner crashed: {error}")
        return runner.finish("refused", f"runner crashed before admission: "
                                        f"{error}; never started")


if __name__ == "__main__":
    from .config import load
    sys.exit(main(load(), sys.argv[1]))
