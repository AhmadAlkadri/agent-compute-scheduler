# Supported platforms and execution paths

"Supported" means the path is guarded, queued (one shared slot per host),
cleanup-verified, and covered by the full suite in the standard test
profile on that platform: every ACS job of the suite runs through the
internal guard of the tree under test (`src/agent_compute/guard/engine.py`,
backend `acs-guard`, the only backend since `0.2.0`), with an isolated
guard home, state and queue. What has actually been used for real, and
what has only been tested, is in `docs/limitations.md`.

| Host | Venue / path | Status | Tests | Notes |
|---|---|---|---|---|
| macOS (arm64) | host process through ACS (queue → runner → internal guard) | **supported** | the standard profile, run on macOS | macOS tracks the process group only (below) |
| macOS | container: `docker exec` into a **listed** container through the guard's container venue | **supported, with caveats** (below) | the Docker-backed tests, disposable containers from the public test image | needs Docker Desktop |
| Linux (x86_64; tested on Ubuntu 24.04 under WSL2, native Linux expected to work but not separately validated) | host process, admitted on the host's own lock, guard and queue; driven from another host (`--host <label>`, or placement) | **supported** | the standard profile, run on Linux (WSL2) | native Linux is not separately validated |
| Linux | container through the guard's container venue | **supported, with caveats** | the same | |
| host A → host B | **pinned** remote submit `--host B submit` with a local placement record (`<state>/placements/<request-id>.json`, written before the call), the `on.<host>` mapping and the code requirement when submitted from a checkout here; `status`/`wait`/`cancel`/`reconcile --terminate`/`log`/`fetch` routed to the recorded host; the remote exit code in band | **supported** | fake-ssh tests on both platforms (`tests/test_placement.py`, `tests/test_remote.py`) | live-used from the maintainer's macOS host to the Linux one; not the reverse |
| any → any configured host | **automatic placement**: an unpinned submit of a project that declares several hosts (`projects.<p>.hosts`), placed by the fewest-unfinished-jobs policy (tie: local, then config order), sent once, no retry or fallback | **experimental**: tested with a fake ssh, not exercised in production | `tests/test_g3b_placement.py` | |
| any | **lost-reply resolution**: `reply-lost` resolved on the same host by request id (`placements resolve`, `status R`, `status --resolve`, `hosts`; `_request` read-only there), `resolved-not-accepted` only after the send's call timeout plus 900 s with the placer and its ssh client proven gone | **supported** in the harness | `tests/test_g3b_placement.py` | |
| any | `hosts` (every configured host's read-only summary, in parallel, with identity checks) | **supported** | `tests/test_placement.py` | an `0.1.x` remote is reported `old-remote`, ineligible |
| either | probe through ACS: `agent-compute submit/run --probe [--probe-s S]` (host and container venues) | **supported** (tested, not used in production) | `tests/test_probe_and_nested.py` | |
| either | `agent-compute submit` from inside a running ACS job's host workload (a step submitting its successor) | **supported** (tested, not used in production): queued behind the running job, `parent_job` recorded; `run`/`wait` there refused (exit 2) | the same | |
| either | a direct caller of an external guard sharing the guard home | interop only | mutual exclusion in both orders (legacy test profile) | allowed by that guard; outside queue order; **does not honor ACS blocks** |
| either | a project's own light lane (work run beside the slot, outside ACS) | outside ACS by design | — | the project's rule |
| either | a project's own queue supervisor that starts heavy work by itself | not an ACS path | shares the lock only through a guard sharing the guard home | do not start new supervisors for heavy work on managed hosts |
| either | `systemd-run` / `nohup` drivers, or a host command that itself runs `docker exec` / `docker run -d` / `setsid` | **unsupported** | the orphaned-`docker exec` failure class is reproduced (`test_orphaned_docker_exec_regression`); the setsid limitations are pinned (host: `test_limitation_a_setsid_escape_is_not_tracked`; container: `test_limitation_env_scrubbed_setsid_descendant_escapes`) | forbidden by the skill |
| either | the retired `smc-guard` backend (an external guard script run by ACS) | **retired** in `0.2.0`: a config naming it does not load | historical | |

## Caveats of the container venue

- The workload's environment is only what the guard's `docker exec`
  passes (thread counts and its job variables). `--env` with
  `--container` is refused; put variables in the command
  (`-- env NAME=VALUE <command>`). A set `env_passthrough` variable is
  recorded as `container.env_not_forwarded` and reported.
- The guard execs by container **name**. ACS checks the id before the
  guard runs and reads the provenance file by id; after admission it
  records `container_name_check` and logs a rebound name, and the
  cleanup scan also scans a container rebound to the name.
- The guard applies per-job limits to the target container with
  `docker update --memory/--memory-swap/--cpus`. ACS accepts only containers listed for the project. The tests run
  only against disposable containers.
- **Historical guard defect (the retired `smc-guard` backend only, an
  external guard script)**: its in-container group kill used `kill -SIG
  -- -PGID`, which dash rejects, so only the leader was signalled on a
  guard cancel or timeout; ACS detected the survivors (`unknown`, host
  blocked). The internal guard never had it, and since `0.2.0` that script
  cannot be selected as a backend.
- ACS records the container id, and the in-container leader's pid and start
  ticks when the guard captured them. A project may name a container
  provenance file (`container_provenance`, for example a sync record inside
  the container): ACS records its hash, and a changed container or
  provenance holds a queued job.
- **Cleanup evidence never comes from the guard record.** The guard's
  `docker exec … setsid -w` forks under `docker exec` and waits only for
  the new session leader. For fast jobs the guard's in-container pid read
  usually loses the race (`container_pid: null`). ACS therefore scans the
  container, located by its recorded id, for the guard's job marker
  (`ACS_JOB_MARKER=<job marker>`, plus any alias the host configures:
  `SMC_GUARD_JOB` on a host configured for compatibility with an older
  guard; the job's manifest records which), which every descendant inherits and which survives `setsid()`. The job's set is the marker holders plus, to a
  fixpoint, every process that shares a member's session or process group
  or whose parent is a member, plus the recorded leader's session/group;
  it must be empty. The parent link catches a leader that dropped the
  marker (`exec env -i CMD`) and leads its own session while its marked
  `setsid -w` parent lives. ACS (`reconcile` and the runner's check) and,
  since `0.1.2`, the internal guard's
  own container helper apply it: the guard's members are the same closure,
  it never signals such an unmarked leader nor its marked parent (below), and the job then ends `cleanup-unverified` with the in-flight
  marker kept. Before `0.1.2` the guard's cancel killed that parent when the
  in-container pid was never captured, and both checks read clean.
- `--terminate` never signals an unmarked process *directly*. `killpg`
  through a proven marked group leader does reach every member of that
  group, marked or not; they all belong to the job. It never signals a
  marker holder that has an unmarked descendant (signalling it would erase the only link
  to that descendant, so the evidence would read clean while it runs). Such
  holders are reported under `skipped` ("unmarked child; handle
  manually"); the in-container helper re-checks this right before each
  kill. The host stays blocked until a human handles them.
- **Orphaned sessions are fail-closed.** Every `docker exec`'d process
  leads its own session, so a descendant that scrubbed the marker but
  stayed in the job's session is left, once the leader exits, in a
  session (or group) whose leader is gone. Any unmarked process that is
  not provably older than the job and whose sid or pgrp names no live
  process makes the evidence unverifiable (`cleanup.orphaned`). Their group
  leader is gone, so no `killpg` through a proven leader reaches them,
  and `--terminate` does not stop these: they
  need handling by hand, then `reconcile`. This can also block on
  unrelated orphaned processes started after the job in a shared
  container (a false block, never a false clean).
- A fast job that leaves nothing behind completes normally; one that
  leaves a descendant running blocks the host until that descendant is
  gone (then `reconcile`) or is terminated.
- A process whose main thread called `pthread_exit` (a `Z` tgid with live
  threads) counts as live; its environ is read through a live thread.
- The scan needs `python3` in the container, and must be able to read
  `/proc/<pid>/environ`. A process of another uid (in-container root has no
  `CAP_SYS_PTRACE` by default) that may have started after the job makes
  the evidence unverifiable, so the host stays blocked. Processes that
  provably started before the job are ignored; the container clock offset
  is bounded by the scan itself.
- The guard execs by container **name**, while ACS records the id. If the
  name is now bound to a different running container, that container is
  marker-scanned too (and `--terminate` signals in whichever container
  holds marker processes). A name lookup error, or a rebound container
  that cannot be scanned (or no job marker), is unverifiable.
- The docker daemon's ID (`docker info`) is recorded at submission. The
  evidence, including "No such container", is trusted only from that
  daemon; a different or unreadable daemon is unverifiable, and a queued
  job is held (`held-inputs-changed`) if the daemon changed. Manifests from
  before this field say "daemon not verified" in their evidence.
- **A changed daemon ID makes older blocks permanently unverifiable** (a
  Docker Desktop factory reset, a recreated WSL VM or docker engine). ACS
  has **no** operator command to clear a block; `reconcile` clears only on
  evidence, and agents never edit ACS state. The procedure is manual and
  operator-only: inspect the host and its containers for the job's
  processes (its job marker, its recorded ids), stop any that
  remain, then remove that job's entry from
  `<state_dir>/blocks/<host>.json` by hand and record in
  the job's notes what was inspected. There is no audited mechanism for
  this yet.
- The job marker is `<case>/<UTC second>`, not unique per launch. Two
  launches of the same case in the same second (for example a direct call
  of an external guard and an ACS job) share it. For the evidence that only
  widens the set (fail-closed), but `reconcile --terminate` could then
  signal the other launch's processes, which carry the same marker. A
  per-launch nonce is a possible future change of the guard.

## Host venue identity

- **Discriminator.** Linux: `start = "ticks:N"` (start ticks since boot)
  plus `boot_id`, unchanged in form. macOS: `start_utc_s`, the kernel
  start second, read with `ps -o lstart` under `TZ=UTC0 LC_ALL=C` (PATH
  kept) and converted with `timegm`; `start` is left empty and
  `start_epoch` holds the same UTC second. Nothing depends on the
  checker's `TZ`, locale or the DST fall-back hour.
- **Legacy records** (`0.1.0-eb369eb937` and earlier on macOS: `start =
  "lstart:<local wall time>"`, `start_epoch = mktime` of it) are still
  read. Alive if the UTC start equals `start_epoch`, or if `ps` rendered
  in the system zone (TZ unset, as the runner rendered it) equals the
  string. Recycled only if the process at the pid started more than 12 h
  (the largest real zone lag) plus 3 s after the string read as UTC.
  Anything between is `unknown`, so a live legacy leader is never called
  recycled, whatever `TZ` the checker has. The 12 h bound assumes the
  recording runner's zone was a real one (UTC offsets in [−12 h, +14 h]).
  A contrived POSIX `TZ` string with a larger offset (e.g. `TZ=XXX20`) is
  excluded; the runner's environment is the scrubbed queue env without
  `TZ`, i.e. the Mac's system zone.
- **Plausibility.** A `start_utc_s` that is ≤ 0, has no `start_epoch`, or
  differs from `start_epoch` by more than 13 h is not treated as exact: the
  identity falls back to the epoch path (`unknown` on a mismatch).
- **Recycled → clean** needs positive evidence: an exact recorded start
  and a process at the pid that started after it beyond 3 s (Linux: more
  ticks in the same boot, or a different `boot_id`; pre-`boot_id` Linux
  records: fewer ticks and a recorded start before this boot). A mismatch
  in the other direction, an epoch-only identity (reconcile's
  `workload_from_record`, or a runner that never got an exact start) that
  does not match within 3 s, or a failed `ps` is `unknown` → unverified →
  the host stays blocked.
- **Members.** A group's members are never filtered by start time: a
  member cannot predate its setsid leader, and a clock stepped backwards
  after the capture (an NTP step, a WSL `hwclock -s`) must not hide live
  members.
- **Signals.** An exact-start live leader: `killpg` of its group. A leader
  pid that is free, with members left: never `killpg`. Had our group
  emptied, the pid been reused and the new process double-forked, its
  child would sit in a group with our id. A host block's time proves
  nothing here: a lost-supervision block can be written long after the
  runner died, the group emptied and the id was reused.
  - The bound is the runner's last **proof** that the exact-start leader
    was alive. While the guard runs, the runner re-proves it at most every
    10 s (and once more when the guard exits) and records the moment,
    taken before the proof, as `workload_seen_utc` (on Linux also
    `workload_seen_ticks`, the uptime in clock ticks, and
    `workload_seen_boot_id`). It is in the job state and in any block
    evidence the runner writes (`workload_seen`).
  - Argument: at the proof moment the leader was alive, so group id P was
    ours and could not be reused. A member that started before that
    moment and is still in group P with the same start has kept P
    occupied ever since, so P was never reused and the member is ours
    (a process cannot join group P from another session, and a process
    older than a reuse cannot have been forked by it).
  - So each member whose exact start is at or after the recorded leader
    start and strictly before the proof minus 1 s (the macOS start
    resolution is 1 s) is signalled by `os.kill`, re-proven (same start,
    same group) immediately before. On Linux the comparison is in start
    ticks against the proof's uptime ticks in the same boot, immune to
    wall-clock steps. A recorded `boot_id` that cannot be compared (the
    current one differs or cannot be read) gives no window at all:
    nothing is signalled, and fewer ticks at the pid are then `unknown`,
    never recycled.
  - Residual limits:
    - on macOS, and on Linux records without proof ticks, the window is
      wall-clock seconds. If the clock steps backwards by more than 1 s
      after the last proof, a process started after the proof (for
      example the child of a later reuse) can carry a start time before
      the proof and fall inside the window;
    - the "same start, still in group P" argument assumes group P's
      members descend from its session. A contrived process that is
      older than the proof and, after a reuse of P, `setpgid()`s itself
      into the new group P (only possible from inside that new session)
      would be taken for a member.
  - Without a proof (a runner from before this change, a leader that died
    before the first proof, the runner's own cancel path) or without an
    exact leader start (epoch-only or legacy records), nothing is
    signalled and `reconcile --terminate` reports why (`not_signalled`).
    An epoch-only match is reported (`leader_alive`) but never signalled;
    such a workload needs handling by hand.
- **Capture.** The runner retries `identity()` on every poll until it
  records an exact start matching the guard's pid and `started_utc`
  (within 5 s). A pid held by a process whose start does not match is
  never recorded as the workload.
- **Admission.** A prior unfinished job whose runner is `unknown`
  (a failed `ps`, or a pid held by a process that cannot be matched)
  never lets a job proceed. The runner waits, holding the ACS admission
  lock, with `wait_reason` "cannot verify that the runner of unfinished
  job(s) … is gone", and re-checks every 2 s. At the admission deadline
  it re-runs liveness on each such prior runner: still `unknown` → a
  durable host block (re-checked under the state lock, with the runner
  identity as evidence); gone → the lost-supervision block; matched
  again → no block. Then it ends `admission-timeout` ("never started"), with that
  reason. `reconcile` then lists the job with the blocker "cannot tell
  whether its runner is alive". **Operator procedure** (operator only;
  agents report it and stop): look at the recorded runner pid
  (`status JOB`, `blocks/<host>.json` evidence). If `ps` itself fails, fix
  that and run `reconcile` again. If the pid is held by a process that is
  not that job's `agent-compute` runner, and no process of the job remains
  (its recorded workload group/session, its guard, its container marker),
  the job is lost: remove its entry from `blocks/<host>.json` by hand
  (the manual procedure above) and record what was inspected in the job's
  notes. If it *is* the runner, wait for it.
- **Lost supervision.** `detect_lost`, the runner's pre-admission
  check and its duplicate-dispatch refusal re-read the job under the state
  lock before blocking: a job that finished, or whose runner identity
  changed, during the check is not blocked. `wait` reports "host blocked"
  only when the block exists.
- **Mixed versions.** Roll forward rather than back: `install.py activate`
  refuses a version below the installation's safety floor (the installer's
  minimum, and the level of the version that is current).
- **The activation floor's limit.** The floor (`src/agent_compute/safety.py`)
  is enforced only by this installer and later ones. An older checkout's
  `install.py` (for example `edfd726`'s) can still activate
  `0.1.0-eb369eb937` or older; operators deploy and roll back only from
  current `main`. The installed CLI backstops it read-only: `doctor`
  reports an ERROR, and `submit`/`run` refuse new work, when `current` is below
  `MIN_ACTIVATABLE_LEVEL` or below the highest installed level. A
  pre-floor version made current has no backstop of its own (its CLI
  predates it); a floored copy run by its path still reports it.
- **macOS `setpgid()` escape (limitation).** On
  macOS host tracking reads process groups only (`ps` has no `sid`
  column there), so a descendant that calls `setpgid()` into a new group
  of its own escapes tracking exactly like a `setsid()` one: once the
  leader exits, the evidence reads clean while that descendant runs. On
  Linux the session is tracked too, so only `setsid()` escapes there.
  The skill forbids commands that change their process group or
  session.

## Never-claimed jobs and request ids

- A Pueue task runs the runner in `/` (`pueue add -w /`), never in the
  job's cwd: a cwd removed while the job is queued can no longer make
  Pueue fail to spawn the runner. The runner gives the guard the
  manifest cwd; if it is gone the job ends `held-inputs-changed` (never
  started). Paths in the machine config must be absolute (the examples
  are; `--config` is made absolute when it is read).
- A job that no runner claimed is finished as never started when nothing
  can dispatch it any more: its task ended (`Done`, e.g. FailedToSpawn)
  or is missing from the running daemon (`launch-failed`), or it has no
  task at all 900 s after acceptance (`refused`: the submitting client
  died before enqueueing). `status`, `doctor`, `reconcile`, `resume` and
  `wait` do this; `cancel` does it at once. It is a compare-and-set under
  the state lock, and the runner's claim, under the same lock and before
  any guard call, refuses a finished job, so no runner can start a job
  after it was finished this way.
- Such a job no longer closes the dispatch gate on every daemon start.
- `submit` reserves the job and binds its `--request-id` in one
  state-lock section before building the manifest, so two concurrent
  submits with the same id cannot both be accepted.

## What each guarantee rests on

| Guarantee | Mechanism | Limit |
|---|---|---|
| Mutual exclusion | the guard's `flock` on the configured guard home's `lock` (`~/.smc-guard/lock` on a host configured for compatibility with an older guard), plus the ACS host admission lock that serialises ACS runners until outcome and block are written | direct callers of an external guard on the same home ignore ACS blocks |
| One slot, shared by all projects | one Pueue group, parallelism 1 | order applies only to ACS submissions |
| Workload identity | pid + exact start, independent of TZ/locale: Linux start ticks + boot id, macOS kernel start second in UTC; legacy local `lstart:` records still read (host); container id + the guard's job marker (`ACS_JOB_MARKER`, or the configured alias) in each process's environ, plus the in-container leader pid + start ticks when captured (container) | a process whose environ is scrubbed (`env -i`, an exec with a fresh environment, an overwritten environ area) loses the marker |
| Cleanup evidence | group/session membership of the recorded leader after the guard exits (host); one in-container `/proc` scan for marker holders plus their sessions and groups, the container located by recorded id (container). Runner and `reconcile` use the same function; the guard record alone never suffices | host: a descendant that calls `setsid()` escapes, and on macOS also one that calls `setpgid()` into a new group (macOS). Container: a descendant that **both** drops the marker from its environment **and** calls `setsid()` (leading a live, unmarked session of its own) escapes; an unmarked process in a session/group whose leader is gone blocks (fail-closed, may be a false block) and is never signalled by ACS; an env-scrubbed leader cannot be signalled either. The marker is not unique per launch (see above). There is **no cgroup containment on either host**; on macOS no hard containment is possible. Legacy callers ignore ACS blocks |
| Memory safety | the guard's pre-flight, sampling, pause and kill (unchanged) | sampled, not a hard boundary (host venue) |
| No duplicate execution | an atomic runner claim, request ids, no auto-retry, and a dispatch gate after daemon restarts | no exactly-once promise after a crash; reconcile first |
| Remote admission | `--host` runs the remote host's own ACS over the configured route; nothing runs locally if the route fails | an `0.1.x` remote: ssh exit 255 can also follow a remote acceptance, so check `--host X status` by request id before resubmitting |
| Placement | client-side: the destination's own admission decides; the submitting host only chooses among hosts that answer, records the choice first and sends once | no fairness beyond FIFO per host; no migration of queued work; a placed job's host is fixed at submit |
| No duplicate across hosts | a placement record per remote request id, written before the call; a request id bound on one host is never sent to another; a lost reply resolved only on the same host by request id | a destination that cannot prove absence keeps the record `reply-lost` (fail-closed: the agent waits or asks the operator) |

## Mixed-mode limitation

While a direct caller of an external guard (a script that predates ACS)
shares the guard home with ACS, exclusion holds but full crash-safe mixed
operation does **not**: after an ambiguous ACS end, such a caller can still
be admitted by its guard once the flock is free. ACS blocks only ACS
admissions. The default guard home is private to the installation, so this
applies only to a host that configured a shared guard home for
compatibility.
