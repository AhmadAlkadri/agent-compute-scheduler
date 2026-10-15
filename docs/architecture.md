# ADR-001: agent-compute architecture

Status: accepted, 2026-09-26; later amended by ADR-002 and ADR-003 below.
Scope: the bootstrap. This was a thin compatibility adapter plus a small
queue, not a hardened scheduler.

A note on references: `PR #n`, ids of the forms `A<n>`, `C<n>`, `F<n>` and
names such as "campaign G" or "slice G3b" in this document refer to the
private development record in which the code was developed and used before
this repository was published. They are not links to this repository; see
`docs/provenance.md`.

## Problem

Independent (sometimes unattended) agent sessions for different scientific
repositories share one Mac and one Linux/WSL2 sidecar. **Agents may work
concurrently, but resource-intensive jobs must share a common admission
mechanism on the machine actually doing the computation.** Coordination is
agent-only. Scientific commands, Makefiles, entrypoints, imports and tests
stay unchanged, and collaborators need none of this.

## Decision

```text
repository agent instructions + installed global skill   (explain the protocol)
                         |
                  agent-compute CLI                       (deterministic coordination)
                         |
      one dedicated pueued per host, one group, parallelism 1   (durable order)
                         |
      runner: ACS admission lock, gate, block, input/backend revalidation,
              workload identity, cleanup verification
                         |
      resource guard                        (lock, memory pre-flight, threads,
                                             monitoring, wall-time, records)
                         |
                  unchanged workload command
```

1. **Reuse the existing guard unchanged.** The resource guard began as an
   external script (`guard.py`) of the first project that used ACS. ACS
   invoked it in place through machine-local config, pinned its content hash
   and refused to follow an edited guard. It never imported it and never took
   its lock (`~/.smc-guard/lock`) itself. (Campaign S2 added an internal port,
   `src/agent_compute/guard/`; since S4 it is the selectable backend
   `acs-guard`, and ACS owns the guard: see ADR-002 below, which supersedes
   this item for hosts switched to it.)
2. **Pueue is the queue backend** (4.0.4 on both hosts). ACS runs its *own*
   daemon (config, state, unix socket under `~/.local/state/agent-compute/`),
   with one group `acs-heavy`, parallelism 1. Project names are metadata. No
   Task Spooler, no custom daemon.
3. **Queue order is not safety.** Safety comes from the guard (admission,
   limits) plus the runner (identity, cleanup evidence, durable block).
4. **Lock availability is not recovery evidence.** A released flock, a dead
   pid or a stale heartbeat establishes lost supervision only. Recovery needs
   positive evidence that the identified workload is gone (`reconcile`).
   The guard record alone is never that evidence: the runner and
   `reconcile` share one evidence function per venue.
   - Host venue: the recorded leader (pid + exact start) and its process
     group/session. The start discriminator never depends on the
     checker's time zone or locale: Linux records `ticks:N` (start ticks
     since boot, plus `boot_id`); macOS records `start_utc_s`, the kernel
     start second read with `ps` under `TZ=UTC0 LC_ALL=C`. Records written
     by `0.1.0-eb369eb937` and earlier (a local-time `lstart:` string plus
     a `mktime` epoch) are still read: the leader is alive if its UTC start
     equals the recorded epoch or its system-zone rendering equals the
     string. A pid counts as **recycled** (→ clean, never signalled) only
     when the process now at it provably started after the recorded
     leader, beyond a 3 s tolerance, against an exact recorded start; for
     a legacy local string that means later than the string read as UTC
     plus 12 h (the largest lag of a real zone; contrived POSIX `TZ`
     strings are excluded). Group members are not filtered by start
     time. Any other mismatch, an
     epoch-only identity that does not match, or a failed `ps` is
     `unknown`: unverified, so the host stays blocked. The runner retries
     the exact start on every poll until it matches the guard's pid and
     `started_utc`.
   - Container venue (`procs.container_job_evidence`): the container is
     located by its recorded **id**. If it no longer exists or is stopped,
     nothing of the job runs. Otherwise one in-container `/proc` scan
     reads every live process. The job's set is every process whose
     environ carries the guard's `SMC_GUARD_JOB=<job_id>` marker, plus,
     to a fixpoint, every process sharing a member's session or process
     group or whose parent is a member, plus the recorded leader's
     session/group. It is clean only if that set
     is empty. Because the guard execs by container name, a different
     running container now bound to that name is marker-scanned as well.
     A process whose environ cannot be read and that may have
     started after the job, an unmarked process that may have started
     after the job in a session or group whose leader is gone, or a
     leader pid whose owner cannot be proven, makes the evidence
     unverifiable. All of it is trusted only from the docker daemon
     recorded at submission. What still escapes: a descendant that both
     drops the marker and calls `setsid()`, so that it leads a live,
     unmarked session of its own. The marker is needed because under
     `docker exec` the guard's `setsid -w` forks and waits only for the
     new session leader, and the in-container pid is often never
     captured for fast jobs.
   - Limitation of the guard's container kill (G1/C3, register A27):
     since G1 the internal guard applies the D4 lineage rule also when it
     captured the in-container pid. If a member of the recorded group has
     a live descendant that is unmarked and outside the recorded group and
     session (it dropped the marker and called `setsid()`), the guard's
     memory-ceiling, wall-time or cancel kill no longer `killpg`s the
     group: it holds back that descendant and its whole ancestor chain,
     the captured leader included, and kills only the other members by
     pid. The job runs on, ends `cleanup-unverified`, and the host is
     blocked. This is fail-closed for admission (no false clean, the
     in-flight marker stays) but not for host memory: the held-back
     processes keep their memory until they end or a human acts.
     `reconcile` resolves the block only on evidence, once they are gone.
5. **One heavy slot per execution host.** No packing, preemption, placement,
   distributed locks, dashboards or workflow language. Remote hosts are
   reached only by explicit `--host`, through the existing ssh route to that
   host's own agent-compute.
   (Amended by ADR-003, below. Since campaign G slice G3a, `hosts` reads
   every configured host's read-only `_summary`; `--host X submit` writes a
   local placement record first; `status`/`wait`/`cancel`/`reconcile
   --terminate`/`log`/`fetch` of such a job go to its recorded host, whose
   own gates apply. Since slice G3b an unpinned submit of a project
   declaring several hosts is placed by the client-side policy of ADR-003
   decision 4 and sent once (no retry, no fallback); a lost reply is
   resolved only by request id on the same host (`placements resolve`,
   `status R`, `status --resolve`, `hosts`; `_request` is read-only
   there), by time only after the send's call timeout plus 900 s and
   while the sender and its ssh client are proven gone; a placed or pinned
   submit from a checkout requires its commit at the destination (at
   submit and at dispatch); a call that arrived routed is never routed on,
   register A40.)

## Researched principles and what was borrowed

| Source | Borrowed | Not borrowed |
|---|---|---|
| Slurm: allocation vs execution vs pending-work management; job states PENDING/RUNNING/COMPLETING/FAILED/CANCELLED/TIMEOUT — <https://slurm.schedmd.com/overview.html>, <https://slurm.schedmd.com/job_state_codes.html> | explicit resource requests, one admission authority per pool, ownership, observable phases (`queued`, `admission-wait`, `executing`, `cleanup`) separate from terminal outcomes, cleanup before reuse | Slurm itself, Kubernetes, Redis, any control plane |
| Pueue: persisted queue and logs, task ids, JSON status, groups with their own parallelism, macOS + Linux — <https://github.com/Nukesor/pueue>, <https://github.com/Nukesor/pueue/wiki/Groups> | durable submission, JSON status, one one-slot group | one group per project (that would multiply the slot) |
| Task Spooler — <https://viric.name/soft/ts/> | considered as the smaller alternative | not implemented (one backend only) |
| `flock(2)` is advisory and tied to open file descriptions — <https://man7.org/linux/man-pages/man2/flock.2.html> | lock ≠ workload lifetime; ACS never infers safety from a free lock | — |
| cgroup v2 and systemd kill semantics — <https://docs.kernel.org/admin-guide/cgroup-v2.html>, <https://www.freedesktop.org/software/systemd/man/latest/systemd.kill.html>, <https://www.freedesktop.org/software/systemd/man/latest/systemd-run.html> | the distinction between sampled monitoring and a hard boundary; checked on a Linux host (cgroup2, systemd 255, user@ delegation of cpu/memory/pids) | cgroup containment is **not** implemented in this bootstrap (see limits) |
| `docker exec` starts a process in an existing container; limits apply per container — <https://docs.docker.com/reference/cli/docker/container/exec/>, <https://docs.docker.com/engine/containers/resource_constraints/> | killing the host-side client does not stop the in-container process: verified and turned into a regression test | changing limits of shared busy containers in tests |
| launchd / systemd user services — <https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/CreatingLaunchdJobs.html>, <https://www.freedesktop.org/software/systemd/man/latest/user@.service.html> | considered | no login persistence was added: the daemon starts on demand, and pending work never starts by itself after a restart |
| GitHub CLI `repo create --source --remote --private` — <https://cli.github.com/manual/gh_repo_create> | used to create the private origin | — |
| Claude Code personal skills in `~/.claude/skills/`, Codex user skills in `~/.agents/skills/` (symlinked folders supported) — <https://docs.claude.com/en/docs/claude-code/skills>, <https://developers.openai.com/codex/skills> | one canonical skill source, installed as a versioned copy and symlinked into both | editing the repo never changes the active skill |

The URLs are the ordinary references behind the design. The API facts ACS
depends on were verified locally against the installed versions, not taken
from those pages.

## Pueue facts verified on 4.0.4 (they shaped the design)

- `pueue add` stores the **client's entire environment** with the task, and
  in this session that included a messaging token. So tasks are added with a
  scrubbed environment (`PATH HOME LANG USER LOGNAME`), and the task command
  is a fixed quoted runner call carrying only the job id. The workload argv
  and the controlled job environment live in the ACS manifest.
- `pueue kill` SIGKILLs the task without a TERM, and a new-session
  descendant survives it. ACS never uses it: cancellation signals the runner,
  which stops the workload through the guard's own kill path.
- `pueue shutdown` SIGKILLs running tasks. `agent-compute daemon stop`
  pauses dispatch first and refuses while anything runs.
- When pueued is SIGKILLed, its running task continues. On restart the task
  is marked `Killed`, its group is paused and queued tasks stay queued.
  Tasks are never replayed.
- `pueue remove` refuses running tasks but succeeds on finished ones. So
  cancel removes only a task it saw `Queued`, then compare-and-sets the ACS
  state.
- `pueue restart` of a finished task re-runs its command. The runner's
  atomic claim refuses a second dispatch.
- The unix socket path must fit in `sun_path`, so the tests use short `/tmp`
  paths.

## Consequences

- Guarded foreground (`run`) and durable queueing (`submit`/`wait`) are both
  available. `run` takes its turn in the same queue.
- An ambiguous end (guard died, runner died, cleanup unverifiable)
  **durably blocks ACS admission** for that host
  (`~/.local/state/agent-compute/blocks/<host>.json`) and pauses the group.
  `reconcile` clears it only on evidence, and writes a durable history
  entry with that evidence on the job's state. A missing job directory,
  manifest or state is not evidence. `reconcile --terminate JOB` signals
  only proven identities:
  - host: `killpg` of a process group whose leader matches the recorded
    **exact** start (an epoch-only match is never signalled). If the
    leader pid is free, never `killpg`: only members whose exact start
    lies between the recorded leader start and the runner's last proof
    that the leader was alive (`workload_seen_utc`, minus 1 s; Linux:
    uptime ticks) are signalled, one by one and re-proven just before.
    Such a member was in the group while it was provably ours and has
    kept its id occupied since, so the id was never reused. A block time
    proves nothing (the block can be written after a reuse); with no proof
    nothing is signalled (`docs/execution-paths.md`);
  - container: processes from a fresh scan, re-proven (marker + start
    ticks) by one in-container python3 immediately before `os.kill`;
    a group is signalled only through its proven leader. Unmarked
    processes are never signalled directly (`killpg` through a proven
    marked leader reaches its whole group, which is the job's), nor a
    marker holder with an unmarked
    descendant (that would erase the only link to it); they are left for
    a human.
- Legacy direct guard callers keep working and keep mutual exclusion through
  the shared flock. They do not honor ACS blocks and are outside queue order.
- The guard remains the single source of resource policy. ADR-002 moves it
  into this repository.

# ADR-002: ACS owns the guard (backend `acs-guard`)

Status: accepted with campaign slice S4, 2026-09-30. Supersedes ADR-001
decision 1 on a host whose machine config says `[backend].kind =
"acs-guard"`. Hosts were switched one by one by their operator; `smc-guard`
stayed selectable for rollback until 0.2.0, which retired it (ADR-003 decision 2, campaign G
slice G2b): since then the internal guard is the only backend, and a guard
defect is answered by rolling forward.

```text
public caller (agent-compute CLI)
  └─► ACS admission (request id, blocks, dispatch gate, <state>/admission.lock,
      Pueue group parallelism 1, backend/input revalidation)
        └─► runner ─► python3 <versions/V>/src/agent_compute/guard/engine.py run
                       --acs-job ID --acs-state DIR --profile-file <job>/guard-profile.json …
              ├─ flock <guard home>/lock (same flags as guard.py; ~/.smc-guard on a host configured for compatibility)
              └─► workload
```

Never ACS → shim → ACS: a submitted command that runs `guard.py`,
`agent-compute`, `python -m agent_compute` or the engine itself is refused
at submit, and a guard nested in a guarded job only passes through.

## Decisions

1. **The guard is ACS code.** The runner executes the engine of **its own
   version directory** (the version the job was submitted with; the queue
   task calls that version's `bin/agent-compute`) as a separate process;
   admission never imports it. The engine is standard-library only.
2. **Pin by content.** `backend.sha256` pins `engine.py`. `submit` refuses
   and the runner refuses at dispatch ("never started") when the engine's
   sha differs from the pin or from the manifest, when the engine path is
   not the one recorded at submission, or when the backend kind changed
   since submission. A release that changes `engine.py` therefore needs
   evidence and an operator `pin-backend` per host, exactly like a guard.py
   change before.
3. **Machine and project parameters come from the config.** The profile
   is `[backend.profile]` (guard.py's `PROFILES` numbers; no defaults in
   `config.py`); the runner writes it to `<job>/guard-profile.json`
   (`agent-compute-guard-profile/1`) and passes `--profile-file`.
   `dedicated_budget_gb` keeps its meaning and precedence (`--budget-gb`,
   over the profile budget), for both backends. Per project,
   `reserve_detector` (required: `"smc-ladder"` or `"none"`) and
   `container_limits` (required for a project with containers: the ones
   the guard may `docker update`). The runner also passes `--project-root`
   (the worktree), `--cwd`, `--outputs`, `--runlog`, `--baseline` and
   `--container-workdir` from the manifest. (Since 0.2.0, ADR-003 decision
   3: `reserve_detector` is optional, default `"none"`, and the budget cap
   is the host's RAM.)
4. **The lock (invariant rewritten).** Only a guard process opens and
   flocks `~/.smc-guard/lock` (`open(…, "a+")`, `flock(LOCK_EX|LOCK_NB)`,
   same path as guard.py, so the legacy guard.py and the internal guard
   exclude each other until S5 retires the direct callers). The admission
   layer (cli, runner, queue, reconcile, doctor, status) never opens it for
   locking: it keeps its own `<state>/admission.lock` and observes the
   lock's users read-only. The lock is taken once per job. Nobody renames,
   relocates or unlinks it. This replaces "ACS never creates, locks, moves
   or unlinks the lock", which protected the lock from a second, divergent
   implementation; now ACS owns the only maintained one. *(Amended by
   ADR-003 decision 1, campaign G slice G2a: the lock is the configured
   guard home's `lock`, `<state>/guard/lock` by default; a host may keep
   `~/.smc-guard` by configuration, so direct callers of an external
   `guard.py` still exclude ACS jobs on the same lock. Since
   0.2.0 `guard.py` is no longer an ACS backend (ADR-003 decision 2): it
   remains only such a direct caller, outside queue order.)*
5. **No direct route to the internal guard.** Its `run`/`probe` refuse
   (exit 7) unless `--acs-job/--acs-state` name a job whose state records
   the guard's parent as its claimed runner and has no outcome. This is
   cooperative (same-user files can be forged), not a security boundary.
   Sandbox tests bypass it only with `ACS_GUARD_TEST_UNCLAIMED=1` and a
   `SMC_GUARD_HOME` other than `~/.smc-guard`. The `SMC_GUARD_JOB`
   pass-through is unchanged and checked first. *(Amended by campaign G
   slice G2a, ADR-003 decision 1: the guard home is configured
   (`ACS_GUARD_HOME`), the bypass needs a sandbox home, and the marker
   pass-through (`ACS_JOB_MARKER` or `SMC_GUARD_JOB`) requires the
   enclosing claimed, unfinished ACS job whose guard's in-flight marker
   carries that value; a marker alone is refused, exit 7.)*
6. **Ambiguous ends.** The guard's `cleanup-unverified` outcome, or a guard
   record whose `cleanup` is not clean, is an ambiguous end: `unknown` and
   a durable host block even when ACS's own check finds nothing (the guard
   sees more: its kill path, Linux session members).
7. **The in-flight marker.** The guard's `inflight.json` (S3) is shown
   read-only by `status` (`admission.inflight_marker`) and `doctor`.
   `reconcile` removes it (only that file) when it maps to an ACS job of
   this host (`acs_job`, written by the guard since S4, or a unique guard
   job id) that is not blocked, has an outcome, whose runner and guard are
   gone and whose processes ACS's own per-venue evidence proves gone now,
   while ACS's admission lock is free; it writes a history entry on that
   job. Otherwise the marker stays and holds the host's heavy slot, with
   the reason. While it stays, the runner backs off between guard attempts
   (as for a resource refusal) instead of polling.

## Consequences

- Edits to an external `guard.py` no longer affect a switched host's ACS jobs.
  Direct guard.py callers keep excluding ACS jobs through the lock, but do
  not honor ACS blocks or the marker (S5).
- A code rollback below S4 is refused by the floor (level 3). Until 0.2.0
  a backend rollback was a config change (`pin-backend --backend
  smc-guard`), and the config kept `[backend.profile]` and the project
  keys, which the S4 code accepted under `smc-guard` (guard.py got
  `--profile <name>`). A pre-S4 version cannot load such a config at all:
  fail-closed. 0.2.0 retired that rollback with the backend (ADR-003).



# ADR-003: a general-purpose ACS with client-side placement (accepted)

Status: proposed 2026-10-05; **accepted 2026-10-08**, once slices G2a, G2b,
G3a and G3b had merged with their gates and independent reviews (PRs #32,
#33, #36, #37; "What was built" below). It amends ADR-001 decision 5 ("no placement") and ADR-002 decisions 3 and 4
(configuration and the lock path) as stated below; everything else in
ADR-001 and ADR-002 stands.

## Problem

ACS still depends on the history, naming, configuration and private
dependencies of the project it was extracted from (the lock path
`~/.smc-guard`, the `SMC_GUARD_*` names, the `smc-*` record schemas, a
private `guard.py` as a selectable backend and as a test dependency, a
48 GB cap and the operator's profile numbers in code and fixtures), and it
has no way to choose a machine: a job runs where it is submitted unless
the agent names a host. Agents on different projects should be able to
submit heavy work and let ACS choose a suitable machine, or pin one, with
each machine keeping its own admission.

## Decisions

1. **Names are neutral by default; compatibility is configuration.** The
   guard home defaults to `<state>/guard`; the job marker is
   `ACS_JOB_MARKER`; record schemas are `agent-compute-*`. A host may
   configure `guard_home` and `marker_aliases` to stay compatible with
   direct callers of an external guard on the same lock. Every job
   manifest records the names it was launched with; evidence functions
   use the manifest's names. The lock-identity invariant of ADR-002
   decision 4 now reads "the configured guard home's `lock`"; nobody
   renames, relocates or unlinks a lock file, and a change of `guard_home`
   is an operator config change at an idle checkpoint, refused while any
   unfinished job or in-flight marker references the old home.
2. **The internal guard is the only backend.** `kind = "acs-guard"` stays
   as the one value so that a future backend is a schema addition. The
   external `guard.py` backend is retired; the historical exclusion tests
   against it live in the legacy test profile as evidence.
3. **Resource policy is explicit per host.** Profile numbers, the floor,
   the container CPU count and the dedicated budget are configuration; a
   fresh install derives defaults from the host and prints them; the cap
   is the host's RAM, not a constant. The admission rule (measured
   available memory plus the macOS Docker VM credit against floor plus
   reserve) and the declared budget's role (a ceiling, never an admission
   input) are documented in `docs/memory-model.md`. Records default to the
   state directory; writing into a repository tree is a per-project
   opt-in, and admission retries write no runlog lines.
4. **Placement is a client-side policy over per-host admission.** Each
   host keeps its own ACS, lock, queue and blocks (ADR-001 decisions 2–5,
   unchanged). The submitting host's config lists routes
   (`[hosts.*]`), per-project eligible hosts (`projects.<p>.hosts`) and
   per-host mappings (`projects.<p>.on.<host>`). `agent-compute hosts`
   reads every configured host's strictly read-only summary (identity,
   version, floor, block incl. `unreadable`, gate, queue depth, running
   job, budget, project checkouts). An unpinned submit for a project with
   more than one eligible host is placed: candidates are reachable,
   identity-consistent, not blocked, gate open, project configured, budget
   adequate; the choice is the fewest non-terminal jobs, tie → local, then
   config order; every other host's reason is reported. `--host X` pins;
   the destination's admission checks apply unchanged. There is no
   central scheduler, no shadow queue, no retry on another host.
5. **Durable identity across a lost reply.** Placed and remote submits
   always carry a request id (generated if absent) and write a local
   placement record before any remote call. A lost reply is resolved only
   by querying the same destination by request id; a job proven never
   reserved there is never-started and may be submitted again by the
   agent; anything else stays bound. An uncertain execution is never
   replayed elsewhere. Local `status`, `wait`, `cancel` and `reconcile`
   route by placement record to the recorded destination.
6. **Code consistency is a submit-time contract, not an assumption.**
   Different checkouts are not equivalent: when the local cwd is a
   worktree of the project, its HEAD is required at the destination
   (refused at submit and at dispatch on mismatch) unless the agent passes
   `--any-commit`; the destination's HEAD is recorded.
7. **One machine is one host.** Each installation has a persistent
   `host_id`; the `hosts` listing refuses two routes with one identity and
   reports a route whose reported `host` differs from its key; renaming a
   host's label is refused while jobs or blocks exist under the old name.
8. **Versions are tagged and recorded.** The package version is `0.2.0`
   at the baseline; every deployed commit is tagged; `--version` prints the
   installed version name and source sha; every job, placement and
   diagnostic record carries them; a per-host append-only event log
   (`<state>/events/<host>.jsonl`, derived, never read for a decision)
   gives the cross-machine order with host id, boot id and monotonic
   seconds beside UTC.

## Consequences

- Fairness stays FIFO per host (campaign decision D9); placement across
  machines is the relief for contention.
- The gate matrix becomes the standard profile on each affected host plus
  the legacy profile on the Mac as evidence (campaign decision D7).
- A `v0.2.0` CLI refuses a v1 config until `config migrate` has run; a
  pre-`v0.2.0` runner under a v2 config fails closed before its claim.
- An existing installation keeps its effective values through the
  migration (lock path, marker alias, records, profiles, detectors): no
  policy changes silently with the release.
- One admission authority per host (campaign decision D13): `doctor`
  checks it read-only on every run (`src/agent_compute/authority.py`;
  `doctor --json` key `single_authority`): only the configured guard home
  may have lock users or an in-flight marker, only the configured state
  dir unfinished jobs or live runners, only the configured Pueue config a
  daemon serving the ACS group or an agent-compute queue; anything else at
  a known location is an ERROR, never an admission input. So the
  production doctor truthfully reports an ERROR while a test suite or a
  throwaway demonstration runs its own sandbox queue on the same host. Limits: a
  `pueued` started without `-c` is a user's own Pueue, never an error (ACS
  always passes `-c`); `ACS_TEST_SCAN_HOME` is honoured only with
  `test_mode` on, so a demonstration config sees the production
  installation (the expected ERROR) only with `test_mode` off.

## What was built (G2a–G3b)

| Decision | Built in | What exists |
|---|---|---|
| 1. Neutral names, compatibility by configuration | G2a (PR #32, level 5) | config schema `agent-compute-config/2` and `config migrate --authorized-by` (keeps every effective value; the backup and the comparison are recorded); `[backend].guard_home` (default `<state>/guard`), `marker_aliases`, `legacy_workload_env`; the job marker `ACS_JOB_MARKER`; record schemas `agent-compute-guard-record/1` and `agent-compute-baseline/1` (the `smc-*` ones still read); every manifest records its guard home, markers and schema versions, and reconcile uses the manifest's; `doctor` reports an ERROR when the guard home or the host label changed under recorded work, and `submit` refuses a relabelled host |
| 2. The internal guard is the only backend | G2b (PR #33), G4 (PR #35) | `kind = "acs-guard"` only (a config naming `smc-guard` does not load; a v1 `smc-guard` config is not migrated); the `guard.py` comparisons and defect reproductions run only in the legacy test profile (`ACS_TEST_LEGACY_GUARD`), as evidence |
| 3. Explicit per-host resource policy | G2b | `config init` prints host-derived profile numbers and writes nothing; the budget cap is the host's RAM; records default to `{state}/projects/{project}/…` (`records_in_tree = true` is the per-project opt-in); admission retries write no runlog lines; `reserve_detector` defaults to `none`; recorded authorizations (`<state>/authorizations.jsonl`, decision D8) for `pin-backend` and `config migrate`; `docs/memory-model.md` |
| 4. Client-side placement over per-host admission | G3a (PR #36, level 6), G3b (PR #37) | `[hosts.*]` validation (no alias equal to `host`, no two entries on one route; `connect_timeout_s`, `call_timeout_s`, `reply_grace_s`); `projects.<p>.hosts` and `projects.<p>.on.<host>` (cwd, container, workdir); `_summary` (strictly read-only) and `hosts`; placement records `<state>/placements/<request-id>.json` written and fsynced before any remote call; routing of `status`/`wait`/`cancel`/`reconcile --terminate`/`log`/`fetch` by record; `--host X log`/`fetch`; `admission_memory` in the view; the remote exit code in band (A38); the policy `placement.evaluate` (fewest unfinished jobs, tie to the local host, then the project's order) with every host's status and reasons in the reply; one send, no retry, no fallback |
| 5. Durable identity across a lost reply | G3a, G3b | request ids mandatory for remote and placed submits (generated when absent); a request id bound here or recorded for one host is never sent to another (A39, level 6); `reply-lost` resolved only on the same host by request id (`_request`, read-only there; `placements resolve`, `status R`, `status --resolve`, `hosts`), by time only after the send's call timeout plus 900 s and while the placer and its ssh client are proven gone; a same-host re-send after `resolved-not-accepted`; a call that arrived routed is never routed on (`ACS_NO_REROUTE`, A40) |
| 6. Code consistency as a submit-time contract | G3b | `--require-commit`, `--require-clean`, `--any-commit`, set automatically for a placed or pinned submit from a checkout of the project, checked at submit and again at dispatch (`refused`, never started); the destination's HEAD in the record and the view (`code`) |
| 7. One machine is one host | G3a | a persistent `host_id` per installation; `hosts` reports an alias whose reply names another host, a duplicate host id and an old remote (ineligible) |
| 8. Versions tagged and recorded | G2a | `__version__` `0.2.0`; `--version` prints the installed version name and source sha; job manifests and states, placement records, `doctor`/`reconcile`/`hosts` replies carry version, host and host id; the event log `<state>/events/<host>.jsonl` (with boot id and monotonic seconds; derived, never read for a decision). |

The single-authority check (`src/agent_compute/authority.py`; the bullet
under Consequences above) was built after this table, in a later change:
when the table was written `doctor` had no single-authority line.
