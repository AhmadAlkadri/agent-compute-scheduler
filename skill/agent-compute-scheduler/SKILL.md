---
name: agent-compute-scheduler
description: Submit, place, monitor, cancel and recover resource-intensive jobs (solves, renders, refinement studies, heavy test suites, container or FEniCS workloads) through the shared `agent-compute` admission queue, which gives each configured host one heavy slot and can choose the host for you. Use before launching any heavy computation from a repository whose agent instructions (`.agents/compute-scheduler.md`) point here; also to check a job's status, wait for it, cancel your own job, recover after an interrupted session, read a refusal, or brief a subagent that will submit jobs.
---

# agent-compute: shared admission for heavy jobs

Agents on different projects work concurrently and do not know about one
another. **Resource-intensive jobs share one admission mechanism on the
machine that actually does the computation.** That mechanism is
`agent-compute` (ACS): on every configured host, one queue with a single
heavy slot, an admission check around ACS's own resource guard, and a
per-job supervisor that records what ran and proves it gone afterwards.
You submit a job and either let ACS choose a suitable host or name one;
every host keeps its own admission, lock, queue and blocks.

You never call a guard yourself: ACS runs it. This is agent-only
coordination: it does not change any scientific command, Makefile or
entrypoint, and collaborators never need it. You wrap an otherwise
unchanged command.

Names used below: a **host** is one machine with its own ACS installation
and a label in the machine config (for example `mac` and
`linux`, a Linux host reached from the Mac over ssh). A **project** is a
config entry that maps your repository's checkouts (its `roots`) to its
limits, containers and hosts. Your project's own file,
`.agents/compute-scheduler.md`, gives your project's values; `agent-compute
doctor` shows what is configured on a host.

## Before your first submit

1. Read this skill in full, then your project's `.agents/compute-scheduler.md`.
   An orchestrator that delegates heavy work passes the "Brief for a
   subagent" block below **and** makes the subagent load this skill (by
   mechanism, not by asking it to): see that section.
2. Check the installation and the hosts:

   ```bash
   agent-compute --version        # installed version and source commit
   agent-compute doctor           # this host: readiness, budget, projects
   agent-compute hosts --json     # every configured host, read-only
   ```

3. Commit your inputs (and sync any container copy of the tree) before you
   submit. See "The changed-input rule".

Project-specific values (your checkouts on each host, the host list your
project declares, its container names, ceilings and input exclusions) are
in your project file by design, not in this skill.

### What `doctor` tells you

`agent-compute doctor` (`--json` for machine output) is read-only. Besides
the backend, the safety floor, the queue and the block, it shows the
**machine profile** (`floor_gb`, `budget_gb`, the container factor), the
**host memory budget**, and per project the roots, ceilings, containers and
record paths. A line marked as an error that names the safety floor or the
backend pin means: report it and stop (see "Refusals and defects").

One admission authority per host.
`agent-compute doctor` also checks, read-only, that this host has exactly one admission authority.
The checks are `single authority: guard home`, `single authority: state dir` and `single authority: queue daemon` (`doctor --json`: `single_authority`).
A FAIL means one of four things: another guard home at a known location has lock users or an in-flight marker; another agent-compute state dir has unfinished jobs or a live runner; another `pueued` serves the ACS group or another agent-compute queue; or a location that cannot be read, which therefore cannot be ruled out.
The detail names the paths, jobs and pids.
The check never blocks your job by itself.
It is also not an outage: while the ACS test suite or a throwaway demonstration install runs on the same host, doctor truthfully reports its sandbox queue this way.
Otherwise a FAIL is a defect report, not something for you to fix.
Do not stop, kill or signal the other daemon.
Do not clean up its lock, marker or state dir, and do not point ACS at the other config.
Report the doctor output (an executor to its orchestrator, an orchestrator to the ACS maintainer session) and keep submitting through the configured installation only.

## When it applies

- You are an agent on a host with ACS installed (`agent-compute doctor`
  answers), and
- the job is heavy: a solve, render, time loop, refinement or convergence
  study, FEniCS/`dolfin` work, a container job, anything multi-threaded,
  over ~1 GB, or longer than ~10 minutes, and
- your repository's instructions point to this skill.

Reading, editing, reasoning, git, and small single-thread probes that your
project classifies as light do **not** need a slot. ACS has no light lane:
light work follows your project's own rule (for example a project guard's
`--light` mode), never `agent-compute`. Never hold a slot while thinking.
Never classify a production solve or render as light.

## Where a job runs: hosts and placement

```bash
agent-compute hosts --json
```

lists every configured host (this one and each `[hosts]` route, asked in
parallel, read-only): `state` (`ok`, `unreachable`, `timeout`, `error`,
`unparseable`, `old-remote`: a version without `hosts`; `not-probed` for a
call that arrived from another host), `eligible` and the `reasons` it is
not (blocked, gate closed, below the safety floor, an alias or a renamed
remote, a duplicate host id), the host's `summary` (version, unfinished
jobs by phase, the running job, budget, projects and their checkout HEAD)
and the open placement records. Each unreachable host can take up to the
probe timeout (30 s by default).

Where a `submit`/`run` goes (your project file says which hosts your
project declares; so does `hosts --json`, under this host's
`summary.projects.<p>.hosts`):

| You pass | The project declares (`projects.<p>.hosts`) | The job runs |
|---|---|---|
| no `--host` | one host, or none (default: this host) | here, as always |
| no `--host` | several hosts | **placed**: ACS chooses one (below) |
| `--host X` | X among them (default: this host only), checked when your `--cwd` is a checkout of the project here or you pass `--project` | **pinned** to X; X's own admission applies unchanged |

**Placement.** ACS asks every host of the project (read-only). A candidate
answers as itself, is at the safety floor with its engine pinned, is not
blocked (an unreadable block excludes it), has its dispatch gate open and
no lost supervision suspected, has the project configured, your
`--reserve-gb` within its budget (when both are known), a known cwd there
(`projects.<p>.on.<host>`), and the same commit as your checkout (below).
ACS chooses the candidate with the **fewest unfinished jobs**; a tie goes
to this host, then to the project's host order. The JSON reply's
`placement.candidates` lists every host with `status` (`chosen`, `busy`,
`unreachable`, `blocked`, `unsuitable`), its `reasons` and the host's
reply under `raw`. With no candidate the submit is refused (exit 2,
`"accepted": false`, nothing written anywhere). The job is sent **once**: a
refusal or a lost reply on the chosen host is never followed by a send to
another host, by ACS or by you under the same request id.

**Paths on another host.** For a remote host the cwd (and `--container`/
`--container-workdir`) are mapped from `projects.<p>.on.<host>` when you
submit from a checkout of the project here; a `--host X submit` from such a
checkout is mapped the same way. Otherwise pass an absolute `--cwd` that
exists on X (for example `agent-compute --host X submit --cwd
<absolute path on X> …`); nothing is mapped or
checked here then, and X's own config resolves the project.

**Code consistency (automatic).** When your cwd is a git checkout of the
project, ACS itself adds `--require-commit <your HEAD>` to a placed or
pinned submit (plus `--require-clean` when your checkout has no uncommitted
changes to tracked files); you do not pass them. `--any-commit` opts out,
when the commit does not matter. The destination refuses at submit (exit 2,
both shas named) and again at dispatch (outcome `refused`, never started)
when its checkout differs. The job view's `code` shows the requirement and
the destination's HEAD.

**Getting the commit there is your project's step.** ACS verifies the
commit at the destination; it never pushes, pulls, syncs or copies a
checkout or a container tree. Bring the destination's checkout (and any
container copy) to your commit with your project's own procedure (your
project file's sync section, the `{{SYNC}}` slot of the ACS repository's
`docs/consumer-template.md`; for example a push and a pull, or the project's sync script),
then submit. A `--require-commit` refusal is the signal that this step is
missing; never answer it with `--any-commit` when the commit matters.

**FIFO is the contract.** Each host runs its queue in submission order,
one heavy job at a time. There is no priority, reservation or
time-slicing, and a long job holds the slot for up to its
`--timeout-min`. The reliefs are placement (a project that declares
several hosts goes to the least busy one) and coordination between
orchestrators by message (the hold protocol: one orchestrator asks the
other for a hold on a host, the other answers "hold ok <host>", then
"starting …" and "… done" frame the window; `docs/operating-model.md` in
the ACS repository). Project files never carry cross-project priority
rules ("submit only when project Y has nothing queued"); if you need a
window, ask for one.

**Reaching another orchestrator.** Use peer-session messaging: list the
active sessions (Claude Code: `ListAgents`) and message the other
project's orchestrator by its session name (`SendMessage`; names change
per session, so match the project prefix, for example `project-a-`,
`project-b-`). If none is active, or for
anything about ACS itself, message the ACS maintainer session
(`agent-compute-scheduler-…`), and the operator when none runs. A job's
`project` and `submitter` fields in `status --json` tell you whose it is.
An executor never does this itself: it reports to its orchestrator.

**A hold request** from the ACS maintainer (a deploy or a gate window) or
another orchestrator: stop submitting to that host, answer "hold ok
<host>" (say if a job of yours is still queued or running, and whether it
may finish first), and resume only on "… done".

## Submitting

```bash
agent-compute [--host <X>] submit --case <key> --timeout-min <M> \
    [--reserve-gb <G>] [--threads <N>] [--slice <ID>] --request-id <run>:<step> \
    [--submitter <session>] [--cwd <dir>] -- <command ...>
agent-compute submit --container <listed container> --case <key> \
    --timeout-min <M> --reserve-gb <G> --request-id <run>:<step> -- <command ...>
agent-compute submit --probe --probe-s 60 --case <key> --timeout-min 5 \
    --request-id <run>:probe -- <command ...>
```

- `--slice <ID>` is an optional free label for a step of your work; it is
  recorded in the job record and the project's runlog, and ACS does not
  interpret it.
- `--host X` (and `--config`) are global options: they go **before** the
  subcommand (`agent-compute --host linux status <job>`); after it they are
  refused.
- `submit` success means **accepted**, never started or succeeded. Keep
  the job id: it is `job_id` in the JSON (`--json`) and, without `--json`,
  the **last line** of every `submit` result (`… | tail -1`), also when the
  job named is an earlier job holding your `--request-id`.
- **Always pass `--request-id`**, stable for the step: re-running the same
  step uses the same id, and ACS then refuses the duplicate (exit 2,
  `"accepted": false`, naming the first job in `job_id`) while that first
  job may have run. Form `<run>:<step>`, for example
  `--request-id st8-prod-n24:render-3`: 1 to 128 characters of
  `A-Z a-z 0-9 . _ : -`, starting with a letter or digit (no whitespace, no
  `/`, nothing that needs quoting); anything else is refused. The id is
  free again only when its job provably never started (`refused`,
  `admission-timeout`, `held-inputs-changed`, `launch-failed`, or
  `cancelled` with positive evidence that the guard never began launching:
  no guard pid, no `samples.csv`, no guard outcome beyond a refusal; a cancel
  after the launch keeps the id, whether or not a workload identity was
  recorded: check its outputs first). A placed or remote submit without an id gets a
  generated one (`<host>-<UTC time>-<hex>`, printed under `placement`), but
  a generated id cannot protect a re-run: give your own.
- `--submitter <label>` (or `AGENT_COMPUTE_SUBMITTER`, same alphabet as a
  request id) records a free label linking the job to your session; `--owner`
  is a free-text label that `status --mine` matches.
- `--timeout-min` is mandatory and capped per project. `--case` is the
  guard's baseline key (ACS adds the project's `case_prefix`); an
  unmeasured case needs `--reserve-gb`, or a probe first.
- **`--reserve-gb`** is the memory you declare. Declare at least the
  case's measured peak, with headroom. **Never shrink it below a measured
  peak to get admitted**: the guard kills a job above its limit
  (`killed-resource`), and a long admission wait is something to report,
  not to work around (see "Host memory budget").
- **Probe** (`--probe`, optional `--probe-s S`, default 60, at most
  `--timeout-min` × 60): the guard's probe mode. It runs the workload
  through the same slot and admission as any job, stops it at S seconds and
  records the case's peak as a `probe` baseline row (a probe that finishes
  first records a measurement). A probe needs no `--reserve-gb` (the guard
  then reserves the case's last measurement, or the whole budget); declare
  one for a container probe (below). A probe stopped at its limit ends
  `succeeded` with `guard_outcome` `probe-stopped`.
- Run from the worktree the job belongs to (or pass `--cwd`). The project is
  resolved from the path; its runlog/baseline locations come from the
  machine config (by default ACS's state directory; inside the repository
  only where the project opted in). The guard writes a runlog line only for
  an admitted job, never per refused or retried attempt; a refusal's reason
  is in `status` (`wait_reason`, `admission_memory`).
- **Environment.** Your session environment is **not** forwarded; only a
  fixed allow-list, the project's `env_passthrough` names and explicit
  `--env NAME=VALUE` (host jobs). Credential-like variable *names* are
  refused. A container job (`--container`) gets only the guard's own
  variables: `--env` is **refused** there, and passthrough names do not
  reach it (`submit` says so in its notices); put variables in the command,
  `-- env NAME=VALUE <command ...>`.
- **Never put a credential (token, password, key) in the command's argv or
  in an `--env` value.** ACS screens names only: values are stored in the
  job manifest, `output.log`, the guard record and the baseline. A workload
  that needs a secret reads it from a file the operator provides.
- Options are never abbreviated: write each one in full.
- Submit your **next ready step**, not a whole speculative campaign: a long
  chain holds other projects behind it (FIFO).
- `agent-compute run … -- <command>` is `submit` plus a blocking wait with a
  bounded admission wait (`--admission-timeout`, default 900 s). It is for
  project tooling that must block; an agent uses `submit` and `wait`, since
  a `run` outlives a tool call's timeout.

### The changed-input rule (exactly as enforced)

At submit ACS fingerprints the job's inputs, and before **every** admission
attempt, from dispatch until the guard launches the workload (phase
`launched`), it fingerprints them again. Any difference ends the job
`held-inputs-changed` (exit 13; never started). The fingerprint is:

- the worktree's `HEAD` (so **any commit**, checkout or rebase in that
  worktree, wherever its files are);
- the uncommitted changes to tracked files under the project's input
  pathspecs (`inputs` in the config; your project file lists the excluded
  directories);
- every untracked, non-ignored file under those pathspecs, by content;
- every argv operand that names an existing file (a script under `/tmp`
  too), by content, and every `--input PATH`;
- for a container job: the container's id, the docker daemon's id and the
  project's container provenance file (when it has one), so a container
  sync, recreate or Docker reset counts too.

So: **commit and sync first, then submit, then change nothing in that
worktree (and that container) until the job is `launched`.** Submit a step
whose operands another still-queued job produces only after that producer
has finished. Work on other changes in another worktree. On
`held-inputs-changed`: run `agent-compute status <job> --json`, print its
`reason` (it names what changed: `git_head`, `diff_sha256`,
`untracked_sha256`, `operands`, `extra_inputs`, the container or its
provenance), report it to whoever briefed you, then resubmit against the
new inputs (the request id is free again).

## Waiting (the harness-compatible recipe)

```bash
agent-compute wait <job> --timeout 540     # one blocking call; exit 75 = still running
```

- Run it as **one foreground tool call** whose own timeout is above wait's
  (a Bash tool timeout of 600 000 ms for `--timeout 540`). `wait` returns
  within a few seconds of `--timeout` (exit 75), even when the queue daemon
  is slow.
- On **exit 75**, call the same command again; any other exit code is the
  outcome (table below). Never call `wait` without `--timeout` from a tool
  with a timeout.
- **No `sleep` chains**, no `python -c 'time.sleep(…)'`, no tight `status`
  polling loops. Between waits, do other work or wait again.
- **Or in the background (Claude Code).** Start the same command with
  the Bash tool's `run_in_background: true`; the harness notifies you when
  it exits, and you keep working meanwhile. On exit 75, re-arm it (start
  the same background command again); any other exit code is the outcome.
  The background command is that job's one waiter: never also wait in the
  foreground or start a second one.
- **One waiter per job.** Never start a second `wait` on a job while one is
  running (in your session, a background task or a subagent).
- While waiting, `agent-compute status <job> --json` tells you why: `phase`,
  `wait_reason`, `queue_position` and `job_ahead` (the job before yours: its
  project, phase, `timeout_min` and elapsed time), and `admission_memory`
  (the last memory decision). A wait you did not expect is normal under
  FIFO; one that contradicts the declared policy is a defect (see
  "Refusals and defects").

## Outcomes

Exit codes of `wait` and `run` (and the queue task's). Read `outcome` in the
JSON when it matters: it is authoritative.

| outcome | exit | started? | meaning | what you do |
|---|---|---|---|---|
| `succeeded` | 0 | yes | the workload exited 0 (also a probe stopped at its limit) | read results from your outputs and `evidence.log` |
| `failed` | 1 | yes | the workload exited nonzero; its own code is `exit_code` | debug your command; an `agent-compute` crash also exits 1, with no JSON outcome |
| `launch-failed` | 10 | never | the guard could not start the workload (or the queue task ended before any runner took it) | fix the command or cwd; resubmit |
| `refused` | 11 | never | an admission result, not a failed calculation: a refusal at dispatch (budget, backend, a lost-supervision block, or the automatic `--require-commit` of a placed or pinned submit: the destination's checkout is not at your commit), or a submit whose client died before enqueueing (finished 15 min after acceptance) | read `reason`; for a commit mismatch bring the destination's checkout to your commit (your project's sync step) and resubmit, or `--any-commit` if the commit does not matter; see "Refusals and defects" |
| `admission-timeout` | 12 | never | the admission wait (`--lock-wait`, capped by the host) ran out | resubmit later, or report a wait that contradicts the policy |
| `held-inputs-changed` | 13 | never | the inputs changed between submit and launch | print the reason, report it, resubmit (above) |
| `cancelled` | 14 | maybe | an ACS `cancel` | check outputs if it may have started |
| `timed-out` | 15 | yes | the guard's wall-time ceiling (`--timeout-min`) | raise `--timeout-min` within the cap, or split the work |
| `killed-resource` | 16 | yes | the guard killed it above its memory limit | raise `--reserve-gb` to the measured peak plus headroom |
| `interrupted` | 17 | yes | the workload was interrupted (a signal not from `cancel`) | inspect `evidence.log` |
| `unknown` | 18 | maybe | cleanup or supervision could not be established; **the host is blocked** | see "Recovery" (`reconcile`) |

Other exit codes:

| exit | meaning |
|---|---|
| 2 | `"accepted": false`: not accepted (usage or config error, a submit-time refusal, a `--request-id` still bound to an earlier job named in `job_id`, or `run`/`wait` refused inside a job). Nothing was started by this call. **Or** `"accepted": "unknown"` (`submit`/`run`): the enqueue reply was lost after a runner claimed the job; the job **may be running**, its `job_id` is in the reply and its request id stays bound: `status`/`wait` it, never resubmit |
| 75 | `wait --timeout` expired (or a remote call's timeout); the job continues |
| 130 | `run` interrupted (Ctrl-C); the job continues (`cancel` it if you meant to stop it) |
| 255 | `--host` transport (ssh) failure: the remote may have accepted it (a lost reply, below) |

Compare `accepted` with `true` and `false` exactly: the string `"unknown"`
is truthy. Over `--host`, the remote command's own exit code comes back
in band from a 0.2.0 client (a v0.1.x client over a remote
route got any nonzero code as 1: there, read the JSON instead).

A job whose queue task ended before any runner took it (for example its
`--cwd` could not be used), or that was never enqueued because the
submitting session died mid-submit, is finished by `status`/`wait`/`cancel`
as `launch-failed`/`refused` "never started" (the second 15 min after
acceptance); its request id is then free again.

An `admission-timeout` (or a `wait_reason`) saying ACS "cannot verify that
the runner of unfinished job(s) … is gone" means ACS will not admit
anything beside that job; at the deadline it blocks the host for it and
`reconcile` lists it as "cannot tell whether its runner is alive". Report
it to the operator with the job id and the recorded runner pid; do not
resubmit in a loop and do not edit ACS state.

`unknown` is about cleanup, not about the result: the workload's own
`exit_code` and `guard_outcome` are still recorded. For a container job, ACS
checks the container for any process still carrying the job's marker
(`ACS_JOB_MARKER`, or an alias the host configures; the job's manifest
records which) or sharing a session/group with one, and for any unmarked
process in a session whose leader has exited. A job that leaves a
background process running therefore ends `unknown` and blocks the host
until that process is gone. Do not leave background processes behind. A
container image needs `python3`: ACS runs its scans, signals and the
provenance hash as small `python3` helpers inside the container (from `/`).

Guard outcome → ACS outcome (when cleanup is verified clean and the guard
exited normally):

| `guard_outcome` | ACS `outcome` |
|---|---|
| `completed` | `succeeded` |
| `failed` | `failed` (nonzero `exit_code`) |
| `killed-walltime` | `timed-out` |
| `killed-budget`, `killed-paused` | `killed-resource` |
| `killed-interrupted` | `interrupted`, or `cancelled` after an ACS `cancel` |
| `launch-failed` | `launch-failed`; with no workload pid nothing started and `guard_outcome` is `null` in the view |
| `refused`, `lock-busy`, `not-started` | pre-launch: ACS decides these from the guard's exit code (`refused`, a retried lock wait, `admission-timeout`), and `guard_outcome` is `null` |
| `probe-stopped` | `succeeded` for a job submitted with `--probe`; `unknown` for any other job |
| `cleanup-unverified` | `unknown`, and the host is blocked: the guard could not prove the job's processes gone |
| `running`, missing, or anything else | `unknown` |

Whatever the guard outcome, a cleanup that is not verified clean, or a guard
killed by a signal, gives `unknown`.

## Status JSON

```bash
agent-compute status <job> --json            # one job (a job id or a request id)
agent-compute status --all --unfinished --mine <owner|request-id-prefix> --json
agent-compute status --request-id <R> --json # the job(s) this host bound or recorded under R
agent-compute log <job> --tail 50            # the job's output.log
```

`status JOB --json`, `wait JOB --json` and `cancel JOB --json` on a
finished job share **one shape**: the job view's fields at the top level,
`admission` (the host's admission state) and `acs` (the diagnostic
identity: `version`, `source_sha`, `safety_level`, `engine_sha256`, `host`,
`host_id`, `utc`) under their own keys, and the view again under `"job"`
(the shape before 0.2.0; kept). `wait` adds a `note` when it stops early;
`cancel` on a finished job adds `cancelled: false` and a `note` ("already
<outcome>; nothing to cancel"). `status --all --json` lists views under
`"jobs"` (`--unfinished`: only jobs without an outcome; `--mine X`: only
jobs whose `--owner` is X or whose `--request-id` starts with X) and the
open placement records. **Never parse `reason`** (free text). Keys are only
ever added; every key below is always present (`null` when not known yet),
except `queue_task` (only when the queue is up), `unreadable` (only when a
record cannot be read) and `route` (only in a reply routed to another
host):

| Field | Meaning |
|---|---|
| `job_id`, `project`, `host`, `case`, `request_id`, `owner`, `submitter` | identity (from the manifest) |
| `acs_version`, `acs_source_sha` | the ACS version that accepted the job (`null`/the package version for jobs before 0.2.0) |
| `phase` | lifecycle phase: `accepted`, `queued`, `dispatched`, `dispatch-held` (the dispatch gate is closed or unreadable: `admission.dispatch.reason` says why and what lifts it), `admission-wait`, `launched` (the guard has begun launching the workload; its identity is not captured yet; `wait_reason` says so), `executing` (the workload identity is recorded), `cleanup`, `done` |
| `outcome` | the ACS outcome (above); `null` while unfinished |
| `guard_outcome` | the guard's own result word; `null` if the guard never launched the workload |
| `exit_code` | the workload's exit code; `null` if it never ran |
| `peak_rss_gb` | peak resident memory measured by the guard; `null` if it never ran |
| `cleanup` | `{verified, clean, method, reason}` from the post-run cleanup check; `null` if none ran |
| `accepted_utc`, `dispatched_utc`, `admitted_utc`, `ended_utc` | timestamps (`admitted_utc`: the workload identity was recorded) |
| `queue_wait_s` | accepted → admitted, or → ended for a job that never launched; `null` until then |
| `dispatch_wait_s`, `admission_wait_s` | accepted → dispatched, dispatched → admitted; `null` until known |
| `execution_wall_s` | guard-measured wall time (s) |
| `wait_reason` | why it is waiting (resources, lock, gate), or "the guard launched the workload; identity not yet captured"; `null` once the wait is over |
| `queue_position`, `job_ahead` | for a queued job: its place among the queued jobs (1 = next) and the job ahead, `{id, project, phase, timeout_min, elapsed_s}`; `null` otherwise |
| `reason` | human-readable only; do not parse |
| `probe` | `{probe_s}` for a job submitted with `--probe`, else `null` |
| `parent_job` | for a job submitted from inside another job: `{acs_job, guard_job, smc_guard_job}` (metadata only), else `null` |
| `queue_task` | `{id, status}` of the queue task, when the queue is up |
| `workload` | the recorded workload identity (pid/start, container), once captured; `null` before |
| `evidence` | paths: `manifest`, `state`, `log` (`output.log`), `guard_records` (dir), `guard_record` (this attempt's guard record, once the guard began launching) |
| `admission_memory` | the memory decision of the last guard attempt that recorded one: `{host_available_gb, floor_gb, estimate_gb, needed_gb, vm_credit_gb, attempt, refusal, path}`; `null` before any. Read this instead of the guard's files |
| `route` | only in a reply routed by a placement record (another host's job): `{host, transport, ssh, distro, agent_compute, via, record, request_id, phase}`; `host` and `evidence` are then that host's |
| `code` | the code requirement and the checkout at submit: `{head, dirty, required_commit, require_clean, any_commit}` |
| `placement` | for a placed job: `{request_id, placing_host, policy: {rule, chosen, candidates: [{host, status, reasons, unfinished, head, …}]}, code}` (in a routed reply, from this host's placement record); `null` when the job was not placed |

`admission` (in `status --json`): `blocked` (the host's block record, its
`jobs` keyed by job id, or `null`), `dispatch_gate`, `dispatch` (`{held,
reason}`: whether queued work can start now and, if not, why and what
lifts it: a closed gate, a paused queue group, a block),
`compute_lock_holder`, `inflight_marker`, `lost_supervision`. A job that
ended `held-inputs-changed` is printed with its reason on the first line of
`wait`, human `status` and `run`, and a `submit` that reuses its
`--request-id` names it in `notices`.

The workload's own stdout and stderr are **not** on `agent-compute`'s
stdout: they are in `evidence.log` (`output.log`, interleaved with ACS and
guard lines). `agent-compute log <job> [--tail N]` prints it, also for a
job on another host (`agent-compute --host <X> log <job>`, or routed by a
placement record); `agent-compute --host <X> fetch <job> <dir>` copies that
job's manifest, state, `output.log` and guard records into `<dir>/<job>`
(never overwriting). No ssh or WSL paths by hand.

## Placement records and lost replies

Every placed or pinned (`--host X`) submit is recorded here first:
`<state>/placements/<request-id>.json`, written and fsynced before any
remote call, with phase `committed` → `sent` → `accepted` | `refused` |
`reply-lost` → (`accepted` | `resolved-not-accepted`). After that,
`status`, `wait`, `cancel`, `log` and `fetch` given its job id or request id,
and `reconcile --terminate <job>`, go to the recorded host by themselves
(the view carries `route` and `placement`). A request id recorded for one
host is never submitted on another, and one this host bound to a job that
may have run is never sent to another host.

A **lost reply** (exit 255 or 75 on a remote submit, phase `reply-lost`)
may hide an accepted job. It is resolved only by asking that same host by
request id:

```bash
agent-compute placements resolve <R> --json   # or all open records: placements resolve
agent-compute status <R> --json               # that record, resolved on its host
agent-compute status --resolve --all --json   # every open record first
```

(`hosts` resolves them too.) Found: `accepted`, with the job id: wait for
that job. Not found, the host answering, and only when (a) the send's
client call timeout **plus 15 minutes** has passed since the send (30
minutes with the default 15-minute call timeout; longer if the route's
`reply_grace_s` says so) and (b) the sending process and its ssh client are
proven gone (ACS proves this itself, from the start-time identities it
recorded in the placement record before sending; you never look up or
check pids, and need no ssh: you only run `placements resolve` or `status
<R>`, again later if it is still `reply-lost`): then
`resolved-not-accepted`, and you may submit it again, to
that host with the same id or anywhere under a new id (ACS never does). A
`run` has no client timeout: it is never resolved by time while it is still
`sent`, and after its client ended, 15 minutes count from that end.
Unreachable, or anything unprovable: it stays `reply-lost`, and the same id
is refused (nothing sent) until it is resolved. **Plain `status` never asks
another host**: it lists the open records from this host's state (phase and
`age_s`); only `status <R>`, `status --resolve`, `hosts` and `placements
resolve` query the records' hosts.

## Cancel: your own jobs only

```bash
agent-compute cancel <job> --json
```

Cancel only a job you (or the session that briefed you) submitted, by its
id. A queued job is removed before it starts; a running one is stopped
through the guard's own kill path and ends `cancelled`. A placed job is
cancelled on its recorded host. `cancel` exits 0 when it cancelled the job;
on a finished job it prints `cancelled: false` and the job's view. Never
stop a job, a process or a container in any other way.

## Refusals and defects

A refusal is information, and most refusals are your input to fix: a
missing `--reserve-gb`, a container not listed for the project, a
`--timeout-min` above the cap, a dirty or different checkout at the
destination under the automatic `--require-commit` (exit 2 at submit, or
`refused` at dispatch: sync the destination with your project's own step,
or `--any-commit`), a request id still bound. Read `reason` and the
`notices`, fix the submit, and resubmit.

**Judging whether a memory wait "fits".** The rule is **available
(+ the Docker VM credit, for a container job on macOS) ≥ `floor_gb` +
reserve**. Read the numbers from `status <job> --json`'s `admission_memory`
(`host_available_gb`, `vm_credit_gb`, `floor_gb`, `estimate_gb`: the
reserve used, `needed_gb`, `refusal`); `floor_gb` is also in `doctor`'s
machine profile. A host job's reserve must also be within the host memory
budget. The full model, with the credit's conditions, is the ACS
repository's `docs/memory-model.md`.

**A refusal or wait that contradicts the declared policy is a defect
report, not a planning input.** Examples: a job refused or waiting on
memory although `admission_memory` shows available (+ credit) ≥ floor +
reserve, or `doctor`'s budget says it fits; a
container refused that `doctor` lists for your project; a host reported
ineligible for a reason that `hosts` contradicts; a `wait_reason` or phase
that does not match what the job is doing. Then:

1. do not re-plan the work around the limit and do not work around it;
2. collect `agent-compute --version`, `agent-compute status <job> --json`
   (its `admission_memory`, `wait_reason`, `reason`) and `agent-compute
   doctor`;
3. report it, with what you expected and the declared number or rule
   behind that expectation: an executor to its orchestrator; an
   orchestrator to the ACS maintainer session (and in its project's
   dogfood log, below);
4. meanwhile "Deferred: resources occupied" is an acceptable outcome.

**A refusal saying the installation is below the safety floor**, or that
the "backend guard … differs from the verified" pin, means the installed
ACS must not run work: report it to the operator with the text and stop.

## Recovery after an interrupted session

1. `agent-compute status --all --json` (and the open placement records it
   lists; `agent-compute hosts --json` for the other hosts). Find your jobs
   by `--request-id`, `--mine`, case and time. **Do not resubmit** a job
   that is unfinished, `unknown`, `accepted: "unknown"` or behind a
   `reply-lost` record: it may have run.
2. Resolve your open placement records by request id (`placements resolve
   <R>`); wait for the jobs they name.
3. Re-submitting with the same `--request-id` is refused while the earlier
   request may have executed; that is the duplicate guard.
4. **If `admission.blocked` is set, read before acting.** Its `jobs` name the
   job(s) blocking the host (for another host: `agent-compute --host <X>
   status --all --json`, and `--host <X>` before each command below).
   **One reconcile policy:**
   - **Only your own jobs block it** (yours, or your orchestrator's; plain
     `reconcile` acts on every job in the block): read `agent-compute
     status <job> --json` (`cleanup`, `outcome`, `workload`) and then run
     `agent-compute reconcile --json`. It resolves jobs whose runner, guard
     and workload are verifiably gone (writing the evidence into the job's
     history) and lists concrete blockers otherwise. If it names surviving
     workload processes of your job and the evidence shows they are your
     job's, `agent-compute reconcile --terminate <job>`: it signals only
     that job's recorded process group, or in a container only processes
     carrying that job's marker, re-proven right before each signal. If the
     surviving process simply finishes, a plain `agent-compute reconcile`
     then resolves the block.
   - **Another project's job is in the block** (or one you cannot
     attribute): do **not** run `reconcile` or `--terminate`. Report it (job
     id, project, `status` evidence): an executor to its orchestrator, an
     orchestrator to that project's orchestrator or the ACS maintainer
     session.
   - Blockers that no agent resolves; report them to the operator: unmarked
     processes (`cleanup.orphaned`, `cleanup.unmarked_members`; `--terminate`
     signals only marker-proven processes and their groups, and skips a
     marked parent of an unmarked one), a **different docker daemon**
     answering (or its identity unreadable), "cannot tell whether its
     runner is alive". Unidentified processes are never "safe to kill".
   - `admission.inflight_marker` is the internal guard's record of a
     workload that may still run: `reconcile` removes it once the evidence
     proves that job's processes gone, or says why it stays. Never delete it.
5. After the block clears: `agent-compute resume` (also needed once after a
   queue-daemon restart that found pending work: pending jobs never start on
   their own after a restart; `admission.dispatch.reason` says so).

A stale heartbeat, a free lock or a dead pid is evidence of lost
supervision, not permission to clear a block.

## Host memory budget

A host job's `--reserve-gb` must fit the host budget or the guard refuses
it before it starts, and a host job is killed above it. The budget is the
machine profile's `budget_gb` unless the operator declared the host
**dedicated compute** (`backend.dedicated_budget_gb`, at most the host's
RAM, for every project); `agent-compute doctor` shows it as `host memory
budget` (for example 48 GB dedicated on a 64 GB Mac). The budget is a ceiling,
never an admission test: the guard admits a job when the host's measured
available memory (plus, for a container job on macOS, the Docker VM credit
below) covers the profile's floor plus your reserve, and waits otherwise
(`docs/memory-model.md` in the ACS repository states the whole model).

Container jobs are not governed by the host budget. **A container job's
memory limit** is the profile's `container_memory_factor` (1.5 by
default; `doctor` shows it under `machine profile`) × the job's
reserve, rounded up to whole GB: at least 1 GB, at most the Docker VM's
memory minus the profile's `vm_reserve_gb`. The reserve is your
`--reserve-gb`, or the case's measured peak if that is larger (a probe: its
`--reserve-gb`, else the last measurement, else the whole budget). The
guard kills the job above that limit (`killed-resource`). When the project
lists the container in `container_limits` (`doctor`, per project), the guard
also applies the limit to the container before the launch (`docker update`
with no swap and the profile's CPUs); otherwise the container keeps its own
limits and the guard's sampling alone holds the job to it. Declare a
reserve with headroom: `--reserve-gb 6` gives a 9 GB limit. On macOS
(Docker Desktop) a container job's admission also counts the memory that is
free inside the Docker VM (up to your reserve), waits while the VM itself is
too busy, and is refused outright when the reserve is larger than the VM;
that includes a `--probe` with no declared reserve, which reserves the whole
budget: declare `--reserve-gb` for a container probe.

Agents never set or change the budget declaration (see "Never").

## Forwarding from project tooling

A project's own launcher (a guard shim, a Makefile target, a step
supervisor) forwards a heavy job to ACS as `agent-compute submit` (detached;
`wait` later) or `agent-compute run` (blocking). Option mapping from a
guard-style launcher: `--case K` → `--case K`; `--timeout-min M` → same;
`--reserve-gb G` or `--estimate-gb G` → `--reserve-gb G`; `--threads`,
`--dense-threads`, `--slice`, `--units` → same; a container venue
`--container C [--container-workdir W]` → same (a container listed for the
project); a probe → `--probe [--probe-s S]`; a bounded admission wait →
`run --admission-timeout S`; `--cwd D` → same (absolute with `--host`). ACS
sets the outputs, runlog, baseline, profile and budget itself. Pass
`--request-id <run>:<step>` so a re-run of the launcher can never start a
step twice, and `--json` for machine output. With `--json`, stdout is one
JSON object: the job view once the job has an outcome, `{"accepted": false,
"reason": …}` when it was not accepted, `{"accepted": true, "job_id",
"host", "project", "evidence", "notices", "meaning"}` from `submit`, or
`{"accepted": "unknown", "job_id", "host", "evidence", "reason"}` after a
lost enqueue reply. A launcher that parses workload output reads
`evidence.log` after the outcome.

**Next steps from inside a running job.** A host workload of an ACS job
carries `ACS_JOB_ID` (its job id) and the guard's `ACS_JOB_MARKER` (plus any
alias the host configures; `doctor` lists them).
- `submit` from inside a job is allowed: the new job is independent, queued
  behind the current one (it never nests and is never admitted while the
  current job holds the slot), and its view records `parent_job`. Neither
  marker reaches the new job's environment.
- `run`, and `wait` on an unfinished job, are **refused** (exit 2) while the
  job named by `ACS_JOB_ID` is unfinished, or when only a guard marker is
  set: they would wait on the slot this job holds. Submit the next step and
  exit, or wait from outside the job. `status` stays available.
- A step chain therefore either submits its successor as its last action
  (with a stable `--request-id`), or is driven from outside ACS (a session
  or launcher that is not itself a job).

## Never

- Launch expensive work directly because coordination is unavailable,
  blocked or slow. "Deferred: resources occupied" is an acceptable outcome.
- **Kill processes by pattern** (`pkill`, `killall`, `kill $(pgrep …)`, `kill
  -9 -1`): a consumer's `pkill -f cat` once took down Docker Desktop and
  three agent sessions.
- **Stop any process you did not start; stop your own ACS jobs only through
  `agent-compute cancel <job>`.** Never stop or restart a shared container
  or Docker itself. The one exception is a Docker Desktop restart on the
  Mac (the cure for its container-start wedge): only the session that holds
  the operator's delegation for it (the operator names it), only
  with ACS idle (`agent-compute status`: nothing executing or queued in a
  container) and consumer holds in place, and only by this procedure:
  `docker desktop stop`, then `open -a Docker`, then `docker start <the
  listed containers>`, then, in a later tool call, verify that a plain
  `docker run --rm <image> true` returns within seconds. Everyone else
  reports a wedge (or a needed restart) to that session or the ACS
  maintainer session, and never restarts Docker.
- `docker exec` into a container while an ACS job may run in it (not even a
  quick check): an unmarked process there can block the host.
- Cancel or reconcile another project's job (see "Recovery"; report it).
- Set or fabricate `ACS_JOB_MARKER`, a marker alias (`SMC_GUARD_JOB` on a host
  configured for it) or `ACS_JOB_ID`; run a guard (`guard.py`, ACS's
  `agent_compute/guard/engine.py`) directly, except your project's
  documented light lane; or pass `agent-compute`, `guard.py`, the engine or
  a `make` target that calls a guard as the submitted command (`submit`
  refuses them, also behind `env` or `python [-m]`): give ACS the bare
  workload command.
- Clear, move or delete the guard home's `lock` or `inflight.json` (the
  guard home is in `doctor`; `~/.smc-guard` on a host configured for it) or any
  ACS state; create a second queue, group or per-project pool;
  force-start queued tasks (`pueue start <id>`), `pueue kill`, `pueue
  reset`, or change group parallelism, to meet a deadline or otherwise.
- Shrink `--reserve-gb` below a measured peak, raise host limits or project
  caps, set or change `backend.dedicated_budget_gb`, edit the machine
  config, or point ACS at another config (`--config`, `AGENT_COMPUTE_CONFIG`)
  to get admitted.
- Treat a paused or stopped job as freeing memory.
- Run operator-only commands: `agent-compute pin-backend`, `agent-compute
  config migrate`, `install.py uninstall --force`, two installs at once, or
  `install.py activate` onto an older version. (`agent-compute config init`
  only prints a starting config for a new host; setting up a host is the
  operator's.)
- Work around an "installation below the safety floor" refusal or a
  backend-pin refusal: report it to the operator, who must roll forward or
  pin with evidence.
- Wrap a command that detaches or escapes its process group: `nohup`,
  `setsid`, `setpgid()`/`set -m` job control, `systemd-run`, `docker run
  -d`, background `&` daemons, or a host script that itself runs `docker
  exec`. Container work goes through `--container <listed container>` so
  the guard and ACS track the in-container process.
- Write cross-project priority rules into a project file, or start a
  second waiter on a job.
- Merge, install, deploy, pin or reconfigure ACS as a consumer executor
  (see "Reporting and improving ACS").

## Brief for a subagent

An orchestrator that delegates heavy work to a subagent does two things:

1. **Delivers this skill by mechanism.** Preload it in the subagent's
   definition (Claude Code: the agent file's frontmatter list `skills:` with
   `- agent-compute-scheduler`, as a consumer project's
   `.claude/agents/executor.md` would), or make
   the brief's first instruction "call `Skill(agent-compute-scheduler)`
   before your first `agent-compute` command" and check the subagent's
   report for it. Brief wording alone is not delivery: executors that were
   only told about the skill ran dozens of jobs without reading it.
2. **Pastes the block below verbatim** into the brief, filling the
   `<placeholders>` (and nothing else).

```text
ACS rules (agent-compute-scheduler). They bind you for this whole task.

0. Before your first agent-compute command, load the skill:
   Skill(agent-compute-scheduler). Then read <project checkout>/.agents/compute-scheduler.md.
1. Heavy work (anything that file calls heavy) runs only through
   `agent-compute submit`. Never launch it directly, never bypass a refusal,
   block or queue, even for a deadline. "Deferred: resources occupied" is an
   acceptable result.
2. Every submit carries a stable request id of the form <run>:<step>, e.g.
   --request-id <run-prefix>:step-3 (letters, digits, . _ : - only; no spaces;
   it needs no quoting). A re-run of the same step uses the same id. Also pass
   --submitter <session label>. Destination: <no --host (ACS places it) | --host <X>>.
3. Commit (and sync the container copy, if any) BEFORE you submit. From submit
   until the job is `launched`, change nothing in that worktree or container:
   no edits, commits, checkouts, new untracked files, edits to files named in
   the command, or container syncs. Submit a step whose inputs another queued
   job produces only after that job has finished.
4. If a job ends held-inputs-changed: run `agent-compute status <job> --json`,
   print its `reason`, report it to me, then resubmit against the new inputs.
5. Waiting: `agent-compute wait <job> --timeout 540` as ONE foreground command
   with a tool timeout of at least 600 s (Bash timeout 600000), or the same
   command with run_in_background: true (the harness notifies you on exit).
   Exit 75 = still running: run (or re-arm) the same command again. No sleep
   loops, no python time.sleep, no second waiter on the same job.
6. Results: `agent-compute status <job> --json` (outcome, exit_code,
   guard_outcome, evidence.log); never parse `reason`. Compare `accepted` with
   true/false exactly: "unknown" means the job may be running; wait for it,
   never resubmit it.
7. Stop only your own jobs, only with `agent-compute cancel <job>`. Never kill
   by pattern (pkill, killall, kill $(pgrep ...)), never stop a process or a
   container you did not start, never `docker exec` into a container while an
   ACS job may run in it.
8. Never set ACS_JOB_MARKER, SMC_GUARD_JOB or ACS_JOB_ID; never run a guard
   (guard.py, engine.py) directly except the light lane your project file
   documents; never submit agent-compute or a guard as the command.
9. Never shrink --reserve-gb below a measured peak to get admitted; never
   raise limits, edit ACS config or state, touch the guard home's lock or
   inflight.json, or call pueue.
10. Blocked host: read `agent-compute status --all --json` first
   (admission.blocked.jobs). Only if every blocking job is yours: read
   `agent-compute status <job> --json`, then `agent-compute reconcile --json`,
   and `agent-compute reconcile --terminate <job>` only for processes the
   evidence shows are your job's. Another project's job in the block: run
   nothing, report it to me.
11. A refusal or wait that contradicts the declared policy (doctor's budget,
   the project's ceilings, containers or hosts) is a defect: report it to me
   with the job id, `agent-compute --version` and `status --json`; do not plan
   around it.
12. Never merge, install, deploy, pin or reconfigure ACS. ACS defects go to me
   in a written report.
13. End your report with every job id you submitted, its request id, host and
   outcome, and every refusal or hold with its reason.
```

## Reporting and improving ACS

- **Observations.** A consumer orchestrator keeps an append-only dogfood
  log in its repository (`.agents/acs-dogfood.md`; format in the ACS
  repository's `docs/dogfood-log-template.md`): date, ACS version (from
  `agent-compute --version` or a job's `acs_version`), job id(s), what
  happened, what was expected, evidence path, reported to, status. Routine
  successes are worth a line too.
- **Consumer executors** never merge, install, deploy, pin or reconfigure
  ACS. A defect goes to their orchestrator as a written report.
- **An orchestrator** reports defects to the ACS maintainer session, and may
  fix ACS **upstream**: the ACS development repository, where it lives and
  its change policy are named by the operator in your project's
  `.agents/compute-scheduler.md` (the ACS development repository field);
  fix ACS defects there, never in a clone or fork you pick yourself. Follow
  that repository's `AGENTS.md`: branch → regression test that fails first
  → PR → gates → review → merge. If the file names no such repository, ask
  the operator. Never vendor the fix into a scientific repository, and never
  work around ACS by launching expensive work directly.
- **Merging is not deploying.** A new version is installed only by the
  documented procedure (`install.py install` from a clean commit, then
  `doctor`) at an idle checkpoint, announced with holds, normally by the
  maintainer session (`docs/operating-model.md`). Never replace the active
  scheduler under live jobs in any other way.

## Supported paths and limits (details: the ACS repository's `docs/execution-paths.md`)

- Host venue and container venue (containers listed for the project only),
  on every host whose `doctor` reports them: guarded, queued,
  cleanup-verified. A remote host is reached only through its configured
  route, by placement or `--host`, and admits on its own lock; if the route
  fails nothing runs locally instead.
- Process tracking is sampled evidence, not cgroup containment. On the
  host it is by process group/session plus start time, and a descendant
  that calls `setsid()` escapes it; on macOS only the process group is
  tracked, so a descendant that calls `setpgid()` into a new group escapes
  too. In a container it is by the job marker plus sessions/groups, and a
  descendant that both scrubs its environment (`env -i`) and calls
  `setsid()` escapes it. Unmarked leftovers in an orphaned session block the
  host rather than pass. On macOS there is no hard containment at all.
- Direct callers of an external guard that shares the guard home (a host
  configured for compatibility with a project's own guard script on
  `~/.smc-guard/lock`)
  still exclude ACS jobs through the lock, but they are outside queue order
  and do **not** honour an ACS recovery block.
- No exactly-once promise after a crash: reconcile and resolve before
  resubmitting.
