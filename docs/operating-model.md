# Operating model: who may do what

ACS is shared infrastructure on hosts where several agent sessions work at
once. This document states the roles, what each may do, how operator
actions are authorized and recorded, and the coordination protocol
(notices, holds, gate windows) used when several sessions share a host.
`AGENTS.md` is the law for changes to this repository; the skill is the law
for using ACS; this document connects the two.

The multi-session protocol (messaging between sessions, notices and holds,
the maintainer session, the dogfood log) is **optional**: it matters only
when several agent sessions share a host. A single person running one agent
on one host can ignore it; the sections on roles and recorded
authorizations still apply.

## Roles

| Role | Who (an example setup) | Purpose |
|---|---|---|
| **Operator** | the human who owns the hosts | owns the machines, their configs, the Docker VM and the budget declarations; authorizes deploys and operator-only actions; decides open questions |
| **Maintainer session** | an ACS orchestrator session, for example `agent-compute-scheduler-<suffix>` | maintains this repository and the installations; deploys; triages the consumers' observations; one at a time |
| **ACS slice executor, ACS reviewer** | the maintainer's subagents | implement and validate one slice, meaning one bounded, separately reviewable piece of work on this repository, such as a single fix or feature (executor); review a safety-semantic diff against the invariants (reviewer, never the executor) |
| **Consumer orchestrator** | one session per consuming project, for example `project-a-<suffix>` | plans and submits its project's heavy work, briefs its executors, keeps the project's ACS files, answers holds, reports defects |
| **Consumer executor** | a consumer orchestrator's subagent | submits, waits for and cancels its own jobs under the brief |

Session names change from session to session; match on the prefix.

**Reaching another session.** Sessions talk by peer-session messaging (in
Claude Code: `ListAgents` lists the active sessions, `SendMessage` to a
session name delivers a message). A consumer orchestrator that needs
another project's orchestrator (a window, a job of theirs blocking the host)
messages it directly; if none is active, or the matter is ACS itself, it
messages the maintainer session, and the operator when none runs.
Executors do not message other sessions: they report to their orchestrator.

## What each role may do

| Action | Operator | Maintainer | Consumer orchestrator | Consumer executor |
|---|---|---|---|---|
| submit, wait, status, log, fetch, hosts | yes | yes | yes | yes (under the brief) |
| `cancel` | any job | any job, with its owner's agreement or at the operator's direction | its project's jobs | its own jobs |
| `reconcile`, `reconcile --terminate` | yes | yes, on evidence | its project's jobs only, after reading the evidence | its own jobs only, after reading the evidence |
| `resume` (after a block clears or a daemon restart) | yes | yes | after its own reconcile | after its own reconcile |
| answer a hold request | — | — | yes ("hold ok <host>") | no: its orchestrator does |
| request a hold or a Mac quiet window | yes | yes | yes, from another orchestrator | no |
| restart Docker Desktop on a Mac | yes | no: requests it | only the session the operator delegated it to, with holds in place and no ACS job running | no |
| fix ACS upstream (branch → PR → gates → review → merge, `AGENTS.md`) | yes | yes | yes | **no**: a written defect report to its orchestrator |
| `install.py install` (deploy) at an idle checkpoint, with notices | yes | yes, under an operator authorization | only when no maintainer session runs, with the same notices, checkpoint and deploy record | **no** |
| `pin-backend`, `config migrate` (with `--authorized-by`) | yes | yes, with a recorded operator authorization | no | no |
| edit the machine config, the budget declaration, the Docker VM size | yes | only as authorized and recorded | no | no |
| `install.py uninstall --force`, concurrent installs, editing ACS state or a block by hand | yes (manual procedures) | no | no | no |

Never, for anyone: launching heavy work outside ACS because ACS is
unavailable, killing processes by pattern, deleting or moving the guard
home's lock, force-starting or killing queue tasks
(`pueue start <id>`, `pueue kill`, `pueue reset`).

## Recorded authorizations

Operator actions are not a terminal ceremony; they are authorizations
recorded with the action:

- `agent-compute pin-backend --evidence <committed gate file>
  --authorized-by "<who, when, what>"` and `agent-compute config migrate
  --authorized-by "<who, when, what>"` append one JSON line to
  `<state>/authorizations.jsonl` (schema `agent-compute-authorization/1`,
  mode 0600, fsynced, **before** the config is rewritten: no record, no
  change): UTC time, action, the authorization text, the config path, the
  evidence (pin) or the backup and the effective-value comparison
  (migrate), the config diff summary, the installed version and source
  sha, the host and host id. `doctor` shows the last pin's authorization
  ("backend pin authorization").
- The text is one line (at most 500 characters): **who** authorized (the
  operator), **when** (UTC, and where: the session or message), **what**
  (the scope, and its time box if it has one). For example: `"operator,
  <UTC time> in <session>: install the new release on both hosts incl. pin
  and config migrate"`.
- The operator may delegate by message, time-boxed and scoped to a deploy
  or a body of work. The session acting on it quotes the delegation in
  `--authorized-by`, and the maintainer's deploy record names it. An
  approval covers only what it listed; a new engine change needs its own.

## Deploy notices and holds

Merging changes nothing on the hosts (`AGENTS.md`, "Operator actions":
installing a version is a separate, recorded step).
A deploy, a backend pin, a config migration, a Docker Desktop restart and
a Docker-backed gate run on a Mac each need the host quiet, so they follow
one protocol:

1. **Find the recipients.** List the active sessions and message every
   consumer orchestrator that submits to the host by its session name
   (match the prefix). A new session that has not seen the protocol gets
   it explained in the first message.
2. **Hold request.** Say what will happen, on which host, why, and for how
   long: "hold <host> for <what>, about <N> min; reply 'hold ok <host>'".
   Ask how they want their queued work handled (drain naturally, or cancel
   their own job).
3. **"hold ok <host>".** Each recipient stops submitting to that host and
   answers, naming any job of its own still queued or running and whether
   it may finish first. Do not start before every recipient has answered
   (or has been confirmed inactive).
4. **Checkpoint.** Verify the host idle: no unfinished job, no block, no
   in-flight marker, no lock user, no legacy process (`agent-compute
   status`, `install.py status`, the guard home).
5. **"starting <what> on <host>".**
6. Do the work; record it (for a deploy: UTC, from and to version, tag,
   config change, pin, authorization, doctor evidence, smoke job, notices
   and acknowledgements).
7. **"<what> done on <host>"**, naming the visible changes and anything the
   consumers must re-read (the skill, a config change). The hold ends with
   this message and only with it.

Holds between consumer orchestrators (for a project that needs a window on
a busy host) use the same three messages: request, "hold ok", "done" (FIFO
is the contract).

### Docker-backed gate windows on a Mac

The full test suite with Docker creates and removes disposable containers.
On a Mac that shares containers with agents, run it only in a window with
consumer holds and **no ACS job executing in a shared container**.

- **Pre-step.** A plain `timeout -k 5 30 docker run --rm <the image of the
  run> true` must return within seconds; otherwise do not start the suite
  (`-k 5`: a wedged `docker run` ignores the first signal).
- **The wedge.** Docker Desktop on a Mac can stick container starts in
  `Created` while `docker version` still answers, typically at the first
  container start after an idle stretch. If `docker run` hangs: stop, do
  not retry, and request a restart.
- **The Docker Desktop restart is delegated.** It stops the shared
  containers, so only the session holding the operator's delegation for it
  does it, only with ACS idle (`agent-compute status`: nothing executing or
  queued in a container) and consumer holds in place. Procedure:
  `docker desktop stop`; `open -a Docker` (never from a tool shell that may
  exit with it); `docker start <the listed containers>`; then, in a later
  tool call, verify that a plain `docker run --rm <image> true` returns
  within seconds; then "done" to the holders. Everyone else reports a wedge
  to that session or the maintainer and never restarts Docker (the skill's
  "Never" list).

## Placement across hosts is the relief for waits

ACS has no priority, reservation or time-slicing. When a project waits
behind another's long jobs:

1. a project that declares several hosts (`projects.<p>.hosts`) is placed on
   the least busy eligible host automatically (experimental, see
   `docs/limitations.md`); declaring a second host for a project is an
   operator config change at an idle checkpoint (`docs/consumer-template.md`
   then regenerates its file);
2. otherwise the orchestrators negotiate a window by message (above);
3. a wait that contradicts the declared policy is a defect report, not a
   planning input (skill, "Refusals and defects").

## The defect path

consumer executor → its orchestrator (a written report) → the project's
dogfood log (`docs/dogfood-log-template.md`) and the maintainer → a
tracked issue or a "no action" note with the version that produced it → a
fix by the `AGENTS.md` workflow → a deploy by the protocol above → the
consumer's log gets a resolution line. A consumer orchestrator may also
open the upstream PR itself (`AGENTS.md`, "Classifying a change" and
"Review and release gates"); it tells the
maintainer, who deploys (with no maintainer running, the orchestrator
deploys by the same protocol and records it).
