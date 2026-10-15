# Consumer instruction file: template and worked example

Every repository whose agents submit heavy work through ACS keeps one
agent-only file, `.agents/compute-scheduler.md`. The skill
(`skill/agent-compute-scheduler/SKILL.md`) says how ACS works everywhere;
this file says what is true **for this project**: where its checkouts
are, which hosts it may run on, its ceilings and containers, its input
exclusions, its light lane and its project-specific rules. It is derived
from the project's entries in the machine config of each host
(`~/.config/agent-compute/config.toml`, `[projects.<key>]`), so it is
regenerated from the template below whenever that config changes (a
deploy notice names such changes). It never contains a credential, a
token, a private key or a config file's secrets: only paths, names and
numbers that `agent-compute doctor` also prints.

Who edits it: the project's orchestrator (a consumer executor never does).
Who checks it against the hosts: the same orchestrator, with `agent-compute
doctor` and `agent-compute hosts --json` on the host its agents run on.

Contents: [the consumer's loop](#the-consumers-loop), [the
slots](#the-slots-and-where-each-value-comes-from), [the
template](#the-template), [a worked example](#worked-example-project-a).

## The consumer's loop

ACS is dogfooded: the projects that use it are where its defects show.
The maintainer's half of the loop is to triage what the consumers report
(`docs/operating-model.md`, "The defect path"); the consumer's half, stated
once here (the template's "Reporting" section below is its short form in
each project's file), is:

1. **Use ACS for every heavy job** the project file names, through the
   skill and its "Brief for a subagent" block; never around it, not even
   when it refuses or waits ("Deferred: resources occupied" is an
   acceptable outcome).
2. **Log every observation in the append-only `.agents/acs-dogfood.md`**
   (format: `docs/dogfood-log-template.md`): one entry per friction,
   surprise, defect, operator intervention or workaround you were tempted
   to make, and a one-line entry for routine successes (they are the
   baseline). The orchestrator writes it; executors report to the
   orchestrator. Every entry names the ACS version that produced it (the
   job view's `acs_version`), the job ids, the evidence paths, who it was
   reported to and the writer's session prefix. Never edit an entry: a
   resolution is appended under it.
3. **The defect-report rule.** A refusal or a wait that contradicts the
   declared policy (the skill, this project's file, the numbers `doctor`
   prints) is a **defect report, not a planning input**: log it, send it
   to the ACS maintainer (a session, whose prefix is in the project file; the
   operator when none runs) with the job id, the version and the evidence
   paths, and do not shrink the plan, the reserve or the ceilings to make
   it go away. An executor sends its report to its orchestrator, in
   writing; it never merges, installs, deploys or reconfigures ACS. An
   orchestrator may fix ACS upstream itself (`AGENTS.md`, "Classifying a
   change" and "Review and release gates": branch, fail-first test, PR,
   gates) and tells the
   maintainer session, which deploys; it never vendors the fix into the
   scientific repository.
4. **When to ask for a window.** FIFO is the contract: there are no
   priority rules in project files. Ask by message (`ListAgents`,
   `SendMessage`; request → "hold ok <host>" → "done") when the project
   needs a host sooner than the queue gives it (message the orchestrator
   whose jobs are ahead), needs the host quiet (a long exclusive study, a
   container rebuild), or needs a Docker Desktop restart on a Mac (the
   action of the session the operator has delegated it to). A
   project that declares several hosts is placed automatically, which is
   the configured relief for waits (`docs/operating-model.md`).
5. **Answer holds.** When the maintainer or another orchestrator asks for
   a hold, stop submitting to that host, answer "hold ok <host>" naming
   any job of yours still queued or running and whether it may finish
   first, and resume only after its "done". After a deploy notice,
   re-read what it names (the skill, a config change) and regenerate this
   project's file if the machine config changed.
6. **Notes on request.** After the first week on a new release line, the
   maintainer may ask for a short note on what the skill made easy or
   hard; send it to the maintainer.

## The slots and where each value comes from

| Slot | Value | Source (per host) |
|---|---|---|
| `{{PROJECT}}` | the ACS project key | `[projects.<key>]` |
| `{{REPO_NAME}}` | the repository's name, if different from the key | the project |
| `{{HOSTS}}` | the hosts the project may run on, in order | `projects.<key>.hosts` on the submitting host (default: that host only) |
| `{{PLACEMENT}}` | "placed automatically" (several hosts) or "this host only; other hosts by `--host X` with an absolute `--cwd`" | the same |
| `{{ROOTS.<host>}}` | the checkout globs on each host | `roots` of that host's config |
| `{{CWD.<host>}}` | the checkout another host maps to (`--cwd` there) | `projects.<key>.on.<host>.cwd` (when declared), else the remote config's `roots` |
| `{{CASE_PREFIX}}` | what ACS prepends to `--case` | `case_prefix` |
| `{{RECORDS}}` | where the guard's runlog and baseline go | `runlog`, `baseline` (`{state}/projects/{project}/…` by default; in-tree only with `records_in_tree = true`) |
| `{{INPUT_EXCLUDES}}` | the directories the changed-input check ignores | the `:(exclude)…` entries of `inputs` |
| `{{ENV_PASSTHROUGH}}` | session variables a host job receives | `env_passthrough` |
| `{{MAX_TIMEOUT_MIN.<host>}}`, `{{MAX_THREADS.<host>}}` | the ceilings | `max_timeout_min`, `max_threads` |
| `{{CONTAINERS.<host>}}` | the containers the project may use, and `--container-workdir` | `containers`, `container_workdir`, `on.<host>.container` |
| `{{CONTAINER_LIMITS}}` | containers the guard may `docker update` per job | `container_limits` |
| `{{PROVENANCE}}` | the container file whose change holds a queued job | `container_provenance` (none: say so) |
| `{{RESERVE_DETECTOR}}` | how a reserve is derived from the command | `reserve_detector` (`none` by default) |
| `{{BUDGET.<host>}}` | the host memory budget | `agent-compute doctor`: "host memory budget" (a host fact, not a project setting) |
| `{{SYNC}}` | how a checkout (and any container copy) is brought to the submitting commit on each host before a submit: ACS verifies the commit at the destination (the automatic `--require-commit` of a placed or pinned submit) but never pushes, pulls or syncs | the project's own procedure (for example push and pull, a sync script) |
| `{{LIGHT_LANE}}` | what the project runs without a slot, and how | the project's own rules (ACS has none) |
| `{{PROJECT_RULES}}` | project-specific rules: container ownership, sync procedures, output layout, what counts as heavy | the project's own rules |
| `{{DOGFOOD_LOG}}` | the project's append-only ACS observation log | `.agents/acs-dogfood.md` (format: `docs/dogfood-log-template.md`) |
| `{{MAINTAINER}}` | how to reach the ACS maintainer | the current maintainer session's prefix, else the operator |
| `{{ACS_DEV_REPO}}` | where an orchestrator reports or fixes ACS defects | named by the operator: an `owner/name`, a URL or a local checkout path; the skill names none, and this file is the only place that does. A user of the public release puts the public repository's issue tracker (or their own fork, to prepare a change), as that repository's `CONTRIBUTING.md` describes; the maintainer's private development repository is a different value that only the maintainer uses |

Rules for filling it:
- One column per host the project may run on; a host where the project is
  not configured is written "not configured" (an agent must not submit
  there).
- Numbers are the config's, copied; never a promise of more. If the file
  and `doctor` disagree, `doctor` wins and the file is regenerated.
- No cross-project priority rule ("submit only when project Y has nothing
  queued"): FIFO is the contract; windows are
  negotiated by message (`docs/operating-model.md`).
- History (what an older ACS did) goes into the project's own history, not
  into this file.

## The template

````markdown
# Shared compute admission: {{REPO_NAME}}

Agent-only. Applies when an agent runs on a host with ACS installed
(`agent-compute doctor` answers). Collaborators, CI and ordinary launches
ignore this file; nothing in the code depends on it.

## Rule

Heavy work runs only through `agent-compute`, the shared admission queue
(global skill `agent-compute-scheduler`: load it with
`Skill(agent-compute-scheduler)` before the first submit, and pass its
"Brief for a subagent" block to every executor that may submit). Heavy
here means: {{PROJECT_RULES.heavy}}. "Deferred: resources occupied" is an
acceptable outcome. Never bypass the queue, clear locks, raise ceilings or
shrink a reserve below a measured peak.

Light work: {{LIGHT_LANE}}.

Before the first job of a session: `agent-compute --version`,
`agent-compute doctor`, `agent-compute hosts --json`.

## This project's settings (machine config `[projects.{{PROJECT}}]`)

| | {{host 1}} | {{host 2}} |
|---|---|---|
| hosts and placement | {{HOSTS}}; {{PLACEMENT}} | |
| checkouts | `{{ROOTS.host1}}` | `{{CWD.host2}}` |
| case key | `--case <key>`, recorded as `{{CASE_PREFIX}}<key>` | same |
| records (runlog, baseline) | {{RECORDS.host1}} | {{RECORDS.host2}} |
| ceilings | `--timeout-min` ≤ {{MAX_TIMEOUT_MIN.host1}}, `--threads` ≤ {{MAX_THREADS.host1}} | ≤ {{MAX_TIMEOUT_MIN.host2}}, ≤ {{MAX_THREADS.host2}} |
| containers | {{CONTAINERS.host1}} | {{CONTAINERS.host2}} |
| container limits applied by the guard | {{CONTAINER_LIMITS}} | |
| host memory budget (doctor) | {{BUDGET.host1}} | {{BUDGET.host2}} |
| env | host jobs: {{ENV_PASSTHROUGH}} pass through, others by `--env NAME=VALUE`; container jobs get neither: `-- env NAME=VALUE <cmd>` | same |
| reserve detector | {{RESERVE_DETECTOR}} | |

Examples:

```bash
agent-compute submit --case <key> --timeout-min 60 --reserve-gb 4 \
    --request-id <run>:<step> --submitter <session> -- <command>
agent-compute submit --container <container> --case <key> --timeout-min 60 \
    --reserve-gb 6 --request-id <run>:<step> -- <command>
agent-compute wait <job> --timeout 540
```

## The changed-input rule here

The fingerprint covers the worktree's HEAD, its uncommitted and untracked
files except `{{INPUT_EXCLUDES}}`, every file named in the command, and,
for a container job, the container's id, the docker daemon and
{{PROVENANCE}}. Commit and sync first; then change nothing in that
worktree or container until the job is `launched`. {{SYNC}}

## Project-specific rules

{{PROJECT_RULES}}

## Reporting

Observations, friction and defects go into `{{DOGFOOD_LOG}}` (append-only;
format in the ACS repository's `docs/dogfood-log-template.md`) and, for a
defect, to {{MAINTAINER}}. A refusal that contradicts the settings above
is a defect report, not a planning input. Executors report to the
orchestrator; they never merge, install, deploy or reconfigure ACS.

ACS development repository: `{{ACS_DEV_REPO}}` (named by the operator).
An orchestrator fixes ACS defects there, per that repository's `AGENTS.md`
(branch, fail-first test, PR, gates, review), never in this project's repository. If
the field is empty or `unset`, ask the operator before fixing anything.
````

## Worked example: project-a

A neutral example, filled the way `examples/config.example.toml` would be
for a scientific project with two hosts: a laptop (`laptop`, macOS) and a
Linux box (`box`, reached over ssh). Secret-free. Two facts differ from
automatic placement: the laptop's entry declares no `hosts`, so a laptop
submit runs on the laptop, and a `box` job is pinned with an absolute
`--cwd` (which the laptop does not map or check).

````markdown
# Shared compute admission: project-a

Agent-only. Applies when an agent runs on a host with ACS installed
(`agent-compute doctor` answers): the laptop, and the Linux box through
`--host box`. Collaborators, CI and ordinary launches ignore this file;
nothing in the code depends on it.

## Rule

Heavy work runs only through `agent-compute`, the shared admission queue
(global skill `agent-compute-scheduler`: load it with
`Skill(agent-compute-scheduler)` before the first submit, and pass its
"Brief for a subagent" block to every executor that may submit). Heavy
here means: production solves and renders, refinement studies, container
jobs, multi-threaded jobs, anything over ~1 GB or ~10 minutes. "Deferred:
resources occupied" is an acceptable outcome. Never bypass the queue, clear
locks, raise ceilings or shrink a reserve below a measured peak.

Light work: `python3 -m pytest tests/unit` and plots measured at most 1 GB
and single-thread run directly, beside the heavy slot, outside ACS. Never
start a `nohup` driver or a `systemd-run` unit for heavy work on these
hosts.

Before the first job of a session: `agent-compute --version`,
`agent-compute doctor`, `agent-compute hosts --json` (and `agent-compute
--host box doctor`).

## This project's settings (machine config `[projects.project-a]`)

| | Laptop (`laptop`) | Box (`box`) |
|---|---|---|
| hosts and placement | the laptop's entry declares no `hosts`: a submit from the laptop runs on the laptop | pinned only: `agent-compute --host box submit --cwd <remote checkout> …` (absolute `--cwd`) |
| checkouts | `~/src/project-a`, `~/src/project-a-*` | `<remote checkout>` |
| case key | `--case <key>`, recorded as `pa.<key>` | same |
| records (runlog, baseline) | `~/.local/state/agent-compute/projects/project-a/{runlog,baseline}-laptop.*` (state dir; `status` prints them) | the same under the box's state dir |
| ceilings | `--timeout-min` ≤ 480, `--threads` ≤ 8 | ≤ 480, ≤ 4 |
| containers | `--container solver-box`, workdir `/work` | `--container solver-box`, same workdir |
| container limits applied by the guard | `solver-box` | `solver-box` |
| host memory budget (doctor) | as `doctor` prints | as `doctor` prints |
| env | host jobs: `PA_SLICE` passes through, others by `--env NAME=VALUE`; container jobs get neither: `-- env PA_SLICE=… NAME=VALUE <cmd>` | same |
| reserve detector | `none` | `none` |

Examples:

```bash
agent-compute submit --case solve-n24 --timeout-min 180 --reserve-gb 6 \
    --request-id solve-n24:run --submitter <session> \
    -- python3 scripts/run_x.py --n 24
agent-compute submit --container solver-box --case render --timeout-min 60 \
    --reserve-gb 6 --request-id render:fig3 -- python3 scripts/render.py --run fig3
agent-compute --host box submit --cwd <remote checkout> --container solver-box \
    --case <key> --timeout-min <M> --reserve-gb <G> --request-id <run>:<step> \
    -- python3 scripts/<script>.py
agent-compute wait <job> --timeout 540
```

## The changed-input rule here

The fingerprint covers the worktree's HEAD, its uncommitted and untracked
files except `docs/ outputs/ scratch/ .agents/`, every file named in the
command (a script under `/tmp` too), and, for a container job, the
container's id, the docker daemon and `.sync_provenance.json` inside the
container. Commit and sync first; then change nothing in that worktree or
container until the job is `launched`. Container trees are copies, not bind
mounts: sync `solver-box` from the worktree the job belongs to before
submitting (`scripts/sync_to_container.sh solver-box`), on either host.

## Project-specific rules

- Numbers an older version of this file bound (a smaller host budget) are
  history; ACS's machine config and the installed skill are the current
  policy. Agents never change a budget or the Docker VM.
- A Docker Desktop restart on the laptop stops `solver-box`; it is the
  orchestrator's action under the operator's delegation, only with consumer
  holds in place and no ACS job running (`docs/operating-model.md`).

## Reporting

Observations, friction and defects go into `.agents/acs-dogfood.md`
(append-only; format in the ACS repository's `docs/dogfood-log-template.md`)
and, for a defect, to the ACS maintainer (a session; the operator when none
runs). A refusal that contradicts the settings above is a defect report,
not a planning input. Executors report to the orchestrator; they never
merge, install, deploy or reconfigure ACS.

ACS development repository: `<owner/name, URL or local checkout path the
operator names>`. An orchestrator fixes ACS defects there, per that
repository's `AGENTS.md` (branch, fail-first test, PR, gates, review),
never in this project's repository. If the field is empty or `unset`, ask the
operator before fixing anything.
````
