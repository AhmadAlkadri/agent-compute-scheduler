# agent-compute-scheduler

**Status: alpha.** Agent-only admission for heavy jobs on one or two hosts.

> Agents may work concurrently, but resource-intensive jobs must share a
> common admission mechanism on the machine actually doing the computation.

Coding agents working on different projects submit heavy work (solves,
renders, studies, container workloads) without knowing about one another.
`agent-compute` (ACS) gives every configured **host** one heavy slot and
admits each job only when that host can take it. An agent submits a job
and either lets ACS **choose a suitable host** or **names one**; every
host keeps its own admission, lock, queue and blocks, and nothing is ever
replayed on another host after an uncertain end.

## What it is, and what it is not

ACS is a small, standard-library Python tool that sits between the agents
of one person (or one team) and a few machines they own. Per host it gives
you:

- **one heavy slot**, through one Pueue queue with a single group of
  parallelism 1, in FIFO order; projects are metadata, never separate pools;
- **an internal guard** (`src/agent_compute/guard/engine.py`, backend
  `acs-guard`, pinned by sha256): the lock, a memory pre-flight against
  measured available memory, thread pins, sampling, wall-time and memory
  kills, and the records;
- **a per-job runner** that revalidates the job's inputs, records the
  workload's identity (pid and exact start time, or container id and a job
  marker), verifies after the end that nothing of the job is left, and
  **blocks the host durably** when it cannot prove the workload gone;
- **optional placement across hosts**, on the submitting side: a registry
  of routes, per-project eligible hosts and path mappings, a read-only
  summary of every host, and a local placement record for every job sent
  elsewhere.

Your code, Makefiles, entrypoints and tests stay unchanged; agents wrap an
otherwise unchanged command. Collaborators who do not run agents never
need ACS.

ACS is **not**:

- a cluster scheduler (no Slurm, no multi-user accounting, no hundreds of
  nodes);
- **containment**: tracking is cooperative, there are no cgroups, and a
  process that deliberately detaches can escape it (see
  [`docs/limitations.md`](docs/limitations.md));
- a fairness mechanism: scheduling is FIFO per host, and nothing more;
- a security boundary against a hostile user of the same account.

## Supported paths

| | Supported in the alpha |
|---|---|
| Platforms | macOS arm64 (with Docker Desktop for container jobs); Linux x86_64 (tested on Ubuntu 24.04 under WSL2; native Linux is expected to work but is not separately validated) |
| Venues | host processes; `docker exec` into a **listed** container |
| Queue | Pueue 4 (`pueue`, `pueued`) |
| Python | 3.11 or later (standard library only) |
| Hosts | one or two per operator |
| Cross-host | explicit `--host <label>` routing over ssh |
| Automatic placement | **experimental**: tested in the suite, not exercised in production |

The full matrix, with the caveats of each path, is
[`docs/execution-paths.md`](docs/execution-paths.md); what is tested,
deployed and used for real is in [`docs/limitations.md`](docs/limitations.md).

## Quickstart

This is a throwaway trial in a separate directory, so it does not touch a
real installation. Requirements: Python 3.11 or later; git; `ps` (the procps
package on a minimal Linux image); Pueue 4
(`pueue` and `pueued` on `PATH`; upstream project
<https://github.com/Nukesor/pueue>, version 4, also available from package
managers such as Homebrew and from the upstream release binaries); Docker
only for container jobs.

```bash
git clone <this repository> && cd agent-compute-scheduler

# 1. install into a throwaway home (short path: Unix socket limit, below)
export ACS_INSTALL_HOME=/tmp/acs-trial
python3 bin/agent-compute config init --label trial > /tmp/acs-trial.toml
$EDITOR /tmp/acs-trial.toml     # state_dir is already absolute, under /tmp/acs-trial; add one project (below)
python3 install.py install --config-from /tmp/acs-trial.toml
ACS=/tmp/acs-trial/.local/bin/agent-compute     # reads /tmp/acs-trial/.config/agent-compute/config.toml

# 2. run the full standard test suite on this host with the gate tool (about
#    20 minutes; it needs pytest in the interpreter you give it) and commit its
#    evidence file in a local evidence repository; the label is the config's host
python3 -m venv .venv && .venv/bin/pip install pytest
python3 tools/gate_run.py --label trial --docker hidden --python .venv/bin/python \
    --out /tmp/acs-trial-gate/gate-trial.txt --evidence-repo /tmp/acs-trial-evidence

# 3. pin the engine, as the host's operator (not an agent)
$ACS pin-backend --evidence /tmp/acs-trial-evidence/gate-trial.txt \
    --authorized-by "<who, when, what>"
$ACS doctor

# 4. a first job, from the project's checkout
cd <your checkout>
$ACS submit --case hello --timeout-min 5 --reserve-gb 1 \
    --request-id hello:1 -- python3 -c 'print("hello from a slot")'
$ACS wait <job id printed by submit> --timeout 120
$ACS status <job id> --json
```

`config init` prints only the machine part of the config. Add one project
by copying the `[projects.my-project]` table of
[`examples/config.example.toml`](examples/config.example.toml) and editing
at least `roots` (globs of the git checkouts of your project, for example
`["~/src/my-project", "~/src/my-project-*"]`) and `case_prefix`; the other
keys of that table can stay as they are for a first job
(`inputs = ["."]`, `max_timeout_min`, `max_threads`, no containers). A
container job needs three more keys in that table: `containers` (the
running containers the project may use), `container_limits` (required once
`containers` is non-empty: the containers whose memory and CPU limits the
guard applies per job, or `[]` for none) and `container_workdir` (the
working directory inside the container; without it every submit must pass
`--container-workdir`). Leave out the commented `[hosts.*]` and
`hosts`/`on` lines unless you set up a second host.

The arguments of a first job: `--case` names the kind of work, a key under
which ACS keeps the job's measured memory peak (the project's `case_prefix`
is prepended); `--reserve-gb` is your estimate of the job's peak memory,
required for a case that has never been measured; `--request-id` is a
stable id of this request, so that a repeated submit of the same id is
refused instead of running twice. The skill's "Submitting" section has the
rest.

Take the trial down with `$ACS daemon stop`, `ACS_INSTALL_HOME=/tmp/acs-trial
python3 install.py uninstall` and the removal of `/tmp/acs-trial`.

Notes on the trial:

- The wrapper of a trial installation is bound to the trial's own config
  (`<ACS_INSTALL_HOME>/.config/agent-compute/config.toml`), so it never
  reads a real installation's config by default. `--config` and a
  `AGENT_COMPUTE_CONFIG` you set still override it (in that order). The
  real installation's wrapper is unchanged. `config init` run with
  `ACS_INSTALL_HOME` set prints an absolute `state_dir` under that home and
  refuses one that the real installation's config (or the config named by
  `AGENT_COMPUTE_CONFIG`) already uses. `install.py uninstall` refuses while
  `AGENT_COMPUTE_CONFIG` or `--config` names a config other than the
  trial's.
- `doctor` reports a second queue daemon on a host that already has an
  installation (`single authority: queue daemon`). That is the truthful
  report of a second authority, not an outage.
- A Unix socket path must stay under about 100 characters, so keep the
  install home short (the scratch directories of agent harnesses are often
  too long; `/tmp/<name>` works), or set a shorter absolute `state_dir`. The Pueue config and socket are derived
  from it: `<state_dir>/pueue/pueue.yml`, `<state_dir>/pueue/run/pueue.sock`.

## Install (per host)

```bash
git clone <this repository> && cd agent-compute-scheduler
python3 bin/agent-compute config init --label <host> > /tmp/acs-config.toml
$EDITOR /tmp/acs-config.toml          # review every value; add projects and routes
python3 install.py install --config-from /tmp/acs-config.toml
agent-compute doctor
```

`config init` prints a config with host-derived profile numbers
(`floor_gb = max(2, 0.125 × RAM)`, `budget_gb = 0.25 × RAM`,
`container_cpus = cores − 2`) and writes nothing;
`examples/config.example.toml` is the same starting point by hand. Review in
particular:

- **`state_dir`** is printed as an **absolute** path under the install home
  `config init` runs from (`$ACS_INSTALL_HOME`, else your home: for the
  host's one installation that is the `~/.local/state/agent-compute` it has
  always meant), and `config init` refuses a state dir that another
  installation's config already uses. `~` in a config is always the real
  `HOME`, never `ACS_INSTALL_HOME`.
- **`python`, `queue.pueue` and `queue.pueued`** are recorded as found on
  your `PATH`, unresolved, and `install.py` writes the interpreter running
  it (`sys.executable`) into the wrapper (`install --python` overrides it).
  Check that each is a stable absolute path (for example
  `/opt/homebrew/bin/python3`, `~/.local/bin/pueue`), not a path into a
  temporary or symlinked directory.
- the guard home (`[backend] guard_home`, default `<state_dir>/guard`), the
  profile numbers, and every project's roots and containers.

`install.py install --config-from <file>` copies the config to
`~/.config/agent-compute/config.toml` (0600) only if no machine config
exists (it is never overwritten).

### Pin the engine

Until the host's operator pins the installed engine, every submit is
refused (`doctor`: `backend guard matches verified fingerprint` FAIL;
`config init` prints a placeholder sha). The pin ties the guard code to a
full test run on this host:

```bash
# 1. a full-suite run on this host, written by the gate tool and committed in a
#    local evidence repository (no remote is ever added)
python3 tools/gate_run.py --label <host> --docker hidden|with \
    --python <interpreter with pytest> --out <file> --evidence-repo <dir>
# 2. the operator pins, with their own authorization text
agent-compute pin-backend --evidence <dir>/<file> --authorized-by "<who, when, what>"
```

`tools/gate_run.py` (standard library only) refuses a checkout with
uncommitted tracked changes, untracked pytest configuration files, a set
`PYTEST_PLUGINS`, a missing Pueue 4.x (`pueue` and `pueued`), then runs `<interpreter> -m pytest -v -rs -p
no:cacheprovider` from this repository with no path, no `-k`/`-m` filter and
no other pytest argument (an extra argument, or `PYTEST_ADDOPTS` in the
environment, is refused). `--label` is this config's `host` (`agent-compute
doctor` prints it), so a host with a new label needs its own run; `--docker
hidden` removes `docker` from the run's PATH (nothing is uninstalled) and the
container tests skip with a counted `[docker]` reason; `--docker with` needs a
working Docker with the test image (`ACS_TEST_IMAGE`, else the public pinned
image; `ACS_TEST_PULL=1` pulls it) and records it, and a run that still skipped
Docker tests is reported as unresolved (as is any skipped Pueue test). The
run takes about 20 minutes with Docker on a recent Mac. If HEAD, the engine or
the tracked files change during the run, the file is marked `REFUSED` with
`EXIT 65` and is not committed. The tool exits with pytest's exit status
(0-5), or with its own code (64 refused before the run, 65 repository changed,
66 unresolved evidence, 67 evidence repository, 68 write error), and commits
the file (only that file) when the run passed with a `consistent`, `untagged 0`
skip accounting. The engine in the header is the checkout's
`src/agent_compute/guard/engine.py`: run the tool from the checkout the
installed version was installed from (`doctor` prints the installed engine's
sha256).

The file has the gate evidence format. Line 1, written before the run:

```text
# <host> full suite on <host>, DOCKER HIDDEN (...) | WITH Docker (image <image>); profile standard; source sha <40 hex>; engine sha256 <64 hex>; pueue <v>; Python <v> (<interpreter>); tracked changes: 0; redactions: <n>; written by tools/gate_run.py 1; <UTC time>
```

line 2 is the skip accounting (`# ` plus `python3 tools/skip_report.py
--summary-line`), then the pytest output, ending with `EXIT <code>`.

Redaction: before writing, the tool replaces the value of any `NAME=value`,
`'NAME': 'value'` or `"NAME": "value"` whose NAME contains `TOKEN`, `KEY`,
`SECRET`, `PASSWORD`, `PASSWD`, `AUTH`, `CREDENTIAL`, `COOKIE` or `SESSION`
(any case), and any occurrence of the value of such an environment variable of
its own process, with `<redacted by gate_run>`, and records the count in the
header (`redactions: <n>`). It is not a secret scanner: a secret in another
shape is not found, so read the file before you share it.

`pin-backend` accepts an evidence file only if it is committed and unmodified
in a git repository; its header is the first line, carries the tool's
`written by tools/gate_run.py <n>` token and says `full suite on <this host>,`,
the internal engine (`profile standard` or `legacy`), a full source sha,
`tracked changes: 0` and the installed engine's sha256; line 2 is a skip
accounting that is `consistent` with `untagged 0`; the output has no
`deselected` test, a collected count equal to the skip line's and to the
final summary's, 0 failed and no errors, no Docker skip under a `WITH Docker`
header, no Pueue skip (the header records `pueue 4.x`), and the file ends
with `EXIT 0`; the skip categories are recomputed from the output and must
equal line 2, and the header, collected count, summary and `EXIT` lines each
appear exactly once. **These checks are of form: they catch
a smoke run, a filtered run, a missing or hand-edited header and a truncated
file, not forgery.** Someone who can write and commit a file can still write a
well-formed one; the committed-and-unmodified rule and your own
`--authorized-by` text, recorded in `<state_dir>/authorizations.jsonl`, are
what remain. The command changes only `[backend].sha256` in the config. A
release that changes `engine.py` needs a new run and a new pin on each host.

### The installer

`install.py` takes global options before the subcommand: `--prefix`
(default `~/.local/share/agent-compute`), `--bin` (default `~/.local/bin`)
and `--config` (default `~/.config/agent-compute/config.toml`); `install`
takes `--config-from`, `--python` and (for development only)
`--allow-dirty`. The environment variable **`ACS_INSTALL_HOME`** replaces
`~` in all of these defaults and in the skill-link directories
(`<home>/.claude/skills`, `<home>/.agents/skills`). When the config is not
the one the CLI reads by default (a non-default `ACS_INSTALL_HOME` or
`--config`), the wrapper it writes exports that config as the CLI's
default (`--config` and the caller's `AGENT_COMPUTE_CONFIG` still win);
the installer records that config in its manifest. `uninstall` looks for
unfinished jobs in the recorded config (for an older manifest: the one the
installed wrapper binds, or the CLI's default config) and in the `state_dir`
it names, expanded as the CLI does (`~` is the real `HOME`), and refuses
(also with `--force`) a `--config` or an `AGENT_COMPUTE_CONFIG` that names
another file. (An installation made by an older installer recorded no binding and
its wrapper binds none: `uninstall` then examines both the CLI's default config
and the installation's own under its install home.) Reinstalling with a
different `--config` rebinds the installation, and jobs under the old config
are then outside what `uninstall` examines: finish or cancel them first. It
cannot see jobs of a CLI that was run with an explicit
`--config`/`AGENT_COMPUTE_CONFIG` elsewhere. `install.py` is not part of a
version directory: run `uninstall` from the current `main` checkout.

The installer:

- copies `bin/ src/ skill/` from a **clean commit** into
  `~/.local/share/agent-compute/versions/<version>-<sha10>/` (read-only),
  records its safety level, and points `current` at it;
- writes a two-line wrapper `~/.local/bin/agent-compute`;
- symlinks the skill into `~/.claude/skills/` and `~/.agents/skills/` when
  those directories exist;
- records every file it owns in
  `~/.local/share/agent-compute/install-manifest.json`, refuses name
  collisions, edits no shell profiles and installs no services. The Pueue
  daemon starts on demand.

If `~/.local/bin` is not on `PATH`, invoke `~/.local/bin/agent-compute`.

**Upgrade**: `python3 install.py install` from a clean commit of the new
release, at a moment when nothing is queued or running on the host (check
`agent-compute status` and `python3 install.py status`), then
`agent-compute doctor`. Queued jobs keep running the *code* version
directory they were submitted with, but the runner loads the live
`config.toml` when a job is dispatched. A release that changes the engine
needs a new pin. A config in an older schema is refused until the operator
runs `agent-compute config migrate --authorized-by …`, which keeps every
effective value and a backup.

**Rollback**: `python3 install.py activate <version>` refuses, with no
force option, a version whose recorded safety level is missing or lower
than the active one's (or below the installer's minimum). A bad release is
answered by rolling forward with a fix.

**Uninstall**: `python3 install.py uninstall` removes only the owned files
(the version dirs, `current`, the wrapper, the skill links, the manifest
and a config it created) and refuses while jobs are unfinished, or
while it cannot read the config or the job records to tell
(it names the path). `--keep-config` keeps a config the installer created; a pre-existing
config is never touched. It does **not** stop the queue daemon (run
`agent-compute daemon stop` first, with nothing queued or running), and it
leaves what it does not own: the state dir (jobs, records, `host-id.json`,
`authorizations.jsonl`, the event log) and the guard home. `uninstall
--force` and concurrent installs on one host are **operator-only**.

## Configuration

`~/.config/agent-compute/config.toml` (mode 0600; schema
`agent-compute-config/2`; unknown keys are errors). It holds:

- the host label and the state directory (local disk; jobs, blocks,
  placement records, the event log, authorizations);
- `[backend]`: the engine pin, the guard home (default `<state>/guard`),
  optional marker aliases for compatibility with another guard sharing the
  guard home, the optional dedicated budget, and the machine profile
  `[backend.profile]` (budget, floor, container CPUs);
- `[queue]`: the Pueue binaries and the one group;
- `[hosts.<label>]`: routes to other hosts (`ssh` or `ssh-wsl`, timeouts);
- per project `[projects.<key>]`: roots (worktree globs), case prefix,
  record locations (state dir by default; in the tree only with
  `records_in_tree = true`), input pathspecs for the changed-input check,
  env passthrough, timeout and thread ceilings, containers (`containers`,
  with `container_limits` and `container_workdir`, as in the example), the
  reserve detector, and `hosts` plus `[projects.<key>.on.<host>]` (the
  checkout and container on another host) for placement.

[`examples/config.example.toml`](examples/config.example.toml) is a
commented starting point; [`docs/memory-model.md`](docs/memory-model.md)
explains the profile numbers and the admission rule.

## Using it

```bash
agent-compute --version                      # installed version and source commit
agent-compute doctor                         # this host's readiness, incl. the single-authority checks
agent-compute hosts --json                   # every configured host, read-only
agent-compute submit --case <key> --timeout-min 60 --reserve-gb 2 \
    --request-id <run>:<step> -- python3 scripts/run_x.py --n 16
agent-compute status <job> --json            # phase, wait reason, outcome, evidence
agent-compute wait <job> --timeout 540       # blocking; exit 75 = still running
agent-compute cancel <job>                   # your own job only
agent-compute --host other submit --cwd /abs/path/on/other ... -- <cmd>   # pinned to a host
agent-compute placements resolve <request-id>   # after a lost reply
agent-compute reconcile [--terminate <job>]     # after an ambiguous end (own job)
agent-compute resume                            # after reconcile / a daemon restart
```

- `submit` means **accepted**, never "succeeded"; the outcome comes from
  `wait`/`status`. The workload's own exit code is kept; queue wait and
  execution time are reported separately.
- A project that declares several hosts (`projects.<p>.hosts`) is placed
  on the eligible host with the fewest unfinished jobs (experimental);
  `--host X` pins. The job is sent once: no retry, no fallback to another
  host. A placed or pinned submit from a checkout requires that commit at
  the destination unless `--any-commit`.
- `--timeout-min` is required and capped per project; an unmeasured case
  needs `--reserve-gb` or a `--probe`.
- The job environment is controlled: only `PATH HOME LANG LC_ALL USER
  LOGNAME SHELL TMPDIR`, the project's `env_passthrough` names and explicit
  `--env NAME=VALUE` reach a host job; credential-like names are refused; a
  container job gets only the guard's own variables.

`doctor` checks, read-only: the config, the host identity and the
migration, the installation's safety floor, the engine pin and its
authorization, the machine profile and memory budget, the state dir, Pueue
and the one-slot group, blocks, lost supervision and the dispatch gate,
each project and remote host, and that the host has **exactly one
admission authority** (one guard home with users, one state dir with live
work, one Pueue daemon serving an agent-compute queue).

## Agent integration

ACS is for agents, and the agent-facing protocol is a skill:

- [`skill/agent-compute-scheduler/SKILL.md`](skill/agent-compute-scheduler/SKILL.md)
  is the canonical skill. The installer links it into
  `~/.claude/skills/` and `~/.agents/skills/` when those directories
  exist. It covers placement, submitting, waiting, outcomes and exit
  codes, recovery, refusals, and the block to paste into a subagent's
  brief. `python3 tools/skill_check.py` verifies that every command,
  option and outcome it names exists in the CLI.
- Each project that submits heavy work keeps one agent-only file,
  `.agents/compute-scheduler.md`, with what is true for that project (its
  checkouts, hosts, ceilings, containers). The template, the slots and
  where each value comes from are in
  [`docs/consumer-template.md`](docs/consumer-template.md).
- [`docs/operating-model.md`](docs/operating-model.md) describes the
  roles (operator, maintainer, orchestrator, executor), what each may do,
  recorded authorizations, and the notice-and-hold protocol for deploys.
- [`docs/dogfood-log-template.md`](docs/dogfood-log-template.md) is the
  format of the append-only observation log that a consuming project keeps.

Agents never run `pin-backend`, `config migrate`, `uninstall --force` or
anything that edits ACS state by hand; they report to their operator.

The multi-session machinery (messaging other sessions, notices and holds
before a deploy, the dogfood log, a maintainer session) matters only when
several agent sessions share a host. A single user with one agent on one
host needs the skill, a project file if they like, and `doctor`.

## Limitations

Read [`docs/limitations.md`](docs/limitations.md) before relying on ACS. In
short: tracking is cooperative (a process that detaches escapes it), a
container job needs `python3` in the container, there is no audited command
to clear a block, credentials are screened by variable name only, scheduling
is FIFO, and automatic placement has not run in production.

## Development and testing

```bash
uv venv -p 3.12 .venv && uv pip install -p .venv/bin/python pytest
# or, without uv (standard library only): python3 -m venv .venv && .venv/bin/pip install pytest
.venv/bin/python -m pytest -v -rs     # the standard profile: the internal guard; isolated; never touches real locks
ACS_TEST_PULL=1 .venv/bin/python -m pytest -v -rs   # container tests: pull the public pinned test image if absent
ACS_TEST_IMAGE=<image> .venv/bin/python -m pytest -v -rs   # another image (needs python3, bash, setsid, nice, sha256sum)
python3 tools/skip_report.py <pytest -v -rs output>  # skip accounting (per reason, file, category)
tools/ci.sh                           # what CI runs: pinned Pueue 4.0.4 (if absent), the standard profile, skip accounting, skill check
python3 tools/skill_check.py          # every command, option and outcome in the skill exists in the CLI
python3 tools/ledger_report.py --since <tag>   # read-only job report over a state dir, per version
```

The standard profile needs this repository, Python 3.11 or later with
pytest, `ps` (procps) and Pueue on `PATH`; Docker is optional. The full suite takes about
20 minutes with Docker on the maintainer's Mac. It runs the internal guard
of this tree with a temporary guard home, fake memory readings and generic
test profiles (`tests/guard_profiles/`), a private `pueued` on a short
`/tmp` socket, and disposable, labelled containers from the public image
`python:3.11.6-slim-bookworm`, pinned by digest (`tests/conftest.py`). The
suite never touches a real lock, a real queue or an existing container.
Without Docker, or without the image, the container tests skip with a
counted `[docker]` reason (`tools/skip_report.py` accounts every skip by
category and flags an unclassified one): such a run is not a gate for a
change that touches container code.

Continuous integration (`.github/workflows/ci.yml`) runs on GitHub-hosted
`ubuntu-24.04` runners for Python 3.11, 3.12 and 3.14, on pull requests and on
pushes to `main`, with a read-only token and no secrets. It runs
`tools/ci.sh`, which you can run yourself from a clone: Pueue 4.0.4 by
pinned checksum (or the Pueue 4 already on `PATH`), pytest at pinned versions,
the standard profile with Docker and the public pinned image (`ACS_TEST_PULL=1`),
the skip accounting (it fails unless the summary says `consistent` and
`untagged 0`) and `tools/skill_check.py`. CI does not cover the macOS Docker
Desktop paths or a real two-host installation: the maintainer validates those
in the release gates on a macOS machine with Docker Desktop and a WSL2
distribution.

## Provenance

Where parts of the code come from, and how to read the references to a
private development record in source comments, is in
[`docs/provenance.md`](docs/provenance.md).

## License

MIT; see [`LICENSE`](LICENSE).

## Documents

| Document | What it covers |
|---|---|
| [`skill/agent-compute-scheduler/SKILL.md`](skill/agent-compute-scheduler/SKILL.md) | the agent-facing protocol (canonical source; installed as a skill) |
| [`docs/consumer-template.md`](docs/consumer-template.md) | the per-project instruction file (`.agents/compute-scheduler.md`), with a worked example |
| [`docs/operating-model.md`](docs/operating-model.md) | roles, recorded authorizations, notices and holds |
| [`docs/dogfood-log-template.md`](docs/dogfood-log-template.md) | the observation log format |
| [`docs/architecture.md`](docs/architecture.md) | decision records ADR-001 to ADR-003 and researched sources |
| [`docs/execution-paths.md`](docs/execution-paths.md) | supported host × venue × placement paths, limits |
| [`docs/memory-model.md`](docs/memory-model.md) | admission memory, the macOS Docker VM credit, budgets and container limits |
| [`docs/limitations.md`](docs/limitations.md) | limitations; what is tested, deployed and exercised in real use |
| [`docs/provenance.md`](docs/provenance.md) | origin of the code; how to read references in source comments |
