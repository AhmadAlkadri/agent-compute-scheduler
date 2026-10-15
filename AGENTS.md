# AGENTS.md: contribution policy

This file is for anyone changing this repository, human or coding agent.
Users of the tool who only submit jobs need the skill
(`skill/agent-compute-scheduler/SKILL.md`), not this file.

`agent-compute-scheduler` provides shared agent compute admission for one or
two hosts: the `agent-compute` CLI, an internal resource guard
(`src/agent_compute/guard/`, backend `acs-guard`), a per-host Pueue queue, an
installer and a canonical skill. What it is and what it is not: see
[`README.md`](README.md). The design is in
[`docs/architecture.md`](docs/architecture.md); the limits are in
[`docs/limitations.md`](docs/limitations.md). How to propose a change, and
how an accepted change reaches a release: [`CONTRIBUTING.md`](CONTRIBUTING.md).

## What a change must preserve

A change preserves these invariants, or says that it changes one and
justifies it.

- **One heavy slot per host.** One group with parallelism 1 per host.
  Projects are metadata, never pools.
- **The lock.** The lock is the file `lock` in the configured guard home
  (`backend.guard_home`, default `<state_dir>/guard`; a deployment may keep
  a legacy home such as `~/.smc-guard` by configuration, see
  [`docs/provenance.md`](docs/provenance.md)). Only a **guard process**
  opens and flocks it: the internal guard, run by the runner as its own
  process for one claimed job, or an external caller of a guard that shares
  the same home. A guard opens it with `open(..., "a+")`, which may create
  it, then `flock(LOCK_EX|LOCK_NB)` on the same path. The admission layer
  (cli, runner, queue, reconcile, doctor, status) never opens it for
  locking; it observes its users read-only. The lock is taken once per job,
  never a second time by admission or by a nested guard (the marker
  pass-through requires the enclosing claimed job). Nobody renames,
  relocates or unlinks a lock file. A change of `guard_home` is an
  operator's configuration change at an idle checkpoint, refused
  (`doctor`, `submit`, dispatch) while any unfinished job or in-flight
  marker references the old home. `reconcile` may remove only the guard's
  in-flight marker (`inflight.json`, next to the lock), and only on ACS
  evidence.
- **An ambiguous end blocks the host durably.** Reconcile works on evidence
  only. Signals go only to start-time-proven identities.
- **No automatic start or replay after a daemon restart.** There are no
  force or bypass options for agents. The internal guard runs only for a
  claimed ACS job (`run` and `probe` without the claim are refused); its
  test-only bypass works only with a sandbox guard home.
- **The job environment is controlled.** No credentials go into manifests,
  logs or the queue.

Fail closed. A change that turns a refusal, a block or a "cannot prove it"
into a pass needs a stated reason and a test that shows the new boundary.

## Classifying a change

Classify every change with this mechanical rule, from the files and
functions the diff touches. The size of the diff does not matter.

A diff is **safety-semantic** if it touches any of:

- `src/agent_compute/procs.py`, `runner.py`, `queue.py`, `state.py`,
  `config.py` or `safety.py` (the installation safety levels);
- anything under `src/agent_compute/guard/` (the internal guard: it takes
  the lock, launches, kills and verifies cleanup);
- anything under `bin/`;
- all of `install.py`;
- in `src/agent_compute/cli.py`: the reconcile, cancel, `detect_lost`,
  terminate, resume, daemon or wait code (`cmd_reconcile`,
  `_reconcile_job`, `_never_launched`, `_record_resolution`,
  `_workload_seen`, `terminate`, `cancel_job`, `cmd_cancel`,
  `_finish_if_unclaimed`, `settle_unclaimed`, `detect_lost`, `cmd_resume`,
  `cmd_daemon`, `cmd_wait`, `cmd_pin_backend` and its evidence and config
  writers, `reconcile_inflight` and `_marker_job` (the in-flight marker),
  `backend_path` and `backend_identity`, the reservation and enqueue in
  `accept` (`_reserve`, `_mark_queued`), the safety-floor checks in
  `accept` and `cmd_doctor`, and anything they call that decides an
  outcome, a block, a signal, an admission or the accepted backend).

Everything else is **ordinary**: portability, argument handling, queue and
status output, logging and evidence, regression tests, docs, the skill,
backend compatibility that keeps every invariant. **When in doubt, it is
safety-semantic.**

## Tests

1. Reproduce the defect, and add a **regression test that fails first**: it
   must fail on the code without your fix and pass with it. State in the
   pull request how you saw it fail.
2. A claimed execution path needs a test that runs it. A change is not
   done while a path it claims is untested.
3. Work on a branch, never directly on `main`, and open a pull request from
   the template.

### The gate a contributor can run

The standard profile is the gate profile. It needs this repository,
Python 3.11 or later with `pytest`, and `pueue`/`pueued` (Pueue 4) on
`PATH`. It uses no private path, no operator file and no external guard.

```bash
python -m pytest -v -rs
```

- Docker is optional. The container tests use the public image pinned by
  digest (`conftest.PUBLIC_IMAGE`), pulled only with `ACS_TEST_PULL=1`;
  `ACS_TEST_IMAGE=<image>` overrides it. Without Docker or without the
  image those tests skip with a counted `[docker]` reason. Such a run is
  **not** the gate of a change that touches container code: run it with
  Docker, or say in the pull request that you could not.
- **Account for every skip.** Save the output and run
  `python3 tools/skip_report.py --summary-line <file>`. The summary must say
  `consistent` and `untagged 0`; the pull request states each category's
  count (`docker`, `platform`, `legacy`, `pueue`, `host`, `clone`, `root`).
  An `untagged` or `INCONSISTENT` result is unresolved evidence.
- `python3 tools/skill_check.py` must pass when you change the skill or a
  command, option or outcome it names.
- The `legacy` profile (`ACS_TEST_LEGACY_GUARD`) compares against an
  external guard and exists only where such a file exists; a contributor
  does not need it.

Hard blockers, whatever the size of the change: unresolved evidence
(including an unaccounted skip); an execution path that is claimed but
untested; any weakened fail-closed invariant.

## Review and release gates

Every pull request is reviewed publicly by the maintainer. An **ordinary**
change is reviewed against the diff and the standard-profile run in the
pull request. A change that touches only documentation (files under
`docs/`, Markdown files, `LICENSE`, `NOTICE`) is reviewed against the diff
and the documentation checks (`tools/skill_check.py`,
`tests/test_repository_names.py`, `tests/test_skill_check.py`,
`tests/test_status_view_contract.py`), which CI also runs as part of the
standard profile.

A **safety-semantic** change additionally needs an independent review
against the invariants above (by a reviewer who did not write it). After it
is reviewed and integrated, the maintainer runs the full release gates on
the maintainer's own hosts. Those gates are never run as untrusted
execution of a contribution: nothing from a pull request is executed on the
maintainer's hosts before it has been reviewed. How an accepted change
becomes part of a release is described in
[`CONTRIBUTING.md`](CONTRIBUTING.md).

A contributor, human or agent, does not merge a safety-semantic change on
their own authority, and an agent does not merge its own pull request. The
maintainer decides what is merged and released.

## Safety levels

The installation safety levels live in `src/agent_compute/safety.py`,
shared by `install.py` and the installed CLI. The installer records
`SAFETY_LEVEL` in every installed version directory and refuses to activate
a version whose level is missing, below `MIN_ACTIVATABLE_LEVEL`, or below
the active version's.

**A change that fixes a false-safe decision** (a false clean, a wrongful
unblock, a fail-open admission) **must raise `SAFETY_LEVEL`, and
`MIN_ACTIVATABLE_LEVEL` with it**, so that no installation can be rolled
back onto the defect. Record the new level and its fix in the comment
beside the constant, and say so in the pull request. A change that adds a
level for an old version is safety-semantic.

## Operator actions

`agent-compute pin-backend` and `agent-compute config migrate` are actions
for the operator of a host, not for an agent and not for a contribution's
tests against a real installation. Tests and examples use throwaway
directories (`ACS_INSTALL_HOME`, a temporary state directory); they never
touch a real lock, queue, configuration or installed version.

## What a contribution must not do

- Add a force or bypass option, or make an agent-facing command skip a
  safety check.
- Put a credential, a token, a personal path or a private host name in code,
  tests, fixtures, docs, logs or examples.
- Replace or edit the engine (`src/agent_compute/guard/engine.py`) without
  discussing it first: it is pinned by its sha256 and a change needs new
  evidence from a full-suite run.
- Cite private identifiers in new code or comments (see
  [`docs/provenance.md`](docs/provenance.md)): describe the mechanism.
