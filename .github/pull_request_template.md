<!-- Read AGENTS.md and CONTRIBUTING.md first. Every field is required; write "n/a" with a reason. -->

## Defect and fix

- Defect (with the reproduction), or the issue this addresses:
- Fix:
- Related issue(s):

## Change class

- [ ] **ordinary**
- [ ] **safety-semantic**

Mechanical rule (`AGENTS.md`): the change is safety-semantic if the diff
touches `src/agent_compute/procs.py`, `runner.py`, `queue.py`, `state.py`,
`config.py`, `safety.py`; anything in `src/agent_compute/guard/` or `bin/`;
any of `install.py`; the reconcile / cancel / `detect_lost` / terminate /
resume / daemon / wait / `pin-backend` code or the safety-floor checks in
`cli.py`. **When in doubt, it is safety-semantic.**

Does this PR fix a false-safe decision (false clean, wrongful unblock,
fail-open admission)? If yes, `SAFETY_LEVEL` and `MIN_ACTIVATABLE_LEVEL`
in `src/agent_compute/safety.py` are raised to: ___

## Files touched

<!-- list every file; mark the ones that make it safety-semantic -->

## Fail-first test

- Test(s):
- How you saw it fail before the fix (command and the failing line):

## The standard-profile run you did

- Host and platform (OS, architecture, Python, Pueue versions):
- Docker: with (image) / without. A change that touches container code
  needs the run with Docker.
- Command: `python -m pytest -v -rs`
- Result (passed / failed / skipped):
- Skip summary, from `python3 tools/skip_report.py --summary-line <output file>`
  (it must say `consistent` and `untagged 0`; give the count of each
  category: `docker`, `platform`, `legacy`, `pueue`, `host`, `clone`, `root`):
- `python3 tools/skill_check.py`, if the skill or a command it names changed:

## Checks

- [ ] No credential, personal path or private host name in the diff, the
      tests or this description.
- [ ] New code and comments describe the mechanism and cite no private
      identifiers (`CONTRIBUTING.md`).
- [ ] No invariant of `AGENTS.md` is weakened, or this description says
      which one changes and why.

## Release gates

The maintainer reviews every pull request. A safety-semantic change also
gets an independent review, and after it is integrated the maintainer runs
the full release gates on the maintainer's own hosts; nothing in a pull
request is executed there before review. You do not need to run those.
