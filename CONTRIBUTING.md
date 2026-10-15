# Contributing

Thank you for looking at `agent-compute-scheduler`. This is an **alpha**
tool that decides when heavy jobs may start and kills them, so changes are
reviewed carefully and some take longer than others. Read
[`AGENTS.md`](AGENTS.md) first: it is the contribution policy (the
invariants, the classification rule and the tests) for people and for
coding agents alike. What the tool is, and its limits, are in
[`README.md`](README.md) and [`docs/limitations.md`](docs/limitations.md).

## Propose a change

1. **Open an issue first** for anything that changes behaviour: a new
   option, a new outcome, a different refusal or block, a change to a
   documented rule. Describe the problem, not only the patch. A typo, a doc
   fix or a test that only adds coverage can go straight to a pull request.
2. **A defect that could make ACS start something it should not, miss a
   block, signal the wrong process or leak a credential is a security
   report**: follow [`SECURITY.md`](SECURITY.md) instead of opening a public
   issue.
3. **Write a regression test that fails first.** Reproduce the defect, add
   the test, see it fail on the current code, then fix. The pull request
   says how you saw it fail.
4. **Branch and open a pull request** from the template
   (`.github/pull_request_template.md`). Never push to `main`.
5. **Classify the change** with the mechanical rule in
   [`AGENTS.md`](AGENTS.md) (safety-semantic or ordinary, from the files
   and functions touched; when in doubt, safety-semantic) and say whether it
   fixes a false-safe decision, which raises the safety level
   (`src/agent_compute/safety.py`).
6. **Run the standard profile** and attach its summary:

   ```bash
   python3 -m venv .venv && .venv/bin/pip install pytest
   .venv/bin/python -m pytest -v -rs 2>&1 | tee pytest.out
   python3 tools/skip_report.py --summary-line pytest.out
   python3 tools/skill_check.py
   ```

   You need Python 3.11 or later, `ps` (procps) and Pueue 4 on `PATH`. Docker is optional,
   but a change that touches container code is not validated without it
   (`ACS_TEST_PULL=1` pulls the public test image). The skip summary must
   say `consistent` and `untagged 0`.

Keep a pull request to one topic. Describe what changed and why; do not
include credentials, personal paths or host names in code, tests, logs or
the description.

## What CI runs

Continuous integration (`.github/workflows/ci.yml`) runs on GitHub-hosted
`ubuntu-24.04` runners for Python 3.11, 3.12 and 3.14, on pull requests and on
pushes to `main`, with a read-only token and no secrets. It runs
`tools/ci.sh`, which you can run yourself from a clone: Pueue 4.0.4 by
pinned checksum, pytest at pinned versions, the standard profile with Docker
and the public pinned test image, the skip accounting (`consistent` and
`untagged 0`) and the skill check. CI does not cover the macOS Docker Desktop
path or a real two-host installation; the maintainer validates those in the
release gates below. No pull request is ever run on the maintainer's machines
before it has been reviewed.

## How accepted changes are integrated

The maintainer develops this tool in a private repository, where it is
exercised on the maintainer's own hosts, and exports each release to this
repository as an ordinary commit on top of the public history. A public
contribution therefore takes this path:

1. It is **reviewed publicly**, on its pull request. A safety-semantic
   change also gets an independent review against the invariants.
2. When accepted, it is **integrated into the private development
   repository** with its author preserved, or with a `Co-authored-by`
   trailer when the maintainer edited it, and **gated there**: the full
   suite on the maintainer's hosts, with Docker where containers are
   touched (a documentation-only change is gated by the documentation
   checks instead). For a safety-semantic change this happens only after review and
   integration, never as untrusted execution of the contribution.
3. It **appears in the next release export** on top of the public history.
   The export never rewrites or overwrites the public history, and it never
   discards a contributor's commits. If a pull request is merged here
   first, the next export records it as integrated (with the private commit
   that carries it) and builds on it. The exporter verifies each such
   integration by patch identity and refuses an export that would replace
   what a contribution wrote, unless the maintainer accepts that knowingly,
   path by path.

So a merged pull request may be followed, in the next release, by a
maintainer commit that carries the gated form of the same change. A
release is deployed on the maintainer's hosts through a separate, recorded
procedure; merging here deploys nothing anywhere.

## Style and references

- Python 3.11 or later, standard library only for the tool itself
  (`pytest` is the one test-time dependency).
- Keep the safety comments that explain why a check exists. Describe the
  mechanism in new comments and tests.
- **Do not cite private identifiers in new code.** Older source comments
  refer to `PR #n`, register ids (`A<n>`, `C<n>`, `F<n>`) and campaign or
  slice names of the maintainer's private development record; they are not
  links to this repository ([`docs/provenance.md`](docs/provenance.md)).
  New code and tests should state the mechanism instead.
- Identifiers such as `SMC_GUARD_*` and `~/.smc-guard` are legacy
  compatibility names; keep them working and do not rename them in passing.

## Licensing of contributions

This repository is under the MIT License (see `LICENSE`). Contributions are
accepted under the same license (inbound = outbound): by submitting a
contribution you state that you have the right to submit it under the MIT
License.
