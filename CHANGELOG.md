# Changelog

All notable changes to this project are listed here, newest first. The
project is in **alpha**: under version 0.x, anything may change between
releases. Versions follow semantic versioning in spirit; a release is a tag
on this repository.

## 0.3.0 (alpha, 2026-10-15)

This is the first public release of `agent-compute-scheduler`, a scheduler
that was developed and used privately since 2026-09 on the maintainer's own
macOS and Linux (WSL2) hosts, where agents of several projects shared one
heavy slot per host. Earlier version numbers (0.1.x, 0.2.x) belong to that
private development and were never published; this repository starts its
history at 0.3.0. Read [`README.md`](README.md) for what the tool is and
[`docs/limitations.md`](docs/limitations.md) for what it does not do.

Changes since the last privately deployed version (0.2.1):

**Safety fixes** (each raises the installation safety level, so no host can
be rolled back onto the defect; the level is now **11**, and `install.py
activate` refuses any version below it):

- **Python 3.14 filesystem checks.** Since Python 3.14, `pathlib`'s
  `exists()`, `is_dir()`, `is_file()` and `is_symlink()` answer `False` when
  a path cannot be examined (for example a permission error) instead of
  raising. Every decision-relevant check outside the guard engine now goes
  through strict probes (`src/agent_compute/fsprobe.py`): only "no such file"
  counts as absent; any other error refuses, blocks, or is reported, on every
  supported Python version. The runner refuses to dispatch a job whose
  baseline table cannot be examined. An installed version whose safety level
  cannot be read is never treated as level 0. A test guards against new raw
  predicates in decision code.
- **Gate evidence and the engine pin.** New `tools/gate_run.py` runs the
  full standard suite and writes the gate-evidence file; `pin-backend`
  accepts only such a full, passing, unedited run (a filtered or smoke run,
  a run during which the checkout changed, a run without Pueue, or
  concatenated or hand-edited output is refused). Sensitive-named
  environment values are redacted from the evidence.
- **Installation isolation.** `install.py uninstall` acts on the
  configuration and state directory its installation uses (as the installed
  CLI resolves them) and refuses when `AGENT_COMPUTE_CONFIG` names another
  configuration; scratch installations no longer reach the operator's.
- **Request ids after a cancel.** A cancelled job's request id is reusable
  only on positive evidence that the job never launched (host and container
  venues).
- **Uninstall with unreadable state.** `install.py uninstall` refuses unless
  it has positive evidence that no job is unfinished; an unreadable
  configuration, state directory or job record is a refusal, not "no jobs".

**Release and development:**

- The standard test profile runs from a clean clone with synthetic fixtures
  only (Python 3.11 or later, pytest, Pueue; Docker optional, with the
  public test image pinned by digest); every skip is counted by category.
- `tools/ci.sh` and a GitHub Actions workflow (Python 3.11, 3.12 and 3.14;
  Pueue 4.0.4 by pinned checksum; read-only token, no secrets).
- Public documentation: README quickstart, limitations (what is tested,
  deployed and exercised in real use), provenance, contributing and
  security policies. The skill and the docs name no repository.
- License: MIT.

The guard engine is unchanged from 0.2.1 (same sha256), so an existing
engine pin stays valid.
