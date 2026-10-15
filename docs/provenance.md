# Provenance

This document says where parts of this repository come from and how to read
some references inside the source. It is a plain statement of origin; it is
not a license (see `LICENSE`) and not legal advice.

## The resource guard

The resource guard (`src/agent_compute/guard/engine.py`, backend
`acs-guard`) was extracted from an earlier private research tool by the
same author, ported faithfully and then parameterized and generalized in
this repository. Its tests (`tests/test_internal_guard.py`) were ported from
that tool's guard tests, and `tests/memory_hog.py` (a synthetic memory
allocator used by those tests) was copied from it unchanged. The earlier
tool was never published and had no license of its own; the author of that
tool and of this repository is the same.

The engine's module docstring ends with a pointer to two campaign records
(`docs/campaign-extraction.md` and `docs/campaign-generalization.md`). Those
two files belong to the private development record and are not part of this
repository, so the paths do not resolve here. The pointer is kept as it is,
because the engine file is pinned by its content hash (`backend.sha256` in
the configuration, checked by the pin and at every dispatch) and is not
edited for wording.

## References in source comments and docs

Comments and test docstrings in the source, and parts of
`docs/architecture.md`, cite `PR #n`, register ids of the forms `A<n>`,
`C<n>` and `F<n>`, and "campaign" and "slice" names. They refer to the
**private development record** of this project, in which the code was
developed, gated and used on the maintainer's own hosts since 2026-09,
before this repository existed. They are **not** links to this
repository's pull requests or issues: `PR #12` in a comment is not pull
request 12 here. Rewriting them would have meant editing the most
safety-sensitive files for no change in behaviour, so they stay as they
are, and the history of this repository does not contain the record they
point to. New code should avoid such references and describe the mechanism
instead (see `CONTRIBUTING.md`).

## Legacy names

Some identifiers carry the name of the earlier tool: the environment
variables `SMC_GUARD_*` (and `SMC_DENSE_BLAS_THREADS`, `SMC_SLICE`), the
guard home `~/.smc-guard`, the backend name `smc-guard`, and the record
and table schemas `smc-guard-record/1` and `smc-perf-baseline/1`. They are
compatibility identifiers: the guard still reads them so that existing
guard homes, records and workloads keep working, and some tests exercise
them. They do not mean that the earlier tool is a dependency. The
`smc-guard` backend itself was retired in version 0.2.0; a configuration
that names it does not load.

## How the code was written

Most of the code, tests and documentation in this repository were written
with AI coding assistants, under the direction and review of the author.

## External software

This repository vendors no third-party source code. The tool itself uses
only the Python standard library (Python 3.11 or later). It needs Pueue 4
as an external program for its queue, and the container tests use the
public image `python:3.11.6-slim-bookworm`, pulled on request and not
redistributed. `pytest` is the only test-time Python dependency. Each of
these is under its own license.
