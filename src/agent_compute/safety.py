"""Installation safety levels (the activation floor).  Read-only.

Shared by ``install.py`` (which records the level and refuses to activate a
version below the floor) and the installed CLI (``doctor`` reports, and
``submit`` refuses, when the installation is below the floor).

Every installed version directory records ``SAFETY_LEVEL`` of the tree it
was copied from in its ``SAFETY`` file (0 for a dirty tree).  The level is
monotonically increasing.  A PR that fixes a false-safe decision (a false
clean, a wrongful unblock, a fail-open admission) MUST raise SAFETY_LEVEL,
and MIN_ACTIVATABLE_LEVEL with it, because every earlier version then
contains that defect.  Changing this module is safety-semantic.

Limit: the floor binds only code that has this module.  An older checkout's
``install.py`` can still point ``current`` at a pre-floor version, and that
version's CLI has no backstop.  Operators deploy and roll back only from
current ``main``.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from .fsprobe import (is_dir_strict, is_symlink_strict, lexists_strict,
                      listdir_strict)

#   1 = container cleanup evidence fix (eb369eb, PR #4)
#   2 = host identity TZ/locale fix, fail-closed liveness (edfd726, PR #6)
#   3 = campaign S4 (acs-guard backend): the S3 internal-guard false-clean
#       fixes become live (verified group kill incl. the dash container gap,
#       host survivors after a kill or a normal end -> cleanup-unverified,
#       signals before the launch, the in-flight marker after guard death),
#       and the runner blocks on the guard's cleanup-unverified / unclean
#       `cleanup` even when its own check finds nothing.  An older version
#       cannot load an acs-guard config and would run guard.py's gaps
#   4 = campaign G slice G1 (boundary audit C1-C5, v0.1.2):
#       C1 a lost enqueue reply after the claim keeps the request binding
#       C2 equal Linux start ticks are alive only on the same boot id
#       C3 the guard's container cancel keeps D4: never a false clean
#       C4 an unreadable block/gate/binding/job state is never "absent"
#       C5 container signals only under the recorded docker daemon
#   5 = campaign G slice G2a (v0.2.0): the internal guard's nested
#       pass-through no longer admits on a job-marker variable alone (a
#       stale or forged marker ran heavy work without the lock beside the
#       one heavy job); it requires the enclosing claimed, unfinished ACS
#       job whose guard's in-flight marker carries that value.  Also new
#       at this level: dispatch refuses a container the project no longer
#       lists (C6) and a guard home or marker names changed since submission
#   6 = campaign G slice G3a (v0.2.0; register A39, found in review round
#       1): `--host X submit|run --request-id R` was forwarded without any
#       local check, so a request id this host had bound to a job that may
#       have run (or whose binding cannot be read) was accepted again on
#       another host: a duplicate admission of one request across hosts
#       (fail-open; the class of C1).  It is now refused unless that job
#       provably never started; a placement record binds its request id to
#       its host (unreadable: everywhere), and the local reservation checks
#       it under the state lock
#   7 = campaign PA slice K1 (register A25 widened, A49): `install.py
#       uninstall` read "no unfinished jobs" from an unreadable or
#       unparsable config, a relative / `$VAR` / non-string `state_dir`, an
#       unreadable state dir, jobs dir or `state.json`, and crashed on
#       non-object JSON, so it could remove a version directory a job still
#       used (fail-open).  It now refuses, naming the path, unless it has
#       positive evidence (the jobs were read, or the correctly resolved
#       state dir was never created).  A version below this level carries
#       the defect on the documented uninstall path
#   8 = campaign PA slice K2 (register A24): a job cancelled after the guard
#       began launching but before the runner captured its workload identity
#       (a container's in-container pid not yet read; a host workload
#       between the guard's launch and the first capture poll) had no
#       `workload` field, so `_request_reusable` freed its request id and a
#       duplicate run of one request was admitted (fail-open).  A cancel now
#       frees the id only with positive never-launched evidence (no launch
#       field in the state, no launched/executing/cleanup phase in the
#       history, no guard-record pid, start, exit or launch outcome, no
#       samples.csv, every record readable); anything unreadable keeps it.
#       A version below this level carries the defect on `submit`/`run` and
#       on the cross-host check
#   9 = campaign PA slice K3 (K1's review note N1, register A43, A44):
#       `install.py uninstall` looked for unfinished jobs in a config and a
#       state dir that the installed CLI did not use: it ignored
#       `AGENT_COMPUTE_CONFIG`, took the config from the flags of the
#       uninstall command instead of the one the installation's wrapper
#       makes its CLI read, and, under `ACS_INSTALL_HOME`, expanded `~`
#       against the sandbox home while the CLI expands it against `$HOME`.
#       It could find "no jobs" where the CLI's state had unfinished ones
#       and remove a version directory they use (fail-open).  Now the
#       installer records the config the wrapper binds (`bound_config` in
#       the install manifest; for an older manifest it is derived from the
#       owned wrapper; an unbound wrapper, as every one before K3, means
#       both the CLI's default config and the installation's own under its
#       install home, which are one file for the default installation, and
#       both are examined); uninstall reads those configs (and the
#       `state_dir` they name, `~` against `$HOME` as the CLI expands it),
#       and refuses, even with
#       `--force`, a `--config` or an `AGENT_COMPUTE_CONFIG` that names
#       another file.  It guarantees the installation's own CLI state is
#       examined; it cannot see jobs of a CLI run with an explicit
#       `--config`/`AGENT_COMPUTE_CONFIG` elsewhere.  The wrapper of a
#       non-default installation binds its own config, and `config init`
#       prints an absolute state dir of its install home and refuses one
#       that another installation uses.
#       LIMIT: `install.py` is not copied into a version directory, so the
#       activation floor protects the wrapper and `config init` half (they
#       ship in the version), NOT the uninstall half: an operator runs
#       `uninstall` from the current `main` checkout (the same rule as for
#       every deploy and rollback), and an older checkout's `install.py`
#       still carries the defect
#  10 = campaign PA slice K4 (register A47, A4): `pin-backend` accepted any
#       committed file whose first line named the host, the profile and the
#       engine sha256 and whose pytest summary showed 0 failed: a smoke run
#       of one file, a `-k`-filtered run or a hand-written header pinned an
#       engine that no full suite had passed (fail-open admission of the
#       backend).  The check now also requires the header token of
#       `tools/gate_run.py` and `full suite on <host>`, a full source sha,
#       `tracked changes: 0`, a `consistent`/`untagged 0` skip line, no
#       `deselected`, a collected count equal to the skip line's and the
#       summary's, no failure or error, no Docker skip under a WITH Docker
#       header, and `EXIT 0`.  A version below this level accepts such
#       evidence on the documented pin path.  LIMIT: the check is of form,
#       not of forgery (A4): it catches mistakes; the committed-and-unmodified
#       rule and the operator's recorded authorization remain the controls
#  11 = campaign PA slice K6 (register A49): since Python 3.14 (the Mac's
#       production interpreter) `Path.exists`/`is_dir`/`is_file`/`is_symlink`
#       answer False for any OSError, and `Path.glob`/`os.walk` skip an
#       unreadable directory on every version, so a path that could not be
#       examined read as an absent one in decisions: no unfinished job (the
#       jobs dir), no pending queue task, no placement record (a request id
#       bound on another host, A39), no cancel request, no samples.csv or
#       guard record (reconcile cleared a block as "the guard never began
#       launching", and the runner assumed no launch), no lock user, no
#       in-flight marker of the previous guard home or of a scanned guard
#       home, a /proc entry, an input file or subtree (hashed as missing), a
#       higher installed level, `current`, an install manifest (written over).
#       All now go through `agent_compute.fsprobe` (absence is ENOENT or
#       ENOTDIR only; anything else is refused, blocked or unverified).  A
#       version below this level contains the defect on 3.14
SAFETY_LEVEL = 11
# No version below this level may become active again, whatever is active.
MIN_ACTIVATABLE_LEVEL = 11
# The level of a dirty (uncommitted) tree: never activatable.
DIRTY_LEVEL = 0
# Levels of version dirs installed before the SAFETY file existed.  Used
# only when a dir has no SAFETY file (installed dirs are never edited); the
# name must still match the dir's tree.  An unlevelled name not listed here
# counts as 0.
LEGACY_LEVELS = {"0.1.0-edfd7268fe": 2, "0.1.0-eb369eb937": 1}
VERSION_PARTS = ("bin", "src", "skill")
ROLL_FORWARD = ("roll forward instead: install a newer clean commit of "
                "main (there is no force option)")

_LEVEL = re.compile(r"[0-9]+")


def safety_level(version_dir: Path) -> int | None:
    """The level recorded in ``version_dir``; without a SAFETY file, the
    LEGACY_LEVELS entry for its name; None if neither, or unparsable (only
    ASCII digits are a level)."""
    safety = Path(version_dir) / "SAFETY"
    try:
        missing = not lexists_strict(safety)
    except OSError:
        return None             # A49: unexaminable is not "predates SAFETY"
    if missing:
        return LEGACY_LEVELS.get(Path(version_dir).name)
    try:
        text = safety.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError):
        return None
    return int(text) if _LEVEL.fullmatch(text) else None


def level_problem(version_dir: Path) -> str | None:
    """Why the level recorded in ``version_dir`` is unknown although the
    version has (or may have) a SAFETY file: one that cannot be examined,
    read or parsed.  None when the level is known, or when there is no
    SAFETY file at all (a version that predates the floor: its legacy
    level, or none, is a known fact).  A rollback or floor check must not
    read such a version as level 0 (K6 review, A49)."""
    safety = Path(version_dir) / "SAFETY"
    try:
        if not lexists_strict(safety):
            return None
    except OSError as error:
        return (f"the level of {Path(version_dir).name} cannot be examined "
                f"({error.strerror or error})")
    if safety_level(version_dir) is None:
        return (f"the level of {Path(version_dir).name} cannot be read "
                f"from {safety}")
    return None


def installed_versions(prefix: Path) -> list[Path]:
    """The installed version directories; ``[]`` only when there is no
    ``versions`` directory.  One that cannot be examined or listed raises
    ``OSError`` (``floor_problems`` reports it; A49)."""
    versions = Path(prefix) / "versions"
    return [versions / name for name in listdir_strict(versions)
            if not name.startswith(".")
            and is_dir_strict(versions / name)
            and not is_symlink_strict(versions / name)]


def floor_problems(prefix: Path) -> list[str]:
    """Why the installation under ``prefix`` is below the floor (empty: it
    is not).  ``current`` must be at or above MIN_ACTIVATABLE_LEVEL and at
    or above the highest level among the installed version dirs."""
    current = Path(prefix) / "current"
    if not current.is_symlink():
        return [f"{current} is not a link to an installed version"]
    active = Path(os.path.realpath(current))
    level = safety_level(active) or 0
    problems = []
    if level < MIN_ACTIVATABLE_LEVEL:
        problems.append(f"the active version {active.name} has safety level "
                        f"{level}, below the floor {MIN_ACTIVATABLE_LEVEL}")
    try:
        installed = installed_versions(prefix)
    except OSError as error:
        problems.append(f"the installed versions under {prefix}/versions "
                        f"cannot be listed ({error.strerror or error}): "
                        "the highest installed level is unknown")
        installed = []
    for version in installed:
        problem = level_problem(version)
        if problem:
            problems.append(f"{problem}: the highest installed level is "
                            "unknown")
    highest = max([safety_level(d) or 0 for d in installed], default=0)
    if level < highest:
        problems.append(f"the active version {active.name} has safety level "
                        f"{level}, below the highest installed level "
                        f"{highest}")
    if problems:
        problems.append(ROLL_FORWARD)
    return problems


def installation(module_file: Path | str | None = None
                 ) -> tuple[Path, Path] | None:
    """(prefix, version dir) of the installed copy this code runs from, or
    None when it runs from a checkout (``<prefix>/versions/<v>/src/
    agent_compute/<module>.py``)."""
    path = Path(os.path.realpath(module_file or __file__))
    parents = path.parents
    if len(parents) < 5 or parents[1].name != "src" or \
            parents[3].name != "versions":
        return None
    return parents[4], parents[2]


def running_floor_problems() -> list[str] | None:
    """Floor problems of the installation this code runs from; None when it
    is not an installed copy (a development checkout, the tests)."""
    found = installation()
    if found is None:
        return None
    prefix, running = found
    problems = floor_problems(prefix)
    level = safety_level(running) or 0
    if level < MIN_ACTIVATABLE_LEVEL and not any(
            running.name in p for p in problems):
        problems[:0] = [f"the running version {running.name} has safety "
                        f"level {level}, below the floor "
                        f"{MIN_ACTIVATABLE_LEVEL}"]
        if ROLL_FORWARD not in problems:
            problems.append(ROLL_FORWARD)
    return problems
