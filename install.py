#!/usr/bin/env python3
"""Install, upgrade, roll back or uninstall agent-compute for this user.

    python3 install.py install  [--config-from examples/config.example.toml]
    python3 install.py status
    python3 install.py activate <version>      # roll forward (floor-checked)
    python3 install.py uninstall [--keep-config]

What it owns (recorded in <prefix>/install-manifest.json, and nothing else):

* <prefix>/versions/<version>/   a controlled, read-only copy of bin/, src/
                                 and skill/ from a clean commit
* <prefix>/current               symlink to the active version
* <bin>/agent-compute            a short wrapper (no shell-profile edits); for
                                 a non-default config it also binds this
                                 installation's config as the CLI's default
* ~/.claude/skills/agent-compute-scheduler   symlink -> current skill
* ~/.agents/skills/agent-compute-scheduler   symlink -> current skill
* the machine config, only if this installer created it

Editing the repository never changes the active installation: queued jobs
run the version directory they were submitted with (their *code*; the
runner reads the live machine config).  Existing files it does not own are
never overwritten; a name collision aborts the install.

Safety floor (src/agent_compute/safety.py): each version directory records
SAFETY_LEVEL in its ``SAFETY`` file (0 for a dirty tree; dirs installed
before that: LEGACY_LEVELS, by name).  ``activate`` (and ``install``)
never point ``current`` at a version whose level is missing, below
MIN_ACTIVATABLE_LEVEL, or below the level of the active version.  There is no force option.  The floor binds
only this installer and later ones: deploy and roll back only from current
``main``.  The installed CLI backstops it (``doctor`` error, ``submit``
refusal when ``current`` is below the floor).
"""

from __future__ import annotations

import argparse
import errno
import json
import os
import re
import shlex
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent
SKILL = "agent-compute-scheduler"
MANIFEST_SCHEMA = "agent-compute-install/1"

# The safety levels and their parsing live in the package, so the installed
# CLI can check its own installation (``doctor``/``submit``) with the same
# rules.  See src/agent_compute/safety.py.
sys.path.insert(0, str(REPO / "src"))
from agent_compute.config import (DEFAULT_CONFIG, ENV_CONFIG,  # noqa: E402
                                  ENV_INSTALL_HOME, expand)
from agent_compute.fsprobe import (exists_strict, is_file_strict,  # noqa: E402
                                   is_symlink_strict, lexists_strict)
from agent_compute.safety import (DIRTY_LEVEL, LEGACY_LEVELS,  # noqa: E402
                                  MIN_ACTIVATABLE_LEVEL, ROLL_FORWARD,
                                  SAFETY_LEVEL, VERSION_PARTS, level_problem,
                                  safety_level)
from agent_compute.state import OUTCOMES  # noqa: E402  (terminal outcomes)


def home() -> Path:
    return Path(os.environ.get(ENV_INSTALL_HOME, Path.home()))


def paths(args) -> dict:
    h = home()
    # absolute and ``~``/``$VAR``-expanded as the CLI expands ``--config``
    config = (expand(args.config).absolute() if args.config
              else h / ".config/agent-compute/config.toml")
    return {
        "prefix": Path(args.prefix or h / ".local/share/agent-compute"),
        "bin": Path(args.bin or h / ".local/bin"),
        "skill_targets": [h / ".claude/skills", h / ".agents/skills"],
        "config": config,
    }


def same_file(first, second) -> bool:
    """The same path once links are resolved (no error: a path that does
    not exist resolves to itself)."""
    return os.path.realpath(first) == os.path.realpath(second)


def cli_default_config() -> Path:
    """The config the installed CLI reads when neither ``--config`` nor
    ``AGENT_COMPUTE_CONFIG`` is given (``config.load``)."""
    return expand(DEFAULT_CONFIG).absolute()


def git(*args: str) -> str:
    return subprocess.run(["git", "-C", str(REPO), *args], capture_output=True,
                          text=True, check=True).stdout.strip()


def version_name(allow_dirty: bool) -> str:
    sys.path.insert(0, str(REPO / "src"))
    from agent_compute import __version__
    sha = git("rev-parse", "--short=10", "HEAD")
    # install.py too: a locally edited installer could stamp any level
    dirty = bool(git("status", "--porcelain", "--", "bin", "src", "skill",
                     "install.py"))
    if dirty and not allow_dirty:
        raise SystemExit("install: bin/, src/, skill/ or install.py has "
                         "uncommitted "
                         "changes; commit first (or --allow-dirty for a "
                         "throwaway test install)")
    return f"{__version__}-{sha}" + ("-dirty" if dirty else "")


def load_manifest(prefix: Path) -> dict:
    path = prefix / "install-manifest.json"
    try:
        present = is_file_strict(path)
    except OSError as error:
        # A49: not a fresh manifest, which save_manifest would write over
        # the record of what this installer owns
        raise SystemExit(f"install: {path} cannot be examined "
                         f"({error.strerror or error}); nothing was "
                         "changed") from None
    if present:
        return json.loads(path.read_text())
    return {"schema": MANIFEST_SCHEMA, "versions": [], "links": [],
            "files": [], "created_config": None, "active": None}


def save_manifest(prefix: Path, manifest: dict) -> None:
    prefix.mkdir(parents=True, exist_ok=True)
    temp = prefix / ".install-manifest.json.tmp"
    temp.write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n")
    os.replace(temp, prefix / "install-manifest.json")


def owned_link(path: Path, prefix: Path) -> bool:
    return path.is_symlink() and str(os.readlink(path)).startswith(str(prefix))


def make_readonly(root: Path) -> None:
    for dirpath, dirnames, filenames in os.walk(root):
        for name in filenames:
            full = Path(dirpath) / name
            mode = 0o555 if os.access(full, os.X_OK) else 0o444
            full.chmod(mode)
        Path(dirpath).chmod(0o555)


def make_writable(root: Path) -> None:
    for dirpath, dirnames, filenames in os.walk(root):
        Path(dirpath).chmod(0o755)
        for name in filenames:
            (Path(dirpath) / name).chmod(0o644)


def atomic_symlink(target: Path, link: Path) -> None:
    temp = link.with_name(f".{link.name}.tmp-{os.getpid()}")
    if temp.is_symlink() or temp.exists():
        temp.unlink()
    temp.symlink_to(target)
    os.replace(temp, link)


def wrapper_text(prefix: Path, python: str, config: Path | None = None
                 ) -> str:
    """The two-line wrapper.  For the default installation (``config`` is
    the file the CLI reads by default, or None) it is exactly what it has
    always been.  An installation whose config is elsewhere (a throwaway
    ``ACS_INSTALL_HOME``, ``--config``) exports that config as the default
    of its CLI (K3, A43): ``--config`` wins over the caller's
    ``AGENT_COMPUTE_CONFIG`` (the CLI's own order), which wins over this
    one; an empty variable counts as unset, as the CLI treats it."""
    head = ("#!/bin/sh\n# agent-compute wrapper (owned by agent-compute-"
            "scheduler/install.py)\n")
    bind = ""
    if config is not None and not same_file(config, cli_default_config()):
        bind = ("# this installation's config (a caller's AGENT_COMPUTE_CONFIG "
                "or --config wins)\n"
                f'if [ -z "${{{ENV_CONFIG}:-}}" ]; then {ENV_CONFIG}='
                f"{shlex.quote(str(config))}; export {ENV_CONFIG}; fi\n")
    return head + bind + \
        f'exec "{python}" "{prefix}/current/bin/agent-compute" "$@"\n'


def check_collisions(p: dict, manifest: dict, python: str) -> list[str]:
    problems = []
    wrapper = p["bin"] / "agent-compute"
    try:
        there = lexists_strict(wrapper)
    except OSError as error:            # A49: not "nothing there"
        problems.append(f"{wrapper} cannot be examined "
                        f"({error.strerror or error})")
        there = False
    if there:
        if str(wrapper) not in manifest["files"]:
            problems.append(f"{wrapper} exists and is not owned by this "
                            "installer")
    for base in p["skill_targets"]:
        link = base / SKILL
        try:
            there = lexists_strict(link)
        except OSError as error:
            problems.append(f"{link} cannot be examined "
                            f"({error.strerror or error})")
            continue
        if there and not owned_link(link, p["prefix"]):
            problems.append(f"{link} exists and is not owned by this installer")
    return problems


def missing_dirs(path: Path) -> list[str]:
    """``path`` and its ancestors that do not exist yet (deepest first)."""
    created = []
    while not exists_strict(path):      # A49: raises when it cannot tell
        created.append(str(path))
        path = path.parent
    return created


def cmd_install(args) -> int:
    p = paths(args)
    prefix = p["prefix"]
    manifest = load_manifest(prefix)
    new_dirs = missing_dirs(prefix / "versions") + missing_dirs(p["bin"])
    if args.config_from:
        new_dirs += missing_dirs(p["config"].parent)
    python = args.python or sys.executable
    problems = check_collisions(p, manifest, python)
    if problems:
        print("install: refusing (name collision):\n  " +
              "\n  ".join(problems), file=sys.stderr)
        return 1
    version = version_name(args.allow_dirty)
    target = prefix / "versions" / version
    if not target.exists():
        staging = prefix / "versions" / f".{version}.staging-{os.getpid()}"
        if staging.exists():
            make_writable(staging)
            shutil.rmtree(staging)
        staging.mkdir(parents=True)
        for part in ("bin", "src", "skill"):
            shutil.copytree(REPO / part, staging / part,
                            ignore=shutil.ignore_patterns("__pycache__",
                                                          "*.pyc"))
        (staging / "VERSION").write_text(version + "\n")
        # the full source sha (D10: `agent-compute --version` and every
        # job record name it; VERSION carries only its first 10 digits)
        (staging / "SOURCE").write_text(git("rev-parse", "HEAD") + "\n")
        level = DIRTY_LEVEL if version.endswith("-dirty") else SAFETY_LEVEL
        (staging / "SAFETY").write_text(f"{level}\n")
        make_readonly(staging)
        os.replace(staging, target)
    if str(target) not in manifest["versions"]:
        manifest["versions"].append(str(target))
    problems = activation_problems(prefix, version)
    if problems:
        save_manifest(prefix, manifest)      # the new dir stays owned
        print(f"install: refusing to activate {version}:\n  " +
              "\n  ".join(problems), file=sys.stderr)
        return 1
    atomic_symlink(target, prefix / "current")
    manifest["active"] = version
    # the config this installation's wrapper makes its CLI read by default
    # (K3 round 2): uninstall looks for unfinished jobs in that one
    manifest["bound_config"] = os.path.realpath(p["config"])
    p["bin"].mkdir(parents=True, exist_ok=True)
    wrapper = p["bin"] / "agent-compute"
    text = wrapper_text(prefix, python, p["config"])
    if not wrapper.exists() or wrapper.read_text() != text:
        temp = wrapper.with_name(".agent-compute.tmp")
        temp.write_text(text)
        temp.chmod(0o755)
        os.replace(temp, wrapper)
    if str(wrapper) not in manifest["files"]:
        manifest["files"].append(str(wrapper))
    for base in p["skill_targets"]:
        if not base.is_dir():
            print(f"install: {base} does not exist; skipping that client "
                  "(create it to enable the skill there)")
            continue
        link = base / SKILL
        atomic_symlink(prefix / "current" / "skill" / SKILL, link)
        if str(link) not in manifest["links"]:
            manifest["links"].append(str(link))
    if args.config_from and not p["config"].exists():
        p["config"].parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        shutil.copyfile(args.config_from, p["config"])
        p["config"].chmod(0o600)
        manifest["created_config"] = str(p["config"])
        print(f"install: wrote {p['config']} from {args.config_from}; "
              "review it, then run `agent-compute doctor`")
    elif args.config_from:
        print(f"install: kept the existing {p['config']} (never overwritten)")
    manifest.setdefault("created_dirs", [])
    manifest["created_dirs"] += [d for d in new_dirs
                                 if d not in manifest["created_dirs"]]
    before = load_manifest(prefix)
    if {k: v for k, v in manifest.items() if k != "updated"} != \
            {k: v for k, v in before.items() if k != "updated"}:
        manifest["updated"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        save_manifest(prefix, manifest)
    print(json.dumps({"installed": version, "prefix": str(prefix),
                      "wrapper": str(wrapper), "skills": manifest["links"]},
                     indent=1))
    return 0


class Unanswerable(Exception):
    """The question "does an unfinished job reference this installation?"
    cannot be answered from the evidence at hand (K1; register A25, A49)."""

    def __init__(self, path, reason: str):
        super().__init__(f"{reason}: {path}")
        self.path = path
        self.reason = reason


# Absence is ENOENT or ENOTDIR and nothing else: every other error (EACCES,
# EIO, ELOOP, ...) leaves the question open.  Path.exists/is_dir/is_file are
# not used for this: since Python 3.14 they turn a PermissionError into
# "absent" (A49), and Path.glob skips an unreadable directory on every
# version.
_ABSENT = (errno.ENOENT, errno.ENOTDIR)
DEFAULT_STATE_DIR = "~/.local/state/agent-compute"      # config.DEFAULT_STATE_DIR
_UNEXPANDED = re.compile(r"\$(?:\w|\{)")


def _absent(path) -> None:
    """``path`` gave ENOENT/ENOTDIR.  Walk up to its first existing
    component; a symlink on the way whose target is absent (a dangling link
    in the middle of the path, or the path itself) is not absence but an
    unanswered question: ``Unanswerable`` naming the link.  Limit: a plain
    mount point of an unmounted volume, with no link in the path, is
    indistinguishable from a path that was never created (docs/limitations.md).
    """
    here = Path(path)
    for part in (here, *here.parents):
        try:
            found = os.lstat(part)
        except OSError as error:
            if error.errno in _ABSENT:
                continue
            raise Unanswerable(part, f"cannot be examined "
                                     f"({error.strerror})") from None
        if stat.S_ISLNK(found.st_mode):
            try:
                os.stat(part)
            except OSError:
                raise Unanswerable(part, "a link whose target is absent "
                                         f"(on the way to {path})") from None
        return


def _stat(path) -> os.stat_result | None:
    """``os.stat`` of ``path``; None when it is absent (ENOENT/ENOTDIR and
    no dangling link on the way, see ``_absent``); ``Unanswerable`` for any
    other error."""
    try:
        return os.stat(path)
    except OSError as error:
        if error.errno not in _ABSENT:
            raise Unanswerable(path, f"cannot be examined "
                                     f"({error.strerror})") from None
    except ValueError as error:
        raise Unanswerable(path, f"not a usable path ({error})") from None
    _absent(path)
    return None


def _read(path) -> bytes | None:
    """The bytes of ``path``; None when it is absent (``_absent``);
    ``Unanswerable`` for any other error."""
    try:
        with open(path, "rb") as handle:
            return handle.read()
    except OSError as error:
        if error.errno not in _ABSENT:
            raise Unanswerable(path, f"cannot be read "
                                     f"({error.strerror})") from None
    _absent(path)
    return None


def _parse(data: bytes, path, parser):
    try:
        return parser(data.decode("utf-8"))
    except (ValueError, RecursionError) as error:
        raise Unanswerable(path, f"cannot be parsed ({error})") from None


def resolve_state_dir(config: Path, raw: dict) -> Path:
    """The state dir the machine config names, expanded exactly as the CLI
    does (``config.expand``: ``~`` against ``$HOME``, never against
    ``ACS_INSTALL_HOME``, then ``$VAR`` from the environment; K3, K1's note
    N1); ``Unanswerable`` when the result is not an absolute path or still
    holds a variable."""
    value = raw.get("state_dir", DEFAULT_STATE_DIR)
    if not isinstance(value, str):
        raise Unanswerable(config, f"state_dir must be a string, got "
                                   f"{value!r}")
    text = os.path.expanduser(value)
    if text.startswith("~"):
        raise Unanswerable(config, f"state_dir {value!r} names a home "
                                   f"that cannot be resolved")
    text = os.path.expandvars(text)
    if _UNEXPANDED.search(text):
        raise Unanswerable(config, f"state_dir {value!r} holds an "
                                   f"environment variable that is not set")
    if "\0" in text or not os.path.isabs(text):
        raise Unanswerable(config, f"state_dir {value!r} is not an absolute "
                                   f"path")
    return Path(text)


def active_jobs(config: Path) -> list[str]:
    """The unfinished jobs of the state dir the machine config names.

    Returns a list only on positive evidence: the jobs were listed and read
    (an empty list), or the correctly resolved state dir (or its ``jobs``)
    does not exist, so no job was ever accepted.  Raises ``Unanswerable``,
    naming the path, whenever that cannot be established.  A job directory
    without ``state.json``, or whose ``outcome`` is not one of the terminal
    ``state.OUTCOMES``, counts as unfinished (it is being accepted, or its
    record is lost)."""
    import tomllib
    data = _read(config)
    if data is None:
        state = resolve_state_dir(config, {})
        if _stat(state) is None:
            return []                       # no config, no state: never ran
        raise Unanswerable(config, f"missing, while the default state dir "
                                   f"{state} exists (its jobs cannot be "
                                   f"attributed or ruled out)")
    raw = _parse(data, config, tomllib.loads)
    if not isinstance(raw, dict):
        raise Unanswerable(config, "is not a TOML table")
    state = resolve_state_dir(config, raw)
    found = _stat(state)
    if found is None:
        return []
    if not stat.S_ISDIR(found.st_mode):
        raise Unanswerable(state, "the state dir is not a directory")
    jobs = state / "jobs"
    found = _stat(jobs)
    if found is None:
        return []
    if not stat.S_ISDIR(found.st_mode):
        raise Unanswerable(jobs, "the jobs path is not a directory")
    try:
        with os.scandir(jobs) as entries:
            names = sorted(entry.name for entry in entries)
    except OSError as error:
        raise Unanswerable(jobs, f"cannot be listed "
                                 f"({error.strerror})") from None
    active = []
    for name in names:
        job = jobs / name
        found = _stat(job)
        if found is None or not stat.S_ISDIR(found.st_mode):
            continue                        # gone since the listing / not a job
        state_file = job / "state.json"
        text = _read(state_file)
        if text is None:
            active.append(name)             # no record: unknown, not finished
            continue
        record = _parse(text, state_file, json.loads)
        if not isinstance(record, dict):
            raise Unanswerable(state_file, "is not a JSON object")
        if not (isinstance(record.get("outcome"), str)
                and record["outcome"] in OUTCOMES):
            active.append(name)             # only a terminal outcome ends a job
    return active


def package_version(version_dir: Path) -> str | None:
    import re
    try:
        text = (version_dir / "src/agent_compute/__init__.py").read_text()
    except OSError:
        return None
    match = re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', text, re.M)
    return match.group(1) if match else None


def activation_problems(prefix: Path, version: str) -> list[str]:
    """Why ``version`` must not become ``current`` (empty: it may)."""
    roll = ROLL_FORWARD
    if not version or "/" in version or version.startswith(".") or \
            version in ("current",):
        return [f"{version!r} is not a version name"]
    target = prefix / "versions" / version
    if target.is_symlink() or not target.is_dir():
        return [f"{target} is not an installed version directory"]
    problems = [f"{target}/{part} is missing" for part in VERSION_PARTS
                if not (target / part).is_dir() or
                (target / part).is_symlink()]
    try:
        recorded = (target / "VERSION").read_text().strip()
    except OSError:
        recorded = None
    if recorded != version:
        problems.append(f"the name {version} does not match its tree's "
                        f"VERSION ({recorded!r})")
    package = package_version(target)
    if package is None or not version.startswith(package + "-"):
        problems.append(f"the name {version} does not match its tree's "
                        f"package version ({package!r})")
    level = safety_level(target)
    current = prefix / "current"
    active_level = 0
    active = None
    try:
        linked = is_symlink_strict(current)
    except OSError as error:
        # A49: without it the active level reads 0 and any lower version
        # would pass the rollback check
        return problems + [f"{current} cannot be examined "
                           f"({error.strerror or error}); the active level "
                           f"is unknown; {roll}"]
    if linked:
        active_dir = Path(os.path.realpath(current))
        active = active_dir.name
        unknown = level_problem(active_dir)
        if unknown:
            # K6 review: an unreadable SAFETY of the active version must
            # not read as level 0 (any lower version would then pass)
            return problems + [f"{unknown}; the active level is unknown; "
                               f"{roll}"]
        active_level = safety_level(active_dir) or 0
    if level is None:
        problems.append(f"{version} has no recorded safety level and no "
                        "legacy level (it predates the activation floor); "
                        f"{roll}")
    elif level < MIN_ACTIVATABLE_LEVEL:
        problems.append(f"{version} has safety level {level}, below the "
                        f"floor {MIN_ACTIVATABLE_LEVEL}; {roll}")
    elif level < active_level:
        problems.append(f"{version} has safety level {level}, lower than the "
                        f"active {active} (level {active_level}); {roll}")
    return problems


def cmd_activate(args) -> int:
    p = paths(args)
    problems = activation_problems(p["prefix"], args.version)
    if problems:
        print(f"activate: refusing {args.version!r}:\n  " +
              "\n  ".join(problems), file=sys.stderr)
        return 1
    target = p["prefix"] / "versions" / args.version
    atomic_symlink(target, p["prefix"] / "current")
    manifest = load_manifest(p["prefix"])
    manifest["active"] = args.version
    save_manifest(p["prefix"], manifest)
    print(f"active: {args.version}")
    return 0


_BIND = re.compile(r'^if \[ -z "\$\{' + ENV_CONFIG + r':-\}" \]; then '
                   + ENV_CONFIG + r"=(.+); export " + ENV_CONFIG + r"; fi$",
                   re.M)


def bound_configs(manifest: dict) -> list[Path]:
    """The configs whose state this installation's jobs may be in: the one
    its CLI reads by default.  That is the one recorded at install time
    (``bound_config``); for a manifest written before that, the one the
    owned wrapper exports.  A wrapper that exports none (any wrapper before
    K3, or the default installation's now) or none left gives TWO: the CLI's
    default config and the installation's own under its install home (a
    trial of an older installer ran its jobs with an explicit ``--config``
    naming the latter, which that wrapper does not record); for the default
    installation they are one file.  ``Unanswerable`` when a wrapper cannot
    be read or understood, or the wrappers disagree."""
    recorded = manifest.get("bound_config")
    if isinstance(recorded, str) and recorded:
        return [Path(recorded)]
    found = set()
    for name in manifest.get("files", []):
        data = _read(name)
        if data is None:
            continue
        text = data.decode("utf-8", "replace")
        if "owned by agent-compute-scheduler" not in text:
            continue
        match = _BIND.search(text)
        if match:
            try:
                (value,) = shlex.split(match.group(1))
            except ValueError:
                raise Unanswerable(name, "its config binding cannot be "
                                         "parsed") from None
            found.add(os.path.realpath(value))
        elif ENV_CONFIG in text:
            raise Unanswerable(name, "its config binding cannot be "
                                     "understood")
    if len(found) > 1:
        raise Unanswerable(sorted(found)[0], "the installation's wrappers "
                                             "bind different configs")
    if found:
        return [Path(found.pop())]
    own = home() / ".config/agent-compute/config.toml"
    configs = [cli_default_config()]
    if not same_file(own, configs[0]):
        configs.append(own)
    return configs


def environment_config_problem(configs: list[Path]) -> str | None:
    """The installed CLI reads ``AGENT_COMPUTE_CONFIG`` before the
    installation's own config (``wrapper_text``).  When it is set and names
    another file than this installation's config(s), the jobs this uninstall
    must look for may be in either: refuse rather than guess (K3, N1)."""
    named = os.environ.get(ENV_CONFIG)
    if not named:
        return None                         # unset or empty: the CLI's `or`
    other = expand(named).absolute()
    if any(same_file(other, config) for config in configs):
        return None
    return (f"{ENV_CONFIG} names {other}, but this installation's config "
            f"is {_names(configs)}: the installed CLI would read the first, "
            f"and this uninstall cannot tell which one holds the jobs "
            f"(unset {ENV_CONFIG}, or name the installation's config)")


def _names(configs: list[Path]) -> str:
    return " or ".join(str(config) for config in configs)


def cmd_uninstall(args) -> int:
    p = paths(args)
    manifest = load_manifest(p["prefix"])
    # The CLI of this installation reads the config its wrapper binds, not
    # the one the flags of THIS command would name: look at that one.  A
    # --config naming another file, or an AGENT_COMPUTE_CONFIG that does, is
    # refused (even with --force: it is not evidence of jobs but a question
    # about which state to examine, and the answer is to name the right one)
    try:
        configs = bound_configs(manifest)
    except Unanswerable as why:
        print(f"uninstall: refusing (nothing was removed): the config this "
              f"installation's CLI reads cannot be established: {why}",
              file=sys.stderr)
        return 1
    if args.config and not any(same_file(p["config"], c) for c in configs):
        print(f"uninstall: refusing (nothing was removed): --config names "
              f"{p['config']}, but this installation's wrapper binds "
              f"{_names(configs)}, the config its CLI reads; name that one, "
              f"or omit --config", file=sys.stderr)
        return 1
    problem = environment_config_problem(configs)
    if problem:                             # no --force: not a guess to force
        print(f"uninstall: refusing (nothing was removed): {problem}",
              file=sys.stderr)
        return 1
    jobs, unanswered = [], []
    for config in configs:                  # every candidate is examined
        try:
            jobs += active_jobs(config)
        except Unanswerable as why:
            unanswered.append(why)
    if unanswered and not args.force:
        print(f"uninstall: cannot establish that no job is unfinished "
              f"(nothing was removed): {'; '.join(map(str, unanswered))}; "
              f"fix it, or --force", file=sys.stderr)
        return 1
    if jobs and not args.force:
        print(f"uninstall: unfinished jobs reference the installation: "
              f"{jobs}; wait or cancel them (or --force)", file=sys.stderr)
        return 1
    removed = []
    for link in manifest["links"]:
        path = Path(link)
        if owned_link(path, p["prefix"]):
            path.unlink()
            removed.append(link)
    for file in manifest["files"]:
        path = Path(file)
        if path.is_file() and "owned by agent-compute-scheduler" in \
                path.read_text(errors="replace"):
            path.unlink()
            removed.append(file)
    current = p["prefix"] / "current"
    if current.is_symlink():
        current.unlink()
        removed.append(str(current))
    for version in manifest["versions"]:
        path = Path(version)
        if path.is_dir() and str(path).startswith(str(p["prefix"])):
            make_writable(path)
            shutil.rmtree(path)
            removed.append(version)
    if manifest.get("created_config") and not args.keep_config:
        path = Path(manifest["created_config"])
        if path.is_file():
            path.unlink()
            removed.append(str(path))
    (p["prefix"] / "install-manifest.json").unlink(missing_ok=True)
    dirs = [str(p["prefix"] / "versions"), str(p["prefix"]),
            *manifest.get("created_dirs", [])]
    for empty in sorted(set(dirs), key=lambda d: -d.count("/")):
        try:
            Path(empty).rmdir()          # only ever removes an empty dir
        except OSError:
            pass
    print(json.dumps({"removed": removed}, indent=1))
    return 0


def cmd_status(args) -> int:
    p = paths(args)
    manifest = load_manifest(p["prefix"])
    current = p["prefix"] / "current"
    manifest["current_link"] = os.readlink(current) if current.is_symlink() \
        else None
    versions = p["prefix"] / "versions"
    dirs = {Path(v) for v in manifest["versions"]}
    if versions.is_dir():
        dirs |= {d for d in versions.iterdir()
                 if d.is_dir() and not d.name.startswith(".")}
    manifest["safety_levels"] = {d.name: safety_level(d) for d in sorted(dirs)}
    manifest["min_activatable_level"] = MIN_ACTIVATABLE_LEVEL
    print(json.dumps(manifest, indent=1, sort_keys=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--prefix")
    parser.add_argument("--bin")
    parser.add_argument("--config")
    sub = parser.add_subparsers(dest="cmd", required=True)
    inst = sub.add_parser("install")
    inst.add_argument("--python", help="interpreter for the wrapper "
                                       "(default: the one running this)")
    inst.add_argument("--config-from", help="example config to copy if no "
                                            "machine config exists")
    inst.add_argument("--allow-dirty", action="store_true")
    act = sub.add_parser("activate")
    act.add_argument("version")
    un = sub.add_parser("uninstall")
    un.add_argument("--keep-config", action="store_true")
    un.add_argument("--force", action="store_true",
                    help="operator only: remove even while unfinished jobs "
                         "reference the installation (never for agents)")
    sub.add_parser("status")
    args = parser.parse_args()
    return {"install": cmd_install, "activate": cmd_activate,
            "uninstall": cmd_uninstall, "status": cmd_status}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
