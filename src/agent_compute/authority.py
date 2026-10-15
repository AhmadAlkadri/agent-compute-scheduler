"""The single-authority condition, read-only (campaign G decision D13,
ADR-003): on one host there is one admission authority.

``doctor`` reports, on every run, three sub-checks:

* ``guard_home``: the configured guard home is the only one with lock
  users.  Any other guard home at a known location with a live lock user
  (``procs.lock_file_users``: lsof or ``/proc``, never opening the lock) or
  an in-flight marker (``inflight.json``) is a second admission authority.
  A lock whose users cannot be read cannot be ruled out (fail closed).
* ``state_dir``: the configured state dir is the only one at work.  Another
  agent-compute state dir at a known location with an unfinished job (or a
  job state that cannot be read, C4) or a live runner (a recorded runner
  identity proven alive by its start time) is a second authority.  A
  config at a known location that cannot be read cannot be ruled out.
* ``queue_daemon``: the configured Pueue config's daemon is the only ACS
  queue.  Another ``pueued`` of this host is a second authority when it
  serves the ACS group (its persisted ``state.json``) or is an agent-compute
  queue (``<state>/pueue/pueue.yml`` over ``<state>/pueue/data``, as
  ``queue.write_pueue_config`` writes it), whatever its group; one whose
  config or state cannot be read cannot be ruled out.  A ``pueued`` started
  without a config argument is a user's own Pueue (ACS always passes
  ``-c``): listed, never an error, even if it has a group of the ACS
  group's name (a limit: no ACS installation runs that way).  Pids are
  read from ``/proc/*/cmdline`` on Linux and from ``ps`` elsewhere (the
  executable by ``comm``, the arguments as one string, so paths with
  spaces stay whole); nothing is signalled or contacted.

Every location that cannot be read (a permission error, an unreadable
``guard-home.json``, a process table that cannot be listed) is an ERROR
that names it ("cannot be ruled out"), never a crash of doctor and never a
pass.

The known locations: the legacy home ``~/.smc-guard`` (as a guard home and
as a state dir), the default state dir ``~/.local/state/agent-compute``,
the config named by ``AGENT_COMPUTE_CONFIG`` and the default config
``~/.config/agent-compute/config.toml`` (their state dirs and guard
homes, read raw, never loaded), ``<state>/guard`` of every known state dir
(the configured one included), and the guard homes recorded in the
configured state dir's ``guard-home.json``.

Scope: a production config scans the real home and every ``pueued`` of the
host.  A ``test_mode`` config is a sandbox: it scans the known locations
under ``ACS_TEST_SCAN_HOME`` (honoured only in test mode) instead of the
real home and counts only the daemons whose config lies there; without it,
only what its own config names.

Nothing here writes, locks, signals or blocks: it is a report.  Its verdict
is never an admission input.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tomllib
from pathlib import Path

from . import procs
from .config import (DEFAULT_STATE_DIR, ENV_CONFIG, LEGACY_CONFIG_SCHEMA,
                     Config)
from .fsprobe import exists_strict, is_dir_strict
from .state import JobStore, Unreadable, read_json

SCAN_HOME_ENV = "ACS_TEST_SCAN_HOME"
LEGACY_HOME = ".smc-guard"
DEFAULT_CONFIG_REL = Path(".config") / "agent-compute" / "config.toml"
DEFAULT_STATE_REL = Path(DEFAULT_STATE_DIR.removeprefix("~/"))
GUARD_HOME_RECORD = "guard-home.json"


def _real(path) -> str:
    return os.path.realpath(str(path))


def _under(path: str, root: str) -> bool:
    path, root = _real(path), _real(root)
    return path == root or path.startswith(root.rstrip(os.sep) + os.sep)


def scan_scope(config) -> dict:
    """Where the scan looks: ``home`` (the home of the known locations, or
    None for none) and ``processes`` (``"all"``, a directory the daemons'
    configs must lie under, or None for the configured daemon only)."""
    if not config.test_mode:
        return {"home": str(Path.home()), "processes": "all",
                "env_config": True, "note": "production: the real home and "
                "every pueued of this host"}
    hook = os.environ.get(SCAN_HOME_ENV)
    if hook:
        return {"home": hook, "processes": hook, "env_config": True,
                "note": f"test mode: the known locations under "
                        f"{SCAN_HOME_ENV} and the daemons configured there"}
    return {"home": None, "processes": None, "env_config": False,
            "note": f"test mode without {SCAN_HOME_ENV}: only what this "
                    "config names; no location or daemon outside the "
                    "sandbox is scanned"}


def _expand(text: str, home: str | None) -> Path:
    """``~`` and ``$VARS`` as the config module expands them, ``~`` being
    the scanned home."""
    text = os.path.expandvars(text)
    if home and (text == "~" or text.startswith("~/")):
        return Path(home) / text[2:]
    return Path(os.path.expanduser(text))


def config_locations(path: Path, home: str | None) -> dict:
    """The state dir and guard home a config file names, read raw (never
    loaded, never refused for its schema).  ``{"error"}`` when it cannot be
    read."""
    try:
        raw = tomllib.loads(path.read_text())
        state_text = raw.get("state_dir", DEFAULT_STATE_DIR)
        backend = raw.get("backend") or {}
        if not isinstance(state_text, str) or not isinstance(backend, dict):
            raise ValueError("state_dir or [backend] has the wrong type")
        state = _expand(state_text, home)
        value = backend.get("guard_home")
        if value is not None and not isinstance(value, str):
            raise ValueError("backend.guard_home is not a string")
        if value:
            guard = _expand(value.replace("{state}", str(state)), home)
        elif raw.get("schema") == LEGACY_CONFIG_SCHEMA:
            # every version before 0.2.0: the guard's own home
            guard = _expand(f"~/{LEGACY_HOME}", home)
        else:
            guard = state / "guard"
    except (OSError, ValueError, tomllib.TOMLDecodeError) as error:
        return {"error": f"{type(error).__name__}: {error}"}
    return {"state_dir": str(state), "guard_home": str(guard),
            "schema": raw.get("schema")}


def _known_configs(config: Config, scope: dict) -> list[tuple[Path, str]]:
    found = []
    if scope["env_config"] and os.environ.get(ENV_CONFIG):
        found.append((_expand(os.environ[ENV_CONFIG], scope["home"]),
                      f"${ENV_CONFIG}"))
    if scope["home"]:
        found.append((Path(scope["home"]) / DEFAULT_CONFIG_REL,
                      "the default config"))
    mine = _real(config.path)
    return [(p, why) for p, why in found if _real(p) != mine]


def _dedup(items: list[tuple[str, str]], exclude: str) -> list[tuple[str,
                                                                     str]]:
    seen, out = {exclude}, []
    for path, why in items:
        key = _real(path)
        if key in seen:
            continue
        seen.add(key)
        out.append((path, why))
    return out


UNSURE = "cannot be ruled out as a second admission authority"


def _state_dir_scan(state_dir: Path, source: str) -> dict:
    """One state dir; a location that cannot be read (``OSError``, for
    example a directory without search permission) is a problem, never a
    crash of doctor."""
    try:
        return _scan_state(state_dir, source)
    except (OSError, procs.ProcCheckError) as error:
        return {"state_dir": str(state_dir), "source": source,
                "exists": None, "unfinished": [], "live_runners": [],
                "error": f"{type(error).__name__}: {error}",
                "problem": f"{state_dir} ({source}) cannot be read "
                           f"({type(error).__name__}: {error}): a state dir "
                           f"that {UNSURE}"}


def _scan_state(state_dir: Path, source: str) -> dict:
    entry = {"state_dir": str(state_dir), "source": source,
             "exists": is_dir_strict(state_dir), "unfinished": [],
             "live_runners": [], "problem": None}
    jobs = state_dir / "jobs"
    if not is_dir_strict(jobs):         # A49: raises when it cannot tell
        return entry
    store = JobStore(state_dir)
    try:
        ids = store.all_ids()
    except OSError as error:
        entry["problem"] = (f"its jobs cannot be listed ({error}): an "
                            "agent-compute state dir that cannot be ruled "
                            "out as a second admission authority")
        return entry
    for job_id in ids:
        try:
            st = store.state(job_id)
        except ValueError:
            continue
        if isinstance(st, Unreadable):
            entry["unfinished"].append(job_id)      # C4: it may be
            continue
        st = st if isinstance(st, dict) else {}
        if not st.get("outcome"):
            entry["unfinished"].append(job_id)
        runner = st.get("runner")
        if isinstance(runner, dict) and \
                procs.liveness(runner) == procs.ALIVE:
            entry["live_runners"].append({"job_id": job_id,
                                          "pid": runner.get("pid")})
    if entry["unfinished"] or entry["live_runners"]:
        entry["problem"] = (
            f"another agent-compute state dir ({source}) has unfinished "
            f"jobs {entry['unfinished']} or live runners "
            f"{[r['pid'] for r in entry['live_runners']]}: a second "
            "admission authority on this host")
    return entry


def state_dir_check(config: Config, scope: dict,
                    configs: list[tuple[Path, dict, str]]) -> dict:
    candidates: list[tuple[str, str]] = []
    problems = []
    if scope["home"]:
        home = Path(scope["home"])
        candidates.append((str(home / DEFAULT_STATE_REL),
                           "the default state dir"))
        candidates.append((str(home / LEGACY_HOME), "the legacy home"))
    for path, where, why in configs:
        if "error" in where:
            problems.append(f"the config at {path} ({why}) cannot be read "
                            f"({where['error']}): its state dir cannot be "
                            "ruled out as a second admission authority")
            continue
        candidates.append((where["state_dir"], f"the state dir of {path} "
                                               f"({why})"))
    scanned = [_state_dir_scan(Path(p), why) for p, why in
               _dedup(candidates, _real(config.state_dir))]
    problems += [s["problem"] for s in scanned if s["problem"]]
    return {"ok": not problems, "configured": str(config.state_dir),
            "scanned": scanned, "problems": problems}


def _guard_home_scan(home: Path, source: str) -> dict:
    """One guard home; a location that cannot be read is a problem, never a
    crash of doctor."""
    try:
        return _scan_guard_home(home, source)
    except OSError as error:
        return {"guard_home": str(home), "source": source,
                "lock": str(home / "lock"), "lock_exists": None,
                "lock_users": None, "inflight": None,
                "error": f"{type(error).__name__}: {error}",
                "problem": f"{home} ({source}) cannot be read "
                           f"({type(error).__name__}: {error}): a guard home "
                           f"that {UNSURE}"}


def _scan_guard_home(home: Path, source: str) -> dict:
    lock = home / "lock"
    users = procs.lock_file_users(lock)
    inflight = home / "inflight.json"
    entry = {"guard_home": str(home), "source": source,
             "lock": str(lock), "lock_exists": exists_strict(lock),
             "lock_users": users, "inflight": str(inflight)
             if exists_strict(inflight) else None, "problem": None}
    if users is None:
        entry["problem"] = (f"the users of {lock} cannot be read: another "
                            f"guard home ({source}) that cannot be ruled "
                            "out as a second admission authority")
    elif users or entry["inflight"]:
        entry["problem"] = (
            f"another guard home ({source}) has lock users {users} "
            + (f"and an in-flight marker {entry['inflight']} "
               if entry["inflight"] else "")
            + "(read-only): a second admission authority on this host")
    return entry


def guard_home_check(config: Config, scope: dict,
                     configs: list[tuple[Path, dict, str]],
                     state_scanned: list[dict]) -> dict:
    configured = config.effective_guard_home
    candidates: list[tuple[str, str]] = []
    if scope["home"]:
        candidates.append((str(Path(scope["home"]) / LEGACY_HOME),
                           "the legacy home"))
    candidates.append((str(config.state_dir / "guard"),
                       "the configured state dir's default guard home"))
    problems = []
    record = read_json(config.state_dir / GUARD_HOME_RECORD)
    if isinstance(record, Unreadable):
        # C4: never read as absent
        problems.append(f"{record.path} cannot be read ({record.why}): the "
                        f"guard homes it records {UNSURE}")
    elif isinstance(record, dict):
        for key in ("guard_home", "previous"):
            if isinstance(record.get(key), str):
                candidates.append((record[key], f"recorded in "
                                   f"{GUARD_HOME_RECORD} ({key})"))
    for path, where, why in configs:
        if "error" not in where:
            candidates.append((where["guard_home"],
                               f"the guard home of {path} ({why})"))
    for entry in state_scanned:
        candidates.append((str(Path(entry["state_dir"]) / "guard"),
                           f"the default guard home of {entry['source']}"))
    scanned = [_guard_home_scan(Path(p), why) for p, why in
               _dedup(candidates, _real(configured))]
    problems += [s["problem"] for s in scanned if s["problem"]]
    try:
        users = procs.lock_file_users(configured / "lock")
    except OSError:
        users = None                    # reported only; ours may have users
    return {"ok": not problems, "configured": str(configured),
            "configured_lock_users": users,
            "scanned": scanned, "problems": problems}


def daemon_config(argv: list[str]) -> str | None:
    """The config argument of a ``pueued`` command line (``-c X``,
    ``-cX``, ``--config X``, ``--config=X``); None without one."""
    for i, arg in enumerate(argv[1:], start=1):
        if arg in ("-c", "--config"):
            return argv[i + 1] if i + 1 < len(argv) else None
        if arg.startswith("--config="):
            return arg.split("=", 1)[1]
        if arg.startswith("-c") and not arg.startswith("--"):
            return arg[2:]
    return None


_CONFIG_FLAG = re.compile(r"(?:^|\s)(?:--config(?:=|\s+)|-c\s+|-c(?=\S))(.*)$")


def config_from_tail(tail: str, mine: str | None = None) -> str | None:
    """The config path in the arguments of a ``pueued`` command line as
    ``ps`` prints it (one string, so a path may contain spaces): the
    configured path when the arguments start with it; else the longest
    prefix, cut at a space, that is an existing file; else the first word
    (which then cannot be read: never ruled out).  None without a config
    argument."""
    match = _CONFIG_FLAG.search(tail)
    if not match:
        return None
    rest = match.group(1).rstrip()
    if not rest:
        return None
    if mine and (rest == mine or rest.startswith(mine + " ")):
        return mine
    cuts = [i for i, char in enumerate(rest) if char == " "] + [len(rest)]
    for cut in reversed(cuts):
        if os.path.isfile(rest[:cut]):
            return rest[:cut]
    return rest.split()[0]


def daemon_config_of(daemon: dict, mine: str | None = None) -> str | None:
    """The config argument of a listed daemon: from its exact argv
    (``/proc``) or from its ``ps`` argument string (macOS)."""
    if daemon.get("argv") is not None:
        return daemon_config(daemon["argv"])
    return config_from_tail(daemon.get("tail") or "", mine)


def list_pueued_ps() -> list[dict]:
    """Every ``pueued`` in ``ps`` (macOS): the executable by ``comm`` (a
    whole path, spaces included), then its arguments as one string (the
    command line after the executable): ``{"pid", "exe", "tail"}``."""
    found = []
    for line in procs._ps(["-axww", "-o", "pid=,comm="]).splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) != 2 or not parts[0].isdigit():
            continue
        comm = parts[1].rstrip()
        if os.path.basename(comm) != "pueued":
            continue
        args = procs._ps(["-ww", "-o", "args=", "-p", parts[0]]).strip()
        if not args:
            continue                    # gone meanwhile
        if args.startswith(comm):
            tail = args[len(comm):]
        else:
            tail = args.split(None, 1)[1] if " " in args else ""
        found.append({"pid": int(parts[0]), "exe": comm, "tail": tail,
                      "cwd": None})
    return found


def list_pueued() -> list[dict]:
    """Every ``pueued`` of this host, read-only: ``{"pid", "argv"}`` from
    ``/proc`` on Linux, ``list_pueued_ps`` elsewhere (raises
    ``procs.ProcCheckError`` when the table cannot be read)."""
    found = []
    if sys.platform.startswith("linux"):
        try:
            names = os.listdir("/proc")
        except OSError as error:
            raise procs.ProcCheckError(str(error)) from error
        for name in names:
            if not name.isdigit():
                continue
            try:
                raw = Path(f"/proc/{name}/cmdline").read_bytes()
            except OSError:
                continue
            argv = [a.decode(errors="replace") for a in raw.split(b"\0") if a]
            if argv and os.path.basename(argv[0]) == "pueued":
                found.append({"pid": int(name), "argv": argv,
                              "cwd": _proc_cwd(name)})
        return found
    return list_pueued_ps()


def _proc_cwd(pid: str) -> str | None:
    try:
        return os.readlink(f"/proc/{pid}/cwd")
    except OSError:
        return None


def _yaml_value(text: str, key: str) -> str | None:
    """A scalar of Pueue's flat YAML (``key: value``, plain or quoted)."""
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith(f"{key}:"):
            continue
        value = stripped[len(key) + 1:].strip()
        if value.startswith('"'):
            try:
                return json.loads(value)
            except ValueError:
                return None
        if value.startswith("'") and value.endswith("'") and len(value) > 1:
            return value[1:-1]
        return value.split(" #", 1)[0].strip() or None
    return None


def classify_daemon(daemon: dict, config) -> dict:
    """Whether one ``pueued`` (not the configured one) is a second ACS
    queue: ``problem`` None when it is provably not."""
    entry = {"pid": daemon["pid"], "config": None, "groups": [],
             "acs_queue_of": None, "problem": None}
    cfg = daemon_config_of(daemon)
    if cfg is None:
        entry["note"] = ("no config argument: a user's own Pueue (ACS "
                         "always starts its daemon with -c)")
        return entry
    if not os.path.isabs(cfg) and daemon.get("cwd"):
        cfg = os.path.join(daemon["cwd"], cfg)
    entry["config"] = cfg
    unsure = ("cannot be ruled out as a second ACS queue (inspect it "
              "read-only)")
    if not os.path.isabs(cfg):
        entry["problem"] = f"pid {daemon['pid']}: relative config {cfg}; " \
                           + unsure
        return entry
    try:
        text = Path(cfg).read_text()
    except OSError as error:
        entry["problem"] = (f"pid {daemon['pid']}: its config {cfg} cannot "
                            f"be read ({type(error).__name__}); " + unsure)
        return entry
    data = _yaml_value(text, "pueue_directory")
    base = Path(cfg).parent
    if base.name == "pueue" and data and \
            _real(data) == _real(base / "data"):
        entry["acs_queue_of"] = str(base.parent)
    groups = None
    if data:
        state = read_json(Path(data) / "state.json")
        if isinstance(state, dict) and isinstance(state.get("groups"), dict):
            groups = sorted(state["groups"])
    if groups is not None:
        entry["groups"] = groups
    if entry["acs_queue_of"]:
        entry["problem"] = (f"pid {daemon['pid']} serves the agent-compute "
                            f"queue of the state dir {entry['acs_queue_of']} "
                            f"({cfg}): a second admission authority on this "
                            "host")
    elif groups is None:
        entry["problem"] = (f"pid {daemon['pid']}: the persisted state of "
                            f"{cfg} cannot be read; " + unsure)
    elif config.queue_group in groups:
        entry["problem"] = (f"pid {daemon['pid']} ({cfg}) serves the ACS "
                            f"group {config.queue_group!r}: a second "
                            "admission authority on this host")
    return entry


def queue_daemon_check(config: Config, scope: dict) -> dict:
    from . import queue as q
    configured = str(q.pueue_config_path(config))
    mine = _real(configured)
    try:
        socket_exists = exists_strict(q.socket_path(config))
    except OSError:
        socket_exists = None
    detail = {"ok": True, "configured_config": configured,
              "configured_socket": str(q.socket_path(config)),
              "socket_exists": socket_exists,
              "ours": [], "others": [], "problems": []}
    try:
        daemons = list_pueued()
    except procs.ProcCheckError as error:
        detail["problems"].append(f"the process table cannot be read "
                                  f"({error}): a second queue daemon cannot "
                                  "be ruled out")
        detail["ok"] = False
        return detail
    for daemon in daemons:
        cfg = daemon_config_of(daemon, configured)
        if cfg is not None and not os.path.isabs(cfg) and daemon.get("cwd"):
            cfg = os.path.join(daemon["cwd"], cfg)
        if cfg is not None and _real(cfg) == mine:
            detail["ours"].append(daemon["pid"])
            continue
        if scope["processes"] != "all" and (
                scope["processes"] is None or cfg is None or
                not _under(cfg, scope["processes"])):
            continue                    # test mode: outside the sandbox
        detail["others"].append(classify_daemon(daemon, config))
    if len(detail["ours"]) > 1:
        detail["problems"].append(f"{len(detail['ours'])} daemons "
                                  f"{detail['ours']} serve the configured "
                                  "queue")
    detail["problems"] += [d["problem"] for d in detail["others"]
                           if d["problem"]]
    detail["ok"] = not detail["problems"]
    return detail


def _present(path: Path) -> bool:
    """Whether a file is there; one that cannot be looked at counts as
    there (its read then reports it)."""
    try:
        return exists_strict(path)      # A49: Path.exists no longer raises
    except OSError:
        return True


def _guarded(label: str, scan) -> dict:
    """A sub-check that failed unexpectedly is an ERROR row with the
    reason, never a crash of doctor (the scan is a report)."""
    try:
        return scan()
    except Exception as error:          # noqa: BLE001: fail closed, report
        return {"ok": False, "scanned": [],
                "problems": [f"the {label} scan failed ({type(error).__name__}"
                             f": {error}): a second admission authority "
                             "cannot be ruled out"]}


def single_authority(config: Config) -> dict:
    """The three sub-checks, read-only."""
    scope = scan_scope(config)
    configs = [(path, config_locations(path, scope["home"]), why)
               for path, why in _known_configs(config, scope)
               if _present(path)]
    state = _guarded("state dir", lambda: state_dir_check(config, scope,
                                                          configs))
    guard = _guarded("guard home", lambda: guard_home_check(
        config, scope, configs, state.get("scanned") or []))
    queue = _guarded("queue daemon", lambda: queue_daemon_check(config,
                                                                scope))
    return {"ok": guard["ok"] and state["ok"] and queue["ok"],
            "scope": {k: scope[k] for k in ("home", "processes", "note")},
            "guard_home": guard, "state_dir": state, "queue_daemon": queue,
            "note": "read-only (D13): exactly one guard home with lock "
                    "users, one state dir, one queue daemon; never an "
                    "admission input"}
