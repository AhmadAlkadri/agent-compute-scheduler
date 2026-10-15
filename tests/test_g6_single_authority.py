"""Campaign G decision D13 (G6 step 3, ADR-003): ``doctor`` checks and
reports the **single-authority condition** on every run.

On one host there is exactly one admission authority:

* **one guard home with lock users**: the configured guard home.  Any other
  guard home at a known location (the legacy ``~/.smc-guard``, the default
  ``<state>/guard`` of every known state dir, the guard home of the config
  named by ``AGENT_COMPUTE_CONFIG`` and of the default config, the homes
  recorded in ``<state>/guard-home.json``) with a live lock user or an
  in-flight marker is an ERROR: it would be a second admission authority.
* **one state dir**: the configured one.  Another agent-compute state dir
  at a known location (the default ``~/.local/state/agent-compute``, the
  state dir of the ``AGENT_COMPUTE_CONFIG`` and default configs, the
  legacy ``~/.smc-guard`` home) with an unfinished job (or one whose state
  cannot be read, C4) or a live runner is an ERROR.
* **one queue daemon socket**: the configured Pueue config's.  Another
  ``pueued`` on this host serving the ACS group, or an agent-compute queue
  config of another state dir, is an ERROR; pids are reported, read-only
  (``ps`` / ``/proc``), never signalled.

The check is read-only (it opens no lock, writes nothing outside the
configured state dir, signals nothing) and never blocks a job by itself;
doctor's exit code reflects it like its other ERROR checks; ``doctor
--json`` carries it under ``single_authority`` (``guard_home``,
``state_dir``, ``queue_daemon``).

Scope in tests: a ``test_mode`` config is a sandbox, so its doctor scans
the known locations under ``ACS_TEST_SCAN_HOME`` (honoured only in test
mode) instead of the real home, and counts only the ``pueued`` daemons
whose config lies there; without it, a test-mode doctor scans no location
outside its own config.  A production config always scans the real home
and every ``pueued`` of the host.  The sleepers holding a lock here are
started by the test and killed by pid at teardown, never by pattern.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from conftest import PUEUE, PUEUED, Env, skip_reason

SCAN = "ACS_TEST_SCAN_HOME"
SUBS = ("guard_home", "state_dir", "queue_daemon")
ROWS = {"guard_home": "single authority: guard home",
        "state_dir": "single authority: state dir",
        "queue_daemon": "single authority: queue daemon"}

# holds an exclusive flock on argv[1] (created if absent), says so, sleeps
HOLD = ("import fcntl, sys, time\n"
        "f = open(sys.argv[1], 'a+')\n"
        "fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
        "print('held', flush=True)\n"
        "time.sleep(600)\n")


def doctor(env: Env, **extra) -> tuple[int, dict]:
    result = env.acs("doctor", "--json", env=env.env(**extra))
    assert result.stdout, result.stderr
    return result.returncode, json.loads(result.stdout)


def sub(report: dict, name: str) -> dict:
    return report["single_authority"][name]


def row(report: dict, name: str) -> dict:
    found = [c for c in report["checks"] if c["check"] == ROWS[name]]
    assert len(found) == 1, [c["check"] for c in report["checks"]]
    return found[0]


def others_ok(report: dict) -> bool:
    """Every check but the three single-authority ones passes (or is only
    a warning or information)."""
    return all(c["ok"] or c["level"] in ("warning", "info")
               for c in report["checks"] if c["check"] not in ROWS.values())


@pytest.fixture
def sleepers():
    started: list[subprocess.Popen] = []

    def start(lock: Path) -> subprocess.Popen:
        lock.parent.mkdir(parents=True, exist_ok=True)
        proc = subprocess.Popen([sys.executable, "-c", HOLD, str(lock)],
                                stdout=subprocess.PIPE, text=True)
        started.append(proc)
        assert proc.stdout.readline().strip() == "held"
        return proc

    yield start
    for proc in started:
        if proc.poll() is None:
            os.kill(proc.pid, signal.SIGKILL)       # by pid, never by pattern
        proc.wait(timeout=30)


@pytest.fixture
def home():
    """A sandbox home for the known locations (short, under /tmp: a Pueue
    socket may live there)."""
    path = Path(tempfile.mkdtemp(prefix="acs-sa-", dir="/tmp"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


def unfinished_job(state_dir: Path, job_id: str = "acs-other-1",
                   **fields) -> Path:
    path = state_dir / "jobs" / job_id / "state.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"schema": "agent-compute-state/1",
                                "job_id": job_id, "phase": "queued",
                                **fields}))
    return path


def snapshot(root: Path) -> dict:
    return {str(p): (p.stat().st_mtime_ns, p.read_bytes()) if p.is_file()
            else ("dir", p.stat().st_mtime_ns)
            for p in sorted(root.rglob("*"))}


# --- the report ---------------------------------------------------------------------------

def test_the_configured_authority_alone_passes(env_direct, home, sleepers):
    env = env_direct
    holder = sleepers(env.guard_home / "lock")     # the configured lock in use
    code, report = doctor(env, **{SCAN: str(home)})
    assert others_ok(report), report["checks"]
    single = report["single_authority"]
    assert set(SUBS) <= set(single) and single["ok"] is True, single
    for name in SUBS:
        assert sub(report, name)["ok"] is True, sub(report, name)
        assert sub(report, name)["problems"] == []
        assert row(report, name)["result"] == "pass"
        assert row(report, name)["level"] == "ok"
    assert code == 0 and report["ok"] is True
    guard = sub(report, "guard_home")
    assert os.path.realpath(guard["configured"]) == \
        os.path.realpath(env.guard_home)
    assert holder.pid in guard["configured_lock_users"]
    state = sub(report, "state_dir")
    assert os.path.realpath(state["configured"]) == \
        os.path.realpath(env.state)
    # the known locations were looked at, in the sandbox home
    scanned = {os.path.realpath(s["guard_home"]) for s in guard["scanned"]}
    assert os.path.realpath(home / ".smc-guard") in scanned
    scanned = {os.path.realpath(s["state_dir"]) for s in state["scanned"]}
    assert os.path.realpath(home / ".local" / "state" / "agent-compute") \
        in scanned
    queue = sub(report, "queue_daemon")
    assert queue["configured_config"].endswith("pueue.yml")
    assert queue["configured_socket"].endswith("pueue.sock")


def test_human_output_names_the_three_checks(env_direct, home):
    result = env_direct.acs("doctor", env=env_direct.env(**{SCAN: str(home)}))
    for name in ROWS.values():
        assert f"[PASS] {name}" in result.stdout, result.stdout


# --- guard homes ------------------------------------------------------------------------

def test_a_second_guard_home_with_a_lock_user_is_an_error(env_direct, home,
                                                          sleepers):
    env = env_direct
    legacy = home / ".smc-guard"
    holder = sleepers(legacy / "lock")
    code, report = doctor(env, **{SCAN: str(home)})
    guard = sub(report, "guard_home")
    assert guard["ok"] is False
    assert row(report, "guard_home")["result"] == "fail"
    assert row(report, "guard_home")["level"] == "error"
    text = json.dumps(guard)
    assert str(legacy) in text and str(holder.pid) in text
    hit = [s for s in guard["scanned"]
           if os.path.realpath(s["guard_home"]) == os.path.realpath(legacy)]
    assert hit and holder.pid in hit[0]["lock_users"] and hit[0]["problem"]
    assert report["single_authority"]["ok"] is False
    assert code == 1 and report["ok"] is False
    # human output: the same verdict
    human = env.acs("doctor", env=env.env(**{SCAN: str(home)}))
    assert f"[FAIL] {ROWS['guard_home']}" in human.stdout
    assert "PROBLEMS FOUND" in human.stdout and human.returncode == 1
    # the user gone (killed by its pid): no second authority left
    os.kill(holder.pid, signal.SIGKILL)
    holder.wait(timeout=30)
    code, report = doctor(env, **{SCAN: str(home)})
    assert sub(report, "guard_home")["ok"] is True, sub(report, "guard_home")
    assert code == 0


def test_a_second_guard_home_with_an_inflight_marker_is_an_error(
        env_direct, home):
    other = home / ".local" / "state" / "agent-compute" / "guard"
    other.mkdir(parents=True)
    (other / "inflight.json").write_text(json.dumps({"job_id": "x"}))
    code, report = doctor(env_direct, **{SCAN: str(home)})
    guard = sub(report, "guard_home")
    assert guard["ok"] is False and code == 1
    assert str(other / "inflight.json") in json.dumps(guard)


def test_the_guard_home_of_agent_compute_config_is_scanned(env_direct, home,
                                                           sleepers):
    """A config named by AGENT_COMPUTE_CONFIG (here another install's) is a
    known location: its guard home (``{state}`` and ``~`` expanded as that
    config would, ``~`` being the scanned home) is checked."""
    other = home / "other.toml"
    other.write_text('schema = "agent-compute-config/2"\n'
                     'state_dir = "~/elsewhere"\n'
                     '[backend]\nguard_home = "{state}/gh"\n')
    holder = sleepers(home / "elsewhere" / "gh" / "lock")
    code, report = doctor(env_direct, **{SCAN: str(home),
                                         "AGENT_COMPUTE_CONFIG": str(other)})
    guard = sub(report, "guard_home")
    assert guard["ok"] is False and code == 1
    assert str(home / "elsewhere" / "gh") in json.dumps(guard)
    assert str(holder.pid) in json.dumps(guard)


def test_doctor_scans_read_only(env_direct, home):
    """Nothing is created, opened for locking or changed at a scanned
    location: an absent lock stays absent."""
    (home / ".smc-guard").mkdir()
    unfinished_job(home / ".local" / "state" / "agent-compute")
    before = snapshot(home)
    doctor(env_direct, **{SCAN: str(home)})
    assert snapshot(home) == before
    assert not (home / ".smc-guard" / "lock").exists()
    assert sorted(p.name for p in (home / ".smc-guard").iterdir()) == []


# --- state dirs -------------------------------------------------------------------------

def test_a_second_state_dir_with_an_unfinished_job_is_an_error(env_direct,
                                                               home):
    other = home / ".local" / "state" / "agent-compute"
    path = unfinished_job(other, "acs-other-1")
    code, report = doctor(env_direct, **{SCAN: str(home)})
    state = sub(report, "state_dir")
    assert state["ok"] is False and code == 1
    assert row(report, "state_dir")["result"] == "fail"
    hit = [s for s in state["scanned"]
           if os.path.realpath(s["state_dir"]) == os.path.realpath(other)]
    assert hit and hit[0]["unfinished"] == ["acs-other-1"], hit
    assert hit[0]["problem"]
    # finished: no second authority left
    data = json.loads(path.read_text())
    data["outcome"] = "succeeded"
    path.write_text(json.dumps(data))
    code, report = doctor(env_direct, **{SCAN: str(home)})
    assert sub(report, "state_dir")["ok"] is True, sub(report, "state_dir")
    assert code == 0


def test_an_unreadable_job_state_counts_as_unfinished(env_direct, home):
    """C4: a state that cannot be read may be unfinished."""
    other = home / ".local" / "state" / "agent-compute"
    path = other / "jobs" / "acs-other-2" / "state.json"
    path.parent.mkdir(parents=True)
    path.write_text("{not json")
    code, report = doctor(env_direct, **{SCAN: str(home)})
    assert sub(report, "state_dir")["ok"] is False and code == 1
    assert "acs-other-2" in json.dumps(sub(report, "state_dir"))


def test_a_live_runner_in_a_second_state_dir_is_an_error(env_direct, home,
                                                         sleepers):
    """A finished job whose runner still runs (proven by its start time)
    is still a second authority at work."""
    from agent_compute import procs
    runner = sleepers(home / "runner-stand-in.lock")
    other = home / ".local" / "state" / "agent-compute"
    unfinished_job(other, "acs-other-3", outcome="succeeded",
                   runner=procs.identity(runner.pid))
    code, report = doctor(env_direct, **{SCAN: str(home)})
    state = sub(report, "state_dir")
    assert state["ok"] is False and code == 1
    hit = [s for s in state["scanned"]
           if os.path.realpath(s["state_dir"]) == os.path.realpath(other)]
    assert hit and hit[0]["live_runners"], hit
    assert str(runner.pid) in json.dumps(hit[0]["live_runners"])


def test_the_state_dir_of_agent_compute_config_is_scanned(env_direct, home):
    other_state = home / "another-install" / "state"
    other = home / "another.toml"
    other.write_text('schema = "agent-compute-config/2"\n'
                     f'state_dir = "{other_state}"\n')
    unfinished_job(other_state, "acs-other-4")
    code, report = doctor(env_direct, **{SCAN: str(home),
                                         "AGENT_COMPUTE_CONFIG": str(other)})
    state = sub(report, "state_dir")
    assert state["ok"] is False and code == 1
    assert "acs-other-4" in json.dumps(state)
    assert str(other) in json.dumps(state)          # where it was found


def test_the_legacy_home_is_scanned_as_a_state_dir(env_direct, home):
    unfinished_job(home / ".smc-guard", "acs-other-5")
    code, report = doctor(env_direct, **{SCAN: str(home)})
    assert sub(report, "state_dir")["ok"] is False and code == 1
    assert "acs-other-5" in json.dumps(sub(report, "state_dir"))


def test_an_unreadable_config_at_a_known_location_is_an_error(env_direct,
                                                              home):
    """A config there whose state dir cannot be read cannot be ruled out."""
    default = home / ".config" / "agent-compute" / "config.toml"
    default.parent.mkdir(parents=True)
    default.write_text("schema = [unterminated\n")
    code, report = doctor(env_direct, **{SCAN: str(home)})
    assert sub(report, "state_dir")["ok"] is False and code == 1
    assert str(default) in json.dumps(sub(report, "state_dir"))


def test_the_check_never_blocks_a_job(env_queue, home):
    """Doctor reports; it is no admission input: a submit is accepted and
    runs while doctor reports a second authority."""
    env = env_queue
    unfinished_job(home / ".local" / "state" / "agent-compute")
    code, report = doctor(env, **{SCAN: str(home)})
    assert sub(report, "state_dir")["ok"] is False and code == 1
    job = env.submit(workload_args=["--out", str(env.root / "w.out")],
                     env=env.env(**{SCAN: str(home)}))["job_id"]
    assert env.wait_outcome(job)["outcome"] == "succeeded"


# --- queue daemons ------------------------------------------------------------------------

def pueued_pid(state: Path) -> int:
    return int((state / "pueue" / "run" / "pueue.pid").read_text())


def test_a_second_acs_daemon_with_the_acs_group_is_an_error(env_queue):
    if not (PUEUE and PUEUED):
        pytest.skip(skip_reason("pueue", "pueue/pueued not on PATH"))
    a = env_queue
    b = Env(queue=True)
    try:
        assert a.acs("daemon", "start").returncode == 0
        assert b.acs("daemon", "start").returncode == 0
        mine, theirs = pueued_pid(a.state), pueued_pid(b.state)
        code, report = doctor(a, **{SCAN: str(b.root)})
        queue = sub(report, "queue_daemon")
        assert queue["ok"] is False and code == 1
        assert row(report, "queue_daemon")["result"] == "fail"
        assert mine in queue["ours"], queue
        hit = [d for d in queue["others"] if d["pid"] == theirs]
        assert hit and hit[0]["problem"], queue
        assert "acs-heavy" in hit[0]["groups"]
        assert os.path.realpath(hit[0]["config"]) == \
            os.path.realpath(b.state / "pueue" / "pueue.yml")
        # both daemons are untouched (read-only: no signal, no shutdown)
        assert b.acs("daemon", "status", "--json").returncode == 0
        assert json.loads(b.acs("daemon", "status", "--json").stdout)[
            "running"] is True
        # a test-mode doctor without the scan home sees its own sandbox only
        code, report = doctor(a)
        queue = sub(report, "queue_daemon")
        assert queue["ok"] is True, queue
        assert theirs not in [d["pid"] for d in queue["others"]]
        assert mine in queue["ours"]
    finally:
        b.cleanup()


def test_an_acs_queue_of_another_state_dir_is_an_error_whatever_its_group(
        env_queue):
    """Another installation's queue (an agent-compute Pueue config:
    ``<state>/pueue/pueue.yml`` over ``<state>/pueue/data``) is a second
    authority even under another group name."""
    a = env_queue
    b = Env(queue=True)
    try:
        b.config.write_text(b.config.read_text().replace(
            'group = "acs-heavy"', 'group = "other-heavy"'))
        assert b.acs("daemon", "start").returncode == 0
        theirs = pueued_pid(b.state)
        code, report = doctor(a, **{SCAN: str(b.root)})
        queue = sub(report, "queue_daemon")
        hit = [d for d in queue["others"] if d["pid"] == theirs]
        assert hit and hit[0]["problem"], queue
        assert "acs-heavy" not in hit[0]["groups"]
        assert queue["ok"] is False and code == 1
    finally:
        b.cleanup()


def stop_own_daemon(pid_file: Path, cfg: Path) -> None:
    """This test's own pueued, if the graceful shutdown left it running:
    killed by its pid, once its command line is proven to name ``cfg``
    (never by pattern)."""
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            pid = int(pid_file.read_text())
        except (OSError, ValueError):
            return
        args = subprocess.run(["ps", "-ww", "-o", "args=", "-p", str(pid)],
                              capture_output=True, text=True,
                              timeout=30).stdout
        if str(cfg) not in args:
            return
        time.sleep(0.5)
    os.kill(pid, signal.SIGKILL)


def test_a_foreign_pueued_is_reported_and_an_error_only_with_the_acs_group(
        env_queue, home):
    """A Pueue daemon that is not an agent-compute queue is listed, not an
    error, until it serves the ACS group."""
    base = home / "q"
    cfg = base / "pueue.yml"
    (base / "data").mkdir(parents=True)
    (base / "run").mkdir()
    cfg.write_text("shared:\n"
                   f"  pueue_directory: {base / 'data'}\n"
                   f"  runtime_directory: {base / 'run'}\n"
                   "  use_unix_socket: true\n"
                   f"  unix_socket_path: {base / 'run' / 'pueue.sock'}\n")
    try:
        # the daemonized pueued keeps its stdio: never a pipe we wait on
        with open(base / "pueued.log", "ab") as log:
            subprocess.run([PUEUED, "-c", str(cfg), "-d"],
                           stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                           timeout=60, check=True, start_new_session=True)
        client = [PUEUE, "-c", str(cfg)]
        deadline = time.time() + 30
        while subprocess.run([*client, "status"], capture_output=True,
                             timeout=30).returncode:
            assert time.time() < deadline, "the foreign pueued did not start"
            time.sleep(0.2)
        theirs = int((base / "run" / "pueue.pid").read_text())
        code, report = doctor(env_queue, **{SCAN: str(home)})
        queue = sub(report, "queue_daemon")
        hit = [d for d in queue["others"] if d["pid"] == theirs]
        assert hit and not hit[0]["problem"], queue
        assert queue["ok"] is True
        subprocess.run([*client, "group", "add", "acs-heavy"],
                       capture_output=True, timeout=30, check=True)
        deadline = time.time() + 30
        while True:
            code, report = doctor(env_queue, **{SCAN: str(home)})
            queue = sub(report, "queue_daemon")
            hit = [d for d in queue["others"] if d["pid"] == theirs]
            if hit and hit[0]["problem"] or time.time() > deadline:
                break
            time.sleep(0.5)     # Pueue persists its state asynchronously
        assert hit and hit[0]["problem"], queue
        assert "acs-heavy" in hit[0]["groups"]
        assert queue["ok"] is False and code == 1
    finally:
        subprocess.run([PUEUE, "-c", str(cfg), "shutdown"],
                       capture_output=True, timeout=30)
        stop_own_daemon(base / "run" / "pueue.pid", cfg)


# --- units ------------------------------------------------------------------------------

@pytest.mark.parametrize("argv,expected", [
    (["/opt/homebrew/bin/pueued", "-c", "/x/pueue.yml", "-d"], "/x/pueue.yml"),
    (["pueued", "--config", "/y/pueue.yml"], "/y/pueue.yml"),
    (["pueued", "--config=/z/pueue.yml"], "/z/pueue.yml"),
    (["pueued", "-c/w/pueue.yml"], "/w/pueue.yml"),
    (["pueued", "-d"], None),
])
def test_the_daemon_config_argument(argv, expected):
    from agent_compute import authority
    assert authority.daemon_config(argv) == expected


def test_a_daemon_without_a_config_argument_is_not_an_acs_queue():
    """ACS always starts its daemon with ``-c``: one without is a user's
    own Pueue, listed and never an error."""
    from agent_compute import authority
    config = SimpleNamespace(queue_group="acs-heavy")
    verdict = authority.classify_daemon(
        {"pid": 4242, "argv": ["pueued", "-d"]}, config)
    assert verdict["problem"] is None and verdict["config"] is None


def test_an_unreadable_daemon_config_cannot_be_ruled_out(tmp_path):
    from agent_compute import authority
    config = SimpleNamespace(queue_group="acs-heavy")
    verdict = authority.classify_daemon(
        {"pid": 4243, "argv": ["pueued", "-c", str(tmp_path / "gone.yml")]},
        config)
    assert verdict["problem"], verdict


def test_the_scan_home_hook_is_honoured_only_in_test_mode(monkeypatch,
                                                          tmp_path):
    from agent_compute import authority
    monkeypatch.setenv(SCAN, str(tmp_path))
    production = authority.scan_scope(SimpleNamespace(test_mode=False))
    assert production["home"] == str(Path.home())
    assert production["processes"] == "all"
    sandbox = authority.scan_scope(SimpleNamespace(test_mode=True))
    assert sandbox["home"] == str(tmp_path)
    assert sandbox["processes"] == str(tmp_path)
    monkeypatch.delenv(SCAN)
    bare = authority.scan_scope(SimpleNamespace(test_mode=True))
    assert bare["home"] is None and bare["processes"] is None


# --- review round 1 (independent review of fb79c78) ------------------------------------------

needs_non_root = pytest.mark.skipif(
    os.geteuid() == 0,
    reason=skip_reason("root", "root ignores an unreadable directory"))


def loaded(env: Env):
    from agent_compute import config as cfgmod
    return cfgmod.load(env.config)


def test_unreadable_lock_users_are_an_error(env_direct, home, monkeypatch):
    """Finding 1: lock users that cannot be read (lsof failed) cannot be
    ruled out: an ERROR, never a pass."""
    from agent_compute import authority, procs
    legacy = home / ".smc-guard"
    legacy.mkdir()
    (legacy / "lock").write_text("")
    real = procs.lock_file_users

    def users(lock):
        return None if Path(lock).parent == legacy else real(lock)

    monkeypatch.setattr(procs, "lock_file_users", users)
    monkeypatch.setenv(SCAN, str(home))
    guard = authority.single_authority(loaded(env_direct))["guard_home"]
    assert guard["ok"] is False
    hit = [s for s in guard["scanned"] if s["guard_home"] == str(legacy)]
    assert hit and hit[0]["lock_users"] is None and hit[0]["problem"]
    assert "cannot be read" in hit[0]["problem"]


def test_an_unreadable_process_table_is_an_error(env_direct, home,
                                                 monkeypatch):
    """Finding 1: no process table, no proof of a single daemon."""
    from agent_compute import authority, procs

    def fail():
        raise procs.ProcCheckError("ps exited 2")

    monkeypatch.setattr(authority, "list_pueued", fail)
    monkeypatch.setenv(SCAN, str(home))
    queue = authority.single_authority(loaded(env_direct))["queue_daemon"]
    assert queue["ok"] is False
    assert "process table" in json.dumps(queue["problems"])


def test_two_daemons_on_the_configured_queue_are_an_error(env_direct, home,
                                                         monkeypatch):
    """Finding 1: two daemons on our own pueue.yml."""
    from agent_compute import authority, queue as q
    config = loaded(env_direct)
    mine = str(q.pueue_config_path(config))
    monkeypatch.setattr(authority, "list_pueued", lambda: [
        {"pid": 70001, "argv": ["pueued", "-c", mine]},
        {"pid": 70002, "argv": ["pueued", "--config", mine, "-d"]}])
    monkeypatch.setenv(SCAN, str(home))
    queue = authority.single_authority(config)["queue_daemon"]
    assert queue["ours"] == [70001, 70002]
    assert queue["ok"] is False and "70001" in json.dumps(queue["problems"])


def test_the_homes_recorded_in_guard_home_json_are_scanned(env_direct,
                                                          sleepers):
    """Finding 1: the previous home recorded in <state>/guard-home.json is a
    known location (no scan home needed: it is the config's own record)."""
    env = env_direct
    previous = env.root / "previous-home"
    holder = sleepers(previous / "lock")
    env.state.mkdir(parents=True, exist_ok=True)
    (env.state / "guard-home.json").write_text(json.dumps(
        {"guard_home": str(env.guard_home), "previous": str(previous)}))
    code, report = doctor(env)
    guard = sub(report, "guard_home")
    assert guard["ok"] is False and code == 1, guard
    hit = [s for s in guard["scanned"] if s["guard_home"] == str(previous)]
    assert hit and holder.pid in hit[0]["lock_users"], guard["scanned"]
    assert "guard-home.json" in hit[0]["source"]


def test_an_unreadable_guard_home_record_is_an_error(env_direct):
    """Finding 4 (C4): an unreadable guard-home.json is never read as an
    absent one: the homes it records cannot be ruled out."""
    env = env_direct
    env.state.mkdir(parents=True, exist_ok=True)
    (env.state / "guard-home.json").write_text("{not json")
    code, report = doctor(env)
    guard = sub(report, "guard_home")
    assert guard["ok"] is False and code == 1
    assert "guard-home.json" in json.dumps(guard["problems"])


@needs_non_root
def test_an_unreadable_scanned_location_is_an_error_row_not_a_crash(
        env_direct, home):
    """Finding 2: a known location that cannot be searched (mode 000) is
    reported as a second authority that cannot be ruled out; doctor keeps
    every other check and its JSON output."""
    legacy = home / ".smc-guard"
    legacy.mkdir()
    (legacy / "jobs").mkdir()
    legacy.chmod(0)
    try:
        result = env_direct.acs("doctor", "--json",
                                env=env_direct.env(**{SCAN: str(home)}))
        assert result.stdout, result.stderr
        report = json.loads(result.stdout)
        assert result.returncode == 1 and report["ok"] is False
        assert others_ok(report), report["checks"]
        names = {c["check"] for c in report["checks"]}
        assert {"config", "host id", "dispatch gate open"} <= names
        for name, key in (("guard_home", "guard_home"),
                          ("state_dir", "state_dir")):
            part = sub(report, name)
            assert part["ok"] is False, part
            assert row(report, name)["result"] == "fail"
            # reported at the location itself, the rest of the scan kept
            hit = [s for s in part["scanned"] if s[key] == str(legacy)]
            assert hit and hit[0]["problem"], part
            assert "cannot be read" in hit[0]["problem"]
            assert "PermissionError" in hit[0]["problem"]
        human = env_direct.acs("doctor",
                               env=env_direct.env(**{SCAN: str(home)}))
        assert "PROBLEMS FOUND" in human.stdout, human.stderr
        assert "Traceback" not in human.stderr
    finally:
        legacy.chmod(0o700)


def fake_ps(table: dict):
    """A stand-in for ``procs._ps``: ``-axww -o pid=,comm=`` lists the
    processes; ``-ww -o args= -p PID`` gives one command line."""
    def ps(args, local=False):
        if "-p" in args:
            pid = int(args[args.index("-p") + 1])
            return table[pid][1] + "\n" if pid in table else ""
        return "".join(f"{pid:>6} {comm}\n"
                       for pid, (comm, _) in table.items())
    return ps


def test_macos_ps_keeps_paths_with_spaces(monkeypatch, tmp_path):
    """Finding 3: on macOS the command line is not split on spaces: a
    pueued binary and a config under paths with spaces are found whole."""
    from agent_compute import authority, procs
    tools = "/Applications/My Tools/pueued"
    ours = tmp_path / "Application Support" / "s" / "pueue" / "pueue.yml"
    ours.parent.mkdir(parents=True)
    ours.write_text("shared:\n")
    other = tmp_path / "other place" / "pueue.yml"
    other.parent.mkdir(parents=True)
    other.write_text("shared:\n")
    monkeypatch.setattr(procs, "_ps", fake_ps({
        501: ("/opt/homebrew/bin/pueued",
              f"/opt/homebrew/bin/pueued --config {ours}"),
        502: (tools, f"{tools} -c {other} -d"),
        503: ("/bin/zsh", "/bin/zsh -c echo pueued -c /x"),
    }))
    found = {d["pid"]: d for d in authority.list_pueued_ps()}
    assert sorted(found) == [501, 502]
    assert found[502]["exe"] == tools
    assert found[502]["tail"] == f" -c {other} -d"
    assert found[501]["tail"] == f" --config {ours}"
    assert authority.daemon_config_of(found[501], str(ours)) == str(ours)
    assert authority.daemon_config_of(found[502], str(ours)) == str(other)
    config = SimpleNamespace(queue_group="acs-heavy")
    assert authority.classify_daemon(found[502], config)["config"] == \
        str(other)
