"""Host process identity (slice 6, audit findings A1, A2 and A9).

A1: a live host leader was called "pid recycled" -> clean whenever its
start discriminator mismatched: the macOS ``ps lstart`` string is rendered
in the checker's TZ, an epoch-only identity had a 3 s tolerance, the DST
fall-back hour made ``mktime`` pick the wrong reading, and the runner kept
the exact start only if its first ``identity()`` call matched.
A2: ``Runner.dead_jobs`` treated liveness ``unknown`` as "proceed".
A9: ``detect_lost`` blocked a job whose runner finished during the check.

Every test here failed first on ``main`` = ``7d6663d`` (except those named
``test_keep_*``, which pin behaviour the repair must keep); see
the private development evidence.  Only bare test
processes (killed in ``finally``) and the isolated harness are used.
"""

from __future__ import annotations

import calendar
import json
import os
import signal
import subprocess
import sys
import time
import types
from pathlib import Path

import pytest

from agent_compute import cli, procs
from agent_compute.runner import Runner, workload_from_record
from agent_compute.state import JobStore, write_json
from conftest import REPO, Env, QuietEnv

MAC = sys.platform == "darwin"
needs_mac = pytest.mark.skipif(
    not MAC, reason="[platform] the lstart form is macOS-only")
SRC = str(REPO / "src")


def exact(ident) -> bool:
    """An exact recorded start: the old ``start`` string or the new
    ``start_utc_s`` (kept independent of the code under test)."""
    ident = ident or {}
    return bool(ident.get("start")) or ident.get("start_utc_s") is not None


def sleeper(seconds: int = 120) -> subprocess.Popen:
    proc = subprocess.Popen([sys.executable, "-c",
                             f"import time; time.sleep({seconds})"],
                            start_new_session=True)
    time.sleep(0.5)
    return proc


def kill(proc: subprocess.Popen) -> None:
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except OSError:
        pass
    proc.wait(timeout=30)


def no_tz_env(**extra) -> dict:
    env = QuietEnv((k, v) for k, v in os.environ.items() if k != "TZ")
    env.update(extra)
    return env


CHECK = r'''
import json, sys
sys.path.insert(0, sys.argv[1])
from agent_compute import procs
w = json.loads(sys.argv[2])
print(json.dumps({"liveness": procs.liveness(w),
                  "survivors": procs.host_survivors(w)}))
'''

RECORD = r'''
import json, sys
sys.path.insert(0, sys.argv[1])
from agent_compute import procs
print(json.dumps(procs.identity(int(sys.argv[2]))))
'''

# What a 0.1.0-eb369eb937 runner recorded on macOS: ``ps lstart`` rendered
# in its own environment (the scrubbed queue env: no TZ, so the system
# zone), and ``time.mktime`` of that string.
LEGACY_RECORD = r'''
import json, os, subprocess, sys, time
pid = int(sys.argv[1])
out = subprocess.run(["ps", "-o", "stat=,lstart=", "-p", str(pid)],
                     capture_output=True, text=True).stdout.strip()
stat, _, lstart = out.partition(" ")
lstart = " ".join(lstart.split())
epoch = time.mktime(time.strptime(lstart, "%a %b %d %H:%M:%S %Y"))
print(json.dumps({"pid": pid, "start": "lstart:" + lstart,
                  "start_epoch": epoch, "pgid": os.getpgid(pid),
                  "sid": os.getsid(pid)}))
'''


def check(workload: dict, tz: str | None) -> dict:
    env = no_tz_env()
    if tz:
        env["TZ"] = tz
    out = subprocess.run([sys.executable, "-c", CHECK, SRC,
                          json.dumps(workload)],
                         env=env, capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


# -- e1: epoch-only identities ------------------------------------------------------

@pytest.mark.parametrize("offset", [0, 2, 4, 6, 3600])
def test_e1_epoch_only_identity_of_a_live_leader_is_never_clean(offset):
    """reconcile's ``workload_from_record`` (and the runner's capture
    fallback) has only the guard's ``started_utc``.  Off by more than 3 s it
    used to say "leader pid recycled" -> clean for a LIVE leader."""
    leader = sleeper()
    try:
        epoch = procs.start_info(leader.pid)[1]
        started = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                time.gmtime(epoch - offset))
        workload = workload_from_record({"container": None},
                                        {"pid": leader.pid,
                                         "started_utc": started})
        evidence = procs.host_survivors(workload)
        assert leader.poll() is None
        assert evidence["clean"] is False, evidence
        assert "recycled" not in json.dumps(evidence)
        # an inexact (epoch-only) match never justifies a signal
        assert not evidence.get("signalable"), evidence
        assert procs.signal_host_group(workload, 0) is False
    finally:
        kill(leader)


# -- e2: the exact start, checked from another TZ -----------------------------------

@pytest.mark.parametrize("tz", [None, "UTC", "Europe/Paris"])
def test_e2_exact_identity_is_alive_from_any_tz(tz):
    leader = sleeper()
    try:
        out = subprocess.run([sys.executable, "-c", RECORD, SRC,
                              str(leader.pid)], env=no_tz_env(),
                             capture_output=True, text=True, timeout=60)
        workload = {**json.loads(out.stdout), "venue": "host"}
        result = check(workload, tz)
        assert result["liveness"] == procs.ALIVE, result
        ev = result["survivors"]
        assert ev["leader_alive"] is True and ev["clean"] is False, ev
        assert ev["signalable"] is True, ev
        assert "recycled" not in json.dumps(ev)
    finally:
        kill(leader)


@needs_mac
@pytest.mark.parametrize("tz", [None, "UTC", "Europe/Paris"])
def test_e2_legacy_eb369eb937_identity_is_alive_from_any_tz(tz):
    """Mixed versions: a runner of the deployed 0.1.0-eb369eb937 recorded a
    local-time ``lstart:`` string; a newer reconcile run with any TZ must
    still see the live leader (never "recycled")."""
    leader = sleeper()
    try:
        out = subprocess.run([sys.executable, "-c", LEGACY_RECORD,
                              str(leader.pid)],
                             env={k: os.environ[k] for k in
                                  ("PATH", "HOME", "LANG", "USER", "LOGNAME")
                                  if k in os.environ},
                             capture_output=True, text=True, timeout=60)
        legacy = json.loads(out.stdout)
        assert legacy["start"].startswith("lstart:")
        workload = {**legacy, "venue": "host"}
        result = check(workload, tz)
        assert result["liveness"] == procs.ALIVE, result
        ev = result["survivors"]
        assert ev["leader_alive"] is True and ev["clean"] is False, ev
        assert "recycled" not in json.dumps(ev)
        # a legacy start_epoch that is an hour off (the DST mktime pick)
        # is still recognised through the local rendering, never recycled
        shifted = {**workload, "start_epoch": legacy["start_epoch"] + 3600}
        result = check(shifted, tz)
        assert result["survivors"]["clean"] is False, result
        assert "recycled" not in json.dumps(result)
    finally:
        kill(leader)


# -- recycled pids stay clean with an exact recorded start ---------------------------

def _earlier_exact(ident: dict, seconds: int) -> dict:
    """The same identity, as if recorded for a process that started
    ``seconds`` earlier at the same pid."""
    moved = dict(ident)
    if ident.get("start_utc_s") is not None:
        moved["start_utc_s"] = ident["start_utc_s"] - seconds
    elif str(ident.get("start")).startswith("ticks:"):
        ticks = int(ident["start"][6:]) - seconds * os.sysconf("SC_CLK_TCK")
        moved["start"] = f"ticks:{ticks}"
    elif str(ident.get("start")).startswith("lstart:"):     # pre-fix form
        fmt = "%a %b %d %H:%M:%S %Y"
        epoch = time.mktime(time.strptime(ident["start"][7:], fmt)) - seconds
        moved["start"] = "lstart:" + " ".join(
            time.strftime("%a %b %e %H:%M:%S %Y",
                          time.localtime(epoch)).split())
    else:
        raise AssertionError(f"no exact start in {ident}")
    moved["start_epoch"] = ident["start_epoch"] - seconds
    return moved


def test_keep_recycled_pid_with_exact_start_is_clean():
    """A process that provably started after the recorded leader holds its
    pid: the recorded group is gone (POSIX: a pid is not reused while its
    group exists), so this is clean -- and the holder is never signalled."""
    holder = sleeper()
    try:
        recorded = {**_earlier_exact(procs.identity(holder.pid), 600),
                    "venue": "host"}
        ev = procs.host_survivors(recorded)
        assert ev["verified"] is True and ev["clean"] is True, ev
        assert ev["members"] == [] and ev["leader_alive"] is False
        assert "recycled" in ev["note"]
        assert procs.liveness(recorded) == procs.GONE
        assert procs.signal_host_group(recorded, signal.SIGKILL) is False
        assert holder.poll() is None
    finally:
        kill(holder)


def test_keep_exact_start_mismatch_in_the_wrong_direction_is_unverified():
    """The process at the pid started BEFORE the recorded leader (a clock
    step, a reboot, a corrupt record): nothing proves it is a later reuse,
    so it is unverified, never clean."""
    holder = sleeper()
    try:
        recorded = {**_earlier_exact(procs.identity(holder.pid), -600),
                    "venue": "host"}
        ev = procs.host_survivors(recorded)
        assert ev["verified"] is False and ev["clean"] is False, ev
        assert procs.liveness(recorded) == procs.UNKNOWN
        assert procs.signal_host_group(recorded, signal.SIGKILL) is False
        assert holder.poll() is None
    finally:
        kill(holder)


@needs_mac
def test_legacy_lstart_recycled_only_beyond_any_utc_offset():
    """A legacy (local-time) record whose process at the pid started more
    than any real UTC offset (+12 h) after the recorded wall time is a
    recycled pid, whatever zone the string was rendered in."""
    holder = sleeper()
    try:
        epoch = procs.identity(holder.pid)["start_epoch"] - 2 * 86400
        render = subprocess.run(
            [sys.executable, "-c", "import sys, time; print(' '.join("
             "time.strftime('%a %b %e %H:%M:%S %Y', "
             "time.localtime(float(sys.argv[1]))).split()))", str(epoch)],
            env=no_tz_env(), capture_output=True, text=True).stdout.strip()
        legacy = {"venue": "host", "pid": holder.pid,
                  "start": "lstart:" + render, "start_epoch": epoch}
        for tz in (None, "UTC", "Pacific/Kiritimati", "Etc/GMT+12"):
            ev = check(legacy, tz)["survivors"]
            assert ev["clean"] is True and "recycled" in ev["note"], (tz, ev)
        # one hour later is inside the zone uncertainty: unverified
        near = dict(legacy, start_epoch=procs.identity(holder.pid)
                    ["start_epoch"] - 3600)
        near["start"] = "lstart:" + subprocess.run(
            [sys.executable, "-c", "import sys, time; print(' '.join("
             "time.strftime('%a %b %e %H:%M:%S %Y', "
             "time.localtime(float(sys.argv[1]))).split()))",
             str(near["start_epoch"])],
            env=no_tz_env(), capture_output=True, text=True).stdout.strip()
        ev = check(near, "UTC")["survivors"]
        assert ev["clean"] is False and ev["verified"] is False, ev
        assert holder.poll() is None
    finally:
        kill(holder)


# -- e4: the DST-ambiguous hour, by injected ps output -------------------------------

# 2026-11-01 01:30 in America/Los_Angeles happens twice: 08:30Z (PDT) and
# 09:30Z (PST).  The process below truly started at 08:30:00Z.
TRUE_START = 1793521800
PST_READING = 1793525400
SYSTEM_TZ = "America/Los_Angeles"


class FakePs:
    """Stands in for /bin/ps: renders the fake process's start the way ps
    does, in the TZ of the environment ps is given (the inherited one when
    none is passed), and records every environment it was given."""

    def __init__(self, pid: int, start: int):
        self.pid, self.start, self.envs = pid, start, []

    def __call__(self, argv, **kwargs):
        env = kwargs.get("env")
        self.envs.append(env)
        tz = (env if env is not None else os.environ).get("TZ") or \
            SYSTEM_TZ                       # TZ unset: the system zone
        saved = os.environ.get("TZ")
        try:
            if tz is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = tz
            time.tzset()
            lstart = time.strftime("%a %b %e %H:%M:%S %Y",
                                   time.localtime(self.start))
        finally:
            if saved is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = saved
            time.tzset()
        if argv[:2] == ["ps", "-A"]:
            out = f"S {self.pid} {self.pid}\n"
        elif str(self.pid) in argv:
            out = f"S    {lstart}\n"
        else:
            out = ""
        return subprocess.CompletedProcess(argv, 0, out, "")


@pytest.fixture
def los_angeles(monkeypatch):
    """The 'system' zone is America/Los_Angeles (TZ unset = that zone)."""
    monkeypatch.setenv("TZ", SYSTEM_TZ)
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


@pytest.mark.skipif(not MAC, reason="[platform] exercises the macOS ps path")
def test_e4_start_epoch_is_exact_in_the_dst_fall_back_hour(monkeypatch,
                                                           los_angeles):
    fake = FakePs(os.getpid(), TRUE_START)
    monkeypatch.setattr(procs.subprocess, "run", fake)
    assert procs.start_info(os.getpid())[1] == TRUE_START
    for env in fake.envs:
        assert env is not None and env.get("TZ") == "UTC0" and \
            env.get("LC_ALL") == "C" and env.get("PATH"), env
    ident = procs.identity(os.getpid())
    assert ident["start_epoch"] == TRUE_START
    assert procs.liveness(ident) == procs.ALIVE
    # the checker's own TZ does not matter any more
    monkeypatch.setenv("TZ", "UTC")
    time.tzset()
    assert procs.liveness(ident) == procs.ALIVE


@pytest.mark.skipif(not MAC, reason="[platform] exercises the macOS ps path")
def test_e4_legacy_dst_record_is_alive_and_never_recycled(monkeypatch,
                                                          los_angeles):
    """An eb369eb937 record made in the ambiguous hour: its string is the
    local rendering, its epoch is mktime's PST pick (an hour late)."""
    fake = FakePs(os.getpid(), TRUE_START)
    monkeypatch.setattr(procs.subprocess, "run", fake)
    legacy = {"venue": "host", "pid": os.getpid(),
              "start": "lstart:Sun Nov 1 01:30:00 2026",
              "start_epoch": float(PST_READING)}
    monkeypatch.setenv("TZ", "UTC")       # the checker's session sets TZ
    time.tzset()
    assert procs.liveness(legacy) == procs.ALIVE
    ev = procs.host_survivors(legacy)
    assert ev["clean"] is False and ev["leader_alive"] is True, ev
    # a different process at the pid, 30 min after the recorded wall time:
    # inside the zone uncertainty of a local string -> unverified.  (One
    # that started exactly 3600 s later renders to the same local string in
    # the repeated hour; like the pre-fix comparison, the string cannot
    # tell those two apart.)
    later = FakePs(os.getpid(), TRUE_START + 1800)
    monkeypatch.setattr(procs.subprocess, "run", later)
    legacy_pdt = dict(legacy, start_epoch=float(TRUE_START))
    ev = procs.host_survivors(legacy_pdt)
    assert ev["clean"] is False and ev["verified"] is False, ev


@pytest.mark.skipif(not MAC, reason="[platform] exercises the macOS ps path")
def test_e4_capture_keeps_the_exact_start_in_the_dst_hour(monkeypatch,
                                                          los_angeles,
                                                          tmp_path):
    fake = FakePs(os.getpid(), TRUE_START)
    monkeypatch.setattr(procs.subprocess, "run", fake)
    runner = _bare_runner(tmp_path)
    attempt = tmp_path / "attempt"
    attempt.mkdir()
    write_json(attempt / "guard.json", {"pid": os.getpid(),
                                        "started_utc": "2026-11-01T08:30:00Z",
                                        "job_id": "case/x"})
    runner.capture_workload(attempt)
    assert exact(runner.workload), runner.workload


# -- capture retries until it holds an exact start -----------------------------------

def _bare_runner(tmp_path: Path) -> Runner:
    runner = Runner.__new__(Runner)
    runner.store = JobStore(tmp_path / "state")
    runner.job_id = "acs-capture"
    runner.dir = runner.store.job_dir(runner.job_id)
    runner.dir.mkdir(parents=True)
    runner.manifest = {"host": "h", "container": None}
    runner.host, runner.workload, runner.guard_proc = "h", None, None
    runner.config = types.SimpleNamespace(state_dir=tmp_path / "state")
    return runner


def test_capture_retries_until_an_exact_start_is_recorded(monkeypatch,
                                                          tmp_path):
    leader = sleeper()
    try:
        runner = _bare_runner(tmp_path)
        attempt = tmp_path / "attempt"
        attempt.mkdir()
        started = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                time.gmtime(procs.start_info(leader.pid)[1]))
        write_json(attempt / "guard.json", {"pid": leader.pid,
                                            "started_utc": started,
                                            "job_id": "case/x"})
        real = procs.identity
        calls = []

        def flaky(pid):
            calls.append(pid)
            if len(calls) == 1:           # ps failed on the first poll
                return {"pid": pid, "start": None, "start_epoch": None,
                        "note": "start time unavailable"}
            return real(pid)

        monkeypatch.setattr(procs, "identity", flaky)
        runner.capture_workload(attempt)
        assert runner.workload and not exact(runner.workload)
        runner.capture_workload(attempt)            # the next poll
        assert exact(runner.workload), runner.workload
        state = runner.store.state(runner.job_id)
        assert exact(state["workload"]), state
        assert procs.host_survivors(runner.workload)["signalable"] is True
    finally:
        kill(leader)


def test_capture_never_records_a_start_that_does_not_match_the_guard(
        tmp_path):
    """The pid the guard recorded is held by a process that started long
    before the guard's ``started_utc``: never record its start."""
    other = sleeper()
    try:
        runner = _bare_runner(tmp_path)
        attempt = tmp_path / "attempt"
        attempt.mkdir()
        started = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                time.gmtime(time.time() + 600))
        write_json(attempt / "guard.json", {"pid": other.pid,
                                            "started_utc": started})
        for _ in range(3):
            runner.capture_workload(attempt)
        assert not exact(runner.workload), runner.workload
        assert procs.host_survivors(runner.workload)["clean"] is False
    finally:
        kill(other)


# -- e3: end to end, reconcile from a TZ=UTC session ---------------------------------

@pytest.mark.skipif(not MAC, reason="[platform] the TZ-rendered discriminator is macOS")
def test_e3_reconcile_with_tz_keeps_the_block_of_a_live_workload():
    env = Env(queue=False)
    try:
        base = {k: v for k, v in env.env().items() if k != "TZ"}
        job = env.submit(workload_args=["--out", str(env.root / "w.jsonl"),
                                        "--sleep", "90"], env=base)
        st = env.wait_phase(job["job_id"], "executing")
        # the runner captures the exact start (on a later poll if needed)
        st = env.wait_for(lambda: (lambda s: s if exact(
            s.get("workload") or {}) else None)(env.state_of(job["job_id"])),
            what="exact workload start")
        wpid = st["workload"]["pid"]
        os.kill(st["runner"]["pid"], signal.SIGKILL)
        os.kill(st["guard"]["pid"], signal.SIGKILL)
        time.sleep(1)
        for tz in ("UTC", "Europe/Paris", None):
            e = dict(base, **({"TZ": tz} if tz else {}))
            rec = json.loads(env.acs("reconcile", "--json", env=e).stdout)
            assert rec["blocked"] is True, (tz, rec)
            os.kill(wpid, 0)
        nxt_out = env.root / "n.jsonl"
        nxt = env.submit("beta", workload_args=["--out", str(nxt_out)],
                         env=base)
        fin = env.wait_outcome(nxt["job_id"])
        assert fin["outcome"] == "refused", fin
        assert not nxt_out.exists()
        os.kill(wpid, 0)
        # the exact-start leader is signalable from a TZ=UTC session
        rec = json.loads(env.acs("reconcile", "--terminate", job["job_id"],
                                 "--json", env=dict(base, TZ="UTC")).stdout)
        assert rec["blocked"] is False, rec
        with pytest.raises(ProcessLookupError):
            for _ in range(20):
                os.kill(wpid, 0)
                time.sleep(0.5)
    finally:
        env.cleanup()


# -- A2: liveness unknown never proceeds to admission --------------------------------

def _other_job(store: JobStore, job_id: str, runner: dict) -> None:
    write_json(store.job_dir(job_id) / "manifest.json",
               {"host": "testhost", "job_id": job_id})
    store.update_state(job_id, phase="executing", runner=runner)


def _clone(env: Env, src: str, new: str, wait_s: float) -> None:
    man = json.loads((env.state / "jobs" / src / "manifest.json").read_text())
    man.update(job_id=new, admission_wait_s=wait_s)
    write_json(env.state / "jobs" / new / "manifest.json", man)


class Reached(Exception):
    pass


def _run_in_process(env: Env, job_id: str) -> int:
    from agent_compute.config import load
    runner = Runner(load(env.config), job_id)
    handlers = {s: signal.getsignal(s) for s in (signal.SIGTERM,
                                                 signal.SIGINT)}
    mask = os.umask(0o077)
    os.umask(mask)
    try:
        return runner.run()
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
        os.umask(mask)
        if runner.admission_fd is not None:
            os.close(runner.admission_fd)
        # the runner identity recorded here is pytest's own process: drop
        # it so the harness teardown never signals it
        runner.store.update_state(job_id, runner={
            "pid": None, "note": "in-process test runner"})


@pytest.fixture
def one_finished_job(env_direct):
    env = env_direct
    job = env.submit(workload_args=["--out", str(env.root / "first")])
    assert env.wait_outcome(job["job_id"])["outcome"] == "succeeded"
    # restore the operand state fingerprinted at submission, so a clone of
    # this manifest passes the inputs re-check
    (env.root / "first").unlink()
    return env, job["job_id"]


def test_a2_ps_failure_on_a_prior_runner_does_not_proceed(one_finished_job,
                                                          monkeypatch):
    env, first = one_finished_job
    store = JobStore(env.state)
    bystander = sleeper()
    try:
        _other_job(store, "acs-a2-other", procs.identity(bystander.pid))
        _clone(env, first, "acs-a2-new", 3.0)
        real = procs.start_info

        def failing(pid):
            if pid and int(pid) == bystander.pid:
                raise procs.ProcCheckError("ps exited 137")
            return real(pid)

        def guard_called(*_a, **_k):
            raise Reached("the guard was invoked")

        monkeypatch.setattr(procs, "start_info", failing)
        monkeypatch.setattr(Runner, "run_guard_attempt", guard_called)
        rc = _run_in_process(env, "acs-a2-new")
        state = store.state("acs-a2-new")
        assert state["outcome"] == "admission-timeout", state
        assert "never started" in state["reason"]
        assert "acs-a2-other" in state["reason"], state["reason"]
        assert rc == 12
        # at the deadline the unverifiable prior job is blocked durably, so
        # reconcile lists it (review R4)
        assert list(store.block("testhost")["jobs"]) == ["acs-a2-other"]
    finally:
        kill(bystander)


def test_a2_startless_runner_on_a_live_pid_does_not_proceed(env_direct):
    """End to end: an unfinished job whose runner identity has no start and
    whose pid is held by a live process (liveness unknown).  The next job
    must not reach the guard."""
    env = env_direct
    store = JobStore(env.state)
    bystander = sleeper()
    try:
        _other_job(store, "acs-a2-startless",
                   {"pid": bystander.pid, "start": None, "start_epoch": None,
                    "note": "start time unavailable"})
        out = env.root / "next.jsonl"
        job = env.submit(workload_args=["--out", str(out)],
                         extra=("--lock-wait", "4"))
        state = env.wait_outcome(job["job_id"], timeout=60)
        assert state["outcome"] == "admission-timeout", state
        assert "acs-a2-startless" in state["reason"], state
        assert not out.exists()
        assert bystander.poll() is None
        rec = json.loads(env.acs("reconcile", "--json").stdout)
        assert rec["blocked"] is True
        assert [(u["job_id"], u["runner"]) for u in rec["unresolved"]] == \
            [("acs-a2-startless", procs.UNKNOWN)], rec
    finally:
        kill(bystander)


# -- A9: a runner that finishes during a lost-supervision check ----------------------

def test_a9_detect_lost_does_not_block_a_job_that_just_finished(tmp_path,
                                                               monkeypatch):
    store = JobStore(tmp_path)
    runner = {"pid": 999998, "start": "ticks:1", "start_epoch": 1.0}
    _other_job(store, "acs-fin", runner)
    cfg = types.SimpleNamespace(state_dir=tmp_path, host="testhost",
                                queue_enabled=False)

    def finishing(ident):
        # the runner writes its outcome and exits during the ps call
        store.update_state("acs-fin", phase="done", outcome="succeeded")
        return procs.GONE

    monkeypatch.setattr(procs, "liveness", finishing)
    assert cli.detect_lost(cfg) == []
    assert store.block("testhost") is None
    assert store.state("acs-fin")["outcome"] == "succeeded"


def test_a9_detect_lost_does_not_block_a_replaced_runner(tmp_path,
                                                        monkeypatch):
    store = JobStore(tmp_path)
    old = {"pid": 999998, "start": "ticks:1", "start_epoch": 1.0}
    _other_job(store, "acs-swap", old)
    cfg = types.SimpleNamespace(state_dir=tmp_path, host="testhost",
                                queue_enabled=False)

    def swapped(ident):
        store.update_state("acs-swap", runner={"pid": 999997,
                                               "start": "ticks:2"})
        return procs.GONE

    monkeypatch.setattr(procs, "liveness", swapped)
    assert cli.detect_lost(cfg) == []
    assert store.block("testhost") is None


def test_keep_detect_lost_blocks_a_really_lost_job(tmp_path, monkeypatch):
    store = JobStore(tmp_path)
    _other_job(store, "acs-lost", {"pid": 999998, "start": "ticks:1"})
    cfg = types.SimpleNamespace(state_dir=tmp_path, host="testhost",
                                queue_enabled=False)
    monkeypatch.setattr(procs, "liveness", lambda ident: procs.GONE)
    assert cli.detect_lost(cfg) == ["acs-lost"]
    assert list(store.block("testhost")["jobs"]) == ["acs-lost"]


def test_a9_admission_does_not_block_a_prior_job_that_just_finished(
        one_finished_job, monkeypatch):
    """The runner's own pre-admission lost-supervision check has the same
    read-then-block shape as ``detect_lost``."""
    env, first = one_finished_job
    store = JobStore(env.state)
    bystander = sleeper()
    try:
        ident = procs.identity(bystander.pid)
        _other_job(store, "acs-a9-other", ident)
        _clone(env, first, "acs-a9-new", 30.0)
        real = procs.liveness

        def finishing(recorded):
            if recorded and recorded.get("pid") == bystander.pid:
                store.update_state("acs-a9-other", phase="done",
                                   outcome="cancelled")
                return procs.GONE
            return real(recorded)

        def guard_called(*_a, **_k):
            raise Reached("the guard was invoked")

        monkeypatch.setattr(procs, "liveness", finishing)
        monkeypatch.setattr(Runner, "run_guard_attempt", guard_called)
        with pytest.raises(Reached):
            _run_in_process(env, "acs-a9-new")
        assert store.block("testhost") is None
    finally:
        kill(bystander)


def test_keep_admission_blocks_a_really_lost_prior_job(one_finished_job):
    env, first = one_finished_job
    store = JobStore(env.state)
    gone = sleeper()
    ident = procs.identity(gone.pid)
    kill(gone)
    for _ in range(50):
        if procs.start_info(gone.pid) is None:
            break
        time.sleep(0.1)
    _other_job(store, "acs-lost-other", ident)
    _clone(env, first, "acs-lost-new", 30.0)
    rc = _run_in_process(env, "acs-lost-new")
    state = store.state("acs-lost-new")
    assert state["outcome"] == "refused" and rc == 11, state
    assert "acs-lost-other" in store.block("testhost")["jobs"]


# -- the comparison rules, by injected process-table readings ------------------------
#
# These exercise both platforms' branches on either host: ``start_info``
# (and the boot facts) are injected, the rules are pure.

HZ = os.sysconf("SC_CLK_TCK")


@pytest.mark.parametrize("recorded, now, boot, expected", [
    # same boot (boot_id recorded by the fixed runner)
    ({"start": "ticks:5000", "boot_id": "b1"}, 5000, "b1", procs.ALIVE),
    ({"start": "ticks:5000", "boot_id": "b1"}, 5000 + 10 * HZ, "b1",
     procs.RECYCLED),
    ({"start": "ticks:5000", "boot_id": "b1"}, 5000 + HZ, "b1",
     procs.UNKNOWN),                        # within tolerance
    ({"start": "ticks:5000", "boot_id": "b1"}, 4000, "b1", procs.UNKNOWN),
    # a later boot: ticks restarted
    ({"start": "ticks:5000", "boot_id": "b1"}, 4000, "b2", procs.RECYCLED),
    ({"start": "ticks:5000", "boot_id": "b1"}, 4000, None, procs.UNKNOWN),
    # pre-boot_id records: fewer ticks count as a later boot only if the
    # recorded start is before this boot
    ({"start": "ticks:5000", "start_epoch": 1000.0}, 4000, "b2",
     procs.RECYCLED),
    ({"start": "ticks:5000", "start_epoch": 9000.0}, 4000, "b2",
     procs.UNKNOWN),
    ({"start": "ticks:5000", "start_epoch": 1000.0}, 5000 + 10 * HZ, "b2",
     procs.RECYCLED),
    # C2 (G1): equal ticks prove nothing across boots; the boot id decides
    # before tick equality does
    ({"start": "ticks:5000", "boot_id": "b1"}, 5000, "b2", procs.RECYCLED),
    ({"start": "ticks:5000", "boot_id": "b1"}, 5000, None, procs.UNKNOWN),
    # ... and a record without a boot id (legacy, or unreadable at capture)
    # cannot rule out a later boot
    ({"start": "ticks:5000", "start_epoch": 1000.0}, 5000, "b2",
     procs.UNKNOWN),
    ({"start": "ticks:5000"}, 5000, "b2", procs.UNKNOWN),
    ({"start": "ticks:5000", "boot_id": None}, 5000, "b1", procs.UNKNOWN),
    # macOS UTC seconds
    ({"start": None, "start_utc_s": 1000, "start_epoch": 1000.0}, "utc:1000",
     None, procs.ALIVE),
    ({"start": None, "start_utc_s": 1000, "start_epoch": 1000.0}, "utc:1002",
     None, procs.UNKNOWN),
    ({"start": None, "start_utc_s": 1000, "start_epoch": 1000.0}, "utc:1010",
     None, procs.RECYCLED),
    ({"start": None, "start_utc_s": 1000, "start_epoch": 1000.0}, "utc:990",
     None, procs.UNKNOWN),
    # a discriminator from the other platform, or none at all
    ({"start": None, "start_utc_s": 1000}, 5000, "b1", procs.UNKNOWN),
    ({"start": "ticks:5000"}, "utc:1000", None, procs.UNKNOWN),
    ({"start": "mystery:1"}, "utc:1000", None, procs.UNKNOWN),
    ({"start": None, "start_epoch": None}, "utc:1000", None, procs.UNKNOWN),
    # epoch-only: alive within 3 s, otherwise unknown, never recycled
    ({"start": None, "start_epoch": 998.0}, "utc:1000", None, procs.ALIVE),
    ({"start": None, "start_epoch": 900.0}, "utc:1000", None, procs.UNKNOWN),
    ({"start": None, "start_epoch": 1100.0}, "utc:1000", None, procs.UNKNOWN),
])
def test_match_rules(monkeypatch, recorded, now, boot, expected):
    if isinstance(now, int):
        info = (f"ticks:{now}", 5000.0 + now / HZ)
    else:
        info = (now, float(now[4:]))
    monkeypatch.setattr(procs, "start_info", lambda pid: info)
    monkeypatch.setattr(procs, "_boot_id", lambda: boot)
    monkeypatch.setattr(procs, "_linux_boot_epoch", lambda: 5000.0)
    ident = {"pid": 4242, **recorded}
    assert procs.match(ident) == expected
    assert procs.liveness(ident) == (procs.GONE if expected == procs.RECYCLED
                                     else expected)


# -- C2 (G1): no signal caller acts on a later-boot identity ------------------------
#
# Basis: the private development record (register item C2).
# The pid and the start ticks of a recorded runner, guard or workload are
# reused on a later boot (the same tick count): every caller must read it as
# gone (a different boot id) or unknown (no or unreadable boot id), never
# alive.

LATER_BOOT = {"pid": 4242, "start": "ticks:5000", "start_epoch": 50.0,
              "boot_id": "b1"}


@pytest.fixture
def later_boot(monkeypatch):
    """pid 4242 is live with the recorded tick count, on boot b2; every
    signal is recorded instead of sent."""
    sent = []
    monkeypatch.setattr(procs, "start_info",
                        lambda pid: ("ticks:5000", 5050.0)
                        if int(pid) == 4242 else None)
    monkeypatch.setattr(procs, "_boot_id", lambda: "b2")
    monkeypatch.setattr(procs, "group_members", lambda leader: [4242])
    monkeypatch.setattr(procs.os, "killpg",
                        lambda pid, sig: sent.append(("killpg", pid, sig)))
    monkeypatch.setattr(procs.os, "kill",
                        lambda pid, sig: sent.append(("kill", pid, sig)))
    return sent


@pytest.mark.parametrize("ident", [
    LATER_BOOT,
    {k: v for k, v in LATER_BOOT.items() if k != "boot_id"},   # legacy
], ids=["boot-id", "legacy"])
def test_c2_signal_host_group_never_signals_a_later_boot_identity(
        later_boot, ident):
    assert procs.liveness(ident) != procs.ALIVE
    assert procs.signal_host_group(dict(ident), 15) is False
    assert procs.signal_host_group(dict(ident), 9, {
        "workload_seen_utc": "2026-01-01T00:00:00Z",
        "workload_seen_ticks": 6000, "workload_seen_boot_id": "b1"}) is False
    assert later_boot == []


def test_c2_signal_host_group_unreadable_boot_signals_nothing(later_boot,
                                                              monkeypatch):
    monkeypatch.setattr(procs, "_boot_id", lambda: None)
    assert procs.match(LATER_BOOT) == procs.UNKNOWN
    assert procs.signal_host_group(dict(LATER_BOOT), 15) is False
    assert later_boot == []


def test_c2_cancel_job_never_signals_a_later_boot_runner(later_boot,
                                                         tmp_path):
    store = JobStore(tmp_path)
    _other_job(store, "acs-c2-cancel", dict(LATER_BOOT))
    cfg = types.SimpleNamespace(state_dir=tmp_path, host="testhost",
                                queue_enabled=False)
    result = cli.cancel_job(cfg, "acs-c2-cancel", wait_s=1.0)
    assert later_boot == [], result
    assert result["cancelled"] is False


def test_c2_terminate_never_signals_a_later_boot_guard_or_workload(
        later_boot):
    done = cli.terminate(None, None, dict(LATER_BOOT), dict(LATER_BOOT))
    assert later_boot == [], done
    assert "guard_term" not in done and "workload_term" not in done


def test_c2_detect_lost_blocks_a_runner_from_an_earlier_boot(later_boot,
                                                             tmp_path):
    store = JobStore(tmp_path)
    _other_job(store, "acs-c2-lost", dict(LATER_BOOT))
    cfg = types.SimpleNamespace(state_dir=tmp_path, host="testhost",
                                queue_enabled=False)
    assert cli.detect_lost(cfg) == ["acs-c2-lost"]
    assert list(store.block("testhost")["jobs"]) == ["acs-c2-lost"]


def test_c2_keep_same_boot_equal_ticks_is_alive(later_boot, monkeypatch):
    monkeypatch.setattr(procs, "_boot_id", lambda: "b1")
    assert procs.match(LATER_BOOT) == procs.ALIVE
    assert procs.signal_host_group(dict(LATER_BOOT), 15) is True
    assert later_boot == [("killpg", 4242, 15)]


def test_match_ps_failure_and_empty_pid(monkeypatch):
    def failing(pid):
        raise procs.ProcCheckError("ps exited 137")
    monkeypatch.setattr(procs, "start_info", failing)
    assert procs.match({"pid": 4242, "start": "ticks:1"}) == procs.UNKNOWN
    ev = procs.host_survivors({"pid": 4242, "start": "ticks:1"})
    assert ev["verified"] is False and ev["clean"] is False
    monkeypatch.setattr(procs, "start_info", lambda pid: None)
    assert procs.match({"pid": 4242, "start": "ticks:1"}) == procs.GONE
    assert procs.match({"pid": None}) == procs.GONE


def test_signal_gating_holds(monkeypatch):
    """``signal_host_group`` killpg's only an exact-start live leader;
    never a recycled or unmatched pid.  A group whose leader pid is free is
    never killpg'd (members are signalled one by one, only with a block
    time: tests/test_host_identity_review.py)."""
    sent = []
    monkeypatch.setattr(procs.os, "killpg", lambda pid, sig: sent.append(pid))
    monkeypatch.setattr(procs, "group_members", lambda leader, m=None: [7])
    exact = {"start_utc_s": 1000, "start_epoch": 1000.0}
    cases = [
        (("utc:1000", 1000.0), exact, True),                     # exact
        (("utc:1000", 1000.0), {"start_epoch": 999.0}, False),   # epoch-only
        (("utc:1010", 1010.0), exact, False),                    # recycled
        (("utc:1001", 1001.0), exact, False),                    # unknown
        (None, exact, False),         # leader gone, group left: no killpg
    ]
    for info, recorded, expected in cases:
        sent.clear()
        monkeypatch.setattr(procs, "start_info", lambda pid, i=info: i)
        assert procs.signal_host_group({"pid": 4242, **recorded}, 0) is \
            expected, (info, recorded)
        assert bool(sent) is expected


@needs_mac
def test_identity_record_shape_on_macos():
    """New macOS records keep ``start`` empty (a pre-fix reader then uses
    the TZ-independent ``start_epoch``) and carry ``start_utc_s``."""
    ident = procs.identity(os.getpid())
    assert ident["start"] is None
    assert ident["start_epoch"] == float(ident["start_utc_s"])
    assert abs(ident["start_utc_s"] - calendar.timegm(time.gmtime())) < \
        86400 * 30
