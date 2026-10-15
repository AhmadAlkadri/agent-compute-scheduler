"""Cleanup-evidence audit, slice 2 (2026-09-27): failing-first regressions.

``runner.terminal_without_captured_workload`` (used by
``Runner.verify_cleanup`` and ``cli.cmd_reconcile``) calls a container job
clean from the guard record ALONE whenever ``pid`` is set, ``container_pid``
is null and the outcome is anything but None/"running".  It justifies that
with "``setsid -w`` waits for the whole session".  Measured in disposable
containers (recorded in the private development evidence), that is
false:

* under ``docker exec`` the exec'd ``setsid`` is already a process-group
  leader, so it FORKS; ``-w`` then waits for its direct child (the
  pidfile's ``$$``) only, and ``docker exec`` returns when that child exits;
* a background descendant stays in the leader's session/pgrp and survives;
* with ``container_pid`` null the guard's kill paths (walltime, interrupt,
  budget, probe) signal only the host-side docker CLIENT, which leaves the
  in-container workload running.

Tests whose name starts with ``test_desired_`` encode the SAFE behavior;
they failed at slice 2 (``162eb32``) and pass since the slice-3 repair
(``procs.container_job_evidence``; the helper above is deleted).  ``test_baseline_`` and ``test_limitation_`` tests
pin present behavior that a repair must keep (or that is the documented
unsupported boundary).  Docker tests use only disposable ``acs-test-*``
containers created and removed here; the tests that force the
``container_pid``-missing path do it without editing the guard, with a PATH
shim in front of ``docker`` that fails only guard.py's ``docker exec <c> cat
/tmp/smc-guard-*.pid`` (the record shape of the fast-job race).  A read-only
root filesystem was tried first and rejected: the amd64 test image runs
under Rosetta on the arm64 Mac, and Rosetta needs a writable /tmp.

Since 0.2.0 (campaign G slice G2b) ACS runs only the internal guard; the
reproductions of guard.py's own gaps (a, d1, d2, f) run guard.py directly
in the legacy profile (``ACS_TEST_LEGACY_GUARD``) and show that ACS's
cleanup evidence does not call its records clean.  Their internal-guard
counterparts run in the standard profile (named beside each).
"""

from __future__ import annotations

import json
import os
import secrets
import shutil
import signal
import stat
import subprocess
import time
from pathlib import Path

import pytest

from agent_compute import procs
from agent_compute.runner import Runner
from conftest import needs_legacy
from test_children_docker import IMAGE, needs_docker

REAL_DOCKER = shutil.which("docker")

# -- helpers -------------------------------------------------------------------

_SCAN = r"""
import json, os
out, me = [], os.getpid()
for n in os.listdir('/proc'):
    if not n.isdigit() or int(n) == me:
        continue
    try:
        st = open('/proc/%s/stat' % n).read()
        cmd = open('/proc/%s/cmdline' % n, 'rb').read()
        env = open('/proc/%s/environ' % n, 'rb').read().split(b'\0')
    except OSError:
        continue
    f = st.rsplit(')', 1)[1].split()
    if f[0] in ('Z', 'X'):
        continue
    job = next((e.split(b'=', 1)[1].decode() for e in env
                if e.startswith((b'ACS_JOB_MARKER=', b'SMC_GUARD_JOB='))),
               None)
    out.append({'pid': int(n), 'ppid': int(f[1]), 'pgrp': int(f[2]),
                'sid': int(f[3]), 'stat': f[0], 'job': job,
                'cmd': cmd.replace(b'\0', b' ').decode(errors='replace')})
print(json.dumps(out))
"""


def scan(container: str, tag: str | None = None) -> list[dict]:
    """Live (non-zombie) processes in a disposable container, with the job
    marker's value from their environ (ACS_JOB_MARKER, or the legacy
    SMC_GUARD_JOB: G2a); optionally only ``tag`` ones."""
    out = subprocess.run([REAL_DOCKER, "exec", container, "python3", "-c",
                          _SCAN], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    rows = json.loads(out.stdout)
    return [r for r in rows if tag is None or tag in r["cmd"]]


def guard_record(env, job_id: str) -> dict:
    attempts = sorted((env.state / "jobs" / job_id / "guard").glob(
        "attempt-*/guard.json"))
    return json.loads(attempts[-1].read_text()) if attempts else {}


def blocked(env) -> bool:
    return (env.state / "blocks" / "testhost.json").exists()


def submit_container(env, name, argv, timeout_min="1", path_env=None):
    result = env.acs("submit", "--json", "--case", "c", "--timeout-min",
                     timeout_min, "--reserve-gb", "0.5",
                     "--cwd", str(env.projects["alpha"]),
                     "--container", name, "--", *argv, env=path_env)
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


def report(label: str, **facts) -> None:
    print(f"\n[{label}] " + json.dumps(facts, indent=1, default=str))


def legacy_record(out: Path) -> dict:
    path = out / "guard.json"
    return json.loads(path.read_text()) if path.is_file() else {}


def evidence_of(name: str, record: dict) -> dict:
    """ACS's cleanup evidence (the function the runner and reconcile
    share) for a guard.py record of a job in container ``name``."""
    return procs.container_job_evidence(
        {"name": name, "id": procs.container_id(name), "workdir": "/work"},
        record)


@pytest.fixture
def make_container():
    """Factory for disposable ``acs-test-*`` containers, removed in finally."""
    names: list[str] = []

    def make(init: bool = False) -> str:
        name = f"acs-test-{secrets.token_hex(4)}"
        argv = [REAL_DOCKER, "run", "-d", "--rm", "--name", name,
                "--memory", "1g", "--cpus", "1",
                "--label", "agent-compute-test=1"]
        if init:
            argv.append("--init")
        names.append(name)
        subprocess.run(argv + [IMAGE, "sleep", "3600"], check=True,
                       capture_output=True)
        subprocess.run([REAL_DOCKER, "exec", name, "mkdir", "-p", "/work"],
                       check=True, capture_output=True)
        return name

    try:
        yield make
    finally:
        for name in names:
            subprocess.run([REAL_DOCKER, "rm", "-f", name],
                           capture_output=True)


def docker_shim(env, tmp_path: Path, pid_read: str) -> dict:
    """An environment whose PATH puts a ``docker`` shim first.  Only the
    guard's in-container pid-file read is altered: ``fail`` makes it fail
    at once (container_pid stays null); ``hang`` sleeps past the guard's
    15 s timeout (a slow daemon).  Everything else is the real docker."""
    bindir = tmp_path / "shim"
    bindir.mkdir(exist_ok=True)
    action = {"fail": "echo 'cat: No such file or directory' >&2; exit 1",
              "hang": "exec sleep 20"}[pid_read]
    shim = bindir / "docker"
    shim.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = exec ] && [ "$3" = cat ]; then\n'
        f'  case "$4" in /tmp/smc-guard-*) {action};; esac\n'
        "fi\n"
        f'exec "{REAL_DOCKER}" "$@"\n')
    shim.chmod(0o755)
    return env.env(PATH=f"{bindir}:{os.environ['PATH']}")


# -- (a) fast job leaving a same-session background child ----------------------

# guard.py reproduction, legacy profile (internal-guard counterpart:
# test_acs_guard_containers.py::test_same_session_survivor_blocks_and_reconcile_resolves_the_marker)
@needs_legacy
@needs_docker
@pytest.mark.parametrize("init", [False, True], ids=["plain", "init"])
def test_desired_a_fast_job_with_same_session_survivor_is_not_clean(
        env_direct, make_container, init, tmp_path):
    env = env_direct
    name = make_container(init=init)
    blind = docker_shim(env, tmp_path, "fail")
    tag = f"acs-bg-{secrets.token_hex(3)}"
    # the leader exits at once; its background child keeps the leader's
    # session/pgrp (no setsid) and has its stdio redirected
    script = f"( exec -a {tag} sleep 75 ) >/dev/null 2>&1 </dev/null & exit 0"
    guard, out = env.legacy_container(name, ["bash", "-c", script],
                                      path_env=blind)
    guard.wait(90)
    record = legacy_record(out)
    survivors = scan(name, tag)
    evidence = evidence_of(name, record)
    report("a", init=init, guard_outcome=record.get("outcome"),
           guard_pid=record.get("pid"), container_pid=record.get(
               "container_pid"), wall_s=record.get("wall_s"),
           guard_job=record.get("job_id"), survivors=survivors,
           evidence=evidence)
    # preconditions of the reproduction
    assert record.get("pid") and record.get("container_pid") is None
    assert record.get("outcome") == "completed"
    assert survivors, "expected the same-session child to outlive the leader"
    child = survivors[0]
    assert child["sid"] == child["pgrp"] != child["pid"]   # leader's session
    assert child["job"] == record["job_id"]      # carries SMC_GUARD_JOB
    # the safe behavior: ACS's evidence does not call it clean
    assert evidence["clean"] is False, \
        "the cleanup evidence called the job clean while its same-session " \
        "child still runs"


@needs_docker
def test_desired_a_natural_race_without_shim(env_direct, make_container):
    """The same job with the real docker (no shim): on the Mac the guard's
    pid read loses the race every time (3/3 in slice2-natural.txt), so the
    helper path is the natural one.  Robust to either race outcome: it only
    requires that a live same-session child is never called clean."""
    env = env_direct
    name = make_container()
    env.set_container(name)
    tag = f"acs-nat-{secrets.token_hex(3)}"
    script = f"( exec -a {tag} sleep 75 ) >/dev/null 2>&1 </dev/null & exit 0"
    job = submit_container(env, name, ["bash", "-c", script])
    state = env.wait_outcome(job["job_id"], timeout=60)
    record = guard_record(env, job["job_id"])
    survivors = scan(name, tag)
    report("a-natural", outcome=state.get("outcome"),
           cleanup=state.get("cleanup"),
           container_pid=record.get("container_pid"), survivors=survivors,
           blocked=blocked(env))
    assert survivors
    assert state["cleanup"]["clean"] is False, \
        "ACS called the job clean while its same-session child still runs"


# -- (b) the recoverability baseline -----------------------------------------------

@needs_docker
def test_baseline_b_fast_job_without_descendants_completes(env_direct,
                                                           make_container,
                                                           tmp_path):
    """A fast container job with no descendants and no captured
    container_pid must stay recoverable (not a durable block).  A repair
    must keep this passing, with evidence rather than the record alone."""
    env = env_direct
    name = make_container()
    env.set_container(name)
    job = submit_container(env, name, ["bash", "-c", "exit 0"],
                           path_env=docker_shim(env, tmp_path, "fail"))
    state = env.wait_outcome(job["job_id"], timeout=60)
    record = guard_record(env, job["job_id"])
    left = [r for r in scan(name) if r["job"] == record.get("job_id")]
    report("b", outcome=state.get("outcome"), cleanup=state.get("cleanup"),
           container_pid=record.get("container_pid"), left=left,
           blocked=blocked(env))
    # the shim blinds only guard.py's pid read; the internal guard's execs
    # run with `-w /` and do not match it (S3), so it may capture the pid:
    # the recoverability below must hold either way
    assert not left
    assert state["outcome"] == "succeeded", state
    assert state["cleanup"]["clean"] is True
    assert not blocked(env)


# -- (c) the documented setsid boundary (kept separate from (a)) -------------------

@needs_docker
def test_limitation_c_setsid_escape_keeping_the_marker_is_caught(
        env_direct, make_container):
    """Formerly the documented boundary: a descendant that calls setsid()
    leaves the tracked session even when container_pid IS captured.  Since
    slice 3 the evidence keys on SMC_GUARD_JOB, which survives setsid, so
    the escape is caught (blocked) and ``reconcile --terminate`` stops it.
    The remaining boundary (marker scrubbed AND session changed) is pinned
    in test_container_job_evidence.py."""
    env = env_direct
    name = make_container()
    env.set_container(name)
    tag = f"acs-detached-{secrets.token_hex(3)}"
    code = ("import subprocess, time; subprocess.Popen(['bash', '-c', "
            f"'exec -a {tag} sleep 75'], start_new_session=True, "
            "stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, "
            "stderr=subprocess.DEVNULL); time.sleep(4)")
    job = submit_container(env, name, ["python3", "-c", code])
    state = env.wait_outcome(job["job_id"], timeout=60)
    record = guard_record(env, job["job_id"])
    escaped = scan(name, tag)
    report("c", outcome=state.get("outcome"), cleanup=state.get("cleanup"),
           container_pid=record.get("container_pid"),
           guard_job=record.get("job_id"), escaped=escaped)
    assert record.get("container_pid"), "precondition: pid was captured"
    assert escaped and escaped[0]["sid"] == escaped[0]["pid"]
    assert escaped[0]["job"] == record["job_id"]   # environ survives setsid
    assert state["cleanup"]["clean"] is False      # now detected
    assert escaped[0]["pid"] in [m["pid"] for m in state["cleanup"]["members"]]
    assert blocked(env)
    rec = json.loads(env.acs("reconcile", "--terminate", job["job_id"],
                             "--json").stdout)
    assert rec["blocked"] is False, rec
    assert not scan(name, tag)


# -- (d) guard kills with container_pid null signal only the docker client ---------

# guard.py reproduction, legacy profile (internal-guard counterpart:
# test_acs_guard_containers.py::test_walltime_kill_stops_the_whole_container_job)
@needs_legacy
@needs_docker
def test_desired_d1_walltime_kill_without_container_pid_is_not_clean(
        env_direct, make_container, tmp_path):
    env = env_direct
    name = make_container()
    blind = docker_shim(env, tmp_path, "fail")
    tag = f"acs-wall-{secrets.token_hex(3)}"
    guard, out = env.legacy_container(
        name, ["python3", "-c", f"import time; time.sleep(75)  # {tag}"],
        timeout_min="0.1", path_env=blind)
    guard.wait(80)
    record = legacy_record(out)
    alive = scan(name, tag)
    evidence = evidence_of(name, record)
    report("d1", guard_outcome=record.get("outcome"),
           container_pid=record.get("container_pid"), alive=alive,
           evidence=evidence)
    assert record.get("outcome") == "killed-walltime"
    assert record.get("container_pid") is None
    assert alive, "expected the in-container workload to outlive the kill"
    assert evidence["clean"] is False, \
        "the evidence called a walltime-killed job clean while it runs"


# guard.py reproduction, legacy profile (internal-guard counterpart:
# test_guard_lifecycle.py::test_acs_cancel_of_an_internal_guard_container_job_kills_its_children)
@needs_legacy
@needs_docker
def test_desired_d2_cancel_without_container_pid_is_not_clean(
        env_direct, make_container, tmp_path):
    env = env_direct
    name = make_container()
    blind = docker_shim(env, tmp_path, "fail")
    tag = f"acs-int-{secrets.token_hex(3)}"
    guard, out = env.legacy_container(
        name, ["python3", "-c", f"import time; time.sleep(75)  # {tag}"],
        timeout_min="2", path_env=blind)
    # guard.py writes guard.json once launch() gives up on the pid (30 s)
    env.wait_for(lambda: legacy_record(out).get("pid"), timeout=50,
                 what="guard record after launch")
    time.sleep(1.5)
    guard.send_signal(signal.SIGTERM)        # what ACS's cancel sent it
    guard.wait(40)
    record = legacy_record(out)
    alive = scan(name, tag)
    evidence = evidence_of(name, record)
    report("d2", guard_outcome=record.get("outcome"),
           container_pid=record.get("container_pid"), alive=alive,
           evidence=evidence)
    assert record.get("outcome") == "killed-interrupted"
    assert record.get("container_pid") is None
    assert alive, "expected the in-container workload to outlive the cancel"
    assert evidence["clean"] is False, \
        "the evidence called a cancelled job clean while its workload runs"


# -- (e) reconcile on a record without container_pid, live survivor ----------------

def _craft_block(env, job_id: str, container: dict, record: dict,
                 samples: bool = True, job_dir: bool = True) -> None:
    if job_dir:
        attempt = env.state / "jobs" / job_id / "guard" / "attempt-1"
        attempt.mkdir(parents=True)
        (env.state / "jobs" / job_id / "manifest.json").write_text(
            json.dumps({"container": container} if container else {}))
        if samples:
            (attempt / "samples.csv").write_text("")
        if record is not None:
            (attempt / "guard.json").write_text(json.dumps(record))
    (env.state / "blocks").mkdir(parents=True, exist_ok=True)
    (env.state / "blocks" / "testhost.json").write_text(json.dumps(
        {"host": "testhost", "created_utc": "2026-09-27T12:00:00Z",
         "jobs": {job_id: {"reason": "workload cleanup could not be "
                                     "established",
                           "evidence": {"workload": None},
                           "utc": "2026-09-27T12:00:00Z"}}}))


@needs_docker
@pytest.mark.parametrize("outcome", ["completed", "killed-walltime"])
def test_desired_e_reconcile_keeps_block_while_in_container_survivor_lives(
        env_direct, make_container, outcome):
    env = env_direct
    name = make_container()
    cid = procs.container_id(name)
    guard_job = f"c/2026-09-27T12-00-00-{secrets.token_hex(2)}"
    tag = f"acs-rec-{secrets.token_hex(3)}"
    subprocess.run([REAL_DOCKER, "exec", "-d", "-e",
                    f"SMC_GUARD_JOB={guard_job}", name, "bash", "-c",
                    f"exec -a {tag} sleep 75"], check=True)
    env.wait_for(lambda: scan(name, tag), timeout=20, what="survivor")
    job_id = f"acs-20260927T120000Z-{secrets.token_hex(3)}"
    _craft_block(env, job_id, {"name": name, "id": cid, "workdir": "/work"},
                 {"job_id": guard_job, "pid": 999999, "container_pid": None,
                  "outcome": outcome, "exit_code": 0,
                  "started_utc": "2026-09-27T12:00:00Z"})
    rec = json.loads(env.acs("reconcile", "--json").stdout)
    alive = scan(name, tag)
    report("e", outcome=outcome, reconcile=rec, alive=alive)
    assert alive
    assert rec["blocked"] is True, \
        "reconcile cleared the block while the job's workload still runs"


# -- (f) unit-level: which record shapes does each path call clean? ----------------

MISSING = object()
SHAPES = {
    "completed": {"pid": 4242, "container_pid": None, "outcome": "completed"},
    "failed": {"pid": 4242, "container_pid": None, "outcome": "failed"},
    "killed-walltime": {"pid": 4242, "container_pid": None,
                        "outcome": "killed-walltime"},
    "killed-interrupted": {"pid": 4242, "container_pid": None,
                           "outcome": "killed-interrupted"},
    "killed-budget": {"pid": 4242, "container_pid": None,
                      "outcome": "killed-budget"},
    "killed-paused": {"pid": 4242, "container_pid": None,
                      "outcome": "killed-paused"},
    "probe-stopped": {"pid": 4242, "container_pid": None,
                      "outcome": "probe-stopped"},
    "weird": {"pid": 4242, "container_pid": None, "outcome": "weird"},
    "empty-string": {"pid": 4242, "container_pid": None, "outcome": ""},
    "lock-busy": {"pid": 4242, "container_pid": None, "outcome": "lock-busy"},
    "refused": {"pid": 4242, "container_pid": None, "outcome": "refused"},
    "not-started": {"pid": 4242, "container_pid": None,
                    "outcome": "not-started"},
    "launch-failed": {"pid": 4242, "container_pid": None,
                      "outcome": "launch-failed"},
    "pid-string": {"pid": "abc", "container_pid": None,
                   "outcome": "completed"},
    "outcome-int": {"pid": 4242, "container_pid": None, "outcome": 0},
    "outcome-list": {"pid": 4242, "container_pid": None,
                     "outcome": ["completed"]},
    "container_pid-string": {"pid": 4242, "container_pid": "",
                             "outcome": "completed"},
    # controls (already fail closed)
    "running": {"pid": 4242, "container_pid": None, "outcome": "running"},
    "outcome-none": {"pid": 4242, "container_pid": None, "outcome": None},
    "no-pid": {"pid": None, "container_pid": None, "outcome": "completed"},
    "guard.json-missing": MISSING,
}
ABSENT = {"name": "acs-test-absent", "id": "0" * 64, "workdir": "/work"}


def _docker_unreachable(*_a, **_k):
    raise OSError("docker unreachable (test)")


@pytest.mark.parametrize("shape", list(SHAPES))
def test_desired_f_runner_never_clean_from_record_alone(shape, monkeypatch):
    """No live observation is possible (docker unreachable), so no record
    shape may be called clean by Runner.verify_cleanup."""
    monkeypatch.setattr(procs, "docker", _docker_unreachable)
    runner = Runner.__new__(Runner)
    runner.manifest = {"container": ABSENT}
    runner.workload = None
    record = {} if SHAPES[shape] is MISSING else dict(SHAPES[shape])
    evidence = runner.verify_cleanup(record)
    assert evidence.get("clean") is False, evidence


@pytest.fixture
def fake_docker_path(tmp_path):
    """A PATH whose ``docker`` always fails as an unreachable daemon, so the
    CLI subprocess can never observe (or touch) a real container."""
    bindir = tmp_path / "fakebin"
    bindir.mkdir()
    shim = bindir / "docker"
    shim.write_text("#!/bin/sh\necho 'Cannot connect to the Docker daemon' "
                    ">&2\nexit 1\n")
    shim.chmod(shim.stat().st_mode | stat.S_IEXEC)
    return f"{bindir}:{os.environ['PATH']}"


@pytest.mark.parametrize("shape", list(SHAPES))
def test_desired_f_reconcile_never_clears_from_record_alone(
        shape, env_direct, fake_docker_path):
    env = env_direct
    job_id = f"acs-20260927T120000Z-{secrets.token_hex(3)}"
    record = None if SHAPES[shape] is MISSING else SHAPES[shape]
    _craft_block(env, job_id, ABSENT, record)
    result = env.acs("reconcile", "--json",
                     env=env.env(PATH=fake_docker_path))
    # judged on the durable block itself: a reconcile that crashes (e.g. an
    # unhashable outcome) leaves the block and so still fails closed
    assert blocked(env), result.stdout + result.stderr[-400:]


def test_desired_f_classify_launch_failed_with_pid_is_not_never_started(
        tmp_path, monkeypatch):
    """guard.launch(): a ``docker exec cat pidfile`` that times out (15 s)
    raises out of launch(); run() records ``launch-failed`` with ``pid`` set
    and exits, releasing the lock, while the docker client and its
    in-container workload keep running.  classify() reports it
    "never started" without any cleanup verification or block."""
    monkeypatch.setattr(procs, "docker", _docker_unreachable)
    attempt = tmp_path / "attempt-1"
    attempt.mkdir()
    (attempt / "samples.csv").write_text("")
    (attempt / "guard.json").write_text(json.dumps(
        {"pid": 4242, "container_pid": None, "outcome": "launch-failed"}))
    runner = Runner.__new__(Runner)
    runner.manifest = {"container": ABSENT}
    runner.workload = None
    calls: dict = {}
    runner.update = lambda **kw: None
    runner.cancel_request = lambda: None
    runner.block = lambda reason, evidence, **kw: calls.setdefault(
        "block", reason)
    runner.finish = lambda outcome, reason, **kw: calls.setdefault(
        "finish", (outcome, reason)) and 0
    runner.classify(6, attempt)
    assert "block" in calls, calls


# guard.py reproduction, legacy profile (internal guard: a hung pid read is
# 'not yet' (S3), and its execs run with -w / (the shim does not match))
@needs_legacy
@needs_docker
def test_desired_f_launch_failed_after_pid_read_timeout_leaves_workload(
        env_direct, make_container, tmp_path):
    """The classify gap through guard.py: a PATH shim delays only its
    ``docker exec <c> cat /tmp/smc-guard-*.pid`` past its 15 s timeout (a
    slow daemon); everything else is the real docker.  guard.py reports
    ``launch-failed`` with a pid and releases the lock while the workload
    runs; the runner's classify of that record shape blocks (the unit test
    above), and the evidence does not call it clean."""
    env = env_direct
    name = make_container()
    path_env = docker_shim(env, tmp_path, "hang")
    tag = f"acs-lf-{secrets.token_hex(3)}"
    guard, out = env.legacy_container(
        name, ["python3", "-c", f"import time; time.sleep(75)  # {tag}"],
        timeout_min="2", path_env=path_env)
    guard.wait(60)
    record = legacy_record(out)
    alive = scan(name, tag)
    evidence = evidence_of(name, record)
    report("f-launch-failed", guard_outcome=record.get("outcome"),
           pid=record.get("pid"), container_pid=record.get("container_pid"),
           alive=alive, lock_free=env.lock_free(), evidence=evidence)
    assert record.get("outcome") == "launch-failed" and record.get("pid")
    assert alive, "expected the workload to run on after launch-failed"
    assert env.lock_free()
    assert evidence["clean"] is False, evidence


# -- other false-clean candidates ---------------------------------------------------

@needs_docker
def test_desired_i_epoch_fallback_never_calls_a_live_leader_recycled(
        make_container):
    """container_survivors with only a start EPOCH (reconcile's
    workload_from_record, or a runner whose first scan failed) treats an
    epoch more than EPOCH_TOLERANCE_S + 5 s away as 'pid recycled' -> clean,
    even though the leader is live.  A slow docker exec or a VM clock
    offset is enough; here the recorded epoch is 20 s early."""
    name = make_container()
    tag = f"acs-epoch-{secrets.token_hex(3)}"
    launched = time.time()
    subprocess.run([REAL_DOCKER, "exec", "-d", name, "bash", "-c",
                    f"exec -a {tag} sleep 75"], check=True)
    rows = []
    for _ in range(40):
        rows = scan(name, tag)
        if rows:
            break
        time.sleep(0.25)
    assert rows
    leader = rows[0]["pid"]
    exact = procs.container_scan(name, leader)
    workload = {"venue": "container", "container": name,
                "container_id": procs.container_id(name),
                "container_pid": leader, "container_start": None,
                "container_start_epoch": launched - 20}
    evidence = procs.container_survivors(workload)
    report("i", host_launch_epoch=launched, in_container_epoch=exact.get(
        "epoch"), offset_s=(exact.get("epoch") or 0) - launched,
           evidence=evidence)
    assert scan(name, tag)
    assert evidence["clean"] is False, evidence


@needs_docker
def test_desired_ii_renamed_container_is_not_called_replaced(make_container):
    """container_survivors resolves the recorded container by NAME; if the
    original was renamed and a new container took the name, the recorded
    id no longer matches and the job is called clean ('replaced') while
    its workload still runs in the renamed original."""
    original = make_container()
    original_id = procs.container_id(original)
    tag = f"acs-ren-{secrets.token_hex(3)}"
    subprocess.run([REAL_DOCKER, "exec", "-d", original, "bash", "-c",
                    f"exec -a {tag} sleep 75"], check=True)
    rows = []
    for _ in range(40):
        rows = scan(original, tag)
        if rows:
            break
        time.sleep(0.25)
    assert rows
    exact = procs.container_scan(original, rows[0]["pid"])
    moved = f"{original}-moved"
    subprocess.run([REAL_DOCKER, "rename", original, moved], check=True)
    try:
        subprocess.run([REAL_DOCKER, "run", "-d", "--rm", "--name", original,
                        "--label", "agent-compute-test=1", IMAGE, "sleep",
                        "120"], check=True, capture_output=True)
        evidence = procs.container_survivors(
            {"venue": "container", "container": original,
             "container_id": original_id, "container_pid": rows[0]["pid"],
             "container_start": exact["start"],
             "container_start_epoch": exact["epoch"]})
        still = scan(moved, tag)
        report("ii", evidence=evidence, still_running_in_renamed=still)
        assert still
        assert evidence["clean"] is False, evidence
    finally:
        subprocess.run([REAL_DOCKER, "rm", "-f", moved, original],
                       capture_output=True)


def test_desired_iii_reconcile_without_any_job_evidence_keeps_block(
        env_direct, fake_docker_path):
    """A block entry whose job directory is missing (or has no samples.csv
    and no state) is cleared as 'the guard never began launching': absence
    of evidence treated as evidence of absence."""
    env = env_direct
    job_id = f"acs-20260927T120000Z-{secrets.token_hex(3)}"
    _craft_block(env, job_id, ABSENT, None, job_dir=False)
    rec = json.loads(env.acs("reconcile", "--json",
                             env=env.env(PATH=fake_docker_path)).stdout)
    report("iii", reconcile=rec)
    assert rec["blocked"] is True, rec
