"""A20: ACS's own in-container execs must not run in the container's
WorkingDir, must bound themselves, and must say why they failed.

The defect (2026-09-28 and 2026-10-05, Mac).  Every ACS ``docker exec <c>
python3 -c ...`` (the leader-ticks scan, the job scan, the signal helper)
and the provenance ``sha256sum`` ran in the container's ``WorkingDir``.
Python puts the cwd (and ``PYTHONPATH``) first on ``sys.path``.  When the
WorkingDir is a hung Docker Desktop bind mount, each exec hangs; the host
``docker`` client is killed at ``DOCKER_TIMEOUT_S`` but the in-container
process stays, so every retry leaked a stuck scanner (9 after one
``reconcile --terminate`` on 2026-10-05).  The *decision* was fail-closed
and correct ("in-container scan failed"); these tests pin that it stays so.

The fix: one helper (``procs.container_python``) runs every ACS helper as
``docker exec -w / <cid> python3 -I -S -c SCRIPT``; every script's first
statement is ``signal.alarm(SCRIPT_ALARM_S)`` (20 s, below the host's
30 s); the failure reasons distinguish a host timeout, the in-container
alarm, a failed exec and unparseable output; after a host-side timeout in a
container, this process does not spawn another helper there (it could only
pile up behind the stuck one) and fails closed instead.

The fake-docker tests need no Docker.  The live tests use only disposable
``acs-test-*`` containers, removed in ``finally``.
"""

from __future__ import annotations

import json
import os
import secrets
import subprocess
import sys
import time

import pytest

from agent_compute import procs, runner
from test_children_docker import IMAGE, needs_docker
from test_cleanup_evidence import REAL_DOCKER

JOB = "alpha.c/20261005T151752Z"
CID = "a" * 64
RECORD = {"job_id": JOB, "pid": 4242, "container_pid": 7,
          "outcome": "completed", "started_utc": "2026-10-05T15:17:52Z"}
SCAN = {"now": 1.0e9, "btime": 0.0, "settled": True, "tck": 100,
        "procs": [{"pid": 1, "ppid": 0, "pgrp": 1, "sid": 1, "ticks": 1,
                   "env": "no-marker"}]}
MARKED = {**SCAN, "procs": SCAN["procs"] + [
    {"pid": 30, "ppid": 1, "pgrp": 30, "sid": 30, "ticks": 5,
     "env": "has-marker"}]}


def python_exec(args) -> tuple[str, str, list]:
    """(container, script, script args) of an ACS python exec."""
    i = args.index("python3")
    rest = list(args[i + 1:])
    script = rest[rest.index("-c") + 1]
    return args[i - 1], script, rest[rest.index("-c") + 2:]


def fake_docker(monkeypatch, answer=None, calls=None):
    """``inspect`` says running; each python exec gets ``answer(script)``
    (a CompletedProcess or an exception to raise)."""
    calls = [] if calls is None else calls

    def default(script):
        if script == procs._JOB_SIGNAL or "os.kill" in script:
            out = {"groups": [30], "pids": [30], "skipped": []}
        elif "hashlib" in script:
            out = {"sha256": "ab" * 32}
        elif "'start'" in script:
            out = {"start": "ticks:5", "epoch": 1.0}
        else:
            out = MARKED
        return subprocess.CompletedProcess([], 0, json.dumps(out) + "\n", "")

    def docker(*args, timeout=30):
        calls.append(args)
        if args[0] == "info":
            return subprocess.CompletedProcess(args, 0, "daemon-1\n", "")
        if args[0] == "inspect":
            return subprocess.CompletedProcess(args, 0, f"{CID} true", "")
        if args[0] != "exec":
            raise AssertionError(args)
        _, script, _ = python_exec(args)
        result = (answer or default)(script)
        if isinstance(result, BaseException):
            raise result
        return result
    monkeypatch.setattr(procs, "docker", docker)
    return calls


def every_acs_exec(monkeypatch) -> list:
    calls = fake_docker(monkeypatch)
    procs.container_scan(CID, 7)
    procs.container_job_evidence({"name": "c", "id": CID}, RECORD)
    procs.signal_container_job({"name": "c", "id": CID}, RECORD, None, 15)
    assert runner.container_provenance(CID, "/work/prov.txt") == "ab" * 32
    return [c for c in calls if c[0] == "exec"]


# -- every exec: from "/", isolated, alarm first ----------------------------------

def test_every_acs_exec_runs_from_root_isolated(monkeypatch):
    execs = every_acs_exec(monkeypatch)
    # leader ticks, job scan (evidence), job scan + signal, provenance
    assert len(execs) == 5, execs
    for args in execs:
        assert args[:4] == ("exec", "-w", "/", CID), args[:4]
        assert args[4:8] == ("python3", "-I", "-S", "-c"), args[4:8]


def test_every_acs_script_starts_with_the_alarm(monkeypatch):
    assert procs.SCRIPT_ALARM_S == 20 < procs.DOCKER_TIMEOUT_S == 30
    scripts = {python_exec(c)[1] for c in every_acs_exec(monkeypatch)}
    assert len(scripts) == 4
    for script in scripts:
        first = [line for line in script.splitlines() if line.strip()][:2]
        assert first == ["import signal",
                         f"signal.alarm({procs.SCRIPT_ALARM_S})"], first


def test_the_scripts_compile():
    for script in (procs._CONTAINER_SCAN, procs._JOB_SCAN,
                   procs._JOB_SIGNAL, runner._PROVENANCE):
        compile(script, "<acs-helper>", "exec")


def test_provenance_of_a_relative_path_is_none_without_an_exec(monkeypatch):
    calls = fake_docker(monkeypatch)
    assert runner.container_provenance(CID, "work/prov.txt") is None
    assert runner.container_provenance(CID, "") is None
    assert calls == []


@pytest.mark.parametrize("bad", ["", "abc123", "zz" * 32, None, 5])
def test_provenance_of_a_malformed_answer_is_none(monkeypatch, bad):
    fake_docker(monkeypatch, lambda s: subprocess.CompletedProcess(
        [], 0, json.dumps({"sha256": bad}), ""))
    assert runner.container_provenance(CID, "/work/prov.txt") is None


# -- failure reasons: distinguished, and every one fails closed --------------------

FAILURES = {
    "host-timeout": (subprocess.TimeoutExpired(["docker"], 30),
                     "timed out (host 30 s)"),
    "alarm": (subprocess.CompletedProcess([], 142, "", ""),
              "timed out (in-container alarm, 20 s)"),
    "failed": (subprocess.CompletedProcess([], 1, "", "boom: no python3"),
               "failed (rc 1"),
    "unparseable": (subprocess.CompletedProcess([], 0, "garbage\n", ""),
                    "unparseable"),
    "not-an-object": (subprocess.CompletedProcess([], 0, "[1, 2]\n", ""),
                      "unparseable"),
    "empty": (subprocess.CompletedProcess([], 0, "", ""), "unparseable"),
    "oserror": (OSError("no docker binary"), "docker exec failed"),
}


@pytest.mark.parametrize("case", sorted(FAILURES))
def test_scan_failure_reason_is_specific_and_unverified(monkeypatch, case):
    result, reason = FAILURES[case]
    fake_docker(monkeypatch, lambda s: result)
    ev = procs.container_job_evidence({"name": "c", "id": CID}, RECORD)
    assert ev["verified"] is False and ev["clean"] is False, ev
    assert ev["reason"].startswith("in-container scan failed"), ev
    assert reason in ev["reason"], ev


@pytest.mark.parametrize("case", sorted(FAILURES))
def test_signal_helper_failure_signals_nothing(monkeypatch, case):
    result, reason = FAILURES[case]

    def answer(script):
        if script == procs._JOB_SIGNAL:
            return result
        return subprocess.CompletedProcess(
            [], 0, json.dumps(MARKED), "")
    fake_docker(monkeypatch, answer)
    done = procs.signal_container_job({"name": "c", "id": CID}, RECORD,
                                      None, 15)
    assert done["signalled"] is False, done
    assert reason in done["containers"][CID]["reason"], done


@pytest.mark.parametrize("case", sorted(FAILURES))
def test_failed_provenance_is_none(monkeypatch, case):
    fake_docker(monkeypatch, lambda s: FAILURES[case][0])
    assert runner.container_provenance(CID, "/work/prov.txt") is None


def test_leader_ticks_failure_is_none(monkeypatch):
    for case in FAILURES:
        procs._STUCK.clear()
        fake_docker(monkeypatch, lambda s: FAILURES[case][0])
        assert procs.container_scan(CID, 7) is None, case


# -- no pile-up behind a stuck helper ----------------------------------------------

def test_after_a_host_timeout_no_new_exec_in_that_container(monkeypatch):
    calls = fake_docker(monkeypatch, lambda s: subprocess.TimeoutExpired(
        ["docker"], 30))
    first = procs.container_job_evidence({"name": "c", "id": CID}, RECORD)
    assert "timed out (host 30 s)" in first["reason"]
    n = len([c for c in calls if c[0] == "exec"])
    # the helper is answerable again, but a stuck one may still be there:
    # every later call fails closed without spawning another
    fake_docker(monkeypatch, calls=calls)
    for _ in range(3):
        ev = procs.container_job_evidence({"name": "c", "id": CID}, RECORD)
        assert ev["verified"] is False and ev["clean"] is False, ev
        assert "earlier ACS helper" in ev["reason"], ev
    done = procs.signal_container_job({"name": "c", "id": CID}, RECORD,
                                      None, 15)
    assert done["signalled"] is False
    assert runner.container_provenance(CID, "/work/prov.txt") is None
    assert procs.container_scan(CID, 7) is None
    assert len([c for c in calls if c[0] == "exec"]) == n


def test_other_failures_do_not_suppress_later_execs(monkeypatch):
    """Only a host-side timeout can leave a helper behind; an alarm, a
    failed exec or garbage ended the in-container process."""
    for case in ("alarm", "failed", "unparseable", "oserror"):
        procs._STUCK.clear()
        fake_docker(monkeypatch, lambda s: FAILURES[case][0])
        procs.container_job_evidence({"name": "c", "id": CID}, RECORD)
        fake_docker(monkeypatch)
        ev = procs.container_job_evidence({"name": "c", "id": CID}, RECORD)
        assert ev["verified"] is True, (case, ev)


def test_a_stuck_container_does_not_affect_another(monkeypatch):
    other = "b" * 64
    procs._STUCK.add(other)
    calls = fake_docker(monkeypatch)
    ev = procs.container_job_evidence({"name": "c", "id": CID}, RECORD)
    assert ev["verified"] is True, ev
    assert any(c[0] == "exec" for c in calls)


# -- the alarm itself (a blocking FIFO read, run by the local python) -------------

def test_alarm_ends_a_blocking_fifo_read_locally(tmp_path):
    """The prelude's alarm ends a killable wait (a FIFO with no writer),
    with the SIGALRM death the helper recognizes in a container (142)."""
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    script = procs.bounded("open(%r).read()\nprint('{}')\n" % str(fifo),
                           alarm_s=1)
    start = time.monotonic()
    out = subprocess.run([sys.executable, "-I", "-S", "-c", script],
                         capture_output=True, text=True, timeout=20)
    assert out.returncode == -14, out      # killed by SIGALRM
    assert time.monotonic() - start < 10
    assert procs.ALARM_EXIT == 128 + 14


# -- live: disposable containers ---------------------------------------------------

@pytest.fixture
def hostile_container(tmp_path):
    """A disposable container whose WorkingDir and PYTHONPATH hold a
    ``json.py`` that blocks forever on a FIFO read (the hung bind mount's
    stand-in: an import from the cwd that never returns)."""
    name = f"acs-test-{secrets.token_hex(4)}"
    try:
        subprocess.run(
            [REAL_DOCKER, "run", "-d", "--rm", "--name", name,
             "--memory", "1g", "--cpus", "1",
             "--label", "agent-compute-test=1", "-w", "/hang",
             "-e", "PYTHONPATH=/hang", IMAGE, "sh", "-c",
             "mkdir -p /hang && mkfifo /hang/block && "
             "printf 'open(\"/hang/block\").read()\\n' > /hang/json.py && "
             "printf 'open(\"/hang/block\").read()\\n' > /hang/signal.py && "
             "printf 'open(\"/hang/block\").read()\\n' > /hang/hashlib.py && "
             "echo $$ > /pid && touch /hang/ready && exec sleep 3600"],
            check=True, capture_output=True, timeout=120)
        for _ in range(100):
            if subprocess.run([REAL_DOCKER, "exec", "-w", "/", name, "test",
                               "-e", "/hang/ready"],
                              capture_output=True, timeout=30).returncode == 0:
                break
            time.sleep(0.1)
        cid = subprocess.run([REAL_DOCKER, "inspect", "--format", "{{.Id}}",
                              name], capture_output=True, text=True,
                             check=True, timeout=30).stdout.strip()
        yield name, cid
    finally:
        subprocess.run([REAL_DOCKER, "rm", "-f", name], capture_output=True,
                       timeout=120)


def stuck_pythons(name: str) -> list[str]:
    """python3 processes left in the container (``docker top`` reads the
    host's view; it does not exec in the container)."""
    out = subprocess.run([REAL_DOCKER, "top", name, "-eo", "pid,args"],
                         capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    return [line for line in out.stdout.splitlines()[1:] if "python" in line]


@needs_docker
def test_live_scan_ignores_a_blocking_cwd_and_pythonpath(hostile_container):
    name, cid = hostile_container
    # the stand-in really blocks a cwd-relative import (old behaviour)
    with pytest.raises(subprocess.TimeoutExpired):
        subprocess.run([REAL_DOCKER, "exec", name, "python3", "-c",
                        "import json"], capture_output=True, timeout=5)
    subprocess.run([REAL_DOCKER, "exec", "-w", "/", name, "sh", "-c",
                    "echo x > /hang/block"], capture_output=True, timeout=30)
    for _ in range(50):
        if not stuck_pythons(name):
            break
        time.sleep(0.1)
    assert stuck_pythons(name) == []
    start = time.monotonic()
    # the container's main process (its own pidfile, not an assumed pid)
    main_pid = int(subprocess.run(
        [REAL_DOCKER, "exec", "-w", "/", name, "cat", "/pid"],
        capture_output=True, text=True, check=True, timeout=30).stdout)
    scan = procs._scan_job(cid, JOB)
    leader = procs.container_scan(cid, main_pid)
    sha = runner.container_provenance(cid, "/hang/json.py")
    elapsed = time.monotonic() - start
    assert scan[1] is None and scan[0]["settled"] is True, scan
    assert leader and leader["start"].startswith("ticks:"), leader
    assert sha and len(sha) == 64, sha
    assert elapsed < 20, elapsed
    # the main process stands in for a live recorded leader, identified by
    # its exact start ticks: verified evidence that the "job" is alive
    ev = procs.container_job_evidence(
        {"name": name, "id": cid}, {**RECORD, "container_pid": main_pid},
        {"container_start": leader["start"]})
    assert ev["verified"] is True and ev["leader_alive"] is True, ev
    assert ev["clean"] is False, ev
    assert stuck_pythons(name) == []


@needs_docker
def test_live_alarm_ends_a_blocked_helper_with_142(hostile_container,
                                                  monkeypatch):
    """In the container, a helper blocked on a FIFO read dies of its alarm
    (docker exec exit 142) and is reported as such; nothing is left
    behind."""
    name, cid = hostile_container
    script = procs.bounded("open('/hang/block').read()\n", alarm_s=2)
    seen = []
    docker = procs.docker

    def recording(*args, **kwargs):
        out = docker(*args, **kwargs)
        seen.append(out.returncode)
        return out
    monkeypatch.setattr(procs, "docker", recording)
    start = time.monotonic()
    answer, reason = procs.container_python(cid, script)
    assert answer is None
    assert procs.ALARM_EXIT == 142 and seen == [142], seen
    # the reason names the configured SCRIPT_ALARM_S, not this test's 2 s
    assert reason.startswith("timed out (in-container alarm"), reason
    assert time.monotonic() - start < 15
    assert cid not in procs._STUCK
    assert stuck_pythons(name) == []
