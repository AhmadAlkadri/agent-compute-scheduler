"""Container cleanup evidence (cleanup-evidence campaign, slice 3).

A container job is judged only by ``procs.container_job_evidence``: the
recorded container located by id, and one in-container scan for the guard's
``SMC_GUARD_JOB=<job_id>`` marker plus the sessions and process groups of
the marker holders.  The guard record alone never makes a job clean.

The rule tests at the top feed synthetic scans (no docker).  The rest use
disposable ``acs-test-*`` containers only, removed in ``finally`` by the
``make_container`` fixture.
"""

from __future__ import annotations

import json
import os
import secrets
import subprocess
import time

import pytest

from agent_compute import procs
from conftest import needs_legacy
from test_children_docker import IMAGE, needs_docker
from test_cleanup_evidence import (  # noqa: F401 (fixtures)
    ABSENT, MISSING, REAL_DOCKER, SHAPES, _craft_block, blocked,
    fake_docker_path, guard_record, make_container, report, scan,
    submit_container)

JOB = "alpha.c/20260927T120000Z"
T0 = 1_790_000_000.0               # the job's start (host clock)


# -- the rule, on synthetic scans ------------------------------------------------

def proc(pid, sid=None, pgrp=None, at=10.0, env=procs.NO_MARKER, ppid=1):
    """A scanned process that started ``at`` seconds after the job."""
    ticks = int((T0 + at) * 100)
    return {"pid": pid, "ppid": ppid, "sid": sid or pid, "pgrp": pgrp or pid,
            "ticks": ticks, "start": f"ticks:{ticks}", "env": env}


def fake_scan(*processes, offset=0.0):
    """Container clock = host clock + ``offset``; scanned 100 s after T0."""
    return {"btime": offset, "tck": 100, "now": T0 + 100 + offset,
            "host_before": T0 + 99.5, "host_after": T0 + 100,
            "procs": list(processes)}


def judge(monkeypatch, *processes, record=None, workload=None, offset=0.0):
    monkeypatch.setattr(procs, "_locate", lambda ref: {"state": "running",
                                                       "id": "f" * 64})
    record = {"job_id": JOB, "pid": 4242, "container_pid": None,
              "outcome": "completed",
              "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                           time.gmtime(T0)),
              **(record or {})}
    return procs.container_job_evidence({"name": "c", "id": "f" * 64},
                                        record, workload,
                                        scan=fake_scan(*processes,
                                                       offset=offset))


INIT = proc(1, at=-3600)
OTHER = proc(50, at=20)                          # unrelated exec, no marker


def test_rule_no_job_process_is_clean(monkeypatch):
    ev = judge(monkeypatch, INIT, OTHER)
    assert ev["verified"] and ev["clean"] is True and ev["members"] == []


def test_rule_marker_holder_is_a_member(monkeypatch):
    ev = judge(monkeypatch, INIT, proc(30, env=procs.HAS_MARKER))
    assert ev["clean"] is False and [m["pid"] for m in ev["members"]] == [30]
    assert ev["members"][0]["start"].startswith("ticks:")


def test_rule_env_scrubbed_process_in_a_marker_session_is_a_member(
        monkeypatch):
    ev = judge(monkeypatch, INIT, proc(30, env=procs.HAS_MARKER),
               proc(31, sid=30, pgrp=30))
    assert ev["clean"] is False
    assert {m["pid"] for m in ev["members"]} == {30, 31}


def test_rule_session_of_a_gone_leader_is_the_jobs(monkeypatch):
    # leader 30 exited; a scrubbed child still carries its sid
    ev = judge(monkeypatch, INIT, proc(31, sid=30, pgrp=30),
               record={"container_pid": 30})
    assert ev["leader"] == procs.GONE and ev["clean"] is False


def test_rule_unreadable_after_the_job_is_unverifiable(monkeypatch):
    ev = judge(monkeypatch, INIT, proc(40, at=5, env=procs.UNREADABLE))
    assert ev["verified"] is False and ev["clean"] is False
    assert [p["pid"] for p in ev["unreadable"]] == [40]


def test_rule_unreadable_before_the_job_is_ignored(monkeypatch):
    ev = judge(monkeypatch, INIT, proc(40, at=-60, env=procs.UNREADABLE))
    assert ev["verified"] is True and ev["clean"] is True


def test_rule_container_clock_offset_never_hides_a_new_process(monkeypatch):
    """A VM clock 60 s behind makes a new process LOOK a minute old; the
    offset is bounded by the scan's own clock, so it is not ignored."""
    late = proc(40, at=5, env=procs.UNREADABLE)
    ev = judge(monkeypatch, INIT, late, offset=-60.0)
    assert ev["verified"] is False


def test_rule_unsettled_process_table_is_unverifiable(monkeypatch):
    monkeypatch.setattr(procs, "_locate", lambda ref: {"state": "running",
                                                       "id": "f" * 64})
    scan = fake_scan(INIT)
    scan["settled"] = False
    ev = procs.container_job_evidence(
        {"id": "f" * 64}, {"job_id": JOB, "pid": 1}, scan=scan)
    assert ev["verified"] is False and ev["clean"] is False


def test_rule_leader_with_marker_is_alive(monkeypatch):
    ev = judge(monkeypatch, INIT, proc(30, env=procs.HAS_MARKER),
               record={"container_pid": 30})
    assert ev["leader"] == procs.ALIVE and ev["clean"] is False


def test_rule_readable_leader_pid_without_marker_after_the_job_is_unknown(
        monkeypatch):
    """Recycled pid or this job's leader after `env -i`?  Time alone must
    not decide, so it is unverifiable, never clean."""
    ev = judge(monkeypatch, INIT, proc(30, at=2), record={"container_pid": 30})
    assert ev["leader"] == procs.UNKNOWN and ev["verified"] is False


def test_rule_leader_pid_held_by_an_older_process_is_recycled(monkeypatch):
    ev = judge(monkeypatch, INIT, proc(30, at=-600),
               record={"container_pid": 30})
    assert ev["leader"] == procs.RECYCLED and ev["clean"] is True


def test_rule_captured_ticks_match_without_marker_is_alive(monkeypatch):
    leader = proc(30, at=1)
    ev = judge(monkeypatch, INIT, leader, record={"container_pid": 30},
               workload={"container_start": leader["start"]})
    assert ev["leader"] == procs.ALIVE and ev["clean"] is False


def test_rule_legacy_record_without_job_id(monkeypatch):
    """No marker: only the recorded leader's session, never time alone."""
    no_id = {"job_id": None, "container_pid": 30}
    alive = proc(30, at=1)
    assert judge(monkeypatch, INIT, alive, record=no_id)["verified"] is False
    assert judge(monkeypatch, INIT, alive, record=no_id, workload={
        "container_start": alive["start"]})["clean"] is False
    assert judge(monkeypatch, INIT, alive, record=no_id, workload={
        "container_start": "ticks:1"})["clean"] is True       # exact mismatch
    assert judge(monkeypatch, INIT, proc(31, sid=30, pgrp=30),
                 record=no_id)["clean"] is False
    assert judge(monkeypatch, INIT, record=no_id)["clean"] is True
    ev = judge(monkeypatch, INIT, record={"job_id": None})
    assert ev["verified"] is False and "identifies" in ev["reason"]


@pytest.mark.parametrize("value", ["abc", "", 3.0, True, [1], -5])
def test_rule_malformed_pid_fields_never_crash(monkeypatch, value):
    ev = judge(monkeypatch, INIT, record={"container_pid": value})
    assert ev["clean"] in (True, False)
    assert procs.record_problem({"pid": value}) is not None


def test_container_gone_and_docker_errors(monkeypatch):
    def run(stdout="", stderr="", rc=0):
        return lambda *a, **k: subprocess.CompletedProcess(a, rc, stdout,
                                                           stderr)
    record = {"job_id": JOB, "pid": 1}
    cont = {"id": "f" * 64}
    monkeypatch.setattr(procs, "docker", run(
        stderr="Error: No such container: " + "f" * 64, rc=1))
    ev = procs.container_job_evidence(cont, record)
    assert ev["clean"] is True and ev["method"] == "container-gone"
    monkeypatch.setattr(procs, "docker", run(
        stderr="Cannot connect to the Docker daemon", rc=1))
    assert procs.container_job_evidence(cont, record)["verified"] is False
    monkeypatch.setattr(procs, "docker", run(stdout="e" * 64 + " true"))
    assert procs.container_job_evidence(cont, record)["verified"] is False
    monkeypatch.setattr(procs, "docker", run(stdout="f" * 64 + " false"))
    assert procs.container_job_evidence(cont, record)["method"] == \
        "container-stopped"


RECORDED, OTHER_ID = "a" * 64, "b" * 64


def stub_docker(monkeypatch, *, recorded="gone", by_name="running",
                name_err=None, scans=None, calls=None):
    """A docker stub: ``inspect`` answers for the recorded id and for the
    name ``c``; ``exec <id> python3 -c SCAN`` returns ``scans[id]``."""
    scans = scans or {}

    def fake(*args, **_k):
        if calls is not None:
            calls.append(args)
        if args[0] == "inspect":
            ref = args[-1]
            if ref == RECORDED:
                state, cid = recorded, RECORDED
            else:
                if name_err:
                    return subprocess.CompletedProcess(args, 1, "", name_err)
                state, cid = by_name, OTHER_ID
            if state == "gone":
                return subprocess.CompletedProcess(
                    args, 1, "", f"Error: No such container: {ref}")
            return subprocess.CompletedProcess(
                args, 0, f"{cid} {'true' if state == 'running' else 'false'}",
                "")
        if args[0] == "exec":
            # A20: every ACS exec is `exec -w / <cid> python3 -I -S -c ...`
            assert args[1:3] == ("-w", "/") and \
                args[4:8] == ("python3", "-I", "-S", "-c"), args[:8]
        if args[0] == "exec" and args[8] == procs._JOB_SCAN:
            scan = scans.get(args[3])
            if scan is None:
                return subprocess.CompletedProcess(args, 1, "", "exec failed")
            return subprocess.CompletedProcess(args, 0, json.dumps(scan), "")
        if args[0] == "exec" and args[8] == procs._JOB_SIGNAL:
            return subprocess.CompletedProcess(
                args, 0, json.dumps({"groups": [], "pids": [30],
                                     "skipped": []}), "")
        raise AssertionError(args)
    monkeypatch.setattr(procs, "docker", fake)


def live_scan(*processes):
    scan = fake_scan(*processes)
    scan["now"] = time.time()
    scan["btime"] = 0.0
    return scan


REBOUND_RECORD = {"job_id": JOB, "pid": 4242, "container_pid": 7,
                  "started_utc": "2026-09-27T12:00:00Z"}


def test_name_rebound_to_another_container_is_scanned(monkeypatch):
    """The guard execs by NAME: if the name now points at a different
    running container, a gone/stopped recorded id is not enough."""
    holder = proc(30, env=procs.HAS_MARKER)
    stub_docker(monkeypatch, recorded="gone",
                scans={OTHER_ID: live_scan(INIT, holder,
                                           proc(31, sid=30, pgrp=30))})
    cont = {"name": "c", "id": RECORDED}
    ev = procs.container_job_evidence(cont, REBOUND_RECORD)
    assert ev["verified"] is True and ev["clean"] is False, ev
    assert ev["rebound_container"] == OTHER_ID
    assert {m["pid"] for m in ev["members"]} == {30, 31}
    # the recorded container stopped: same
    stub_docker(monkeypatch, recorded="stopped",
                scans={OTHER_ID: live_scan(INIT, holder)})
    assert procs.container_job_evidence(cont, REBOUND_RECORD)["clean"] is False
    # nothing of the job in the rebound container: clean
    stub_docker(monkeypatch, recorded="gone",
                scans={OTHER_ID: live_scan(INIT, proc(30))})
    ev = procs.container_job_evidence(cont, REBOUND_RECORD)
    assert ev["clean"] is True and ev["rebound_members"] == []
    # nothing bound to the name / name bound to a stopped container: fine
    for by_name in ("gone", "stopped"):
        stub_docker(monkeypatch, recorded="gone", by_name=by_name)
        assert procs.container_job_evidence(cont, REBOUND_RECORD)[
            "clean"] is True


@pytest.mark.parametrize("case", ["scan-fails", "unsettled", "unreadable",
                                  "no-marker", "name-lookup-error"])
def test_name_rebound_unobservable_is_unverifiable(monkeypatch, case):
    scan = live_scan(INIT)
    if case == "unsettled":
        scan["settled"] = False
    if case == "unreadable":
        scan = live_scan(INIT, proc(40, at=5, env=procs.UNREADABLE))
        scan["now"] = T0 + 100      # keep the job-relative clock
    scans = {} if case == "scan-fails" else {OTHER_ID: scan}
    record = dict(REBOUND_RECORD)
    if case == "no-marker":
        record["job_id"] = None
    stub_docker(monkeypatch, recorded="gone", scans=scans,
                name_err=("Cannot connect to the Docker daemon"
                          if case == "name-lookup-error" else None))
    ev = procs.container_job_evidence({"name": "c", "id": RECORDED}, record)
    assert ev["verified"] is False and ev["clean"] is False, ev


def test_signal_runs_in_the_container_that_holds_the_marker(monkeypatch):
    calls: list = []
    stub_docker(monkeypatch, recorded="gone", calls=calls,
                scans={OTHER_ID: live_scan(INIT, proc(
                    30, env=procs.HAS_MARKER))})
    done = procs.signal_container_job({"name": "c", "id": RECORDED},
                                      REBOUND_RECORD, None, 15)
    assert done["signalled"] is True, done
    signal_calls = [c for c in calls if c[0] == "exec" and
                    c[8] == procs._JOB_SIGNAL]
    assert [c[3] for c in signal_calls] == [OTHER_ID]


# -- reconcile on crafted blocks (fake unreachable docker) ------------------------

def craft_job(env, container, record, *, samples=True, state=None) -> str:
    """A blocked job with a job dir, manifest, state (runner gone) and a
    guard record: only the evidence decides."""
    job_id = f"acs-20260927T120000Z-{secrets.token_hex(3)}"
    _craft_block(env, job_id, container, record, samples=samples)
    (env.state / "jobs" / job_id / "state.json").write_text(json.dumps(
        {"job_id": job_id, "phase": "cleanup", "history": [],
         **(state or {})}))
    return job_id


@pytest.mark.parametrize("shape", list(SHAPES) + ["with-job-id", "list"])
def test_reconcile_with_state_never_clears_without_observation(
        shape, env_direct, fake_docker_path):
    env = env_direct
    record = {"list": ["x"],
              "with-job-id": {"job_id": JOB, "pid": 4242,
                              "container_pid": None, "outcome": "completed",
                              "started_utc": "2026-09-27T12:00:00Z"}}.get(
        shape, SHAPES.get(shape))
    job_id = craft_job(env, ABSENT, None if record is MISSING else record)
    result = env.acs("reconcile", "--json",
                     env=env.env(PATH=fake_docker_path))
    rec = json.loads(result.stdout)
    assert rec["blocked"] is True and blocked(env), rec
    assert rec["unresolved"][0]["job_id"] == job_id
    assert rec["unresolved"][0]["blocker"]


@pytest.mark.parametrize("shape", ["list", "pid-string", "outcome-list",
                                   "container_pid-string", "outcome-int"])
def test_reconcile_malformed_record_stays_blocked_with_reason(
        shape, env_direct, fake_docker_path):
    env = env_direct
    record = ["x"] if shape == "list" else SHAPES[shape]
    craft_job(env, ABSENT, record)
    rec = json.loads(env.acs("reconcile", "--json",
                             env=env.env(PATH=fake_docker_path)).stdout)
    assert rec["blocked"] is True
    assert "malformed" in rec["unresolved"][0]["blocker"], rec


def test_reconcile_docker_unreachable_keeps_block(env_direct,
                                                  fake_docker_path):
    env = env_direct
    craft_job(env, ABSENT, {"job_id": JOB, "pid": 4242, "container_pid": 17,
                            "outcome": "completed",
                            "started_utc": "2026-09-27T12:00:00Z"})
    rec = json.loads(env.acs("reconcile", "--json",
                             env=env.env(PATH=fake_docker_path)).stdout)
    assert rec["blocked"] is True
    cleanup = rec["unresolved"][0]["cleanup"]
    assert cleanup["verified"] is False and "docker" in cleanup["reason"]


@pytest.mark.parametrize("missing", ["manifest", "state"])
def test_reconcile_missing_manifest_or_state_keeps_block(
        env_direct, fake_docker_path, missing):
    env = env_direct
    job_id = craft_job(env, ABSENT, None, samples=False)
    name = "manifest.json" if missing == "manifest" else "state.json"
    (env.state / "jobs" / job_id / name).unlink()
    rec = json.loads(env.acs("reconcile", "--json",
                             env=env.env(PATH=fake_docker_path)).stdout)
    assert rec["blocked"] is True
    assert "no job evidence" in rec["unresolved"][0]["blocker"]


def test_reconcile_never_launched_needs_positive_evidence(env_direct,
                                                          fake_docker_path):
    """Job dir present, no pid, no samples.csv: never launched; resolved
    with a durable history entry."""
    env = env_direct
    job_id = craft_job(env, ABSENT, {"pid": None, "outcome": "refused"},
                       samples=False)
    rec = json.loads(env.acs("reconcile", "--json",
                             env=env.env(PATH=fake_docker_path)).stdout)
    assert rec["blocked"] is False, rec
    history = env.state_of(job_id)["history"]
    assert history[-1]["event"] == "reconciled"
    assert history[-1]["evidence"]["method"] == "never-launched"


# -- live containers ---------------------------------------------------------------

def utc(epoch: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch))


@needs_docker
def test_fast_job_without_descendants_succeeds_with_scan_evidence(
        env_queue, make_container):
    """Recoverability (the intent of the retired PR #2/#3 tests): a fast
    container job with nothing left behind is `succeeded` and the host
    stays unblocked, on scan evidence rather than the record."""
    env = env_queue
    name = make_container()
    env.set_container(name)
    job = submit_container(env, name, ["bash", "-c", "exit 0"])
    state = env.wait_outcome(job["job_id"], timeout=60)
    report("fast-clean", outcome=state["outcome"], cleanup=state["cleanup"])
    assert state["outcome"] == "succeeded", state
    assert state["cleanup"]["clean"] is True
    assert state["cleanup"]["method"] == "marker-scan"
    assert state["cleanup"]["guard_job_id"] == guard_record(
        env, job["job_id"])["job_id"]
    assert not blocked(env)


@needs_docker
@pytest.mark.parametrize("outcome,expected", [("completed", "succeeded"),
                                              ("running", "unknown")])
def test_reconcile_resolves_fast_job_block_on_scan_evidence(
        env_direct, make_container, outcome, expected):
    """The SK7 record shape (pid set, container_pid null) with nothing of
    the job left in the container: reconcile resolves it on a scan, and
    records the evidence durably."""
    env = env_direct
    name = make_container()
    record = {"job_id": JOB, "pid": 70704, "container_pid": None,
              "outcome": outcome, "exit_code": 0,
              "started_utc": utc(time.time() - 5)}
    job_id = craft_job(env, {"name": name, "id": procs.container_id(name),
                             "workdir": "/work"}, record)
    rec = json.loads(env.acs("reconcile", "--json").stdout)
    assert rec["blocked"] is False, rec
    st = env.state_of(job_id)
    assert st["outcome"] == expected
    assert st["history"][-1]["event"] == "reconciled"
    assert st["history"][-1]["evidence"]["method"] == "marker-scan"


# guard.py reproduction, legacy profile (internal-guard counterpart:
# test_acs_guard_containers.py::test_same_session_survivor_blocks_and_reconcile_resolves_the_marker)
@needs_legacy
@needs_docker
def test_guard_py_fast_job_survivor_is_not_clean_until_it_exits(
        env_direct, make_container):
    """guard.py ends a fast container job ``completed`` while a same-session
    child it never tracked still runs.  Until 0.2.0 ACS ran guard.py as a
    backend, blocked on this and resolved the block once the survivor
    exited; now guard.py runs directly, and ACS's cleanup evidence (the one
    function the runner and reconcile share) is shown to say not clean
    while the survivor lives and clean once it has exited."""
    env = env_direct
    name = make_container()
    tag = f"acs-surv-{secrets.token_hex(3)}"
    script = f"( exec -a {tag} sleep 40 ) >/dev/null 2>&1 </dev/null & exit 0"
    guard, out = env.legacy_container(name, ["bash", "-c", script])
    assert guard.wait(90) == 0, guard.stdout.read()
    record = json.loads((out / "guard.json").read_text())
    container = {"name": name, "id": procs.container_id(name),
                 "workdir": "/work"}
    assert scan(name, tag), "precondition: survivor alive"
    assert record["outcome"] == "completed"
    first = procs.container_job_evidence(container, record)
    assert first["clean"] is False, first
    assert first["members"], first
    env.wait_for(lambda: not scan(name, tag), timeout=60,
                 what="survivor exit")
    second = procs.container_job_evidence(container, record)
    report("resolve-after-exit", first=first, second=second)
    assert second["clean"] is True, second
    assert second.get("method") == "marker-scan", second


@needs_docker
def test_terminate_signals_only_marker_processes(env_direct, make_container):
    env = env_direct
    name = make_container()
    env.set_container(name)
    tag = f"acs-term-{secrets.token_hex(3)}"
    script = (f"( exec -a {tag} sleep 300 ) >/dev/null 2>&1 </dev/null & "
              "exit 0")
    job = submit_container(env, name, ["bash", "-c", script])
    # decoys: same name, no marker / another job's marker
    subprocess.run([REAL_DOCKER, "exec", "-d", name, "bash", "-c",
                    f"exec -a {tag} sleep 300"], check=True)
    subprocess.run([REAL_DOCKER, "exec", "-d", "-e",
                    "SMC_GUARD_JOB=other.c/20260101T000000Z", name, "bash",
                    "-c", f"exec -a {tag} sleep 300"], check=True)
    state = env.wait_outcome(job["job_id"], timeout=60)
    guard_job = guard_record(env, job["job_id"])["job_id"]
    env.wait_for(lambda: len(scan(name, tag)) == 3, timeout=20,
                 what="survivor and two decoys")
    before = scan(name, tag)
    assert state["outcome"] == "unknown" and blocked(env)
    rec = json.loads(env.acs("reconcile", "--terminate", job["job_id"],
                             "--json").stdout)
    after = scan(name, tag)
    report("terminate", rec=rec, before=before, after=after)
    assert rec["blocked"] is False, rec
    assert [r for r in before if r["job"] == guard_job]
    assert not [r for r in after if r["job"] == guard_job]
    assert len(after) == 2 and all(r["job"] != guard_job for r in after)


@needs_docker
def test_unreadable_environ_after_the_job_is_unverifiable(make_container):
    """In-container root without CAP_SYS_PTRACE cannot read another uid's
    environ: such a process may be the job's, so no verdict."""
    name = make_container()
    cid = procs.container_id(name)
    start = time.time() - 1
    subprocess.run([REAL_DOCKER, "exec", "-d", "--user", "1000", name,
                    "sleep", "120"], check=True)
    record = {"job_id": JOB, "pid": 1, "started_utc": utc(start)}
    ev = {}
    for _ in range(20):
        ev = procs.container_job_evidence({"name": name, "id": cid}, record)
        if ev.get("unreadable"):
            break
        time.sleep(0.5)
    report("unreadable", evidence=ev)
    assert ev["verified"] is False and ev["clean"] is False
    assert ev["unreadable"]


@needs_docker
def test_unreadable_environ_before_the_job_is_ignored(make_container):
    name = make_container()
    cid = procs.container_id(name)
    subprocess.run([REAL_DOCKER, "exec", "-d", "--user", "1000", name,
                    "sleep", "120"], check=True)
    time.sleep(10)
    record = {"job_id": JOB, "pid": 1, "started_utc": utc(time.time())}
    ev = procs.container_job_evidence({"name": name, "id": cid}, record)
    report("unreadable-before", evidence=ev)
    assert ev["verified"] is True and ev["clean"] is True, ev


@needs_docker
def test_successor_never_starts_while_survivor_lives(env_queue,
                                                     make_container):
    env = env_queue
    name = make_container()
    env.set_container(name)
    tag = f"acs-succ-{secrets.token_hex(3)}"
    script = f"( exec -a {tag} sleep 45 ) >/dev/null 2>&1 </dev/null & exit 0"
    first = submit_container(env, name, ["bash", "-c", script])
    marker = env.root / "successor.jsonl"
    second = env.submit(workload_args=["--out", str(marker)])
    env.wait_outcome(first["job_id"], timeout=60)
    observed = []
    while scan(name, tag):
        observed.append((marker.exists(),
                         env.state_of(second["job_id"]).get("outcome")))
        assert not marker.exists(), "successor ran while survivor lived"
        time.sleep(1)
    report("successor", observed=observed[-3:])
    assert observed, "precondition: survivor was alive"
    rec = json.loads(env.acs("reconcile", "--json").stdout)
    assert rec["blocked"] is False, rec
    assert env.acs("resume", "--json").returncode == 0
    third = env.submit(workload_args=["--out", str(env.root / "t.jsonl")])
    assert env.wait_outcome(third["job_id"])["outcome"] == "succeeded"


@needs_docker
def test_limitation_env_scrubbed_setsid_descendant_escapes(env_direct,
                                                           make_container):
    """The remaining container boundary, pinned so the docs stay honest: a
    descendant that BOTH drops its environment (`env -i`) AND leaves the
    session carries no marker and shares no session/group with one."""
    env = env_direct
    name = make_container()
    env.set_container(name)
    tag = f"acs-scrub-{secrets.token_hex(3)}"
    code = ("import subprocess, time; subprocess.Popen(['env', '-i', "
            f"'bash', '-c', 'exec -a {tag} sleep 60'], "
            "start_new_session=True, stdin=subprocess.DEVNULL, "
            "stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL); "
            "time.sleep(2)")
    job = submit_container(env, name, ["python3", "-c", code])
    state = env.wait_outcome(job["job_id"], timeout=60)
    escaped = scan(name, tag)
    report("scrub-escape", outcome=state["outcome"],
           cleanup=state["cleanup"], escaped=escaped)
    assert escaped and escaped[0]["job"] is None
    assert escaped[0]["sid"] == escaped[0]["pid"]
    assert state["cleanup"]["clean"] is True           # not detected


@needs_docker
def test_name_rebound_live_survivor_is_found_and_terminated(env_direct,
                                                           make_container):
    """Container A (recorded id) is removed; B takes its name and holds a
    marker-carrying process: not clean; `reconcile --terminate` kills it
    in B, then the block resolves."""
    env = env_direct
    name = make_container()
    recorded_id = procs.container_id(name)
    subprocess.run([REAL_DOCKER, "rm", "-f", name], check=True,
                   capture_output=True)
    subprocess.run([REAL_DOCKER, "run", "-d", "--rm", "--name", name,
                    "--memory", "1g", "--cpus", "1", "--label",
                    "agent-compute-test=1", IMAGE,
                    "sleep", "3600"], check=True, capture_output=True)
    assert procs.container_id(name) != recorded_id
    tag = f"acs-rebound-{secrets.token_hex(3)}"
    start = time.time() - 2
    subprocess.run([REAL_DOCKER, "exec", "-d", "-e", f"SMC_GUARD_JOB={JOB}",
                    name, "bash", "-c", f"exec -a {tag} sleep 300"],
                   check=True)
    env.wait_for(lambda: scan(name, tag), timeout=20, what="marker process")
    container = {"name": name, "id": recorded_id, "workdir": "/work"}
    record = {"job_id": JOB, "pid": 4242, "container_pid": None,
              "outcome": "completed", "exit_code": 0,
              "started_utc": utc(start)}
    ev = procs.container_job_evidence(container, record)
    job_id = craft_job(env, container, record)
    rec = json.loads(env.acs("reconcile", "--terminate", job_id,
                             "--json").stdout)
    left = scan(name, tag)
    report("rebound", evidence=ev, reconcile=rec, left=left)
    assert ev["clean"] is False and ev["rebound_members"], ev
    assert rec["blocked"] is False, rec
    assert not left
    assert env.state_of(job_id)["history"][-1]["event"] == "reconciled"
