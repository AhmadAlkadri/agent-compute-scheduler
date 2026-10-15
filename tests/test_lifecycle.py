"""4. Lifecycle failures: submitting client, runner, guard, queue daemon."""

import json
import os
import signal
import subprocess
import sys
import time

import pytest

from conftest import PUEUE, WORKLOAD, events


def test_killing_the_submitting_client_does_not_stop_the_job(env_any):
    env = env_any
    out = env.root / "w.jsonl"
    client = env.acs_bg("run", "--json", "--case", "t", "--timeout-min", "1",
                        "--reserve-gb", "0.5", "--cwd",
                        str(env.projects["alpha"]), "--", sys.executable,
                        str(WORKLOAD), "--out", str(out), "--sleep", "3")
    env.wait_for(lambda: out.exists(), what="workload start")
    client.kill()
    job_id = sorted(p.name for p in (env.state / "jobs").iterdir())[0]
    state = env.wait_outcome(job_id)
    assert state["outcome"] == "succeeded", state
    assert [e["event"] for e in events(out)] == ["start", "end"]


def test_killing_the_runner_blocks_host_until_reconciled(env_any):
    env = env_any
    out, later_out = env.root / "w.jsonl", env.root / "later.jsonl"
    job = env.submit(workload_args=["--out", str(out), "--sleep", "4"])
    state = env.wait_phase(job["job_id"], "executing")
    os.kill(state["runner"]["pid"], signal.SIGKILL)
    # guard and workload continue: the guard still holds the lock
    assert not env.lock_free()
    status = json.loads(env.acs("status", "--json").stdout)
    assert job["job_id"] in status["admission"]["lost_supervision"]
    assert job["job_id"] in status["admission"]["blocked"]["jobs"]
    later = env.submit("beta", workload_args=["--out", str(later_out)])
    # while blocked, the reconcile refuses to clear a live guard
    rec = json.loads(env.acs("reconcile", "--json").stdout)
    assert rec["blocked"] is True
    assert "guard process is alive" in rec["unresolved"][0]["blocker"]
    env.wait_for(lambda: env.lock_free(), timeout=30, what="guard finishes")
    rec = json.loads(env.acs("reconcile", "--json").stdout)
    assert rec["blocked"] is False, rec
    final = env.state_of(job["job_id"])
    assert final["outcome"] == "succeeded"          # from the guard record
    assert "reconciled after lost supervision" in final["reason"]
    if env.queue:
        # the paused shared group kept the later job queued
        assert env.state_of(later["job_id"]).get("phase") == "queued"
        assert json.loads(env.acs("resume", "--json").stdout)["resumed"]
        assert env.wait_outcome(later["job_id"])["outcome"] == "succeeded"
    else:
        # the detached runner reached dispatch while the host was blocked
        later_state = env.wait_outcome(later["job_id"])
        assert later_state["outcome"] == "refused"
        assert "blocked" in later_state["reason"]
        assert not later_out.exists()
    assert [e["event"] for e in events(out)] == ["start", "end"]


def test_killing_the_guard_leaves_a_durable_block(env_any):
    env = env_any
    out = env.root / "w.jsonl"
    job = env.submit(workload_args=["--out", str(out), "--sleep", "120"])
    state = env.wait_phase(job["job_id"], "executing")
    workload_pid = state["workload"]["pid"]
    os.kill(state["guard"]["pid"], signal.SIGKILL)
    final = env.wait_outcome(job["job_id"])
    assert final["outcome"] == "unknown", final
    assert final["cleanup"]["clean"] is False
    # the kernel released the flock, but ACS admission stays blocked
    assert env.lock_free()
    block = json.loads(env.acs("status", "--json").stdout)["admission"]["blocked"]
    assert job["job_id"] in block["jobs"]
    os.kill(workload_pid, 0)                        # still running
    rec = json.loads(env.acs("reconcile", "--json").stdout)
    assert rec["blocked"] is True
    assert "workload processes remain" in rec["unresolved"][0]["blocker"]
    blocked = env.submit("beta", workload_args=["--out", str(env.root / "n")])
    if not env.queue:
        assert env.wait_outcome(blocked["job_id"])["outcome"] == "refused"
    rec = json.loads(env.acs("reconcile", "--terminate", job["job_id"],
                             "--json").stdout)
    assert rec["blocked"] is False, rec
    with pytest.raises(ProcessLookupError):
        os.kill(workload_pid, 0)
    assert env.state_of(job["job_id"])["outcome"] == "unknown"   # not success
    if env.queue:
        env.acs("resume", check=True)
        assert env.wait_outcome(blocked["job_id"])["outcome"] == "succeeded"


def test_killing_the_queue_daemon(env_queue):
    env = env_queue
    a_out, b_out = env.root / "a.jsonl", env.root / "b.jsonl"
    a = env.submit("alpha", workload_args=["--out", str(a_out), "--sleep", "4"])
    b = env.submit("beta", workload_args=["--out", str(b_out), "--sleep", "0"])
    env.wait_phase(a["job_id"], "executing")
    pid = int((env.state / "pueue" / "run" / "pueue.pid").read_text())
    os.kill(pid, signal.SIGKILL)
    # the runner keeps supervising without the daemon
    assert env.wait_outcome(a["job_id"])["outcome"] == "succeeded"
    started = json.loads(env.acs("daemon", "start", "--json").stdout)
    assert any("dispatch gate closed" in n for n in started["notices"])
    time.sleep(3)
    assert env.state_of(b["job_id"]).get("phase") == "queued", \
        "pending work must not start automatically after a daemon restart"
    assert not b_out.exists()
    assert json.loads(env.acs("resume", "--json").stdout)["resumed"]
    assert env.wait_outcome(b["job_id"])["outcome"] == "succeeded"
    assert [e["event"] for e in events(a_out)] == ["start", "end"]


def _pueue(env, *args):
    return subprocess.run([PUEUE, "-c", str(env.state / "pueue" /
                                            "pueue.yml"), *args],
                          capture_output=True, text=True)


def test_restarting_a_finished_task_does_not_duplicate_it(env_queue):
    env = env_queue
    out = env.root / "w.jsonl"
    job = env.submit(workload_args=["--out", str(out)])
    state = env.wait_outcome(job["job_id"])
    assert state["outcome"] == "succeeded"
    assert _pueue(env, "restart", str(state["pueue_task"])).returncode == 0
    time.sleep(4)
    assert len(events(out)) == 2          # one start, one end: not rerun
    assert env.state_of(job["job_id"])["outcome"] == "succeeded"


def test_redispatch_of_an_uncertain_job_is_refused_and_blocks(env_queue):
    env = env_queue
    out = env.root / "w.jsonl"
    job = env.submit(workload_args=["--out", str(out), "--sleep", "3"])
    state = env.wait_phase(job["job_id"], "executing")
    os.kill(state["runner"]["pid"], signal.SIGKILL)
    env.wait_for(lambda: env.lock_free(), timeout=30, what="guard finishes")
    assert _pueue(env, "restart", str(state["pueue_task"])).returncode == 0
    time.sleep(4)
    assert [e["event"] for e in events(out)] == ["start", "end"]
    block = json.loads(env.acs("status", "--json").stdout)["admission"]["blocked"]
    assert job["job_id"] in block["jobs"]


def test_request_id_prevents_duplicate_resubmission(env_any):
    env = env_any
    out = env.root / "w.jsonl"
    first = env.submit(workload_args=["--out", str(out), "--sleep", "1"],
                       extra=["--request-id", "slice-7-step-3"])
    again = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                    "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]),
                    "--request-id", "slice-7-step-3", "--",
                    sys.executable, str(WORKLOAD), "--out", str(out))
    assert again.returncode == 2
    assert json.loads(again.stdout)["job_id"] == first["job_id"]
    env.wait_outcome(first["job_id"])
    assert len(events(out)) == 2
