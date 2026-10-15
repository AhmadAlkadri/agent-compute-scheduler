"""Regression tests for the independent review of c0bde9e."""

import json
import os
import signal
import subprocess
import sys
import time

import pytest

from agent_compute import procs
from conftest import PUEUE, WORKLOAD, Env, copy_tree, events


def test_block_is_written_before_a_waiting_runner_can_start(env_direct):
    """Review A2: the guard's flock is released when a guard dies; a second
    detached runner must still not launch beside the orphan."""
    env = env_direct
    a_out, b_out = env.root / "a.jsonl", env.root / "b.jsonl"
    a = env.submit("alpha", workload_args=["--out", str(a_out),
                                           "--sleep", "120"])
    state = env.wait_phase(a["job_id"], "executing")
    b = env.submit("beta", workload_args=["--out", str(b_out)])
    env.wait_for(lambda: "another ACS job" in
                 (env.state_of(b["job_id"]).get("wait_reason") or ""),
                 what="b waits on the ACS admission lock")
    os.kill(state["guard"]["pid"], signal.SIGKILL)
    assert env.wait_outcome(a["job_id"])["outcome"] == "unknown"
    b_state = env.wait_outcome(b["job_id"])
    assert b_state["outcome"] == "refused", b_state
    assert "blocked" in b_state["reason"]
    assert not b_out.exists()
    env.acs("reconcile", "--terminate", a["job_id"], check=True)


def test_inputs_rechecked_after_a_long_lock_wait(env_any):
    """Review A1: inputs are re-validated before every guard attempt."""
    env = env_any
    legacy_out = env.root / "legacy.jsonl"
    legacy = env.direct(legacy_out, sleep=6)
    env.wait_for(lambda: legacy_out.exists(), what="legacy start")
    out = env.root / "w.jsonl"
    job = env.submit(workload_args=["--out", str(out)])
    env.wait_for(lambda: "compute lock held by" in
                 (env.state_of(job["job_id"]).get("wait_reason") or ""),
                 what="lock wait")
    (env.projects["alpha"] / "input.txt").write_text("edited during wait\n")
    legacy.wait(60)
    state = env.wait_outcome(job["job_id"])
    assert state["outcome"] == "held-inputs-changed", state
    assert not out.exists()


def test_waiting_does_not_flood_the_project_runlog(env_direct):
    env = env_direct
    legacy_out = env.root / "legacy.jsonl"
    legacy = env.direct(legacy_out, sleep=8)
    env.wait_for(lambda: legacy_out.exists(), what="legacy start")
    job = env.submit(workload_args=["--out", str(env.root / "w")])
    legacy.wait(60)
    assert env.wait_outcome(job["job_id"])["outcome"] == "succeeded"
    runlog = (env.state / "projects" / "alpha" / "runlog.md").read_text()
    # D3 (G2b): no lock-busy line at all; the job's end line only
    assert "guard:lock-busy" not in runlog, runlog
    assert runlog.count(" | completed | ") == 1, runlog


def test_terminate_never_signals_an_unproven_pid(env_direct):
    """Review A3: a recorded pid whose start time does not match is not the
    job's; reconcile must refuse to signal it."""
    env = env_direct
    bystander = subprocess.Popen([sys.executable, "-c",
                                  "import time; time.sleep(120)"],
                                 start_new_session=True)
    env.procs.append(bystander)
    job = env.submit(workload_args=["--out", str(env.root / "w")])
    env.wait_outcome(job["job_id"])
    from agent_compute.state import JobStore
    store = JobStore(env.state)
    fake = {"venue": "host", "pid": bystander.pid, "start": None,
            "start_epoch": time.time() - 3600}      # wrong discriminator
    store.update_state(job["job_id"], workload=fake)
    store.set_block("testhost", job["job_id"], "test", {"workload": fake})
    rec = json.loads(env.acs("reconcile", "--terminate", job["job_id"],
                             "--json").stdout)
    assert bystander.poll() is None, "an unproven pid was signalled"
    # an epoch-only identity never proves a recycled pid (audit A1): the
    # mismatch is unverified, so the host stays blocked
    assert rec["blocked"] is True
    # an EXACT recorded start that the process at the pid provably
    # postdates is a recycled pid: clean, and still never signalled
    exact = {**procs.identity(bystander.pid), "venue": "host"}
    if exact.get("start_utc_s") is not None:
        exact["start_utc_s"] -= 3600
    else:
        exact["start"] = "ticks:%d" % (int(exact["start"][6:]) -
                                       3600 * os.sysconf("SC_CLK_TCK"))
    exact["start_epoch"] -= 3600
    store.update_state(job["job_id"], workload=exact)
    rec = json.loads(env.acs("reconcile", "--terminate", job["job_id"],
                             "--json").stdout)
    assert bystander.poll() is None, "an unproven pid was signalled"
    assert rec["blocked"] is False
    fake["start_epoch"] = None
    store.update_state(job["job_id"], workload=fake)
    store.set_block("testhost", job["job_id"], "test", {"workload": fake})
    rec = json.loads(env.acs("reconcile", "--terminate", job["job_id"],
                             "--json").stdout)
    assert bystander.poll() is None
    assert rec["blocked"] is True                    # no discriminator
    bystander.kill()


def test_guard_usage_error_before_launch_is_refused_not_unknown(tmp_path):
    """Review A4: a guard that exits before launching (argparse rc 2): an
    installed copy whose engine is a fake, pinned by the config."""
    env = Env(queue=False, tree=copy_tree(
        tmp_path, "import sys\nprint('usage: engine.py', file=sys.stderr)\n"
                  "sys.exit(2)\n"))
    try:
        job = env.submit(workload_args=["--out", str(env.root / "w")])
        state = env.wait_outcome(job["job_id"])
        assert state["outcome"] == "refused", state
        assert "never started" in state["reason"]
        status = json.loads(env.acs("status", "--json").stdout)
        assert status["admission"]["blocked"] is None
    finally:
        env.cleanup()


def test_cancel_does_not_overwrite_a_finished_outcome(env_any):
    env = env_any
    job = env.submit(workload_args=["--out", str(env.root / "w")])
    assert env.wait_outcome(job["job_id"])["outcome"] == "succeeded"
    result = json.loads(env.acs("cancel", job["job_id"], "--json").stdout)
    assert result["cancelled"] is False
    assert env.state_of(job["job_id"])["outcome"] == "succeeded"


def test_unresponsive_daemon_is_not_replaced(env_queue):
    """Review B1: never start a second pueued on the same state."""
    env = env_queue
    env.submit(workload_args=["--out", str(env.root / "w")])
    pid = int((env.state / "pueue" / "run" / "pueue.pid").read_text())
    os.kill(pid, signal.SIGSTOP)
    try:
        result = env.acs("submit", "--case", "t", "--timeout-min", "1",
                         "--reserve-gb", "0.5", "--cwd",
                         str(env.projects["alpha"]), "--", "true",
                         timeout=120)
        assert result.returncode == 2
        assert "not answering" in result.stdout + result.stderr
        out = subprocess.run(["pgrep", "-f", str(env.state / "pueue")],
                             capture_output=True, text=True).stdout.split()
        assert out == [str(pid)]
    finally:
        os.kill(pid, signal.SIGCONT)


def test_daemon_stop_refuses_while_a_job_runs(env_queue):
    """Review B4: `pueue shutdown` kills running tasks."""
    env = env_queue
    out = env.root / "w.jsonl"
    job = env.submit(workload_args=["--out", str(out), "--sleep", "4"])
    env.wait_phase(job["job_id"], "executing")
    result = json.loads(env.acs("daemon", "stop", "--json").stdout)
    assert result["stopped"] is False
    assert env.wait_outcome(job["job_id"])["outcome"] == "succeeded"
    assert json.loads(env.acs("daemon", "stop", "--json").stdout)["stopped"]


def test_block_pauses_an_existing_group(env_queue):
    """Review B5."""
    env = env_queue
    first = env.submit(workload_args=["--out", str(env.root / "a")])
    env.wait_outcome(first["job_id"])
    from agent_compute.state import JobStore
    JobStore(env.state).set_block("testhost", first["job_id"], "test", {})
    later = env.submit("beta", workload_args=["--out", str(env.root / "b")])
    time.sleep(3)
    assert env.state_of(later["job_id"]).get("phase") == "queued"
    assert not (env.root / "b").exists()


def test_daemon_survives_the_invoking_session(env_queue):
    """Observed on the Lenovo: a daemon forked inside the CLI's session died
    when the ssh -> WSL session ended."""
    env = env_queue
    proc = subprocess.Popen([sys.executable, str(__import__("conftest").BIN),
                             "--config", str(env.config), "daemon", "start"],
                            start_new_session=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, env=env.env())
    proc.wait(60)
    pid = int((env.state / "pueue" / "run" / "pueue.pid").read_text())
    try:
        os.killpg(proc.pid, signal.SIGHUP)      # the invoking session's group
    except ProcessLookupError:
        pass
    time.sleep(1)
    os.kill(pid, 0)                              # still alive
    assert json.loads(env.acs("daemon", "status", "--json").stdout)["running"]
