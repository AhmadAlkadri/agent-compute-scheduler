"""The test harness's teardown signals only processes whose identity it has
proven (register A60, campaign PA Z2 finding Z2-25).

``Env.cleanup`` kills what a test's jobs recorded.  A recorded host workload
used to be killed by process group on its pid alone, so a test that stored an
invented pid, or a pid that another process had meanwhile taken, made the
harness SIGKILL something it never started.  The teardown now requires the
same proof it already required for guard and runner pids
(``procs.same_process``: the recorded start matches the live process).
"""
from __future__ import annotations

import json
import subprocess
import sys
import time

from agent_compute import procs


def _victim() -> subprocess.Popen:
    # its own session and process group, like a real host workload
    return subprocess.Popen([sys.executable, "-c",
                             "import time; time.sleep(120)"],
                            start_new_session=True)


def _record(env, job: str, workload: dict) -> None:
    job_dir = env.state / "jobs" / job
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "state.json").write_text(json.dumps(
        {"job_id": job, "phase": "done", "outcome": "cancelled",
         "workload": workload}))


def _wait_exit(proc: subprocess.Popen, seconds: float) -> int | None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return proc.returncode
        time.sleep(0.05)
    return proc.poll()


def test_cleanup_leaves_an_unproven_host_workload_pid_alone(env_direct):
    """A record with a pid but no start proves nothing: no signal."""
    victim = _victim()
    try:
        _record(env_direct, "acs-fake-unproven",
                {"venue": "host", "pid": victim.pid, "pgid": victim.pid})
        env_direct.cleanup()
        assert _wait_exit(victim, 1.0) is None, \
            "the teardown killed a process whose identity it never proved"
    finally:
        victim.kill()
        victim.wait(timeout=10)


def test_cleanup_kills_a_proven_host_workload_group(env_direct):
    """A record with the live process's own start is proven: the group is
    signalled, as before."""
    victim = _victim()
    try:
        ident = procs.identity(victim.pid)
        assert ident and ident.get("pid") == victim.pid
        ident = dict(ident, venue="host", pgid=victim.pid)
        _record(env_direct, "acs-fake-proven", ident)
        env_direct.cleanup()
        code = _wait_exit(victim, 5.0)
        assert code is not None and code < 0, \
            f"the proven workload was not signalled (returncode {code})"
    finally:
        if victim.poll() is None:
            victim.kill()
        victim.wait(timeout=10)
