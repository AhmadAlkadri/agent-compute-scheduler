"""5. Child processes and the Docker `docker exec` lifecycle boundary.

Docker tests use a disposable container with a unique name, created and
removed here.  They never touch an existing (scientific) container.
"""

import json
import os
import secrets
import shutil
import signal
import subprocess
import time

import pytest

from conftest import TEST_IMAGE as IMAGE  # noqa: F401 (re-exported)
from conftest import events, needs_docker, needs_legacy


def pids_of(path):
    text = path.read_text() if path.exists() else ""
    return [int(line.split()[1]) for line in text.splitlines() if line.strip()]


def alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    out = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)],
                         capture_output=True, text=True).stdout.strip()
    return bool(out) and not out.startswith("Z")


def test_cancel_reaps_children_and_grandchildren(env_any):
    env = env_any
    out = env.root / "w.jsonl"
    job = env.submit(workload_args=["--out", str(out), "--sleep", "60",
                                    "--children"])
    env.wait_phase(job["job_id"], "executing")
    pidfile = env.root / "w.jsonl.pids"
    env.wait_for(lambda: len(pids_of(pidfile)) == 2, what="child+grandchild")
    family = pids_of(pidfile) + [events(out)[0]["pid"]]
    result = json.loads(env.acs("cancel", job["job_id"], "--json").stdout)
    assert result["cancelled"], result
    env.wait_for(lambda: not any(alive(p) for p in family), timeout=20,
                 what="whole process group gone")
    assert env.state_of(job["job_id"])["cleanup"]["clean"] is True


def test_guard_death_with_children_then_targeted_terminate(env_any):
    env = env_any
    out = env.root / "w.jsonl"
    job = env.submit(workload_args=["--out", str(out), "--sleep", "120",
                                    "--children"])
    state = env.wait_phase(job["job_id"], "executing")
    pidfile = env.root / "w.jsonl.pids"
    env.wait_for(lambda: len(pids_of(pidfile)) == 2, what="child+grandchild")
    family = pids_of(pidfile) + [state["workload"]["pid"]]
    os.kill(state["guard"]["pid"], signal.SIGKILL)
    final = env.wait_outcome(job["job_id"])
    assert final["outcome"] == "unknown"
    assert set(family) <= set(final["cleanup"]["members"])
    rec = json.loads(env.acs("reconcile", "--terminate", job["job_id"],
                             "--json").stdout)
    assert rec["blocked"] is False, rec
    assert not any(alive(p) for p in family)


def test_limitation_a_setsid_escape_is_not_tracked(env_direct):
    """Documented limitation (docs/execution-paths.md): a descendant that
    starts its own session leaves the tracked group on macOS and Linux.
    This test pins the behaviour so the documentation stays honest."""
    env = env_direct
    marker = env.root / "escaped.pid"
    script = env.projects["alpha"] / "escape.py"
    script.write_text(
        "import subprocess, sys, time\n"
        "p = subprocess.Popen([sys.executable, '-c', 'import time; "
        "time.sleep(60)'], start_new_session=True)\n"
        f"open({str(marker)!r}, 'w').write(str(p.pid))\n")
    import sys
    result = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]),
                     "--", sys.executable, str(script))
    state = env.wait_outcome(json.loads(result.stdout)["job_id"])
    escaped = int(marker.read_text())
    try:
        assert state["outcome"] == "succeeded"
        assert state["cleanup"]["clean"] is True      # not detected
        assert alive(escaped)                          # but still running
    finally:
        os.kill(escaped, signal.SIGKILL)


# --- Docker -------------------------------------------------------------------------

@pytest.fixture
def container():
    name = f"acs-test-{secrets.token_hex(4)}"
    subprocess.run(["docker", "run", "-d", "--rm", "--name", name,
                    "--memory", "1g", "--cpus", "1", "--label",
                    "agent-compute-test=1", IMAGE, "sleep", "3600"],
                   check=True, capture_output=True)
    subprocess.run(["docker", "exec", name, "mkdir", "-p", "/work"],
                   check=True, capture_output=True)
    yield name
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def in_container_alive(name, pattern):
    out = subprocess.run(["docker", "exec", name, "python3", "-c",
                          "import os,sys\n"
                          "for p in os.listdir('/proc'):\n"
                          "  if p.isdigit():\n"
                          "    try: c=open('/proc/%s/cmdline'%p).read()\n"
                          "    except OSError: continue\n"
                          "    if sys.argv[1] in c and 'import os,sys' not in c: print(p)",
                          pattern], capture_output=True, text=True)
    return [int(x) for x in out.stdout.split()]


def submit_container(env, name, seconds, tag):
    env.set_container(name)
    result = env.acs("submit", "--json", "--case", "c", "--timeout-min", "2",
                     "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]),
                     "--container", name, "--",
                     "python3", "-c",
                     f"import time; time.sleep({seconds})  # {tag}")
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


@needs_docker
def test_container_job_succeeds(env_any, container):
    env = env_any
    job = submit_container(env, container, 1, "ok-tag")
    state = env.wait_outcome(job["job_id"], timeout=120)
    assert state["outcome"] == "succeeded", state
    assert state["cleanup"]["clean"] is True


@needs_docker
def test_container_cancel_stops_the_in_container_workload(env_any, container):
    env = env_any
    job = submit_container(env, container, 300, "cancel-tag")
    state = env.wait_phase(job["job_id"], "executing", timeout=120)
    assert state["workload"]["container_pid"]
    assert in_container_alive(container, "cancel-tag")
    result = json.loads(env.acs("cancel", job["job_id"], "--json").stdout)
    assert result["cancelled"], result
    assert not in_container_alive(container, "cancel-tag")


@needs_docker
def test_orphaned_docker_exec_regression(env_any, container):
    """The SL5 failure class (rerender.md, 2026-09-25): the host-side
    supervision (runner, guard and docker client) dies; the `docker exec`'d
    process keeps running inside the container and the flock is released.
    ACS must block the host and be able to terminate exactly that process."""
    env = env_any
    job = submit_container(env, container, 300, "orphan-tag")
    state = env.wait_phase(job["job_id"], "executing", timeout=120)
    runner_pgid = os.getpgid(state["runner"]["pid"])
    os.killpg(runner_pgid, signal.SIGKILL)      # runner + guard + docker CLI
    time.sleep(2)
    assert in_container_alive(container, "orphan-tag"), \
        "expected the documented orphan: killing the host side leaves it"
    assert env.lock_free()                      # the flock alone says "free"
    status = json.loads(env.acs("status", "--json").stdout)
    assert job["job_id"] in status["admission"]["blocked"]["jobs"]
    rec = json.loads(env.acs("reconcile", "--json").stdout)
    assert rec["blocked"] is True
    assert rec["unresolved"][0]["cleanup"]["members"], rec
    rec = json.loads(env.acs("reconcile", "--terminate", job["job_id"],
                             "--json").stdout)
    assert rec["blocked"] is False, rec
    assert not in_container_alive(container, "orphan-tag")


@needs_docker
def test_guard_death_on_container_path_blocks(env_any, container):
    env = env_any
    job = submit_container(env, container, 300, "guard-tag")
    state = env.wait_phase(job["job_id"], "executing", timeout=120)
    os.kill(state["guard"]["pid"], signal.SIGKILL)
    final = env.wait_outcome(job["job_id"], timeout=60)
    assert final["outcome"] == "unknown", final
    assert in_container_alive(container, "guard-tag")
    rec = json.loads(env.acs("reconcile", "--terminate", job["job_id"],
                             "--json").stdout)
    assert rec["blocked"] is False, rec
    assert not in_container_alive(container, "guard-tag")


# guard.py reproduction, legacy profile (internal-guard counterpart:
# test_guard_lifecycle.py::test_acs_cancel_of_an_internal_guard_container_job_kills_its_children)
@needs_legacy
@needs_docker
def test_guard_py_group_kill_gap_in_dash_containers(env_direct, container):
    """Finding (2026-09-26): guard.py signals an in-container job with
    `kill -SIG -- -PGID`, which Debian/Ubuntu dash rejects, so only the
    leader dies and its children survive a guard cancel (SIGTERM, what
    ACS's cancel sent it).  Until 0.2.0 ACS ran guard.py as a backend and
    caught this (block, terminate); since then the engine is the only
    guard, so the defect is reproduced on guard.py directly, and ACS's
    cleanup evidence is shown not to call its record clean."""
    from agent_compute import procs
    env = env_direct
    code = ("import subprocess, time; "
            "subprocess.Popen(['python3', '-c', 'import time; "
            "time.sleep(300)  # child-tag']); time.sleep(300)  # parent-tag")
    guard, out = env.legacy_container(container, ["python3", "-c", code],
                                      timeout_min="2")
    record = env.wait_for(
        lambda: (lambda r: r if r.get("container_pid") else None)(
            json.loads((out / "guard.json").read_text())
            if (out / "guard.json").is_file() else {}),
        timeout=120, what="guard.py's record with the in-container pid")
    env.wait_for(lambda: in_container_alive(container, "child-tag"),
                 what="in-container child")
    guard.send_signal(signal.SIGTERM)
    guard.wait(90)
    record = json.loads((out / "guard.json").read_text())
    assert not in_container_alive(container, "parent-tag")
    assert in_container_alive(container, "child-tag")      # the guard gap
    evidence = procs.container_job_evidence(
        {"name": container, "id": procs.container_id(container),
         "workdir": "/work"}, record)
    assert evidence["clean"] is False, evidence
