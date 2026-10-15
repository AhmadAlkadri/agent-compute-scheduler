"""Guard lifecycle fixes in the internal guard (campaign slice S3).

Each test here failed against the S2 engine (``e3afe7c``) and passes after
the fix (recorded in the private development evidence):

1. the container venue kills the whole in-container group under dash (and
   verifies it), where ``kill -TERM -- -PGID`` reached only the leader;
2. the host venue kills group members that outlive the leader, and never
   reports a job clean while its group still has live members;
3. SIGTERM/SIGINT are caught from before the launch: a signal right after
   ``Popen`` (host) or during the container pid discovery stops the job
   through the kill path, not by the default disposition;
4. a guard that dies mid-job leaves a durable in-flight marker; the next
   guard refuses (exit 4) until the workload is proven gone, and ACS blocks
   the host on its own evidence;
5. ``docker update`` only with ``--container-limits``;
6. A20: every guard ``docker exec`` runs from ``/``;
7. R2: no abbreviated options (``--profile mac`` is refused).

Same sandbox as ``test_internal_guard.py``: an isolated guard home,
fake host memory, temporary records; containers are disposable and
labelled.  The real lock, queue, state and shared containers are never
touched.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from conftest import FREE_MEM_ENV, Env, container_kill
from test_internal_guard import (ENGINE, GB, MAC, PY, Sandbox,  # noqa: F401
                                 container_profile, guard_container,
                                 needs_docker, reader, wait_for)

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture
def sandbox(tmp_path):
    return Sandbox(tmp_path)


def marker_path(sandbox) -> Path:
    return sandbox.home / "inflight.json"


def tagged_pids(tag: str) -> list[int]:
    """Host processes whose command line carries ``tag`` (never this
    process or a ps)."""
    out = subprocess.run(["ps", "-A", "-o", "pid=,command="],
                         capture_output=True, text=True).stdout
    pids = []
    for line in out.splitlines():
        pid, _, command = line.strip().partition(" ")
        if tag in command and pid.isdigit() and int(pid) != os.getpid() \
                and "ps -A" not in command:
            pids.append(int(pid))
    return pids


def alive(pid: int) -> bool:
    out = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)],
                         capture_output=True, text=True).stdout.strip()
    return bool(out) and not out.startswith("Z")


def kill_quietly(pids) -> None:
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass


def in_container_alive(name: str, tag: str) -> list[int]:
    out = subprocess.run(
        ["docker", "exec", "-w", "/", name, "python3", "-I", "-S", "-c",
         "import os,sys\n"
         "for p in os.listdir('/proc'):\n"
         "  if p.isdigit():\n"
         "    try: c=open('/proc/%s/cmdline'%p).read()\n"
         "    except OSError: continue\n"
         "    try: s=open('/proc/%s/stat'%p).read().rsplit(')',1)[1].split()[0]\n"
         "    except OSError: continue\n"
         "    if sys.argv[1] in c and 'import os,sys' not in c and s not in 'ZX': print(p)",
         tag], capture_output=True, text=True, timeout=60)
    return [int(x) for x in out.stdout.split()]


def run_detached_io(sandbox, mode, case, command, *extra, timeout=60.0,
                    outputs=None):
    """``sandbox.run`` with stdout/stderr in files: a process that outlives
    the guard (the point of these tests) keeps a pipe open, and
    ``capture_output`` would wait for it."""
    out = sandbox.tmp / f"{case}-{time.time_ns()}.out"
    err = out.with_suffix(".err")
    with open(out, "w") as o, open(err, "w") as e:
        proc = subprocess.run(
            [*sandbox.args(mode, case, *extra, outputs=outputs), "--",
             *command], env=sandbox.env, stdout=o, stderr=e,
            stdin=subprocess.DEVNULL, timeout=timeout)
    return subprocess.CompletedProcess(proc.args, proc.returncode,
                                       out.read_text(), err.read_text())


def script(tmp_path: Path, name: str, text: str) -> list[str]:
    path = tmp_path / name
    path.write_text(textwrap.dedent(text))
    return [PY, str(path)]


def container_args(sandbox, container, *extra):
    return ("--venue", "container", "--container", container,
            "--container-workdir", "/work", *extra)


# --- 1. container group kill under dash ------------------------------------------------

PARENT_WITH_CHILD = (
    "import subprocess, time; "
    "subprocess.Popen(['python3', '-c', 'import time; "
    "time.sleep(300)  # {tag}-child']); time.sleep(300)  # {tag}-parent")


@needs_docker
def test_container_cancel_kills_the_whole_in_container_group_under_dash(
        sandbox, guard_container):
    """Finding 1 (§6): `kill -TERM -- -PGID` is rejected by dash, so the
    leader died and its children survived a guard cancel.  Now the group is
    signalled with os.killpg, and verified empty."""
    sh = subprocess.run(["docker", "exec", guard_container, "readlink", "-f",
                         "/bin/sh"], capture_output=True, text=True).stdout
    assert sh.strip().endswith("dash"), sh     # the defect's precondition
    sandbox.set_row("dash", 0.05)
    tag = "dash-kill"
    proc = subprocess.Popen(
        [*sandbox.args("run", "dash", *container_args(sandbox, guard_container),
                       profile=container_profile(sandbox)),
         "--", "python3", "-c", PARENT_WITH_CHILD.format(tag=tag)],
        env=sandbox.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True)
    record = reader(sandbox.outputs / "dash" / "guard.json")
    wait_for(lambda: record().get("container_pid"), 60, "container pid")
    wait_for(lambda: in_container_alive(guard_container, f"{tag}-child"), 30,
             "in-container child")
    proc.send_signal(signal.SIGTERM)
    _, err = proc.communicate(timeout=90)
    assert proc.returncode == 5, err
    assert not in_container_alive(guard_container, f"{tag}-parent")
    assert not in_container_alive(guard_container, f"{tag}-child"), \
        "the in-container child survived the guard's cancel (dash gap)"
    final = record()
    assert final["outcome"] == "killed-interrupted", final
    assert final["cleanup"]["verified"] is True, final["cleanup"]
    assert final["cleanup"]["clean"] is True, final["cleanup"]
    assert final["cleanup"]["survivors"] == []
    assert not marker_path(sandbox).exists()


# --- 2. host venue: group members after a kill, and after a normal end -------------------

def test_kill_also_stops_group_members_that_outlive_the_leader(sandbox,
                                                              tmp_path):
    """guard.py stopped waiting once the LEADER exited: a group member
    that ignores SIGTERM was never sent SIGKILL.  Now the group must be
    empty before the kill path ends, and the record says so."""
    pidfile = tmp_path / "child.pid"
    ready = tmp_path / "child.ready"
    stubborn = script(tmp_path, "stubborn.py", f"""
        import signal, time
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        open({str(ready)!r}, "w").write("ready")
        time.sleep(120)
    """)
    leader = script(tmp_path, "leader.py", f"""
        import os, subprocess, time
        child = subprocess.Popen({stubborn!r})
        while not os.path.exists({str(ready)!r}):
            time.sleep(0.05)
        open({str(pidfile)!r}, "w").write(str(child.pid))
        time.sleep(120)
    """)
    sandbox.set_row("outlive", 0.01)
    child = None
    try:
        result = run_detached_io(sandbox, "run", "outlive", leader,
                                 "--timeout-min", "0.05", "--grace-s", "1")
        child = int(pidfile.read_text())
        assert result.returncode == 5, result.stderr
        final = sandbox.record("outlive")
        assert final["outcome"] == "killed-walltime", final
        assert not alive(child), "a group member outlived the guard's kill"
        assert final["cleanup"]["clean"] is True, final["cleanup"]
        assert final["cleanup"]["survivors"] == []
        assert not marker_path(sandbox).exists()
    finally:
        if child:
            kill_quietly([child])


def test_members_left_after_the_leader_ends_keep_the_host_taken(sandbox,
                                                                tmp_path):
    """A leader that exits 0 while a member of its group keeps running is
    not a clean end: the record names the survivor, the outcome is
    `cleanup-unverified` (exit 5), the in-flight marker stays, and the next
    guard is refused (exit 4) until the survivor is gone."""
    pidfile = tmp_path / "bg.pid"
    code = (f"import subprocess, sys; p = subprocess.Popen([sys.executable, "
            f"'-c', 'import time; time.sleep(120)  # bg-survivor']); "
            f"open({str(pidfile)!r}, 'w').write(str(p.pid))")
    sandbox.set_row("bg", 0.01)
    survivor = None
    try:
        result = run_detached_io(sandbox, "run", "bg", [PY, "-c", code])
        survivor = int(pidfile.read_text())
        assert result.returncode == 5, result.stderr
        final = sandbox.record("bg")
        assert final["outcome"] == "cleanup-unverified", final
        assert final["cleanup"]["clean"] is False
        assert final["cleanup"]["outcome"] == "completed"
        assert survivor in final["cleanup"]["survivors"], final["cleanup"]
        assert alive(survivor)                 # recorded, not killed
        assert "guard:cleanup-unverified" in sandbox.runlog_text()
        marker = json.loads(marker_path(sandbox).read_text())
        assert marker["job_id"] == final["job_id"]
        assert marker["phase"] == "unclean"

        blocked = sandbox.run("run", "bg", [PY, "-c", "pass"],
                              outputs=str(sandbox.outputs / "next"))
        assert blocked.returncode == 4, blocked.stderr
        assert "in-flight workload" in blocked.stderr
        assert not (sandbox.outputs / "next" / "samples.csv").exists()

        kill_quietly([survivor])
        wait_for(lambda: not alive(survivor), 10, "survivor gone")
        after = sandbox.run("run", "bg", [PY, "-c", "pass"],
                            outputs=str(sandbox.outputs / "after"))
        assert after.returncode == 0, after.stderr
        record = json.loads((sandbox.outputs / "after" /
                             "guard.json").read_text())
        assert [a for a in record["actions"]
                if a["action"] == "stale-inflight"], record["actions"]
        assert record["cleanup"]["clean"] is True
        assert not marker_path(sandbox).exists()
    finally:
        if survivor:
            kill_quietly([survivor])


def test_limitation_a_setsid_escape_is_not_tracked_by_the_guard(sandbox,
                                                                tmp_path):
    """Documented limit: tracking is cooperative (process group, and on
    Linux the session), not containment.  A descendant that starts its own
    session leaves both and is not seen.  Pinned so the docs stay
    honest."""
    pidfile = tmp_path / "escaped.pid"
    code = (f"import subprocess, sys; p = subprocess.Popen([sys.executable, "
            f"'-c', 'import time; time.sleep(60)'], start_new_session=True); "
            f"open({str(pidfile)!r}, 'w').write(str(p.pid))")
    sandbox.set_row("escape", 0.01)
    escaped = None
    try:
        result = run_detached_io(sandbox, "run", "escape", [PY, "-c", code])
        escaped = int(pidfile.read_text())
        assert result.returncode == 0, result.stderr
        final = sandbox.record("escape")
        assert final["outcome"] == "completed"
        assert alive(escaped)                            # running, and
        assert final["cleanup"]["clean"] is True        # not detected
    finally:
        if escaped:
            kill_quietly([escaped])


# --- 3. a signal between Popen and the monitor ---------------------------------------------

SEAM = textwrap.dedent("""
    # Test seam (S3): in the guard process only, deliver SIGTERM to itself
    # the moment the workload's Popen returns -- the window between the
    # launch and guard.py's handlers, deterministically.
    import os, signal, subprocess
    if os.environ.pop("ACS_TEST_SIGTERM_AFTER_LAUNCH", "") == "1":
        _init = subprocess.Popen.__init__
        _fired = []

        def _patched(self, args, *a, **kw):
            _init(self, args, *a, **kw)
            argv = [str(x) for x in args] if isinstance(args, (list, tuple)) \\
                else [str(args)]
            launch = kw.get("start_new_session") or (
                argv[:2] == ["docker", "exec"] and "setsid" in argv)
            if launch and not _fired:
                _fired.append(1)
                os.kill(os.getpid(), signal.SIGTERM)

        subprocess.Popen.__init__ = _patched
""")


def seam_env(sandbox) -> dict:
    seam = sandbox.tmp / "seam"
    seam.mkdir(exist_ok=True)
    (seam / "sitecustomize.py").write_text(SEAM)
    return {**sandbox.env, "PYTHONPATH": str(seam),
            "ACS_TEST_SIGTERM_AFTER_LAUNCH": "1"}


def run_seamed(sandbox, argv, command, timeout=60.0):
    out = sandbox.tmp / f"seamed-{time.time_ns()}.err"
    with open(out, "w") as err:
        proc = subprocess.run([*argv, "--", *command], env=seam_env(sandbox),
                              stdout=subprocess.DEVNULL, stderr=err,
                              stdin=subprocess.DEVNULL, timeout=timeout)
    return subprocess.CompletedProcess(proc.args, proc.returncode, "",
                                       out.read_text())


def test_sigterm_right_after_the_host_launch_stops_the_job(sandbox):
    sandbox.set_row("window", 0.01)
    tag = f"window-host-{os.getpid()}-{time.time_ns()}"
    try:
        result = run_seamed(sandbox, sandbox.args("run", "window"),
                            [PY, "-c", f"import time; time.sleep(60)  # {tag}"])
        assert result.returncode == 5, (result.returncode, result.stderr)
        final = sandbox.record("window")
        assert final["outcome"] == "killed-interrupted", final
        assert final["pid"] is not None           # it had been launched
        assert final["cleanup"]["clean"] is True, final["cleanup"]
        wait_for(lambda: not tagged_pids(tag), 10, "the workload gone")
        assert not marker_path(sandbox).exists()
    finally:
        kill_quietly(tagged_pids(tag))


@needs_docker
def test_sigterm_during_the_container_pid_discovery_stops_the_job(
        sandbox, guard_container):
    sandbox.set_row("window-c", 0.05)
    tag = "window-container"
    result = run_seamed(
        sandbox, sandbox.args("run", "window-c",
                              *container_args(sandbox, guard_container),
                              profile=container_profile(sandbox)),
        ["python3", "-c", f"import time; time.sleep(300)  # {tag}"],
        timeout=120)
    assert result.returncode == 5, (result.returncode, result.stderr)
    assert not in_container_alive(guard_container, tag)
    final = sandbox.record("window-c")
    assert final["outcome"] == "killed-interrupted", final
    assert final["cleanup"]["clean"] is True, final["cleanup"]
    assert not marker_path(sandbox).exists()


# --- 4. guard death: the in-flight marker, standalone and under ACS -----------------------

def test_a_killed_guard_leaves_a_marker_that_holds_the_host(sandbox):
    """The kernel frees a SIGKILLed guard's flock, not its workload.  The
    marker keeps the host's one heavy slot taken until the workload is
    proven gone; then the next guard clears it and runs."""
    sandbox.set_row("dead", 0.01)
    tag = f"dead-guard-{os.getpid()}-{time.time_ns()}"
    guard = subprocess.Popen(
        [*sandbox.args("run", "dead"), "--", PY, "-c",
         f"import time; time.sleep(120)  # {tag}"],
        env=sandbox.env, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL)
    record = reader(sandbox.outputs / "dead" / "guard.json")
    try:
        wait_for(lambda: record().get("pid"), 20, "job start")
        workload = record()["pid"]
        marker = reader(marker_path(sandbox))
        wait_for(lambda: marker().get("pid") == workload, 10, "the marker")
        wait_for(lambda: marker().get("start"), 10, "the leader start")
        guard.kill()
        guard.wait(timeout=30)
        assert alive(workload)              # the orphan

        refused = sandbox.run("run", "dead", [PY, "-c", "pass"],
                              outputs=str(sandbox.outputs / "second"))
        assert refused.returncode == 4, refused.stderr
        assert "in-flight workload" in refused.stderr
        assert "is alive" in refused.stderr
        # G2b (D3): no runlog line; the attempt's record says why
        assert "guard:lock-busy" not in sandbox.runlog_text()
        second = json.loads((sandbox.outputs / "second" / "guard.json")
                            .read_text())
        assert second["outcome"] == "lock-busy"
        assert "in-flight workload" in second["refusal"]
        # --wait keeps waiting on it, then gives up with the same exit
        started = time.time()
        waited = sandbox.run("run", "dead", [PY, "-c", "pass"], "--wait", "2",
                             outputs=str(sandbox.outputs / "third"))
        assert waited.returncode == 4 and time.time() - started >= 2
        status = json.loads(subprocess.run(
            [PY, str(ENGINE), "status"], env=sandbox.env,
            capture_output=True, text=True).stdout)
        assert status["inflight"]["pid"] == workload

        os.killpg(workload, signal.SIGKILL)
        wait_for(lambda: not alive(workload), 10, "workload gone")
        after = sandbox.run("run", "dead", [PY, "-c", "pass"],
                            outputs=str(sandbox.outputs / "after"))
        assert after.returncode == 0, after.stderr
        actions = json.loads((sandbox.outputs / "after" /
                              "guard.json").read_text())["actions"]
        assert [a for a in actions if a["action"] == "stale-inflight"]
        assert not marker_path(sandbox).exists()
    finally:
        if guard.poll() is None:
            guard.kill()
        kill_quietly(tagged_pids(tag))


@needs_docker
def test_a_killed_guard_leaves_a_marker_for_its_container_workload(
        sandbox, guard_container):
    sandbox.set_row("dead-c", 0.05)
    tag = "dead-container-guard"
    guard = subprocess.Popen(
        [*sandbox.args("run", "dead-c",
                       *container_args(sandbox, guard_container),
                       profile=container_profile(sandbox)),
         "--", "python3", "-c", f"import time; time.sleep(300)  # {tag}"],
        env=sandbox.env, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL)
    record = reader(sandbox.outputs / "dead-c" / "guard.json")
    try:
        wait_for(lambda: record().get("container_pid"), 60, "container pid")
        marker = reader(marker_path(sandbox))
        wait_for(lambda: marker().get("container_pid"), 20, "the marker")
        guard.kill()
        guard.wait(timeout=30)
        assert in_container_alive(guard_container, tag)
        sandbox.set_row("next", 0.01)
        refused = sandbox.run("run", "next", [PY, "-c", "pass"])
        assert refused.returncode == 4, refused.stderr
        assert "in-container processes remain" in refused.stderr
        container_kill(guard_container,
                       in_container_alive(guard_container, tag))
        wait_for(lambda: not in_container_alive(guard_container, tag), 30,
                 "in-container workload gone")
        after = sandbox.run("run", "next", [PY, "-c", "pass"])
        assert after.returncode == 0, after.stderr
        assert not marker_path(sandbox).exists()
    finally:
        if guard.poll() is None:
            guard.kill()


@pytest.fixture
def acs_internal(tmp_path):
    """ACS (direct mode, isolated state/lock) with the acs-guard backend:
    the runner executes the internal engine of this tree with a per-job
    profile file and the job's ACS claim (S4; in S3 a test-only wrapper
    added the profile file)."""
    env = Env(queue=False)
    yield env
    env.cleanup()


def test_guard_death_under_acs_blocks_and_the_marker_holds_the_host(
        acs_internal):
    """Requirement 4(a): SIGKILL the internal guard mid-job under the ACS
    runner.  ACS blocks the host and refuses the next job until reconcile
    on evidence; a direct caller of the internal guard is refused by the
    marker meanwhile; after the orphan is terminated the next ACS job runs
    and its guard clears the marker."""
    env = acs_internal
    out = env.root / "w.jsonl"
    job = env.submit(workload_args=["--out", str(out), "--sleep", "120"])
    state = env.wait_phase(job["job_id"], "executing")
    workload_pid = state["workload"]["pid"]
    marker = env.guard_home / "inflight.json"
    env.wait_for(lambda: marker.is_file() and json.loads(
        marker.read_text()).get("pid") == workload_pid, what="the marker")
    os.kill(state["guard"]["pid"], signal.SIGKILL)
    final = env.wait_outcome(job["job_id"])
    assert final["outcome"] == "unknown", final
    assert final["cleanup"]["clean"] is False
    assert env.lock_free()                      # the flock alone says "free"
    block = json.loads(env.acs("status", "--json").stdout)["admission"][
        "blocked"]
    assert job["job_id"] in block["jobs"]
    os.kill(workload_pid, 0)                    # the orphan runs

    blocked = env.submit("beta", workload_args=["--out",
                                                str(env.root / "n")])
    assert env.wait_outcome(blocked["job_id"])["outcome"] == "refused"
    direct = subprocess.run(
        [PY, str(ENGINE), "run", "--case", "direct", "--profile-file",
         str(MAC), "--reserve-gb", "0.5", "--cwd", str(env.root),
         "--outputs", str(env.root / "direct"),
         "--runlog", str(env.root / "direct-runlog.md"),
         "--baseline", str(env.root / "direct-baseline.json"),
         "--", PY, "-c", "pass"],
        env=env.env(**{"SMC_GUARD_HOME": str(env.guard_home),
                       FREE_MEM_ENV: str(env.free),
                       "ACS_GUARD_TEST_UNCLAIMED": "1"}),
        capture_output=True, text=True, timeout=60)
    assert direct.returncode == 4, direct.stderr
    assert "in-flight workload" in direct.stderr
    assert not (env.root / "direct" / "samples.csv").exists()
    status = json.loads(env.acs("status", "--json").stdout)
    assert status["admission"]["inflight_marker"]["acs_job"] == job["job_id"]

    rec = json.loads(env.acs("reconcile", "--terminate", job["job_id"],
                             "--json").stdout)
    assert rec["blocked"] is False, rec
    with pytest.raises(ProcessLookupError):
        os.kill(workload_pid, 0)
    # S4 (R3): reconcile resolved the marker on the job's evidence and
    # removed only that file, with a durable history entry
    assert rec["inflight_marker"]["resolved"] is True, rec
    assert not marker.exists()
    assert env.lock_free() and (env.guard_home / "lock").exists()
    history = env.state_of(job["job_id"])["history"]
    assert [h for h in history if h.get("event") == "inflight-marker-cleared"]
    ok = env.submit("beta", workload_args=["--out", str(env.root / "ok")])
    done = env.wait_outcome(ok["job_id"])
    assert done["outcome"] == "succeeded", done
    assert not marker.exists()


@needs_docker
def test_acs_cancel_of_an_internal_guard_container_job_kills_its_children(
        acs_internal, guard_container):
    """The internal-guard analogue of test_children_docker.py::
    test_guard_group_kill_gap_in_dash_containers_is_caught: through ACS, a
    cancel now ends the in-container children too, and ACS finds the end
    clean (no block)."""
    env = acs_internal
    env.set_container(guard_container)
    tag = "acs-dash"
    result = env.acs("submit", "--json", "--case", "c", "--timeout-min", "2",
                     "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]),
                     "--container", guard_container, "--", "python3", "-c",
                     PARENT_WITH_CHILD.format(tag=tag))
    job = json.loads(result.stdout)
    env.wait_phase(job["job_id"], "executing", timeout=120)
    env.wait_for(lambda: in_container_alive(guard_container, f"{tag}-child"),
                 what="in-container child")
    env.acs("cancel", job["job_id"], "--json")
    final = env.wait_outcome(job["job_id"], timeout=120)
    assert final["outcome"] == "cancelled", final
    assert final["cleanup"]["clean"] is True, final["cleanup"]
    assert not in_container_alive(guard_container, f"{tag}-parent")
    assert not in_container_alive(guard_container, f"{tag}-child")
    status = json.loads(env.acs("status", "--json").stdout)
    assert status["admission"]["blocked"] is None
    assert not (env.guard_home / "inflight.json").exists()


# --- 5. docker update only when opted in ------------------------------------------------

def container_limits(name: str) -> tuple[int, int]:
    out = subprocess.run(["docker", "inspect", "--format",
                          "{{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}",
                          name], capture_output=True, text=True,
                         check=True).stdout.split()
    return int(out[0]), int(out[1])


@needs_docker
def test_container_limits_change_only_with_container_limits(sandbox,
                                                            guard_container):
    before = container_limits(guard_container)
    assert before[0] == 1 * GB
    sandbox.set_row("limits", 1.0)       # cap = ceil(1.5 x 1.0) = 2 GB
    plain = sandbox.run("run", "limits", ["python3", "-c", "pass"],
                        *container_args(sandbox, guard_container),
                        timeout=120, profile=container_profile(sandbox))
    assert plain.returncode == 0, plain.stderr
    assert container_limits(guard_container) == before, \
        "the guard changed a container's limits without --container-limits"
    record = sandbox.record("limits")
    assert record["docker"]["memory_gb"] == 2.0     # the sampler's ceiling
    assert record["docker"]["limits_applied"] is False

    opted = sandbox.run("run", "limits", ["python3", "-c", "pass"],
                        *container_args(sandbox, guard_container,
                                        "--container-limits"),
                        timeout=120, profile=container_profile(sandbox),
                        outputs=str(sandbox.outputs / "opted"))
    assert opted.returncode == 0, opted.stderr
    record = json.loads((sandbox.outputs / "opted" / "guard.json")
                        .read_text())
    assert record["docker"]["limits_applied"] is True
    assert container_limits(guard_container)[0] == 2 * GB


# --- 5/6. every docker call, against a fake docker (no Docker needed) ----------------------

FAKE_DOCKER = """#!{python}
import json, sys, time
argv = sys.argv[1:]
with open({log!r}, "a") as handle:
    handle.write(json.dumps(argv) + "\\n")
if argv[:1] == ["info"]:
    print("8589934592 4 linux")
elif argv[:1] == ["inspect"]:
    print("fakeid" if "{{{{.Id}}}}" in argv else "true")
elif argv[:1] == ["exec"]:
    if "setsid" in argv:
        time.sleep(1.5)
    elif "python3" in argv:
        print(json.dumps({{"leader_ticks": "7", "stranger": False,
                          "members": [], "signalled": [], "errors": []}}))
    elif "cat" in argv and argv[-1].endswith(".pid"):
        print("4242")
    elif "cat" in argv:
        print("1000")
"""


@pytest.mark.parametrize("opt_in", [False, True], ids=["default", "opt-in"])
def test_every_docker_exec_runs_from_root_and_update_is_opt_in(sandbox,
                                                              opt_in):
    log = sandbox.tmp / "docker.log"
    fake = sandbox.tmp / "fakebin" / "docker"
    fake.parent.mkdir()
    fake.write_text(FAKE_DOCKER.format(python=PY, log=str(log)))
    fake.chmod(0o755)
    sandbox.set_row("fake", 0.05)
    env = {**sandbox.env,
           "PATH": f"{fake.parent}{os.pathsep}{os.environ['PATH']}"}
    extra = ("--container-limits",) if opt_in else ()
    result = subprocess.run(
        [*sandbox.args("run", "fake",
                       *container_args(sandbox, "fake-c", *extra)),
         "--", "python3", "-c", "pass"],
        env=env, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    execs = [c for c in calls if c[0] == "exec"]
    for call in execs:
        assert call[1:3] == ["-w", "/"], call        # A20
    kinds = {("setsid" in c and "workload") or ("python3" in c and "helper")
             or c[-1] for c in execs}
    # the launch, the pid read, a memory read, the helper, the pid-file rm
    assert "workload" in kinds and "helper" in kinds, execs
    for call in execs:
        if "python3" in call:
            assert call[call.index("python3") + 1:][:2] == ["-I", "-S"], call
    updates = [c for c in calls if c[0] == "update"]
    assert bool(updates) == opt_in, calls
    record = sandbox.record("fake")
    assert record["docker"]["limits_applied"] is opt_in
    assert record["cleanup"]["clean"] is True


# --- 7. R2: no abbreviated options ------------------------------------------------------

def test_abbreviated_and_legacy_options_are_refused(sandbox):
    """`--profile mac` (guard.py's option) used to be read as
    `--profile-file mac` by argparse's prefix matching."""
    sandbox.set_row("abbrev", 0.01)
    base = [PY, str(ENGINE), "run", "--case", "abbrev", "--cwd",
            str(sandbox.tmp), "--baseline", str(sandbox.baseline),
            "--runlog", str(sandbox.runlog),
            "--outputs", str(sandbox.outputs / "abbrev")]
    legacy = subprocess.run([*base, "--profile", "mac", "--", PY, "-c",
                             "pass"], env=sandbox.env, capture_output=True,
                            text=True, timeout=30)
    assert legacy.returncode == 2, legacy.stderr
    assert "unrecognized arguments: --profile" in legacy.stderr
    short = subprocess.run([*base, "--profile-file", str(MAC), "--time", "1",
                            "--", PY, "-c", "pass"], env=sandbox.env,
                           capture_output=True, text=True, timeout=30)
    assert short.returncode == 2, short.stderr
    assert "unrecognized arguments: --time" in short.stderr
    status = subprocess.run([PY, str(ENGINE), "status", "--profile", "x"],
                            env=sandbox.env, capture_output=True, text=True,
                            timeout=30)
    assert status.returncode == 2, status.stderr
    assert not sandbox.outputs.exists()


# --- the recycled-leader rule needs no pid-reuse assumption ----------------------------------

def test_a_recycled_leader_pid_never_hides_other_group_members(monkeypatch):
    """A live process at the recorded leader's pid with another start is
    set aside, but every other member of that group still counts: the
    check never relies on the kernel refusing to reuse a pid whose group
    exists (verified on Linux, not on macOS)."""
    import types
    from agent_compute.guard import engine
    marker = {"schema": engine.INFLIGHT_SCHEMA, "job_id": "c/1",
              "venue": "host", "pid": 100, "start": "lstart:A"}
    monkeypatch.setattr(engine, "process_start", lambda pid: "lstart:B")
    monkeypatch.setattr(engine, "host_group_members", lambda pid: [100, 101])
    assert engine.inflight_gone(marker)[0] is False
    monkeypatch.setattr(engine, "host_group_members", lambda pid: [100])
    assert engine.inflight_gone(marker)[0] is True
    monkeypatch.setattr(engine, "process_start", lambda pid: "lstart:A")
    assert engine.inflight_gone(marker) == (False,
                                            "the workload leader 100 is alive")
    monkeypatch.setattr(engine, "host_group_members", lambda pid: None)
    assert engine.inflight_gone(marker)[0] is False

    job = types.SimpleNamespace(venue="host", proc=types.SimpleNamespace(pid=100))
    job._cleanup_evidence = types.MethodType(engine.Job._cleanup_evidence, job)
    job._live_members = lambda: [100, 101]
    evidence = engine.Job._verify_end(job)
    assert evidence["clean"] is False and evidence["survivors"] == [101]
    job._live_members = lambda: [100]
    assert engine.Job._verify_end(job)["clean"] is True
