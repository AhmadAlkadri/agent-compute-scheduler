"""Regressions from the independent review of slice 3 (``4dd3d77``).

B1: an env-scrubbed descendant that STAYS in the job's (orphaned) session
    was called clean on the natural fast path (``container_pid`` null):
    nothing carried the marker, and no marker holder's session was left
    to name it.  Reviewer repros: /tmp/acs-review-1/expb2.py and
    /tmp/acs-review-2/test_review_scrub_in_session.py.
B2: a process whose main thread called ``pthread_exit`` shows ``Z`` in
    ``/proc/<tgid>/stat`` while its other threads run; it was treated as
    gone.  Reviewer repro: /tmp/acs-review-1/exp.py.
S2: "No such container" was trusted without checking which docker daemon
    answered.

Disposable ``acs-test-*`` containers only (``make_container`` removes them
in ``finally``).
"""

from __future__ import annotations

import json
import secrets
import subprocess
import time

import pytest

from agent_compute import procs
from conftest import container_kill, container_kill_matching
from test_children_docker import needs_docker
from test_cleanup_evidence import (  # noqa: F401 (fixtures)
    REAL_DOCKER, blocked, fake_docker_path, guard_record, make_container,
    report, scan, submit_container)
from test_container_job_evidence import (INIT, JOB, craft_job, judge, proc,
                                         utc)


def guard_style_launch(name: str, job: str, cmd: str) -> subprocess.Popen:
    """The guard's container launch shape, with the marker."""
    script = f"echo $$ > /tmp/{secrets.token_hex(3)}.pid; cd /tmp && " \
             f"exec nice -n 5 {cmd}"
    return subprocess.Popen([REAL_DOCKER, "exec", "-e", f"SMC_GUARD_JOB={job}",
                             name, "setsid", "-w", "bash", "-c", script])


def kill_by_tag(name: str, tag: str) -> None:
    container_kill(name, [row["pid"] for row in scan(name, tag)],
                   docker=REAL_DOCKER)


# -- B1: rule ------------------------------------------------------------------------

def test_b1_rule_orphaned_session_after_the_job_is_unverifiable(monkeypatch):
    # sid/pgrp 30 names no live process (the gone leader), no marker
    ev = judge(monkeypatch, INIT, proc(31, sid=30, pgrp=30))
    assert ev["verified"] is False and ev["clean"] is False, ev
    assert [p["pid"] for p in ev["orphaned"]] == [31]


def test_b1_rule_orphaned_session_older_than_the_job_is_ignored(monkeypatch):
    ev = judge(monkeypatch, INIT, proc(31, sid=30, pgrp=30, at=-600))
    assert ev["verified"] is True and ev["clean"] is True, ev


def test_b1_rule_orphaned_group_in_live_session_is_unverifiable(monkeypatch):
    ev = judge(monkeypatch, INIT, proc(50), proc(31, sid=50, pgrp=30))
    assert ev["verified"] is False, ev


def test_b1_rule_live_session_leader_is_fine(monkeypatch):
    ev = judge(monkeypatch, INIT, proc(50), proc(51, sid=50, pgrp=50))
    assert ev["verified"] is True and ev["clean"] is True, ev


# -- B1: live --------------------------------------------------------------------------

@needs_docker
def test_b1_scrubbed_child_in_orphaned_session_is_not_clean(make_container):
    """expb2.py: guard launch shape, container_pid null, a child started
    with `env -i` that stays in the job's session."""
    name = make_container()
    cid = procs.container_id(name)
    tag = f"acs-b1-{secrets.token_hex(3)}"
    started = utc(time.time() - 1)
    client = guard_style_launch(
        name, JOB, f"bash -c \"env -i bash -c 'exec -a {tag} sleep 60' "
                   ">/dev/null 2>&1 </dev/null & exit 0\"")
    client.wait(timeout=30)
    try:
        rows = []
        for _ in range(40):
            rows = scan(name, tag)
            if rows:
                break
            time.sleep(0.25)
        assert rows and rows[0]["job"] is None, "precondition: scrubbed"
        record = {"job_id": JOB, "pid": client.pid, "container_pid": None,
                  "outcome": "completed", "started_utc": started}
        container = {"name": name, "id": cid}
        ev = procs.container_job_evidence(container, record)
        done = procs.signal_container_job(container, record, None, 15)
        still = scan(name, tag)
        report("b1-live", evidence=ev, signal=done, still=still)
        assert ev["clean"] is False and ev["verified"] is False, ev
        assert rows[0]["pid"] in [p["pid"] for p in ev["orphaned"]]
        assert done["signalled"] is False                  # never unmarked
        assert still, "an unmarked process must not be signalled"
    finally:
        kill_by_tag(name, tag)


@needs_docker
def test_b1_scrubbed_child_through_the_queue(env_queue, make_container):
    """The reviewer's queue probe: never a false clean, the successor never
    runs while the survivor lives, --terminate does not signal it, and the
    block resolves once it is gone."""
    env = env_queue
    name = make_container()
    env.set_container(name)
    tag = f"acs-b1q-{secrets.token_hex(3)}"
    script = (f"env -i bash -c 'exec -a {tag} sleep 45' >/dev/null 2>&1 "
              "</dev/null & exit 0")
    job = submit_container(env, name, ["bash", "-c", script])
    marker = env.root / "successor.jsonl"
    second = env.submit(workload_args=["--out", str(marker)])
    state = env.wait_outcome(job["job_id"], timeout=60)
    assert scan(name, tag), "precondition: survivor alive"
    report("b1-queue", outcome=state["outcome"], cleanup=state["cleanup"],
           container_pid=guard_record(env, job["job_id"]).get(
               "container_pid"))
    assert state["cleanup"]["clean"] is False, state["cleanup"]
    assert blocked(env)
    rec = json.loads(env.acs("reconcile", "--terminate", job["job_id"],
                             "--json").stdout)
    assert rec["blocked"] is True, rec
    assert "by hand" in rec["unresolved"][0]["blocker"], rec
    assert scan(name, tag), "--terminate must not signal unmarked processes"
    while scan(name, tag):
        assert not marker.exists(), "successor ran while survivor lived"
        time.sleep(1)
    rec = json.loads(env.acs("reconcile", "--json").stdout)
    assert rec["blocked"] is False, rec
    assert not blocked(env)
    # A22: one reconcile appends "reconciled" and, when the internal guard
    # kept its in-flight marker, also "inflight-marker-cleared"; their order
    # is not part of the contract.  Require "reconciled" after the entry that
    # recorded the blocking outcome, in either order with the marker entry.
    history = env.state_of(job["job_id"])["history"]
    block_at = max(i for i, h in enumerate(history)
                   if h.get("outcome") == state["outcome"]
                   and "event" not in h)
    after = [h.get("event") for h in history[block_at + 1:]]
    assert "reconciled" in after, history
    assert second["job_id"]


# -- B2: main thread pthread_exit, worker threads still running -----------------------

ZOMBIE_LEADER = ("import ctypes,threading,time\n"
                 "def w():\n    t=time.time()\n"
                 "    while time.time()-t<120: time.sleep(0.05)\n"
                 "threading.Thread(target=w).start()\n"
                 "ctypes.CDLL(None).pthread_exit(None)\n")


_LIVE_THREADS = r"""
import os, sys
n = 0
for pid in os.listdir('/proc'):
    if not pid.isdigit() or int(pid) == os.getpid():
        continue
    try:
        if sys.argv[1] not in open('/proc/%s/cmdline' % pid).read():
            continue
        for tid in os.listdir('/proc/%s/task' % pid):
            st = open('/proc/%s/task/%s/stat' % (pid, tid)).read()
            if st.rsplit(')', 1)[1].split()[0] not in ('Z', 'X'):
                n += 1
    except OSError:
        continue
print(n)
"""


def live_threads(name: str, needle: str) -> int:
    """Non-zombie THREADS of processes whose cmdline has ``needle`` (the
    test's own scan() skips a Z tgid, which is exactly the bug)."""
    out = subprocess.run([REAL_DOCKER, "exec", name, "python3", "-c",
                          _LIVE_THREADS, needle], capture_output=True,
                         text=True)
    return int(out.stdout.strip() or 0)


def zombie_leader_container(make_container) -> tuple[str, str]:
    name = make_container()
    subprocess.run([REAL_DOCKER, "exec", name, "sh", "-c",
                    f"cat > /tmp/z.py <<'X'\n{ZOMBIE_LEADER}X"], check=True)
    return name, procs.container_id(name)


@needs_docker
def test_b2_zombie_main_thread_alone_is_not_gone(make_container):
    """The Z-leader process is the job's only process (no setsid parent
    waiting on it): its live threads must still count."""
    name, cid = zombie_leader_container(make_container)
    started = utc(time.time() - 1)
    subprocess.run([REAL_DOCKER, "exec", "-d", "-e", f"SMC_GUARD_JOB={JOB}",
                    name, "python3", "/tmp/z.py"], check=True)
    for _ in range(40):
        if live_threads(name, "z.py"):
            break
        time.sleep(0.25)
    time.sleep(1)
    container = {"name": name, "id": cid}
    record = {"job_id": JOB, "pid": 4242, "container_pid": None,
              "started_utc": started}
    ev = procs.container_job_evidence(container, record)
    done = procs.signal_container_job(container, record, None, 9)
    time.sleep(1)
    left = live_threads(name, "z.py")
    report("b2-alone", evidence=ev, signal=done, left=left)
    assert ev["clean"] is False and ev["members"], ev
    assert done["signalled"] is True, done
    assert left == 0


@needs_docker
def test_b2_terminate_kills_zombie_leader_before_resolving(env_direct,
                                                           make_container):
    """exp.py: after `reconcile --terminate` the setsid parent is gone; the
    Z-leader process must not then read as gone while its threads run."""
    env = env_direct
    name, cid = zombie_leader_container(make_container)
    started = utc(time.time() - 1)
    client = guard_style_launch(name, JOB, "python3 /tmp/z.py")
    try:
        for _ in range(40):
            if live_threads(name, "z.py"):
                break
            time.sleep(0.25)
        time.sleep(1)
        container = {"name": name, "id": cid, "workdir": "/work"}
        record = {"job_id": JOB, "pid": client.pid, "container_pid": None,
                  "outcome": "completed", "exit_code": 0,
                  "started_utc": started}
        job_id = craft_job(env, container, record)
        first = json.loads(env.acs("reconcile", "--json").stdout)
        second = json.loads(env.acs("reconcile", "--terminate", job_id,
                                    "--json").stdout)
        left = live_threads(name, "z.py")
        report("b2", first=first, second=second, left=left)
        assert first["blocked"] is True, first
        assert left == 0 or second["blocked"] is True, \
            "block cleared while the Z-leader process still computes"
        assert left == 0, "terminate left the Z-leader process running"
        assert second["blocked"] is False, second
    finally:
        if client.poll() is None:
            client.kill()


# -- S2: the docker daemon that answers -------------------------------------------------

def test_s2_daemon_mismatch_is_unverifiable(monkeypatch):
    def fake(*args, **_k):
        if args[:2] == ("info", "--format"):
            return subprocess.CompletedProcess(args, 0, "OTHER-DAEMON\n", "")
        return subprocess.CompletedProcess(args, 1, "",
                                           "Error: No such container: x")
    monkeypatch.setattr(procs, "docker", fake)
    cont = {"name": "c", "id": "a" * 64, "daemon_id": "RECORDED-DAEMON"}
    ev = procs.container_job_evidence(cont, {"job_id": JOB, "pid": 1})
    assert ev["verified"] is False and "daemon" in ev["reason"], ev


def test_s2_daemon_unreadable_is_unverifiable(monkeypatch):
    def fake(*args, **_k):
        if args[0] == "info":
            return subprocess.CompletedProcess(args, 1, "", "cannot connect")
        return subprocess.CompletedProcess(args, 1, "",
                                           "Error: No such container: x")
    monkeypatch.setattr(procs, "docker", fake)
    cont = {"name": "c", "id": "a" * 64, "daemon_id": "RECORDED-DAEMON"}
    assert procs.container_job_evidence(
        cont, {"job_id": JOB, "pid": 1})["verified"] is False


def test_s2_daemon_match_trusts_gone_and_legacy_notes_it(monkeypatch):
    def fake(*args, **_k):
        if args[0] == "info":
            return subprocess.CompletedProcess(args, 0, "D1\n", "")
        return subprocess.CompletedProcess(args, 1, "",
                                           "Error: No such container: x")
    monkeypatch.setattr(procs, "docker", fake)
    record = {"job_id": JOB, "pid": 1}
    ev = procs.container_job_evidence({"name": "c", "id": "a" * 64,
                                       "daemon_id": "D1"}, record)
    assert ev["clean"] is True and ev["daemon_verified"] is True, ev
    ev = procs.container_job_evidence({"name": "c", "id": "a" * 64}, record)
    assert ev["clean"] is True and ev["daemon_verified"] is False, ev


@needs_docker
def test_s2_submission_records_daemon_and_queued_job_refuses_mismatch(
        env_direct, make_container):
    env = env_direct
    name = make_container()
    env.set_container(name)
    job = submit_container(env, name, ["bash", "-c", "exit 0"])
    env.wait_outcome(job["job_id"], timeout=60)
    manifest = json.loads((env.state / "jobs" / job["job_id"] /
                           "manifest.json").read_text())
    assert manifest["container"]["daemon_id"]
    from agent_compute.runner import Runner
    runner = Runner.__new__(Runner)
    runner.manifest = dict(manifest, container=dict(manifest["container"],
                                                   daemon_id="OTHER"))
    runner.manifest["inputs"] = manifest["inputs"]
    problem = runner.inputs_changed()
    assert problem and "daemon" in problem, problem


@needs_docker
def test_b2_linux_host_path_start_info_sees_live_threads(make_container):
    """The host venue on Linux (the Lenovo) uses procs.start_info: a Z/X
    tgid with a live thread is live; a true zombie is not.  Run with the
    repository's procs.py inside a disposable Linux container (the Mac
    host cannot exercise the Linux branch)."""
    name, _ = zombie_leader_container(make_container)
    # procs.py imports its sibling fsprobe.py (campaign PA K6): copy both
    # into a package in the container
    pkg = __import__("pathlib").Path(procs.__file__).parent
    subprocess.run([REAL_DOCKER, "exec", name, "mkdir", "-p",
                    "/tmp/under_test/agent_compute"], check=True)
    subprocess.run([REAL_DOCKER, "exec", name, "touch",
                    "/tmp/under_test/agent_compute/__init__.py"], check=True)
    for module in ("procs.py", "fsprobe.py"):
        subprocess.run([REAL_DOCKER, "cp", str(pkg / module),
                        f"{name}:/tmp/under_test/agent_compute/{module}"],
                       check=True)
    subprocess.run([REAL_DOCKER, "exec", "-d", name, "python3", "/tmp/z.py"],
                   check=True)
    subprocess.run([REAL_DOCKER, "exec", "-d", name, "python3", "-c",
                    "import os, time\npid = os.fork()\nif pid == 0:\n"
                    "    os._exit(0)\ntime.sleep(60)  # zombie-parent"],
                   check=True)
    time.sleep(2)
    probe = r"""
import json, os, sys
sys.path.insert(0, '/tmp/under_test')
import agent_compute.procs as p
out = {}
for pid in os.listdir('/proc'):
    if not pid.isdigit() or int(pid) == os.getpid():
        continue
    try:
        state = open('/proc/%s/stat' % pid).read().rsplit(')', 1)[1].split()[0]
        cmd = open('/proc/%s/cmdline' % pid).read()
    except OSError:
        continue
    if state == 'Z':
        kind = 'zleader' if len(os.listdir('/proc/%s/task' % pid)) > 1 \
            else 'zombie'
        out[kind] = p.start_info(int(pid)) is not None
print(json.dumps(out))
"""
    res = subprocess.run([REAL_DOCKER, "exec", name, "python3", "-c", probe],
                         capture_output=True, text=True)
    seen = json.loads(res.stdout or "{}")
    report("b2-linux-host", seen=seen, stderr=res.stderr[-300:])
    assert seen == {"zleader": True, "zombie": False}, seen


# -- D4 (re-review of 2fbaf93): the LEADER drops the marker ----------------------------
# /tmp/acs-review-1/expd.py, section D4: `exec env -i CMD` under the guard's
# `setsid -w`; container_pid never captured.  The only marker holder is the
# setsid parent; signalling it erased the only link to the unmarked leader,
# which leads its own live session, and the evidence then read clean.

def test_d4_rule_unmarked_child_of_a_holder_is_a_member(monkeypatch):
    holder = proc(20, env=procs.HAS_MARKER)
    leader = proc(21, ppid=20)                  # own session and group
    grandchild = proc(22, sid=22, pgrp=22, ppid=21)
    ev = judge(monkeypatch, INIT, holder, leader, grandchild)
    assert ev["clean"] is False
    assert {m["pid"] for m in ev["members"]} == {20, 21, 22}
    assert {m["pid"] for m in ev["unmarked_members"]} == {21, 22}


# -- C5 (G1): signalling is gated on the recorded docker daemon, like evidence ------
#
# Basis: the private development record (register item C5).
# The evidence refused a daemon other than the one recorded at submission,
# but the signal path resolved the recorded name in whichever daemon
# answered and signalled marker holders there (`reconcile --terminate`).

C5_REC, C5_OTHER = "a" * 64, "b" * 64
C5_RECORD = {"job_id": JOB, "pid": 4242, "container_pid": None,
             "started_utc": "2026-10-05T00:00:00Z"}


@pytest.fixture
def daemon_b(monkeypatch):
    """Daemon B answers; the recorded container is unknown there and the
    recorded name is bound to another running container whose scan has a
    marker holder.  Every in-container helper call is recorded."""
    state = {"daemon": "daemon-B", "helpers": [], "docker": []}

    def fake_docker(*args, **_k):
        state["docker"].append(args)
        out = subprocess.CompletedProcess(list(args), 0, "", "")
        if args[:1] == ("info",):
            if state["daemon"] is None:
                out.returncode, out.stderr = 1, "Cannot connect"
            else:
                out.stdout = state["daemon"] + "\n"
        elif args[:1] == ("inspect",):
            target = args[-1]
            if target == C5_REC:
                out.returncode = 1
                out.stderr = "Error: No such container: " + C5_REC
            elif target == "fenics":
                out.stdout = f"{C5_OTHER} true\n"
        return out

    def fake_python(cid, script, *args, **_k):
        state["helpers"].append((cid, script))
        if script == procs._JOB_SCAN:
            return {"now": 2000.0, "btime": 1000.0, "tck": 100,
                    "settled": True,
                    "procs": [{"pid": 7, "ppid": 1, "pgrp": 7, "sid": 7,
                               "ticks": 900, "env": procs.HAS_MARKER}]}, None
        if script == procs._JOB_SIGNAL:
            return {"groups": [7], "pids": [7], "skipped": []}, None
        return None, "unexpected helper"

    monkeypatch.setattr(procs, "docker", fake_docker)
    monkeypatch.setattr(procs, "container_python", fake_python)
    return state


@pytest.mark.parametrize("daemon", ["daemon-B", None],
                         ids=["other-daemon", "daemon-unreadable"])
@pytest.mark.parametrize("sig", [15, 9], ids=["TERM", "KILL"])
def test_c5_signal_refuses_before_any_exec_under_another_daemon(daemon_b,
                                                                daemon, sig):
    daemon_b["daemon"] = daemon
    container = {"id": C5_REC, "name": "fenics", "daemon_id": "daemon-A"}
    done = procs.signal_container_job(container, C5_RECORD, None, sig)
    assert done["signalled"] is False, done
    assert "daemon" in done.get("reason", ""), done
    assert daemon_b["helpers"] == [], daemon_b["helpers"]


def test_c5_terminate_sends_nothing_under_another_daemon(daemon_b,
                                                         monkeypatch):
    from agent_compute import cli
    monkeypatch.setattr(cli.time, "sleep", lambda s: None)
    container = {"id": C5_REC, "name": "fenics", "daemon_id": "daemon-A"}
    done = cli.terminate(container, C5_RECORD, None, None)
    signals = [h for h in daemon_b["helpers"] if h[1] == procs._JOB_SIGNAL]
    assert signals == [], done
    assert all(not (r or {}).get("signalled") for r in done.values()), done


@pytest.mark.parametrize("container", [
    {"id": C5_REC, "name": "fenics", "daemon_id": "daemon-B"},   # same
    {"id": C5_REC, "name": "fenics"},                            # legacy
], ids=["same-daemon", "legacy-manifest"])
def test_c5_keep_signal_under_the_recorded_or_an_unrecorded_daemon(
        daemon_b, container):
    done = procs.signal_container_job(container, C5_RECORD, None, 15)
    assert done["signalled"] is True, done
    assert (C5_OTHER, procs._JOB_SIGNAL) in daemon_b["helpers"]


# -- C5 (G1 review M1): the runner's cancel fallback carries the daemon id --------

C5_WORKLOAD = {"venue": "container", "container_id": C5_REC,
               "container": "fenics", "guard_job_id": JOB,
               "container_pid": None, "started_utc": "2026-10-05T00:00:00Z"}


def _no_inspect_no_exec(state) -> bool:
    return state["helpers"] == [] and \
        all(args[:1] == ("info",) for args in state["docker"])


@pytest.mark.parametrize("daemon", ["daemon-B", None],
                         ids=["other-daemon", "daemon-unreadable"])
@pytest.mark.parametrize("sig", [15, 9], ids=["TERM", "KILL"])
def test_c5_signal_workload_refuses_with_the_manifest_daemon_id(daemon_b,
                                                                daemon, sig):
    daemon_b["daemon"] = daemon
    assert procs.signal_workload(dict(C5_WORKLOAD), sig, "daemon-A") is False
    assert _no_inspect_no_exec(daemon_b), daemon_b


def test_c5_keep_signal_workload_without_a_daemon_id_is_unchanged(daemon_b):
    """A legacy manifest (no daemon id): the recorded id is located as
    before (no daemon check), so the inspect still happens."""
    procs.signal_workload(dict(C5_WORKLOAD), 9, None)
    assert any(args[:1] == ("inspect",) for args in daemon_b["docker"])


def _cancel_fallback(tmp_path, monkeypatch, container) -> None:
    """Drive ``Runner.run_guard_attempt``'s real cancel fallback: a guard
    that ignores SIGTERM, a cancel request, no grace; the fallback then
    signals the identified workload with SIGKILL and kills the guard."""
    import sys

    from agent_compute import runner as runner_mod
    from agent_compute.runner import Runner
    from agent_compute.state import JobStore
    job = "acs-20261005T000000Z-c5fall"
    store = JobStore(tmp_path / "state")
    job_dir = store.job_dir(job)
    job_dir.mkdir(parents=True)
    r = Runner.__new__(Runner)
    r.store, r.job_id, r.dir = store, job, job_dir
    r.manifest = {"cwd": str(tmp_path), "container": container}
    r.workload, r.seen, r.guard_proc, r.cancel = dict(C5_WORKLOAD), None, \
        None, False
    ready = tmp_path / "ready"
    stubborn = [sys.executable, "-c",
                "import signal, sys, time; "
                "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                "open(sys.argv[1], 'w').close(); time.sleep(60)",
                str(ready)]
    monkeypatch.setattr(Runner, "guard_argv", lambda self, d, w: stubborn)
    monkeypatch.setattr(Runner, "guard_env", lambda self: None)
    monkeypatch.setattr(Runner, "update", lambda self, **k: None)
    monkeypatch.setattr(Runner, "capture_workload", lambda self, d: None)
    monkeypatch.setattr(Runner, "prove_workload", lambda self, **k: None)
    # the cancel is requested once the guard ignores SIGTERM
    monkeypatch.setattr(Runner, "cancel_request", lambda self: {
        "outcome": "cancelled", "reason": "test"} if ready.exists() else None)
    monkeypatch.setattr(runner_mod, "CANCEL_GRACE_S", 0.0)
    monkeypatch.setattr(runner_mod, "POLL_S", 0.05)
    try:
        r.run_guard_attempt(1, 1.0)
    finally:
        if r.guard_proc is not None and r.guard_proc.poll() is None:
            r.guard_proc.kill()
            r.guard_proc.wait(10)
    assert "guard did not stop in time" in \
        (job_dir / "output.log").read_text()


@pytest.mark.parametrize("daemon", ["daemon-B", None],
                         ids=["other-daemon", "daemon-unreadable"])
def test_c5_runner_cancel_fallback_refuses_under_another_daemon(
        daemon_b, daemon, tmp_path, monkeypatch):
    daemon_b["daemon"] = daemon
    _cancel_fallback(tmp_path, monkeypatch,
                     {"id": C5_REC, "name": "fenics",
                      "daemon_id": "daemon-A"})
    assert daemon_b["docker"], "precondition: the fallback ran"
    assert _no_inspect_no_exec(daemon_b), daemon_b


def test_c5_keep_runner_cancel_fallback_signals_under_the_recorded_daemon(
        daemon_b, tmp_path, monkeypatch):
    """Same daemon: the fallback goes on to locate and scan the container
    (the recorded id is gone here, so nothing is signalled)."""
    _cancel_fallback(tmp_path, monkeypatch,
                     {"id": C5_REC, "name": "fenics",
                      "daemon_id": "daemon-B"})
    assert any(args[:1] == ("inspect",) for args in daemon_b["docker"])


def test_d4_plan_skips_a_holder_with_an_unmarked_descendant():
    holder = proc(20, env=procs.HAS_MARKER)
    plain = proc(30, env=procs.HAS_MARKER)
    scan_ = {"procs": [INIT, holder, proc(21, ppid=20), plain]}
    plan, skipped = procs._signal_plan(scan_)
    assert plan["pids"] == [[30, plain["ticks"]]]
    assert [30, plain["ticks"]] in plan["groups"]
    assert skipped == [[20, "unmarked child; handle manually"]]


# the in-container python3 is resolved by bash before `env -i` clears PATH
# (python:slim has it in /usr/local/bin, smc-dev in /usr/bin)
SPIN = ("\"$(command -v python3)\" -c 'import time\nt=time.time()\n"
        "while time.time()-t<120: pass'")


@needs_docker
def test_d4_leader_dropping_the_marker_is_never_called_clean(env_direct,
                                                             make_container):
    env = env_direct
    name = make_container()
    cid = procs.container_id(name)
    started = utc(time.time() - 1)
    client = guard_style_launch(name, JOB, "env -i " + SPIN)
    try:
        time.sleep(2)
        container = {"name": name, "id": cid, "workdir": "/work"}
        record = {"job_id": JOB, "pid": client.pid, "container_pid": None,
                  "outcome": "killed-interrupted", "started_utc": started}
        before = procs.container_job_evidence(container, record)
        done = procs.signal_container_job(container, record, None, 15)
        time.sleep(1)
        after = procs.container_job_evidence(container, record)
        job_id = craft_job(env, container, record)
        rec = json.loads(env.acs("reconcile", "--terminate", job_id,
                                 "--json").stdout)
        spinning = [r for r in scan(name) if "time.time()-t<120" in r["cmd"]
                    and r["job"] is None]
        report("d4", before=before, signal=done, after=after,
               reconcile=rec, spinning=spinning)
        assert spinning, "precondition: the unmarked leader runs"
        assert before["clean"] is False
        assert after["clean"] is False, after          # the reviewer's D4
        assert spinning[0]["pid"] in [m["pid"] for m in before["members"]]
        assert rec["blocked"] is True, rec
        assert "by hand" in rec["unresolved"][0]["blocker"], rec
        skipped = [s for r in done.get("containers", {}).values()
                   for s in r.get("skipped", [])]
        assert any("unmarked child" in s[1] for s in skipped), done
    finally:
        client.kill()
        container_kill_matching(name, "time.time()-t<120",
                                docker=REAL_DOCKER)


@pytest.mark.skipif(
    not __import__("sys").platform.startswith("linux"),
    reason="[platform] Linux host only: exercises "
           "procs.group_members/host_survivors on "
           "the host's own /proc (runs natively on the Lenovo WSL host; "
           "macOS has no per-thread /proc view)")
def test_b2_linux_host_zombie_main_thread_is_a_live_member():
    """The host venue (Lenovo): a process in its own session whose main
    thread called pthread_exit while a worker thread spins must stay a
    member of its group and keep host_survivors unclean."""
    import os
    import sys
    code = ("import ctypes,threading,time\n"
            "def w():\n    t=time.time()\n"
            "    while time.time()-t<60: time.sleep(0.05)\n"
            "threading.Thread(target=w).start()\n"
            "ctypes.CDLL(None).pthread_exit(None)\n")
    child = subprocess.Popen([sys.executable, "-c", code],
                             start_new_session=True)
    try:
        ident = procs.identity(child.pid)
        state = ""
        for _ in range(100):
            state = open(f"/proc/{child.pid}/stat").read().rsplit(
                ")", 1)[1].split()[0]
            if state == "Z":
                break
            time.sleep(0.1)
        assert state == "Z", "precondition: the main thread exited"
        members = procs.group_members(child.pid, ident["start_epoch"])
        evidence = procs.host_survivors(ident)
        report("b2-linux-host-native", ident=ident, members=members,
               evidence=evidence)
        assert child.pid in members
        assert evidence["clean"] is False and evidence["leader_alive"], \
            evidence
        assert os.getpgid(child.pid) == child.pid
    finally:
        child.kill()
        child.wait(timeout=30)
