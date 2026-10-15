"""Host path findings of the independent review of 9de68ce (slice 6).

R1: ``group_members``' ``min_epoch`` filter dropped members that seemed to
    start before the recorded leader; a backwards clock step after the
    capture then hid live members (false clean).
R2: a free leader pid with members was ``killpg``'d; if the group had
    emptied, the pid was reused and the new process double-forked, its
    child sits in that group and was signalled (wrongful signal).
R3: an implausible ``start_utc_s`` was trusted as exact.
R4: a prior runner that stays ``unknown`` never became actionable.
R5: the duplicate-dispatch block had no re-check; ``wait`` said "host
    blocked" when nothing was blocked.

Re-review of 3a3fd0a (blocked on R2): the block time does not prove the
group was still ours (a lost-supervision block can be written long after
the group emptied and the pid was reused).  The member window is now
bounded by the runner's last proof that the exact-start leader was alive
(``workload_seen_utc``; Linux: uptime ticks).  Those tests failed first on
3a3fd0a (failfirst-rereview.txt).

Each test failed first on ``9de68ce`` unless named ``test_keep_*``
(recorded in the private development evidence).  Bare test
processes (killed in ``finally``) and the isolated harness only; no Docker.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import types

import pytest

from agent_compute import cli, procs
from agent_compute.runner import Runner
from agent_compute.state import JobStore, write_json
from test_host_identity import (_clone, _earlier_exact, _other_job,
                                _run_in_process, kill, one_finished_job)

assert one_finished_job        # a fixture, re-exported for these tests

HZ = os.sysconf("SC_CLK_TCK")

# A session leader that forks one child (same process group) and exits
# after ``linger`` seconds.  It prints the child's pid.
FORKER = r'''
import os, sys, time
child = os.fork()
if child == 0:
    time.sleep(120)
    os._exit(0)
print(child, flush=True)
time.sleep(float(sys.argv[1]))
'''


def forker(linger: float = 1.0) -> tuple[subprocess.Popen, int]:
    leader = subprocess.Popen([sys.executable, "-c", FORKER, str(linger)],
                              start_new_session=True, stdout=subprocess.PIPE,
                              text=True)
    child = int(leader.stdout.readline())
    return leader, child


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def reap(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass


def shifted(ident: dict, seconds: int) -> dict:
    """The identity as if recorded ``seconds`` later (a clock stepped
    backwards after the capture, or a record from a later clock)."""
    return _earlier_exact(ident, -seconds)


def utc(epoch: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch))


# -- R1: no min-epoch filter on group members ----------------------------------------

def test_r1_members_are_not_hidden_by_a_later_recorded_start():
    leader, child = forker(linger=1.0)
    try:
        ident = procs.identity(leader.pid)
        leader.wait(timeout=30)
        for _ in range(50):
            if procs.start_info(leader.pid) is None:
                break
            time.sleep(0.1)
        recorded = {**shifted(ident, 10), "venue": "host"}
        ev = procs.host_survivors(recorded)
        assert alive(child)
        assert ev["clean"] is False, ev
        assert child in ev.get("members", []), ev
    finally:
        reap(child)
        kill(leader)


# -- R2: a free leader pid: signal proven members only, never killpg -----------------

def _job_with_block(env, workload: dict, created_utc: str | None,
                    seen_utc: str | None = None) -> str:
    """A finished harness job whose state carries ``workload`` (and, if
    given, the runner's last proof that its exact-start leader was alive,
    ``workload_seen_utc``), and a host block for it created at
    ``created_utc``."""
    job = env.submit(workload_args=["--out", str(env.root / "w")])
    assert env.wait_outcome(job["job_id"])["outcome"] == "succeeded"
    store = JobStore(env.state)
    store.update_state(job["job_id"], workload=workload,
                       workload_seen_utc=seen_utc)
    store.set_block("testhost", job["job_id"], "test", {"workload": workload})
    block = json.loads(store.block_path("testhost").read_text())
    if created_utc is None:
        block.pop("created_utc", None)
    else:
        block["created_utc"] = created_utc
    write_json(store.block_path("testhost"), block)
    return job["job_id"]


def test_r2_reuse_before_the_block_is_never_signalled(env_direct):
    """Re-review of 3a3fd0a: our leader started 600 s ago at pid P and was
    last proven alive 300 s ago; then its group emptied, P was reused by a
    process that double-forked a child into group P and exited, and only
    AFTER that was the lost-supervision block written (e.g. by a later
    ``status``).  The child started after the last proof: never signal."""
    env = env_direct
    daemon, child = forker(linger=1.0)
    try:
        ident = procs.identity(daemon.pid)
        daemon.wait(timeout=30)
        recorded = {**_earlier_exact(ident, 600), "venue": "host"}
        job_id = _job_with_block(env, recorded, utc(time.time() + 1),
                                 seen_utc=utc(ident["start_epoch"] - 300))
        rec = json.loads(env.acs("reconcile", "--terminate", job_id,
                                 "--json").stdout)
        time.sleep(1)
        assert alive(child), "a process of a reused group was signalled"
        assert rec["blocked"] is True, rec
        assert alive(child)
    finally:
        reap(child)
        kill(daemon)


def test_r2_reuse_after_the_block_is_never_signalled(env_direct):
    """Our leader started 600 s ago, was last proven alive 300 s ago; the
    host was blocked 60 s ago; then P was reused and a child forked into
    group P.  That child must survive ``reconcile --terminate``."""
    env = env_direct
    daemon, child = forker(linger=1.0)
    try:
        ident = procs.identity(daemon.pid)
        daemon.wait(timeout=30)
        recorded = {**_earlier_exact(ident, 600), "venue": "host"}
        job_id = _job_with_block(env, recorded,
                                 utc(ident["start_epoch"] - 60),
                                 seen_utc=utc(ident["start_epoch"] - 300))
        rec = json.loads(env.acs("reconcile", "--terminate", job_id,
                                 "--json").stdout)
        time.sleep(1)
        assert alive(child), "a process of a reused group was signalled"
        assert rec["blocked"] is True, rec
        assert procs.signal_host_group(recorded, signal.SIGKILL) is False
        assert alive(child)
    finally:
        reap(child)
        kill(daemon)


def test_r2_no_proof_time_signals_nothing(env_direct):
    """A block time is no proof (the block can be written long after the
    group emptied): without ``workload_seen_utc`` nothing is signalled."""
    env = env_direct
    leader, child = forker(linger=1.0)
    try:
        ident = procs.identity(leader.pid)
        leader.wait(timeout=30)
        recorded = {**ident, "venue": "host"}
        job_id = _job_with_block(env, recorded, utc(time.time() + 1))
        rec = json.loads(env.acs("reconcile", "--terminate", job_id,
                                 "--json").stdout)
        time.sleep(1)
        assert alive(child)
        assert rec["blocked"] is True, rec
        assert "workload_seen_utc" in json.dumps(rec["unresolved"]), rec
        # the runner's own cancel path passes no proof either
        assert procs.signal_host_group(recorded, signal.SIGKILL) is False
        assert alive(child)
    finally:
        reap(child)
        kill(leader)


def test_keep_r2_members_of_our_group_are_terminated(env_direct):
    """Our leader forked a member, was proven alive 2.5 s later, then
    exited: the member (started before that proof) is signalled and the
    job resolves."""
    env = env_direct
    leader, child = forker(linger=3.5)
    try:
        ident = procs.identity(leader.pid)
        time.sleep(2.5)
        seen = utc(time.time())               # taken before the proof
        assert procs.match({**ident, "venue": "host"}) == procs.ALIVE
        leader.wait(timeout=30)
        recorded = {**ident, "venue": "host"}
        job_id = _job_with_block(env, recorded, utc(time.time() + 1),
                                 seen_utc=seen)
        rec = json.loads(env.acs("reconcile", "--terminate", job_id,
                                 "--json").stdout)
        assert rec["blocked"] is False, rec
        assert not alive(child)
    finally:
        reap(child)
        kill(leader)


ORPHANER = r"""
import os, sys, time
child = os.fork()
if child == 0:
    time.sleep(120)
    os._exit(0)
open(sys.argv[1], "w").write(str(child))
time.sleep(float(sys.argv[2]))
"""


def test_r2_real_orphaned_member_is_terminated_after_the_leader_died(
        env_direct):
    """End to end: the runner proves the leader alive while the guard runs
    (``workload_seen_utc``); the leader exits leaving a member in its
    group, so the job ends ``unknown`` and blocks; ``reconcile
    --terminate`` then signals that member and resolves."""
    env = env_direct
    marker = env.root / "child.pid"
    result = env.acs("submit", "--json", "--case", "t", "--timeout-min", "2",
                     "--reserve-gb", "0.5", "--cwd",
                     str(env.projects["alpha"]), "--", sys.executable, "-c",
                     ORPHANER, str(marker), "13")
    assert result.returncode == 0, result.stdout + result.stderr
    job_id = json.loads(result.stdout)["job_id"]
    child = None
    try:
        state = env.wait_outcome(job_id, timeout=90)
        child = int(marker.read_text())
        assert state["outcome"] == "unknown", state
        assert alive(child)
        seen = procs.utc_to_epoch(state.get("workload_seen_utc"))
        assert seen is not None, state
        entry = JobStore(env.state).block("testhost")["jobs"][job_id]
        assert entry["evidence"].get("workload_seen", {}).get(
            "workload_seen_utc") == state["workload_seen_utc"], entry
        rec = json.loads(env.acs("reconcile", "--terminate", job_id,
                                 "--json").stdout)
        assert rec["blocked"] is False, rec
        env.wait_for(lambda: not alive(child), timeout=20,
                     what="orphaned member terminated")
    finally:
        if child:
            reap(child)


def test_r2_member_signalling_is_per_process(monkeypatch):
    """With the leader pid free, members are signalled one by one (never
    ``killpg``), each re-proven right before the signal, and only those
    that started strictly before the last proof minus 1 s."""
    killed, groups = [], []
    leader_start = 1000
    member_starts = {11: 1005, 12: 999, 13: 1100, 14: 1049}
    monkeypatch.setattr(procs.os, "kill",
                        lambda pid, sig: killed.append(pid))
    monkeypatch.setattr(procs.os, "killpg",
                        lambda pid, sig: groups.append(pid))
    monkeypatch.setattr(procs.os, "getpgid", lambda pid: 4242)
    monkeypatch.setattr(procs, "group_members",
                        lambda leader, m=None: [11, 12, 13, 14])

    def info(pid):
        if pid == 4242:
            return None
        return (f"utc:{member_starts[pid]}", float(member_starts[pid]))

    monkeypatch.setattr(procs, "start_info", info)
    workload = {"pid": 4242, "start": None, "start_utc_s": leader_start,
                "start_epoch": float(leader_start)}
    seen = {"workload_seen_utc": utc(1050)}
    assert procs.signal_host_group(workload, 15, seen) is True
    assert killed == [11] and groups == []


def test_r2_linux_member_window_uses_ticks(monkeypatch):
    """Linux: the window is in start ticks against the uptime ticks taken
    at the proof, in the same boot (immune to wall-clock steps)."""
    killed = []
    monkeypatch.setattr(procs.os, "kill",
                        lambda pid, sig: killed.append(pid))
    monkeypatch.setattr(procs.os, "killpg",
                        lambda pid, sig: pytest.fail("killpg"))
    monkeypatch.setattr(procs.os, "getpgid", lambda pid: 4242)
    monkeypatch.setattr(procs, "group_members",
                        lambda leader, m=None: [21, 22, 23])
    monkeypatch.setattr(procs, "_boot_id", lambda: "b1")
    ticks = {21: 5000 + 5 * HZ, 22: 5000 - 1, 23: 5000 + 60 * HZ}
    monkeypatch.setattr(procs, "start_info", lambda pid: None if pid == 4242
                        else (f"ticks:{ticks[pid]}", 9e9))   # wall clock off
    workload = {"pid": 4242, "start": "ticks:5000", "start_epoch": 1.0,
                "boot_id": "b1"}
    seen = {"workload_seen_utc": utc(0), "workload_seen_ticks":
            5000 + 30 * HZ, "workload_seen_boot_id": "b1"}
    assert procs.signal_host_group(workload, 15, seen) is True
    assert killed == [21]
    killed.clear()
    other_boot = dict(seen, workload_seen_boot_id="b0")
    assert procs.signal_host_group(workload, 15, other_boot) is False
    assert killed == []


# -- R3: an implausible start_utc_s is not exact ------------------------------------

@pytest.mark.parametrize("recorded", [
    {"start": None, "start_utc_s": 0, "start_epoch": 1000.0},
    {"start": None, "start_utc_s": -5, "start_epoch": 1000.0},
    {"start": None, "start_utc_s": 1000, "start_epoch": 1000.0 + 14 * 3600},
    {"start": None, "start_utc_s": 1000, "start_epoch": None},
])
def test_r3_implausible_start_utc_s_is_not_exact(monkeypatch, recorded):
    monkeypatch.setattr(procs, "start_info", lambda pid: ("utc:1010", 1010.0))
    ident = {"pid": 4242, **recorded}
    assert procs.has_exact_start(ident) is False
    assert procs.match(ident) == procs.UNKNOWN


# -- R4: a persistent unknown becomes a durable, reconcilable block -----------------

def test_r4_unknown_prior_runner_is_blocked_at_the_deadline(one_finished_job,
                                                           monkeypatch):
    env, first = one_finished_job
    store = JobStore(env.state)
    bystander = subprocess.Popen([sys.executable, "-c",
                                  "import time; time.sleep(120)"],
                                 start_new_session=True)
    try:
        time.sleep(0.3)
        runner = procs.identity(bystander.pid)
        _other_job(store, "acs-r4-other", runner)
        _clone(env, first, "acs-r4-new", 3.0)
        real = procs.start_info

        def failing(pid):
            if pid and int(pid) == bystander.pid:
                raise procs.ProcCheckError("ps exited 137")
            return real(pid)

        monkeypatch.setattr(procs, "start_info", failing)
        rc = _run_in_process(env, "acs-r4-new")
        state = store.state("acs-r4-new")
        assert rc == 12 and state["outcome"] == "admission-timeout", state
        block = store.block("testhost")
        assert block and "acs-r4-other" in block["jobs"], block
        entry = block["jobs"]["acs-r4-other"]
        assert entry["evidence"]["runner"] == runner
        assert "unknown" in entry["reason"]
        monkeypatch.undo()
        rec = json.loads(env.acs("reconcile", "--json").stdout)
        assert rec["blocked"] is True
        assert [u["job_id"] for u in rec["unresolved"]] == ["acs-r4-other"]
    finally:
        kill(bystander)


def test_keep_r4_a_prior_job_that_finished_meanwhile_is_not_blocked(
        one_finished_job, monkeypatch):
    """The deadline block re-checks under the state lock: a prior job that
    finished while we waited is not blocked.  (Passes on 9de68ce only
    because nothing was blocked at the deadline there.)"""
    env, first = one_finished_job
    store = JobStore(env.state)
    bystander = subprocess.Popen([sys.executable, "-c",
                                  "import time; time.sleep(120)"],
                                 start_new_session=True)
    try:
        time.sleep(0.3)
        _other_job(store, "acs-r4-fin", procs.identity(bystander.pid))
        _clone(env, first, "acs-r4-new2", 3.0)
        real = procs.start_info
        calls = []

        def failing(pid):
            if pid and int(pid) == bystander.pid:
                calls.append(pid)
                if len(calls) == 2:
                    # the last check before the 3 s deadline (t ~ 2 s): it
                    # finishes after its state was read
                    store.update_state("acs-r4-fin", phase="done",
                                       outcome="cancelled")
                raise procs.ProcCheckError("ps exited 137")
            return real(pid)

        monkeypatch.setattr(procs, "start_info", failing)
        rc = _run_in_process(env, "acs-r4-new2")
        assert rc == 12, store.state("acs-r4-new2")
        assert len(calls) >= 2      # (+1: liveness re-run at the deadline)
        assert store.block("testhost") is None
    finally:
        kill(bystander)


def test_r4_a_prior_runner_proven_alive_at_the_deadline_is_not_blocked(
        one_finished_job, monkeypatch):
    """Re-review R4: liveness is re-run right before the deadline block;
    a prior runner that can be matched again is not blocked."""
    env, first = one_finished_job
    store = JobStore(env.state)
    bystander = subprocess.Popen([sys.executable, "-c",
                                  "import time; time.sleep(120)"],
                                 start_new_session=True)
    try:
        time.sleep(0.3)
        _other_job(store, "acs-r4-back", procs.identity(bystander.pid))
        _clone(env, first, "acs-r4-new3", 3.0)
        real = procs.start_info
        calls = []

        def flaky(pid):
            if pid and int(pid) == bystander.pid:
                calls.append(pid)
                if len(calls) <= 2:          # the checks at t ~ 0 s, 2 s
                    raise procs.ProcCheckError("ps exited 137")
            return real(pid)

        monkeypatch.setattr(procs, "start_info", flaky)
        rc = _run_in_process(env, "acs-r4-new3")
        assert rc == 12, store.state("acs-r4-new3")
        assert len(calls) >= 3, "liveness was not re-run at the deadline"
        assert store.block("testhost") is None
    finally:
        kill(bystander)


# -- R5: duplicate-dispatch re-check; wait only reports a real block ----------------

def test_r5_duplicate_dispatch_does_not_block_a_job_that_finished(
        one_finished_job, monkeypatch):
    env, first = one_finished_job
    store = JobStore(env.state)
    _clone(env, first, "acs-r5-dup", 30.0)
    prior = {"pid": 999997, "start": "ticks:1", "start_epoch": 1.0}
    store.update_state("acs-r5-dup", phase="dispatched", runner=prior)

    def finishing(ident):
        store.update_state("acs-r5-dup", phase="done", outcome="succeeded")
        return procs.GONE

    monkeypatch.setattr(procs, "liveness", finishing)
    from agent_compute.config import load
    runner = Runner(load(env.config), "acs-r5-dup")
    assert runner.run() == 19
    assert store.block("testhost") is None


def test_keep_r5_duplicate_dispatch_blocks_a_lost_runner(one_finished_job,
                                                         monkeypatch):
    env, first = one_finished_job
    store = JobStore(env.state)
    _clone(env, first, "acs-r5-lost", 30.0)
    prior = {"pid": 999997, "start": "ticks:1", "start_epoch": 1.0}
    store.update_state("acs-r5-lost", phase="dispatched", runner=prior)
    monkeypatch.setattr(procs, "liveness", lambda ident: procs.GONE)
    from agent_compute.config import load
    assert Runner(load(env.config), "acs-r5-lost").run() == 19
    assert "acs-r5-lost" in store.block("testhost")["jobs"]


def test_r5_wait_does_not_claim_a_block_that_was_not_written(
        tmp_path, monkeypatch, capsys):
    store = JobStore(tmp_path)
    write_json(store.job_dir("acs-r5-wait") / "manifest.json",
               {"host": "testhost", "job_id": "acs-r5-wait"})
    store.update_state("acs-r5-wait", phase="executing",
                       runner={"pid": 999996, "start": "ticks:1"})
    cfg = types.SimpleNamespace(state_dir=tmp_path, host="testhost",
                                queue_enabled=False,
                                effective_guard_home=tmp_path / "guard",
                                queue_group="acs-heavy")
    monkeypatch.setattr(procs, "liveness", lambda ident: procs.GONE)
    monkeypatch.setattr(cli, "detect_lost", lambda config: [])
    args = types.SimpleNamespace(job="acs-r5-wait", timeout=2, json=True)
    rc = cli.cmd_wait(cfg, args)
    out = capsys.readouterr().out
    assert "host blocked" not in out, out
    assert rc == cli.EXIT_WAIT_TIMEOUT


def test_unreadable_boot_id_signals_nothing(monkeypatch):
    """Final review of d896616: a recorded boot_id with an unreadable
    current one is not 'the same boot'.  No member window (nothing is
    signalled, with a reason), and never recycled/clean on that basis."""
    killed = []
    monkeypatch.setattr(procs.os, "kill",
                        lambda pid, sig: killed.append(pid))
    monkeypatch.setattr(procs.os, "killpg",
                        lambda pid, sig: pytest.fail("killpg"))
    monkeypatch.setattr(procs.os, "getpgid", lambda pid: 4242)
    monkeypatch.setattr(procs, "group_members",
                        lambda leader, m=None: [21])
    monkeypatch.setattr(procs, "_boot_id", lambda: None)     # unreadable
    monkeypatch.setattr(procs, "start_info", lambda pid: None if pid == 4242
                        else (f"ticks:{5000 + 5 * HZ}", 100.0))
    workload = {"pid": 4242, "start": "ticks:5000", "start_epoch": 50.0,
                "boot_id": "b1"}
    seen = {"workload_seen_utc": utc(1000), "workload_seen_ticks":
            5000 + 30 * HZ, "workload_seen_boot_id": "b1"}
    assert procs.signal_host_group(workload, 15, seen) is False
    assert killed == []
    reason = procs.member_signal_refusal(workload, seen)
    assert reason and "boot" in reason, reason
    # a recorded proof boot_id alone (pre-boot_id workload record) is not
    # verifiable either
    legacy = {k: v for k, v in workload.items() if k != "boot_id"}
    assert procs.signal_host_group(legacy, 15, seen) is False
    assert killed == []
    # fewer ticks at the pid: never recycled when the boot cannot be read
    monkeypatch.setattr(procs, "start_info",
                        lambda pid: ("ticks:4000", 40.0))
    assert procs.match({"pid": 4242, "start": "ticks:5000",
                        "boot_id": "b1"}) == procs.UNKNOWN
    ev = procs.host_survivors({"pid": 4242, "start": "ticks:5000",
                               "boot_id": "b1"})
    assert ev["clean"] is False and ev["verified"] is False, ev
