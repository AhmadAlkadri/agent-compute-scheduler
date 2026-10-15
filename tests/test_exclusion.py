"""1. Mutual exclusion between independent clients: a guard called
directly on the host's lock (the internal guard; in the legacy profile also
an external guard.py, the pre-ACS path) and ACS jobs, in both orders, and
two ACS clients."""

import time

from conftest import events


def interval(path):
    evs = events(path)
    start = next(e["t"] for e in evs if e["event"] == "start")
    end = next(e["t"] for e in evs if e["event"] == "end")
    return start, end


def test_direct_first_then_acs(env_any, direct_guard):
    env = env_any
    direct_out, acs_out = env.root / "direct.jsonl", env.root / "acs.jsonl"
    direct = env.direct(direct_out, sleep=4, guard=direct_guard)
    env.wait_for(lambda: direct_out.exists(), what="direct start")
    job = env.submit(workload_args=["--out", str(acs_out), "--sleep", "1"])
    waiting = env.wait_phase(job["job_id"], "admission-wait")
    env.wait_for(lambda: "compute lock held by" in
                 (env.state_of(job["job_id"]).get("wait_reason") or ""),
                 what="lock-holder wait reason")
    assert not acs_out.exists(), \
        "ACS workload started while a direct guard held the lock"
    assert direct.wait(60) == 0
    state = env.wait_outcome(job["job_id"])
    assert state["outcome"] == "succeeded", state
    l0, l1 = interval(direct_out)
    a0, a1 = interval(acs_out)
    assert a0 >= l1, (l0, l1, a0, a1)
    assert waiting["phase"] == "admission-wait"


def test_acs_first_then_direct(env_any, direct_guard):
    env = env_any
    direct_out, acs_out = env.root / "direct.jsonl", env.root / "acs.jsonl"
    job = env.submit(workload_args=["--out", str(acs_out), "--sleep", "4"])
    env.wait_phase(job["job_id"], "executing")
    env.wait_for(lambda: acs_out.exists(), what="acs start")
    direct = env.direct(direct_out, sleep=1, wait=60, guard=direct_guard)
    time.sleep(1.5)
    assert not direct_out.exists(), \
        "direct workload started while ACS held the lock"
    assert direct.wait(60) == 0
    assert env.wait_outcome(job["job_id"])["outcome"] == "succeeded"
    a0, a1 = interval(acs_out)
    l0, l1 = interval(direct_out)
    assert l0 >= a1, (a0, a1, l0, l1)


def test_two_acs_clients_do_not_overlap(env_any):
    env = env_any
    outs = [env.root / "a.jsonl", env.root / "b.jsonl"]
    jobs = [env.submit("alpha", workload_args=["--out", str(outs[0]),
                                               "--sleep", "2"]),
            env.submit("beta", workload_args=["--out", str(outs[1]),
                                              "--sleep", "2"])]
    for job in jobs:
        assert env.wait_outcome(job["job_id"])["outcome"] == "succeeded"
    (a0, a1), (b0, b1) = interval(outs[0]), interval(outs[1])
    assert a1 <= b0 or b1 <= a0, ((a0, a1), (b0, b1))
