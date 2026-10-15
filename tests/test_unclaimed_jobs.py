"""A3 + A12: jobs that no runner ever claimed must not stay stuck forever.

A3: a queued job whose ``--cwd`` vanished ended as Pueue ``FailedToSpawn``
but stayed ``queued`` in ACS: ``cancel`` could not finish it, its request id
was burnt and every daemon start re-closed the dispatch gate (audit e7).
A12: the request-id check and bind were not atomic, and a client killed
between bind and enqueue stranded an ``accepted`` job.
"""

import datetime as dt
import json
import subprocess
import sys
import threading
import time

import pytest

from conftest import PUEUE, WORKLOAD

from agent_compute import cli
from agent_compute import queue as q
from agent_compute.config import load
from agent_compute.state import NEVER_STARTED, JobStore


def pueue(env, *args):
    return subprocess.run([PUEUE, "-c", str(env.state / "pueue" / "pueue.yml"),
                           *args], capture_output=True, text=True, timeout=60)


def submit_args(env, *extra, project="alpha", out="x.jsonl"):
    return cli.parser().parse_args(
        ["submit", "--json", "--case", "t", "--timeout-min", "1",
         "--reserve-gb", "0.5", "--cwd", str(env.projects[project]), *extra])


def workload(env, out, sleep=0):
    return [sys.executable, str(WORKLOAD), "--out", str(env.root / out),
            "--sleep", str(sleep)]


def backdate(env, job_id, seconds):
    path = env.state / "jobs" / job_id / "state.json"
    st = json.loads(path.read_text())
    old = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=seconds)
    st["accepted_utc"] = old.strftime("%Y-%m-%dT%H:%M:%SZ")
    path.write_text(json.dumps(st))


# --- A3 -------------------------------------------------------------------

def test_e7_job_whose_cwd_vanished_while_queued_is_not_stuck(env_queue):
    """e7 reproduction: the Pueue task no longer depends on the job cwd, so
    the job reaches a never-started outcome; its request id is reusable and
    the next daemon start does not close the gate for it."""
    env = env_queue
    sub = env.projects["alpha"] / "sub"
    sub.mkdir()
    a = env.submit("beta", workload_args=["--out", str(env.root / "a.jsonl"),
                                          "--sleep", "4"])
    r = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                "--reserve-gb", "0.5", "--cwd", str(sub),
                "--request-id", "e7-req", "--",
                *workload(env, "b.jsonl"))
    assert r.returncode == 0, r.stdout + r.stderr
    b = json.loads(r.stdout)["job_id"]
    sub.rmdir()
    env.wait_outcome(a["job_id"])
    st = env.wait_outcome(b, timeout=60)
    assert st["outcome"] in NEVER_STARTED, st
    assert "never started" in st["reason"]
    assert not (env.root / "b.jsonl").exists()
    again = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                    "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]),
                    "--request-id", "e7-req", "--",
                    *workload(env, "c.jsonl"))
    assert again.returncode == 0, again.stdout + again.stderr
    env.wait_outcome(json.loads(again.stdout)["job_id"])
    pueue(env, "shutdown")
    env.wait_for(lambda: not q.daemon_running(load(env.config)),
                 what="daemon down")
    d = json.loads(env.acs("daemon", "start", "--json", check=True).stdout)
    assert not any("gate closed" in n for n in d["notices"]), d


def test_status_settles_an_unclaimed_job_whose_task_is_gone(env_queue):
    """A task that ended or vanished without any runner claim is finished as
    never started by `status`, and does not re-close the gate."""
    env = env_queue
    a = env.submit("alpha", workload_args=["--out", str(env.root / "a.jsonl"),
                                           "--sleep", "5"])
    b = env.submit("beta", workload_args=["--out", str(env.root / "b.jsonl")])
    env.wait_phase(a["job_id"], "executing")
    task = env.state_of(b["job_id"])["pueue_task"]
    assert pueue(env, "remove", str(task)).returncode == 0
    view = json.loads(env.acs("status", b["job_id"], "--json").stdout)
    assert view["job"]["outcome"] == "launch-failed", view
    assert "never started" in view["job"]["reason"]
    assert "runner" not in env.state_of(b["job_id"])
    assert env.wait_outcome(a["job_id"])["outcome"] == "succeeded"
    assert not (env.root / "b.jsonl").exists()
    pueue(env, "shutdown")
    env.wait_for(lambda: not q.daemon_running(load(env.config)),
                 what="daemon down")
    d = json.loads(env.acs("daemon", "start", "--json", check=True).stdout)
    assert not any("gate closed" in n for n in d["notices"]), d


def test_cancel_finishes_an_unclaimed_job_whose_task_is_gone(env_queue):
    env = env_queue
    a = env.submit("alpha", workload_args=["--out", str(env.root / "a.jsonl"),
                                           "--sleep", "8"])
    b = env.submit("beta", workload_args=["--out", str(env.root / "b.jsonl")])
    env.wait_phase(a["job_id"], "executing")
    task = env.state_of(b["job_id"])["pueue_task"]
    assert pueue(env, "remove", str(task)).returncode == 0
    t0 = time.time()
    result = json.loads(env.acs("cancel", b["job_id"], "--json").stdout)
    assert result["cancelled"] is True, result
    assert "never started" in result["reason"]
    assert time.time() - t0 < 30
    assert env.state_of(b["job_id"])["outcome"] == "cancelled"
    env.wait_outcome(a["job_id"])


def test_settle_rules_for_task_states(env_queue, monkeypatch):
    """Unit: only an unclaimed, unfinished job whose task is Done or missing
    is settled; a claimed job, or a Queued/Running task, is never touched."""
    env = env_queue
    config = load(env.config)
    store = JobStore(config.state_dir)
    ids = {}
    for name, extra in (("done", {"pueue_task": 1}),
                        ("missing", {"pueue_task": 2}),
                        ("queued", {"pueue_task": 3}),
                        ("running", {"pueue_task": 4}),
                        ("claimed", {"pueue_task": 5,
                                     "runner": {"pid": 1, "start": "x"}})):
        job = f"acs-20260101T000000Z-{name}"
        (env.state / "jobs" / job).mkdir(parents=True)
        (env.state / "jobs" / job / "manifest.json").write_text(
            json.dumps({"host": config.host, "job_id": job}))
        store.update_state(job, phase="queued", accepted_utc="2026-01-01T"
                           "00:00:00Z", **extra)
        ids[name] = job
    tasks = {"1": {"status": {"Done": {"result": {"FailedToSpawn": "cwd"}}}},
             "3": {"status": "Queued"},
             "4": {"status": {"Running": {}}},
             "5": {"status": {"Done": {"result": "Success"}}}}
    settled = cli.settle_unclaimed(config, store, tasks=tasks)
    assert sorted(settled) == sorted([ids["done"], ids["missing"]])
    for name in ("done", "missing"):
        st = store.state(ids[name])
        assert st["outcome"] == "launch-failed"
        assert "never started" in st["reason"]
    for name in ("queued", "running", "claimed"):
        assert not store.state(ids[name]).get("outcome")


def test_pending_work_ignores_an_unclaimed_job_whose_task_ended(env_queue):
    env = env_queue
    config = load(env.config)
    store = JobStore(config.state_dir)
    job = "acs-20260101T000000Z-ended1"
    (env.state / "jobs" / job).mkdir(parents=True)
    store.update_state(job, phase="queued", pueue_task=7)
    data = env.state / "pueue" / "data"
    data.mkdir(parents=True, exist_ok=True)
    (data / "state.json").write_text(json.dumps({"tasks": {"7": {
        "group": "acs-heavy",
        "status": {"Done": {"result": {"FailedToSpawn": "no such dir"}}}}}}))
    assert q._pending_work(config) == []
    (data / "state.json").write_text(json.dumps({"tasks": {"7": {
        "group": "acs-heavy", "status": "Queued"}}}))
    assert q._pending_work(config)          # still fail-closed when queued


def test_task_runs_in_root_not_in_the_job_cwd(env_queue):
    env = env_queue
    job = env.submit("alpha", workload_args=["--out", str(env.root / "a")])
    env.wait_outcome(job["job_id"])
    task = env.state_of(job["job_id"])["pueue_task"]
    status = json.loads(pueue(env, "status", "--json").stdout)
    assert status["tasks"][str(task)]["path"] == "/"


# --- A12 ------------------------------------------------------------------

def test_concurrent_duplicate_request_id_binds_once(env_direct, monkeypatch):
    env = env_direct
    config = load(env.config)
    real = cli.build_manifest

    def slow(*a, **k):
        time.sleep(1.0)
        return real(*a, **k)

    monkeypatch.setattr(cli, "build_manifest", slow)
    monkeypatch.setattr(cli, "spawn_detached_runner", lambda *a: None)
    results = []

    def one(name):
        args = submit_args(env, "--request-id", "dup-req")
        results.append(cli.accept(config, args, workload(env, name)))

    threads = [threading.Thread(target=one, args=(f"{i}.jsonl",))
               for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    codes = sorted(code for code, _ in results)
    assert codes == [0, cli.EXIT_NOT_ACCEPTED], results
    accepted = [r for code, r in results if code == 0][0]
    assert JobStore(config.state_dir).request_job("dup-req") == \
        accepted["job_id"]


def _strand(env, config, monkeypatch, target, request_id):
    """Simulate a client killed after binding but before enqueueing."""
    def killed(*a, **k):
        raise KeyboardInterrupt
    monkeypatch.setattr(target[0], target[1], killed)
    args = submit_args(env, "--request-id", request_id)
    try:
        cli.accept(config, args, workload(env, "stranded.jsonl"))
    except KeyboardInterrupt:
        pass
    else:
        raise AssertionError("the simulated kill did not happen")
    monkeypatch.undo()
    job = JobStore(config.state_dir).request_job(request_id)
    assert job
    st = JobStore(config.state_dir).state(job)
    assert st["phase"] == "accepted" and not st.get("outcome")
    assert st.get("pueue_task") is None and not st.get("runner")
    return job


def test_bound_without_task_is_settled_after_the_grace(env_direct,
                                                      monkeypatch):
    env = env_direct
    config = load(env.config)
    job = _strand(env, config, monkeypatch, (cli, "spawn_detached_runner"),
                  "strand-req")
    fresh = json.loads(env.acs("status", job, "--json").stdout)["job"]
    assert fresh["outcome"] is None      # within the grace: left alone
    again = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                    "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]),
                    "--request-id", "strand-req", "--",
                    *workload(env, "n.jsonl"))
    assert again.returncode == cli.EXIT_NOT_ACCEPTED
    backdate(env, job, cli.UNCLAIMED_GRACE_S + 60)
    view = json.loads(env.acs("status", job, "--json").stdout)["job"]
    assert view["outcome"] in NEVER_STARTED, view
    assert "never started" in view["reason"]
    again = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                    "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]),
                    "--request-id", "strand-req", "--",
                    *workload(env, "n.jsonl"))
    assert again.returncode == 0, again.stdout + again.stderr
    env.wait_outcome(json.loads(again.stdout)["job_id"])


def test_resubmit_settles_a_stale_bound_without_task(env_direct,
                                                     monkeypatch):
    env = env_direct
    config = load(env.config)
    job = _strand(env, config, monkeypatch, (cli, "spawn_detached_runner"),
                  "strand2")
    backdate(env, job, cli.UNCLAIMED_GRACE_S + 60)
    again = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                    "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]),
                    "--request-id", "strand2", "--", *workload(env, "n.jsonl"))
    assert again.returncode == 0, again.stdout + again.stderr
    assert env.state_of(job)["outcome"] in NEVER_STARTED
    env.wait_outcome(json.loads(again.stdout)["job_id"])


def test_cancel_finishes_a_bound_without_task_job_at_once(env_queue,
                                                          monkeypatch):
    env = env_queue
    config = load(env.config)
    job = _strand(env, config, monkeypatch, (q, "add_task"), "strand3")
    t0 = time.time()
    result = json.loads(env.acs("cancel", job, "--json").stdout)
    assert result["cancelled"] is True, result
    assert "never started" in result["reason"]
    assert time.time() - t0 < 30
    assert env.state_of(job)["outcome"] == "cancelled"


def test_accept_never_enqueues_a_job_finished_while_enqueueing(env_queue,
                                                               monkeypatch):
    """If the job is settled (or cancelled) between bind and enqueue, the
    freshly added task is removed and submit reports not accepted."""
    env = env_queue
    config = load(env.config)
    store = JobStore(config.state_dir)
    real = q.add_task

    def add_then_cancel(config_, job_id):
        task = real(config_, job_id)
        cli._finish_if_unclaimed(store, job_id, "cancelled",
                                 "cancelled meanwhile; never started")
        return task

    monkeypatch.setattr(q, "add_task", add_then_cancel)
    q.ensure_daemon(config)
    pueue(env, "pause", "-g", "acs-heavy")
    code, result = cli.accept(config, submit_args(env),
                              workload(env, "late.jsonl"))
    assert code == cli.EXIT_NOT_ACCEPTED, result
    job = result["job_id"]
    assert store.state(job)["outcome"] == "cancelled"
    tasks = json.loads(pueue(env, "status", "--json").stdout)["tasks"]
    assert not any(t.get("label") == job for t in tasks.values()), tasks


# --- C1 (boundary audit; campaign G slice G1) ------------------------------
#
# `pueue add` is a side effect followed by reply handling.  When the reply
# is lost (a client-side timeout, a nonzero exit after the daemon stored the
# task, unparseable stdout) or `_mark_queued` fails after the add, the
# runner may already have claimed the job.  The request binding must then
# survive and the reply must say the job may be running: a resubmit with
# the same id returns the existing job instead of starting it twice.
# Basis: the private development record (register item C1).

C1_RUNNER = {"pid": 999999, "start": "ticks:1", "note": "fake claim (C1)"}


def _claim_like_the_runner(store, job_id):
    """What ``Runner.claim`` writes, under the state lock."""
    store.update_state(job_id, phase="dispatched", runner=C1_RUNNER,
                       reason="runner started")


def _c1_failure(kind, store):
    """A fake ``q.add_task`` whose daemon accepted the task, whose runner
    claimed the job, and whose client side then failed as ``kind``."""
    def add_task(config_, job_id):
        _claim_like_the_runner(store, job_id)
        if kind == "timeout":
            raise subprocess.TimeoutExpired(["pueue", "add"], 60)
        if kind == "nonzero":
            raise q.QueueError("pueue add failed: connection reset")
        if kind == "bad-stdout":
            return int("New task added (id 7).")
        return 7                           # kind == "mark-queued-oserror"
    return add_task


@pytest.mark.parametrize("kind", ["timeout", "nonzero", "bad-stdout",
                                  "mark-queued-oserror"])
def test_c1_enqueue_reply_lost_after_claim_keeps_binding(env_queue,
                                                          monkeypatch, kind):
    env = env_queue
    config = load(env.config)
    store = JobStore(config.state_dir)
    monkeypatch.setattr(q, "ensure_daemon", lambda config_: [])
    monkeypatch.setattr(q, "add_task", _c1_failure(kind, store))
    if kind == "mark-queued-oserror":
        def full_disk(*_a, **_k):
            raise OSError(28, "No space left on device")
        monkeypatch.setattr(cli, "_mark_queued", full_disk)
    code, result = cli.accept(config, submit_args(env, "--request-id",
                                                  "c1-req"),
                              workload(env, "c1.jsonl"))
    job = result.get("job_id")
    assert job, result
    assert code == cli.EXIT_NOT_ACCEPTED, result
    assert result["accepted"] == "unknown", result
    assert "may be running" in result["reason"], result
    assert store.request_job("c1-req") == job          # the binding survives
    st = store.state(job)
    assert not st.get("outcome"), st                   # accept wrote none
    assert st["runner"] == C1_RUNNER

    def never(*_a, **_k):
        raise AssertionError("a resubmit must not enqueue a second job")
    monkeypatch.setattr(q, "add_task", never)
    code2, again = cli.accept(config, submit_args(env, "--request-id",
                                                  "c1-req"),
                              workload(env, "c1.jsonl"))
    assert code2 == cli.EXIT_NOT_ACCEPTED, again
    assert again["job_id"] == job, again               # the existing job
    assert again["accepted"] is False
    assert [j for j in store.all_ids()] == [job]


def test_c1_keep_enqueue_failure_before_claim_frees_the_id(env_queue,
                                                          monkeypatch):
    """Unchanged: a failure before any runner claim finishes the job as
    never started and frees the request id."""
    env = env_queue
    config = load(env.config)
    store = JobStore(config.state_dir)
    monkeypatch.setattr(q, "ensure_daemon", lambda config_: [])

    def refused(config_, job_id):
        raise q.QueueError("pueue add failed: no daemon")
    monkeypatch.setattr(q, "add_task", refused)
    code, result = cli.accept(config, submit_args(env, "--request-id",
                                                  "c1-free"),
                              workload(env, "c1.jsonl"))
    assert code == cli.EXIT_NOT_ACCEPTED and result["accepted"] is False
    job = result["job_id"]
    st = store.state(job)
    assert st["outcome"] in NEVER_STARTED and "never started" in st["reason"]
    assert store.request_job("c1-free") is None
    monkeypatch.setattr(q, "add_task", lambda config_, job_id: 7)
    code2, again = cli.accept(config, submit_args(env, "--request-id",
                                                  "c1-free"),
                              workload(env, "c1.jsonl"))
    assert code2 == 0 and again["accepted"] is True, again
    assert again["job_id"] != job
    assert store.request_job("c1-free") == again["job_id"]
