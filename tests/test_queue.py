"""2. Queue behaviour: one shared heavy group for every project."""

import json
import subprocess

from conftest import PUEUE, events


def pueue_status(env):
    out = subprocess.run([PUEUE, "-c", str(env.state / "pueue" / "pueue.yml"),
                          "status", "--json"], capture_output=True, text=True,
                         check=True).stdout
    return json.loads(out)


def test_projects_share_one_single_slot_group(env_queue):
    env = env_queue
    outs = [env.root / "a.jsonl", env.root / "b.jsonl", env.root / "c.jsonl"]
    a = env.submit("alpha", workload_args=["--out", str(outs[0]),
                                           "--sleep", "3"])
    b = env.submit("beta", workload_args=["--out", str(outs[1]),
                                          "--sleep", "1"])
    c = env.submit("alpha", workload_args=["--out", str(outs[2]),
                                           "--sleep", "1"])
    env.wait_phase(a["job_id"], "executing")
    status = pueue_status(env)
    groups = status["groups"]
    assert groups["acs-heavy"]["parallel_tasks"] == 1
    assert set(groups) == {"default", "acs-heavy"}     # no per-project groups
    assert {t["group"] for t in status["tasks"].values()} == {"acs-heavy"}
    view = json.loads(env.acs("status", "--json").stdout)
    phases = {j["job_id"]: (j["phase"], j["queue_task"]["status"])
              for j in view["jobs"]}
    assert phases[a["job_id"]] == ("executing", "Running")
    assert phases[b["job_id"]] == ("queued", "Queued")
    assert phases[c["job_id"]] == ("queued", "Queued")
    for job in (a, b, c):
        assert env.wait_outcome(job["job_id"])["outcome"] == "succeeded"
    starts = [events(o)[0]["t"] for o in outs]
    assert starts == sorted(starts), "simple FIFO order within the shared queue"


def test_status_distinguishes_queued_resource_wait_and_executing(env_queue):
    env = env_queue
    legacy_out = env.root / "legacy.jsonl"
    legacy = env.direct(legacy_out, sleep=4)
    env.wait_for(lambda: legacy_out.exists(), what="legacy start")
    a = env.submit("alpha", workload_args=["--out", str(env.root / "a"),
                                           "--sleep", "1"])
    b = env.submit("beta", workload_args=["--out", str(env.root / "b"),
                                          "--sleep", "1"])
    env.wait_for(lambda: "compute lock held by" in
                 (env.state_of(a["job_id"]).get("wait_reason") or ""),
                 what="resource wait")
    view = {j["job_id"]: j for j in
            json.loads(env.acs("status", "--json").stdout)["jobs"]}
    assert view[a["job_id"]]["phase"] == "admission-wait"
    assert "direct/" in view[a["job_id"]]["wait_reason"]
    assert view[b["job_id"]]["phase"] == "queued"
    report = json.loads(env.acs("status", "--json").stdout)
    assert report["admission"]["compute_lock_holder"]["job_id"].startswith(
        "direct/")
    legacy.wait(60)
    env.wait_phase(a["job_id"], "executing")
    for job in (a, b):
        assert env.wait_outcome(job["job_id"])["outcome"] == "succeeded"


def test_cancel_pending_job_never_starts(env_queue):
    env = env_queue
    blocker_out, pending_out = env.root / "blocker.jsonl", env.root / "p.jsonl"
    blocker = env.submit("alpha", workload_args=["--out", str(blocker_out),
                                                 "--sleep", "3"])
    pending = env.submit("beta", workload_args=["--out", str(pending_out),
                                                "--sleep", "1"])
    env.wait_phase(blocker["job_id"], "executing")
    result = json.loads(env.acs("cancel", pending["job_id"], "--json").stdout)
    assert result["cancelled"] is True, result
    assert "never started" in result["reason"]
    assert env.wait_outcome(blocker["job_id"])["outcome"] == "succeeded"
    state = env.state_of(pending["job_id"])
    assert state["outcome"] == "cancelled"
    assert "runner" not in state
    assert not pending_out.exists()
    tasks = pueue_status(env)["tasks"]
    assert str(state["pueue_task"]) not in tasks        # removed, never run


def test_run_takes_its_turn_in_the_queue(env_queue):
    env = env_queue
    first_out, run_out = env.root / "first.jsonl", env.root / "run.jsonl"
    first = env.submit("alpha", workload_args=["--out", str(first_out),
                                               "--sleep", "3"])
    env.wait_phase(first["job_id"], "executing")
    import sys
    from conftest import WORKLOAD
    result = env.acs("run", "--json", "--case", "r", "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--cwd", str(env.projects["beta"]),
                     "--admission-timeout", "60", "--",
                     sys.executable, str(WORKLOAD), "--out", str(run_out),
                     "--sleep", "0")
    assert result.returncode == 0, result.stdout + result.stderr
    view = json.loads(result.stdout)
    assert view["outcome"] == "succeeded"
    assert events(run_out)[0]["t"] >= events(first_out)[-1]["t"]
