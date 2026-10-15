import json
from conftest import events


def test_success_direct(env_direct):
    env = env_direct
    out = env.root / "w.jsonl"
    job = env.submit(workload_args=["--out", str(out), "--sleep", "1"])
    state = env.wait_outcome(job["job_id"])
    assert state["outcome"] == "succeeded", state
    assert [e["event"] for e in events(out)] == ["start", "end"]


def test_success_queue(env_queue):
    env = env_queue
    out = env.root / "w.jsonl"
    job = env.submit(workload_args=["--out", str(out), "--sleep", "1"])
    state = env.wait_outcome(job["job_id"])
    assert state["outcome"] == "succeeded", state

    # register A41: the runner writes the outcome before its Pueue task
    # process exits, so the task may still read "Running" for a moment
    def task_status():
        status = json.loads(env.acs("status", job["job_id"],
                                    "--json").stdout)
        found = status["job"]["queue_task"]["status"]
        return found if found == "Done" else None
    assert env.wait_for(task_status, timeout=30, interval=0.5,
                        what="the queue task to end") == "Done"
