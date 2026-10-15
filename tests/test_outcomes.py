"""3. Outcomes keep correct status and evidence."""

import json
import sys

from conftest import WORKLOAD, events


def test_nonzero_exit_is_failed_with_exit_code(env_any):
    env = env_any
    out = env.root / "w.jsonl"
    job = env.submit(workload_args=["--out", str(out), "--exit", "3"])
    state = env.wait_outcome(job["job_id"])
    assert state["outcome"] == "failed", state
    assert state["exit_code"] == 3        # not mistaken for guard exit 3
    assert state["guard_outcome"] == "failed"
    assert state["cleanup"]["clean"] is True


def test_missing_executable_is_launch_failed(env_any):
    env = env_any
    result = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]),
                     "--", "/nonexistent/solver", "--flag")
    job = json.loads(result.stdout)
    state = env.wait_outcome(job["job_id"])
    assert state["outcome"] == "launch-failed", state
    assert "never started" in state["reason"]
    assert env.lock_free()


def test_admission_refusal_is_not_a_failed_calculation(env_any):
    env = env_any
    # a reserve above the host budget is a terminal guard refusal
    result = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                     "--reserve-gb", "99", "--cwd", str(env.projects["alpha"]),
                     "--", sys.executable, str(WORKLOAD), "--out",
                     str(env.root / "never"))
    state = env.wait_outcome(json.loads(result.stdout)["job_id"])
    assert state["outcome"] == "refused", state
    assert "exceeds the host budget" in state["reason"]
    assert not (env.root / "never").exists()


def test_insufficient_memory_waits_then_times_out(env_any):
    env = env_any
    env.free.write_text(str(1 * 1024 ** 3))     # simulated: below the floor
    job = env.submit(workload_args=["--out", str(env.root / "never")],
                     extra=["--lock-wait", "4"])
    env.wait_for(lambda: "insufficient host resources" in
                 (env.state_of(job["job_id"]).get("wait_reason") or ""),
                 what="resource wait reason")
    state = env.wait_outcome(job["job_id"])
    assert state["outcome"] == "admission-timeout", state
    assert state["attempt"] >= 2
    assert not (env.root / "never").exists()


def test_memory_recovers_during_admission_wait(env_any):
    env = env_any
    env.free.write_text(str(1 * 1024 ** 3))
    out = env.root / "w.jsonl"
    job = env.submit(workload_args=["--out", str(out)],
                     extra=["--lock-wait", "30"])
    env.wait_for(lambda: (env.state_of(job["job_id"]).get("attempt") or 0) >= 2,
                 what="a refused attempt")
    env.free.write_text(str(64 * 1024 ** 3))
    state = env.wait_outcome(job["job_id"])
    assert state["outcome"] == "succeeded", state


def test_bounded_run_wait_expires_without_starting(env_any):
    env = env_any
    legacy_out = env.root / "legacy.jsonl"
    legacy = env.direct(legacy_out, sleep=8)
    env.wait_for(lambda: legacy_out.exists(), what="legacy start")
    never = env.root / "never"
    result = env.acs("run", "--json", "--case", "t", "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]),
                     "--admission-timeout", "3", "--",
                     sys.executable, str(WORKLOAD), "--out", str(never))
    view = json.loads(result.stdout)
    assert view["outcome"] == "admission-timeout", view
    assert result.returncode == 12
    assert not never.exists()
    legacy.wait(60)


def test_cancel_running_job(env_any):
    env = env_any
    out = env.root / "w.jsonl"
    job = env.submit(workload_args=["--out", str(out), "--sleep", "60"])
    env.wait_phase(job["job_id"], "executing")
    env.wait_for(lambda: out.exists(), what="workload start")
    result = json.loads(env.acs("cancel", job["job_id"], "--json").stdout)
    assert result["cancelled"] is True, result
    state = env.state_of(job["job_id"])
    assert state["outcome"] == "cancelled"
    assert state["guard_outcome"] == "killed-interrupted"
    assert state["cleanup"]["clean"] is True
    assert [e["event"] for e in events(out)] == ["start"]
    assert env.lock_free()


def test_execution_timeout(env_any):
    env = env_any
    out = env.root / "w.jsonl"
    job = env.submit(timeout_min=0.05, workload_args=["--out", str(out),
                                                      "--sleep", "60"])
    state = env.wait_outcome(job["job_id"], timeout=60)
    assert state["outcome"] == "timed-out", state
    assert state["guard_outcome"] == "killed-walltime"
    assert state["cleanup"]["clean"] is True


def test_timeout_must_be_finite_and_within_project_cap(env_any):
    env = env_any
    for value in ("0", "-1", "inf", "6"):          # cap is 5 min in tests
        result = env.acs("submit", "--case", "t", "--timeout-min", value,
                         "--cwd", str(env.projects["alpha"]), "--", "true")
        assert result.returncode == 2, (value, result.stderr)
    result = env.acs("submit", "--case", "t", "--cwd",
                     str(env.projects["alpha"]), "--", "true")
    assert result.returncode == 2          # --timeout-min is required


def _status_view(env, job_id: str) -> dict:
    result = env.acs("status", job_id, "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)["job"]


def test_status_json_exposes_the_guard_result_of_a_cancelled_job(env_any):
    """A19: consumers must not parse `reason`; the structured guard result
    is in the status view."""
    env = env_any
    out = env.root / "w.jsonl"
    job = env.submit(workload_args=["--out", str(out), "--sleep", "60"])
    env.wait_phase(job["job_id"], "executing")
    env.wait_for(lambda: out.exists(), what="workload start")
    env.acs("cancel", job["job_id"], "--json")
    state = env.wait_outcome(job["job_id"])
    view = _status_view(env, job["job_id"])
    assert view["outcome"] == "cancelled", view
    assert view["guard_outcome"] == "killed-interrupted"
    assert view["exit_code"] == state["exit_code"]
    assert view["peak_rss_gb"] == state["peak_rss_gb"]
    assert view["execution_wall_s"] == state["wall_s"]
    assert view["cleanup"] == {"verified": True, "clean": True,
                               "method": state["cleanup"].get("method"),
                               "reason": state["cleanup"].get("reason")}


def test_status_json_exposes_the_guard_result_of_a_failed_job(env_any):
    env = env_any
    job = env.submit(workload_args=["--out", str(env.root / "w.jsonl"),
                                    "--exit", "3"])
    env.wait_outcome(job["job_id"])
    view = _status_view(env, job["job_id"])
    assert view["outcome"] == "failed", view
    assert view["guard_outcome"] == "failed"
    assert view["exit_code"] == 3
    assert view["cleanup"]["clean"] is True


def test_status_json_guard_fields_are_null_when_never_launched(env_any):
    env = env_any
    result = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                     "--reserve-gb", "99", "--cwd", str(env.projects["alpha"]),
                     "--", sys.executable, str(WORKLOAD), "--out",
                     str(env.root / "never"))
    job_id = json.loads(result.stdout)["job_id"]
    env.wait_outcome(job_id)
    view = _status_view(env, job_id)
    assert view["outcome"] == "refused", view
    for key in ("guard_outcome", "exit_code", "peak_rss_gb", "cleanup"):
        assert key in view and view[key] is None, (key, view)
