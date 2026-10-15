"""The machine-readable status contract consumers rely on (dogfooding
investigation, recorded privately).

Audit A19 (2026-09-27) came from a consumer whose finalizer had to read
``guard.json`` because ``status --json`` lacked ``guard_outcome``; the skill
now documents the job view's fields and promises that keys are only ever
added.  These tests pin that documented field set and the meaning of the
two durations, through the real CLI in the isolated harness (direct mode:
no queue daemon; the configured guard runs a seconds-long workload in a
sandboxed guard home).

What this establishes: every field named in the skill's "Status JSON"
table is present in ``status JOB --json`` (``job``), in ``status --all
--json`` (``jobs``) and in ``wait --json``; ``queue_wait_s`` is accepted ->
admitted and ``execution_wall_s`` is the guard's wall time; the evidence
paths name files that exist; ``submit --json`` says "accepted", never
"succeeded".  It does not establish anything about the values' semantics
beyond those checks.

G2a (campaign G): the v1 key set is frozen.  ``FROZEN_V1_KEYS`` is every
key the v0.1.2 view always emitted, plus ``queue_wait_s`` and
``workload``, which were conditional and are now always present (null
until known).  ``queue_task`` (queue up) and ``unreadable`` (C4) stay
conditional.  The view of a finished job and of a queued job carry them
all.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

from conftest import WORKLOAD

# the field table of skill/agent-compute-scheduler/SKILL.md, "Status JSON"
DOCUMENTED_FIELDS = {
    "job_id", "project", "host", "case", "request_id", "owner",
    "phase", "outcome", "guard_outcome", "exit_code", "peak_rss_gb",
    "cleanup", "accepted_utc", "admitted_utc", "ended_utc",
    "queue_wait_s", "execution_wall_s", "wait_reason", "reason",
    "workload", "evidence",
    # G2a
    "submitter", "acs_version", "acs_source_sha", "dispatched_utc",
    "dispatch_wait_s", "admission_wait_s", "queue_position", "job_ahead",
    "probe", "parent_job",
    # G3a
    "admission_memory",
    # G3b
    "code", "placement",
}
# G2a: every key the v0.1.2 view always emitted, never removed; plus
# queue_wait_s and workload, now always present (null until known)
FROZEN_V1_KEYS = {
    "job_id", "project", "host", "case", "request_id", "owner", "phase",
    "outcome", "reason", "wait_reason", "exit_code", "guard_outcome",
    "peak_rss_gb", "cleanup", "accepted_utc", "admitted_utc", "ended_utc",
    "execution_wall_s", "probe", "parent_job", "evidence", "queue_wait_s",
    "workload"}
# documented as "when the queue is up"; absent in direct mode
QUEUE_ONLY_FIELDS = {"queue_task"}
EVIDENCE_KEYS = {"manifest", "state", "log", "guard_records", "guard_record"}


def _seconds(a: str, b: str) -> float:
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return (dt.datetime.strptime(b, fmt) -
            dt.datetime.strptime(a, fmt)).total_seconds()


def test_submit_json_says_accepted_not_succeeded(env_direct):
    env = env_direct
    job = env.submit(workload_args=["--out", str(env.root / "w.jsonl")],
                     extra=["--request-id", "contract-1", "--slice", "S"])
    assert job["accepted"] is True
    assert job["job_id"].startswith("acs-")
    assert "accepted" in job["meaning"] and "not succeeded" in job["meaning"]
    assert set(job) >= {"accepted", "job_id", "host", "project", "evidence",
                        "notices", "meaning"}
    env.wait_outcome(job["job_id"])


def test_status_view_carries_every_documented_field(env_direct):
    env = env_direct
    job = env.submit(workload_args=["--out", str(env.root / "w.jsonl"),
                                    "--sleep", "1.5"],
                     extra=["--request-id", "contract-2", "--slice", "S"])
    state = env.wait_outcome(job["job_id"])
    assert state["outcome"] == "succeeded", state

    one = env.acs("status", job["job_id"], "--json", check=True)
    view = json.loads(one.stdout)["job"]
    missing = DOCUMENTED_FIELDS - set(view)
    assert not missing, f"documented fields missing from the view: {missing}"
    assert view["job_id"] == job["job_id"]
    assert view["request_id"] == "contract-2"
    assert view["phase"] == "done" and view["outcome"] == "succeeded"
    assert view["guard_outcome"] == "completed" and view["exit_code"] == 0
    assert view["cleanup"]["clean"] is True
    assert set(view["cleanup"]) == {"verified", "clean", "method", "reason"}

    # the two durations: queue wait = accepted -> admitted, execution =
    # the guard's measured wall time
    assert view["queue_wait_s"] == _seconds(view["accepted_utc"],
                                            view["admitted_utc"])
    assert view["execution_wall_s"] == state["wall_s"]
    assert view["execution_wall_s"] >= 1

    # evidence paths point at files that exist
    assert set(view["evidence"]) >= EVIDENCE_KEYS
    for key in ("manifest", "state", "log", "guard_record"):
        assert Path(view["evidence"][key]).is_file(), key
    assert Path(view["evidence"]["guard_records"]).is_dir()

    # the same view under `status --all --json` and `wait --json`
    all_views = json.loads(env.acs("status", "--all", "--json",
                                   check=True).stdout)["jobs"]
    mine = [v for v in all_views if v["job_id"] == job["job_id"]]
    assert len(mine) == 1 and DOCUMENTED_FIELDS <= set(mine[0])
    waited = json.loads(env.acs("wait", job["job_id"], "--json",
                                "--timeout", "5", check=True).stdout)
    assert DOCUMENTED_FIELDS <= set(waited)
    assert waited["outcome"] == "succeeded"
    assert QUEUE_ONLY_FIELDS.isdisjoint(view) or view.get("queue_task")


def test_never_launched_job_keeps_the_field_set_with_nulls(env_direct):
    env = env_direct
    result = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                     "--reserve-gb", "99", "--cwd", str(env.projects["alpha"]),
                     "--", "python3", str(WORKLOAD), "--out",
                     str(env.root / "never"))
    job_id = json.loads(result.stdout)["job_id"]
    env.wait_outcome(job_id)
    view = json.loads(env.acs("status", job_id, "--json",
                              check=True).stdout)["job"]
    assert view["outcome"] == "refused"
    assert DOCUMENTED_FIELDS - {"workload", "admitted_utc"} <= set(view)
    for key in ("guard_outcome", "exit_code", "peak_rss_gb", "cleanup",
                "admitted_utc"):
        assert view.get(key) is None, (key, view.get(key))
    # a refused job still reports its queue wait (accepted -> ended)
    assert view["queue_wait_s"] == _seconds(view["accepted_utc"],
                                            view["ended_utc"])


def test_the_v1_key_set_is_frozen_for_a_finished_job(env_direct):
    env = env_direct
    job = env.submit(workload_args=["--out", str(env.root / "w.jsonl")])
    env.wait_outcome(job["job_id"])
    report = json.loads(env.acs("status", job["job_id"], "--json",
                                check=True).stdout)
    for view in (report["job"], report):
        assert FROZEN_V1_KEYS <= set(view), FROZEN_V1_KEYS - set(view)


def test_a_queued_job_view_carries_the_frozen_keys(env_queue):
    env = env_queue
    first = env.submit(case="a", workload_args=[
        "--out", str(env.root / "a.jsonl"), "--sleep", "30"])["job_id"]
    env.wait_phase(first, "executing", timeout=60)
    second = env.submit(case="b", workload_args=[
        "--out", str(env.root / "b.jsonl")])["job_id"]
    try:
        report = json.loads(env.acs("status", second, "--json",
                                    check=True).stdout)
        view = report["job"]
        assert view["phase"] == "queued"
        assert FROZEN_V1_KEYS <= set(view), FROZEN_V1_KEYS - set(view)
        assert view["queue_wait_s"] is None and view["workload"] is None
        assert view["outcome"] is None and view["admitted_utc"] is None
        assert view["queue_task"]["status"] == "Queued"
        listed = json.loads(env.acs("status", "--json",
                                    check=True).stdout)["jobs"]
        mine = [v for v in listed if v["job_id"] == second]
        assert mine and FROZEN_V1_KEYS <= set(mine[0])
    finally:
        env.acs("cancel", second)
        env.acs("cancel", first)
