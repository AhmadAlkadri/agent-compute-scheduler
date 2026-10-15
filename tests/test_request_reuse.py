"""Campaign PA slice K2 (register A24): a request id is free again after a
cancel only with positive never-started evidence.

``_request_reusable`` freed the id of any ``cancelled`` job with no
``workload`` field.  A job cancelled after the guard began launching but
before the runner captured its workload identity (a container job: the
in-container pid not yet read; a host job: the window between the guard's
launch and the first ``capture_workload`` poll) has no ``workload``, yet may
have run: the same request id was then admitted a second time.

These tests build the state a cancelled job leaves (through the real
``accept`` path: reservation, binding, state) and resubmit the request id.
The container cases are state fixtures; no Docker is needed.
"""

import json
import os
from pathlib import Path

import pytest

from conftest import skip_reason

from test_unclaimed_jobs import submit_args, workload

from agent_compute import cli
from agent_compute.config import load
from agent_compute.state import NEVER_STARTED, JobStore

RUNNER = {"pid": 999999, "start": "ticks:1", "note": "fake claim (K2)"}

needs_non_root = pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason=skip_reason("root", "root ignores a mode-000 path"))


@pytest.fixture
def fx(env_direct, monkeypatch):
    """(env, config, store, bind): ``bind(request_id)`` accepts a first
    submission (no runner is spawned) and returns its job id."""
    env = env_direct
    config = load(env.config)
    store = JobStore(config.state_dir)
    monkeypatch.setattr(cli, "spawn_detached_runner", lambda *a: None)

    def bind(request_id):
        code, result = cli.accept(config, submit_args(env, "--request-id",
                                                      request_id),
                                  workload(env, "k2.jsonl"))
        assert code == 0 and result["accepted"] is True, result
        return result["job_id"]
    return env, config, store, bind


def resubmit(env, config, request_id):
    return cli.accept(config, submit_args(env, "--request-id", request_id),
                      workload(env, "k2-again.jsonl"))


def set_venue(store, job, venue):
    """The manifest of a container job names its container."""
    path = store.job_dir(job) / "manifest.json"
    man = json.loads(path.read_text())
    if venue == "container":
        man["container"] = {"name": "k2-ctr", "id": "sha256:" + "0" * 64,
                            "workdir": "/work"}
    else:
        man.pop("container", None)
    path.write_text(json.dumps(man))


def end_cancelled(store, job, *, launched=False, executing=False,
                  claimed=True, **extra):
    """What the runner leaves of a cancelled job: claimed, optionally
    ``launched`` / ``executing`` in the history, then ``cancelled``."""
    if claimed:
        store.update_state(job, phase="dispatched", runner=RUNNER,
                           reason="runner started")
        store.update_state(job, phase="admission-wait", reason="waiting")
    if launched:
        store.update_state(job, phase="launched", reason="launched")
    if executing:
        store.update_state(job, phase="executing", reason="executing")
    return store.update_state(job, phase="done", outcome="cancelled",
                              reason="cancelled by request", **extra)


def attempt(store, job, n=1):
    path = store.job_dir(job) / "guard" / f"attempt-{n}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def put_record(store, job, record, n=1):
    path = attempt(store, job, n) / "guard.json"
    path.write_text(record if isinstance(record, str)
                    else json.dumps(record))
    return path


def assert_kept(fx, request_id, job):
    env, config, store, _ = fx
    code, result = resubmit(env, config, request_id)
    assert code == cli.EXIT_NOT_ACCEPTED, result
    assert result["accepted"] is False, result
    assert result.get("job_id") == job, result
    assert store.request_job(request_id) == job
    assert store.all_ids() == [job]            # no second job was created
    # the same verdict where a request id is sent to another host (A39)
    assert cli.host_binding_refusal(config, request_id), \
        "the id would be sent to another host"


def assert_freed(fx, request_id, job):
    env, config, store, _ = fx
    # the same verdict where a request id is sent to another host (A39)
    assert cli.host_binding_refusal(config, request_id) is None
    code, result = resubmit(env, config, request_id)
    assert code == 0 and result["accepted"] is True, result
    assert result["job_id"] != job
    assert store.request_job(request_id) == result["job_id"]


# --- a launched, cancelled job keeps its request id (the defect) -----------

def _guard_outcome_no_pid(store, job):
    put_record(store, job, {"outcome": "killed-interrupted", "pid": None,
                            "container_pid": None})


def _guard_outcome_completed(store, job):
    put_record(store, job, {"outcome": "completed", "exit_code": 0})


def _guard_outcome_running(store, job):
    put_record(store, job, {"outcome": "running", "pid": None})


def _guard_pid_only(store, job):
    # container venue: the client pid is recorded, the in-container pid is
    # still being resolved (the window before `capture_workload` records)
    put_record(store, job, {"outcome": None, "pid": 4242,
                            "container_pid": None, "venue": "container"})


def _guard_container_pid_only(store, job):
    put_record(store, job, {"outcome": None, "pid": None,
                            "container_pid": 77})


def _samples_only(store, job):
    (attempt(store, job) / "samples.csv").write_text("t,rss\n")


def _samples_in_second_attempt(store, job):
    put_record(store, job, {"outcome": "refused", "pid": None})
    (attempt(store, job, 2) / "samples.csv").write_text("t,rss\n")


def _pid_in_second_attempt(store, job):
    put_record(store, job, {"outcome": "refused", "pid": None})
    put_record(store, job, {"outcome": None, "pid": 4243}, n=2)


def _host_window(store, job):
    # host venue: the guard launched (pid, start) and the cancel came before
    # the runner's first capture_workload poll wrote `workload`
    put_record(store, job, {"outcome": "running", "pid": 4244,
                            "started_utc": "2026-10-10T00:00:00Z",
                            "venue": "host"})


def _host_window_samples(store, job):
    # between samples.csv (opened before the launch) and the first pid
    put_record(store, job, {"outcome": "running", "pid": None})
    (attempt(store, job) / "samples.csv").write_text("t,rss\n")


KEPT_BY_RECORDS = [
    ("container", _guard_outcome_no_pid),
    ("container", _guard_outcome_completed),
    ("container", _guard_outcome_running),
    ("container", _guard_pid_only),
    ("container", _guard_container_pid_only),
    ("container", _samples_only),
    ("container", _samples_in_second_attempt),
    ("container", _pid_in_second_attempt),
    ("host", _host_window),
    ("host", _host_window_samples),
    ("host", _guard_outcome_completed),
    ("host", _samples_only),
]


@pytest.mark.parametrize("venue,fixture", KEPT_BY_RECORDS,
                         ids=[f"{v}-{f.__name__.strip('_')}"
                              for v, f in KEPT_BY_RECORDS])
def test_cancelled_job_with_launch_records_but_no_workload_keeps_its_id(
        fx, venue, fixture):
    _, _, store, bind = fx
    job = bind("k2-records")
    set_venue(store, job, venue)
    end_cancelled(store, job)
    fixture(store, job)
    assert "workload" not in store.state(job)
    assert_kept(fx, "k2-records", job)


@pytest.mark.parametrize("changes", [
    {"guard_outcome": "killed-interrupted"},
    {"guard_record": "/somewhere/attempt-1/guard.json"},
    {"cleanup": {"verified": True, "clean": True}},
    {"exit_code": 143},
    {"admitted_utc": "2026-10-10T00:00:00Z"},
    {"execution_wall_s": 1.5},
    {"cleanup_s": 0.2},
    {"container_name_check": {"match": True}},
    {"workload_seen_utc": "2026-10-10T00:00:00Z"},
], ids=lambda c: next(iter(c)))
def test_cancelled_job_whose_state_shows_a_launch_keeps_its_id(fx, changes):
    _, _, store, bind = fx
    job = bind("k2-state")
    set_venue(store, job, "container")
    end_cancelled(store, job, **changes)
    assert_kept(fx, "k2-state", job)


@pytest.mark.parametrize("kw", [{"launched": True}, {"executing": True}],
                         ids=["phase-launched", "phase-executing"])
def test_cancelled_job_that_reached_launched_keeps_its_id(fx, kw):
    """The history shows the runner saw the launch, whatever the files."""
    _, _, store, bind = fx
    job = bind("k2-phase")
    set_venue(store, job, "container")
    end_cancelled(store, job, **kw)
    assert_kept(fx, "k2-phase", job)


def test_cancelled_job_in_cleanup_phase_keeps_its_id(fx):
    _, _, store, bind = fx
    job = bind("k2-cleanup")
    end_cancelled(store, job)
    store.update_state(job, history=store.state(job)["history"] + [
        store.history(phase="cleanup", reason="verifying workload cleanup")])
    assert_kept(fx, "k2-cleanup", job)


def test_cancelled_job_with_a_malformed_history_keeps_its_id(fx):
    _, _, store, bind = fx
    job = bind("k2-history")
    end_cancelled(store, job)
    store.update_state(job, history="not a list")
    assert_kept(fx, "k2-history", job)


# --- an unreadable record is not absence (fail closed) ---------------------

@pytest.mark.parametrize("content", ["{not json", "", "[1, 2]", "\xff\xfe"],
                         ids=["truncated", "empty", "wrong-shape",
                              "not-utf8"])
def test_cancelled_job_with_an_unreadable_guard_record_keeps_its_id(fx,
                                                                    content):
    _, _, store, bind = fx
    job = bind("k2-unreadable")
    end_cancelled(store, job)
    path = attempt(store, job) / "guard.json"
    if content == "\xff\xfe":
        path.write_bytes(b"\xff\xfe\x00")
    else:
        path.write_text(content)
    assert_kept(fx, "k2-unreadable", job)


def test_cancelled_job_with_a_directory_for_its_guard_record_keeps_its_id(fx):
    _, _, store, bind = fx
    job = bind("k2-dir")
    end_cancelled(store, job)
    (attempt(store, job) / "guard.json").mkdir()
    assert_kept(fx, "k2-dir", job)


def test_cancelled_job_with_an_unreadable_record_in_a_later_attempt_keeps_its_id(fx):
    _, _, store, bind = fx
    job = bind("k2-later")
    end_cancelled(store, job)
    put_record(store, job, {"outcome": "refused", "pid": None})
    put_record(store, job, "{broken", n=2)
    assert_kept(fx, "k2-later", job)


@needs_non_root
def test_cancelled_job_with_an_unreadable_guard_directory_keeps_its_id(fx):
    _, _, store, bind = fx
    job = bind("k2-guard-dir")
    end_cancelled(store, job)
    put_record(store, job, {"outcome": "refused", "pid": None})
    guard = store.job_dir(job) / "guard"
    os.chmod(guard, 0)
    try:
        assert_kept(fx, "k2-guard-dir", job)
    finally:
        os.chmod(guard, 0o755)


@needs_non_root
def test_cancelled_job_with_a_mode_000_guard_record_keeps_its_id(fx):
    _, _, store, bind = fx
    job = bind("k2-mode0")
    end_cancelled(store, job)
    path = put_record(store, job, {"outcome": "refused", "pid": None})
    os.chmod(path, 0)
    try:
        assert_kept(fx, "k2-mode0", job)
    finally:
        os.chmod(path, 0o644)


@needs_non_root
def test_cancelled_job_with_an_unsearchable_attempt_keeps_its_id(fx):
    """samples.csv cannot be shown absent when its directory cannot be
    searched (Python 3.14's ``Path.exists`` would say False)."""
    _, _, store, bind = fx
    job = bind("k2-attempt-dir")
    end_cancelled(store, job)
    adir = attempt(store, job)
    os.chmod(adir, 0)
    try:
        assert_kept(fx, "k2-attempt-dir", job)
    finally:
        os.chmod(adir, 0o755)


def test_cancelled_job_whose_state_cannot_be_read_keeps_its_id(fx):
    _, _, store, bind = fx
    job = bind("k2-state-unreadable")
    end_cancelled(store, job)
    path = store.job_dir(job) / "state.json"
    good = path.read_text()
    path.write_text("{broken")
    env, config, _, _ = fx
    try:
        code, result = resubmit(env, config, "k2-state-unreadable")
        assert code == cli.EXIT_NOT_ACCEPTED and result["accepted"] is False
        assert store.request_job("k2-state-unreadable") == job
    finally:
        path.write_text(good)       # the harness's cleanup reads it


# --- a job with a workload keeps its id (as before) ------------------------

def test_cancelled_job_with_a_workload_keeps_its_id(fx):
    _, _, store, bind = fx
    job = bind("k2-workload")
    # an identity no live process can have: a pid above every platform's
    # pid_max and no start.  The harness teardown signals only proven
    # identities (A60), so a real-looking fake pid must never be used here
    end_cancelled(store, job, workload={"venue": "host", "pid": 4194305})
    assert_kept(fx, "k2-workload", job)


# --- positive never-started evidence frees the id --------------------------

def test_cancelled_before_any_runner_claim_frees_the_id(fx):
    _, _, store, bind = fx
    job = bind("k2-free-unclaimed")
    assert cli._finish_if_unclaimed(store, job, "cancelled",
                                    "cancelled: not yet enqueued; never "
                                    "started")
    assert_freed(fx, "k2-free-unclaimed", job)


def test_cancelled_after_a_claim_with_no_guard_records_frees_the_id(fx):
    _, _, store, bind = fx
    job = bind("k2-free-claimed")
    end_cancelled(store, job)
    assert_freed(fx, "k2-free-claimed", job)


def test_cancelled_with_an_empty_attempt_directory_frees_the_id(fx):
    _, _, store, bind = fx
    job = bind("k2-free-empty")
    end_cancelled(store, job)
    attempt(store, job)
    assert_freed(fx, "k2-free-empty", job)


@pytest.mark.parametrize("outcome", ["refused", "lock-busy", "not-started",
                                     None], ids=str)
def test_cancelled_during_admission_with_only_nonlaunch_records_frees_the_id(
        fx, outcome):
    """Guard records that prove no launch: the guard's refusal and
    lock-busy outcomes, with no pid and no samples.csv, in every attempt."""
    _, _, store, bind = fx
    job = bind("k2-free-admission")
    end_cancelled(store, job)
    put_record(store, job, {"outcome": "refused", "pid": None,
                            "container_pid": None, "refusal": "low memory"})
    put_record(store, job, {"outcome": outcome, "pid": None,
                            "container_pid": None}, n=2)
    assert_freed(fx, "k2-free-admission", job)


@pytest.mark.parametrize("outcome", sorted(NEVER_STARTED))
def test_never_started_outcomes_still_free_the_id(fx, outcome):
    _, _, store, bind = fx
    job = bind("k2-free-never")
    store.update_state(job, phase="done", outcome=outcome,
                       reason="never started")
    assert_freed(fx, "k2-free-never", job)


def test_the_decision_is_the_helpers(fx):
    """Direct: the helper's verdict for the shapes above."""
    _, _, store, bind = fx
    job = bind("k2-helper")
    assert cli._request_reusable(store, job) is False        # not finished
    end_cancelled(store, job)
    assert cli._request_reusable(store, job) is True
    _samples_only(store, job)
    assert cli._request_reusable(store, job) is False
    assert cli._request_reusable(store, "acs-does-not-exist") is False
