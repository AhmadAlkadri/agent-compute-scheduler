"""Campaign G slice G2a: status visibility (open question Q11, finding F18,
the executor corpus §4.2).

What this establishes:

* the job view always carries ``queue_wait_s`` and ``workload`` (null
  until known) and ``dispatched_utc``; the v1 key set is frozen (every key
  v0.1.2 emitted stays), also for a queued job;
* a queued job's view names the job ahead of it (project, timeout,
  elapsed) and its queue position;
* ``wait_reason`` is cleared when the wait ends, and "the guard launched
  the workload; identity not yet captured" is shown (phase ``executing``)
  between the launch and the identity capture;
* ``status`` shows why dispatch is held (the gate's reason, a paused
  group), and ``doctor``'s dispatch check agrees; doctor's labels read
  PASS/FAIL;
* ``status JOB --json`` and ``wait --json`` share one shape (the view at
  the top level, ``admission`` under its own key);
* ``cancel --json`` on a finished job returns the job view;
* ``held-inputs-changed`` prints its reason in ``wait``, human ``status``
  and in a ``submit`` that reuses the held job's request id;
* request ids are validated, and the job id is the last line of every
  human ``submit`` result;
* ``status --all`` takes ``--unfinished`` and ``--mine``;
* ``wait`` honours ``--timeout`` when the queue daemon does not answer.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

import pytest

from conftest import PUEUE, WORKLOAD, Env

from agent_compute import cli
from agent_compute import config as cfgmod
from agent_compute import queue as q
from agent_compute import runner as runmod
from agent_compute.state import JobStore, write_json

PY = sys.executable

from test_status_view_contract import FROZEN_V1_KEYS as FROZEN_V1  # noqa: E402

ADDED_V2 = {"dispatched_utc", "queue_position", "job_ahead", "acs_version",
            "acs_source_sha", "submitter", "dispatch_wait_s",
            "admission_wait_s"}


def view_of(env: Env, job: str) -> dict:
    result = env.acs("status", job, "--json")
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def sleeper(env: Env, case: str, seconds: float, *extra) -> str:
    result = env.acs("submit", "--json", "--case", case, "--timeout-min", "2",
                     "--reserve-gb", "0.5", "--cwd",
                     str(env.projects["alpha"]), *extra, "--", PY,
                     str(WORKLOAD), "--out", str(env.root / f"{case}.out"),
                     "--sleep", str(seconds))
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)["job_id"]


# --- the view and its shape ----------------------------------------------------------

def test_a_finished_view_keeps_the_frozen_keys_and_clears_wait_reason(
        env_direct):
    env = env_direct
    job = sleeper(env, "f", 0.5)
    state = env.wait_outcome(job)
    assert state["outcome"] == "succeeded", state
    assert state.get("wait_reason") is None, state
    report = view_of(env, job)
    assert FROZEN_V1 | ADDED_V2 <= set(report), \
        (FROZEN_V1 | ADDED_V2) - set(report)
    assert report["wait_reason"] is None
    assert report["dispatched_utc"] and report["queue_wait_s"] is not None
    assert report["queue_position"] is None and report["job_ahead"] is None
    # one shape: the view at the top level, admission apart, the legacy
    # "job" key kept
    assert report["job"]["job_id"] == report["job_id"] == job
    assert "dispatch" in report["admission"]
    waited = json.loads(env.acs("wait", job, "--json", "--timeout",
                                "5").stdout)
    assert set(waited) == set(report)
    assert {k: waited[k] for k in FROZEN_V1} == \
        {k: report[k] for k in FROZEN_V1}


def test_a_queued_view_names_the_job_ahead_and_its_position(env_queue):
    env = env_queue
    first = sleeper(env, "a", 30)
    env.wait_phase(first, "executing", timeout=60)
    second = sleeper(env, "b", 1, "--request-id", "pos-b")
    third = sleeper(env, "c", 1)
    try:
        b, c = view_of(env, second), view_of(env, third)
        for view in (b, c):
            assert FROZEN_V1 <= set(view), FROZEN_V1 - set(view)
            assert view["phase"] == "queued"
            assert view["queue_wait_s"] is None
            assert view["workload"] is None
            assert view["dispatched_utc"] is None
        assert b["queue_position"] == 1 and c["queue_position"] == 2
        assert b["job_ahead"]["id"] == first
        assert b["job_ahead"]["project"] == "alpha"
        assert b["job_ahead"]["timeout_min"] == 2.0
        assert b["job_ahead"]["elapsed_s"] >= 0
        assert c["job_ahead"]["id"] == second
        assert "job_id" not in b["job_ahead"]       # never a second job_id
    finally:
        for job in (third, second, first):
            env.acs("cancel", job)


def test_the_view_of_a_job_in_admission_wait_has_its_durations(env_direct):
    env = env_direct
    holder = sleeper(env, "h", 20)
    env.wait_phase(holder, "executing", timeout=60)
    waiting = sleeper(env, "w", 1)
    try:
        env.wait_phase(waiting, "admission-wait", timeout=60)
        view = view_of(env, waiting)
        assert view["queue_wait_s"] is None and view["workload"] is None
        assert view["dispatched_utc"] is not None
    finally:
        env.acs("cancel", waiting)
        env.acs("cancel", holder)


# --- launched, identity not yet captured ---------------------------------------------

def test_launch_without_an_identity_is_shown_as_launched(env_direct):
    from test_acs_guard_backend import prepare_job
    env = env_direct
    job = prepare_job(env, cfgmod.load(env.config))
    runner = runmod.Runner(cfgmod.load(env.config), job)
    runner.update(phase="admission-wait", wait_reason="guard pre-flight",
                  reason="guard pre-flight")
    attempt = env.state / "jobs" / job / "guard" / "attempt-1"
    attempt.mkdir(parents=True)
    write_json(attempt / "guard.json", {"pid": 999999, "job_id": "alpha.t/x",
                                        "started_utc": "2026-10-06T00:00:00Z"})
    runner.observe_launch(attempt)
    state = env.state_of(job)
    assert state["phase"] == "launched"
    assert "identity not yet captured" in state["wait_reason"]
    assert "workload" not in state
    # the capture clears it
    proc = subprocess.Popen([PY, "-c", "import time; time.sleep(20)"])
    try:
        from agent_compute.state import utc_now
        write_json(attempt / "guard.json", {"pid": proc.pid,
                                            "job_id": "alpha.t/x",
                                            "started_utc": utc_now()})
        runner.capture_workload(attempt)
        state = env.state_of(job)
        assert state["phase"] == "executing"
        assert state["workload"]["pid"] == proc.pid
        assert state.get("wait_reason") is None
    finally:
        proc.kill()


# --- why dispatch is held -------------------------------------------------------------

def test_status_shows_the_gate_reason_and_doctor_agrees(env_direct):
    env = env_direct
    store = JobStore(env.state)
    store.close_gate("operator test: hold everything")
    report = json.loads(env.acs("status", "--json").stdout)
    dispatch = report["admission"]["dispatch"]
    assert dispatch["held"] is True
    assert "operator test: hold everything" in dispatch["reason"]
    assert "resume" in dispatch["reason"]
    doctor = json.loads(env.acs("doctor", "--json").stdout)
    check = [c for c in doctor["checks"] if c["check"] == "dispatch gate open"]
    assert check[0]["ok"] is False
    assert "operator test" in json.dumps(check[0]["detail"])


@pytest.mark.skipif(PUEUE is None, reason="[pueue] pueue not on PATH")
def test_a_paused_group_is_held_dispatch_in_status_and_doctor(env_queue):
    env = env_queue
    assert env.acs("daemon", "start").returncode == 0
    pueue_cfg = env.state / "pueue" / "pueue.yml"
    subprocess.run([PUEUE, "-c", str(pueue_cfg), "pause", "-g", "acs-heavy",
                    "--wait"], check=True, capture_output=True, timeout=60)
    report = json.loads(env.acs("status", "--json").stdout)
    assert report["admission"]["dispatch_gate"] is None
    dispatch = report["admission"]["dispatch"]
    assert dispatch["held"] is True and "paused" in dispatch["reason"]
    assert "agent-compute resume" in dispatch["reason"]
    doctor = json.loads(env.acs("doctor", "--json").stdout)
    check = [c for c in doctor["checks"] if c["check"] == "dispatch gate open"]
    assert check[0]["ok"] is False
    assert "paused" in json.dumps(check[0]["detail"])
    human = env.acs("doctor").stdout
    assert "[FAIL] dispatch gate open" in human or \
        "[WARN] dispatch gate open" in human
    assert "[PASS] config" in human
    assert "[ok  ]" not in human and "[ERRO]" not in human
    assert env.acs("resume").returncode == 0
    report = json.loads(env.acs("status", "--json").stdout)
    assert report["admission"]["dispatch"]["held"] is False


def test_doctor_json_checks_carry_a_result_word(env_direct):
    doctor = json.loads(env_direct.acs("doctor", "--json").stdout)
    for check in doctor["checks"]:
        assert check["result"] in ("pass", "fail", "warn", "info"), check
        assert (check["result"] == "pass") == bool(check["ok"]), check


# --- cancel on a finished job -----------------------------------------------------------

def test_cancel_json_on_a_finished_job_returns_its_view(env_direct):
    env = env_direct
    job = sleeper(env, "x", 0.2)
    env.wait_outcome(job)
    result = env.acs("cancel", job, "--json")
    assert result.returncode == 1
    out = json.loads(result.stdout)
    assert out["cancelled"] is False
    assert out["job_id"] == job and out["outcome"] == "succeeded"
    assert FROZEN_V1 <= set(out)
    assert "already succeeded" in out["note"]


# --- held-inputs-changed reasons -------------------------------------------------------------

def _held(env: Env, request_id: str | None = None) -> str:
    from test_acs_guard_backend import prepare_job
    job = prepare_job(env, cfgmod.load(env.config))
    store = JobStore(env.state)
    if request_id:
        store.bind_request(request_id, job)
        man_path = env.state / "jobs" / job / "manifest.json"
        man = json.loads(man_path.read_text())
        man["request_id"] = request_id
        write_json(man_path, man)
    store.update_state(job, phase="done", outcome="held-inputs-changed",
                       reason="inputs changed while queued (diff_sha256); "
                              "resubmit against the new inputs; never "
                              "started")
    return job


def test_held_reasons_are_printed(env_direct):
    env = env_direct
    job = _held(env, "held-1")
    waited = env.acs("wait", job)
    assert waited.returncode == runmod.RC["held-inputs-changed"]
    first = waited.stdout.splitlines()[0]
    assert "held-inputs-changed" in first and "diff_sha256" in first
    status = env.acs("status", "--all").stdout
    assert any("held-inputs-changed" in line and "diff_sha256" in line
               for line in status.splitlines())
    result = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--request-id", "held-1",
                     "--cwd", str(env.projects["alpha"]), "--", PY, "-c",
                     "pass")
    assert result.returncode == 0, result.stdout + result.stderr
    notices = " ".join(json.loads(result.stdout)["notices"])
    assert job in notices and "diff_sha256" in notices
    env.wait_outcome(json.loads(result.stdout)["job_id"])


# --- request ids and the submit result -------------------------------------------------------

@pytest.mark.parametrize("request_id", ["two words", "tab\there", "a/b",
                                        "", " lead", "x" * 129, "ü-umlaut",
                                        ".hidden"])
def test_bad_request_ids_are_refused(env_direct, request_id):
    env = env_direct
    result = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--request-id", request_id,
                     "--cwd", str(env.projects["alpha"]), "--", PY, "-c",
                     "pass")
    assert result.returncode == 2
    assert "request id" in (result.stdout + result.stderr)
    assert not (env.state / "jobs").exists() or \
        not list((env.state / "jobs").glob("acs-*"))


def test_good_request_ids_and_the_job_id_last(env_direct):
    env = env_direct
    result = env.acs("submit", "--case", "t", "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--request-id",
                     "nx1-blocks:L7.b_2", "--cwd",
                     str(env.projects["alpha"]), "--", PY, "-c", "pass")
    assert result.returncode == 0, result.stderr
    job = result.stdout.strip().splitlines()[-1]
    assert job.startswith("acs-") and " " not in job
    env.wait_outcome(job)
    again = env.acs("submit", "--case", "t", "--timeout-min", "1",
                    "--reserve-gb", "0.5", "--request-id",
                    "nx1-blocks:L7.b_2", "--cwd",
                    str(env.projects["alpha"]), "--", PY, "-c", "pass")
    assert again.returncode == 2
    assert again.stdout.strip().splitlines()[-1] == job


# --- status filters -----------------------------------------------------------------------

def test_status_all_unfinished_and_mine(env_direct):
    env = env_direct
    done = sleeper(env, "d", 0.1, "--owner", "alice")
    env.wait_outcome(done)
    running = sleeper(env, "r", 20, "--request-id", "bob-run-1")
    try:
        env.wait_phase(running, "executing", timeout=60)

        def ids(*args):
            out = json.loads(env.acs("status", "--all", "--json",
                                     *args).stdout)
            return {j["job_id"] for j in out["jobs"]}
        assert ids() == {done, running}
        assert ids("--unfinished") == {running}
        assert ids("--mine", "alice") == {done}
        assert ids("--mine", "bob-run") == {running}
        assert ids("--mine", "nobody") == set()
        assert ids("--unfinished", "--mine", "alice") == set()
    finally:
        env.acs("cancel", running)


# --- wait honours its timeout -------------------------------------------------------------

def test_wait_honours_its_timeout_when_the_queue_does_not_answer(
        env_queue, monkeypatch, capsys):
    env = env_queue
    config = cfgmod.load(env.config)
    from test_acs_guard_backend import prepare_job
    job = prepare_job(env, config)
    JobStore(env.state).update_state(job, phase="queued", pueue_task=7)
    sock = q.socket_path(config)
    sock.parent.mkdir(parents=True, exist_ok=True)
    sock.write_text("")             # looks like a daemon socket

    def hung_pueue(config, *args, check=True, timeout=60):
        time.sleep(min(float(timeout), 30.0))
        raise q.QueueError("pueue did not answer")
    monkeypatch.setattr(q, "pueue", hung_pueue)
    started = time.monotonic()
    rc = cli.cmd_wait(config, argparse.Namespace(job=job, timeout=2.0,
                                                 json=True))
    elapsed = time.monotonic() - started
    assert rc == cli.EXIT_WAIT_TIMEOUT
    assert elapsed < 2.0 + 3.0, elapsed
    out = json.loads(capsys.readouterr().out)
    assert out["note"] == "wait timed out; the job continues"


# --- review round 1 ------------------------------------------------------------------------

def test_a_legacy_binding_for_another_id_is_refused_naming_both(env_direct):
    """Review round 1, item 3 (A32): a binding written by v0.1.2 for the id
    `a b` sits at requests/a_b.json; a request for the (valid) id `a_b`
    must not be answered with that other job: refused, both ids named."""
    from agent_compute.state import private_dir
    env = env_direct
    other = "acs-20261005T120000Z-0a0b0c"
    private_dir(env.state / "requests")
    write_json(env.state / "requests" / "a_b.json",
               {"request_id": "a b", "job_id": other,
                "utc": "2026-10-05T12:00:00Z"})
    result = env.acs("submit", "--case", "t", "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--request-id", "a_b", "--cwd",
                     str(env.projects["alpha"]), "--", PY, "-c", "pass")
    assert result.returncode == 2
    assert "'a b'" in result.stdout and "'a_b'" in result.stdout
    assert result.stdout.strip().splitlines()[-1] != other
    jobs = env.state / "jobs"
    assert not jobs.exists() or not list(jobs.glob("acs-*"))
    assert json.loads((env.state / "requests" / "a_b.json").read_text())[
        "request_id"] == "a b"                      # never replaced


@pytest.mark.parametrize("label", ["two words", "x" * 129, "line\nbreak",
                                   "tab\there", ""])
def test_bad_submitters_are_refused(env_direct, label):
    """Review round 1, item 5: --submitter (and AGENT_COMPUTE_SUBMITTER) is
    screened like a request id."""
    env = env_direct
    base = ["submit", "--json", "--case", "t", "--timeout-min", "1",
            "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"])]
    for args, environ in (([*base, "--submitter", label], None),
                          (base, env.env(AGENT_COMPUTE_SUBMITTER=label))):
        if environ is not None and label == "":
            continue                      # an empty variable means unset
        result = env.acs(*args, "--", PY, "-c", "pass", env=environ)
        assert result.returncode == 2, (label, result.stdout)
        assert "submitter" in result.stdout + result.stderr
    jobs = env.state / "jobs"
    assert not jobs.exists() or not list(jobs.glob("acs-*"))


def test_a_good_submitter_is_recorded(env_direct):
    env = env_direct
    result = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--submitter", "session-ab:12.x",
                     "--cwd", str(env.projects["alpha"]), "--", PY, "-c",
                     "pass")
    assert result.returncode == 0, result.stdout + result.stderr
    job = json.loads(result.stdout)["job_id"]
    env.wait_outcome(job)
    assert view_of(env, job)["submitter"] == "session-ab:12.x"
