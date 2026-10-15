"""Campaign G slice G2a: the per-host event log (decision D11).

What this establishes: every lifecycle transition that ACS writes into a
job's ``state.json`` (accepted, queued, dispatched, the phases, the
outcome, a reconcile) also appends one JSON line to
``<state>/events/<host>.jsonl`` with the job id, request id, host, host
id, version, phase, outcome, reason, UTC time, the host's monotonic clock
and boot id, and the writer (runner or CLI, with its pid); history entries
carry the writer and the monotonic reading too; ``doctor`` reports the
log; and the log is derived data: a damaged log changes no decision.
"""

from __future__ import annotations

import json
import sys

from conftest import WORKLOAD, Env

from agent_compute.state import JobStore

PY = sys.executable
FIELDS = {"schema", "utc", "monotonic_s", "boot_id", "job_id", "request_id",
          "host", "host_id", "version", "source_sha", "event", "phase",
          "outcome", "reason", "actor"}


def lines(env: Env) -> list[dict]:
    path = env.state / "events" / "testhost.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()
            if line.strip()]


def submit(env: Env, case: str, *extra) -> str:
    result = env.acs("submit", "--json", "--case", case, "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--cwd",
                     str(env.projects["alpha"]), *extra, "--", PY,
                     str(WORKLOAD), "--out", str(env.root / f"{case}.out"))
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)["job_id"]


def test_every_transition_is_one_event_line(env_any):
    env = env_any
    job = submit(env, "e", "--request-id", "ev-1")
    state = env.wait_outcome(job)
    assert state["outcome"] == "succeeded", state
    mine = [e for e in lines(env) if e["job_id"] == job]
    for event in mine:
        assert FIELDS <= set(event), FIELDS - set(event)
        assert event["schema"] == "agent-compute-event/1"
        assert event["host"] == "testhost" and event["request_id"] == "ev-1"
        assert isinstance(event["monotonic_s"], float)
        assert event["actor"].split(":")[0] in ("cli", "runner",
                                                "reconcile")
    phases = [e["phase"] for e in mine if e["event"] == "phase"]
    # (a fast runner may claim the job before `queued` is recorded)
    expected = ["accepted", "dispatched", "admission-wait", "executing",
                "cleanup", "done"]
    assert [p for p in phases if p in expected] == expected, phases
    outcomes = [e for e in mine if e["event"] == "outcome"]
    assert len(outcomes) == 1 and outcomes[0]["outcome"] == "succeeded"
    host_id = json.loads((env.state / "host-id.json").read_text())["host_id"]
    assert {e["host_id"] for e in mine} == {host_id}
    # the same transitions, in order, as the state's history
    history = [h.get("phase") or h.get("outcome") for h in state["history"]]
    logged = [e["phase"] if e["event"] == "phase" else e["outcome"]
              for e in mine]
    assert logged == history, (logged, history)
    for item in state["history"]:
        assert "actor" in item and "monotonic_s" in item, item
    # monotonic order within one boot
    assert [e["monotonic_s"] for e in mine] == \
        sorted(e["monotonic_s"] for e in mine)


def test_reconcile_and_never_started_ends_are_logged(env_direct):
    env = env_direct
    store = JobStore(env.state)
    from test_acs_guard_backend import prepare_job
    from agent_compute import config as cfgmod
    job = prepare_job(env, cfgmod.load(env.config))
    from agent_compute import cli
    assert cli._finish_if_unclaimed(store, job, "refused", "test; never "
                                    "started")
    ended = [e for e in lines(env) if e["job_id"] == job and
             e["event"] == "outcome"]
    assert ended and ended[0]["outcome"] == "refused"
    store.set_block("testhost", job, "test block", {})
    result = env.acs("reconcile", "--json")
    assert result.returncode == 0, result.stdout
    reconciled = [e for e in lines(env) if e["job_id"] == job and
                  e["event"] == "reconciled"]
    assert reconciled and reconciled[0]["actor"].startswith("reconcile")


def test_doctor_reports_the_event_log(env_direct):
    env = env_direct
    job = submit(env, "d")
    env.wait_outcome(job)
    doctor = json.loads(env.acs("doctor", "--json").stdout)
    check = [c for c in doctor["checks"] if c["check"] == "event log"]
    assert check and check[0]["ok"] is True
    detail = check[0]["detail"]
    assert detail["path"] == str(env.state / "events" / "testhost.jsonl")
    assert detail["lines"] == len(lines(env))
    assert detail["last_utc"]
    assert "never read for a decision" in detail["note"]


def test_a_damaged_log_changes_no_decision(env_direct):
    env = env_direct
    path = env.state / "events" / "testhost.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not json\n\x00\x01garbage")
    job = submit(env, "g")
    assert env.wait_outcome(job)["outcome"] == "succeeded"
    text = path.read_text()
    assert text.startswith("{not json\n\x00\x01garbage")
    assert job in text
    # unwritable: still no effect on the job
    path.unlink()
    path.mkdir()
    job = submit(env, "h")
    assert env.wait_outcome(job)["outcome"] == "succeeded"


# --- review round 1 ------------------------------------------------------------------------

ELAPSED = ("queue_wait_s", "admission_wait_s", "execution_wall_s",
           "cleanup_s")


def test_the_runner_stores_its_elapsed_values(env_direct):
    """Review round 1, item 2 (§6): at finish the runner stores the elapsed
    values it measured with its own clocks."""
    env = env_direct
    result = env.acs("submit", "--json", "--case", "w", "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--cwd",
                     str(env.projects["alpha"]), "--", PY, str(WORKLOAD),
                     "--out", str(env.root / "w.out"), "--sleep", "1.5")
    job = json.loads(result.stdout)["job_id"]
    state = env.wait_outcome(job)
    assert state["outcome"] == "succeeded", state
    for key in ELAPSED:
        assert isinstance(state.get(key), float), (key, state.get(key))
        assert state[key] >= 0, (key, state[key])
    assert state["execution_wall_s"] >= 1.4
    assert state["queue_wait_s"] >= state["admission_wait_s"] - 1.0
    assert state["elapsed_clock"] == "runner"
    # a job refused before any launch: no execution, no cleanup
    refused = env.acs("submit", "--json", "--case", "r", "--timeout-min",
                      "1", "--reserve-gb", "99", "--cwd",
                      str(env.projects["alpha"]), "--", PY, "-c", "pass")
    job = json.loads(refused.stdout)["job_id"]
    state = env.wait_outcome(job)
    assert state["outcome"] == "refused", state
    assert state["execution_wall_s"] is None and state["cleanup_s"] is None
    assert isinstance(state["queue_wait_s"], float)
    assert isinstance(state["admission_wait_s"], float)


def test_events_run_no_subprocess_under_the_state_lock(tmp_path,
                                                       monkeypatch):
    """Review round 1, item 6: the per-process identity (boot id: sysctl on
    macOS; a checkout's source sha: git) is computed outside the state
    lock; nothing the event log does under the lock spawns a process."""
    import subprocess as sp
    from agent_compute import identity
    from agent_compute import state as statemod
    held = []
    violations = []
    real_lock = statemod.state_lock

    import contextlib

    @contextlib.contextmanager
    def watched(state_dir):
        with real_lock(state_dir):
            held.append(1)
            try:
                yield
            finally:
                held.pop()
    real_run = sp.run

    def run(*args, **kwargs):
        if held:
            violations.append(args[0] if args else kwargs.get("args"))
        return real_run(*args, **kwargs)
    monkeypatch.setattr(statemod, "state_lock", watched)
    monkeypatch.setattr(statemod.subprocess, "run", run)
    monkeypatch.setattr(identity.subprocess, "run", run)
    monkeypatch.setattr(statemod, "_BOOT_ID", [])
    monkeypatch.setattr(identity, "_CACHE", {})
    state_dir = tmp_path / "s"
    statemod.ensure_host_id(state_dir, "h1")
    store = statemod.JobStore(state_dir)
    job = "acs-20261006T000000Z-abcdef"
    statemod.write_json(store.job_dir(job) / "manifest.json",
                        {"job_id": job, "host": "h1"})
    store.update_state(job, phase="queued", reason="test")
    store.update_state(job, phase="done", outcome="refused", reason="test")
    assert not violations, violations
    lines = statemod.events_path(state_dir, "h1").read_text().splitlines()
    assert len(lines) == 3          # queued, done, refused
    assert all(json.loads(line)["version"] for line in lines)


def test_an_event_after_a_damaged_last_line_starts_its_own_line(tmp_path):
    from agent_compute import state as statemod
    path = statemod.events_path(tmp_path, "h1")
    path.parent.mkdir(parents=True)
    path.write_text('{"half": ')
    assert statemod.append_event(tmp_path, "h1", {"event": "x"})
    lines = path.read_text().splitlines()
    assert lines[0] == '{"half": ' and json.loads(lines[1]) == {"event": "x"}
