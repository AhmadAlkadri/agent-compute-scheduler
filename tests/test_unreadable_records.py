"""C4 (boundary audit; campaign G slice G1): an unreadable control record is
never read as an absent one.

``state.read_json`` returned the same ``None`` for a missing file, invalid
JSON, a truncated or empty file, a directory at the path and an I/O or
permission error.  Every reader of the host block and of the dispatch gate
took ``None`` as "no restriction": with a truncated block ``reconcile``
reported "host unblocked" and ``resume`` reopened dispatch; ``_add_block``
replaced a corrupt block with one holding only the new job; a corrupt
request binding let the same id be accepted again; a corrupt job state was
skipped by ``prior_runners``, ``detect_lost`` and ``claim``.

Each record kind is damaged in each way (truncated JSON, an empty file, a
directory at the path, mode 000, JSON of the wrong shape) and every reader
must fail closed, leaving the damaged file as it was.  A missing file keeps
its old meaning (the ``test_keep_*`` cases).
Basis: the private development record (register item C4).
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import sys
import types

import pytest

from agent_compute import cli, procs
from agent_compute import queue as q
from agent_compute.config import load
from agent_compute.runner import RC, Runner
from agent_compute.state import (JobStore, RecordUnreadable, read_json,
                                 write_json)
from conftest import WORKLOAD

HOST = "testhost"
DAMAGE = ["truncated", "empty", "directory", "mode000", "wrong-shape"]
WRONG_SHAPE = {
    "block": '{"host": "testhost", "jobs": ["acs-20260101T000000Z-aaaaaa"]}',
    "gate": '["closed"]',
    "binding": '{"request_id": "c4-req"}',
    "state": '["executing"]',
}
TRUNCATED = {
    "block": '{"host": "testhost", "jobs": {"acs-2026',
    "gate": '{"closed_utc": "2026-',
    "binding": '{"request_id": "c4-req", "job_id": "acs-2',
    "state": '{"phase": "executing", "runner": {"pid": 4',
}
ROOT = os.geteuid() == 0


class Reached(Exception):
    pass


def damage(path, kind: str, record: str) -> None:
    if kind == "mode000" and ROOT:
        pytest.skip("[root] root reads a mode-000 file")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if kind == "directory":
        path.mkdir()
        (path / "x").write_text("{}")
        return
    path.write_text({"truncated": TRUNCATED[record], "empty": "",
                     "mode000": '{"jobs": {}}',
                     "wrong-shape": WRONG_SHAPE[record]}[kind])
    if kind == "mode000":
        path.chmod(0)


def fingerprint(path):
    st = os.lstat(path)
    return (st.st_mode, st.st_size, st.st_mtime_ns, st.st_ino)


def repair(path) -> None:
    """Remove a damaged file (the harness teardown reads every state)."""
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    elif path.exists() or path.is_symlink():
        path.chmod(0o600)
        path.unlink()


@pytest.fixture
def env(env_direct):
    damaged = []
    env_direct.damaged = damaged
    yield env_direct
    for path in damaged:
        repair(path)


def submit_args(env, *extra):
    return cli.parser().parse_args(
        ["submit", "--json", "--case", "t", "--timeout-min", "1",
         "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]), *extra])


def workload(env):
    return [sys.executable, str(WORKLOAD), "--out", str(env.root / "w.jsonl")]


def block_path(env):
    return env.state / "blocks" / f"{HOST}.json"


def gate_path(env):
    return env.state / "dispatch-gate.json"


def damage_block(env, kind):
    path = block_path(env)
    damage(path, kind, "block")
    env.damaged.append(path)
    return path


def damage_gate(env, kind):
    path = gate_path(env)
    damage(path, kind, "gate")
    env.damaged.append(path)
    return path


def manifest_job(env, job_id: str, **extra) -> None:
    write_json(env.state / "jobs" / job_id / "manifest.json",
               {"host": HOST, "job_id": job_id, "admission_wait_s": 30,
                "project": "alpha", **extra})


def run_in_process(env, job_id: str, monkeypatch) -> int:
    """``Runner.run`` up to the guard: any later step raises ``Reached``."""
    def reached(*_a, **_k):
        raise Reached("the runner went past the admission checks")
    monkeypatch.setattr(Runner, "prior_runners", lambda self: ({}, {}))
    monkeypatch.setattr(Runner, "revalidate", reached)
    monkeypatch.setattr(Runner, "run_guard_attempt", reached)
    runner = Runner(load(env.config), job_id)
    handlers = {s: signal.getsignal(s) for s in (signal.SIGTERM,
                                                 signal.SIGINT)}
    mask = os.umask(0o077)
    os.umask(mask)
    try:
        return runner.run()
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
        os.umask(mask)
        if runner.admission_fd is not None:
            os.close(runner.admission_fd)
        # never leave pytest's own pid as a recorded runner
        state = env.state / "jobs" / job_id / "state.json"
        if isinstance(read_json(state), dict):
            JobStore(env.state).update_state(job_id, runner={
                "pid": None, "note": "in-process test runner"})


def acs_json(env, *args):
    result = env.acs(*args, "--json")
    try:
        return result.returncode, json.loads(result.stdout)
    except ValueError:
        return result.returncode, {"stdout": result.stdout,
                                   "stderr": result.stderr[-2000:]}


# --- read_json ------------------------------------------------------------------

@pytest.mark.parametrize("kind", ["truncated", "empty", "directory", "mode000",
                                  "not-utf8", "json-array"])
def test_c4_read_json_distinguishes_absent_from_unreadable(tmp_path, kind):
    path = tmp_path / "record.json"
    assert read_json(path) is None                       # absent
    if kind == "not-utf8":
        path.write_bytes(b'{"jobs": "\xff\xfe"}')
    elif kind == "json-array":
        path.write_text('["acs-x"]')
    else:
        damage(path, kind, "block")
    try:
        result = read_json(path)
        assert result is not None, "an unreadable record read as absent"
        assert not isinstance(result, dict), result
        assert result, "an unreadable record must read as present"
    finally:
        repair(path)


# --- the host block --------------------------------------------------------------

@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_runner_refuses_with_an_unreadable_block(env, kind, monkeypatch):
    path = damage_block(env, kind)
    before = fingerprint(path)
    manifest_job(env, "acs-20261005T000000Z-c4blk1")
    rc = run_in_process(env, "acs-20261005T000000Z-c4blk1", monkeypatch)
    st = JobStore(env.state).state("acs-20261005T000000Z-c4blk1")
    assert rc == RC["refused"], st
    assert st["outcome"] == "refused" and "unreadable" in st["reason"], st
    assert "never started" in st["reason"]
    assert fingerprint(path) == before


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_reconcile_and_resume_fail_closed_on_an_unreadable_block(env,
                                                                    kind):
    path = damage_block(env, kind)
    before = fingerprint(path)
    rc, report = acs_json(env, "reconcile")
    assert rc == 1, report
    assert report.get("blocked") is True, report
    assert any(str(path) in json.dumps(u) or "unreadable" in json.dumps(u)
               for u in report.get("unresolved", [])), report
    rc, resumed = acs_json(env, "resume")
    assert rc == 1 and resumed.get("resumed") is False, resumed
    assert fingerprint(path) == before


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_doctor_errors_on_an_unreadable_block(env, kind):
    damage_block(env, kind)
    _, report = acs_json(env, "doctor")
    checks = {c["check"]: c for c in report.get("checks", [])}
    assert checks["host admission not blocked"]["ok"] is False, report
    assert report["ok"] is False


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_add_block_never_replaces_an_unreadable_block(env, kind):
    path = damage_block(env, kind)
    before = fingerprint(path)
    store = JobStore(env.state)
    try:
        store.set_block(HOST, "acs-20261005T000000Z-c4new1", "a new block",
                        {})
    except Exception:       # noqa: BLE001 - refusing by raising is fine
        pass
    assert fingerprint(path) == before, "the damaged block was replaced"


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_detect_lost_never_replaces_an_unreadable_block(env, kind,
                                                           monkeypatch):
    path = damage_block(env, kind)
    before = fingerprint(path)
    store = JobStore(env.state)
    lost = "acs-20261005T000000Z-c4lost"
    manifest_job(env, lost)
    store.update_state(lost, phase="executing",
                       runner={"pid": 999998, "start": "ticks:1"})
    monkeypatch.setattr(procs, "liveness", lambda ident: procs.GONE)
    try:
        cli.detect_lost(load(env.config))
    except Exception:       # noqa: BLE001
        pass
    assert fingerprint(path) == before


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_accept_refuses_with_an_unreadable_block(env, kind, monkeypatch):
    damage_block(env, kind)
    monkeypatch.setattr(cli, "spawn_detached_runner", lambda *a: None)
    code, result = cli.accept(load(env.config), submit_args(env),
                              workload(env))
    assert code == cli.EXIT_NOT_ACCEPTED, result
    assert result["accepted"] is False and "unreadable" in result["reason"]
    assert JobStore(env.state).all_ids() == []


def _fake_daemon(monkeypatch):
    calls = []
    monkeypatch.setattr(q, "write_pueue_config", lambda config: None)
    monkeypatch.setattr(q, "daemon_running", lambda config: True)
    monkeypatch.setattr(q, "status_json", lambda config: {
        "groups": {"acs-heavy": {"parallel_tasks": 1}}})
    monkeypatch.setattr(q, "pueue", lambda config, *args, **k: calls.append(
        args) or types.SimpleNamespace(returncode=0, stdout="", stderr=""))
    return calls


@pytest.mark.parametrize("record", ["block", "gate"])
@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_ensure_daemon_pauses_on_an_unreadable_record(env_queue, kind,
                                                         record, monkeypatch):
    env = env_queue
    path = block_path(env) if record == "block" else gate_path(env)
    damage(path, kind, record)
    try:
        calls = _fake_daemon(monkeypatch)
        q.ensure_daemon(load(env.config))
        assert any(args[:1] == ("pause",) for args in calls), calls
    finally:
        repair(path)


# --- the dispatch gate -------------------------------------------------------------

class Held(Exception):
    pass


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_runner_holds_dispatch_on_an_unreadable_gate(env, kind,
                                                        monkeypatch):
    path = damage_gate(env, kind)
    before = fingerprint(path)
    job = "acs-20261005T000000Z-c4gat1"
    manifest_job(env, job)

    def held(self, seconds):
        raise Held()
    monkeypatch.setattr(Runner, "_sleep_cancellable", held)
    with pytest.raises(Held):
        run_in_process(env, job, monkeypatch)
    st = JobStore(env.state).state(job)
    assert st["phase"] == "dispatch-held", st
    assert "unreadable" in st["reason"], st
    assert fingerprint(path) == before


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_resume_refuses_and_doctor_errors_on_an_unreadable_gate(env,
                                                                  kind):
    path = damage_gate(env, kind)
    before = fingerprint(path)
    rc, resumed = acs_json(env, "resume")
    assert rc == 1 and resumed.get("resumed") is False, resumed
    assert fingerprint(path) == before
    _, report = acs_json(env, "doctor")
    errors = [c for c in report.get("checks", [])
              if not c["ok"] and c["level"] == "error"]
    assert any(str(path) in json.dumps(c) for c in errors), report


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_accept_refuses_with_an_unreadable_gate(env, kind, monkeypatch):
    damage_gate(env, kind)
    monkeypatch.setattr(cli, "spawn_detached_runner", lambda *a: None)
    code, result = cli.accept(load(env.config), submit_args(env),
                              workload(env))
    assert code == cli.EXIT_NOT_ACCEPTED, result
    assert result["accepted"] is False and "unreadable" in result["reason"]
    assert JobStore(env.state).all_ids() == []


# --- request bindings ----------------------------------------------------------------

@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_accept_refuses_a_request_id_whose_binding_is_unreadable(
        env, kind, monkeypatch):
    path = env.state / "requests" / "c4-req.json"
    damage(path, kind, "binding")
    env.damaged.append(path)
    before = fingerprint(path)
    monkeypatch.setattr(cli, "spawn_detached_runner", lambda *a: None)
    code, result = cli.accept(load(env.config),
                              submit_args(env, "--request-id", "c4-req"),
                              workload(env))
    assert code == cli.EXIT_NOT_ACCEPTED, result
    assert result["accepted"] is False, result
    assert "unreadable" in result["reason"], result
    assert JobStore(env.state).all_ids() == []
    assert fingerprint(path) == before


# --- job states ------------------------------------------------------------------------

OTHER = "acs-20261005T000000Z-c4oth1"
SELF = "acs-20261005T000000Z-c4slf1"


def damage_state(env, job_id, kind):
    manifest_job(env, job_id)
    path = env.state / "jobs" / job_id / "state.json"
    damage(path, kind, "state")
    env.damaged.append(path)
    return path


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_prior_runners_does_not_skip_an_unreadable_job_state(env, kind):
    damage_state(env, OTHER, kind)
    manifest_job(env, SELF)
    dead, unverified = Runner(load(env.config), SELF).prior_runners()
    assert OTHER in dead or OTHER in unverified, (dead, unverified)


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_runner_never_admits_beside_an_unreadable_job_state(env, kind,
                                                               monkeypatch):
    damage_state(env, OTHER, kind)
    manifest_job(env, SELF)

    def reached(*_a, **_k):
        raise Reached("admitted beside an unreadable job state")
    monkeypatch.setattr(Runner, "revalidate", reached)
    monkeypatch.setattr(Runner, "run_guard_attempt", reached)
    monkeypatch.setattr(Runner, "_sleep_cancellable",
                        lambda self, s: (_ for _ in ()).throw(Held()))
    runner = Runner(load(env.config), SELF)
    handlers = {s: signal.getsignal(s) for s in (signal.SIGTERM,
                                                 signal.SIGINT)}
    try:
        try:
            rc = runner.run()
        except Held:
            rc = None               # waiting, not admitted: also fail-closed
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
        if runner.admission_fd is not None:
            os.close(runner.admission_fd)
        JobStore(env.state).update_state(SELF, runner={
            "pid": None, "note": "in-process test runner"})
    if rc is not None:
        st = JobStore(env.state).state(SELF)
        assert st["outcome"] == "refused", st


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_detect_lost_reports_an_unreadable_job_state(env, kind):
    path = damage_state(env, OTHER, kind)
    before = fingerprint(path)
    lost = cli.detect_lost(load(env.config))
    assert OTHER in lost, lost
    block = JobStore(env.state).block(HOST)
    assert isinstance(block, dict) and OTHER in block["jobs"], block
    assert fingerprint(path) == before


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_claim_never_overwrites_an_unreadable_job_state(env, kind):
    path = damage_state(env, OTHER, kind)
    before = fingerprint(path)
    runner = Runner(load(env.config), OTHER)
    with pytest.raises(Exception):      # noqa: B017 - any refusal
        runner.claim()
    assert fingerprint(path) == before


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_finish_if_unclaimed_never_overwrites_an_unreadable_state(env,
                                                                     kind):
    path = damage_state(env, OTHER, kind)
    before = fingerprint(path)
    store = JobStore(env.state)
    try:
        finished = cli._finish_if_unclaimed(store, OTHER, "refused",
                                            "never started")
    except Exception:       # noqa: BLE001
        finished = False
    assert finished is False
    assert fingerprint(path) == before


# --- a missing file keeps its meaning ------------------------------------------------

def test_keep_absent_block_and_gate_admit(env, monkeypatch):
    manifest_job(env, SELF)
    with pytest.raises(Reached):
        run_in_process(env, SELF, monkeypatch)


def test_keep_absent_block_reconciles_and_resumes(env):
    rc, report = acs_json(env, "reconcile")
    assert rc == 0 and report["blocked"] is False, report
    rc, resumed = acs_json(env, "resume")
    assert rc == 0 and resumed["resumed"] is True, resumed


def test_keep_absent_binding_and_state_accept(env, monkeypatch):
    monkeypatch.setattr(cli, "spawn_detached_runner", lambda *a: None)
    manifest_job(env, OTHER)            # a job reserved, no state yet
    code, result = cli.accept(load(env.config),
                              submit_args(env, "--request-id", "c4-new"),
                              workload(env))
    assert code == 0 and result["accepted"] is True, result
    manifest_job(env, SELF)
    dead, unverified = Runner(load(env.config), SELF).prior_runners()
    assert OTHER not in dead and OTHER not in unverified


def test_c4_accept_answers_unknown_when_its_job_state_turns_unreadable(
        env_queue, monkeypatch):
    """Review of the C1/C4 fixes: after the add, ``_mark_queued`` finds the
    job's state unreadable.  It never overwrites it, the compare-and-set
    proves nothing, so the answer is ``accepted: "unknown"`` and the
    request id stays bound (not a traceback, not "never started")."""
    env = env_queue
    config = load(env.config)
    store = JobStore(config.state_dir)
    damaged = []

    def add_then_damage(config_, job_id):
        path = env.state / "jobs" / job_id / "state.json"
        path.write_text('{"phase": "dispat')
        damaged.append((path, fingerprint(path)))
        return 7
    monkeypatch.setattr(q, "ensure_daemon", lambda config_: [])
    monkeypatch.setattr(q, "add_task", add_then_damage)
    try:
        code, result = cli.accept(config, submit_args(env, "--request-id",
                                                      "c4-mq"),
                                  workload(env))
        assert code == cli.EXIT_NOT_ACCEPTED, result
        assert result["accepted"] == "unknown", result
        assert store.request_job("c4-mq") == result["job_id"]
        path, before = damaged[0]
        assert fingerprint(path) == before
    finally:
        for path, _ in damaged:
            repair(path)


# --- G1 review M3: the other readers the PR claims fail closed --------------------

def _blocked_for(env, job_id) -> bool:
    block = JobStore(env.state).block(HOST)
    return isinstance(block, dict) and job_id in block["jobs"]


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_runner_blocks_the_host_when_its_own_state_is_unclaimable(
        env, kind, monkeypatch):
    """``Runner.run``: an unreadable own state is not claimed (no workload)
    and the host is blocked for it, re-read under the state lock."""
    path = damage_state(env, OTHER, kind)
    before = fingerprint(path)
    rc = run_in_process(env, OTHER, monkeypatch)
    assert rc == RC["duplicate-dispatch"]
    assert _blocked_for(env, OTHER)
    assert fingerprint(path) == before


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_runner_crash_handler_blocks_on_an_unreadable_state(env, kind,
                                                               monkeypatch):
    """``runner.main``: a crash after which the job state cannot be read
    blocks the host and re-raises; the state is not overwritten."""
    from agent_compute import runner as runner_mod
    manifest_job(env, OTHER)
    path = env.state / "jobs" / OTHER / "state.json"
    damaged = {}

    def crash(self):
        damage(path, kind, "state")
        env.damaged.append(path)
        damaged["before"] = fingerprint(path)
        raise RuntimeError("simulated runner crash")
    monkeypatch.setattr(Runner, "run", crash)
    with pytest.raises(RuntimeError):
        runner_mod.main(load(env.config), OTHER)
    assert _blocked_for(env, OTHER)
    assert "runner crashed" in \
        JobStore(env.state).block(HOST)["jobs"][OTHER]["reason"]
    assert fingerprint(path) == damaged["before"]


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_cancel_job_signals_nothing_on_an_unreadable_state(env, kind,
                                                              monkeypatch):
    path = damage_state(env, OTHER, kind)
    before = fingerprint(path)
    sent = []
    monkeypatch.setattr(cli.os, "kill", lambda *a: sent.append(a))
    result = cli.cancel_job(load(env.config), OTHER, wait_s=1.0)
    assert result["cancelled"] is False and "unreadable" in result["reason"]
    assert sent == []
    assert not (env.state / "jobs" / OTHER / "cancel-requested").exists()
    assert fingerprint(path) == before


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_wait_returns_18_on_an_unreadable_state(env, kind, capsys):
    path = damage_state(env, OTHER, kind)
    before = fingerprint(path)
    args = cli.parser().parse_args(["wait", OTHER, "--timeout", "5",
                                    "--json"])
    assert cli.cmd_wait(load(env.config), args) == 18
    out = json.loads(capsys.readouterr().out)
    assert "unreadable" in out["note"] and out["unreadable"], out
    assert _blocked_for(env, OTHER)
    assert fingerprint(path) == before


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_daemon_stop_refuses_beside_an_unreadable_state(env_queue, kind,
                                                           monkeypatch,
                                                           capsys):
    env = env_queue
    manifest_job(env, OTHER)
    path = env.state / "jobs" / OTHER / "state.json"
    damage(path, kind, "state")
    try:
        before = fingerprint(path)
        stopped = []
        monkeypatch.setattr(q, "pause_group_quietly", lambda config: None)
        monkeypatch.setattr(q, "running_tasks", lambda config: [])
        monkeypatch.setattr(q, "shutdown",
                            lambda config: stopped.append(True))
        args = cli.parser().parse_args(["daemon", "stop", "--json"])
        assert cli.cmd_daemon(load(env.config), args) == 1
        out = json.loads(capsys.readouterr().out)
        assert out["stopped"] is False and OTHER in out["reason"], out
        assert stopped == []
        assert fingerprint(path) == before
    finally:
        repair(path)


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_record_resolution_never_overwrites_an_unreadable_state(env,
                                                                   kind):
    path = damage_state(env, OTHER, kind)
    before = fingerprint(path)
    store = JobStore(env.state)
    with pytest.raises(RecordUnreadable):
        cli._record_resolution(store, OTHER, {"reason": "r"},
                               {"verified": True, "clean": True}, {})
    assert fingerprint(path) == before


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_marker_history_never_overwrites_an_unreadable_state(env, kind):
    path = damage_state(env, OTHER, kind)
    before = fingerprint(path)
    cli._marker_history(JobStore(env.state), OTHER, {"job_id": OTHER},
                        {"event": "inflight-marker-cleared"})
    assert fingerprint(path) == before


@pytest.mark.parametrize("kind", DAMAGE)
def test_c4_pending_work_counts_an_unreadable_state(env_queue, kind):
    """A daemon (re)start with an unreadable job state closes the dispatch
    gate: the job may be queued."""
    env = env_queue
    manifest_job(env, OTHER)
    path = env.state / "jobs" / OTHER / "state.json"
    damage(path, kind, "state")
    try:
        before = fingerprint(path)
        assert OTHER in q._pending_work(load(env.config))
        assert fingerprint(path) == before
    finally:
        repair(path)
