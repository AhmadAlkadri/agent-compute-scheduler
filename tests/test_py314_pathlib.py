"""A49 (campaign PA slice K6): no safety decision reads a path it cannot
examine as an absent one, on any supported Python.

Since Python 3.14, ``Path.exists``/``is_dir``/``is_file``/``is_symlink`` answer
``False`` for *any* ``OSError`` (3.12 raised a ``PermissionError``), and
``Path.glob``/``rglob``/``os.walk`` skip an unreadable directory on every
version.  A decision that took ``False`` for "nothing there" (no unfinished
job, no pending task, no cancel request, no lock user, no in-flight marker, a
request id never bound) was fail-open exactly when the path could not be
examined.  ``agent_compute.fsprobe`` treats only ENOENT/ENOTDIR as absence.

Every test makes one site's path unexaminable and asserts the fail-closed
outcome.  The ``lenient`` fixture gives ``Path.exists``/``is_dir``/``is_file``/
``is_symlink`` the 3.14 answers (the real behaviour of a 3.14 interpreter,
where it changes nothing), so each test fails against the old code on 3.12
and on 3.14 alike.  Unexaminable means a directory without permission
(``chmod 000``; skipped for root with the ``[root]`` reason), a symlink loop
(ELOOP; also swallowed by ``pathlib`` on every version), or, for a single
path inside a readable tree, ``deny`` (``os.stat``/``os.lstat``/``open`` of it
raise ``PermissionError``).

The last tests are the regression guard: a decision module may call the raw
``pathlib`` predicates only at the reviewed sites of ``ALLOWED`` below.
"""

from __future__ import annotations

import builtins
import errno
import importlib.util
import io
import json
import os
import pathlib
import re
import stat
import sys
from pathlib import Path

import pytest

from agent_compute import authority, cli, fingerprint, fsprobe, procs
from agent_compute import placement as pl
from agent_compute import queue as q
from agent_compute import safety
from agent_compute.config import load
from agent_compute.runner import Runner
from agent_compute.state import JobStore, Unreadable, write_json

REPO = Path(__file__).resolve().parents[1]
ROOT = os.geteuid() == 0
HOST = "testhost"

_spec = importlib.util.spec_from_file_location("acs_install_k6",
                                               REPO / "install.py")
install = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(install)


# --- fixtures --------------------------------------------------------------------

@pytest.fixture
def lenient(monkeypatch):
    """``Path.exists``/``is_dir``/``is_file``/``is_symlink`` as Python 3.14
    answers them: False for any OSError."""
    def make(test, call):
        def predicate(self, *args, **kwargs):
            try:
                return test(call(self))
            except (OSError, ValueError):
                return False
        return predicate

    for name, test, call in (
            ("exists", lambda st: True, os.stat),
            ("is_dir", lambda st: stat.S_ISDIR(st.st_mode), os.stat),
            ("is_file", lambda st: stat.S_ISREG(st.st_mode), os.stat),
            ("is_symlink", lambda st: stat.S_ISLNK(st.st_mode), os.lstat)):
        # os.stat/os.lstat looked up per call: a ``deny`` is honoured
        monkeypatch.setattr(pathlib.Path, name,
                            make(test, lambda p, c=call.__name__:
                                 getattr(os, c)(p)))


@pytest.fixture
def lock_out(request, lenient):
    """``lock_out(directory)``: the directory's contents can no longer be
    examined or listed (mode 000); mode restored at teardown.  Request it
    after the fixtures that create the files (tmp_path, env_direct)."""
    changed = []

    def lock(directory):
        if ROOT:
            pytest.skip("[root] root reads a mode-000 directory")
        directory = Path(directory)
        changed.append((directory, stat.S_IMODE(os.stat(directory).st_mode)))
        os.chmod(directory, 0)
        return directory

    yield lock
    for directory, mode in reversed(changed):
        os.chmod(directory, mode)


@pytest.fixture
def loop(lenient):
    """``loop(path)``: ``path`` becomes a symlink to itself (ELOOP)."""
    def make(path):
        path = Path(path)
        if path.is_symlink():
            return path
        if os.path.lexists(path):
            if path.is_dir():
                import shutil
                shutil.rmtree(path)
            else:
                path.unlink()
        path.parent.mkdir(parents=True, exist_ok=True)
        os.symlink(path.name, path)
        return path
    return make


@pytest.fixture
def deny(monkeypatch, lenient):
    """``deny(path)``: ``os.stat``/``os.lstat``/``open`` of ``path`` and of
    anything under it raise PermissionError (the rest of the tree stays
    readable).  ``deny.listing(path)``: only listing the directory ``path``
    (``os.scandir``/``os.listdir``) fails."""
    denied: list[str] = []
    unlistable: list[str] = []
    real = {name: getattr(os, name) for name in ("stat", "lstat", "scandir",
                                                 "listdir")}
    real_open = io.open

    def hit(arg, roots, below=True) -> bool:
        if isinstance(arg, int):
            return False
        try:
            text = os.fspath(arg)
        except TypeError:
            return False
        if isinstance(text, bytes):
            text = os.fsdecode(text)
        text = os.path.abspath(text)
        return any(text == d or (below and text.startswith(d + os.sep))
                   for d in roots)

    def wrap(name, roots, below):
        def guarded(path=".", *args, **kwargs):
            if hit(path, roots, below):
                raise PermissionError(errno.EACCES, "Permission denied",
                                      str(path))
            return real[name](path, *args, **kwargs)
        return guarded

    def guarded_open(file, *args, **kwargs):
        if hit(file, denied):
            raise PermissionError(errno.EACCES, "Permission denied",
                                  str(file))
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(os, "stat", wrap("stat", denied, True))
    monkeypatch.setattr(os, "lstat", wrap("lstat", denied, True))
    monkeypatch.setattr(os, "scandir", wrap("scandir", unlistable, False))
    monkeypatch.setattr(os, "listdir", wrap("listdir", unlistable, False))
    monkeypatch.setattr(io, "open", guarded_open)
    monkeypatch.setattr(builtins, "open", guarded_open)

    def add(path):
        denied.append(os.path.abspath(os.fspath(path)))

    def listing(path):
        unlistable.append(os.path.abspath(os.fspath(path)))
    add.listing = listing
    return add


def stub_job(env, job_id: str, *, state=None, manifest=None) -> Path:
    job = env.state / "jobs" / job_id
    job.mkdir(parents=True, exist_ok=True)
    write_json(job / "manifest.json", {"host": HOST, "job_id": job_id,
                                       "admission_wait_s": 30,
                                       "project": "alpha",
                                       **(manifest or {})})
    if state is not None:
        write_json(job / "state.json", state)
    return job


# --- fsprobe: the helper itself ---------------------------------------------------

def test_fsprobe_absence_is_enoent_and_enotdir_only(tmp_path):
    assert fsprobe.exists_strict(tmp_path / "nope") is False
    (tmp_path / "f").write_text("x")
    assert fsprobe.exists_strict(tmp_path / "f") is True
    assert fsprobe.exists_strict(tmp_path / "f" / "below") is False  # ENOTDIR
    assert fsprobe.is_file_strict(tmp_path / "f") is True
    assert fsprobe.is_dir_strict(tmp_path) is True
    assert fsprobe.is_dir_strict(tmp_path / "f") is False
    assert fsprobe.is_file_strict(tmp_path) is False
    (tmp_path / "dangling").symlink_to("missing")
    assert fsprobe.exists_strict(tmp_path / "dangling") is False
    assert fsprobe.lexists_strict(tmp_path / "dangling") is True
    assert fsprobe.is_symlink_strict(tmp_path / "dangling") is True
    assert fsprobe.is_symlink_strict(tmp_path / "f") is False


def test_fsprobe_raises_for_every_other_error(tmp_path):
    (tmp_path / "self").symlink_to("self")
    for probe in (fsprobe.exists_strict, fsprobe.is_dir_strict,
                  fsprobe.is_file_strict):
        with pytest.raises(OSError) as raised:
            probe(tmp_path / "self")
        assert raised.value.errno == errno.ELOOP
    with pytest.raises(OSError):
        fsprobe.exists_strict("a" * 5000)                  # ENAMETOOLONG
    with pytest.raises(OSError) as raised:
        fsprobe.exists_strict("a\0b")
    assert raised.value.errno == errno.EINVAL
    # the symlink itself is examinable: it is a link
    assert fsprobe.is_symlink_strict(tmp_path / "self") is True


def test_fsprobe_permission_error_is_not_absence(tmp_path, lock_out):
    (tmp_path / "d").mkdir()
    (tmp_path / "d" / "f").write_text("x")
    lock_out(tmp_path / "d")
    for probe in (fsprobe.exists_strict, fsprobe.is_file_strict,
                  fsprobe.is_dir_strict, fsprobe.lexists_strict,
                  fsprobe.is_symlink_strict):
        with pytest.raises(PermissionError):
            probe(tmp_path / "d" / "f")
    with pytest.raises(PermissionError):
        fsprobe.listdir_strict(tmp_path / "d")
    with pytest.raises(PermissionError):
        fsprobe.glob_strict(tmp_path / "d", "*")
    with pytest.raises(PermissionError):
        fsprobe.walk_files_strict(tmp_path / "d")


def test_fsprobe_listing_helpers(tmp_path):
    assert fsprobe.listdir_strict(tmp_path / "nope") == []
    (tmp_path / "a" / "b").mkdir(parents=True)
    (tmp_path / "a" / "x.json").write_text("{}")
    (tmp_path / "a" / "y.txt").write_text("")
    (tmp_path / "a" / "b" / "z").write_text("")
    (tmp_path / "a" / "ln").symlink_to("x.json")
    assert fsprobe.listdir_strict(tmp_path / "a") == ["b", "ln", "x.json",
                                                      "y.txt"]
    assert fsprobe.glob_strict(tmp_path / "a", "*.json") == [
        tmp_path / "a" / "x.json"]
    assert fsprobe.walk_files_strict(tmp_path / "nope") == []
    assert fsprobe.walk_files_strict(tmp_path / "a") == [
        tmp_path / "a" / "b" / "z", tmp_path / "a" / "ln",
        tmp_path / "a" / "x.json", tmp_path / "a" / "y.txt"]


# --- state: the unfinished-job scan ------------------------------------------------

def test_all_ids_raises_when_the_jobs_dir_cannot_be_examined(tmp_path,
                                                             lock_out):
    state = tmp_path / "s"
    (state / "jobs" / "acs-20261010T000000Z-aaaaaa").mkdir(parents=True)
    store = JobStore(state)
    assert store.all_ids() == ["acs-20261010T000000Z-aaaaaa"]
    lock_out(state)
    with pytest.raises(OSError):
        store.all_ids()                 # not [], "no jobs"


def test_all_ids_raises_when_the_jobs_dir_cannot_be_listed(tmp_path,
                                                           lock_out):
    state = tmp_path / "s"
    (state / "jobs" / "acs-20261010T000000Z-aaaaaa").mkdir(parents=True)
    lock_out(state / "jobs")            # stat works, listing does not
    with pytest.raises(OSError):
        JobStore(state).all_ids()


def test_all_ids_raises_for_a_looping_jobs_dir(tmp_path, loop):
    state = tmp_path / "s"
    state.mkdir()
    loop(state / "jobs")
    with pytest.raises(OSError):
        JobStore(state).all_ids()


def test_all_ids_of_a_state_dir_never_created_is_empty(tmp_path, lenient):
    assert JobStore(tmp_path / "never").all_ids() == []


# --- queue: the pending-work scan and the daemon probe ------------------------------

def test_pending_work_counts_an_unexaminable_queue_state_as_pending(
        env_direct, lock_out):
    config = load(env_direct.config)
    data = config.pueue_dir / "data"
    data.mkdir(parents=True)
    (data / "state.json").write_text('{"tasks": {}}')
    assert q._pending_work(config) == []
    lock_out(data)
    assert q._pending_work(config) == ["<unreadable pueue state>"]


def test_pending_work_counts_a_looping_queue_state_as_pending(env_direct,
                                                              loop):
    config = load(env_direct.config)
    loop(config.pueue_dir / "data" / "state.json")
    assert q._pending_work(config) == ["<unreadable pueue state>"]


def test_daemon_running_refuses_to_guess_when_the_socket_cannot_be_examined(
        env_direct, lock_out):
    config = load(env_direct.config)
    run = config.pueue_dir / "run"
    run.mkdir(parents=True)
    assert q.daemon_running(config) is False          # no socket: not running
    lock_out(run)
    with pytest.raises(q.QueueError, match="cannot be examined"):
        q.daemon_running(config)


# --- placement: the request-id bindings of other hosts (A39) ------------------------

def test_placement_records_of_an_unlistable_dir_are_unreadable_not_none(
        tmp_path, lock_out):
    state = tmp_path / "s"
    record = pl.record_path(state, "req-1")
    write_json(record, {"request_id": "req-1", "host": "other",
                        "phase": "accepted"})
    assert [r["request_id"] for r in pl.records(state)] == ["req-1"]
    lock_out(record.parent)
    found = pl.records(state)
    assert len(found) == 1 and isinstance(found[0], Unreadable), found


def test_placement_records_of_an_unexaminable_state_dir_are_unreadable(
        tmp_path, lock_out):
    state = tmp_path / "s"
    write_json(pl.record_path(state, "req-1"),
               {"request_id": "req-1", "host": "other", "phase": "accepted"})
    lock_out(state)
    found = pl.records(state)
    assert len(found) == 1 and isinstance(found[0], Unreadable), found


def test_no_placement_dir_means_no_records(tmp_path, lenient):
    assert pl.records(tmp_path / "s") == []


def test_an_unexaminable_state_dir_never_lets_a_request_id_through(
        env_direct, lock_out):
    """The two checks of a request id bound on another host (A39) answered
    "no record" for a state dir that could not be examined."""
    config = load(env_direct.config)
    lock_out(config.state_dir.parent)
    refusal = cli.request_bound_elsewhere(config, "req-1")
    assert refusal and refusal["accepted"] is False
    assert "cannot be examined" in refusal["reason"]
    held = cli.record_refusal(config, "req-1", None)
    assert held and "cannot be examined" in held["reason"]


def test_pending_work_counts_an_unexaminable_jobs_dir_as_pending(env_direct,
                                                                  loop):
    config = load(env_direct.config)
    loop(config.state_dir / "jobs")
    assert q._pending_work(config) == ["<unreadable jobs dir>"]


def test_a_remote_send_is_refused_when_the_state_dir_cannot_be_examined(
        env_direct, loop, monkeypatch, capsys):
    """Nothing is sent when the records that could forbid the send (a
    placement record, a local binding) cannot be looked at."""
    from types import SimpleNamespace
    from agent_compute import remote as remote_mod
    config = load(env_direct.config)
    loop(config.state_dir)
    monkeypatch.setattr(remote_mod, "route",
                        lambda cfg, host: SimpleNamespace(view=lambda: {}))

    def sent(*_a, **_k):
        raise AssertionError("the command was sent")
    monkeypatch.setattr(remote_mod, "call", sent)
    args = SimpleNamespace(cmd="submit", json=True)
    code = cli.send_remote(config, args, "other", ["submit", "--host",
                                                   "other"], [], "req-1",
                           pinned=True, project=None)
    assert code == cli.EXIT_NOT_ACCEPTED
    assert "cannot be examined" in capsys.readouterr().out


# --- procs: process identity and the lock's users -----------------------------------

def test_lock_file_users_cannot_tell_when_the_lock_cannot_be_examined(
        tmp_path, lock_out):
    home = tmp_path / "g"
    home.mkdir()
    (home / "lock").write_text("")
    lock_out(home)
    assert procs.lock_file_users(home / "lock") is None     # not []


def test_lock_file_users_of_an_absent_lock_is_empty(tmp_path, lenient):
    assert procs.lock_file_users(tmp_path / "nolock") == []


def test_start_info_raises_when_the_proc_entry_cannot_be_examined(
        monkeypatch, deny):
    """/proc/<pid> hidden by an error other than ENOENT is not "no such
    process" (a stale identity would then read as gone)."""
    monkeypatch.setattr(sys, "platform", "linux")
    deny("/proc/4242")
    with pytest.raises(procs.ProcCheckError):
        procs.start_info(4242)


# --- runner: cancellation and the launch ---------------------------------------------

def test_a_cancel_request_that_cannot_be_examined_is_a_cancellation(
        env_direct, lock_out):
    job_id = "acs-20261010T000000Z-k6can1"
    stub_job(env_direct, job_id)
    runner = Runner(load(env_direct.config), job_id)
    assert runner.cancel_request() is None
    lock_out(runner.dir)
    request = runner.cancel_request()
    assert request and request["outcome"] == "cancelled", request


def test_a_looping_cancel_request_is_a_cancellation(env_direct, loop):
    job_id = "acs-20261010T000000Z-k6can2"
    stub_job(env_direct, job_id)
    runner = Runner(load(env_direct.config), job_id)
    loop(runner.dir / "cancel-requested")
    request = runner.cancel_request()
    assert request and request["outcome"] == "cancelled", request


def test_launch_began_assumes_the_worst_for_samples_it_cannot_examine(
        env_direct, loop):
    job_id = "acs-20261010T000000Z-k6lau1"
    stub_job(env_direct, job_id)
    runner = Runner(load(env_direct.config), job_id)
    attempt = runner.dir / "guard" / "attempt-1"
    attempt.mkdir(parents=True)
    assert runner.launch_began(attempt) is False
    loop(attempt / "samples.csv")
    assert runner.launch_began(attempt) is True


def test_dispatch_refuses_a_baseline_table_it_cannot_examine(env_direct,
                                                             lock_out):
    """The guard (pinned, unchanged) would read the baseline of a directory
    it cannot search as an empty table: admission without the recorded
    peak.  The runner refuses before the guard starts."""
    from agent_compute.runner import engine_path
    config = load(env_direct.config)
    baseline = env_direct.root / "tables" / "baseline.json"
    baseline.parent.mkdir()
    job_id = "acs-20261010T000000Z-k6bas1"
    stub_job(env_direct, job_id, manifest={
        "paths": {"baseline": str(baseline)},
        "backend": {"kind": "acs-guard", "path": str(engine_path()),
                    "sha256": config.guard_sha256},
        "guard_home": str(config.effective_guard_home),
        "marker": config.markers[0], "marker_aliases": config.markers[1:],
        "legacy_workload_env": config.legacy_workload_env,
        "container": None, "code": None})
    runner = Runner(config, job_id)
    assert runner.baseline_problem() is None            # absent: a first run
    assert runner.revalidate() is None
    lock_out(baseline.parent.parent)
    problem = runner.revalidate()
    assert problem and "cannot be examined" in problem, problem
    assert "baseline table" in problem


# --- cli: reconcile evidence ---------------------------------------------------------

def test_guard_records_and_samples_raise_when_the_guard_dir_cannot_be_listed(
        env_direct, lock_out):
    job_id = "acs-20261010T000000Z-k6rec1"
    job = stub_job(env_direct, job_id)
    (job / "guard" / "attempt-1").mkdir(parents=True)
    store = JobStore(env_direct.state)
    assert len(cli._guard_records(store, job_id)) == 1
    lock_out(job / "guard")
    with pytest.raises(OSError):
        cli._guard_records(store, job_id)
    with pytest.raises(OSError):
        cli._samples_exist(store, job_id)


def test_never_launched_needs_positive_evidence_not_an_unlistable_dir(
        env_direct, lock_out):
    job_id = "acs-20261010T000000Z-k6rec2"
    job = stub_job(env_direct, job_id)
    (job / "guard" / "attempt-1").mkdir(parents=True)
    store = JobStore(env_direct.state)
    assert cli._never_launched(store, job_id, [], {})       # nothing launched
    lock_out(job / "guard")
    assert cli._never_launched(store, job_id, [], {}) is None


def test_reconcile_does_not_clear_a_job_whose_guard_records_cannot_be_read(
        env_direct, lock_out, monkeypatch):
    """The wrongful unblock: an unlistable ``guard/`` read as "no guard
    record, no samples.csv: the guard never began launching"."""
    job_id = "acs-20261010T000000Z-k6rec3"
    runner = {"pid": 999991, "start": "ticks:1"}
    job = stub_job(env_direct, job_id, state={
        "phase": "executing", "runner": runner,
        "guard": {"pid": 999992, "start": "ticks:2"}, "outcome": "unknown"})
    (job / "guard" / "attempt-1").mkdir(parents=True)
    monkeypatch.setattr(procs, "liveness", lambda ident: procs.GONE)
    store = JobStore(env_direct.state)
    facts, _ = cli._reconcile_job(store, job_id, {}, None)
    assert "blocker" not in facts, facts            # the control: it is clear
    lock_out(job / "guard")
    facts, _ = cli._reconcile_job(store, job_id, {}, None)
    assert "guard" in facts.get("blocker", ""), facts
    assert "cleanup" not in facts or facts["cleanup"].get("method") != \
        "never-launched"


def test_a_marker_is_not_mapped_to_a_job_when_another_jobs_records_cannot_be_read(
        env_direct, lock_out):
    first = stub_job(env_direct, "acs-20261010T000000Z-k6mrk1", state={
        "workload": {"guard_job_id": "g-1"}})
    second = stub_job(env_direct, "acs-20261010T000000Z-k6mrk2", state={})
    (second / "guard" / "attempt-1").mkdir(parents=True)
    store = JobStore(env_direct.state)
    config = load(env_direct.config)
    marker = {"job_id": "g-1"}
    assert cli._marker_job(store, config, marker) == first.name
    lock_out(second / "guard")
    assert cli._marker_job(store, config, marker) is None


def test_a_release_keeps_a_job_directory_whose_manifest_cannot_be_examined(
        env_direct, loop):
    job_id = "acs-20261010T000000Z-k6rel1"
    job = env_direct.state / "jobs" / job_id
    job.mkdir(parents=True)
    loop(job / "manifest.json")
    cli._release(JobStore(env_direct.state), job_id, None)
    assert job.is_dir(), "the directory of a job that may have a manifest"


def test_a_release_still_removes_a_directory_with_no_manifest(env_direct,
                                                              lenient):
    job_id = "acs-20261010T000000Z-k6rel2"
    job = env_direct.state / "jobs" / job_id
    job.mkdir(parents=True)
    cli._release(JobStore(env_direct.state), job_id, None)
    assert not job.exists()


# --- cli: renames, guard homes, evidence ---------------------------------------------

def test_a_rename_is_refused_when_the_blocks_cannot_be_listed(
        env_direct, lock_out):
    config = load(env_direct.config)
    blocks = env_direct.state / "blocks"
    blocks.mkdir(parents=True)
    store = JobStore(env_direct.state)
    assert cli.rename_problem(config, store) is None
    lock_out(blocks)
    problem = cli.rename_problem(config, store)
    assert problem and problem["blocks"], problem


def test_guard_home_change_is_refused_when_the_old_marker_cannot_be_examined(
        env_direct, tmp_path, loop):
    config = load(env_direct.config)
    old = tmp_path / "old-home"
    old.mkdir()
    write_json(env_direct.state / cli.GUARD_HOME_RECORD,
               {"guard_home": str(old)})
    store = JobStore(env_direct.state)
    assert cli.guard_home_change_problem(config, store) is None
    loop(old / "inflight.json")
    problem = cli.guard_home_change_problem(config, store)
    assert problem, "an in-flight marker that cannot be examined"
    assert str(old / "inflight.json") in json.dumps(problem)


def test_the_evidence_files_of_an_unlistable_guard_dir_are_an_error(
        env_direct, lock_out):
    job_id = "acs-20261010T000000Z-k6fet1"
    job = stub_job(env_direct, job_id, state={})
    (job / "guard" / "attempt-1").mkdir(parents=True)
    (job / "guard" / "attempt-1" / "guard.json").write_text("{}")
    store = JobStore(env_direct.state)
    names = {str(p) for p in cli._evidence_files(store, job_id)}
    assert "guard/attempt-1/guard.json" in names
    lock_out(job / "guard")
    with pytest.raises(OSError):
        cli._evidence_files(store, job_id)      # not a bundle without them


def test_pin_evidence_names_a_file_it_cannot_examine(tmp_path, loop):
    path = loop(tmp_path / "evidence.txt")
    problem, _ = cli.pin_evidence_problem(path, "0" * 64, HOST)
    assert problem and "cannot be examined" in problem, problem


# --- authority: the single-authority scan ---------------------------------------------

def test_a_state_dir_with_an_unexaminable_jobs_dir_is_a_problem(tmp_path,
                                                                loop):
    state = tmp_path / "other"
    state.mkdir()
    loop(state / "jobs")
    entry = authority._state_dir_scan(state, "the default state dir")
    assert entry["problem"] and "cannot be read" in entry["problem"], entry


def test_a_guard_home_with_an_unexaminable_marker_is_a_problem(tmp_path, loop):
    home = tmp_path / "g"
    home.mkdir()
    loop(home / "inflight.json")
    entry = authority._guard_home_scan(home, "the legacy home")
    assert entry["problem"] and "cannot be read" in entry["problem"], entry


def test_a_guard_home_with_an_unexaminable_lock_is_a_problem(tmp_path,
                                                             lock_out):
    home = tmp_path / "g"
    home.mkdir()
    (home / "lock").write_text("")
    lock_out(home)
    entry = authority._guard_home_scan(home, "the legacy home")
    assert entry["problem"], entry


def test_a_file_that_cannot_be_looked_at_counts_as_there(tmp_path, loop):
    assert authority._present(tmp_path / "absent") is False
    assert authority._present(loop(tmp_path / "x")) is True


# --- fingerprint: the inputs a job is admitted on ---------------------------------------

def test_an_unreadable_input_is_not_fingerprinted_as_missing(tmp_path,
                                                             lock_out):
    import hashlib
    inputs = tmp_path / "in"
    inputs.mkdir()
    (inputs / "a.txt").write_text("a")
    digest = hashlib.sha256()
    assert fingerprint._hash_tree(tmp_path / "nothing", digest, "x") == [
        "x (missing)"]
    lock_out(inputs)
    with pytest.raises(OSError):
        fingerprint._hash_tree(inputs / "a.txt", digest, "x")


def test_an_unlistable_subdirectory_of_an_input_is_not_skipped(tmp_path,
                                                               lock_out):
    import hashlib
    tree = tmp_path / "tree"
    (tree / "sub").mkdir(parents=True)
    (tree / "top.txt").write_text("t")
    (tree / "sub" / "deep.txt").write_text("d")
    lock_out(tree / "sub")
    with pytest.raises(OSError):
        fingerprint._hash_tree(tree, hashlib.sha256(), "x")


def test_a_submit_with_an_unreadable_input_is_refused_not_crashed(
        env_direct, lock_out):
    tree = env_direct.root / "inputs"
    tree.mkdir()
    (tree / "a.txt").write_text("a")
    lock_out(tree)
    args = cli.parser().parse_args(
        ["submit", "--json", "--case", "t", "--timeout-min", "1",
         "--reserve-gb", "0.5", "--cwd", str(env_direct.projects["alpha"]),
         "--input", str(tree / "a.txt")])
    with pytest.raises(cli.ConfigError, match="cannot be fingerprinted"):
        cli.build_manifest(load(env_direct.config), args,
                           [sys.executable, "-c", "pass"])


# --- safety: the activation floor --------------------------------------------------------

def installed_prefix(tmp_path: Path) -> Path:
    prefix = tmp_path / "prefix"
    active = prefix / "versions" / "9.9.9-aaaaaaaaaa"
    active.mkdir(parents=True)
    (active / "SAFETY").write_text(f"{safety.SAFETY_LEVEL}\n")
    (prefix / "current").symlink_to(active)
    return prefix


def test_the_floor_checks_a_version_whose_level_cannot_be_examined(
        tmp_path, lock_out):
    prefix = installed_prefix(tmp_path)
    assert safety.floor_problems(prefix) == []
    other = prefix / "versions" / "9.9.9-bbbbbbbbbb"
    other.mkdir()
    (other / "SAFETY").write_text(f"{safety.SAFETY_LEVEL + 5}\n")
    assert safety.floor_problems(prefix), "a higher installed level exists"
    (other / "SAFETY").write_text(f"{safety.SAFETY_LEVEL}\n")
    assert safety.floor_problems(prefix) == []
    lock_out(other)
    problems = safety.floor_problems(prefix)
    assert any("9.9.9-bbbbbbbbbb" in p for p in problems), problems


def test_the_floor_checks_an_unlistable_versions_dir(tmp_path, deny):
    prefix = installed_prefix(tmp_path)
    assert safety.floor_problems(prefix) == []
    # the active version's own SAFETY stays readable: only the listing fails
    deny.listing(prefix / "versions")
    problems = safety.floor_problems(prefix)
    assert any("cannot be listed" in p for p in problems), problems


def test_a_legacy_level_is_not_assumed_for_an_unexaminable_safety_file(
        tmp_path, lock_out):
    version = tmp_path / "0.1.0-eb369eb937"
    version.mkdir()
    assert safety.safety_level(version) == 1                # the legacy name
    lock_out(version)
    assert safety.safety_level(version) is None


# --- install.py -------------------------------------------------------------------------

def test_install_refuses_to_start_over_a_manifest_it_cannot_examine(
        tmp_path, lock_out):
    prefix = tmp_path / "prefix"
    prefix.mkdir()
    (prefix / "install-manifest.json").write_text("{}")
    lock_out(prefix)
    with pytest.raises(SystemExit):
        install.load_manifest(prefix)    # not a fresh manifest to overwrite


def test_a_missing_manifest_is_a_fresh_one(tmp_path, lenient):
    assert install.load_manifest(tmp_path / "nowhere")["versions"] == []


def test_install_reports_a_wrapper_it_cannot_examine_as_a_collision(
        tmp_path, lock_out):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "agent-compute").write_text("not ours")
    (tmp_path / "prefix").mkdir()
    lock_out(bin_dir)
    p = {"bin": bin_dir, "skill_targets": [], "prefix": tmp_path / "prefix"}
    problems = install.check_collisions(p, {"files": []}, sys.executable)
    assert problems and "cannot be examined" in problems[0], problems


def test_install_does_not_record_an_existing_dir_it_cannot_see_as_created(
        tmp_path, lock_out):
    parent = tmp_path / "parent"
    (parent / "child").mkdir(parents=True)
    lock_out(parent)
    with pytest.raises(OSError):
        install.missing_dirs(parent / "child" / "new")


def test_activation_refuses_when_the_current_link_cannot_be_examined(
        tmp_path, deny):
    """Without ``current`` examined, the active level reads 0 and a version
    below the active one would pass the "not lower than active" check."""
    prefix = tmp_path / "prefix"
    version = "9.9.9-aaaaaaaaaa"
    target = prefix / "versions" / version
    for part in ("bin", "src/agent_compute", "skill"):
        (target / part).mkdir(parents=True)
    (target / "src/agent_compute/__init__.py").write_text(
        '__version__ = "9.9.9"\n')
    (target / "VERSION").write_text(version + "\n")
    (target / "SAFETY").write_text(f"{safety.SAFETY_LEVEL}\n")
    assert install.activation_problems(prefix, version) == []
    deny(prefix / "current")
    problems = install.activation_problems(prefix, version)
    assert any("current" in p and "cannot be examined" in p
               for p in problems), problems



def _version(prefix, version, level):
    target = prefix / "versions" / version
    for part in ("bin", "src/agent_compute", "skill"):
        (target / part).mkdir(parents=True)
    (target / "src/agent_compute/__init__.py").write_text(
        f'__version__ = "{version.split("-")[0]}"\n')
    (target / "VERSION").write_text(version + "\n")
    (target / "SAFETY").write_text(f"{level}\n")
    return target


def test_an_unreadable_active_level_is_not_level_zero(tmp_path, lock_out):
    """K6 review: an active version whose SAFETY exists but cannot be read
    must not read as level 0 (a lower version would pass the rollback
    check once a higher level exists)."""
    prefix = tmp_path / "prefix"
    low = _version(prefix, "9.9.8-aaaaaaaaaa", safety.SAFETY_LEVEL)
    high = _version(prefix, "9.9.9-bbbbbbbbbb", safety.SAFETY_LEVEL + 1)
    (prefix / "current").symlink_to(high)
    assert install.activation_problems(prefix, low.name)   # lower: refused
    lock_out(high / "SAFETY")
    problems = install.activation_problems(prefix, low.name)
    assert any("active level is unknown" in p for p in problems), problems


def test_an_unparsable_active_level_is_not_level_zero(tmp_path):
    prefix = tmp_path / "prefix"
    low = _version(prefix, "9.9.8-aaaaaaaaaa", safety.SAFETY_LEVEL)
    high = _version(prefix, "9.9.9-bbbbbbbbbb", safety.SAFETY_LEVEL + 1)
    (high / "SAFETY").write_text("twelve\n")
    (prefix / "current").symlink_to(high)
    problems = install.activation_problems(prefix, low.name)
    assert any("active level is unknown" in p for p in problems), problems


def test_a_pre_floor_active_version_still_allows_rolling_forward(tmp_path):
    """No SAFETY file and no legacy entry is a known fact (it predates the
    floor), not an unknown level: rolling forward stays possible."""
    prefix = tmp_path / "prefix"
    new = _version(prefix, "9.9.9-bbbbbbbbbb", safety.SAFETY_LEVEL)
    old = _version(prefix, "9.9.7-cccccccccc", 1)
    (old / "SAFETY").unlink()
    (prefix / "current").symlink_to(old)
    assert install.activation_problems(prefix, new.name) == []


def test_the_floor_check_reports_an_unreadable_installed_level(tmp_path):
    prefix = tmp_path / "prefix"
    active = _version(prefix, "9.9.8-aaaaaaaaaa", safety.SAFETY_LEVEL)
    other = _version(prefix, "9.9.9-bbbbbbbbbb", safety.SAFETY_LEVEL + 1)
    (other / "SAFETY").write_text("unknown\n")
    (prefix / "current").symlink_to(active)
    problems = safety.floor_problems(prefix)
    assert any("highest installed level is unknown" in p
               for p in problems), problems


def test_a_symlink_to_an_unexaminable_target_does_not_refuse_the_walk(
        tmp_path, lock_out):
    """K6 review: symlinks are never hashed (os.walk does not descend into
    one); a link whose target cannot be examined must not refuse a valid
    input tree, and the digest is the one without the link."""
    import hashlib
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "a.txt").write_text("a")
    hidden = tmp_path / "hidden"
    (hidden / "d").mkdir(parents=True)
    plain = fingerprint._hash_tree(tree, hashlib.sha256(), "x")
    (tree / "to-dir").symlink_to(hidden / "d")
    (tree / "to-file").symlink_to(hidden / "f.txt")
    lock_out(hidden)
    assert fingerprint._hash_tree(tree, hashlib.sha256(), "x") == plain

# --- the regression guard -----------------------------------------------------------------

# Decision modules: ``src/agent_compute/*.py``, ``bin/agent-compute`` and
# ``install.py``.  A raw pathlib predicate (``exists``, ``is_dir``,
# ``is_file``, ``is_symlink``, ``glob``, ``rglob``, ``iterdir``) in them is
# fail-open for a path that cannot be examined (Python 3.14, and ``glob`` on
# every version): a decision uses ``agent_compute.fsprobe``.  The sites
# below are the reviewed ones that may stay, each as (file, source line):
# (how many, why a False answer is not a fail-open decision).  A new site
# either uses ``fsprobe`` or is reviewed into this table in the same change.
# ``fsprobe.py`` is the implementation; ``guard/engine.py`` is pinned by
# sha256 (a change there needs the operator, campaign PA D3) and is not
# scanned.
REFUSES = "False refuses or reports a problem, never admits or removes"
DISPLAY = "shown in a report only; no decision reads it"
CREATES = ("creation or cleanup: the write that follows fails the same "
           "way (the parent cannot be searched) or the target is checked "
           "again")
KEEPS = "False skips a removal or an overwrite, which leaves things as they are"
ROUTES = ("routing or resolving only: False leaves the record as it is or "
          "falls to the local command, which reads the same state "
          "fail-closed (C4)")
ALLOWED = {
    # K6 review: the os.path / os.walk / glob forms (they swallow errors on
    # every Python version), each reviewed with its reason below
    ("src/agent_compute/authority.py", "if os.path.isfile(rest[:cut]):"):
        (1, "another pueued's config path: False falls back to the first "
            "word, which 'cannot be read: never ruled out' (fail-closed)"),
    ("src/agent_compute/fingerprint.py",
     '"""``os.walk(top)``\'s files, sorted by directory then name, but a'):
        (1, "a docstring naming os.walk, not a call"),
    ("install.py", "for dirpath, dirnames, filenames in os.walk(root):"):
        (2, "make_readonly/make_writable of the installer's own version "
            "copy: a directory it cannot list also cannot be removed "
            "(rmtree raises)"),
    ("src/agent_compute/cli.py", "if not cwd.is_dir():"):
        (2, "the submit's --cwd: False refuses (build_manifest) or does "
            "not route (_local_project)"),
    ("src/agent_compute/cli.py",
     'attempts = sorted((p for p in root.glob("attempt-*")'):
        (1, DISPLAY + " (the last admission memory of status)"),
    ("src/agent_compute/cli.py",
     'missing = [name for name, ok in (("job directory", job_dir.is_dir()),'):
        (1, "reconcile: False is a blocker (no job evidence)"),
    ("src/agent_compute/cli.py",
     '{"lock": str(lock), "exists": lock.exists(),'):
        (1, DISPLAY + " (doctor's compute lock location)"),
    ("src/agent_compute/cli.py", "if not state_dir.is_dir():"):
        (1, CREATES + " (the operator authorization record: private_dir "
            "and os.open raise on an unexaminable state dir)"),
    ("src/agent_compute/cli.py",
     "if state_dir.is_dir() and not os.access(state_dir, os.W_OK):"):
        (1, CREATES + " (the authorization's host id: ensure_host_id "
            "raises, the record is then absent and the action refused)"),
    ("src/agent_compute/cli.py",
     'view = {"root": root, "path": str(path), "exists": path.is_dir()}'):
        (1, DISPLAY + " (doctor's checkout view)"),
    ("src/agent_compute/cli.py",
     "if not config.state_dir.is_dir() or routed_from():"):
        (1, ROUTES + " (resolve_placements)"),
    ("src/agent_compute/cli.py", "if target.exists():"):
        (1, "fetch never overwrites: the mkdir that follows (no exist_ok) "
            "refuses an existing directory it cannot see"),
    ("src/agent_compute/cli.py", "if not config.state_dir.is_dir():"):
        (1, ROUTES + " (placement_route)"),
    ("src/agent_compute/config.py", "if not path.is_file():"):
        (1, "load: False raises ConfigError (no config)"),
    ("src/agent_compute/fingerprint.py",
     'info = {"path": str(guard), "exists": guard.is_file()}'):
        (1, REFUSES + " (the backend identity: every user refuses on "
            "exists False)"),
    ("src/agent_compute/fingerprint.py", "if not guard.is_file():"):
        (1, REFUSES + " (the backend identity)"),
    ("src/agent_compute/runner.py", 'if not Path(man["cwd"]).is_dir():'):
        (1, "dispatch: False holds the job (the cwd no longer exists)"),
    ("src/agent_compute/safety.py", "if not current.is_symlink():"):
        (1, "the floor: False is a problem (current is not a link)"),
    ("install.py",
     "return path.is_symlink() and str(os.readlink(path)).startswith("
     "str(prefix))"):
        (1, KEEPS + " (owned_link: False is 'not ours')"),
    ("install.py", "if temp.is_symlink() or temp.exists():"):
        (1, CREATES),
    ("install.py", "if not target.exists():"):
        (1, CREATES + " (os.replace of the staging dir)"),
    ("install.py", "if staging.exists():"): (1, CREATES),
    ("install.py", "if not wrapper.exists() or wrapper.read_text() != text:"):
        (1, CREATES + " (the wrapper was checked for ownership above)"),
    ("install.py", "if not base.is_dir():"):
        (1, "False skips linking the skill into a client directory"),
    ("install.py", 'if args.config_from and not p["config"].exists():'):
        (1, CREATES + " (copyfile into a directory that cannot be searched "
            "fails)"),
    ("install.py", "if target.is_symlink() or not target.is_dir():"):
        (1, REFUSES + " (activation_problems)"),
    ("install.py", "if not (target / part).is_dir() or"):
        (1, REFUSES + " (activation_problems)"),
    ("install.py", "(target / part).is_symlink()]"):
        (1, REFUSES + " (activation_problems)"),
    ("install.py",
     'if path.is_file() and "owned by agent-compute-scheduler" in \\'):
        (1, KEEPS + " (uninstall removes only what it can read as its own)"),
    ("install.py", "if current.is_symlink():"):
        (1, KEEPS + " (uninstall)"),
    ("install.py", 'if path.is_dir() and str(path).startswith(str(p["prefix"])):'):
        (1, KEEPS + " (uninstall)"),
    ("install.py", "if path.is_file():"):
        (1, KEEPS + " (uninstall removes the config it created)"),
    ("install.py",
     'manifest["current_link"] = os.readlink(current) if current.is_symlink() \\'):
        (1, DISPLAY + " (install.py status)"),
    ("install.py", "if versions.is_dir():"):
        (1, DISPLAY + " (install.py status)"),
    ("install.py", "dirs |= {d for d in versions.iterdir()"):
        (1, DISPLAY + " (install.py status)"),
    ("install.py", 'if d.is_dir() and not d.name.startswith(".")}'):
        (1, DISPLAY + " (install.py status)"),
}
GUARDED = ("src/agent_compute/*.py", "bin/agent-compute", "install.py")
SKIPPED = ("src/agent_compute/fsprobe.py",)
RAW = re.compile(r"\.(exists|is_dir|is_file|is_symlink|glob|rglob|iterdir)"
                 r"\("
                 # K6 review: these swallow errors on every Python version
                 r"|\bos\.path\.(isdir|isfile|islink|lexists)\("
                 r"|\bos\.walk\(|\bglob\.i?glob\(")


def raw_sites() -> dict:
    found: dict = {}
    for pattern in GUARDED:
        for path in sorted(REPO.glob(pattern)):
            rel = path.relative_to(REPO).as_posix()
            if rel in SKIPPED:
                continue
            for number, line in enumerate(path.read_text().splitlines(), 1):
                code = line.split("#", 1)[0]
                if RAW.search(code):
                    found.setdefault((rel, code.strip()), []).append(number)
    return found


def test_no_new_raw_pathlib_predicate_in_a_decision_module():
    """A new ``.exists()``/``.is_dir()``/``.is_file()``/``.is_symlink()``/
    ``.glob()``/``.rglob()``/``.iterdir()`` in ``src/agent_compute``, ``bin``
    or ``install.py`` fails here until it uses ``fsprobe`` or is reviewed
    into ``ALLOWED`` with its reason."""
    found = raw_sites()
    new = {k: v for k, v in found.items() if k not in ALLOWED}
    assert not new, (
        "raw pathlib predicates are fail-open for a path that cannot be "
        "examined (Python 3.14; A49): use agent_compute.fsprobe, or review "
        "the site into ALLOWED with its reason:\n" + "\n".join(
            f"  {f}:{lines}: {text}" for (f, text), lines in new.items()))
    for key, (count, why) in ALLOWED.items():
        assert why.strip(), key
        assert key in found, f"ALLOWED entry no longer in the code: {key}"
        assert len(found[key]) == count, (key, found[key], count)


def test_the_guard_finds_a_planted_site(tmp_path, monkeypatch):
    """The scan itself: a decision module with a new predicate is seen."""
    fake = tmp_path / "src" / "agent_compute"
    fake.mkdir(parents=True)
    (fake / "newmod.py").write_text(
        "def f(p):\n    return p.exists()  # comment .is_dir( ignored\n")
    monkeypatch.setitem(globals(), "REPO", tmp_path)
    assert raw_sites() == {("src/agent_compute/newmod.py",
                            "return p.exists()"): [2]}
