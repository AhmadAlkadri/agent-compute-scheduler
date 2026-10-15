"""C3 (boundary audit; campaign G slice G1): the internal guard's container
cancel must not erase the D4 lineage evidence.

When the guard never read the in-container pid file (30 s of failed reads,
the A20 failure class), the only process its helper can select is the
marked ``setsid -w`` parent of the workload leader.  After ``exec env -i
CMD`` that leader carries no marker and leads its own session: the parent
is the only link to it.  The helper killed the parent; the guard's re-scan
and ACS's evidence then both read clean while the leader ran on, the job
got a definite outcome and the in-flight marker was cleared.  ACS's own
signal plan skips such a parent ("unmarked child; handle manually").

These tests run the engine's REAL helper script (``engine._CONTAINER_GROUP``)
against a synthetic ``/proc`` (no Docker): its ``open``, ``os``, ``signal``
and ``sys`` are fakes, and every signal it sends is applied to the table
(the process dies, its children are reparented to pid 1).  The
disposable-container counterpart is
``test_acs_guard_containers.py::test_c3_env_i_leader_with_an_unread_pid_file_is_never_cleaned``.
Basis: the private development record (register item C3).
"""

from __future__ import annotations

import builtins
import contextlib
import io
import json
import signal
import subprocess
import types

from agent_compute.guard import engine

JOB = "case/20261005T000000Z"
MARK = b"SMC_GUARD_JOB=" + JOB.encode()
TERM, KILL = int(signal.SIGTERM), int(signal.SIGKILL)


def row(ppid, pgrp, sid, ticks, marked, reuse=None):
    """One process.  ``reuse``: after that many reads of its stat, the pid
    belongs to another (unmarked) process with other start ticks."""
    return {"ppid": ppid, "pgrp": pgrp, "sid": sid, "ticks": ticks,
            "marked": marked, "reuse": reuse}


class FakeContainer:
    def __init__(self, rows: dict):
        self.rows = {pid: dict(r) for pid, r in rows.items()}
        self.reads: dict = {}
        self.sent: list = []

    # -- the synthetic /proc ------------------------------------------------
    def _proc(self, pid: int) -> dict:
        if pid not in self.rows:
            raise FileNotFoundError(f"/proc/{pid}")
        return self.rows[pid]

    def _stat(self, pid: int) -> str:
        r = self._proc(pid)
        self.reads[pid] = self.reads.get(pid, 0) + 1
        if r["reuse"] is not None and self.reads[pid] > r["reuse"]:
            r.update(ticks=r["ticks"] + 777, marked=False, reuse=None)
        fields = ["S", r["ppid"], r["pgrp"], r["sid"]] + [0] * 15 + \
            [r["ticks"]]
        return f"{pid} (x) " + " ".join(map(str, fields))

    def open(self, path, mode="r", *_a, **_k):
        parts = str(path).split("/")
        if len(parts) < 4 or parts[1] != "proc" or not parts[2].isdigit():
            raise FileNotFoundError(path)
        pid, leaf = int(parts[2]), parts[3]
        if leaf == "stat" and len(parts) == 4:
            return io.StringIO(self._stat(pid))
        if leaf == "environ":
            r = self._proc(pid)
            data = b"PATH=/bin\0" + (MARK + b"\0" if r["marked"] else b"")
            return io.BytesIO(data)
        raise FileNotFoundError(path)

    def listdir(self, path):
        if str(path) == "/proc":
            return [str(pid) for pid in self.rows] + ["self", "stat"]
        raise FileNotFoundError(path)

    # -- signals act on the table ---------------------------------------------
    def _die(self, pid: int) -> None:
        self.rows.pop(pid, None)
        for r in self.rows.values():
            if r["ppid"] == pid:
                r["ppid"] = 1

    def kill(self, pid, sig):
        self.sent.append(["kill", pid, sig])
        if pid not in self.rows:
            raise ProcessLookupError(pid)
        if sig:
            self._die(pid)

    def killpg(self, pgid, sig):
        self.sent.append(["killpg", pgid, sig])
        victims = [p for p, r in self.rows.items() if r["pgrp"] == pgid]
        if not victims:
            raise ProcessLookupError(pgid)
        for pid in victims:
            self._die(pid)

    # -- run the engine's helper ------------------------------------------------
    def helper(self, pgid: int = 0, ticks: str = "", sig: int = 0) -> dict:
        fake_os = types.SimpleNamespace(
            listdir=self.listdir, getpid=lambda: 999, kill=self.kill,
            killpg=self.killpg)
        fake_signal = types.SimpleNamespace(
            alarm=lambda n: 0, signal=lambda *a: None,
            SIGALRM=signal.SIGALRM, SIG_DFL=signal.SIG_DFL)
        fake_sys = types.SimpleNamespace(
            argv=["-c", JOB, str(pgid), ticks or "", str(sig)])
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            return {"os": fake_os, "signal": fake_signal,
                    "sys": fake_sys}.get(name) or \
                real_import(name, *args, **kwargs)

        fake_builtins = dict(vars(builtins), open=self.open,
                             __import__=fake_import)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            exec(engine._CONTAINER_GROUP,
                 {"__builtins__": fake_builtins, "__name__": "__main__"})
        return json.loads(out.getvalue().strip().splitlines()[-1])


INIT = {1: row(0, 1, 1, 10, False)}
# pid 20: the guard's marked `setsid -w` (its own session under docker
# exec); pid 21: its child, the workload leader after `exec env -i CMD`
# (unmarked, leads session 21).
ENV_I = {**INIT, 20: row(1, 20, 20, 5000, True),
         21: row(20, 21, 21, 5001, False)}


def pids(answer) -> list:
    return sorted(int(m["pid"]) for m in answer["members"])


def test_c3_helper_never_signals_a_marked_parent_of_an_unmarked_leader():
    box = FakeContainer(ENV_I)
    answer = box.helper(pgid=0, sig=TERM)           # pid never captured
    assert not any(target in (20, -20) for _, target, _s in box.sent), \
        box.sent
    assert 20 in box.rows and 21 in box.rows
    assert 20 in pids(answer) and 21 in pids(answer), answer
    skipped = answer.get("skipped") or []
    assert any(s[0] == 20 and "unmarked" in s[1] for s in skipped), answer


def test_c3_helper_lists_the_unmarked_descendant_without_a_signal():
    box = FakeContainer(ENV_I)
    answer = box.helper(pgid=0, sig=0)
    assert pids(answer) == [20, 21], answer
    assert [int(m["pid"]) for m in answer.get("unmarked") or []] == [21]


class FakeClient:
    """The host-side ``docker exec`` client: it lives while the in-container
    `setsid -w` parent does."""
    def __init__(self, box: FakeContainer):
        self.box, self.returncode = box, None

    def poll(self):
        if 20 in self.box.rows:
            return None
        self.returncode = 0
        return 0

    def wait(self, timeout=None):
        if self.poll() is None:
            raise subprocess.TimeoutExpired("docker exec", timeout)
        return 0

    def kill(self):
        pass                    # the host client only; not the container

    send_signal = lambda self, sig: None  # noqa: E731


def fake_job(box: FakeContainer, monkeypatch) -> types.SimpleNamespace:
    monkeypatch.setattr(
        engine, "container_group",
        lambda container, job_id, pgid, ticks, sig=0, markers=None:
        box.helper(int(pgid or 0), ticks or "", int(sig)))
    job = types.SimpleNamespace(
        venue="container", container="c", job_id=JOB, container_pid=None,
        container_ticks=None, grace=0.2, paused_since=None,
        proc=FakeClient(box), cleanup=None, markers=["ACS_JOB_MARKER"])
    for name in ("_signal_job", "_live_members", "_cleanup_evidence",
                 "_wait_until_gone", "_stop_job"):
        setattr(job, name, types.MethodType(getattr(engine.Job, name), job))
    return job


def test_c3_stop_job_is_never_clean_while_the_unmarked_leader_lives(
        monkeypatch):
    box = FakeContainer(ENV_I)
    job = fake_job(box, monkeypatch)
    job._stop_job()
    assert 21 in box.rows, "precondition: the unmarked leader still runs"
    assert job.cleanup["clean"] is not True, job.cleanup
    assert 21 in (job.cleanup.get("survivors") or []), job.cleanup


def test_c3_helper_skips_a_target_whose_pid_changed_between_its_scans():
    """A marked process whose pid is reused between the helper's first and
    second scan (other start ticks, no marker) is not signalled.  (The
    fail-first record names this test
    ``test_c3_helper_reproves_a_target_right_before_the_signal``; renamed in
    the G1 review, M2a: the second scan catches it, not the last-moment
    re-proof, which the next test covers.)"""
    box = FakeContainer({**INIT, 30: row(1, 30, 30, 6000, True, reuse=1)})
    box.helper(pgid=0, sig=TERM)
    assert ["kill", 30, TERM] not in box.sent, box.sent
    assert ["killpg", 30, TERM] not in box.sent, box.sent


def test_c3_helper_reproves_a_target_right_before_the_signal():
    """G1 review M2a: both scans agree (the pid is reused only after them,
    on the third stat read), so only the re-proof right before ``os.kill``
    can catch it: no signal, reported 'not proven right before the
    signal'."""
    box = FakeContainer({**INIT, 30: row(1, 30, 30, 6000, True, reuse=2)})
    answer = box.helper(pgid=0, sig=TERM)
    assert box.sent == [], box.sent
    assert [30, "not proven right before the signal"] in answer["skipped"], \
        answer


# G1 review M2b: the pid was captured (21, ticks 5001); 23 is in its group;
# 22 is a child of 21 that called setsid() and dropped the marker, so only
# its parent links it to the job.  The recorded group must not be killed
# (that would kill 21, the only link); 20 and 21 are held back; only 23,
# which has no such descendant, is signalled, by pid.
ESCAPED = {**INIT, 20: row(1, 20, 20, 5000, True),
           21: row(20, 21, 21, 5001, True),
           22: row(21, 22, 22, 5002, False),
           23: row(21, 21, 21, 5003, True)}


def test_c3_helper_holds_back_a_captured_group_with_an_escaped_descendant():
    box = FakeContainer(ESCAPED)
    answer = box.helper(pgid=21, ticks="5001", sig=TERM)
    assert not any(kind == "killpg" for kind, _p, _s in box.sent), box.sent
    assert box.sent == [["kill", 23, TERM]], box.sent
    held = {s[0]: s[1] for s in answer["skipped"]}
    assert "unmarked" in held.get(20, "") and "unmarked" in held.get(21, ""), \
        answer
    assert [m["pid"] for m in answer["unmarked"]] == [22], answer
    assert {20, 21, 22} <= set(box.rows)


def test_c3_stop_job_with_an_escaped_descendant_is_not_clean(monkeypatch):
    box = FakeContainer(ESCAPED)
    job = fake_job(box, monkeypatch)
    job.container_pid, job.container_ticks = 21, "5001"
    job._stop_job()
    assert 23 not in box.rows and {20, 21, 22} <= set(box.rows), box.rows
    assert job.cleanup["clean"] is not True, job.cleanup
    assert job.cleanup.get("unmarked_survivors") == [22], job.cleanup
    assert sorted(job.cleanup["survivors"]) == [20, 21, 22], job.cleanup
    assert {s[0] for s in job.cleanup.get("not_signalled") or []} >= \
        {20, 21}, job.cleanup


# -- behaviour the fix must keep ------------------------------------------------------

def test_c3_keep_marked_job_without_unmarked_descendants_is_stopped(
        monkeypatch):
    box = FakeContainer({**INIT, 20: row(1, 20, 20, 5000, True),
                         21: row(20, 21, 21, 5001, True),
                         22: row(21, 21, 21, 5002, True)})
    job = fake_job(box, monkeypatch)
    job._stop_job()
    assert set(box.rows) == {1}, box.rows
    assert job.cleanup["clean"] is True and job.cleanup["verified"] is True


def test_c3_keep_captured_group_is_killed_with_its_unmarked_members(
        monkeypatch):
    """The pid was captured (the normal case): an unmarked process that
    stayed in the recorded group (a child started with its own environment)
    is still covered by the group, so the group kill proceeds."""
    box = FakeContainer({**INIT, 20: row(1, 20, 20, 5000, True),
                         21: row(20, 21, 21, 5001, True),
                         22: row(21, 21, 21, 5002, False)})
    job = fake_job(box, monkeypatch)
    job.container_pid, job.container_ticks = 21, "5001"
    job._stop_job()
    assert ["killpg", 21, TERM] in box.sent, box.sent
    assert set(box.rows) == {1}, box.rows
    assert job.cleanup["clean"] is True, job.cleanup


def test_c3_keep_recycled_leader_pid_is_not_killed():
    """A captured pid whose start ticks changed (another process now) is a
    stranger: its group is never signalled."""
    box = FakeContainer({**INIT, 21: row(1, 21, 21, 9999, False)})
    answer = box.helper(pgid=21, ticks="5001", sig=TERM)
    assert answer["stranger"] is True
    assert box.sent == [] and pids(answer) == []
