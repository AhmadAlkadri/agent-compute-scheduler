"""Campaign S4: container reproductions through the acs-guard backend.

The guard.py reproductions in ``test_cleanup_evidence.py``,
``test_container_job_evidence.py`` and ``test_children_docker.py`` pin
guard.py's own defects (the legacy profile since G2b: a dash child that
survives a cancel, a walltime kill that reaches only the docker client, a
same-session child ACS must catch).  With the internal guard those defects
are fixed (S3), so the same scenarios are checked here for the safe
outcome, end to end through ACS (disposable labelled containers only):

* a fast job leaving a same-session child: the guard says
  ``cleanup-unverified``, ACS blocks, its marker holds the host, and once the
  child is gone ``reconcile`` resolves the block and (R3) the marker;
* a walltime kill stops the whole in-container job: ``timed-out``, clean,
  no block, no marker.

The dash-cancel counterpart is ``test_guard_lifecycle.py::
test_acs_cancel_of_an_internal_guard_container_job_kills_its_children``.
The ``hang`` pid-read shim of ``test_desired_f_…`` does not apply: the
internal guard's execs run with ``-w /`` (the shim matches ``exec C cat``)
and a hung read is "not yet" (S3 table, "Found here").
"""

from __future__ import annotations

import json
import os
import secrets
import shutil
import subprocess

import pytest

from conftest import Env
from test_children_docker import needs_docker
from test_cleanup_evidence import (blocked, guard_record,  # noqa: F401
                                   make_container, report, scan,
                                   submit_container)


@pytest.fixture
def acs_env():
    env = Env(queue=False)
    yield env
    env.cleanup()


@needs_docker
def test_same_session_survivor_blocks_and_reconcile_resolves_the_marker(
        acs_env, make_container):
    env = acs_env
    name = make_container()
    env.set_container(name)
    tag = f"acs-s4-surv-{secrets.token_hex(3)}"
    script = f"( exec -a {tag} sleep 30 ) >/dev/null 2>&1 </dev/null & exit 0"
    job = submit_container(env, name, ["bash", "-c", script])
    state = env.wait_outcome(job["job_id"], timeout=90)
    record = guard_record(env, job["job_id"])
    report("s4-survivor", outcome=state.get("outcome"),
           guard_outcome=state.get("guard_outcome"),
           cleanup=state.get("cleanup"), guard_cleanup=record.get("cleanup"))
    assert scan(name, tag), "precondition: the same-session child runs"
    assert state["outcome"] == "unknown" and blocked(env)
    assert state["guard_outcome"] == "cleanup-unverified"
    assert state["exit_code"] == 0                   # the workload's result
    status = json.loads(env.acs("status", "--json").stdout)
    marker = status["admission"]["inflight_marker"]
    assert marker and marker["acs_job"] == job["job_id"], status
    first = json.loads(env.acs("reconcile", "--json").stdout)
    assert first["blocked"] is True
    assert first["inflight_marker"]["resolved"] is False
    env.wait_for(lambda: not scan(name, tag), timeout=60,
                 what="survivor exit")
    second = json.loads(env.acs("reconcile", "--json").stdout)
    report("s4-survivor-reconcile", first=first, second=second)
    assert second["blocked"] is False, second
    assert second["inflight_marker"]["resolved"] is True
    assert not (env.guard_home / "inflight.json").exists()
    events = [h.get("event") for h in env.state_of(job["job_id"])["history"]]
    assert "reconciled" in events and "inflight-marker-cleared" in events


@needs_docker
def test_walltime_kill_stops_the_whole_container_job(acs_env, make_container):
    env = acs_env
    name = make_container()
    env.set_container(name)
    tag = f"acs-s4-wall-{secrets.token_hex(3)}"
    code = ("import subprocess, time; subprocess.Popen(['python3', '-c', "
            f"'import time; time.sleep(75)  # {tag}-child']); "
            f"time.sleep(75)  # {tag}")
    job = submit_container(env, name, ["python3", "-c", code],
                           timeout_min="0.1")
    state = env.wait_outcome(job["job_id"], timeout=90)
    record = guard_record(env, job["job_id"])
    report("s4-walltime", outcome=state.get("outcome"),
           cleanup=state.get("cleanup"), guard_cleanup=record.get("cleanup"),
           alive=scan(name, tag))
    assert record["outcome"] == "killed-walltime"
    assert state["outcome"] == "timed-out", state
    assert state["cleanup"]["clean"] is True
    assert record["cleanup"]["clean"] is True
    assert not scan(name, tag)
    assert not blocked(env)
    assert not (env.guard_home / "inflight.json").exists()


def engine_pid_read_shim(env, tmp_path):
    """A PATH whose ``docker`` fails only the internal guard's read of its
    in-container pid file (``docker exec -w / C cat /tmp/acs-guard-*``;
    G2a: the neutral name, ``/tmp/smc-guard-*`` before it), so
    ``container_pid`` is never captured (the A20 class: 30 s of failed
    reads).  Everything else is the real docker."""
    real = shutil.which("docker")
    bindir = tmp_path / "shim"
    bindir.mkdir(exist_ok=True)
    shim = bindir / "docker"
    shim.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = exec ] && [ "$2" = -w ] && [ "$5" = cat ]; then\n'
        '  case "$6" in /tmp/acs-guard-*|/tmp/smc-guard-*) '
        "echo 'cat: simulated unreadable pid file' >&2; exit 1;; esac\n"
        "fi\n"
        f'exec "{real}" "$@"\n')
    shim.chmod(0o755)
    return env.env(PATH=f"{bindir}:{os.environ['PATH']}")


@needs_docker
def test_c3_env_i_leader_with_an_unread_pid_file_is_never_cleaned(
        acs_env, make_container, tmp_path):
    """C3 (G1): with the pid file never read, the guard's only marked
    process is the `setsid -w` parent of an `exec env -i` leader.  A cancel
    must not kill that parent (the only link to the leader): the job ends
    `unknown`, the host is blocked, the in-flight marker stays, and
    reconcile names the leader among `unmarked_members`.  Unit
    counterpart: tests/test_engine_container_lineage.py."""
    env = acs_env
    name = make_container()
    env.set_container(name)
    path_env = engine_pid_read_shim(env, tmp_path)
    tag = f"acs-c3-{secrets.token_hex(3)}"
    job = submit_container(env, name,
                           ["env", "-i", "bash", "-c",
                            f"exec -a {tag} sleep 600"],
                           timeout_min="5", path_env=path_env)
    job_id = job["job_id"]
    try:
        env.wait_for(lambda: [r for r in scan(name, tag)
                              if r["job"] is None], timeout=90,
                     what="the env -i leader runs")
        env.wait_for(lambda: guard_record(env, job_id).get("pid"),
                     timeout=120, what="the guard's record after launch")
        record = guard_record(env, job_id)
        assert record.get("container_pid") is None, \
            "precondition: the pid file was never read"
        cancel = env.acs("cancel", job_id, "--json", env=path_env,
                         timeout=240)
        state = env.wait_outcome(job_id, timeout=180)
        record = guard_record(env, job_id)
        # the tag is also in the marked `setsid -w` parent's command line:
        # the leader is the tagged process without the marker
        alive = [r for r in scan(name, tag) if r["job"] is None]
        report("c3-env-i", cancel=cancel.stdout, outcome=state.get("outcome"),
               guard_outcome=state.get("guard_outcome"),
               guard_cleanup=record.get("cleanup"),
               cleanup=state.get("cleanup"), alive=alive)
        assert alive, "precondition: the unmarked leader still runs"
        assert record.get("outcome") == "cleanup-unverified", record
        assert record["cleanup"]["clean"] is not True
        assert alive[0]["pid"] in \
            (record["cleanup"].get("unmarked_survivors") or []), record
        assert state["outcome"] == "unknown", state
        assert blocked(env)
        assert (env.guard_home / "inflight.json").exists(), \
            "the in-flight marker must stay while the leader lives"
        rec = json.loads(env.acs("reconcile", "--json").stdout)
        report("c3-env-i-reconcile", reconcile=rec)
        assert rec["blocked"] is True, rec
        mine = [u for u in rec["unresolved"] if u.get("job_id") == job_id]
        assert mine, rec
        unmarked = (mine[0].get("cleanup") or {}).get("unmarked_members") \
            or []
        assert alive[0]["pid"] in [m["pid"] for m in unmarked], mine[0]
    finally:
        # the unmarked leader: the container goes, and the leader with it
        subprocess.run([shutil.which("docker") or "docker", "rm", "-f", name],
                       capture_output=True)
