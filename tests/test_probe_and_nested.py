"""Campaign S5a: probes through ACS, submitting from inside a running ACS
job, and the `run` exit-code contract that project wrappers forward to.

* ``submit/run --probe [--probe-s S]`` runs the guard's ``probe`` mode
  (the engine's ``probe``; guard.py's under smc-guard until G2b retired
  it), through the same queue slot and admission as ``run``.  A probe
  stopped at its limit (guard outcome ``probe-stopped``, guard exit 0) is
  ``succeeded``; an unmeasured case needs no reserve for a probe (the
  guard's own rule), while a ``run`` of it is refused before it starts.
* From inside a running ACS job (the runner exports ``ACS_JOB_ID`` to the
  guard, whose host workload inherits it; the guard's ``SMC_GUARD_JOB``
  marks any guarded job): ``submit`` is accepted and queued behind the
  current job, with ``parent_job`` recorded as metadata; ``run`` and
  ``wait`` on an unfinished job of this host are refused (they would wait
  on the slot the current job holds).  The child's environment carries
  neither marker, so its own guard does not pass straight through.
* ``run`` exits with the runner's code for the outcome (``runner.RC``), 2
  when nothing was accepted.
"""

from __future__ import annotations

import argparse
import json
import secrets
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from conftest import BIN, WORKLOAD, Env, events
from test_children_docker import needs_docker
from test_cleanup_evidence import (blocked, guard_record,  # noqa: F401
                                   make_container, scan)

from agent_compute import cli
from agent_compute import config as cfgmod
from agent_compute import runner as runmod
from agent_compute.state import JobStore, private_dir, write_json

PY = sys.executable
# the one backend since G2b (the fixtures ran each case under both kinds)
BACKENDS = ("acs-guard",)


@pytest.fixture(params=[(b, q) for b in BACKENDS for q in ("direct", "queue")],
                ids=lambda p: f"{p[0]}-{p[1]}")
def env_both(request):
    """With and without the queue."""
    backend, mode = request.param
    env = Env(queue=mode == "queue")
    yield env
    env.cleanup()


@pytest.fixture(params=BACKENDS)
def env_backend(request):
    env = Env(queue=False)
    yield env
    env.cleanup()


def attempt_argv(env: Env, job_id: str) -> str:
    return (env.state / "jobs" / job_id / "output.log").read_text()


def baseline(env: Env, project="alpha") -> dict:
    path = env.state / "projects" / project / "baseline.json"
    return json.loads(path.read_text()) if path.exists() else {}


# --- probes -----------------------------------------------------------------------

def test_probe_of_an_unmeasured_case_needs_no_reserve_and_is_stopped(env_both):
    """Fail-first: ACS had no probe (``--probe`` was an unknown option)."""
    env = env_both
    out = env.root / "probe.jsonl"
    result = env.acs("submit", "--json", "--case", "fresh", "--timeout-min",
                     "1", "--probe", "--probe-s", "2", "--cwd",
                     str(env.projects["alpha"]), "--", PY, str(WORKLOAD),
                     "--out", str(out), "--sleep", "60")
    assert result.returncode == 0, result.stdout + result.stderr
    job = json.loads(result.stdout)["job_id"]
    state = env.wait_outcome(job, timeout=90)
    record = guard_record(env, job)
    assert record.get("outcome") == "probe-stopped", record
    assert record.get("probe") is True
    assert state["outcome"] == "succeeded", state
    assert state["guard_outcome"] == "probe-stopped"
    assert state["cleanup"]["clean"] is True
    assert not blocked(env)
    log = attempt_argv(env, job)
    assert "'probe'" in log and "'--probe-s', '2'" in log, log
    row = baseline(env)["cases"]["alpha.fresh"]
    assert row["kind"] == "probe", row
    view = json.loads(env.acs("status", job, "--json").stdout)["job"]
    assert view["probe"] == {"probe_s": 2.0}, view
    assert [e["event"] for e in events(out)] == ["start"]   # stopped


def test_run_of_an_unmeasured_case_without_reserve_is_refused(env_backend):
    """The contrast: the guard's rule for a non-probe job, never started.
    ``run`` exits with RC['refused'] (11)."""
    env = env_backend
    out = env.root / "never.jsonl"
    result = env.acs("run", "--json", "--case", "fresh", "--timeout-min", "1",
                     "--cwd", str(env.projects["alpha"]), "--", PY,
                     str(WORKLOAD), "--out", str(out))
    view = json.loads(result.stdout)
    assert result.returncode == runmod.RC["refused"] == 11, result.stderr
    assert view["outcome"] == "refused" and view["guard_outcome"] is None
    assert "no measured peak" in view["reason"]
    assert "through ACS: submit it with `--probe` first" in view["reason"]
    assert not out.exists()


def test_run_probe_exits_zero_with_the_probe_result(env_backend):
    env = env_backend
    result = env.acs("run", "--json", "--case", "p", "--timeout-min", "1",
                     "--probe", "--probe-s", "2", "--reserve-gb", "0.5",
                     "--cwd", str(env.projects["alpha"]), "--", PY,
                     str(WORKLOAD), "--out", str(env.root / "o"), "--sleep",
                     "60")
    view = json.loads(result.stdout)
    assert result.returncode == 0, result.stdout + result.stderr
    assert view["outcome"] == "succeeded"
    assert view["guard_outcome"] == "probe-stopped"


def test_a_probe_that_finishes_first_is_a_measurement(env_backend):
    env = env_backend
    result = env.acs("run", "--json", "--case", "quick", "--timeout-min", "1",
                     "--probe", "--cwd", str(env.projects["alpha"]), "--", PY,
                     str(WORKLOAD), "--out", str(env.root / "o"))
    view = json.loads(result.stdout)
    assert result.returncode == 0, result.stdout + result.stderr
    assert view["guard_outcome"] == "completed"
    assert view["probe"] == {"probe_s": 60.0}                 # guard default
    assert baseline(env)["cases"]["alpha.quick"]["kind"] == "measured"


@pytest.mark.parametrize("extra, message", [
    (["--probe-s", "5"], "--probe-s needs --probe"),
    (["--probe", "--probe-s", "0"], "--probe-s must be"),
    (["--probe", "--probe-s", "61"], "--probe-s must be"),
    (["--probe"], "--probe-s must be"),     # the 60 s default > 0.5 min
])
def test_probe_options_are_screened(env_direct, extra, message):
    env = env_direct
    timeout = "0.5" if extra == ["--probe"] else "1"
    result = env.acs("submit", "--json", "--case", "p", "--timeout-min",
                     timeout, *extra, "--cwd", str(env.projects["alpha"]),
                     "--", PY, str(WORKLOAD), "--out", str(env.root / "o"))
    assert result.returncode == cli.EXIT_NOT_ACCEPTED, result.stdout
    assert message in result.stderr, result.stderr
    assert not list((env.state / "jobs").glob("acs-*/manifest.json")) \
        if (env.state / "jobs").exists() else True


def _manifest_job(env: Env, config, *, probe: bool) -> str:
    store = JobStore(config.state_dir)
    private_dir(config.state_dir)
    private_dir(config.jobs_dir)
    job_id, _ = cli._reserve(store, config.host, None)
    args = argparse.Namespace(
        cwd=str(env.projects["alpha"]), project=None, timeout_min=1.0,
        threads=1, dense_threads=None, container=None, env=[],
        container_workdir=None, lock_wait=30, input=[], request_id=None,
        owner=None, case="t", slice=None, units=None, reserve_gb=0.5,
        probe=probe, probe_s=None)
    manifest = cli.build_manifest(config, args, [PY, str(WORKLOAD), "--out",
                                                 str(env.root / "x")], job_id)
    write_json(store.job_dir(job_id) / "manifest.json", manifest)
    return job_id


@pytest.mark.parametrize("probe, expected", [(True, "succeeded"),
                                             (False, "unknown")])
def test_probe_stopped_is_a_result_only_for_a_probe(env_backend, probe,
                                                    expected):
    """``probe-stopped`` means the guard stopped the job at the probe limit:
    a probe's success, but never a result for a job ACS did not ask to
    probe (before S5a it was ``unknown`` for every job)."""
    env = env_backend
    config = cfgmod.load(env.config)
    job = _manifest_job(env, config, probe=probe)
    runner = runmod.Runner(config, job)
    attempt = env.state / "jobs" / job / "guard" / "attempt-1"
    attempt.mkdir(parents=True)
    (attempt / "samples.csv").write_text("t\n")
    dead = subprocess.Popen(["true"])
    dead.wait()
    (attempt / "guard.json").write_text(json.dumps({
        "schema": "smc-guard-record/1", "pid": dead.pid,
        "job_id": "alpha.t/20260930T000000Z",
        "started_utc": "2026-09-30T00:00:00Z", "exit_code": -15,
        "outcome": "probe-stopped", "probe": probe}))
    runner.classify(0, attempt)
    state = env.state_of(job)
    assert state["outcome"] == expected, state
    assert state["guard_outcome"] == "probe-stopped"
    assert JobStore(config.state_dir).block(config.host) is None


@needs_docker
def test_container_probe_is_stopped_and_clean(env_backend, make_container):
    env = env_backend
    name = make_container()
    env.set_container(name)
    tag = f"acs-s5a-probe-{secrets.token_hex(3)}"
    result = env.acs("submit", "--json", "--case", "cp", "--timeout-min", "1",
                     "--probe", "--probe-s", "3", "--reserve-gb", "0.5",
                     "--cwd", str(env.projects["alpha"]), "--container", name,
                     "--", "python3", "-c",
                     f"import time; time.sleep(60)  # {tag}")
    assert result.returncode == 0, result.stdout + result.stderr
    job = json.loads(result.stdout)["job_id"]
    state = env.wait_outcome(job, timeout=120)
    record = guard_record(env, job)
    assert record.get("outcome") == "probe-stopped", record
    assert state["outcome"] == "succeeded", state
    assert state["cleanup"]["clean"] is True
    assert not scan(name, tag)
    assert not blocked(env)
    assert "'probe'" in attempt_argv(env, job)


# --- submitting from inside a running ACS job -------------------------------------

STEP = textwrap.dedent("""\
    import json, os, subprocess, sys, time
    py, acs, config, report, child_cmd = sys.argv[1:6]
    child = json.loads(child_cmd)
    def call(*args):
        r = subprocess.run([py, acs, "--config", config, *args],
                           capture_output=True, text=True, timeout=120)
        return {"rc": r.returncode, "out": r.stdout, "err": r.stderr}
    facts = {"env": {k: v for k, v in os.environ.items()
                     if k.startswith(("ACS_", "SMC_GUARD_"))}}
    facts["submit"] = call("submit", "--json", "--case", "b",
                           "--timeout-min", "1", "--reserve-gb", "0.5",
                           "--cwd", os.getcwd(), "--", *child)
    child_id = json.loads(facts["submit"]["out"]).get("job_id")
    facts["run"] = call("run", "--json", "--case", "c", "--timeout-min", "1",
                        "--reserve-gb", "0.5", "--cwd", os.getcwd(), "--",
                        py, "-c", "pass")
    facts["wait"] = call("wait", child_id, "--json", "--timeout", "5")
    time.sleep(3)
    facts["ended"] = time.time()
    with open(report, "w") as handle:
        json.dump(facts, handle)
    """)


def test_submit_from_inside_a_job_queues_behind_it(env_both):
    """Fail-first: ``run``/``wait`` from inside a job were accepted and sat
    on the slot the job holds; nothing recorded the parent; the workload
    had no ACS job marker."""
    env = env_both
    script = env.root / "step.py"
    script.write_text(STEP)
    report = env.root / "report.json"
    child_out = env.root / "child.jsonl"
    child = [PY, str(WORKLOAD), "--out", str(child_out)]
    result = env.acs("submit", "--json", "--case", "a", "--timeout-min", "2",
                     "--reserve-gb", "0.5", "--cwd",
                     str(env.projects["alpha"]), "--", PY, str(script), PY,
                     str(BIN), str(env.config), str(report),
                     json.dumps(child))
    assert result.returncode == 0, result.stdout + result.stderr
    a = json.loads(result.stdout)["job_id"]
    a_state = env.wait_outcome(a, timeout=120)
    assert a_state["outcome"] == "succeeded", a_state
    facts = json.loads(report.read_text())
    # inside A: its own ACS job id and its guard's marker
    assert facts["env"].get("ACS_JOB_ID") == a, facts["env"]
    marker = "ACS_JOB_MARKER"
    assert facts["env"].get(marker, "").startswith("alpha.a/")
    # submit accepted; run and wait refused with the actionable message
    assert facts["submit"]["rc"] == 0, facts["submit"]
    b = json.loads(facts["submit"]["out"])["job_id"]
    for key in ("run", "wait"):
        assert facts[key]["rc"] == cli.EXIT_NOT_ACCEPTED, facts[key]
        assert "would wait on the slot this job holds" in facts[key]["out"], \
            facts[key]
    ids = [p.name for p in (env.state / "jobs").glob("acs-*")]
    assert len(ids) == 2, ids               # A and B; `run` made none
    # B ran after A ended, as an independent job
    b_state = env.wait_outcome(b, timeout=120)
    assert b_state["outcome"] == "succeeded", b_state
    man = json.loads((env.state / "jobs" / b / "manifest.json").read_text())
    assert man["parent_job"]["acs_job"] == a
    assert man["parent_job"]["guard_job"].startswith("alpha.a/")
    assert not any(k.startswith(("SMC_GUARD_", "ACS_JOB")) for k in man["env"])
    start = events(child_out)[0]
    assert start["t"] > facts["ended"], (start["t"], facts["ended"])
    # its own guard ran it (no pass-through): its own markers
    assert start["env"][marker].startswith("alpha.b/"), start["env"]
    assert start["env"]["ACS_JOB_ID"] == b
    view = json.loads(env.acs("status", b, "--json").stdout)["job"]
    assert view["parent_job"]["acs_job"] == a


def _marked(env: Env, **markers) -> dict:
    return env.env(**markers)


def test_run_and_wait_are_allowed_once_the_marked_job_has_ended(env_direct):
    """A process that outlived its job (a detached supervisor) holds no
    slot: once that job has an outcome it may run and wait."""
    env = env_direct
    first = env.submit(case="a", workload_args=("--out", str(env.root / "f")))
    env.wait_outcome(first["job_id"])
    marked = _marked(env, ACS_JOB_ID=first["job_id"],
                     SMC_GUARD_JOB="alpha.a/20260930T000000Z")
    result = env.acs("run", "--json", "--case", "b", "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--cwd",
                     str(env.projects["alpha"]), "--", PY, str(WORKLOAD),
                     "--out", str(env.root / "o"), env=marked)
    assert result.returncode == 0, result.stdout + result.stderr
    waited = env.acs("wait", first["job_id"], "--json", env=marked)
    assert waited.returncode == 0, waited.stdout + waited.stderr


def test_wait_on_a_finished_job_is_allowed_from_inside_a_job(env_direct):
    env = env_direct
    done = env.submit(case="a", workload_args=("--out", str(env.root / "d")))
    env.wait_outcome(done["job_id"])
    running = env.submit(case="r", workload_args=(
        "--out", str(env.root / "r"), "--sleep", "30"))
    env.wait_phase(running["job_id"], "executing")
    marked = _marked(env, ACS_JOB_ID=running["job_id"])
    ok = env.acs("wait", done["job_id"], "--json", env=marked)
    assert ok.returncode == 0, ok.stdout + ok.stderr
    refused = env.acs("wait", running["job_id"], "--json", env=marked)
    assert refused.returncode == cli.EXIT_NOT_ACCEPTED, refused.stdout
    assert "would wait on the slot this job holds" in refused.stdout
    env.acs("cancel", running["job_id"])


def test_a_guard_marker_alone_refuses_run(env_direct):
    """Inside a job guarded without an ACS job id (a legacy guard.py job,
    or an ACS job of an older version) nothing proves the slot free."""
    env = env_direct
    marked = _marked(env, SMC_GUARD_JOB="alpha.x/20260930T000000Z")
    result = env.acs("run", "--json", "--case", "b", "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--cwd",
                     str(env.projects["alpha"]), "--", PY, str(WORKLOAD),
                     "--out", str(env.root / "o"), env=marked)
    assert result.returncode == cli.EXIT_NOT_ACCEPTED, result.stdout
    assert "would wait on the slot this job holds" in result.stdout
    assert not (env.state / "jobs").exists() or \
        not list((env.state / "jobs").glob("acs-*"))
    # submit stays allowed, and records the guard marker as its parent
    sub = env.acs("submit", "--json", "--case", "b", "--timeout-min", "1",
                  "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]),
                  "--", PY, str(WORKLOAD), "--out", str(env.root / "o2"),
                  env=marked)
    assert sub.returncode == 0, sub.stdout + sub.stderr
    job = json.loads(sub.stdout)["job_id"]
    man = json.loads((env.state / "jobs" / job / "manifest.json").read_text())
    assert man["parent_job"] == {"acs_job": None,
                                 "guard_job": "alpha.x/20260930T000000Z",
                                 "smc_guard_job": "alpha.x/20260930T000000Z"}
    env.wait_outcome(job)


def test_the_acs_job_marker_is_a_control_variable(env_direct):
    env = env_direct
    result = env.acs("submit", "--json", "--case", "b", "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--env", "ACS_JOB_ID=x",
                     "--cwd", str(env.projects["alpha"]), "--", PY,
                     str(WORKLOAD), "--out", str(env.root / "o"))
    assert result.returncode == cli.EXIT_NOT_ACCEPTED
    assert "ACS_JOB_ID" in result.stderr


def test_a_job_outside_any_job_has_no_parent(env_direct):
    env = env_direct
    job = env.submit(case="a", workload_args=(
        "--out", str(env.root / "a")))["job_id"]
    man = json.loads((env.state / "jobs" / job / "manifest.json").read_text())
    assert man["parent_job"] is None
    env.wait_outcome(job)


# --- the run exit-code contract ---------------------------------------------------

def test_run_exit_codes_follow_the_outcome(env_direct):
    """`run` exits with runner.RC of the outcome: 0 succeeded, 1 failed
    (the workload's code is `exit_code` in the JSON), 11 refused, and 2
    when nothing was accepted."""
    env = env_direct
    base = ["run", "--json", "--timeout-min", "1", "--reserve-gb", "0.5",
            "--cwd", str(env.projects["alpha"])]
    ok = env.acs(*base, "--case", "ok", "--", PY, str(WORKLOAD), "--out",
                 str(env.root / "a"))
    assert ok.returncode == 0 and json.loads(ok.stdout)["outcome"] == \
        "succeeded"
    bad = env.acs(*base, "--case", "bad", "--", PY, str(WORKLOAD), "--out",
                  str(env.root / "b"), "--exit", "3")
    view = json.loads(bad.stdout)
    assert bad.returncode == runmod.RC["failed"] == 1
    assert view["outcome"] == "failed" and view["exit_code"] == 3
    not_accepted = env.acs("run", "--json", "--case", "x", "--timeout-min",
                           "999", "--cwd", str(env.projects["alpha"]), "--",
                           PY, "-c", "pass")
    assert not_accepted.returncode == cli.EXIT_NOT_ACCEPTED == 2
    assert runmod.RC == {
        "succeeded": 0, "failed": 1, "launch-failed": 10, "refused": 11,
        "admission-timeout": 12, "held-inputs-changed": 13, "cancelled": 14,
        "timed-out": 15, "killed-resource": 16, "interrupted": 17,
        "unknown": 18, "duplicate-dispatch": 19}
    assert cli.EXIT_WAIT_TIMEOUT == 75
