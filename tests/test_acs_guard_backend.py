"""Campaign S4: the ACS-internal guard as the ACS backend (``acs-guard``).

Covers the backend switch:
* config: ``[backend].kind = "acs-guard"``, the pinned engine sha256, the
  required ``[backend.profile]`` (no defaults in config.py), the per-project
  ``reserve_detector`` (R1) and ``container_limits`` (R4), fail-closed on
  anything unknown or missing;
* runner: the engine of this version dir, the per-job profile file, the
  ACS claim, the project flags, the dedicated budget exactly as for
  guard.py; dispatch refuses on an engine sha or backend-kind mismatch;
  ``cleanup-unverified`` (or an unclean guard ``cleanup``) blocks;
* the internal guard refuses ``run``/``probe`` without the ACS claim;
* R3: status/doctor show the in-flight marker and ``reconcile`` removes it
  only on ACS evidence (never the lock file);
* ``pin-backend``: the engine pinned with committed evidence and a recorded
  authorization (since G2b the only backend; the ``smc-guard`` switch and
  rollback are retired, D1);
* an older installed version (main before S4) with an acs-guard config
  fails closed without running anything;
* a job holds its guard home's lock exactly once (its guard; admission
  never opens it).
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
import tomllib
from pathlib import Path

import pytest

from conftest import (BIN, ENGINE, FREE_MEM_ENV, REPO, TEST_PROFILE, WORKLOAD,
                      Env, QuietEnv, gate_body, gate_text, git, sha256,
                      skip_reason)

from agent_compute import cli, procs
from agent_compute import config as cfgmod
from agent_compute import runner as runmod
from agent_compute.state import JobStore, write_json

sys.path.insert(0, str(REPO / "src" / "agent_compute" / "guard"))
import engine  # noqa: E402  (the engine by path, as the runner runs it)

PY = sys.executable
PROFILE_TOML = "\n".join(f"{k} = {json.dumps(v)}"
                         for k, v in TEST_PROFILE.items())


@pytest.fixture
def acs_env():
    env = Env(queue=False)
    yield env
    env.cleanup()


@pytest.fixture
def either_env():
    """Behaviour that did not depend on the backend kind (the runner's
    classify, the marker in status/reconcile): it ran under both kinds
    until G2b retired smc-guard; the internal guard now."""
    env = Env(queue=False)
    yield env
    env.cleanup()


def edit(env: Env, old: str, new: str, count: int = 1) -> None:
    text = env.config.read_text()
    assert old in text, old
    env.config.write_text(text.replace(old, new, count))


def load(env: Env):
    return cfgmod.load(env.config)


def acs(env: Env, *args, env_vars=None, timeout=180):
    return subprocess.run([PY, str(BIN), "--config", str(env.config), *args],
                          capture_output=True, text=True,
                          stdin=subprocess.DEVNULL,
                          env=env_vars or env.env(), timeout=timeout)


def prepare_job(env: Env, config, *, project="alpha", argv=None,
                case="t") -> str:
    """A job accepted with this config's manifest and not yet claimed (no
    runner started), as the queue would hold it."""
    store = JobStore(config.state_dir)
    from agent_compute.state import private_dir
    private_dir(config.state_dir)
    private_dir(config.jobs_dir)
    job_id, _ = cli._reserve(store, config.host, None)
    args = argparse.Namespace(
        cwd=str(env.projects[project]), project=None, timeout_min=1.0,
        threads=1, dense_threads=None, container=None, env=[],
        container_workdir=None, lock_wait=30, input=[], request_id=None,
        owner=None, case=case, slice=None, units=None, reserve_gb=0.5)
    manifest = cli.build_manifest(
        config, args, argv or [PY, str(WORKLOAD), "--out",
                               str(env.root / f"{job_id}.out")], job_id)
    write_json(store.job_dir(job_id) / "manifest.json", manifest)
    return job_id


def exec_runner(env: Env, job_id: str, bin_path: Path = BIN,
                timeout=120) -> subprocess.CompletedProcess:
    return subprocess.run([PY, str(bin_path), "--config", str(env.config),
                           "_exec", job_id], capture_output=True, text=True,
                          env=env.env(), timeout=timeout, cwd="/")


# --- 1. config ------------------------------------------------------------------------

def test_acs_guard_config_loads_without_filling_defaults(acs_env):
    config = load(acs_env)
    assert config.backend_kind == "acs-guard"
    assert config.guard_sha256 == sha256(ENGINE)
    profile = dict(config.profile)
    assert profile.pop("schema") == "agent-compute-guard-profile/1"
    assert set(profile) == set(TEST_PROFILE)       # nothing filled in
    assert profile["runlog_tag"] == (TEST_PROFILE["runlog_tag"] or None)
    assert config.effective_budget_gb == TEST_PROFILE["budget_gb"]
    alpha = config.projects["alpha"]
    assert alpha.detector == "smc-ladder"
    assert alpha.container_limits == ["__CONTAINER__"]


@pytest.mark.parametrize("old,new,message", [
    ('kind = "acs-guard"', 'kind = "internal"', "backend.kind must be"),
    # G2a: strict validation names the unknown table first
    ("[backend.profile]\n", "[backend.unused]\n", "unknown keys ['unused']"),
    ("container_cpus =", "cpus =", "unknown keys ['cpus']"),
    ("floor_gb =", "# floor_gb =", "missing keys ['floor_gb']"),
    # G2b: the cap is the host's RAM (no host has a petabyte)
    ("budget_gb = ", "budget_gb = 1000000 #", "must be <= this host's RAM"),
    ("budget_gb = ", "budget_gb = -1 #", "must be a positive number"),
    ("container_cpus = ", "container_cpus = 2.5 #", "must be an integer"),
    ('reserve_detector = "smc-ladder"', 'reserve_detector = "ladder"',
     "reserve_detector must be one of"),
    ('container_limits = ["__CONTAINER__"]', 'container_limits = ["other"]',
     "not in its containers"),
    ('name = "', 'name = "" #', "name must be a non-empty string"),
])
def test_bad_acs_guard_config_fails_closed(acs_env, old, new, message):
    edit(acs_env, old, new)
    with pytest.raises(cfgmod.ConfigError, match=re.escape(message)):
        load(acs_env)
    doctor = acs(acs_env, "doctor", "--json")
    assert doctor.returncode == 2 and message in doctor.stderr
    submit = acs(acs_env, "submit", "--case", "t", "--timeout-min", "1",
                 "--cwd", str(acs_env.projects["alpha"]), "--", "true")
    assert submit.returncode == 2 and message in submit.stderr
    assert not (acs_env.state / "jobs").exists() or \
        not any((acs_env.state / "jobs").iterdir())


def test_acs_guard_needs_a_pinned_engine_sha(acs_env):
    edit(acs_env, f'sha256 = "{sha256(ENGINE)}"\n', "")
    with pytest.raises(cfgmod.ConfigError, match="needs backend.sha256"):
        load(acs_env)


def test_r1_a_project_without_reserve_detector_has_none(acs_env):
    """R1 as revised by D4 (G2b): the reserve detector is optional and
    defaults to "none" (no detection); "smc-ladder", the shipped example
    detector, is an explicit choice that doctor shows."""
    edit(acs_env, 'reserve_detector = "smc-ladder"\n', "", count=1)
    config = load(acs_env)
    assert config.projects["alpha"].reserve_detector == "none"
    assert config.projects["alpha"].detector is None
    assert config.projects["beta"].detector == "smc-ladder"
    doctor = json.loads(acs(acs_env, "doctor", "--json").stdout)
    shown = {c["check"]: c["detail"] for c in doctor["checks"]}
    assert shown["project alpha"]["reserve_detector"] == "none"
    assert shown["project beta"]["reserve_detector"] == "smc-ladder"


def test_r4_a_project_with_containers_must_declare_container_limits(acs_env):
    edit(acs_env, 'container_limits = ["__CONTAINER__"]\n', "", count=1)
    with pytest.raises(cfgmod.ConfigError,
                       match="container_limits is required"):
        load(acs_env)
    edit(acs_env, 'case_prefix = "alpha."\n',
         'case_prefix = "alpha."\ncontainer_limits = []\n')
    assert load(acs_env).projects["alpha"].container_limits == []


def test_unknown_project_key_is_a_config_error(acs_env):
    edit(acs_env, 'case_prefix = "alpha."\n',
         'case_prefix = "alpha."\nreserve_detectr = "none"\n')
    with pytest.raises(cfgmod.ConfigError, match="reserve_detectr"):
        load(acs_env)


PROFILES = [
    {"name": "m", "budget_gb": 12, "floor_gb": 8, "container_cpus": 10,
     "runlog_tag": None},
    {"name": "m", "budget_gb": 12, "floor_gb": 8, "container_cpus": 10,
     "runlog_tag": "host=x", "disk_floor_gb": 5, "light_budget_gb": 0.5,
     "vm_reserve_gb": 1, "container_memory_factor": 2},
    {"name": "m", "budget_gb": 12, "floor_gb": 8, "container_cpus": 10},
    {"name": "m", "budget_gb": 0, "floor_gb": 8, "container_cpus": 10,
     "runlog_tag": None},
    {"name": "m", "budget_gb": 12, "floor_gb": 8, "container_cpus": True,
     "runlog_tag": None},
    {"name": "", "budget_gb": 12, "floor_gb": 8, "container_cpus": 1,
     "runlog_tag": None},
    {"name": "m", "budget_gb": 12, "floor_gb": 8, "container_cpus": 1,
     "runlog_tag": None, "extra": 1},
    {"name": "m", "budget_gb": 12, "floor_gb": float("inf"),
     "container_cpus": 1, "runlog_tag": None},
]


@pytest.mark.parametrize("profile", PROFILES)
def test_config_and_engine_agree_on_profiles(profile):
    """The admission layer never imports the engine, so it mirrors its
    profile validation; both accept and refuse the same profiles."""
    try:
        engine.parse_profile({"schema": engine.PROFILE_SCHEMA, **profile})
        engine_ok = True
    except engine.ProfileError:
        engine_ok = False
    table = {k: ("" if v is None else v) for k, v in profile.items()}
    try:
        # the engine applies no cap; config.py caps budget_gb by the host's
        # RAM (G2b), which these profiles stay below
        mine = cfgmod.backend_profile(table, 64.0)
        config_ok = True
    except cfgmod.ConfigError:
        config_ok = False
    assert config_ok == engine_ok, profile
    if config_ok:
        # what config.py hands the engine parses to the same effective
        # profile (the engine fills the optional keys, config.py never)
        assert engine.parse_profile(mine) == engine.parse_profile(
            {"schema": engine.PROFILE_SCHEMA, **profile})
    assert cfgmod.RESERVE_DETECTORS == engine.RESERVE_DETECTORS
    assert cfgmod.PROFILE_SCHEMA == engine.PROFILE_SCHEMA


# --- 2. runner argv ----------------------------------------------------------------------

def _argv_of(env: Env, config, job_id: str, container=None) -> list[str]:
    runner = runmod.Runner(config, job_id)
    if container:
        runner.manifest["container"] = {"name": container, "id": "x" * 12,
                                        "workdir": "/work", "daemon_id": "d"}
    return runner.guard_argv(env.root / "attempt", 10.0)


def _opt(argv, name):
    cut = argv.index("--")
    head = argv[:cut]
    return head[head.index(name) + 1] if name in head else None


def test_runner_passes_the_engine_the_job_claim_profile_and_project(acs_env):
    config = load(acs_env)
    job = prepare_job(acs_env, config)
    argv = _argv_of(acs_env, config, job)
    assert argv[:3] == [config.python, str(runmod.ENGINE), "run"]
    assert runmod.ENGINE == ENGINE.resolve()
    assert _opt(argv, "--acs-job") == job
    assert _opt(argv, "--acs-state") == str(config.state_dir)
    profile_file = Path(_opt(argv, "--profile-file"))
    assert profile_file.parent == acs_env.state / "jobs" / job
    assert json.loads(profile_file.read_text()) == config.profile
    assert engine.parse_profile(json.loads(profile_file.read_text()))
    manifest = json.loads((acs_env.state / "jobs" / job /
                           "manifest.json").read_text())
    assert _opt(argv, "--project-root") == manifest["worktree"]
    assert _opt(argv, "--cwd") == manifest["cwd"]
    assert _opt(argv, "--reserve-detector") == "smc-ladder"
    assert "--profile" not in argv[:argv.index("--")]
    assert "--budget-gb" not in argv[:argv.index("--")]
    assert "--container-limits" not in argv
    assert manifest["backend"]["kind"] == "acs-guard"
    assert manifest["backend"]["path"] == str(runmod.ENGINE)
    assert manifest["backend"]["sha256"] == sha256(ENGINE)
    assert manifest["backend"]["profile"] == config.profile


def test_container_limits_only_for_listed_containers(acs_env):
    acs_env.set_container("c-listed")
    edit(acs_env, 'containers = ["c-listed"]\n',
         'containers = ["c-listed", "c-other"]\n')
    config = load(acs_env)
    job = prepare_job(acs_env, config)
    listed = _argv_of(acs_env, config, job, container="c-listed")
    assert "--container-limits" in listed[:listed.index("--")]
    assert _opt(listed, "--container-workdir") == "/work"
    other = _argv_of(acs_env, config, job, container="c-other")
    assert "--container-limits" not in other[:other.index("--")]


def test_reserve_detector_none_passes_no_detector(acs_env):
    edit(acs_env, 'reserve_detector = "smc-ladder"', 'reserve_detector = "none"')
    config = load(acs_env)
    argv = _argv_of(acs_env, config, prepare_job(acs_env, config))
    assert "--reserve-detector" not in argv


@pytest.mark.parametrize("declared", [None, 6])
def test_dedicated_budget_reaches_the_engine(tmp_path, declared):
    """The effective host budget of a job: the dedicated declaration
    (--budget-gb) wins, else the profile's.  (Before G2b this compared
    guard.py and the engine; the declared value is now below every test
    host's RAM, the cap since G2b.)"""
    budgets, argvs = {}, {}
    for backend in ("acs-guard",):
        env = Env(queue=False)
        try:
            if declared:
                edit(env, "[backend]\n",
                     f"[backend]\ndedicated_budget_gb = {declared}\n")
            config = load(env)
            argvs[backend] = _argv_of(env, config, prepare_job(env, config))
            out = env.root / "ran"
            job = env.submit(workload_args=["--out", str(out)])
            state = env.wait_outcome(job["job_id"])
            # G1 review F8: once ended `unknown` in a full Mac run (not
            # reproduced); say why, in full (pytest truncates a dict)
            why = (f"{backend}: outcome {state.get('outcome')!r}, reason "
                   f"{state.get('reason')!r}, guard_outcome "
                   f"{state.get('guard_outcome')!r}, exit_code "
                   f"{state.get('exit_code')!r}, cleanup "
                   f"{json.dumps(state.get('cleanup'))}, guard_cleanup "
                   f"{json.dumps(state.get('guard_cleanup'))}")
            if state["outcome"] != "succeeded":
                print(why)
                log = env.state / "jobs" / job["job_id"] / "output.log"
                print(log.read_text()[-4000:] if log.is_file() else "")
            assert state["outcome"] == "succeeded", why
            record = json.loads(Path(state["guard_record"]).read_text())
            budgets[backend] = record["thresholds"]["budget_gb"]
        finally:
            env.cleanup()
    expected = float(declared or TEST_PROFILE["budget_gb"])
    assert budgets == {"acs-guard": expected}
    for argv in argvs.values():
        assert _opt(argv, "--budget-gb") == (f"{declared:g}" if declared
                                              else None)


# --- 3. dispatch-time revalidation ------------------------------------------------------

def test_dispatch_refuses_when_the_pinned_engine_sha_differs(acs_env):
    config = load(acs_env)
    job = prepare_job(acs_env, config)
    other = "0" * 64
    edit(acs_env, f'sha256 = "{sha256(ENGINE)}"', f'sha256 = "{other}"')
    result = exec_runner(acs_env, job)
    state = acs_env.state_of(job)
    assert state["outcome"] == "refused", (state, result.stderr)
    assert "is not the verified 000000000000" in state["reason"]
    assert "never started" in state["reason"]
    assert not (acs_env.root / f"{job}.out").exists()
    assert not (acs_env.state / "jobs" / job / "guard").exists()


def test_dispatch_refuses_when_the_engine_changed_since_submission(acs_env):
    config = load(acs_env)
    job = prepare_job(acs_env, config)
    path = acs_env.state / "jobs" / job / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["backend"]["sha256"] = "1" * 64
    path.write_text(json.dumps(manifest))
    exec_runner(acs_env, job)
    state = acs_env.state_of(job)
    assert state["outcome"] == "refused"
    assert "changed since submission" in state["reason"]
    assert not (acs_env.root / f"{job}.out").exists()


@pytest.mark.parametrize("kind", ["smc-guard", None])
def test_dispatch_refuses_a_job_accepted_for_the_retired_backend(acs_env,
                                                                 kind):
    """A job accepted for guard.py (or with no recorded kind, which predates
    the engine) is never run by the engine: refused, never started."""
    config = load(acs_env)
    job = prepare_job(acs_env, config)
    path = acs_env.state / "jobs" / job / "manifest.json"
    manifest = json.loads(path.read_text())
    if kind is None:
        del manifest["backend"]["kind"]
    else:
        manifest["backend"]["kind"] = kind
    path.write_text(json.dumps(manifest))
    exec_runner(acs_env, job)
    state = acs_env.state_of(job)
    assert state["outcome"] == "refused", state
    assert f"submitted for backend {kind!r}" in state["reason"]
    assert "never started" in state["reason"]
    assert not (acs_env.root / f"{job}.out").exists()
    assert not (acs_env.state / "jobs" / job / "guard").exists()


# --- 4. cleanup-unverified is an ambiguous end that blocks ----------------------------

def test_guard_outcome_cleanup_unverified_maps_to_unknown():
    assert runmod.GUARD_OUTCOME["cleanup-unverified"] == "unknown"


def _classify(env: Env, record: dict, rc: int = 5):
    config = load(env)
    job = prepare_job(env, config)
    runner = runmod.Runner(config, job)
    attempt = env.state / "jobs" / job / "guard" / "attempt-1"
    attempt.mkdir(parents=True)
    (attempt / "samples.csv").write_text("t\n")
    dead = subprocess.Popen(["true"])
    dead.wait()
    record = {"schema": "smc-guard-record/1", "pid": dead.pid,
              "job_id": "alpha.t/20260930T000000Z",
              "started_utc": "2026-09-30T00:00:00Z", "exit_code": 0,
              **record}
    (attempt / "guard.json").write_text(json.dumps(record))
    runner.classify(rc, attempt)
    return job, env.state_of(job), JobStore(config.state_dir).block(
        config.host)


def test_cleanup_unverified_blocks_even_when_acs_finds_nothing(either_env):
    """Fail-first: before S4 the outcome became `unknown` without a block
    when ACS's own check found the group empty."""
    job, state, block = _classify(either_env, {
        "outcome": "cleanup-unverified",
        "cleanup": {"clean": False, "verified": False,
                    "reason": "process table unreadable",
                    "outcome": "completed"}})
    assert state["cleanup"]["clean"] is True         # ACS saw nothing
    assert state["outcome"] == "unknown"
    assert state["guard_cleanup"]["reason"] == "process table unreadable"
    assert block and job in block["jobs"], block
    assert "could not verify its workload's cleanup" in \
        block["jobs"][job]["reason"]


def test_an_unclean_guard_cleanup_record_blocks(either_env):
    job, state, block = _classify(either_env, {
        "outcome": "completed", "cleanup": {"clean": False,
                                            "reason": "survivors"}}, rc=0)
    assert state["outcome"] == "unknown"
    assert block and job in block["jobs"]


def test_a_clean_guard_cleanup_record_does_not_block(either_env):
    job, state, block = _classify(either_env, {
        "outcome": "completed", "cleanup": {"clean": True}}, rc=0)
    assert state["outcome"] == "succeeded"
    assert block is None


def test_group_member_outliving_the_leader_blocks_through_acs(acs_env):
    """End to end through the engine: the leader exits 0, a member of its
    group keeps running; the engine says cleanup-unverified, ACS blocks."""
    tag = f"acs-s4-{os.getpid()}-{time.time_ns()}"
    code = ("import subprocess, sys\n"
            f"subprocess.Popen([sys.executable, '-c', "
            f"'import time; time.sleep(60)  # {tag}'])\n")
    job = acs_env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                      "--reserve-gb", "0.5", "--cwd",
                      str(acs_env.projects["alpha"]), "--", PY, "-c", code)
    job_id = json.loads(job.stdout)["job_id"]
    try:
        state = acs_env.wait_outcome(job_id, timeout=120)
        assert state["outcome"] == "unknown", state
        assert state["guard_outcome"] == "cleanup-unverified"
        block = json.loads(acs(acs_env, "status", "--json").stdout)[
            "admission"]["blocked"]
        assert job_id in block["jobs"]
    finally:
        out = subprocess.run(["pgrep", "-f", tag], capture_output=True,
                             text=True)
        for pid in out.stdout.split():
            try:
                os.kill(int(pid), signal.SIGKILL)
            except OSError:
                pass


# --- 5. no direct route to the internal guard ----------------------------------------

def _engine_run(tmp: Path, extra_env: dict, *args, home: Path | None = None):
    home = home or tmp / "home"
    home.mkdir(exist_ok=True)
    free = tmp / "free"
    free.write_text(str(64 * 1024 ** 3))
    env = QuietEnv((k, v) for k, v in os.environ.items()
                   if not k.startswith(("SMC_GUARD_", "ACS_GUARD_")))
    env.update({FREE_MEM_ENV: str(free), **extra_env})
    profile = tmp / "profile.json"
    profile.write_text(json.dumps({"schema": engine.PROFILE_SCHEMA,
                                   **{k: (v or None) if k == "runlog_tag"
                                      else v
                                      for k, v in TEST_PROFILE.items()}}))
    out = tmp / "ran"
    argv = [PY, str(ENGINE), "run", "--case", "direct", "--profile-file",
            str(profile), "--reserve-gb", "0.5", "--cwd", str(tmp),
            "--outputs", str(tmp / "out"), "--runlog", str(tmp / "runlog.md"),
            "--baseline", str(tmp / "baseline.json"), *args, "--",
            PY, "-c", f"open({str(out)!r}, 'w').write('x')"]
    result = subprocess.run(argv, capture_output=True, text=True, env=env,
                            timeout=60)
    return result, out


def test_direct_run_without_an_acs_claim_is_refused(tmp_path):
    result, out = _engine_run(tmp_path, {
        "SMC_GUARD_HOME": str(tmp_path / "home")})
    assert result.returncode == engine.EXIT_USAGE, result.stderr
    assert "runs only for an ACS job" in result.stderr
    assert "agent-compute submit" in result.stderr
    assert not out.exists()
    assert not (tmp_path / "home" / "lock").exists()


def test_probe_without_an_acs_claim_is_refused(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    free = tmp_path / "free"
    free.write_text(str(64 * 1024 ** 3))
    env = QuietEnv((k, v) for k, v in os.environ.items()
                   if not k.startswith(("SMC_GUARD_", "ACS_GUARD_")))
    env.update({"SMC_GUARD_HOME": str(home), FREE_MEM_ENV: str(free)})
    result = subprocess.run(
        [PY, str(ENGINE), "probe", "--case", "p", "--profile-file",
         str(REPO / "tests" / "guard_profiles" / "mac.json"), "--cwd",
         str(tmp_path), "--outputs", str(tmp_path / "o"), "--runlog",
         str(tmp_path / "r.md"), "--baseline", str(tmp_path / "b.json"),
         "--", PY, "-c", "pass"], capture_output=True, text=True, env=env,
        timeout=60)
    assert result.returncode == engine.EXIT_USAGE
    assert "runs only for an ACS job" in result.stderr
    assert not (home / "lock").exists()


def test_the_test_bypass_never_reaches_the_default_lock(tmp_path):
    """ACS_GUARD_TEST_UNCLAIMED needs SMC_GUARD_HOME naming another home.
    HOME is a temp dir here, so even a failure could not touch the real
    lock."""
    fake_home = tmp_path / "user"
    fake_home.mkdir()
    for extra in ({"HOME": str(fake_home), "ACS_GUARD_TEST_UNCLAIMED": "1"},
                  {"HOME": str(fake_home), "ACS_GUARD_TEST_UNCLAIMED": "1",
                   "SMC_GUARD_HOME": str(fake_home / ".smc-guard")}):
        result, out = _engine_run(tmp_path, extra)
        assert result.returncode == engine.EXIT_USAGE, result.stderr
        assert "sandbox tests only" in result.stderr
        assert not out.exists()
        assert not (fake_home / ".smc-guard" / "lock").exists()


def _claim(tmp: Path, job: str, runner_pid, outcome=None) -> Path:
    state_dir = tmp / "acs-state"
    job_dir = state_dir / "jobs" / job
    job_dir.mkdir(parents=True, exist_ok=True)
    state = {"job_id": job, "runner": {"pid": runner_pid}}
    if outcome:
        state["outcome"] = outcome
    (job_dir / "state.json").write_text(json.dumps(state))
    return state_dir


@pytest.mark.parametrize("case", ["other-runner", "finished", "bad-id",
                                  "missing-state", "no-state-dir"])
def test_a_forged_or_stale_claim_is_refused(tmp_path, case):
    job = "acs-20260930T000000Z-abcdef"
    runner_pid = 1 if case == "other-runner" else os.getpid()
    state_dir = _claim(tmp_path, job, runner_pid,
                       outcome="succeeded" if case == "finished" else None)
    args = ["--acs-job", "../x" if case == "bad-id" else
            ("acs-nope" if case == "missing-state" else job)]
    if case != "no-state-dir":
        args += ["--acs-state", str(state_dir)]
    result, out = _engine_run(tmp_path, {
        "SMC_GUARD_HOME": str(tmp_path / "home")}, *args)
    assert result.returncode == engine.EXIT_USAGE, result.stderr
    assert "refused" in result.stderr
    assert not out.exists()


def test_a_claim_by_the_parent_runner_is_accepted(tmp_path):
    """The engine's parent (here: this test process) is the job's recorded
    runner, and the job has no outcome: the guard runs the job."""
    job = "acs-20260930T000000Z-abcdef"
    state_dir = _claim(tmp_path, job, os.getpid())
    result, out = _engine_run(tmp_path, {
        "SMC_GUARD_HOME": str(tmp_path / "home")}, "--acs-job", job,
        "--acs-state", str(state_dir))
    assert result.returncode == 0, result.stderr
    assert out.exists()
    record = json.loads((tmp_path / "out" / "guard.json").read_text())
    assert record["outcome"] == "completed"


def test_nested_pass_through_needs_the_claim_and_status_none(tmp_path):
    """G2a: a marker alone no longer passes through (the enclosing claimed
    ACS job is required: tests/test_g2a_names.py); status needs no claim."""
    result, out = _engine_run(tmp_path, {
        "SMC_GUARD_HOME": str(tmp_path / "home"),
        "SMC_GUARD_JOB": "outer/20260930T000000Z"})
    assert result.returncode == engine.EXIT_USAGE, result.stderr
    assert "no enclosing ACS job" in result.stderr and not out.exists()
    status = subprocess.run([PY, str(ENGINE), "status"], capture_output=True,
                            text=True, timeout=60,
                            env=QuietEnv({**os.environ, "SMC_GUARD_HOME":
                                          str(tmp_path / "home")}))
    assert status.returncode == 0, status.stderr


def test_acs_refuses_a_command_that_runs_the_engine_itself(acs_env):
    result = acs_env.acs("submit", "--json", "--case", "t", "--timeout-min",
                         "1", "--cwd", str(acs_env.projects["alpha"]), "--",
                         PY, str(ENGINE), "run", "--case", "x", "--", "true")
    assert result.returncode == 2
    assert "ACS-internal guard" in result.stderr


# --- 6. the in-flight marker (R3) -------------------------------------------------------

def _finished_job(env: Env) -> tuple[str, dict]:
    job = env.submit(workload_args=["--out", str(env.root / "done")])
    state = env.wait_outcome(job["job_id"])
    assert state["outcome"] == "succeeded", state
    record = json.loads(Path(state["guard_record"]).read_text())
    return job["job_id"], record


def _marker(env: Env, **fields) -> Path:
    env.guard_home.mkdir(exist_ok=True)
    path = env.guard_home / "inflight.json"
    data = {"schema": "agent-compute-guard-inflight/1", "venue": "host",
            "phase": "launching", "pid": None, "start": None, **fields}
    path.write_text(json.dumps(data))
    return path


def _lock_stat(env: Env):
    lock = env.guard_home / "lock"
    return (lock.exists(), lock.stat().st_ino if lock.exists() else None,
            lock.stat().st_mtime_ns if lock.exists() else None)


def test_status_and_doctor_show_the_marker(either_env):
    job_id, record = _finished_job(either_env)
    _marker(either_env, job_id=record["job_id"], acs_job="acs-unknown")
    status = json.loads(acs(either_env, "status", "--json").stdout)
    marker = status["admission"]["inflight_marker"]
    assert marker["acs_job"] == "acs-unknown"
    assert marker["path"] == str(either_env.guard_home / "inflight.json")
    doctor = json.loads(acs(either_env, "doctor", "--json").stdout)
    rows = {c["check"]: c for c in doctor["checks"]}
    assert rows["no stale in-flight marker"]["ok"] is False
    assert doctor["ok"] is False
    assert (either_env.guard_home / "inflight.json").exists()   # read-only


def test_reconcile_removes_a_marker_of_a_resolved_job_on_evidence(either_env):
    """The launching-phase marker (the S3 'unprovable' case: no pid) of an
    ACS job whose processes ACS proves gone is removed by reconcile, with
    a durable history entry; the lock file is never touched."""
    job_id, record = _finished_job(either_env)
    before = _lock_stat(either_env)
    path = _marker(either_env, job_id=record["job_id"], acs_job=job_id)
    result = acs(either_env, "reconcile", "--json")
    report = json.loads(result.stdout)
    assert result.returncode == 0, report
    assert report["inflight_marker"]["resolved"] is True
    assert report["inflight_marker"]["evidence"]["clean"] is True
    assert not path.exists()
    assert _lock_stat(either_env) == before
    history = either_env.state_of(job_id)["history"]
    entry = [h for h in history if h.get("event") ==
             "inflight-marker-cleared"]
    assert entry and entry[0]["marker"]["acs_job"] == job_id
    # the next job runs
    nxt = either_env.submit("beta", workload_args=["--out",
                                                str(either_env.root / "n")])
    assert either_env.wait_outcome(nxt["job_id"])["outcome"] == "succeeded"


def test_reconcile_maps_a_marker_without_acs_job_by_guard_job_id(either_env):
    job_id, record = _finished_job(either_env)
    path = _marker(either_env, job_id=record["job_id"])
    report = json.loads(acs(either_env, "reconcile", "--json").stdout)
    assert report["inflight_marker"]["resolved"] is True
    assert report["inflight_marker"]["job_id"] == job_id
    assert not path.exists()


@pytest.mark.parametrize("case", ["unknown-job", "malformed", "no-outcome",
                                  "wrong-guard-job", "admission-held",
                                  "blocked"])
def test_reconcile_keeps_a_marker_without_evidence(acs_env, case):
    job_id, record = _finished_job(acs_env)
    path = acs_env.guard_home / "inflight.json"
    fd = None
    sleeper = subprocess.Popen([PY, "-c", "import time; time.sleep(120)"])
    if case == "unknown-job":
        _marker(acs_env, job_id="nobody/20260930T000000Z")
    elif case == "malformed":
        path.write_text("{not json")
    elif case == "no-outcome":
        # as if still running: no outcome, and a live runner
        state_path = acs_env.state / "jobs" / job_id / "state.json"
        state = json.loads(state_path.read_text())
        state.pop("outcome")
        state["runner"] = procs.identity(sleeper.pid)
        state_path.write_text(json.dumps(state))
        _marker(acs_env, job_id=record["job_id"], acs_job=job_id)
    elif case == "wrong-guard-job":
        _marker(acs_env, job_id="alpha.t/19990101T000000Z", acs_job=job_id)
    elif case == "admission-held":
        _marker(acs_env, job_id=record["job_id"], acs_job=job_id)
        fd = os.open(acs_env.state / "admission.lock", os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX)
    elif case == "blocked":
        JobStore(acs_env.state).set_block("testhost", job_id, "test",
                                          {"test": True})
        # no evidence (a missing manifest proves nothing): the block stays
        (acs_env.state / "jobs" / job_id / "manifest.json").unlink()
        _marker(acs_env, job_id=record["job_id"], acs_job=job_id)
    before = path.read_bytes()
    try:
        result = acs(acs_env, "reconcile", "--json")
    finally:
        if fd is not None:
            os.close(fd)
        sleeper.kill()
        sleeper.wait()
    report = json.loads(result.stdout)
    assert result.returncode == 1, report
    assert report["blocked"] is True
    assert report["inflight_marker"]["resolved"] is False
    assert report["inflight_marker"]["blocker"]
    assert path.read_bytes() == before


def test_reconcile_keeps_a_marker_whose_workload_is_alive(acs_env):
    job_id, record = _finished_job(acs_env)
    sleeper = subprocess.Popen([PY, "-c", "import time; time.sleep(60)"],
                               start_new_session=True)
    try:
        ident = procs.identity(sleeper.pid)
        state_path = acs_env.state / "jobs" / job_id / "state.json"
        state = json.loads(state_path.read_text())
        state["workload"] = {**ident, "venue": "host",
                             "guard_job_id": record["job_id"]}
        state_path.write_text(json.dumps(state))
        path = _marker(acs_env, job_id=record["job_id"], acs_job=job_id,
                       pid=sleeper.pid, phase="unclean")
        report = json.loads(acs(acs_env, "reconcile", "--json").stdout)
        assert report["inflight_marker"]["resolved"] is False
        assert "not proven gone" in report["inflight_marker"]["blocker"] or \
            "remain" in report["inflight_marker"]["blocker"]
        assert path.exists()
    finally:
        sleeper.kill()
        sleeper.wait()


def test_runner_backs_off_while_an_unproven_marker_holds_the_host(acs_env):
    edit(acs_env, "lock_wait_max_s = 120", "lock_wait_max_s = 25")
    edit(acs_env, "admission_retry_s = 1", "admission_retry_s = 30")
    _marker(acs_env, job_id="nobody/20260930T000000Z", acs_job="acs-nobody")
    job = acs_env.submit(extra=("--lock-wait", "25"),
                         workload_args=["--out", str(acs_env.root / "x")])
    state = acs_env.wait_phase(job["job_id"], "admission-wait", timeout=60)
    acs_env.wait_for(lambda: "in-flight marker" in (acs_env.state_of(
        job["job_id"]).get("wait_reason") or ""), timeout=60,
        what="marker wait reason")
    final = acs_env.wait_outcome(job["job_id"], timeout=90)
    assert final["outcome"] == "admission-timeout", final
    assert not (acs_env.root / "x").exists()
    attempts = list((acs_env.state / "jobs" / job["job_id"] / "guard")
                    .glob("attempt-*"))
    assert len(attempts) == 1, attempts     # backed off, not polled
    assert (acs_env.guard_home / "inflight.json").exists()
    del state


# --- 7. one holder of the lock --------------------------------------------------------

def test_a_job_holds_the_guard_lock_exactly_once(acs_env):
    job = acs_env.submit(workload_args=["--out", str(acs_env.root / "w"),
                                        "--sleep", "8"])
    state = acs_env.wait_phase(job["job_id"], "executing")
    users = acs_env.wait_for(lambda: procs.lock_file_users(
        acs_env.guard_home / "lock") or None, what="lock users")
    assert users == [state["guard"]["pid"]], (users, state["guard"])
    assert state["runner"]["pid"] not in users
    assert acs_env.wait_outcome(job["job_id"])["outcome"] == "succeeded"


# --- 8. pin-backend: switch and rollback -----------------------------------------------

def _evidence(env: Env, header: str, name="pytest-mac.txt") -> Path:
    repo = env.root / "evidence-repo"
    if not repo.exists():
        repo.mkdir()
        git(repo, "init", "-q", "-b", "main")
    path = repo / name
    path.write_text(gate_text(header, gate_body(passed=600, skipped=1)))
    git(repo, "add", name)
    git(repo, "commit", "-q", "-m", name)
    return path


ENGINE_HEADER = ("# mac full suite on testhost, DOCKER HIDDEN (test); profile "
                 "standard; source sha " + "a" * 40 + "; engine sha256 {sha}; "
                 "pueue 4.0.4; Python 3.12.14 (/x/python); tracked "
                 "changes: 0; redactions: 0; written by tools/gate_run.py 1; "
                 "2026-10-10T12:00:00Z")
AUTH = "operator test, 2026-10-06, pin the engine"


def test_pin_backend_pins_the_engine_changing_only_its_sha256_line(acs_env):
    """G2b (D1, D8): the only backend is pinned with committed evidence and
    a recorded authorization; only the [backend] sha256 line changes."""
    env = acs_env
    edit(env, f'sha256 = "{sha256(ENGINE)}"', f'sha256 = "{"0" * 64}"')
    before = env.config.read_text()
    evidence = _evidence(env, ENGINE_HEADER.format(sha=sha256(ENGINE)))
    result = acs(env, "pin-backend", "--backend", "acs-guard", "--evidence",
                 str(evidence), "--authorized-by", AUTH, "--json")
    assert result.returncode == 0, result.stderr
    after = env.config.read_text()
    changed = [(a, b) for a, b in zip(before.splitlines(),
                                      after.splitlines()) if a != b]
    assert changed == [(f'sha256 = "{"0" * 64}"',
                        f'sha256 = "{sha256(ENGINE)}"')]
    assert stat.S_IMODE(env.config.stat().st_mode) == 0o600
    ran = env.submit(workload_args=["--out", str(env.root / "a")])
    done = env.wait_outcome(ran["job_id"])
    assert done["outcome"] == "succeeded", done
    manifest = json.loads((env.state / "jobs" / ran["job_id"] /
                           "manifest.json").read_text())
    assert manifest["backend"]["kind"] == "acs-guard"


@pytest.mark.parametrize("case", ["no-engine-token", "wrong-sha",
                                  "no-authorization", "guard-py-run"])
def test_pin_backend_refusals(acs_env, case):
    env = acs_env
    edit(env, f'sha256 = "{sha256(ENGINE)}"', f'sha256 = "{"0" * 64}"')
    header = ENGINE_HEADER.format(sha=sha256(ENGINE))
    if case == "no-engine-token":
        header = header.replace("profile standard; ", "")
    if case == "wrong-sha":
        header = ENGINE_HEADER.format(sha="0" * 64)
    if case == "guard-py-run":
        # a suite that ran guard.py as the backend (before 0.2.0)
        header = header.replace("profile standard", "backend smc-guard")
    evidence = _evidence(env, header)
    before = env.config.read_text()
    args = ["pin-backend", "--evidence", str(evidence)]
    if case != "no-authorization":
        args += ["--authorized-by", AUTH]
    result = acs(env, *args)
    assert result.returncode != 0, result.stdout
    assert env.config.read_text() == before
    assert not (env.state / "authorizations.jsonl").exists()
    expected = {"no-engine-token": "ran the internal engine",
                "wrong-sha": "does not name the new engine sha256",
                "no-authorization": "--authorized-by",
                "guard-py-run": "ran the internal engine"}[case]
    assert expected in result.stderr


def test_set_backend_keys_keeps_every_other_line():
    text = ('# c\n[backend]\nkind = "smc-guard"  # k\nguard = "g"\n'
            'sha256 = "a"\n\n[backend.profile]\nname = "mac"\n'
            'sha256 = "not me"\n[queue]\nenabled = false\n')
    out = cli.set_backend_keys(text, {"kind": "acs-guard", "sha256": "b"})
    assert out == text.replace('kind = "smc-guard"  # k',
                               'kind = "acs-guard"').replace(
        'sha256 = "a"', 'sha256 = "b"')
    with pytest.raises(cfgmod.ConfigError):
        cli.set_backend_keys(text, {"kind": 'x"\nmalicious = "1'})


# --- 9. an older installed version with an acs-guard config -----------------------------

OLD = "d2c6d98"         # main before S4 (the last smc-guard-only version)


@pytest.fixture
def old_version(tmp_path):
    if subprocess.run(["git", "-C", str(REPO), "cat-file", "-e",
                       f"{OLD}^{{commit}}"], capture_output=True).returncode:
        pytest.skip(skip_reason("clone", f"{OLD} is not in this clone"))
    root = tmp_path / "old"
    root.mkdir()
    archive = subprocess.run(["git", "-C", str(REPO), "archive", OLD, "bin",
                              "src"], capture_output=True, check=True)
    subprocess.run(["tar", "-x", "-C", str(root)], input=archive.stdout,
                   check=True)
    return root / "bin" / "agent-compute"


def test_an_older_version_with_an_acs_guard_config_fails_closed(acs_env,
                                                                old_version):
    """What a pre-S4 version (every installed one) does when the machine
    config says acs-guard: every command, and the runner of a job it had
    queued, stops at config load, before any claim, guard or workload."""
    for args in (["doctor"], ["status"], ["submit", "--case", "t",
                                          "--timeout-min", "1", "--cwd",
                                          str(acs_env.projects["alpha"]),
                                          "--", "true"]):
        result = subprocess.run([PY, str(old_version), "--config",
                                 str(acs_env.config), *args],
                                capture_output=True, text=True,
                                env=acs_env.env(), timeout=60)
        assert result.returncode == 2, (args, result.stdout, result.stderr)
        # G2a: a v2 config stops it even earlier, at the schema check
        assert "backend.kind must be 'smc-guard'" in result.stderr or \
            "schema must be 'agent-compute-config/1'" in result.stderr
    job = prepare_job(acs_env, load(acs_env))
    result = exec_runner(acs_env, job, bin_path=old_version)
    assert result.returncode == 2
    assert "schema must be" in result.stderr or \
        "backend.kind must be" in result.stderr
    state = acs_env.state_of(job)
    assert "runner" not in state and not state.get("outcome")
    assert not (acs_env.root / f"{job}.out").exists()
    assert not (acs_env.guard_home / "lock").exists()
    assert not (acs_env.guard_home / "current.json").exists()
    # the new CLI later settles it as never started (A3/A12 path)
    assert cli._finish_if_unclaimed(JobStore(acs_env.state), job, "refused",
                                    "test: never started")
