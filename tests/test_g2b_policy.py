"""Campaign G slice G2b: resource policy, records and the backend set.

What this establishes (the private development record's G2b slice,
decisions D1, D3, D4, D8; coupling inventory C, D, E, F; findings F15,
F21):

* D1: ``backend.kind`` has one value, ``acs-guard``; a config naming the
  retired ``smc-guard`` (or a guard script path) fails to load with the
  reason; ``pin-backend`` offers no other backend; the v1 migration refuses
  a ``smc-guard`` config; the test harness's standard profile runs every
  ACS job through the internal engine and needs no ``guard.py``;
* D4: the budget cap is the host's RAM (no fixed 48 GB); ``config init``
  prints a fresh config with host-derived profile defaults
  (``floor_gb = max(2, 0.125 x RAM)``, ``budget_gb = 0.25 x RAM``,
  ``container_cpus = cores - 2``); ``doctor`` prints the host's resources
  and those defaults; ``reserve_detector`` is optional, default ``"none"``;
* D3: a runlog or baseline inside the repository tree is an explicit
  per-project opt-in (``records_in_tree = true``), which the migration
  writes for the projects whose v1 records were in the tree;
  the engine writes runlog lines only for an admitted attempt and its end
  (no ``guard:refuse``/``guard:lock-busy`` line per retry; the refusal is
  kept in the attempt's ``guard.json``); ``doctor`` warns when a project's
  runlog or baseline is inside its ``inputs`` pathspecs;
* D8: ``pin-backend`` takes ``--authorized-by`` and appends the
  authorization record (the evidence file, whether that run had Docker,
  the config change, the version); no terminal ceremony; the evidence check
  is as strict as before;
* ``docs/memory-model.md`` quotes the code it describes verbatim.

Host resources are faked: in-process by replacing
``config.host_resources``, in a CLI subprocess through
``ACS_TEST_HOST_MEMORY_GB``/``ACS_TEST_HOST_CORES`` (honoured only by code
running from a checkout, never by an installed version).
"""

from __future__ import annotations

import dataclasses
import fcntl
import inspect
import json
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

import conftest
from conftest import (BIN, ENGINE, REPO, Env, QuietEnv, gate_body, gate_header,
                      gate_text, git, mark_sandbox, sha256)
from test_g2a_config import load_text, refused, text
from test_g2a_migrate import migrate, sandbox

from agent_compute import cli
from agent_compute import config as cfgmod

PY = sys.executable
GB = 1024 ** 3
AUTH = "operator test, 2026-10-06, pin the engine of this tree"


def fake_host(monkeypatch, memory_gb: float, cores: int) -> None:
    monkeypatch.setattr(cfgmod, "host_resources",
                        lambda *a, **k: {"memory_gb": float(memory_gb),
                                         "cores": cores, "source": "test"},
                        raising=False)


def host_env(memory_gb: float, cores: int) -> dict:
    return {"ACS_TEST_HOST_MEMORY_GB": str(memory_gb),
            "ACS_TEST_HOST_CORES": str(cores)}


def doctor(env: Env, **extra) -> dict:
    result = env.acs("doctor", "--json", env=env.env(**extra))
    assert result.stdout.strip(), result.stderr
    return json.loads(result.stdout)


def check_named(report: dict, prefix: str) -> dict:
    found = [c for c in report["checks"] if c["check"].startswith(prefix)]
    assert len(found) == 1, [c["check"] for c in report["checks"]]
    return found[0]


def edit(env: Env, old: str, new: str) -> None:
    body = env.config.read_text()
    assert old in body, old
    env.config.write_text(body.replace(old, new, 1))


# --- D1: the backend set -----------------------------------------------------------------

def test_a_config_naming_smc_guard_fails_to_load_with_the_reason(tmp_path):
    body = text(tmp_path).replace('kind = "acs-guard"',
                                  'kind = "smc-guard"\nguard = "/g.py"')
    message = refused(tmp_path, body)
    assert "smc-guard" in message and "retired" in message
    assert "acs-guard" in message


def test_the_backend_kind_has_one_value():
    assert cfgmod.BACKEND_KINDS == ("acs-guard",)


def test_a_guard_script_path_is_refused(tmp_path):
    body = text(tmp_path, backend='guard = "/x/scripts/dev/guard.py"')
    message = refused(tmp_path, body)
    assert "backend.guard" in message and "retired" in message


def test_the_config_has_no_external_guard_fields():
    names = {f.name for f in dataclasses.fields(cfgmod.Config)}
    assert not names & {"guard", "guard_profile"}, names


def test_pin_backend_offers_only_the_internal_engine():
    with pytest.raises(SystemExit):
        cli.parser().parse_args(["pin-backend", "--backend", "smc-guard",
                                 "--evidence", "x.txt"])
    args = cli.parser().parse_args(["pin-backend", "--backend", "acs-guard",
                                    "--evidence", "x.txt"])
    assert args.backend == "acs-guard"


def test_the_migration_refuses_a_smc_guard_v1_config(tmp_path):
    config, env = sandbox(tmp_path, "example-mac")     # v1, kind smc-guard
    original = config.read_text()
    result = migrate(config, env)
    assert result.returncode == 1, result.stdout
    assert "smc-guard" in result.stderr and "retired" in result.stderr
    assert config.read_text() == original
    assert not list(config.parent.glob("*.pre-v2-*"))


def test_the_standard_profile_needs_no_guard_py():
    """The harness runs every ACS job through the internal engine of this
    tree; a guard.py is only the legacy profile's
    (ACS_TEST_LEGACY_GUARD)."""
    assert not hasattr(conftest, "DEFAULT_GUARD")
    assert not hasattr(conftest, "BACKEND")
    assert conftest.LEGACY_GUARD == (
        Path(os.environ["ACS_TEST_LEGACY_GUARD"]).expanduser()
        if os.environ.get("ACS_TEST_LEGACY_GUARD") else None)
    env = Env(queue=False)
    try:
        raw = tomllib.loads(env.config.read_text())
        assert raw["backend"]["kind"] == "acs-guard"
        assert "guard" not in raw["backend"]
        assert raw["backend"]["sha256"] == sha256(ENGINE)
    finally:
        env.cleanup()


# --- D4: the host-derived cap and defaults -------------------------------------------------

def test_the_budget_cap_is_the_hosts_memory(tmp_path, monkeypatch):
    assert not hasattr(cfgmod, "DEDICATED_BUDGET_MAX_GB")
    fake_host(monkeypatch, 128, 32)
    big = text(tmp_path).replace("budget_gb = 1", "budget_gb = 60")
    assert load_text(tmp_path, big).profile["budget_gb"] == 60.0
    dedicated = text(tmp_path, backend="dedicated_budget_gb = 100")
    assert load_text(tmp_path, dedicated).dedicated_budget_gb == 100.0
    fake_host(monkeypatch, 32, 8)
    for body in (text(tmp_path).replace("budget_gb = 1", "budget_gb = 40"),
                 text(tmp_path, backend="dedicated_budget_gb = 33")):
        message = refused(tmp_path, body)
        assert "32" in message and "RAM" in message, message
    assert load_text(tmp_path, text(
        tmp_path, backend="dedicated_budget_gb = 32")).dedicated_budget_gb \
        == 32.0


@pytest.mark.parametrize("memory,cores,floor,budget,cpus", [
    (64, 12, 8.0, 16.0, 10), (10, 8, 2.0, 2.5, 6), (16, 1, 2.0, 4.0, 1)])
def test_config_init_prints_host_derived_defaults(tmp_path, monkeypatch,
                                                  capsys, memory, cores,
                                                  floor, budget, cpus):
    fake_host(monkeypatch, memory, cores)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("AGENT_COMPUTE_CONFIG", str(tmp_path / "none.toml"))
    assert cli.main(["config", "init", "--label", "build-01"]) == 0
    out = capsys.readouterr().out
    assert not (tmp_path / "none.toml").exists()      # printed, not written
    raw = tomllib.loads(out)
    assert raw["schema"] == "agent-compute-config/2"
    assert raw["host"] == "build-01"
    assert raw["backend"]["kind"] == "acs-guard"
    profile = raw["backend"]["profile"]
    assert profile["floor_gb"] == floor
    assert profile["budget_gb"] == budget
    assert profile["container_cpus"] == cpus
    # neutral names: no guard home override, no marker aliases, no legacy
    # workload names, no project-specific detector or in-tree records
    for key in ("guard_home", "marker_aliases", "legacy_workload_env",
                "guard", "dedicated_budget_gb"):
        assert key not in raw["backend"], key
    assert "smc" not in out.lower().replace("smc-ladder", "")
    for body in (raw.get("projects") or {}).values():
        assert body.get("reserve_detector", "none") == "none"
        assert "runlog" not in body and "baseline" not in body
        assert "records_in_tree" not in body
    path = tmp_path / "init.toml"
    path.write_text(out)
    config = cfgmod.load(path)
    assert config.markers == ["ACS_JOB_MARKER"]
    assert config.effective_guard_home == config.state_dir / "guard"
    assert "records default to" in out and "reserve_detector" in out


def test_doctor_prints_the_host_resources_and_derived_defaults(env_direct):
    report = doctor(env_direct, **host_env(64, 12))
    check = check_named(report, "host resources")
    detail = check["detail"]
    assert detail["memory_gb"] == 64.0 and detail["cores"] == 12
    assert detail["budget_cap_gb"] == 64.0
    assert detail["derived_defaults"] == {"floor_gb": 8.0, "budget_gb": 16.0,
                                          "container_cpus": 10}
    profile = cfgmod.load(env_direct.config).profile
    assert detail["configured"]["budget_gb"] == profile["budget_gb"]
    assert detail["configured"]["floor_gb"] == profile["floor_gb"]


def test_reserve_detector_is_optional_and_defaults_to_none(tmp_path):
    config = load_text(tmp_path, text(
        tmp_path, projects='[projects.p]\nroots = ["/x"]\n'))
    project = config.projects["p"]
    assert project.reserve_detector == "none"
    assert project.detector is None
    for value in ("smc-ladder", "none"):
        config = load_text(tmp_path, text(
            tmp_path, projects='[projects.p]\nroots = ["/x"]\n'
                               f'reserve_detector = "{value}"\n'))
        assert config.projects["p"].reserve_detector == value
    assert "reserve_detector" in refused(tmp_path, text(
        tmp_path, projects='[projects.p]\nroots = ["/x"]\n'
                           'reserve_detector = "other"\n'))


def test_a_project_without_a_detector_runs_without_one(env_direct):
    from test_acs_guard_backend import _argv_of, prepare_job
    env = env_direct
    body = env.config.read_text()
    assert body.count('reserve_detector = "smc-ladder"\n') == 2
    env.config.write_text(body.replace('reserve_detector = "smc-ladder"\n',
                                       ""))
    config = cfgmod.load(env.config)
    argv = _argv_of(env, config, prepare_job(env, config))
    assert "--reserve-detector" not in argv[:argv.index("--")]


# --- D3: records ---------------------------------------------------------------------------

@pytest.mark.parametrize("template", ["docs/runlog.md",
                                      "{worktree}/docs/runlog.md",
                                      "/x/sub/docs/runlog.md"])
@pytest.mark.parametrize("key", ["runlog", "baseline"])
def test_in_tree_records_need_the_explicit_opt_in(tmp_path, template, key):
    project = (f'[projects.p]\nroots = ["/x/*", "/x"]\n'
               f'reserve_detector = "none"\n{key} = "{template}"\n')
    message = refused(tmp_path, text(tmp_path, projects=project))
    assert "records_in_tree" in message and key in message
    config = load_text(tmp_path, text(
        tmp_path, projects=project + "records_in_tree = true\n"))
    assert config.projects["p"].records_in_tree is True
    state = load_text(tmp_path, text(
        tmp_path, projects=f'[projects.p]\nroots = ["/x"]\n'
                           'reserve_detector = "none"\n'
                           f'{key} = "{{state}}/projects/{{project}}/r"\n'))
    assert state.projects["p"].records_in_tree is False


@pytest.mark.parametrize("name", ["sample-a", "sample-b"])
def test_the_migration_keeps_the_operators_records_detectors_and_numbers(
        tmp_path, name):
    config, env = sandbox(tmp_path, name)
    env.update(host_env(64, 12))
    before = tomllib.loads(config.read_text())
    result = migrate(config, env, "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    after = tomllib.loads(config.read_text())
    # the project whose v1 records were in its tree keeps them, by explicit
    # opt-in
    smc = after["projects"]["project-a"]
    assert smc["records_in_tree"] is True
    for key in ("runlog", "baseline"):
        assert smc[key] == before["projects"]["project-a"][key]
    for project, body in after["projects"].items():
        if project != "project-a":
            assert "records_in_tree" not in body, project
        # every detector kept, as configured
        assert body["reserve_detector"] == \
            before["projects"][project]["reserve_detector"] == "smc-ladder"
    # the operator's numbers, unchanged
    assert after["backend"]["profile"] == before["backend"]["profile"]
    assert after["backend"].get("dedicated_budget_gb") == \
        before["backend"].get("dedicated_budget_gb")
    assert after["backend"]["sha256"] == before["backend"]["sha256"]
    # the retired backend's guard path is commented out, not kept as a key
    assert "guard" not in after["backend"]
    assert re.search(r"^#.*guard = ", config.read_text(), re.M)
    rows = {r["key"]: r for r in json.loads(result.stdout)["effective"]}
    assert rows["projects.project-a.records_in_tree"][
        "verdict"] == "same"


def _engine(tmp: Path, *args, case="j", rows=None, home=None,
            command=("-c", "pass")):
    """The internal guard run directly in a sandbox (the test-only claim
    bypass, a sandbox guard home, a fake free-memory reading)."""
    home = home or mark_sandbox(tmp / "home")
    free = tmp / "free"
    if not free.exists():
        free.write_text(str(64 * GB))
    baseline = tmp / "baseline.json"
    if rows is not None:
        baseline.write_text(json.dumps({
            "schema": "agent-compute-baseline/1",
            "cases": {c: {"venue": "host", "kind": "measured",
                          "peak_rss_gb": peak, "wall_s": 1.0, "threads": 1,
                          "workers": 1} for c, peak in rows.items()}}))
    env = QuietEnv((k, v) for k, v in os.environ.items()
                   if not k.startswith(("SMC_GUARD_", "ACS_")))
    env.update({"ACS_GUARD_HOME": str(home), conftest.FREE_MEM_ENV: str(free),
                "ACS_GUARD_TEST_UNCLAIMED": "1"})
    out = tmp / "out" / case
    argv = [PY, str(ENGINE), "run", "--case", case, "--profile-file",
            str(REPO / "tests" / "guard_profiles" / "mac.json"),
            "--cwd", str(tmp), "--outputs", str(out),
            "--runlog", str(tmp / "runlog.md"), "--baseline", str(baseline),
            "--interval-s", "0.25", *args, "--", PY, *command]
    result = subprocess.run(argv, capture_output=True, text=True, env=env,
                            timeout=90)
    record = json.loads((out / "guard.json").read_text()) \
        if (out / "guard.json").is_file() else None
    return result, record


def test_a_refused_attempt_writes_no_runlog_line(tmp_path):
    result, record = _engine(tmp_path)          # unmeasured, no reserve
    assert result.returncode == 3, result.stderr
    assert not (tmp_path / "runlog.md").exists()
    assert record["outcome"] == "refused"
    assert "no measured peak" in record["refusal"]


def test_a_lock_busy_attempt_writes_no_runlog_line(tmp_path):
    home = mark_sandbox(tmp_path / "home")
    with open(home / "lock", "a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        result, record = _engine(tmp_path, "--reserve-gb", "0.5", home=home)
    assert result.returncode == 4, result.stderr
    assert not (tmp_path / "runlog.md").exists()
    assert record["outcome"] == "lock-busy"
    assert "another job holds the lock" in record["refusal"]


def test_an_admitted_attempt_writes_its_notes_then_its_end(tmp_path):
    """A note made before the launch (here: a ladder that declares no
    reserve, admitted on its measurement) is written once the attempt is
    admitted, before its end line."""
    (tmp_path / "ladder.json").write_text(json.dumps(
        {"name": "l", "ladder": [{"name": "a"}, {"name": "b"}]}))
    result, record = _engine(tmp_path, "--reserve-detector", "smc-ladder",
                             rows={"j": 0.5},
                             command=("-c", "pass", "ladder.json"))
    assert result.returncode == 0, result.stderr
    kinds = [line.split(" | ")[3] for line in
             (tmp_path / "runlog.md").read_text().splitlines()
             if line.startswith("- ")]
    assert kinds == ["guard:ladder-undeclared", "completed"], kinds
    assert record.get("refusal") is None


def test_retries_through_acs_add_no_runlog_line(env_direct):
    env = env_direct
    env.free.write_text(str(1 * GB))           # below the floor + reserve
    job = env.submit(workload_args=["--out", str(env.root / "w")])
    # the third attempt has started: the first two were refused
    env.wait_for(lambda: (env.state_of(job["job_id"]).get("attempt") or 0)
                 >= 3, timeout=60, what="two refused attempts")
    env.free.write_text(str(64 * GB))
    state = env.wait_outcome(job["job_id"], timeout=90)
    assert state["outcome"] == "succeeded", state
    runlog = (env.state / "projects" / "alpha" / "runlog.md").read_text()
    lines = [line for line in runlog.splitlines() if line.startswith("- ")]
    assert not [line for line in lines if "guard:" in line], runlog
    assert len([line for line in lines if " | completed | " in line]) == 1
    attempts = sorted((env.state / "jobs" / job["job_id"] / "guard")
                      .glob("attempt-*/guard.json"))
    refusals = [json.loads(p.read_text()).get("refusal") for p in attempts]
    assert sum(1 for r in refusals if r and "host available memory" in r) \
        >= 2, refusals


def test_doctor_warns_when_records_are_inside_the_inputs(env_direct):
    env = env_direct
    edit(env, 'runlog = "{state}/projects/{project}/runlog.md"\n',
         'runlog = "docs/runlog.md"\nrecords_in_tree = true\n')
    report = doctor(env)
    alpha = check_named(report, "project alpha records")
    assert alpha["result"] == "warn" and alpha["level"] == "warning"
    assert "docs/runlog.md" in json.dumps(alpha["detail"])
    assert check_named(report, "project beta records")["result"] == "pass"
    assert report["ok"] is True                 # a warning, not an error
    edit(env, "records_in_tree = true\n",
         'records_in_tree = true\ninputs = [".", ":(exclude)docs"]\n')
    assert check_named(doctor(env), "project alpha records")["result"] == \
        "pass"


# --- D8: pin-backend as a recorded authorization -------------------------------------------

def evidence(env: Env, sha: str, *, docker="WITH Docker",
             mode="profile standard", host="testhost",
             name="pytest-mac.txt") -> Path:
    repo = env.root / "evidence-repo"
    if not repo.exists():
        repo.mkdir()
        git(repo, "init", "-q", "-b", "main")
    path = repo / name
    path.write_text(gate_text(
        gate_header(host, sha, docker=docker, mode=mode),
        gate_body(reason="[platform] Linux host only")))
    git(repo, "add", name)
    git(repo, "commit", "-q", "-m", "evidence")
    return path


def unpinned(env: Env) -> str:
    sha = sha256(ENGINE)
    edit(env, f'sha256 = "{sha}"', f'sha256 = "{"0" * 64}"')
    return sha


def test_pin_backend_appends_the_authorization_record(env_direct):
    env = env_direct
    sha = unpinned(env)
    path = evidence(env, sha)
    result = env.acs("pin-backend", "--evidence", str(path),
                     "--authorized-by", AUTH, "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    assert tomllib.loads(env.config.read_text())["backend"]["sha256"] == sha
    report = json.loads(result.stdout)
    assert report["evidence"]["docker"] == "with"
    lines = (env.state / "authorizations.jsonl").read_text().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["schema"] == "agent-compute-authorization/1"
    assert record["action"] == "pin-backend"
    assert record["authorized_by"] == AUTH
    assert record["config"] == str(env.config)
    assert record["evidence"]["path"] == str(path.resolve())
    assert record["evidence"]["commit"]
    assert record["evidence"]["docker"] == "with"
    assert record["diff_summary"] == {
        "changed": {"backend.sha256": ["0" * 64, sha]}}
    assert record["version"] and "source_sha" in record
    assert record["host"] == "testhost" and record["host_id"]
    assert record["engine_sha256"] == sha
    assert oct((env.state / "authorizations.jsonl").stat().st_mode
               & 0o777) == "0o600"


@pytest.mark.parametrize("auth", [None, "", "   ", "two\nlines"])
def test_pin_backend_without_an_authorization_is_refused(env_direct, auth):
    env = env_direct
    sha = unpinned(env)
    path = evidence(env, sha)
    before = env.config.read_text()
    args = ["pin-backend", "--evidence", str(path)]
    if auth is not None:
        args += ["--authorized-by", auth]
    result = env.acs(*args)
    assert result.returncode != 0, result.stdout
    assert env.config.read_text() == before
    assert not (env.state / "authorizations.jsonl").exists()


def test_pin_backend_has_no_terminal_ceremony():
    with pytest.raises(SystemExit):
        cli.parser().parse_args(["pin-backend", "--evidence", "x.txt",
                                 "--i-am-the-operator"])
    source = inspect.getsource(cli.cmd_pin_backend)
    assert "input(" not in source and "isatty" not in source


@pytest.mark.parametrize("phrase,expected", [
    ("WITH Docker", "with"), ("mac WITH DOCKER (the gate)", "with"),
    ("DOCKER HIDDEN (/usr/local/bin off PATH)", "hidden"),
    ("no word about it", "not stated")])
def test_the_evidence_check_reports_whether_the_run_had_docker(
        env_direct, phrase, expected):
    sha = sha256(ENGINE)
    path = evidence(env_direct, sha, docker=phrase)
    problem, record = cli.pin_evidence_problem(path, sha, "testhost")
    assert problem is None, problem
    assert record["docker"] == expected


@pytest.mark.parametrize("mode,ok", [
    ("profile standard", True), ("profile legacy", True),
    ("backend acs-guard (ACS_TEST_BACKEND=acs-guard)", True),
    ("backend smc-guard (ACS_TEST_GUARD)", False), ("", False)])
def test_the_evidence_must_have_run_the_internal_engine(env_direct, mode,
                                                        ok):
    sha = sha256(ENGINE)
    path = evidence(env_direct, sha, mode=mode)
    problem, _ = cli.pin_evidence_problem(path, sha, "testhost")
    assert (problem is None) == ok, problem


def test_doctor_shows_the_last_pin_authorization(env_direct):
    env = env_direct
    sha = unpinned(env)
    path = evidence(env, sha, docker="DOCKER HIDDEN")
    assert env.acs("pin-backend", "--evidence", str(path), "--authorized-by",
                   AUTH).returncode == 0
    check = check_named(doctor(env), "backend pin authorization")
    assert check["detail"]["authorized_by"] == AUTH
    assert check["detail"]["evidence"]["docker"] == "hidden"
    assert check["detail"]["engine_sha256"] == sha


# --- docs/memory-model.md -------------------------------------------------------------------

def test_the_memory_model_quotes_the_code_verbatim():
    doc = (REPO / "docs" / "memory-model.md").read_text()
    sources = {name: (REPO / "src" / "agent_compute" / name).read_text()
               for name in ("guard/engine.py", "config.py")}
    blocks = re.findall(r"```python\n# (\S+)\n(.*?)```", doc, re.S)
    assert len(blocks) >= 5, "the formulae are quoted from the code"
    for name, block in blocks:
        source = sources[name]
        for line in block.splitlines():
            if line.strip() and line.strip() != "...":
                assert line.rstrip() in source, (name, line)
    for phrase in ("floor_gb", "Docker Desktop", "credit 0", "budget_gb",
                   "1.5", "pause", "ceiling"):
        assert phrase in doc, phrase
