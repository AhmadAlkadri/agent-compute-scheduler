"""Campaign G slice G2a: neutral names with recorded aliases (decision D2),
record identity, the nested pass-through claim and the dispatch-time
container recheck (register C6).

What this establishes:

* the internal guard exports ``ACS_JOB_MARKER`` (plus each configured
  alias, same value) and ``ACS_CASE``, ``ACS_WORKERS``,
  ``ACS_DENSE_THREADS``; the legacy names (``SMC_GUARD_CASE``,
  ``SMC_GUARD_WORKERS``, ``SMC_DENSE_BLAS_THREADS``) only with
  ``--legacy-workload-env``; a container workload gets the same names;
* through ACS: a job runs under the configured guard home (default
  ``<state_dir>/guard``) and its workload sees the configured names;
* records: ``guard.json`` is ``agent-compute-guard-record/1`` with the
  marker names and the engine sha256; a new baseline file is
  ``agent-compute-baseline/1``; a legacy ``smc-perf-baseline/1`` table is
  read and keeps its schema; ``procs.record_problem`` accepts both record
  schemas and refuses an unknown one; the in-container pid file and the
  VM temp dir have neutral names; the engine's comments and help carry no
  project history but one note;
* the manifest records the names, the guard home, the schema versions, the
  version and source sha, the host id and an optional submitter;
* the evidence functions take the marker names from the job's manifest (a
  manifest written by v0.1.2 has none: ``SMC_GUARD_JOB``), never fewer than
  the names any version exported;
* the engine's nested pass-through requires the enclosing claimed ACS job;
* dispatch refuses (never started) a job whose container the project no
  longer lists, or whose guard home or markers changed since submission.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from conftest import BIN, ENGINE, REPO, Env, QuietEnv, sha256
from test_internal_guard import Sandbox

from agent_compute import cli, procs
from agent_compute import config as cfgmod
from agent_compute import runner as runmod
from agent_compute.state import JobStore, write_json

PY = sys.executable

sys.path.insert(0, str(REPO / "src" / "agent_compute" / "guard"))
import engine  # noqa: E402


@pytest.fixture
def sandbox(tmp_path):
    return Sandbox(tmp_path)


ENV_PROBE = [PY, "-c", "import json, os; print(json.dumps({k: v for k, v "
             "in os.environ.items() if k.startswith(('ACS_', 'SMC_', "
             "'MY_'))}))"]


def env_of(result) -> dict:
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.strip().splitlines()[-1])


# --- the engine's names ------------------------------------------------------------

def test_the_engine_exports_neutral_names_by_default(sandbox):
    sandbox.set_row("env", 0.01)
    env = env_of(sandbox.run("run", "env", ENV_PROBE, "--dense-threads",
                             "2"))
    assert env["ACS_JOB_MARKER"].startswith("env/")
    assert env["ACS_CASE"] == "env" and env["ACS_WORKERS"] == "1"
    assert env["ACS_DENSE_THREADS"] == "2"
    for legacy in ("SMC_GUARD_JOB", "SMC_GUARD_CASE", "SMC_GUARD_WORKERS",
                   "SMC_DENSE_BLAS_THREADS"):
        assert legacy not in env, legacy


def test_the_engine_exports_aliases_and_legacy_names_when_asked(sandbox):
    sandbox.set_row("env", 0.01)
    env = env_of(sandbox.run("run", "env", ENV_PROBE, "--dense-threads", "2",
                             "--marker-alias", "SMC_GUARD_JOB",
                             "--marker-alias", "MY_JOB",
                             "--legacy-workload-env"))
    marker = env["ACS_JOB_MARKER"]
    assert env["SMC_GUARD_JOB"] == marker and env["MY_JOB"] == marker
    assert env["SMC_GUARD_CASE"] == env["ACS_CASE"] == "env"
    assert env["SMC_GUARD_WORKERS"] == env["ACS_WORKERS"]
    assert env["SMC_DENSE_BLAS_THREADS"] == env["ACS_DENSE_THREADS"] == "2"
    record = sandbox.record("env")
    assert record["markers"] == ["ACS_JOB_MARKER", "SMC_GUARD_JOB", "MY_JOB"]


@pytest.mark.parametrize("alias", ["ACS_JOB_MARKER", "lower", "ACS_JOB_ID",
                                   "PATH"])
def test_the_engine_refuses_a_bad_alias(sandbox, alias):
    sandbox.set_row("env", 0.01)
    result = sandbox.run("run", "env", ENV_PROBE, "--marker-alias", alias)
    assert result.returncode == engine.EXIT_USAGE
    assert not (sandbox.outputs / "env").exists()


def test_the_slice_falls_back_to_acs_slice_and_legacy_only_when_asked(
        sandbox):
    sandbox.set_row("s", 0.01)
    args = [a for a in sandbox.args("run", "s") if a not in ("--slice",
                                                              "test")]
    for extra, env, expected in (
            ([], {"ACS_SLICE": "A1"}, "A1"),
            ([], {"SMC_SLICE": "L1"}, "-"),
            (["--legacy-workload-env"], {"SMC_SLICE": "L1"}, "L1")):
        result = subprocess.run([*args, *extra, "--", PY, "-c", "pass"],
                                env={**sandbox.env, **env},
                                capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stderr
        assert sandbox.record("s")["slice"] == expected, (extra, env)


def test_records_carry_the_neutral_schemas(sandbox):
    sandbox.set_row("legacy", 0.01)
    result = sandbox.run("run", "legacy", [PY, "-c", "pass"])
    assert result.returncode == 0, result.stderr
    record = sandbox.record("legacy")
    assert record["schema"] == "agent-compute-guard-record/1"
    assert record["engine_sha256"] == sha256(ENGINE)
    assert record["markers"] == ["ACS_JOB_MARKER"]
    # an existing legacy table is read and keeps its schema
    table = json.loads(sandbox.baseline.read_text())
    assert table["schema"] == "smc-perf-baseline/1"
    assert "legacy" in table["cases"]
    # a new table is neutral
    sandbox.baseline.unlink()
    result = sandbox.run("probe", "fresh", [PY, "-c", "pass"],
                         "--reserve-gb", "0.5")
    assert result.returncode == 0, result.stderr
    assert json.loads(sandbox.baseline.read_text())["schema"] == \
        "agent-compute-baseline/1"
    assert engine.GUARD_RECORD_SCHEMAS == ("agent-compute-guard-record/1",
                                           "smc-guard-record/1")
    assert engine.BASELINE_SCHEMAS == ("agent-compute-baseline/1",
                                       "smc-perf-baseline/1")


def test_record_problem_accepts_both_schemas_and_refuses_others():
    for schema in ("agent-compute-guard-record/1", "smc-guard-record/1"):
        assert procs.record_problem({"schema": schema, "pid": 1}) is None
    assert procs.record_problem({"pid": 1}) is None     # crafted, no schema
    problem = procs.record_problem({"schema": "other/9", "pid": 1})
    assert problem and "other/9" in problem


def test_neutral_names_in_container_and_temp_paths():
    source = ENGINE.read_text()
    assert "/tmp/acs-guard-" in source and "/tmp/smc-guard-" not in source
    assert 'prefix="acs-guard-vm-"' in source
    assert "smc-guard-vm-" not in source


# history words of the project the engine was extracted from (the names of
# the private projects are screened by the private tree's release tests, which
# run this same check with them; only the project-neutral words live here)
HISTORY = re.compile(r"guard\.py|\bLA2\b|"
                     r"\bB0\.[0-9]|\bB3\.3\b|\bB4\.1\b|\bLD[0-9]\b|roadmap|"
                     r"operator rule|bie/|ladder_drive|\bb2[0-9]_|\bb3[0-9]_|"
                     r"renders: 60|steps=840|frames=388|plot_trisurf|"
                     r"render-translate|qa-baseline|scripts/dev/queue")


def test_the_engine_carries_no_project_history_but_one_note():
    """Every history word is inside the one "History:" paragraph of the
    module docstring, which points at the extraction campaign."""
    text = ENGINE.read_text()
    start = text.index("History:")
    end = text.index("\n\n", start)
    note = text[start:end]
    assert "docs/campaign-extraction.md" in note
    assert text.count("History:") == 1
    outside = text[:start] + text[end:]
    hits = [line.strip() for line in outside.splitlines()
            if HISTORY.search(line)]
    assert not hits, hits


def test_help_texts_are_neutral():
    result = subprocess.run([PY, str(ENGINE), "run", "--help"],
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0
    assert not HISTORY.search(result.stdout), result.stdout
    for option in ("--marker-alias", "--legacy-workload-env"):
        assert option in result.stdout
    assert set(cli.REQUIRED_ENGINE_OPTIONS) >= {"--marker-alias",
                                                "--legacy-workload-env"}


def test_the_marker_constants_agree():
    assert cfgmod.JOB_MARKER == engine.JOB_MARKER == "ACS_JOB_MARKER"
    assert cfgmod.LEGACY_JOB_MARKER == engine.LEGACY_JOB_MARKER == \
        "SMC_GUARD_JOB"
    assert set(procs.KNOWN_MARKERS) == {"ACS_JOB_MARKER", "SMC_GUARD_JOB"}


# --- through ACS -----------------------------------------------------------------------

@pytest.fixture
def acs_env():
    env = Env(queue=False)
    yield env
    env.cleanup()


def _edit(env: Env, old: str, new: str) -> None:
    text = env.config.read_text()
    assert old in text, old
    env.config.write_text(text.replace(old, new, 1))


def _probe_job(env: Env, *extra) -> tuple[str, dict]:
    out = env.root / f"probe-{len(list(env.root.glob('probe-*')))}.json"
    script = ("import json, os, sys; json.dump({k: v for k, v in "
              "os.environ.items() if k.startswith(('ACS_', 'SMC_', 'MY_'))}, "
              "open(sys.argv[1], 'w'))")
    result = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--cwd",
                     str(env.projects["alpha"]), *extra, "--", PY, "-c",
                     script, str(out))
    assert result.returncode == 0, result.stdout + result.stderr
    job = json.loads(result.stdout)["job_id"]
    state = env.wait_outcome(job)
    assert state["outcome"] == "succeeded", state
    return job, json.loads(out.read_text())


def test_a_job_runs_under_the_default_guard_home(acs_env):
    env = acs_env
    _edit(env, f'guard_home = "{env.guard_home}"\n', "")
    job, seen = _probe_job(env)
    home = env.state / "guard"
    assert seen["ACS_GUARD_HOME"] == str(home)
    assert (home / "lock").exists()
    assert not (env.guard_home / "lock").exists()
    man = json.loads((env.state / "jobs" / job / "manifest.json")
                     .read_text())
    assert man["guard_home"] == str(home)
    assert seen["ACS_JOB_MARKER"].startswith("alpha.t/")
    assert "SMC_GUARD_JOB" not in seen and "SMC_GUARD_CASE" not in seen
    assert seen["ACS_CASE"] == "alpha.t"


def test_a_job_sees_the_configured_aliases(acs_env):
    env = acs_env
    _edit(env, 'lock_wait_max_s', 'marker_aliases = ["SMC_GUARD_JOB"]\n'
               'legacy_workload_env = true\nlock_wait_max_s')
    job, seen = _probe_job(env)
    assert seen["SMC_GUARD_JOB"] == seen["ACS_JOB_MARKER"]
    assert seen["SMC_GUARD_CASE"] == seen["ACS_CASE"] == "alpha.t"
    man = json.loads((env.state / "jobs" / job / "manifest.json")
                     .read_text())
    assert man["marker"] == "ACS_JOB_MARKER"
    assert man["marker_aliases"] == ["SMC_GUARD_JOB"]
    assert man["legacy_workload_env"] is True


def test_the_manifest_records_names_versions_and_identity(acs_env):
    env = acs_env
    job, _ = _probe_job(env, "--submitter", "session-abc")
    man = json.loads((env.state / "jobs" / job / "manifest.json")
                     .read_text())
    from agent_compute import identity
    ident = identity.identity()
    assert man["acs_version"] == ident["version"]
    import agent_compute
    assert man["acs_version"].startswith(agent_compute.__version__)
    assert man["acs_source_sha"] == ident["source_sha"]
    assert man["host"] == "testhost"
    host_id = json.loads((env.state / "host-id.json").read_text())["host_id"]
    assert man["host_id"] == host_id
    assert man["marker"] == "ACS_JOB_MARKER" and man["marker_aliases"] == []
    assert man["guard_home"] == str(env.guard_home)
    assert man["schemas"] == {
        "manifest": "agent-compute-manifest/1",
        "state": "agent-compute-state/1",
        "config": "agent-compute-config/2",
        "guard_record": "agent-compute-guard-record/1",
        "baseline": "agent-compute-baseline/1",
        "event": "agent-compute-event/1"}
    assert man["submitter"] == "session-abc"
    job2, _ = _probe_job(env)
    man2 = json.loads((env.state / "jobs" / job2 / "manifest.json")
                      .read_text())
    assert man2["submitter"] is None
    env_vars = env.env(AGENT_COMPUTE_SUBMITTER="from-env")
    result = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--cwd",
                     str(env.projects["alpha"]), "--", PY, "-c", "pass",
                     env=env_vars)
    job3 = json.loads(result.stdout)["job_id"]
    env.wait_outcome(job3)
    man3 = json.loads((env.state / "jobs" / job3 / "manifest.json")
                      .read_text())
    assert man3["submitter"] == "from-env"


def test_version_flag_prints_the_version_and_sha():
    result = subprocess.run([PY, str(BIN), "--version"], capture_output=True,
                            text=True, timeout=60)
    assert result.returncode == 0
    from agent_compute import identity
    ident = identity.identity()
    assert result.stdout.split() == [ident["version"],
                                     ident["source_sha"] or "unknown"]
    import agent_compute
    assert ident["package_version"] == agent_compute.__version__


def test_identity_of_an_installed_copy(tmp_path):
    from agent_compute import identity
    vdir = tmp_path / "prefix" / "versions" / "0.2.1-0123456789"
    (vdir / "src" / "agent_compute").mkdir(parents=True)
    (vdir / "SOURCE").write_text("0123456789" + "ab" * 15 + "\n")
    got = identity.identity_of(vdir)
    # package_version is the running package's (identity_of reads no code)
    assert got == {"version": "0.2.1-0123456789",
                   "source_sha": "0123456789" + "ab" * 15,
                   "package_version": identity.__version__}
    (vdir / "SOURCE").unlink()
    assert identity.identity_of(vdir)["source_sha"] == "0123456789"


DIAGNOSTIC = {"version", "source_sha", "safety_level", "engine_sha256",
              "host", "host_id", "utc"}


def test_diagnostic_json_carries_the_identity(acs_env):
    env = acs_env
    doctor = json.loads(env.acs("doctor", "--json").stdout)
    assert DIAGNOSTIC <= set(doctor), set(doctor)
    assert doctor["engine_sha256"] == sha256(ENGINE)
    assert doctor["host"] == "testhost" and doctor["host_id"]
    assert doctor["acs"]["host_id"] == doctor["host_id"]
    for args in (["status", "--json"], ["status", "--all", "--json"],
                 ["reconcile", "--json"]):
        report = json.loads(env.acs(*args).stdout)
        assert DIAGNOSTIC <= set(report["acs"]), (args, report.get("acs"))
        assert report["acs"]["host_id"] == doctor["host_id"]


# --- the evidence functions read the manifest's names ---------------------------------------

def test_job_markers_from_a_manifest():
    old = {"job_id": "acs-x", "host": "h"}            # v0.1.2 shape
    assert procs.job_markers(old) == ("SMC_GUARD_JOB",)
    new = {"marker": "ACS_JOB_MARKER", "marker_aliases": ["MY_JOB"]}
    assert procs.job_markers(new) == ("ACS_JOB_MARKER", "MY_JOB")
    # the scan never looks for fewer names than any version exported
    assert set(procs.scan_markers(old)) == {"ACS_JOB_MARKER",
                                            "SMC_GUARD_JOB"}
    assert set(procs.scan_markers(new)) == {"ACS_JOB_MARKER",
                                            "SMC_GUARD_JOB", "MY_JOB"}


def _capture_docker(monkeypatch):
    calls = []

    def fake(*args, **_kw):
        calls.append(args)
        if args[0] == "inspect":
            return subprocess.CompletedProcess(args, 0, "c" * 64 + " true",
                                               "")
        if args[0] == "info":
            return subprocess.CompletedProcess(args, 0, "D\n", "")
        if args[0] == "exec":
            if args[8] == procs._JOB_SCAN:
                return subprocess.CompletedProcess(args, 0, json.dumps(
                    {"btime": 0, "tck": 100, "now": 1e9, "settled": True,
                     "procs": []}), "")
            return subprocess.CompletedProcess(
                args, 0, json.dumps({"groups": [], "pids": [],
                                     "skipped": []}), "")
        raise AssertionError(args)
    monkeypatch.setattr(procs, "docker", fake)
    return calls


@pytest.mark.parametrize("manifest,expected", [
    ({}, {"ACS_JOB_MARKER", "SMC_GUARD_JOB"}),
    ({"marker": "ACS_JOB_MARKER", "marker_aliases": ["MY_JOB"]},
     {"ACS_JOB_MARKER", "SMC_GUARD_JOB", "MY_JOB"})])
def test_the_container_scan_and_signal_take_the_manifest_s_names(
        monkeypatch, manifest, expected):
    calls = _capture_docker(monkeypatch)
    container = {"name": "c", "id": "c" * 64}
    record = {"job_id": "alpha.t/20261006T000000Z", "pid": 5}
    markers = procs.scan_markers(manifest)
    procs.container_job_evidence(container, record, None, markers=markers)
    procs.signal_container_job(container, record, None, 15,
                               markers=markers)
    helpers = [c for c in calls if c[0] == "exec"]
    assert [h[8] for h in helpers] == [procs._JOB_SCAN, procs._JOB_SCAN]
    for helper in helpers:
        assert set(json.loads(helper[-1])) == expected, helper[9:]


def test_reconcile_of_a_v012_job_uses_its_recorded_names(monkeypatch,
                                                          acs_env):
    """A job submitted by 0.1.0-63b148e3ad / v0.1.2: its manifest has no
    marker fields; reconcile scans for SMC_GUARD_JOB (and the neutral
    name), never for a name it does not know."""
    env = acs_env
    calls = _capture_docker(monkeypatch)
    store = JobStore(env.state)
    job = "acs-20261005T120000Z-0a1b2c"
    job_dir = store.job_dir(job)
    (job_dir / "guard" / "attempt-1").mkdir(parents=True)
    manifest = {"schema": "agent-compute-manifest/1", "job_id": job,
                "acs_version": "0.1.0", "host": "testhost",
                "project": "alpha", "argv": ["x"],
                "container": {"name": "c", "id": "c" * 64,
                              "workdir": "/w", "daemon_id": "D"},
                "backend": {"kind": "acs-guard", "path": "/v/engine.py",
                            "sha256": "0" * 64}}
    write_json(job_dir / "manifest.json", manifest)
    write_json(job_dir / "state.json", {"job_id": job, "phase": "done",
                                        "outcome": "unknown",
                                        "history": []})
    write_json(job_dir / "guard" / "attempt-1" / "guard.json",
               {"schema": "smc-guard-record/1",
                "job_id": "alpha.t/20261005T120000Z", "pid": 7,
                "outcome": "completed"})
    (job_dir / "guard" / "attempt-1" / "samples.csv").write_text("t\n")
    store.set_block("testhost", job, "test", {})
    facts, _ = cli._reconcile_job(store, job, {}, None)
    scans = [c for c in calls if c[0] == "exec" and c[8] == procs._JOB_SCAN]
    assert scans, calls
    assert "SMC_GUARD_JOB" in json.loads(scans[0][-1])
    assert facts["cleanup"]["clean"] is True, facts


SCAN_HERE = r"""
import json, os, subprocess, sys, time
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'],
                         env={'PATH': os.environ['PATH'], NAME: 'j/1'})
time.sleep(0.5)
try:
    out = subprocess.run([sys.executable, '-I', '-S', '-c', SCRIPT, 'j/1',
                          json.dumps(MARKERS)], capture_output=True,
                         text=True, timeout=30)
    procs = json.loads(out.stdout)['procs']
    print(json.dumps([p['env'] for p in procs if p['pid'] == child.pid]))
finally:
    child.kill()
"""


@pytest.mark.skipif(not sys.platform.startswith("linux"),
                    reason="[platform] Linux host only: runs the "
                           "in-container scan on the host's own /proc")
@pytest.mark.parametrize("name,markers,expected", [
    ("MY_JOB", ["ACS_JOB_MARKER", "SMC_GUARD_JOB", "MY_JOB"], "has-marker"),
    ("SMC_GUARD_JOB", ["ACS_JOB_MARKER", "SMC_GUARD_JOB"], "has-marker"),
    ("MY_JOB", ["ACS_JOB_MARKER", "SMC_GUARD_JOB"], "no-marker")])
def test_the_scan_script_matches_any_listed_name(name, markers, expected):
    code = SCAN_HERE.replace("NAME", repr(name)).replace(
        "SCRIPT", repr(procs._JOB_SCAN)).replace("MARKERS", repr(markers))
    result = subprocess.run([PY, "-c", code], capture_output=True,
                            text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == [expected]


# --- the nested pass-through needs the enclosing claim ------------------------------------

def _nested(tmp_path: Path, env_extra: dict) -> subprocess.CompletedProcess:
    profile = tmp_path / "profile.json"
    from conftest import TEST_PROFILE
    profile.write_text(json.dumps({"schema": "agent-compute-guard-profile/1",
                                   **{k: (v or None) if k == "runlog_tag"
                                      else v
                                      for k, v in TEST_PROFILE.items()}}))
    out = tmp_path / "nested-ran"
    env = QuietEnv((k, v) for k, v in os.environ.items()
                   if not k.startswith(("SMC_GUARD_", "ACS_")))
    env.update(env_extra)
    argv = [PY, str(ENGINE), "run", "--case", "inner", "--", PY, "-c",
            f"open({str(out)!r}, 'w').write('x')"]
    result = subprocess.run(argv, capture_output=True, text=True, env=env,
                            timeout=60)
    result.ran = out.exists()
    return result


def _enclosing(tmp_path: Path, *, outcome=None, runner=True,
               marker="outer/20261006T000000Z", acs_job=None) -> dict:
    home = tmp_path / "home"
    home.mkdir(exist_ok=True)
    state = tmp_path / "state"
    job = "acs-20261006T000000Z-abcdef"
    job_dir = state / "jobs" / job
    job_dir.mkdir(parents=True, exist_ok=True)
    st = {"job_id": job}
    if runner:
        st["runner"] = {"pid": os.getpid()}
    if outcome:
        st["outcome"] = outcome
    (job_dir / "state.json").write_text(json.dumps(st))
    (home / "inflight.json").write_text(json.dumps(
        {"schema": "agent-compute-guard-inflight/1",
         "job_id": "outer/20261006T000000Z", "acs_job": acs_job or job,
         "acs_state": str(state)}))
    return {"ACS_GUARD_HOME": str(home), "ACS_JOB_ID": job,
            "ACS_JOB_MARKER": marker}


def test_a_marker_alone_no_longer_passes_through(tmp_path):
    for env_extra in ({"SMC_GUARD_JOB": "outer/1"},
                      {"ACS_JOB_MARKER": "outer/1"},
                      {"ACS_JOB_MARKER": "outer/1", "ACS_JOB_ID": "acs-x"}):
        result = _nested(tmp_path, env_extra)
        assert result.returncode == engine.EXIT_USAGE, result.stderr
        assert "nested" in result.stderr and not result.ran


def test_the_enclosing_claimed_job_lets_it_pass_through(tmp_path):
    result = _nested(tmp_path, _enclosing(tmp_path))
    assert result.returncode == 0, result.stderr
    assert "pass-through" in result.stderr and result.ran


@pytest.mark.parametrize("case", ["finished", "unclaimed", "other-marker",
                                  "other-job", "no-home"])
def test_a_stale_or_foreign_enclosing_job_is_refused(tmp_path, case):
    extra = _enclosing(tmp_path,
                       outcome="succeeded" if case == "finished" else None,
                       runner=case != "unclaimed",
                       marker="other/1" if case == "other-marker" else
                       "outer/20261006T000000Z",
                       acs_job="acs-20261006T000000Z-ffffff"
                       if case == "other-job" else None)
    if case == "no-home":
        extra.pop("ACS_GUARD_HOME")
    result = _nested(tmp_path, extra)
    assert result.returncode == engine.EXIT_USAGE, result.stderr
    assert not result.ran


def test_a_real_nested_call_inside_an_acs_job_passes_through(acs_env):
    env = acs_env
    inner = [PY, str(ENGINE), "run", "--case", "inner", "--", PY, "-c",
             "import os; print('inner-ran', os.environ['ACS_JOB_MARKER'])"]
    result = env.acs("submit", "--json", "--case", "outer", "--timeout-min",
                     "1", "--reserve-gb", "0.5", "--cwd",
                     str(env.projects["alpha"]), "--", "/bin/sh", "-c",
                     shlex.join(inner))
    assert result.returncode == 0, result.stdout + result.stderr
    job = json.loads(result.stdout)["job_id"]
    state = env.wait_outcome(job)
    assert state["outcome"] == "succeeded", state
    log = (env.state / "jobs" / job / "output.log").read_text()
    assert "pass-through" in log and "inner-ran alpha.outer/" in log


# --- dispatch-time rechecks ----------------------------------------------------------------

def test_dispatch_refuses_a_container_no_longer_listed(acs_env, monkeypatch):
    """C6: the runner loads the live config at dispatch; a project that
    stopped listing the job's container refuses it, never started."""
    from test_acs_guard_backend import prepare_job
    env = acs_env
    env.set_container("listed-c")
    config = cfgmod.load(env.config)
    job = prepare_job(env, config)
    man_path = env.state / "jobs" / job / "manifest.json"
    man = json.loads(man_path.read_text())
    man["container"] = {"name": "listed-c", "id": "c" * 64, "workdir": "/w",
                        "daemon_id": "D"}
    write_json(man_path, man)
    _edit(env, 'containers = ["listed-c"]', 'containers = ["other-c"]')
    _edit(env, 'container_limits = ["listed-c"]',
          'container_limits = ["other-c"]')
    runner = runmod.Runner(cfgmod.load(env.config), job)
    problem = runner.revalidate()
    assert problem and "listed-c" in problem and "no longer" in problem


@pytest.mark.parametrize("change", ["guard_home", "aliases",
                                    "legacy_workload_env"])
def test_dispatch_refuses_changed_names(acs_env, change):
    from test_acs_guard_backend import exec_runner, prepare_job
    env = acs_env
    job = prepare_job(env, cfgmod.load(env.config))
    if change == "guard_home":
        _edit(env, f'guard_home = "{env.guard_home}"',
              f'guard_home = "{env.root / "other-home"}"')
    elif change == "aliases":
        _edit(env, "lock_wait_max_s", 'marker_aliases = ["MY_JOB"]\n'
                                      "lock_wait_max_s")
    else:
        _edit(env, "lock_wait_max_s", "legacy_workload_env = true\n"
                                      "lock_wait_max_s")
    result = exec_runner(env, job)
    state = env.state_of(job)
    assert state["outcome"] == "refused", (state, result.stderr)
    assert "never started" in state["reason"]
    assert "since submission" in state["reason"]
    assert not (env.root / f"{job}.out").exists()


# --- review round 1 ------------------------------------------------------------------------

def test_the_test_bypass_needs_the_sandbox_sentinel_in_any_home(tmp_path):
    """Review round 1, item 1: with G2a any absolute guard home is a
    possible production home, so the test-only claim bypass works only in
    a home holding the harness's sentinel (``ACS_TEST_SANDBOX``), never in
    a custom home without it (refused like any unclaimed run, exit 7)."""
    from conftest import SANDBOX_SENTINEL, mark_sandbox
    from test_acs_guard_backend import _engine_run
    home = tmp_path / "custom" / "guard"            # a custom guard_home
    home.mkdir(parents=True)
    extra = {"ACS_GUARD_TEST_UNCLAIMED": "1", "ACS_GUARD_HOME": str(home)}
    result, out = _engine_run(tmp_path, extra, home=home)
    assert result.returncode == engine.EXIT_USAGE, result.stderr
    assert "sandbox" in result.stderr and SANDBOX_SENTINEL in result.stderr
    assert not out.exists() and not (home / "lock").exists()
    # a directory or a symlink at the name is not the sentinel
    (home / SANDBOX_SENTINEL).mkdir()
    result, out = _engine_run(tmp_path, extra, home=home)
    assert result.returncode == engine.EXIT_USAGE and not out.exists()
    (home / SANDBOX_SENTINEL).rmdir()
    mark_sandbox(home)
    result, out = _engine_run(tmp_path, extra, home=home)
    assert result.returncode == 0, result.stderr
    assert out.exists()
    assert engine.SANDBOX_SENTINEL == SANDBOX_SENTINEL


def test_the_sentinel_never_opens_a_real_home(tmp_path):
    """Even with the sentinel, the legacy home, the default ACS home and a
    home beside an ACS state dir stay refused."""
    from conftest import mark_sandbox
    from test_acs_guard_backend import _engine_run
    fake_user = tmp_path / "user"
    state = tmp_path / "state"
    (state / "jobs").mkdir(parents=True)
    for home in (fake_user / ".smc-guard",
                 fake_user / ".local" / "state" / "agent-compute" / "guard",
                 state / "guard"):
        mark_sandbox(home)
        result, out = _engine_run(tmp_path, {
            "HOME": str(fake_user), "ACS_GUARD_TEST_UNCLAIMED": "1",
            "ACS_GUARD_HOME": str(home)}, home=home)
        assert result.returncode == engine.EXIT_USAGE, (home, result.stderr)
        assert not out.exists() and not (home / "lock").exists()


def test_a_nested_call_under_a_dead_runner_is_refused(tmp_path):
    """Review round 1, item 4: the enclosing job's recorded runner pid is
    dead (the job was never finished: a lost runner) -> refused, exit 7."""
    dead = subprocess.Popen([PY, "-c", "pass"])
    dead.wait()
    extra = _enclosing(tmp_path)
    state_path = next((tmp_path / "state" / "jobs").glob("*/state.json"))
    st = json.loads(state_path.read_text())
    st["runner"] = {"pid": dead.pid}
    state_path.write_text(json.dumps(st))
    result = _nested(tmp_path, extra)
    assert result.returncode == engine.EXIT_USAGE, result.stderr
    assert "no live claimed runner" in result.stderr and not result.ran
