"""The ACS-internal guard (``agent_compute.guard``): the guard tests of the
earlier private research tool, carried over (see docs/provenance.md).

Ported from that tool's ``tests/test_guard.py`` (commit ``19590ef``, the
tests of its ``scripts/dev/guard.py`` ``f53ddde2…``).  Same sandbox pattern: an
isolated guard home per test (``ACS_GUARD_HOME``), the engine's
fake-memory hook (``conftest.FREE_MEM_ENV``) for the host memory reading,
its sampler-failure hook (``conftest.SAMPLER_FAIL_ENV``) for a blind
sampler, temporary baseline/runlog/outputs.  No real guard home, the ACS
queue and state and every shared container are never touched.

What changed in the port, and only that:
- the guard is run as ``python3 src/agent_compute/guard/engine.py`` with an
  explicit ``--profile-file`` and ``--cwd``, which guard.py defaulted from
  its checkout: the sandbox's generic ``tests/guard_profiles/sandbox.json``
  (guard.py's mac numbers under a neutral name, since G4), and the named
  examples ``mac.json``/``lenovo.json`` where a test is about a host's
  numbers; every fixture profile sets a 1 GB disk floor;
- the profile test selects profiles by file (there is no ``--profile NAME``
  or ``SMC_GUARD_PROFILE`` any more) and the ladder tests opt in with
  ``--reserve-detector smc-ladder``;
- the container tests use a disposable, labelled container from the
  harness's test image (``conftest.TEST_IMAGE``) instead of
  ``SMC_GUARD_TEST_CONTAINER=<running container>``;
- not carried over: ``test_launcher_gate_refuses_unguarded_and_accepts_guarded``
  and ``test_worker_gate_clamps_to_authorization``, which test that tool's
  ``membrane.jobguard`` package, not the guard.

The tests of what is new (profile parsing, the required project arguments,
the opt-in ladder detector) are at the end.
"""

from __future__ import annotations

import ast
import json
import os
import re
import secrets
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from conftest import (FREE_MEM_ENV, GUARD_ENV_PREFIXES, QuietEnv,
                      SAMPLER_FAIL_ENV,
                      TEST_IMAGE as IMAGE, needs_docker)

REPO = Path(__file__).resolve().parents[1]
ENGINE = REPO / "src" / "agent_compute" / "guard" / "engine.py"
HOG = REPO / "tests" / "memory_hog.py"
PROFILES = REPO / "tests" / "guard_profiles"
SANDBOX = PROFILES / "sandbox.json"   # generic: the default of every Sandbox
MAC = PROFILES / "mac.json"           # named examples (the operator hosts)
LENOVO = PROFILES / "lenovo.json"
PY = sys.executable
GB = 1024 ** 3


def load_engine():
    from agent_compute.guard import engine
    return engine


class Sandbox:
    def __init__(self, tmp_path: Path):
        self.tmp = tmp_path
        self.home = tmp_path / "guard-home"
        self.baseline = tmp_path / "baseline.json"
        self.runlog = tmp_path / "runlog.md"
        self.outputs = tmp_path / "out"
        self.fake_free = tmp_path / "free_bytes"
        self.fake_free.write_text(str(40 * GB))
        self.profile = SANDBOX
        self.env = QuietEnv((k, v) for k, v in os.environ.items()
                            if not k.startswith(GUARD_ENV_PREFIXES))
        self.env["ACS_GUARD_HOME"] = str(self.home)
        # the bypass below needs the harness's sandbox sentinel in the home
        from conftest import mark_sandbox
        mark_sandbox(self.home)
        self.env[FREE_MEM_ENV] = str(self.fake_free)
        self.env["PYTHONUNBUFFERED"] = "1"
        # S4: run/probe need an ACS claim; a sandbox (own guard home)
        # may bypass it with the test-only variable
        self.env["ACS_GUARD_TEST_UNCLAIMED"] = "1"

    def set_row(self, case: str, peak_gb: float, kind: str = "measured",
                workers: int = 1):
        data = {"schema": "smc-perf-baseline/1", "cases": {}}
        if self.baseline.is_file():
            data = json.loads(self.baseline.read_text())
        data["cases"][case] = {"venue": "host", "kind": kind,
                               "peak_rss_gb": peak_gb, "wall_s": 1.0,
                               "threads": 1, "workers": workers}
        self.baseline.write_text(json.dumps(data))

    def args(self, mode: str, case: str, *extra: str,
             outputs: str | None = None, profile: Path | None = None):
        out = outputs or str(self.outputs / case)
        return [PY, str(ENGINE), mode, "--case", case,
                "--profile-file", str(profile or self.profile),
                "--cwd", str(self.tmp),
                "--baseline", str(self.baseline), "--runlog", str(self.runlog),
                "--outputs", out, "--interval-s", "0.25", "--slice", "test",
                *extra]

    def run(self, mode: str, case: str, command: list[str], *extra: str,
            timeout: float = 60.0, outputs: str | None = None,
            profile: Path | None = None):
        return subprocess.run(
            [*self.args(mode, case, *extra, outputs=outputs, profile=profile),
             "--", *command],
            env=self.env, capture_output=True, text=True, timeout=timeout)

    def record(self, case: str) -> dict:
        return json.loads((self.outputs / case / "guard.json").read_text())

    def runlog_text(self) -> str:
        return self.runlog.read_text() if self.runlog.is_file() else ""


@pytest.fixture
def sandbox(tmp_path):
    return Sandbox(tmp_path)


def hog(gb: float, rate: float = 2.0, hold: float = 1.0) -> list[str]:
    return [PY, str(HOG), "--gb", str(gb), "--rate-gb-s", str(rate),
            "--hold-s", str(hold)]


def sleeper(seconds: float) -> list[str]:
    return [PY, "-c", f"import time; time.sleep({seconds})"]


def wait_for(predicate, timeout: float, what: str):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for {what}")


def process_state(pid: int) -> str:
    out = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)],
                         capture_output=True, text=True).stdout.strip()
    return out


def reader(path: Path):
    def record():
        try:
            return json.loads(path.read_text())
        except (OSError, ValueError):
            return {}
    return record


# --- unit-level arithmetic -----------------------------------------------------

def test_container_memory_cap_and_worker_arithmetic():
    guard = load_engine()
    assert guard.docker_memory_cap_gb(0.05, 24.0) == 1.0
    assert guard.docker_memory_cap_gb(2.0, 24.0) == 3.0
    assert guard.docker_memory_cap_gb(2.1, 24.0) == 4.0
    assert guard.docker_memory_cap_gb(20.0, 24.0) == 22.0
    assert guard.allowed_workers(0.0, 12.0, 12) == 1
    assert guard.allowed_workers(1.0, 12.0, 12) == 10
    assert guard.allowed_workers(5.0, 12.0, 12) == 2
    assert guard.allowed_workers(0.5, 12.0, 4) == 2


def test_host_memory_sample_reports_available_bytes(monkeypatch):
    guard = load_engine()
    monkeypatch.delenv(FREE_MEM_ENV, raising=False)
    sample = guard.host_memory_sample()
    assert sample["available"] > 0
    assert sample["available"] <= guard.host_memory_total()


# --- pre-flight ----------------------------------------------------------------

def test_refuses_unmeasured_case(sandbox):
    result = sandbox.run("run", "unknown-case", sleeper(0.1))
    assert result.returncode == 3, result.stderr
    assert "no measured peak" in result.stderr
    assert "probe --case unknown-case" in result.stderr
    # G2b (D3): a refusal writes no runlog line; guard.json keeps it
    assert "guard:refuse" not in sandbox.runlog_text()
    assert sandbox.record("unknown-case")["outcome"] == "refused"
    assert "no measured peak" in sandbox.record("unknown-case")["refusal"]


def test_refuses_estimate_that_does_not_fit(sandbox):
    sandbox.set_row("huge", 1000.0)
    result = sandbox.run("run", "huge", sleeper(0.1), "--budget-gb", "2000")
    assert result.returncode == 3
    assert "floor" in result.stderr and "estimate" in result.stderr
    assert "| test | huge/" not in sandbox.runlog_text()     # G2b (D3)
    assert "floor" in sandbox.record("huge")["refusal"]


def test_refuses_measured_peak_over_host_budget(sandbox):
    sandbox.set_row("too-big", 13.0)
    result = sandbox.run("run", "too-big", sleeper(0.1))
    assert result.returncode == 3
    assert "exceeds the host budget" in result.stderr


def test_refuses_worker_fanout_without_single_worker_peak(sandbox):
    sandbox.set_row("fan", 0.05, kind="probe", workers=2)
    result = sandbox.run("run", "fan", sleeper(0.1), "--workers", "3")
    assert result.returncode == 3
    assert "single-worker peak" in result.stderr
    sandbox.set_row("fan", 5.0)
    result = sandbox.run("run", "fan", sleeper(0.1), "--workers", "4")
    assert result.returncode == 3
    assert "exceed the allowed" in result.stderr


def test_probe_records_peak_and_completion(sandbox):
    result = sandbox.run("probe", "hog-probe", hog(0.3, rate=4.0, hold=0.5))
    assert result.returncode == 0, result.stderr
    row = json.loads(sandbox.baseline.read_text())["cases"]["hog-probe"]
    assert row["kind"] == "measured" and row["completed"] is True
    assert row["peak_rss_gb"] >= 0.25, row
    assert row["threads"] == 1 and row["workers"] == 1
    record = sandbox.record("hog-probe")
    assert record["outcome"] == "completed"
    assert record["peak_rss_gb"] >= 0.25
    assert (sandbox.outputs / "hog-probe" / "samples.csv").is_file()
    line = [l for l in sandbox.runlog_text().splitlines()
            if "hog-probe/" in l and "| completed |" in l]
    assert line and "probe" in line[0] and "peak_rss=" in line[0]


def test_probe_reserve_follows_declaration_then_measurement(sandbox):
    # declared reserve too large for the (fake) host -> refused with that estimate
    sandbox.fake_free.write_text(str(12 * GB))
    result = sandbox.run("probe", "reserve", sleeper(0.1), "--estimate-gb", "5")
    assert result.returncode == 3 and "estimate 5.00 GB" in result.stderr
    # a measured row sets the reserve; 0.5 GB fits under 12 GB free
    sandbox.set_row("reserve", 0.5)
    result = sandbox.run("probe", "reserve", sleeper(0.1))
    assert result.returncode == 0, result.stderr
    assert sandbox.record("reserve")["estimate_gb"] == 0.5
    # an oversized declaration is clamped to the budget, a tiny one to 0.25
    result = sandbox.run("probe", "reserve", sleeper(0.1), "--estimate-gb", "0.01", "--budget-gb", "3")
    assert result.returncode == 0 and sandbox.record("reserve")["estimate_gb"] == 0.25


def test_baseline_peak_never_decreases_under_the_same_case_key(sandbox):
    """B4.1: a case key's reserve is the LARGEST thing that ever ran under
    it, whatever the previous row's kind; this run's own peak is kept beside
    it as ``last_peak_rss_gb``."""
    # a heavy probe stopped at its limit: a `probe` row with a real peak
    result = sandbox.run("probe", "peak-key", hog(0.6, rate=6.0, hold=30),
                         "--probe-s", "1", "--estimate-gb", "2")
    assert result.returncode == 0, result.stderr
    heavy = json.loads(sandbox.baseline.read_text())["cases"]["peak-key"]
    assert heavy["kind"] == "probe" and heavy["completed"] is False
    assert heavy["peak_rss_gb"] >= 0.4, heavy

    # a cheap COMPLETED run under the same key must not lower the reserve
    result = sandbox.run("run", "peak-key", sleeper(0.2))
    assert result.returncode == 0, result.stderr
    row = json.loads(sandbox.baseline.read_text())["cases"]["peak-key"]
    assert row["kind"] == "measured" and row["completed"] is True
    assert row["peak_rss_gb"] == heavy["peak_rss_gb"], row
    assert row["last_peak_rss_gb"] < 0.2, row

    # a heavier run still raises it, and then carries no stale `last_...`
    result = sandbox.run("run", "peak-key", hog(1.2, rate=6.0, hold=0.5))
    assert result.returncode == 0, result.stderr
    row = json.loads(sandbox.baseline.read_text())["cases"]["peak-key"]
    assert row["peak_rss_gb"] > heavy["peak_rss_gb"], row
    assert "last_peak_rss_gb" not in row, row

    # the rule is per worker count, as it always was
    sandbox.set_row("peak-workers", 5.0, workers=4)
    result = sandbox.run("run", "peak-workers", sleeper(0.2))
    assert result.returncode == 0, result.stderr
    row = json.loads(sandbox.baseline.read_text())["cases"]["peak-workers"]
    assert row["workers"] == 1 and row["peak_rss_gb"] < 0.2, row


def test_probe_stops_at_its_limit_and_records_a_probe_row(sandbox):
    result = sandbox.run("probe", "long-probe", sleeper(30), "--probe-s", "1")
    assert result.returncode == 0, result.stderr
    row = json.loads(sandbox.baseline.read_text())["cases"]["long-probe"]
    assert row["kind"] == "probe" and row["completed"] is False
    assert sandbox.record("long-probe")["outcome"] == "probe-stopped"
    assert sandbox.record("long-probe")["wall_s"] < 15


def test_a_probe_never_overwrites_a_measured_row(sandbox):
    """guard.py `record_baseline`: a probe never overwrites a completed
    measurement (the rule is in guard.py; the earlier tool had no direct test)."""
    sandbox.set_row("kept", 0.5)
    before = json.loads(sandbox.baseline.read_text())["cases"]["kept"]
    result = sandbox.run("probe", "kept", sleeper(30), "--probe-s", "1")
    assert result.returncode == 0, result.stderr
    assert sandbox.record("kept")["outcome"] == "probe-stopped"
    after = json.loads(sandbox.baseline.read_text())["cases"]["kept"]
    assert after == before


# --- enforcement ---------------------------------------------------------------

def test_kills_process_group_over_budget_within_5s(sandbox):
    sandbox.set_row("hog-kill", 0.1)
    started = time.time()
    result = sandbox.run("run", "hog-kill", hog(3.0, rate=1.0, hold=30),
                         "--budget-gb", "0.5", timeout=40)
    elapsed = time.time() - started
    assert result.returncode == 5, result.stderr
    record = sandbox.record("hog-kill")
    assert record["outcome"] == "killed-budget"
    assert record["wall_s"] < 5.0, record
    assert elapsed < 12.0
    assert record["peak_rss_gb"] < 1.5
    assert any(a["action"] == "kill" for a in record["actions"])
    assert "guard:kill" in sandbox.runlog_text()
    assert "killed-budget" in sandbox.runlog_text()
    # nothing survives in the job's process group
    assert process_state(record["pid"]) == ""
    # a killed job never becomes a baseline measurement
    row = json.loads(sandbox.baseline.read_text())["cases"]["hog-kill"]
    assert row["peak_rss_gb"] == 0.1


def test_wall_time_ceiling_kills_and_logs_wall_time_class(sandbox):
    sandbox.set_row("slow", 0.01)
    result = sandbox.run("run", "slow", sleeper(30), "--timeout-min", "0.02",
                         timeout=40)
    assert result.returncode == 5
    record = sandbox.record("slow")
    assert record["outcome"] == "killed-walltime"
    assert "wall time" in record["actions"][-1]["reason"]


def test_pauses_and_resumes_on_injected_floor_breach(sandbox):
    sandbox.set_row("pausable", 0.01)
    proc = subprocess.Popen(
        [*sandbox.args("run", "pausable", "--resume-hold-s", "1"),
         "--", *sleeper(25)],
        env=sandbox.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True)
    record = reader(sandbox.outputs / "pausable" / "guard.json")
    wait_for(lambda: record().get("pid"), 10, "job start")
    pid = record()["pid"]
    time.sleep(1.0)
    sandbox.fake_free.write_text(str(1 * GB))          # breach the floor
    wait_for(lambda: any(a["action"] == "pause"
                         for a in record().get("actions", [])),
             10, "pause action")
    time.sleep(0.5)
    assert process_state(pid).startswith("T"), process_state(pid)
    sandbox.fake_free.write_text(str(40 * GB))         # recover
    wait_for(lambda: any(a["action"] == "resume"
                         for a in record().get("actions", [])),
             15, "resume action")
    time.sleep(0.5)
    assert not process_state(pid).startswith("T")
    out, err = proc.communicate(timeout=40)
    assert proc.returncode == 0, err
    final = record()
    assert final["outcome"] == "completed"
    assert final["paused_s"] > 0.5
    assert [a["action"] for a in final["actions"]] == ["pause", "resume"]
    text = sandbox.runlog_text()
    assert "guard:pause" in text and "guard:resume" in text


def test_kills_after_max_pause(sandbox):
    sandbox.set_row("stuck", 0.01)
    proc = subprocess.Popen(
        [*sandbox.args("run", "stuck", "--max-pause-s", "1.5"),
         "--", *sleeper(25)],
        env=sandbox.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True)
    record = reader(sandbox.outputs / "stuck" / "guard.json")
    # inject the breach only once the job is running (pre-flight reads the
    # same fake file)
    wait_for(lambda: bool(record().get("pid")), 10, "job start")
    time.sleep(0.3)
    sandbox.fake_free.write_text(str(1 * GB))
    out, err = proc.communicate(timeout=40)
    assert proc.returncode == 5, err
    final = sandbox.record("stuck")
    assert final["outcome"] == "killed-paused", final


# --- lock -----------------------------------------------------------------------

def test_single_job_lock_refuses_then_frees(sandbox):
    sandbox.set_row("lock-a", 0.01)
    sandbox.set_row("lock-b", 0.01)
    first = subprocess.Popen(
        [*sandbox.args("run", "lock-a"), "--", *sleeper(4)],
        env=sandbox.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True)
    wait_for(lambda: (sandbox.home / "current.json").is_file(), 10,
             "lock holder")
    holder = json.loads((sandbox.home / "current.json").read_text())
    assert holder["job_id"].startswith("lock-a/")
    second = sandbox.run("run", "lock-b", sleeper(0.1))
    assert second.returncode == 4, second.stderr
    assert "another job holds the lock" in second.stderr
    # G2b (D3): no runlog line; the attempt's record says why
    assert "guard:lock-busy" not in sandbox.runlog_text()
    assert sandbox.record("lock-b")["outcome"] == "lock-busy"
    assert "another job holds the lock" in \
        sandbox.record("lock-b")["refusal"]
    first.communicate(timeout=30)
    assert first.returncode == 0
    assert not (sandbox.home / "current.json").exists()
    # the lock file itself is never unlinked
    assert (sandbox.home / "lock").is_file()
    third = sandbox.run("run", "lock-b", sleeper(0.1))
    assert third.returncode == 0, third.stderr


def test_light_job_runs_beside_the_lock_holder_within_its_cap(sandbox):
    sandbox.set_row("heavy", 0.01)
    sandbox.set_row("light", 0.2)
    sandbox.set_row("not-light", 3.0)
    heavy = subprocess.Popen(
        [*sandbox.args("run", "heavy"), "--", *sleeper(6)],
        env=sandbox.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    wait_for(lambda: (sandbox.home / "current.json").is_file(), 10, "lock holder")
    light = sandbox.run("run", "light", sleeper(0.2), "--light")
    assert light.returncode == 0, light.stderr
    record = sandbox.record("light")
    assert record["light"] is True and record["thresholds"]["budget_gb"] == 1.5
    assert "| light" in sandbox.runlog_text()
    heavy_still = json.loads((sandbox.home / "current.json").read_text())
    assert heavy_still["job_id"].startswith("heavy/")
    too_big = sandbox.run("run", "not-light", sleeper(0.1), "--light")
    assert too_big.returncode == 3 and "run it as a normal job" in too_big.stderr
    threaded = sandbox.run("run", "light", sleeper(0.1), "--light", "--threads", "4")
    assert threaded.returncode == 3
    heavy.communicate(timeout=30)
    assert heavy.returncode == 0
    # a light hog is killed at the light cap, not the host budget
    sandbox.set_row("light-hog", 0.1)
    hogged = sandbox.run("run", "light-hog", hog(3.0, rate=1.0, hold=30), "--light", timeout=40)
    assert hogged.returncode == 5 and sandbox.record("light-hog")["outcome"] == "killed-budget"


def test_wait_option_queues_behind_the_lock(sandbox):
    sandbox.set_row("wait-a", 0.01)
    sandbox.set_row("wait-b", 0.01)
    first = subprocess.Popen(
        [*sandbox.args("run", "wait-a"), "--", *sleeper(2)],
        env=sandbox.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True)
    wait_for(lambda: (sandbox.home / "current.json").is_file(), 10, "lock")
    second = sandbox.run("run", "wait-b", sleeper(0.1), "--wait", "20")
    assert second.returncode == 0, second.stderr
    first.communicate(timeout=30)


# --- environment ----------------------------------------------------------------

def test_threads_pinned_and_job_env_exported(sandbox):
    sandbox.set_row("env", 0.01)
    probe = [PY, "-c", "import os; print(os.environ['OMP_NUM_THREADS'], "
             "os.environ['OPENBLAS_NUM_THREADS'], "
             "os.environ['VECLIB_MAXIMUM_THREADS'], "
             "os.environ['ACS_JOB_MARKER'], os.environ['ACS_WORKERS'])"]
    result = sandbox.run("run", "env", probe)
    assert result.returncode == 0, result.stderr
    fields = result.stdout.split()
    assert fields[:3] == ["1", "1", "1"]
    assert fields[3].startswith("env/")
    assert fields[4] == "1"
    result = sandbox.run("run", "env", probe, "--threads", "8")
    assert result.stdout.split()[:3] == ["8", "8", "8"]


def test_dense_threads_reaches_the_child_and_the_records(sandbox):
    """--dense-threads is a SEPARATE knob from --threads (G2a: exported as
    ACS_DENSE_THREADS; the legacy name only with --legacy-workload-env)."""
    sandbox.set_row("dense", 0.01)
    probe = [PY, "-c", "import os; print(os.environ.get("
             "'ACS_DENSE_THREADS', 'unset'), "
             "os.environ['OPENBLAS_NUM_THREADS'])"]

    plain = sandbox.run("run", "dense", probe)
    assert plain.returncode == 0, plain.stderr
    assert plain.stdout.split() == ["unset", "1"]
    assert sandbox.record("dense")["dense_threads"] is None

    raised = sandbox.run("run", "dense", probe, "--dense-threads", "8")
    assert raised.returncode == 0, raised.stderr
    # the dense count reached the child; the global pin is still 1
    assert raised.stdout.split() == ["8", "1"]
    assert sandbox.record("dense")["dense_threads"] == 8
    assert "dense_threads=8" in sandbox.runlog_text()

    # never above the CPU quota, never below one
    too_many = sandbox.run("run", "dense", probe, "--dense-threads", "16")
    assert too_many.returncode != 0
    assert "--dense-threads must be <= 10" in too_many.stderr


def test_nested_guard_on_a_marker_alone_is_refused(sandbox):
    """G2a: the pass-through needs the enclosing claimed ACS job
    (tests/test_g2a_names.py); a marker alone is refused, without the lock
    or the baseline."""
    env = dict(sandbox.env)
    env["SMC_GUARD_JOB"] = "outer/123"
    result = subprocess.run(
        [*sandbox.args("run", "never-measured"), "--", PY, "-c",
         "import os; print('inner', os.environ['SMC_GUARD_JOB'])"],
        env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 7, result.stderr
    assert "inner" not in result.stdout
    assert "no enclosing ACS job" in result.stderr
    assert not sandbox.runlog.exists()
    assert not (sandbox.home / "lock").exists()


def test_status_and_baseline_commands(sandbox):
    sandbox.set_row("shown", 0.5)
    status = subprocess.run([PY, str(ENGINE), "status"], env=sandbox.env,
                            capture_output=True, text=True)
    assert status.returncode == 0
    assert json.loads(status.stdout)["lock_holder"] is None
    table = subprocess.run([PY, str(ENGINE), "baseline", "--baseline",
                            str(sandbox.baseline)], capture_output=True,
                           text=True)
    assert table.returncode == 0 and "shown" in table.stdout


def test_host_profile_sets_caps_and_runlog_tag(sandbox):
    """B0.3: one profile per machine, now supplied as a file; the Mac
    numbers are untouched."""
    sandbox.set_row("prof", 0.01)
    lenovo = sandbox.run("run", "prof", sleeper(0.1), profile=LENOVO,
                         outputs=str(sandbox.outputs / "lenovo"))
    assert lenovo.returncode == 0, lenovo.stderr
    record = json.loads((sandbox.outputs / "lenovo" / "guard.json").read_text())
    assert record["profile"] == "lenovo"
    assert record["thresholds"]["budget_gb"] == 5.0
    assert record["thresholds"]["floor_gb"] == 1.5
    assert "| host=lenovo\n" in sandbox.runlog_text()
    mac = sandbox.run("run", "prof", sleeper(0.1), profile=MAC,
                      outputs=str(sandbox.outputs / "mac"))
    assert mac.returncode == 0, mac.stderr
    record = json.loads((sandbox.outputs / "mac" / "guard.json").read_text())
    assert record["profile"] == "mac"
    assert record["thresholds"]["budget_gb"] == 12.0
    assert record["thresholds"]["floor_gb"] == 8.0
    mac_lines = [l for l in sandbox.runlog_text().splitlines()
                 if "| prof/" in l and "host=" not in l]
    assert len(mac_lines) == 1
    # explicit flags still override the profile (ACS's dedicated budget)
    override = sandbox.run("run", "prof", sleeper(0.1), "--budget-gb", "3",
                           profile=LENOVO, outputs=str(sandbox.outputs / "ov"))
    assert override.returncode == 0, override.stderr
    record = json.loads((sandbox.outputs / "ov" / "guard.json").read_text())
    assert record["thresholds"]["budget_gb"] == 3.0
    assert record["thresholds"]["floor_gb"] == 1.5
    # a refusal writes no runlog line (G2b, D3), tagged or not
    refused = sandbox.run("run", "never-measured", sleeper(0.1),
                          profile=LENOVO)
    assert refused.returncode == 3
    assert not any("guard:refuse" in l
                   for l in sandbox.runlog_text().splitlines())
    # status reports the profile it is given; the CPU quota bounds
    # --dense-threads
    status = subprocess.run([PY, str(ENGINE), "status", "--profile-file",
                             str(LENOVO)], env=sandbox.env,
                            capture_output=True, text=True)
    assert status.returncode == 0, status.stderr
    shown = json.loads(status.stdout)
    assert shown["profile"] == "lenovo" and shown["container_cpus"] == 8
    too_many = subprocess.run(
        [*sandbox.args("run", "prof", "--dense-threads", "9", profile=LENOVO),
         "--", *sleeper(0.1)],
        env=sandbox.env, capture_output=True, text=True)
    assert too_many.returncode == 2 and "<= 8" in too_many.stderr
    fine_on_mac = subprocess.run(
        [*sandbox.args("run", "prof", "--dense-threads", "9", profile=MAC),
         "--", *sleeper(0.1)], env=sandbox.env, capture_output=True, text=True)
    assert fine_on_mac.returncode == 0, fine_on_mac.stderr
    # SMC_GUARD_PROFILE no longer selects anything: the file decides
    env = dict(sandbox.env, SMC_GUARD_PROFILE="lenovo")
    status = subprocess.run([PY, str(ENGINE), "status", "--profile-file",
                             str(MAC)], env=env, capture_output=True,
                            text=True)
    assert json.loads(status.stdout)["profile"] == "mac"
    bad = sandbox.tmp / "bad-profile.json"
    bad.write_text(json.dumps({"name": "nope"}))
    unknown = subprocess.run([PY, str(ENGINE), "status", "--profile-file",
                              str(bad)], env=sandbox.env,
                             capture_output=True, text=True)
    assert unknown.returncode == 7 and "bad profile" in unknown.stderr


# --- container venue (a disposable, labelled container) -----------------------------

@pytest.fixture
def guard_container():
    """A throwaway container with the memory hog at /work; the only
    container these tests touch (``docker update``/``pause`` included)."""
    name = f"acs-test-{secrets.token_hex(4)}"
    subprocess.run(["docker", "run", "-d", "--rm", "--name", name,
                    "--memory", "1g", "--cpus", "1", "--label",
                    "agent-compute-test=1", IMAGE, "sleep", "3600"],
                   check=True, capture_output=True, timeout=120)
    try:
        subprocess.run(["docker", "exec", name, "mkdir", "-p", "/work"],
                       check=True, capture_output=True, timeout=60)
        subprocess.run(["docker", "cp", str(HOG), f"{name}:/work/memory_hog.py"],
                       check=True, capture_output=True, timeout=60)
        yield name
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True,
                       timeout=120)


def container_profile(sandbox, floor_gb: float | None = None) -> Path:
    """The sandbox profile (guard.py's mac numbers), with the container CPU
    quota of the one-CPU disposable container (docker refuses more CPUs
    than the VM has); optionally another host floor."""
    data = json.loads(SANDBOX.read_text())
    data["container_cpus"] = 1
    if floor_gb is not None:
        data["floor_gb"] = floor_gb
    path = sandbox.tmp / "container-profile.json"
    path.write_text(json.dumps(data))
    return path


@needs_docker
def test_container_hog_is_killed_by_its_memory_cap(sandbox, guard_container):
    sandbox.env.pop(FREE_MEM_ENV)
    sandbox.set_row("container-hog", 0.05)
    result = sandbox.run(
        "run", "container-hog",
        ["python3", "memory_hog.py", "--gb", "4", "--rate-gb-s",
         "1", "--hold-s", "30"],
        "--venue", "container", "--container", guard_container,
        "--container-workdir", "/work",
        "--container-limits",       # S3: docker update is opt-in
        timeout=120,
        # real host memory here, so not the Mac's 8 GB floor: on the
        # 10 GB Lenovo VM it paused the job at "host available 8.0 GB <
        # floor 8.0 GB" until the timeout (S4, S5a gate runs).  The floor
        # is not what this test checks: the 1 GB container cap is.
        profile=container_profile(sandbox, floor_gb=1.0))
    record = sandbox.record("container-hog")
    assert record["docker"]["memory_gb"] == 1.0
    assert record["docker"]["cpus"] == 1
    assert record["docker"]["limits_applied"] is True
    # either the cgroup OOM-killer (SIGKILL: 137 from docker, 9 through
    # `setsid -w`) or the guard (exit 5) stops it
    assert result.returncode in (5, 9, 137), (result.returncode, result.stderr)
    assert record["outcome"] in ("killed-budget", "failed")
    assert record["wall_s"] < 30
    assert record["peak_rss_gb"] < 1.5


@needs_docker
def test_container_job_pauses_and_resumes(sandbox, guard_container):
    sandbox.set_row("container-pause", 0.05)
    proc = subprocess.Popen(
        [*sandbox.args("run", "container-pause", "--resume-hold-s", "1",
                       "--venue", "container", "--container", guard_container,
                       "--container-workdir", "/work",
                       profile=container_profile(sandbox)),
         "--", "python3", "-c", "import time; time.sleep(30)"],
        env=sandbox.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True)
    record = reader(sandbox.outputs / "container-pause" / "guard.json")
    wait_for(lambda: record().get("container_pid"), 40, "container pid")
    time.sleep(1.0)
    sandbox.fake_free.write_text(str(1 * GB))
    wait_for(lambda: any(a["action"] == "pause"
                         for a in record().get("actions", [])),
             20, "container pause")
    paused = subprocess.run(["docker", "inspect", "--format",
                             "{{.State.Paused}}", guard_container],
                            capture_output=True, text=True).stdout.strip()
    assert paused == "true"
    sandbox.fake_free.write_text(str(40 * GB))
    wait_for(lambda: any(a["action"] == "resume"
                         for a in record().get("actions", [])),
             30, "container resume")
    proc.communicate(timeout=90)
    assert proc.returncode == 0
    assert record()["outcome"] == "completed"


@needs_docker
def test_container_job_runs_in_its_workdir_with_the_job_env(sandbox,
                                                            guard_container):
    """The container venue's launch (``docker exec … setsid -w bash -c``),
    the exported variables and ``--container-workdir``."""
    sandbox.set_row("container-env", 0.05)
    result = sandbox.run(
        "run", "container-env",
        ["python3", "-c", "import os; print(os.getcwd(), "
         "os.environ['ACS_JOB_MARKER'], os.environ['OMP_NUM_THREADS'])"],
        "--venue", "container", "--container", guard_container,
        "--container-workdir", "/work", timeout=120,
        profile=container_profile(sandbox))
    assert result.returncode == 0, result.stderr
    cwd, job, threads = result.stdout.split()
    assert cwd == "/work" and job.startswith("container-env/")
    assert threads == "1"
    record = sandbox.record("container-env")
    assert record["outcome"] == "completed"
    assert record["venue"] == "container"
    # container_pid may be null here: a job this short can end before the
    # guard's first pid-file poll (guard.py's launch() loop stops when the
    # `docker exec` has exited). The pid is pinned by the pause test, whose
    # job runs long enough (Lenovo gate a81fc70, 2026-09-29).
    # the pid file is removed afterwards
    left = subprocess.run(["docker", "exec", guard_container, "sh", "-c",
                           "ls /tmp/acs-guard-*.pid 2>/dev/null || true"],
                          capture_output=True, text=True).stdout.strip()
    assert left == ""


def test_container_sampler_timeout_keeps_the_last_sample_and_does_not_raise(monkeypatch):
    """B3.3 (2026-09-09): a transient ``docker exec`` timeout inside the
    sampler must keep the last sample and log."""
    import types

    guard_mod = load_engine()

    def boom(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=["docker", "exec"], timeout=15)

    monkeypatch.setattr(guard_mod, "docker", boom)
    calls = []
    ns = types.SimpleNamespace(venue="container", paused_since=None,
                               container="smc-test", samples=[(0.0, 2.5, 20.0)],
                               action=lambda kind, reason: calls.append((kind, reason)))
    assert guard_mod.Job.sample_rss(ns) == 2.5
    assert guard_mod.Job.sample_rss(ns) == 2.5
    assert ns.sampler_failures == 2
    assert [c[0] for c in calls] == ["sampler-unavailable"]
    # a successful read resets the counter
    monkeypatch.setattr(guard_mod, "docker", lambda *a, **k: types.SimpleNamespace(
        returncode=0, stdout="4096\n"))
    assert guard_mod.Job.sample_rss(ns) == 4096.0
    assert ns.sampler_failures == 0


# --- LA2: admission takes max(measurement, declared reserve) ---------------------

LADDER = ("--reserve-detector", "smc-ladder")


def ladder_config_file(tmp_path: Path, rungs, **extra) -> Path:
    path = tmp_path / "ladder-config.json"
    config = {"name": "fake-ladder", "t_end": 1.0,
              "scenario": {"kind": "pearling_axial", "n": 8},
              "ladder": rungs}
    config.update(extra)
    path.write_text(json.dumps(config))
    return path


def ladder_command(config: Path) -> list[str]:
    return [PY, "-c", "import sys; print(' '.join(sys.argv[1:]))",
            "--config", str(config)]


def test_admission_takes_the_max_of_measurement_and_declared_reserve(sandbox):
    """LA2 repair 1: admission is ``max(measured peak, declared reserve)``."""
    sandbox.set_row("mixed", 0.5)
    admitted = sandbox.run("run", "mixed", sleeper(0.1), "--reserve-gb", "2.5")
    assert admitted.returncode == 0, admitted.stderr
    assert sandbox.record("mixed")["estimate_gb"] == 2.5
    assert sandbox.record("mixed")["declared_reserve_gb"] == 2.5

    # the declaration, not the measurement, decides a tight admission
    sandbox.fake_free.write_text(str(9 * GB))
    refused = sandbox.run("run", "mixed", sleeper(0.1), "--reserve-gb", "2.5")
    assert refused.returncode == 3, refused.stderr
    assert "estimate 2.50 GB" in refused.stderr
    on_measurement = sandbox.run("run", "mixed", sleeper(0.1))
    assert on_measurement.returncode == 0, on_measurement.stderr

    # the measurement wins when it is the larger of the two
    sandbox.fake_free.write_text(str(40 * GB))
    sandbox.set_row("mixed", 0.9)
    larger = sandbox.run("run", "mixed", sleeper(0.1), "--reserve-gb", "0.2")
    assert larger.returncode == 0, larger.stderr
    assert sandbox.record("mixed")["estimate_gb"] == 0.9


def test_declared_reserve_admits_an_unmeasured_case_and_binds_it(sandbox):
    """A declaration is not a free pass: with no measurement the job is held
    to 1.5 x what it declared, exactly like a probe."""
    result = sandbox.run("run", "declared-only", hog(3.0, rate=1.0, hold=30),
                         "--reserve-gb", "0.5", timeout=40)
    assert result.returncode == 5, result.stderr
    record = sandbox.record("declared-only")
    assert record["outcome"] == "killed-budget"
    assert record["thresholds"]["job_budget_gb"] == 1.0, record["thresholds"]
    assert record["estimate_gb"] == 0.5
    # with no measurement AND no declaration the refusal is the old one
    refused = sandbox.run("run", "nothing-declared", sleeper(0.1))
    assert refused.returncode == 3
    assert "no measured peak" in refused.stderr
    assert "--reserve-gb" in refused.stderr


def test_ladder_job_admission_uses_its_heaviest_declared_rung(sandbox, tmp_path):
    """LA2 repair 1, with the opt-in detector: a ladder declares the reserve
    of its heaviest rung."""
    config = ladder_config_file(tmp_path, [{"name": "fixed"},
                                           {"name": "line-search"}])
    command = ladder_command(config)

    refused = sandbox.run("run", "ladder-new", command, *LADDER)
    assert refused.returncode == 3, refused.stderr
    assert "line-search" in refused.stderr and "reserve_gb" in refused.stderr

    declared = sandbox.run("run", "ladder-new", command, "--reserve-gb", "2",
                           *LADDER)
    assert declared.returncode == 0, declared.stderr
    record = sandbox.record("ladder-new")
    assert record["estimate_gb"] == 2.0
    assert record["ladder"]["heaviest_rung"] == "line-search"
    assert record["ladder"]["config"] == str(config)

    # the heaviest rung is the one that asks for the most, not the last one
    config.write_text(json.dumps({
        "name": "fake-ladder", "t_end": 1.0,
        "ladder": [{"name": "fixed", "reserve_gb": 1.0},
                   {"name": "line-search", "reserve_gb": 3.0},
                   {"name": "stabilization"}]}))
    from_config = sandbox.run("run", "ladder-declared", command, *LADDER)
    assert from_config.returncode == 0, from_config.stderr
    record = sandbox.record("ladder-declared")
    assert record["estimate_gb"] == 3.0
    assert record["ladder"]["heaviest_rung"] == "line-search"
    assert record["ladder"]["declared_reserve_gb"] == 3.0

    # a measurement alone: admitted, but the gap is on the record
    config.write_text(json.dumps({
        "name": "fake-ladder", "t_end": 1.0,
        "ladder": [{"name": "fixed"}, {"name": "line-search"}]}))
    sandbox.set_row("ladder-old", 0.5)
    flagged = sandbox.run("run", "ladder-old", command, *LADDER)
    assert flagged.returncode == 0, flagged.stderr
    assert sandbox.record("ladder-old")["estimate_gb"] == 0.5
    assert "guard:ladder-undeclared" in sandbox.runlog_text()
    assert any(a["action"] == "ladder-undeclared"
               for a in sandbox.record("ladder-old")["actions"])

    # a command with no ladder config is not a ladder job
    sandbox.set_row("plain", 0.5)
    plain = sandbox.run("run", "plain", sleeper(0.1), *LADDER)
    assert plain.returncode == 0, plain.stderr
    assert sandbox.record("plain")["ladder"] is None


def test_lenovo_profile_refuses_an_over_memory_dense_case_regardless_of_elapsed_time(sandbox, tmp_path):
    """LD1: the lenovo profile's 5 GB host budget refuses a case whose
    reserve exceeds it AT PREFLIGHT, before the command ever starts."""
    marker = sandbox.outputs / "never-started.marker"
    never_run = [PY, "-c", f"open({str(marker)!r}, 'w').close()"]

    # 1) a MEASURED case over budget
    sandbox.set_row("quench-n25", 7.314)
    measured = sandbox.run("run", "quench-n25", never_run, profile=LENOVO,
                           outputs=str(sandbox.outputs / "measured"))
    assert measured.returncode == 3, measured.stderr
    assert "exceeds the host budget 5.0 GB" in measured.stderr
    assert not marker.exists(), "a refused job must never start the command"
    record = json.loads((sandbox.outputs / "measured" / "guard.json").read_text())
    assert record["outcome"] == "refused" and record["profile"] == "lenovo"
    assert record.get("wall_s") is None, record.get("wall_s")

    # 2) a DECLARED-ONLY reserve over budget
    declared = sandbox.run("run", "quench-n33-new", never_run, "--reserve-gb",
                           "8.368", profile=LENOVO,
                           outputs=str(sandbox.outputs / "declared"))
    assert declared.returncode == 3, declared.stderr
    assert "exceeds the host budget 5.0 GB" in declared.stderr
    assert not marker.exists()

    # 3) a LADDER job whose heaviest rung declares an over-budget reserve
    config = ladder_config_file(tmp_path, [
        {"name": "fixed", "reserve_gb": 1.0},
        {"name": "line-search", "reserve_gb": 8.368}])
    ladder_over = sandbox.run("run", "quench-ladder", ladder_command(config),
                              *LADDER, profile=LENOVO,
                              outputs=str(sandbox.outputs / "ladder"))
    assert ladder_over.returncode == 3, ladder_over.stderr
    assert "exceeds the host budget 5.0 GB" in ladder_over.stderr

    # 4) elapsed time does not matter
    slow_but_refused = sandbox.run(
        "run", "quench-n25", [PY, "-c", "import time; time.sleep(30)"],
        profile=LENOVO, outputs=str(sandbox.outputs / "slow"), timeout=10.0)
    assert slow_but_refused.returncode == 3, slow_but_refused.stderr

    # 5) control: a 4.5 GB case fits at the same profile
    sandbox.set_row("pull-n25", 4.5)
    fits = sandbox.run("run", "pull-n25", sleeper(0.1), profile=LENOVO,
                       outputs=str(sandbox.outputs / "fits"))
    assert fits.returncode == 0, fits.stderr

    # 6) the same 7.314 GB case is NOT refused under the mac profile
    mac_fits = sandbox.run("run", "quench-n25", sleeper(0.1), profile=MAC,
                           outputs=str(sandbox.outputs / "mac"))
    assert mac_fits.returncode == 0, mac_fits.stderr


# --- LA2: sampler failure policy ------------------------------------------------

def test_sampler_staleness_logs_then_pauses_then_resumes(sandbox):
    """LA2 repair 2: a sampler hiccup must not crash the guard and a stale
    sample must not be trusted indefinitely."""
    sandbox.set_row("stale", 0.01)
    fail = sandbox.fake_free.parent / "sampler_fail"
    env = {**sandbox.env, SAMPLER_FAIL_ENV: str(fail)}
    proc = subprocess.Popen(
        [*sandbox.args("run", "stale", "--sampler-stale-s", "0.5",
                       "--sampler-pause-s", "1.5", "--max-pause-s", "60"),
         "--", *sleeper(30)],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    record = reader(sandbox.outputs / "stale" / "guard.json")

    def actions():
        return [a["action"] for a in record().get("actions", [])]

    wait_for(lambda: record().get("pid"), 10, "job start")
    pid = record()["pid"]
    fail.write_text("1")
    wait_for(lambda: "sampler-stale" in actions(), 15, "sampler-stale")
    wait_for(lambda: "pause" in actions(), 15, "sampler pause")
    time.sleep(0.5)
    assert process_state(pid).startswith("T"), process_state(pid)
    paused = [a for a in record()["actions"] if a["action"] == "pause"][0]
    assert "no sample" in paused["reason"]
    fail.write_text("0")
    wait_for(lambda: "resume" in actions(), 15, "sampler resume")
    assert not process_state(pid).startswith("T")
    proc.communicate(timeout=60)
    assert proc.returncode == 0
    final = record()
    assert final["outcome"] == "completed"
    text = sandbox.runlog_text()
    assert "guard:sampler-stale" in text
    assert "guard:pause" in text and "guard:resume" in text


def test_sampler_that_never_recovers_is_killed_at_the_paused_ceiling(sandbox):
    sandbox.set_row("blind", 0.01)
    fail = sandbox.fake_free.parent / "sampler_fail_forever"
    fail.write_text("1")
    env = {**sandbox.env, SAMPLER_FAIL_ENV: str(fail)}
    result = subprocess.run(
        [*sandbox.args("run", "blind", "--sampler-stale-s", "0.3",
                       "--sampler-pause-s", "0.6", "--max-pause-s", "2"),
         "--", *sleeper(40)],
        env=env, capture_output=True, text=True, timeout=90)
    assert result.returncode == 5, result.stderr
    record = sandbox.record("blind")
    assert record["outcome"] == "killed-paused", record
    kinds = [a["action"] for a in record["actions"]]
    assert kinds[:3] == ["sampler-unavailable", "sampler-stale", "pause"]
    assert kinds[-1] == "kill"
    assert "killed-paused" in sandbox.runlog_text()


# --- LA2: ownership and lifecycle -----------------------------------------------

def test_stale_lock_from_a_dead_holder_is_reported_and_reclaimed(sandbox):
    dead = subprocess.Popen([PY, "-c", "pass"])
    dead.wait()
    sandbox.home.mkdir(parents=True, exist_ok=True)
    (sandbox.home / "current.json").write_text(json.dumps(
        {"job_id": "ghost/20260101T000000Z", "pid": dead.pid,
         "venue": "host", "command": ["sleep", "1"],
         "started_utc": "2026-01-01T00:00:00Z"}) + "\n")

    status = subprocess.run([PY, str(ENGINE), "status"], env=sandbox.env,
                            capture_output=True, text=True)
    assert status.returncode == 0, status.stderr
    holder = json.loads(status.stdout)["lock_holder"]
    assert holder["job_id"] == "ghost/20260101T000000Z"
    assert holder["alive"] is False and holder["stale"] is True

    sandbox.set_row("after-ghost", 0.01)
    result = sandbox.run("run", "after-ghost", sleeper(0.2))
    assert result.returncode == 0, result.stderr
    assert "guard:stale-lock" in sandbox.runlog_text()
    # and a live holder is not called stale
    live = subprocess.Popen(
        [*sandbox.args("run", "after-ghost"), "--", *sleeper(4)],
        env=sandbox.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True)
    wait_for(lambda: (sandbox.home / "current.json").is_file(), 10, "lock")
    status = subprocess.run([PY, str(ENGINE), "status"], env=sandbox.env,
                            capture_output=True, text=True)
    holder = json.loads(status.stdout)["lock_holder"]
    assert holder["alive"] is True and holder["stale"] is False
    live.communicate(timeout=30)


@pytest.mark.parametrize("sig,name", [("TERM", "SIGTERM"), ("INT", "SIGINT")])
def test_interrupting_the_guard_reaps_the_child_and_logs_it(sandbox, sig, name):
    signum = getattr(signal, f"SIG{sig}")
    sandbox.set_row("cancel", 0.01)
    proc = subprocess.Popen(
        [*sandbox.args("run", "cancel", outputs=str(sandbox.outputs / sig)),
         "--", *sleeper(60)],
        env=sandbox.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True)
    record = reader(sandbox.outputs / sig / "guard.json")
    wait_for(lambda: record().get("pid"), 10, "job start")
    child = record()["pid"]
    wait_for(lambda: process_state(child) != "", 10, "child running")
    proc.send_signal(signum)
    proc.communicate(timeout=40)
    assert proc.returncode == 5
    final = record()
    assert final["outcome"] == "killed-interrupted", final
    assert any(a["action"] == "kill" for a in final["actions"])
    wait_for(lambda: process_state(child) == "", 10, f"child reaped after {name}")
    text = sandbox.runlog_text()
    assert "guard:kill" in text and "killed-interrupted" in text
    assert not (sandbox.home / "current.json").exists()


def test_a_killed_job_with_units_does_not_divide_by_the_declared_count(sandbox):
    """LD3: ``s/step`` is computed only for a completed job."""
    sandbox.set_row("cancel-units", 0.01)
    proc = subprocess.Popen(
        [*sandbox.args("run", "cancel-units", "--units", "steps=16"),
         "--", *sleeper(60)],
        env=sandbox.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True)
    record = reader(sandbox.outputs / "cancel-units" / "guard.json")
    wait_for(lambda: record().get("pid"), 10, "job start")
    child = record()["pid"]
    wait_for(lambda: process_state(child) != "", 10, "child running")
    time.sleep(1.0)                    # a real partial run, not an instant kill
    proc.send_signal(signal.SIGTERM)
    proc.communicate(timeout=40)
    assert proc.returncode == 5
    final = record()
    assert final["outcome"] == "killed-interrupted", final
    text = sandbox.runlog_text()
    lines = [line for line in text.splitlines() if "cancel-units" in line
             or ("killed-interrupted" in line and "steps=16" in line)]
    assert lines, text
    line = lines[-1]
    assert "s/step=-" in line, line
    assert "s/step=2." not in line and "s/step=1." not in line, line
    assert "16 steps (declared)" in line or "16 steps" in line, line


@pytest.mark.parametrize("second_delay_s", [0.05, 0.1, 0.2, 0.4, 0.8])
def test_a_repeated_cancel_signal_still_exits_killed_not_the_os_default(
        sandbox, second_delay_s):
    """LD5: repeated cancel signals never change the outcome."""
    sandbox.set_row("repeat-cancel", 0.01)
    proc = subprocess.Popen(
        [*sandbox.args("run", "repeat-cancel",
                       outputs=str(sandbox.outputs / f"d{second_delay_s}")),
         "--", *sleeper(60)],
        env=sandbox.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True)
    record = reader(sandbox.outputs / f"d{second_delay_s}" / "guard.json")
    wait_for(lambda: record().get("pid"), 10, "job start")
    child = record()["pid"]
    wait_for(lambda: process_state(child) != "", 10, "child running")
    proc.send_signal(signal.SIGTERM)     # the supervisor's 1st cancel tick
    time.sleep(second_delay_s)
    try:
        proc.send_signal(signal.SIGTERM)  # the supervisor's 2nd tick
    except ProcessLookupError:
        pass
    proc.communicate(timeout=40)
    assert proc.returncode == 5, (
        f"a repeated cancel signal (delay {second_delay_s} s) must still "
        f"exit through the guard's own kill path (5), got {proc.returncode}")
    final = record()
    assert final["outcome"] == "killed-interrupted", final


def test_the_final_record_is_always_written_and_never_half_written(sandbox):
    """LA2: ``guard.json`` is complete the moment the guard exits and
    parseable at every moment while it runs."""
    sandbox.set_row("record", 0.01)
    record_path = sandbox.outputs / "record" / "guard.json"
    proc = subprocess.Popen(
        [*sandbox.args("run", "record"), "--", *sleeper(3)],
        env=sandbox.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True)
    wait_for(record_path.is_file, 10, "the record to appear")
    reads = 0
    while proc.poll() is None:
        json.loads(record_path.read_text())
        reads += 1
    proc.communicate(timeout=30)
    assert reads > 10, reads
    assert proc.returncode == 0
    final = json.loads(record_path.read_text())
    assert final["outcome"] == "completed", final
    assert final["exit_code"] == 0, final
    assert final["wall_s"] >= 3.0, final
    assert not list((sandbox.outputs / "record").glob(".guard.json.*.tmp"))


def test_baseline_reconcile_raises_rows_from_the_records(sandbox):
    """B4.1 ``baseline --reconcile``: dry run by default, ``--apply`` needs
    a ``--case`` (exit 7), and it needs ``--records`` now that there is no
    default records root."""
    sandbox.set_row("rec", 0.1)
    rec_dir = sandbox.tmp / "records" / "rec" / "20260101T000000Z"
    rec_dir.mkdir(parents=True)
    (rec_dir / "guard.json").write_text(json.dumps(
        {"job_id": "rec/20260101T000000Z", "peak_rss_gb": 0.7}))
    base = [PY, str(ENGINE), "baseline", "--baseline", str(sandbox.baseline)]
    records = ["--reconcile", "--records", str(sandbox.tmp / "records")]
    no_records = subprocess.run([*base, "--reconcile"], capture_output=True,
                                text=True)
    assert no_records.returncode == 2 and "--records" in no_records.stderr
    dry = subprocess.run([*base, *records], capture_output=True, text=True)
    assert dry.returncode == 0 and "0.100 -> 0.700" in dry.stdout
    assert "dry run" in dry.stdout
    no_case = subprocess.run([*base, *records, "--apply"],
                             capture_output=True, text=True)
    assert no_case.returncode == 7
    applied = subprocess.run([*base, *records, "--apply", "--case", "rec"],
                             capture_output=True, text=True)
    assert applied.returncode == 0, applied.stderr
    row = json.loads(sandbox.baseline.read_text())["cases"]["rec"]
    assert row["peak_rss_gb"] == 0.7 and row["last_peak_rss_gb"] == 0.1


# --- new in the port: parameterization -------------------------------------------

def test_profile_file_is_validated_and_fills_only_guard_py_defaults():
    guard = load_engine()
    # the fixture states its (test) disk floor; the other optional keys
    # are filled with guard.py's constants
    mac = guard.load_profile(MAC)
    assert mac == {"name": "mac", "budget_gb": 12.0, "floor_gb": 8.0,
                   "container_cpus": 10, "runlog_tag": None,
                   "disk_floor_gb": 1.0, "light_budget_gb": 1.0,
                   "vm_reserve_gb": 2.0, "container_memory_factor": 1.5}
    good = {k: v for k, v in json.loads(MAC.read_text()).items()
            if k != "disk_floor_gb"}
    assert guard.parse_profile(good)["disk_floor_gb"] == 20.0
    bad_cases = [
        ({**good, "schema": "x"}, "schema"),
        ({**good, "budget": 12}, "unknown keys: budget"),
        ({k: v for k, v in good.items() if k != "floor_gb"}, "missing keys"),
        ({k: v for k, v in good.items() if k != "runlog_tag"}, "missing keys"),
        ({**good, "budget_gb": 0}, "budget_gb"),
        ({**good, "budget_gb": True}, "budget_gb"),
        ({**good, "floor_gb": float("nan")}, "floor_gb"),
        ({**good, "container_cpus": 2.5}, "container_cpus"),
        ({**good, "container_cpus": 0}, "container_cpus"),
        ({**good, "runlog_tag": ""}, "runlog_tag"),
        ({**good, "name": ""}, "name"),
        ({**good, "disk_floor_gb": -1}, "disk_floor_gb"),
        ([good], "object"),
    ]
    for data, needle in bad_cases:
        with pytest.raises(guard.ProfileError, match=needle):
            guard.parse_profile(data)
    tuned = guard.parse_profile({**good, "disk_floor_gb": 5,
                                 "light_budget_gb": 0.5})
    assert tuned["disk_floor_gb"] == 5.0 and tuned["light_budget_gb"] == 0.5


def test_project_and_profile_arguments_are_required(sandbox):
    """guard.py defaulted these from its checkout (ROOT); here a run
    without them is a usage error that takes no lock and runs nothing."""
    sandbox.set_row("req", 0.01)
    marker = sandbox.tmp / "ran"
    cmd = [PY, "-c", f"open({str(marker)!r}, 'w').close()"]
    full = sandbox.args("run", "req")
    def without(flag, argv=full):
        i = argv.index(flag)
        return argv[:i] + argv[i + 2:]
    for flag, needle in (("--profile-file", "--profile-file"),
                         ("--runlog", "--runlog"),
                         ("--baseline", "--baseline"),
                         ("--cwd", "--cwd or --project-root"),
                         ("--outputs", "--outputs")):
        result = subprocess.run([*without(flag), "--", *cmd], env=sandbox.env,
                                capture_output=True, text=True, timeout=30)
        assert result.returncode == 2, (flag, result.stderr)
        assert needle in result.stderr, (flag, result.stderr)
    both = subprocess.run([*full, "--outputs-root", str(sandbox.tmp / "r"),
                           "--", *cmd], env=sandbox.env, capture_output=True,
                          text=True, timeout=30)
    assert both.returncode == 2 and "exactly one of" in both.stderr
    container = subprocess.run([*full, "--venue", "container", "--container",
                                "no-such-container", "--", *cmd],
                               env=sandbox.env, capture_output=True,
                               text=True, timeout=30)
    assert container.returncode == 2
    assert "--container-workdir" in container.stderr
    missing_file = subprocess.run(
        [*without("--profile-file"), "--profile-file",
         str(sandbox.tmp / "absent.json"), "--", *cmd], env=sandbox.env,
        capture_output=True, text=True, timeout=30)
    assert missing_file.returncode == 7 and "bad profile" in missing_file.stderr
    assert not marker.exists()
    assert not (sandbox.home / "current.json").exists()
    assert not sandbox.runlog.exists()


def test_outputs_root_and_project_root(sandbox):
    """--outputs-root gives guard.py's <root>/<case>/<stamp> layout;
    --project-root is the default --cwd and the base of the record's
    baseline path (guard.py's ROOT)."""
    project = sandbox.tmp / "project"
    (project / "docs" / "perf").mkdir(parents=True)
    baseline = project / "docs" / "perf" / "baseline.json"
    baseline.write_text(json.dumps({"schema": "smc-perf-baseline/1", "cases": {
        "pr": {"venue": "host", "kind": "measured", "peak_rss_gb": 0.01,
               "wall_s": 1.0, "threads": 1, "workers": 1}}}))
    argv = [PY, str(ENGINE), "run", "--case", "pr", "--profile-file", str(MAC),
            "--project-root", str(project), "--baseline", str(baseline),
            "--runlog", str(sandbox.runlog),
            "--outputs-root", str(project / "outputs" / "guard"),
            "--interval-s", "0.25", "--", PY, "-c",
            "import os; print(os.getcwd())"]
    result = subprocess.run(argv, env=sandbox.env, capture_output=True,
                            text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert Path(result.stdout.strip()).resolve() == project.resolve()
    records = list((project / "outputs" / "guard" / "pr").glob("*/guard.json"))
    assert len(records) == 1
    record = json.loads(records[0].read_text())
    assert record["job_id"] == f"pr/{records[0].parent.name}"
    assert record["baseline"] == "docs/perf/baseline.json"


def test_a_new_runlog_gets_the_generic_or_the_supplied_header(sandbox):
    sandbox.set_row("hdr", 0.01)
    result = sandbox.run("run", "hdr", sleeper(0.1))
    assert result.returncode == 0, result.stderr
    text = sandbox.runlog_text()
    assert text.startswith("# Run log\n") and "agent_compute.guard" in text
    assert "scripts/dev/guard.py" not in text
    header = sandbox.tmp / "header.md"
    header.write_text("# Project run log\n\n")
    other = sandbox.tmp / "other-runlog.md"
    argv = sandbox.args("run", "hdr")
    argv[argv.index("--runlog") + 1] = str(other)
    result = subprocess.run([*argv, "--runlog-header-file", str(header), "--",
                             *sleeper(0.1)], env=sandbox.env,
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    lines = other.read_text().splitlines()
    assert lines[:2] == ["# Project run log", ""]
    assert "| test | hdr/" in lines[2] and "| completed |" in lines[2]


def test_ladder_detection_is_off_unless_the_project_opts_in(sandbox, tmp_path):
    """A ladder config argument means nothing without
    ``--reserve-detector smc-ladder`` (Stokes semantics stay opt-in)."""
    config = ladder_config_file(tmp_path, [{"name": "fixed"},
                                           {"name": "line-search"}])
    command = ladder_command(config)
    # opted out: no ladder refusal text, the ordinary unmeasured refusal
    refused = sandbox.run("run", "ladder-off", command)
    assert refused.returncode == 3
    assert "ladder job" not in refused.stderr
    assert "no measured peak" in refused.stderr
    sandbox.set_row("ladder-off", 0.5)
    admitted = sandbox.run("run", "ladder-off", command)
    assert admitted.returncode == 0, admitted.stderr
    assert sandbox.record("ladder-off")["ladder"] is None
    assert "ladder-undeclared" not in sandbox.runlog_text()
    unknown = sandbox.run("run", "ladder-off", command,
                          "--reserve-detector", "other")
    assert unknown.returncode == 2


def test_ladder_search_base_is_the_project_root(sandbox, tmp_path):
    """guard.py searched a relative config path under --cwd, the process
    cwd, then its ROOT; ROOT is now --project-root."""
    project = tmp_path / "proj"
    project.mkdir()
    ladder_config_file(project, [{"name": "a", "reserve_gb": 0.5}])
    command = [PY, "-c", "pass", "--config", "ladder-config.json"]
    sandbox.set_row("rel", 0.1)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    argv = sandbox.args("run", "rel", *LADDER)
    argv[argv.index("--cwd") + 1] = str(elsewhere)
    found = subprocess.run([*argv, "--project-root", str(project), "--",
                            *command], env=sandbox.env, capture_output=True,
                           text=True, timeout=60, cwd=str(elsewhere))
    assert found.returncode == 0, found.stderr
    assert sandbox.record("rel")["ladder"]["config"] == str(
        project / "ladder-config.json")
    not_found = subprocess.run([*argv, "--", *command], env=sandbox.env,
                               capture_output=True, text=True, timeout=60,
                               cwd=str(elsewhere))
    assert not_found.returncode == 0, not_found.stderr
    assert sandbox.record("rel")["ladder"] is None


def test_runs_as_a_module_and_by_path(sandbox):
    env = dict(sandbox.env, PYTHONPATH=str(REPO / "src"))
    by_module = subprocess.run([PY, "-m", "agent_compute.guard", "status"],
                               env=env, capture_output=True, text=True,
                               timeout=30, cwd=str(sandbox.tmp))
    by_path = subprocess.run([PY, str(ENGINE), "status"], env=sandbox.env,
                             capture_output=True, text=True, timeout=30,
                             cwd=str(sandbox.tmp))
    for result in (by_module, by_path):
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["lock_holder"] is None


def test_engine_is_standard_library_only():
    tree = ast.parse(ENGINE.read_text())
    names = {alias.name.split(".")[0] for node in ast.walk(tree)
             if isinstance(node, ast.Import) for alias in node.names}
    names |= {node.module.split(".")[0] for node in ast.walk(tree)
              if isinstance(node, ast.ImportFrom) and node.module}
    assert names <= set(sys.stdlib_module_names) | {"__future__"}, names


def test_admission_runs_the_internal_guard_only_as_its_own_process():
    """S4 (was: inert in S2/S3): the acs-guard backend runs the engine as a
    separate process from the runner's own version dir.  No ACS module, the
    installer or the launcher imports it; the runner names it by path."""
    sources = [*(REPO / "src" / "agent_compute").glob("*.py"),
               REPO / "install.py", *(REPO / "bin").iterdir()]
    assert sources
    for path in sources:
        text = path.read_text()
        for pattern in (r"agent_compute\.guard\b", r"from \.guard\b",
                        r"from \. import [^\n]*\bguard\b",
                        r"\bimport guard\b", r"import engine\b"):
            assert not re.search(pattern, text), (path, pattern)
    runner = (REPO / "src" / "agent_compute" / "runner.py").read_text()
    assert 'Path(__file__).resolve().parent / "guard" / "engine.py"' in runner

