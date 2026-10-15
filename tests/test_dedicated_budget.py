"""Dedicated-host memory budget (operator declaration in the machine config).

`backend.dedicated_budget_gb` declares the host dedicated compute: the host
budget the guard admits and kills at is raised from the profile's (12 GB on
the Mac's) to the declared value, for every project, never above the host's
RAM (G2b, D4; a fixed 48 GB before). The guard's own host-available-memory
floor check still applies. An invalid value fails closed: the config does
not load, so submit, doctor and the runner all refuse.

The declared value lies above the test profile's budget and below the
host's real RAM (the runner loads the config with the real cap): the
generic test profile's 2 GB budget (``tests/guard_profiles/standard.json``,
G4), a 3 GB declaration and a 2.5 GB reserve between them, so the module
runs on any host with at least 3 GB of RAM (4 GB for one of the valid
values) and skips with a counted ``[host]`` reason below that.
"""

import json
import subprocess
import sys

import pytest

from conftest import TEST_PROFILE, WORKLOAD, Env, skip_reason

from agent_compute import config as cfg

HOST_RAM_GB = cfg.host_resources()["memory_gb"]
DECLARED = TEST_PROFILE["budget_gb"] + 1.0
ABOVE_PROFILE = TEST_PROFILE["budget_gb"] + 0.5     # a reserve
if not (TEST_PROFILE["budget_gb"] < ABOVE_PROFILE < DECLARED and
        max(DECLARED, 4.0) <= HOST_RAM_GB):
    pytest.skip(skip_reason("host", f"this host's RAM ({HOST_RAM_GB} GB) is "
                                    f"below the declared budgets the tests "
                                    f"use ({DECLARED:g} and 4 GB)"),
                allow_module_level=True)


def _config_with(env: Env, line: str) -> None:
    text = env.config.read_text()
    env.config.write_text(text.replace('[backend]\n', f'[backend]\n{line}\n', 1))


# -- config validation (fails closed) -------------------------------------------

@pytest.mark.parametrize("value", [f"{DECLARED:g}", "4", "0.5",
                                   f"{HOST_RAM_GB:g}"])
def test_valid_dedicated_budget_loads(value):
    env = Env(queue=False)
    try:
        _config_with(env, f"dedicated_budget_gb = {value}")
        assert cfg.load(env.config).dedicated_budget_gb == float(value)
    finally:
        env.cleanup()


def test_absent_dedicated_budget_is_none():
    env = Env(queue=False)
    try:
        assert cfg.load(env.config).dedicated_budget_gb is None
    finally:
        env.cleanup()


@pytest.mark.parametrize("value", [f"{HOST_RAM_GB + 0.01:.3f}",
                                   "1000000", "0", "-1", "nan", "inf",
                                   '"48"', "true"])
def test_invalid_dedicated_budget_fails_closed(value):
    env = Env(queue=False)
    try:
        _config_with(env, f"dedicated_budget_gb = {value}")
        with pytest.raises(cfg.ConfigError, match="dedicated_budget_gb"):
            cfg.load(env.config)
        # every command refuses on an invalid declaration
        result = env.acs("doctor")
        assert result.returncode != 0
        assert "dedicated_budget_gb" in (result.stdout + result.stderr)
        result = env.acs("submit", "--json", "--case", "t", "--timeout-min",
                         "1", "--reserve-gb", "0.5", "--cwd",
                         str(env.projects["alpha"]), "--", sys.executable,
                         str(WORKLOAD), "--out", str(env.root / "never"))
        assert result.returncode != 0
        assert not (env.root / "never").exists()
    finally:
        env.cleanup()


# -- effect on admission ---------------------------------------------------------

def _submit_reserve(env: Env, reserve_gb: float, out):
    result = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                     "--reserve-gb", f"{reserve_gb:g}",
                     "--cwd", str(env.projects["alpha"]), "--",
                     sys.executable, str(WORKLOAD), "--out", str(out))
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)["job_id"]


def test_dedicated_budget_admits_reserve_above_profile_default(env_any):
    """Fail-first: the reserve is refused under the profile budget."""
    env = env_any
    _config_with(env, f"dedicated_budget_gb = {DECLARED:g}")
    out = env.root / "ran.jsonl"
    state = env.wait_outcome(_submit_reserve(env, ABOVE_PROFILE, out))
    assert state["outcome"] == "succeeded", state
    assert out.exists()
    job_dir = env.state / "jobs" / state["job_id"]
    records = sorted(job_dir.glob("guard/attempt-*/guard.json"))
    assert records, list(job_dir.rglob("*"))
    guard = json.loads(records[-1].read_text())
    assert guard["thresholds"]["budget_gb"] == DECLARED
    manifest = json.loads((job_dir / "manifest.json").read_text())
    assert manifest["backend"]["dedicated_budget_gb"] == DECLARED


def test_dedicated_budget_is_still_a_ceiling(env_any):
    env = env_any
    _config_with(env, f"dedicated_budget_gb = {DECLARED:g}")
    state = env.wait_outcome(_submit_reserve(env, DECLARED + 1,
                                             env.root / "never"))
    assert state["outcome"] == "refused", state
    assert f"exceeds the host budget {DECLARED:.1f} GB" in state["reason"]
    assert not (env.root / "never").exists()


def test_without_declaration_profile_default_applies(env_any):
    env = env_any
    state = env.wait_outcome(_submit_reserve(env, ABOVE_PROFILE,
                                             env.root / "never"))
    assert state["outcome"] == "refused", state
    assert "exceeds the host budget" in state["reason"]
    assert not (env.root / "never").exists()


def test_doctor_reports_budget(env_any):
    env = env_any
    result = env.acs("doctor", "--json")
    rows = {c["check"]: c for c in json.loads(result.stdout)["checks"]}
    assert rows["host memory budget"]["detail"]["dedicated"] is False
    _config_with(env, f"dedicated_budget_gb = {DECLARED:g}")
    result = env.acs("doctor", "--json")
    rows = {c["check"]: c for c in json.loads(result.stdout)["checks"]}
    row = rows["host memory budget"]
    assert row["ok"] is True
    assert row["detail"]["dedicated"] is True
    assert row["detail"]["budget_gb"] == DECLARED


# -- dispatch-time semantics (queue mode: the runner starts, and loads the
#    live config, only when the queue dispatches the job) -----------------------

def _pueue_status(env: Env) -> dict:
    from conftest import PUEUE
    out = subprocess.run([PUEUE, "-c", str(env.state / "pueue" / "pueue.yml"),
                          "status", "--json"], capture_output=True, text=True,
                         check=True).stdout
    return json.loads(out)


def _hold_slot(env: Env):
    blocker = env.submit("alpha", workload_args=[
        "--out", str(env.root / "blocker.jsonl"), "--sleep", "4"])
    env.wait_phase(blocker["job_id"], "executing")
    return blocker


def test_declaration_made_while_queued_applies_at_dispatch(env_queue):
    env = env_queue
    blocker = _hold_slot(env)
    out = env.root / "pending.jsonl"
    pending = _submit_reserve(env, ABOVE_PROFILE, out)  # declared after
    _config_with(env, f"dedicated_budget_gb = {DECLARED:g}")
    assert env.wait_outcome(blocker["job_id"])["outcome"] == "succeeded"
    state = env.wait_outcome(pending)
    assert state["outcome"] == "succeeded", state
    assert out.exists()
    job_dir = env.state / "jobs" / pending
    guard = json.loads(sorted(job_dir.glob("guard/attempt-*/guard.json"))[-1]
                       .read_text())
    assert guard["thresholds"]["budget_gb"] == DECLARED  # at dispatch
    manifest = json.loads((job_dir / "manifest.json").read_text())
    assert manifest["backend"]["dedicated_budget_gb"] is None   # at submit


def test_invalid_declaration_at_dispatch_never_starts_the_job(env_queue):
    env = env_queue
    blocker = _hold_slot(env)
    out = env.root / "pending.jsonl"
    pending = _submit_reserve(env, 0.5, out)
    good = env.config.read_text()
    _config_with(env, "dedicated_budget_gb = 1000000")  # above the RAM
    assert env.wait_outcome(blocker["job_id"])["outcome"] == "succeeded"
    task = str(env.state_of(pending)["pueue_task"])
    env.wait_for(lambda: "Done" in json.dumps(
        _pueue_status(env)["tasks"].get(task, {}).get("status", {})),
        what="pending task dispatched and ended")
    assert not out.exists()
    assert "runner" not in env.state_of(pending)      # never claimed
    assert env.acs("status", pending).returncode != 0  # config refused
    env.config.write_text(good)                       # operator fixes it
    env.acs("status", pending)                        # settles unclaimed
    state = env.wait_outcome(pending)
    assert state["outcome"] == "launch-failed", state
    assert "never started" in state["reason"]
    assert not out.exists()


def test_floor_still_bounds_admission_under_declaration(env_any):
    env = env_any
    _config_with(env, f"dedicated_budget_gb = {DECLARED:g}")
    # available == reserve: short by the test profile's floor, yet within
    # the budget
    reserve = DECLARED - 1
    env.free.write_text(str(int(reserve * 1024 ** 3)))
    out = env.root / "never"
    result = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                     "--reserve-gb", f"{reserve:g}", "--lock-wait", "4",
                     "--cwd", str(env.projects["alpha"]), "--",
                     sys.executable, str(WORKLOAD), "--out", str(out))
    job_id = json.loads(result.stdout)["job_id"]
    env.wait_for(lambda: "insufficient host resources" in
                 (env.state_of(job_id).get("wait_reason") or ""),
                 what="resource wait reason")
    state = env.wait_outcome(job_id)
    assert state["outcome"] == "admission-timeout", state
    assert not out.exists()
