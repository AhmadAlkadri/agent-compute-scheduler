"""6. Isolation and execution context."""

import json
import re
import stat
import sys

from conftest import WORKLOAD, events

LITERALS = ["a b", "it's", '"double"', "$HOME", "`id`", "$(id)", "; rm -rf /",
            "| cat", "*", "?", "\\back", "tab\there", "new\nline", "", "--",
            "--timeout-min", "ünïcode"]


def test_literal_argument_preservation(env_any):
    env = env_any
    out = env.root / "argv.jsonl"
    job = env.submit(workload_args=["--out", str(out), *LITERALS])
    assert env.wait_outcome(job["job_id"])["outcome"] == "succeeded"
    argv = events(out)[0]["argv"]
    assert argv[1:] == ["--out", str(out), *LITERALS]


def test_projects_keep_separate_records(env_any):
    env = env_any
    a = env.submit("alpha", case="solve", workload_args=["--out",
                                                         str(env.root / "a")])
    b = env.submit("beta", case="solve", workload_args=["--out",
                                                        str(env.root / "b")])
    for job in (a, b):
        assert env.wait_outcome(job["job_id"])["outcome"] == "succeeded"
    alpha_log = (env.state / "projects" / "alpha" / "runlog.md").read_text()
    beta_log = (env.state / "projects" / "beta" / "runlog.md").read_text()
    assert "alpha.solve/" in alpha_log and "beta." not in alpha_log
    assert "beta.solve/" in beta_log and "alpha." not in beta_log
    for job, project in ((a, "alpha"), (b, "beta")):
        man = json.loads((env.state / "jobs" / job["job_id"] /
                          "manifest.json").read_text())
        assert man["project"] == project
        assert man["guard_case"] == f"{project}.solve"
        assert man["cwd"].endswith(f"proj {project}")


def test_unknown_host_never_runs_locally(env_any):
    env = env_any
    result = env.acs("--host", "nowhere", "submit", "--case", "t",
                     "--timeout-min", "1", "--cwd", str(env.projects["alpha"]),
                     "--", "true")
    assert result.returncode == 2
    assert "neither this host" in result.stderr
    assert not (env.state / "jobs").exists() or \
        not list((env.state / "jobs").iterdir())


def test_unconfigured_directory_is_refused(env_any):
    env = env_any
    result = env.acs("submit", "--case", "t", "--timeout-min", "1",
                     "--cwd", str(env.root), "--", "true")
    assert result.returncode == 2
    assert "matches 0 configured projects" in result.stderr


def test_source_change_while_queued_holds_the_job(env_any):
    env = env_any
    blocker = env.submit("alpha", workload_args=["--out", str(env.root / "b"),
                                                 "--sleep", "3"])
    env.wait_phase(blocker["job_id"], "executing")
    held_out = env.root / "held.jsonl"
    held = env.submit("alpha", workload_args=["--out", str(held_out)])
    (env.projects["alpha"] / "input.txt").write_text("alpha v2 (uncommitted)\n")
    state = env.wait_outcome(held["job_id"])
    assert state["outcome"] == "held-inputs-changed", state
    assert "diff_sha256" in state["reason"]
    assert not held_out.exists()


def test_operand_script_outside_the_worktree_is_fingerprinted(env_any):
    env = env_any
    script = env.root / "chain.sh"            # like /tmp/st7/chainC.sh
    script.write_text("#!/bin/sh\necho v1\n")
    script.chmod(0o755)
    blocker = env.submit("alpha", workload_args=["--out", str(env.root / "b"),
                                                 "--sleep", "3"])
    env.wait_phase(blocker["job_id"], "executing")
    result = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                     "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]),
                     "--", "/bin/sh", str(script))
    held = json.loads(result.stdout)
    script.write_text("#!/bin/sh\necho v2\n")
    state = env.wait_outcome(held["job_id"])
    assert state["outcome"] == "held-inputs-changed", state
    assert "operand" in state["reason"]


def test_untracked_file_change_is_detected(env_any):
    env = env_any
    extra = env.projects["alpha"] / "new_input.dat"
    extra.write_text("1\n")
    blocker = env.submit("alpha", workload_args=["--out", str(env.root / "b"),
                                                 "--sleep", "3"])
    env.wait_phase(blocker["job_id"], "executing")
    held = env.submit("alpha", workload_args=["--out", str(env.root / "h")])
    extra.write_text("2\n")
    state = env.wait_outcome(held["job_id"])
    assert state["outcome"] == "held-inputs-changed"
    assert "untracked_sha256" in state["reason"]


def test_stale_guard_variables_are_removed(env_any):
    env = env_any
    legacy_out = env.root / "legacy.jsonl"
    legacy = env.direct(legacy_out, sleep=3)
    env.wait_for(lambda: legacy_out.exists(), what="legacy start")
    out = env.root / "w.jsonl"
    stale = env.env(SMC_GUARD_JOB="stale/20200101T000000Z",
                    SMC_GUARD_CASE="stale", SMC_GUARD_HOME="/nonexistent")
    job = env.submit(workload_args=["--out", str(out)], env=stale)
    # a stale SMC_GUARD_JOB would make the guard pass straight through;
    # instead the job waits for the (test) lock like any other
    env.wait_phase(job["job_id"], "admission-wait")
    legacy.wait(60)
    assert env.wait_outcome(job["job_id"])["outcome"] == "succeeded"
    workload_env = events(out)[0]["env"]
    # G2a: the internal guard's neutral names; the stale legacy ones never
    # reach the job (no alias is configured here)
    assert workload_env["ACS_JOB_MARKER"].startswith("alpha.t/")
    assert workload_env["ACS_CASE"] == "alpha.t"
    assert "SMC_GUARD_JOB" not in workload_env
    assert "SMC_GUARD_CASE" not in workload_env
    assert workload_env["ACS_GUARD_HOME"] == str(env.guard_home)
    assert "SMC_GUARD_HOME" not in workload_env
    log = (env.state / "jobs" / job["job_id"] / "output.log").read_text()
    assert "pass-through" not in log
    assert events(out)[0]["t"] >= events(legacy_out)[-1]["t"]


def test_controlled_environment_and_no_credentials(env_any):
    env = env_any
    out = env.root / "w.jsonl"
    submitter = env.env(FAKE_API_TOKEN="supersecret-123",
                        GITHUB_TOKEN="ghp_supersecret",
                        ACS_TEST_PASS="passed-through",
                        UNRELATED_VAR="should-not-travel")
    job = env.submit(workload_args=["--out", str(out)], env=submitter,
                     extra=["--env", "FAKE_CASE_OPTION=7"])
    assert env.wait_outcome(job["job_id"])["outcome"] == "succeeded"
    man = json.loads((env.state / "jobs" / job["job_id"] /
                      "manifest.json").read_text())
    assert man["env"]["ACS_TEST_PASS"] == "passed-through"
    assert man["env"]["FAKE_CASE_OPTION"] == "7"
    assert "UNRELATED_VAR" not in man["env"]
    for path in env.state.rglob("*"):
        if path.is_file() and not path.is_socket():
            text = path.read_bytes()
            assert b"supersecret" not in text, path
            assert b"should-not-travel" not in text, path
    refused = env.acs("submit", "--case", "t", "--timeout-min", "1",
                      "--cwd", str(env.projects["alpha"]),
                      "--env", "GITHUB_TOKEN=x", "--", "true")
    assert refused.returncode == 2 and "credentials" in refused.stderr
    refused = env.acs("submit", "--case", "t", "--timeout-min", "1",
                      "--cwd", str(env.projects["alpha"]),
                      "--env", "SMC_GUARD_JOB=x", "--", "true")
    assert refused.returncode == 2


def test_private_permissions(env_any):
    env = env_any
    job = env.submit(workload_args=["--out", str(env.root / "w")])
    env.wait_outcome(job["job_id"])
    for path in [env.state, env.state / "jobs", env.state / "jobs" / job["job_id"]]:
        assert stat.S_IMODE(path.stat().st_mode) == 0o700, path
    for name in ("manifest.json", "state.json", "output.log"):
        mode = stat.S_IMODE((env.state / "jobs" / job["job_id"] /
                             name).stat().st_mode)
        assert mode & 0o077 == 0, (name, oct(mode))
    if env.queue:
        assert stat.S_IMODE((env.state / "pueue").stat().st_mode) == 0o700
        assert stat.S_IMODE((env.state / "pueue" / "run").stat().st_mode) == 0o700


def test_backend_change_is_refused(env_any, tmp_path):
    env = env_any
    cfg = env.config.read_text()
    # a different (well-formed) pinned sha256: the backend is not the
    # verified one, whichever backend kind the suite runs
    env.config.write_text(re.sub(r'sha256 = "[0-9a-f]{4}', 'sha256 = "0000',
                                 cfg, count=1))
    result = env.acs("submit", "--case", "t", "--timeout-min", "1",
                     "--cwd", str(env.projects["alpha"]), "--", "true")
    assert result.returncode == 2
    assert "verified" in result.stderr
    doctor = env.acs("doctor", "--json")
    report = json.loads(doctor.stdout)
    assert report["ok"] is False
    assert any(c["check"].startswith("backend guard matches") and not c["ok"]
               for c in report["checks"])


def test_doctor_is_read_only_and_reports(env_any):
    env = env_any
    report = json.loads(env.acs("doctor", "--json").stdout)
    names = {c["check"]: c for c in report["checks"]}
    assert names["backend guard options"]["ok"]
    assert names["state dir private"]["ok"]
    assert not names["test mode off"]["ok"]           # flagged as a warning
    assert not (env.guard_home / "lock").exists()     # doctor never creates it
    assert sys.version_info >= (3, 11)


def test_argv_echo_for_transport_checks(env_direct):
    env = env_direct
    result = env.acs("_argv", "--", *LITERALS)
    assert json.loads(result.stdout)["argv"] == LITERALS
