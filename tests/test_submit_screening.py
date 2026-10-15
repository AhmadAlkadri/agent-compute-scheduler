"""Submit-time screening of what a job asks for.

A7: `--env` and `env_passthrough` never reach a container workload (the
guard's `docker exec` exports only its own variables).
"""

import json
import sys

import pytest

from conftest import WORKLOAD

from agent_compute import cli, procs
from agent_compute.config import load


def submit_container(env, *extra, cmd="submit"):
    return env.acs(cmd, "--json", "--case", "t", "--timeout-min", "1",
                   "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]),
                   "--container", "__CONTAINER__", *extra, "--",
                   "python3", "-c", "pass")


@pytest.mark.parametrize("cmd", ["submit", "run"])
def test_container_job_refuses_env(env_direct, cmd):
    r = submit_container(env_direct, "--env", "FOO=1", cmd=cmd)
    assert r.returncode == cli.EXIT_NOT_ACCEPTED, r.stdout + r.stderr
    assert "env NAME=VALUE" in r.stderr, r.stderr
    assert not list((env_direct.state / "jobs").glob("acs-*")) \
        if (env_direct.state / "jobs").exists() else True


def _fake_container(monkeypatch):
    monkeypatch.setattr(procs, "container_id", lambda name: "c" * 64)
    monkeypatch.setattr(procs, "docker_daemon_id", lambda: "daemon-1")


def _args(env, *extra):
    return cli.parser().parse_args(
        ["submit", "--case", "t", "--timeout-min", "1", "--reserve-gb", "0.5",
         "--cwd", str(env.projects["alpha"]), "--container", "__CONTAINER__",
         *extra])


def test_container_passthrough_is_recorded_not_forwarded(env_direct,
                                                         monkeypatch):
    env = env_direct
    config = load(env.config)
    _fake_container(monkeypatch)
    monkeypatch.setenv("ACS_TEST_PASS", "secretless-value")
    man = cli.build_manifest(config, _args(env), ["python3", "-c", "pass"])
    assert man["container"]["env_not_forwarded"] == ["ACS_TEST_PASS"]
    assert "secretless-value" not in json.dumps(man["container"])
    monkeypatch.setattr(cli, "spawn_detached_runner", lambda *a: None)
    code, result = cli.accept(config, _args(env), ["python3", "-c", "pass"])
    assert code == 0, result
    assert any("ACS_TEST_PASS" in n and "env NAME=VALUE" in n
               for n in result["notices"]), result["notices"]


def test_container_passthrough_unset_is_silent(env_direct, monkeypatch):
    env = env_direct
    _fake_container(monkeypatch)
    monkeypatch.delenv("ACS_TEST_PASS", raising=False)
    man = cli.build_manifest(load(env.config), _args(env),
                             ["python3", "-c", "pass"])
    assert "env_not_forwarded" not in man["container"]


def test_host_job_still_takes_env(env_direct):
    env = env_direct
    job = env.submit(workload_args=["--out", str(env.root / "w")],
                     extra=["--env", "FOO=1"])
    assert env.wait_outcome(job["job_id"])["outcome"] == "succeeded"
    man = json.loads((env.state / "jobs" / job["job_id"] /
                      "manifest.json").read_text())
    assert man["env"]["FOO"] == "1"


# --- A14: a nested guard / agent-compute is refused -----------------------

NESTED = [
    ["guard.py", "run", "--case", "x", "--", "true"],
    ["/repo/scripts/dev/guard.py", "run"],
    [sys.executable, "/repo/scripts/dev/guard.py", "run", "--", "true"],
    ["python3", "-u", "scripts/dev/guard.py", "run"],
    ["python", "-X", "dev", "guard.py", "run"],
    ["python3.12", "-m", "agent_compute", "status"],
    ["python3", "-m", "agent_compute.cli", "status"],
    ["python3", "-um", "agent_compute", "status"],
    ["agent-compute", "status"],
    ["/nonexistent/.local/bin/agent-compute", "status"],
    ["python3", "bin/agent-compute", "status"],
    ["env", "A=1", "python3", "scripts/dev/guard.py", "run"],
    ["/usr/bin/env", "-i", "PATH=/bin", "agent-compute", "status"],
]
ALLOWED = [
    [sys.executable, str(WORKLOAD), "--out", "guard.py"],
    ["python3", "-c", "print('guard.py')"],
    ["python3", "-m", "pytest", "tests/test_guard.py"],
    ["make", "guard.py"],
    ["env", "A=1", "python3", "solve.py", "agent-compute"],
    ["guard.pyc"],
]


def _no_real_acs(env):
    """Were the refusal ever missing, the nested command would run as the
    workload: keep it away from every real agent-compute and config."""
    import os
    from pathlib import Path
    path = [d for d in os.environ.get("PATH", "").split(os.pathsep)
            if d and not (Path(d) / "agent-compute").exists()]
    return env.env(PATH=os.pathsep.join(path), HOME=str(env.root),
                   AGENT_COMPUTE_CONFIG=str(env.root / "no-config.toml"))


def _argv_id(argv):
    """The test id of a command: the interpreter and the workload script are
    named, not spelled (their paths differ between checkouts, and a path in a
    test id lands in every log of the run)."""
    shown = ["<python>" if a == sys.executable else
             "<workload>" if a == str(WORKLOAD) else a for a in argv]
    return " ".join(shown)[:40]


@pytest.mark.parametrize("argv", NESTED, ids=_argv_id)
def test_nested_guard_is_refused(env_direct, argv):
    r = env_direct.acs("submit", "--json", "--case", "t", "--timeout-min",
                       "1", "--reserve-gb", "0.5", "--cwd",
                       str(env_direct.projects["alpha"]), "--", *argv,
                       env=_no_real_acs(env_direct))
    assert r.returncode == cli.EXIT_NOT_ACCEPTED, r.stdout + r.stderr
    assert "pass the workload directly" in r.stderr, r.stderr


@pytest.mark.parametrize("argv", ALLOWED, ids=_argv_id)
def test_ordinary_commands_are_not_mistaken_for_a_nested_guard(argv):
    assert cli.nested_guard(argv) is None
