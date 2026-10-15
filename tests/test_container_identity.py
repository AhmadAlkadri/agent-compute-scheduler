"""A13: container provenance is read by the recorded container id, and a
name rebound after admission is logged and recorded.

Before, ``container_provenance()`` exec'd by name after the id check (a
TOCTOU: a container re-created under the same name between the two was
hashed instead of the recorded one).
"""

import json
import os
import subprocess

import pytest

from agent_compute import cli, procs
from agent_compute.config import load
from agent_compute.runner import Runner
from agent_compute.state import JobStore, utc_now, write_json

CID = "c" * 64
OTHER = "e" * 64
SHA = "ab" * 32


@pytest.fixture
def cenv(env_direct):
    env = env_direct
    env.config.write_text(env.config.read_text().replace(
        'container_workdir = "/work"\n',
        'container_workdir = "/work"\ncontainer_provenance = "prov.txt"\n'))
    return env


def fake_docker(monkeypatch, bound=CID):
    calls = []

    def docker(*args, timeout=30):
        calls.append(args)
        if args[0] == "exec":
            # A20: the provenance helper (`exec -w / <cid> python3 -I -S`)
            return subprocess.CompletedProcess(
                args, 0, json.dumps({"sha256": SHA}) + "\n", "")
        return subprocess.CompletedProcess(args, 1, "", "unexpected")
    monkeypatch.setattr(procs, "docker", docker)
    monkeypatch.setattr(procs, "container_id",
                        lambda name: bound if name == "__CONTAINER__" else
                        None)
    monkeypatch.setattr(procs, "docker_daemon_id", lambda: "daemon-1")
    return calls


def args(env):
    return cli.parser().parse_args(
        ["submit", "--case", "t", "--timeout-min", "1", "--reserve-gb", "0.5",
         "--cwd", str(env.projects["alpha"]), "--container", "__CONTAINER__"])


def make_job(env, monkeypatch, bound=CID):
    config = load(env.config)
    calls = fake_docker(monkeypatch)
    man = cli.build_manifest(config, args(env), ["python3", "-c", "pass"])
    store = JobStore(config.state_dir)
    job_dir = store.job_dir(man["job_id"])
    job_dir.mkdir(parents=True)
    write_json(job_dir / "manifest.json", man)
    store.update_state(man["job_id"], phase="queued")
    fake_docker(monkeypatch, bound)
    return config, man, calls


def test_submit_reads_provenance_by_container_id(cenv, monkeypatch):
    _, man, calls = make_job(cenv, monkeypatch)
    execs = [c for c in calls if c[0] == "exec"]
    assert execs and all(c[3] == CID for c in execs), calls
    assert man["container"]["provenance_sha256"] == SHA


def test_revalidation_reads_provenance_by_container_id(cenv, monkeypatch):
    config, man, _ = make_job(cenv, monkeypatch)
    calls = fake_docker(monkeypatch)
    runner = Runner(config, man["job_id"])
    assert runner.inputs_changed() is None
    execs = [c for c in calls if c[0] == "exec"]
    assert execs and all(c[3] == CID for c in execs), calls


def _admit(config, man, monkeypatch):
    runner = Runner(config, man["job_id"])
    attempt = runner.dir / "guard" / "attempt-1"
    attempt.mkdir(parents=True)
    write_json(attempt / "guard.json",
               {"pid": os.getpid(), "container_pid": 5, "job_id": "g-1",
                "started_utc": utc_now(), "outcome": "running"})
    monkeypatch.setattr(procs, "container_scan", lambda *a: None)
    runner.capture_workload(attempt)
    return runner, JobStore(config.state_dir).state(man["job_id"])


def test_name_rebound_after_admission_is_logged_and_recorded(cenv,
                                                             monkeypatch):
    config, man, _ = make_job(cenv, monkeypatch, bound=OTHER)
    runner, st = _admit(config, man, monkeypatch)
    check = st["container_name_check"]
    assert check["match"] is False
    assert check["recorded_id"] == CID and check["bound_id"] == OTHER
    log = (runner.dir / "output.log").read_text()
    assert "rebound" in log and OTHER[:12] in log


def test_name_still_bound_after_admission_is_recorded_as_a_match(
        cenv, monkeypatch):
    config, man, _ = make_job(cenv, monkeypatch)
    runner, st = _admit(config, man, monkeypatch)
    assert st["container_name_check"]["match"] is True
    assert "rebound" not in (runner.dir / "output.log").read_text()
