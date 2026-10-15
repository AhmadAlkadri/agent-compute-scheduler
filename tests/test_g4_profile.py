"""Campaign G slice G4: the standalone test profile (decision D7).

What this establishes about the harness itself (``tests/conftest.py``,
``tests/guard_profiles/``):

- the fixtures are generic: every ``Env`` uses one host-independent test
  profile (``guard_profiles/standard.json``), whatever the platform, small
  enough to load on any host the suite supports; the engine sandbox uses a
  generic profile too; the Mac and Lenovo numbers stay only as the named
  examples ``mac.json``/``lenovo.json``;
- every fixture profile states its disk floor (the engine's 20 GB default
  would make a guard-running test depend on the free disk of ``/tmp``);
- the default container image is public and pinned by digest;
  ``ACS_TEST_IMAGE`` overrides it; the harness pulls it only on request
  (``ACS_TEST_PULL=1``, for the daemon's own platform) and otherwise skips
  the Docker-backed tests with a counted ``[docker]`` reason (absent image,
  an image of another architecture, no CLI, no daemon, a failed pull),
  shown here with a fake ``docker``;
- every skip in the suite carries a category tag that
  ``tools/skip_report.py`` counts;
- the test tree names no private path, and the job's test hooks are
  referenced through the engine's own names.

The Docker-backed probe at the end runs the image the suite would use and
checks what the container tests need inside it.
"""
from __future__ import annotations

import ast
import json
import os
import re
import secrets
import subprocess
import sys
from pathlib import Path

import pytest

import conftest
from conftest import REPO, Env, QuietEnv

from agent_compute import config as cfgmod

TESTS = REPO / "tests"
PROFILES = TESTS / "guard_profiles"
PUBLIC = re.compile(r"^python:3\.11\.6-slim-bookworm@sha256:[0-9a-f]{64}$")


# --- generic fixtures -------------------------------------------------------------------

def test_the_env_profile_is_one_generic_fixture():
    data = json.loads((PROFILES / "standard.json").read_text())
    assert data.pop("schema") == "agent-compute-guard-profile/1"
    expected = {k: ("" if v is None else v) for k, v in data.items()}
    assert conftest.TEST_PROFILE == expected
    assert conftest.TEST_PROFILE["name"] not in ("mac", "lenovo")
    assert not hasattr(conftest, "HOST_PROFILE")      # no platform switch


@pytest.mark.parametrize("ram_gb", [4, 64])
def test_the_env_config_loads_on_a_small_host(monkeypatch, ram_gb):
    """The runner loads the config with the host's real RAM as the budget
    cap (G2b); the Env profile must fit any supported host."""
    monkeypatch.setattr(cfgmod, "host_resources",
                        lambda *a, **k: {"memory_gb": float(ram_gb),
                                         "cores": 4, "source": "test"},
                        raising=False)
    env = Env(queue=False)
    try:
        config = cfgmod.load(env.config)
        assert config.profile["budget_gb"] <= 2.0
        assert config.profile["disk_floor_gb"] == \
            conftest.TEST_PROFILE["disk_floor_gb"]
    finally:
        env.cleanup()


def test_every_fixture_profile_states_its_disk_floor():
    from agent_compute.guard import engine
    names = sorted(p.name for p in PROFILES.glob("*.json"))
    assert {"standard.json", "sandbox.json", "mac.json",
            "lenovo.json"} <= set(names)
    for path in PROFILES.glob("*.json"):
        raw = json.loads(path.read_text())
        assert "disk_floor_gb" in raw, path.name
        assert raw["disk_floor_gb"] <= 1.0, path.name
        assert engine.load_profile(path)["disk_floor_gb"] == \
            raw["disk_floor_gb"]
    # the engine's own default is unchanged (a host config's)
    assert engine.DISK_FLOOR_GB == 20.0


def test_the_named_examples_keep_the_hosts_numbers():
    from agent_compute.guard import engine
    mac = engine.load_profile(PROFILES / "mac.json")
    lenovo = engine.load_profile(PROFILES / "lenovo.json")
    assert (mac["name"], mac["budget_gb"], mac["floor_gb"],
            mac["container_cpus"], mac["runlog_tag"]) == \
        ("mac", 12.0, 8.0, 10, None)
    assert (lenovo["name"], lenovo["budget_gb"], lenovo["floor_gb"],
            lenovo["container_cpus"], lenovo["runlog_tag"]) == \
        ("lenovo", 5.0, 1.5, 8, "host=lenovo")


def test_the_engine_sandbox_uses_a_generic_profile(tmp_path):
    from test_internal_guard import Sandbox
    sandbox = Sandbox(tmp_path)
    assert sandbox.profile == PROFILES / "sandbox.json"
    data = json.loads(sandbox.profile.read_text())
    assert data["name"] not in ("mac", "lenovo")
    # the ported guard tests' numbers (guard.py's mac row), under a neutral
    # name and with the test disk floor
    assert (data["budget_gb"], data["floor_gb"], data["container_cpus"]) \
        == (12.0, 8.0, 10)
    # the sandbox sets the neutral guard home variable, not the legacy one
    assert sandbox.env["ACS_GUARD_HOME"] == str(sandbox.home)
    assert "SMC_GUARD_HOME" not in sandbox.env


# --- the test image ---------------------------------------------------------------------

def test_the_default_image_is_public_and_pinned_by_digest():
    assert PUBLIC.match(conftest.PUBLIC_IMAGE), conftest.PUBLIC_IMAGE
    # no private image is named by the harness; the override is the
    # documented ACS_TEST_IMAGE (below)
    assert not hasattr(conftest, "LEGACY_EVIDENCE_IMAGE")


def _conftest_value(name: str, **env) -> str:
    clean = QuietEnv((k, v) for k, v in os.environ.items()
                     if not k.startswith("ACS_TEST_"))
    clean.update(env)
    result = subprocess.run(
        [sys.executable, "-c", f"import conftest; print(conftest.{name})"],
        cwd=TESTS, env=clean, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def test_acs_test_image_overrides_the_default():
    assert PUBLIC.match(_conftest_value("TEST_IMAGE"))
    assert _conftest_value("TEST_IMAGE",
                           ACS_TEST_IMAGE="example/other:1") == \
        "example/other:1"
    assert _conftest_value("PULL") == "False"
    assert _conftest_value("PULL", ACS_TEST_PULL="1") == "True"


def fake_docker(tmp_path: Path, *, daemon="linux/arm64", present=None,
                pull_gives=None, pull_fails=False) -> tuple[Path, Path]:
    """A ``docker`` that answers ``version``, ``image inspect`` and
    ``pull`` from a state file, and logs every call."""
    state = tmp_path / "docker-state.json"
    log = tmp_path / "docker-calls.log"
    state.write_text(json.dumps({"present": present,
                                 "pull_gives": pull_gives,
                                 "pull_fails": pull_fails,
                                 "daemon": daemon}))
    script = tmp_path / "docker"
    script.write_text(f"""#!{sys.executable}
import json, sys
state_path = {str(state)!r}
state = json.load(open(state_path))
open({str(log)!r}, "a").write(" ".join(sys.argv[1:]) + "\\n")
args = sys.argv[1:]
if args[:1] == ["version"]:
    if state["daemon"] is None:
        print("Cannot connect to the Docker daemon", file=sys.stderr)
        sys.exit(1)
    print(state["daemon"])
elif args[:2] == ["image", "inspect"]:
    if state["present"] is None:
        print("No such image", file=sys.stderr)
        sys.exit(1)
    print(state["present"])
elif args[:1] == ["pull"]:
    if state["pull_fails"]:
        print("pull access denied: network unreachable", file=sys.stderr)
        sys.exit(1)
    state["present"] = state["pull_gives"]
    json.dump(state, open(state_path, "w"))
else:
    sys.exit(2)
""")
    script.chmod(0o755)
    return script, log


IMAGE = "python:3.11.6-slim-bookworm@sha256:" + "a" * 64


def calls(log: Path) -> list[str]:
    return log.read_text().splitlines() if log.exists() else []


def test_no_docker_cli_is_a_counted_skip():
    status = conftest.docker_image_status(IMAGE, pull=True, docker=None)
    assert status["ok"] is False
    assert status["reason"].startswith("[docker] ")
    assert "no docker CLI" in status["reason"]


def test_an_unreachable_daemon_is_a_counted_skip(tmp_path):
    docker, log = fake_docker(tmp_path, daemon=None)
    status = conftest.docker_image_status(IMAGE, pull=True,
                                          docker=str(docker))
    assert status["ok"] is False
    assert status["reason"].startswith("[docker] ")
    assert "daemon" in status["reason"]
    assert not any(c.startswith("pull") for c in calls(log))


def test_an_absent_image_is_skipped_unless_a_pull_is_requested(tmp_path):
    docker, log = fake_docker(tmp_path, present=None,
                              pull_gives="linux/arm64")
    status = conftest.docker_image_status(IMAGE, pull=False,
                                          docker=str(docker))
    assert status["ok"] is False
    assert status["reason"].startswith("[docker] ")
    assert IMAGE in status["reason"] and "ACS_TEST_PULL=1" in \
        status["reason"]
    assert not any(c.startswith("pull") for c in calls(log))


def test_a_requested_pull_fetches_the_daemons_platform(tmp_path):
    docker, log = fake_docker(tmp_path, present=None,
                              pull_gives="linux/arm64")
    status = conftest.docker_image_status(IMAGE, pull=True,
                                          docker=str(docker))
    assert status["ok"] is True, status
    assert status["pulled"] is True
    assert f"pull --platform linux/arm64 {IMAGE}" in calls(log)


def test_a_failed_pull_is_a_counted_skip(tmp_path):
    docker, log = fake_docker(tmp_path, present=None, pull_fails=True)
    status = conftest.docker_image_status(IMAGE, pull=True,
                                          docker=str(docker))
    assert status["ok"] is False
    assert status["reason"].startswith("[docker] ")
    assert "pull" in status["reason"] and "network unreachable" in \
        status["reason"]


def test_an_image_of_another_architecture_is_not_used(tmp_path):
    """An image present only for another architecture would run emulated
    (or not at all): treated as absent, never used."""
    docker, log = fake_docker(tmp_path, present="linux/amd64",
                              pull_gives="linux/arm64")
    status = conftest.docker_image_status(IMAGE, pull=False,
                                          docker=str(docker))
    assert status["ok"] is False
    assert "linux/amd64" in status["reason"] and "linux/arm64" in \
        status["reason"]
    assert not any(c.startswith("pull") for c in calls(log))
    pulled = conftest.docker_image_status(IMAGE, pull=True,
                                          docker=str(docker))
    assert pulled["ok"] is True and pulled["pulled"] is True
    assert f"pull --platform linux/arm64 {IMAGE}" in calls(log)


def test_a_present_image_is_used_without_a_pull(tmp_path):
    docker, log = fake_docker(tmp_path, present="linux/arm64")
    status = conftest.docker_image_status(IMAGE, pull=True,
                                          docker=str(docker))
    assert status == {"ok": True, "reason": None, "pulled": False,
                      "image": IMAGE, "platform": "linux/arm64"}
    assert not any(c.startswith("pull") for c in calls(log))


# --- skip accounting --------------------------------------------------------------------

def test_the_harness_and_the_report_share_the_categories():
    sys.path.insert(0, str(REPO / "tools"))
    import skip_report
    assert tuple(conftest.SKIP_CATEGORIES) == skip_report.CATEGORIES
    assert conftest.skip_reason("docker", "x") == "[docker] x"
    with pytest.raises(ValueError):
        conftest.skip_reason("weather", "x")
    assert skip_report.category(conftest.LEGACY_SKIP_REASON) == "legacy"


def _reason_node(call: ast.Call) -> ast.AST | None:
    func = call.func
    name = ast.unparse(func)
    if name in ("pytest.skip",):
        return call.args[0] if call.args else next(
            (k.value for k in call.keywords if k.arg == "reason"), None)
    if name in ("pytest.mark.skipif",):
        return next((k.value for k in call.keywords if k.arg == "reason"),
                    call.args[1] if len(call.args) > 1 else None)
    return None


def test_every_skip_in_the_suite_carries_a_category_tag():
    """A skip reason opens with ``[category]`` (or is built by
    ``skip_reason``), so tools/skip_report.py counts it."""
    untagged = []
    for path in sorted(TESTS.glob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            reason = _reason_node(node)
            if reason is None:
                continue
            if isinstance(reason, ast.Call) and \
                    ast.unparse(reason.func).endswith("skip_reason"):
                continue
            if isinstance(reason, ast.Name):     # a shared constant
                continue
            head = reason
            if isinstance(head, ast.JoinedStr):
                head = head.values[0] if head.values else None
            text = head.value if isinstance(head, ast.Constant) else ""
            if not (isinstance(text, str) and re.match(r"^\[[a-z]+\] ",
                                                       text)):
                untagged.append(f"{path.name}:{node.lineno}")
    assert not untagged, untagged


# --- no private path, neutral hook names ---------------------------------------------------

# (the owner's account names are screened by the private deny-patterns, which
# the private tree's release checks run over this directory; only the generic
# shapes live here)
PRIVATE = re.compile(r"/Users/|/home/[a-z]|Documents/GitHub|~/smc\b")


def test_the_test_tree_names_no_private_path():
    found = [f"{p.name}:{n}" for p in sorted(TESTS.glob("*.py"))
             if p.name != "test_g4_profile.py"        # the pattern itself
             for n, line in enumerate(p.read_text().splitlines(), 1)
             if PRIVATE.search(line)]
    assert not found, found


def test_the_harness_names_the_hooks_through_the_engine():
    from agent_compute.guard import engine
    assert conftest.FREE_MEM_ENV == engine.ENV_FREE_FILE
    assert conftest.SAMPLER_FAIL_ENV == engine.ENV_SAMPLER_FAIL_FILE
    assert conftest.VM_FOOTPRINT_ENV == engine.ENV_VM_FOOTPRINT_FILE
    hooks = re.compile(r"SMC_GUARD_(FREE_MEM|SAMPLER_FAIL|VM_FOOTPRINT)_FILE")
    # the literal names stay only where a test is about the names (the
    # config's allowed hooks, the guard.py parity of the legacy profile)
    allowed = {"test_g2a_config.py", "test_guard_interop.py",
               "test_g4_profile.py"}
    found = [f"{p.name}:{n}" for p in sorted(TESTS.glob("*.py"))
             if p.name not in allowed
             for n, line in enumerate(p.read_text().splitlines(), 1)
             if hooks.search(line)]
    assert not found, found


PROCPS_IN_CONTAINER = re.compile(
    r'"exec",(?:[^\]\n]|\n)*?"(kill|pkill|pgrep|ps|top)"')


def test_no_test_needs_procps_inside_the_container():
    """The public image (Debian slim) has no procps: an in-container
    ``kill``/``pkill``/``ps`` silently fails (found by the first Lenovo
    run with the public image, 2026-10-08: a leftover workload).  Tests
    signal and scan in-container processes through ``python3``
    (``conftest.container_kill``, ``container_kill_matching``)."""
    found = []
    for path in sorted(TESTS.glob("*.py")):
        text = path.read_text()
        for match in PROCPS_IN_CONTAINER.finditer(text):
            found.append(f"{path.name}:{text[:match.start()].count(chr(10)) + 1}"
                         f" {match.group(1)}")
    assert not found, found


# --- the image the suite uses has what the container tests need ------------------------

@pytest.mark.needs_docker
def test_the_test_image_has_what_the_container_tests_need():
    """python3 (from ``/``, isolated: the A20 scan form), bash, setsid,
    nice and sha256sum, in a disposable labelled container."""
    name = f"acs-test-{secrets.token_hex(4)}"
    probe = ("set -e; cd /; for t in python3 bash setsid nice sha256sum "
             "sleep; do command -v $t; done; "
             "python3 -I -S -c 'import sys, os; "
             "assert sys.version_info >= (3, 8); print(os.getcwd())'; "
             "echo x | sha256sum; setsid -w true; nice -n 5 true")
    result = subprocess.run(
        ["docker", "run", "--rm", "--name", name, "--label",
         "agent-compute-test=1", "-w", "/", conftest.TEST_IMAGE, "bash",
         "-c", probe], capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stdout + result.stderr
    lines = result.stdout.splitlines()
    assert len([l for l in lines if l.startswith("/")]) >= 6, lines
    assert "/" in lines
