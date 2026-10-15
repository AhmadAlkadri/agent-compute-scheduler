"""Installed-CLI backstop for the activation floor (review of 090c01b).

The floor in ``install.py`` binds only this installer and later ones: an
older checkout's installer can still point ``current`` at a pre-floor
version.  So the *installed* CLI checks its own installation, read-only:
``doctor`` reports an error and ``submit`` refuses when ``current`` is below
the highest installed level or below MIN_ACTIVATABLE_LEVEL.

Everything runs under a sandbox prefix (``ACS_INSTALL_HOME``) and the
isolated test harness; the real installation is never read or touched.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from test_install import make_source, run as install

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
try:
    from agent_compute import safety  # noqa: E402
except ImportError:                   # before the module existed (fail-first)
    safety = None


def make_version(prefix: Path, name: str, level, parts=("bin", "src",
                                                        "skill")) -> Path:
    d = prefix / "versions" / name
    for part in parts:
        (d / part).mkdir(parents=True)
    (d / "VERSION").write_text(name + "\n")
    if level is not None:
        (d / "SAFETY").write_text(f"{level}\n")
    return d


def point_current(prefix: Path, name: str) -> None:
    """What an older (pre-floor) installer's ``activate`` does."""
    link = prefix / "current"
    if link.is_symlink():
        link.unlink()
    link.symlink_to(prefix / "versions" / name)


# --- the shared, read-only level logic -------------------------------------

def test_floor_ok_at_the_highest_level(tmp_path):
    make_version(tmp_path, "0.1.0-aaaaaaaaaa", safety.SAFETY_LEVEL)
    make_version(tmp_path, "0.1.0-edfd7268fe", None)          # legacy 2
    point_current(tmp_path, "0.1.0-aaaaaaaaaa")
    assert safety.floor_problems(tmp_path) == []
    # S4 (level 3): the legacy level-2 dir is below the floor now
    point_current(tmp_path, "0.1.0-edfd7268fe")
    assert safety.floor_problems(tmp_path)


@pytest.mark.parametrize("name,level", [
    ("0.1.0-eb369eb937", None),     # legacy 1: an old installer's rollback
    ("0.1.0-6e843380a2", None),     # unlevelled, not in the table: 0
    ("0.1.0-dirty00001-dirty", 0),  # a dirty install
    ("0.1.0-levelone01", 1),
    ("0.1.0-edfd7268fe", None),     # legacy 2: below the S4 floor
    ("0.1.0-leveltwo01", 2),        # every pre-S4 floored version
    ("0.1.0-levelthr01", 3),        # every pre-G1 version (C1-C5)
    ("0.1.2-levelfour1", 4)])       # every pre-G2a version (v0.1.2)
def test_floor_problem_below_the_minimum(tmp_path, name, level):
    make_version(tmp_path, "0.1.0-aaaaaaaaaa", safety.SAFETY_LEVEL)
    make_version(tmp_path, name, level)
    point_current(tmp_path, name)
    problems = safety.floor_problems(tmp_path)
    assert problems and "roll forward" in " ".join(problems)


def test_floor_problem_below_the_highest_installed(tmp_path):
    make_version(tmp_path, "0.1.0-aaaaaaaaaa", safety.SAFETY_LEVEL)
    make_version(tmp_path, "0.1.0-bbbbbbbbbb", safety.SAFETY_LEVEL + 1)
    point_current(tmp_path, "0.1.0-aaaaaaaaaa")
    problems = safety.floor_problems(tmp_path)
    assert any("highest installed" in p for p in problems)


def test_floor_problem_without_current(tmp_path):
    make_version(tmp_path, "0.1.0-aaaaaaaaaa", 2)
    assert safety.floor_problems(tmp_path)


def test_floor_check_is_read_only(tmp_path):
    make_version(tmp_path, "0.1.0-aaaaaaaaaa", 2)
    make_version(tmp_path, "0.1.0-eb369eb937", None)
    point_current(tmp_path, "0.1.0-eb369eb937")
    before = sorted((str(p), p.stat().st_mtime_ns, p.is_symlink())
                    for p in tmp_path.rglob("*"))
    safety.floor_problems(tmp_path)
    assert sorted((str(p), p.stat().st_mtime_ns, p.is_symlink())
                  for p in tmp_path.rglob("*")) == before


def test_installation_is_found_only_for_an_installed_copy(tmp_path):
    fake = tmp_path / "versions" / "0.1.0-aaaaaaaaaa" / "src" / \
        "agent_compute" / "safety.py"
    assert safety.installation(fake) == (tmp_path,
                                         tmp_path / "versions" /
                                         "0.1.0-aaaaaaaaaa")
    assert safety.installation(Path(safety.__file__)) is None   # checkout


# --- the installed CLI ------------------------------------------------------

@pytest.fixture
def installed(tmp_path, env_direct):
    home = tmp_path / "home"
    home.mkdir()
    make_source(tmp_path / "source")
    install(home, "install")
    prefix = home / ".local/share/agent-compute"
    active = Path(os.path.realpath(prefix / "current"))
    return env_direct, prefix, active


def acs_installed(env, version_dir: Path, *args):
    return subprocess.run([sys.executable, str(version_dir / "bin" /
                                               "agent-compute"),
                           "--config", str(env.config), *args],
                          capture_output=True, text=True, env=env.env(),
                          timeout=120)


def floor_check(report: dict) -> dict:
    return next(c for c in report["checks"]
                if c["check"] == "installation safety floor")


def submit_args(env):
    return ("submit", "--json", "--case", "t", "--timeout-min", "1",
            "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]),
            "--", "true")


def test_doctor_from_a_checkout_does_not_check_the_floor(env_direct):
    report = json.loads(env_direct.acs("doctor", "--json").stdout)
    check = floor_check(report)
    assert check["ok"] and check["level"] == "ok"
    assert "not an installed copy" in json.dumps(check["detail"])


def test_installed_doctor_passes_at_the_floor(installed):
    env, prefix, active = installed
    report = json.loads(acs_installed(env, active, "doctor", "--json").stdout)
    check = floor_check(report)
    assert check["ok"], check


def test_installed_doctor_and_submit_refuse_below_the_highest_level(
        installed):
    env, prefix, active = installed
    make_version(prefix, "0.1.0-future00001",
                 safety.SAFETY_LEVEL + 1)               # a newer install
    doctor = acs_installed(env, active, "doctor", "--json")
    assert doctor.returncode == 1
    report = json.loads(doctor.stdout)
    check = floor_check(report)
    assert not check["ok"] and check["level"] == "error"
    assert report["ok"] is False
    result = acs_installed(env, active, *submit_args(env))
    assert result.returncode == 2
    body = json.loads(result.stdout)
    assert body["accepted"] is False
    assert "below the safety floor" in body["reason"]
    assert "roll forward" in body["reason"]
    assert not list((env.state / "jobs").glob("*")) \
        if (env.state / "jobs").exists() else True


def test_installed_submit_refuses_when_current_was_moved_below_the_floor(
        installed):
    """An older installer rolled `current` back: the running copy (still
    this version, e.g. invoked by its path) sees `current` below the floor."""
    env, prefix, active = installed
    make_version(prefix, "0.1.0-eb369eb937", None)     # legacy level 1
    point_current(prefix, "0.1.0-eb369eb937")
    report = json.loads(acs_installed(env, active, "doctor",
                                      "--json").stdout)
    assert not floor_check(report)["ok"]
    result = acs_installed(env, active, *submit_args(env))
    assert result.returncode == 2
    assert "below the safety floor" in json.loads(result.stdout)["reason"]
