"""Register A34 (independent review of PR #33, follow-up 1), fixed in G3a.

``pin-backend`` and ``config migrate`` wrote the machine config first and
appended the authorization record (``<state>/authorizations.jsonl``, D8)
afterwards: when the append failed, the engine was pinned (or the config
migrated) with no record.  Now the record is appended first and the config
is written only after it; a record that cannot be appended refuses the
config write (exit 1, the config unchanged, no backup left); a config
write that fails after the record is recorded too (``action``
``pin-backend-failed`` / ``config-migrate-failed``).

The state dir is made read-only (0500) with its host id already present,
so nothing may need to create a file in it; a test running as root (which
ignores the mode) is skipped with a counted ``[root]`` reason.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import tempfile
from pathlib import Path

import pytest

from conftest import Env, copy_tree, sha256, skip_reason

from test_g2a_migrate import migrate, sandbox
from test_pin_backend import AUTH, acs, evidence, pinned

needs_non_root = pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason=skip_reason("root", "root ignores a read-only directory"))


@pytest.fixture
def edited():
    tmp = Path(tempfile.mkdtemp(prefix="acs-a34-", dir="/tmp"))
    env = Env(queue=False, tree=copy_tree(tmp))
    old = sha256(env.engine)
    with open(env.engine, "a") as handle:
        handle.write("\n# an unreviewed edit\n")
    env.old_sha, env.new_sha = old, sha256(env.engine)
    yield env
    for path in (env.state, env.config.parent):
        if path.exists():
            os.chmod(path, 0o700)
    env.cleanup()
    shutil.rmtree(tmp, ignore_errors=True)


def read_only(path: Path) -> None:
    os.chmod(path, stat.S_IRUSR | stat.S_IXUSR)


def records(state: Path) -> list[dict]:
    path = state / "authorizations.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()]


@needs_non_root
def test_pin_backend_refuses_when_the_record_cannot_be_appended(edited):
    env = edited
    assert env.acs("doctor").returncode in (0, 1)       # creates the host id
    assert (env.state / "host-id.json").exists()
    ev = evidence(env, env.new_sha)
    before = env.config.read_text()
    read_only(env.state)
    try:
        r = acs(env, "pin-backend", "--evidence", str(ev),
                "--authorized-by", AUTH)
    finally:
        os.chmod(env.state, 0o700)
    assert r.returncode == 1, r.stdout + r.stderr
    assert "authorization record" in r.stderr and "not" in r.stderr
    assert env.config.read_text() == before
    assert pinned(env) == env.old_sha
    assert records(env.state) == []


@needs_non_root
def test_pin_backend_records_first_then_writes_the_config(edited):
    env = edited
    # the state dir (inside the config's directory here) exists
    assert env.acs("doctor").returncode in (0, 1)
    ev = evidence(env, env.new_sha)
    before = env.config.read_text()
    # the config's directory refuses the temp-and-rename write
    read_only(env.config.parent)
    try:
        r = acs(env, "pin-backend", "--evidence", str(ev),
                "--authorized-by", AUTH)
    finally:
        os.chmod(env.config.parent, 0o700)
    assert r.returncode == 1, r.stdout + r.stderr
    assert env.config.read_text() == before
    found = records(env.state)
    assert [x["action"] for x in found] == ["pin-backend",
                                            "pin-backend-failed"]
    assert found[0]["engine_sha256"] == env.new_sha
    assert found[0]["authorized_by"] == AUTH
    assert found[1]["error"]
    # doctor shows the recorded pin is not the current one
    view = json.loads(env.acs("doctor", "--json").stdout)
    [check] = [c for c in view["checks"]
               if c["check"] == "backend pin authorization"]
    assert check["detail"]["is_the_current_pin"] is False


def test_pin_backend_still_records_and_pins(edited):
    env = edited
    ev = evidence(env, env.new_sha)
    r = acs(env, "pin-backend", "--evidence", str(ev), "--authorized-by",
            AUTH, "--json")
    assert r.returncode == 0, r.stdout + r.stderr
    assert pinned(env) == env.new_sha
    assert [x["action"] for x in records(env.state)] == ["pin-backend"]


@needs_non_root
def test_config_migrate_refuses_when_the_record_cannot_be_appended(
        tmp_path):
    config, env = sandbox(tmp_path, "sample-a")
    original = config.read_text()
    state = tmp_path / "home" / ".local" / "state" / "agent-compute"
    state.mkdir(parents=True)
    os.chmod(state, 0o700)
    (state / "host-id.json").write_text(json.dumps(
        {"schema": "agent-compute-host-id/1", "host_id": "f" * 32,
         "created_utc": "2026-10-08T00:00:00Z", "created_host": "alpha"}))
    read_only(state)
    try:
        result = migrate(config, env)
    finally:
        os.chmod(state, 0o700)
    assert result.returncode == 1, result.stdout + result.stderr
    assert "authorization record" in result.stderr
    assert config.read_text() == original
    assert not list(config.parent.glob("config.toml.pre-v2-*"))
    assert records(state) == []


@needs_non_root
def test_config_migrate_records_first_then_writes_the_config(tmp_path):
    config, env = sandbox(tmp_path, "sample-a")
    original = config.read_text()
    state = tmp_path / "home" / ".local" / "state" / "agent-compute"
    read_only(config.parent)
    try:
        result = migrate(config, env)
    finally:
        os.chmod(config.parent, 0o700)
    assert result.returncode == 1, result.stdout + result.stderr
    assert config.read_text() == original
    found = records(state)
    assert [x["action"] for x in found] == ["config-migrate",
                                            "config-migrate-failed"]
    assert found[1]["error"]



@needs_non_root
def test_pin_backend_refuses_cleanly_without_a_host_id(edited):
    """Review round 1, finding 11: a read-only state dir without
    host-id.json gives the same refusal, not a traceback."""
    env = edited
    ev = evidence(env, env.new_sha)
    before = env.config.read_text()
    env.state.mkdir(parents=True, mode=0o700)
    read_only(env.state)
    try:
        r = acs(env, "pin-backend", "--evidence", str(ev),
                "--authorized-by", AUTH)
    finally:
        os.chmod(env.state, 0o700)
    assert r.returncode == 1, r.stdout + r.stderr
    assert "Traceback" not in r.stderr
    assert "authorization record" in r.stderr
    assert env.config.read_text() == before
    assert not (env.state / "host-id.json").exists()
