"""Environment copies stay out of pytest failure output (campaign PA, E4).

Why: a test sandbox builds a copy of the whole process environment for the
commands it runs.  When an assertion about such a command fails, pytest's
assertion rewriting prints the arguments of the asserted call, so the whole
copy (every variable of the machine that ran the suite, credentials of the
surrounding tooling included) lands in the failure text, and a failure text
is what gets pasted into evidence files and issues.  One such dump was
committed once.  ``conftest.QuietEnv`` is a ``dict`` whose ``repr`` names only
the number of variables; the helpers that copy ``os.environ`` return it.

What this establishes: with a sentinel secret in the process environment, the
failure message of an assertion over the environment of the migration
sandbox, of the ACS harness and of the engine sandbox contains no value of it
(a plain ``dict`` copy, the control, does, so the test can see what it looks
for); the copies are ``QuietEnv`` and still work as a ``dict`` for
``subprocess``.
"""
from __future__ import annotations

import os
import subprocess
import sys

import pytest

from conftest import Env, QuietEnv

from test_g2a_migrate import sandbox
from test_internal_guard import Sandbox

SENTINEL = "s3ntinel-0123456789abcdef-must-not-leak"


class Result:
    """Stands for a CompletedProcess whose asserted call received ``env``."""
    returncode = 1


def run_with(config, env):
    return Result()


def failure_message(config, env) -> str:
    with pytest.raises(AssertionError) as raised:
        assert run_with(config, env).returncode == 0
    return str(raised.value)


@pytest.fixture
def secret(monkeypatch):
    """A small process environment with the sentinel first: pytest cuts a
    long repr at 240 characters, so a full real environment could hide the
    value from a plain copy and the test would pass for the wrong reason."""
    real = dict(os.environ)
    small = {"SENTINEL_API_KEY": SENTINEL}
    small.update({k: real[k] for k in ("PATH", "HOME", "TMPDIR", "LANG")
                  if k in real})
    monkeypatch.setattr(os, "environ", small)
    return SENTINEL


def test_the_control_shows_a_plain_copy_leaks(secret):
    plain = dict(os.environ)
    assert SENTINEL in failure_message("config", plain)


def test_the_migration_sandbox_environment_is_not_printed(tmp_path, secret):
    config, env = sandbox(tmp_path, "sample-a")
    assert env["SENTINEL_API_KEY"] == SENTINEL      # it is in the copy
    message = failure_message(config, env)
    assert SENTINEL not in message
    assert "<environment: " in message
    assert SENTINEL not in repr(env) and SENTINEL not in str(env)


def test_the_acs_harness_environment_is_not_printed(secret):
    env = Env(queue=False)
    try:
        copy = env.env()
        assert copy["SENTINEL_API_KEY"] == SENTINEL
        assert SENTINEL not in failure_message("config", copy)
    finally:
        env.cleanup()


def test_the_engine_sandbox_environment_is_not_printed(tmp_path, secret):
    box = Sandbox(tmp_path)
    assert box.env["SENTINEL_API_KEY"] == SENTINEL
    assert SENTINEL not in failure_message("config", box.env)


def test_a_quiet_env_is_still_a_dict_for_subprocess(secret):
    env = QuietEnv({"A_VARIABLE": "value-1"})
    assert isinstance(env, dict)
    assert repr(env) == "<environment: 1 variables>"
    out = subprocess.run(
        [sys.executable, "-c",
         "import os; print(os.environ['A_VARIABLE'])"],
        capture_output=True, text=True, env=env, timeout=60)
    assert out.stdout.strip() == "value-1"
    # updates keep the type
    env.update(B_VARIABLE="value-2")
    assert isinstance(env, QuietEnv) and "value-2" not in repr(env)
