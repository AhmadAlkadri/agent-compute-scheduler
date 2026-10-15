"""A4: `pin-backend` is operator-only and evidence-bound (audit e8); D8
(G2b): an operator action is a recorded authorization.

Before: any caller could accept an unreviewed guard with one command, and
the submit refusal advertised it.  Now the refusal says to report the
mismatch; `pin-backend` needs a committed suite output that names the new
engine sha256 on this host and shows 0 failed, and `--authorized-by "<who,
when, what>"` (since G2b; it replaced the re-typed sha at a terminal and
`--i-am-the-operator`), rewrites the config atomically (0600), changing the
`sha256` line of the `[backend]` table only, and appends the authorization
to `<state>/authorizations.jsonl`.

The edited engine is a copy of this tree (``conftest.copy_tree``) whose
engine changed after it was pinned, as a new installed version's would.
"""

import os
import shutil
import stat
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

import pytest

from conftest import (Env, copy_tree, gate_body, gate_header, gate_text, git,
                      sha256)

from agent_compute import cli

AUTH = "operator test, 2026-10-06, accept the edited engine"


@pytest.fixture
def edited():
    """An isolated env whose engine (a copy) was edited after pinning."""
    tmp = Path(tempfile.mkdtemp(prefix="acs-pin-", dir="/tmp"))
    env = Env(queue=False, tree=copy_tree(tmp))
    old = sha256(env.engine)
    with open(env.engine, "a") as handle:
        handle.write("\n# an unreviewed edit\n")
    env.old_sha, env.new_sha = old, sha256(env.engine)
    yield env
    env.cleanup()
    shutil.rmtree(tmp, ignore_errors=True)


def acs(env, *args):
    # stdin is never a terminal here: this is how an agent calls it
    return subprocess.run([sys.executable, str(env.bin), "--config",
                           str(env.config), *args], capture_output=True,
                          text=True, stdin=subprocess.DEVNULL, env=env.env(),
                          timeout=120)


def evidence(env, sha, *, host="testhost", summary=None, commit=True,
             name="pytest-mac.txt") -> Path:
    repo = env.root / "evidence-repo"
    if not repo.exists():
        repo.mkdir()
        git(repo, "init", "-q", "-b", "main")
    path = repo / name
    body = gate_body()
    if summary is not None:
        body = body.rstrip("\n").rsplit("\n", 1)[0] + "\n" + summary + "\n"
    path.write_text(gate_text(gate_header(host, sha), body))
    if commit:
        git(repo, "add", name)
        git(repo, "commit", "-q", "-m", "evidence")
    return path


def submit(env):
    return acs(env, "submit", "--json", "--case", "t", "--timeout-min", "1",
               "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]),
               "--", "true")


def pinned(env) -> str:
    return tomllib.loads(env.config.read_text())["backend"]["sha256"]


def test_e8_refusal_does_not_advertise_pin_backend(edited):
    r = submit(edited)
    assert r.returncode == cli.EXIT_NOT_ACCEPTED
    assert "pin-backend" not in r.stderr, r.stderr
    assert "report" in r.stderr.lower()


def test_e8_pin_without_evidence_is_refused(edited):
    before = edited.config.read_text()
    for args in (("pin-backend", "--json"),
                 ("pin-backend", "--json", "--authorized-by", AUTH)):
        r = acs(edited, *args)
        assert r.returncode != 0, r.stdout
    assert edited.config.read_text() == before
    assert submit(edited).returncode == cli.EXIT_NOT_ACCEPTED


def test_pin_needs_a_recorded_authorization(edited):
    ev = evidence(edited, edited.new_sha)
    before = edited.config.read_text()
    r = acs(edited, "pin-backend", "--evidence", str(ev))
    assert r.returncode == cli.EXIT_NOT_ACCEPTED, r.stdout + r.stderr
    assert "--authorized-by" in r.stderr and "operator" in r.stderr
    assert edited.config.read_text() == before
    assert not (edited.state / "authorizations.jsonl").exists()


@pytest.mark.parametrize("case", ["other-sha", "failed", "errors",
                                  "no-summary", "other-host", "uncommitted",
                                  "modified", "missing"])
def test_evidence_mismatch_is_refused(edited, case):
    sha, host, summary, commit = edited.new_sha, "testhost", None, True
    if case == "other-sha":
        sha = edited.old_sha
    elif case == "failed":
        summary = "===== 1 failed, 353 passed, 1 skipped in 900.00s ====="
    elif case == "errors":
        summary = "===== 354 passed, 2 errors in 900.00s ====="
    elif case == "no-summary":
        summary = "(interrupted)"
    elif case == "other-host":
        host = "lenovo"
    elif case == "uncommitted":
        commit = False
    ev = evidence(edited, sha, host=host, summary=summary, commit=commit)
    if case == "modified":
        ev.write_text(ev.read_text() + "edited after the commit\n")
    if case == "missing":
        ev = ev.with_name("absent.txt")
    before = edited.config.read_text()
    r = acs(edited, "pin-backend", "--evidence", str(ev),
            "--authorized-by", AUTH)
    assert r.returncode == 1, (case, r.stdout, r.stderr)
    assert edited.config.read_text() == before
    assert not (edited.state / "authorizations.jsonl").exists()
    assert submit(edited).returncode == cli.EXIT_NOT_ACCEPTED


def test_pin_with_evidence_writes_the_config_atomically(edited):
    ev = evidence(edited, edited.new_sha)
    os.chmod(edited.config, 0o644)
    inode = edited.config.stat().st_ino
    r = acs(edited, "pin-backend", "--evidence", str(ev),
            "--authorized-by", AUTH, "--json")
    assert r.returncode == 0, r.stdout + r.stderr
    assert pinned(edited) == edited.new_sha
    st = edited.config.stat()
    assert stat.S_IMODE(st.st_mode) == 0o600
    assert st.st_ino != inode                 # replaced, not rewritten
    assert [p.name for p in edited.config.parent.iterdir()
            if p.name.startswith(".config.toml")] == []
    assert (edited.state / "authorizations.jsonl").exists()
    assert submit(edited).returncode == 0


def test_only_the_backend_sha256_line_is_replaced(edited):
    # G2a: a top-level sha256 key is an unknown key in a v2 config (and the
    # rewritten config must load), so only another table's line is checked
    # through the CLI; set_backend_sha's own test covers a top-level one
    text = edited.config.read_text()
    text = text.replace("[backend.extra_env]\n",
                        '[backend.extra_env]\nsha256 = "extra-untouched"\n', 1)
    edited.config.write_text(text)
    ev = evidence(edited, edited.new_sha)
    r = acs(edited, "pin-backend", "--evidence", str(ev),
            "--authorized-by", AUTH)
    assert r.returncode == 0, r.stdout + r.stderr
    raw = tomllib.loads(edited.config.read_text())
    assert raw["backend"]["extra_env"]["sha256"] == "extra-untouched"
    assert raw["backend"]["sha256"] == edited.new_sha


def test_set_backend_sha_inserts_when_absent():
    text = ('schema = "x"\nsha256 = "top"\n\n[backend]\nkind = "acs-guard"\n'
            '\n[backend.extra_env]\nsha256 = "e"\n')
    new = cli.set_backend_sha(text, "ab" * 32)
    raw = tomllib.loads(new)
    assert raw["backend"]["sha256"] == "ab" * 32
    assert raw["sha256"] == "top" and raw["backend"]["extra_env"]["sha256"] \
        == "e"
    with pytest.raises(cli.ConfigError):
        cli.set_backend_sha('backend = {kind = "acs-guard"}\n', "ab" * 32)
