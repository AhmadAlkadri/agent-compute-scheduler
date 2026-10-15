"""K1 (campaign PA; register A25 widened, A49): ``install.py uninstall``
never reads "no unfinished jobs" without positive evidence.

Without ``--force`` it refuses (exit 1, the path and the reason named,
nothing removed) whenever the question cannot be answered: an unreadable,
unparsable or wrong-shaped config, a ``state_dir`` that is not an absolute
string (relative, an unexpanded ``$VAR``), a state dir, ``jobs`` directory,
job directory or ``state.json`` that exists but cannot be read, a state file
that is not a JSON object, a job directory without ``state.json``.  A state
dir that was correctly resolved and never created (ENOENT) is a true "no
jobs".  Absence is decided with ``errno`` (ENOENT/ENOTDIR only), not with
``Path.exists`` & co. (Python 3.14 turns a PermissionError into "absent",
A49), so these tests hold on 3.12 and 3.14.
"""

import json
import os
import stat
from pathlib import Path

import pytest

from test_install import (home, prefix_of, real_home,  # noqa: F401
                          run)                          # (home: fixture)

CONFIG_REL = ".config/agent-compute/config.toml"


def snapshot(root: Path) -> dict:
    """Every path under ``root`` with its mode and link target; reads no
    file (some are deliberately unreadable)."""
    out = {}
    for base, dirs, files in os.walk(root):
        for name in dirs + files:
            path = Path(base) / name
            st = os.lstat(path)
            out[str(path)] = (stat.S_IMODE(st.st_mode),
                              os.readlink(path) if path.is_symlink() else
                              st.st_size if stat.S_ISREG(st.st_mode)
                              else None)
    return out


@pytest.fixture
def restore():
    """Chmod'ed paths are made accessible again so tmp_path can be removed."""
    changed = []

    def chmod(path: Path, mode: int):
        if os.geteuid() == 0:
            pytest.skip("[root] root reads a mode-000 file")
        changed.append(path)
        os.chmod(path, mode)

    yield chmod
    for path in reversed(changed):
        try:
            os.chmod(path, 0o755 if path.is_dir() else 0o644)
        except OSError:
            pass


@pytest.fixture
def installed(home, tmp_path, monkeypatch):
    monkeypatch.delenv("ACS_K1_UNSET_VAR", raising=False)
    run(home, "install")
    state = tmp_path / "state"
    return home, state


def write_config(home: Path, state_dir_line: str | None) -> Path:
    config = home / CONFIG_REL
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text('schema = "agent-compute-config/2"\nhost = "t"\n' +
                      (state_dir_line + "\n" if state_dir_line else ""))
    return config


def configure(home: Path, state: Path) -> Path:
    return write_config(home, f'state_dir = "{state}"')


def make_job(state: Path, name: str, body=None, *, raw: str | None = None):
    job = state / "jobs" / name
    job.mkdir(parents=True)
    if raw is not None:
        (job / "state.json").write_text(raw)
    elif body is not None:
        (job / "state.json").write_text(json.dumps(body))
    return job


def refused(home: Path, *needles: str):
    before = snapshot(home)
    result = run(home, "uninstall", check=False)
    assert result.returncode == 1, result.stdout + result.stderr
    for needle in needles:
        assert str(needle) in result.stderr, result.stderr
    assert "removed" not in result.stdout
    assert snapshot(home) == before
    assert (prefix_of(home) / "current").is_symlink()
    assert (home / ".local/bin/agent-compute").is_file()
    return result


# --- the config -------------------------------------------------------------

def test_unreadable_config_refuses(installed, restore):
    home, state = installed
    config = configure(home, state)
    restore(config, 0o000)
    refused(home, config)


def test_unparsable_config_refuses(installed):
    home, state = installed
    config = home / CONFIG_REL
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text("this is [not toml\n")
    refused(home, config)


def test_config_that_is_not_utf8_refuses(installed):
    home, state = installed
    config = home / CONFIG_REL
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_bytes(b'state_dir = "\xff\xfe"\n')
    refused(home, config)


def test_config_that_is_a_directory_refuses(installed):
    home, state = installed
    config = home / CONFIG_REL
    config.mkdir(parents=True)
    refused(home, config)


@pytest.mark.parametrize("line", ["state_dir = 5", "state_dir = true",
                                  "state_dir = [\"/x\"]",
                                  "state_dir = { a = 1 }"])
def test_non_string_state_dir_refuses(installed, line):
    home, state = installed
    config = write_config(home, line)
    refused(home, config, "state_dir")


@pytest.mark.parametrize("value", ["state", "./state", "", "../state"])
def test_relative_state_dir_refuses(installed, value):
    home, state = installed
    config = write_config(home, f'state_dir = "{value}"')
    refused(home, config, "state_dir", "absolute")


@pytest.mark.parametrize("value", ["$ACS_K1_UNSET_VAR/state",
                                   "${ACS_K1_UNSET_VAR}/state",
                                   "/abs/$ACS_K1_UNSET_VAR/state"])
def test_unexpanded_variable_state_dir_refuses(installed, value):
    home, state = installed
    config = write_config(home, f'state_dir = "{value}"')
    refused(home, config, "state_dir", "ACS_K1_UNSET_VAR")


def test_unknown_user_tilde_state_dir_refuses(installed):
    home, state = installed
    config = write_config(home, 'state_dir = "~acs-no-such-user-k1/state"')
    refused(home, config, "state_dir")


def test_nul_in_state_dir_refuses(installed):
    home, state = installed
    config = write_config(home, 'state_dir = "/tmp/a\\u0000b"')
    refused(home, config, "state_dir")


# --- the state tree ---------------------------------------------------------

def test_unreadable_state_dir_refuses(installed, restore):
    home, state = installed
    configure(home, state)
    make_job(state, "acs-1", {"outcome": "succeeded"})
    restore(state, 0o000)
    refused(home, state)


def test_state_dir_that_is_a_file_refuses(installed):
    home, state = installed
    configure(home, state)
    state.write_text("not a directory\n")
    refused(home, state)


def test_dangling_state_dir_link_refuses(installed, tmp_path):
    home, state = installed
    configure(home, state)
    state.symlink_to(tmp_path / "unmounted-volume")
    refused(home, state)


def test_unreadable_jobs_dir_refuses(installed, restore):
    home, state = installed
    configure(home, state)
    job = make_job(state, "acs-1", {"outcome": "succeeded"})
    restore(state / "jobs", 0o000)
    refused(home, state / "jobs")


def test_jobs_that_is_a_file_refuses(installed):
    home, state = installed
    configure(home, state)
    state.mkdir()
    (state / "jobs").write_text("x\n")
    refused(home, state / "jobs")


def test_unreadable_state_json_refuses(installed, restore):
    home, state = installed
    configure(home, state)
    job = make_job(state, "acs-1", {"outcome": "succeeded"})
    restore(job / "state.json", 0o000)
    refused(home, job / "state.json")


def test_unreadable_job_dir_refuses(installed, restore):
    home, state = installed
    configure(home, state)
    job = make_job(state, "acs-1", {"outcome": "succeeded"})
    restore(job, 0o000)
    refused(home, job)


def test_invalid_json_refuses(installed):
    home, state = installed
    configure(home, state)
    job = make_job(state, "acs-1", raw="{not json")
    refused(home, job / "state.json")


@pytest.mark.parametrize("raw", ["[]", '["outcome"]', "null", "5", '"done"'])
def test_non_object_json_refuses(installed, raw):
    home, state = installed
    configure(home, state)
    job = make_job(state, "acs-1", raw=raw)
    result = refused(home, job / "state.json")
    assert "Traceback" not in result.stderr


# N2 (review of K1): only a terminal outcome (state.OUTCOMES) finishes a job;
# any other value is an unfinished job, named
@pytest.mark.parametrize("outcome", ["running", "queued", "bogus", "", None,
                                     5, ["succeeded"], {"a": 1}, True])
def test_a_non_terminal_outcome_is_an_unfinished_job(installed, outcome):
    home, state = installed
    configure(home, state)
    make_job(state, "acs-1", {"outcome": outcome})
    refused(home, "acs-1", "unfinished jobs reference")


def test_a_state_without_an_outcome_is_an_unfinished_job(installed):
    home, state = installed
    configure(home, state)
    make_job(state, "acs-1", {"phase": "running"})
    refused(home, "acs-1", "unfinished jobs reference")


def test_every_terminal_outcome_finishes_a_job(installed):
    from agent_compute.state import OUTCOMES
    home, state = installed
    configure(home, state)
    for n, outcome in enumerate(OUTCOMES):
        make_job(state, f"acs-{n}", {"outcome": outcome})
    run(home, "uninstall")
    assert not (home / ".local/bin/agent-compute").exists()


# F1 / F2 (review of K1): a dangling link is not absence
def test_dangling_config_link_refuses(installed, tmp_path):
    home, state = installed
    other = tmp_path / "elsewhere-state"
    make_job(other, "acs-live", {"phase": "running"})
    assert not (home / ".local/state/agent-compute").exists()
    config = home / CONFIG_REL
    config.parent.mkdir(parents=True, exist_ok=True)
    config.symlink_to(tmp_path / "unmounted" / "config.toml")
    refused(home, config, "link")


def test_dangling_link_in_the_middle_of_the_state_dir_path_refuses(
        installed, tmp_path):
    home, state = installed
    (home / "data").symlink_to(tmp_path / "unmounted-data")
    write_config(home, f'state_dir = "{home}/data/deeper/acs-state"')
    refused(home, home / "data", "link")


def test_dangling_link_directly_above_the_state_dir_refuses(installed,
                                                            tmp_path):
    home, state = installed
    (home / "data").symlink_to(tmp_path / "unmounted-data")
    write_config(home, f'state_dir = "{home}/data/acs-state"')
    refused(home, home / "data", "link")


def test_dangling_jobs_link_refuses(installed, tmp_path):
    home, state = installed
    configure(home, state)
    state.mkdir()
    (state / "jobs").symlink_to(tmp_path / "unmounted-jobs")
    refused(home, state / "jobs", "link")


def test_state_dir_below_a_live_link_that_was_never_created_is_no_jobs(
        installed, tmp_path):
    home, state = installed
    real = tmp_path / "real-data"
    real.mkdir()
    (home / "data").symlink_to(real)
    write_config(home, f'state_dir = "{home}/data/deeper/acs-state"')
    run(home, "uninstall")
    assert not (home / ".local/bin/agent-compute").exists()


def test_job_dir_without_state_json_counts_as_unfinished(installed):
    home, state = installed
    configure(home, state)
    make_job(state, "acs-1", {"outcome": "succeeded"})
    job = make_job(state, "acs-2")
    refused(home, "acs-2", "unfinished jobs reference")


def test_a_bad_job_among_finished_ones_refuses(installed):
    home, state = installed
    configure(home, state)
    for n in range(3):
        make_job(state, f"acs-ok{n}", {"outcome": "succeeded"})
    job = make_job(state, "acs-bad", raw="{")
    refused(home, job / "state.json")


def test_unfinished_job_still_refuses_and_is_named(installed):
    home, state = installed
    configure(home, state)
    make_job(state, "acs-done", {"outcome": "succeeded"})
    make_job(state, "acs-live", {"phase": "running"})
    result = refused(home, "acs-live", "unfinished jobs reference")
    assert "acs-done" not in result.stderr


def test_tilde_state_dir_resolves_against_the_clis_home(installed):
    """K3 (K1's note N1): ``~`` is the CLI's ``$HOME`` (the synthetic real
    home of ``run``), not the sandbox install home."""
    home, state = installed
    write_config(home, 'state_dir = "~/k1-state"')
    make_job(real_home(home) / "k1-state", "acs-live", {"phase": "running"})
    refused(home, "acs-live", "unfinished jobs reference")


# --- the cases that proceed -------------------------------------------------

def test_readable_idle_state_uninstalls(installed):
    home, state = installed
    configure(home, state)
    make_job(state, "acs-1", {"outcome": "succeeded"})
    make_job(state, "acs-2", {"outcome": "failed"})
    (state / "jobs" / "stray-file").write_text("x\n")
    result = run(home, "uninstall")
    assert "removed" in result.stdout
    assert not (prefix_of(home) / "current").exists()
    assert not (home / ".local/bin/agent-compute").exists()


def test_never_created_state_dir_is_no_jobs(installed):
    home, state = installed
    configure(home, state)
    assert not state.exists()
    run(home, "uninstall")
    assert not (home / ".local/bin/agent-compute").exists()


def test_state_dir_without_jobs_dir_is_no_jobs(installed):
    home, state = installed
    configure(home, state)
    state.mkdir()
    run(home, "uninstall")
    assert not (home / ".local/bin/agent-compute").exists()


def test_state_dir_below_a_file_is_no_jobs(installed, tmp_path):
    home, state = installed
    blocker = tmp_path / "blocker"
    blocker.write_text("x\n")
    write_config(home, f'state_dir = "{blocker}/state"')
    run(home, "uninstall")
    assert not (home / ".local/bin/agent-compute").exists()


def test_absent_state_dir_key_uses_the_default_under_the_install_home(
        installed):
    home, state = installed
    write_config(home, None)
    run(home, "uninstall")                       # default dir never created
    assert not (home / ".local/bin/agent-compute").exists()


def test_absent_state_dir_key_default_with_unfinished_job_refuses(installed):
    home, state = installed
    write_config(home, None)
    # the default ``~/.local/state/agent-compute`` is the CLI's, under HOME
    make_job(real_home(home) / ".local/state/agent-compute", "acs-live",
             {"phase": "running"})
    refused(home, "acs-live", "unfinished jobs reference")


def test_missing_config_and_no_default_state_dir_uninstalls(installed):
    home, state = installed
    assert not (home / CONFIG_REL).exists()
    run(home, "uninstall")
    assert not (home / ".local/bin/agent-compute").exists()


def test_missing_config_with_an_existing_default_state_dir_refuses(installed):
    """Without the config the state of an earlier use cannot be attributed
    or ruled out: refuse, naming both."""
    home, state = installed
    default = real_home(home) / ".local/state/agent-compute"
    make_job(default, "acs-1", {"outcome": "succeeded"})
    refused(home, home / CONFIG_REL, default)


# --- --force is unchanged ---------------------------------------------------

def test_force_still_uninstalls_with_an_unfinished_job(installed):
    home, state = installed
    configure(home, state)
    make_job(state, "acs-live", {"phase": "running"})
    result = run(home, "uninstall", "--force")
    assert "removed" in result.stdout
    assert not (home / ".local/bin/agent-compute").exists()


@pytest.mark.parametrize("case", ["bad-toml", "relative", "non-dict"])
def test_force_still_uninstalls_under_an_unanswerable_state(installed, case):
    home, state = installed
    if case == "bad-toml":
        config = home / CONFIG_REL
        config.parent.mkdir(parents=True, exist_ok=True)
        config.write_text("[nope\n")
    elif case == "relative":
        write_config(home, 'state_dir = "rel"')
    else:
        configure(home, state)
        make_job(state, "acs-1", raw="[]")
    result = run(home, "uninstall", "--force")
    assert result.returncode == 0 and "removed" in result.stdout
    assert not (home / ".local/bin/agent-compute").exists()
