"""K3 (campaign PA; register A43, A44, and K1's review note N1): a second
or trial installation is isolated from the default one, and ``uninstall``
looks at the config and state dir the installed CLI uses.

* A43: the wrapper of an installation whose config is not the default one
  (``ACS_INSTALL_HOME``, or ``install.py --config``) makes its CLI use that
  installation's own config.  Precedence: ``--config`` > the caller's
  ``AGENT_COMPUTE_CONFIG`` > the installation's own config.  The default
  installation's wrapper is unchanged, byte for byte.
* A44: ``config init`` prints an absolute ``state_dir`` derived from the
  install home it runs from, and refuses to print one that another
  installation's config already uses (or contains, or lies inside).
* N1: ``uninstall`` resolves ``~`` as the CLI does (``$HOME``, not
  ``ACS_INSTALL_HOME``) and refuses, naming both, when
  ``AGENT_COMPUTE_CONFIG`` names another config than the installation's.

Every test uses synthetic homes: ``HOME`` is a temporary directory, never the
developer's; nothing here touches a real installation.
"""

import json
import os
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

import pytest

from conftest import QuietEnv
from test_install import make_source

REPO = Path(__file__).resolve().parents[1]
CONFIG_REL = ".config/agent-compute/config.toml"
STATE_REL = ".local/state/agent-compute"
# the default installation's wrapper, as it was before K3 (a test pins it)
OLD_WRAPPER = ('#!/bin/sh\n# agent-compute wrapper (owned by agent-compute-'
               'scheduler/install.py)\nexec "{python}" "{prefix}/current/bin/'
               'agent-compute" "$@"\n')


def env_for(fake: Path, **extra) -> QuietEnv:
    """The environment of a command: ``HOME`` the synthetic real home, no
    ``AGENT_COMPUTE_CONFIG`` / ``ACS_INSTALL_HOME`` unless given (None
    removes)."""
    env = QuietEnv({k: v for k, v in os.environ.items()
                    if k not in ("AGENT_COMPUTE_CONFIG", "ACS_INSTALL_HOME")})
    env["HOME"] = str(fake)
    for key, value in extra.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = str(value)
    return env


class World:
    def __init__(self, root: Path):
        self.root = root
        self.source = make_source(root / "source")
        self.fake = root / "realhome"           # the synthetic "real" home
        self.fake.mkdir()
        self.sandbox = root / "sandbox"         # an ACS_INSTALL_HOME

    def install(self, install_home, *args, **extra):
        """``install.py [args] install`` with ``install_home`` as
        ``ACS_INSTALL_HOME`` (None: the variable is unset)."""
        env = env_for(self.fake, ACS_INSTALL_HOME=install_home, **extra)
        result = subprocess.run(
            [sys.executable, str(self.source / "install.py"), *args,
             "install"], capture_output=True, text=True, env=env)
        assert result.returncode == 0, result.stdout + result.stderr
        return result

    def installer(self, install_home, *args, **extra):
        env = env_for(self.fake, ACS_INSTALL_HOME=install_home, **extra)
        return subprocess.run(
            [sys.executable, str(self.source / "install.py"), *args],
            capture_output=True, text=True, env=env)

    def wrapper_of(self, install_home: Path) -> Path:
        return install_home / ".local/bin/agent-compute"

    def status(self, wrapper: Path, *args, **extra):
        """``<wrapper> [args] status``: with no config at the place it
        looks, the CLI names that place."""
        return subprocess.run([str(wrapper), *args, "status"],
                              capture_output=True, text=True,
                              env=env_for(self.fake, **extra))

    def config_init(self, install_home, *args, **extra):
        return subprocess.run(
            [sys.executable, str(REPO / "bin" / "agent-compute"), "config",
             "init", "--label", "trial", *args], capture_output=True,
            text=True, env=env_for(self.fake, ACS_INSTALL_HOME=install_home,
                                   **extra))


@pytest.fixture
def world(tmp_path):
    return World(tmp_path)


def config_of(root: Path) -> Path:
    return root / CONFIG_REL


def write_cfg(path: Path, state_dir: str | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('schema = "agent-compute-config/2"\nhost = "t"\n' +
                    (f'state_dir = "{state_dir}"\n' if state_dir else ""))
    return path


# --- A43: the wrapper --------------------------------------------------------

def test_a_sandbox_wrapper_uses_the_sandbox_config_by_default(world):
    world.install(world.sandbox)
    result = world.status(world.wrapper_of(world.sandbox))
    assert result.returncode != 0
    assert str(config_of(world.sandbox)) in result.stderr, result.stderr
    assert str(config_of(world.fake)) not in result.stderr


def test_a_sandbox_wrapper_does_not_read_the_default_installations_config(
        world):
    """The default installation's config exists; the sandbox's does not:
    the sandbox CLI must say its own is missing, not read the other."""
    write_cfg(config_of(world.fake))
    world.install(world.sandbox)
    result = world.status(world.wrapper_of(world.sandbox))
    assert f"no agent-compute config at {config_of(world.sandbox)}" in \
        result.stderr, result.stderr


def test_dash_dash_config_overrides_the_installations_config(world, tmp_path):
    world.install(world.sandbox)
    other = tmp_path / "chosen.toml"
    result = world.status(world.wrapper_of(world.sandbox), "--config",
                          str(other))
    assert str(other) in result.stderr
    assert str(config_of(world.sandbox)) not in result.stderr


def test_a_callers_environment_config_overrides_the_installations_config(
        world, tmp_path):
    world.install(world.sandbox)
    other = tmp_path / "from-env.toml"
    result = world.status(world.wrapper_of(world.sandbox),
                          AGENT_COMPUTE_CONFIG=other)
    assert str(other) in result.stderr
    assert str(config_of(world.sandbox)) not in result.stderr


def test_dash_dash_config_beats_the_environment_config(world, tmp_path):
    world.install(world.sandbox)
    flag, env = tmp_path / "flag.toml", tmp_path / "env.toml"
    result = world.status(world.wrapper_of(world.sandbox), "--config",
                          str(flag), AGENT_COMPUTE_CONFIG=env)
    assert str(flag) in result.stderr and str(env) not in result.stderr


def test_an_empty_environment_config_is_unset(world):
    """The CLI treats an empty AGENT_COMPUTE_CONFIG as unset; so does the
    wrapper."""
    world.install(world.sandbox)
    result = world.status(world.wrapper_of(world.sandbox),
                          AGENT_COMPUTE_CONFIG="")
    assert str(config_of(world.sandbox)) in result.stderr


def test_install_dash_dash_config_binds_the_wrapper_to_that_config(world,
                                                                   tmp_path):
    chosen = tmp_path / "chosen" / "my.toml"
    world.install(world.sandbox, "--config", str(chosen))
    result = world.status(world.wrapper_of(world.sandbox))
    assert str(chosen) in result.stderr, result.stderr


def test_the_wrapper_quotes_an_awkward_config_path(world, tmp_path):
    # (no `$name`: the CLI expands those in any config path, as `expand` does)
    awkward = tmp_path / "it's a \"q\"; `dir` &" / "c f.toml"
    world.install(world.sandbox, "--config", str(awkward))
    result = world.status(world.wrapper_of(world.sandbox))
    assert str(awkward) in result.stderr, result.stderr


@pytest.mark.parametrize("install_home", ["fake", "unset"])
def test_the_default_installation_wrapper_is_unchanged(world, install_home):
    """With the install home the real home (ACS_INSTALL_HOME equal to HOME,
    or unset), the wrapper is byte-for-byte the pre-K3 one and the CLI
    resolves the same config as before."""
    given = world.fake if install_home == "fake" else None
    (world.fake / ".local/bin").mkdir(parents=True)
    world.install(given)
    wrapper = world.wrapper_of(world.fake)
    prefix = world.fake / ".local/share/agent-compute"
    assert wrapper.read_text() == OLD_WRAPPER.format(python=sys.executable,
                                                     prefix=prefix)
    assert "AGENT_COMPUTE_CONFIG" not in wrapper.read_text()
    result = world.status(wrapper)
    assert f"no agent-compute config at {config_of(world.fake)}" in \
        result.stderr
    other = world.root / "other.toml"
    assert str(other) in world.status(
        wrapper, AGENT_COMPUTE_CONFIG=other).stderr


# --- A44: config init --------------------------------------------------------

def init_state_dir(result) -> str:
    assert result.returncode == 0, result.stderr
    return tomllib.loads(result.stdout)["state_dir"]


def test_config_init_in_a_sandbox_prints_an_absolute_state_dir_under_it(
        world):
    result = world.config_init(world.sandbox)
    state = init_state_dir(result)
    assert state == str(world.sandbox / STATE_REL)
    assert os.path.isabs(state) and "~" not in state
    assert str(config_of(world.sandbox)) in result.stdout     # where to save it


def test_config_init_for_the_default_installation_names_the_same_state_dir(
        world):
    """Written absolute, the effective path is the one ``~`` meant."""
    state = init_state_dir(world.config_init(None))
    assert state == str(world.fake / STATE_REL)
    from agent_compute.config import expand
    old = os.environ.get("HOME")
    os.environ["HOME"] = str(world.fake)
    try:
        assert str(expand("~/.local/state/agent-compute")) == state
    finally:
        if old is None:
            del os.environ["HOME"]
        else:
            os.environ["HOME"] = old


def test_config_init_for_the_default_installation_with_its_own_config(world):
    """Re-running init where the installation's own config already names
    that state dir is not a conflict."""
    write_cfg(config_of(world.fake), str(world.fake / STATE_REL))
    state = init_state_dir(world.config_init(None))
    assert state == str(world.fake / STATE_REL)


def test_config_init_refuses_a_state_dir_the_default_installation_uses(world):
    other = write_cfg(config_of(world.fake),
                      str(world.sandbox / STATE_REL))
    result = world.config_init(world.sandbox)
    assert result.returncode != 0 and result.stdout == ""
    assert str(other) in result.stderr, result.stderr


def test_config_init_expands_the_other_configs_tilde_against_home(world):
    """The other installation's ``~`` is the real HOME, as its CLI expands
    it (here reaching the sandbox's state dir through ``..``)."""
    other = write_cfg(config_of(world.fake),
                      "~/../sandbox/.local/state/agent-compute")
    result = world.config_init(world.sandbox)
    assert result.returncode != 0 and result.stdout == ""
    assert str(other) in result.stderr


def test_config_init_through_a_link_to_the_default_home_is_not_another(world):
    """An install home that resolves to the real home is the default
    installation, not another one."""
    link = world.root / "link-home"
    link.symlink_to(world.fake)
    write_cfg(config_of(world.fake), str(link / STATE_REL))
    assert init_state_dir(world.config_init(link)) == str(link / STATE_REL)


def test_config_init_refuses_a_state_dir_of_the_environment_config(world,
                                                                   tmp_path):
    other = write_cfg(tmp_path / "elsewhere.toml",
                      str(world.sandbox / STATE_REL))
    result = world.config_init(world.sandbox, AGENT_COMPUTE_CONFIG=other)
    assert result.returncode != 0 and result.stdout == ""
    assert str(other) in result.stderr


@pytest.mark.parametrize("theirs", ["{sb}/.local/state",
                                    "{sb}/.local/state/agent-compute/inner"])
def test_config_init_refuses_a_nested_state_dir(world, theirs):
    write_cfg(config_of(world.fake), theirs.format(sb=world.sandbox))
    result = world.config_init(world.sandbox)
    assert result.returncode != 0 and result.stdout == ""


def test_config_init_accepts_a_different_state_dir(world):
    write_cfg(config_of(world.fake), str(world.root / "prod-state"))
    assert init_state_dir(world.config_init(world.sandbox)) == \
        str(world.sandbox / STATE_REL)


def test_config_init_refuses_an_unreadable_other_config(world):
    if os.geteuid() == 0:
        pytest.skip("[root] root reads a mode-000 file")
    other = write_cfg(config_of(world.fake))
    other.chmod(0o000)
    try:
        result = world.config_init(world.sandbox)
    finally:
        other.chmod(0o644)
    assert result.returncode != 0 and result.stdout == ""
    assert str(other) in result.stderr


def test_config_init_refuses_an_unparsable_other_config(world):
    other = config_of(world.fake)
    other.parent.mkdir(parents=True)
    other.write_text("this is [not toml\n")
    result = world.config_init(world.sandbox)
    assert result.returncode != 0 and result.stdout == ""
    assert str(other) in result.stderr


def test_config_init_writes_nothing(world):
    write_cfg(config_of(world.fake))
    before = sorted(str(p) for p in world.root.rglob("*"))
    world.config_init(world.sandbox)
    assert sorted(str(p) for p in world.root.rglob("*")) == before


# --- N1: uninstall resolves what the CLI resolves ----------------------------

def make_job(state: Path, name: str, body: dict) -> None:
    job = state / "jobs" / name
    job.mkdir(parents=True)
    (job / "state.json").write_text(json.dumps(body))


def test_uninstall_uses_the_clis_home_for_a_tilde_state_dir(world):
    """K1's review note: the sandbox config keeps ``~`` (the CLI's HOME, the
    synthetic real home); an unfinished job is in that state dir.  Uninstall
    used the sandbox home and found none."""
    world.install(world.sandbox)
    write_cfg(config_of(world.sandbox))              # state_dir: the default ~
    make_job(world.fake / STATE_REL, "acs-live", {"phase": "running"})
    before = sorted(str(p) for p in world.sandbox.rglob("*"))
    result = world.installer(world.sandbox, "uninstall")
    assert result.returncode == 1, result.stdout + result.stderr
    assert "acs-live" in result.stderr
    assert "removed" not in result.stdout
    assert sorted(str(p) for p in world.sandbox.rglob("*")) == before


def test_uninstall_with_a_tilde_state_dir_and_no_job_proceeds(world):
    world.install(world.sandbox)
    write_cfg(config_of(world.sandbox), "~/k3-state")
    make_job(world.fake / "k3-state", "acs-done", {"outcome": "succeeded"})
    result = world.installer(world.sandbox, "uninstall")
    assert result.returncode == 0, result.stderr
    assert not (world.sandbox / ".local/share/agent-compute/current"
                ).is_symlink()


def test_uninstall_refuses_a_mismatching_environment_config(world, tmp_path):
    world.install(world.sandbox)
    write_cfg(config_of(world.sandbox), str(tmp_path / "state"))
    other = write_cfg(tmp_path / "elsewhere.toml", str(tmp_path / "s2"))
    result = world.installer(world.sandbox, "uninstall",
                             AGENT_COMPUTE_CONFIG=other)
    assert result.returncode == 1, result.stdout + result.stderr
    assert str(other) in result.stderr
    assert str(config_of(world.sandbox)) in result.stderr
    assert "removed" not in result.stdout
    assert (world.sandbox / ".local/share/agent-compute/current").is_symlink()


def test_uninstall_accepts_the_environment_config_naming_the_same_file(
        world, tmp_path):
    world.install(world.sandbox)
    write_cfg(config_of(world.sandbox), str(tmp_path / "state"))
    link = tmp_path / "link.toml"
    link.symlink_to(config_of(world.sandbox))
    for value in (config_of(world.sandbox), link, ""):
        result = world.installer(world.sandbox, "uninstall",
                                 AGENT_COMPUTE_CONFIG=value, **{})
        assert result.returncode == 0, result.stderr
        world.install(world.sandbox)
        write_cfg(config_of(world.sandbox), str(tmp_path / "state"))


def test_uninstall_refuses_a_mismatching_environment_config_even_if_idle(
        world, tmp_path):
    """Nothing to attribute: no guessing which of two configs is the CLI's."""
    world.install(world.sandbox)
    result = world.installer(world.sandbox, "uninstall",
                             AGENT_COMPUTE_CONFIG=tmp_path / "nowhere.toml")
    assert result.returncode == 1
    assert "nowhere.toml" in result.stderr


# --- the single-authority check is unchanged ---------------------------------

def test_the_single_authority_check_still_reports_a_second_authority(
        env_direct):
    """A sandbox CLI started by its wrapper has AGENT_COMPUTE_CONFIG set to
    its own config; a second state dir with an unfinished job at a known
    location is still an ERROR."""
    home = Path(tempfile.mkdtemp(prefix="acs-k3-", dir="/tmp"))
    try:
        other = home / STATE_REL / "jobs" / "acs-other-1"
        other.mkdir(parents=True)
        (other / "state.json").write_text(json.dumps(
            {"schema": "agent-compute-state/1", "job_id": "acs-other-1",
             "phase": "queued"}))
        result = env_direct.acs(
            "doctor", "--json", env=env_direct.env(
                ACS_TEST_SCAN_HOME=str(home),
                AGENT_COMPUTE_CONFIG=str(env_direct.config)))
        report = json.loads(result.stdout)
        state = report["single_authority"]["state_dir"]
        assert state["ok"] is False and result.returncode == 1
        assert any(s["unfinished"] == ["acs-other-1"]
                   for s in state["scanned"])
    finally:
        import shutil
        shutil.rmtree(home, ignore_errors=True)


# --- N1, round 2: uninstall reads the config the wrapper binds ----------------

def manifest_path(home: Path) -> Path:
    return home / ".local/share/agent-compute/install-manifest.json"


def drop_binding_record(home: Path) -> None:
    """A manifest written before the binding was recorded."""
    path = manifest_path(home)
    data = json.loads(path.read_text())
    data.pop("bound_config", None)
    path.write_text(json.dumps(data))


def alt_installation(world, *, job: bool):
    """A sandbox installed with ``--config sb/alt.toml`` whose state dir
    holds an unfinished job (or only a finished one)."""
    alt = world.sandbox / "alt.toml"
    state = world.sandbox / "state"
    write_cfg(alt, str(state))
    world.install(world.sandbox, "--config", str(alt))
    make_job(state, "acs-live" if job else "acs-done",
             {"phase": "running"} if job else {"outcome": "succeeded"})
    return alt


def intact(world) -> bool:
    return (world.sandbox / ".local/share/agent-compute/current"
            ).is_symlink() and world.wrapper_of(world.sandbox).is_file()


def test_uninstall_without_config_reads_the_config_the_wrapper_binds(world):
    """Installed with ``--config alt.toml``; uninstalled without the flag:
    the default config is absent, but the CLI reads alt.toml, which has an
    unfinished job."""
    alt_installation(world, job=True)
    result = world.installer(world.sandbox, "uninstall")
    assert result.returncode == 1, result.stdout + result.stderr
    assert "acs-live" in result.stderr
    assert "removed" not in result.stdout and intact(world)


def test_uninstall_of_an_idle_bound_installation_proceeds(world):
    alt_installation(world, job=False)
    result = world.installer(world.sandbox, "uninstall")
    assert result.returncode == 0, result.stderr
    assert not intact(world)


def test_uninstall_refuses_a_config_other_than_the_bound_one(world, tmp_path):
    """The converse: installed with the default config, uninstalled with
    ``--config other-idle.toml``."""
    state = tmp_path / "state"
    write_cfg(config_of(world.sandbox), str(state))
    make_job(state, "acs-live", {"phase": "running"})
    world.install(world.sandbox)
    idle = write_cfg(tmp_path / "other-idle.toml", str(tmp_path / "idle"))
    result = world.installer(world.sandbox, "--config", str(idle),
                             "uninstall")
    assert result.returncode == 1, result.stdout + result.stderr
    assert str(idle) in result.stderr
    assert str(config_of(world.sandbox)) in result.stderr
    assert "removed" not in result.stdout and intact(world)


def test_a_config_mismatch_is_not_bypassed_by_force(world, tmp_path):
    world.install(world.sandbox)
    idle = write_cfg(tmp_path / "other-idle.toml", str(tmp_path / "idle"))
    result = world.installer(world.sandbox, "--config", str(idle),
                             "uninstall", "--force")
    assert result.returncode == 1
    assert str(idle) in result.stderr and intact(world)


def test_the_flag_naming_the_bound_config_is_accepted(world):
    alt = alt_installation(world, job=False)
    result = world.installer(world.sandbox, "--config", str(alt), "uninstall")
    assert result.returncode == 0, result.stderr


def test_an_older_manifest_takes_the_binding_from_the_wrapper(world):
    """No recorded binding (installed before): derived from the owned
    wrapper, bound or unbound."""
    alt_installation(world, job=True)
    drop_binding_record(world.sandbox)
    result = world.installer(world.sandbox, "uninstall")
    assert result.returncode == 1 and "acs-live" in result.stderr
    assert intact(world)
    other = write_cfg(world.root / "o.toml", str(world.root / "idle"))
    result = world.installer(world.sandbox, "--config", str(other),
                             "uninstall")
    assert result.returncode == 1 and intact(world)
    assert str(other) in result.stderr
    assert str(world.sandbox / "alt.toml") in result.stderr


def test_an_older_manifest_with_an_unbound_wrapper_means_the_default_config(
        world):
    """The default installation's wrapper binds nothing: the CLI reads its
    default config, and uninstall must read that one."""
    (world.fake / ".local/bin").mkdir(parents=True)
    world.install(None)
    drop_binding_record(world.fake)
    other = write_cfg(world.root / "o.toml", str(world.root / "idle"))
    result = world.installer(None, "--config", str(other), "uninstall")
    assert result.returncode == 1, result.stdout + result.stderr
    assert str(other) in result.stderr and str(config_of(world.fake)) in \
        result.stderr
    assert (world.fake / ".local/bin/agent-compute").is_file()


# --- N1, round 3: a trial installed before K3 -----------------------------------

import importlib.util

_spec = importlib.util.spec_from_file_location("acs_install_k3",
                                               REPO / "install.py")
installer = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(installer)


def pre_k3_trial(world, *, job: bool):
    """A sandbox as an installer before K3 left it: no recorded binding and
    an unbound wrapper; its jobs ran with an explicit ``--config`` naming
    the sandbox config, so they sit in the sandbox state dir."""
    state = world.sandbox / "state"
    write_cfg(config_of(world.sandbox), str(state))
    world.install(world.sandbox)
    drop_binding_record(world.sandbox)
    wrapper = world.wrapper_of(world.sandbox)
    prefix = world.sandbox / ".local/share/agent-compute"
    wrapper.write_text(OLD_WRAPPER.format(python=sys.executable,
                                          prefix=prefix))
    make_job(state, "acs-live" if job else "acs-done",
             {"phase": "running"} if job else {"outcome": "succeeded"})


def test_a_pre_k3_trial_uninstall_examines_the_sandbox_config(world):
    pre_k3_trial(world, job=True)
    result = world.installer(world.sandbox, "uninstall")
    assert result.returncode == 1, result.stdout + result.stderr
    assert "acs-live" in result.stderr
    assert "removed" not in result.stdout and intact(world)


def test_a_pre_k3_trial_uninstall_with_the_sandbox_config_examines_it(world):
    pre_k3_trial(world, job=True)
    result = world.installer(world.sandbox, "--config",
                             str(config_of(world.sandbox)), "uninstall")
    assert result.returncode == 1, result.stdout + result.stderr
    assert "acs-live" in result.stderr and "wrapper binds" not in result.stderr
    assert intact(world)


def test_an_idle_pre_k3_trial_uninstalls(world):
    pre_k3_trial(world, job=False)
    result = world.installer(world.sandbox, "uninstall")
    assert result.returncode == 0, result.stderr
    assert not intact(world)


def test_a_pre_k3_trial_examines_the_default_config_too(world):
    """Its jobs may equally have run with the real default config."""
    pre_k3_trial(world, job=False)
    state = world.root / "prod-state"
    write_cfg(config_of(world.fake), str(state))
    make_job(state, "acs-prod", {"phase": "running"})
    result = world.installer(world.sandbox, "uninstall")
    assert result.returncode == 1 and "acs-prod" in result.stderr
    assert intact(world)


def test_for_the_default_installation_the_two_candidates_are_one_file(
        world, monkeypatch):
    """Behaviour unchanged where the install home is the real home."""
    monkeypatch.setenv("HOME", str(world.fake))
    monkeypatch.delenv("ACS_INSTALL_HOME", raising=False)
    found = installer.bound_configs({"files": []})
    assert len(found) == 1 and found[0] == config_of(world.fake)
