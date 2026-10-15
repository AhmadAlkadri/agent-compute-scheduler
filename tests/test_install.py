"""7. Installation: idempotent, collision-safe, reversible, discoverable."""

import json
import os
import subprocess
import sys
from pathlib import Path

import shutil

import pytest

from conftest import QuietEnv

REPO = Path(__file__).resolve().parents[1]


def make_source(root: Path) -> Path:
    """A clean git copy of the working tree (bin, src, skill, install.py).

    Installs run from it, so a test's version is clean (and its safety level
    real) whatever state the developer's checkout is in."""
    root.mkdir(parents=True)
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc")
    for part in ("bin", "src", "skill"):
        shutil.copytree(REPO / part, root / part, ignore=ignore)
    shutil.copy2(REPO / "install.py", root / "install.py")
    (root / ".gitignore").write_text("__pycache__/\n*.pyc\n")
    genv = QuietEnv({**os.environ, "GIT_AUTHOR_NAME": "t",
                     "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t",
                     "GIT_COMMITTER_EMAIL": "t@t"})
    for cmd in (["init", "-q"], ["add", "-A"], ["commit", "-q", "-m", "src"]):
        subprocess.run(["git", "-C", str(root), *cmd], check=True,
                       capture_output=True, env=genv)
    return root


def real_home(home: Path) -> Path:
    """The ``HOME`` of a test's installer and CLI: a synthetic "real" home,
    distinct from the sandbox install home ``home`` (K3: ``~`` in a config
    is the CLI's ``$HOME``, never ``ACS_INSTALL_HOME``).  A test never reads
    the developer's real home."""
    return home.parent / "realhome"


def run(home: Path, *args, check=True):
    source = home.parent / "source"
    script = (source if source.is_dir() else REPO) / "install.py"
    env = QuietEnv({k: v for k, v in os.environ.items()
                    if k != "AGENT_COMPUTE_CONFIG"})
    env.update({"ACS_INSTALL_HOME": str(home), "HOME": str(real_home(home))})
    result = subprocess.run([sys.executable, str(script), *args],
                            capture_output=True, text=True, env=env)
    if check and result.returncode != 0:
        raise AssertionError(result.stdout + result.stderr)
    return result


def tree(root: Path) -> dict:
    return {str(p.relative_to(root)): (os.readlink(p) if p.is_symlink() else
                                      p.read_bytes() if p.is_file() else None)
            for p in sorted(root.rglob("*"))}


@pytest.fixture
def home(tmp_path):
    h = tmp_path / "home"
    (h / ".claude/skills/unrelated-skill").mkdir(parents=True)
    (h / ".claude/skills/unrelated-skill/SKILL.md").write_text("keep me\n")
    (h / ".agents/skills").mkdir(parents=True)
    (h / ".agents/skills/other").symlink_to("/nonexistent/other")
    (h / ".local/bin").mkdir(parents=True)
    (h / ".local/bin/other-tool").write_text("#!/bin/sh\n")
    make_source(tmp_path / "source")
    return h


def test_install_is_idempotent_and_discoverable(home):
    run(home, "install")
    first = tree(home)
    run(home, "install")
    assert tree(home) == first
    for client in (".claude/skills", ".agents/skills"):
        skill = home / client / "agent-compute-scheduler" / "SKILL.md"
        text = skill.read_text()
        assert text.startswith("---\nname: agent-compute-scheduler\n")
        assert "description:" in text.split("---")[1]
        # a controlled copy, not the development checkout
        assert str(REPO) not in os.path.realpath(skill)
    wrapper = home / ".local/bin/agent-compute"
    out = subprocess.run([str(wrapper), "--version"], capture_output=True,
                         text=True)
    assert out.returncode == 0 and out.stdout.strip()
    assert (home / ".claude/skills/unrelated-skill/SKILL.md").read_text() == \
        "keep me\n"
    assert os.readlink(home / ".agents/skills/other") == "/nonexistent/other"


def test_installed_copy_is_read_only_and_repo_edits_do_not_leak(home):
    run(home, "install")
    current = Path(os.path.realpath(home / ".local/share/agent-compute/current"))
    cli = current / "src/agent_compute/cli.py"
    assert not os.access(cli, os.W_OK)
    assert cli.read_bytes() == (REPO / "src/agent_compute/cli.py").read_bytes()


def test_install_ships_the_internal_guard_read_only_and_runnable(home,
                                                                 tmp_path):
    """Campaign S2: the internal guard is copied with the rest of src/ (no
    installer change) and runs from the version directory, by path and as
    a module.  The acs-guard backend runs it from there (S4)."""
    run(home, "install")
    current = Path(os.path.realpath(home / ".local/share/agent-compute/current"))
    engine = current / "src/agent_compute/guard/engine.py"
    assert engine.read_bytes() == \
        (REPO / "src/agent_compute/guard/engine.py").read_bytes()
    assert not os.access(engine, os.W_OK)
    env = QuietEnv((k, v) for k, v in os.environ.items()
                   if not k.startswith("SMC_GUARD_"))
    env["SMC_GUARD_HOME"] = str(tmp_path / "guard-home")
    for argv, extra in (([str(engine)], {}),
                        (["-m", "agent_compute.guard"],
                         {"PYTHONPATH": str(current / "src")})):
        out = subprocess.run([sys.executable, *argv, "status"],
                             capture_output=True, text=True, timeout=30,
                             cwd=str(tmp_path), env={**env, **extra})
        assert out.returncode == 0, out.stderr
        assert json.loads(out.stdout)["lock_holder"] is None


def test_collision_is_refused(home):
    (home / ".claude/skills/agent-compute-scheduler").mkdir()
    result = run(home, "install", check=False)
    assert result.returncode == 1
    assert "not owned" in result.stderr
    assert not (home / ".local/bin/agent-compute").exists()


def test_uninstall_removes_only_owned_files(home):
    before = tree(home)
    run(home, "install")
    run(home, "uninstall")
    assert tree(home) == before


def test_rollback_activates_a_previous_version(home):
    run(home, "install")
    status = json.loads(run(home, "status").stdout)
    version = status["active"]
    assert run(home, "activate", version).returncode == 0
    missing = run(home, "activate", "0.0.0-nonexistent", check=False)
    assert missing.returncode == 1


def test_config_is_never_overwritten(home, tmp_path):
    cfg = home / ".config/agent-compute/config.toml"
    cfg.parent.mkdir(parents=True)
    cfg.write_text("# operator's own\n")
    example = REPO / "examples" / "config.example.toml"
    run(home, "install", "--config-from", str(example))
    assert cfg.read_text() == "# operator's own\n"
    run(home, "uninstall")
    assert cfg.read_text() == "# operator's own\n"


# --- A5: activation safety floor -------------------------------------------

import importlib.util

_spec = importlib.util.spec_from_file_location("acs_install", REPO / "install.py")
installer = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(installer)
# the package version prefix of every version this tree installs (a name
# that does not start with it is refused by `activate`); the legacy names
# below (0.1.0-edfd7268fe, 0.1.0-eb369eb937) stay literal on purpose
PKG = installer.package_version(REPO)


def vname(suffix: str) -> str:
    return f"{PKG}-{suffix}"


def prefix_of(home: Path) -> Path:
    return home / ".local/share/agent-compute"


def active_name(home: Path) -> str:
    return json.loads(run(home, "status").stdout)["active"]


def current_target(home: Path) -> str:
    return os.readlink(prefix_of(home) / "current")


def clone_version(home: Path, name: str, level, *, version_text=None,
                  drop=()) -> Path:
    """A copy of the installed version under ``name`` with a chosen level.

    ``level`` None removes the SAFETY record (like every pre-floor dir)."""
    versions = prefix_of(home) / "versions"
    source = versions / active_name(home)
    target = versions / name
    shutil.copytree(source, target, symlinks=True)
    for dirpath, dirnames, filenames in os.walk(target):
        Path(dirpath).chmod(0o755)
        for f in filenames:
            (Path(dirpath) / f).chmod(0o644)
    (target / "VERSION").write_text((version_text or name) + "\n")
    safety = target / "SAFETY"
    if level is None:
        safety.unlink(missing_ok=True)
    else:
        safety.write_text(f"{level}\n")
    for part in drop:
        shutil.rmtree(target / part)
    return target


def test_safety_constants_are_set():
    # 3 = campaign S4 (the acs-guard backend makes S3's fixes live)
    # 4 = campaign G slice G1 (boundary audit C1-C5)
    # 5 = campaign G slice G2a (the nested pass-through needs the claim)
    # 6 = campaign G slice G3a (a request id bound on this host is never
    #     submitted on another: register A39)
    # 7 = campaign PA slice K1 (uninstall refuses unless it has positive
    #     evidence of no unfinished jobs: register A25 widened)
    # 8 = campaign PA slice K2 (a cancelled job's request id is freed only
    #     by positive never-launched evidence: register A24)
    # 9 = campaign PA slice K3 (uninstall resolves the config and state dir
    #     the installed CLI uses; installation isolation: N1, A43, A44)
    # 10 = campaign PA slice K4 (pin-backend accepts only the full-suite
    #     file tools/gate_run.py writes: register A47)
    assert installer.SAFETY_LEVEL == 11
    assert installer.MIN_ACTIVATABLE_LEVEL == 11
    assert installer.MIN_ACTIVATABLE_LEVEL <= installer.SAFETY_LEVEL


def test_install_records_the_safety_level(home):
    run(home, "install")
    version = prefix_of(home) / "versions" / active_name(home)
    assert (version / "SAFETY").read_text().strip() == \
        str(installer.SAFETY_LEVEL)
    assert not os.access(version / "SAFETY", os.W_OK)
    status = json.loads(run(home, "status").stdout)
    assert status["safety_levels"][active_name(home)] == installer.SAFETY_LEVEL


def test_activate_refuses_a_version_without_a_level(home):
    run(home, "install")
    before = current_target(home)
    clone_version(home, vname("prefloor01"), None)  # like every deployed dir
    result = run(home, "activate", vname("prefloor01"), check=False)
    assert result.returncode == 1
    assert "no recorded safety level" in result.stderr
    assert "roll forward instead" in result.stderr
    assert current_target(home) == before


def test_activate_refuses_a_level_below_the_floor(home):
    run(home, "install")
    before = current_target(home)
    clone_version(home, vname("levelone01"), 1)
    result = run(home, "activate", vname("levelone01"), check=False)
    assert result.returncode == 1
    assert "roll forward instead" in result.stderr
    assert current_target(home) == before


def test_activate_refuses_a_level_below_the_active_one(home):
    run(home, "install")
    original = active_name(home)
    clone_version(home, vname("future00001"), installer.SAFETY_LEVEL + 1)
    assert run(home, "activate", vname("future00001")).returncode == 0  # higher
    before = current_target(home)
    result = run(home, "activate", original, check=False)             # lower
    assert result.returncode == 1
    assert "lower than the active" in result.stderr
    assert "roll forward instead" in result.stderr
    assert current_target(home) == before
    assert active_name(home) == vname("future00001")


def test_activate_accepts_an_equal_or_higher_level(home):
    run(home, "install")
    clone_version(home, vname("samelevel01"), installer.SAFETY_LEVEL)
    assert run(home, "activate", vname("samelevel01")).returncode == 0
    assert active_name(home) == vname("samelevel01")
    clone_version(home, vname("higherlvl01"), installer.SAFETY_LEVEL + 1)
    assert run(home, "activate", vname("higherlvl01")).returncode == 0
    assert current_target(home).endswith("/versions/" + vname("higherlvl01"))


@pytest.mark.parametrize("damage", ["bin", "src", "skill"])
def test_activate_refuses_an_incomplete_tree(home, damage):
    run(home, "install")
    before = current_target(home)
    clone_version(home, vname("broken0001"), 2, drop=(damage,))
    result = run(home, "activate", vname("broken0001"), check=False)
    assert result.returncode == 1
    assert damage in result.stderr
    assert current_target(home) == before


def test_activate_refuses_a_name_that_does_not_match_its_tree(home):
    run(home, "install")
    before = current_target(home)
    clone_version(home, vname("renamed001"), 2, version_text=vname("other00001"))
    result = run(home, "activate", vname("renamed001"), check=False)
    assert result.returncode == 1
    assert "does not match" in result.stderr
    # the package version inside the tree must match the name's prefix too
    clone_version(home, "9.9.9-wrongpkg01", 2)
    result = run(home, "activate", "9.9.9-wrongpkg01", check=False)
    assert result.returncode == 1
    assert "does not match" in result.stderr
    assert current_target(home) == before


def test_activate_refuses_paths_outside_versions(home):
    run(home, "install")
    for name in ("../versions", ".hidden", "a/b", ""):
        assert run(home, "activate", name, check=False).returncode == 1


def test_activate_has_no_force_option(home):
    run(home, "install")
    result = run(home, "activate", "--force", active_name(home), check=False)
    assert result.returncode == 2               # argparse: unknown option
    assert "unrecognized arguments" in result.stderr or \
        "--force" in result.stderr


def test_install_refuses_to_move_current_below_the_active_level(home):
    run(home, "install")
    clone_version(home, vname("future00002"), installer.SAFETY_LEVEL + 1)
    run(home, "activate", vname("future00002"))
    result = run(home, "install", check=False)
    assert result.returncode == 1
    assert "roll forward instead" in result.stderr
    assert active_name(home) == vname("future00002")
    assert current_target(home).endswith("/versions/" + vname("future00002"))


# --- legacy levels: dirs installed before the floor (never edited) ---------

def test_legacy_levels_table():
    assert installer.LEGACY_LEVELS == {"0.1.0-edfd7268fe": 2,
                                       "0.1.0-eb369eb937": 1}


def test_the_unlevelled_edfd726_is_read_by_its_legacy_level_and_refused_by_the_s4_floor(home):
    """Level 2 (legacy, by name) was the last rollback target below the
    first floored versions; the S4 floor (3) refuses it too."""
    run(home, "install")              # active level 3
    before = current_target(home)
    target = clone_version(home, "0.1.0-edfd7268fe", None)
    assert not (target / "SAFETY").exists()            # the dir is not edited
    result = run(home, "activate", "0.1.0-edfd7268fe", check=False)
    assert result.returncode == 1
    assert "roll forward instead" in result.stderr
    assert current_target(home) == before
    assert not (target / "SAFETY").exists()
    status = json.loads(run(home, "status").stdout)
    assert status["safety_levels"]["0.1.0-edfd7268fe"] == 2


def test_legacy_level_counts_as_the_active_level(home):
    run(home, "install")
    clone_version(home, "0.1.0-edfd7268fe", None)
    # as an older (pre-floor) installer would leave it: active legacy 2
    prefix = home / ".local/share/agent-compute"
    (prefix / "current").unlink()
    (prefix / "current").symlink_to(prefix / "versions" / "0.1.0-edfd7268fe")
    clone_version(home, vname("levelone02"), 1)
    result = run(home, "activate", vname("levelone02"), check=False)
    assert result.returncode == 1
    assert current_target(home).endswith("/versions/0.1.0-edfd7268fe")


def test_legacy_edfd726_is_refused_below_a_higher_active_level(home):
    run(home, "install")
    clone_version(home, "0.1.0-edfd7268fe", None)
    clone_version(home, vname("future00003"), installer.SAFETY_LEVEL + 1)
    run(home, "activate", vname("future00003"))
    result = run(home, "activate", "0.1.0-edfd7268fe", check=False)
    assert result.returncode == 1
    assert "roll forward instead" in result.stderr


def test_legacy_eb369eb937_is_refused_by_the_floor(home):
    run(home, "install")
    before = current_target(home)
    clone_version(home, "0.1.0-eb369eb937", None)
    result = run(home, "activate", "0.1.0-eb369eb937", check=False)
    assert result.returncode == 1
    assert "below the floor" in result.stderr
    assert "roll forward instead" in result.stderr
    assert current_target(home) == before


def test_legacy_name_still_needs_a_valid_tree(home):
    run(home, "install")
    before = current_target(home)
    clone_version(home, "0.1.0-edfd7268fe", None, drop=("skill",))
    result = run(home, "activate", "0.1.0-edfd7268fe", check=False)
    assert result.returncode == 1
    assert "skill" in result.stderr
    assert current_target(home) == before


def test_legacy_name_must_match_its_tree(home):
    run(home, "install")
    before = current_target(home)
    clone_version(home, "0.1.0-edfd7268fe", None,
                  version_text="0.1.0-somethingelse")
    result = run(home, "activate", "0.1.0-edfd7268fe", check=False)
    assert result.returncode == 1
    assert "does not match" in result.stderr
    assert current_target(home) == before


# --- review of 090c01b ------------------------------------------------------

def test_dirty_install_gets_level_zero_and_is_never_activated(home, tmp_path):
    source = tmp_path / "source"
    init = source / "src/agent_compute/__init__.py"
    init.write_text(init.read_text() + "# a local, uncommitted edit\n")
    result = run(home, "install", "--allow-dirty", check=False)
    assert result.returncode == 1
    assert "below the floor" in result.stderr
    dirty = [d for d in (prefix_of(home) / "versions").iterdir()
             if d.name.endswith("-dirty")]
    assert len(dirty) == 1
    assert (dirty[0] / "SAFETY").read_text().strip() == "0"
    assert not (prefix_of(home) / "current").exists()
    # nor above an active clean version, nor by a later activate
    init.write_text(init.read_text().replace("# a local, uncommitted edit\n",
                                             ""))
    run(home, "install")
    before = current_target(home)
    assert run(home, "activate", dirty[0].name, check=False).returncode == 1
    assert current_target(home) == before


@pytest.mark.parametrize("text,level", [
    ("2", 2), ("2\n", 2), (" 3 \n", 3), ("0", 0),
    ("\u00b2", None),          # superscript two: isdigit() is True
    ("\u0663", None),          # Arabic-Indic three
    ("\uff12", None),          # fullwidth two
    ("-1", None), ("", None), ("2.0", None), ("two", None)])
def test_safety_file_parses_ascii_digits_only(tmp_path, text, level):
    version = tmp_path / "0.1.0-parsetest1"
    version.mkdir()
    (version / "SAFETY").write_text(text, encoding="utf-8")
    assert installer.safety_level(version) == level


def test_unicode_digit_level_is_refused_not_a_crash(home):
    run(home, "install")
    before = current_target(home)
    target = clone_version(home, vname("unicode001"), 2)
    (target / "SAFETY").write_text("\u00b2\n", encoding="utf-8")
    result = run(home, "activate", vname("unicode001"), check=False)
    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    assert current_target(home) == before


# --- slice 7 review, low item: install.py is in the dirty check -----------

def test_a_locally_edited_installer_is_a_dirty_install(home, tmp_path):
    source = tmp_path / "source"
    script = source / "install.py"
    script.write_text(script.read_text() + "# a local, uncommitted edit\n")
    result = run(home, "install", check=False)
    assert result.returncode != 0
    assert "uncommitted" in result.stderr + result.stdout
    assert not (prefix_of(home) / "versions").exists() or not any(
        (prefix_of(home) / "versions").iterdir())
    result = run(home, "install", "--allow-dirty", check=False)
    dirty = [d for d in (prefix_of(home) / "versions").iterdir()
             if d.name.endswith("-dirty")]
    assert len(dirty) == 1
    assert (dirty[0] / "SAFETY").read_text().strip() == "0"


def test_installed_version_names_itself_and_its_source(home):
    """G2a (D10): `--version` of an installed copy prints the version dir
    name and the full source sha, which install.py records in SOURCE."""
    run(home, "install")
    name = active_name(home)
    current = Path(os.path.realpath(home / ".local/share/agent-compute/current"))
    source = (current / "SOURCE").read_text().strip()
    assert len(source) == 40 and source.startswith(name.split("-")[1])
    out = subprocess.run([str(home / ".local/bin/agent-compute"),
                          "--version"], capture_output=True, text=True)
    assert out.returncode == 0
    assert out.stdout.split() == [name, source]
