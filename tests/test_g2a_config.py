"""Campaign G slice G2a: the machine config schema ``agent-compute-config/2``.

What this establishes (the private development record's G2a slice; the
coupling inventory, sections B, H and J):

* a v1 config is refused with the instruction to migrate;
* strict validation: an unknown key anywhere (top level, ``[backend]``,
  ``[queue]``, ``[hosts.*]``, ``[projects.*]``, ``[backend.profile]``) is a
  ``ConfigError`` (v1 ignored the first three and raised an uncaught
  ``TypeError`` for the fourth), and so is a value of the wrong type;
* ``host`` is required and validated (it names files: the block, the
  event log);
* ``guard_home`` is allowed in production; its default is
  ``<state_dir>/guard``; ``{state}`` may be used in it;
* ``marker_aliases`` (default ``[]``) and ``legacy_workload_env`` (default
  false) are validated;
* ``[hosts.*]`` gains ``connect_timeout_s``/``call_timeout_s``;
* ``backend.extra_env`` is screened like ``--env`` (credential-like names,
  guard control variables, the job markers);
* a persistent ``host_id`` is created in the state dir on first use and
  never replaced;
* a rename of ``host`` is refused (``doctor`` ERROR, ``submit`` refusal)
  while an unfinished job or a block exists under another name.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

from conftest import REPO, Env

from agent_compute import config as cfgmod
from agent_compute import state as statemod

PY = sys.executable
SHA = "a" * 64


def text(tmp: Path, *, top: str = "", backend: str = "", queue: str = "",
         hosts: str = "", projects: str | None = None,
         schema: str | None = None, host: str | None = "h1") -> str:
    schema = schema or cfgmod.CONFIG_SCHEMA
    lines = [f'schema = "{schema}"']
    if host is not None:
        lines.append(f'host = "{host}"')
    lines += [f'state_dir = "{tmp / "s"}"', top,
              "[backend]", 'kind = "acs-guard"', f'sha256 = "{SHA}"', backend,
              "[backend.profile]", 'name = "t"', "budget_gb = 1",
              "floor_gb = 1", "container_cpus = 1", 'runlog_tag = ""',
              "[queue]", "enabled = false", queue, hosts]
    lines.append(projects if projects is not None else
                 '[projects.p]\nroots = ["/x"]\nreserve_detector = "none"\n')
    return "\n".join(lines) + "\n"


def load_text(tmp: Path, body: str):
    path = tmp / "config.toml"
    path.write_text(body)
    return cfgmod.load(path)


def refused(tmp: Path, body: str) -> str:
    with pytest.raises(cfgmod.ConfigError) as info:
        load_text(tmp, body)
    return str(info.value)


# --- the schema --------------------------------------------------------------------

def test_the_schema_is_v2():
    assert cfgmod.CONFIG_SCHEMA == "agent-compute-config/2"


def test_a_v1_config_is_refused_with_the_migrate_instruction(tmp_path):
    message = refused(tmp_path, text(tmp_path,
                                     schema="agent-compute-config/1"))
    assert "agent-compute-config/1" in message
    assert "config migrate" in message and "--authorized-by" in message


def test_the_base_config_loads(tmp_path):
    config = load_text(tmp_path, text(tmp_path))
    assert config.host == "h1"


@pytest.mark.parametrize("where", ["top", "backend", "queue", "hosts",
                                   "projects", "profile", "extra-table"])
def test_an_unknown_key_anywhere_is_an_error(tmp_path, where):
    kwargs = {"top": {"top": "dedicated_budget = 48"},
              "backend": {"backend": "dedicated_budget = 48"},
              "queue": {"queue": "paralel = 2"},
              "hosts": {"hosts": '[hosts.r]\ntransport = "ssh"\nssh = "r"\n'
                                 'agent_compute = "/x"\nconect_timeout = 3'},
              "projects": {"projects": '[projects.p]\nroots = ["/x"]\n'
                                       'reserve_detector = "none"\n'
                                       'max_thread = 4\n'},
              "profile": {"backend": ""},
              "extra-table": {"top": ""}}[where]
    body = text(tmp_path, **kwargs)
    if where == "profile":
        body = body.replace("container_cpus = 1", "container_cpus = 1\n"
                            "flor_gb = 2")
    if where == "extra-table":
        body += "[machine]\nname = \"x\"\n"
    message = refused(tmp_path, body)
    assert "unknown" in message, message


@pytest.mark.parametrize("bad", [
    ("top", "test_mode = \"yes\""),
    ("top", "python = 3"),
    ("backend", "lock_wait_max_s = \"long\""),
    ("backend", "admission_retry_s = -1"),
    ("queue", "group = \"two words\""),
    ("projects", '[projects.p]\nroots = ["/x"]\nreserve_detector = "none"\n'
                 'max_threads = "4"\n'),
    ("projects", '[projects.p]\nroots = "/x"\nreserve_detector = "none"\n'),
    ("projects", '[projects.p]\nroots = ["/x"]\nreserve_detector = "none"\n'
                 'env_passthrough = "X"\n'),
    ("projects", '[projects.p]\nroots = ["/x"]\nreserve_detector = "none"\n'
                 'containers = [1]\n'),
    ("hosts", '[hosts.r]\ntransport = "telnet"\nssh = "r"\n'
              'agent_compute = "/x"\n'),
    ("hosts", '[hosts.r]\ntransport = "ssh"\nagent_compute = "/x"\n'),
])
def test_values_of_the_wrong_type_are_errors(tmp_path, bad):
    where, line = bad
    refused(tmp_path, text(tmp_path, **{where: line}))


@pytest.mark.parametrize("host", ["", "two words", "../up", ".hidden",
                                  "a/b", "x" * 65, None])
def test_host_is_required_and_validated(tmp_path, host):
    message = refused(tmp_path, text(tmp_path, host=host))
    assert "host" in message


@pytest.mark.parametrize("host", ["mac", "lenovo", "build-01", "h.lab"])
def test_valid_host_names(tmp_path, host):
    assert load_text(tmp_path, text(tmp_path, host=host)).host == host


# --- guard home and markers ----------------------------------------------------------

def test_guard_home_defaults_to_the_state_dir(tmp_path):
    config = load_text(tmp_path, text(tmp_path))
    assert config.guard_home is None
    assert config.effective_guard_home == tmp_path / "s" / "guard"


def test_guard_home_may_be_configured_in_production(tmp_path):
    config = load_text(tmp_path, text(
        tmp_path, backend=f'guard_home = "{tmp_path / "legacy-home"}"'))
    assert not config.test_mode
    assert config.effective_guard_home == tmp_path / "legacy-home"
    config = load_text(tmp_path, text(
        tmp_path, backend='guard_home = "{state}/other"'))
    assert config.effective_guard_home == tmp_path / "s" / "other"


def test_guard_home_must_be_absolute(tmp_path):
    message = refused(tmp_path, text(tmp_path,
                                     backend='guard_home = "rel/home"'))
    assert "guard_home" in message


def test_marker_aliases_default_and_validation(tmp_path):
    config = load_text(tmp_path, text(tmp_path))
    assert config.marker_aliases == [] and config.legacy_workload_env is False
    assert config.markers == ["ACS_JOB_MARKER"]
    config = load_text(tmp_path, text(
        tmp_path, backend='marker_aliases = ["SMC_GUARD_JOB"]\n'
                          'legacy_workload_env = true'))
    assert config.marker_aliases == ["SMC_GUARD_JOB"]
    assert config.markers == ["ACS_JOB_MARKER", "SMC_GUARD_JOB"]
    assert config.legacy_workload_env is True


@pytest.mark.parametrize("aliases", [
    '["ACS_JOB_MARKER"]', '["ACS_JOB_ID"]', '["lower_case"]', '["PATH"]',
    '["MY_TOKEN"]', '["X", "X"]', '["ACS_GUARD_X"]', '"SMC_GUARD_JOB"',
    '["OMP_NUM_THREADS"]', '["ACS_CASE"]', '[""]'])
def test_bad_marker_aliases_are_refused(tmp_path, aliases):
    message = refused(tmp_path, text(tmp_path,
                                     backend=f"marker_aliases = {aliases}"))
    assert "marker_aliases" in message


# --- remote routes ------------------------------------------------------------------

def test_host_routes_take_timeouts(tmp_path):
    config = load_text(tmp_path, text(
        tmp_path, hosts='[hosts.r]\ntransport = "ssh-wsl"\nssh = "r"\n'
                        'distro = "d"\nagent_compute = "/x"\n'
                        'connect_timeout_s = 10\ncall_timeout_s = 120\n'))
    remote = config.remotes["r"]
    assert (remote.connect_timeout_s, remote.call_timeout_s) == (10.0, 120.0)
    config = load_text(tmp_path, text(
        tmp_path, hosts='[hosts.r]\ntransport = "ssh"\nssh = "r"\n'
                        'agent_compute = "/x"\n'))
    assert config.remotes["r"].connect_timeout_s is None
    for bad in ("0", "-3", '"10"'):
        refused(tmp_path, text(
            tmp_path, hosts='[hosts.r]\ntransport = "ssh"\nssh = "r"\n'
                            f'agent_compute = "/x"\nconnect_timeout_s = {bad}'
                            '\n'))


# --- extra_env is screened like --env ------------------------------------------------

@pytest.mark.parametrize("name", ["API_TOKEN", "MY_PASSWORD", "SMC_GUARD_JOB",
                                  "ACS_JOB_ID", "ACS_JOB_MARKER",
                                  "ACS_GUARD_HOME", "ACS_GUARD_TEST_UNCLAIMED",
                                  "SMC_GUARD_HOME", "ACS_CASE",
                                  "SMC_GUARD_FREE_MEM_FILE"])
def test_extra_env_is_screened_like_env(tmp_path, name):
    message = refused(tmp_path, text(
        tmp_path, backend=f'[backend.extra_env]\n{name} = "x"'))
    assert name in message


def test_extra_env_refuses_a_configured_alias_and_non_scalars(tmp_path):
    refused(tmp_path, text(tmp_path, backend='marker_aliases = ["MY_JOB"]\n'
                                             '[backend.extra_env]\n'
                                             'MY_JOB = "x"'))
    refused(tmp_path, text(tmp_path, backend='[backend.extra_env]\n'
                                             'GOOD = ["a", "b"]'))
    config = load_text(tmp_path, text(tmp_path, backend='[backend.extra_env]\n'
                                                        'GOOD = "a"'))
    assert config.guard_extra_env == {"GOOD": "a"}


def test_test_hooks_only_in_test_mode(tmp_path):
    body = text(tmp_path, top="test_mode = true",
                backend='[backend.extra_env]\nSMC_GUARD_FREE_MEM_FILE = "/f"')
    assert load_text(tmp_path, body).guard_extra_env == {
        "SMC_GUARD_FREE_MEM_FILE": "/f"}


@pytest.mark.parametrize("name", ["API_TOKEN", "SMC_GUARD_JOB",
                                  "ACS_JOB_MARKER", "ACS_GUARD_HOME",
                                  "ACS_JOB_ID"])
def test_env_passthrough_refuses_control_variables(tmp_path, name):
    refused(tmp_path, text(tmp_path, projects=(
        f'[projects.p]\nroots = ["/x"]\nreserve_detector = "none"\n'
        f'env_passthrough = ["{name}"]\n')))


# --- host identity -------------------------------------------------------------------

def test_host_id_is_created_once_and_never_replaced(tmp_path):
    state = tmp_path / "s"
    first = statemod.ensure_host_id(state, "h1")
    assert isinstance(first, dict) and len(first["host_id"]) >= 32
    assert first["schema"] == "agent-compute-host-id/1"
    path = state / "host-id.json"
    assert oct(path.stat().st_mode & 0o777) == "0o600"
    again = statemod.ensure_host_id(state, "renamed")
    assert again["host_id"] == first["host_id"]
    assert statemod.read_host_id(state)["host_id"] == first["host_id"]
    # an unreadable record is reported, never replaced
    path.write_text("{trunc")
    bad = statemod.ensure_host_id(state, "h1")
    assert isinstance(bad, statemod.Unreadable)
    assert path.read_text() == "{trunc"


# --- a host rename while anything is recorded under the old name -------------------------

def _env_config(env: Env, host: str) -> None:
    text_ = env.config.read_text()
    env.config.write_text(text_.replace('host = "testhost"',
                                        f'host = "{host}"'))


def test_a_rename_with_an_unfinished_job_is_refused(env_direct):
    env = env_direct
    job = env.submit(workload_args=["--out", str(env.root / "o"),
                                    "--sleep", "30"])
    env.wait_phase(job["job_id"], ("executing",))
    _env_config(env, "renamed")
    try:
        doctor = env.acs("doctor", "--json")
        report = json.loads(doctor.stdout)
        check = [c for c in report["checks"]
                 if c["check"] == "host label unchanged under recorded work"]
        assert check and check[0]["ok"] is False, report["checks"]
        assert job["job_id"] in json.dumps(check[0]["detail"])
        sub = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                      "--reserve-gb", "0.5", "--cwd",
                      str(env.projects["alpha"]), "--", PY, "-c", "pass")
        assert sub.returncode == 2
        assert "renamed" in sub.stdout and "testhost" in sub.stdout
    finally:
        _env_config_back(env)
        env.acs("cancel", job["job_id"])


def _env_config_back(env: Env) -> None:
    text_ = env.config.read_text()
    env.config.write_text(text_.replace('host = "renamed"',
                                        'host = "testhost"'))


def test_a_rename_with_a_block_under_the_old_name_is_refused(env_direct):
    env = env_direct
    statemod.private_dir(env.state / "blocks")
    statemod.write_json(env.state / "blocks" / "testhost.json",
                        {"host": "testhost", "jobs": {"acs-x": {}}})
    _env_config(env, "renamed")
    report = json.loads(env.acs("doctor", "--json").stdout)
    check = [c for c in report["checks"]
             if c["check"] == "host label unchanged under recorded work"]
    assert check and check[0]["ok"] is False
    assert "testhost" in json.dumps(check[0]["detail"])


def test_a_rename_with_nothing_recorded_is_allowed(env_direct):
    env = env_direct
    done = env.submit(workload_args=["--out", str(env.root / "o")])
    env.wait_outcome(done["job_id"])
    _env_config(env, "renamed")
    report = json.loads(env.acs("doctor", "--json").stdout)
    check = [c for c in report["checks"]
             if c["check"] == "host label unchanged under recorded work"]
    assert check and check[0]["ok"] is True, check
    job = env.submit(workload_args=["--out", str(env.root / "o2")])
    assert env.wait_outcome(job["job_id"])["outcome"] == "succeeded"


# --- a guard home changes only at an idle checkpoint (D2) ----------------------------------

def _set_home(env: Env, home) -> None:
    import re as _re
    text_ = env.config.read_text()
    env.config.write_text(_re.sub(r'guard_home = "[^"]*"',
                                  f'guard_home = "{home}"', text_, count=1))


def test_a_guard_home_change_with_an_unfinished_job_is_refused(env_direct):
    env = env_direct
    job = env.submit(workload_args=["--out", str(env.root / "o"),
                                    "--sleep", "30"])
    env.wait_phase(job["job_id"], ("executing",))
    _set_home(env, env.root / "new-home")
    try:
        report = json.loads(env.acs("doctor", "--json").stdout)
        check = [c for c in report["checks"]
                 if c["check"] == "guard home unchanged under recorded work"]
        assert check and check[0]["ok"] is False, report["checks"]
        assert job["job_id"] in json.dumps(check[0]["detail"])
        sub = env.acs("submit", "--json", "--case", "t", "--timeout-min",
                      "1", "--reserve-gb", "0.5", "--cwd",
                      str(env.projects["alpha"]), "--", PY, "-c", "pass")
        assert sub.returncode == 2 and "guard home" in sub.stdout
    finally:
        _set_home(env, env.guard_home)
        env.acs("cancel", job["job_id"])


def test_a_guard_home_change_with_a_marker_in_the_old_home_is_refused(
        env_direct):
    env = env_direct
    done = env.submit(workload_args=["--out", str(env.root / "o")])
    env.wait_outcome(done["job_id"])
    assert json.loads((env.state / "guard-home.json").read_text())[
        "guard_home"] == str(env.guard_home)
    (env.guard_home / "inflight.json").write_text("{}")
    _set_home(env, env.root / "new-home")
    report = json.loads(env.acs("doctor", "--json").stdout)
    check = [c for c in report["checks"]
             if c["check"] == "guard home unchanged under recorded work"]
    assert check and check[0]["ok"] is False
    assert "inflight.json" in json.dumps(check[0]["detail"])
    # review round 1: submit refuses on the same evidence, nothing written
    sub = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                  "--reserve-gb", "0.5", "--cwd", str(env.projects["alpha"]),
                  "--", PY, "-c", "pass")
    assert sub.returncode == 2, sub.stdout
    assert "guard home" in sub.stdout and "inflight.json" in sub.stdout
    (env.guard_home / "inflight.json").unlink()
    report = json.loads(env.acs("doctor", "--json").stdout)
    check = [c for c in report["checks"]
             if c["check"] == "guard home unchanged under recorded work"]
    assert check[0]["ok"] is True, check
    job = env.submit(workload_args=["--out", str(env.root / "o2")])
    assert env.wait_outcome(job["job_id"])["outcome"] == "succeeded"
    assert json.loads((env.state / "guard-home.json").read_text())[
        "guard_home"] == str(env.root / "new-home")
    assert (env.root / "new-home" / "lock").exists()


# --- the examples load -------------------------------------------------------------------------

# The shipped example, and the synthetic v2 configs that a v1 sample becomes
# under ``config migrate`` (tests/fixtures/config-v2: a dedicated workstation
# with a 48 GB host budget and a small WSL2 sidecar of 10 GB).
V2_EXAMPLES = {
    "config.example.toml": (REPO / "examples" / "config.example.toml", 16.0),
    "sample-a.toml": (REPO / "tests" / "fixtures" / "config-v2" /
                      "sample-a.toml", 64.0),
    "sample-b.toml": (REPO / "tests" / "fixtures" / "config-v2" /
                      "sample-b.toml", 10.0),
}


@pytest.mark.parametrize("name", list(V2_EXAMPLES))
def test_the_examples_are_v2_and_load(tmp_path, name, monkeypatch):
    # G2b: the budget cap is the loading host's RAM; the samples stand for
    # two machines (64 GB, 12 cores; a 10 GB VM)
    path, memory = V2_EXAMPLES[name]
    monkeypatch.setattr(cfgmod, "host_resources", lambda: {
        "memory_gb": memory, "cores": 8, "source": "test"})
    config = load_text(tmp_path, path.read_text())
    assert config.raw["schema"] == "agent-compute-config/2"
    if name == "config.example.toml":
        assert config.guard_home is None
        assert config.markers == ["ACS_JOB_MARKER"]
        assert not config.legacy_workload_env
        text_ = path.read_text()
        assert "smc" not in text_.lower() .replace("smc-ladder", "")
    else:
        assert str(config.effective_guard_home).endswith("/.smc-guard")


def test_acs_guard_without_a_profile_is_refused(tmp_path):
    body = text(tmp_path)
    start = body.index("[backend.profile]")
    end = body.index("[queue]")
    message = refused(tmp_path, body[:start] + body[end:])
    assert "needs the machine profile" in message


def test_concurrent_first_uses_create_one_host_id(tmp_path):
    """Threads of one process (and processes) racing to create the host id
    agree on one record (a race the Lenovo gate found in a first version:
    a shared temp name)."""
    import threading
    state = tmp_path / "s"
    found, errors = [], []

    def one():
        try:
            found.append(statemod.ensure_host_id(state, "h1")["host_id"])
        except Exception as error:  # noqa: BLE001
            errors.append(error)
    threads = [threading.Thread(target=one) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors, errors
    assert len(set(found)) == 1 and len(found) == 16
    assert not [p for p in state.iterdir() if p.name.endswith(".tmp")]
