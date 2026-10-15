"""Campaign G slice G2a: ``agent-compute config migrate`` (decision D8).

The migration rewrites a v1 config to ``agent-compute-config/2`` with the
**same effective values**.  "Before" is computed by the code that runs on
the hosts today: ``src/`` of ``eb84c0e`` (``v0.1.2``, deployed as
``0.1.2-eb84c0e791`` on both hosts), loading the v1 file in a subprocess.
"After" is this tree's ``load`` of the migrated file.  Both run with
``HOME`` set to a temporary directory, so every ``~`` path (the state dir,
the guard home) expands there and nothing of the real installation is
read or written.

Inputs: ``tests/fixtures/config-v1/sample-{a,b}.toml``, synthetic v1
configs with neutral names that keep every structural case of the two live
configs the migration was first written against (a dedicated workstation with
a declared host budget, a second host over ssh-wsl, three projects with and
without in-tree records, containers and detectors; a small sidecar with one
project and a fractional floor).  The private tree checks that structural
equivalence against the live files.

Allowed differences, by design and nothing else: the workload-visible
names may only grow (``markers``: ``ACS_JOB_MARKER`` beside the kept
``SMC_GUARD_JOB``; ``workload_env`` and ``slice_env``: the ``ACS_*`` names
beside the kept legacy ones).  Every other value (host, state dir, python,
backend, pin, profile, dedicated budget, guard home, extra env, admission
timings, queue, routes, every project field incl. runlog/baseline paths
and detectors) must be equal.

G2b: the external guard.py backend (``smc-guard``) is retired, so the two
v1 examples (``kind = "smc-guard"``) are no longer migrated (refused with
the reason: ``tests/test_g2b_policy.py``) and ``backend.guard`` /
``guard_profile`` (without effect under ``acs-guard``) are no effective
values; ``projects.*.records_in_tree`` is (true where v1 wrote the records
into the tree, kept by the migration as the explicit opt-in).
The budget cap is the loading host's RAM since G2b; the sandbox fakes a
64 GB host (``ACS_TEST_HOST_MEMORY_GB``, honoured only from a checkout) so
the Mac's config migrates on the Lenovo too.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from conftest import BIN, REPO, Env, QuietEnv, skip_reason

from agent_compute import config as cfgmod

PY = sys.executable
FIXTURES = REPO / "tests" / "fixtures" / "config-v1"
NAMES = ["sample-a", "sample-b"]
OLD = "eb84c0e"     # v0.1.2, deployed on both hosts (0.1.2-eb84c0e791)
GROWS = {"markers", "workload_env", "slice_env"}
AUTH = "operator test, 2026-10-06, migrate the test config"

# The v0.1.2 Config, mapped to the effective-value keys.  This mapping is
# the test's own statement of what v1 meant (the engine of v0.1.2 exported
# SMC_GUARD_JOB, SMC_GUARD_CASE, SMC_GUARD_WORKERS and SMC_DENSE_BLAS_THREADS
# and read SMC_SLICE; guard.py does the same).
OLD_EFFECTIVE = r"""
import dataclasses, fnmatch, json, os, sys
from pathlib import Path
from agent_compute import config as c
cfg = c.load(sys.argv[1])

def in_tree(template, p):
    # v1 wrote a record into the tree when its path named {worktree}, was
    # relative (resolved against the worktree) or lay under a root
    if "{worktree}" in template:
        return True
    path = c.expand(template.format(state=cfg.state_dir, project=p.name,
                                    host=cfg.host, worktree="/w"))
    if not path.is_absolute():
        return True
    real = Path(os.path.realpath(path))
    roots = [os.path.realpath(c.expand(r)) for r in p.roots]
    return any(fnmatch.fnmatch(str(x), r) for x in (real, *real.parents)
               for r in roots)

out = {"host": cfg.host, "state_dir": str(cfg.state_dir),
       "python": cfg.python, "job_path": cfg.job_path,
       "test_mode": cfg.test_mode,
       "backend.kind": cfg.backend_kind,
       "backend.sha256": cfg.guard_sha256,
       "backend.profile": cfg.profile,
       "backend.dedicated_budget_gb": cfg.dedicated_budget_gb,
       "backend.guard_home": str(cfg.effective_guard_home),
       "backend.extra_env": cfg.guard_extra_env,
       "backend.lock_wait_max_s": cfg.lock_wait_max_s,
       "backend.admission_retry_s": cfg.admission_retry_s,
       "backend.admission_retry_max_s": cfg.admission_retry_max_s,
       "markers": ["SMC_GUARD_JOB"],
       "workload_env": ["SMC_DENSE_BLAS_THREADS", "SMC_GUARD_CASE",
                        "SMC_GUARD_WORKERS"],
       "slice_env": ["SMC_SLICE"],
       "queue.enabled": cfg.queue_enabled, "queue.pueue": cfg.pueue,
       "queue.pueued": cfg.pueued, "queue.group": cfg.queue_group}
for name, r in cfg.remotes.items():
    for key in ("transport", "ssh", "agent_compute", "distro"):
        out[f"hosts.{name}.{key}"] = getattr(r, key)
for name, p in cfg.projects.items():
    for f in dataclasses.fields(p):
        if f.name != "name":
            out[f"projects.{name}.{f.name}"] = getattr(p, f.name)
    out[f"projects.{name}.records_in_tree"] = \
        in_tree(p.runlog, p) or in_tree(p.baseline, p)
print(json.dumps(out, sort_keys=True))
"""


@pytest.fixture(scope="module")
def old_src(tmp_path_factory):
    if subprocess.run(["git", "-C", str(REPO), "cat-file", "-e",
                       f"{OLD}^{{commit}}"], capture_output=True).returncode:
        pytest.skip(skip_reason("clone", f"{OLD} is not in this clone"))
    root = tmp_path_factory.mktemp("old")
    archive = subprocess.run(["git", "-C", str(REPO), "archive", OLD, "bin",
                              "src"], capture_output=True, check=True)
    subprocess.run(["tar", "-x", "-C", str(root)], input=archive.stdout,
                   check=True)
    return root


def sandbox(tmp_path: Path, name: str) -> tuple[Path, dict]:
    home = tmp_path / "home"
    (home / ".config" / "agent-compute").mkdir(parents=True)
    config = home / ".config" / "agent-compute" / "config.toml"
    shutil.copy(FIXTURES / f"{name}.toml", config)
    config.chmod(0o600)
    env = QuietEnv((k, v) for k, v in os.environ.items()
                   if not k.startswith(("SMC_GUARD_", "ACS_")) and
                   k != "AGENT_COMPUTE_CONFIG")
    env["HOME"] = str(home)
    # G2b: the Mac's 48 GB dedicated budget loads only on a host with at
    # least that much RAM; fake the Mac's 64 GB on every test host
    env.update(ACS_TEST_HOST_MEMORY_GB="64", ACS_TEST_HOST_CORES="12")
    return config, env


def old_effective(old_src: Path, config: Path, env: dict) -> dict:
    result = subprocess.run([PY, "-c", OLD_EFFECTIVE, str(config)],
                            capture_output=True, text=True, timeout=60,
                            env=QuietEnv({**env, "PYTHONPATH":
                                          str(old_src / "src")}))
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def new_effective(config: Path, env: dict) -> dict:
    script = ("import json, sys\nfrom agent_compute import config as c\n"
              "print(json.dumps(c.effective_values(c.load(sys.argv[1])), "
              "sort_keys=True, default=str))")
    result = subprocess.run([PY, "-c", script, str(config)],
                            capture_output=True, text=True, timeout=60,
                            env=QuietEnv({**env, "PYTHONPATH": str(REPO / "src")}))
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def migrate(config: Path, env: dict, *extra, auth=AUTH):
    args = [PY, str(BIN), "--config", str(config), "config", "migrate",
            *extra]
    if auth is not None:
        args += ["--authorized-by", auth]
    return subprocess.run(args, capture_output=True, text=True, timeout=120,
                          env=env)


@pytest.mark.parametrize("name", NAMES)
def test_migration_keeps_every_effective_value(tmp_path, old_src, name):
    config, env = sandbox(tmp_path, name)
    before = old_effective(old_src, config, env)
    original = config.read_text()
    result = migrate(config, env, "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["migrated"] is True
    raw = tomllib.loads(config.read_text())
    assert raw["schema"] == "agent-compute-config/2"
    after = new_effective(config, env)
    # the production "before" reader agrees with v0.1.2 itself
    script = ("import json, sys, tomllib\nfrom agent_compute import config "
              "as c\nraw = tomllib.loads(open(sys.argv[1]).read())\n"
              "print(json.dumps(c.v1_effective_values(raw), sort_keys=True, "
              "default=str))")
    backup = Path(report["backup"])
    assert backup.read_text() == original
    mine = subprocess.run([PY, "-c", script, str(backup)],
                          capture_output=True, text=True, timeout=60,
                          env=QuietEnv({**env, "PYTHONPATH": str(REPO / "src")}))
    assert mine.returncode == 0, mine.stderr
    assert json.loads(mine.stdout) == before
    # every value equal, except the names that may only grow
    assert set(after) >= set(before), set(before) - set(after)
    differ = {k for k in before if before[k] != after[k]}
    assert differ <= GROWS, {k: (before[k], after[k]) for k in differ - GROWS}
    for key in differ:
        assert set(before[key]) < set(after[key]), (key, before[key],
                                                    after[key])
    extra = set(after) - set(before)
    assert all(after[k] is None for k in extra if k.startswith("hosts.")), \
        {k: after[k] for k in extra}
    # the comparison the command printed says the same
    rows = {row["key"]: row for row in report["effective"]}
    assert {k for k, r in rows.items()
            if r["verdict"] not in ("same", "added")} == differ
    assert all(r["after"] is None for r in rows.values()
               if r["verdict"] == "added")
    assert all(rows[k]["verdict"] == "superset" for k in differ)
    # the operator's legacy names are kept by configuration
    if before["backend.kind"] == "acs-guard":
        assert after["markers"] == ["ACS_JOB_MARKER", "SMC_GUARD_JOB"]
        assert raw["backend"]["marker_aliases"] == ["SMC_GUARD_JOB"]
        assert raw["backend"]["legacy_workload_env"] is True
    assert raw["backend"]["guard_home"] == "~/.smc-guard"
    assert after["backend.guard_home"] == str(tmp_path / "home" /
                                              ".smc-guard")
    for project in raw["projects"].values():
        assert "runlog" in project and "baseline" in project


@pytest.mark.parametrize("name", ["sample-a", "sample-b"])
def test_migration_prints_the_diff_keeps_a_backup_and_records_it(tmp_path,
                                                                 name):
    config, env = sandbox(tmp_path, name)
    original = config.read_text()
    result = migrate(config, env)
    assert result.returncode == 0, result.stdout + result.stderr
    out = result.stdout
    assert '-schema = "agent-compute-config/1"' in out
    assert '+schema = "agent-compute-config/2"' in out
    assert '+guard_home = "~/.smc-guard"' in out
    assert "backup" in out
    backups = list(config.parent.glob("config.toml.pre-v2-*"))
    assert len(backups) == 1 and backups[0].read_text() == original
    assert oct(backups[0].stat().st_mode & 0o777) == "0o600"
    assert oct(config.stat().st_mode & 0o777) == "0o600"
    state = tmp_path / "home" / ".local" / "state" / "agent-compute"
    lines = (state / "authorizations.jsonl").read_text().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["schema"] == "agent-compute-authorization/1"
    assert record["action"] == "config-migrate"
    assert record["authorized_by"] == AUTH
    assert record["config"] == str(config)
    assert record["backup"] == str(backups[0])
    assert record["from_schema"] == "agent-compute-config/1"
    assert record["to_schema"] == "agent-compute-config/2"
    assert record["version"] and "source_sha" in record
    assert record["host"] in ("alpha", "beta") and record["host_id"]
    assert record["diff_summary"]["added"]
    assert "guard_home" in json.dumps(record["diff_summary"])
    # a second run has nothing to do and records nothing
    again = migrate(config, env)
    assert again.returncode == 0 and "nothing to do" in again.stdout
    assert len((state / "authorizations.jsonl").read_text().splitlines()) \
        == 1


def test_migration_needs_an_authorization(tmp_path):
    config, env = sandbox(tmp_path, "sample-b")
    original = config.read_text()
    for auth in (None, "", "   ", "two\nlines"):
        result = migrate(config, env, auth=auth)
        assert result.returncode != 0
        assert config.read_text() == original
    assert not list(config.parent.glob("*.pre-v2-*"))


@pytest.mark.parametrize("damage", ["unfinished", "unreadable"])
def test_migration_refuses_while_a_job_is_unfinished(tmp_path, damage):
    config, env = sandbox(tmp_path, "sample-b")
    original = config.read_text()
    job = (tmp_path / "home" / ".local" / "state" / "agent-compute" / "jobs"
           / "acs-20261006T000000Z-abcdef")
    job.mkdir(parents=True)
    (job / "state.json").write_text(
        json.dumps({"job_id": job.name, "phase": "queued"})
        if damage == "unfinished" else "{trunc")
    result = migrate(config, env)
    assert result.returncode == 1, result.stdout
    assert "unfinished" in result.stderr and job.name in result.stderr
    assert config.read_text() == original
    assert not list(config.parent.glob("*.pre-v2-*"))


def test_migration_refuses_keys_v1_silently_ignored(tmp_path):
    config, env = sandbox(tmp_path, "sample-b")
    config.write_text(config.read_text().replace(
        "admission_retry_s = 120", "admission_retry_s = 120\n"
        "dedicated_budget = 48"))
    original = config.read_text()
    result = migrate(config, env)
    assert result.returncode == 1
    assert "dedicated_budget" in result.stderr
    assert config.read_text() == original


def test_migration_dry_run_writes_nothing(tmp_path):
    config, env = sandbox(tmp_path, "sample-a")
    original = config.read_text()
    result = migrate(config, env, "--dry-run")
    assert result.returncode == 0, result.stderr
    assert '+schema = "agent-compute-config/2"' in result.stdout
    assert config.read_text() == original
    assert not list(config.parent.glob("*.pre-v2-*"))
    state = tmp_path / "home" / ".local" / "state" / "agent-compute"
    assert not (state / "authorizations.jsonl").exists()


@pytest.mark.parametrize("name", ["sample-a", "sample-b"])
def test_doctor_prints_the_migration_side_by_side(tmp_path, name):
    config, env = sandbox(tmp_path, name)
    assert migrate(config, env).returncode == 0
    result = subprocess.run([PY, str(BIN), "--config", str(config), "doctor",
                             "--json"], capture_output=True, text=True,
                            timeout=180, env=env)
    report = json.loads(result.stdout)
    check = [c for c in report["checks"]
             if c["check"] == "config migration kept the effective values"]
    assert check and check[0]["ok"] is True, report["checks"]
    rows = check[0]["detail"]["values"]
    by_key = {r["key"]: r for r in rows}
    assert by_key["backend.guard_home"]["before"] == \
        by_key["backend.guard_home"]["after"]
    assert by_key["markers"]["verdict"] == "superset"
    assert check[0]["detail"]["backup"].startswith(str(config))


def test_a_v2_cli_refuses_a_v1_config_with_the_instruction(tmp_path):
    config, env = sandbox(tmp_path, "sample-b")
    for args in (["doctor"], ["status"]):
        result = subprocess.run([PY, str(BIN), "--config", str(config),
                                 *args], capture_output=True, text=True,
                                timeout=60, env=env)
        assert result.returncode == 2
        assert "config migrate" in result.stderr


# --- a pre-0.2.0 version under a v2 config ---------------------------------------------

@pytest.fixture
def old_version(old_src):
    return old_src / "bin" / "agent-compute"


def test_an_older_version_under_a_v2_config_fails_closed(old_version):
    """What v0.1.2 (both hosts today) does once the config is v2: every
    command, and the runner of a job it had queued, stops at config load,
    before any claim, guard or workload."""
    from test_acs_guard_backend import exec_runner, prepare_job
    env = Env(queue=False)
    try:
        for args in (["doctor"], ["status"],
                     ["submit", "--case", "t", "--timeout-min", "1",
                      "--cwd", str(env.projects["alpha"]), "--", "true"]):
            result = subprocess.run([PY, str(old_version), "--config",
                                     str(env.config), *args],
                                    capture_output=True, text=True,
                                    env=env.env(), timeout=60)
            assert result.returncode == 2, (args, result.stderr)
            assert "schema must be 'agent-compute-config/1'" in \
                result.stderr
        job = prepare_job(env, cfgmod.load(env.config))
        result = exec_runner(env, job, bin_path=old_version)
        assert result.returncode == 2
        assert "schema must be" in result.stderr
        state = env.state_of(job)
        assert "runner" not in state and not state.get("outcome")
        assert not (env.root / f"{job}.out").exists()
        assert not (env.guard_home / "lock").exists()
    finally:
        env.cleanup()


def test_moving_off_the_legacy_home_with_a_marker_left_is_refused(tmp_path):
    """D2: a host that leaves ~/.smc-guard (the home of every version before
    0.2.0) while it still holds an in-flight marker is refused by doctor
    and submit until the marker is resolved."""
    config, env = sandbox(tmp_path, "sample-b")
    assert migrate(config, env).returncode == 0
    config.write_text(config.read_text().replace(
        'guard_home = "~/.smc-guard"', 'guard_home = "{state}/guard"'))
    legacy = tmp_path / "home" / ".smc-guard"
    legacy.mkdir()
    (legacy / "inflight.json").write_text("{}")

    def check():
        result = subprocess.run([PY, str(BIN), "--config", str(config),
                                 "doctor", "--json"], capture_output=True,
                                text=True, timeout=180, env=env)
        report = json.loads(result.stdout)
        return [c for c in report["checks"]
                if c["check"] == "guard home unchanged under recorded work"][0]
    found = check()
    assert found["ok"] is False and "inflight.json" in json.dumps(found)
    (legacy / "inflight.json").unlink()
    assert check()["ok"] is True


# --- A42: doctor judges the migration, not a later authorized change -------------------
#
# Observed on the Lenovo on 2026-10-09 (the v0.2.0 migration): after `config
# migrate` and the `pin-backend` that must follow it (the engine of 0.2.0 is
# not the one the v1 config pinned), doctor compared the live config with the
# pre-migration backup and failed "config migration kept the effective
# values" on `backend.sha256`.  The check now judges the migration: a key a
# later recorded, successful authorization of this config changed, to the
# value the config holds now, is reported under "changed_later"; any other
# difference (a hand edit, a failed pin, a pin recorded before the migration
# or for another config) is still a problem, and an unreadable record file or
# backup still fails the check.

CHECK = "config migration kept the effective values"
PIN_AUTH = "operator test, 2026-10-09, pin the 0.2.x engine after migrating"
ENGINE = REPO / "src" / "agent_compute" / "guard" / "engine.py"
V1_SHA = "d817fd4f02ac8c20b295e217915ffd338cc12952bfa1300209559816ba47b295"


def engine_sha() -> str:
    import hashlib
    return hashlib.sha256(ENGINE.read_bytes()).hexdigest()


def pin_evidence(tmp_path: Path, host: str, sha: str) -> Path:
    from conftest import gate_header, gate_text, git
    repo = tmp_path / "evidence-repo"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    path = repo / f"pytest-{host}.txt"
    path.write_text(gate_text(gate_header(host, sha)))
    git(repo, "add", path.name)
    git(repo, "commit", "-q", "-m", "evidence")
    return path


def pin(config: Path, env: dict, evidence: Path):
    return subprocess.run([PY, str(BIN), "--config", str(config),
                           "pin-backend", "--json", "--evidence",
                           str(evidence), "--authorized-by", PIN_AUTH],
                          capture_output=True, text=True, timeout=120,
                          env=env, stdin=subprocess.DEVNULL)


def migration_doctor_check(config: Path, env: dict) -> dict:
    result = subprocess.run([PY, str(BIN), "--config", str(config), "doctor",
                             "--json"], capture_output=True, text=True,
                            timeout=180, env=env)
    report = json.loads(result.stdout)
    found = [c for c in report["checks"] if c["check"] == CHECK]
    assert len(found) == 1, report["checks"]
    return found[0]


def auth_file(tmp_path: Path) -> Path:
    return (tmp_path / "home" / ".local" / "state" / "agent-compute" /
            "authorizations.jsonl")


def migrate_and_pin(tmp_path: Path, name: str) -> tuple[Path, dict, dict]:
    config, env = sandbox(tmp_path, name)
    assert V1_SHA in config.read_text() and engine_sha() != V1_SHA
    assert migrate(config, env).returncode == 0
    host = tomllib.loads(config.read_text())["host"]
    result = pin(config, env, pin_evidence(tmp_path, host, engine_sha()))
    assert result.returncode == 0, result.stdout + result.stderr
    assert tomllib.loads(config.read_text())["backend"]["sha256"] == \
        engine_sha()
    records = [json.loads(line) for line in
               auth_file(tmp_path).read_text().splitlines()]
    assert [r["action"] for r in records] == ["config-migrate", "pin-backend"]
    return config, env, records[1]


@pytest.mark.parametrize("name", NAMES)
def test_doctor_migration_check_passes_after_the_required_pin(tmp_path,
                                                              name):
    config, env, pinned = migrate_and_pin(tmp_path, name)
    found = migration_doctor_check(config, env)
    detail = found["detail"]
    assert found["ok"] is True, detail
    assert detail["problems"] == []
    later = detail["changed_later"]
    assert [entry["key"] for entry in later] == ["backend.sha256"]
    entry = later[0]
    assert entry["backup"] == V1_SHA and entry["now"] == engine_sha()
    assert entry["action"] == "pin-backend"
    assert entry["utc"] == pinned["utc"]
    assert entry["authorized_by"] == PIN_AUTH
    assert entry["note"] == (f"changed later by pin-backend at "
                             f"{pinned['utc']} (authorized)")
    rows = {row["key"]: row for row in detail["values"]}
    assert rows["backend.sha256"]["verdict"] == "changed-later"
    # the names that grow by design are still a superset, not a problem
    for key in GROWS:
        assert rows[key]["verdict"] == "superset", rows[key]
    assert not [r for r in rows.values()
                if r["verdict"] in ("changed", "removed")]


def test_doctor_migration_check_still_fails_on_a_real_divergence(tmp_path):
    config, env, _ = migrate_and_pin(tmp_path, "sample-b")
    text = config.read_text()
    assert text.count("floor_gb = 1.5") == 1
    config.write_text(text.replace("floor_gb = 1.5", "floor_gb = 2.5"))
    found = migration_doctor_check(config, env)
    detail = found["detail"]
    assert found["ok"] is False
    assert len(detail["problems"]) == 1, detail["problems"]
    assert detail["problems"][0].startswith("backend.profile: ")
    assert "'floor_gb': 1.5" in detail["problems"][0]
    assert "'floor_gb': 2.5" in detail["problems"][0]
    # the authorized pin is still reported as such, not as a problem
    assert [e["key"] for e in detail["changed_later"]] == ["backend.sha256"]


def _fake_pin(config: Path, utc: str, new: str, *, path=None) -> dict:
    return {"schema": "agent-compute-authorization/1", "utc": utc,
            "action": "pin-backend", "authorized_by": PIN_AUTH,
            "config": str(path or config), "engine_sha256": new,
            "diff_summary": {"changed": {"backend.sha256": [V1_SHA, new]}},
            "host": "beta", "host_id": "x"}


@pytest.mark.parametrize("case", ["hand-edit-after-pin", "failed-pin",
                                  "pin-before-migration",
                                  "pin-of-another-config"])
def test_doctor_migration_check_excuses_only_a_matching_later_pin(tmp_path,
                                                                  case):
    """A difference is excused only by a later, successful authorization of
    this config that wrote the value the config holds now."""
    other = "ab" * 32
    if case == "hand-edit-after-pin":
        config, env, _ = migrate_and_pin(tmp_path, "sample-b")
        now = other
        config.write_text(config.read_text().replace(engine_sha(), other))
    else:
        config, env = sandbox(tmp_path, "sample-b")
        records = auth_file(tmp_path)
        records.parent.mkdir(parents=True, exist_ok=True)
        if case == "pin-before-migration":
            records.write_text(json.dumps(_fake_pin(
                config, "2026-10-09T01:00:00Z", other)) + "\n")
        assert migrate(config, env).returncode == 0
        if case == "failed-pin":
            pin_record = _fake_pin(config, "2026-10-09T23:00:00Z", other)
            failed = {"schema": "agent-compute-authorization/1",
                      "utc": "2026-10-09T23:00:01Z",
                      "action": "pin-backend-failed",
                      "authorized_by": PIN_AUTH,
                      "authorization_utc": pin_record["utc"],
                      "config": str(config), "error": "OSError: test",
                      "host": "beta", "host_id": "x"}
            with records.open("a") as handle:
                handle.write(json.dumps(pin_record) + "\n")
                handle.write(json.dumps(failed) + "\n")
        elif case == "pin-of-another-config":
            with records.open("a") as handle:
                handle.write(json.dumps(_fake_pin(
                    config, "2026-10-09T23:00:00Z", other,
                    path=config.with_name("other.toml"))) + "\n")
        now = other
        config.write_text(config.read_text().replace(V1_SHA, other))
    found = migration_doctor_check(config, env)
    detail = found["detail"]
    assert found["ok"] is False, (case, detail)
    assert any(p.startswith("backend.sha256: ") and now in p
               for p in detail["problems"]), detail["problems"]
    assert detail.get("changed_later", []) == []


@pytest.mark.parametrize("damage", ["unreadable-authorizations",
                                    "missing-backup"])
def test_doctor_migration_check_fails_closed_without_its_records(tmp_path,
                                                                 damage):
    config, env, _ = migrate_and_pin(tmp_path, "sample-b")
    records = auth_file(tmp_path)
    if damage == "unreadable-authorizations":
        if os.geteuid() == 0:
            pytest.skip("[root] root reads a mode-000 file")
        records.chmod(0)
    else:
        backup = Path(json.loads(records.read_text().splitlines()[0])
                      ["backup"])
        backup.unlink()
    try:
        found = migration_doctor_check(config, env)
    finally:
        records.chmod(0o600)
    assert found["ok"] is False, found
    assert found["detail"]["error"]
