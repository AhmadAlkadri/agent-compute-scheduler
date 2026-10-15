"""Isolated test harness.

Every test gets its own guard home (so no real lock is ever touched), a
fake host-memory reading (so nothing pressures the real host), throwaway
git repositories standing in for two projects, and — when the queue is
enabled — a private pueued instance on a short /tmp socket path (AF_UNIX
limits the path length).  Every ACS job runs through the internal guard of
this tree (``src/agent_compute/guard/engine.py``), the only backend.

Test profiles (campaign G decision D7; G2b, G4):

* **standard** (the default): needs only this repository, Python >= 3.11,
  Pueue and, for the container tests, Docker with the test image.  No
  ``guard.py``, no private path, no operator file: every ``Env`` uses the
  generic fixture ``tests/guard_profiles/standard.json`` (host-independent,
  small enough to load on any supported host), the engine sandbox of the
  guard tests ``guard_profiles/sandbox.json``; ``mac.json`` and
  ``lenovo.json`` are the operator hosts' numbers kept as named examples.
  Every fixture profile states its ``disk_floor_gb`` (1 GB: the engine's
  20 GB default is a host's, not a test's).
* **legacy**: ``ACS_TEST_LEGACY_GUARD=<guard.py>`` adds the historical
  comparisons with an external ``guard.py`` on one lock (exclusion both
  ways, record parity, the profile constants) and the reproductions of
  ``guard.py``'s own defects, run against that file directly (ACS no longer
  runs it).  ``ACS_TEST_LEGACY_GUARDS`` (``os.pathsep``-separated) adds
  further copies to the interop exclusion and nesting cases.  Evidence,
  not a merge gate.

The container tests (marker ``needs_docker``) use ``TEST_IMAGE``: the
public ``PUBLIC_IMAGE`` pinned by digest, or ``ACS_TEST_IMAGE`` (an image
of your own, for example a local build).  The harness never pulls unless asked:
``ACS_TEST_PULL=1`` pulls the image for the Docker daemon's own platform;
otherwise an absent image (or one present only for another architecture)
skips those tests with a counted ``[docker]`` reason.

Every skip reason opens with a category tag (``skip_reason``) that
``tools/skip_report.py`` counts: docker, platform, legacy, pueue, host,
clone, root.

``ACS_TEST_BACKEND`` and ``ACS_TEST_GUARD`` (the two backend modes before
0.2.0) are refused, so that no run is mislabelled.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
BIN = REPO / "bin" / "agent-compute"
WORKLOAD = REPO / "tests" / "workload.py"
GB = 1024 ** 3
for _retired in ("ACS_TEST_BACKEND", "ACS_TEST_GUARD"):
    if os.environ.get(_retired):
        raise SystemExit(
            f"{_retired} was removed in 0.2.0 (campaign G slice G2b): every "
            "ACS job runs through the internal guard (the standard "
            "profile); the historical guard.py comparisons are the legacy "
            "profile, ACS_TEST_LEGACY_GUARD=<guard.py>")
# The legacy profile (D7): an external guard.py for the historical
# comparisons, or None (the standard profile).
LEGACY_GUARD = (Path(os.environ["ACS_TEST_LEGACY_GUARD"]).expanduser()
                if os.environ.get("ACS_TEST_LEGACY_GUARD") else None)
if LEGACY_GUARD is not None and not LEGACY_GUARD.is_file():
    raise SystemExit(f"ACS_TEST_LEGACY_GUARD={LEGACY_GUARD}: no such file")
PROFILE = "legacy" if LEGACY_GUARD is not None else "standard"

# Skip accounting (G4): every skip reason opens with one of these tags;
# tools/skip_report.py counts them (its CATEGORIES are the same).
SKIP_CATEGORIES = ("docker", "platform", "legacy", "pueue", "host", "clone",
                   "root")


def skip_reason(category: str, text: str) -> str:
    if category not in SKIP_CATEGORIES:
        raise ValueError(f"unknown skip category {category!r}")
    return f"[{category}] {text}"


LEGACY_SKIP_REASON = skip_reason(
    "legacy", "legacy profile only (ACS_TEST_LEGACY_GUARD=<guard.py>): a "
              "historical comparison with an external guard.py")
needs_legacy = pytest.mark.skipif(LEGACY_GUARD is None,
                                  reason=LEGACY_SKIP_REASON)
ENGINE = REPO / "src" / "agent_compute" / "guard" / "engine.py"
PROFILES = REPO / "tests" / "guard_profiles"
# The profile of every Env: one generic fixture, whatever the platform (G4;
# the operator hosts' numbers are the named examples mac.json/lenovo.json).
# TOML has no null, so a missing runlog tag is "".
TEST_PROFILE = {k: ("" if v is None else v) for k, v in
                json.loads((PROFILES / "standard.json").read_text()).items()
                if k != "schema"}
PUEUE = shutil.which("pueue")
PUEUED = shutil.which("pueued")

sys.path.insert(0, str(REPO / "src"))

# The engine's test hooks (fake readings), by the engine's own names: a
# rename there is a rename here.
from agent_compute.guard import engine as _engine  # noqa: E402

FREE_MEM_ENV = _engine.ENV_FREE_FILE              # fake available bytes
SAMPLER_FAIL_ENV = _engine.ENV_SAMPLER_FAIL_FILE  # a blind sampler
VM_FOOTPRINT_ENV = _engine.ENV_VM_FOOTPRINT_FILE  # the Docker VM's resident bytes
# What a test process never inherits into a sandbox: an enclosing job's or
# guard's variables (both the neutral and the legacy prefixes).
GUARD_ENV_PREFIXES = ("SMC_GUARD_", "ACS_GUARD_")


class QuietEnv(dict):
    """A copy of an environment that never shows its contents.

    The sandboxes copy ``os.environ`` for the commands they run.  An
    assertion over a call that received such a copy makes pytest print the
    arguments of the call with ``saferepr``, which calls ``__repr__``: a
    plain ``dict`` would put every variable of the machine into the failure
    text (and from there into an evidence file or an issue).  The repr names
    only the number of variables.  Everything else is a ``dict``
    (``subprocess`` takes it as an environment; ``update`` and indexing are
    unchanged).  Use it for every helper that copies ``os.environ`` into an
    object a test passes to a call it asserts on."""

    def __repr__(self) -> str:
        return f"<environment: {len(self)} variables>"

    __str__ = __repr__

# --- the container image (G4) -------------------------------------------------------------
# Public and pinned: the multi-architecture index digest of
# python:3.11.6-slim-bookworm (Debian 12 slim: python3 in /usr/local/bin,
# bash, setsid, nice, sha256sum; what the container tests use).
PUBLIC_IMAGE = ("python:3.11.6-slim-bookworm@sha256:"
                "cc758519481092eb5a4a5ab0c1b303e288880d59afc601958d19e95b300bc86b")
# ACS_TEST_IMAGE=<image> overrides it (any image with python3, bash, setsid,
# nice and sha256sum; a local build is fine).
TEST_IMAGE = os.environ.get("ACS_TEST_IMAGE") or PUBLIC_IMAGE
PULL = os.environ.get("ACS_TEST_PULL") == "1"
PULL_TIMEOUT_S = 900
needs_docker = pytest.mark.needs_docker


def docker_image_status(image: str, *, pull: bool,
                        docker: str | None) -> dict:
    """Whether the container tests can run ``image`` with the ``docker``
    CLI: ``{"ok", "reason", "pulled", "image", "platform"}``.  An image
    present only for another architecture than the daemon's counts as
    absent (it would run emulated); ``pull`` fetches the daemon's own
    platform.  ``reason`` is a ``[docker]`` skip reason when not ok."""
    status = {"ok": False, "reason": None, "pulled": False, "image": image,
              "platform": None}

    def run(*args, timeout=60):
        try:
            return subprocess.run([docker, *args], capture_output=True,
                                  text=True, timeout=timeout)
        except (OSError, subprocess.SubprocessError) as error:
            return subprocess.CompletedProcess(args, 1, "", str(error))

    def last(text: str) -> str:
        lines = [line for line in text.strip().splitlines() if line.strip()]
        return lines[-1][:200] if lines else "no output"

    if not docker:
        status["reason"] = skip_reason("docker", "no docker CLI on PATH")
        return status
    version = run("version", "--format", "{{.Server.Os}}/{{.Server.Arch}}")
    platform = version.stdout.strip()
    if version.returncode or not platform:
        status["reason"] = skip_reason(
            "docker", f"the docker daemon does not answer "
                      f"({last(version.stderr or version.stdout)})")
        return status
    status["platform"] = platform

    def local() -> str | None:
        found = run("image", "inspect", "--format",
                    "{{.Os}}/{{.Architecture}}", image)
        return found.stdout.strip() if found.returncode == 0 else None

    have = local()
    if have == platform:
        status["ok"] = True
        return status
    if not pull:
        status["reason"] = skip_reason("docker", (
            f"test image {image} is not present locally"
            if have is None else
            f"test image {image} is present only for {have}, the docker "
            f"daemon runs {platform}") + " (ACS_TEST_PULL=1 pulls it for "
            f"{platform})")
        return status
    pulled = run("pull", "--platform", platform, image,
                 timeout=PULL_TIMEOUT_S)
    if pulled.returncode:
        status["reason"] = skip_reason(
            "docker", f"pulling test image {image} for {platform} failed "
                      f"({last(pulled.stderr or pulled.stdout)})")
        return status
    have = local()
    if have != platform:
        status["reason"] = skip_reason(
            "docker", f"test image {image} is not present for {platform} "
                      f"after the pull (found: {have})")
        return status
    status.update(ok=True, pulled=True)
    return status


# In-container signals go through python3: the public slim image has no
# procps (no kill, pkill or ps).
_KILL = ("import os, sys\n"
         "for p in sys.argv[1:]:\n"
         "    try: os.kill(int(p), 9)\n"
         "    except (OSError, ValueError): pass\n")
_KILL_MATCHING = (
    "import os, sys\n"
    "for p in os.listdir('/proc'):\n"
    "    if not p.isdigit() or int(p) == os.getpid(): continue\n"
    "    try: c = open('/proc/%s/cmdline' % p).read()\n"
    "    except OSError: continue\n"
    "    if sys.argv[1] in c:\n"
    "        try: os.kill(int(p), 9)\n"
    "        except OSError: pass\n")


def container_kill(name: str, pids, docker: str = "docker") -> None:
    """SIGKILL these in-container pids (a test's own cleanup)."""
    pids = [str(int(p)) for p in pids]
    if pids:
        subprocess.run([docker, "exec", "-w", "/", name, "python3", "-I",
                        "-S", "-c", _KILL, *pids], capture_output=True,
                       timeout=60)


def container_kill_matching(name: str, pattern: str,
                            docker: str = "docker") -> None:
    """SIGKILL every in-container process whose command line contains
    ``pattern`` (a test's own cleanup; the killer itself is spared)."""
    subprocess.run([docker, "exec", "-w", "/", name, "python3", "-I", "-S",
                    "-c", _KILL_MATCHING, pattern], capture_output=True,
                   timeout=60)


_DOCKER_STATUS: dict | None = None


def docker_status() -> dict:
    """``docker_image_status`` of ``TEST_IMAGE``, computed once per
    session (and only when a collected test needs Docker)."""
    global _DOCKER_STATUS
    if _DOCKER_STATUS is None:
        _DOCKER_STATUS = docker_image_status(TEST_IMAGE, pull=PULL,
                                             docker=shutil.which("docker"))
    return _DOCKER_STATUS


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "needs_docker: a container test; skipped with a counted "
        "[docker] reason unless docker runs and TEST_IMAGE is present for "
        "the daemon's platform (ACS_TEST_PULL=1 pulls it)")


def pytest_report_header(config):
    return [f"acs test profile: {PROFILE} (legacy guard: "
            f"{LEGACY_GUARD or 'none'}); test image: {TEST_IMAGE} ("
            f"{'ACS_TEST_IMAGE' if os.environ.get('ACS_TEST_IMAGE') else 'the public default'}"
            f"; ACS_TEST_PULL={'1' if PULL else 'unset'})"]


def pytest_collection_modifyitems(config, items):
    docker_items = [item for item in items
                    if item.get_closest_marker("needs_docker")]
    if not docker_items:
        return
    status = docker_status()
    if not status["ok"]:
        skip = pytest.mark.skip(reason=status["reason"])
        for item in docker_items:
            item.add_marker(skip)


def pytest_terminal_summary(terminalreporter):
    if _DOCKER_STATUS is not None:
        status = _DOCKER_STATUS
        where = f" on {status['platform']}" if status["platform"] else ""
        terminalreporter.write_line(
            f"acs test image: {status['image']}{where}: "
            + ("used" + (" (pulled by this run)" if status["pulled"] else
                         " (present)") if status["ok"]
               else f"not used: {status['reason']}"))

# the variables a job's guard and workload carry (never inherited by a test
# process from a job this suite itself may run inside of)
JOB_VARIABLES = ("ACS_JOB_ID", "ACS_JOB_MARKER", "ACS_GUARD_HOME",
                 "ACS_CASE", "ACS_WORKERS", "ACS_DENSE_THREADS", "ACS_SLICE",
                 "AGENT_COMPUTE_SUBMITTER")


# The sandbox sentinel (review round 1 of G2a): the internal guard's
# test-only claim bypass (ACS_GUARD_TEST_UNCLAIMED=1) works only in a guard
# home that holds this file, which only the test harness creates (every
# Env's guard home, and the engine sandboxes of the guard tests).  A
# production guard home never has it, whatever its path.
SANDBOX_SENTINEL = "ACS_TEST_SANDBOX"


def mark_sandbox(home: Path) -> Path:
    home.mkdir(parents=True, exist_ok=True)
    (home / SANDBOX_SENTINEL).write_text(
        "agent-compute test sandbox: the engine's test-only claim bypass "
        "works only in a guard home holding this file (tests/conftest.py)\n")
    return home


def config_schema() -> str:
    from agent_compute.config import CONFIG_SCHEMA
    return CONFIG_SCHEMA


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True,
                   capture_output=True,
                   env=QuietEnv({**os.environ, "GIT_AUTHOR_NAME": "t",
                                 "GIT_AUTHOR_EMAIL": "t@t",
                                 "GIT_COMMITTER_NAME": "t",
                                 "GIT_COMMITTER_EMAIL": "t@t"}))


# --- gate evidence as tools/gate_run.py writes it (campaign PA, slice K4) -----------------
# Synthetic: the shape of a tool-written full-suite file, built by hand so a
# test can mutate one part of it.  The real tool is exercised in
# tests/test_gate_run.py with a fake interpreter.

def gate_header(host: str, sha: str, *, docker: str = "DOCKER HIDDEN (test)",
                mode: str = "profile standard", source: str = "a" * 40,
                redactions: int = 0, token: bool = True,
                changes: int = 0) -> str:
    return (f"# {host} full suite on {host}, {docker}; {mode}; source sha "
            f"{source}; engine sha256 {sha}; pueue 4.0.4; Python "
            f"3.12.14 (/x/python); tracked changes: {changes}; redactions: "
            f"{redactions}" + ("; written by tools/gate_run.py 1" if token
                               else "") + "; 2026-10-10T12:00:00Z")


def gate_body(*, passed: int = 5, skipped: int = 2, failed: int = 0,
              errors: int = 0, deselected: int = 0,
              collected: int | None = None, reason: str | None = None) -> str:
    """A synthetic ``pytest -v -rs`` output that the skip report finds
    consistent."""
    reason = reason or skip_reason("docker", "no docker CLI on PATH")
    total = passed + skipped + failed + errors
    n = total if collected is None else collected
    lines = ["============================= test session starts "
             "==============================",
             "platform darwin -- Python 3.12.14, pytest-9.1.1, pluggy-1.6.0",
             "acs test profile: standard (legacy guard: none)"]
    lines.append(f"collecting ... collected {n + deselected} items"
                 + (f" / {deselected} deselected / {n} selected"
                    if deselected else ""))
    lines.append("")
    outcomes = (["PASSED"] * passed + ["SKIPPED"] * skipped
                + ["FAILED"] * failed + ["ERROR"] * errors)
    for i, outcome in enumerate(outcomes):
        pct = int(100 * (i + 1) / max(len(outcomes), 1))
        lines.append(f"tests/test_x.py::test_{i} {outcome} [{pct:3d}%]")
    lines += ["", "=========================== short test summary info "
              "============================"]
    if skipped:
        lines.append(f"SKIPPED [{skipped}] tests/test_x.py:3: {reason}")
    parts = [f"{count} {word}" for count, word in
             ((failed, "failed"), (passed, "passed"), (skipped, "skipped"),
              (deselected, "deselected"), (errors, "errors")) if count]
    lines.append("=" * 8 + " " + ", ".join(parts) + " in 12.34s " + "=" * 8)
    return "\n".join(lines) + "\n"


def gate_text(header: str, body: str | None = None, *, skip_line=None,
              exit_code: int = 0) -> str:
    """Header, skip line (computed from the body unless given; ``False``
    omits it), body and ``EXIT``: a tool-shaped evidence file."""
    body = gate_body() if body is None else body
    if skip_line is None:
        sys.path.insert(0, str(REPO / "tools"))
        try:
            import skip_report
            skip_line = "# " + skip_report.summary_line(
                skip_report.parse(body))
        finally:
            sys.path.pop(0)
    return (header + "\n" + (skip_line + "\n" if skip_line else "") + body
            + f"EXIT {exit_code}\n")


def profile_toml(profile: dict) -> str:
    return "[backend.profile]\n" + "".join(
        f"{k} = {json.dumps(v)}\n" for k, v in profile.items())


def engine_profile(profile: dict = TEST_PROFILE) -> dict:
    """``profile`` as the engine's --profile-file JSON."""
    return {"schema": "agent-compute-guard-profile/1",
            **{k: (v or None) if k == "runlog_tag" else v
               for k, v in profile.items()}}


def copy_tree(root: Path, engine_text: str | None = None) -> Path:
    """A copy of this tree's ``bin/`` and ``src/`` (standing in for another
    installed version), optionally with another engine: a CLI run from it
    runs its runner and its engine."""
    tree = root / "tree"
    shutil.copytree(REPO / "bin", tree / "bin")
    shutil.copytree(REPO / "src", tree / "src",
                    ignore=shutil.ignore_patterns("__pycache__"))
    if engine_text is not None:
        (tree / "src" / "agent_compute" / "guard" / "engine.py").write_text(
            engine_text)
    return tree


class Env:
    def __init__(self, queue: bool, extra_config: str = "",
                 tree: Path | None = None):
        """``tree``: run the CLI (and so its runner and engine) from a
        copy made by ``copy_tree`` instead of this checkout; the config
        pins that copy's engine."""
        self.tree = tree or REPO
        self.bin = self.tree / "bin" / "agent-compute"
        self.engine = self.tree / "src" / "agent_compute" / "guard" / \
            "engine.py"
        if queue and not (PUEUE and PUEUED):
            pytest.skip(skip_reason("pueue", "pueue/pueued not on PATH "
                                              "(queue-mode tests)"))
        self.root = Path(tempfile.mkdtemp(prefix="acs-", dir="/tmp"))
        self.state = self.root / "s"
        self.guard_home = mark_sandbox(self.root / "g")
        self.free = self.root / "free"
        self.free.write_text(str(64 * GB))
        self.queue = queue
        self.projects = {}
        for name in ("alpha", "beta"):
            repo = self.root / f"proj {name}"       # a space in the path
            repo.mkdir()
            git(repo, "init", "-q", "-b", "main")
            (repo / "input.txt").write_text(f"{name} v1\n")
            git(repo, "add", ".")
            git(repo, "commit", "-q", "-m", "init")
            self.projects[name] = repo
        self.config = self.root / "config.toml"
        q = (f'[queue]\nenabled = true\npueue = "{PUEUE}"\n'
             f'pueued = "{PUEUED}"\ngroup = "acs-heavy"\n') if queue else \
            '[queue]\nenabled = false\n'
        projects = ""
        for name, repo in self.projects.items():
            projects += (
                f'[projects.{name}]\nroots = ["{repo}"]\n'
                f'case_prefix = "{name}."\n'
                'runlog = "{state}/projects/{project}/runlog.md"\n'
                'baseline = "{state}/projects/{project}/baseline.json"\n'
                'max_timeout_min = 5\nmax_threads = 2\n'
                'env_passthrough = ["ACS_TEST_PASS"]\n'
                'containers = ["__CONTAINER__"]\n'
                'container_workdir = "/work"\n'
                # the operator hosts' choice (guard.py's behaviour before
                # 0.2.0: ladder detection on, docker update on every
                # container job), declared; the default detector is "none"
                'reserve_detector = "smc-ladder"\n'
                'container_limits = ["__CONTAINER__"]\n\n')
        self.config.write_text(
            f'schema = "{config_schema()}"\n'
            'test_mode = true\n'
            'host = "testhost"\n'
            f'state_dir = "{self.state}"\n'
            f'python = "{sys.executable}"\n\n'
            '[backend]\nkind = "acs-guard"\n'
            f'sha256 = "{sha256(self.engine)}"\n'
            f'guard_home = "{self.guard_home}"\n'
            'lock_wait_max_s = 120\nadmission_retry_s = 1\n'
            f'[backend.extra_env]\n{FREE_MEM_ENV} = "{self.free}"\n'
            + profile_toml(TEST_PROFILE)
            + "\n" + q + "\n" + projects + extra_config)
        self.procs: list[subprocess.Popen] = []

    def set_container(self, name: str) -> None:
        self.config.write_text(self.config.read_text().replace(
            "__CONTAINER__", name))

    # -- invocations -------------------------------------------------------
    def env(self, **extra) -> dict:
        # never the markers of a job this suite itself may run inside of
        env = QuietEnv((k, v) for k, v in os.environ.items()
                       if not k.startswith("SMC_GUARD_")
                       and k not in JOB_VARIABLES)
        env.update(extra)
        return env

    def acs(self, *args, env=None, timeout=180, check=False):
        result = subprocess.run([sys.executable, str(self.bin), "--config",
                                 str(self.config), *args],
                                capture_output=True, text=True,
                                env=env or self.env(), timeout=timeout)
        if check and result.returncode != 0:
            raise AssertionError(f"acs {args} -> {result.returncode}\n"
                                 f"{result.stdout}\n{result.stderr}")
        return result

    def acs_bg(self, *args, env=None) -> subprocess.Popen:
        proc = subprocess.Popen([sys.executable, str(self.bin), "--config",
                                 str(self.config), *args],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, env=env or self.env())
        self.procs.append(proc)
        return proc

    def submit(self, project="alpha", *, case="t", timeout_min=2,
               workload_args=(), extra=(), env=None) -> dict:
        result = self.acs("submit", "--json", "--case", case,
                          "--timeout-min", str(timeout_min),
                          "--reserve-gb", "0.5",
                          "--cwd", str(self.projects[project]), *extra, "--",
                          sys.executable, str(WORKLOAD), *workload_args,
                          env=env)
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout)

    def direct(self, out: Path, sleep: float, wait: float | None = None,
               case="direct", guard: Path | None = None) -> subprocess.Popen:
        """A guard called directly on this host's lock, outside ACS (a
        holder ACS must wait for): the internal guard with its sandbox-only
        claim bypass, or (``guard``, the legacy profile) an external
        guard.py, the pre-ACS path."""
        argv = [sys.executable, str(guard or self.engine), "run", "--case",
                case]
        if guard is None:
            profile = self.root / "direct-profile.json"
            profile.write_text(json.dumps(engine_profile()))
            argv += ["--profile-file", str(profile)]
        argv += ["--reserve-gb", "0.5", "--cwd", str(self.root),
                 "--outputs", str(self.root / "direct-out" / str(time.time())),
                 "--runlog", str(self.root / "direct-runlog.md"),
                 "--baseline", str(self.root / "direct-baseline.json"),
                 "--timeout-min", "2"]
        if wait:
            argv += ["--wait", str(wait)]
        argv += ["--", sys.executable, str(WORKLOAD), "--out", str(out),
                 "--sleep", str(sleep)]
        env = self.env(**{FREE_MEM_ENV: str(self.free)})
        if guard is None:
            env.update(ACS_GUARD_HOME=str(self.guard_home),
                       ACS_GUARD_TEST_UNCLAIMED="1")
        else:
            env.update(SMC_GUARD_HOME=str(self.guard_home))
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, env=env)
        self.procs.append(proc)
        return proc

    def legacy_container(self, container: str, command: list[str], *,
                         timeout_min: str = "1", case: str = "legacy",
                         path_env: dict | None = None
                         ) -> tuple[subprocess.Popen, Path]:
        """Legacy profile: the external guard.py run directly on a container
        job (ACS no longer runs it), on this Env's sandbox lock and fake
        memory.  Returns the guard process and its outputs dir
        (``guard.json``)."""
        assert LEGACY_GUARD is not None, "legacy profile only"
        out = self.root / "legacy-out" / f"{case}-{time.time()}"
        argv = [sys.executable, str(LEGACY_GUARD), "run", "--case", case,
                "--reserve-gb", "0.5", "--cwd", str(self.root),
                "--outputs", str(out),
                "--runlog", str(self.root / "legacy-runlog.md"),
                "--baseline", str(self.root / "legacy-baseline.json"),
                "--timeout-min", str(timeout_min), "--venue", "container",
                "--container", container, "--container-workdir", "/work",
                "--", *command]
        env = dict(path_env or self.env())
        # guard.py reads the legacy home variable only
        env.update({"SMC_GUARD_HOME": str(self.guard_home),
                    FREE_MEM_ENV: str(self.free)})
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, env=env)
        self.procs.append(proc)
        return proc, out

    def state_of(self, job_id: str) -> dict:
        path = self.state / "jobs" / job_id / "state.json"
        return json.loads(path.read_text()) if path.exists() else {}

    def wait_for(self, predicate, timeout=60, interval=0.2, what="condition"):
        deadline = time.time() + timeout
        while time.time() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(interval)
        raise AssertionError(f"timed out waiting for {what}")

    def wait_phase(self, job_id: str, phases, timeout=60) -> dict:
        phases = (phases,) if isinstance(phases, str) else phases
        return self.wait_for(
            lambda: (lambda s: s if (s.get("phase") in phases or
                                     s.get("outcome") in phases) else None)(
                self.state_of(job_id)), timeout=timeout,
            what=f"{job_id} in {phases}")

    def wait_outcome(self, job_id: str, timeout=90) -> dict:
        return self.wait_for(lambda: (lambda s: s if s.get("outcome") else
                                      None)(self.state_of(job_id)),
                             timeout=timeout, what=f"{job_id} outcome")

    def lock_free(self) -> bool:
        """Probe the *test* lock (never the real one)."""
        import fcntl
        self.guard_home.mkdir(exist_ok=True)
        with open(self.guard_home / "lock", "a+") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return False
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            return True

    # -- teardown ----------------------------------------------------------
    def cleanup(self) -> None:
        for proc in self.procs:
            if proc.poll() is None:
                proc.kill()
        # kill anything this test's jobs recorded, and only a recorded
        # process whose identity is proven (A60: the recorded start matches
        # the live process; a pid alone, or a pid another process now holds,
        # is never signalled)
        from agent_compute import procs
        for state in self.state.glob("jobs/*/state.json"):
            data = json.loads(state.read_text())
            for key in ("workload", "guard", "runner"):
                ident = data.get(key) or {}
                pid = ident.get("pid")
                if not pid:
                    continue
                if key == "workload" and ident.get("venue") == "host":
                    if procs.same_process(ident):
                        try:
                            os.killpg(int(pid), signal.SIGKILL)
                        except OSError:
                            pass
                else:
                    if procs.same_process(ident):
                        try:
                            os.kill(int(pid), signal.SIGKILL)
                        except OSError:
                            pass
        for pids in self.root.glob("**/*.pids"):
            for line in pids.read_text().split("\n"):
                if line.strip():
                    try:
                        os.kill(int(line.split()[1]), signal.SIGKILL)
                    except (OSError, ValueError, IndexError):
                        pass
        if self.queue:
            subprocess.run([PUEUE, "-c", str(self.state / "pueue" /
                                             "pueue.yml"), "shutdown"],
                           capture_output=True, timeout=30)
        shutil.rmtree(self.root, ignore_errors=True)


def events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()
            if line.strip()]


@pytest.fixture(autouse=True)
def _fresh_stuck_execs():
    """A20: ``procs`` remembers, per process, containers where an ACS
    helper timed out on the host side; every test starts without that."""
    from agent_compute import procs
    stuck = getattr(procs, "_STUCK", None)
    if stuck is not None:
        stuck.clear()
    yield
    if stuck is not None:
        stuck.clear()


@pytest.fixture
def env_direct():
    env = Env(queue=False)
    yield env
    env.cleanup()


@pytest.fixture
def env_queue():
    env = Env(queue=True)
    yield env
    env.cleanup()


@pytest.fixture(params=["direct", "queue"])
def env_any(request):
    env = Env(queue=request.param == "queue")
    yield env
    env.cleanup()


# the direct guard callers of the exclusion tests: the internal guard, and
# in the legacy profile an external guard.py too
DIRECT_GUARDS = [None] + ([LEGACY_GUARD] if LEGACY_GUARD else [])


@pytest.fixture(params=DIRECT_GUARDS,
                ids=["engine"] + (["guard.py"] if LEGACY_GUARD else []))
def direct_guard(request):
    return request.param
