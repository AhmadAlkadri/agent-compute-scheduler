"""tools/gate_run.py and the pin check it feeds (campaign PA slice K4;
register A47, A4; audit prevention (b)).

Before: the pin accepted any committed file whose first line named the host,
the profile and the engine sha256 and whose pytest summary showed 0 failed;
a smoke run of one file, a ``-k`` run or a hand-written header passed, and
there was no tool to write the file the README described.  Now:

* ``tools/gate_run.py`` runs the full suite (no path, no filter, no extra
  pytest argument), writes the gate evidence format (header before the run,
  the skip line, the pytest output, ``EXIT``), redacts the values of
  sensitive-named environment variables from what it writes and counts them,
  and may commit the file in a local evidence repository;
* ``cli.pin_evidence_problem`` additionally requires the tool's header token
  and ``full suite``, a ``consistent``/``untagged 0`` skip line, no
  ``deselected``, a collected count equal to the summary's, no failure or
  error, ``EXIT 0``; it keeps the committed-and-unmodified rule and the host,
  engine and sha256 checks.

The check is of form (A4): it catches mistakes, not forgery.  The tool's
tests use a fake interpreter that prints a canned pytest output; the real
suite is never run here.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from conftest import REPO, gate_body, gate_header, gate_text, git, sha256

from agent_compute import cli

TOOL = REPO / "tools" / "gate_run.py"
SHA = "ab" * 32
HOST = "testhost"
REDACTED = "<redacted by gate_run>"


# --- the pin check ------------------------------------------------------------------------

def commit(tmp_path: Path, text: str, name: str = "gate.txt") -> Path:
    repo = tmp_path / "evidence-repo"
    if not repo.exists():
        repo.mkdir()
        git(repo, "init", "-q", "-b", "main")
    path = repo / name
    path.write_text(text)
    git(repo, "add", name)
    git(repo, "commit", "-q", "-m", name)
    return path


def problem(path: Path, sha: str = SHA, host: str = HOST):
    return cli.pin_evidence_problem(path, sha, host)[0]


def good(**header) -> str:
    return gate_text(gate_header(HOST, SHA, **header))


def test_a_tool_shaped_full_suite_file_is_accepted(tmp_path):
    assert problem(commit(tmp_path, good())) is None


def test_a_smoke_file_is_refused(tmp_path):
    # the old shape: a short run of one file, header without the tool's token
    text = (f"# {HOST} full suite on {HOST}, DOCKER HIDDEN; profile standard; "
            f"engine sha256 {SHA}\n"
            "============================= test session starts ====\n"
            "collected 1 item\n\n"
            "tests/test_smoke.py::test_x PASSED [100%]\n\n"
            "======= 1 passed in 0.10s =======\nEXIT 0\n")
    found = problem(commit(tmp_path, text))
    assert found and "gate_run" in found, found


def test_a_k_run_is_refused(tmp_path):
    text = gate_text(gate_header(HOST, SHA), gate_body(deselected=40))
    found = problem(commit(tmp_path, text))
    assert found and "deselected" in found, found


def test_a_missing_skip_line_is_refused(tmp_path):
    found = problem(commit(tmp_path, gate_text(gate_header(HOST, SHA),
                                               skip_line=False)))
    assert found and "skip" in found, found


def test_a_header_without_the_tool_token_is_refused(tmp_path):
    found = problem(commit(tmp_path, good(token=False)))
    assert found and "gate_run" in found, found


def test_a_header_edited_after_the_commit_is_refused(tmp_path):
    path = commit(tmp_path, good())
    path.write_text(path.read_text().replace("tracked changes: 0",
                                             "tracked changes: 1", 1))
    found = problem(path)
    assert found and "differs from its committed version" in found, found


def test_an_unknown_tool_format_is_refused(tmp_path):
    text = good().replace("gate_run.py 1;", "gate_run.py 7;", 1)
    found = problem(commit(tmp_path, text))
    assert found and "gate_run" in found, found


def test_an_inconsistent_skip_line_is_refused(tmp_path):
    body = gate_body()
    bad = body.replace("SKIPPED [2] ", "SKIPPED [1] ", 1)   # short summary lies
    found = problem(commit(tmp_path, gate_text(gate_header(HOST, SHA), bad)))
    assert found and "INCONSISTENT" in found, found


def test_an_untagged_skip_is_refused(tmp_path):
    body = gate_body(reason="nobody classified this one")
    found = problem(commit(tmp_path, gate_text(gate_header(HOST, SHA), body)))
    assert found and "untagged" in found, found


def test_a_collected_count_that_is_not_the_summary_is_refused(tmp_path):
    # collected 12, but the run reports 7 outcomes
    body = gate_body(collected=12)
    found = problem(commit(tmp_path, gate_text(gate_header(HOST, SHA), body)))
    assert found and "collected" in found, found


def test_a_skip_line_with_another_collected_count_is_refused(tmp_path):
    text = good().replace("skip-report: collected 7;",
                          "skip-report: collected 70;", 1)
    found = problem(commit(tmp_path, text))
    assert found and "collected" in found, found


@pytest.mark.parametrize("name,text,fragment", [
    ("failed", gate_text(gate_header(HOST, SHA), gate_body(failed=1)),
     "did not pass"),
    ("errors", gate_text(gate_header(HOST, SHA), gate_body(errors=1)),
     "did not pass"),
    ("exit-1", gate_text(gate_header(HOST, SHA), exit_code=1), "EXIT"),
    ("no-exit", gate_text(gate_header(HOST, SHA)).rsplit("EXIT", 1)[0],
     "EXIT"),
    ("tracked-changes", good(changes=2), "tracked changes"),
    ("no-redactions", good().replace("redactions: 0; ", ""), "redactions"),
    ("short-source-sha", good(source="abc1234"), "source sha"),
    ("not-line-one", "\n" + good(), "first line"),
    ("other-host", gate_text(gate_header("elsewhere", SHA)), "host"),
], ids=lambda value: value if len(value) < 30 else "")
def test_other_unproven_runs_are_refused(tmp_path, name, text, fragment):
    found = problem(commit(tmp_path, text))
    assert found and fragment in found, (name, found)


def test_a_with_docker_header_over_docker_skips_is_refused(tmp_path):
    # the body skipped the Docker-backed tests (2 [docker] skips)
    found = problem(commit(tmp_path, good(
        docker="WITH Docker (image python:3.11.6-slim-bookworm)")))
    assert found and "docker" in found.lower(), found


def test_a_with_docker_header_without_docker_skips_is_accepted(tmp_path):
    body = gate_body(skipped=1, reason="[platform] Linux host only")
    text = gate_text(gate_header(
        HOST, SHA, docker="WITH Docker (image python:3.11.6-slim-bookworm)"),
        body)
    assert problem(commit(tmp_path, text)) is None


# --- the tool ------------------------------------------------------------------------------

FAKE_PY = """#!/bin/sh
if [ "$1" = "--version" ]; then echo "Python 3.12.99"; exit 0; fi
echo "$@" > "$FAKE_ARGS"
head -c 1000 "$FAKE_OUT" > "$FAKE_PROBE" 2>/dev/null
command -v docker > "$FAKE_DOCKER" 2>/dev/null
if [ -n "$FAKE_HOOK" ]; then sh -c "$FAKE_HOOK"; fi
cat "$FAKE_CANNED"
exit "${FAKE_EXIT:-0}"
"""

FAKE_DOCKER = """#!/bin/sh
case "$1" in
  version) echo linux/arm64 ;;
  image) echo linux/arm64 ;;
  *) exit 1 ;;
esac
"""

FAKE_PUEUE = """#!/bin/sh
echo "{name} {version}"
"""

SECRETS = {"token": "sentinel-token-value-1", "key": "sentinel-key-value-2",
           "pw": "sentinel-pw-value-3", "env": "sentinel-env-value-4"}


class Mini:
    """A tiny git repository holding copies of the tool, the skip report, the
    engine and the harness's conftest, and a fake interpreter."""

    def __init__(self, root: Path):
        self.root = root
        self.repo = root / "repo"
        for rel in ("tools/gate_run.py", "tools/skip_report.py",
                    "src/agent_compute/guard/engine.py", "tests/conftest.py"):
            if not (REPO / rel).exists():
                assert rel != "tools/gate_run.py", "tools/gate_run.py missing"
                continue
            (self.repo / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(REPO / rel, self.repo / rel)
        git(self.repo, "init", "-q", "-b", "main")
        git(self.repo, "add", ".")
        git(self.repo, "commit", "-q", "-m", "mini")
        self.fake = root / "fakepy"
        self.fake.write_text(FAKE_PY)
        self.fake.chmod(0o755)
        self.bin = root / "bin"
        self.bin.mkdir()
        for name in ("pueue", "pueued"):
            self.fake_tool(name, "4.0.4")
        self.canned = root / "canned.txt"
        self.out = root / "gate-mini.txt"
        self.evidence = root / "evidence"
        self.probe = root / "probe.txt"
        self.args_seen = root / "args.txt"
        self.docker_seen = root / "docker.txt"
        self.set_body(gate_body(passed=6, skipped=1, reason="[platform] "
                                "Linux host only"))

    def fake_tool(self, name: str, version: str):
        path = self.bin / name
        path.write_text(FAKE_PUEUE.format(name=name, version=version))
        path.chmod(0o755)

    def set_body(self, body: str, *, secrets: bool = False):
        if secrets:
            extra = (f"ACS_TEST_API_TOKEN={SECRETS['token']}\n"
                     f"environ: {{'HOME': '/h', 'GITHUB_KEY': "
                     f"'{SECRETS['key']}'}}\n"
                     f'config: {{"db_password": "{SECRETS["pw"]}"}}\n'
                     f"a bare copy of {SECRETS['env']} in a message\n")
            head, tail = body.split("\n\n", 1)
            body = head + "\n" + extra + "\n" + tail
        self.canned.write_text(body)

    @property
    def engine_sha(self) -> str:
        return sha256(self.repo / "src/agent_compute/guard/engine.py")

    def env(self, **extra) -> dict:
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("PYTEST", "ACS_"))}
        env.update(PATH=f"{self.bin}:/usr/bin:/bin", FAKE_ARGS=str(
            self.args_seen), FAKE_OUT=str(self.out), FAKE_PROBE=str(
            self.probe), FAKE_DOCKER=str(self.docker_seen),
            FAKE_CANNED=str(self.canned), **extra)
        return env

    def run(self, *args, env=None, label="mini", python=True, out=True):
        cmd = [sys.executable, str(self.repo / "tools" / "gate_run.py"),
               "--label", label]
        if out:
            cmd += ["--out", str(self.out)]
        if python:
            cmd += ["--python", str(self.fake)]
        return subprocess.run([*cmd, *args], capture_output=True, text=True,
                              env=env or self.env(), timeout=120,
                              cwd=str(self.root), stdin=subprocess.DEVNULL)


@pytest.fixture
def mini(tmp_path):
    (tmp_path / "repo").mkdir()
    return Mini(tmp_path)


def test_the_tool_exists_and_is_stdlib_only():
    assert TOOL.is_file()
    assert os.access(TOOL, os.X_OK)
    text = TOOL.read_text()
    assert text.startswith("#!/usr/bin/env python3")
    assert "import pytest" not in text


def test_it_writes_the_evidence_format_and_the_pin_accepts_it(mini):
    r = mini.run("--docker", "hidden", "--evidence-repo", str(mini.evidence))
    assert r.returncode == 0, r.stdout + r.stderr
    lines = mini.out.read_text().splitlines()
    assert lines[0].startswith("# mini full suite on mini, DOCKER HIDDEN (")
    for part in ("profile standard", "source sha ", "engine sha256 "
                 + mini.engine_sha, "Python 3.12.99", "tracked changes: 0",
                 "redactions: 0", "written by tools/gate_run.py 1"):
        assert part in lines[0], (part, lines[0])
    sha = subprocess.run(["git", "-C", str(mini.repo), "rev-parse", "HEAD"],
                         capture_output=True, text=True).stdout.strip()
    assert f"source sha {sha}" in lines[0]
    assert lines[1].startswith("# skip-report: collected 7; 6 passed, 1 "
                               "skipped;") and lines[1].endswith(
        "untagged 0; consistent")
    assert lines[-1] == "EXIT 0"
    committed = mini.evidence / mini.out.name
    assert problem(committed, mini.engine_sha, "mini") is None
    # a copy with one header byte changed, committed, is refused: the engine
    # sha256 it names is no longer the installed one
    text = committed.read_text().replace("engine sha256 " + mini.engine_sha[:1],
                                         "engine sha256 "
                                         + ("0" if mini.engine_sha[0] != "0"
                                            else "1"), 1)
    other = mini.evidence / "gate-edited.txt"
    other.write_text(text)
    git(mini.evidence, "add", other.name)
    git(mini.evidence, "commit", "-q", "-m", "edited")
    assert problem(other, mini.engine_sha, "mini")


def test_it_runs_the_full_suite_with_no_filter(mini):
    assert mini.run("--docker", "hidden").returncode == 0
    assert mini.args_seen.read_text().split() == [
        "-m", "pytest", "-v", "-rs", "-p", "no:cacheprovider"]


def test_it_writes_the_header_before_the_run(mini):
    assert mini.run("--docker", "hidden").returncode == 0
    first = mini.probe.read_text().splitlines()[0]
    assert first.startswith("# mini full suite on mini,")
    assert "written by tools/gate_run.py 1" in first


@pytest.mark.parametrize("extra", [("-k", "smoke"), ("-m", "not slow"),
                                   ("tests/test_x.py",), ("--", "-x"),
                                   ("--deselect", "a::b")])
def test_it_refuses_extra_pytest_arguments(mini, extra):
    r = mini.run("--docker", "hidden", *extra)
    assert r.returncode != 0
    assert "pytest" in r.stderr
    assert not mini.args_seen.exists() and not mini.out.exists()


def test_it_refuses_pytest_addopts_in_the_environment(mini):
    r = mini.run("--docker", "hidden",
                 env=mini.env(PYTEST_ADDOPTS="-k smoke"))
    assert r.returncode != 0 and "PYTEST_ADDOPTS" in r.stderr
    assert not mini.args_seen.exists() and not mini.out.exists()


def test_it_refuses_a_dirty_tree(mini):
    with open(mini.repo / "src/agent_compute/guard/engine.py", "a") as f:
        f.write("# edit\n")
    r = mini.run("--docker", "hidden")
    assert r.returncode != 0 and "uncommitted" in r.stderr
    assert not mini.args_seen.exists() and not mini.out.exists()


def test_it_refuses_an_existing_out_file_and_a_bad_label(mini):
    mini.out.write_text("old\n")
    assert mini.run("--docker", "hidden").returncode != 0
    assert mini.out.read_text() == "old\n"
    mini.out.unlink()
    r = mini.run("--docker", "hidden", label="bad label;")
    assert r.returncode != 0 and not mini.out.exists()


def test_it_redacts_sensitive_values_and_counts_them(mini):
    mini.set_body(gate_body(passed=6, skipped=1, reason="[platform] x"),
                  secrets=True)
    r = mini.run("--docker", "hidden",
                 env=mini.env(ACS_FAKE_SECRET=SECRETS["env"]))
    assert r.returncode == 0, r.stdout + r.stderr
    text = mini.out.read_text()
    for value in SECRETS.values():
        assert value not in text, value
        assert value not in r.stdout + r.stderr
    assert text.count(REDACTED) == 4
    assert "redactions: 4;" in text.splitlines()[0]
    assert "ACS_TEST_API_TOKEN=" + REDACTED in text
    assert "'GITHUB_KEY': '" + REDACTED + "'" in text
    assert '"db_password": "' + REDACTED + '"' in text
    assert "'HOME': '/h'" in text                       # not sensitive
    assert "a bare copy of " + REDACTED + " in a message" in text
    # the redacted file is still a consistent full-suite file
    assert text.splitlines()[1].endswith("untagged 0; consistent")


def test_a_failing_run_is_written_not_committed_and_exits_non_zero(mini):
    mini.set_body(gate_body(passed=5, failed=1, skipped=1,
                            reason="[platform] x"))
    r = mini.run("--docker", "hidden", "--evidence-repo", str(mini.evidence),
                 env=mini.env(FAKE_EXIT="1"))
    assert r.returncode == 1                 # pytest's own code
    text = mini.out.read_text()
    assert text.splitlines()[-1] == "EXIT 1"
    assert not (mini.evidence / mini.out.name).exists()
    assert problem(mini.out, mini.engine_sha, "mini")


def test_the_evidence_repository_is_local_and_committed(mini):
    r = mini.run("--docker", "hidden", "--evidence-repo", str(mini.evidence),
                 "--author", "Gate Test <gate@example.invalid>")
    assert r.returncode == 0, r.stdout + r.stderr
    remotes = subprocess.run(["git", "-C", str(mini.evidence), "remote"],
                             capture_output=True, text=True)
    assert remotes.returncode == 0 and remotes.stdout.strip() == ""
    head = subprocess.run(["git", "-C", str(mini.evidence), "rev-parse",
                           "HEAD"], capture_output=True, text=True
                          ).stdout.strip()
    assert len(head) == 40 and head in r.stdout
    author = subprocess.run(["git", "-C", str(mini.evidence), "log", "-1",
                             "--format=%an <%ae>"], capture_output=True,
                            text=True).stdout.strip()
    assert author == "Gate Test <gate@example.invalid>"
    tracked = subprocess.run(["git", "-C", str(mini.evidence), "ls-files"],
                             capture_output=True, text=True).stdout.split()
    assert tracked == [mini.out.name]
    status = subprocess.run(["git", "-C", str(mini.evidence), "status",
                             "--porcelain"], capture_output=True, text=True)
    assert status.stdout.strip() == ""


def test_docker_hidden_removes_only_docker_from_the_path(mini):
    docker = mini.bin / "docker"
    docker.write_text(FAKE_DOCKER)
    docker.chmod(0o755)
    other = mini.bin / "othertool"
    other.write_text("#!/bin/sh\nexit 0\n")
    other.chmod(0o755)
    assert mini.run("--docker", "hidden").returncode == 0
    assert mini.docker_seen.read_text().strip() == ""    # not found
    assert docker.exists() and other.exists()            # nothing uninstalled
    assert "DOCKER HIDDEN" in mini.out.read_text().splitlines()[0]


def test_docker_with_needs_a_working_docker(mini):
    # a docker that does not answer, first on PATH: host-independent (a real
    # docker may sit in /usr/bin)
    broken = mini.bin / "docker"
    broken.write_text("#!/bin/sh\nexit 1\n")
    broken.chmod(0o755)
    r = mini.run("--docker", "with")
    assert r.returncode != 0 and "docker" in r.stderr.lower()
    assert not mini.args_seen.exists() and not mini.out.exists()


def test_docker_with_records_the_image(mini):
    docker = mini.bin / "docker"
    docker.write_text(FAKE_DOCKER)
    docker.chmod(0o755)
    r = mini.run("--docker", "with",
                 env=mini.env(ACS_TEST_IMAGE="local/image:1"))
    assert r.returncode == 0, r.stdout + r.stderr
    first = mini.out.read_text().splitlines()[0]
    assert "WITH Docker (image local/image:1)" in first
    assert mini.docker_seen.read_text().strip().endswith("docker")


def test_docker_with_over_docker_skips_is_not_evidence(mini):
    docker = mini.bin / "docker"
    docker.write_text(FAKE_DOCKER)
    docker.chmod(0o755)
    mini.set_body(gate_body(passed=5, skipped=2))        # [docker] skips
    r = mini.run("--docker", "with", "--evidence-repo", str(mini.evidence))
    assert r.returncode == 66 and "docker" in r.stderr.lower()
    assert not (mini.evidence / mini.out.name).exists()


def test_the_docker_mode_is_required(mini):
    r = mini.run()
    assert r.returncode != 0 and not mini.out.exists()


# --- review round 2 (CHANGES REQUIRED on 4cbed21) ------------------------------------------

def two_lines(text: str, line: str, after: str) -> str:
    """``text`` with ``line`` inserted after the first line starting with
    ``after``."""
    out, done = [], False
    for each in text.splitlines():
        out.append(each)
        if not done and each.startswith(after):
            out.append(line)
            done = True
    assert done, after
    return "\n".join(out) + "\n"


@pytest.mark.parametrize("name,make,fragment", [
    ("two-headers", lambda: two_lines(
        good(), gate_header(HOST, SHA), "# skip-report"), "header"),
    ("two-collected", lambda: two_lines(
        good(), "collecting ... collected 7 items", "collecting"),
     "collected"),
    ("two-summaries", lambda: two_lines(
        good(), "======== 5 passed, 2 skipped in 12.34s ========",
        "========"), "summary"),
    ("two-exits", lambda: good() + "EXIT 0\n", "EXIT"),
    ("two-skip-lines", lambda: two_lines(
        good(), "# skip-report: collected 7; 5 passed, 2 skipped; skips by "
        "category: docker 2; untagged 0; consistent", "# skip-report"),
     "skip"),
    ("refused-trailer", lambda: good().replace(
        "EXIT 0\n", "# gate_run: REFUSED, the tree changed during the "
        "run\nEXIT 0\n"), "REFUSED"),
    ("no-pueue", lambda: good().replace("pueue 4.0.4", "pueue not on PATH"),
     "pueue"),
    ("pueue-skip", lambda: gate_text(gate_header(HOST, SHA), gate_body(
        reason="[pueue] pueue not installed")), "pueue"),
], ids=lambda value: value if isinstance(value, str) and len(value) < 20
    else "")
def test_review_round2_unproven_files_are_refused(tmp_path, name, make,
                                                  fragment):
    found = problem(commit(tmp_path, make()))
    assert found and fragment in found, (name, found)


def test_a_copied_skip_line_is_checked_against_the_body(tmp_path):
    # the body skipped 2 Docker tests; line 2 claims platform skips instead
    honest = good(docker="WITH Docker (image x)")
    lie = honest.replace("skips by category: docker 2",
                         "skips by category: platform 2", 1)
    assert lie != honest
    found = problem(commit(tmp_path, lie))
    assert found and "categories" in found, found


def mini_commit(mini: Mini, text: str, message: str) -> str:
    target = mini.repo / "src/agent_compute/guard/engine.py"
    target.write_text(target.read_text() + text)
    git(mini.repo, "commit", "-qam", message)
    return subprocess.run(["git", "-C", str(mini.repo), "rev-parse", "HEAD"],
                          capture_output=True, text=True).stdout.strip()


def test_head_changing_during_the_run_is_not_evidence(mini):
    git(mini.repo, "checkout", "-q", "-b", "other")
    mini_commit(mini, "# another engine\n", "other engine")
    git(mini.repo, "checkout", "-q", "main")
    hook = f"git -C '{mini.repo}' checkout -q other"
    r = mini.run("--docker", "hidden", "--evidence-repo", str(mini.evidence),
                 env=mini.env(FAKE_HOOK=hook))
    assert r.returncode == 65, r.stdout + r.stderr
    assert not (mini.evidence / mini.out.name).exists()
    text = mini.out.read_text()
    assert text.splitlines()[-1] == "EXIT 65"
    assert "# gate_run: REFUSED" in text
    assert problem(mini.out, mini.engine_sha, "mini")


def test_a_tree_dirtied_during_the_run_is_not_evidence(mini):
    hook = f"echo '# x' >> '{mini.repo}/src/agent_compute/guard/engine.py'"
    r = mini.run("--docker", "hidden", "--evidence-repo", str(mini.evidence),
                 env=mini.env(FAKE_HOOK=hook))
    assert r.returncode == 65, r.stdout + r.stderr
    assert not (mini.evidence / mini.out.name).exists()
    text = mini.out.read_text()
    assert text.splitlines()[-1] == "EXIT 65"
    assert problem(mini.out, mini.engine_sha, "mini")


def test_a_run_without_pueue_is_refused(mini):
    # a pueue that does not answer, first on PATH (a real one may sit in
    # /usr/bin)
    (mini.bin / "pueue").write_text("#!/bin/sh\nexit 1\n")
    r = mini.run("--docker", "hidden")
    assert r.returncode == 64 and "pueue" in r.stderr
    assert not mini.args_seen.exists() and not mini.out.exists()


def test_a_run_with_the_wrong_pueue_major_is_refused(mini):
    mini.fake_tool("pueued", "3.4.1")
    r = mini.run("--docker", "hidden")
    assert r.returncode == 64 and "pueue" in r.stderr
    assert not mini.args_seen.exists() and not mini.out.exists()


@pytest.mark.parametrize("name", ["conftest.py", "pytest.ini", "tox.ini",
                                  "setup.cfg", "sub/conftest.py"])
def test_an_untracked_pytest_configuration_is_refused(mini, name):
    path = mini.repo / name
    path.parent.mkdir(exist_ok=True)
    path.write_text("[pytest]\naddopts = -k nothing\n")
    r = mini.run("--docker", "hidden")
    assert r.returncode == 64 and name in r.stderr, r.stderr
    assert not mini.args_seen.exists() and not mini.out.exists()


def test_pytest_plugins_in_the_environment_is_refused(mini):
    r = mini.run("--docker", "hidden", env=mini.env(PYTEST_PLUGINS="x"))
    assert r.returncode == 64 and "PYTEST_PLUGINS" in r.stderr
    assert not mini.args_seen.exists() and not mini.out.exists()


def test_the_evidence_commit_holds_only_the_evidence_file(mini):
    mini.evidence.mkdir()
    git(mini.evidence, "init", "-q", "-b", "main")
    (mini.evidence / "unrelated.txt").write_text("staged by someone\n")
    git(mini.evidence, "add", "unrelated.txt")
    r = mini.run("--docker", "hidden", "--evidence-repo", str(mini.evidence),
                 "--author", "T <t@example.invalid>")
    assert r.returncode == 0, r.stdout + r.stderr
    names = subprocess.run(["git", "-C", str(mini.evidence), "show",
                            "--name-only", "--format=", "HEAD"],
                           capture_output=True, text=True).stdout.split()
    assert names == [mini.out.name]


def test_the_tools_exit_codes_are_not_pytests(mini):
    # refused before the run
    assert mini.run("--docker", "hidden", "-k", "x").returncode == 64
    # an evidence repository that cannot take the file
    mini.evidence.mkdir()
    git(mini.evidence, "init", "-q", "-b", "main")
    (mini.evidence / mini.out.name).write_text("already\n")
    r = mini.run("--docker", "hidden", "--evidence-repo", str(mini.evidence))
    assert r.returncode == 67, r.stdout + r.stderr


def test_a_symlinked_out_path_is_refused(mini, tmp_path):
    target = tmp_path / "elsewhere.txt"
    target.write_text("keep\n")
    mini.out.symlink_to(target)
    r = mini.run("--docker", "hidden")
    assert r.returncode == 64
    assert target.read_text() == "keep\n"


def test_an_unwritable_out_directory_exits_with_a_tool_code(mini, tmp_path):
    locked = tmp_path / "locked"
    locked.mkdir()
    locked.chmod(0o500)
    try:
        if os.access(locked, os.W_OK):
            pytest.skip("[root] the directory stays writable for this user")
        mini.out = locked / "gate.txt"
        r = mini.run("--docker", "hidden")
        assert r.returncode == 64 and str(locked) in r.stderr, r.stderr
        assert "Traceback" not in r.stderr
    finally:
        locked.chmod(0o700)


@pytest.mark.parametrize("name", ["pytest.toml", ".pytest.toml",
                                  ".pytest.ini", "sub/pytest.toml"])
def test_an_untracked_pytest_toml_configuration_is_refused(mini, name):
    # pytest 9 reads these before pytest.ini and pyproject.toml
    path = mini.repo / name
    path.parent.mkdir(exist_ok=True)
    path.write_text("[pytest]\ntestpaths = ['nothing']\n")
    r = mini.run("--docker", "hidden")
    assert r.returncode == 64 and name in r.stderr, r.stderr
    assert not mini.args_seen.exists() and not mini.out.exists()
