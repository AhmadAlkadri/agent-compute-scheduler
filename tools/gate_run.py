#!/usr/bin/env python3
"""Run the full test suite and write gate evidence for `agent-compute pin-backend`.

    python3 tools/gate_run.py --label <host label> --docker hidden|with \\
        --out <file> [--python <interpreter>] [--evidence-repo <dir>] \\
        [--author "Name <email>"]

It runs `<python> -m pytest -v -rs -p no:cacheprovider` in the repository the
tool lives in, with no path, no `-k`, no `-m` and no other pytest argument
(any extra argument is refused, and so is `PYTEST_ADDOPTS` in the
environment), and refuses a tree with tracked changes.  The file it writes
has the gate evidence format:

1. the header (written BEFORE the run, rewritten with the redaction count
   after it): `# <label> full suite on <label>, WITH Docker (image ...) |
   DOCKER HIDDEN (...); profile standard|legacy; source sha <40 hex>; engine
   sha256 <64 hex>; pueue <v>; Python <v> (<interpreter>); tracked changes:
   0; redactions: <n>; written by tools/gate_run.py <format>; <UTC time>`;
2. `# ` and the output of `tools/skip_report.py --summary-line`;
3. the pytest output;
4. `EXIT <code>`.

`<label>` must be the `host` of the machine config that will pin (`agent-compute
doctor` prints it).  `--docker hidden` runs with `docker` removed from PATH
(a temporary directory of symlinks stands in for each PATH directory that holds
one; nothing is uninstalled or changed) and the Docker-backed tests skip with
a counted `[docker]` reason.  `--docker with` needs a Docker daemon that
answers and the test image (`ACS_TEST_IMAGE` or the public default of
`tests/conftest.py`) present for its platform (or `ACS_TEST_PULL=1`); it
records the image, and a run that still skipped Docker tests is reported as
unresolved evidence (exit 5, not committed).

Redaction (audit prevention (b)): before the final file is written, the value
of any entry `NAME=value`, `'NAME': 'value'` or `"NAME": "value"` whose NAME
matches TOKEN, KEY, SECRET, PASSWORD, PASSWD, AUTH, CREDENTIAL, COOKIE or
SESSION (case-insensitive) becomes `<redacted by gate_run>`, and so does any
occurrence of the value of such an environment variable of this process (8
characters or more).  The count is in the header; values are never printed.
It is a mistake-catcher, not a scanner: a secret in another shape (a bearer
header, a value without a sensitive name) is not found; read the file before
sharing it.

`--evidence-repo <dir>` initializes a local git repository there if there is
none (never a remote), copies the file in and commits it (author from
`--author`, else the repository's or global git config, else a placeholder);
the commit id is printed (only that file is committed).  Nothing is committed
unless the run exited 0, the skip accounting is `consistent` with `untagged 0`,
HEAD, the engine and the tree stayed as they were, no Pueue test skipped and,
with `--docker with`, no Docker test skipped.  Pueue and pueued 4.x must be on
PATH, and an untracked pytest.ini, .pytest.ini, pytest.toml, .pytest.toml,
conftest.py, tox.ini or setup.cfg (any directory), or a set
PYTEST_PLUGINS, is refused (each could change what runs).

Exit status: pytest's own (0-5) when nothing else went wrong; the tool's own
codes are distinct from pytest's: 64 refused before the run (usage, dirty tree,
untracked pytest configuration, environment, no Pueue 4, Docker, existing or
unwritable output); 65 the tree, HEAD or the engine changed during the run (the
file is written with a `# gate_run: REFUSED` line and `EXIT 65`, never
committed); 66 the run finished but its evidence is unresolved (not committed);
67 the evidence repository could not take the file; 68 the evidence file could
not be written after the run.  Standard library only.
"""

from __future__ import annotations

import argparse
import ast
import datetime
import hashlib
import importlib.util
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

FORMAT = 1                       # the header's `written by tools/gate_run.py <n>`
REDACTED = "<redacted by gate_run>"
# the tool's own exit codes are distinct from pytest's (0-5)
EXIT_REFUSED, EXIT_CHANGED, EXIT_UNRESOLVED, EXIT_REPO, EXIT_WRITE = \
    64, 65, 66, 67, 68
PYTEST_CONFIG_FILES = ("pytest.ini", ".pytest.ini", "pytest.toml",
                      ".pytest.toml", "conftest.py", "tox.ini", "setup.cfg")
PYTEST_ARGS = ("-m", "pytest", "-v", "-rs", "-p", "no:cacheprovider")
SENSITIVE = re.compile(r"TOKEN|KEY|SECRET|PASSWORD|PASSWD|AUTH|CREDENTIAL|"
                       r"COOKIE|SESSION", re.I)
LABEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_NAME = r"[A-Za-z_][A-Za-z0-9_]*"
_ASSIGN = re.compile(
    rf"(?P<name>\b{_NAME})=(?!<redacted by gate_run>)"
    r"(?P<value>\"[^\"\n]*\"|'[^'\n]*'|[^\s'\"]+)")
_DICT = re.compile(
    rf"(?P<q1>['\"])(?P<name>{_NAME})(?P=q1)(?P<sep>\s*:\s*)"
    r"(?P<q2>['\"])(?P<value>(?:\\.|(?!(?P=q2))[^\\\n])*)(?P=q2)")
MIN_ENV_VALUE = 8
ROOT = Path(__file__).resolve().parent.parent
ENGINE = Path("src/agent_compute/guard/engine.py")
CONFTEST = Path("tests/conftest.py")


class Refused(Exception):
    def __init__(self, message: str, code: int = EXIT_REFUSED):
        super().__init__(message)
        self.code = code


def fail(message: str) -> None:
    raise Refused(message)


def utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


# --- redaction ----------------------------------------------------------------------------

def redact(text: str, environ=None) -> tuple[str, int]:
    """``text`` with sensitive values replaced, and how many were."""
    count = 0
    environ = os.environ if environ is None else environ
    values = sorted({v for k, v in environ.items()
                     if SENSITIVE.search(k) and len(v) >= MIN_ENV_VALUE},
                    key=len, reverse=True)
    for value in values:
        count += text.count(value)
        text = text.replace(value, REDACTED)

    def dict_sub(match):
        nonlocal count
        if not SENSITIVE.search(match.group("name")) or \
                match.group("value") == REDACTED:
            return match.group(0)
        count += 1
        q1, q2 = match.group("q1"), match.group("q2")
        return f"{q1}{match.group('name')}{q1}{match.group('sep')}" \
               f"{q2}{REDACTED}{q2}"

    def assign_sub(match):
        nonlocal count
        if not SENSITIVE.search(match.group("name")):
            return match.group(0)
        count += 1
        return f"{match.group('name')}={REDACTED}"

    text = _DICT.sub(dict_sub, text)
    text = _ASSIGN.sub(assign_sub, text)
    return text, count


# --- helpers ------------------------------------------------------------------------------

def git(repo: Path, *args: str, env=None) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "-C", str(repo), *args],
                          capture_output=True, text=True, timeout=120,
                          env=env)


def tracked_changes(repo: Path) -> list[str]:
    status = git(repo, "status", "--porcelain", "--untracked-files=no")
    if status.returncode:
        fail(f"git cannot inspect {repo}: {status.stderr.strip()}")
    return [line for line in status.stdout.splitlines() if line.strip()]


def untracked_pytest_config(repo: Path) -> list[str]:
    """Untracked files that could change what pytest runs."""
    status = git(repo, "status", "--porcelain", "--untracked-files=all")
    if status.returncode:
        fail(f"git cannot inspect {repo}: {status.stderr.strip()}")
    found = []
    for line in status.stdout.splitlines():
        if line.startswith("?? "):
            name = line[3:].strip().strip('"')
            if os.path.basename(name) in PYTEST_CONFIG_FILES:
                found.append(name)
    return found


def snapshot(repo: Path) -> dict:
    """What a run must not change: HEAD, the engine, the tracked files and
    the pytest configuration files."""
    head = git(repo, "rev-parse", "HEAD")
    if head.returncode:
        fail(f"git cannot read HEAD of {repo}: {head.stderr.strip()}")
    try:
        engine = hashlib.sha256((repo / ENGINE).read_bytes()).hexdigest()
    except OSError as error:
        fail(f"cannot read {repo / ENGINE}: {error}")
    return {"head": head.stdout.strip(), "engine": engine,
            "dirty": tracked_changes(repo),
            "config": untracked_pytest_config(repo)}


def changes_since(before: dict, repo: Path) -> list[str]:
    """How the repository differs from ``before`` now (empty if it does not)."""
    try:
        now = snapshot(repo)
    except Refused as error:
        return [f"unreadable after the run ({error})"]
    out = []
    if now["head"] != before["head"]:
        out.append(f"HEAD {before['head'][:12]} -> {now['head'][:12]}")
    if now["engine"] != before["engine"]:
        out.append(f"engine sha256 {before['engine'][:12]} -> "
                   f"{now['engine'][:12]}")
    if now["dirty"]:
        out.append(f"{len(now['dirty'])} tracked file(s) changed")
    if now["config"]:
        out.append(f"untracked pytest configuration {now['config']}")
    return out


def pueue_version(environ) -> str:
    """``pueue 4.x.y`` (and a Pueue daemon of major 4 on PATH), else refuse:
    the standard profile's queue-mode tests need both."""
    found = []
    for name in ("pueue", "pueued"):
        path = shutil.which(name, path=environ.get("PATH"))
        text = version_of([path, "--version"], "") if path else ""
        match = re.match(rf"{name} v?(\d+)\.(\d+)(?:\.(\d+))?", text)
        if not match or match.group(1) != "4":
            fail(f"{name} 4.x is not on PATH (found: "
                 f"{text or 'nothing'}): the standard profile needs Pueue 4")
        found.append(text.split()[1].lstrip("v"))
    return f"pueue {found[0]}"


def write_new(path: Path, text: str, code: int) -> None:
    """Create ``path`` (never an existing file, never through a symlink)."""
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                     getattr(os, "O_NOFOLLOW", 0), 0o644)
        with os.fdopen(fd, "w") as handle:
            handle.write(text)
    except OSError as error:
        raise Refused(f"cannot create {path}: {error}", code) from None


def load_skip_report():
    path = ROOT / "tools" / "skip_report.py"
    spec = importlib.util.spec_from_file_location("gate_skip_report", path)
    if spec is None or spec.loader is None or not path.exists():
        fail(f"{path} is missing: the skip accounting cannot run")
    module = importlib.util.module_from_spec(spec)
    sys.modules["gate_skip_report"] = module
    spec.loader.exec_module(module)
    return module


def version_of(command: list[str], default: str) -> str:
    try:
        done = subprocess.run(command, capture_output=True, text=True,
                              timeout=30)
    except (OSError, subprocess.SubprocessError):
        return default
    text = (done.stdout or done.stderr).strip()
    return text.splitlines()[0] if done.returncode == 0 and text else default


def public_image() -> str:
    """The harness's default test image (``PUBLIC_IMAGE`` of
    tests/conftest.py), read without importing it."""
    try:
        tree = ast.parse((ROOT / CONFTEST).read_text())
    except (OSError, SyntaxError):
        fail(f"{ROOT / CONFTEST} cannot be read: the test image is unknown")
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "PUBLIC_IMAGE"
                for t in node.targets):
            try:
                return str(ast.literal_eval(node.value))
            except ValueError:
                break
    fail("PUBLIC_IMAGE is not defined in tests/conftest.py")


# --- Docker -------------------------------------------------------------------------------

def hide_docker(path: str, shim_root: Path) -> str:
    """``path`` with ``docker`` unreachable: each directory that holds a
    ``docker`` entry is replaced by a directory of symlinks to its other
    entries.  Nothing in the real directories is touched."""
    parts = []
    for index, folder in enumerate(path.split(os.pathsep)):
        if folder and os.path.lexists(os.path.join(folder, "docker")):
            shim = shim_root / str(index)
            shim.mkdir()
            try:
                for name in os.listdir(folder):
                    if name != "docker":
                        os.symlink(os.path.join(folder, name), shim / name)
            except OSError as error:
                fail(f"cannot hide docker from {folder}: {error}")
            parts.append(str(shim))
        else:
            parts.append(folder)
    result = os.pathsep.join(parts)
    if shutil.which("docker", path=result):
        fail("docker is still on PATH after hiding it")
    return result


def docker_problem(image: str, environ) -> str | None:
    """Why ``--docker with`` cannot hold (None if it can): the harness's own
    test (conftest ``docker_image_status``) in short."""
    docker = shutil.which("docker", path=environ.get("PATH"))
    if not docker:
        return "no docker CLI on PATH"

    def run(*args):
        try:
            return subprocess.run([docker, *args], capture_output=True,
                                  text=True, timeout=60)
        except (OSError, subprocess.SubprocessError) as error:
            return subprocess.CompletedProcess(args, 1, "", str(error))

    server = run("version", "--format", "{{.Server.Os}}/{{.Server.Arch}}")
    platform = server.stdout.strip()
    if server.returncode or not platform:
        return "the docker daemon does not answer"
    if environ.get("ACS_TEST_PULL") == "1":
        return None
    have = run("image", "inspect", "--format", "{{.Os}}/{{.Architecture}}",
               image)
    if have.returncode or have.stdout.strip() != platform:
        return (f"test image {image} is not present for {platform} "
                "(pull it, or set ACS_TEST_PULL=1)")
    return None


# --- the run ------------------------------------------------------------------------------

def build_header(args, *, docker: str, profile: str, source: str, engine: str,
                 pueue: str, python_version: str, redactions: str,
                 started: str) -> str:
    return (f"# {args.label} full suite on {args.label}, {docker}; profile "
            f"{profile}; source sha {source}; engine sha256 {engine}; "
            f"{pueue}; Python {python_version} ({args.python}); tracked "
            f"changes: 0; redactions: {redactions}; written by "
            f"tools/gate_run.py {FORMAT}; {started}")


def parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the full suite and write gate evidence.",
        allow_abbrev=False)
    parser.add_argument("--label", required=True,
                        help="the host label (the machine config's `host`)")
    parser.add_argument("--out", required=True, type=Path,
                        help="the evidence file to write (must not exist)")
    parser.add_argument("--docker", required=True, choices=("hidden", "with"))
    parser.add_argument("--python", default=sys.executable,
                        help="the interpreter that runs pytest")
    parser.add_argument("--evidence-repo", type=Path, default=None)
    parser.add_argument("--author", default=None,
                        help='"Name <email>" for the evidence commit')
    args, extra = parser.parse_known_args(argv)
    if extra:
        fail(f"no pytest argument is accepted ({' '.join(extra)}): the gate "
             "is the whole suite, with no path and no -k/-m filter")
    if not LABEL.fullmatch(args.label):
        fail(f"--label {args.label!r}: use letters, digits, '.', '_' and "
             "'-' (the machine config's host)")
    return args


def evidence_commit(args, out: Path) -> str:
    repo = args.evidence_repo
    try:
        repo.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise Refused(f"cannot create {repo}: {error}", EXIT_REPO) from None
    inside = git(repo, "rev-parse", "--show-toplevel")
    if inside.returncode or Path(inside.stdout.strip()).resolve() != \
            repo.resolve():
        made = subprocess.run(["git", "-C", str(repo), "init", "-q", "-b",
                               "main"], capture_output=True, text=True)
        if made.returncode:
            raise Refused(f"cannot initialize {repo}: {made.stderr.strip()}",
                          EXIT_REPO)
    target = repo / out.name
    if os.path.lexists(target):
        raise Refused(f"{target} exists: evidence is written once",
                      EXIT_REPO)
    try:
        write_new(target, out.read_text(), EXIT_REPO)
    except OSError as error:
        raise Refused(f"cannot read {out}: {error}", EXIT_REPO) from None
    identity = []
    if args.author:
        match = re.fullmatch(r"\s*(.+?)\s*<([^<>\s]+)>\s*", args.author)
        if not match:
            raise Refused('--author must look like "Name <email>"',
                          EXIT_REPO)
        identity = ["-c", f"user.name={match.group(1)}",
                    "-c", f"user.email={match.group(2)}"]
    elif not git(repo, "config", "user.name").stdout.strip() or \
            not git(repo, "config", "user.email").stdout.strip():
        identity = ["-c", "user.name=gate_run", "-c",
                    "user.email=gate_run@localhost"]
    for step in (["add", "--", target.name],
                 [*identity, "-c", "commit.gpgsign=false", "commit", "-q",
                  "--no-verify", "-m", f"gate evidence: {target.name}",
                  "--", target.name]):
        done = git(repo, *step)
        if done.returncode:
            raise Refused(f"git {step[-1] if step[0] == 'add' else 'commit'}"
                          f" failed in {repo}: {done.stderr.strip()}",
                          EXIT_REPO)
    return git(repo, "rev-parse", "HEAD").stdout.strip()


def main(argv: list[str] | None = None) -> int:
    try:
        return run(parse(sys.argv[1:] if argv is None else argv))
    except Refused as error:
        print(f"gate_run: {error}", file=sys.stderr)
        return error.code
    except OSError as error:
        print(f"gate_run: I/O error: {error}", file=sys.stderr)
        return EXIT_WRITE


def run(args: argparse.Namespace) -> int:
    for name in ("PYTEST_ADDOPTS", "PYTEST_PLUGINS"):
        if os.environ.get(name):
            fail(f"{name} is set in the environment and could change what "
                 "runs; unset it")
    top = git(ROOT, "rev-parse", "--show-toplevel")
    if top.returncode or Path(top.stdout.strip()).resolve() != ROOT.resolve():
        fail(f"{ROOT} is not the top of a git repository")
    before = snapshot(ROOT)
    if before["dirty"]:
        fail(f"the tree has uncommitted tracked changes "
             f"({len(before['dirty'])}; first: {before['dirty'][0]!r}): "
             "commit or discard them, the evidence names one commit")
    if before["config"]:
        fail(f"untracked pytest configuration in the repository "
             f"({', '.join(before['config'])}): remove it or commit it; it "
             "could change what runs")
    source, engine = before["head"], before["engine"]
    args.out = args.out.expanduser().absolute()
    if os.path.lexists(args.out):
        fail(f"{args.out} exists: evidence is written once")
    args.python = str(Path(args.python).expanduser().absolute()) \
        if os.sep in args.python else (shutil.which(args.python) or
                                       args.python)
    python_version = version_of([args.python, "--version"], "")
    if not python_version:
        fail(f"{args.python} does not answer --version")
    python_version = python_version.removeprefix("Python ").strip()
    skip_report = load_skip_report()

    environ = dict(os.environ)
    scratch = Path(tempfile.mkdtemp(prefix="gate-run-"))
    try:
        if args.docker == "hidden":
            (scratch / "path").mkdir()
            environ["PATH"] = hide_docker(environ.get("PATH", ""),
                                          scratch / "path")
            docker = ("DOCKER HIDDEN (docker removed from PATH by "
                      "tools/gate_run.py; nothing uninstalled; the "
                      "Docker-backed tests skip with a counted [docker] "
                      "reason)")
        else:
            image = environ.get("ACS_TEST_IMAGE") or public_image()
            why = docker_problem(image, environ)
            if why:
                fail(f"--docker with: {why}")
            docker = f"WITH Docker (image {image})"
        profile = "legacy" if environ.get("ACS_TEST_LEGACY_GUARD") \
            else "standard"
        pueue = pueue_version(environ)
        started = utc_now()
        fields = dict(docker=docker, profile=profile, source=source,
                      engine=engine, pueue=pueue,
                      python_version=python_version, started=started)
        pending = build_header(args, redactions="pending", **fields)
        try:
            args.out.parent.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            fail(f"cannot create {args.out.parent}: {error}")
        write_new(args.out, pending + "\n", EXIT_REFUSED)  # before the run
        raw = scratch / "raw.txt"
        with open(raw, "wb") as handle:
            done = subprocess.run([args.python, *PYTEST_ARGS], cwd=ROOT,
                                  env=environ, stdout=handle,
                                  stderr=subprocess.STDOUT,
                                  stdin=subprocess.DEVNULL)
        code = done.returncode
        changed = changes_since(before, ROOT)
        body = raw.read_text(errors="replace")
        if body and not body.endswith("\n"):
            body += "\n"
        body, count = redact(body)
        header, header_count = redact(build_header(
            args, redactions=str(0), **fields))
        header = header.replace("redactions: 0;",
                                f"redactions: {count + header_count};", 1)
        report = skip_report.parse(body)
        line = "# " + skip_report.summary_line(report)
        trailer = ""
        if changed:
            trailer = ("# gate_run: REFUSED, the repository changed during "
                       "the run (" + "; ".join(changed) + ")\n")
        final = args.out.with_name(args.out.name + ".tmp")
        write_new(final, f"{header}\n{line}\n{body}{trailer}EXIT "
                  f"{EXIT_CHANGED if changed else code}\n", EXIT_WRITE)
        try:
            os.replace(final, args.out)
        except OSError as error:
            raise Refused(f"cannot write {args.out}: {error}",
                          EXIT_WRITE) from None
    finally:
        shutil.rmtree(scratch, ignore_errors=True)

    print(f"gate_run: {args.out} written; {line[2:]}; EXIT {code}; "
          f"redactions {count + header_count}")
    if changed:
        print("gate_run: the repository changed during the run, so this is "
              "not evidence of one commit (" + "; ".join(changed) + "); the "
              "file is marked REFUSED and was not committed",
              file=sys.stderr)
        return EXIT_CHANGED
    unresolved = []
    if code:
        unresolved.append(f"pytest exited {code}")
    if not report["consistent"] or report["by_category"].get("untagged"):
        unresolved.append("the skip accounting is not `consistent` with "
                          "`untagged 0`")
    if report["by_category"].get("pueue"):
        unresolved.append(f"{report['by_category']['pueue']} Pueue tests "
                          "skipped (the standard profile needs Pueue)")
    if args.docker == "with" and report["by_category"].get("docker"):
        unresolved.append(f"WITH Docker was declared but "
                          f"{report['by_category']['docker']} Docker tests "
                          "skipped")
    if code:
        return code
    if unresolved:
        print("gate_run: unresolved evidence, not committed: "
              + "; ".join(unresolved), file=sys.stderr)
        return EXIT_UNRESOLVED
    if args.evidence_repo is not None:
        try:
            print(f"gate_run: committed {args.out.name} in "
                  f"{args.evidence_repo}: {evidence_commit(args, args.out)}")
        except Refused as error:
            print(f"gate_run: {error}", file=sys.stderr)
            return error.code
    return 0


if __name__ == "__main__":
    sys.exit(main())
