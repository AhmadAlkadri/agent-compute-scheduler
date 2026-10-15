"""Fingerprints of the backend guard and of a job's inputs.

A queued job records what it would run against; at dispatch the same
fingerprint is recomputed and a mismatch holds the job instead of running
different code.  "HEAD plus a dirty flag" is not enough, so the input
fingerprint hashes the actual uncommitted content:

* ``git rev-parse HEAD`` of the worktree;
* ``git diff --binary HEAD`` restricted to the project's input pathspecs
  (staged and unstaged tracked changes);
* every untracked, non-ignored file under those pathspecs, by content;
* every argv operand that names an existing regular file (scripts outside
  the worktree, such as ``/tmp/chain.sh``, included), by content;
* any extra ``--input`` paths, by content.
"""

from __future__ import annotations

import hashlib
import os
import stat
import subprocess
from pathlib import Path

from .fsprobe import (is_dir_strict, is_file_strict, is_symlink_strict,
                      listdir_strict, stat_strict)

MAX_OPERAND_BYTES = 256 * 1024 * 1024


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _git(worktree: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "-C", str(worktree), *args],
                          capture_output=True, timeout=120)


def checkout_state(worktree: Path) -> tuple[str | None, bool | None]:
    """G3b (code consistency): a checkout's HEAD and whether its tracked
    files differ from it (untracked files are not compared), read without
    optional locks (no index refresh); None where git cannot tell."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env["GIT_OPTIONAL_LOCKS"] = "0"

    def run(*args: str):
        try:
            return subprocess.run(["git", "--no-optional-locks", "-c",
                                   "core.fsmonitor=false", "-C",
                                   str(worktree), *args],
                                  capture_output=True, text=True, timeout=60,
                                  env=env, stdin=subprocess.DEVNULL)
        except (OSError, subprocess.SubprocessError):
            return None
    head = run("rev-parse", "HEAD")
    if head is None or head.returncode != 0 or not head.stdout.strip():
        return None, None
    status = run("status", "--porcelain", "--untracked-files=no")
    dirty = bool(status.stdout.strip()) if status is not None and \
        status.returncode == 0 else None
    return head.stdout.strip(), dirty


def code_problem(code: dict | None, worktree, where: str) -> str | None:
    """Why a job's checkout does not meet its code requirement (G3b:
    ``--require-commit``/``--require-clean``), or None.  ``code``: the
    manifest's ``code`` with the checkout's current ``head`` and ``dirty``
    (None: unreadable, which refuses)."""
    if not isinstance(code, dict):
        return None
    required = code.get("required_commit")
    if required and code.get("head") != required:
        return (f"code consistency: the checkout {worktree} {where} is at "
                f"{code.get('head') or 'an unreadable HEAD'}, not the "
                f"required {required} (--require-commit: the placing "
                "host's checkout)")
    if code.get("require_clean") and code.get("dirty") is not False:
        return (f"code consistency: the checkout {worktree} {where} " +
                ("has uncommitted changes to tracked files"
                 if code.get("dirty") else
                 "cannot be read for uncommitted changes") +
                " (--require-clean: the placing host's checkout is clean)")
    return None


def guard_identity(guard: Path) -> dict:
    """Location, content hash and source revision of the backend guard."""
    info = {"path": str(guard), "exists": guard.is_file()}
    if not guard.is_file():
        return info
    info["sha256"] = sha256_file(guard)
    repo = guard.parent
    rev = _git(repo, "log", "-1", "--format=%H", "--", guard.name)
    if rev.returncode == 0:
        info["last_commit"] = rev.stdout.decode().strip() or None
        dirty = _git(repo, "status", "--porcelain", "--", guard.name)
        info["dirty"] = bool(dirty.stdout.strip())
    return info


def _walk_files(top: Path):
    """``os.walk(top)``'s files, sorted by directory then name, but a
    directory that cannot be listed raises (``os.walk`` skips it, so its
    files would not be in the fingerprint; A49)."""
    names = listdir_strict(top)
    # lstat first (K6 review): a symlink is never followed (os.walk does not
    # descend into one and the caller never hashes one), so a link whose
    # target cannot be examined must not refuse the walk
    dirs = [n for n in names if _real_dir(top / n)]
    yield top, [n for n in names if n not in dirs]
    for name in dirs:
        yield from _walk_files(top / name)


def _real_dir(path: Path) -> bool:
    """A directory that is not a symlink (lstat only; absence: False)."""
    found = stat_strict(path, follow=False)
    return found is not None and stat.S_ISDIR(found.st_mode)


def _hash_tree(path: Path, digest, label: str) -> list[str]:
    """Hash a file or a tree; an input that cannot be examined raises
    ``OSError`` (A49: it is never recorded as "missing", which an
    unreadable input would then equal at submit and at dispatch)."""
    seen = []
    if is_file_strict(path):
        digest.update(f"{label}\0{sha256_file(path)}\n".encode())
        seen.append(label)
    elif is_dir_strict(path):
        for root, files in _walk_files(path):
            for name in sorted(files):
                full = Path(root) / name
                if not is_symlink_strict(full) and is_file_strict(full):
                    rel = f"{label}/{full.relative_to(path)}"
                    digest.update(f"{rel}\0{sha256_file(full)}\n".encode())
                    seen.append(rel)
    else:
        digest.update(f"{label}\0missing\n".encode())
        seen.append(f"{label} (missing)")
    return seen


def inputs_fingerprint(worktree: Path, cwd: Path, argv: list[str],
                       pathspecs: list[str],
                       extra_inputs: list[str]) -> dict:
    digest = hashlib.sha256()
    record: dict = {"worktree": str(worktree), "pathspecs": pathspecs}
    head = _git(worktree, "rev-parse", "HEAD")
    if head.returncode == 0:
        record["git_head"] = head.stdout.decode().strip()
        diff = _git(worktree, "diff", "--binary", "HEAD", "--", *pathspecs)
        if diff.returncode != 0:
            raise RuntimeError(f"git diff failed in {worktree}: "
                               f"{diff.stderr.decode()[:300]}")
        record["diff_sha256"] = sha256_bytes(diff.stdout)
        record["diff_bytes"] = len(diff.stdout)
        untracked = _git(worktree, "ls-files", "-z", "--others",
                         "--exclude-standard", "--", *pathspecs)
        if untracked.returncode != 0:
            raise RuntimeError(f"git ls-files failed in {worktree}")
        names = sorted(n for n in untracked.stdout.decode().split("\0") if n)
        u_digest = hashlib.sha256()
        for name in names:
            path = worktree / name
            if not is_symlink_strict(path) and is_file_strict(path):
                u_digest.update(f"{name}\0{sha256_file(path)}\n".encode())
        record["untracked_count"] = len(names)
        record["untracked_sha256"] = u_digest.hexdigest()
        digest.update(f"head\0{record['git_head']}\ndiff\0"
                      f"{record['diff_sha256']}\nuntracked\0"
                      f"{record['untracked_sha256']}\n".encode())
    else:
        record["git_head"] = None
        record["note"] = "not a git worktree: only operands and --input hashed"
    operands = []
    candidates = []
    for arg in argv:
        candidates.append(arg)
        if arg.startswith("-") and "=" in arg:
            candidates.append(arg.split("=", 1)[1])     # --config=path
    for arg in candidates:
        if not arg:
            continue
        candidate = Path(arg) if os.path.isabs(arg) else cwd / arg
        try:
            if is_file_strict(candidate) and \
                    candidate.stat().st_size <= MAX_OPERAND_BYTES:
                operands.append(arg)
                digest.update(f"operand\0{arg}\0"
                              f"{sha256_file(candidate)}\n".encode())
        except OSError:
            continue
    record["operands"] = operands
    extras = []
    for item in extra_inputs:
        path = Path(item) if os.path.isabs(item) else cwd / item
        extras += _hash_tree(path, digest, f"input:{item}")
    record["extra_inputs"] = extras
    record["sha256"] = digest.hexdigest()
    return record
