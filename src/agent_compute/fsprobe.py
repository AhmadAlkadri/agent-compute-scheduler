"""Filesystem probes that never read an unreadable path as an absent one.

``pathlib.Path.exists``, ``is_dir``, ``is_file`` and ``is_symlink`` answer
``False`` for *any* ``OSError`` since Python 3.14 (register A49; on 3.12 only
``ENOENT``/``ENOTDIR``/``EBADF``/``ELOOP`` are swallowed and a
``PermissionError`` propagates), and ``Path.glob``/``rglob``/``os.walk`` skip
an unreadable directory on every version.  A safety decision that takes
"False" to mean "nothing there" (no unfinished job, no pending task, no
cancel request, no lock user) is then fail-open exactly when the path cannot
be examined.

These helpers use ``os.stat``/``os.lstat``/``os.scandir`` directly and are
identical on every supported Python:

* absence is ``ENOENT`` or ``ENOTDIR`` and nothing else;
* any other ``OSError`` (``EACCES``, ``EIO``, ``ELOOP``, ``ENAMETOOLONG``,
  ...) propagates unchanged, and the caller turns it into a refusal, an
  "unverified", a block or an error row;
* a path that cannot exist at all (an embedded NUL) is ``EINVAL``, never
  absent.

``tests/test_py314_pathlib.py`` fails if a decision module calls the raw
pathlib predicates again outside its reviewed allow-list.
"""

from __future__ import annotations

import errno
import fnmatch
import os
import stat
from pathlib import Path

ABSENT = (errno.ENOENT, errno.ENOTDIR)


def _check(path) -> str:
    text = os.fspath(path)
    if isinstance(text, bytes):
        text = os.fsdecode(text)
    if "\0" in text:
        raise OSError(errno.EINVAL, "embedded null byte in path", text)
    return text


def stat_strict(path, *, follow: bool = True) -> os.stat_result | None:
    """``os.stat`` (``os.lstat`` when ``follow`` is False) of ``path``;
    None when it is absent (ENOENT/ENOTDIR); any other ``OSError``
    propagates."""
    text = _check(path)
    try:
        return os.stat(text) if follow else os.lstat(text)
    except OSError as error:
        if error.errno in ABSENT:
            return None
        raise


def exists_strict(path) -> bool:
    """``Path.exists`` (follows links) where only absence is False."""
    return stat_strict(path) is not None


def lexists_strict(path) -> bool:
    """Whether anything is at ``path``, a dangling link included."""
    return stat_strict(path, follow=False) is not None


def is_dir_strict(path) -> bool:
    found = stat_strict(path)
    return found is not None and stat.S_ISDIR(found.st_mode)


def is_file_strict(path) -> bool:
    found = stat_strict(path)
    return found is not None and stat.S_ISREG(found.st_mode)


def is_symlink_strict(path) -> bool:
    found = stat_strict(path, follow=False)
    return found is not None and stat.S_ISLNK(found.st_mode)


def listdir_strict(path) -> list[str]:
    """The sorted entry names of directory ``path``; ``[]`` when it is
    absent (ENOENT/ENOTDIR: use ``is_dir_strict`` to tell that from an empty
    one); any other ``OSError`` (a directory that cannot be listed)
    propagates, where ``Path.glob`` and ``os.walk`` would skip it."""
    text = _check(path)
    try:
        with os.scandir(text) as entries:
            return sorted(entry.name for entry in entries)
    except OSError as error:
        if error.errno in ABSENT:
            return []
        raise


def glob_strict(directory, pattern: str) -> list[Path]:
    """``sorted(Path(directory).glob(pattern))`` for a pattern without a
    separator, that does not skip an unreadable directory."""
    base = Path(directory)
    return [base / name for name in listdir_strict(base)
            if fnmatch.fnmatchcase(name, pattern)]


def walk_files_strict(root) -> list[Path]:
    """Every non-directory path under directory ``root`` (symlinks are
    listed, not followed), sorted; ``[]`` when ``root`` is absent.  A
    directory that cannot be listed raises (``Path.rglob``/``os.walk``
    skip it)."""
    base = Path(root)
    found: list[Path] = []
    for name in listdir_strict(base):
        path = base / name
        info = stat_strict(path, follow=False)
        if info is None:
            continue
        if stat.S_ISDIR(info.st_mode):
            found.extend(walk_files_strict(path))
        else:
            found.append(path)
    return found
