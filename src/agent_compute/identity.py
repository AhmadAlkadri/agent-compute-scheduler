"""Version identity of the running ACS (campaign G decision D10).

An installed copy runs from ``<prefix>/versions/<version>/src/agent_compute``:
its version is the directory's name (``<package version>-<sha10>``) and its
source sha the ``SOURCE`` file ``install.py`` writes beside ``VERSION`` (an
older version dir has none: the name's sha10 is all that is known).  A
checkout (the tests, development) reports ``<package version>+checkout``
and the checkout's ``HEAD``.  Read-only; cached per process.
"""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

from . import __version__, safety

_CACHE: dict = {}


def identity_of(version_dir: Path) -> dict:
    """The identity of an installed version directory."""
    version_dir = Path(version_dir)
    name = version_dir.name
    sha = None
    try:
        text = (version_dir / "SOURCE").read_text().strip()
        if len(text) == 40 and not text.strip("0123456789abcdef"):
            sha = text
    except OSError:
        pass
    if sha is None:
        parts = name.split("-")
        if len(parts) >= 2 and len(parts[1]) == 10 and \
                not parts[1].strip("0123456789abcdef"):
            sha = parts[1]
    return {"version": name, "source_sha": sha,
            "package_version": __version__}


def _checkout_sha() -> str | None:
    root = Path(__file__).resolve().parents[2]
    try:
        out = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"],
                             capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return None
    sha = out.stdout.strip()
    return sha if out.returncode == 0 and len(sha) == 40 else None


def identity() -> dict:
    """``{"version", "source_sha", "package_version"}`` of this code."""
    if "identity" not in _CACHE:
        found = safety.installation()
        if found is not None:
            _CACHE["identity"] = identity_of(found[1])
        else:
            _CACHE["identity"] = {"version": f"{__version__}+checkout",
                                  "source_sha": _checkout_sha(),
                                  "package_version": __version__}
    return dict(_CACHE["identity"])


def engine_sha256() -> str | None:
    """sha256 of this version's internal guard engine."""
    if "engine" not in _CACHE:
        path = Path(__file__).resolve().parent / "guard" / "engine.py"
        try:
            _CACHE["engine"] = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError:
            _CACHE["engine"] = None
    return _CACHE["engine"]


def running_safety_level() -> int | None:
    """The safety level of the version this code runs from (a checkout:
    the tree's ``SAFETY_LEVEL``)."""
    found = safety.installation()
    if found is None:
        return safety.SAFETY_LEVEL
    return safety.safety_level(found[1])
