"""``pyproject.toml`` describes the package that ``src/agent_compute`` is.

What this establishes: the project version in ``pyproject.toml`` equals
``agent_compute.__version__`` (there is no build backend that could derive
one from the other, so a release bump of one without the other fails here),
and the description is present and names no project of its own origin.
No harness, no guard, no Docker.
"""
from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))


def _pyproject() -> dict:
    return tomllib.loads((REPO / "pyproject.toml").read_text())["project"]


def test_pyproject_version_equals_the_package_version():
    import agent_compute
    assert _pyproject()["version"] == agent_compute.__version__


def test_the_version_is_a_plain_release_number():
    # D1 of the release plan: alpha is a status, not a version suffix.
    assert re.fullmatch(r"\d+\.\d+\.\d+", _pyproject()["version"])


def test_the_description_says_what_the_tool_is():
    description = _pyproject()["description"]
    assert description.startswith("Agent-only admission")
    assert "guard" in description and "Pueue" in description
