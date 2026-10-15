"""The skill and the shipped docs name no GitHub repository of this project
(campaign PA, slice N1).

Why: the repository is renamed (slice N2) and its old name is later reused
by a different, public repository.  A sentence that tells an agent to fix
ACS "upstream at <owner>/agent-compute-scheduler" would then send private
fixes to the wrong place, and every installed version carries the skill.
The development repository is named by the operator in the consuming
project's ``.agents/compute-scheduler.md`` instead.

What this establishes: no file of the skill (``skill/**``), the shipped docs
or the tools' and package's sources contains an ``owner/agent-compute-scheduler``
slug (with or without a suffix), a ``github.com/<owner>/agent-compute-scheduler``
URL, or a local checkout path ``.../GitHub/agent-compute-scheduler``; the
skill and the consumer template contain no ``/home/<name>/`` or
macOS user-home path.  The patterns are generic: this file names no
account and no private slug.  The skill's own directory
(``skill/agent-compute-scheduler/``, ``.../skills/agent-compute-scheduler``)
is an installed path of the product, not a repository, and is allowed.
No harness, no guard, no Docker.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

SHIPPED_DOCS = [
    "README.md",
    "docs/architecture.md",
    "docs/execution-paths.md",
    "docs/memory-model.md",
    "docs/consumer-template.md",
    "docs/operating-model.md",
    "docs/dogfood-log-template.md",
]

# owner/agent-compute-scheduler[-private]; the owner token may be anything
# (an account, "GitHub" in a checkout path).
SLUG = re.compile(r"(?<![A-Za-z0-9_.-])([A-Za-z0-9][A-Za-z0-9-]*)/agent-compute-scheduler(?:-private)?\b")
URL = re.compile(r"github\.com[/:][^/\s]+/agent-compute-scheduler")
HOME = re.compile(r"(?:/home|/Users)/[A-Za-z0-9][A-Za-z0-9._-]*/")
# a directory of the installed skill, not a repository
SKILL_DIR_OWNERS = {"skill", "skills"}


def _files():
    out = []
    out += [p for p in sorted((REPO / "skill").rglob("*")) if p.is_file()]
    out += [REPO / name for name in SHIPPED_DOCS]
    out += sorted((REPO / "tools").glob("*.py"))
    out += sorted((REPO / "src").rglob("*.py"))
    return out


def _repo_references(text: str) -> list[str]:
    hits = [m.group(0) for m in SLUG.finditer(text)
            if m.group(1) not in SKILL_DIR_OWNERS]
    hits += [m.group(0) for m in URL.finditer(text)]
    return hits


@pytest.mark.parametrize("path", _files(), ids=lambda p: str(p.relative_to(REPO)))
def test_no_concrete_repository_name_in_shipped_text(path):
    text = path.read_text(encoding="utf-8", errors="replace")
    found = []
    for n, line in enumerate(text.splitlines(), 1):
        for hit in _repo_references(line):
            found.append(f"{path.relative_to(REPO)}:{n}: {hit}")
    assert found == [], "a concrete repository of this project is named: " + "; ".join(found)


@pytest.mark.parametrize("rel", ["skill/agent-compute-scheduler/SKILL.md",
                                 "docs/consumer-template.md"])
def test_no_home_path_in_the_skill_or_the_consumer_template(rel):
    text = (REPO / rel).read_text(encoding="utf-8")
    found = [f"{rel}:{n}: {m.group(0)}"
             for n, line in enumerate(text.splitlines(), 1)
             for m in HOME.finditer(line)]
    assert found == [], "a home path is named: " + "; ".join(found)


def test_the_patterns_catch_what_they_are_for():
    # mutated copies: a slug (either name), a URL, a checkout path are found;
    # the skill's own directory and a bare local directory name are not
    assert _repo_references("fix at some-owner/agent-compute-scheduler now")
    assert _repo_references("fix at some-owner/agent-compute-scheduler-private now")
    assert _repo_references("see https://github.com/x/agent-compute-scheduler/pull/1")
    assert _repo_references("~/Docu" "ments/GitHub/agent-compute-scheduler")
    assert not _repo_references("skill/agent-compute-scheduler/SKILL.md")
    assert not _repo_references("~/.agents/skills/agent-compute-scheduler")
    assert not _repo_references("~/agent-compute-scheduler")
    assert HOME.search("cwd /ho" "me/someone/work") and HOME.search("/Us" "ers/someone/x/")
    assert not HOME.search("~/work and <home>/work")
