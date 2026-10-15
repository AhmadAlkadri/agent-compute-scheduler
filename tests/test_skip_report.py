"""tools/skip_report.py: skip accounting for gate evidence (campaign G
slice G4).

What this establishes: from a ``pytest -v -rs`` output the tool counts the
skips per reason (the short-summary ``SKIPPED [n]`` lines), per test file
(the ``-v`` result lines) and per category (the ``[tag]`` that opens a
harness skip reason, or the known untagged reasons of older evidence); it
prints one summary line for gate headers; it calls a run INCONSISTENT, and
exits 1, when the three counts disagree or the run has no pytest summary;
an unclassified reason is counted as ``untagged``, never dropped.  It is
checked on synthetic outputs and on four synthetic gate runs
(``tests/fixtures/skip_report``: macOS without Docker and Linux with it, each
in the standard and the legacy profile; they carry the skip shapes of real
runs from before the category tags, so that older evidence stays
accountable).  No harness, no guard, no Docker.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
TOOL = REPO / "tools" / "skip_report.py"
sys.path.insert(0, str(REPO / "tools"))
import skip_report  # noqa: E402

FIXTURES = REPO / "tests" / "fixtures" / "skip_report"

RUN = """\
# mac full suite on mac, ...; profile standard; source sha abc
============================= test session starts ==============================
collecting ... collected 7 items

tests/test_a.py::test_one PASSED                                         [ 14%]
tests/test_a.py::test_two[x y-[backend]\\n-z] SKIPPED                     [ 28%]
tests/test_a.py::test_two[q] SKIPPED                                     [ 42%]
tests/test_b.py::test_three[direct] SKIPPED                              [ 57%]
tests/test_b.py::test_three[queue] PASSED                                [ 71%]
tests/test_c.py::test_four SKIPPED (exercises the macOS ps path)         [ 85%]
tests/test_c.py::test_five PASSED                                        [100%]

=========================== short test summary info ============================
SKIPPED [2] tests/test_a.py:10: [docker] docker CLI not on PATH
SKIPPED [1] tests/conftest.py: [legacy] legacy profile only (ACS_TEST_LEGACY_GUARD=<guard.py>)
SKIPPED [1] tests/test_c.py:4: exercises the macOS ps path
================== 3 passed, 4 skipped in 12.34s (0:00:12) ===================
EXIT 0
"""


def run_tool(*args) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(TOOL), *map(str, args)],
                          capture_output=True, text=True, timeout=60)


def test_counts_per_reason_file_and_category():
    report = skip_report.parse(RUN)
    assert report["collected"] == 7
    assert report["outcomes"] == {"passed": 3, "skipped": 4}
    assert report["by_category"] == {"docker": 2, "legacy": 1,
                                     "platform": 1}
    assert report["by_file"] == {"tests/test_a.py": 2, "tests/test_b.py": 1,
                                 "tests/test_c.py": 1}
    assert report["by_reason"]["[docker] docker CLI not on PATH"] == 2
    assert report["consistent"] is True
    assert skip_report.summary_line(report) == (
        "skip-report: collected 7; 3 passed, 4 skipped; skips by category: "
        "docker 2, platform 1, legacy 1; untagged 0; consistent")


def test_the_cli_prints_the_report_and_the_summary_line(tmp_path):
    path = tmp_path / "run.txt"
    path.write_text(RUN)
    full = run_tool(path)
    assert full.returncode == 0, full.stderr
    assert "by category:" in full.stdout and "by file:" in full.stdout
    assert "      2  tests/test_a.py" in full.stdout
    assert full.stdout.rstrip().splitlines()[-1].startswith("skip-report: ")
    line = run_tool("--summary-line", path)
    assert line.returncode == 0
    assert line.stdout.count("\n") == 1
    assert line.stdout.startswith("skip-report: collected 7; ")
    as_json = json.loads(run_tool("--json", path).stdout)
    assert as_json["by_category"]["docker"] == 2
    assert as_json["summary_line"] == line.stdout.strip()


def test_an_unclassified_reason_is_counted_as_untagged():
    text = RUN.replace("[docker] docker CLI not on PATH",
                       "something nobody classified")
    report = skip_report.parse(text)
    assert report["by_category"] == {"untagged": 2, "legacy": 1,
                                     "platform": 1}
    assert "untagged 2; consistent" in skip_report.summary_line(report)
    assert report["consistent"] is True     # counted, never dropped


def test_an_unknown_tag_is_untagged():
    assert skip_report.category("[weather] it rained") == "untagged"
    assert skip_report.category("[docker] x") == "docker"


@pytest.mark.parametrize("edit,why", [
    (lambda t: t.replace("SKIPPED [2] tests/test_a.py:10",
                         "SKIPPED [1] tests/test_a.py:10"),
     "short summary 3"),
    (lambda t: t.replace("tests/test_a.py::test_two[q] SKIPPED",
                         "tests/test_a.py::test_two[q] PASSED"),
     "-v lines 3"),
    (lambda t: t.replace("3 passed, 4 skipped", "3 passed, 5 skipped"),
     "final 5"),
])
def test_disagreeing_counts_are_inconsistent(tmp_path, edit, why):
    path = tmp_path / "run.txt"
    path.write_text(edit(RUN))
    result = run_tool("--summary-line", path)
    assert result.returncode == 1
    assert "INCONSISTENT" in result.stdout and why in result.stdout


def test_an_incomplete_run_is_inconsistent(tmp_path):
    path = tmp_path / "run.txt"
    path.write_text(RUN.split("=========================== short")[0])
    result = run_tool("--summary-line", path)
    assert result.returncode == 1
    assert "no pytest summary" in result.stdout
    assert "INCONSISTENT" in result.stdout


def test_an_unreadable_file_is_an_error(tmp_path):
    result = run_tool(tmp_path / "absent.txt")
    assert result.returncode == 2 and "cannot read" in result.stderr


@pytest.mark.parametrize("name,expected", [
    # the hand accounting of the real runs these fixtures are shaped on
    ("standard-nodocker-darwin.txt",
     "collected 1138; 1051 passed, 87 skipped; skips by category: docker 52, "
     "platform 4, legacy 31; untagged 0; consistent"),
    ("legacy-nodocker-darwin.txt",
     "collected 1151; 1095 passed, 56 skipped; skips by category: docker 52, "
     "platform 4; untagged 0; consistent"),
    ("standard-docker-linux.txt",
     "collected 1138; 1091 passed, 47 skipped; skips by category: "
     "platform 9, legacy 38; untagged 0; consistent"),
    ("legacy-docker-linux.txt",
     "collected 1142; 1133 passed, 9 skipped; skips by category: "
     "platform 9; untagged 0; consistent"),
])
def test_the_synthetic_gate_runs_are_accounted(name, expected):
    result = run_tool("--summary-line", FIXTURES / name)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == f"skip-report: {expected}"
