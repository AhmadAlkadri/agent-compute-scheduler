"""The skill <-> CLI checklist (campaign G slice G5).

What this establishes: every ``agent-compute`` command and option that
``skill/agent-compute-scheduler/SKILL.md`` names exists in this tree's
``--help`` (``tools/skill_check.py`` runs ``bin/agent-compute <cmd>
--help``); every outcome in the skill's outcome table is in
``runner.RC`` with the same exit code, and the "never started" rows are
``state.NEVER_STARTED``; the guard-outcome table, the job phases and the
placement phases and statuses match the code; the status table's fields
are the contract test's documented field set; the frontmatter loads; the
"Brief for a subagent" block carries each rule G5 requires
(section 4.1 of the private executor-corpus record).  The checker is shown to
catch a wrong subcommand, option, exit code and outcome set on mutated
copies.  It does not establish that an agent follows the skill (the cold
read and the dogfood log do).  No harness, no guard, no Docker.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tools"))
import skill_check  # noqa: E402

SKILL = REPO / "skill" / "agent-compute-scheduler" / "SKILL.md"


@pytest.fixture(scope="module")
def report():
    return skill_check.check(SKILL)


@pytest.fixture(scope="module")
def helps():
    return skill_check.Help()


def test_the_skill_names_only_existing_commands_options_and_outcomes(report):
    assert report.problems == []
    paths = {" ".join(path) for path in report.paths}
    # the commands G5 requires the skill to teach (and the operator-only
    # ones it names in "Never")
    assert paths >= {"(global)", "doctor", "hosts", "submit", "run",
                     "status", "wait", "cancel", "reconcile", "resume",
                     "log", "fetch", "placements resolve", "config migrate",
                     "config init", "pin-backend"}
    options = {(" ".join(path), name) for path, name in report.options}
    assert options >= {("status", "--request-id"), ("status", "--resolve"),
                       ("status", "--all"), ("status", "--unfinished"),
                       ("status", "--mine"), ("submit", "--submitter"),
                       ("submit", "--request-id"), ("submit", "--probe"),
                       ("submit", "--container"), ("log", "--tail"),
                       ("wait", "--timeout"), ("reconcile", "--terminate")}
    assert {"--host", "--version"} <= report.global_options
    assert {"--require-commit", "--require-clean", "--any-commit",
            "--submitter", "--resolve"} <= report.loose_options
    assert report.outcomes == 11
    assert report.commands >= 50


def test_the_frontmatter_loads():
    text = SKILL.read_text()
    assert text.startswith("---\nname: agent-compute-scheduler\n")
    front = text.split("---")[1]
    description = re.search(r"^description: (.+)$", front, re.M).group(1)
    assert 100 < len(description) <= 1024
    # neutral: no project or machine is named in the trigger text (the
    # private projects' names are screened by the private tree's release
    # tests over the same description)
    for word in ("lenovo", "smc"):
        assert word not in description.lower()


def test_the_status_table_is_the_contract_tests_field_set():
    from test_status_view_contract import DOCUMENTED_FIELDS, QUEUE_ONLY_FIELDS
    text = SKILL.read_text()
    table = skill_check.section(text, "Status JSON")
    fields = set()
    for row in skill_check.table_rows(table)[1:]:
        if row[0].startswith("`") and len(row) == 2:
            fields |= set(re.findall(r"`([a-z_]+)`", row[0]))
    # `route` only in a reply routed by a placement record (G3a)
    assert fields == DOCUMENTED_FIELDS | QUEUE_ONLY_FIELDS | {"route"}


def test_the_brief_carries_every_required_rule():
    text = SKILL.read_text()
    brief = skill_check.section(text, "Brief for a subagent")
    block = re.search(r"```text\n(.*?)```", brief, re.S).group(1)
    required = {
        # 4.1 item 1: the skill delivered by mechanism
        "Skill(agent-compute-scheduler)": brief,
        "preload": brief.lower(),
        # submit only through ACS, never bypass
        "runs only through": block,
        "never bypass": block,
        # 4.1 item 2: a stable request id, quoting-safe <run>:<step>
        "<run>:<step>": block,
        "it needs no quoting": block,
        # 4.1 item 3: the changed-input rule as enforced
        "BEFORE you submit": block,
        "no edits, commits": block,
        "another queued": block,
        # 4.1 item 4: print and report a hold's reason
        "held-inputs-changed": block,
        "print its `reason`": block,
        # 4.1 item 5: no marker, no direct guard
        "Never set ACS_JOB_MARKER": block,
        "never run a guard": block,
        # 4.1 item 6: never merge, install or deploy ACS
        "Never merge, install, deploy": block,
        # 4.1 item 7: one reconcile policy
        "Only if every blocking job is yours": block,
        "Another project's job in the block: run nothing": block,
        # 4.1 item 8: never shrink the reserve below a measured peak
        "Never shrink --reserve-gb below a measured peak": block,
        # 4.1 item 10: the waiting recipe
        "--timeout 540": block,
        "600000": block,
        "Exit 75": block,
        "No sleep loops": block,
        "no second waiter": block,
        # no pattern kills; stop only your own jobs through cancel
        "Never kill by pattern": block,
        "Stop only your own jobs, only with `agent-compute cancel <job>`":
            block,
        "`docker exec`": block,
        # F15: a refusal contradicting the declared policy is a defect
        "is a defect": block,
    }
    def flat(text: str) -> str:
        return " ".join(text.split())
    missing = [phrase for phrase, where in required.items()
               if flat(phrase) not in flat(where)]
    assert missing == []


def test_the_never_list_fifo_and_the_defect_rule():
    def flat(text: str) -> str:
        return " ".join(text.split())
    text = flat(SKILL.read_text())
    never = flat(skill_check.section(SKILL.read_text(), "Never"))
    assert "**Kill processes by pattern**" in never
    assert ("**Stop any process you did not start; stop your own ACS jobs "
            "only through `agent-compute cancel <job>`.**") in never
    assert "**FIFO is the contract.**" in text
    assert '"hold ok <host>"' in text
    assert ("**A refusal or wait that contradicts the declared policy is a "
            "defect report, not a planning input.**") in text



def test_the_cold_read_gaps_are_closed():
    """The cold read of 2026-10-08 (a private record): each gap of its
    disposition is answered in the skill."""
    def flat(text: str) -> str:
        return " ".join(text.split())
    text = flat(SKILL.read_text())
    for phrase in (
            # 1: --require-commit is automatic; --any-commit opts out
            "ACS itself adds `--require-commit <your HEAD>` to a placed or "
            "pinned submit",
            "`--any-commit` opts out",
            # 2: ACS proves the sender gone; the agent only resolves
            "ACS proves this itself, from the start-time identities it "
            "recorded in the placement record",
            "you never look up or check pids",
            # 3: the floor and the fit rule
            "available (+ the Docker VM credit, for a container job on "
            "macOS) ≥ `floor_gb` + reserve",
            "`floor_gb` is also in `doctor`'s machine profile",
            "`docs/memory-model.md`",
            # 4: sync is the project's step
            "it never pushes, pulls, syncs or copies a checkout",
            "the `{{SYNC}}` slot",
            # 5: reaching another orchestrator
            "`ListAgents`", "`SendMessage`",
            # 6: the background wait recipe
            "`run_in_background: true`", "re-arm it",
            # 7: the Docker Desktop restart exception
            "`docker desktop stop`, then `open -a Docker`, then `docker "
            "start <the listed containers>`",
            "Everyone else reports a wedge",
            # the single-authority doctor paragraph (PR #39), verbatim
            "One admission authority per host. `agent-compute doctor` also "
            "checks, read-only, that this host has exactly one admission "
            "authority.",
            "or a location that cannot be read, which therefore cannot be "
            "ruled out.",
            "It is also not an outage: while the ACS test suite or a "
            "throwaway demonstration install runs on the same host, doctor "
            "truthfully reports its sandbox queue this way."):
        assert flat(phrase) in text, phrase
    template = (REPO / "docs" / "consumer-template.md").read_text()
    assert "| `{{SYNC}}` |" in template


# --- the checker catches drift -------------------------------------------------------

def _mutated(tmp_path, old: str, new: str) -> skill_check.Report:
    text = SKILL.read_text()
    assert old in text
    path = tmp_path / "SKILL.md"
    path.write_text(text.replace(old, new, 1))
    return skill_check.check(path)


def test_an_unknown_subcommand_or_option_is_caught(tmp_path):
    report = _mutated(tmp_path, "agent-compute hosts --json     #",
                      "agent-compute hostz --json     #")
    assert any("no subcommand 'hostz'" in p for p in report.problems)
    report = _mutated(tmp_path, "agent-compute status --resolve --all --json",
                      "agent-compute status --resolv --all --json")
    assert any("option --resolv is not in `agent-compute status --help`" in p
               for p in report.problems)
    # a global option after the subcommand is refused by argparse
    report = _mutated(tmp_path, "agent-compute wait <job> --timeout 540     #",
                      "agent-compute wait <job> --host lenovo --timeout 540 #")
    assert any("option --host is not in `agent-compute wait --help` (a "
               "global option" in p for p in report.problems)
    report = _mutated(tmp_path, "`--any-commit`", "`--any-commitz`")
    assert any("--any-commitz" in p for p in report.problems)


def test_a_wrong_exit_code_or_outcome_set_is_caught(tmp_path):
    report = _mutated(tmp_path, "| `timed-out` | 15 |", "| `timed-out` | 16 |")
    assert any("'timed-out': the skill says exit 16" in p
               for p in report.problems)
    report = _mutated(tmp_path, "| `cancelled` | 14 | maybe |",
                      "| `cancelled` | 14 | never |")
    assert any("NEVER_STARTED" in p for p in report.problems)
    text = SKILL.read_text()
    row = next(line for line in text.splitlines()
               if line.startswith("| `interrupted` | 17 |"))
    report = _mutated(tmp_path, row + "\n", "")
    assert any("missing from the table: ['interrupted']" in p
               for p in report.problems)
    report = _mutated(tmp_path, "| `unknown` | 18 |", "| `vanished` | 18 |")
    assert any("'vanished' is not in runner.RC" in p
               for p in report.problems)


def test_an_option_choice_is_not_taken_for_a_subcommand(helps):
    # `pin-backend [--backend {acs-guard}]`: an option's choices, while
    # `daemon {start,stop,status}` is a positional choice
    assert helps.choices(("pin-backend",)) == set()
    assert helps.choices(("daemon",)) == {"start", "stop", "status"}
    assert helps.choices(("config",)) == {"migrate", "init"}


@pytest.mark.parametrize("name", ["README.md", "docs/consumer-template.md",
                                  "docs/operating-model.md",
                                  "docs/dogfood-log-template.md"])
def test_the_other_agent_facing_docs_name_only_existing_commands(name, helps):
    report = skill_check.Report()
    skill_check.check_commands((REPO / name).read_text(), helps, report)
    assert report.problems == []
    assert report.commands > 0
