#!/usr/bin/env python3
"""The skill <-> CLI checklist (campaign G slice G5).

Checks ``skill/agent-compute-scheduler/SKILL.md`` against the CLI and the
outcome vocabulary of this tree, so that the agent instructions never name
a command, an option or an outcome that the product does not have:

* every ``agent-compute ...`` command in a fenced block or an inline code
  span: its subcommand (and a ``config``/``placements`` sub-subcommand, or a
  ``daemon`` action) exists, and every option it names before ``--``
  exists in that command's ``--help`` (run as ``bin/agent-compute <cmd>
  --help`` of this tree), or among the global options;
* every inline code span that starts with a subcommand name or ``--host``
  (``status --resolve``, ``reconcile --terminate <job>``): the same;
* every ``--option`` anywhere in the skill is an option of some
  agent-compute command, or one of ``FOREIGN`` (options of other tools the
  skill names, each with its tool);
* the table under ``## Outcomes``: its outcomes are exactly
  ``state.OUTCOMES`` (= ``runner.RC`` without the runner-internal
  ``duplicate-dispatch``), each row's exit code is ``runner.RC[outcome]``,
  and the rows marked ``never`` are exactly ``state.NEVER_STARTED``;
* the ACS outcomes of the guard-outcome table are in ``runner.RC``, and its
  guard outcomes cover ``runner.GUARD_OUTCOME``;
* the ``phase`` row of the status table names exactly ``state.PHASES``
  plus ``done``; the placement phases (``placement.PHASES``) and candidate
  statuses (``placement.STATUSES``) are all named.

Usage: ``python3 tools/skill_check.py [SKILL.md]``; prints the counts and
exits 1 with the problems.  ``tests/test_skill_check.py`` runs it.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SKILL = REPO / "skill" / "agent-compute-scheduler" / "SKILL.md"
CLI = REPO / "bin" / "agent-compute"
sys.path.insert(0, str(REPO / "src"))

# options of other tools that the skill names (never agent-compute's)
FOREIGN = {
    "--light": "a project guard's light mode (not an ACS route)",
    "--estimate-gb": "a guard-style launcher's reserve option (mapped to "
                     "--reserve-gb)",
    "--force": "install.py uninstall --force (operator-only)",
    "--rm": "docker run --rm (the Docker Desktop restart check)",
}
# global options that take a value
GLOBAL_VALUE = {"--host", "--config"}
OPTION = re.compile(r"(?<![\w-])(--[a-z][a-z0-9]*(?:-[a-z0-9]+)*)")
FENCE = re.compile(r"^[ \t]*```([^\n]*)\n(.*?)^[ \t]*```", re.S | re.M)
SPAN = re.compile(r"`([^`]+)`")          # a code span may wrap a line
# `agent-compute` as a command word: not agent-compute-scheduler
COMMAND = re.compile(r"(?<![\w/.-])agent-compute(?![\w/.-])")
STOP = re.compile(r"\s#|\||;|&&|`|\)\s|\)$")
PLACEHOLDER = re.compile(r"^(<.*|\.\.\.|…|[A-Z][A-Z_|-]*)$")


@dataclass
class Report:
    commands: int = 0                       # command occurrences checked
    paths: set = field(default_factory=set)         # distinct command paths
    options: set = field(default_factory=set)       # (path, option) verified
    global_options: set = field(default_factory=set)
    loose_options: set = field(default_factory=set)  # every --opt in the text
    outcomes: int = 0
    problems: list = field(default_factory=list)


class Help:
    """``--help`` of this tree's CLI, per command path, cached."""

    def __init__(self) -> None:
        self.cache: dict[tuple, str] = {}
        self.home = tempfile.mkdtemp(prefix="acs-skill-check-")

    def text(self, path: tuple) -> str | None:
        if path not in self.cache:
            env = {k: v for k, v in os.environ.items()
                   if not k.startswith(("AGENT_COMPUTE", "ACS_"))}
            env["HOME"] = self.home
            out = subprocess.run([sys.executable, str(CLI), *path, "--help"],
                                 capture_output=True, text=True, env=env,
                                 timeout=60)
            self.cache[path] = out.stdout if out.returncode == 0 else None
        return self.cache[path]

    def options(self, path: tuple) -> set[str]:
        text = self.text(path) or ""
        part = text.split("\noptions:", 1)
        return set(OPTION.findall(part[1] if len(part) == 2 else text))

    def choices(self, path: tuple) -> set[str]:
        """The subcommands (or positional choices) in ``path``'s usage; not
        an option's choices (``[--backend {acs-guard}]``)."""
        text = self.text(path) or ""
        usage = text.split("\n\n", 1)[0]
        for before, choices in re.findall(r"(\S*)\s+\{([a-z_,-]+)\}", usage):
            if before.lstrip("[").startswith("-") and \
                    not before.endswith("]"):
                continue
            return set(choices.split(","))
        return set()


def tokens_of(rest: str) -> list[str]:
    stop = STOP.search(rest)
    if stop:
        rest = rest[:stop.start()]
    out = []
    for token in rest.split():
        token = token.strip("[](),.\"'")
        if token:
            out.append(token)
    return out


def check_command(tokens: list[str], helps: Help, report: Report,
                  where: str) -> None:
    """``tokens``: what follows ``agent-compute`` (global options first)."""
    glob = helps.options(())
    i = 0
    while i < len(tokens) and tokens[i].startswith("-"):
        name = tokens[i].split("=", 1)[0]
        if name not in glob:
            report.problems.append(f"{where}: global option {name} is not "
                                   "in `agent-compute --help`")
        report.global_options.add(name)
        i += 2 if name in GLOBAL_VALUE and "=" not in tokens[i] else 1
    if i >= len(tokens) or PLACEHOLDER.match(tokens[i]):
        if i:
            report.commands += 1
            report.paths.add(("(global)",))
        return
    path = (tokens[i],)
    if tokens[i] not in helps.choices(()):
        report.problems.append(f"{where}: no subcommand {tokens[i]!r} in "
                               "`agent-compute --help`")
        return
    i += 1
    nested = helps.choices(path)
    if nested:
        if i >= len(tokens) or tokens[i] not in nested:
            report.problems.append(
                f"{where}: `{path[0]}` needs one of {sorted(nested)}, got "
                f"{tokens[i] if i < len(tokens) else 'nothing'!r}")
            return
        if path[0] in ("config", "placements"):
            path = path + (tokens[i],)
        i += 1
    report.commands += 1
    report.paths.add(path)
    # a global option (--host, --config) goes before the subcommand only:
    # after it, argparse refuses it
    allowed = helps.options(path)
    for token in tokens[i:]:
        if token == "--":
            break
        if not token.startswith("--"):
            continue
        name = token.split("=", 1)[0]
        if name not in allowed:
            hint = " (a global option: put it before the subcommand)" \
                if name in glob else ""
            report.problems.append(f"{where}: option {name} is not in "
                                   f"`agent-compute {' '.join(path)} --help`"
                                   f"{hint}")
        else:
            report.options.add((path, name))


def lines_of_fences(text: str) -> tuple[list[str], list[str]]:
    """(command lines of shell blocks, with continuations joined; the code
    spans of ``text`` blocks, whose prose is not a command)"""
    lines, spans = [], []
    for language, block in FENCE.findall(text):
        if language.strip() == "text":
            spans.extend(SPAN.findall(block.replace("\n", " ")))
            continue
        joined = re.sub(r"\\\n\s*", " ", block)
        lines.extend(joined.splitlines())
    return lines, spans


def check_commands(text: str, helps: Help, report: Report) -> None:
    lines, text_spans = lines_of_fences(text)
    for line in lines:
        for match in COMMAND.finditer(line):
            check_command(tokens_of(line[match.end():]), helps, report,
                          f"fenced line {line.strip()!r}")
    prose = FENCE.sub("", text)
    for span in text_spans:
        match = COMMAND.search(span)
        if match:
            check_command(tokens_of(span[match.end():]), helps, report,
                          f"brief span `{span}`")
    subcommands = helps.choices(())
    spans = [" ".join(span.split()) for paragraph in prose.split("\n\n")
             for span in SPAN.findall(paragraph)]
    for span in spans:
        match = COMMAND.search(span)
        if match:
            check_command(tokens_of(span[match.end():]), helps, report,
                          f"span `{span}`")
            continue
        first = span.split()[0] if span.split() else ""
        if first in subcommands or first == "--host":
            if len(span.split()) > 1:
                check_command(tokens_of(span), helps, report, f"span `{span}`")


def check_loose_options(text: str, helps: Help, report: Report) -> None:
    known = set(helps.options(()))
    for name in helps.choices(()):
        known |= helps.options((name,))
        for sub in helps.choices((name,)):
            if name in ("config", "placements"):
                known |= helps.options((name, sub))
    for name in sorted(set(OPTION.findall(text))):
        report.loose_options.add(name)
        if name not in known and name not in FOREIGN:
            report.problems.append(f"option {name} appears in the skill but "
                                   "in no agent-compute --help and not in "
                                   "FOREIGN")


def section(text: str, heading: str) -> str:
    start = text.index(f"\n## {heading}")
    end = text.find("\n## ", start + 1)
    return text[start:end if end > 0 else len(text)]


def table_rows(block: str) -> list[list[str]]:
    rows = []
    for line in block.splitlines():
        if line.startswith("|") and not re.match(r"^\|[-| ]+\|$", line):
            rows.append([cell.strip() for cell in line.strip("|").split("|")])
    return rows


def check_outcomes(text: str, report: Report) -> None:
    from agent_compute import placement, runner, state
    rc = dict(runner.RC)
    internal = {"duplicate-dispatch"}       # the runner's, never an outcome
    if set(rc) - internal != set(state.OUTCOMES):
        report.problems.append("runner.RC without duplicate-dispatch differs "
                               "from state.OUTCOMES")
    outcomes = section(text, "Outcomes")
    first_table = outcomes.split("\n\n| exit |")[0]
    seen, never = {}, set()
    for row in table_rows(first_table)[1:]:
        name = row[0].strip("`")
        try:
            code = int(row[1])
        except ValueError:
            report.problems.append(f"outcome row {row[0]}: exit {row[1]!r}")
            continue
        seen[name] = code
        if row[2] == "never":
            never.add(name)
        if name not in rc or name in internal:
            report.problems.append(f"outcome {name!r} is not in runner.RC")
        elif rc[name] != code:
            report.problems.append(f"outcome {name!r}: the skill says exit "
                                   f"{code}, runner.RC says {rc[name]}")
    report.outcomes = len(seen)
    missing = set(state.OUTCOMES) - set(seen)
    if missing:
        report.problems.append(f"outcomes missing from the table: "
                               f"{sorted(missing)}")
    if never != set(state.NEVER_STARTED):
        report.problems.append(f"'never' rows {sorted(never)} != "
                               f"state.NEVER_STARTED "
                               f"{sorted(state.NEVER_STARTED)}")
    guard = outcomes.split("| `guard_outcome` | ACS `outcome` |")[1]
    guard = guard.split("\n\n")[0]
    named = set()
    # words of the ACS column that are not outcomes: the value null and
    # the command cancel ("`cancelled` after an ACS `cancel`")
    not_outcomes = {"null", "cancel"}
    for row in table_rows(guard):
        named |= set(re.findall(r"`([a-z-]+)`", row[0]))
        for word in re.findall(r"`([a-z]+(?:-[a-z]+)*)`", row[1]):
            if word in not_outcomes:
                continue
            if word not in rc or word in internal:
                report.problems.append(f"guard table: ACS outcome {word!r} "
                                       "is not in runner.RC")
    if set(runner.GUARD_OUTCOME) - named:
        report.problems.append("guard outcomes missing from the guard table: "
                               f"{sorted(set(runner.GUARD_OUTCOME) - named)}")
    phase_row = next((line for line in text.splitlines()
                      if line.startswith("| `phase` |")), "")
    phases = {word for word in re.findall(r"`([^`]+)`", phase_row)
              if re.fullmatch(r"[a-z]+(?:-[a-z]+)*", word)} - {"phase"}
    if phases != set(state.PHASES) | {"done"}:
        report.problems.append(f"status table phases {sorted(phases)} != "
                               "state.PHASES + done")
    records = section(text, "Placement records and lost replies")
    for phase in placement.PHASES:
        if f"`{phase}`" not in records:
            report.problems.append(f"placement phase {phase!r} not named")
    for status in placement.STATUSES:
        if f"`{status}`" not in text:
            report.problems.append(f"placement candidate status {status!r} "
                                   "not named")


def check(path: Path = SKILL) -> Report:
    text = path.read_text()
    helps = Help()
    report = Report()
    check_commands(text, helps, report)
    check_loose_options(text, helps, report)
    check_outcomes(text, report)
    return report


def main(argv: list[str]) -> int:
    path = Path(argv[1]) if len(argv) > 1 else SKILL
    report = check(path)
    print(f"skill-check {path.name}: {report.commands} command occurrences "
          f"({len(report.paths)} distinct commands: "
          f"{', '.join(sorted(' '.join(p) for p in report.paths))}); "
          f"{len(report.options)} command options and "
          f"{len(report.global_options)} global options verified against "
          f"--help; {len(report.loose_options)} distinct --options in the "
          f"text ({len([o for o in report.loose_options if o in FOREIGN])} "
          f"foreign: {', '.join(sorted(o for o in report.loose_options if o in FOREIGN))}); "
          f"{report.outcomes} outcomes verified against runner.RC; "
          f"{'OK' if not report.problems else f'{len(report.problems)} PROBLEMS'}")
    for problem in report.problems:
        print(f"  - {problem}")
    return 1 if report.problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
