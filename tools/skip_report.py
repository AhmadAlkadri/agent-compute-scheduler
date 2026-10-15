#!/usr/bin/env python3
"""Skip accounting for a pytest run.

Reads the output of ``pytest -v -rs`` (a saved run, gate-evidence header
lines included) and prints how many tests
skipped, per reason, per category and per test file, and one summary line
that gate evidence headers carry (README, "Pin the engine"):

    skip-report: collected 1138; 1051 passed, 87 skipped; skips by category: docker 52, legacy 31, platform 4; untagged 0; consistent

The category of a skip is the tag that opens its reason, as the harness
writes it (``tests/conftest.py`` ``skip_reason``): ``[docker]``,
``[platform]``, ``[legacy]``, ``[pueue]``, ``[host]``, ``[clone]``,
``[root]``.  Reasons written before the tags existed carry no tag; the few known ones are
recognised by their text (``INFERRED``) so that older evidence can be
accounted the same way.  Anything else is ``untagged``: a skip nobody
classified, which a gate review must explain.

"consistent" means the three counts agree: the short-summary ``SKIPPED
[n]`` lines, the per-test ``SKIPPED`` lines of ``-v`` and the final pytest
summary.  Otherwise the line says ``INCONSISTENT`` and the exit status is 1
(also when no pytest summary is found: the run did not complete).

Usage::

    python3 tools/skip_report.py <pytest -v -rs output>          # report
    python3 tools/skip_report.py --summary-line <file>           # the line only
    python3 tools/skip_report.py --json <file>                   # machine-readable

Standard library only; it reads the file and nothing else.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

CATEGORIES = ("docker", "platform", "legacy", "pueue", "host", "clone",
              "root")
# what each category means (printed with the report)
MEANING = {
    "docker": "no docker CLI or daemon, or the test image is absent",
    "platform": "the test exercises the other operating system",
    "legacy": "legacy profile only (ACS_TEST_LEGACY_GUARD=<guard.py>)",
    "pueue": "pueue/pueued not on PATH (a standard-profile prerequisite)",
    "host": "this host's resources are below what the test declares",
    "clone": "a commit the test needs is not in this clone (shallow clone)",
    "root": "the test cannot hold when run as root",
    "untagged": "a skip with no category: explain it in the gate",
}
# reasons written before the tags (G2b and earlier evidence)
INFERRED = (
    (re.compile(r"^docker or image .* missing$"), "docker"),
    (re.compile(r"^legacy profile only\b"), "legacy"),
    (re.compile(r"^legacy guard not found at "), "legacy"),
    (re.compile(r"Linux host only|macOS-only|macOS ps path|"
                r"discriminator is macOS"), "platform"),
    (re.compile(r"^pueue not installed$"), "pueue"),
    (re.compile(r" is not in this clone$"), "clone"),
    (re.compile(r"^this host's RAM "), "host"),
    (re.compile(r"^root reads a mode-000 file$"), "root"),
)
TAG = re.compile(r"^\[(?P<tag>[a-z]+)\]\s*")
# "SKIPPED [n] <file>:<line>: <reason>"; the line is absent for a skip
# raised outside a test function (a parametrized fixture, for example)
SHORT = re.compile(r"^SKIPPED \[(?P<n>\d+)\] (?P<where>\S+?)(?::(?P<line>\d+))?"
                   r": (?P<reason>.*)$")
# one -v result line: "<file>.py::<test id> SKIPPED [ 5%]" (the test id may
# hold spaces; newer pytest may print "SKIPPED (reason)")
VERBOSE = re.compile(r"^(?P<file>\S+?\.py)(?:::.*?)? (?P<outcome>PASSED|"
                     r"FAILED|SKIPPED|ERROR|XFAIL|XPASS)(?: \(.*\))?"
                     r"\s+\[\s*\d+%\]$")
FINAL = re.compile(r"^=+ (?P<body>(?:\d+ \w+(?:, )?)+.*?) in [\d.]+s"
                   r"(?: \([\d:]+\))? =+$")
COLLECTED = re.compile(r"\bcollected (?P<n>\d+) items?\b")


def category(reason: str) -> str:
    tagged = TAG.match(reason)
    if tagged:
        tag = tagged.group("tag")
        return tag if tag in CATEGORIES else "untagged"
    for pattern, name in INFERRED:
        if pattern.search(reason):
            return name
    return "untagged"


def parse(text: str) -> dict:
    by_reason: Counter = Counter()
    by_file: Counter = Counter()
    short_total = 0
    final = None
    collected = None
    for line in text.splitlines():
        line = line.rstrip()
        short = SHORT.match(line)
        if short:
            n = int(short.group("n"))
            short_total += n
            by_reason[short.group("reason")] += n
            continue
        verbose = VERBOSE.match(line)
        if verbose and verbose.group("outcome") == "SKIPPED":
            by_file[verbose.group("file")] += 1
            continue
        found = FINAL.match(line)
        if found:
            final = found.group("body")
        count = COLLECTED.search(line)
        if count and collected is None:
            collected = int(count.group("n"))
    outcomes = ({kind: int(n) for n, kind in
                 re.findall(r"(\d+) (\w+)", final)} if final else {})
    by_category: Counter = Counter()
    for reason, n in by_reason.items():
        by_category[category(reason)] += n
    final_skipped = outcomes.get("skipped", 0)
    consistent = (final is not None and short_total == final_skipped
                  and sum(by_file.values()) == final_skipped)
    return {"collected": collected, "outcomes": outcomes, "final": final,
            "skipped": final_skipped, "short_total": short_total,
            "verbose_total": sum(by_file.values()),
            "by_category": dict(by_category),
            "by_reason": dict(by_reason), "by_file": dict(by_file),
            "consistent": consistent}


def summary_line(report: dict) -> str:
    cats = report["by_category"]
    named = ", ".join(f"{name} {cats[name]}" for name in CATEGORIES
                      if cats.get(name))
    outcomes = ", ".join(f"{n} {kind}" for kind, n in
                         report["outcomes"].items()) or "no pytest summary"
    state = "consistent" if report["consistent"] else (
        f"INCONSISTENT (short summary {report['short_total']}, -v lines "
        f"{report['verbose_total']}, final {report['skipped']})")
    return (f"skip-report: collected {report['collected']}; {outcomes}; "
            f"skips by category: {named or 'none'}; untagged "
            f"{cats.get('untagged', 0)}; {state}")


def render(report: dict, path: str) -> str:
    out = [f"skip report for {path}",
           f"collected: {report['collected']}; pytest summary: "
           f"{report['final'] or '(none: the run did not complete)'}", "",
           "by category:"]
    for name in (*CATEGORIES, "untagged"):
        if report["by_category"].get(name):
            out.append(f"  {report['by_category'][name]:5d}  {name:9s} "
                       f"{MEANING[name]}")
    out += ["", "by reason:"]
    for reason, n in sorted(report["by_reason"].items(),
                            key=lambda item: (-item[1], item[0])):
        out.append(f"  {n:5d}  [{category(reason)}] {reason}"
                   if not TAG.match(reason) else f"  {n:5d}  {reason}")
    out += ["", "by file:"]
    for name, n in sorted(report["by_file"].items()):
        out.append(f"  {n:5d}  {name}")
    out += ["", summary_line(report)]
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("path", type=Path,
                        help="a pytest -v -rs output (evidence) file")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--summary-line", action="store_true",
                      help="print only the summary line")
    mode.add_argument("--json", action="store_true",
                      help="print the report as JSON")
    args = parser.parse_args(argv)
    try:
        text = args.path.read_text(errors="replace")
    except OSError as error:
        print(f"skip_report: cannot read {args.path}: {error}",
              file=sys.stderr)
        return 2
    report = parse(text)
    if args.json:
        print(json.dumps({**report, "summary_line": summary_line(report)},
                         indent=2, sort_keys=True))
    elif args.summary_line:
        print(summary_line(report))
    else:
        print(render(report, str(args.path)))
    return 0 if report["consistent"] else 1


if __name__ == "__main__":
    sys.exit(main())
