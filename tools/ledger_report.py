#!/usr/bin/env python3
"""Read-only aggregate report over an agent-compute state directory.

Written during a dogfooding investigation of the first weeks of real use,
it reproduces the measurements that investigation reported from the job
records: outcomes, dispatch versus admission waits, guard attempts, holds,
cancellations, executing- and dispatched-interval overlaps (the one-slot
invariant as it was actually observed), slot occupancy per UTC day and the
``unknown`` outcomes with no recorded reconciliation.

Definitions (corrected after an independent review of the first version):

- ``dispatch_wait_s`` is accepted -> dispatched (time in the Pueue queue).
  It is *not* the CLI's ``queue_wait_s`` (accepted -> admitted).
  ``admission_s`` is dispatched -> admitted.
- An executed job is one with an execution time (``wall_s``, or
  admitted..ended).  Its executing interval is admitted..ended; an executed
  job without both timestamps is *untimed* and listed, never silently
  dropped from the denominator.
- Overlaps enumerate every intersecting pair with the true intersection
  ``min(end0, end1) - start1``; intervals that only touch (one ends in the
  second the next starts) are counted separately, since whole-second
  timestamps cannot exclude a sub-second overlap there.
- Slot occupancy per UTC day splits each admitted..ended interval at UTC
  midnight; untimed executed jobs are reported beside it, their ``wall_s``
  anchored at ``ended - wall_s`` (an approximation).
- An ``unknown`` outcome with neither a ``reconciled`` key nor a
  ``reconciled`` history entry is either still in a host block
  (``blocked_now``) or was cleared without a record.

Per-version reports (a maintainer's dogfooding loop: one report per
installed version):

- ``--since <tag-or-sha>`` keeps the jobs whose manifest ``acs_source_sha``
  (written since G2a, ``v0.2.0``) is that commit or a descendant of it in
  the history of ``--repo`` (default: the checkout this tool lives in),
  decided by ``git merge-base --is-ancestor``.  A job without
  ``acs_source_sha`` (accepted before G2a) is reported apart as
  *untagged*; a sha the repository does not know (a clone that lacks the
  commit, an ambiguous short sha) as *unresolved*; a job directory whose
  records cannot be read as *unreadable*; the jobs of earlier or unrelated
  commits as *excluded*, counted by version.  None is dropped silently.
  A ``--since`` that does not resolve to a commit is an error (exit 2),
  never an empty report.
- ``--version <name>`` keeps the jobs whose manifest ``acs_version`` is
  exactly that installed version name (for example ``0.2.1-5c66104136``);
  jobs without one are *untagged*.
- The selection block (``selection`` in ``--json``) carries the counts and
  the versions; the rest of the report covers the selected jobs only.
  Git is called read-only (``rev-parse``, ``merge-base``).

It reads ``<state>/jobs/*/manifest.json``, ``state.json`` and
``<state>/blocks/*.json`` only.  It never writes, never starts the queue
daemon, never calls ``agent-compute``.  The
usual read-only ``agent-compute status`` is not used because it settles
never-claimed jobs and may write a lost-supervision block (cli.detect_lost).

Usage::

    python3 tools/ledger_report.py                 # ~/.local/state/agent-compute
    python3 tools/ledger_report.py --state DIR     # another state directory
    python3 tools/ledger_report.py --json          # machine-readable
    python3 tools/ledger_report.py --csv FILE      # per-job table (no argv, no env)
    python3 tools/ledger_report.py --since v0.2.0  # jobs of v0.2.0 and later
    python3 tools/ledger_report.py --since v0.2.0 --repo ~/agent-compute-scheduler
    python3 tools/ledger_report.py --version 0.2.1-5c66104136

The per-job CSV carries only job id, the ACS version, project, container,
backend kind, request/slice presence, timestamps, waits, attempts, outcome, guard outcome,
exit code and the cleanup verdict: nothing from ``argv`` or ``env``.
"""
from __future__ import annotations

import argparse
import collections
import csv
import datetime as dt
import json
import os
import re
import statistics
import subprocess
import sys
from pathlib import Path

FMT = "%Y-%m-%dT%H:%M:%SZ"
REPO = Path(__file__).resolve().parents[1]
SHA = re.compile(r"[0-9a-f]{7,40}")
CSV_FIELDS = ["job_id", "acs_version", "project", "container", "backend", "has_request_id",
              "has_slice", "accepted_utc", "dispatched_utc", "admitted_utc",
              "ended_utc", "dispatch_wait_s", "admission_s", "execution_s", "attempts",
              "outcome", "guard_outcome", "exit_code", "cleanup_clean",
              "reconciled"]


def parse(s):
    try:
        return dt.datetime.strptime(s, FMT) if isinstance(s, str) else None
    except ValueError:
        return None


def read_json(path: Path):
    try:
        with open(path) as handle:
            value = json.load(handle)
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def load_jobs(state_dir: Path) -> list[dict]:
    """One flat row per job directory; unreadable records give a row with
    ``unreadable`` set, so a corrupt job never silently disappears."""
    rows = []
    jobs_dir = state_dir / "jobs"
    if not jobs_dir.is_dir():
        return rows
    for job_dir in sorted(p for p in jobs_dir.iterdir() if p.is_dir()):
        man = read_json(job_dir / "manifest.json")
        st = read_json(job_dir / "state.json")
        row = {"job_id": job_dir.name, "unreadable": man is None or st is None}
        man = man or {}
        st = st or {}
        res = man.get("resources") or {}
        cleanup = st.get("cleanup")
        if not isinstance(cleanup, dict):
            cleanup = {}
        accepted, dispatched = parse(st.get("accepted_utc")), \
            parse(st.get("dispatched_utc"))
        admitted, ended = parse(st.get("admitted_utc")), parse(st.get("ended_utc"))
        history = st.get("history")
        history = history if isinstance(history, list) else []
        wall = st.get("wall_s") if isinstance(
            st.get("wall_s"), (int, float)) and not isinstance(
            st.get("wall_s"), bool) else None
        version, source = man.get("acs_version"), man.get("acs_source_sha")
        row.update({
            "acs_version": version if isinstance(version, str) and version
            else None,
            "acs_source_sha": source if isinstance(source, str) and source
            else None,
            "project": man.get("project"),
            "container": (man.get("container") or {}).get("name")
            if isinstance(man.get("container"), dict) else None,
            "backend": (man.get("backend") or {}).get("kind")
            if isinstance(man.get("backend"), dict) else None,
            "has_request_id": bool(man.get("request_id")),
            "has_slice": bool(man.get("slice")),
            "probe": bool(res.get("probe")),
            "parent_job": bool(man.get("parent_job")),
            "accepted_utc": st.get("accepted_utc"),
            "dispatched_utc": st.get("dispatched_utc"),
            "admitted_utc": st.get("admitted_utc"),
            "ended_utc": st.get("ended_utc"),
            "accepted": accepted, "dispatched": dispatched,
            "admitted": admitted, "ended": ended,
            "dispatch_wait_s": (dispatched - accepted).total_seconds()
            if accepted and dispatched else None,
            "admission_s": (admitted - dispatched).total_seconds()
            if dispatched and admitted else None,
            "wall_s": wall,
            "execution_s": wall if wall is not None else (
                (ended - admitted).total_seconds() if admitted and ended
                else None),
            "attempts": st.get("attempt") if isinstance(st.get("attempt"), int)
            else None,
            "outcome": st.get("outcome"),
            "phase": st.get("phase"),
            "guard_outcome": st.get("guard_outcome"),
            "exit_code": st.get("exit_code"),
            "cleanup_clean": cleanup.get("clean"),
            "reconciled": bool(st.get("reconciled")) or any(
                isinstance(h, dict) and h.get("event") == "reconciled"
                for h in history),
            "reason": st.get("reason") if isinstance(st.get("reason"), str)
            else "",
        })
        rows.append(row)
    return rows


def percentile(values, p):
    values = sorted(values)
    return values[int(round(p * (len(values) - 1)))] if values else None


def summary(values) -> dict:
    values = [v for v in values if isinstance(v, (int, float))]
    if not values:
        return {"n": 0}
    return {"n": len(values), "median": statistics.median(values),
            "p90": percentile(values, 0.9), "max": max(values),
            "total": sum(values)}


def read_blocks(state_dir: Path) -> list[str]:
    """Job ids in the host admission block files (read only)."""
    blocks_dir = state_dir / "blocks"
    ids = set()
    if blocks_dir.is_dir():
        for path in sorted(blocks_dir.glob("*.json")):
            jobs = (read_json(path) or {}).get("jobs")
            if isinstance(jobs, dict):
                ids.update(str(job) for job in jobs)
    return sorted(ids)


def interval_pairs(rows, start_key: str, end_key: str) -> dict:
    """Every pair of jobs whose start_key..end_key intervals intersect, with
    the true intersection ``min(end0, end1) - start1`` in seconds, plus the
    number of pairs that only touch (end0 == start1).  Sorted by start, so
    the inner scan stops at the first interval starting after end0."""
    intervals = sorted((r[start_key], r[end_key], r["job_id"]) for r in rows
                       if r[start_key] and r[end_key])
    pairs, touching = [], 0
    for i, (s0, e0, j0) in enumerate(intervals):
        for s1, e1, j1 in intervals[i + 1:]:
            if s1 > e0:
                break
            if s1 == e0:
                touching += 1
            elif e1 > s1:
                pairs.append((j0, j1, (min(e0, e1) - s1).total_seconds()))
    return {"checked": len(intervals), "pairs": pairs,
            "touching_pairs": touching}


def executed(row) -> bool:
    return row["execution_s"] is not None


def untimed_executed(rows) -> list[str]:
    """Executed jobs without both admitted_utc and ended_utc: outside the
    executing-overlap check and the timed occupancy."""
    return [r["job_id"] for r in rows
            if executed(r) and not (r["admitted"] and r["ended"])]


def executing_overlaps(rows) -> dict:
    """Pairs of jobs whose executing intervals (admitted..ended) intersect,
    with the denominator and coverage.  An empty ``pairs`` is the observed
    one-heavy-job-per-host invariant over the ``checked`` intervals."""
    found = interval_pairs(rows, "admitted", "ended")
    return {"checked": found["checked"],
            "executed": sum(1 for r in rows if executed(r)),
            "untimed_executed": untimed_executed(rows),
            "pairs": found["pairs"],
            "touching_pairs": found["touching_pairs"]}


def dispatched_overlaps(rows) -> dict:
    """The runner-level check: dispatched..ended intervals (they cover the
    untimed executed jobs too)."""
    return interval_pairs(rows, "dispatched", "ended")


def split_hours(start, end, busy) -> None:
    """Add start..end to ``busy`` (UTC day -> hours), split at midnight."""
    while start < end:
        midnight = dt.datetime.combine(start.date() + dt.timedelta(days=1),
                                       dt.time())
        stop = min(end, midnight)
        busy[start.strftime("%Y-%m-%d")] += (stop - start).total_seconds() / 3600
        start = stop


def busy_hours_per_day(rows) -> dict:
    """Slot occupancy per UTC day: admitted..ended, split at UTC midnight."""
    busy = collections.defaultdict(float)
    for r in rows:
        if r["admitted"] and r["ended"]:
            split_hours(r["admitted"], r["ended"], busy)
    return {day: round(hours, 2) for day, hours in sorted(busy.items())}


def busy_hours_untimed(rows) -> dict:
    """The untimed executed jobs, reported apart from the timed occupancy:
    their wall_s anchored at ended - wall_s (approximate), split at UTC
    midnight; jobs with neither wall_s nor ended_utc are only listed."""
    busy = collections.defaultdict(float)
    ids = set(untimed_executed(rows))
    for r in rows:
        if r["job_id"] in ids and r["ended"] and r["wall_s"]:
            split_hours(r["ended"] - dt.timedelta(seconds=r["wall_s"]),
                        r["ended"], busy)
    return {"jobs": sorted(ids),
            "wall_hours_per_day": {day: round(hours, 2)
                                   for day, hours in sorted(busy.items())}}


def unknown_without_record(rows, blocked: list[str]) -> dict:
    """``unknown`` outcomes with no recorded reconciliation: still blocking
    the host, or cleared without a record (a pre-``_record_resolution``
    reconcile, or a hand edit)."""
    missing = [r["job_id"] for r in rows
               if r["outcome"] == "unknown" and not r["reconciled"]]
    return {"blocked_now": [j for j in missing if j in blocked],
            "cleared_without_record": [j for j in missing if j not in blocked]}


def hold_causes(rows) -> dict:
    causes = collections.Counter()
    for r in rows:
        if r["outcome"] == "held-inputs-changed":
            causes[r["reason"].split("; resubmit")[0]] += 1
    return dict(causes)


def cancel_kinds(rows) -> dict:
    kinds = collections.Counter()
    for r in rows:
        if r["outcome"] == "cancelled":
            kinds[r["reason"].split(", exit")[0]] += 1
    return dict(kinds)


def report(rows: list[dict], blocked: list[str] | None = None) -> dict:
    blocked = blocked or []

    def counter(key):
        return dict(collections.Counter(str(r.get(key)) for r in rows))
    unfinished = [r["job_id"] for r in rows if not r["outcome"]]
    return {
        "jobs": len(rows),
        "unreadable": [r["job_id"] for r in rows if r["unreadable"]],
        "unfinished": unfinished,
        "outcomes": counter("outcome"),
        "acs_versions": counter("acs_version"),
        "projects": counter("project"),
        "backends": counter("backend"),
        "containers": counter("container"),
        "guard_outcomes": counter("guard_outcome"),
        "request_id_set": sum(1 for r in rows if r["has_request_id"]),
        "slice_set": sum(1 for r in rows if r["has_slice"]),
        "probe_jobs": sum(1 for r in rows if r["probe"]),
        "parent_job_set": sum(1 for r in rows if r["parent_job"]),
        "reconciled": [r["job_id"] for r in rows if r["reconciled"]],
        "unclean_cleanup": [r["job_id"] for r in rows
                            if r["cleanup_clean"] is False],
        "blocked": blocked,
        "unknown_without_record": unknown_without_record(rows, blocked),
        "dispatch_wait_s": summary(r["dispatch_wait_s"] for r in rows),
        "dispatch_waits_over_1h": sum(1 for r in rows
                                      if (r["dispatch_wait_s"] or 0) > 3600),
        "admission_wait_s": summary(r["admission_s"] for r in rows),
        "admission_waits_over_10min": sum(1 for r in rows
                                          if (r["admission_s"] or 0) > 600),
        "guard_attempts_over_1": dict(collections.Counter(
            str(r["attempts"]) for r in rows
            if r["attempts"] and r["attempts"] > 1)),
        "execution_s": summary(r["execution_s"] for r in rows),
        "hold_causes": hold_causes(rows),
        "cancel_kinds": cancel_kinds(rows),
        "failed_exit_codes": dict(collections.Counter(
            str(r["exit_code"]) for r in rows if r["outcome"] == "failed")),
        "executing_overlaps": executing_overlaps(rows),
        "dispatched_overlaps": dispatched_overlaps(rows),
        "busy_hours_per_day": busy_hours_per_day(rows),
        "busy_hours_untimed": busy_hours_untimed(rows),
        "jobs_per_day": dict(collections.Counter(
            r["accepted"].strftime("%Y-%m-%d") for r in rows
            if r["accepted"])),
    }


class SelectionError(Exception):
    """``--since`` names nothing this repository resolves to a commit."""


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess | None:
    try:
        return subprocess.run(["git", "-C", str(repo), *args],
                              capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None


def resolve_commit(repo: Path, ref: str) -> str | None:
    """The full sha of ``ref`` (a tag, a branch or a sha) in ``repo``'s
    history, or None (read-only ``git rev-parse``)."""
    if not ref or ref.startswith("-"):
        return None
    out = _git(repo, "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}")
    sha = out.stdout.strip() if out is not None and out.returncode == 0 \
        else ""
    return sha if re.fullmatch(r"[0-9a-f]{40}", sha) else None


def _counter(rows, key) -> dict:
    return dict(collections.Counter(str(r.get(key)) for r in rows))


def select(rows: list[dict], since: str | None = None,
           version: str | None = None, repo: Path = REPO):
    """The rows of one version (``version``, exact ``acs_version``) or of a
    commit and its descendants (``since``, by ``acs_source_sha``), and the
    selection record; ``(rows, None)`` without either.  Raises
    SelectionError when ``since`` does not resolve in ``repo``."""
    if since is None and version is None:
        return rows, None
    repo = Path(os.path.expanduser(str(repo)))
    since_commit = None
    if since is not None:
        since_commit = resolve_commit(repo, since)
        if since_commit is None:
            raise SelectionError(
                f"cannot resolve --since {since} to a commit in {repo} "
                "(a tag or sha of that repository's history; --repo names "
                "another checkout)")
    chosen, excluded, untagged, unresolved, unreadable = [], [], [], [], []
    descends: dict[str, bool | None] = {}
    for r in rows:
        if r["unreadable"]:
            unreadable.append(r["job_id"])
            continue
        if version is not None:
            if r["acs_version"] is None:
                untagged.append(r)
            elif r["acs_version"] == version:
                chosen.append(r)
            else:
                excluded.append(r)
            continue
        sha = r["acs_source_sha"]
        if sha is None:
            untagged.append(r)
            continue
        if sha not in descends:
            verdict = None
            full = resolve_commit(repo, sha) if SHA.fullmatch(sha) else None
            if full is not None:
                out = _git(repo, "merge-base", "--is-ancestor", since_commit,
                           full)
                if out is not None and out.returncode in (0, 1):
                    verdict = out.returncode == 0
            descends[sha] = verdict
        if descends[sha] is None:
            unresolved.append(r)
        elif descends[sha]:
            chosen.append(r)
        else:
            excluded.append(r)
    accepted = sorted(r["accepted_utc"] for r in untagged
                      if isinstance(r["accepted_utc"], str))
    selection = {
        "since": since, "since_commit": since_commit, "version": version,
        "repo": str(repo) if since is not None else None,
        "selected": len(chosen),
        "acs_versions": _counter(chosen, "acs_version"),
        "excluded": {"jobs": len(excluded),
                     "acs_versions": _counter(excluded, "acs_version")},
        "untagged": {"jobs": len(untagged),
                     "outcomes": _counter(untagged, "outcome"),
                     "accepted_utc": [accepted[0], accepted[-1]]
                     if accepted else []},
        "unresolved": {"jobs": [r["job_id"] for r in unresolved],
                       "source_shas": sorted({r["acs_source_sha"]
                                              for r in unresolved})},
        "unreadable": unreadable,
    }
    return chosen, selection


def human_selection(sel: dict) -> str:
    if sel["since"] is not None:
        what = (f"since {sel['since']} ({sel['since_commit'][:10]}) in "
                f"{sel['repo']}: jobs whose acs_source_sha is that commit or "
                "a descendant")
    else:
        what = f"version {sel['version']} (exact acs_version)"
    return "\n".join([
        f"selection: {what}",
        f"  selected: {sel['selected']} {sel['acs_versions']}",
        f"  excluded (earlier or unrelated): {sel['excluded']}",
        f"  untagged (no acs_source_sha or acs_version: accepted before "
        f"G2a): {sel['untagged']}",
        f"  unresolved (sha unknown to the repository): "
        f"{sel['unresolved']}",
        f"  unreadable: {sel['unreadable']}"])


def write_csv(rows: list[dict], path: Path) -> None:
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: ("" if r.get(k) is None else r.get(k))
                             for k in CSV_FIELDS})


def human(rep: dict) -> str:
    lines = [f"jobs: {rep['jobs']} (unfinished: {len(rep['unfinished'])}, "
             f"unreadable: {len(rep['unreadable'])})",
             f"outcomes: {rep['outcomes']}",
             f"acs versions: {rep['acs_versions']}",
             f"projects: {rep['projects']}",
             f"backends: {rep['backends']}",
             f"containers: {rep['containers']}",
             f"request_id set: {rep['request_id_set']}/{rep['jobs']}; "
             f"slice set: {rep['slice_set']}; probe jobs: {rep['probe_jobs']}; "
             f"parent_job set: {rep['parent_job_set']}",
             f"dispatch wait (accepted->dispatched): {rep['dispatch_wait_s']}; "
             f"over 1 h: {rep['dispatch_waits_over_1h']}",
             f"admission wait (dispatched->admitted): {rep['admission_wait_s']}; "
             f"over 10 min: {rep['admission_waits_over_10min']}",
             f"guard attempts > 1: {rep['guard_attempts_over_1']}",
             f"execution (guard wall): {rep['execution_s']}",
             f"hold causes: {rep['hold_causes']}",
             f"cancel kinds: {rep['cancel_kinds']}",
             f"failed exit codes: {rep['failed_exit_codes']}",
             f"unclean cleanup (unknown): {rep['unclean_cleanup']}",
             f"reconciled: {rep['reconciled']}",
             f"blocked now: {rep['blocked']}",
             f"unknown without a reconciled record: "
             f"{rep['unknown_without_record']}",
             f"executing overlaps (admitted..ended): "
             f"{rep['executing_overlaps']}",
             f"dispatched overlaps (dispatched..ended): "
             f"{rep['dispatched_overlaps']}",
             f"slot occupancy per UTC day (timed, split at midnight): "
             f"{rep['busy_hours_per_day']}",
             f"untimed executed jobs (wall_s, approximate): "
             f"{rep['busy_hours_untimed']}",
             f"jobs per day: {rep['jobs_per_day']}"]
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only aggregate report over an agent-compute state "
                    "directory.")
    parser.add_argument("--state", default=os.environ.get(
        "AGENT_COMPUTE_STATE", "~/.local/state/agent-compute"))
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--csv", metavar="FILE")
    which = parser.add_mutually_exclusive_group()
    which.add_argument("--since", metavar="TAG_OR_SHA",
                       help="only jobs whose manifest acs_source_sha is this "
                            "commit or a descendant (in --repo's history)")
    which.add_argument("--version", metavar="NAME",
                       help="only jobs whose manifest acs_version is exactly "
                            "NAME (an installed version name)")
    parser.add_argument("--repo", default=str(REPO),
                        help="the ACS checkout whose history --since is read "
                             "from (default: this tool's checkout)")
    args = parser.parse_args(argv)
    state_dir = Path(os.path.expanduser(args.state))
    rows = load_jobs(state_dir)
    try:
        rows, selection = select(rows, since=args.since, version=args.version,
                                 repo=Path(args.repo))
    except SelectionError as exc:
        print(f"ledger_report: {exc}", file=sys.stderr)
        return 2
    rep = report(rows, read_blocks(state_dir))
    if selection is not None:
        rep["selection"] = selection
    if args.csv:
        write_csv(rows, Path(args.csv))
    if args.json:
        json.dump(rep, sys.stdout, indent=1, default=str)
        print()
    else:
        print(f"state: {state_dir}")
        if selection is not None:
            print(human_selection(selection))
        print(human(rep))
    return 0


if __name__ == "__main__":
    sys.exit(main())
