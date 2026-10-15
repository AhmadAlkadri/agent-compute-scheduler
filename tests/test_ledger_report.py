"""tools/ledger_report.py: the read-only aggregate over a state directory
(dogfooding investigation, recorded privately).

What this establishes: the report derives every number from the recorded
state files alone, with the definitions the investigation used (as
corrected by the PR #24 review, §1-§5 of its private record):
``dispatch_wait_s`` is accepted -> dispatched, ``admission_s`` is
dispatched -> admitted, ``execution_s`` is the guard's ``wall_s``; an
executing overlap is every pair of jobs whose admitted..ended intervals
intersect, with the true intersection, touching pairs counted apart and
executed jobs without both timestamps listed as untimed; slot occupancy is
split at UTC midnight; an ``unknown`` outcome without a reconciliation
record is flagged; a job directory whose records cannot be read is
reported, never dropped.  It runs on a synthetic state directory: no
guard, no queue, no live state.
"""
from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
TOOL = REPO / "tools" / "ledger_report.py"
sys.path.insert(0, str(REPO / "tools"))
import ledger_report  # noqa: E402


def make_job(state: Path, job_id: str, manifest: dict, st: dict) -> None:
    job_dir = state / "jobs" / job_id
    job_dir.mkdir(parents=True)
    (job_dir / "manifest.json").write_text(json.dumps(manifest))
    (job_dir / "state.json").write_text(json.dumps(st))


def synthetic_state(tmp_path: Path) -> Path:
    state = tmp_path / "state"
    make_job(state, "acs-1", {"project": "alpha", "request_id": "r1",
                              "slice": "S1", "backend": {"kind": "acs-guard"},
                              "container": None, "resources": {"probe": False}},
             {"accepted_utc": "2026-10-01T00:00:00Z",
              "dispatched_utc": "2026-10-01T00:00:10Z",
              "admitted_utc": "2026-10-01T00:00:12Z",
              "ended_utc": "2026-10-01T00:10:12Z", "wall_s": 600.0,
              "attempt": 1, "outcome": "succeeded", "phase": "done",
              "guard_outcome": "completed", "exit_code": 0,
              "cleanup": {"verified": True, "clean": True},
              "reason": "guard outcome completed, exit 0"})
    # waited in the queue behind acs-1, then 3 guard attempts
    make_job(state, "acs-2", {"project": "beta", "request_id": None,
                              "slice": None, "backend": {"kind": "smc-guard"},
                              "container": {"name": "c1"},
                              "resources": {"probe": True}},
             {"accepted_utc": "2026-10-01T00:00:05Z",
              "dispatched_utc": "2026-10-01T00:10:13Z",
              "admitted_utc": "2026-10-01T00:20:13Z",
              "ended_utc": "2026-10-01T00:21:13Z", "wall_s": 60.0,
              "attempt": 3, "outcome": "unknown", "phase": "done",
              "guard_outcome": "completed", "exit_code": 0,
              "cleanup": {"verified": True, "clean": False},
              "reconciled": {"utc": "2026-10-01T01:00:00Z"},
              "reason": "workload cleanup could not be established"})
    make_job(state, "acs-3", {"project": "alpha", "request_id": "r3",
                              "backend": {"kind": "acs-guard"}},
             {"accepted_utc": "2026-10-01T02:00:00Z",
              "dispatched_utc": "2026-10-01T02:00:01Z",
              "ended_utc": "2026-10-01T02:00:02Z",
              "outcome": "held-inputs-changed", "phase": "done",
              "reason": "inputs changed while queued (git_head); resubmit "
                        "against the new inputs; never started"})
    make_job(state, "acs-4", {"project": "alpha", "request_id": "r4",
                              "backend": {"kind": "acs-guard"}},
             {"accepted_utc": "2026-10-01T03:00:00Z",
              "outcome": "cancelled", "phase": "done",
              "ended_utc": "2026-10-01T03:00:30Z",
              "reason": "cancelled by request: removed from the queue before "
                        "it started; never started"})
    make_job(state, "acs-5", {"project": "alpha"},
             {"accepted_utc": "2026-10-01T04:00:00Z", "phase": "queued"})
    # a corrupt record must be reported, never dropped
    broken = state / "jobs" / "acs-6"
    broken.mkdir()
    (broken / "manifest.json").write_text("{")
    (broken / "state.json").write_text("{}")
    return state


def test_report_derives_the_investigations_measures(tmp_path):
    state = synthetic_state(tmp_path)
    rows = ledger_report.load_jobs(state)
    rep = ledger_report.report(rows)
    assert rep["jobs"] == 6
    assert rep["unreadable"] == ["acs-6"]
    assert rep["unfinished"] == ["acs-5", "acs-6"]
    assert rep["outcomes"]["succeeded"] == 1
    assert rep["outcomes"]["unknown"] == 1
    assert rep["outcomes"]["held-inputs-changed"] == 1
    by_id = {r["job_id"]: r for r in rows}
    assert by_id["acs-1"]["dispatch_wait_s"] == 10.0
    assert by_id["acs-1"]["admission_s"] == 2.0
    assert by_id["acs-1"]["execution_s"] == 600.0
    assert by_id["acs-2"]["dispatch_wait_s"] == 608.0  # behind acs-1
    assert by_id["acs-2"]["admission_s"] == 600.0      # 3 refused attempts
    assert rep["guard_attempts_over_1"] == {"3": 1}
    assert rep["dispatch_wait_s"]["n"] == 3
    assert rep["dispatch_wait_s"]["max"] == 608.0
    assert rep["request_id_set"] == 3 and rep["probe_jobs"] == 1
    assert rep["reconciled"] == ["acs-2"]
    assert rep["unclean_cleanup"] == ["acs-2"]
    assert rep["hold_causes"] == {
        "inputs changed while queued (git_head)": 1}
    assert rep["cancel_kinds"] == {
        "cancelled by request: removed from the queue before it started; "
        "never started": 1}
    assert rep["executing_overlaps"] == {
        "checked": 2, "executed": 2, "untimed_executed": [], "pairs": [],
        "touching_pairs": 0}
    assert rep["dispatched_overlaps"] == {"checked": 3, "pairs": [],
                                          "touching_pairs": 0}
    assert rep["busy_hours_per_day"] == {"2026-10-01": round(660 / 3600, 2)}
    assert rep["unknown_without_record"] == {"blocked_now": [],
                                             "cleared_without_record": []}


def interval_job(state: Path, job_id: str, start: str, end: str,
                 **extra) -> None:
    make_job(state, job_id, {"project": "p"},
             {"admitted_utc": f"2026-10-01T{start}Z",
              "ended_utc": f"2026-10-01T{end}Z", "outcome": "succeeded",
              **extra})


def overlap_pairs(state: Path) -> list:
    rep = ledger_report.report(ledger_report.load_jobs(state))
    return rep["executing_overlaps"]["pairs"]


def test_overlap_detection_sees_two_jobs_executing_at_once(tmp_path):
    # B lies inside A: the intersection is B's 10 minutes, not end(A)-start(B)
    state = tmp_path / "s"
    interval_job(state, "acs-a", "00:00:00", "01:00:00")
    interval_job(state, "acs-b", "00:30:00", "00:40:00")
    assert overlap_pairs(state) == [("acs-a", "acs-b", 600.0)]


def test_overlap_enumerates_every_intersecting_pair(tmp_path):
    # review §2 counterexample: (A, C) is not adjacent once sorted by start
    state = tmp_path / "s"
    interval_job(state, "acs-a", "00:00:00", "01:00:00")
    interval_job(state, "acs-b", "00:10:00", "00:20:00")
    interval_job(state, "acs-c", "00:30:00", "00:40:00")
    assert overlap_pairs(state) == [("acs-a", "acs-b", 600.0),
                                    ("acs-a", "acs-c", 600.0)]


def test_touching_intervals_are_counted_not_reported_as_overlaps(tmp_path):
    state = tmp_path / "s"
    interval_job(state, "acs-a", "00:00:00", "00:10:00")
    interval_job(state, "acs-b", "00:10:00", "00:20:00")
    interval_job(state, "acs-c", "00:20:01", "00:30:00")
    rep = ledger_report.report(ledger_report.load_jobs(state))
    assert rep["executing_overlaps"]["pairs"] == []
    assert rep["executing_overlaps"]["touching_pairs"] == 1
    assert rep["executing_overlaps"]["checked"] == 3


def test_dispatched_overlap_check_covers_untimed_jobs(tmp_path):
    state = tmp_path / "s"
    make_job(state, "acs-a", {"project": "p"},
             {"dispatched_utc": "2026-10-01T00:00:00Z",
              "admitted_utc": "2026-10-01T00:00:05Z",
              "ended_utc": "2026-10-01T00:10:00Z", "outcome": "succeeded"})
    # untimed (no admitted_utc) but dispatched while acs-a still ran
    make_job(state, "acs-b", {"project": "p"},
             {"dispatched_utc": "2026-10-01T00:09:00Z",
              "ended_utc": "2026-10-01T00:12:00Z", "wall_s": 0.6,
              "outcome": "failed"})
    rep = ledger_report.report(ledger_report.load_jobs(state))
    assert rep["executing_overlaps"]["pairs"] == []
    assert rep["dispatched_overlaps"] == {
        "checked": 2, "pairs": [("acs-a", "acs-b", 60.0)], "touching_pairs": 0}


def test_busy_hours_split_at_utc_midnight(tmp_path):
    state = tmp_path / "s"
    make_job(state, "acs-a", {"project": "p"},
             {"admitted_utc": "2026-10-01T23:00:00Z",
              "ended_utc": "2026-10-02T01:00:00Z", "outcome": "succeeded"})
    rep = ledger_report.report(ledger_report.load_jobs(state))
    assert rep["busy_hours_per_day"] == {"2026-10-01": 1.0, "2026-10-02": 1.0}


def test_executed_job_without_admitted_utc_is_untimed(tmp_path):
    # a container job whose in-container pid was never resolved (A20's
    # 77d1b8): wall_s and ended_utc, no admitted_utc
    state = tmp_path / "s"
    interval_job(state, "acs-a", "00:00:00", "00:10:00")
    make_job(state, "acs-u", {"project": "p"},
             {"dispatched_utc": "2026-10-01T22:30:00Z",
              "ended_utc": "2026-10-02T00:30:00Z", "wall_s": 3600.0,
              "outcome": "unknown", "reconciled": {"utc": "x"}})
    make_job(state, "acs-q", {"project": "p"},          # never executed
             {"accepted_utc": "2026-10-01T00:00:00Z", "phase": "queued"})
    rep = ledger_report.report(ledger_report.load_jobs(state))
    ov = rep["executing_overlaps"]
    assert ov["executed"] == 2 and ov["checked"] == 1
    assert ov["untimed_executed"] == ["acs-u"]
    assert rep["busy_hours_per_day"] == {"2026-10-01": round(600 / 3600, 2)}
    assert rep["busy_hours_untimed"] == {
        "jobs": ["acs-u"],
        "wall_hours_per_day": {"2026-10-01": 0.5, "2026-10-02": 0.5}}


def test_unknown_without_reconciliation_is_flagged(tmp_path):
    state = tmp_path / "s"
    # reconciled by the current reconcile: history entry and key
    interval_job(state, "acs-r", "00:00:00", "00:01:00", outcome="unknown",
                 history=[{"event": "reconciled", "utc": "x"}],
                 reconciled={"utc": "x"})
    # history entry only (still counts as recorded)
    interval_job(state, "acs-h", "00:02:00", "00:03:00", outcome="unknown",
                 history=[{"event": "reconciled", "utc": "x"}])
    # no record, no block: cleared without record (review §4, 500618)
    interval_job(state, "acs-c", "00:04:00", "00:05:00", outcome="unknown")
    # no record, still in the host block
    interval_job(state, "acs-b", "00:06:00", "00:07:00", outcome="unknown")
    (state / "blocks").mkdir()
    (state / "blocks" / "host.json").write_text(json.dumps(
        {"host": "host", "jobs": {"acs-b": {"reason": "r", "utc": "x"}}}))
    rows = ledger_report.load_jobs(state)
    rep = ledger_report.report(rows, ledger_report.read_blocks(state))
    assert rep["reconciled"] == ["acs-h", "acs-r"]
    assert rep["blocked"] == ["acs-b"]
    assert rep["unknown_without_record"] == {
        "blocked_now": ["acs-b"], "cleared_without_record": ["acs-c"]}


def test_cli_json_and_csv_carry_no_argv_or_env(tmp_path):
    state = synthetic_state(tmp_path)
    # a manifest with argv/env that must never reach the CSV
    man = json.loads((state / "jobs" / "acs-1" / "manifest.json").read_text())
    man["argv"] = ["python3", "secret-script.py", "--token", "SENSITIVE"]
    man["env"] = {"PATH": "/private/path"}
    (state / "jobs" / "acs-1" / "manifest.json").write_text(json.dumps(man))
    out_csv = tmp_path / "jobs.csv"
    result = subprocess.run([sys.executable, str(TOOL), "--state", str(state),
                             "--json", "--csv", str(out_csv)],
                            capture_output=True, text=True, check=True)
    rep = json.loads(result.stdout)
    assert rep["jobs"] == 6
    text = out_csv.read_text()
    assert "SENSITIVE" not in text and "/private/path" not in text
    rows = list(csv.DictReader(out_csv.open()))
    assert [r["job_id"] for r in rows] == [f"acs-{i}" for i in range(1, 7)]
    assert rows[0]["dispatch_wait_s"] == "10.0"
    assert rows[0]["outcome"] == "succeeded"
    assert set(rows[0]) == set(ledger_report.CSV_FIELDS)


def test_missing_state_dir_is_an_empty_report(tmp_path):
    rep = ledger_report.report(ledger_report.load_jobs(tmp_path / "nope"))
    assert rep["jobs"] == 0
    assert rep["executing_overlaps"]["pairs"] == []
    assert rep["executing_overlaps"]["checked"] == 0


# --- per-version reports: --since <tag-or-sha>, --version <name> (G7) --------------
#
# What this establishes: the report can be restricted to the jobs a given
# ACS release (and its successors) accepted, decided from each job
# manifest's ``acs_source_sha`` against the repository's history (``git
# merge-base --is-ancestor``), or to one installed version name exactly.
# Jobs without ``acs_source_sha`` (accepted before G2a) are reported apart
# as untagged, a sha the repository does not know as unresolved, and an
# unreadable manifest by job id: none is dropped silently or counted as
# "earlier".  An unresolvable ``--since`` is an error, never an empty
# report.  The state directory is not modified.

GIT = ["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid",
       "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false",
       "-c", "init.defaultBranch=main"]


def git(repo: Path, *args: str) -> str:
    out = subprocess.run([*GIT, "-C", str(repo), *args], capture_output=True,
                         text=True, check=True)
    return out.stdout.strip()


def history(tmp_path: Path) -> tuple[Path, dict]:
    """c1 -> c2 (tag v1) -> c3 on main; s1 on a side branch from c1."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q")
    shas = {}
    for name in ("c1", "c2", "c3"):
        (repo / "f").write_text(name)
        git(repo, "add", "f")
        git(repo, "commit", "-q", "-m", name)
        shas[name] = git(repo, "rev-parse", "HEAD")
        if name == "c2":
            git(repo, "tag", "-a", "v1", "-m", "v1")
    git(repo, "checkout", "-q", "-b", "side", shas["c1"])
    (repo / "g").write_text("s1")
    git(repo, "add", "g")
    git(repo, "commit", "-q", "-m", "s1")
    shas["s1"] = git(repo, "rev-parse", "HEAD")
    git(repo, "checkout", "-q", "main")
    return repo, shas


def versioned_state(tmp_path: Path, shas: dict) -> Path:
    state = tmp_path / "vstate"

    def job(job_id, version, sha, outcome="succeeded",
            accepted="2026-10-09T01:00:00Z"):
        man = {"project": "p"}
        if version is not None:
            man["acs_version"] = version
        if sha is not None:
            man["acs_source_sha"] = sha
        make_job(state, job_id, man, {"accepted_utc": accepted,
                                      "outcome": outcome, "phase": "done"})
    job("acs-c1", f"0.1.9-{shas['c1'][:10]}", shas["c1"])
    job("acs-c2", f"0.2.0-{shas['c2'][:10]}", shas["c2"])
    # an older version dir without a SOURCE file: the name's sha10 only
    job("acs-c3", f"0.2.1-{shas['c3'][:10]}", shas["c3"][:10], "failed")
    job("acs-s1", f"0.2.0-{shas['s1'][:10]}", shas["s1"])
    job("acs-old1", None, None, "cancelled", "2026-10-01T00:00:00Z")
    job("acs-old2", None, None, "succeeded", "2026-10-02T00:00:00Z")
    job("acs-bad", "0.2.0-deadbeef00", "deadbeef00" * 4)
    broken = state / "jobs" / "acs-zz"
    broken.mkdir()
    (broken / "manifest.json").write_text("{")
    (broken / "state.json").write_text("{}")
    return state


def test_since_keeps_the_jobs_at_or_after_the_commit(tmp_path):
    repo, shas = history(tmp_path)
    state = versioned_state(tmp_path, shas)
    rows = ledger_report.load_jobs(state)
    chosen, sel = ledger_report.select(rows, since="v1", repo=repo)
    assert [r["job_id"] for r in chosen] == ["acs-c2", "acs-c3"]
    assert sel["since"] == "v1" and sel["since_commit"] == shas["c2"]
    assert sel["selected"] == 2
    # c1 is before v1; s1 is on a branch v1 is not an ancestor of
    assert sel["excluded"] == {"jobs": 2, "acs_versions": {
        f"0.1.9-{shas['c1'][:10]}": 1, f"0.2.0-{shas['s1'][:10]}": 1}}
    assert sel["untagged"] == {
        "jobs": 2, "outcomes": {"cancelled": 1, "succeeded": 1},
        "accepted_utc": ["2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z"]}
    assert sel["unresolved"] == {"jobs": ["acs-bad"],
                                 "source_shas": ["deadbeef00" * 4]}
    assert sel["unreadable"] == ["acs-zz"]
    assert sel["acs_versions"] == {f"0.2.0-{shas['c2'][:10]}": 1,
                                   f"0.2.1-{shas['c3'][:10]}": 1}
    rep = ledger_report.report(chosen)
    assert rep["jobs"] == 2
    assert rep["outcomes"] == {"succeeded": 1, "failed": 1}
    assert rep["acs_versions"] == sel["acs_versions"]


def test_since_takes_a_sha_and_includes_that_commit(tmp_path):
    repo, shas = history(tmp_path)
    rows = ledger_report.load_jobs(versioned_state(tmp_path, shas))
    chosen, sel = ledger_report.select(rows, since=shas["c3"][:7], repo=repo)
    assert [r["job_id"] for r in chosen] == ["acs-c3"]
    assert sel["since_commit"] == shas["c3"]
    chosen, _ = ledger_report.select(rows, since=shas["c1"], repo=repo)
    assert [r["job_id"] for r in chosen] == ["acs-c1", "acs-c2", "acs-c3",
                                             "acs-s1"]


def test_version_is_an_exact_match(tmp_path):
    repo, shas = history(tmp_path)
    rows = ledger_report.load_jobs(versioned_state(tmp_path, shas))
    name = f"0.2.0-{shas['c2'][:10]}"
    chosen, sel = ledger_report.select(rows, version=name, repo=repo)
    assert [r["job_id"] for r in chosen] == ["acs-c2"]
    assert sel["version"] == name and sel["selected"] == 1
    assert sel["untagged"]["jobs"] == 2
    assert sel["unreadable"] == ["acs-zz"]
    # a prefix is not a match
    chosen, _ = ledger_report.select(rows, version="0.2.0", repo=repo)
    assert chosen == []


def test_an_unresolvable_since_is_an_error_not_an_empty_report(tmp_path):
    repo, shas = history(tmp_path)
    state = versioned_state(tmp_path, shas)
    for since, where in (("v9", repo), ("v1", tmp_path / "not-a-repo")):
        result = subprocess.run(
            [sys.executable, str(TOOL), "--state", str(state), "--repo",
             str(where), "--since", since, "--json"],
            capture_output=True, text=True)
        assert result.returncode == 2, result
        assert result.stdout == ""
        assert f"cannot resolve --since {since}" in result.stderr


def test_since_and_version_exclude_each_other(tmp_path):
    result = subprocess.run(
        [sys.executable, str(TOOL), "--state", str(tmp_path), "--since", "v1",
         "--version", "x"], capture_output=True, text=True)
    assert result.returncode == 2
    assert "not allowed with" in result.stderr


def test_cli_since_json_and_csv_without_touching_the_state(tmp_path):
    repo, shas = history(tmp_path)
    state = versioned_state(tmp_path, shas)

    def snapshot():
        return sorted((str(p.relative_to(state)), p.stat().st_mtime_ns,
                       p.stat().st_size) for p in state.rglob("*"))
    before = snapshot()
    out_csv = tmp_path / "jobs.csv"
    result = subprocess.run(
        [sys.executable, str(TOOL), "--state", str(state), "--repo",
         str(repo), "--since", "v1", "--json", "--csv", str(out_csv)],
        capture_output=True, text=True, check=True)
    rep = json.loads(result.stdout)
    assert rep["jobs"] == 2
    assert rep["selection"]["since_commit"] == shas["c2"]
    assert rep["selection"]["untagged"]["jobs"] == 2
    rows = list(csv.DictReader(out_csv.open()))
    assert [r["job_id"] for r in rows] == ["acs-c2", "acs-c3"]
    assert rows[0]["acs_version"] == f"0.2.0-{shas['c2'][:10]}"
    human = subprocess.run(
        [sys.executable, str(TOOL), "--state", str(state), "--repo",
         str(repo), "--since", "v1"], capture_output=True, text=True,
        check=True).stdout
    assert f"since v1 ({shas['c2'][:10]})" in human
    assert "untagged (no acs_source_sha" in human
    assert snapshot() == before


def test_without_a_selection_every_job_is_reported(tmp_path):
    repo, shas = history(tmp_path)
    rows = ledger_report.load_jobs(versioned_state(tmp_path, shas))
    chosen, sel = ledger_report.select(rows, repo=repo)
    assert len(chosen) == 8 and sel is None
    rep = ledger_report.report(chosen)
    assert rep["acs_versions"]["None"] == 3   # two untagged, one unreadable
