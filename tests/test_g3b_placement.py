"""Campaign G slice G3b: automatic placement and lost-reply resolution
(decisions D5, D6; basis record Part 2 items 6, 8 and decisions 1-10;
register A40).

Every remote is reached through the fake ssh of ``tests/test_placement.py``
(``Pair``: a near Env, label ``testhost``, the placing host; a far Env,
label ``far``).  Nothing here reaches a real host.

What this establishes:

* the policy table (``placement.evaluate``, from synthetic summaries):
  every reason a host is not a candidate (unreachable, unsuitable,
  blocked), busy and the tie rules (fewest non-terminal jobs; a tie to the
  local host, then the project's host order); nothing eligible -> nothing
  chosen;
* a placed submit (no ``--host``, a project declaring several hosts): the
  read-only probes, the record committed here (with the policy snapshot)
  before anything is sent and before anything is written on the
  destination, one send to the chosen host with the ``on.<host>`` mapping
  and the code requirement, a request id generated when absent; the reply
  lists every host with ``chosen`` or why not (with ``raw``); no eligible
  host -> exit 2, nothing written anywhere; the chosen host refusing, or
  its reply lost, never causes a second send;
* the code requirement: ``--require-commit`` (the placing checkout's HEAD)
  and ``--require-clean`` (when it is clean) refused at submit (both shas
  named) and at dispatch (never started); ``--any-commit``;
* lost replies: ``placements resolve``/``status``/``hosts`` query the same
  host by request id (read-only there): found -> accepted; not found ->
  ``reply-lost`` within the grace, ``resolved-not-accepted`` after it
  while the host answers; unreachable -> stays; the grace is configurable
  (at least 900 s) and covers the send's own call timeout; a re-send waits
  for the resolution and goes to the same host only;
* A40: a routed call is never routed on (two hosts whose records point at
  each other do not bounce a ``status``);
* visibility: the records' state and age in ``status --all`` and
  ``hosts``; a placed job's view carries the policy snapshot;
* a project declaring one host is unchanged (no probe, no record).
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import os
import re
import subprocess

import pytest

from conftest import BIN, WORKLOAD, Env, git
from test_placement import PY, Pair, _with_hosts, snapshot

from agent_compute import cli
from agent_compute import config as cfgmod
from agent_compute import placement as pl
from agent_compute.state import write_json


# --- helpers ------------------------------------------------------------------------------

def head_of(repo) -> str:
    return subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                          capture_output=True, text=True,
                          check=True).stdout.strip()


def align(p: Pair) -> None:
    """The far checkout of alpha at the near one's commit (two clones of
    one history)."""
    far, near = p.far.projects["alpha"], p.near.projects["alpha"]
    git(far, "fetch", "-q", str(near), "main")
    git(far, "reset", "-q", "--hard", "FETCH_HEAD")
    assert head_of(far) == head_of(near)


def placed_pair(*, far_queue: bool = False, order='["testhost", "far"]',
                mapping: str | None = "", near_extra="", **kw) -> Pair:
    """A Pair whose near config declares alpha on both hosts, with
    ``on.far.cwd`` = the far checkout (plus ``mapping``), both checkouts
    at one commit."""
    p = Pair(far_queue=far_queue, near_extra=near_extra, **kw)
    text = _with_hosts(p.near.config.read_text(), order)
    if mapping is not None:
        text += (f'[projects.alpha.on.far]\ncwd = "{p.far.projects["alpha"]}"'
                 f'\n{mapping}')
    p.near.config.write_text(text)
    align(p)
    return p


def block_here(env: Env) -> None:
    write_json(env.state / "blocks" / "testhost.json",
               {"jobs": {"acs-20261008T000000Z-b10c4e":
                         {"reason": "test block"}},
                "reason": "test block"})


def placed(p: Pair, *extra, name="w", sleep=0, cmd="submit", modes=None,
           request_id=None, cwd=None, **env_extra):
    args = [cmd, "--json", "--case", "t", "--timeout-min", "1",
            "--reserve-gb", "0.5", "--cwd",
            str(cwd or p.near.projects["alpha"])]
    if request_id:
        args += ["--request-id", request_id]
    return p.acs(*args, *extra, "--", PY, str(WORKLOAD), "--out",
                 str(p.far.root / f"{name}.jsonl"), "--sleep", str(sleep),
                 modes=modes, **env_extra)


def scripts(p: Pair) -> list[tuple[str, str]]:
    """Every call the fake ssh saw: (target, the decoded remote script)."""
    found = []
    for line in p.ssh_log.read_text().splitlines() if p.ssh_log.exists() \
            else []:
        words = line.split()
        target = next(w for w in words if not w.startswith("-") and
                      "=" not in w)
        match = re.search(r"echo ([A-Za-z0-9+/=]+) \|", line)
        found.append((target, base64.b64decode(match.group(1)).decode()
                      if match else ""))
    return found


def subcommands(p: Pair) -> list[str]:
    from agent_compute import remote
    return [remote.command_of(script.split("\n")[0].split()[2:])
            for _, script in scripts(p)]


def backdate(p: Pair, request_id: str, seconds: float) -> None:
    """The record's send moved ``seconds`` into the past (the grace)."""
    path = p.near.state / "placements" / f"{request_id}.json"
    record = json.loads(path.read_text())
    then = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=seconds)
    record["sent_utc"] = then.strftime("%Y-%m-%dT%H:%M:%SZ")
    path.write_text(json.dumps(record))


def resolve(p: Pair, *ids, modes=None) -> dict:
    r = p.acs("placements", "resolve", *ids, "--json", modes=modes)
    assert r.returncode == 0, r.stdout + r.stderr
    return json.loads(r.stdout)


# --- the policy table (placement.evaluate) ---------------------------------------------------

def S(host, *, host_id=None, unfinished=0, project="alpha", hosts=None,
      cwd_root=None, head=None, dirty=False, **over) -> dict:
    """A synthetic ``_summary`` of ``host`` that is eligible unless
    ``over`` says otherwise."""
    summary = {
        "schema": cli.SUMMARY_SCHEMA, "host": host,
        "host_id": host_id or f"id-{host}", "version": "0.2.0-test",
        "floor": {"ok": True, "problems": []},
        "pin": {"ok": True}, "blocked": False, "gate": "open",
        "gate_reason": None, "dispatch": {"held": False},
        "lost_suspected": [],
        "jobs": {"unfinished": unfinished},
        "budget": {"effective_budget_gb": 8.0},
        "projects": {project: {"hosts": hosts or [host],
                               "checkouts": [{"path": cwd_root or
                                              f"/srv/{host}/alpha",
                                              "head": head or "a" * 40,
                                              "dirty": dirty}]}}}
    summary.update(over)
    return summary


def probe(summary=None, state="ok", reasons=()) -> dict:
    return {"state": state, "summary": summary, "reasons": list(reasons),
            "raw": {"rc": 0 if state == "ok" else 255, "stdout": "x"}}


def decide(order, probes, local="mac", **kw):
    cwd = {h: f"/srv/{h}/alpha" for h in order if h != local}
    kw.setdefault("cwd", cwd)
    return pl.evaluate("alpha", order, local, probes, **kw)


def statuses(decision) -> list[tuple[str, str]]:
    return [(c["host"], c["status"]) for c in decision["candidates"]]


def test_policy_fewest_unfinished_jobs_wins():
    d = decide(["mac", "lenovo"], {"mac": probe(S("mac", unfinished=2)),
                                   "lenovo": probe(S("lenovo",
                                                     unfinished=1))})
    assert d["chosen"] == "lenovo"
    assert statuses(d) == [("mac", "busy"), ("lenovo", "chosen")]
    assert d["candidates"][0]["reasons"] == ["2 unfinished job(s); lenovo "
                                             "has 1"]
    assert d["candidates"][0]["unfinished"] == 2
    assert d["rule"] == pl.RULE


def test_policy_tie_goes_to_the_local_host_then_config_order():
    probes = {h: probe(S(h, unfinished=1)) for h in ("a", "mac", "b")}
    d = decide(["a", "mac", "b"], probes)
    assert d["chosen"] == "mac"                       # local, though second
    assert statuses(d) == [("a", "busy"), ("mac", "chosen"), ("b", "busy")]
    assert "the local host (mac)" in d["candidates"][0]["reasons"][0]
    # the local host not tied (or not listed): config order
    probes["mac"] = probe(S("mac", unfinished=3))
    d = decide(["b", "mac", "a"], probes)
    assert d["chosen"] == "b"
    assert "earlier in the project's host order" in \
        d["candidates"][2]["reasons"][0]
    d = decide(["a", "b"], {h: probes[h] for h in ("a", "b")})
    assert d["chosen"] == "a"


REASONS = [
    # (what the far host reports, its status, a word of the reason)
    (probe(None, "unreachable", ["ssh exited 255"]), "unreachable", "255"),
    (probe(None, "timeout", ["no reply within 30 s"]), "unreachable",
     "no reply"),
    (probe(None, "error", ["the route could not be used"]), "unreachable",
     "route"),
    (probe(None, "old-remote", ["no `_summary`"]), "unsuitable",
     "_summary"),
    (probe(None, "unparseable", ["exit 0 without a summary object"]),
     "unsuitable", "summary object"),
    (probe(S("other")), "unsuitable", "renamed"),
    (probe(S("far", host_id="id-mac")), "unsuitable", "same host id"),
    (probe(S("far", floor={"ok": False, "problems": ["level 3"]})),
     "unsuitable", "safety floor"),
    (probe(S("far", pin={"ok": False})), "unsuitable", "pinned sha256"),
    (probe(S("far", blocked=True)), "blocked", "blocked"),
    (probe(S("far", blocked="unreadable")), "blocked", "unreadable"),
    (probe(S("far", gate="closed", gate_reason="restart")), "blocked",
     "gate closed"),
    (probe(S("far", gate="unreadable")), "blocked", "gate record"),
    (probe(S("far", dispatch={"held": True, "reason": "paused"})),
     "blocked", "held"),
    (probe(S("far", lost_suspected=[{"job_id": "acs-x"}])), "blocked",
     "lost supervision"),
    (probe(S("far", projects={"beta": {}})), "unsuitable",
     "not configured"),
    (probe(S("far", hosts=["mac"])), "unsuitable", "does not list"),
    (probe(S("far", budget={"effective_budget_gb": 2.0})), "unsuitable",
     "below the declared reserve"),
    (probe(S("far", jobs={})), "unsuitable", "count"),
    (probe(S("far", head="b" * 40)), "unsuitable", "not the required"),
    (probe(S("far", dirty=True)), "unsuitable", "uncommitted"),
]


@pytest.mark.parametrize("far,status,word", REASONS)
def test_policy_reports_why_a_host_is_not_a_candidate(far, status, word):
    d = decide(["mac", "far"], {"mac": probe(S("mac", unfinished=5)),
                                "far": far},
               reserve_gb=4.0, required_commit="a" * 40, require_clean=True)
    # never the far one (a host id shared with mac: neither, one
    # installation under two names)
    assert d["chosen"] == (None if word == "same host id" else "mac")
    [_, item] = d["candidates"]
    assert item["status"] == status, item
    assert any(word in reason for reason in item["reasons"]), item
    assert item["raw"] == far["raw"]            # the reply, verbatim


def test_policy_per_job_conditions():
    good = {"mac": probe(S("mac", unfinished=5)), "far": probe(S("far"))}
    # unknown budget or no declared reserve: not a reason
    unknown = {**good, "far": probe(S("far", budget={}))}
    assert decide(["mac", "far"], unknown, reserve_gb=4.0)["chosen"] == "far"
    assert decide(["mac", "far"], good, reserve_gb=None)["chosen"] == "far"
    # no cwd mapping for a remote: unsuitable
    d = decide(["mac", "far"], good, cwd={})
    assert statuses(d)[1] == ("far", "unsuitable")
    assert "on.far.cwd" in d["candidates"][1]["reasons"][0]
    # a container job without a container mapping there: unsuitable
    d = decide(["mac", "far"], good, container={"far": None})
    assert "container" in " ".join(d["candidates"][1]["reasons"])
    assert decide(["mac", "far"], good,
                  container={"far": "box"})["chosen"] == "far"
    # a checkout that is not reported (a pattern root): not compared
    d = decide(["mac", "far"], good, cwd={"far": "/elsewhere"},
               required_commit="b" * 40, require_clean=True)
    assert d["chosen"] == "far"
    assert d["candidates"][1]["head"] is None
    # the reported HEAD is in the candidate
    d = decide(["mac", "far"], good)
    assert d["candidates"][1]["head"] == "a" * 40
    assert d["candidates"][1]["dirty"] is False


def test_policy_with_no_candidate_chooses_nothing():
    d = decide(["mac", "far"], {"mac": probe(S("mac", blocked=True)),
                                "far": probe(None, "unreachable",
                                             ["refused"])})
    assert d["chosen"] is None
    assert statuses(d) == [("mac", "blocked"), ("far", "unreachable")]


def test_grace_is_never_below_900_s_nor_the_send_timeout_plus_900_s():
    """Review round 1, finding 1: the client's call timeout does not bound
    the remote command, so the grace is the send's call timeout PLUS the
    destination's own grace (900 s), or the route's reply_grace_s when
    longer."""
    assert pl.grace_s(None, {}) == 900.0                 # this host's own
    assert pl.grace_s(2000, {}) == 2000.0
    assert pl.grace_s(None, {"replies": [{"timeout_s": 1800.0}]}) == 2700.0
    assert pl.grace_s(None, {"send": {"call_timeout_s": 900.0}}) == 1800.0
    assert pl.grace_s(3000, {"send": {"call_timeout_s": 900.0}}) == 3000.0
    # a record without a known timeout (G3a) gets the default call timeout
    assert pl.grace_s(None, {}, 900.0) == 1800.0
    # a resolution query's own timeout does not count
    assert pl.grace_s(None, {"replies": [{"timeout_s": 5000.0,
                                          "kind": "resolution"}]}) == 900.0


@pytest.mark.parametrize("value,ok", [(900, True), (3600, True),
                                      (899, False), (60, False)])
def test_reply_grace_is_configurable_and_at_least_900_s(value, ok):
    env = Env(queue=False, extra_config=(
        '[hosts.far]\ntransport = "ssh"\nssh = "far-target"\n'
        f'agent_compute = "/x"\nreply_grace_s = {value}\n'))
    try:
        if ok:
            assert cfgmod.load(env.config).remotes["far"].reply_grace_s == \
                float(value)
        else:
            with pytest.raises(cfgmod.ConfigError) as error:
                cfgmod.load(env.config)
            assert "reply_grace_s" in str(error.value)
    finally:
        env.cleanup()


# --- a placed submit ----------------------------------------------------------------------

def test_a_placed_submit_ties_to_this_host_and_reports_the_other():
    p = placed_pair()
    try:
        near_head = head_of(p.near.projects["alpha"])
        r = placed(p)
        assert r.returncode == 0, r.stdout + r.stderr
        reply = json.loads(r.stdout)
        job = reply["job_id"]
        assert reply["accepted"] is True and reply["host"] == "testhost"
        note = reply["placement"]
        rid = note["request_id"]
        assert rid.startswith("testhost-")             # generated
        assert note["chosen"] == "testhost" and note["pinned"] is False
        [here, far] = note["candidates"]
        assert (here["status"], far["status"]) == ("chosen", "busy")
        assert "a tie at 0" in far["reasons"][0]
        assert far["raw"]["rc"] == 0 and far["raw"]["stdout"]
        assert far["head"] == near_head
        # the record: committed with the policy, then accepted here
        rec = p.record(rid)
        assert rec["host"] == "testhost" and rec["pinned"] is False
        assert rec["phase"] == "accepted" and rec["remote_job_id"] == job
        assert [h["phase"] for h in rec["history"]] == ["committed",
                                                       "accepted"]
        assert rec["policy"]["chosen"] == "testhost"
        assert rec["policy"]["candidates"][1]["raw"]["stdout"]
        assert rec["code"]["required_commit"] == near_head
        # the job: its manifest carries the placement and the requirement
        man = json.loads((p.near.state / "jobs" / job /
                          "manifest.json").read_text())
        assert man["request_id"] == rid
        assert man["placement"]["policy"]["chosen"] == "testhost"
        assert "raw" not in man["placement"]["policy"]["candidates"][1]
        assert man["code"]["required_commit"] == near_head
        # only far's read-only summary was asked; nothing ran there
        assert subcommands(p) == ["_summary"]
        assert not (p.far.state / "jobs").exists()
        p.near.wait_outcome(job)
        # the job's view shows the policy snapshot
        view = json.loads(p.acs("status", job, "--json").stdout)
        assert [c["host"] for c in view["placement"]["policy"][
            "candidates"]] == ["testhost", "far"]
        assert view["code"]["required_commit"] == near_head
    finally:
        p.cleanup()


def test_a_placed_submit_goes_to_the_only_eligible_host_after_its_record():
    """Ordering: the probes come first and write nothing anywhere; the
    record is committed (and marked sent) before the submit is sent, and
    nothing exists on the destination before that."""
    p = placed_pair()
    try:
        block_here(p.near)
        order = p.near.root / "order.tsv"
        r = placed(p, FAKE_SSH_ORDER=str(order),
                   FAKE_SSH_ORDER_NEAR=str(p.near.state / "placements"),
                   FAKE_SSH_ORDER_FAR=str(p.far.state / "jobs"))
        assert r.returncode == 0, r.stdout + r.stderr
        reply = json.loads(r.stdout)
        job, rid = reply["job_id"], reply["placement"]["request_id"]
        assert reply["host"] == "far"
        [here, far] = reply["placement"]["candidates"]
        assert here["status"] == "blocked" and \
            "host admission is blocked" in here["reasons"]
        assert far["status"] == "chosen"
        calls = [line.split("\t") for line in
                 order.read_text().splitlines()]
        from agent_compute import remote
        assert [remote.command_of(c[0].split()[2:]) for c in calls] == \
            ["_summary", "submit"]
        probe_call, submit_call = calls
        assert probe_call[1] == "0" and probe_call[2] == ""   # no record yet
        assert submit_call[1] == "0"                     # nothing there yet
        at_send = json.loads(submit_call[2])
        assert at_send["phase"] == "sent" and at_send["host"] == "far"
        assert at_send["policy"]["chosen"] == "far"
        rec = p.record(rid)
        assert rec["phase"] == "accepted" and rec["remote_job_id"] == job
        assert rec["pinned"] is False and rec["mapping"]["cwd"] == \
            str(p.far.projects["alpha"])
        man = json.loads((p.far.state / "jobs" / job /
                          "manifest.json").read_text())
        assert man["request_id"] == rid
        assert man["cwd"] == str(p.far.projects["alpha"].resolve())
        assert man["code"]["required_commit"] == \
            head_of(p.near.projects["alpha"])
        assert man["code"]["require_clean"] is True
        assert rec["destination"]["head"] == man["code"]["head"]
        # the placed job's view (routed) carries the policy snapshot
        p.far.wait_outcome(job)
        view = json.loads(p.acs("status", job, "--json").stdout)
        assert view["route"]["host"] == "far"
        assert [(c["host"], c["status"]) for c in
                view["placement"]["policy"]["candidates"]] == [
            ("testhost", "blocked"), ("far", "chosen")]
        assert "raw" not in view["placement"]["policy"]["candidates"][0]
        human = p.acs("status", job)
        assert "placed on far by testhost's policy" in human.stderr
    finally:
        p.cleanup()


def test_a_busier_local_host_is_reported_busy():
    p = placed_pair()
    try:
        job = "acs-20261008T000000Z-0b0b0b"
        write_json(p.near.state / "jobs" / job / "state.json",
                   {"job_id": job, "host": "testhost", "phase": "queued",
                    "pueue_task": 7, "accepted_utc": cli.utc_now(),
                    "history": []})
        r = placed(p, request_id="busy-1")
        assert r.returncode == 0, r.stdout + r.stderr
        reply = json.loads(r.stdout)
        [here, far] = reply["placement"]["candidates"]
        assert (here["status"], far["status"]) == ("busy", "chosen")
        assert here["reasons"] == ["1 unfinished job(s); far has 0"]
        p.far.wait_outcome(reply["job_id"])
    finally:
        p.cleanup()


@pytest.mark.parametrize("mode,status", [("unreachable", "unreachable"),
                                         ("old", "unsuitable"),
                                         ("nojson", "unsuitable")])
def test_an_unusable_remote_is_reported_and_not_chosen(mode, status):
    p = placed_pair()
    try:
        r = placed(p, modes={"far-target": mode})
        assert r.returncode == 0, r.stdout + r.stderr
        reply = json.loads(r.stdout)
        [here, far] = reply["placement"]["candidates"]
        assert (here["status"], far["status"]) == ("chosen", status)
        assert far["raw"]["stdout"] is not None and far["reasons"]
        assert subcommands(p) == ["_summary"]
        p.near.wait_outcome(reply["job_id"])
    finally:
        p.cleanup()


def test_no_eligible_host_writes_nothing_anywhere():
    p = placed_pair()
    try:
        block_here(p.near)
        before = snapshot(p.far.state)
        r = placed(p, request_id="none-1", modes={"far-target":
                                                 "unreachable"})
        assert r.returncode == 2, r.stdout + r.stderr
        refusal = json.loads(r.stdout)
        assert refusal["accepted"] is False
        assert "no eligible host" in refusal["reason"]
        assert [(c["host"], c["status"]) for c in
                refusal["placement"]["candidates"]] == [
            ("testhost", "blocked"), ("far", "unreachable")]
        assert not (p.near.state / "placements").exists()
        assert not (p.near.state / "jobs").exists()
        assert not (p.near.state / "requests").exists()
        assert snapshot(p.far.state) == before
        assert subcommands(p) == ["_summary"]
    finally:
        p.cleanup()


def test_the_chosen_host_refusing_is_never_followed_by_another_send():
    p = placed_pair()
    try:
        block_here(p.near)
        p.far.config.write_text(p.far.config.read_text().replace(
            "max_threads = 2", "max_threads = 1", 1))
        r = placed(p, "--threads", "2", request_id="nf-1")
        assert r.returncode == 2, r.stdout + r.stderr
        rec = p.record("nf-1")
        assert rec["phase"] == "refused" and rec["host"] == "far"
        assert subcommands(p) == ["_summary", "submit"]
        assert not (p.near.state / "jobs").exists()
    finally:
        p.cleanup()


def test_a_lost_reply_is_never_followed_by_another_send_and_resolves_read_only():
    p = placed_pair()
    try:
        block_here(p.near)
        r = placed(p, request_id="lost-1",
                   modes={"far-target": "drop-after"}, FAKE_SSH_ONLY="submit")
        assert r.returncode == 255, r.stdout + r.stderr
        rec = p.record("lost-1")
        assert rec["phase"] == "reply-lost" and rec["host"] == "far"
        assert subcommands(p) == ["_summary", "submit"]
        assert not (p.near.state / "jobs").exists()
        [job] = os.listdir(p.far.state / "jobs")
        p.far.wait_outcome(job)
        # resolution asks far by request id, read-only there
        before = snapshot(p.far.state, p.far.guard_home)
        report = resolve(p, "lost-1")
        assert snapshot(p.far.state, p.far.guard_home) == before
        [item] = report["resolutions"]
        assert (item["before"], item["after"], item["job_id"]) == \
            ("reply-lost", "accepted", job)
        assert subcommands(p)[-1] == "_request"
        rec = p.record("lost-1")
        assert rec["phase"] == "accepted" and rec["remote_job_id"] == job
        assert rec["resolution"]["found"] is True
        assert rec["replies"][-1]["kind"] == "resolution"
        # resolved: nothing open, nothing asked again
        calls = p.calls()
        assert resolve(p)["resolutions"] == []
        assert p.calls() == calls
    finally:
        p.cleanup()


def test_the_mapping_reaches_the_forwarded_arguments():
    p = placed_pair(mapping='container = "alpha-box"\n'
                            'container_workdir = "/srv/work"\n')
    try:
        block_here(p.near)
        sub = p.near.projects["alpha"] / "sub dir"
        sub.mkdir()
        r = placed(p, "--container", "local-box", "--container-workdir",
                   "/work", request_id="map-1", cwd=sub)
        # refused there (its checkout has no "sub dir"): the point is
        # what was sent
        assert r.returncode == 2, r.stdout + r.stderr
        rec = p.record("map-1")
        assert rec["phase"] == "refused"
        argv = rec["argv"]
        assert argv[argv.index("--cwd") + 1] == \
            f"{p.far.projects['alpha']}/sub dir"
        assert argv[argv.index("--container") + 1] == "alpha-box"
        assert argv[argv.index("--container-workdir") + 1] == "/srv/work"
        assert argv.count("--cwd") == argv.count("--container") == 1
        assert rec["mapping"]["container"] == "alpha-box"
        assert rec["mapping"]["from"]["container"] == "local-box"
    finally:
        p.cleanup()


def test_a_container_job_needs_a_container_mapping_there():
    p = placed_pair()
    try:
        block_here(p.near)
        r = placed(p, "--container", "local-box", request_id="map-2")
        assert r.returncode == 2, r.stdout + r.stderr
        far = json.loads(r.stdout)["placement"]["candidates"][1]
        assert far["status"] == "unsuitable"
        assert "container" in " ".join(far["reasons"])
        assert not (p.near.state / "placements").exists()
    finally:
        p.cleanup()


@pytest.mark.parametrize("cmd", ["submit", "run"])
def test_a_placed_run_or_submit_on_the_remote_host(cmd):
    p = placed_pair(far_queue=True)
    try:
        block_here(p.near)
        r = placed(p, cmd=cmd, request_id=f"pr-{cmd}")
        assert r.returncode == 0, r.stdout + r.stderr
        reply = json.loads(r.stdout)
        rec = p.record(f"pr-{cmd}")
        assert rec["phase"] == "accepted" and rec["command"] == cmd
        assert rec["remote_job_id"] == reply["job_id"]
        if cmd == "run":
            assert reply["outcome"] == "succeeded"
        else:
            p.far.wait_outcome(reply["job_id"])
    finally:
        p.cleanup()


def test_a_project_with_one_host_is_unchanged():
    p = placed_pair(order='["testhost"]', mapping=None)
    try:
        job = p.near.submit(workload_args=["--out",
                                           str(p.near.root / "one.jsonl")],
                            env=p.env())
        assert "placement" not in job
        assert p.calls() == 0
        assert not (p.near.state / "placements").exists()
        p.near.wait_outcome(job["job_id"])
    finally:
        p.cleanup()


# --- the code requirement ------------------------------------------------------------------

def pinned(p: Pair, *extra, request_id, cwd=None, name="pin",
           **env_extra):
    return p.acs("--host", "far", "submit", "--json", "--case", "t",
                 "--timeout-min", "1", "--reserve-gb", "0.5", "--cwd",
                 str(cwd or p.near.projects["alpha"]), "--request-id",
                 request_id, *extra, "--", PY, str(WORKLOAD), "--out",
                 str(p.far.root / f"{name}.jsonl"), **env_extra)


def test_a_pinned_submit_from_a_checkout_is_mapped_and_requires_its_commit():
    p = placed_pair()
    try:
        r = pinned(p, request_id="pc-1")
        assert r.returncode == 0, r.stdout + r.stderr
        job = json.loads(r.stdout)["job_id"]
        rec = p.record("pc-1")
        assert rec["pinned"] is True and rec["policy"] is None
        assert rec["mapping"]["cwd"] == str(p.far.projects["alpha"])
        near_head = head_of(p.near.projects["alpha"])
        assert rec["argv"][rec["argv"].index("--require-commit") + 1] == \
            near_head
        assert "--require-clean" in rec["argv"]
        man = json.loads((p.far.state / "jobs" / job /
                          "manifest.json").read_text())
        assert man["code"]["required_commit"] == near_head
        assert rec["destination"]["code"]["head"] == near_head
        p.far.wait_outcome(job)
    finally:
        p.cleanup()


def test_a_commit_mismatch_is_refused_at_submit_naming_both():
    p = placed_pair()
    try:
        far_repo = p.far.projects["alpha"]
        (far_repo / "input.txt").write_text("alpha v2\n")
        git(far_repo, "commit", "-q", "-am", "far moves on")
        r = pinned(p, request_id="mm-1")
        assert r.returncode == 2, r.stdout + r.stderr
        refusal = json.loads(r.stdout)
        assert refusal["accepted"] is False
        assert head_of(far_repo) in refusal["reason"]
        assert head_of(p.near.projects["alpha"]) in refusal["reason"]
        assert "never started" in refusal["reason"]
        assert p.record("mm-1")["phase"] == "refused"
        # nothing kept there: no job, no binding
        assert not list((p.far.state / "jobs").iterdir())
        assert not (p.far.state / "requests" / "mm-1.json").exists()
        # placed: far is unsuitable from its own summary, nothing sent
        block_here(p.near)
        calls = p.calls()
        r = placed(p, request_id="mm-2")
        assert r.returncode == 2
        far = json.loads(r.stdout)["placement"]["candidates"][1]
        assert far["status"] == "unsuitable"
        assert "not the required" in " ".join(far["reasons"])
        assert p.calls() == calls + 1               # the probe only
        # --any-commit: no requirement, accepted
        r = pinned(p, "--any-commit", request_id="mm-3")
        assert r.returncode == 0, r.stdout + r.stderr
        rec = p.record("mm-3")
        assert "--require-commit" not in rec["argv"]
        man = json.loads((p.far.state / "jobs" / json.loads(
            r.stdout)["job_id"] / "manifest.json").read_text())
        assert man["code"]["any_commit"] is True
        assert man["code"]["required_commit"] is None
        p.far.wait_outcome(json.loads(r.stdout)["job_id"])
    finally:
        p.cleanup()


def test_dirty_there_is_refused_only_when_this_checkout_is_clean():
    p = placed_pair()
    try:
        (p.far.projects["alpha"] / "input.txt").write_text("edited\n")
        r = pinned(p, request_id="dc-1")
        assert r.returncode == 2, r.stdout + r.stderr
        assert "uncommitted changes" in json.loads(r.stdout)["reason"]
        # this checkout dirty too: only the commit is required
        (p.near.projects["alpha"] / "input.txt").write_text("edited here\n")
        r = pinned(p, request_id="dc-2", name="dc2")
        assert r.returncode == 0, r.stdout + r.stderr
        assert "--require-clean" not in p.record("dc-2")["argv"]
        p.far.wait_outcome(json.loads(r.stdout)["job_id"])
    finally:
        p.cleanup()


def test_the_commit_options_are_checked(env_direct):
    env = env_direct
    for extra, word in ((["--require-commit", "abc"], "full commit sha"),
                        (["--require-commit", "a" * 40, "--any-commit"],
                         "excludes")):
        r = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                    "--cwd", str(env.projects["alpha"]), *extra, "--", "true")
        assert r.returncode == 2, r.stdout + r.stderr
        assert word in json.loads(r.stdout)["reason"]
    wrong = "0" * 40
    r = env.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                "--cwd", str(env.projects["alpha"]), "--require-commit",
                wrong, "--request-id", "wr-1", "--", "true")
    assert r.returncode == 2, r.stdout + r.stderr
    refusal = json.loads(r.stdout)
    assert wrong in refusal["reason"]
    assert head_of(env.projects["alpha"]) in refusal["reason"]
    assert not list((env.state / "jobs").iterdir())
    assert not (env.state / "requests" / "wr-1.json").exists()


@pytest.mark.parametrize("change", ["commit", "dirty"])
def test_the_code_requirement_is_checked_again_at_dispatch(env_direct,
                                                           change):
    env = env_direct
    repo = env.projects["alpha"]
    gate = env.state / "dispatch-gate.json"
    env.state.mkdir(parents=True, exist_ok=True)
    gate.write_text('{"reason": "test: hold dispatch"}')
    job = env.submit(workload_args=["--out", str(env.root / "d.jsonl")],
                     extra=["--require-commit", head_of(repo),
                            "--require-clean"])
    env.wait_phase(job["job_id"], "dispatch-held")
    required = head_of(repo)
    (repo / "input.txt").write_text("changed while queued\n")
    if change == "commit":
        git(repo, "commit", "-q", "-am", "moved while queued")
    gate.unlink()
    state = env.wait_outcome(job["job_id"])
    assert state["outcome"] == "refused", state
    assert "at dispatch" in state["reason"]
    assert "never started" in state["reason"]
    if change == "commit":
        assert required in state["reason"] and head_of(repo) in \
            state["reason"]
    else:
        assert "uncommitted changes" in state["reason"]
    assert not (env.root / "d.jsonl").exists()


# --- lost replies: resolution by request id -------------------------------------------------

def test_not_found_stays_within_the_grace_then_resolves_not_accepted():
    p = placed_pair()
    try:
        # a send that never reached far
        r = p.acs("--host", "far", "submit", "--json", "--case", "t",
                  "--timeout-min", "1", "--cwd",
                  str(p.near.projects["alpha"]), "--request-id", "g-2",
                  "--", "true", modes={"far-target": "unreachable"})
        assert r.returncode == 255
        before = snapshot(p.far.state, p.far.guard_home)
        [item] = resolve(p, "g-2")["resolutions"]
        assert (item["before"], item["after"]) == ("reply-lost",
                                                   "reply-lost")
        assert item["found"] is False and "grace" in item["reason"]
        # the send's call timeout (900 s) plus 900 s (review round 1)
        assert item["grace_s"] == 1800.0
        # past the grace but unreachable: stays
        backdate(p, "g-2", 1900)
        [item] = resolve(p, "g-2", modes={"far-target":
                                         "unreachable"})["resolutions"]
        assert item["after"] == "reply-lost" and "unreachable" in \
            item["reason"]
        # an old destination without `_request`: stays
        [item] = resolve(p, "g-2", modes={"far-target":
                                         "old"})["resolutions"]
        assert item["after"] == "reply-lost"
        # reachable and answering, past the grace: resolved not accepted
        [item] = resolve(p, "g-2")["resolutions"]
        assert item["after"] == "resolved-not-accepted", item
        assert snapshot(p.far.state, p.far.guard_home) == before
        rec = p.record("g-2")
        assert rec["phase"] == "resolved-not-accepted"
        assert rec["resolution"]["found"] is False
        assert rec["resolution"]["age_s"] >= 1800
        # bound to far still: not here, not placed elsewhere
        here = p.acs("submit", "--json", "--case", "t", "--timeout-min", "1",
                     "--cwd", str(p.near.projects["beta"]), "--request-id",
                     "g-2", "--", "true")
        assert here.returncode == 2 and "far" in \
            json.loads(here.stdout)["reason"]
        calls = p.calls()
        again = placed(p, request_id="g-2")
        assert again.returncode == 2
        assert "--host far" in json.loads(again.stdout)["reason"]
        assert p.calls() == calls
        # the same host may receive it again: the agent's re-send
        again = pinned(p, request_id="g-2", name="g2")
        assert again.returncode == 0, again.stdout + again.stderr
        rec = p.record("g-2")
        assert rec["phase"] == "accepted"
        assert [h["phase"] for h in rec["history"]][-3:] == [
            "committed", "sent", "accepted"]
        p.far.wait_outcome(json.loads(again.stdout)["job_id"])
    finally:
        p.cleanup()


def test_the_grace_is_the_routes_own():
    p = placed_pair(near_extra="")
    try:
        p.near.config.write_text(p.near.config.read_text().replace(
            'ssh = "far-target"\n', 'ssh = "far-target"\n'
            'reply_grace_s = 2000\n', 1))
        r = p.acs("--host", "far", "submit", "--json", "--case", "t",
                  "--timeout-min", "1", "--cwd",
                  str(p.near.projects["alpha"]), "--request-id", "gr-1",
                  "--", "true", modes={"far-target": "unreachable"})
        assert r.returncode == 255
        backdate(p, "gr-1", 1000)
        [item] = resolve(p, "gr-1")["resolutions"]
        assert item["after"] == "reply-lost" and item["grace_s"] == 2000.0
        backdate(p, "gr-1", 2100)
        [item] = resolve(p, "gr-1")["resolutions"]
        assert item["after"] == "resolved-not-accepted"
    finally:
        p.cleanup()


def test_a_resend_while_reply_lost_resolves_first_and_sends_nothing():
    p = placed_pair()
    try:
        r = p.acs("--host", "far", "submit", "--json", "--case", "t",
                  "--timeout-min", "1", "--cwd",
                  str(p.near.projects["alpha"]), "--request-id", "rs-1",
                  "--", "true", modes={"far-target": "unreachable"})
        assert r.returncode == 255
        calls = p.calls()
        again = pinned(p, request_id="rs-1")
        assert again.returncode == 2, again.stdout + again.stderr
        assert "placements resolve rs-1" in json.loads(again.stdout)["reason"]
        assert subcommands(p)[calls:] == ["_request"]     # no submit sent
        assert not (p.far.state / "jobs").exists()
        assert p.record("rs-1")["phase"] == "reply-lost"
    finally:
        p.cleanup()


def test_status_and_hosts_show_the_records_with_state_and_age():
    p = placed_pair()
    try:
        r = p.acs("--host", "far", "submit", "--json", "--case", "t",
                  "--timeout-min", "1", "--cwd",
                  str(p.near.projects["alpha"]), "--request-id", "age-1",
                  "--", "true", modes={"far-target": "unreachable"})
        assert r.returncode == 255
        backdate(p, "age-1", 120)
        listing = json.loads(p.acs("status", "--all", "--json").stdout)
        [item] = listing["placements"]
        assert item["phase"] == "reply-lost" and item["open"] is True
        assert 100 <= item["age_s"] < 900
        report = json.loads(p.acs("hosts", "--json").stdout)
        [item] = report["placements_open"]
        assert item["request_id"] == "age-1" and item["age_s"] >= 100
        [res] = report["resolutions"]
        assert res["after"] == "reply-lost" and res["found"] is False
    finally:
        p.cleanup()


def test_a_resolution_never_proves_absence_over_an_unreadable_record():
    p = placed_pair()
    try:
        r = p.acs("--host", "far", "submit", "--json", "--case", "t",
                  "--timeout-min", "1", "--cwd",
                  str(p.near.projects["alpha"]), "--request-id", "ur-1",
                  "--", "true", modes={"far-target": "unreachable"})
        assert r.returncode == 255
        backdate(p, "ur-1", 1000)
        bad = p.far.state / "jobs" / "acs-20261008T000000Z-badbad"
        bad.mkdir(parents=True)
        (bad / "state.json").write_text("{trunc")
        [item] = resolve(p, "ur-1")["resolutions"]
        assert item["after"] == "reply-lost" and item["found"] is None
        assert "cannot prove" in item["reason"]
        (bad / "state.json").write_text("{}")       # for the cleanup
    finally:
        p.cleanup()


# --- A40: no bounce ---------------------------------------------------------------------------

def test_crossed_records_never_bounce_a_call():
    """Register A40: the near host's record sends R to far, far's own
    record sends R back to testhost; a routed call arrives marked and is
    answered where it lands."""
    p = Pair(far_queue=False)
    try:
        near_wrapper = p.far.root / "near-agent-compute"
        near_wrapper.write_text(f'#!/bin/bash\nexec "{PY}" "{BIN}" '
                                f'--config "{p.near.config}" "$@"\n')
        near_wrapper.chmod(0o755)
        p.far.config.write_text(p.far.config.read_text() + (
            '[hosts.testhost]\ntransport = "ssh"\nssh = "near-target"\n'
            f'agent_compute = "{near_wrapper}"\n'))
        now = cli.utc_now()
        for env, host in ((p.near, "far"), (p.far, "testhost")):
            write_json(env.state / "placements" / "x-1.json",
                       {"schema": pl.PLACEMENT_SCHEMA, "request_id": "x-1",
                        "host": host, "phase": "reply-lost",
                        "created_utc": now, "sent_utc": now,
                        "history": [], "replies": []})
        r = p.acs("status", "x-1", "--json", timeout=120)
        assert r.returncode == 0, r.stdout + r.stderr
        assert json.loads(r.stdout)["host"] == "far"
        targets = [t for t, _ in scripts(p)]
        assert targets and set(targets) == {"far-target"}, targets
        assert "exceeds the bound" not in r.stderr
        # every remote script carries the marker with the sender's label
        assert all(s.startswith("ACS_NO_REROUTE=testhost ")
                   for _, s in scripts(p))
        # what far does with a marked call that would route on
        marked = p.far.env(ACS_NO_REROUTE="testhost",
                           PATH=p.env()["PATH"],
                           FAKE_SSH_LOG=str(p.ssh_log))
        calls = p.calls()
        fwd = p.far.acs("--host", "testhost", "status", env=marked)
        assert fwd.returncode == 2 and "A40" in fwd.stderr
        res = p.far.acs("placements", "resolve", env=marked)
        assert res.returncode == 2 and "A40" in res.stderr
        hosts = json.loads(p.far.acs("hosts", "--json", env=marked).stdout)
        [back] = [h for h in hosts["hosts"] if h["name"] == "testhost"]
        assert back["state"] == "not-probed" and back["eligible"] is False
        assert hosts["resolutions"] == []
        status = p.far.acs("status", "--request-id", "x-1", "--json",
                           env=marked)
        assert status.returncode == 0, status.stdout + status.stderr
        assert json.loads(status.stdout)["host"] == "far"
        assert p.calls() == calls                   # far called no one
    finally:
        p.cleanup()


def test_a_marked_submit_is_never_placed():
    p = placed_pair()
    try:
        env = p.env(ACS_NO_REROUTE="elsewhere")
        r = p.near.acs("submit", "--json", "--case", "t", "--timeout-min",
                       "1", "--reserve-gb", "0.5", "--cwd",
                       str(p.near.projects["alpha"]), "--", "true", env=env)
        assert r.returncode == 0, r.stdout + r.stderr
        assert "placement" not in json.loads(r.stdout)
        assert p.calls() == 0
        assert not (p.near.state / "placements").exists()
        p.near.wait_outcome(json.loads(r.stdout)["job_id"])
    finally:
        p.cleanup()


def test_a_record_of_this_host_is_resolved_in_process():
    """A placement on this host whose process ended between the record's
    commit and the acceptance: resolved here, by the local request
    binding (no remote call)."""
    p = placed_pair()
    try:
        now = cli.utc_now()
        for rid in ("loc-1", "loc-2"):
            write_json(p.near.state / "placements" / f"{rid}.json",
                       {"schema": pl.PLACEMENT_SCHEMA, "request_id": rid,
                        "host": "testhost", "phase": "committed",
                        "created_utc": now, "history": [], "replies": []})
        # (beta: a project of this host only, so not placed again)
        job = p.near.submit("beta", workload_args=["--out",
                                                   str(p.near.root /
                                                       "l.jsonl")],
                            extra=["--request-id", "loc-1"], env=p.env())
        p.near.wait_outcome(job["job_id"])
        report = resolve(p)
        got = {r["request_id"]: (r["after"], r["job_id"])
               for r in report["resolutions"]}
        assert got == {"loc-1": ("accepted", job["job_id"]),
                       "loc-2": ("committed", None)}
        backdate(p, "loc-2", 1000)
        [item] = resolve(p, "loc-2")["resolutions"]
        assert item["after"] == "resolved-not-accepted"
        assert item["raw"]["in_process"] is True
        assert p.calls() == 0
    finally:
        p.cleanup()


# --- review round 1 (independent review of 4dd5e1a) -----------------------------------------

def utc_ago(seconds: float) -> str:
    then = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=seconds)
    return then.strftime("%Y-%m-%dT%H:%M:%SZ")


def open_record(env: Env, rid: str, host: str = "far", *,
                phase: str = "reply-lost", age_s: float = 0, send=None,
                placer=None, replies=None) -> None:
    """A placement record written as a sender would have left it."""
    record = {"schema": pl.PLACEMENT_SCHEMA, "request_id": rid,
              "host": host, "phase": phase, "created_utc": utc_ago(age_s),
              "sent_utc": utc_ago(age_s), "history": [],
              "replies": replies or []}
    if send is not None:
        record["send"] = send
    if placer is not None:
        record["placer"] = placer
    write_json(env.state / "placements" / f"{rid}.json", record)


def live_process():
    from agent_compute import procs
    proc = subprocess.Popen([PY, "-c", "import time; time.sleep(300)"])
    return proc, procs.identity(proc.pid)


def gone_identity() -> dict:
    from agent_compute import procs
    proc = subprocess.Popen([PY, "-c", "pass"])
    ident = procs.identity(proc.pid)
    proc.wait()
    return ident


def one(p: Pair, rid: str) -> dict:
    [item] = resolve(p, rid)["resolutions"]
    return item


def test_the_grace_exceeds_the_send_call_timeout():
    """Finding 1: a send whose client timed out at T may still reserve on
    the destination after T; "not found" proves nothing before T + 900 s
    (the destination's own grace)."""
    p = placed_pair()
    try:
        send = {"call_timeout_s": 1200.0, "command": "submit",
                "client": gone_identity()}
        # (a margin of 10 s each side of T + 900: the age is read when the
        # resolution runs, seconds after the record is written; the
        # Lenovo gate at 72b07ae caught a 1 s margin)
        for age in (1201, 2090):
            open_record(p.near, "gt-1", age_s=age, send=send)
            item = one(p, "gt-1")
            assert item["after"] == "reply-lost", (age, item)
            assert item["grace_s"] == 2100.0
        open_record(p.near, "gt-1", age_s=2110, send=send)
        assert one(p, "gt-1")["after"] == "resolved-not-accepted"
        # a G3a record (no send recorded): its reply's call timeout counts
        open_record(p.near, "gt-2", age_s=901,
                    replies=[{"utc": utc_ago(1), "rc": 255,
                              "timeout_s": 900.0}])
        assert one(p, "gt-2")["after"] == "reply-lost"
    finally:
        p.cleanup()


def test_never_by_time_while_the_sends_ssh_client_lives():
    """Finding 1 (and 5): a sender that died leaves its ssh client running
    with no timeout; while that start-time-proven process lives, the
    command may still reach the destination."""
    p = placed_pair()
    proc, ident = live_process()
    try:
        open_record(p.near, "cl-1", age_s=100000,
                    send={"call_timeout_s": 900.0, "command": "submit",
                          "client": ident})
        item = one(p, "cl-1")
        assert item["after"] == "reply-lost"
        assert "ssh client" in item["reason"]
        proc.kill()
        proc.wait()
        assert one(p, "cl-1")["after"] == "resolved-not-accepted"
    finally:
        proc.kill()
        p.cleanup()


def test_never_by_time_while_the_placing_process_lives():
    p = placed_pair()
    proc, ident = live_process()
    try:
        open_record(p.near, "pl-1", phase="sent", age_s=100000,
                    placer=ident, send={"call_timeout_s": 900.0,
                                        "command": "submit",
                                        "client": gone_identity()})
        item = one(p, "pl-1")
        assert item["after"] == "sent" and "placing process" in \
            item["reason"]
        proc.kill()
        proc.wait()
        assert one(p, "pl-1")["after"] == "resolved-not-accepted"
    finally:
        proc.kill()
        p.cleanup()


def test_an_unbounded_run_send_is_never_resolved_by_time_while_sent():
    """Finding 1: a `run` send has no client timeout: while its record is
    `sent`, only a positive answer resolves it; once its client ended
    (`reply-lost`), "not found" counts only the grace after that end."""
    p = placed_pair()
    try:
        send = {"call_timeout_s": None, "command": "run",
                "client": gone_identity()}
        open_record(p.near, "run-x", phase="sent", age_s=100000, send=send)
        item = one(p, "run-x")
        assert item["after"] == "sent" and "unbounded" in item["reason"]
        open_record(p.near, "run-x", age_s=100000, send=send,
                    replies=[{"utc": utc_ago(60), "rc": 255,
                              "timeout_s": None}])
        item = one(p, "run-x")
        assert item["after"] == "reply-lost" and "ended" in item["reason"]
        open_record(p.near, "run-x", age_s=100000, send=send,
                    replies=[{"utc": utc_ago(1000), "rc": 255,
                              "timeout_s": None}])
        assert one(p, "run-x")["after"] == "resolved-not-accepted"
    finally:
        p.cleanup()


def test_the_send_records_its_timeout_and_client_before_anything_is_sent():
    p = placed_pair()
    try:
        seen = p.near.root / "seen.json"
        pids = p.near.root / "pids.txt"
        r = pinned(p, request_id="id-1", FAKE_SSH_SNAPSHOT=str(seen),
                   FAKE_SSH_SNAPSHOT_SRC=str(p.near.state / "placements"),
                   FAKE_SSH_PIDFILE=str(pids))
        assert r.returncode == 0, r.stdout + r.stderr
        at_call = json.loads(seen.read_text())
        assert at_call["phase"] == "sent"
        assert at_call["send"]["call_timeout_s"] == 900.0
        assert at_call["send"]["client"]["pid"] == \
            int(pids.read_text().split()[0])
        assert at_call["send"]["client"]["start_epoch"]
        assert at_call["placer"]["pid"] != at_call["send"]["client"]["pid"]
        p.far.wait_outcome(json.loads(r.stdout)["job_id"])
    finally:
        p.cleanup()


def test_nothing_is_sent_when_the_client_cannot_be_recorded(monkeypatch,
                                                            capsys):
    p = placed_pair()
    try:
        for key, value in p.env().items():
            monkeypatch.setenv(key, value)

        def fail(*args, **kwargs):
            raise OSError("disk full (test)")
        monkeypatch.setattr(pl, "note_client", fail)
        rc = cli.main(["--config", str(p.near.config), "--host", "far",
                       "submit", "--json", "--case", "t", "--timeout-min",
                       "1", "--cwd", str(p.near.projects["alpha"]),
                       "--request-id", "nc-1", "--", "true"])
        captured = capsys.readouterr()
        assert rc == 2, captured.out + captured.err
        assert p.calls() == 0
        rec = p.record("nc-1")
        assert rec["phase"] == "refused" and "nothing was sent" in \
            rec["reason"], (rec, captured.err)
        assert not (p.far.state / "jobs").exists()
    finally:
        p.cleanup()


def busy_here(env: Env) -> None:
    job = "acs-20261008T000000Z-0b0b0b"
    write_json(env.state / "jobs" / job / "state.json",
               {"job_id": job, "host": "testhost", "phase": "queued",
                "pueue_task": 7, "accepted_utc": cli.utc_now(),
                "history": []})


@pytest.mark.parametrize("failure", ["refused", "lost"])
def test_no_fallback_with_two_eligible_hosts(failure):
    """Finding 2: both hosts eligible (this one busy, far chosen); far
    refuses, or its reply is lost: nothing is ever sent or started here."""
    p = placed_pair()
    try:
        busy_here(p.near)
        extra, modes, env = (), None, {}
        if failure == "refused":
            p.far.config.write_text(p.far.config.read_text().replace(
                "max_threads = 2", "max_threads = 1", 1))
            extra = ("--threads", "2")
        else:
            modes, env = {"far-target": "drop-after"}, {"FAKE_SSH_ONLY":
                                                        "submit"}
        r = placed(p, *extra, request_id="fb-1", modes=modes, **env)
        assert r.returncode == (2 if failure == "refused" else 255), \
            r.stdout + r.stderr
        assert subcommands(p) == ["_summary", "submit"]
        assert os.listdir(p.near.state / "jobs") == \
            ["acs-20261008T000000Z-0b0b0b"]
        assert not (p.near.state / "requests").exists()
        rec = p.record("fb-1")
        assert rec["host"] == "far"
        assert rec["phase"] == ("refused" if failure == "refused"
                                else "reply-lost")
        assert [(c["host"], c["status"]) for c in
                rec["policy"]["candidates"]] == [("testhost", "busy"),
                                                 ("far", "chosen")]
    finally:
        p.cleanup()


def test_request_lookup_writes_nothing():
    """Finding 3: `_request` on a host where `detect_lost` and
    `settle_unclaimed` would write (a lost runner, a stale unclaimed job)
    and with no host id yet: the state dir is byte-identical after it."""
    from test_placement import lost_job
    env = Env(queue=False)
    try:
        lost = lost_job(env)
        stale = "acs-20261001T000000Z-57a1e0"
        write_json(env.state / "jobs" / stale / "state.json",
                   {"job_id": stale, "host": "testhost", "phase": "accepted",
                    "request_id": "stale-1",
                    "accepted_utc": "2026-10-01T00:00:00Z", "history": []})
        done = "acs-20261002T000000Z-d0d0d0"
        write_json(env.state / "jobs" / done / "manifest.json",
                   {"job_id": done, "host": "testhost", "project": "alpha",
                    "request_id": "rq-1"})
        write_json(env.state / "jobs" / done / "state.json",
                   {"job_id": done, "host": "testhost", "phase": "done",
                    "outcome": "succeeded", "request_id": "rq-1",
                    "history": []})
        events = env.state / "events" / "testhost.jsonl"
        events.parent.mkdir(parents=True)
        events.write_text('{"event": "phase"}\n')
        assert not (env.state / "host-id.json").exists()
        before = snapshot(env.state, env.guard_home)
        found = {}
        for rid in ("rq-1", "nope-1", "lost-1", "stale-1"):
            r = env.acs("_request", rid, "--json")
            assert r.returncode == 0, r.stdout + r.stderr
            found[rid] = json.loads(r.stdout)["found"]
        assert snapshot(env.state, env.guard_home) == before
        assert found == {"rq-1": True, "nope-1": False, "lost-1": True,
                         "stale-1": "pending"}
        assert env.state_of(lost).get("outcome") is None
        assert env.state_of(stale).get("outcome") is None
    finally:
        env.cleanup()


def test_the_send_is_refused_under_the_lock_by_an_open_record_of_its_host(
        monkeypatch, capsys):
    """Finding 4 (R2-3's pattern): an open record for the SAME host written
    between the read-only pre-check and the commit (a concurrent sender)
    refuses the send under the state lock; G3a's rule let a same-host send
    through."""
    from agent_compute import remote
    p = placed_pair()
    try:
        real_commit = pl.commit

        def racing_commit(state_dir, request_id, host, **kw):
            write_json(p.near.state / "placements" / f"{request_id}.json",
                       {"schema": pl.PLACEMENT_SCHEMA,
                        "request_id": request_id, "host": host,
                        "phase": "sent", "created_utc": cli.utc_now(),
                        "sent_utc": cli.utc_now(), "history": [],
                        "replies": []})
            return real_commit(state_dir, request_id, host, **kw)
        calls = []

        def fake_call(config, host, args, *a, **k):
            calls.append(list(args))
            return remote.Reply(host, list(args), 255, "", "",
                                transport_rc=255, rc_source="transport")
        monkeypatch.setattr(pl, "commit", racing_commit)
        monkeypatch.setattr(cli.remote_mod, "call", fake_call)
        rc = cli.main(["--config", str(p.near.config), "--host", "far",
                       "submit", "--json", "--case", "t", "--timeout-min",
                       "1", "--cwd", str(p.near.projects["alpha"]),
                       "--request-id", "race-2", "--", "true"])
        out = json.loads(capsys.readouterr().out)
        assert rc == 2 and out["accepted"] is False
        assert not any("submit" in c for c in calls), calls
        assert p.record("race-2")["phase"] == "sent"
    finally:
        p.cleanup()


def test_plain_status_asks_no_remote_host():
    """Follow-up 6: plain `status` and `status --all` list the open records
    from this host's own state, with their age; only `status R`, `status
    --resolve`, `hosts` and `placements resolve` ask the record's host."""
    p = placed_pair()
    try:
        r = p.acs("--host", "far", "submit", "--json", "--case", "t",
                  "--timeout-min", "1", "--cwd",
                  str(p.near.projects["alpha"]), "--request-id", "ps-1",
                  "--", "true", modes={"far-target": "unreachable"})
        assert r.returncode == 255
        calls = p.calls()
        for args in (("status", "--json"), ("status", "--all", "--json")):
            listing = json.loads(p.acs(*args).stdout)
            [item] = listing["placements"]
            assert item["phase"] == "reply-lost" and item["age_s"] >= 0
            assert p.calls() == calls, args
        r = p.acs("status", "--all", "--resolve", "--json")
        assert r.returncode == 0, r.stdout + r.stderr
        assert subcommands(p)[calls:] == ["_request"]
        r = p.acs("status", "ps-1", "--json")
        assert subcommands(p)[calls + 1] == "_request"
    finally:
        p.cleanup()
