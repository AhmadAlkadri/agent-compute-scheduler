"""Placement records (campaign G slice G3a; decisions D5, D6).

A job submitted to another host (``--host X submit``, and from G3b an
automatically placed one) has a local, durable record on the submitting
host: ``<state>/placements/<request-id>.json``.  It is written (atomic,
fsynced, the directory fsynced) **before** any remote call, so that a reply
lost on the way back never leaves a job with no trace here, and it carries
every reply verbatim.

Phases::

    committed -> sent -> accepted | refused | reply-lost

* ``committed``: the destination is fixed (the record names it and its
  route); nothing was sent yet;
* ``sent``: written immediately before the remote call;
* ``accepted``: the destination's reply names the job it bound the
  request id to (an acceptance, or a refusal that the request id already
  maps to a job there: the destination dedupes, C1);
* ``refused``: the destination refused it before reserving anything (a
  reply object with ``accepted: false`` and no job, or the CLI's own exit
  2 without a reply object: a usage or configuration error);
* ``reply-lost``: anything else (ssh exit 255, a timeout, a reply without
  JSON): the destination may have accepted it;
* ``resolved-not-accepted`` (G3b): a lost reply resolved by querying the
  SAME destination by request id (read-only there, ``_request``), which
  found no job for it although the destination was reachable and answered,
  at least the grace after the send (``grace_s``: the route's
  ``reply_grace_s``, at least ``MIN_GRACE_S``, and never less than the
  send's call timeout plus ``MIN_GRACE_S``: that timeout ends the client,
  not the remote command), and never while the placing process or the
  send's ssh client (start-time-proven identities recorded in ``placer``
  and ``send.client``) may still run; an unbounded send (``run``) is not
  resolved by time while it is ``sent``, and after its client ended
  (``reply-lost``) its grace counts from that end (review round 1 of
  G3b).  A query that finds the job resolves the record
  ``accepted`` (with the job id); an unreachable destination, a reply that
  is not a usable answer, or one that cannot prove the absence (an
  unreadable record there, a reservation in progress) leaves it
  ``reply-lost``.  Resolution never sends the job anywhere: the agent may
  submit again; ACS never does.

A request id bound to one host by a record is never sent to another host,
unless the record is ``refused`` (proven never reserved there); an
unreadable record binds it everywhere (C4).  A request id that this host
itself bound to a job (``requests/<id>.json``) is never sent to another
host either, unless that job provably never started (register A39, review
round 1 of G3a): the caller passes that check as ``guard``, run under the
same state lock as the commit, and the local reservation checks the
record under that lock too (``cli._reserve``).  A re-send of a request id
is allowed only when its record is ``refused`` (any host) or
``resolved-not-accepted`` (the same host only, G3b: until then the record
binds it; ``blocks_send``).

Automatic placement (G3b, decision D5): ``evaluate`` is the policy over
the hosts' read-only summaries: a candidate is reachable,
identity-consistent (it reports the name it is configured under, and no
other configured name reports its host id), at or above the safety floor,
with its engine pin, not blocked (an unreadable block excludes), its
dispatch gate open, no lost supervision suspected, the project configured
there (listing that host), the declared reserve within its budget (when
both are known), a known cwd there, and (when the placing checkout's HEAD
is required and the destination reports it) the same commit.  The choice
is the fewest non-terminal jobs; a tie goes to the local host, then to the
project's host order.  Every host is reported: ``chosen``, or ``busy``,
``unreachable``, ``blocked`` or ``unsuitable`` with its reasons and the
raw reply.  The choice is made once: no retry, no fallback to another
host.

Records keep no credentials: the forwarded argv is kept with every
``--env`` value replaced by ``<redacted>`` (names kept), the destination's
replies verbatim (ACS output, which carries no credentials by invariant,
and the transport's diagnostics).  Mode 0600 in a 0700 directory.
"""

from __future__ import annotations

import os
import secrets
from pathlib import Path

from .config import REPLY_GRACE_MIN_S
from .fsprobe import glob_strict, is_dir_strict
from .state import (Unreadable, _safe, history_item, private_dir,
                    read_json, state_lock, utc_now, write_json)

PLACEMENT_SCHEMA = "agent-compute-placement/1"
PHASES = ("committed", "sent", "accepted", "refused", "reply-lost",
          "resolved-not-accepted")
OPEN = ("committed", "sent", "reply-lost")
REDACTED = "<redacted>"
# the least grace after a send before "not found" may become
# resolved-not-accepted: the destination's own unclaimed grace
# (cli.UNCLAIMED_GRACE_S, the slowest legitimate submit)
MIN_GRACE_S = REPLY_GRACE_MIN_S
RULE = ("fewest non-terminal jobs; a tie goes to the local host, then to "
        "the project's host order")
STATUSES = ("chosen", "busy", "unreachable", "blocked", "unsuitable")


def placements_dir(state_dir: Path) -> Path:
    return Path(state_dir) / "placements"


def record_path(state_dir: Path, request_id: str) -> Path:
    return placements_dir(state_dir) / f"{_safe(request_id)}.json"


def new_request_id(host: str) -> str:
    """A request id for a remote submit given none (D6): the placing host's
    label, the UTC time and 6 hex digits (``cli.REQUEST_ID``'s alphabet)."""
    import datetime as dt
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{host}-{stamp}-{secrets.token_hex(3)}"


def redact_argv(argv: list[str]) -> list[str]:
    """``argv`` with every ``--env`` value of the ACS options replaced
    (names kept); the workload command after ``--`` is kept as given (the
    destination's manifest records it too)."""
    out: list[str] = []
    value_next = False
    for i, arg in enumerate(argv):
        if arg == "--" and not value_next:
            return out + argv[i:]
        if value_next:
            out.append(f"{arg.partition('=')[0]}={REDACTED}")
            value_next = False
        elif arg == "--env":
            out.append(arg)
            value_next = True
        elif arg.startswith("--env="):
            name = arg[len("--env="):].partition("=")[0]
            out.append(f"--env={name}={REDACTED}")
        else:
            out.append(arg)
    return out


def read(state_dir: Path, request_id: str) -> dict | Unreadable | None:
    record = read_json(record_path(state_dir, request_id))
    if isinstance(record, dict) and record.get("request_id") != request_id:
        return Unreadable(record_path(state_dir, request_id),
                          f"it records request id "
                          f"{record.get('request_id')!r}, not "
                          f"{request_id!r}")
    return record


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write(state_dir: Path, record: dict) -> None:
    path = record_path(state_dir, record["request_id"])
    private_dir(path.parent)
    write_json(path, record)
    _fsync_dir(path.parent)


def _event(phase: str, **fields) -> dict:
    return history_item("cli", phase=phase, **fields)


def frees(record) -> bool:
    """A record that binds its request id nowhere: ``refused`` without a
    job (proven never reserved there)."""
    return isinstance(record, dict) and record.get("phase") == "refused" \
        and not record.get("remote_job_id")


def bound_elsewhere(record, host: str) -> bool:
    """Whether a record binds its request id to a host other than
    ``host`` (an unreadable record binds it everywhere: C4)."""
    if isinstance(record, Unreadable):
        return True
    if not isinstance(record, dict):
        return False
    if record.get("host") == host:
        return False
    return not frees(record)


def blocks_send(record, host: str) -> bool:
    """Whether an existing record forbids sending its request id to
    ``host`` now (G3b): only a record that frees it (``refused`` without a
    job) or one ``resolved-not-accepted`` by ``host`` itself allows a
    send; an open, accepted or unreadable record binds the id (a lost
    reply is resolved by request id first, never re-sent blind)."""
    if record is None:
        return False
    if isinstance(record, Unreadable) or not isinstance(record, dict):
        return True
    if frees(record):
        return False
    return not (record.get("phase") == "resolved-not-accepted" and
                record.get("host") == host)


def commit(state_dir: Path, request_id: str, host: str, *,
           route: dict | None, pinned: bool, project: str | None,
           argv: list[str], placing_host: str,
           placing_host_id: str | None, version: dict,
           command: str = "submit", policy: dict | None = None,
           code: dict | None = None, mapping: dict | None = None,
           placer: dict | None = None,
           guard=None) -> tuple[dict, dict | Unreadable | str | None]:
    """Write the ``committed`` record (under the state lock).  Returns
    ``(record, None)``; ``({}, existing)`` when an existing record binds
    the request id (``blocks_send``: another host, an open or accepted
    record, or one that cannot be read); ``({}, reason)`` when ``guard()``
    (run under the lock) returns a refusal.  Nothing is written in either
    refusal.  A record re-sent to the same host (after
    ``resolved-not-accepted``) is reused: its history and replies are
    kept.  ``policy``: the automatic placement's snapshot (every host and
    why it was or was not chosen, the raw replies), None when pinned;
    ``code``: the code-consistency requirement sent; ``mapping``: the
    ``on.<host>`` values applied to the forwarded arguments."""
    with state_lock(Path(state_dir)):
        existing = read(state_dir, request_id)
        if blocks_send(existing, host):
            return {}, existing
        refusal = guard() if guard is not None else None
        if refusal:
            return {}, refusal
        now = utc_now()
        record = existing if isinstance(existing, dict) else {
            "schema": PLACEMENT_SCHEMA, "request_id": request_id,
            "created_utc": now, "history": [], "replies": []}
        record.update({
            "host": host, "route": route, "pinned": pinned,
            "command": command,
            "project": project, "argv": redact_argv(argv),
            "placing_host": placing_host,
            "placing_host_id": placing_host_id,
            "acs_version": version.get("version"),
            "acs_source_sha": version.get("source_sha"),
            "policy": policy, "code": code, "mapping": mapping,
            # review round 1: the placing process (start-time-proven): no
            # resolution by time while it may still be sending
            "placer": placer, "send": None,
            "phase": "committed", "updated_utc": now})
        # a re-send to the same host keeps what an earlier reply named
        record.setdefault("remote_job_id", None)
        record.setdefault("destination", None)
        record["history"].append(_event("committed",
                                        reason=f"destination {host} fixed"))
        _write(state_dir, record)
        return record, None


def advance(state_dir: Path, record: dict, phase: str, reason: str,
            reply: dict | None = None, **fields) -> dict:
    """Move the record to ``phase`` (fsynced), adding ``reply`` verbatim.
    The record on disk is the base (a concurrent same-host re-send keeps
    its entries); ``record`` only when it cannot be read."""
    if phase not in PHASES:
        raise ValueError(f"unknown placement phase {phase!r}")
    with state_lock(Path(state_dir)):
        current = read(state_dir, record["request_id"])
        record = dict(current if isinstance(current, dict) else record)
        now = utc_now()
        if record.get("phase") == "accepted" and phase == "reply-lost" and \
                record.get("remote_job_id"):
            # G3b: a query by request id found the job meanwhile (a
            # resolution while this sender waited); a reply lost after
            # that changes nothing
            record["history"] = [*record.get("history", []),
                                 _event("accepted", reason="kept: " +
                                        reason)]
            if reply is not None:
                record["replies"] = [*record.get("replies", []),
                                     {"utc": now, **reply}]
            record["updated_utc"] = now
            _write(state_dir, record)
            return record
        record["history"] = [*record.get("history", []),
                             _event(phase, reason=reason)]
        if reply is not None:
            record["replies"] = [*record.get("replies", []),
                                 {"utc": now, **reply}]
        record.update(fields)
        if phase == "sent":
            record["sent_utc"] = now
        record.update(phase=phase, reason=reason, updated_utc=now)
        _write(state_dir, record)
        return record


def note_client(state_dir: Path, request_id: str, client: dict) -> dict:
    """Record the start-time-proven identity of a send's ssh client
    (``send.client``) on the ``sent`` record, before the client may run
    (``remote`` gate).  Raises when it cannot (nothing is sent then)."""
    if not isinstance(client, dict) or not client.get("pid"):
        raise ValueError("no identity for the ssh client")
    with state_lock(Path(state_dir)):
        record = read(state_dir, request_id)
        if not isinstance(record, dict) or record.get("phase") != "sent" or \
                not isinstance(record.get("send"), dict):
            raise ValueError("the record is not a send in progress")
        record = dict(record)
        record["send"] = {**record["send"], "client": client}
        record["updated_utc"] = utc_now()
        _write(state_dir, record)
        return record


def resolve(state_dir: Path, request_id: str, phase: str, reason: str,
            reply: dict | None, resolution: dict, **fields) -> dict | None:
    """G3b: move an OPEN record to ``accepted`` or
    ``resolved-not-accepted`` on the evidence of a query by request id
    (``reply`` verbatim, ``resolution`` the decision's inputs).  Under the
    state lock, and only while the record on disk is still open (a sender's
    own reply, or another resolution, may have settled it meanwhile): None
    then, and nothing is written."""
    if phase not in ("accepted", "resolved-not-accepted"):
        raise ValueError(f"not a resolution phase: {phase!r}")
    with state_lock(Path(state_dir)):
        current = read(state_dir, request_id)
        if not isinstance(current, dict) or current.get("phase") not in OPEN:
            return None
        record = dict(current)
        now = utc_now()
        record["history"] = [*record.get("history", []),
                             _event(phase, reason=reason)]
        if reply is not None:
            record["replies"] = [*record.get("replies", []),
                                 {"utc": now, "kind": "resolution",
                                  **reply}]
        record["resolution"] = {"utc": now, **resolution}
        record.update(fields)
        record.update(phase=phase, reason=reason, updated_utc=now,
                      resolved_utc=now)
        _write(state_dir, record)
        return record


def sent_utc(record: dict) -> str | None:
    """When the record's request was (last) sent: ``sent_utc``, else the
    last ``sent`` history entry (G3a records), else its creation."""
    if isinstance(record.get("sent_utc"), str):
        return record["sent_utc"]
    for item in reversed(record.get("history") or []):
        if isinstance(item, dict) and item.get("phase") == "sent" and \
                isinstance(item.get("utc"), str):
            return item["utc"]
    return record.get("created_utc")


def unbounded(record: dict) -> bool:
    """A send without a client call timeout (``run``)."""
    send = record.get("send")
    return isinstance(send, dict) and "call_timeout_s" in send and \
        send["call_timeout_s"] is None


def grace_s(configured: float | None, record: dict,
            default_timeout_s: float = 0.0) -> float:
    """The grace before "not found" may resolve a record
    ``resolved-not-accepted``: the route's ``reply_grace_s`` (at least
    ``MIN_GRACE_S``; the config refuses less), and never less than the
    send's call timeout plus ``MIN_GRACE_S`` (review round 1: the timeout
    ends the client, not the remote command, which may reserve the job
    until the destination's own grace after it).  The timeout is the one
    recorded at ``sent`` (``send.call_timeout_s``), else the longest of the
    send replies (a G3a record), else ``default_timeout_s``.  An unbounded
    send (``run``): ``reply_grace_s`` counted from its client's end
    (``grace_start``)."""
    base = max(MIN_GRACE_S, float(configured or MIN_GRACE_S))
    send = record.get("send") if isinstance(record.get("send"), dict) \
        else {}
    if "call_timeout_s" in send:
        if send["call_timeout_s"] is None:
            return base
        timeout = float(send["call_timeout_s"])
    else:
        timeouts = [float(r["timeout_s"]) for r in record.get("replies") or []
                    if isinstance(r, dict) and r.get("kind") != "resolution"
                    and isinstance(r.get("timeout_s"), (int, float))]
        timeout = max(timeouts) if timeouts else float(default_timeout_s or 0)
    return max(base, timeout + MIN_GRACE_S)


def grace_start(record: dict) -> str | None:
    """Where the grace is counted from: the send; for an unbounded send,
    the end of its client (the last send reply), None while there is
    none."""
    if not unbounded(record):
        return sent_utc(record)
    ends = [r.get("utc") for r in record.get("replies") or []
            if isinstance(r, dict) and r.get("kind") != "resolution" and
            isinstance(r.get("utc"), str)]
    return ends[-1] if ends else None


def _age_s(since, now: str | None = None) -> float | None:
    import datetime as dt
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    if not isinstance(since, str):
        return None
    try:
        then = dt.datetime.strptime(since, fmt)
        current = dt.datetime.strptime(now or utc_now(), fmt)
    except ValueError:
        return None
    return (current - then).total_seconds()


def records(state_dir: Path) -> list[dict | Unreadable]:
    """Every placement record of this host, oldest first by name."""
    root = placements_dir(state_dir)
    try:
        if not is_dir_strict(root):
            return []
        paths = glob_strict(root, "*.json")
    except OSError as error:
        # A49: a placements dir that cannot be examined or listed may bind
        # request ids to other hosts: one unreadable record, which binds
        # every request id everywhere (C4), never "no records"
        return [Unreadable(root, f"{type(error).__name__}: "
                                 f"{error.strerror or error}")]
    return [read_json(path) for path in paths]


def view(record) -> dict:
    """The listing form of a record (``status --all``, ``hosts``)."""
    if isinstance(record, Unreadable):
        return record.view()
    destination = record.get("destination") or {}
    sent = sent_utc(record)
    return {"request_id": record.get("request_id"),
            "host": record.get("host"), "phase": record.get("phase"),
            "open": record.get("phase") in OPEN,
            "pinned": record.get("pinned"),
            "remote_job_id": record.get("remote_job_id"),
            "project": record.get("project"),
            "destination_host_id": destination.get("host_id"),
            "destination_head": destination.get("head"),
            "created_utc": record.get("created_utc"),
            "sent_utc": sent,
            # G3b: how long since the send (since creation when never
            # sent), and since the last change
            "age_s": _age_s(sent),
            "updated_utc": record.get("updated_utc"),
            "since_update_s": _age_s(record.get("updated_utc")),
            "reason": record.get("reason")}


def policy_view(record) -> dict | None:
    """The placement as a job view shows it (G3b): the policy snapshot's
    candidates and reasons (without the raw replies, which the record
    keeps), the code requirement, the mapping and the destination."""
    if not isinstance(record, dict):
        return None
    policy = record.get("policy")
    return {"request_id": record.get("request_id"),
            "host": record.get("host"), "phase": record.get("phase"),
            "pinned": record.get("pinned"),
            "placing_host": record.get("placing_host"),
            "policy": strip_raw(policy) if isinstance(policy, dict)
            else None,
            "code": record.get("code"), "mapping": record.get("mapping"),
            "destination": record.get("destination")}


def strip_raw(policy: dict) -> dict:
    return {**policy, "candidates": [
        {k: v for k, v in c.items() if k != "raw"}
        for c in policy.get("candidates") or []]}


def find(state_dir: Path, ident: str) -> tuple[dict | None, list]:
    """The record naming ``ident`` as its request id or its destination's
    job id; and the unreadable records seen on the way (C4: they are
    reported, never read as absent)."""
    unreadable = []
    try:
        direct = read(state_dir, ident)
    except ValueError:
        direct = None
    if isinstance(direct, dict):
        return direct, unreadable
    if isinstance(direct, Unreadable):
        unreadable.append(direct)
    for record in records(state_dir):
        if isinstance(record, Unreadable):
            if record not in unreadable and \
                    all(record.path != u.path for u in unreadable):
                unreadable.append(record)
            continue
        if record.get("remote_job_id") == ident:
            return record, unreadable
    return None, unreadable


# -- the automatic placement policy (G3b, D5) ------------------------------------------

def summary_problems(name: str, summary: dict) -> list[tuple[str, str]]:
    """Why a host that answered cannot take work now, each with its
    category (``unsuitable`` or ``blocked``): the conditions that do not
    depend on the project (``hosts`` reports them too)."""
    found = []
    if summary.get("host") != name:
        found.append(("unsuitable",
                      f"reports host {summary.get('host')!r}, not {name!r}: "
                      "an alias of another entry, or a renamed remote"))
    floor = summary.get("floor") or {}
    if floor.get("ok") is False:
        found.append(("unsuitable", "below the safety floor: " +
                      "; ".join(floor.get("problems") or [])))
    if summary.get("blocked") == "unreadable":
        found.append(("blocked", "its host block record is unreadable"))
    elif summary.get("blocked"):
        found.append(("blocked", "host admission is blocked"))
    if summary.get("gate") == "unreadable":
        found.append(("blocked", "its dispatch gate record is unreadable"))
    elif summary.get("gate") == "closed":
        found.append(("blocked", "dispatch gate closed: "
                      f"{summary.get('gate_reason')}"))
    elif (summary.get("dispatch") or {}).get("held"):
        found.append(("blocked", "dispatch held: "
                      f"{(summary.get('dispatch') or {}).get('reason')}"))
    if (summary.get("pin") or {}).get("ok") is False:
        found.append(("unsuitable", "its engine differs from its pinned "
                      "sha256 (submits are refused there)"))
    if summary.get("lost_suspected"):
        found.append(("blocked", "lost supervision suspected: " + ", ".join(
            j.get("job_id") or "?" for j in summary["lost_suspected"])))
    return found


def checkout_for(summary: dict, project: str, cwd: str | None
                 ) -> dict | None:
    """The reported checkout of ``project`` that contains ``cwd`` (the
    longest such root), or None when none is known."""
    if not cwd:
        return None
    found = None
    info = (summary.get("projects") or {}).get(project) or {}
    for checkout in info.get("checkouts") or []:
        path = checkout.get("path") if isinstance(checkout, dict) else None
        if not isinstance(path, str) or checkout.get("pattern"):
            continue
        root = path.rstrip("/") or "/"
        if cwd == root or cwd.startswith(root.rstrip("/") + "/"):
            if found is None or len(root) > len(found["path"]):
                found = checkout
    return found


TRANSPORT_UNREACHABLE = ("unreachable", "timeout", "error")


def evaluate(project: str, order: list[str], local: str,
             probes: dict[str, dict], *, reserve_gb: float | None = None,
             cwd: dict | None = None, container: dict | None = None,
             required_commit: str | None = None,
             require_clean: bool = False) -> dict:
    """The placement decision (module docstring).  ``order``: the project's
    hosts in config order; ``probes[name]``: ``{"state"`` (``ok``,
    ``unreachable``, ``timeout``, ``error``, ``old-remote``,
    ``unparseable``), ``"summary"``, ``"reasons"`` (the transport's),
    ``"raw"}``; ``cwd[name]``/``container[name]``: the job's cwd and
    container on each remote (None: unknown; ``container`` only when the
    job names one).  Returns ``{"rule", "project", "chosen",
    "candidates"}``: every host of ``order``, in order, with ``status``
    and ``reasons``."""
    cwd = cwd or {}
    by_id: dict[str, list[str]] = {}
    for name in order:
        summary = (probes.get(name) or {}).get("summary") or {}
        if summary.get("host_id"):
            by_id.setdefault(summary["host_id"], []).append(name)
    candidates = []
    for name in order:
        probe = probes.get(name) or {"state": "error",
                                     "reasons": ["not probed"]}
        summary = probe.get("summary")
        item = {"host": name, "local": name == local, "status": None,
                "reasons": [], "unfinished": None, "host_id": None,
                "version": None, "head": None, "dirty": None,
                "cwd": None if name == local else cwd.get(name),
                "raw": probe.get("raw")}
        found: list[tuple[str, str]] = []
        state = probe.get("state")
        if state in TRANSPORT_UNREACHABLE:
            found += [("unreachable", r) for r in probe.get("reasons") or
                      [f"no usable reply ({state})"]]
        elif state != "ok" or not isinstance(summary, dict):
            found += [("unsuitable", r) for r in probe.get("reasons") or
                      [f"no usable summary ({state})"]]
        else:
            item.update(host_id=summary.get("host_id"),
                        version=summary.get("version"))
            found += summary_problems(name, summary)
            others = [n for n in by_id.get(summary.get("host_id"), [])
                      if n != name]
            if others:
                found.append(("unsuitable", "the same host id as "
                              f"{', '.join(others)}: one installation "
                              "under several names"))
            info = (summary.get("projects") or {}).get(project)
            if not isinstance(info, dict):
                found.append(("unsuitable", f"project {project!r} is not "
                              "configured there"))
            elif name not in (info.get("hosts") or [name]):
                found.append(("unsuitable", f"project {project!r} there "
                              f"does not list {name!r} among its hosts "
                              f"{info.get('hosts')}"))
            budget = (summary.get("budget") or {}).get("effective_budget_gb")
            if isinstance(reserve_gb, (int, float)) and \
                    isinstance(budget, (int, float)) and budget < reserve_gb:
                found.append(("unsuitable", f"its budget {budget:g} GB is "
                              f"below the declared reserve {reserve_gb:g} "
                              "GB"))
            if name != local:
                if not cwd.get(name):
                    found.append(("unsuitable", f"no projects.{project}"
                                  f".on.{name}.cwd here: the job's cwd "
                                  "there is unknown"))
                if container is not None and not container.get(name):
                    found.append(("unsuitable", "the job names a "
                                  f"container; no projects.{project}.on."
                                  f"{name}.container here"))
                checkout = checkout_for(summary, project, cwd.get(name))
                if checkout is not None:
                    item.update(head=checkout.get("head"),
                                dirty=checkout.get("dirty"))
                    if required_commit and checkout.get("head") and \
                            checkout["head"] != required_commit:
                        found.append(("unsuitable", "its checkout "
                                      f"{checkout.get('path')} is at "
                                      f"{checkout['head']}, not the "
                                      f"required {required_commit}"))
                    if require_clean and checkout.get("dirty"):
                        found.append(("unsuitable", "its checkout "
                                      f"{checkout.get('path')} has "
                                      "uncommitted changes; this one has "
                                      "none"))
            jobs = (summary.get("jobs") or {}).get("unfinished")
            if isinstance(jobs, bool) or not isinstance(jobs, int):
                found.append(("unsuitable", "it reports no count of "
                              "unfinished jobs"))
            else:
                item["unfinished"] = jobs
        for category in ("unreachable", "unsuitable", "blocked"):
            if any(c == category for c, _ in found):
                item["status"] = category
                break
        item["reasons"] = [r for _, r in found]
        candidates.append(item)
    eligible = [c for c in candidates if c["status"] is None]
    chosen = None
    if eligible:
        low = min(c["unfinished"] for c in eligible)
        tied = [c for c in eligible if c["unfinished"] == low]
        chosen = next((c for c in tied if c["local"]), tied[0])
        for item in eligible:
            if item is chosen:
                item["status"] = "chosen"
            elif item["unfinished"] > low:
                item["status"] = "busy"
                item["reasons"] = [f"{item['unfinished']} unfinished "
                                   f"job(s); {chosen['host']} has {low}"]
            else:
                item["status"] = "busy"
                item["reasons"] = [
                    f"a tie at {low} unfinished job(s): it goes to " +
                    (f"the local host ({chosen['host']})" if chosen["local"]
                     else f"{chosen['host']}, earlier in the project's "
                     "host order")]
    return {"rule": RULE, "project": project,
            "chosen": chosen["host"] if chosen else None,
            "candidates": candidates}
