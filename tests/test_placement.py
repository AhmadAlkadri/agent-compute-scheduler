"""Campaign G slice G3a: hosts registry, identity and placement records
(decisions D5, D6; basis record Part 2 items 1-5, 7, 9; findings F8; the
executor corpus §4.2 item 9; the coupling inventory J).

Every remote here is reached through a **fake ssh** on ``PATH`` (a script
that answers like the remote): it runs the forwarded command against a
second isolated ``Env`` (its own config, state, queue and guard home), or,
per target, answers like an unreachable host, a timed-out one, an old
remote without ``_summary`` (the verbatim argparse reply of v0.1.2), a
reply with rc 0 but no JSON, or drops the connection after the remote
accepted the job.  Nothing here reaches a real host.

What this establishes:

* config: ``projects.<p>.hosts`` (ordered; default ``[host]``),
  ``projects.<p>.on.<host>`` (cwd, container, container_workdir for a
  remote), ``[hosts.*]`` validation (no alias equal to ``host``, no two
  entries on one route), and the connect/call timeouts on every remote
  call (``remote.call``, capture mode: rc, stdout, stderr verbatim);
* ``_summary --json`` reports host, host id, version, source sha, floor,
  ``blocked`` (true/false/"unreadable"), gate, daemon, jobs by phase, the
  running job, the budget, the projects and their checkouts' HEAD and
  dirty state, and **writes nothing** (state dir, events log, guard home,
  checkouts: file list and mtimes before and after); "lost suspected" is
  reported, never written;
* ``hosts --json``: every host in parallel with timeouts; reachability,
  alias and renamed-remote mismatches, duplicate host ids, an unreadable
  block, an old remote (ineligible, the reply verbatim), rc 0 without
  JSON; the open placement records; it writes nothing;
* ``--host X submit`` writes a placement record (``pinned: true``),
  fsynced before the call (the fake ssh copies it at call time): committed
  -> sent -> accepted | refused | reply-lost, the destination's host id,
  version and HEAD, every reply verbatim, a request id generated when
  absent, ``--env`` values redacted; a request id bound to one host is
  never sent to another; ``status --all`` lists the records;
* ``status --request-id R`` (binding and record scan);
* ``status``/``wait``/``cancel``/``reconcile --terminate``/``log``/``fetch``
  given a job id or request id of a local placement record go to the
  recorded host (the view carries ``route``); nothing runs locally when
  the route fails;
* ``--host X log`` and ``--host X fetch`` (manifest, state, output.log,
  guard records); ``status`` shows ``admission_memory``;
* the boundary: a project whose only host is another one is refused
  without ``--host`` (exit 2, the candidates, nothing written); a project
  whose only host is this one runs here; since G3b a project that declares
  several hosts is placed (``tests/test_g3b_placement.py``).

Since G3b the fake ssh also bounds calls made from inside calls
(``FAKE_SSH_MAX_DEPTH``: a bounce between hosts, register A40, ends with
exit 255 instead of running away), can apply its mode to one subcommand
only (``FAKE_SSH_ONLY``) and logs, per call, the far side's job count and
the placing host's record at call time (``FAKE_SSH_ORDER``).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from conftest import BIN, WORKLOAD, Env, git

from agent_compute import config as cfgmod
from agent_compute import procs, remote
from agent_compute.state import write_json

PY = sys.executable

# The reply of agent-compute v0.1.2 (0.1.2-eb84c0e791, deployed on both
# hosts until v0.2.0) to `_summary --json`, verbatim (argparse, exit 2).
OLD_REMOTE_STDERR = (
    "usage: agent-compute [-h] [--config CONFIG] [--host HOST] [--version]\n"
    "                     {doctor,submit,run,status,wait,cancel,reconcile,"
    "resume,daemon,pin-backend,_exec,_argv} ...\n"
    "agent-compute: error: argument cmd: invalid choice: '_summary' (choose "
    "from 'doctor', 'submit', 'run', 'status', 'wait', 'cancel', "
    "'reconcile', 'resume', 'daemon', 'pin-backend', '_exec', '_argv')\n")

FAKE_SSH = r"""#!/bin/bash
# fake ssh: `ssh -o K=V ... TARGET INNER` runs INNER here, or answers like
# the remote would per mode (FAKE_SSH_MODE, or FAKE_SSH_MODE_<target>)
echo "$*" >> "$FAKE_SSH_LOG"
while [ "$1" = "-o" ]; do shift 2; done
target="$1"; inner="$2"
var="FAKE_SSH_MODE_${target//-/_}"
mode="${!var:-$FAKE_SSH_MODE}"
# G3b: a bound on calls made from inside calls (a call bouncing between
# hosts, register A40, ends here instead of running away)
depth=$(( ${FAKE_SSH_DEPTH:-0} + 1 )); export FAKE_SSH_DEPTH=$depth
if [ "$depth" -gt "${FAKE_SSH_MAX_DEPTH:-4}" ]; then
    echo "fake ssh: call depth $depth to $target exceeds the bound" >&2
    exit 255
fi
script=$(printf '%s' "$inner" |
         sed -n 's/.*echo \([A-Za-z0-9+\/=]*\) |.*/\1/p' | base64 -d 2>/dev/null)
# G3b: the mode applies only to calls of this subcommand (else normal)
if [ -n "$FAKE_SSH_ONLY" ]; then
    case "$script" in *" $FAKE_SSH_ONLY "*) ;; *) mode="";; esac
fi
if [ -n "$FAKE_SSH_ORDER" ]; then
    # G3b ordering: per call, the far side's job count and the placing
    # host's record, at call time
    jobs=$(ls "$FAKE_SSH_ORDER_FAR" 2>/dev/null | wc -l | tr -d ' ')
    rec=$(cat "$FAKE_SSH_ORDER_NEAR"/*.json 2>/dev/null | tr -d '\n')
    printf '%s\t%s\t%s\n' "$(printf '%s' "$script" | head -1)" "$jobs" \
        "$rec" >> "$FAKE_SSH_ORDER"
fi
# G3b review round 1: this process's pid (the client ACS recorded: the
# gated shell execs into it, so the pid is the same)
[ -n "$FAKE_SSH_PIDFILE" ] && echo "$$" >> "$FAKE_SSH_PIDFILE"
if [ -n "$FAKE_SSH_SNAPSHOT" ]; then
    # what the placing host had recorded when the call was made
    for f in "$FAKE_SSH_SNAPSHOT_SRC"/*.json; do
        [ -f "$f" ] && cat "$f" >> "$FAKE_SSH_SNAPSHOT"
    done
fi
case "$mode" in
    unreachable)
        echo "ssh: connect to host $target port 22: Connection refused" >&2
        exit 255;;
    timeout)
        exec sleep 60;;
    old)
        printf '%s' "$FAKE_SSH_OLD_STDERR" >&2
        exit 2;;
    nojson)
        echo "Welcome to $target"
        exit 0;;
    slow)
        sleep "${FAKE_SSH_DELAY:-2}";;
    drop-after)
        # the remote runs it; the connection drops before the reply
        bash -c "$inner" > /dev/null 2>&1
        echo "Connection to $target closed by remote host." >&2
        exit 255;;
    collapse)
        # a Windows OpenSSH server with PowerShell: every nonzero exit
        # code arrives as 1 (the live Lenovo route, 2026-10-08)
        bash -c "$inner"; rc=$?
        [ "$rc" = 0 ] && exit 0
        exit 1;;
esac
bash -c "$inner"; rc=$?
exit $rc
"""
FAKE_WSL = """#!/bin/bash
# fake wsl: `wsl -d DISTRO -- CMD...` runs CMD
while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do shift; done
shift
exec "$@"
"""


class Pair:
    """A near Env (the placing host, label ``testhost``) whose ``[hosts]``
    entries reach, through the fake ssh, one far Env (label ``far``
    unless renamed)."""

    def __init__(self, *, far_queue=False, far_label="far",
                 routes=(("far", "far-target"),), transport="ssh",
                 near_extra="", call_timeout_s=None, old_routes=()):
        self.far = Env(queue=far_queue)
        self.far.config.write_text(self.far.config.read_text().replace(
            'host = "testhost"', f'host = "{far_label}"'))
        self.fakebin = self.far.root / "fakebin"
        self.fakebin.mkdir()
        for name, text in (("ssh", FAKE_SSH), ("wsl", FAKE_WSL)):
            (self.fakebin / name).write_text(text)
            (self.fakebin / name).chmod(0o755)
        self.wrapper = self.far.root / "remote-agent-compute"
        self.wrapper.write_text(f'#!/bin/bash\nexec "{PY}" "{BIN}" '
                                f'--config "{self.far.config}" "$@"\n')
        self.wrapper.chmod(0o755)
        # an ACS older than 0.2.0 at the far end: its CLI's reply to
        # `_summary`, verbatim (argparse usage error, exit 2)
        self.old_wrapper = self.far.root / "old-agent-compute"
        (self.far.root / "old-stderr.txt").write_text(OLD_REMOTE_STDERR)
        self.old_wrapper.write_text(
            f'#!/bin/bash\ncat "{self.far.root / "old-stderr.txt"}" >&2\n'
            'exit 2\n')
        self.old_wrapper.chmod(0o755)
        distro = 'distro = "Ubuntu"\n' if transport == "ssh-wsl" else ""
        timeouts = (f"call_timeout_s = {call_timeout_s}\n"
                    if call_timeout_s else "")
        tables = "".join(
            f'[hosts.{key}]\ntransport = "{transport}"\nssh = "{target}"\n'
            f'agent_compute = '
            f'"{self.old_wrapper if key in old_routes else self.wrapper}"\n'
            f'{distro}{timeouts}\n'
            for key, target in routes)
        self.near = Env(queue=False, extra_config=near_extra + tables)
        self.ssh_log = self.far.root / "ssh.log"

    def env(self, modes=None, **extra):
        env = self.near.env(
            PATH=f"{self.fakebin}{os.pathsep}{os.environ['PATH']}",
            FAKE_SSH_LOG=str(self.ssh_log), FAKE_SSH_MODE="",
            FAKE_SSH_OLD_STDERR=OLD_REMOTE_STDERR, **extra)
        for target, mode in (modes or {}).items():
            env[f"FAKE_SSH_MODE_{target.replace('-', '_')}"] = mode
        return env

    def acs(self, *args, modes=None, timeout=180, **extra):
        return self.near.acs(*args, env=self.env(modes, **extra),
                             timeout=timeout)

    def calls(self) -> int:
        return len(self.ssh_log.read_text().splitlines()) \
            if self.ssh_log.exists() else 0

    def submit(self, *extra, name="w", sleep=0, host="far", modes=None,
               request_id=None, **env_extra):
        args = ["--host", host, "submit", "--json", "--case", "t",
                "--timeout-min", "1", "--reserve-gb", "0.5", "--cwd",
                str(self.far.projects["alpha"])]
        if request_id:
            args += ["--request-id", request_id]
        return self.acs(*args, *extra, "--", PY, str(WORKLOAD), "--out",
                        str(self.far.root / f"{name}.jsonl"), "--sleep",
                        str(sleep), modes=modes, **env_extra)

    def record(self, request_id: str) -> dict:
        return json.loads((self.near.state / "placements" /
                           f"{request_id}.json").read_text())

    def cleanup(self):
        self.near.cleanup()
        self.far.cleanup()


@pytest.fixture
def pair():
    p = Pair()
    yield p
    p.cleanup()


@pytest.fixture
def qpair():
    """The far side with its queue (jobs that run, wait, cancel)."""
    p = Pair(far_queue=True)
    yield p
    p.cleanup()


def snapshot(*roots: Path) -> dict:
    """Every path under ``roots`` (and the roots) with its type, mode, size
    and mtime: a write anywhere changes it."""
    found = {}
    for root in roots:
        root = Path(root)
        if not root.exists():
            found[str(root)] = None
            continue
        for path in [root, *root.rglob("*")]:
            st = path.lstat()
            found[str(path)] = (st.st_mode, st.st_size, st.st_mtime_ns)
    return found


def summary(env: Env, *args) -> dict:
    result = env.acs("_summary", "--json", *args)
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


def hosts(p: Pair, modes=None, **extra) -> dict:
    result = p.acs("hosts", "--json", modes=modes, **extra)
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


def entry(report: dict, name: str) -> dict:
    [found] = [h for h in report["hosts"] if h["name"] == name]
    return found


def lost_job(env: Env) -> str:
    """A job of this host whose recorded runner is gone (lost supervision,
    not yet detected)."""
    proc = subprocess.Popen([PY, "-c", "import time; time.sleep(0.5)"])
    ident = procs.identity(proc.pid)
    proc.wait()
    job = "acs-20261008T000000Z-10571a"
    write_json(env.state / "jobs" / job / "manifest.json",
               {"job_id": job, "host": "testhost", "project": "alpha",
                "request_id": "lost-1"})
    write_json(env.state / "jobs" / job / "state.json",
               {"job_id": job, "host": "testhost", "phase": "executing",
                "runner": ident, "accepted_utc": "2026-10-08T00:00:00Z",
                "history": []})
    return job


# --- config ---------------------------------------------------------------------------

def test_project_hosts_default_to_this_host():
    env = Env(queue=False)
    try:
        config = cfgmod.load(env.config)
        assert config.projects["alpha"].hosts == ["testhost"]
        assert config.projects["alpha"].on == {}
    finally:
        env.cleanup()


MAPPED = ('[hosts.far]\ntransport = "ssh"\nssh = "far-target"\n'
          'agent_compute = "/opt/acs/bin/agent-compute"\n'
          'connect_timeout_s = 5\ncall_timeout_s = 30\n'
          '[projects.alpha.on.far]\ncwd = "/srv/alpha"\n'
          'container = "alpha-box"\ncontainer_workdir = "/work"\n')


def _with_hosts(text: str, hosts: str) -> str:
    return text.replace("[projects.alpha]\n",
                        f"[projects.alpha]\nhosts = {hosts}\n", 1)


def test_project_hosts_and_the_remote_mapping_load():
    env = Env(queue=False, extra_config=MAPPED)
    try:
        env.config.write_text(_with_hosts(env.config.read_text(),
                                          '["far", "testhost"]'))
        config = cfgmod.load(env.config)
        alpha = config.projects["alpha"]
        assert alpha.hosts == ["far", "testhost"]           # ordered
        assert alpha.on == {"far": {"cwd": "/srv/alpha",
                                    "container": "alpha-box",
                                    "container_workdir": "/work"}}
        assert config.projects["beta"].hosts == ["testhost"]
        far = config.remotes["far"]
        assert (far.connect_timeout_s, far.call_timeout_s) == (5.0, 30.0)
    finally:
        env.cleanup()


@pytest.mark.parametrize("hosts,extra,needle", [
    ('["nowhere"]', "", "nowhere"),
    ('["testhost", "testhost"]', "", "twice"),
    ("[]", "", "non-empty"),
    ('"testhost"', "", "list"),
    ('["testhost"]', '[projects.alpha.on.far]\ncwd = "/srv/a"\n',
     "not one of"),
    ('["testhost", "far"]', '[projects.alpha.on.testhost]\ncwd = "/a"\n',
     "this host"),
    ('["testhost", "far"]', '[projects.alpha.on.far]\ncwdd = "/srv/a"\n',
     "unknown keys"),
    ('["testhost", "far"]', '[projects.alpha.on.far]\ncwd = "srv/a"\n',
     "absolute"),
    # review round 1 (finding 13): the container workdir too
    ('["testhost", "far"]', '[projects.alpha.on.far]\ncwd = "/srv/a"\n'
     'container_workdir = "work"\n', "absolute"),
])
def test_bad_project_hosts_are_refused(hosts, extra, needle):
    route = ('[hosts.far]\ntransport = "ssh"\nssh = "far-target"\n'
             'agent_compute = "/x"\n')
    env = Env(queue=False, extra_config=route + extra)
    try:
        env.config.write_text(_with_hosts(env.config.read_text(), hosts))
        with pytest.raises(cfgmod.ConfigError) as error:
            cfgmod.load(env.config)
        assert needle in str(error.value), str(error.value)
    finally:
        env.cleanup()


def test_a_remote_named_like_this_host_is_refused():
    env = Env(queue=False, extra_config=(
        '[hosts.testhost]\ntransport = "ssh"\nssh = "t"\n'
        'agent_compute = "/x"\n'))
    try:
        with pytest.raises(cfgmod.ConfigError) as error:
            cfgmod.load(env.config)
        assert "hosts.testhost" in str(error.value)
        assert "this host" in str(error.value)
    finally:
        env.cleanup()


@pytest.mark.parametrize("second", [
    '[hosts.b]\ntransport = "ssh"\nssh = "same"\nagent_compute = "/y"\n',
    '[hosts.b]\ntransport = "ssh-wsl"\nssh = "same"\ndistro = "d"\n'
    'agent_compute = "/x"\n'])
def test_two_remotes_on_one_route_are_refused(second):
    first = ('[hosts.a]\ntransport = "' +
             ("ssh-wsl" if "wsl" in second else "ssh") +
             '"\nssh = "same"\n' + ('distro = "d"\n' if "wsl" in second
                                    else "") + 'agent_compute = "/x"\n')
    env = Env(queue=False, extra_config=first + second)
    try:
        with pytest.raises(cfgmod.ConfigError) as error:
            cfgmod.load(env.config)
        assert "same route" in str(error.value)
    finally:
        env.cleanup()


def test_two_remotes_on_different_routes_load():
    env = Env(queue=False, extra_config=(
        '[hosts.a]\ntransport = "ssh"\nssh = "one"\nagent_compute = "/x"\n'
        '[hosts.b]\ntransport = "ssh-wsl"\nssh = "one"\ndistro = "d"\n'
        'agent_compute = "/x"\n'))
    try:
        assert sorted(cfgmod.load(env.config).remotes) == ["a", "b"]
    finally:
        env.cleanup()


# --- remote.call: capture mode and timeouts --------------------------------------------

@pytest.fixture
def routed():
    env = Env(queue=False, extra_config=MAPPED + (
        '[hosts.plain]\ntransport = "ssh-wsl"\nssh = "plain-target"\n'
        'distro = "d"\nagent_compute = "/opt/acs"\n'))
    env.config.write_text(_with_hosts(env.config.read_text(),
                                      '["testhost", "far"]'))
    yield cfgmod.load(env.config)
    env.cleanup()


def test_call_captures_the_reply_verbatim(routed, monkeypatch):
    seen = []

    def fake(argv, timeout):
        seen.append((argv, timeout))
        return 3, b"out \x00line\n", b"err line\n", False
    monkeypatch.setattr(remote, "_run", fake)
    reply = remote.call(routed, "far", ["status", "--json"])
    assert (reply.rc, reply.stdout, reply.stderr) == (3, "out line\n",
                                                      "err line\n")
    assert reply.timed_out is False and reply.error is None
    assert reply.elapsed_s >= 0
    assert reply.as_record()["rc"] == 3


def test_every_remote_call_carries_the_connect_timeout_and_keepalives(
        routed, monkeypatch):
    seen = []
    monkeypatch.setattr(remote, "_run", lambda argv, timeout: (
        seen.append((argv, timeout)) or (0, b"{}", b"", False)))
    remote.call(routed, "far", ["status"])
    remote.call(routed, "plain", ["status"])
    remote.forward(routed, "far", ["status"])
    for argv, _ in seen:
        assert argv[0] == "ssh" and "BatchMode=yes" in argv
        assert any(a.startswith("ConnectTimeout=") for a in argv)
        assert any(a.startswith("ServerAliveInterval=") for a in argv)
        assert any(a.startswith("ServerAliveCountMax=") for a in argv)
    assert "ConnectTimeout=5" in seen[0][0]                 # configured
    assert f"ConnectTimeout={remote.CONNECT_TIMEOUT_S:g}" in seen[1][0]
    # the configured call timeout bounds every call of that host; a host
    # without one: 900 s for a call, 30 s for a probe
    assert seen[0][1] == 30.0 and seen[2][1] == 30.0
    assert seen[1][1] == remote.CALL_TIMEOUT_S
    seen.clear()
    remote.call(routed, "plain", ["_summary", "--json"], probe=True)
    assert seen[0][1] == remote.PROBE_TIMEOUT_S


def test_a_wait_gets_its_own_timeout_plus_the_call_timeout(routed,
                                                           monkeypatch):
    seen = []
    monkeypatch.setattr(remote, "_run", lambda argv, timeout: (
        seen.append(timeout) or (0, b"", b"", False)))
    remote.forward(routed, "far", ["wait", "acs-x", "--timeout", "540"])
    remote.forward(routed, "far", ["wait", "acs-x"])
    remote.forward(routed, "far", ["run", "--case", "c", "--", "true"])
    assert seen == [570.0, None, None]


def test_a_timed_out_call_is_reported(routed, monkeypatch, capsys):
    monkeypatch.setattr(remote, "_run", lambda argv, timeout: (
        None, b"partial", b"", True))
    reply = remote.call(routed, "far", ["status"])
    assert reply.timed_out is True and reply.rc is None
    assert reply.stdout == "partial"
    assert remote.forward(routed, "far", ["status"]) == 75
    assert "timed out" in capsys.readouterr().err


def test_a_real_timeout_kills_only_the_calls_own_process_group(tmp_path):
    started = time.monotonic()
    rc, out, err, timed_out = remote._run(
        ["bash", "-c", "echo before; sleep 30 & sleep 30; echo never"], 1.0)
    assert timed_out and rc is None
    assert b"before" in out and b"never" not in out
    assert time.monotonic() - started < 10


# --- _summary: read-only -----------------------------------------------------------------

def test_summary_reports_identity_state_and_projects(env_direct):
    env = env_direct
    job = env.submit(workload_args=["--out", str(env.root / "w.jsonl")])
    env.wait_outcome(job["job_id"])
    s = summary(env)
    assert s["schema"] == "agent-compute-summary/1"
    assert s["read_only"] is True
    assert s["host"] == "testhost"
    host_id = json.loads((env.state / "host-id.json").read_text())["host_id"]
    assert s["host_id"] == host_id
    assert s["version"] and s["source_sha"] and s["engine_sha256"]
    assert s["floor"]["ok"] in (True, None)
    assert s["pin"]["ok"] is True
    assert s["blocked"] is False and s["gate"] == "open"
    assert s["daemon"]["enabled"] is False
    assert s["jobs"]["unfinished"] == 0 and s["jobs"]["by_phase"] == {}
    assert s["running"] is None and s["lost_suspected"] == []
    assert s["budget"]["effective_budget_gb"] > 0
    assert set(s["projects"]) == {"alpha", "beta"}
    [checkout] = s["projects"]["alpha"]["checkouts"]
    head = subprocess.run(["git", "-C", str(env.projects["alpha"]),
                           "rev-parse", "HEAD"], capture_output=True,
                          text=True).stdout.strip()
    assert checkout["head"] == head and checkout["dirty"] is False
    assert s["projects"]["alpha"]["hosts"] == ["testhost"]
    (env.projects["beta"] / "input.txt").write_text("changed\n")
    assert summary(env)["projects"]["beta"]["checkouts"][0]["dirty"] is True


def test_summary_counts_unfinished_jobs_and_the_running_one(env_queue):
    env = env_queue
    a = env.submit(workload_args=["--out", str(env.root / "a.jsonl"),
                                  "--sleep", "30"])
    b = env.submit(workload_args=["--out", str(env.root / "b.jsonl")])
    env.wait_phase(a["job_id"], "executing")
    s = summary(env)
    assert s["jobs"]["unfinished"] == 2
    assert s["jobs"]["by_phase"].get("executing") == 1
    assert s["running"]["job_id"] == a["job_id"]
    assert s["daemon"]["running"] is True
    assert env.acs("cancel", a["job_id"]).returncode == 0
    env.wait_outcome(b["job_id"])


@pytest.mark.parametrize("mode", ["direct", "queue"])
def test_summary_writes_nothing(mode):
    env = Env(queue=mode == "queue")
    try:
        job = env.submit(workload_args=["--out", str(env.root / "w.jsonl")])
        env.wait_outcome(job["job_id"])
        runner = env.state_of(job["job_id"]).get("runner")
        env.wait_for(lambda: procs.liveness(runner) == procs.GONE,
                     what="the runner's exit")
        lost = lost_job(env)
        # an unclaimed job past the grace (settle_unclaimed would finish it)
        stale = "acs-20261001T000000Z-57a1e0"
        write_json(env.state / "jobs" / stale / "state.json",
                   {"job_id": stale, "host": "testhost", "phase": "accepted",
                    "accepted_utc": "2026-10-01T00:00:00Z", "history": []})
        (env.projects["beta"] / "input.txt").write_text("dirty\n")
        events = env.state / "events" / "testhost.jsonl"
        assert events.exists()
        roots = (env.state, env.guard_home, env.projects["alpha"],
                 env.projects["beta"])
        if mode == "queue":
            # the daemon's own files are its own (socket, logs)
            roots = (env.state / "jobs", env.state / "events",
                     env.state / "requests", env.guard_home,
                     env.projects["alpha"], env.projects["beta"])
            before_top = sorted(p.name for p in env.state.iterdir())
        before = snapshot(*roots)
        time.sleep(0.05)
        s = summary(env)
        assert snapshot(*roots) == before
        if mode == "queue":
            assert sorted(p.name for p in env.state.iterdir()) == before_top
        assert not (env.state / "blocks").exists() or \
            not list((env.state / "blocks").iterdir())
        suspected = {j["job_id"] for j in s["lost_suspected"]}
        assert lost in suspected
        assert env.state_of(stale).get("outcome") is None
        assert env.state_of(lost).get("outcome") is None
    finally:
        env.cleanup()


def test_summary_on_a_fresh_state_dir_creates_nothing():
    env = Env(queue=False)
    try:
        assert not env.state.exists()
        s = summary(env)
        assert s["host_id"] is None
        assert not env.state.exists()
    finally:
        env.cleanup()


@pytest.mark.parametrize("content,expected", [
    (None, False), ('{"host": "testhost", "jobs": {"acs-x": {}}}', True),
    ("{not json", "unreadable"), ('{"jobs": []}', "unreadable")])
def test_summary_blocked_is_true_false_or_unreadable(env_direct, content,
                                                     expected):
    env = env_direct
    if content is not None:
        (env.state / "blocks").mkdir(parents=True)
        (env.state / "blocks" / "testhost.json").write_text(content)
    assert summary(env)["blocked"] == expected


def test_summary_reports_a_closed_or_unreadable_gate(env_direct):
    env = env_direct
    env.state.mkdir(parents=True, exist_ok=True)
    gate = env.state / "dispatch-gate.json"
    gate.write_text('{"reason": "daemon restarted"}')
    s = summary(env)
    assert s["gate"] == "closed" and "daemon restarted" in s["gate_reason"]
    gate.write_text("")
    assert summary(env)["gate"] == "unreadable"


# --- hosts ----------------------------------------------------------------------------------

def test_hosts_lists_this_host_and_the_remote_with_identity(pair):
    assert pair.far.acs("doctor").returncode in (0, 1)       # its host id
    far_id = json.loads((pair.far.state / "host-id.json").read_text())
    report = hosts(pair)
    assert [h["name"] for h in report["hosts"]] == ["testhost", "far"]
    local = entry(report, "testhost")
    assert local["local"] is True and local["state"] == "ok"
    far = entry(report, "far")
    assert far["local"] is False and far["reachable"] is True
    assert far["state"] == "ok", far
    assert far["summary"]["host"] == "far"
    assert far["summary"]["host_id"] == far_id["host_id"]
    assert far["summary"]["version"] and far["summary"]["source_sha"]
    assert far["eligible"] is True and far["reasons"] == []
    assert far["route"] == {"transport": "ssh", "ssh": "far-target",
                            "distro": None,
                            "agent_compute": str(pair.wrapper)}
    assert far["raw"]["rc"] == 0
    assert json.loads(far["raw"]["stdout"])["host"] == "far"
    assert report["identity_problems"] == []
    assert report["acs"]["host"] == "testhost"
    human = pair.acs("hosts")
    assert human.returncode == 0 and "far" in human.stdout


def test_hosts_reports_an_alias_and_a_duplicate_host_id():
    p = Pair(routes=(("far", "far-target"), ("far-alias", "alias-target")))
    try:
        assert p.far.acs("doctor").returncode in (0, 1)
        report = hosts(p)
        alias = entry(report, "far-alias")
        assert alias["reachable"] is True and alias["eligible"] is False
        assert any("reports host 'far'" in r for r in alias["reasons"]), alias
        far = entry(report, "far")
        assert far["eligible"] is False
        assert any("same host id" in r for r in far["reasons"]), far
        assert any("far-alias" in prob and "far" in prob
                   for prob in report["identity_problems"])
    finally:
        p.cleanup()


def test_hosts_reports_a_renamed_remote():
    p = Pair(far_label="renamed")
    try:
        report = hosts(p)
        far = entry(report, "far")
        assert far["reachable"] is True and far["eligible"] is False
        assert any("reports host 'renamed'" in r for r in far["reasons"])
    finally:
        p.cleanup()


def test_hosts_reports_unreachable_and_timed_out_remotes():
    p = Pair(routes=(("far", "far-target"), ("gone", "gone-target")),
             call_timeout_s=2)
    try:
        started = time.monotonic()
        report = hosts(p, modes={"far-target": "timeout",
                                 "gone-target": "unreachable"})
        assert time.monotonic() - started < 20
        far, gone = entry(report, "far"), entry(report, "gone")
        assert far["state"] == "timeout" and far["reachable"] is False
        assert far["raw"]["timed_out"] is True and far["raw"]["rc"] is None
        assert gone["state"] == "unreachable"
        assert gone["raw"]["rc"] == 255
        assert "Connection refused" in gone["raw"]["stderr"]
        assert not far["eligible"] and not gone["eligible"]
        assert entry(report, "testhost")["eligible"] is True
    finally:
        p.cleanup()


def test_hosts_reports_an_old_remote_ineligible_with_its_reply(pair):
    report = hosts(pair, modes={"far-target": "old"})
    far = entry(report, "far")
    assert far["state"] == "old-remote" and far["eligible"] is False
    assert far["reachable"] is True
    assert any("_summary" in r and "older" in r for r in far["reasons"])
    assert far["raw"] == {**far["raw"], "rc": 2, "stdout": "",
                          "stderr": OLD_REMOTE_STDERR}
    assert entry(report, "testhost")["eligible"] is True


def test_hosts_reports_a_reply_with_rc_0_but_no_json(pair):
    far = entry(hosts(pair, modes={"far-target": "nojson"}), "far")
    assert far["state"] == "unparseable" and far["eligible"] is False
    assert far["raw"]["rc"] == 0 and "Welcome" in far["raw"]["stdout"]


def test_hosts_reports_an_unreadable_block_of_the_remote(pair):
    (pair.far.state / "blocks").mkdir(parents=True)
    (pair.far.state / "blocks" / "far.json").write_text("{trunc")
    far = entry(hosts(pair), "far")
    assert far["state"] == "ok" and far["summary"]["blocked"] == "unreadable"
    assert far["eligible"] is False
    assert any("unreadable" in r for r in far["reasons"])


def test_hosts_queries_the_remotes_in_parallel():
    p = Pair(routes=(("far", "far-target"), ("far2", "far2-target")))
    try:
        started = time.monotonic()
        report = hosts(p, modes={"far-target": "slow", "far2-target": "slow"},
                       FAKE_SSH_DELAY="4")
        elapsed = time.monotonic() - started
        assert entry(report, "far")["reachable"]
        assert entry(report, "far2")["reachable"]
        assert elapsed < 7.5, elapsed           # sequential: >= 8 s
    finally:
        p.cleanup()


def test_hosts_writes_nothing(pair):
    assert pair.far.acs("doctor").returncode in (0, 1)
    near_roots = (pair.near.root,)
    far_roots = (pair.far.state, pair.far.guard_home)
    before = snapshot(*near_roots), snapshot(*far_roots)
    hosts(pair)
    hosts(pair, modes={"far-target": "old"})
    assert (snapshot(*near_roots), snapshot(*far_roots)) == before


# --- placement records ----------------------------------------------------------------------

def test_a_pinned_submit_writes_its_record_before_the_call(qpair):
    p = qpair
    seen = p.near.root / "seen-at-call.txt"
    r = p.submit(request_id="pin-1", FAKE_SSH_SNAPSHOT=str(seen),
                 FAKE_SSH_SNAPSHOT_SRC=str(p.near.state / "placements"))
    assert r.returncode == 0, r.stdout + r.stderr
    reply = json.loads(r.stdout)
    assert reply["accepted"] is True and reply["host"] == "far"
    # the record existed, phase "sent", when the fake ssh ran
    at_call = json.loads(seen.read_text())
    assert at_call["phase"] == "sent" and at_call["request_id"] == "pin-1"
    assert [h["phase"] for h in at_call["history"]] == ["committed", "sent"]
    rec = p.record("pin-1")
    assert rec["schema"] == "agent-compute-placement/1"
    assert rec["pinned"] is True and rec["host"] == "far"
    assert rec["phase"] == "accepted"
    assert rec["remote_job_id"] == reply["job_id"]
    assert [h["phase"] for h in rec["history"]] == ["committed", "sent",
                                                   "accepted"]
    assert rec["route"]["ssh"] == "far-target"
    assert rec["placing_host"] == "testhost"
    far_manifest = json.loads((p.far.state / "jobs" / reply["job_id"] /
                               "manifest.json").read_text())
    assert rec["destination"]["host"] == "far"
    assert rec["destination"]["host_id"] == far_manifest["host_id"]
    assert rec["destination"]["version"] == far_manifest["acs_version"]
    assert rec["destination"]["head"] == \
        far_manifest["inputs"]["fingerprint"]["git_head"]
    [sent] = rec["replies"]
    assert sent["rc"] == 0 and json.loads(sent["stdout"]) == {
        k: v for k, v in reply.items() if k != "placement"}
    assert reply["placement"]["request_id"] == "pin-1"
    assert reply["placement"]["phase"] == "accepted"
    assert oct((p.near.state / "placements" / "pin-1.json").stat().st_mode
               & 0o777) == "0o600"
    p.far.wait_outcome(reply["job_id"])


def test_a_remote_submit_without_a_request_id_gets_one(qpair):
    p = qpair
    r = p.submit()
    assert r.returncode == 0, r.stdout + r.stderr
    reply = json.loads(r.stdout)
    rid = reply["placement"]["request_id"]
    assert rid.startswith("testhost-")
    assert p.record(rid)["remote_job_id"] == reply["job_id"]
    far_manifest = json.loads((p.far.state / "jobs" / reply["job_id"] /
                               "manifest.json").read_text())
    assert far_manifest["request_id"] == rid
    p.far.wait_outcome(reply["job_id"])


def test_human_remote_submit_keeps_the_job_id_last(qpair):
    p = qpair
    args = ["--host", "far", "submit", "--case", "t", "--timeout-min", "1",
            "--reserve-gb", "0.5", "--cwd", str(p.far.projects["alpha"]),
            "--request-id", "human-1", "--", PY, str(WORKLOAD), "--out",
            str(p.far.root / "h.jsonl")]
    r = p.acs(*args)
    assert r.returncode == 0, r.stdout + r.stderr
    job = r.stdout.strip().splitlines()[-1]
    assert job.startswith("acs-") and p.record("human-1")["remote_job_id"] \
        == job
    assert "placement record" in r.stderr
    p.far.wait_outcome(job)


def test_a_refused_remote_submit_is_recorded_refused(qpair):
    p = qpair
    (p.far.state / "blocks").mkdir(parents=True, exist_ok=True)
    (p.far.state / "blocks" / "far.json").write_text("{trunc")
    r = p.submit(request_id="ref-1")
    assert r.returncode == 2, r.stdout + r.stderr
    assert json.loads(r.stdout)["accepted"] is False
    rec = p.record("ref-1")
    assert rec["phase"] == "refused" and rec["remote_job_id"] is None
    assert rec["replies"][-1]["rc"] == 2
    # a usage error on the far side (no reply object) is refused too
    (p.far.state / "blocks" / "far.json").unlink()
    r = p.submit("--threads", "99", request_id="ref-2")
    assert r.returncode == 2
    assert p.record("ref-2")["phase"] == "refused"


def test_a_dropped_reply_after_acceptance_is_recorded_reply_lost(qpair):
    p = qpair
    r = p.submit(request_id="drop-1", modes={"far-target": "drop-after"})
    assert r.returncode == 255
    assert "may have accepted" in r.stderr
    rec = p.record("drop-1")
    assert rec["phase"] == "reply-lost" and rec["remote_job_id"] is None
    last = rec["replies"][-1]
    assert last["rc"] == 255 and "closed by remote host" in last["stderr"]
    assert last["stdout"] == "" and last["rc_source"] == "transport"
    # the far side did accept it
    [job] = [j for j in os.listdir(p.far.state / "jobs")]
    # the request id is never sent to another host
    p2 = p.acs("--host", "testhost", "submit", "--json", "--case", "t",
               "--timeout-min", "1", "--reserve-gb", "0.5", "--cwd",
               str(p.near.projects["alpha"]), "--request-id", "drop-1", "--",
               "true")
    assert p2.returncode == 2 and json.loads(p2.stdout)["accepted"] is False
    assert "far" in json.loads(p2.stdout)["reason"]
    assert not (p.near.state / "jobs").exists() or \
        not list((p.near.state / "jobs").iterdir())
    # G3b: `hosts` resolves it by request id on far: found, accepted
    report = hosts(p)
    [resolved] = report["resolutions"]
    assert (resolved["before"], resolved["after"], resolved["job_id"]) == \
        ("reply-lost", "accepted", job)
    assert not report["placements_open"]
    rec = p.record("drop-1")
    assert rec["phase"] == "accepted" and rec["remote_job_id"] == job
    assert rec["resolution"]["found"] is True
    assert rec["replies"][-1]["kind"] == "resolution"
    # a re-send to the same host names the job; nothing is sent
    calls = p.calls()
    again = p.submit(request_id="drop-1")
    assert again.returncode == 2, again.stdout + again.stderr
    assert json.loads(again.stdout)["job_id"] == job
    assert p.calls() == calls
    assert len(p.record("drop-1")["replies"]) == 2
    p.far.wait_outcome(job)


def test_rc_0_without_a_json_reply_is_reply_lost(pair):
    r = pair.submit(request_id="nj-1", modes={"far-target": "nojson"})
    rec = pair.record("nj-1")
    assert rec["phase"] == "reply-lost"
    assert rec["replies"][-1]["rc"] == 0
    assert "Welcome" in rec["replies"][-1]["stdout"]
    assert r.returncode != 0


def test_a_timed_out_submit_is_reply_lost():
    p = Pair(call_timeout_s=2)
    try:
        r = p.submit(request_id="to-1", modes={"far-target": "timeout"})
        assert r.returncode == 75
        rec = p.record("to-1")
        assert rec["phase"] == "reply-lost"
        assert rec["replies"][-1]["timed_out"] is True
    finally:
        p.cleanup()


def test_an_unreachable_destination_is_reply_lost_and_nothing_runs_here(
        pair):
    r = pair.submit(request_id="un-1", modes={"far-target": "unreachable"})
    assert r.returncode == 255
    assert pair.record("un-1")["phase"] == "reply-lost"
    assert not (pair.near.state / "jobs").exists()


def test_the_record_keeps_env_names_not_values(qpair):
    p = qpair
    r = p.submit("--env", "ACS_TEST_PASS=s3cr3t-value", request_id="env-1")
    assert r.returncode == 0, r.stdout + r.stderr
    text = (p.near.state / "placements" / "env-1.json").read_text()
    assert "s3cr3t-value" not in text
    assert "ACS_TEST_PASS=<redacted>" in json.loads(text)["argv"]
    calls = p.calls()
    bad = p.submit("--env", "API_TOKEN=x", request_id="env-2")
    assert bad.returncode == 2 and "credentials" in bad.stderr
    assert p.calls() == calls                       # nothing sent
    assert not (p.near.state / "placements" / "env-2.json").exists()
    p.far.wait_outcome(json.loads(r.stdout)["job_id"])


def test_no_call_when_the_record_cannot_be_written(pair):
    pair.near.state.mkdir(parents=True, exist_ok=True)
    (pair.near.state / "placements").write_text("a file, not a directory")
    r = pair.submit(request_id="nw-1")
    assert r.returncode == 2
    assert "placement record" in r.stderr and "nothing was sent" in r.stderr
    assert pair.calls() == 0


def test_status_all_lists_the_placement_records(qpair):
    p = qpair
    ok = p.submit(request_id="list-1")
    lost = p.submit(request_id="list-2", modes={"far-target": "unreachable"})
    assert ok.returncode == 0 and lost.returncode == 255
    listing = json.loads(p.acs("status", "--all", "--json").stdout)
    phases = {r["request_id"]: r["phase"] for r in listing["placements"]}
    assert phases == {"list-1": "accepted", "list-2": "reply-lost"}
    assert all(r["host"] == "far" and r["pinned"] for r in
               listing["placements"])
    default = json.loads(p.acs("status", "--json").stdout)
    assert [r["request_id"] for r in default["placements"]] == ["list-2"]
    p.far.wait_outcome(json.loads(ok.stdout)["job_id"])


# --- status --request-id ---------------------------------------------------------------------

def test_status_by_request_id_on_a_host(env_direct):
    env = env_direct
    job = env.submit(workload_args=["--out", str(env.root / "w.jsonl")],
                     extra=["--request-id", "rid-1"])
    env.wait_outcome(job["job_id"])
    r = env.acs("status", "--request-id", "rid-1", "--json")
    assert r.returncode == 0, r.stderr
    found = json.loads(r.stdout)
    assert found["found"] is True and found["binding"] == job["job_id"]
    assert [j["job_id"] for j in found["jobs"]] == [job["job_id"]]
    assert found["jobs"][0]["outcome"] == "succeeded"
    # C1: the binding alone is not proof; the job records are scanned too
    (env.state / "requests" / "rid-1.json").unlink()
    found = json.loads(env.acs("status", "--request-id", "rid-1",
                               "--json").stdout)
    assert found["binding"] is None and found["found"] is True
    assert [j["job_id"] for j in found["jobs"]] == [job["job_id"]]
    none = json.loads(env.acs("status", "--request-id", "rid-x",
                              "--json").stdout)
    assert none["found"] is False and none["jobs"] == []
    assert env.acs("status", job["job_id"], "--request-id",
                   "rid-1").returncode == 2


# --- routing by placement record -------------------------------------------------------------

def test_status_wait_cancel_route_by_job_id_and_request_id(qpair):
    p = qpair
    a = json.loads(p.submit(request_id="route-a", name="ra",
                            sleep=2).stdout)
    b = json.loads(p.submit(request_id="route-b", name="rb",
                            sleep=60).stdout)
    for ident in (a["job_id"], "route-a"):
        r = p.acs("status", ident, "--json")
        assert r.returncode == 0, r.stdout + r.stderr
        view = json.loads(r.stdout)
        assert view["job_id"] == a["job_id"] and view["host"] == "far"
        assert view["route"]["host"] == "far"
        assert view["route"]["ssh"] == "far-target"
        assert view["route"]["request_id"] == "route-a"
        assert view["evidence"]["state"].startswith(str(p.far.state))
    w = p.acs("wait", "route-a", "--timeout", "90", "--json")
    assert w.returncode == 0, w.stdout + w.stderr
    assert json.loads(w.stdout)["outcome"] == "succeeded"
    c = p.acs("cancel", b["job_id"], "--json")
    assert c.returncode == 0, c.stdout + c.stderr
    assert json.loads(c.stdout)["cancelled"] is True
    assert p.far.state_of(b["job_id"])["outcome"] == "cancelled"
    rc = p.acs("reconcile", "--terminate", a["job_id"], "--json")
    assert rc.returncode == 0, rc.stdout + rc.stderr
    report = json.loads(rc.stdout)
    assert report["host"] == "far" and report["route"]["host"] == "far"
    # all of it happened on the far side
    assert not (p.near.state / "jobs").exists()


def test_a_reply_lost_request_is_found_by_request_id(qpair):
    p = qpair
    p.submit(request_id="lost-2", modes={"far-target": "drop-after"})
    r = p.acs("status", "lost-2", "--json")
    assert r.returncode == 0, r.stdout + r.stderr
    view = json.loads(r.stdout)
    # G3b: status resolved it by request id first (found: accepted), then
    # routed by the job id it now knows
    rec = p.record("lost-2")
    assert rec["phase"] == "accepted"
    assert view["job_id"] == rec["remote_job_id"]
    assert view["route"]["host"] == "far"
    assert "reply-lost -> accepted" in r.stderr
    w = p.acs("wait", "lost-2", "--timeout", "90", "--json")
    assert w.returncode == 0, w.stdout + w.stderr
    assert json.loads(w.stdout)["job_id"] == rec["remote_job_id"]


def test_routing_never_falls_back_to_this_host(qpair):
    p = qpair
    a = json.loads(p.submit(request_id="nf-1").stdout)
    r = p.acs("status", a["job_id"], "--json",
              modes={"far-target": "unreachable"})
    assert r.returncode == 255
    assert "nothing was run locally" in r.stderr
    p.far.wait_outcome(a["job_id"])


def test_an_unknown_job_stays_local(pair):
    r = pair.acs("wait", "acs-20261008T000000Z-000000", "--json")
    assert r.returncode == 2 and "unknown job" in r.stderr
    assert pair.calls() == 0


# --- remote evidence: log, fetch, admission memory ------------------------------------------

def test_log_and_fetch_over_the_route(qpair, tmp_path):
    p = qpair
    reply = json.loads(p.submit(request_id="ev-1").stdout)
    job = reply["job_id"]
    p.far.wait_outcome(job)
    far_log = (p.far.state / "jobs" / job / "output.log").read_text()
    r = p.acs("--host", "far", "log", job, "--tail", "3")
    assert r.returncode == 0, r.stderr
    assert r.stdout.splitlines() == far_log.splitlines()[-3:]
    routed = p.acs("log", job)
    assert routed.returncode == 0 and routed.stdout == far_log
    dest = tmp_path / "fetched"
    f = p.acs("--host", "far", "fetch", job, str(dest), "--json")
    assert f.returncode == 0, f.stdout + f.stderr
    report = json.loads(f.stdout)
    root = dest / job
    names = {str(Path(x["path"])) for x in report["files"]}
    assert {"manifest.json", "state.json", "output.log"} <= names
    assert any(n.startswith("guard/attempt-1/") and n.endswith("guard.json")
               for n in names)
    for name in names:
        assert (root / name).read_bytes() == \
            (p.far.state / "jobs" / job / name).read_bytes()
    # routed by the placement record, into another directory
    g = p.acs("fetch", "ev-1", str(tmp_path / "again"), "--json")
    assert g.returncode == 0, g.stdout + g.stderr
    assert (tmp_path / "again" / job / "state.json").exists()
    # never clobbers
    assert p.acs("fetch", job, str(dest)).returncode != 0


def test_local_log_and_fetch(env_direct, tmp_path):
    env = env_direct
    job = env.submit(workload_args=["--out", str(env.root / "w.jsonl")])
    env.wait_outcome(job["job_id"])
    r = env.acs("log", job["job_id"], "--tail", "1")
    assert r.returncode == 0 and len(r.stdout.splitlines()) <= 1
    f = env.acs("fetch", job["job_id"], str(tmp_path / "x"))
    assert f.returncode == 0, f.stderr
    assert (tmp_path / "x" / job["job_id"] / "manifest.json").exists()
    assert env.acs("log", "acs-20261008T000000Z-000000").returncode != 0


def test_status_shows_the_admission_memory_of_the_last_attempt(env_direct):
    env = env_direct
    job = env.submit(workload_args=["--out", str(env.root / "w.jsonl")])
    env.wait_outcome(job["job_id"])
    view = json.loads(env.acs("status", job["job_id"], "--json").stdout)
    memory = view["admission_memory"]
    assert memory["attempt"] == 1
    assert memory["host_available_gb"] == pytest.approx(64.0, rel=0.01)
    assert {"floor_gb", "estimate_gb", "needed_gb"} <= set(memory)
    assert memory["path"].endswith("attempt-1/guard.json")


# --- the G3a boundary: no automatic choice ---------------------------------------------------

BOUNDARY = ('[hosts.far]\ntransport = "ssh"\nssh = "far-target"\n'
            'agent_compute = "/opt/acs/bin/agent-compute"\n'
            '[projects.alpha.on.far]\ncwd = "/srv/alpha"\n'
            'container = "alpha-box"\ncontainer_workdir = "/work"\n')


@pytest.mark.parametrize("command", ["submit", "run"])
def test_declared_hosts_without_host_are_placed(command):
    """G3b: what G3a refused ("choose one with --host") is placed: the
    reply lists every host with its status (here a tie at 0 jobs: the
    local host); a project declaring nothing runs here as before."""
    p = Pair()
    try:
        p.near.config.write_text(
            _with_hosts(p.near.config.read_text(), '["testhost", "far"]')
            + f'[projects.alpha.on.far]\ncwd = "{p.far.projects["alpha"]}"\n')
        # both checkouts at one commit (the code requirement)
        git(p.far.projects["alpha"], "fetch", "-q",
            str(p.near.projects["alpha"]), "main")
        git(p.far.projects["alpha"], "reset", "-q", "--hard", "FETCH_HEAD")
        r = p.acs(command, "--json", "--case", "t", "--timeout-min", "1",
                  "--reserve-gb", "0.5", "--cwd",
                  str(p.near.projects["alpha"]), "--", PY, str(WORKLOAD),
                  "--out", str(p.near.root / "placed.jsonl"))
        assert r.returncode == 0, r.stdout + r.stderr
        reply = json.loads(r.stdout)
        job = reply["job_id"]
        if command == "submit":
            placement = reply["placement"]
            assert placement["chosen"] == "testhost"
            assert [(c["host"], c["status"]) for c in
                    placement["candidates"]] == [("testhost", "chosen"),
                                                 ("far", "busy")]
            p.near.wait_outcome(job)
        else:
            assert reply["outcome"] == "succeeded"
        assert p.record(reply["request_id"] if command == "run" else
                        placement["request_id"])["remote_job_id"] == job
        # beta declares nothing: it runs here as before, nothing is asked
        calls = p.calls()
        beta = p.acs("submit", "--json", "--case", "t", "--timeout-min",
                     "1", "--reserve-gb", "0.5", "--cwd",
                     str(p.near.projects["beta"]), "--", "true")
        assert beta.returncode == 0, beta.stdout + beta.stderr
        assert "placement" not in json.loads(beta.stdout)
        assert p.calls() == calls
        p.near.wait_outcome(json.loads(beta.stdout)["job_id"])
    finally:
        p.cleanup()


def test_a_project_whose_only_host_is_this_one_runs_here():
    env = Env(queue=False, extra_config=BOUNDARY.split("[projects.")[0])
    try:
        env.config.write_text(_with_hosts(env.config.read_text(),
                                          '["testhost"]'))
        job = env.submit(workload_args=["--out", str(env.root / "w.jsonl")])
        assert job["accepted"] is True
        env.wait_outcome(job["job_id"])
    finally:
        env.cleanup()


def test_a_project_not_declared_on_this_host_is_refused_here():
    env = Env(queue=False, extra_config=BOUNDARY)
    try:
        env.config.write_text(_with_hosts(env.config.read_text(),
                                          '["far"]'))
        for args in ((), ("--host", "testhost")):
            r = env.acs(*args, "submit", "--json", "--case", "t",
                        "--timeout-min", "1", "--cwd",
                        str(env.projects["alpha"]), "--", "true")
            assert r.returncode == 2, r.stdout + r.stderr
            refusal = json.loads(r.stdout)
            assert refusal["accepted"] is False
            assert [c["host"] for c in refusal["candidates"]] == ["far"]
        assert not (env.state / "jobs").exists()
    finally:
        env.cleanup()


def test_a_pinned_remote_submit_for_a_project_not_declared_there_is_refused(
        pair):
    pair.near.config.write_text(_with_hosts(pair.near.config.read_text(),
                                            '["testhost"]'))
    r = pair.submit("--project", "alpha", request_id="nd-1")
    assert r.returncode == 2
    assert "far" in r.stderr and "hosts" in r.stderr
    assert pair.calls() == 0


def test_hosts_submit_and_routing_over_the_ssh_wsl_transport():
    p = Pair(far_queue=True, transport="ssh-wsl")
    try:
        far = entry(hosts(p), "far")
        assert far["state"] == "ok" and far["route"]["distro"] == "Ubuntu"
        r = p.submit(request_id="wsl-1", name="wsl")
        assert r.returncode == 0, r.stdout + r.stderr
        job = json.loads(r.stdout)["job_id"]
        assert p.record("wsl-1")["route"]["transport"] == "ssh-wsl"
        w = p.acs("wait", "wsl-1", "--timeout", "90", "--json")
        assert w.returncode == 0, w.stdout + w.stderr
        assert json.loads(w.stdout)["route"]["transport"] == "ssh-wsl"
        assert "wsl -d Ubuntu" in p.ssh_log.read_text()
        p.far.wait_outcome(job)
    finally:
        p.cleanup()


# --- the exit code in band (found by the live route test, 2026-10-08) -------------------

def _marker_of(argv) -> str:
    """The exit-code marker the remote script of ``argv`` prints."""
    import base64
    import re
    script = base64.b64decode(argv[-1].split()[1]).decode()
    return re.search(r"__ACS_REMOTE_RC_[0-9a-f]+__=", script).group(0)


def test_the_remote_exit_code_travels_in_band(routed, monkeypatch):
    monkeypatch.setattr(remote, "_run", lambda argv, timeout: (
        1, b'{"accepted": false}',
        f"why\n\n{_marker_of(argv)}2\n".encode(), False))
    reply = remote.call(routed, "far", ["submit"])
    assert (reply.rc, reply.transport_rc, reply.rc_source) == (2, 1,
                                                               "remote")
    assert reply.stderr == "why\n"
    monkeypatch.setattr(remote, "_run", lambda argv, timeout: (
        255, b"", b"Connection closed\n", False))
    reply = remote.call(routed, "far", ["submit"])
    assert (reply.rc, reply.transport_rc, reply.rc_source) == (
        255, 255, "transport")


def test_the_remote_script_reports_its_exit_code(routed):
    import base64
    argv = remote.ssh_argv(routed.remotes["far"], ["status"], "0a1b2c")
    script = base64.b64decode(argv[-1].split()[1]).decode()
    result = subprocess.run(["bash", "-c", script.replace(
        "/opt/acs/bin/agent-compute", "false")], capture_output=True,
        text=True)
    assert result.returncode == 1
    assert result.stderr.endswith(f"{remote.marker('0a1b2c')}1\n")


def test_exit_codes_survive_a_route_that_collapses_them(qpair):
    p = qpair
    modes = {"far-target": "collapse"}
    a = json.loads(p.submit(request_id="col-1", name="col", sleep=30,
                            modes=modes).stdout)
    # a wait timeout is 75 there, not the route's 1
    w = p.acs("--host", "far", "wait", a["job_id"], "--timeout", "1",
              "--json", modes=modes)
    assert w.returncode == 75, w.stdout + w.stderr
    assert "__ACS_REMOTE_RC__" not in w.stderr
    # a refusal (exit 2 with its reply object) is recorded refused
    (p.far.state / "blocks").mkdir(parents=True, exist_ok=True)
    (p.far.state / "blocks" / "far.json").write_text("{trunc")
    r = p.submit(request_id="col-2", modes=modes)
    assert r.returncode == 2, r.stdout + r.stderr
    rec = p.record("col-2")
    assert rec["phase"] == "refused"
    assert (rec["replies"][-1]["rc"], rec["replies"][-1]["transport_rc"]) \
        == (2, 1)
    (p.far.state / "blocks" / "far.json").unlink()
    assert p.acs("cancel", a["job_id"], modes=modes).returncode == 0
    p.far.wait_outcome(a["job_id"])


def test_an_old_remote_behind_a_collapsing_route_is_recognised():
    p = Pair(routes=(("far", "far-target"), ("old", "old-target")),
             old_routes=("old",))
    try:
        report = hosts(p, modes={"old-target": "collapse"})
        old = entry(report, "old")
        assert old["state"] == "old-remote" and old["eligible"] is False
        assert old["raw"]["rc"] == 2 and old["raw"]["transport_rc"] == 1
        assert old["raw"]["stderr"] == OLD_REMOTE_STDERR
        assert entry(report, "far")["state"] == "ok"
    finally:
        p.cleanup()



# --- review round 1 (independent review of 4877d22) ---------------------------------------

def test_a_forged_or_unanchored_exit_code_trailer_is_not_taken(routed,
                                                               monkeypatch):
    """Finding 5: the trailer counts only at the start of a line and with
    this call's own nonce; a workload's text never sets the exit code."""
    def reply(stderr_of):
        monkeypatch.setattr(remote, "_run", lambda argv, timeout: (
            255, b"", stderr_of(argv).encode(), False))
        return remote.call(routed, "far", ["status"])
    # not at the start of a line
    r = reply(lambda argv: f"workload said {_marker_of(argv)}0\n")
    assert (r.rc, r.rc_source) == (255, "transport")
    # another call's nonce (or the old fixed marker)
    for forged in ("__ACS_REMOTE_RC_00ff00ff00ff00ff__=0\n",
                   "__ACS_REMOTE_RC__=0\n"):
        r = reply(lambda argv: forged)
        assert (r.rc, r.rc_source) == (255, "transport"), forged
    # the real one, on its own line
    # (the script prints a newline before it: removed with it)
    r = reply(lambda argv: f"x\n\n{_marker_of(argv)}0\n")
    assert (r.rc, r.rc_source, r.stderr) == (0, "remote", "x\n")
    # every call gets its own nonce
    seen = []
    monkeypatch.setattr(remote, "_run", lambda argv, timeout: (
        seen.append(_marker_of(argv)) or (0, b"", b"", False)))
    remote.call(routed, "far", ["status"])
    remote.call(routed, "far", ["status"])
    assert len(set(seen)) == 2


def test_an_unreadable_placement_record_binds_its_request_id_everywhere(
        pair):
    """Finding 1: C4 for placement records: a corrupted record binds its
    request id on every host; nothing is sent, nothing is overwritten."""
    placements = pair.near.state / "placements"
    placements.mkdir(parents=True)
    bad = placements / "bad-1.json"
    for content in ("", '{"request_id": "bad-1", "host": "fa'):
        bad.write_text(content)
        before = snapshot(pair.near.state)
        local = pair.acs("submit", "--json", "--case", "t", "--timeout-min",
                         "1", "--reserve-gb", "0.5", "--cwd",
                         str(pair.near.projects["alpha"]), "--request-id",
                         "bad-1", "--", "true")
        assert local.returncode == 2, local.stdout + local.stderr
        assert json.loads(local.stdout)["accepted"] is False
        assert "unreadable" in json.loads(local.stdout)["reason"]
        remote_r = pair.submit(request_id="bad-1")
        assert remote_r.returncode == 2, remote_r.stdout + remote_r.stderr
        assert "unreadable" in remote_r.stderr
        assert pair.calls() == 0
        assert bad.read_text() == content
        status = pair.acs("status", "bad-1", "--json")
        assert "unreadable" in status.stderr and "bad-1" in status.stderr
        assert pair.calls() == 0
        assert snapshot(pair.near.state) == before


def test_a_request_id_bound_here_is_never_sent_to_another_host(qpair):
    """Finding 2: a request id this host bound to a job that may have run
    (or whose binding cannot be read) is refused for any other host:
    nothing sent, no record; one that provably never started may go."""
    p = qpair
    job = p.near.submit(workload_args=["--out",
                                       str(p.near.root / "here.jsonl")],
                        extra=["--request-id", "dup-1"])
    r = p.submit(request_id="dup-1")
    assert r.returncode == 2, r.stdout + r.stderr
    refusal = json.loads(r.stdout)
    assert refusal["accepted"] is False and job["job_id"] in \
        refusal["reason"]
    assert p.calls() == 0
    assert not (p.near.state / "placements" / "dup-1.json").exists()
    p.near.wait_outcome(job["job_id"])
    # still refused after it ended: it ran
    assert p.submit(request_id="dup-1").returncode == 2
    assert p.calls() == 0
    # an unreadable binding: refused (C4)
    (p.near.state / "requests" / "dup-2.json").write_text("{trunc")
    r = p.submit(request_id="dup-2")
    assert r.returncode == 2 and "unreadable" in r.stdout
    assert p.calls() == 0
    # a binding to a job that never started may be placed elsewhere
    never = "acs-20261008T000000Z-0e0e0e"
    write_json(p.near.state / "jobs" / never / "state.json",
               {"job_id": never, "host": "testhost", "phase": "done",
                "outcome": "refused", "request_id": "dup-3",
                "history": []})
    write_json(p.near.state / "requests" / "dup-3.json",
               {"request_id": "dup-3", "job_id": never})
    r = p.submit(request_id="dup-3", name="dup3")
    assert r.returncode == 0, r.stdout + r.stderr
    p.far.wait_outcome(json.loads(r.stdout)["job_id"])


def test_reserve_refuses_a_request_id_a_record_binds_elsewhere(env_direct):
    """Finding 8: the local submit's check of the placement record is made
    under the state lock, in the reservation itself."""
    from agent_compute import cli
    from agent_compute.state import JobStore
    env = env_direct
    write_json(env.state / "placements" / "elsewhere-1.json",
               {"request_id": "elsewhere-1", "host": "far",
                "phase": "sent"})
    store = JobStore(env.state)
    job_id, existing = cli._reserve(store, "testhost", "elsewhere-1")
    assert job_id == "" and existing is not None
    assert not (env.state / "jobs").exists() or \
        not list((env.state / "jobs").iterdir())
    assert store.request_job("elsewhere-1") is None


def test_abbreviated_options_are_refused(pair):
    """Finding 4: `--en NAME=V` is not `--env` (it would escape the
    redaction of the placement record)."""
    from agent_compute import cli
    with pytest.raises(SystemExit):
        cli.parser().parse_args(["submit", "--case", "c", "--timeout-min",
                                 "1", "--en", "A=1"])
    r = pair.submit("--en", "ACS_TEST_PASS=s3cr3t", request_id="abbr-1")
    assert r.returncode == 2
    assert pair.calls() == 0
    assert not (pair.near.state / "placements" / "abbr-1.json").exists()


def test_a_pinned_remote_run_writes_a_placement_record(qpair):
    """Finding 3: `--host X run` records the job like `submit` (written
    before the call; the job's end in the reply)."""
    p = qpair
    seen = p.near.root / "seen-run.txt"
    r = p.acs("--host", "far", "run", "--json", "--case", "t",
              "--timeout-min", "1", "--reserve-gb", "0.5", "--cwd",
              str(p.far.projects["alpha"]), "--request-id", "run-1", "--",
              PY, str(WORKLOAD), "--out", str(p.far.root / "run1.jsonl"),
              FAKE_SSH_SNAPSHOT=str(seen),
              FAKE_SSH_SNAPSHOT_SRC=str(p.near.state / "placements"))
    assert r.returncode == 0, r.stdout + r.stderr
    view = json.loads(r.stdout)
    assert view["outcome"] == "succeeded"
    assert json.loads(seen.read_text())["phase"] == "sent"
    rec = p.record("run-1")
    assert rec["phase"] == "accepted" and rec["pinned"] is True
    assert rec["remote_job_id"] == view["job_id"]
    assert rec["command"] == "run"
    assert view["placement"]["request_id"] == "run-1"
    # without a request id one is generated, and recorded
    r = p.acs("--host", "far", "run", "--json", "--case", "t",
              "--timeout-min", "1", "--reserve-gb", "0.5", "--cwd",
              str(p.far.projects["alpha"]), "--", PY, str(WORKLOAD),
              "--out", str(p.far.root / "run2.jsonl"))
    assert r.returncode == 0, r.stdout + r.stderr
    rid = json.loads(r.stdout)["placement"]["request_id"]
    assert p.record(rid)["remote_job_id"] == json.loads(r.stdout)["job_id"]
    # a refused run is recorded refused
    (p.far.state / "blocks").mkdir(parents=True, exist_ok=True)
    (p.far.state / "blocks" / "far.json").write_text("{trunc")
    r = p.acs("--host", "far", "run", "--json", "--case", "t",
              "--timeout-min", "1", "--reserve-gb", "0.5", "--cwd",
              str(p.far.projects["alpha"]), "--request-id", "run-3", "--",
              "true")
    assert r.returncode == 2, r.stdout + r.stderr
    assert p.record("run-3")["phase"] == "refused"


def test_fetch_checks_the_job_id_here_first(pair, tmp_path):
    """Finding 10: `--host X fetch` validates the job id before the call
    (it names the directory written here)."""
    for bad in ("../acs-x", "acs-a/b", "not-a-job"):
        r = pair.acs("--host", "far", "fetch", bad, str(tmp_path / "d"))
        assert r.returncode == 2, (bad, r.stdout + r.stderr)
    assert pair.calls() == 0
    assert not (tmp_path / "d").exists()


def test_the_boundary_is_checked_again_after_the_reservation(monkeypatch,
                                                             capsys):
    """Finding 7: a first project resolution that failed (the early check
    then lets it through) cannot skip the boundary: accept checks the
    project it resolved, releases the reservation and refuses."""
    from agent_compute import cli
    env = Env(queue=False, extra_config=BOUNDARY)
    try:
        env.config.write_text(_with_hosts(env.config.read_text(),
                                          '["testhost", "far"]'))
        monkeypatch.setattr(cli, "placement_refusal",
                            lambda config, args, pinned_here=False: None)
        # G3b: nor is it placed (the early resolution failed)
        monkeypatch.setattr(cli, "placed_project", lambda config, args: None)
        rc = cli.main(["--config", str(env.config), "submit", "--json",
                       "--case", "t", "--timeout-min", "1", "--reserve-gb",
                       "0.5", "--cwd", str(env.projects["alpha"]),
                       "--request-id", "late-1", "--", "true"])
        refusal = json.loads(capsys.readouterr().out)
        assert rc == 2 and refusal["accepted"] is False
        assert [c["host"] for c in refusal["candidates"]] == ["testhost",
                                                              "far"]
        assert not list((env.state / "jobs").iterdir())
        assert not (env.state / "requests" / "late-1.json").exists()
    finally:
        env.cleanup()


# --- review round 2 (independent review of 07f8b8e) ---------------------------------------

def test_a_pinned_submit_and_run_reach_a_destination_that_declares_several_hosts():
    """R2-1: the pin travels with the command.  The destination's own
    config lists the project on two hosts; a pinned `--host far submit`
    (and `run`) is its own pinned submit there, not an unpinned one that
    its G3a boundary refuses ("choose one with --host")."""
    p = Pair(far_queue=True)
    try:
        text = _with_hosts(p.far.config.read_text(), '["far", "back"]')
        p.far.config.write_text(
            text + '[hosts.back]\ntransport = "ssh"\nssh = "back-target"\n'
                   'agent_compute = "/opt/acs/bin/agent-compute"\n')
        # (G3a's check that an unpinned submit on far itself is refused is
        # gone: G3b places it, tests/test_g3b_placement.py)
        r = p.submit(request_id="sym-1", name="sym1")
        assert r.returncode == 0, r.stdout + r.stderr
        reply = json.loads(r.stdout)
        assert reply["accepted"] is True and reply["host"] == "far"
        assert p.record("sym-1")["phase"] == "accepted"
        p.far.wait_outcome(reply["job_id"])
        run = p.acs("--host", "far", "run", "--json", "--case", "t",
                    "--timeout-min", "1", "--reserve-gb", "0.5", "--cwd",
                    str(p.far.projects["alpha"]), "--request-id", "sym-2",
                    "--", PY, str(WORKLOAD), "--out",
                    str(p.far.root / "sym2.jsonl"))
        assert run.returncode == 0, run.stdout + run.stderr
        assert json.loads(run.stdout)["outcome"] == "succeeded"
        assert p.record("sym-2")["phase"] == "accepted"
    finally:
        p.cleanup()


def test_the_binding_is_checked_again_under_the_lock(pair, monkeypatch,
                                                     capsys):
    """R2-3: a request id bound here between the read-only pre-check and
    the record's commit (a concurrent local submit) is still refused: the
    guard runs under the state lock with the commit; nothing is sent."""
    from agent_compute import cli
    from agent_compute import placement as plmod
    job = "acs-20261008T000000Z-c0ffee"
    real_commit = plmod.commit

    def racing_commit(state_dir, request_id, host, **kw):
        # the concurrent local submit wins the race right here
        write_json(pair.near.state / "jobs" / job / "state.json",
                   {"job_id": job, "host": "testhost", "phase": "executing",
                    "request_id": request_id, "history": []})
        write_json(pair.near.state / "requests" / f"{request_id}.json",
                   {"request_id": request_id, "job_id": job})
        return real_commit(state_dir, request_id, host, **kw)
    calls = []
    monkeypatch.setattr(plmod, "commit", racing_commit)
    monkeypatch.setattr(cli.remote_mod, "call",
                        lambda *a, **k: calls.append(a) or None)
    rc = cli.main(["--config", str(pair.near.config), "--host", "far",
                   "submit", "--json", "--case", "t", "--timeout-min", "1",
                   "--cwd", str(pair.far.projects["alpha"]), "--request-id",
                   "race-1", "--", "true"])
    refusal = json.loads(capsys.readouterr().out)
    assert rc == 2 and refusal["accepted"] is False
    assert job in refusal["reason"]
    assert calls == []
    assert not (pair.near.state / "placements" / "race-1.json").exists()


def test_the_forwarded_pin_does_not_hide_the_command(routed):
    """R2-1's forwarded `--host X` comes before the subcommand: the client's
    timeouts and its A11 wording still see `run`, `wait` and `submit`."""
    far = routed.remotes["far"]                       # call_timeout_s 30
    assert remote.command_of(["--host", "far", "run", "--case", "c"]) == "run"
    assert remote.call_timeout(far, ["--host", "far", "run", "--case",
                                     "c"]) is None
    assert remote.call_timeout(far, ["--host", "far", "wait", "acs-x",
                                     "--timeout", "5"]) == 35.0
    assert remote.call_timeout(far, ["--host", "far", "submit"]) == 30.0
