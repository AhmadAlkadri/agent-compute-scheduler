"""Remote `--host` invocation.

A11: ssh rc 255 on a remote submit/run used to print "nothing was run
locally", which invites a duplicate remote submit although the remote may
have accepted the job before the connection dropped.
"""

import pytest

from agent_compute import remote
from agent_compute.config import load

REMOTE_CONFIG = ('[hosts.fake]\ntransport = "ssh"\nssh = "fake-target"\n'
                 'agent_compute = "/nonexistent/agent-compute"\n')


@pytest.fixture
def local():
    from conftest import Env
    env = Env(queue=False, extra_config=REMOTE_CONFIG)
    yield env
    env.cleanup()


def _ssh_255(monkeypatch):
    # G3a: every remote call goes through remote._run (its own process
    # group, killed on timeout); the ssh client is faked there
    calls = []

    def fake_run(argv, timeout):
        calls.append(argv)
        return 255, b"", b"", False
    monkeypatch.setattr(remote, "_run", fake_run)
    return calls


@pytest.mark.parametrize("cmd", ["submit", "run"])
def test_remote_submit_255_warns_it_may_have_been_accepted(local, monkeypatch,
                                                           capsys, cmd):
    calls = _ssh_255(monkeypatch)
    rc = remote.forward(load(local.config), "fake",
                        [cmd, "--case", "t", "--timeout-min", "1", "--",
                         "true"])
    assert rc == 255 and calls
    err = capsys.readouterr().err
    assert "may have accepted" in err, err
    assert "agent-compute --host fake status" in err
    assert "--request-id" in err
    assert "nothing was run locally" not in err


def test_remote_status_255_says_nothing_ran_locally(local, monkeypatch,
                                                    capsys):
    _ssh_255(monkeypatch)
    rc = remote.forward(load(local.config), "fake", ["status", "--json"])
    assert rc == 255
    err = capsys.readouterr().err
    assert "nothing was run locally" in err
    assert "may have accepted" not in err


# --- A15: automated remote round trip through a fake ssh ------------------

FAKE_SSH = """#!/bin/bash
# fake ssh: `ssh -o BatchMode=yes TARGET INNER` runs INNER locally
echo "$*" >> "$FAKE_SSH_LOG"
while [ "$1" = "-o" ]; do shift 2; done
target="$1"; inner="$2"
if [ "$FAKE_SSH_MODE" = unreachable ]; then
    echo "ssh: connect to host $target port 22: Connection refused" >&2
    exit 255
fi
if [ "$FAKE_SSH_MODE" = drop-after ]; then
    # the remote runs it, and the connection drops before its reply
    # arrives (G3a: the exit-code trailer is lost with it)
    bash -c "$inner" > /dev/null 2>&1
    echo "Connection to $target closed by remote host." >&2
    exit 255
fi
bash -c "$inner"; rc=$?
exit $rc
"""
FAKE_WSL = """#!/bin/bash
# fake wsl: `wsl -d DISTRO -- CMD...` runs CMD
while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do shift; done
shift
exec "$@"
"""


@pytest.fixture(params=["ssh", "ssh-wsl"])
def pair(request):
    """A local env whose `--host fake` reaches, through a fake ssh on PATH,
    a second isolated env (its own config, state, queue and guard home)."""
    import os
    import sys
    from conftest import BIN, Env
    far = Env(queue=True)
    far.config.write_text(far.config.read_text().replace(
        'host = "testhost"', 'host = "fake"'))
    fakebin = far.root / "fakebin"
    fakebin.mkdir()
    for name, text in (("ssh", FAKE_SSH), ("wsl", FAKE_WSL)):
        (fakebin / name).write_text(text)
        (fakebin / name).chmod(0o755)
    wrapper = far.root / "remote-agent-compute"
    wrapper.write_text(f'#!/bin/bash\nexec "{sys.executable}" "{BIN}" '
                       f'--config "{far.config}" "$@"\n')
    wrapper.chmod(0o755)
    distro = 'distro = "Ubuntu"\n' if request.param == "ssh-wsl" else ""
    near = Env(queue=False, extra_config=(
        f'[hosts.fake]\ntransport = "{request.param}"\nssh = "fake-target"\n'
        f'agent_compute = "{wrapper}"\n{distro}'))
    near.ssh_log = far.root / "ssh.log"

    def call(*args, mode="", timeout=180):
        env = near.env(PATH=f"{fakebin}{os.pathsep}{os.environ['PATH']}",
                       FAKE_SSH_LOG=str(near.ssh_log), FAKE_SSH_MODE=mode)
        return near.acs("--host", "fake", *args, env=env, timeout=timeout)
    near.remote = call
    yield near, far
    near.cleanup()
    far.cleanup()


def _submit(near, far, name, request_id, sleep, mode=""):
    import sys
    from conftest import WORKLOAD
    return near.remote("submit", "--json", "--case", "t", "--timeout-min",
                       "1", "--reserve-gb", "0.5", "--cwd",
                       str(far.projects["alpha"]), "--request-id", request_id,
                       "--", sys.executable, str(WORKLOAD), "--out",
                       str(far.root / f"{name}.jsonl"), "--sleep", str(sleep),
                       mode=mode)


def test_remote_round_trip(pair):
    import json
    near, far = pair
    r = _submit(near, far, "a", "rt-a", 3)
    assert r.returncode == 0, r.stdout + r.stderr
    a = json.loads(r.stdout)
    assert a["accepted"] is True and a["host"] == "fake"
    r = _submit(near, far, "b", "rt-b", 0)
    assert r.returncode == 0, r.stdout + r.stderr
    b = json.loads(r.stdout)
    view = json.loads(near.remote("status", b["job_id"], "--json").stdout)
    assert view["host"] == "fake" and view["job"]["job_id"] == b["job_id"]
    c = near.remote("cancel", b["job_id"], "--json")
    assert c.returncode == 0, c.stdout + c.stderr
    assert json.loads(c.stdout)["cancelled"] is True
    w = near.remote("wait", a["job_id"], "--timeout", "90", "--json")
    assert w.returncode == 0, w.stdout + w.stderr
    assert json.loads(w.stdout)["outcome"] == "succeeded"
    rc = near.remote("reconcile", "--json")
    assert rc.returncode == 0, rc.stdout + rc.stderr
    assert json.loads(rc.stdout)["blocked"] is False
    # everything happened on the far side, nothing on this host
    assert far.state_of(a["job_id"])["outcome"] == "succeeded"
    assert far.state_of(b["job_id"])["outcome"] == "cancelled"
    assert not (near.state / "jobs").exists() or \
        not list((near.state / "jobs").iterdir())
    assert "fake-target" in near.ssh_log.read_text()


def test_remote_255_after_acceptance(pair):
    import json
    near, far = pair
    r = _submit(near, far, "d", "rt-d", 0, mode="drop-after")
    assert r.returncode == 255
    assert "may have accepted" in r.stderr and "--request-id" in r.stderr
    jobs = json.loads(near.remote("status", "--all", "--json").stdout)["jobs"]
    [job] = [j for j in jobs if j["request_id"] == "rt-d"]
    again = _submit(near, far, "d2", "rt-d", 0)
    assert again.returncode == 2, again.stdout + again.stderr
    assert json.loads(again.stdout)["job_id"] == job["job_id"]
    w = near.remote("wait", job["job_id"], "--timeout", "90", "--json")
    assert w.returncode == 0, w.stdout + w.stderr


def test_remote_unreachable(pair):
    near, _ = pair
    r = near.remote("status", "--json", mode="unreachable")
    assert r.returncode == 255
    assert "nothing was run locally" in r.stderr
    r = _submit(near, pair[1], "e", "rt-e", 0, mode="unreachable")
    assert r.returncode == 255 and "may have accepted" in r.stderr
