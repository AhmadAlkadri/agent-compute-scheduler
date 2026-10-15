"""Explicit remote invocation of agent-compute on another execution host.

Admission must happen on the host that consumes the resources, so
``agent-compute --host lenovo ...`` runs the *remote* host's own
agent-compute (its config, its queue, its guard, its lock) over the existing
ssh route.  There is no network-facing scheduler API and no automatic host
selection here: the placement policy (campaign G slice G3b) chooses a
destination in ``cli``/``placement`` and sends once through this module;
if the route fails, the command fails — it never runs locally, and it is
never sent to another host.

Two modes (G3a):

* ``forward``: the remote's stdout and stderr are written through and its
  exit code returned (every forwarded command, as before);
* ``call``: capture mode, the reply ``(rc, stdout, stderr)`` returned
  verbatim (NULs stripped) for a record (``hosts``, placement records,
  routing by record).

Every remote call has a connect timeout and keepalives on the ssh client
(``-o ConnectTimeout``, ``ServerAliveInterval``/``ServerAliveCountMax``:
a half-open connection ends instead of hanging the CLI) and a client-side
call timeout, from the host's ``[hosts.<name>]`` entry:

* ``connect_timeout_s`` (default ``CONNECT_TIMEOUT_S``);
* ``call_timeout_s`` bounds every call to that host; without it, a probe
  (``_summary``) gets ``PROBE_TIMEOUT_S`` and any other call
  ``CALL_TIMEOUT_S`` (the slowest legitimate submit, the manifest's git and
  docker reads plus the enqueue, is about 7 min: ``cli.UNCLAIMED_GRACE_S``);
* a ``wait --timeout T`` gets ``T`` plus the call timeout; ``wait``
  without one and ``run`` (submit and wait) are not bounded on the client
  (the keepalives still end a dead connection).

A call that times out is killed with its own process group (the ssh
client and anything it started; never another process) and reported (exit
75): the remote command may have completed.  That timeout bounds the
client only: the remote command may run on after it (G3b review round 1).

A gated call (``on_start``, G3b: a submit or run that a placement record
binds) starts the ssh client as a shell that waits for a go on its stdin
and then execs ssh (same pid, same start time); ``on_start(pid)`` records
that start-time-proven identity first, and only then is the go written.
If it cannot be recorded, the shell exits without running ssh: nothing is
sent (``Reply.sent`` False).  So a sender that dies leaves either no
client, or one whose identity is on record (a resolution by time waits
while it lives).

The remote command's exit code travels in band (G3a, found by the live
route test): the remote script prints ``__ACS_REMOTE_RC_<nonce>__=<rc>`` as
the last line of its stderr, and ``call`` takes the code from there and
removes the line.  The nonce is this call's own (generated here, sent only
inside the base64 script), and the line counts only at the start of a line
at the very end of stderr (review round 1): a workload's text cannot set
the code.  The transport's own exit code is not the command's: over
``ssh-wsl`` a Windows OpenSSH server whose shell is PowerShell turns every
nonzero exit into 1 (a refusal's 2, a wait timeout's 75 and an old
remote's usage error all arrived as 1).  Without the trailer (the route
failed, the connection dropped before the command ended, or the reply was
cut) the transport's code is used: 255 still means "the remote may have
accepted it".

No re-routing (register A40, G3b): every remote script runs the remote CLI
with ``ACS_NO_REROUTE=<this host's label>`` in its environment (set inside
the base64 script, so no transport strips it; an ACS older than G3b
ignores it).  A destination that sees it never routes the call on: no
``--host`` forwarding, no routing by its own placement records, no
automatic placement, no resolution queries, no remote probes.  So two
hosts whose records point at each other cannot bounce a call between
them.
"""

from __future__ import annotations

import base64
import os
import re
import secrets
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass

from .config import Config, ConfigError, RemoteHost

CONNECT_TIMEOUT_S = 10.0
PROBE_TIMEOUT_S = 30.0
CALL_TIMEOUT_S = 900.0
KEEPALIVE_INTERVAL_S = 15
KEEPALIVE_COUNT = 4
EXIT_TIMED_OUT = 75
EXIT_ROUTE = 255
# A40 (G3b): set inside every remote script; its value names the sender
NO_REROUTE_ENV = "ACS_NO_REROUTE"
# the gate of a recorded send (module docstring): the client starts only
# after its identity is on record
GATE_SCRIPT = ('read go || exit 75; [ "$go" = go ] || exit 75; '
               'exec "$@" </dev/null')


class NotSent(Exception):
    """A gated call whose client identity could not be recorded: its
    shell exited before running ssh."""
# commands the client does not bound (they wait for a job by contract)
UNBOUNDED = ("run",)
DRAIN_TIMEOUT_S = 5.0


def marker(nonce: str) -> str:
    """The exit-code trailer's marker of one call."""
    return f"__ACS_REMOTE_RC_{nonce}__="


def _trailer(nonce: str) -> re.Pattern:
    return re.compile(r"(?:\A|\n)" + re.escape(marker(nonce)) +
                      r"(\d{1,3})[\r\n\s]*\Z")


@dataclass
class Reply:
    """One remote call's reply, verbatim (NULs stripped; the exit-code
    trailer removed).  ``rc`` is the remote command's exit code when its
    trailer arrived, else the transport's; None when the call timed out or
    could not be started (``error``).  ``transport_rc`` is the ssh
    client's own exit code."""
    host: str
    args: list
    rc: int | None
    stdout: str
    stderr: str
    timed_out: bool = False
    error: str | None = None
    elapsed_s: float = 0.0
    timeout_s: float | None = None
    transport_rc: int | None = None
    rc_source: str | None = None        # "remote" (trailer) or "transport"
    sent: bool = True                   # False: a gated call never ran ssh

    def as_record(self) -> dict:
        return {"rc": self.rc, "stdout": self.stdout, "stderr": self.stderr,
                "transport_rc": self.transport_rc,
                "rc_source": self.rc_source,
                "timed_out": self.timed_out, "error": self.error,
                "elapsed_s": round(self.elapsed_s, 3),
                "timeout_s": self.timeout_s, "sent": self.sent}


def route(config: Config, host: str) -> RemoteHost:
    remote = config.remotes.get(host)
    if remote is None:
        raise ConfigError(f"host '{host}' is neither this host "
                          f"('{config.host}') nor a configured remote "
                          f"({', '.join(sorted(config.remotes)) or 'none'})")
    return remote


def command_of(args: list[str]) -> str:
    """The subcommand of a forwarded argv (after the global options, such
    as the pin `--host X` a remote submit carries)."""
    skip = False
    for arg in args:
        if skip:
            skip = False
            continue
        if arg in ("--host", "--config"):
            skip = True
            continue
        if arg.startswith(("--host=", "--config=")):
            continue
        return arg
    return ""


def _wait_timeout(args: list[str]) -> float | None:
    for i, arg in enumerate(args):
        value = None
        if arg == "--timeout" and i + 1 < len(args):
            value = args[i + 1]
        elif arg.startswith("--timeout="):
            value = arg.split("=", 1)[1]
        if value is not None:
            try:
                return float(value)
            except ValueError:
                return None
    return None


def call_timeout(remote: RemoteHost, args: list[str],
                 probe: bool = False) -> float | None:
    """The client-side bound of a call (module docstring)."""
    base = remote.call_timeout_s
    if base is None:
        base = PROBE_TIMEOUT_S if probe else CALL_TIMEOUT_S
    command = command_of(args)
    if command in UNBOUNDED:
        return None
    if command == "wait":
        own = _wait_timeout(args)
        return None if own is None else own + base
    return base


def ssh_argv(remote: RemoteHost, args: list[str], nonce: str,
             origin: str = "1") -> list[str]:
    # the command (marked as routed, A40), then its exit code in band on
    # stderr, on a line of its own (module docstring)
    if not re.fullmatch(r"[0-9a-f]{6,64}", nonce):
        raise ValueError(f"bad nonce {nonce!r}")
    script = (f"{NO_REROUTE_ENV}={shlex.quote(origin or '1')} " +
              " ".join(shlex.quote(a) for a in
                       [remote.agent_compute, *args]) + "\n"
              f"rc=$?\nprintf '\\n%s%d\\n' {marker(nonce)} \"$rc\" >&2\n"
              "exit \"$rc\"\n")
    b64 = base64.b64encode(script.encode()).decode()
    if remote.transport == "ssh-wsl":
        # The Windows side only ever sees base64 text (as lenovo.sh does).
        inner = f"wsl -d {remote.distro} -- bash -c 'echo {b64} | base64 -d | bash'"
    elif remote.transport == "ssh":
        inner = f"echo {b64} | base64 -d | bash"
    else:
        raise ConfigError(f"unknown transport {remote.transport!r}")
    connect = remote.connect_timeout_s or CONNECT_TIMEOUT_S
    return ["ssh", "-o", "BatchMode=yes",
            "-o", f"ConnectTimeout={connect:g}",
            "-o", f"ServerAliveInterval={KEEPALIVE_INTERVAL_S}",
            "-o", f"ServerAliveCountMax={KEEPALIVE_COUNT}",
            remote.ssh, inner]


def _run(argv: list[str], timeout: float | None, on_start=None
         ) -> tuple[int | None, bytes, bytes, bool]:
    """Run the ssh client in its own process group; on timeout kill that
    group (the client and what it started: proven ours, we created it) and
    return what it had written.  ``(rc, stdout, stderr, timed_out)``.
    ``on_start``: the gate (module docstring); raises ``NotSent`` when it
    does not return True."""
    gated = on_start is not None
    if gated:
        argv = ["/bin/sh", "-c", GATE_SCRIPT, "acs-remote", *argv]
    proc = subprocess.Popen(argv,
                            stdin=subprocess.PIPE if gated
                            else subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            start_new_session=True)
    if gated:
        try:
            ok, why = bool(on_start(proc.pid)), "it was not recorded"
        except Exception as error:      # noqa: BLE001 - nothing is sent
            ok, why = False, f"{type(error).__name__}: {error}"
        if not ok:
            # anything but the go: the shell exits without running ssh
            proc.communicate(input=b"stop\n")
            raise NotSent(f"the client's identity could not be recorded "
                          f"({why})")
    try:
        out, err = proc.communicate(input=b"go\n" if gated else None,
                                    timeout=timeout)
        return proc.returncode, out, err, False
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError:
            proc.kill()
        try:
            out, err = proc.communicate(timeout=DRAIN_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            # a descendant that left the group holds the pipes: stop
            # reading (review round 1, note 9)
            for stream in (proc.stdout, proc.stderr):
                try:
                    stream.close()
                except OSError:
                    pass
            proc.wait()
            out = err = b""
        return None, out or b"", err or b"", True


def _text(data: bytes | None) -> str:
    return (data or b"").replace(b"\0", b"").decode(errors="replace")


def split_trailer(stderr: str, nonce: str) -> tuple[str, int | None]:
    """``stderr`` without this call's exit-code trailer, and the code it
    carried (None when there is none).  The script prints a newline before
    the marker, so the line is removed with it."""
    found = _trailer(nonce).search(stderr)
    if found is None:
        return stderr, None
    return stderr[:found.start()], int(found.group(1))


def call(config: Config, host: str, args: list[str],
         timeout: float | None = None, probe: bool = False,
         on_start=None) -> Reply:
    """Capture mode: run ``args`` on ``host`` and return the reply.
    ``on_start(pid)``: the gate of a recorded send (module docstring)."""
    remote = route(config, host)
    nonce = secrets.token_hex(8)
    argv = ssh_argv(remote, args, nonce, config.host)
    if timeout is None:
        timeout = call_timeout(remote, args, probe)
    started = time.monotonic()
    try:
        rc, out, err, timed_out = _run(argv, timeout) if on_start is None \
            else _run(argv, timeout, on_start)
    except NotSent as error:
        return Reply(host, list(args), None, "", "", False,
                     f"not sent: {error}", time.monotonic() - started,
                     timeout, sent=False)
    except OSError as error:
        return Reply(host, list(args), None, "", "", False,
                     f"{type(error).__name__}: {error}",
                     time.monotonic() - started, timeout)
    stderr, remote_rc = split_trailer(_text(err), nonce)
    return Reply(host, list(args),
                 None if timed_out else
                 remote_rc if remote_rc is not None else rc,
                 _text(out), stderr, timed_out, None,
                 time.monotonic() - started, timeout, transport_rc=rc,
                 rc_source=None if timed_out else
                 "remote" if remote_rc is not None else "transport")


def explain(reply: Reply) -> None:
    """The client's own words after a failed route (stderr)."""
    host = reply.host
    command = command_of(reply.args)
    if not reply.sent:
        print(f"agent-compute: nothing was sent to {host} ({reply.error})",
              file=sys.stderr)
    elif reply.error is not None:
        print(f"agent-compute: the route to {host} could not be used "
              f"({reply.error}); nothing was run locally", file=sys.stderr)
    elif reply.timed_out:
        print(f"agent-compute: remote call to {host} timed out after "
              f"{reply.timeout_s:g} s; the remote command may have "
              "completed; nothing was run locally", file=sys.stderr)
    elif reply.rc == EXIT_ROUTE:
        if command in ("submit", "run"):
            # A11: 255 is also what ssh returns when the connection drops
            # after the remote accepted the job
            print(f"agent-compute: ssh to {host} exited 255: the route "
                  "failed, or the connection dropped after the remote "
                  "accepted the job.  The remote may have accepted it: "
                  f"check `agent-compute --host {host} status` before "
                  "resubmitting; use --request-id (a resubmit with the "
                  "same id is refused while the first may have run).  "
                  "Nothing ran on this host.", file=sys.stderr)
        else:
            print(f"agent-compute: ssh to {host} exited 255 (the route "
                  "failed, or the connection dropped); nothing was run "
                  "locally", file=sys.stderr)


def exit_code(reply: Reply) -> int:
    if reply.timed_out:
        return EXIT_TIMED_OUT
    if reply.rc is None:
        return EXIT_ROUTE
    return reply.rc


def forward(config: Config, host: str, args: list[str],
            timeout: float | None = None) -> int:
    """Write-through mode: the remote's output on ours, its exit code."""
    reply = call(config, host, args, timeout)
    sys.stdout.write(reply.stdout)
    sys.stdout.flush()
    sys.stderr.write(reply.stderr)
    explain(reply)
    return exit_code(reply)
