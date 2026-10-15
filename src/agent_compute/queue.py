"""Minimal Pueue integration: one dedicated daemon, one one-slot group.

ACS runs its *own* pueued instance (config, state and a unix socket under the
ACS state dir), so it never touches another Pueue queue.  Every ACS job is a
task in a single group with parallelism 1 on this host; project names are
metadata, never separate groups.

Verified Pueue 4.0.4 behaviour this module relies on (docs/evidence):

* ``pueue add`` stores the *client's whole environment* with the task, so
  tasks are added with a scrubbed environment and the job's own environment
  lives in the ACS manifest;
* tasks run through ``sh -c``; the task command is a fixed, quoted runner
  invocation carrying only the job id — never the workload argv;
* ``pueue kill`` SIGKILLs the task and leaves new-session descendants
  running, so ACS never uses it on a running task: cancellation goes through
  the runner, which stops the workload through the guard;
* ``pueue remove`` refuses running tasks, so a successful remove proves the
  task never started;
* after the daemon dies with a task running, the restarted daemon marks the
  task ``Killed`` (the task itself kept running) and pauses its group; queued
  tasks stay queued.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import time
from pathlib import Path

from .config import Config
from .fsprobe import exists_strict
from .state import JobStore, Unreadable, private_dir, read_json

SCRUBBED_ENV_KEYS = ("PATH", "HOME", "LANG", "USER", "LOGNAME")


# Pueue 4 task states in which the task has ended for good (every result:
# Success, Failed, FailedToSpawn, Killed, Errored, DependencyFailed, is a
# `Done` variant).  ACS never runs `pueue restart`, so an ended task is
# never dispatched again.
TERMINAL_TASK_STATES = ("Done",)


class QueueError(Exception):
    pass


def pueue_config_path(config: Config) -> Path:
    return config.pueue_dir / "pueue.yml"


def socket_path(config: Config) -> Path:
    return config.pueue_dir / "run" / "pueue.sock"


def write_pueue_config(config: Config) -> Path:
    base = private_dir(config.pueue_dir)
    private_dir(base / "run")
    private_dir(base / "data")
    sock = socket_path(config)
    if len(str(sock)) > 100:
        raise QueueError(f"socket path too long for AF_UNIX: {sock}")
    text = (
        "shared:\n"
        f"  pueue_directory: {json.dumps(str(base / 'data'))}\n"
        f"  runtime_directory: {json.dumps(str(base / 'run'))}\n"
        "  use_unix_socket: true\n"
        f"  unix_socket_path: {json.dumps(str(sock))}\n"
        "daemon:\n"
        "  default_parallel_tasks: 1\n"
        "  pause_group_on_failure: false\n"
        "  pause_all_on_failure: false\n")
    path = pueue_config_path(config)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write(text)
    return path


def scrubbed_env() -> dict[str, str]:
    env = {k: os.environ[k] for k in SCRUBBED_ENV_KEYS if k in os.environ}
    env.setdefault("PATH", "/usr/bin:/bin")
    return env


def pueue(config: Config, *args: str, check: bool = True,
          timeout: float = 60) -> subprocess.CompletedProcess:
    if not config.pueue:
        raise QueueError("queue.pueue is not configured")
    argv = [config.pueue, "--color", "never", "-c",
            str(pueue_config_path(config)), *args]
    result = subprocess.run(argv, capture_output=True, text=True,
                            env=scrubbed_env(), timeout=timeout)
    if check and result.returncode != 0:
        raise QueueError(f"pueue {' '.join(args)} failed: "
                         f"{(result.stderr or result.stdout).strip()[:400]}")
    return result


def daemon_pid_alive(config: Config) -> bool:
    try:
        pid = int((config.pueue_dir / "run" / "pueue.pid").read_text())
    except (OSError, ValueError):
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    out = subprocess.run(["ps", "-o", "comm=", "-p", str(pid)],
                         capture_output=True, text=True, timeout=30)
    return "pueued" in out.stdout


def daemon_running(config: Config, timeout: float | None = None) -> bool:
    try:
        present = exists_strict(socket_path(config))
    except OSError as error:
        # A49: a socket that cannot be examined is not "no daemon" (that
        # would start a second one beside a live queue)
        raise QueueError(f"the queue socket {socket_path(config)} cannot be "
                         f"examined ({error.strerror or error})") from None
    if not present:
        return False
    try:
        return pueue(config, "status", "--json", check=False,
                     timeout=timeout or 30).returncode == 0
    except (OSError, subprocess.SubprocessError, QueueError):
        return False


def running_tasks(config: Config) -> list[str]:
    if not daemon_running(config):
        return []
    tasks = status_json(config).get("tasks", {})
    return [tid for tid, task in tasks.items()
            if task.get("group") == config.queue_group and
            _status_name(task.get("status")) in ("Running", "Paused")]


def _status_name(status) -> str:
    return status if isinstance(status, str) else next(iter(status or {}), "")


def status_json(config: Config, timeout: float | None = None) -> dict:
    return json.loads(pueue(config, "status", "--json",
                            timeout=timeout or 60).stdout)


def _pending_work(config: Config) -> list[str]:
    """Pending tasks that a (re)started daemon could dispatch.  Fails closed:
    an unreadable persisted queue, or any unfinished ACS job that was queued
    but never claimed, counts as pending."""
    pending = []
    path = config.pueue_dir / "data" / "state.json"
    tasks: dict = {}
    try:
        present = exists_strict(path)
    except OSError:
        return ["<unreadable pueue state>"]         # A49
    if present:
        state = read_json(path)
        if not isinstance(state, dict) or \
                not isinstance(state.get("tasks") or {}, dict):
            return ["<unreadable pueue state>"]
        tasks = state.get("tasks") or {}
        for task_id, task in tasks.items():
            if not isinstance(task, dict):
                return ["<unreadable pueue state>"]
            if task.get("group") == config.queue_group and \
                    _status_name(task.get("status")) in ("Queued", "Stashed",
                                                         "Locked"):
                pending.append(task_id)
    store = JobStore(config.state_dir)
    try:
        job_ids = store.all_ids()
    except OSError:
        return pending + ["<unreadable jobs dir>"]      # A49
    for job_id in job_ids:
        st = store.state(job_id)
        if isinstance(st, Unreadable):
            pending.append(job_id)      # C4: it may be queued; counts
            continue
        st = st or {}
        if st.get("pueue_task") is not None and not st.get("outcome") and \
                not st.get("runner"):
            # A3: a task the persisted queue holds as Done, or no longer
            # holds, can never be dispatched by the daemon about to start,
            # so it must not close the gate on every start.  (`status` then
            # finishes the job as never started.)  Any other state counts.
            task = tasks.get(str(st["pueue_task"]))
            if task is None or _status_name(task.get("status")) in \
                    TERMINAL_TASK_STATES:
                continue
            pending.append(job_id)
    return pending


def ensure_daemon(config: Config) -> list[str]:
    """Start the dedicated daemon if needed; returns notices.

    Pending work never starts automatically on a daemon (re)start: if the
    persisted queue holds tasks, the ACS dispatch gate is closed *before*
    the daemon starts and the group is paused; ``agent-compute resume``
    reopens both after an agent has looked at ``status``.
    """
    notices: list[str] = []
    if not config.queue_enabled:
        raise QueueError("the queue is disabled in this host's config")
    write_pueue_config(config)
    store = JobStore(config.state_dir)
    if not daemon_running(config):
        if daemon_pid_alive(config):
            raise QueueError("the queue daemon is alive but not answering; "
                             "not starting a second one (inspect "
                             f"{config.pueue_dir / 'pueued.log'})")
        stale = socket_path(config)
        if exists_strict(stale):
            stale.unlink()
        pending = _pending_work(config)
        if pending:
            store.close_gate(f"queue daemon restarted with {len(pending)} "
                             "pending task(s); review `agent-compute status` "
                             "then `agent-compute resume`")
            notices.append("dispatch gate closed: pending tasks survived a "
                           "daemon restart")
        # the forked daemon inherits stdio: never a pipe we wait on
        fd = os.open(config.pueue_dir / "pueued.log",
                     os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            # own session: the daemon must not die with the invoking CLI's
            # process group or login session (observed over ssh -> WSL)
            subprocess.run([config.pueued, "-c", str(pueue_config_path(config)),
                            "-d"], stdin=subprocess.DEVNULL, stdout=fd,
                           stderr=fd, env=scrubbed_env(), timeout=60,
                           cwd="/", start_new_session=True)
        finally:
            os.close(fd)
        for _ in range(50):
            if daemon_running(config):
                break
            time.sleep(0.2)
        else:
            raise QueueError("pueued did not come up")
        notices.append("started the agent-compute queue daemon")
        if pending:
            pueue(config, "pause", "-g", config.queue_group, "--wait",
                  check=False)
    groups = status_json(config).get("groups", {})
    if config.queue_group not in groups:
        pueue(config, "group", "add", "-p", "1", config.queue_group)
    block, gate = store.block(config.host), store.gate()
    if block or gate:
        # C4: an unreadable block or gate is true here, so it pauses too
        pueue(config, "pause", "-g", config.queue_group, "--wait",
              check=False)
        notices += [f"group paused: {record.reason}" for record in
                    (block, gate) if isinstance(record, Unreadable)]
    if groups.get(config.queue_group, {}).get("parallel_tasks", 1) != 1:
        pueue(config, "parallel", "-g", config.queue_group, "1")
        notices.append("reset the shared heavy group to one slot")
    return notices


def runner_command(config: Config, job_id: str) -> str:
    from . import __file__ as package_init
    entry = Path(package_init).resolve().parent.parent.parent / "bin" / \
        "agent-compute"
    parts = [config.python, str(entry), "--config",
             os.path.abspath(config.path), "_exec", job_id]
    return " ".join(shlex.quote(p) for p in parts)


def add_task(config: Config, job_id: str) -> int:
    """Queue the job's runner.  The task runs in ``/`` (A3): the runner
    needs no working directory (its entry and config paths are absolute; it
    gives the guard the manifest's cwd itself), so a job cwd removed while
    queued cannot make Pueue fail to spawn the runner and strand the job."""
    result = pueue(config, "add", "-g", config.queue_group, "-l", job_id,
                   "-w", "/", "-p", "--", runner_command(config, job_id))
    return int(result.stdout.strip())


def task_status(config: Config, task_id: int) -> tuple[str, dict]:
    tasks = status_json(config).get("tasks", {})
    task = tasks.get(str(task_id))
    if task is None:
        return "Missing", {}
    status = task.get("status")
    if isinstance(status, str):
        return status, {}
    name = next(iter(status))
    return name, status[name] or {}


def remove_queued(config: Config, task_id: int) -> bool:
    """True only when Pueue removed the task before it started."""
    return pueue(config, "remove", str(task_id), check=False).returncode == 0


def pause_group_quietly(config: Config) -> None:
    if not config.queue_enabled:
        return
    try:
        if daemon_running(config):
            pueue(config, "pause", "-g", config.queue_group, "--wait",
                  check=False)
    except (OSError, subprocess.SubprocessError, QueueError):
        pass


def resume_group(config: Config) -> None:
    pueue(config, "start", "-g", config.queue_group)


def shutdown(config: Config) -> None:
    """Pause dispatch (running tasks continue) and stop the daemon."""
    pueue(config, "pause", "-g", config.queue_group, "--wait", check=False)
    pueue(config, "shutdown", check=False)
