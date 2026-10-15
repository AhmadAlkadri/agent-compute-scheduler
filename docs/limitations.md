# Limitations and what has been exercised

ACS is **alpha** software. This page says what it does not do, which
claims are backed by tests, by deployment or by real use, and what to do
about each limit. It is meant to be read before you rely on ACS for
anything you cannot afford to lose.

## What ACS is, in one paragraph

ACS is an admission layer for heavy jobs that cooperating agents submit on
one or two hosts: one heavy slot per host, a queue in FIFO order, a guard
that checks memory before it starts a job, a record of the workload's
identity, a check that the job left nothing behind, and a durable block on
the host when an end cannot be proven clean. It is not a cluster scheduler,
it does not contain a hostile or careless process, and it promises nothing
beyond FIFO order.

## Tested, deployed, exercised in real use

These are three different claims. A capability can be tested without ever
having run for real.

**Tested.** The standard test profile (`python -m pytest -v -rs`, see the
README) runs about 1,670 tests on a clean clone with only Python 3.11 or
later, pytest and Pueue; the Docker-backed tests need Docker and the public
test image and skip with a counted reason without them. The suite covers
the host and container venues, cancellation, recovery and blocks, the
config schema and its migration, the installer, the gate-evidence pin, and
automatic placement and routed submits with a fake ssh. It has been run on
macOS arm64 with Docker Desktop and on Linux x86_64 under WSL2. Tests on a
fake ssh are not tests on a real second host.

**Deployed.** The maintainer installs each release on their own two hosts
(one macOS arm64 machine with Docker Desktop, one WSL2 distribution on
another machine, reached over ssh) before it is published, with the
installer described in the README. The release notes name the version and
the date. Version 0.3.0 was installed on both of the maintainer's hosts on
2026-10-12, before publication, and each ran a smoke job (including one
routed from the macOS host to the Linux host with `--host`).

**Exercised in real use.** Production use so far is the maintainer's own:
in the order of 700 jobs across the two hosts by early October 2026, host
and container venues, cancellations of running jobs (each ended with a
clean verdict), held jobs whose inputs changed, refusals and admission
waits. Most of those jobs ran on the earlier 0.1.x line; a few ran on
0.2.x, including a job routed to the second host with `--host`. Version
0.3.0 has been in real use on the macOS host since 2026-10-14, running a
consuming project's host-venue jobs (checkpointed simulation windows of
minutes to hours). Several capabilities are therefore tested but have
**not** been exercised in real use:

- automatic placement across hosts (configured but never used in
  production): tested in the suite, never exercised for real, and no
  isolated two-host validation has been run; treat it as
  **experimental**;
- `--probe` jobs, and a job that submits its successor from inside a job;
- routing from the Linux host back to the macOS host;
- `uninstall` on a live installation;
- the first-engine-pin path for a new operator who is not the maintainer;
- every change made for this release (see the release notes).

## Support envelope

One or two hosts per operator; macOS arm64 (with Docker Desktop) and Linux
x86_64 (tested on Ubuntu 24.04 under WSL2; native Linux is expected to work
but is not separately validated); host and container venues; Pueue 4; Python 3.11 or
later; the internal guard. Cross-host use is explicit routing with
`--host <label>` over ssh; automatic placement is experimental. Native
Windows, macOS on Intel, other Linux architectures and Pueue 3 are not
tested. The macOS Docker paths are validated by the maintainer on a real
Mac and by the maintainer's release gates, not by continuous integration:
CI (GitHub-hosted Ubuntu runners, Python 3.11, 3.12 and 3.14, the standard
profile with the public pinned test image, skip accounting and the skill
check, `tools/ci.sh`) does not cover macOS Docker Desktop or a real two-host
installation.

## Limitations

Each item says what the limit is, who bears it and what to do.

### Containment and cleanup

1. **ACS is not containment.** Tracking is cooperative and nothing is
   enforced by the kernel (no cgroups, on either platform). A host process
   that calls `setsid()` leaves the tracked group, and on macOS so does
   one that calls `setpgid()` (macOS tracks the process group only). In a
   container, a descendant that clears its environment (`env -i`) and
   calls `setsid()` is not tracked, and the job can end `clean` while that
   process still runs. Do not submit commands that detach (`nohup`,
   `setsid`, `systemd-run`, `docker run -d`).
2. **A container job needs `python3` in the container** and readable
   `/proc/<pid>/environ`. A process of another uid, or an unmarked process
   in an orphaned session (also an unrelated one in a shared container),
   makes cleanup unverifiable and blocks the host until someone inspects
   it. This is fail-closed: a false block, never a false clean.
3. **Held-back kill.** If a member of a container job has an escaped,
   unmarked descendant, the guard does not signal the group, to keep the
   only evidence of that descendant. The job runs on, ends
   `cleanup-unverified` and the host is blocked, but the held processes
   keep their memory until they end or a person acts. Memory is not
   protected in that case.
4. **Some stale host groups are never signalled**: records without a usable
   proof-of-life time, epoch-only or legacy identities, a changed boot id.
   They need handling by hand.
5. **The job marker is `<case>/<UTC second>`**, not unique per launch. Two
   launches of one case in the same second share it, and `reconcile
   --terminate` could then signal the other launch's processes. It needs
   two live jobs, which the one-slot rule prevents unless a foreign guard
   shares the guard home (next item).
6. **Do not share the guard home with other lock users.** A guard that is
   not ACS's does not honour ACS blocks or the in-flight marker; it only
   excludes ACS jobs through the lock. The default guard home is private
   to the installation.
7. **A container launch that cannot enter its workdir** (a hung bind
   mount) holds the slot until `--timeout-min`.

### Recovery

8. **There is no audited command to clear a block.** `reconcile` clears a
   block only on evidence, and agents never edit ACS state. A block that
   can never be verified (for example after the Docker daemon's identity
   changed) needs the manual, operator-only procedure in
   `docs/execution-paths.md`.
9. **No exactly-once promise after a crash.** Nothing is started or
   replayed automatically after a daemon restart; `reconcile` first, then
   `resume`.

### Inputs, credentials and paths

10. **Credentials are screened by variable name only.** A secret in argv or
    in `--env` persists in the job manifest and logs. Read credentials from
    a file instead.
11. **The held-inputs check** covers argv file operands and the project's
    configured pathspecs. A script that reads files by computed paths is
    not covered.
12. **Paths derive from `HOME`.** The state directory, the config and the
    guard home follow the caller's `HOME`, and `AGENT_COMPUTE_CONFIG`
    selects any config. `doctor` reports a second state dir, queue daemon
    or guard home (read-only), but does not prevent one.
13. **Cooperative trust.** The files ACS writes are same-user files; a
    process of the same user can forge them. The claim that only a claimed
    ACS job may run the internal guard is cooperative, not a security
    boundary.

### Installation and operation

14. **The installer takes no lock.** Do not run two installs on one host.
    Immutability of an installed version is by file mode only.
    `uninstall --force`, `config migrate` and `pin-backend` are
    operator-only; agents never run them.
    `uninstall` (without `--force`) refuses when it cannot read the config or
    the job records, and when a dangling symlink lies on the way to them. A
    plain mount point of an unmounted volume with no link in the path
    cannot be told from a state dir that was never created: it reads as
    "no jobs". Uninstall only with the state volume mounted.
15. **A fresh installation is refused** while `~/.smc-guard` (the guard
    home of the earlier private tool this one grew out of) has lock users
    or an in-flight marker. Hosts without such a directory are
    unaffected.
16. **Doctor's configuration-change check** can accept a changed key that
    an earlier recorded action excuses. No admission decision reads it.
17. **Record gaps (minor).** `stale-lock` and `stale-inflight` actions
    appear in the guard's record and the job history, not in the project's
    runlog.
18. **Docker Desktop on macOS.** Container starts can wedge after hours of
    VM uptime, and admission uses the VM's reported memory under a
    heuristic credit (`docs/memory-model.md`). Both are properties of the
    environment; check that a plain `docker run --rm <image> true` returns
    quickly before you start container work.

### Scheduling

19. **Scheduling is FIFO**, one heavy slot per host. There are no
    priorities, reservations, time slicing or fairness beyond arrival
    order. A long job delays everything behind it; the relief is to place
    work on another host, not a priority rule.
20. **A placed job's host is fixed at submit.** There is no migration of
    queued work and no retry or fallback on another host after an
    uncertain end (this is deliberate: it could run a job twice).

## What is deferred

Ideas that are not defects and are not scheduled for the alpha: a
per-launch job marker; a re-check of a late direct child in the guard
before it signals; a fuller record of stale-lock actions; an audited
command to clear a block; tests that pile up many jobs or drive the runner
end to end for every cleanup shape.

## Where to read more

`docs/execution-paths.md` lists every supported host, venue and path with
its caveats; `docs/architecture.md` has the design decisions;
`docs/memory-model.md` the admission rule. `docs/provenance.md` says where
parts of the code come from and how to read the references to a private
development record in source comments.
