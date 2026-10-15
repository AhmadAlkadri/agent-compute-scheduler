# The memory model

What the guard does with memory, in one place. It describes the internal guard
(`src/agent_compute/guard/engine.py`, the only backend since 0.2.0) and the
machine config (`src/agent_compute/config.py`). Every code block below is
quoted verbatim from those files (the comment line names the file);
`tests/test_g2b_policy.py::test_the_memory_model_quotes_the_code_verbatim`
fails when a quote no longer matches the code. Prose that is not a quote
says what the code does; where it is a judgement, it says so.

This document describes the mechanism as it is; it adds none. The admission
rule, the VM credit formula and the pause are the ones the maintainer's
hosts have run since the internal guard replaced the external one
(2026-10).

## 1. Three notions

1. **The declared reserve** (the job's *estimate*): the memory the job is
   expected to need. It is the case's measured peak from the project's
   baseline table, or what the submitter declared (`--reserve-gb`, or a
   ladder config's heaviest-rung reserve when the project uses the
   `smc-ladder` detector), whichever is larger; a probe reserves what it
   declares, else its last measurement, else the whole budget.

```python
# guard/engine.py
            # admission is max(measured peak, declared reserve)
            self.estimate_gb = max(measured_gb or 0.0,
                                   self.declared_reserve or 0.0)
            self.declared_only = measured_gb is None
```

   With neither a measurement nor a declaration a run is refused before it
   starts ("no measured peak ... (or declare the reserve with
   --reserve-gb)"); through ACS that is `--probe` first or `--reserve-gb`.

2. **The measured available memory** of the host, sampled when the job is
   admitted and every second while it runs. On macOS it is the physical
   memory minus wired, anonymous and compressor-held pages plus purgeable
   ones (`vm_stat`); on Linux it is the kernel's `MemAvailable`:

```python
# guard/engine.py
        used = (wired + anonymous + compressor - purgeable) * page
        return {"available": float(total - used), "source": "vm_stat",
...
    return {"available": float(meminfo.get("MemAvailable", 0)),
            "source": "/proc/meminfo"}
```

3. **The budget**: a per-job memory *ceiling* (section 5). It is the
   profile's `budget_gb`, or the operator's `backend.dedicated_budget_gb`
   for every project of the host when declared (the runner passes it as
   `--budget-gb`, read from the live config at dispatch). The budget is
   never compared with free memory: a job is admitted on notion 2 against
   notion 1, and killed or refused against notion 3.

## 2. Admission

A job is admitted when the measured available memory, plus the macOS
Docker VM credit for a container job (section 3), covers the profile's
floor plus the job's reserve:

```python
# guard/engine.py
        memory = host_memory_sample()
        available_gb = memory["available"] / GB
        needed_gb = self.floor_gb + self.estimate_gb
...
        if not (available_gb + credit_gb >= needed_gb):
```

so, in words: **available + credit >= floor_gb + reserve**. Before that,
a host job whose reserve exceeds the budget is refused outright (it could
never run within its ceiling):

```python
# guard/engine.py
            if self.venue == "host" and self.estimate_gb > self.budget_gb:
                return self.refuse(
                    f"reserve {self.estimate_gb:.2f} GB exceeds the "
                    f"host budget {self.budget_gb:.1f} GB")
```

A memory refusal ("host available memory ..."), a free-disk refusal and a
busy Docker VM guest ("Docker VM guest memory ...") are retried by the ACS
runner with a doubling backoff (`admission_retry_s`, 120 s, up to
`admission_retry_max_s`, 900 s) until the job's admission deadline
(`--lock-wait`, at most `lock_wait_max_s`); then the job ends
`admission-timeout`, never started. Since 0.2.0 a refused attempt writes no
line to the project's runlog; its reason is in the attempt's
`guard.json` (`refusal`) and the job's `output.log`, and `agent-compute
status` shows it as the wait reason.

The floor is the margin the host keeps for everything else (the operator's
sessions, the IDE, Docker itself). Its value is configuration
(`[backend.profile].floor_gb`; section 7 for the default of a fresh
config).

## 3. The macOS Docker VM credit

On macOS, Docker Desktop runs containers in one virtual machine whose guest
RAM the host counts as used memory of one process, whether or not the
guest is using it. Without a correction a container job would be refused
for memory the VM already holds (two jobs once waited 8 and 12 hours for
that reason). The credit is that correction, and it applies only
there:

```python
# guard/engine.py
def docker_vm_separate() -> bool:
    """True where containers run in a VM whose guest RAM the host counts as
    used memory of one VM process (Docker Desktop on macOS)."""
    return sys.platform == "darwin"
```

The credit is the VM's resident host memory that is free in the guest,
less an overhead, and never more than the job's reserve:

```python
# guard/engine.py
    used = guest_total_gb - guest_available_gb
    # assumes 4 KiB guest pages (struct page ~1.56 % of RAM); a 16 KiB-page
    # guest kernel would need less, so this stays conservative there
    overhead = VM_CREDIT_OVERHEAD_GB \
        + VM_CREDIT_OVERHEAD_FRACTION * guest_total_gb
    raw = max(0.0, min(resident_gb, guest_total_gb) - used - overhead)
    return raw, min(raw, estimate_gb)
```

```python
# guard/engine.py
VM_CREDIT_OVERHEAD_GB = 0.25       # VMM overhead counted in the footprint
VM_CREDIT_OVERHEAD_FRACTION = 0.03  # guest RAM outside MemTotal (kernel, memmap)
```

**Its Docker Desktop assumption.** The VM's resident memory is read with
`footprint` from exactly one Virtualization.framework VM process, and only
when Docker Desktop's own VZ helper is running beside it (so the VM is
Docker's); the guest's `/proc/meminfo`, read in the job's container, is used
only when its `MemTotal` matches `docker info`'s within 1 %:

```python
# guard/engine.py
VM_PROCESS_NAME = "com.apple.Virtualization.VirtualMachine"
DOCKER_VZ_HELPER = "com.docker.virtualization"
...
VM_MEMTOTAL_TOLERANCE = 0.01
```

Any failed or ambiguous measurement gives **credit 0** with the reason in
the guard record (`admission_memory.vm_credit.reason`). So on macOS with
another container runtime (colima, OrbStack, Podman machine, Rancher
Desktop: no `com.docker.virtualization` process, or another VM beside
Docker's) the credit is 0 and admission is the plain rule of section 2;
the job then needs host available memory for its whole reserve, which is
conservative. On Linux (a WSL2 distro included) containers are
host processes and `MemAvailable` already counts them: no credit, no guest
check.

With a credit, the job must also fit in the VM: larger than the guest's
`MemTotal` is a final refusal; larger than the guest's `MemAvailable` now
is a refusal the runner retries ("Docker VM guest memory").

## 4. The run-time floor pause

While the job runs, the guard samples the host's available memory every
second, **without** the VM credit, and pauses the job (SIGSTOP of its
process group, or `docker pause`) when it falls below the floor; it resumes
the job after the available memory has stayed above floor + 2 GB for 60 s,
and kills it after 30 minutes paused (`killed-paused`):

```python
# guard/engine.py
        floor = self.floor_gb * GB
        resume_level = (self.floor_gb + RESUME_MARGIN_GB) * GB
...
                    if not math.isnan(available) and available < floor:
                        self.pause(f"host available {available / GB:.1f} GB "
                                   f"< floor {self.floor_gb:.1f} GB")
...
                    if paused_for > self.max_pause:
                        self.kill("killed-paused",
```

```python
# guard/engine.py
RESUME_MARGIN_GB = 2.0         # resume when available > floor + margin ...
RESUME_HOLD_S = 60.0           # ... for this long
MAX_PAUSE_S = 1800.0           # kill after this long paused
```

The pause is the backstop of the credit: a credit that was too generous is
caught here, since the pause takes no credit.

## 5. The budget is a ceiling only

The budget bounds the job's own memory; it is never an admission test
(a design decision: admission stays measured available memory against floor
plus reserve). A host job is killed
when its process group's resident memory exceeds its ceiling:

```python
# guard/engine.py
                if rss > job_budget * GB:
                    self.kill("killed-budget",
```

The ceiling is the budget, except for a job admitted on a *declaration*
(a probe, `--reserve-gb` or a ladder's declared reserve without a measured
row): that job is held to 1.5 x what it declared, so a job that outgrows
its own declaration is killed instead of the host:

```python
# guard/engine.py
        if self.venue == "container":
            return float(self.docker_memory_gb or self.budget_gb)
        if (self.probe or self.declared_only) and self.estimate_gb > 0:
            return min(self.budget_gb,
                       float(math.ceil(self.estimate_gb * self.memory_factor)))
        return self.budget_gb
```

**The cap.** A configured budget (`[backend.profile].budget_gb`, and
`backend.dedicated_budget_gb`) may not exceed the host's RAM, read at every
config load (since 0.2.0; before, a fixed 48 GB, 75 % of the 64 GB machine
the first configs were written for, which the v1 format keeps):

```python
# config.py
    if profile["budget_gb"] > cap_gb:
        raise ConfigError(f"backend.profile.budget_gb must be <= this "
...
    if not (math.isfinite(value) and 0 < value <= cap_gb):
```

`doctor` prints the host's RAM and cores, this cap and the configured
numbers ("host resources and profile defaults").

## 6. Containers: the limit is 1.5 x the reserve

A container job's memory ceiling is 1.5 x its reserve, rounded up to whole
GB, at least 1 GB, and never above the Docker VM's memory minus 2 GB:

```python
# guard/engine.py
CONTAINER_MEMORY_FACTOR = 1.5
VM_RESERVE_GB = 2.0
```

```python
# guard/engine.py
def docker_memory_cap_gb(peak_gb: float, vm_gb: float,
                         factor: float = CONTAINER_MEMORY_FACTOR,
                         vm_reserve_gb: float = VM_RESERVE_GB) -> float:
    cap = math.ceil(max(peak_gb, 0.0) * factor)
    cap = max(cap, 1)
    return float(min(cap, max(vm_gb - vm_reserve_gb, 1.0)))
```

```python
# guard/engine.py
            # the per-job memory ceiling: the sampler kills the job above it
            # (job_budget_gb) whether or not it is applied to the container
            self.docker_memory_gb = docker_memory_cap_gb(
                self.estimate_gb, self.vm["vm_memory_gb"],
                self.memory_factor, self.vm_reserve_gb)
```

The guard always enforces it by sampling the container's memory (the job
is killed above it). It also applies it to the container itself (`docker
update --memory/--memory-swap`, with the profile's `container_cpus` as the
CPU quota) only for the containers a project lists in `container_limits`:
a shared container's limits belong to its owner. The factor and the VM
reserve are the profile's optional `container_memory_factor` and
`vm_reserve_gb` (defaults above).

## 7. A fresh host's defaults

`agent-compute config init` prints a config for a new host with profile
numbers derived from the host; `doctor` prints the same derivation
beside the configured numbers. They are defaults for a *fresh* config only:
the migration of an existing config keeps the operator's numbers unchanged.

```python
# config.py
    floor = max(2.0, math.ceil(round(0.125 * memory_gb * 10, 6)) / 10)
    budget = max(0.1, math.floor(round(0.25 * memory_gb * 10, 6)) / 10)
    return {"floor_gb": floor, "budget_gb": budget,
            "container_cpus": max(1, (cores or 1) - 2)}
```

That is `floor_gb = max(2, 0.125 x RAM)`, `budget_gb = 0.25 x RAM`,
`container_cpus = cores - 2` (at least 1). For a 64 GB, 12-core Mac that is
8 / 16 / 10. For a 10 GB WSL2 VM with 8 CPUs (9.713 GB as read) it is
2 / 2.4 / 6; an operator may configure other numbers, for example a
profile of 8 / 12 / 10 with a declared dedicated budget of 48 GB on the
Mac. (Judgement,
not code: the defaults are deliberately conservative for a machine that is
also someone's workstation; a dedicated host raises the budget with
`dedicated_budget_gb`.)

## 8. What is fixed in code

Not configurable through ACS: the resume margin and hold, the pause
ceiling, the kill grace (10 s), the 1-second sample interval, the VM credit
overhead and the MemTotal tolerance (above), and the 0.25 GB minimum
estimate of a probe. The profile's optional keys (`disk_floor_gb` 20,
`light_budget_gb` 1, `vm_reserve_gb` 2, `container_memory_factor` 1.5)
have the engine's defaults when the profile does not set them.
