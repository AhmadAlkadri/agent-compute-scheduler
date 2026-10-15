"""Container-venue admission on macOS: the Docker VM credit.

The defect (found in the earlier private research tool, 2026-10-01): on macOS the guard's host "available"
memory is total minus wired, anonymous and compressor pages plus purgeable.
Docker Desktop's VM keeps its guest RAM as anonymous memory of
``com.apple.Virtualization.VirtualMachine`` after container jobs end, so
memory the VM already holds on the host, and that is free inside the guest,
was subtracted from "available".  A container job runs inside that memory,
yet it was refused as if it needed fresh host pages: 11-13 GB available on
a 64 GB Mac with the VM bloated, and a 13 GB container job waited ~20 h.

The fix (container venue on darwin only): a credit for VM memory that is
resident on the host and free in the guest, capped at the estimate; a
fit-in-VM check on the guest's MemAvailable; credit 0 (today's behaviour)
whenever the measurement fails or is ambiguous.  The host venue and Linux
are unchanged.

No Docker is needed: the in-process tests replace ``engine.docker`` and the
platform gate; the end-to-end test uses a fake ``docker`` on PATH; the VM's
host footprint comes from the engine's VM footprint test hook
(``conftest.VM_FOOTPRINT_ENV``).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys

import pytest

from conftest import VM_FOOTPRINT_ENV
from test_internal_guard import MAC, PY, Sandbox, load_engine

GB = 1024 ** 3
KB = 1024
ENV_VM = VM_FOOTPRINT_ENV


def meminfo(total_gb: float, available_gb: float) -> str:
    return (f"MemTotal:       {int(total_gb * GB / KB)} kB\n"
            f"MemFree:        {int(available_gb * GB / KB)} kB\n"
            f"MemAvailable:   {int(available_gb * GB / KB)} kB\n"
            "Buffers:          126164 kB\n")


class FakeDocker:
    """In-process stand-in for ``engine.docker``: records every call."""

    def __init__(self, guest, info_total: int | None = None):
        """``guest``: the meminfo text, None (the exec fails) or an
        exception to raise from the exec.  ``info_total``: docker info's
        MemTotal in bytes (default: the guest's own MemTotal, else 24 GB)."""
        self.guest = guest
        if info_total is None:
            info_total = 24 * GB
            if isinstance(guest, str):
                for line in guest.splitlines():
                    if line.startswith("MemTotal:") and \
                            line.split()[1].isdigit():
                        info_total = int(line.split()[1]) * KB
        self.info_total = info_total
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, *args, timeout=None, check=True):
        self.calls.append(args)
        out, rc = "", 0
        if args[:1] == ("info",):
            out = f"{self.info_total} 10 linux\n"
        elif args[:1] == ("inspect",):
            out = "fakeid\n" if "{{.Id}}" in args else "true\n"
        elif args[:1] == ("exec",) and "/proc/meminfo" in args:
            if isinstance(self.guest, BaseException):
                raise self.guest
            if self.guest is None:
                out, rc = "", 1
            else:
                out = self.guest
        if check and rc:
            raise subprocess.CalledProcessError(rc, ["docker", *args], out, "")
        return subprocess.CompletedProcess(["docker", *args], rc, out, "")

    def meminfo_reads(self) -> int:
        return sum(1 for c in self.calls
                   if c[:1] == ("exec",) and "/proc/meminfo" in c)


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    box = Sandbox(tmp_path)
    for key, value in box.env.items():
        if key.startswith(("SMC_GUARD_", "ACS_GUARD_")):
            monkeypatch.setenv(key, value)
    monkeypatch.delenv(ENV_VM, raising=False)
    box.vm_file = tmp_path / "vm_resident_bytes"
    return box


def setup(sandbox, monkeypatch, *, host_gb: float, vm_gb, guest,
          separate: bool = True, info_total: int | None = None
          ) -> FakeDocker:
    """Host available memory, the VM's resident host footprint (a number,
    or a raw string for a broken reading), the guest's /proc/meminfo (None:
    the exec fails) and the platform gate (darwin: separate VM)."""
    engine = load_engine()
    sandbox.fake_free.write_text(str(host_gb * GB))
    if vm_gb is not None:
        sandbox.vm_file.write_text(vm_gb if isinstance(vm_gb, str)
                                   else str(vm_gb * GB))
        monkeypatch.setenv(ENV_VM, str(sandbox.vm_file))
    fake = FakeDocker(guest, info_total)
    monkeypatch.setattr(engine, "docker", fake)
    # raising=False: the gate is new with the fix (fail-first on main)
    monkeypatch.setattr(engine, "docker_vm_separate", lambda: separate,
                        raising=False)
    return fake


def job(sandbox, case: str, peak_gb: float | None, *extra: str):
    engine = load_engine()
    if peak_gb is not None:
        sandbox.set_row(case, peak_gb)
    parser = argparse.ArgumentParser(allow_abbrev=False)
    engine.add_run_arguments(parser)
    parser.add_argument("--probe", action="store_true")
    args = parser.parse_args(sandbox.args("run", case, *extra)[3:])
    return engine.Job(args, ["python3", "-c", "pass"],
                      engine.load_profile(MAC))


CONTAINER = ("--venue", "container", "--container", "smc-step-a",
             "--container-workdir", "/work")


def record(sandbox, case):
    return sandbox.record(case)


# --- the reported scenario -------------------------------------------------------

def test_bloated_vm_container_job_is_admitted_with_the_vm_credit(
        sandbox, monkeypatch):
    """Host 11 GB available (floor 8), the VM holds 24 GB on the host of
    which 22 GB are free in the guest, estimate 13 GB.  Before the fix:
    refused (11 < 8 + 13).  After: admitted, 11 + min(credit, 13) >= 21."""
    fake = setup(sandbox, monkeypatch, host_gb=11, vm_gb=24,
                 guest=meminfo(24, 22))
    j = job(sandbox, "bloated", 13.0, *CONTAINER)
    assert j.preflight() is None, sandbox.runlog_text()
    assert fake.meminfo_reads() == 1
    j.write_record()
    rec = record(sandbox, "bloated")
    adm = rec["admission_memory"]
    assert adm["host_available_gb"] == pytest.approx(11.0)
    assert adm["needed_gb"] == pytest.approx(21.0)
    assert adm["vm_credit_gb"] == pytest.approx(13.0)      # capped
    credit = adm["vm_credit"]
    assert credit["reason"] is None
    assert credit["vm_resident_gb"] == pytest.approx(24.0)
    assert credit["vm_source"] == "fake-file"
    assert credit["guest_mem_total_gb"] == pytest.approx(24.0)
    assert credit["guest_mem_available_gb"] == pytest.approx(22.0)
    # 24 resident - 2 guest used - overhead (0.25 + 3 % of 24)
    assert credit["raw_credit_gb"] == pytest.approx(24 - 2 - 0.97, abs=0.01)
    assert credit["credit_gb"] == pytest.approx(13.0)
    # the record keeps the thresholds and the raw host sample beside it
    assert rec["thresholds"]["floor_gb"] == 8.0
    assert adm["sample"]["source"] == "fake-file"


def test_bloated_vm_end_to_end_with_a_fake_docker(tmp_path):
    """The same scenario through the engine's CLI with a fake ``docker`` on
    PATH.  macOS: admitted and run.  Linux: unchanged, refused (containers
    share the kernel there, so /proc/meminfo already accounts for them)."""
    sandbox = Sandbox(tmp_path)
    sandbox.fake_free.write_text(str(11 * GB))
    vm = tmp_path / "vm_resident_bytes"
    vm.write_text(str(24 * GB))
    guest = tmp_path / "guest_meminfo"
    guest.write_text(meminfo(24, 22))
    log = tmp_path / "docker.log"
    fake = tmp_path / "fakebin" / "docker"
    fake.parent.mkdir()
    fake.write_text(f"""#!{PY}
import json, sys, time
argv = sys.argv[1:]
with open({str(log)!r}, "a") as handle:
    handle.write(json.dumps(argv) + "\\n")
if argv[:1] == ["info"]:
    print("{24 * GB} 10 linux")
elif argv[:1] == ["inspect"]:
    print("fakeid" if "{{{{.Id}}}}" in argv else "true")
elif argv[:1] == ["exec"]:
    if "setsid" in argv:
        time.sleep(1.5)
    elif "python3" in argv:
        print(json.dumps({{"leader_ticks": "7", "stranger": False,
                          "members": [], "signalled": [], "errors": []}}))
    elif "/proc/meminfo" in argv:
        sys.stdout.write(open({str(guest)!r}).read())
    elif "cat" in argv and argv[-1].endswith(".pid"):
        print("4242")
    elif "cat" in argv:
        print("1000")
""")
    fake.chmod(0o755)
    sandbox.set_row("e2e", 13.0)
    env = {**sandbox.env, ENV_VM: str(vm),
           "PATH": f"{fake.parent}{os.pathsep}{os.environ['PATH']}"}
    result = subprocess.run(
        [*sandbox.args("run", "e2e", *CONTAINER), "--", "python3", "-c",
         "pass"], env=env, capture_output=True, text=True, timeout=60)
    rec = sandbox.record("e2e")
    if sys.platform == "darwin":
        assert result.returncode == 0, result.stderr
        assert rec["admission_memory"]["vm_credit_gb"] == pytest.approx(13.0)
        assert rec["cleanup"]["clean"] is True
    else:
        assert result.returncode == 3, result.stderr
        assert "host available memory 11.0 GB < floor 8.0 GB" in result.stderr
        assert rec["admission_memory"]["vm_credit"] is None
        # the host refusal comes before any docker call (no log at all)
        calls = ([json.loads(line) for line in log.read_text().splitlines()]
                 if log.is_file() else [])
        assert not [c for c in calls if "/proc/meminfo" in c]


# --- fail-closed: the credit is 0 and the decision is today's -------------------

@pytest.mark.parametrize("vm_gb,guest,reason", [
    ("garbage", meminfo(24, 22), "VM host footprint"),
    ("nan", meminfo(24, 22), "VM host footprint"),
    ("-5", meminfo(24, 22), "VM host footprint"),
    (24, None, "guest /proc/meminfo"),
    (24, "MemTotal: 100 kB\n", "guest /proc/meminfo"),
    (24, meminfo(24, 30), "guest /proc/meminfo"),
], ids=["vm-unreadable", "vm-nan", "vm-negative", "guest-exec-fails",
        "guest-no-available", "guest-available-above-total"])
def test_a_failed_measurement_gives_no_credit(sandbox, monkeypatch, vm_gb,
                                              guest, reason):
    setup(sandbox, monkeypatch, host_gb=11, vm_gb=vm_gb, guest=guest)
    j = job(sandbox, "failed", 13.0, *CONTAINER)
    assert j.preflight() == 3
    text = refusal(sandbox, "failed")
    assert "host available memory 11.0 GB" in text
    assert "floor 8.0 GB" in text and "estimate 13.00 GB" in text
    credit = record(sandbox, "failed")["admission_memory"]["vm_credit"]
    assert credit["credit_gb"] == 0.0
    assert reason in credit["reason"], credit
    assert "no Docker VM credit" in text and reason in text


def test_ambiguous_vm_processes_give_no_credit(sandbox, monkeypatch):
    """Two Virtualization.framework VMs: which one is Docker's is unknown."""
    engine = load_engine()
    setup(sandbox, monkeypatch, host_gb=11, vm_gb=None, guest=meminfo(24, 22))
    ps = ("  4581 /Applications/Docker.app/Contents/MacOS/"
          "com.docker.virtualization\n"
          "  4591 /System/Library/Frameworks/Virtualization.framework/"
          "Versions/A/XPCServices/com.apple.Virtualization.VirtualMachine.xpc/"
          "Contents/MacOS/com.apple.Virtualization.VirtualMachine\n"
          "  5000 /System/Library/Frameworks/Virtualization.framework/"
          "Versions/A/XPCServices/com.apple.Virtualization.VirtualMachine.xpc/"
          "Contents/MacOS/com.apple.Virtualization.VirtualMachine\n")
    real_run = subprocess.run

    def fake_run(argv, *a, **kw):
        if argv[:1] == ["ps"]:
            return subprocess.CompletedProcess(argv, 0, ps, "")
        if argv[:1] == ["footprint"]:
            raise AssertionError("footprint must not run on an ambiguous VM")
        return real_run(argv, *a, **kw)

    monkeypatch.setattr(engine.subprocess, "run", fake_run)
    j = job(sandbox, "ambiguous", 13.0, *CONTAINER)
    assert j.preflight() == 3
    credit = record(sandbox, "ambiguous")["admission_memory"]["vm_credit"]
    assert credit["credit_gb"] == 0.0
    assert "2 com.apple.Virtualization.VirtualMachine processes" in \
        credit["reason"]


@pytest.mark.parametrize("ps,needle", [
    ("", "no com.apple.Virtualization.VirtualMachine process"),
    ("  4591 /x/com.apple.Virtualization.VirtualMachine\n",
     "no com.docker.virtualization process"),
], ids=["no-vm", "vm-not-dockers"])
def test_vm_identity_is_required(monkeypatch, ps, needle):
    engine = load_engine()
    monkeypatch.delenv(ENV_VM, raising=False)
    monkeypatch.setattr(
        engine.subprocess, "run",
        lambda argv, *a, **kw: subprocess.CompletedProcess(argv, 0, ps, ""))
    resident, info = engine.vm_host_footprint()
    assert resident is None and needle in info["reason"], info


def test_footprint_json_is_read_as_resident_dirty_memory(monkeypatch,
                                                         tmp_path):
    """Resident = phys_footprint (never above the sum of dirty) minus the
    compressed/swapped part: only pages that are resident on the host and
    counted as used there can be reused by the job at no host cost."""
    engine = load_engine()
    monkeypatch.delenv(ENV_VM, raising=False)
    ps = ("  4581 /A/com.docker.virtualization\n"
          "  4591 /B/com.apple.Virtualization.VirtualMachine\n")
    report = {"processes": [{
        "name": "com.apple.Virtualization.VirtualMachine", "pid": 4591,
        "footprint": 6124545216,
        "auxiliary": {"phys_footprint": 6124545216},
        "categories": {
            "App-Specific Tag 1": {"dirty": 6067077120,
                                   "swapped": 1792671744},
            "Malloc Large": {"dirty": 57468096, "swapped": 45088768}}}]}

    def fake_run(argv, *a, **kw):
        if argv[:1] == ["ps"]:
            return subprocess.CompletedProcess(argv, 0, ps, "")
        assert argv[:1] == ["footprint"] and argv[-1] == "4591", argv
        out = argv[argv.index("-j") + 1]
        with open(out, "w") as handle:
            json.dump(report, handle)
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(engine.subprocess, "run", fake_run)
    resident, info = engine.vm_host_footprint()
    assert info["reason"] is None, info
    assert resident == 6124545216 - (1792671744 + 45088768)
    assert info["source"] == "footprint" and info["pid"] == 4591
    # a report for another process (the pid was recycled) is refused
    report["processes"][0]["name"] = "something-else"
    resident, info = engine.vm_host_footprint()
    assert resident is None and "footprint" in info["reason"]


# --- the job must fit in the VM ---------------------------------------------------

def test_a_job_larger_than_the_guest_available_memory_is_refused(
        sandbox, monkeypatch):
    """Plenty of host memory, but the guest has only 10 GB available for a
    13 GB job: it cannot fit in the VM, whatever the host has."""
    setup(sandbox, monkeypatch, host_gb=40, vm_gb=24, guest=meminfo(24, 10))
    j = job(sandbox, "nofit", 13.0, *CONTAINER)
    assert j.preflight() == 3
    text = refusal(sandbox, "nofit")
    assert "does not fit in the Docker VM" in text
    assert "guest MemAvailable 10.0 GB < estimate 13.00 GB" in text
    rec = record(sandbox, "nofit")
    assert rec["outcome"] == "refused"
    assert rec["admission_memory"]["vm_credit"]["guest_mem_available_gb"] \
        == pytest.approx(10.0)


def refusal(sandbox, case: str) -> str:
    """The refusal of ``case``'s attempt: since G2b (D3) it is in the
    attempt's guard record (``refusal``), not in the runlog."""
    found = record(sandbox, case).get("refusal")
    assert found, f"no refusal recorded for {case}"
    return found


def test_a_busy_guest_is_a_wait_and_a_too_small_guest_is_final(
        sandbox, monkeypatch):
    """The ACS runner retries a refusal whose text has a
    RETRYABLE_REFUSALS key and ends the job otherwise.  A guest that is
    only busy (MemAvailable < estimate <= MemTotal) is a wait, like host
    memory; a guest smaller than the job can never take it."""
    from agent_compute import runner

    def retryable(reason: str) -> bool:
        return any(key in reason for key in runner.RETRYABLE_REFUSALS)

    setup(sandbox, monkeypatch, host_gb=40, vm_gb=24, guest=meminfo(24, 10))
    assert job(sandbox, "busy", 13.0, *CONTAINER).preflight() == 3
    busy = refusal(sandbox, "busy")
    assert "guest MemAvailable 10.0 GB < estimate 13.00 GB" in busy
    assert retryable(busy), busy

    setup(sandbox, monkeypatch, host_gb=40, vm_gb=24, guest=meminfo(10, 9))
    assert job(sandbox, "small", 13.0, *CONTAINER).preflight() == 3
    small = refusal(sandbox, "small")
    assert "does not fit in the Docker VM: guest MemTotal 10.0 GB < " \
           "estimate 13.00 GB" in small
    assert not retryable(small), small

    # the credit's host refusal stays a host-memory wait
    setup(sandbox, monkeypatch, host_gb=5, vm_gb=24, guest=meminfo(24, 22))
    assert job(sandbox, "hostwait", 2.0, *CONTAINER).preflight() == 3
    assert retryable(refusal(sandbox, "hostwait"))


# --- the credit is capped at the estimate -----------------------------------------

def test_the_credit_never_exceeds_the_estimate(sandbox, monkeypatch):
    """Raw credit ~21 GB, estimate 2 GB: only 2 GB is credited, never the
    floor.  Host 9: 9 + 2 >= 8 + 2, admitted.  Host 5: 5 + 2 < 10, refused,
    although 5 + 21 would have passed."""
    setup(sandbox, monkeypatch, host_gb=9, vm_gb=24, guest=meminfo(24, 22))
    j = job(sandbox, "cap", 2.0, *CONTAINER)
    assert j.preflight() is None, sandbox.runlog_text()
    j.write_record()
    adm = record(sandbox, "cap")["admission_memory"]
    assert adm["vm_credit_gb"] == pytest.approx(2.0)
    assert adm["vm_credit"]["raw_credit_gb"] > 20

    sandbox.fake_free.write_text(str(5 * GB))
    j = job(sandbox, "cap5", 2.0, *CONTAINER)
    assert j.preflight() == 3
    text = refusal(sandbox, "cap5")
    assert "host available memory 5.0 GB + Docker VM credit 2.00 GB" in text
    assert record(sandbox, "cap5")["admission_memory"]["vm_credit_gb"] \
        == pytest.approx(2.0)


def test_credit_arithmetic_is_bounded():
    engine = load_engine()
    f = engine.vm_credit_gb
    # resident, guest total, guest available, estimate -> (raw, applied)
    raw, applied = f(24.0, 24.0, 22.0, 13.0)
    assert raw == pytest.approx(21.03) and applied == 13.0
    # the VM holds less than the guest uses: no credit, never negative
    assert f(1.0, 24.0, 22.0, 13.0) == (0.0, 0.0)
    # resident above the guest size is capped at the guest size
    assert f(100.0, 24.0, 22.0, 50.0)[0] == pytest.approx(21.03)
    for bad in (math.nan, math.inf, -1.0):
        assert f(bad, 24.0, 22.0, 13.0) == (0.0, 0.0)
        assert f(24.0, bad, 22.0, 13.0) == (0.0, 0.0)
        assert f(24.0, 24.0, bad, 13.0) == (0.0, 0.0)


# --- unchanged: host venue, Linux --------------------------------------------------

def test_host_venue_is_unchanged(sandbox, monkeypatch):
    fake = setup(sandbox, monkeypatch, host_gb=11, vm_gb=24,
                 guest=meminfo(24, 22))
    j = job(sandbox, "hostjob", 13.0, "--budget-gb", "48")
    assert j.preflight() == 3
    assert "host available memory 11.0 GB < floor 8.0 GB + estimate " \
           "13.00 GB" in refusal(sandbox, "hostjob")
    assert fake.calls == []                  # no docker at all
    assert record(sandbox, "hostjob")["admission_memory"]["vm_credit"] is None


def test_linux_container_venue_is_unchanged(sandbox, monkeypatch):
    fake = setup(sandbox, monkeypatch, host_gb=11, vm_gb=24,
                 guest=meminfo(24, 22), separate=False)
    j = job(sandbox, "linuxjob", 13.0, *CONTAINER)
    assert j.preflight() == 3
    assert "host available memory 11.0 GB < floor 8.0 GB + estimate " \
           "13.00 GB" in refusal(sandbox, "linuxjob")
    assert fake.meminfo_reads() == 0
    assert record(sandbox, "linuxjob")["admission_memory"]["vm_credit"] \
        is None
    # and admitted exactly as before when the host has the memory
    sandbox.fake_free.write_text(str(21 * GB))
    j = job(sandbox, "linuxjob2", 13.0, *CONTAINER)
    assert j.preflight() is None
    assert fake.meminfo_reads() == 0


def test_the_gate_is_darwin_only(monkeypatch):
    engine = load_engine()
    monkeypatch.setattr(engine.sys, "platform", "linux")
    assert engine.docker_vm_separate() is False
    monkeypatch.setattr(engine.sys, "platform", "darwin")
    assert engine.docker_vm_separate() is True


# --- review round 1 (B1, B2, N2, N6) ------------------------------------------------
#
# B1: the real `vm_host_footprint()` (no VM footprint hook),
# driven through a fake subprocess.run.  Every failure gives credit 0 with a
# reason, through `vm_credit` (the guest reading is good and verified).

PS_OK = ("  4581 /A/com.docker.virtualization\n"
         "  4591 /B/com.apple.Virtualization.VirtualMachine\n")


def footprint_report(**overrides) -> dict:
    proc = {"name": "com.apple.Virtualization.VirtualMachine", "pid": 4591,
            "footprint": 20 * GB, "auxiliary": {"phys_footprint": 20 * GB},
            "categories": {"App-Specific Tag 1": {"dirty": 20 * GB,
                                                  "swapped": 1 * GB}}}
    proc.update(overrides)
    return {"processes": [proc]}


UNDECODABLE = UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")

# (what ps does, what footprint does, a substring of the reason)
FOOTPRINT_FAILURES = {
    "ps-raises": (OSError("no ps"), None, "ps failed"),
    "ps-timeout": (subprocess.TimeoutExpired(["ps"], 10), None, "ps failed"),
    "ps-exit": ((1, ""), None, "ps exit 1"),
    "ps-non-utf8": (UNDECODABLE, None, "ps failed"),
    "ps-superscript-pid": (("  ²4591 /B/com.apple.Virtualization."
                            "VirtualMachine\n  4581 /A/com.docker."
                            "virtualization\n"), None,
                           "no com.apple.Virtualization.VirtualMachine"),
    "footprint-timeout": (PS_OK, subprocess.TimeoutExpired(["footprint"], 10),
                          "footprint failed"),
    "footprint-exit": (PS_OK, (1, None), "footprint exit 1"),
    "footprint-no-report": (PS_OK, (0, None), "no report"),
    "footprint-bad-json": (PS_OK, (0, "{not json"), "footprint failed"),
    "footprint-non-utf8": (PS_OK, UNDECODABLE, "footprint failed"),
    "report-not-object": (PS_OK, (0, "[1, 2]"), "not an object"),
    "processes-not-list": (PS_OK, (0, {"processes": {"pid": 4591}}),
                           "no process list"),
    "processes-a-string": (PS_OK, (0, {"processes": "4591"}),
                           "no process list"),
    "processes-a-number": (PS_OK, (0, {"processes": 5}), "no process list"),
    "phys-missing": (PS_OK, (0, footprint_report(auxiliary={})),
                     "phys_footprint is None"),
    "phys-a-string": (PS_OK, (0, footprint_report(
        auxiliary={"phys_footprint": "21474836480"})),
        "phys_footprint is '21474836480'"),
    "phys-bool": (PS_OK, (0, footprint_report(
        auxiliary={"phys_footprint": True})), "phys_footprint is True"),
    "categories-not-dict": (PS_OK, (0, footprint_report(
        categories=[{"dirty": 1, "swapped": 0}])), "no categories"),
    "category-not-dict": (PS_OK, (0, footprint_report(
        categories={"x": 5})), "is not an object"),
    "dirty-nan": (PS_OK, (0, footprint_report(
        categories={"x": {"dirty": float("nan"), "swapped": 0}})), "dirty"),
    "dirty-inf": (PS_OK, (0, footprint_report(
        categories={"x": {"dirty": float("inf"), "swapped": 0}})), "dirty"),
    "swapped-above-footprint": (PS_OK, (0, footprint_report(
        categories={"x": {"dirty": 20 * GB, "swapped": 21 * GB}})),
        "exceeds the footprint"),
    "recycled-pid": (PS_OK, (0, footprint_report(name="other")),
                     "no single"),
}


def fake_subprocess(ps, footprint):
    def run(argv, *a, **kw):
        if argv[:1] == ["ps"]:
            action = ps
        elif argv[:1] == ["footprint"]:
            action = footprint
            out = argv[argv.index("-j") + 1]
            if isinstance(action, tuple) and action[1] is not None:
                with open(out, "w") as handle:
                    body = action[1]
                    handle.write(body if isinstance(body, str)
                                 else json.dumps(body))
        else:
            raise AssertionError(f"unexpected command {argv}")
        if isinstance(action, BaseException):
            raise action
        if isinstance(action, str):
            return subprocess.CompletedProcess(argv, 0, action, "")
        return subprocess.CompletedProcess(argv, action[0], "", "")
    return run


@pytest.mark.parametrize("name", sorted(FOOTPRINT_FAILURES))
def test_a_real_footprint_failure_gives_no_credit(monkeypatch, name):
    engine = load_engine()
    ps, footprint, needle = FOOTPRINT_FAILURES[name]
    monkeypatch.delenv(ENV_VM, raising=False)
    monkeypatch.setattr(engine.subprocess, "run",
                        fake_subprocess(ps, footprint))
    resident, info = engine.vm_host_footprint()
    assert resident is None and needle in info["reason"], info
    monkeypatch.setattr(engine, "docker", FakeDocker(meminfo(24, 22)))
    credit = engine.vm_credit("c", 13.0, 24 * GB)
    assert credit["credit_gb"] == 0.0 and needle in credit["reason"], credit
    assert credit["guest_verified"] is True      # the guest reading was good


def test_the_real_footprint_path_gives_a_credit(monkeypatch):
    """The contrast: the same fake with a good report credits."""
    engine = load_engine()
    monkeypatch.delenv(ENV_VM, raising=False)
    monkeypatch.setattr(engine.subprocess, "run",
                        fake_subprocess(PS_OK, (0, footprint_report())))
    monkeypatch.setattr(engine, "docker", FakeDocker(meminfo(24, 22)))
    credit = engine.vm_credit("c", 13.0, 24 * GB)
    assert credit["reason"] is None, credit
    assert credit["vm_resident_gb"] == pytest.approx(19.0)
    assert credit["credit_gb"] == pytest.approx(13.0)


# B2: inputs that raised instead of giving credit 0

@pytest.mark.parametrize("guest,needle", [
    (UNDECODABLE, "docker exec failed"),
    (subprocess.TimeoutExpired(["docker"], 10), "docker exec failed"),
    ("MemTotal:       ²5165824 kB\nMemAvailable:   23068672 kB\n",
     "unreadable MemTotal"),
    ("MemTotal:       25165824 kB\nMemAvailable:   ٣ kB\n",
     "unreadable MemAvailable"),
    ("MemTotal:       25165824 MB\nMemAvailable:   1 kB\n",
     "unreadable MemTotal"),
], ids=["exec-non-utf8", "exec-timeout", "superscript-digit",
        "arabic-indic-digit", "wrong-unit"])
def test_an_undecodable_guest_reading_gives_no_credit(monkeypatch, guest,
                                                      needle):
    engine = load_engine()
    monkeypatch.setattr(engine, "docker", FakeDocker(guest, 24 * GB))
    credit = engine.vm_credit("c", 13.0, 24 * GB)
    assert credit["credit_gb"] == 0.0 and needle in credit["reason"], credit
    assert credit["guest_verified"] is False


def test_no_unexpected_exception_leaves_vm_credit(monkeypatch):
    engine = load_engine()

    def boom(container):
        raise RuntimeError("surprise")

    monkeypatch.setattr(engine, "guest_meminfo", boom)
    credit = engine.vm_credit("c", 13.0, 24 * GB)
    assert credit["credit_gb"] == 0.0
    assert credit["reason"] == "unexpected RuntimeError: surprise"
    assert credit["guest_verified"] is False

    def interrupt(container):
        raise KeyboardInterrupt

    monkeypatch.setattr(engine, "guest_meminfo", interrupt)
    with pytest.raises(KeyboardInterrupt):
        engine.vm_credit("c", 13.0, 24 * GB)


# N2: the guest reading must be the VM's own

@pytest.mark.parametrize("info_gb,trusted", [
    (24.0, True), (24.2, True), (30.0, False), (13.0, False), (None, False),
], ids=["equal", "within-1pct", "larger", "smaller", "docker-info-fails"])
def test_the_guest_memtotal_must_match_docker_info(sandbox, monkeypatch,
                                                   info_gb, trusted):
    """A container view of /proc/meminfo (lxcfs/sysbox, a replaced cat)
    differs from docker info's MemTotal: no credit, and no fit check on it
    (today's decision); within 1 % it is the VM's own."""
    engine = load_engine()
    fake = setup(sandbox, monkeypatch, host_gb=11, vm_gb=24,
                 guest=meminfo(24, 22),
                 info_total=None if info_gb is None else int(info_gb * GB))
    if info_gb is None:
        def broken(*args, **kwargs):
            if args[:1] == ("info",):
                raise subprocess.TimeoutExpired(["docker", "info"], 30)
            return fake(*args, **kwargs)
        monkeypatch.setattr(engine, "docker", broken)
    j = job(sandbox, f"memtotal-{info_gb}", 13.0, *CONTAINER)
    code = j.preflight()
    credit = j.vm_credit
    if trusted:
        assert code is None, sandbox.runlog_text()
        assert credit["guest_verified"] is True
        assert credit["credit_gb"] == pytest.approx(13.0)
    else:
        assert code == 3
        assert credit["guest_verified"] is False
        assert credit["credit_gb"] == 0.0
        assert ("differs from docker info" if info_gb is not None
                else "docker info MemTotal unknown") in credit["reason"]
        text = refusal(sandbox, f"memtotal-{info_gb}")
        if info_gb is None:
            # the container checks refuse as before, never a credit
            assert "host available memory 11.0 GB" in text, text
        else:
            assert "host available memory 11.0 GB + Docker VM credit " \
                   "0.00 GB" in text, text

    # an untrusted guest reading never refuses on the fit check either
    if info_gb == 13.0:
        sandbox.fake_free.write_text(str(40 * GB))
        fake.guest = meminfo(24, 5)        # "too busy", but not the VM's
        assert job(sandbox, "untrusted-fit", 13.0, *CONTAINER).preflight() \
            is None, sandbox.runlog_text()


# N6: the two behaviour changes, for a probe too

def test_a_container_probe_larger_than_the_guest_is_refused_outright(
        sandbox, monkeypatch):
    """A probe of an unmeasured case with no declared reserve reserves the
    whole budget (12 GB on the Mac test profile).  On a guest of 10 GB it is
    now refused outright, never waited for; on a busy 24 GB guest it waits."""
    from agent_compute import runner
    setup(sandbox, monkeypatch, host_gb=40, vm_gb=24, guest=meminfo(10, 9))
    j = job(sandbox, "probe-small", None, *CONTAINER, "--probe")
    assert j.preflight() == 3
    assert j.estimate_gb == 12.0
    text = refusal(sandbox, "probe-small")
    assert "does not fit in the Docker VM: guest MemTotal 10.0 GB < " \
           "estimate 12.00 GB" in text
    assert not any(k in text for k in runner.RETRYABLE_REFUSALS)

    setup(sandbox, monkeypatch, host_gb=40, vm_gb=24, guest=meminfo(24, 6))
    j = job(sandbox, "probe-busy", None, *CONTAINER, "--probe")
    assert j.preflight() == 3
    text = refusal(sandbox, "probe-busy")
    assert "Docker VM guest memory" in text
    assert any(k in text for k in runner.RETRYABLE_REFUSALS)
