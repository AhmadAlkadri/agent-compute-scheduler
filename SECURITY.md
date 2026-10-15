# Security policy

## Supported versions

This is an alpha tool. Only the **latest release** receives security fixes.
A fix is released as a new version; installed versions are not edited in
place, and the installer's safety floor (`src/agent_compute/safety.py`)
prevents rolling back onto a version with a known false-safe defect.

## Reporting a vulnerability

Please report a vulnerability **privately**, through GitHub private
vulnerability reporting for this repository (the "Report a vulnerability"
button on the repository's Security tab). Do not open a public issue or
pull request for it.

Include the version (`agent-compute --version`, or the commit), the
platform, what you expected and what happened, and the smallest reproduction
you have. Do not include real credentials or personal data; synthetic ones
reproduce the problem as well.

This is a small project maintained by one person: expect an acknowledgement
within a few days, not hours, and a fix or a statement of the limitation
after the maintainer has reproduced the issue. Reports are credited in the
release notes unless you ask otherwise.

## What is in scope

ACS's purpose is to admit heavy jobs safely, so these count as
vulnerabilities:

- **A false clean**: a job reported as ended cleanly while the workload, or
  a process of it, is still running or cannot be proven gone.
- **A wrongful unblock**: a host block cleared, ignored or bypassed when the
  end of a job was ambiguous.
- **A fail-open admission**: a job admitted, started or replayed when a
  check could not be made, was unreadable, or failed; a second heavy job
  running beside the first; a request admitted twice.
- **A signal to an unproven identity**: ACS sending a signal to a process or
  container whose identity it has not proven (start time, or container id
  and job marker) to be the job's.
- **A credential leak**: a secret reaching a manifest, a log, the queue or a
  record.
- **A bypass of the guard's lock** or of the claim that the internal guard
  requires, or an option that lets an agent skip a safety check.

## What is not in scope

These are documented limits ([`docs/limitations.md`](docs/limitations.md)),
not vulnerabilities, though a report that makes one worse is welcome:

- A process that deliberately detaches from its job escapes tracking
  (cooperative tracking, not containment).
- ACS is not a security boundary against a hostile user of the same
  account, and it does not isolate a job's filesystem or network access.
- Credentials are screened by variable name only.
- Scheduling is FIFO, and ACS provides no fairness or accounting.
- Vulnerabilities in Pueue, Docker, Python or the operating system: report
  those to their projects.
