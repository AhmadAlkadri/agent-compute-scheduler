# Dogfood log: format for consumer observations

Every consuming repository keeps one append-only ACS observation log,
`.agents/acs-dogfood.md`, written by its orchestrator (executors report to
the orchestrator; they do not write it). It is the consumer's half of the
dogfooding loop (`docs/consumer-template.md`, "The consumer's loop"; the
maintainer's half is to triage the log, `docs/operating-model.md`, "The
defect path"): the ACS maintainer triages it into a findings list, and
every entry ties an observation to the ACS version that produced it.

## Rules

- **Append-only.** Never edit or delete an entry. A later fact (a cause, a
  fix, a resolution) is a new entry, or an indented "Resolution (date,
  who)" paragraph under the original, that names the earlier entry by date
  and job id.
- **One entry per observation:** a friction, a surprise, a defect, an
  operator intervention, a workaround you were tempted to make, and routine
  successes too (a one-line entry: they are the baseline that makes the
  failures interpretable).
- **Never work around ACS to make an entry go away.** A refusal or wait
  that contradicts the declared policy is a defect report (skill: "Refusals
  and defects"): log it and send it.
- **Evidence by path, not by paste.** Name the job dir, `output.log`, the
  guard record, the `status --json` you saved; paste only the decisive
  line. No credentials, tokens or private keys, ever.
- **Session transcripts are not committed.** Name the session id prefix
  (for example `project-a-7d`, `project-b-c4`) so a later
  investigation can open them.

## Entry format

```text
- <UTC date or time> · <ACS version> · <job id(s)> (<request id>, <host>, <venue/container>) ·
  what happened: <observed facts, in order, with the fields read: phase, outcome,
  guard_outcome, exit_code, wait_reason, admission_memory> · expected: <what the
  skill, the project file or the declared numbers led you to expect> · evidence:
  <paths> · reported to: <maintainer session prefix | upstream PR #n | not reported:
  why> · session: <session id prefix> · status: <open | resolved (how, which
  version) | no action (why)>
```

| Field | Content | Where it comes from |
|---|---|---|
| date | UTC, at least the date; a time for anything on a shared host | the job's `accepted_utc`/`ended_utc`, or the clock |
| ACS version | the installed version name, e.g. `0.2.0-<sha10>` | the job view's `acs_version` (the version that accepted the job), else `agent-compute --version` at the time |
| job id(s) | every job involved, with its request id, host and venue | `status --json` (`job_id`, `request_id`, `host`, `workload`) |
| what happened | facts in order; the JSON fields you acted on | `status`/`wait` JSON, `output.log`, `reconcile --json` |
| expected | the rule or number behind the expectation, quoted with its source | the skill, `.agents/compute-scheduler.md`, `doctor` |
| evidence | paths that a reader can open | the view's `evidence` paths, saved outputs, `fetch` copies |
| reported to | who received it, or why it was not reported | the maintainer session's prefix; an upstream PR number |
| session | the session id prefix of the writer | the harness |
| status | `open`, `resolved` (with the version that fixed it), or `no action` (with why) | updated only by appending |

## Examples (illustrative, not real jobs)

```text
- 2026-10-09 14:02Z · 0.2.0-<sha10> · acs-20261009T135501Z-1a2b3c (p9:render-2, mac,
  container solver-box) · what happened: queued behind one job (job_ahead: another
  project, timeout_min 60), queue_wait_s 1730, then succeeded, exit_code 0,
  peak_rss_gb 2.7 · expected: as observed (FIFO) · evidence: the job's output.log ·
  reported to: not reported (routine) · session: project-a-xx · status: no action
  (baseline)
- 2026-10-09 15:40Z · 0.2.0-<sha10> · acs-20261009T151200Z-4d5e6f (p9:render-3, mac) ·
  what happened: held-inputs-changed, reason "inputs changed while queued (git_head)";
  the orchestrator had committed a docs change in the job's worktree while it was queued ·
  expected: the hold (our slip: the changed-input rule counts any commit) · evidence:
  saved status JSON outputs/P9/acs/4d5e6f.json · reported to: not reported (own slip) ·
  session: project-a-xx · status: no action; resubmitted as acs-…-7a8b9c
```

## Triage (the maintainer's side)

The ACS maintainer reads each consumer log on request or on a regular
schedule, and gives every entry either a row in its findings list or a "no
action" note, each with the version that produced the observation, the
evidence path and the reporter. The consumer's entry gets an appended
resolution line naming the row or the note.
