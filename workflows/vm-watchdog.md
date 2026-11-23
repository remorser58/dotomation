# VM watchdog: checkpoint, backup anchor and reset recovery

An instruction-only port of the operator's watchdog kit. No script, daemon, schedule, hook, credential or telemetry is part of this repository, and nothing on this page runs by itself.

## Source of truth

The operator's own watchdog kit, outside this repository, on `local`:

- `RECOVERY.ko.md` — placement, the wake contract, pre-registration, checkpointing, external effects, manual replacement judgement.
- `PURE-REPLAY.ko.md` — side-effect-free stage replay, external anchors, the latest-mirror check, the 20-minute read contract.
- `CANARY.ko.md` — the actual process-loss canary and what it did not prove.
- `TOOLCHAIN.ko.md` — pinned Node, Playwright and Chromium rebuild steps.
- `REV-OPERATIONS-HANDOFF.ko.md` — static operations handoff.
- `recovery.py`, pinned SHA-256 `9aa963419b668c0b8f3924bdedba21b7fe62332329bba52978667c381f4042a3`, plus `drills.py` and the owner-only `ledger.sqlite`.

Read those files for exact command syntax; this page carries the decisions and limits. Do not copy the ledger, do not vendor the Python into a project, and do not run `drills.py` as part of ordinary work — it writes only inside that dedicated directory and belongs to its owner.

## Placement

VM first, then `local` ([hosts](hosts.md)). The VM is the only host whose state can vanish mid-step, which is why this procedure exists. Anything that must survive belongs on `local` or in the external anchor.

## Checkpoint before relying on VM state

1. Before the first command on a VM for a task, register in the ledger on `local`: job ID, approved scope, destination, input versions and hashes, acceptance criteria, last confirmed stage, exactly one next stage, deliverable and stop condition. If the task cannot survive losing the VM, hold it instead.
2. Do not fold a running user task into this procedure as a trial. The first real application is one new, side-effect-free, single-stage task.
3. Preserve only what is needed: at most 1 MiB of UTF-8 per capsule, 1–64 flat files, minimised and reviewed for sensitivity. Base64 is neither encryption nor redaction. Large results stay as a small reference spec — version, size, SHA-256 — plus an independent read of the real artifact; verifying the reference does not verify the original. Never copy a whole `.git`, home directory, credentials or user folders, and never reset, clean or stash the original to make a copy.
4. After each stage, move the result to `local` first, verify the user-visible result, then record the `checkpoint` with the stage hash and the single `next_stage`. Do not report progress from VM-local state alone.

## The backup anchor

- The anchor is a small external record of account, store UUID, job ID, revision, head and checkpoint SHA, kept outside the shared folder on `local`, at the root the operator registers for it: `<work root>/.dots-recovery/<DOT>/`. A second folder inside the shared folder is not an independent anchor.
- A historical anchor proves only that the value is contained in the ledger history. It is not the latest mirror. Accepting a mirror needs a single-writer window, `identity`, `status`, the external anchor read back independently, and a second `identity` and `status` read showing nothing moved; otherwise HOLD.
- Tokens and account sessions are never backed up.

## What a wake reads

Only these, in this order, through an approved route:

1. `sha256sum recovery.py` equals the pin. If it does not, stop.
2. `python3 -B recovery.py identity` returns the pinned store UUID `83df0c08-09cd-4466-a280-c6f3248b5178`.
3. `python3 -B recovery.py status` — ledger integrity, capsules, and each job's current revision and head.
4. `python3 -B recovery.py anchor JOB REV SHA` for the job's history check.
5. For a registered, active job only: the Dots environment list for that exact environment ID and state, then `python3 -B recovery.py poll STORE_ID ENV_ID SIGNAL` with an observed signal — `ready` or `connected`, `suspended` or `offline` or `unknown` or `provisioning`, `lost` or `failed` or `deleting`, or `absent` when a complete list omits the ID.

No returned value is execution authority. Empty registration and empty anchor means idle: start nothing. One side only is a mismatch. A stopped or cancelled job is not woken. A truncated list is read further; an error, an absence or a slow answer is not a VM replacement. If the timer stopped, or the ledger read, verification or anchor check failed, the result is UNKNOWN — not success, and not a licence to keep writing.

## Reset detection limits

- Dots exposes no official VM generation or reset event to this kit. The environment tools observed were list and attach, and attach can start or provision an offline environment, so it is not an inspection tool.
- Signals alone are not proof: the same environment ID, boot time, `boot_id`, PID 1 start tick, mount or PID namespace, overlayfs workspace, a missing marker or folder, a new thread ID, a dropped connection or a suspend. Identical facts do not prove the same generation either.
- A manual replacement judgement needs both a control-plane record showing a different environment and generation pair, and an independent job binding showing that the replacement environment was assigned to that job. Each is a single checked evidence capsule with a real source and reference. A SHA proves content integrity, not the truth of the source.
- Never call a stage replay a reset detection, and never describe a canary process loss as a VM replacement.

## Resume only the next stage

- After a confirmed checkpoint, resume exactly one `next_stage` in a fresh, empty, approved target: recover only the needed files, compare hashes, re-confirm authentication, network, runtime version and scope, skip the last completed stage, run the next one, verify the real user path, store the artifact and the external evidence, then record `complete`.
- Stop if the target already holds work, uncommitted changes exist, or the base, authority or scope changed.
- A late or duplicated attempt never becomes the current result. A revision mismatch means reading the remote state first; never re-run to resolve a read failure.
- Replacement is capped at two per job, with a 30-minute wait before the second. On the cap, a clock regression or a repeating cause, stop. Do not mint a new job ID to get around a limit.

## No automatic external writes

- A VM never performs an external write without owner approval, independent destination readback and idempotency. Commit the intent before the write, use one idempotency key from the start, and confirm at the destination — remote ref and SHA, PR number and state, service receipt — by reading it, not from a response, a cache or a timeout.
- Already applied: record a `receipt` bound to that operation and destination; do not resend. Not visible or not provable: keep the intent, do not write, retry or send through another channel, and hold for manual review. ABSENT is not permission to resend. Without destination idempotency and a definitive read, exactly-once cannot be promised.
- `init`, `stage`, `record`, `preview`, attach and resume, creating jobs or sessions, and Git, PR, message, deploy or API writes are never automatic. A missing parent readback never creates a new task.

## Live records and the reset suspicion

When this procedure applies, the affected agent appends `state: "blocked"` with `note: "vm reset suspected"` and the observation as `evidence` ([live status](live-status.md)). That is a suspicion, not a finding: the coordinator keeps the job on HOLD, does not reassign the VM work to another host, and records the next safe step. A 30-minute live-record gap is a separate stall check; neither replaces the other. Recovery resumes only from the last confirmed checkpoint, and only for the next stage.

## Every dot
The procedure is shared; the accounts are not. Each dot keeps its own job IDs, ledger entries, store UUID confirmation and external anchor subfolder. Never merge, overwrite or borrow across accounts, and never import another dot's checkpoint as your own. A dot with no registered job has nothing to recover: it starts new work under the normal placement rules.

## Not included

No resident watcher, no schedule of this repository's own, no 24/7 duplication, no automatic attach, no backup of secrets, no external copy installed by this kit. The 20-minute read contract in `PURE-REPLAY.ko.md` is the operator's schedule and must be confirmed in Dots before anyone calls it running.
