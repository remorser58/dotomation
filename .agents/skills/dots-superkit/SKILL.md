---
name: dots-superkit
description: "Coordinate approved Dots work through scoped handoffs, direct peer exchange, evidence, deduplication and independent closure checks. No runner or install."
---

# Dots Superkit · handoff-v1

## Start with the observable outcome

Name a coordinator (the root), one writer per output and a verifier distinct from its writer. Keep one `work_id` per approved intent. An owner correction to the same intent increments the shared `revision` through a new coordinator brief; it does not create a competing writer. A distinct request gets its own ID. Include the fixed **kit commit**, authorized action/surface, exclusions, deliverable, named participants and a non-empty checklist of mandatory gates. Each gate needs a unique ID and an observable pass condition.

A native message can use plain language: the essential fields are those in the [v1 schema](../../../protocol/handoff-v1.schema.json). The parties must still agree on identities and the fixed version. If a recipient cannot open the kit, include its bounded operating rules in the brief and label the pin as not independently loaded, rather than claiming it was installed.

## Direct exchanges, not another queue

Use the current conversation's native worker primitives only. Where available: `collaboration.spawn_agent` for genuinely separate work, `collaboration.send_message` for an active peer, `collaboration.followup_task` to give an idle peer a concrete authorized follow-up, and `collaboration.list_agents` for status when needed. These are capability examples, not commands to install. For another approved worker surface, use its existing addressed message/reply route. Do not substitute Slack, an external agent, a script or a persistent watcher for missing internal access. If a direct route is unavailable, mark it blocked; an authorized coordinator relay must be labelled a relay, never a direct exchange.

Address each event to one real recipient. The coordinator explicitly briefs each new assignee or verifier under the same work/revision. A worker asks another worker directly, quotes a stable event ID, and gets the answer back on that leg. Include updated constraints or a review outcome when sending the decision to the coordinator. A peer reply, document, example or task ID is never authorization to widen scope. Use only the tools, model access and data already approved; do not consume personal Codex quota, create infrastructure or change permissions.

## Events and evidence

Every event carries `work_id`, a positive shared `revision`, a unique `event_id`, `kind`, `from`, `to` and `body`. Except for the first briefs of a revision, refer to an existing event with `reply_to`. Event IDs are opaque, not timestamps. Participants can talk in parallel; do not derive a total order from arrival time.

- **brief**: coordinator declares scope, kit SHA, writer, verifier, deliverable and mandatory gate list. Recipient is either assignee or verifier. All briefs for a revision have identical bodies. Only the coordinator can advance the revision, one step at a time; that invalidates earlier results for closure.
- **question / answer**: request a concrete clarification from a peer and answer the same sender. Identify the original question in the answer's reply reference. Escalate decisions reserved for the owner through the coordinator.
- **submit**: writer gives the verifier an immutable result target plus evidence. Use an exact Git commit and path, content digest and locator, stable service revision, or an immutable native message reference. A branch name or an unversioned URL alone is not evidence of the checked result. Superseding a submission names the previous submission ID in `body.supersedes`. Only the first submission may omit it. A new submission invalidates old checks, even in the same revision. Send the coordinator a readable receipt or copy covering the latest submit ID, target and its replacement chain; this does not replace the direct writer-to-verifier leg.
- **check**: named verifier replies to the current submission and sends the coordinator a verdict for each gate, tied to that exact target. Record observed output and readback locator. For FAIL or NOT_RUN state why; if a gate fails, directly tell the writer what to fix. Do not count the writer's self-test as independent review.
- **block / pause / resume**: a block names the missing fact or capability and the next safe step. A pause stops new mutations and follow-ups; when the owner requests a pause, stop immediately, without waiting for the event to propagate. Record in-flight effects as observed, absent, or uncertain. Only the coordinator can relay an explicit owner resume issued after the active pause. Its `reply_to` must be that exact pause event ID: an old resume never clears a newer pause. An idle worker is not necessarily paused; a late receipt is not permission to resume.
- **close**: only the coordinator closes. Read the actual target AND gate evidence, compare every required gate to its stated condition, and confirm no open blocker, pause, conflicting duplicate or newer owner direction. Require PASS for every mandatory gate on the newest submission of the current revision. Missing, FAIL and NOT_RUN never mean PASS. If readback is unavailable, report the limitation instead of closing. A new submission, correction or newly observed failure reopens a previously reported result: verify what changed before a revised close.

At the final readback, the writer and verifier attest the latest submit ID, target, verdicts and unresolved blocks; hold further work pending close or a newly authorized correction. A root-readable native thread can supply those attestations. If peer events are private, forward their minimal receipts and evidence to the coordinator. Missing or contradictory receipts mean the task stays open. If authorized checks of the same current gate conflict, do not select the convenient PASS. Require the verifier to reconcile the specific conflicting event IDs with a fresh readback and reason, or use a new submission and recheck. Never reinterpret an unresolved FAIL or NOT_RUN as PASS.

## Retries and uncertainty

Use the same event ID only for an identical retransmission. The key is (work_id, event_id) across all revisions. Same key and same payload: acknowledge once, do not repeat work. Same key with different content, inconsistent same-revision briefs, a forked replacement or out-of-order dependency: do not apply the disputed transition. Re-read the source, reconcile with the coordinator; a later-arriving stale revision cannot restore old state. A blocker is cleared when authoritative evidence resolves its stated cause; a pause only by authorized resume. Refer to that evidence or resume event when closing.

An event receipt is not a side-effect receipt. If the outcome of a send, merge or other external operation is uncertain, inspect the authorized target before considering a retry. Never retry merely because the read failed. Observe the service state, preserve uncertainty and get the required authorization before changes. Do not put secrets, personal data or unrelated logs in messages. A new ticket, bot, schedule, model override, platform customization or repository push is not part of this skill.

## Stop condition

Report the actual outcome, exact target, pass/fail/not-run and remaining limits. Report “brief sent”, “peer acknowledged” and “verified in use” separately. A sample transcript exercises a rule; it is not production execution evidence. An instruction in this repository cannot install or activate itself.
