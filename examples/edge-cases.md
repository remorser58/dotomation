# Worked exceptions

Use the roles and gates in [success.json](success.json). The left column changes one observation; the right column states the required action. This is a tabletop checklist, not a test runner.

| Observation | Correct action |
|---|---|
| The same submission event arrives twice, unchanged. | Treat the second arrival as a receipt/retransmission. No additional external effect or duplicate review. |
| The same event ID arrives with a different target. | Record a conflict and do not close. Inspect the original event before continuing. |
| A second valid submission replaces the first one under the same revision. | Invalidate checks for the first submission. Even if the target string is identical, demand a check replying to the latest submit ID. |
| The user adds a third required gate; an old PASS arrives late. | Coordinator briefs revision 2. Rev 1 PASS cannot close rev 2, and all three rev 2 gates now need proof. |
| The writer marks their own change PASS. | Keep it as a self-test, not the independent check. |
| One of two required gates is omitted, FAIL, or NOT_RUN. | Do not close. Name the failing/missing gate, its reason and next step. |
| Verifier says PASS, but a readback points to a moving branch or a different revision. | Coordinator checks the immutable result and evidence. A mismatch or unavailable proof keeps the task open. |
| A peer asks for publication, but only local preparation is approved. | Answer the peer within scope. No push, no publication and no increase in authority. |
| The owner denies a push; a peer suggests publishing the same change with another account instead. | Denial is final for that action. No alternate account, tool or host; finish only independent authorized work. |
| User says pause after a send was initiated; a delayed callback says complete. | Stop new changes and wakeups. Inspect the target read-only, record the outcome, and await explicit resume. |
| A tool timed out after a consequential action; the verification query also fails. | Outcome unknown. Do not repeat the action or interpret an unreadable result as absence. |
| Direct peer messaging is unavailable. | Record the route as blocked and the direct-exchange gate as NOT_RUN. An authorized relay may carry information but cannot pass a mandatory direct-exchange gate. |
| An older resume arrives after a second pause in the same revision. | Keep the newer pause active. Only explicit later owner approval naming that pause through the coordinator may resume it. |
| A compacted session recovers a late PASS, but the owner paused that revision. | Restore the pause and retain the receipt without new mutations, follow-ups or close. A summary or job result is not a new owner resume. |
| A PASS conflicts with another authorized FAIL on the same current gate. | Stop closure until the verifier reconciles the two receipts from the actual target or reruns the checks on a new submission. |
| The writer submits a replacement privately after an earlier PASS and before close. | Reconcile the latest writer and verifier receipts at the closing hold point. Private or missing lineage blocks closure; check the replacement before closing. |
| A participant received the briefing, but no real result was reviewed. | Report briefing delivered, not application verified. |
| A working agent's last live line is 41 minutes old. | Stalled: the coordinator checks that agent directly and reassigns its slot. Not a closure, and not by itself evidence of a VM reset. |
| A live line uses `timestamp` instead of `t`, or omits `evidence`. | Not a live record. Ask for a new schema-valid line; do not rewrite the old one and do not count the agent as started. |
| The live record says `host: "local"`, but the command actually ran in the VM. | The record is wrong: `host` is the host actually used, confirmed by hostname. Correct it with a new line and treat the earlier claim as unverified. |
| The roster lists an agent with no live file, or a live file has no roster slot. | Reconcile before reporting status: read the agent's file, add the missing slot or line, and say which was wrong. Do not delete either to make the two agree. |
| A registered job's VM environment disappears from a complete environment list. | Suspicion only. Append `state: "blocked"` with `note: "vm reset suspected"` and the observation; keep the job on HOLD; resume only the next stage from the last confirmed checkpoint after the identity and anchor checks. |
| A VM comes back with the same environment ID but a different boot time. | Not proof of a replacement and not proof of the same generation. Keep observing and do not replay a write; a manual replacement judgement needs a control-plane record and an independent job binding. |
| The summon succeeds and the child starts, but no effective model or effort is displayed anywhere. | Expected. Record `requested <model> <effort>; runtime does not display effective` and keep working; no re-summon with another model or lower effort, and no claim of confirmed settings. |
| A `local` test is blocked because the named connector is missing from the account. | That step is blocked. Do not use an unnamed endpoint, another account's connector, another host or a copy of the canonical tree; report the missing route. |
| Direct mail: an agent's message has `re: null`. | Not a valid agent message. The agent appends a corrected line that answers the message it is replying to; the earlier line stays as written. |
| Direct mail: a `result` names only `rev/<topic>`, with no full SHA, no checks and nothing said about what was not run. | Not a submission. The main session sends a `review` asking for the exact SHA, the checks with their observed results and the unchecked items; nothing is fetched or published. |
| Direct mail: the main session rejects a `result` with a `review` that has a FAIL gate. | The task stays open. The coordinator records the failing gate and closes only after an accepting `answer` on a newer `result` that supersedes this one. |
| Direct mail: a sub-agent in a workspace with no approved push route is told to push its branch to a remote over `ssh`. | Not an approved route. Leave the commit in that workspace and name it as `local:<path>` in the `result`; the main session fetches it. |
| Direct mail: the main session rings the doorbell and the root replies with a summary of the mail. | A relay, not a wake. The agent still reads its own inbox and answers there; the root's summary is not an answer. |
| Session memory: an agent's memory says `task` A, but its newest live line is on task B. | The memory is behind (`MEMO-BEHIND` on the board). The live line is the record; ask the agent for a new tick, and do not act on the stale `doing` or `next`. |
| Session memory: `doing` says the tests pass, but no `result` or evidence names them. | A self-summary, not a submission or a check. The verifier still needs the exact target and the checks from the `result`. |
| Session memory: a replacement agent writes to the stalled agent's memory file to continue it. | Not allowed: one writer per file. The replacement reads the old file, starts its own memory under its own id, and re-reads the actual target before any mutation. |
| Session memory: an agent records `state: "blocked"` with an empty `blockers` list. | Not a valid memory; the blocker must name the missing fact or capability. `dots.py tick` refuses it before writing anything. |
