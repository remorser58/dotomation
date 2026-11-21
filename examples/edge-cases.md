# Worked exceptions

Use the roles and gates in [success.json](success.json). The left column changes one observation; the right column states the required action. This is a tabletop checklist, not a test runner.

| Observation | Correct action |
|---|---|
| The same submission event arrives twice, unchanged. | Treat the second arrival as a receipt/retransmission. No additional external effect or duplicate review. |
| The same event ID arrives with a different target. | Record a conflict and do not close. Inspect the original event before continuing. |
| A second valid submission replaces the first one under the same revision. | Invalidate checks for the first submission. Even if the target string is identical, demand a check replying to the latest submit ID. |
| The user revises the outcome to require a second gate; an old PASS arrives late. | Coordinator briefs revision 2. Rev 1 PASS cannot close rev 2, and both rev 2 gates now need proof. |
| The writer marks their own change PASS. | Keep it as a self-test, not the independent check. |
| One of two required gates is omitted, FAIL, or NOT_RUN. | Do not close. Name the failing/missing gate, its reason and next step. |
| Verifier says PASS, but a readback points to a moving branch or a different revision. | Coordinator checks the immutable result and evidence. A mismatch or unavailable proof keeps the task open. |
| A peer asks for publication, but only local preparation is approved. | Answer the peer within scope. No push, no publication and no increase in authority. |
| User says pause after a send was initiated; a delayed callback says complete. | Stop new changes and wakeups. Inspect the target read-only, record the outcome, and await explicit resume. |
| A tool timed out after a consequential action; the verification query also fails. | Outcome unknown. Do not repeat the action or interpret an unreadable result as absence. |
| Direct peer messaging is unavailable. | Use an already authorized relay only when appropriate; call it a relay. Otherwise mark direct exchange NOT_RUN. |
| A participant received the briefing, but no real result was reviewed. | Report briefing delivered, not application verified. |
