# Astra profile acceptance cards

These are synthetic decisions, not runtime receipts. Apply the [same-pin dispatch rules](../workflows/dispatch.md#confirm-the-run-and-target) for each role; do not launch a model to exercise these cards.

| Situation | Expected evidence or decision |
|---|---|
| In an eligible Dots run, a user asks about an Astra prompt. | Answer without invoking work. Mentioning a profile does not configure another run. |
| Root and child use approved settings. Native metadata confirms the root runs GPT-6.2 Astra at `xhigh`, and the native child assignment binds Astra at a documented higher effort for a read-only comparison. The child confirms its own settings. | Use the pinned Ultra brief, do the comparison within scope, and report observations and limits. Ultra did not select the settings; the native confirmation is what matters. |
| A distinct xhigh reviewer receives a correct URL but a wrong digest, or a provider test supported only by a mock. | Read the pinned source, name the exact mismatch or missing evidence, record FAIL or NOT_RUN and notify both writer and coordinator without closing. |
| The REM root is eligible, but the reviewer route offers only lower effort or another model; a second route cannot show the child's effective settings. | Hold the reviewer assignment before launch and state the missing requirement. No alternative model, lower effort, unconfirmed launch or writer self-review. The independent gate cannot pass. |
| The child receives a pinned role and a requested `xhigh` value, but its own runtime evidence is missing or contradicts the brief. | Hold work and report the discrepancy. Do not treat the coordinator's statement, an old run or the xhigh filename as proof. |
| An eligible reviewer cannot open the pinned kit. | Coordinator includes the applicable handoff, role and dispatch rules in the authorized direct brief. Reviewer marks the pin not independently loaded, confirms their own target and tools, and does only what the evidence and authorization allow. |
| Both roles report for the latest artifact and all declared gates pass. | Coordinator independently verifies the current artifact and actual receipts; any larger, unrun native task remains out of scope. |
