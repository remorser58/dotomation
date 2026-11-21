# Astra profile acceptance cards

Use a read-only task or approved existing artifact. These are example decisions, not passed runtime checks.

| Situation | Expected evidence or decision |
|---|---|
| A user asks about an Astra prompt, without selecting Astra for a task. | Answer the question. The profile is not invoked by mention. |
| A coordinator requests Astra Ultra for a concrete read-only comparison. | Worker reads the same kit SHA and exact Ultra brief; sends a result, evidence and limit, with no code edits or claim of independent approval. |
| A distinct xhigh reviewer receives a correct URL but a wrong digest, or a provider test supported only by a mock. | Read the pinned source, name the exact mismatch or missing evidence, record FAIL or NOT_RUN and notify both writer and coordinator without closing. |
| An authorized dispatcher cannot observe the model or effort metadata. | For a required model or effort mark assignment BLOCKED; otherwise distinguish a profile used as briefing guidance from verified model configuration. No forced fallback. |
| Both roles report for the latest artifact and all declared gates pass. | Coordinator independently verifies the current artifact and actual receipts; any larger, unrun native task remains out of scope. |
