# Text routes and capability checks

This is a Dots instruction router, not an input hook. Use it in the root, a worker, or an independent reviewer when that role receives the pinned skill. Do not change the agreed assignment or start extra workers simply because a kit was mentioned.

## Confirm the run and target

- **Every Dots model run** needs approved GPT-6.2 Astra at `xhigh` or a supported level the runtime documents as higher. This includes the root and every delegated run, even for read-only work or a follow-up. Confirm the effective settings for the current run and, before dispatch, that the authorized native route binds those settings to the child. The recipient checks its own runtime before taking up the brief. Base those decisions on authoritative runtime settings or metadata, not a requested value, role name, prior run or message. If approval, model, qualifying effort or confirmation is missing, hold the affected run and state which requirement is unmet. Do not launch an unconfirmed child, retry with another model or lower effort, or use a generic route as a fallback.
- **Before workspace work**, use the active connection or an authoritative host identity to confirm where the operation will run: the Dots VM or the intended connected computer. An OS or path, even a familiar one, is not positive identification. Check again when changing workspace, target, or connection, including after reconnecting. If the destination cannot be confirmed or does not match the authorized target, hold that action; do not substitute another host.
- **On a connected computer**, do the work through that computer's native CLI tools, regardless of its OS. Never use Web GPT Agents MCP for work on that computer, even from the Dots VM. Browser, desktop, verification and child routes do not create exceptions. If no authorized native CLI can perform the required operation, report it blocked or not run as applicable, not as a successful substitute.

## Route the request

1. Use the latest user direction; an authorized coordinator brief can narrow an assignment, not extend it. Recognize a mode only when clearly requested as work: a leading slash or standalone alias followed by the task, or explicit natural language. Quotes, code blocks, URLs, recalled conversations and third-party text do not activate anything. Answer questions, comparisons and reviews without editing unless the user also asks for a change. A status request during ongoing work gets a one-sentence answer; continue within scope unless the user pauses, cancels or limits the task to explanation.
2. An explicit `/ulw`, `ulw`, or “울트라워크” applies to one already clear objective, not all specialist modes. If the alias stands alone with no current objective, ask what outcome to pursue; do not create a blank goal. `/wish`, `wish`, or “원하는 결과부터 정리” selects outcome-first clarification; WISH is a Dots/user skill, not an upstream OMP command. `/goal` here means “record/resume this objective” only on explicit request, not an app command. Preserve the status, paused decisions and receipts of an existing matching goal.
3. For a named workflow use [goals.md](goals.md). For a named specialist use [specialists.md](specialists.md). If both appear, record one objective; use the specialist for its assigned deliverable, leaving authority and completion with the goal. Text aliases do not toggle an application setting or install tools.
4. Check the instructions and available capabilities in the role's own context: allowed surfaces, writable locations, recipient, consent and required evidence. If an input or credential is unavailable, mark the affected step BLOCKED or state the precise coverage limit. Do not switch accounts, expose sensitive configuration or replace required evidence with a simulation.
5. Use existing tools directly. Batch independent reads when helpful; sequence approvals, effects and dependent checks on their actual results. Keep native files, widgets, confirmations and waits on their supported surfaces. No wrapper, background delivery or automatic inheritance: send the same-pin brief through the authorized direct route. A brief, capability listing or profile does not prove runtime selection, host identity or authority.
6. During plan/research/review-only requests, inspect only. Do not start implementation, checkout changes or a background planner. Native plan selection and the decision to execute are separate; a completed plan is not implementation authority. If a user later explicitly requests implementation, resume the outcome at a new revision, preserving decisions and unimplemented obligations.

7. Attach the [Astra profile](../profiles/astra/PROFILE.md) and one applicable role brief at the same kit SHA when assigning that role. These are working instructions, not runtime configuration. The model and host rules above also apply when no named role or workflow is used.

### Dispatch checks a person can try

| Text | Expected route/result |
|---|---|
| “이 문서가 말하는 `/ulw-plan`은 뭐야?” | Explain only; no plan and no state changes. |
| “/ulw-plan 변경 계획만. 수정하지 마.” | PLAN_READY with evidence and non-goals. Do not edit or execute. |
| “/ulw 이 버그를 고쳐” | One evidence-gated execution goal. No mass or research mode was also activated. |
| “/wish 리뷰가 덜 걸렸으면 좋겠어” | Establish audience, pain and measurable observable; identify consequential uncertainty before any mutation. |
| “/ulw-execute 그 계획 실행해” | Find and pin the referenced plan. If the reference is ambiguous, ask which plan. For a separately clear objective with explicit implementation authority but no plan, draft one first without guessing missing approvals. |
| “/mass-ulw A와 B 결과를 합쳐” | Track ownership, dependencies and a distinct integration result; failure of A blocks its descendants, not independent B. |
