# Goal and workflow operations

Use the [same-pin handoff and operating rules](../.agents/skills/dotomation/SKILL.md) for the root and every child. Persist only in an already approved task record, not an unapproved consumer-repo install. Reuse the matching authorized goal; never reset a paused, cancelled or budget-limited goal to bypass its state.

## 1. Goal card

Record: stable goal/work ID, revision, owner’s outcome and non-goals, current authorization/exclusions, acceptance criteria with evidence classes (`EXECUTED` vs `TRACED` vs `UNVERIFIED`, plus explicit `Not verified` gaps), exact inputs, owners and read-only reviewers, dependencies (no cycles), remaining attempt budget (`<= 3` materially different approaches per blocker), pending effects and the next decision. On resume or after compaction, read actual artifacts/receipts and checkpoints (`dead_ends`), and distinguish applied effects from pending ones before continuing.

For a clear implementation request, start authorized work directly without adding planning ceremony; use `/super-plan` when the mode calls for one or a concrete decision, dependency or risk warrants it. For **WISH**, establish who needs what change, the observable outcome, and existing constraints/permissions; ask only about consequential unknowns or hard stops.

## 2. Specific modes

Canonical command names follow `omp-superkit@0159023e` **SUPER** (`super*`), while legacy `/ulw*` aliases remain accepted identically:

| Canonical route (legacy alias) | Operator action | Stop / output |
|---|---|---|
| `/super` (`/ulw`) | Start or resume one scoped objective; register criteria, choose owned work and checks, then execute. | Accept only evidence from the current candidate (`EXECUTED` vs `TRACED` vs `Not verified`). Bare `/super` does not activate every specialty. |
| `/super-plan` (`/ulw-plan`) | Read target, existing tests, and affected surfaces. Output only an authorized plan with alternatives, boundaries, dependency order, verification, and rollback. Ask an independent reviewer/critic to inspect it (`--read-only`). | `PLAN_READY` or `BLOCKED`. No implementation, PR, deploy, or automatic transition to execute. |
| `/hyperplan` | Explicit request only. Five distinct roles across Round 1 independent claims, Round 2 evidence-backed cross-critique, Round 3 revision/rebuttal, plus separate synthesis planner. | Never simulate unavailable agents or claim a shortened review was 3 rounds. |
| `/super-run` (`/ulw-execute`) | Pin the approved plan's exact revision (or bootstrap via `/super-plan` if authorized without plan). Assign disjoint writers (`allowed_paths`) and separate read-only reviewers. | Verified scoped deliverable, or precise blocker. A plan alone never approves pushing, merging to `main`, releasing, or spending. |
| `/super-loop` (`/ulw-loop`) | Keep one goal through diagnose, bounded attempts (`<= 3` distinct approaches per blocker; undo own interfering edits before switching/isolating), exact evidence, independent review, and decision. | Stop on verified outcome, `/stop-continuation`, missing capability/approval, resource limit, or exhausted budget. |
| `/super-graph` (`/mass-ulw`) | Parse a dependency DAG, give each node one writer with non-overlapping paths, pin upstream receipts, preserve independent ready lanes when another blocks (`loop block` / skip-blocked), and add an explicit integration node. | Failed/stale predecessor blocks dependent nodes only, never independent lanes. |
| `/super-research` (`/ulw-research`) | Define the question, competing explanations (including smallest discriminating check), scope/date, and evidence bar. Assign stable claim IDs, link supporting/contradicting observations and alternate-source checks, separate fact from inference, and have a separate reviewer challenge the top causal link. | Traceable findings (`EXECUTED`/`TRACED`) plus explicit `Not verified` / remaining unknowns. |
| `/superstack` (`/superstack off`) | Branch-local, outcome-driven skill router inside the current session/branch only. Composes relevant specialist skills directly for the active outcome **without** activating a `/super` goal or creating child subgoals. | Never inherited by child subagents (`subagent: true`); `/superstack off` deactivates branch routing immediately. |
| `/stop-continuation` / `/resume-continuation` | Pause or resume continuation cleanly (`dots.py loop pause/resume` in Mode 1; `subdots.py pause/resume` + local `hooks/` in Mode 2). | Preserves state & receipts without losing queue/checkpoint history. |
| `/wish` | Turn an ambiguous wish into a concise verifiable brief before activating the same evidence workflow. | Explicit outcome contract; `BLOCKED` only on consequential unknowns or hard stops. |

## 3. Child enforcement & gate adapter

When coordinating subagents under Dotomation:
1. **Isolate children (`subagent: true`):** Even if the coordinator uses `/super` or `/superstack`, never pass parent mode flags down as unbounded scope. Pin each worker/reviewer via the compiled **Astra v2 specialist profile**:
   - Mode 1: `dots.py loop init <DOT> <track> ... --astra-role <worker|reviewer|coordinator> --require-astra-ack` and `dots_pilot.py brief`.
   - Mode 2: `subdots.py hub start --strict-steering --require-astra-ack`, `subdots.py provision <agent> --brief`, `subdots.py contract set <agent> --astra-role reviewer --read-only` for reviewers, and `modes/sub-dots/hooks/subagent-hooks.json`.
2. **Freeze inputs & verify against real surfaces:** Require exact candidate commit/digest, post-edit `EXECUTED` test/check receipts (`required_checks`), and an independent read-only reviewer verdict (`APPROVED`, `CHANGES_REQUESTED`, or `BLOCKED` with `Not verified` gaps). Do not upgrade mocks, self-tests, or `TRACED`-only inspection to `PASS`.
3. **Preserve pause, cancellation, and refusal boundaries:** Do not restart paused/cancelled work on late results, and treat tool/host/user denials as final (`Refusals are final`). Return one decision: `PLAN_READY` / `EVIDENCE_COLLECTED` / `VERIFIED_SCOPE` / `BLOCKED` / `PAUSED` / `CANCELLED`.
