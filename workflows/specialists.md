# Specialist routes

These recipes apply to an authorized delegated assignment (`subagent: true`) under the [common handoff](../.agents/skills/dotomation/SKILL.md), [Astra v2 profile](../profiles/astra/PROFILE.md), and [run/host/tool rules](dispatch.md#confirm-the-run-and-target). Parent `/super` or `/superstack` sessions select these routes when needed, while delegated child subagents remain bounded to their assigned role, contract paths, and verification gates. Return `VERIFICATION` separating `EXECUTED`, `TRACED`, and `Not verified` limits.

### Engineering

| Route | What the operator actually does | Acceptance / safe fallback |
|---|---|---|
| programming | Trace requested behavior from real entry point, read neighboring implementation/tests, apply minimal root-cause edit without speculative wrappers or fallback guards, verify real flow + regressions. | Name candidate, `EXECUTED` checks, and `Not verified` gaps. Stay read-only if only explanation/plan is authorized. |
| debugging | Reproduce original failure, run smallest discriminating check when two explanations diverge, fix root cause; after `<= 3` distinct failures undo ONLY own interfering edits and check alternate sources. | Separate reproduced cause, `TRACED` hypotheses, and untested paths. Do not bypass denials. |
| refactor | Inventory callers, inputs, outputs, and error paths. Preserve public behavior with characterization evidence across bounded steps. | Verify callers and equivalent outputs on final candidate; behavior changes need separate approval. |
| review-work | Read actual diff, contracts, and tests as an independent read-only reviewer (`--astra-role reviewer --read-only`). Lead with severity-ordered findings (`[CRITICAL]`..`[LOW]`) + exact `file:line` and concrete failure scenario. | Report `APPROVED` \| `CHANGES_REQUESTED` \| `BLOCKED` + explicit `Not verified` list; never edit review target or manufacture minor nits when clean. |
| ast-grep | Use already installed syntax-aware search inside allowed paths; inspect matches and near misses cleanly. | If only text search exists, label it textual and sample false positives. No unrequested install. |
| lsp-setup | Inspect language-server capability, root, and config; run actual diagnostics/symbol references when exposed. | Report observed provider results or `Not verified` gap read-only. |
| remove-ai-slops | Identify duplicated wrappers, dead compatibility shims, and junk tests (prose-pinning, mock-echoes); verify preserved behavior after cuts. | Do not touch unrelated user work or drop tested public contracts without call-site proof. |
| init-deep | Map actual repository roots, instructions, entry points, and test commands from top level. | Cite observed paths/commands; write repository docs only when explicitly authorized. |

### Design, data and evaluation (`visual-qa` dual-oracle)

For any rendered visual/UI change, run **both** verification oracles on the actual built candidate:
1. **Functional oracle:** Interactive user flows, state transitions, focus/keyboard navigation, loading/empty/error states.
2. **Reference-fidelity / perceived-state oracle:** Rendered screenshot/DOM inspection matching typography, spacing, hierarchy, color contrast, and visual intent across desktop AND mobile viewports (and multiple camera angles/lighting for 3D renders), checking for clipping, overlap, occlusion, or blank surfaces.
If visual rendering cannot run through an allowed route, label visual fidelity `UNVERIFIED` / `NOT_RUN` — never infer visual cleanliness from passing unit tests or static CSS reads alone. Keep copy limited to user-requested text and clear operational UI labels; drop decorative filler.

| Route | What the operator actually does | Acceptance / safe fallback |
|---|---|---|
| frontend | Define audience, task, visual hierarchy, key states, and viewport bounds; reuse existing design system; render real surface and inspect primary + edge flows. | Verify desktop + mobile viewports, a11y/focus, loading/empty/error states (`EXECUTED`); list unrendered states as `Not verified`. |
| visual-qa | Pin build commit, render target viewports/angles, operate interactive flow, and audit rendered screenshots against functional oracle AND visual reference fidelity. | Record per-viewport findings (`EXECUTED` screenshot vs `TRACED` code vs `Not verified`). DOM/test-only checks never prove layout fidelity. |
| visualize | Choose chart/diagram/table fitting the data question; label units, controls, defaults, and source cleanly using existing tooling. | Verify live render matches exact input dataset (`EXECUTED`). |
| data-scientist | Pin entity grain, time window, exclusions, missingness, duplicates, and baseline; compute metrics + independent cross-check; separate correlation from causation. | Report values, units, denominators, source version, and uncertainty (`EXECUTED` vs `Not verified`). |
| eval-integrity | Define capability/failure modes before scoring; enforce train/eval split and leakage checks; keep denominators (including skips/timeouts) and judge independence explicit. | Preserve raw per-case receipts, deterministic test hygiene (no fixed sleeps unless testing time), and independence limits. |

### Research, interaction and continuity

| Route | What the operator actually does | Acceptance / safe fallback |
|---|---|---|
| web-browser (upstream directory: browser) | Use approved browser surface only where dispatch permits; act once and reread outcome. Connected computer work stays native CLI only. | Respect `Refusals are final`; never bypass logins, WAFs, or denials via alternate routes. |
| ultimate-browsing | Compare independent primary sources and public docs; check alternate public sources before concluding absence. | Separate direct fetch vs snippet, date observed, and `Not verified` gaps. |
| computer-use | Use permitted desktop automation only where dispatch allows; verify current window/element before acting and reread result. | Never use as a substitute for native CLI on a connected computer; record blocked surfaces accurately. |
| coding-agent-sessions | Search relevant existing history by exact session IDs, timestamps, and latest user decision, then verify live target state again. | Never scan private credentials/unrelated sessions. |
| reflection | Review verified artifacts, failed hypotheses, and owner corrections after a run; propose one concrete testable improvement. | Do not edit project/global rules unless explicitly requested by the owner. |
| memory-advisor | Classify durable preferences/constraints by source and scope; propose minimal storage without storing secrets or speculation. | Respect approved memory stores and explicit user corrections. |

Multi-track orchestration, continuation pause/resume, and branch-local `/superstack` routing use [goals.md](goals.md), not ad-hoc wrappers.
