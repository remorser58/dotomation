# Specialist routes

These recipes apply to an authorized assignment under the [common handoff](../.agents/skills/dots-superkit/SKILL.md) and [run/host/tool rules](dispatch.md#confirm-the-run-and-target). Use the current Dots catalog only within those boundaries. Return the inputs opened, observation, decision or output, and limits. A missing tool does not make a different kind of evidence equivalent.

### Engineering

| Route | What the operator actually does | Acceptance / safe fallback |
|---|---|---|
| programming | Trace the requested behavior from the real entry point, read neighboring implementation and tests, form one scoped change, verify the user-facing path and related regressions. | Name the candidate and check outputs. If implementation is not authorized, only explain or plan. |
| debugging | Reproduce or record the original failure, isolate the smallest observed difference, test one causal hypothesis, then check the corrected path and a control. | Separate reproduced cause, suspected cause and untested fix. Do not reproduce exploit payloads or intrusive activity. |
| refactor | Inventory callers, inputs, outputs and error paths. Preserve public behavior with characterization evidence while changing internals in bounded steps. | Check callers and equivalent outputs on the exact final candidate; behavior changes need separate approval. |
| review-work | Read the actual diff, related contracts and existing tests. Prioritize actionable regressions with path, scenario, impact and requested check. Reviewer does not edit their own review target. | Distinguish “no further finding in scope” from full coverage; list missing evidence, ignored areas and unresolved findings. |
| ast-grep | For a structural code question, use an already installed syntax-aware search only within allowed source paths. Inspect representative matches and near misses with the surrounding syntax. | If only text search is present, label it textual, sample false positives and do not claim parser coverage. No installation. |
| lsp-setup | Check the editor/project's observed language-service capability, project root and config; request an actual diagnostic, symbol reference or definition from the available interface. | Return the provider/operation actually observed or unavailable. Diagnose configuration read-only; installation, network or persistent configuration changes are separate work. |
| remove-ai-slops | Identify duplicated wrappers, dead branches or unsupported claims in the requested change. Propose removing only material shown unnecessary; for edits verify preserved behavior. | Do not “clean up” unrelated user changes, shorten for appearance alone or delete a public API without call-site evidence. |
| init-deep | Map actual repository roots, applicable instructions, entry points, existing tests, build and ownership boundaries. Start with top level and follow relevant paths, not unrelated home directories. | Report observed commands and unverified assumptions. Write repository guidance only when authorized; never spread this kit by editing consumers. |

### Design, data and evaluation

For a visual change, compare the actual render against the reference or stated intent: type, imagery and layout. On the web, exercise the flow at desktop and mobile widths and look for blank content, clipping and overlap. For 3D, inspect framing and occlusion from several angles. Keep interface copy the user requested or people need to understand and operate the surface. Record what was rendered on the exact candidate; if the required render is unavailable through an allowed route, record the visual check as NOT_RUN.

| Route | What the operator actually does | Acceptance / safe fallback |
|---|---|---|
| frontend | Define the audience, task, visual hierarchy, key states and viewport constraints. Reuse the observed design system. For authorized implementation, render the intended surface and inspect the actual main flow. | Test relevant viewport, focus/accessibility and loading/empty/error states. A spec or generated mock is not an implementation screenshot; no imported frontend corpora. |
| visual-qa | Pin the build and target viewport, render it, operate the target flow, inspect layout and relevant alternative states, and compare with intended behavior. | Record observed viewport/state, evidence and discrepancy. A DOM-only check cannot prove the screenshot looks right; if no rendering, mark visual QA NOT_RUN. |
| visualize | Choose whether the audience needs a relationship, process, distribution or interaction. Use an existing suitable chart/diagram/interactive surface and label units, controls, defaults and source. | Verify the result renders and the example matches its input; if only textual, report explanation not a rendered visual. |
| data-scientist | Establish entity grain, time window, exclusions, missingness, duplicates and comparison baseline. Compute the relevant measures and a small independent cross-check; distinguish association from causal inference. | Return values, units, denominators, source version and uncertainty. A synthetic dataset does not establish an effect on actual users. |
| eval-integrity | Define the capability and failure modes before scoring. Separate training/examples from evaluation, avoid leakage and cherry-picking, record denominators including skips and label judge independence. | Keep original per-case evidence, environment, sampling limits and disagreements; test success is not product impact. Do not approve your own evidence. |

### Research, interaction and continuity

| Route | What the operator actually does | Acceptance / safe fallback |
|---|---|---|
| web-browser (upstream directory: browser) | For an expressly authorized web interaction, use the approved surface only where dispatch permits it. Identify a control, act once, and reread the outcome. Work on a connected computer is native CLI only; a browser surface or connector is not an alternate route to it. | Surface, recipient and state must match. Do not switch accounts, bypass warnings or claim success on an unreadable page. For factual lookup use public research. |
| ultimate-browsing | Use authorized public search, primary pages and direct public links. Compare independent evidence; if extraction fails, try another authorized public representation. Any delegated model run still needs the common approved runtime. | Separate snippet, full content, date observed and coverage. No WAF, login or access-control bypass; do not invent access to private archives. |
| computer-use | Use permitted desktop interaction only where it meets dispatch's tool boundary and the task needs it. Inspect the current surface, choose a grounded control, act, and reread the result. Do not use desktop control as an alternative to native CLI for a connected computer. | If the required interaction cannot be observed or performed through the approved route, state what was not done. A mock or another host cannot fill that gap. |
| coding-agent-sessions | For an authorized historical question, read the relevant existing conversation/connector history, match actual IDs, dates and latest user decision, then verify any externally consequential state again. | Do not scan hidden session files, token storage or other accounts. Say if the required history is unavailable. |
| reflection | Review a finished attempt's verified artifacts, failed assumptions, alternative interventions and the latest owner's correction. Produce one concrete next-run improvement with an observable check. | Separate causal evidence from hindsight. No edits to global rules or private memory without authority. |
| memory-advisor | Identify whether a fact is stable preference, project-local choice or one-off constraint. Identify its source, audience and expected lifetime; propose the smallest destination and honor corrections. | Use only an already approved memory service and retention policy; otherwise recommend without storing. Do not preserve secrets, hidden history or speculative conclusions. |

Planning, goal work and adversarial review during any of these routes use the appropriate [workflow](goals.md), not a second harness.
