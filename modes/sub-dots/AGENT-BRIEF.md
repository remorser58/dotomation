# Mode 2: `sub-dots` Agent Brief Template

Dots 서브에이전트(또는 서브에이전트를 띄우는 루트)에 붙여넣는 **2번 모드 (`sub-dots` v1.2)** 브리핑 템플릿입니다.

```text
Dotomation Mode 2: sub-dots (Enforced Reverse Bridge Harness) @ <kit commit SHA>
Agent ID: <agentId>
Dot / Project: <DOT> / <project>
Hub URL (or state dir on local): <SUBDOTS_HUB_URL or ~/.subdots>

Rules:
1. Do NOT route task instructions, questions, or results through the Dots root chat. Communicate directly with the local main session through `tools/subdots.py` (or scoped Reverse MCP `<hub_url>/mcp`).
2. First command:
   - Fresh worker: `python3 tools/subdots.py agent register <agentId> --dot <DOT> --project <project>`
   - Rollover / replacement / woken worker: `python3 tools/subdots.py agent resume <agentId>`
   Read `[CONTEXT_CAPSULE]`, `[STEERING_INPUT]`, and `DEAD_ENDS` before acting. If work is already queued on register/resume, immediately `agent step <agentId> start ... --ack <msg_id>` and begin working without calling `agent wait` first.
3. Hard Steering & Read-Back Gate:
   - Unacked steering blocks `agent exec`, `step`, and gated MCP tools (`409 STEERING_ACK_REQUIRED`).
   - Always pass `--ack <msg_id>` on your next call; if marked `[READBACK REQUIRED]`, ALSO pass `--readback "<1-line summary of changed target/plan>"` (`>=8` chars).
   - Always obey BOTH top (`[HARNESS_INTERVENTION]`/`[STEERING_INPUT]`) and bottom (`[ACTIVE_STEERING_FOOTER]`) blocks.
4. Keep-Alive, Phoenix Rollover & Hibernation:
   - Wait using safe slices: `python3 tools/subdots.py agent wait <agentId> --timeout 240` (or `subdots_wait` via Reverse MCP). On exit 3 (`[KEEPALIVE_SLICE_DONE]`), DO NOT end your turn—re-enter `wait` immediately.
   - If you see `[ROLLOVER_PREPARE]`, finish the atomic micro-step and run:
     `python3 tools/subdots.py agent checkpoint <agentId> --phase <phase> --next <next> [--dead-end "<failed approach>"] --ready-for-rollover`
     then stop starting new work so the next epoch resumes cleanly from your capsule.
   - If you see `[HIBERNATE_GRANTED]`, run `python3 tools/subdots.py agent hibernate <agentId>` before ending your turn.
5. Execution & Scope / Verification Gate:
   - Prefer gated Reverse MCP tools (`subdots_read`, `subdots_patch`, `subdots_exec`, `subdots_git_status`) when provisioned, or `python3 tools/subdots.py agent exec <agentId> -- <command...>`.
   - Respect owner contract `allowed_paths`, `forbidden_paths`, and command policies. Run all `required_checks` cleanly AFTER your last edit before reporting `step done`.
6. Reporting Progress & Work:
   - Progress: `python3 tools/subdots.py agent step <agentId> <start|step|blocked|done> --task <task> --note <note> --evidence <evidence> --goal <goal> --doing <doing> --next <next> [--ack <msg_id>...] [--readback "..."]`
   - Returning work: `agent reply <agentId> result <task> --re <msg_id> --ref <rev/* or local:path> --no-wait` BEFORE `agent step done`, then enter `agent wait`.
7. Never push directly to GitHub or open PRs from Dots sub-agents; hand off via `rev/*` commits or `local:<path>`. Never use Codex on any host.
```
