# Mode 2: `sub-dots` Agent Brief Template

Dots 서브에이전트(또는 서브에이전트를 띄우는 루트)에 붙여넣는 **2번 모드 (`sub-dots`)** 브리핑 템플릿입니다.

```text
Dotomation Mode 2: sub-dots (Reverse Bridge Harness) @ <kit commit SHA>
Agent ID: <agentId>
Dot / Project: <DOT> / <project>
Hub URL (or state dir on local): <SUBDOTS_HUB_URL or ~/.subdots>

Rules:
1. Do NOT route task instructions, questions, or results through the Dots root chat. Communicate directly with the local main session through `tools/subdots.py` (using `SUBDOTS_HUB_URL` / `--hub-url` or Reverse MCP `<hub_url>/mcp`).
2. First command: if not pre-provisioned with explicit `--epoch`/`--session-token`, register with the hub to obtain fencing credentials:
   python3 tools/subdots.py agent register <agentId> --dot <DOT> --project <project>
   (Note: `agent register` already prints queued tasks via `[STEERING_INPUT]` piggyback. If a task arrives on register, run `agent step <agentId> start ... --ack <msg_id>` and begin working immediately without calling `agent wait` first.)
3. When waiting for work (no queued task received on register) or after `step done` while awaiting owner `answer`/`release`, keep the loop alive without ending your turn:
   - Waiting: `python3 tools/subdots.py agent wait <agentId> --timeout 3300` (exit 0=mail, 3=timeout, 4=signal, 5=stale epoch)
   - Returning work: send `agent reply <agentId> result <task> --re <msg_id> --ref <rev/* or local:path> --no-wait` BEFORE `agent step <agentId> done` (to clear the Done-Gate), then run `agent step done`, then `agent wait` (or pass without `--no-wait` after `done` if all owed mail is already answered).
   - Lease / Ack helpers: `agent ack <agentId> <id...>` and `agent heartbeat <agentId>`.
4. Run commands through the supervised runner so local steering (`steer --interrupt`) can redirect you mid-command (exit 130=interrupted, 5=stale epoch):
   python3 tools/subdots.py agent exec <agentId> -- <command...>
5. Report progress at start, every material step (<=15m), blocked, and done:
   python3 tools/subdots.py agent step <agentId> <start|step|blocked|done> --task <task> --note <note> --evidence <evidence> --goal <goal> --doing <doing> --next <next> [--ack <msg_id>...]
6. Always inspect and handle `[STEERING_INPUT]` and `[HARNESS_INTERVENTION]` blocks printed at the top of any command/MCP output, and include `--ack <msg_id>` on your next call.
7. Never push directly to GitHub or open PRs from Dots sub-agents; hand off via `rev/*` commits or `local:<path>` in your `reply`. Never use Codex on any host.
```
