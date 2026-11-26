# Mode 2: `sub-dots` Agent Brief Template

Dots 서브에이전트(또는 서브에이전트를 띄우는 루트)에 붙여넣는 **2번 모드 (`sub-dots`)** 브리핑 템플릿입니다.

```text
Dotomation Mode 2: sub-dots (Reverse Bridge Harness) @ <kit commit SHA>
Agent ID: <agentId>
Dot / Project: <DOT> / <project>
Hub URL (or state dir on local): <SUBDOTS_HUB_URL or ~/.subdots>

Rules:
1. Do NOT route task instructions, questions, or results through the Dots root chat. Communicate directly with the local main session through `tools/subdots.py` (or the Reverse MCP endpoint `<hub_url>/mcp`).
2. First command: register with the hub to receive your fencing epoch and session token:
   python3 tools/subdots.py agent register <agentId> --dot <DOT> --project <project>
3. When waiting for work or after submitting a `result`/`question`/`block`, keep the loop alive without ending your turn:
   - Waiting: `python3 tools/subdots.py agent wait <agentId> --timeout 3300`
   - Returning work: `python3 tools/subdots.py agent reply <agentId> result <task> --re <msg_id> --ref <rev/* or local:path>` (blocks until the local session answers or releases you).
4. Run commands through the supervised runner so local steering (`steer --interrupt`) can redirect you mid-command:
   python3 tools/subdots.py agent exec <agentId> -- <command...>
5. Report progress at start, every material step (<=15m), blocked, and done:
   python3 tools/subdots.py agent step <agentId> <start|step|blocked|done> --task <task> --note <note> --evidence <evidence> --goal <goal> --doing <doing> --next <next> [--ack <msg_id>...]
6. Always inspect and handle `[STEERING_INPUT]` and `[HARNESS_INTERVENTION]` blocks printed at the top of any command/MCP output, and include `--ack <msg_id>` on your next call.
7. Never push directly to GitHub or open PRs from Dots sub-agents; hand off via `rev/*` commits or `local:<path>` in your `reply`. Never use Codex on any host.
```
