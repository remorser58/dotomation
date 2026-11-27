# Mode 2: `sub-dots` Protocol & Contract (`tools/subdots.py` v1.2)

## 1. Storage & Security Boundary

- **Owner-private directory:** `~/.subdots` (or `--state-dir` / `$SUBDOTS_STATE_DIR`), enforced mode `0700`, non-symlink, owned by current UID.
  - Strictly separate from any Dots-root-writable `.dots-ops/` folder and holds `subdots.sqlite`, `runtime.json` (`0600`), `hub.lock` (`0600`), `heartbeat.json` (`0600`), `hub.log` (`0600`), `sessions/`, `hook_state.json` (`0600`), and `hook_state.lock`.
  - **Contract privilege separation:** Owner contracts (`contracts` table) can ONLY be mutated over the owner-private Unix control socket (`contract.set` / `contract.clear` / `subdots_set_contract`) — NEVER via HTTP `/agent/*` or Reverse MCP.
  - **Per-agent isolation:** `subdots.py provision <agentId>` issues scoped `/a/<agent_token>` URLs; paired with `--require-provision`, open global-token registration is disabled.
  - **Hook placeholders:** `{task}` and `{why}` are allowlist-sanitized before argv substitution and must never be interpolated into remote shell strings.
- **SQLite WAL ledger (`subdots.sqlite`, `-wal`, `-shm`, mode `0600`):**
  - Core tables: `agents` (tracks liveness, fatigue counters, `observed_dirty_json`, `observed_checks_json`), `messages` (`require_readback`, `readback_text`), `events` (bounded 10,000-row retention), `exec_runs`, `contracts`, and `checkpoints`.
  - Existing pre-v1.2 databases migrate online and idempotently in `Ledger.__init__`.

## 2. Two-Phase Delivery, Hard Steering Gates & Done Proof

1. **Enqueue (`to_agent`):** Local session calls `send`, `steer`, `ask`, `pause`, `resume`, `cancel`, `release`, or stdio MCP `subdots_send`/`subdots_steer`/`subdots_control` (optionally with `require_readback=True` and idempotent `request_id`).
2. **Offer & Sandwich Injection:** Every sub-agent call (`register`, `resume`, `poll`, `step`, `heartbeat`, `reply`, `exec/*`, `checkpoint`, `tool/*`, or Reverse MCP `tools/call`) collects pending messages (`queued -> offered`), periodically injects `[CONTEXT_CAPSULE]` every `capsule_every_n` calls, and appends `[ACTIVE_STEERING_FOOTER]` after tool output when alerts remain.
3. **Read-Back Ack (`READBACK_REQUIRED`):** Acking any message marked `require_readback=1` without `--readback` (minimum 8 non-whitespace chars) fails with `400 READBACK_REQUIRED`.
4. **Hard Steering Gate (`STEERING_ACK_REQUIRED`):** When hub `--strict-steering` or `contract.strict_steering` is active (or a `require_readback` message is unacked), `exec_start`, `agent gate-check`, `tool_*`, and previously offered `step` reports fail with `409 STEERING_ACK_REQUIRED`, promoting queued messages to `offered` and returning `[STEERING_INPUT]` + `[ACTIVE_STEERING_FOOTER]` (HTTP/Reverse MCP responses also include `[CONTEXT_CAPSULE]`).
5. **Verified Done Gate:** `agent_step(..., state="done")` checks:
   - `409 AGENT_PAUSED` / `409 AGENT_SPINNING` if paused or spinning
   - `409 UNANSWERED_WORK` if any `task`/`steer`/`interrupt`/`question`/`review`/`intervention` remains unanswered
   - `409 SCOPE_VIOLATION` if any live `git status` or `base_git_head..HEAD` committed path (or `observed_dirty`) violates `contract.allowed_paths` / `contract.forbidden_paths` (also emitting `scope_violation`)
   - `409 VERIFICATION_REQUIRED` if any `contract.required_checks` lacks an exact passing (`exit_code == 0`) execution recorded at or after the latest mutation / dirty file or commit timestamp (self-reported `/agent/exec/finish` is not credited when `--enable-tool-proxy` / `enable_tools` is active).

## 3. Epoch Fencing & 3-State Phoenix Lifecycle

- **`register` / `resume`:** Bumps `epoch`, issues a fresh `session_token`, resets per-epoch fatigue counters (`epoch_tool_calls`, `epoch_output_bytes`, `epoch_started_at`), re-queues `offered` messages, and on `resume` (or whenever contract/checkpoint exists) injects the full `[CONTEXT_CAPSULE]` including cumulative `dead_ends` (`MAX_DEAD_ENDS=30`). Stale epochs always receive `409 STALE_EPOCH` (exit `5`).
- **Context Fatigue Rollover:** Crossing `rollover_max_calls`, `rollover_max_bytes`, or `rollover_max_age_s` emits `rollover_requested` and enqueues priority-8 `kind="rollover"` (`[ROLLOVER_PREPARE]`). Submitting `agent checkpoint --ready-for-rollover` sets `state='rollover_ready'`, emits `rollover_ready`, and invokes `--wake-cmd` with `why=PHOENIX_ROLLOVER`.
- **Hibernation & On-Demand Wake:** `agent hibernate` (refused if unanswered work exists) parks `state='hibernating'` (watchdog-exempt). Sending new steering to `hibernating`/`rollover_ready` re-arms `working` and triggers `--wake-cmd` (`why=WAKE_FROM_HIBERNATION`). Contracts with `idle_hibernate_s > 0` auto-enqueue `kind="hibernate"` during `watchdog_tick`.

## 4. Gated Reverse MCP Tool Proxy & Host Guards

- **Opt-in defense in depth:** `tool_read`, `tool_patch`, `tool_exec`, and `tool_git_status` reject all requests with `403 TOOL_PROXY_DISABLED` unless **BOTH** the Hub was started with `--enable-tool-proxy` **AND** the owner contract sets `enable_tools=True` with a valid non-symlink `workspace_root`.
- **Workspace path safety:** Rejects absolute paths, `..` traversal, backslashes, NUL bytes, symlink components/targets, case-insensitive `.git/` read/write access (`forbid_git_dir=True`), and `forbidden_paths`/`allowed_paths` mismatches (`403 SCOPE_VIOLATION`, with case-insensitive `forbidden_paths`), reading/patching via `O_NOFOLLOW` + `dir_fd` (`<= 512 KiB`) and scrubbing full file content prior to line-slicing/truncation without holding the SQLite lock across I/O.
- **Command policy:** Enforces workspace-contained `cwd` (walking directory segments with `O_NOFOLLOW`, forbidding `.git`), rejects `forbidden_cmds` regex matches (`403 COMMAND_FORBIDDEN`), validates `allowed_cmd_prefixes` without shell chaining/redirection metacharacters (`403 COMMAND_NOT_ALLOWED`), runs argv-only (`shell=False`) with a sanitized minimal child environment and live process-group kill on interrupt/timeout.
- **Git hooks (`install-git-hooks`):** Installs worktree-aware `# subdots-managed-hook` `pre-commit` (`agent gate-check --action commit`) and `pre-push` (hard block) contained inside the repository/common git directory, rejecting flag-like `SUBDOTS_AGENT` values and without storing Hub bearer tokens in `.git/hooks`.

## 5. Control Socket, Local MCP & IDE Hooks

- **Control Socket (`c.sock`):** Supports `hub.status`, `hub.stop`, `hub.brief`, `agent.list`, `agent.provision`, `agent.status`, `contract.set`, `contract.get`, `contract.clear`, `message.send`, `message.await`, `message.inbox`, `message.mark_read`, `events.list`, `events.follow`, `events.await_any`, `control.*`.
- **Local Hooks (`subdots.py hook <session-start|pre-invocation|stop|doctor>`):** Fail-open `{}` on stopped hub/invalid input, deduplicated sanitized `<subdots-status>` injection, and bounded (`MAX_HOOK_CONTINUATIONS=8`) `Stop` continuation when unread inbox / spin / dead / rollover alarms remain.
