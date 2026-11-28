# Mode 2: `sub-dots` Protocol & Contract (`tools/subdots.py` v1.3)

## 1. Storage & Security Boundary

- **Owner-private directory:** `~/.subdots` (or `--state-dir` / `$SUBDOTS_STATE_DIR`), enforced mode `0700`, non-symlink, owned by current UID.
  - Strictly separate from any Dots-root-writable `.dots-ops/` folder and holds `subdots.sqlite`, `runtime.json` (`0600`), `hub.lock` (`0600`), `heartbeat.json` (`0600`), `hub.log` (`0600`), `sessions/`, `hook_state.json` (`0600`), and `hook_state.lock`.
  - **Contract privilege separation:** Owner contracts (`contracts` table, including `astra_role`, `read_only`, and `require_astra_ack`) can ONLY be mutated over the owner-private Unix control socket (`contract.set` / `contract.clear` / `subdots_set_contract`) — NEVER via HTTP `/agent/*` or Reverse MCP. Thus a sub-agent registering `--role worker` cannot weaken a contract pinned with `--astra-role reviewer --read-only`.
  - **Per-agent isolation:** `subdots.py provision <agentId>` issues scoped `/a/<agent_token>` URLs; paired with `--require-provision`, open global-token registration is disabled.
  - **Hook placeholders:** `{task}` and `{why}` are allowlist-sanitized before argv substitution and must never be interpolated into remote shell strings.
- **SQLite WAL ledger (`subdots.sqlite`, `-wal`, `-shm`, mode `0600`):**
  - Core tables: `agents` (tracks liveness, fatigue counters, `observed_dirty_json`, `observed_checks_json`, `astra_acked_epoch`, `astra_acked_sha`), `messages` (`require_readback`, `readback_text`), `events` (bounded 10,000-row retention), `exec_runs`, `contracts` (`require_astra_ack`, `read_only`, `astra_role`, `base_git_head`), and `checkpoints`.
  - Existing pre-v1.3 databases migrate online and idempotently in `Ledger.__init__`.

## 2. Astra v2 Subagent Harness Enforcement (`ASTRA_SOURCE_PIN = "0159023e"`)

1. **Canonical Profile Compilation (`astra_profile_bundle`):** Identical byte-for-byte in `tools/subdots.py` and `tools/dots.py`, normalizing role aliases into `worker` (`xhigh`), `reviewer` (`xhigh`, read-only), or `coordinator` (`ultra`) with a deterministic 16-hex SHA-256 over role, effort, mode, and Astra v2 behavioral rules (`subagent: true` scope isolation, no `/super` or `/superstack` inheritance, `Refusals are final`, 3-attempt reassessment, `EXECUTED` vs `TRACED` + `Not verified` gaps).
2. **Automatic Delivery Channels:**
   - **Reverse MCP `initialize`:** Emits `"instructions"` tailored to the bound agent's contract/registered role (when connected via `/a/<agent_token>/mcp`, else `worker`). Local stdio MCP emits `coordinator` `"instructions"` and exposes `subdots_astra_profile`.
   - **`agent_register`, `agent_resume`, `provision`:** Return `astra_profile` metadata and full `astra_profile_text` (`print_agent_context` & Reverse MCP content blocks); `provision --brief` renders a ready-to-paste role brief (redacted unless `--show-token`).
   - **Periodic `[CONTEXT_CAPSULE]` Refresh:** Every capsule carries `ASTRA_PROFILE:` compact summary + `astra_profile` status metadata so compaction cannot erase specialist rules.
3. **Per-Epoch Mechanical Gate (`ASTRA_PROFILE_ACK_REQUIRED`):**
   - When `hub start --require-astra-ack` or `contract.require_astra_ack` is enabled, every call to `agent_step`, `agent_reply`, `agent_checkpoint`, `agent_hibernate`, `exec_start`, `agent_gate_check`, `tool_read`, `tool_patch`, `tool_exec`, `tool_git_status` checks `astra_acked_epoch == agent.epoch` and `hmac.compare_digest(astra_acked_sha, expected_sha)`.
   - Unacked calls fail with `409 ASTRA_PROFILE_ACK_REQUIRED` (returning `required_profile_sha`, `astra_role`, full `astra_profile_text`, any pending `messages`, `capsule_text`, and `steering_footer`). Bad SHAs fail with `400 ASTRA_PROFILE_MISMATCH`.
   - Every `agent_register` / `agent_resume` / `local_provision_agent` bumps `epoch`, automatically re-requiring `--profile-sha` acknowledgement on reload/rollover.
   - Sub-agents may ACK via `agent ack <id> --profile-sha <sha>` (standalone or combined with `--ack`) or inline `--profile-sha` on `step`/`poll`/`heartbeat`/`checkpoint`/`hibernate`/`reply`/`exec`/`gate-check`/`tool_*`.
4. **Read-Only Reviewer Enforcement (`contract.read_only` or `contract.astra_role == "reviewer"`):**
   - Setting `--astra-role reviewer` automatically normalizes `read_only = true` in the contract (`--read-only` is implied even if omitted).
   - Rejects `tool_patch`, `agent_gate_check(action in ('tool_write', 'commit'))`, and unallowlisted command execution (`tool_exec`, `exec_start`, `agent_gate_check(action='exec')` whenever `allowed_cmd_prefixes` is empty) with `403 SCOPE_VIOLATION`, and rejects `agent_step(..., state='done')` (`409 SCOPE_VIOLATION`) whenever dirty or uncommitted/post-base files exist in the workspace (`scope_violation` events emitted).

## 3. Two-Phase Delivery, Hard Steering Gates & Done Proof

1. **Enqueue (`to_agent`):** Local session calls `send`, `steer`, `ask`, `pause`, `resume`, `cancel`, `release`, or stdio MCP `subdots_send`/`subdots_steer`/`subdots_control` (optionally with `require_readback=True` and idempotent `request_id`).
2. **Offer & Sandwich Injection:** Every sub-agent call collects pending messages (`queued -> offered`), stamps `[STEERING_INPUT]` with `AUTHORITY: OWNER-DELEGATED LOCAL SESSION`, injects `[CONTEXT_CAPSULE]` every `capsule_every_n` calls, and appends `[ACTIVE_STEERING_FOOTER]` after tool output when alerts remain.
   - **Anti-False-Refusal Guard (`autonomy_guard`):** Preserves all hard-stop exemptions (`git push`, PR, deploy, release, wipe/`.git/`/symlinks, spending, Codex, credentials/hosts, `SCOPE_VIOLATION`, untrusted prompt injection) and `question` inquiries; detects non-hard-stop excuses with unified per-window counters across `reply` and `step`.
3. **Read-Back Ack (`READBACK_REQUIRED`) & Hard Steering Gate (`STEERING_ACK_REQUIRED`):** Blocking unacked steering with `409 STEERING_ACK_REQUIRED` (`400 READBACK_REQUIRED` if `--readback` missing/short).
4. **Verified Done Gate:** `agent_step(..., state="done")` enforces `AGENT_PAUSED`, `AGENT_SPINNING`, `UNANSWERED_WORK`, `read_only` dirty check, `allowed_paths`/`forbidden_paths` (`SCOPE_VIOLATION`), and `required_checks` (`VERIFICATION_REQUIRED`).

## 4. Epoch Fencing, Phoenix Lifecycle & Tool Proxy

- Same epoch-fencing (`409 STALE_EPOCH`), planned Phoenix rollover (`rollover_ready` precedence + `rollover_stalled`), hibernation + `--idle-slot-after-s`, anti-spin (`SPIN_COMMAND_LOOP`, `SPIN_NO_PROGRESS`, `SPIN_POLL_THRASH`, `SPIN_START_CHURN`), gated Reverse MCP tool proxy (`--enable-tool-proxy` + `enable_tools` + `O_NOFOLLOW` + case-insensitive `.git/` protection + `argv` `shell=False`), and worktree-aware `install-git-hooks` guarantees as v1.2.

## 5. Control Socket, Local MCP & IDE/Subagent Hooks

- **Control Socket (`c.sock`):** `hub.status`, `hub.stop`, `hub.brief`, `agent.list`, `agent.provision`, `agent.status`, `contract.set`, `contract.get`, `contract.clear`, `message.send`, `message.await`, `message.inbox`, `message.mark_read`, `events.list`, `events.follow`, `events.await_any`, `control.*`.
- **Local Session Hooks (`subdots.py hook <session-start|pre-invocation|stop|doctor>`):** Fail-open `{}` on stopped hub, deduplicated `<subdots-status>` injection, bounded `MAX_HOOK_CONTINUATIONS=8` continuation.
- **Subagent Hooks (`subdots.py hook <subagent-start|subagent-pre-invocation|subagent-stop>` via [`subagent-hooks.json`](hooks/subagent-hooks.json)):**
  - `subagent-start` works with Hub up OR down (injects `<astra-subagent-profile>` + optional `<subdots-agent-status>` / `<dots-loop-status>` using `O_NOFOLLOW` reads).
  - `subagent-pre-invocation` deduplicates in `hook_state.json["subagents"]`, re-injecting `<astra-subagent-reminder>` on new steering, unacked Astra profile, rollover, or Mode 1 loop changes.
  - `subagent-stop` continues bounded turns (`MAX_HOOK_CONTINUATIONS=8`) when Mode 2 subagent owes ACK/steering/rollover or Mode 1 loop track remains active/ready/continuous.
