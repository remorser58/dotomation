# Mode 2: `sub-dots` Protocol & Contract (`tools/subdots.py`)

## 1. Storage & Security Boundary

- **Owner-private directory:** `~/.subdots` (or `--state-dir` / `$SUBDOTS_STATE_DIR`), enforced mode `0700`, non-symlink, owned by current UID.
  - Strictly separate from any Dots-root-writable `.dots-ops/` folder so a Dots root cannot forge hub state, wake commands, or delivery markers.
  - **Same-UID vs multi-agent isolation:** Processes running under the same OS UID share access to `c.sock`, `runtime.json`, and `sessions/<agent>.json`. When running untrusted sub-agents under separate OS users or containers/VMs, use `subdots.py provision <agentId>` to issue per-agent `/a/<agent_token>` URLs (optionally paired with `hub start --require-provision` to disable open global-token registration) so one sub-agent cannot impersonate or bump another agent's epoch.
  - **Hook placeholders:** `{task}` and `{why}` are allowlist-sanitized to single non-whitespace tokens (`[^A-Za-z0-9._:/@+,=-]+` -> `_`, no leading `-`) before argv substitution and must never be interpolated into remote shell commands (e.g., `ssh ... sh -c`) or script interpreters (`osascript -e`); pass them strictly as standalone argv items.
- **`runtime.json` (`0600`), `hub.lock` (`0600`):** Written atomically / flocked with `O_NONBLOCK` + `O_NOFOLLOW`. `hub.lock` prevents concurrent Hubs on the same state directory. Holds `pid`, `socket_path`, `http_host`, `http_port`, `token`, `http_url`, `mcp_url`, `public_host`, `ops_base`, `require_provision`.
- **Unix control socket (`c.sock`, `0600`):** Uses `<state_dir>/c.sock` when UTF-8 path length is `<= 96` bytes, else `/tmp/sd-<sha256[:8]>-<random>/c.sock` inside an unpredictable `0700` `mkdtemp` dir (preventing macOS `ENAMETOOLONG` and shared-`/tmp` pre-creation races).
- **SQLite WAL ledger (`subdots.sqlite`, `-wal`, `-shm`, mode `0600`):** `journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout=5000`, serialized through a single `RLock` without holding the lock across `Condition.wait()` or subprocess calls. Events preserve redacted payloads with bounded 10,000-row retention.

## 2. Two-Phase Guaranteed Delivery & Piggyback

1. **Enqueue (`to_agent`):** Local session calls `send`, `steer`, `ask`, `pause`, `resume`, `cancel`, `release`, or stdio MCP `subdots_send`/`subdots_steer`/`subdots_control`.
   - Optional `request_id` enforces idempotency via SHA-256 `payload_hash` (identical retry returns existing receipt; mismatched payload raises `409 REQUEST_ID_CONFLICT`).
2. **Offer (`queued -> offered`):** Any sub-agent call (`register`, `poll`, `step`, `heartbeat`, `reply`, `exec/start`, `exec/heartbeat`, `exec/finish`, or Reverse MCP `tools/call`) collects pending `to_agent` messages ordered by `(priority DESC, created_at ASC)` and marks them `offered`. Agents receiving a task on `register` should immediately run `agent step ... start --ack <id>` and start working without first blocking in `agent wait`. Additionally, polling while in `completed` state (e.g., awaiting coordinator `answer`/`release` after `step done`) preserves `completed` rather than reverting to `waiting`.
3. **Re-offer & Escalation:** If an `offered` message is not ACKed within `reoffer_s` (default `45s`), the next call re-offers it up to `max_offers` (default `5`). Exceeding `max_offers` marks it `unacked_failed` and triggers `--notify-cmd` (still recoverable via `--ack` or `reply --re`).
4. **Ack (`offered -> acked`) & Answer (`answered`):** Passing `ack_ids` promotes `queued`/`offered`/`unacked_failed` messages to `acked`. Calling `agent reply ... --re <id>` promotes that message to `answered`.
5. **Done Gate & Spin Lockout:** `agent step ... done` returns `409 UNANSWERED_WORK` (exit `2`) while any `task`, `steer`, `interrupt`, `question`, `review`, or `intervention` message lacks an answer (`queued`, `offered`, `acked`, or `unacked_failed`). `agent exec` refuses execution (`409 AGENT_SPINNING`) while the agent is in `spinning` state or has a pending intervention.

## 3. Epoch Fencing

- Every `agent register` increments `epoch` and issues a fresh 48-hex `session_token`, re-queueing any unacked `offered` messages for the new session.
- All subsequent agent calls validate `(epoch, session_token)` in constant time; stale callers receive `409 STALE_EPOCH` (exit `5`).

## 4. Anti-Death Lease Watchdog

- Agents renew `lease_expires_at` (`now + lease_ttl_s`, default `45s`) on every call and continuously while holding `/agent/poll` (`active_poll_count > 0`).
- When `active_poll_count == 0` and `now > lease_expires_at`, the background watchdog transitions the agent to `stalled` (`lease_expired` event) and invokes `--wake-cmd` with exponential backoff (`wake_backoff_s * 2**wake_count`, capped at `max_wakes`).
- Past `lease_expires_at + dead_grace_s` (default `60s`), the agent transitions to `dead` (`agent_dead` event) and triggers `--notify-cmd` once per outage episode (past `notify_after_s`).

## 5. Deterministic Anti-Spin Detector

1. `SPIN_COMMAND_LOOP`: Within `spin_window_s` (default `600s`), identical `(cmd_norm, exit_code, stderr_hash)` fails (`exit_code != 0`) `>= spin_cmd_repeats` (default `3`) times. A `0` exit clears prior failures for `cmd_norm`.
2. `SPIN_NO_PROGRESS`: Within `spin_window_s`, `>= spin_step_repeats` (default `4`) consecutive `start`/`step` calls carry the exact same normalized `(state, task, doing, note, evidence)` signature without an intervening `reply`.
3. `SPIN_POLL_THRASH`: `>= spin_poll_burst` (default `12`) zero-hold empty polls (`hold_s < 1.0` and `elapsed < 0.5s`) within `60s`. Legitimate chunked `wait` calls (`hold_s >= 1.0`) never trigger this rule.

Triggering any rule sets agent state to `spinning`, increments `spin_strikes`, enqueues a priority-10 `[HARNESS_INTERVENTION]` message, and triggers `--notify-cmd`.

## 6. HTTP, Reverse Streamable-HTTP MCP & Control Socket

- **HTTP Security:** Rejects non-loopback `Host` (unless matching `--public-host`) with `403`, rejects non-loopback `Origin` with `403`, rejects protocol-relative `//` targets / obs-fold / invalid header names (`400`), enforces `16 KiB` head / `2 MiB` body limits under monotonic deadlines (`431`/`413`/`408`), authenticates path `/<token>/...`, per-agent `/a/<agent_token>/...`, or `Authorization: Bearer ...` in constant time (`401`/`403`), and answers `GET /.well-known/oauth-protected-resource/(?:a/)?<token>/mcp` per RFC 9728.
- **Reverse MCP (`/<token>/mcp` or `/a/<agent_token>/mcp`):** Supports `initialize`, `ping`, `tools/list`, `tools/call` (`subdots_register`, `subdots_wait` bounded to `<= 25s`, `subdots_step`, `subdots_reply`, `subdots_ack`, `subdots_heartbeat`).
- **Local Stdio MCP (`subdots.py mcp-stdio`):** Exposes 9 owner tools over `c.sock`: `subdots_list_agents`, `subdots_agent_status`, `subdots_inbox`, `subdots_mark_read`, `subdots_send`, `subdots_steer`, `subdots_await_reply`, `subdots_events`, `subdots_control`.
- **Control Socket (`c.sock`):** Line-delimited JSON with `hello` (`protocol_version=1`), unary commands (`hub.status`, `hub.stop`, `agent.list`, `agent.provision`, `agent.status`, `message.send`, `message.await`, `message.inbox`, `message.mark_read`, `events.list`, `control.*`), and `events.follow` NDJSON streaming.

## 7. `.dots-ops/` Mirroring

When `--ops-base` (or `--mirror-ops` with `$DOTS_OPS_BASE`) is set on `hub start`, the Hub mirrors messages (`mail-v1`), steps (`live-record-v1`), and memory (`memory-v1`) using `O_NONBLOCK` + `S_ISREG` checks so `dots.py board` and `dots_monitor.py` can read Mode 2 activity safely.
