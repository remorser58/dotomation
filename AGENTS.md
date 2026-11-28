# Maintainer notes (`v1.4`)

This repository supports two user-selectable operating modes — **Mode 1 (`dots-only`)** in `modes/dots-only/` (Dots quota only, zero local quota) and **Mode 2 (`sub-dots`)** in `modes/sub-dots/` + `tools/subdots.py` (reverse `web-gpt-agent` local-to-subagent steering) — alongside the instruction/skill layer (aligned with `dbc-hbin/omp-superkit@0159023e` SUPER/Superstack/Astra-v2) and the four owner-requested Python 3.9+ stdlib tools below. The skill's entry point is [.agents/skills/dotomation/SKILL.md](.agents/skills/dotomation/SKILL.md); read it and [dispatch](workflows/dispatch.md) before editing the kit. Preserve strict schemas in `protocol/` (`memory-v1.schema.json` has `additionalProperties: false` — never add extra keys to `memory/<DOT>/<agent>.json`; store Mode 1 loop/Astra tracking in `loops/<DOT>/<track>.state.json`).

Verification: run all three Python 3.9/3.12/3.14 unittest suites sequentially with `PYTHONDONTWRITEBYTECODE=1` and `-B`, verify cross-tool Astra v2 parity (`dots.py` == `subdots.py`), verify hook scripts cleanly, and never bundle third-party dependencies, credentials, private paths, or `__pycache__`.

## The first exception: `tools/dots_monitor.py`

Read-only activity observer ([monitoring](workflows/monitoring.md)) comparing live records, daemon request counts / retained-session health, and GitHub evidence using Python stdlib + `ssh`/`gh`. Never mutates monitored hosts or installs background schedules.

## The second exception: `tools/dots.py`

Single-file Python 3.9+ stdlib helper ([direct mail](workflows/direct-mail.md#steering-send-wait-reply), [Mode 1 README](modes/dots-only/README.md)) implementing `tick`, `board`, `memo`, `send`, `inbox`, `read`, `reply`, `wait`, `init`, `watch`, `prompt`, `loop` (`init|enqueue|claim|ack|progress|complete|block|pause|resume|status`), and release `update`. Keeps `memory-v1.schema.json` untouched while compiling and enforcing `[ASTRA_HARNESS_PROFILE v2]` (`ASTRA_SOURCE_PIN = "0159023e"`) inside `prompt` and `loop`. `SECRET` redaction covers PEM/PGP private key blocks and quoted secret assignments.

## The third exception: `tools/subdots.py` (Mode 2: `sub-dots`)

Single-file Python 3.9+ stdlib Reverse Bridge Hub (`tools/subdots.py`, [Mode 2 README](modes/sub-dots/README.md), [PROTOCOL.md](modes/sub-dots/PROTOCOL.md)):
- Owner-private `~/.subdots` (`0700`), SQLite WAL (`0600`) with online migrations, Unix control socket (`0600`), local `mcp-stdio` (15 tools including `subdots_astra_profile`), loopback HTTP + Reverse Streamable-HTTP MCP (`initialize.instructions` role-bound to `/a/<agent_token>/mcp`), hard steering/readback/`ASTRA_PROFILE_ACK_REQUIRED` per-epoch gates, owner-contract `--astra-role` & `--read-only` reviewer enforcement, `[CONTEXT_CAPSULE]` + Phoenix rollover, anti-spin/anti-false-refusal guards, gated tool proxy (`--enable-tool-proxy`), git pre-commit/pre-push hooks, and local/subagent IDE hooks (`hooks/hooks.json`, `hooks/subagent-hooks.json`, `hook subagent-start|subagent-pre-invocation|subagent-stop`).

## The fourth exception: `tools/dots_pilot.py`

Single-file Python 3.9+ stdlib zero-AI-token Mode 1 pilot (`tools/dots_pilot.py`, [`tools/dots-pilot.example.json`](tools/dots-pilot.example.json)): private `~/.dotomation-pilot` (`0700`/`0600` + `flock`), SHA-256 pinning of `dots.py` & `dots_monitor.py`, preset initialization (`--require-astra-ack`), role-bound `brief` generation via pinned `dots.py prompt`, nonce-authenticated `watch` cycles, cooldown-guarded opt-in daemon heal, and `render-service` template printing (never auto-installs services).
