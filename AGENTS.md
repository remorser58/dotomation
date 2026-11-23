# Maintainer notes

This repository contains an instruction-only Dots skill and two Python files the owner asked for (see below). The skill's entry point is [.agents/skills/dots-superkit/SKILL.md](.agents/skills/dots-superkit/SKILL.md); read it and [dispatch](workflows/dispatch.md) before editing the kit. Dispatch supplies the common Dots run and workspace requirements; role and workflow instructions must preserve them in downstream briefs. The JSON Schema in `protocol/` defines message shape, not authorization or ordering. Keep `examples/` consistent with both. Preserve stable protocol identifiers; introduce a new version for incompatible changes.

Verification: read the final skill, match the worked conversation to the schema, then test negative cases against the closure rules using approved existing tools. Use only synthetic evidence in examples. No install, executable, daemon, hook, account, credential or telemetry is part of this repository, with the two exceptions below. Do not publish or push without authorization.

## The first exception: `tools/dots_monitor.py`

The owner asked for a way to see whether a dot and its sub-agents are actually working, and self-reports alone cannot show that. That request, and only that one, is the first exception to the rule above:

- **What it is.** One read-only Python 3 file, `tools/dots_monitor.py` ([monitoring](workflows/monitoring.md)), with its standard-library test `tools/test_dots_monitor.py` and one JSON config template, `tools/dots-monitor.example.json`, holding host names, paths, account numbers and repository names only. An instance keeps its filled-in config outside this repository. It reads the live records, the Web GPT Agent daemon request log of each dot's account, and GitHub references through `gh api`. `watch` is a foreground command the reader starts and stops; nothing schedules or restarts it.
- **Still not part of this repository:** no install step, no package or dependency, no service, daemon, scheduler, hook, plugin, account, credential or telemetry. Standard library only, plus `ssh` and `gh` as existing CLIs the reader already has.
- **Read-only everywhere.** It never appends, edits, deletes or renames a record and never writes to a host it reads; the remote side runs only `wc -c` and `tail -c` over the configured files. It never messages a dot and never assigns work: it reports, and a coordinator still decides.
- **Bounded and redacted.** Every subprocess has a timeout, every file read is a byte tail, and anything that looks like a token is redacted before display.

## The second exception: `tools/dots.py`

The owner asked for a way to steer a dot's sub-agent from their own local session and to set that up for any user with one local machine ("하고 슈퍼킷에 세팅해라. 남들이 그냥 바로 써도 자기 환경에 맞춰서 쓸 수 있게"). Conversations inside a dot cannot be read or written from outside it, so the only possible route is a file the agent reads plus a command the agent holds open. That request, and only that one, is the second exception:

- **What it is.** One Python 3 file, `tools/dots.py` ([direct mail](workflows/direct-mail.md#steering-send-wait-reply), [session memory](workflows/session-memory.md#one-call-per-step)), with its standard-library test `tools/test_dots.py`. `tick`, `board` and `memo` keep the live record, the session memory and the mail read in one call; `send`, `inbox`, `read`, `reply` and `wait` carry direct mail and hold a wait open for a new instruction; `init` writes the instance config `<base>/dots.json` and creates the folder tree, then prints the brief to paste to each dot root.
- **Still not part of this repository:** no install step, no package or dependency, no service, daemon, scheduler, hook, plugin, account, credential or telemetry. Standard library only. Every process the tool starts is one an agent or the user started, and it ends by itself: `wait` exits 0 on mail, 3 on its timeout or 4 on a signal, and the wake command `send` may run has a 25-second timeout and is never retried.
- **Protocol unchanged.** It writes only [mail-v1](protocol/mail-v1.schema.json), [live-record-v1](protocol/live-record-v1.schema.json) and [memory-v1](protocol/memory-v1.schema.json) shapes, plus the read markers, its own `listen/` markers and `dots.json`. A line written by hand or by an instance's own helper stays readable, and the tool reads them the same way. No new kind, field or protocol version.
- **Its own limits.** It is not a message broker and not a watcher: `send` reports whether the agent is listening and otherwise prints the doorbell; nothing retries or wakes an agent twice. `wait` polls about once a second while an agent holds it open, and the cost of that (one model call per poll, at most one every five minutes) is stated where the agent reads it.

Any other executable, service or write path is still out of scope. If a future change needs one, it needs its own decision from the owner, not this paragraph.
