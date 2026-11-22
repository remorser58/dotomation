# Maintainer notes

This repository contains an instruction-only Dots skill and one read-only monitoring tool (see below). The skill's entry point is [.agents/skills/dots-superkit/SKILL.md](.agents/skills/dots-superkit/SKILL.md); read it and [dispatch](workflows/dispatch.md) before editing the kit. Dispatch supplies the common Dots run and workspace requirements; role and workflow instructions must preserve them in downstream briefs. The JSON Schema in `protocol/` defines message shape, not authorization or ordering. Keep `examples/` consistent with both. Preserve stable protocol identifiers; introduce a new version for incompatible changes.

Verification: read the final skill, match the worked conversation to the schema, then test negative cases against the closure rules using approved existing tools. Use only synthetic evidence in examples. No install, executable, daemon, hook, account, credential or telemetry is part of this repository, with the single exception below. Do not publish or push without authorization.

## The one exception: `tools/dots_monitor.py`

The owner asked for a way to see whether REV, REM and their sub-agents are actually working, and self-reports alone cannot show that. That one request, and only that one, is an exception to the rule above:

- **What it is.** One read-only Python 3 file, `tools/dots_monitor.py` ([monitoring](workflows/monitoring.md)), with its standard-library test `tools/test_dots_monitor.py` and two JSON configs holding host names, paths, account numbers and repository names only. It reads the live records, the Web GPT Agent daemon request log of each dot's account, and GitHub references through `gh api`. `watch` is a foreground command the reader starts and stops; nothing schedules or restarts it.
- **Still not part of this repository:** no install step, no package or dependency, no service, daemon, scheduler, hook, plugin, account, credential or telemetry. Standard library only, plus `ssh` and `gh` as existing CLIs the reader already has.
- **Read-only everywhere.** It never appends, edits, deletes or renames a record and never writes to a host it reads; the remote side runs only `wc -c` and `tail -c` over the configured files. It never messages a dot and never assigns work: it reports, and a coordinator still decides.
- **Bounded and redacted.** Every subprocess has a timeout, every file read is a byte tail, and anything that looks like a token is redacted before display.

Any other executable, service or write path is still out of scope. If a future change needs one, it needs its own decision from the owner, not this paragraph.
