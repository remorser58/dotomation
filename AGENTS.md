# Maintainer notes

This repository contains an instruction-only Dots skill. Its entry point is [.agents/skills/dots-superkit/SKILL.md](.agents/skills/dots-superkit/SKILL.md); read it and [dispatch](workflows/dispatch.md) before editing the kit. Dispatch supplies the common Dots run and workspace requirements; role and workflow instructions must preserve them in downstream briefs. The JSON Schema in `protocol/` defines message shape, not authorization or ordering. Keep `examples/` consistent with both. Preserve stable protocol identifiers; introduce a new version for incompatible changes.

Verification: read the final skill, match the worked conversation to the schema, then test negative cases against the closure rules using approved existing tools. Use only synthetic evidence in examples. No install, executable, daemon, hook, account, credential or telemetry is part of this repository. Do not publish or push without authorization.
