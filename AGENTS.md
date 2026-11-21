# Maintainer notes

This repository contains an instruction-only Dots skill. Read `.agents/skills/dots-superkit/SKILL.md` before editing the protocol. The JSON Schema in `protocol/` defines message shape, not authorization or ordering. Keep `examples/` consistent with both. Preserve stable protocol identifiers; introduce a new version for incompatible changes.

Verification: read the final skill, match the worked conversation to the schema, then test negative cases against the closure rules using approved existing tools. Use only synthetic evidence in examples. No install, executable, daemon, hook, account, credential or telemetry is part of this repository. Do not publish or push without authorization.
