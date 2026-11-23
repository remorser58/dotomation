# Hosts, approved routes and placement

Where work runs, how it is reached, and what is never used. The [dispatch rules](dispatch.md#confirm-the-run-and-target) apply on every host.

## Placement: the VM first, then the one local machine

The kit assumes two hosts: the Dots VM and the user's one local computer. Every dot, project and shared folder in this kit lives on `local`; the VM is a workspace, not a place where canonical state lives.

| Host | `host` label | Use it for | Prerequisites and limits |
|---|---|---|---|
| Dots VM | `vm` | Default for research, read-only analysis, and derived artifacts that do not change the canonical copy; existing Node and Playwright checks. | State can disappear mid-step. Checkpoint inputs, results and evidence to `local` before relying on VM state ([VM watchdog](vm-watchdog.md)). A VM browser pass is not the user's own browser or a device result. |
| Local computer | `local` | Canonical writes, the canonical copy of each project, the shared Dots folder, long runs, the checkpoint and evidence store, approved external writes, and anything that needs the user's own machine: logins, real devices, native build and signing. | Confirm the hostname and the approved work root first. Do not reach canonical writes through a VM copy or a proxy. |

A task that needs the user's machine, a login, a device or a native build belongs on `local`; a VM slot is not a faster local machine. Anything that must survive belongs on `local` or in the external anchor ([VM watchdog](vm-watchdog.md)).

The shared Dots folder is `<work root>/.dots-ops/` on `local`, where `<work root>` is the approved work root on that machine. The live records, session memory, mail, task records and handoffs all live under it ([live status](live-status.md), [session memory](session-memory.md), [direct mail](direct-mail.md)).

### Further hosts come from `init`

Two hosts are the default, not the limit. When the user has another computer, `python3 tools/dots.py init --host LABEL=PURPOSE` records it in `<base>/dots.json` with the label a live record's `host` uses and what that computer is for — a machine that only builds and signs, a machine with a device attached, a second desktop. The same command adds a dot and a project. It only adds: a second run reports what it kept and never rewrites an entry, a record or a mail line ([direct mail](direct-mail.md#dotsjson-the-optional-config-for-one-machine)).

An added host is still a host: confirm its hostname, use only a route the owner approved for that account ([approved routes](#approved-routes)), and never move an existing task to it automatically. If a label is missing from `dots.json`, the record uses the hostname the agent confirmed and the board shows that; a label nobody configured is not a route.

## Approved routes

Two kinds of route reach a host, and only these:

- A computer attached through Dots' own device connection is worked on through that computer's native CLI tools. Do not reach it through a Web GPT Agents MCP route, from the VM or anywhere else.
- An owner-approved, named Web GPT Agent connector **is** an authorised route to its host. Use the connector the user installed for that account and host, within the same scope, host-confirmation, tool-limit and readback rules as any other route.

A connector name is not a hostname: confirm the host (hostname) before workspace work and again after a reconnect, a host change or a workspace change. Do not use a connector for a host or account it was not installed for, and do not assume one account's connector carries another account's permissions or files.

The distinction is the route's authorisation, not the host. The same host can be reached by its native CLI when it is attached as a Dots device, or by the owner-installed connector named for that account. An unnamed, unapproved or other-account MCP endpoint is neither.

Never Codex: Codex Native2, Codex cloud, or any Codex-linked tool. No Codex quota is spent, on any host, for any reason. The owner's rule is absolute; this kit offers no substitution and no revision path for it.

If no authorised route can perform the operation, report it blocked or not run. Do not substitute another host, another account's connector, a copy of the canonical tree or a proxy.

The kit remains instruction-only: it does not install, register or configure a connector, and it cannot verify that a named connector exists. If the named connector is absent from the account, that step is blocked; it is not a reason to use a different route.

## Parallel work

Independent lanes may run on different hosts at the same time. Final changes to one canonical copy stay with one writer, and an existing task is never moved to another host automatically. Record the host actually used in the live record ([live status](live-status.md)); a task that changes host gets a new `step` line naming the new host.
