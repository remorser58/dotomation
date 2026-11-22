# Hosts, approved routes and placement

Where work runs, how it is reached, and what is never used. The [dispatch rules](dispatch.md#confirm-the-run-and-target) apply on every host.

## Placement: VM first, then Osaka, then the Mac mini

| Host | `host` label | Use it for | Prerequisites and limits |
|---|---|---|---|
| Dots VM | `vm` | Default. Public-source research, read-only analysis, derived artifacts that do not change the canonical copy, existing Node and Playwright checks. | State can disappear mid-step. Checkpoint inputs, results and evidence to Osaka before relying on VM state ([VM watchdog](vm-watchdog.md)). A VM browser pass is not Safari or a device result. |
| Osaka OCI | `osaka` | Canonical work: the projects whose canonical copy is Osaka, writes, long sessions, the checkpoint and evidence store, approved external writes. | Confirm the hostname and the approved work root first. Do not reach canonical writes through a VM copy, another Mac or a proxy. |
| Mac mini | `mini` | Only what needs that Mac: Apple native build and signing, Hancom, real Safari or real devices, login-only UI. | Every write this task produces — code, logs, profiles, downloads, `TMPDIR`, build output — must stay under the approved external `/Volumes/SanDisk2TB`. If redirection cannot be confirmed, do not run. |
| MacBook | `macbook` | Only when the owner names it for that work. | Same host confirmation and route rules. |

Minimise Mac mini use. If the task does not need Apple build or signing, Hancom, real Safari or devices, or a login-only UI, it does not belong on the mini; move it to the VM or Osaka. A Mac mini slot is not a faster VM.

## Approved routes

The owner installed Web GPT Agent connectors, named per account. These are the approved routes to those hosts:

| Account | Host | Connector |
|---|---|---|
| 17 | Osaka OCI | `Web GPT Agent OCI 17` |
| 17 | Mac mini | `Web GPT Agent Mac mini 17` |
| 18 | Osaka OCI | `webgptagent_OCI_Osaka` |
| 18 | Mac mini | `Web GPT Agent Mac mini` |
| 18 | MacBook | `Web GPT Agent MacBook Secure` |

Use the connector installed for the account that holds the work. A connector name is not a hostname: confirm the host (hostname) before workspace work and again after a reconnect, a host change or a workspace change. Do not use a connector for a host or account it was not installed for, and do not assume one account's connector carries another account's permissions or files.

## Revised never-MCP rule (owner revision, 2026-11-23 KST)

The earlier rule read: "On a connected computer, do the work through that computer's native CLI tools … Never use Web GPT Agents MCP for work on that computer, even from the Dots VM." It was written before the owner installed named connectors per account, and it blocked REV from running the Remorswarm tests on Osaka. The owner explicitly revised it:

- A computer attached through Dots' own device connection is worked on through that computer's native CLI tools. Do not reach it through a Web GPT Agents MCP route, from the VM or anywhere else.
- An owner-approved, named Web GPT Agent connector **is** an authorised route to its host. Work through it within the same scope, host-confirmation, tool-limit and readback rules as any other route.
- The distinction is the route's authorisation, not the host. The same Osaka host can be reached by its native CLI when it is attached as a Dots device, or by the owner-installed connector named for that account. An unnamed, unapproved or other-account MCP endpoint is neither.
- Never Codex: Codex Native2, Codex cloud, or any Codex-linked tool. No Codex quota is spent, on any host, for any reason. The owner's rule is absolute; this kit offers no substitution and no revision path for it.
- If no authorised route can perform the operation, report it blocked or not run. Do not substitute another host, another account's connector, a copy of the canonical tree or a proxy.

The kit remains instruction-only: it does not install, register or configure a connector, and it cannot verify that a named connector exists. If the named connector is absent from the account, that step is blocked; it is not a reason to use a different route.

## Parallel work

Independent lanes may run on different hosts at the same time. Final changes to one canonical copy stay with one writer, and an existing task is never moved to another host automatically. Record the host actually used in the live record ([live status](live-status.md)); a task that changes host gets a new `step` line naming the new host.
