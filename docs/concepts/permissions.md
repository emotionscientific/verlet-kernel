# Permissions

This page covers what an agent is allowed to do and how the runtime decides.
There are two layers. Bindings decide which tools exist for a thread at all.
Controllers decide, call by call, whether a bound tool may run.

## What A Thread Can Reach

These are fixed when the thread starts and written in its bind receipt:

- **Tools.** Only bound tools exist for the model. See
  [Tools And Bindings](tools-and-bindings.md).
- **Files.** The virtual shell sees an in-memory filesystem. A host directory
  appears only when the manifest asks for a workspace and the operator names
  the directory and the mode (read-only or read-write).
- **Host commands.** Only the `verlet-process` package runs real host
  programs, and only for a manifest that binds it.
- **Secrets.** A tool receives a secret only when its row lists the secret in
  `allowed_secrets`. The model never sees secret values.
- **Private network.** A tool reaches loopback or private addresses only when
  its row lists them in `allowed_private_network`.
- **Child threads.** An agent can start child threads only when the manifest
  sets `allow_child_agents = true` and binds the `verlet-threads` tools.

## Deciding Each Tool Call

Before a bound tool runs, the runtime writes a `tool.call.requested` event.
If the thread has a **controller**, the controller reads that event and
writes a decision to the thread's `control:` stream:

| Decision | Effect |
| --- | --- |
| allow | the call runs as requested |
| rewrite | the call runs with arguments the controller supplied |
| deny | the call does not run |
| wait | the call is held (`tool.call.suspended`) |

The call and its decision sit next to each other on the record. If a
controller writes conflicting or malformed decisions, or no decision at all,
the call is denied.

**With no controller, every bound tool runs.** The default agent has no
controller. Treat binding a tool as permission to use it.

## Controllers

A controller is a **coupling**: a function the runtime runs when a given
kind of event appears on a thread, with a declared place to write its
output. A manifest declares couplings in `[[couplings]]` rows. Two built-in
couplings act as controllers:

- `std::permission.tool_gate` applies a fixed decision (allow, rewrite, deny,
  or wait) to calls that match a tool name;
- `std::permission.approval_gate` holds matching calls and writes an
  `approval.requested` event for a person to answer.

The row format and every built-in coupling are described under
[Coupling Templates](../standard-operations.md#coupling-templates). A coupling
row must name a published operation in `function_ref`, even for the built-in
templates, so setting up a gate today takes an extra publish step.

## Approvals

When an approval gate holds a call, the approval appears in the web console's
inspector with Approve and Deny buttons. Clients can list pending approvals
with `thread/approvals/list` and answer with `approval/resolve` over RPC. The
answer is written to the record as `approval.resolved`, with the decision and
an optional reason.

Three parts are not built yet:

- The terminal chat (`verlet chat`) does not show approvals.
- An approval does not release the held call. The call stays held after
  `approval.resolved` is recorded. Connecting the two is in progress.
- `approval.resolved` does not yet record who answered.

## Hooks

The runtime has a hook pipeline in the style of Claude Code hooks (before and
after tool use, on prompt submit, around compaction, and on stop). It is not
configurable yet: the manifest rejects a `hooks` section, and the
`hooks/list` RPC method returns an empty list. Use a controller coupling for
per-call decisions.

## Who Asked

Every message that enters a thread is recorded with the principal (the user
or service identity) it came in under, in an `io.ingress.received` event.
Local clients on the same machine connect over a Unix socket without a
token. Remote clients need a bearer token. `verlet identity bootstrap`
creates the first operator and prints one. See
[RPC Control Plane](../app-server.md#authentication).
