# Verlet Docs

These docs are for engineers who have built on an agent harness before
(Claude Code, Codex, or an agent SDK) and want to know how Verlet works and
how to build on it. If you are new to agent runtimes in general, read the
[primer](https://emotionscientific.github.io/verlet-kernel/primer/agents-in-version-control.html)
first.

## Start

- [Overview](index.md): what Verlet is, and how it maps onto the harness you
  already know.
- [Getting Started](getting-started.md): install, chat, write an agent, and
  read what it did.
- [Agents in Version Control](https://emotionscientific.github.io/verlet-kernel/primer/agents-in-version-control.html):
  the long-form primer on why the runtime is built this way
  ([PDF](primer/agents-in-version-control.pdf), [source](primer/src/)).

## Concepts

- [The Record](concepts/the-record.md): threads, turns, events, receipts,
  resume, and fork.
- [Tools And Bindings](concepts/tools-and-bindings.md): how a tool gets into
  an agent, and why nothing else can put one there.
- [Permissions](concepts/permissions.md): tool-call decisions, approvals,
  secrets, and network access.
- [Kernel Invariants](kernel-invariants.md): the rules every other page relies
  on.

## Guides

- [Chat Console](chat.md) and [Provider Setup](provider-setup.md): the
  terminal console and connecting a model.
- [Verlet Agent CLI](agent-cli.md): write, plan, publish, and run agent
  manifests.
- [Rust Wasm Operation Dev Kit](wasm-operation-dev-kit.md): build your own
  tools, and install tool kits.
- [OpenAPI Operation Imports](openapi-adapter.md): turn an HTTP API into
  tools.
- [Secret Management](secret-management.md): give a tool a secret without
  giving it to the model.
- [Verlet Daemon](daemon.md): run Verlet in the background or as a system
  service.
- [MCP Server](mcp-server.md) and [ACP Agent](acp-agent.md): use Verlet from
  an MCP client or an ACP editor.

## Reference

- [Verlet CLI](cli.md) and the `verlet(1)` [man page](man/verlet.1).
- [Agent Manifest Ontology](agent-manifest-ontology.md): the manifest format.
- [Standard Operations](standard-operations.md): the tools that ship with the
  runtime.
- [ABI: Verlet Operation Boundary](abi.md): the contract every tool
  implements.
- [RPC Control Plane](app-server.md): the app-server protocol for clients and
  embedders.
- [Command Contracts](command-contracts.md): how operations appear as
  virtual-bash commands.
- [Provider Adapter Surface](provider-adapters.md),
  [Metadata And Provider Auth Storage](provider-storage.md), and
  [Tool Publish Storage](publish-storage.md).
- [ACP Thread Projection](acp-thread-projection.md).
- [Frozen Format IDs](format-ids.md): identifiers that keep their pre-rename
  names.

## How It Is Built

These pages are for contributors and for readers who want the internals.

- [Repository Map](repository-map.md): where the code lives.
- [Verlet IO](io.md): how messages enter and leave threads exactly once.
- [Threat Model](threat-model.md).
- [How Verlet Is Tested](how-verlet-is-tested.md) and
  [Testing Guidelines](testing-guidelines.md).
- [Runtime Primitives](developers/runtime-primitives.md) and
  [Protocol Surfaces](developers/protocol-surfaces.md).
- [Documentation System](developers/documentation-system.md): how these docs
  are organized and written.
- [Public API Coverage](public-api-coverage.md): which public surfaces have
  docs and man pages, and which still need them.
- [V1 Release Candidate Gate](v1-release-candidate.md).
- [Roadmap](roadmap.md).
- Architecture decision records: [0001](adr/0001-stream-schema-v1.md)
  stream schema, [0002](adr/0002-guest-encoding-v1-component-model-later.md)
  guest encoding, [0003](adr/0003-durable-ingress-outcome-protocol.md)
  durable ingress, [0004](adr/0004-seeded-fault-plans-and-scenario-engine.md)
  fault plans, [0005](adr/0005-turso-storage-engine.md) storage engine,
  [0006](adr/0006-unified-orchestration-semantics.md) orchestration,
  [0007](adr/0007-adapter-envelope-contract-v0.md) adapter envelopes,
  [0008](adr/0008-identity-plane-v0.md) identity, and
  [0009](adr/0009-orchestrator-boundary-v0.md) orchestrator boundary.
