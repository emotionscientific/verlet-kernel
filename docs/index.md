# Verlet

Verlet is an open-source runtime for AI agents, written in Rust. It runs the
same loop as Claude Code or Codex: a model, a system prompt, a set of tools,
and turns. What changes is where the agent's state lives. Verlet writes every
step of a run to an append-only record, and that record is the agent's only
state. Resume, fork, audit, and debugging all read the same record.

Verlet is experimental. It is the engine under Verlet Cloud, the managed
agents service.

## If You Know A Harness

Most of what you know carries over. This table maps the parts of a typical
harness to their Verlet equivalents.

| In your harness | In Verlet |
| --- | --- |
| System prompt, `CLAUDE.md`, `AGENTS.md` | An **agent manifest**: a folder with `verlet.agent.toml` and `prompts/system.md`. Verlet does not load project instruction files on its own. |
| Tool list, MCP server config | **Tool rows** in the manifest. When a thread starts, the runtime writes one `binding.attached` event for each tool package. The model's tools come from those events and from nothing else. |
| Session transcript file | The thread's **record**: typed events in a local database under `.verlet/state/`. |
| `--resume`, `--continue` | Resume reads the record, including which manifest and tools the thread was bound to. It does not re-read the manifest from disk. |
| Bash tool | **Virtual bash** over an in-memory filesystem. A host directory is visible only when the manifest asks for a workspace and the operator names the directory. |
| Permission prompts, allow lists | A **controller** that sees each tool call before it runs and decides allow, rewrite, deny, or wait. With no controller, every attached tool runs. |
| Hooks | Not configurable yet. The hook pipeline exists in the code but has no user-facing switch. |
| Subagents | **Child threads**, started with the `thread_spawn` tool. Each child has its own record and its own tools. |
| Skills | `SKILL.md` directories, published as packages or discovered from `.agents/skills`. |
| SDK, app-server | A JSON-RPC server with the same message shapes as the Codex app-server, plus an MCP server and an ACP agent. |

## What Is Different

**The record is the state.** A thread is the ordered list of its events:
user messages, model replies, tool calls, tool results, and the runtime's own
decisions. When Verlet restarts, it rebuilds each thread by reading that
list. Nothing the agent did lives only in memory.

**Tools come only from bindings.** A tool exists for an agent when its
binding event is on the record. A prompt cannot grant a tool, and a config
change on disk does not change a running thread. A thread's tools change only
through new attach and detach events, which are written when the thread is
bound to a manifest again.

**Everything is pinned by hash.** Published tools, prompts, and skills are
stored by content hash. When you publish an agent, its tool references are
resolved to exact hashes. The record names those hashes, so you can always
tell which code and which prompt produced a result.

**Every decision is written down.** When the runtime resolves a name, builds
the model's context, or decides whether a tool call may run, it writes a
receipt: an event that says what it decided and from which inputs. `verlet
debug bind` and `verlet debug journal` print these.

## What Works Today

The current release is v0.5.1. You can:

- chat with an agent in the terminal (`verlet chat`) or the browser
  (`verlet console`), against OpenAI, Anthropic, any OpenAI-compatible
  endpoint, or a ChatGPT plan;
- write an agent manifest, publish it, and start threads from it;
- give agents tools written in Rust and compiled to Wasm, tools imported from
  an OpenAPI description, and tools from remote MCP servers;
- mount a host directory into a thread, read-only or read-write;
- hold a tool call for approval, and record the approval or denial (from
  the web console or over RPC);
- start child threads, including in separate local processes;
- resume and fork threads, and read their full record.

These are designed but not built yet:

- a way to configure hooks;
- running a held tool call once someone approves it (today the approval is
  recorded, but the call stays held), and an approval prompt in the terminal
  chat;
- context budgets (the manifest accepts `budget_share`, but it has no effect
  yet);
- remote and sandboxed placement, and a public package registry.

## Read Next

- [Getting Started](getting-started.md): install, chat, write an agent, and
  read its record.
- [The Record](concepts/the-record.md), [Tools And Bindings](concepts/tools-and-bindings.md),
  and [Permissions](concepts/permissions.md): the three ideas the rest of the
  docs build on.
- [Verlet Docs](README.md): the full list of pages.
