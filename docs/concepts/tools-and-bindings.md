# Tools And Bindings

A thread's tools are written on its record when the thread starts, and the
model sees only those tools. This page explains where tools come from and how
they reach a thread.

## Operations And Tools

An **operation** is an executable contract: a name, a typed input, a typed
output, and the effects it may have. Operations are published into a local
registry (`.verlet/operations/` by default) and stored by content hash, so
a reference like `op://http-fetch@sha256:…` always means the same code.

A **tool** is how the model sees an operation. The same operation can appear
as a function the model calls directly or as a command inside the agent's
virtual shell.

## From Manifest To Thread

A tool reaches a thread in three steps:

1. **Declare.** The agent manifest lists tool rows. Each row names an
   operation.
2. **Publish.** `verlet agent publish` checks every row against the
   operations registry and records each reference with its exact hash.
   (`--resolve-ops` rewrites `op://name` and `op://name@latest` in your file
   to the current hash first.)
3. **Bind.** When a thread starts, the runtime writes one `binding.attached`
   event per tool package. The thread's tools are the attached packages minus
   any that a `binding.detached` event later removed.

A **binding** is one of those attach events. Nothing else adds a tool. A tool
named in the system prompt but not bound does not exist for the model, and
editing the manifest on disk does not change a running thread. The runtime
writes attach and detach events when a thread is bound or re-bound to a
manifest. There is no RPC method yet to attach or detach a single tool on a
running thread.

## Tool Rows

**`direct_tool`** makes an operation a function the model calls directly:

```toml
[[tools]]
type = "direct_tool"
id = "json_query"
tool_name = "json_query"
operation_ref = "op://json-query@sha256:<hash>"
```

**`bash_tool`** makes an operation a command inside the virtual shell. The
model calls it through the `bash` tool, and can pipe it like any command:

```toml
[[tools]]
type = "bash_tool"
id = "http_fetch"
command = "http_fetch"
operation_ref = "op://http-fetch@sha256:<hash>"
```

**`protocol_tool_import`** connects a remote MCP server. By default the model
reaches its tools through three search tools (`tool_search`,
`tool_describe`, `tool_call`) instead of seeing every tool up front:

```toml
[[tools]]
type = "protocol_tool_import"
id = "search"
protocol = "mcp"
server_ref = "mcp://search"
include_tools = ["search"]
```

`server_ref` names an MCP source you registered with `verlet tool source add`
(HTTP or SSE transport). To expose one MCP tool as a direct tool, pin it by
hash with `expose = ["direct_tool"]` and `pin = "mcptool://<server>/<tool>@sha256:<hash>"`.

Read operation hashes with `verlet tool list`. `op://<package>@sha256:<hash>`
binds every operation in a package. `op://<package>/<operation>@sha256:<hash>`
binds one.

## Where Operations Come From

| Source | How you get it |
| --- | --- |
| Your own Rust code, compiled to Wasm | `verlet tool build` and `verlet tool publish`. See [Rust Wasm Operation Dev Kit](../wasm-operation-dev-kit.md). |
| An OpenAPI description | `verlet import build` and `verlet import publish`. See [OpenAPI Operation Imports](../openapi-adapter.md). |
| A remote MCP server | `verlet tool source add`, then a `protocol_tool_import` row. |
| Standard operations: `http-fetch`, `file-read`, `json-query` | `scripts/seed-ops.sh` in a source checkout. See [Standard Operations](../standard-operations.md). |
| Built-in packages | Published by the server at startup. See below. |

The server publishes four built-in packages each time it starts:

- `verlet-threads`: start and manage child threads;
- `verlet-schedule`: start, list, and revoke scheduled work (mandates);
- `verlet-process`: run **real commands on the host**, in the server's
  working directory, with no sandbox;
- `verlet-notify`: record notifications and outgoing channel messages for a
  delivery adapter to send.

A manifest must bind them like any other package. Bind `verlet-process` only
when you mean to give the agent a real shell.

## The Virtual Shell And The Workspace

When a thread has any operation or skill bound, it also gets a `bash` tool.
This shell is virtual. It runs built-in commands and your `bash_tool` operations over an
in-memory filesystem, and it cannot start host programs. It starts in
`/workspace`, which is empty scratch space unless the thread has a
workspace.

To give an agent a host directory, the manifest asks for a mount point and
the least access it needs:

```toml
[workspace]
guest_path = "/workspace"
min_mode = "rw"   # "ro" is the default
```

The manifest never contains a host path. The operator supplies the directory
and mode in the daemon config, and a thread start over RPC can override it:

```toml
[daemon.runtime.workspace]
host_path = "../my-project"
mode = "rw"
```

The bind fails if either side is missing. The bind receipt records the host
path, guest path, and mode.

## Tool Kits

A **kit** is a bundle of tool packages installed for the default agent that
`verlet chat` uses. The repository ships the Pi kit (`agent-tools/pi-kit`),
with `read`, `write`, `edit`, `find`, and `grep` tools that work on the
workspace. Install a kit from a source checkout with `verlet kit install
<kit-dir>`, then restart the server. `verlet kit list` and `verlet kit
remove` manage installed kits. See [Kits](../wasm-operation-dev-kit.md#kits).

## Skills

Skills are `SKILL.md` directories. Publish one with `verlet skill publish`,
or convert an existing skill folder with `verlet skill import`. A manifest
that lists a skill resource shows the model an index of the skills and puts
their bodies at `/skills/<name>.md` in the virtual shell, read-only. Local
agents can instead set `[skills] discover = true` to read skills from
`.agents/skills` in the workspace. See [Verlet Agent CLI](../agent-cli.md).

## Secrets And Network Access

A tool row can allow specific secrets and private network destinations:

```toml
[tools.attachment]
allowed_secrets = ["SEARCH_API_KEY"]

[tools.attachment.allowed_private_network]
"http://127.0.0.1:*" = ["GET"]
```

The runtime passes the secret to that tool when it runs. The model never
sees the value. A tool reaches private or loopback addresses only when its
row allows them. See [Permissions](permissions.md) and
[Secret Management](../secret-management.md).
