# The Record

Every Verlet agent runs on a record: an append-only list of typed events. The
record holds the conversation, every tool call and result, and every decision
the runtime made along the way. It is the agent's only state. This page
explains how the record is organized and what reads it.

## Threads And Turns

A **thread** is one line of work with its own record. A chat session is a
thread. A child agent is a thread. Each thread is bound to one agent
manifest when it starts.

A **turn** is one round of work on a thread. It starts when a message is
submitted and ends when the agent has nothing left to do, a limit stops it,
or it fails. One turn can contain many model calls and tool calls.

## Streams And Events

Each thread writes to two streams:

- the `thread:<id>` stream holds the conversation and everything the agent
  did: messages, model replies, tool calls, tool results, and the context
  shown to the model;
- the `control:<id>` stream holds decisions about the thread: whether a
  message was admitted, and whether a tool call may run.

An **event** has a kind, a position in its stream, an id, a timestamp, and a
JSON payload. The kinds you will see most:

| Kind | Written when |
| --- | --- |
| `session.entry.appended` | a message, reply, or runtime entry joins the conversation |
| `turn.submitted`, `turn.completed`, `turn.failed` | a turn starts, ends, or fails |
| `context.compile.completed` | the runtime builds the model's input for one call |
| `tool.call.requested`, `tool.call.completed` | the model asks for a tool, and the tool returns |
| `tool.call.decision`, `tool.call.suspended` | a controller allows, rewrites, denies, or holds a call |
| `manifest.compile.completed`, `manifest.bind.completed` | the thread's manifest is resolved and bound |
| `binding.attached`, `binding.detached` | a tool is added to or removed from the thread |
| `thread.spawned` | a child thread starts |

The full list of event kinds is fixed in the `verlet-history` crate. Event
kinds are only ever added, so an old record stays readable.

## Receipts

A **receipt** is an event that explains a decision: what the runtime decided
and from which inputs. Three receipts matter most:

- `manifest.compile.completed` names the agent manifest by ref and hash;
- `manifest.bind.completed` names the model, runtime settings, placement,
  and tools the thread was given;
- `context.compile.completed` describes exactly what the model was shown on
  one call, with a hash of that input.

Receipts are how you answer "why did the agent do that?" from data. The
model's reply, the context it saw, and the tools it had are all on the same
record, in order.

## Where The Record Lives

A project's state lives in `.verlet/state/` under the project directory.
The record is `session_history.turso`, and thread metadata is
`metadata.turso`. Turso is a storage engine that uses the SQLite file format.
User-level state lives in `~/.verlet/state/` (set `VERLET_HOME` to move
`~/.verlet`).

One server process owns a state directory at a time. Read the record through
that server, or read a stopped server's record directly:

```sh
verlet debug journal --thread <thread-id>          # through the running server
verlet debug journal --journal .verlet/state/session_history.turso   # read-only, server stopped
verlet debug bind <thread-id>                      # the thread's setup, from its receipts
```

Clients can read the same events over RPC with `journal/events/list`.

## Resume

When a server restarts, or a client reopens a thread, the runtime rebuilds
the thread by reading its record. It binds the thread to the same manifest
hash and the same tools recorded in its bind receipt. It does not look up
the agent by name again, so publishing a new version of the agent does not
change threads that already exist. A resume can change only the working
directory and the model provider.

## Fork

A **fork** is a new thread that starts from an existing thread's history.
The new thread's history refers to the parent's events up to the fork point
rather than copying them. `/fork` in the terminal chat and `thread/fork` over
RPC keep the parent's manifest and tools. `thread/rebindFork` forks onto a
different agent, placement, or workspace.

## Child Threads

An agent can start child threads with the `thread_spawn` tool, then send
them work, wait for them, check them, or cancel them (`thread_submit`,
`thread_wait`, `thread_status`, `thread_cancel`). These tools come from the
built-in `verlet-threads` package. A manifest must bind them and set
`allow_child_agents = true` in `[policies]`. Each child has its own record,
and the parent's record holds a `thread.spawned` event that links to it.

A child can run in a separate local process. This needs the daemon's sync
endpoint (`[daemon.sync] listen`) and a `remote` placement target when the
child is spawned. See [Verlet Daemon](../daemon.md).

## Long Threads

The runtime sends the model the full conversation by default. To keep long
threads within the model's context window, compact them:

- `/compact` in the terminal chat, or `thread/compact/start` over RPC, asks
  the model to summarize the conversation so far;
- setting `runtime.compaction.auto_at_text_bytes` in the manifest compacts
  automatically when the conversation grows past that size.

Compaction adds a summary event. The earlier events stay on the record.

## Limits And Interruption

A turn stops calling tools after `runtime.max_tool_rounds` rounds (default
8, or `"unlimited"`). `runtime.turn_timeout_ms` bounds a whole turn.

A new message can reach a running turn in three ways:

- **queue** waits until the current turn ends;
- **steer** is delivered between tool rounds, without stopping a running
  model call or tool call;
- **interrupt** cancels the current turn. Running tools get
  `runtime.cancellation_grace_ms` (default five seconds) to stop, and the
  record notes whether each one stopped in time.

The manifest also accepts `policies.budgets.max_turns` and
`max_tool_calls_per_turn`, but the runtime does not enforce them yet.
