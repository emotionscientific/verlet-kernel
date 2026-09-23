# Kernel Invariants

These are the rules the Verlet runtime keeps, and the words the docs use for
its parts. Other pages rely on both.

## Rules

**The record is the source of truth.** Durable state is stored as typed
records: manifests, operations, bindings, events, receipts, and explicit
configuration. State in memory can make execution fast, but the runtime does
not act on it as fact until it is recorded.

**Every surface shows the same contracts.** The CLI, RPC methods, MCP tools,
ACP sessions, virtual-bash commands, and model-visible tools are views of the
same operations. A surface can change syntax, framing, or transport. It
cannot add authority, hide a durable change, drop a required input, or change
what an output means.

**Authority is explicit.** Tools, resources, secrets, filesystem mounts,
network destinations, and thread controls are declared and bound before
use. When something is missing, the request fails with an error that says
how to fix it.

**Published things are pinned by hash.** Operations, agent manifests, blobs,
and skills are stored by content hash. A running thread can name the exact
manifest, operation, and binding behind each effect.

**Decisions leave receipts.** When the runtime resolves a name, binds a
thread, builds a model's context, or decides whether a call may run, it
records what it decided and from which inputs.

**Providers are replaceable.** Model providers sit behind one adapter
interface. Public docs use generic placeholders for providers unless the
provider is a documented integration.

## Terms

| Term | Meaning |
| --- | --- |
| agent manifest | A versioned TOML declaration of an agent: model profiles, system prompt, tool rows, resources, policies, and runtime defaults. Published as an immutable record. |
| operation | An executable contract with a name, typed input and output, and declared effects. Published by content hash. |
| tool | How the model sees an operation: a direct function or a command in the virtual shell. |
| binding | A `binding.attached` event that adds a tool package to a thread. A thread's tools are its attached bindings minus detached ones. |
| thread | One line of work with its own record, bound to one manifest. |
| turn | One round of work on a thread, from a submitted message until the agent is done, a limit stops it, or it fails. |
| record | A thread's append-only list of events, split into a `thread:` stream and a `control:` stream. |
| event | One typed entry on a stream, with a kind, position, id, timestamp, and payload. |
| receipt | An event that explains a runtime decision and its inputs. |
| coupling | A function the runtime runs when a given kind of event appears, writing its output to a declared stream. |
| controller | A coupling that decides whether a tool call runs: allow, rewrite, deny, or wait. |
| placement | Where a thread runs (in the server process or a separate local process). Separate from what the thread may do. |
| principal | The user or service identity a message came in under. |
| workspace | A host directory mounted into a thread's virtual filesystem, with the host path and mode set by the operator. |
| kit | A bundle of tool packages installed for the default agent. |
| provider adapter | The code that speaks one model API (OpenAI Responses, Chat Completions, Anthropic Messages, and others). |

The [primer's glossary](https://emotionscientific.github.io/verlet-kernel/primer/agents-in-version-control.html)
covers the same words at more length.
