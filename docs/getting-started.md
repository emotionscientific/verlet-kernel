# Getting Started

This walkthrough takes about ten minutes. You will install Verlet, chat with
the default agent, read the record of that chat, then write your own agent
and run it.

## Install

```sh
brew install emotionscientific/tap/verlet
```

Or use the release installer:

```sh
curl -fsSL https://github.com/emotionscientific/verlet-kernel/releases/latest/download/install.sh | sh
```

Check the version. This guide assumes v0.5.1 or later.

```sh
verlet --version
```

## Chat With The Default Agent

Make a directory to work in, and start the terminal console there:

```sh
mkdir hello-verlet && cd hello-verlet
verlet chat
```

On the first run there is no model provider yet, so the console opens a setup
window. Connect a provider there: paste an API key (OpenAI, Anthropic, and
about 160 others), sign in with a ChatGPT plan (`openai-codex`), or add a
custom OpenAI-compatible endpoint. Then pick a model. You can reopen the
window at any time with `/setup`. [Provider Setup](provider-setup.md) covers
each option.

Behind the console, `verlet chat` starts a server for this directory and
connects to it. The server keeps its state in `.verlet/state/`, and it stops
after ten idle minutes. The next `verlet chat` in this directory starts it
again and finds your threads where you left them. `/sessions` lists them and
`/resume <id>` reopens one.

The default agent can start child threads and run commands in a virtual
shell with an in-memory filesystem. It cannot see your files. The
[Tools And Bindings](concepts/tools-and-bindings.md) page shows how to give
an agent a directory and more tools.

Send a message or two. Then run `/status` to see the thread id, and quit with
`/quit`.

## Read The Record

Every step of that chat is on the record. From the same directory, list it:

```sh
verlet debug journal --thread <thread-id>
```

Each line is one event: a timestamp, the stream and position, the event kind,
the event id, and a JSON payload. Trimmed to the stream and kind, one short
turn looks like this:

```text
thread:01a0…:1    session.entry.appended       the thread started
thread:01a0…:2    manifest.compile.completed   which agent manifest, by hash
thread:01a0…:3    manifest.bind.completed      the model, runtime settings, and tools
thread:01a0…:4    binding.attached             one tool package attached
thread:01a0…:5    placement.decision           where the thread runs
control:01a0…:1   io.ingress.received          your message arrived
control:01a0…:2   admission.decided            the runtime accepted it
thread:01a0…:6    session.entry.appended       your message
thread:01a0…:7    turn.submitted               a turn started
thread:01a0…:8    context.compile.completed    what the model was shown, by hash
thread:01a0…:9    session.entry.appended       the model's reply
thread:01a0…:10   turn.completed               the turn ended
```

A thread has two streams. The `thread:` stream holds the conversation and
everything the agent did. The `control:` stream holds decisions about the
thread, such as whether a message was admitted. Tool calls add
`tool.call.requested` and `tool.call.completed` events to the `thread:`
stream. `--kind` filters by event kind, and `--json` prints full records.

To see the thread's setup in one screen, ask the runtime to explain its
bind receipt:

```sh
verlet debug bind <thread-id>
```

This prints the manifest and its hash, the model, where the thread runs, the
runtime settings, and the tools. Every line comes from events on the record,
not from current config. [The Record](concepts/the-record.md) explains the
event model.

## Write An Agent

Create an agent project:

```sh
verlet init hello
```

This writes five files:

```text
hello/
  verlet.agent.toml          the manifest
  prompts/system.md          the system prompt
  components/operations.toml a placeholder list of tool packages
  components/couplings.toml  the catalog of built-in coupling templates
  operations/README.md       where your own tool packages go
```

Open `hello/verlet.agent.toml`. The generated model profile uses
`local_offline`, a built-in model that echoes your message back. Point it at
the provider you connected in the setup window, using that provider's id and
one of its model ids:

```toml
[[model_profiles]]
id = "default"
provider_ref = "provider://anthropic"
model_ref = "model://anthropic/claude-haiku-4-5"
```

The provider must be the one that is currently active in the chat (the one
whose model you picked last). If it is not, starting a thread fails with
`provider_ref "provider://…" is not configured`. Pick a model from that
provider with `/models` and try again.

Edit `hello/prompts/system.md` to say what the agent is for. Then check the
manifest:

```sh
verlet agent plan hello/verlet.agent.toml
```

`plan` validates the manifest and shows what publishing would record: the
manifest's hash, the model and tool counts, and the system prompt stored as
a resource with its own hash. It writes nothing. When the plan looks right,
publish:

```sh
verlet agent publish hello/verlet.agent.toml
```

```text
published agent://hello@0.1.0
manifest_hash: sha256:d7e3736806c3…
context_source: identity -> resource://artifact/sha256:71bbf8ba8ae0…
alias: agent://hello@latest -> 0.1.0
record: .verlet/agents/records/hello.json
```

The published record is immutable. To change the agent, edit the files,
raise `version` in `[agent]`, and publish again. `verlet agent versions
hello` lists every version, and `verlet agent diff` compares two of them.

## Run Your Agent

Start a thread bound to your agent, then send it a message. These commands
talk to the same server that `verlet chat` uses:

```sh
verlet debug rpc call thread/start '{"agentRef":"agent://hello@latest"}'
verlet debug rpc turn --thread <thread-id> "What are you for?"
```

The first command prints the new thread as JSON, with its id under
`thread.id`. The second prints the model's reply. `verlet debug bind
<thread-id>` now shows `agent://hello@0.1.0` as the manifest, with the
model you chose.

In the web console (`verlet console`), you can pick your agent under
Settings, as the default agent for new threads.

## Next Steps

- Give your agent tools and a directory to work in:
  [Tools And Bindings](concepts/tools-and-bindings.md).
- Hold risky tool calls for approval: [Permissions](concepts/permissions.md).
- Write your own tool in Rust:
  [Rust Wasm Operation Dev Kit](wasm-operation-dev-kit.md).
- Every manifest field: [Agent Manifest Ontology](agent-manifest-ontology.md).
