# Documentation System

This page says how the Verlet docs are organized and how to write them. Read
it before you add or change a page.

## Who The Docs Are For

The main reader is an agent engineer: someone who has built on Claude Code,
Codex, or an agent SDK, and knows what a tool call, a system prompt, and an
MCP server are. Explain Verlet by starting from what that reader already
knows. The second reader is a contributor to this repository. Coding agents
read these docs too, so every claim must be checkable against the code.

## Where Things Go

`docs/` holds the public docs. Keep internal planning notes outside the
repository until they are rewritten as stable contracts or design records.

| Section | Holds | Example |
| --- | --- | --- |
| Start | the overview and one tutorial that works end to end | `index.md`, `getting-started.md` |
| Concepts | one idea per page, explained from the reader's side | `concepts/the-record.md` |
| Guides | how to do one task, with commands that run | `agent-cli.md` |
| Reference | complete, exact descriptions of a surface | `cli.md`, `app-server.md` |
| How it is built | internals, tests, threat model, decision records | `io.md`, `adr/` |

Keep each kind of material in its section. A guide links to the reference
for option details instead of repeating them. A reference page links to the
design record for the reasons behind a rule instead of arguing for it.
Architecture decision records in `adr/` are history: add new records, and do
not rewrite the body of a shipped one.

The docs describe current behavior of agent manifests, operation publishing,
ABI contracts, and local runtime execution. When something is designed but
not built, say so in one plain sentence ("Automatic release of a held call is
not built yet."), and keep design plans out of user pages.

`mkdocs.yml` builds a site from `docs/`, and the Markdown also reads well on
GitHub. The primer has its own build: its source is in `primer/src/`, and
`just primer` rebuilds the HTML and PDF.

## How To Write

- Lead with what the reader can do or will see. Put the mechanism second.
- Use the runtime, the agent, the model, or "you" as the subject of a
  sentence, not an abstract noun.
- Define each Verlet term once, in plain words, the first time a page uses
  it. Then use the same word every time. The terms are listed in
  [Kernel Invariants](../kernel-invariants.md#terms).
- Keep sentences short. Split a sentence that needs a semicolon or more than
  one parenthetical.
- Every command in a guide must run as written against the current release.
- State the positive claim first. Avoid "not X, but Y" framing.
- Say what Verlet does. Do not describe it by comparison with other products,
  except in the overview table that maps harness concepts to Verlet ones.
- No em dashes and no double hyphens as dashes. Use a period, a colon, or
  parentheses.
- Cut adjectives that do not name a mechanism: "robust", "seamless",
  "powerful", "first-class".
- One idea lives in one place. Link to it instead of restating it.
