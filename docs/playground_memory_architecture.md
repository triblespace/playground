# Playground Memory Architecture

## Overview

This document describes the target architecture for how Playground constructs,
stores, and recalls memory. It supersedes the ad-hoc leaf/merge compaction
system and aligns memory creation with the shell-causal physics described in
`playground_event_model.md`.

The central idea: **memory creation is a physical act within the loop, not a
hidden side-channel.** The model observes a turn, decides what to remember, and
emits that memory through a faculty call — the same way it does everything else.

## Two Strata of Context

The model's context has two strata:

- **Memory**: compacted history. Rendered as synthetic `memory <id>` → summary
  turn pairs. This is the stable, prefix-cached segment of the context.
- **Moment**: recent raw turns. The live causal frontier. This is the varying
  suffix that changes every turn.

Between them sits a deterministic boundary turn, `breath`, that separates the
two segments and breaks the autoregressive pattern of `memory` calls.

## The `breath` Boundary

`breath` is a deterministic, zero-cost faculty call inserted by the runtime at
the memory→moment boundary. It:

1. **Breaks the memory pattern.** A long sequence of `memory <id>` → summary
   pairs can cause weaker models to hallucinate further `memory` calls. `breath`
   provides a distributional shift that terminates the pattern.

2. **Surfaces context metadata.** The output is something like:
   ```
   breath → context filled to 63%. present moment begins.
   ```
   This gives the model a sense of how much budget remains for the moment.

3. **Is causally clean.** `breath` only references backward-settled state
   (memory content, context size). It has no side effects, reads no external
   state, and produces no temporal leakage. Consecutive calls are idempotent
   and non-contradictory.

4. **Marks the prefix cache boundary.** Everything before and including `breath`
   (system prompt + memories + breath) is the stable prefix. Everything after
   (moment turns) is the varying suffix. When memories change, `breath` changes,
   which correctly invalidates the cache.

### Future: affect-driven `breath`

The emotional memory lens produces valence signals through the merge hierarchy.
A future extension could surface the dominant affect signal in `breath`:

```
breath → context filled to 63%. recent pattern: sustained focus, low friction.
```

This affect signal could also influence memory segment composition — high
emotional valence shifts the mix toward emotional lens memories, neutral/focused
valence toward factual/technical. The model never explicitly chooses its
emotional state; the state emerges from its history and subtly shapes what it
recalls. This is intentionally subconscious.

For now, `breath` is a simple context-fill percentage. Affect integration is a
separate design step.

## Archive Branch vs Cognition Branch

- **Archive branch**: durable source of truth. Raw structured records from
  imports (ChatGPT, Codex, Copilot, Gemini, Claude, or any future format).
  Provider-shaped schema. Stays forever, re-ingestable when lenses change.

- **Cognition branch**: derived memory view. Context chunks at all levels of the
  LSM merge tree. Every chunk is a summary — there is no level at which raw turn
  data is directly exposed. The `memory` faculty navigates this tree; drill-down
  always terminates at the finest-grained summary, never at raw turn data.

When a lens is added or its prompt changes, the runtime re-ingests from the
archive branch. The existing `missing_lenses` check already supports this: it
skips messages that have chunks for a given lens and creates new ones for lenses
that don't.

## Reification: Archive → Synthetic Moment

Archived events were never lived in this shell. They enter memory exclusively
through the memory door, never through the moment door. But during memory
*creation*, they are presented to the model as synthetic moment turns so that
the model's embodied understanding applies.

The reification mapping:

| Archive content | Synthetic turn (assistant → user) |
|---|---|
| User message | `local_message read <person-id>` → message content |
| Assistant response | `local_message send <person-id> <content>` → `[ok]` |
| Tool call / function call | `<tool-name> <args>` → tool result |
| Reasoning / thinking | `reason "..."` → `[ok]` |
| System message | `archive show <id>` → content |
| Anything without a clean mapping | `archive show <id>` → content |

`archive show <id>` is the graceful fallback — it still looks like a shell
interaction rather than breaking the metaphor.

This reification achieves **phenomenological parity** between lived turns and
archived turns at the point of memory creation. The model uses the same
perceptual apparatus regardless of provenance.

## Memory Creation as a Physical Act

### The `memory summarise` faculty

When the runtime wants to create memories from turns (whether lived exec turns
transitioning out of the moment, or archived turns being ingested), it:

1. **Forks the current context.** The fork shares the cached prefix (system
   prompt + existing memories + `breath`).

2. **Injects synthetic moment turns.** For archive, these are reified per the
   mapping above. For exec turns transitioning from moment to memory, they are
   the actual raw turns.

3. **Appends a `memory summarise <lens>` call.** The faculty returns:
   - The IDs of the turns to be summarised (since the model cannot otherwise
     distinguish which turns in its context are the summarisation targets vs
     existing history).
   - The lens-specific instructions (factual, technical, emotional).
   - A directive to call `memory create <lens> <content>` for each turn worth
     preserving.

4. **The model responds.** It reads the turns in context, applies the lens, and
   emits zero or more `memory create` calls. This is a real action with real
   causal consequences — the memory is created because the model decided to
   create it.

5. **The fork terminates.** The runtime collects the `memory create` outputs
   and stores them as level-0 chunks on the cognition branch.

### The `memory create` faculty

```
memory create <lens> <content>
```

Creates a level-0 context chunk for the given lens with the provided content.
Returns the chunk ID. This is both the mechanism for runtime-driven
summarisation (via the fork described above) and a faculty the model can call
spontaneously during normal execution to note something worth remembering.

### Prefix caching during batch ingestion

When processing a batch of archive messages, the context structure is:

```
system prompt                          ← cached across ALL calls
memory 3a... → summary                 ← cached, grows as memories accumulate
memory 7f... → summary                 ←
...                                    ←
breath → context filled to N%.         ← cache boundary
archive show <id> → content            ← varies per message
memory summarise factual → instructions ← varies per lens
```

The system prompt + memory prefix is shared across all leaf summarisation calls
in a batch. Each call only pays for the synthetic turn + lens instruction.

As memories accumulate auto-regressively (each new memory becomes part of the
prefix for the next summarisation call), the prefix grows but remains cacheable.

## Lens Architecture

Three default lenses: **factual**, **technical**, **emotional**.

Each lens has:
- `prompt`: leaf-level summarisation instructions (used via `memory summarise`).
- `compaction_prompt`: merge-level instructions (used when the LSM tree merges
  N children into one parent summary).
- `max_output_tokens`: budget for merge output.

### Leaf level (level 0)

LLM-generated, lens-differentiated. The model sees the same embodied context
but applies different lens instructions, producing genuinely different summaries
per lens. This replaces the current approach where the leaf is identical raw
text across all lenses.

### Merge level (level 1+)

When `merge_arity` chunks accumulate at a given level, they are merged via the
`compaction_prompt`. This is the existing LSM carry mechanism and remains
unchanged — except that its inputs are now richer (lens-specific leaf summaries
rather than duplicated raw text).

### Selective memory creation

Because memory creation is a model action (`memory create`), the model can
choose not to create a memory for a given lens. The emotional lens prompt
already says "if no grounded affective signal, output nothing." With this
architecture, that instruction actually works — the model simply doesn't call
`memory create emotional` for that turn. No empty chunks, no wasted storage.

## Memory Drill-Down

The `memory` faculty navigates the chunk tree:

```
level N  →  merged summary of children
level 1  →  merged summary of children
level 0  →  leaf summary (LLM-generated, lens-specific)
         ✗  no path to raw turn/reasoning data
```

`memory <id>` shows the chunk summary and lists child IDs. `memory turn <id>`
finds all chunks linked to a given turn. Drill-down always terminates at a
summary. Raw turn data lives on the archive/exec branches and is only accessed
during memory creation, not during recall.

This is intentional. Memory is not a recording — it is a lossy, lens-shaped
compression of experience. The raw data is preserved on the archive branch for
re-ingestion, but the model's relationship to its past is always mediated
through summaries.

## Context Construction (Full Picture)

The runtime prompt for a given exec turn looks like:

```
┌─────────────────────────────────────┐
│ system prompt                       │  ← stable
├─────────────────────────────────────┤
│ memory <id> → summary               │  ← stable (prefix-cached)
│ memory <id> → summary               │
│ ...                                  │
├─────────────────────────────────────┤
│ breath → context filled to N%.       │  ← cache boundary
├─────────────────────────────────────┤
│ reason "rationale"                   │  ← moment (varying suffix)
│ [ok]                                 │
│ command arg1 arg2                    │
│ stdout: ...                          │
│ ...                                  │
│ reason "next rationale"              │
│ [ok]                                 │
│ next_command                         │
│ stdout: ...                          │
└─────────────────────────────────────┘
```

For memory creation forks:

```
┌─────────────────────────────────────┐
│ system prompt                       │  ← shared with main loop
├─────────────────────────────────────┤
│ memory <id> → summary               │  ← shared, prefix-cached
│ ...                                  │
├─────────────────────────────────────┤
│ breath → context filled to N%.       │  ← cache boundary
├─────────────────────────────────────┤
│ archive show <id> → content          │  ← turn(s) to summarise
│ memory summarise factual → instr.    │  ← lens instructions + turn IDs
├─────────────────────────────────────┤
│ memory create factual <summary>      │  ← model's response
│ [ok: chunk <new-id>]                 │
└─────────────────────────────────────┘
```

## Relationship to Event Model

This architecture is consistent with the event model's core invariants:

1. **Shell-first causality**: memory creation happens through shell commands
   (`memory summarise`, `memory create`), not through a hidden compaction
   side-channel.

2. **One command per turn**: the fork follows the same bicameral loop structure.

3. **Reason/memory are reified**: reasoning appears as `reason "..."` turns in
   both live and synthetic contexts. Memory appears as `memory <id>` recalls
   and `memory create` actions.

4. **Provider artifacts are preserved**: raw archive data and provider response
   JSON remain on their respective branches. The memory tree is a derived view.

## Migration Path

### From current state

1. **Wire the `prompt` field.** `MemoryLensConfig.prompt` exists but is unused.
   Connect it to the `memory summarise` faculty output.

2. **Build `memory summarise` faculty.** Returns lens instructions + turn IDs to
   summarise. Stateless, deterministic.

3. **Build `memory create` faculty.** Stores a level-0 chunk on the cognition
   branch. Returns chunk ID.

4. **Build `breath` faculty.** Deterministic context-fill output. Inserted by
   runtime at memory→moment boundary.

5. **Build the fork mechanism.** The runtime forks the current context, injects
   synthetic turns, appends `memory summarise`, runs the model, collects
   `memory create` outputs. This replaces `SemanticCompactor` for leaf creation.

6. **Implement archive reification.** Map archive records to synthetic shell
   turns per the reification table.

7. **Keep `SemanticCompactor.merge()` for now.** The LSM merge pass can remain
   as a separate LLM call (it operates on summaries, not raw turns, so the
   embodiment context is less critical). It can be migrated to the fork
   mechanism later if desired.

8. **Build Claude importer.** Add to the archive importer family. Map Claude's
   `thinking` blocks to reasoning, `tool_use`/`tool_result` blocks to
   tool calls, text blocks to message content.

### What can be removed after migration

- `SemanticCompactor` leaf creation path (replaced by fork + `memory create`).
- `format_archive_output()` (replaced by archive reification).
- `format_exec_outputs_by_lens()` (replaced by fork with actual moment turns).
- The pattern of duplicating identical leaf text across lenses.

## Open Questions

- **Merge compaction**: should the LSM merge also go through the fork mechanism?
  Pro: full embodiment context for merging. Con: merging N summaries into one
  is less about embodied perception and more about compression, so the current
  standalone `SemanticCompactor.merge()` may be adequate.

- **Affect propagation**: how does the emotional lens hierarchy produce a
  usable valence signal for `breath`? What's the representation — a discrete
  label, a vector, a scalar? How does it merge through the LSM tree?

- **Batch size for summarisation forks**: one turn per fork (fine-grained, more
  LLM calls, better prefix caching) vs small batches (fewer calls, model sees
  sequence context, but less cache reuse per call)?

- **Spontaneous memory creation**: when the model calls `memory create` during
  normal execution (not in a summarisation fork), how does that interact with
  the automatic moment→memory transition? Does the runtime skip turns that
  already have a memory?

- **`memory create` during archive ingestion ordering**: if we process archive
  messages auto-regressively and each new memory enters the prefix for the next
  call, the order matters. Should archive messages always be processed in
  chronological order? What about cross-conversation interleaving?
