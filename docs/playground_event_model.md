# Playground Event Model (Shell Physics + Mistral Adapter)

## Why

Playground treats the shell as the model's physical reality:

- the model emits one command string,
- the runtime executes it,
- the model observes concrete command output.

To keep reasoning/memory grounded, reified cognition must follow the same
causal interface (shell actions and shell observations), not a hidden side
channel.

## Core Invariants

1. **Shell-first causality**: model-visible state advances through command/result
   transitions.
2. **One side-effecting command per turn**: the command/result pair is the lived
   primitive.
3. **Reason/memory are reified**: they are modeled as shell-native operations.
4. **Provider artifacts are preserved**: rich transport data is retained, but
   projected through shell-causal views for context construction.

## Canonical Model

Use two peer record streams (linked, not subordinate):

- **Turns**: command/result records in shell causality.
- **Artifacts**: provider/faculty artifacts (reasoning chunks, memory chunks,
  ids, encrypted reasoning payloads, provenance metadata).

Artifacts link to turns (for ownership/provenance), but are not flattened into
ad-hoc monolithic text blobs by default.

## Provider Mapping

### Chat-Completions -> Canonical

- Preserve provider response JSON (`response_raw`) losslessly.
- Project model output command into a turn.
- Capture provider thinking/reasoning text as reasoning artifacts attached to the turn.
- Capture explicit `reason` faculty outputs as reason artifacts with the same
  shape class as provider reasoning (different provenance).

### Canonical -> Chat-Completions

- Build prompt messages from canonical turns/artifacts.
- Reify reasoning as shell-native synthetic turns (e.g. `reason "..."` + ack
  output) where needed.

## Prompt Construction Rules

- `moment`: recent raw command/result turns (+ compatible reasoning artifacts).
- `memory`: compacted recalled history via `memory` faculty projections.
- Do not rely on a generic `reasoning:` text side channel as the primary model.
- Reasoning should be visible as provider reasoning artifacts or shell-reified
  `reason` turns.

## Concrete Migration Steps

1. **Projection contract**
   - Define one canonical projection shape for turn + reasoning artifact links.
   - Keep existing `llm_chat::reasoning_text` as compatibility fallback during
     migration.
2. **Context builder**
   - Stop using `reasoning:` inline section as primary source for turn context.
   - Build context from turn records + artifact projection (moment then memory).
3. **Reason faculty alignment**
   - Remove mixed-mode reason text echo duplication in stderr; keep id/ack.
   - Keep reason content available once in causal stream.
4. **Reason artifact continuity**
   - Keep provider reasoning artifacts attached to turns.
   - Keep shell-reified `reason` turns as the provider-agnostic path.
5. **Diagnostics**
   - Show artifact provenance (`provider` vs `reason` faculty) in timeline rows.
   - Keep raw-response inspection available for debugging.
6. **Cleanup**
   - After parity validation, remove redundant fallback paths and dead
     `reasoning:`-centric joins.

## Notes

- This model keeps the shell-causal worldview coherent for the agent while still
  preserving richer provider artifacts for replay, migration, and debugging.
- It intentionally separates **storage fidelity** from **model-facing causal
  projection**.
