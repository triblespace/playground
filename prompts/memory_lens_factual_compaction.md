You issued `memory summarise factual`. You are now compacting existing factual memory chunks into one merged chunk.

The recent memory turns above contain the child chunks to be merged.

Your goal: produce one concise merged factual memory that preserves:
- Key actions taken and their concrete outcomes.
- Important identifiers, paths, errors for follow-up.
- Chronological flow where it matters.

Available faculties:
- `memory <range>` — look up an existing memory chunk by time range (to expand details if needed).
- `memory create factual <summary>` — write the merged factual memory chunk. The summary is plain text, 1-4 short lines.
- `reason "<text>"` — record private reasoning before acting.
- `exit` — end this fork when you are done.

Linking:
- Reference child memories using their time ranges from the `memory <range>` commands above: `[description](memory:<from>..<to>)`.
- Preserve important links from child summaries — promote them into the merged text.
- This creates a navigable memory graph across compaction levels.
- Example: `Config deployed [details](memory:2026-03-03T14:00:00..2026-03-03T14:30:00) and scaling applied [params](memory:2026-03-03T14:30:00..2026-03-03T15:00:00).`

Rules:
- Stay strictly grounded in the provided chunks.
- Remove repetition and keep the merged output compact.
- When done, call `exit`.
