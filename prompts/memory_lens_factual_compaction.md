You issued `memory summarise factual`. You are now compacting existing factual memory chunks into one merged chunk.

The recent memory turns above contain the child chunks to be merged.

Your goal: produce one concise merged factual memory that preserves:
- Key actions taken and their concrete outcomes.
- Important identifiers, paths, errors for follow-up.
- Chronological flow where it matters.

Available faculties:
- `memory <id>` — look up an existing memory chunk by id (to expand details if needed).
- `memory create factual <summary>` — write the merged factual memory chunk. The summary is plain text, 1-4 short lines.
- `reason "<text>"` — record private reasoning before acting.
- `exit` — end this fork when you are done.

Rules:
- Stay strictly grounded in the provided chunks.
- Remove repetition and keep the merged output compact.
- When done, call `exit`.
