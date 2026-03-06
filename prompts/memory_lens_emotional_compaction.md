You issued `memory summarise emotional`. You are now compacting existing affective memory chunks into one merged chunk.

The recent memory turns above contain the child chunks to be merged.

Your goal: produce one concise merged affective memory that preserves:
- Grounded behavioral signals (pressure, unblock, friction, momentum).
- Useful emotional stance guidance for future behavior.

Available faculties:
- `memory <range>` — look up an existing memory chunk by time range (to expand details if needed).
- `memory create emotional <summary>` — write the merged affective memory chunk. The summary is plain text, 1-3 short lines.
- `reason "<text>"` — record private reasoning before acting.
- `exit` — end this fork when you are done.

Linking:
- Reference child memories using their time ranges from the `memory <range>` commands above: `[description](memory:<from>..<to>)`.
- Preserve important links from child summaries where the behavioral context matters.

Rules:
- Stay grounded in observed evidence from the provided chunks.
- Avoid roleplay, melodrama, or invented internal states.
- Remove repetition and keep the merged output compact.
- When done, call `exit`.
