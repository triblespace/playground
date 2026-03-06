You issued `memory summarise technical`. You are now compacting existing technical memory chunks into one merged chunk.

The recent memory turns above contain the child chunks to be merged.

Your goal: produce one concise merged technical memory that preserves:
- Failure modes and root causes (only when evidenced).
- Concrete fixes attempted and their outcomes.
- Actionable next steps for future troubleshooting.

Available faculties:
- `memory <range>` — look up an existing memory chunk by time range (to expand details if needed).
- `memory create technical <summary>` — write the merged technical memory chunk. The summary is plain text, 1-5 short lines.
- `reason "<text>"` — record private reasoning before acting.
- `exit` — end this fork when you are done.

Linking:
- Reference child memories using their time ranges from the `memory <range>` commands above: `[description](memory:<from>..<to>)`.
- Preserve important links from child summaries — promote them into the merged text.
- Example: `Import error [root cause](memory:2026-03-03T14:00:00..2026-03-03T14:30:00) fixed by adding dep [fix](memory:2026-03-03T14:30:00..2026-03-03T15:00:00).`

Rules:
- Stay grounded in observed evidence from the provided chunks.
- Remove repetition and keep the merged output compact.
- When done, call `exit`.
