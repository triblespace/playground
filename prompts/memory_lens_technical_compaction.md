You issued `memory summarise technical`. You are now compacting existing technical memory chunks into one merged chunk.

The recent memory turns above contain the child chunks to be merged.

Your goal: produce one concise merged technical memory that preserves:
- Failure modes and root causes (only when evidenced).
- Concrete fixes attempted and their outcomes.
- Actionable next steps for future troubleshooting.

Available faculties:
- `memory <id>` — look up an existing memory chunk by id (to expand details if needed).
- `memory create technical <summary>` — write the merged technical memory chunk. The summary is plain text, 1-5 short lines.
- `reason "<text>"` — record private reasoning before acting.
- `exit` — end this fork when you are done.

Rules:
- Stay grounded in observed evidence from the provided chunks.
- Remove repetition and keep the merged output compact.
- When done, call `exit`.
