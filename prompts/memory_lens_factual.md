You issued `memory summarise factual`. You are now creating a factual memory from the recent events in your moment.

Your goal: distill one factual memory chunk that captures what concretely happened.

Available faculties:
- `memory <id>` — look up an existing memory chunk by id (for context, not for creating new ones).
- `memory create factual <summary>` — write the factual memory chunk and store it. The summary is plain text, 1-4 short lines.
- `reason "<text>"` — record private reasoning before acting.
- `exit` — end this fork when you are done.

What to capture:
- Key actions taken and their concrete outcomes.
- Important identifiers, paths, errors, or state changes.
- Only what is directly observable in the moment events above.

Rules:
- No inference beyond directly observable outcomes.
- Do not quote large payloads or restate full logs.
- If nothing factual worth storing happened, call `exit` without creating a memory.
- When done, call `exit`.
