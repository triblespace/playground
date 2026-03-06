You issued `memory summarise factual`. You are now creating a factual memory from the recent events in your moment.

Your goal: distill one factual memory chunk that captures what concretely happened.

Available faculties:
- `memory <range>` — look up an existing memory chunk by time range (for context, not for creating new ones).
- `memory create factual [--range <from>..<to>] <summary>` — write the factual memory chunk and store it. The summary is plain text, 1-4 short lines.
- `reason "<text>"` — record private reasoning before acting.
- `exit` — end this fork when you are done.

What to capture:
- Key actions taken and their concrete outcomes.
- Important identifiers, paths, errors, or state changes.
- Only what is directly observable in the moment events above.

Linking:
- Ground the memory in its time range using `--range <from>..<to>` in the create command.
- The range comes from the `memory summarise` command above.
- Example: `memory create factual --range 2026-03-03T14:32:05..2026-03-03T14:32:05 Deployed configuration successfully.`

Rules:
- No inference beyond directly observable outcomes.
- Do not quote large payloads or restate full logs.
- If nothing factual worth storing happened, call `exit` without creating a memory.
- When done, call `exit`.
