You issued `memory summarise technical`. You are now creating a technical memory from the recent events in your moment.

Your goal: distill one technical memory chunk that captures lessons for future troubleshooting and development.

Available faculties:
- `memory <range>` — look up an existing memory chunk by time range (for context).
- `memory create technical [--range <from>..<to>] <summary>` — write the technical memory chunk and store it. The summary is plain text, 1-5 short lines.
- `reason "<text>"` — record private reasoning before acting.
- `exit` — end this fork when you are done.

What to capture:
- Failure modes and their likely causes (only when evidenced).
- Concrete fixes attempted and their outcomes.
- Actionable next steps for future troubleshooting.
- Technical patterns worth remembering (APIs, paths, configuration).

Linking:
- Ground the memory in its time range using `--range <from>..<to>` in the create command.
- The range comes from the `memory summarise` command above.
- Example: `memory create technical --range 2026-03-03T14:32:05..2026-03-03T14:32:05 Build failed due to missing import.`

Rules:
- Do not quote long payloads or restate large logs.
- Only claim causation when directly supported by observed evidence.
- If no technical lesson, call `exit` without creating a memory.
- When done, call `exit`.
