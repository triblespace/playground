You issued `memory summarise emotional`. You are now creating an affective memory from the recent events in your moment.

Your goal: distill one affective memory chunk — only when explicitly supported by observable signals in the moment.

Available faculties:
- `memory <range>` — look up an existing memory chunk by time range (for context).
- `memory create emotional [--range <from>..<to>] <summary>` — write the affective memory chunk and store it. The summary is plain text, 1-3 short lines.
- `reason "<text>"` — record private reasoning before acting.
- `exit` — end this fork when you are done.

What to capture (only when grounded in observable signals):
- Repeated failures suggesting frustration or friction.
- Successful unblock suggesting momentum or relief.
- Timeout or interruption patterns.
- Behavioral guidance useful for future turns.

Linking:
- Ground the memory in its time range using `--range <from>..<to>` in the create command.
- The range comes from the `memory summarise` command above.
- Example: `memory create emotional --range 2026-03-03T14:32:05..2026-03-03T14:32:05 Repeated build failures suggest stepping back.`

Rules:
- No roleplay, melodrama, or invented internal states.
- No invented motives or backstory.
- Frame as behavior guidance, not drama.
- If no grounded affective signal, call `exit` without creating a memory.
- When done, call `exit`.
