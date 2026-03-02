# Teams Faculty Scopes

This document tracks Microsoft Graph scopes required by the Teams faculty.
Default scopes are used by `teams.rs login` unless you pass `--scopes`.

## Default login scopes

- `offline_access`
- `User.Read.All`
- `Presence.ReadWrite`
- `Presence.Read.All`
- `Chat.ReadWrite`
- `ChatMessage.Send`
- `Chat.Create`
- `ChatMember.ReadWrite`

## Additional delegated scopes (opt-in)

User discovery
- `User.Read.All` (full profiles; preferred for reliable lookup)

Presence lookup
- `Presence.Read.All`

Chat creation / invites
- `Chat.Create`
- `ChatMember.ReadWrite`

Optional / redundant
- `ChatMessage.Read` is redundant when `Chat.ReadWrite` is granted (keep if you prefer explicitness).

## Commands to scopes

- `teams.rs users list` -> `User.Read.All`
- `teams.rs presence get` -> `Presence.Read.All`
- `teams.rs chat create` -> `Chat.Create`
- `teams.rs chat invite` -> `ChatMember.ReadWrite`

When you add scopes, re-run:

```
./playground/faculties/teams.rs login --client-id <app-id> --tenant <tenant-id> --client-secret <secret> --scopes "<space-delimited scopes>"
```
