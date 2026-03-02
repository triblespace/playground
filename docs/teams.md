# Teams Faculty

The Teams faculty (`playground/faculties/teams.rs`) ingests Microsoft Teams messages via the
Microsoft Graph delta API and writes them into TribleSpace.
The `read` command always syncs from Graph before reading the pile.

## Storage Model

- Messages are stored as `archive::kind_message` with `archive::author`,
  `archive::created_at`, and `archive::content`.
- Authors are `archive::kind_author` entities; `teams::user_id` captures the
  Teams user id when available.
- Chats are `teams::kind_chat` entities linked by `teams::chat` and
  `teams::chat_id`.
- Raw message JSON is stored in `teams::message_raw` for provenance.
- The delta cursor is stored as a `teams::kind_cursor` entity with
  `teams::delta_link` and `archive::created_at`.
- App credentials and the derived user id are stored as `teams::kind_config`
  with `teams::tenant`, `teams::client_id`, `teams::client_secret`, and
  `teams::user_id`.
- Attachments referenced by messages are fetched during sync and stored as
  `archive::kind_attachment` with `archive::attachment_data` (file bytes),
  `archive::attachment_source_id`, and optional name/mime/size metadata. Messages
  link to attachments via `archive::attachment`.
- Ingest errors are appended as `teams::kind_log` in the log branch (default:
  `logs`).

## Usage

```
playground/faculties/teams.rs
playground/faculties/teams.rs send <chat_id> "Hello from the agent"
playground/faculties/teams.rs users list "Jan"
playground/faculties/teams.rs users list
playground/faculties/teams.rs presence set Available
playground/faculties/teams.rs presence get <user_id>
playground/faculties/teams.rs chat create <user_id> <user_id>
playground/faculties/teams.rs chat create --group --topic "Project Updates" <user_id>
playground/faculties/teams.rs chat invite <chat_id> <user_id>
playground/faculties/teams.rs presence set Busy --activity InAConferenceCall --duration-mins 30
playground/faculties/teams.rs login --client-id <app-id> --tenant <tenant-id> --client-secret <secret>
playground/faculties/teams.rs read --limit 10 --descending
playground/faculties/teams.rs read <chat_id> --limit 10
playground/faculties/teams.rs attachments list --chat-id <chat_id> --limit 10 --descending
playground/faculties/teams.rs attachments backfill --chat-id <chat_id> --limit 50
playground/faculties/teams.rs attachments export <source_id> ./attachments
```

## Configuration

- `--pile` (env: `PILE`, default: `./self.pile`)
- `--branch` (env: `TRIBLESPACE_BRANCH`, default: `teams`)
- `--delta-url` (env: `TEAMS_DELTA_URL`, default:
  `https://graph.microsoft.com/v1.0/users/{user_id}/chats/getAllMessages/delta`)
- `--token` (env: `TEAMS_TOKEN`)
- `--token-command` (env: `TEAMS_TOKEN_COMMAND`, default: Azure CLI)
- `TRIBLESPACE_LOG_BRANCH` overrides the log branch (default: `logs`).
- `read` supports an optional positional `chat_id` plus `--since`, `--limit`, and `--descending` for
  local history queries and always syncs before reading.
- `send` requires a delegated token with `ChatMessage.Send` permission (from
  `--token`, `--token-command`, or `login`).
- `presence set` uses a delegated token and requires `Presence.ReadWrite` (included in
  the default `login` scopes; re-run login if the scope is missing).
- `presence set` accepts availability values: `Available`, `Busy`, `Away`, `DoNotDisturb`;
  activity values: `Available`, `InACall`, `InAConferenceCall`, `Away`, `Presenting`.
- `presence get`, `users list`, `chat create`, and `chat invite` require additional
  delegated scopes; see `teams_scopes.md`.
- `attachments export` writes the attachment data to disk (defaults to `./attachments`)
  and uses the stored attachment name or source id for the filename; add `--overwrite`
  to replace an existing file.
- Sync uses app-only client credentials stored in the pile by `login`
  (tenant, client id, client secret, and derived user id).
- `login` uses the device-code flow and stores a refreshable delegated token in
  the pile branch, plus the app config (including user id).
- Credentials are stored in plaintext inside the Teams pile branch by design.
