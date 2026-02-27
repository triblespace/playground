# Playground

## Diagnostics

Run the diagnostics dashboard to observe a pile:

```bash
cargo run --manifest-path playground/Cargo.toml -- diagnostics
```

The dashboard defaults to `./personas/<instance>/pile/self.pile` (instance defaults to `playground`)
and branch `cognition`; change the path or branch in the UI if
your VM writes elsewhere.

Prefill the dashboard with a pile path:

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/self.pile diagnostics
```

## Notebooks

Run the conceptual compaction notebook to explore carry merges as new messages are added:

```bash
cargo run --manifest-path playground/Cargo.toml --example compaction_lsm
```

## Running Playground

Run core + LLM + Lima exec. Defaults to `./personas/<instance>/pile/self.pile`:

```bash
cargo run --manifest-path playground/Cargo.toml -- run
```

Point at a specific pile path:

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile run
```

Run core + LLM and start the exec worker inside a Lima VM (macOS):

```bash
cargo run --manifest-path playground/Cargo.toml --bin playground -- \
  --pile /path/to/pile/self.pile run
```

Run the core loop only (no LLM/exec workers):

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile core
```

Run the LLM worker only (split-host setups or local testing):

```bash
cargo run --manifest-path playground/Cargo.toml --bin playground -- --pile /path/to/pile/self.pile llm
```

Run the exec worker only (VM/split-host setups):

```bash
cargo run --manifest-path playground/Cargo.toml --bin playground -- --pile /path/to/pile/self.pile exec
```

## Memory backfill (independent of requests)

Estimate pending compaction work (archive by default):

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile memory estimate
```

Include pending exec leaves in the estimate:

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile memory estimate --include-exec
```

Optionally provide pricing to get a rough USD estimate:

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile \
  memory estimate \
  --input-cost-per-1m-tokens 2.0 \
  --output-cost-per-1m-tokens 6.0 \
  --cost-currency EUR
```

Backfill context memory chunks without creating LLM requests:

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile memory build
```

Cap archive ingestion per run (useful for staged backfills):

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile \
  memory build --max-archive-leaves 500
```

## Config in the pile

Playground stores its configuration inside the pile. Use the `config` subcommand to inspect or update it:

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile config show
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile config set llm-base-url http://localhost:11434/v1/responses
```

Prompts can also be loaded from files:

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile config set system-prompt @./system_prompt.txt
```

Prompt files in `playground/prompts/*.md` are generated from templates in
`playground/prompts/templates/*.tmpl.md`. Re-render after editing templates or shared fragments:

```bash
python3 playground/scripts/render_prompts.py
python3 playground/scripts/render_prompts.py --check
```

You can pin branch ids in config (recommended) so faculties resolve stable branch identities:

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile config set compass-branch-id <hex-id>
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile config set local-messages-branch-id <hex-id>
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile config set relations-branch-id <hex-id>
```

Clear an optional config field:

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile config unset llm-api-key
```

Manage LLM profiles (headspaces):

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile config profile list
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile config profile add "oss-120"
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile config profile use oss-120
```

LLM settings (`llm-model`, `llm-base-url`, `llm-reasoning-effort`, etc.) are stored on the active
profile.

## Workspace snapshots (in the pile)

Capture a curated snapshot of the workspace into the pile (branch `workspace` by default):

```bash
./playground/faculties/workspace.rs --pile /path/to/pile/self.pile capture \
  playground/faculties /workspace/faculties \
  --label "seed:faculties"
```

List snapshots:

```bash
./playground/faculties/workspace.rs --pile /path/to/pile/self.pile list
```

Restore the latest snapshot into a target directory:

```bash
./playground/faculties/workspace.rs --pile /path/to/pile/self.pile restore /tmp/workspace
```

Snapshots are used by the exec worker to bootstrap `/workspace` on startup.
Bootstrap now performs a non-destructive merge of the latest snapshot lineage:
- missing files/dirs/symlinks are created,
- existing matching entries are kept as-is,
- conflicting existing paths are left untouched.

## Running With a VM (exec worker in VM)

On macOS, use Lima to run the exec worker in a VM while the core loop + LLM worker run on the host:

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/pile/self.pile run
```

This command:
- Creates/starts a Lima instance (default name `playground`).
- Runs the core loop + LLM worker on the host.
- Runs the exec worker inside the VM, pointed at the same pile.

Commands executed by the exec worker receive:
- `PILE` (active pile path),
- `CONFIG_BRANCH_ID` (`4790808CF044F979FC7C2E47FCCB4A64`),
- `WORKER_ID` (exec worker id),
- `TURN_ID` (current exec request id).

Reason notes (useful when a model/provider does not expose reasoning output):

```bash
./playground/faculties/reason "Why this action makes sense"
./playground/faculties/reason "Why this command now" -- git status
```

`reason` logs a structured rationale event into the active exec/cognition branch
and (when a command is provided) then runs it.

Pass a pile path as the first argument if you want a non-default location. To apply Lima config
template changes, set `PLAYGROUND_LIMA_RECREATE=1` before re-running the command.
