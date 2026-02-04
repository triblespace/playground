# Playground

## Dashboard

Run the GORBIE dashboard example to observe a pile:

```bash
cargo run --manifest-path playground/Cargo.toml --example playground_dashboard
```

The dashboard defaults to `./self.pile` and branch `main`; change the path or branch in the UI if
your VM writes elsewhere.

## Running Playground

Run core + LLM + Lima exec. Defaults to `./self.pile`:

```bash
cargo run --manifest-path playground/Cargo.toml -- run
```

Point at a specific pile path:

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/self.pile run
```

Run core + LLM and start the exec worker inside a Lima VM (macOS):

```bash
cargo run --manifest-path playground/Cargo.toml --bin playground -- \
  --pile /path/to/self.pile run
```

Run the core loop only (no LLM/exec workers):

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/self.pile core
```

Run the LLM worker only (split-host setups or local testing):

```bash
cargo run --manifest-path playground/Cargo.toml --bin playground -- --pile /path/to/self.pile llm
```

Run the exec worker only (VM/split-host setups):

```bash
cargo run --manifest-path playground/Cargo.toml --bin playground -- --pile /path/to/self.pile exec
```

## Config in the pile

Playground stores its configuration inside the pile. Use the `config` subcommand to inspect or update it:

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/self.pile config show
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/self.pile config set llm-base-url http://localhost:11434/v1/responses
```

Prompts can also be loaded from files:

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/self.pile config set system-prompt @./system_prompt.txt
```

## Workspace snapshots (in the pile)

Capture a curated snapshot of the workspace into the pile (branch `workspace` by default):

```bash
./playground/faculties/workspace.rs --pile /path/to/self.pile capture \
  playground/faculties /workspace/faculties \
  --label "seed:faculties"
```

List snapshots:

```bash
./playground/faculties/workspace.rs --pile /path/to/self.pile list
```

Restore the latest snapshot into a target directory:

```bash
./playground/faculties/workspace.rs --pile /path/to/self.pile restore /tmp/workspace
```

Snapshots are used by the exec worker to bootstrap a workspace inside the VM when
`PLAYGROUND_WORKSPACE_BOOTSTRAP=1` and the target directory is empty.

## Running With a VM (exec worker in VM)

On macOS, use Lima to run the exec worker in a VM while the core loop + LLM worker run on the host:

```bash
cargo run --manifest-path playground/Cargo.toml -- --pile /path/to/self.pile run
```

This command:
- Creates/starts a Lima instance (default name `playground`).
- Runs the core loop + LLM worker on the host.
- Runs the exec worker inside the VM, pointed at the same pile.

Pass a pile path as the first argument if you want a non-default location. To apply Lima config
template changes, set `PLAYGROUND_LIMA_RECREATE=1` before re-running the command.
