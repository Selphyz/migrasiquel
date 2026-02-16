# Repository Guidelines

## Project Structure & Module Organization
Migrasquiel is a single binary crate under `src`. Entry point `src/main.rs` wires the CLI to the async runtime. Command parsing lives in `src/cli.rs`; operational logic is split into `src/dump.rs`, `src/restore.rs`, and `src/migrate.rs`. Backend implementations live in `src/engine`, currently `mysql.rs` behind `mod.rs`. Shared helpers, including SQL escaping utilities and tests, sit in `src/util`. Inline `#[cfg(test)]` modules accompany the code they exercise; add new ones near the feature they cover.

## Build, Test, and Development Commands
Use `cargo build` for debug builds during development and `cargo build --release` before distributing binaries (`target/release/migrasquiel`). Run the tool locally with `cargo run -- <subcommand>`; for example, `cargo run -- dump --help` prints the usage. Keep formatting strict with `cargo fmt`. Static analysis is enforced with `cargo clippy --all-targets --all-features`. Execute tests via `cargo test`, which will run the module-level suites and any future integration tests.

## Coding Style & Naming Conventions
Follow `rustfmt` defaults (4-space indent, trailing commas, crate ordering). Modules and files stay `snake_case`, types `PascalCase`, and constants `SCREAMING_SNAKE_CASE`. Favor expressive function names that mirror CLI terminology (e.g., `build_dump_command`). When introducing Clap arguments, keep long help text in doc comments and prefer explicit `value_parser` declarations.

## Testing Guidelines
Prefer focused unit tests collocated with the implementation; name test functions after the scenario (`dump_stream_handles_gzip`). Asynchronous logic should use `#[tokio::test(flavor = "multi_thread")]` to match runtime defaults. Integration tests can land in a future `tests/` directory once cross-command flows stabilize. Strive to cover error handling paths, particularly connection URL parsing and stream chunking.

## Commit & Pull Request Guidelines
Git history currently relies on concise, lower-case summaries (`migracion mysql por testear`, `first commit`). Continue with a single-line imperative subject ≤72 characters, optionally prefixed with a scope (`feat: add gzip restore`). Describe user-visible behavior in the subject and elaborate in the body when needed. Pull requests should link the motivating issue, outline validation steps (commands run, databases used), and mention any required environment variables or credentials. Screenshots are unnecessary unless documenting progress output changes.
Do not use git commands unless explicitly requested.

## Configuration & Secrets
Connection URLs may be passed via `--source`, `--destination`, or environment variables such as `MYSQL_SOURCE_URL`. Never commit `.env` files or sample credentials. When sharing reproduction steps, redact hostnames and passwords, and prefer using local `mysql://user:pass@localhost:3306/db` examples.
