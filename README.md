# Db View TUI

[![Go](https://img.shields.io/badge/Go-1.22%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-supported-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](./LICENSE)

`Db View TUI` is a terminal UI for exploring remote PostgreSQL databases.

It combines connection management, schema browsing, table inspection, query execution, and SQL file execution in one keyboard-driven interface. The project is built in Go and uses `Bubble Tea` for the TUI layer.

## Features

- Full-screen TUI for day-to-day PostgreSQL work
- Saved connection management with default connection support
- Schema switching inside the UI
- Table browser grouped by schema
- Table inspection:
  - columns and constraints
  - table privileges
  - foreign keys
  - data preview
- One-off SQL query runner
- SQL file and directory execution with:
  - dry run
  - transaction mode
  - stop/continue on error
  - variable substitution
- Classic CLI commands still available for scripting

## Why This Project

This tool is meant for people who want something lighter than a full desktop database client, but much more comfortable than jumping between raw SQL commands and multiple terminal tools.

It is especially useful when you need to:

- inspect a production-like schema quickly
- browse tables by schema
- validate permissions or column constraints
- run quick read queries
- execute SQL scripts from a terminal workflow

## Stack

- Go
- Cobra
- pgx/v5
- Bubble Tea
- Bubbles
- Lip Gloss
- Pixi

## Installation

### Run from source with Pixi

```bash
pixi install
pixi run go build -o dbctl .
./dbctl
```

### Build manually

If you already have Go installed:

```bash
go build -o dbctl .
./dbctl
```

## Quick Start

### 1. Start the app

```bash
./dbctl
```

### 2. Add a connection

Open the `Connections` tab and create a new saved connection.

You can store either:

- a full PostgreSQL URL
- or `host / port / database / user / password / sslmode`

### 3. Pick a schema

Press `s` anywhere in the TUI to switch the active schema.

### 4. Browse tables

Open the `Schema` tab, select `Tables`, load the list, move focus to the right panel, choose a table, and the app will open its columns view immediately.

## TUI Workflow

### Main Tabs

- `1` `Connections`
- `2` `Schema`
- `3` `Query`
- `4` `Exec`

### Global Keys

- `q` quit
- `s` switch schema

### Connections

- `Enter` select current connection
- `u` set default connection
- `t` test connection
- `n` new connection
- `e` edit connection
- `d` delete connection

### Schema

- `Enter` run selected action
- `Tab` or `Right` focus the tables panel
- `Up` / `Down` move through actions or tables
- `x` clear selected table

When a table is selected, table-specific actions become available:

- `Describe`
- `Columns`
- `Privileges`
- `Data`
- `FKeys`

### Query

- `Ctrl+E` run query
- `Ctrl+K` clear editor

### Exec

- `Tab` switch fields
- `Ctrl+E` execute
- `Ctrl+D` toggle dry run
- `Ctrl+T` toggle transaction
- `Ctrl+G` toggle continue-on-error

## CLI Commands

The TUI is the primary interface, but the CLI is also usable directly.

### Root

```bash
./dbctl --help
```

### Connections

```bash
./dbctl connect add local
./dbctl connect list
./dbctl connect use local
./dbctl connect test local
./dbctl connect rm local
```

### Schema

```bash
./dbctl schema tables
./dbctl schema describe users
./dbctl schema columns users
./dbctl schema privileges users
./dbctl schema data users --limit 50
./dbctl schema indexes
./dbctl schema fkeys users
./dbctl schema sizes
./dbctl schema users
./dbctl schema activity
./dbctl schema locks
```

### Query

```bash
./dbctl query "select now();"
```

### SQL Execution

```bash
./dbctl exec migrations/
./dbctl exec migrations/ --dry-run
./dbctl exec migrations/ --transaction
./dbctl exec migrations/ --var env=prod
```

## Configuration

Saved connections are stored in:

```text
~/.dbctl/config.yaml
```

History for the interactive shell is stored in:

```text
~/.dbctl/history
```

You can also provide an ad-hoc connection string:

```bash
./dbctl --url "postgres://user:pass@host:5432/dbname?sslmode=require"
```

Environment-based passwords are supported for saved connections:

```bash
DBCTL_PASS_PROD=secret ./dbctl
```

## Project Layout

```text
cmd/            cobra commands
internal/config config loading and persistence
internal/db     postgres connection and introspection logic
internal/output output rendering
internal/repl   line-based shell
internal/sqlrun sql file execution logic
internal/tui    full-screen terminal interface
```

## Current Status

The project is already usable, but still evolving. The UI and workflows are being actively refined.

Good areas for future improvements:

- richer table browsing
- better in-pane pagination and filtering
- schema/object search
- query history inside the TUI
- export workflows
- automated release pipeline

## Development

```bash
pixi install
pixi run gofmt -w cmd/*.go internal/**/*.go
pixi run go build ./...
pixi run go build -o dbctl .
```

## Contributing

Issues, pull requests, and UX improvements are welcome.

If you want to contribute, focus on:

- TUI ergonomics
- PostgreSQL introspection quality
- reliability of script execution
- safer handling of connection secrets

## License

[MIT](./LICENSE)
