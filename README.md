# Db View TUI

[![Go](https://img.shields.io/badge/Go-1.22%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-supported-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](./LICENSE)

`dbctl` is a keyboard-driven terminal UI for PostgreSQL. Browse schemas, filter table data with include/exclude syntax, pin columns while scrolling, and run SQL scripts — all without leaving your terminal.

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
  - interactive data preview with:
    - row and column navigation
    - horizontal scrolling with pinned columns
    - sorting by the selected column
    - stacked filters across multiple columns
    - include / exclude text search
    - full cell and full row inspection
    - saved presets per table
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

- `Ctrl+D` or `Ctrl+C` quit
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

### Data View

Inside `Schema -> Data`, the right pane becomes a table viewer.

- `Up` / `Down` move between rows
- `Left` / `Right` move between columns
- `Tab`, `Esc`, or `q` return to `Schema Actions`
- `f` pin the selected column so it stays visible while scrolling horizontally
- `o` sort by the selected column, toggling `asc/desc`
- `a` edit the filter for the selected column
- `d` remove the filter for the selected column
- `r` reset all filters and sorting
- `v` open the full value + full row inspector
- `w` save the current data view as a preset
- `p` open saved presets for the current table

Filtering syntax in `Argument`:

- `le mans` include rows that contain `le mans`
- `-(women)` exclude rows that contain `(women)`
- `le mans -(women)` include and exclude in the same column
- `12345` search for a numeric value
- `limit=100 le mans -(women)` same filter with a larger page size
- `l=100 le mans` short form for page size

Filters are stored per selected column, so you can add one filter on `city`, another on `country`, and they will all be applied together.

Saved presets remember:

- active filters across columns
- sorting
- pinned column
- page size

Presets are scoped to the current `schema.table`.

### Query

- `Ctrl+E` run query
- `Ctrl+K` clear editor

### Exec

- `Tab` switch fields
- `Ctrl+E` execute
- `Ctrl+R` toggle dry run
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

The same config file also stores saved `Data` presets for individual tables.

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
