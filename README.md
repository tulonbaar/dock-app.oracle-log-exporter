# dock-app.oracle-log-exporter

Log exporter for Oracle 11g, running in Docker, written in .NET 10.

The solution:
- connects to Oracle using the configured user,
- reads table metadata and dynamically exports available columns,
- allows selected columns to be ignored,
- works incrementally (without duplicating records),
- writes data to a JSONL file (one line = one JSON record).

## How deduplication works

The exporter supports two incremental modes.

If a compatible `DATE`/`TIMESTAMP` column is configured or auto-detected, the exporter uses a watermark stored in the state file:
- `LastTimestampUtc` (the latest value of the timestamp column),
- fingerprints for rows already exported at the current timestamp boundary.

The query fetches only rows where:
- `timestamp >= LastTimestampUtc`
- and skips rows whose fingerprint has already been exported for `LastTimestampUtc`

This avoids re-exporting rows already processed at the same timestamp boundary without relying on Oracle `ROWID`, so it also works for views where `ROWID` is unavailable.

If there is no compatible time column, the exporter falls back to latest-batch mode and fetches up to `EXPORT_FETCH_BATCH_SIZE` rows on every polling cycle.

- When `ROWID` is available, it reads the physically latest rows (`ORDER BY ROWID DESC`).
- For sources where `ROWID` is not selectable (for example some views), it still fetches up to `EXPORT_FETCH_BATCH_SIZE` rows, but without stable ordering guarantees.

In both cases, every exported record gets `_extracted_at_utc` set by the application.

## Requirements

- Docker + Docker Compose
- Network access from the container to Oracle 11g
- A readable Oracle table; a timestamp column is optional

## Configuration

1. Copy the environment template:

```bash
cp .env.example .env
```

2. Fill in `.env`:

- `ORACLE_HOST` - host Oracle
- `ORACLE_PORT` - port (default: 1521)
- `ORACLE_SERVICE_NAME` - database service name
- `ORACLE_USER` / `ORACLE_PASSWORD` - user and password
- `EXPORT_TABLE` - `TABLE` or `OWNER.TABLE`
- `EXPORT_TIMESTAMP_COLUMN` - optional timestamp column used for incremental fetch; if empty, the exporter auto-detects the first compatible `DATE`/`TIMESTAMP` column and falls back to latest-batch mode if none exists
- `EXPORT_IGNORED_COLUMNS` - CSV list of ignored columns
- `EXPORT_ADDITIONAL_WHERE` - optional additional SQL filter
- `EXPORT_POLL_INTERVAL_SECONDS` - polling interval in seconds (for example, 300 = 5 minutes)
- `EXPORT_FETCH_BATCH_SIZE` - maximum number of rows per cycle
- `EXPORT_INITIAL_LOOKBACK_MINUTES` - how many minutes back to start on first run
- `DOCKER_LOG_MAX_SIZE` - max size of a single container log file before rotation (for example, `10m`)
- `DOCKER_LOG_MAX_FILE` - number of rotated log files to keep (for example, `5`)

## Run

```bash
docker compose up -d --build
```

Logs:

```bash
docker compose logs -f oracle-log-exporter
```

Stop:

```bash
docker compose down
```

## Export outputs

- Data: `./output/oracle-log-YYYYMMDD.jsonl`
- Watermark state: `./state/export-state.json`

## Project structure

- `src/OracleLogExporter/Program.cs` - main exporter logic
- `src/OracleLogExporter/OracleLogExporter.csproj` - .NET 10 project
- `Dockerfile` - container image
- `docker-compose.yml` - service runtime definition
- `.env.example` - configuration template

## Practical notes

- The Oracle user must have permissions to read the table and metadata (`ALL_TAB_COLUMNS`).
- For very high data volume, consider increasing `EXPORT_FETCH_BATCH_SIZE` and reducing the polling interval.
- `EXPORT_ADDITIONAL_WHERE` is injected into the query as a raw SQL fragment. Use only trusted administrative configuration.
- Latest-batch fallback mode (without timestamp) intentionally re-reads rows every cycle; it is designed for “always export something” behavior, not strict deduplication.
