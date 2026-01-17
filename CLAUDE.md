# CLAUDE.md

## Project Overview
Go-based UDP/TCP ingestion service that receives data packets, parses them, and batch-inserts into ClickHouse.

## Build & Run
```bash
go build -o ./build/clickhouse-udp   # Build
./clickhouse-udp                      # Run locally
./deploy.sh                           # Deploy via SSH
```

## Architecture
- **main.go** - UDP/TCP servers on port 9030, schema manager, batch processor
- **convert.go** - Type conversion for all ClickHouse types (Int, UInt, Float, DateTime, Map, Array)
- **parsepacket.go** - Packet parsing (JSON with `_t` table field, or legacy CSV format)

## Key Configuration (hardcoded in main.go)
- UDP/TCP port: 9030
- ClickHouse: 127.0.0.1:9000, user "default"
- Batch size: 2000 items, timeout: 30s
- Schema refresh: 10 minutes

## Data Format
JSON packets require `_t` field for table name. Other fields map to columns.
```json
{"_t": "table_name", "column1": "value1", "column2": 123}
```

## Patterns
- Goroutines with context cancellation for graceful shutdown
- Batching by table:column-list key
- Retry logic: 5 attempts with 5s delays for DB inserts
- Schema auto-discovery via `system.columns`
- Type normalization: strips Nullable/LowCardinality wrappers
