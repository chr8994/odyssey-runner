# Odyssey Runner Service

Background worker service for processing data stream synchronization jobs. This service listens to the PGMQ `stream_sync_jobs` queue and processes sync requests scheduled by pg_cron or triggered manually.

## Architecture

```
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────────┐
│   pg_cron       │────>│ PGMQ Queue           │────>│ Odyssey Runner      │
│   (scheduler)   │     │ stream_sync_jobs     │     │ (this service)      │
└─────────────────┘     └──────────────────────┘     └─────────────────────┘
                                                              │
                                                              ▼
                                                     ┌─────────────────────┐
                                                     │ Sync Process        │
                                                     │ - Download source   │
                                                     │ - Transform data    │
                                                     │ - Export Parquet    │
                                                     │ - Upload to bucket  │
                                                     │ - Update state      │
                                                     └─────────────────────┘
```

## Quick Start

### Prerequisites

- Node.js 20+
- Supabase project with PGMQ and required database functions
- Environment variables configured

### Installation

```bash
# Clone and navigate to the project
cd odyssey-runner

# Install dependencies
npm install

# Copy environment template
cp .env.example .env

# Edit .env with your Supabase credentials
```

### Development

```bash
# Run in development mode with hot reload
npm run dev
```

### Production

```bash
# Build TypeScript
npm run build

# Start production server
npm start
```

### Docker

```bash
# Build and run with Docker Compose
docker compose up -d

# View logs
docker compose logs -f

# Stop
docker compose down
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SUPABASE_URL` | Supabase project URL | **Required** |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key | **Required** |
| `POLL_INTERVAL_MS` | Queue polling interval | `5000` |
| `BATCH_SIZE` | Jobs per batch | `5` |
| `VISIBILITY_TIMEOUT_SECONDS` | Job visibility timeout | `300` |
| `MAX_RETRIES` | Max retry attempts | `3` |
| `HEALTH_CHECK_ENABLED` | Enable health server | `true` |
| `HEALTH_CHECK_PORT` | Health server port | `8080` |

## Health Checks

The service exposes health check endpoints:

- `GET /health` - Liveness probe
- `GET /healthz` - Liveness probe (k8s style)
- `GET /ready` - Readiness probe
- `GET /readyz` - Readiness probe (k8s style)

## Database Functions Required

The service expects these PostgreSQL functions to be available:

### `get_pending_sync_jobs(p_batch_size, p_visibility_timeout)`

Reads pending jobs from the PGMQ queue.

```sql
SELECT * FROM get_pending_sync_jobs(5, 300);
```

### `update_sync_run_status(p_run_id, p_status, ...)`

Updates sync run record with progress/results.

```sql
SELECT update_sync_run_status(
  'run-uuid',
  'completed',
  1000,  -- rows_processed
  800,   -- rows_inserted
  ...
);
```

### `acknowledge_sync_job(p_msg_id)`

Removes processed message from queue.

```sql
SELECT acknowledge_sync_job(12345);
```

## Project Structure

```
odyssey-runner/
├── src/
│   ├── index.ts           # Entry point with graceful shutdown
│   ├── config.ts          # Environment configuration
│   ├── queue-processor.ts # PGMQ polling and job orchestration
│   ├── sync-processor.ts  # Sync logic (placeholder)
│   └── types.ts           # TypeScript interfaces
├── Dockerfile
├── docker-compose.yml
├── package.json
├── tsconfig.json
├── .env.example
└── README.md
```

## Current Status

This is a **scaffolded implementation** with the following status:

| Component | Status |
|-----------|--------|
| Queue Polling | ✅ Implemented |
| Status Updates | ✅ Implemented |
| Acknowledgment | ✅ Implemented |
| Graceful Shutdown | ✅ Implemented |
| Health Checks | ✅ Implemented |
| **Sync Logic** | ⏳ **Placeholder** |
| DuckDB Integration | ⏳ Not started |

### TODO: Implement Sync Logic

The `sync-processor.ts` contains placeholder logic. Full implementation needs:

1. Download source file from Supabase storage
2. Initialize DuckDB for data transformation
3. Apply field mappings and generate `$_ref` hash
4. Export transformed data to Parquet format
5. Upload Parquet file to `streams` bucket
6. Update `data_sync_state` and `data_models` tables

## License

UNLICENSED - Private project
