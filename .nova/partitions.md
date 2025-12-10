# User-Scoped Database Partitions

> **Architecture Documentation for Odyssey Runner**  
> Last Updated: December 2024

## Overview

This document describes the **User-Scoped Database Partition** system - an architecture for creating isolated, per-user DuckDB databases that enable secure, sandboxed querying for integrations like Astro AI.

### Problem Statement

The current Odyssey Runner processes data at the **tenant level**, producing Parquet files stored as:
```
streams/{tenant_id}/{stream_id}.parquet
```

This works well for tenant-wide analytics, but presents challenges when:
- Users need to query **only their own data** (privacy/security)
- AI chatbots like Astro AI need **sandboxed query environments**
- Performance suffers when scanning large tenant files for single-user queries

### Solution

Create **pre-built DuckDB database files** for each user partition, containing only that user's data from relevant models.

---

## Architecture

### Storage Structure

```
supabase-storage/
├── streams/                              # EXISTING: Tenant-level data
│   └── {tenant_id}/
│       └── {stream_id}.parquet
│
└── user-streams/                         # NEW: User-scoped databases
    └── {tenant_id}/
        └── {user_id}/                    # e.g., "0-1-1-1" or UUID
            ├── data.db                   # Single DuckDB file with ALL user tables
            ├── metadata.json             # Stats, last updated, table list
            └── .lock                     # Prevent concurrent writes
```

### Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Storage Format** | Single `.db` file | Faster loading, no view creation, includes indexes |
| **Partition Key** | `user_id` field | Most common isolation requirement |
| **Generation** | On-demand + scheduled | Balance freshness vs. cost |
| **Caching** | 30-day expiration | Remove unused partitions |

---

## Database Schema

### `user_databases` Table

Tracks all generated user database files.

```sql
CREATE TABLE user_databases (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL,
  user_id TEXT NOT NULL,                    -- "0-1-1-1" format or UUID string
  database_path TEXT NOT NULL,              -- "user-streams/{tenant}/{user}/data.db"
  file_size_bytes BIGINT,
  total_rows BIGINT,
  tables_included TEXT[],                   -- ["wallet_sales", "communities", ...]
  status TEXT DEFAULT 'building',           -- building, ready, stale, error
  last_built_at TIMESTAMPTZ,
  last_accessed_at TIMESTAMPTZ,
  error_message TEXT,
  metadata JSONB,                           -- Additional stats, version info
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  
  UNIQUE(tenant_id, user_id)
);

-- Indexes for fast lookups
CREATE INDEX idx_user_db_lookup ON user_databases(tenant_id, user_id);
CREATE INDEX idx_user_db_stale ON user_databases(last_built_at) WHERE status = 'stale';
CREATE INDEX idx_user_db_cleanup ON user_databases(last_accessed_at) WHERE status = 'ready';
```

### `user_database_sources` Table

Tracks which source models feed each user database (for invalidation).

```sql
CREATE TABLE user_database_sources (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_database_id UUID REFERENCES user_databases(id) ON DELETE CASCADE,
  source_model_id UUID REFERENCES data_models(id),
  source_updated_at TIMESTAMPTZ,            -- When source was last updated
  partition_field TEXT DEFAULT 'user_id',   -- Field used for filtering
  row_count BIGINT,                         -- Rows from this source
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_user_db_sources ON user_database_sources(user_database_id);
CREATE INDEX idx_user_db_source_model ON user_database_sources(source_model_id);
```

### `partition_rules` Table (Optional - For Multi-Dimension Partitioning)

For more flexible partitioning beyond user_id.

```sql
CREATE TABLE partition_rules (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID,
  name TEXT,                                -- "user-scoped", "community-scoped"
  partition_field TEXT,                     -- "user_id", "community_id"
  partition_field_type TEXT,                -- "number", "string", "uuid"
  applies_to_models TEXT[],                 -- ["wallet_sales"] or ["*"] for all
  storage_pattern TEXT,                     -- Template for paths
  min_rows_threshold INT DEFAULT 1,         -- Don't partition if < N rows
  enabled BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

---

## Build Process

### How User Databases Are Created

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       User Database Build Pipeline                       │
└─────────────────────────────────────────────────────────────────────────┘

  1. TRIGGER                    2. QUEUE                    3. BUILD
  ┌──────────────┐           ┌──────────────┐           ┌──────────────┐
  │ Model Update │──────────>│ PGMQ Queue   │──────────>│ UserDatabase │
  │ On-Demand    │           │ user_db_jobs │           │ Builder      │
  │ Scheduled    │           └──────────────┘           └──────────────┘
  └──────────────┘                                             │
                                                               ▼
  6. SERVE                     5. INDEX                   4. UPLOAD
  ┌──────────────┐           ┌──────────────┐           ┌──────────────┐
  │ Astro AI     │<──────────│ user_databases│<──────────│ Supabase     │
  │ Queries      │           │ table updated │           │ Storage      │
  └──────────────┘           └──────────────┘           └──────────────┘
```

### Build Steps Detail

#### Step 1: Identify Source Models

```typescript
// Find all data_models with user_id field in schema
const modelsWithUserField = await supabase
  .from('data_models')
  .select('*')
  .eq('tenant_id', tenantId)
  .eq('status', 'ready');

// Filter by schema containing user_id
const applicableModels = modelsWithUserField.filter(model => {
  const schema = model.schema as { children: { name: string }[] };
  return schema?.children?.some(col => 
    col.name === 'user_id' || col.name === 'userId'
  );
});
```

#### Step 2: Create Temporary DuckDB Database

```typescript
const tempDbPath = `/tmp/${tenantId}_${userId}_${Date.now()}.db`;
const db = await DuckDBInstance.create(tempDbPath);
const conn = await db.connect();
```

#### Step 3: Extract User Data from Each Model

```typescript
for (const model of applicableModels) {
  const tableName = sanitizeTableName(model.name);
  
  // Read from tenant parquet, filter by user_id
  await conn.run(`
    CREATE TABLE ${tableName} AS
    SELECT * FROM read_parquet('${model.parquet_path}')
    WHERE user_id = '${userId}'
  `);
  
  // Add useful indexes
  await addIndexes(conn, tableName, model.schema);
}
```

#### Step 4: Upload to Storage

```typescript
await conn.close(); // Flush to disk

const dbBuffer = await fs.readFile(tempDbPath);
const finalPath = `user-streams/${tenantId}/${userId}/data.db`;

await supabase.storage
  .from('user-streams')
  .upload(finalPath, dbBuffer, {
    contentType: 'application/x-duckdb',
    upsert: true
  });
```

#### Step 5: Record in Database

```typescript
await supabase.from('user_databases').upsert({
  tenant_id: tenantId,
  user_id: userId,
  database_path: finalPath,
  file_size_bytes: dbBuffer.byteLength,
  total_rows: totalRows,
  tables_included: tableNames,
  status: 'ready',
  last_built_at: new Date().toISOString()
});
```

---

## Queue System

### PGMQ Queue: `user_database_jobs`

```typescript
// Job payload structure
interface UserDatabaseJob {
  tenant_id: string;
  user_id: string;
  trigger: 'manual' | 'scheduled' | 'model_updated' | 'on_demand';
  priority: 'high' | 'normal' | 'low';
}

// Queue job for processing
await supabase.rpc('pgmq_send', {
  queue_name: 'user_database_jobs',
  message: {
    tenant_id: '11111111-1111-1111-1111-111111111111',
    user_id: '0-1-1-1',
    trigger: 'on_demand',
    priority: 'high'
  }
});
```

### Queue Processor

```typescript
class UserDatabaseQueueProcessor {
  async start() {
    while (this.isRunning) {
      const jobs = await this.fetchPendingJobs(5); // Batch of 5
      
      for (const job of jobs) {
        try {
          await this.builder.buildForUser(job.tenant_id, job.user_id);
          await this.acknowledgeJob(job.msg_id);
        } catch (error) {
          await this.handleFailure(job, error);
        }
      }
      
      await this.sleep(5000); // Poll every 5s
    }
  }
}
```

---

## Astro AI Integration

### API Endpoint: `GET /api/user-database/:userId`

```typescript
export async function getUserDatabase(
  tenantId: string,
  userId: string
): Promise<DuckDBInstance> {
  
  // 1. Check if database exists and is fresh
  const { data: userDb } = await supabase
    .from('user_databases')
    .select('*')
    .eq('tenant_id', tenantId)
    .eq('user_id', userId)
    .single();
  
  // 2. Build on-demand if missing or stale
  if (!userDb || userDb.status === 'stale') {
    await queueUserDatabaseBuild(tenantId, userId, 'on_demand', 'high');
    await waitForDatabaseReady(tenantId, userId, 30000); // 30s timeout
  }
  
  // 3. Download from storage
  const { data: dbBuffer } = await supabase.storage
    .from('user-streams')
    .download(userDb.database_path);
  
  // 4. Load into DuckDB
  const tempPath = `/tmp/${tenantId}_${userId}_${Date.now()}.db`;
  await fs.writeFile(tempPath, Buffer.from(await dbBuffer.arrayBuffer()));
  
  const db = await DuckDBInstance.create(tempPath);
  
  // 5. Update access timestamp (for cache eviction)
  await supabase
    .from('user_databases')
    .update({ last_accessed_at: new Date().toISOString() })
    .eq('id', userDb.id);
  
  return db;
}
```

### Query Handler for Astro AI

```typescript
export async function handleUserQuery(
  tenantId: string,
  userId: string,
  naturalLanguageQuery: string
): Promise<QueryResult> {
  
  // 1. Get user's sandboxed database
  const db = await getUserDatabase(tenantId, userId);
  const conn = await db.connect();
  
  try {
    // 2. List available tables (for AI context)
    const tables = await conn.runAndReadAll(`
      SELECT table_name FROM information_schema.tables 
      WHERE table_schema = 'main'
    `);
    
    // 3. Convert natural language to SQL (Astro AI logic)
    const sql = await convertToSQL(naturalLanguageQuery, tables);
    
    // 4. Execute query (scoped to user's data only)
    const result = await conn.runAndReadAll(sql);
    
    return {
      success: true,
      rows: result.getRows(),
      columns: result.getColumnNames(),
      rowCount: result.getRows().length
    };
    
  } catch (error) {
    return {
      success: false,
      error: error.message
    };
  } finally {
    await conn.close();
  }
}
```

### Usage Example

```typescript
// Astro AI chatbot integration
const chatbot = new AstroAI({
  tenantId: '11111111-1111-1111-1111-111111111111',
  userId: '0-1-1-1'
});

// User asks: "Show me my total sales this year"
const response = await chatbot.query("Show me my total sales this year");

// Behind the scenes:
// 1. Gets user's sandboxed DuckDB with only THEIR data
// 2. Converts to SQL: SELECT SUM(sale_price) FROM wallet_sales WHERE ...
// 3. Executes against pre-filtered data
// 4. Returns: { total_sales: 450000 }
```

---

## Automation

### Trigger: Rebuild on Model Updates

When a source model is updated (e.g., new wallet_sales sync), mark user databases as stale.

```sql
CREATE OR REPLACE FUNCTION trigger_user_database_rebuild()
RETURNS TRIGGER AS $$
BEGIN
  -- Mark dependent user databases as stale
  UPDATE user_databases 
  SET status = 'stale', updated_at = NOW()
  WHERE tenant_id = NEW.tenant_id
  AND tables_included @> ARRAY[NEW.name];
  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER data_models_updated
AFTER UPDATE OF updated_at ON data_models
FOR EACH ROW
EXECUTE FUNCTION trigger_user_database_rebuild();
```

### Scheduled Cleanup: Remove Unused Databases

```typescript
// Run daily via cron
export async function cleanupStaleDatabases() {
  const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
  
  // Find databases not accessed in 30 days
  const { data: staleDbs } = await supabase
    .from('user_databases')
    .select('*')
    .lt('last_accessed_at', thirtyDaysAgo.toISOString())
    .eq('status', 'ready');
  
  for (const db of staleDbs || []) {
    // Delete from storage
    await supabase.storage
      .from('user-streams')
      .remove([db.database_path]);
    
    // Delete record
    await supabase
      .from('user_databases')
      .delete()
      .eq('id', db.id);
    
    console.log(`[Cleanup] Removed stale database: ${db.user_id}`);
  }
}
```

### Scheduled Pre-Build: Active Users

```typescript
// Pre-build databases for frequently active users
export async function prebuildActiveUserDatabases() {
  // Get users who queried in the last 7 days
  const { data: activeUsers } = await supabase
    .from('user_databases')
    .select('tenant_id, user_id')
    .gte('last_accessed_at', new Date(Date.now() - 7 * 24 * 60 * 60 * 1000))
    .eq('status', 'stale');
  
  for (const user of activeUsers || []) {
    await queueUserDatabaseBuild(
      user.tenant_id, 
      user.user_id, 
      'scheduled', 
      'normal'
    );
  }
}
```

---

## Scalability Analysis

### Performance Characteristics

| Metric | Tenant-Level (Before) | User-Scoped (After) |
|--------|----------------------|---------------------|
| Query Latency | ~500ms (scan 50MB) | ~50ms (scan 500KB) |
| Cold Start | N/A | ~500ms (download + load) |
| Warm Cache | N/A | ~10ms (local DB) |
| Storage per User | N/A | ~500KB - 5MB |

### Scaling Numbers

| Users | Storage | Generation Time* | Storage Cost |
|-------|---------|------------------|--------------|
| 100 | 50MB | 3 min | $0.001/mo |
| 1,000 | 500MB | 20 min | $0.01/mo |
| 10,000 | 5GB | 3 hours | $0.12/mo |
| 100,000 | 50GB | 30 hours | $1.15/mo |

*With 50× parallelization

### Bottleneck Mitigation

#### 1. Storage Growth
```typescript
// Compression strategy
await conn.run(`
  COPY (...) TO '${path}' (
    FORMAT PARQUET, 
    COMPRESSION 'ZSTD',     -- 50% smaller than SNAPPY
    ROW_GROUP_SIZE 100000
  )
`);
```

#### 2. Generation Time
```typescript
// Parallel processing
const queue = new PQueue({ concurrency: 50 });
for (const userId of userIds) {
  queue.add(() => builder.buildForUser(tenantId, userId));
}
await queue.onIdle();
```

#### 3. File System Limits
```typescript
// Hierarchical paths to avoid directory limits
const hashedPath = hashUserId(userId).substring(0, 2); // "a3"
const path = `user-streams/${tenantId}/${hashedPath}/${userId}/data.db`;
// Results in: user-streams/tenant/a3/user-123/data.db
```

---

## Security Considerations

### Data Isolation

- ✅ User databases contain **only that user's data**
- ✅ No way to query other users' data from sandboxed DB
- ✅ Storage paths include tenant_id for multi-tenant isolation

### Access Control

```typescript
// Validate user can access this database
async function validateAccess(authUserId: string, requestedUserId: string) {
  if (authUserId !== requestedUserId) {
    // Check if admin or has permission
    const hasAccess = await checkUserPermissions(authUserId, requestedUserId);
    if (!hasAccess) {
      throw new Error('Access denied');
    }
  }
}
```

### Query Sanitization

```typescript
// Prevent SQL injection in natural language to SQL conversion
const BLOCKED_PATTERNS = [
  /ATTACH\s+DATABASE/i,
  /LOAD\s+EXTENSION/i,
  /COPY\s+.*\s+TO/i,
  /DROP\s+/i,
  /DELETE\s+FROM/i,
  /TRUNCATE/i
];

function sanitizeQuery(sql: string): string {
  for (const pattern of BLOCKED_PATTERNS) {
    if (pattern.test(sql)) {
      throw new Error('Query contains blocked operations');
    }
  }
  return sql;
}
```

---

## Implementation Checklist

### Phase 1: Infrastructure
- [ ] Create `user_databases` table
- [ ] Create `user_database_sources` table
- [ ] Create `user_database_jobs` PGMQ queue
- [ ] Create `user-streams` storage bucket

### Phase 2: Build System
- [ ] Implement `UserDatabaseBuilder` class
- [ ] Implement `UserDatabaseQueueProcessor`
- [ ] Add to odyssey-runner startup

### Phase 3: Integration
- [ ] Create API endpoint `GET /api/user-database/:userId`
- [ ] Implement Astro AI query handler
- [ ] Add authentication/authorization

### Phase 4: Automation
- [ ] Add trigger for model update invalidation
- [ ] Add cleanup cron job (30-day expiration)
- [ ] Add pre-build for active users

### Phase 5: Monitoring
- [ ] Add logging for build times
- [ ] Add metrics for query latency
- [ ] Add alerting for build failures

---

## Future Enhancements

### Multi-Dimension Partitioning
```typescript
// Support partitioning by any field
const rules = [
  { field: 'user_id', name: 'user-scoped' },
  { field: 'community_id', name: 'community-scoped' },
  { field: 'sale_date', name: 'date-scoped', type: 'range' }
];
```

### Composite Partitions
```typescript
// User + Date range partitioning for very large users
const path = `user-streams/${tenantId}/${userId}/${year}/${month}/data.db`;
```

### Real-Time Updates
```typescript
// Stream changes to user databases using CDC
await conn.run(`
  INSERT INTO wallet_sales 
  SELECT * FROM delta_changes 
  WHERE user_id = '${userId}'
`);
```

---

## Glossary

| Term | Definition |
|------|------------|
| **Partition** | A subset of data filtered by a specific field value |
| **User Database** | A DuckDB `.db` file containing all tables for a single user |
| **Tenant-Level** | Data scoped to an entire organization/tenant |
| **User-Scoped** | Data filtered to a single user within a tenant |
| **Stale** | A database that needs rebuilding due to source updates |
| **On-Demand** | Building a database when first requested (lazy evaluation) |

---

## References

- [DuckDB Documentation](https://duckdb.org/docs/)
- [Supabase Storage](https://supabase.com/docs/guides/storage)
- [PGMQ - PostgreSQL Message Queue](https://github.com/tembo-io/pgmq)
- [Parquet Format](https://parquet.apache.org/)
