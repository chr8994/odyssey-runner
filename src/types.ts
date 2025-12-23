/**
 * Types for the Stream Runner Service
 */

/**
 * Trigger type for sync jobs
 */
export type TriggerType = 'scheduled' | 'manual' | 'api' | 'cascade';

/**
 * Status of a sync run
 */
export type SyncStatus = 'pending' | 'processing' | 'completed' | 'failed' | 'cancelled';

/**
 * Message structure from PGMQ stream_sync_jobs queue
 */
export interface QueueMessage {
  msg_id: number;
  read_ct: number;
  enqueued_at: string;
  vt: string;
  message: SyncJobPayload;
}

/**
 * Payload inside the queue message
 */
export interface SyncJobPayload {
  stream_id: string;
  tenant_id: string;
  run_id: string;
  trigger_type: TriggerType;
  stream_name: string;
  queued_at: string;
}

/**
 * Result returned from the sync processor
 */
export interface SyncResult {
  success: boolean;
  parquetPath: string | null;
  rowCount: number;
  fileSize: number;
  error?: string;
  /** Partition info if file was split into multiple parts */
  partitions?: PartitionInfo[];
}

/**
 * Information about a file partition
 */
export interface PartitionInfo {
  partNumber: number;
  path: string;
  rowCount: number;
  fileSize: number;
}

/**
 * Job from get_pending_sync_jobs RPC function
 */
export interface PendingSyncJob {
  msg_id: number;
  stream_id: string;
  tenant_id: string;
  run_id: string;
  trigger_type: TriggerType;
  stream_name: string;
  queued_at: string;
}

/**
 * Parameters for update_sync_run_status RPC function
 */
export interface UpdateSyncRunStatusParams {
  p_run_id: string;
  p_status: SyncStatus;
  p_rows_processed?: number;
  p_rows_inserted?: number;
  p_rows_updated?: number;
  p_rows_deleted?: number;
  p_file_size_bytes?: number;
  p_parquet_path?: string;
  p_error_message?: string;
  p_error_details?: Record<string, unknown>;
}

/**
 * Data stream configuration from database
 */
export interface DataStream {
  id: string;
  tenant_id: string;
  name: string;
  schema: SchemaField[];
  source_data_source_id: string | null;
  status: string;
  data_source?: DataSource;
  /** Primary key columns for CDC tracking */
  primary_key?: string[];
  /** Sync mode: full_refresh or incremental */
  sync_mode?: 'full_refresh' | 'incremental';
  /** Cursor field for incremental syncs and $_last_updated tracking */
  cursor_field?: string;
}

/**
 * Data source configuration
 */
export interface DataSource {
  id: string;
  name: string;
  type: string;
  config: DataSourceConfig;
  tenant_id: string;
  /** UUID of the vault secret containing credentials */
  vault_secret_id?: string;
}

/**
 * Data source config object
 */
export interface DataSourceConfig {
  parquetFilePath?: string;
  storedFilePath?: string;
  filePath?: string;
  // SFTP-specific config
  host?: string;
  port?: number;
  username?: string;
  remotePath?: string;
  filePattern?: string;
  selectedFile?: string;
  // Vault reference for credentials
  credentialSecretId?: string;
  [key: string]: unknown;
}

/**
 * SFTP credentials from vault
 */
export interface SftpCredentials {
  password?: string;
  privateKey?: string;
}

/**
 * Context passed to source processors
 */
export interface ProcessorContext {
  streamId: string;
  tenantId: string;
  runId: string;
  stream: DataStream;
  dataSource: DataSource;
  schema: StreamSchemaColumn[];
  fieldMappings: FieldMapping[];
}

/**
 * Result from downloading source data
 */
export interface SourceDataResult {
  tempFilePath: string;
  fileName: string;
  fileSize: number;
}

/**
 * Supported data source types
 */
export type DataSourceType = 'file' | 'sftp' | 'postgres' | 'mysql' | 'api';

/**
 * Schema field definition
 */
export interface SchemaField {
  name: string;
  type: string;
  nullable?: boolean;
  description?: string;
}

/**
 * Stream schema column (alias for SchemaField)
 */
export interface StreamSchemaColumn {
  name: string;
  type: string;
  nullable?: boolean;
  description?: string;
}

/**
 * Field mapping for data transformation
 */
export interface FieldMapping {
  source: string;
  target: string;
  transform?: string | null;
}

/**
 * Data mapping record
 */
export interface DataMapping {
  id: string;
  data_stream_id: string;
  field_mappings: FieldMapping[];
}

// ============================================================================
// DERIVED MODEL TYPES
// ============================================================================

/**
 * Data model from database
 */
export interface DataModel {
  id: string;
  tenant_id: string;
  name: string;
  display_name?: string | null;
  description?: string | null;
  model_kind: 'base' | 'derived';
  source_models?: string[] | null;  // UUIDs of source models for derived models
  query_definition?: QueryDefinition | null;
  schema?: SchemaField[] | null;
  primary_key?: string[];
  parquet_path?: string | null;
  row_count?: number;
  file_size_bytes?: number;
  status?: string;
  error_message?: string | null;
  last_materialized_at?: string | null;
  last_refresh_status?: string | null;
  last_refresh_error?: string | null;
  last_refresh_at?: string | null;
  created_at: string;
  updated_at: string;
}

/**
 * Query definition for derived models (JSON DSL)
 */
export interface QueryDefinition {
  type: 'join' | 'aggregation' | 'filter' | 'custom';
  from?: string;  // Base model name to start from
  select?: string[];  // Columns to select
  joins?: JoinDefinition[];
  where?: string;  // SQL WHERE clause
  groupBy?: string[];
  orderBy?: string[];
  sql?: string;  // For custom SQL queries
}

/**
 * Join definition in query
 */
export interface JoinDefinition {
  type: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'left' | 'right' | 'inner' | 'full';
  table?: string;  // Model name to join (preferred)
  model?: string;  // Alternative: Model name to join (for backwards compatibility)
  on: string | [string, string];  // Join condition (SQL string or [leftCol, rightCol] array)
}

/**
 * Derived model refresh job payload
 */
export interface DerivedModelJobPayload {
  model_id: string;
  tenant_id: string;
  triggered_by?: string;  // Model ID that triggered this refresh
  trigger_type?: 'manual' | 'scheduled' | 'cascade' | 'api';
}

/**
 * Context for derived model processor
 */
export interface DerivedModelContext {
  modelId: string;
  tenantId: string;
  dataModel: DataModel;
}

/**
 * Result from derived model refresh
 */
export interface DerivedModelRefreshResult {
  success: boolean;
  parquetPath: string | null;
  rowCount: number;
  fileSize: number;
  error?: string;
}

/**
 * Source model info with parquet path
 */
export interface SourceModelInfo {
  id: string;
  name: string;
  parquet_path: string;
  tempPath?: string;
}

/**
 * Manifest for derived model processing
 */
export interface DerivedModelManifest {
  sourceModels: SourceModelInfo[];
}

// ============================================================================
// OUTBOUND STREAM TYPES (Reverse ETL)
// ============================================================================

/**
 * Output format for outbound streams
 */
export type OutputFormat = 'parquet' | 'csv' | 'json' | 'jsonl' | 'sql_insert';

/**
 * Trigger type for outbound jobs
 */
export type OutboundTriggerType = 'manual' | 'schedule' | 'api' | 'cascade';

/**
 * Outbound sync job payload from PGMQ queue
 */
export interface OutboundJobPayload {
  stream_id: string;
  tenant_id: string;
  triggered_by?: OutboundTriggerType;
  trigger_user_id?: string;
  trigger_timestamp: string;
}

/**
 * Outbound stream with target configuration
 */
export interface OutboundStream {
  id: string;
  name: string;
  tenant_id: string;
  direction: 'outbound' | 'bidirectional';
  source_model_id: string;
  target_data_source_id: string;
  output_format: OutputFormat;
  target_config?: SftpTargetConfig | DatabaseTargetConfig | ApiTargetConfig | null;
  nested_data_mode?: 'flatten' | 'json' | 'expand';
  include_header?: boolean;
  csv_delimiter?: string;
  status: string;
}

/**
 * SFTP target configuration
 */
export interface SftpTargetConfig {
  remotePath: string;
  fileName?: string;
  overwriteExisting?: boolean;
  createDirectories?: boolean;
  archiveAfterUpload?: boolean;
}

/**
 * Database target configuration
 */
export interface DatabaseTargetConfig {
  tableName: string;
  schemaName?: string;
  truncateFirst?: boolean;
  upsertMode?: boolean;
  upsertKeys?: string[];
  primaryKey?: string[];
  createIfNotExists?: boolean;
  batchSize?: number;
}

/**
 * API target configuration
 */
export interface ApiTargetConfig {
  endpoint: string;
  method: 'POST' | 'PUT' | 'PATCH';
  headers?: Record<string, string>;
  batchSize?: number;
  payloadFormat?: 'array' | 'single' | 'wrapped';
  wrapperKey?: string;
  authType?: 'none' | 'bearer' | 'api_key' | 'basic';
  authConfig?: Record<string, string>;
}

/**
 * Context for outbound stream processor
 */
export interface OutboundStreamContext {
  streamId: string;
  tenantId: string;
  jobId: string;
  stream: OutboundStream;
  sourceModel: DataModel;
  targetDataSource: DataSource;
}

/**
 * Result from outbound sync operation
 */
export interface OutboundSyncResult {
  success: boolean;
  stream_id: string;
  job_id: string;
  rows_exported: number;
  bytes_written: number;
  output_path?: string;
  target_table?: string;
  duration_ms: number;
  error?: string;
}

// ============================================================================
// PARTITION TYPES (User-Scoped Database Partitions)
// ============================================================================

/**
 * Trigger type for partition build jobs
 */
export type PartitionTriggerType = 'manual' | 'scheduled' | 'model_updated' | 'on_demand';

/**
 * Priority levels for partition builds
 */
export type PartitionPriority = 'high' | 'normal' | 'low';

/**
 * Status of a partition database
 */
export type PartitionDatabaseStatus = 'pending' | 'building' | 'ready' | 'stale' | 'failed';

/**
 * Partition build job payload from PGMQ queue
 */
export interface PartitionBuildJob {
  partition_rule_id: string;
  partition_value: string;
  tenant_id: string;
  rule_name: string;
  trigger_type: PartitionTriggerType;
  priority?: PartitionPriority;
  queued_at?: string;
}

/**
 * Partition rule configuration from database
 */
export interface PartitionRule {
  id: string;
  tenant_id: string;
  name: string;
  slug: string;
  description?: string | null;
  partition_column: string;
  refresh_strategy: 'on_demand' | 'scheduled' | 'on_update';
  cron_schedule?: string | null;
  status: string;
  created_at: string;
  updated_at: string;
}

/**
 * Model mapping in a partition rule
 */
export interface PartitionRuleModelMapping {
  id: string;
  partition_rule_id: string;
  data_model_id: string;
  table_alias?: string | null;
  is_primary: boolean;
  join_config?: PartitionJoinConfig | null;
  created_at: string;
}

/**
 * Join configuration for non-primary models
 */
export interface PartitionJoinConfig {
  type: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL';
  foreignKey: string;
  primaryKey: string;
}

/**
 * Partition database record
 */
export interface PartitionDatabase {
  id: string;
  partition_rule_id: string;
  tenant_id: string;
  partition_value: string;
  storage_path?: string | null;
  status: PartitionDatabaseStatus;
  total_rows?: number | null;
  file_size_bytes?: number | null;
  build_started_at?: string | null;
  build_completed_at?: string | null;
  last_built_at?: string | null;
  error_message?: string | null;
  created_at: string;
  updated_at: string;
}

/**
 * Context for partition builder
 */
export interface PartitionBuildContext {
  ruleId: string;
  partitionValue: string;
  tenantId: string;
  rule: PartitionRule;
  modelMappings: PartitionRuleModelMappingWithModel[];
}

/**
 * Model mapping with the full data model attached
 */
export interface PartitionRuleModelMappingWithModel extends PartitionRuleModelMapping {
  data_model: DataModel;
}

/**
 * Result from partition build operation
 */
export interface PartitionBuildResult {
  success: boolean;
  databasePath: string | null;
  totalRows: number;
  fileSize: number;
  tablesIncluded: string[];
  error?: string;
}
