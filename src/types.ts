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
