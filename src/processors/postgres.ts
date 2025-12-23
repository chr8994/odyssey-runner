/**
 * PostgreSQL Processor - Uses DuckDB's native PostgreSQL scanner extension
 * 
 * Connects directly to PostgreSQL databases and exports to Parquet
 * without intermediate files.
 */

import { SupabaseClient } from '@supabase/supabase-js';
import { DuckDBInstance } from '@duckdb/node-api';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  ProcessorContext,
  SourceDataResult,
  SyncResult,
  StreamSchemaColumn,
  FieldMapping,
  PartitionInfo,
} from '../types.js';
import { BaseProcessor } from './base-processor.js';

// System column prefix for CDC tracking
const SYSTEM_COLUMN_PREFIX = '$_';

// Max file size before partitioning (10 GiB - increased limit)
const MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024 * 1024;

// Target rows per partition (can be adjusted based on data density)
const DEFAULT_ROWS_PER_PARTITION = 30000;

// PostgreSQL statement timeout (in milliseconds) - prevents long-running queries from being killed
// Default: 10 minutes for large dataset exports
const POSTGRES_STATEMENT_TIMEOUT_MS = 60 * 60 * 1000;

/**
 * PostgreSQL credentials from vault
 */
interface PostgresCredentials {
  password?: string;
}

/**
 * PostgreSQL configuration from data source
 * Note: The config may have either 'user' or 'username' depending on how it was created
 */
interface PostgresConfig {
  host: string;
  port: number | string;
  database: string;
  user?: string;      // Database config stores as 'user'
  username?: string;  // Support both for backwards compatibility
  schema?: string;    // PostgreSQL schema (defaults to 'public')
  table?: string;
  table_name?: string;      // Stream config field name
  query?: string;
  selected_table?: string;  // Data source config field name for table
}

/**
 * PostgreSQL Processor using DuckDB's postgres_scanner extension
 */
export class PostgresProcessor extends BaseProcessor {
  constructor(supabase: SupabaseClient) {
    super(supabase);
  }
  
  getTypeName(): string {
    return 'Postgres';
  }
  
  /**
   * Not used for PostgreSQL - we use DuckDB's native PostgreSQL scanner
   * This is only here to satisfy the abstract method requirement
   */
  async downloadSourceData(context: ProcessorContext): Promise<SourceDataResult> {
    throw new Error('PostgreSQL processor does not use downloadSourceData - use process() directly');
  }
  
  /**
   * Main process method - orchestrates the PostgreSQL sync using DuckDB
   * Overrides base class to use DuckDB's postgres_scanner extension
   */
  async process(context: ProcessorContext): Promise<SyncResult> {
    const startTime = Date.now();
    let tempParquetPath: string | null = null;
    
    const logPrefix = `[${this.getTypeName()}Processor]`;
    
    console.log(`${logPrefix} Processing stream: ${context.stream.name} (${context.streamId})`);
    console.log(`${logPrefix} Tenant: ${context.tenantId}`);
    
    try {
      // Get PostgreSQL configuration
      const config = context.dataSource.config as unknown as PostgresConfig;
      
      if (!config.host || !config.database) {
        throw new Error('PostgreSQL configuration missing required fields: host, database');
      }
      
      // Get credentials from vault
      const credentials = await this.getPostgresCredentials(context.dataSource.vault_secret_id);
      
      console.log(`${logPrefix} Connecting to PostgreSQL: ${config.host}:${config.port || 5432}/${config.database}`);
      
      // Process with DuckDB PostgreSQL scanner
      const { parquetPath, rowCount, fileSize } = await this.processWithPostgresScanner(
        config,
        credentials,
        context
      );
      tempParquetPath = parquetPath;
      
      // Upload to streams bucket using stream to avoid 2 GiB Buffer limit
      const parquetStoragePath = `${context.tenantId}/${context.streamId}.parquet`;
      const parquetStream = fs.createReadStream(tempParquetPath);
      
      const { error: uploadError } = await this.supabase
        .storage
        .from('streams')
        .upload(parquetStoragePath, parquetStream, {
          contentType: 'application/x-parquet',
          upsert: true,
        });
      
      if (uploadError) {
        throw new Error(`Failed to upload Parquet file: ${uploadError.message}`);
      }
      
      console.log(`${logPrefix} Uploaded to streams/${parquetStoragePath}`);
      
      const syncedAt = new Date().toISOString();
      
      // Update sync state
      await this.updateSyncState(context.streamId, context.tenantId, {
        success: true,
        parquetPath: parquetStoragePath,
        rowCount,
        fileSize,
      });
      
      // Update linked data model
      await this.updateLinkedDataModel(context.streamId, {
        parquetPath: parquetStoragePath,
        rowCount,
        fileSize,
        syncedAt,
      });
      
      // Update stream status
      await this.supabase
        .from('data_streams')
        .update({
          status: 'active',
          updated_at: syncedAt,
        })
        .eq('id', context.streamId);
      
      const duration = Date.now() - startTime;
      console.log(`${logPrefix} Sync completed in ${duration}ms`);
      
      return {
        success: true,
        parquetPath: parquetStoragePath,
        rowCount,
        fileSize,
      };
      
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      
      console.error(`${logPrefix} Sync failed after ${duration}ms:`, errorMessage);
      
      return {
        success: false,
        parquetPath: null,
        rowCount: 0,
        fileSize: 0,
        error: errorMessage,
      };
      
    } finally {
      // Cleanup temp parquet file
      this.cleanupTempFile(tempParquetPath);
    }
  }
  
  /**
   * Execute a promise with a timeout
   */
  private async executeWithTimeout<T>(
    promise: Promise<T>,
    timeoutMs: number,
    timeoutMessage: string
  ): Promise<T> {
    return Promise.race([
      promise,
      new Promise<T>((_, reject) =>
        setTimeout(() => reject(new Error(timeoutMessage)), timeoutMs)
      ),
    ]);
  }
  
  /**
   * Get PostgreSQL credentials from vault using nova_get_secret_by_id RPC function
   */
  private async getPostgresCredentials(vaultSecretId?: string): Promise<PostgresCredentials> {
    if (!vaultSecretId) {
      console.warn('[PostgresProcessor] No vault_secret_id provided, trying without credentials');
      return {};
    }
    
    try {
      console.log(`[PostgresProcessor] Fetching credentials from vault by ID: ${vaultSecretId}`);
      
      // Fetch secret from vault using nova_get_secret_by_id (takes UUID)
      const { data, error } = await this.supabase
        .rpc('nova_get_secret_by_id', { secret_id: vaultSecretId });
      
      if (error) {
        console.error('[PostgresProcessor] Vault RPC error:', error);
        return {};
      }
      
      if (!data || data.length === 0) {
        console.warn(`[PostgresProcessor] Secret not found with ID: ${vaultSecretId}`);
        return {};
      }
      
      // The RPC returns an array with decrypted_value column
      const decryptedValue = data[0]?.decrypted_value;
      
      if (!decryptedValue) {
        console.warn('[PostgresProcessor] Secret has no decrypted value');
        return {};
      }
      
      // Parse secret value (expected to be JSON with password)
      const secret = typeof decryptedValue === 'string' ? JSON.parse(decryptedValue) : decryptedValue;
      
      console.log('[PostgresProcessor] Credentials loaded from vault successfully');
      
      return {
        password: secret.password,
      };
    } catch (err) {
      console.error('[PostgresProcessor] Error fetching credentials:', err);
    }
    
    return {};
  }
  
  /**
   * Process PostgreSQL data using DuckDB's postgres_scanner extension
   */
  private async processWithPostgresScanner(
    config: PostgresConfig,
    credentials: PostgresCredentials,
    context: ProcessorContext
  ): Promise<{ parquetPath: string; rowCount: number; fileSize: number }> {
    const logPrefix = '[PostgresProcessor]';
    
    // Initialize DuckDB
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    
    try {
      // Install and load PostgreSQL extension
      console.log(`${logPrefix} Installing PostgreSQL scanner extension...`);
      await connection.run('INSTALL postgres');
      await connection.run('LOAD postgres');
      
      // Build PostgreSQL connection string using DuckDB's expected parameter names
      const host = config.host;
      const port = typeof config.port === 'string' ? parseInt(config.port, 10) : (config.port || 5432);
      const database = config.database;
      // Support both 'user' and 'username' fields (database stores as 'user')
      const username = config.user || config.username || 'postgres';
      const password = credentials.password;
      
      console.log(`[PostgresProcessor] Using username: ${username}`);
      console.log(`${logPrefix} Statement timeout: ${POSTGRES_STATEMENT_TIMEOUT_MS / 1000} seconds`);
      
      // DuckDB PostgreSQL extension expects: host, port, dbname, user, password, options
      // Build connection parts - only include password if we have a password
      const connectionParts = [
        `host=${host}`,
        `port=${port}`,
        `dbname=${database}`,
        `user=${username}`,
      ];
      
      // Only add password if password exists (avoid empty password= causing parse error)
      if (password) {
        const escapedPassword = password.replace(/'/g, "''");
        connectionParts.push(`password=${escapedPassword}`);
        console.log(`${logPrefix} Using password authentication`);
      } else {
        console.log(`${logPrefix} No password provided, attempting passwordless connection`);
      }
      
      // Attach PostgreSQL database
      console.log(`${logPrefix} Attaching PostgreSQL database...`);
      const attachQuery = `ATTACH '${connectionParts.join(' ')}' AS postgres_db (TYPE postgres)`;
      await connection.run(attachQuery);
      
      // Set statement_timeout to prevent PostgreSQL from killing long-running queries
      // This is critical for large dataset exports that may take several minutes
      // Note: This sets the timeout for the current DuckDB session
      const timeoutSeconds = Math.floor(POSTGRES_STATEMENT_TIMEOUT_MS / 1000);
      console.log(`${logPrefix} Setting statement_timeout to ${timeoutSeconds} seconds...`);
      try {
        await connection.run(`SET statement_timeout = '${timeoutSeconds}s'`);
        console.log(`${logPrefix} Statement timeout configured successfully`);
      } catch (timeoutError) {
        console.warn(`${logPrefix} Warning: Could not set statement_timeout:`, timeoutError);
        console.warn(`${logPrefix} Export may be subject to database default timeout`);
      }
      
      // Determine the source query
      // Support 'table', 'table_name', and 'selected_table' field names
      const tableName = config.table || config.table_name || config.selected_table;
      const schema = config.schema || 'public';
      
      let sourceQuery: string;
      if (config.query) {
        // Custom query provided
        sourceQuery = config.query;
        console.log(`${logPrefix} Using custom query`);
      } else if (tableName) {
        // Table name provided - handle schema.table format
        // If table already has a dot (schema.table), use it directly
        // Otherwise prepend the configured schema (defaults to 'public')
        let fullTableName: string;
        if (tableName.includes('.')) {
          const [tableSchema, table] = tableName.split('.');
          fullTableName = `postgres_db.${tableSchema}.${table}`;
        } else {
          fullTableName = `postgres_db.${schema}.${tableName}`;
        }
        sourceQuery = `SELECT * FROM ${fullTableName}`;
        console.log(`${logPrefix} Reading from table: ${tableName} (schema: ${schema})`);
      } else {
        throw new Error('PostgreSQL configuration must include either "table", "selected_table", or "query"');
      }
      
      // Build SELECT with field mappings and type casting
      const selectQuery = await this.buildPostgresSelectQuery(
        connection,
        sourceQuery,
        context.schema,
        context.fieldMappings
      );
      
      // Get column names and add $_ref hash
      const describeQuery = `DESCRIBE SELECT * FROM (${selectQuery}) t`;
      const describeReader = await connection.runAndReadAll(describeQuery);
      const columnNames = describeReader.getRows().map(row => String(row[0]));
      
      // Use table-qualified column names to avoid DuckDB ambiguity
      const hashExpressions = columnNames.map(col => `COALESCE(source_with_hash."${col}"::VARCHAR, '')`);
      const hashColumn = `md5(CONCAT_WS('|', ${hashExpressions.join(', ')})) AS "${SYSTEM_COLUMN_PREFIX}ref"`;
      const quotedColumns = columnNames.map(col => `source_with_hash."${col}"`).join(', ');
      const selectWithHash = `SELECT ${quotedColumns}, ${hashColumn} FROM (${selectQuery}) source_with_hash`;
      
      console.log(`${logPrefix} Added $_ref hash column for CDC tracking`);
      
      // Get row count
      const countQuery = `SELECT COUNT(*) as count FROM (${selectWithHash}) t`;
      const countReader = await connection.runAndReadAll(countQuery);
      const rowCount = Number(countReader.getRows()[0]?.[0] || 0);
      console.log(`${logPrefix} Row count: ${rowCount}`);
      
      // Get column info for debugging
      const describeFullQuery = `DESCRIBE SELECT * FROM (${selectWithHash}) t`;
      const describeFullReader = await connection.runAndReadAll(describeFullQuery);
      const columnInfo = describeFullReader.getRows();
      console.log(`${logPrefix} Column count: ${columnInfo.length}`);
      console.log(`${logPrefix} Columns: ${columnInfo.map(row => `${row[0]}:${row[1]}`).join(', ')}`);
      
      // Estimate if we need partitioning
      // Use simple heuristic: average 1KB per row per column (conservative estimate)
      const estimatedBytesPerRow = columnInfo.length * 1024;
      const estimatedTotalBytes = rowCount * estimatedBytesPerRow;
      const needsPartitioning = estimatedTotalBytes > MAX_FILE_SIZE_BYTES;
      
      console.log(`${logPrefix} Estimated total size: ${(estimatedTotalBytes / 1024 / 1024 / 1024).toFixed(2)} GiB`);
      console.log(`${logPrefix} Needs partitioning: ${needsPartitioning}`);
      
      let fileSize = 0;
      let tempParquetPath: string;
      
      if (needsPartitioning) {
        // Calculate number of partitions needed
        const rowsPerPartition = Math.floor(MAX_FILE_SIZE_BYTES / estimatedBytesPerRow);
        const numPartitions = Math.ceil(rowCount / rowsPerPartition);
        
        console.log(`${logPrefix} Exporting in ${numPartitions} partition(s) (~${rowsPerPartition} rows each)`);
        console.log(`${logPrefix} WARNING: File exceeds 10 GiB limit - using partitioned export`);
        
        // For now, we'll export to a single file and check the size
        // If it exceeds the limit, we'll throw an error with a helpful message
        const singleFilePath = path.join(os.tmpdir(), `${context.streamId}.parquet`);
        const escapedPath = singleFilePath.replace(/'/g, "''");
        const exportQuery = `COPY (${selectWithHash}) TO '${escapedPath}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')`;
        
        console.log(`${logPrefix} Starting Parquet export...`);
        const exportStartTime = Date.now();
        const timeoutMs = 10 * 60 * 1000; // 10 minutes for large datasets
        
        try {
          await this.executeWithTimeout(
            connection.run(exportQuery),
            timeoutMs,
            `Parquet export timed out after ${timeoutMs / 1000} seconds`
          );
          
          const exportDuration = Date.now() - exportStartTime;
          console.log(`${logPrefix} COPY command completed in ${exportDuration}ms`);
          
          if (!fs.existsSync(singleFilePath)) {
            throw new Error('Parquet file was not created despite successful COPY command');
          }
          
          fileSize = fs.statSync(singleFilePath).size;
          console.log(`${logPrefix} Created Parquet file: ${fileSize} bytes (${(fileSize / 1024 / 1024).toFixed(2)} MB)`);
          
          // Check if file exceeds Supabase Storage limit
          console.log(`${logPrefix} File size check: ${fileSize} bytes vs MAX_FILE_SIZE_BYTES: ${MAX_FILE_SIZE_BYTES} bytes`);
          console.log(`${logPrefix} File size in GiB: ${(fileSize / 1024 / 1024 / 1024).toFixed(2)} GiB vs Limit: ${(MAX_FILE_SIZE_BYTES / 1024 / 1024 / 1024).toFixed(2)} GiB`);
          if (fileSize > MAX_FILE_SIZE_BYTES) {
            fs.unlinkSync(singleFilePath); // Clean up oversized file
            throw new Error(
              `File size (${fileSize} bytes = ${(fileSize / 1024 / 1024 / 1024).toFixed(2)} GiB) exceeds limit of ${(MAX_FILE_SIZE_BYTES / 1024 / 1024 / 1024).toFixed(2)} GiB. ` +
              `This dataset has ${rowCount} rows with ${columnInfo.length} columns. ` +
              `Consider: 1) Filtering to fewer columns, 2) Using incremental sync with cursor_field, ` +
              `or 3) Partitioning the source table into multiple streams.`
            );
          }
          
          tempParquetPath = singleFilePath;
          console.log(`${logPrefix} Export rate: ${(rowCount / (exportDuration / 1000)).toFixed(0)} rows/sec`);
          
        } catch (error) {
          const exportDuration = Date.now() - exportStartTime;
          console.error(`${logPrefix} Export failed after ${exportDuration}ms:`, error);
          
          if (fs.existsSync(singleFilePath)) {
            const partialSize = fs.statSync(singleFilePath).size;
            console.error(`${logPrefix} Partial file created: ${partialSize} bytes`);
            fs.unlinkSync(singleFilePath);
          }
          
          throw error;
        }
      } else {
        // Standard single-file export
        const singleFilePath = path.join(os.tmpdir(), `${context.streamId}.parquet`);
        const escapedPath = singleFilePath.replace(/'/g, "''");
        const exportQuery = `COPY (${selectWithHash}) TO '${escapedPath}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')`;
        
        console.log(`${logPrefix} Starting Parquet export...`);
        console.log(`${logPrefix} Export path: ${singleFilePath}`);
        console.log(`${logPrefix} Estimated data size: ${rowCount} rows × ${columnInfo.length} columns`);
        
        const exportStartTime = Date.now();
        const timeoutMs = 10 * 60 * 1000; // 10 minutes
        
        try {
          await this.executeWithTimeout(
            connection.run(exportQuery),
            timeoutMs,
            `Parquet export timed out after ${timeoutMs / 1000} seconds`
          );
          
          const exportDuration = Date.now() - exportStartTime;
          console.log(`${logPrefix} COPY command completed in ${exportDuration}ms`);
          
          if (!fs.existsSync(singleFilePath)) {
            throw new Error('Parquet file was not created despite successful COPY command');
          }
          
          fileSize = fs.statSync(singleFilePath).size;
          console.log(`${logPrefix} Created Parquet file: ${fileSize} bytes (${(fileSize / 1024 / 1024).toFixed(2)} MB)`);
          console.log(`${logPrefix} File size in GiB: ${(fileSize / 1024 / 1024 / 1024).toFixed(2)} GiB vs Limit: ${(MAX_FILE_SIZE_BYTES / 1024 / 1024 / 1024).toFixed(2)} GiB`);
          console.log(`${logPrefix} Export rate: ${(rowCount / (exportDuration / 1000)).toFixed(0)} rows/sec`);
          
        } catch (error) {
          const exportDuration = Date.now() - exportStartTime;
          console.error(`${logPrefix} Export failed after ${exportDuration}ms:`, error);
          
          if (fs.existsSync(singleFilePath)) {
            const partialSize = fs.statSync(singleFilePath).size;
            console.error(`${logPrefix} Partial file created: ${partialSize} bytes`);
            fs.unlinkSync(singleFilePath);
          }
          
          throw error;
        }
        
        tempParquetPath = singleFilePath;
      }
      
      // Detach PostgreSQL database
      await connection.run('DETACH postgres_db');
      
      return { parquetPath: tempParquetPath, rowCount, fileSize };
      
    } finally {
      // DuckDB cleanup handled by instance going out of scope
    }
  }
  
  /**
   * Build SELECT query with field mappings and type casting for PostgreSQL
   */
  private async buildPostgresSelectQuery(
    connection: any,
    sourceQuery: string,
    schema: StreamSchemaColumn[],
    fieldMappings: FieldMapping[]
  ): Promise<string> {
    const logPrefix = '[PostgresProcessor]';
    
    if (fieldMappings.length > 0) {
      // Use field mappings to map source columns to target columns
      const columnExpressions = fieldMappings.map((fm: FieldMapping) => {
        const sourceCol = `source_data."${fm.source}"`;
        const targetCol = fm.target;
        
        // Find the target schema column to get type info
        const schemaCol = schema?.find((col: StreamSchemaColumn) => col.name === fm.target);
        
        // Apply type casting based on schema type
        let expression = sourceCol;
        if (schemaCol) {
          expression = this.applyTypeCasting(sourceCol, schemaCol.type);
        }
        
        return `${expression} AS "${targetCol}"`;
      });
      
      const mappedDataQuery = `SELECT ${columnExpressions.join(', ')} FROM (${sourceQuery}) source_data`;
      console.log(`${logPrefix} Using field mappings for column aliasing`);
      return mappedDataQuery;
      
    } else if (schema && schema.length > 0) {
      // Fallback: Use stream schema directly
      const columnExpressions = schema.map((col: StreamSchemaColumn) => {
        const colName = `"${col.name}"`;
        const expression = this.applyTypeCasting(colName, col.type);
        return `${expression} AS "${col.name}"`;
      });
      
      const schemaDataQuery = `SELECT ${columnExpressions.join(', ')} FROM (${sourceQuery}) source_data`;
      console.log(`${logPrefix} Using stream schema for column selection`);
      return schemaDataQuery;
      
    } else {
      // No schema defined, use all columns as-is
      console.log(`${logPrefix} No schema or mappings, using all columns as-is`);
      return sourceQuery;
    }
  }
}
