/**
 * MySQL Processor - Uses DuckDB's native MySQL scanner extension
 * 
 * Connects directly to MySQL databases and exports to Parquet
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
} from '../types.js';
import { BaseProcessor } from './base-processor.js';

// System column prefix for CDC tracking
const SYSTEM_COLUMN_PREFIX = '$_';

/**
 * MySQL credentials from vault
 */
interface MySqlCredentials {
  password?: string;
}

/**
 * MySQL configuration from data source
 * Note: The config may have either 'user' or 'username' depending on how it was created
 */
interface MySqlConfig {
  host: string;
  port: number | string;
  database: string;
  user?: string;      // Database config stores as 'user'
  username?: string;  // Support both for backwards compatibility
  table?: string;
  table_name?: string;      // Stream config field name
  query?: string;
  selected_table?: string;  // Data source config field name for table
}

/**
 * MySQL Processor using DuckDB's mysql_scanner extension
 */
export class MySqlProcessor extends BaseProcessor {
  constructor(supabase: SupabaseClient) {
    super(supabase);
  }
  
  getTypeName(): string {
    return 'MySql';
  }
  
  /**
   * Not used for MySQL - we use DuckDB's native MySQL scanner
   * This is only here to satisfy the abstract method requirement
   */
  async downloadSourceData(context: ProcessorContext): Promise<SourceDataResult> {
    throw new Error('MySQL processor does not use downloadSourceData - use process() directly');
  }
  
  /**
   * Main process method - orchestrates the MySQL sync using DuckDB
   * Overrides base class to use DuckDB's mysql_scanner extension
   */
  async process(context: ProcessorContext): Promise<SyncResult> {
    const startTime = Date.now();
    let tempParquetPath: string | null = null;
    
    const logPrefix = `[${this.getTypeName()}Processor]`;
    
    console.log(`${logPrefix} Processing stream: ${context.stream.name} (${context.streamId})`);
    console.log(`${logPrefix} Tenant: ${context.tenantId}`);
    
    try {
      // Get MySQL configuration
      const config = context.dataSource.config as unknown as MySqlConfig;
      
      if (!config.host || !config.database) {
        throw new Error('MySQL configuration missing required fields: host, database');
      }
      
      // Get credentials from vault
      const credentials = await this.getMySqlCredentials(context.dataSource.vault_secret_id);
      
      console.log(`${logPrefix} Connecting to MySQL: ${config.host}:${config.port || 3306}/${config.database}`);
      
      // Process with DuckDB MySQL scanner
      const { parquetPath, rowCount, fileSize } = await this.processWithMySqlScanner(
        config,
        credentials,
        context
      );
      tempParquetPath = parquetPath;
      
      // Upload to streams bucket
      const parquetStoragePath = `${context.tenantId}/${context.streamId}.parquet`;
      const parquetBuffer = fs.readFileSync(tempParquetPath);
      
      const { error: uploadError } = await this.supabase
        .storage
        .from('streams')
        .upload(parquetStoragePath, parquetBuffer, {
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
   * Get MySQL credentials from vault using nova_get_secret_by_id RPC function
   */
  private async getMySqlCredentials(vaultSecretId?: string): Promise<MySqlCredentials> {
    if (!vaultSecretId) {
      console.warn('[MySqlProcessor] No vault_secret_id provided, trying without credentials');
      return {};
    }
    
    try {
      console.log(`[MySqlProcessor] Fetching credentials from vault by ID: ${vaultSecretId}`);
      
      // Fetch secret from vault using nova_get_secret_by_id (takes UUID)
      const { data, error } = await this.supabase
        .rpc('nova_get_secret_by_id', { secret_id: vaultSecretId });
      
      if (error) {
        console.error('[MySqlProcessor] Vault RPC error:', error);
        return {};
      }
      
      if (!data || data.length === 0) {
        console.warn(`[MySqlProcessor] Secret not found with ID: ${vaultSecretId}`);
        return {};
      }
      
      // The RPC returns an array with decrypted_value column
      const decryptedValue = data[0]?.decrypted_value;
      
      if (!decryptedValue) {
        console.warn('[MySqlProcessor] Secret has no decrypted value');
        return {};
      }
      
      // Parse secret value (expected to be JSON with password)
      const secret = typeof decryptedValue === 'string' ? JSON.parse(decryptedValue) : decryptedValue;
      
      console.log('[MySqlProcessor] Credentials loaded from vault successfully');
      
      return {
        password: secret.password,
      };
    } catch (err) {
      console.error('[MySqlProcessor] Error fetching credentials:', err);
    }
    
    return {};
  }
  
  /**
   * Process MySQL data using DuckDB's mysql_scanner extension
   */
  private async processWithMySqlScanner(
    config: MySqlConfig,
    credentials: MySqlCredentials,
    context: ProcessorContext
  ): Promise<{ parquetPath: string; rowCount: number; fileSize: number }> {
    const logPrefix = '[MySqlProcessor]';
    
    // Initialize DuckDB
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    
    try {
      // Install and load MySQL extension
      console.log(`${logPrefix} Installing MySQL scanner extension...`);
      await connection.run('INSTALL mysql');
      await connection.run('LOAD mysql');
      
      // Build MySQL connection string using DuckDB's expected parameter names
      const host = config.host;
      const port = typeof config.port === 'string' ? parseInt(config.port, 10) : (config.port || 3306);
      const database = config.database;
      // Support both 'user' and 'username' fields (database stores as 'user')
      const username = config.user || config.username || 'root';
      const password = credentials.password;
      
      console.log(`[MySqlProcessor] Using username: ${username}`);
      
      // DuckDB MySQL extension expects: host, user, passwd, db, port, socket
      // Build connection parts - only include passwd if we have a password
      const connectionParts = [
        `host=${host}`,
        `port=${port}`,
        `user=${username}`,
        `db=${database}`,
      ];
      
      // Only add passwd if password exists (avoid empty passwd= causing parse error)
      if (password) {
        const escapedPassword = password.replace(/'/g, "''");
        connectionParts.splice(3, 0, `passwd=${escapedPassword}`); // Insert after user
        console.log(`${logPrefix} Using password authentication`);
      } else {
        console.log(`${logPrefix} No password provided, attempting passwordless connection`);
      }
      
      // Attach MySQL database
      console.log(`${logPrefix} Attaching MySQL database...`);
      const attachQuery = `ATTACH '${connectionParts.join(' ')}' AS mysql_db (TYPE mysql)`;
      await connection.run(attachQuery);
      
      // Determine the source query
      // Support 'table', 'table_name', and 'selected_table' field names
      const tableName = config.table || config.table_name || config.selected_table;
      
      let sourceQuery: string;
      if (config.query) {
        // Custom query provided
        sourceQuery = config.query;
        console.log(`${logPrefix} Using custom query`);
      } else if (tableName) {
        // Table name provided - handle schema.table format
        // If table already has a dot (schema.table), use it directly
        // Otherwise prepend mysql_db.
        const fullTableName = tableName.includes('.') 
          ? `mysql_db.${tableName.split('.')[0]}.${tableName.split('.')[1]}`
          : `mysql_db.${tableName}`;
        sourceQuery = `SELECT * FROM ${fullTableName}`;
        console.log(`${logPrefix} Reading from table: ${tableName}`);
      } else {
        throw new Error('MySQL configuration must include either "table", "selected_table", or "query"');
      }
      
      // Build SELECT with field mappings and type casting
      const selectQuery = await this.buildMySqlSelectQuery(
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
      
      // Export to Parquet
      const tempParquetPath = path.join(os.tmpdir(), `${context.streamId}.parquet`);
      const escapedParquetPath = tempParquetPath.replace(/'/g, "''");
      const exportQuery = `COPY (${selectWithHash}) TO '${escapedParquetPath}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')`;
      await connection.run(exportQuery);
      
      const fileSize = fs.statSync(tempParquetPath).size;
      console.log(`${logPrefix} Created Parquet file: ${fileSize} bytes`);
      
      // Detach MySQL database
      await connection.run('DETACH mysql_db');
      
      return { parquetPath: tempParquetPath, rowCount, fileSize };
      
    } finally {
      // DuckDB cleanup handled by instance going out of scope
    }
  }
  
  /**
   * Build SELECT query with field mappings and type casting for MySQL
   */
  private async buildMySqlSelectQuery(
    connection: any,
    sourceQuery: string,
    schema: StreamSchemaColumn[],
    fieldMappings: FieldMapping[]
  ): Promise<string> {
    const logPrefix = '[MySqlProcessor]';
    
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
