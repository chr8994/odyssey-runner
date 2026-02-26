/**
 * Outbound Processor - Reverse ETL
 * 
 * Exports data FROM data models TO external destinations
 * Supports:
 * - CSV export to SFTP
 * - Direct INSERT to MySQL/Postgres databases
 */

import { SupabaseClient } from '@supabase/supabase-js';
import { DuckDBInstance } from '@duckdb/node-api';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  OutboundStreamContext,
  OutboundSyncResult,
  SftpTargetConfig,
  DatabaseTargetConfig,
  DataSource,
  SftpCredentials,
} from '../types.js';

/**
 * Outbound processor - handles reverse ETL operations
 */
export class OutboundProcessor {
  private supabase: SupabaseClient;
  private tempDir: string;
  
  constructor(supabase: SupabaseClient) {
    this.supabase = supabase;
    this.tempDir = os.tmpdir();
  }
  
  /**
   * Main process method - orchestrates the outbound sync
   */
  async process(context: OutboundStreamContext): Promise<OutboundSyncResult> {
    const startTime = Date.now();
    let tempCsvPath: string | null = null;
    let instance: DuckDBInstance | null = null;
    
    const logPrefix = '[OutboundProcessor]';
    
    console.log(`${logPrefix} Processing outbound stream: ${context.stream.name} (${context.streamId})`);
    console.log(`${logPrefix} Source model: ${context.sourceModel.name}`);
    console.log(`${logPrefix} Target: ${context.targetDataSource.type}`);
    console.log(`${logPrefix} Output format: ${context.stream.output_format}`);
    
    try {
      // Validate source model has parquet data
      if (!context.sourceModel.parquet_path) {
        throw new Error(`Source model ${context.sourceModel.name} has no parquet data. Run a refresh first.`);
      }
      
      // Step 1: Download source model's parquet file
      console.log(`${logPrefix} Downloading source parquet from: ${context.sourceModel.parquet_path}`);
      const parquetData = await this.downloadParquetFile(context.sourceModel.parquet_path);
      const tempParquetPath = this.getTempFilePath('source_model', `${context.sourceModel.id}.parquet`);
      fs.writeFileSync(tempParquetPath, parquetData);
      
      // Step 2: Convert to CSV using DuckDB
      console.log(`${logPrefix} Converting Parquet to CSV...`);
      instance = await DuckDBInstance.create(':memory:');
      const connection = await instance.connect();
      
      const escapedParquetPath = tempParquetPath.replace(/'/g, "''");
      tempCsvPath = this.getTempFilePath('export', `${context.streamId}.csv`);
      const escapedCsvPath = tempCsvPath.replace(/'/g, "''");
      
      // Read from parquet and export to CSV
      const includeHeader = context.stream.include_header !== false;
      const delimiter = context.stream.csv_delimiter || ',';
      
      const exportQuery = `
        COPY (SELECT * FROM read_parquet('${escapedParquetPath}'))
        TO '${escapedCsvPath}'
        (FORMAT CSV, HEADER ${includeHeader}, DELIMITER '${delimiter}')
      `;
      
      await connection.run(exportQuery);
      
      // Get row count and file size
      const countQuery = `SELECT COUNT(*) as count FROM read_parquet('${escapedParquetPath}')`;
      const countReader = await connection.runAndReadAll(countQuery);
      const rowCount = Number(countReader.getRows()[0]?.[0] || 0);
      
      const fileSize = fs.statSync(tempCsvPath).size;
      console.log(`${logPrefix} Generated CSV: ${rowCount} rows, ${fileSize} bytes`);
      
      // Step 3: Upload/Insert to target destination
      let outputPath: string | undefined;
      let targetTable: string | undefined;
      
      switch (context.targetDataSource.type) {
        case 'sftp':
          outputPath = await this.uploadToSftp(
            tempCsvPath,
            context.targetDataSource,
            context.stream.target_config as SftpTargetConfig
          );
          console.log(`${logPrefix} Uploaded to SFTP: ${outputPath}`);
          break;
          
        case 'postgres':
        case 'mysql':
          targetTable = await this.insertToDatabase(
            instance,
            tempParquetPath,
            context.targetDataSource,
            context.stream.target_config as DatabaseTargetConfig,
            context.tenantId,
            context.streamId
          );
          console.log(`${logPrefix} Inserted to ${context.targetDataSource.type}: ${targetTable}`);
          break;
          
        default:
          throw new Error(`Unsupported target type: ${context.targetDataSource.type}`);
      }
      
      // Step 4: Update stream sync statistics
      await this.updateOutboundSyncState(context.streamId, {
        success: true,
        rows_exported: rowCount,
        bytes_written: fileSize,
        output_path: outputPath ?? undefined,
        target_table: targetTable ?? undefined,
      });
      
      const duration = Date.now() - startTime;
      console.log(`${logPrefix} Outbound sync completed in ${duration}ms`);
      
      // Cleanup
      this.cleanupTempFile(tempParquetPath);
      this.cleanupTempFile(tempCsvPath);
      
      return {
        success: true,
        stream_id: context.streamId,
        job_id: context.jobId,
        rows_exported: rowCount,
        bytes_written: fileSize,
        output_path: outputPath,
        target_table: targetTable,
        duration_ms: duration,
      };
      
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      
      console.error(`${logPrefix} Outbound sync failed after ${duration}ms:`, errorMessage);
      
      // Cleanup
      this.cleanupTempFile(tempCsvPath);
      
      return {
        success: false,
        stream_id: context.streamId,
        job_id: context.jobId,
        rows_exported: 0,
        bytes_written: 0,
        duration_ms: duration,
        error: errorMessage,
      };
    } finally {
      // DuckDB cleanup
      if (instance) {
        // Instance will be cleaned up automatically when it goes out of scope
      }
    }
  }
  
  /**
   * Download parquet file from Supabase storage
   */
  private async downloadParquetFile(storagePath: string): Promise<Buffer> {
    const { data, error } = await this.supabase
      .storage
      .from('streams')
      .download(storagePath);
    
    if (error || !data) {
      throw new Error(`Failed to download parquet file: ${error?.message || 'No data'}`);
    }
    
    return Buffer.from(await data.arrayBuffer());
  }
  
  /**
   * Upload CSV file to SFTP server
   */
  private async uploadToSftp(
    localCsvPath: string,
    dataSource: DataSource,
    targetConfig: SftpTargetConfig
  ): Promise<string> {
    const logPrefix = '[OutboundProcessor:SFTP]';
    
    // Dynamic import of ssh2-sftp-client (ESM module)
    const { default: SftpClient } = await import('ssh2-sftp-client');
    
    // Get SFTP credentials from vault
    const credentials = await this.getCredentialsFromVault(dataSource.vault_secret_id);
    
    // Build SFTP config
    const sftpConfig = {
      host: dataSource.config.host!,
      port: dataSource.config.port || 22,
      username: dataSource.config.username!,
      password: credentials.password,
      privateKey: credentials.privateKey,
    };
    
    const sftp = new SftpClient();
    
    try {
      console.log(`${logPrefix} Connecting to ${sftpConfig.host}:${sftpConfig.port}...`);
      await sftp.connect(sftpConfig);
      
      // Build remote file path
      const remotePath = targetConfig.remotePath;
      let fileName = targetConfig.fileName || `export_${Date.now()}.csv`;
      
      // Replace placeholders in filename
      fileName = fileName
        .replace('{date}', new Date().toISOString().split('T')[0])
        .replace('{timestamp}', Date.now().toString())
        .replace('{model_name}', path.basename(localCsvPath, '.csv'));
      
      const remoteFilePath = `${remotePath}/${fileName}`;
      
      // Create directories if needed
      if (targetConfig.createDirectories !== false) {
        console.log(`${logPrefix} Ensuring directory exists: ${remotePath}`);
        await sftp.mkdir(remotePath, true);
      }
      
      // Handle existing file
      if (!targetConfig.overwriteExisting) {
        const exists = await sftp.exists(remoteFilePath);
        if (exists) {
          // Archive existing file if configured
          if (targetConfig.archiveAfterUpload) {
            const archivePath = `${remotePath}/archive`;
            await sftp.mkdir(archivePath, true);
            const archiveFile = `${archivePath}/${fileName}.${Date.now()}.bak`;
            console.log(`${logPrefix} Archiving existing file to: ${archiveFile}`);
            await sftp.rename(remoteFilePath, archiveFile);
          } else {
            throw new Error(`File already exists: ${remoteFilePath}. Set overwriteExisting=true to replace.`);
          }
        }
      }
      
      // Upload file
      console.log(`${logPrefix} Uploading to: ${remoteFilePath}`);
      await sftp.fastPut(localCsvPath, remoteFilePath);
      
      console.log(`${logPrefix} Upload complete`);
      return remoteFilePath;
      
    } finally {
      await sftp.end();
    }
  }
  
  /**
   * Get connection config from stored_connections table
   * This resolves the source_connection_id from data_sources.config to get the actual connection details
   */
  private async getStoredConnectionConfig(connectionId: string): Promise<{
    host: string;
    port: number;
    database: string;
    username: string;
    password: string;
    ssl?: boolean;
    connector_type: string;
  }> {
    const logPrefix = '[OutboundProcessor:StoredConnection]';
    
    // Fetch the stored connection record
    const { data: connection, error: connError } = await this.supabase
      .from('stored_connections')
      .select('*')
      .eq('id', connectionId)
      .single();
    
    if (connError || !connection) {
      throw new Error(`Failed to fetch stored connection ${connectionId}: ${connError?.message || 'Not found'}`);
    }
    
    console.log(`${logPrefix} Found stored connection: ${connection.name} (${connection.connector_type})`);
    
    // Get credentials from vault using the connection's vault_secret_id
    const credentials = await this.getCredentialsFromVault(connection.vault_secret_id);
    
    // The vault secret should contain the full connection config
    return {
      host: credentials.host,
      port: credentials.port || (connection.connector_type === 'postgres' ? 5432 : 3306),
      database: credentials.database,
      username: credentials.username || credentials.user,
      password: credentials.password,
      ssl: credentials.ssl,
      connector_type: connection.connector_type,
    };
  }
  
  /**
   * Regex pattern to match duplicate join columns like $_ref_1, name_2, etc.
   * These are created by DuckDB when joining tables with duplicate column names
   */
  private readonly DUPLICATE_COLUMN_PATTERN = /_\d+$/;
  
  /**
   * System column prefixes to exclude from exports
   * Only underscore-prefixed columns are excluded (DuckDB system columns like _rowid)
   * CDC columns starting with $_ are KEPT for visibility in target databases
   */
  private readonly SYSTEM_COLUMN_PREFIXES = ['_'];
  
  /**
   * Check if a column is a system/internal column that should be excluded from exports
   * Rules:
   * - Columns starting with underscore (_) are excluded (DuckDB system columns)
   * - Columns ending with _N (where N is a number) are excluded (join duplicates)
   * - CDC columns starting with $_ are KEPT ($_operation, $_timestamp, $_last_updated, $_ref)
   */
  private isSystemColumn(columnName: string): boolean {
    // Check for duplicate join columns (e.g., $_ref_1, name_2)
    if (this.DUPLICATE_COLUMN_PATTERN.test(columnName)) {
      return true;
    }
    
    // Check for underscore-prefixed system columns (but not $_ columns)
    return this.SYSTEM_COLUMN_PREFIXES.some(prefix => columnName.startsWith(prefix));
  }
  
  /**
   * The standard row hash column used for deduplication
   * This column contains a hash of the row content for change detection
   */
  private readonly ROW_HASH_COLUMN = '$_ref';
  
  /**
   * CDC (Change Data Capture) columns that should be excluded from target database exports
   * These columns are used internally for tracking but should not be sent to external databases
   * Note: $_ref is handled separately as it's used for change detection logic
   */
  private readonly CDC_COLUMNS = ['$_operation', '$_timestamp', '$_last_updated'];
  
  /**
   * Calculate table checksum from all $_ref values
   * This is used to quickly detect if any data changed since last sync
   */
  private async calculateTableChecksum(
    connection: any,
    parquetPath: string
  ): Promise<{ checksum: string; rowCount: number }> {
    const escapedParquet = parquetPath.replace(/'/g, "''");
    
    // Check if $_ref column exists
    const columnsQuery = `SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('${escapedParquet}'))`;
    const columnsResult = await connection.runAndReadAll(columnsQuery);
    const columns = columnsResult.getRows().map((row: any[]) => row[0] as string);
    
    if (!columns.includes(this.ROW_HASH_COLUMN)) {
      // No $_ref column, calculate checksum from all data
      const checksumQuery = `
        SELECT 
          md5(string_agg(CAST(* AS VARCHAR), '|' ORDER BY 1)) as checksum,
          COUNT(*) as row_count
        FROM read_parquet('${escapedParquet}')
      `;
      const result = await connection.runAndReadAll(checksumQuery);
      const row = result.getRows()[0];
      return {
        checksum: row[0] || 'empty',
        rowCount: Number(row[1] || 0),
      };
    }
    
    // Use $_ref column for efficient checksum
    const checksumQuery = `
      SELECT 
        md5(string_agg("${this.ROW_HASH_COLUMN}", '|' ORDER BY "${this.ROW_HASH_COLUMN}")) as checksum,
        COUNT(*) as row_count
      FROM read_parquet('${escapedParquet}')
    `;
    const result = await connection.runAndReadAll(checksumQuery);
    const row = result.getRows()[0];
    return {
      checksum: row[0] || 'empty',
      rowCount: Number(row[1] || 0),
    };
  }
  
  /**
   * Insert data to MySQL or Postgres database
   */
  private async insertToDatabase(
    instance: DuckDBInstance,
    parquetPath: string,
    dataSource: DataSource,
    targetConfig: DatabaseTargetConfig,
    tenantId: string,
    streamId: string
  ): Promise<string> {
    const logPrefix = `[OutboundProcessor:${dataSource.type.toUpperCase()}]`;
    const connection = await instance.connect();
    
    try {
      // Resolve connection details - either from stored_connections or directly from data source
      let connConfig: {
        host: string;
        port: number;
        database: string;
        username: string;
        password: string;
        ssl?: boolean;
        connector_type: string;
      };
      
      const sourceConnectionId = dataSource.config.source_connection_id as string | undefined;
      
      if (sourceConnectionId) {
        // New pattern: connection details are in stored_connections table
        console.log(`${logPrefix} Using stored connection: ${sourceConnectionId}`);
        connConfig = await this.getStoredConnectionConfig(sourceConnectionId);
      } else {
        // Legacy pattern: connection details are in data source config + vault
        console.log(`${logPrefix} Using legacy connection config from data source`);
        const credentials = await this.getCredentialsFromVault(dataSource.vault_secret_id);
        connConfig = {
          host: dataSource.config.host as string,
          port: (dataSource.config.port as number) || (dataSource.type === 'postgres' ? 5432 : 3306),
          database: dataSource.config.database as string,
          username: (dataSource.config.username || dataSource.config.user) as string,
          password: credentials.password,
          ssl: dataSource.config.ssl as boolean | undefined,
          connector_type: dataSource.type,
        };
      }
      
      // Build connection string using resolved config
      const connStr = this.buildDatabaseConnectionStringFromConfig(connConfig);
      
      // Load appropriate extension
      const dbType = dataSource.type === 'postgres' ? 'POSTGRES' : 'MYSQL';
      console.log(`${logPrefix} Loading ${dbType} extension...`);
      await connection.run(`INSTALL ${dataSource.type}`);
      await connection.run(`LOAD ${dataSource.type}`);
      
      // Attach target database
      const alias = 'target_db';
      console.log(`${logPrefix} Attaching target database...`);
      await connection.run(`ATTACH '${connStr}' AS ${alias} (TYPE ${dbType})`);
      
      // Build target table name
      const schemaName = targetConfig.schemaName || 'public';
      const tableName = targetConfig.tableName;
      const fullTableName = `${alias}.${schemaName}."${tableName}"`;
      
      // Read source data
      const escapedParquet = parquetPath.replace(/'/g, "''");
      
      // Get all column names from parquet
      const columnsQuery = `SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('${escapedParquet}'))`;
      const columnsResult = await connection.runAndReadAll(columnsQuery);
      const allColumns = columnsResult.getRows().map((row: any[]) => row[0] as string);
      
      // Check if $_ref column exists for auto-upsert
      const hasRefColumn = allColumns.includes(this.ROW_HASH_COLUMN);
      
      // Filter out system columns (but keep $_ref for change detection)
      const sourceColumns = allColumns.filter(col => !this.isSystemColumn(col));
      const excludedColumns = allColumns.filter(col => this.isSystemColumn(col));
      
      if (excludedColumns.length > 0) {
        console.log(`${logPrefix} Excluding ${excludedColumns.length} system column(s): ${excludedColumns.join(', ')}`);
      }
      console.log(`${logPrefix} Source has ${sourceColumns.length} columns`);
      
      // Get target table columns and nullability constraints (if table exists)
      let targetColumns: string[] = [];
      const targetColumnNullability = new Map<string, boolean>(); // column name -> is nullable
      try {
        const targetColumnsQuery = `SELECT column_name, is_nullable FROM (DESCRIBE ${fullTableName})`;
        const targetColumnsResult = await connection.runAndReadAll(targetColumnsQuery);
        for (const row of targetColumnsResult.getRows()) {
          const colName = row[0] as string;
          const isNullable = String(row[1]).toUpperCase() === 'YES';
          targetColumns.push(colName);
          targetColumnNullability.set(colName.toLowerCase(), isNullable);
        }
        console.log(`${logPrefix} Target table has ${targetColumns.length} columns`);
      } catch (e) {
        // Table doesn't exist yet, will be created
        console.log(`${logPrefix} Target table doesn't exist yet, will use source columns`);
      }
      
      // Find columns that exist in both source and target (for exporting to target)
      // If target table doesn't exist yet, use all source columns except $_ref and CDC columns
      let exportColumns: string[];
      const cdcColumnSet = new Set([this.ROW_HASH_COLUMN, ...this.CDC_COLUMNS]);
      
      if (targetColumns.length > 0) {
        const targetColumnSet = new Set(targetColumns.map(c => c.toLowerCase()));
        // Filter source columns by what exists in target, but exclude $_ref and CDC columns from export
        exportColumns = sourceColumns.filter(col => 
          !cdcColumnSet.has(col) && targetColumnSet.has(col.toLowerCase())
        );
        
        const skippedColumns = sourceColumns.filter(col => 
          !cdcColumnSet.has(col) && !targetColumnSet.has(col.toLowerCase())
        );
        if (skippedColumns.length > 0) {
          console.log(`${logPrefix} Skipping ${skippedColumns.length} column(s) not in target: ${skippedColumns.join(', ')}`);
        }
      } else {
        // New table - export all columns except $_ref and CDC columns (which are for internal tracking only)
        exportColumns = sourceColumns.filter(col => !cdcColumnSet.has(col));
        const excludedCdcColumns = sourceColumns.filter(col => cdcColumnSet.has(col));
        if (excludedCdcColumns.length > 0) {
          console.log(`${logPrefix} Excluding ${excludedCdcColumns.length} CDC column(s) from export: ${excludedCdcColumns.join(', ')}`);
        }
      }
      
      console.log(`${logPrefix} Exporting ${exportColumns.length} columns to target`);
      
      // Get schema info to detect date/timestamp columns
      const schemaQuery = `SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('${escapedParquet}'))`;
      const schemaResult = await connection.runAndReadAll(schemaQuery);
      const schemaMap = new Map<string, string>();
      for (const row of schemaResult.getRows()) {
        const colName = String(row[0]);
        const colType = String(row[1]).toUpperCase();
        schemaMap.set(colName, colType);
      }
      
      // Build column expressions with smart NULL handling based on target column constraints
      const columnExpressions = exportColumns.map(col => {
        const colType = schemaMap.get(col);
        const isTargetNullable = targetColumnNullability.get(col.toLowerCase()) !== false; // default to true if unknown
        
        // Handle date/timestamp columns with smart NULL/sentinel value logic
        if (colType?.includes('DATE') || colType?.includes('TIMESTAMP')) {
          const sentinelValue = colType.includes('TIMESTAMP') ? "TIMESTAMP '1900-01-01 00:00:00'" : "DATE '1900-01-01'";
          
          if (isTargetNullable) {
            console.log(`${logPrefix} Converting zero dates to NULL for column: ${col} (${colType}, nullable)`);
            // Target column is nullable - convert zero dates to NULL
            return `
              CASE 
                WHEN "${col}" IS NULL THEN NULL
                WHEN TRY_CAST("${col}" AS VARCHAR) = '0000-00-00' THEN NULL
                WHEN TRY_CAST("${col}" AS VARCHAR) LIKE '0000-00-00%' THEN NULL
                WHEN TRY_CAST("${col}" AS DATE) IS NULL THEN NULL
                WHEN YEAR(TRY_CAST("${col}" AS DATE)) = 0 THEN NULL
                WHEN TRY_CAST("${col}" AS DATE) = DATE '0000-01-01' THEN NULL
                WHEN TRY_CAST("${col}" AS DATE) < DATE '1900-01-01' THEN NULL
                ELSE CAST("${col}" AS ${colType})
              END AS "${col}"
            `.trim();
          } else {
            console.log(`${logPrefix} Converting zero dates to sentinel for column: ${col} (${colType}, NOT NULL)`);
            // Target column is NOT NULL - convert zero dates to sentinel value
            return `
              CASE 
                WHEN "${col}" IS NULL THEN ${sentinelValue}
                WHEN TRY_CAST("${col}" AS VARCHAR) = '0000-00-00' THEN ${sentinelValue}
                WHEN TRY_CAST("${col}" AS VARCHAR) LIKE '0000-00-00%' THEN ${sentinelValue}
                WHEN TRY_CAST("${col}" AS DATE) IS NULL THEN ${sentinelValue}
                WHEN YEAR(TRY_CAST("${col}" AS DATE)) = 0 THEN ${sentinelValue}
                WHEN TRY_CAST("${col}" AS DATE) = DATE '0000-01-01' THEN ${sentinelValue}
                WHEN TRY_CAST("${col}" AS DATE) < DATE '1900-01-01' THEN ${sentinelValue}
                ELSE CAST("${col}" AS ${colType})
              END AS "${col}"
            `.trim();
          }
        }
        
        // Handle text/varchar columns that are NOT NULL
        if (!isTargetNullable && (colType?.includes('VARCHAR') || colType?.includes('TEXT') || colType?.includes('CHAR'))) {
          console.log(`${logPrefix} Converting NULL to empty string for column: ${col} (${colType}, NOT NULL)`);
          return `COALESCE("${col}", '') AS "${col}"`;
        }
        
        // Handle numeric columns that are NOT NULL
        if (!isTargetNullable && (colType?.includes('INT') || colType?.includes('DECIMAL') || colType?.includes('NUMERIC') || colType?.includes('FLOAT') || colType?.includes('DOUBLE'))) {
          console.log(`${logPrefix} Converting NULL to 0 for column: ${col} (${colType}, NOT NULL)`);
          return `COALESCE("${col}", 0) AS "${col}"`;
        }
        
        // Default: use column as-is with explicit type casting if we know the type
        if (colType) {
          return `CAST("${col}" AS ${colType}) AS "${col}"`;
        }
        return `"${col}"`;
      });
      
      // Recalculate $_ref hash from transformed columns (after date conversions)
      // This ensures the hash is stable across syncs even with date format changes
      if (hasRefColumn) {
        const hashColumns = exportColumns.map(col => `COALESCE(CAST("${col}" AS VARCHAR), '')`).join(', ');
        const recalculatedRefExpr = `md5(CONCAT_WS('|', ${hashColumns})) AS "${this.ROW_HASH_COLUMN}"`;
        columnExpressions.push(recalculatedRefExpr);
        console.log(`${logPrefix} Recalculating ${this.ROW_HASH_COLUMN} from transformed columns for stable change detection`);
      }
      
      const columnList = columnExpressions.join(', ');
      let sourceQuery = `SELECT ${columnList} FROM read_parquet('${escapedParquet}')`;
      
      const dateColumns = exportColumns.filter(col => {
        const colType = schemaMap.get(col);
        return colType?.includes('DATE') || colType?.includes('TIMESTAMP');
      });
      const notNullColumns = exportColumns.filter(col => {
        return targetColumnNullability.get(col.toLowerCase()) === false;
      });
      console.log(`${logPrefix} Applied smart NULL handling to ${dateColumns.length} date/timestamp column(s)`);
      console.log(`${logPrefix} Target has ${notNullColumns.length} NOT NULL column(s): ${notNullColumns.slice(0, 5).join(', ')}${notNullColumns.length > 5 ? '...' : ''}`);
      
      // DEBUG: Sample the source query to see what we're sending to MySQL
      try {
        const debugQuery = `
          SELECT contractDate, cancellationDate, closingDate,
                 TRY_CAST(contractDate AS VARCHAR) as contractDate_str,
                 contractDate IS NULL as contractDate_is_null,
                 cancellationDate IS NULL as cancellationDate_is_null
          FROM (${sourceQuery})
          LIMIT 3
        `;
        const debugReader = await connection.runAndReadAll(debugQuery);
        const debugRows = debugReader.getRows();
        console.log(`${logPrefix} DEBUG - Sample values BEFORE MySQL insert:`, JSON.stringify(debugRows, null, 2));
      } catch (e) {
        console.log(`${logPrefix} DEBUG - Could not sample source query`);
      }
      
      // Determine upsert keys - use primaryKey from config, fallback to upsertKeys
      let effectiveUpsertKeys = targetConfig.primaryKey || targetConfig.upsertKeys || [];
      let useUpsert = targetConfig.upsertMode === true || effectiveUpsertKeys.length > 0;
      
      if (effectiveUpsertKeys.length > 0 && !targetConfig.truncateFirst) {
        console.log(`${logPrefix} Using primary key columns for upsert: ${effectiveUpsertKeys.join(', ')}`);
      }
      
      // Create table if needed
      if (targetConfig.createIfNotExists) {
        console.log(`${logPrefix} Creating table if not exists: ${tableName}`);
        await connection.run(`
          CREATE TABLE IF NOT EXISTS ${fullTableName}
          AS ${sourceQuery} LIMIT 0
        `);
        
        // Re-query table schema after creation to get actual nullability constraints
        if (targetColumns.length === 0) {
          console.log(`${logPrefix} Re-querying table schema after creation to get constraints...`);
          try {
            // Query MySQL information_schema directly to get column metadata
            // DESCRIBE on MySQL-attached tables doesn't return is_nullable
            const infoSchemaQuery = `
              SELECT COLUMN_NAME, IS_NULLABLE 
              FROM ${alias}.information_schema.COLUMNS 
              WHERE TABLE_SCHEMA = '${schemaName}' 
                AND TABLE_NAME = '${tableName}'
            `;
            const targetColumnsResult = await connection.runAndReadAll(infoSchemaQuery);
            targetColumns = []; // Clear and rebuild
            targetColumnNullability.clear();
            for (const row of targetColumnsResult.getRows()) {
              const colName = row[0] as string;
              const isNullable = String(row[1]).toUpperCase() === 'YES';
              targetColumns.push(colName);
              targetColumnNullability.set(colName.toLowerCase(), isNullable);
            }
            const notNullCount = Array.from(targetColumnNullability.values()).filter(v => !v).length;
            console.log(`${logPrefix} Re-queried schema from information_schema: ${targetColumns.length} columns, ${notNullCount} NOT NULL`);
            
            // IMPORTANT: Rebuild column expressions with actual NOT NULL constraints
            console.log(`${logPrefix} Rebuilding column expressions with actual constraints...`);
            const rebuiltColumnExpressions = exportColumns.map(col => {
              const colType = schemaMap.get(col);
              const isTargetNullable = targetColumnNullability.get(col.toLowerCase()) !== false;
              
              // Handle date/timestamp columns with smart NULL/sentinel value logic
              if (colType?.includes('DATE') || colType?.includes('TIMESTAMP')) {
                const sentinelValue = colType.includes('TIMESTAMP') ? "TIMESTAMP '1900-01-01 00:00:00'" : "DATE '1900-01-01'";
                
                if (isTargetNullable) {
                  // Target column is nullable - convert zero dates to NULL
                  return `
                    CASE 
                      WHEN "${col}" IS NULL THEN NULL
                      WHEN TRY_CAST("${col}" AS VARCHAR) = '0000-00-00' THEN NULL
                      WHEN TRY_CAST("${col}" AS VARCHAR) LIKE '0000-00-00%' THEN NULL
                      WHEN TRY_CAST("${col}" AS DATE) IS NULL THEN NULL
                      WHEN YEAR(TRY_CAST("${col}" AS DATE)) = 0 THEN NULL
                      WHEN TRY_CAST("${col}" AS DATE) = DATE '0000-01-01' THEN NULL
                      WHEN TRY_CAST("${col}" AS DATE) < DATE '1900-01-01' THEN NULL
                      ELSE CAST("${col}" AS ${colType})
                    END AS "${col}"
                  `.trim();
                } else {
                  console.log(`${logPrefix} Converting zero dates to sentinel for column: ${col} (${colType}, NOT NULL)`);
                  // Target column is NOT NULL - convert zero dates to sentinel value
                  return `
                    CASE 
                      WHEN "${col}" IS NULL THEN ${sentinelValue}
                      WHEN TRY_CAST("${col}" AS VARCHAR) = '0000-00-00' THEN ${sentinelValue}
                      WHEN TRY_CAST("${col}" AS VARCHAR) LIKE '0000-00-00%' THEN ${sentinelValue}
                      WHEN TRY_CAST("${col}" AS DATE) IS NULL THEN ${sentinelValue}
                      WHEN YEAR(TRY_CAST("${col}" AS DATE)) = 0 THEN ${sentinelValue}
                      WHEN TRY_CAST("${col}" AS DATE) = DATE '0000-01-01' THEN ${sentinelValue}
                      WHEN TRY_CAST("${col}" AS DATE) < DATE '1900-01-01' THEN ${sentinelValue}
                      ELSE CAST("${col}" AS ${colType})
                    END AS "${col}"
                  `.trim();
                }
              }
              
              // Handle text/varchar columns that are NOT NULL
              if (!isTargetNullable && (colType?.includes('VARCHAR') || colType?.includes('TEXT') || colType?.includes('CHAR'))) {
                console.log(`${logPrefix} Converting NULL to empty string for column: ${col} (${colType}, NOT NULL)`);
                return `COALESCE("${col}", '') AS "${col}"`;
              }
              
              // Handle numeric columns that are NOT NULL
              if (!isTargetNullable && (colType?.includes('INT') || colType?.includes('DECIMAL') || colType?.includes('NUMERIC') || colType?.includes('FLOAT') || colType?.includes('DOUBLE'))) {
                console.log(`${logPrefix} Converting NULL to 0 for column: ${col} (${colType}, NOT NULL)`);
                return `COALESCE("${col}", 0) AS "${col}"`;
              }
              
              // Default: use column as-is with explicit type casting if we know the type
              if (colType) {
                return `CAST("${col}" AS ${colType}) AS "${col}"`;
              }
              return `"${col}"`;
            });
            
            // Recalculate $_ref hash from transformed columns
            if (hasRefColumn) {
              const hashColumns = exportColumns.map(col => `COALESCE(CAST("${col}" AS VARCHAR), '')`).join(', ');
              const recalculatedRefExpr = `md5(CONCAT_WS('|', ${hashColumns})) AS "${this.ROW_HASH_COLUMN}"`;
              rebuiltColumnExpressions.push(recalculatedRefExpr);
            }
            
            const rebuiltColumnList = rebuiltColumnExpressions.join(', ');
            sourceQuery = `SELECT ${rebuiltColumnList} FROM read_parquet('${escapedParquet}')`;
            
            const notNullColumnsRebuilt = exportColumns.filter(col => {
              return targetColumnNullability.get(col.toLowerCase()) === false;
            });
            console.log(`${logPrefix} Rebuilt query with ${notNullColumnsRebuilt.length} NOT NULL columns handled`);
          } catch (e) {
            console.warn(`${logPrefix} Could not re-query table schema after creation:`, e);
          }
        }
        
        // If we have upsert keys, add PRIMARY KEY constraint
        if (effectiveUpsertKeys.length > 0) {
          const pkColumns = effectiveUpsertKeys.map(k => `"${k}"`).join(', ');
          try {
            console.log(`${logPrefix} Adding PRIMARY KEY constraint on: ${effectiveUpsertKeys.join(', ')}`);
            await connection.run(`
              ALTER TABLE ${fullTableName}
              ADD PRIMARY KEY (${pkColumns})
            `);
            console.log(`${logPrefix} PRIMARY KEY constraint added successfully`);
          } catch (e) {
            console.warn(`${logPrefix} Could not add PRIMARY KEY (may already exist):`, e);
          }
        } else {
          console.log(`${logPrefix} Note: Table created without PK/unique constraints.`);
        }
      }
      
      // Truncate if configured
      if (targetConfig.truncateFirst) {
        console.log(`${logPrefix} Truncating table: ${tableName}`);
        await connection.run(`DELETE FROM ${fullTableName}`);
      }
      
      // Insert or upsert data
      if (useUpsert && effectiveUpsertKeys.length > 0) {
        console.log(`${logPrefix} Upserting data with keys: ${effectiveUpsertKeys.join(', ')}`);
        
        // For upsert, we need to handle conflict resolution
        // This is database-specific
        if (dataSource.type === 'postgres') {
          // Postgres: ON CONFLICT DO UPDATE
          const conflictKeys = effectiveUpsertKeys.map(k => `"${k}"`).join(', ');
          
          // Get columns to update (all except conflict keys)
          const updateColumns = exportColumns.filter(col => !effectiveUpsertKeys.includes(col));
          
          if (updateColumns.length > 0) {
            const updateSet = updateColumns.map(col => `"${col}" = EXCLUDED."${col}"`).join(', ');
            
            await connection.run(`
              INSERT INTO ${fullTableName}
              ${sourceQuery}
              ON CONFLICT (${conflictKeys}) DO UPDATE SET ${updateSet}
            `);
          } else {
            // No columns to update, just skip duplicates
            await connection.run(`
              INSERT INTO ${fullTableName}
              ${sourceQuery}
              ON CONFLICT (${conflictKeys}) DO NOTHING
            `);
          }
        } else {
          // MySQL: Use INSERT ... ON DUPLICATE KEY UPDATE for proper upsert
          
          // Build list of column names (without CASE expressions) for final SELECT
          const simpleColumnList = exportColumns.map(col => `"${col}"`).join(', ');
          
          // Deduplicate source data by primary key to avoid multiple updates per key
          // Use ROW_NUMBER() to keep only one row per key combination
          const partitionKeys = effectiveUpsertKeys.map(k => `"${k}"`).join(', ');
          
          // Only include $_ref for change detection when doing incremental upserts
          // In truncate mode, we don't need change detection (and it causes column count mismatch)
          const selectColumns = (hasRefColumn && targetConfig.truncateFirst !== true)
            ? `${simpleColumnList}, "${this.ROW_HASH_COLUMN}"`
            : simpleColumnList;
          
          const deduplicatedQuery = `
            WITH source_with_transforms AS (${sourceQuery})
            SELECT ${selectColumns}
            FROM (
              SELECT *, 
                     ROW_NUMBER() OVER (PARTITION BY ${partitionKeys} ORDER BY ${partitionKeys}) as _rn
              FROM source_with_transforms
            ) sub
            WHERE _rn = 1
          `;
          
          // Check if truncateFirst is explicitly enabled
          if (targetConfig.truncateFirst === true) {
            // Truncate mode: clear the table and reload all data
            console.log(`${logPrefix} Using Truncate + Insert pattern for MySQL`);
            console.log(`${logPrefix} Truncating target table...`);
            await connection.run(`DELETE FROM ${fullTableName}`);
            
            // Simple INSERT (no duplicates after truncate)
            console.log(`${logPrefix} Inserting ${effectiveUpsertKeys.length ? 'deduplicated' : ''} rows...`);
            await connection.run(`INSERT INTO ${fullTableName} (${simpleColumnList}) ${deduplicatedQuery}`);
          } else {
            // Upsert mode: Use change detection with parquet snapshots (DEFAULT)
            console.log(`${logPrefix} Using change detection approach for MySQL upsert`);
            
            // Try to download previous snapshot
            const snapshotPath = `${tenantId}/${streamId}_snapshot.parquet`;
            let hasPreviousSnapshot = false;
            let tempPreviousPath: string | null = null;
            
            try {
              console.log(`${logPrefix} Checking for previous snapshot: streams/${snapshotPath}`);
              const { data: snapshotData } = await this.supabase
                .storage
                .from('streams')
                .download(snapshotPath);
              
              if (snapshotData) {
                tempPreviousPath = this.getTempFilePath('snapshot', `${streamId}_previous.parquet`);
                const snapshotBuffer = Buffer.from(await snapshotData.arrayBuffer());
                fs.writeFileSync(tempPreviousPath, snapshotBuffer);
                hasPreviousSnapshot = true;
                console.log(`${logPrefix} Previous snapshot found: streams/${snapshotPath}`);
                console.log(`${logPrefix} Previous snapshot downloaded to: ${tempPreviousPath}`);
              }
            } catch (e) {
              console.log(`${logPrefix} No previous snapshot found at: streams/${snapshotPath}`);
            }
            
            if (hasPreviousSnapshot && tempPreviousPath) {
              // Perform change detection
              const escapedPrevious = tempPreviousPath.replace(/'/g, "''");
              
              // Load both parquets
              console.log(`${logPrefix} Comparing current vs previous data...`);
              
              // Identify new rows (in current but not in previous)
              const pkJoinConditions = effectiveUpsertKeys.map(k => 
                `current."${k}" = previous."${k}"`
              ).join(' AND ');
              
              // Qualify column names with table alias to avoid ambiguity
              // Also include $_ref if it exists (needed for UPDATE detection)
              const simpleColumnListQualified = exportColumns.map(col => `current."${col}"`).join(', ');
              const simpleColumnListQualifiedWithRef = hasRefColumn
                ? `${simpleColumnListQualified}, current."${this.ROW_HASH_COLUMN}"`
                : simpleColumnListQualified;
              
              const newRowsQuery = `
                SELECT ${simpleColumnListQualified}
                FROM (${deduplicatedQuery}) current
                LEFT JOIN read_parquet('${escapedPrevious}') previous
                  ON ${pkJoinConditions}
                WHERE previous."${effectiveUpsertKeys[0]}" IS NULL
              `;
              
              const newRowsCount = await connection.runAndReadAll(`SELECT COUNT(*) FROM (${newRowsQuery})`);
              const newCount = Number(newRowsCount.getRows()[0]?.[0] || 0);
              console.log(`${logPrefix} Found ${newCount} new rows to INSERT`);
              
              if (newCount > 0) {
                // Use simple INSERT without ON CONFLICT (since DuckDB can't see MySQL constraints)
                // If duplicates exist, MySQL will reject them but we'll catch and ignore the error
                try {
                  await connection.run(`
                    INSERT INTO ${fullTableName} (${simpleColumnList})
                    ${newRowsQuery}
                  `);
                  console.log(`${logPrefix} Inserted ${newCount} new rows`);
                } catch (insertError: any) {
                  // Check if it's a duplicate key error (expected when snapshot and MySQL are out of sync)
                  if (insertError.message && insertError.message.includes('Duplicate entry')) {
                    console.log(`${logPrefix} Some rows already existed in MySQL (snapshot out of sync) - skipping duplicates`);
                    console.log(`${logPrefix} Note: Some of the ${newCount} "new" rows may have already existed`);
                  } else {
                    // Not a duplicate error, re-throw
                    throw insertError;
                  }
                }
              }
              
              // Identify updated rows (exist in both, but values differ)
              // Use $_ref hash column if available for efficient comparison
              let updatedRowsQuery: string;
              
              if (hasRefColumn) {
                updatedRowsQuery = `
                  SELECT ${simpleColumnListQualified}
                  FROM (${deduplicatedQuery}) current
                  INNER JOIN read_parquet('${escapedPrevious}') previous
                    ON ${pkJoinConditions}
                  WHERE current."${this.ROW_HASH_COLUMN}" != previous."${this.ROW_HASH_COLUMN}"
                `;
              } else {
                // Fallback: compare all non-PK columns (less efficient)
                const nonPkColumns = exportColumns.filter(col => !effectiveUpsertKeys.includes(col));
                const valueComparisons = nonPkColumns.map(col =>
                  `(current."${col}" IS DISTINCT FROM previous."${col}")`
                ).join(' OR ');
                
                updatedRowsQuery = `
                  SELECT ${simpleColumnListQualified}
                  FROM (${deduplicatedQuery}) current
                  INNER JOIN read_parquet('${escapedPrevious}') previous
                    ON ${pkJoinConditions}
                  WHERE ${valueComparisons}
                `;
              }
              
              const updatedRowsCount = await connection.runAndReadAll(`SELECT COUNT(*) FROM (${updatedRowsQuery})`);
              const updateCount = Number(updatedRowsCount.getRows()[0]?.[0] || 0);
              console.log(`${logPrefix} Found ${updateCount} rows to UPDATE`);
              
              if (updateCount > 0) {
                // Create a temporary staging table for updates
                console.log(`${logPrefix} Creating staging table for ${updateCount} updates...`);
                await connection.run(`CREATE TEMP TABLE updates_staging AS ${updatedRowsQuery}`);
                
                // Use UPDATE FROM pattern
                const nonPkColumns = exportColumns.filter(col => !effectiveUpsertKeys.includes(col));
                const updateSetClause = nonPkColumns.map(col => `"${col}" = updates_staging."${col}"`).join(', ');
                const whereClause = effectiveUpsertKeys.map(k => `${fullTableName}."${k}" = updates_staging."${k}"`).join(' AND ');
                
                try {
                  // Try UPDATE FROM syntax (may not be supported by MySQL)
                  await connection.run(`
                    UPDATE ${fullTableName}
                    SET ${updateSetClause}
                    FROM updates_staging
                    WHERE ${whereClause}
                  `);
                  console.log(`${logPrefix} Updated ${updateCount} rows via UPDATE FROM`);
                } catch (e) {
                  // UPDATE FROM not supported - use individual UPDATE statements
                  // The MySQL connector doesn't support complex CASE-based bulk updates
                  console.log(`${logPrefix} UPDATE FROM not supported, using individual UPDATE statements...`);
                  const updatedRows = await connection.runAndReadAll(`SELECT * FROM updates_staging`);
                  const rows = updatedRows.getRows();
                  
                  // Execute individual UPDATE statements for each row
                  // Less efficient but works within MySQL connector limitations
                  const pkIndexes = effectiveUpsertKeys.map(k => exportColumns.indexOf(k));
                  const nonPkIndexes = nonPkColumns.map(col => exportColumns.indexOf(col));
                  
                  let successfulUpdates = 0;
                  for (let i = 0; i < rows.length; i++) {
                    const row = rows[i];
                    
                    // Build WHERE clause from primary key values
                    const whereConditions = effectiveUpsertKeys.map((k, pkIdx) => {
                      const val = row[pkIndexes[pkIdx]];
                      const valStr = val === null ? 'NULL' : `'${String(val).replace(/'/g, "''")}'`;
                      return `"${k}" = ${valStr}`;
                    }).join(' AND ');
                    
                    // Build SET clause from non-PK columns
                    const setStatements = nonPkColumns.map((col, idx) => {
                      const val = row[nonPkIndexes[idx]];
                      const valStr = val === null ? 'NULL' : `'${String(val).replace(/'/g, "''")}'`;
                      return `"${col}" = ${valStr}`;
                    }).join(', ');
                    
                    const updateQuery = `
                      UPDATE ${fullTableName}
                      SET ${setStatements}
                      WHERE ${whereConditions}
                    `;
                    
                    try {
                      await connection.run(updateQuery);
                      successfulUpdates++;
                      
                      // Log progress every 50 rows
                      if ((i + 1) % 50 === 0 || (i + 1) === rows.length) {
                        console.log(`${logPrefix} Updated ${i + 1}/${rows.length} rows...`);
                      }
                    } catch (updateError) {
                      console.error(`${logPrefix} Failed to update row ${i + 1}:`, updateError);
                      // Continue with other updates
                    }
                  }
                  
                  console.log(`${logPrefix} Updated ${successfulUpdates}/${rows.length} rows via individual UPDATE statements`);
                }
                
                // Clean up staging table
                try {
                  await connection.run(`DROP TABLE updates_staging`);
                } catch (e) {
                  console.warn(`${logPrefix} Failed to drop staging table:`, e);
                }
              }
              
              console.log(`${logPrefix} Change detection complete: ${newCount} inserted, ${updateCount} updated`);
              
              // Gap fill: Check for rows in source that are missing from the MySQL target
              // This catches rows that were missed by previous bootstrap bugs or failed inserts
              if (effectiveUpsertKeys.length > 0) {
                try {
                  console.log(`${logPrefix} Gap fill: Checking for rows missing from target table...`);
                  
                  const gapFillJoinConditions = effectiveUpsertKeys.map(k => 
                    `source_data."${k}" = existing."${k}"`
                  ).join(' AND ');
                  
                  const simpleColumnListFromSource = exportColumns.map(col => `source_data."${col}"`).join(', ');
                  
                  const missingRowsQuery = `
                    SELECT ${simpleColumnListFromSource}
                    FROM (${deduplicatedQuery}) source_data
                    LEFT JOIN ${fullTableName} existing
                      ON ${gapFillJoinConditions}
                    WHERE existing."${effectiveUpsertKeys[0]}" IS NULL
                  `;
                  
                  const missingCountResult = await connection.runAndReadAll(
                    `SELECT COUNT(*) FROM (${missingRowsQuery})`
                  );
                  const missingCount = Number(missingCountResult.getRows()[0]?.[0] || 0);
                  
                  if (missingCount > 0) {
                    console.log(`${logPrefix} Gap fill: Found ${missingCount} rows in source missing from target - inserting...`);
                    try {
                      await connection.run(`
                        INSERT INTO ${fullTableName} (${simpleColumnList})
                        ${missingRowsQuery}
                      `);
                      console.log(`${logPrefix} Gap fill: Inserted ${missingCount} missing rows into target`);
                    } catch (insertError: any) {
                      if (insertError.message && insertError.message.includes('Duplicate entry')) {
                        console.log(`${logPrefix} Gap fill: Some rows already existed (race condition) - skipping duplicates`);
                      } else {
                        throw insertError;
                      }
                    }
                  } else {
                    console.log(`${logPrefix} Gap fill: All source rows exist in target - no gaps found`);
                  }
                } catch (gapFillError: any) {
                  console.error(`${logPrefix} Gap fill check failed (non-fatal):`, gapFillError.message);
                }
              }
              
              // Cleanup previous snapshot temp file
              this.cleanupTempFile(tempPreviousPath);
            } else {
              // No previous snapshot - bootstrap mode
              // Insert only rows that don't already exist in the target (safe for existing data)
              console.log(`${logPrefix} No snapshot found - bootstrapping with safe "insert missing rows" approach`);
              
              if (effectiveUpsertKeys.length > 0) {
                // Use primary keys to detect which rows are missing from the target
                console.log(`${logPrefix} Checking target table for existing rows using keys: ${effectiveUpsertKeys.join(', ')}`);
                
                try {
                  // Query: SELECT from source LEFT JOIN target ON PKs WHERE target PK IS NULL
                  const pkJoinConditions = effectiveUpsertKeys.map(k => 
                    `source_data."${k}" = existing."${k}"`
                  ).join(' AND ');
                  
                  const simpleColumnListFromSource = exportColumns.map(col => `source_data."${col}"`).join(', ');
                  
                  const missingRowsQuery = `
                    SELECT ${simpleColumnListFromSource}
                    FROM (${deduplicatedQuery}) source_data
                    LEFT JOIN ${fullTableName} existing
                      ON ${pkJoinConditions}
                    WHERE existing."${effectiveUpsertKeys[0]}" IS NULL
                  `;
                  
                  // Count missing rows first
                  const missingCountResult = await connection.runAndReadAll(
                    `SELECT COUNT(*) FROM (${missingRowsQuery})`
                  );
                  const missingCount = Number(missingCountResult.getRows()[0]?.[0] || 0);
                  
                  console.log(`${logPrefix} Bootstrap: Found ${missingCount} rows missing from target (out of source data)`);
                  
                  if (missingCount > 0) {
                    try {
                      await connection.run(`
                        INSERT INTO ${fullTableName} (${simpleColumnList})
                        ${missingRowsQuery}
                      `);
                      console.log(`${logPrefix} Bootstrap: Inserted ${missingCount} missing rows into target`);
                    } catch (insertError: any) {
                      if (insertError.message && insertError.message.includes('Duplicate entry')) {
                        console.log(`${logPrefix} Bootstrap: Some rows already existed (race condition) - skipping duplicates`);
                      } else {
                        throw insertError;
                      }
                    }
                  } else {
                    console.log(`${logPrefix} Bootstrap: All source rows already exist in target - nothing to insert`);
                  }
                } catch (bootstrapError: any) {
                  console.error(`${logPrefix} Bootstrap insert failed:`, bootstrapError.message);
                  console.log(`${logPrefix} Falling back to saving snapshot only (data will sync on next run with changes)`);
                }
              } else {
                // No primary keys defined - can't safely determine what's missing
                console.log(`${logPrefix} No primary keys defined - skipping bootstrap INSERT to avoid duplicates`);
                console.log(`${logPrefix} Configure primaryKey or upsertKeys in target_config to enable safe bootstrap`);
              }
            }
            
            // Save current parquet as the new snapshot for next sync
            console.log(`${logPrefix} Saving current snapshot for next sync...`);
            const snapshotBuffer = fs.readFileSync(parquetPath);
            const { error: uploadError } = await this.supabase
              .storage
              .from('streams')
              .upload(snapshotPath, snapshotBuffer, {
                contentType: 'application/x-parquet',
                upsert: true,
              });
            
            if (uploadError) {
              console.warn(`${logPrefix} Failed to save snapshot:`, uploadError);
            } else {
              console.log(`${logPrefix} Current snapshot saved to: streams/${snapshotPath}`);
            }
            
            // Summary
            console.log(`${logPrefix} === Snapshot Summary ===`);
            if (hasPreviousSnapshot) {
              console.log(`${logPrefix} Previous snapshot: streams/${snapshotPath} (used for comparison)`);
            } else {
              console.log(`${logPrefix} Previous snapshot: none (bootstrap mode)`);
            }
            console.log(`${logPrefix} Current snapshot: streams/${snapshotPath} (saved for next sync)`);
            console.log(`${logPrefix} Current parquet source: ${parquetPath}`);
            
            console.log(`${logPrefix} Upsert complete (change detection)`);
          }
        }
      } else {
        // Simple insert (only when truncate is used or no deduplication needed)
        console.log(`${logPrefix} Inserting data into: ${tableName}`);
        const batchSize = targetConfig.batchSize || 1000;
        
        // Get total row count
        const countQuery = `SELECT COUNT(*) as count FROM (${sourceQuery}) t`;
        const countReader = await connection.runAndReadAll(countQuery);
        const totalRows = Number(countReader.getRows()[0]?.[0] || 0);
        
        console.log(`${logPrefix} Inserting ${totalRows} rows in batches of ${batchSize}...`);
        
        // Insert in batches
        let offset = 0;
        while (offset < totalRows) {
          await connection.run(`
            INSERT INTO ${fullTableName}
            ${sourceQuery}
            LIMIT ${batchSize} OFFSET ${offset}
          `);
          offset += batchSize;
          console.log(`${logPrefix} Inserted ${Math.min(offset, totalRows)}/${totalRows} rows`);
        }
      }
      
      console.log(`${logPrefix} Insert complete`);
      return `${schemaName}.${tableName}`;
      
    } catch (error) {
      console.error(`${logPrefix} Database insert failed:`, error);
      throw error;
    }
  }
  
  /**
   * Get credentials from Supabase Vault
   */
  private async getCredentialsFromVault(vaultSecretId: string | undefined): Promise<any> {
    if (!vaultSecretId) {
      throw new Error('No vault secret ID configured for this data source');
    }
    
    // Fetch secret from vault using nova_get_secret_by_id (takes UUID)
    const { data, error } = await this.supabase.rpc('nova_get_secret_by_id', {
      secret_id: vaultSecretId,
    });
    
    if (error) {
      throw new Error(`Failed to retrieve credentials from vault: ${error.message}`);
    }
    
    if (!data || data.length === 0) {
      throw new Error(`Secret not found with ID: ${vaultSecretId}`);
    }
    
    // The RPC returns an array with decrypted_value column
    const decryptedValue = data[0]?.decrypted_value;
    
    if (!decryptedValue) {
      throw new Error('Secret has no decrypted value');
    }
    
    // Parse secret value (expected to be JSON with credentials)
    return typeof decryptedValue === 'string' ? JSON.parse(decryptedValue) : decryptedValue;
  }
  
  /**
   * Build database connection string from resolved config
   * This is the new unified method that works with both stored_connections and legacy patterns
   */
  private buildDatabaseConnectionStringFromConfig(config: {
    host: string;
    port: number;
    database: string;
    username: string;
    password: string;
    ssl?: boolean;
    connector_type: string;
  }): string {
    const encodedUser = encodeURIComponent(config.username);
    const encodedPassword = encodeURIComponent(config.password);
    
    if (config.connector_type === 'postgres') {
      let connStr = `postgresql://${encodedUser}:${encodedPassword}@${config.host}:${config.port}/${config.database}`;
      
      if (config.ssl) {
        connStr += '?sslmode=require';
      } else {
        connStr += '?sslmode=disable';
      }
      
      return connStr;
    } else if (config.connector_type === 'mysql') {
      return `mysql://${encodedUser}:${encodedPassword}@${config.host}:${config.port}/${config.database}`;
    }
    
    throw new Error(`Unsupported database type: ${config.connector_type}`);
  }
  
  /**
   * Build database connection string (legacy method - kept for backward compatibility)
   */
  private buildDatabaseConnectionString(dataSource: DataSource, credentials: any): string {
    const host = dataSource.config.host;
    const port = dataSource.config.port || (dataSource.type === 'postgres' ? 5432 : 3306);
    const database = dataSource.config.database;
    const user = (dataSource.config.username || dataSource.config.user) as string;
    const password = credentials.password;
    
    const encodedUser = encodeURIComponent(user);
    const encodedPassword = encodeURIComponent(password);
    
    if (dataSource.type === 'postgres') {
      let connStr = `postgresql://${encodedUser}:${encodedPassword}@${host}:${port}/${database}`;
      
      if (dataSource.config.ssl) {
        connStr += '?sslmode=require';
      } else {
        connStr += '?sslmode=disable';
      }
      
      return connStr;
    } else if (dataSource.type === 'mysql') {
      return `mysql://${encodedUser}:${encodedPassword}@${host}:${port}/${database}`;
    }
    
    throw new Error(`Unsupported database type: ${dataSource.type}`);
  }
  
  /**
   * Update outbound sync state in database
   */
  private async updateOutboundSyncState(
    streamId: string,
    result: {
      success: boolean;
      rows_exported: number;
      bytes_written: number;
      output_path?: string;
      target_table?: string;
      table_checksum?: string;
    }
  ): Promise<void> {
    try {
      const { error } = await this.supabase
        .from('data_sync_state')
        .upsert({
          data_stream_id: streamId,
          last_sync_at: new Date().toISOString(),
          sync_metadata: {
            last_outbound_sync_at: new Date().toISOString(),
            outbound_rows_synced: result.rows_exported,
            outbound_file_path: result.output_path,
            target_table: result.target_table,
            outbound_bytes_written: result.bytes_written,
            // Table checksum for change detection
            table_checksum: result.table_checksum,
            table_checksum_at: result.table_checksum ? new Date().toISOString() : undefined,
          },
          updated_at: new Date().toISOString(),
        }, {
          onConflict: 'data_stream_id',
        });
      
      if (error) {
        console.error('[OutboundProcessor] Error updating sync state:', error);
      }
    } catch (error) {
      console.error('[OutboundProcessor] Error updating sync state:', error);
    }
  }
  
  /**
   * Cleanup a temp file
   */
  private cleanupTempFile(filePath: string | null | undefined): void {
    if (filePath && fs.existsSync(filePath)) {
      try {
        fs.unlinkSync(filePath);
        console.log(`[OutboundProcessor] Cleaned up temp file: ${filePath}`);
      } catch (e) {
        console.warn(`[OutboundProcessor] Failed to cleanup temp file:`, e);
      }
    }
  }
  
  /**
   * Generate a temp file path
   */
  private getTempFilePath(prefix: string, fileName: string): string {
    return path.join(this.tempDir, `${prefix}_${Date.now()}_${fileName}`);
  }
}
