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
            context.stream.target_config as DatabaseTargetConfig
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
        output_path: outputPath,
        target_table: targetTable,
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
        output_path,
        target_table,
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
   * Insert data to MySQL or Postgres database
   */
  private async insertToDatabase(
    instance: DuckDBInstance,
    parquetPath: string,
    dataSource: DataSource,
    targetConfig: DatabaseTargetConfig
  ): Promise<string> {
    const logPrefix = `[OutboundProcessor:${dataSource.type.toUpperCase()}]`;
    const connection = await instance.connect();
    
    try {
      // Get database credentials from vault
      const credentials = await this.getCredentialsFromVault(dataSource.vault_secret_id);
      
      // Build connection string
      const connStr = this.buildDatabaseConnectionString(dataSource, credentials);
      
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
      const sourceQuery = `SELECT * FROM read_parquet('${escapedParquet}')`;
      
      // Create table if needed
      if (targetConfig.createIfNotExists) {
        console.log(`${logPrefix} Creating table if not exists: ${tableName}`);
        await connection.run(`
          CREATE TABLE IF NOT EXISTS ${fullTableName}
          AS ${sourceQuery} LIMIT 0
        `);
      }
      
      // Truncate if configured
      if (targetConfig.truncateFirst) {
        console.log(`${logPrefix} Truncating table: ${tableName}`);
        await connection.run(`DELETE FROM ${fullTableName}`);
      }
      
      // Insert or upsert data
      if (targetConfig.upsertMode && targetConfig.upsertKeys && targetConfig.upsertKeys.length > 0) {
        console.log(`${logPrefix} Upserting data with keys: ${targetConfig.upsertKeys.join(', ')}`);
        
        // For upsert, we need to handle conflict resolution
        // This is database-specific
        if (dataSource.type === 'postgres') {
          // Postgres: ON CONFLICT DO UPDATE
          const conflictKeys = targetConfig.upsertKeys.map(k => `"${k}"`).join(', ');
          const updateSet = `SET ${
            // Update all columns except conflict keys
            Object.keys((await connection.runAndReadAll(`${sourceQuery} LIMIT 1`)).getRows()[0] || {})
              .filter(col => !targetConfig.upsertKeys!.includes(col))
              .map(col => `"${col}" = EXCLUDED."${col}"`)
              .join(', ')
          }`;
          
          await connection.run(`
            INSERT INTO ${fullTableName}
            ${sourceQuery}
            ON CONFLICT (${conflictKeys}) DO UPDATE ${updateSet}
          `);
        } else {
          // MySQL: ON DUPLICATE KEY UPDATE
          console.warn(`${logPrefix} MySQL upsert not fully implemented yet, falling back to INSERT`);
          await connection.run(`INSERT INTO ${fullTableName} ${sourceQuery}`);
        }
      } else {
        // Simple insert
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
    
    const { data, error } = await this.supabase.rpc('nova_get_secret', {
      secret_id: vaultSecretId,
    });
    
    if (error || !data) {
      throw new Error(`Failed to retrieve credentials from vault: ${error?.message || 'No data'}`);
    }
    
    return JSON.parse(data.secret_value);
  }
  
  /**
   * Build database connection string
   */
  private buildDatabaseConnectionString(dataSource: DataSource, credentials: any): string {
    const host = dataSource.config.host;
    const port = dataSource.config.port || (dataSource.type === 'postgres' ? 5432 : 3306);
    const database = dataSource.config.database;
    const user = dataSource.config.username || dataSource.config.user;
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
