/**
 * Base Processor - Abstract base class for all source processors
 * 
 * Provides shared functionality for:
 * - DuckDB initialization and query building
 * - Field mappings and type casting
 * - Parquet export and upload
 * - Sync state updates
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

// System column prefix for CDC tracking
const SYSTEM_COLUMN_PREFIX = '$_';

/**
 * Abstract base class for source processors
 */
export abstract class BaseProcessor {
  protected supabase: SupabaseClient;
  protected tempDir: string;
  
  constructor(supabase: SupabaseClient) {
    this.supabase = supabase;
    this.tempDir = os.tmpdir();
  }
  
  /**
   * Get the processor type name for logging
   */
  abstract getTypeName(): string;
  
  /**
   * Download source data and return path to temp file
   * Must be implemented by each processor
   */
  abstract downloadSourceData(context: ProcessorContext): Promise<SourceDataResult>;
  
  /**
   * Main process method - orchestrates the sync
   */
  async process(context: ProcessorContext): Promise<SyncResult> {
    const startTime = Date.now();
    let sourceResult: SourceDataResult | null = null;
    let tempParquetPath: string | null = null;
    
    const logPrefix = `[${this.getTypeName()}Processor]`;
    
    console.log(`${logPrefix} Processing stream: ${context.stream.name} (${context.streamId})`);
    console.log(`${logPrefix} Tenant: ${context.tenantId}`);
    
    try {
      // Step 1: Download source data
      console.log(`${logPrefix} Downloading source data...`);
      sourceResult = await this.downloadSourceData(context);
      console.log(`${logPrefix} Source file downloaded: ${sourceResult.fileName} (${sourceResult.fileSize} bytes)`);
      
      // Step 2: Process with DuckDB and export to Parquet
      const { parquetPath, rowCount, fileSize } = await this.processWithDuckDB(
        sourceResult.tempFilePath,
        sourceResult.fileName,
        context
      );
      tempParquetPath = parquetPath;
      
      // Step 3: Upload to streams bucket
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
      
      // Step 4: Update sync state
      await this.updateSyncState(context.streamId, context.tenantId, {
        success: true,
        parquetPath: parquetStoragePath,
        rowCount,
        fileSize,
      });
      
      // Step 5: Update linked data model
      await this.updateLinkedDataModel(context.streamId, {
        parquetPath: parquetStoragePath,
        rowCount,
        fileSize,
        syncedAt,
      });
      
      // Step 6: Update stream status
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
      // Cleanup temp files
      this.cleanupTempFile(sourceResult?.tempFilePath);
      this.cleanupTempFile(tempParquetPath);
    }
  }
  
  /**
   * Process source file with DuckDB and export to Parquet
   */
  protected async processWithDuckDB(
    sourcePath: string,
    fileName: string,
    context: ProcessorContext
  ): Promise<{ parquetPath: string; rowCount: number; fileSize: number }> {
    const logPrefix = `[${this.getTypeName()}Processor]`;
    
    // Initialize DuckDB
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    
    try {
      // Build read query based on file type
      const extension = fileName.split('.').pop()?.toLowerCase() || '';
      const escapedPath = sourcePath.replace(/'/g, "''");
      let readQuery: string;
      
      switch (extension) {
        case 'csv':
          readQuery = `SELECT * FROM read_csv('${escapedPath}', header=true, auto_detect=true)`;
          break;
        case 'json':
          readQuery = `SELECT * FROM read_json('${escapedPath}', auto_detect=true)`;
          break;
        case 'parquet':
          readQuery = `SELECT * FROM read_parquet('${escapedPath}')`;
          break;
        case 'xlsx':
        case 'xls':
          await connection.run('INSTALL spatial');
          await connection.run('LOAD spatial');
          readQuery = `SELECT * FROM st_read('${escapedPath}')`;
          break;
        default:
          readQuery = `SELECT * FROM read_csv('${escapedPath}', header=true, auto_detect=true)`;
      }
      
      console.log(`${logPrefix} Reading ${extension.toUpperCase()} file`);
      
      // Build SELECT with field mappings and type casting
      const selectQuery = await this.buildSelectQuery(
        connection,
        readQuery,
        context.schema,
        context.fieldMappings
      );
      
      // Get column names and add $_ref hash
      const describeQuery = `DESCRIBE SELECT * FROM (${selectQuery}) t`;
      const describeReader = await connection.runAndReadAll(describeQuery);
      const columnNames = describeReader.getRows().map(row => String(row[0]));
      
      // Use table-qualified column names to avoid DuckDB ambiguity between aliases and source columns
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
      const tempParquetPath = path.join(this.tempDir, `${context.streamId}.parquet`);
      const escapedParquetPath = tempParquetPath.replace(/'/g, "''");
      const exportQuery = `COPY (${selectWithHash}) TO '${escapedParquetPath}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')`;
      await connection.run(exportQuery);
      
      const fileSize = fs.statSync(tempParquetPath).size;
      console.log(`${logPrefix} Created Parquet file: ${fileSize} bytes`);
      
      return { parquetPath: tempParquetPath, rowCount, fileSize };
      
    } finally {
      // DuckDB cleanup handled by instance going out of scope
    }
  }
  
  /**
   * Build SELECT query with field mappings and type casting
   */
  protected async buildSelectQuery(
    connection: any,
    readQuery: string,
    schema: StreamSchemaColumn[],
    fieldMappings: FieldMapping[]
  ): Promise<string> {
    const logPrefix = `[${this.getTypeName()}Processor]`;
    
    if (fieldMappings.length > 0) {
      // Use field mappings to map source columns to target columns
      // Use table-qualified names to avoid DuckDB ambiguity between source columns and output aliases
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
      
      const mappedDataQuery = `SELECT ${columnExpressions.join(', ')} FROM (${readQuery}) source_data`;
      console.log(`${logPrefix} Using field mappings for column aliasing`);
      return mappedDataQuery;
      
    } else if (schema && schema.length > 0) {
      // Fallback: Use stream schema directly
      const columnExpressions = schema.map((col: StreamSchemaColumn) => {
        const colName = `"${col.name}"`;
        const expression = this.applyTypeCasting(colName, col.type);
        return `${expression} AS "${col.name}"`;
      });
      
      const schemaDataQuery = `SELECT ${columnExpressions.join(', ')} FROM (${readQuery}) source_data`;
      console.log(`${logPrefix} Using stream schema for column selection`);
      return schemaDataQuery;
      
    } else {
      // No schema defined, use all columns as-is
      console.log(`${logPrefix} No schema or mappings, using all columns as-is`);
      return readQuery;
    }
  }
  
  /**
   * Apply type casting based on schema type
   */
  protected applyTypeCasting(columnExpression: string, schemaType: string | undefined): string {
    if (!schemaType) return columnExpression;
    
    switch (schemaType.toLowerCase()) {
      case 'number':
      case 'double':
      case 'float':
        return `TRY_CAST(${columnExpression} AS DOUBLE)`;
      case 'integer':
      case 'int':
      case 'bigint':
        return `TRY_CAST(${columnExpression} AS BIGINT)`;
      case 'boolean':
      case 'bool':
        return `TRY_CAST(${columnExpression} AS BOOLEAN)`;
      case 'date':
        return `TRY_CAST(${columnExpression} AS DATE)`;
      case 'timestamp':
      case 'datetime':
        return `TRY_CAST(${columnExpression} AS TIMESTAMP)`;
      default:
        return columnExpression;
    }
  }
  
  /**
   * Update sync state after sync
   */
  protected async updateSyncState(
    streamId: string,
    tenantId: string,
    result: SyncResult
  ): Promise<void> {
    const syncedAt = new Date().toISOString();
    
    try {
      const { error: syncStateError } = await this.supabase
        .from('data_sync_state')
        .upsert({
          data_stream_id: streamId,
          tenant_id: tenantId,
          last_sync_at: syncedAt,
          last_successful_sync_at: result.success ? syncedAt : undefined,
          total_rows: result.rowCount,
          rows_inserted: result.rowCount,
          rows_updated: 0,
          rows_deleted: 0,
          last_snapshot_path: result.parquetPath,
          sync_status: result.success ? 'completed' : 'failed',
          updated_at: syncedAt,
        }, {
          onConflict: 'data_stream_id',
        });
      
      if (syncStateError) {
        console.error(`[${this.getTypeName()}Processor] Error updating sync state:`, syncStateError);
      }
    } catch (error) {
      console.error(`[${this.getTypeName()}Processor] Error updating sync state:`, error);
    }
  }
  
  /**
   * Update linked data model with parquet info
   */
  protected async updateLinkedDataModel(
    streamId: string,
    info: {
      parquetPath: string;
      rowCount: number;
      fileSize: number;
      syncedAt: string;
    }
  ): Promise<void> {
    try {
      const { data: dataModel, error: modelFetchError } = await this.supabase
        .from('data_models')
        .select('id')
        .eq('source_stream_id', streamId)
        .single();
      
      if (modelFetchError || !dataModel) {
        // No linked model, that's ok
        return;
      }
      
      const { error: modelUpdateError } = await this.supabase
        .from('data_models')
        .update({
          parquet_path: info.parquetPath,
          row_count: info.rowCount,
          file_size_bytes: info.fileSize,
          last_materialized_at: info.syncedAt,
          status: 'ready',
          updated_at: info.syncedAt,
        })
        .eq('id', dataModel.id);
      
      if (modelUpdateError) {
        console.warn(`[${this.getTypeName()}Processor] Failed to update data model:`, modelUpdateError);
      } else {
        console.log(`[${this.getTypeName()}Processor] Updated data model ${dataModel.id} with parquet path`);
      }
    } catch (error) {
      console.error(`[${this.getTypeName()}Processor] Error updating data model:`, error);
    }
  }
  
  /**
   * Cleanup a temp file
   */
  protected cleanupTempFile(filePath: string | null | undefined): void {
    if (filePath && fs.existsSync(filePath)) {
      try {
        fs.unlinkSync(filePath);
        console.log(`[${this.getTypeName()}Processor] Cleaned up temp file: ${filePath}`);
      } catch (e) {
        console.warn(`[${this.getTypeName()}Processor] Failed to cleanup temp file:`, e);
      }
    }
  }
  
  /**
   * Generate a temp file path
   */
  protected getTempFilePath(prefix: string, fileName: string): string {
    return path.join(this.tempDir, `${prefix}_${Date.now()}_${fileName}`);
  }
}
