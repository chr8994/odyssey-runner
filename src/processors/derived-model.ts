/**
 * Derived Model Processor - Handles derived model refreshes
 * 
 * Derived models are computed from one or more source models using SQL transformations.
 * This processor:
 * 1. Downloads parquet files from all source models
 * 2. Loads them into DuckDB as tables
 * 3. Executes the query_definition SQL (joins, aggregations, filters)
 * 4. Adds $_ref hash column for CDC tracking
 * 5. Exports to parquet
 * 6. Updates the data_model record
 */

import { SupabaseClient } from '@supabase/supabase-js';
import { DuckDBInstance } from '@duckdb/node-api';
import * as fs from 'fs';
import * as path from 'path';
import {
  DerivedModelContext,
  DerivedModelRefreshResult,
  SourceModelInfo,
  QueryDefinition,
  JoinDefinition,
} from '../types.js';

// System column prefix for CDC tracking
const SYSTEM_COLUMN_PREFIX = '$_';

/**
 * Processor for derived model refreshes
 */
export class DerivedModelProcessor {
  protected supabase: SupabaseClient;
  protected tempDir: string;
  
  constructor(supabase: SupabaseClient) {
    this.supabase = supabase;
    this.tempDir = require('os').tmpdir();
  }
  
  /**
   * Process a derived model refresh
   */
  async process(context: DerivedModelContext): Promise<DerivedModelRefreshResult> {
    const logPrefix = '[DerivedModelProcessor]';
    const startTime = Date.now();
    const tempFiles: string[] = [];
    
    console.log(`${logPrefix} Processing derived model: ${context.dataModel.name} (${context.modelId})`);
    console.log(`${logPrefix} Tenant: ${context.tenantId}`);
    
    try {
      // Step 1: Validate derived model
      if (context.dataModel.model_kind !== 'derived') {
        throw new Error('Only derived models can be processed by this processor');
      }
      
      const sourceModelIds = context.dataModel.source_models || [];
      if (sourceModelIds.length === 0) {
        throw new Error('Derived model has no source models');
      }
      
      if (!context.dataModel.query_definition) {
        throw new Error('Derived model has no query_definition');
      }
      
      console.log(`${logPrefix} Loading ${sourceModelIds.length} source model(s)`);
      
      // Step 2: Fetch all source models
      const { data: sourceModels, error: fetchError } = await this.supabase
        .from('data_models')
        .select('id, name, parquet_path')
        .in('id', sourceModelIds);
      
      if (fetchError || !sourceModels) {
        throw new Error(`Failed to fetch source models: ${fetchError?.message}`);
      }
      
      // Validate all source models have parquet files
      for (const model of sourceModels) {
        if (!model.parquet_path) {
          throw new Error(`Source model ${model.name} has no parquet file`);
        }
      }
      
      // Step 3: Download all source model parquet files
      const sourceModelInfos: SourceModelInfo[] = [];
      for (const model of sourceModels) {
        const tempPath = await this.downloadModelParquet(model);
        tempFiles.push(tempPath);
        sourceModelInfos.push({
          id: model.id,
          name: model.name,
          parquet_path: model.parquet_path!,
          tempPath,
        });
      }
      
      console.log(`${logPrefix} Downloaded ${sourceModelInfos.length} source parquet file(s)`);
      
      // Step 4: Process with DuckDB
      const { parquetPath, rowCount, fileSize } = await this.processWithDuckDB(
        sourceModelInfos,
        context.dataModel.query_definition,
        context
      );
      tempFiles.push(parquetPath);
      
      // Step 5: Upload to streams bucket
      const parquetStoragePath = `${context.tenantId}/${context.modelId}.parquet`;
      const parquetBuffer = fs.readFileSync(parquetPath);
      
      const { error: uploadError } = await this.supabase
        .storage
        .from('streams')
        .upload(parquetStoragePath, parquetBuffer, {
          contentType: 'application/x-parquet',
          upsert: true,
        });
      
      if (uploadError) {
        throw new Error(`Failed to upload parquet: ${uploadError.message}`);
      }
      
      console.log(`${logPrefix} Uploaded to streams/${parquetStoragePath}`);
      
      const refreshedAt = new Date().toISOString();
      
      // Step 6: Update data_model with refresh results
      const { error: updateError } = await this.supabase
        .from('data_models')
        .update({
          parquet_path: parquetStoragePath,
          row_count: rowCount,
          file_size_bytes: fileSize,
          last_materialized_at: refreshedAt,
          last_refresh_at: refreshedAt,
          last_refresh_status: 'completed',
          last_refresh_error: null,
          status: 'ready',
          updated_at: refreshedAt,
        })
        .eq('id', context.modelId);
      
      if (updateError) {
        console.warn(`${logPrefix} Failed to update data model:`, updateError);
      } else {
        console.log(`${logPrefix} Updated data model with refresh results`);
      }
      
      const duration = Date.now() - startTime;
      console.log(`${logPrefix} Refresh completed in ${duration}ms`);
      
      return {
        success: true,
        parquetPath: parquetStoragePath,
        rowCount,
        fileSize,
      };
      
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      
      console.error(`${logPrefix} Refresh failed after ${duration}ms:`, errorMessage);
      
      // Update model with error status
      try {
        await this.supabase
          .from('data_models')
          .update({
            last_refresh_at: new Date().toISOString(),
            last_refresh_status: 'failed',
            last_refresh_error: errorMessage,
            status: 'error',
            error_message: errorMessage,
          })
          .eq('id', context.modelId);
      } catch (updateError) {
        console.error(`${logPrefix} Failed to update error status:`, updateError);
      }
      
      return {
        success: false,
        parquetPath: null,
        rowCount: 0,
        fileSize: 0,
        error: errorMessage,
      };
      
    } finally {
      // Cleanup temp files
      for (const tempFile of tempFiles) {
        this.cleanupTempFile(tempFile);
      }
    }
  }
  
  /**
   * Download a model's parquet file from storage
   */
  private async downloadModelParquet(model: { id: string; name: string; parquet_path: string }): Promise<string> {
    const logPrefix = '[DerivedModelProcessor]';
    
    if (!model.parquet_path) {
      throw new Error(`Model ${model.name} has no parquet file`);
    }
    
    console.log(`${logPrefix} Downloading ${model.name} from ${model.parquet_path}`);
    
    const { data, error } = await this.supabase
      .storage
      .from('streams')
      .download(model.parquet_path);
    
    if (error || !data) {
      throw new Error(`Failed to download ${model.name}: ${error?.message}`);
    }
    
    const tempPath = this.getTempFilePath('source', `${model.name}.parquet`);
    const buffer = Buffer.from(await data.arrayBuffer());
    fs.writeFileSync(tempPath, buffer);
    
    console.log(`${logPrefix} Downloaded ${model.name}: ${buffer.length} bytes`);
    
    return tempPath;
  }
  
  /**
   * Process source models with DuckDB and export to parquet
   */
  private async processWithDuckDB(
    sourceModels: SourceModelInfo[],
    queryDef: QueryDefinition,
    context: DerivedModelContext
  ): Promise<{ parquetPath: string; rowCount: number; fileSize: number }> {
    const logPrefix = '[DerivedModelProcessor]';
    
    // Initialize DuckDB
    const instance = await DuckDBInstance.create(':memory:');
    const connection = await instance.connect();
    
    try {
      // Load all source model parquet files as tables
      for (const source of sourceModels) {
        const escapedPath = source.tempPath!.replace(/'/g, "''");
        const tableName = this.sanitizeTableName(source.name);
        
        await connection.run(`
          CREATE TABLE ${tableName} AS 
          SELECT * FROM read_parquet('${escapedPath}')
        `);
        
        console.log(`${logPrefix} Loaded table: ${tableName}`);
      }
      
      // Build SQL from query_definition
      const sql = this.buildSQLFromQueryDefinition(queryDef, sourceModels);
      console.log(`${logPrefix} Executing query: ${sql.substring(0, 200)}...`);
      
      // Create result table
      await connection.run(`CREATE TABLE result AS ${sql}`);
      
      // Get column names
      const describeReader = await connection.runAndReadAll(
        `SELECT column_name FROM information_schema.columns WHERE table_name = 'result' ORDER BY ordinal_position`
      );
      const columnNames = describeReader.getRows().map(row => String(row[0]));
      
      console.log(`${logPrefix} Result columns: ${columnNames.join(', ')}`);
      
      // Build $_ref hash column
      const hashExpressions = columnNames.map(col => 
        `COALESCE(result."${col}"::VARCHAR, '')`
      );
      const hashColumn = `md5(CONCAT_WS('|', ${hashExpressions.join(', ')})) AS "${SYSTEM_COLUMN_PREFIX}ref"`;
      const quotedColumns = columnNames.map(col => `result."${col}"`).join(', ');
      const selectWithHash = `SELECT ${quotedColumns}, ${hashColumn} FROM result`;
      
      console.log(`${logPrefix} Added $_ref hash column for CDC tracking`);
      
      // Get row count
      const countReader = await connection.runAndReadAll(
        `SELECT COUNT(*) as count FROM (${selectWithHash}) t`
      );
      const rowCount = Number(countReader.getRows()[0]?.[0] || 0);
      console.log(`${logPrefix} Row count: ${rowCount}`);
      
      // Export to parquet
      const tempParquetPath = path.join(this.tempDir, `${context.modelId}.parquet`);
      const escapedParquetPath = tempParquetPath.replace(/'/g, "''");
      const exportQuery = `COPY (${selectWithHash}) TO '${escapedParquetPath}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')`;
      await connection.run(exportQuery);
      
      const fileSize = fs.statSync(tempParquetPath).size;
      console.log(`${logPrefix} Created parquet: ${fileSize} bytes`);
      
      return { parquetPath: tempParquetPath, rowCount, fileSize };
      
    } finally {
      // DuckDB cleanup handled by instance going out of scope
    }
  }
  
  /**
   * Build SQL query from query_definition JSON DSL
   */
  private buildSQLFromQueryDefinition(queryDef: QueryDefinition, sourceModels: SourceModelInfo[]): string {
    // Support custom SQL
    if (queryDef.type === 'custom' && queryDef.sql) {
      return queryDef.sql;
    }
    
    // Build structured query
    if (queryDef.type === 'join' || queryDef.type === 'aggregation' || queryDef.type === 'filter') {
      const fromTable = queryDef.from ? this.sanitizeTableName(queryDef.from) : this.sanitizeTableName(sourceModels[0].name);
      const selectCols = queryDef.select && queryDef.select.length > 0 
        ? queryDef.select.join(', ') 
        : '*';
      
      let sql = `SELECT ${selectCols} FROM ${fromTable}`;
      
      // Add joins
      if (queryDef.joins && queryDef.joins.length > 0) {
        for (const join of queryDef.joins) {
          const joinTable = this.sanitizeTableName(join.table);
          sql += ` ${join.type} JOIN ${joinTable} ON ${join.on}`;
        }
      }
      
      // Add WHERE clause
      if (queryDef.where) {
        sql += ` WHERE ${queryDef.where}`;
      }
      
      // Add GROUP BY
      if (queryDef.groupBy && queryDef.groupBy.length > 0) {
        sql += ` GROUP BY ${queryDef.groupBy.join(', ')}`;
      }
      
      // Add ORDER BY
      if (queryDef.orderBy && queryDef.orderBy.length > 0) {
        sql += ` ORDER BY ${queryDef.orderBy.join(', ')}`;
      }
      
      return sql;
    }
    
    throw new Error(`Unsupported query type: ${queryDef.type}`);
  }
  
  /**
   * Sanitize model name for use as SQL table name
   */
  private sanitizeTableName(name: string): string {
    return name.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
  }
  
  /**
   * Cleanup a temp file
   */
  private cleanupTempFile(filePath: string | null | undefined): void {
    if (filePath && fs.existsSync(filePath)) {
      try {
        fs.unlinkSync(filePath);
        console.log(`[DerivedModelProcessor] Cleaned up temp file: ${filePath}`);
      } catch (e) {
        console.warn(`[DerivedModelProcessor] Failed to cleanup temp file:`, e);
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
