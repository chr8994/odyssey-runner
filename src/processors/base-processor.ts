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
   * Download previous parquet file for CDC comparison
   * Returns temp file path if previous sync exists, null otherwise
   * Uses .previous.parquet file for two-version storage strategy
   */
  protected async downloadPreviousParquet(
    tenantId: string,
    streamId: string
  ): Promise<string | null> {
    const logPrefix = `[${this.getTypeName()}Processor]`;
    const previousParquetPath = `${tenantId}/${streamId}.previous.parquet`;
    
    try {
      const { data, error } = await this.supabase
        .storage
        .from('streams')
        .download(previousParquetPath);
      
      if (error || !data) {
        console.log(`${logPrefix} No previous parquet found (first sync or .previous.parquet doesn't exist)`);
        return null;
      }
      
      // Write to temp file for DuckDB to read
      const tempPath = path.join(this.tempDir, `prev_${streamId}_${Date.now()}.parquet`);
      const buffer = Buffer.from(await data.arrayBuffer());
      fs.writeFileSync(tempPath, buffer);
      
      console.log(`${logPrefix} Downloaded previous parquet for CDC comparison from ${previousParquetPath}`);
      return tempPath;
      
    } catch (error) {
      console.warn(`${logPrefix} Failed to download previous parquet:`, error);
      return null;
    }
  }
  
  /**
   * Archive current parquet as previous for two-version strategy
   * Copies current .parquet to .previous.parquet before uploading new version
   */
  protected async archiveCurrentAsPrevious(
    tenantId: string,
    streamId: string
  ): Promise<void> {
    const logPrefix = `[${this.getTypeName()}Processor]`;
    const currentPath = `${tenantId}/${streamId}.parquet`;
    const previousPath = `${tenantId}/${streamId}.previous.parquet`;
    
    try {
      // Check if current parquet exists
      const { data, error } = await this.supabase
        .storage
        .from('streams')
        .download(currentPath);
      
      if (error || !data) {
        console.log(`${logPrefix} No current parquet to archive (first sync)`);
        return;
      }
      
      // Upload as .previous.parquet
      const buffer = Buffer.from(await data.arrayBuffer());
      const { error: uploadError } = await this.supabase
        .storage
        .from('streams')
        .upload(previousPath, buffer, {
          contentType: 'application/x-parquet',
          upsert: true,
        });
      
      if (uploadError) {
        console.warn(`${logPrefix} Failed to archive previous parquet:`, uploadError.message);
      } else {
        console.log(`${logPrefix} Archived current parquet as ${previousPath}`);
      }
      
    } catch (error) {
      console.warn(`${logPrefix} Error archiving current parquet:`, error);
      // Don't fail the sync if archiving fails
    }
  }
  
  /**
   * Append changes to rolling history file
   * Only appends INSERT, UPDATE, DELETE operations (excludes NO_CHANGE)
   * SNAPSHOT operations are converted to INSERT for history purposes
   * Adds $_sync_detected_at timestamp for each change
   */
  protected async appendChangesToHistory(
    connection: any,
    cdcResultQuery: string,
    tenantId: string,
    streamId: string,
    syncTimestamp: string
  ): Promise<number> {
    const logPrefix = `[${this.getTypeName()}Processor]`;
    const historyPath = `${tenantId}/${streamId}.history.parquet`;
    
    try {
      // First, get all columns from the CDC result
      const describeQuery = `DESCRIBE ${cdcResultQuery}`;
      const describeReader = await connection.runAndReadAll(describeQuery);
      const allColumns = describeReader.getRows().map((row: any) => String(row[0]));
      
      // Select all columns except $_operation (we'll redefine it)
      const nonOpColumns = allColumns.filter((col: string) => col !== `${SYSTEM_COLUMN_PREFIX}operation`);
      const columnList = nonOpColumns.map((col: string) => `"${col}"`).join(', ');
      
      // Extract only changes (exclude NO_CHANGE)
      // Convert SNAPSHOT to INSERT for history tracking
      const changesOnlyQuery = `
        SELECT 
          ${columnList},
          CASE 
            WHEN "${SYSTEM_COLUMN_PREFIX}operation" = 'SNAPSHOT' THEN 'INSERT'
            ELSE "${SYSTEM_COLUMN_PREFIX}operation"
          END AS "${SYSTEM_COLUMN_PREFIX}operation",
          '${syncTimestamp}'::TIMESTAMP AS "${SYSTEM_COLUMN_PREFIX}sync_detected_at"
        FROM (${cdcResultQuery}) t
        WHERE "${SYSTEM_COLUMN_PREFIX}operation" IN ('INSERT', 'UPDATE', 'DELETE', 'SNAPSHOT')
      `;
      
      console.log(`${logPrefix} Building history query with ${nonOpColumns.length} columns`);
      
      // Count changes
      const countQuery = `SELECT COUNT(*) as count FROM (${changesOnlyQuery}) t`;
      const countReader = await connection.runAndReadAll(countQuery);
      const changeCount = Number(countReader.getRows()[0]?.[0] || 0);
      
      if (changeCount === 0) {
        console.log(`${logPrefix} No changes detected, skipping history append`);
        return 0;
      }
      
      console.log(`${logPrefix} Appending ${changeCount} change(s) to history...`);
      
      // Download existing history if it exists
      let existingHistoryPath: string | null = null;
      try {
        const { data: historyData, error: historyError } = await this.supabase
          .storage
          .from('streams')
          .download(historyPath);
        
        if (!historyError && historyData) {
          existingHistoryPath = path.join(this.tempDir, `history_${streamId}_${Date.now()}.parquet`);
          const buffer = Buffer.from(await historyData.arrayBuffer());
          fs.writeFileSync(existingHistoryPath, buffer);
          console.log(`${logPrefix} Downloaded existing history for append`);
        }
      } catch (error) {
        console.log(`${logPrefix} No existing history file (will create new one)`);
      }
      
      // Build query to combine existing history with new changes
      // Use DISTINCT to avoid duplicates from COALESCE in DELETE rows
      let combinedQuery: string;
      if (existingHistoryPath) {
        const escapedHistoryPath = existingHistoryPath.replace(/'/g, "''");
        combinedQuery = `
          SELECT DISTINCT * FROM (
            SELECT * FROM read_parquet('${escapedHistoryPath}')
            UNION ALL
            SELECT * FROM (${changesOnlyQuery}) new_changes
          ) combined
        `;
        console.log(`${logPrefix} Deduplicating combined history (existing + new changes)`);
      } else {
        combinedQuery = `SELECT DISTINCT * FROM (${changesOnlyQuery}) t`;
        console.log(`${logPrefix} Creating new history with deduplication`);
      }
      
      // Export combined history to temp file
      const tempHistoryPath = path.join(this.tempDir, `new_history_${streamId}_${Date.now()}.parquet`);
      const escapedTempPath = tempHistoryPath.replace(/'/g, "''");
      await connection.run(`
        COPY (${combinedQuery}) 
        TO '${escapedTempPath}' 
        (FORMAT PARQUET, COMPRESSION 'SNAPPY')
      `);
      
      // Upload updated history
      const historyBuffer = fs.readFileSync(tempHistoryPath);
      const { error: uploadError } = await this.supabase
        .storage
        .from('streams')
        .upload(historyPath, historyBuffer, {
          contentType: 'application/x-parquet',
          upsert: true,
        });
      
      if (uploadError) {
        console.warn(`${logPrefix} Failed to upload history:`, uploadError.message);
      } else {
        console.log(`${logPrefix} Updated history file with ${changeCount} new change(s)`);
      }
      
      // Cleanup temp files
      this.cleanupTempFile(existingHistoryPath);
      this.cleanupTempFile(tempHistoryPath);
      
      return changeCount;
      
    } catch (error) {
      console.error(`${logPrefix} Error appending to history:`, error);
      // Don't fail the sync if history fails
      return 0;
    }
  }
  
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
      
      // Step 3: Archive current parquet as previous (for two-version strategy)
      await this.archiveCurrentAsPrevious(context.tenantId, context.streamId);
      
      // Step 4: Upload new parquet to streams bucket using stream to avoid 2 GiB Buffer limit
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
      
      // First, create the base data as a CTE to ensure column aliases are fully materialized
      // before we try to reference them in the hash calculation
      const cteQuery = `WITH base_data AS (${selectQuery}) SELECT * FROM base_data`;
      
      // Get column names from the CTE output
      const describeQuery = `DESCRIBE ${cteQuery}`;
      const describeReader = await connection.runAndReadAll(describeQuery);
      const columnNames = describeReader.getRows().map(row => String(row[0]));
      
      // Build hash using explicit column references from the CTE
      // Use unquoted column refs in CONCAT_WS to avoid DuckDB's alias resolution issues
      const hashExpressions = columnNames.map(col => `COALESCE("${col}"::VARCHAR, '')`);
      const hashColumn = `md5(CONCAT_WS('|', ${hashExpressions.join(', ')})) AS "${SYSTEM_COLUMN_PREFIX}ref"`;
      const quotedColumns = columnNames.map(col => `"${col}"`).join(', ');
      
      // Use CTE to first materialize the aliased columns, then select from that CTE to add the hash
      const selectWithHash = `WITH base_data AS (${selectQuery}) SELECT ${quotedColumns}, ${hashColumn} FROM base_data`;
      
      console.log(`${logPrefix} Added $_ref hash column for CDC tracking`);
      
      // ============================================================
      // CDC LOGIC: Detect row-level changes if previous sync exists
      // ============================================================
      const primaryKeys = context.stream.primary_key || [];
      const hasPrimaryKeys = primaryKeys.length > 0;
      const syncMode = context.stream.sync_mode || 'full_refresh';
      const cursorField = context.stream.cursor_field;
      
      // === CDC DECISION DEBUG ===
      console.log(`${logPrefix} ==================== CDC Decision Debug ====================`);
      console.log(`${logPrefix} Primary keys configured: ${JSON.stringify(primaryKeys)}`);
      console.log(`${logPrefix} Has primary keys: ${hasPrimaryKeys}`);
      console.log(`${logPrefix} Sync mode: ${syncMode}`);
      console.log(`${logPrefix} Cursor field: ${cursorField || 'none'}`);
      console.log(`${logPrefix} Will attempt CDC: ${hasPrimaryKeys} (using two-version storage strategy)`);
      console.log(`${logPrefix} ============================================================`);
      
      let finalQuery: string;
      let operationCounts = { INSERT: 0, UPDATE: 0, DELETE: 0, NO_CHANGE: 0, SNAPSHOT: 0 };
      
      // Check for previous parquet (for CDC comparison)
      // With two-version storage, we can do CDC even with full_refresh mode
      let previousParquetPath: string | null = null;
      if (hasPrimaryKeys) {
        console.log(`${logPrefix} Attempting to download previous parquet for CDC...`);
        previousParquetPath = await this.downloadPreviousParquet(
          context.tenantId,
          context.streamId
        );
        if (previousParquetPath) {
          console.log(`${logPrefix} Previous parquet found: ${previousParquetPath}`);
        } else {
          console.log(`${logPrefix} No previous parquet found - this will be a SNAPSHOT sync (first sync)`);
        }
      } else {
        console.log(`${logPrefix} Skipping previous parquet download (no primary keys configured)`);
      }
      
      if (previousParquetPath && hasPrimaryKeys) {
        // CDC MODE: Compare against previous sync
        console.log(`${logPrefix} CDC mode: Comparing against previous sync`);
        
        // Get all columns including $_ref
        const allColumnsQuery = `DESCRIBE ${selectWithHash}`;
        const allColumnsReader = await connection.runAndReadAll(allColumnsQuery);
        const allColumns = allColumnsReader.getRows().map(row => String(row[0]));
        
        // Build CDC comparison query
        finalQuery = await this.buildCDCComparisonQuery(
          connection,
          selectWithHash,
          previousParquetPath,
          primaryKeys,
          allColumns,
          cursorField
        );
        
        // Count operations for stats
        const countQuery = `
          SELECT 
            "${SYSTEM_COLUMN_PREFIX}operation",
            COUNT(*) as count
          FROM (${finalQuery}) t
          GROUP BY "${SYSTEM_COLUMN_PREFIX}operation"
        `;
        const countReader = await connection.runAndReadAll(countQuery);
        const countRows = countReader.getRows();
        
        for (const row of countRows) {
          const operation = String(row[0]) as keyof typeof operationCounts;
          const count = Number(row[1]);
          if (operation in operationCounts) {
            operationCounts[operation] = count;
          }
        }
        
        console.log(`${logPrefix} CDC detected: ${operationCounts.INSERT} INSERT, ${operationCounts.UPDATE} UPDATE, ${operationCounts.DELETE} DELETE, ${operationCounts.NO_CHANGE} NO_CHANGE`);
        
        // Append changes to rolling history file
        const syncTimestamp = new Date().toISOString();
        await this.appendChangesToHistory(
          connection,
          finalQuery,
          context.tenantId,
          context.streamId,
          syncTimestamp
        );
        
      } else {
        // SNAPSHOT MODE: First sync or no primary keys
        const reason = !hasPrimaryKeys 
          ? 'no primary keys configured' 
          : (syncMode === 'full_refresh' ? 'full refresh mode' : 'first sync');
        console.log(`${logPrefix} Snapshot mode (${reason})`);
        
        // Add system columns for snapshot
        const allColumnsQuery = `DESCRIBE ${selectWithHash}`;
        const allColumnsReader = await connection.runAndReadAll(allColumnsQuery);
        const allColumns = allColumnsReader.getRows().map(row => String(row[0]));
        const quotedCols = allColumns.map(col => `"${col}"`).join(', ');
        
        finalQuery = `
          SELECT 
            ${quotedCols},
            'SNAPSHOT' AS "${SYSTEM_COLUMN_PREFIX}operation",
            CURRENT_TIMESTAMP AS "${SYSTEM_COLUMN_PREFIX}timestamp",
            CURRENT_TIMESTAMP AS "${SYSTEM_COLUMN_PREFIX}last_updated"
          FROM (${selectWithHash}) t
        `;
        
        // Get row count for SNAPSHOT
        const countQuery = `SELECT COUNT(*) as count FROM (${finalQuery}) t`;
        const countReader = await connection.runAndReadAll(countQuery);
        operationCounts.SNAPSHOT = Number(countReader.getRows()[0]?.[0] || 0);
        
        console.log(`${logPrefix} Snapshot: ${operationCounts.SNAPSHOT} rows`);
        
        // Create history file on first sync if primary keys exist
        // Treat SNAPSHOT rows as INSERT for history purposes
        if (hasPrimaryKeys && operationCounts.SNAPSHOT > 0) {
          console.log(`${logPrefix} Creating initial history file with ${operationCounts.SNAPSHOT} SNAPSHOT rows (treated as INSERT)`);
          const syncTimestamp = new Date().toISOString();
          await this.appendChangesToHistory(
            connection,
            finalQuery,
            context.tenantId,
            context.streamId,
            syncTimestamp
          );
        }
      }
      
      // Get total row count
      const countQuery = `SELECT COUNT(*) as count FROM (${finalQuery}) t`;
      const countReader = await connection.runAndReadAll(countQuery);
      const rowCount = Number(countReader.getRows()[0]?.[0] || 0);
      console.log(`${logPrefix} Total row count: ${rowCount}`);
      
      // Export to Parquet
      const tempParquetPath = path.join(this.tempDir, `${context.streamId}.parquet`);
      const escapedParquetPath = tempParquetPath.replace(/'/g, "''");
      const exportQuery = `COPY (${finalQuery}) TO '${escapedParquetPath}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')`;
      await connection.run(exportQuery);
      
      const fileSize = fs.statSync(tempParquetPath).size;
      console.log(`${logPrefix} Created Parquet file: ${fileSize} bytes`);
      
      // DEBUG: Sample date columns to see what's in the parquet
      try {
        const sampleQuery = `
          SELECT contractDate, cancellationDate, closingDate,
                 TRY_CAST(contractDate AS VARCHAR) as contractDate_str,
                 TRY_CAST(cancellationDate AS VARCHAR) as cancellationDate_str,
                 contractDate IS NULL as contractDate_is_null
          FROM (${finalQuery}) 
          LIMIT 3
        `;
        const sampleReader = await connection.runAndReadAll(sampleQuery);
        const sampleRows = sampleReader.getRows();
        console.log(`${logPrefix} DEBUG - Sample date values in finalQuery:`, JSON.stringify(sampleRows, null, 2));
      } catch (e) {
        console.log(`${logPrefix} DEBUG - Could not sample date columns`);
      }
      
      // Cleanup previous parquet temp file
      if (previousParquetPath) {
        this.cleanupTempFile(previousParquetPath);
      }
      
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
        
        // Find the target schema column to get type info (case-insensitive match)
        const schemaCol = schema?.find((col: StreamSchemaColumn) => 
          col.name.toLowerCase() === fm.target.toLowerCase()
        );
        
        // Apply type casting based on schema type
        let expression = sourceCol;
        if (schemaCol) {
          expression = this.applyTypeCasting(sourceCol, schemaCol.type);
          // Apply zero date conversion for date/timestamp columns
          expression = this.applyZeroDateConversion(expression, schemaCol.type);
        } else {
          // No schema match - still try zero date conversion if we can detect date from source
          // This handles cases where schema column names don't match exactly
          console.log(`${logPrefix} No schema match for field mapping target: ${targetCol}`);
        }
        
        return `${expression} AS "${targetCol}"`;
      });
      
      const mappedDataQuery = `SELECT ${columnExpressions.join(', ')} FROM (${readQuery}) source_data`;
      console.log(`${logPrefix} Using field mappings for column aliasing`);
      return mappedDataQuery;
      
    } else if (schema && schema.length > 0) {
      // Fallback: Use stream schema directly
      const columnExpressions = schema.map((col: StreamSchemaColumn) => {
        const sourceCol = `source_data."${col.name}"`;
        let expression = this.applyTypeCasting(sourceCol, col.type);
        // Apply zero date conversion for date/timestamp columns
        expression = this.applyZeroDateConversion(expression, col.type);
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
   * Apply zero date conversion for date/timestamp columns
   * Converts MySQL zero dates (0000-00-00, 0000-00-00 00:00:00) to NULL
   */
  protected applyZeroDateConversion(columnExpression: string, schemaType: string | undefined): string {
    const logPrefix = `[${this.getTypeName()}Processor]`;
    
    if (!schemaType) {
      return columnExpression;
    }
    
    const lowerType = schemaType.toLowerCase();
    const isDateType = lowerType === 'date' || lowerType === 'timestamp' || lowerType === 'datetime';
    
    if (!isDateType) {
      return columnExpression;
    }
    
    console.log(`${logPrefix} Applying zero date conversion for column with type: ${schemaType}`);
    
    // Wrap the column expression in a CASE statement that converts zero dates to NULL
    // Try multiple detection methods since DuckDB may represent zero dates differently
    return `
      CASE 
        WHEN ${columnExpression} IS NULL THEN NULL
        WHEN TRY_CAST(${columnExpression} AS VARCHAR) IN ('0000-00-00', '0000-00-00 00:00:00', '', 'NULL') THEN NULL
        WHEN TRY_CAST(${columnExpression} AS VARCHAR) LIKE '0000-00-00%' THEN NULL
        WHEN TRY_CAST(${columnExpression} AS VARCHAR) LIKE '%-01-01 00:00:00' AND YEAR(TRY_CAST(${columnExpression} AS DATE)) <= 1 THEN NULL
        ELSE ${columnExpression}
      END
    `.trim().replace(/\s+/g, ' ');
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
    const logPrefix = `[${this.getTypeName()}Processor]`;
    
    try {
      const { data: dataModel, error: modelFetchError } = await this.supabase
        .from('data_models')
        .select('id, model_kind')
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
        console.warn(`${logPrefix} Failed to update data model:`, modelUpdateError);
      } else {
        console.log(`${logPrefix} Updated data model ${dataModel.id} with parquet path`);
        
        // CASCADE REFRESH: Find and queue dependent derived models
        // Only base models can have dependents (derived models don't have streams)
        if (dataModel.model_kind === 'base' || dataModel.model_kind === 'staging') {
          await this.triggerCascadeRefresh(dataModel.id);
        }
      }
    } catch (error) {
      console.error(`${logPrefix} Error updating data model:`, error);
    }
  }
  
  /**
   * Trigger cascade refresh for derived models that depend on this base model
   */
  protected async triggerCascadeRefresh(baseModelId: string): Promise<void> {
    const logPrefix = `[${this.getTypeName()}Processor]`;
    
    try {
      // Find derived models that depend on this base model
      const { data: dependentModels, error } = await this.supabase
        .from('data_models')
        .select('id, name, tenant_id')
        .eq('model_kind', 'derived')
        .contains('source_models', [baseModelId]);
      
      if (error) {
        console.error(`${logPrefix} Error finding dependent models:`, error);
        return;
      }
      
      if (!dependentModels || dependentModels.length === 0) {
        console.log(`${logPrefix} No dependent derived models found`);
        return;
      }
      
      console.log(`${logPrefix} Triggering cascade for ${dependentModels.length} dependent derived model(s)`);
      
      // Queue each dependent model for refresh
      for (const derived of dependentModels) {
        try {
          await this.queueDerivedModelRefresh(
            derived.id,
            derived.tenant_id,
            baseModelId
          );
          
          console.log(`${logPrefix} Queued cascade refresh for: ${derived.name} (${derived.id})`);
        } catch (queueError) {
          console.error(`${logPrefix} Failed to queue ${derived.name}:`, queueError);
          // Continue with other models even if one fails
        }
      }
    } catch (error) {
      console.error(`${logPrefix} Cascade refresh failed:`, error);
      // Don't throw - cascade failure shouldn't fail the main sync
    }
  }
  
  /**
   * Queue a derived model refresh job to PGMQ
   */
  protected async queueDerivedModelRefresh(
    modelId: string,
    tenantId: string,
    triggeredBy: string
  ): Promise<void> {
    const { error } = await this.supabase.rpc('nova_pgmq_send', {
      queue_name: 'stream_sync_jobs_derived',
      message: {
        model_id: modelId,
        tenant_id: tenantId,
        triggered_by: triggeredBy,
        trigger_type: 'cascade',
      },
    });
    
    if (error) {
      throw new Error(`Failed to queue derived model refresh: ${error.message}`);
    }
  }
  
  /**
   * Build DuckDB query to detect row-level changes via FULL OUTER JOIN
   * Compares current data against previous parquet to tag each row with operation
   */
  protected async buildCDCComparisonQuery(
    connection: any,
    currentDataCTE: string,
    previousParquetPath: string,
    primaryKeys: string[],
    allColumns: string[],
    cursorField?: string | null
  ): Promise<string> {
    const logPrefix = `[${this.getTypeName()}Processor]`;
    const escapedPrevPath = previousParquetPath.replace(/'/g, "''");
    
    // Build primary key join condition
    const pkJoinConditions = primaryKeys.map(pk => 
      `curr."${pk}" = prev."${pk}"`
    ).join(' AND ');
    
    // Build NULL filters for primary keys (prevent NULL=NULL false matches causing cartesian products)
    // Note: No table alias prefix here since these are used in CTE WHERE clauses before aliases are defined
    const currPkNotNull = primaryKeys.map(pk => `"${pk}" IS NOT NULL`).join(' AND ');
    const prevPkNotNull = primaryKeys.map(pk => `"${pk}" IS NOT NULL`).join(' AND ');
    
    // Check what columns actually exist in the previous parquet
    // This handles schema evolution (e.g., $_last_updated may not exist in older files)
    const prevSchemaQuery = `SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('${escapedPrevPath}'))`;
    const prevSchemaReader = await connection.runAndReadAll(prevSchemaQuery);
    const prevParquetColumns = new Set(prevSchemaReader.getRows().map((row: any) => String(row[0])));
    
    const hasLastUpdatedInPrev = prevParquetColumns.has(`${SYSTEM_COLUMN_PREFIX}last_updated`);
    console.log(`${logPrefix} Previous parquet has $_last_updated column: ${hasLastUpdatedInPrev}`);
    
    // Filter out system columns from previous parquet (they're from the previous CDC run)
    // Keep: data columns, $_ref (for change detection)
    // Only include $_last_updated if it exists in the previous parquet
    const dataColumnsOnly = allColumns.filter(col => {
      if (!col.startsWith(SYSTEM_COLUMN_PREFIX)) return true;
      if (col === `${SYSTEM_COLUMN_PREFIX}ref`) return true;
      if (col === `${SYSTEM_COLUMN_PREFIX}last_updated` && hasLastUpdatedInPrev) return true;
      return false;
    });
    
    // Build SELECT list for previous parquet - only include columns that exist in it
    // Also explicitly add $_last_updated if it exists in prev (even though it's not in current data)
    const prevColumnsForSelect = dataColumnsOnly.filter(col => prevParquetColumns.has(col));
    if (hasLastUpdatedInPrev && !prevColumnsForSelect.includes(`${SYSTEM_COLUMN_PREFIX}last_updated`)) {
      prevColumnsForSelect.push(`${SYSTEM_COLUMN_PREFIX}last_updated`);
    }
    const prevColumns = prevColumnsForSelect.map(col => `"${col}"`).join(', ');
    
    // Build column selections - use curr value, only fallback to prev for DELETE operations
    // This prevents COALESCE from restoring old values (like zero dates) when curr is intentionally NULL
    const columnSelections = dataColumnsOnly.map(col => {
      if (prevParquetColumns.has(col)) {
        // Only use prev value if curr row doesn't exist at all (DELETE operation)
        // For UPDATE/NO_CHANGE, keep curr value even if NULL
        return `CASE WHEN curr."${col}" IS NULL AND prev."${col}" IS NOT NULL AND curr."${SYSTEM_COLUMN_PREFIX}ref" IS NULL THEN prev."${col}" ELSE curr."${col}" END AS "${col}"`;
      } else {
        return `curr."${col}" AS "${col}"`;
      }
    }).join(', ');
    
    // Build $_last_updated logic - use CURRENT_TIMESTAMP if prev doesn't have the column
    const lastUpdatedExpression = hasLastUpdatedInPrev
      ? `CASE
          WHEN prev."${SYSTEM_COLUMN_PREFIX}ref" IS NULL THEN CURRENT_TIMESTAMP
          WHEN curr."${SYSTEM_COLUMN_PREFIX}ref" IS NULL THEN prev."${SYSTEM_COLUMN_PREFIX}last_updated"
          WHEN curr."${SYSTEM_COLUMN_PREFIX}ref" != prev."${SYSTEM_COLUMN_PREFIX}ref" THEN CURRENT_TIMESTAMP
          ELSE prev."${SYSTEM_COLUMN_PREFIX}last_updated"
        END`
      : 'CURRENT_TIMESTAMP';
    
    // Build partition key expression for deduplication
    const partitionKeys = primaryKeys.map(pk => `"${pk}"`).join(', ');
    
    // Deduplicate current data by primary key (keep first row per PK combination)
    // This prevents cartesian product when source has duplicate PKs
    const currDeduped = `
      SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionKeys} ORDER BY ${partitionKeys}) as _rn
        FROM (${currentDataCTE})
        WHERE ${currPkNotNull}
      ) WHERE _rn = 1
    `;
    
    // Deduplicate previous data by primary key
    const prevDeduped = `
      SELECT ${prevColumns} FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partitionKeys} ORDER BY ${partitionKeys}) as _rn
        FROM read_parquet('${escapedPrevPath}')
        WHERE ${prevPkNotNull}
      ) WHERE _rn = 1
    `;
    
    console.log(`${logPrefix} Deduplicating by primary key to prevent cartesian product`);
    
    const cdcQuery = `
      WITH 
        curr_filtered AS (${currDeduped}),
        prev_filtered AS (${prevDeduped})
      SELECT 
        ${columnSelections},
        CASE
          WHEN prev."${SYSTEM_COLUMN_PREFIX}ref" IS NULL THEN 'INSERT'
          WHEN curr."${SYSTEM_COLUMN_PREFIX}ref" IS NULL THEN 'DELETE'
          WHEN curr."${SYSTEM_COLUMN_PREFIX}ref" != prev."${SYSTEM_COLUMN_PREFIX}ref" THEN 'UPDATE'
          ELSE 'NO_CHANGE'
        END AS "${SYSTEM_COLUMN_PREFIX}operation",
        CURRENT_TIMESTAMP AS "${SYSTEM_COLUMN_PREFIX}timestamp",
        ${lastUpdatedExpression} AS "${SYSTEM_COLUMN_PREFIX}last_updated"
      FROM curr_filtered curr
      FULL OUTER JOIN prev_filtered prev ON ${pkJoinConditions}
    `;
    
    console.log(`${logPrefix} Built CDC comparison query with ${primaryKeys.length} primary key(s) (filtering NULLs)`);
    console.log(`${logPrefix} CDC Query Debug - Primary Keys: ${primaryKeys.join(', ')}`);
    console.log(`${logPrefix} CDC Query Debug - Join Condition: ${pkJoinConditions}`);
    console.log(`${logPrefix} CDC Query Debug - Curr PK Filter: ${currPkNotNull}`);
    console.log(`${logPrefix} CDC Query Debug - Prev PK Filter: ${prevPkNotNull}`);
    return cdcQuery;
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
