/**
 * Partition Builder
 * 
 * Builds DuckDB databases containing filtered data for user-scoped partitions.
 */

import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { SupabaseClient } from '@supabase/supabase-js';
import {
  PartitionBuildJob,
  PartitionBuildResult,
  PartitionBuildContext,
  PartitionRule,
  PartitionRuleModelMappingWithModel,
  DataModel,
} from './types.js';

const PARTITIONS_BUCKET = 'partitions';

function isNumericValue(value: string): boolean {
  if (value.trim() === '') return false;
  const num = Number(value);
  return !isNaN(num) && isFinite(num);
}

function formatPartitionValueForSQL(value: string): string {
  if (isNumericValue(value)) {
    return value;
  }
  const escaped = value.replace(/'/g, "''");
  return "'" + escaped + "'";
}

export class PartitionBuilder {
  private supabase: SupabaseClient;
  
  constructor(supabase: SupabaseClient) {
    this.supabase = supabase;
  }
  
  async build(job: PartitionBuildJob): Promise<PartitionBuildResult> {
    const { partition_rule_id, partition_value, tenant_id } = job;
    let tempDir: string | null = null;
    
    try {
      const context = await this.buildContext(partition_rule_id, partition_value, tenant_id);
      if (!context) {
        return { success: false, databasePath: null, totalRows: 0, fileSize: 0, tablesIncluded: [], error: 'Failed to build context' };
      }
      
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'partition-'));
      const sourceFiles = await this.downloadSourceParquets(context, tempDir);
      
      if (sourceFiles.length === 0) {
        return { success: false, databasePath: null, totalRows: 0, fileSize: 0, tablesIncluded: [], error: 'No source parquet files found' };
      }
      
      const dbPath = path.join(tempDir, 'data.db');
      const buildStats = await this.buildFilteredDatabase(context, sourceFiles, dbPath);
      const storagePath = await this.uploadDatabase(context, dbPath);
      
      return { success: true, databasePath: storagePath, totalRows: buildStats.totalRows, fileSize: buildStats.fileSize, tablesIncluded: buildStats.tablesIncluded };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      console.error('[PartitionBuilder] Error:', errorMessage);
      return { success: false, databasePath: null, totalRows: 0, fileSize: 0, tablesIncluded: [], error: errorMessage };
    } finally {
      if (tempDir) {
        try { fs.rmSync(tempDir, { recursive: true, force: true }); } catch (err) { }
      }
    }
  }
  
  private async buildContext(ruleId: string, partitionValue: string, tenantId: string): Promise<PartitionBuildContext | null> {
    try {
      const { data: rule, error: ruleError } = await this.supabase
        .from('partition_rules').select('*').eq('id', ruleId).eq('tenant_id', tenantId).single();
      
      if (ruleError || !rule) { console.error('[PartitionBuilder] Error fetching rule:', ruleError); return null; }
      
      const { data: mappings, error: mappingsError } = await this.supabase
        .from('partition_rule_model_mappings')
        .select(`*, data_model:data_models(*)`)
        .eq('partition_rule_id', ruleId);
      
      if (mappingsError || !mappings || mappings.length === 0) { console.error('[PartitionBuilder] Error fetching mappings:', mappingsError); return null; }
      
      return { ruleId, partitionValue, tenantId, rule: rule as PartitionRule, modelMappings: mappings as PartitionRuleModelMappingWithModel[] };
    } catch (error) { console.error('[PartitionBuilder] Error building context:', error); return null; }
  }
  
  private async downloadSourceParquets(context: PartitionBuildContext, tempDir: string): Promise<Array<{ mapping: PartitionRuleModelMappingWithModel; localPath: string }>> {
    const downloadedFiles: Array<{ mapping: PartitionRuleModelMappingWithModel; localPath: string }> = [];
    
    for (const mapping of context.modelMappings) {
      const model = mapping.data_model;
      if (!model?.parquet_path) { console.warn(`[PartitionBuilder] Model ${model?.name} has no parquet path, skipping`); continue; }
      
      try {
        const { data: fileData, error } = await this.supabase.storage.from('streams').download(model.parquet_path);
        if (error || !fileData) { console.error(`[PartitionBuilder] Error downloading ${model.parquet_path}:`, error); continue; }
        
        const localPath = path.join(tempDir, `${model.name}.parquet`);
        const buffer = Buffer.from(await fileData.arrayBuffer());
        fs.writeFileSync(localPath, buffer);
        
        downloadedFiles.push({ mapping, localPath });
        console.log(`[PartitionBuilder] Downloaded ${model.name} to ${localPath}`);
      } catch (error) { console.error(`[PartitionBuilder] Failed to download ${model.name}:`, error); }
    }
    return downloadedFiles;
  }
  
  private async buildFilteredDatabase(context: PartitionBuildContext, sourceFiles: Array<{ mapping: PartitionRuleModelMappingWithModel; localPath: string }>, dbPath: string): Promise<{ totalRows: number; fileSize: number; tablesIncluded: string[] }> {
    const duckdb = await import('@duckdb/node-api');
    const instance = await duckdb.DuckDBInstance.create(dbPath);
    const connection = await instance.connect();
    
    let totalRows = 0;
    const tablesIncluded: string[] = [];
    
    try {
      for (const { mapping, localPath } of sourceFiles) {
        const model = mapping.data_model;
        const tableName = mapping.table_alias || model.name;
        const partitionColumn = context.rule.partition_column;
        const partitionValue = context.partitionValue;
        
        const formattedValue = formatPartitionValueForSQL(partitionValue);
        const sql = `CREATE TABLE "${tableName}" AS SELECT * FROM read_parquet('${localPath}') WHERE "${partitionColumn}" = ${formattedValue}`;
        
        console.log(`[PartitionBuilder] Creating table ${tableName} with filter ${partitionColumn}=${formattedValue}`);
        await connection.run(sql);
        
        const countResult = await connection.run(`SELECT COUNT(*) as cnt FROM "${tableName}"`);
        const rows = await countResult.getRowsJS();
        const rowCount = (rows as unknown[][])[0]?.[0] || 0;
        
        totalRows += Number(rowCount);
        tablesIncluded.push(tableName);
        console.log(`[PartitionBuilder] Table ${tableName}: ${rowCount} rows`);
      }
      
      const stats = fs.statSync(dbPath);
      return { totalRows, fileSize: stats.size, tablesIncluded };
    } catch (error) { throw error; }
  }
  
  private async uploadDatabase(context: PartitionBuildContext, dbPath: string): Promise<string> {
    const { rule, partitionValue, tenantId } = context;
    const storagePath = `${tenantId}/${rule.slug}/${partitionValue}/data.db`;
    
    const fileBuffer = fs.readFileSync(dbPath);
    const { error } = await this.supabase.storage.from(PARTITIONS_BUCKET).upload(storagePath, fileBuffer, { contentType: 'application/octet-stream', upsert: true });
    
    if (error) { throw new Error(`Failed to upload database: ${error.message}`); }
    console.log(`[PartitionBuilder] Uploaded to ${PARTITIONS_BUCKET}/${storagePath}`);
    return storagePath;
  }
}
