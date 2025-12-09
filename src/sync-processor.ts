/**
 * Sync Processor - Orchestrates data stream synchronization
 * 
 * This processor:
 * 1. Fetches stream configuration with data source
 * 2. Routes to the appropriate processor based on data source type
 * 3. Delegates the actual sync work to type-specific processors
 * 
 * Supported source types:
 * - file: Files in Supabase Storage (CSV, JSON, Parquet, Excel)
 * - sftp: Direct SFTP server connections
 * - postgres: PostgreSQL databases (future)
 * - mysql: MySQL databases (future)
 */

import { SupabaseClient } from '@supabase/supabase-js';
import { getProcessor, getSupportedTypes } from './processors/index.js';
import { 
  SyncResult, 
  PendingSyncJob, 
  ProcessorContext, 
  FieldMapping,
  DataStream,
  DataSource,
  StreamSchemaColumn,
} from './types.js';

export class SyncProcessor {
  private supabase: SupabaseClient;
  
  constructor(supabase: SupabaseClient) {
    this.supabase = supabase;
  }
  
  /**
   * Process a sync job for a data stream
   * 
   * @param job - The sync job to process
   * @returns SyncResult with outcome details
   */
  async process(job: PendingSyncJob): Promise<SyncResult> {
    const startTime = Date.now();
    
    console.log(`[SyncProcessor] Processing stream: ${job.stream_name} (${job.stream_id})`);
    console.log(`[SyncProcessor] Tenant: ${job.tenant_id}`);
    console.log(`[SyncProcessor] Trigger: ${job.trigger_type}`);
    
    try {
      // Step 1: Fetch stream with data source
      const stream = await this.fetchStream(job.stream_id);
      if (!stream) {
        throw new Error(`Stream not found: ${job.stream_id}`);
      }
      
      const dataSource = stream.data_source;
      const streamDirection = stream.direction || 'inbound';
      
      // Validate stream direction
      if (streamDirection === 'outbound') {
        throw new Error('Outbound streams cannot be synced via this processor');
      }
      
      // Validate data source
      if (!dataSource) {
        throw new Error('Stream has no source data source configured');
      }
      
      console.log(`[SyncProcessor] Stream loaded: ${stream.name}`);
      console.log(`[SyncProcessor] Source type: ${dataSource.type}`);
      console.log(`[SyncProcessor] Supported types: ${getSupportedTypes().join(', ')}`);
      
      // Step 2: Get the appropriate processor for this source type
      const processor = getProcessor(dataSource.type, this.supabase);
      
      if (!processor) {
        throw new Error(
          `Unsupported data source type: ${dataSource.type}. ` +
          `Supported types: ${getSupportedTypes().join(', ')}`
        );
      }
      
      // Step 3: Fetch field mappings
      const fieldMappings = await this.fetchFieldMappings(job.stream_id);
      console.log(`[SyncProcessor] Found ${fieldMappings.length} field mappings`);
      
      // Step 4: Build processor context
      const context: ProcessorContext = {
        streamId: job.stream_id,
        tenantId: job.tenant_id,
        runId: job.run_id,
        stream: stream as unknown as DataStream,
        dataSource: dataSource as DataSource,
        schema: (stream.schema || []) as StreamSchemaColumn[],
        fieldMappings,
      };
      
      // Step 5: Delegate to the processor
      const result = await processor.process(context);
      
      const duration = Date.now() - startTime;
      console.log(`[SyncProcessor] Sync ${result.success ? 'completed' : 'failed'} in ${duration}ms`);
      
      return result;
      
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      
      console.error(`[SyncProcessor] Sync failed after ${duration}ms:`, errorMessage);
      
      return {
        success: false,
        parquetPath: null,
        rowCount: 0,
        fileSize: 0,
        error: errorMessage,
      };
    }
  }
  
  /**
   * Fetch stream with data source from database
   */
  private async fetchStream(streamId: string) {
    const { data, error } = await this.supabase
      .from('data_streams')
      .select(`
        *,
        data_source:data_sources!data_streams_source_data_source_id_fkey (
          id, name, type, config, tenant_id, vault_secret_id
        )
      `)
      .eq('id', streamId)
      .single();
    
    if (error) {
      console.error('[SyncProcessor] Error fetching stream:', error);
      return null;
    }
    
    return data;
  }
  
  /**
   * Fetch field mappings for a stream
   */
  private async fetchFieldMappings(streamId: string): Promise<FieldMapping[]> {
    const { data: dataMapping } = await this.supabase
      .from('data_mappings')
      .select('id, field_mappings')
      .eq('data_stream_id', streamId)
      .single();
    
    return dataMapping?.field_mappings || [];
  }
}
