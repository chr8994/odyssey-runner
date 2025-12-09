/**
 * Outbound Queue Processor
 * 
 * Processes outbound sync jobs from the PGMQ queue
 * Polls stream_sync_jobs_outbound queue and executes reverse ETL operations
 */

import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { OutboundProcessor } from './processors/outbound.js';
import {
  OutboundJobPayload,
  OutboundStreamContext,
  OutboundStream,
  DataModel,
  DataSource,
} from './types.js';

const QUEUE_NAME = 'stream_sync_jobs_outbound';
const POLL_INTERVAL_MS = 5000; // Poll every 5 seconds
const VIS_TIMEOUT_SECONDS = 300; // 5 minutes visibility timeout

export class OutboundQueueProcessor {
  private supabase: SupabaseClient;
  private processor: OutboundProcessor;
  private isRunning: boolean = false;
  private pollTimeout: NodeJS.Timeout | null = null;
  
  constructor() {
    // Initialize Supabase client
    const supabaseUrl = process.env.ORCHESTRATOR_SUPABASE_URL;
    const supabaseKey = process.env.ORCHESTRATOR_SUPABASE_SERVICE_ROLE_KEY;
    
    if (!supabaseUrl || !supabaseKey) {
      throw new Error('Missing ORCHESTRATOR_SUPABASE_URL or ORCHESTRATOR_SUPABASE_SERVICE_ROLE_KEY');
    }
    
    this.supabase = createClient(supabaseUrl, supabaseKey);
    this.processor = new OutboundProcessor(this.supabase);
  }
  
  /**
   * Start polling the queue
   */
  async start(): Promise<void> {
    console.log('[OutboundQueueProcessor] Starting...');
    this.isRunning = true;
    await this.poll();
  }
  
  /**
   * Stop polling the queue
   */
  stop(): void {
    console.log('[OutboundQueueProcessor] Stopping...');
    this.isRunning = false;
    if (this.pollTimeout) {
      clearTimeout(this.pollTimeout);
      this.pollTimeout = null;
    }
  }
  
  /**
   * Poll for jobs and process them
   */
  private async poll(): Promise<void> {
    if (!this.isRunning) return;
    
    try {
      // Read message from queue
      const { data: messages, error } = await this.supabase.rpc('pgmq_read', {
        queue_name: QUEUE_NAME,
        vt: VIS_TIMEOUT_SECONDS,
        qty: 1,
      });
      
      if (error) {
        console.error('[OutboundQueueProcessor] Error reading from queue:', error);
      } else if (messages && messages.length > 0) {
        const message = messages[0];
        await this.processJob(message.msg_id, message.message);
      }
    } catch (error) {
      console.error('[OutboundQueueProcessor] Error in poll cycle:', error);
    }
    
    // Schedule next poll
    if (this.isRunning) {
      this.pollTimeout = setTimeout(() => this.poll(), POLL_INTERVAL_MS);
    }
  }
  
  /**
   * Process a single outbound job
   */
  private async processJob(msgId: number, payload: OutboundJobPayload): Promise<void> {
    const { stream_id, tenant_id } = payload;
    
    console.log(`[OutboundQueueProcessor] Processing job ${msgId} for stream: ${stream_id}`);
    
    try {
      // Update job status to 'running'
      await this.updateJobStatus(stream_id, msgId.toString(), 'running');
      
      // Fetch stream with related data
      const context = await this.buildContext(stream_id, tenant_id, msgId.toString());
      
      if (!context) {
        throw new Error('Failed to build context');
      }
      
      // Execute outbound sync
      const result = await this.processor.process(context);
      
      if (result.success) {
        console.log(`[OutboundQueueProcessor] Job ${msgId} completed successfully`);
        console.log(`[OutboundQueueProcessor] Exported ${result.rows_exported} rows`);
        
        // Update job status to 'completed'
        await this.updateJobStatus(
          stream_id,
          msgId.toString(),
          'completed',
          null,
          result.rows_exported,
          result.bytes_written,
          result.output_path
        );
        
        // Delete message from queue (acknowledge)
        await this.supabase.rpc('pgmq_delete', {
          queue_name: QUEUE_NAME,
          msg_id: msgId,
        });
      } else {
        console.error(`[OutboundQueueProcessor] Job ${msgId} failed:`, result.error);
        
        // Update job status to 'failed'
        await this.updateJobStatus(
          stream_id,
          msgId.toString(),
          'failed',
          result.error
        );
        
        // Archive the message (move to dead letter queue)
        await this.supabase.rpc('pgmq_archive', {
          queue_name: QUEUE_NAME,
          msg_id: msgId,
        });
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      console.error(`[OutboundQueueProcessor] Job ${msgId} failed with exception:`, errorMessage);
      
      // Update job status to 'failed'
      await this.updateJobStatus(
        stream_id,
        msgId.toString(),
        'failed',
        errorMessage
      );
      
      // Archive the message
      await this.supabase.rpc('pgmq_archive', {
        queue_name: QUEUE_NAME,
        msg_id: msgId,
      });
    }
  }
  
  /**
   * Build execution context for outbound processor
   */
  private async buildContext(
    streamId: string,
    tenantId: string,
    jobId: string
  ): Promise<OutboundStreamContext | null> {
    try {
      // Fetch stream with related entities
      const { data: stream, error: streamError } = await this.supabase
        .from('data_streams')
        .select(`
          *,
          source_model:data_models!data_streams_source_model_id_fkey(*),
          target_data_source:data_sources!data_streams_target_data_source_id_fkey(*)
        `)
        .eq('id', streamId)
        .single();
      
      if (streamError || !stream) {
        console.error('[OutboundQueueProcessor] Error fetching stream:', streamError);
        return null;
      }
      
      // Validate outbound stream
      if (stream.direction !== 'outbound' && stream.direction !== 'bidirectional') {
        console.error('[OutboundQueueProcessor] Stream is not outbound:', stream.direction);
        return null;
      }
      
      if (!stream.source_model) {
        console.error('[OutboundQueueProcessor] Stream has no source model');
        return null;
      }
      
      if (!stream.target_data_source) {
        console.error('[OutboundQueueProcessor] Stream has no target data source');
        return null;
      }
      
      return {
        streamId,
        tenantId,
        jobId,
        stream: stream as OutboundStream,
        sourceModel: stream.source_model as DataModel,
        targetDataSource: stream.target_data_source as DataSource,
      };
    } catch (error) {
      console.error('[OutboundQueueProcessor] Error building context:', error);
      return null;
    }
  }
  
  /**
   * Update job status in database
   */
  private async updateJobStatus(
    streamId: string,
    jobId: string,
    status: string,
    error?: string | null,
    rowsExported?: number,
    bytesWritten?: number,
    outputPath?: string
  ): Promise<void> {
    try {
      await this.supabase.rpc('update_outbound_job_status', {
        p_stream_id: streamId,
        p_job_id: jobId,
        p_status: status,
        p_error: error || null,
        p_rows_exported: rowsExported || null,
        p_bytes_written: bytesWritten || null,
        p_output_path: outputPath || null,
      });
    } catch (error) {
      console.error('[OutboundQueueProcessor] Error updating job status:', error);
    }
  }
}

// Main execution
if (import.meta.url === `file://${process.argv[1]}`) {
  const processor = new OutboundQueueProcessor();
  
  // Start processor
  processor.start().catch((error) => {
    console.error('[OutboundQueueProcessor] Fatal error:', error);
    process.exit(1);
  });
  
  // Handle graceful shutdown
  const shutdown = () => {
    console.log('[OutboundQueueProcessor] Received shutdown signal');
    processor.stop();
    process.exit(0);
  };
  
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}
