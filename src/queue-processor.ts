/**
 * Queue Processor - Handles PGMQ polling and job orchestration
 * 
 * This processor:
 * 1. Polls the stream_sync_jobs PGMQ queue
 * 2. Processes jobs in batches
 * 3. Updates job status throughout processing
 * 4. Acknowledges successful jobs
 * 5. Handles retries and failures
 */

import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { config } from './config.js';
import { SyncProcessor } from './sync-processor.js';
import { PendingSyncJob, UpdateSyncRunStatusParams } from './types.js';

export class QueueProcessor {
  private supabase: SupabaseClient;
  private syncProcessor: SyncProcessor;
  private isRunning = false;
  
  constructor() {
    this.supabase = createClient(
      config.supabase.url,
      config.supabase.serviceKey,
      {
        auth: {
          autoRefreshToken: false,
          persistSession: false,
        },
      }
    );
    
    this.syncProcessor = new SyncProcessor(this.supabase);
  }
  
  /**
   * Start the queue processor
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      console.log('[QueueProcessor] Already running');
      return;
    }
    
    this.isRunning = true;
    console.log('[QueueProcessor] Starting queue processor...');
    console.log(`[QueueProcessor] Queue: stream_sync_jobs`);
    console.log(`[QueueProcessor] Batch size: ${config.queue.batchSize}`);
    console.log(`[QueueProcessor] Poll interval: ${config.queue.pollIntervalMs}ms`);
    console.log(`[QueueProcessor] Visibility timeout: ${config.queue.visibilityTimeoutSeconds}s`);
    
    await this.runWorker();
  }
  
  /**
   * Stop the queue processor gracefully
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      return;
    }
    
    console.log('[QueueProcessor] Stopping queue processor...');
    this.isRunning = false;
    console.log('[QueueProcessor] Stopped');
  }
  
  /**
   * Main worker loop - continuously polls for and processes jobs
   */
  private async runWorker(): Promise<void> {
    console.log('[QueueProcessor] Worker started. Listening for sync jobs...');
    
    while (this.isRunning) {
      try {
        // Fetch pending jobs from PGMQ
        const jobs = await this.getPendingJobs();
        
        if (!jobs || jobs.length === 0) {
          // No jobs available, wait and poll again
          await this.delay(config.queue.pollIntervalMs);
          continue;
        }
        
        console.log(`[QueueProcessor] Processing ${jobs.length} job(s)`);
        
        // Process each job sequentially
        for (const job of jobs) {
          if (!this.isRunning) {
            console.log('[QueueProcessor] Shutdown requested, stopping job processing');
            break;
          }
          
          await this.processJob(job);
        }
        
        // Small delay between batches
        await this.delay(1000);
        
      } catch (error) {
        console.error('[QueueProcessor] Worker error:', error);
        await this.delay(config.queue.pollIntervalMs);
      }
    }
  }
  
  /**
   * Fetch pending jobs from the PGMQ queue
   */
  private async getPendingJobs(): Promise<PendingSyncJob[] | null> {
    try {
      const { data, error } = await this.supabase.rpc('get_pending_sync_jobs', {
        p_batch_size: config.queue.batchSize,
        p_visibility_timeout: config.queue.visibilityTimeoutSeconds,
      });
      
      if (error) {
        console.error('[QueueProcessor] Error fetching jobs:', error);
        return null;
      }
      
      return data as PendingSyncJob[];
      
    } catch (error) {
      console.error('[QueueProcessor] Error in getPendingJobs:', error);
      return null;
    }
  }
  
  /**
   * Process a single sync job
   */
  private async processJob(job: PendingSyncJob): Promise<void> {
    const startTime = Date.now();
    
    console.log('═══════════════════════════════════════════════');
    console.log(`[QueueProcessor] Job started: ${job.stream_name}`);
    console.log(`[QueueProcessor] Stream ID: ${job.stream_id}`);
    console.log(`[QueueProcessor] Run ID: ${job.run_id}`);
    console.log(`[QueueProcessor] Trigger: ${job.trigger_type}`);
    console.log('═══════════════════════════════════════════════');
    
    try {
      // Mark job as processing
      await this.updateSyncRunStatus({
        p_run_id: job.run_id,
        p_status: 'processing',
      });
      
      // Execute the sync
      const result = await this.syncProcessor.process(job);
      
      const duration = Date.now() - startTime;
      
      if (result.success) {
        // Mark job as completed
        await this.updateSyncRunStatus({
          p_run_id: job.run_id,
          p_status: 'completed',
          p_rows_processed: result.rowCount,
          p_rows_inserted: result.rowCount,
          p_file_size_bytes: result.fileSize,
          p_parquet_path: result.parquetPath || undefined,
        });
        
        // Acknowledge the message (remove from queue)
        await this.acknowledgeJob(job.msg_id);
        
        console.log(`[QueueProcessor] ✓ Job completed in ${duration}ms: ${job.stream_name}`);
        
      } else {
        // Mark job as failed
        await this.updateSyncRunStatus({
          p_run_id: job.run_id,
          p_status: 'failed',
          p_error_message: result.error || 'Unknown error',
          p_error_details: { duration_ms: duration },
        });
        
        // Acknowledge to prevent infinite retries
        // (the failure is recorded in stream_sync_runs table)
        await this.acknowledgeJob(job.msg_id);
        
        console.log(`[QueueProcessor] ✗ Job failed after ${duration}ms: ${job.stream_name}`);
        console.log(`[QueueProcessor] Error: ${result.error}`);
      }
      
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      
      console.error(`[QueueProcessor] Job error after ${duration}ms:`, error);
      
      // Mark job as failed
      await this.updateSyncRunStatus({
        p_run_id: job.run_id,
        p_status: 'failed',
        p_error_message: errorMessage,
        p_error_details: {
          duration_ms: duration,
          stack: error instanceof Error ? error.stack : undefined,
        },
      });
      
      // Acknowledge to prevent infinite retries
      await this.acknowledgeJob(job.msg_id);
    }
  }
  
  /**
   * Update the status of a sync run
   */
  private async updateSyncRunStatus(params: UpdateSyncRunStatusParams): Promise<void> {
    try {
      const { error } = await this.supabase.rpc('update_sync_run_status', params);
      
      if (error) {
        console.error('[QueueProcessor] Error updating sync run status:', error);
      }
    } catch (error) {
      console.error('[QueueProcessor] Error in updateSyncRunStatus:', error);
    }
  }
  
  /**
   * Acknowledge a job (remove from queue)
   */
  private async acknowledgeJob(msgId: number): Promise<boolean> {
    try {
      const { data, error } = await this.supabase.rpc('acknowledge_sync_job', {
        p_msg_id: msgId,
      });
      
      if (error) {
        console.error('[QueueProcessor] Error acknowledging job:', error);
        return false;
      }
      
      return data === true;
      
    } catch (error) {
      console.error('[QueueProcessor] Error in acknowledgeJob:', error);
      return false;
    }
  }
  
  /**
   * Utility function to delay execution
   */
  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
