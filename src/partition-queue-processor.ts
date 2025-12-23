/**
 * Partition Queue Processor
 * 
 * Processes partition build jobs from the PGMQ queue.
 * Builds DuckDB databases with filtered data for user-scoped partitions.
 * 
 * Queue: partition_build_jobs
 */

import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { config } from './config.js';
import { PartitionBuildJob, PartitionBuildResult } from './types.js';
import { PartitionBuilder } from './partition-builder.js';

const QUEUE_NAME = 'partition_build_jobs';
const POLL_INTERVAL_MS = 5000;
const VIS_TIMEOUT_SECONDS = 600; // 10 minutes for large partitions

export class PartitionQueueProcessor {
  private supabase: SupabaseClient;
  private builder: PartitionBuilder;
  private isRunning: boolean = false;
  private pollTimeout: NodeJS.Timeout | null = null;
  
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
    
    this.builder = new PartitionBuilder(this.supabase);
  }
  
  /**
   * Start polling the queue
   */
  async start(): Promise<void> {
    console.log('[PartitionQueueProcessor] Starting...');
    console.log(`[PartitionQueueProcessor] Queue: ${QUEUE_NAME}`);
    console.log(`[PartitionQueueProcessor] Poll interval: ${POLL_INTERVAL_MS}ms`);
    
    this.isRunning = true;
    await this.poll();
  }
  
  /**
   * Stop polling the queue
   */
  stop(): void {
    console.log('[PartitionQueueProcessor] Stopping...');
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
      // Read message from queue using nova wrapper
      const { data: messages, error } = await this.supabase.rpc('nova_pgmq_read', {
        queue_name: QUEUE_NAME,
        visibility_timeout: VIS_TIMEOUT_SECONDS,
        quantity: 1,
      });
      
      if (error) {
        console.error('[PartitionQueueProcessor] Error reading from queue:', error);
      } else if (messages && messages.length > 0) {
        const message = messages[0];
        await this.processJob(message.msg_id, message.message);
      }
    } catch (error) {
      console.error('[PartitionQueueProcessor] Error in poll cycle:', error);
    }
    
    // Schedule next poll
    if (this.isRunning) {
      this.pollTimeout = setTimeout(() => this.poll(), POLL_INTERVAL_MS);
    }
  }
  
  /**
   * Process a single partition build job
   */
  private async processJob(msgId: number, payload: PartitionBuildJob): Promise<void> {
    const { partition_rule_id, partition_value, tenant_id } = payload;
    
    console.log('═══════════════════════════════════════════════');
    console.log(`[PartitionQueueProcessor] Processing job ${msgId}`);
    console.log(`[PartitionQueueProcessor] Rule ID: ${partition_rule_id}`);
    console.log(`[PartitionQueueProcessor] Partition Value: ${partition_value}`);
    console.log(`[PartitionQueueProcessor] Tenant: ${tenant_id}`);
    console.log('═══════════════════════════════════════════════');
    
    const startTime = Date.now();
    
    try {
      // Update job status to 'building'
      await this.updatePartitionDatabaseStatus(
        partition_rule_id,
        partition_value,
        tenant_id,
        'building'
      );
      
      // Execute partition build
      const result = await this.builder.build(payload);
      
      const duration = Date.now() - startTime;
      
      if (result.success) {
        console.log(`[PartitionQueueProcessor] ✓ Job completed in ${duration}ms`);
        console.log(`[PartitionQueueProcessor] Rows: ${result.totalRows}, Tables: ${result.tablesIncluded.length}`);
        
        // Update partition database record
        await this.updatePartitionDatabaseStatus(
          partition_rule_id,
          partition_value,
          tenant_id,
          'ready',
          null,
          result.totalRows,
          result.fileSize,
          result.databasePath
        );
        
        // Delete message from queue (acknowledge) using nova wrapper
        await this.supabase.rpc('nova_pgmq_delete', {
          queue_name: QUEUE_NAME,
          msg_id: msgId,
        });
      } else {
        console.error(`[PartitionQueueProcessor] ✗ Job failed in ${duration}ms:`, result.error);
        
        // Update status to 'failed'
        await this.updatePartitionDatabaseStatus(
          partition_rule_id,
          partition_value,
          tenant_id,
          'failed',
          result.error
        );
        
        // Delete the message (no archive wrapper available)
        await this.supabase.rpc('nova_pgmq_delete', {
          queue_name: QUEUE_NAME,
          msg_id: msgId,
        });
      }
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      
      console.error(`[PartitionQueueProcessor] Job ${msgId} failed with exception after ${duration}ms:`, errorMessage);
      
      // Update status to 'failed'
      await this.updatePartitionDatabaseStatus(
        partition_rule_id,
        partition_value,
        tenant_id,
        'failed',
        errorMessage
      );
      
      // Delete the message (no archive wrapper available)
      await this.supabase.rpc('nova_pgmq_delete', {
        queue_name: QUEUE_NAME,
        msg_id: msgId,
      });
    }
  }
  
  /**
   * Update partition database status
   */
  private async updatePartitionDatabaseStatus(
    ruleId: string,
    partitionValue: string,
    tenantId: string,
    status: string,
    error?: string | null,
    totalRows?: number,
    fileSize?: number,
    storagePath?: string | null
  ): Promise<void> {
    try {
      const updates: Record<string, unknown> = {
        status,
        updated_at: new Date().toISOString(),
      };
      
      if (status === 'building') {
        updates.build_started_at = new Date().toISOString();
      }
      
      if (status === 'ready') {
        updates.build_completed_at = new Date().toISOString();
        updates.last_built_at = new Date().toISOString();
        if (totalRows !== undefined) updates.total_rows = totalRows;
        if (fileSize !== undefined) updates.file_size_bytes = fileSize;
        if (storagePath) updates.storage_path = storagePath;
      }
      
      if (error) {
        updates.error_message = error;
      }
      
      const { error: updateError } = await this.supabase
        .from('partition_databases')
        .update(updates)
        .eq('partition_rule_id', ruleId)
        .eq('partition_value', partitionValue)
        .eq('tenant_id', tenantId);
      
      if (updateError) {
        console.error('[PartitionQueueProcessor] Error updating partition database status:', updateError);
      }
    } catch (error) {
      console.error('[PartitionQueueProcessor] Error in updatePartitionDatabaseStatus:', error);
    }
  }
}
