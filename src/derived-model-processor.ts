/**
 * Derived Model Job Processor - Orchestrates derived model refresh jobs
 * 
 * This orchestrator:
 * 1. Fetches derived model configuration from database
 * 2. Calls DerivedModelProcessor to perform the refresh
 * 3. Handles success/failure updates
 * 4. Triggers cascade refresh for dependent derived models
 */

import { SupabaseClient } from '@supabase/supabase-js';
import { DerivedModelProcessor } from './processors/derived-model.js';
import {
  DerivedModelJobPayload,
  DerivedModelContext,
  DerivedModelRefreshResult,
  DataModel,
} from './types.js';

export class DerivedModelJobProcessor {
  private supabase: SupabaseClient;
  private processor: DerivedModelProcessor;
  
  constructor(supabase: SupabaseClient) {
    this.supabase = supabase;
    this.processor = new DerivedModelProcessor(supabase);
  }
  
  /**
   * Process a derived model refresh job
   */
  async process(job: DerivedModelJobPayload): Promise<DerivedModelRefreshResult> {
    const startTime = Date.now();
    const logPrefix = '[DerivedModelJob]';
    
    console.log(`${logPrefix} Processing model: ${job.model_id}`);
    console.log(`${logPrefix} Tenant: ${job.tenant_id}`);
    console.log(`${logPrefix} Triggered by: ${job.triggered_by || 'manual'} (${job.trigger_type || 'manual'})`);
    
    try {
      // Step 1: Fetch derived model from database
      const { data: dataModel, error: fetchError } = await this.supabase
        .from('data_models')
        .select('*')
        .eq('id', job.model_id)
        .single();
      
      if (fetchError || !dataModel) {
        throw new Error(`Model not found: ${job.model_id}`);
      }
      
      if (dataModel.model_kind !== 'derived') {
        throw new Error('Only derived models can be refreshed via this processor');
      }
      
      console.log(`${logPrefix} Loaded derived model: ${dataModel.name}`);
      
      // Step 2: Update model status to processing
      await this.updateModelStatus(job.model_id, 'processing', null);
      
      // Step 3: Build context and process
      const context: DerivedModelContext = {
        modelId: job.model_id,
        tenantId: job.tenant_id,
        dataModel: dataModel as unknown as DataModel,
      };
      
      const result = await this.processor.process(context);
      
      const duration = Date.now() - startTime;
      console.log(`${logPrefix} Refresh ${result.success ? 'completed' : 'failed'} in ${duration}ms`);
      
      // Step 4: If successful, trigger cascade for dependent models
      if (result.success) {
        await this.triggerCascadeRefresh(job.model_id, job.tenant_id);
      }
      
      return result;
      
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      
      console.error(`${logPrefix} Failed after ${duration}ms:`, errorMessage);
      
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
   * Update model refresh status
   */
  private async updateModelStatus(
    modelId: string,
    status: string,
    error: string | null
  ): Promise<void> {
    const updateData: any = {
      last_refresh_status: status,
      last_refresh_at: new Date().toISOString(),
    };
    
    if (error) {
      updateData.last_refresh_error = error;
      updateData.status = 'error';
    }
    
    await this.supabase
      .from('data_models')
      .update(updateData)
      .eq('id', modelId);
  }
  
  /**
   * Find and queue dependent derived models for cascade refresh
   */
  private async triggerCascadeRefresh(modelId: string, tenantId: string): Promise<void> {
    const logPrefix = '[DerivedModelJob]';
    
    try {
      // Find derived models that depend on this model
      const { data: dependentModels, error } = await this.supabase
        .from('data_models')
        .select('id, name')
        .eq('model_kind', 'derived')
        .contains('source_models', [modelId]);
      
      if (error) {
        console.error(`${logPrefix} Error finding dependent models:`, error);
        return;
      }
      
      if (!dependentModels || dependentModels.length === 0) {
        console.log(`${logPrefix} No dependent derived models found`);
        return;
      }
      
      console.log(`${logPrefix} Triggering cascade for ${dependentModels.length} dependent model(s)`);
      
      // Queue each dependent model for refresh
      for (const derived of dependentModels) {
        try {
          await this.queueDerivedModelRefresh({
            model_id: derived.id,
            tenant_id: tenantId,
            triggered_by: modelId,
            trigger_type: 'cascade',
          });
          
          console.log(`${logPrefix} Queued cascade refresh for: ${derived.name} (${derived.id})`);
        } catch (queueError) {
          console.error(`${logPrefix} Failed to queue ${derived.name}:`, queueError);
          // Continue with other models even if one fails
        }
      }
    } catch (error) {
      console.error(`${logPrefix} Cascade refresh failed:`, error);
      // Don't throw - cascade failure shouldn't fail the main refresh
    }
  }
  
  /**
   * Queue a derived model refresh job to PGMQ
   */
  private async queueDerivedModelRefresh(job: DerivedModelJobPayload): Promise<void> {
    const { error } = await this.supabase.rpc('pgmq_send', {
      queue_name: 'stream_sync_jobs_derived',
      msg: JSON.stringify(job),
    });
    
    if (error) {
      throw new Error(`Failed to queue derived model refresh: ${error.message}`);
    }
  }
}
