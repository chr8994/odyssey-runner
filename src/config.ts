/**
 * Configuration for the Stream Runner Service
 * Loads and validates environment variables
 */

export interface Config {
  /**
   * Node environment
   */
  nodeEnv: string;
  
  /**
   * Supabase configuration
   */
  supabase: {
    url: string;
    serviceKey: string;
  };
  
  /**
   * Queue processing configuration
   */
  queue: {
    /**
     * Polling interval in milliseconds when queue is empty
     */
    pollIntervalMs: number;
    
    /**
     * Number of jobs to fetch per batch
     */
    batchSize: number;
    
    /**
     * Visibility timeout in seconds - how long a job is hidden after being read
     * If not acknowledged within this time, it becomes visible again
     */
    visibilityTimeoutSeconds: number;
    
    /**
     * Maximum number of retry attempts before marking as failed
     */
    maxRetries: number;
  };
  
  /**
   * Health check server configuration
   */
  healthCheck: {
    enabled: boolean;
    port: number;
  };
  
  /**
   * Logging level
   */
  logLevel: string;
}

/**
 * Load configuration from environment variables
 */
function loadConfig(): Config {
  return {
    nodeEnv: process.env.NODE_ENV || 'development',
    
    supabase: {
      url: process.env.SUPABASE_URL || '',
      serviceKey: process.env.SUPABASE_SERVICE_ROLE_KEY || '',
    },
    
    queue: {
      pollIntervalMs: parseInt(process.env.POLL_INTERVAL_MS || '5000', 10),
      batchSize: parseInt(process.env.BATCH_SIZE || '5', 10),
      visibilityTimeoutSeconds: parseInt(process.env.VISIBILITY_TIMEOUT_SECONDS || '300', 10),
      maxRetries: parseInt(process.env.MAX_RETRIES || '3', 10),
    },
    
    healthCheck: {
      enabled: process.env.HEALTH_CHECK_ENABLED !== 'false',
      port: parseInt(process.env.HEALTH_CHECK_PORT || '8080', 10),
    },
    
    logLevel: process.env.LOG_LEVEL || 'info',
  };
}

/**
 * Validate that required configuration values are present
 * @throws Error if required config is missing
 */
export function validateConfig(): void {
  const missingVars: string[] = [];
  
  if (!config.supabase.url) {
    missingVars.push('SUPABASE_URL');
  }
  
  if (!config.supabase.serviceKey) {
    missingVars.push('SUPABASE_SERVICE_ROLE_KEY');
  }
  
  if (missingVars.length > 0) {
    throw new Error(
      `Missing required environment variables: ${missingVars.join(', ')}\n` +
      'Please check your .env file or environment configuration.'
    );
  }
  
  // Validate numeric values
  if (config.queue.pollIntervalMs < 100) {
    console.warn('[Config] POLL_INTERVAL_MS is very low (<100ms), this may cause high CPU usage');
  }
  
  if (config.queue.batchSize < 1 || config.queue.batchSize > 100) {
    console.warn('[Config] BATCH_SIZE should be between 1 and 100');
  }
  
  if (config.queue.visibilityTimeoutSeconds < 30) {
    console.warn('[Config] VISIBILITY_TIMEOUT_SECONDS is very low (<30s), jobs may be retried prematurely');
  }
  
  console.log('[Config] Configuration validated successfully');
  console.log(`[Config] Environment: ${config.nodeEnv}`);
  console.log(`[Config] Poll interval: ${config.queue.pollIntervalMs}ms`);
  console.log(`[Config] Batch size: ${config.queue.batchSize}`);
  console.log(`[Config] Visibility timeout: ${config.queue.visibilityTimeoutSeconds}s`);
  console.log(`[Config] Max retries: ${config.queue.maxRetries}`);
  console.log(`[Config] Health check: ${config.healthCheck.enabled ? `enabled on port ${config.healthCheck.port}` : 'disabled'}`);
}

// Export singleton config instance
export const config = loadConfig();
