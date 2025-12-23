/**
 * Sync Runner - Entry point for inbound data stream synchronization
 * 
 * Processes jobs from the stream_sync_jobs PGMQ queue.
 * Handles pulling data from sources (file, SFTP, MySQL, etc.) and
 * writing to Parquet files in Supabase storage.
 * 
 * Run with: yarn run sync
 * Run with custom port: yarn run sync -- --port 3001
 */

// Parse command line arguments BEFORE importing anything
const args = process.argv.slice(2);
const portIndex = args.indexOf('--port');
if (portIndex !== -1 && args[portIndex + 1]) {
  process.env.HEALTH_CHECK_PORT = args[portIndex + 1];
  console.log(`[Args] Using custom health check port: ${args[portIndex + 1]}`);
}

// Use dynamic imports to ensure args are parsed before config loads
const { BaseRunner } = await import('./base-runner.js');
const { QueueProcessor } = await import('../queue-processor.js');

/**
 * Sync Runner - Inbound data stream synchronization
 */
class SyncRunner extends BaseRunner {
  getServiceName(): string {
    return 'Odyssey Sync Runner';
  }
  
  createQueueProcessor() {
    return new QueueProcessor();
  }
}

// Start the runner
const runner = new SyncRunner();
runner.start();
