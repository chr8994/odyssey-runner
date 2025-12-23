/**
 * Outbound Runner - Entry point for outbound data stream synchronization
 * 
 * Processes jobs from the stream_sync_jobs_outbound PGMQ queue.
 * Handles pushing data from models to external targets (MySQL, SFTP, etc.)
 * 
 * Run with: yarn run outbound
 * Run with custom port: yarn run outbound -- --port 3002
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
const { OutboundQueueProcessor } = await import('../outbound-processor.js');

/**
 * Outbound Runner - Reverse ETL synchronization
 */
class OutboundRunner extends BaseRunner {
  getServiceName(): string {
    return 'Odyssey Outbound Runner';
  }
  
  createQueueProcessor() {
    return new OutboundQueueProcessor();
  }
}

// Start the runner
const runner = new OutboundRunner();
runner.start();
