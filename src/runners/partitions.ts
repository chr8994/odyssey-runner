/**
 * Partitions Runner - Entry point for user-scoped database partition builds
 * 
 * Processes jobs from the partition_build_jobs PGMQ queue.
 * Builds DuckDB databases containing filtered data for specific partition values.
 * 
 * Run with: yarn run partitions
 * Run with custom port: yarn run partitions -- --port 3003
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
const { PartitionQueueProcessor } = await import('../partition-queue-processor.js');

/**
 * Partitions Runner - User-scoped database generation
 */
class PartitionsRunner extends BaseRunner {
  getServiceName(): string {
    return 'Odyssey Partitions Runner';
  }
  
  createQueueProcessor() {
    return new PartitionQueueProcessor();
  }
}

// Start the runner
const runner = new PartitionsRunner();
runner.start();
