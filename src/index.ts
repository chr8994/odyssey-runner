/**
 * Odyssey Runner Service - Entry Point
 * 
 * Background worker service for processing data stream synchronization jobs.
 * Listens to the PGMQ stream_sync_jobs queue and processes sync requests.
 */

// Load environment variables from .env file FIRST before any other imports
import 'dotenv/config';

import http from 'http';
import { config, validateConfig } from './config.js';
import { QueueProcessor } from './queue-processor.js';

// Global state
let queueProcessor: QueueProcessor | null = null;
let healthServer: http.Server | null = null;
let isShuttingDown = false;

/**
 * Main entry point
 */
async function main() {
  try {
    console.log('═══════════════════════════════════════════════');
    console.log('[Service] Starting Odyssey Runner Service...');
    console.log('═══════════════════════════════════════════════');
    
    // Validate configuration
    validateConfig();
    
    // Start health check server
    if (config.healthCheck.enabled) {
      startHealthServer();
    }
    
    // Create and start the queue processor
    queueProcessor = new QueueProcessor();
    await queueProcessor.start();
    
    console.log('[Service] Odyssey Runner Service started successfully');
    console.log('[Service] Waiting for sync jobs...');
    
  } catch (error) {
    console.error('[Service] Failed to start:', error);
    process.exit(1);
  }
}

/**
 * Start HTTP health check server
 */
function startHealthServer(): void {
  healthServer = http.createServer((req, res) => {
    if (req.url === '/health' || req.url === '/healthz') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'healthy',
        service: 'odyssey-runner',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
      }));
    } else if (req.url === '/ready' || req.url === '/readyz') {
      // Ready check - can include queue connectivity check
      const isReady = queueProcessor !== null && !isShuttingDown;
      res.writeHead(isReady ? 200 : 503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        ready: isReady,
        service: 'odyssey-runner',
        timestamp: new Date().toISOString(),
      }));
    } else {
      res.writeHead(404);
      res.end('Not Found');
    }
  });
  
  healthServer.listen(config.healthCheck.port, () => {
    console.log(`[HealthCheck] Server listening on port ${config.healthCheck.port}`);
    console.log(`[HealthCheck] Endpoints: /health, /healthz, /ready, /readyz`);
  });
}

/**
 * Graceful shutdown handler
 */
async function shutdown(signal: string): Promise<void> {
  if (isShuttingDown) {
    console.log('[Service] Shutdown already in progress...');
    return;
  }
  
  isShuttingDown = true;
  console.log(`\n[Service] Received ${signal}, shutting down gracefully...`);
  
  // Stop queue processor
  if (queueProcessor) {
    console.log('[Service] Stopping queue processor...');
    await queueProcessor.stop();
  }
  
  // Stop health server
  if (healthServer) {
    console.log('[Service] Stopping health check server...');
    await new Promise<void>((resolve) => {
      healthServer!.close(() => resolve());
    });
  }
  
  console.log('[Service] Shutdown complete');
  process.exit(0);
}

// Register shutdown handlers
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// Handle uncaught errors
process.on('uncaughtException', (error) => {
  console.error('[Service] Uncaught exception:', error);
  shutdown('uncaughtException').catch(() => process.exit(1));
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('[Service] Unhandled rejection at:', promise, 'reason:', reason);
  shutdown('unhandledRejection').catch(() => process.exit(1));
});

// Start the service
main();
