/**
 * Base Runner - Shared functionality for all Odyssey Runner services
 * 
 * Provides common infrastructure:
 * - Health check server
 * - Graceful shutdown handling
 * - Configuration validation
 * - Signal handling
 */

import http from 'http';
import { config, validateConfig } from '../config.js';

/**
 * Interface for queue processors that can be started/stopped
 */
export interface QueueProcessorInterface {
  start(): Promise<void>;
  stop(): void | Promise<void>;
}

/**
 * Abstract base class for all runner services
 */
export abstract class BaseRunner {
  protected healthServer: http.Server | null = null;
  protected isShuttingDown = false;
  protected queueProcessor: QueueProcessorInterface | null = null;
  
  /**
   * Get the service name for logging
   */
  abstract getServiceName(): string;
  
  /**
   * Create the queue processor for this runner
   */
  abstract createQueueProcessor(): QueueProcessorInterface;
  
  /**
   * Main entry point - starts the runner service
   */
  async start(): Promise<void> {
    try {
      console.log('═══════════════════════════════════════════════');
      console.log(`[Service] Starting ${this.getServiceName()}...`);
      console.log('═══════════════════════════════════════════════');
      
      // Validate configuration
      validateConfig();
      
      // Start health check server
      if (config.healthCheck.enabled) {
        this.startHealthServer();
      }
      
      // Create and start the queue processor
      this.queueProcessor = this.createQueueProcessor();
      await this.queueProcessor.start();
      
      console.log(`[Service] ${this.getServiceName()} started successfully`);
      console.log('[Service] Waiting for jobs...');
      
      // Register shutdown handlers
      this.registerShutdownHandlers();
      
    } catch (error) {
      console.error('[Service] Failed to start:', error);
      process.exit(1);
    }
  }
  
  /**
   * Start HTTP health check server
   */
  protected startHealthServer(): void {
    const serviceName = this.getServiceName().toLowerCase().replace(/\s+/g, '-');
    
    this.healthServer = http.createServer((req, res) => {
      if (req.url === '/health' || req.url === '/healthz') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          status: 'healthy',
          service: serviceName,
          timestamp: new Date().toISOString(),
          uptime: process.uptime(),
        }));
      } else if (req.url === '/ready' || req.url === '/readyz') {
        // Ready check - can include queue connectivity check
        const isReady = this.queueProcessor !== null && !this.isShuttingDown;
        res.writeHead(isReady ? 200 : 503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          ready: isReady,
          service: serviceName,
          timestamp: new Date().toISOString(),
        }));
      } else {
        res.writeHead(404);
        res.end('Not Found');
      }
    });
    
    this.healthServer.listen(config.healthCheck.port, () => {
      console.log(`[HealthCheck] Server listening on port ${config.healthCheck.port}`);
      console.log(`[HealthCheck] Endpoints: /health, /healthz, /ready, /readyz`);
    });
  }
  
  /**
   * Register signal handlers for graceful shutdown
   */
  protected registerShutdownHandlers(): void {
    process.on('SIGTERM', () => this.shutdown('SIGTERM'));
    process.on('SIGINT', () => this.shutdown('SIGINT'));
    
    // Handle uncaught errors
    process.on('uncaughtException', (error) => {
      console.error('[Service] Uncaught exception:', error);
      this.shutdown('uncaughtException').catch(() => process.exit(1));
    });
    
    process.on('unhandledRejection', (reason, promise) => {
      console.error('[Service] Unhandled rejection at:', promise, 'reason:', reason);
      this.shutdown('unhandledRejection').catch(() => process.exit(1));
    });
  }
  
  /**
   * Graceful shutdown handler
   */
  protected async shutdown(signal: string): Promise<void> {
    if (this.isShuttingDown) {
      console.log('[Service] Shutdown already in progress...');
      return;
    }
    
    this.isShuttingDown = true;
    console.log(`\n[Service] Received ${signal}, shutting down gracefully...`);
    
    // Stop queue processor
    if (this.queueProcessor) {
      console.log('[Service] Stopping queue processor...');
      await this.queueProcessor.stop();
    }
    
    // Stop health server
    if (this.healthServer) {
      console.log('[Service] Stopping health check server...');
      await new Promise<void>((resolve) => {
        this.healthServer!.close(() => resolve());
      });
    }
    
    console.log('[Service] Shutdown complete');
    process.exit(0);
  }
}
