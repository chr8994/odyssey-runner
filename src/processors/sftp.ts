/**
 * SFTP Processor - Downloads files directly from SFTP servers
 * 
 * Features:
 * - Direct SFTP connection (no pre-download required)
 * - Jump host/bastion support via environment variables
 * - Vault integration for credentials
 */

import { SupabaseClient } from '@supabase/supabase-js';
import SftpClient from 'ssh2-sftp-client';
import { Client as SSHClient } from 'ssh2';
import * as fs from 'fs';
import { BaseProcessor } from './base-processor.js';
import { ProcessorContext, SourceDataResult, SftpCredentials } from '../types.js';

/**
 * File selection mode - determines how files are selected
 */
type FileSelectionMode = 'specific_file' | 'folder_most_recent';

/**
 * Selection strategy for folder mode - how to determine "most recent"
 */
type SelectionStrategy = 'most_recent_by_date' | 'most_recent_by_name';

/**
 * SFTP connection configuration
 */
interface SftpConfig {
  host: string;
  port: number;
  username: string;
  password?: string;
  privateKey?: string;
  remotePath: string;
  filePattern?: string;
  selectedFile?: string;
  /** Selected folder for folder_most_recent mode */
  selectedFolder?: string;
  /** File selection mode: specific_file or folder_most_recent */
  fileSelectionMode?: FileSelectionMode;
  /** Selection strategy when in folder mode */
  selectionStrategy?: SelectionStrategy;
}

/**
 * Jump host configuration
 */
interface JumpHostConfig {
  enabled: boolean;
  host: string;
  port: number;
  username: string;
  password?: string;
  privateKey?: string;
}

/**
 * SFTP connection result
 */
interface SftpConnectionResult {
  sftp: SftpClient;
  jumpClient?: SSHClient;
}

export class SftpProcessor extends BaseProcessor {
  constructor(supabase: SupabaseClient) {
    super(supabase);
  }
  
  getTypeName(): string {
    return 'SFTP';
  }
  
  /**
   * Download source file directly from SFTP server
   * 
   * Supports two file selection modes:
   * 1. specific_file: Use the exact file path stored in selectedFile
   * 2. folder_most_recent: Scan folder and select most recent file based on strategy
   */
  async downloadSourceData(context: ProcessorContext): Promise<SourceDataResult> {
    const { dataSource } = context;
    const config = dataSource.config;
    
    // Build SFTP config from data source
    const sftpConfig: SftpConfig = {
      host: config.host as string,
      port: (config.port as number) || 22,
      username: config.username as string,
      remotePath: config.remotePath as string,
      filePattern: config.filePattern as string | undefined,
      selectedFile: config.selectedFile as string | undefined,
      selectedFolder: config.selectedFolder as string | undefined,
      fileSelectionMode: (config.fileSelectionMode as FileSelectionMode) || 'specific_file',
      selectionStrategy: (config.selectionStrategy as SelectionStrategy) || 'most_recent_by_date',
    };
    
    if (!sftpConfig.host || !sftpConfig.username) {
      throw new Error('SFTP configuration incomplete: host and username required');
    }
    
    // Get credentials from vault if configured
    // vault_secret_id is at dataSource level, not inside config
    const credentials = await this.getCredentials(dataSource.vault_secret_id as string | undefined);
    
    // Determine which file to download based on selection mode
    let fullRemotePath: string;
    
    if (sftpConfig.fileSelectionMode === 'folder_most_recent') {
      // Folder mode: scan folder and find most recent file
      // Use selectedFolder (user's folder choice) over remotePath (initial connection path)
      const folderPath = sftpConfig.selectedFolder || sftpConfig.remotePath;
      
      console.log(`[SFTPProcessor] Mode: folder_most_recent (strategy: ${sftpConfig.selectionStrategy})`);
      console.log(`[SFTPProcessor] Target folder: ${folderPath} (selectedFolder: ${sftpConfig.selectedFolder}, remotePath: ${sftpConfig.remotePath})`);
      
      if (!folderPath) {
        throw new Error('SFTP configuration incomplete: selectedFolder or remotePath required for folder mode');
      }
      
      // Create a modified config with the correct folder path for file listing
      const folderConfig: SftpConfig = { ...sftpConfig, remotePath: folderPath };
      const targetFileName = await this.findMatchingFile(folderConfig, credentials);
      
      if (!targetFileName) {
        throw new Error(`No matching files found in folder: ${folderPath}`);
      }
      
      fullRemotePath = `${folderPath}/${targetFileName}`.replace(/\/\//g, '/');
      console.log(`[SFTPProcessor] Selected file via ${sftpConfig.selectionStrategy}: ${targetFileName}`);
      
    } else {
      // Specific file mode: use the selectedFile directly
      console.log(`[SFTPProcessor] Mode: specific_file`);
      
      if (!sftpConfig.selectedFile) {
        throw new Error('No file specified for SFTP source');
      }
      
      // Check if selectedFile is an absolute path (starts with /)
      if (sftpConfig.selectedFile.startsWith('/')) {
        // Use as-is, it's already a full path
        fullRemotePath = sftpConfig.selectedFile;
      } else {
        // Combine remotePath + selectedFile
        fullRemotePath = `${sftpConfig.remotePath}/${sftpConfig.selectedFile}`.replace(/\/\//g, '/');
      }
    }
    
    console.log(`[SFTPProcessor] Full remote path: ${fullRemotePath}`);
    
    // Connect and download
    let connection: SftpConnectionResult | null = null;
    
    try {
      console.log(`[SFTPProcessor] Connecting to ${sftpConfig.host}:${sftpConfig.port}`);
      connection = await this.connectSftp(sftpConfig, credentials);
      console.log(`[SFTPProcessor] Connected successfully`);
      
      // Download file to buffer
      console.log(`[SFTPProcessor] Downloading ${fullRemotePath}`);
      const result = await connection.sftp.get(fullRemotePath);
      const buffer = Buffer.isBuffer(result) ? result : Buffer.from(result as string);
      
      // Write to temp file
      const fileName = fullRemotePath.split('/').pop() || 'file';
      const tempFilePath = this.getTempFilePath('sftp', fileName);
      fs.writeFileSync(tempFilePath, buffer);
      
      console.log(`[SFTPProcessor] Downloaded ${buffer.length} bytes to ${tempFilePath}`);
      
      return {
        tempFilePath,
        fileName,
        fileSize: buffer.length,
      };
      
    } finally {
      // Cleanup connection
      if (connection) {
        try {
          await connection.sftp.end();
        } catch {
          // Ignore close errors
        }
        connection.jumpClient?.end();
      }
    }
  }
  
  /**
   * Get credentials from vault using nova_get_secret_by_id RPC function
   */
  private async getCredentials(vaultSecretId: string | undefined): Promise<SftpCredentials> {
    if (!vaultSecretId) {
      // Check for direct credentials in environment (for testing)
      console.log(`[SFTPProcessor] No vault_secret_id, checking environment variables`);
      return {
        password: process.env.SFTP_DEFAULT_PASSWORD,
        privateKey: process.env.SFTP_DEFAULT_PRIVATE_KEY,
      };
    }
    
    try {
      console.log(`[SFTPProcessor] Fetching credentials from vault by ID: ${vaultSecretId}`);
      
      // Fetch secret from vault using nova_get_secret_by_id (takes UUID)
      const { data, error } = await this.supabase
        .rpc('nova_get_secret_by_id', { secret_id: vaultSecretId });
      
      if (error) {
        console.warn(`[SFTPProcessor] Vault RPC error: ${error.message}`);
        return {};
      }
      
      if (!data || data.length === 0) {
        console.warn(`[SFTPProcessor] Secret not found with ID: ${vaultSecretId}`);
        return {};
      }
      
      // The RPC returns an array with decrypted_value column
      const decryptedValue = data[0]?.decrypted_value;
      
      if (!decryptedValue) {
        console.warn(`[SFTPProcessor] Secret has no decrypted value`);
        return {};
      }
      
      // Parse secret value (expected to be JSON with password or privateKey)
      const secret = typeof decryptedValue === 'string' ? JSON.parse(decryptedValue) : decryptedValue;
      
      console.log(`[SFTPProcessor] Credentials loaded from vault successfully`);
      
      return {
        password: secret.password,
        privateKey: secret.privateKey,
      };
    } catch (error) {
      console.warn(`[SFTPProcessor] Error fetching credentials:`, error);
      return {};
    }
  }
  
  /**
   * Find matching file based on config pattern and selection strategy
   * Supports both 'most_recent_by_date' and 'most_recent_by_name' strategies
   */
  private async findMatchingFile(
    config: SftpConfig,
    credentials: SftpCredentials
  ): Promise<string | undefined> {
    let connection: SftpConnectionResult | null = null;
    
    try {
      connection = await this.connectSftp(config, credentials);
      
      console.log(`[SFTPProcessor] Listing files in: ${config.remotePath}`);
      const listing = await connection.sftp.list(config.remotePath);
      
      // Filter to only regular files (not directories or symlinks)
      let files = listing.filter(item => item.type === '-');
      console.log(`[SFTPProcessor] Found ${files.length} files in folder`);
      
      // Apply file pattern filter if specified
      if (config.filePattern) {
        const pattern = new RegExp(
          config.filePattern.replace(/\./g, '\\.').replace(/\*/g, '.*').replace(/\?/g, '.')
        );
        files = files.filter(f => pattern.test(f.name));
        console.log(`[SFTPProcessor] After pattern filter '${config.filePattern}': ${files.length} files`);
      }
      
      if (files.length === 0) {
        console.log(`[SFTPProcessor] No matching files found`);
        return undefined;
      }
      
      // Sort based on selection strategy
      const strategy = config.selectionStrategy || 'most_recent_by_date';
      
      if (strategy === 'most_recent_by_name') {
        // Sort alphabetically descending (Z-A, or highest number first)
        files.sort((a, b) => b.name.localeCompare(a.name));
        console.log(`[SFTPProcessor] Sorted by name (descending), top files: ${files.slice(0, 3).map(f => f.name).join(', ')}`);
      } else {
        // Default: sort by modify time descending (latest first)
        files.sort((a, b) => b.modifyTime - a.modifyTime);
        console.log(`[SFTPProcessor] Sorted by date (descending), top files: ${files.slice(0, 3).map(f => `${f.name} (${new Date(f.modifyTime * 1000).toISOString()})`).join(', ')}`);
      }
      
      return files[0].name;
      
    } finally {
      if (connection) {
        try {
          await connection.sftp.end();
        } catch {
          // Ignore close errors
        }
        connection.jumpClient?.end();
      }
    }
  }
  
  /**
   * Get jump host configuration from environment
   */
  private getJumpHostConfig(): JumpHostConfig {
    return {
      enabled: process.env.SFTP_JUMP_HOST_ENABLED === 'true',
      host: process.env.SFTP_JUMP_HOST || '',
      port: parseInt(process.env.SFTP_JUMP_HOST_PORT || '22', 10),
      username: process.env.SFTP_JUMP_HOST_USERNAME || '',
      password: process.env.SFTP_JUMP_HOST_PASSWORD,
      privateKey: process.env.SFTP_JUMP_HOST_PRIVATE_KEY,
    };
  }
  
  /**
   * Connect to SFTP (handles both direct and jump host connections)
   */
  private async connectSftp(
    config: SftpConfig,
    credentials: SftpCredentials
  ): Promise<SftpConnectionResult> {
    const sftp = new SftpClient();
    const jumpHostConfig = this.getJumpHostConfig();
    
    const fullConfig: SftpConfig = {
      ...config,
      password: credentials.password,
      privateKey: credentials.privateKey,
    };
    
    if (jumpHostConfig.enabled && jumpHostConfig.host) {
      const jumpClient = await this.connectSftpViaJumpHost(sftp, jumpHostConfig, fullConfig);
      return { sftp, jumpClient };
    } else {
      await this.connectSftpDirect(sftp, fullConfig);
      return { sftp };
    }
  }
  
  /**
   * Connect to SFTP directly
   */
  private async connectSftpDirect(sftp: SftpClient, config: SftpConfig): Promise<void> {
    const connectOptions: SftpClient.ConnectOptions = {
      host: config.host,
      port: config.port,
      username: config.username,
      readyTimeout: parseInt(process.env.SFTP_READY_TIMEOUT || '30000', 10),
    };
    
    if (config.privateKey) {
      connectOptions.privateKey = config.privateKey;
    } else if (config.password) {
      connectOptions.password = config.password;
    }
    
    await sftp.connect(connectOptions);
  }
  
  /**
   * Connect to SFTP via jump host (bastion)
   */
  private async connectSftpViaJumpHost(
    sftp: SftpClient,
    jumpHost: JumpHostConfig,
    destination: SftpConfig
  ): Promise<SSHClient> {
    return new Promise((resolve, reject) => {
      const jumpClient = new SSHClient();
      
      jumpClient.on('ready', () => {
        console.log(`[SFTPProcessor] Connected to jump host: ${jumpHost.host}`);
        
        jumpClient.forwardOut(
          '127.0.0.1',
          12345,
          destination.host,
          destination.port,
          async (err, stream) => {
            if (err) {
              jumpClient.end();
              return reject(new Error(`Port forwarding failed: ${err.message}`));
            }
            
            try {
              const connectOptions: SftpClient.ConnectOptions = {
                host: destination.host,
                port: destination.port,
                username: destination.username,
                sock: stream,
                readyTimeout: parseInt(process.env.SFTP_READY_TIMEOUT || '30000', 10),
              };
              
              if (destination.privateKey) {
                connectOptions.privateKey = destination.privateKey;
              } else if (destination.password) {
                connectOptions.password = destination.password;
              }
              
              await sftp.connect(connectOptions);
              console.log(`[SFTPProcessor] Connected to SFTP via jump host`);
              resolve(jumpClient);
            } catch (error) {
              jumpClient.end();
              reject(error);
            }
          }
        );
      });
      
      jumpClient.on('error', (err) => {
        reject(new Error(`Jump host connection failed: ${err.message}`));
      });
      
      const jumpConnectOptions: Parameters<typeof jumpClient.connect>[0] = {
        host: jumpHost.host,
        port: jumpHost.port,
        username: jumpHost.username,
        readyTimeout: parseInt(process.env.SFTP_CONNECTION_TIMEOUT || '30000', 10),
      };
      
      if (jumpHost.privateKey) {
        jumpConnectOptions.privateKey = jumpHost.privateKey;
      } else if (jumpHost.password) {
        jumpConnectOptions.password = jumpHost.password;
      }
      
      jumpClient.connect(jumpConnectOptions);
    });
  }
}
