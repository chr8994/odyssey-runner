/**
 * File Processor - Handles files stored in Supabase Storage
 * 
 * Supports downloading files from:
 * - sources bucket (Parquet files)
 * - schema-files bucket (CSV, JSON, Excel files)
 */

import { SupabaseClient } from '@supabase/supabase-js';
import * as fs from 'fs';
import { BaseProcessor } from './base-processor.js';
import { ProcessorContext, SourceDataResult } from '../types.js';

export class FileProcessor extends BaseProcessor {
  constructor(supabase: SupabaseClient) {
    super(supabase);
  }
  
  getTypeName(): string {
    return 'File';
  }
  
  /**
   * Download source file from Supabase Storage
   */
  async downloadSourceData(context: ProcessorContext): Promise<SourceDataResult> {
    const { dataSource } = context;
    const config = dataSource.config;
    
    // Determine source file path and bucket
    let sourceFilePath: string | null = null;
    let sourceBucket = 'sources';
    
    if (config?.parquetFilePath) {
      sourceFilePath = config.parquetFilePath;
      sourceBucket = 'sources';
    } else if (config?.storedFilePath) {
      sourceFilePath = config.storedFilePath;
      sourceBucket = 'schema-files';
    } else if (dataSource.type === 'file' && config?.filePath) {
      sourceFilePath = config.filePath;
      sourceBucket = 'schema-files';
    }
    
    if (!sourceFilePath) {
      throw new Error('No source file configured for this data source');
    }
    
    console.log(`[FileProcessor] Downloading from ${sourceBucket}/${sourceFilePath}`);
    
    // Download file from Supabase Storage
    const { data: fileData, error: downloadError } = await this.supabase
      .storage
      .from(sourceBucket)
      .download(sourceFilePath);
    
    if (downloadError || !fileData) {
      throw new Error(`Failed to download source file: ${downloadError?.message}`);
    }
    
    // Write to temp file
    const fileName = sourceFilePath.split('/').pop() || 'file';
    const tempFilePath = this.getTempFilePath('file', fileName);
    const buffer = Buffer.from(await fileData.arrayBuffer());
    fs.writeFileSync(tempFilePath, buffer);
    
    return {
      tempFilePath,
      fileName,
      fileSize: buffer.length,
    };
  }
}
