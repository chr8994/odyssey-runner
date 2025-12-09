/**
 * Processor Registry - Factory for source processors
 * 
 * Routes to the correct processor based on data source type
 */

import { SupabaseClient } from '@supabase/supabase-js';
import { BaseProcessor } from './base-processor.js';
import { FileProcessor } from './file.js';
import { SftpProcessor } from './sftp.js';
import { MySqlProcessor } from './mysql.js';
import { DataSourceType } from '../types.js';

/**
 * Registry of available processors
 */
export const PROCESSOR_TYPES = {
  file: FileProcessor,
  sftp: SftpProcessor,
  mysql: MySqlProcessor,
  // Future processors:
  // postgres: PostgresProcessor,
  // api: ApiProcessor,
} as const;

/**
 * Get a processor instance for a given data source type
 * 
 * @param sourceType - The type of data source (file, sftp, postgres, etc.)
 * @param supabase - Supabase client instance
 * @returns Processor instance or null if type not supported
 */
export function getProcessor(
  sourceType: string,
  supabase: SupabaseClient
): BaseProcessor | null {
  // Normalize type
  const normalizedType = sourceType.toLowerCase() as DataSourceType;
  
  // Get processor class
  const ProcessorClass = PROCESSOR_TYPES[normalizedType as keyof typeof PROCESSOR_TYPES];
  
  if (!ProcessorClass) {
    console.warn(`[ProcessorRegistry] No processor found for type: ${sourceType}`);
    return null;
  }
  
  return new ProcessorClass(supabase);
}

/**
 * Check if a processor exists for a given source type
 */
export function hasProcessor(sourceType: string): boolean {
  const normalizedType = sourceType.toLowerCase();
  return normalizedType in PROCESSOR_TYPES;
}

/**
 * Get list of supported source types
 */
export function getSupportedTypes(): string[] {
  return Object.keys(PROCESSOR_TYPES);
}

// Re-export processors
export { BaseProcessor } from './base-processor.js';
export { FileProcessor } from './file.js';
export { SftpProcessor } from './sftp.js';
export { MySqlProcessor } from './mysql.js';
export { DerivedModelProcessor } from './derived-model.js';
export { OutboundProcessor } from './outbound.js';
