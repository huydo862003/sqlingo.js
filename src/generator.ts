import type { Expression } from './expressions';
import type { ParseOptions } from './parser';

export interface GeneratorOptions extends ParseOptions {
  pretty?: boolean;
  [key: string]: unknown;
}

export interface TranspileOptions extends ParseOptions {
  pretty?: boolean;
  [key: string]: unknown;
}

export class Generator {
  generate (_expression: Expression, _opts?: GeneratorOptions): string {
    throw new Error('Generator not implemented');
  }
}

export function generate (expression: Expression, opts?: GeneratorOptions): string {
  throw new Error(`generate not implemented: ${expression}, ${JSON.stringify(opts)}`);
}
