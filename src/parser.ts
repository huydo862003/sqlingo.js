import type { Dialects } from './dialects';
import type { ErrorLevel } from './errors';
import type { Expression } from './expressions';
import type { Token } from './tokens';

export interface ParseOptions {
  errorLevel?: ErrorLevel;
  [key: string]: unknown;
}

export class Parser {
  parse (_sql: string, _opts?: ParseOptions): Array<Expression | null> {
    throw new Error('Parser not implemented');
  }

  parseInto (_into: unknown, _sql: string, _opts?: ParseOptions): Array<Expression | null> {
    throw new Error('Parser.parseInto not implemented');
  }
}

export function parse (
  _sql: string | Token[],
  _dialect?: Dialects,
  _read?: Dialects,
  _opts?: ParseOptions,
): Array<Expression | null> {
  throw new Error('parse not implemented');
}
