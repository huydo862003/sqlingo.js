/**
 * @hdnax/sqlglot.js
 * A JavaScript port of SQLGlot - SQL parser, transpiler, optimizer, and engine
 *
 * Based on SQLGlot by Toby Mao (https://github.com/tobymao/sqlglot)
 */

import packageJson from '../package.json';
import {
  ErrorLevel, SqlglotError, ParseError, TokenError, UnsupportedError,
} from './errors';
import type { Token } from './tokens';
import {
  TokenType, Tokenizer,
} from './tokens';
import type { ParseOptions } from './parser';
import {
  Parser, parse, parseOne,
} from './parser';
import type {
  GeneratorOptions, TranspileOptions,
} from './generator';
import {
  Generator, generate,
} from './generator';
import {
  Schema, MappingSchema,
} from './schema';
import type { Dialects } from './dialects/index';
import { diff } from './diff';
import { lineage } from './lineage';
import { optimize } from './optimizer/index';
import { execute } from './executor/index';

export {
  Expression,
  alias,
  and,
  case_,
  cast,
  column,
  condition,
  delete_,
  except,
  from,
  func,
  insert,
  intersect,
  maybeParse,
  merge,
  not,
  or,
  select,
  subquery,
  table,
  toColumn,
  toIdentifier,
  toTable,
  union,
  findTables,
} from './expressions';

export const version = packageJson.version;

export {
  ErrorLevel, SqlglotError, ParseError, TokenError, UnsupportedError,
};
export type { Token };
export {
  TokenType, Tokenizer,
};
export type { ParseOptions };
// NOTE: parse() and parseOne() are imported from parser.ts (not defined here)
// to avoid circular dependencies. In Python sqlglot, these are in __init__.py.
export {
  Parser, parse, parseOne,
};
export type {
  GeneratorOptions, TranspileOptions,
};
export {
  Generator, generate,
};
export {
  Schema, MappingSchema,
};
export { diff };
export { lineage };
export { optimize };
export { execute };

export let pretty = false;

export function setPretty (value: boolean): void {
  pretty = value;
}

export function transpile (
  sql: string,
  read?: Dialects,
  write?: Dialects,
  identity = true,
  errorLevel?: ErrorLevel,
  opts?: TranspileOptions,
): string[] {
  const writeDialect = identity
    ? (write ?? read)
    : write;
  throw new UnsupportedError(
    `transpile() not yet implemented (sql: "${sql}", read: "${read}", write: "${writeDialect}", errorLevel: ${errorLevel}, opts: ${JSON.stringify(opts)})`,
  );
}
