/**
 * @hdnax/sqlglot.js
 * A JavaScript port of SQLGlot - SQL parser, transpiler, optimizer, and engine
 *
 * Based on SQLGlot by Toby Mao (https://github.com/tobymao/sqlglot)
 */

import packageJson from '../package.json';
import { ErrorLevel, SqlglotError, ParseError, TokenError, UnsupportedError } from './errors';
import type { Token } from './tokens';
import { TokenType, Tokenizer } from './tokens';
import type { ParseOptions } from './parser';
import { Parser, parse } from './parser';
import type { GeneratorOptions, TranspileOptions } from './generator';
import { Generator, generate } from './generator';
import {
  Expression,
  alias,
  and,
  case,
  cast,
  column,
  condition,
  delete,
  except,
  from,
  func,
  insert,
  intersect,
  maybeParseExpr,
  mergeExpr,
  notExpr,
  or,
  select,
  subqueryExpr,
  table,
  toColumn,
  toIdentifier,
  toTable,
  union,
  findTables,
} from './expressions';
import { Schema, MappingSchema } from './schema';
import type { Dialects } from './dialects/index';
import { diff } from './diff';
import { lineage } from './lineage';
import { optimize } from './optimizer/index';
import { execute } from './executor/index';

export const version = packageJson.version;

export { ErrorLevel, SqlglotError, ParseError, TokenError, UnsupportedError };
export type { Token };
export { TokenType, Tokenizer };
export type { ParseOptions };
export { Parser, parse };
export type { GeneratorOptions, TranspileOptions };
export { Generator, generate };
export {
  Expression,
  alias as aliasExpr,
  and as andExpr,
  case as caseExpr,
  cast as castExpr,
  column as columnExpr,
  condition as conditionExpr,
  delete as deleteExpr,
  except as exceptExpr,
  from as fromExpr,
  func as funcExpr,
  insert as insertExpr,
  intersect as intersectExpr,
  maybeParseExpr,
  mergeExpr,
  notExpr,
  or as orExpr,
  select as selectExpr,
  subqueryExpr,
  table as tableExpr,
  toColumn,
  toIdentifier,
  toTable,
  union as unionExpr,
  findTables,
};
export { Schema, MappingSchema };
export { diff };
export { lineage };
export { optimize };
export { execute };

export let pretty = false;

export function setPretty (value: boolean): void {
  pretty = value;
}

export function parseOne (
  sql: string,
  read?: Dialects,
  dialect?: Dialects,
  opts?: ParseOptions,
): Expression {
  const activeDialect = read ?? dialect;
  const result = parse(sql, activeDialect, undefined, opts);

  for (const expression of result) {
    if (!expression) {
      throw new ParseError(`No expression was parsed from '${sql}'`);
    }
    return expression;
  }

  throw new ParseError(`No expression was parsed from '${sql}'`);
}

export function transpile (
  sql: string,
  read?: Dialects,
  write?: Dialects,
  identity = true,
  errorLevel?: ErrorLevel,
  opts?: TranspileOptions,
): string[] {
  const writeDialect = identity ? (write ?? read) : write;
  throw new UnsupportedError(
    `transpile() not yet implemented (sql: "${sql}", read: "${read}", write: "${writeDialect}", errorLevel: ${errorLevel}, opts: ${JSON.stringify(opts)})`,
  );
}
