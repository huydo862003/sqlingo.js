/**
 * @hdnax/sqlglot.js
 * A JavaScript port of SQLGlot - SQL parser, transpiler, optimizer, and engine
 *
 * Based on SQLGlot by Toby Mao (https://github.com/tobymao/sqlglot)
 */

import packageJson from '../package.json';
import { ErrorLevel, SqlglotError, ParseError, TokenError, UnsupportedError } from './errors';
import type { Token } from './tokens';
import { TokenType, Tokenizer, tokenize } from './tokens';
import type { ParseOptions } from './parser';
import { Parser, parse } from './parser';
import type { GeneratorOptions, TranspileOptions } from './generator';
import { Generator, generate } from './generator';
import {
  Expression,
  aliasExpr,
  andExpr,
  caseExpr,
  castExpr,
  columnExpr,
  conditionExpr,
  deleteExpr,
  exceptExpr,
  fromExpr,
  funcExpr,
  insertExpr,
  intersectExpr,
  maybeParseExpr,
  mergeExpr,
  notExpr,
  orExpr,
  selectExpr,
  subqueryExpr,
  tableExpr,
  toColumn,
  toIdentifier,
  toTable,
  unionExpr,
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
export { TokenType, Tokenizer, tokenize };
export type { ParseOptions };
export { Parser, parse };
export type { GeneratorOptions, TranspileOptions };
export { Generator, generate };
export {
  Expression,
  aliasExpr,
  andExpr,
  caseExpr,
  castExpr,
  columnExpr,
  conditionExpr,
  deleteExpr,
  exceptExpr,
  fromExpr,
  funcExpr,
  insertExpr,
  intersectExpr,
  maybeParseExpr,
  mergeExpr,
  notExpr,
  orExpr,
  selectExpr,
  subqueryExpr,
  tableExpr,
  toColumn,
  toIdentifier,
  toTable,
  unionExpr,
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
