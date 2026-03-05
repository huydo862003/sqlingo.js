/**
 * @hdnax/slot.js
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
import {
  Dialect, type Dialects, type DialectType,
} from './dialects/dialect';
import { diff } from './diff';
import { lineage } from './lineage';
import { optimize } from './optimizer/optimizer';
import { execute } from './executor';

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

export {
  dump, load,
} from './serde';

export const version = packageJson.version;

export {
  ErrorLevel, SqlglotError, ParseError, TokenError, UnsupportedError,
};
export type { Token };
export {
  TokenType, Tokenizer,
};
export type { ParseOptions };
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
export {
  Dialect, type Dialects, type DialectType,
};
export { diff };
export { lineage };
export { optimize };
export { execute };

export let pretty = false;

export function setPretty (value: boolean): void {
  pretty = value;
}

export function tokenize (sql: string, read?: DialectType, dialect?: DialectType): Token[] {
  return Dialect.getOrRaise(read ?? dialect).tokenize(sql);
}

export function transpile (
  sql: string,
  opts: TranspileOptions = {},
): string[] {
  const {
    read, dialect, write, identity = true, errorLevel, ...rest
  } = opts;
  const writeDialect = identity ? (write ?? read ?? dialect) : write;
  const writeDial = Dialect.getOrRaise(writeDialect);
  return parse(sql, {
    read: read ?? dialect,
    errorLevel,
    ...rest,
  }).map((expression) =>
    expression
      ? writeDial.generate(expression, {
        copy: false,
        ...rest,
      })
      : '');
}
