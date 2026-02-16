// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/qualify.py

import type { Expression } from '../expressions';
import {
  Dialect, type DialectType,
} from '../dialects/dialect';
import {
  ensureSchema, type Schema,
} from '../schema';
import { isolateTableSelects } from './isolate_table_selects';
import { normalizeIdentifiers } from './normalize_identifiers';
import {
  qualifyColumns,
  quoteIdentifiers,
  validateQualifyColumns,
} from './qualify_columns';
import { qualifyTables } from './qualify_tables';

/**
 * Rewrite sqlglot AST to have normalized and qualified tables and columns.
 *
 * This step is necessary for all further SQLGlot optimizations.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { qualify } from 'sqlglot/optimizer';
 *
 *     const schema = { tbl: { col: "INT" } };
 *     const expression = parseOne("SELECT col FROM tbl");
 *     qualify(expression, { schema }).sql();
 *     // 'SELECT "tbl"."col" AS "col" FROM "tbl" AS "tbl"'
 *     ```
 *
 * @param expression - Expression to qualify
 * @param options - Qualification options
 * @param options.dialect - SQL dialect
 * @param options.db - Default database name for tables
 * @param options.catalog - Default catalog name for tables
 * @param options.schema - Schema to infer column names and types
 * @param options.expandAliasRefs - Whether to expand references to aliases (default: true)
 * @param options.expandStars - Whether to expand star queries (default: true). This is necessary for most optimizer rules!
 * @param options.inferSchema - Whether to infer the schema if missing
 * @param options.isolateTables - Whether to isolate table selects (default: false)
 * @param options.qualifyColumns - Whether to qualify columns (default: true)
 * @param options.allowPartialQualification - Whether to allow partial qualification (default: false)
 * @param options.validateQualifyColumns - Whether to validate columns (default: true)
 * @param options.quoteIdentifiers - Whether to quote identifiers (default: true). This is necessary for case-sensitive queries!
 * @param options.identify - If true, quote all identifiers, else only necessary ones (default: true)
 * @param options.canonicalizeTableAliases - Use canonical aliases (_0, _1, ...) instead of preserving table names (default: false)
 * @param options.onQualify - Callback after a table has been qualified
 * @param options.sql - Original SQL string for error highlighting
 * @returns The qualified expression
 */
export function qualify<E extends Expression> (
  expression: E,
  options: {
    dialect?: DialectType;
    db?: string;
    catalog?: string;
    schema?: Record<string, unknown> | Schema;
    expandAliasRefs?: boolean;
    expandStars?: boolean;
    inferSchema?: boolean;
    isolateTables?: boolean;
    qualifyColumns?: boolean;
    allowPartialQualification?: boolean;
    validateQualifyColumns?: boolean;
    quoteIdentifiers?: boolean;
    identify?: boolean;
    canonicalizeTableAliases?: boolean;
    onQualify?: (expression: Expression) => void;
    sql?: string;
  } = {},
): E {
  const {
    dialect: dialectArg,
    db,
    catalog,
    schema: schemaArg,
    expandAliasRefs = true,
    expandStars = true,
    inferSchema,
    isolateTables = false,
    qualifyColumns: qualifyColumnsFlag = true,
    allowPartialQualification = false,
    validateQualifyColumns: validateQualifyColumnsFlag = true,
    quoteIdentifiers: quoteIdentifiersFlag = true,
    identify = true,
    canonicalizeTableAliases = false,
    onQualify,
    sql,
  } = options;

  const schema = ensureSchema(schemaArg, { dialect: dialectArg });
  const dialect = Dialect.getOrRaise(dialectArg);

  expression = normalizeIdentifiers(expression, {
    dialect,
    storeOriginalColumnIdentifiers: true,
  }) as E;

  expression = qualifyTables(expression, {
    db,
    catalog,
    dialect,
    onQualify,
    canonicalizeTableAliases,
  }) as E;

  if (isolateTables) {
    expression = isolateTableSelects(expression, {
      schema,
      dialect,
    }) as E;
  }

  if (qualifyColumnsFlag) {
    expression = qualifyColumns(expression, {
      schema,
      expandAliasRefs,
      expandStars,
      inferSchema,
      allowPartialQualification,
    }) as E;
  }

  if (quoteIdentifiersFlag) {
    expression = quoteIdentifiers(expression, {
      dialect,
      identify,
    }) as E;
  }

  if (validateQualifyColumnsFlag) {
    validateQualifyColumns(expression, sql);
  }

  return expression;
}
