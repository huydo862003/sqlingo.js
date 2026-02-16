// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/optimizer.py

import {
  maybeParse, type Expression,
} from '../expressions';
import type { DialectType } from '../dialects/dialect';
import type { Schema } from '../schema';
import { ensureSchema } from '../schema';
import { annotateTypes } from './annotate_types';
import { canonicalize } from './canonicalize';
import { eliminateCtes } from './eliminate_ctes';
import { eliminateJoins } from './eliminate_joins';
import { eliminateSubqueries } from './eliminate_subqueries';
import { mergeSubqueries } from './merge_subqueries';
import { normalize } from './normalize';
import { optimizeJoins } from './optimize_joins';
import { pushdownPredicates } from './pushdown_predicates';
import { pushdownProjections } from './pushdown_projections';
import { qualify } from './qualify';
import { quoteIdentifiers } from './qualify_columns';
import { simplify } from './simplify';
import { unnestSubqueries } from './unnest_subqueries';

/**
 * Optimizer rule function type.
 */
type OptimizerRule = (expression: Expression, options?: Record<string, unknown>) => Expression;

/**
 * Default sequence of optimizer rules.
 *
 * NOTE: canonicalize is omitted as it's not fully implemented yet.
 * The Python version includes: qualify, pushdown_projections, normalize,
 * unnest_subqueries, pushdown_predicates, optimize_joins, eliminate_subqueries,
 * merge_subqueries, eliminate_joins, eliminate_ctes, quote_identifiers,
 * annotate_types, canonicalize, simplify
 */
export const RULES: OptimizerRule[] = [
  qualify,
  pushdownProjections,
  normalize,
  unnestSubqueries,
  pushdownPredicates,
  optimizeJoins,
  eliminateSubqueries,
  mergeSubqueries,
  eliminateJoins,
  eliminateCtes,
  quoteIdentifiers,
  annotateTypes,
  canonicalize,
  simplify,
];

/**
 * Rewrite a sqlglot AST into an optimized form.
 *
 * Example:
 *     ```ts
 *     import { optimize } from 'sqlglot/optimizer';
 *
 *     const optimized = optimize(
 *       "SELECT a FROM (SELECT x.a FROM x) CROSS JOIN y",
 *       { schema: { x: { a: "INT" }, y: { b: "INT" } } }
 *     );
 *     optimized.sql();
 *     // 'SELECT x.a FROM x CROSS JOIN y'
 *     ```
 *
 * @param expression - Expression to optimize (string or Expression)
 * @param options - Optimization options
 * @param options.schema - Database schema (table -> column -> type mapping)
 * @param options.db - Default database name
 * @param options.catalog - Default catalog name
 * @param options.dialect - SQL dialect
 * @param options.rules - Sequence of optimizer rules to use (default: RULES)
 * @param options.sql - Original SQL string for error highlighting
 * @param options.isolateTables - Whether to isolate tables (default: true)
 * @param options.quoteIdentifiers - Whether to quote identifiers (default: false)
 * @returns The optimized expression
 */
export function optimize (
  expression: string | Expression,
  options: {
    schema?: Record<string, unknown> | Schema;
    db?: string;
    catalog?: string;
    dialect?: DialectType;
    rules?: OptimizerRule[];
    sql?: string;
    isolateTables?: boolean;
    quoteIdentifiers?: boolean;
    [key: string]: unknown;
  } = {},
): Expression {
  const {
    schema: schemaArg,
    db,
    catalog,
    dialect,
    rules = RULES,
    sql,
    isolateTables = true,
    quoteIdentifiers: quoteIdentifiersFlag = false,
    ...extraOptions
  } = options;

  const schema = ensureSchema(schemaArg, { dialect });

  // Base options passed to rules
  const possibleOptions: Record<string, unknown> = {
    db,
    catalog,
    schema,
    dialect,
    sql,
    isolateTables,
    quoteIdentifiers: quoteIdentifiersFlag,
    ...extraOptions,
  };

  // Parse expression if it's a string
  let optimized = maybeParse(expression, {
    dialect,
    copy: true,
  });

  // Apply each rule in sequence
  for (const rule of rules) {
    optimized = rule(optimized, possibleOptions);
  }

  return optimized;
}
