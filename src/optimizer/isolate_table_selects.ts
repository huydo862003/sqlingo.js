// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/isolate_table_selects.py

import type {
  Expression,
} from '../expressions';
import {
  alias as aliasExpr,
  select as selectExpr,
  SubqueryExpr,
  TableExpr,
} from '../expressions';
import { OptimizeError } from '../errors';
import type { DialectType } from '../dialects/dialect';
import {
  ensureSchema, type Schema,
} from '../schema';
import { traverseScope } from './scope';

/**
 * Isolate table references in queries with multiple sources.
 *
 * This wraps table references in a subquery to avoid ambiguous column references
 * when the same table appears multiple times.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { isolateTableSelects } from 'sqlglot/optimizer';
 *
 *     const sql = "SELECT a FROM t AS t1 JOIN t AS t2 ON t1.id = t2.id";
 *     const expression = parseOne(sql);
 *     isolateTableSelects(expression).sql();
 *     // Wraps t references in subqueries for isolation
 *     ```
 *
 * @param expression - Expression to optimize
 * @param options - Optimization options
 * @param options.schema - Database schema
 * @param options.dialect - SQL dialect
 * @returns The optimized expression
 */
export function isolateTableSelects<E extends Expression> (
  expression: E,
  options: {
    schema?: Record<string, unknown> | Schema;
    dialect?: DialectType;
  } = {},
): E {
  const {
    schema: schemaArg, dialect,
  } = options;
  const schema = ensureSchema(schemaArg, { dialect });

  for (const scope of traverseScope(expression)) {
    if (Object.keys(scope.selectedSources).length === 1) {
      continue;
    }

    for (const [, sourceEntry] of Object.entries(scope.selectedSources)) {
      // sourceEntry is [node, source] where source is TableExpr | Scope
      // We need the actual source (second element) to check if it's a Table
      const [node, actualSource] = sourceEntry;

      // Only process actual table references, not CTEs (which are Scope objects)
      if (!(actualSource instanceof TableExpr)) {
        continue;
      }

      const source = node as TableExpr;

      if (!source.parent) {
        continue;
      }

      // Skip if:
      // - No column names in schema for this table
      // - Parent is already a Subquery
      // - Parent's parent is a Table (already isolated)
      const columnNames = schema.columnNames?.(actualSource);
      if (!columnNames || columnNames.length === 0) {
        continue;
      }

      if (source.parent instanceof SubqueryExpr) {
        continue;
      }

      if (source.parent?.parent instanceof TableExpr) {
        continue;
      }

      // Table must have an alias for isolation to work
      const tableAlias = source.alias;
      if (!tableAlias) {
        throw new OptimizeError('Tables require an alias. Run qualify_tables optimization.');
      }

      // Wrap table in SELECT * FROM table subquery
      const aliasName = source.aliasOrName;
      const subquery = selectExpr('*')
        .from(aliasExpr(source, aliasName, { table: true }), { copy: false })
        .subquery(aliasName, { copy: false });

      source.replace(subquery);
    }
  }

  return expression;
}
