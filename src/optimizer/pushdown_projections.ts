// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/pushdown_projections.py

import {
  Expression,

  AggFuncExpr,
  alias,
  column,
  ColumnExpr,
  LiteralExpr,
  MaxExpr,
  QueryTransformExpr,
  SelectExpr,
  SetOperationExpr,
} from '../expressions';
import { type DialectType } from '../dialects/dialect';
import { seqGet } from '../helper';
import {
  ensureSchema, type Schema,
} from '../schema';
import { Resolver } from './resolver';
import {
  Scope, traverseScope,
} from './scope';

// Sentinel value that means an outer query selecting ALL columns
const SELECT_ALL = Symbol('SELECT_ALL');

/**
 * Selection to use if selection list is empty
 */
function defaultSelection (isAgg: boolean): Expression {
  return alias(
    isAgg
      ? new MaxExpr({ this: LiteralExpr.number(1) })
      : '1',
    '_',
  );
}

/**
 * Rewrite sqlglot AST to remove unused column projections.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { pushdownProjections } from 'sqlglot/optimizer';
 *
 *     const sql = "SELECT y.a AS a FROM (SELECT x.a AS a, x.b AS b FROM x) AS y";
 *     const expression = parseOne(sql);
 *     pushdownProjections(expression).sql();
 *     // 'SELECT y.a AS a FROM (SELECT x.a AS a FROM x) AS y'
 *     ```
 *
 * @param expression - Expression to optimize
 * @param options - Optimization options
 * @param options.schema - Database schema
 * @param options.removeUnusedSelections - Remove unused SELECT columns (default: true)
 * @param options.dialect - SQL dialect
 * @returns The optimized expression
 */
export function pushdownProjections<E extends Expression> (
  expression: E,
  options: {
    schema?: Record<string, unknown> | Schema;
    removeUnusedSelections?: boolean;
    dialect?: DialectType;
  } = {},
): E {
  const {
    schema: schemaArg,
    removeUnusedSelections = true,
    dialect,
  } = options;

  // Map of Scope to all columns being selected by outer queries
  const schema = ensureSchema(schemaArg, { dialect });
  const sourceColumnAliasCount = new Map<Expression | Scope, number>();
  const referencedColumns = new Map<Scope, Set<string | symbol>>();

  // We build the scope tree (which is traversed in DFS postorder), then iterate
  // over the result in reverse order. This should ensure that the set of selected
  // columns for a particular scope are completely built by the time we get to it.
  const scopes = Array.from(traverseScope(expression));

  for (let i = scopes.length - 1; 0 <= i; i--) {
    const scope = scopes[i];
    let parentSelections = referencedColumns.get(scope) || new Set([SELECT_ALL]);
    const aliasCount = sourceColumnAliasCount.get(scope) || 0;

    // We can't remove columns from SELECT DISTINCT nor UNION DISTINCT
    const scopeExpr = scope.expression;
    if (scopeExpr.getArgKey('distinct')) {
      parentSelections = new Set([SELECT_ALL]);
    }

    if (scopeExpr instanceof SetOperationExpr) {
      const kind = scopeExpr.$kind;
      const side = scopeExpr.$side;

      if (!kind && !side) {
        // Do not optimize this set operation if it's using the BigQuery specific
        // kind / side syntax (e.g INNER UNION ALL BY NAME)
        const [left, right] = scope.unionScopes;

        if (left.expression.selects.length !== right.expression.selects.length) {
          throw new Error(
            `Invalid set operation due to column mismatch: ${scope.expression.sql({ dialect })}.`,
          );
        }

        referencedColumns.set(left, parentSelections);

        if (right.expression.selects.some((select) => select instanceof Expression && select.isStar)) {
          referencedColumns.set(right, parentSelections);
        } else if (!left.expression.selects.some((select) => select instanceof Expression && select.isStar)) {
          if (scopeExpr.$byName) {
            referencedColumns.set(right, referencedColumns.get(left) || new Set());
          } else {
            for (let j = 0; j < left.expression.selects.length; j++) {
              const leftSelect = left.expression.selects[j];
              if (!(leftSelect instanceof Expression)) {
                continue;
              }

              if (parentSelections.has(SELECT_ALL) || parentSelections.has(leftSelect.aliasOrName)) {
                const rightSelect = right.expression.selects[j];
                if (rightSelect instanceof Expression) {
                  referencedColumns.get(right)!.add(rightSelect.aliasOrName);
                }
              }
            }
          }
        }
      }
    }

    if (scopeExpr instanceof SelectExpr) {
      if (removeUnusedSelections) {
        removeUnusedSelections_(scope, parentSelections, schema, aliasCount);
      }

      if (scopeExpr.isStar) {
        continue;
      }

      // Group columns by source name
      const selects = new Map<string, Set<string>>();
      for (const col of scope.columns) {
        const tableName = col.table || '';
        const colName = col.name;

        if (!selects.has(tableName)) {
          selects.set(tableName, new Set());
        }
        selects.get(tableName)!.add(colName);
      }

      // Push the selected columns down to the next scope
      for (const [name, source] of Object.entries(scope.selectedSources)) {
        const [node, sourceScope] = source;

        if (sourceScope instanceof Scope) {
          const firstSelect = seqGet(sourceScope.expression.selects, 0);

          let columns: Set<string | symbol>;
          if (0 < scope.pivots.length || firstSelect instanceof QueryTransformExpr) {
            columns = new Set([SELECT_ALL]);
          } else {
            columns = selects.get(name) || new Set();
          }

          if (!referencedColumns.has(sourceScope)) {
            referencedColumns.set(sourceScope, new Set());
          }
          for (const col of columns) {
            referencedColumns.get(sourceScope)!.add(col);
          }
        }

        const columnAliases = node.aliasColumnNames;
        if (columnAliases && 0 < columnAliases.length) {
          sourceColumnAliasCount.set(sourceScope as Scope, columnAliases.length);
        }
      }
    }
  }

  return expression;
}

function removeUnusedSelections_ (
  scope: Scope,
  parentSelections: Set<string | symbol>,
  schema: Schema,
  aliasCountParam: number,
): void {
  const scopeArgs = scope.expression.args as Record<string, unknown>;
  const order = scopeArgs.order as Expression | undefined;

  const orderRefs = new Set<string>();
  if (order) {
    // Assume columns without a qualified table are references to output columns
    for (const col of order.findAll(ColumnExpr)) {
      if (!col.table) {
        orderRefs.add(col.name);
      }
    }
  }

  const newSelections: Expression[] = [];
  let removed = false;
  let star = false;
  let isAgg = false;
  let aliasCount = aliasCountParam;

  const selectAll = parentSelections.has(SELECT_ALL);
  const select = scope.expression as SelectExpr;

  for (const selection of select.selects) {
    if (typeof selection === 'string' || typeof selection === 'number' || typeof selection === 'boolean') {
      continue;
    }

    const selectionExpr = selection as Expression;
    const name = selectionExpr.aliasOrName;

    if (selectAll || parentSelections.has(name) || orderRefs.has(name) || 0 < aliasCount) {
      newSelections.push(selectionExpr);
      aliasCount -= 1;
    } else {
      if (selectionExpr.isStar) {
        star = true;
      }
      removed = true;
    }

    if (!isAgg && selectionExpr.find(AggFuncExpr)) {
      isAgg = true;
    }
  }

  if (star) {
    const resolver = new Resolver(scope, schema, {});
    const names = new Set(newSelections.map((s) => s.aliasOrName));

    const sortedSelections = Array.from(parentSelections)
      .filter((name): name is string => typeof name === 'string')
      .sort();

    for (const name of sortedSelections) {
      if (!names.has(name)) {
        const table = resolver.getTable(name);
        newSelections.push(
          alias(column({
            col: name,
            table: table?.aliasOrName,
          }), name, { copy: false }),
        );
      }
    }
  }

  // If there are no remaining selections, just select a single constant
  if (newSelections.length === 0) {
    newSelections.push(defaultSelection(isAgg));
  }

  select.select(...newSelections, {
    append: false,
    copy: false,
  });

  if (removed) {
    scope.clearCache();
  }
}
