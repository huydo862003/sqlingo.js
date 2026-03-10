// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/eliminate_subqueries.py

import type {
  Expression, ExpressionHash,
} from '../expressions';
import {
  alias,
  CteExpr,
  DdlExpr,
  LateralExpr,
  SubqueryExpr,
  table,
  TableAliasExpr,
  TableExpr,
  toIdentifier,
  WithExpr,
} from '../expressions';
import {
  assertIsInstanceOf, isInstanceOf,
} from '../port_internals';
import { findNewName } from '../helper';
import type { Scope } from './scope';
import { buildScope } from './scope';

type ExistingCTEsMapping = Map<ExpressionHash, string>;
type TakenNameMapping = Map<string, Scope | TableExpr>;

/**
 * Rewrite derived tables as CTEs, deduplicating if possible.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { eliminateSubqueries } from 'sqlglot/optimizer';
 *
 *     const expression = parseOne("SELECT a FROM (SELECT * FROM x) AS y");
 *     eliminateSubqueries(expression).sql();
 *     // 'WITH y AS (SELECT * FROM x) SELECT a FROM y AS y'
 *     ```
 *
 * This also deduplicates common subqueries:
 *     ```ts
 *     const expression = parseOne("SELECT a FROM (SELECT * FROM x) AS y CROSS JOIN (SELECT * FROM x) AS z");
 *     eliminateSubqueries(expression).sql();
 *     // 'WITH y AS (SELECT * FROM x) SELECT a FROM y AS y CROSS JOIN y AS z'
 *     ```
 *
 * @param expression - Expression to optimize
 * @returns The optimized expression
 */
export function eliminateSubqueries<E extends Expression> (expression: E): E {
  if (expression instanceof SubqueryExpr && expression.args.this) {
    eliminateSubqueries(expression.args.this);
    return expression;
  }

  const root = buildScope(expression);

  if (!root) {
    return expression;
  }

  const taken: TakenNameMapping = new Map();

  // All CTE aliases in the root scope are taken
  for (const scope of root.cteScopes) {
    const parent = scope.expression.parent;
    if (parent) {
      taken.set(parent.alias, scope);
    }
  }

  // All table names are taken
  for (const scope of root.traverse()) {
    for (const [, source] of scope.sources) {
      if (source instanceof TableExpr) {
        taken.set(source.name, source);
      }
    }
  }

  const existingCtes: ExistingCTEsMapping = new Map();

  const withClause = root.expression.args.with;
  let recursive = false;

  if (withClause) {
    assertIsInstanceOf(withClause, WithExpr);
    recursive = Boolean(withClause.args.recursive);
    for (const cte of withClause.args.expressions ?? []) {
      if (isInstanceOf(cte, CteExpr) && cte.args.this) {
        if (cte.args.this) existingCtes.set(cte.args.this.sqlKey, cte.alias);
      }
    }
  }

  const newCtes: Expression[] = [];

  // We're adding more CTEs, but we want to maintain the DAG order.
  // Derived tables within an existing CTE need to come before the existing CTE.
  for (const cteScope of root.cteScopes) {
    // Append all the new CTEs from this existing CTE
    for (const scope of cteScope.traverse()) {
      if (scope === cteScope) {
        // Don't try to eliminate this CTE itself
        continue;
      }
      const newCte = eliminate(scope, existingCtes, taken);
      if (newCte) {
        newCtes.push(newCte);
      }
    }

    // Append the existing CTE itself
    const cteParent = cteScope.expression.parent;
    if (cteParent) {
      newCtes.push(cteParent);
    }
  }

  // Now append the rest
  const restScopes = [
    ...root.unionScopes,
    ...root.subqueryScopes,
    ...root.tableScopes,
  ];
  for (const scope of restScopes) {
    for (const childScope of scope.traverse()) {
      const newCte = eliminate(childScope, existingCtes, taken);
      if (newCte) {
        newCtes.push(newCte);
      }
    }
  }

  if (0 < newCtes.length) {
    const query = expression instanceof DdlExpr
      ? expression.args.expression
      : expression;

    query?.setArgKey('with', new WithExpr({
      expressions: newCtes as CteExpr[],
      recursive,
    }));
  }

  return expression;
}

function eliminate (
  scope: Scope,
  existingCtes: ExistingCTEsMapping,
  taken: TakenNameMapping,
): Expression | undefined {
  if (scope.isDerivedTable) {
    return eliminateDerivedTable(scope, existingCtes, taken);
  }

  if (scope.isCte) {
    return eliminateCte(scope, existingCtes, taken);
  }

  return undefined;
}

function eliminateDerivedTable (
  scope: Scope,
  existingCtes: ExistingCTEsMapping,
  taken: TakenNameMapping,
): Expression | undefined {
  // This makes sure that we don't:
  // - drop the "pivot" arg from a pivoted subquery
  // - eliminate a lateral correlated subquery
  if (scope.parent && (0 < scope.parent.pivots.length || scope.parent.expression instanceof LateralExpr)) {
    return undefined;
  }

  // Get rid of redundant exp.Subquery expressions, i.e. those that are just used as wrappers
  let toReplace = scope.expression.parent;
  if (!toReplace) {
    return undefined;
  }

  // Unwrap nested subqueries
  if (toReplace instanceof SubqueryExpr) {
    toReplace = (toReplace as SubqueryExpr).unwrap();
  }

  const [name, cte] = newCte(scope, existingCtes, taken);

  const tableExpr = alias(table(name), toReplace.alias || name, { copy: false });
  const toReplaceArgs = toReplace.args as Record<string, unknown>;
  const joins = toReplaceArgs.joins;
  if (joins && Array.isArray(joins)) {
    (tableExpr as TableExpr).setArgKey('joins', joins);
  }

  toReplace.replace(tableExpr);

  return cte;
}

function eliminateCte (
  scope: Scope,
  existingCtes: ExistingCTEsMapping,
  taken: TakenNameMapping,
): Expression | undefined {
  const parent = scope.expression.parent;
  if (!parent) {
    return undefined;
  }

  const [name, cte] = newCte(scope, existingCtes, taken);

  const withClause = parent.parent;
  parent.pop();

  if (withClause) {
    const withExpressions = withClause.args.expressions;
    if (!withExpressions || withExpressions.length === 0) {
      withClause.pop();
    }
  }

  // Rename references to this CTE
  if (scope.parent) {
    for (const childScope of scope.parent.traverse()) {
      for (const [, source] of Object.entries(childScope.selectedSources)) {
        const [tableExpr, sourceScope] = source;
        if (sourceScope === scope) {
          const newTable = alias(
            table(name),
            (tableExpr as Expression).aliasOrName,
            { copy: false },
          );
          tableExpr.replace(newTable);
        }
      }
    }
  }

  return cte;
}

function newCte (
  scope: Scope,
  existingCtes: ExistingCTEsMapping,
  taken: TakenNameMapping,
): [string, Expression | undefined] {
  /**
   * Returns:
   *     tuple of (name, cte)
   *     where `name` is a new name for this CTE in the root scope and `cte` is a new CTE instance.
   *     If this CTE duplicates an existing CTE, `cte` will be undefined.
   */
  const duplicateCteAlias = existingCtes.get(scope.expression.sqlKey);
  const parent = scope.expression.parent;
  let name = parent?.alias || '';

  if (!name) {
    name = findNewName(Array.from(taken.keys()), 'cte');
  }

  if (duplicateCteAlias) {
    name = duplicateCteAlias;
  } else if (taken.has(name)) {
    name = findNewName(Array.from(taken.keys()), name);
  }

  taken.set(name, scope);

  let cte: Expression | undefined;
  if (!duplicateCteAlias) {
    existingCtes.set(scope.expression.sqlKey, name);
    cte = new CteExpr({
      this: scope.expression,
      alias: new TableAliasExpr({ this: toIdentifier(name) }),
    });
  }

  return [name, cte];
}
