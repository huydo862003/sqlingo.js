// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/eliminate_ctes.py

import type { Expression } from '../expressions';
import {
  buildScope, Scope,
} from './scope';

/**
 * Remove unused CTEs from an expression.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { eliminateCtes } from 'sqlglot/optimizer';
 *
 *     const sql = "WITH y AS (SELECT a FROM x) SELECT a FROM z";
 *     const expression = parseOne(sql);
 *     eliminateCtes(expression).sql();
 *     // 'SELECT a FROM z'
 *     ```
 *
 * @param expression - Expression to optimize
 * @returns The optimized expression
 */
export function eliminateCtes<E extends Expression> (expression: E): E {
  const root = buildScope(expression);

  if (root) {
    const refCount = root.refCount();

    // Traverse the scope tree in reverse so we can remove chains of unused CTEs
    const scopes: Scope[] = [];
    for (const scope of root.traverse()) {
      scopes.push(scope);
    }

    for (let i = scopes.length - 1; 0 <= i; i--) {
      const scope = scopes[i];
      if (scope.isCte) {
        const scopeId = scope;
        const count = refCount.get(scopeId) || 0;

        if (count <= 0) {
          const cteNode = scope.expression.parent;
          if (!cteNode) {
            continue;
          }

          const withNode = cteNode.parent;
          cteNode.pop();

          // Pop the entire WITH clause if this is the last CTE
          if (withNode) {
            const withExpressions = withNode.expressions;
            if (!withExpressions || withExpressions.length === 0) {
              withNode.pop();
            }
          }

          // Decrement the ref count for all sources this CTE selects from
          for (const [, source] of Object.entries(scope.selectedSources)) {
            const [, sourceScope] = source;
            if (sourceScope instanceof Scope) {
              const currentCount = refCount.get(sourceScope) || 0;
              refCount.set(sourceScope, currentCount - 1);
            }
          }
        }
      }
    }
  }

  return expression;
}
