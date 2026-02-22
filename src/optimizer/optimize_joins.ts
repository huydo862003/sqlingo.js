// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/optimize_joins.py

import type { Expression } from '../expressions';
import {
  combine,
  columnTableNames,
  ConnectorExpr,
  FromExpr,
  JoinExpr,
  JoinExprKind,
  SelectExpr,
  true_,
} from '../expressions';
import { filterInstanceOf } from '../port_internals';
import { tsort } from '../helper';

const JOIN_ATTRS = [
  'on',
  'side',
  'kind',
  'using',
  'method',
] as const;

/**
 * Removes cross joins if possible and reorder joins based on predicate dependencies.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { optimizeJoins } from 'sqlglot/optimizer';
 *
 *     const expr = parseOne("SELECT * FROM x CROSS JOIN y JOIN z ON x.a = z.a AND y.a = z.a");
 *     optimizeJoins(expr).sql();
 *     // 'SELECT * FROM x JOIN z ON x.a = z.a AND TRUE JOIN y ON y.a = z.a'
 *     ```
 *
 * @param expression - The expression to optimize
 * @returns The optimized expression
 */
export function optimizeJoins (expression: Expression): Expression {
  for (const select of expression.findAll(SelectExpr)) {
    const joins = filterInstanceOf(select.args.joins ?? [], JoinExpr);

    if (!isReorderable(joins)) {
      continue;
    }

    const references: Record<string, JoinExpr[]> = {};
    const crossJoins: [string, JoinExpr][] = [];

    for (const join of joins) {
      const tables = otherTableNames(join);

      if (0 < tables.size) {
        for (const table of tables) {
          if (!references[table]) {
            references[table] = [];
          }
          references[table].push(join);
        }
      } else {
        crossJoins.push([join.aliasOrName, join]);
      }
    }

    for (const [name, join] of crossJoins) {
      for (const dep of references[name] ?? []) {
        const on = dep.args.on;

        if (on instanceof ConnectorExpr) {
          if (otherTableNames(dep).size < 2) {
            continue;
          }

          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const operator = on.constructor as new (args: any) => ConnectorExpr;
          for (const predicate of on.flatten()) {
            if (columnTableNames(predicate).has(name)) {
              predicate.replace(true_());
              const combined = combine(
                [join.args.on, predicate],
                operator,
                { copy: false },
              );
              join.on(combined, {
                append: false,
                copy: false,
              });
            }
          }
        }
      }
    }
  }

  expression = reorderJoins(expression);
  expression = normalize(expression);
  return expression;
}

/**
 * Reorder joins by topological sort order based on predicate references.
 *
 * @param expression - The expression to reorder joins in
 * @returns The expression with reordered joins
 */
export function reorderJoins (expression: Expression): Expression {
  for (const from of expression.findAll(FromExpr)) {
    const parent = from.parent;
    if (!parent) continue;

    const joins = filterInstanceOf(parent.args.joins ?? [], JoinExpr);

    if (!isReorderable(joins)) {
      continue;
    }

    const joinsByName = new Map<string, JoinExpr>();
    for (const join of joins) {
      joinsByName.set(join.aliasOrName, join);
    }

    const dag = new Map<string, Set<string>>();
    for (const [name, join] of joinsByName) {
      dag.set(name, otherTableNames(join));
    }

    const sorted = tsort(dag);
    const fromName = from.aliasOrName;
    const reorderedJoins = sorted
      .filter((name) => name !== fromName && joinsByName.has(name))
      .map((name) => joinsByName.get(name) as JoinExpr);

    parent.args.joins = reorderedJoins;
  }

  return expression;
}

/**
 * Remove INNER and OUTER from joins as they are optional.
 *
 * @param expression - The expression to normalize
 * @returns The normalized expression
 */
export function normalize (expression: Expression): Expression {
  for (const join of expression.findAll(JoinExpr)) {
    // Check if any join attributes are present
    const hasAnyAttr = JOIN_ATTRS.some((k) => join.args[k] != null);

    if (!hasAnyAttr) {
      join.args.kind = JoinExprKind.CROSS;
    }

    if (join.args.kind === JoinExprKind.CROSS) {
      join.args.on = undefined;
    } else {
      if (join.args.kind === JoinExprKind.INNER || join.args.kind === JoinExprKind.OUTER) {
        join.args.kind = undefined;
      }

      if (!join.args.on && !join.args.using) {
        join.args.on = true_();
      }
    }
  }

  return expression;
}

/**
 * Get the set of table names referenced in a join's ON clause,
 * excluding the join's own table.
 *
 * @param join - The join expression
 * @returns Set of referenced table names
 */
export function otherTableNames (join: JoinExpr): Set<string> {
  const on = join.args.on;
  return on ? columnTableNames(on, { exclude: join.aliasOrName }) : new Set();
}

/**
 * Checks if joins can be reordered without changing query semantics.
 *
 * Joins with a side (LEFT, RIGHT, FULL) cannot be reordered easily,
 * the order affects which rows are included in the result.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { isReorderable } from 'sqlglot/optimizer/optimizeJoins';
 *
 *     const ast = parseOne("SELECT * FROM x JOIN y ON x.id = y.id JOIN z ON y.id = z.id");
 *     const select = ast.find(SelectExpr);
 *     isReorderable(select.args.joins ?? []);
 *     // true
 *
 *     const ast2 = parseOne("SELECT * FROM x LEFT JOIN y ON x.id = y.id JOIN z ON y.id = z.id");
 *     const select2 = ast2.find(SelectExpr);
 *     isReorderable(select2.args.joins ?? []);
 *     // false
 *     ```
 *
 * @param joins - Array of join expressions
 * @returns True if joins can be safely reordered
 */
export function isReorderable (joins: JoinExpr[]): boolean {
  return !joins.some((join) => join.args.side);
}
