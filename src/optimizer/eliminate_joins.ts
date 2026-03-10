// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/eliminate_joins.py

import {
  AggFuncExpr,
  and,
  AndExpr,
  ColumnExpr,
  EqExpr,
  Expression,
  JoinExpr,
  JoinExprKind,
  LimitExpr,
  SelectExpr,
  true_,
} from '../expressions';
import { assertIsInstanceOf } from '../port_internals';
import { normalized } from './normalize';
import {
  Scope, traverseScope,
} from './scope';

/**
 * Remove unused joins from an expression.
 *
 * This only removes joins when we know that the join condition doesn't produce duplicate rows.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { eliminateJoins } from 'sqlglot/optimizer';
 *
 *     const sql = "SELECT x.a FROM x LEFT JOIN (SELECT DISTINCT y.b FROM y) AS y ON x.b = y.b";
 *     const expression = parseOne(sql);
 *     eliminateJoins(expression).sql();
 *     // 'SELECT x.a FROM x'
 *     ```
 *
 * @param expression - Expression to optimize
 * @returns The optimized expression
 */
export function eliminateJoins<E extends Expression> (expression: E): E {
  for (const scope of traverseScope(expression)) {
    // If any columns in this scope aren't qualified, it's hard to determine if a join isn't used.
    // It's probably possible to infer this from the outputs of derived tables.
    // But for now, let's just skip this rule.
    if (0 < scope.unqualifiedColumns.length) {
      continue;
    }

    const joins = scope.expression.args.joins;
    if (!joins) {
      continue;
    }

    // Reverse the joins so we can remove chains of unused joins
    for (let i = joins.length - 1; 0 <= i; i--) {
      const join: Expression = joins[i];
      assertIsInstanceOf(join, JoinExpr);
      if (join.isSemiOrAntiJoin) {
        continue;
      }

      const alias = join.aliasOrName;
      if (alias && shouldEliminateJoin(scope, join, alias)) {
        join.pop();
        scope.removeSource(alias);
      }
    }
  }

  return expression;
}

function shouldEliminateJoin (scope: Scope, join: JoinExpr, alias: string): boolean {
  const innerSource = scope.sources.get(alias);
  if (!(innerSource instanceof Scope)) {
    return false;
  }

  if (joinIsUsed(scope, join, alias)) {
    return false;
  }

  const side = join.args.side;
  const onClause = join.args.on;

  return (
    (side === JoinExprKind.LEFT && isJoinedOnAllUniqueOutputs(innerSource, join))
    || (!onClause && hasSingleOutputRow(innerSource))
  );
}

function joinIsUsed (scope: Scope, join: JoinExpr, alias: string): boolean {
  // We need to find all columns that reference this join.
  // But columns in the ON clause shouldn't count.
  const onClause = join.args.on;

  const onClauseColumns = new Set<Expression>();
  if (onClause) {
    for (const column of onClause.findAll(ColumnExpr)) {
      onClauseColumns.add(column);
    }
  }

  const sourceColumns = scope.sourceColumns(alias);
  return sourceColumns.some((column) => !onClauseColumns.has(column));
}

function isJoinedOnAllUniqueOutputs (scope: Scope, join: JoinExpr): boolean {
  const uniqueOutputs_ = uniqueOutputs(scope);
  if (!uniqueOutputs_ || uniqueOutputs_.size === 0) {
    return false;
  }

  const { joinKeys } = joinCondition(join);
  const joinKeyNames = new Set(joinKeys.map((k) => k.name));

  for (const output of uniqueOutputs_) {
    if (!joinKeyNames.has(output)) {
      return false;
    }
  }

  return true;
}

function uniqueOutputs (scope: Scope): Set<string> | undefined {
  const expression = scope.expression;
  if (!(expression instanceof SelectExpr)) {
    return undefined;
  }

  const select = expression;

  // DISTINCT makes all outputs unique
  if (select.args.distinct) {
    return new Set(select.namedSelects);
  }

  // GROUP BY makes grouped columns unique
  const group = select.args.group;
  if (group) {
    const groupedSqls = new Set(
      (group.args.expressions ?? [])
        .filter((e): e is Expression => e instanceof Expression)
        .map((e) => e.sql()),
    );
    const groupedOutputSqls = new Set<string>();
    const uniqueOutputs = new Set<string>();

    for (const selectExpr of select.selects) {
      if (!(selectExpr instanceof Expression)) {
        continue;
      }

      const outputSql = selectExpr.unalias().sql();
      if (groupedSqls.has(outputSql)) {
        groupedOutputSqls.add(outputSql);
        uniqueOutputs.add(selectExpr.aliasOrName);
      }
    }

    // All the grouped expressions must be in the output
    const allGroupedInOutput = [...groupedSqls].every((s) => groupedOutputSqls.has(s));

    return allGroupedInOutput ? uniqueOutputs : new Set();
  }

  // Single row output makes all outputs unique
  if (hasSingleOutputRow(scope)) {
    return new Set(select.namedSelects);
  }

  return new Set();
}

function hasSingleOutputRow (scope: Scope): boolean {
  const expression = scope.expression;
  if (!(expression instanceof SelectExpr)) {
    return false;
  }

  const select = expression;

  // No FROM clause means single row
  if (!select.args.from) {
    return true;
  }

  // All aggregates without GROUP BY means single row
  const allAggregates = select.selects.every((e) => {
    if (!(e instanceof Expression)) {
      return false;
    }
    const unaliased = e.unalias();
    return unaliased instanceof AggFuncExpr;
  });

  if (allAggregates) {
    return true;
  }

  // LIMIT 1 means single row
  if (isLimit1(scope)) {
    return true;
  }

  return false;
}

function isLimit1 (scope: Scope): boolean {
  const limit = scope.expression.getArgKey('limit');
  if (!(limit instanceof LimitExpr)) {
    return false;
  }
  return limit.args.expression?.args.this === '1';
}

/**
 * Extract the join condition from a join expression.
 *
 * @param join - The join expression
 * @returns Tuple of (source key, join key, remaining predicate)
 */
export function joinCondition (
  join: JoinExpr,
): {
  sourceKeys: Expression[];
  joinKeys: Expression[];
  on: Expression;
} {
  const name = join.aliasOrName;
  const onClause = join.args.on;
  const on = (onClause || true_()).copy();

  const sourceKeys: Expression[] = [];
  const joinKeys: Expression[] = [];

  function extractCondition (condition: Expression): void {
    const [left, right] = condition.unnestOperands();
    if (!left || !right) {
      return;
    }

    const leftTables = columnTableNames(left);
    const rightTables = columnTableNames(right);

    if (name && leftTables.has(name) && !rightTables.has(name)) {
      joinKeys.push(left);
      sourceKeys.push(right);
      condition.replace(true_());
    } else if (name && rightTables.has(name) && !leftTables.has(name)) {
      joinKeys.push(right);
      sourceKeys.push(left);
      condition.replace(true_());
    }
  }

  if (normalized(on)) {
    // CNF form: AND of EQ conditions
    const andOn = on instanceof AndExpr ? on : and([on, true_()], { copy: false });
    for (const condition of andOn.flatten()) {
      if (condition instanceof EqExpr) {
        extractCondition(condition);
      }
    }
  } else if (normalized(on, { dnf: true })) {
    // DNF form: OR of ANDs — find EQ conditions present in every OR branch
    let conditions: EqExpr[] | undefined;

    for (const orBranch of on.flatten()) {
      const parts = Array.from(orBranch.flatten())
        .filter((p): p is EqExpr => p instanceof EqExpr);

      if (conditions === undefined) {
        conditions = parts;
      } else {
        const temp: EqExpr[] = [];
        for (const p of parts) {
          const cs = conditions.filter((c) => p.sql() === c.sql());
          if (0 < cs.length) {
            temp.push(p);
            temp.push(...cs);
          }
        }
        conditions = temp;
      }
    }

    if (conditions) {
      for (const condition of conditions) {
        extractCondition(condition);
      }
    }
  }

  return {
    sourceKeys,
    joinKeys,
    on,
  };
}

function columnTableNames (expression: Expression): Set<string> {
  const tables = new Set<string>();
  for (const column of expression.findAll(ColumnExpr)) {
    if (column.table) {
      tables.add(column.table);
    }
  }
  return tables;
}
