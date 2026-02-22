// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/unnest_subqueries.py

import type {
  ColumnExpr,
} from '../expressions';
import {
  Expression,
  InExpr,
  SelectExpr,
  SetOperationExpr,
  WhereExpr,
  HavingExpr,
  JoinExpr,
  AnyExpr,
  FuncExpr,
  TableExpr,
  FromExpr,
  AggFuncExpr,
  ArrayAggExpr,
  BinaryExpr,
  CoalesceExpr,
  column,
  alias,
  condition,
  CountExpr,
  LimitExpr,
  LiteralExpr,
  MaxExpr,
  null_,
  OffsetExpr,
  OrExpr,
  PredicateExpr,
  select,
  toIdentifier,
  true_,
  EqExpr,
  AllExpr,
  SubqueryExpr,
  ExistsExpr,
  JoinExprKind,
  ConditionExpr,
} from '../expressions';
import { is } from '../port_internals';
import { nameSequence } from '../helper';
import {
  findInScope, ScopeType, traverseScope,
} from './scope';

/**
 * Rewrite sqlglot AST to convert some predicates with subqueries into joins.
 *
 * - Convert scalar subqueries into cross joins
 * - Convert correlated or vectorized subqueries into a group by so it is not a many-to-many left join
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { unnestSubqueries } from 'sqlglot/optimizer';
 *
 *     const expression = parseOne("SELECT * FROM x AS x WHERE (SELECT y.a AS a FROM y AS y WHERE x.a = y.a) = 1");
 *     unnestSubqueries(expression).sql();
 *     // 'SELECT * FROM x AS x LEFT JOIN (SELECT y.a AS a FROM y AS y WHERE TRUE GROUP BY y.a) AS _u_0 ON x.a = _u_0.a WHERE _u_0.a = 1'
 *     ```
 *
 * @param expression - Expression to unnest
 * @returns The unnested expression
 */
export function unnestSubqueries<E extends Expression> (expression: E): E {
  const nextAliasName = nameSequence('_u_');

  for (const scope of traverseScope(expression)) {
    const select = scope.expression;
    const parent = select.parentSelect;

    if (!parent || !is(parent, SelectExpr)) {
      continue;
    }

    if (!is(select, SelectExpr)) {
      continue;
    }

    const selectExpr: SelectExpr = select;

    if (0 < scope.externalColumns.length) {
      decorrelate(selectExpr, parent, scope.externalColumns, nextAliasName);
    } else if (scope.scopeType === ScopeType.SUBQUERY) {
      unnest(selectExpr, parent, nextAliasName);
    }
  }

  return expression;
}

function unnest (
  selectExpr: SelectExpr,
  parentSelect: SelectExpr,
  nextAliasName: () => string,
): void {
  if (1 < selectExpr.selects.length) {
    return;
  }

  const predicate = selectExpr.findAncestor(ConditionExpr);
  if (
    !predicate
    // Do not unnest subqueries inside table-valued functions such as
    // FROM GENERATE_SERIES(...), FROM UNNEST(...) etc in order to preserve join order
    || (
      predicate instanceof FuncExpr
      && (predicate.parent instanceof TableExpr
        || predicate.parent instanceof FromExpr
        || predicate.parent instanceof JoinExpr)
    )
    || parentSelect !== predicate.parentSelect
    || !parentSelect.args.from
  ) {
    return;
  }

  let selectToUse = selectExpr;
  if (selectExpr instanceof SetOperationExpr) {
    selectToUse = select(selectExpr.selects).from(selectExpr.subquery(nextAliasName()), { copy: false });
  }

  const aliasExpr = nextAliasName();
  const clause = predicate.findAncestor(HavingExpr, WhereExpr, JoinExpr);

  // This subquery returns a scalar and can just be converted to a cross join
  if (!(predicate instanceof InExpr || predicate instanceof AnyExpr)) {
    let columnExpr: Expression = column({
      col: selectToUse.selects[0].aliasOrName,
      table: aliasExpr,
    });

    const clauseParentSelect = clause?.parentSelect;

    if (
      (clause instanceof HavingExpr && clauseParentSelect === parentSelect)
      || (
        (!clause || clauseParentSelect !== parentSelect)
        && (
          parentSelect.args.group
          || parentSelect.selects.some((sel) => findInScope(sel, AggFuncExpr))
        )
      )
    ) {
      columnExpr = new MaxExpr({ this: columnExpr });
    } else if (!(selectExpr.parent instanceof SubqueryExpr)) {
      return;
    }

    let joinType = JoinExprKind.CROSS;
    let onClause: Expression | undefined;
    if (predicate instanceof ExistsExpr) {
      // If a subquery returns no rows, cross-joining against it incorrectly eliminates all rows
      // from the parent query. Therefore, we use a LEFT JOIN that always matches (ON TRUE), then
      // check for non-NULL column values to determine whether the subquery contained rows.
      columnExpr = columnExpr.is(null_()).not();
      joinType = JoinExprKind.LEFT;
      onClause = true_();
    }

    replace(selectExpr.parent!, columnExpr);
    parentSelect.join(selectToUse, {
      on: onClause,
      joinType,
      joinAlias: aliasExpr,
      copy: false,
    });

    return;
  }

  if (selectToUse.find([LimitExpr, OffsetExpr])) {
    return;
  }

  let predicateToUse = predicate;
  if (predicate instanceof AnyExpr) {
    const eqPredicate = predicate.findAncestor(EqExpr);

    if (!eqPredicate || parentSelect !== eqPredicate.parentSelect) {
      return;
    }

    predicateToUse = eqPredicate;
  }

  const columnExpr = otherOperand(predicateToUse);
  const value = selectToUse.selects[0];
  const valueThis = value.this;
  if (!(valueThis instanceof Expression)) {
    return;
  }

  const joinKey = column({
    col: value.alias,
    table: aliasExpr,
  });
  const joinKeyNotNull = joinKey.is(null_()).not();

  if (clause instanceof JoinExpr) {
    replace(predicateToUse, true_());
    parentSelect.where(joinKeyNotNull, { copy: false });
  } else {
    replace(predicateToUse, joinKeyNotNull);
  }

  const group = selectToUse.args.group;

  if (group) {
    // Simulate set comparison in sqlglot
    if ([value.this].length !== new Set(group.expressions).size || value.this !== group.expressions[0]) {
      selectToUse = select(
        alias(column({
          col: value.alias,
          table: '_q',
        }), value.alias, { copy: false }),
      )
        .from(selectToUse.subquery('_q', { copy: false }), { copy: false })
        .groupBy(column({
          col: value.alias,
          table: '_q',
        }), { copy: false });
    }
  } else if (!findInScope(valueThis, AggFuncExpr)) {
    selectToUse = selectToUse.groupBy(valueThis, { copy: false });
  }

  parentSelect.join(
    selectToUse,
    {
      on: columnExpr!.eq(joinKey),
      joinType: JoinExprKind.LEFT,
      joinAlias: aliasExpr,
      copy: false,
    },
  );
}

function decorrelate (
  select: SelectExpr,
  parentSelect: SelectExpr,
  externalColumns: ColumnExpr[],
  nextAliasName: () => string,
): void {
  const where = select.args.where;

  if (!where || where.find(OrExpr) || select.find([LimitExpr, OffsetExpr])) {
    return;
  }

  const tableAlias = nextAliasName();
  const keys: [Expression, ColumnExpr, Expression][] = [];

  // for all external columns in the where statement, find the relevant predicate
  // keys to convert it into a join
  for (const column of externalColumns) {
    if (column.findAncestor(WhereExpr) !== where) {
      return;
    }

    const predicate = column.findAncestor(PredicateExpr);

    if (!predicate || predicate.findAncestor(WhereExpr) !== where) {
      return;
    }

    let key: Expression | undefined;

    if (predicate instanceof BinaryExpr) {
      key = Array.from(predicate.left?.walk() || []).some((node) => node === column)
        ? predicate.right
        : predicate.left;
    } else {
      return;
    }

    if (!key) return;

    keys.push([
      key,
      column,
      predicate,
    ]);
  }

  if (!keys.some(([
    , , predicate,
  ]) => predicate instanceof EqExpr)) {
    return;
  }

  const isSubqueryProjection = parentSelect.selects
    .map((s) => s.unalias())
    .some((node) => node instanceof SubqueryExpr && node === select.parent);

  const value = select.selects[0];
  const valueThis = value.this;
  if (!(valueThis instanceof Expression)) return;
  const keyAliases = new Map<Expression, string>();
  const groupBy: Expression[] = [];

  for (const [
    key, , predicate,
  ] of keys) {
    // if we filter on the value of the subquery, it needs to be unique
    if (key === value.this) {
      keyAliases.set(key, value.alias);
      groupBy.push(key);
    } else {
      if (!keyAliases.has(key)) {
        keyAliases.set(key, nextAliasName());
      }
      // all predicates that are equalities must also be in the unique
      // so that we don't do a many to many join
      if (predicate instanceof EqExpr && !groupBy.includes(key)) {
        groupBy.push(key);
      }
    }
  }

  const parentPredicate = select.findAncestor(PredicateExpr);

  // if the value of the subquery is not an agg or a key, we need to collect it into an array
  // so that it can be grouped. For subquery projections, we use a MAX aggregation instead.
  const aggFunc = isSubqueryProjection ? MaxExpr : ArrayAggExpr;
  if (!value.find(AggFuncExpr) && !groupBy.includes(valueThis)) {
    select.select(
      alias(new aggFunc({ this: valueThis }), value.alias, { quoted: false }),
      {
        append: false,
        copy: false,
      },
    );
  }

  // exists queries should not have any selects as it only checks if there are any rows
  // all selects will be added by the optimizer and only used for join keys
  if (parentPredicate instanceof ExistsExpr) {
    select.args.expressions = [];
  }

  for (const [key, keyAlias] of keyAliases) {
    if (groupBy.includes(key)) {
      // add all keys to the projections of the subquery
      // so that we can use it as a join key
      if (parentPredicate instanceof ExistsExpr || key !== value.this) {
        select.select(`${key} AS ${keyAlias}`, { copy: false });
      }
    } else {
      select.select(alias(new aggFunc({ this: key.copy() }), keyAlias, { quoted: false }), { copy: false });
    }
  }

  let aliasExpr: Expression = column({
    col: value.alias,
    table: tableAlias,
  });
  const other = otherOperand(parentPredicate);
  const opType = parentPredicate?.parent?._constructor;

  if (parentPredicate instanceof ExistsExpr) {
    aliasExpr = column({
      col: Array.from(keyAliases.values())[0],
      table: tableAlias,
    });
    replace(parentPredicate, `NOT ${aliasExpr} IS NULL`);
  } else if (parentPredicate instanceof AllExpr) {
    if (!opType || !(opType.prototype instanceof BinaryExpr || opType === BinaryExpr)) return;
    const predicateExpr = new opType({
      this: other,
      expression: column({ col: '_x' }),
    });
    replace(parentPredicate.parent!, `ARRAY_ALL(${aliasExpr}, _x -> ${predicateExpr})`);
  } else if (parentPredicate instanceof AnyExpr) {
    if (!opType || !(opType.prototype instanceof BinaryExpr || opType === BinaryExpr)) return;
    if (groupBy.includes(value.this as Expression)) {
      const predicateExpr = new opType({
        this: other,
        expression: aliasExpr,
      });
      replace(parentPredicate.parent!, predicateExpr);
    } else {
      const predicateExpr = new opType({
        this: other,
        expression: column({ col: '_x' }),
      });
      replace(parentPredicate, `ARRAY_ANY(${aliasExpr}, _x -> ${predicateExpr})`);
    }
  } else if (parentPredicate instanceof InExpr) {
    if (groupBy.includes(value.this as Expression)) {
      replace(parentPredicate, `${other} = ${aliasExpr}`);
    } else {
      replace(parentPredicate, `ARRAY_ANY(${aliasExpr}, _x -> _x = ${parentPredicate.this})`);
    }
  } else {
    if (isSubqueryProjection && select.parent?.alias) {
      aliasExpr = alias(aliasExpr, select.parent.alias, { copy: false });
    }

    // COUNT always returns 0 on empty datasets, so we need take that into consideration here
    // by transforming all counts into 0 and using that as the coalesced value
    if (value.find(CountExpr)) {
      const removeAggs = (node: Expression): Expression => {
        if (node instanceof CountExpr) {
          return LiteralExpr.number(0);
        } else if (node instanceof AggFuncExpr) {
          return null_();
        }
        return node;
      };

      aliasExpr = new CoalesceExpr({
        this: aliasExpr,
        expressions: [(value.this as Expression).transform(removeAggs)],
      });
    }

    select.parent?.replace(aliasExpr);
  }

  for (const [
    key,
    columnExpr,
    predicate,
  ] of keys) {
    predicate.replace(true_());
    const nested = column({
      col: keyAliases.get(key)!,
      table: tableAlias,
    });

    if (isSubqueryProjection) {
      key.replace(nested);
      if (!(predicate instanceof EqExpr)) {
        parentSelect.where(predicate, { copy: false });
      }
      continue;
    }

    if (groupBy.includes(key)) {
      key.replace(nested);
    } else if (predicate instanceof EqExpr) {
      replace(parentPredicate!, `(${parentPredicate} AND ARRAY_CONTAINS(${nested}, ${columnExpr}))`);
    } else {
      key.replace(toIdentifier('_x'));
      replace(parentPredicate!, `(${parentPredicate} AND ARRAY_ANY(${nested}, _x -> ${predicate}))`);
    }
  }

  parentSelect.join(
    select.groupBy(...groupBy, { copy: false }),
    {
      on: keys.filter(([
        , , predicate,
      ]) => predicate instanceof EqExpr).map(([
        , , predicate,
      ]) => predicate),
      joinType: JoinExprKind.LEFT,
      joinAlias: tableAlias,
      copy: false,
    },
  );
}

function replace (expression: Expression, conditionExpr: string | Expression): Expression {
  return expression.replace(condition(conditionExpr));
}

function otherOperand (expression: Expression | undefined): Expression | undefined {
  if (expression instanceof InExpr) {
    return expression.args.this as Expression;
  }

  if (expression instanceof AnyExpr || expression instanceof AllExpr) {
    return otherOperand(expression.parent);
  }

  if (expression instanceof BinaryExpr) {
    return (
      expression.left instanceof SubqueryExpr
      || expression.left instanceof AnyExpr
      || expression.left instanceof ExistsExpr
      || expression.left instanceof AllExpr
    )
      ? expression.right as Expression
      : expression.left as Expression;
  }

  return undefined;
}
