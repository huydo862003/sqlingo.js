// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/merge_subqueries.py

import { isInstanceOf } from '../port_internals';
import {
  AggFuncExpr,
  alias as aliasExpr,
  BinaryExpr,
  ColumnExpr,
  columnTableNames,
  EqExpr,
  ExplodeExpr,
  Expression,
  FromExpr,
  FuncExpr,
  GroupExpr,
  HavingExpr,
  JoinExpr,
  JoinExprKind,
  NeqExpr,
  OrderExpr,
  ParenExpr,
  paren as parenExpr,
  QueryTransformExpr,
  SelectExpr,
  SubqueryExpr,
  TableAliasExpr,
  TableExpr,
  toIdentifier,
  UnaryExpr,
  WhereExpr,
  WindowExpr,
} from '../expressions';
import {
  findNewName, seqGet,
} from '../helper';
import {
  Scope, traverseScope,
} from './scope';

/**
 * Rewrite sqlglot AST to merge derived tables into the outer query.
 *
 * This also merges CTEs if they are selected from only once.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { mergeSubqueries } from 'sqlglot/optimizer';
 *
 *     const expression = parseOne("SELECT a FROM (SELECT x.a FROM x) CROSS JOIN y");
 *     mergeSubqueries(expression).sql();
 *     // 'SELECT x.a FROM x CROSS JOIN y'
 *     ```
 *
 * If `leaveTablesIsolated` is True, this will not merge inner queries into outer
 * queries if it would result in multiple table selects in a single query.
 *
 * Inspired by https://dev.mysql.com/doc/refman/8.0/en/derived-table-optimization.html
 *
 * @param expression - Expression to optimize
 * @param options - Optimization options
 * @param options.leaveTablesIsolated - Don't merge if it creates multiple table selects (default: false)
 * @returns The optimized expression
 */
export function mergeSubqueries<E extends Expression> (
  expression: E,
  options: {
    leaveTablesIsolated?: boolean;
  } = {},
): E {
  const { leaveTablesIsolated = false } = options;

  expression = mergeCtes(expression, leaveTablesIsolated) as E;
  expression = mergeDerivedTables(expression, leaveTablesIsolated) as E;

  return expression;
}

// If a derived table has these Select args, it can't be merged
const UNMERGABLE_ARGS = new Set(
  Array.from(SelectExpr.availableArgs).filter(
    (arg) => ![
      'expressions',
      'from',
      'joins',
      'where',
      'order',
      'hint',
    ].includes(arg),
  ),
);

const SAFE_TO_REPLACE_UNWRAPPED = [
  ColumnExpr,
  EqExpr,
  FuncExpr,
  NeqExpr,
  ParenExpr,
] as const;

type FromOrJoin = FromExpr | JoinExpr;

function mergeCtes<E extends Expression> (
  expression: E,
  leaveTablesIsolated: boolean,
): E {
  const scopes = Array.from(traverseScope(expression));

  // All places where we select from CTEs
  // Key on CTE scope ID to detect CTEs selected from multiple times
  const cteSelections = new Map<Scope, [Scope, Scope, Expression][]>();

  for (const outerScope of scopes) {
    for (const [, sourceEntry] of Object.entries(outerScope.selectedSources)) {
      const [table, innerSource] = sourceEntry;
      if (innerSource instanceof Scope && innerSource.isCte) {
        let scopeList = cteSelections.get(innerSource);
        if (!scopeList) {
          scopeList = [];
          cteSelections.set(innerSource, scopeList);
        }
        scopeList.push([
          outerScope,
          innerSource,
          table,
        ]);
      }
    }
  }

  // Only merge CTEs that are selected from exactly once
  const singularCteSelections: [Scope, Scope, Expression][] = [];
  for (const [, selections] of cteSelections) {
    if (selections.length === 1) {
      singularCteSelections.push(selections[0]);
    }
  }

  for (const [
    outerScope,
    innerScope,
    table,
  ] of singularCteSelections) {
    // Find FromExpr or JoinExpr ancestor
    const fromOrJoin = table.findAncestor<FromExpr | JoinExpr>(FromExpr, JoinExpr);

    if (fromOrJoin && mergeable(outerScope, innerScope, leaveTablesIsolated, fromOrJoin)) {
      const alias = table.aliasOrName;
      renameInnerSources(outerScope, innerScope, alias);
      mergeFrom(outerScope, innerScope, table as SubqueryExpr | TableExpr, alias);
      mergeExpressions(outerScope, innerScope, alias);
      mergeOrder(outerScope, innerScope);
      mergeJoins(outerScope, innerScope, fromOrJoin);
      mergeWhere(outerScope, innerScope, fromOrJoin);
      mergeHints(outerScope, innerScope);
      popCte(innerScope);
      outerScope.clearCache();
    }
  }

  return expression;
}

function mergeDerivedTables<E extends Expression> (
  expression: E,
  leaveTablesIsolated: boolean,
): E {
  for (const outerScope of traverseScope(expression)) {
    for (const subquery of outerScope.derivedTables) {
      // Find FromExpr or JoinExpr ancestor
      const fromOrJoin = subquery.findAncestor<FromExpr | JoinExpr>(FromExpr, JoinExpr);

      const alias = subquery.aliasOrName;
      const innerScope = outerScope.sources.get(alias);

      if (
        innerScope instanceof Scope
        && fromOrJoin
        && mergeable(outerScope, innerScope, leaveTablesIsolated, fromOrJoin)
      ) {
        renameInnerSources(outerScope, innerScope, alias);
        mergeFrom(outerScope, innerScope, subquery, alias);
        mergeExpressions(outerScope, innerScope, alias);
        mergeOrder(outerScope, innerScope);
        mergeJoins(outerScope, innerScope, fromOrJoin);
        mergeWhere(outerScope, innerScope, fromOrJoin);
        mergeHints(outerScope, innerScope);
        outerScope.clearCache();
      }
    }
  }

  return expression;
}

function mergeable (
  outerScope: Scope,
  innerScope: Scope,
  leaveTablesIsolated: boolean,
  fromOrJoin: FromOrJoin,
): boolean {
  const innerSelect = innerScope.expression.unnest();

  // Check if window expressions are in unmergable operations
  function isWindowExpressionInUnmergableOperation (): boolean {
    const windowAliases = new Set<string>();
    if (!(innerSelect instanceof SelectExpr)) {
      return false;
    }
    const innerSelectExpr = innerSelect;

    for (const s of innerSelectExpr.selects) {
      if (!(s instanceof Expression)) {
        continue;
      }
      if (s.find(WindowExpr)) {
        windowAliases.add(s.aliasOrName);
      }
    }

    const innerSelectName = fromOrJoin.aliasOrName;
    const unmergableWindowColumns: ColumnExpr[] = [];

    for (const column of outerScope.columns) {
      // Check if column has unmergable ancestor
      const hasUnmergableAncestor = column.findAncestor(
        WhereExpr,
        GroupExpr,
        OrderExpr,
        JoinExpr,
        HavingExpr,
        AggFuncExpr,
      ) !== undefined;

      if (hasUnmergableAncestor) {
        unmergableWindowColumns.push(column);
      }
    }

    const windowExpressionsInUnmergable = unmergableWindowColumns.filter(
      (column) => column.table === innerSelectName && windowAliases.has(column.name),
    );

    return 0 < windowExpressionsInUnmergable.length;
  }

  // Check if outer select joins on inner select's join
  function outerSelectJoinsOnInnerSelectJoin (): boolean {
    if (!(fromOrJoin instanceof JoinExpr)) {
      return false;
    }

    const alias = fromOrJoin.aliasOrName;
    const on = fromOrJoin.args.on;

    if (!on) {
      return false;
    }

    const selections = Array.from(on.findAll(ColumnExpr))
      .filter((c) => c.table === alias)
      .map((c) => c.name);

    const innerFrom = innerScope.expression.getArgKey('from') as FromExpr | undefined;

    if (!innerFrom) {
      return false;
    }

    const innerFromTable = innerFrom.aliasOrName;

    if (!(innerScope.expression instanceof SelectExpr)) {
      return false;
    }

    const innerSelectExpr = innerScope.expression;
    const innerProjections = new Map<string, Expression>();

    for (const s of innerSelectExpr.selects) {
      if (!(s instanceof Expression)) {
        continue;
      }
      innerProjections.set(s.aliasOrName, s);
    }

    return selections.some((selection) => {
      const projection = innerProjections.get(selection);
      if (!projection) {
        return false;
      }
      const columns = Array.from(projection.findAll(ColumnExpr));
      return columns.some((col) => col.table !== innerFromTable);
    });
  }

  // Check if this is a recursive CTE
  function isRecursive (): boolean {
    const cte = innerScope.expression.parent;
    let node: Expression | undefined = outerScope.expression.parent;

    while (node) {
      if (node === cte) {
        return true;
      }
      node = node.parent;
    }
    return false;
  }

  // Main mergeability checks
  if (!(outerScope.expression instanceof SelectExpr)) {
    return false;
  }

  const outerSelectExpr = outerScope.expression;

  if (outerSelectExpr.isStar) {
    return false;
  }

  if (!(innerSelect instanceof SelectExpr)) {
    return false;
  }

  const innerSelectExpr = innerSelect;

  // Check for unmergable args
  for (const arg of UNMERGABLE_ARGS) {
    if (innerSelectExpr.getArgKey(arg)) {
      return false;
    }
  }

  if (!innerSelectExpr.args.from) {
    return false;
  }

  if (0 < outerScope.pivots.length) {
    return false;
  }

  // Check for AggFunc, Select, or Explode in inner expressions
  for (const e of innerSelectExpr.expressions) {
    if (!(e instanceof Expression)) {
      continue;
    }
    if (e.find(AggFuncExpr) || e.find(SelectExpr) || e.find(ExplodeExpr)) {
      return false;
    }
  }

  if (leaveTablesIsolated && 1 < Object.keys(outerScope.selectedSources).length) {
    return false;
  }

  if (fromOrJoin instanceof JoinExpr && innerSelectExpr.args.joins) {
    return false;
  }

  const joinSide = fromOrJoin instanceof JoinExpr ? fromOrJoin.args.side : undefined;

  if (
    fromOrJoin instanceof JoinExpr
    && innerSelectExpr.args.where
    && (joinSide === JoinExprKind.FULL || joinSide === JoinExprKind.LEFT || joinSide === JoinExprKind.RIGHT)
  ) {
    return false;
  }

  const outerJoins = outerSelectExpr.args.joins;

  if (
    fromOrJoin instanceof FromExpr
    && innerSelectExpr.args.where
    && outerJoins
    && outerJoins.some((j) => isInstanceOf(j, JoinExpr) && (j.args.side === JoinExprKind.FULL || j.args.side === JoinExprKind.RIGHT))
  ) {
    return false;
  }

  if (outerSelectJoinsOnInnerSelectJoin()) {
    return false;
  }

  if (isWindowExpressionInUnmergableOperation()) {
    return false;
  }

  if (isRecursive()) {
    return false;
  }

  if (innerSelectExpr.args.order && outerScope.isUnion) {
    return false;
  }

  const firstExpr = seqGet(innerSelectExpr.expressions, 0);
  if (firstExpr instanceof QueryTransformExpr) {
    return false;
  }

  return true;
}

function renameInnerSources (outerScope: Scope, innerScope: Scope, alias: string): void {
  const innerTaken = new Set(Object.keys(innerScope.selectedSources));
  const outerTaken = new Set(Object.keys(outerScope.selectedSources));
  const conflicts = new Set(Array.from(innerTaken).filter((x) => outerTaken.has(x)));
  conflicts.delete(alias);

  const taken = new Set([...outerTaken, ...innerTaken]);

  for (const conflict of conflicts) {
    const newName = findNewName(Array.from(taken), conflict);

    const sourceEntry = innerScope.selectedSources[conflict];
    if (!sourceEntry) {
      continue;
    }

    const [source] = sourceEntry;
    const newAlias = toIdentifier(newName);

    if (source instanceof TableExpr) {
      if (source.alias) {
        source.setArgKey('alias', newAlias);
      } else {
        source.replace(aliasExpr(source, newAlias, { copy: false }));
      }
    } else if (source?.parent instanceof SubqueryExpr) {
      source.parent.setArgKey('alias', new TableAliasExpr({ this: newAlias }));
    }

    for (const column of innerScope.sourceColumns(conflict)) {
      column.setArgKey('table', toIdentifier(newName));
    }

    innerScope.renameSource(conflict, newName);
    taken.add(newName);
  }
}

function mergeFrom (
  outerScope: Scope,
  innerScope: Scope,
  nodeToReplace: SubqueryExpr | TableExpr,
  alias: string,
): void {
  const from = innerScope.expression.getArgKey('from') as FromExpr;
  const newSubquery = from.this as Expression;

  newSubquery.setArgKey('joins', nodeToReplace.args.joins);
  nodeToReplace.replace(newSubquery);

  // Update join hints
  for (const joinHint of outerScope.joinHints) {
    const tables = Array.from(joinHint.findAll(TableExpr));
    for (const table of tables) {
      if (table.aliasOrName === nodeToReplace.aliasOrName) {
        table.setArgKey('this', toIdentifier(newSubquery.aliasOrName));
      }
    }
  }

  outerScope.removeSource(alias);
  const newSubquerySource = innerScope.sources.get(newSubquery.aliasOrName);
  if (newSubquerySource !== undefined) {
    outerScope.addSource(newSubquery.aliasOrName, newSubquerySource);
  }
}

function mergeJoins (outerScope: Scope, innerScope: Scope, fromOrJoin: FromOrJoin): void {
  const newJoins: JoinExpr[] = [];

  const joins = innerScope.expression.getArgKey('joins') as JoinExpr[] | undefined;

  if (joins) {
    for (const join of joins) {
      newJoins.push(join);
      const joinSource = innerScope.sources.get(join.aliasOrName);
      if (joinSource) {
        outerScope.addSource(join.aliasOrName, joinSource);
      }
    }
  }

  if (0 < newJoins.length) {
    const outerJoins = (outerScope.expression.getArgKey('joins') as JoinExpr[] | undefined) || [];

    // Maintain join order
    let position: number;
    if (fromOrJoin instanceof FromExpr) {
      position = 0;
    } else {
      position = outerJoins.indexOf(fromOrJoin) + 1;
    }

    outerJoins.splice(position, 0, ...newJoins);
    (outerScope.expression as SelectExpr).setArgKey('joins', outerJoins);
  }
}

function mergeExpressions (outerScope: Scope, innerScope: Scope, alias: string): void {
  // Collect all columns that reference the alias of the inner query
  const outerColumns = new Map<string, ColumnExpr[]>();

  for (const column of outerScope.columns) {
    if (column.table === alias) {
      const name = column.name;
      let columnList = outerColumns.get(name);
      if (!columnList) {
        columnList = [];
        outerColumns.set(name, columnList);
      }
      columnList.push(column);
    }
  }

  // Replace columns with the projection expression in the inner query
  if (!(innerScope.expression instanceof SelectExpr)) {
    return;
  }

  const innerSelectExpr = innerScope.expression;

  for (const expr of innerSelectExpr.expressions) {
    if (!(expr instanceof Expression)) {
      continue;
    }

    const projectionName = expr.aliasOrName;

    if (!projectionName) {
      continue;
    }

    const columnsToReplace = outerColumns.get(projectionName) || [];

    const unaliasedExpr = expr.unalias();
    const mustWrapExpression = !SAFE_TO_REPLACE_UNWRAPPED.some((cls) => expr instanceof cls);
    const isNumber = unaliasedExpr.isNumber;

    for (const column of columnsToReplace) {
      const parent = column.parent;

      // Don't merge literal numbers in GROUP BY (positional context)
      if (isNumber && parent instanceof GroupExpr) {
        column.replace(toIdentifier(column.name));
        continue;
      }

      let replacementExpr = unaliasedExpr;

      // Wrap in parens if needed to preserve precedence
      if (
        parent
        && (parent instanceof UnaryExpr || parent instanceof BinaryExpr)
        && mustWrapExpression
      ) {
        replacementExpr = parenExpr(replacementExpr, { copy: false });
      }

      // Make sure we don't change the column name
      if (parent instanceof SelectExpr && column.name !== replacementExpr.name) {
        replacementExpr = aliasExpr(replacementExpr, column.name, { copy: false });
      }

      column.replace(replacementExpr.copy());
    }
  }
}

function mergeWhere (outerScope: Scope, innerScope: Scope, fromOrJoin: FromOrJoin): void {
  const where = innerScope.expression.getArgKey('where') as WhereExpr | undefined;

  if (!where) {
    return;
  }

  const whereThis = where.this;
  if (!(whereThis instanceof Expression)) {
    return;
  }

  const outerExpression = outerScope.expression as SelectExpr;

  if (fromOrJoin instanceof JoinExpr) {
    // Merge predicates from outer join to ON clause if columns are already joined
    const from = outerExpression.args.from;
    const sources = new Set<string>();

    if (from) {
      sources.add(from.aliasOrName);
    }

    const joins = outerExpression.args.joins;
    if (joins) {
      for (const join of joins) {
        const source = join.aliasOrName;
        sources.add(source);
        if (source === fromOrJoin.aliasOrName) {
          break;
        }
      }
    }

    const whereTables = columnTableNames(whereThis);
    const allTablesInSources = Array.from(whereTables).every((t) => sources.has(t));

    if (allTablesInSources) {
      fromOrJoin.on(whereThis, { copy: false });
      return;
    }
  }

  outerExpression.where(whereThis, { copy: false });
}

function mergeOrder (outerScope: Scope, innerScope: Scope): void {
  const outerSelectExpr = outerScope.expression as SelectExpr;

  if (
    outerSelectExpr.args.group
    || outerSelectExpr.args.distinct
    || outerSelectExpr.args.having
    || outerSelectExpr.args.order
    || Object.keys(outerScope.selectedSources).length !== 1
  ) {
    return;
  }

  const hasAgg = outerSelectExpr.expressions.some((expr) => {
    if (!(expr instanceof Expression)) {
      return false;
    }
    return expr.find(AggFuncExpr) !== undefined;
  });

  if (hasAgg) {
    return;
  }

  outerSelectExpr.setArgKey('order', innerScope.expression.getArgKey('order'));
}

function mergeHints (outerScope: Scope, innerScope: Scope): void {
  const innerScopeHint = innerScope.expression.getArgKey('hint') as Expression | undefined;

  if (!innerScopeHint) {
    return;
  }

  const outerSelectExpr = outerScope.expression as SelectExpr;
  const outerScopeHint = outerSelectExpr.args.hint;

  if (outerScopeHint) {
    const innerHintExpressions = innerScopeHint.expressions;
    for (const hintExpression of innerHintExpressions) {
      if (hintExpression instanceof Expression) {
        outerScopeHint.append('expressions', hintExpression);
      }
    }
  } else {
    outerSelectExpr.setArgKey('hint', innerScopeHint);
  }
}

function popCte (innerScope: Scope): void {
  const cte = innerScope.expression.parent;
  if (!cte) {
    return;
  }

  const with_ = cte.parent;
  if (!with_) {
    return;
  }

  const withExpressions = with_.expressions;
  if (withExpressions.length === 1) {
    with_.pop();
  } else {
    cte.pop();
  }
}
