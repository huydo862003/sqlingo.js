// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/pushdown_predicates.py

import {
  AggFuncExpr,
  AliasExpr,
  Expression,
  AndExpr,
  ColumnExpr,
  FromExpr,
  JoinExpr,
  JoinExprKind,
  or as orExpr,
  OrExpr,
  SelectExpr,
  TableExpr,
  true_ as trueExpr,
  UnnestExpr,
  WhereExpr,
  WindowExpr,
  columnTableNames,
  WithExpr,
} from '../expressions';
import {
  Dialect, type DialectType,
} from '../dialects/dialect';
import { Athena } from '../dialects/athena';
import { Presto } from '../dialects/presto';
import { narrowInstanceOf } from '../port_internals';
import { normalized } from './normalize';
import {
  buildScope, findInScope, Scope,
} from './scope';
import { simplify } from './simplify';

/**
 * Rewrite sqlglot AST to pushdown predicates in FROMs and JOINs.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { pushdownPredicates } from 'sqlglot/optimizer';
 *
 *     const sql = "SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y WHERE y.a = 1";
 *     const expression = parseOne(sql);
 *     pushdownPredicates(expression).sql();
 *     // 'SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x WHERE x.a = 1) AS y WHERE TRUE'
 *     ```
 *
 * @param expression - Expression to optimize
 * @param options - Optimization options
 * @param options.dialect - SQL dialect
 * @returns The optimized expression
 */
export function pushdownPredicates<E extends Expression> (
  expression: E,
  options: {
    dialect?: DialectType;
  } = {},
): E {
  const { dialect: dialectArg } = options;
  const root = buildScope(expression);

  const dialect = Dialect.getOrRaise(dialectArg);

  const unnestRequiresCrossJoin = dialect instanceof Athena || dialect instanceof Presto;

  if (root) {
    const scopeRefCount = root.refCount();

    for (const scope of Array.from(root.traverse()).reverse()) {
      const select = scope.expression;
      const whereClause = select.getArgKey('where') as WhereExpr | undefined;

      if (whereClause) {
        let selectedSources = scope.selectedSources;
        const joins = select.getArgKey('joins') as JoinExpr[] | undefined;
        const joinIndex = new Map<string, number>();

        if (joins) {
          joins.forEach((join, i) => {
            const name = join.aliasOrName;
            if (name) {
              joinIndex.set(name, i);
            }
          });
        }

        // A right join can only push down to itself and not the source FROM table
        // Presto, Trino and Athena don't support inner joins where the RHS is an UNNEST expression
        let pushdownAllowed = true;
        for (const [k, source] of Object.entries(selectedSources)) {
          const [node] = source;
          if (!node) continue;

          const parent = node.findAncestor<JoinExpr | FromExpr>(JoinExpr, FromExpr);

          if (parent instanceof JoinExpr) {
            const joinParent = parent as JoinExpr;
            if (joinParent.args.side === JoinExprKind.RIGHT) {
              selectedSources = { [k]: source };
              break;
            }
            if (node instanceof UnnestExpr && unnestRequiresCrossJoin) {
              pushdownAllowed = false;
              break;
            }
          }
        }

        if (pushdownAllowed) {
          const whereThis = whereClause.args.this;
          if (whereThis instanceof Expression) {
            pushdown(whereThis, selectedSources, scopeRefCount, dialect, joinIndex);
          }
        }
      }

      // Joins should only pushdown into itself, not to other joins
      // So we limit the selected sources to only itself
      const joins = select.getArgKey('joins') as JoinExpr[] | undefined;
      if (joins) {
        for (const join of joins) {
          const name = join.aliasOrName;
          if (name && name in scope.selectedSources) {
            const onClause = join.args.on;
            pushdown(
              onClause,
              { [name]: scope.selectedSources[name] },
              scopeRefCount,
              dialect,
            );
          }
        }
      }
    }
  }

  return expression;
}

function pushdown (
  condition: Expression | undefined,
  sources: Record<string, [Expression, Scope | Expression]>,
  scopeRefCount: Map<Scope | Expression, number>,
  dialect: Dialect,
  joinIndex?: Map<string, number>,
): void {
  if (!condition) {
    return;
  }

  const simplified = simplify(condition, { dialect });
  condition = condition.replace(simplified);
  const cnfLike = normalized(condition) || !normalized(condition, { dnf: true });

  const predicates = (cnfLike ? condition instanceof AndExpr : condition instanceof OrExpr)
    ? Array.from(condition.flatten())
    : [condition];

  if (cnfLike) {
    pushdownCnf(predicates, sources, scopeRefCount, joinIndex);
  } else {
    pushdownDnf(predicates, sources, scopeRefCount);
  }
}

function pushdownCnf (
  predicates: Iterable<Expression>,
  sources: Record<string, [Expression, Scope | Expression]>,
  scopeRefCount: Map<Scope | Expression, number>,
  joinIndex?: Map<string, number>,
): void {
  /**
   * If the predicates are in CNF like form, we can simply replace each block in the parent.
   */
  const joinIndexMap = joinIndex || new Map();

  for (const predicate of predicates) {
    const nodes = nodesForPredicate(predicate, sources, scopeRefCount);

    for (const [name, node] of Object.entries(nodes)) {
      if (node instanceof JoinExpr) {
        const predicateTables = columnTableNames(predicate, { exclude: name });

        // Don't push the predicate if it references tables that appear in later joins
        const thisIndex = joinIndexMap.get(name) ?? -1;
        const canPush = Array.from(predicateTables).every((table) => {
          const tableIndex = joinIndexMap.get(table) ?? -1;
          return tableIndex < thisIndex;
        });

        if (canPush) {
          predicate.replace(trueExpr());
          node.on(predicate, { copy: false });
          break;
        }
      } else if (node instanceof SelectExpr) {
        predicate.replace(trueExpr());
        const innerPredicate = replaceAliases(node, predicate);

        if (findInScope(innerPredicate, AggFuncExpr)) {
          // Add to HAVING clause
          node.having(innerPredicate, { copy: false });
        } else {
          node.where(innerPredicate, { copy: false });
        }
      }
    }
  }
}

function pushdownDnf (
  predicates: Iterable<Expression>,
  sources: Record<string, [Expression, Scope | Expression]>,
  scopeRefCount: Map<Scope | Expression, number>,
): void {
  /**
   * If the predicates are in DNF form, we can only push down conditions that are in all blocks.
   * Additionally, we can't remove predicates from their original form.
   */
  // Find all the tables that can be pushed down to
  // These are tables that are referenced in all blocks of a DNF
  const pushdownTables = new Set<string>();

  for (const a of predicates) {
    let aTables = columnTableNames(a);

    for (const b of predicates) {
      const bTables = columnTableNames(b);
      aTables = new Set(Array.from(aTables).filter((t) => bTables.has(t)));
    }

    for (const table of aTables) {
      pushdownTables.add(table);
    }
  }

  const conditions = new Map<string, Expression>();

  let nodes = {};

  // Pushdown all predicates to their respective nodes
  for (const table of Array.from(pushdownTables).sort()) {
    for (const predicate of predicates) {
      nodes = nodesForPredicate(predicate, sources, scopeRefCount);

      if (!(table in nodes)) {
        continue;
      }

      const existing = conditions.get(table);
      conditions.set(table, existing ? orExpr([existing, predicate]) : predicate);
    }

    for (const [name, node] of Object.entries(nodes)) {
      const condition = conditions.get(name);
      if (!condition) {
        continue;
      }

      if (node instanceof JoinExpr) {
        node.on(condition, { copy: false });
      } else if (node instanceof SelectExpr) {
        const innerPredicate = replaceAliases(node, condition);

        if (findInScope(innerPredicate, AggFuncExpr)) {
          // Add to HAVING clause
          node.having(innerPredicate, { copy: false });
        } else {
          node.where(innerPredicate, { copy: false });
        }
      }
    }
  }
}

function nodesForPredicate (
  predicate: Expression,
  sources: Record<string, [Expression, Scope | Expression]>,
  scopeRefCount: Map<Expression | Scope, number>,
): Record<string, Expression> {
  const nodes: Record<string, Expression> = {};
  const tables = columnTableNames(predicate);
  const whereCondition = predicate.findAncestor<JoinExpr | WhereExpr>(JoinExpr, WhereExpr) instanceof WhereExpr;

  for (const table of Array.from(tables).sort()) {
    const sourceEntry = sources[table];
    if (!sourceEntry) {
      continue;
    }

    let node = sourceEntry[0] as Expression | undefined;
    const [, source] = sourceEntry;

    // If the predicate is in a where statement we can try to push it down
    // We want to find the root join or from statement
    if (node && whereCondition) {
      node = node.findAncestor<JoinExpr | FromExpr>(JoinExpr, FromExpr);
    }

    // A node can reference a CTE which should be pushed down
    if (node instanceof FromExpr && !(source instanceof TableExpr)) {
      const with_ = narrowInstanceOf((source instanceof Scope ? source.parent : (source as Expression).parent?.args)?.expression, Expression)?.getArgKey('with');
      if (with_ instanceof WithExpr && with_?.recursive) {
        return {};
      }
      node = source instanceof Scope ? source.expression : (source as Expression).args.expression as Expression | undefined;
    }

    if (node instanceof JoinExpr) {
      const side = node.side;
      if (side && side !== JoinExprKind.RIGHT) {
        return {};
      }
      nodes[table] = node;
    } else if (node instanceof SelectExpr && tables.size === 1) {
      // We can't push down window expressions
      const hasWindowExpression = node.selects.some((sel) => {
        if (!(sel instanceof Expression)) {
          return false;
        }
        return sel.find(WindowExpr) !== undefined;
      });

      const hasGroup = Boolean(node.args.group);

      // We can't push down predicates to select statements if they are referenced multiple times
      const refCount = scopeRefCount.get(source) || 0;

      if (!hasGroup && refCount < 2 && !hasWindowExpression) {
        nodes[table] = node;
      }
    }
  }

  return nodes;
}

function replaceAliases (source: SelectExpr, predicate: Expression): Expression {
  const aliases = new Map<string, Expression>();

  for (const select of source.selects) {
    if (!(select instanceof Expression)) {
      continue;
    }

    if (select instanceof AliasExpr) {
      const aliasThis = select.args.this;
      if (aliasThis instanceof Expression) {
        aliases.set(select.alias, aliasThis);
      }
    } else {
      aliases.set(select.name, select);
    }
  }

  function replaceAlias (column: Expression): Expression {
    if (column instanceof ColumnExpr) {
      const replacement = aliases.get(column.name);
      if (replacement) {
        return replacement.copy();
      }
    }
    return column;
  }

  return predicate.transform(replaceAlias);
}
