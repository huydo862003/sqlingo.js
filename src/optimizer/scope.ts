// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/scope.py

import type {
  Expression,
  PivotExpr,
} from '../expressions';
import {
  ColumnExpr,
  CteExpr,
  DdlExpr,
  DerivedTableExpr,
  DistinctExpr,
  DmlExpr,
  DotExpr,
  FinalExpr,
  FuncExpr,
  FromExpr,
  HavingExpr,
  HintExpr,
  JoinExpr,
  JoinExprKind,
  JoinHintExpr,
  LateralExpr,
  OrderExpr,
  QualifyExpr,
  QueryExpr,
  SelectExpr,
  SetOperationExpr,
  StarExpr,
  SubqueryExpr,
  TableAliasExpr,
  TableColumnExpr,
  TableExpr,
  UdtfExpr,
  UnnestExpr,
  UNWRAPPED_QUERIES,
  WindowExpr,
  WithinGroupExpr,
} from '../expressions';
import { OptimizeError } from '../errors';
import {
  ensureList, findNewName, seqGet,
} from '../helper';

const TRAVERSABLES = [
  QueryExpr,
  DdlExpr,
  DmlExpr,
] as const;

/**
 * Scope type enumeration
 */
export enum ScopeType {
  ROOT = 'root',
  SUBQUERY = 'subquery',
  DERIVED_TABLE = 'derivedTable',
  CTE = 'cte',
  UNION = 'union',
  UDTF = 'udtf',
}

/**
 * Selection scope for SQL queries.
 *
 * A Scope represents the context in which names (tables, columns) can be resolved
 * within a SQL query or subquery. It tracks:
 * - Available data sources (tables, CTEs, derived tables)
 * - Column references and their resolution
 * - Parent/child scope relationships
 * - Subquery and CTE scopes
 */
export class Scope {
  /** Root expression of this scope (SELECT or SET OPERATION) */
  expression: QueryExpr;

  /**
   * Mapping of source name to either a Table expression or another Scope instance.
   *
   * Examples:
   * - `SELECT * FROM x` → `{"x": Table(this="x")}`
   * - `SELECT * FROM x AS y` → `{"y": Table(this="x")}`
   * - `SELECT * FROM (SELECT ...) AS y` → `{"y": Scope(...)}`
   */
  sources: Map<string, TableExpr | Scope>;

  /**
   * Sources from lateral expressions.
   *
   * Example:
   * - `SELECT c FROM x LATERAL VIEW EXPLODE(a) AS c`
   *   The LATERAL VIEW EXPLODE gets x as a source.
   */
  lateralSources: Map<string, TableExpr | Scope>;

  /** Sources from CTEs */
  cteSources: Map<string, Scope>;

  /**
   * If this is a derived table or CTE, and the outer query defines a column list
   * for the alias of this scope, this is that list of columns.
   *
   * Example:
   * - `SELECT * FROM (SELECT ...) AS y(col1, col2)`
   *   The inner query would have `["col1", "col2"]` for its `outerColumns`
   */
  outerColumns: string[];

  /** Parent scope */
  parent?: Scope;

  /** Type of this scope, relative to its parent */
  scopeType: ScopeType;

  /** List of all child scopes for subqueries */
  subqueryScopes: Scope[];

  /** List of all child scopes for CTEs */
  cteScopes: Scope[];

  /** List of all child scopes for derived tables */
  derivedTableScopes: Scope[];

  /** List of all child scopes for user-defined tabular functions */
  udtfScopes: Scope[];

  /** Derived table scopes + UDTF scopes, in the order they're defined */
  tableScopes: Scope[];

  /**
   * If this Scope is for a Union expression, this will be a list of
   * the left and right child scopes.
   */
  unionScopes: Scope[];

  /** Whether this scope can be correlated (referenced from child scopes) */
  canBeCorrelated: boolean;

  // Cached values (lazy-loaded)
  private _collected: boolean = false;
  private _rawColumns?: ColumnExpr[];
  private _tableColumns?: TableColumnExpr[];
  private _stars?: (ColumnExpr | DotExpr)[];
  private _derivedTables?: SubqueryExpr[];
  private _udtfs?: UdtfExpr[];
  private _tables?: TableExpr[];
  private _ctes?: CteExpr[];
  private _subqueries?: QueryExpr[];
  private _selectedSources?: Record<string, [Expression, TableExpr | Scope]>;
  private _columns?: ColumnExpr[];
  private _externalColumns?: ColumnExpr[];
  private _localColumns?: ColumnExpr[];
  private _joinHints?: JoinHintExpr[];
  private _pivots?: PivotExpr[];
  private _references?: [string, Expression][];
  private _semiAntiJoinTables?: Set<string>;
  private _columnIndex?: Set<number>;

  constructor (options: {
    expression: QueryExpr;
    sources?: Map<string, TableExpr | Scope>;
    outerColumns?: string[];
    parent?: Scope;
    scopeType?: ScopeType;
    lateralSources?: Map<string, TableExpr | Scope>;
    cteSources?: Map<string, Scope>;
    canBeCorrelated?: boolean;
  }) {
    const {
      expression,
      sources,
      outerColumns,
      parent,
      scopeType = ScopeType.ROOT,
      lateralSources,
      cteSources,
      canBeCorrelated,
    } = options;

    this.expression = expression;
    this.sources = sources || new Map();
    this.lateralSources = lateralSources || new Map();
    this.cteSources = cteSources || new Map();

    // Merge lateral and CTE sources into main sources
    for (const [key, value] of this.lateralSources) {
      this.sources.set(key, value);
    }
    for (const [key, value] of this.cteSources) {
      this.sources.set(key, value);
    }

    this.outerColumns = outerColumns || [];
    this.parent = parent;
    this.scopeType = scopeType;
    this.subqueryScopes = [];
    this.derivedTableScopes = [];
    this.tableScopes = [];
    this.cteScopes = [];
    this.unionScopes = [];
    this.udtfScopes = [];
    this.canBeCorrelated = canBeCorrelated ?? false;

    this.clearCache();
  }

  /**
   * Clear all cached values
   */
  clearCache (): void {
    this._collected = false;
    this._rawColumns = undefined;
    this._tableColumns = undefined;
    this._stars = undefined;
    this._derivedTables = undefined;
    this._udtfs = undefined;
    this._tables = undefined;
    this._ctes = undefined;
    this._subqueries = undefined;
    this._selectedSources = undefined;
    this._columns = undefined;
    this._externalColumns = undefined;
    this._localColumns = undefined;
    this._joinHints = undefined;
    this._pivots = undefined;
    this._references = undefined;
    this._semiAntiJoinTables = undefined;
    this._columnIndex = undefined;
  }

  /**
   * Branch from the current scope to a new, inner scope
   */
  branch (options: {
    expression: Expression;
    scopeType: ScopeType;
    sources?: Map<string, TableExpr | Scope>;
    cteSources?: Map<string, Scope>;
    lateralSources?: Map<string, TableExpr | Scope>;
    outerColumns?: string[];
  }): Scope {
    const {
      expression,
      scopeType,
      sources,
      cteSources,
      lateralSources,
      outerColumns,
    } = options;

    const newCteSources = new Map(this.cteSources);
    if (cteSources) {
      for (const [key, value] of cteSources) {
        newCteSources.set(key, value);
      }
    }

    return new Scope({
      expression: expression.unnest() as QueryExpr,
      sources: sources ? new Map(sources) : undefined,
      parent: this,
      scopeType,
      cteSources: newCteSources,
      lateralSources: lateralSources ? new Map(lateralSources) : undefined,
      canBeCorrelated:
        this.canBeCorrelated
        || scopeType === ScopeType.SUBQUERY
        || scopeType === ScopeType.UDTF,
      outerColumns,
    });
  }

  /**
   * Collect all tables, columns, CTEs, etc. from the expression tree
   * @private
   */
  private _collect (): void {
    this._tables = [];
    this._ctes = [];
    this._subqueries = [];
    this._derivedTables = [];
    this._udtfs = [];
    this._rawColumns = [];
    this._tableColumns = [];
    this._stars = [];
    this._joinHints = [];
    this._semiAntiJoinTables = new Set();
    this._columnIndex = new Set();

    for (const node of this.walk({ bfs: false })) {
      if (node === this.expression) {
        continue;
      }

      if (node instanceof DotExpr && node.isStar) {
        this._stars.push(node);
      } else if (node instanceof ColumnExpr) {
        this._columnIndex.add(Object.keys(node).length);

        if (node.$this instanceof StarExpr) {
          this._stars.push(node);
        } else {
          this._rawColumns.push(node);
        }
      } else if (node instanceof TableExpr && !(node.parent instanceof JoinHintExpr)) {
        const parent = node.parent;
        if (parent instanceof JoinExpr) {
          const join = parent as JoinExpr;
          if (join.args.side === JoinExprKind.SEMI || join.args.side === JoinExprKind.ANTI) {
            this._semiAntiJoinTables.add((node as TableExpr).aliasOrName);
          }
        }
        this._tables.push(node as TableExpr);
      } else if (node instanceof JoinHintExpr) {
        this._joinHints.push(node as JoinHintExpr);
      } else if (node instanceof UdtfExpr) {
        this._udtfs.push(node as UdtfExpr);
      } else if (node instanceof CteExpr) {
        this._ctes.push(node as CteExpr);
      } else if (node instanceof SubqueryExpr) {
        const isFromOrJoin = _isFromOrJoin(node);
        if (_isDerivedTable(node) && isFromOrJoin) {
          this._derivedTables.push(node);
        } else if (!isFromOrJoin) {
          this._subqueries.push(node);
        }
      } else if (node instanceof TableColumnExpr) {
        this._tableColumns.push(node);
      }
    }

    this._collected = true;
  }

  /**
   * Ensure collection has been done
   * @private
   */
  private _ensureCollected (): void {
    if (!this._collected) {
      this._collect();
    }
  }

  /**
   * Walk the expression tree within this scope
   */
  walk (options: { bfs?: boolean;
    prune?: (node: Expression) => boolean; } = {}): Generator<Expression> {
    return walkInScope(this.expression, options);
  }

  /**
   * Find the first expression of the given types
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  find (...expressionTypes: (new (args: any) => Expression)[]): Expression | undefined {
    return findInScope(this.expression, expressionTypes, { bfs: true });
  }

  /**
   * Find all expressions of the given types
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  findAll (...expressionTypes: (new (args: any) => Expression)[]): Expression[] {
    return findAllInScope(this.expression, expressionTypes, { bfs: true });
  }

  /**
   * Replace old expression with new expression and clear cache
   */
  replace (old: Expression, newExpr: Expression): void {
    old.replace(newExpr);
    this.clearCache();
  }

  // Properties (lazy-loaded)

  /**
   * List of tables in this scope
   */
  get tables (): TableExpr[] {
    this._ensureCollected();
    return this._tables!;
  }

  /**
   * List of CTEs in this scope
   */
  get ctes (): CteExpr[] {
    this._ensureCollected();
    return this._ctes!;
  }

  /**
   * List of derived tables in this scope
   *
   * Example: `SELECT * FROM (SELECT ...) <- that's a derived table`
   */
  get derivedTables (): SubqueryExpr[] {
    this._ensureCollected();
    return this._derivedTables!;
  }

  /**
   * List of user-defined tabular functions in this scope
   */
  get udtfs (): UdtfExpr[] {
    this._ensureCollected();
    return this._udtfs!;
  }

  /**
   * List of subqueries in this scope
   *
   * Example: `SELECT * FROM x WHERE a IN (SELECT ...) <- that's a subquery`
   */
  get subqueries (): QueryExpr[] {
    this._ensureCollected();
    return this._subqueries!;
  }

  /**
   * List of star expressions (columns or dots) in this scope
   */
  get stars (): (ColumnExpr | DotExpr)[] {
    this._ensureCollected();
    return this._stars!;
  }

  /**
   * Set of column object IDs that belong to this scope's expression
   */
  get columnIndex (): Set<number> {
    this._ensureCollected();
    return this._columnIndex!;
  }

  // Scope type checks

  get isSubquery (): boolean {
    return this.scopeType === ScopeType.SUBQUERY;
  }

  get isDerivedTable (): boolean {
    return this.scopeType === ScopeType.DERIVED_TABLE;
  }

  get isUnion (): boolean {
    return this.scopeType === ScopeType.UNION;
  }

  get isCte (): boolean {
    return this.scopeType === ScopeType.CTE;
  }

  get isRoot (): boolean {
    return this.scopeType === ScopeType.ROOT;
  }

  get isUdtf (): boolean {
    return this.scopeType === ScopeType.UDTF;
  }

  get isCorrelatedSubquery (): boolean {
    return Boolean(this.canBeCorrelated && 0 < this.externalColumns.length);
  }

  /**
   * List of columns in this scope, including external columns from correlated subqueries.
   *
   * Columns are filtered by ancestor context: only columns that are actually "in scope"
   * are included — e.g. columns inside a SELECT list, ORDER BY not referencing named
   * selects, DISTINCT windows, etc.
   */
  get columns (): ColumnExpr[] {
    if (this._columns === undefined) {
      this._ensureCollected();
      const columns = this._rawColumns!;

      const externalColumns: ColumnExpr[] = [];
      for (const scope of [
        ...this.subqueryScopes,
        ...this.udtfScopes,
        ...this.derivedTableScopes.filter((dts) => dts.canBeCorrelated),
      ]) {
        externalColumns.push(...scope.externalColumns);
      }

      const namedSelects = new Set(this.expression.namedSelects);

      this._columns = [];
      for (const column of [...columns, ...externalColumns]) {
        const ancestor = column.findAncestor<Expression>(
          SelectExpr,
          QualifyExpr,
          OrderExpr,
          HavingExpr,
          HintExpr,
          TableExpr,
          StarExpr,
          DistinctExpr,
        );
        if (
          !ancestor
          || column.table
          || ancestor instanceof SelectExpr
          || (ancestor instanceof TableExpr && !(ancestor.$this instanceof FuncExpr))
          || (
            (ancestor instanceof OrderExpr || ancestor instanceof DistinctExpr)
            && (
              ancestor.parent instanceof WindowExpr
              || ancestor.parent instanceof WithinGroupExpr
              || !(ancestor.parent instanceof SelectExpr)
              || !namedSelects.has(column.name)
            )
          )
          || (ancestor instanceof StarExpr && column.argKey !== 'except')
        ) {
          this._columns.push(column);
        }
      }
    }
    return this._columns;
  }

  /**
   * Table columns in this scope
   */
  get tableColumns (): TableColumnExpr[] {
    if (this._tableColumns === undefined) {
      this._ensureCollected();
    }
    return this._tableColumns!;
  }

  /**
   * Columns that reference sources in outer scopes
   */
  get externalColumns (): ColumnExpr[] {
    if (this._externalColumns === undefined) {
      this._externalColumns = this.columns.filter(
        (c) => !this.sources.has(c.table) && !this.semiOrAntiJoinTables.has(c.table),
      );
    }
    return this._externalColumns;
  }

  /**
   * Columns that reference sources in the current scope (not external)
   */
  get localColumns (): ColumnExpr[] {
    if (this._localColumns === undefined) {
      const externalSet = new Set(this.externalColumns);
      this._localColumns = this.columns.filter((c) => !externalSet.has(c));
    }
    return this._localColumns;
  }

  /**
   * Unqualified columns in the current scope
   */
  get unqualifiedColumns (): ColumnExpr[] {
    return this.columns.filter((c) => !c.table);
  }

  /**
   * Join hints in this scope
   */
  get joinHints (): JoinHintExpr[] {
    if (this._joinHints === undefined) {
      return [];
    }
    return this._joinHints;
  }

  /**
   * Pivot expressions in this scope
   */
  get pivots (): PivotExpr[] {
    if (!this._pivots) {
      this._pivots = [];
      for (const [, node] of this.references) {
        const pivots = node.args.pivots;
        if (pivots && Array.isArray(pivots)) {
          this._pivots.push(...pivots as PivotExpr[]);
        }
      }
    }
    return this._pivots;
  }

  /**
   * Semi or anti join table names
   */
  get semiOrAntiJoinTables (): Set<string> {
    return this._semiAntiJoinTables || new Set();
  }

  /**
   * List of (name, expression) tuples for all references in this scope
   */
  get references (): [string, Expression][] {
    if (this._references === undefined) {
      this._references = [];

      for (const table of this.tables) {
        this._references.push([table.aliasOrName, table]);
      }

      for (const expression of [...this.derivedTables, ...this.udtfs]) {
        const alias = expression.alias || '';
        const node = expression.getArgKey('pivots') ? expression : expression.unnest();
        this._references.push([alias, node]);
      }
    }
    return this._references;
  }

  /**
   * Mapping of actually selected sources (from FROM/JOIN clauses)
   */
  get selectedSources (): Record<string, [Expression, TableExpr | Scope]> {
    if (this._selectedSources === undefined) {
      this._selectedSources = {};

      for (const [name, node] of this.references) {
        if (this.semiOrAntiJoinTables.has(name)) {
          continue;
        }

        if (name in this._selectedSources) {
          throw new OptimizeError(`Alias already used: ${name}`);
        }

        if (this.sources.has(name)) {
          this._selectedSources[name] = [node, this.sources.get(name)!];
        }
      }
    }
    return this._selectedSources;
  }

  /**
   * Get all columns for a particular source
   */
  sourceColumns (sourceName: string): ColumnExpr[] {
    return this.columns.filter((col) => col.table === sourceName);
  }

  /**
   * Rename a source in this scope
   */
  renameSource (oldName: string, newName: string): void {
    const name = oldName || '';
    if (this.sources.has(name)) {
      const source = this.sources.get(name)!;
      this.sources.delete(name);
      this.sources.set(newName, source);
    }
  }

  /**
   * Add a source to this scope
   */
  addSource (name: string, source: TableExpr | Scope): void {
    this.sources.set(name, source);
    this.clearCache();
  }

  /**
   * Remove a source from this scope
   */
  removeSource (name: string): void {
    this.sources.delete(name);
    this.clearCache();
  }

  /**
   * Traverse the scope tree in depth-first post-order
   */
  * traverse (): Generator<Scope> {
    const stack: Scope[] = [this];
    const result: Scope[] = [];

    while (0 < stack.length) {
      const scope = stack.pop()!;
      result.push(scope);
      stack.push(
        ...scope.cteScopes,
        ...scope.unionScopes,
        ...scope.tableScopes,
        ...scope.subqueryScopes,
      );
    }

    // Yield in reversed order (post-order)
    for (let i = result.length - 1; 0 <= i; i--) {
      yield result[i];
    }
  }

  /**
   * Count the number of times each scope is referenced
   */
  refCount (): Map<Scope | Expression, number> {
    const scopeRefCount = new Map<Scope | Expression, number>();

    for (const scope of this.traverse()) {
      for (const [, source] of Object.values(scope.selectedSources)) {
        if (source instanceof Scope) {
          scopeRefCount.set(source, scopeRefCount.get(scope) || 0 + 1);
        }
      }

      for (const name of scope.semiOrAntiJoinTables) {
        if (scope.sources.has(name)) {
          const source = scope.sources.get(name)!;
          if (source instanceof Scope) {
            scopeRefCount.set(source, (scopeRefCount.get(source) || 0) + 1);
          }
        }
      }
    }

    return scopeRefCount;
  }

  toString (): string {
    return `<Scope ${this.scopeType}>`;
  }
}

/**
 * Traverse scopes in an expression tree
 * @param expression - The root expression to traverse
 * @returns List of all scopes found
 */
export function traverseScope (expression: Expression): Scope[] {
  if (!TRAVERSABLES.some((T) => expression instanceof T)) {
    return [];
  }

  const scopes: Scope[] = [];
  for (const s of _traverseScope(new Scope({ expression: expression as QueryExpr }))) {
    scopes.push(s);
  }
  return scopes;
}

function* _traverseScope (scope: Scope): Generator<Scope> {
  const expression = scope.expression;

  if (expression instanceof SelectExpr) {
    yield* _traverseSelect(scope);
  } else if (expression instanceof SetOperationExpr) {
    yield* _traverseCtes(scope);
    yield* _traverseUnion(scope);
    return;
  } else if (expression instanceof SubqueryExpr) {
    if (scope.isRoot) {
      yield* _traverseSelect(scope);
    } else {
      yield* _traverseSubqueries(scope);
    }
  } else if (expression instanceof TableExpr) {
    yield* _traverseTables(scope);
  } else if (expression instanceof UdtfExpr) {
    yield* _traverseUdtfs(scope);
  } else if (expression instanceof DdlExpr) {
    if (expression.expression instanceof QueryExpr) {
      yield* _traverseCtes(scope);
      yield* _traverseScope(
        new Scope({
          expression: expression.expression as QueryExpr,
          cteSources: scope.cteSources,
        }),
      );
    }
    return;
  } else if (expression instanceof DmlExpr) {
    yield* _traverseCtes(scope);
    for (const query of findAllInScope(expression, [QueryExpr])) {
      const parent = query.parent;
      if (parent && !(parent instanceof CteExpr) && !(parent instanceof SubqueryExpr)) {
        yield* _traverseScope(
          new Scope({
            expression: query,
            cteSources: scope.cteSources,
          }),
        );
      }
    }
    return;
  }

  yield scope;
}

function* _traverseSelect (scope: Scope): Generator<Scope> {
  yield* _traverseCtes(scope);
  yield* _traverseTables(scope);
  yield* _traverseSubqueries(scope);
}

function* _traverseUnion (scope: Scope): Generator<Scope> {
  let prevScope: Scope | undefined;
  const unionScopeStack: Scope[] = [scope];
  const setOp = scope.expression as SetOperationExpr;

  const expressionStack = [setOp.right, setOp.left];

  while (0 < expressionStack.length) {
    const expression = expressionStack.pop()!;
    const unionScope = unionScopeStack[unionScopeStack.length - 1];

    const newScope = unionScope.branch({
      expression,
      outerColumns: unionScope.outerColumns,
      scopeType: ScopeType.UNION,
    });

    if (expression instanceof SetOperationExpr) {
      yield* _traverseCtes(newScope);
      unionScopeStack.push(newScope);
      expressionStack.push(expression.right);
      expressionStack.push(expression.left);
      continue;
    }

    let lastScope: Scope | undefined;
    for (const s of _traverseScope(newScope)) {
      yield s;
      lastScope = s;
    }

    if (prevScope && lastScope) {
      unionScopeStack.pop();
      unionScope.unionScopes = [prevScope, lastScope];
      prevScope = unionScope;
      yield unionScope;
    } else {
      prevScope = lastScope;
    }
  }
}

function* _traverseCtes (scope: Scope): Generator<Scope> {
  const sources = new Map<string, Scope>();

  for (const cte of scope.ctes) {
    const cteName = cte.alias;
    const with_ = scope.expression.args.with;

    if (with_ && with_.$recursive) {
      const union = cte.$this;

      if (union instanceof SetOperationExpr) {
        sources.set(cteName, scope.branch({
          expression: union.$this,
          scopeType: ScopeType.CTE,
        }));
      }
    }

    let childScope: Scope | undefined;
    for (const s of _traverseScope(
      scope.branch({
        expression: cte.$this,
        cteSources: sources,
        outerColumns: cte.aliasColumnNames || [],
        scopeType: ScopeType.CTE,
      }),
    )) {
      yield s;
      childScope = s;
    }

    if (childScope) {
      sources.set(cteName, childScope);
      scope.cteScopes.push(childScope);
    }
  }

  for (const [key, value] of sources) {
    scope.sources.set(key, value);
    scope.cteSources.set(key, value);
  }
}

/**
 * We represent (tbl1 JOIN tbl2) as a Subquery, but it's not really a "derived table",
 * as it doesn't introduce a new scope. If an alias is present, it shadows all names
 * under the Subquery, so that's one exception to this rule.
 */
function _isDerivedTable (expression: SubqueryExpr): boolean {
  return Boolean(
    expression.alias
    || UNWRAPPED_QUERIES.some((T) => expression.$this instanceof T),
  );
}

/**
 * Determine if `expression` is the FROM or JOIN clause of a SELECT statement.
 */
function _isFromOrJoin (expression: Expression): boolean {
  let parent = expression.parent;

  while (parent instanceof SubqueryExpr) {
    parent = parent.parent;
  }

  return parent instanceof FromExpr || parent instanceof JoinExpr;
}

function _getSourceAlias (expression: Expression): string {
  const aliasArg = expression.getArgKey('alias');
  let aliasName = expression.alias;

  if (!aliasName && aliasArg instanceof TableAliasExpr && aliasArg.columns.length === 1) {
    aliasName = (aliasArg.columns[0] as Expression).name;
  }

  return aliasName;
}

function* _traverseTables (scope: Scope): Generator<Scope> {
  const sources = new Map<string, TableExpr | Scope>();

  // Traverse FROMs, JOINs, and LATERALs in the order they are defined
  const expressions: Expression[] = [];
  const from = scope.expression.getArgKey('from') as FromExpr | undefined;

  if (from?.$this) {
    expressions.push(from.$this);
  }

  for (const join of (scope.expression.getArgKey('joins') as JoinExpr[] | undefined) || []) {
    expressions.push(join.$this);
  }

  if (scope.expression instanceof TableExpr) {
    expressions.push(scope.expression);
  }

  for (const lateral of (scope.expression.getArgKey('laterals') as Expression[] | undefined) || []) {
    expressions.push(lateral);
  }

  let i = 0;
  while (i < expressions.length) {
    let expression = expressions[i++];

    if (expression instanceof FinalExpr) {
      expression = expression.getArgKey('this') as Expression;
    }

    if (expression instanceof TableExpr) {
      const tableName = expression.name;
      const sourceName = expression.aliasOrName;

      if (scope.sources.has(tableName) && !expression.db) {
        // This is a reference to a parent source (e.g. a CTE), not an actual table, unless
        // it is pivoted, because then we get back a new table and hence a new source.
        const pivots = expression.$pivots;
        if (pivots && 0 < pivots.length) {
          sources.set(pivots[0].alias, expression);
        } else {
          sources.set(sourceName, scope.sources.get(tableName)!);
        }
      } else if (sources.has(sourceName)) {
        sources.set(findNewName(Array.from(sources.keys()), tableName), expression);
      } else {
        sources.set(sourceName, expression);
      }

      // Make sure to not include the joins twice
      if ((expression as Expression) !== scope.expression) {
        for (const join of expression.$joins || []) {
          expressions.push(join.$this);
        }
      }

      continue;
    }

    if (!(expression instanceof DerivedTableExpr)) {
      continue;
    }

    let lateralSources: Map<string, TableExpr | Scope> | undefined;
    let scopeType: ScopeType;
    let scopes: Scope[];

    if (expression instanceof UdtfExpr) {
      lateralSources = sources;
      scopeType = ScopeType.UDTF;
      scopes = scope.udtfScopes;
    } else if (expression instanceof SubqueryExpr && _isDerivedTable(expression)) {
      lateralSources = undefined;
      scopeType = ScopeType.DERIVED_TABLE;
      scopes = scope.derivedTableScopes;
      for (const join of (expression.getArgKey('joins') as JoinExpr[]) || []) {
        expressions.push(join.$this);
      }
    } else {
      // Makes sure we check for possible sources in nested table constructs
      expressions.push(expression.getArgKey('this') as Expression);
      for (const join of (expression.getArgKey('joins') as JoinExpr[]) || []) {
        expressions.push(join.$this);
      }
      continue;
    }

    let childScope: Scope | undefined;
    for (const s of _traverseScope(
      scope.branch({
        expression,
        lateralSources,
        outerColumns: expression.aliasColumnNames || [],
        scopeType,
      }),
    )) {
      yield s;
      childScope = s;
      // Tables without aliases will be set as "".
      // Until qualify_columns runs (which adds aliases on everything), only a single
      // unaliased derived table is allowed (the latest one wins).
      sources.set(_getSourceAlias(expression), childScope);
    }

    if (childScope) {
      scopes.push(childScope);
      scope.tableScopes.push(childScope);
    }
  }

  for (const [key, value] of sources) {
    scope.sources.set(key, value);
  }
}

function* _traverseSubqueries (scope: Scope): Generator<Scope> {
  for (const subquery of scope.subqueries) {
    let top: Scope | undefined;
    for (const childScope of _traverseScope(
      scope.branch({
        expression: subquery,
        scopeType: ScopeType.SUBQUERY,
      }),
    )) {
      yield childScope;
      top = childScope;
    }
    if (top) {
      scope.subqueryScopes.push(top);
    }
  }
}

function* _traverseUdtfs (scope: Scope): Generator<Scope> {
  let expressions: Expression[];

  if (scope.expression instanceof UnnestExpr) {
    expressions = scope.expression.$expressions;
  } else if (scope.expression instanceof LateralExpr) {
    expressions = [scope.expression.$this];
  } else {
    expressions = [];
  }

  const sources = new Map<string, Scope>();

  for (const expression of expressions) {
    if (!(expression instanceof SubqueryExpr)) {
      continue;
    }

    let top: Scope | undefined;
    for (const childScope of _traverseScope(
      scope.branch({
        expression,
        scopeType: ScopeType.SUBQUERY,
        outerColumns: expression.aliasColumnNames || [],
      }),
    )) {
      yield childScope;
      top = childScope;
      sources.set(_getSourceAlias(expression), childScope);
    }

    if (top) {
      scope.subqueryScopes.push(top);
    }
  }

  for (const [key, value] of sources) {
    scope.sources.set(key, value);
  }
}

/**
 * Build a scope tree from an expression
 * @param expression - The root expression
 * @returns The root scope, or undefined if expression is not traversable
 */
export function buildScope (expression: Expression): Scope | undefined {
  const scopes = traverseScope(expression);
  return seqGet(scopes, -1);
}

/**
 * Walk the expression tree, stopping at nodes that start child scopes.
 *
 * Scope boundaries are: CTEs, derived tables in FROM/JOIN, Query nodes
 * inside UDTF parents, and unwrapped queries (SELECT / SET OPERATION).
 * Boundary nodes themselves are yielded; their subtrees are not traversed.
 * For Subquery / UDTF boundary nodes their joins, laterals, and pivots
 * are still visited via recursive calls.
 */
export function* walkInScope (
  expression: Expression,
  options: { bfs?: boolean;
    prune?: (node: Expression) => boolean; } = {},
): Generator<Expression> {
  const {
    bfs = true, prune,
  } = options;
  let crossedScopeBoundary = false;

  for (const node of expression.walk({
    bfs,
    prune: (n) => crossedScopeBoundary || (prune ? prune(n) : false),
  })) {
    crossedScopeBoundary = false;

    yield node;

    if (node === expression) {
      continue;
    }

    if (
      node instanceof CteExpr
      || (
        (node.parent instanceof FromExpr || node.parent instanceof JoinExpr)
        && node instanceof SubqueryExpr
        && _isDerivedTable(node)
      )
      || (node.parent instanceof UdtfExpr && node instanceof QueryExpr)
      || UNWRAPPED_QUERIES.some((T) => node instanceof T)
    ) {
      crossedScopeBoundary = true;

      if (node instanceof SubqueryExpr || node instanceof UdtfExpr) {
        for (const key of [
          'joins',
          'laterals',
          'pivots',
        ] as const) {
          const args = node.getArgKey(key);
          if (Array.isArray(args)) {
            for (const arg of args as Expression[]) {
              yield* walkInScope(arg, { bfs });
            }
          }
        }
      }
    }
  }
}

/**
 * Find all expressions of given types within a scope
 */
export function findAllInScope<E extends Expression> (
  expression: Expression,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expressionTypes: (new (...args: any) => E)[],
  options: { bfs?: boolean } = {},
): E[] {
  const { bfs = true } = options;
  const results: E[] = [];

  for (const node of walkInScope(expression, { bfs })) {
    for (const ExprType of expressionTypes) {
      if (node instanceof ExprType) {
        results.push(node);
        break;
      }
    }
  }

  return results;
}

/**
 * Find the first expression of given types within a scope
 */
export function findInScope (
  expression: Expression,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expressionTypes: (new (...args: any) => Expression) | (new (...args: any) => Expression)[],
  options: { bfs?: boolean } = {},
): Expression | undefined {
  const { bfs = true } = options;

  for (const node of walkInScope(expression, { bfs })) {
    for (const ExprType of ensureList(expressionTypes)) {
      if (node instanceof ExprType) {
        return node;
      }
    }
  }

  return undefined;
}
