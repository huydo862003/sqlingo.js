// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/qualify_columns.py

import type {
  ColumnDefExpr,
  CteExpr,
  DataTypeExpr,
  SetOperationExpr,
} from '../expressions';
import {
  IdentifierExpr,
  AggFuncExpr,
  Expression,
  AliasesExpr,
  AliasExpr,
  alias as aliasExpr,
  and as andExpr,
  column as columnExpr,
  ColumnExpr,
  CONSTANTS,
  CoalesceExpr,
  DistinctExpr,
  DotExpr,
  ExplodeExpr,
  GroupExpr,
  HavingExpr,
  InExpr,
  LiteralExpr,
  paren as parenExpr,
  PivotExpr,
  PropertyEqExpr,
  PseudocolumnExpr,
  QueryTransformExpr,
  SelectExpr,
  StarExpr,
  StructExpr,
  SubqueryExpr,
  TableAliasExpr,
  TableColumnExpr,
  toIdentifier,
  UnnestExpr,
  WindowExpr,
  WithExpr,
  ParenExpr,
  DataTypeExprKind,
  alias,
} from '../expressions';
import {
  Dialect, type DialectType,
} from '../dialects/dialect';
import {
  ensureSchema, type Schema,
} from '../schema';
import { OptimizeError } from '../errors';
import { seqGet } from '../helper';
import { TypeAnnotator } from './annotate_types';
import { Resolver } from './resolver';
import {
  buildScope, Scope, traverseScope, walkInScope,
} from './scope';
import { simplifyParens } from './simplify';

/**
 * Rewrite sqlglot AST to have fully qualified columns.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { qualifyColumns } from 'sqlglot/optimizer';
 *
 *     const schema = { tbl: { col: "INT" } };
 *     const expression = parseOne("SELECT col FROM tbl");
 *     qualifyColumns(expression, schema).sql();
 *     // 'SELECT tbl.col AS col FROM tbl'
 *     ```
 *
 * @param expression - Expression to qualify
 * @param options - Qualification options
 * @param options.schema - Database schema
 * @param options.expandAliasRefs - Whether to expand references to aliases (default: true)
 * @param options.expandStars - Whether to expand star queries (default: true)
 * @param options.inferSchema - Whether to infer the schema if missing
 * @param options.allowPartialQualification - Whether to allow partial qualification (default: false)
 * @param options.dialect - SQL dialect
 * @returns The qualified expression
 *
 * Notes:
 *     - Currently only handles a single PIVOT or UNPIVOT operator
 */
export function qualifyColumns<E extends Expression> (
  expression: E,
  options: {
    schema?: Record<string, unknown> | Schema;
    expandAliasRefs?: boolean;
    expandStars?: boolean;
    inferSchema?: boolean;
    allowPartialQualification?: boolean;
    dialect?: DialectType;
  } = {},
): E {
  const {
    schema: schemaArg,
    expandAliasRefs = true,
    expandStars = true,
    inferSchema: inferSchemaArg,
    allowPartialQualification = false,
    dialect: dialectArg,
  } = options;

  const schema = ensureSchema(schemaArg, { dialect: dialectArg });
  const annotator = new TypeAnnotator({ schema });
  const inferSchema = inferSchemaArg ?? Boolean(schema.empty);
  const dialect = schema.dialect || new Dialect();
  const dialectClass = dialect._constructor;
  const pseudocolumns = dialectClass.PSEUDOCOLUMNS;

  for (const scope of traverseScope(expression)) {
    if (dialectClass.PREFER_CTE_ALIAS_COLUMN) {
      pushdownCteAliasColumns(scope);
    }

    const scopeExpression = scope.expression;
    const isSelect = scopeExpression instanceof SelectExpr;

    separatePseudocolumns(scope, pseudocolumns);

    const resolver = new Resolver(scope, schema, { inferSchema });
    popTableColumnAliases(scope.ctes);
    popTableColumnAliases(scope.derivedTables);
    const usingColumnTables = expandUsing(scope, resolver);

    if ((schema.empty || dialectClass.FORCE_EARLY_ALIAS_REF_EXPANSION) && expandAliasRefs) {
      expandAliasRefs_(
        scope,
        resolver,
        dialect,
        dialectClass.EXPAND_ONLY_GROUP_ALIAS_REF,
      );
    }

    convertColumnsToDots(scope, resolver);
    qualifyColumnsInScope(
      scope,
      resolver,
      allowPartialQualification,
    );

    if (!schema.empty && expandAliasRefs) {
      expandAliasRefs_(scope, resolver, dialect, false);
    }

    if (isSelect) {
      if (expandStars) {
        expandStars_(
          scope,
          resolver,
          usingColumnTables,
          pseudocolumns,
          annotator,
        );
      }
      qualifyOutputs(scope);
    }

    expandGroupBy(scope, dialect);
    expandOrderByAndDistinctOn(scope, resolver);

    if (dialectClass.ANNOTATE_ALL_SCOPES) {
      annotator.annotateScope(scope);
    }
  }

  return expression;
}

/**
 * Raise an error if any columns aren't qualified
 *
 * @param expression - Expression to validate
 * @param sql - Optional SQL string for error highlighting
 * @returns The validated expression
 * @throws OptimizeError if unqualified columns are found
 */
export function validateQualifyColumns<E extends Expression> (
  expression: E,
  _sql?: string,
): E {
  const allUnqualifiedColumns: ColumnExpr[] = [];

  for (const scope of traverseScope(expression)) {
    if (!(scope.expression instanceof SelectExpr)) {
      continue;
    }

    let unqualifiedColumns = scope.unqualifiedColumns;

    if (0 < scope.externalColumns.length && !scope.isCorrelatedSubquery && scope.pivots.length === 0) {
      const column = scope.externalColumns[0];
      const forTable = column.table ? ` for table: '${column.table}'` : '';
      const line = (column.this as Expression)?.meta?.['line'] as number | undefined;
      const col = (column.this as Expression)?.meta?.['col'] as number | undefined;

      let errorMsg = `Column '${column.name}' could not be resolved${forTable}.`;
      if (line && col) {
        errorMsg += ` Line: ${line}, Col: ${col}`;
      }

      throw new OptimizeError(errorMsg);
    }

    if (0 < unqualifiedColumns.length && 0 < scope.pivots.length && scope.pivots[0].unpivot) {
      const unpivotColumnSet = new Set(unpivotColumns(scope.pivots[0]));
      unqualifiedColumns = unqualifiedColumns.filter((c) => !unpivotColumnSet.has(c));
    }

    allUnqualifiedColumns.push(...unqualifiedColumns);
  }

  if (0 < allUnqualifiedColumns.length) {
    const firstColumn = allUnqualifiedColumns[0];
    const line = firstColumn.$this.meta['line'];
    const col = firstColumn.$this.meta['col'];

    let errorMsg = `Ambiguous column '${firstColumn.name}'`;
    if (line && col) {
      errorMsg += ` (Line: ${line}, Col: ${col})`;
    }

    throw new OptimizeError(errorMsg);
  }

  return expression;
}

function separatePseudocolumns (scope: Scope, pseudocolumns: Set<string>): void {
  if (pseudocolumns.size === 0) {
    return;
  }

  let hasPseudocolumns = false;
  const scopeExpression = scope.expression;

  for (const column of scope.columns) {
    const name = column.name.toUpperCase();
    if (!pseudocolumns.has(name)) {
      continue;
    }

    if (name !== 'LEVEL' || (
      scopeExpression instanceof SelectExpr
      && (scopeExpression.args as Record<string, unknown>).connect
    )) {
      column.replace(new PseudocolumnExpr({ ...column.args }));
      hasPseudocolumns = true;
    }
  }

  if (hasPseudocolumns) {
    scope.clearCache();
  }
}

function unpivotColumns (unpivot: PivotExpr): ColumnExpr[] {
  const fields = unpivot.$fields || [];
  const nameColumns = fields
    .filter((field): field is InExpr => field instanceof InExpr && field.this instanceof ColumnExpr)
    .map((field) => field.this as ColumnExpr);

  const valueColumns: ColumnExpr[] = [];
  for (const e of unpivot.expressions as Expression[]) {
    for (const col of e.findAll(ColumnExpr)) {
      valueColumns.push(col);
    }
  }

  return [...nameColumns, ...valueColumns];
}

function popTableColumnAliases (derivedTables: Expression[]): void {
  for (const table of derivedTables) {
    if (table.parent instanceof WithExpr && table.parent.$recursive) {
      continue;
    }
    const tableAlias = table.getArgKey('alias');
    if (tableAlias instanceof TableAliasExpr) {
      tableAlias.args.columns = undefined;
    }
  }
}

function expandUsing (scope: Scope, resolver: Resolver): Map<string, string[]> {
  const joins = (scope.expression as SelectExpr).args.joins || [];
  if (joins.length === 0) {
    return new Map();
  }

  const names = new Set(joins.map((j) => j.aliasOrName));
  const ordered: string[] = Object.keys(scope.selectedSources).filter((k) => !names.has(k));

  if (0 < names.size && ordered.length === 0) {
    throw new OptimizeError(`Joins ${[...names].join(',')} missing source table ${scope.expression}`);
  }

  // column name -> first source name that has it
  const columns: Record<string, string> = {};

  const updateSourceColumns = (sourceName: string): void => {
    for (const colName of resolver.getSourceColumns(sourceName)) {
      if (!(colName in columns)) {
        columns[colName] = sourceName;
      }
    }
  };

  for (const sourceName of ordered) {
    updateSourceColumns(sourceName);
  }

  // column name -> ordered map of table names
  const columnTables = new Map<string, string[]>();

  for (let i = 0; i < joins.length; i++) {
    const join = joins[i];
    const sourceTable = ordered[ordered.length - 1];
    if (sourceTable) {
      updateSourceColumns(sourceTable);
    }

    const joinTable = join.aliasOrName;
    ordered.push(joinTable);

    const using = join.$using;
    if (!using) continue;

    const joinColumns = resolver.getSourceColumns(joinTable);

    const conditions: Expression[] = [];
    const usingIdentifierCount = using.length;
    const isSemiOrAntiJoin = join.isSemiOrAntiJoin;

    for (const identifierNode of using) {
      const identifier = (identifierNode as Expression).name;
      let table = columns[identifier];

      if (!table || !joinColumns.includes(identifier)) {
        if (0 < Object.keys(columns).length && !('*' in columns) && 0 < joinColumns.length) {
          throw new OptimizeError(`Cannot automatically join: ${identifier}`);
        }
      }

      table = table || sourceTable;

      let lhs: Expression;
      if (i === 0 || usingIdentifierCount === 1) {
        lhs = columnExpr({
          col: identifier,
          table,
        });
      } else {
        const coalesceColumns = ordered.slice(0, -1)
          .filter((t) => resolver.getSourceColumns(t).includes(identifier))
          .map((t) => columnExpr({
            col: identifier,
            table: t,
          }));
        if (1 < coalesceColumns.length) {
          lhs = new CoalesceExpr({
            this: coalesceColumns[0],
            expressions: coalesceColumns.slice(1),
          });
        } else {
          lhs = columnExpr({
            col: identifier,
            table,
          });
        }
      }

      conditions.push(lhs.eq(columnExpr({
        col: identifier,
        table: joinTable,
      })));

      if (!isSemiOrAntiJoin) {
        if (!columnTables.has(identifier)) {
          columnTables.set(identifier, []);
        }
        const tables_ = columnTables.get(identifier)!;
        if (!tables_.includes(table)) tables_.push(table);
        if (!tables_.includes(joinTable)) tables_.push(joinTable);
      }
    }

    join.args.using = undefined;
    join.args.on = andExpr(conditions, { copy: false });
  }

  if (0 < columnTables.size) {
    for (const column of scope.columns) {
      if (!column.table && columnTables.has(column.name)) {
        const tables_ = columnTables.get(column.name)!;
        const coalesceArgs = tables_.map((t) => columnExpr({
          col: column.name,
          table: t,
        }));
        let replacement: Expression = new CoalesceExpr({
          this: coalesceArgs[0],
          expressions: coalesceArgs.slice(1),
        });

        if (column.parent instanceof SelectExpr) {
          replacement = aliasExpr(replacement, column.name, { copy: false }) as Expression;
        } else if (column.parent instanceof StructExpr) {
          replacement = new PropertyEqExpr({
            this: toIdentifier(column.name),
            expression: replacement,
          });
        }

        scope.replace(column, replacement);
      }
    }
  }

  return columnTables;
}

function expandAliasRefs_ (
  scope: Scope,
  resolver: Resolver,
  dialect: Dialect,
  expandOnlyGroupby: boolean,
): void {
  const expression = scope.expression;
  const dialectClass = dialect._constructor;

  if (!(expression instanceof SelectExpr) || dialectClass.DISABLES_ALIAS_REF_EXPANSION) {
    return;
  }

  const aliasToExpression = new Map<string, [Expression, number]>();
  const projections = new Set(expression.selects.map((s) => (s as Expression).aliasOrName));
  let replaced = false;

  const replaceColumns = (
    node: Expression | undefined,
    options: { resolveTable?: boolean;
      literalIndex?: boolean; } = {},
  ): void => {
    const {
      resolveTable = false, literalIndex = false,
    } = options;
    const isGroupBy = node instanceof GroupExpr;
    const isHaving = node instanceof HavingExpr;
    if (!node || (expandOnlyGroupby && !isGroupBy)) {
      return;
    }

    for (const column of walkInScope(node, { prune: (n) => n.isStar })) {
      if (!(column instanceof ColumnExpr)) continue;

      if (expandOnlyGroupby && isGroupBy && column.parent !== node) {
        continue;
      }

      let skipReplace = false;
      const table = (resolveTable && !column.table) ? resolver.getTable(column.name) : undefined;
      const aliasEntry = aliasToExpression.get(column.name);
      const aliasExpr_ = aliasEntry ? aliasEntry[0] : undefined;
      const aliasIdx = aliasEntry ? aliasEntry[1] : 1;

      if (aliasExpr_) {
        skipReplace = Boolean(
          aliasExpr_.find(AggFuncExpr)
          && column.findAncestor(AggFuncExpr)
          && !(column.findAncestor<WindowExpr | SelectExpr>(WindowExpr, SelectExpr) instanceof WindowExpr),
        );

        if (isHaving && dialectClass.PROJECTION_ALIASES_SHADOW_SOURCE_NAMES) {
          skipReplace = skipReplace || Array.from(aliasExpr_.findAll(ColumnExpr)).some(
            (n) => projections.has(n.parts[0]?.name || ''),
          );
        }
      } else if (dialectClass.PROJECTION_ALIASES_SHADOW_SOURCE_NAMES && (isGroupBy || isHaving)) {
        const columnTable = table ? table.name : column.table;
        if (columnTable && projections.has(columnTable)) {
          column.replace(toIdentifier(column.name));
          replaced = true;
          continue;
        }
      }

      if (table && (!aliasExpr_ || skipReplace)) {
        column.args.table = table as IdentifierExpr;
      } else if (!column.table && aliasExpr_ && !skipReplace) {
        if ((aliasExpr_ instanceof LiteralExpr || aliasExpr_.isNumber) && (literalIndex || resolveTable)) {
          if (literalIndex) {
            column.replace(LiteralExpr.number(aliasIdx));
            replaced = true;
          }
        } else {
          replaced = true;
          const parenNode = column.replace(parenExpr(aliasExpr_)) as Expression;
          const simplified = simplifyParens(parenNode, dialect);
          if (simplified !== parenNode) {
            parenNode.replace(simplified);
          }
        }
      }
    }
  };

  for (let i = 0; i < expression.selects.length; i++) {
    const projection = expression.selects[i] as Expression;
    replaceColumns(projection);
    if (projection instanceof AliasExpr) {
      aliasToExpression.set(projection.alias, [projection.this as Expression, i + 1]);
    }
  }

  // Handle recursive CTE alias columns
  let parentScope: Scope | undefined = scope;
  let onRightSubTree = false;
  while (parentScope && !parentScope.isCte) {
    if (parentScope.isUnion && parentScope.parent) {
      onRightSubTree = (parentScope.parent.expression as SetOperationExpr).args.expression === parentScope.expression;
    }
    parentScope = parentScope.parent;
  }

  if (parentScope && onRightSubTree) {
    const cteExpr = parentScope.expression.parent;
    if (cteExpr) {
      const withNode = cteExpr.findAncestor(WithExpr);
      if (withNode?.$recursive) {
        const aliasArg = (cteExpr as CteExpr).args.alias;
        const aliasColumns = aliasArg instanceof TableAliasExpr ? aliasArg.columns : [];
        const columnsSource: Expression[] = 0 < aliasColumns.length
          ? aliasColumns as Expression[]
          : ((cteExpr as CteExpr).$this as SelectExpr)?.selects || [];
        for (const col of columnsSource) {
          if (col instanceof Expression) {
            aliasToExpression.delete(col.outputName);
          }
        }
      }
    }
  }

  replaceColumns(expression.args.where as Expression | undefined);
  replaceColumns(expression.args.group as Expression | undefined, { literalIndex: true });
  replaceColumns(expression.args.having as Expression | undefined, { resolveTable: true });
  replaceColumns(expression.args.qualify as Expression | undefined, { resolveTable: true });

  if (dialectClass.SUPPORTS_ALIAS_REFS_IN_JOIN_CONDITIONS) {
    for (const join of expression.args.joins || []) {
      replaceColumns(join);
    }
  }

  if (replaced) {
    scope.clearCache();
  }
}

function convertColumnsToDots (scope: Scope, resolver: Resolver): void {
  let converted = false;
  const allCols: (ColumnExpr | DotExpr)[] = [...scope.columns, ...scope.stars];

  for (const column of allCols) {
    if (column instanceof DotExpr) continue;

    const columnTable = column.table;
    const dotParts = (column.meta['dotParts'] as unknown[] | undefined) || [];
    delete column.meta['dotParts'];

    if (
      columnTable
      && !scope.sources.has(columnTable)
      && (
        !scope.parent
        || !scope.parent.sources.has(columnTable)
        || !scope.isCorrelatedSubquery
      )
    ) {
      const parts = column.parts;
      if (parts.length < 2) continue;

      const firstPart = parts[0];
      const remainingParts = parts.slice(1);
      if (!firstPart) continue;

      let newRoot: IdentifierExpr;
      let fieldParts: Expression[];
      let newTable: Expression | undefined;
      let wasQualified: boolean;

      if (scope.sources.has(firstPart.name)) {
        // Column is already table-qualified: firstPart is the table, remainingParts[0] is column
        if (remainingParts.length === 0) continue;
        newTable = firstPart;
        newRoot = remainingParts[0] as IdentifierExpr;
        fieldParts = remainingParts.slice(1);
        wasQualified = true;
      } else {
        // firstPart is the column name, resolver finds the table
        newTable = resolver.getTable(firstPart.name);
        newRoot = firstPart as IdentifierExpr;
        fieldParts = remainingParts;
        wasQualified = false;
      }

      if (newTable) {
        converted = true;
        const newColumn = columnExpr({
          col: newRoot,
          table: newTable as IdentifierExpr,
        });
        if (0 < dotParts.length) {
          newColumn.meta['dotParts'] = dotParts.slice(wasQualified ? 2 : 1);
        }
        if (0 < fieldParts.length) {
          column.replace(DotExpr.build([newColumn, ...fieldParts]));
        } else {
          column.replace(newColumn);
        }
      }
    }
  }

  if (converted) {
    scope.clearCache();
  }
}

function qualifyColumnsInScope (
  scope: Scope,
  resolver: Resolver,
  allowPartialQualification: boolean,
): void {
  const dialectClass = resolver.dialect._constructor;

  for (const column of scope.columns) {
    const columnTable = column.table;
    const columnName = column.name;

    if (columnTable && scope.sources.has(columnTable)) {
      const sourceColumns = resolver.getSourceColumns(columnTable);
      if (
        !allowPartialQualification
        && 0 < sourceColumns.length
        && !sourceColumns.includes(columnName)
        && !sourceColumns.includes('*')
      ) {
        throw new OptimizeError(`Unknown column: ${columnName}`);
      }
    }

    if (!columnTable) {
      if (0 < scope.pivots.length && !column.findAncestor(PivotExpr)) {
        column.args.table = toIdentifier((scope.pivots[0] as PivotExpr).alias);
        continue;
      }

      const table = resolver.getTable(column);

      if (
        table
        && scope.sources.get(table.name) instanceof Scope
        && (scope.sources.get(table.name) as Scope).columnIndex.has(Object.keys(column).length)
      ) {
        continue;
      }

      if (table) {
        column.args.table = table as IdentifierExpr;
      } else if (
        dialectClass.TABLES_REFERENCEABLE_AS_COLUMNS
        && column.parts.length === 1
        && columnName in scope.selectedSources
      ) {
        scope.replace(column, new TableColumnExpr({ this: column.$this }));
      }
    }
  }

  for (const pivot of scope.pivots) {
    if (pivot instanceof PivotExpr) {
      for (const column of pivot.findAll(ColumnExpr)) {
        if (!column.table && resolver.allColumns.has(column.name)) {
          const table = resolver.getTable(column.name);
          if (table) {
            column.args.table = table as IdentifierExpr;
          }
        }
      }
    }
  }
}

function addExceptColumns (
  expression: Expression,
  tables: string[],
  exceptColumns: Map<string, Set<string>>,
): void {
  const except_ = expression.getArgKey('except') ?? expression.getArgKey('except');
  if (!except_) return;
  const exceptList = Array.isArray(except_) ? except_ : [except_];
  const columns = new Set(
    (exceptList as Expression[])
      .filter((e): e is Expression => e instanceof Expression)
      .map((e) => e.name),
  );
  for (const table of tables) {
    exceptColumns.set(table, columns);
  }
}

function addRenameColumns (
  expression: Expression,
  tables: string[],
  renameColumns: Map<string, Record<string, string>>,
): void {
  const rename = expression.getArgKey('rename') as Expression[] | undefined;
  if (!rename || rename.length === 0) return;
  const columns: Record<string, string> = {};
  for (const e of rename) {
    if (e instanceof Expression) {
      const thisExpr = e.this as Expression | undefined;
      if (thisExpr instanceof Expression) {
        columns[thisExpr.name] = (e as Expression).alias;
      }
    }
  }
  for (const table of tables) {
    renameColumns.set(table, columns);
  }
}

function addReplaceColumns (
  expression: Expression,
  tables: string[],
  replaceColumns: Map<string, Record<string, AliasExpr>>,
): void {
  const replace = expression.getArgKey('replace') as Expression[] | undefined;
  if (!replace || replace.length === 0) return;
  const columns: Record<string, AliasExpr> = {};
  for (const e of replace) {
    if (e instanceof AliasExpr) {
      columns[e.alias] = e;
    }
  }
  for (const table of tables) {
    replaceColumns.set(table, columns);
  }
}

function expandStructStarsNoParens (expression: DotExpr): AliasExpr[] {
  const dotColumn = expression.find(ColumnExpr);
  if (!(dotColumn instanceof ColumnExpr) || !dotColumn.isType(DataTypeExprKind.STRUCT)) {
    return [];
  }

  // All nested struct values are ColumnDefs, so normalize the first Column in one
  const dotColumnCopy = dotColumn.copy();
  let startingStruct: IdentifierExpr | DotExpr | DataTypeExpr | ColumnDefExpr | undefined = dotColumnCopy.type;

  // First part is the table name and last part is the star so they can be dropped
  const dotParts = expression.parts.slice(1, -1);

  // If we're expanding a nested struct eg. t.c.f1.f2.* find the last struct (f2 in this case)
  outer: for (const part of dotParts.slice(1)) {
    const fieldExprs = startingStruct?.$expressions || [];
    for (const field of fieldExprs) {
      // Unable to expand star unless all fields are named
      if (!(field.this instanceof IdentifierExpr)) {
        return [];
      }

      if (!('$kind' in field) || !(field.$kind instanceof Expression)) {
        return [];
      }

      const fieldKind = field.$kind;

      if (field.name === part.name && fieldKind?.isType(DataTypeExprKind.STRUCT)) {
        startingStruct = fieldKind as typeof startingStruct;
        break outer;
      }
    }
    // There is no matching field in the struct
    return [];
  }

  const takenNames = new Set<string>();
  const newSelections: AliasExpr[] = [];

  for (const field of (startingStruct?.$expressions || [])) {
    const name = field.name;
    const fieldThis = field.this;

    // Ambiguous or anonymous fields can't be expanded
    if (takenNames.has(name) || !(fieldThis instanceof IdentifierExpr)) {
      return [];
    }

    takenNames.add(name);

    const thisIdent = fieldThis.copy() as IdentifierExpr;
    const allParts = [...dotParts.map((p) => p.copy()), thisIdent];
    const [root, ...parts] = allParts;

    const newColumn = columnExpr(
      {
        col: root as IdentifierExpr,
        table: dotColumnCopy.args.table as IdentifierExpr | undefined,
      },
      { fields: parts as IdentifierExpr[] },
    );

    newSelections.push(aliasExpr(newColumn, thisIdent, { copy: false }) as AliasExpr);
  }

  return newSelections;
}

function expandStructStarsWithParens (expression: DotExpr): AliasExpr[] {
  if (!(expression.$this instanceof ParenExpr)) {
    return [];
  }

  const dotColumn = expression.find(ColumnExpr);
  if (!(dotColumn instanceof ColumnExpr) || !dotColumn.isType(DataTypeExprKind.STRUCT)) {
    return [];
  }

  let parent = dotColumn.parent;
  let startingStruct: string | ColumnDefExpr | DotExpr | IdentifierExpr | DataTypeExpr | undefined = dotColumn.type;

  while (parent !== undefined) {
    if (parent instanceof ParenExpr) {
      parent = parent.parent;
      continue;
    }

    if (!(parent instanceof DotExpr)) {
      return [];
    }

    const rhs = parent.right;
    if (rhs instanceof StarExpr) {
      break;
    }

    if (!(rhs instanceof IdentifierExpr)) {
      return [];
    }

    let matched = false;
    const expressions: (DataTypeExpr | ColumnDefExpr)[] = (startingStruct as DataTypeExpr).$expressions || [];
    for (const structFieldDef of expressions) {
      if (structFieldDef.name === rhs.name) {
        matched = true;
        startingStruct = structFieldDef.$kind;
        break;
      }
    }

    if (!matched) return [];

    parent = parent.parent;
  }

  const newSelections = [];

  const outerParen = expression.$this;

  const expressions: (DataTypeExpr | ColumnDefExpr)[] = (startingStruct as DataTypeExpr).$expressions || [];
  for (const structFieldDef of expressions) {
    const newIdentifier = structFieldDef.$this instanceof IdentifierExpr ? structFieldDef.$this.copy() : new IdentifierExpr({ this: structFieldDef.$this.toString() });
    const newDot = DotExpr.build([outerParen.copy(), newIdentifier]);
    const newAlias = alias(newDot, newIdentifier, { copy: false }) as AliasExpr;
    newSelections.push(newAlias);
  }

  return newSelections;
}

function expandStars_ (
  scope: Scope,
  resolver: Resolver,
  usingColumnTables: Map<string, string[]>,
  pseudocolumns: Set<string>,
  annotator: TypeAnnotator,
): void {
  const newSelections: Expression[] = [];
  const exceptColumns = new Map<string, Set<string>>();
  const replaceColumnsMap = new Map<string, Record<string, AliasExpr>>();
  const renameColumnsMap = new Map<string, Record<string, string>>();
  const coalesedColumns = new Set<string>();
  const dialectClass = resolver.dialect._constructor;

  let pivotOutputColumns: string[] | undefined = undefined;
  const pivotExcludeColumns = new Set<string>();

  const pivot = seqGet(scope.pivots, 0);
  if (pivot instanceof PivotExpr && !pivot.aliasColumnNames.length) {
    if (pivot.unpivot) {
      pivotOutputColumns = unpivotColumns(pivot).map((c) => c.outputName);

      for (const field of pivot.$fields || []) {
        if (field instanceof InExpr) {
          for (const e of field.expressions as Expression[]) {
            for (const c of (e as Expression).findAll(ColumnExpr)) {
              pivotExcludeColumns.add(c.outputName);
            }
          }
        }
      }
    } else {
      for (const c of pivot.findAll(ColumnExpr)) {
        pivotExcludeColumns.add(c.outputName);
      }

      const pivotColumns = pivot.getArgKey('columns') as Expression[] | undefined;
      pivotOutputColumns = (pivotColumns || []).map((c) => (c as Expression).outputName);
      if (!pivotOutputColumns.length) {
        pivotOutputColumns = (pivot.expressions as Expression[]).map((c) => c.aliasOrName);
      }
    }
  }

  if (dialectClass.SUPPORTS_STRUCT_STAR_EXPANSION && scope.stars.some((col) => col instanceof DotExpr)) {
    annotator.annotateScope(scope);
  }

  for (const expression of scope.expression.selects) {
    const tables: string[] = [];

    if (expression instanceof StarExpr) {
      tables.push(...Object.keys(scope.selectedSources));
      addExceptColumns(expression, tables, exceptColumns);
      addReplaceColumns(expression, tables, replaceColumnsMap);
      addRenameColumns(expression, tables, renameColumnsMap);
    } else if ((expression as Expression).isStar) {
      if (!(expression instanceof DotExpr)) {
        const tableName = expression instanceof ColumnExpr ? expression.table : '';
        if (tableName) tables.push(tableName);
        const exprThis = (expression as Expression).this;
        if (exprThis instanceof Expression) {
          addExceptColumns(exprThis, tables, exceptColumns);
          addReplaceColumns(exprThis, tables, replaceColumnsMap);
          addRenameColumns(exprThis, tables, renameColumnsMap);
        }
      } else if (dialectClass.SUPPORTS_STRUCT_STAR_EXPANSION && !dialectClass.REQUIRES_PARENTHESIZED_STRUCT_ACCESS) {
        const structFields = expandStructStarsNoParens(expression as DotExpr);
        if (0 < structFields.length) {
          newSelections.push(...structFields);
          continue;
        }
      } else if (dialectClass.REQUIRES_PARENTHESIZED_STRUCT_ACCESS) {
        const structFields = expandStructStarsWithParens(expression as DotExpr);
        if (0 < structFields.length) {
          newSelections.push(...structFields);
          continue;
        }
      }
    }

    if (tables.length === 0) {
      newSelections.push(expression as Expression);
      continue;
    }

    for (const table of tables) {
      if (!scope.sources.has(table)) {
        throw new OptimizeError(`Unknown table: ${table}`);
      }

      let columns = resolver.getSourceColumns(table, { onlyVisible: true });
      if (!columns.length) columns = scope.outerColumns;

      if (0 < pseudocolumns.size && dialectClass.EXCLUDES_PSEUDOCOLUMNS_FROM_STAR) {
        columns = columns.filter((name) => !pseudocolumns.has(name.toUpperCase()));
      }

      if (!columns.length || columns.includes('*')) {
        return;
      }

      const columnsToExclude = exceptColumns.get(table) || new Set<string>();
      const renamedColumns = renameColumnsMap.get(table) || {};
      const replacedColumns = replaceColumnsMap.get(table) || {};

      if (pivot instanceof PivotExpr) {
        let pivotColumns: string[] | undefined;
        if (pivotOutputColumns && 0 < pivotExcludeColumns.size) {
          pivotColumns = columns.filter((c) => !pivotExcludeColumns.has(c));
          pivotColumns.push(...pivotOutputColumns);
        } else {
          pivotColumns = pivot.aliasColumnNames;
        }

        if (0 < pivotColumns.length) {
          for (const name of pivotColumns) {
            if (!columnsToExclude.has(name)) {
              newSelections.push(
                aliasExpr(columnExpr({
                  col: name,
                  table: pivot.alias,
                }), name, { copy: false }) as Expression,
              );
            }
          }
          continue;
        }
      }

      for (const name of columns) {
        if (columnsToExclude.has(name) || coalesedColumns.has(name)) continue;

        if (usingColumnTables.has(name) && usingColumnTables.get(name)!.includes(table)) {
          coalesedColumns.add(name);
          const tablesForCol = usingColumnTables.get(name)!;
          const coalesceArgs = tablesForCol.map((t) => columnExpr({
            col: name,
            table: t,
          }));
          newSelections.push(
            aliasExpr(
              new CoalesceExpr({
                this: coalesceArgs[0],
                expressions: coalesceArgs.slice(1),
              }),
              name,
              { copy: false },
            ) as Expression,
          );
        } else {
          const alias_ = renamedColumns[name] ?? name;
          const selectionExpr = replacedColumns[name] || columnExpr({
            col: name,
            table,
          });
          newSelections.push(
            alias_ !== name
              ? aliasExpr(selectionExpr, alias_, { copy: false }) as Expression
              : selectionExpr,
          );
        }
      }
    }
  }

  if (0 < newSelections.length && scope.expression instanceof SelectExpr) {
    scope.expression.args.expressions = newSelections;
  }
}

export function qualifyOutputs (scopeOrExpression: Scope | Expression): void {
  let scopeInstance: Scope;

  if (scopeOrExpression instanceof Scope) {
    scopeInstance = scopeOrExpression;
  } else {
    const built = buildScope(scopeOrExpression);
    if (!(built instanceof Scope)) return;
    scopeInstance = built;
  }

  if (!(scopeInstance.expression instanceof SelectExpr)) {
    return;
  }

  const selects = scopeInstance.expression.selects;
  const outerColumns = scopeInstance.outerColumns;
  const newSelections: Expression[] = [];
  const maxLen = Math.max(selects.length, outerColumns.length);

  for (let i = 0; i < maxLen; i++) {
    let selection = selects[i] as Expression | undefined;
    const aliasedColumn = outerColumns[i];

    if (!selection || selection instanceof QueryTransformExpr) {
      break;
    }

    if (selection instanceof SubqueryExpr) {
      if (!selection.outputName) {
        selection.args.alias = new TableAliasExpr({ this: toIdentifier(`_col_${i}`) });
      }
    } else if (!(selection instanceof AliasExpr) && !(selection instanceof AliasesExpr) && !selection.isStar) {
      selection = aliasExpr(
        selection,
        selection.outputName || `_col_${i}`,
        { copy: false },
      ) as Expression;
    }

    if (aliasedColumn) {
      selection.setArgKey('alias', toIdentifier(aliasedColumn));
    }

    newSelections.push(selection);
  }

  if (0 < newSelections.length && scopeInstance.expression instanceof SelectExpr) {
    scopeInstance.expression.args.expressions = newSelections;
  }
}

function selectByPos (scope: Scope, node: LiteralExpr): AliasExpr {
  const index = Number(node.this) - 1;
  const select = scope.expression.selects[index] as Expression | undefined;
  if (!(select instanceof AliasExpr)) {
    throw new OptimizeError(`Unknown output column: ${node.name}`);
  }
  return select;
}

function expandPositionalReferences (
  scope: Scope,
  expressions: Expression[],
  dialect: Dialect,
  alias: boolean = false,
): Expression[] {
  const dialectClass = dialect._constructor;
  const newNodes: Expression[] = [];
  let ambiguousProjections: Set<string> | undefined;

  for (const node of expressions) {
    if (node.isInt) {
      const select = selectByPos(scope, node as LiteralExpr);
      if (alias) {
        const selectAlias = select.alias;
        newNodes.push(selectAlias ? columnExpr({ col: selectAlias }) : node);
      } else {
        const selectThis = select.this as Expression;
        let ambiguous = false;

        if (dialectClass.PROJECTION_ALIASES_SHADOW_SOURCE_NAMES) {
          if (ambiguousProjections === undefined) {
            ambiguousProjections = new Set(
              scope.expression.selects
                .map((s) => s.aliasOrName)
                .filter((name) => name in scope.selectedSources),
            );
          }
          ambiguous = Array.from(selectThis.findAll(ColumnExpr)).some(
            (col) => ambiguousProjections!.has(col.parts[0]?.name || ''),
          );
        }

        if (
          CONSTANTS.some((C) => selectThis instanceof C)
          || selectThis.isNumber
          || selectThis.find(ExplodeExpr)
          || selectThis.find(UnnestExpr)
          || ambiguous
        ) {
          newNodes.push(node);
        } else {
          newNodes.push(selectThis.copy());
        }
      }
    } else {
      newNodes.push(node);
    }
  }

  return newNodes;
}

function expandGroupBy (scope: Scope, dialect: Dialect): void {
  const group = (scope.expression as SelectExpr).args.group;
  if (!group) return;

  const groupExpressions = group.args.expressions || [];
  group.args.expressions = expandPositionalReferences(scope, groupExpressions, dialect);
  (scope.expression as SelectExpr).args.group = group;
}

function expandOrderByAndDistinctOn (scope: Scope, resolver: Resolver): void {
  for (const modifierKey of ['order', 'distinct']) {
    let modifier = scope.expression.getArgKey(modifierKey) as Expression | undefined;

    if (modifier instanceof DistinctExpr) {
      modifier = modifier.$on;
    }

    if (!(modifier instanceof Expression)) {
      continue;
    }

    let modifierExpressions = modifier.expressions as Expression[];

    if (modifierKey === 'order') {
      modifierExpressions = modifierExpressions.map(
        (ordered) => ordered.this as Expression,
      );
    }

    const expanded = expandPositionalReferences(scope, modifierExpressions, resolver.dialect, true);

    for (let j = 0; j < modifierExpressions.length; j++) {
      const original = modifierExpressions[j];
      const expandedNode = expanded[j];

      for (const agg of original.findAll(AggFuncExpr)) {
        for (const col of agg.findAll(ColumnExpr)) {
          if (!col.table) {
            const table = resolver.getTable(col.name);
            if (table) {
              col.setArgKey('table', table);
            }
          }
        }
      }

      original.replace(expandedNode);
    }

    if (scope.expression.getArgKey('group')) {
      const selectsMap: Array<{
        key: Expression;
        value: Expression;
      }> = scope.expression.selects
        .filter((s): s is AliasExpr => s instanceof AliasExpr)
        .map((s) => ({
          key: s.this as Expression,
          value: columnExpr({ col: s.aliasOrName }),
        }));

      for (const expr of modifierExpressions) {
        if (expr.isInt) {
          expr.replace(toIdentifier(selectByPos(scope, expr as LiteralExpr).alias));
        } else {
          const match = selectsMap.find((entry) => entry.key === expr);
          if (match) expr.replace(match.value);
        }
      }
    }
  }
}

export function quoteIdentifiers (
  expression: Expression,
  options: {
    dialect?: DialectType;
    identify?: boolean;
  } = {},
): Expression {
  const {
    dialect: dialectArg, identify = true,
  } = options;
  const dialect = Dialect.getOrRaise(dialectArg);
  return expression.transform(
    Dialect.getOrRaise(dialect).quoteIdentifier,
    {
      identify,
      copy: false,
    },
  );
}

export function pushdownCteAliasColumns (scope: Scope): void {
  for (const cte of scope.ctes) {
    const aliasColumnNames = cte.aliasColumnNames;
    if (0 < aliasColumnNames.length && cte.$this instanceof SelectExpr) {
      const selectExpr = cte.$this;
      const expressions = selectExpr.args.expressions || [];
      const newExpressions: Expression[] = [];

      for (let i = 0; i < aliasColumnNames.length; i++) {
        const alias_ = aliasColumnNames[i];
        const projection = expressions[i];
        if (!projection) break;

        let newProjection: Expression;
        if (projection instanceof AliasExpr) {
          projection.args.alias = toIdentifier(alias_);
          newProjection = projection;
        } else {
          newProjection = aliasExpr(projection, alias_) as Expression;
        }
        newExpressions.push(newProjection);
      }

      selectExpr.setArgKey('expressions', newExpressions);
    }
  }
}
