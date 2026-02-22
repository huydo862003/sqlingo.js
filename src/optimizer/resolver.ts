// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/resolver.py

import type { DataTypeExpr } from '../expressions';
import {
  Expression,
  IdentifierExpr,
  JoinExprKind,
  SetOperationExprKind,
  ColumnExpr,
  DataTypeExprKind,
  JoinExpr,
  QueryExpr,
  SelectExpr,
  SetOperationExpr,
  SubqueryExpr,
  TableAliasExpr,
  TableExpr,
  toIdentifier,
  UnnestExpr,
  ValuesExpr,
  QueryTransformExpr,
} from '../expressions';
import { Dialect } from '../dialects/dialect';
import {
  seqGet, SingleValuedMapping,
} from '../helper';
import type { Schema } from '../schema';
import { MapBinaryTuple } from '../port_internals/binary_tuple_map';
import { is } from '../port_internals';
import { OptimizeError } from '../errors';
import { Scope } from './scope';

/**
 * Helper class for resolving columns to their source tables.
 *
 * This class provides methods to determine which table a column belongs to,
 * handling ambiguous column names, join contexts, and schema inference.
 *
 * Example:
 *     ```ts
 *     import { buildScope } from 'sqlglot/optimizer';
 *     import { Resolver } from 'sqlglot/optimizer';
 *
 *     const scope = buildScope(expression);
 *     const resolver = new Resolver(scope, schema);
 *     const table = resolver.getTable('column_name');
 *     ```
 */
export class Resolver {
  scope: Scope;
  schema: Schema;
  dialect: Dialect;
  private inferSchema: boolean;
  private sourceColumns?: Map<string, string[]>;
  private unambiguousColumns?: Map<string, string>;
  private allColumnsCache?: Set<string>;
  private getSourceColumnsCache: MapBinaryTuple<[string, boolean], string[]>;

  constructor (scope: Scope, schema: Schema, options: { inferSchema?: boolean } = {}) {
    const { inferSchema = true } = options;
    this.scope = scope;
    this.schema = schema;
    this.dialect = schema.dialect || new Dialect();
    this.inferSchema = inferSchema;
    this.getSourceColumnsCache = new MapBinaryTuple();
  }

  /**
   * Get the table for a column name.
   *
   * @param column - The column expression or column name to find the table for
   * @returns The table identifier if it can be found/inferred
   */
  getTable (column: string | ColumnExpr): Expression | undefined {
    const columnName = typeof column === 'string' ? column : column.name;

    let tableName = this.getTableNameFromSources(columnName);

    if (!tableName && typeof column !== 'string') {
      const joinContext = this.getColumnJoinContext(column);
      if (joinContext) {
        try {
          const availableSources = this.getAvailableSourceColumns(joinContext);
          tableName = this.getTableNameFromSources(columnName, availableSources);
        } catch (e) {
          if (!(e instanceof OptimizeError)) throw e;
          // Column is still ambiguous, try schema inference below
        }
      }
    }

    if (!tableName && this.inferSchema) {
      const allSourceColumns = this.getAllSourceColumns();
      const sourcesWithoutSchema: string[] = [];
      for (const [source, columns] of allSourceColumns) {
        if (!columns || columns.length === 0 || columns.includes('*')) {
          sourcesWithoutSchema.push(source);
        }
      }
      if (sourcesWithoutSchema.length === 1) {
        tableName = sourcesWithoutSchema[0];
      }
    }

    if (!tableName) {
      return undefined;
    }

    const selectedSource = this.scope.selectedSources[tableName];
    if (!selectedSource) {
      return toIdentifier(tableName);
    }

    const [node] = selectedSource;

    let currentNode: Expression | undefined = node;
    if (currentNode instanceof QueryExpr) {
      while (currentNode && currentNode.alias !== tableName) {
        currentNode = currentNode.parent;
      }
    }

    if (currentNode) {
      const nodeAlias = currentNode.getArgKey('alias');
      if (nodeAlias instanceof Expression && (nodeAlias.this instanceof IdentifierExpr || typeof nodeAlias.this === 'string')) {
        return toIdentifier(nodeAlias.this);
      }
    }

    return toIdentifier(tableName);
  }

  /**
   * All available columns of all sources in this scope
   */
  get allColumns (): Set<string> {
    if (!this.allColumnsCache) {
      this.allColumnsCache = new Set();
      for (const columns of this.getAllSourceColumns().values()) {
        for (const column of columns) {
          this.allColumnsCache.add(column);
        }
      }
    }
    return this.allColumnsCache;
  }

  /**
   * Get source columns from a set operation (UNION, INTERSECT, EXCEPT).
   *
   * @param expression - The set operation expression
   * @returns List of column names
   */
  getSourceColumnsFromSetOp (expression: Expression): string[] {
    if (expression instanceof SelectExpr) {
      return (expression as SelectExpr).namedSelects;
    }

    if (expression instanceof SubqueryExpr) {
      const subqueryThis = expression.this;
      if (subqueryThis instanceof SetOperationExpr) {
        return this.getSourceColumnsFromSetOp(subqueryThis);
      }
    }

    if (!(expression instanceof SetOperationExpr)) {
      throw new Error(`Unknown set operation: ${expression}`);
    }

    const setOp = expression;
    const onColumnList = setOp.args.on;
    if (onColumnList) {
      return onColumnList.map((col) => col.name);
    }

    const side = setOp.args.side;
    const kind = setOp.args.kind;

    if (side || kind) {
      const leftExpr = setOp.this;
      const rightExpr = setOp.expression;

      if (!leftExpr || !rightExpr) {
        return [];
      }

      const left = this.getSourceColumnsFromSetOp(leftExpr as Expression);
      const right = this.getSourceColumnsFromSetOp(rightExpr as Expression);

      if (side === JoinExprKind.LEFT) {
        return left;
      } else if (side === JoinExprKind.FULL) {
        const combined = [...left, ...right];
        return Array.from(new Set(combined));
      } else if (kind === SetOperationExprKind.INNER) {
        const leftSet = new Set(left);
        const rightSet = new Set(right);
        return Array.from(leftSet).filter((col) => rightSet.has(col));
      }
    }

    return expression.namedSelects;
  }

  /**
   * Resolve the source columns for a given source name.
   *
   * @param name - The source name
   * @param onlyVisible - Whether to only return visible columns
   * @returns List of column names
   */
  getSourceColumns (name: string, options: { onlyVisible?: boolean } = {}): string[] {
    const { onlyVisible = false } = options;
    if (this.getSourceColumnsCache.has(name, onlyVisible)) {
      return this.getSourceColumnsCache.get(name, onlyVisible)!;
    }

    const source = this.scope.sources.get(name);
    if (!source) {
      throw new Error(`Unknown table: ${name}`);
    }

    let columns: string[] = [];

    if (source instanceof TableExpr) {
      columns = this.schema.columnNames?.(source, { onlyVisible }) || [];
    } else if (source instanceof Scope) {
      const sourceExpr = source.expression;
      if (sourceExpr instanceof ValuesExpr || sourceExpr instanceof UnnestExpr) {
        columns = sourceExpr.namedSelects;

        if (this.dialect._constructor.UNNEST_COLUMN_ONLY && sourceExpr instanceof UnnestExpr) {
          const unnest = sourceExpr;

          if (!unnest.type || unnest.type.this === DataTypeExprKind.UNKNOWN) {
            const unnestExpressions = unnest.expressions;
            const unnestExpr = seqGet(unnestExpressions, 0);
            if (unnestExpr instanceof ColumnExpr && this.scope.parent) {
              const colType = this.getUnnestColumnType(unnestExpr);
              if (colType && colType.isType(DataTypeExprKind.ARRAY)) {
                const elementTypes = colType.args.expressions;
                if (elementTypes && 0 < elementTypes.length) {
                  unnest.type = elementTypes[0].copy();
                } else {
                  unnest.type = colType.copy();
                }
              }
            }
          }

          if (unnest.isType(DataTypeExprKind.STRUCT)) {
            for (const field of unnest.type?.args.expressions || []) {
              if (is(field, Expression)) {
                columns.push(field.name);
              }
            }
          }
        }
      } else if (sourceExpr instanceof SetOperationExpr) {
        columns = this.getSourceColumnsFromSetOp(sourceExpr);
      } else {
        const select = seqGet(sourceExpr.expressions, 0);
        if (select instanceof QueryTransformExpr) {
          const schema = select.args.schema;
          columns = schema
            ? schema.expressions.map((c) => {
              if (c instanceof Expression) {
                return c.name;
              }
              return String(c);
            })
            : ['key', 'value'];
        } else {
          columns = sourceExpr.namedSelects;
        }
      }
    }

    const [node] = this.scope.selectedSources[name] || [undefined, undefined];

    let columnAliases: string[];
    if (node instanceof Scope) {
      columnAliases = node.expression.aliasColumnNames;
    } else if (node instanceof Expression) {
      columnAliases = node.aliasColumnNames;
    } else {
      columnAliases = [];
    }

    if (columnAliases.length) {
      const newColumns: string[] = [];
      for (let i = 0; i < Math.max(columns.length, columnAliases.length); i++) {
        const alias = seqGet(columnAliases, i);
        const colName = seqGet(columns, i);
        newColumns.push(alias || colName || '');
      }
      columns = newColumns;
    }

    this.getSourceColumnsCache.set(name, onlyVisible, columns);
    return columns;
  }

  private getAllSourceColumns (): Map<string, string[]> {
    if (!this.sourceColumns) {
      this.sourceColumns = new Map();
      const allSources = {
        ...this.scope.selectedSources,
        ...this.scope.lateralSources,
      };
      for (const sourceName of Object.keys(allSources)) {
        this.sourceColumns.set(sourceName, this.getSourceColumns(sourceName));
      }
    }
    return this.sourceColumns;
  }

  private getTableNameFromSources (columnName: string, sourceColumns?: Map<string, string[]>): string | undefined {
    let unambiguousColumns: Map<string, string>;

    if (!sourceColumns) {
      if (!this.unambiguousColumns) {
        this.unambiguousColumns = this.getUnambiguousColumns(this.getAllSourceColumns());
      }
      unambiguousColumns = this.unambiguousColumns;
    } else {
      unambiguousColumns = this.getUnambiguousColumns(sourceColumns);
    }

    return unambiguousColumns.get(columnName);
  }

  private getColumnJoinContext (column: ColumnExpr): JoinExpr | undefined {
    const joins = this.scope.expression.args.joins;

    if (!joins || this.scope.expression.getArgKey('laterals') || this.scope.expression.getArgKey('pivots')) {
      return undefined;
    }

    const joinAncestor = column.findAncestor<JoinExpr | SelectExpr>(JoinExpr, SelectExpr);

    if (joinAncestor instanceof JoinExpr && Object.keys(this.scope.selectedSources).includes(joinAncestor.aliasOrName)) {
      return joinAncestor;
    }

    return undefined;
  }

  private getAvailableSourceColumns (joinAncestor: JoinExpr): Map<string, string[]> {
    const {
      from, joins,
    } = this.scope.expression.args;

    if (!from || !joins) {
      return new Map();
    }

    const availableSources = new Map<string, string[]>();
    const fromName = from.aliasOrName;
    if (fromName) {
      availableSources.set(fromName, this.getSourceColumns(fromName));
    }

    for (const join of joins.slice(0, joins.indexOf(joinAncestor) + 1)) {
      const joinName = join.aliasOrName;
      if (joinName) {
        availableSources.set(joinName, this.getSourceColumns(joinName));
      }
    }

    return availableSources;
  }

  private getUnambiguousColumns (sourceColumns: Map<string, string[]>): Map<string, string> {
    if (sourceColumns.size === 0) {
      return new Map();
    }

    const sourceColumnsPairs = Array.from(sourceColumns.entries());
    const [firstTable, firstColumns] = sourceColumnsPairs[0];

    if (sourceColumnsPairs.length === 1) {
      return new SingleValuedMapping(firstColumns, firstTable);
    }

    const unnestOriginalAliases = new Map<string, string>();
    if (this.dialect._constructor.UNNEST_COLUMN_ONLY) {
      for (const [sourceName, source] of this.scope.sources) {
        if (source instanceof Scope && source.expression instanceof UnnestExpr) {
          const aliasArg = source.expression.args.alias;
          if (is(aliasArg, TableAliasExpr) && aliasArg.columns.length) {
            unnestOriginalAliases.set(aliasArg.columns[0].name, sourceName);
          }
        }
      }
    }

    const unambiguousColumns = new Map<string, string>();
    for (const col of firstColumns) {
      unambiguousColumns.set(col, firstTable);
    }

    const allColumns = new Set(firstColumns);

    for (const [table, columns] of sourceColumnsPairs.slice(1)) {
      const unique = new Set(columns);
      const ambiguous = new Set([...allColumns].filter((c) => unique.has(c)));
      for (const col of columns) allColumns.add(col);

      for (const column of ambiguous) {
        if (unnestOriginalAliases.has(column)) {
          unambiguousColumns.set(column, unnestOriginalAliases.get(column)!);
          continue;
        }
        unambiguousColumns.delete(column);
      }

      for (const column of unique) {
        if (!ambiguous.has(column)) {
          unambiguousColumns.set(column, table);
        }
      }
    }

    return unambiguousColumns;
  }

  private getUnnestColumnType (column: ColumnExpr): DataTypeExpr | undefined {
    const scope = this.scope.parent;
    if (!scope) {
      return undefined;
    }

    let tableName: string | undefined;
    if (column.table) {
      tableName = column.table;
    } else {
      const parentResolver = new Resolver(scope, this.schema, { inferSchema: this.inferSchema });
      const tableIdentifier = parentResolver.getTable(column);
      if (!tableIdentifier) {
        return undefined;
      }
      tableName = tableIdentifier.name;
    }

    const source = scope.sources.get(tableName);
    return source ? this.getColumnTypeFromScope(source, column) : undefined;
  }

  private getColumnTypeFromScope (source: Scope | TableExpr, column: ColumnExpr): DataTypeExpr | undefined {
    if (source instanceof TableExpr) {
      const colType = this.schema.getColumnType?.(source, column);
      if (colType) {
        const colTypeThis = typeof colType.this === 'string' ? colType.this as DataTypeExprKind : DataTypeExprKind.UNKNOWN;
        if (colTypeThis !== DataTypeExprKind.UNKNOWN) {
          return colType;
        }
      }
    } else if (source instanceof Scope) {
      for (const [, nestedSource] of source.sources) {
        const colType = this.getColumnTypeFromScope(nestedSource, column);
        if (colType) {
          const colTypeThis = typeof colType.this === 'string' ? colType.this as DataTypeExprKind : DataTypeExprKind.UNKNOWN;
          if (colTypeThis !== DataTypeExprKind.UNKNOWN) {
            return colType;
          }
        }
      }
    }

    return undefined;
  }
}
