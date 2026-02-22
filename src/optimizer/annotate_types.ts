// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/annotate_types.py

import type {
  ColumnDefExprKind,
  IntervalExpr,
} from '../expressions';
import {
  AddExpr,
  AnonymousExpr,
  ArrayExpr,
  BinaryExpr,
  BracketExpr,
  CastExpr,
  ColumnExpr,
  ColumnDefExpr,
  DataTypeExpr,
  DataTypeExprKind,
  DateTruncExpr,
  DivExpr,
  DotExpr,
  ExplodeExpr,
  Expression,
  ExtractExpr,
  LateralExpr,
  LiteralExpr,
  MapExpr,
  NotExpr,
  PredicateExpr,
  QueryExpr,
  SetOperationExpr,
  SliceExpr,
  StructExpr,
  SubExpr,
  SubqueryExpr,
  TableExpr,
  TableFromRowsExpr,
  TimeUnitExpr,
  ToMapExpr,
  UdtfExpr,
  UnaryExpr,
  UnnestExpr,
  VarMapExpr,
  toIdentifier,
  IdentifierExpr,
} from '../expressions';
import {
  Dialect, type DialectType,
} from '../dialects/dialect';
import {
  ensureList, isDateUnit, isIsoDate, isIsoDatetime, seqGet,
} from '../helper';
import {
  MapBinaryTuple, assertIsInstanceOf, is,
} from '../port_internals';
import {
  ensureSchema, MappingSchema, type Schema,
} from '../schema';
import {
  Scope, traverseScope,
} from './scope';

/** EXTRACT/DATE_PART specifiers that return BIGINT instead of INT */
const BIGINT_EXTRACT_DATE_PARTS = new Set([
  'EPOCH_SECOND',
  'EPOCH_MILLISECOND',
  'EPOCH_MICROSECOND',
  'EPOCH_NANOSECOND',
  'NANOSECOND',
]);

type BinaryCoercionFunc = (l: Expression, r: Expression) => DataTypeExprKind;
type BinaryCoercions = MapBinaryTuple<[DataTypeExprKind, DataTypeExprKind], BinaryCoercionFunc>;

/** Per-expression-class annotation spec: provide an annotator callback or a fixed return type */
export interface ExpressionMetadataEntry {
  annotator?: (annotator: TypeAnnotator, expression: Expression) => void;
  returns?: DataTypeExprKind;
}

/**
 * Maps expression constructors to their annotation spec.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ExpressionMetadata = Map<new (...args: any[]) => Expression, ExpressionMetadataEntry>;

/**
 * Infers the types of an expression, annotating its AST accordingly.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { annotateTypes } from 'sqlglot/optimizer';
 *
 *     const schema = { y: { cola: "SMALLINT" } };
 *     const sql = "SELECT x.cola + 2.5 AS cola FROM (SELECT y.cola AS cola FROM y AS y) AS x";
 *     const annotatedExpr = annotateTypes(parseOne(sql), { schema });
 *     annotatedExpr.expressions[0].type.this // Get the type of "x.cola + 2.5 AS cola"
 *     // DataTypeExprKind.DOUBLE
 *     ```
 *
 * @param expression - Expression to annotate
 * @param options - Annotation options
 * @param options.schema - Database schema
 * @param options.dialect - SQL dialect
 * @param options.overwriteTypes - Re-annotate existing AST types (default: true)
 * @returns The expression annotated with types
 */
export function annotateTypes<E extends Expression> (
  expression: E,
  options: {
    schema?: Record<string, unknown> | Schema;
    expressionMetadata?: ExpressionMetadata;
    coercesTo?: Map<DataTypeExprKind, Set<DataTypeExprKind>>;
    dialect?: DialectType;
    overwriteTypes?: boolean;
  } = {},
): E {
  const {
    schema: schemaArg, expressionMetadata, coercesTo, dialect, overwriteTypes = true,
  } = options;
  const schema = ensureSchema(schemaArg, { dialect });

  return new TypeAnnotator({
    schema,
    expressionMetadata,
    coercesTo,
    overwriteTypes,
  }).annotate(expression);
}

function coerceDateLiteral (l: Expression, unit?: Expression): DataTypeExprKind {
  const dateText = l.name;
  const isIsoDate_ = isIsoDate(dateText);

  if (isIsoDate_ && isDateUnit(unit)) {
    return DataTypeExprKind.DATE;
  }

  if (isIsoDate_ || isIsoDatetime(dateText)) {
    return DataTypeExprKind.DATETIME;
  }

  return DataTypeExprKind.UNKNOWN;
}

function coerceDate (l: Expression, unit?: Expression): DataTypeExprKind {
  if (!isDateUnit(unit)) {
    return DataTypeExprKind.DATETIME;
  }
  const typeThis = l.type?.args.this;
  if (typeof typeThis === 'string') {
    return typeThis as DataTypeExprKind;
  }
  return typeThis instanceof IdentifierExpr ? typeThis.args.this as DataTypeExprKind : DataTypeExprKind.UNKNOWN;
}

function swapArgs (func: BinaryCoercionFunc): BinaryCoercionFunc {
  return (l: Expression, r: Expression) => func(r, l);
}

function swapAll (coercions: BinaryCoercions): BinaryCoercions {
  const map: BinaryCoercions = new MapBinaryTuple();
  for (const [[left, right], func] of coercions) {
    map.set(left, right, func);
    map.set(right, left, swapArgs(func));
  }
  return map;
}

/**
 * Type annotator for SQL expressions.
 * Walks the AST and infers types for all expressions based on schema and type rules.
 */
export class TypeAnnotator {
  static NESTED_TYPES = new Set([DataTypeExprKind.ARRAY]);

  // Specifies what types a given type can be coerced into
  // Highest-to-lowest precedence: lowest types can coerce INTO higher types
  // E.g. CHAR (lowest) can coerce to TEXT (highest)
  static COERCES_TO: Map<DataTypeExprKind, Set<DataTypeExprKind>> = (() => {
    const map = new Map<DataTypeExprKind, Set<DataTypeExprKind>>();

    // Text precedence: highest to lowest (TEXT wins over CHAR in coercion)
    const textPrecedence = [
      DataTypeExprKind.TEXT,
      DataTypeExprKind.NVARCHAR,
      DataTypeExprKind.VARCHAR,
      DataTypeExprKind.NCHAR,
      DataTypeExprKind.CHAR,
    ];

    // Numeric precedence: highest to lowest (DECFLOAT wins over TINYINT)
    const numericPrecedence = [
      DataTypeExprKind.DECFLOAT,
      DataTypeExprKind.DOUBLE,
      DataTypeExprKind.FLOAT,
      DataTypeExprKind.BIGDECIMAL,
      DataTypeExprKind.DECIMAL,
      DataTypeExprKind.BIGINT,
      DataTypeExprKind.INT,
      DataTypeExprKind.SMALLINT,
      DataTypeExprKind.TINYINT,
    ];

    // Timelike precedence: highest to lowest (TIMESTAMPLTZ wins over DATE)
    const timelikePrecedence = [
      DataTypeExprKind.TIMESTAMPLTZ,
      DataTypeExprKind.TIMESTAMPTZ,
      DataTypeExprKind.TIMESTAMP,
      DataTypeExprKind.DATETIME,
      DataTypeExprKind.DATE,
    ];

    // Build COERCES_TO map: for each type in highest-to-lowest order,
    // it can coerce into all previously seen types (which are higher precedence)
    for (const precedence of [
      textPrecedence,
      numericPrecedence,
      timelikePrecedence,
    ]) {
      const coercesTo = new Set<DataTypeExprKind>();
      for (const dataType of precedence) {
        map.set(dataType, new Set(coercesTo));
        coercesTo.add(dataType);
      }
    }

    return map;
  })();

  // Coercion functions for binary operations where COERCES_TO is not sufficient
  static BINARY_COERCIONS: BinaryCoercions = swapAll((() => {
    const map: BinaryCoercions = new MapBinaryTuple();

    // text + interval → DATE/DATETIME based on whether text is ISO date
    for (const t of DataTypeExpr.TEXT_TYPES) {
      map.set(
        t,
        DataTypeExprKind.INTERVAL,
        (l: Expression, r: Expression): DataTypeExprKind =>
          coerceDateLiteral(l, (r as IntervalExpr).unit),
      );
    }

    // text + numeric → return the numeric type (match most dialect semantics)
    for (const text of DataTypeExpr.TEXT_TYPES) {
      for (const numeric of DataTypeExpr.NUMERIC_TYPES) {
        map.set(
          text,
          numeric,
          (l: Expression, r: Expression): DataTypeExprKind => {
            const lTypeKind = l.type?.this as DataTypeExprKind;
            return DataTypeExpr.NUMERIC_TYPES.has(lTypeKind) ? lTypeKind : r.type?.this as DataTypeExprKind;
          },
        );
      }
    }

    // date + interval → DATE/DATETIME based on interval unit
    map.set(
      DataTypeExprKind.DATE,
      DataTypeExprKind.INTERVAL,
      (l: Expression, r: Expression): DataTypeExprKind =>
        coerceDate(l, (r as IntervalExpr).unit),
    );

    return map;
  })());

  schema: Schema;
  dialect: Dialect;
  overwriteTypes: boolean;
  expressionMetadata: ExpressionMetadata;
  coercesTo: Map<DataTypeExprKind, Set<DataTypeExprKind>>;
  binaryCoercions: BinaryCoercions;

  private visited: Set<Expression>;
  private nullExpressions: Map<Expression, Expression>;
  private supportsNullType: boolean;
  private setopColumnTypes: Map<Expression, Record<string, DataTypeExpr | DataTypeExprKind>>;
  private scopeSelects: Map<Scope, Map<string, Record<string, ColumnDefExpr | DataTypeExpr | DataTypeExprKind | undefined>>>;

  constructor (options: {
    schema: Schema;
    expressionMetadata?: ExpressionMetadata;
    coercesTo?: Map<DataTypeExprKind, Set<DataTypeExprKind>>;
    binaryCoercions?: BinaryCoercions;
    overwriteTypes?: boolean;
  }) {
    const {
      schema, expressionMetadata, coercesTo, binaryCoercions, overwriteTypes = true,
    } = options;

    this.schema = schema;
    this.dialect = schema.dialect || new Dialect();
    this.overwriteTypes = overwriteTypes;

    this.expressionMetadata = expressionMetadata ?? new Map();

    // coercesTo priority: provided param → dialect.COERCES_TO → TypeAnnotator.COERCES_TO
    if (coercesTo) {
      this.coercesTo = coercesTo;
    } else {
      const dialectCoercesTo = this.dialect._constructor.COERCES_TO;
      if (0 < Object.keys(dialectCoercesTo).length) {
        this.coercesTo = new Map(
          Object.entries(dialectCoercesTo).map(([k, v]) => [k as DataTypeExprKind, new Set(v as Iterable<DataTypeExprKind>)]),
        );
      } else {
        this.coercesTo = TypeAnnotator.COERCES_TO;
      }
    }

    this.binaryCoercions = binaryCoercions ?? TypeAnnotator.BINARY_COERCIONS;

    this.visited = new Set();
    this.nullExpressions = new Map();
    this.supportsNullType = this.dialect._constructor.SUPPORTS_NULL_TYPE;
    this.setopColumnTypes = new Map();
    this.scopeSelects = new Map();
  }

  clear (): void {
    this.visited.clear();
    this.nullExpressions.clear();
    this.setopColumnTypes.clear();
    this.scopeSelects.clear();
  }

  annotate<E extends Expression> (expression: E, options: { annotateScope?: boolean } = {}): E {
    const { annotateScope = true } = options;

    if (annotateScope) {
      for (const scope of traverseScope(expression)) {
        this.annotateScope(scope);
      }
    }

    // Also annotate non-traversable expressions (top-level or those outside scopes)
    this.annotateExpression(expression);

    // Replace NULL type with the dialect's default null type
    const defaultNullType = this.dialect._constructor.DEFAULT_NULL_TYPE;
    for (const [, expr] of this.nullExpressions) {
      expr.type = defaultNullType as DataTypeExprKind;
    }

    return expression;
  }

  annotateScope (scope: Scope): void {
    if (this.schema instanceof MappingSchema) {
      for (const tableColumn of scope.tableColumns) {
        const source = scope.sources.get(tableColumn.name);

        if (source instanceof TableExpr) {
          const schema = this.schema.find(source, {
            raiseOnMissing: false,
            ensureDataTypes: true,
          });
          if (typeof schema !== 'object' || !schema) continue;
          const structType = new DataTypeExpr({
            this: DataTypeExprKind.STRUCT,
            expressions: Object.entries(schema).map(([c, kind]) =>
              new ColumnDefExpr({
                this: toIdentifier(c),
                kind,
              })),
            nested: true,
          });
          this.setType(tableColumn, structType);
        } else if (
          source instanceof Scope
          && source.expression instanceof QueryExpr
          && (source.expression.meta['queryType'] as DataTypeExpr
            || DataTypeExpr.build(DataTypeExprKind.UNKNOWN)
          ).isType(DataTypeExprKind.STRUCT)
        ) {
          this.setType(tableColumn, source.expression.meta['queryType'] as DataTypeExpr);
        }
      }
    }

    this.annotateExpression(scope.expression, scope);

    if (this.dialect._constructor.QUERY_RESULTS_ARE_STRUCTS && scope.expression instanceof QueryExpr) {
      const structType = new DataTypeExpr({
        this: DataTypeExprKind.STRUCT,
        expressions: scope.expression.selects.map((select) =>
          new ColumnDefExpr({
            this: toIdentifier(select.outputName),
            kind: select.type?.copy() as unknown as ColumnDefExprKind,
          })),
        nested: true,
      });

      if (!structType.expressions.some((cd) => {
        const kind = (cd as unknown as ColumnDefExpr).args.kind as unknown as DataTypeExpr | undefined;
        return !kind || kind.isType(DataTypeExprKind.UNKNOWN);
      })) {
        scope.expression.meta['queryType'] = structType;
      }
    }
  }

  private getScopeSelects (
    scope: Scope,
  ): Map<string, Record<string, DataTypeExpr | ColumnDefExpr | DataTypeExprKind | undefined>> {
    if (this.scopeSelects.has(scope)) {
      return this.scopeSelects.get(scope)!;
    }

    const selects = new Map<string, Record<string, DataTypeExpr | DataTypeExprKind | ColumnDefExpr | undefined>>();

    for (const [name, source] of scope.sources) {
      if (!(source instanceof Scope)) {
        continue;
      }

      const expression = source.expression;

      if (expression instanceof UdtfExpr && !(expression instanceof TableFromRowsExpr)) {
        let values: Expression[] = [];

        if (expression instanceof LateralExpr) {
          const inner = expression.args.this;
          if (inner instanceof ExplodeExpr) {
            values = [inner.args.this as Expression];
          }
        } else if (expression instanceof UnnestExpr) {
          values = [expression];
        } else {
          // Other UDTFs: first expression's sub-expressions
          const firstExpr = seqGet(expression.expressions, 0);
          if (firstExpr) {
            values = (firstExpr instanceof Expression ? firstExpr.expressions : []) as Expression[];
          }
        }

        if (values.length === 0) {
          continue;
        }

        const aliasColumnNames = expression.aliasColumnNames;

        // Handle Unnest with STRUCT result type
        if (
          expression instanceof UnnestExpr
          && expression.type
          && expression.type.isType(DataTypeExprKind.STRUCT)
        ) {
          const colRecord: Record<string, DataTypeExpr | DataTypeExprKind | ColumnDefExpr | undefined> = {};
          for (const colDef of (expression.type.args.expressions || []) as Expression[]) {
            const fieldName = colDef.name;
            const fieldType = (colDef as ColumnDefExpr).args.kind as unknown as DataTypeExpr | DataTypeExprKind | undefined;
            if (fieldName) {
              colRecord[fieldName] = fieldType;
            }
          }
          selects.set(name, colRecord);
        } else {
          const colRecord: Record<string, DataTypeExpr | DataTypeExprKind | ColumnDefExpr | undefined> = {};
          for (let i = 0; i < aliasColumnNames.length && i < values.length; i++) {
            const vType = values[i].type;
            colRecord[aliasColumnNames[i]] = is(vType, DataTypeExpr) ? vType : is(vType, ColumnDefExpr) ? vType : undefined;
          }
          selects.set(name, colRecord);
        }
      } else if (
        expression instanceof SetOperationExpr
        && is(expression.left, QueryExpr) && is(expression.right, QueryExpr)
        && expression.left.selects.length === expression.right.selects.length
      ) {
        selects.set(name, this.getSetopColumnTypes(expression) as Record<string, DataTypeExpr | DataTypeExprKind | undefined>);
      } else {
        const colRecord: Record<string, ColumnDefExpr | DataTypeExpr | DataTypeExprKind | undefined> = {};
        for (const s of expression.selects) {
          const sType = s.type;
          colRecord[s.aliasOrName] = is(sType, DataTypeExpr) ? sType : is(sType, ColumnDefExpr) ? sType : undefined;
        }
        selects.set(name, colRecord);
      }
    }

    this.scopeSelects.set(scope, selects);
    return selects;
  }

  private getSetopColumnTypes (
    setop: SetOperationExpr,
  ): Record<string, DataTypeExpr | DataTypeExprKind> {
    if (this.setopColumnTypes.has(setop)) {
      return this.setopColumnTypes.get(setop)!;
    }

    const colTypes: Record<string, DataTypeExpr | DataTypeExprKind> = {};

    const setopLeft = setop.left;
    const setopRight = setop.right;
    if (
      !is(setopLeft, QueryExpr) || !is(setopRight, QueryExpr)
      || !setopLeft.selects.length
      || !setopRight.selects.length
      || setopLeft.selects.length !== setopRight.selects.length
    ) {
      return colTypes;
    }

    for (const node of setop.walk({ prune: (n) => !(n instanceof SetOperationExpr) && !(n instanceof SubqueryExpr) })) {
      if (!(node instanceof SetOperationExpr)) {
        continue;
      }

      let setopCols: Record<string, DataTypeExpr | DataTypeExprKind>;

      const nodeLeft = node.left;
      const nodeRight = node.right;
      assertIsInstanceOf(nodeLeft, QueryExpr);
      assertIsInstanceOf(nodeRight, QueryExpr);

      if (node.args.byName) {
        const rightTypeBySelect: Record<string, DataTypeExpr | DataTypeExprKind | ColumnDefExpr | undefined> = {};
        for (const s of nodeRight.selects) {
          const sType = s.type;
          rightTypeBySelect[s.aliasOrName] = is(sType, DataTypeExpr) ? sType : is(sType, ColumnDefExpr) ? sType : undefined;
        }
        setopCols = {};
        for (const s of nodeLeft.selects) {
          const sType = s.type;
          const lType = (is(sType, DataTypeExpr) ? sType : undefined) ?? DataTypeExprKind.UNKNOWN;
          const rType = rightTypeBySelect[s.aliasOrName] ?? DataTypeExprKind.UNKNOWN;
          setopCols[s.aliasOrName] = this.maybeCoerce(lType, rType);
        }
      } else {
        setopCols = {};
        for (let i = 0; i < nodeLeft.selects.length; i++) {
          const ls = nodeLeft.selects[i];
          const rs = nodeRight.selects[i];
          const lsType = ls.type;
          const rsType = rs.type;
          const lType = (is(lsType, DataTypeExpr) ? lsType : undefined) ?? DataTypeExprKind.UNKNOWN;
          const rType = (is(rsType, DataTypeExpr) ? rsType : undefined) ?? DataTypeExprKind.UNKNOWN;
          setopCols[ls.aliasOrName] = this.maybeCoerce(lType, rType);
        }
      }

      // Coerce intermediate results with previously registered types
      for (const [colName, colType] of Object.entries(setopCols)) {
        const prevType = colTypes[colName] ?? DataTypeExprKind.NULL;
        colTypes[colName] = this.maybeCoerce(colType, prevType);
      }
    }

    this.setopColumnTypes.set(setop, colTypes);
    return colTypes;
  }

  /**
   * Annotate a single expression (and all its children) using iterative post-order traversal.
   * If scope is provided, resolves column types from that scope's sources.
   */
  private annotateExpression (expression: Expression, scope?: Scope): void {
    const stack: [Expression, boolean][] = [[expression, false]];

    while (0 < stack.length) {
      const [expr, childrenAnnotated] = stack.pop()!;

      // Skip if already visited or if we're not overwriting and type is known
      if (
        this.visited.has(expr)
        || (!this.overwriteTypes && expr.type && !expr.isType(DataTypeExprKind.UNKNOWN))
      ) {
        continue;
      }

      if (!childrenAnnotated) {
        // Push back for post-processing, then push children
        stack.push([expr, true]);
        for (const child of expr.iterExpressions()) {
          stack.push([child, false]);
        }
        continue;
      }

      // Scope-based column type resolution
      if (scope && expr instanceof ColumnExpr && expr.args['table']) {
        let source: TableExpr | Scope | undefined;
        let sourceScope: Scope | undefined = scope;

        while (sourceScope && !source) {
          source = sourceScope.sources.get(expr.table) as TableExpr | Scope | undefined;
          if (!source) {
            sourceScope = sourceScope.parent;
          }
        }

        if (source instanceof TableExpr) {
          this.setType(expr, this.schema.getColumnType?.(source, expr));
        } else if (source) {
          const colType = this.getScopeSelects(sourceScope!).get(expr.table)?.[expr.name];
          if (colType) {
            this.setType(expr, colType);
          } else if ((source as Scope).expression instanceof UnnestExpr) {
            const unnestType = (source as Scope).expression.type;
            this.setType(expr, is(unnestType, DataTypeExpr) ? unnestType : undefined);
          } else {
            this.setType(expr, DataTypeExprKind.UNKNOWN);
          }
        } else {
          this.setType(expr, DataTypeExprKind.UNKNOWN);
        }

        if ((expr.type?.args as Record<string, unknown> | undefined)?.['nullable'] === false) {
          expr.meta['nonnull'] = true;
        }
        continue;
      }

      // Check custom expressionMetadata first (for dialect/caller overrides)
      const spec = this.expressionMetadata.get(expr.constructor as new (...args: unknown[]) => Expression);
      if (spec) {
        if (spec.annotator) {
          spec.annotator(this, expr);
          continue;
        }
        if (spec.returns !== undefined) {
          this.setType(expr, spec.returns);
          continue;
        }
      }

      // Direct dispatch (default behavior)
      this.dispatchAnnotate(expr);
    }
  }

  /** Dispatch to the appropriate annotation method based on expression type */
  private dispatchAnnotate (expr: Expression): void {
    if (expr instanceof DivExpr) {
      this.annotateDiv(expr);
    } else if (expr instanceof BinaryExpr || expr instanceof AddExpr || expr instanceof SubExpr) {
      this.annotateBinary(expr);
    } else if (expr instanceof UnaryExpr) {
      this.annotateUnary(expr);
    } else if (expr instanceof LiteralExpr) {
      this.annotateLiteral(expr);
    } else if (expr instanceof CastExpr) {
      this.setType(expr, expr.args.to as DataTypeExpr);
    } else if (expr instanceof BracketExpr) {
      this.annotateBracket(expr);
    } else if (expr instanceof DotExpr) {
      this.annotateDot(expr);
    } else if (expr instanceof ExtractExpr) {
      this.annotateExtract(expr);
    } else if (expr instanceof SubqueryExpr) {
      this.annotateSubquery(expr);
    } else if (expr instanceof StructExpr) {
      this.annotateStruct(expr);
    } else if (expr instanceof MapExpr || expr instanceof VarMapExpr) {
      this.annotateMap(expr);
    } else if (expr instanceof ToMapExpr) {
      this.annotateToMap(expr);
    } else if (expr instanceof ExplodeExpr) {
      this.annotateExplode(expr);
    } else if (expr instanceof UnnestExpr) {
      this.annotateUnnest(expr);
    } else if (expr instanceof TimeUnitExpr || expr instanceof DateTruncExpr) {
      this.annotateTimeunit(expr);
    } else {
      this.setType(expr, DataTypeExprKind.UNKNOWN);
    }
  }

  /**
   * Determine the result type when combining two operands.
   * If either type is parameterized (e.g., DECIMAL(18, 2)), returns it as-is.
   * Propagates UNKNOWN upward. NULL coerces into the other type.
   */
  maybeCoerce (
    type1: DataTypeExpr | DataTypeExprKind | ColumnDefExpr | undefined,
    type2: DataTypeExpr | DataTypeExprKind | ColumnDefExpr | undefined,
  ): DataTypeExpr | DataTypeExprKind {
    let type1Value: DataTypeExprKind;
    if (type1 instanceof DataTypeExpr) {
      if (type1.args.expressions && 0 < type1.args.expressions.length) {
        return type1; // Parameterized type - return as-is
      }
      type1Value = type1.this as DataTypeExprKind;
    } else {
      type1Value = (type1 ?? DataTypeExprKind.UNKNOWN) as DataTypeExprKind;
    }

    let type2Value: DataTypeExprKind;
    if (type2 instanceof DataTypeExpr) {
      if (type2.args.expressions && 0 < (type2.args.expressions as unknown[]).length) {
        return type2;
      }
      type2Value = type2.this as DataTypeExprKind;
    } else {
      type2Value = (type2 ?? DataTypeExprKind.UNKNOWN) as DataTypeExprKind;
    }

    // Propagate UNKNOWN
    if (type1Value === DataTypeExprKind.UNKNOWN || type2Value === DataTypeExprKind.UNKNOWN) {
      return DataTypeExprKind.UNKNOWN;
    }

    // NULL coerces to the other type
    if (type1Value === DataTypeExprKind.NULL) {
      return type2Value;
    }
    if (type2Value === DataTypeExprKind.NULL) {
      return type1Value;
    }

    return this.coercesTo.get(type1Value)?.has(type2Value) ? type2Value : type1Value;
  }

  private setType (expression: Expression, targetType?: ColumnDefExpr | DataTypeExpr | DataTypeExprKind): Expression {
    const prevType = expression.type;
    expression.type = targetType || DataTypeExprKind.UNKNOWN;

    this.visited.add(expression);

    if (!this.supportsNullType && expression.type?.args.this === DataTypeExprKind.NULL) {
      this.nullExpressions.set(expression, expression);
    } else if (prevType?.isType(DataTypeExprKind.NULL)) {
      this.nullExpressions.delete(expression);
    }

    // JSON dot access is case sensitive — undo normalization when type is JSON
    if (
      expression instanceof ColumnExpr
      && expression.isType(DataTypeExprKind.JSON)
      && expression.meta['dotParts']
    ) {
      const dotParts = expression.meta['dotParts'] as string[];
      const iter = dotParts[Symbol.iterator]();
      let parent = expression.parent;
      while (parent instanceof DotExpr) {
        const next = iter.next();
        if (!next.done) {
          parent.args.expression = toIdentifier(next.value, { quoted: true });
        }
        parent = parent.parent;
      }
      delete expression.meta['dotParts'];
    }

    return expression;
  }

  private annotateBinary (expression: BinaryExpr): void {
    const left = expression.left;
    const right = expression.right;

    if (!left || !right) {
      this.setType(expression);
      return;
    }

    const leftType = left.type?.args.this?.toString() as DataTypeExprKind;
    const rightType = right.type?.args.this?.toString() as DataTypeExprKind;

    // Connectors (AND, OR) and predicates (=, <, >, IS, etc.) always return BOOLEAN
    if (expression instanceof PredicateExpr) {
      this.setType(expression, DataTypeExprKind.BOOLEAN);
    } else {
      const coercionFunc = this.binaryCoercions.get(leftType, rightType);
      if (coercionFunc) {
        this.setType(expression, coercionFunc(left, right));
      } else {
        this.annotateByArgs(expression, [left, right]);
      }
    }

    // If either operand is non-null, the result may be non-null
    if (left.meta['nonnull'] === true && right.meta['nonnull'] === true) {
      expression.meta['nonnull'] = true;
    }
  }

  private annotateUnary (expression: UnaryExpr): void {
    if (expression instanceof NotExpr) {
      this.setType(expression, DataTypeExprKind.BOOLEAN);
    } else {
      const inner = expression.args.this as Expression;
      const innerType = inner?.type;
      this.setType(expression, is(innerType, DataTypeExpr) ? innerType : is(innerType, ColumnDefExpr) ? innerType : undefined);
    }

    const inner = expression.args.this as Expression;
    if (inner?.meta['nonnull'] === true) {
      expression.meta['nonnull'] = true;
    }
  }

  private annotateLiteral (expression: LiteralExpr): void {
    if (expression.args.isString) {
      this.setType(expression, DataTypeExprKind.VARCHAR);
    } else if (expression.isInt) {
      this.setType(expression, DataTypeExprKind.INT);
    } else {
      this.setType(expression, DataTypeExprKind.DOUBLE);
    }
    expression.meta['nonnull'] = true;
  }

  /**
   * Annotate by coercing types from expression arguments.
   * Handles literal vs non-literal type priority, array wrapping, and type promotion.
   */
  annotateByArgs (
    expression: Expression,
    args: (string | Expression | Expression[])[],
    options: { promote?: boolean;
      array?: boolean; } = {},
  ): void {
    const {
      promote = false, array = false,
    } = options;

    let literalType: DataTypeExpr | DataTypeExprKind | null = null;
    let nonLiteralType: DataTypeExpr | DataTypeExprKind | null = null;
    let nestedType: ColumnDefExpr | DataTypeExpr | undefined;

    outer:
    for (const arg of args) {
      let expressions: Expression[];

      if (typeof arg === 'string') {
        const val = (expression.args as Record<string, unknown>)[arg];
        expressions = ensureList(val as Expression | Expression[] | undefined) as Expression[];
      } else if (Array.isArray(arg)) {
        expressions = arg;
      } else {
        expressions = [arg];
      }

      for (const expr of expressions) {
        if (!(expr instanceof Expression)) {
          continue;
        }
        const exprType = expr.type;
        if (!exprType) {
          continue;
        }

        const narrowedExprType: DataTypeExpr | ColumnDefExpr | undefined = is(exprType, DataTypeExpr) ? exprType : is(exprType, ColumnDefExpr) ? exprType : undefined;
        if (!narrowedExprType) {
          continue;
        }

        // Stop at the first nested type (e.g., ARRAY<INT>)
        if (narrowedExprType.getArgKey('nested')) {
          nestedType = narrowedExprType;
          break outer;
        }

        if (expr instanceof LiteralExpr) {
          literalType = this.maybeCoerce(literalType ?? narrowedExprType, narrowedExprType);
        } else {
          nonLiteralType = this.maybeCoerce(nonLiteralType ?? narrowedExprType, narrowedExprType);
        }
      }
    }

    let resultType: ColumnDefExpr | DataTypeExpr | DataTypeExprKind | undefined;

    if (nestedType) {
      resultType = nestedType;
    } else if (literalType !== null && nonLiteralType !== null) {
      if (this.dialect._constructor.PRIORITIZE_NON_LITERAL_TYPES) {
        const litKind = literalType instanceof DataTypeExpr ? literalType.this as DataTypeExprKind : literalType;
        const nonLitKind = nonLiteralType instanceof DataTypeExpr ? nonLiteralType.this as DataTypeExprKind : nonLiteralType;
        if (
          (DataTypeExpr.INTEGER_TYPES.has(litKind) && DataTypeExpr.INTEGER_TYPES.has(nonLitKind))
          || (DataTypeExpr.REAL_TYPES.has(litKind) && DataTypeExpr.REAL_TYPES.has(nonLitKind))
        ) {
          resultType = nonLiteralType;
        }
        // else fall through to use maybeCoerce below
      }
    } else {
      resultType = literalType ?? nonLiteralType ?? DataTypeExprKind.UNKNOWN;
    }

    this.setType(
      expression,
      resultType ?? this.maybeCoerce(nonLiteralType ?? undefined, literalType ?? undefined),
    );

    if (promote) {
      const currentKind = expression.type?.this as DataTypeExprKind;
      if (DataTypeExpr.INTEGER_TYPES.has(currentKind)) {
        this.setType(expression, DataTypeExprKind.BIGINT);
      } else if (DataTypeExpr.FLOAT_TYPES.has(currentKind)) {
        this.setType(expression, DataTypeExprKind.DOUBLE);
      }
    }

    if (array) {
      const elementType = expression.type?.copy() ?? new DataTypeExpr({ this: DataTypeExprKind.UNKNOWN });
      this.setType(expression, new DataTypeExpr({
        this: DataTypeExprKind.ARRAY,
        expressions: [elementType],
        nested: true,
      }));
    }
  }

  private annotateTimeunit (expression: TimeUnitExpr | DateTruncExpr): void {
    const inner = expression.args.this as Expression;
    if (!inner?.type) {
      this.setType(expression, DataTypeExprKind.UNKNOWN);
      return;
    }
    const innerKind = inner.type.this as DataTypeExprKind;
    let datatype: DataTypeExprKind;

    if (DataTypeExpr.TEXT_TYPES.has(innerKind)) {
      const unit = (expression as TimeUnitExpr).unit;
      datatype = coerceDateLiteral(inner, unit);
    } else if (DataTypeExpr.TEMPORAL_TYPES.has(innerKind)) {
      const unit = (expression as TimeUnitExpr).unit;
      datatype = coerceDate(inner, unit);
    } else {
      datatype = DataTypeExprKind.UNKNOWN;
    }

    this.setType(expression, datatype);
  }

  private annotateBracket (expression: BracketExpr): void {
    const bracketArg = seqGet(expression.args.expressions, 0);
    const thisExpr = expression.args.this;

    if (bracketArg instanceof SliceExpr) {
      // Slice returns same type as the collection
      const thisExprType = thisExpr.type;
      this.setType(expression, is(thisExprType, DataTypeExpr) ? thisExprType : undefined);
    } else if (thisExpr.type?.isType(DataTypeExprKind.ARRAY)) {
      // Array indexing returns the element type
      const elemType = seqGet(thisExpr.type.args.expressions as DataTypeExpr[] | undefined ?? [], 0);
      this.setType(expression, elemType);
    } else if (thisExpr instanceof MapExpr || thisExpr instanceof VarMapExpr) {
      // Map access: find the corresponding value type
      const keys = thisExpr.keys as Expression[];
      if (bracketArg) {
        const index = keys.findIndex((k) => k instanceof Expression && k.equals(bracketArg));
        if (0 <= index) {
          const values = thisExpr.values as Expression[];
          const value = seqGet(values, index);
          const valueType = value instanceof Expression ? value.type : undefined;
          this.setType(expression, is(valueType, DataTypeExpr) ? valueType : undefined);
        } else {
          this.setType(expression, DataTypeExprKind.UNKNOWN);
        }
      } else {
        this.setType(expression, DataTypeExprKind.UNKNOWN);
      }
    } else {
      this.setType(expression, DataTypeExprKind.UNKNOWN);
    }
  }

  private annotateDiv (expression: DivExpr): void {
    const left = expression.left;
    const right = expression.right;
    const leftType = left?.type?.this as DataTypeExprKind;
    const rightType = right?.type?.this as DataTypeExprKind;

    if (
      expression.args['typed']
      && DataTypeExpr.INTEGER_TYPES.has(leftType)
      && DataTypeExpr.INTEGER_TYPES.has(rightType)
    ) {
      this.setType(expression, DataTypeExprKind.BIGINT);
    } else {
      this.setType(expression, this.maybeCoerce(leftType, rightType));
      // If result is not a real type, promote to DOUBLE
      const currentKind = expression.type?.this as DataTypeExprKind;
      if (!DataTypeExpr.REAL_TYPES.has(currentKind)) {
        const curType = expression.type;
        this.setType(expression, this.maybeCoerce(is(curType, DataTypeExpr) ? curType : undefined, DataTypeExprKind.DOUBLE));
      }
    }
  }

  private annotateDot (expression: DotExpr): void {
    this.setType(expression);

    // Propagate type from qualified UDF calls (e.g., db.my_udf(...))
    const exprRight = expression.args.expression as Expression;
    if (exprRight instanceof AnonymousExpr) {
      const exprRightType = exprRight.type;
      this.setType(expression, is(exprRightType, DataTypeExpr) ? exprRightType : undefined);
      return;
    }

    const thisType = (expression.args.this as Expression)?.type;

    if (thisType && thisType.isType(DataTypeExprKind.STRUCT)) {
      for (const field of (thisType.args.expressions || []) as Expression[]) {
        if (field.name === exprRight?.name) {
          const fieldType = (field as ColumnDefExpr).args.kind as unknown as DataTypeExpr | DataTypeExprKind | undefined;
          this.setType(expression, fieldType);
          break;
        }
      }
    }
  }

  private annotateExtract (expression: ExtractExpr): void {
    const part = expression.name;
    if (part === 'TIME') {
      this.setType(expression, DataTypeExprKind.TIME);
    } else if (part === 'DATE') {
      this.setType(expression, DataTypeExprKind.DATE);
    } else if (part && BIGINT_EXTRACT_DATE_PARTS.has(part.toUpperCase())) {
      this.setType(expression, DataTypeExprKind.BIGINT);
    } else {
      this.setType(expression, DataTypeExprKind.INT);
    }
  }

  private annotateExplode (expression: ExplodeExpr): void {
    const thisExpr = expression.args.this as Expression;
    const elemType = seqGet(thisExpr?.type?.args.expressions as DataTypeExpr[] | undefined ?? [], 0);
    this.setType(expression, elemType);
  }

  private annotateUnnest (expression: UnnestExpr): void {
    const child = seqGet(expression.args.expressions, 0);
    let exprType: DataTypeExpr | undefined;

    if (child && child.isType(DataTypeExprKind.ARRAY)) {
      exprType = seqGet(child.type?.args.expressions as DataTypeExpr[] | undefined ?? [], 0);
    }

    this.setType(expression, exprType);
  }

  private annotateSubquery (expression: SubqueryExpr): void {
    const query = expression.unnest();

    if (query instanceof QueryExpr) {
      const selects = query.selects;
      if (selects.length === 1) {
        const selectType = selects[0].type;
        this.setType(expression, is(selectType, DataTypeExpr) ? selectType : is(selectType, ColumnDefExpr) ? selectType : undefined);
        return;
      }
    }

    this.setType(expression, DataTypeExprKind.UNKNOWN);
  }

  private annotateStructValue (
    expr: Expression,
  ): ColumnDefExpr | DataTypeExpr | DataTypeExprKind | null {
    let nameExpr: Expression | undefined;
    const exprType0 = expr.type;
    let kind: ColumnDefExpr | DataTypeExpr | DataTypeExprKind | undefined = is(exprType0, DataTypeExpr) ? exprType0 : is(exprType0, ColumnDefExpr) ? exprType0 : undefined;

    const alias = expr.args['alias'];
    if (alias instanceof Expression) {
      nameExpr = (alias as Expression).copy();
    } else if (expr.args['expression'] instanceof Expression) {
      // STRUCT(key = value) or STRUCT(key := value)
      nameExpr = (expr.args.this as Expression)?.copy();
      const argExprType = (expr.args['expression'] as Expression)?.type;
      kind = is(argExprType, DataTypeExpr) ? argExprType : is(argExprType, ColumnDefExpr) ? argExprType : undefined;
    } else if (expr instanceof ColumnExpr) {
      // STRUCT(c)
      nameExpr = (expr.args.this as Expression)?.copy();
    }

    if (is(kind, DataTypeExpr) && kind.isType(DataTypeExprKind.UNKNOWN)) {
      return null;
    }

    if (nameExpr) {
      return new ColumnDefExpr({
        this: nameExpr,
        kind: kind as unknown as ColumnDefExprKind,
      });
    }

    return kind ?? DataTypeExprKind.UNKNOWN;
  }

  private annotateStruct (expression: StructExpr): void {
    const expressions: (ColumnDefExpr | DataTypeExpr | DataTypeExprKind)[] = [];

    for (const expr of expression.expressions as Expression[]) {
      const structFieldType = this.annotateStructValue(expr);
      if (structFieldType === null) {
        this.setType(expression);
        return;
      }
      expressions.push(structFieldType);
    }

    this.setType(expression, new DataTypeExpr({
      this: DataTypeExprKind.STRUCT,
      expressions: expressions as unknown as DataTypeExpr[],
      nested: true,
    }));
  }

  private annotateMap (expression: MapExpr | VarMapExpr): void {
    // Get keys/values ArrayExpr
    let keysExpr: ArrayExpr | undefined;
    let valuesExpr: ArrayExpr | undefined;

    if (expression instanceof VarMapExpr) {
      keysExpr = expression.args.keys as ArrayExpr;
      valuesExpr = expression.args.values as ArrayExpr;
    } else {
      // MapExpr: keys/values are Expression[] where [0] is an ArrayExpr
      const keysArr = expression.args['keys'] as Expression[] | undefined;
      const valuesArr = expression.args['values'] as Expression[] | undefined;
      keysExpr = keysArr?.[0] instanceof ArrayExpr ? keysArr[0] as ArrayExpr : undefined;
      valuesExpr = valuesArr?.[0] instanceof ArrayExpr ? valuesArr[0] as ArrayExpr : undefined;
    }

    const mapType = new DataTypeExpr({ this: DataTypeExprKind.MAP });

    if (keysExpr instanceof ArrayExpr && valuesExpr instanceof ArrayExpr) {
      const keyTypeExpr = seqGet(keysExpr.type?.args.expressions as DataTypeExpr[] | undefined ?? [], 0);
      const valueTypeExpr = seqGet(valuesExpr.type?.args.expressions as DataTypeExpr[] | undefined ?? [], 0);
      const keyKind = keyTypeExpr instanceof DataTypeExpr ? keyTypeExpr.this as DataTypeExprKind : keyTypeExpr;
      const valueKind = valueTypeExpr instanceof DataTypeExpr ? valueTypeExpr.this as DataTypeExprKind : valueTypeExpr;

      if (keyKind !== DataTypeExprKind.UNKNOWN && valueKind !== DataTypeExprKind.UNKNOWN) {
        mapType.args.expressions = [keyTypeExpr ?? new DataTypeExpr({ this: DataTypeExprKind.UNKNOWN }), valueTypeExpr ?? new DataTypeExpr({ this: DataTypeExprKind.UNKNOWN })];
        mapType.args['nested'] = true;
      }
    }

    this.setType(expression, mapType);
  }

  private annotateToMap (expression: ToMapExpr): void {
    const mapType = new DataTypeExpr({ this: DataTypeExprKind.MAP });
    const arg = expression.args.this as Expression;

    if (arg && arg.isType(DataTypeExprKind.STRUCT)) {
      for (const colDef of (arg.type?.args.expressions || []) as Expression[]) {
        const kind = (colDef as ColumnDefExpr).args.kind as unknown as DataTypeExpr | DataTypeExprKind | undefined;
        const kindKind = kind instanceof DataTypeExpr ? kind.this as DataTypeExprKind : kind;
        if (kindKind !== DataTypeExprKind.UNKNOWN) {
          mapType.args.expressions = [DataTypeExpr.build(DataTypeExprKind.VARCHAR), kind instanceof DataTypeExpr ? kind : new DataTypeExpr({ this: kindKind ?? DataTypeExprKind.UNKNOWN })];
          mapType.args['nested'] = true;
          break;
        }
      }
    }

    this.setType(expression, mapType);
  }

  annotateByArrayElement (expression: Expression): void {
    const arrayArg = expression.args.this as Expression;
    if (arrayArg?.type?.isType(DataTypeExprKind.ARRAY)) {
      const elemType = seqGet(arrayArg.type.args.expressions as DataTypeExpr[] | undefined ?? [], 0);
      this.setType(expression, elemType ?? DataTypeExprKind.UNKNOWN);
    } else {
      this.setType(expression, DataTypeExprKind.UNKNOWN);
    }
  }
}
