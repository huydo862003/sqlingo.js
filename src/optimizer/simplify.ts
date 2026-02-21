// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/simplify.py

import { DateTime } from 'luxon';
import type {
  AliasExpr,
  AnonymousExpr,
  ColumnDefExpr,
  ExpressionValueList,
  FromExpr,
  ILikeExpr,
  IntDivExpr,
  LikeExpr,
  ModExpr,
  SubqueryExpr,
  TableAliasExpr,
  TableExpr,
  VarExpr,
} from '../expressions';
import {
  DivExpr,
  Expression,
  AddExpr,
  AllExpr,
  and,
  AndExpr,
  AnyExpr,
  BetweenExpr,
  BinaryExpr,
  BooleanExpr,
  BracketExpr,
  CaseExpr,
  CastExpr,
  CoalesceExpr,
  ColumnExpr,
  ConcatExpr,
  ConcatWsExpr,
  ConditionExpr,
  ConnectorExpr,
  CONSTANTS,
  DataTypeExpr,
  DataTypeExprKind,
  DateAddExpr,
  DateSubExpr,
  DatetimeAddExpr,
  DatetimeSubExpr,
  DateTruncExpr,
  DotExpr,
  DPipeExpr,
  EqExpr,
  FuncExpr,
  GtExpr,
  GteExpr,
  HintExpr,
  IdentifierExpr,
  IfExpr,
  InExpr,
  IntervalExpr,
  IsExpr,
  JoinExpr,
  LambdaExpr,
  LiteralExpr,
  LtExpr,
  LteExpr,
  MulExpr,
  NeqExpr,
  NegExpr,
  NONNULL_CONSTANTS,
  NotExpr,
  NullExpr,
  not,
  NullSafeEqExpr,
  NullSafeNeqExpr,
  or,
  OrExpr,
  paren,
  ParenExpr,
  xor,
  PredicateExpr,
  PropertyEqExpr,
  QueryExpr,
  RandExpr,
  RandnExpr,
  SelectExpr,
  StarExpr,
  StartsWithExpr,
  SubExpr,
  SubqueryPredicateExpr,
  TimestampTruncExpr,
  TsOrDsToDateExpr,
  UnaryExpr,
  WhereExpr,
  XorExpr,
  true_,
  false_,
  JoinExprKind,
  null_,
} from '../expressions';
import {
  Dialect, type DialectType,
} from '../dialects/dialect';
import {
  first, whileChanging,
} from '../helper';
import { ensureSchema } from '../schema';
import { MapBinaryTuple } from '../port_internals/binary_tuple_map';
import { TypeAnnotator } from './annotate_types';
import { normalized } from './normalize';
import {
  findAllInScope, walkInScope,
} from './scope';

const FINAL = 'final';

const SIMPLIFIABLE = [
  BinaryExpr,
  FuncExpr,
  LambdaExpr,
  PredicateExpr,
  UnaryExpr,
];

/**
 * Rewrite sqlglot AST to simplify expressions.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { simplify } from 'sqlglot/optimizer';
 *
 *     const expression = parseOne("TRUE AND TRUE");
 *     simplify(expression).sql();
 *     // 'TRUE'
 *     ```
 *
 * @param expression - Expression to simplify
 * @param options - Simplification options
 * @param options.constantPropagation - Whether the constant propagation rule should be used
 * @param options.coalesceSimplification - Whether the simplify coalesce rule should be used.
 *   This rule tries to remove coalesce functions, which can be useful in certain analyses but
 *   can leave the query more verbose.
 * @param options.dialect - Dialect to use for simplification
 * @returns Simplified expression
 */
export function simplify<E extends Expression> (
  expression: E,
  options: {
    constantPropagation?: boolean;
    coalesceSimplification?: boolean;
    dialect?: DialectType;
  } = {},
): E {
  const {
    constantPropagation = false, coalesceSimplification = false, dialect,
  } = options;
  return new Simplifier({ dialect }).simplify(expression, {
    constantPropagation,
    coalesceSimplification,
  });
}

/**
 * Exception raised when an unsupported unit is encountered during simplification.
 */
export class UnsupportedUnit extends Error {
  constructor (message?: string) {
    super(message);
    this.name = 'UnsupportedUnit';
  }
}

/**
 * Decorator that ignores a simplification function if any of the specified exceptions are raised.
 *
 * @param exceptions - Error classes to catch
 * @returns Method decorator
 */
export function catch_ (...exceptions: (new (...args: never[]) => Error)[]) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return function <This, Args extends any[], Return>(
    target: (this: This, ...args: Args) => Return,
    _context: ClassMethodDecoratorContext<This, (this: This, ...args: Args) => Return>,
  ) {
    return function (this: This, ...args: Args): Return {
      try {
        return target.apply(this, args);
      } catch (error) {
        if (exceptions.some((ExceptionType) => error instanceof ExceptionType)) {
          return args[0] as Return; // Return the expression (first argument)
        }
        throw error;
      }
    };
  };
}

/**
 * Decorator that annotates types on the result if the expression changed.
 * Preserves the original expression's type on the new expression.
 *
 * @returns Method decorator
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function annotateTypesOnChange<This extends Simplifier, Args extends [Expression, ...any[]], Return extends Expression> (
  target: (this: This, ...args: Args) => Return,
  _context: ClassMethodDecoratorContext<This, (this: This, ...args: Args) => Return>,
) {
  return function (this: This, ...args: Args): Return {
    const expression = args[0];
    const newExpression = target.apply(this, args);

    if (newExpression === undefined) {
      return newExpression;
    }

    if (this.annotateNewExpressions && expression !== newExpression) {
      this._annotator.clear();

      const annotated = this._annotator.annotate(newExpression, { annotateScope: false });

      annotated.type = expression.type;

      return annotated as Return;
    }

    return newExpression;
  };
}

/**
 * Flatten nested connectors (AND/OR).
 *
 * A AND (B AND C) -> A AND B AND C
 * A OR (B OR C) -> A OR B OR C
 */
export function flatten (expression: Expression): Expression {
  if (!(expression instanceof ConnectorExpr)) {
    return expression;
  }

  // Iterate over argument values
  const args = expression.args;
  const argValues = Object.values(args);

  for (const node of argValues) {
    if (!(node instanceof Expression)) {
      continue;
    }

    const child = node.unnest();
    if (child._constructor === expression._constructor) {
      node.replace(child);
    }
  }

  return expression;
}

/**
 * Simplify parentheses by removing unnecessary ones based on expression context.
 *
 * @param expression - Expression to simplify
 * @param dialect - SQL dialect
 * @returns Expression with simplified parentheses
 */
export function simplifyParens (expression: Expression, dialect: DialectType): Expression {
  if (!(expression instanceof ParenExpr)) {
    return expression;
  }

  const thisExpr = expression.$this;
  const parent = expression.parent;
  const parentIsPredicate = parent instanceof PredicateExpr;

  if (thisExpr instanceof SelectExpr) {
    return expression;
  }

  if (parent instanceof SubqueryPredicateExpr || parent instanceof BracketExpr) {
    return expression;
  }

  if (
    Dialect.getOrRaise(dialect)._constructor.REQUIRES_PARENTHESIZED_STRUCT_ACCESS
    && parent instanceof DotExpr
    && (parent.right instanceof IdentifierExpr || parent.right instanceof StarExpr)
  ) {
    return expression;
  }

  if (
    !(parent instanceof ConditionExpr || parent instanceof BinaryExpr)
    || parent instanceof ParenExpr
    || (
      !(thisExpr instanceof BinaryExpr)
      && !((thisExpr instanceof NotExpr || thisExpr instanceof IsExpr) && parentIsPredicate)
    )
    || (
      thisExpr instanceof PredicateExpr
      && !(parentIsPredicate || parent instanceof NegExpr)
    )
    || (thisExpr instanceof AddExpr && parent instanceof AddExpr)
    || (thisExpr instanceof MulExpr && parent instanceof MulExpr)
    || (thisExpr instanceof MulExpr && (parent instanceof AddExpr || parent instanceof SubExpr))
  ) {
    return thisExpr;
  }

  return expression;
}

/**
 * Propagate constants for conjunctions in DNF.
 *
 * Example: `SELECT * FROM t WHERE a = b AND b = 5` becomes `SELECT * FROM t WHERE a = 5 AND b = 5`
 *
 * Reference: https://www.sqlite.org/optoverview.html
 *
 * @param expression - Expression to propagate constants in
 * @param options - Options
 * @param options.root - Whether this is the root expression
 * @returns Expression with propagated constants
 */
export function propagateConstants (
  expression: Expression,
  options: { root?: boolean } = {},
): Expression {
  const { root = true } = options;

  if (
    expression instanceof AndExpr
    && (root || !expression.sameParent)
    && normalized(expression, { dnf: true })
  ) {
    const constantMapping = new Map<ColumnExpr, [ColumnExpr, LiteralExpr]>();

    for (const expr of walkInScope(expression, { prune: (node) => node instanceof IfExpr })) {
      if (expr instanceof EqExpr) {
        const l = expr.left;
        const r = expr.right;

        if (l instanceof ColumnExpr && r instanceof LiteralExpr) {
          constantMapping.set(l, [l, r]);
        }
      }
    }

    if (0 < constantMapping.size) {
      for (const column of findAllInScope(expression, [ColumnExpr])) {
        const parent = column.parent;
        const mapping = constantMapping.get(column);
        const [columnObj, constant] = mapping || [undefined, undefined];

        if (
          columnObj !== undefined
          && column !== columnObj
          && !(parent instanceof IsExpr && parent.expression instanceof NullExpr)
        ) {
          column.replace(constant!.copy());
        }
      }
    }
  }

  return expression;
}

/** Check if expression is a number */
function _isNumber (expression: unknown): boolean {
  return expression instanceof Expression && expression.isNumber;
}

/** Check if expression is an interval */
function _isInterval (expression: unknown): expression is IntervalExpr {
  return expression instanceof IntervalExpr && extractInterval(expression) !== undefined;
}

/** Check if expression is a non-null constant */
function _isNonnullConstant (expression: unknown): boolean {
  return NONNULL_CONSTANTS.some((c) => expression instanceof c) || _isDateLiteral(expression);
}

/** Check if expression is a constant */
function _isConstant (expression: unknown): boolean {
  return CONSTANTS.some((c) => expression instanceof c) || _isDateLiteral(expression);
}

type DateRange = [DateTime, DateTime];

/**
 * Get the date range for a DATE_TRUNC equality comparison.
 *
 * Example: `_datetruncRange(DateTime.fromISO('2021-01-01'), 'year')` returns `[DateTime(2021-01-01), DateTime(2022-01-01)]`
 *
 * @param date - The date to get range for
 * @param unit - The time unit
 * @param dialect - SQL dialect
 * @returns Tuple of [min, max) or undefined if value can never be equal for unit
 */
function _datetruncRange (date: DateTime, unit: string, dialect: Dialect): DateRange | undefined {
  const floor = dateFloor(date, unit, dialect);

  if (date.toMillis() !== floor.toMillis()) {
    return undefined;
  }

  return [floor, DateTime.fromMillis(floor.toMillis() + interval(unit))];
}

/**
 * Get the logical expression for a date range.
 *
 * @param left - Left expression
 * @param drange - Date range
 * @param targetType - Target data type
 * @returns Logical expression
 */
function datetruncEqExpression (
  left: Expression,
  drange: DateRange,
  targetType: DataTypeExpr | ColumnDefExpr | undefined,
): Expression {
  return and(
    [
      new GteExpr({
        this: left,
        expression: dateLiteral(drange[0], targetType),
      }),
      new LteExpr({
        this: left,
        expression: dateLiteral(drange[1], targetType),
      }),
    ],
    { copy: false },
  );
}

/**
 * Date trunc equality expression.
 *
 * @param left - Left expression
 * @param date - Date value
 * @param unit - Time unit
 * @param dialect - SQL dialect
 * @param targetType - Target data type
 * @returns Expression or undefined
 */
function datetruncEq (
  left: Expression,
  date: DateTime,
  unit: string,
  dialect: Dialect,
  targetType: DataTypeExpr | ColumnDefExpr | undefined,
): Expression | undefined {
  const drange = _datetruncRange(date, unit, dialect);
  if (!drange) {
    return undefined;
  }

  return datetruncEqExpression(left, drange, targetType);
}

/**
 * Date trunc not equality expression.
 *
 * @param left - Left expression
 * @param date - Date value
 * @param unit - Time unit
 * @param dialect - SQL dialect
 * @param targetType - Target data type
 * @returns Expression or undefined
 */
function datetruncNeq (
  left: Expression,
  date: DateTime,
  unit: string,
  dialect: Dialect,
  targetType: DataTypeExpr | ColumnDefExpr | undefined,
): Expression | undefined {
  const drange = _datetruncRange(date, unit, dialect);
  if (!drange) {
    return undefined;
  }

  return and(
    [
      new LteExpr({
        this: left,
        expression: dateLiteral(drange[0], targetType),
      }),
      new GteExpr({
        this: left,
        expression: dateLiteral(drange[1], targetType),
      }),
    ],
    { copy: false },
  );
}

/** Check if expression is always true */
function alwaysTrue (expression: unknown): boolean {
  return (expression instanceof BooleanExpr && expression.$this)
    || (expression instanceof LiteralExpr && expression.isNumber && !isZero(expression));
}

/** Check if expression is always false */
function alwaysFalse (expression: unknown): boolean {
  return isFalse(expression) || isNull(expression) || isZero(expression);
}

/** Check if expression is zero */
function isZero (expression: unknown): boolean {
  return expression instanceof LiteralExpr && expression.toValue() === 0;
}

/** Check if b is complement of a */
function _isComplement (a: unknown, b: unknown): boolean {
  return b instanceof NotExpr && b.this === a;
}

/** Check if expression is false */
function isFalse (a: unknown): a is BooleanExpr & { $this: false } {
  return a instanceof BooleanExpr && !a.$this;
}

/** Check if expression is null */
function isNull (a: unknown): a is NullExpr {
  return a instanceof NullExpr;
}

/**
 * Evaluate boolean expression with two values.
 *
 * @param expression - Boolean expression to evaluate
 * @param a - First value
 * @param b - Second value
 * @returns Boolean literal or undefined
 */
function evalBoolean (expression: Expression, a: number | string, b: number | string): BooleanExpr | undefined {
  if (expression instanceof EqExpr || expression instanceof IsExpr) {
    return booleanLiteral(a === b);
  }
  if (expression instanceof NeqExpr) {
    return booleanLiteral(a !== b);
  }
  if (expression instanceof GtExpr) {
    return booleanLiteral(b < a);
  }
  if (expression instanceof GteExpr) {
    return booleanLiteral(b <= a);
  }
  if (expression instanceof LtExpr) {
    return booleanLiteral(a < b);
  }
  if (expression instanceof LteExpr) {
    return booleanLiteral(a <= b);
  }
  return undefined;
}

/**
 * Cast value to date.
 *
 * @param value - Value to cast
 * @returns DateTime or undefined
 */
function castAsDate (value: unknown): DateTime | undefined {
  if (DateTime.isDateTime(value)) {
    return value.startOf('day');
  }
  const dt = typeof value === 'string' ? DateTime.fromISO(value) : DateTime.fromMillis(value as number);
  return dt.isValid ? dt.startOf('day') : undefined;
}

/**
 * Cast value to datetime.
 *
 * @param value - Value to cast
 * @returns DateTime or undefined
 */
function castAsDatetime (value: unknown): DateTime | undefined {
  if (DateTime.isDateTime(value)) {
    return value;
  }
  const dt = typeof value === 'string' ? DateTime.fromISO(value) : DateTime.fromMillis(value as number);
  return dt.isValid ? dt : undefined;
}

/**
 * Cast value to specific type.
 *
 * @param value - Value to cast
 * @param to - Target data type
 * @returns Casted value or undefined
 */
function castValue (value: unknown, to: DataTypeExpr): DateTime | undefined {
  if (!value) {
    return undefined;
  }
  if (to.isType(DataTypeExprKind.DATE)) {
    return castAsDate(value);
  }
  if (to.isType(Array.from(DataTypeExpr.TEMPORAL_TYPES))) {
    return castAsDatetime(value);
  }
  return undefined;
}

/**
 * Extract date from cast expression.
 *
 * @param cast - Cast expression
 * @returns DateTime or undefined
 */
function extractDate (cast: unknown): DateTime | undefined {
  let to: DataTypeExpr;

  if (cast instanceof CastExpr && cast.to instanceof DataTypeExpr) {
    to = cast.to;
  } else if (cast instanceof TsOrDsToDateExpr && !cast.args.format) {
    to = new DataTypeExpr({ this: DataTypeExprKind.DATE });
  } else {
    return undefined;
  }

  let value: unknown;
  const thisArg = cast.$this;

  if (thisArg instanceof LiteralExpr) {
    value = thisArg.name;
  } else if (thisArg instanceof CastExpr || thisArg instanceof TsOrDsToDateExpr) {
    value = extractDate(thisArg);
  } else {
    return undefined;
  }

  return castValue(value, to);
}

function _isDateLiteral (expression: unknown): boolean {
  return extractDate(expression) !== undefined;
}

function extractInterval (expression: IntervalExpr): number | undefined {
  if (expression.$this === undefined) {
    return undefined;
  }
  try {
    const n = parseInt(String(expression.$this.toValue()));
    const unit = expression.text('unit').toLowerCase();
    return interval(unit, n);
  } catch (e) {
    if (e instanceof UnsupportedUnit) {
      return undefined;
    }
    throw e;
  }
}

function extractType (...expressions: Expression[]): DataTypeExpr | ColumnDefExpr | undefined {
  let targetType: DataTypeExpr | ColumnDefExpr | undefined;

  for (const expression of expressions) {
    if (expression instanceof CastExpr) {
      targetType = expression.to;
    } else {
      targetType = expression.type;
    }
    if (targetType) {
      break;
    }
  }

  return targetType;
}

function dateLiteral (date: DateTime, targetType?: DataTypeExpr | ColumnDefExpr): Expression {
  let type: DataTypeExprKind;

  if (!targetType || !(targetType.isType(Array.from(DataTypeExpr.TEMPORAL_TYPES)))) {
    type = date.hour === 0 && date.minute === 0 && date.second === 0
      ? DataTypeExprKind.DATE
      : DataTypeExprKind.DATETIME;
  } else {
    type = targetType.this as DataTypeExprKind;
  }

  return new CastExpr({
    this: LiteralExpr.string(date.toISO()),
    to: new DataTypeExpr({ this: type }),
  });
}

function interval (unit: string, n: number = 1): number {
  const u = unit.toLowerCase();
  const duration = DateTime.now();

  switch (u) {
    case 'year':
      return duration.plus({ years: n }).diff(duration).milliseconds;
    case 'quarter':
      return duration.plus({ months: 3 * n }).diff(duration).milliseconds;
    case 'month':
      return duration.plus({ months: n }).diff(duration).milliseconds;
    case 'week':
      return duration.plus({ weeks: n }).diff(duration).milliseconds;
    case 'day':
      return duration.plus({ days: n }).diff(duration).milliseconds;
    case 'hour':
      return duration.plus({ hours: n }).diff(duration).milliseconds;
    case 'minute':
      return duration.plus({ minutes: n }).diff(duration).milliseconds;
    case 'second':
      return duration.plus({ seconds: n }).diff(duration).milliseconds;
    default:
      throw new UnsupportedUnit(`Unsupported unit: ${unit}`);
  }
}

function dateFloor (d: DateTime, unit: string, dialect: Dialect): DateTime {
  switch (unit.toLowerCase()) {
    case 'year':
      return d.set({
        month: 1,
        day: 1,
        hour: 0,
        minute: 0,
        second: 0,
        millisecond: 0,
      });
    case 'quarter': {
      const month = Math.floor((d.month - 1) / 3) * 3 + 1;
      return d.set({
        month,
        day: 1,
        hour: 0,
        minute: 0,
        second: 0,
        millisecond: 0,
      });
    }
    case 'month':
      return d.set({
        day: 1,
        hour: 0,
        minute: 0,
        second: 0,
        millisecond: 0,
      });
    case 'week': {
      const weekStart = d.startOf('week').plus({ days: dialect._constructor.WEEK_OFFSET || 0 });
      return weekStart;
    }
    case 'day':
      return d.set({
        hour: 0,
        minute: 0,
        second: 0,
        millisecond: 0,
      });
    default:
      throw new UnsupportedUnit(`Unsupported unit: ${unit}`);
  }
}

function dateCeil (d: DateTime, unit: string, dialect: Dialect): DateTime {
  const floor = dateFloor(d, unit, dialect);

  if (floor.toMillis() === d.toMillis()) {
    return d;
  }

  return DateTime.fromMillis(floor.toMillis() + interval(unit));
}

function booleanLiteral (condition: boolean): BooleanExpr {
  return condition ? true_() : false_();
}

/**
 * Normalize/canonicalize dependencies.
 */
export class Simplifier {
  readonly dialect: Dialect;
  readonly annotateNewExpressions: boolean;
  readonly _annotator: TypeAnnotator;

  constructor (options: {
    dialect?: DialectType;
    annotateNewExpressions?: boolean;
  } = {}) {
    const {
      dialect, annotateNewExpressions = true,
    } = options;

    this.dialect = Dialect.getOrRaise(dialect);
    this.annotateNewExpressions = annotateNewExpressions;
    this._annotator = new TypeAnnotator({
      schema: ensureSchema(undefined, { dialect: this.dialect }),
      overwriteTypes: false,
    });
  }

  static TINYINT_MIN = -128;
  static TINYINT_MAX = 127;
  static UTINYINT_MIN = 0;
  static UTINYINT_MAX = 255;

  static COMPLEMENT_COMPARISONS: Record<string, typeof Expression> = {
    [LtExpr.key]: GteExpr,
    [GtExpr.key]: LteExpr,
    [LteExpr.key]: GtExpr,
    [GteExpr.key]: LtExpr,
    [EqExpr.key]: NeqExpr,
    [NeqExpr.key]: EqExpr,
  };

  static COMPLEMENT_SUBQUERY_PREDICATES: Record<string, typeof Expression> = {
    [AllExpr.key]: AnyExpr,
    [AnyExpr.key]: AllExpr,
  };

  static LT_Lte = [LtExpr, LteExpr] as const;
  static GT_GtE = [GtExpr, GteExpr] as const;

  static COMPARISONS = [
    ...Simplifier.LT_Lte,
    ...Simplifier.GT_GtE,
    EqExpr,
    NeqExpr,
    IsExpr,
  ] as const;

  static INVERSE_COMPARISONS: Record<string, typeof Expression> = {
    [LtExpr.key]: GtExpr,
    [GtExpr.key]: LtExpr,
    [LteExpr.key]: GteExpr,
    [GteExpr.key]: LteExpr,
  };

  static NONDETERMINISTIC = [RandExpr, RandnExpr] as const;
  static AND_OR = [AndExpr, OrExpr] as const;

  static INVERSE_DATE_OPS: Record<string, typeof Expression> = {
    [DateAddExpr.key]: SubExpr,
    [DateSubExpr.key]: AddExpr,
    [DatetimeAddExpr.key]: SubExpr,
    [DatetimeSubExpr.key]: AddExpr,
  };

  static INVERSE_OPS: Record<string, typeof Expression> = {
    ...Simplifier.INVERSE_DATE_OPS,
    [AddExpr.key]: SubExpr,
    [SubExpr.key]: AddExpr,
  };

  static NULL_OK = [
    NullSafeEqExpr,
    NullSafeNeqExpr,
    PropertyEqExpr,
  ] as const;

  static CONCATS = [ConcatExpr, DPipeExpr] as const;

  static DATETRUNC_BINARY_COMPARISONS: Record<
    string,
    (l: Expression, dt: DateTime, u: string, d: Dialect, t?: DataTypeExpr | ColumnDefExpr) => Expression | undefined
  > = {
    [LtExpr.key]: (l, dt, u, d, t) => {
      const floor = dateFloor(dt, u, d);
      const ceilValue = dt.toMillis() === floor.toMillis() ? dt : DateTime.fromMillis(floor.toMillis() + interval(u));
      return new LtExpr({
        this: l,
        expression: dateLiteral(ceilValue, t),
      });
    },
    [GtExpr.key]: (l, dt, u, d, t) => {
      const floor = dateFloor(dt, u, d);
      return new GteExpr({
        this: l,
        expression: dateLiteral(DateTime.fromMillis(floor.toMillis() + interval(u)), t),
      });
    },
    [LteExpr.key]: (l, dt, u, d, t) => {
      const floor = dateFloor(dt, u, d);
      return new LtExpr({
        this: l,
        expression: dateLiteral(DateTime.fromMillis(floor.toMillis() + interval(u)), t),
      });
    },
    [GteExpr.key]: (l, dt, u, d, t) => {
      const ceil = dateCeil(dt, u, d);
      return new GteExpr({
        this: l,
        expression: dateLiteral(ceil, t),
      });
    },
    [EqExpr.key]: datetruncEq,
    [NeqExpr.key]: datetruncNeq,
  };

  static DATETRUNC_COMPARISONS = new Set([InExpr.key, ...Object.keys(Simplifier.DATETRUNC_BINARY_COMPARISONS)]);

  static DATETRUNCS = [DateTruncExpr, TimestampTruncExpr] as const;

  static SAFE_CONNECTOR_ELIMINATION_RESULT = [ConnectorExpr, BooleanExpr] as const;

  // CROSS joins result in an empty table if the right table is empty.
  // So we can only simplify certain types of joins to CROSS.
  // Or in other words, LEFT JOIN x ON TRUE != CROSS JOIN x
  static JOINS = [
    ['', ''],
    ['', JoinExprKind.INNER],
    [JoinExprKind.RIGHT, ''],
    [JoinExprKind.RIGHT, JoinExprKind.OUTER],
  ] as const;

  simplify<E extends Expression> (
    expression: E,
    options: {
      constantPropagation?: boolean;
      coalesceSimplification?: boolean;
    } = {},
  ): E {
    const {
      constantPropagation = false, coalesceSimplification = false,
    } = options;
    const wheres: WhereExpr[] = [];
    const joins: JoinExpr[] = [];

    for (const node of expression.walk({
      prune: (n) => {
        if (n instanceof ConditionExpr) return true;
        return Boolean(n.meta[FINAL]);
      },
    })) {
      if (node.meta[FINAL]) {
        continue;
      }

      // group by expressions cannot be simplified, for example
      // select x + 1 + 1 FROM y GROUP BY x + 1 + 1
      // the projection must exactly match the group by key
      const nodeArgs = node.args as Record<string, unknown>;
      const group = nodeArgs.group;

      if (group instanceof Expression && node instanceof SelectExpr) {
        const groupExpressions = group.expressions;

        if (groupExpressions) {
          const groups = new Set(groupExpressions);
          group.meta[FINAL] = true;

          const selects = node.selects;
          for (const s of selects) {
            for (const n of s.walk({ prune: (node) => Boolean(node.meta[FINAL]) })) {
              if (groups.has(n)) {
                s.meta[FINAL] = true;
                break;
              }
            }
          }

          const having = node.$having;
          if (having) {
            for (const n of having.walk()) {
              if (groups.has(n)) {
                having.meta[FINAL] = true;
                break;
              }
            }
          }
        }
      }

      if (node instanceof ConditionExpr) {
        const simplified = whileChanging(
          node,
          (e: Expression) => this._simplify(e, constantPropagation, coalesceSimplification),
        );

        if (node === expression) {
          expression = simplified as E;
        }
      } else if (node instanceof WhereExpr) {
        wheres.push(node);
      } else if (node instanceof JoinExpr) {
        // snowflake match_conditions have very strict ordering rules
        const matchCondition = node.$matchCondition;
        if (matchCondition) {
          matchCondition.meta[FINAL] = true;
        }

        joins.push(node);
      }
    }

    for (const where of wheres) {
      if (alwaysTrue(where.$this)) {
        where.pop();
      }
    }

    for (const join of joins) {
      const on = join.$on;
      const using = join.$using;
      const method = join.$method;
      const side = join.$side || '';
      const kind = join.$kind || '';

      if (
        on
        && alwaysTrue(on as Expression)
        && !using
        && !method
        && Simplifier.JOINS.some(([s, k]) => s === side && k === kind)
      ) {
        on.pop();
        join.setArgKey('side', undefined);
        join.setArgKey('kind', JoinExprKind.CROSS);
      }
    }

    return expression;
  }

  _simplify (
    expression: Expression,
    constantPropagation: boolean,
    coalesceSimplification: boolean,
  ): Expression {
    const preTransformationStack: Expression[] = [expression];
    const postTransformationStack: [Expression, Expression | undefined][] = [];

    while (0 < preTransformationStack.length) {
      const original = preTransformationStack.pop()!;
      let node = original;

      if (!SIMPLIFIABLE.some((cls) => node instanceof cls)) {
        if (node instanceof QueryExpr) {
          this.simplify(node, {
            constantPropagation,
            coalesceSimplification,
          });
        }
        continue;
      }

      const parent = node.parent;
      const root = node === expression;

      node = this.rewriteBetween(node);
      node = this.uniqSort(node, { root });
      node = this.absorbAndEliminate(node, { root });
      node = this.simplifyConcat(node);
      node = this.simplifyConditionals(node);

      if (constantPropagation) {
        node = propagateConstants(node, { root });
      }

      if (node !== original) {
        original.replace(node);
      }

      for (const n of node.iterExpressions({ reverse: true })) {
        if (!n.meta[FINAL]) {
          preTransformationStack.push(n);
        }
      }
      postTransformationStack.push([node, parent]);
    }

    while (0 < postTransformationStack.length) {
      const [original, parent] = postTransformationStack.pop()!;
      const root = original === expression;

      // Resets parent, arg_key, index pointers– this is needed because some of the
      // previous transformations mutate the AST, leading to an inconsistent state
      for (const [k, v] of Object.entries(original.args)) {
        original.setArgKey(k, v);
      }

      // Post-order transformations
      let node = this.simplifyNot(original);
      node = flatten(node);
      node = this.simplifyConnectors(node, { root });
      node = this.removeComplements(node, { root });

      if (coalesceSimplification) {
        node = this.simplifyCoalesce(node);
      }
      if (parent) {
        node.parent = parent;
      }

      node = this.simplifyLiterals(node, { root });
      node = this.simplifyEquality(node);
      node = simplifyParens(node, this.dialect);
      node = this.simplifyDatetrunc(node);
      node = this.sortComparison(node);
      node = this.simplifyStartswith(node);

      if (node !== original) {
        original.replace(node);
      }
    }

    return expression;
  }

  /**
   * Absorption and elimination.
   *
   * absorption:
   *     A AND (A OR B) -> A
   *     A OR (A AND B) -> A
   *     A AND (NOT A OR B) -> A AND B
   *     A OR (NOT A AND B) -> A OR B
   * elimination:
   *     (A AND B) OR (A AND NOT B) -> A
   *     (A OR B) AND (A OR NOT B) -> A
   */
  @catch_(UnsupportedUnit)
  absorbAndEliminate (expression: Expression, options: { root?: boolean } = {}): Expression {
    const { root = true } = options;

    if (Simplifier.AND_OR.some((cls) => expression instanceof cls) && (root || !expression.sameParent)) {
      const kind = expression instanceof AndExpr ? OrExpr : AndExpr;
      const ops = Array.from(expression.flatten());

      // Initialize lookup tables:
      // Set of all operands, used to find complements for absorption.
      const opSet = new Set<Expression | string | number | boolean>();
      // Sub-operands, used to find subsets for absorption.
      const subops = new Map<Expression | string | number | boolean, Set<Expression | string | number | boolean>[]>();
      // Pairs of complements, used for elimination.
      // Keyed by (plain, plain) where the first key is the content of the NOT side.
      const pairs = new MapBinaryTuple<[Expression, Expression], [Expression, Expression][]>();

      // Populate the lookup tables
      for (const op of ops) {
        opSet.add(op);

        if (!(op instanceof kind)) {
          // In cases like: A OR (A AND B)
          // Subop will be: ^
          if (!subops.has(op)) {
            subops.set(op, []);
          }
          subops.get(op)!.push(new Set([op]));
          continue;
        }

        // In cases like: (A AND B) OR (A AND B AND C)
        // Subops will be: ^     ^
        const subset = new Set(op.flatten());
        for (const i of subset) {
          if (!subops.has(i)) {
            subops.set(i, []);
          }
          subops.get(i)!.push(subset);
        }

        const [a, b] = op.unnestOperands();
        if (a && b) {
          if (a instanceof NotExpr && a.this instanceof Expression) {
            if (!pairs.has(a.this, b)) {
              pairs.set(a.this, b, []);
            }
            pairs.get(a.this, b)!.push([op, b]);
          }
          if (b instanceof NotExpr && b.this instanceof Expression) {
            if (!pairs.has(b.this, a)) {
              pairs.set(b.this, a, []);
            }
            pairs.get(b.this, a)!.push([op, a]);
          }
        }
      }

      for (const op of ops) {
        if (!(op instanceof kind)) {
          continue;
        }

        const [a, b] = op.unnestOperands();
        if (!a || !b) {
          continue;
        }

        // Absorb
        if (a instanceof NotExpr && opSet.has(a.this as Expression)) {
          a.replace(kind === AndExpr ? true_() : false_());
          continue;
        }
        if (b instanceof NotExpr && opSet.has(b.this as Expression)) {
          b.replace(kind === AndExpr ? true_() : false_());
          continue;
        }

        const superset = new Set(op.flatten());
        // Check if any element in superset has subsets that are proper subsets of superset
        const hasProperSubset = Array.from(superset).some((i) => {
          const subsetsForI = subops.get(i);
          if (!subsetsForI) return false;

          return subsetsForI.some((subset) => {
            // Check if subset is a proper subset of superset (subset < superset)
            if (superset.size <= subset.size) return false;

            for (const item of subset) {
              if (!superset.has(item as Expression)) return false;
            }
            return true;
          });
        });

        if (hasProperSubset) {
          op.replace(kind === AndExpr ? false_() : true_());
          continue;
        }

        // Eliminate: look up (a, b) for (NOT a AND b) pairs, and (b, a) for (NOT b AND a) pairs
        for (const [other, complement] of [...(pairs.get(a, b) || []), ...(pairs.get(b, a) || [])]) {
          op.replace(complement);
          other.replace(complement);
        }
      }
    }

    return expression;
  }

  @annotateTypesOnChange
  simplifyConcat (expression: Expression): Expression {
    // Reduces all groups that contain string literals by concatenating them
    if (!Simplifier.CONCATS.some((cls) => expression instanceof cls)) {
      return expression;
    }

    // We can't reduce a CONCAT_WS call if we don't statically know the separator
    if (expression instanceof ConcatWsExpr) {
      const [firstExpr] = expression.expressions;
      if (!(firstExpr instanceof Expression) || !firstExpr.isString) {
        return expression;
      }
    }

    let sepExpr: Expression | undefined;
    let expressions: ExpressionValueList;
    let sep: string;
    let concatType: typeof ConcatExpr | typeof ConcatWsExpr;
    let args: Record<string, unknown>;

    if (expression instanceof ConcatWsExpr) {
      const [first, ...rest] = expression.expressions;
      sepExpr = first as Expression;
      expressions = rest;
      sep = sepExpr.name;
      concatType = ConcatWsExpr;
      args = {};
    } else {
      expressions = expression.expressions;
      sep = '';
      concatType = ConcatExpr;
      args = {
        safe: (expression as ConcatExpr).$safe,
        coalesce: (expression as ConcatExpr).$coalesce,
      };
    }

    const newArgs: Expression[] = [];
    let currentStringGroup: string[] = [];

    for (const e of 0 < expressions.length ? expressions : Array.from(expression.flatten())) {
      if (!(e instanceof Expression)) continue;
      if (e.isString) {
        currentStringGroup.push(e.name);
      } else {
        if (0 < currentStringGroup.length) {
          newArgs.push(LiteralExpr.string(currentStringGroup.join(sep)));
          currentStringGroup = [];
        }
        newArgs.push(e);
      }
    }

    if (0 < currentStringGroup.length) {
      newArgs.push(LiteralExpr.string(currentStringGroup.join(sep)));
    }

    if (newArgs.length === 1 && newArgs[0].isString) {
      return newArgs[0];
    }

    if (concatType === ConcatWsExpr && sepExpr) {
      return new ConcatWsExpr({ expressions: [sepExpr, ...newArgs] });
    } else if (expression instanceof DPipeExpr) {
      return newArgs.reduce((x, y) => new DPipeExpr({
        this: x,
        expression: y,
      }));
    }

    return new concatType({
      expressions: newArgs,
      ...args,
    });
  }

  @annotateTypesOnChange
  simplifyConditionals (expression: Expression): Expression {
    // Simplifies expressions like IF, CASE if their condition is statically known
    if (expression instanceof CaseExpr) {
      const thisExpr = expression.$this;

      for (const caseIf of expression.$ifs || []) {
        let cond = caseIf.this as Expression;
        if (thisExpr) {
          // Convert CASE x WHEN matching_value ... to CASE WHEN x = matching_value ...
          cond = cond.replace(thisExpr.pop().eq(cond));
        }

        if (alwaysTrue(cond)) {
          return caseIf.getArgKey('true') as Expression;
        }

        if (alwaysFalse(cond)) {
          caseIf.pop();
          const remainingIfs = expression.$ifs;
          if (!remainingIfs || remainingIfs.length === 0) {
            return expression.$default as Expression || null_();
          }
        }
      }
    } else if (expression instanceof IfExpr && !(expression.parent instanceof CaseExpr)) {
      const thisExpr = expression.$this;
      if (alwaysTrue(thisExpr)) {
        return expression.$true;
      }
      if (alwaysFalse(thisExpr)) {
        return expression.$false as Expression || null_();
      }
    }

    return expression;
  }

  /**
   * Demorgan's Law
   * NOT (x OR y) -> NOT x AND NOT y
   * NOT (x AND y) -> NOT x OR NOT y
   */
  @catch_(UnsupportedUnit)
  simplifyNot (expression: Expression): Expression {
    if (expression instanceof NotExpr) {
      const thisExpr = expression.$this;
      if (isNull(thisExpr)) {
        return and([null_(), true_()], { copy: false });
      }

      if (thisExpr instanceof Expression) {
        const complement = Simplifier.COMPLEMENT_COMPARISONS[thisExpr._constructor.key];
        if (complement) {
          let rightExpr = thisExpr.expression;

          if (rightExpr instanceof Expression) {
            const complementSubqueryPredicate = Simplifier.COMPLEMENT_SUBQUERY_PREDICATES[rightExpr._constructor.key];
            if (complementSubqueryPredicate) {
              rightExpr = new complementSubqueryPredicate({ this: rightExpr.this });
            }
          }

          return new complement({
            this: thisExpr.this,
            expression: rightExpr,
          });
        }
      }

      if (thisExpr instanceof ParenExpr) {
        const condition = thisExpr.unnest();
        if (condition instanceof AndExpr) {
          return paren(
            or([not(condition.$this, { copy: false }), not(condition.$expression, { copy: false })], { copy: false }),
            { copy: false },
          );
        }
        if (condition instanceof OrExpr) {
          return paren(
            and([not(condition.$this, { copy: false }), not(condition.$expression, { copy: false })], { copy: false }),
            { copy: false },
          );
        }
        if (isNull(condition)) {
          return and([null_(), true_()], { copy: false });
        }
      }

      if (alwaysTrue(thisExpr)) {
        return false_();
      }
      if (isFalse(thisExpr)) {
        return true_();
      }

      if (thisExpr instanceof NotExpr && this.dialect._constructor.SAFE_TO_ELIMINATE_DOUBLE_NEGATION) {
        const inner = thisExpr.$this;
        if (inner instanceof Expression && inner.isType(DataTypeExprKind.BOOLEAN)) {
          // double negation
          // NOT NOT x -> x, if x is BOOLEAN type
          return inner;
        }
      }
    }

    return expression;
  }

  /**
   * Simplify connector expressions (AND/OR).
   */
  @catch_(UnsupportedUnit)
  simplifyConnectors (expression: Expression, options: { root?: boolean } = {}): Expression {
    const { root = true } = options;

    if (expression instanceof ConnectorExpr) {
      const originalParent = expression.parent;
      expression = this._flatSimplify(expression, (expr, left, right) => {
        if (expr instanceof AndExpr) {
          if (isFalse(left) || isFalse(right)) {
            return false_();
          }
          if (isZero(left) || isZero(right)) {
            return false_();
          }
          if (
            (isNull(left) && isNull(right))
            || (isNull(left) && alwaysTrue(right))
            || (alwaysTrue(left) && isNull(right))
          ) {
            return new NullExpr({});
          }
          if (alwaysTrue(left) && alwaysTrue(right)) {
            return true_();
          }
          if (alwaysTrue(left)) {
            return right;
          }
          if (alwaysTrue(right)) {
            return left;
          }
          return this.simplifyComparison(expr, left, right, { or: false });
        } else if (expr instanceof OrExpr) {
          if (alwaysTrue(left) || alwaysTrue(right)) {
            return true_();
          }
          if (
            (isNull(left) && isNull(right))
            || (isNull(left) && alwaysFalse(right))
            || (alwaysFalse(left) && isNull(right))
          ) {
            return new NullExpr({});
          }
          if (isFalse(left)) {
            return right;
          }
          if (isFalse(right)) {
            return left;
          }
          return this.simplifyComparison(expr, left, right, { or: true });
        }
        return undefined;
      }, { root });

      // If we reduced a connector to, e.g., a column (t1 AND ... AND tn -> Tk), then we need
      // to ensure that the resulting type is boolean. We know this is true only for connectors,
      // boolean values and columns that are essentially operands to a connector:
      //
      // A AND (((B)))
      //          ~ this is safe to keep because it will eventually be part of another connector
      if (!Simplifier.SAFE_CONNECTOR_ELIMINATION_RESULT.some((cls) => expression instanceof cls)
        && !expression.isType(DataTypeExprKind.BOOLEAN)) {
        let current = originalParent;
        while (true) {
          if (current instanceof ConnectorExpr) {
            break;
          }
          if (!(current instanceof ParenExpr)) {
            expression = and([expression, true_()], { copy: false });
            break;
          }
          current = current.parent;
        }
      }
    }

    return expression;
  }

  simplifyComparison (
    expression: Expression,
    left: Expression,
    right: Expression,
    options: { or: boolean },
  ): Expression | undefined {
    const { or: or_ = false } = options;

    if (Simplifier.COMPARISONS.some((cls) => left instanceof cls)
      && Simplifier.COMPARISONS.some((cls) => right instanceof cls)) {
      const ll = left.this;
      const lr = left.expression;
      const rl = right.this;
      const rr = right.expression;

      if (!ll || !lr || !rl || !rr) {
        return undefined;
      }

      const largs = new Set([ll, lr]);
      const rargs = new Set([rl, rr]);

      const matching = new Set([...largs].filter((x) => rargs.has(x)));
      const columns = new Set(
        [...matching].filter((m) =>
          m instanceof Expression && !_isConstant(m) && !m.find(Simplifier.NONDETERMINISTIC)),
      );

      if (0 < matching.size && 0 < columns.size) {
        const largsFiltered = [...largs].filter((x) => !columns.has(x));
        const rargsFiltered = [...rargs].filter((x) => !columns.has(x));

        if (largsFiltered.length === 0 || rargsFiltered.length === 0) {
          return expression;
        }

        let l: unknown;
        let r: unknown;
        try {
          l = first(largsFiltered);
          r = first(rargsFiltered);
        } catch {
          return expression;
        }

        if (!(l instanceof Expression) || !(r instanceof Expression)) {
          return undefined;
        }

        const lExpr = l;
        const rExpr = r;

        let lValue: unknown;
        let rValue: unknown;

        if (lExpr.isNumber && rExpr.isNumber) {
          lValue = lExpr.toValue();
          rValue = rExpr.toValue();
        } else if (lExpr.isString && rExpr.isString) {
          lValue = lExpr.name;
          rValue = rExpr.name;
        } else {
          const lDate = extractDate(lExpr);
          if (!lDate) {
            return undefined;
          }
          const rDate = extractDate(rExpr);
          if (!rDate) {
            return undefined;
          }
          // python won't compare date and datetime, but many engines will upcast
          lValue = castAsDatetime(lDate);
          rValue = castAsDatetime(rDate);
        }

        // Generate permutations
        const permutations: [[Expression, unknown], [Expression, unknown]][] = [[[left, lValue], [right, rValue]], [[right, rValue], [left, lValue]]];

        for (const [[a, av], [b, bv]] of permutations) {
          // Convert DateTime to milliseconds for comparison, fallback to string for other types
          const avNum = typeof av === 'number' ? av : (DateTime.isDateTime(av) ? av.toMillis() : String(av));
          const bvNum = typeof bv === 'number' ? bv : (DateTime.isDateTime(bv) ? bv.toMillis() : String(bv));

          if (Simplifier.LT_Lte.some((cls) => a instanceof cls)
            && Simplifier.LT_Lte.some((cls) => b instanceof cls)) {
            return or_ ? (bvNum < avNum ? left : right) : (avNum <= bvNum ? left : right);
          }
          if (Simplifier.GT_GtE.some((cls) => a instanceof cls)
            && Simplifier.GT_GtE.some((cls) => b instanceof cls)) {
            return or_ ? (avNum < bvNum ? left : right) : (bvNum <= avNum ? left : right);
          }

          // we can't ever shortcut to true because the column could be null
          if (!or_) {
            if (a instanceof LtExpr && Simplifier.GT_GtE.some((cls) => b instanceof cls)) {
              if (avNum <= bvNum) {
                return false_();
              }
            } else if (a instanceof GtExpr && Simplifier.LT_Lte.some((cls) => b instanceof cls)) {
              if (bvNum <= avNum) {
                return false_();
              }
            } else if (a instanceof EqExpr) {
              if (b instanceof LtExpr) {
                return bvNum <= avNum ? false_() : a;
              }
              if (b instanceof LteExpr) {
                return bvNum < avNum ? false_() : a;
              }
              if (b instanceof GtExpr) {
                return avNum <= bvNum ? false_() : a;
              }
              if (b instanceof GteExpr) {
                return avNum < bvNum ? false_() : a;
              }
              if (b instanceof NeqExpr) {
                if (avNum === bvNum) {
                  return false_();
                }
                return a;
              }
            }
          }
        }
      }
    }

    return undefined;
  }

  _flatSimplify (
    expression: Expression,
    simplifyFunc: (expr: Expression, left: Expression, right: Expression) => Expression | undefined,
    options: { root?: boolean },
  ): Expression {
    const { root } = options;

    if (!root && expression.sameParent) {
      return expression;
    }

    const operands = [];
    const queue = Array.from(expression.flatten({ unnest: false }));
    const size = queue.length;

    while (queue.length) {
      const a = queue.shift()!;

      let earlyJump = false;
      for (const b of queue) {
        const result = simplifyFunc(expression, a, b);

        if (result && result !== expression) {
          queue.splice(queue.indexOf(b));
          queue.unshift(result);
          earlyJump = true;
          break;
        }
      }
      if (!earlyJump) {
        operands.push(a);
      }
    }

    if (operands.length < size) {
      return operands.reduce((a, b) => new expression._constructor({
        this: a,
        expression: b,
      }));
    }

    return expression;
  }

  /**
   * Removing complements.
   *
   * A AND NOT A -> FALSE (only for non-NULL A)
   * A OR NOT A -> TRUE (only for non-NULL A)
   */
  @catch_(UnsupportedUnit)
  removeComplements (expression: Expression, options: { root?: boolean } = {}): Expression {
    const { root = true } = options;

    if (Simplifier.AND_OR.some((cls) => expression instanceof cls) && (root || !expression.sameParent)) {
      const ops = new Set(expression.flatten());
      for (const op of ops) {
        if (op instanceof NotExpr) {
          const opThis = op.this;
          if (opThis && ops.has(opThis as Expression)) {
            if (expression.meta.nonnull === true) {
              return expression instanceof AndExpr ? false_() : true_();
            }
          }
        }
      }
    }

    return expression;
  }

  @annotateTypesOnChange
  simplifyCoalesce (expression: Expression): Expression {
    // COALESCE(x) -> x
    if (
      expression instanceof CoalesceExpr
      && (expression.expressions.length === 0 || _isNonnullConstant(expression.$this))
      // COALESCE is also used as a Spark partitioning hint
      && !(expression.parent instanceof HintExpr)
    ) {
      return expression.$this as Expression;
    }

    if (this.dialect._constructor.COALESCE_COMPARISON_NON_STANDARD) {
      return expression;
    }

    if (!Simplifier.COMPARISONS.some((cls) => expression instanceof cls)) {
      return expression;
    }

    let coalesce: CoalesceExpr;
    let other: Expression;

    const left = expression.this;
    const right = expression.expression;

    if (left instanceof CoalesceExpr) {
      coalesce = left;
      other = right as Expression;
    } else if (right instanceof CoalesceExpr) {
      coalesce = right;
      other = left as Expression;
    } else {
      return expression;
    }

    // This transformation is valid for non-constants,
    // but it really only does anything if they are both constants.
    if (!_isConstant(other)) {
      return expression;
    }

    // Find the first constant arg
    let argIndex = -1;
    for (let i = 0; i < coalesce.expressions.length; i++) {
      const arg = coalesce.expressions[i];
      if (_isConstant(arg)) {
        argIndex = i;
        break;
      }
    }

    if (argIndex === -1) {
      return expression;
    }

    const arg = coalesce.expressions[argIndex] as Expression;
    coalesce.setArgKey('expressions', coalesce.expressions.slice(0, argIndex));

    // Remove the COALESCE function. This is an optimization, skipping a simplify iteration,
    // since we already remove COALESCE at the top of this function.
    const coalesceOrThis = 0 < coalesce.expressions.length ? coalesce : coalesce.$this;

    // This expression is more complex than when we started, but it will get simplified further
    return paren(
      or([
        and([not(coalesceOrThis?.is(null_()), { copy: false }), expression.copy()], { copy: false }),
        and([
          coalesceOrThis?.is(null_()),
          new (expression._constructor)({
            this: arg.copy(),
            expression: other.copy(),
          }),
        ], { copy: false }),
      ], { copy: false }),
      { copy: false },
    );
  }

  @catch_(UnsupportedUnit)
  simplifyLiterals (expression: Expression, options: { root?: boolean } = {}): Expression {
    const { root = true } = options;

    if (expression instanceof BinaryExpr && !(expression instanceof ConnectorExpr)) {
      return this._flatSimplify(expression, (expr, left, right) => this._simplifyBinary(expr, left, right), { root });
    }

    if (expression instanceof NegExpr && expression.this instanceof NegExpr) {
      const inner = expression.this;
      return inner.$this;
    }

    if (Simplifier.INVERSE_DATE_OPS[expression._constructor.key]) {
      const thisExpr = expression.this;
      const exprExpr = expression.expression;
      if (thisExpr instanceof Expression && exprExpr instanceof Expression) {
        return this._simplifyBinary(expression, thisExpr, exprExpr) || expression;
      }
    }

    return expression;
  }

  _simplifyIntegerCast (expr: Expression): Expression {
    if (!(expr instanceof CastExpr)) {
      return expr;
    }

    const exprThis = expr.$this;
    let thisExpr: Expression;
    if (exprThis instanceof CastExpr) {
      thisExpr = this._simplifyIntegerCast(exprThis);
    } else {
      thisExpr = exprThis;
    }

    if (thisExpr.isInt) {
      const num = thisExpr.toValue() as number;

      const to = expr.args.to;
      if (!to) {
        return expr;
      }

      // Remove the (up)cast from small (byte-sized) integers in predicates which is side-effect free. Downcasts on any
      // integer type might cause overflow, thus the cast cannot be eliminated and the behavior is
      // engine-dependent
      if (
        (
          Simplifier.TINYINT_MIN <= num && num <= Simplifier.TINYINT_MAX
          && to.$this && DataTypeExpr.SIGNED_INTEGER_TYPES.has(to.$this as DataTypeExprKind)
        ) || (
          Simplifier.UTINYINT_MIN <= num && num <= Simplifier.UTINYINT_MAX
          && to.$this && DataTypeExpr.UNSIGNED_INTEGER_TYPES.has(to.$this as DataTypeExprKind)
        )
      ) {
        return thisExpr;
      }
    }

    return expr;
  }

  _simplifyBinary (
    expression: Expression,
    a: Expression,
    b: Expression,
  ): Expression | undefined {
    if (Simplifier.COMPARISONS.some((cls) => expression instanceof cls)) {
      a = this._simplifyIntegerCast(a);
      b = this._simplifyIntegerCast(b);
    }

    if (expression instanceof IsExpr) {
      let c: Expression;
      let not_: boolean;
      if (b instanceof NotExpr) {
        c = b.$this;
        not_ = true;
      } else {
        c = b;
        not_ = false;
      }

      if (isNull(c)) {
        if (a instanceof LiteralExpr) {
          return not_ ? true_() : false_();
        }
        if (isNull(a)) {
          return not_ ? false_() : true_();
        }
      }
    } else if (Simplifier.NULL_OK.some((cls) => expression instanceof cls)) {
      return undefined;
    } else if ((isNull(a) || isNull(b)) && expression.parent instanceof IfExpr) {
      return null_();
    }

    if (a.isNumber && b.isNumber) {
      const numA = a.toValue() as number;
      const numB = b.toValue() as number;

      if (expression instanceof AddExpr) {
        return LiteralExpr.number(numA + numB);
      }
      if (expression instanceof MulExpr) {
        return LiteralExpr.number(numA * numB);
      }

      // We only simplify Sub, Div if a and b have the same parent because they're not associative
      if (expression instanceof SubExpr) {
        return a.parent === b.parent ? LiteralExpr.number(numA - numB) : undefined;
      }
      if (expression instanceof DivExpr) {
        // engines have differing int div behavior so intdiv is not safe
        if ((Number.isInteger(numA) && Number.isInteger(numB)) || a.parent !== b.parent) {
          return undefined;
        }
        return LiteralExpr.number(numA / numB);
      }

      const boolean = evalBoolean(expression, numA, numB);

      if (boolean) {
        return boolean;
      }
    } else if (a.isString && b.isString) {
      const strA = a.this as string;
      const strB = b.this as string;
      const boolean = evalBoolean(expression, strA, strB);

      if (boolean) {
        return boolean;
      }
    } else if (_isDateLiteral(a) && b instanceof IntervalExpr) {
      const date = extractDate(a);
      const interval = extractInterval(b);
      if (date && interval !== undefined) {
        if (expression instanceof AddExpr || expression instanceof DateAddExpr || expression instanceof DatetimeAddExpr) {
          return dateLiteral(date.plus({ milliseconds: interval }), extractType(a));
        }
        if (expression instanceof SubExpr || expression instanceof DateSubExpr || expression instanceof DatetimeSubExpr) {
          return dateLiteral(date.minus({ milliseconds: interval }), extractType(a));
        }
      }
    } else if (a instanceof IntervalExpr && _isDateLiteral(b)) {
      const interval = extractInterval(a);
      const date = extractDate(b);
      // you cannot subtract a date from an interval
      if (interval !== undefined && date && expression instanceof AddExpr) {
        return dateLiteral(date.plus({ milliseconds: interval }), extractType(b));
      }
    } else if (_isDateLiteral(a) && _isDateLiteral(b)) {
      if (expression instanceof PredicateExpr) {
        const dateA = extractDate(a);
        const dateB = extractDate(b);
        if (dateA && dateB) {
          const boolean = evalBoolean(expression, dateA.toMillis(), dateB.toMillis());
          if (boolean) {
            return boolean;
          }
        }
      }
    }

    return undefined;
  }

  /**
   * Use the subtraction and addition properties of equality to simplify expressions:
   *
   *     x + 1 = 3 becomes x = 2
   *
   * There are two binary operations in the above expression: + and =
   * Here's how we reference all the operands in the code below:
   *
   *     l     r
   *     x + 1 = 3
   *     a   b
   */
  @catch_(UnsupportedUnit)
  simplifyEquality (expression: Expression): Expression {
    if (Simplifier.COMPARISONS.some((cls) => expression instanceof cls)) {
      const l = expression.this;
      const r = expression.expression;

      if (!(l instanceof Expression) || !(r instanceof Expression)) {
        return expression;
      }

      const inverseOp = Simplifier.INVERSE_OPS[l._constructor.key];
      if (!inverseOp) {
        return expression;
      }

      let aPredicate: (e: Expression) => boolean;
      let bPredicate: (e: Expression) => boolean;

      if (r.isNumber) {
        aPredicate = _isNumber;
        bPredicate = _isNumber;
      } else if (_isDateLiteral(r)) {
        aPredicate = _isDateLiteral;
        bPredicate = _isInterval;
      } else {
        return expression;
      }

      let a: Expression;
      let b: Expression;

      if (Simplifier.INVERSE_DATE_OPS[l._constructor.key]) {
        // DateAdd, DateSub, etc.
        a = l.this as Expression;
        b = l.expression as Expression; // interval
      } else {
        // Binary operations
        a = l.this as Expression;
        b = l.expression as Expression;
      }

      if (!a || !b) {
        return expression;
      }

      if (!aPredicate(a) && bPredicate(b)) {
        // Keep as is
      } else if (!aPredicate(b) && bPredicate(a)) {
        // Swap
        [a, b] = [b, a];
      } else {
        return expression;
      }

      return new (expression._constructor)({
        this: a,
        expression: new inverseOp({
          this: r,
          expression: b,
        }),
      });
    }

    return expression;
  }

  private _isDatetruncPredicate (left: unknown, right: unknown): boolean {
    return Simplifier.DATETRUNCS.some((cls) => left instanceof cls) && _isDateLiteral(right);
  }

  // Simplify expressions like `DATE_TRUNC('year', x) >= CAST('2021-01-01' AS DATE)`
  @annotateTypesOnChange
  @catch_(UnsupportedUnit)
  simplifyDatetrunc (expression: Expression): Expression {
    const comparison = expression._constructor;

    if (Simplifier.DATETRUNCS.some((cls) => expression instanceof cls)) {
      const e = expression as DateTruncExpr | TimestampTruncExpr;
      const thisExpr = e.$this;
      const truncType = extractType(thisExpr);
      const date = extractDate(thisExpr);
      if (date && e.$unit) {
        const unit = e.$unit.name.toLowerCase();
        return dateLiteral(dateFloor(date, unit, this.dialect), truncType);
      }
    } else if (!Simplifier.DATETRUNC_COMPARISONS.has(comparison.key)) {
      return expression;
    }

    if (expression instanceof BinaryExpr) {
      const l = expression.$this as Expression;
      const r = expression.$expression as Expression;

      if (!this._isDatetruncPredicate(l, r)) {
        return expression;
      }

      const unit = ((l.args as Record<string, unknown>).unit as Expression).name.toLowerCase();
      const truncArg = l.this as Expression;
      const date = extractDate(r);

      if (!date) {
        return expression;
      }

      const result = Simplifier.DATETRUNC_BINARY_COMPARISONS[comparison.key]?.(
        truncArg,
        date,
        unit,
        this.dialect,
        extractType(r),
      );

      return result || expression;
    }

    if (expression instanceof InExpr) {
      const l = expression.$this as Expression;
      const rs = expression.expressions;

      if (rs && rs.every((r) => this._isDatetruncPredicate(l, r))) {
        const unit = ((l.args as Record<string, unknown>).unit as Expression).name.toLowerCase();

        const ranges: DateRange[] = [];
        for (const r of rs) {
          const date = extractDate(r);
          if (!date) {
            return expression;
          }
          const drange = _datetruncRange(date, unit, this.dialect);
          if (drange) {
            ranges.push(drange);
          }
        }

        if (ranges.length === 0) {
          return expression;
        }

        const targetType = extractType(...rs.filter((r): r is Expression => r instanceof Expression));

        return or(
          ranges.map((drange) => datetruncEqExpression(l, drange, targetType)),
          { copy: false },
        );
      }
    }

    return expression;
  }

  @annotateTypesOnChange
  sortComparison (expression: Expression): Expression {
    if (Object.values(Simplifier.COMPLEMENT_COMPARISONS).some((cls) => expression instanceof cls)) {
      const l = expression.this;
      const r = expression.expression;
      const lColumn = l instanceof ColumnExpr;
      const rColumn = r instanceof ColumnExpr;
      const lConst = _isConstant(l);
      const rConst = _isConstant(r);

      if (
        (lColumn && !rColumn)
        || (rConst && !lConst)
        || r instanceof SubqueryPredicateExpr
      ) {
        return expression;
      }

      if (
        (rColumn && !lColumn)
        || (lConst && !rConst)
        || (gen(r as Expression) < gen(l as Expression))
      ) {
        const inverseOp = Simplifier.INVERSE_COMPARISONS[expression._constructor.key] || expression._constructor;
        return new inverseOp({
          this: r,
          expression: l,
        });
      }
    }

    return expression;
  }

  /**
     * Reduces a prefix check to either TRUE or FALSE if both the string and the
     * prefix are statically known.
     *
     * Example:
     *     simplifyStartswith(parseOne("STARTSWITH('foo', 'f')")).sql()
     *     // 'TRUE'
     */
  @annotateTypesOnChange
  simplifyStartswith (expression: Expression): Expression {
    if (
      expression instanceof StartsWithExpr
      && expression.$this instanceof Expression
      && expression.$expression instanceof Expression
      && expression.$this.isString
      && expression.$expression.isString
    ) {
      const result = expression.$this.name.startsWith(expression.$expression.name);
      return result ? true_() : false_();
    }

    return expression;
  }

  /**
   * Rewrite x BETWEEN y AND z to x >= y AND x <= z.
   *
   * This is done because comparison simplification is only done on lt/lte/gt/gte.
   */
  rewriteBetween (expression: Expression): Expression {
    if (!(expression instanceof BetweenExpr)) {
      return expression;
    }

    const parent = expression.parent;
    const negate = parent instanceof NotExpr;

    // BETWEEN has: this, low, high args
    const betweenThis = expression.$this;
    const low = expression.$low;
    const high = expression.$high;

    // Validate all are Expression instances (not primitives)
    if (!(betweenThis instanceof Expression)) {
      return expression;
    }

    if (!(low instanceof Expression)) {
      return expression;
    }

    if (!(high instanceof Expression)) {
      return expression;
    }

    let result = and([
      new GteExpr({
        this: betweenThis.copy(),
        expression: low,
      }),
      new LteExpr({
        this: betweenThis.copy(),
        expression: high,
      }),
    ], { copy: false });

    if (negate) {
      result = paren(result, { copy: false });
    }

    return result;
  }

  /**
   * Uniq and sort a connector.
   *
   * C AND A AND B AND B -> A AND B AND C
   */
  uniqSort (expression: Expression, options: { root?: boolean } = {}): Expression {
    const { root = true } = options;
    if (!(expression instanceof ConnectorExpr) || (!root && expression.sameParent)) {
      return expression;
    }

    const flattened = Array.from(expression.flatten());

    let resultFunc: typeof and | typeof or | typeof xor;
    let deduped: Map<string, Expression> | undefined;
    let arr: [string, Expression][];

    if (expression instanceof XorExpr) {
      resultFunc = xor;
      // Do not deduplicate XOR as A XOR A != A if A == True
      deduped = undefined;
      arr = flattened.map((e) => [this._genSql(e), e] as [string, Expression]);
    } else {
      resultFunc = expression instanceof AndExpr ? and : or;
      deduped = new Map<string, Expression>();
      for (const e of flattened) {
        const sql = this._genSql(e);
        if (!deduped.has(sql)) {
          deduped.set(sql, e);
        }
      }
      arr = Array.from(deduped.entries());
    }

    // Check if the operands are already sorted, if not sort them
    // A AND C AND B -> A AND B AND C
    for (let i = 1; i < arr.length; i++) {
      if (arr[i][0] < arr[i - 1][0]) {
        const sorted = arr.sort((a, b) => a[0].localeCompare(b[0]));
        expression = resultFunc(sorted.map(([, e]) => e), { copy: false });
        return expression;
      }
    }

    // We didn't have to sort but maybe we need to dedup
    if (deduped && deduped.size < flattened.length) {
      const uniqueOperand = flattened[0];
      if (deduped.size === 1) {
        if (uniqueOperand instanceof Expression) {
          expression = and([uniqueOperand, true_()], { copy: false });
        }
      } else {
        expression = resultFunc(Array.from(deduped.values()), { copy: false });
      }
    }

    return expression;
  }

  /**
   * Simple SQL generation for sorting and deduplication.
   * Uses the Gen class for faster generation than the full generator.
   */
  private _genSql (expression: Expression): string {
    return gen(expression);
  }
}

/**
 * Simple pseudo sql generator for quickly generating sortable and uniq strings.
 *
 * Sorting and deduping sql is a necessary step for optimization. Calling the actual
 * generator is expensive so we have a bare minimum sql generator here.
 *
 * @param expression - the expression to convert into a SQL string
 * @param comments - whether to include the expression's comments
 * @returns SQL string
 */
export function gen (expression: Expression, comments: boolean = false): string {
  return new Gen().gen(expression, comments);
}

class Gen {
  private stack: unknown[] = [];
  private sqls: string[] = [];

  gen (expression: Expression, comments: boolean = false): string {
    this.stack = [expression];
    this.sqls = [];

    while (0 < this.stack.length) {
      const node = this.stack.pop()!;

      if (node instanceof Expression) {
        if (comments && node.comments && 0 < node.comments.length) {
          this.stack.push(` /*${node.comments.join(',')}*/`);
        }

        const expHandlerName = `${node._constructor.key}Sql` as keyof this;

        if (typeof this[expHandlerName] === 'function') {
          (this[expHandlerName] as (e: Expression) => void).call(this, node);
        } else if (node instanceof FuncExpr) {
          this.function(node);
        } else {
          const key = node._constructor.key.toUpperCase();
          this.stack.push(this.args(node) ? `${key} ` : key);
        }
      } else if (Array.isArray(node)) {
        for (let i = node.length - 1; 0 <= i; i--) {
          const n = node[i];
          if (n !== undefined && n !== null) {
            if (Array.isArray(n)) {
              // Handle [key, value] pairs from _args
              const [k, v] = n as [string, Expression | string | number | boolean];
              this.stack.push(v as Expression);
              this.stack.push(k);
            } else {
              this.stack.push(n as Expression);
              this.stack.push(',');
            }
          }
        }
        if (0 < node.length) {
          this.stack.pop(); // Remove trailing comma
        }
      } else {
        if (node !== undefined && node !== null) {
          this.sqls.push(String(node));
        }
      }
    }

    return this.sqls.join('');
  }

  addDql (e: AddExpr): void {
    this.binary(e, ' + ');
  }

  aliasSql (e: AliasExpr): void {
    this.stack.push(
      (e.args as Record<string, unknown>).alias as Expression,
      ' AS ',
      e.$this as Expression,
    );
  }

  andSql (e: AndExpr): void {
    this.binary(e, ' AND ');
  }

  anonymousSql (e: AnonymousExpr): void {
    const thisExpr = e.$this;
    let name: string;

    if (typeof thisExpr === 'string') {
      name = thisExpr.toUpperCase();
    } else if (thisExpr instanceof IdentifierExpr) {
      const idName = thisExpr.$this;
      name = thisExpr.$quoted ? `"${idName}"` : String(idName).toUpperCase();
    } else {
      throw new Error(
        `Anonymous.this expects a string or an Identifier, got '${(thisExpr as Expression | undefined)?._constructor?.name || typeof thisExpr}'.`,
      );
    }

    this.stack.push(
      ')',
      e.$expressions || [],
      '(',
      name,
    );
  }

  betweenSql (e: BetweenExpr): void {
    this.stack.push(
      e.$high as Expression,
      ' AND ',
      e.$low as Expression,
      ' BETWEEN ',
      e.$this as Expression,
    );
  }

  booleanSql (e: BooleanExpr): void {
    this.stack.push(e.$this ? 'TRUE' : 'FALSE');
  }

  bracketSql (e: BracketExpr): void {
    this.stack.push(
      ']',
      e.$expressions || [],
      '[',
      e.$this as Expression,
    );
  }

  columnSql (e: ColumnExpr): void {
    const parts = e.parts;
    for (let i = parts.length - 1; 0 <= i; i--) {
      this.stack.push(parts[i], '.');
    }
    this.stack.pop(); // Remove trailing dot
  }

  datatypeSql (e: DataTypeExpr): void {
    this.args(e, 1);
    const thisValue = e.$this;
    const name = typeof thisValue === 'string' ? thisValue : String(thisValue);
    this.stack.push(`${name} `);
  }

  divSql (e: DivExpr): void {
    this.binary(e, ' / ');
  }

  dotSql (e: DotExpr): void {
    this.binary(e, '.');
  }

  eqSql (e: EqExpr): void {
    this.binary(e, ' = ');
  }

  fromSql (e: FromExpr): void {
    this.stack.push(e.$this as Expression, 'FROM ');
  }

  gtSql (e: GtExpr): void {
    this.binary(e, ' > ');
  }

  gteSql (e: GteExpr): void {
    this.binary(e, ' >= ');
  }

  identifierSql (e: IdentifierExpr): void {
    const thisValue = e.$this;
    const str = String(thisValue);
    this.stack.push(e.$quoted ? `"${str}"` : str);
  }

  ilikeSql (e: ILikeExpr): void {
    this.binary(e, ' ILIKE ');
  }

  inSql (e: InExpr): void {
    this.stack.push(')');
    this.args(e, 1);
    this.stack.push(
      '(',
      ' IN ',
      e.$this,
    );
  }

  intdivSql (e: IntDivExpr): void {
    this.binary(e, ' DIV ');
  }

  isSql (e: IsExpr): void {
    this.binary(e, ' IS ');
  }

  likeSql (e: LikeExpr): void {
    this.binary(e, ' LIKE ');
  }

  literalSql (e: LiteralExpr): void {
    const thisValue = e.$this;
    this.stack.push(e.isString ? `'${thisValue}'` : String(thisValue));
  }

  ltSql (e: LtExpr): void {
    this.binary(e, ' < ');
  }

  lteSql (e: LteExpr): void {
    this.binary(e, ' <= ');
  }

  modSql (e: ModExpr): void {
    this.binary(e, ' % ');
  }

  mulSql (e: MulExpr): void {
    this.binary(e, ' * ');
  }

  negSql (e: NegExpr): void {
    this.unary(e, '-');
  }

  neqSql (e: NeqExpr): void {
    this.binary(e, ' <> ');
  }

  notSql (e: NotExpr): void {
    this.unary(e, 'NOT ');
  }

  nullSql (_e: NullExpr): void {
    this.stack.push('NULL');
  }

  orSql (e: OrExpr): void {
    this.binary(e, ' OR ');
  }

  parenSql (e: ParenExpr): void {
    this.stack.push(
      ')',
      e.$this,
      '(',
    );
  }

  subSql (e: SubExpr): void {
    this.binary(e, ' - ');
  }

  subquerySql (e: SubqueryExpr): void {
    this.args(e, 2);
    const alias = e.$alias;
    if (alias) {
      this.stack.push(alias);
    }
    this.stack.push(')', e.$this, '(');
  }

  tableSql (e: TableExpr): void {
    this.args(e, 4);
    const alias = e.$alias;
    if (alias) {
      this.stack.push(alias as Expression);
    }
    const parts = e.parts;
    for (let i = parts.length - 1; 0 <= i; i--) {
      this.stack.push(parts[i], '.');
    }
    this.stack.pop(); // Remove trailing dot
  }

  tableAliasSql (e: TableAliasExpr): void {
    const columns = e.columns;

    if (columns && 0 < columns.length) {
      this.stack.push(')', columns, '(');
    }

    this.stack.push(e.$this || [], ' AS ');
  }

  varSql (e: VarExpr): void {
    this.stack.push(String(e.this));
  }

  private binary (e: BinaryExpr, op: string): void {
    this.stack.push(e.$expression as Expression, op, e.$this as Expression);
  }

  private unary (e: UnaryExpr, op: string): void {
    this.stack.push(e.this as Expression, op);
  }

  private function (e: FuncExpr): void {
    this.stack.push(
      ')',
      Object.values(e.args),
      '(',
      (e._constructor as typeof FuncExpr).sqlName(),
    );
  }

  private args (node: Expression, argIndex: number = 0): boolean {
    const kvs: [string, Expression | string | number | boolean][] = [];
    const argTypes = Object.keys(node._constructor.availableArgs || {});
    const argsToProcess = 0 < argIndex ? argTypes.slice(argIndex) : argTypes;

    for (const k of argsToProcess) {
      const v = (node.args as Record<string, unknown>)[k];

      if (v !== undefined && v !== null) {
        kvs.push([`:${k}`, v as Expression | string | number | boolean]);
      }
    }

    if (0 < kvs.length) {
      this.stack.push(kvs);
      return true;
    }
    return false;
  }
}
