// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/simplify.py

import { DateTime } from 'luxon';
import type {
  AliasExpr,
  AnonymousExpr,
  ExpressionValue,
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
  ColumnDefExpr,
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
  isType,
} from '../expressions';
import {
  Dialect, type DialectType,
} from '../dialects/dialect';
import {
  expressionValueToOrString,
  first, whileChanging,
} from '../helper';
import { ensureSchema } from '../schema';
import { MapBinaryTuple } from '../port_internals/binary_tuple_map';
import {
  cache,
  isInstanceOf, narrowInstanceOf,
} from '../port_internals';
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
export function annotateTypesOnChange<This extends Simplifier, Args extends [Expression, ...any[]], Return extends ExpressionValue> (
  target: (this: This, ...args: Args) => Return,
  _context: ClassMethodDecoratorContext<This, (this: This, ...args: Args) => Return>,
) {
  return function (this: This, ...args: Args): Return {
    const expression = args[0];
    const newExpression = target.apply(this, args);

    if (newExpression === undefined) {
      return newExpression;
    }

    if (this.annotateNewExpressions && expression !== newExpression && newExpression instanceof Expression) {
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

  const thisExpr = expression.args.this;
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
    return thisExpr ?? expression;
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
          && !(parent instanceof IsExpr && parent.args.expression instanceof NullExpr)
        ) {
          column.replace(constant.copy());
        }
      }
    }
  }

  return expression;
}

/** Check if expression is a number */
function isNumber (expression: unknown): boolean {
  return expression instanceof Expression && expression.isNumber;
}

/** Check if expression is an interval */
function isInterval (expression: unknown): expression is IntervalExpr {
  return expression instanceof IntervalExpr && extractInterval(expression) !== undefined;
}

/** Check if expression is a non-null constant */
function isNonnullConstant (expression: unknown): boolean {
  return NONNULL_CONSTANTS.some((c) => expression instanceof c) || isDateLiteral(expression);
}

/** Check if expression is a constant */
function isConstant (expression: unknown): boolean {
  return CONSTANTS.some((c) => expression instanceof c) || isDateLiteral(expression);
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
function dateTruncRange (date: DateTime, unit: string, dialect: Dialect): DateRange | undefined {
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
function dateTruncEqExpression (
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
function dateTruncEq (
  left: Expression,
  date: DateTime,
  unit: string,
  dialect: Dialect,
  targetType: DataTypeExpr | ColumnDefExpr | undefined,
): Expression | undefined {
  const drange = dateTruncRange(date, unit, dialect);
  if (!drange) {
    return undefined;
  }

  return dateTruncEqExpression(left, drange, targetType);
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
function dateTruncNeq (
  left: Expression,
  date: DateTime,
  unit: string,
  dialect: Dialect,
  targetType: DataTypeExpr | ColumnDefExpr | undefined,
): Expression | undefined {
  const drange = dateTruncRange(date, unit, dialect);
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
  return (expression instanceof BooleanExpr && expression.args.this)
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
  return b instanceof NotExpr && b.args.this === a;
}

/** Check if expression is false */
function isFalse (a: unknown): a is BooleanExpr & { $this: false } {
  return a instanceof BooleanExpr && !a.args.this;
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
  const thisArg = cast.args.this;

  if (thisArg instanceof LiteralExpr) {
    value = thisArg.name;
  } else if (thisArg instanceof CastExpr || thisArg instanceof TsOrDsToDateExpr) {
    value = extractDate(thisArg);
  } else {
    return undefined;
  }

  return castValue(value, to);
}

function isDateLiteral (expression: unknown): boolean {
  return extractDate(expression) !== undefined;
}

function extractInterval (expression: IntervalExpr): number | undefined {
  if (expression.args.this === undefined) {
    return undefined;
  }
  try {
    const n = parseInt(String(expression.args.this.toValue()));
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
      const castTo = expression.to;
      targetType = isInstanceOf(castTo, DataTypeExpr) ? castTo : isInstanceOf(castTo, ColumnDefExpr) ? castTo : undefined;
    } else {
      const exprType = expression.type;
      targetType = isInstanceOf(exprType, DataTypeExpr) ? exprType : isInstanceOf(exprType, ColumnDefExpr) ? exprType : undefined;
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
    type = targetType.args.this as DataTypeExprKind;
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
  @cache
  static get COMPLEMENT_COMPARISONS (): Record<string, typeof Expression> {
    return {
      [LtExpr.key]: GteExpr,
      [GtExpr.key]: LteExpr,
      [LteExpr.key]: GtExpr,
      [GteExpr.key]: LtExpr,
      [EqExpr.key]: NeqExpr,
      [NeqExpr.key]: EqExpr,
    };
  }

  @cache
  static get COMPLEMENT_SUBQUERY_PREDICATES (): Record<string, typeof Expression> {
    return {
      [AllExpr.key]: AnyExpr,
      [AnyExpr.key]: AllExpr,
    };
  }

  @cache
  static get LT_Lte (): readonly [typeof LtExpr, typeof LteExpr] {
    return [LtExpr, LteExpr];
  }

  @cache
  static get GT_GtE (): readonly [typeof GtExpr, typeof GteExpr] {
    return [GtExpr, GteExpr];
  }

  @cache
  static get COMPARISONS (): readonly [typeof LtExpr, typeof LteExpr, typeof GtExpr, typeof GteExpr, typeof EqExpr, typeof NeqExpr, typeof IsExpr] {
    return [
      ...Simplifier.LT_Lte,
      ...Simplifier.GT_GtE,
      EqExpr,
      NeqExpr,
      IsExpr,
    ];
  }

  @cache
  static get INVERSE_COMPARISONS (): Record<string, typeof Expression> {
    return {
      [LtExpr.key]: GtExpr,
      [GtExpr.key]: LtExpr,
      [LteExpr.key]: GteExpr,
      [GteExpr.key]: LteExpr,
    };
  }

  @cache
  static get NONDETERMINISTIC (): readonly [typeof RandExpr, typeof RandnExpr] {
    return [RandExpr, RandnExpr];
  }

  @cache
  static get AND_OR (): readonly [typeof AndExpr, typeof OrExpr] {
    return [AndExpr, OrExpr];
  }

  @cache
  static get INVERSE_DATE_OPS (): Record<string, typeof Expression> {
    return {
      [DateAddExpr.key]: SubExpr,
      [DateSubExpr.key]: AddExpr,
      [DatetimeAddExpr.key]: SubExpr,
      [DatetimeSubExpr.key]: AddExpr,
    };
  }

  @cache
  static get INVERSE_OPS (): Record<string, typeof Expression> {
    return {
      ...Simplifier.INVERSE_DATE_OPS,
      [AddExpr.key]: SubExpr,
      [SubExpr.key]: AddExpr,
    };
  }

  @cache
  static get NULL_OK (): readonly [typeof NullSafeEqExpr, typeof NullSafeNeqExpr, typeof PropertyEqExpr] {
    return [
      NullSafeEqExpr,
      NullSafeNeqExpr,
      PropertyEqExpr,
    ];
  }

  @cache
  static get CONCATS (): readonly [typeof ConcatExpr, typeof DPipeExpr] {
    return [ConcatExpr, DPipeExpr];
  }

  @cache
  static get DATETRUNC_BINARY_COMPARISONS (): Record<string, (l: Expression, dt: DateTime, u: string, d: Dialect, t?: DataTypeExpr | ColumnDefExpr) => Expression | undefined> {
    return {
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
      [EqExpr.key]: dateTruncEq,
      [NeqExpr.key]: dateTruncNeq,
    };
  }

  @cache
  static get DATETRUNC_COMPARISONS (): Set<string> {
    return new Set([InExpr.key, ...Object.keys(Simplifier.DATETRUNC_BINARY_COMPARISONS)]);
  }

  @cache
  static get DATETRUNCS (): readonly [typeof DateTruncExpr, typeof TimestampTruncExpr] {
    return [DateTruncExpr, TimestampTruncExpr];
  }

  @cache
  static get SAFE_CONNECTOR_ELIMINATION_RESULT (): readonly [typeof ConnectorExpr, typeof BooleanExpr] {
    return [ConnectorExpr, BooleanExpr];
  }

  // CROSS joins result in an empty table if the right table is empty.
  // So we can only simplify certain types of joins to CROSS.
  // Or in other words, LEFT JOIN x ON TRUE != CROSS JOIN x
  @cache
  static get JOINS (): readonly (readonly [string, string])[] {
    return [
      ['', ''],
      ['', JoinExprKind.INNER],
      [JoinExprKind.RIGHT, ''],
      [JoinExprKind.RIGHT, JoinExprKind.OUTER],
    ];
  }

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
        const groupExpressions = group.args.expressions;

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

          const having = node.args.having;
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
          (e: Expression) => this._simplify(e, {
            constantPropagation,
            coalesceSimplification,
          }),
        );

        if (node === expression) {
          expression = simplified as E;
        }
      } else if (node instanceof WhereExpr) {
        wheres.push(node);
      } else if (node instanceof JoinExpr) {
        // snowflake match_conditions have very strict ordering rules
        const matchCondition = node.args.matchCondition;
        if (matchCondition) {
          matchCondition.meta[FINAL] = true;
        }

        joins.push(node);
      }
    }

    for (const where of wheres) {
      if (alwaysTrue(where.args.this)) {
        where.pop();
      }
    }

    for (const join of joins) {
      const on = join.args.on;
      const using = join.args.using;
      const method = join.args.method;
      const side = join.args.side || '';
      const kind = join.args.kind || '';

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
    options: { constantPropagation: boolean;
      coalesceSimplification: boolean; },
  ): Expression {
    const {
      constantPropagation, coalesceSimplification,
    } = options;
    const preTransformationStack: Expression[] = [expression];
    const postTransformationStack: [Expression, Expression | undefined][] = [];

    while (0 < preTransformationStack.length) {
      const original = preTransformationStack.pop();
      if (!original) continue;
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
        original?.replace(node);
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
        original?.setArgKey(k, v);
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
          if (a instanceof NotExpr && a.args.this instanceof Expression) {
            if (!pairs.has(a.args.this, b)) {
              pairs.set(a.args.this, b, []);
            }
            pairs.get(a.args.this, b)?.push([op, b]);
          }
          if (b instanceof NotExpr && b.args.this instanceof Expression) {
            if (!pairs.has(b.args.this, a)) {
              pairs.set(b.args.this, a, []);
            }
            pairs.get(b.args.this, a)?.push([op, a]);
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
        if (a instanceof NotExpr && opSet.has(a.args.this as Expression)) {
          a.replace(kind === AndExpr ? true_() : false_());
          continue;
        }
        if (b instanceof NotExpr && opSet.has(b.args.this as Expression)) {
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
      const [firstExpr] = expression.args.expressions ?? [];
      if (!(firstExpr instanceof Expression) || !firstExpr.isString) {
        return expression;
      }
    }

    let sepExpr: Expression | undefined;
    let expressions: (ExpressionValue | ExpressionValueList)[];
    let sep: string;
    let concatType: typeof ConcatExpr | typeof ConcatWsExpr;
    let args: Record<string, unknown>;

    if (expression instanceof ConcatWsExpr) {
      const [first, ...rest] = expression.args.expressions ?? [];
      sepExpr = first as Expression;
      expressions = rest;
      sep = sepExpr.name;
      concatType = ConcatWsExpr;
      args = {};
    } else {
      expressions = expression.args.expressions ?? [];
      sep = '';
      concatType = ConcatExpr;
      args = {
        safe: (expression as ConcatExpr).args.safe,
        coalesce: (expression as ConcatExpr).args.coalesce,
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
      const thisExpr = expression.args.this;

      for (const caseIf of expression.args.ifs || []) {
        let cond = caseIf.args.this as Expression;
        if (thisExpr) {
          // Convert CASE x WHEN matching_value ... to CASE WHEN x = matching_value ...
          cond = cond.replace(thisExpr.pop().eq(cond));
        }

        if (alwaysTrue(cond)) {
          return caseIf.getArgKey('true') as Expression;
        }

        if (alwaysFalse(cond)) {
          caseIf.pop();
          const remainingIfs = expression.args.ifs;
          if (!remainingIfs || remainingIfs.length === 0) {
            return expression.args.default as Expression || null_();
          }
        }
      }
    } else if (expression instanceof IfExpr && !(expression.parent instanceof CaseExpr)) {
      const thisExpr = expression.args.this;
      if (alwaysTrue(thisExpr)) {
        return narrowInstanceOf(expression.args.true, Expression) ?? expression;
      }
      if (alwaysFalse(thisExpr)) {
        return narrowInstanceOf(expression.args.false, Expression) ?? null_();
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
      const thisExpr = expression.args.this;
      if (isNull(thisExpr)) {
        return and([null_(), true_()], { copy: false });
      }

      if (thisExpr instanceof Expression) {
        const complement = Simplifier.COMPLEMENT_COMPARISONS[thisExpr._constructor.key];
        if (complement) {
          let rightExpr = thisExpr.args.expression;

          if (rightExpr instanceof Expression) {
            const complementSubqueryPredicate = Simplifier.COMPLEMENT_SUBQUERY_PREDICATES[rightExpr._constructor.key];
            if (complementSubqueryPredicate) {
              rightExpr = new complementSubqueryPredicate({ this: rightExpr.args.this });
            }
          }

          return new complement({
            this: thisExpr.args.this,
            expression: rightExpr,
          });
        }
      }

      if (thisExpr instanceof ParenExpr) {
        const condition = thisExpr.unnest();
        if (condition instanceof AndExpr) {
          return paren(
            or([
              not(
                condition.args.this !== undefined ? expressionValueToOrString(condition.args.this) : undefined,
                { copy: false },
              ),
              not(
                condition.args.expression !== undefined ? expressionValueToOrString(condition.args.expression) : undefined,
                { copy: false },
              ),
            ], { copy: false }),
            { copy: false },
          );
        }
        if (condition instanceof OrExpr) {
          return paren(
            and([
              not(
                condition.args.this,
                { copy: false },
              ),
              not(
                condition.args.expression !== undefined ? expressionValueToOrString(condition.args.expression) : undefined,
                { copy: false },
              ),
            ], { copy: false }),
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
        const inner = thisExpr.args.this;
        if (isType(inner, DataTypeExprKind.BOOLEAN)) {
          // double negation
          // NOT NOT x -> x, if x is BOOLEAN type
          return inner ?? expression;
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
      expression = this.flatSimplify(expression, (expr, left, right) => {
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
            return null_();
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
            return null_();
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
      const ll = left.args.this;
      const lr = left.args.expression;
      const rl = right.args.this;
      const rr = right.args.expression;

      if (!ll || !lr || !rl || !rr) {
        return undefined;
      }

      const largs = new Set([ll, lr]);
      const rargs = new Set([rl, rr]);

      const matching = new Set([...largs].filter((x) => rargs.has(x)));
      const columns = new Set(
        [...matching].filter((m) =>
          m instanceof Expression && !isConstant(m) && !m.find(Simplifier.NONDETERMINISTIC)),
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

  flatSimplify (
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
          const opThis = op.args.this;
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
      && (expression.args.expressions?.length === 0 || isNonnullConstant(expression.args.this))
      // COALESCE is also used as a Spark partitioning hint
      && !(expression.parent instanceof HintExpr)
    ) {
      return expression.args.this as Expression;
    }

    if (this.dialect._constructor.COALESCE_COMPARISON_NON_STANDARD) {
      return expression;
    }

    if (!Simplifier.COMPARISONS.some((cls) => expression instanceof cls)) {
      return expression;
    }

    let coalesce: CoalesceExpr;
    let other: Expression;

    const left = expression.args.this;
    const right = expression.args.expression;

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
    if (!isConstant(other)) {
      return expression;
    }

    // Find the first constant arg
    let argIndex = -1;
    for (let i = 0; i < (coalesce.args.expressions?.length ?? 0); i++) {
      const arg = coalesce.args.expressions?.[i];
      if (isConstant(arg)) {
        argIndex = i;
        break;
      }
    }

    if (argIndex === -1) {
      return expression;
    }

    const arg = coalesce.args.expressions?.[argIndex] as Expression;
    coalesce.setArgKey('expressions', coalesce.args.expressions?.slice(0, argIndex));

    // Remove the COALESCE function. This is an optimization, skipping a simplify iteration,
    // since we already remove COALESCE at the top of this function.
    const coalesceOrThis = 0 < (coalesce.args.expressions?.length ?? 0) ? coalesce : coalesce.args.this;

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
      return this.flatSimplify(expression, (expr, left, right) => this.simplifyBinary(expr, left, right), { root });
    }

    if (expression instanceof NegExpr && expression.args.this instanceof NegExpr) {
      const inner = expression.args.this;
      return inner.args.this ?? expression;
    }

    if (Simplifier.INVERSE_DATE_OPS[expression._constructor.key]) {
      const thisExpr = expression.args.this;
      const exprExpr = expression.args.expression;
      if (thisExpr instanceof Expression && exprExpr instanceof Expression) {
        return this.simplifyBinary(expression, thisExpr, exprExpr) || expression;
      }
    }

    return expression;
  }

  simplifyIntegerCast (expr: Expression): Expression {
    if (!(expr instanceof CastExpr)) {
      return expr;
    }

    const exprThis = expr.args.this;
    let thisExpr: ExpressionValue | undefined;
    if (exprThis instanceof CastExpr) {
      thisExpr = this.simplifyIntegerCast(exprThis);
    } else {
      thisExpr = exprThis;
    }

    if (thisExpr instanceof Expression && thisExpr.isInteger) {
      const num = thisExpr.toValue() as number;

      const to = expr.args.to;
      if (!to) {
        return expr;
      }

      // Remove the (up)cast from small (byte-sized) integers in predicates which is side-effect free. Downcasts on any
      // integer type might cause overflow, thus the cast cannot be eliminated and the behavior is
      // engine-dependent
      if (
        to instanceof Expression && (
          (
            Simplifier.TINYINT_MIN <= num && num <= Simplifier.TINYINT_MAX
            && to.args.this && DataTypeExpr.SIGNED_INTEGER_TYPES.has(to.args.this as DataTypeExprKind)
          ) || (
            Simplifier.UTINYINT_MIN <= num && num <= Simplifier.UTINYINT_MAX
            && to.args.this && DataTypeExpr.UNSIGNED_INTEGER_TYPES.has(to.args.this as DataTypeExprKind)
          )
        )
      ) {
        return thisExpr;
      }
    }

    return expr;
  }

  simplifyBinary (
    expression: Expression,
    a: Expression,
    b: Expression,
  ): Expression | undefined {
    if (Simplifier.COMPARISONS.some((cls) => expression instanceof cls)) {
      a = this.simplifyIntegerCast(a);
      b = this.simplifyIntegerCast(b);
    }

    if (expression instanceof IsExpr) {
      let c: Expression | undefined;
      let not_: boolean;
      if (b instanceof NotExpr) {
        c = b.args.this;
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
      const strA = a.args.this as string;
      const strB = b.args.this as string;
      const boolean = evalBoolean(expression, strA, strB);

      if (boolean) {
        return boolean;
      }
    } else if (isDateLiteral(a) && b instanceof IntervalExpr) {
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
    } else if (a instanceof IntervalExpr && isDateLiteral(b)) {
      const interval = extractInterval(a);
      const date = extractDate(b);
      // you cannot subtract a date from an interval
      if (interval !== undefined && date && expression instanceof AddExpr) {
        return dateLiteral(date.plus({ milliseconds: interval }), extractType(b));
      }
    } else if (isDateLiteral(a) && isDateLiteral(b)) {
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
      const l = expression.args.this;
      const r = expression.args.expression;

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
        aPredicate = isNumber;
        bPredicate = isNumber;
      } else if (isDateLiteral(r)) {
        aPredicate = isDateLiteral;
        bPredicate = isInterval;
      } else {
        return expression;
      }

      let a: Expression;
      let b: Expression;

      if (Simplifier.INVERSE_DATE_OPS[l._constructor.key]) {
        // DateAdd, DateSub, etc.
        a = l.args.this as Expression;
        b = l.args.expression as Expression; // interval
      } else {
        // Binary operations
        a = l.args.this as Expression;
        b = l.args.expression as Expression;
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

  private isDatetruncPredicate (left: unknown, right: unknown): boolean {
    return Simplifier.DATETRUNCS.some((cls) => left instanceof cls) && isDateLiteral(right);
  }

  // Simplify expressions like `DATE_TRUNC('year', x) >= CAST('2021-01-01' AS DATE)`
  @annotateTypesOnChange
  @catch_(UnsupportedUnit)
  simplifyDatetrunc (expression: Expression): Expression {
    const comparison = expression._constructor;

    if (Simplifier.DATETRUNCS.some((cls) => expression instanceof cls)) {
      const e = expression as DateTruncExpr | TimestampTruncExpr;
      const thisExpr = e.args.this;
      const truncType = thisExpr && extractType(thisExpr);
      const date = extractDate(thisExpr);
      if (date && e.args.unit) {
        const unit = e.args.unit.name.toLowerCase();
        return dateLiteral(dateFloor(date, unit, this.dialect), truncType);
      }
    } else if (!Simplifier.DATETRUNC_COMPARISONS.has(comparison.key)) {
      return expression;
    }

    if (expression instanceof BinaryExpr) {
      const l = expression.args.this as Expression;
      const r = expression.args.expression as Expression;

      if (!this.isDatetruncPredicate(l, r)) {
        return expression;
      }

      const unit = ((l.args as Record<string, unknown>).unit as Expression).name.toLowerCase();
      const truncArg = l.args.this as Expression;
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
      const l = expression.args.this as Expression;
      const rs = expression.args.expressions;

      if (rs && rs.every((r) => this.isDatetruncPredicate(l, r))) {
        const unit = ((l.args as Record<string, unknown>).unit as Expression).name.toLowerCase();

        const ranges: DateRange[] = [];
        for (const r of rs) {
          const date = extractDate(r);
          if (!date) {
            return expression;
          }
          const drange = dateTruncRange(date, unit, this.dialect);
          if (drange) {
            ranges.push(drange);
          }
        }

        if (ranges.length === 0) {
          return expression;
        }

        const targetType = extractType(...rs.filter((r): r is Expression => r instanceof Expression));

        return or(
          ranges.map((drange) => dateTruncEqExpression(l, drange, targetType)),
          { copy: false },
        );
      }
    }

    return expression;
  }

  @annotateTypesOnChange
  sortComparison (expression: Expression): Expression {
    if (Object.values(Simplifier.COMPLEMENT_COMPARISONS).some((cls) => expression instanceof cls)) {
      const l = expression.args.this;
      const r = expression.args.expression;
      const lColumn = l instanceof ColumnExpr;
      const rColumn = r instanceof ColumnExpr;
      const lConst = isConstant(l);
      const rConst = isConstant(r);

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
      && expression.args.this instanceof Expression
      && expression.args.expression instanceof Expression
      && expression.args.this.isString
      && expression.args.expression.isString
    ) {
      const result = expression.args.this.name.startsWith(expression.args.expression.name);
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
    const betweenThis = expression.args.this;
    const low = expression.args.low;
    const high = expression.args.high;

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
      arr = flattened.map((e) => [this.genSql(e), e] as [string, Expression]);
    } else {
      resultFunc = expression instanceof AndExpr ? and : or;
      deduped = new Map<string, Expression>();
      for (const e of flattened) {
        const sql = this.genSql(e);
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
  private genSql (expression: Expression): string {
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
export function gen (expression: Expression, options: { comments?: boolean } = {}): string {
  return new Gen().gen(expression, options);
}

class Gen {
  private stack: unknown[] = [];
  private sqls: string[] = [];

  gen (expression: Expression, options: { comments?: boolean } = {}): string {
    const { comments = false } = options;
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
      e.args.this as Expression,
    );
  }

  andSql (e: AndExpr): void {
    this.binary(e, ' AND ');
  }

  anonymousSql (e: AnonymousExpr): void {
    const thisExpr = e.args.this;
    let name: string;

    if (typeof thisExpr === 'string') {
      name = thisExpr.toUpperCase();
    } else if (thisExpr instanceof IdentifierExpr) {
      const idName = thisExpr.args.this;
      name = thisExpr.args.quoted ? `"${idName}"` : String(idName).toUpperCase();
    } else {
      throw new Error(
        `Anonymous.args.this expects a string or an Identifier, got '${(thisExpr as Expression | undefined)?._constructor?.name || typeof thisExpr}'.`,
      );
    }

    this.stack.push(
      ')',
      e.args.expressions || [],
      '(',
      name,
    );
  }

  betweenSql (e: BetweenExpr): void {
    this.stack.push(
      e.args.high as Expression,
      ' AND ',
      e.args.low as Expression,
      ' BETWEEN ',
      e.args.this as Expression,
    );
  }

  booleanSql (e: BooleanExpr): void {
    this.stack.push(e.args.this ? 'TRUE' : 'FALSE');
  }

  bracketSql (e: BracketExpr): void {
    this.stack.push(
      ']',
      e.args.expressions || [],
      '[',
      e.args.this as Expression,
    );
  }

  columnSql (e: ColumnExpr): void {
    const parts = e.parts;
    for (let i = parts.length - 1; 0 <= i; i--) {
      this.stack.push(parts[i], '.');
    }
    this.stack.pop(); // Remove trailing dot
  }

  dataTypeSql (e: DataTypeExpr): void {
    this.args(e, 1);
    const thisValue = e.args.this;
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
    this.stack.push(e.args.this as Expression, 'FROM ');
  }

  gtSql (e: GtExpr): void {
    this.binary(e, ' > ');
  }

  gteSql (e: GteExpr): void {
    this.binary(e, ' >= ');
  }

  identifierSql (e: IdentifierExpr): void {
    const thisValue = e.args.this;
    const str = String(thisValue);
    this.stack.push(e.args.quoted ? `"${str}"` : str);
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
      e.args.this,
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
    const thisValue = e.args.this;
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
      e.args.this,
      '(',
    );
  }

  subSql (e: SubExpr): void {
    this.binary(e, ' - ');
  }

  subquerySql (e: SubqueryExpr): void {
    this.args(e, 2);
    const alias = e.args.alias;
    if (alias) {
      this.stack.push(alias);
    }
    this.stack.push(')', e.args.this, '(');
  }

  tableSql (e: TableExpr): void {
    this.args(e, 4);
    const alias = e.args.alias;
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

    this.stack.push(e.args.this || [], ' AS ');
  }

  varSql (e: VarExpr): void {
    this.stack.push(String(e.args.this));
  }

  private binary (e: BinaryExpr, op: string): void {
    this.stack.push(e.args.expression as Expression, op, e.args.this as Expression);
  }

  private unary (e: UnaryExpr, op: string): void {
    this.stack.push(e.args.this as Expression, op);
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
    const kvs: [string, ExpressionValue][] = [];
    const argTypes = Object.keys(node._constructor.availableArgs || {});
    const argsToProcess = 0 < argIndex ? argTypes.slice(argIndex) : argTypes;

    for (const k of argsToProcess) {
      const v = node.getArgKey(k);

      if (v !== undefined && v !== null) {
        kvs.push([`:${k}`, v as ExpressionValue]);
      }
    }

    if (0 < kvs.length) {
      this.stack.push(kvs);
      return true;
    }
    return false;
  }
}
