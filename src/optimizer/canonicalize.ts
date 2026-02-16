// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/canonicalize.py

import type { Expression } from '../expressions';
import {
  AddExpr,
  BetweenExpr,
  CaseExpr,
  CastExpr,
  CoalesceExpr,
  ConcatExpr,
  ConnectorExpr,
  DataTypeExpr,
  DataTypeExprKind,
  DateAddExpr,
  DateDiffExpr,
  DateExpr,
  DateSubExpr,
  DateTruncExpr,
  EQExpr,
  ExtractExpr,
  GTEExpr,
  GTExpr,
  HavingExpr,
  IfExpr,
  IntervalExpr,
  LiteralExpr,
  LTEExpr,
  LTExpr,
  NEQExpr,
  NotExpr,
  NullSafeEQExpr,
  NullSafeNEQExpr,
  OrderedExpr,
  SubExpr,
  TimestampExpr,
  TsOrDsToDateExpr,
  WhereExpr,
} from '../expressions';
import {
  Dialect, type DialectType,
} from '../dialects/dialect';
import {
  isDateUnit, isIsoDate, isIsoDatetime,
} from '../helper';
import {
  annotateTypes, TypeAnnotator,
} from './annotate_types';

/**
 * Converts a SQL expression into a standard form.
 *
 * This method relies on annotate_types because many of the
 * conversions rely on type inference.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { canonicalize } from 'sqlglot/optimizer';
 *
 *     const expression = parseOne("SELECT CAST('2020-01-01' AS DATE)");
 *     canonicalize(expression).sql();
 *     // Removes redundant cast if possible
 *     ```
 *
 * @param expression - The expression to canonicalize
 * @param options - Canonicalization options
 * @param options.dialect - SQL dialect
 * @returns The canonicalized expression
 */
export function canonicalize (
  expression: Expression,
  options: {
    dialect?: DialectType;
  } = {},
): Expression {
  const { dialect } = options;
  const dialectInstance = Dialect.getOrRaise(dialect);

  function _canonicalize (node: Expression): Expression {
    node = addTextToConcat(node);
    node = replaceDateFuncs(node, dialectInstance);
    node = coerceType(node, { promoteToInferredDatetimeType: Dialect.PROMOTE_TO_INFERRED_DATETIME_TYPE });
    node = removeRedundantCasts(node);
    node = ensureBools(node, _replaceIntPredicate);
    node = removeAscendingOrder(node);
    return node;
  }

  return expression.transform(_canonicalize, { copy: false });
}

/**
 * Convert Add expressions with text type to Concat.
 */
export function addTextToConcat (node: Expression): Expression {
  if (node instanceof AddExpr && node.type && node.type.this && DataTypeExpr.TEXT_TYPES.has(node.type.this as DataTypeExprKind)) {
    const left = node.$this;
    const right = node.$expression;

    if (!left || !right) {
      return node;
    }

    return new ConcatExpr({
      expressions: [left as Expression, right as Expression],
      // All known dialects, i.e. Redshift and T-SQL, that support
      // concatenating strings with the + operator do not coalesce NULLs.
      coalesce: false,
    });
  }
  return node;
}

/**
 * Replace date/timestamp functions with CAST when possible.
 */
export function replaceDateFuncs (node: Expression, dialect: Dialect): Expression {
  // DATE() or TsOrDsToDate() with no expressions, no zone, and string argument
  if ((node instanceof DateExpr || node instanceof TsOrDsToDateExpr)) {
    const expressions = node.args.expressions;
    const zone = node instanceof DateExpr ? node.args.zone : undefined;
    const thisArg = node.args.this;

    const hasExpressions = expressions && Array.isArray(expressions) && 0 < expressions.length;

    if (!hasExpressions && !zone && thisArg) {
      const thisExpr = thisArg as Expression;
      if (thisExpr.isString && isIsoDate(thisExpr.name)) {
        return new CastExpr({
          this: thisArg,
          to: new DataTypeExpr({ this: DataTypeExprKind.DATE }),
        });
      }
    }
  }

  // Timestamp() with no zone
  if (node instanceof TimestampExpr) {
    const args = node.args as { zone?: Expression;
      this?: Expression; };
    const zone = args.zone;

    if (!zone) {
      let nodeToUse = node;
      if (!node.type) {
        nodeToUse = annotateTypes(node, { dialect }) as TimestampExpr;
      }

      const thisArg = (nodeToUse.args as { this?: Expression }).this;
      if (!thisArg) {
        return node;
      }

      const targetType = nodeToUse.type || new DataTypeExpr({ this: DataTypeExprKind.TIMESTAMP });

      return new CastExpr({
        this: thisArg,
        to: targetType,
      });
    }
  }

  return node;
}

/**
 * Coercible date operation types.
 */
const COERCIBLE_DATE_OPS = [
  AddExpr,
  SubExpr,
  EQExpr,
  NEQExpr,
  GTExpr,
  GTEExpr,
  LTExpr,
  LTEExpr,
  NullSafeEQExpr,
  NullSafeNEQExpr,
];

/**
 * Coerce types for date operations.
 */
export function coerceType (node: Expression, options: { promoteToInferredDatetimeType?: boolean } = {}): Expression {
  const { promoteToInferredDatetimeType = false } = options;
  // Check if node is a coercible date op
  const isCoercibleOp = COERCIBLE_DATE_OPS.some((opClass) => node instanceof opClass);

  if (isCoercibleOp) {
    const left = node.this;
    const right = node.expression;

    if (left && right) {
      _coerceDate(left as Expression, right as Expression, promoteToInferredDatetimeType);
    }
  } else if (node instanceof BetweenExpr) {
    const thisArg = node.$this;
    const low = node.args.low;

    if (thisArg && low) {
      _coerceDate(thisArg as Expression, low as Expression, promoteToInferredDatetimeType);
    }
  } else if (node instanceof ExtractExpr) {
    const expr = node.$expression;

    if (expr) {
      const exprNode = expr as Expression;
      const isTemporalType = exprNode.type?.this && DataTypeExpr.TEMPORAL_TYPES.has(exprNode.type.this as DataTypeExprKind);
      if (!isTemporalType) {
        _replaceCast(exprNode, DataTypeExprKind.DATETIME);
      }
    }
  } else if (node instanceof DateAddExpr || node instanceof DateSubExpr || node instanceof DateTruncExpr) {
    const thisArg = node.$this;
    const unit = node.args.unit;

    if (thisArg) {
      _coerceTimeunitArg(thisArg as Expression, unit as Expression | undefined);
    }
  } else if (node instanceof DateDiffExpr) {
    _coerceDateDiffArgs(node);
  }

  return node;
}

/**
 * Remove redundant casts.
 */
export function removeRedundantCasts (expression: Expression): Expression {
  if (expression instanceof CastExpr) {
    const thisArg = expression.$this;
    const to = expression.args.to;

    if (thisArg && to) {
      const thisExpr = thisArg as Expression;
      const toExpr = to as DataTypeExpr;

      // Cast where source and target types match
      if (thisExpr.type && toExpr.equals(thisExpr.type)) {
        return thisExpr;
      }
    }
  }

  if (expression instanceof DateExpr || expression instanceof TsOrDsToDateExpr) {
    const thisArg = expression.$this;

    if (thisArg) {
      const thisExpr = thisArg as Expression;

      if (thisExpr.type && thisExpr.type.this === DataTypeExprKind.DATE) {
        const typeExprs = thisExpr.type.args.expressions;
        const hasExpressions = typeExprs && Array.isArray(typeExprs) && 0 < typeExprs.length;

        if (!hasExpressions) {
          return thisExpr;
        }
      }
    }
  }

  return expression;
}

/**
 * Ensure boolean predicates in connector/filter contexts.
 */
export function ensureBools (
  expression: Expression,
  replaceFunc: (expr: Expression) => void,
): Expression {
  if (expression instanceof ConnectorExpr) {
    const left = expression.args.this;
    const right = expression.$expression;

    if (left) replaceFunc(left as Expression);
    if (right) replaceFunc(right as Expression);
  } else if (expression instanceof NotExpr) {
    const thisArg = expression.$this;
    if (thisArg) replaceFunc(thisArg as Expression);
  } else if (expression instanceof IfExpr) {
    // We can't replace num in CASE x WHEN num ..., because it's not the full predicate
    const parent = expression.parent;
    const isCaseWithThis = parent instanceof CaseExpr && parent.args.this;

    if (!isCaseWithThis) {
      const thisArg = expression.$this;
      if (thisArg) replaceFunc(thisArg as Expression);
    }
  } else if (expression instanceof WhereExpr || expression instanceof HavingExpr) {
    const thisArg = expression.this;
    if (thisArg) replaceFunc(thisArg as Expression);
  }

  return expression;
}

/**
 * Remove explicit ASC ordering (it's the default).
 */
export function removeAscendingOrder (expression: Expression): Expression {
  if (expression instanceof OrderedExpr) {
    const args = expression.args as { desc?: boolean };
    const desc = args.desc;

    if (desc === false) {
      // Convert ORDER BY a ASC to ORDER BY a
      args.desc = undefined;
    }
  }

  return expression;
}

/**
 * Coerce date types between two expressions.
 */
function _coerceDate (
  a: Expression,
  b: Expression,
  promoteToInferredDatetimeType: boolean,
): void {
  // Try both permutations (a, b) and (b, a)
  for (const [left, right] of [[a, b], [b, a]]) {
    if (right instanceof IntervalExpr) {
      const unit = right.args.unit;
      _coerceTimeunitArg(left, unit as Expression | undefined);
    }

    const leftType = left.type;
    if (!leftType || !leftType.this || !DataTypeExpr.TEMPORAL_TYPES.has(leftType.this as DataTypeExprKind)) {
      continue;
    }

    const rightType = right.type;
    if (!rightType || !rightType.this || !DataTypeExpr.TEXT_TYPES.has(rightType.this as DataTypeExprKind)) {
      continue;
    }

    let targetType: DataTypeExprKind;

    if (promoteToInferredDatetimeType) {
      let bType: DataTypeExprKind;

      if (right.isString) {
        const dateText = right.name;
        if (isIsoDate(dateText)) {
          bType = DataTypeExprKind.DATE;
        } else if (isIsoDatetime(dateText)) {
          bType = DataTypeExprKind.DATETIME;
        } else {
          bType = leftType.this as DataTypeExprKind;
        }
      } else {
        // If b is not a datetime string, we conservatively promote it to a DATETIME,
        // in order to ensure there are no surprising truncations due to downcasting
        bType = DataTypeExprKind.DATETIME;
      }

      const coercesTo = TypeAnnotator.COERCES_TO.get(leftType.this as DataTypeExprKind);
      targetType = coercesTo && coercesTo.has(bType) ? bType : leftType.this as DataTypeExprKind;
    } else {
      targetType = leftType.this as DataTypeExprKind;
    }

    if (targetType !== leftType.this) {
      _replaceCast(left, targetType);
    }

    _replaceCast(right, targetType);
  }
}

/**
 * Coerce time unit argument to appropriate type.
 */
function _coerceTimeunitArg (
  arg: Expression,
  unit: Expression | undefined,
): Expression {
  if (!arg.type) {
    return arg;
  }

  if (arg.type.this && DataTypeExpr.TEXT_TYPES.has(arg.type.this as DataTypeExprKind)) {
    const dateText = arg.name;
    const isIsoDate_ = isIsoDate(dateText);

    if (isIsoDate_ && isDateUnit(unit)) {
      return arg.replace(new CastExpr({
        this: arg.copy(),
        to: new DataTypeExpr({ this: DataTypeExprKind.DATE }),
      }));
    }

    // An ISO date is also an ISO datetime, but not vice versa
    if (isIsoDate_ || isIsoDatetime(dateText)) {
      return arg.replace(new CastExpr({
        this: arg.copy(),
        to: new DataTypeExpr({ this: DataTypeExprKind.DATETIME }),
      }));
    }
  } else if (arg.type.this === DataTypeExprKind.DATE && !isDateUnit(unit)) {
    return arg.replace(new CastExpr({
      this: arg.copy(),
      to: new DataTypeExpr({ this: DataTypeExprKind.DATETIME }),
    }));
  }

  return arg;
}

/**
 * Coerce DateDiff arguments to temporal types.
 */
function _coerceDateDiffArgs (node: DateDiffExpr): void {
  const thisArg = node.$this;
  const exprArg = node.$expression;

  for (const arg of [thisArg, exprArg]) {
    if (arg) {
      const argExpr = arg as Expression;
      if (argExpr.type?.this && !DataTypeExpr.TEMPORAL_TYPES.has(argExpr.type.this as DataTypeExprKind)) {
        argExpr.replace(new CastExpr({
          this: argExpr.copy(),
          to: new DataTypeExpr({ this: DataTypeExprKind.DATETIME }),
        }));
      }
    }
  }
}

/**
 * Replace node with a cast to the given type.
 */
function _replaceCast (node: Expression, to: DataTypeExprKind): void {
  node.replace(new CastExpr({
    this: node.copy(),
    to: new DataTypeExpr({ this: to }),
  }));
}

/**
 * Replace integer predicates with != 0.
 *
 * This was originally designed for Presto, which has a boolean type.
 * Presto requires: with y as (select true as x) select x = 0 FROM y -- illegal
 */
function _replaceIntPredicate (expression: Expression): void {
  if (expression instanceof CoalesceExpr) {
    const expressions = expression.$expressions;
    if (expressions) {
      for (const child of expressions) {
        _replaceIntPredicate(child);
      }
    }
  } else if (expression.type?.this && DataTypeExpr.INTEGER_TYPES.has(expression.type.this as DataTypeExprKind)) {
    const zero = new LiteralExpr({
      this: '0',
      isString: false,
    });
    expression.replace(new NEQExpr({
      this: expression,
      expression: zero,
    }));
  }
}
