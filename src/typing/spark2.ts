import {
  Expression,
  DataTypeExpr,
  Atan2Expr,
  RandnExpr,
  FormatExpr,
  RightExpr,
  ConcatExpr,
  PadExpr,
  SubstringExpr,
} from '../expressions/expressions';
import { DataTypeExprKind } from '../expressions/types';
import type { TypeAnnotator } from '../optimizer';
import {
  isInstanceOf, filterInstanceOf,
} from '../port_internals';
import { EXPRESSION_METADATA as HIVE_EXPRESSION_METADATA } from './hive';
import type { ExpressionMetadata } from './dialect';

/**
 * Infers the type of the expression according to the following rules:
 * - If all args are of the same type OR any arg is of targetType, the expr is inferred as such
 * - If any arg is of UNKNOWN type and none of targetType, the expr is inferred as UNKNOWN
 */
function annotateBySimilarArgs (
  this: TypeAnnotator,
  expression: Expression,
  args: Iterable<string>,
  targetType: DataTypeExprKind | DataTypeExpr,
): Expression {
  const expressions: Expression[] = [];

  for (const arg of args) {
    const argExpr = (expression.args as Record<string, unknown>)[arg];
    if (argExpr) {
      const list = Array.isArray(argExpr) ? argExpr : [argExpr];
      expressions.push(...filterInstanceOf(list, Expression));
    }
  }

  let lastDatatype: DataTypeExprKind | DataTypeExpr | undefined;
  let hasUnknown = false;

  for (const expr of expressions) {
    if (expr.isType(DataTypeExprKind.UNKNOWN)) {
      hasUnknown = true;
    } else if (expr.isType(targetType)) {
      hasUnknown = false;
      lastDatatype = targetType;
      break;
    } else {
      const exprType = expr.type;
      lastDatatype = isInstanceOf(exprType, DataTypeExpr) ? exprType : undefined;
    }
  }

  this.setType(
    expression,
    hasUnknown ? DataTypeExprKind.UNKNOWN : lastDatatype,
  );

  return expression;
}

export const EXPRESSION_METADATA: ExpressionMetadata = (() => {
  // Clone the Hive base metadata to apply Spark-specific overrides
  const map: ExpressionMetadata = new Map(HIVE_EXPRESSION_METADATA);

  const extend = (types: (typeof Expression)[], data: Record<string, unknown>) => {
    for (const type of types) map.set(type, data);
  };

  extend([Atan2Expr, RandnExpr], { returns: DataTypeExprKind.DOUBLE });

  extend([FormatExpr, RightExpr], { returns: DataTypeExprKind.VARCHAR });

  map.set(ConcatExpr, {
    annotator: (s: TypeAnnotator, e: ConcatExpr) => annotateBySimilarArgs.call(s, e, ['expressions'], DataTypeExprKind.TEXT),
  });

  map.set(PadExpr, {
    annotator: (s: TypeAnnotator, e: PadExpr) => annotateBySimilarArgs.call(s, e, ['this', 'fillPattern'], DataTypeExprKind.TEXT),
  });

  map.set(SubstringExpr, {
    annotator: (s: TypeAnnotator, e: SubstringExpr) => s.annotateByArgs(e, ['this']),
  });

  return map;
})();
