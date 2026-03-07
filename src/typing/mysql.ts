import type { Expression } from '../expressions';
import {
  DataTypeExprKind,
  Atan2Expr,
  DegreesExpr,
  CurrentVersionExpr,
  EltExpr,
  DayOfMonthExpr,
  DayOfWeekExpr,
  DayOfYearExpr,
  MonthExpr,
  SecondExpr,
  WeekExpr,
  LocaltimeExpr,
} from '../expressions';
import type { ExpressionMetadata } from './dialect';
import { EXPRESSION_METADATA as BASE_EXPRESSION_METADATA } from './dialect';

export const EXPRESSION_METADATA: ExpressionMetadata = (() => {
  // Clone the base metadata to apply dialect-specific overrides
  const map: ExpressionMetadata = new Map(BASE_EXPRESSION_METADATA);

  const extend = (types: (typeof Expression)[], data: Record<string, unknown>) => {
    for (const type of types) map.set(type, data);
  };

  extend([Atan2Expr, DegreesExpr], { returns: DataTypeExprKind.DOUBLE });

  extend([CurrentVersionExpr, EltExpr], { returns: DataTypeExprKind.VARCHAR });

  extend([
    DayOfMonthExpr,
    DayOfWeekExpr,
    DayOfYearExpr,
    MonthExpr,
    SecondExpr,
    WeekExpr,
  ], { returns: DataTypeExprKind.INT });

  map.set(LocaltimeExpr, { returns: DataTypeExprKind.DATETIME });

  return map;
})();
