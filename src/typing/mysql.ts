import type { Expression } from '../expressions/expressions';
import { cache } from '../port_internals';
import { DataTypeExprKind } from '../expressions/types';
import {
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
} from '../expressions/expressions';
import type { ExpressionMetadata } from './dialect';
import { DialectTyping } from './dialect';

export class MySQLTyping {
  @cache
  static get EXPRESSION_METADATA (): ExpressionMetadata {
    // Clone the base metadata to apply dialect-specific overrides
    const map: ExpressionMetadata = new Map(DialectTyping.EXPRESSION_METADATA);

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
  }
}
