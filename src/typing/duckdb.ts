import type { Expression } from '../expressions';
import {
  DataTypeExprKind,
  BitLengthExpr,
  DayExpr,
  DayOfMonthExpr,
  DayOfWeekExpr,
  DayOfYearExpr,
  HourExpr,
  LengthExpr,
  MinuteExpr,
  MonthExpr,
  QuarterExpr,
  SecondExpr,
  WeekExpr,
  YearExpr,
  FactorialExpr,
  Atan2Expr,
  JarowinklerSimilarityExpr,
  RandExpr,
  TimeToUnixExpr,
  ToDaysExpr,
  TimeFromPartsExpr,
} from '../expressions';
import type { ExpressionMetadata } from '.';
import { EXPRESSION_METADATA as BASE_EXPRESSION_METADATA } from '.';

export const EXPRESSION_METADATA: ExpressionMetadata = (() => {
  // Clone the base metadata map to avoid mutating the global definitions
  const map: ExpressionMetadata = new Map(BASE_EXPRESSION_METADATA);

  const extend = (types: (typeof Expression)[], data: Record<string, unknown>) => {
    for (const type of types) map.set(type, data);
  };

  extend([
    BitLengthExpr,
    DayExpr,
    DayOfMonthExpr,
    DayOfWeekExpr,
    DayOfYearExpr,
    HourExpr,
    LengthExpr,
    MinuteExpr,
    MonthExpr,
    QuarterExpr,
    SecondExpr,
    WeekExpr,
    YearExpr,
  ], { returns: DataTypeExprKind.BIGINT });

  extend([FactorialExpr], { returns: DataTypeExprKind.INT128 });

  extend([
    Atan2Expr,
    JarowinklerSimilarityExpr,
    RandExpr,
    TimeToUnixExpr,
  ], { returns: DataTypeExprKind.DOUBLE });

  map.set(ToDaysExpr, { returns: DataTypeExprKind.INTERVAL });
  map.set(TimeFromPartsExpr, { returns: DataTypeExprKind.TIME });

  return map;
})();
