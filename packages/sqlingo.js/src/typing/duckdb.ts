import type {
  Expression,
} from '../expressions/expressions';
import {
  cache,
} from '../port_internals';
import {
  DataTypeExprKind,
} from '../expressions/types';
import {
  BitLengthExpr,
  DateDiffExpr,
  DayExpr,
  DayOfMonthExpr,
  DayOfWeekExpr,
  DayOfWeekIsoExpr,
  DayOfYearExpr,
  HourExpr,
  LengthExpr,
  MinuteExpr,
  MonthExpr,
  QuarterExpr,
  SecondExpr,
  WeekExpr,
  YearExpr,
  CountIfExpr,
  FactorialExpr,
  Atan2Expr,
  JarowinklerSimilarityExpr,
  TimeToUnixExpr,
  FormatExpr,
  ReverseExpr,
  DateBinExpr,
  LocaltimestampExpr,
  ToDaysExpr,
  TimeFromPartsExpr,
} from '../expressions/expressions';
import type {
  TypeAnnotator,
} from '../optimizer';
import type {
  ExpressionMetadata,
} from './dialect';
import {
  DialectTyping,
} from './dialect';

export class DuckDbTyping {
  @cache
  static get EXPRESSION_METADATA (): ExpressionMetadata {
    // Clone the base metadata map to avoid mutating the global definitions
    const map: ExpressionMetadata = new Map(DialectTyping.EXPRESSION_METADATA);

    const extend = (types: (typeof Expression)[], data: Record<string, unknown>) => {
      for (const type of types) map.set(type, data);
    };

    extend([
      BitLengthExpr,
      DateDiffExpr,
      DayExpr,
      DayOfMonthExpr,
      DayOfWeekExpr,
      DayOfWeekIsoExpr,
      DayOfYearExpr,
      HourExpr,
      LengthExpr,
      MinuteExpr,
      MonthExpr,
      QuarterExpr,
      SecondExpr,
      WeekExpr,
      YearExpr,
    ], {
      returns: DataTypeExprKind.BIGINT,
    });

    extend([
      CountIfExpr,
      FactorialExpr,
    ], {
      returns: DataTypeExprKind.INT128,
    });

    extend([
      Atan2Expr,
      JarowinklerSimilarityExpr,
      TimeToUnixExpr,
    ], {
      returns: DataTypeExprKind.DOUBLE,
    });

    extend([
      FormatExpr,
      ReverseExpr,
    ], {
      returns: DataTypeExprKind.VARCHAR,
    });

    map.set(DateBinExpr, {
      annotator: (s: TypeAnnotator, e: Expression) => s.annotateByArgs(e, ['expression']),
    });
    map.set(LocaltimestampExpr, {
      returns: DataTypeExprKind.TIMESTAMP,
    });
    map.set(ToDaysExpr, {
      returns: DataTypeExprKind.INTERVAL,
    });
    map.set(TimeFromPartsExpr, {
      returns: DataTypeExprKind.TIME,
    });

    return map;
  }
}
