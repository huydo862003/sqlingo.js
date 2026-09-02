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
  EncodeExpr,
  UnhexExpr,
  CorrExpr,
  MonthsBetweenExpr,
  AddMonthsExpr,
  TsOrDsAddExpr,
  CurrentDatabaseExpr,
  CurrentSchemaExpr,
  CurrentUserExpr,
  HexExpr,
  NextDayExpr,
  RepeatExpr,
  ReplaceExpr,
  SoundexExpr,
  StrToUnixExpr,
  FactorialExpr,
  MonthExpr,
  SecondExpr,
  ArrayDistinctExpr,
  ArrayExceptExpr,
  ArrayIntersectExpr,
  ApproxQuantileExpr,
  CoalesceExpr,
  IfExpr,
  QuantileExpr,
  RegexpSplitExpr,
  ReverseExpr,
} from '../expressions/expressions';
import type {
  TypeAnnotator,
} from '../optimizer';
import {
  DialectTyping,
} from './dialect';
import type {
  ExpressionMetadata,
} from './dialect';

export class HiveTyping {
  @cache
  static get EXPRESSION_METADATA (): ExpressionMetadata {
    // Clone the base metadata to apply dialect-specific overrides
    const map: ExpressionMetadata = new Map(DialectTyping.EXPRESSION_METADATA);

    const extend = (types: (typeof Expression)[], data: Record<string, unknown>) => {
      for (const type of types) map.set(type, data);
    };

    extend([
      EncodeExpr,
      UnhexExpr,
    ], {
      returns: DataTypeExprKind.BINARY,
    });

    extend([
      CorrExpr,
      MonthsBetweenExpr,
    ], {
      returns: DataTypeExprKind.DOUBLE,
    });

    extend([
      AddMonthsExpr,
      TsOrDsAddExpr,
      CurrentDatabaseExpr,
      CurrentUserExpr,
      CurrentSchemaExpr,
      HexExpr,
      NextDayExpr,
      RepeatExpr,
      ReplaceExpr,
      SoundexExpr,
    ], {
      returns: DataTypeExprKind.VARCHAR,
    });

    extend([
      StrToUnixExpr,
      FactorialExpr,
    ], {
      returns: DataTypeExprKind.BIGINT,
    });

    extend([
      MonthExpr,
      SecondExpr,
    ], {
      returns: DataTypeExprKind.INT,
    });

    extend([
      ArrayDistinctExpr,
      ArrayExceptExpr,
      ReverseExpr,
    ], {
      annotator: (s: TypeAnnotator, e: Expression) => s.annotateByArgs(e, ['this']),
    });

    map.set(ApproxQuantileExpr, {
      annotator: (s: TypeAnnotator, e: ApproxQuantileExpr) => s.annotateByArgs(e, ['quantile']),
    });
    map.set(ArrayIntersectExpr, {
      annotator: (s: TypeAnnotator, e: ArrayIntersectExpr) => s.annotateByArgs(e, ['expressions']),
    });
    map.set(CoalesceExpr, {
      annotator: (s: TypeAnnotator, e: CoalesceExpr) => s.annotateByArgs(e, [
        'this',
        'expressions',
      ], {
        promote: true,
      }),
    });

    map.set(IfExpr, {
      annotator: (s: TypeAnnotator, e: IfExpr) => s.annotateByArgs(e, [
        'true',
        'false',
      ], {
        promote: true,
      }),
    });

    map.set(QuantileExpr, {
      annotator: (s: TypeAnnotator, e: QuantileExpr) => s.annotateByArgs(e, ['quantile']),
    });
    map.set(RegexpSplitExpr, {
      returns: 'ARRAY<STRING>',
    });

    return map;
  }
}
