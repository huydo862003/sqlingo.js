import type { Expression } from '../expressions';
import {
  DataTypeExpr,
  DataTypeExprKind,
  EncodeExpr,
  UnhexExpr,
  CorrExpr,
  MonthsBetweenExpr,
  CurrentDatabaseExpr,
  CurrentSchemaExpr,
  CurrentUserExpr,
  HexExpr,
  RepeatExpr,
  ReplaceExpr,
  SoundexExpr,
  StrToUnixExpr,
  FactorialExpr,
  MonthExpr,
  SecondExpr,
  CoalesceExpr,
  IfExpr,
  RegexpSplitExpr,
  ReverseExpr,
} from '../expressions';
import type { TypeAnnotator } from '../optimizer';
import { EXPRESSION_METADATA as BASE_EXPRESSION_METADATA } from '.';
import type { ExpressionMetadata } from '.';

export const EXPRESSION_METADATA: ExpressionMetadata = (() => {
  // Clone the base metadata to apply dialect-specific overrides
  const map: ExpressionMetadata = new Map(BASE_EXPRESSION_METADATA);

  const extend = (types: (typeof Expression)[], data: Record<string, unknown>) => {
    for (const type of types) map.set(type, data);
  };

  extend([EncodeExpr, UnhexExpr], { returns: DataTypeExprKind.BINARY });

  extend([CorrExpr, MonthsBetweenExpr], { returns: DataTypeExprKind.DOUBLE });

  extend([
    CurrentDatabaseExpr,
    CurrentSchemaExpr,
    CurrentUserExpr,
    HexExpr,
    RepeatExpr,
    ReplaceExpr,
    SoundexExpr,
  ], { returns: DataTypeExprKind.VARCHAR });

  extend([StrToUnixExpr, FactorialExpr], { returns: DataTypeExprKind.BIGINT });

  extend([MonthExpr, SecondExpr], { returns: DataTypeExprKind.INT });

  map.set(CoalesceExpr, {
    annotator: (s: TypeAnnotator, e: CoalesceExpr) => s.annotateByArgs(e, ['this', 'expressions'], { promote: true }),
  });

  map.set(IfExpr, {
    annotator: (s: TypeAnnotator, e: IfExpr) => s.annotateByArgs(e, ['true', 'false'], { promote: true }),
  });

  map.set(RegexpSplitExpr, {
    returns: DataTypeExpr.build('ARRAY<STRING>'),
  });

  map.set(ReverseExpr, {
    annotator: (s: TypeAnnotator, e: ReverseExpr) => s.annotateByArgs(e, ['this']),
  });

  return map;
})();
