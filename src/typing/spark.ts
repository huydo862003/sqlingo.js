import type { Expression } from '../expressions';
import {
  DataTypeExprKind,
  SecExpr,
  CollationExpr,
  CurrentTimezoneExpr,
  MonthnameExpr,
  RandstrExpr,
  SessionUserExpr,
  BitmapCountExpr,
  LocaltimestampExpr,
  ToBinaryExpr,
  DateFromUnixDateExpr,
  ArraySizeExpr,
  OverlayExpr,
} from '../expressions';
import type { TypeAnnotator } from '../optimizer';
import { EXPRESSION_METADATA as SPARK2_METADATA } from './spark2';
import type { ExpressionMetadata } from './dialect';

export const EXPRESSION_METADATA: ExpressionMetadata = (() => {
  // Clone the Spark 2 base metadata to apply specific overrides
  const map: ExpressionMetadata = new Map(SPARK2_METADATA);

  const extend = (types: (typeof Expression)[], data: Record<string, unknown>) => {
    for (const type of types) map.set(type, data);
  };

  extend([SecExpr], { returns: DataTypeExprKind.DOUBLE });

  extend([
    CollationExpr,
    CurrentTimezoneExpr,
    MonthnameExpr,
    RandstrExpr,
    SessionUserExpr,
  ], { returns: DataTypeExprKind.VARCHAR });

  map.set(BitmapCountExpr, { returns: DataTypeExprKind.BIGINT });
  map.set(LocaltimestampExpr, { returns: DataTypeExprKind.TIMESTAMPNTZ });
  map.set(ToBinaryExpr, { returns: DataTypeExprKind.BINARY });
  map.set(DateFromUnixDateExpr, { returns: DataTypeExprKind.DATE });
  map.set(ArraySizeExpr, { returns: DataTypeExprKind.INT });

  map.set(OverlayExpr, {
    annotator: (s: TypeAnnotator, e: Expression) => s.annotateByArgs(e, ['this']),
  });

  return map;
})();
