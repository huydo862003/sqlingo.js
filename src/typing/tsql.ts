import type { Expression } from '../expressions';
import {
  DataTypeExprKind,
  AcosExpr,
  AsinExpr,
  AtanExpr,
  Atan2Expr,
  CosExpr,
  CotExpr,
  SinExpr,
  TanExpr,
  SoundexExpr,
  StuffExpr,
  CurrentTimezoneExpr,
  RadiansExpr,
} from '../expressions';
import type { TypeAnnotator } from '../optimizer';
import { EXPRESSION_METADATA as BASE_EXPRESSION_METADATA } from '.';
import type { ExpressionMetadata } from '.';

export const EXPRESSION_METADATA: ExpressionMetadata = (() => {
  // Clone the base metadata to apply dialect-specific overrides safely
  const map: ExpressionMetadata = new Map(BASE_EXPRESSION_METADATA);

  const extend = (types: (typeof Expression)[], data: Record<string, unknown>) => {
    for (const type of types) map.set(type, data);
  };

  extend([
    AcosExpr,
    AsinExpr,
    AtanExpr,
    Atan2Expr,
    CosExpr,
    CotExpr,
    SinExpr,
    TanExpr,
  ], { returns: DataTypeExprKind.FLOAT });

  extend([SoundexExpr, StuffExpr], { returns: DataTypeExprKind.VARCHAR });

  map.set(CurrentTimezoneExpr, { returns: DataTypeExprKind.NVARCHAR });

  map.set(RadiansExpr, {
    annotator: (s: TypeAnnotator, e: RadiansExpr) => s.annotateByArgs(e, ['this']),
  });

  return map;
})();
