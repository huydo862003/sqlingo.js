import type { Expression } from '../expressions/expressions';
import { cache } from '../port_internals';
import { DataTypeExprKind } from '../expressions/types';
import {
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
} from '../expressions/expressions';
import type { TypeAnnotator } from '../optimizer';
import { DialectTyping } from './dialect';
import type { ExpressionMetadata } from './dialect';

export class TSQLTyping {
  @cache
  static get EXPRESSION_METADATA (): ExpressionMetadata {
    // Clone the base metadata to apply dialect-specific overrides safely
    const map: ExpressionMetadata = new Map(DialectTyping.EXPRESSION_METADATA);

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
  }
}
