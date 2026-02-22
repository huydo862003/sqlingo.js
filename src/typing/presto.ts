import type { Expression } from '../expressions';
import {
  DataTypeExprKind,
  BitwiseAndExpr,
  BitwiseNotExpr,
  BitwiseOrExpr,
  BitwiseXorExpr,
  LengthExpr,
  LevenshteinExpr,
  StrPositionExpr,
  WidthBucketExpr,
  CeilExpr,
  FloorExpr,
  RoundExpr,
  SignExpr,
  ModExpr,
  RandExpr,
  Md5DigestExpr,
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

  extend([
    BitwiseAndExpr,
    BitwiseNotExpr,
    BitwiseOrExpr,
    BitwiseXorExpr,
    LengthExpr,
    LevenshteinExpr,
    StrPositionExpr,
    WidthBucketExpr,
  ], { returns: DataTypeExprKind.BIGINT });

  extend([
    CeilExpr,
    FloorExpr,
    RoundExpr,
    SignExpr,
  ], {
    annotator: (s: TypeAnnotator, e: Expression) => s.annotateByArgs(e, ['this']),
  });

  map.set(ModExpr, {
    annotator: (s: TypeAnnotator, e: ModExpr) => s.annotateByArgs(e, ['this', 'expression']),
  });

  map.set(RandExpr, {
    annotator: (s: TypeAnnotator, e: RandExpr) =>
      e.args.this
        ? s.annotateByArgs(e, ['this'])
        : s.setType(e, DataTypeExprKind.DOUBLE),
  });

  map.set(Md5DigestExpr, { returns: DataTypeExprKind.VARBINARY });

  return map;
})();
