import {
  DataTypeExpr, Expression, SelectExpr, SetOperationExpr, ColumnDefExpr, QueryExpr,
  ApproxTopKExpr, ApproxTopSumExpr, ArrayExpr, ConcatExpr, AvgExpr, CeilExpr,
  ExpExpr, FloorExpr, LnExpr, LogExpr, RoundExpr, SqrtExpr, ArgMaxExpr,
  ArgMinExpr, DateAddExpr, DateTruncExpr, DatetimeTruncExpr, FirstValueExpr,
  GroupConcatExpr, IgnoreNullsExpr, JsonExtractExpr, LeadExpr, LeftExpr,
  LowerExpr, NetFuncExpr, NthValueExpr, PadExpr, PercentileDiscExpr,
  RegexpExtractExpr, RegexpReplaceExpr, RepeatExpr, ReplaceExpr, RespectNullsExpr,
  ReverseExpr, RightExpr, SafeFuncExpr, SafeNegateExpr, SignExpr, SubstringExpr,
  TimestampTruncExpr, TranslateExpr, TrimExpr, UpperExpr, BitwiseAndAggExpr,
  BitwiseCountExpr, BitwiseOrAggExpr, BitwiseXorAggExpr, ByteLengthExpr,
  DenseRankExpr, FarmFingerprintExpr, GroupingExpr, LaxInt64Expr, LengthExpr,
  NtileExpr, RankExpr, RangeBucketExpr, RegexpInstrExpr, RowNumberExpr,
  ByteStringExpr, CodePointsToBytesExpr, Md5DigestExpr, ShaExpr, Sha2Expr,
  Sha1DigestExpr, Sha2DigestExpr, UnhexExpr, JsonBoolExpr, LaxBoolExpr,
  ParseDatetimeExpr, TimestampFromPartsExpr, Atan2Expr, CorrExpr, CosineDistanceExpr,
  CothExpr, CovarPopExpr, CovarSampExpr, CscExpr, CschExpr, CumeDistExpr,
  EuclideanDistanceExpr, Float64Expr, LaxFloat64Expr, PercentRankExpr, RandExpr,
  SecExpr, SechExpr, JsonArrayExpr, JsonArrayAppendExpr, JsonArrayInsertExpr,
  JsonObjectExpr, JsonRemoveExpr, JsonSetExpr, JsonStripNullsExpr, ParseTimeExpr,
  TimeFromPartsExpr, TimeTruncExpr, TsOrDsToTimeExpr, CodePointsToStringExpr,
  FormatExpr, HostExpr, JsonExtractScalarExpr, JsonTypeExpr, LaxStringExpr,
  LowerHexExpr, NormalizeExpr, RegDomainExpr, SafeConvertBytesToStringExpr,
  SoundexExpr, UuidExpr, PercentileContExpr, SafeAddExpr, SafeDivideExpr,
  SafeMultiplyExpr, SafeSubtractExpr, ApproxQuantilesExpr, JsonExtractArrayExpr,
  RegexpExtractAllExpr, SplitExpr, DateFromUnixDateExpr, GenerateTimestampArrayExpr,
  JsonFormatExpr, JsonKeysAtDepthExpr, JsonValueArrayExpr, LagExpr, ParseBignumericExpr,
  ParseNumericExpr, ToCodePointsExpr,
} from '../expressions/expressions';
import { DataTypeExprKind } from '../expressions/types';
import { isInstanceOf } from '../port_internals';
import type { TypeAnnotator } from '../optimizer';
import { TIMESTAMP_EXPRESSIONS } from './dialect';
import type { ExpressionMetadata } from './dialect';

/**
 * Many BigQuery math functions such as CEIL, FLOOR etc follow this return type convention.
 */
function annotateMathFunctions (this: TypeAnnotator, expression: Expression): Expression {
  const thisArg = expression.args.this;
  if (!isInstanceOf(thisArg, Expression)) return expression;
  const thisType = thisArg.type;
  this.setType(
    expression,
    thisArg.isType(DataTypeExpr.INTEGER_TYPES)
      ? DataTypeExprKind.DOUBLE
      : isInstanceOf(thisType, DataTypeExpr) ? thisType : undefined,
  );
  return expression;
}

function annotateSafeDivide (this: TypeAnnotator, expression: SafeDivideExpr): Expression {
  const thisArg = expression.args.this;
  const exprArg = expression.args.expression;
  if (
    isInstanceOf(thisArg, Expression)
    && isInstanceOf(exprArg, Expression)
    && thisArg.isType(DataTypeExpr.INTEGER_TYPES)
    && exprArg.isType(DataTypeExpr.INTEGER_TYPES)
  ) {
    return this.setType(expression, DataTypeExprKind.DOUBLE);
  }

  return annotateByArgsWithCoerce.call(this, expression);
}

function annotateByArgsWithCoerce (this: TypeAnnotator, expression: Expression): Expression {
  const thisArg = expression.args.this;
  const exprArg = expression.args.expression;
  const thisType = isInstanceOf(thisArg, Expression) ? thisArg.type : undefined;
  const exprType = isInstanceOf(exprArg, Expression) ? exprArg.type : undefined;
  this.setType(
    expression,
    this.maybeCoerce(
      isInstanceOf(thisType, DataTypeExpr) ? thisType : undefined,
      isInstanceOf(exprType, DataTypeExpr) ? exprType : undefined,
    ),
  );
  return expression;
}

function annotateByArgsApproxTop (
  this: TypeAnnotator,
  expression: ApproxTopKExpr | ApproxTopSumExpr,
): Expression {
  const thisArg = expression.args.this;
  const thisType = isInstanceOf(thisArg, Expression) ? thisArg.type : undefined;
  const valueTypeExpr = isInstanceOf(thisType, DataTypeExpr)
    ? thisType
    : new DataTypeExpr({ this: DataTypeExprKind.UNKNOWN });

  const structType = new DataTypeExpr({
    this: DataTypeExprKind.STRUCT,
    expressions: [valueTypeExpr, new DataTypeExpr({ this: DataTypeExprKind.BIGINT })],
    nested: true,
  });

  this.setType(
    expression,
    new DataTypeExpr({
      this: DataTypeExprKind.ARRAY,
      expressions: [structType],
      nested: true,
    }),
  );

  return expression;
}

function annotateConcat (this: TypeAnnotator, expression: ConcatExpr): ConcatExpr {
  this.annotateByArgs(expression, ['expressions']);

  // Args must be BYTES or types that can be cast to STRING, return type is either BYTES or STRING
  if (!expression.isType([DataTypeExprKind.BINARY, DataTypeExprKind.UNKNOWN])) {
    this.setType(expression, DataTypeExprKind.VARCHAR);
  }

  return expression;
}

function annotateArray (this: TypeAnnotator, expression: ArrayExpr): ArrayExpr {
  const arrayArgs = expression.args.expressions;

  if (arrayArgs && arrayArgs.length === 1) {
    const firstArg = arrayArgs[0];
    if (!isInstanceOf(firstArg, Expression)) {
      return expression;
    }
    const unnested = firstArg.unnest();
    let projectionType: DataTypeExpr | string | undefined;

    // Handle ARRAY(SELECT ...) - single SELECT query
    if (unnested instanceof SelectExpr) {
      const queryTypeRaw = unnested.meta?.queryType;
      const queryType = isInstanceOf(queryTypeRaw, DataTypeExpr) ? queryTypeRaw : undefined;
      if (
        queryType
        && queryType.isType(DataTypeExprKind.STRUCT)
        && queryType.args.expressions?.length === 1
      ) {
        const colDef = queryType.args.expressions[0];
        if (isInstanceOf(colDef, ColumnDefExpr)) {
          const colKind: unknown = colDef.args.kind;
          if (isInstanceOf(colKind, DataTypeExpr) && !colKind.isType(DataTypeExprKind.UNKNOWN)) {
            projectionType = colKind;
          }
        }
      }
    } else if (unnested instanceof SetOperationExpr) {
      // Handle ARRAY(SELECT ... UNION ALL SELECT ...) - set operations
      const colTypes = this.getSetopColumnTypes(unnested);
      const left = unnested.left;
      if (colTypes && isInstanceOf(left, QueryExpr) && 0 < left.selects.length) {
        const firstColName = left.selects[0].aliasOrName;
        projectionType = colTypes[firstColName];
      }
    }

    // If we successfully determine a projection type and it's not UNKNOWN, wrap it in ARRAY
    if (
      projectionType
      && !(
        (projectionType instanceof DataTypeExpr && projectionType.isType(DataTypeExprKind.UNKNOWN))
        || projectionType === DataTypeExprKind.UNKNOWN
      )
    ) {
      const elementType =
        projectionType instanceof DataTypeExpr
          ? projectionType.copy()
          : new DataTypeExpr({ this: projectionType });

      const arrayType = new DataTypeExpr({
        this: DataTypeExprKind.ARRAY,
        expressions: [elementType],
        nested: true,
      });

      this.setType(expression, arrayType);
      return expression;
    }
  }

  this.annotateByArgs(expression, ['expressions'], { array: true });
  return expression;
}

export const EXPRESSION_METADATA: ExpressionMetadata = (() => {
  const map: ExpressionMetadata = new Map();

  const extend = (types: (typeof Expression)[], data: Record<string, unknown>) => {
    for (const type of types) map.set(type, data);
  };

  // --- Type Returns ---
  extend([
    AvgExpr,
    CeilExpr,
    ExpExpr,
    FloorExpr,
    LnExpr,
    LogExpr,
    RoundExpr,
    SqrtExpr,
  ], {
    annotator: (s: TypeAnnotator, e: Expression) => annotateMathFunctions.call(s, e),
  });

  extend([
    ArgMaxExpr,
    ArgMinExpr,
    DateAddExpr,
    DateTruncExpr,
    DatetimeTruncExpr,
    FirstValueExpr,
    GroupConcatExpr,
    IgnoreNullsExpr,
    JsonExtractExpr,
    LeadExpr,
    LeftExpr,
    LowerExpr,
    NetFuncExpr,
    NthValueExpr,
    PadExpr,
    PercentileDiscExpr,
    RegexpExtractExpr,
    RegexpReplaceExpr,
    RepeatExpr,
    ReplaceExpr,
    RespectNullsExpr,
    ReverseExpr,
    RightExpr,
    SafeFuncExpr,
    SafeNegateExpr,
    SignExpr,
    SubstringExpr,
    TimestampTruncExpr,
    TranslateExpr,
    TrimExpr,
    UpperExpr,
  ], {
    annotator: (s: TypeAnnotator, e: Expression) => s.annotateByArgs(e, ['this']),
  });

  extend([
    BitwiseAndAggExpr,
    BitwiseCountExpr,
    BitwiseOrAggExpr,
    BitwiseXorAggExpr,
    ByteLengthExpr,
    DenseRankExpr,
    FarmFingerprintExpr,
    GroupingExpr,
    LaxInt64Expr,
    LengthExpr,
    NtileExpr,
    RankExpr,
    RangeBucketExpr,
    RegexpInstrExpr,
    RowNumberExpr,
  ], { returns: DataTypeExprKind.BIGINT });

  extend([
    ByteStringExpr,
    CodePointsToBytesExpr,
    Md5DigestExpr,
    ShaExpr,
    Sha2Expr,
    Sha1DigestExpr,
    Sha2DigestExpr,
    UnhexExpr,
  ], { returns: DataTypeExprKind.BINARY });

  extend([JsonBoolExpr, LaxBoolExpr], { returns: DataTypeExprKind.BOOLEAN });

  extend([ParseDatetimeExpr, TimestampFromPartsExpr], { returns: DataTypeExprKind.DATETIME });

  extend([
    Atan2Expr,
    CorrExpr,
    CosineDistanceExpr,
    CothExpr,
    CovarPopExpr,
    CovarSampExpr,
    CscExpr,
    CschExpr,
    CumeDistExpr,
    EuclideanDistanceExpr,
    Float64Expr,
    LaxFloat64Expr,
    PercentRankExpr,
    RandExpr,
    SecExpr,
    SechExpr,
  ], { returns: DataTypeExprKind.DOUBLE });

  extend([
    JsonArrayExpr,
    JsonArrayAppendExpr,
    JsonArrayInsertExpr,
    JsonObjectExpr,
    JsonRemoveExpr,
    JsonSetExpr,
    JsonStripNullsExpr,
  ], { returns: DataTypeExprKind.JSON });

  extend([
    ParseTimeExpr,
    TimeFromPartsExpr,
    TimeTruncExpr,
    TsOrDsToTimeExpr,
  ], { returns: DataTypeExprKind.TIME });

  extend([
    CodePointsToStringExpr,
    FormatExpr,
    HostExpr,
    JsonExtractScalarExpr,
    JsonTypeExpr,
    LaxStringExpr,
    LowerHexExpr,
    NormalizeExpr,
    RegDomainExpr,
    SafeConvertBytesToStringExpr,
    SoundexExpr,
    UuidExpr,
  ], { returns: DataTypeExprKind.VARCHAR });

  extend([
    PercentileContExpr,
    SafeAddExpr,
    SafeDivideExpr,
    SafeMultiplyExpr,
    SafeSubtractExpr,
  ], {
    annotator: (s: TypeAnnotator, e: Expression) => annotateByArgsWithCoerce.call(s, e),
  });

  extend([
    ApproxQuantilesExpr,
    JsonExtractArrayExpr,
    RegexpExtractAllExpr,
    SplitExpr,
  ], {
    annotator: (s: TypeAnnotator, e: Expression) => s.annotateByArgs(e, ['this'], { array: true }),
  });

  TIMESTAMP_EXPRESSIONS.forEach((type) => map.set(type, { returns: DataTypeExprKind.TIMESTAMPTZ }));

  map.set(ApproxTopKExpr, { annotator: (s: TypeAnnotator, e: ApproxTopKExpr) => annotateByArgsApproxTop.call(s, e) });
  map.set(ApproxTopSumExpr, { annotator: (s: TypeAnnotator, e: ApproxTopSumExpr) => annotateByArgsApproxTop.call(s, e) });
  map.set(ArrayExpr, { annotator: annotateArray });
  map.set(ConcatExpr, { annotator: annotateConcat });
  map.set(DateFromUnixDateExpr, { returns: DataTypeExprKind.DATE });

  map.set(GenerateTimestampArrayExpr, {
    annotator: (s: TypeAnnotator, e: GenerateTimestampArrayExpr) => s.setType(e, DataTypeExpr.build('ARRAY<TIMESTAMP>', { dialect: 'bigquery' })),
  });

  map.set(JsonFormatExpr, {
    annotator: (s: TypeAnnotator, e: JsonFormatExpr) => s.setType(e, e.args.toJson ? DataTypeExprKind.JSON : DataTypeExprKind.VARCHAR),
  });

  map.set(JsonKeysAtDepthExpr, {
    annotator: (s: TypeAnnotator, e: JsonKeysAtDepthExpr) => s.setType(e, DataTypeExpr.build('ARRAY<VARCHAR>', { dialect: 'bigquery' })),
  });

  map.set(JsonValueArrayExpr, {
    annotator: (s: TypeAnnotator, e: JsonValueArrayExpr) => s.setType(e, DataTypeExpr.build('ARRAY<VARCHAR>', { dialect: 'bigquery' })),
  });

  map.set(LagExpr, { annotator: (s: TypeAnnotator, e: LagExpr) => s.annotateByArgs(e, ['this', 'default']) });
  map.set(ParseBignumericExpr, { returns: DataTypeExprKind.BIGDECIMAL });
  map.set(ParseNumericExpr, { returns: DataTypeExprKind.DECIMAL });
  map.set(SafeDivideExpr, { annotator: (s: TypeAnnotator, e: SafeDivideExpr) => annotateSafeDivide.call(s, e) });

  map.set(ToCodePointsExpr, {
    annotator: (s: TypeAnnotator, e: ToCodePointsExpr) => s.setType(e, DataTypeExpr.build('ARRAY<BIGINT>', { dialect: 'bigquery' })),
  });

  return map;
})();
