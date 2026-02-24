import type { ExpressionOrString } from '../expressions';
import {
  Expression, DataTypeExprKind,
  DataTypeExpr, ReverseExpr, TimestampFromPartsExpr, DecodeCaseExpr,
  ArgMaxExpr, ArgMinExpr, WithinGroupExpr, PercentileDiscExpr, PercentileContExpr,
  OrderExpr, OrderedExpr, MedianExpr, KurtosisExpr, StrToTimeExpr,
  AddMonthsExpr, CeilExpr, DateTruncExpr, FloorExpr, LeftExpr, ModeExpr, PadExpr,
  RightExpr, RoundExpr, StuffExpr, SubstringExpr, TimeSliceExpr, TimestampTruncExpr,
  ApproxTopKExpr, ApproxTopKEstimateExpr, ArrayExpr, ArrayAggExpr, ArrayAppendExpr,
  ArrayCompactExpr, ArrayConcatExpr, ArrayConstructCompactExpr, ArrayPrependExpr,
  ArrayRemoveExpr, ArraysZipExpr, ArrayUniqueAggExpr, ArrayUnionAggExpr, MapKeysExpr,
  RegexpExtractAllExpr, SplitExpr, StringToArrayExpr, BitmapBitPositionExpr,
  BitmapBucketNumberExpr, BitmapCountExpr, FactorialExpr, GroupingIdExpr,
  Md5NumberLower64Expr, Md5NumberUpper64Expr, RandExpr, Seq8Expr, ZipfExpr,
  Base64DecodeBinaryExpr, BitmapConstructAggExpr, BitmapOrAggExpr, CompressExpr,
  DecompressBinaryExpr, DecryptExpr, DecryptRawExpr, EncryptExpr, EncryptRawExpr,
  HexStringExpr, Md5DigestExpr, Sha1DigestExpr, Sha2DigestExpr, ToBinaryExpr,
  TryBase64DecodeBinaryExpr, TryHexDecodeBinaryExpr, UnhexExpr, BoolandExpr,
  BoolnotExpr, BoolorExpr, BoolxorAggExpr, EqualNullExpr, IsNullValueExpr,
  MapContainsKeyExpr, SearchExpr, SearchIpExpr, ToBooleanExpr, NextDayExpr,
  PreviousDayExpr, BitwiseAndAggExpr, BitwiseOrAggExpr, BitwiseXorAggExpr,
  RegexpCountExpr, RegexpInstrExpr, ToNumberExpr, ApproxPercentileEstimateExpr,
  ApproximateSimilarityExpr, CosineDistanceExpr, CovarPopExpr, CovarSampExpr,
  DotProductExpr, EuclideanDistanceExpr, ManhattanDistanceExpr, MonthsBetweenExpr,
  NormalExpr, ToDecfloatExpr, TryToDecfloatExpr, AcosExpr, AsinExpr, AtanExpr,
  Atan2Expr, CbrtExpr, CosExpr, CotExpr, DegreesExpr, ExpExpr, LnExpr, LogExpr,
  PowExpr, RadiansExpr, RegrAvgxExpr, RegrAvgyExpr, RegrCountExpr, RegrInterceptExpr,
  RegrR2Expr, RegrSlopeExpr, RegrSxxExpr, RegrSxyExpr, RegrSyyExpr, RegrValxExpr,
  RegrValyExpr, SinExpr, SqrtExpr, TanExpr, TanhExpr, ByteLengthExpr, GroupingExpr,
  JarowinklerSimilarityExpr, MapSizeExpr, MinuteExpr, RtrimmedLengthExpr,
  SecondExpr, Seq1Expr, Seq2Expr, Seq4Expr, WidthBucketExpr, ApproxPercentileAccumulateExpr,
  ApproxPercentileCombineExpr, ApproxTopKAccumulateExpr, ApproxTopKCombineExpr,
  ObjectAggExpr, ParseIpExpr, ParseUrlExpr, XmlGetExpr, MapCatExpr, MapDeleteExpr,
  MapInsertExpr, MapPickExpr, ToFileExpr, TimeFromPartsExpr, TsOrDsToTimeExpr,
  CurrentTimestampExpr, LocaltimestampExpr, QuarterExpr, AiAggExpr, AiClassifyExpr,
  AiSummarizeAggExpr, Base64DecodeStringExpr, Base64EncodeExpr, CheckJsonExpr,
  CheckXmlExpr, CollateExpr, CollationExpr, CurrentAccountExpr, CurrentAccountNameExpr,
  CurrentAvailableRolesExpr, CurrentClientExpr, CurrentDatabaseExpr, CurrentIpAddressExpr,
  CurrentSchemasExpr, CurrentSecondaryRolesExpr, CurrentSessionExpr, CurrentStatementExpr,
  CurrentVersionExpr, CurrentTransactionExpr, CurrentWarehouseExpr,
  CurrentOrganizationUserExpr, CurrentRegionExpr, CurrentRoleExpr, CurrentRoleTypeExpr,
  CurrentOrganizationNameExpr, DecompressStringExpr, HexDecodeStringExpr, HexEncodeExpr,
  MonthnameExpr, RandstrExpr, RegexpExtractExpr, RegexpReplaceExpr, RepeatExpr,
  ReplaceExpr, SoundexExpr, SoundexP123Expr, SplitPartExpr, TryBase64DecodeStringExpr,
  TryHexDecodeStringExpr, UuidExpr, MinhashExpr, MinhashCombineExpr, VarianceExpr,
  VariancePopExpr, ConcatWsExpr, ConvertTimezoneExpr, DateAddExpr, HashAggExpr, TimeAddExpr,
} from '../expressions';
import type { TypeAnnotator } from '../optimizer';
import {
  isInstanceOf, filterInstanceOf,
} from '../port_internals';
import { EXPRESSION_METADATA as BASE_EXPRESSION_METADATA } from '.';
import type { ExpressionMetadata } from '.';

const DATE_PARTS = new Set([
  'DAY',
  'WEEK',
  'MONTH',
  'QUARTER',
  'YEAR',
]);
const MAX_PRECISION = 38;
const MAX_SCALE = 37;

function annotateReverse (self: TypeAnnotator, expression: ReverseExpr): ReverseExpr {
  self.annotateByArgs(expression, ['this']);
  if (expression.isType(DataTypeExprKind.NULL)) {
    // Snowflake treats REVERSE(NULL) as a VARCHAR
    self.setType(expression, DataTypeExprKind.VARCHAR);
  }
  return expression;
}

function annotateTimestampFromParts (self: TypeAnnotator, expression: TimestampFromPartsExpr): TimestampFromPartsExpr {
  if (expression.args.zone) {
    self.setType(expression, DataTypeExprKind.TIMESTAMPTZ);
  } else {
    self.setType(expression, DataTypeExprKind.TIMESTAMP);
  }
  return expression;
}

function annotateDateOrTimeAdd (self: TypeAnnotator, expression: Expression): Expression {
  const thisArg = expression.args.this;
  if (isInstanceOf(thisArg, Expression) && thisArg.isType(DataTypeExprKind.DATE) && !DATE_PARTS.has(expression.text('unit').toUpperCase())) {
    self.setType(expression, DataTypeExprKind.TIMESTAMPNTZ);
  } else {
    self.annotateByArgs(expression, ['this']);
  }
  return expression;
}

function annotateDecodeCase (self: TypeAnnotator, expression: DecodeCaseExpr): DecodeCaseExpr {
  const expressions = expression.args.expressions ?? [];
  const returnTypes: (ExpressionOrString | undefined)[] = [];

  for (let i = 2; i < expressions.length; i += 2) {
    returnTypes.push(expressions[i].type);
  }

  if (expressions.length % 2 === 0) {
    returnTypes.push(expressions[expressions.length - 1].type);
  }

  let lastType: DataTypeExpr | DataTypeExprKind | undefined;
  for (const retType of returnTypes) {
    const narrowedRetType = isInstanceOf(retType, DataTypeExpr) ? retType : undefined;
    lastType = self.maybeCoerce(lastType || narrowedRetType, narrowedRetType);
  }

  self.setType(expression, lastType);
  return expression;
}

function annotateArgMaxMin (self: TypeAnnotator, expression: ArgMaxExpr | ArgMinExpr): Expression {
  const thisArg = expression.args.this;
  const thisType = isInstanceOf(thisArg, Expression) ? thisArg.type : undefined;
  self.setType(
    expression,
    expression.args.count ? DataTypeExprKind.ARRAY : (isInstanceOf(thisType, DataTypeExpr) ? thisType : undefined),
  );
  return expression;
}

function annotateWithinGroup (self: TypeAnnotator, expression: WithinGroupExpr): WithinGroupExpr {
  const inner = expression.args.this;
  const orderExpr = expression.args.expression;
  const orderExprs = isInstanceOf(orderExpr, OrderExpr) ? (orderExpr.args.expressions ?? []) : [];

  if (
    (isInstanceOf(inner, PercentileDiscExpr) || isInstanceOf(inner, PercentileContExpr))
    && isInstanceOf(orderExpr, OrderExpr)
    && orderExprs.length === 1
    && isInstanceOf(orderExprs[0], OrderedExpr)
  ) {
    const firstThis = orderExprs[0].args.this;
    const firstType = isInstanceOf(firstThis, Expression) ? firstThis.type : undefined;
    self.setType(expression, isInstanceOf(firstType, DataTypeExpr) ? firstType : undefined);
  }

  return expression;
}

function annotateMedian (self: TypeAnnotator, expression: MedianExpr): MedianExpr {
  self.annotateByArgs(expression, ['this']);
  const thisArg = expression.args.this;
  const inputType = isInstanceOf(thisArg, Expression) ? thisArg.type : undefined;
  if (!isInstanceOf(inputType, DataTypeExpr)) return expression;

  if (inputType.isType(DataTypeExprKind.DOUBLE)) {
    self.setType(expression, DataTypeExprKind.DOUBLE);
  } else {
    const exprs = filterInstanceOf(inputType.args.expressions ?? [], Expression);
    // Assuming text('this') retrieves the numeric value of the AST literal node
    const precision = exprs[0] ? Number(exprs[0].text('this')) : MAX_PRECISION;
    const scale = exprs[1] ? Number(exprs[1].text('this')) : 0;

    const newPrecision = Math.min(precision + 3, MAX_PRECISION);
    const newScale = Math.min(scale + 3, MAX_SCALE);

    const newType = DataTypeExpr.build(`NUMBER(${newPrecision}, ${newScale})`, { dialect: 'snowflake' });
    self.setType(expression, newType);
  }

  return expression;
}

function annotateVariance (self: TypeAnnotator, expression: Expression): Expression {
  self.annotateByArgs(expression, ['this']);
  const thisArg = expression.args.this;
  const inputType = isInstanceOf(thisArg, Expression) ? thisArg.type : undefined;
  if (!isInstanceOf(inputType, DataTypeExpr)) return expression;

  if (inputType.isType(DataTypeExprKind.DECFLOAT)) {
    self.setType(expression, DataTypeExpr.build('DECFLOAT', { dialect: 'snowflake' }));
  } else if (inputType.isType([DataTypeExprKind.FLOAT, DataTypeExprKind.DOUBLE])) {
    self.setType(expression, DataTypeExprKind.DOUBLE);
  } else {
    const exprs = filterInstanceOf(inputType.args.expressions ?? [], Expression);
    const scale = exprs[1] ? Number(exprs[1].text('this')) : 0;
    const newScale = scale === 0 ? 6 : Math.max(12, scale);

    const newType = DataTypeExpr.build(`NUMBER(${MAX_PRECISION}, ${newScale})`, { dialect: 'snowflake' });
    self.setType(expression, newType);
  }

  return expression;
}

function annotateKurtosis (self: TypeAnnotator, expression: KurtosisExpr): KurtosisExpr {
  self.annotateByArgs(expression, ['this']);
  const thisArg = expression.args.this;
  const inputType = isInstanceOf(thisArg, Expression) ? thisArg.type : undefined;
  if (!isInstanceOf(inputType, DataTypeExpr)) {
    self.setType(expression, DataTypeExpr.build(`NUMBER(${MAX_PRECISION}, 12)`, { dialect: 'snowflake' }));
    return expression;
  }

  if (inputType.isType(DataTypeExprKind.DECFLOAT)) {
    self.setType(expression, DataTypeExpr.build('DECFLOAT', { dialect: 'snowflake' }));
  } else if (inputType.isType([DataTypeExprKind.FLOAT, DataTypeExprKind.DOUBLE])) {
    self.setType(expression, DataTypeExprKind.DOUBLE);
  } else {
    self.setType(expression, DataTypeExpr.build(`NUMBER(${MAX_PRECISION}, 12)`, { dialect: 'snowflake' }));
  }

  return expression;
}

function annotateMathWithFloatDecfloat (self: TypeAnnotator, expression: Expression): Expression {
  self.annotateByArgs(expression, ['this']);

  const thisArg = expression.args.this;
  if (isInstanceOf(thisArg, Expression) && thisArg.isType(DataTypeExprKind.DECFLOAT)) {
    const thisType = thisArg.type;
    self.setType(expression, isInstanceOf(thisType, DataTypeExpr) ? thisType : undefined);
  } else {
    self.setType(expression, DataTypeExprKind.DOUBLE);
  }

  return expression;
}

function annotateStrToTime (self: TypeAnnotator, expression: StrToTimeExpr): StrToTimeExpr {
  const targetTypeArg = expression.args.targetType;
  const targetType = isInstanceOf(targetTypeArg, DataTypeExpr) ? targetTypeArg : DataTypeExprKind.TIMESTAMP;

  self.setType(expression, targetType);
  return expression;
}

export const EXPRESSION_METADATA: ExpressionMetadata = (() => {
  const map: ExpressionMetadata = new Map(BASE_EXPRESSION_METADATA);

  const extend = (types: (typeof Expression)[], data: Record<string, unknown>) => {
    for (const type of types) map.set(type, data);
  };

  extend([
    AddMonthsExpr,
    CeilExpr,
    DateTruncExpr,
    FloorExpr,
    LeftExpr,
    ModeExpr,
    PadExpr,
    RightExpr,
    RoundExpr,
    StuffExpr,
    SubstringExpr,
    TimeSliceExpr,
    TimestampTruncExpr,
  ], { annotator: (s: TypeAnnotator, e: Expression) => s.annotateByArgs(e, ['this']) });

  extend([
    ApproxTopKExpr,
    ApproxTopKEstimateExpr,
    ArrayExpr,
    ArrayAggExpr,
    ArrayAppendExpr,
    ArrayCompactExpr,
    ArrayConcatExpr,
    ArrayConstructCompactExpr,
    ArrayPrependExpr,
    ArrayRemoveExpr,
    ArraysZipExpr,
    ArrayUniqueAggExpr,
    ArrayUnionAggExpr,
    MapKeysExpr,
    RegexpExtractAllExpr,
    SplitExpr,
    StringToArrayExpr,
  ], { returns: DataTypeExprKind.ARRAY });

  extend([
    BitmapBitPositionExpr,
    BitmapBucketNumberExpr,
    BitmapCountExpr,
    FactorialExpr,
    GroupingIdExpr,
    Md5NumberLower64Expr,
    Md5NumberUpper64Expr,
    RandExpr,
    Seq8Expr,
    ZipfExpr,
  ], { returns: DataTypeExprKind.BIGINT });

  extend([
    Base64DecodeBinaryExpr,
    BitmapConstructAggExpr,
    BitmapOrAggExpr,
    CompressExpr,
    DecompressBinaryExpr,
    DecryptExpr,
    DecryptRawExpr,
    EncryptExpr,
    EncryptRawExpr,
    HexStringExpr,
    Md5DigestExpr,
    Sha1DigestExpr,
    Sha2DigestExpr,
    ToBinaryExpr,
    TryBase64DecodeBinaryExpr,
    TryHexDecodeBinaryExpr,
    UnhexExpr,
  ], { returns: DataTypeExprKind.BINARY });

  extend([
    BoolandExpr,
    BoolnotExpr,
    BoolorExpr,
    BoolxorAggExpr,
    EqualNullExpr,
    IsNullValueExpr,
    MapContainsKeyExpr,
    SearchExpr,
    SearchIpExpr,
    ToBooleanExpr,
  ], { returns: DataTypeExprKind.BOOLEAN });

  extend([NextDayExpr, PreviousDayExpr], { returns: DataTypeExprKind.DATE });

  extend([
    BitwiseAndAggExpr,
    BitwiseOrAggExpr,
    BitwiseXorAggExpr,
    RegexpCountExpr,
    RegexpInstrExpr,
    ToNumberExpr,
  ], { annotator: (s: TypeAnnotator, e: Expression) => s.setType(e, DataTypeExpr.build('NUMBER', { dialect: 'snowflake' })) });

  extend([
    ApproxPercentileEstimateExpr,
    ApproximateSimilarityExpr,
    CosineDistanceExpr,
    CovarPopExpr,
    CovarSampExpr,
    DotProductExpr,
    EuclideanDistanceExpr,
    ManhattanDistanceExpr,
    MonthsBetweenExpr,
    NormalExpr,
  ], { returns: DataTypeExprKind.DOUBLE });

  map.set(KurtosisExpr, { annotator: annotateKurtosis });

  extend([ToDecfloatExpr, TryToDecfloatExpr], { returns: DataTypeExprKind.DECFLOAT });

  extend([
    AcosExpr,
    AsinExpr,
    AtanExpr,
    Atan2Expr,
    CbrtExpr,
    CosExpr,
    CotExpr,
    DegreesExpr,
    ExpExpr,
    LnExpr,
    LogExpr,
    PowExpr,
    RadiansExpr,
    RegrAvgxExpr,
    RegrAvgyExpr,
    RegrCountExpr,
    RegrInterceptExpr,
    RegrR2Expr,
    RegrSlopeExpr,
    RegrSxxExpr,
    RegrSxyExpr,
    RegrSyyExpr,
    RegrValxExpr,
    RegrValyExpr,
    SinExpr,
    SqrtExpr,
    TanExpr,
    TanhExpr,
  ], { annotator: annotateMathWithFloatDecfloat });

  extend([
    ByteLengthExpr,
    GroupingExpr,
    JarowinklerSimilarityExpr,
    MapSizeExpr,
    MinuteExpr,
    RtrimmedLengthExpr,
    SecondExpr,
    Seq1Expr,
    Seq2Expr,
    Seq4Expr,
    WidthBucketExpr,
  ], { returns: DataTypeExprKind.INT });

  extend([
    ApproxPercentileAccumulateExpr,
    ApproxPercentileCombineExpr,
    ApproxTopKAccumulateExpr,
    ApproxTopKCombineExpr,
    ObjectAggExpr,
    ParseIpExpr,
    ParseUrlExpr,
    XmlGetExpr,
  ], { returns: DataTypeExprKind.OBJECT });

  extend([
    MapCatExpr,
    MapDeleteExpr,
    MapInsertExpr,
    MapPickExpr,
  ], { returns: DataTypeExprKind.MAP });

  map.set(ToFileExpr, { returns: DataTypeExprKind.FILE });

  extend([TimeFromPartsExpr, TsOrDsToTimeExpr], { returns: DataTypeExprKind.TIME });

  extend([CurrentTimestampExpr, LocaltimestampExpr], { returns: DataTypeExprKind.TIMESTAMPLTZ });

  map.set(QuarterExpr, { returns: DataTypeExprKind.TINYINT });

  extend([
    AiAggExpr,
    AiClassifyExpr,
    AiSummarizeAggExpr,
    Base64DecodeStringExpr,
    Base64EncodeExpr,
    CheckJsonExpr,
    CheckXmlExpr,
    CollateExpr,
    CollationExpr,
    CurrentAccountExpr,
    CurrentAccountNameExpr,
    CurrentAvailableRolesExpr,
    CurrentClientExpr,
    CurrentDatabaseExpr,
    CurrentIpAddressExpr,
    CurrentSchemasExpr,
    CurrentSecondaryRolesExpr,
    CurrentSessionExpr,
    CurrentStatementExpr,
    CurrentVersionExpr,
    CurrentTransactionExpr,
    CurrentWarehouseExpr,
    CurrentOrganizationUserExpr,
    CurrentRegionExpr,
    CurrentRoleExpr,
    CurrentRoleTypeExpr,
    CurrentOrganizationNameExpr,
    DecompressStringExpr,
    HexDecodeStringExpr,
    HexEncodeExpr,
    MonthnameExpr,
    RandstrExpr,
    RegexpExtractExpr,
    RegexpReplaceExpr,
    RepeatExpr,
    ReplaceExpr,
    SoundexExpr,
    SoundexP123Expr,
    SplitPartExpr,
    TryBase64DecodeStringExpr,
    TryHexDecodeStringExpr,
    UuidExpr,
  ], { returns: DataTypeExprKind.VARCHAR });

  extend([MinhashExpr, MinhashCombineExpr], { returns: DataTypeExprKind.VARIANT });

  extend([VarianceExpr, VariancePopExpr], { annotator: annotateVariance });

  map.set(ArgMaxExpr, { annotator: annotateArgMaxMin });
  map.set(ArgMinExpr, { annotator: annotateArgMaxMin });
  map.set(ConcatWsExpr, { annotator: (s: TypeAnnotator, e: Expression) => s.annotateByArgs(e, ['expressions']) });

  map.set(ConvertTimezoneExpr, {
    annotator: (s: TypeAnnotator, e: ConvertTimezoneExpr) => s.setType(
      e,
      e.args.sourceTz ? DataTypeExprKind.TIMESTAMPNTZ : DataTypeExprKind.TIMESTAMPTZ,
    ),
  });

  map.set(DateAddExpr, { annotator: annotateDateOrTimeAdd });
  map.set(DecodeCaseExpr, { annotator: annotateDecodeCase });

  map.set(HashAggExpr, {
    annotator: (s: TypeAnnotator, e: Expression) => s.setType(e, DataTypeExpr.build('NUMBER(19, 0)', { dialect: 'snowflake' })),
  });

  map.set(MedianExpr, { annotator: annotateMedian });
  map.set(ReverseExpr, { annotator: annotateReverse });
  map.set(StrToTimeExpr, { annotator: annotateStrToTime });
  map.set(TimeAddExpr, { annotator: annotateDateOrTimeAdd });
  map.set(TimestampFromPartsExpr, { annotator: annotateTimestampFromParts });
  map.set(WithinGroupExpr, { annotator: annotateWithinGroup });

  return map;
})();
