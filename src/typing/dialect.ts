import type { Expression } from '../expressions/expressions';
import {
  cache, filterInstanceOf,
} from '../port_internals';
import { DataTypeExprKind } from '../expressions/types';
import {
  CurrentTimestampExpr,
  StrToTimeExpr,
  TimeStrToTimeExpr,
  TimestampAddExpr,
  TimestampSubExpr,
  UnixToTimeExpr,
  ApproxDistinctExpr, ArraySizeExpr,
  CountIfExpr, Int64Expr, UnixDateExpr, UnixSecondsExpr, UnixMicrosExpr, UnixMillisExpr,
  FromBase32Expr, FromBase64Expr, AllExpr, AnyExpr, BetweenExpr, BooleanExpr,
  ContainsExpr, EndsWithExpr, ExistsExpr, InExpr, IsInfExpr, IsNanExpr,
  LogicalAndExpr, LogicalOrExpr, RegexpLikeExpr, StartsWithExpr, CurrentDateExpr,
  DateExpr, DateFromPartsExpr, DateStrToDateExpr, DiToDateExpr, LastDayExpr,
  StrToDateExpr, TimeStrToDateExpr, TsOrDsToDateExpr, CurrentDatetimeExpr,
  DatetimeExpr, DatetimeAddExpr, DatetimeSubExpr, AsinExpr, AsinhExpr, AcosExpr,
  AcoshExpr, ApproxQuantileExpr, AtanExpr, AtanhExpr, AvgExpr, CbrtExpr, CosExpr,
  CoshExpr, CotExpr, ExpExpr, KurtosisExpr, LnExpr, LogExpr, PiExpr, PowExpr,
  QuantileExpr, RadiansExpr, RoundExpr, SafeDivideExpr, SinExpr, SinhExpr,
  SqrtExpr, StddevExpr, StddevPopExpr, StddevSampExpr, TanExpr, TanhExpr,
  ToDoubleExpr, VarianceExpr, VariancePopExpr, SkewnessExpr, AsciiExpr,
  BitLengthExpr, CeilExpr, DatetimeDiffExpr, GetbitExpr, HourExpr, TimestampDiffExpr,
  TimeDiffExpr, UnicodeExpr, DateToDiExpr, LevenshteinExpr, LengthExpr, SignExpr,
  StrPositionExpr, TsOrDiToDiExpr, QuarterExpr, IntervalExpr, JustifyDaysExpr,
  JustifyHoursExpr, JustifyIntervalExpr, MakeIntervalExpr, ParseJsonExpr,
  CurrentTimeExpr, LocaltimeExpr, TimeExpr, TimeAddExpr, TimeSubExpr,
  TimestampLtzFromPartsExpr, CurrentTimestampLtzExpr, TimestampTzFromPartsExpr,
  DayExpr, DayOfMonthExpr, DayOfWeekExpr, DayOfWeekIsoExpr, DayOfYearExpr,
  MonthExpr, WeekExpr, WeekOfYearExpr, YearExpr, YearOfWeekExpr, YearOfWeekIsoExpr,
  ArrayToStringExpr, ConcatExpr, ConcatWsExpr, ChrExpr, CurrentCatalogExpr,
  DaynameExpr, DateToDateStrExpr, DPipeExpr, GroupConcatExpr, InitcapExpr,
  LowerExpr, Md5Expr, ShaExpr, Sha2Expr, SubstringExpr, StringExpr, TimeToStrExpr,
  TimeToTimeStrExpr, TrimExpr, ToBase32Expr, ToBase64Expr, TranslateExpr,
  TsOrDsToDateStrExpr, UnixToStrExpr, UnixToTimeStrExpr, UpperExpr, RawStringExpr,
  SpaceExpr, AbsExpr, AnyValueExpr, ArrayConcatAggExpr, ArrayReverseExpr,
  ArraySliceExpr, FilterExpr, HavingMaxExpr, LastValueExpr, LimitExpr, OrderExpr,
  SortArrayExpr, WindowExpr, ArrayConcatExpr, CoalesceExpr, GreatestExpr,
  LeastExpr, MaxExpr, MinExpr, ArrayFirstExpr, ArrayLastExpr, AnonymousExpr,
  DateAddExpr, DateSubExpr, DateTruncExpr, CastExpr, TryCastExpr, MapExpr,
  VarMapExpr, ArrayExpr, ArrayAggExpr, BracketExpr, CaseExpr, CountExpr,
  DateDiffExpr, DataTypeExpr, DivExpr, DistinctExpr, DotExpr, ExplodeExpr,
  ExtractExpr, HexStringExpr, GenerateSeriesExpr, GenerateDateArrayExpr,
  GenerateTimestampArrayExpr, IfExpr, LiteralExpr, NullExpr, NullifExpr,
  PropertyEqExpr, StructExpr, SumExpr, TimestampExpr, ToMapExpr, UnnestExpr,
  SubqueryExpr,
} from '../expressions/expressions';
import type { TypeAnnotator } from '../optimizer';

export const TIMESTAMP_EXPRESSIONS = new Set<typeof Expression>([
  CurrentTimestampExpr,
  StrToTimeExpr,
  TimeStrToTimeExpr,
  TimestampAddExpr,
  TimestampSubExpr,
  UnixToTimeExpr,
]);

export type ExpressionMetadata = Map<typeof Expression, Record<string, unknown>>;

export class DialectTyping {
  @cache
  static get EXPRESSION_METADATA (): ExpressionMetadata {
    const map: ExpressionMetadata = new Map();

    const extend = (types: (typeof Expression)[], data: Record<string, unknown>) => {
      for (const type of types) map.set(type, data);
    };

    extend([
      ApproxDistinctExpr,
      ArraySizeExpr,
      CountIfExpr,
      Int64Expr,
      UnixDateExpr,
      UnixSecondsExpr,
      UnixMicrosExpr,
      UnixMillisExpr,
    ], { returns: 'BIGINT' });
    extend([FromBase32Expr, FromBase64Expr], { returns: 'BINARY' });
    extend([
      AllExpr,
      AnyExpr,
      BetweenExpr,
      BooleanExpr,
      ContainsExpr,
      EndsWithExpr,
      ExistsExpr,
      InExpr,
      IsInfExpr,
      IsNanExpr,
      LogicalAndExpr,
      LogicalOrExpr,
      RegexpLikeExpr,
      StartsWithExpr,
    ], { returns: 'BOOLEAN' });
    extend([
      CurrentDateExpr,
      DateExpr,
      DateFromPartsExpr,
      DateStrToDateExpr,
      DiToDateExpr,
      LastDayExpr,
      StrToDateExpr,
      TimeStrToDateExpr,
      TsOrDsToDateExpr,
    ], { returns: 'DATE' });
    extend([
      CurrentDatetimeExpr,
      DatetimeExpr,
      DatetimeAddExpr,
      DatetimeSubExpr,
    ], { returns: 'DATETIME' });
    extend([
      AsinExpr,
      AsinhExpr,
      AcosExpr,
      AcoshExpr,
      ApproxQuantileExpr,
      AtanExpr,
      AtanhExpr,
      AvgExpr,
      CbrtExpr,
      CosExpr,
      CoshExpr,
      CotExpr,
      ExpExpr,
      KurtosisExpr,
      LnExpr,
      LogExpr,
      PiExpr,
      PowExpr,
      QuantileExpr,
      RadiansExpr,
      RoundExpr,
      SafeDivideExpr,
      SinExpr,
      SinhExpr,
      SqrtExpr,
      StddevExpr,
      StddevPopExpr,
      StddevSampExpr,
      TanExpr,
      TanhExpr,
      ToDoubleExpr,
      VarianceExpr,
      VariancePopExpr,
      SkewnessExpr,
    ], { returns: 'DOUBLE' });
    extend([
      AsciiExpr,
      BitLengthExpr,
      CeilExpr,
      DatetimeDiffExpr,
      GetbitExpr,
      HourExpr,
      TimestampDiffExpr,
      TimeDiffExpr,
      UnicodeExpr,
      DateToDiExpr,
      LevenshteinExpr,
      LengthExpr,
      SignExpr,
      StrPositionExpr,
      TsOrDiToDiExpr,
      QuarterExpr,
    ], { returns: 'INT' });
    extend([
      IntervalExpr,
      JustifyDaysExpr,
      JustifyHoursExpr,
      JustifyIntervalExpr,
      MakeIntervalExpr,
    ], { returns: 'INTERVAL' });
    map.set(ParseJsonExpr, { returns: 'Json' });
    extend([
      CurrentTimeExpr,
      LocaltimeExpr,
      TimeExpr,
      TimeAddExpr,
      TimeSubExpr,
    ], { returns: 'TIME' });
    map.set(TimestampLtzFromPartsExpr, { returns: 'TIMESTAMPLTZ' });
    extend([CurrentTimestampLtzExpr, TimestampTzFromPartsExpr], { returns: 'TIMESTAMPTZ' });
    TIMESTAMP_EXPRESSIONS.forEach((type) => map.set(type, { returns: 'TIMESTAMP' }));
    extend([
      DayExpr,
      DayOfMonthExpr,
      DayOfWeekExpr,
      DayOfWeekIsoExpr,
      DayOfYearExpr,
      MonthExpr,
      WeekExpr,
      WeekOfYearExpr,
      YearExpr,
      YearOfWeekExpr,
      YearOfWeekIsoExpr,
    ], { returns: 'TINYINT' });
    extend([
      ArrayToStringExpr,
      ConcatExpr,
      ConcatWsExpr,
      ChrExpr,
      CurrentCatalogExpr,
      DaynameExpr,
      DateToDateStrExpr,
      DPipeExpr,
      GroupConcatExpr,
      InitcapExpr,
      LowerExpr,
      Md5Expr,
      ShaExpr,
      Sha2Expr,
      SubstringExpr,
      StringExpr,
      TimeToStrExpr,
      TimeToTimeStrExpr,
      TrimExpr,
      ToBase32Expr,
      ToBase64Expr,
      TranslateExpr,
      TsOrDsToDateStrExpr,
      UnixToStrExpr,
      UnixToTimeStrExpr,
      UpperExpr,
      RawStringExpr,
      SpaceExpr,
    ], { returns: 'VARCHAR' });

    extend([
      AbsExpr,
      AnyValueExpr,
      ArrayConcatAggExpr,
      ArrayReverseExpr,
      ArraySliceExpr,
      FilterExpr,
      HavingMaxExpr,
      LastValueExpr,
      LimitExpr,
      OrderExpr,
      SortArrayExpr,
      WindowExpr,
    ], { annotator: (s: TypeAnnotator, e: Expression) => s.annotateByArgs(e, ['this']) });
    extend([
      ArrayConcatExpr,
      CoalesceExpr,
      GreatestExpr,
      LeastExpr,
      MaxExpr,
      MinExpr,
    ], { annotator: (s: TypeAnnotator, e: Expression) => s.annotateByArgs(e, ['this', 'expressions']) });
    extend([ArrayFirstExpr, ArrayLastExpr], { annotator: (s: TypeAnnotator, e: Expression) => s.annotateByArrayElement(e) });
    extend([
      DateAddExpr,
      DateSubExpr,
      DateTruncExpr,
    ], { annotator: (s: TypeAnnotator, e: Expression) => s.annotateTimeunit(e as DateTruncExpr) });
    extend([CastExpr, TryCastExpr], { annotator: (s: TypeAnnotator, e: CastExpr | TryCastExpr) => s.setType(e, e.args.to as DataTypeExpr) });
    extend([MapExpr, VarMapExpr], { annotator: (s: TypeAnnotator, e: MapExpr | VarMapExpr) => s.annotateMap(e) });

    map.set(AnonymousExpr, { annotator: (s: TypeAnnotator, e: AnonymousExpr) => s.setType(e, s.schema.getUdfType(e)) });
    map.set(ArrayExpr, { annotator: (s: TypeAnnotator, e: ArrayExpr) => s.annotateByArgs(e, ['expressions'], { array: true }) });
    map.set(ArrayAggExpr, { annotator: (s: TypeAnnotator, e: ArrayAggExpr) => s.annotateByArgs(e, ['this'], { array: true }) });
    map.set(BracketExpr, { annotator: (s: TypeAnnotator, e: BracketExpr) => s.annotateBracket(e) });
    map.set(CaseExpr, { annotator: (s: TypeAnnotator, e: CaseExpr) => s.annotateByArgs(e, [...filterInstanceOf(e.args.ifs ?? [], IfExpr).flatMap((i) => i.args.true ?? []), 'default']) });
    map.set(CountExpr, { annotator: (s: TypeAnnotator, e: CountExpr) => s.setType(e, e.args.bigInt ? DataTypeExprKind.BIGINT : DataTypeExprKind.INT) });
    map.set(DateDiffExpr, { annotator: (s: TypeAnnotator, e: DateDiffExpr) => s.setType(e, e.args.bigInt ? DataTypeExprKind.BIGINT : DataTypeExprKind.INT) });
    map.set(DataTypeExpr, { annotator: (s: TypeAnnotator, e: DataTypeExpr) => s.setType(e, e.copy()) });
    map.set(DivExpr, { annotator: (s: TypeAnnotator, e: DivExpr) => s.annotateDiv(e) });
    map.set(DistinctExpr, { annotator: (s: TypeAnnotator, e: DistinctExpr) => s.annotateByArgs(e, ['expressions']) });
    map.set(DotExpr, { annotator: (s: TypeAnnotator, e: DotExpr) => s.annotateDot(e) });
    map.set(ExplodeExpr, { annotator: (s: TypeAnnotator, e: ExplodeExpr) => s.annotateExplode(e) });
    map.set(ExtractExpr, { annotator: (s: TypeAnnotator, e: ExtractExpr) => s.annotateExtract(e) });
    map.set(HexStringExpr, { annotator: (s: TypeAnnotator, e: HexStringExpr) => s.setType(e, e.args.isInteger ? DataTypeExprKind.BIGINT : DataTypeExprKind.BINARY) });
    map.set(GenerateSeriesExpr, {
      annotator: (s: TypeAnnotator, e: GenerateSeriesExpr) => s.annotateByArgs(e, [
        'start',
        'end',
        'step',
      ], { array: true }),
    });
    map.set(GenerateDateArrayExpr, { annotator: (s: TypeAnnotator, e: GenerateDateArrayExpr) => s.setType(e, DataTypeExpr.build('ARRAY<DATE>')) });
    map.set(GenerateTimestampArrayExpr, { annotator: (s: TypeAnnotator, e: GenerateTimestampArrayExpr) => s.setType(e, DataTypeExpr.build('ARRAY<TIMESTAMP>')) });
    map.set(IfExpr, { annotator: (s: TypeAnnotator, e: IfExpr) => s.annotateByArgs(e, ['true', 'false']) });
    map.set(LiteralExpr, { annotator: (s: TypeAnnotator, e: LiteralExpr) => s.annotateLiteral(e) });
    map.set(NullExpr, { returns: 'NULL' });
    map.set(NullifExpr, { annotator: (s: TypeAnnotator, e: NullifExpr) => s.annotateByArgs(e, ['this', 'expression']) });
    map.set(PropertyEqExpr, { annotator: (s: TypeAnnotator, e: PropertyEqExpr) => s.annotateByArgs(e, ['expression']) });
    map.set(StructExpr, { annotator: (s: TypeAnnotator, e: StructExpr) => s.annotateStruct(e) });
    map.set(SumExpr, { annotator: (s: TypeAnnotator, e: SumExpr) => s.annotateByArgs(e, ['this', 'expressions'], { promote: true }) });
    map.set(TimestampExpr, { annotator: (s: TypeAnnotator, e: TimestampExpr) => s.setType(e, e.args.withTz ? DataTypeExprKind.TIMESTAMPTZ : DataTypeExprKind.TIMESTAMP) });
    map.set(ToMapExpr, { annotator: (s: TypeAnnotator, e: ToMapExpr) => s.annotateToMap(e) });
    map.set(UnnestExpr, { annotator: (s: TypeAnnotator, e: UnnestExpr) => s.annotateUnnest(e) });
    map.set(SubqueryExpr, { annotator: (s: TypeAnnotator, e: SubqueryExpr) => s.annotateSubquery(e) });

    return map;
  }
}
