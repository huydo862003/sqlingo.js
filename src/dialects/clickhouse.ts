import {
  Generator, unsupportedArgs,
} from '../generator';
import {
  buildVarMap, Parser,
} from '../parser';
import {
  Tokenizer, TokenType,
} from '../tokens';
import type {
  Token, TokenPair,
} from '../tokens';
import type {
  ValuesExpr,
  JoinExpr,
  FuncExpr,
  PrimaryKeyColumnConstraintExpr,
  PrimaryKeyExpr,
  ExtractExpr,
  RegexpILikeExpr,
  NeqExpr,
  EqExpr,
  LikePropertyExpr,
  TryCastExpr,
  OffsetExpr,
  PreWhereExpr,
  CreateExpr,
  IsExpr,
} from '../expressions';
import {
  InExpr,
  CastExpr,
  MapExpr, TimeStrToTimeExpr,
  StrPositionExpr,
  PropertiesLocation,
  ToTablePropertyExpr,
  VolatilePropertyExpr,
  LeadExpr,
  LagExpr,
  ChrExpr,
  StddevExpr,
  SchemaCommentPropertyExpr,
  VarianceExpr,
  TrimExpr,
  Sha2DigestExpr,
  Sha1DigestExpr,
  IndexColumnConstraintExpr,
  PartitionExpr,
  PartitionIdExpr,
  ReplacePartitionExpr,
  QuantileExpr,
  CteExpr,
  NotExpr,
  VarMapExpr,
  IdentifierExpr,
  EnginePropertyExpr,
  SplitExpr,
  Expression,
  DateAddExpr,
  DateDiffExpr,
  TimestampTruncExpr,
  ArrayContainsExpr,
  TimeToStrExpr,
  ExplodeExpr,
  PartitionedByPropertyExpr,
  PivotExpr,
  RandExpr,
  StructExpr,
  AnyValueExpr,
  ApproxDistinctExpr,
  ArrayConcatExpr,
  ArraySumExpr,
  CountIfExpr,
  ColumnsExpr,
  ApplyExpr,
  DataTypeExprKind,
  ArrayFilterExpr,
  ArrayRemoveExpr,
  ArrayReverseExpr,
  ArraySliceExpr,
  ArrayExpr,
  ArgMaxExpr,
  ArgMinExpr,
  CurrentDatabaseExpr,
  CurrentSchemasExpr,
  CosineDistanceExpr,
  CompressColumnConstraintExpr,
  var_,
  UnixToTimeExpr,
  cast,
  DivExpr,
  CombinedAggFuncExpr,
  AnonymousExpr,
  StrToDateExpr,
  DataTypeExpr,
  LiteralExpr,
  DataTypeParamExpr,
  toIdentifier,
  TupleExpr,
  CurrentVersionExpr,
  DateSubExpr,
  ILikeExpr,
  JsonExtractScalarExpr,
  LengthExpr,
  LikeExpr,
  EuclideanDistanceExpr,
  RegexpLikeExpr,
  ParseDatetimeExpr,
  TimestampSubExpr,
  TimestampAddExpr,
  XorExpr,
  Md5DigestExpr,
  Sha2Expr,
  RegexpSplitExpr,
  SubstringIndexExpr,
  TypeofExpr,
  LevenshteinExpr,
  JarowinklerSimilarityExpr,
  TableExpr,
  GenerateSeriesExpr,
  TableAliasExpr,
  FinalExpr,
  WindowExpr,
  CombinedParameterizedAggExpr,
  ParameterizedAggExpr,
  AnonymousAggFuncExpr,
  OnClusterExpr,
  ProjectionDefExpr,
  JsonPathKeyExpr,
  JsonPathRootExpr,
  JsonPathSubscriptExpr,
  ShaExpr,
  Md5Expr,
  StartsWithExpr,
  TruncExpr,
  EndsWithExpr,
  NullifExpr,
  MedianExpr,
  JsonExtractExpr,
  AndExpr,
  OrExpr,
  IfExpr,
  PlaceholderExpr,
  PropertyEqExpr,
  CastToStrTypeExpr,
  ComputedColumnConstraintExpr,
  CurrentDateExpr,
  DateStrToDateExpr,
  FarmFingerprintExpr,
  IsNanExpr,
  JsonCastExpr,
  ExpressionKey,
  AnyExpr,
  isType,
  SelectExpr,
  FetchExpr,
  paren,
  QueryExpr,
  SchemaExpr,
} from '../expressions';
import {
  isInt,
  seqGet,
} from '../helper';
import {
  cache, narrowInstanceOf,
} from '../port_internals';
import type { DatetimeDelta } from './dialect';
import {
  buildDateDelta,
  buildFormattedTime,
  noPivotSql,
  Dialect, NormalizationStrategy, Dialects,
  unitToVar,
  renameFunc,
  varMapSql,
  buildLike,
  buildJsonExtractPath,
  trimSql,
  timestampTruncSql,
  sha256Sql,
  sha2DigestSql,
  strPositionSql,
  lengthOrCharLengthSql,
  jsonPathKeyOnlyName,
  jsonExtractSegments,
  removeFromArrayUsingFilter,
  argMaxOrMinNoCount,
  inlineArraySql,
} from './dialect';

const TIMESTAMP_TRUNC_UNITS = new Set([
  'MICROSECOND',
  'MILLISECOND',
  'SECOND',
  'MINUTE',
  'HOUR',
  'DAY',
  'MONTH',
  'QUARTER',
  'YEAR',
]);

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function buildDateTimeFormat<E extends Expression> (exprType: new (args: any) => E): (args: Expression[]) => E {
  return (args: Expression[]) => {
    const expr = buildFormattedTime(exprType, { dialect: Dialects.CLICKHOUSE })(args);
    const timezone = seqGet(args, 2);
    if (timezone) {
      expr.setArgKey('zone', timezone);
    }
    return expr;
  };
}

export function unixToTimeSql (this: Generator, expression: UnixToTimeExpr): string {
  const scale = expression.args.scale;
  const timestamp = expression.args.this;

  if (scale === undefined || scale === UnixToTimeExpr.SECONDS) {
    return this.func('fromUnixTimestamp', [cast(timestamp, DataTypeExprKind.BIGINT)]);
  }
  if (scale === UnixToTimeExpr.MILLIS) {
    return this.func('fromUnixTimestamp64Milli', [cast(timestamp, DataTypeExprKind.BIGINT)]);
  }
  if (scale === UnixToTimeExpr.MICROS) {
    return this.func('fromUnixTimestamp64Micro', [cast(timestamp, DataTypeExprKind.BIGINT)]);
  }
  if (scale === UnixToTimeExpr.NANOS) {
    return this.func('fromUnixTimestamp64Nano', [cast(timestamp, DataTypeExprKind.BIGINT)]);
  }

  return this.func('fromUnixTimestamp', [
    cast(
      new DivExpr({
        this: timestamp,
        expression: this.func('POW', ['10', scale]),
      }),
      DataTypeExprKind.BIGINT,
    ),
  ]);
}

function lowerFunc (sql: string): string {
  const index = sql.indexOf('(');
  if (index === -1) return sql.toLowerCase();
  return sql.slice(0, index).toLowerCase() + sql.slice(index);
}

function quantileSql (this: Generator, expression: QuantileExpr): string {
  const quantile = expression.args.quantile;
  const argsSql = `(${this.sql(expression, 'this')})`;

  let func: string;
  if (quantile instanceof ArrayExpr) {
    func = this.func('quantiles', quantile.args.expressions || []);
  } else {
    func = this.func('quantile', [quantile]);
  }

  return func + argsSql;
}

function buildCountIf (args: Expression[]): CountIfExpr | CombinedAggFuncExpr {
  if (args.length === 1) {
    return new CountIfExpr({ this: seqGet(args, 0) });
  }

  return new CombinedAggFuncExpr({
    this: 'countIf',
    expressions: args,
  });
}

function buildStrToDate (args: Expression[]): CastExpr | AnonymousExpr {
  if (args.length === 3) {
    return new AnonymousExpr({
      this: 'STR_TO_DATE',
      expressions: args,
    });
  }

  const strToDate = StrToDateExpr.fromArgList(args);
  return cast(strToDate, DataTypeExpr.build(DataTypeExprKind.DATETIME));
}

function datetimeDeltaSql (name: string): (this: Generator, expression: DatetimeDelta) => string {
  return function (this: Generator, expression: DatetimeDelta): string {
    if (!expression.unit) {
      return renameFunc(name).call(this, expression);
    }

    const zone = expression.getArgKey('zone');

    return this.func(name, [
      unitToVar(expression),
      expression.args.expression,
      expression.args.this,
      ...(Array.isArray(zone) ? [] : [narrowInstanceOf(zone, 'string', Expression)]),
    ]);
  };
}

function timeStrToTimeSql (this: Generator, expression: TimeStrToTimeExpr): string {
  let ts = expression.args.this;
  const tz = expression.args.zone;

  if (tz && ts instanceof LiteralExpr) {
    let tsString = ts.name.trim();

    // separate [date and time] from [fractional seconds and UTC offset]
    const tsParts = tsString.split('.');
    if (tsParts.length === 2) {
      // separate fractional seconds and UTC offset
      const offsetSep = tsParts[1].includes('+') ? '+' : '-';
      const tsFracParts = tsParts[1].split(offsetSep);
      const numFracParts = tsFracParts.length;

      // pad to 6 digits if fractional seconds present
      tsFracParts[0] = tsFracParts[0].padEnd(6, '0');
      tsString = [
        tsParts[0],
        '.',
        tsFracParts[0],
        1 < numFracParts ? offsetSep : '',
        1 < numFracParts ? tsFracParts[1] : '',
      ].join('');
    }

    // Convert to ISO format without timezone for Clickhouse
    // We use Date parsing; replace ' ' with 'T' for standard ISO compatibility if needed
    const date = new Date(tsString.replace(' ', 'T'));
    const tsWithoutTz = date.toISOString().replace('T', ' ')
      .split('.')[0];

    ts = LiteralExpr.string(tsWithoutTz);
  }

  // Non-nullable DateTime64 with microsecond precision
  const expressions = tz ? [new DataTypeParamExpr({ this: tz })] : [];
  const datatype = DataTypeExpr.build(DataTypeExprKind.DATETIME64, {
    expressions: [new DataTypeParamExpr({ this: LiteralExpr.number(6) }), ...expressions],
    nullable: false,
  });

  return this.sql(cast(
    ts,
    datatype,
    {
      dialect: this.dialect,
    },
  ));
}

function mapSql (this: Generator, expression: MapExpr | VarMapExpr): string {
  if (!(expression.parent && expression.parent.argKey === 'settings')) {
    return lowerFunc(varMapSql.call(this, expression));
  }

  const keys = expression.args.keys;
  const values = expression.args.values;

  if (!(keys instanceof ArrayExpr) || !(values instanceof ArrayExpr)) {
    this.unsupported('Cannot convert array columns into map.');
    return '';
  }

  const args: string[] = [];
  const keyExprs = keys.args.expressions;
  const valueExprs = values.args.expressions;

  for (let i = 0; i < Math.min(keyExprs?.length ?? 0, valueExprs?.length ?? 0); i++) {
    args.push(`${this.sql(keyExprs?.[i])}: ${this.sql(valueExprs?.[i])}`);
  }

  return `{${args.join(', ')}}`;
}

function buildTimestampTrunc (unit: string): (args: Expression[]) => TimestampTruncExpr {
  return (args: Expression[]): TimestampTruncExpr => {
    return new TimestampTruncExpr({
      this: seqGet(args, 0),
      unit: var_(unit),
      zone: seqGet(args, 1),
    });
  };
}

function buildSplitByChar (args: Expression[]): SplitExpr | AnonymousExpr {
  const sep = seqGet(args, 0);
  if (sep instanceof LiteralExpr) {
    const sepValue = sep.toValue();
    if (typeof sepValue === 'string' && new TextEncoder().encode(sepValue).length === 1) {
      return buildSplit(SplitExpr)(args);
    }
  }

  return new AnonymousExpr({
    this: 'splitByChar',
    expressions: args,
  });
}

function buildSplit<E extends Expression> (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expClass: new (args: any) => E,
): (args: Expression[]) => E {
  return (args: Expression[]): E =>
    new expClass({
      this: seqGet(args, 1),
      expression: seqGet(args, 0),
      limit: seqGet(args, 2),
    });
}

class ClickHouseTokenizer extends Tokenizer {
  static COMMENTS: (string | [string, string])[] = [
    '--',
    '#',
    '#!',
    ['/*', '*/'],
  ];

  static IDENTIFIERS = ['"', '`'];
  static IDENTIFIER_ESCAPES = ['\\'];
  static STRING_ESCAPES = ['\'', '\\'];
  static BIT_STRINGS: TokenPair[] = [['0b', '']];
  static HEX_STRINGS: TokenPair[] = [['0x', ''], ['0X', '']];
  static HEREDOC_STRINGS = ['$'];

  static ORIGINAL_KEYWORDS: Record<string, TokenType> = {
    ...Tokenizer.KEYWORDS,
    '.:': TokenType.DOTCOLON,
    'ATTACH': TokenType.COMMAND,
    'DATE32': TokenType.DATE32,
    'DATETIME64': TokenType.DATETIME64,
    'DICTIONARY': TokenType.DICTIONARY,
    'DYNAMIC': TokenType.DYNAMIC,
    'ENUM8': TokenType.ENUM8,
    'ENUM16': TokenType.ENUM16,
    'EXCHANGE': TokenType.COMMAND,
    'FINAL': TokenType.FINAL,
    'FIXEDSTRING': TokenType.FIXEDSTRING,
    'FLOAT32': TokenType.FLOAT,
    'FLOAT64': TokenType.DOUBLE,
    'GLOBAL': TokenType.GLOBAL,
    'LOWCARDINALITY': TokenType.LOWCARDINALITY,
    'MAP': TokenType.MAP,
    'NESTED': TokenType.NESTED,
    'NOTHING': TokenType.NOTHING,
    'SAMPLE': TokenType.TABLE_SAMPLE,
    'TUPLE': TokenType.STRUCT,
    'UINT16': TokenType.USMALLINT,
    'UINT32': TokenType.UINT,
    'UINT64': TokenType.UBIGINT,
    'UINT8': TokenType.UTINYINT,
    'IPV4': TokenType.IPV4,
    'IPV6': TokenType.IPV6,
    'POINT': TokenType.POINT,
    'RING': TokenType.RING,
    'LINESTRING': TokenType.LINESTRING,
    'MULTILINESTRING': TokenType.MULTILINESTRING,
    'POLYGON': TokenType.POLYGON,
    'MULTIPOLYGON': TokenType.MULTIPOLYGON,
    'AGGREGATEFUNCTION': TokenType.AGGREGATEFUNCTION,
    'SIMPLEAGGREGATEFUNCTION': TokenType.SIMPLEAGGREGATEFUNCTION,
    'SYSTEM': TokenType.COMMAND,
    'PREWHERE': TokenType.PREWHERE,
  };

  static {
    delete this.ORIGINAL_KEYWORDS['/*+'];
  }

  @cache
  static get SINGLE_TOKENS (): Record<string, TokenType> {
    return {
      ...Tokenizer.SINGLE_TOKENS,
      $: TokenType.HEREDOC_STRING,
    };
  }
}

class ClickHouseParser extends Parser {
  static MODIFIERS_ATTACHED_TO_SET_OP = false;
  static INTERVAL_SPANS = false;
  static OPTIONAL_ALIAS_TOKEN_CTE = false;
  static JOINS_HAVE_EQUAL_PRECEDENCE = true;

  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> {
    return {
      ...Parser.FUNCTIONS,
      ...Object.fromEntries(
        [...TIMESTAMP_TRUNC_UNITS].map((unit) => [`TOSTARTOF${unit}`, buildTimestampTrunc(unit)]),
      ),
      ANY: AnyValueExpr.fromArgList,
      ARRAYSUM: ArraySumExpr.fromArgList,
      ARRAYREVERSE: ArrayReverseExpr.fromArgList,
      ARRAYSLICE: ArraySliceExpr.fromArgList,
      CURRENTDATABASE: CurrentDatabaseExpr.fromArgList,
      CURRENTSCHEMAS: CurrentSchemasExpr.fromArgList,
      COUNTIF: buildCountIf,
      COSINEDISTANCE: CosineDistanceExpr.fromArgList,
      VERSION: CurrentVersionExpr.fromArgList,
      DATE_ADD: buildDateDelta(DateAddExpr, undefined, { defaultUnit: undefined }),
      DATEADD: buildDateDelta(DateAddExpr, undefined, { defaultUnit: undefined }),
      DATE_DIFF: buildDateDelta(DateDiffExpr, undefined, {
        defaultUnit: undefined,
        supportsTimezone: true,
      }),
      DATEDIFF: buildDateDelta(DateDiffExpr, undefined, {
        defaultUnit: undefined,
        supportsTimezone: true,
      }),
      DATE_FORMAT: buildDateTimeFormat(TimeToStrExpr),
      DATE_SUB: buildDateDelta(DateSubExpr, undefined, { defaultUnit: undefined }),
      DATESUB: buildDateDelta(DateSubExpr, undefined, { defaultUnit: undefined }),
      FORMATDATETIME: buildDateTimeFormat(TimeToStrExpr),
      HAS: ArrayContainsExpr.fromArgList,
      ILIKE: buildLike(ILikeExpr),
      JSONEXTRACTSTRING: buildJsonExtractPath(JsonExtractScalarExpr, { zeroBasedIndexing: false }),
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      LENGTH: (args: any[]) => new LengthExpr({
        this: seqGet(args, 0),
        binary: true,
      }),
      LIKE: buildLike(LikeExpr),
      L2Distance: EuclideanDistanceExpr.fromArgList,
      MAP: buildVarMap,
      MATCH: RegexpLikeExpr.fromArgList,
      NOTLIKE: buildLike(LikeExpr, { notLike: true }),
      PARSEDATETIME: buildDateTimeFormat(ParseDatetimeExpr),
      RANDCANONICAL: RandExpr.fromArgList,
      STR_TO_DATE: buildStrToDate,
      TIMESTAMP_SUB: buildDateDelta(TimestampSubExpr, undefined, { defaultUnit: undefined }),
      TIMESTAMPSUB: buildDateDelta(TimestampSubExpr, undefined, { defaultUnit: undefined }),
      TIMESTAMP_ADD: buildDateDelta(TimestampAddExpr, undefined, { defaultUnit: undefined }),
      TIMESTAMPADD: buildDateDelta(TimestampAddExpr, undefined, { defaultUnit: undefined }),
      TOMONDAY: buildTimestampTrunc('WEEK'),
      UNIQ: ApproxDistinctExpr.fromArgList,
      XOR: (args: Expression[]) => new XorExpr({ expressions: args }),
      MD5: Md5DigestExpr.fromArgList,
      SHA256: (args: Expression[]) =>
        new Sha2Expr({
          this: seqGet(args, 0),
          length: LiteralExpr.number(256),
        }),
      SHA512: (args: Expression[]) =>
        new Sha2Expr({
          this: seqGet(args, 0),
          length: LiteralExpr.number(512),
        }),
      SPLITBYCHAR: buildSplitByChar,
      SPLITBYREGEXP: buildSplit(RegexpSplitExpr),
      SPLITBYSTRING: buildSplit(SplitExpr),
      SUBSTRINGINDEX: SubstringIndexExpr.fromArgList,
      TOTYPENAME: TypeofExpr.fromArgList,
      EDITDISTANCE: LevenshteinExpr.fromArgList,
      JAROWINKLERSIMILARITY: JarowinklerSimilarityExpr.fromArgList,
      LEVENSHTEINDISTANCE: LevenshteinExpr.fromArgList,
    };
  }

  static {
    delete this.FUNCTIONS['TRANSFORM'];
    delete this.FUNCTIONS['APPROX_TOP_SUM'];
  }

  static AGG_FUNCTIONS = new Set([
    'count',
    'min',
    'max',
    'sum',
    'avg',
    'any',
    'stddevPop',
    'stddevSamp',
    'varPop',
    'varSamp',
    'corr',
    'covarPop',
    'covarSamp',
    'entropy',
    'exponentialMovingAverage',
    'intervalLengthSum',
    'kolmogorovSmirnovTest',
    'mannWhitneyUTest',
    'median',
    'rankCorr',
    'sumKahan',
    'studentTTest',
    'welchTTest',
    'anyHeavy',
    'anyLast',
    'boundingRatio',
    'first_value',
    'last_value',
    'argMin',
    'argMax',
    'avgWeighted',
    'topK',
    'approx_top_sum',
    'topKWeighted',
    'deltaSum',
    'deltaSumTimestamp',
    'groupArray',
    'groupArrayLast',
    'groupUniqArray',
    'groupArrayInsertAt',
    'groupArrayMovingAvg',
    'groupArrayMovingSum',
    'groupArraySample',
    'groupBitAnd',
    'groupBitOr',
    'groupBitXor',
    'groupBitmap',
    'groupBitmapAnd',
    'groupBitmapOr',
    'groupBitmapXor',
    'sumWithOverflow',
    'sumMap',
    'minMap',
    'maxMap',
    'skewSamp',
    'skewPop',
    'kurtSamp',
    'kurtPop',
    'uniq',
    'uniqExact',
    'uniqCombined',
    'uniqCombined64',
    'uniqHLL12',
    'uniqTheta',
    'quantile',
    'quantiles',
    'quantileExact',
    'quantilesExact',
    'quantilesExactExclusive',
    'quantileExactLow',
    'quantilesExactLow',
    'quantileExactHigh',
    'quantilesExactHigh',
    'quantileExactWeighted',
    'quantilesExactWeighted',
    'quantileTiming',
    'quantilesTiming',
    'quantileTimingWeighted',
    'quantilesTimingWeighted',
    'quantileDeterministic',
    'quantilesDeterministic',
    'quantileTDigest',
    'quantilesTDigest',
    'quantileTDigestWeighted',
    'quantilesTDigestWeighted',
    'quantileBFloat16',
    'quantilesBFloat16',
    'quantileBFloat16Weighted',
    'quantilesBFloat16Weighted',
    'simpleLinearRegression',
    'stochasticLinearRegression',
    'stochasticLogisticRegression',
    'categoricalInformationValue',
    'contingency',
    'cramersV',
    'cramersVBiasCorrected',
    'theilsU',
    'maxIntersections',
    'maxIntersectionsPosition',
    'meanZTest',
    'quantileInterpolatedWeighted',
    'quantilesInterpolatedWeighted',
    'quantileGK',
    'quantilesGK',
    'sparkBar',
    'sumCount',
    'largestTriangleThreeBuckets',
    'histogram',
    'sequenceMatch',
    'sequenceCount',
    'windowFunnel',
    'retention',
    'uniqUpTo',
    'sequenceNextNode',
    'exponentialTimeDecayedAvg',
  ]);

  static AGG_FUNCTIONS_SUFFIXES = [
    'If',
    'Array',
    'ArrayIf',
    'Map',
    'SimpleState',
    'State',
    'Merge',
    'MergeState',
    'ForEach',
    'Distinct',
    'OrDefault',
    'OrNull',
    'Resample',
    'ArgMin',
    'ArgMax',
  ];

  @cache
  static get FUNC_TOKENS (): Set<TokenType> {
    return new Set([
      ...Parser.FUNC_TOKENS,
      TokenType.AND,
      TokenType.OR,
      TokenType.SET,
    ]);
  }

  @cache
  static get RESERVED_TOKENS (): Set<TokenType> {
    return new Set(
      [...Parser.RESERVED_TOKENS].filter((t) => t !== TokenType.SELECT),
    );
  }

  @cache
  static get ID_VAR_TOKENS (): Set<TokenType> {
    return new Set([...Parser.ID_VAR_TOKENS, TokenType.LIKE]);
  }

  @cache
  static get AGG_FUNC_MAPPING (): Record<string, [string, string]> {
    const mapping: Record<string, [string, string]> = {};
    const suffixes = [...ClickHouseParser.AGG_FUNCTIONS_SUFFIXES, ''];
    for (const sfx of suffixes) {
      for (const f of ClickHouseParser.AGG_FUNCTIONS) {
        mapping[`${f}${sfx}`] = [f, sfx];
      }
    }
    return mapping;
  }

  @cache
  static get FUNCTION_PARSERS (): Record<string, (this: Parser) => Expression> {
    return {
      ...Parser.FUNCTION_PARSERS,
      ARRAYJOIN: function (this: Parser) {
        return this.expression(ExplodeExpr, { this: this.parseExpression() });
      },
      QUANTILE: function (this: Parser) {
        return (this as ClickHouseParser).parseQuantile();
      },
      MEDIAN: function (this: Parser) {
        return (this as ClickHouseParser).parseQuantile();
      },
      COLUMNS: function (this: Parser) {
        return (this as ClickHouseParser).parseColumns();
      },
      TUPLE: function (this: Parser) {
        return StructExpr.fromArgList(this.parseFunctionArgs({ alias: true }));
      },
      AND: function (this: Parser) {
        const args = this.parseFunctionArgs({ alias: false });
        return new AndExpr({
          this: args[0],
          expression: args[1],
        });
      },
      OR: function (this: Parser) {
        const args = this.parseFunctionArgs({ alias: false });
        return new OrExpr({
          this: args[0],
          expression: args[1],
        });
      },
    };
  }

  static {
    delete ClickHouseParser.FUNCTION_PARSERS['MATCH'];
  }

  @cache
  static get PROPERTY_PARSERS (): Record<string, (this: Parser) => Expression> {
    return {
      ...Parser.PROPERTY_PARSERS,
      ENGINE: function (this: Parser) {
        return (this as ClickHouseParser).parseEngineProperty();
      },
    };
  }

  static {
    delete ClickHouseParser.PROPERTY_PARSERS['DYNAMIC'];
  }

  @cache
  static get NO_PAREN_FUNCTION_PARSERS (): Partial<Record<string, (this: Parser) => Expression | undefined>> {
    return { ...Parser.NO_PAREN_FUNCTION_PARSERS };
  }

  static {
    delete ClickHouseParser.NO_PAREN_FUNCTION_PARSERS['ANY'];
  }

  @cache
  static get NO_PAREN_FUNCTIONS (): Partial<Record<TokenType, typeof Expression>> {
    return { ...Parser.NO_PAREN_FUNCTIONS };
  }

  static {
    delete (ClickHouseParser.NO_PAREN_FUNCTIONS as Record<string, unknown>)[TokenType.CURRENT_TIMESTAMP];
  }

  @cache
  static get RANGE_PARSERS (): Partial<Record<TokenType, (this: Parser, this_: Expression) => Expression | undefined>> {
    return {
      ...Parser.RANGE_PARSERS,
      [TokenType.GLOBAL]: function (this: Parser, thisNode: Expression) {
        return (this as ClickHouseParser).parseGlobalIn(thisNode);
      },
    };
  }

  @cache
  static get COLUMN_OPERATORS (): Partial<Record<TokenType, undefined | ((this: Parser, this_?: Expression, to?: Expression) => Expression)>> {
    return { ...Parser.COLUMN_OPERATORS };
  }

  static {
    delete ClickHouseParser.COLUMN_OPERATORS[TokenType.PLACEHOLDER];
  }

  @cache
  static get JOIN_KINDS (): Set<TokenType> {
    return new Set([
      ...Parser.JOIN_KINDS,
      TokenType.ANY,
      TokenType.ASOF,
      TokenType.ARRAY,
    ]);
  }

  @cache
  static get TABLE_ALIAS_TOKENS (): Set<TokenType> {
    return new Set(
      [...Parser.TABLE_ALIAS_TOKENS].filter(
        (t) =>
          ![
            TokenType.ANY,
            TokenType.ARRAY,
            TokenType.FINAL,
            TokenType.FORMAT,
            TokenType.SETTINGS,
          ].includes(t),
      ),
    );
  }

  @cache
  static get ALIAS_TOKENS (): Set<TokenType> {
    return new Set(
      [...Parser.ALIAS_TOKENS].filter((t) => t !== TokenType.FORMAT),
    );
  }

  static LOG_DEFAULTS_TO_LN = true;

  @cache
  static get QUERY_MODIFIER_PARSERS (): Record<string, (this: Parser) => [string, string | Expression | Expression[] | undefined]> {
    return {
      ...Parser.QUERY_MODIFIER_PARSERS,
      [TokenType.SETTINGS]: function (this: Parser) {
        return ['settings', ((this as ClickHouseParser).advance(), this.parseCsv(() => this.parseAssignment()))];
      },
      [TokenType.FORMAT]: function (this: Parser) {
        return ['format', ((this as ClickHouseParser).advance(), this.parseIdVar())];
      },
    };
  }

  @cache
  static get CONSTRAINT_PARSERS (): Record<string, (this: Parser) => Expression> {
    return {
      ...Parser.CONSTRAINT_PARSERS,
      INDEX: function (this: Parser) {
        return (this as ClickHouseParser).parseIndexConstraint();
      },
      CODEC: function (this: Parser) {
        return (this as ClickHouseParser).parseCompress();
      },
    };
  }

  @cache
  static get ALTER_PARSERS (): Record<string, (this: Parser) => Expression> {
    return {
      ...Parser.ALTER_PARSERS,
      REPLACE: function (this: Parser) {
        return (this as ClickHouseParser).parseAlterTableReplace()!;
      },
    };
  }

  @cache
  static get SCHEMA_UNNAMED_CONSTRAINTS (): Set<string> {
    return new Set([...Parser.SCHEMA_UNNAMED_CONSTRAINTS, 'INDEX']);
  }

  @cache
  static get PLACEHOLDER_PARSERS (): Record<string, (this: Parser) => Expression | undefined> {
    return {
      ...Parser.PLACEHOLDER_PARSERS,
      [TokenType.L_BRACE]: function (this: Parser) {
        return (this as ClickHouseParser).parseQueryParameter();
      },
    };
  }

  parseEngineProperty (): EnginePropertyExpr {
    this.match(TokenType.EQ);
    return this.expression(EnginePropertyExpr, {
      this: this.parseField({
        anyToken: true,
        anonymousFunc: true,
      }),
    });
  }

  // https://clickhouse.com/docs/en/sql-reference/statements/create/function
  parseUserDefinedFunctionExpression (): Expression | undefined {
    return this.parseLambda();
  }

  parseTypes (options: { checkFunc?: boolean;
    schema?: boolean;
    allowIdentifiers?: boolean; } = {}): Expression | undefined {
    const {
      checkFunc = false, schema = false, allowIdentifiers = true,
    } = options;
    const dtype = super.parseTypes({
      checkFunc,
      schema,
      allowIdentifiers,
    });

    if (dtype instanceof DataTypeExpr && (dtype.args.nullable as unknown) !== true) {
    // Mark every type as non-nullable which is ClickHouse's default
      dtype.setArgKey('nullable', false);
    }

    return dtype;
  }

  parseExtract (): ExtractExpr | AnonymousExpr {
    const index = this.index;
    const thisNode = this.parseBitwise();

    if (this.match(TokenType.FROM)) {
      this.retreat(index);
      return super.parseExtract() as ExtractExpr;
    }

    this.match(TokenType.COMMA);
    return this.expression(AnonymousExpr, {
      this: 'extract',
      expressions: [thisNode, this.parseBitwise()],
    });
  }

  parseAssignment (): Expression | undefined {
    const thisNode = super.parseAssignment();

    if (this.match(TokenType.PLACEHOLDER)) {
      return this.expression(IfExpr, {
        this: thisNode,
        true: this.parseAssignment(),
        false: this.match(TokenType.COLON) ? this.parseAssignment() : undefined,
      });
    }

    return thisNode;
  }

  parseQueryParameter (): Expression | undefined {
  /**
   * Parse a placeholder expression like SELECT {abc: UInt32} or FROM {table: Identifier}
   */
    const index = this.index;

    const thisNode = this.parseIdVar();
    this.match(TokenType.COLON);

    const kind =
      this.parseTypes({
        checkFunc: false,
        allowIdentifiers: false,
      })
      || (this.matchTextSeq('IDENTIFIER') ? 'Identifier' : undefined);

    if (!kind) {
      this.retreat(index);
      return undefined;
    } else if (!this.match(TokenType.R_BRACE)) {
      this.raiseError('Expecting }');
    }

    let finalThis = thisNode;
    if (thisNode instanceof IdentifierExpr && !thisNode.quoted) {
      finalThis = var_(thisNode.name);
    }

    return this.expression(PlaceholderExpr, {
      this: finalThis,
      kind: kind,
    });
  }

  parseBracket (thisNode?: Expression): Expression | undefined {
    const lBrace = this.match(TokenType.L_BRACE, { advance: false });
    const bracket = super.parseBracket(thisNode);

    if (lBrace && bracket instanceof StructExpr) {
      const varmap = new VarMapExpr({
        keys: new ArrayExpr({ expressions: [] }),
        values: new ArrayExpr({ expressions: [] }),
      });

      for (const expression of (bracket.args.expressions ?? [])) {
        if (!(expression instanceof PropertyEqExpr)) {
          break;
        }

        narrowInstanceOf(varmap.args.keys, Expression)?.append('expressions', LiteralExpr.string(expression.name));
        narrowInstanceOf(varmap.args.values, Expression)?.append('expressions', expression.args.expression);
      }

      return varmap;
    }

    return bracket;
  }

  parseIn (thisNode?: Expression, options: {
    isGlobal?: boolean;
    [index: string]: unknown;
  } = {}): InExpr {
    const { isGlobal = false } = options;
    const result = super.parseIn(thisNode);
    result.setArgKey('isGlobal', isGlobal);
    return result;
  }

  parseGlobalIn (thisNode?: Expression): NotExpr | InExpr | undefined {
    const isNegated = this.match(TokenType.NOT);
    const resultIn = this.match(TokenType.IN) ? this.parseIn(thisNode, { isGlobal: true }) : undefined;

    return isNegated ? this.expression(NotExpr, { this: resultIn }) : resultIn;
  }

  parseTable (options: {
    schema?: boolean;
    joins?: boolean;
    aliasTokens?: Set<TokenType>;
    parseBracket?: boolean;
    isDbReference?: boolean;
    parsePartition?: boolean;
    consumePipe?: boolean;
  } = {}): Expression | undefined {
    const {
      schema = false,
      joins = false,
      aliasTokens,
      parseBracket = false,
      isDbReference = false,
    } = options;

    let thisNode = super.parseTable({
      schema,
      joins,
      aliasTokens,
      parseBracket,
      isDbReference,
    });

    if (thisNode instanceof TableExpr) {
      const inner = thisNode.args.this;
      const alias = thisNode.args.alias;

      if (inner instanceof GenerateSeriesExpr && alias instanceof TableAliasExpr && !alias.args.columns) {
        alias.setArgKey('columns', [toIdentifier('generate_series')]);
      }
    }

    if (this.match(TokenType.FINAL)) {
      thisNode = this.expression(FinalExpr, { this: thisNode });
    }

    return thisNode;
  }

  parsePosition (_options: { haystackFirst?: boolean } = {}): StrPositionExpr {
    return super.parsePosition({ haystackFirst: true });
  }

  parseCte (): CteExpr | undefined {
  // WITH <identifier> AS <subquery expression>
    let cte = this.tryParse(() => super.parseCte() as CteExpr);

    if (!cte) {
    // WITH <expression> AS <identifier>
      cte = this.expression(CteExpr, {
        this: this.parseAssignment(),
        alias: this.parseTableAlias(),
        scalar: true,
      });
    }

    return cte;
  }

  parseJoinParts (): {
    method: Token | undefined;
    side: Token | undefined;
    kind: Token | undefined;
  } {
    const isGlobal = this.match(TokenType.GLOBAL) ? this.prev : undefined;
    const kindPre = this.matchSet(this._constructor.JOIN_KINDS, { advance: false }) ? this.prev : undefined;

    if (kindPre) {
      const kind = this.matchSet(this._constructor.JOIN_KINDS) ? this.prev : undefined;
      const side = this.matchSet(this._constructor.JOIN_SIDES) ? this.prev : undefined;
      return {
        method: isGlobal,
        side,
        kind,
      };
    }

    return {
      method: isGlobal,
      side: this.matchSet(this._constructor.JOIN_SIDES) ? this.prev : undefined,
      kind: this.matchSet(this._constructor.JOIN_KINDS) ? this.prev : undefined,
    };
  }

  parseJoin (options: {
    skipJoinToken?: boolean;
    parseBracket?: boolean;
  } = {}): JoinExpr | undefined {
    const { skipJoinToken = false } = options;
    const join = super.parseJoin({
      skipJoinToken,
      parseBracket: true,
    });

    if (join) {
      const method = join.args.method;
      join.setArgKey('method', undefined);
      join.setArgKey('global', method);

      // tbl ARRAY JOIN arr <-- this should be a `Column` reference, not a `Table`
      if (join.kind === 'ARRAY') {
        for (const table of join.findAll(TableExpr)) {
          table.replace(table.toColumn());
        }
      }
    }

    return join;
  }

  parseFunction (options: {
    functions?: Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression>;
    anonymous?: boolean;
    optionalParens?: boolean;
    anyToken?: boolean;
  } = {}): Expression | undefined {
    const {
      functions, anonymous, optionalParens = true, anyToken = false,
    } = options;

    let expr = super.parseFunction({
      functions,
      anonymous,
      optionalParens,
      anyToken,
    });

    if (!expr) return undefined;

    let func = expr instanceof WindowExpr ? expr.args.this : expr;

    // Aggregate functions can be split in 2 parts: <func_name><suffix>
    const parts =
      func instanceof AnonymousExpr
        ? (this._constructor as typeof ClickHouseParser).AGG_FUNC_MAPPING[(func.args.this ?? '').toString()]
        : undefined;

    if (parts) {
      const anonFunc = func as AnonymousExpr;
      const params = this.parseFuncParams(anonFunc);

      const kwargs: Record<string, unknown> = {
        this: anonFunc.args.this,
        expressions: anonFunc.args.expressions,
      };

      let expClass: typeof Expression;
      if (parts[1]) {
        expClass = params ? CombinedParameterizedAggExpr : CombinedAggFuncExpr;
      } else {
        expClass = params ? ParameterizedAggExpr : AnonymousAggFuncExpr;
      }

      if (params) {
        kwargs.params = params;
      }

      func = this.expression(expClass, kwargs);

      if (expr instanceof WindowExpr) {
      // The window's func was parsed as Anonymous in base parser, fix its type
        expr.setArgKey('this', func);
      } else if (params) {
      // Params blocked super.parseFunction from parsing the following window
        expr = this.parseWindow(func);
      } else {
        expr = func;
      }
    }

    return expr;
  }

  parseFuncParams (thisNode?: FuncExpr): Expression[] | undefined {
    if (this.matchPair(TokenType.R_PAREN, TokenType.L_PAREN)) {
      return this.parseCsv(() => this.parseLambda());
    }

    if (this.match(TokenType.L_PAREN)) {
      const params = this.parseCsv(() => this.parseLambda());
      this.matchRParen(thisNode);
      return params;
    }

    return undefined;
  }

  parseQuantile (): QuantileExpr {
    const thisNode = this.parseLambda() as Expression;
    const params = this.parseFuncParams();

    if (params && 0 < params.length) {
      return this.expression(QuantileExpr, {
        this: params[0],
        quantile: thisNode,
      });
    }

    return this.expression(QuantileExpr, {
      this: thisNode,
      quantile: LiteralExpr.number(0.5),
    });
  }

  parseWrappedIdVars (): Expression[] {
    return super.parseWrappedIdVars({ optional: true });
  }

  parsePrimaryKey (options: {
    wrappedOptional?: boolean;
    inProps?: boolean;
    namedPrimaryKey?: boolean;
  } = {}): PrimaryKeyColumnConstraintExpr | PrimaryKeyExpr {
    const {
      wrappedOptional = false, inProps = false, namedPrimaryKey = false,
    } = options;

    return super.parsePrimaryKey({
      wrappedOptional: wrappedOptional || inProps,
      inProps,
      namedPrimaryKey,
    });
  }

  parseOnProperty (): Expression | undefined {
    const index = this.index;
    if (this.matchTextSeq('CLUSTER')) {
      const thisNode = this.parseString() || this.parseIdVar();
      if (thisNode) {
        return this.expression(OnClusterExpr, { this: thisNode });
      } else {
        this.retreat(index);
      }
    }
    return undefined;
  }

  parseIndexConstraint (): IndexColumnConstraintExpr {
  // INDEX name1 expr TYPE type1(args) GRANULARITY value
    const thisNode = this.parseIdVar();
    const expression = this.parseAssignment();

    const indexType = this.matchTextSeq('TYPE')
      ? (this.parseFunction() || this.parseVar())
      : undefined;

    const granularity = this.matchTextSeq('GRANULARITY') ? this.parseTerm() : undefined;

    return this.expression(IndexColumnConstraintExpr, {
      this: thisNode,
      expression: expression,
      indexType: indexType,
      granularity: granularity,
    });
  }

  parsePartition (): PartitionExpr | undefined {
  // https://clickhouse.com/docs/en/sql-reference/statements/alter/partition#how-to-set-partition-expression
    if (!this.match(TokenType.PARTITION)) {
      return undefined;
    }

    let expressions: Expression[];
    if (this.matchTextSeq('ID')) {
    // Corresponds to the PARTITION ID <string_value> syntax
      expressions = [this.expression(PartitionIdExpr, { this: this.parseString() })];
    } else {
      expressions = this.parseExpressions();
    }

    return this.expression(PartitionExpr, { expressions });
  }

  parseAlterTableReplace (): Expression | undefined {
    const partition = this.parsePartition();

    if (!partition || !this.match(TokenType.FROM)) {
      return undefined;
    }

    return this.expression(ReplacePartitionExpr, {
      expression: partition,
      source: this.parseTableParts(),
    });
  }

  parseProjectionDef (): ProjectionDefExpr | undefined {
    if (!this.matchTextSeq('PROJECTION')) {
      return undefined;
    }

    return this.expression(ProjectionDefExpr, {
      this: this.parseIdVar(),
      expression: this.parseWrapped(() => this.parseStatement()),
    });
  }

  parseConstraint (): Expression | undefined {
    return super.parseConstraint() || this.parseProjectionDef();
  }

  parseAlias (thisNode: Expression | undefined, options: { explicit?: boolean } = {}): Expression | undefined {
    const { explicit = false } = options;
    // In clickhouse "SELECT <expr> APPLY(...)" is a query modifier,
    // so "APPLY" shouldn't be parsed as <expr>'s alias.
    if (this.matchPair(TokenType.APPLY, TokenType.L_PAREN, { advance: false })) {
      return thisNode;
    }

    return super.parseAlias(thisNode, { explicit });
  }

  parseExpression (): Expression | undefined {
    let thisNode = super.parseExpression();

    // Clickhouse allows "SELECT <expr> [APPLY(func)] [...]]" modifier
    while (this.matchPair(TokenType.APPLY, TokenType.L_PAREN)) {
      thisNode = new ApplyExpr({
        this: thisNode,
        expression: this.parseVar({ anyToken: true }),
      });
      this.match(TokenType.R_PAREN);
    }

    return thisNode;
  }

  parseColumns (): Expression {
    let thisNode: Expression = this.expression(ColumnsExpr, {
      this: this.parseLambda(),
    });

    while (this.next && this.matchTextSeq([
      ')',
      'APPLY',
      '(',
    ])) {
      this.match(TokenType.R_PAREN);
      thisNode = new ApplyExpr({
        this: thisNode,
        expression: this.parseVar({ anyToken: true })!,
      });
    }

    return thisNode;
  }

  parseValue (options: { values?: boolean } = {}): TupleExpr | undefined {
    const { values = true } = options;
    const value = super.parseValue({ values });

    if (!value) {
      return undefined;
    }

    // In Clickhouse "SELECT * FROM VALUES (1, 2, 3)" generates a table with a single column.
    // We canonicalize the values into a tuple-of-tuples AST if it's not already one.
    const expressions = value.args.expressions;
    const lastExpr = expressions?.[expressions.length - 1];

    if (values && !(lastExpr instanceof TupleExpr)) {
      value.setArgKey(
        'expressions',
        expressions?.map((expr) =>
          this.expression(TupleExpr, { expressions: [expr] })),
      );
    }

    return value;
  }

  parsePartitionedBy (): PartitionedByPropertyExpr {
  // ClickHouse allows custom expressions as partition key
  // https://clickhouse.com/docs/engines/table-engines/mergetree-family/custom-partitioning-key
    return this.expression(PartitionedByPropertyExpr, {
      this: this.parseAssignment(),
    });
  }
}

export class ClickHouseGenerator extends Generator {
  static QUERY_HINTS = false;
  static STRUCT_DELIMITER = ['(', ')'];
  static NVL2_SUPPORTED = false;
  static TABLESAMPLE_REQUIRES_PARENS = false;
  static TABLESAMPLE_SIZE_IS_ROWS = false;
  static TABLESAMPLE_KEYWORDS = 'SAMPLE';
  static LAST_DAY_SUPPORTS_DATE_PART = false;
  static CAN_IMPLEMENT_ARRAY_ANY = true;
  static SUPPORTS_TO_NUMBER = false;
  static JOIN_HINTS = false;
  static TABLE_HINTS = false;
  static GROUPINGS_SEP = '';
  static SET_OP_MODIFIERS = false;
  static ARRAY_SIZE_NAME = 'LENGTH';
  static WRAP_DERIVED_VALUES = false;

  static STRING_TYPE_MAPPING: Partial<Record<DataTypeExprKind, string>> = {
    [DataTypeExprKind.BLOB]: 'String',
    [DataTypeExprKind.CHAR]: 'String',
    [DataTypeExprKind.LONGBLOB]: 'String',
    [DataTypeExprKind.LONGTEXT]: 'String',
    [DataTypeExprKind.MEDIUMBLOB]: 'String',
    [DataTypeExprKind.MEDIUMTEXT]: 'String',
    [DataTypeExprKind.TINYBLOB]: 'String',
    [DataTypeExprKind.TINYTEXT]: 'String',
    [DataTypeExprKind.TEXT]: 'String',
    [DataTypeExprKind.VARBINARY]: 'String',
    [DataTypeExprKind.VARCHAR]: 'String',
  };

  @cache
  static get SUPPORTED_JSON_PATH_PARTS (): Set<typeof Expression> {
    return new Set([
      JsonPathKeyExpr,
      JsonPathRootExpr,
      JsonPathSubscriptExpr,
    ]);
  }

  @cache
  static get TYPE_MAPPING () {
    return new Map<DataTypeExprKind | string, string>([
      ...Generator.TYPE_MAPPING,
      ...Object.entries(ClickHouseGenerator.STRING_TYPE_MAPPING),
      [DataTypeExprKind.ARRAY, 'Array'],
      [DataTypeExprKind.BOOLEAN, 'Bool'],
      [DataTypeExprKind.BIGINT, 'Int64'],
      [DataTypeExprKind.DATE32, 'Date32'],
      [DataTypeExprKind.DATETIME, 'DateTime'],
      [DataTypeExprKind.DATETIME2, 'DateTime'],
      [DataTypeExprKind.SMALLDATETIME, 'DateTime'],
      [DataTypeExprKind.DATETIME64, 'DateTime64'],
      [DataTypeExprKind.DECIMAL, 'Decimal'],
      [DataTypeExprKind.DECIMAL32, 'Decimal32'],
      [DataTypeExprKind.DECIMAL64, 'Decimal64'],
      [DataTypeExprKind.DECIMAL128, 'Decimal128'],
      [DataTypeExprKind.DECIMAL256, 'Decimal256'],
      [DataTypeExprKind.TIMESTAMP, 'DateTime'],
      [DataTypeExprKind.TIMESTAMPNTZ, 'DateTime'],
      [DataTypeExprKind.TIMESTAMPTZ, 'DateTime'],
      [DataTypeExprKind.DOUBLE, 'Float64'],
      [DataTypeExprKind.ENUM, 'Enum'],
      [DataTypeExprKind.ENUM8, 'Enum8'],
      [DataTypeExprKind.ENUM16, 'Enum16'],
      [DataTypeExprKind.FIXEDSTRING, 'FixedString'],
      [DataTypeExprKind.FLOAT, 'Float32'],
      [DataTypeExprKind.INT, 'Int32'],
      [DataTypeExprKind.MEDIUMINT, 'Int32'],
      [DataTypeExprKind.INT128, 'Int128'],
      [DataTypeExprKind.INT256, 'Int256'],
      [DataTypeExprKind.LOWCARDINALITY, 'LowCardinality'],
      [DataTypeExprKind.MAP, 'Map'],
      [DataTypeExprKind.NESTED, 'Nested'],
      [DataTypeExprKind.NOTHING, 'Nothing'],
      [DataTypeExprKind.SMALLINT, 'Int16'],
      [DataTypeExprKind.STRUCT, 'Tuple'],
      [DataTypeExprKind.TINYINT, 'Int8'],
      [DataTypeExprKind.UBIGINT, 'UInt64'],
      [DataTypeExprKind.UINT, 'UInt32'],
      [DataTypeExprKind.UINT128, 'UInt128'],
      [DataTypeExprKind.UINT256, 'UInt256'],
      [DataTypeExprKind.USMALLINT, 'UInt16'],
      [DataTypeExprKind.UTINYINT, 'UInt8'],
      [DataTypeExprKind.IPV4, 'IPv4'],
      [DataTypeExprKind.IPV6, 'IPv6'],
      [DataTypeExprKind.POINT, 'Point'],
      [DataTypeExprKind.RING, 'Ring'],
      [DataTypeExprKind.LINESTRING, 'LineString'],
      [DataTypeExprKind.MULTILINESTRING, 'MultiLineString'],
      [DataTypeExprKind.POLYGON, 'Polygon'],
      [DataTypeExprKind.MULTIPOLYGON, 'MultiPolygon'],
      [DataTypeExprKind.AGGREGATEFUNCTION, 'AggregateFunction'],
      [DataTypeExprKind.SIMPLEAGGREGATEFUNCTION, 'SimpleAggregateFunction'],
      [DataTypeExprKind.DYNAMIC, 'Dynamic'],
    ]);
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static ORIGINAL_TRANSFORMS = new Map<typeof Expression, (this: Generator, e: any) => string>([
    ...Generator.ORIGINAL_TRANSFORMS,
    [AnyValueExpr, renameFunc('any')],
    [ApproxDistinctExpr, renameFunc('uniq')],
    [ArrayConcatExpr, renameFunc('arrayConcat')],
    [ArrayContainsExpr, renameFunc('has')],
    [
      ArrayFilterExpr,
      function (this: Generator, e: ArrayFilterExpr) {
        return this.func('arrayFilter', [e.args.expression, e.args.this]);
      },
    ],
    [ArrayRemoveExpr, removeFromArrayUsingFilter],
    [ArrayReverseExpr, renameFunc('arrayReverse')],
    [ArraySliceExpr, renameFunc('arraySlice')],
    [ArraySumExpr, renameFunc('arraySum')],
    [ArgMaxExpr, argMaxOrMinNoCount('argMax')],
    [ArgMinExpr, argMaxOrMinNoCount('argMin')],
    [ArrayExpr, inlineArraySql],
    [CastToStrTypeExpr, renameFunc('CAST')],
    [CurrentDatabaseExpr, renameFunc('CURRENT_DATABASE')],
    [CurrentSchemasExpr, renameFunc('CURRENT_SCHEMAS')],
    [CountIfExpr, renameFunc('countIf')],
    [CosineDistanceExpr, renameFunc('cosineDistance')],
    [
      CompressColumnConstraintExpr,
      function (this: Generator, e: CompressColumnConstraintExpr) {
        return `CODEC(${this.expressions(e, {
          key: 'this',
          flat: true,
        })})`;
      },
    ],
    [
      ComputedColumnConstraintExpr,
      function (this: Generator, e: ComputedColumnConstraintExpr) {
        return `${e.args.persisted ? 'MATERIALIZED' : 'ALIAS'} ${this.sql(e, 'this')}`;
      },
    ],
    [
      CurrentDateExpr,
      function (this: Generator, _e: CurrentDateExpr) {
        return this.func('CURRENT_DATE', []);
      },
    ],
    [CurrentVersionExpr, renameFunc('VERSION')],
    [DateAddExpr, datetimeDeltaSql('DATE_ADD')],
    [DateDiffExpr, datetimeDeltaSql('DATE_DIFF')],
    [DateStrToDateExpr, renameFunc('toDate')],
    [DateSubExpr, datetimeDeltaSql('DATE_SUB')],
    [ExplodeExpr, renameFunc('arrayJoin')],
    [FarmFingerprintExpr, renameFunc('farmFingerprint64')],
    [
      FinalExpr,
      function (this: Generator, e: FinalExpr) {
        return `${this.sql(e, 'this')} FINAL`;
      },
    ],
    [IsNanExpr, renameFunc('isNaN')],
    [
      JsonCastExpr,
      function (this: Generator, e: JsonCastExpr) {
        return `${this.sql(e, 'this')}.:${this.sql(e, 'to')}`;
      },
    ],
    [JsonExtractExpr, jsonExtractSegments('JSONExtractString', { quotedIndex: false })],
    [JsonExtractScalarExpr, jsonExtractSegments('JSONExtractString', { quotedIndex: false })],
    [JsonPathKeyExpr, jsonPathKeyOnlyName],
    [JsonPathRootExpr, () => ''],
    [LengthExpr, lengthOrCharLengthSql],
    [MapExpr, mapSql],
    [MedianExpr, renameFunc('median')],
    [NullifExpr, renameFunc('nullIf')],
    [
      PartitionedByPropertyExpr,
      function (this: Generator, e: PartitionedByPropertyExpr) {
        return `PARTITION BY ${this.sql(e, 'this')}`;
      },
    ],
    [PivotExpr, noPivotSql],
    [QuantileExpr, quantileSql],
    [
      RegexpLikeExpr,
      function (this: Generator, e: RegexpLikeExpr) {
        return this.func('match', [e.args.this, e.args.expression]);
      },
    ],
    [RandExpr, renameFunc('randCanonical')],
    [StartsWithExpr, renameFunc('startsWith')],
    [StructExpr, renameFunc('tuple')],
    [TruncExpr, renameFunc('trunc')],
    [EndsWithExpr, renameFunc('endsWith')],
    [EuclideanDistanceExpr, renameFunc('L2Distance')],
    [
      StrPositionExpr,
      function (this: Generator, e: StrPositionExpr) {
        return strPositionSql.call(this, e, {
          funcName: 'POSITION',
          supportsPosition: true,
          useAnsiPosition: false,
        });
      },
    ],
    [
      TimeToStrExpr,
      function (this: Generator, e: TimeToStrExpr) {
        return this.func('formatDateTime', [
          e.args.this,
          this.formatTime(e),
          e.args.zone,
        ]);
      },
    ],
    [TimeStrToTimeExpr, timeStrToTimeSql],
    [TimestampAddExpr, datetimeDeltaSql('TIMESTAMP_ADD')],
    [TimestampSubExpr, datetimeDeltaSql('TIMESTAMP_SUB')],
    [TypeofExpr, renameFunc('toTypeName')],
    [VarMapExpr, mapSql],
    [
      XorExpr,
      function (this: Generator, e: XorExpr) {
        return this.func('xor', [
          e.args.this,
          e.args.expression,
          ...e.args.expressions || [],
        ]);
      },
    ],
    [Md5DigestExpr, renameFunc('MD5')],
    [
      Md5Expr,
      function (this: Generator, e: Md5Expr) {
        return this.func('LOWER', [this.func('HEX', [this.func('MD5', [e.args.this])])]);
      },
    ],
    [ShaExpr, renameFunc('SHA1')],
    [Sha1DigestExpr, renameFunc('SHA1')],
    [Sha2Expr, sha256Sql],
    [Sha2DigestExpr, sha2DigestSql],
    [
      SplitExpr,
      function (this: Generator, e: SplitExpr) {
        return this.func('splitByString', [
          e.args.expression,
          e.args.this,
          typeof e.args.limit === 'number' ? LiteralExpr.number(e.args.limit) : e.args.limit,
        ]);
      },
    ],
    [
      RegexpSplitExpr,
      function (this: Generator, e: RegexpSplitExpr) {
        return this.func('splitByRegexp', [
          e.args.expression,
          e.args.this,
          typeof e.args.limit === 'number' ? LiteralExpr.number(e.args.limit) : e.args.limit,
        ]);
      },
    ],
    [UnixToTimeExpr, unixToTimeSql],
    [
      TimestampTruncExpr,
      timestampTruncSql({
        func: 'dateTrunc',
        zone: true,
      }),
    ],
    [
      TrimExpr,
      function (this: Generator, e: TrimExpr) {
        return trimSql.call(this, e, { defaultTrimType: 'BOTH' });
      },
    ],
    [VarianceExpr, renameFunc('varSamp')],
    [
      SchemaCommentPropertyExpr,
      function (this: Generator, e: SchemaCommentPropertyExpr) {
        return this.nakedProperty(e);
      },
    ],
    [StddevExpr, renameFunc('stddevSamp')],
    [ChrExpr, renameFunc('CHAR')],
    [
      LagExpr,
      function (this: Generator, e: LagExpr) {
        return this.func('lagInFrame', [
          e.args.this,
          e.args.offset,
          e.args.default,
        ]);
      },
    ],
    [
      LeadExpr,
      function (this: Generator, e: LeadExpr) {
        return this.func('leadInFrame', [
          e.args.this,
          e.args.offset,
          e.args.default,
        ]);
      },
    ],
    [JarowinklerSimilarityExpr, renameFunc('jaroWinklerSimilarity')],
    [
      LevenshteinExpr,
      function (this: Generator, e: LevenshteinExpr) {
        unsupportedArgs.call(this, e, 'insCost', 'delCost', 'subCost', 'maxDist');
        return renameFunc('editDistance').call(this, e);
      },
    ],
    [ParseDatetimeExpr, renameFunc('parseDateTime')],
  ]);

  @cache
  static get PROPERTIES_LOCATION () {
    return new Map<typeof Expression, PropertiesLocation>([
      ...Generator.PROPERTIES_LOCATION,
      [OnClusterExpr, PropertiesLocation.POST_NAME],
      [PartitionedByPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [ToTablePropertyExpr, PropertiesLocation.POST_NAME],
      [VolatilePropertyExpr, PropertiesLocation.UNSUPPORTED],
    ]);
  }

  static ON_CLUSTER_TARGETS = new Set([
    'SCHEMA',
    'DATABASE',
    'TABLE',
    'VIEW',
    'DICTIONARY',
    'INDEX',
    'FUNCTION',
    'NAMED COLLECTION',
  ]);

  static NON_NULLABLE_TYPES = new Set([
    DataTypeExprKind.ARRAY,
    DataTypeExprKind.MAP,
    DataTypeExprKind.STRUCT,
    DataTypeExprKind.POINT,
    DataTypeExprKind.RING,
    DataTypeExprKind.LINESTRING,
    DataTypeExprKind.MULTILINESTRING,
    DataTypeExprKind.POLYGON,
    DataTypeExprKind.MULTIPOLYGON,
  ]);

  offsetSql (expression: OffsetExpr): string {
    let offset = super.offsetSql(expression);

    // OFFSET ... FETCH syntax requires a "ROW" or "ROWS" keyword
    const parent = expression.parent;
    if (
      parent instanceof SelectExpr
      && parent.args.limit instanceof FetchExpr
    ) {
      offset = `${offset} ROWS`;
    }

    return offset;
  }

  strToDateSql (expression: StrToDateExpr): string {
    const strToDateSql = this.functionFallbackSql(expression);

    if (!(expression.parent instanceof CastExpr)) {
    // StrToDate returns DATEs in other dialects, so improve transpilation
      return this.castSql(cast(expression, 'DATE'));
    }

    return strToDateSql;
  }

  castSql (expression: CastExpr, options: { safePrefix?: string } = {}): string {
    const { safePrefix } = options;
    const thisNode = expression.args.this;

    if (
      thisNode instanceof StrToDateExpr
      && isType(expression.args.to, 'datetime')
    ) {
      return this.sql(thisNode);
    }

    return super.castSql(expression, { safePrefix });
  }

  tryCastSql (expression: TryCastExpr): string {
    const dtype = expression.args.to;

    if (
      dtype instanceof Expression
      && !dtype.isType(
        [...(this._constructor as typeof ClickHouseGenerator).NON_NULLABLE_TYPES],
        { checkNullable: true },
      )
    ) {
    // Casting x into Nullable(T) behaves similarly to TRY_CAST
      dtype.setArgKey('nullable', true);
    }

    return super.castSql(expression);
  }

  jsonPathSubscriptSql (expression: JsonPathSubscriptExpr): string {
    const thisNode = this.jsonPathPart(expression.args.this);
    return isInt(thisNode) ? (parseInt(thisNode) + 1).toString() : thisNode;
  }

  likePropertySql (expression: LikePropertyExpr): string {
    return `AS ${this.sql(expression, 'this')}`;
  }

  anyToHas (
    expression: EqExpr | NeqExpr,
    defaultCallback: (e: EqExpr | NeqExpr) => string,
    options: {
      prefix?: string;
    } = {},
  ): string {
    const {
      prefix = '',
    } = options;
    let arr: AnyExpr;
    let thisNode: Expression | undefined;

    if (expression.left instanceof AnyExpr) {
      arr = expression.left;
      thisNode = expression.right;
    } else if (expression.right instanceof AnyExpr) {
      arr = expression.right;
      thisNode = expression.left;
    } else {
      return defaultCallback(expression);
    }

    return prefix + this.func('has', [arr.args.this?.unnest(), thisNode]);
  }

  eqSql (expression: EqExpr): string {
    return this.anyToHas(expression, (e) => super.eqSql(e));
  }

  neqSql (expression: NeqExpr): string {
    return this.anyToHas(expression, (e) => super.neqSql(e), { prefix: 'NOT ' });
  }

  regexpILikeSql (expression: RegexpILikeExpr): string {
  // Manually add a flag to make the search case-insensitive
    const regex = this.func('CONCAT', ['\'(?i)\'', expression.args.expression]);
    return this.func('match', [expression.args.this, regex]);
  }

  dataTypeSql (expression: DataTypeExpr): string {
    let dtype: string;
    const Constructor = this._constructor as typeof ClickHouseGenerator;

    // String is the standard ClickHouse type, every other variant is just an alias.
    if (Constructor.STRING_TYPE_MAPPING[expression.args.this as DataTypeExprKind]) {
      dtype = 'String';
    } else {
      dtype = super.dataTypeSql(expression);
    }

    const parent = expression.parent;
    const nullable = expression.args.nullable;

    if (
      nullable === true
      || (nullable === undefined
        && !(
          parent instanceof DataTypeExpr
          && parent.isType(DataTypeExprKind.MAP, { checkNullable: true })
          && (expression.index === null || expression.index === 0)
        )
        && !expression.isType(Constructor.NON_NULLABLE_TYPES, {
          checkNullable: true,
        }))
    ) {
      dtype = `Nullable(${dtype})`;
    }

    return dtype;
  }

  cteSql (expression: CteExpr): string {
    if (expression.args.scalar) {
      const thisSql = this.sql(expression, 'this');
      const aliasSql = this.sql(expression, 'alias');
      return `${thisSql} AS ${aliasSql}`;
    }

    return super.cteSql(expression);
  }

  afterLimitModifiers (expression: Expression): string[] {
    const settings = expression.getArgKey('settings')
      ? this.seg('SETTINGS ') + this.expressions(expression, {
        key: 'settings',
        flat: true,
      })
      : '';

    const format = expression.getArgKey('format')
      ? this.seg('FORMAT ') + this.sql(expression, 'format')
      : '';

    return [
      ...super.afterLimitModifiers(expression),
      settings,
      format,
    ].filter(Boolean);
  }

  placeholderSql (expression: PlaceholderExpr): string {
    return `{${expression.name}: ${this.sql(expression, 'kind')}}`;
  }

  onClusterSql (expression: OnClusterExpr): string {
    return `ON CLUSTER ${this.sql(expression, 'this')}`;
  }

  createableSql (expression: CreateExpr, locations: Map<PropertiesLocation, Expression[]>): string {
    const Constructor = this._constructor as typeof ClickHouseGenerator;
    const postNameLocation = locations.get(PropertiesLocation.POST_NAME);

    if (Constructor.ON_CLUSTER_TARGETS.has(expression.kind as string) && postNameLocation) {
      const thisNode = expression.args.this instanceof SchemaExpr ? expression.args.this : expression;
      const thisName = this.sql(thisNode, 'this');
      const thisProperties = postNameLocation.map((prop) => this.sql(prop)).join(' ');

      let thisSchema = expression.args.this instanceof SchemaExpr ? this.schemaColumnsSql(expression.args.this) : undefined;
      thisSchema = thisSchema ? `${this.sep()}${thisSchema}` : '';

      return `${thisName}${this.sep()}${thisProperties}${thisSchema}`;
    }

    return super.createableSql(expression, locations);
  }

  createSql (expression: CreateExpr): string {
    const query = expression.args.expression;
    let commentProp: SchemaCommentPropertyExpr | undefined;

    if (query instanceof QueryExpr) {
      commentProp = expression.find(SchemaCommentPropertyExpr);
      if (commentProp) {
        commentProp.pop();
        query.replace(paren(query));
      }
    }

    const createSql = super.createSql(expression);
    const commentSql = commentProp ? ` ${this.sql(commentProp)}` : '';

    return `${createSql}${commentSql}`;
  }

  prewhereSql (expression: PreWhereExpr): string {
    const thisSql = this.indent(this.sql(expression, 'this'));
    return `${this.seg('PREWHERE')}${this.sep()}${thisSql}`;
  }

  indexColumnConstraintSql (expression: IndexColumnConstraintExpr): string {
    const thisName = this.sql(expression, 'this');
    const thisSql = thisName ? ` ${thisName}` : '';

    const exprName = this.sql(expression, 'expression');
    const exprSql = exprName ? ` ${exprName}` : '';

    const typeName = this.sql(expression, 'indexType');
    const typeSql = typeName ? ` TYPE ${typeName}` : '';

    const granName = this.sql(expression, 'granularity');
    const granSql = granName ? ` GRANULARITY ${granName}` : '';

    return `INDEX${thisSql}${exprSql}${typeSql}${granSql}`;
  }

  partitionSql (expression: PartitionExpr): string {
    return `PARTITION ${this.expressions(expression, { flat: true })}`;
  }

  partitionIdSql (expression: PartitionIdExpr): string {
    return `ID ${this.sql(expression.args.this)}`;
  }

  replacePartitionSql (expression: ReplacePartitionExpr): string {
    return `REPLACE ${this.sql(expression.args.expression)} FROM ${this.sql(expression, 'source')}`;
  }

  projectionDefSql (expression: ProjectionDefExpr): string {
    return `PROJECTION ${this.sql(expression.args.this)} ${this.wrap(expression.args.expression ?? '')}`;
  }

  isSql (expression: IsExpr): string {
    let isSql = super.isSql(expression);

    if (expression.parent instanceof NotExpr) {
    // value IS NOT NULL -> NOT (value IS NULL)
      isSql = this.wrap(isSql);
    }

    return isSql;
  }

  inSql (expression: InExpr): string {
    let inSql = super.inSql(expression);

    if (expression.parent instanceof NotExpr && expression.args.isGlobal) {
      inSql = inSql.replace('GLOBAL IN', 'GLOBAL NOT IN');
    }

    return inSql;
  }

  notSql (expression: NotExpr): string {
    const thisNode = expression.args.this;

    if (thisNode instanceof InExpr) {
      if (thisNode.args.isGlobal) {
      // let `GLOBAL IN` child interpose `NOT`
        return this.sql(expression, 'this');
      }

      expression.setArgKey('this', paren(thisNode, { copy: false }));
    }

    return super.notSql(expression);
  }

  valuesSql (expression: ValuesExpr, options: { valuesAsTable?: boolean } = {}): string {
    let { valuesAsTable = true } = options;
    const alias = expression.args.alias;
    const expressions = expression.args.expressions;

    if (alias?.args.columns && expressions && 0 < expressions.length) {
      const rowValues = expressions[0].args.expressions;
      valuesAsTable = rowValues?.some((value) => value instanceof TupleExpr) ?? false;
    } else {
      valuesAsTable = true;
    }

    return super.valuesSql(expression, { valuesAsTable });
  }
}

export class ClickHouse extends Dialect {
  static INDEX_OFFSET = 1;
  static NORMALIZE_FUNCTIONS = false as const;
  static NULL_ORDERING = 'nulls_are_last' as const;
  static SUPPORTS_USER_DEFINED_TYPES = false;
  static SAFE_DIVISION = true;
  static LOG_BASE_FIRST: boolean | undefined = undefined;
  static FORCE_EARLY_ALIAS_REF_EXPANSION = true;
  static PRESERVE_ORIGINAL_NAMES = true;
  static NUMBERS_CAN_BE_UNDERSCORE_SEPARATED = true;
  static IDENTIFIERS_CAN_START_WITH_DIGIT = true;
  static HEX_STRING_IS_INTEGER_TYPE = true;

  static NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE;

  static UNESCAPED_SEQUENCES = {
    '\\0': '\0',
  };

  static CREATABLE_KIND_MAPPING = { DATABASE: 'SCHEMA' };

  static SET_OP_DISTINCT_BY_DEFAULT: Partial<Record<ExpressionKey, boolean>> = {
    [ExpressionKey.EXCEPT]: false,
    [ExpressionKey.INTERSECT]: false,
  };

  generateValuesAliases (expression: ValuesExpr): IdentifierExpr[] {
  // Clickhouse allows VALUES to have an embedded structure e.g:
  // VALUES('person String, place String', ('Noah', 'Paris'), ...)
  // In this case, we don't want to qualify the columns
    const values = expression.args.expressions?.[0]?.args.expressions;

    const firstVal = values?.[0];
    const secondVal = values?.[1];

    const structure =
      values && 1 < values.length && firstVal instanceof Expression && firstVal?.isString && secondVal instanceof TupleExpr
        ? firstVal
        : undefined;

    let columnAliases: IdentifierExpr[] = [];

    if (structure) {
    // Split each column definition into the column name e.g:
    // 'person String, place String' -> ['person', 'place']
      const structureColdefs = structure.name.split(',').map((coldef) => coldef.trim());
      columnAliases = structureColdefs.map((coldef) =>
        toIdentifier(coldef.split(' ')[0]));
    } else if (values?.[0] instanceof Expression) {
    // Default column aliases in CH are "c1", "c2", etc.
      const rowWidth = values[0].args.expressions?.length;
      columnAliases = Array.from({ length: rowWidth ?? 0 }, (_, i) =>
        toIdentifier(`c${i + 1}`));
    }

    return columnAliases;
  }

  static Tokenizer = ClickHouseTokenizer;
  static Parser = ClickHouseParser;
  static Generator = ClickHouseGenerator;
}

Dialect.register(Dialects.CLICKHOUSE, ClickHouse);
