import {
  Generator,
  unsupportedArgs,
} from '../generator';
import {
  buildArrayConcat, buildExtractJsonWithPath, Parser,
  binaryRangeParser,
} from '../parser';
import type { TokenPair } from '../tokens';
import {
  Tokenizer, TokenType,
} from '../tokens';
import type {
  ApproxTopKExpr,
  Base64EncodeExpr,
  BitmapBitPositionExpr,
  BitmapBucketNumberExpr,
  BitmapConstructAggExpr,
  CommandExpr,
  CurrentDateExpr,
  FromIso8601TimestampExpr,
  FuncExpr,
  NormalExpr,
  RandstrExpr,
  StrToDateExpr,
  TableSampleExpr,
  TimeSliceExpr,
  ToBinaryExpr,
  TruncExpr,
  UniformExpr,
  ZipfExpr,
  CountIfExpr,
  ColumnDefExpr,
  JoinExpr,
  TimestampLtzFromPartsExpr,
  TimestampTzFromPartsExpr,
  AddMonthsExpr,
  FormatExpr,
  TimestampTruncExpr,
  BitwiseNotExpr,
  WithinGroupExpr,
  LowerExpr,
  PadExpr,
  ReverseExpr,
  ArraysZipExpr,
  ApproximateSimilarityExpr,
  MinhashExpr,
  MinhashCombineExpr,
  NumberToStrExpr,
  SpaceExpr,
  TableFromRowsExpr,
  MapCatExpr,
  ObjectInsertExpr,
  ArrayToStringExpr,
  RespectNullsExpr,
  ExpressionValue,
} from '../expressions';
import {
  FirstValueExpr,
  GeneratorExpr,
  GreatestExpr,
  LeastExpr,
  TrimPosition,
  LagExpr,
  LastValueExpr,
  LeadExpr,
  LikePropertyExpr,
  NthValueExpr,
  PropertiesLocation,
  SequencePropertiesExpr,
  TemporaryPropertyExpr,
  ArrayInsertExpr,
  ArrayRemoveAtExpr,
  ArrayRemoveExpr,
  ArraySortExpr,
  ArraySumExpr,
  ArrayUniqueAggExpr,
  Base64DecodeBinaryExpr,
  Base64DecodeStringExpr,
  BitwiseAndExpr,
  BitwiseLeftShiftExpr,
  BitwiseOrExpr,
  BitwiseRightShiftExpr,
  BoolxorAggExpr,
  ByteLengthExpr,
  CeilExpr,
  CommentColumnConstraintExpr,
  CorrExpr,
  CurrentTimeExpr,
  CurrentTimestampExpr,
  DateExpr,
  DateStrToDateExpr,
  DateSubExpr,
  DatetimeAddExpr,
  DatetimeDiffExpr,
  DatetimeExpr,
  DatetimeSubExpr,
  DatetimeTruncExpr,
  DateToDiExpr,
  DaynameExpr,
  DayOfMonthExpr,
  DayOfWeekExpr,
  DayOfWeekIsoExpr,
  DayOfYearExpr,
  DistinctExpr,
  DiToDateExpr,
  EqExpr,
  EqualNullExpr,
  ExtractExpr,
  FloorExpr,
  GenerateTimestampArrayExpr,
  GroupConcatExpr,
  InitcapExpr,
  IntDivExpr,
  IsArrayExpr,
  IsInfExpr,
  IsNanExpr,
  IsNullValueExpr,
  JsonbExistsExpr,
  JsonbObjectAggExpr,
  JsonExtractArrayExpr,
  JsonFormatExpr,
  JsonObjectAggExpr,
  LastDayExpr,
  LateralExpr,
  LocaltimeExpr,
  LogicalAndExpr,
  LogicalOrExpr,
  MakeIntervalExpr,
  Md5DigestExpr,
  MonthnameExpr,
  MonthsBetweenExpr,
  NullSafeEqExpr,
  PivotExpr,
  PreviousDayExpr,
  RandExpr,
  RegexpILikeExpr,
  RegrValxExpr,
  RegrValyExpr,
  ReturnExpr,
  ReturnsPropertyExpr,
  SchemaExpr,
  Seq1Expr,
  Seq2Expr,
  Seq4Expr,
  Seq8Expr,
  Sha1DigestExpr,
  Sha2DigestExpr,
  ShaExpr,
  StrPositionExpr,
  StrToUnixExpr,
  TimeAddExpr,
  TimeDiffExpr,
  TimeExpr,
  TimestampAddExpr,
  TimestampDiffExpr,
  TimestampExpr,
  TimestampSubExpr,
  TimeStrToDateExpr,
  TimeStrToTimeExpr,
  TimeStrToUnixExpr,
  TimeSubExpr,
  ToBooleanExpr,
  TsOrDiToDiExpr,
  TsOrDsDiffExpr,
  UnixMicrosExpr,
  UnixMillisExpr,
  UnixSecondsExpr,
  UnixToStrExpr,
  UnixToTimeStrExpr,
  XorExpr,
  YearOfWeekExpr,
  YearOfWeekIsoExpr,
  AttachExpr,
  DetachExpr,
  InstallExpr,
  MapExpr,
  ToMapExpr,
  AnyValueExpr,
  ArrayAppendExpr,
  ArrayContainsExpr,
  ArrayFilterExpr,
  BitwiseXorAggExpr,
  BitwiseXorExpr,
  CosineDistanceExpr,
  CurrentVersionExpr,
  DateBinExpr,
  DateFromPartsExpr,
  EncodeExpr,
  EuclideanDistanceExpr,
  ExplodeExpr,
  GetbitExpr,
  JarowinklerSimilarityExpr,
  JsonExtractScalarExpr,
  LevenshteinExpr,
  ParseJsonExpr,
  PercentileContExpr,
  PercentileDiscExpr,
  RegexpExtractAllExpr,
  RegexpExtractExpr,
  RegexpLikeExpr,
  RegexpReplaceExpr,
  RegexpSplitExpr,
  Sha2Expr,
  ShowExpr,
  SplitExpr,
  StrToTimeExpr,
  StructExpr,
  TimeFromPartsExpr,
  TimeToStrExpr,
  TimeToUnixExpr,
  TransformExpr,
  RoundExpr,
  Expression,
  AddExpr,
  AnonymousExpr,
  ArrayExpr,
  ArrayPrependExpr,
  AtTimeZoneExpr,
  BitwiseAndAggExpr,
  BitwiseOrAggExpr,
  BooleanExpr,
  BinaryExpr,
  BracketExpr,
  CaseExpr,
  CastExpr,
  DataTypeExpr,
  DataTypeExprKind,
  DateAddExpr,
  DateDiffExpr,
  DateTruncExpr,
  DecodeExpr,
  DivExpr,
  FromBase64Expr,
  GenerateDateArrayExpr,
  GenerateSeriesExpr,
  HavingExpr,
  HavingMaxExpr,
  HexStringExpr,
  IdentifierExpr,
  IfExpr,
  InExpr,
  IntervalExpr,
  IsExpr,
  isType,
  JsonExtractExpr,
  JsonValueArrayExpr,
  LengthExpr,
  LiteralExpr,
  ModExpr,
  MulExpr,
  NextDayExpr,
  NotExpr,
  OrderExpr,
  OrExpr,
  ParenExpr,
  PropertyEqExpr,
  ReplaceExpr,
  replacePlaceholders,
  SelectExpr,
  SliceExpr,
  SortArrayExpr,
  SubExpr,
  TimestampFromPartsExpr,
  UnhexExpr,
  UnixToTimeExpr,
  UpperExpr,
  VarExpr,
  WeekStartExpr,
  WhereExpr,
  WindowExpr,
  AggFuncExpr,
  null_,
  PlaceholderExpr,
  AbsExpr,
  ArrayOverlapsExpr,
  StartsWithExpr,
  RegexpFullMatchExpr,
  PowExpr,
  IgnoreNullsExpr,
  AliasExpr,
  TableAliasExpr,
  AttachOptionExpr,
  LambdaExpr,
  PositionalColumnExpr,
  ApproxDistinctExpr,
  BoolnotExpr,
  BoolandExpr,
  BoolorExpr,
  ArrayCompactExpr,
  ArrayConstructCompactExpr,
  ArrayConcatExpr,
  TsOrDsAddExpr,
  VariancePopExpr,
  WeekOfYearExpr,
  maybeParse,
  GtExpr,
  TsOrDsToTimeExpr,
  TryCastExpr,
  FilterExpr,
  AndExpr,
  UserDefinedFunctionExpr,
  UnnestExpr,
  JsonPathKeyExpr,
  JsonPathRootExpr,
  JsonPathSubscriptExpr,
  JsonPathWildcardExpr,
  FromExpr,
  ApproxQuantileExpr,
  ApproxQuantilesExpr,
  NegExpr,
  TrimExpr,
  OrderedExpr,
  ArrayAggExpr,
  ColumnExpr,
  ConcatExpr,
  DPipeExpr,
  RepeatExpr,
  StarExpr,
  SubstringExpr,
  AliasesExpr,
  FirstExpr,
  KwargExpr,
  NullifExpr,
  ChrExpr,
  TableExpr,
  ToBase64Expr,
  PosexplodeExpr,
} from '../expressions';
import {
  seqGet, isDateUnit,
} from '../helper';
import {
  cache, narrowInstanceOf,
} from '../port_internals';
import {
  inheritStructFieldNames, preprocess, unqualifyColumns,
} from '../transforms';
import { annotateTypes } from '../optimizer';
import {
  arrowJsonExtractSql,
  dateDeltaToBinaryIntervalOp as dateDeltaToBinaryIntervalOpBase,
  unitToStr,
  Dialect, Dialects,
  NormalizationStrategy,
  dateTruncToTime,
  buildRegexpExtract,
  buildFormattedTime,
  binaryFromFunction,
  pivotColumnNames,
  buildDefaultDecimalType,
  approxCountDistinctSql,
  inlineArrayUnlessQuery,
  arrayAppendSql,
  arrayCompactSql,
  arrayConcatSql,
  renameFunc,
  removeFromArrayUsingFilter,
  noCommentColumnConstraintSql,
  dateStrToDateSql,
  noDatetimeSql,
  encodeDecodeSql,
  getBitSql,
  groupConcatSql,
  explodeToUnnestSql,
  noMakeIntervalSql,
  monthsBetweenSql,
  regexpReplaceGlobalModifier,
  strPositionSql,
  noTimeSql,
  noTimestampSql,
  timeStrToTimeSql,
  countIfToSum,
} from './dialect';
import type { JsonExtractType } from './dialect';
import { strToTimeSql } from './presto';

/**
 * Regex to detect time zones in timestamps of the form [+|-]TT[:tt]
 * The pattern matches timezone offsets that appear after the time portion
 */
export const TIMEZONE_PATTERN = /:\d{2}.*?[+\-]\d{2}(?::\d{2})?/;

/**
 * Characters that must be escaped when building regex expressions in INITCAP
 */
export const REGEX_ESCAPE_REPLACEMENTS: Record<string, string> = {
  '\\': '\\\\',
  '-': '\\-',
  '^': '\\^',
  '[': '\\[',
  ']': '\\]',
};

/**
 * Used to in RANDSTR transpilation
 */
export const RANDSTR_CHAR_POOL = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
export const RANDSTR_SEED = 123456;

/**
 * Whitespace control characters that DuckDB must process with `CHR({val})` calls
 */
export const WS_CONTROL_CHARS_TO_DUCK: Record<string, number> = {
  '\u000b': 11,
  '\u001c': 28,
  '\u001d': 29,
  '\u001e': 30,
  '\u001f': 31,
};

/**
 * Days of week to ISO 8601 day-of-week numbers
 * ISO 8601 standard: Monday=1, ..., Sunday=7
 */
export const WEEK_START_DAY_TO_DOW: Record<string, number> = {
  MONDAY: 1,
  TUESDAY: 2,
  WEDNESDAY: 3,
  THURSDAY: 4,
  FRIDAY: 5,
  SATURDAY: 6,
  SUNDAY: 7,
};

export const MAX_BIT_POSITION = LiteralExpr.number(32768);

/**
 * SEQ function constants
 */
export const SEQ_BASE = '(ROW_NUMBER() OVER (ORDER BY 1) - 1)';
export const SEQ_RESTRICTED = [
  WhereExpr,
  HavingExpr,
  AggFuncExpr,
  OrderExpr,
  SelectExpr,
];

/**
 * Apply base64 alphabet character replacements.
 *
 * Base64 alphabet can be 1-3 chars: 1st = index 62 ('+'), 2nd = index 63 ('/'), 3rd = padding ('=').
 */
function applyBase64AlphabetReplacements (
  result: Expression,
  alphabet: Expression | undefined,
  options: { reverse?: boolean } = {},
): Expression {
  const {
    reverse = false,
  } = options;
  if (alphabet instanceof LiteralExpr && alphabet.isString) {
    const defaultChars = '+/=';
    const newChars = alphabet.args.this ?? '';

    for (let i = 0; i < Math.min(defaultChars.length, newChars.length); i++) {
      const defaultChar = defaultChars[i];
      const newChar = newChars[i] ?? '';

      if (newChar !== defaultChar) {
        const [find, replace] = reverse ? [newChar, defaultChar] : [defaultChar, newChar];
        result = new ReplaceExpr({
          this: result,
          expression: LiteralExpr.string(find),
          replacement: LiteralExpr.string(replace),
        });
      }
    }
  }
  return result;
}

/**
 * Transpile Snowflake BASE64_DECODE_STRING/BINARY to DuckDB.
 */
function base64DecodeSql (self: Generator, expression: Base64EncodeExpr, options: { toString?: boolean }): string {
  const { toString = false } = options;

  let inputExpr = expression.args.this;
  const alphabet = expression.args.alphabet;

  inputExpr = applyBase64AlphabetReplacements(inputExpr ?? null_(), alphabet, { reverse: true });
  inputExpr = new FromBase64Expr({ this: inputExpr });

  if (toString) {
    inputExpr = new DecodeExpr({ this: inputExpr });
  }

  return self.sql(inputExpr);
}

/**
 * DuckDB's LAST_DAY only supports month. Logic handles year, quarter, week.
 */
function lastDaySql (self: Generator, expression: LastDayExpr): string {
  const dateExpr = expression.args.this;
  const unit = expression.text('unit').toUpperCase();

  if (!unit || unit === 'MONTH') {
    return self.func('LAST_DAY', [dateExpr]);
  }

  if (unit === 'YEAR') {
    const yearExpr = self.func('EXTRACT', [new VarExpr({ this: 'YEAR' }), dateExpr]);
    const makeDateExpr = self.func('MAKE_DATE', [
      yearExpr,
      LiteralExpr.number(12),
      LiteralExpr.number(31),
    ]);
    return self.sql(makeDateExpr);
  }

  if (unit === 'QUARTER') {
    const yearExpr = self.func('EXTRACT', [new VarExpr({ this: 'YEAR' }), dateExpr]);
    const quarterExpr = self.func('EXTRACT', [new VarExpr({ this: 'QUARTER' }), dateExpr]);

    const lastMonthExpr = new MulExpr({
      this: quarterExpr,
      expression: LiteralExpr.number(3),
    });
    const firstDayLastMonthExpr = self.func('MAKE_DATE', [
      yearExpr,
      lastMonthExpr,
      LiteralExpr.number(1),
    ]);

    const lastDayExpr = self.func('LAST_DAY', [firstDayLastMonthExpr]);
    return self.sql(lastDayExpr);
  }

  if (unit === 'WEEK') {
    const dow = self.func('EXTRACT', [new VarExpr({ this: 'DAYOFWEEK' }), dateExpr]);
    const daysToSundayExpr = new ModExpr({
      this: new ParenExpr({
        this: new SubExpr({
          this: LiteralExpr.number(7),
          expression: dow,
        }),
      }),
      expression: LiteralExpr.number(7),
    });
    const intervalExpr = new IntervalExpr({
      this: daysToSundayExpr,
      unit: new VarExpr({ this: 'DAY' }),
    });
    const addExpr = new AddExpr({
      this: dateExpr,
      expression: intervalExpr,
    });
    const castExpr = new CastExpr({
      this: addExpr,
      to: new DataTypeExpr({ this: DataTypeExprKind.DATE }),
    });
    return self.sql(castExpr);
  }

  self.unsupported(`Unsupported date part '${unit}' in LAST_DAY function`);
  return self.functionFallbackSql(expression);
}

function isNanosecondUnit (unit: Expression | undefined): boolean {
  return (unit instanceof VarExpr || unit instanceof LiteralExpr) && unit.name.toUpperCase() === 'NANOSECOND';
}

/** Generate NANOSECOND diff using EPOCH_NS since DATE_DIFF doesn't support it. */
function handleNanosecondDiff (self: Generator, endTime: Expression, startTime: Expression): string {
  const endNs = new CastExpr({
    this: endTime,
    to: new DataTypeExpr({ this: DataTypeExprKind.TIMESTAMP_NS }),
  });
  const startNs = new CastExpr({
    this: startTime,
    to: new DataTypeExpr({ this: DataTypeExprKind.TIMESTAMP_NS }),
  });

  return self.sql(
    new SubExpr({
      this: self.func('EPOCH_NS', [endNs]),
      expression: self.func('EPOCH_NS', [startNs]),
    }),
  );
}

/** Transpile TO_BOOLEAN and TRY_TO_BOOLEAN from Snowflake to DuckDB. */
function toBooleanSql (self: Generator, expression: ToBooleanExpr): string {
  const arg = expression.args.this;
  const isSafe = expression.args.safe ?? false;

  const baseCaseExpr = new CaseExpr({})
    .when(
      new UpperExpr({
        this: new CastExpr({
          this: arg,
          to: new DataTypeExpr({ this: DataTypeExprKind.VARCHAR }),
        }),
      }).eq(
        LiteralExpr.string('ON'),
      ),
      new BooleanExpr({ this: true }),
    )
    .when(
      new UpperExpr({
        this: new CastExpr({
          this: arg,
          to: new DataTypeExpr({ this: DataTypeExprKind.VARCHAR }),
        }),
      }).eq(
        LiteralExpr.string('OFF'),
      ),
      new BooleanExpr({ this: false }),
    );

  if (isSafe) {
    baseCaseExpr.else(self.func('TRY_CAST', [arg, DataTypeExpr.build('BOOLEAN')]));
  } else {
    const castToReal = self.func('TRY_CAST', [arg, DataTypeExpr.build('REAL')]);
    const nanInfCheck = new OrExpr({
      this: self.func('ISNAN', [castToReal]),
      expression: self.func('ISINF', [castToReal]),
    });

    baseCaseExpr
      .when(
        nanInfCheck,
        self.func('ERROR', [LiteralExpr.string('TO_BOOLEAN: Non-numeric values NaN and INF are not supported')]),
      )
      .else(new CastExpr({
        this: arg,
        to: new DataTypeExpr({ this: DataTypeExprKind.BOOLEAN }),
      }));
  }

  return self.sql(baseCaseExpr);
}

/** BigQuery -> DuckDB conversion for the DATE function */
function dateSql (self: Generator, expression: DateExpr): string {
  let thisNode = expression.args.this;
  const zone = self.sql(expression, 'zone');

  if (zone) {
    // BigQuery UTC -> specified zone -> keep DATE part
    thisNode = new CastExpr({
      this: thisNode,
      to: new DataTypeExpr({ this: DataTypeExprKind.TIMESTAMP }),
    });
    const atUtc = new AtTimeZoneExpr({
      this: thisNode,
      zone: LiteralExpr.string('UTC'),
    });
    thisNode = new AtTimeZoneExpr({
      this: atUtc,
      zone,
    });
  }

  return self.sql(new CastExpr({
    this: thisNode,
    to: new DataTypeExpr({ this: DataTypeExprKind.DATE }),
  }));
}

/** BigQuery -> DuckDB conversion for the TIME_DIFF function */
function timeDiffSql (self: Generator, expression: TimeDiffExpr): string {
  const unit = expression.args.unit;

  if (isNanosecondUnit(unit)) {
    return handleNanosecondDiff(self, expression.args.expression ?? null_(), expression.args.this ?? null_());
  }

  const thisNode = new CastExpr({
    this: expression.args.this,
    to: new DataTypeExpr({ this: DataTypeExprKind.TIME }),
  });
  const exprNode = new CastExpr({
    this: expression.args.expression,
    to: new DataTypeExpr({ this: DataTypeExprKind.TIME }),
  });

  // BQ flips operands relative to standard DATE_DIFF
  return self.func('DATE_DIFF', [
    unitToStr(expression),
    exprNode,
    thisNode,
  ]);
}

/** Handles NANOSECOND units and float interval rounding for DuckDB */
function dateDeltaToBinaryIntervalOp (options: { cast?: boolean } = {}): (self: Generator, expression: DatetimeAddExpr | DatetimeSubExpr) => string {
  const {
    cast = true,
  } = options;
  const baseImpl = dateDeltaToBinaryIntervalOpBase({ cast });

  return (self: Generator, expression: DatetimeSubExpr | DatetimeAddExpr): string => {
    const unit = narrowInstanceOf(expression.getArgKey('unit'), 'string', Expression);
    let intervalValue: Expression | undefined = expression.args.expression;

    if (isNanosecondUnit(unit instanceof Expression ? unit : undefined)) {
      if (intervalValue instanceof IntervalExpr) {
        intervalValue = intervalValue.args.this;
      }

      const timestampNs = new CastExpr({
        this: narrowInstanceOf(expression.args.this, 'string', Expression),
        to: new DataTypeExpr({ this: DataTypeExprKind.TIMESTAMP_NS }),
      });

      return self.sql(
        self.func('MAKE_TIMESTAMP_NS', [
          new AddExpr({
            this: self.func('EPOCH_NS', [timestampNs]),
            expression: narrowInstanceOf(intervalValue, 'string', Expression),
          }),
        ]),
      );
    }

    if (!intervalValue || intervalValue instanceof IntervalExpr) {
      return baseImpl(self, expression);
    }

    if (isType(intervalValue, DataTypeExpr.REAL_TYPES)) {
      expression.setArgKey('expression', new CastExpr({
        this: self.func('ROUND', [intervalValue]),
        to: new DataTypeExpr({ this: DataTypeExprKind.INT }),
      }));
    }

    return baseImpl(self, expression);
  };
}

/** Transpile ARRAY_INSERT using LIST_CONCAT and DuckDB 1-based slicing */
function arrayInsertSql (self: Generator, expression: ArrayInsertExpr): string {
  const thisNode = expression.args.this;
  const position = expression.args.position;
  const element = expression.args.expression;
  const elementArray = new ArrayExpr({ expressions: [element ?? null_()] });
  const indexOffset = expression.args.offset ?? 0;

  if (!(position instanceof LiteralExpr && position.isNumber)) {
    self.unsupported('ARRAY_INSERT can only be transpiled with a literal position');
    return self.func('ARRAY_INSERT', [
      thisNode,
      position,
      element,
    ]);
  }

  let posValue = parseInt(position.args.this ?? '0');

  // Normalize 1-based to 0-based
  if (0 < posValue) posValue -= indexOffset;
  else if (posValue < 0) posValue += indexOffset;

  let concatExprs: Expression[];

  if (posValue === 0) {
    concatExprs = [elementArray, thisNode ?? null_()];
  } else if (0 < posValue) {
    const sliceStart = new BracketExpr({
      this: thisNode,
      expressions: [
        new SliceExpr({
          this: LiteralExpr.number(1),
          expression: LiteralExpr.number(posValue),
        }),
      ],
    });
    const sliceEnd = new BracketExpr({
      this: thisNode,
      expressions: [new SliceExpr({ this: LiteralExpr.number(posValue + 1) })],
    });
    concatExprs = [
      sliceStart,
      elementArray,
      sliceEnd,
    ];
  } else {
    const arrLen = new LengthExpr({ this: thisNode });
    const sliceEndPos = new AddExpr({
      this: arrLen,
      expression: LiteralExpr.number(posValue),
    });
    const sliceStartPos = new AddExpr({
      this: sliceEndPos,
      expression: LiteralExpr.number(1),
    });

    const sliceStart = new BracketExpr({
      this: thisNode,
      expressions: [
        new SliceExpr({
          this: LiteralExpr.number(1),
          expression: sliceEndPos,
        }),
      ],
    });
    const sliceEnd = new BracketExpr({
      this: thisNode,
      expressions: [new SliceExpr({ this: sliceStartPos })],
    });
    concatExprs = [
      sliceStart,
      elementArray,
      sliceEnd,
    ];
  }

  return self.sql(
    new IfExpr({
      this: new IsExpr({
        this: thisNode,
        expression: null_(),
      }),
      true: null_(),
      false: self.func('LIST_CONCAT', concatExprs),
    }),
  );
}

/** Transpile ARRAY_REMOVE_AT using LIST_CONCAT and DuckDB 1-based slicing */
function arrayRemoveAtSql (self: Generator, expression: ArrayRemoveAtExpr): string {
  const thisNode = expression.args.this;
  const position = expression.args.position;

  if (!(position instanceof LiteralExpr && position.isNumber)) {
    self.unsupported('ARRAY_REMOVE_AT can only be transpiled with a literal position');
    return self.func('ARRAY_REMOVE_AT', [thisNode, position]);
  }

  const posValue = parseInt(position.args.this ?? '0');
  let resultExpr: string | Expression;

  if (posValue === 0) {
    resultExpr = new BracketExpr({
      this: thisNode,
      expressions: [new SliceExpr({ this: LiteralExpr.number(2) })],
    });
  } else if (0 < posValue) {
    const leftSlice = new BracketExpr({
      this: thisNode,
      expressions: [
        new SliceExpr({
          this: LiteralExpr.number(1),
          expression: LiteralExpr.number(posValue),
        }),
      ],
    });
    const rightSlice = new BracketExpr({
      this: thisNode,
      expressions: [new SliceExpr({ this: LiteralExpr.number(posValue + 2) })],
    });
    resultExpr = self.func('LIST_CONCAT', [leftSlice, rightSlice]);
  } else if (posValue === -1) {
    const arrLen = new LengthExpr({ this: thisNode });
    const sliceEnd = new AddExpr({
      this: arrLen,
      expression: LiteralExpr.number(-1),
    });
    resultExpr = new BracketExpr({
      this: thisNode,
      expressions: [
        new SliceExpr({
          this: LiteralExpr.number(1),
          expression: sliceEnd,
        }),
      ],
    });
  } else {
    const arrLen = new LengthExpr({ this: thisNode });
    const sliceEndPos = new AddExpr({
      this: arrLen,
      expression: LiteralExpr.number(posValue),
    });
    const sliceStartPos = new AddExpr({
      this: sliceEndPos,
      expression: LiteralExpr.number(2),
    });

    const leftSlice = new BracketExpr({
      this: thisNode,
      expressions: [
        new SliceExpr({
          this: LiteralExpr.number(1),
          expression: sliceEndPos,
        }),
      ],
    });
    const rightSlice = new BracketExpr({
      this: thisNode,
      expressions: [new SliceExpr({ this: sliceStartPos })],
    });
    resultExpr = self.func('LIST_CONCAT', [leftSlice, rightSlice]);
  }

  return self.sql(
    new IfExpr({
      this: new IsExpr({
        this: thisNode,
        expression: null_(),
      }),
      true: null_(),
      false: resultExpr,
    }),
  );
}

function arraySortSql (self: Generator, expression: ArraySortExpr): string {
  if (expression.args.expression) {
    self.unsupported('DuckDB\'s ARRAY_SORT does not support a comparator.');
  }
  return self.func('ARRAY_SORT', [expression.args.this]);
}

function sortArraySql (self: Generator, expression: SortArrayExpr): string {
  const name = expression.args.asc instanceof BooleanExpr && !(expression.args.asc as BooleanExpr).args.this
    ? 'ARRAY_REVERSE_SORT'
    : 'ARRAY_SORT';
  return self.func(name, [expression.args.this]);
}

function buildSortArrayDesc (args: Expression[]): SortArrayExpr {
  return new SortArrayExpr({
    this: seqGet(args, 0),
    asc: new BooleanExpr({ this: false }),
  });
}

function buildArrayPrepend (args: Expression[]): ArrayPrependExpr {
  return new ArrayPrependExpr({
    this: seqGet(args, 1),
    expression: seqGet(args, 0),
  });
}

function buildDateDiff (args: Expression[]): DateDiffExpr {
  return new DateDiffExpr({
    this: seqGet(args, 2),
    expression: seqGet(args, 1),
    unit: seqGet(args, 0),
  });
}

function buildGenerateSeries (options: { endExclusive?: boolean } = {}): (args: Expression[]) => GenerateSeriesExpr {
  const {
    endExclusive = false,
  } = options;
  return (args: Expression[]): GenerateSeriesExpr => {
    if (args.length === 1) {
      args.unshift(LiteralExpr.number('0'));
    }

    const genSeries = GenerateSeriesExpr.fromArgList(args);
    genSeries.setArgKey('isEndExclusive', endExclusive);

    return genSeries;
  };
}

function buildMakeTimestamp (args: Expression[]): Expression {
  if (args.length === 1) {
    return new UnixToTimeExpr({
      this: seqGet(args, 0),
      scale: UnixToTimeExpr.MICROS,
    });
  }

  return new TimestampFromPartsExpr({
    year: seqGet(args, 0),
    month: seqGet(args, 1),
    day: seqGet(args, 2),
    hour: seqGet(args, 3),
    min: seqGet(args, 4),
    sec: seqGet(args, 5),
  });
}

function showParser (...args: (Expression | string)[]): (self: Parser) => ShowExpr {
  return (self: Parser) => (self as DuckDBParser).parseShowDuckdb(args[0] as string);
}

function structSql (self: Generator, expression: StructExpr): string {
  let ancestorCast = expression.findAncestor<CastExpr | SelectExpr>(CastExpr, SelectExpr);
  ancestorCast = ancestorCast instanceof SelectExpr ? undefined : ancestorCast;

  // Empty struct cast works with MAP() since DuckDB can't parse {}
  if (!expression.args.expressions || expression.args.expressions.length === 0) {
    if (ancestorCast instanceof CastExpr && isType(ancestorCast.args.to, DataTypeExprKind.MAP)) {
      return 'MAP()';
    }
  }

  const structArgs: string[] = [];

  const isBqInlineStruct = (
    !expression.find(PropertyEqExpr)
    && ancestorCast instanceof CastExpr
    && [...ancestorCast.findAll(DataTypeExpr)].some((castedType) => isType(castedType, DataTypeExprKind.STRUCT))
  );

  expression.args.expressions?.forEach((expr, i) => {
    const isPropertyEq = expr instanceof PropertyEqExpr;
    const thisNode = expr.args.this;
    const value = isPropertyEq ? expr.args.expression : expr;

    if (isBqInlineStruct) {
      structArgs.push(self.sql(value));
    } else {
      let key: string;
      if (thisNode instanceof IdentifierExpr) {
        key = self.sql(LiteralExpr.string(expr.name));
      } else if (isPropertyEq) {
        key = self.sql((expr as PropertyEqExpr).args.this);
      } else {
        key = self.sql(LiteralExpr.string(`_${i}`));
      }

      structArgs.push(`${key}: ${self.sql(value)}`);
    }
  });

  const csvArgs = structArgs.join(', ');
  return isBqInlineStruct ? `ROW(${csvArgs})` : `{${csvArgs}}`;
}

function dataTypeSql (self: Generator, expression: DataTypeExpr): string {
  if (isType(expression, 'array')) {
    const base = self.expressions(expression, { flat: true });
    const values = self.expressions(expression, {
      key: 'values',
      flat: true,
    });
    return `${base}[${values}]`;
  }

  if (isType(expression, [
    DataTypeExprKind.TIME,
    DataTypeExprKind.TIMETZ,
    DataTypeExprKind.TIMESTAMPTZ,
  ])) {
    return expression.args.this instanceof Expression ? expression.args.this.name : (expression.args.this ?? '');
  }

  return Generator.prototype.dataTypeSql.call(self, expression);
}

function jsonFormatSql (self: Generator, expression: JsonFormatExpr): string {
  const sql = self.func('TO_JSON', [expression.args.this, expression.args.options]);
  return `CAST(${sql} AS TEXT)`;
}

/**
 * Transpile Snowflake SEQ1/SEQ2/SEQ4/SEQ8 to DuckDB.
 * Generates monotonically increasing integers starting from 0.
 */
function seqSql (self: Generator, expression: FuncExpr, byteWidth: number): string {
  const ancestor = expression.findAncestor(...SEQ_RESTRICTED);
  if (
    ancestor
    && (!(ancestor instanceof OrderExpr || ancestor instanceof SelectExpr)
      || (ancestor instanceof OrderExpr && ancestor.parent instanceof WindowExpr))
  ) {
    self.unsupported('SEQ in restricted context is not supported - use CTE or subquery');
  }

  const bits = byteWidth * 8;
  const maxVal = LiteralExpr.number(Math.pow(2, bits));

  let result: Expression;
  if (expression.name === '1') {
    const half = LiteralExpr.number(Math.pow(2, bits - 1));
    result = replacePlaceholders((self._constructor as typeof DuckDBGenerator).SEQ_SIGNED.copy(), {
      maxVal: maxVal,
      half: half,
    });
  } else {
    result = replacePlaceholders((self._constructor as typeof DuckDBGenerator).SEQ_UNSIGNED.copy(), { maxVal: maxVal });
  }

  return self.sql(result);
}

/** Transpile UNIX timestamps to DuckDB timestamps with scale handling. */
function unixToTimeSql (self: Generator, expression: UnixToTimeExpr): string {
  const scale = expression.args.scale;
  let timestamp = expression.args.this;
  const targetType = expression.args.targetType;

  const isNtz = isType(targetType, [DataTypeExprKind.TIMESTAMP, DataTypeExprKind.TIMESTAMPNTZ]);

  if (scale === UnixToTimeExpr.MILLIS) {
    return self.func('EPOCH_MS', [timestamp]);
  }
  if (scale === UnixToTimeExpr.MICROS) {
    return self.func('MAKE_TIMESTAMP', [timestamp]);
  }

  if (scale !== undefined && scale !== UnixToTimeExpr.SECONDS) {
    timestamp = new DivExpr({
      this: timestamp,
      expression: self.func('POW', [LiteralExpr.number(10), scale]),
    });
  }

  let toTimestamp: Expression = new AnonymousExpr({
    this: 'TO_TIMESTAMP',
    expressions: [timestamp ?? null_()],
  });

  if (isNtz) {
    toTimestamp = new AtTimeZoneExpr({
      this: toTimestamp,
      zone: LiteralExpr.string('UTC'),
    });
  }

  return self.sql(toTimestamp);
}

const WRAPPED_JSON_EXTRACT_EXPRESSIONS = [
  BinaryExpr,
  BracketExpr,
  InExpr,
  NotExpr,
];

/** Wraps arrow JSON extract in parens if required by parent precedence. */
function arrowJsonExtractSqlDuckDB (self: Generator, expression: JsonExtractType): string {
  let arrowSql = arrowJsonExtractSql(self, expression);
  if (
    !expression.sameParent
    && WRAPPED_JSON_EXTRACT_EXPRESSIONS.some((cls) => expression.parent instanceof cls)
  ) {
    arrowSql = self.wrap(arrowSql);
  }
  return arrowSql;
}

/** Infers and applies casts for string literals used in datetime functions. */
function implicitDatetimeCast (
  arg: Expression | undefined,
  type: DataTypeExprKind = DataTypeExprKind.DATE,
): Expression | undefined {
  if (arg instanceof LiteralExpr && arg.isString) {
    const ts = arg.args.this ?? '';
    let targetType = type;
    if (type === DataTypeExprKind.DATE && ts.includes(':')) {
      targetType = TIMEZONE_PATTERN.test(ts) ? DataTypeExprKind.TIMESTAMPTZ : DataTypeExprKind.TIMESTAMP;
    }

    return new CastExpr({
      this: arg,
      to: new DataTypeExpr({ this: targetType }),
    });
  }

  return arg;
}

/** Compute Monday-based day shift for WEEK units. */
function weekUnitToDow (unit: Expression | undefined): number | undefined {
  if (unit instanceof VarExpr && unit.name.toUpperCase() === 'ISOWEEK') {
    return 1;
  }

  if (unit instanceof WeekStartExpr) {
    return WEEK_START_DAY_TO_DOW[unit.name.toUpperCase()];
  }

  return undefined;
}

/** Custom DATE_TRUNC logic for non-Monday week starts. */
function buildWeekTruncExpression (dateExpr: Expression, startDow: number): Expression {
  const shiftDays = startDow === 7 ? 1 : 1 - startDow;

  const shiftedDate =
    shiftDays !== 0
      ? new DateAddExpr({
        this: dateExpr,
        expression: new IntervalExpr({
          this: LiteralExpr.string(shiftDays.toString()),
          unit: new VarExpr({ this: 'DAY' }),
        }),
      })
      : dateExpr;

  return new DateTruncExpr({
    unit: new VarExpr({ this: 'WEEK' }),
    this: shiftedDate,
  });
}

/** Transpile DATE_DIFF with boundary-aware week logic. */
function dateDiffSql (self: Generator, expression: DateDiffExpr): string {
  const unit = expression.args.unit;

  if (isNanosecondUnit(unit)) {
    return handleNanosecondDiff(self, expression.args.this ?? null_(), expression.args.expression ?? null_());
  }

  let thisNode = implicitDatetimeCast(expression.args.this);
  let exprNode = implicitDatetimeCast(expression.args.expression);

  const datePartBoundary = expression.args.datePartBoundary;
  const weekStart = weekUnitToDow(unit);

  if (datePartBoundary && weekStart !== undefined && thisNode && exprNode) {
    expression.setArgKey('unit', LiteralExpr.string('WEEK'));

    thisNode = buildWeekTruncExpression(thisNode, weekStart);
    exprNode = buildWeekTruncExpression(exprNode, weekStart);
  }

  return self.func('DATE_DIFF', [
    unitToStr(expression),
    exprNode,
    thisNode,
  ]);
}

/** BQ's GENERATE_DATE_ARRAY & GENERATE_TIMESTAMP_ARRAY transformed to DuckDB'S GENERATE_SERIES */
function generateDatetimeArraySql (
  self: Generator,
  expression: GenerateDateArrayExpr | GenerateTimestampArrayExpr,
): string {
  const isGenerateDateArray = expression instanceof GenerateDateArrayExpr;

  const type = isGenerateDateArray ? DataTypeExprKind.DATE : DataTypeExprKind.TIMESTAMP;
  const start = implicitDatetimeCast(expression.args.start, type);
  const end = implicitDatetimeCast(expression.args.end, type);

  let genSeries: Expression = new GenerateSeriesExpr({
    start: start,
    end: end,
    step: expression.args.step,
  });

  if (isGenerateDateArray) {
    // Cast TIMESTAMP array result back to DATE array for BQ semantics
    genSeries = new CastExpr({
      this: genSeries,
      to: DataTypeExpr.build('ARRAY<DATE>'),
    });
  }

  return self.sql(genSeries);
}

function jsonExtractValueArraySql (
  self: Generator,
  expression: JsonValueArrayExpr | JsonExtractArrayExpr,
): string {
  const jsonExtractExpr = expression.args.expression instanceof Expression ? expression.args.expression : undefined;
  const jsonExtract = new JsonExtractExpr({
    this: expression.args.this,
    expression: jsonExtractExpr,
  });
  const dataType = expression instanceof JsonValueArrayExpr ? 'ARRAY<STRING>' : 'ARRAY<JSON>';
  return self.sql(new CastExpr({
    this: jsonExtract,
    to: DataTypeExpr.build(dataType),
  }));
}

function castToVarchar (arg: ExpressionValue | undefined): Expression | undefined {
  if (arg === undefined) {
    return undefined;
  }
  if (arg instanceof Expression && arg.type && !isType(arg, [...DataTypeExpr.TEXT_TYPES, DataTypeExprKind.UNKNOWN])) {
    return new CastExpr({
      this: arg,
      to: new DataTypeExpr({ this: DataTypeExprKind.VARCHAR }),
    });
  }
  if (arg instanceof Expression) {
    return arg;
  }
  return LiteralExpr.string(arg.toString());
}

function castToBoolean (arg: Expression | undefined): Expression | undefined {
  if (arg && !isType(arg, DataTypeExprKind.BOOLEAN)) {
    return new CastExpr({
      this: arg,
      to: new DataTypeExpr({ this: DataTypeExprKind.BOOLEAN }),
    });
  }
  return arg;
}

function isBinary (arg: unknown): boolean {
  return isType(arg, [
    DataTypeExprKind.BINARY,
    DataTypeExprKind.VARBINARY,
    DataTypeExprKind.BLOB,
  ]);
}

function genWithCastToBlob (self: Generator, expression: Expression, resultSql: string): string {
  if (isBinary(expression)) {
    const blob = DataTypeExpr.build('BLOB', { dialect: 'duckdb' });
    resultSql = self.sql(new CastExpr({
      this: resultSql,
      to: blob,
    }));
  }
  return resultSql;
}

function castToBit (arg: Expression): Expression {
  if (!isBinary(arg)) return arg;

  let node = arg;
  if (arg instanceof HexStringExpr) {
    node = new UnhexExpr({ this: LiteralExpr.string(arg.args.this) });
  }

  return new CastExpr({
    this: node,
    to: new DataTypeExpr({ this: DataTypeExprKind.BIT }),
  });
}

function prepareBinaryBitwiseArgs (expression: BinaryExpr): void {
  if (expression.args.this instanceof Expression && isBinary(expression.args.this)) {
    expression.setArgKey('this', castToBit(expression.args.this));
  }
  if (expression.args.expression instanceof Expression && isBinary(expression.args.expression)) {
    expression.setArgKey('expression', castToBit(expression.args.expression));
  }
}

/** Transpile Snowflake's NEXT_DAY / PREVIOUS_DAY using formulas and ISODOW */
function dayNavigationSql (self: Generator, expression: NextDayExpr | PreviousDayExpr): string {
  const dateExpr = expression.args.this;
  const dayNameExpr = expression.args.expression;
  const isodowCall = self.func('ISODOW', [dateExpr]);

  let targetDow: Expression;

  if (dayNameExpr instanceof LiteralExpr) {
    const dayNameStr = (dayNameExpr.args.this ?? '').toUpperCase();
    const matchingDay = Object.keys(WEEK_START_DAY_TO_DOW).find((day) =>
      day.startsWith(dayNameStr));

    if (matchingDay) {
      targetDow = LiteralExpr.number(WEEK_START_DAY_TO_DOW[matchingDay]);
    } else {
      return self.functionFallbackSql(expression);
    }
  } else {
    const upperDayName = new UpperExpr({ this: dayNameExpr });
    targetDow = new CaseExpr({
      ifs: Object.entries(WEEK_START_DAY_TO_DOW).map(
        ([day, dowNum]) =>
          new IfExpr({
            this: self.func('STARTS_WITH', [upperDayName.copy(), LiteralExpr.string(day.substring(0, 2))]),
            true: LiteralExpr.number(dowNum),
          }),
      ),
    });
  }

  let dateWithOffset: Expression;
  if (expression instanceof NextDayExpr) {
    // (target_dow - current_dow + 6) % 7 + 1
    const daysOffset = new ModExpr({
      this: new ParenExpr({
        this: new AddExpr({
          this: new SubExpr({
            this: targetDow,
            expression: isodowCall,
          }),
          expression: LiteralExpr.number(6),
        }),
      }),
      expression: LiteralExpr.number(7),
    }).add(1);
    dateWithOffset = new AddExpr({
      this: dateExpr,
      expression: new IntervalExpr({
        this: daysOffset,
        unit: new VarExpr({ this: 'DAY' }),
      }),
    });
  } else {
    // (current_dow - target_dow + 6) % 7 + 1
    const daysOffset = new ModExpr({
      this: new ParenExpr({
        this: new AddExpr({
          this: new SubExpr({
            this: isodowCall,
            expression: targetDow,
          }),
          expression: LiteralExpr.number(6),
        }),
      }),
      expression: LiteralExpr.number(7),
    }).add(1);
    dateWithOffset = new SubExpr({
      this: dateExpr,
      expression: new IntervalExpr({
        this: daysOffset,
        unit: new VarExpr({ this: 'DAY' }),
      }),
    });
  }

  return self.sql(new CastExpr({
    this: dateWithOffset,
    to: new DataTypeExpr({ this: DataTypeExprKind.DATE }),
  }));
}

function anyValueSql (self: Generator, expression: AnyValueExpr): string {
  const having = expression.args.this;
  if (having instanceof HavingMaxExpr) {
    const funcName = having.args.max ? 'ARG_MAX_NULL' : 'ARG_MIN_NULL';
    return self.func(funcName, [having.args.this, having.args.expression]);
  }
  return self.functionFallbackSql(expression);
}

/** Cast non-integer types to INT for bitwise aggregates */
function bitwiseAggSql (
  self: Generator,
  expression: BitwiseOrAggExpr | BitwiseAndAggExpr | BitwiseXorAggExpr,
): string {
  let funcName = 'BIT_XOR';
  if (expression instanceof BitwiseOrAggExpr) funcName = 'BIT_OR';
  else if (expression instanceof BitwiseAndAggExpr) funcName = 'BIT_AND';

  let arg = expression.args.this;

  if (isType(arg, [...DataTypeExpr.REAL_TYPES, ...DataTypeExpr.TEXT_TYPES])) {
    if (isType(arg, DataTypeExpr.FLOAT_TYPES)) {
      arg = self.func('ROUND', [arg]);
    }
    arg = new CastExpr({
      this: arg,
      to: new DataTypeExpr({ this: DataTypeExprKind.INT }),
    });
  }

  return self.func(funcName, [arg]);
}

function literalSqlWithWsChr (self: Generator, literal: string): string {
  /**
   * DuckDB does not support \uXXXX escapes.
   * Uses CHR() for control characters in WS_CONTROL_CHARS_TO_DUCK.
   */
  const chars = literal.split('');
  if (!chars.some((ch) => ch in WS_CONTROL_CHARS_TO_DUCK)) {
    return self.sql(LiteralExpr.string(literal));
  }

  const sqlSegments: string[] = [];
  let currentGroup: string[] = [];
  let isWsGroup = false;

  for (const ch of chars) {
    const isWs = ch in WS_CONTROL_CHARS_TO_DUCK;
    if (isWs !== isWsGroup) {
      if (0 < currentGroup.length) {
        if (isWsGroup) {
          currentGroup.forEach((c) =>
            sqlSegments.push(self.func('CHR', [LiteralExpr.number(WS_CONTROL_CHARS_TO_DUCK[c])])));
        } else {
          sqlSegments.push(self.sql(LiteralExpr.string(currentGroup.join(''))));
        }
      }
      currentGroup = [];
      isWsGroup = isWs;
    }
    currentGroup.push(ch);
  }

  // Handle final group
  if (0 < currentGroup.length) {
    if (isWsGroup) {
      currentGroup.forEach((c) =>
        sqlSegments.push(self.func('CHR', [LiteralExpr.number(WS_CONTROL_CHARS_TO_DUCK[c])])));
    } else {
      sqlSegments.push(self.sql(LiteralExpr.string(currentGroup.join(''))));
    }
  }

  const sql = sqlSegments.join(' || ');
  return sqlSegments.length === 1 ? sql : `(${sql})`;
}

function escapeRegexMetachars (
  self: Generator,
  delimiters: Expression | undefined,
  delimitersSql: string,
): string {
  /** Escapes regex metachars \ - ^ [ ] for INITCAP character classes. */
  if (!delimiters) return delimitersSql;

  if (delimiters instanceof LiteralExpr && delimiters.isString) {
    const literalValue = delimiters.args.this ?? '';
    const escapedLiteral = literalValue
      .split('')
      .map((ch) => REGEX_ESCAPE_REPLACEMENTS[ch] ?? ch)
      .join('');
    return literalSqlWithWsChr(self, escapedLiteral);
  }

  let escapedSql = delimitersSql;
  Object.entries(REGEX_ESCAPE_REPLACEMENTS).forEach(([raw, escaped]) => {
    escapedSql = self.func('REPLACE', [
      escapedSql,
      self.sql(LiteralExpr.string(raw)),
      self.sql(LiteralExpr.string(escaped)),
    ]);
  });

  return escapedSql;
}

function buildCapitalizationSql (self: Generator, valueToSplit: string, delimitersSql: string): string {
  /** Implements custom INITCAP logic using REGEXP_EXTRACT_ALL and LIST_TRANSFORM. */
  if (delimitersSql === '\'\'') {
    return `UPPER(LEFT(${valueToSplit}, 1)) || LOWER(SUBSTRING(${valueToSplit}, 2))`;
  }

  const delimRegexSql = `CONCAT('[', ${delimitersSql}, ']')`;
  const splitRegexSql = `CONCAT('([', ${delimitersSql}, ']+|[^', ${delimitersSql}, ']+)')`;

  return self.func('ARRAY_TO_STRING', [
    new CaseExpr({})
      .when(
        `REGEXP_MATCHES(LEFT(${valueToSplit}, 1), ${delimRegexSql})`,
        self.func('LIST_TRANSFORM', [self.func('REGEXP_EXTRACT_ALL', [valueToSplit, splitRegexSql]), '(seg, idx) -> CASE WHEN idx % 2 = 0 THEN UPPER(LEFT(seg, 1)) || LOWER(SUBSTRING(seg, 2)) ELSE seg END']),
      )
      .else(
        self.func('LIST_TRANSFORM', [self.func('REGEXP_EXTRACT_ALL', [valueToSplit, splitRegexSql]), '(seg, idx) -> CASE WHEN idx % 2 = 1 THEN UPPER(LEFT(seg, 1)) || LOWER(SUBSTRING(seg, 2)) ELSE seg END']),
      ),
    '\'\'',
  ]);
}

function initcapSql (self: Generator, expression: InitcapExpr): string {
  const thisSql = self.sql(expression, 'this');
  let delimiters = expression.args.expression;
  if (!delimiters) {
    delimiters = LiteralExpr.string(self.dialect._constructor.INITCAP_DEFAULT_DELIMITER_CHARS);
  }
  const delimitersSql = self.sql(delimiters);
  const escapedDelimitersSql = escapeRegexMetachars(self, delimiters, delimitersSql);

  return buildCapitalizationSql(self, thisSql, escapedDelimitersSql);
}

function boolxorAggSql (self: Generator, expression: BoolxorAggExpr): string {
  /** Mimics Snowflake BOOLXOR_AGG by generating COUNT_IF(col) = 1. */
  return self.sql(
    new EqExpr({
      this: castToBoolean(expression.args.this as Expression) ?? expression.args.this,
      expression: LiteralExpr.number(1),
    }),
  );
}

function bitshiftSql (self: Generator, expression: BitwiseLeftShiftExpr | BitwiseRightShiftExpr): string {
  /** Transforms bitshift for DuckDB injecting BIT/INT128 casts and fixing precedence. */
  const operator = expression instanceof BitwiseLeftShiftExpr ? '<<' : '>>';
  let resultIsBlob = false;
  const thisNode = expression.args.this;

  if (thisNode instanceof Expression && isBinary(thisNode)) {
    resultIsBlob = true;
    expression.setArgKey('this', new CastExpr({
      this: thisNode,
      to: new DataTypeExpr({ this: DataTypeExprKind.BIT }),
    }));
  } else if (expression.args.requiresInt128 && thisNode instanceof Expression) {
    thisNode.replace(new CastExpr({
      this: thisNode,
      to: new DataTypeExpr({ this: DataTypeExprKind.INT128 }),
    }));
  }

  let resultSql = self.binary(expression, operator);

  if (expression.parent instanceof BinaryExpr) {
    resultSql = `(${resultSql})`;
  }

  if (resultIsBlob) {
    resultSql = self.sql(new CastExpr({
      this: resultSql,
      to: DataTypeExpr.build('BLOB', { dialect: 'duckdb' }),
    }));
  }

  return resultSql;
}

function scaleRoundingSql (
  self: Generator,
  expression: CeilExpr | FloorExpr | RoundExpr | TruncExpr,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  RoundingFuncClass: new (args: any) => Expression,
): string | undefined {
  /** Handle scale parameter: FUNC(x, n) -> ROUND(FUNC(x * 10^n) / 10^n, n) */
  const decimals = expression.args.decimals;

  if (!decimals || expression.args.to !== undefined) {
    return undefined;
  }

  let thisNode = expression.args.this;
  if (thisNode instanceof BinaryExpr) {
    thisNode = new ParenExpr({ this: thisNode });
  }

  const nInt = decimals instanceof LiteralExpr && (decimals.isNumber || decimals.isInteger)
    ? decimals
    : new CastExpr({
      this: decimals,
      to: new DataTypeExpr({ this: DataTypeExprKind.INT }),
    });

  const pow = new PowExpr({
    this: LiteralExpr.number('10'),
    expression: nInt,
  });
  const rounded = new RoundingFuncClass({
    this: new MulExpr({
      this: thisNode,
      expression: pow,
    }),
  });
  const result = new DivExpr({
    this: rounded,
    expression: pow.copy(),
  });

  return (self as DuckDBGenerator).roundSql(
    new RoundExpr({
      this: result,
      decimals,
      castsNonIntegerDecimals: true,
    }),
  );
}

function ceilFloor (self: Generator, expression: FloorExpr | CeilExpr): string {
  const scaledSql = scaleRoundingSql(self, expression, expression._constructor);
  if (scaledSql !== undefined) {
    return scaledSql;
  }
  return self.ceilFloor(expression);
}

/**
 * Transpile Snowflake's REGR_VALX/REGR_VALY to DuckDB equivalent.
 * REGR_VALX(y, x) returns NULL if y is NULL; otherwise returns x.
 */
function regrValSql (self: Generator, expression: RegrValxExpr | RegrValyExpr): string {
  const y = expression.args.this ?? null_();
  const x = expression.args.expression ?? null_();

  let checkForNull: Expression;
  let returnValue: Expression;
  let returnValueAttr: string;

  if (expression instanceof RegrValxExpr) {
    checkForNull = y;
    returnValue = x;
    returnValueAttr = 'expression';
  } else {
    checkForNull = x;
    returnValue = y;
    returnValueAttr = 'this';
  }

  let resultType = returnValue.type;

  // Infer types if unknown
  if (!resultType || typeof resultType === 'string' || resultType.args.this === DataTypeExprKind.UNKNOWN) {
    try {
      const annotated = annotateTypes(expression.copy(), { dialect: self.dialect });
      const annotatedArg = annotated.args[returnValueAttr as keyof typeof annotated.args];
      resultType = annotatedArg instanceof Expression ? annotatedArg.type : undefined;
    } catch {
      // ignore
    }
  }

  // Default to DOUBLE for regression if still unknown
  if (!resultType || typeof resultType === 'string' || resultType.args.this === DataTypeExprKind.UNKNOWN) {
    resultType = DataTypeExpr.build('DOUBLE');
  }

  const typedNull = new CastExpr({
    this: null_(),
    to: resultType,
  });

  return self.sql(
    new IfExpr({
      this: new IsExpr({
        this: checkForNull.copy(),
        expression: null_(),
      }),
      true: typedNull,
      false: returnValue.copy(),
    }),
  );
}

function maybeCorrNullToFalse (
  expression: FilterExpr | WindowExpr | CorrExpr,
): FilterExpr | WindowExpr | CorrExpr | undefined {
  let corr: Expression = expression;
  while (corr instanceof WindowExpr || corr instanceof FilterExpr) {
    corr = corr.args.this ?? null_();
  }

  if (!(corr instanceof CorrExpr) || !corr.args.nullOnZeroVariance) {
    return undefined;
  }

  corr.setArgKey('nullOnZeroVariance', false);
  return expression;
}

function dateFromPartsSql (self: Generator, expression: DateFromPartsExpr): string {
  /**
   * DuckDB's MAKE_DATE doesn't support overflow.
   * Formula: MAKE_DATE(year, 1, 1) + (month-1) MONTHS + (day-1) DAYS
   */
  const yearExpr = expression.args.year;
  const monthExpr = expression.args.month;
  const dayExpr = expression.args.day;

  if (expression.args.allowOverflow) {
    let baseDate: Expression = new AnonymousExpr({
      this: 'MAKE_DATE',
      expressions: [
        yearExpr ?? null_(),
        LiteralExpr.number(1),
        LiteralExpr.number(1),
      ],
    });

    if (monthExpr) {
      baseDate = new AddExpr({
        this: baseDate,
        expression: new IntervalExpr({
          this: new SubExpr({
            this: monthExpr,
            expression: LiteralExpr.number(1),
          }),
          unit: new VarExpr({ this: 'MONTH' }),
        }),
      });
    }

    if (dayExpr) {
      baseDate = new AddExpr({
        this: baseDate,
        expression: new IntervalExpr({
          this: new SubExpr({
            this: dayExpr,
            expression: LiteralExpr.number(1),
          }),
          unit: new VarExpr({ this: 'DAY' }),
        }),
      });
    }

    return self.sql(new CastExpr({
      this: baseDate,
      to: new DataTypeExpr({ this: DataTypeExprKind.DATE }),
    }));
  }

  return self.func('MAKE_DATE', [
    yearExpr,
    monthExpr,
    dayExpr,
  ]);
}

function roundArg (arg: Expression, roundInput?: Expression | boolean): Expression {
  if (roundInput) {
    return new RoundExpr({
      this: arg,
      decimals: LiteralExpr.number(0),
    });
  }
  return arg;
}

function boolnotSql (self: Generator, expression: BoolnotExpr): string {
  const arg = roundArg(expression.args.this ?? null_(), expression.args.roundInput);
  return self.sql(new NotExpr({ this: new ParenExpr({ this: arg }) }));
}

function boolandSql (self: Generator, expression: BoolandExpr): string {
  const roundInput = expression.args.roundInput;
  const left = roundArg(expression.args.this ?? null_(), roundInput);
  const right = roundArg(expression.args.expression ?? null_(), roundInput);
  return self.sql(
    new ParenExpr({
      this: new AndExpr({
        this: new ParenExpr({ this: left }),
        expression: new ParenExpr({ this: right }),
      }),
    }),
  );
}

function boolorSql (self: Generator, expression: BoolorExpr): string {
  const roundInput = expression.args.roundInput;
  const left = roundArg(expression.args.this ?? null_(), roundInput);
  const right = roundArg(expression.args.expression ?? null_(), roundInput);
  return self.sql(
    new ParenExpr({
      this: new OrExpr({
        this: new ParenExpr({ this: left }),
        expression: new ParenExpr({ this: right }),
      }),
    }),
  );
}

function xorSql (self: Generator, expression: XorExpr): string {
  const roundInput = expression.args.roundInput;
  const left = expression.args.this && roundArg(expression.args.this, roundInput);
  const right = expression.args.expression && roundArg(expression.args.expression, roundInput);

  const leftNot = new NotExpr({ this: new ParenExpr({ this: left?.copy() }) });
  const rightNot = new NotExpr({ this: new ParenExpr({ this: right?.copy() }) });

  return self.sql(
    new OrExpr({
      this: new ParenExpr({
        this: new AndExpr({
          this: left?.copy(),
          expression: rightNot,
        }),
      }),
      expression: new ParenExpr({
        this: new AndExpr({
          this: leftNot,
          expression: right?.copy(),
        }),
      }),
    }),
  );
}

function shaSql (
  self: Generator,
  expression: ShaExpr,
  hashFunc: string,
  options: { isBinary?: boolean } = {},
): string {
  const {
    isBinary: _isBinary = false,
  } = options;
  let arg = expression.args.this;

  if (hashFunc === 'SHA256') {
    const length = expression.text('length') || '256';
    if (length !== '256') {
      self.unsupported('DuckDB only supports SHA256 hashing algorithm.');
    }
  }

  if (
    arg instanceof Expression
    && arg.type
    && (!(arg.type instanceof Expression) || arg.type.args.this !== DataTypeExprKind.UNKNOWN)
    && !arg.isType(DataTypeExpr.TEXT_TYPES)
    && !isBinary(arg)
  ) {
    arg = new CastExpr({
      this: arg,
      to: new DataTypeExpr({ this: DataTypeExprKind.VARCHAR }),
    });
  }

  const result = self.func(hashFunc, [arg]);
  return _isBinary ? self.func('UNHEX', [result]) : result;
}

class DuckDBTokenizer extends Tokenizer {
  static BYTE_STRINGS = [['e\'', '\''], ['E\'', '\'']] as TokenPair[];

  static BYTE_STRING_ESCAPES = ['\'', '\\'];
  static HEREDOC_STRINGS = ['$'];

  static HEREDOC_TAG_IS_IDENTIFIER = true;
  static HEREDOC_STRING_ALTERNATIVE = TokenType.PARAMETER;

  @cache
  static get ORIGINAL_KEYWORDS (): Record<string, TokenType> {
    const keywords: Record<string, TokenType> = {
      ...Tokenizer.KEYWORDS,
      '//': TokenType.DIV,
      '**': TokenType.DSTAR,
      '^@': TokenType.CARET_AT,
      '@>': TokenType.AT_GT,
      '<@': TokenType.LT_AT,
      'ATTACH': TokenType.ATTACH,
      'BINARY': TokenType.VARBINARY,
      'BITSTRING': TokenType.BIT,
      'BPCHAR': TokenType.TEXT,
      'CHAR': TokenType.TEXT,
      'DATETIME': TokenType.TIMESTAMPNTZ,
      'DETACH': TokenType.DETACH,
      'FORCE': TokenType.FORCE,
      'INSTALL': TokenType.INSTALL,
      'INT8': TokenType.BIGINT,
      'LOGICAL': TokenType.BOOLEAN,
      'MACRO': TokenType.FUNCTION,
      'ONLY': TokenType.ONLY,
      'PIVOT_WIDER': TokenType.PIVOT,
      'POSITIONAL': TokenType.POSITIONAL,
      'RESET': TokenType.COMMAND,
      'ROW': TokenType.STRUCT,
      'SIGNED': TokenType.INT,
      'STRING': TokenType.TEXT,
      'SUMMARIZE': TokenType.SUMMARIZE,
      'TIMESTAMP': TokenType.TIMESTAMPNTZ,
      'TIMESTAMP_S': TokenType.TIMESTAMP_S,
      'TIMESTAMP_MS': TokenType.TIMESTAMP_MS,
      'TIMESTAMP_NS': TokenType.TIMESTAMP_NS,
      'TIMESTAMP_US': TokenType.TIMESTAMP,
      'UBIGINT': TokenType.UBIGINT,
      'UINTEGER': TokenType.UINT,
      'USMALLINT': TokenType.USMALLINT,
      'UTINYINT': TokenType.UTINYINT,
      'VARCHAR': TokenType.TEXT,
    };
    delete keywords['/*+'];
    return keywords;
  }

  @cache
  static get SINGLE_TOKENS () {
    return {
      ...Tokenizer.SINGLE_TOKENS,
      $: TokenType.PARAMETER,
    };
  }

  @cache
  static get COMMANDS (): Set<TokenType> {
    const commands = new Set(Tokenizer.COMMANDS);
    commands.delete(TokenType.SHOW);
    return commands;
  }
}

class DuckDBParser extends Parser {
  static MAP_KEYS_ARE_ARBITRARY_EXPRESSIONS = true;

  @cache
  static get BITWISE (): Partial<Record<TokenType, typeof Expression>> {
    return (() => {
      const bitwise = { ...Parser.BITWISE };
      delete bitwise[TokenType.CARET];
      return bitwise;
    })();
  }

  @cache
  static get RANGE_PARSERS (): Partial<Record<TokenType, (self: Parser, this_: Expression) => Expression | undefined>> {
    return {
      ...Parser.RANGE_PARSERS,
      [TokenType.DAMP]: binaryRangeParser(ArrayOverlapsExpr),
      [TokenType.CARET_AT]: binaryRangeParser(StartsWithExpr),
      [TokenType.TILDE]: binaryRangeParser(RegexpFullMatchExpr),
    };
  }

  @cache
  static get EXPONENT (): Partial<Record<TokenType, typeof Expression>> {
    return {
      ...Parser.EXPONENT,
      [TokenType.CARET]: PowExpr,
      [TokenType.DSTAR]: PowExpr,
    };
  }

  @cache
  static get FUNCTIONS_WITH_ALIASED_ARGS (): Set<string> {
    return new Set([...Parser.FUNCTIONS_WITH_ALIASED_ARGS, 'STRUCT_PACK']);
  }

  static SHOW_PARSERS = {
    'TABLES': showParser('TABLES'),
    'ALL TABLES': showParser('ALL TABLES'),
  };

  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> {
    return (() => {
      const functions: Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> = {
        ...Parser.FUNCTIONS,
        ANY_VALUE: (args: Expression[]) => new IgnoreNullsExpr({ this: AnyValueExpr.fromArgList(args) }),
        ARRAY_PREPEND: buildArrayPrepend,
        ARRAY_REVERSE_SORT: buildSortArrayDesc,
        ARRAY_SORT: SortArrayExpr.fromArgList,
        BIT_AND: BitwiseAndAggExpr.fromArgList,
        BIT_OR: BitwiseOrAggExpr.fromArgList,
        BIT_XOR: BitwiseXorAggExpr.fromArgList,
        DATEDIFF: buildDateDiff,
        DATE_DIFF: buildDateDiff,
        DATE_TRUNC: (args: Expression[]) => dateTruncToTime([seqGet(args, 0), seqGet(args, 1)]),
        DATETRUNC: (args: Expression[]) => dateTruncToTime([seqGet(args, 0), seqGet(args, 1)]),
        DECODE: (args: Expression[]) => new DecodeExpr({
          this: seqGet(args, 0),
          charset: LiteralExpr.string('utf-8'),
        }),
        EDITDIST3: LevenshteinExpr.fromArgList,
        JARO_WINKLER_SIMILARITY: JarowinklerSimilarityExpr.fromArgList,
        ENCODE: (args: Expression[]) => new EncodeExpr({
          this: seqGet(args, 0),
          charset: LiteralExpr.string('utf-8'),
        }),
        EPOCH: TimeToUnixExpr.fromArgList,
        EPOCH_MS: (args: Expression[]) => new UnixToTimeExpr({
          this: seqGet(args, 0),
          scale: UnixToTimeExpr.MILLIS,
        }),
        GENERATE_SERIES: buildGenerateSeries(),
        GET_BIT: (args: Expression[]) => new GetbitExpr({
          this: seqGet(args, 0),
          expression: seqGet(args, 1),
          zeroIsMsb: true,
        }),
        JSON: ParseJsonExpr.fromArgList,
        JSON_EXTRACT_PATH: buildExtractJsonWithPath(JsonExtractExpr),
        JSON_EXTRACT_STRING: buildExtractJsonWithPath(JsonExtractScalarExpr),
        LIST_APPEND: ArrayAppendExpr.fromArgList,
        LIST_CONCAT: buildArrayConcat,
        LIST_CONTAINS: ArrayContainsExpr.fromArgList,
        LIST_COSINE_DISTANCE: CosineDistanceExpr.fromArgList,
        LIST_DISTANCE: EuclideanDistanceExpr.fromArgList,
        LIST_FILTER: ArrayFilterExpr.fromArgList,
        LIST_HAS: ArrayContainsExpr.fromArgList,
        LIST_HAS_ANY: ArrayOverlapsExpr.fromArgList,
        LIST_PREPEND: buildArrayPrepend,
        LIST_REVERSE_SORT: buildSortArrayDesc,
        LIST_SORT: SortArrayExpr.fromArgList,
        LIST_TRANSFORM: TransformExpr.fromArgList,
        LIST_VALUE: (args: Expression[]) => new ArrayExpr({ expressions: args }),
        MAKE_DATE: DateFromPartsExpr.fromArgList,
        MAKE_TIME: TimeFromPartsExpr.fromArgList,
        MAKE_TIMESTAMP: buildMakeTimestamp,
        QUANTILE_CONT: PercentileContExpr.fromArgList,
        QUANTILE_DISC: PercentileDiscExpr.fromArgList,
        RANGE: buildGenerateSeries({ endExclusive: true }),
        REGEXP_EXTRACT: buildRegexpExtract(RegexpExtractExpr),
        REGEXP_EXTRACT_ALL: buildRegexpExtract(RegexpExtractAllExpr),
        REGEXP_MATCHES: RegexpLikeExpr.fromArgList,
        REGEXP_REPLACE: (args: Expression[]) => new RegexpReplaceExpr({
          this: seqGet(args, 0),
          expression: seqGet(args, 1),
          replacement: seqGet(args, 2),
          modifiers: seqGet(args, 3),
          singleReplace: true,
        }),
        SHA256: (args: Expression[]) => new Sha2Expr({
          this: seqGet(args, 0),
          length: LiteralExpr.number(256),
        }),
        STRFTIME: buildFormattedTime(TimeToStrExpr, { dialect: 'duckdb' }),
        STRING_SPLIT: SplitExpr.fromArgList,
        STRING_SPLIT_REGEX: RegexpSplitExpr.fromArgList,
        STRING_TO_ARRAY: SplitExpr.fromArgList,
        STRPTIME: buildFormattedTime(StrToTimeExpr, { dialect: 'duckdb' }),
        STRUCT_PACK: StructExpr.fromArgList,
        STR_SPLIT: SplitExpr.fromArgList,
        STR_SPLIT_REGEX: RegexpSplitExpr.fromArgList,
        TIME_BUCKET: DateBinExpr.fromArgList,
        TO_TIMESTAMP: UnixToTimeExpr.fromArgList,
        UNNEST: ExplodeExpr.fromArgList,
        VERSION: CurrentVersionExpr.fromArgList,
        XOR: binaryFromFunction(BitwiseXorExpr),
      };

      delete functions['DATE_SUB'];
      delete functions['GLOB'];
      return functions;
    })();
  }

  @cache
  static get FUNCTION_PARSERS (): Partial<Record<string, (self: Parser) => Expression | undefined>> {
    return (() => {
      const parsers = {
        ...Parser.FUNCTION_PARSERS,
        ...Object.fromEntries(
          [
            'GROUP_CONCAT',
            'LISTAGG',
            'STRINGAGG',
          ].map((key) => [key, (self: Parser) => (self as DuckDBParser).parseStringAgg()]),
        ),
      };
      delete parsers['DECODE'];
      return parsers;
    })();
  }

  @cache
  static get NO_PAREN_FUNCTION_PARSERS (): Partial<Record<string, (self: Parser) => Expression | undefined>> {
    return {
      ...Parser.NO_PAREN_FUNCTION_PARSERS,
      'MAP': (self: Parser) => (self as DuckDBParser).parseMap(),
      '@': (self: Parser) => new AbsExpr({ this: (self as DuckDBParser).parseBitwise() }),
    };
  }

  @cache
  static get TABLE_ALIAS_TOKENS (): Set<TokenType> {
    return (() => {
      const tokens = new Set(Parser.TABLE_ALIAS_TOKENS);
      tokens.delete(TokenType.SEMI);
      tokens.delete(TokenType.ANTI);
      return tokens;
    })();
  }

  @cache
  static get PLACEHOLDER_PARSERS (): Partial<Record<TokenType, (self: Parser) => Expression | undefined>> {
    return {
      ...Parser.PLACEHOLDER_PARSERS,
      [TokenType.PARAMETER]: (self: Parser) => (
        (self as DuckDBParser).match(TokenType.NUMBER) || self.matchSet(self._constructor.ID_VAR_TOKENS)
          ? self.expression(PlaceholderExpr, { this: (self as DuckDBParser).prev?.text })
          : undefined
      ),
    };
  }

  @cache
  static get TYPE_CONVERTERS () {
    return {
      [DataTypeExprKind.DECIMAL]: buildDefaultDecimalType(18, 3),
      [DataTypeExprKind.TEXT]: () => DataTypeExpr.build('TEXT') ?? new DataTypeExpr({ this: DataTypeExprKind.TEXT }),
    };
  }

  @cache
  static get STATEMENT_PARSERS (): Partial<Record<TokenType, (self: Parser) => Expression | undefined>> {
    return {
      ...Parser.STATEMENT_PARSERS,
      [TokenType.ATTACH]: (self: Parser) => (self as DuckDBParser).parseAttachDetach(),
      [TokenType.DETACH]: (self: Parser) => (self as DuckDBParser).parseAttachDetach({ isAttach: false }),
      [TokenType.FORCE]: (self: Parser) => (self as DuckDBParser).parseForce(),
      [TokenType.INSTALL]: (self: Parser) => (self as DuckDBParser).parseInstall(),
      [TokenType.SHOW]: (self: Parser) => (self as DuckDBParser).parseShow(),
    };
  }

  @cache
  static get SET_PARSERS (): Record<string, (self: Parser) => Expression | undefined> {
    return {
      ...Parser.SET_PARSERS,
      VARIABLE: (self: Parser) => (self as DuckDBParser).parseSetItemAssignment({ kind: 'VARIABLE' }),
    };
  }

  parseLambda (options: { alias?: boolean } = {}): Expression | undefined {
    const index = this.index;
    if (!this.matchTextSeq('LAMBDA')) {
      return super.parseLambda(options);
    }

    const expressions = this.parseCsv(() => this.parseLambdaArg());
    if (!this.match(TokenType.COLON)) {
      this.retreat(index);
      return undefined;
    }

    const thisExpr = (this as DuckDBParser).replaceLambda(this.parseAssignment(), expressions);
    return this.expression(LambdaExpr, {
      this: thisExpr,
      expressions: expressions,
      colon: true,
    });
  }

  parseExpression (): Expression | undefined {
  // DuckDB supports prefix aliases, e.g. foo: 1
    if (this.next && this.next.tokenType === TokenType.COLON) {
      const alias = this.parseIdVar({ tokens: this._constructor.ALIAS_TOKENS });
      this.match(TokenType.COLON);
      let comments = this.prevComments ?? [];

      const thisExpr = this.parseAssignment();
      if (thisExpr instanceof Expression) {
      // Moves the comment next to the alias in `alias: expr /* comment */`
        comments = [...comments, ...(thisExpr.popComments() ?? [])];
      }

      return this.expression(AliasExpr, {
        comments: comments,
        this: thisExpr,
        alias: alias,
      });
    }

    return super.parseExpression();
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
  // DuckDB supports prefix aliases, e.g. FROM foo: bar
    let alias: Expression | undefined = undefined;
    let comments: string[] = [];

    if (this.next && this.next.tokenType === TokenType.COLON) {
      alias = this.parseTableAlias({
        aliasTokens: options.aliasTokens ?? this._constructor.TABLE_ALIAS_TOKENS,
      });
      this.match(TokenType.COLON);
      comments = this.prevComments ?? [];
    }

    const table = super.parseTable(options);

    if (table instanceof Expression && alias instanceof TableAliasExpr) {
    // Moves the comment next to the alias in `alias: table /* comment */`
      comments = [...comments, ...(table.popComments() ?? [])];
      alias.setArgKey('comments', [...(alias.popComments() ?? []), ...comments]);
      table.setArgKey('alias', alias);
    }

    return table;
  }

  parseTableSample (options: { asModifier?: boolean } = {}): TableSampleExpr | undefined {
    const sample = super.parseTableSample(options);
    if (sample && !sample.args.method) {
      if (sample.args.size) {
        sample.setArgKey('method', new VarExpr({ this: 'RESERVOIR' }));
      } else {
        sample.setArgKey('method', new VarExpr({ this: 'SYSTEM' }));
      }
    }

    return sample;
  }

  parseBracket (thisNode?: Expression): Expression | undefined {
    const bracket = super.parseBracket(thisNode);

    if (
      this.dialect.version.major <= 1
      && this.dialect.version.minor <= 2
      && (this.dialect.version.major !== 1 || this.dialect.version.minor !== 2)
      && bracket instanceof BracketExpr
    ) {
      bracket.setArgKey('returnsListForMaps', true);
    }

    return bracket;
  }

  parseMap (): ToMapExpr | MapExpr {
    if (this.match(TokenType.L_BRACE, { advance: false })) {
      return this.expression(ToMapExpr, { this: this.parseBracket() });
    }

    const args = this.parseWrappedCsv(() => this.parseAssignment());
    return this.expression(MapExpr, {
      keys: seqGet(args, 0),
      values: seqGet(args, 1),
    });
  }

  parseStructTypes (_options: { typeRequired?: boolean } = {}): Expression | undefined {
    return this.parseFieldDef();
  }

  pivotColumnNames (aggregations: Expression[]): string[] {
    if (aggregations.length === 1) {
      return super.pivotColumnNames(aggregations);
    }
    return pivotColumnNames(aggregations, { dialect: 'duckdb' });
  }

  parseAttachDetach (options: { isAttach?: boolean } = {}): AttachExpr | DetachExpr {
    const { isAttach = true } = options;
    const parseAttachOption = () => this.expression(AttachOptionExpr, {
      this: this.parseVar({ anyToken: true }),
      expression: this.parseField({ anyToken: true }),
    });

    this.match(TokenType.DATABASE);
    const exists = this.parseExists({ not: !isAttach });
    const thisNode = this.parseAlias(this.parsePrimaryOrVar(), { explicit: true });

    let expressions: Expression[] | undefined = undefined;
    if (this.match(TokenType.L_PAREN, { advance: false })) {
      expressions = this.parseWrappedCsv(parseAttachOption);
    }

    return isAttach
      ? this.expression(AttachExpr, {
        this: thisNode,
        exists: exists,
        expressions: expressions,
      })
      : this.expression(DetachExpr, {
        this: thisNode,
        exists: exists,
      });
  }

  parseShowDuckdb (thisStr: string): ShowExpr {
    return this.expression(ShowExpr, { this: thisStr });
  }

  parseForce (): InstallExpr | CommandExpr {
    if (!this.match(TokenType.INSTALL)) {
      return (this as DuckDBParser).parseAsCommand(this.prev);
    }

    return this.parseInstall({ force: true });
  }

  parseInstall (options: { force?: boolean } = {}): InstallExpr {
    return this.expression(InstallExpr, {
      this: this.parseIdVar(),
      from: this.match(TokenType.FROM) ? this.parseVarOrString() : undefined,
      force: options.force ?? false,
    });
  }

  /**
   * DuckDB supports positional columns using the # syntax (e.g., #1).
   */
  parsePrimary (): Expression | undefined {
    if (this.matchPair(TokenType.HASH, TokenType.NUMBER)) {
      return new PositionalColumnExpr({
        this: LiteralExpr.number(this.prev?.text || 0),
      });
    }

    return super.parsePrimary();
  }
}

class DuckDBGenerator extends Generator {
  static PARAMETER_TOKEN = '$';
  static NAMED_PLACEHOLDER_TOKEN = '$';
  static JOIN_HINTS = false;
  static TABLE_HINTS = false;
  static QUERY_HINTS = false;
  static LIMIT_FETCH = 'LIMIT';
  static STRUCT_DELIMITER = ['(', ')'];
  static RENAME_TABLE_WITH_DB = false;
  static NVL2_SUPPORTED = false;
  static SEMI_ANTI_JOIN_WITH_SIDE = false;
  static TABLESAMPLE_KEYWORDS = 'USING SAMPLE';
  static TABLESAMPLE_SEED_KEYWORD = 'REPEATABLE';
  static LAST_DAY_SUPPORTS_DATE_PART = false;
  static JSON_KEY_VALUE_PAIR_SEP = ',';
  static IGNORE_NULLS_IN_FUNC = true;
  static JSON_PATH_BRACKETED_KEY_SUPPORTED = false;
  static SUPPORTS_CREATE_TABLE_LIKE = false;
  static MULTI_ARG_DISTINCT = false;
  static CAN_IMPLEMENT_ARRAY_ANY = true;
  static SUPPORTS_TO_NUMBER = false;
  static SUPPORTS_WINDOW_EXCLUDE = true;
  static COPY_HAS_INTO_KEYWORD = false;
  static STAR_EXCEPT = 'EXCLUDE';
  static PAD_FILL_PATTERN_IS_REQUIRED = true;
  static ARRAY_SIZE_DIM_REQUIRED = false;
  static NORMALIZE_EXTRACT_DATE_PARTS = true;
  static SUPPORTS_LIKE_QUANTIFIERS = false;
  static SET_ASSIGNMENT_REQUIRES_VARIABLE_KEYWORD = true;

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (self: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (self: Generator, e: any) => string>([
      ...Generator.TRANSFORMS,
      [AnyValueExpr, anyValueSql],
      [ApproxDistinctExpr, approxCountDistinctSql],
      [BoolnotExpr, boolnotSql],
      [BoolandExpr, boolandSql],
      [BoolorExpr, boolorSql],
      [
        ArrayExpr,
        preprocess(
          [inheritStructFieldNames],
          inlineArrayUnlessQuery,
        ),
      ],
      [ArrayAppendExpr, arrayAppendSql('LIST_APPEND')],
      [ArrayCompactExpr, arrayCompactSql],
      [ArrayConstructCompactExpr, (self, e) => self.sql(new ArrayCompactExpr({ this: new ArrayExpr({ expressions: e.args.expressions }) }))],
      [ArrayConcatExpr, arrayConcatSql('LIST_CONCAT')],
      [ArrayFilterExpr, renameFunc('LIST_FILTER')],
      [ArrayInsertExpr, arrayInsertSql],
      [ArrayRemoveAtExpr, arrayRemoveAtSql],
      [ArrayRemoveExpr, removeFromArrayUsingFilter],
      [ArraySortExpr, arraySortSql],
      [ArrayPrependExpr, arrayAppendSql('LIST_PREPEND', { swapParams: true })],
      [ArraySumExpr, renameFunc('LIST_SUM')],
      [ArrayUniqueAggExpr, (self, e) => self.func('LIST', [new DistinctExpr({ expressions: [e.args.this] })])],
      [Base64DecodeBinaryExpr, (self, e) => base64DecodeSql(self as DuckDBGenerator, e, { toString: false })],
      [Base64DecodeStringExpr, (self, e) => base64DecodeSql(self as DuckDBGenerator, e, { toString: true })],
      [BitwiseAndExpr, (self, e) => (self as DuckDBGenerator).bitwiseOp(e, '&')],
      [BitwiseAndAggExpr, bitwiseAggSql],
      [BitwiseLeftShiftExpr, bitshiftSql],
      [BitwiseOrExpr, (self, e) => (self as DuckDBGenerator).bitwiseOp(e, '|')],
      [BitwiseOrAggExpr, bitwiseAggSql],
      [BitwiseRightShiftExpr, bitshiftSql],
      [BitwiseXorAggExpr, bitwiseAggSql],
      [ByteLengthExpr, (self, e) => self.func('OCTET_LENGTH', [e.args.this])],
      [CommentColumnConstraintExpr, noCommentColumnConstraintSql],
      [CorrExpr, (self, e) => (self as DuckDBGenerator).corrSql(e)],
      [CosineDistanceExpr, renameFunc('LIST_COSINE_DISTANCE')],
      [CurrentTimeExpr, () => 'CURRENT_TIME'],
      [
        CurrentTimestampExpr,
        (self, e) =>
          e.args.sysdate
            ? self.sql(new AtTimeZoneExpr({
              this: new VarExpr({ this: 'CURRENT_TIMESTAMP' }),
              zone: LiteralExpr.string('UTC'),
            }))
            : 'CURRENT_TIMESTAMP',
      ],
      [CurrentVersionExpr, renameFunc('version')],
      [LocaltimeExpr, (self: Generator, e: Expression) => unsupportedArgs('this')(() => 'LOCALTIME')(e)],
      [DayOfMonthExpr, renameFunc('DAYOFMONTH')],
      [DayOfWeekExpr, renameFunc('DAYOFWEEK')],
      [DayOfWeekIsoExpr, renameFunc('ISODOW')],
      [DayOfYearExpr, renameFunc('DAYOFYEAR')],
      [
        DaynameExpr,
        (self, e) =>
          e.args.abbreviated
            ? self.func('STRFTIME', [e.args.this, LiteralExpr.string('%a')])
            : self.func('DAYNAME', [e.args.this]),
      ],
      [
        MonthnameExpr,
        (self, e) =>
          e.args.abbreviated
            ? self.func('STRFTIME', [e.args.this, LiteralExpr.string('%b')])
            : self.func('MONTHNAME', [e.args.this]),
      ],
      [DataTypeExpr, dataTypeSql],
      [DateExpr, dateSql],
      [DateAddExpr, dateDeltaToBinaryIntervalOp()],
      [DateFromPartsExpr, dateFromPartsSql],
      [DateSubExpr, dateDeltaToBinaryIntervalOp()],
      [DateDiffExpr, dateDiffSql],
      [DateStrToDateExpr, dateStrToDateSql],
      [DatetimeExpr, noDatetimeSql],
      [DatetimeDiffExpr, dateDiffSql],
      [DatetimeSubExpr, dateDeltaToBinaryIntervalOp()],
      [DatetimeAddExpr, dateDeltaToBinaryIntervalOp()],
      [DateToDiExpr, (self, e) => `CAST(STRFTIME(${self.sql(e, 'this')}, ${DuckDB.DATEINT_FORMAT}) AS INT)`],
      [DecodeExpr, (self, e) => encodeDecodeSql(self, e, 'DECODE', { replace: false })],
      [
        DiToDateExpr,
        (self, e) =>
          `CAST(STRPTIME(CAST(${self.sql(e, 'this')} AS TEXT), ${DuckDB.DATEINT_FORMAT}) AS DATE)`,
      ],
      [EncodeExpr, (self, e) => encodeDecodeSql(self, e, 'ENCODE', { replace: false })],
      [
        EqualNullExpr,
        (self, e) => self.sql(new NullSafeEqExpr({
          this: e.args.this,
          expression: e.args.expression,
        })),
      ],
      [EuclideanDistanceExpr, renameFunc('LIST_DISTANCE')],
      [GenerateDateArrayExpr, generateDatetimeArraySql],
      [GenerateTimestampArrayExpr, generateDatetimeArraySql],
      [GetbitExpr, getBitSql],
      [GroupConcatExpr, (self, e) => groupConcatSql(self, e, { withinGroup: false })],
      [ExplodeExpr, renameFunc('UNNEST')],
      [IntDivExpr, (self, e) => self.binary(e, '//')],
      [IsInfExpr, renameFunc('ISINF')],
      [IsNanExpr, renameFunc('ISNAN')],
      [
        IsNullValueExpr,
        (self, e) => self.sql(new EqExpr({
          this: self.func('JSON_TYPE', [e.args.this]),
          expression: LiteralExpr.string('NULL'),
        })),
      ],
      [
        IsArrayExpr,
        (self, e) => self.sql(new EqExpr({
          this: self.func('JSON_TYPE', [e.args.this]),
          expression: LiteralExpr.string('ARRAY'),
        })),
      ],
      [CeilExpr, ceilFloor],
      [FloorExpr, ceilFloor],
      [JarowinklerSimilarityExpr, renameFunc('JARO_WINKLER_SIMILARITY')],
      [JsonbExistsExpr, renameFunc('JSON_EXISTS')],
      [JsonExtractExpr, arrowJsonExtractSqlDuckDB],
      [JsonExtractArrayExpr, jsonExtractValueArraySql],
      [JsonFormatExpr, jsonFormatSql],
      [JsonValueArrayExpr, jsonExtractValueArraySql],
      [LateralExpr, explodeToUnnestSql],
      [LogicalOrExpr, (self, e) => self.func('BOOL_OR', [castToBoolean(e.args.this)])],
      [LogicalAndExpr, (self, e) => self.func('BOOL_AND', [castToBoolean(e.args.this)])],
      [Seq1Expr, (self, e) => seqSql(self as DuckDBGenerator, e, 1)],
      [Seq2Expr, (self, e) => seqSql(self as DuckDBGenerator, e, 2)],
      [Seq4Expr, (self, e) => seqSql(self as DuckDBGenerator, e, 4)],
      [Seq8Expr, (self, e) => seqSql(self as DuckDBGenerator, e, 8)],
      [BoolxorAggExpr, boolxorAggSql],
      [MakeIntervalExpr, (self, e) => noMakeIntervalSql(self, e, { sep: ' ' })],
      [InitcapExpr, initcapSql],
      [Md5DigestExpr, (self, e) => self.func('UNHEX', [self.func('MD5', [e.args.this])])],
      [ShaExpr, (self, e) => shaSql(self as DuckDBGenerator, e, 'SHA1')],
      [Sha1DigestExpr, (self, e) => shaSql(self as DuckDBGenerator, e, 'SHA1', { isBinary: true })],
      [Sha2Expr, (self, e) => shaSql(self as DuckDBGenerator, e, 'SHA256')],
      [Sha2DigestExpr, (self, e) => shaSql(self as DuckDBGenerator, e, 'SHA256', { isBinary: true })],
      [MonthsBetweenExpr, monthsBetweenSql],
      [NextDayExpr, dayNavigationSql],
      [PercentileContExpr, renameFunc('QUANTILE_CONT')],
      [PercentileDiscExpr, renameFunc('QUANTILE_DISC')],
      [PivotExpr, preprocess([unqualifyColumns])],
      [PreviousDayExpr, dayNavigationSql],
      [
        RegexpReplaceExpr,
        (self, e) =>
          self.func('REGEXP_REPLACE', [
            e.args.this,
            e.args.expression,
            e.args.replacement,
            regexpReplaceGlobalModifier(e),
          ]),
      ],
      [RegexpLikeExpr, renameFunc('REGEXP_MATCHES')],
      [
        RegexpILikeExpr,
        (self, e) => self.func('REGEXP_MATCHES', [
          e.args.this,
          e.args.expression,
          LiteralExpr.string('i'),
        ]),
      ],
      [RegexpSplitExpr, renameFunc('STR_SPLIT_REGEX')],
      [RegrValxExpr, regrValSql],
      [RegrValyExpr, regrValSql],
      [ReturnExpr, (self, e) => self.sql(e, 'this')],
      [ReturnsPropertyExpr, (self, e) => (e.args.this instanceof SchemaExpr ? 'TABLE' : '')],
      [RandExpr, renameFunc('RANDOM')],
      [SplitExpr, renameFunc('STR_SPLIT')],
      [SortArrayExpr, sortArraySql],
      [StrPositionExpr, strPositionSql],
      [StrToUnixExpr, (self, e) => self.func('EPOCH', [self.func('STRPTIME', [e.args.this, self.formatTime(e)])])],
      [StructExpr, structSql],
      [TransformExpr, renameFunc('LIST_TRANSFORM')],
      [TimeAddExpr, dateDeltaToBinaryIntervalOp()],
      [TimeSubExpr, dateDeltaToBinaryIntervalOp()],
      [TimeExpr, noTimeSql],
      [TimeDiffExpr, timeDiffSql],
      [TimestampExpr, noTimestampSql],
      [TimestampAddExpr, dateDeltaToBinaryIntervalOp()],
      [
        TimestampDiffExpr,
        (self, e) => self.func('DATE_DIFF', [
          LiteralExpr.string(e.args.unit),
          e.args.expression,
          e.args.this,
        ]),
      ],
      [TimestampSubExpr, dateDeltaToBinaryIntervalOp()],
      [
        TimeStrToDateExpr,
        (self, e) => self.sql(new CastExpr({
          this: e.args.this,
          to: new DataTypeExpr({ this: DataTypeExprKind.DATE }),
        })),
      ],
      [TimeStrToTimeExpr, timeStrToTimeSql],
      [
        TimeStrToUnixExpr,
        (self, e) => self.func('EPOCH', [
          new CastExpr({
            this: e.args.this,
            to: new DataTypeExpr({ this: DataTypeExprKind.TIMESTAMP }),
          }),
        ]),
      ],
      [TimeToStrExpr, (self, e) => self.func('STRFTIME', [e.args.this, self.formatTime(e)])],
      [ToBooleanExpr, toBooleanSql],
      [TimeToUnixExpr, renameFunc('EPOCH')],
      [
        TsOrDiToDiExpr,
        (self, e) =>
          `CAST(SUBSTR(REPLACE(CAST(${self.sql(e, 'this')} AS TEXT), '-', ''), 1, 8) AS INT)`,
      ],
      [TsOrDsAddExpr, dateDeltaToBinaryIntervalOp()],
      [
        TsOrDsDiffExpr,
        (self, e) =>
          self.func('DATE_DIFF', [
            `'${e.args.unit || 'DAY'}'`,
            new CastExpr({
              this: e.args.expression,
              to: new DataTypeExpr({ this: DataTypeExprKind.TIMESTAMP }),
            }),
            new CastExpr({
              this: e.args.this,
              to: new DataTypeExpr({ this: DataTypeExprKind.TIMESTAMP }),
            }),
          ]),
      ],
      [UnixMicrosExpr, (self, e) => self.func('EPOCH_US', [implicitDatetimeCast(e.args.this)])],
      [UnixMillisExpr, (self, e) => self.func('EPOCH_MS', [implicitDatetimeCast(e.args.this)])],
      [
        UnixSecondsExpr,
        (self, e) =>
          self.sql(
            new CastExpr({
              this: self.func('EPOCH', [implicitDatetimeCast(e.args.this)]),
              to: new DataTypeExpr({ this: DataTypeExprKind.BIGINT }),
            }),
          ),
      ],
      [
        UnixToStrExpr,
        (self, e) =>
          self.func('STRFTIME', [self.func('TO_TIMESTAMP', [e.args.this]), self.formatTime(e)]),
      ],
      [
        DatetimeTruncExpr,
        (self, e) =>
          self.func('DATE_TRUNC', [
            unitToStr(e),
            new CastExpr({
              this: e.args.this,
              to: new DataTypeExpr({ this: DataTypeExprKind.DATETIME }),
            }),
          ]),
      ],
      [UnixToTimeExpr, unixToTimeSql],
      [UnixToTimeStrExpr, (self, e) => `CAST(TO_TIMESTAMP(${self.sql(e, 'this')}) AS TEXT)`],
      [VariancePopExpr, renameFunc('VAR_POP')],
      [WeekOfYearExpr, renameFunc('WEEKOFYEAR')],
      [
        YearOfWeekExpr,
        (self, e) => self.sql(new ExtractExpr({
          this: new VarExpr({ this: 'ISOYEAR' }),
          expression: e.args.this,
        })),
      ],
      [
        YearOfWeekIsoExpr,
        (self, e) => self.sql(new ExtractExpr({
          this: new VarExpr({ this: 'ISOYEAR' }),
          expression: e.args.this,
        })),
      ],
      [XorExpr, xorSql],
      [JsonObjectAggExpr, renameFunc('JSON_GROUP_OBJECT')],
      [JsonbObjectAggExpr, renameFunc('JSON_GROUP_OBJECT')],
      [DateBinExpr, renameFunc('TIME_BUCKET')],
      [LastDayExpr, lastDaySql],
    ]);
    return transforms;
  }

  @cache
  static get SUPPORTED_JSON_PATH_PARTS (): Set<typeof Expression> {
    return new Set([
      JsonPathKeyExpr,
      JsonPathRootExpr,
      JsonPathSubscriptExpr,
      JsonPathWildcardExpr,
    ]);
  }

  @cache
  static get TYPE_MAPPING () {
    return {
      ...Generator.TYPE_MAPPING,
      [DataTypeExprKind.BINARY]: 'BLOB',
      [DataTypeExprKind.BPCHAR]: 'TEXT',
      [DataTypeExprKind.CHAR]: 'TEXT',
      [DataTypeExprKind.DATETIME]: 'TIMESTAMP',
      [DataTypeExprKind.DECFLOAT]: 'DECIMAL(38, 5)',
      [DataTypeExprKind.FLOAT]: 'REAL',
      [DataTypeExprKind.JSONB]: 'JSON',
      [DataTypeExprKind.NCHAR]: 'TEXT',
      [DataTypeExprKind.NVARCHAR]: 'TEXT',
      [DataTypeExprKind.UINT]: 'UINTEGER',
      [DataTypeExprKind.VARBINARY]: 'BLOB',
      [DataTypeExprKind.ROWVERSION]: 'BLOB',
      [DataTypeExprKind.VARCHAR]: 'TEXT',
      [DataTypeExprKind.TIMESTAMPLTZ]: 'TIMESTAMPTZ',
      [DataTypeExprKind.TIMESTAMPNTZ]: 'TIMESTAMP',
      [DataTypeExprKind.TIMESTAMP_S]: 'TIMESTAMP_S',
      [DataTypeExprKind.TIMESTAMP_MS]: 'TIMESTAMP_MS',
      [DataTypeExprKind.TIMESTAMP_NS]: 'TIMESTAMP_NS',
      [DataTypeExprKind.BIGDECIMAL]: 'DECIMAL(38, 5)',
    };
  }

  static RESERVED_KEYWORDS = new Set([
    'array',
    'analyse',
    'union',
    'all',
    'when',
    'in_p',
    'default',
    'create_p',
    'window',
    'asymmetric',
    'to',
    'else',
    'localtime',
    'from',
    'end_p',
    'select',
    'current_date',
    'foreign',
    'with',
    'grant',
    'session_user',
    'or',
    'except',
    'references',
    'fetch',
    'limit',
    'group_p',
    'leading',
    'into',
    'collate',
    'offset',
    'do',
    'then',
    'localtimestamp',
    'check_p',
    'lateral_p',
    'current_role',
    'where',
    'asc_p',
    'placing',
    'desc_p',
    'user',
    'unique',
    'initially',
    'column',
    'both',
    'some',
    'as',
    'any',
    'only',
    'deferrable',
    'null_p',
    'current_time',
    'true_p',
    'table',
    'case',
    'trailing',
    'variadic',
    'for',
    'on',
    'distinct',
    'false_p',
    'not',
    'constraint',
    'current_timestamp',
    'returning',
    'primary',
    'intersect',
    'having',
    'analyze',
    'current_user',
    'and',
    'cast',
    'symmetric',
    'using',
    'order',
    'current_catalog',
  ]);

  @cache
  static get UNWRAPPED_INTERVAL_VALUES () {
    return new Set<typeof Expression>([LiteralExpr, ParenExpr]);
  }

  @cache
  static get PROPERTIES_LOCATION (): Map<typeof Expression, PropertiesLocation> {
    const locations = new Map<typeof Expression, PropertiesLocation>();

    // Default all existing properties to UNSUPPORTED
    [...Generator.PROPERTIES_LOCATION.keys()].forEach((prop) => {
      locations.set(prop, PropertiesLocation.UNSUPPORTED);
    });

    // Explicit overrides for DuckDB
    locations.set(LikePropertyExpr, PropertiesLocation.POST_SCHEMA);
    locations.set(TemporaryPropertyExpr, PropertiesLocation.POST_CREATE);
    locations.set(ReturnsPropertyExpr, PropertiesLocation.POST_ALIAS);
    locations.set(SequencePropertiesExpr, PropertiesLocation.POST_EXPRESSION);

    return locations;
  }

  @cache
  static get IGNORE_RESPECT_NULLS_WINDOW_FUNCTIONS () {
    return [
      FirstValueExpr,
      LagExpr,
      LastValueExpr,
      LeadExpr,
      NthValueExpr,
    ];
  }

  static ZIPF_TEMPLATE = maybeParse(`
  WITH rand AS (SELECT :random_expr AS r),
  weights AS (
      SELECT i, 1.0 / POWER(i, :s) AS w
      FROM RANGE(1, :n + 1) AS t(i)
  ),
  cdf AS (
      SELECT i, SUM(w) OVER (ORDER BY i) / SUM(w) OVER () AS p
      FROM weights
  )
  SELECT MIN(i)
  FROM cdf
  WHERE p >= (SELECT r FROM rand)
`);

  static NORMAL_TEMPLATE = maybeParse(
    ':mean + (:stddev * SQRT(-2 * LN(GREATEST(:u1, 1e-10))) * COS(2 * PI() * :u2))',
  );

  static SEEDED_RANDOM_TEMPLATE = maybeParse(
    '(ABS(HASH(:seed)) % 1000000) / 1000000.0',
  );

  static SEQ_UNSIGNED = maybeParse(`${SEQ_BASE} % :max_val`);

  static SEQ_SIGNED = maybeParse(`
  (CASE WHEN ${SEQ_BASE} % :max_val >= :half 
   THEN ${SEQ_BASE} % :max_val - :max_val 
   ELSE ${SEQ_BASE} % :max_val END)
`);

  static MAPCAT_TEMPLATE = maybeParse(`CASE
      WHEN :map1 IS NULL OR :map2 IS NULL THEN NULL
      ELSE MAP_FROM_ENTRIES(LIST_FILTER(LIST_TRANSFORM(
          LIST_DISTINCT(LIST_CONCAT(MAP_KEYS(:map1), MAP_KEYS(:map2))),
          __k -> STRUCT_PACK(key := __k, value := COALESCE(:map2[__k], :map1[__k]))
      ), __x -> __x.value IS NOT NULL))
  END
`);

  static EXTRACT_STRFTIME_MAPPINGS: Record<string, [string, string]> = {
    WEEKISO: ['%V', 'INTEGER'],
    YEAROFWEEK: ['%G', 'INTEGER'],
    YEAROFWEEKISO: ['%G', 'INTEGER'],
    NANOSECOND: ['%n', 'BIGINT'],
  };

  static EXTRACT_EPOCH_MAPPINGS: Record<string, string> = {
    EPOCH_SECOND: 'EPOCH',
    EPOCH_MILLISECOND: 'EPOCH_MS',
    EPOCH_MICROSECOND: 'EPOCH_US',
    EPOCH_NANOSECOND: 'EPOCH_NS',
  };

  /**
 * Snowflake's BITMAP_CONSTRUCT_AGG aggregates integers into a compact binary bitmap.
 * DuckDB implementation uses LIST_TRANSFORM and LIST_REDUCE to build hex strings
 * before converting to binary BLOBs.
 */
  static BITMAP_CONSTRUCT_AGG_TEMPLATE = maybeParse(`
  SELECT CASE
      WHEN l IS NULL OR LENGTH(l) = 0 THEN NULL
      WHEN LENGTH(l) != LENGTH(LIST_FILTER(l, __v -> __v BETWEEN 0 AND 32767)) THEN NULL
      WHEN LENGTH(l) < 5 THEN UNHEX(PRINTF('%04X', LENGTH(l)) || h || REPEAT('00', GREATEST(0, 4 - LENGTH(l)) * 2))
      ELSE UNHEX('08000000000000000000' || h)
  END
  FROM (
      SELECT l, COALESCE(LIST_REDUCE(
          LIST_TRANSFORM(l, __x -> PRINTF('%02X%02X', CAST(__x AS INT) & 255, (CAST(__x AS INT) >> 8) & 255)),
          (__a, __b) -> __a || __b, ''
      ), '') AS h
      FROM (SELECT LIST_SORT(LIST_DISTINCT(LIST(:arg) FILTER(NOT :arg IS NULL))) AS l)
  )
`);

  /**
   * Template for RANDSTR transpilation using a character pool and seeded hash.
   */
  static RANDSTR_TEMPLATE = maybeParse(`
  SELECT LISTAGG(
      SUBSTRING(
          '${RANDSTR_CHAR_POOL}',
          1 + CAST(FLOOR(random_value * 62) AS INT),
          1
      ),
      ''
  )
  FROM (
      SELECT (ABS(HASH(i + :seed)) % 1000) / 1000.0 AS random_value
      FROM RANGE(:length) AS t(i)
  )
`);

  /**
 * Template for MINHASH transpilation.
 * Returns JSON matching Snowflake format: {"state": [...], "type": "minhash", "version": 1}
 */
  static MINHASH_TEMPLATE = maybeParse(`
  SELECT JSON_OBJECT('state', LIST(min_h ORDER BY seed), 'type', 'minhash', 'version', 1)
  FROM (
      SELECT seed, LIST_MIN(LIST_TRANSFORM(vals, __v -> HASH(CAST(__v AS VARCHAR) || CAST(seed AS VARCHAR)))) AS min_h
      FROM (SELECT LIST(:expr) AS vals), RANGE(0, :k) AS t(seed)
  )
`);

  /**
 * Template for MINHASH_COMBINE transpilation.
 * Combines multiple minhash signatures by taking element-wise minimum.
 */
  static MINHASH_COMBINE_TEMPLATE = maybeParse(`
  SELECT JSON_OBJECT('state', LIST(min_h ORDER BY idx), 'type', 'minhash', 'version', 1)
  FROM (
      SELECT
          pos AS idx,
          MIN(val) AS min_h
      FROM
          UNNEST(LIST(:expr)) AS _(sig),
          UNNEST(CAST(sig -> 'state' AS UBIGINT[])) WITH ORDINALITY AS t(val, pos)
      GROUP BY pos
  )
`);

  static APPROXIMATE_SIMILARITY_TEMPLATE = maybeParse(`
  SELECT CAST(SUM(CASE WHEN num_distinct = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*)
  FROM (
      SELECT pos, COUNT(DISTINCT h) AS num_distinct
      FROM (
          SELECT h, pos
          FROM UNNEST(LIST(:expr)) AS _(sig),
               UNNEST(CAST(sig -> 'state' AS UBIGINT[])) WITH ORDINALITY AS s(h, pos)
      )
      GROUP BY pos
  )
`);

  static ARRAYS_ZIP_TEMPLATE = maybeParse(`
  CASE WHEN :null_check THEN NULL
  WHEN :all_empty_check THEN [:empty_struct]
  ELSE LIST_TRANSFORM(RANGE(0, :max_len), __i -> :transform_struct)
  END
`);

  /** Transform Snowflake's TIME_SLICE to DuckDB's time_bucket. */
  timeSliceSql (expression: TimeSliceExpr): string {
    const dateExpr = expression.args.this;
    const sliceLength = expression.args.expression;
    const unit = expression.args.unit;
    const kind = expression.text('kind').toUpperCase();

    const intervalExpr = new IntervalExpr({
      this: sliceLength,
      unit: unit,
    });
    const timeBucketExpr = this.func('time_bucket', [intervalExpr, dateExpr]);

    if (kind !== 'END') {
      return this.sql(timeBucketExpr);
    }

    const addExpr = new AddExpr({
      this: timeBucketExpr,
      expression: intervalExpr.copy(),
    });

    if (dateExpr?.isType(DataTypeExprKind.DATE)) {
      return this.sql(new CastExpr({
        this: addExpr,
        to: new DataTypeExpr({ this: DataTypeExprKind.DATE }),
      }));
    }

    return this.sql(addExpr);
  }

  /** Snowflake BITMAP_BUCKET_NUMBER to DuckDB CASE expression. */
  bitmapBucketNumberSql (this: DuckDBGenerator, expression: BitmapBucketNumberExpr): string {
    const value = expression.args.this;

    const positiveFormula = new AddExpr({
      this: new DivExpr({
        this: new SubExpr({
          this: value,
          expression: LiteralExpr.number(1),
        }),
        expression: LiteralExpr.number(32768),
      }),
      expression: LiteralExpr.number(1),
    });

    const nonPositiveFormula = new DivExpr({
      this: value,
      expression: LiteralExpr.number(32768),
    });

    return this.sql(
      new CaseExpr({})
        .when(new GtExpr({
          this: value,
          expression: LiteralExpr.number(0),
        }), positiveFormula)
        .else(nonPositiveFormula),
    );
  }

  /** Snowflake BITMAP_BIT_POSITION to DuckDB modulo CASE expression. */
  bitmapBitPositionSql (self: DuckDBGenerator, expression: BitmapBitPositionExpr): string {
    const thisNode = expression.args.this;

    return self.sql(
      new ModExpr({
        this: new ParenExpr({
          this: new IfExpr({
            this: new GtExpr({
              this: thisNode,
              expression: LiteralExpr.number(0),
            }),
            true: new SubExpr({
              this: thisNode,
              expression: LiteralExpr.number(1),
            }),
            false: new AbsExpr({ this: thisNode }),
          }),
        }),
        expression: MAX_BIT_POSITION,
      }),
    );
  }

  bitmapConstructAggSql (expression: BitmapConstructAggExpr): string {
  /** Snowflake BITMAP_CONSTRUCT_AGG using replacePlaceholders. */
    const arg = expression.args.this;
    return `(${this.sql(replacePlaceholders((this._constructor as typeof DuckDBGenerator).BITMAP_CONSTRUCT_AGG_TEMPLATE, { arg: arg }))})`;
  }

  nthValueSql (self: DuckDBGenerator, expression: NthValueExpr): string {
    const fromFirst = expression.args.fromFirst ?? true;
    if (!fromFirst) {
      self.unsupported('DuckDB\'s NTH_VALUE doesn\'t support starting from the end');
    }

    return self.functionFallbackSql(expression);
  }

  /**
   * Transpile Snowflake's RANDSTR to DuckDB equivalent using deterministic hash-based random.
   * RANDSTR(length, generator) generates a random string of specified length.
   */
  randstrSql (expression: RandstrExpr): string {
    const length = expression.args.this;
    const generator = expression.args.generator;

    let seedValue: Expression;

    if (generator) {
      if (generator instanceof RandExpr) {
        // If it's RANDOM(), use its seed if available, otherwise use RANDOM() itself
        seedValue = generator.args.this ?? generator;
      } else {
        // Const/int or other expression - use as seed directly
        seedValue = generator;
      }
    } else {
      // No generator specified, use default seed (arbitrary but deterministic)
      seedValue = LiteralExpr.number(RANDSTR_SEED);
    }

    const replacements = {
      seed: seedValue,
      length: length,
    };
    return `(${this.sql(replacePlaceholders((this._constructor as typeof DuckDBGenerator).RANDSTR_TEMPLATE, replacements))})`;
  }

  /**
     * Transpile Snowflake's ZIPF to DuckDB using CDF-based inverse sampling.
     */
  zipfSql (expression: ZipfExpr): string {
    const s = expression.args.this;
    const n = expression.args.elementcount;
    const gen = expression.args.gen;

    let randomExpr: Expression;

    if (!(gen instanceof RandExpr)) {
      // (ABS(HASH(seed)) % 1000000) / 1000000.0
      randomExpr = new DivExpr({
        this: new ParenExpr({
          this: new ModExpr({
            this: new AbsExpr({
              this: new AnonymousExpr({
                this: 'HASH',
                expressions: [(gen ?? null_()).copy()],
              }),
            }),
            expression: LiteralExpr.number(1000000),
          }),
        }),
        expression: LiteralExpr.number(1000000.0),
      });
    } else {
      // Use RANDOM() for non-deterministic output
      randomExpr = new RandExpr({});
    }

    const replacements = {
      s,
      n,
      randomExpr: randomExpr,
    };
    return `(${this.sql(replacePlaceholders((this._constructor as typeof DuckDBGenerator).ZIPF_TEMPLATE, replacements))})`;
  }

  /**
   * TO_BINARY and TRY_TO_BINARY transpilation:
   * Maps format (HEX, UTF-8, BASE64) to native DuckDB functions.
   */
  toBinarySql (expression: ToBinaryExpr): string {
    const value = expression.args.this;
    const formatArg = narrowInstanceOf(expression.args.format, Expression);
    const isSafe = expression.args.safe;
    const binaryCheck = isBinary(expression);

    if (!formatArg && !binaryCheck) {
      const funcName = isSafe ? 'TRY_TO_BINARY' : 'TO_BINARY';
      return this.func(funcName, [value]);
    }

    const fmt = formatArg?.name?.toUpperCase() ?? 'HEX';
    let result: string;

    if (fmt === 'UTF-8' || fmt === 'UTF8') {
      result = this.func('ENCODE', [value]);
    } else if (fmt === 'BASE64') {
      result = this.func('FROM_BASE64', [value]);
    } else if (fmt === 'HEX') {
      result = this.func('UNHEX', [value]);
    } else {
      if (isSafe) {
        return this.sql(null_());
      } else {
        this.unsupported(`format ${fmt} is not supported`);
        result = this.func('TO_BINARY', [value]);
      }
    }

    return isSafe ? `TRY(${result})` : result;
  }

  /**
   * Handle GREATEST/LEAST functions with dialect-aware NULL behavior.
   */
  greatestLeastSql (expression: GreatestExpr | LeastExpr): string {
    const allArgs = [expression.args.this, ...(expression.args.expressions ?? [])];
    const fallbackSql = this.functionFallbackSql(expression);

    if (expression.args.ignoreNulls) {
      return this.sql(fallbackSql);
    }

    const firstArg = allArgs[0] ?? null_();
    const caseExpr = new CaseExpr({}).when(
      new OrExpr({
        this: new IsExpr({
          this: firstArg.copy(),
          expression: null_(),
        }),
        expression: allArgs.slice(1).reduce(
          (acc, arg) =>
            new OrExpr({
              this: acc,
              expression: new IsExpr({
                this: (arg ?? null_()).copy(),
                expression: null_(),
              }),
            }),
          new IsExpr({
            this: firstArg.copy(),
            expression: null_(),
          }),
        ),
      }),
      null_(),
    );

    caseExpr.setArgKey('default', fallbackSql);
    return this.sql(caseExpr);
  }

  /** Transpile Snowflake GENERATOR to DuckDB range() */
  generatorSql (expression: GeneratorExpr): string {
    const rowcount = expression.args.rowcount;
    const timeLimit = expression.args.timeLimit;

    if (timeLimit) {
      this.unsupported('GENERATOR TIMELIMIT parameter is not supported in DuckDB');
    }

    if (!rowcount) {
      this.unsupported('GENERATOR without ROWCOUNT is not supported in DuckDB');
      return this.func('range', [LiteralExpr.number(0)]);
    }

    return this.func('range', [rowcount]);
  }

  greatestSql (expression: GreatestExpr): string {
    return this.greatestLeastSql(expression);
  }

  leastSql (expression: LeastExpr): string {
    return this.greatestLeastSql(expression);
  }

  lambdaSql (expression: LambdaExpr, options: { arrowSep?: string;
    wrap?: boolean; } = {}): string {
    let arrowSep = options.arrowSep ?? '->';
    let wrap = options.wrap ?? true;
    let prefix = '';

    if (expression.args.colon) {
      prefix = 'LAMBDA ';
      arrowSep = ':';
      wrap = false;
    }

    const lambdaSql = super.lambdaSql(expression, {
      arrowSep,
      wrap,
    });
    return `${prefix}${lambdaSql}`;
  }

  showSql (expression: ShowExpr): string {
    return `SHOW ${expression.name}`;
  }

  installSql (expression: InstallExpr): string {
    const force = expression.args.force ? 'FORCE ' : '';
    const thisSql = this.sql(expression, 'this');
    const fromClause = expression.args.from ? ` FROM ${expression.args.from}` : '';
    return `${force}INSTALL ${thisSql}${fromClause}`;
  }

  approxTopKSql (expression: ApproxTopKExpr): string {
    this.unsupported(
      'APPROX_TOP_K cannot be transpiled to DuckDB due to incompatible return types. ',
    );
    return this.functionFallbackSql(expression);
  }

  fromIso8601TimestampSql (expression: FromIso8601TimestampExpr): string {
    return this.sql(
      new CastExpr({
        this: expression.args.this,
        to: new DataTypeExpr({ this: DataTypeExprKind.TIMESTAMPTZ }),
      }),
    );
  }

  strToTimeSql (expression: StrToTimeExpr): string {
    const targetType = expression.args.targetType;
    const needsTz =
      targetType
      && [DataTypeExprKind.TIMESTAMPLTZ, DataTypeExprKind.TIMESTAMPTZ].includes(targetType.args.this as DataTypeExprKind);

    if (expression.args.safe) {
      const formattedTime = this.formatTime(expression);
      const castType = needsTz ? DataTypeExprKind.TIMESTAMPTZ : DataTypeExprKind.TIMESTAMP;
      return this.sql(
        new CastExpr({
          this: this.func('TRY_STRPTIME', [expression.args.this, formattedTime]),
          to: new DataTypeExpr({ this: castType }),
        }),
      );
    }

    const baseSql = strToTimeSql(this, expression);
    if (needsTz) {
      return this.sql(
        new CastExpr({
          this: baseSql,
          to: new DataTypeExpr({ this: DataTypeExprKind.TIMESTAMPTZ }),
        }),
      );
    }
    return baseSql;
  }

  strToDateSql (expression: StrToDateExpr): string {
    const formattedTime = this.formatTime(expression);
    const functionName = !expression.args.safe ? 'STRPTIME' : 'TRY_STRPTIME';
    return this.sql(
      new CastExpr({
        this: this.func(functionName, [expression.args.this, formattedTime]),
        to: new DataTypeExpr({ this: DataTypeExprKind.DATE }),
      }),
    );
  }

  tsOrDsToTimeSql (expression: TsOrDsToTimeExpr): string {
    const thisNode = expression.args.this;
    const timeFormat = this.formatTime(expression);
    const safe = expression.args.safe;
    const timeType = DataTypeExpr.build('TIME', { dialect: 'duckdb' });
    const CastClass = safe ? TryCastExpr : CastExpr;

    if (timeFormat) {
      const funcName = safe ? 'TRY_STRPTIME' : 'STRPTIME';
      const strptime = new AnonymousExpr({
        this: funcName,
        expressions: [thisNode ?? null_(), timeFormat],
      });
      return this.sql(new CastClass({
        this: strptime,
        to: timeType,
      }));
    }

    if (thisNode instanceof TsOrDsToTimeExpr || thisNode?.isType(DataTypeExprKind.TIME)) {
      return this.sql(thisNode ?? null_());
    }

    return this.sql(new CastClass({
      this: thisNode,
      to: timeType,
    }));
  }

  currentDateSql (expression: CurrentDateExpr): string {
    if (!expression.args.this) {
      return 'CURRENT_DATE';
    }

    const expr = new CastExpr({
      this: new AtTimeZoneExpr({
        this: new CurrentTimestampExpr({}),
        zone: expression.args.this,
      }),
      to: new DataTypeExpr({ this: DataTypeExprKind.DATE }),
    });
    return this.sql(expr);
  }

  parseJsonSql (expression: ParseJsonExpr): string {
    const arg = expression.args.this;
    if (expression.args.safe) {
      return this.sql(
        new CaseExpr({})
          .when(this.func('json_valid', [arg ?? null_()]), (arg instanceof Expression || typeof arg === 'string') ? arg : '')
          .else(null_()),
      );
    }
    return this.func('JSON', [arg]);
  }

  @unsupportedArgs('decimals')
  truncSql (expression: TruncExpr): string {
    return this.func('TRUNC', [expression.args.this]);
  }

  normalSql (expression: NormalExpr): string {
    const mean = expression.args.this;
    const stddev = expression.args.stddev;
    const gen = expression.args.gen;

    let u1: Expression;
    let u2: Expression;

    if (gen instanceof RandExpr && !gen.args.this) {
      u1 = new RandExpr({});
      u2 = new RandExpr({});
    } else {
      const seed = (gen instanceof RandExpr ? gen.args.this : gen) ?? null_();
      u1 = replacePlaceholders((this._constructor as typeof DuckDBGenerator).SEEDED_RANDOM_TEMPLATE, { seed });
      u2 = replacePlaceholders((this._constructor as typeof DuckDBGenerator).SEEDED_RANDOM_TEMPLATE, {
        seed: new AddExpr({
          this: seed.copy(),
          expression: LiteralExpr.number(1),
        }),
      });
    }

    const replacements = {
      mean,
      stddev,
      u1,
      u2,
    };
    return this.sql(replacePlaceholders((this._constructor as typeof DuckDBGenerator).NORMAL_TEMPLATE, replacements));
  }

  uniformSql (expression: UniformExpr): string {
    const minVal = expression.args.this;
    const maxVal = expression.args.expression;
    const gen = expression.args.gen;

    const isIntResult = minVal instanceof LiteralExpr && minVal.isInteger
      && maxVal instanceof LiteralExpr && maxVal.isInteger;

    let randomExpr: Expression;
    if (!(gen instanceof RandExpr)) {
      randomExpr = new DivExpr({
        this: new ParenExpr({
          this: new ModExpr({
            this: new AbsExpr({
              this: new AnonymousExpr({
                this: 'HASH',
                expressions: [gen || ''],
              }),
            }),
            expression: LiteralExpr.number(1000000),
          }),
        }),
        expression: LiteralExpr.number(1000000.0),
      });
    } else {
      randomExpr = new RandExpr({});
    }

    let rangeExpr: Expression = new SubExpr({
      this: maxVal,
      expression: minVal,
    });
    if (isIntResult) {
      rangeExpr = new AddExpr({
        this: rangeExpr,
        expression: LiteralExpr.number(1),
      });
    }

    let result: Expression = new AddExpr({
      this: minVal,
      expression: new MulExpr({
        this: randomExpr,
        expression: new ParenExpr({ this: rangeExpr }),
      }),
    });

    if (isIntResult) {
      result = new CastExpr({
        this: new FloorExpr({ this: result }),
        to: new DataTypeExpr({ this: DataTypeExprKind.BIGINT }),
      });
    }

    return this.sql(result);
  }

  timeFromPartsSql (expression: TimeFromPartsExpr): string {
    /** Snowflake's TIME_FROM_PARTS supports overflow and nanoseconds. */
    const nano = expression.args.nano;
    const overflow = expression.args.overflow;

    if (overflow) {
      const hour = narrowInstanceOf(expression.args.hour, 'string', LiteralExpr);
      const minute = narrowInstanceOf(expression.args.min, 'string', LiteralExpr);
      let sec: Expression | string | undefined = narrowInstanceOf(expression.args.sec, 'string', LiteralExpr);

      // Efficiency path: use MAKE_TIME for standard ranges
      if (!nano && [
        hour,
        minute,
        sec,
      ].every((arg) => arg instanceof LiteralExpr && arg.isNumber)) {
        const hVal = parseInt((hour instanceof LiteralExpr ? hour.args.this : hour) ?? '0');
        const mVal = parseInt((minute instanceof LiteralExpr ? minute.args.this : minute) ?? '0');
        const sVal = parseInt((sec instanceof Expression ? sec.args.this as string : sec) ?? '0');
        if (0 <= hVal && hVal <= 23 && 0 <= mVal && mVal <= 59 && 0 <= sVal && sVal <= 59) {
          return renameFunc('MAKE_TIME')(this, expression);
        }
      }

      // Overflow or nanoseconds: use INTERVAL arithmetic
      if (nano) {
        sec = new AddExpr({
          this: sec,
          expression: new DivExpr({
            this: nano.pop(),
            expression: LiteralExpr.number(1000000000.0),
          }),
        });
      }

      const totalSeconds = new AddExpr({
        this: new AddExpr({
          this: new MulExpr({
            this: hour,
            expression: LiteralExpr.number(3600),
          }),
          expression: new MulExpr({
            this: minute,
            expression: LiteralExpr.number(60),
          }),
        }),
        expression: sec,
      });

      return this.sql(
        new AddExpr({
          this: new CastExpr({
            this: LiteralExpr.string('00:00:00'),
            to: DataTypeExpr.build('TIME'),
          }),
          expression: new IntervalExpr({
            this: totalSeconds,
            unit: new VarExpr({ this: 'SECOND' }),
          }),
        }),
      );
    }

    if (nano) {
      expression.setArgKey(
        'sec',
        new AddExpr({
          this: expression.args.sec,
          expression: new DivExpr({
            this: nano.pop(),
            expression: LiteralExpr.number(1000000000.0),
          }),
        }),
      );
    }

    return renameFunc('MAKE_TIME')(this, expression);
  }

  /** Transpile EXTRACT with DuckDB strftime/epoch mappings. */
  extractSql (expression: ExtractExpr): string {
    const datetimeExpr = expression.args.expression;

    if (datetimeExpr?.isType([DataTypeExprKind.TIMESTAMPTZ, DataTypeExprKind.TIMESTAMPLTZ])) {
      this.unsupported(
        'EXTRACT from TIMESTAMPTZ / TIMESTAMPLTZ may produce different results due to timezone handling differences',
      );
    }

    const partName = narrowInstanceOf(expression.args.this, Expression)?.name.toUpperCase();

    if (partName !== undefined && partName in DuckDBGenerator.EXTRACT_STRFTIME_MAPPINGS) {
      const [fmt, castType] = DuckDBGenerator.EXTRACT_STRFTIME_MAPPINGS[partName];

      const isNanoTime =
        partName === 'NANOSECOND'
        && datetimeExpr?.isType([DataTypeExprKind.TIME, DataTypeExprKind.TIMETZ]);

      const datetimeExprSafe = datetimeExpr ?? null_();
      if (isNanoTime) {
        this.unsupported('Parameter NANOSECOND is not supported with TIME type in DuckDB');
        return this.sql(
          new CastExpr({
            this: new MulExpr({
              this: new ExtractExpr({
                this: new VarExpr({ this: 'MICROSECOND' }),
                expression: datetimeExprSafe,
              }),
              expression: LiteralExpr.number(1000),
            }),
            to: DataTypeExpr.build(castType, { dialect: 'duckdb' }),
          }),
        );
      }

      let strftimeInput: Expression = datetimeExprSafe;
      if (partName === 'NANOSECOND') {
        strftimeInput = new CastExpr({
          this: datetimeExprSafe,
          to: new DataTypeExpr({ this: DataTypeExprKind.TIMESTAMP_NS }),
        });
      }

      return this.sql(
        new CastExpr({
          this: new AnonymousExpr({
            this: 'STRFTIME',
            expressions: [strftimeInput, LiteralExpr.string(fmt)],
          }),
          to: DataTypeExpr.build(castType, { dialect: 'duckdb' }),
        }),
      );
    }

    if (partName !== undefined && partName in DuckDBGenerator.EXTRACT_EPOCH_MAPPINGS) {
      const funcName = DuckDBGenerator.EXTRACT_EPOCH_MAPPINGS[partName];
      let result: Expression = new AnonymousExpr({
        this: funcName,
        expressions: [datetimeExpr ?? null_()],
      });
      if (partName === 'EPOCH_SECOND') {
        result = new CastExpr({
          this: result,
          to: DataTypeExpr.build('BIGINT', { dialect: 'duckdb' }),
        });
      }
      return this.sql(result);
    }

    return super.extractSql(expression);
  }

  timestampFromPartsSql (expression: TimestampFromPartsExpr): string {
    const dateExpr = expression.args.this;
    const timeExpr = expression.args.expression;

    if (dateExpr && timeExpr) {
      return this.sql(new AddExpr({
        this: dateExpr,
        expression: timeExpr,
      }));
    }

    let sec = expression.args.sec;
    if (!sec) return renameFunc('MAKE_TIMESTAMP')(this, expression);

    const milli = expression.args.milli;
    if (milli) {
      sec = new AddExpr({
        this: sec,
        expression: new DivExpr({
          this: milli.pop(),
          expression: LiteralExpr.number(1000.0),
        }),
      });
    }

    const nano = expression.args.nano;
    if (nano) {
      sec = new AddExpr({
        this: sec,
        expression: new DivExpr({
          this: nano.pop(),
          expression: LiteralExpr.number(1000000000.0),
        }),
      });
    }

    if (milli || nano) {
      expression.setArgKey('sec', sec);
    }

    return renameFunc('MAKE_TIMESTAMP')(this, expression);
  }

  timestampLtzFromPartsSql (expression: TimestampLtzFromPartsExpr): string {
    const nano = expression.args.nano;
    if (nano) nano.pop();

    const timestamp = renameFunc('MAKE_TIMESTAMP')(this, expression);
    return `CAST(${timestamp} AS TIMESTAMPTZ)`;
  }

  timestampTzFromPartsSql (expression: TimestampTzFromPartsExpr): string {
    let zone = expression.args.zone;
    if (zone) zone = zone.pop();

    const nano = expression.args.nano;
    if (nano) nano.pop();

    const timestamp = renameFunc('MAKE_TIMESTAMP')(this, expression);

    if (zone) {
      return `${timestamp} AT TIME ZONE ${this.sql(zone)}`;
    }

    return timestamp;
  }

  tableSampleSql (expression: TableSampleExpr, options: { tablesampleKeyword?: string } = {}): string {
    let keyword = options.tablesampleKeyword;
    if (!(expression.parent instanceof SelectExpr)) {
      keyword = 'TABLESAMPLE';
    }

    if (expression.args.size) {
      const method = expression.args.method;
      if (method instanceof Expression && method.name?.toUpperCase() !== 'RESERVOIR') {
        this.unsupported(
          `Sampling method ${method} is not supported with a discrete sample count, defaulting to reservoir sampling`,
        );
        expression.setArgKey('method', new VarExpr({ this: 'RESERVOIR' }));
      }
    }

    return super.tableSampleSql(expression, { tablesampleKeyword: keyword });
  }

  columnDefSql (expression: ColumnDefExpr, options: { sep?: string } = {}): string {
    if (expression.parent instanceof UserDefinedFunctionExpr) {
      return this.sql(expression, 'this');
    }
    return super.columnDefSql(expression, options);
  }

  joinSql (expression: JoinExpr): string {
    const isInnerOuter = [
      '',
      'INNER',
      'OUTER',
    ].includes(expression.args.kind ?? '');

    if (
      !expression.args.using
      && !expression.args.on
      && !expression.args.method
      && isInnerOuter
    ) {
      /**
       * Some dialects support `LEFT/INNER JOIN UNNEST(...)` without an explicit ON clause.
       * DuckDB requires one, so we inject a dummy `ON TRUE`.
       */
      if (expression.args.this instanceof UnnestExpr) {
        return super.joinSql(expression.on(new BooleanExpr({ this: true })));
      }

      expression.setArgKey('side', undefined);
      expression.setArgKey('kind', undefined);
    }

    return super.joinSql(expression);
  }

  generateSeriesSql (expression: GenerateSeriesExpr): string {
    // GENERATE_SERIES(a, b) -> [a, b], RANGE(a, b) -> [a, b)
    if (expression.args.isEndExclusive) {
      return renameFunc('RANGE')(this, expression);
    }

    return this.functionFallbackSql(expression);
  }

  countifSql (expression: CountIfExpr): string {
    if (1 < this.dialect.version.major || (this.dialect.version.major === 1 && 2 <= this.dialect.version.minor)) {
      return this.functionFallbackSql(expression);
    }

    return countIfToSum(this, expression);
  }

  bracketSql (expression: BracketExpr): string {
    if (1 < this.dialect.version.major || (this.dialect.version.major === 1 && 2 <= this.dialect.version.minor)) {
      return super.bracketSql(expression);
    }

    /** * In DuckDB < 1.2, array literals followed by brackets often need parens: ([1, 2])[1]
     * For Maps, DuckDB returns a list of keys; we wrap to return the value.
     */
    const thisNode = expression.args.this;
    if (thisNode instanceof ArrayExpr) {
      thisNode.replace(new ParenExpr({ this: thisNode.copy() }));
    }

    let bracket = super.bracketSql(expression);

    if (!expression.args.returnsListForMaps) {
      const thisNodeExpr = narrowInstanceOf(thisNode, Expression);
      const annotated = thisNodeExpr?.type
        ? thisNodeExpr
        : thisNodeExpr && annotateTypes(thisNodeExpr, { dialect: this.dialect });

      if (isType(annotated, DataTypeExprKind.MAP)) {
        bracket = `(${bracket})[1]`;
      }
    }

    return bracket;
  }

  /**
   * DuckDB requires ORDER BY inside the function for ARRAY_AGG.
   * Transform: ARRAY_AGG(x) WITHIN GROUP (ORDER BY y) -> ARRAY_AGG(x ORDER BY y)
   */
  withinGroupSql (expression: WithinGroupExpr): string {
    const func = expression.args.this;

    if (func instanceof ArrayAggExpr) {
      const order = expression.args.expression;
      if (!(order instanceof OrderExpr)) {
        return this.sql(func);
      }

      const originalThis = func.args.this;

      func.setArgKey(
        'this',
        new OrderExpr({
          this: narrowInstanceOf(func.args.this, Expression)?.copy(),
          expressions: order.args.expressions,
        }),
      );

      const arrayAggSql = this.functionFallbackSql(func as FuncExpr);
      return originalThis instanceof Expression ? this.addArrayAggNullFilter(arrayAggSql, func, originalThis) : '';
    }

    const expressionSql = this.sql(expression, 'expression');

    if (func instanceof PercentileContExpr || func instanceof PercentileDiscExpr) {
      /**
       * Reorder arguments for DuckDB percentiles:
       * percentile_cont(fraction) WITHIN GROUP (ORDER BY col) -> percentile_cont(col, fraction)
       */
      const orderCol = expression.find(OrderedExpr);
      if (orderCol) {
        func.setArgKey('expression', func.args.this);
        func.setArgKey('this', orderCol.args.this);
      }
    }

    const thisSql = this.sql(expression, 'this').replace(/\)$/, '');
    return `${thisSql}${expressionSql})`;
  }

  lengthSql (expression: LengthExpr): string {
    const arg = expression.args.this;

    if (!expression.args.binary || (arg instanceof LiteralExpr && arg.isString)) {
      return this.func('LENGTH', [arg]);
    }

    const annotated = arg?.type
      ? arg
      : arg && annotateTypes(arg, { dialect: this.dialect });

    if (annotated?.isType(DataTypeExpr.TEXT_TYPES)) {
      return this.func('LENGTH', [arg]);
    }

    /** Resolve BLOB vs String length dynamically using TYPEOF */
    const blob = new CastExpr({
      this: arg,
      to: new DataTypeExpr({ this: DataTypeExprKind.VARBINARY }),
    });
    const varchar = new CastExpr({
      this: arg,
      to: new DataTypeExpr({ this: DataTypeExprKind.VARCHAR }),
    });

    const caseNode = new CaseExpr({
      this: new AnonymousExpr({
        this: 'TYPEOF',
        expressions: arg ? [arg] : [],
      }),
    })
      .when(LiteralExpr.string('BLOB'), new ByteLengthExpr({ this: blob }))
      .else(new AnonymousExpr({
        this: 'LENGTH',
        expressions: [varchar],
      }));

    return this.sql(caseNode);
  }

  @unsupportedArgs('insCost', 'delCost', 'subCost')
  levenshteinSql (expression: LevenshteinExpr): string {
    const thisNode = expression.args.this;
    const exprNode = expression.args.expression;
    const maxDist = expression.args.maxDist;

    if (!maxDist) {
      return this.func('LEVENSHTEIN', [thisNode, exprNode]);
    }

    /** Emulate Snowflake: if distance > maxDist, return maxDist */
    const levenshtein = new LevenshteinExpr({
      this: thisNode,
      expression: exprNode,
    });
    return this.sql(new LeastExpr({
      this: levenshtein,
      expressions: [maxDist],
      ignoreNulls: false,
    }));
  }

  padSql (expression: PadExpr): string {
    const stringArg = expression.args.this;
    const fillArg = expression.args.fillPattern ?? LiteralExpr.string(' ');

    if (isBinary(stringArg) || isBinary(fillArg)) {
      const lengthArg = expression.args.expression;
      const isLeft = expression.args.isLeft;

      const inputLen = new ByteLengthExpr({ this: stringArg });
      const charsNeeded = new SubExpr({
        this: lengthArg,
        expression: inputLen,
      });
      const padCount = new GreatestExpr({
        this: LiteralExpr.number(0),
        expressions: [charsNeeded],
        ignoreNulls: true,
      });
      const repeatExpr = new RepeatExpr({
        this: fillArg,
        times: [padCount],
      });

      const [left, right] = isLeft ? [repeatExpr, stringArg] : [stringArg, repeatExpr];
      return this.sql(new DPipeExpr({
        this: left,
        expression: right,
      }));
    }

    return super.padSql(expression);
  }

  minhashSql (expression: MinhashExpr): string {
    const k = expression.args.this;
    const exprs = expression.args.expressions ?? [];

    if (exprs.length !== 1 || exprs[0] instanceof StarExpr) {
      this.unsupported('MINHASH with multiple expressions or * requires manual restructuring');
      return this.func('MINHASH', [k, ...exprs]);
    }

    const result = replacePlaceholders(DuckDBGenerator.MINHASH_TEMPLATE.copy(), {
      expr: exprs[0],
      k: k,
    });
    return `(${this.sql(result)})`;
  }

  minhashCombineSql (expression: MinhashCombineExpr): string {
    const result = replacePlaceholders(DuckDBGenerator.MINHASH_COMBINE_TEMPLATE.copy(), {
      expr: expression.args.this,
    });
    return `(${this.sql(result)})`;
  }

  approximateSimilaritySql (expression: ApproximateSimilarityExpr): string {
    const result = replacePlaceholders(DuckDBGenerator.APPROXIMATE_SIMILARITY_TEMPLATE.copy(), {
      expr: expression.args.this,
    });
    return `(${this.sql(result)})`;
  }

  arraysZipSql (expression: ArraysZipExpr): string {
    const args = expression.args.expressions ?? [];

    if (args.length === 0) {
      /** Return [{}] - Represented as MAP([], []) for empty structs in DuckDB */
      return this.sql(new ArrayExpr({
        expressions: [
          new MapExpr({
            keys: [],
            values: [],
          }),
        ],
      }));
    }

    const lengths = args.map((arg) => new LengthExpr({ this: arg }));
    const maxLen = lengths.length === 1
      ? lengths[0]
      : new GreatestExpr({
        this: lengths[0],
        expressions: lengths.slice(1),
        ignoreNulls: false,
      });

    /** Snowflake pads to longest; DuckDB truncates. Logic below emulates padding. */
    const emptyStruct = this.func(
      'STRUCT',
      args.map((_, i) => new PropertyEqExpr({
        this: LiteralExpr.string(`$${i + 1}`),
        expression: null_(),
      })),
    );

    const index = new AddExpr({
      this: new ColumnExpr({ this: new IdentifierExpr({ this: '__i' }) }),
      expression: LiteralExpr.number(1),
    });
    const transformStruct = this.func(
      'STRUCT',
      args.map((arg, i) => new PropertyEqExpr({
        this: LiteralExpr.string(`$${i + 1}`),
        expression: new BracketExpr({
          this: new AnonymousExpr({
            this: 'COALESCE',
            expressions: [arg, new ArrayExpr({})],
          }),
          expressions: [index],
        }),
      })),
    );

    const result = replacePlaceholders(DuckDBGenerator.ARRAYS_ZIP_TEMPLATE.copy(), {
      nullCheck: args.reduce((acc, arg) => new OrExpr({
        this: acc,
        expression: new IsExpr({
          this: arg,
          expression: null_(),
        }),
      }), new IsExpr({
        this: args[0],
        expression: null_(),
      })),
      allEmptyCheck: args.reduce((acc, arg) => new AndExpr({
        this: acc,
        expression: new EqExpr({
          this: new LengthExpr({ this: arg }),
          expression: LiteralExpr.number(0),
        }),
      }), new EqExpr({
        this: new LengthExpr({ this: args[0] }),
        expression: LiteralExpr.number(0),
      })),
      emptyStruct: emptyStruct,
      maxLen: maxLen,
      transformStruct: transformStruct,
    });

    return this.sql(result);
  }

  lowerSql (expression: LowerExpr): string {
    const resultSql = this.func('LOWER', [castToVarchar(expression.args.this)]);
    return genWithCastToBlob(this, expression, resultSql);
  }

  upperSql (expression: UpperExpr): string {
    const resultSql = this.func('UPPER', [castToVarchar(expression.args.this)]);
    return genWithCastToBlob(this, expression, resultSql);
  }

  reverseSql (expression: ReverseExpr): string {
    const resultSql = this.func('REVERSE', [castToVarchar(expression.args.this)]);
    return genWithCastToBlob(this, expression, resultSql);
  }

  /**
   * DuckDB TO_BASE64 requires BLOB input.
   * Snowflake BASE64_ENCODE implicitly encodes UTF-8 bytes for VARCHAR.
   */
  base64EncodeSql (expression: Base64EncodeExpr): string {
    let result = expression.args.this;

    if (result?.isType(DataTypeExpr.TEXT_TYPES)) {
      result = new AnonymousExpr({
        this: 'ENCODE',
        expressions: [result],
      });
    }

    result = new ToBase64Expr({ this: result });

    const maxLineLength = expression.args.maxLineLength;
    const alphabet = expression.args.alphabet;

    // Handle custom alphabet
    result = applyBase64AlphabetReplacements(result, alphabet);

    // Handle max_line_length by inserting newlines every N characters using regex
    const lineLength =
      maxLineLength instanceof LiteralExpr && maxLineLength.isNumber
        ? parseInt(maxLineLength.args.this ?? '0')
        : 0;

    if (0 < lineLength) {
      const newline = new ChrExpr({ expressions: [LiteralExpr.number(10)] });
      result = new TrimExpr({
        this: new RegexpReplaceExpr({
          this: result,
          expression: LiteralExpr.string(`(.{${lineLength}})`),
          replacement: new ConcatExpr({
            expressions: [LiteralExpr.string('\\1'), newline.copy()],
          }),
        }),
        expression: newline,
        position: TrimPosition.TRAILING,
      });
    }

    return this.sql(result);
  }

  replaceSql (expression: ReplaceExpr): string {
    const resultSql = this.func('REPLACE', [
      castToVarchar(expression.args.this),
      castToVarchar(expression.args.expression),
      castToVarchar(expression.args.replacement),
    ]);
    return genWithCastToBlob(this, expression, resultSql);
  }

  bitwiseOp (expression: BinaryExpr, op: string): string {
    prepareBinaryBitwiseArgs(expression);
    const resultSql = this.binary(expression, op);
    return genWithCastToBlob(this, expression, resultSql);
  }

  bitwiseXorSql (expression: BitwiseXorExpr): string {
    prepareBinaryBitwiseArgs(expression);
    const resultSql = this.func('XOR', [expression.args.this, expression.args.expression]);
    return genWithCastToBlob(this, expression, resultSql);
  }

  objectInsertSql (expression: ObjectInsertExpr): string {
    const thisNode = expression.args.this;
    const key = expression.args.key;
    const keySql = key instanceof Expression ? key.name : '';
    const valueSql = this.sql(expression, 'value');

    const kvSql = `${keySql} := ${valueSql}`;

    /** If input is an empty struct, use STRUCT_PACK.
     * DuckDB's STRUCT_INSERT isn't valid on an empty {} literal.
     */
    if (thisNode instanceof StructExpr && (!thisNode.args.expressions || thisNode.args.expressions.length === 0)) {
      return this.func('STRUCT_PACK', [kvSql]);
    }

    return this.func('STRUCT_INSERT', [thisNode, kvSql]);
  }

  mapCatSql (expression: MapCatExpr): string {
    const result = replacePlaceholders(DuckDBGenerator.MAPCAT_TEMPLATE.copy(), {
      map1: expression.args.this,
      map2: expression.args.expression,
    });
    return this.sql(result);
  }

  startsWithSql (expression: StartsWithExpr): string {
    return this.func('STARTS_WITH', [castToVarchar(expression.args.this)!, castToVarchar(expression.args.expression)!]);
  }

  spaceSql (expression: SpaceExpr): string {
    /** DuckDB's REPEAT requires BIGINT for count. */
    return this.sql(
      new RepeatExpr({
        this: LiteralExpr.string(' '),
        times: [
          new CastExpr({
            this: expression.args.this,
            to: new DataTypeExpr({ this: DataTypeExprKind.BIGINT }),
          }),
        ],
      }),
    );
  }

  tableFromRowsSql (expression: TableFromRowsExpr): string {
    if (expression.args.this instanceof GeneratorExpr) {
      const table = new TableExpr({
        this: expression.args.this,
        alias: expression.args.alias,
        joins: expression.args.joins,
      });
      return this.sql(table);
    }

    return super.tableFromRowsSql(expression);
  }

  unnestSql (expression: UnnestExpr): string {
    const explodeArray = expression.args.explodeArray;
    if (explodeArray) {
      /** Transpile BQ nested UNNEST to DuckDB subquery with max_depth => 2.
       */
      if (!expression.args.expressions) {
        expression.args.expressions = [];
      }
      expression.args.expressions?.push(
        new KwargExpr({
          this: new VarExpr({ this: 'max_depth' }),
          expression: LiteralExpr.number(2),
        }),
      );

      let alias = expression.args.alias;
      if (alias instanceof TableAliasExpr) {
        expression.setArgKey('alias', undefined);
        if (alias.args.columns && 0 < alias.args.columns.length) {
          alias = new TableAliasExpr({ this: seqGet(alias.args.columns, 0) });
        }
      }

      const unnestStr = super.unnestSql(expression);
      const aliasSql = alias ? ` ${this.sql(alias)}` : '';
      return `(SELECT ${unnestStr})${aliasSql}`;
    }

    return super.unnestSql(expression);
  }

  ignoreNullsSql (expression: IgnoreNullsExpr): string {
    const thisNode = expression.args.this;

    if ((this._constructor as typeof DuckDBGenerator).IGNORE_RESPECT_NULLS_WINDOW_FUNCTIONS.some((cls) => thisNode instanceof cls)) {
      return super.ignoreNullsSql(expression);
    }

    let node = thisNode;
    if (node instanceof FirstExpr) {
      node = new AnyValueExpr({ this: node.args.this });
    }

    if (!(node instanceof AnyValueExpr || node instanceof ApproxQuantilesExpr)) {
      this.unsupported('IGNORE NULLS is not supported for non-window functions.');
    }

    return this.sql(node);
  }

  respectNullsSql (expression: RespectNullsExpr): string {
    if ((this._constructor as typeof DuckDBGenerator).IGNORE_RESPECT_NULLS_WINDOW_FUNCTIONS.some((cls) => expression.args.this instanceof cls)) {
      return super.respectNullsSql(expression);
    }

    this.unsupported('RESPECT NULLS is not supported for non-window functions.');
    return this.sql(expression, 'this');
  }

  arrayToStringSql (expression: ArrayToStringExpr): string {
    let thisSql = this.sql(expression, 'this');
    const nullText = this.sql(expression, 'null');

    if (nullText) {
      thisSql = `LIST_TRANSFORM(${thisSql}, x -> COALESCE(x, ${nullText}))`;
    }

    return this.func('ARRAY_TO_STRING', [thisSql, expression.args.expression]);
  }

  regexpExtractSql (expression: RegexpExtractExpr): string {
    let thisNode: Expression = expression.args.this as Expression;
    const group = expression.args.group;
    const params = expression.args.parameters;
    const position = expression.args.position;
    const occurrence = expression.args.occurrence;
    const nullIfPosOverflow = expression.args.nullIfPosOverflow;

    if (position && (!(position instanceof LiteralExpr && position.isInteger) || 1 < parseInt(position.args.this ?? '0'))) {
      thisNode = new SubstringExpr({
        this: thisNode,
        start: position,
      });

      if (nullIfPosOverflow) {
        thisNode = new NullifExpr({
          this: thisNode,
          expression: LiteralExpr.string(''),
        });
      }
    }

    let groupArg = group;
    const defaultGroup = String(this.dialect._constructor.REGEXP_EXTRACT_DEFAULT_GROUP);
    if (!params && group instanceof IdentifierExpr && group.name === defaultGroup) {
      groupArg = undefined;
    }

    if (occurrence && (!(occurrence instanceof LiteralExpr && occurrence.isInteger) || 1 < parseInt((occurrence as LiteralExpr).args.this ?? '0'))) {
      return this.func('ARRAY_EXTRACT', [
        this.func('REGEXP_EXTRACT_ALL', [
          thisNode,
          expression.args.expression,
          groupArg,
          params,
        ]),
        LiteralExpr.number(parseInt((occurrence as LiteralExpr).args.this ?? '0')),
      ]);
    }

    return this.func('REGEXP_EXTRACT', [
      thisNode,
      expression.args.expression,
      groupArg,
      params,
    ]);
  }

  numberToStrSql (expression: NumberToStrExpr): string {
    const fmt = expression.args.format;
    if (fmt && /^\d+$/.test(fmt)) {
      return this.func('FORMAT', [LiteralExpr.string(`{:,.${fmt}f}`), expression.args.this]);
    }

    this.unsupported('Only integer formats are supported by NumberToStr');
    return this.functionFallbackSql(expression);
  }

  autoIncrementColumnConstraintSql (_expression: ColumnDefExpr): string {
    this.unsupported('The AUTOINCREMENT column constraint is not supported by DuckDB');
    return '';
  }

  aliasesSql (expression: AliasesExpr): string {
    const thisNode = expression.args.this;
    if (thisNode instanceof PosexplodeExpr) {
      return this.posexplodeSql(thisNode);
    }

    return super.aliasesSql(expression);
  }

  posexplodeSql (expression: PosexplodeExpr): string {
    const thisNode = expression.args.this as Expression;
    const parent = expression.parent;

    /** The default Spark aliases are "pos" and "col" */
    let pos: Expression | undefined = new IdentifierExpr({
      this: 'pos',
      quoted: false,
    });
    let col: Expression | undefined = new IdentifierExpr({
      this: 'col',
      quoted: false,
    });

    if (parent instanceof AliasesExpr) {
      const aliases = parent.args.expressions;
      pos = aliases?.[0];
      col = aliases?.[1];
    } else if (parent instanceof TableExpr) {
      const alias = parent.args.alias;
      if (alias instanceof TableAliasExpr) {
        const columns = alias.args.columns;
        if (columns && 0 < columns.length) {
          pos = columns[0] as Expression;
          col = (columns[1] as Expression) ?? col;
        }
        alias.pop();
      }
    }

    /**
     * Translate Spark POSEXPLODE to DuckDB UNNEST + GENERATE_SUBSCRIPTS.
     * Subtract 1 from DuckDB's 1-indexed subscripts to match Spark's 0-indexed pos.
     */
    const unnestSql = this.sql(new UnnestExpr({
      expressions: [thisNode],
      alias: col as Expression,
    }));
    const genSubscripts = this.sql(
      new AliasExpr({
        this: new SubExpr({
          this: new AnonymousExpr({
            this: 'GENERATE_SUBSCRIPTS',
            expressions: [thisNode, LiteralExpr.number(1)],
          }),
          expression: LiteralExpr.number(1),
        }),
        alias: pos,
      }),
    );

    const posexplodeSql = this.formatArgs([genSubscripts, unnestSql]);

    if (parent instanceof FromExpr || (parent && parent.parent instanceof FromExpr)) {
      return `(SELECT ${posexplodeSql})`;
    }

    return posexplodeSql;
  }

  /**
   * Handles Snowflake rounding behavior for float months and preserves
   * input types (DATE/TIMESTAMPTZ) which DuckDB usually promotes to TIMESTAMP.
   */
  addMonthsSql (expression: AddMonthsExpr): string {
    let thisNode = expression.args.this;
    if (!thisNode?.type) {
      thisNode = thisNode && annotateTypes(thisNode, { dialect: this.dialect });
    }

    if (thisNode?.isType(DataTypeExpr.TEXT_TYPES)) {
      thisNode = new CastExpr({
        this: thisNode,
        to: new DataTypeExpr({ this: DataTypeExprKind.TIMESTAMP }),
      });
    }

    const monthsExpr = expression.args.expression;
    const annotatedMonths = monthsExpr?.type
      ? monthsExpr
      : monthsExpr && annotateTypes(monthsExpr, { dialect: this.dialect });

    let intervalOrToMonths: Expression;
    if (annotatedMonths?.isType([
      DataTypeExprKind.FLOAT,
      DataTypeExprKind.DOUBLE,
      DataTypeExprKind.DECIMAL,
    ])) {
      /** Float case: Round and use TO_MONTHS */
      intervalOrToMonths = new AnonymousExpr({
        this: 'TO_MONTHS',
        expressions: [
          new CastExpr({
            this: new AnonymousExpr({
              this: 'ROUND',
              expressions: monthsExpr && [monthsExpr],
            }),
            to: new DataTypeExpr({ this: DataTypeExprKind.INT }),
          }),
        ],
      });
    } else {
      intervalOrToMonths = new IntervalExpr({
        this: monthsExpr,
        unit: new VarExpr({ this: 'MONTH' }),
      });
    }

    const dateAddExpr = new AddExpr({
      this: thisNode,
      expression: intervalOrToMonths,
    });

    /** Apply end-of-month preservation: last day of month + 1 month = last day of next month */
    const preserveEom = expression.args.preserveEndOfMonth;
    let resultExpr: Expression = dateAddExpr;

    if (preserveEom) {
      resultExpr = new CaseExpr({}).when(
        new EqExpr({
          this: this.func('LAST_DAY', [thisNode]),
          expression: thisNode,
        }),
        this.func('LAST_DAY', [dateAddExpr]),
      )
        .else(dateAddExpr);
    }

    if (thisNode?.isType([DataTypeExprKind.DATE, DataTypeExprKind.TIMESTAMPTZ])) {
      return this.sql(new CastExpr({
        this: resultExpr,
        to: thisNode.type,
      }));
    }

    return this.sql(resultExpr);
  }

  formatSql (expression: FormatExpr): string {
    if (expression.name.toLowerCase() === '%s' && expression.args.expressions?.length === 1) {
      return this.func('FORMAT', [LiteralExpr.string('{}'), expression.args.expressions[0]]);
    }
    return this.functionFallbackSql(expression);
  }

  dateTruncSql (expression: DateTruncExpr): string {
    const unit = unitToStr(expression);
    const dateNode = expression.args.this;
    const result = this.func('DATE_TRUNC', [unit, dateNode]);

    if (
      expression.args.inputTypePreserved
      && dateNode?.isType(DataTypeExpr.TEMPORAL_TYPES)
      && !(isDateUnit(unit) && dateNode.isType(DataTypeExprKind.DATE))
    ) {
      return this.sql(new CastExpr({
        this: result,
        to: dateNode.type,
      }));
    }

    return result;
  }

  timestampTruncSql (expression: TimestampTruncExpr): string {
    const unit = unitToStr(expression);
    const zone = expression.args.zone;
    let timestamp = expression.args.this;
    const isDate = isDateUnit(unit);

    if (isDate && zone) {
      /** BigQuery compatibility: Truncate in target TZ and return as UTC */
      timestamp = new AtTimeZoneExpr({
        this: timestamp,
        zone: zone,
      });
      const resultSql = this.func('DATE_TRUNC', [unit, timestamp]);
      return this.sql(new AtTimeZoneExpr({
        this: resultSql,
        zone: zone,
      }));
    }

    let result = this.func('DATE_TRUNC', [unit, timestamp]);
    if (expression.args.inputTypePreserved) {
      if (timestamp?.isType([DataTypeExprKind.TIME, DataTypeExprKind.TIMETZ])) {
        const dummyDate = new CastExpr({
          this: LiteralExpr.string('1970-01-01'),
          to: new DataTypeExpr({ this: DataTypeExprKind.DATE }),
        });
        const dateTime = new AddExpr({
          this: dummyDate,
          expression: timestamp,
        });
        result = this.func('DATE_TRUNC', [unit, dateTime]);
        return this.sql(new CastExpr({
          this: result,
          to: timestamp.type,
        }));
      }

      if (timestamp?.isType(DataTypeExpr.TEMPORAL_TYPES) && !(isDate && timestamp.isType(DataTypeExprKind.DATE))) {
        return this.sql(new CastExpr({
          this: result,
          to: timestamp.type,
        }));
      }
    }

    return result;
  }

  trimSql (expression: TrimExpr): string {
    expression.args.this?.replace(castToVarchar(expression.args.this));
    if (expression.args.expression instanceof Expression) {
      expression.args.expression.replace(castToVarchar(expression.args.expression));
    }

    const resultSql = super.trimSql(expression);
    return genWithCastToBlob(this, expression, resultSql);
  }

  roundSql (expression: RoundExpr): string {
    const thisNode = expression.args.this;
    let decimals = expression.args.decimals;
    let truncate = expression.args.truncate;

    if (decimals && expression.args.castsNonIntegerDecimals) {
      if (!(decimals instanceof LiteralExpr && decimals.isInteger)) {
        decimals = new CastExpr({
          this: decimals,
          to: new DataTypeExpr({ this: DataTypeExprKind.INT }),
        });
      }
    }

    let func = 'ROUND';
    if (truncate instanceof LiteralExpr) {
      const truncVal = truncate.args.this?.toUpperCase();
      if (truncVal !== undefined && ['ROUND_HALF_EVEN', 'HALF_TO_EVEN'].includes(truncVal)) {
        func = 'ROUND_EVEN';
        truncate = undefined;
      } else if (truncVal !== undefined && ['ROUND_HALF_AWAY_FROM_ZERO', 'HALF_AWAY_FROM_ZERO'].includes(truncVal)) {
        truncate = undefined;
      }
    }

    return this.func(func, [
      thisNode,
      decimals,
      truncate,
    ]);
  }

  approxQuantileSql (expression: ApproxQuantileExpr): string {
    let result = this.func('APPROX_QUANTILE', [expression.args.this, expression.args.quantile]);

    if (expression.isType(DataTypeExpr.REAL_TYPES)) {
      result = `CAST(${result} AS DOUBLE)`;
    }

    return result;
  }

  approxQuantilesSql (expression: ApproxQuantilesExpr): string {
    /** * BigQuery APPROX_QUANTILES(expr, n) divisions.
     * DuckDB requires explicit array of probabilities [0.0, ..., 1.0].
     */
    const thisNode = expression.args.this;
    let numQuantilesExpr: Expression;

    if (thisNode instanceof DistinctExpr) {
      if (!thisNode.args.expressions || thisNode.args.expressions.length < 2) {
        this.unsupported('APPROX_QUANTILES requires a bucket count argument');
        return this.functionFallbackSql(expression);
      }
      numQuantilesExpr = thisNode.args.expressions[1].pop();
    } else {
      numQuantilesExpr = expression.args.expression as Expression;
    }

    if (!(numQuantilesExpr instanceof LiteralExpr && numQuantilesExpr.isInteger)) {
      this.unsupported('APPROX_QUANTILES bucket count must be a positive integer');
      return this.functionFallbackSql(expression);
    }

    const numQuantiles = parseInt(numQuantilesExpr.args.this ?? '0');
    if (numQuantiles <= 0) {
      this.unsupported('APPROX_QUANTILES bucket count must be a positive integer');
      return this.functionFallbackSql(expression);
    }

    const quantiles = Array.from({ length: numQuantiles + 1 }, (_, i) =>
      LiteralExpr.number(i / numQuantiles));

    return this.sql(
      new ApproxQuantileExpr({
        this: thisNode,
        quantile: new ArrayExpr({ expressions: quantiles }),
      }),
    );
  }

  jsonExtractScalarSql (expression: JsonExtractScalarExpr): string {
    if (expression.args.scalarOnly) {
      expression = new JsonExtractScalarExpr({
        this: new AnonymousExpr({
          this: 'JSON_VALUE',
          expressions: [expression.args.this, expression.args.expression].filter(
            (e): e is Expression => e instanceof Expression,
          ),
        }),
        expression: LiteralExpr.string('$'),
      });
    }
    return arrowJsonExtractSqlDuckDB(this, expression);
  }

  bitwiseNotSql (expression: BitwiseNotExpr): string {
    const thisNode = expression.args.this as Expression;
    if (isBinary(thisNode)) {
      expression.type = new DataTypeExpr({ this: DataTypeExprKind.BINARY });
    }

    let arg = castToBit(thisNode);
    if (thisNode instanceof NegExpr) {
      arg = new ParenExpr({ this: arg });
    }

    expression.setArgKey('this', arg);
    const resultSql = `~${this.sql(expression, 'this')}`;
    return genWithCastToBlob(this, expression, resultSql);
  }

  windowSql (expression: WindowExpr): string {
    const thisNode = expression.args.this;
    if (thisNode instanceof CorrExpr || (thisNode instanceof FilterExpr && thisNode.args.this instanceof CorrExpr)) {
      return this.corrSql(expression);
    }
    return super.windowSql(expression);
  }

  filterSql (expression: FilterExpr): string {
    if (expression.args.this instanceof CorrExpr) {
      return this.corrSql(expression);
    }
    return super.filterSql(expression);
  }

  corrSql (expression: FilterExpr | WindowExpr | CorrExpr): string {
    if (expression instanceof CorrExpr && !expression.args.nullOnZeroVariance) {
      return this.func('CORR', [expression.args.this, expression.args.expression]);
    }

    const corrExpr = maybeCorrNullToFalse(expression);
    if (!corrExpr) {
      if (expression instanceof WindowExpr) return super.windowSql(expression);
      if (expression instanceof FilterExpr) return super.filterSql(expression);
      return this.sql(expression);
    }

    /** Emulate NULL_ON_ZERO_VARIANCE using CASE + ISNAN */
    return this.sql(
      new CaseExpr({})
        .when(new IsNanExpr({ this: corrExpr }), null_())
        .else(corrExpr),
    );
  }
}

export class DuckDB extends Dialect {
  static NULL_ORDERING = 'nulls_are_last' as const;
  static SUPPORTS_USER_DEFINED_TYPES = true;
  static SAFE_DIVISION = true;
  static INDEX_OFFSET = 1;
  static CONCAT_COALESCE = true;
  static SUPPORTS_ORDER_BY_ALL = true;
  static SUPPORTS_FIXED_SIZE_ARRAYS = true;
  static STRICT_JSON_PATH_SYNTAX = false;
  static NUMBERS_CAN_BE_UNDERSCORE_SEPARATED = true;

  static NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE;

  @cache
  static get DATE_PART_MAPPING (): Record<string, string> {
    const mapping: Record<string, string> = {
      ...Dialect.DATE_PART_MAPPING,
      DAYOFWEEKISO: 'ISODOW',
    };
    delete mapping['WEEKDAY'];
    return mapping;
  }

  static EXPRESSION_METADATA = { ...Dialect.EXPRESSION_METADATA };

  static INVERSE_TIME_MAPPING = {
    '%e': '%-d', // BigQuery's space-padded day (%e) -> DuckDB's no-padding day (%-d)
    '%:z': '%z', // In DuckDB %z can represent ±HH:MM, ±HHMM, or ±HH.
    '%-z': '%z',
    '%f_zero': '%n',
    '%f_one': '%n',
    '%f_two': '%n',
    '%f_three': '%g',
    '%f_four': '%n',
    '%f_five': '%n',
    '%f_seven': '%n',
    '%f_eight': '%n',
    '%f_nine': '%n',
  };

  toJsonPath (path: Expression | undefined): Expression | undefined {
    if (path instanceof LiteralExpr && path.isString) {
      /**
       * DuckDB supports JSON pointer syntax (starting with `/`) and
       * back-of-list access `[#-i]`. We return these as-is to avoid
       * invalid JSON path canonicalization.
       */
      const pathText = path.name;
      if (pathText.startsWith('/') || pathText.includes('[#')) {
        return path;
      }
    }

    return super.toJsonPath(path);
  }

  static Tokenizer = DuckDBTokenizer;
  static Parser = DuckDBParser;
  static Generator = DuckDBGenerator;
}

Dialect.register(Dialects.DUCKDB, DuckDB);
