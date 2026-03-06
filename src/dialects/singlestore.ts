import { cache } from '../port_internals';
import type {
  AllExpr,
  AlterColumnExpr,
  CollateExpr,
  ComputedColumnConstraintExpr,
  CurrentTimeExpr,
  Expression, IdentifierExpr,
  JsonExtractArrayExpr,
  JsonValueExpr,
  StandardHashExpr,
  TruncateTableExpr,
} from '../expressions';
import {
  ApproxDistinctExpr,
  ApproxQuantileExpr,
  CastExpr, DataTypeExprKind,
  DataTypeExpr,
  DataTypeParamExpr,
  LiteralExpr,
  StrToDateExpr, StrToTimeExpr, TimeToStrExpr, ToCharExpr, TsOrDsToDateExpr,
  ParenExpr,
  DayOfWeekExpr,
  StrToUnixExpr,
  UnixToTimeExpr,
  DateBinExpr,
  JsonbExistsExpr,
  JsonbExtractExpr,
  JsonbExtractScalarExpr,
  JsonExtractExpr,
  JsonExtractScalarExpr,
  JsonArrayContainsExpr,
  JsonKeysExpr,
  JsonFormatExpr,
  JsonArrayExpr,
  JsonObjectExpr,
  DateExpr,
  TimestampDiffExpr,
  HllExpr,
  VariancePopExpr,
  ContainsExpr,
  RegexpExtractAllExpr,
  RegexpExtractExpr,
  ReduceExpr,
  JsonArrayAggExpr,
  UtcTimeExpr,
  UtcDateExpr,
  UtcTimestampExpr,
  TryCastExpr,
  JsonExistsExpr,
  RenameColumnExpr,
  JsonPathKeyExpr,
  JsonPathRootExpr,
  JsonPathSubscriptExpr,
  CastToStrTypeExpr,
  TimeToUnixExpr,
  TimeStrToUnixExpr,
  UnixSecondsExpr,
  UnixToStrExpr,
  UnixToTimeStrExpr,
  TimeStrToDateExpr,
  FromTimeZoneExpr,
  DiToDateExpr,
  DateToDiExpr,
  TsOrDiToDiExpr,
  TimeExpr,
  DatetimeAddExpr,
  DatetimeTruncExpr,
  DatetimeSubExpr,
  TimestampTruncExpr,
  DatetimeDiffExpr,
  DateTruncExpr,
  DateDiffExpr,
  TsOrDsDiffExpr,
  CurrentDatetimeExpr,
  CurrentTimestampExpr,
  CurrentDateExpr,
  DayOfWeekIsoExpr,
  DayOfMonthExpr,
  CountIfExpr,
  LogicalOrExpr,
  LogicalAndExpr,
  VarianceExpr,
  XorExpr,
  CbrtExpr,
  PowExpr,
  RegexpLikeExpr,
  RepeatExpr,
  MulExpr,
  IsAsciiExpr,
  Md5DigestExpr,
  RegexpILikeExpr,
  StartsWithExpr,
  FromBaseExpr,
  LowerExpr,
  StuffExpr,
  NationalExpr,
  MatchAgainstExpr,
  ShowExpr,
  DescribeExpr,
} from '../expressions';
import {
  unsupportedArgs, type Generator,
} from '../generator';
import { seqGet } from '../helper';
import type { Parser } from '../parser';
import { TokenType } from '../tokens';
import {
  boolXorSql,
  buildFormattedTime,
  buildJsonExtractPath,
  countIfToSum,
  dateAddIntervalSql,
  Dialect, Dialects,
  jsonExtractSegments,
  jsonPathKeyOnlyName,
  renameFunc,
  timestampDiffSql,
  timestampTruncSql,
} from './dialect';
import {
  dateAddSql,
  MySQL, removeTsOrDsToDate, showParser,
} from './mysql';

function castToTime6 (
  expression?: Expression,
  timeType: DataTypeExprKind = DataTypeExprKind.TIME,
): CastExpr {
  return new CastExpr({
    this: expression,
    to: DataTypeExpr.build(timeType, {
      expressions: [new DataTypeParamExpr({ this: LiteralExpr.number(6) })],
    }),
  });
}

class SingleStoreTokenizer extends MySQL.Tokenizer {
  static BYTE_STRINGS: [string, string][] = [['e\'', '\''], ['E\'', '\'']];

  static KEYWORDS = {
    ...MySQL.Tokenizer.KEYWORDS,
    'BSON': TokenType.JSONB,
    'GEOGRAPHYPOINT': TokenType.GEOGRAPHYPOINT,
    'TIMESTAMP': TokenType.TIMESTAMP,
    'UTC_DATE': TokenType.UTC_DATE,
    'UTC_TIME': TokenType.UTC_TIME,
    'UTC_TIMESTAMP': TokenType.UTC_TIMESTAMP,
    ':>': TokenType.COLON_GT,
    '!:>': TokenType.NCOLON_GT,
    '::$': TokenType.DCOLONDOLLAR,
    '::%': TokenType.DCOLONPERCENT,
    '::?': TokenType.DCOLONQMARK,
    'RECORD': TokenType.STRUCT,
  };
}

class SingleStoreParser extends MySQL.Parser {
  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> {
    return (() => {
      const functions = {
        ...MySQL.Parser.FUNCTIONS,
        TO_DATE: buildFormattedTime(TsOrDsToDateExpr, { dialect: 'singlestore' }),
        TO_TIMESTAMP: buildFormattedTime(StrToTimeExpr, { dialect: 'singlestore' }),
        TO_CHAR: buildFormattedTime(ToCharExpr, { dialect: 'singlestore' }),
        STR_TO_DATE: buildFormattedTime(StrToDateExpr, { dialect: 'mysql' }),
        DATE_FORMAT: buildFormattedTime(TimeToStrExpr, { dialect: 'mysql' }),
        TIME_FORMAT: (args: Expression[]) =>
          new TimeToStrExpr({
            this: castToTime6(seqGet(args, 0)),
            format: MySQL.formatTime(seqGet(args, 1)),
          }),
        HOUR: (args: Expression[]) =>
          new CastExpr({
            this: new TimeToStrExpr({
              this: castToTime6(seqGet(args, 0)),
              format: MySQL.formatTime(LiteralExpr.string('%k')),
            }),
            to: DataTypeExpr.build(DataTypeExprKind.INT),
          }),
        MICROSECOND: (args: Expression[]) =>
          new CastExpr({
            this: new TimeToStrExpr({
              this: castToTime6(seqGet(args, 0)),
              format: MySQL.formatTime(LiteralExpr.string('%f')),
            }),
            to: DataTypeExpr.build(DataTypeExprKind.INT),
          }),
        SECOND: (args: Expression[]) =>
          new CastExpr({
            this: new TimeToStrExpr({
              this: castToTime6(seqGet(args, 0)),
              format: MySQL.formatTime(LiteralExpr.string('%s')),
            }),
            to: DataTypeExpr.build(DataTypeExprKind.INT),
          }),
        MINUTE: (args: Expression[]) =>
          new CastExpr({
            this: new TimeToStrExpr({
              this: castToTime6(seqGet(args, 0)),
              format: MySQL.formatTime(LiteralExpr.string('%i')),
            }),
            to: DataTypeExpr.build(DataTypeExprKind.INT),
          }),
        MONTHNAME: (args: Expression[]) =>
          new TimeToStrExpr({
            this: seqGet(args, 0),
            format: MySQL.formatTime(LiteralExpr.string('%M')),
          }),
        WEEKDAY: (args: Expression[]) =>
          new ParenExpr({
            this: (new DayOfWeekExpr({ this: seqGet(args, 0) }).add(5))
              .mod(7),
          }),
        UNIX_TIMESTAMP: StrToUnixExpr.fromArgList,
        FROM_UNIXTIME: buildFormattedTime(UnixToTimeExpr, { dialect: 'mysql' }),
        TIME_BUCKET: (args: Expression[]) =>
          new DateBinExpr({
            this: seqGet(args, 0),
            expression: seqGet(args, 1),
            origin: seqGet(args, 2),
          }),
        BSON_EXTRACT_BSON: buildJsonExtractPath(JsonbExtractExpr),
        BSON_EXTRACT_STRING: buildJsonExtractPath(JsonbExtractScalarExpr, { jsonType: 'STRING' }),
        BSON_EXTRACT_DOUBLE: buildJsonExtractPath(JsonbExtractScalarExpr, { jsonType: 'DOUBLE' }),
        BSON_EXTRACT_BIGINT: buildJsonExtractPath(JsonbExtractScalarExpr, { jsonType: 'BIGINT' }),
        JSON_EXTRACT_JSON: buildJsonExtractPath(JsonExtractExpr),
        JSON_EXTRACT_STRING: buildJsonExtractPath(JsonExtractScalarExpr, { jsonType: 'STRING' }),
        JSON_EXTRACT_DOUBLE: buildJsonExtractPath(JsonExtractScalarExpr, { jsonType: 'DOUBLE' }),
        JSON_EXTRACT_BIGINT: buildJsonExtractPath(JsonExtractScalarExpr, { jsonType: 'BIGINT' }),
        JSON_ARRAY_CONTAINS_STRING: (args: Expression[]) =>
          new JsonArrayContainsExpr({
            this: seqGet(args, 1),
            expression: seqGet(args, 0),
            jsonType: 'STRING',
          }),
        JSON_ARRAY_CONTAINS_DOUBLE: (args: Expression[]) =>
          new JsonArrayContainsExpr({
            this: seqGet(args, 1),
            expression: seqGet(args, 0),
            jsonType: 'DOUBLE',
          }),
        JSON_ARRAY_CONTAINS_JSON: (args: Expression[]) =>
          new JsonArrayContainsExpr({
            this: seqGet(args, 1),
            expression: seqGet(args, 0),
            jsonType: 'JSON',
          }),
        JSON_KEYS: (args: Expression[]) =>
          new JsonKeysExpr({
            this: seqGet(args, 0),
            expressions: args.slice(1),
          }),
        JSON_PRETTY: JsonFormatExpr.fromArgList,
        JSON_BUILD_ARRAY: (args: Expression[]) => new JsonArrayExpr({ expressions: args }),
        JSON_BUILD_OBJECT: (args: Expression[]) => new JsonObjectExpr({ expressions: args }),
        DATE: DateExpr.fromArgList,
        DAYNAME: (args: Expression[]) =>
          new TimeToStrExpr({
            this: seqGet(args, 0),
            format: MySQL.formatTime(LiteralExpr.string('%W')),
          }),
        TIMESTAMPDIFF: (args: Expression[]) =>
          new TimestampDiffExpr({
            this: seqGet(args, 2),
            expression: seqGet(args, 1),
            unit: seqGet(args, 0),
          }),
        APPROX_COUNT_DISTINCT: HllExpr.fromArgList,
        APPROX_PERCENTILE: (args: Expression[]) =>
          new ApproxQuantileExpr({
            this: seqGet(args, 0),
            quantile: seqGet(args, 1),
            errorTolerance: seqGet(args, 2),
          }),
        VARIANCE: VariancePopExpr.fromArgList,
        INSTR: ContainsExpr.fromArgList,
        REGEXP_MATCH: (args: Expression[]) =>
          new RegexpExtractAllExpr({
            this: seqGet(args, 0),
            expression: seqGet(args, 1),
            parameters: seqGet(args, 2),
          }),
        REGEXP_SUBSTR: (args: Expression[]) =>
          new RegexpExtractExpr({
            this: seqGet(args, 0),
            expression: seqGet(args, 1),
            position: seqGet(args, 2),
            occurrence: seqGet(args, 3),
            parameters: seqGet(args, 4),
          }),
        REDUCE: (args: Expression[]) =>
          new ReduceExpr({
            initial: seqGet(args, 0),
            this: seqGet(args, 1),
            merge: seqGet(args, 2),
          }),
      };
      return functions;
    })();
  }

  @cache
  static get FUNCTION_PARSERS (): Partial<Record<string, (self: Parser) => Expression | undefined>> {
    return {
      ...MySQL.Parser.FUNCTION_PARSERS,
      JSON_AGG: (self: Parser) =>
        self.expression(JsonArrayAggExpr, {
          this: (self as SingleStoreParser).parseTerm(),
          order: (self as SingleStoreParser).parseOrder(),
        }),
    };
  }

  @cache
  static get NO_PAREN_FUNCTIONS (): Partial<Record<TokenType, typeof Expression>> {
    return {
      ...MySQL.Parser.NO_PAREN_FUNCTIONS,
      [TokenType.UTC_DATE]: UtcDateExpr,
      [TokenType.UTC_TIME]: UtcTimeExpr,
      [TokenType.UTC_TIMESTAMP]: UtcTimestampExpr,
    };
  }

  static CAST_COLUMN_OPERATORS = new Set([TokenType.COLON_GT, TokenType.NCOLON_GT]);
  @cache
  static get COLUMN_OPERATORS (): Partial<Record<TokenType, undefined | ((self: Parser, this_?: Expression, to?: Expression) => Expression)>> {
    return (() => {
      const operators = {
        ...MySQL.Parser.COLUMN_OPERATORS,
        [TokenType.COLON_GT]: (self: Parser, thisNode?: Expression, to?: Expression) =>
          self.expression(CastExpr, {
            this: thisNode,
            to,
          }),
        [TokenType.NCOLON_GT]: (self: Parser, thisNode?: Expression, to?: Expression) =>
          self.expression(TryCastExpr, {
            this: thisNode,
            to,
          }),
        [TokenType.DCOLON]: (self: Parser, thisNode?: Expression, path?: Expression) =>
          buildJsonExtractPath(JsonExtractExpr)([thisNode, LiteralExpr.string((path as IdentifierExpr).name)]),
        [TokenType.DCOLONDOLLAR]: (self: Parser, thisNode?: Expression, path?: Expression) =>
          buildJsonExtractPath(JsonExtractScalarExpr, { jsonType: 'STRING' })([thisNode, LiteralExpr.string((path as IdentifierExpr).name)]),
        [TokenType.DCOLONPERCENT]: (self: Parser, thisNode?: Expression, path?: Expression) =>
          buildJsonExtractPath(JsonExtractScalarExpr, { jsonType: 'DOUBLE' })([thisNode, LiteralExpr.string((path as IdentifierExpr).name)]),
        [TokenType.DCOLONQMARK]: (self: Parser, thisNode?: Expression, path?: Expression) =>
          self.expression(JsonExistsExpr, {
            this: thisNode,
            path: path?.name,
            fromDcolonqmark: true,
          }),
      };
      delete operators[TokenType.ARROW];
      delete operators[TokenType.DARROW];
      delete operators[TokenType.HASH_ARROW];
      delete operators[TokenType.DHASH_ARROW];
      delete operators[TokenType.PLACEHOLDER];
      return operators;
    })();
  }

  @cache
  static get SHOW_PARSERS (): Record<string, (self: Parser) => Expression> {
    return {
      ...MySQL.Parser.SHOW_PARSERS,
      'AGGREGATES': showParser('AGGREGATES'),
      'CDC EXTRACTOR POOL': showParser('CDC EXTRACTOR POOL'),
      'CREATE AGGREGATE': showParser('CREATE AGGREGATE', { target: true }),
      'CREATE PIPELINE': showParser('CREATE PIPELINE', { target: true }),
      'CREATE PROJECTION': showParser('CREATE PROJECTION', { target: true }),
      'DATABASE STATUS': showParser('DATABASE STATUS'),
      'DISTRIBUTED_PLANCACHE STATUS': showParser('DISTRIBUTED_PLANCACHE STATUS'),
      'FULLTEXT SERVICE METRICS LOCAL': showParser('FULLTEXT SERVICE METRICS LOCAL'),
      'FULLTEXT SERVICE METRICS FOR NODE': showParser('FULLTEXT SERVICE METRICS FOR NODE', { target: true }),
      'FULLTEXT SERVICE STATUS': showParser('FULLTEXT SERVICE STATUS'),
      'FUNCTIONS': showParser('FUNCTIONS'),
      'GROUPS': showParser('GROUPS'),
      'GROUPS FOR ROLE': showParser('GROUPS FOR ROLE', { target: true }),
      'GROUPS FOR USER': showParser('GROUPS FOR USER', { target: true }),
      'INDEXES': showParser('INDEX', { target: 'FROM' }),
      'KEYS': showParser('INDEX', { target: 'FROM' }),
      'LINKS': showParser('LINKS', { target: 'ON' }),
      'LOAD ERRORS': showParser('LOAD ERRORS'),
      'LOAD WARNINGS': showParser('LOAD WARNINGS'),
      'PARTITIONS': showParser('PARTITIONS', { target: 'ON' }),
      'PIPELINES': showParser('PIPELINES'),
      'PLAN': showParser('PLAN', { target: true }),
      'PLANCACHE': showParser('PLANCACHE'),
      'PROCEDURES': showParser('PROCEDURES'),
      'PROJECTIONS': showParser('PROJECTIONS', { target: 'ON TABLE' }),
      'REPLICATION STATUS': showParser('REPLICATION STATUS'),
      'REPRODUCTION': showParser('REPRODUCTION'),
      'RESOURCE POOLS': showParser('RESOURCE POOLS'),
      'ROLES': showParser('ROLES'),
      'ROLES FOR USER': showParser('ROLES FOR USER', { target: true }),
      'ROLES FOR GROUP': showParser('ROLES FOR GROUP', { target: true }),
      'STATUS EXTENDED': showParser('STATUS EXTENDED'),
      'USERS': showParser('USERS'),
      'USERS FOR ROLE': showParser('USERS FOR ROLE', { target: true }),
      'USERS FOR GROUP': showParser('USERS FOR GROUP', { target: true }),
    };
  }

  @cache
  static get ALTER_PARSERS (): Partial<Record<string, (self: Parser) => Expression | Expression[] | undefined>> {
    return {
      ...MySQL.Parser.ALTER_PARSERS,
      CHANGE: (self: Parser) =>
        self.expression(RenameColumnExpr, {
          this: (self as SingleStoreParser).parseColumn(),
          to: (self as SingleStoreParser).parseColumn(),
        }),
    };
  }

  parseVectorExpressions (expressions: Expression[]): Expression[] {
    let typeName = (expressions[1] as IdentifierExpr).name.toUpperCase();
    const aliases: Record<string, string> = (this.dialect._constructor as typeof SingleStore).VECTOR_TYPE_ALIASES;
    if (typeName in aliases) {
      typeName = aliases[typeName];
    }

    return [DataTypeExpr.build(typeName, { dialect: this.dialect })!, expressions[0]];
  }
}

class SingleStoreGenerator extends MySQL.Generator {
  static SUPPORTS_UESCAPE = false;
  static NULL_ORDERING_SUPPORTED = true;
  static MATCH_AGAINST_TABLE_PREFIX = 'TABLE ';
  static STRUCT_DELIMITER = ['(', ')'];

  static UNICODE_SUBSTITUTE = (_match: string, group: string) => {
    return String.fromCharCode(parseInt(group, 16));
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
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (self: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (self: Generator, e: any) => string>([
      ...MySQL.Generator.TRANSFORMS,
      [
        TsOrDsToDateExpr,
        (self: Generator, e: TsOrDsToDateExpr) =>
          e.args.format
            ? self.func('TO_DATE', [e.args.this, self.formatTime(e)])
            : self.func('DATE', [e.args.this]),
      ],
      [StrToTimeExpr, (self: Generator, e: StrToTimeExpr) => self.func('TO_TIMESTAMP', [e.args.this, self.formatTime(e)])],
      [ToCharExpr, (self: Generator, e: ToCharExpr) => self.func('TO_CHAR', [e.args.this, self.formatTime(e)])],
      [
        StrToDateExpr,
        (self: Generator, e: StrToDateExpr) =>
          self.func('STR_TO_DATE', [
            e.args.this,
            self.formatTime(
              e,
              MySQL.INVERSE_TIME_MAPPING,
              MySQL.INVERSE_TIME_TRIE,
            ),
          ]),
      ],
      [
        TimeToStrExpr,
        (self: Generator, e: TimeToStrExpr) =>
          self.func('DATE_FORMAT', [
            e.args.this,
            self.formatTime(
              e,
              MySQL.INVERSE_TIME_MAPPING,
              MySQL.INVERSE_TIME_TRIE,
            ),
          ]),
      ],
      [DateExpr, (self: Generator, e: DateExpr) => unsupportedArgs<DateExpr>('zone', 'expressions')((e) => renameFunc('DATE')(self, e))(e)],
      [
        CastExpr,
        (self: Generator, e: CastExpr) => unsupportedArgs<CastExpr>(
          'format',
          'action',
          'default',
        )(
          (e: CastExpr) => `${self.sql(e.args.this)} :> ${self.sql(e.args.to)}`,
        )(e),
      ],
      [
        TryCastExpr,
        (self: Generator, e: TryCastExpr) => unsupportedArgs<TryCastExpr>(
          'format',
          'action',
          'default',
        )(
          (e: TryCastExpr) => `${self.sql(e.args.this)} !:> ${self.sql(e.args.to)}`,
        )(e),
      ],
      [
        CastToStrTypeExpr,
        (self: Generator, e: CastToStrTypeExpr) =>
          self.sql(new CastExpr({
            this: e.args.this,
            to: DataTypeExpr.build(e.args.to?.name),
          })),
      ],
      [StrToUnixExpr, (self: Generator, e: StrToUnixExpr) => unsupportedArgs<StrToUnixExpr>('format')((e) => renameFunc('UNIX_TIMESTAMP')(self, e))(e)],
      [TimeToUnixExpr, renameFunc('UNIX_TIMESTAMP')],
      [TimeStrToUnixExpr, renameFunc('UNIX_TIMESTAMP')],
      [UnixSecondsExpr, renameFunc('UNIX_TIMESTAMP')],
      [
        UnixToStrExpr,
        (self: Generator, e: UnixToStrExpr) =>
          self.func('FROM_UNIXTIME', [
            e.args.this,
            self.formatTime(
              e,
              MySQL.INVERSE_TIME_MAPPING,
              MySQL.INVERSE_TIME_TRIE,
            ),
          ]),
      ],
      [
        UnixToTimeExpr,
        (self: Generator, e: UnixToTimeExpr) => unsupportedArgs<UnixToTimeExpr>(
          'scale',
          'zone',
          'hours',
          'minutes',
        )(
          (e: UnixToTimeExpr) =>
            self.func('FROM_UNIXTIME', [
              e.args.this,
              self.formatTime(
                e,
                MySQL.INVERSE_TIME_MAPPING,
                MySQL.INVERSE_TIME_TRIE,
              ),
            ]),
        )(e),
      ],
      [UnixToTimeStrExpr, (self: Generator, e: UnixToTimeStrExpr) => `FROM_UNIXTIME(${self.sql(e.args.this)}) :> TEXT`],
      [
        DateBinExpr,
        (self: Generator, e: DateBinExpr) => unsupportedArgs<DateBinExpr>('unit', 'zone')(
          (e: DateBinExpr) =>
            self.func('TIME_BUCKET', [
              e.args.this,
              e.args.expression,
              e.args.origin,
            ]),
        )(e),
      ],
      [
        TimeStrToDateExpr,
        (self: Generator, e: TimeStrToDateExpr) => self.sql(new CastExpr({
          this: e.args.this,
          to: DataTypeExpr.build(DataTypeExprKind.DATE),
        })),
      ],
      [
        FromTimeZoneExpr,
        (self: Generator, e: FromTimeZoneExpr) => self.func('CONVERT_TZ', [
          e.args.this,
          e.args.zone,
          '\'UTC\'',
        ]),
      ],
      [DiToDateExpr, (self: Generator, e: DiToDateExpr) => `STR_TO_DATE(${self.sql(e.args.this)}, ${SingleStore.DATEINT_FORMAT})`],
      [DateToDiExpr, (self: Generator, e: DateToDiExpr) => `(DATE_FORMAT(${self.sql(e.args.this)}, ${SingleStore.DATEINT_FORMAT}) :> INT)`],
      [TsOrDiToDiExpr, (self: Generator, e: TsOrDiToDiExpr) => `(DATE_FORMAT(${self.sql(e.args.this)}, ${SingleStore.DATEINT_FORMAT}) :> INT)`],
      [TimeExpr, (self: Generator, e: TimeExpr) => unsupportedArgs<TimeExpr>('zone')((e: TimeExpr) => `${self.sql(e.args.this)} :> TIME`)(e)],
      [DatetimeAddExpr, removeTsOrDsToDate(dateAddSql('ADD'))],
      [DatetimeTruncExpr, (self: Generator, e: DatetimeTruncExpr) => unsupportedArgs<DatetimeTruncExpr>('zone')((e) => timestampTruncSql()(self, e as TimestampTruncExpr))(e)],
      [DatetimeSubExpr, dateAddIntervalSql('DATE', 'SUB')],
      [DatetimeDiffExpr, timestampDiffSql],
      [DateTruncExpr, (self: Generator, e: DateTruncExpr) => unsupportedArgs<DateTruncExpr>('zone')((e) => timestampTruncSql()(self, e as TimestampTruncExpr))(e)],
      [
        DateDiffExpr,
        (self: Generator, e: DateDiffExpr) => unsupportedArgs<DateDiffExpr>('zone')(
          (e: DateDiffExpr) =>
            e.args.unit !== undefined ? timestampDiffSql(self, e) : self.func('DATEDIFF', [e.args.this, e.args.expression]),
        )(e),
      ],
      [
        TsOrDsDiffExpr,
        (self: Generator, e: TsOrDsDiffExpr) =>
          e.args.unit !== undefined ? timestampDiffSql(self, e) : self.func('DATEDIFF', [e.args.this, e.args.expression]),
      ],
      [TimestampTruncExpr, (self: Generator, e: TimestampTruncExpr) => unsupportedArgs<TimestampTruncExpr>('zone')((e) => timestampTruncSql()(self, e))(e)],
      [
        CurrentDatetimeExpr,
        (self: Generator, _e: CurrentDatetimeExpr) =>
          self.sql(castToTime6(new CurrentTimestampExpr({ this: LiteralExpr.number(6) }), DataTypeExprKind.DATETIME)),
      ],
      [
        JsonExtractExpr,
        (self: Generator, e: JsonExtractExpr) => unsupportedArgs<JsonExtractExpr>(
          'onlyJsonTypes',
          'expressions',
          'variantExtract',
          'jsonQuery',
          'option',
          'quote',
          'onCondition',
          'requiresJson',
        )((e) => jsonExtractSegments('JSON_EXTRACT_JSON')(self, e))(e),
      ],
      [JsonbExtractExpr, jsonExtractSegments('BSON_EXTRACT_BSON')],
      [JsonPathKeyExpr, jsonPathKeyOnlyName],
      [JsonPathSubscriptExpr, (self: Generator, e: JsonPathSubscriptExpr) => self.jsonPathPart(e.args.this)],
      [JsonPathRootExpr, () => ''],
      [JsonFormatExpr, (self: Generator, e: JsonFormatExpr) => unsupportedArgs<JsonFormatExpr>('options', 'isJson')((e) => renameFunc('JSON_PRETTY')(self, e))(e)],
      [
        JsonArrayAggExpr,
        (self: Generator, e: JsonArrayAggExpr) => unsupportedArgs<JsonArrayAggExpr>(
          'nullHandling',
          'returnType',
          'strict',
        )(
          (e: JsonArrayAggExpr) =>
            self.func('JSON_AGG', [e.args.this], { suffix: `${self.sql(e.args.order)})` }),
        )(e),
      ],
      [
        JsonArrayExpr,
        (self: Generator, e: JsonArrayExpr) => unsupportedArgs<JsonArrayExpr>(
          'nullHandling',
          'returnType',
          'strict',
        )((e) => renameFunc('JSON_BUILD_ARRAY')(self, e))(e),
      ],
      [JsonbExistsExpr, (self: Generator, e: JsonbExistsExpr) => self.func('BSON_MATCH_ANY_EXISTS', [e.args.this, e.args.path])],
      [
        JsonExistsExpr,
        (self: Generator, e: JsonExistsExpr) =>
          e.args.fromDcolonqmark
            ? `${self.sql(e.args.this)}::?${self.sql(e.args.path)}`
            : self.func('JSON_MATCH_ANY_EXISTS', [e.args.this, e.args.path]),
      ],
      [
        JsonObjectExpr,
        (self: Generator, e: JsonObjectExpr) => unsupportedArgs<JsonObjectExpr>(
          'nullHandling',
          'uniqueKeys',
          'returnType',
          'encoding',
        )((e) => renameFunc('JSON_BUILD_OBJECT')(self, e))(e),
      ],
      [DayOfWeekIsoExpr, (self: Generator, e: DayOfWeekIsoExpr) => `((${self.func('DAYOFWEEK', [e.args.this])} % 7) + 1)`],
      [DayOfMonthExpr, renameFunc('DAY')],
      [HllExpr, renameFunc('APPROX_COUNT_DISTINCT')],
      [ApproxDistinctExpr, renameFunc('APPROX_COUNT_DISTINCT')],
      [CountIfExpr, countIfToSum],
      [LogicalOrExpr, (self: Generator, e: LogicalOrExpr) => `MAX(ABS(${self.sql(e.args.this)}))`],
      [LogicalAndExpr, (self: Generator, e: LogicalAndExpr) => `MIN(ABS(${self.sql(e.args.this)}))`],
      [
        ApproxQuantileExpr,
        (self: Generator, e: ApproxQuantileExpr) => unsupportedArgs<ApproxQuantileExpr>('accuracy', 'weight')(
          (e: ApproxQuantileExpr) =>
            self.func('APPROX_PERCENTILE', [
              e.args.this,
              e.args.quantile,
              e.args.errorTolerance,
            ]),
        )(e),
      ],
      [VarianceExpr, renameFunc('VAR_SAMP')],
      [VariancePopExpr, renameFunc('VAR_POP')],
      [XorExpr, boolXorSql],
      [
        CbrtExpr,
        (self: Generator, e: CbrtExpr) =>
          self.sql(new PowExpr({
            this: e.args.this as Expression,
            expression: LiteralExpr.number(1).div(LiteralExpr.number(3)),
          })),
      ],
      [RegexpLikeExpr, (self: Generator, e: RegexpLikeExpr) => self.binary(e, 'RLIKE')],
      [
        RepeatExpr,
        (self: Generator, e: RepeatExpr) =>
          self.func('LPAD', [
            LiteralExpr.string(''),
            new MulExpr({
              this: self.func('LENGTH', [e.args.this]),
              expression: e.args.times?.[0],
            }),
            e.args.this,
          ]),
      ],
      [IsAsciiExpr, (self: Generator, e: IsAsciiExpr) => `(${self.sql(e.args.this)} RLIKE '^[\\x00-\\x7f]*$')`],
      [Md5DigestExpr, (self: Generator, e: Md5DigestExpr) => self.func('UNHEX', [self.func('MD5', [e.args.this])])],
      [ContainsExpr, renameFunc('INSTR')],
      [
        RegexpExtractAllExpr,
        (self: Generator, e: RegexpExtractAllExpr) => unsupportedArgs<RegexpExtractAllExpr>(
          'position',
          'occurrence',
          'group',
        )(
          (e: RegexpExtractAllExpr) =>
            self.func('REGEXP_MATCH', [
              e.args.this,
              e.args.expression,
              e.args.parameters,
            ]),
        )(e),
      ],
      [
        RegexpExtractExpr,
        (self: Generator, e: RegexpExtractExpr) => unsupportedArgs<RegexpExtractExpr>('group')(
          (e: RegexpExtractExpr) =>
            self.func('REGEXP_SUBSTR', [
              e.args.this,
              e.args.expression,
              e.args.position,
              e.args.occurrence,
              e.args.parameters,
            ]),
        )(e),
      ],
      [
        StartsWithExpr,
        (self: Generator, e: StartsWithExpr) =>
          self.func('REGEXP_INSTR', [e.args.this, self.func('CONCAT', [LiteralExpr.string('^'), e.args.expression])]),
      ],
      [
        FromBaseExpr,
        (self: Generator, e: FromBaseExpr) => self.func('CONV', [
          e.args.this,
          e.args.expression,
          LiteralExpr.number(10),
        ]),
      ],
      [
        RegexpILikeExpr,
        (self: Generator, e: RegexpILikeExpr) =>
          self.binary(
            new RegexpLikeExpr({
              this: new LowerExpr({ this: e.args.this }),
              expression: new LowerExpr({ this: e.args.expression }),
            }),
            'RLIKE',
          ),
      ],
      [
        StuffExpr,
        (self: Generator, e: StuffExpr) =>
          self.func('CONCAT', [
            self.func('SUBSTRING', [
              e.args.this,
              LiteralExpr.number(1),
              e.args.start?.sub(1),
            ]),
            e.args.expression,
            self.func('SUBSTRING', [e.args.this, e.args.start?.add(e.args.length)]),
          ]),
      ],
      [NationalExpr, (self: Generator, e: NationalExpr) => (self as SingleStoreGenerator).nationalSql(e, { prefix: '' })],
      [
        ReduceExpr,
        (self: Generator, e: ReduceExpr) => unsupportedArgs<ReduceExpr>('finish')(
          (e: ReduceExpr) => self.func('REDUCE', [
            e.args.initial,
            e.args.this,
            e.args.merge,
          ]),
        )(e),
      ],
      [
        MatchAgainstExpr,
        (self: Generator, e: MatchAgainstExpr) => unsupportedArgs<MatchAgainstExpr>('modifier')(
          (e: MatchAgainstExpr) => self.matchAgainstSql(e),
        )(e),
      ],
      [
        ShowExpr,
        (self: Generator, e: ShowExpr) => unsupportedArgs<ShowExpr>(
          'history',
          'terse',
          'offset',
          'startsWith',
          'limit',
          'from',
          'scope',
          'scopeKind',
          'mutex',
          'query',
          'channel',
          'log',
          'types',
          'privileges',
        )((e: ShowExpr) => self.showSql(e))(e),
      ],
      [
        DescribeExpr,
        (self: Generator, e: DescribeExpr) => unsupportedArgs<DescribeExpr>(
          'style',
          'kind',
          'expressions',
          'partition',
          'format',
        )((e: DescribeExpr) => self.describeSql(e))(e),
      ],
    ]);
    transforms.delete(JsonExtractScalarExpr);
    transforms.delete(CurrentDateExpr);
    return transforms;
  }

  @cache
  static get UNSUPPORTED_TYPES (): Set<DataTypeExprKind> {
    return new Set([
      DataTypeExprKind.ARRAY,
      DataTypeExprKind.AGGREGATEFUNCTION,
      DataTypeExprKind.SIMPLEAGGREGATEFUNCTION,
      DataTypeExprKind.BIGSERIAL,
      DataTypeExprKind.BPCHAR,
      DataTypeExprKind.DATEMULTIRANGE,
      DataTypeExprKind.DATERANGE,
      DataTypeExprKind.DYNAMIC,
      DataTypeExprKind.HLLSKETCH,
      DataTypeExprKind.HSTORE,
      DataTypeExprKind.IMAGE,
      DataTypeExprKind.INET,
      DataTypeExprKind.INT128,
      DataTypeExprKind.INT256,
      DataTypeExprKind.INT4MULTIRANGE,
      DataTypeExprKind.INT4RANGE,
      DataTypeExprKind.INT8MULTIRANGE,
      DataTypeExprKind.INT8RANGE,
      DataTypeExprKind.INTERVAL,
      DataTypeExprKind.IPADDRESS,
      DataTypeExprKind.IPPREFIX,
      DataTypeExprKind.IPV4,
      DataTypeExprKind.IPV6,
      DataTypeExprKind.LIST,
      DataTypeExprKind.MAP,
      DataTypeExprKind.LOWCARDINALITY,
      DataTypeExprKind.MONEY,
      DataTypeExprKind.MULTILINESTRING,
      DataTypeExprKind.NAME,
      DataTypeExprKind.NESTED,
      DataTypeExprKind.NOTHING,
      DataTypeExprKind.NULL,
      DataTypeExprKind.NUMMULTIRANGE,
      DataTypeExprKind.NUMRANGE,
      DataTypeExprKind.OBJECT,
      DataTypeExprKind.RANGE,
      DataTypeExprKind.ROWVERSION,
      DataTypeExprKind.SERIAL,
      DataTypeExprKind.SMALLSERIAL,
      DataTypeExprKind.SMALLMONEY,
      DataTypeExprKind.SUPER,
      DataTypeExprKind.TIMETZ,
      DataTypeExprKind.TIMESTAMPNTZ,
      DataTypeExprKind.TIMESTAMPLTZ,
      DataTypeExprKind.TIMESTAMPTZ,
      DataTypeExprKind.TIMESTAMP_NS,
      DataTypeExprKind.TSMULTIRANGE,
      DataTypeExprKind.TSRANGE,
      DataTypeExprKind.TSTZMULTIRANGE,
      DataTypeExprKind.TSTZRANGE,
      DataTypeExprKind.UINT128,
      DataTypeExprKind.UINT256,
      DataTypeExprKind.UNION,
      DataTypeExprKind.UNKNOWN,
      DataTypeExprKind.USERDEFINED,
      DataTypeExprKind.UUID,
      DataTypeExprKind.VARIANT,
      DataTypeExprKind.XML,
      DataTypeExprKind.TDIGEST,
    ]);
  }

  @cache
  static get TYPE_MAPPING () {
    return {
      ...MySQL.Generator.TYPE_MAPPING,
      [DataTypeExprKind.BIGDECIMAL]: 'DECIMAL',
      [DataTypeExprKind.BIT]: 'BOOLEAN',
      [DataTypeExprKind.DATE32]: 'DATE',
      [DataTypeExprKind.DATETIME64]: 'DATETIME',
      [DataTypeExprKind.DECIMAL32]: 'DECIMAL',
      [DataTypeExprKind.DECIMAL64]: 'DECIMAL',
      [DataTypeExprKind.DECIMAL128]: 'DECIMAL',
      [DataTypeExprKind.DECIMAL256]: 'DECIMAL',
      [DataTypeExprKind.ENUM8]: 'ENUM',
      [DataTypeExprKind.ENUM16]: 'ENUM',
      [DataTypeExprKind.FIXEDSTRING]: 'TEXT',
      [DataTypeExprKind.GEOMETRY]: 'GEOGRAPHY',
      [DataTypeExprKind.POINT]: 'GEOGRAPHYPOINT',
      [DataTypeExprKind.RING]: 'GEOGRAPHY',
      [DataTypeExprKind.LINESTRING]: 'GEOGRAPHY',
      [DataTypeExprKind.POLYGON]: 'GEOGRAPHY',
      [DataTypeExprKind.MULTIPOLYGON]: 'GEOGRAPHY',
      [DataTypeExprKind.STRUCT]: 'RECORD',
      [DataTypeExprKind.JSONB]: 'BSON',
      [DataTypeExprKind.TIMESTAMP]: 'TIMESTAMP',
      [DataTypeExprKind.TIMESTAMP_S]: 'TIMESTAMP',
      [DataTypeExprKind.TIMESTAMP_MS]: 'TIMESTAMP(6)',
    };
  }

  static RESERVED_KEYWORDS = new Set([
    'abs',
    'absolute',
    'access',
    'account',
    'acos',
    'action',
    'add',
    'adddate',
    'addtime',
    'admin',
    'aes_decrypt',
    'aes_encrypt',
    'after',
    'against',
    'aggregate',
    'aggregates',
    'aggregator',
    'aggregator_id',
    'aggregator_plan_hash',
    'aggregators',
    'algorithm',
    'all',
    'also',
    'alter',
    'always',
    'analyse',
    'analyze',
    'and',
    'anti_join',
    'any',
    'any_value',
    'approx_count_distinct',
    'approx_count_distinct_accumulate',
    'approx_count_distinct_combine',
    'approx_count_distinct_estimate',
    'approx_geography_intersects',
    'approx_percentile',
    'arghistory',
    'arrange',
    'arrangement',
    'array',
    'as',
    'asc',
    'ascii',
    'asensitive',
    'asin',
    'asm',
    'assertion',
    'assignment',
    'ast',
    'asymmetric',
    'async',
    'at',
    'atan',
    'atan2',
    'attach',
    'attribute',
    'authorization',
    'auto',
    'auto_increment',
    'auto_reprovision',
    'autostats',
    'autostats_cardinality_mode',
    'autostats_enabled',
    'autostats_histogram_mode',
    'autostats_sampling',
    'availability',
    'avg',
    'avg_row_length',
    'avro',
    'azure',
    'background',
    '_background_threads_for_cleanup',
    'backup',
    'backup_history',
    'backup_id',
    'backward',
    'batch',
    'batches',
    'batch_interval',
    '_batch_size_limit',
    'before',
    'begin',
    'between',
    'bigint',
    'bin',
    'binary',
    '_binary',
    'bit',
    'bit_and',
    'bit_count',
    'bit_or',
    'bit_xor',
    'blob',
    'bool',
    'boolean',
    'bootstrap',
    'both',
    '_bt',
    'btree',
    'bucket_count',
    'by',
    'byte',
    'byte_length',
    'cache',
    'call',
    'call_for_pipeline',
    'called',
    'capture',
    'cascade',
    'cascaded',
    'case',
    'cast',
    'catalog',
    'ceil',
    'ceiling',
    'chain',
    'change',
    'char',
    'character',
    'characteristics',
    'character_length',
    'char_length',
    'charset',
    'check',
    'checkpoint',
    '_check_can_connect',
    '_check_consistency',
    'checksum',
    '_checksum',
    'class',
    'clear',
    'client',
    'client_found_rows',
    'close',
    'cluster',
    'clustered',
    'cnf',
    'coalesce',
    'coercibility',
    'collate',
    'collation',
    'collect',
    'column',
    'columnar',
    'columns',
    'columnstore',
    'columnstore_segment_rows',
    'comment',
    'comments',
    'commit',
    'committed',
    '_commit_log_tail',
    'compact',
    'compile',
    'compressed',
    'compression',
    'concat',
    'concat_ws',
    'concurrent',
    'concurrently',
    'condition',
    'configuration',
    'connection',
    'connection_id',
    'connections',
    'config',
    'constraint',
    'constraints',
    'content',
    'continue',
    '_continue_replay',
    'conv',
    'conversion',
    'convert',
    'convert_tz',
    'copy',
    '_core',
    'cos',
    'cost',
    'cot',
    'count',
    'create',
    'credentials',
    'cross',
    'cube',
    'csv',
    'cume_dist',
    'curdate',
    'current',
    'current_catalog',
    'current_date',
    'current_role',
    'current_schema',
    'current_security_groups',
    'current_security_roles',
    'current_time',
    'current_timestamp',
    'current_user',
    'cursor',
    'curtime',
    'cycle',
    'data',
    'database',
    'databases',
    'date',
    'date_add',
    'datediff',
    'date_format',
    'date_sub',
    'date_trunc',
    'datetime',
    'day',
    'day_hour',
    'day_microsecond',
    'day_minute',
    'dayname',
    'dayofmonth',
    'dayofweek',
    'dayofyear',
    'day_second',
    'deallocate',
    'dec',
    'decimal',
    'declare',
    'decode',
    'default',
    'defaults',
    'deferrable',
    'deferred',
    'defined',
    'definer',
    'degrees',
    'delayed',
    'delay_key_write',
    'delete',
    'delimiter',
    'delimiters',
    'dense_rank',
    'desc',
    'describe',
    'detach',
    'deterministic',
    'dictionary',
    'differential',
    'directory',
    'disable',
    'discard',
    '_disconnect',
    'disk',
    'distinct',
    'distinctrow',
    'distributed_joins',
    'div',
    'do',
    'document',
    'domain',
    'dot_product',
    'double',
    'drop',
    '_drop_profile',
    'dual',
    'dump',
    'duplicate',
    'dynamic',
    'earliest',
    'each',
    'echo',
    'election',
    'else',
    'elseif',
    'elt',
    'enable',
    'enclosed',
    'encoding',
    'encrypted',
    'end',
    'engine',
    'engines',
    'enum',
    'errors',
    'escape',
    'escaped',
    'estimate',
    'euclidean_distance',
    'event',
    'events',
    'except',
    'exclude',
    'excluding',
    'exclusive',
    'execute',
    'exists',
    'exit',
    'exp',
    'explain',
    'extended',
    'extension',
    'external',
    'external_host',
    'external_port',
    'extract',
    'extractor',
    'extractors',
    'extra_join',
    '_failover',
    'failed_login_attempts',
    'failure',
    'false',
    'family',
    'fault',
    'fetch',
    'field',
    'fields',
    'file',
    'files',
    'fill',
    'first',
    'first_value',
    'fix_alter',
    'fixed',
    'float',
    'float4',
    'float8',
    'floor',
    'flush',
    'following',
    'for',
    'force',
    'force_compiled_mode',
    'force_interpreter_mode',
    'foreground',
    'foreign',
    'format',
    'forward',
    'found_rows',
    'freeze',
    'from',
    'from_base64',
    'from_days',
    'from_unixtime',
    'fs',
    '_fsync',
    'full',
    'fulltext',
    'function',
    'functions',
    'gc',
    'gcs',
    'get_format',
    '_gc',
    '_gcx',
    'generate',
    'geography',
    'geography_area',
    'geography_contains',
    'geography_distance',
    'geography_intersects',
    'geography_latitude',
    'geography_length',
    'geography_longitude',
    'geographypoint',
    'geography_point',
    'geography_within_distance',
    'geometry',
    'geometry_area',
    'geometry_contains',
    'geometry_distance',
    'geometry_filter',
    'geometry_intersects',
    'geometry_length',
    'geometrypoint',
    'geometry_point',
    'geometry_within_distance',
    'geometry_x',
    'geometry_y',
    'global',
    '_global_version_timestamp',
    'grant',
    'granted',
    'grants',
    'greatest',
    'group',
    'grouping',
    'groups',
    'group_concat',
    'gzip',
    'handle',
    'handler',
    'hard_cpu_limit_percentage',
    'hash',
    'has_temp_tables',
    'having',
    'hdfs',
    'header',
    'heartbeat_no_logging',
    'hex',
    'highlight',
    'high_priority',
    'hold',
    'holding',
    'host',
    'hosts',
    'hour',
    'hour_microsecond',
    'hour_minute',
    'hour_second',
    'identified',
    'identity',
    'if',
    'ifnull',
    'ignore',
    'ifnull',
    'ilike',
    'immediate',
    'immutable',
    'implicit',
    'import',
    'in',
    'including',
    'increment',
    'incremental',
    'index',
    'indexes',
    'inet_aton',
    'inet_ntoa',
    'inet6_aton',
    'inet6_ntoa',
    'infile',
    'inherit',
    'inherits',
    '_init_profile',
    'init',
    'initcap',
    'initialize',
    'initially',
    'inject',
    'inline',
    'inner',
    'inout',
    'input',
    'insensitive',
    'insert',
    'insert_method',
    'instance',
    'instead',
    'instr',
    'int',
    'int1',
    'int2',
    'int3',
    'int4',
    'int8',
    'integer',
    '_internal_dynamic_typecast',
    'interpreter_mode',
    'intersect',
    'interval',
    'into',
    'invoker',
    'is',
    'isnull',
    'isolation',
    'iterate',
    'join',
    'json',
    'json_agg',
    'json_array_contains_double',
    'json_array_contains_json',
    'json_array_contains_string',
    'json_array_push_double',
    'json_array_push_json',
    'json_array_push_string',
    'json_delete_key',
    'json_extract_double',
    'json_extract_json',
    'json_extract_string',
    'json_extract_bigint',
    'json_get_type',
    'json_length',
    'json_set_double',
    'json_set_json',
    'json_set_string',
    'json_splice_double',
    'json_splice_json',
    'json_splice_string',
    'kafka',
    'key',
    'key_block_size',
    'keys',
    'kill',
    'killall',
    'label',
    'lag',
    'language',
    'large',
    'last',
    'last_day',
    'last_insert_id',
    'last_value',
    'lateral',
    'latest',
    'lc_collate',
    'lc_ctype',
    'lcase',
    'lead',
    'leading',
    'leaf',
    'leakproof',
    'least',
    'leave',
    'leaves',
    'left',
    'length',
    'level',
    'license',
    'like',
    'limit',
    'lines',
    'listen',
    'llvm',
    'ln',
    'load',
    'loaddata_where',
    '_load',
    'local',
    'localtime',
    'localtimestamp',
    'locate',
    'location',
    'lock',
    'log',
    'log10',
    'log2',
    'long',
    'longblob',
    'longtext',
    'loop',
    'lower',
    'low_priority',
    'lpad',
    '_ls',
    'ltrim',
    'lz4',
    'management',
    '_management_thread',
    'mapping',
    'master',
    'match',
    'materialized',
    'max',
    'maxvalue',
    'max_concurrency',
    'max_errors',
    'max_partitions_per_batch',
    'max_queue_depth',
    'max_retries_per_batch_partition',
    'max_rows',
    'mbc',
    'md5',
    'mpl',
    'median',
    'mediumblob',
    'mediumint',
    'mediumtext',
    'member',
    'memory',
    'memory_percentage',
    '_memsql_table_id_lookup',
    'memsql',
    'memsql_deserialize',
    'memsql_imitating_kafka',
    'memsql_serialize',
    'merge',
    'metadata',
    'microsecond',
    'middleint',
    'min',
    'min_rows',
    'minus',
    'minute',
    'minute_microsecond',
    'minute_second',
    'minvalue',
    'mod',
    'mode',
    'model',
    'modifies',
    'modify',
    'month',
    'monthname',
    'months_between',
    'move',
    'mpl',
    'names',
    'named',
    'namespace',
    'national',
    'natural',
    'nchar',
    'next',
    'no',
    'node',
    'none',
    'no_query_rewrite',
    'noparam',
    'not',
    'nothing',
    'notify',
    'now',
    'nowait',
    'no_write_to_binlog',
    'norely',
    'nth_value',
    'ntile',
    'null',
    'nullcols',
    'nullif',
    'nulls',
    'numeric',
    'nvarchar',
    'object',
    'octet_length',
    'of',
    'off',
    'offline',
    'offset',
    'offsets',
    'oids',
    'on',
    'online',
    'only',
    'open',
    'operator',
    'optimization',
    'optimize',
    'optimizer',
    'optimizer_state',
    'option',
    'options',
    'optionally',
    'or',
    'order',
    'ordered_serialize',
    'orphan',
    'out',
    'out_of_order',
    'outer',
    'outfile',
    'over',
    'overlaps',
    'overlay',
    'owned',
    'owner',
    'pack_keys',
    'paired',
    'parser',
    'parquet',
    'partial',
    'partition',
    'partition_id',
    'partitioning',
    'partitions',
    'passing',
    'password',
    'password_lock_time',
    'pause',
    '_pause_replay',
    'percent_rank',
    'percentile_cont',
    'percentile_disc',
    'periodic',
    'persisted',
    'pi',
    'pipeline',
    'pipelines',
    'pivot',
    'placing',
    'plan',
    'plans',
    'plancache',
    'plugins',
    'pool',
    'pools',
    'port',
    'position',
    'pow',
    'power',
    'preceding',
    'precision',
    'prepare',
    'prepared',
    'preserve',
    'primary',
    'prior',
    'privileges',
    'procedural',
    'procedure',
    'procedures',
    'process',
    'processlist',
    'profile',
    'profiles',
    'program',
    'promote',
    'proxy',
    'purge',
    'quarter',
    'queries',
    'query',
    'query_timeout',
    'queue',
    'quote',
    'radians',
    'rand',
    'range',
    'rank',
    'read',
    '_read',
    'reads',
    'real',
    'reassign',
    'rebalance',
    'recheck',
    'record',
    'recursive',
    'redundancy',
    'redundant',
    'ref',
    'reference',
    'references',
    'refresh',
    'regexp',
    'reindex',
    'relative',
    'release',
    'reload',
    'rely',
    'remote',
    'remove',
    'rename',
    'repair',
    '_repair_table',
    'repeat',
    'repeatable',
    '_repl',
    '_reprovisioning',
    'replace',
    'replica',
    'replicate',
    'replicating',
    'replication',
    'durability',
    'require',
    'resource',
    'resource_pool',
    'reset',
    'restart',
    'restore',
    'restrict',
    'result',
    '_resurrect',
    'retry',
    'return',
    'returning',
    'returns',
    'reverse',
    'revoke',
    'rg_pool',
    'right',
    'right_anti_join',
    'right_semi_join',
    'right_straight_join',
    'rlike',
    'role',
    'roles',
    'rollback',
    'rollup',
    'round',
    'routine',
    'row',
    'row_count',
    'row_format',
    'row_number',
    'rows',
    'rowstore',
    'rule',
    'rpad',
    '_rpc',
    'rtrim',
    'running',
    's3',
    'safe',
    'save',
    'savepoint',
    'scalar',
    'schema',
    'schemas',
    'schema_binding',
    'scroll',
    'search',
    'second',
    'second_microsecond',
    'sec_to_time',
    'security',
    'select',
    'semi_join',
    '_send_threads',
    'sensitive',
    'separator',
    'sequence',
    'sequences',
    'serial',
    'serializable',
    'series',
    'service_user',
    'server',
    'session',
    'session_user',
    'set',
    'setof',
    'security_lists_intersect',
    'sha',
    'sha1',
    'sha2',
    'shard',
    'sharded',
    'sharded_id',
    'share',
    'show',
    'shutdown',
    'sigmoid',
    'sign',
    'signal',
    'similar',
    'simple',
    'site',
    'signed',
    'sin',
    'skip',
    'skipped_batches',
    'sleep',
    '_sleep',
    'smallint',
    'snapshot',
    '_snapshot',
    '_snapshots',
    'soft_cpu_limit_percentage',
    'some',
    'soname',
    'sparse',
    'spatial',
    'spatial_check_index',
    'specific',
    'split',
    'sql',
    'sql_big_result',
    'sql_buffer_result',
    'sql_cache',
    'sql_calc_found_rows',
    'sqlexception',
    'sql_mode',
    'sql_no_cache',
    'sql_no_logging',
    'sql_small_result',
    'sqlstate',
    'sqlwarning',
    'sqrt',
    'ssl',
    'stable',
    'standalone',
    'start',
    'starting',
    'state',
    'statement',
    'statistics',
    'stats',
    'status',
    'std',
    'stddev',
    'stddev_pop',
    'stddev_samp',
    'stdin',
    'stdout',
    'stop',
    'storage',
    'str_to_date',
    'straight_join',
    'strict',
    'string',
    'strip',
    'subdate',
    'substr',
    'substring',
    'substring_index',
    'success',
    'sum',
    'super',
    'symmetric',
    'sync_snapshot',
    'sync',
    '_sync',
    '_sync2',
    '_sync_partitions',
    '_sync_snapshot',
    'synchronize',
    'sysid',
    'system',
    'table',
    'table_checksum',
    'tables',
    'tablespace',
    'tags',
    'tan',
    'target_size',
    'task',
    'temp',
    'template',
    'temporary',
    'temptable',
    '_term_bump',
    'terminate',
    'terminated',
    'test',
    'text',
    'then',
    'time',
    'timediff',
    'time_bucket',
    'time_format',
    'timeout',
    'timestamp',
    'timestampadd',
    'timestampdiff',
    'timezone',
    'time_to_sec',
    'tinyblob',
    'tinyint',
    'tinytext',
    'to',
    'to_base64',
    'to_char',
    'to_date',
    'to_days',
    'to_json',
    'to_number',
    'to_seconds',
    'to_timestamp',
    'tracelogs',
    'traditional',
    'trailing',
    'transform',
    'transaction',
    '_transactions_experimental',
    'treat',
    'trigger',
    'triggers',
    'trim',
    'true',
    'trunc',
    'truncate',
    'trusted',
    'two_phase',
    '_twopcid',
    'type',
    'types',
    'ucase',
    'unbounded',
    'uncommitted',
    'undefined',
    'undo',
    'unencrypted',
    'unenforced',
    'unhex',
    'unhold',
    'unicode',
    'union',
    'unique',
    '_unittest',
    'unix_timestamp',
    'unknown',
    'unlisten',
    '_unload',
    'unlock',
    'unlogged',
    'unpivot',
    'unsigned',
    'until',
    'update',
    'upgrade',
    'upper',
    'usage',
    'use',
    'user',
    'users',
    'using',
    'utc_date',
    'utc_time',
    'utc_timestamp',
    '_utf8',
    'vacuum',
    'valid',
    'validate',
    'validator',
    'value',
    'values',
    'varbinary',
    'varchar',
    'varcharacter',
    'variables',
    'variadic',
    'variance',
    'var_pop',
    'var_samp',
    'varying',
    'vector_sub',
    'verbose',
    'version',
    'view',
    'void',
    'volatile',
    'voting',
    'wait',
    '_wake',
    'warnings',
    'week',
    'weekday',
    'weekofyear',
    'when',
    'where',
    'while',
    'whitespace',
    'window',
    'with',
    'without',
    'within',
    '_wm_heartbeat',
    'work',
    'workload',
    'wrapper',
    'write',
    'xact_id',
    'xor',
    'year',
    'year_month',
    'yes',
    'zerofill',
    'zone',
  ]);

  jsonExtractScalarSql (expression: JsonExtractScalarExpr): string {
    const jsonType = expression.args.jsonType;
    const funcName = jsonType === undefined ? 'JSON_EXTRACT_JSON' : `JSON_EXTRACT_${jsonType}`;
    return jsonExtractSegments(funcName)(this, expression);
  }

  jsonbExtractScalarSql (expression: JsonbExtractScalarExpr): string {
    const jsonType = expression.args.jsonType;
    const funcName = jsonType === undefined ? 'BSON_EXTRACT_BSON' : `BSON_EXTRACT_${jsonType}`;
    return jsonExtractSegments(funcName)(this, expression);
  }

  jsonExtractArraySql (expression: JsonExtractArrayExpr): string {
    this.unsupported('Arrays are not supported in SingleStore');
    return this.functionFallbackSql(expression);
  }

  @unsupportedArgs('onCondition')
  jsonValueSql (expression: JsonValueExpr): string {
    let res: Expression = new JsonExtractScalarExpr({
      this: expression.args.this,
      expression: expression.args.path,
      jsonType: 'STRING',
    });

    const returning = expression.args.returning;
    if (returning !== undefined) {
      res = new CastExpr({
        this: res,
        to: returning,
      });
    }

    return this.sql(res);
  }

  allSql (expression: AllExpr): string {
    this.unsupported('ALL subquery predicate is not supported in SingleStore');
    return super.allSql(expression);
  }

  jsonArrayContainsSql (expression: JsonArrayContainsExpr): string {
    const jsonType = expression.text('jsonType').toUpperCase();

    if (jsonType) {
      return this.func(`JSON_ARRAY_CONTAINS_${jsonType}`, [expression.args.expression, expression.args.this]);
    }

    return this.func('JSON_ARRAY_CONTAINS_JSON', [expression.args.expression, this.func('TO_JSON', [expression.args.this])]);
  }

  @unsupportedArgs('kind', 'values')
  dataTypeSql (expression: DataTypeExpr): string {
    if (expression.args.nested && !expression.isType(DataTypeExprKind.STRUCT)) {
      this.unsupported(
        `Argument 'nested' is not supported for representation of '${expression.args.this}' in SingleStore`,
      );
    }

    if (expression.isType(DataTypeExprKind.VARBINARY) && !expression.args.expressions?.length) {
      // VARBINARY must always have a size - if it doesn't, we always generate BLOB
      return 'BLOB';
    }

    if (
      expression.isType([
        DataTypeExprKind.DECIMAL32,
        DataTypeExprKind.DECIMAL64,
        DataTypeExprKind.DECIMAL128,
        DataTypeExprKind.DECIMAL256,
      ])
    ) {
      const scale = this.expressions(expression, { flat: true });
      let precision: string;

      if (expression.isType(DataTypeExprKind.DECIMAL32)) {
        precision = '9';
      } else if (expression.isType(DataTypeExprKind.DECIMAL64)) {
        precision = '18';
      } else if (expression.isType(DataTypeExprKind.DECIMAL128)) {
        precision = '38';
      } else {
        // 65 is a maximum precision supported in SingleStore
        precision = '65';
      }

      return scale ? `DECIMAL(${precision}, ${scale[0]})` : `DECIMAL(${precision})`;
    }

    if (expression.isType(DataTypeExprKind.VECTOR)) {
      const expressions = expression.args.expressions || [];
      if (expressions.length === 2) {
        let typeName = this.sql(expressions[0]);
        const inverseAliases = (this.dialect.constructor as typeof SingleStore).INVERSE_VECTOR_TYPE_ALIASES;
        if (typeName in inverseAliases) {
          typeName = inverseAliases[typeName];
        }

        return `VECTOR(${this.sql(expressions[1])}, ${typeName})`;
      }
    }

    return super.dataTypeSql(expression);
  }

  // SingleStore does not support setting a collation for column in the SELECT query,
  // so we cast column to a LONGTEXT type with specific collation
  collateSql (expression: CollateExpr): string {
    return this.binary(expression, ':> LONGTEXT COLLATE');
  }

  currentDateSql (expression: CurrentDateExpr): string {
    const timezone = expression.args.this;
    if (timezone) {
      if (timezone instanceof LiteralExpr && timezone.args.this?.toLowerCase() === 'utc') {
        return this.func('UTC_DATE', []);
      }
      this.unsupported('CurrentDate with timezone is not supported in SingleStore');
    }

    return this.func('CURRENT_DATE', []);
  }

  currentTimeSql (expression: CurrentTimeExpr): string {
    const arg = expression.args.this;
    if (arg) {
      if (arg instanceof LiteralExpr && arg.args.this?.toLowerCase() === 'utc') {
        return this.func('UTC_TIME', []);
      }
      if (arg instanceof LiteralExpr && arg.isNumber) {
        return this.func('CURRENT_TIME', [arg]);
      }
      this.unsupported('CurrentTime with timezone is not supported in SingleStore');
    }

    return this.func('CURRENT_TIME', []);
  }

  currentTimestampSql (expression: CurrentTimestampExpr): string {
    const arg = expression.args.this;
    if (arg) {
      if (arg instanceof LiteralExpr && arg.args.this?.toLowerCase() === 'utc') {
        return this.func('UTC_TIMESTAMP', []);
      }
      if (arg instanceof LiteralExpr && arg.isNumber) {
        return this.func('CURRENT_TIMESTAMP', [arg]);
      }
      this.unsupported('CurrentTimestamp with timezone is not supported in SingleStore');
    }

    return this.func('CURRENT_TIMESTAMP', []);
  }

  standardHashSql (expression: StandardHashExpr): string {
    const hashFunction = expression.args.expression;
    if (hashFunction === undefined) {
      return this.func('SHA', [expression.args.this]);
    }
    if (hashFunction instanceof LiteralExpr) {
      const name = hashFunction.args.this?.toLowerCase();
      if (name === 'sha') {
        return this.func('SHA', [expression.args.this]);
      }
      if (name === 'md5') {
        return this.func('MD5', [expression.args.this]);
      }

      this.unsupported(`${hashFunction.args.this} hash method is not supported in SingleStore`);
      return this.func('SHA', [expression.args.this]);
    }

    this.unsupported('STANDARD_HASH function is not supported in SingleStore');
    return this.func('SHA', [expression.args.this]);
  }

  @unsupportedArgs(
    'isDatabase',
    'exists',
    'cluster',
    'identity',
    'option',
    'partition',
  )
  truncateTableSql (expression: TruncateTableExpr): string {
    const statements: string[] = [];
    for (const expr of expression.args.expressions ?? []) {
      statements.push(`TRUNCATE ${this.sql(expr)}`);
    }

    return statements.join('; ');
  }

  @unsupportedArgs('exists')
  renameColumnSql (expression: RenameColumnExpr): string {
    const oldColumn = this.sql(expression, 'this');
    const newColumn = this.sql(expression, 'to');
    return `CHANGE ${oldColumn} ${newColumn}`;
  }

  @unsupportedArgs(
    'drop',
    'comment',
    'allowNull',
    'visible',
    'using',
  )
  alterColumnSql (expression: AlterColumnExpr): string {
    const alter = super.alterColumnSql(expression);

    const collate = this.sql(expression, 'collate');
    const collatePart = collate ? ` COLLATE ${collate}` : '';
    return `${alter}${collatePart}`;
  }

  computedColumnConstraintSql (expression: ComputedColumnConstraintExpr): string {
    const thisSql = this.sql(expression, 'this');
    const notNull = expression.args.notNull ? ' NOT NULL' : '';
    const type = this.sql(expression, 'dataType') || 'AUTO';
    return `AS ${thisSql} PERSISTED ${type}${notNull}`;
  }
}

export class SingleStore extends MySQL {
  static SUPPORTS_ORDER_BY_ALL = true;

  static TIME_MAPPING: Record<string, string> = {
    D: '%u',
    DD: '%d',
    DY: '%a',
    HH: '%I',
    HH12: '%I',
    HH24: '%H',
    MI: '%M',
    MM: '%m',
    MON: '%b',
    MONTH: '%B',
    SS: '%S',
    RR: '%y',
    YY: '%y',
    YYYY: '%Y',
    FF6: '%f',
  };

  static VECTOR_TYPE_ALIASES = {
    I8: 'TINYINT',
    I16: 'SMALLINT',
    I32: 'INT',
    I64: 'BIGINT',
    F32: 'FLOAT',
    F64: 'DOUBLE',
  };

  @cache
  static get INVERSE_VECTOR_TYPE_ALIASES () {
    return Object.fromEntries(
      Object.entries(SingleStore.VECTOR_TYPE_ALIASES).map(([k, v]) => [v, k]),
    );
  }

  static Tokenizer = SingleStoreTokenizer;
  static Parser = SingleStoreParser;
  static Generator = SingleStoreGenerator;
}

Dialect.register(Dialects.SINGLESTORE, SingleStore);
