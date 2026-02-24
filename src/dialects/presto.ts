import type {
  GroupConcatExpr,
  CreateExpr,
  DeleteExpr,
  TransactionExpr,
  JsonExtractExpr,
  IntervalExpr,
  StrToUnixExpr,
  Md5Expr,
  ExtractExpr,
} from '../expressions';
import {
  Expression,
  ColumnExpr,
  DotExpr,
  DataTypeExprKind, LocationPropertyExpr, PropertyEqExpr, StructExpr,
  SchemaExpr,
  PartitionedByPropertyExpr,
  IdentifierExpr,
  ArrayExpr,
  LiteralExpr,
  ColumnDefExpr,
  PropertyExpr,
  BooleanExpr,
  FuncExpr,
  CastExpr,
  TableExpr,
  JsonPathKeyExpr,
  CreateExprKind,
  EncodeExpr,
  DataTypeExpr,
  MulExpr,
  TimeToUnixExpr,
  TimeStrToDateExpr,
  TimestampTruncExpr,
  TimeStrToUnixExpr,
  TimeStrToTimeExpr,
  TimestampAddExpr,
  TimestampExpr,
  RegexpReplaceExpr,
  LevenshteinExpr,
  CurrentTimestampExpr,
  RegexpExtractExpr,
  GenerateSeriesExpr,
  ArrayUniqueAggExpr,
  StrToMapExpr,
  StrPositionExpr,
  RegexpExtractAllExpr,
  ArraySliceExpr,
  Md5DigestExpr,
  Sha2Expr,
  WeekOfYearExpr,
  DateDiffExpr,
  TimeToStrExpr,
  DayOfWeekIsoExpr,
  DayOfYearExpr,
  UnhexExpr,
  DecodeExpr,
  AnyValueExpr,
  ApproxDistinctExpr,
  BitwiseAndExpr,
  BitwiseNotExpr,
  BitwiseOrExpr,
  BitwiseXorExpr,
  ArraySizeExpr,
  ArrayContainsExpr,
  DateAddExpr,
  PropertiesLocation,
  InitcapExpr,
  SortArrayExpr,
  QuantileExpr,
  StrToDateExpr,
  StrToTimeExpr,
  TsOrDsToDateExpr,
  BracketExpr,
  JsonFormatExpr,
  VolatilePropertyExpr,
  ApproxQuantileExpr,
  ArgMaxExpr,
  ArgMinExpr,
  ArrayAnyExpr,
  ArrayConcatExpr,
  ArrayToStringExpr,
  AtTimeZoneExpr,
  BitwiseLeftShiftExpr,
  BitwiseRightShiftExpr,
  CurrentTimeExpr,
  CurrentUserExpr,
  DateStrToDateExpr,
  DateToDiExpr,
  DateSubExpr,
  DayOfWeekExpr,
  DiToDateExpr,
  FileFormatPropertyExpr,
  FirstExpr,
  FromTimeZoneExpr,
  SelectExpr,
  SchemaCommentPropertyExpr,
  RightExpr,
  StructExtractExpr,
  PivotExpr,
  LogicalAndExpr,
  LogicalOrExpr,
  GenerateDateArrayExpr,
  IfExpr,
  ILikeExpr,
  LastExpr,
  LastDayExpr,
  LateralExpr,
  LeftExpr,
  Sha2DigestExpr,
  Sha1DigestExpr,
  ShaExpr,
  XorExpr,
  TruncExpr,
  WithinGroupExpr,
  WithExpr,
  VariancePopExpr,
  UnixToTimeStrExpr,
  UnixToTimeExpr,
  UnixToStrExpr,
  TsOrDsDiffExpr,
  TsOrDsAddExpr,
  TsOrDiToDiExpr,
  TryCastExpr,
  ToCharExpr,
  InlineExpr,
  ExplodeExpr,
  TableAliasExpr,
  MatchRecognizeExpr,
  isType,
} from '../expressions';
import {
  annotateTypes, findAllInScope,
} from '../optimizer';
import { Parser } from '../parser';
import {
  Tokenizer, TokenType,
} from '../tokens';
import {
  Generator, unsupportedArgs,
} from '../generator';
import {
  addRecursiveCteColumnNames,
  eliminateDistinctOn,
  eliminateQualify,
  eliminateSemiAndAntiJoins,
  eliminateWindowClause,
  epochCastToTs, explodeProjectionToUnnest, inheritStructFieldNames, preprocess, removeWithinGroupForPercentiles, unnestGenerateSeries, unqualifyColumns,
} from '../transforms';
import {
  applyIndexOffset, seqGet,
} from '../helper';
import {
  narrowInstanceOf,
} from '../port_internals';
import type { DateAddOrSub } from './dialect';
import {
  binaryFromFunction,
  boolXorSql,
  buildFormattedTime,
  buildRegexpExtract,
  buildReplaceWithOptionalReplacement,
  dateStrToDateSql,
  dateTruncToTime,
  Dialect, Dialects, encodeDecodeSql, explodeToUnnestSql, ifSql, leftToSubstringSql, noIlikeSql, noPivotSql, NormalizationStrategy,
  regexpExtractSql,
  renameFunc,
  rightToSubstringSql,
  sequenceSql,
  sha256Sql,
  sha2DigestSql,
  strPositionSql,
  structExtractSql,
  timestampTruncSql,
  timeStrToTimeSql,
  tsOrDsAddCast,
  unitToStr,
} from './dialect';
import { MySQL } from './mysql';
import { Hive } from './hive';

/**
 * Presto doesn't have a native INITCAP. We simulate it using REGEXP_REPLACE with a lambda.
 * It transforms 'hello world' into 'Hello World'.
 */
export function initcapSql (self: Generator, expression: InitcapExpr): string {
  const delimiters = expression.args.expression;

  // Presto's simulation only supports default whitespace delimiters
  if (delimiters && !(delimiters.isString && delimiters.args.this === self.dialect._constructor.INITCAP_DEFAULT_DELIMITER_CHARS)) {
    self.unsupported('INITCAP does not support custom delimiters in Presto.');
  }

  const regex = '(\\w)(\\w*)';
  const lambda = 'x -> UPPER(x[1]) || LOWER(x[2])';
  return `REGEXP_REPLACE(${self.sql(expression, 'this')}, '${regex}', ${lambda})`;
}

/**
 * Presto uses ARRAY_SORT. If descending is requested, we provide a custom comparator lambda.
 */
export function noSortArray (self: Generator, expression: SortArrayExpr): string {
  let comparator: string | undefined;

  if (expression.args.asc instanceof BooleanExpr && !expression.args.asc.args.this) {
    comparator = '(a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END';
  }

  return self.func('ARRAY_SORT', [expression.args.this, comparator]);
}

/**
 * Handles schema generation, with special logic for PARTITIONED BY properties
 * where column names must be rendered as string literals within an array.
 */
export function schemaSql (self: Generator, expression: SchemaExpr): string {
  if (expression.parent instanceof PartitionedByPropertyExpr) {
    // Columns in the ARRAY[] string literals should not be quoted identifiers
    expression.transform((n) => {
      if (n instanceof IdentifierExpr) return n.name;
      return n;
    }, { copy: false });

    const partitionExprs = expression.args.expressions?.map((c) => {
      return (c instanceof FuncExpr || c instanceof PropertyExpr)
        ? self.sql(c)
        : self.sql(c, 'this');
    }) || [];

    return self.sql(
      new ArrayExpr({
        expressions: partitionExprs.map((c) => new LiteralExpr({
          this: c,
          isString: true,
        })),
      }),
    );
  }

  // Pre-process column definitions if they are nested in other properties
  if (expression.parent) {
    const siblings = expression.parent.findAll(SchemaExpr);
    for (const schema of siblings) {
      if (schema === expression) continue;

      const columnDefs = schema.findAll(ColumnDefExpr);
      if (0 < [...columnDefs].length && schema.parent instanceof PropertyExpr) {
        expression.args.expressions?.push(...columnDefs);
      }
    }
  }

  return self.schemaSql(expression);
}

/**
 * Presto lacks exact quantiles, so we pivot to the approximate version.
 */
export function quantileSql (self: Generator, expression: QuantileExpr): string {
  self.unsupported('Presto does not support exact quantiles; using APPROX_PERCENTILE.');
  return self.func('APPROX_PERCENTILE', [expression.args.this, expression.args.quantile]);
}

/**
 * Maps StrToDate/Time to Presto's DATE_PARSE function.
 */
export function strToTimeSql (
  self: Generator,
  expression: StrToDateExpr | StrToTimeExpr | TsOrDsToDateExpr,
): string {
  return self.func('DATE_PARSE', [expression.args.this, self.formatTime(expression)]);
}

/**
 * Handles converting a Timestamp or DateString to a Date.
 * If a custom format is provided, it uses DATE_PARSE; otherwise, it performs a nested cast.
 */
export function tsOrDsToDateSql (self: Generator, expression: TsOrDsToDateExpr): string {
  const timeFormat = self.formatTime(expression);

  if (timeFormat && timeFormat !== Presto.TIME_FORMAT && timeFormat !== Presto.DATE_FORMAT) {
    const parsedTime = strToTimeSql(self, expression); // from previous step
    return self.sql(new CastExpr({
      this: LiteralExpr.string({ this: parsedTime }),
      to: LiteralExpr.string({ this: DataTypeExprKind.DATE }),
    }));
  }

  // Standard case: CAST(CAST(x AS TIMESTAMP) AS DATE)
  return self.sql(
    new CastExpr({
      this: new CastExpr({
        this: expression.args.this,
        to: LiteralExpr.string({ this: DataTypeExprKind.TIMESTAMP }),
      }),
      to: LiteralExpr.string({ this: DataTypeExprKind.DATE }),
    }),
  );
}

export function tsOrDsAddSql (self: Generator, expression: TsOrDsAddExpr): string {
  const standardized = tsOrDsAddCast(expression);
  const unit = unitToStr(standardized);
  return self.func('DATE_ADD', [
    unit,
    standardized.args.expression,
    standardized.args.this,
  ]);
}

export function tsOrDsDiffSql (self: Generator, expression: TsOrDsDiffExpr): string {
  const thisTs = new CastExpr({
    this: expression.args.this,
    to: LiteralExpr.string({ this: DataTypeExprKind.TIMESTAMP }),
  });
  const exprTs = new CastExpr({
    this: expression.args.expression,
    to: LiteralExpr.string({ this: DataTypeExprKind.TIMESTAMP }),
  });
  const unit = unitToStr(expression);
  return self.func('DATE_DIFF', [
    unit,
    exprTs,
    thisTs,
  ]);
}

/**
 * Builder for approximate percentiles which maps to ApproxQuantile AST nodes.
 */
export function buildApproxPercentile (args: Expression[]): ApproxQuantileExpr {
  if (args.length === 4) {
    return new ApproxQuantileExpr({
      this: seqGet(args, 0),
      weight: seqGet(args, 1),
      quantile: seqGet(args, 2),
      accuracy: seqGet(args, 3),
    });
  }
  if (args.length === 3) {
    return new ApproxQuantileExpr({
      this: seqGet(args, 0),
      quantile: seqGet(args, 1),
      accuracy: seqGet(args, 2),
    });
  }
  return ApproxQuantileExpr.fromArgList(args);
}

/**
 * Builder for FROM_UNIXTIME, supporting timezone and offset arguments.
 */
export function buildFromUnixtime (args: Expression[]): UnixToTimeExpr {
  if (args.length === 3) {
    return new UnixToTimeExpr({
      this: seqGet(args, 0),
      hours: seqGet(args, 1),
      minutes: seqGet(args, 2),
    });
  }
  if (args.length === 2) {
    return new UnixToTimeExpr({
      this: seqGet(args, 0),
      zone: seqGet(args, 1),
    });
  }

  return UnixToTimeExpr.fromArgList(args);
}

/**
 * In Trino, FIRST/LAST are navigation functions within MATCH_RECOGNIZE.
 * Everywhere else, they are treated as ARBITRARY.
 */
export function firstLastSql (self: Generator, expression: FuncExpr): string {
  if (expression.findAncestor<MatchRecognizeExpr | SelectExpr>(MatchRecognizeExpr, SelectExpr)) {
    return self.functionFallbackSql(expression);
  }
  return renameFunc('ARBITRARY')(self, expression);
}

/**
 * Handles UNIX timestamp conversion, potentially scaling for milliseconds/microseconds.
 */
export function unixToTimeSql (self: Generator, expression: UnixToTimeExpr): string {
  const scale = expression.args.scale;
  const timestamp = self.sql(expression, 'this');

  if (!scale || scale === UnixToTimeExpr.SECONDS) {
    return renameFunc('FROM_UNIXTIME')(self, expression);
  }

  // Presto expects seconds (Double) for from_unixtime
  return `FROM_UNIXTIME(CAST(${timestamp} AS DOUBLE) / POW(10, ${self.sql(scale)}))`;
}

/**
 * Ensures a value is an Integer for date arithmetic. Casts to BIGINT if necessary.
 */
function toInt (self: Generator, expression: Expression): Expression {
  if (!expression.type) {
    annotateTypes(expression, { dialect: self.dialect });
  }
  if (expression.type instanceof Expression && !DataTypeExpr.INTEGER_TYPES.has(expression.type.args.this as DataTypeExprKind)) {
    return new CastExpr({
      this: expression,
      to: LiteralExpr.string(DataTypeExprKind.BIGINT),
    });
  }
  return expression;
}

/**
 * Presto's TO_CHAR implementation actually follows Teradata's format conventions.
 */
export function buildToChar (args: Expression[]): TimeToStrExpr {
  const fmt = seqGet(args, 1);
  if (fmt instanceof LiteralExpr) {
    fmt.setArgKey('this', (fmt.args.this ?? '').toUpperCase());
  }
  return buildFormattedTime(TimeToStrExpr, { dialect: Dialects.TERADATA })(args);
}

/**
 * Higher-order function to create SQL for date addition or subtraction.
 */
export function dateDeltaSql (name: string, options: { negateInterval?: boolean } = {}): (self: Generator, expression: DateAddOrSub) => string {
  const { negateInterval = false } = options;
  return (self: Generator, expression: DateAddOrSub): string => {
    const interval = expression.args.expression
      ? toInt(self, expression.args.expression)
      : new LiteralExpr({
        this: '0',
        isString: false,
      });
    const finalInterval = negateInterval ? interval.mul(-1) : interval;

    return self.func(
      name,
      [
        unitToStr(expression),
        self.sql(finalInterval),
        self.sql(expression, 'this'),
      ],
    );
  };
}

/**
 * Converts Spark/Hive style EXPLODE into Presto UNNEST, with special handling for
 * Arrays of Structs which Presto flattens into multiple columns.
 */
export function explodeToUnnestSqlPresto (self: Generator, expression: LateralExpr): string {
  const explode = expression.args.this;

  if (explode instanceof ExplodeExpr) {
    const explodedType = explode.args.this instanceof Expression ? explode.args.this.type : explode.args.this;
    const alias = expression.args.alias;

    // Best-effort transpilation for LATERAL VIEW EXPLODE on a struct array
    if (
      alias instanceof TableAliasExpr
      && explodedType instanceof DataTypeExpr
      && explodedType.isType(DataTypeExprKind.ARRAY)
      && 0 < (explodedType.args.expressions?.length || 0)
      && explodedType.args.expressions?.[0].isType(DataTypeExprKind.STRUCT)
    ) {
      // Presto unnesting a ROW produces N columns. We fix the alias to match the internal struct fields.
      const structFields = explodedType.args.expressions[0].args.expressions;
      alias.setArgKey('columns', structFields?.flatMap((c) => c instanceof Expression && c.args.this instanceof Expression ? c.args.this.copy() : []));
    }
  } else if (explode instanceof InlineExpr) {
    // Presto doesn't have INLINE; we pivot to EXPLODE
    explode.replace(new ExplodeExpr({ this: explode.args.this instanceof Expression ? explode.args.this.copy() : explode.args.this?.toString() ?? '' }));
  }

  return explodeToUnnestSql(self, expression);
}

/**
 * Fixes column qualifications in a SELECT scope after an EXPLODE -> UNNEST transformation.
 * Useful when transpiling from Spark where fields might be qualified with the struct name.
 */
export function amendExplodedColumnTable (expression: Expression): Expression {
  // Types must be inferred (annotated) for this amendment to work safely
  if (expression instanceof SelectExpr && expression.type) {
    const laterals = expression.args.laterals || [];

    for (const lateral of laterals) {
      const alias = lateral.args.alias;

      if (
        !(lateral.args.this instanceof ExplodeExpr)
        || !(alias instanceof TableAliasExpr)
        || alias.args.columns?.length !== 1
      ) {
        continue;
      }

      const newTable = alias.args.this;
      const oldTable = alias.args.columns[0] instanceof Expression ? alias.args.columns[0].name.toLowerCase() : alias.args.columns[0];

      // Find all columns in the current scope that might be incorrectly qualified
      for (const column of findAllInScope(expression, [ColumnExpr])) {
        const colDb = column.args.db?.toString().toLowerCase();
        const colTable = column.args.table?.toString().toLowerCase();
        const colName = column.name.toLowerCase();

        if (colDb === oldTable) {
          // Move the 'db' reference to 'table' as part of the flattening
          column.setArgKey('table', column.args.db instanceof Expression ? column.args.db.pop() : column.args.db);
        } else if (colTable === oldTable) {
          // Re-map the table reference to the new unnested table alias
          column.setArgKey('table', newTable instanceof Expression ? newTable.copy() : newTable);
        } else if (colName === oldTable && column.parent instanceof DotExpr) {
          // Handle Dot notation (struct.field) by replacing it with a flat column reference
          column.parent.replace(new ColumnExpr({
            this: column.parent.args.expression,
            table: narrowInstanceOf(newTable as unknown, 'string', Expression),
          }));
        }
      }
    }
  }

  return expression;
}

class PrestoTokenizer extends Tokenizer {
  public HEX_STRINGS: [string, string][] = [['x\'', '\''], ['X\'', '\'']];

  public UNICODE_STRINGS: [string, string][] = (() => {
    const prefixes = ['U&', 'u&'];
    const quotes = Tokenizer.QUOTES as string[];
    const result: [string, string][] = [];

    for (const q of quotes) {
      for (const p of prefixes) {
        result.push([p + q, q]);
      }
    }
    return result;
  })();

  public NESTED_COMMENTS = false;

  static ORIGINAL_KEYWORDS: Record<string, TokenType> = (() => {
    const keywords = {
      ...Tokenizer.KEYWORDS,
      'DEALLOCATE PREPARE': TokenType.COMMAND,
      'DESCRIBE INPUT': TokenType.COMMAND,
      'DESCRIBE OUTPUT': TokenType.COMMAND,
      'RESET SESSION': TokenType.COMMAND,
      'START': TokenType.BEGIN,
      'MATCH_RECOGNIZE': TokenType.MATCH_RECOGNIZE,
      'ROW': TokenType.STRUCT,
      'IPADDRESS': TokenType.IPADDRESS,
      'IPPREFIX': TokenType.IPPREFIX,
      'TDIGEST': TokenType.TDIGEST,
      'HYPERLOGLOG': TokenType.HLLSKETCH,
    } as Record<string, TokenType>;

    // Remove Hive-style hints and BigQuery-style QUALIFY from base keywords
    delete keywords['/*+'];
    delete keywords['QUALIFY'];

    return keywords;
  })();
}

class PrestoParser extends Parser {
  public static VALUES_FOLLOWED_BY_PAREN = false;
  public static ZONE_AWARE_TIMESTAMP_CONSTRUCTOR = true;

  public static FUNCTIONS: Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> = {
    ...Parser.FUNCTIONS,
    ARBITRARY: AnyValueExpr.fromArgList,
    APPROX_DISTINCT: ApproxDistinctExpr.fromArgList,
    APPROX_PERCENTILE: buildApproxPercentile,
    BITWISE_AND: binaryFromFunction(BitwiseAndExpr),
    BITWISE_NOT: (args: Expression[]) => new BitwiseNotExpr({ this: seqGet(args, 0) }),
    BITWISE_OR: binaryFromFunction(BitwiseOrExpr),
    BITWISE_XOR: binaryFromFunction(BitwiseXorExpr),
    CARDINALITY: ArraySizeExpr.fromArgList,
    CONTAINS: ArrayContainsExpr.fromArgList,
    // Presto/Trino: DATE_ADD(unit, value, timestamp)
    DATE_ADD: (args: Expression[]) =>
      new DateAddExpr({
        this: seqGet(args, 2),
        expression: seqGet(args, 1),
        unit: seqGet(args, 0),
      }),
    DATE_DIFF: (args: Expression[]) =>
      new DateDiffExpr({
        this: seqGet(args, 2),
        expression: seqGet(args, 1),
        unit: seqGet(args, 0),
      }),
    DATE_FORMAT: buildFormattedTime(TimeToStrExpr, { dialect: Dialects.PRESTO }),
    DATE_PARSE: buildFormattedTime(StrToTimeExpr, { dialect: Dialects.PRESTO }),
    DATE_TRUNC: dateTruncToTime,
    DAY_OF_WEEK: DayOfWeekIsoExpr.fromArgList,
    DOW: DayOfWeekIsoExpr.fromArgList,
    DOY: DayOfYearExpr.fromArgList,
    // ELEMENT_AT is 1-indexed and returns NULL instead of throwing on out-of-bounds
    ELEMENT_AT: (args: Expression[]) =>
      new BracketExpr({
        this: seqGet(args, 0),
        expressions: [seqGet(args, 1)!],
        offset: 1,
        safe: true,
      }),
    FROM_HEX: UnhexExpr.fromArgList,
    FROM_UNIXTIME: buildFromUnixtime,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    FROM_UTF8: (args: any[]) =>
      new DecodeExpr({
        this: seqGet(args, 0),
        replace: seqGet(args, 1),
        charset: new LiteralExpr({
          this: 'utf-8',
          isString: true,
        }),
      }),
    JSON_FORMAT: (args: Expression[]) =>
      new JsonFormatExpr({
        this: seqGet(args, 0),
        options: seqGet(args, 1),
        isJson: true,
      }),
    LEVENSHTEIN_DISTANCE: LevenshteinExpr.fromArgList,
    NOW: CurrentTimestampExpr.fromArgList,
    REGEXP_EXTRACT: buildRegexpExtract(RegexpExtractExpr),
    REGEXP_EXTRACT_ALL: buildRegexpExtract(RegexpExtractAllExpr),
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    REGEXP_REPLACE: (args: any[]) =>
      new RegexpReplaceExpr({
        this: seqGet(args, 0),
        expression: seqGet(args, 1),
        replacement: seqGet(args, 2) ?? '',
      }),
    REPLACE: buildReplaceWithOptionalReplacement,
    ROW: StructExpr.fromArgList,
    SEQUENCE: GenerateSeriesExpr.fromArgList,
    SET_AGG: ArrayUniqueAggExpr.fromArgList,
    SPLIT_TO_MAP: StrToMapExpr.fromArgList,
    STRPOS: (args: Expression[]) =>
      new StrPositionExpr({
        this: seqGet(args, 0),
        substr: seqGet(args, 1),
        occurrence: seqGet(args, 2),
      }),
    SLICE: ArraySliceExpr.fromArgList,
    TO_CHAR: buildToChar,
    TO_UNIXTIME: TimeToUnixExpr.fromArgList,
    TO_UTF8: (args: Expression[]) =>
      new EncodeExpr({
        this: seqGet(args, 0),
        charset: new LiteralExpr({
          this: 'utf-8',
          isString: true,
        }),
      }),
    MD5: Md5DigestExpr.fromArgList,
    SHA256: (args: Expression[]) =>
      new Sha2Expr({
        this: seqGet(args, 0),
        length: new LiteralExpr({
          this: '256',
          isString: false,
        }),
      }),
    SHA512: (args: Expression[]) =>
      new Sha2Expr({
        this: seqGet(args, 0),
        length: new LiteralExpr({
          this: '512',
          isString: false,
        }),
      }),
    WEEK: WeekOfYearExpr.fromArgList,
  };

  public static FUNCTION_PARSERS: Partial<Record<string, (self: Parser) => Expression | undefined>> = (() => {
    const parsers = { ...Parser.FUNCTION_PARSERS };
    // Presto uses its own TRIM logic, so we remove the base SQL parser
    delete parsers['TRIM'];
    return parsers;
  })();
}

class PrestoGenerator extends Generator {
  public static INTERVAL_ALLOWS_PLURAL_FORM = false;
  public static JOIN_HINTS = false;
  public static TABLE_HINTS = false;
  public static QUERY_HINTS = false;
  public static IS_BOOL_ALLOWED = false;
  public static TZ_TO_WITH_TIME_ZONE = true;
  public static NVL2_SUPPORTED = false;
  public static STRUCT_DELIMITER = ['(', ')'] as [string, string];
  public static LIMIT_ONLY_LITERALS = true;
  public static SUPPORTS_SINGLE_ARG_CONCAT = false;
  public static LIKE_PROPERTY_INSIDE_SCHEMA = true;
  public static MULTI_ARG_DISTINCT = false;
  public static SUPPORTS_TO_NUMBER = false;
  public static HEX_FUNC = 'TO_HEX';
  public static PARSE_JSON_NAME = 'JSON_PARSE';
  public static PAD_FILL_PATTERN_IS_REQUIRED = true;
  public static EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = false;
  public static SUPPORTS_MEDIAN = false;
  public static ARRAY_SIZE_NAME = 'CARDINALITY';

  public static PROPERTIES_LOCATION = new Map([
    ...Generator.PROPERTIES_LOCATION,
    [LocationPropertyExpr, PropertiesLocation.UNSUPPORTED],
    [VolatilePropertyExpr, PropertiesLocation.UNSUPPORTED],
  ]);

  public static TYPE_MAPPING = new Map([
    ...Generator.TYPE_MAPPING,
    [DataTypeExprKind.BINARY, 'VARBINARY'],
    [DataTypeExprKind.BIT, 'BOOLEAN'],
    [DataTypeExprKind.DATETIME, 'TIMESTAMP'],
    [DataTypeExprKind.DATETIME64, 'TIMESTAMP'],
    [DataTypeExprKind.FLOAT, 'REAL'],
    [DataTypeExprKind.HLLSKETCH, 'HYPERLOGLOG'],
    [DataTypeExprKind.INT, 'INTEGER'],
    [DataTypeExprKind.STRUCT, 'ROW'],
    [DataTypeExprKind.TEXT, 'VARCHAR'],
    [DataTypeExprKind.TIMESTAMPTZ, 'TIMESTAMP'],
    [DataTypeExprKind.TIMESTAMPNTZ, 'TIMESTAMP'],
    [DataTypeExprKind.TIMETZ, 'TIME'],
  ]);

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  public static ORIGINAL_TRANSFORMS = new Map<typeof Expression, (self: Generator, e: any) => string>([
    ...Generator.TRANSFORMS,
    [AnyValueExpr, renameFunc('ARBITRARY')],
    [
      ApproxQuantileExpr,
      (self: Generator, e: ApproxQuantileExpr) => self.func(
        'APPROX_PERCENTILE',
        [
          e.args.this,
          e.args.weight,
          e.args.quantile,
          e.args.accuracy,
        ],
      ),
    ],
    [ArgMaxExpr, renameFunc('MAX_BY')],
    [ArgMinExpr, renameFunc('MIN_BY')],
    [
      ArrayExpr,
      preprocess(
        [inheritStructFieldNames],
        (self, e) => `ARRAY[${self.expressions(e, { flat: true })}]`,
      ),
    ],
    [ArrayAnyExpr, renameFunc('ANY_MATCH')],
    [ArrayConcatExpr, renameFunc('CONCAT')],
    [ArrayContainsExpr, renameFunc('CONTAINS')],
    [ArrayToStringExpr, renameFunc('ARRAY_JOIN')],
    [ArrayUniqueAggExpr, renameFunc('SET_AGG')],
    [ArraySliceExpr, renameFunc('SLICE')],
    [AtTimeZoneExpr, renameFunc('AT_TIMEZONE')],
    [BitwiseAndExpr, (self: Generator, e: BitwiseAndExpr) => self.func('BITWISE_AND', [e.args.this, e.args.expression])],
    [BitwiseLeftShiftExpr, (self: Generator, e: BitwiseLeftShiftExpr) => self.func('BITWISE_ARITHMETIC_SHIFT_LEFT', [e.args.this, e.args.expression])],
    [BitwiseNotExpr, (self: Generator, e: BitwiseNotExpr) => self.func('BITWISE_NOT', [e.args.this])],
    [BitwiseOrExpr, (self: Generator, e: BitwiseOrExpr) => self.func('BITWISE_OR', [e.args.this, e.args.expression])],
    [BitwiseRightShiftExpr, (self: Generator, e: BitwiseRightShiftExpr) => self.func('BITWISE_ARITHMETIC_SHIFT_RIGHT', [e.args.this, e.args.expression])],
    [BitwiseXorExpr, (self: Generator, e: BitwiseXorExpr) => self.func('BITWISE_XOR', [e.args.this, e.args.expression])],
    [CastExpr, preprocess([epochCastToTs])],
    [CurrentTimeExpr, (_self: Generator, _e: Expression) => 'CURRENT_TIME'],
    [CurrentTimestampExpr, (_self: Generator, _e: Expression) => 'CURRENT_TIMESTAMP'],
    [CurrentUserExpr, (_self: Generator, _e: Expression) => 'CURRENT_USER'],
    [DateAddExpr, dateDeltaSql('DATE_ADD')],
    [
      DateDiffExpr,
      (self: Generator, e: DateDiffExpr) => self.func('DATE_DIFF', [
        unitToStr(e),
        e.args.expression,
        e.args.this,
      ]),
    ],
    [DateStrToDateExpr, dateStrToDateSql],
    [DateToDiExpr, (self: Generator, e: Expression) => `CAST(DATE_FORMAT(${self.sql(e, 'this')}, ${self.dialect._constructor.DATEINT_FORMAT}) AS INT)`],
    [DateSubExpr, dateDeltaSql('DATE_ADD', { negateInterval: true })],
    [DayOfWeekExpr, (self: Generator, e: DayOfWeekExpr) => `((${self.func('DAY_OF_WEEK', [e.args.this])} % 7) + 1)`],
    [DayOfWeekIsoExpr, renameFunc('DAY_OF_WEEK')],
    [DecodeExpr, (self: Generator, e: DecodeExpr) => encodeDecodeSql(self, e, 'FROM_UTF8')],
    [DiToDateExpr, (self: Generator, e: Expression) => `CAST(DATE_PARSE(CAST(${self.sql(e, 'this')} AS VARCHAR), ${self.dialect._constructor.DATEINT_FORMAT}) AS DATE)`],
    [EncodeExpr, (self: Generator, e: EncodeExpr) => encodeDecodeSql(self, e, 'TO_UTF8')],
    [
      FileFormatPropertyExpr,
      (self: Generator, e: Expression) => `format=${self.sql(new LiteralExpr({
        this: e.name,
        isString: true,
      }))}`,
    ],
    [FirstExpr, firstLastSql],
    [FromTimeZoneExpr, (self: Generator, e: Expression) => `WITH_TIMEZONE(${self.sql(e, 'this')}, ${self.sql(e, 'zone')}) AT TIME ZONE 'UTC'`],
    [GenerateSeriesExpr, sequenceSql],
    [GenerateDateArrayExpr, sequenceSql],
    [IfExpr, ifSql()],
    [ILikeExpr, noIlikeSql],
    [InitcapExpr, initcapSql],
    [LastExpr, firstLastSql],
    [LastDayExpr, (self: Generator, e: LastDayExpr) => self.func('LAST_DAY_OF_MONTH', [e.args.this])],
    [LateralExpr, explodeToUnnestSqlPresto],
    [LeftExpr, leftToSubstringSql],
    [LevenshteinExpr, (self: Generator, e: Expression) => unsupportedArgs('insCost', 'delCost', 'subCost', 'maxDist')((e) => renameFunc('LEVENSHTEIN_DISTANCE')(self, e))(e)],
    [LogicalAndExpr, renameFunc('BOOL_AND')],
    [LogicalOrExpr, renameFunc('BOOL_OR')],
    [PivotExpr, noPivotSql],
    [QuantileExpr, quantileSql],
    [RegexpExtractExpr, regexpExtractSql],
    [RegexpExtractAllExpr, regexpExtractSql],
    [RightExpr, rightToSubstringSql],
    [SchemaExpr, schemaSql],
    [SchemaCommentPropertyExpr, (self: Generator, e: SchemaCommentPropertyExpr) => self.nakedProperty(e)],
    [
      SelectExpr,
      preprocess([
        eliminateWindowClause,
        eliminateQualify,
        eliminateDistinctOn,
        explodeProjectionToUnnest(1),
        eliminateSemiAndAntiJoins,
        amendExplodedColumnTable,
      ]),
    ],
    [SortArrayExpr, noSortArray],
    [StrPositionExpr, (self: Generator, e: StrPositionExpr) => strPositionSql(self, e, { supportsOccurrence: true })],
    [StrToDateExpr, (self: Generator, e: StrPositionExpr) => `CAST(${strToTimeSql(self, e)} AS DATE)`],
    [StrToMapExpr, renameFunc('SPLIT_TO_MAP')],
    [StrToTimeExpr, strToTimeSql],
    [StructExtractExpr, structExtractSql],
    [TableExpr, preprocess([unnestGenerateSeries])],
    [
      TimestampExpr,
      (self: Generator, e: TimestampExpr) => {
        // Presto doesn't support the TIMESTAMP keyword in the same way as Postgres/Spark
        return `CAST(${self.sql(e.args.this)} AS TIMESTAMP)`;
      },
    ],
    [TimestampAddExpr, dateDeltaSql('DATE_ADD')],
    [TimestampTruncExpr, timestampTruncSql()],
    [TimeStrToDateExpr, timeStrToTimeSql],
    [TimeStrToTimeExpr, timeStrToTimeSql],
    [TimeStrToUnixExpr, (self: Generator, e: TimeStrToUnixExpr) => self.func('TO_UNIXTIME', [self.func('DATE_PARSE', [e.args.this, self.dialect._constructor.TIME_FORMAT])])],
    [TimeToStrExpr, (self: Generator, e: TimeToStrExpr) => self.func('DATE_FORMAT', [e.args.this, self.formatTime(e)])],
    [TimeToUnixExpr, renameFunc('TO_UNIXTIME')],
    [ToCharExpr, (self: Generator, e: ToCharExpr) => self.func('DATE_FORMAT', [e.args.this, self.formatTime(e)])],
    [TryCastExpr, preprocess([epochCastToTs])],
    [TsOrDiToDiExpr, (self: Generator, e: TsOrDiToDiExpr) => `CAST(SUBSTR(REPLACE(CAST(${self.sql(e, 'this')} AS VARCHAR), '-', ''), 1, 8) AS INT)`],
    [TsOrDsAddExpr, tsOrDsAddSql],
    [TsOrDsDiffExpr, tsOrDsDiffSql],
    [TsOrDsToDateExpr, tsOrDsToDateSql],
    [UnhexExpr, renameFunc('FROM_HEX')],
    [UnixToStrExpr, (self: Generator, e: Expression) => `DATE_FORMAT(FROM_UNIXTIME(${self.sql(e, 'this')}), ${self.formatTime(e)})`],
    [UnixToTimeExpr, unixToTimeSql],
    [UnixToTimeStrExpr, (self: Generator, e: Expression) => `CAST(FROM_UNIXTIME(${self.sql(e, 'this')}) AS VARCHAR)`],
    [VariancePopExpr, renameFunc('VAR_POP')],
    [WithExpr, preprocess([addRecursiveCteColumnNames])],
    [WithinGroupExpr, preprocess([removeWithinGroupForPercentiles])],
    [TruncExpr, renameFunc('TRUNCATE')],
    [XorExpr, boolXorSql],
    [Md5DigestExpr, renameFunc('MD5')],
    [ShaExpr, renameFunc('SHA1')],
    [Sha1DigestExpr, renameFunc('SHA1')],
    [Sha2Expr, sha256Sql],
    [Sha2DigestExpr, sha2DigestSql],
  ]);

  public static RESERVED_KEYWORDS = new Set([
    'alter',
    'and',
    'as',
    'between',
    'by',
    'case',
    'cast',
    'constraint',
    'create',
    'cross',
    'current_time',
    'current_timestamp',
    'deallocate',
    'delete',
    'describe',
    'distinct',
    'drop',
    'else',
    'end',
    'escape',
    'except',
    'execute',
    'exists',
    'extract',
    'false',
    'for',
    'from',
    'full',
    'group',
    'having',
    'in',
    'inner',
    'insert',
    'intersect',
    'into',
    'is',
    'join',
    'left',
    'like',
    'natural',
    'not',
    'undefined',
    'on',
    'or',
    'order',
    'outer',
    'prepare',
    'right',
    'select',
    'table',
    'then',
    'true',
    'union',
    'using',
    'values',
    'when',
    'where',
    'with',
  ]);

  /**
   * Handles Presto's specific EXTRACT logic for high-precision EPOCHs.
   * Converts EPOCH_MILLISECOND, etc., into Unix time math.
   */
  public extractSql (expression: ExtractExpr): string {
    const datePart = expression.name.toUpperCase();

    if (!datePart.startsWith('EPOCH')) {
      return super.extractSql(expression);
    }

    let scale: number | undefined = undefined;
    if (datePart === 'EPOCH_MILLISECOND') scale = 10 ** 3;
    else if (datePart === 'EPOCH_MICROSECOND') scale = 10 ** 6;
    else if (datePart === 'EPOCH_NANOSECOND') scale = 10 ** 9;

    const value = expression.args.expression;
    const ts = new CastExpr({
      this: value,
      to: DataTypeExpr.build('TIMESTAMP'),
    });
    let toUnix: Expression = new TimeToUnixExpr({ this: ts });

    if (scale) {
      toUnix = new MulExpr({
        this: toUnix,
        expression: new LiteralExpr({
          this: scale.toString(),
          isString: false,
        }),
      });
    }

    return this.sql(toUnix);
  }

  /**
     * Presto requires the input to JSON_FORMAT to be of type JSON.
     */
  public jsonFormatSql (expression: JsonFormatExpr): string {
    let thisArg = expression.args.this;
    const isJson = expression.args.isJson;

    if (thisArg && !(isJson || thisArg.type)) {
      thisArg = annotateTypes(thisArg, { dialect: this.dialect });
    }

    if (!(isJson || thisArg?.isType(DataTypeExprKind.JSON))) {
      thisArg?.replace(new CastExpr({
        this: thisArg,
        to: LiteralExpr.string(DataTypeExprKind.JSON),
      }));
    }

    return this.functionFallbackSql(expression);
  }

  /**
     * Presto's MD5 returns VARBINARY, so it needs to be hexed and lowercased
     * for standard string representation.
     */
  public md5Sql (expression: Md5Expr): string {
    let thisArg = expression.args.this;

    if (!thisArg?.type && thisArg) {
      thisArg = annotateTypes(thisArg, { dialect: this.dialect });
    }

    // Presto requires strings to be encoded to UTF-8 before hashing
    if (thisArg?.isType(DataTypeExpr.TEXT_TYPES)) {
      thisArg = new EncodeExpr({
        this: thisArg,
        charset: new LiteralExpr({
          this: 'utf-8',
          isString: true,
        }),
      });
    }

    return this.func(
      'LOWER',
      [this.func('TO_HEX', [this.func('MD5', [this.sql(thisArg)])])],
    );
  }

  /**
     * Converts a string to a Unix timestamp.
     * Uses a COALESCE(TRY(DATE_PARSE), PARSE_DATETIME) pattern to handle
     * standard formats and those containing timezones (Hive style).
     */
  public strToUnixSql (expression: StrToUnixExpr): string {
    const thisArg = expression.args.this;
    const valueAsText = new CastExpr({
      this: thisArg,
      to: LiteralExpr.string(DataTypeExprKind.TEXT),
    });
    const valueAsTimestamp = thisArg?.args.isString
      ? new CastExpr({
        this: thisArg,
        to: LiteralExpr.string(DataTypeExprKind.TIMESTAMP),
      })
      : thisArg;

    const parseWithoutTz = this.func('DATE_PARSE', [valueAsText, this.formatTime(expression)]);

    const formattedValue = this.func('DATE_FORMAT', [valueAsTimestamp, this.formatTime(expression)]);

    const parseWithTz = this.func(
      'PARSE_DATETIME',
      [
        formattedValue,
        this.formatTime(
          expression,
          Hive.INVERSE_TIME_MAPPING,
          Hive.INVERSE_TIME_TRIE,
        ),
      ],
    );

    const coalesced = this.func('COALESCE', [this.func('TRY', [parseWithoutTz]), parseWithTz]);
    return this.func('TO_UNIXTIME', [coalesced]);
  }

  public bracketSql (expression: BracketExpr): string {
    if (expression.args.safe) {
      return this.func(
        'ELEMENT_AT',
        [
          expression.args.this,
          expression.args.this instanceof Expression
            ? seqGet(
              applyIndexOffset(
                expression.args.this,
                expression.args.expressions ?? [],
                1 - (expression.args.offset || 0),
                { dialect: this.dialect },
              ),
              0,
            )
            : undefined,
        ],
      );
    }
    return super.bracketSql(expression);
  }

  /**
     * Presto uses ROW(...) for structs. If types are known, it casts the row
     * to a specific ROW schema to maintain field naming.
     */
  public structSql (expression: StructExpr): string {
    if (!expression.type) {
      annotateTypes(expression, { dialect: this.dialect });
    }

    const values: string[] = [];
    const schema: string[] = [];
    let unknownType = false;

    for (const e of expression.args.expressions || []) {
      if (e instanceof PropertyEqExpr) {
        if (isType(e.type, DataTypeExprKind.UNKNOWN)) {
          unknownType = true;
        } else {
          schema.push(`${this.sql(e, 'this')} ${this.sql(e.type)}`);
        }
        values.push(this.sql(e, 'expression'));
      } else {
        values.push(this.sql(e));
      }
    }

    const size = expression.args.expressions?.length;

    if (!size || schema.length !== size) {
      if (unknownType) {
        this.unsupported('Cannot convert untyped key-value definitions (try annotate_types).');
      }
      return this.func('ROW', values);
    }

    return `CAST(ROW(${values.join(', ')}) AS ROW(${schema.join(', ')}))`;
  }

  public intervalSql (expression: IntervalExpr): string {
    const unit = expression.text('unit').toUpperCase();
    if (expression.args.this && unit.startsWith('WEEK')) {
      // Presto interval doesn't support weeks directly in some versions; convert to days.
      return `(${expression.args.this.name} * INTERVAL '7' DAY)`;
    }
    return super.intervalSql(expression);
  }

  public transactionSql (expression: TransactionExpr): string {
    const modes = expression.args.modes;
    const modesStr = modes ? ` ${modes.join(', ')}` : '';
    return `START TRANSACTION${modesStr}`;
  }

  public createSql (expression: CreateExpr): string {
    // Presto CREATE VIEW does not support explicit column lists in the header.
    const createThis = expression.args.this instanceof Expression ? expression.args.this : undefined;
    if (expression.args.kind === CreateExprKind.VIEW && createThis?.args.expressions) {
      createThis.setArgKey('expressions', undefined);
    }
    return super.createSql(expression);
  }

  /**
   * Presto DELETE is restrictive (no aliases, single table).
   */
  public deleteSql (expression: DeleteExpr): string {
    const tables = expression.args.tables || [expression.args.this];
    if (1 < tables.length) {
      return super.deleteSql(expression);
    }

    const table = tables[0];
    expression.setArgKey('this', table);
    expression.setArgKey('tables', undefined);

    if (table instanceof TableExpr) {
      const tableAlias = table.args.alias;
      if (tableAlias) {
        tableAlias.pop(); // Remove alias as Presto doesn't support it in DELETE
        expression = expression.transform(unqualifyColumns) as DeleteExpr;
      }
    }

    return super.deleteSql(expression);
  }

  public jsonExtractSql (expression: JsonExtractExpr): string {
    const isJsonExtract = this.dialect.settings.variantExtractIsJsonExtract ?? true;

    if (!expression.args.variantExtract || isJsonExtract) {
      return this.func('JSON_EXTRACT', [
        expression.args.this,
        expression.args.expression,
        ...expression.args.expressions || [],
      ]);
    }

    const thisArg = this.sql(expression, 'this');
    const segments: string[] = [];

    // Convert JSONPath '$.x.y' to ROW access 'col.x.y'
    for (const pathKey of narrowInstanceOf(expression.args.expression, Expression)?.args.expressions?.slice(1) || []) {
      if (!(pathKey instanceof JsonPathKeyExpr)) {
        this.unsupported(`Cannot transpile JSONPath segment '${pathKey}' to ROW access`);
        continue;
      }
      let key = pathKey.args.this?.toString();
      if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(key ?? '')) {
        key = `"${key}"`;
      }
      segments.push(`.${key}`);
    }

    return `${thisArg}${segments.join('')}`;
  }

  public groupConcatSql (expression: GroupConcatExpr): string {
    // Presto simulates GROUP_CONCAT by aggregating into an array and joining it.
    return this.func(
      'ARRAY_JOIN',
      [this.func('ARRAY_AGG', [expression.args.this]), expression.args.separator],
    );
  }
}

export class Presto extends Dialect {
  static INDEX_OFFSET = 1;
  static NULL_ORDERING = 'nulls_are_last' as const;
  static TIME_FORMAT = MySQL.TIME_FORMAT;
  static STRICT_STRING_CONCAT = true;
  static SUPPORTS_SEMI_ANTI_JOIN = false;
  static TYPED_DIVISION = true;
  static TABLESAMPLE_SIZE_IS_PERCENT = true;
  static LOG_BASE_FIRST: boolean | undefined = undefined;
  static SUPPORTS_VALUES_DEFAULT = false;
  static LEAST_GREATEST_IGNORES_NULLS = false;

  static TIME_MAPPING = MySQL.TIME_MAPPING;

  // Presto/Trino follow a case-insensitive strategy for identifiers
  static NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE;

  static SUPPORTED_SETTINGS = new Set([...Dialect.SUPPORTED_SETTINGS, 'variantExtractIsJsonExtract']);

  static Tokenizer = PrestoTokenizer;
  static Parser = PrestoParser;
  static Generator = PrestoGenerator;
}
Dialect.register(Dialects.PRESTO, Presto);
