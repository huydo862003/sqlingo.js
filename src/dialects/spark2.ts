import type {
  AlterColumnExpr,
  CommandExpr, StructExpr,
} from '../expressions';
import {
  ApproxDistinctExpr,
  ApproxQuantileExpr,
  ArraySliceExpr,
  ArraySortExpr,
  ArraySumExpr,
  ArrayToStringExpr,
  AtTimeZoneExpr,
  AutoIncrementPropertyExpr,
  BitwiseLeftShiftExpr,
  BitwiseRightShiftExpr,
  cast,
  CastExpr,
  CharacterSetPropertyExpr,
  CollatePropertyExpr,
  CreateExpr,
  DataTypeExpr,
  DataTypeExprKind,
  DateFromPartsExpr,
  DateTruncExpr,
  DayOfMonthExpr,
  DayOfWeekExpr,
  DayOfWeekIsoExpr,
  DayOfYearExpr,
  DivExpr,
  DropExpr,
  EnginePropertyExpr,
  Expression,
  FileFormatPropertyExpr, FormatExpr, FromExpr, FromTimeZoneExpr, ILikeExpr, JsonExtractExpr, JsonExtractScalarExpr, LeftExpr, LiteralExpr, LogicalAndExpr, LogicalOrExpr, MapExpr, MonthsBetweenExpr,
  PivotExpr, PropertiesLocation, ReduceExpr, RegexpLikeExpr, RegexpReplaceExpr, RightExpr, select, SelectExpr, Sha2DigestExpr, StrToDateExpr, StrToTimeExpr,
  StrToUnixExpr, TimestampTruncExpr, TsOrDsToDateExpr, UnixToTimeExpr,
  var_,
  VariancePopExpr,
  WeekOfYearExpr,
  WithinGroupExpr,
} from '../expressions';
import type { Generator } from '../generator';
import { seqGet } from '../helper';
import type { Parser } from '../parser';
import { buildTrim } from '../parser';
import { narrowInstanceOf } from '../port_internals';
import { TokenType } from '../tokens';
import {
  anyToExists,
  ctasWithTmpTablesToCreateTmpView,
  eliminateDistinctOn,
  eliminateQualify,
  moveSchemaColumnsToPartitionedBy,
  preprocess, removeUniqueConstraints, removeWithinGroupForPercentiles, unnestToExplode, unqualifyColumns,
} from '../transforms';
import { EXPRESSION_METADATA } from '../typing';
import {
  binaryFromFunction, buildFormattedTime, Dialect, Dialects, isParseJson, pivotColumnNames,
  renameFunc,
  unitToStr,
} from './dialect';
import { Hive } from './hive';

function mapSql (self: Generator, expression: MapExpr): string {
  const keys = expression.args.keys;
  const values = expression.args.values;

  if (!keys || !values) {
    return self.func('MAP', []);
  }

  return self.func('MAP_FROM_ARRAYS', [...keys, ...values]);
}

export function buildAsCast (toType: string) {
  return (args: Expression[]): CastExpr => {
    return new CastExpr({
      this: seqGet(args, 0),
      to: DataTypeExpr.build(toType),
    });
  };
}

function strToDate (self: Generator, expression: StrToDateExpr): string {
  const timeFormat = self.formatTime(expression);
  if (timeFormat === Hive.DATE_FORMAT) {
    return self.func('TO_DATE', [expression.args.this]);
  }
  return self.func('TO_DATE', [expression.args.this, timeFormat]);
}

function unixToTimeSql (self: Generator, expression: UnixToTimeExpr): string {
  const scale = expression.args.scale;
  const timestamp = expression.args.this;

  if (scale === undefined) {
    return self.sql(
      new CastExpr({
        this: self.func('from_unixtime', [timestamp]),
        to: DataTypeExpr.build(DataTypeExprKind.TIMESTAMP),
      }),
    );
  }

  if (scale === UnixToTimeExpr.SECONDS) {
    return self.func('TIMESTAMP_SECONDS', [timestamp]);
  }
  if (scale === UnixToTimeExpr.MILLIS) {
    return self.func('TIMESTAMP_MILLIS', [timestamp]);
  }
  if (scale === UnixToTimeExpr.MICROS) {
    return self.func('TIMESTAMP_MICROS', [timestamp]);
  }

  const unixSeconds = new DivExpr({
    this: timestamp,
    expression: self.func('POW', [LiteralExpr.number(10), scale]),
  });

  return self.func('TIMESTAMP_SECONDS', [unixSeconds]);
}

/**
 * Spark doesn't allow PIVOT aliases, so we remove them and wrap
 * in a subquery to preserve semantics.
 */
function unaliasPivot (expression: Expression): Expression {
  if (expression instanceof FromExpr && expression.args.this?.args.pivots?.length) {
    const pivot = expression.args.this.args.pivots[0];
    if (pivot.args.alias) {
      const aliasNode = narrowInstanceOf(pivot.args.alias, Expression)?.pop();
      return new FromExpr({
        this: expression.args.this.replace(
          select('*')
            .from(expression.args.this.copy(), { copy: false })
            .subquery(undefined, {
              alias: aliasNode,
              copy: false,
            }),
        ),
      });
    }
  }

  return expression;
}

/**
 * Spark doesn't allow qualified columns in PIVOT's field.
 */
function unqualifyPivotColumns (expression: Expression): Expression {
  if (expression instanceof PivotExpr) {
    expression.setArgKey(
      'fields',
      expression.args.fields?.map((field) => unqualifyColumns(field)),
    );
  }

  return expression;
}

export function temporaryStorageProvider (expression: Expression): Expression {
  // Spark/Databricks require a storage provider for temporary tables
  const provider = new FileFormatPropertyExpr({ this: LiteralExpr.string('parquet') });

  const properties = expression.getArgKey('properties');
  if (properties instanceof Expression) {
    properties.append('expressions', provider);
  }

  return expression;
}

class Spark2Tokenizer extends Hive.Tokenizer {
  static HEX_STRINGS: [string, string][] = [['X\'', '\''], ['x\'', '\'']];

  static ORIGINAL_KEYWORDS = {
    ...Hive.Tokenizer.KEYWORDS,
    TIMESTAMP: TokenType.TIMESTAMPTZ,
  };
}

class Spark2Parser extends Hive.Parser {
  static TRIM_PATTERN_FIRST = true;
  static CHANGE_COLUMN_ALTER_SYNTAX = true;

  static FUNCTIONS = {
    ...Hive.Parser.FUNCTIONS,
    AGGREGATE: ReduceExpr.fromArgList,
    BOOLEAN: buildAsCast('boolean'),
    DATE: buildAsCast('date'),
    DATE_TRUNC: (args: Expression[]) =>
      new TimestampTruncExpr({
        this: seqGet(args, 1),
        unit: var_(seqGet(args, 0)),
      }),
    DAYOFMONTH: (args: Expression[]) =>
      new DayOfMonthExpr({ this: new TsOrDsToDateExpr({ this: seqGet(args, 0) }) }),
    DAYOFWEEK: (args: Expression[]) =>
      new DayOfWeekExpr({ this: new TsOrDsToDateExpr({ this: seqGet(args, 0) }) }),
    DAYOFYEAR: (args: Expression[]) =>
      new DayOfYearExpr({ this: new TsOrDsToDateExpr({ this: seqGet(args, 0) }) }),
    DOUBLE: buildAsCast('double'),
    FLOAT: buildAsCast('float'),
    FORMAT_STRING: FormatExpr.fromArgList,
    FROM_UTC_TIMESTAMP: (args: Expression[], { dialect }: { dialect: Dialect }) =>
      new AtTimeZoneExpr({
        this: cast(seqGet(args, 0) || var_(''), DataTypeExprKind.TIMESTAMP, {
          dialect,
        }),
        zone: seqGet(args, 1),
      }),
    LTRIM: (args: Expression[]) => buildTrim(args, { reverseArgs: true }),
    INT: buildAsCast('int'),
    MAP_FROM_ARRAYS: MapExpr.fromArgList,
    RLIKE: RegexpLikeExpr.fromArgList,
    RTRIM: (args: Expression[]) => buildTrim(args, {
      isLeft: false,
      reverseArgs: true,
    }),
    SHIFTLEFT: binaryFromFunction(BitwiseLeftShiftExpr),
    SHIFTRIGHT: binaryFromFunction(BitwiseRightShiftExpr),
    STRING: buildAsCast('string'),
    SLICE: ArraySliceExpr.fromArgList,
    TIMESTAMP: buildAsCast('timestamp'),
    TO_TIMESTAMP: (args: Expression[]) =>
      args.length === 1
        ? buildAsCast('timestamp')(args)
        : buildFormattedTime(StrToTimeExpr, { dialect: 'spark' })(args),
    TO_UNIX_TIMESTAMP: StrToUnixExpr.fromArgList,
    TO_UTC_TIMESTAMP: (args: Expression[], { dialect }: { dialect: Dialect }) =>
      new FromTimeZoneExpr({
        this: cast(seqGet(args, 0) || var_(''), DataTypeExprKind.TIMESTAMP, {
          dialect,
        }),
        zone: seqGet(args, 1),
      }),
    TRUNC: (args: Expression[]) => new DateTruncExpr({
      unit: seqGet(args, 1),
      this: seqGet(args, 0),
    }),
    WEEKOFYEAR: (args: Expression[]) =>
      new WeekOfYearExpr({ this: new TsOrDsToDateExpr({ this: seqGet(args, 0) }) }),
  };

  static FUNCTION_PARSERS = {
    ...Hive.Parser.FUNCTION_PARSERS,
    APPROX_PERCENTILE: (self: Parser) => (self as Spark2Parser).parseQuantileFunction(ApproxQuantileExpr),
    BROADCAST: (self: Parser) => self.parseJoinHint('BROADCAST'),
    BROADCASTJOIN: (self: Parser) => self.parseJoinHint('BROADCASTJOIN'),
    MAPJOIN: (self: Parser) => self.parseJoinHint('MAPJOIN'),
    MERGE: (self: Parser) => self.parseJoinHint('MERGE'),
    SHUFFLEMERGE: (self: Parser) => self.parseJoinHint('SHUFFLEMERGE'),
    MERGEJOIN: (self: Parser) => self.parseJoinHint('MERGEJOIN'),
    SHUFFLE_HASH: (self: Parser) => self.parseJoinHint('SHUFFLE_HASH'),
    SHUFFLE_REPLICATE_NL: (self: Parser) => self.parseJoinHint('SHUFFLE_REPLICATE_NL'),
  };

  parseDropColumn (): DropExpr | CommandExpr | undefined {
    return (
      this.matchTextSeq(['DROP', 'COLUMNS'])
      && this.expression(DropExpr, {
        this: this.parseSchema(),
        kind: 'COLUMNS',
      })
    );
  }

  pivotColumnNames (aggregations: Expression[]): string[] {
    if (aggregations.length === 1) {
      return [];
    }
    return pivotColumnNames(aggregations, { dialect: 'spark' });
  }
}

class Spark2Generator extends Hive.Generator {
  static QUERY_HINTS = true;
  static NVL2_SUPPORTED = true;
  static CAN_IMPLEMENT_ARRAY_ANY = true;
  static ALTER_SET_TYPE = 'TYPE';
  static PARSE_JSON_NAME?: string;

  static PROPERTIES_LOCATION = new Map([
    ...Hive.Generator.PROPERTIES_LOCATION,
    [EnginePropertyExpr, PropertiesLocation.UNSUPPORTED],
    [AutoIncrementPropertyExpr, PropertiesLocation.UNSUPPORTED],
    [CharacterSetPropertyExpr, PropertiesLocation.UNSUPPORTED],
    [CollatePropertyExpr, PropertiesLocation.UNSUPPORTED],
  ]);

  static TS_OR_DS_EXPRESSIONS = new Set([
    ...Hive.Generator.TS_OR_DS_EXPRESSIONS,
    DayOfMonthExpr,
    DayOfWeekExpr,
    DayOfYearExpr,
    WeekOfYearExpr,
  ]);

  static ORIGINAL_TRANSFORMS = (() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (self: Generator, e: any) => string>([
      ...Hive.Generator.TRANSFORMS.entries(),
      [ApproxDistinctExpr, renameFunc('APPROX_COUNT_DISTINCT')],
      [
        ArraySumExpr,
        (self: Generator, e: ArraySumExpr) =>
          `AGGREGATE(${self.sql(e, 'this')}, 0, (acc, x) -> acc + x, acc -> acc)`,
      ],
      [ArrayToStringExpr, renameFunc('ARRAY_JOIN')],
      [ArraySliceExpr, renameFunc('SLICE')],
      [
        AtTimeZoneExpr,
        (self: Generator, e: AtTimeZoneExpr) =>
          self.func('FROM_UTC_TIMESTAMP', [e.args.this, e.args.zone]),
      ],
      [BitwiseLeftShiftExpr, renameFunc('SHIFTLEFT')],
      [BitwiseRightShiftExpr, renameFunc('SHIFTRIGHT')],
      [
        CreateExpr,
        preprocess([
          removeUniqueConstraints,
          (e: Expression) => ctasWithTmpTablesToCreateTmpView(e, temporaryStorageProvider),
          moveSchemaColumnsToPartitionedBy,
        ]),
      ],
      [DateFromPartsExpr, renameFunc('MAKE_DATE')],
      [DateTruncExpr, (self: Generator, e: DateTruncExpr) => self.func('TRUNC', [e.args.this, unitToStr(e)])],
      [DayOfMonthExpr, renameFunc('DAYOFMONTH')],
      [DayOfWeekExpr, renameFunc('DAYOFWEEK')],
      [
        DayOfWeekIsoExpr,
        (self: Generator, e: DayOfWeekIsoExpr) =>
          '(( ' + self.func('DAYOFWEEK', [e.args.this]) + ' % 7) + 1)',
      ],
      [DayOfYearExpr, renameFunc('DAYOFYEAR')],
      [FormatExpr, renameFunc('FORMAT_STRING')],
      [FromExpr, preprocess([unaliasPivot])],
      [
        FromTimeZoneExpr,
        (self: Generator, e: FromTimeZoneExpr) =>
          self.func('TO_UTC_TIMESTAMP', [e.args.this, e.args.zone]),
      ],
      [LogicalAndExpr, renameFunc('BOOL_AND')],
      [LogicalOrExpr, renameFunc('BOOL_OR')],
      [MapExpr, mapSql],
      [PivotExpr, preprocess([unqualifyPivotColumns])],
      [ReduceExpr, renameFunc('AGGREGATE')],
      [
        RegexpReplaceExpr,
        (self: Generator, e: RegexpReplaceExpr) =>
          self.func('REGEXP_REPLACE', [
            e.args.this,
            e.args.expression,
            e.args.replacement,
            e.args.position,
          ]),
      ],
      [
        SelectExpr,
        preprocess([
          eliminateQualify,
          eliminateDistinctOn,
          unnestToExplode,
          anyToExists,
        ]),
      ],
      [
        Sha2DigestExpr,
        (self: Generator, e: Sha2DigestExpr) =>
          self.func('SHA2', [e.args.this, e.args.length || LiteralExpr.number(256)]),
      ],
      [StrToDateExpr, strToDate],
      [
        StrToTimeExpr,
        (self: Generator, e: StrToTimeExpr) =>
          self.func('TO_TIMESTAMP', [e.args.this, self.formatTime(e)]),
      ],
      [
        TimestampTruncExpr,
        (self: Generator, e: TimestampTruncExpr) =>
          self.func('DATE_TRUNC', [unitToStr(e), e.args.this]),
      ],
      [UnixToTimeExpr, unixToTimeSql],
      [VariancePopExpr, renameFunc('VAR_POP')],
      [WeekOfYearExpr, renameFunc('WEEKOFYEAR')],
      [WithinGroupExpr, preprocess([removeWithinGroupForPercentiles])],
    ]);

    [
      ArraySortExpr,
      ILikeExpr,
      LeftExpr,
      MonthsBetweenExpr,
      RightExpr,
    ].forEach((expr) => transforms.delete(expr));

    return transforms;
  })();

  static WRAP_DERIVED_VALUES = false;
  static CREATE_FUNCTION_RETURN_AS = false;

  structSql (expression: StructExpr): string {
    return super.structSql(expression);
  }

  castSql (expression: CastExpr, options: { safePrefix?: string } = {}): string {
    const arg = expression.args.this;
    const isJsonExtract =
      (arg instanceof JsonExtractExpr || arg instanceof JsonExtractScalarExpr)
      && !arg.getArgKey('variantExtract');

    if (narrowInstanceOf(expression.args.to, Expression)?.getArgKey('nested') && (isParseJson(arg) || isJsonExtract)) {
      const schema = `'${this.sql(expression, 'to')}'`;
      return this.func('FROM_JSON', [isJsonExtract ? arg : (arg as Expression).args.this, schema]);
    }

    if (isParseJson(expression)) {
      return this.func('TO_JSON', [arg]);
    }

    return super.castSql(expression, options);
  }

  fileFormatPropertySql (expression: FileFormatPropertyExpr): string {
    if (expression.args.hiveFormat) {
      return super.fileFormatPropertySql(expression);
    }

    return `USING ${expression.name.toUpperCase()}`;
  }

  alterColumnSql (expression: AlterColumnExpr): string {
    const thisNode = this.sql(expression, 'this');
    const newName = this.sql(expression, 'renameTo') || thisNode;
    const comment = this.sql(expression, 'comment');

    if (newName === thisNode) {
      if (comment) {
        return `ALTER COLUMN ${thisNode} COMMENT ${comment}`;
      }
      return super.alterColumnSql(expression);
    }
    return `RENAME COLUMN ${thisNode} TO ${newName}`;
  }

  renameColumnSql (): string {
    return super.renameColumnSql();
  }
}

export class Spark2 extends Hive {
  static ALTER_TABLE_SUPPORTS_CASCADE = false;
  static EXPRESSION_METADATA = { ...EXPRESSION_METADATA };
  static INITCAP_DEFAULT_DELIMITER_CHARS = ' ';

  static Tokenizer = Spark2Tokenizer;
  static Parser = Spark2Parser;
  static Generator = Spark2Generator;
}

Dialect.register(Dialects.SPARK2, Spark2);
