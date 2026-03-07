import { cache } from '../port_internals';
import type {
  Expression,
  ExpressionValue,
  GeneratedAsIdentityColumnConstraintExpr,
  GeneratedAsRowColumnConstraintExpr,
  ReadParquetExpr,
} from '../expressions';
import {
  TryCastExpr,
  GroupConcatExpr,
  ComputedColumnConstraintExpr,
  PlaceholderExpr,
  AnyValueExpr,
  ArrayAppendExpr,
  ArrayConstructCompactExpr,
  ArrayPrependExpr,
  BitwiseAndAggExpr,
  BitwiseCountExpr,
  BitwiseOrAggExpr,
  BitwiseXorAggExpr,
  CurrentVersionExpr,
  DateDiffExpr,
  DateFromUnixDateExpr,
  DatetimeAddExpr,
  DatetimeDiffExpr,
  DatetimeSubExpr,
  EndsWithExpr,
  SafeAddExpr,
  SafeMultiplyExpr,
  SafeSubtractExpr,
  StartsWithExpr,
  TimeAddExpr,
  TimeSubExpr,
  TimestampAddExpr,
  TimestampDiffExpr,
  TimestampSubExpr,
  TsOrDsToDateExpr,
  DataTypeExprKind,
  ArrayInsertExpr,
  ILikeExpr,
  var_,
  TsOrDsAddExpr,
  LiteralExpr,
  toIdentifier,
  ArrayToStringExpr,
  ArrayAggExpr,
  LikeExpr,
  BracketExpr,
  JsonKeysExpr,
  CreateExpr,
  PartitionedByPropertyExpr,
} from '../expressions';
import type { Generator } from '../generator';
import { seqGet } from '../helper';
import type { Parser } from '../parser';
import type { TokenPair } from '../tokens';
import { TokenType } from '../tokens';
import {
  ctasWithTmpTablesToCreateTmpView, movePartitionedByToSchemaColumns, preprocess, removeUniqueConstraints,
} from '../transforms';
import { DialectTyping } from '../typing';
import {
  arrayAppendSql,
  dateDeltaToBinaryIntervalOp,
  unitToVar,
  Dialect, Dialects,
  renameFunc,
  groupConcatSql as baseGroupConcatSql,
  buildLike,
  buildDateDelta,
  timestampDiffSql,
} from './dialect';
import { buildWithIgnoreNulls } from './hive';
import {
  buildAsCast, Spark2,
  temporaryStorageProvider,
} from './spark2';

/**
 * Although Spark docs don't mention the "unit" argument, Spark3 added support for
 * it at some point. Databricks also supports this variant.
 */
function buildDatediff (args: Expression[]): Expression {
  let unit: Expression | undefined = undefined;
  let thisNode = seqGet(args, 0);
  const expression = seqGet(args, 1);

  if (args.length === 3) {
    unit = var_(thisNode?.name);
    thisNode = args[2];
  }

  return new DateDiffExpr({
    this: new TsOrDsToDateExpr({ this: thisNode }),
    expression: new TsOrDsToDateExpr({ this: expression }),
    unit: unit,
  });
}

function buildDateAdd (args: Expression[]): Expression {
  const expression = seqGet(args, 1);

  if (args.length === 2) {
    // DATE_ADD(startDate, numDays INTEGER)
    return new TsOrDsAddExpr({
      this: seqGet(args, 0),
      expression: expression,
      unit: LiteralExpr.string('DAY'),
    });
  }

  // DATE_ADD / DATEADD / TIMESTAMPADD(unit, value integer, expr)
  return new TimestampAddExpr({
    this: seqGet(args, 2),
    expression: expression,
    unit: seqGet(args, 0),
  });
}

function normalizePartition (e: Exclude<ExpressionValue, undefined>): Expression {
  /** Normalize the expressions in PARTITION BY (<expression>, <expression>, ...) */
  if (typeof e === 'string' || typeof e === 'number' || typeof e === 'boolean') {
    return toIdentifier(e.toString());
  }
  if (e instanceof LiteralExpr) {
    return toIdentifier(e.name);
  }
  return e;
}

function dateAddSql (this: Generator, expression: TsOrDsAddExpr | TimestampAddExpr): string {
  const unit = expression.args.unit;

  if (
    !unit
    || (expression instanceof TsOrDsAddExpr && expression.text('unit').toUpperCase() === 'DAY')
  ) {
    // Coming from Hive/Spark2 DATE_ADD or roundtripping the 2-arg version of Spark3/DB
    return this.func('DATE_ADD', [expression.args.this, expression.args.expression]);
  }

  let thisSql = this.func('DATE_ADD', [
    unitToVar(expression),
    expression.args.expression,
    expression.args.this,
  ]);

  if (expression instanceof TsOrDsAddExpr) {
    // The 3 arg version of DATE_ADD produces a timestamp in Spark3/DB
    const returnType = expression.returnType;
    if (!returnType?.isType([DataTypeExprKind.TIMESTAMP, DataTypeExprKind.DATETIME])) {
      thisSql = `CAST(${thisSql} AS ${returnType})`;
    }
  }

  return thisSql;
}

function groupConcatSql (this: Generator, expression: GroupConcatExpr): string {
  if (this.dialect.version.major < 4) {
    const expr = new ArrayToStringExpr({
      this: new ArrayAggExpr({ this: expression.args.this }),
      expression: expression.args.separator || LiteralExpr.string(''),
    });
    return this.sql(expr);
  }

  return baseGroupConcatSql.call(this, expression);
}

class SparkTokenizer extends Spark2.Tokenizer {
  static STRING_ESCAPES_ALLOWED_IN_RAW_STRINGS = false;

  @cache
  static get RAW_STRINGS (): TokenPair[] { return Spark2.Tokenizer.QUOTES.flatMap((q) => [[`r${q}`, q], [`R${q}`, q]]) as TokenPair[]; }
}

class SparkParser extends Spark2.Parser {
  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> {
    return {
      ...Spark2.Parser.FUNCTIONS,
      ANY_VALUE: buildWithIgnoreNulls(AnyValueExpr),
      ARRAY_INSERT: (args: Expression[]) =>
        new ArrayInsertExpr({
          this: seqGet(args, 0),
          position: seqGet(args, 1),
          expression: seqGet(args, 2),
          offset: 1,
        }),
      BIT_AND: BitwiseAndAggExpr.fromArgList,
      BIT_OR: BitwiseOrAggExpr.fromArgList,
      BIT_XOR: BitwiseXorAggExpr.fromArgList,
      BIT_COUNT: BitwiseCountExpr.fromArgList,
      DATE_ADD: buildDateAdd,
      DATEADD: buildDateAdd,
      TIMESTAMPADD: buildDateAdd,
      TIMESTAMPDIFF: buildDateDelta(TimestampDiffExpr),
      TRY_ADD: SafeAddExpr.fromArgList,
      TRY_MULTIPLY: SafeMultiplyExpr.fromArgList,
      TRY_SUBTRACT: SafeSubtractExpr.fromArgList,
      DATEDIFF: buildDatediff,
      DATE_DIFF: buildDatediff,
      JSON_OBJECT_KEYS: JsonKeysExpr.fromArgList,
      LISTAGG: GroupConcatExpr.fromArgList,
      TIMESTAMP_LTZ: buildAsCast('TIMESTAMP_LTZ'),
      TIMESTAMP_NTZ: buildAsCast('TIMESTAMP_NTZ'),
      TRY_ELEMENT_AT: (args: Expression[]) =>
        new BracketExpr({
          this: seqGet(args, 0),
          expressions: [args[1]],
          offset: 1,
          safe: true,
        }),
      LIKE: buildLike(LikeExpr),
      ILIKE: buildLike(ILikeExpr),
    };
  }

  @cache
  static get PLACEHOLDER_PARSERS (): Partial<Record<TokenType, (this: Parser) => Expression | undefined>> {
    return {
      ...Spark2.Parser.PLACEHOLDER_PARSERS,
      [TokenType.L_BRACE]: function (this: Parser) {
        return (this as SparkParser).parseQueryParameter();
      },
    };
  }

  @cache
  static get FUNCTION_PARSERS (): Partial<Record<string, (this: Parser) => Expression | undefined>> {
    return {
      ...Spark2.Parser.FUNCTION_PARSERS,
      SUBSTR: function (this: Parser) {
        return this.parseSubstring();
      },
    };
  }

  parseQueryParameter (): Expression | undefined {
    const thisNode = this.parseIdVar();
    this.match(TokenType.R_BRACE);
    return this.expression(PlaceholderExpr, {
      this: thisNode,
      widget: true,
    });
  }

  parseGeneratedAsIdentity (): GeneratedAsIdentityColumnConstraintExpr | ComputedColumnConstraintExpr | GeneratedAsRowColumnConstraintExpr {
    const thisNode = super.parseGeneratedAsIdentity();
    if (thisNode.args.expression) {
      return this.expression(ComputedColumnConstraintExpr, { this: thisNode.args.expression });
    }
    return thisNode;
  }

  parsePivotAggregation (): Expression | undefined {
    const aggregateExpr = this.parseFunction() || this.parseDisjunction();
    return this.parseAlias(aggregateExpr);
  }
}

class SparkGenerator extends Spark2.Generator {
  static SUPPORTS_TO_NUMBER = true;
  static PAD_FILL_PATTERN_IS_REQUIRED = false;
  static SUPPORTS_CONVERT_TIMEZONE = true;
  static SUPPORTS_MEDIAN = true;
  static SUPPORTS_UNIX_SECONDS = true;
  static SUPPORTS_DECODE_CASE = true;
  static PARSE_JSON_NAME?: string;

  @cache
  static get TYPE_MAPPING () {
    return {
      ...Spark2.Generator.TYPE_MAPPING,
      [DataTypeExprKind.MONEY]: 'DECIMAL(15, 4)',
      [DataTypeExprKind.SMALLMONEY]: 'DECIMAL(6, 4)',
      [DataTypeExprKind.UUID]: 'STRING',
      [DataTypeExprKind.TIMESTAMPLTZ]: 'TIMESTAMP_LTZ',
      [DataTypeExprKind.TIMESTAMPNTZ]: 'TIMESTAMP_NTZ',
    };
  }

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (this: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (this: Generator, e: any) => string>([
      ...Spark2.Generator.TRANSFORMS.entries(),
      [
        ArrayConstructCompactExpr,
        function (this: Generator, e: ArrayConstructCompactExpr) {
          return this.func('ARRAY_COMPACT', [this.func('ARRAY', e.args.expressions || [])]);
        },
      ],
      [ArrayAppendExpr, arrayAppendSql('ARRAY_APPEND')],
      [ArrayPrependExpr, arrayAppendSql('ARRAY_PREPEND')],
      [BitwiseAndAggExpr, renameFunc('BIT_AND')],
      [BitwiseOrAggExpr, renameFunc('BIT_OR')],
      [BitwiseXorAggExpr, renameFunc('BIT_XOR')],
      [BitwiseCountExpr, renameFunc('BIT_COUNT')],
      [
        CreateExpr,
        preprocess([
          removeUniqueConstraints,
          (e) => ctasWithTmpTablesToCreateTmpView(e, temporaryStorageProvider),
          movePartitionedByToSchemaColumns,
        ]),
      ],
      [CurrentVersionExpr, renameFunc('VERSION')],
      [DateFromUnixDateExpr, renameFunc('DATE_FROM_UNIX_DATE')],
      [DatetimeAddExpr, dateDeltaToBinaryIntervalOp({ cast: false })],
      [DatetimeSubExpr, dateDeltaToBinaryIntervalOp({ cast: false })],
      [GroupConcatExpr, groupConcatSql],
      [EndsWithExpr, renameFunc('ENDSWITH')],
      [JsonKeysExpr, renameFunc('JSON_OBJECT_KEYS')],
      [
        PartitionedByPropertyExpr,
        function (this: Generator, e: PartitionedByPropertyExpr) {
          return `PARTITIONED BY ${this.wrap(
            this.expressions(undefined, {
              sqls: e.args.this?.args.expressions?.map((expr) => normalizePartition(expr as ExpressionValue)),
              skipFirst: true,
            }),
          )}`;
        },
      ],
      [SafeAddExpr, renameFunc('TRY_ADD')],
      [SafeMultiplyExpr, renameFunc('TRY_MULTIPLY')],
      [SafeSubtractExpr, renameFunc('TRY_SUBTRACT')],
      [StartsWithExpr, renameFunc('STARTSWITH')],
      [TimeAddExpr, dateDeltaToBinaryIntervalOp({ cast: false })],
      [TimeSubExpr, dateDeltaToBinaryIntervalOp({ cast: false })],
      [TsOrDsAddExpr, dateAddSql],
      [TimestampAddExpr, dateAddSql],
      [TimestampSubExpr, dateDeltaToBinaryIntervalOp({ cast: false })],
      [DatetimeDiffExpr, timestampDiffSql],
      [TimestampDiffExpr, timestampDiffSql],
      [
        TryCastExpr,
        function (this: Generator, e: TryCastExpr) {
          return e.args.safe ? this.tryCastSql(e) : this.castSql(e);
        },
      ],
    ]);

    transforms.delete(AnyValueExpr);
    transforms.delete(DateDiffExpr);

    return transforms;
  }

  bracketSql (expression: BracketExpr): string {
    if (expression.args.safe) {
      const key = seqGet(this.bracketOffsetExpressions(expression, { indexOffset: 1 }), 0);
      return this.func('TRY_ELEMENT_AT', [expression.args.this, key]);
    }
    return super.bracketSql(expression);
  }

  computedColumnConstraintSql (expression: ComputedColumnConstraintExpr): string {
    return `GENERATED ALWAYS AS (${this.sql(expression, 'this')})`;
  }

  anyValueSql (expression: AnyValueExpr): string {
    return this.functionFallbackSql(expression);
  }

  dateDiffSql (expression: DateDiffExpr): string {
    const end = this.sql(expression, 'this');
    const start = this.sql(expression, 'expression');

    if (expression.args.unit) {
      return this.func('DATEDIFF', [
        unitToVar(expression),
        start,
        end,
      ]);
    }
    return this.func('DATEDIFF', [end, start]);
  }

  placeholderSql (expression: PlaceholderExpr): string {
    if (!expression.args.widget) {
      return super.placeholderSql(expression);
    }
    return `{${expression.name}}`;
  }

  readParquetSql (expression: ReadParquetExpr): string {
    if (expression.args.expressions?.length !== 1) {
      this.unsupported('READ_PARQUET with multiple arguments is not supported');
      return '';
    }
    const parquetFile = expression.args.expressions[0];
    return `parquet.\`${parquetFile.name}\``;
  }
}

export class Spark extends Spark2 {
  static SUPPORTS_ORDER_BY_ALL = true;
  static SUPPORTS_NULL_TYPE = true;
  static ARRAY_FUNCS_PROPAGATES_NULLS = true;
  @cache
  static get EXPRESSION_METADATA () {
    return new Map(DialectTyping.EXPRESSION_METADATA);
  }

  static Tokenizer = SparkTokenizer;
  static Parser = SparkParser;
  static Generator = SparkGenerator;
}

Dialect.register(Dialects.SPARK, Spark);
