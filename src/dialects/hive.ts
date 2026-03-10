import { cache } from '../port_internals';
import type {
  AlterSetExpr,
  ColumnDefExpr,
  ConstraintExpr,
  ExistsExpr,
  RowFormatSerdePropertyExpr,
  SchemaExpr,
  StructExpr,
  TruncExpr,
  VersionExpr,
  IdentifierExpr,
  ExpressionValue,
  StarExpr,
  UnnestExpr,
} from '../expressions';
import {
  AlterColumnExpr,
  AnyValueExpr,
  ApproxDistinctExpr,
  ApproxQuantileExpr,
  ArgMaxExpr,
  ArgMinExpr,
  ArrayAggExpr,
  ArrayConcatExpr,
  ArrayExpr,
  ArraySizeExpr,
  ArraySortExpr,
  ArrayToStringExpr,
  ArrayUniqueAggExpr,
  BooleanExpr,
  CastExpr,
  ClusteredColumnConstraintExpr,
  ColumnExpr,
  CreateExpr,
  CurrentTimestampExpr,
  DataTypeExpr,
  DataTypeExprKind,
  DataTypeParamExpr,
  DateAddExpr,
  DateDiffExpr,
  DateStrToDateExpr,
  DateSubExpr,
  DateToDiExpr,
  DayExpr,
  DayOfMonthExpr,
  DayOfWeekExpr,
  DiToDateExpr,
  DistinctExpr,
  EqExpr,
  Expression,
  FileFormatPropertyExpr,
  FirstExpr,
  FirstValueExpr,
  FromBase64Expr,
  GenerateDateArrayExpr,
  GenerateSeriesExpr,
  IfExpr,
  ILikeExpr,
  IgnoreNullsExpr,
  InputOutputFormatExpr,
  InsertExpr,
  IntDivExpr,
  IsNanExpr,
  JsonExtractExpr,
  JsonExtractScalarExpr,
  JsonFormatExpr,
  JsonPathKeyExpr,
  JsonPathRootExpr,
  JsonPathSubscriptExpr,
  JsonPathWildcardExpr,
  LastExpr,
  LastValueExpr,
  LeftExpr,
  LevenshteinExpr,
  LiteralExpr,
  MapExpr,
  MaxExpr,
  Md5DigestExpr,
  MinExpr,
  MonthExpr,
  MonthsBetweenExpr,
  MulExpr,
  NationalExpr,
  NonClusteredColumnConstraintExpr,
  NotForReplicationColumnConstraintExpr,
  NotNullColumnConstraintExpr,
  NumberToStrExpr,
  OnPropertyExpr,
  OrderExpr,
  OrderedExpr,
  ParameterExpr,
  PartitionByTruncateExpr,
  PartitionedByBucketExpr,
  PartitionedByPropertyExpr,
  PrimaryKeyColumnConstraintExpr,
  PropertiesExpr,
  PropertiesLocation,
  PropertyEqExpr,
  PropertyExpr,
  QuantileExpr,
  QueryTransformExpr,
  RegexpExtractAllExpr,
  RegexpExtractExpr,
  RegexpLikeExpr,
  RegexpReplaceExpr,
  RegexpSplitExpr,
  RightExpr,
  SchemaCommentPropertyExpr,
  SelectExpr,
  SerdePropertiesExpr,
  SetItemExpr,
  SetOperationExpr,
  SplitExpr,
  StarMapExpr,
  StorageHandlerPropertyExpr,
  StrToMapExpr,
  StrPositionExpr,
  StrToDateExpr,
  StrToTimeExpr,
  StrToUnixExpr,
  StructExtractExpr,
  SubqueryExpr,
  TableExpr,
  TimeStrToDateExpr,
  TimeStrToTimeExpr,
  TimeStrToUnixExpr,
  TimeToStrExpr,
  TimeToUnixExpr,
  TimestampTruncExpr,
  ToBase64Expr,
  TransformExpr,
  TrimExpr,
  TryCastExpr,
  TsOrDiToDiExpr,
  TsOrDsAddExpr,
  TsOrDsDiffExpr,
  TsOrDsToDateExpr,
  UnicodeExpr,
  UnixToStrExpr,
  UnixToTimeExpr,
  UnixToTimeStrExpr,
  VarExpr,
  VarMapExpr,
  VolatilePropertyExpr,
  WeekOfYearExpr,
  WithDataPropertyExpr,
  WithExpr,
  YearExpr,
  toIdentifier,
  var_,
} from '../expressions';
import {
  Generator, unsupportedArgs,
} from '../generator';
import { seqGet } from '../helper';
import { Parser } from '../parser';
import {
  Tokenizer, TokenType,
} from '../tokens';
import {
  anyToExists,
  ctasWithTmpTablesToCreateTmpView,
  eliminateDistinctOn,
  eliminateQualify,
  inheritStructFieldNames,
  moveSchemaColumnsToPartitionedBy,
  preprocess,
  removeUniqueConstraints,
  unnestGenerateSeries,
  unnestToExplode,
} from '../transforms';
import { HiveTyping } from '../typing/hive';
import { TypeAnnotator } from '../optimizer';
import {
  approxCountDistinctSql,
  argMaxOrMinNoCount,
  buildFormattedTime,
  buildRegexpExtract,
  dateStrToDateSql,
  Dialect,
  Dialects,
  ifSql,
  isParseJson,
  leftToSubstringSql,
  maxOrGreatest,
  minOrLeast,
  noIlikeSql,
  noRecursiveCteSql,
  noTrycastSql,
  NormalizationStrategy,
  propertySql,
  regexpExtractSql,
  regexpReplaceSql,
  renameFunc,
  rightToSubstringSql,
  sequenceSql,
  strPositionSql,
  structExtractSql,
  timeFormat,
  timeStrToTimeSql,
  trimSql,
  unitToStr,
  varMapSql,
} from './dialect';

const DATE_DELTA_INTERVAL: Record<string, [string, number]> = {
  YEAR: ['ADD_MONTHS', 12],
  MONTH: ['ADD_MONTHS', 1],
  QUARTER: ['ADD_MONTHS', 3],
  WEEK: ['DATE_ADD', 7],
  DAY: ['DATE_ADD', 1],
};

const TIME_DIFF_FACTOR: Record<string, string> = {
  MILLISECOND: ' * 1000',
  SECOND: '',
  MINUTE: ' / 60',
  HOUR: ' / 3600',
};

const DIFF_MONTH_SWITCH = new Set([
  'YEAR',
  'QUARTER',
  'MONTH',
]);

function addDateSql (this: Generator, expression: DateAddExpr | DateSubExpr | TsOrDsAddExpr): string {
  if (expression instanceof TsOrDsAddExpr && !expression.args.unit) {
    return this.func('DATE_ADD', [expression.args.this, expression.args.expression]);
  }

  const unit = expression.text('unit').toUpperCase();
  // eslint-disable-next-line prefer-const
  let [func, multiplier] = DATE_DELTA_INTERVAL[unit] || ['DATE_ADD', 1];

  if (expression instanceof DateSubExpr) {
    multiplier *= -1;
  }

  let increment = expression.args.expression;
  if (increment instanceof LiteralExpr) {
    const value = increment.isNumber ? parseFloat(increment.args.this ?? '0') : parseInt(increment.args.this ?? '0');
    increment = LiteralExpr.number(value * multiplier);
  } else if (multiplier !== 1) {
    increment = new MulExpr({
      this: increment,
      expression: LiteralExpr.number(multiplier),
    });
  }

  return this.func(func, [expression.args.this, increment]);
}

function dateDiffSql (this: Generator, expression: DateDiffExpr | TsOrDsDiffExpr): string {
  const unit = expression.text('unit').toUpperCase();

  const factor = TIME_DIFF_FACTOR[unit];
  if (factor !== undefined) {
    const left = this.sql(expression, 'this');
    const right = this.sql(expression, 'expression');
    const secDiff = `UNIX_TIMESTAMP(${left}) - UNIX_TIMESTAMP(${right})`;
    return factor ? `(${secDiff})${factor}` : secDiff;
  }

  const monthsBetween = DIFF_MONTH_SWITCH.has(unit);
  const sqlFunc = monthsBetween ? 'MONTHS_BETWEEN' : 'DATEDIFF';
  const multiplier = DATE_DELTA_INTERVAL[unit]?.[1] || 1;
  const multiplierSql = 1 < multiplier ? ` / ${multiplier}` : '';

  let diffSql = `${sqlFunc}(${this.formatArgs([expression.args.this, expression.args.expression])})`;

  if (monthsBetween || multiplierSql) {
    diffSql = `CAST(${diffSql}${multiplierSql} AS INT)`;
  }

  return diffSql;
}

function jsonFormatSql (this: Generator, expression: JsonFormatExpr): string {
  const thisNode = expression.args.this;

  if (thisNode && isParseJson(thisNode)) {
    if (thisNode.args.this instanceof LiteralExpr && thisNode.args.this.isString) {
      const wrappedJson = LiteralExpr.string(`[${thisNode.args.this.args.this}]`);

      const fromJson = this.func('FROM_JSON', [wrappedJson, this.func('SCHEMA_OF_JSON', [wrappedJson])]);
      const toJson = this.func('TO_JSON', [fromJson]);

      return this.func('REGEXP_EXTRACT', [
        toJson,
        LiteralExpr.string('\'^.(.*).$\''),
        LiteralExpr.number(1),
      ]);
    }
    return this.sql(thisNode);
  }

  return this.func('TO_JSON', [thisNode, expression.args.options]);
}

function arraySortSql (this: Generator, expression: ArraySortExpr): string {
  if (expression.args.expression) {
    this.unsupported('Unsupported arg \'expression\' for ArraySort');
  }
  return this.func('SORT_ARRAY', [expression.args.this]);
}

function strToUnixSql (this: Generator, expression: StrToUnixExpr): string {
  return this.func('UNIX_TIMESTAMP', [expression.args.this, timeFormat('hive').call(this, expression)]);
}

function unixToTimeSql (this: Generator, expression: UnixToTimeExpr): string {
  const timestamp = this.sql(expression, 'this');
  const scale = expression.args.scale;

  if (scale === undefined || scale.toValue() === UnixToTimeExpr.SECONDS.toValue()) {
    return renameFunc('FROM_UNIXTIME').call(this, expression);
  }

  return `FROM_UNIXTIME(${timestamp} / POW(10, ${scale}))`;
}

function strToDateSql (this: Generator, expression: StrToDateExpr): string {
  let thisSql = this.sql(expression, 'this');
  const timeFormatSql = this.formatTime(expression);

  if (timeFormatSql !== null && timeFormatSql !== undefined && ![Hive.TIME_FORMAT, Hive.DATE_FORMAT].includes(timeFormatSql)) {
    thisSql = `FROM_UNIXTIME(UNIX_TIMESTAMP(${thisSql}, ${timeFormatSql}))`;
  }

  return `CAST(${thisSql} AS DATE)`;
}

function strToTimeSql (this: Generator, expression: StrToTimeExpr): string {
  let thisSql = this.sql(expression, 'this');
  const timeFormatSql = this.formatTime(expression);

  if (timeFormatSql !== null && timeFormatSql !== undefined && ![Hive.TIME_FORMAT, Hive.DATE_FORMAT].includes(timeFormatSql)) {
    thisSql = `FROM_UNIXTIME(UNIX_TIMESTAMP(${thisSql}, ${timeFormatSql}))`;
  }

  return `CAST(${thisSql} AS TIMESTAMP)`;
}

function toDateSql (this: Generator, expression: TsOrDsToDateExpr): string {
  const timeFormatSql = this.formatTime(expression);

  if (timeFormatSql && ![Hive.TIME_FORMAT, Hive.DATE_FORMAT].includes(timeFormatSql)) {
    return this.func('TO_DATE', [expression.args.this, timeFormatSql]);
  }

  if (expression.parent instanceof Expression && (this._constructor as typeof HiveGenerator).TS_OR_DS_EXPRESSIONS.has(expression.parent._constructor)) {
    return this.sql(expression, 'this');
  }

  return this.func('TO_DATE', [expression.args.this]);
}

export function buildWithIgnoreNulls (ExpClass: typeof Expression) {
  return (args: Expression[]) => {
    const thisNode = new ExpClass({ this: seqGet(args, 0) });
    if (seqGet(args, 1) instanceof BooleanExpr && seqGet(args, 1)?.args.this === true) {
      return new IgnoreNullsExpr({ this: thisNode });
    }
    return thisNode;
  };
}

function buildToDate (args: Expression[]): TsOrDsToDateExpr {
  const expr = buildFormattedTime(TsOrDsToDateExpr, { dialect: 'hive' })(args);
  expr.setArgKey('safe', true);
  return expr;
}

function buildDateAdd (args: Expression[]): TsOrDsAddExpr {
  let expression = seqGet(args, 1);
  if (expression) {
    expression = expression.mul(-1);
  }

  return new TsOrDsAddExpr({
    this: seqGet(args, 0),
    expression: expression,
    unit: var_('DAY'),
  });
}

class HiveTokenizer extends Tokenizer {
  @cache
  static get QUOTES () {
    return ['\'', '"'];
  }

  @cache
  static get IDENTIFIERS () {
    return ['`'];
  }

  @cache
  static get STRING_ESCAPES () {
    return ['\\'];
  }

  @cache
  static get SINGLE_TOKENS () {
    return {
      ...Tokenizer.SINGLE_TOKENS,
      $: TokenType.PARAMETER,
    };
  }

  @cache
  static get ORIGINAL_KEYWORDS (): Record<string, TokenType> {
    return {
      ...Tokenizer.KEYWORDS,
      'ADD ARCHIVE': TokenType.COMMAND,
      'ADD ARCHIVES': TokenType.COMMAND,
      'ADD FILE': TokenType.COMMAND,
      'ADD FILES': TokenType.COMMAND,
      'ADD JAR': TokenType.COMMAND,
      'ADD JARS': TokenType.COMMAND,
      'MINUS': TokenType.EXCEPT,
      'MSCK REPAIR': TokenType.COMMAND,
      'REFRESH': TokenType.REFRESH,
      'TIMESTAMP AS OF': TokenType.TIMESTAMP_SNAPSHOT,
      'VERSION AS OF': TokenType.VERSION_SNAPSHOT,
      'SERDEPROPERTIES': TokenType.SERDE_PROPERTIES,
    };
  }

  @cache
  static get NUMERIC_LITERALS () {
    return {
      L: 'BIGINT',
      S: 'SMALLINT',
      Y: 'TINYINT',
      D: 'DOUBLE',
      F: 'FLOAT',
      BD: 'DECIMAL',
    };
  }
}

class HiveParser extends Parser {
  @cache
  static get ID_VAR_TOKENS (): Set<TokenType> {
    return new Set([
      ...Parser.ID_VAR_TOKENS,
      TokenType.SESSION_USER,
      TokenType.CURRENT_CATALOG,
      TokenType.STRAIGHT_JOIN,
    ]);
  }

  // port from _Dialect metaclass logic
  @cache
  static get NO_PAREN_FUNCTIONS () {
    const noParenFunctions = { ...Parser.NO_PAREN_FUNCTIONS };
    delete noParenFunctions[TokenType.CURRENT_TIME];
    delete noParenFunctions[TokenType.LOCALTIME];
    delete noParenFunctions[TokenType.LOCALTIMESTAMP];
    return noParenFunctions;
  }

  static LOG_DEFAULTS_TO_LN = true;
  static STRICT_CAST = false;
  static VALUES_FOLLOWED_BY_PAREN = false;
  static JOINS_HAVE_EQUAL_PRECEDENCE = true;
  static ADD_JOIN_ON_TRUE = true;
  static ALTER_TABLE_PARTITIONS = true;
  static CHANGE_COLUMN_ALTER_SYNTAX = false;

  @cache
  static get FUNCTION_PARSERS (): Partial<Record<string, (this: Parser) => Expression | undefined>> {
    return {
      ...Parser.FUNCTION_PARSERS,
      PERCENTILE: function (this: Parser) {
        return (this as HiveParser).parseQuantileFunction(QuantileExpr);
      },
      PERCENTILE_APPROX: function (this: Parser) {
        return (this as HiveParser).parseQuantileFunction(ApproxQuantileExpr);
      },
    };
  }

  @cache
  static get FUNCTIONS (): Record<string, (expression: Expression[], options: { dialect: Dialect }) => Expression> {
    return {
      ...Parser.FUNCTIONS,
      ADD_MONTHS: (args: Expression[]): TsOrDsAddExpr => new TsOrDsAddExpr({
        this: seqGet(args, 0),
        expression: seqGet(args, 1),
        unit: var_('MONTH'),
      }),
      BASE64: (args: unknown[]) => ToBase64Expr.fromArgList(args),
      COLLECT_LIST: (args: Expression[]) => new ArrayAggExpr({
        this: seqGet(args, 0),
        nullsExcluded: true,
      }),
      COLLECT_SET: (args: unknown[]) => ArrayUniqueAggExpr.fromArgList(args),
      DATE_ADD: (args: Expression[]) => new TsOrDsAddExpr({
        this: seqGet(args, 0),
        expression: seqGet(args, 1),
        unit: new VarExpr({ this: 'DAY' }),
      }),
      DATE_FORMAT: (args: Expression[]) => buildFormattedTime(TimeToStrExpr, { dialect: 'hive' })([new TimeStrToTimeExpr({ this: seqGet(args, 0) }), seqGet(args, 1)]),
      DATE_SUB: buildDateAdd,
      DATEDIFF: (args: Expression[]) => new DateDiffExpr({
        this: new TsOrDsToDateExpr({ this: seqGet(args, 0) }),
        expression: new TsOrDsToDateExpr({ this: seqGet(args, 1) }),
      }),
      DAY: (args: Expression[]) => new DayExpr({ this: new TsOrDsToDateExpr({ this: seqGet(args, 0) }) }),
      FIRST: buildWithIgnoreNulls(FirstExpr),
      FIRST_VALUE: buildWithIgnoreNulls(FirstValueExpr),
      FROM_UNIXTIME: buildFormattedTime(UnixToStrExpr, {
        dialect: 'hive',
        defaultValue: true,
      }),
      GET_JSON_OBJECT: (args: Expression[], { dialect }: { dialect: Dialect }) => new JsonExtractScalarExpr({
        this: seqGet(args, 0),
        expression: dialect.toJsonPath(seqGet(args, 1)),
      }),
      LAST: buildWithIgnoreNulls(LastExpr),
      LAST_VALUE: buildWithIgnoreNulls(LastValueExpr),
      MAP: (args: Expression[]) => {
        if (args.length === 1 && args[0].isStar) {
          return new StarMapExpr({
            this: args[0],
          });
        }
        const keys: Expression[] = [];
        const values: Expression[] = [];
        for (let i = 0; i < args.length; i += 2) {
          keys.push(args[i]);
          if (args[i + 1]) values.push(args[i + 1]);
        }
        return new VarMapExpr({
          keys: new ArrayExpr({ expressions: keys }),
          values: new ArrayExpr({ expressions: values }),
        });
      },
      MONTH: (args: Expression[]) => new MonthExpr({ this: new TsOrDsToDateExpr({ this: seqGet(args, 0) }) }),
      REGEXP_EXTRACT: buildRegexpExtract(RegexpExtractExpr),
      REGEXP_EXTRACT_ALL: buildRegexpExtract(RegexpExtractAllExpr),
      SEQUENCE: (args: unknown[]) => GenerateSeriesExpr.fromArgList(args),
      SIZE: (args: unknown[]) => ArraySizeExpr.fromArgList(args),
      SPLIT: (args: unknown[]) => RegexpSplitExpr.fromArgList(args),
      STR_TO_MAP: (args: Expression[]) => new StrToMapExpr({
        this: seqGet(args, 0),
        pairDelim: seqGet(args, 1) || LiteralExpr.string(','),
        keyValueDelim: seqGet(args, 2) || LiteralExpr.string(':'),
      }),
      TO_DATE: buildToDate,
      TO_JSON: (args: unknown[]) => JsonFormatExpr.fromArgList(args),
      TRUNC: (args: unknown[]) => TimestampTruncExpr.fromArgList(args),
      UNBASE64: (args: unknown[]) => FromBase64Expr.fromArgList(args),
      UNIX_TIMESTAMP: (args: Expression[]) => buildFormattedTime(StrToUnixExpr, {
        dialect: 'hive',
        defaultValue: true,
      })(
        0 < args.length ? args : [new CurrentTimestampExpr({})],
      ),
      YEAR: (args: Expression[]) => new YearExpr({ this: new TsOrDsToDateExpr({ this: seqGet(args, 0) }) }),
    };
  }

  @cache
  static get NO_PAREN_FUNCTION_PARSERS (): Partial<Record<string, (this: Parser) => Expression | undefined>> {
    return {
      ...Parser.NO_PAREN_FUNCTION_PARSERS,
      TRANSFORM: function (this: Parser) {
        return (this as HiveParser).parseTransform();
      },
    };
  }

  @cache
  static get PROPERTY_PARSERS (): Record<string, (this: Parser, ...args: unknown[]) => Expression | Expression[] | undefined> {
    return {
      ...Parser.PROPERTY_PARSERS,
      SERDEPROPERTIES: function (this: Parser) {
        return new SerdePropertiesExpr({
          expressions: this.parseWrappedCsv(() => this.parseProperty() as Expression | undefined),
        });
      },
    };
  }

  @cache
  static get ALTER_PARSERS (): Partial<Record<string, (this: Parser) => Expression | Expression[] | undefined>> {
    return {
      ...Parser.ALTER_PARSERS,
      CHANGE: function (this: Parser) {
        return (this as HiveParser).parseAlterTableChange();
      },
    };
  }

  parseTransform (): TransformExpr | QueryTransformExpr | undefined {
    if (!this.match(TokenType.L_PAREN, { advance: false })) {
      this.retreat(this.index - 1);
      return undefined;
    }

    const args = this.parseWrappedCsv(() => (this as HiveParser).parseLambda());
    const rowFormatBefore = (this as HiveParser).parseRowFormat({ matchRow: true });

    let recordWriter: string | Expression | undefined = undefined;
    if (this.matchTextSeq('RECORDWRITER')) {
      recordWriter = (this as HiveParser).parseString();
    }

    if (!this.match(TokenType.USING)) {
      return TransformExpr.fromArgList(args);
    }

    const commandScript = (this as HiveParser).parseString();

    this.match(TokenType.ALIAS);
    const schema = this.parseSchema();

    const rowFormatAfter = (this as HiveParser).parseRowFormat({ matchRow: true });
    let recordReader: string | Expression | undefined = undefined;
    if (this.matchTextSeq('RECORDREADER')) {
      recordReader = (this as HiveParser).parseString();
    }

    return this.expression(QueryTransformExpr, {
      expressions: args,
      commandScript: commandScript,
      schema: schema,
      rowFormatBefore: rowFormatBefore,
      recordWriter: recordWriter,
      rowFormatAfter: rowFormatAfter,
      recordReader: recordReader,
    });
  }

  parseQuantileFunction (FuncClass: typeof QuantileExpr | typeof ApproxQuantileExpr): QuantileExpr | ApproxQuantileExpr {
    let firstArg: Expression | undefined;
    if (this.match(TokenType.DISTINCT)) {
      firstArg = this.expression(DistinctExpr, { expressions: [(this as HiveParser).parseLambda()] });
    } else {
      this.match(TokenType.ALL);
      firstArg = (this as HiveParser).parseLambda();
    }

    const args = [firstArg];
    if (this.match(TokenType.COMMA)) {
      args.push(...this.parseFunctionArgs());
    }

    return FuncClass.fromArgList(args);
  }

  parseTypes (options: { checkFunc?: boolean;
    schema?: boolean;
    allowIdentifiers?: boolean; } = {}): Expression | undefined {
    const {
      checkFunc = false, schema = false, allowIdentifiers = true,
    } = options;
    const thisNode = super.parseTypes({
      checkFunc,
      schema,
      allowIdentifiers,
    });

    if (thisNode && !schema) {
      return thisNode.transform((node: Expression, _opts: Record<string, unknown>): string | Expression | undefined => {
        if (node instanceof DataTypeExpr && node.isType(['char', 'varchar'])) {
          return node.replace(DataTypeExpr.build('text')) as Expression | undefined;
        }
        return node;
      }, { copy: false });
    }

    return thisNode;
  }

  parseAlterTableChange (): Expression | undefined {
    this.match(TokenType.COLUMN);
    const thisNode = this.parseField({ anyToken: true });

    if ((this.constructor as typeof HiveParser).CHANGE_COLUMN_ALTER_SYNTAX && this.matchTextSeq('TYPE')) {
      return this.expression(AlterColumnExpr, {
        this: thisNode,
        dtype: this.parseTypes({ schema: true }),
      });
    }

    const columnNew = this.parseField({ anyToken: true });
    const dtype = this.parseTypes({ schema: true });

    const comment = this.match(TokenType.COMMENT) && (this as HiveParser).parseString();

    if (!thisNode || !columnNew || !dtype) {
      this.raiseError('Expected \'CHANGE COLUMN\' to be followed by \'column_name\' \'column_name\' \'data_type\'');
    }

    return this.expression(AlterColumnExpr, {
      this: thisNode,
      renameTo: columnNew,
      dtype: dtype,
      comment: comment,
    });
  }

  parsePartitionAndOrder (): [Expression[], Expression | undefined] {
    return [
      this.matchSet([TokenType.PARTITION_BY, TokenType.DISTRIBUTE_BY])
        ? this.parseCsv(() => this.parseAssignment())
        : [],
      super.parseOrder({ skipOrderToken: this.match(TokenType.SORT_BY) }) ?? undefined,
    ];
  }

  parseParameter (): ParameterExpr {
    this.match(TokenType.L_BRACE);
    const thisNode = this.parseIdentifier() || (this as HiveParser).parsePrimaryOrVar();
    const expression =
      this.match(TokenType.COLON) && (this.parseIdentifier() || (this as HiveParser).parsePrimaryOrVar());
    this.match(TokenType.R_BRACE);

    return this.expression(ParameterExpr, {
      this: thisNode,
      expression: expression,
    });
  }

  toPropEq (expression: Expression, index: number): Expression {
    if (expression.isStar) {
      return expression;
    }

    let key: ExpressionValue<IdentifierExpr | StarExpr>;
    if (expression instanceof ColumnExpr) {
      key = expression.args.this ?? toIdentifier(`col${index + 1}`);
    } else {
      key = toIdentifier(`col${index + 1}`);
    }

    return this.expression(PropertyEqExpr, {
      this: key,
      expression: expression,
    });
  }

  // port from _Dialect metaclass logic
  @cache
  static get TABLE_ALIAS_TOKENS (): Set<TokenType> {
    return new Set([...Parser.TABLE_ALIAS_TOKENS, TokenType.STRAIGHT_JOIN]);
  }
}
class HiveGenerator extends Generator {
  // port from _Dialect metaclass logic
  static SUPPORTS_DECODE_CASE = false;
  // port from _Dialect metaclass logic
  static readonly SELECT_KINDS: string[] = [];
  // port from _Dialect metaclass logic
  static TRY_SUPPORTED = false;
  // port from _Dialect metaclass logic
  static SUPPORTS_UESCAPE = false;
  static LIMIT_FETCH = 'LIMIT';
  static TABLESAMPLE_WITH_METHOD = false;
  static JOIN_HINTS = false;
  static TABLE_HINTS = false;
  static QUERY_HINTS = false;
  static INDEX_ON = 'ON TABLE';
  static EXTRACT_ALLOWS_QUOTES = false;
  static NVL2_SUPPORTED = false;
  static LAST_DAY_SUPPORTS_DATE_PART = false;
  static JSON_PATH_SINGLE_QUOTE_ESCAPE = true;
  static SUPPORTS_TO_NUMBER = false;
  static WITH_PROPERTIES_PREFIX = 'TBLPROPERTIES';
  static PARSE_JSON_NAME?: string = undefined;
  static PAD_FILL_PATTERN_IS_REQUIRED = true;
  static SUPPORTS_MEDIAN = false;
  static ARRAY_SIZE_NAME = 'SIZE';
  static ALTER_SET_TYPE = '';

  @cache
  static get EXPRESSIONS_WITHOUT_NESTED_CTES () {
    return new Set([
      InsertExpr,
      SelectExpr,
      SubqueryExpr,
      SetOperationExpr,
    ]);
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
    return new Map([
      ...Generator.TYPE_MAPPING,
      [DataTypeExprKind.BIT, 'BOOLEAN'],
      [DataTypeExprKind.BLOB, 'BINARY'],
      [DataTypeExprKind.DATETIME, 'TIMESTAMP'],
      [DataTypeExprKind.ROWVERSION, 'BINARY'],
      [DataTypeExprKind.TEXT, 'STRING'],
      [DataTypeExprKind.TIME, 'TIMESTAMP'],
      [DataTypeExprKind.TIMESTAMPNTZ, 'TIMESTAMP'],
      [DataTypeExprKind.TIMESTAMPTZ, 'TIMESTAMP'],
      [DataTypeExprKind.UTINYINT, 'SMALLINT'],
      [DataTypeExprKind.VARBINARY, 'BINARY'],
    ]);
  }

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (this: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (this: Generator, e: any) => string>([
      ...Generator.TRANSFORMS,
      [PropertyExpr, propertySql],
      [AnyValueExpr, renameFunc('FIRST')],
      [ApproxDistinctExpr, approxCountDistinctSql],
      [ArgMaxExpr, argMaxOrMinNoCount('MAX_BY')],
      [ArgMinExpr, argMaxOrMinNoCount('MIN_BY')],
      [ArrayExpr, preprocess([inheritStructFieldNames])],
      [ArrayConcatExpr, renameFunc('CONCAT')],
      [
        ArrayToStringExpr,
        function (this: Generator, e: ArrayToStringExpr) {
          return this.func('CONCAT_WS', [e.args.expression, e.args.this]);
        },
      ],
      [
        ArraySortExpr,
        function (this: Generator, e: ArraySortExpr) {
          return arraySortSql.call(this as HiveGenerator, e);
        },
      ],
      [WithExpr, noRecursiveCteSql],
      [
        DateAddExpr,
        function (this: Generator, e: DateAddExpr) {
          return addDateSql.call(this as HiveGenerator, e);
        },
      ],
      [
        DateDiffExpr,
        function (this: Generator, e: DateDiffExpr) {
          return dateDiffSql.call(this as HiveGenerator, e);
        },
      ],
      [DateStrToDateExpr, dateStrToDateSql],
      [
        DateSubExpr,
        function (this: Generator, e: DateSubExpr) {
          return addDateSql.call(this as HiveGenerator, e);
        },
      ],
      [
        DateToDiExpr,
        function (this: Generator, e: DateToDiExpr) {
          return `CAST(DATE_FORMAT(${this.sql(e, 'this')}, ${Hive.DATEINT_FORMAT}) AS INT)`;
        },
      ],
      [
        DiToDateExpr,
        function (this: Generator, e: DiToDateExpr) {
          return `TO_DATE(CAST(${this.sql(e, 'this')} AS STRING), ${Hive.DATEINT_FORMAT})`;
        },
      ],
      [
        StorageHandlerPropertyExpr,
        function (this: Generator, e: StorageHandlerPropertyExpr) {
          return `STORED BY ${this.sql(e, 'this')}`;
        },
      ],
      [FromBase64Expr, renameFunc('UNBASE64')],
      [GenerateSeriesExpr, sequenceSql],
      [GenerateDateArrayExpr, sequenceSql],
      [IfExpr, ifSql()],
      [ILikeExpr, noIlikeSql],
      [
        IntDivExpr,
        function (this: Generator, e: IntDivExpr) {
          return this.binary(e, 'DIV');
        },
      ],
      [IsNanExpr, renameFunc('ISNAN')],
      [
        JsonExtractExpr,
        function (this: Generator, e: JsonExtractExpr) {
          return this.func('GET_JSON_OBJECT', [e.args.this, e.args.expression]);
        },
      ],
      [
        JsonExtractScalarExpr,
        function (this: Generator, e: JsonExtractScalarExpr) {
          return this.func('GET_JSON_OBJECT', [e.args.this, e.args.expression]);
        },
      ],
      [
        JsonFormatExpr,
        function (this: Generator, e: JsonFormatExpr) {
          return jsonFormatSql.call(this as HiveGenerator, e);
        },
      ],
      [LeftExpr, leftToSubstringSql],
      [MapExpr, varMapSql],
      [MaxExpr, maxOrGreatest],
      [
        Md5DigestExpr,
        function (this: Generator, e: Md5DigestExpr) {
          return this.func('UNHEX', [this.func('MD5', [e.args.this])]);
        },
      ],
      [MinExpr, minOrLeast],
      [
        MonthsBetweenExpr,
        function (this: Generator, e: MonthsBetweenExpr) {
          return this.func('MONTHS_BETWEEN', [e.args.this, e.args.expression]);
        },
      ],
      [
        NotNullColumnConstraintExpr,
        function (this: Generator, e: NotNullColumnConstraintExpr) {
          return e.args.allowNull ? '' : 'NOT NULL';
        },
      ],
      [VarMapExpr, varMapSql],
      [
        CreateExpr,
        preprocess([
          removeUniqueConstraints,
          ctasWithTmpTablesToCreateTmpView,
          moveSchemaColumnsToPartitionedBy,
        ]),
      ],
      [QuantileExpr, renameFunc('PERCENTILE')],
      [ApproxQuantileExpr, renameFunc('PERCENTILE_APPROX')],
      [RegexpExtractExpr, regexpExtractSql],
      [RegexpExtractAllExpr, regexpExtractSql],
      [RegexpReplaceExpr, regexpReplaceSql],
      [
        RegexpLikeExpr,
        function (this: Generator, e: RegexpLikeExpr) {
          return this.binary(e, 'RLIKE');
        },
      ],
      [RegexpSplitExpr, renameFunc('SPLIT')],
      [RightExpr, rightToSubstringSql],
      [
        SchemaCommentPropertyExpr,
        function (this: Generator, e: SchemaCommentPropertyExpr) {
          return this.nakedProperty(e);
        },
      ],
      [ArrayUniqueAggExpr, renameFunc('COLLECT_SET')],
      [
        SplitExpr,
        function (this: Generator, e: SplitExpr) {
          return this.func('SPLIT', [
            e.args.this,
            this.func('CONCAT', [
              LiteralExpr.string('\\Q'),
              e.args.expression,
              LiteralExpr.string('\\E'),
            ]),
          ]);
        },
      ],
      [
        SelectExpr,
        preprocess([
          eliminateQualify,
          eliminateDistinctOn,
          (e: Expression) => unnestToExplode(e, { unnestUsingArraysZip: false }),
          anyToExists,
        ]),
      ],
      [
        StrPositionExpr,
        function (this: Generator, e: StrPositionExpr) {
          return strPositionSql.call(this, e, {
            funcName: 'LOCATE',
            supportsPosition: true,
          });
        },
      ],
      [
        StrToDateExpr,
        function (this: Generator, e: StrToDateExpr) {
          return strToDateSql.call(this as HiveGenerator, e);
        },
      ],
      [
        StrToTimeExpr,
        function (this: Generator, e: StrToTimeExpr) {
          return strToTimeSql.call(this as HiveGenerator, e);
        },
      ],
      [
        StrToUnixExpr,
        function (this: Generator, e: StrToUnixExpr) {
          return strToUnixSql.call(this as HiveGenerator, e);
        },
      ],
      [StructExtractExpr, structExtractSql],
      [StarMapExpr, renameFunc('MAP')],
      [TableExpr, preprocess([unnestGenerateSeries])],
      [TimeStrToDateExpr, renameFunc('TO_DATE')],
      [TimeStrToTimeExpr, timeStrToTimeSql],
      [TimeStrToUnixExpr, renameFunc('UNIX_TIMESTAMP')],
      [
        TimestampTruncExpr,
        function (this: Generator, e: TimestampTruncExpr) {
          return this.func('TRUNC', [e.args.this, unitToStr(e)]);
        },
      ],
      [TimeToUnixExpr, renameFunc('UNIX_TIMESTAMP')],
      [ToBase64Expr, renameFunc('BASE64')],
      [
        TsOrDiToDiExpr,
        function (this: Generator, e: TsOrDiToDiExpr) {
          return `CAST(SUBSTR(REPLACE(CAST(${this.sql(e, 'this')} AS STRING), '-', ''), 1, 8) AS INT)`;
        },
      ],
      [
        TsOrDsAddExpr,
        function (this: Generator, e: TsOrDsAddExpr) {
          return addDateSql.call(this as HiveGenerator, e);
        },
      ],
      [
        TsOrDsDiffExpr,
        function (this: Generator, e: TsOrDsDiffExpr) {
          return dateDiffSql.call(this as HiveGenerator, e);
        },
      ],
      [
        TsOrDsToDateExpr,
        function (this: Generator, e: TsOrDsToDateExpr) {
          return toDateSql.call(this as HiveGenerator, e);
        },
      ],
      [TryCastExpr, noTrycastSql],
      [TrimExpr, trimSql],
      [UnicodeExpr, renameFunc('ASCII')],
      [
        UnixToStrExpr,
        function (this: Generator, e: UnixToStrExpr) {
          return this.func('FROM_UNIXTIME', [e.args.this, timeFormat('hive').call(this, e)]);
        },
      ],
      [
        UnixToTimeExpr,
        function (this: Generator, e: UnixToTimeExpr) {
          return unixToTimeSql.call(this as HiveGenerator, e);
        },
      ],
      [UnixToTimeStrExpr, renameFunc('FROM_UNIXTIME')],
      [
        PartitionedByPropertyExpr,
        function (this: Generator, e: PartitionedByPropertyExpr) {
          return `PARTITIONED BY ${this.sql(e, 'this')}`;
        },
      ],
      [NumberToStrExpr, renameFunc('FORMAT_NUMBER')],
      [
        NationalExpr,
        function (this: Generator, e: NationalExpr) {
          return this.nationalSql(e, { prefix: '' });
        },
      ],
      [
        ClusteredColumnConstraintExpr,
        function (this: Generator, e: ClusteredColumnConstraintExpr) {
          return `(${this.expressions(e, {
            key: 'this',
            indent: false,
          })})`;
        },
      ],
      [
        NonClusteredColumnConstraintExpr,
        function (this: Generator, e: NonClusteredColumnConstraintExpr) {
          return `(${this.expressions(e, {
            key: 'this',
            indent: false,
          })})`;
        },
      ],
      [NotForReplicationColumnConstraintExpr, () => ''],
      [OnPropertyExpr, () => ''],
      [
        PartitionedByBucketExpr,
        function (this: Generator, e: PartitionedByBucketExpr) {
          return this.func('BUCKET', [e.args.expression, e.args.this]);
        },
      ],
      [
        PartitionByTruncateExpr,
        function (this: Generator, e: PartitionByTruncateExpr) {
          return this.func('TRUNCATE', [e.args.expression, e.args.this]);
        },
      ],
      [PrimaryKeyColumnConstraintExpr, () => 'PRIMARY KEY'],
      [WeekOfYearExpr, renameFunc('WEEKOFYEAR')],
      [DayOfMonthExpr, renameFunc('DAYOFMONTH')],
      [DayOfWeekExpr, renameFunc('DAYOFWEEK')],
      [
        LevenshteinExpr,
        function (this: Generator, e: Expression) {
          unsupportedArgs.call(this, e, 'insCost', 'delCost', 'subCost', 'maxDist');
          return renameFunc('LEVENSHTEIN').call(this, e);
        },
      ],
    ]);
    return transforms;
  }

  @cache
  static get PROPERTIES_LOCATION (): Map<typeof Expression, PropertiesLocation> {
    return new Map([
      ...Generator.PROPERTIES_LOCATION,
      [FileFormatPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [PartitionedByPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [VolatilePropertyExpr, PropertiesLocation.UNSUPPORTED],
      [WithDataPropertyExpr, PropertiesLocation.UNSUPPORTED],
    ] as [typeof Expression, PropertiesLocation][]);
  }

  @cache
  static get TS_OR_DS_EXPRESSIONS (): Set<typeof Expression> {
    return new Set([
      DateDiffExpr,
      DayExpr,
      MonthExpr,
      YearExpr,
    ]);
  }

  unnestSql (expression: UnnestExpr): string {
    return renameFunc('EXPLODE').call(this, expression);
  }

  jsonPathKeySql (expression: JsonPathKeyExpr): string {
    if (expression.args.this instanceof JsonPathWildcardExpr) {
      this.unsupported('Unsupported wildcard in JSONPathKey expression');
      return '';
    }

    return super.jsonPathKeySql(expression);
  }

  parameterSql (expression: ParameterExpr): string {
    let thisSql = this.sql(expression, 'this');
    const expressionSql = this.sql(expression, 'expression');

    const parent = expression.parent;
    thisSql = expressionSql ? `${thisSql}:${expressionSql}` : thisSql;

    if (parent instanceof EqExpr && parent.parent instanceof SetItemExpr) {
      return thisSql;
    }

    return `\${${thisSql}}`;
  }

  schemaSql (expression: SchemaExpr): string {
    for (const ordered of expression.findAll(OrderedExpr)) {
      if (ordered.args.desc === false) {
        ordered.setArgKey('desc', undefined);
      }
    }

    return super.schemaSql(expression);
  }

  constraintSql (expression: ConstraintExpr): string {
    for (const prop of Array.from(expression.findAll(PropertiesExpr))) {
      prop.pop();
    }

    const thisSql = this.sql(expression, 'this');
    const expressions = this.expressions(expression, {
      sep: ' ',
      flat: true,
    });
    return `CONSTRAINT ${thisSql} ${expressions}`;
  }

  rowFormatSerdePropertySql (expression: RowFormatSerdePropertyExpr): string {
    let serdeProps = this.sql(expression, 'serdeProperties');
    serdeProps = serdeProps ? ` ${serdeProps}` : '';
    return `ROW FORMAT SERDE ${this.sql(expression, 'this')}${serdeProps}`;
  }

  arrayAggSql (expression: ArrayAggExpr): string {
    return this.func(
      'COLLECT_LIST',
      [expression.args.this instanceof OrderExpr ? expression.args.this.args.this : expression.args.this],
    );
  }

  truncSql (expression: TruncExpr): string {
    if (expression.args.decimals) {
      this.unsupported('Unsupported arg \'decimals\' for TRUNC');
    }
    return this.sql(new CastExpr({
      this: expression.args.this,
      to: DataTypeExpr.build('bigint'),
    }));
  }

  dataTypeSql (expression: DataTypeExpr): string {
    if (
      expression.isType(Generator.PARAMETERIZABLE_TEXT_TYPES)
      && (!expression.args.expressions || (expression.args.expressions[0] as IdentifierExpr).name === 'MAX')
    ) {
      expression = DataTypeExpr.build('text') ?? expression;
    } else if (expression.isType(DataTypeExprKind.TEXT) && expression.args.expressions) {
      expression.setArgKey('this', DataTypeExprKind.VARCHAR);
    } else if (expression.isType(DataTypeExpr.TEMPORAL_TYPES)) {
      expression = DataTypeExpr.build(expression.args.this) ?? expression;
    } else if (expression.isType('float')) {
      const sizeExpression = expression.find(DataTypeParamExpr);
      if (sizeExpression instanceof DataTypeParamExpr) {
        const size = parseInt(sizeExpression.args.this?.args.this?.toString() ?? '0');
        expression = (size <= 32 ? DataTypeExpr.build('float') : DataTypeExpr.build('double')) ?? expression;
      }
    }

    return super.dataTypeSql(expression);
  }

  versionSql (expression: VersionExpr): string {
    const sql = super.versionSql(expression);
    return sql.replace('FOR ', '');
  }

  structSql (expression: StructExpr): string {
    const values: (string | Expression)[] = [];

    expression.args.expressions?.forEach((e) => {
      if (e instanceof PropertyEqExpr) {
        this.unsupported('Hive does not support named structs.');
        const exprVal = e.args.expression;
        values.push((exprVal instanceof Expression || typeof exprVal === 'string') ? exprVal : '');
      } else {
        values.push(e);
      }
    });

    return this.func('STRUCT', values);
  }

  columnDefSql (expression: ColumnDefExpr, options: { sep?: string } = {}): string {
    const { sep = ' ' } = options;
    return super.columnDefSql(expression, {
      sep:
        expression.parent instanceof DataTypeExpr && expression.parent.isType('struct')
          ? ': '
          : sep,
    });
  }

  alterColumnSql (expression: AlterColumnExpr): string {
    const thisSql = this.sql(expression, 'this');
    const newName = this.sql(expression, 'renameTo') || thisSql;
    const dtype = this.sql(expression, 'dtype');
    const commentSql = this.sql(expression, 'comment');
    const comment = commentSql ? ` COMMENT ${commentSql}` : '';

    const defaultVal = this.sql(expression, 'default');
    const visible = expression.args.visible;
    const allowNoNull = expression.args.allowNull;
    const drop = expression.args.drop;

    if (defaultVal || drop || visible || allowNoNull) {
      this.unsupported('Unsupported CHANGE COLUMN syntax');
    }

    if (!dtype) {
      this.unsupported('CHANGE COLUMN without a type is not supported');
    }

    return `CHANGE COLUMN ${thisSql} ${newName} ${dtype}${comment}`;
  }

  renameColumnSql (_expr: Expression): string {
    this.unsupported('Cannot rename columns without data type defined in Hive');
    return '';
  }

  alterSetSql (expression: AlterSetExpr): string {
    let exprs = this.expressions(expression, { flat: true });
    exprs = exprs ? ` ${exprs}` : '';

    let location = this.sql(expression, 'location');
    location = location ? ` LOCATION ${location}` : '';

    let fileFormat = this.expressions(expression, {
      key: 'fileFormat',
      flat: true,
      sep: ' ',
    });
    fileFormat = fileFormat ? ` FILEFORMAT ${fileFormat}` : '';

    let serde = this.sql(expression, 'serde');
    serde = serde ? ` SERDE ${serde}` : '';

    let tags = this.expressions(expression, {
      key: 'tag',
      flat: true,
      sep: '',
    });
    tags = tags ? ` TAGS ${tags}` : '';

    return `SET${serde}${exprs}${location}${fileFormat}${tags}`;
  }

  serdePropertiesSql (expression: SerdePropertiesExpr): string {
    const prefix = expression.args.with ? 'WITH ' : '';
    const exprs = this.expressions(expression, { flat: true });

    return `${prefix}SERDEPROPERTIES (${exprs})`;
  }

  existsSql (expression: ExistsExpr): string {
    if (expression.args.expression) {
      return this.functionFallbackSql(expression);
    }

    return super.existsSql(expression);
  }

  timeToStrSql (expression: TimeToStrExpr): string {
    let thisNode = expression.args.this;
    if (thisNode instanceof TimeStrToTimeExpr) {
      thisNode = thisNode.args.this;
    }

    return this.func('DATE_FORMAT', [thisNode, this.formatTime(expression)]);
  }

  fileFormatPropertySql (expression: FileFormatPropertyExpr): string {
    let thisSql: string;
    if (expression.args.this instanceof InputOutputFormatExpr) {
      thisSql = this.sql(expression, 'this');
    } else {
      thisSql = expression.name.toUpperCase();
    }

    return `STORED AS ${thisSql}`;
  }
}

export class Hive extends Dialect {
  static DIALECT_NAME = Dialects.HIVE;
  static ALIAS_POST_TABLESAMPLE = true;
  static IDENTIFIERS_CAN_START_WITH_DIGIT = true;
  static SUPPORTS_USER_DEFINED_TYPES = false;

  static get EXPRESSION_METADATA () {
    return new Map(HiveTyping.EXPRESSION_METADATA);
  }

  @cache
  static get COERCES_TO (): Map<DataTypeExprKind, Set<DataTypeExprKind>> {
    const coercesTo = new Map(TypeAnnotator.COERCES_TO);
    for (const targetType of [
      ...DataTypeExpr.NUMERIC_TYPES,
      ...DataTypeExpr.TEMPORAL_TYPES,
      DataTypeExprKind.INTERVAL,
    ]) {
      const existing = coercesTo.get(targetType as DataTypeExprKind) ?? new Set<DataTypeExprKind>();
      coercesTo.set(targetType as DataTypeExprKind, new Set([...existing, ...DataTypeExpr.TEXT_TYPES]));
    }
    return coercesTo;
  }

  static SAFE_DIVISION = true;
  static ARRAY_AGG_INCLUDES_NULLS = undefined;
  static REGEXP_EXTRACT_DEFAULT_GROUP = 1;
  static ALTER_TABLE_SUPPORTS_CASCADE = true;

  @cache
  static get NORMALIZATION_STRATEGY () {
    return NormalizationStrategy.CASE_INSENSITIVE;
  }

  static INITCAP_DEFAULT_DELIMITER_CHARS = ' \t\n\r\f\u000b\u001c\u001d\u001e\u001f';

  @cache
  static get TIME_MAPPING () {
    return {
      y: '%Y',
      Y: '%Y',
      YYYY: '%Y',
      yyyy: '%Y',
      YY: '%y',
      yy: '%y',
      MMMM: '%B',
      MMM: '%b',
      MM: '%m',
      M: '%-m',
      dd: '%d',
      d: '%-d',
      HH: '%H',
      H: '%-H',
      hh: '%I',
      h: '%-I',
      mm: '%M',
      m: '%-M',
      ss: '%S',
      s: '%-S',
      SSSSSS: '%f',
      a: '%p',
      DD: '%j',
      D: '%-j',
      E: '%a',
      EE: '%a',
      EEE: '%a',
      EEEE: '%A',
      z: '%Z',
      Z: '%z',
    };
  }

  static DATE_FORMAT = '\'yyyy-MM-dd\'';
  static DATEINT_FORMAT = '\'yyyyMMdd\'';
  static TIME_FORMAT = '\'yyyy-MM-dd HH:mm:ss\'';

  static Tokenizer = HiveTokenizer;
  static Parser = HiveParser;
  static Generator = HiveGenerator;
}

Dialect.register(Dialects.HIVE, Hive);
