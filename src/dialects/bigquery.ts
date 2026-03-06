import {
  Generator,
  unsupportedArgs,
} from '../generator';
import {
  buildExtractJsonWithPath, Parser,
} from '../parser';
import type { TokenPair } from '../tokens';
import {
  Token,
  Tokenizer, TokenType,
} from '../tokens';
import type {
  ExpressionKey,
  CommandExpr,
  JsonObjectExpr,
  JsonObjectAggExpr,
  ModExpr,
  VersionExpr,
  EqExpr,
  AtTimeZoneExpr,
  TryCastExpr,
  DeclareItemExpr,
} from '../expressions';
import {
  ValuesExpr,
  CreateExpr,
  HexExpr,
  ArrayContainsExpr,
  TsOrDsAddExpr,
  TsOrDsDiffExpr,
  FuncExpr,
  NetFuncExpr,
  SafeFuncExpr,
  Expression,
  TupleExpr,
  StructExpr,
  TableAliasExpr,
  UnnestExpr,
  ArrayExpr,
  ReturnsPropertyExpr,
  SchemaExpr,
  SubqueryExpr,
  LiteralExpr,
  SelectExpr,
  DateFromPartsExpr,
  TsOrDsToTimeExpr,
  TsOrDsToDatetimeExpr,
  DateDiffExpr,
  CurrentDatetimeExpr,
  StabilityPropertyExpr,
  ApproxTopKExpr,
  ApproxDistinctExpr,
  ArgMaxExpr,
  ArgMinExpr,
  BitwiseAndAggExpr,
  BitwiseOrAggExpr,
  BitwiseXorAggExpr,
  BitwiseCountExpr,
  ByteLengthExpr,
  CastExpr,
  CountIfExpr,
  DateAddExpr,
  DateSubExpr,
  DatetimeAddExpr,
  DatetimeSubExpr,
  DateFromUnixDateExpr,
  GenerateSeriesExpr,
  GroupConcatExpr,
  IfExpr,
  IntDivExpr,
  MaxExpr,
  MinExpr,
  RegexpLikeExpr,
  TimeAddExpr,
  TimestampAddExpr,
  TimestampDiffExpr,
  TimeStrToTimeExpr,
  TsOrDsToTimestampExpr,
  UnhexExpr,
  UnixDateExpr,
  UuidExpr,
  VariancePopExpr,
  SafeDivideExpr,
  DataTypeExprKind,
  DateTruncExpr,
  BracketExpr,
  literal,
  PropertyEqExpr,
  toIdentifier,
  DateStrToDateExpr,
  ILikeExpr,
  JsonExtractExpr,
  JsonExtractArrayExpr,
  JsonExtractScalarExpr,
  ShaExpr,
  Sha2Expr,
  Sha1DigestExpr,
  Sha2DigestExpr,
  StrPositionExpr,
  Md5DigestExpr,
  JsonFormatExpr,
  JsonBoolExpr,
  TimeSubExpr,
  TimestampSubExpr,
  JsonArrayExpr,
  DateExpr,
  array,
  ArrayFilterExpr,
  ArrayRemoveExpr,
  CollatePropertyExpr,
  CommitExpr,
  RollbackExpr,
  TransactionExpr,
  FromTimeZoneExpr,
  HexStringExpr,
  Int64Expr,
  JsonKeysAtDepthExpr,
  JsonValueArrayExpr,
  LevenshteinExpr,
  LowerHexExpr,
  Md5Expr,
  NormalizeExpr,
  PartitionedByPropertyExpr,
  ParseDatetimeExpr,
  ParseTimeExpr,
  RegexpExtractExpr,
  RegexpExtractAllExpr,
  RegexpReplaceExpr,
  SessionUserExpr,
  SplitExpr,
  StrToDateExpr,
  StrToTimeExpr,
  TimestampExpr,
  TimeFromPartsExpr,
  TimestampFromPartsExpr,
  TsOrDsToDateExpr,
  UnixToTimeExpr,
  LengthExpr,
  ContainsExpr,
  JsonKeysExpr,
  JsonStripNullsExpr,
  cast,
  func,
  KwargExpr,
  var_,
  LowerExpr,
  WeekStartExpr,
  TimeExpr,
  DatetimeExpr,
  TimeToStrExpr,
  ExistsExpr,
  ColumnExpr,
  alias,
  IdentifierExpr,
  CreateExprKind,
  DotExpr,
  TableExpr,
  UserDefinedFunctionExpr,
  MakeIntervalExpr,
  MlForecastExpr,
  PredictExpr,
  VectorSearchExpr,
  FeaturesAtTimeExpr,
  GenerateEmbeddingExpr,
  MlTranslateExpr,
  TranslateExpr,
  ForInExpr,
  ExportExpr,
  PropertiesExpr,
  AliasExpr,
  CteExpr,
  QueryExpr,
  VarExpr,
  JsonKeyValueExpr,
  isType,
  JsonPathKeyExpr,
  JsonPathRootExpr,
  JsonPathSubscriptExpr,
  VolatilePropertyExpr,
  PropertiesLocation,
  StringExpr,
  ParenExpr,
  NullExpr,
  UpdateExpr,
  DataTypeExpr,
} from '../expressions';
import {
  annotateTypes, TypeAnnotator,
} from '../optimizer/annotate_types';
import {
  BIGQUERY_EXPRESSION_METADATA, type ExpressionMetadata,
} from '../typing';
import {
  seqGet,
  splitNumWords,
} from '../helper';
import {
  cache, filterInstanceOf, narrowInstanceOf,
} from '../port_internals';
import { JsonPathTokenizer } from '../jsonpath';
import {
  eliminateDistinctOn,
  eliminateSemiAndAntiJoins,
  explodeProjectionToUnnest,
  preprocess,
  unqualifyUnnest,
  removePrecisionParameterizedTypes,
} from '../transforms';
import type { JsonExtractType } from './dialect';
import {
  argMaxOrMinNoCount,
  dateAddIntervalSql,
  dateStrToDateSql,
  ifSql,
  inlineArrayUnlessQuery,
  maxOrGreatest,
  minOrLeast,
  noIlikeSql,
  buildDateDeltaWithInterval,
  sha256Sql,
  timeStrToTimeSql,
  unitToVar,
  strPositionSql,
  sha2DigestSql,
  Dialect, NormalizationStrategy, Dialects,
  groupConcatSql,
  renameFunc,
  binaryFromFunction,
  filterArrayUsingUnnest,
  tsOrDsAddCast,
  regexpReplaceSql,
  buildFormattedTime,
} from './dialect';

const DQUOTES_ESCAPING_JSON_FUNCTIONS = [
  'JSON_QUERY',
  'JSON_VALUE',
  'JSON_QUERY_ARRAY',
];

const MAKE_INTERVAL_KWARGS = [
  'year',
  'month',
  'day',
  'hour',
  'minute',
  'second',
];

function derivedTableValuesToUnnest (self: Generator, expression: ValuesExpr): string {
  if (!expression.findAncestor(SelectExpr)) {
    return self.valuesSql(expression);
  }

  const structs: StructExpr[] = [];
  const aliasExpr = expression.args.alias;
  for (const tup of expression.findAll(TupleExpr)) {
    const fieldAliases = filterInstanceOf(
      aliasExpr && aliasExpr.args.columns
        ? aliasExpr.args.columns
        : (Array.from({ length: (tup.args.expressions || []).length }, (_, i) => `_c${i}`)),
      'string',
      IdentifierExpr,
    );
    const expressions = fieldAliases?.map((name, i) =>
      new PropertyEqExpr({
        this: toIdentifier(name),
        expression: (tup.args.expressions || [])[i],
      }));
    structs.push(new StructExpr({ expressions }));
  }

  const aliasNameOnly = aliasExpr?.args.this ? new TableAliasExpr({ columns: [aliasExpr.args.this] }) : undefined;
  return self.unnestSql(
    new UnnestExpr({
      expressions: [array(...structs)],
      alias: aliasNameOnly,
    }),
  );
}

function returnsPropertySql (self: Generator, expression: ReturnsPropertyExpr): string {
  let thisExpr: string | Expression | undefined = expression.args.this;
  if (thisExpr instanceof SchemaExpr) {
    thisExpr = `${self.sql(thisExpr, 'this')} <${self.expressions(thisExpr)}>`;
  } else {
    thisExpr = self.sql(thisExpr);
  }
  return `RETURNS ${thisExpr}`;
}

function createSql (self: Generator, expression: CreateExpr): string {
  const returns = expression.find(ReturnsPropertyExpr);
  if (expression.args.kind === CreateExprKind.FUNCTION && returns && returns.args.isTable) {
    expression.setArgKey('kind', 'TABLE FUNCTION');

    if (expression.args.expression instanceof SubqueryExpr || expression.args.expression instanceof LiteralExpr) {
      expression.setArgKey('expression', expression.args.expression.args.this);
    }
  }

  return self.createSql(expression);
}

function aliasOrderedGroup (expression: Expression): Expression {
  if (expression instanceof SelectExpr) {
    const group = expression.args.group;
    const order = expression.args.order;
    if (group && order) {
      const aliases: Record<string, IdentifierExpr> = {};

      for (const select of expression.selects) {
        const selectAlias = narrowInstanceOf(select.args.alias, IdentifierExpr);
        if (select instanceof AliasExpr && selectAlias) {
          aliases[select.args.this?.toString() ?? ''] = selectAlias;
        }
      }

      for (const grouped of group.args.expressions || []) {
        if (!(grouped instanceof Expression)) {
          continue;
        }

        // Skip integer literals (ordinal references)
        if (grouped.isInteger) {
          continue;
        }

        const alias = aliases[grouped.toString()];

        if (alias) {
          grouped.replace(new ColumnExpr({ this: alias }));
        }
      }
    }
  }
  return expression;
}

/**
 * BigQuery doesn't allow column names when defining a CTE (e.g. WITH x (a, b) AS...),
 * so we try to push them down into the inner SELECT statement (e.g. WITH x AS (SELECT ... AS a, ... AS b)).
 */
export function pushdownCteColumnNames (expression: Expression): Expression {
  if (!(expression instanceof CteExpr)) {
    return expression;
  }

  const alias = expression.args.alias;
  if (!alias) {
    return expression;
  }

  const columns = alias.getArgKey('columns');
  if (
    !Array.isArray(columns)
    || !columns.length
  ) {
    return expression;
  }
  const cteQuery = expression.args.this;

  if (!(cteQuery instanceof QueryExpr)) {
    return expression;
  }

  if (cteQuery.isStar) {
    console.warn(
      'Can\'t push down CTE column names for star queries. Run the query through'
      + ' the optimizer or use \'qualify\' to expand the star projections first.',
    );
    return expression;
  }

  // Remove the columns from the CTE definition: WITH x (a, b) -> WITH x
  alias.setArgKey('columns', undefined);

  const selects = cteQuery.selects;

  // Iterate through the aliases and the inner SELECTs simultaneously
  for (let i = 0; i < columns.length; i++) {
    if (selects.length <= i) break;

    const name = columns[i];
    const toReplace = selects[i];
    let selectContent = toReplace;

    if (toReplace instanceof AliasExpr && toReplace.args.this) {
      selectContent = toReplace.args.this;
    }

    // Inner aliases are shadowed by the CTE column names, so we replace the
    // entire projection with a new AliasExpr using the CTE's column name.
    toReplace.replace(
      new AliasExpr({
        this: selectContent,
        alias: narrowInstanceOf(name, 'string', Expression),
      }),
    );
  }

  return expression;
}

/**
 * Builds a StrToTime expression tailored for BigQuery's parsing format.
 */
export function buildParseTimestamp (args: Expression[]): StrToTimeExpr {
  const thisExpr = buildFormattedTime(StrToTimeExpr, { dialect: Dialects.BIGQUERY })([seqGet(args, 1), seqGet(args, 0)]);
  thisExpr.setArgKey('zone', seqGet(args, 2)!);
  return thisExpr;
}

/**
 * Builds a Timestamp expression ensuring with_tz is set to true.
 */
export function buildTimestamp (args: Expression[]): TimestampExpr {
  const timestamp = TimestampExpr.fromArgList(args);
  timestamp.setArgKey('withTz', true);
  return timestamp;
}

/**
 * Chooses between Date and DateFromParts based on argument count.
 */
export function buildDate (args: Expression[]): DateExpr | DateFromPartsExpr {
  const exprType = args.length === 3 ? DateFromPartsExpr : DateExpr;
  return exprType.fromArgList(args) as DateExpr | DateFromPartsExpr;
}

/**
 * Simplifies TO_HEX(MD5(..)) structures commonly found in BigQuery
 * into a single MD5Expr for easier transpilation.
 */
export function buildToHex (args: Expression[]): HexExpr | Md5Expr | LowerHexExpr {
  const arg = seqGet(args, 0);

  if (arg instanceof Md5DigestExpr) {
    return new Md5Expr({ this: arg.args.this });
  }

  return new LowerHexExpr({ this: arg });
}

/**
 * Builds JSONStripNulls and safely maps kwargs to their specific properties.
 */
export function buildJsonStripNulls (args: Expression[]): JsonStripNullsExpr {
  const expression = new JsonStripNullsExpr({ this: seqGet(args, 0)! });

  for (const arg of args.slice(1)) {
    if (arg instanceof KwargExpr) {
      const kwargName = arg.args.this?.name.toLowerCase();
      if (kwargName !== undefined) {
        expression.setArgKey(kwargName, arg);
      }
    } else {
      expression.setArgKey('expression', arg);
    }
  }

  return expression;
}

/**
 * Converts BigQuery's ARRAY_CONTAINS logic into a universally compatible
 * EXISTS(SELECT 1 FROM UNNEST(...) ...) subquery structure.
 */
export function arrayContainsSql (self: BigQueryGenerator, expression: ArrayContainsExpr): string {
  const select = new SelectExpr({ expressions: [new IdentifierExpr({ this: '1' })] })
    .from(
      new UnnestExpr({ expressions: filterInstanceOf([expression.left], Expression) })
        .as('_unnest', { table: ['_col'] }),
    )
    .where(
      new ColumnExpr({ this: '_col' }).eq(expression.right),
    );

  const exists = new ExistsExpr({ this: select });

  return self.sql(exists);
}

export function buildTime (args: Expression[]): Expression {
  if (args.length === 1) {
    return new TsOrDsToTimeExpr({ this: args[0] });
  }
  if (args.length === 2) {
    return TimeExpr.fromArgList(args);
  }
  return TimeFromPartsExpr.fromArgList(args);
}

export function buildDatetime (args: Expression[]): Expression {
  if (args.length === 1) {
    return TsOrDsToDatetimeExpr.fromArgList(args);
  }
  if (args.length === 2) {
    return DatetimeExpr.fromArgList(args);
  }
  return TimestampFromPartsExpr.fromArgList(args);
}

export function buildDateDiff (args: Expression[]): DateDiffExpr {
  const expr = new DateDiffExpr({
    this: seqGet(args, 0)!,
    expression: seqGet(args, 1)!,
    unit: seqGet(args, 2),
    datePartBoundary: true,
  });

  // Normalize plain WEEK to WEEK(SUNDAY) to preserve the semantic in the AST to facilitate transpilation
  const unit = expr.args.unit;

  if (unit instanceof VarExpr && unit.name.toUpperCase() === 'WEEK') {
    expr.setArgKey('unit', new WeekStartExpr({ this: new VarExpr({ this: 'SUNDAY' }) }));
  }

  return expr;
}

export function buildRegexpExtract<T extends Expression> (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  exprType: new (args: any) => T,
  defaultGroup?: Expression,
): (args: Expression[], options: { dialect: BigQuery }) => T {
  return (args: Expression[], { dialect }: { dialect: BigQuery }): T => {
    let group = false;
    try {
      const pattern = args[1]?.name;
      const matches = pattern.match(/(^|[^\\])(\\\\)*\((?!\?)/g)?.length ?? 0;
      group = matches === 1;
    } catch {
      group = false;
    }

    // Default group is used for the transpilation of REGEXP_EXTRACT_ALL
    const kwargs: Record<string, unknown> = {
      this: seqGet(args, 0),
      expression: seqGet(args, 1),
      position: seqGet(args, 2),
      occurrence: seqGet(args, 3),
      group: group ? LiteralExpr.number(1) : defaultGroup,
    };

    if (exprType === RegexpExtractExpr as unknown) {
      kwargs.nullIfPosOverflow = dialect._constructor.REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL;
    }

    return new exprType(kwargs);
  };
}

export function buildExtractJsonWithDefaultPath<T extends typeof Expression> (
  exprType: T,
): (args: Expression[], options: { dialect: Dialect }) => InstanceType<T> {
  return (args: Expression[], options: { dialect: Dialect }): InstanceType<T> => {
    if (args.length === 1) {
      // The default value for the JSONPath is '$' i.e all of the data
      args.push(LiteralExpr.string('$'));
    }
    return buildExtractJsonWithPath(exprType)(args, options) as InstanceType<T>;
  };
}

/**
 * Maps Levenshtein distance to BigQuery's EDIT_DISTANCE function,
 * wrapping the 'max_dist' argument in a named Kwarg if present.
 */
export const levenshteinSql = (self: Generator, expression: LevenshteinExpr) => (unsupportedArgs<LevenshteinExpr>('insCost', 'delCost', 'subCost')(
  (expression: LevenshteinExpr): string => {
    let maxDist = expression.args.maxDist;

    if (maxDist) {
      maxDist = new KwargExpr({
        this: new VarExpr({ this: 'max_distance' }),
        expression: maxDist,
      });
    }

    return self.func(
      'EDIT_DISTANCE',
      [
        expression.args.this,
        expression.args.expression,
        maxDist,
      ],
    );
  },
))(expression);

/**
 * Builder for Levenshtein expressions, extracting the max distance value
 * from the parsed argument list.
 */
export function buildLevenshtein (args: Expression[]): LevenshteinExpr {
  const maxDist = seqGet(args, 2);

  return new LevenshteinExpr({
    this: seqGet(args, 0),
    expression: seqGet(args, 1),
    maxDist: narrowInstanceOf(maxDist?.args.expression, Expression),
  });
}

function jsonExtractSql (self: BigQueryGenerator, expression: JsonExtractType): string {
  const name = expression.meta?.name || (expression._constructor as typeof FuncExpr).sqlName();
  const upper = typeof name === 'string' ? name.toUpperCase() : '';

  const dquoteEscaping = DQUOTES_ESCAPING_JSON_FUNCTIONS.includes(upper);

  if (dquoteEscaping) {
    self.quoteJsonPathKeyUsingBrackets = false;
  }

  const sql = renameFunc(upper)(self, expression);

  if (dquoteEscaping) {
    self.quoteJsonPathKeyUsingBrackets = true;
  }

  return sql;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function buildFormatTime (exprType: new (args: any) => Expression): (args: Expression[]) => Expression {
  return (args: Expression[]) => {
    const formatted = buildFormattedTime(TimeToStrExpr, { dialect: Dialects.BIGQUERY })(
      [new exprType({ this: args[1] }), args[0]],
    );
    formatted.setArgKey('zone', args[2]);
    return formatted;
  };
}

/**
 * Builds a Contains expression (typically for BigQuery's CONTAINS_SUBSTR).
 * Wraps operands in LOWER() to ensure case-insensitive transpilation to other dialects.
 */
export function buildContainsSubstring (args: Expression[]): ContainsExpr {
  // Lowercase the operands in case of transpilation, as ContainsExpr
  // is case-sensitive on other dialects
  const thisExpr = new LowerExpr({ this: seqGet(args, 0) });
  const expr = new LowerExpr({ this: seqGet(args, 1) });

  return new ContainsExpr({
    this: thisExpr,
    expression: expr,
    jsonScope: seqGet(args, 2),
  });
}

function strToDatetimeSql (self: Generator, expression: StrToDateExpr | StrToTimeExpr): string {
  const thisStr = self.sql(expression, 'this');
  const dtype = expression instanceof StrToDateExpr ? 'DATE' : 'TIMESTAMP';

  if (expression.args.safe) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const dialectCls = (self as any).dialect._constructor;
    const fmt = self.formatTime(
      expression,
      dialectCls.INVERSE_FORMAT_MAPPING,
      dialectCls.INVERSE_FORMAT_TRIE,
    );
    return `SAFE_CAST(${thisStr} AS ${dtype} FORMAT ${fmt})`;
  }

  const fmt = self.formatTime(expression);
  return self.func(`PARSE_${dtype}`, [
    fmt,
    thisStr,
    expression.getArgKey('zone') as string | Expression | undefined,
  ]);
}

function tsOrDsAddSql (self: Generator, expression: TsOrDsAddExpr): string {
  return dateAddIntervalSql('DATE', 'ADD')(self, tsOrDsAddCast(expression));
}

function tsOrDsDiffSql (self: Generator, expression: TsOrDsDiffExpr): string {
  expression.args.this?.replace(cast(expression.args.this, DataTypeExprKind.TIMESTAMP));
  expression.args.expression?.replace(cast(expression.args.expression, DataTypeExprKind.TIMESTAMP));
  const unit = unitToVar(expression);
  return self.func('DATE_DIFF', [
    expression.args.this,
    expression.args.expression,
    unit,
  ]);
}

function unixToTimeSql (self: Generator, expression: UnixToTimeExpr): string {
  const scale = expression.args.scale;
  const timestamp = expression.args.this;

  if (!scale || scale.name === UnixToTimeExpr.SECONDS.name) {
    return self.func('TIMESTAMP_SECONDS', [timestamp]);
  }
  if (scale.name === UnixToTimeExpr.MILLIS.name) {
    return self.func('TIMESTAMP_MILLIS', [timestamp]);
  }
  if (scale.name === UnixToTimeExpr.MICROS.name) {
    return self.func('TIMESTAMP_MICROS', [timestamp]);
  }

  const powExpr = func('POW', literal('10'), scale);
  const unixSeconds = cast(
    new IntDivExpr({
      this: timestamp,
      expression: powExpr,
    }),
    DataTypeExprKind.BIGINT,
  );
  return self.func('TIMESTAMP_SECONDS', [unixSeconds]);
}

export class BigQueryTokenizer extends Tokenizer {
  public static QUOTES: TokenPair[] = [
    '\'',
    '"',
    '"""',
    '\'\'\'',
  ];

  public static COMMENTS: TokenPair[] = [
    '--',
    '#',
    ['/*', '*/'],
  ];

  public static IDENTIFIERS = ['`'];
  public static STRING_ESCAPES = ['\\'];
  public static HEX_STRINGS: TokenPair[] = [['0x', ''], ['0X', '']];

  public static BYTE_STRINGS: TokenPair[] = (['b', 'B'] as const).flatMap((prefix) =>
    (BigQueryTokenizer.QUOTES as string[]).map((q): TokenPair => [prefix + q, q as string]));

  public static RAW_STRINGS: TokenPair[] = (['r', 'R'] as const).flatMap((prefix) =>
    (BigQueryTokenizer.QUOTES as string[]).map((q): TokenPair => [prefix + q, q as string]));

  public static NESTED_COMMENTS = false;

  public static ORIGINAL_KEYWORDS: Record<string, TokenType> = (() => {
    const keywords: Record<string, TokenType> = {
      ...Tokenizer.KEYWORDS,
      'ANY TYPE': TokenType.VARIANT,
      'BEGIN': TokenType.COMMAND,
      'BEGIN TRANSACTION': TokenType.BEGIN,
      'BYTEINT': TokenType.INT,
      'BYTES': TokenType.BINARY,
      'CURRENT_DATETIME': TokenType.CURRENT_DATETIME,
      'DATETIME': TokenType.TIMESTAMP,
      'DECLARE': TokenType.DECLARE,
      'ELSEIF': TokenType.COMMAND,
      'EXCEPTION': TokenType.COMMAND,
      'EXPORT': TokenType.EXPORT,
      'FLOAT64': TokenType.DOUBLE,
      'FOR SYSTEM_TIME': TokenType.TIMESTAMP_SNAPSHOT,
      'LOOP': TokenType.COMMAND,
      'MODEL': TokenType.MODEL,
      'NOT DETERMINISTIC': TokenType.VOLATILE,
      'RECORD': TokenType.STRUCT,
      'REPEAT': TokenType.COMMAND,
      'TIMESTAMP': TokenType.TIMESTAMPTZ,
      'WHILE': TokenType.COMMAND,
    };
    delete keywords['DIV'];
    delete keywords['VALUES'];
    delete keywords['/*+'];
    return keywords;
  })();
}

export class BigQueryParser extends Parser {
  public static PREFIXED_PIVOT_COLUMNS = true;
  public static LOG_DEFAULTS_TO_LN = true;
  public static SUPPORTS_IMPLICIT_UNNEST = true;
  public static JOINS_HAVE_EQUAL_PRECEDENCE = true;

  // BigQuery does not allow ASC/DESC to be used as an identifier, allows GRANT as an identifier
  @cache
  static get ID_VAR_TOKENS (): Set<TokenType> {
    return (() => {
      const s = new Set([...Parser.ID_VAR_TOKENS, TokenType.GRANT]);
      s.delete(TokenType.ASC);
      s.delete(TokenType.DESC);
      return s;
    })();
  }

  @cache
  static get ALIAS_TOKENS (): Set<TokenType> {
    return (() => {
      const s = new Set([...Parser.ALIAS_TOKENS, TokenType.GRANT]);
      s.delete(TokenType.ASC);
      s.delete(TokenType.DESC);
      return s;
    })();
  }

  @cache
  static get TABLE_ALIAS_TOKENS (): Set<TokenType> {
    return (() => {
      const s = new Set([...Parser.TABLE_ALIAS_TOKENS, TokenType.GRANT]);
      s.delete(TokenType.ASC);
      s.delete(TokenType.DESC);
      return s;
    })();
  }

  @cache
  static get COMMENT_TABLE_ALIAS_TOKENS (): Set<TokenType> {
    return (() => {
      const s = new Set([...Parser.COMMENT_TABLE_ALIAS_TOKENS, TokenType.GRANT]);
      s.delete(TokenType.ASC);
      s.delete(TokenType.DESC);
      return s;
    })();
  }

  @cache
  static get UPDATE_ALIAS_TOKENS (): Set<TokenType> {
    return (() => {
      const s = new Set([...Parser.UPDATE_ALIAS_TOKENS, TokenType.GRANT]);
      s.delete(TokenType.ASC);
      s.delete(TokenType.DESC);
      return s;
    })();
  }

  @cache
  static get NESTED_TYPE_TOKENS (): Set<TokenType> {
    return new Set([...Parser.NESTED_TYPE_TOKENS, TokenType.TABLE]);
  }

  @cache
  static get PROPERTY_PARSERS () {
    return {
      ...Parser.PROPERTY_PARSERS,
      'NOT DETERMINISTIC': (self: Parser) => self.expression(StabilityPropertyExpr, { this: LiteralExpr.string('VOLATILE') }),
      'OPTIONS': (self: Parser) => self.parseWithProperty(),
    };
  }

  @cache
  static get CONSTRAINT_PARSERS () {
    return {
      ...Parser.CONSTRAINT_PARSERS,
      OPTIONS: (self: Parser) => self.expression(PropertiesExpr, { expressions: self.parseWithProperty() }),
    };
  }

  @cache
  static get RANGE_PARSERS (): Partial<Record<TokenType, (self: Parser, this_: Expression) => Expression | undefined>> {
    return (() => {
      const m = { ...Parser.RANGE_PARSERS };
      delete m[TokenType.OVERLAPS];
      return m;
    })();
  }

  public static DASHED_TABLE_PART_FOLLOW_TOKENS: Set<TokenType> = new Set([
    TokenType.DOT,
    TokenType.L_PAREN,
    TokenType.R_PAREN,
  ]);

  @cache
  static get STATEMENT_PARSERS () {
    return {
      ...Parser.STATEMENT_PARSERS,
      [TokenType.ELSE]: (self: Parser) => self.parseAsCommand((self as BigQueryParser).prev),
      [TokenType.END]: (self: Parser) => self.parseAsCommand((self as BigQueryParser).prev),
      [TokenType.FOR]: (self: Parser) => (self as BigQueryParser).parseForIn(),
      [TokenType.EXPORT]: (self: Parser) => (self as BigQueryParser).parseExportData(),
      [TokenType.DECLARE]: (self: Parser) => self.parseDeclare(),
    };
  }

  public static BRACKET_OFFSETS: Record<string, [number, boolean]> = {
    OFFSET: [0, false],
    ORDINAL: [1, false],
    SAFE_OFFSET: [0, true],
    SAFE_ORDINAL: [1, true],
  };

  @cache
  static get NO_PAREN_FUNCTIONS () {
    return {
      ...Parser.NO_PAREN_FUNCTIONS,
      [TokenType.CURRENT_DATETIME]: CurrentDatetimeExpr,
    };
  }

  @cache
  static get FUNCTIONS () {
    return (() => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const fns: Record<string, (args: Expression[], dialect: any) => Expression> = {
        ...Parser.FUNCTIONS,
        APPROX_TOP_COUNT: ApproxTopKExpr.fromArgList,
        BIT_AND: BitwiseAndAggExpr.fromArgList,
        BIT_OR: BitwiseOrAggExpr.fromArgList,
        BIT_XOR: BitwiseXorAggExpr.fromArgList,
        BIT_COUNT: BitwiseCountExpr.fromArgList,
        BOOL: JsonBoolExpr.fromArgList,
        CONTAINS_SUBSTR: buildContainsSubstring,
        DATE: buildDate,
        DATE_ADD: (args) => buildDateDeltaWithInterval(DateAddExpr)(args)!,
        DATE_DIFF: buildDateDiff,
        DATE_SUB: (args) => buildDateDeltaWithInterval(DateSubExpr)(args)!,
        DATE_TRUNC: (args) => new DateTruncExpr({
          unit: seqGet(args, 1)!,
          this: seqGet(args, 0)!,
          zone: seqGet(args, 2),
        }),
        DATETIME: buildDatetime,
        DATETIME_ADD: (args) => buildDateDeltaWithInterval(DatetimeAddExpr)(args)!,
        DATETIME_SUB: (args) => buildDateDeltaWithInterval(DatetimeSubExpr)(args)!,
        DIV: binaryFromFunction(IntDivExpr),
        EDIT_DISTANCE: buildLevenshtein,
        FORMAT_DATE: buildFormatTime(TsOrDsToDateExpr),
        GENERATE_ARRAY: GenerateSeriesExpr.fromArgList,
        JSON_EXTRACT_SCALAR: buildExtractJsonWithDefaultPath(JsonExtractScalarExpr),
        JSON_EXTRACT_ARRAY: buildExtractJsonWithDefaultPath(JsonExtractArrayExpr),
        JSON_EXTRACT_STRING_ARRAY: buildExtractJsonWithDefaultPath(JsonValueArrayExpr),
        JSON_KEYS: JsonKeysExpr.fromArgList,
        JSON_QUERY: buildExtractJsonWithPath(JsonExtractExpr),
        JSON_QUERY_ARRAY: buildExtractJsonWithDefaultPath(JsonExtractArrayExpr),
        JSON_STRIP_NULLS: buildJsonStripNulls,
        JSON_VALUE: buildExtractJsonWithDefaultPath(JsonExtractScalarExpr),
        JSON_VALUE_ARRAY: buildExtractJsonWithDefaultPath(JsonValueArrayExpr),
        LENGTH: (args) => new LengthExpr({
          this: seqGet(args, 0)!,
          binary: true,
        }),
        MD5: Md5DigestExpr.fromArgList,
        SHA1: Sha1DigestExpr.fromArgList,
        NORMALIZE_AND_CASEFOLD: (args) => new NormalizeExpr({
          this: seqGet(args, 0)!,
          form: seqGet(args, 1),
          isCasefold: true,
        }),
        OCTET_LENGTH: ByteLengthExpr.fromArgList,
        TO_HEX: buildToHex,
        PARSE_DATE: (args) => buildFormattedTime(StrToDateExpr, { dialect: Dialects.BIGQUERY })(
          [seqGet(args, 1), seqGet(args, 0)],
        ),
        PARSE_TIME: (args) => buildFormattedTime(ParseTimeExpr, { dialect: Dialects.BIGQUERY })(
          [seqGet(args, 1), seqGet(args, 0)],
        ),
        PARSE_TIMESTAMP: buildParseTimestamp,
        PARSE_DATETIME: (args) => buildFormattedTime(ParseDatetimeExpr, { dialect: Dialects.BIGQUERY })(
          [seqGet(args, 1), seqGet(args, 0)],
        ),
        REGEXP_CONTAINS: RegexpLikeExpr.fromArgList,
        REGEXP_EXTRACT: buildRegexpExtract(RegexpExtractExpr),
        REGEXP_SUBSTR: buildRegexpExtract(RegexpExtractExpr),
        REGEXP_EXTRACT_ALL: buildRegexpExtract(RegexpExtractAllExpr, literal(0)),
        SHA256: (args) => new Sha2DigestExpr({
          this: seqGet(args, 0)!,
          length: literal(256),
        }),
        SHA512: (args) => new Sha2Expr({
          this: seqGet(args, 0)!,
          length: literal(512),
        }),
        SPLIT: (args) => new SplitExpr({
          // https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#split
          this: seqGet(args, 0)!,
          expression: seqGet(args, 1) || literal(','),
        }),
        STRPOS: StrPositionExpr.fromArgList,
        TIME: buildTime,
        TIME_ADD: (args) => buildDateDeltaWithInterval(TimeAddExpr)(args)!,
        TIME_SUB: (args) => buildDateDeltaWithInterval(TimeSubExpr)(args)!,
        TIMESTAMP: buildTimestamp,
        TIMESTAMP_ADD: (args) => buildDateDeltaWithInterval(TimestampAddExpr)(args)!,
        TIMESTAMP_SUB: (args) => buildDateDeltaWithInterval(TimestampSubExpr)(args)!,
        TIMESTAMP_MICROS: (args) => new UnixToTimeExpr({
          this: seqGet(args, 0)!,
          scale: UnixToTimeExpr.MICROS,
        }),
        TIMESTAMP_MILLIS: (args) => new UnixToTimeExpr({
          this: seqGet(args, 0)!,
          scale: UnixToTimeExpr.MILLIS,
        }),
        TIMESTAMP_SECONDS: (args) => new UnixToTimeExpr({ this: seqGet(args, 0)! }),
        TO_JSON: (args) => new JsonFormatExpr({
          this: seqGet(args, 0)!,
          options: seqGet(args, 1),
          toJson: true,
        }),
        TO_JSON_STRING: JsonFormatExpr.fromArgList,
        FORMAT_DATETIME: buildFormatTime(TsOrDsToDatetimeExpr),
        FORMAT_TIMESTAMP: buildFormatTime(TsOrDsToTimestampExpr),
        FORMAT_TIME: buildFormatTime(TsOrDsToTimeExpr),
        FROM_HEX: UnhexExpr.fromArgList,
        WEEK: (args) => new WeekStartExpr({ this: var_(seqGet(args, 0)) }),
      };
        // Remove SEARCH to avoid parameter routing issues - let it fall back to Anonymous function
      delete fns['SEARCH'];
      return fns;
    })();
  }

  @cache
  static get FUNCTION_PARSERS () {
    return (() => {
      const fps = {
        ...Parser.FUNCTION_PARSERS,
        ARRAY: (self: Parser) => self.expression(ArrayExpr, {
          expressions: [self.parseStatement()],
          structNameInheritance: true,
        }),
        JSON_ARRAY: (self: Parser) => self.expression(JsonArrayExpr, {
          expressions: self.parseCsv(() => self.parseBitwise()),
        }),
        MAKE_INTERVAL: (self: Parser) => (self as BigQueryParser).parseMakeInterval(),
        PREDICT: (self: Parser) => (self as BigQueryParser).parseMl(PredictExpr),
        TRANSLATE: (self: Parser) => (self as BigQueryParser).parseTranslate(),
        FEATURES_AT_TIME: (self: Parser) => (self as BigQueryParser).parseFeaturesAtTime(),
        GENERATE_EMBEDDING: (self: Parser) => (self as BigQueryParser).parseMl(GenerateEmbeddingExpr),
        GENERATE_TEXT_EMBEDDING: (self: Parser) => (self as BigQueryParser).parseMl(GenerateEmbeddingExpr, { isText: true }),
        VECTOR_SEARCH: (self: Parser) => (self as BigQueryParser).parseVectorSearch(),
        FORECAST: (self: Parser) => (self as BigQueryParser).parseMl(MlForecastExpr),
      };
      delete (fps as Record<string, unknown>)['TRIM'];
      return fps;
    })();
  }

  parseForIn (): ForInExpr | CommandExpr {
    const index = this.index;
    const thisExpr = this.parseRange();
    this.matchTextSeq('DO');
    if (this.match(TokenType.COMMAND)) {
      this.retreat(index);
      return this.parseAsCommand(this.prev)!;
    }
    return this.expression(ForInExpr, {
      this: thisExpr,
      expression: this.parseStatement(),
    });
  }

  parseExportData (): ExportExpr {
    this.matchTextSeq('DATA');
    return this.expression(ExportExpr, {
      connection: this.matchTextSeq('WITH') && this.matchTextSeq('CONNECTION') && this.parseTableParts(),
      options: this.parseProperties(),
      this: this.matchTextSeq('AS') && this.parseSelect(),
    });
  }

  parseMakeInterval (): MakeIntervalExpr {
    const expr = new MakeIntervalExpr({});
    for (const argKey of MAKE_INTERVAL_KWARGS) {
      const value = this.parseLambda();
      if (!value) break;

      // Non-named arguments are filled sequentially, (optionally) followed by named arguments
      // that can appear in any order e.g MAKE_INTERVAL(1, minute => 5, day => 2)
      const key = value instanceof KwargExpr ? value.args.this?.name : argKey;
      if (key !== undefined) {
        expr.setArgKey(key, value);
      }
      this.match(TokenType.COMMA);
    }
    return expr;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  parseMl<T extends Expression> (exprType: new (args: any) => T, kwargs: Record<string, unknown> = {}): T {
    this.matchTextSeq('MODEL');
    const thisExpr = this.parseTable();
    this.match(TokenType.COMMA);
    this.matchTextSeq('TABLE');

    // Certain functions like ML.FORECAST require a STRUCT argument but not a TABLE/SELECT one
    const expression = !this.match(TokenType.STRUCT, { advance: false })
      ? this.parseTable()
      : undefined;
    this.match(TokenType.COMMA);

    return this.expression(exprType, {
      this: thisExpr,
      expression,
      paramsStruct: this.parseBitwise(),
      ...kwargs,
    });
  }

  parseTranslate (): TranslateExpr | MlTranslateExpr {
    // Check if this is ML.TRANSLATE by looking at the preceding token
    const token = this.tokens[this.index - 4];
    if (token && token.text.toUpperCase() === 'ML') {
      return this.parseMl(MlTranslateExpr);
    }
    return TranslateExpr.fromArgList(this.parseFunctionArgs());
  }

  parseFeaturesAtTime (): FeaturesAtTimeExpr {
    this.match(TokenType.TABLE);
    const thisExpr = this.parseTable();
    const expr = this.expression(FeaturesAtTimeExpr, { this: thisExpr });

    while (this.match(TokenType.COMMA)) {
      const arg = this.parseLambda();
      // Get the LHS of the Kwarg and set the arg to that value, e.g
      // "num_rows => 1" sets the expr's `num_rows` arg
      if (arg) {
        const kwargThis = (arg as KwargExpr).args.this;
        if (kwargThis) {
          expr.setArgKey(kwargThis.name, arg);
        }
      }
    }
    return expr;
  }

  parseVectorSearch (): VectorSearchExpr {
    this.match(TokenType.TABLE);
    const baseTable = this.parseTable();
    this.match(TokenType.COMMA);

    const columnToSearch = this.parseBitwise();
    this.match(TokenType.COMMA);

    this.match(TokenType.TABLE);
    const queryTable = this.parseTable();

    const expr = this.expression(VectorSearchExpr, {
      this: baseTable,
      columnToSearch,
      queryTable,
    });

    while (this.match(TokenType.COMMA)) {
      // query_column_to_search can be named argument or positional
      if (this.match(TokenType.STRING, { advance: false })) {
        const queryColumn = this.parseString();
        expr.setArgKey('queryColumnToSearch', queryColumn);
      } else {
        const arg = this.parseLambda();
        if (arg instanceof KwargExpr && arg.args.this) {
          expr.setArgKey(arg.args.this.name, arg);
        }
      }
    }
    return expr;
  }

  parseTablePart (options: { schema?: boolean } = {}): Expression | undefined {
    const { schema = false } = options;
    let thisNode = super.parseTablePart({ schema }) || this.parseNumber();

    if (!thisNode) return undefined;

    // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#table_names
    if (thisNode instanceof IdentifierExpr) {
      let tableName = thisNode.name;

      while (this.match(TokenType.DASH, { advance: false }) && this.next) {
        const start = this.curr;

        while (
          this.isConnected()
          && !this.matchSet((this._constructor as typeof BigQueryParser).DASHED_TABLE_PART_FOLLOW_TOKENS, { advance: false })
        ) {
          this.advance();
        }

        if (start === this.curr) {
          break;
        }

        tableName += this.findSql(start, this.prev);
      }

      thisNode = new IdentifierExpr({
        this: tableName,
        quoted: thisNode.args.quoted,
      }).updatePositions(thisNode);
    } else if (thisNode instanceof LiteralExpr) {
      let tableName = thisNode.name;

      if (this.isConnected() && this.parseVar({ anyToken: true })) {
        tableName += this.prev?.text;
      }

      thisNode = new IdentifierExpr({
        this: tableName,
        quoted: true,
      }).updatePositions(thisNode);
    }

    return thisNode;
  }

  parseTableParts (
    options: {
      schema?: boolean;
      isDbReference?: boolean;
      wildcard?: boolean;
    } = {},
  ): TableExpr {
    const {
      schema = false, isDbReference = false, wildcard: _wildcard = false,
    } = options;

    let table = super.parseTableParts({
      schema,
      isDbReference,
      wildcard: true,
    });

    // proj-1.db.tbl -- `1.` is tokenized as a float so we need to unravel it here
    if (!table.catalog) {
      if (table.db) {
        const previousDb = table.args.db;
        const parts = table.db.split('.');
        if (parts.length === 2 && !narrowInstanceOf(table.args.db, IdentifierExpr)?.quoted) {
          table.setArgKey(
            'catalog',
            new IdentifierExpr({ this: parts[0] }).updatePositions(previousDb),
          );
          table.setArgKey(
            'db',
            new IdentifierExpr({ this: parts[1] }).updatePositions(previousDb),
          );
        }
      } else {
        const previousThis = table.args.this;
        const parts = table.name.split('.');
        if (parts.length === 2 && !narrowInstanceOf(table.args.this, IdentifierExpr)?.quoted) {
          table.setArgKey(
            'db',
            new IdentifierExpr({ this: parts[0] }).updatePositions(narrowInstanceOf(previousThis, Token, Expression)),
          );
          table.setArgKey(
            'this',
            new IdentifierExpr({ this: parts[1] }).updatePositions(narrowInstanceOf(previousThis, Token, Expression)),
          );
        }
      }
    }

    let aliasExpr: IdentifierExpr | undefined = undefined;

    if (
      table.args.this instanceof IdentifierExpr
      && table.parts.some((p) => p.name.includes('.'))
    ) {
      aliasExpr = table.args.this;

      const joinedParts = table.parts.map((p) => p.name).join('.');
      const splitParts = splitNumWords(joinedParts, '.', 3);

      const [
        catalog,
        db,
        thisNode,
        ...rest
      ] = splitParts.map((p) =>
        toIdentifier(p, { quoted: true }));

      [
        catalog,
        db,
        thisNode,
      ].forEach((part) => {
        if (part) part.updatePositions(narrowInstanceOf(table.args.this, Token, Expression));
      });

      let finalThis: Expression | undefined = thisNode;
      if (0 < rest.length && finalThis) {
        finalThis = DotExpr.build([finalThis, ...rest.map((e) => e ?? toIdentifier('', { quoted: true }))]);
      }

      table = new TableExpr({
        this: finalThis,
        db: db,
        catalog: catalog,
        pivots: table.args.pivots,
      });
      table.meta.quotedTable = true;
    }

    const tableParts = table.parts;
    const len = tableParts.length;
    if (1 < len && tableParts[len - 2].name.toUpperCase() === 'INFORMATION_SCHEMA') {
      alias(
        table,
        (aliasExpr || tableParts[len - 1]) as IdentifierExpr,
        {
          table: true,
          copy: false,
        },
      );

      const infoSchemaView = `${tableParts[len - 2].name}.${tableParts[len - 1].name}`;

      const newThis = new IdentifierExpr({
        this: infoSchemaView,
        quoted: true,
      }).updatePositions(undefined, {
        line: tableParts[len - 2].meta?.line as number,
        col: tableParts[len - 1].meta?.col as number,
        start: tableParts[len - 2].meta?.start as number,
        end: tableParts[len - 1].meta?.end as number,
      });

      table.setArgKey('this', newThis);
      table.setArgKey('db', seqGet(tableParts, -3));
      table.setArgKey('catalog', seqGet(tableParts, -4));
    }

    return table;
  }

  parseColumn (): Expression | undefined {
    let column = super.parseColumn();

    if (column instanceof ColumnExpr) {
      const parts = column.parts;

      if (parts.some((p) => p.name.includes('.'))) {
        const joinedParts = parts.map((p) => p.name).join('.');
        const splitParts = splitNumWords(joinedParts, '.', 4);

        const [
          catalog,
          db,
          table,
          thisNode,
          ...rest
        ] = splitParts.map((p) =>
          toIdentifier(p, { quoted: true }));

        let finalThis: Expression = thisNode || toIdentifier('', { quoted: true });
        if (0 < rest.length && finalThis) {
          finalThis = DotExpr.build([finalThis, ...rest.map((p) => p ?? toIdentifier('', { quoted: true }))]);
        }

        column = new ColumnExpr({
          this: finalThis,
          table: table,
          db: db,
          catalog: catalog,
        });
        column.meta.quotedColumn = true;
      }
    }

    return column;
  }

  parseJsonObject (options: { agg?: boolean } = {}): JsonObjectExpr | JsonObjectAggExpr {
    const { agg: _agg = false } = options;
    const jsonObject = super.parseJsonObject();
    const arrayKvPair = seqGet(jsonObject.args.expressions || [], 0);

    if (
      arrayKvPair instanceof Expression
      && arrayKvPair.args.this instanceof ArrayExpr
      && arrayKvPair.args.expression instanceof ArrayExpr
    ) {
      const keys = arrayKvPair.args.this.args.expressions;
      const values = arrayKvPair.args.expression.args.expressions;

      jsonObject.setArgKey(
        'expressions',
        keys?.map((k, i) => new JsonKeyValueExpr({
          this: k,
          expression: values?.[i] || '',
        })),
      );
    }

    return jsonObject;
  }

  parseBracket (thisNode?: Expression): Expression | undefined {
    const bracket = super.parseBracket(thisNode);

    if (bracket instanceof ArrayExpr) {
      bracket.setArgKey('structNameInheritance', true);
    }

    if (thisNode === bracket) {
      return bracket;
    }

    if (bracket instanceof BracketExpr) {
      for (const expression of bracket.args.expressions ?? []) {
        const name = expression.name.toUpperCase();

        if (!(name in (this._constructor as typeof BigQueryParser).BRACKET_OFFSETS)) {
          break;
        }

        const [offset, safe] = (this._constructor as typeof BigQueryParser).BRACKET_OFFSETS[name];
        bracket.setArgKey('offset', offset);
        bracket.setArgKey('safe', safe);
        expression.replace(narrowInstanceOf(expression.args.expressions?.[0], Expression));
      }
    }

    return bracket;
  }

  parseUnnest (options: { withAlias?: boolean } = {}): UnnestExpr | undefined {
    const { withAlias = true } = options;
    const unnest = super.parseUnnest({ withAlias }) as UnnestExpr | undefined;

    if (!unnest) {
      return undefined;
    }

    let unnestExpr = seqGet(unnest.args.expressions ?? [], 0);
    if (unnestExpr instanceof Expression) {
      unnestExpr = annotateTypes(unnestExpr, { dialect: this.dialect });

      // Unnesting a nested array (i.e array of structs) explodes the top-level struct fields
      if (
        unnestExpr.isType(DataTypeExprKind.ARRAY)
        && unnestExpr.type instanceof Expression
        && unnestExpr.type.args.expressions?.some((arrayElem) =>
          isType(arrayElem, DataTypeExprKind.STRUCT))
      ) {
        unnest.setArgKey('explodeArray', true);
      }
    }

    return unnest;
  }

  parseColumnOps (thisNode?: Expression): Expression | undefined {
    const funcIndex = this.index + 1;
    let result = super.parseColumnOps(thisNode);

    if (result instanceof DotExpr && result.args.expression instanceof FuncExpr) {
      const prefix = result.args.this instanceof Expression ? result.args.this.name.toUpperCase() : undefined;

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let FuncClass: (new (args: any) => Expression) | undefined = undefined;

      if (prefix === 'NET') {
        FuncClass = NetFuncExpr;
      } else if (prefix === 'SAFE') {
        FuncClass = SafeFuncExpr;
      }

      if (FuncClass) {
      // Retreat to try and parse a known function instead of an anonymous one
        this.retreat(funcIndex);
        result = new FuncClass({
          this: this.parseFunction({ anyToken: true }),
        });
      }
    }

    return result;
  }
}

export class BigQueryGenerator extends Generator {
  static INTERVAL_ALLOWS_PLURAL_FORM = false;
  static JOIN_HINTS = false;
  static QUERY_HINTS = false;
  static TABLE_HINTS = false;
  static LIMIT_FETCH = 'LIMIT';
  static RENAME_TABLE_WITH_DB = false;
  static NVL2_SUPPORTED = false;
  static UNNEST_WITH_ORDINALITY = false;
  static COLLATE_IS_FUNC = true;
  static LIMIT_ONLY_LITERALS = true;
  static SUPPORTS_TABLE_ALIAS_COLUMNS = false;
  static UNPIVOT_ALIASES_ARE_IDENTIFIERS = false;
  static JSON_KEY_VALUE_PAIR_SEP = ',';
  static NULL_ORDERING_SUPPORTED = false;
  static IGNORE_NULLS_IN_FUNC = true;
  static JSON_PATH_SINGLE_QUOTE_ESCAPE = true;
  static CAN_IMPLEMENT_ARRAY_ANY = true;
  static SUPPORTS_TO_NUMBER = false;
  static NAMED_PLACEHOLDER_TOKEN = '@';
  static HEX_FUNC = 'TO_HEX';
  static WITH_PROPERTIES_PREFIX = 'OPTIONS';
  static SUPPORTS_EXPLODING_PROJECTIONS = false;
  static EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = false;
  static SUPPORTS_UNIX_SECONDS = true;

  static SAFE_JSON_PATH_KEY_RE = /^[_\-a-zA-Z][\-\w]*$/;

  static TS_OR_DS_TYPES = [
    TsOrDsToDatetimeExpr,
    TsOrDsToTimestampExpr,
    TsOrDsToTimeExpr,
    TsOrDsToDateExpr,
  ];

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static ORIGINAL_TRANSFORMS = new Map<typeof Expression, (self: any, e: any) => string>([
    ...Generator.TRANSFORMS,
    [ApproxTopKExpr, renameFunc('APPROX_TOP_COUNT')],
    [ApproxDistinctExpr, renameFunc('APPROX_COUNT_DISTINCT')],
    [ArgMaxExpr, argMaxOrMinNoCount('MAX_BY')],
    [ArgMinExpr, argMaxOrMinNoCount('MIN_BY')],
    [ArrayExpr, inlineArrayUnlessQuery],
    [ArrayContainsExpr, arrayContainsSql],
    [ArrayFilterExpr, filterArrayUsingUnnest],
    [ArrayRemoveExpr, filterArrayUsingUnnest],
    [BitwiseAndAggExpr, renameFunc('BIT_AND')],
    [BitwiseOrAggExpr, renameFunc('BIT_OR')],
    [BitwiseXorAggExpr, renameFunc('BIT_XOR')],
    [BitwiseCountExpr, renameFunc('BIT_COUNT')],
    [ByteLengthExpr, renameFunc('BYTE_LENGTH')],
    [CastExpr, preprocess([removePrecisionParameterizedTypes])],
    [
      CollatePropertyExpr,
      (self: BigQueryGenerator, e: CollatePropertyExpr) =>
        e.args.default
          ? `DEFAULT COLLATE ${self.sql(e, 'this')}`
          : `COLLATE ${self.sql(e, 'this')}`,
    ],
    [CommitExpr, () => 'COMMIT TRANSACTION'],
    [CountIfExpr, renameFunc('COUNTIF')],
    [CreateExpr, createSql],
    [CteExpr, preprocess([pushdownCteColumnNames])],
    [DateAddExpr, dateAddIntervalSql('DATE', 'ADD')],
    [
      DateDiffExpr,
      (self: BigQueryGenerator, e: DateDiffExpr) =>
        self.func('DATE_DIFF', [
          e.args.this,
          e.args.expression,
          unitToVar(e),
        ]),
    ],
    [DateFromPartsExpr, renameFunc('DATE')],
    [DateStrToDateExpr, dateStrToDateSql],
    [DateSubExpr, dateAddIntervalSql('DATE', 'SUB')],
    [DatetimeAddExpr, dateAddIntervalSql('DATETIME', 'ADD')],
    [DatetimeSubExpr, dateAddIntervalSql('DATETIME', 'SUB')],
    [DateFromUnixDateExpr, renameFunc('DATE_FROM_UNIX_DATE')],
    [
      FromTimeZoneExpr,
      (self: BigQueryGenerator, e: FromTimeZoneExpr) =>
        self.func(
          'DATETIME',
          [self.func('TIMESTAMP', [e.args.this, e.args.zone]), '\'UTC\''],
        ),
    ],
    [GenerateSeriesExpr, renameFunc('GENERATE_ARRAY')],
    [
      GroupConcatExpr,
      (self: BigQueryGenerator, e: GroupConcatExpr) =>
        groupConcatSql(self, e, {
          funcName: 'STRING_AGG',
          withinGroup: false,
          sep: undefined,
        }),
    ],
    [
      HexExpr,
      (self: BigQueryGenerator, e: HexExpr) =>
        self.func('UPPER', [self.func('TO_HEX', [self.sql(e, 'this')])]),
    ],
    [
      HexStringExpr,
      (self: BigQueryGenerator, e: HexStringExpr) =>
        self.hexStringSql(e, { binaryFunctionRepr: 'FROM_HEX' }),
    ],
    [IfExpr, ifSql('IF', 'NULL')],
    [ILikeExpr, noIlikeSql],
    [IntDivExpr, renameFunc('DIV')],
    [Int64Expr, renameFunc('INT64')],
    [JsonBoolExpr, renameFunc('BOOL')],
    [JsonExtractExpr, jsonExtractSql],
    [JsonExtractArrayExpr, jsonExtractSql],
    [JsonExtractScalarExpr, jsonExtractSql],
    [
      JsonFormatExpr,
      (self: BigQueryGenerator, e: JsonFormatExpr) =>
        self.func(
          e.args.toJson ? 'TO_JSON' : 'TO_JSON_STRING',
          [e.args.this, ...(Array.isArray(e.args.options) ? e.args.options : e.args.options ? [e.args.options] : [])],
        ),
    ],
    [JsonKeysAtDepthExpr, renameFunc('JSON_KEYS')],
    [JsonValueArrayExpr, renameFunc('JSON_VALUE_ARRAY')],
    [LevenshteinExpr, jsonExtractSql], // Assuming logic similarity
    [MaxExpr, maxOrGreatest],
    [
      Md5Expr,
      (self: BigQueryGenerator, e: Md5Expr) =>
        self.func('TO_HEX', [self.func('MD5', [e.args.this])]),
    ],
    [Md5DigestExpr, renameFunc('MD5')],
    [MinExpr, minOrLeast],
    [
      NormalizeExpr,
      (self: BigQueryGenerator, e: NormalizeExpr) =>
        self.func(
          e.args.isCasefold ? 'NORMALIZE_AND_CASEFOLD' : 'NORMALIZE',
          [e.args.this, e.args.form],
        ),
    ],
    [
      PartitionedByPropertyExpr,
      (self: BigQueryGenerator, e: PartitionedByPropertyExpr) =>
        `PARTITION BY ${self.sql(e, 'this')}`,
    ],
    [
      RegexpExtractExpr,
      (self: BigQueryGenerator, e: RegexpExtractExpr) =>
        self.func(
          'REGEXP_EXTRACT',
          [
            e.args.this,
            e.args.expression,
            e.args.position,
            e.args.occurrence,
          ],
        ),
    ],
    [
      RegexpExtractAllExpr,
      (self: BigQueryGenerator, e: RegexpExtractAllExpr) =>
        self.func('REGEXP_EXTRACT_ALL', [e.args.this, e.args.expression]),
    ],
    [RegexpReplaceExpr, regexpReplaceSql],
    [RegexpLikeExpr, renameFunc('REGEXP_CONTAINS')],
    [ReturnsPropertyExpr, returnsPropertySql],
    [RollbackExpr, () => 'ROLLBACK TRANSACTION'],
    [
      ParseTimeExpr,
      (self: BigQueryGenerator, e: ParseTimeExpr) =>
        self.func('PARSE_TIME', [self.formatTime(e)!, e.args.this]),
    ],
    [
      ParseDatetimeExpr,
      (self: BigQueryGenerator, e: ParseDatetimeExpr) =>
        self.func('PARSE_DATETIME', [self.formatTime(e)!, e.args.this]),
    ],
    [
      SelectExpr,
      preprocess([
        explodeProjectionToUnnest(),
        unqualifyUnnest,
        eliminateDistinctOn,
        aliasOrderedGroup,
        eliminateSemiAndAntiJoins,
      ]),
    ],
    [ShaExpr, renameFunc('SHA1')],
    [Sha2Expr, sha256Sql],
    [Sha1DigestExpr, renameFunc('SHA1')],
    [Sha2DigestExpr, sha2DigestSql],
    [
      StabilityPropertyExpr,
      (self: BigQueryGenerator, e: StabilityPropertyExpr) =>
        e.name === 'IMMUTABLE' ? 'DETERMINISTIC' : 'NOT DETERMINISTIC',
    ],
    [StringExpr, renameFunc('STRING')],
    [
      StrPositionExpr,
      (self: BigQueryGenerator, e: StrPositionExpr) =>
        strPositionSql(self, e, {
          funcName: 'INSTR',
          supportsPosition: true,
          supportsOccurrence: true,
        }),
    ],
    [StrToDateExpr, strToDatetimeSql],
    [StrToTimeExpr, strToDatetimeSql],
    [SessionUserExpr, () => 'SESSION_USER()'],
    [TimeAddExpr, dateAddIntervalSql('TIME', 'ADD')],
    [TimeFromPartsExpr, renameFunc('TIME')],
    [TimestampFromPartsExpr, renameFunc('DATETIME')],
    [TimeSubExpr, dateAddIntervalSql('TIME', 'SUB')],
    [TimestampAddExpr, dateAddIntervalSql('TIMESTAMP', 'ADD')],
    [TimestampDiffExpr, renameFunc('TIMESTAMP_DIFF')],
    [TimestampSubExpr, dateAddIntervalSql('TIMESTAMP', 'SUB')],
    [TimeStrToTimeExpr, timeStrToTimeSql],
    [TransactionExpr, () => 'BEGIN TRANSACTION'],
    [TsOrDsAddExpr, tsOrDsAddSql],
    [TsOrDsDiffExpr, tsOrDsDiffSql],
    [TsOrDsToTimeExpr, renameFunc('TIME')],
    [TsOrDsToDatetimeExpr, renameFunc('DATETIME')],
    [TsOrDsToTimestampExpr, renameFunc('TIMESTAMP')],
    [UnhexExpr, renameFunc('FROM_HEX')],
    [UnixDateExpr, renameFunc('UNIX_DATE')],
    [UnixToTimeExpr, unixToTimeSql],
    [UuidExpr, () => 'GENERATE_UUID()'],
    [ValuesExpr, derivedTableValuesToUnnest],
    [VariancePopExpr, renameFunc('VAR_POP')],
    [SafeDivideExpr, renameFunc('SAFE_DIVIDE')],
  ]);

  static SUPPORTED_JSON_PATH_PARTS = new Set([
    JsonPathKeyExpr,
    JsonPathRootExpr,
    JsonPathSubscriptExpr,
  ]);

  static TYPE_MAPPING = {
    ...Generator.TYPE_MAPPING,
    [DataTypeExprKind.BIGDECIMAL]: 'BIGNUMERIC',
    [DataTypeExprKind.BIGINT]: 'INT64',
    [DataTypeExprKind.BINARY]: 'BYTES',
    [DataTypeExprKind.BLOB]: 'BYTES',
    [DataTypeExprKind.BOOLEAN]: 'BOOL',
    [DataTypeExprKind.CHAR]: 'STRING',
    [DataTypeExprKind.DECIMAL]: 'NUMERIC',
    [DataTypeExprKind.DOUBLE]: 'FLOAT64',
    [DataTypeExprKind.FLOAT]: 'FLOAT64',
    [DataTypeExprKind.INT]: 'INT64',
    [DataTypeExprKind.NCHAR]: 'STRING',
    [DataTypeExprKind.NVARCHAR]: 'STRING',
    [DataTypeExprKind.SMALLINT]: 'INT64',
    [DataTypeExprKind.TEXT]: 'STRING',
    [DataTypeExprKind.TIMESTAMP]: 'DATETIME',
    [DataTypeExprKind.TIMESTAMPNTZ]: 'DATETIME',
    [DataTypeExprKind.TIMESTAMPTZ]: 'TIMESTAMP',
    [DataTypeExprKind.TIMESTAMPLTZ]: 'TIMESTAMP',
    [DataTypeExprKind.TINYINT]: 'INT64',
    [DataTypeExprKind.ROWVERSION]: 'BYTES',
    [DataTypeExprKind.UUID]: 'STRING',
    [DataTypeExprKind.VARBINARY]: 'BYTES',
    [DataTypeExprKind.VARCHAR]: 'STRING',
    [DataTypeExprKind.VARIANT]: 'ANY TYPE',
  };

  static PROPERTIES_LOCATION = new Map([
    ...Generator.PROPERTIES_LOCATION,
    [PartitionedByPropertyExpr, PropertiesLocation.POST_SCHEMA],
    [VolatilePropertyExpr, PropertiesLocation.UNSUPPORTED],
  ]);

  static AFTER_HAVING_MODIFIER_TRANSFORMS = {
    qualify: Generator.AFTER_HAVING_MODIFIER_TRANSFORMS.qualify,
    windows: Generator.AFTER_HAVING_MODIFIER_TRANSFORMS.windows,
  };

  static RESERVED_KEYWORDS = new Set([
    'all',
    'and',
    'any',
    'array',
    'as',
    'asc',
    'assert_rows_modified',
    'at',
    'between',
    'by',
    'case',
    'cast',
    'collate',
    'contains',
    'create',
    'cross',
    'cube',
    'current',
    'default',
    'define',
    'desc',
    'distinct',
    'else',
    'end',
    'enum',
    'escape',
    'except',
    'exclude',
    'exists',
    'extract',
    'false',
    'fetch',
    'following',
    'for',
    'from',
    'full',
    'group',
    'grouping',
    'groups',
    'hash',
    'having',
    'if',
    'ignore',
    'in',
    'inner',
    'intersect',
    'interval',
    'into',
    'is',
    'join',
    'lateral',
    'left',
    'like',
    'limit',
    'lookup',
    'merge',
    'natural',
    'new',
    'no',
    'not',
    'null',
    'nulls',
    'of',
    'on',
    'or',
    'order',
    'outer',
    'over',
    'partition',
    'preceding',
    'proto',
    'qualify',
    'range',
    'recursive',
    'respect',
    'right',
    'rollup',
    'rows',
    'select',
    'set',
    'some',
    'struct',
    'tablesample',
    'then',
    'to',
    'treat',
    'true',
    'unbounded',
    'union',
    'unnest',
    'using',
    'when',
    'where',
    'window',
    'with',
    'within',
  ]);

  dateTruncSql (expression: DateTruncExpr): string {
    const unit = expression.unit;
    const unitSql = unit?.isString ? unit.name : this.sql(unit);
    return this.func('DATE_TRUNC', [
      expression.args.this,
      unitSql,
      expression.args.zone,
    ]);
  }

  modSql (expression: ModExpr): string {
    const thisNode = expression.args.this;
    const expr = expression.args.expression;

    return this.func(
      'MOD',
      [thisNode instanceof ParenExpr ? thisNode.unnest() : thisNode, expr instanceof ParenExpr ? expr.unnest() : expr],
    );
  }

  columnParts (expression: ColumnExpr): string {
    if (expression.meta?.quotedColumn) {
    // If a column reference is of the form `dataset.table`.name, we need
    // to preserve the quoted table path, otherwise the reference breaks
      const tableParts = expression.parts.slice(0, -1).map((p) => p.name)
        .join('.');
      const tablePath = this.sql(new IdentifierExpr({
        this: tableParts,
        quoted: true,
      }));
      return `${tablePath}.${this.sql(expression, 'this')}`;
    }

    return super.columnParts(expression);
  }

  tableParts (expression: TableExpr): string {
  // Depending on the context, `x.y` may not resolve to the same data source as `x`.`y`
    if (expression.meta?.quotedTable) {
      const tableParts = expression.parts.map((p) => p.name).join('.');
      return this.sql(new IdentifierExpr({
        this: tableParts,
        quoted: true,
      }));
    }

    return super.tableParts(expression);
  }

  timeToStrSql (expression: TimeToStrExpr): string {
    const thisNode = expression.args.this;
    let funcName: string;

    if (thisNode instanceof TsOrDsToDatetimeExpr) {
      funcName = 'FORMAT_DATETIME';
    } else if (thisNode instanceof TsOrDsToTimestampExpr) {
      funcName = 'FORMAT_TIMESTAMP';
    } else if (thisNode instanceof TsOrDsToTimeExpr) {
      funcName = 'FORMAT_TIME';
    } else {
      funcName = 'FORMAT_DATE';
    }

    const isTsOrDs = (this._constructor as typeof BigQueryGenerator).TS_OR_DS_TYPES.some(
      (type) => thisNode instanceof type,
    );

    const timeExpr = (isTsOrDs && thisNode) ? thisNode : expression;

    return this.func(
      funcName,
      [
        this.formatTime(expression),
        narrowInstanceOf(timeExpr.args.this, 'string', Expression),
        expression.args.zone,
      ],
    );
  }

  eqSql (expression: EqExpr): string {
  // Operands of = cannot be NULL in BigQuery
    if (expression.left instanceof NullExpr || expression.right instanceof NullExpr) {
      if (!(expression.parent instanceof UpdateExpr)) {
        return 'NULL';
      }
    }

    return this.binary(expression, '=');
  }

  atTimeZoneSql (expression: AtTimeZoneExpr): string {
    const parent = expression.parent;

    // BigQuery allows CAST(.. AS {STRING|TIMESTAMP} [FORMAT <fmt> [AT TIME ZONE <tz>]]).
    // Only the TIMESTAMP one should use the below conversion.
    if (
      !(parent instanceof CastExpr)
      || !isType(parent.args.to, 'text')
    ) {
      return this.func(
        'TIMESTAMP',
        [this.func('DATETIME', [expression.args.this, expression.args.zone])],
      );
    }

    return super.atTimeZoneSql(expression);
  }

  tryCastSql (expression: TryCastExpr): string {
    return this.castSql(expression, { safePrefix: 'SAFE_' });
  }

  bracketSql (expression: BracketExpr): string {
    const thisNode = expression.args.this;
    const expressions = expression.args.expressions;

    if (expressions && expressions.length === 1 && thisNode instanceof Expression && thisNode.isType(DataTypeExprKind.STRUCT)) {
      let arg = expressions[0];
      if (!arg.type) {
        arg = annotateTypes(arg, { dialect: this.dialect });
      }

      if (arg.type && DataTypeExpr.TEXT_TYPES.has(narrowInstanceOf(arg.type, Expression)?.args.this as DataTypeExprKind)) {
      // BQ doesn't support bracket syntax with string values for structs
        return `${this.sql(thisNode)}.${arg.name}`;
      }
    }

    let expressionsSql = this.expressions(expression, { flat: true });
    const offset = expression.args.offset;

    if (offset === 0) {
      expressionsSql = `OFFSET(${expressionsSql})`;
    } else if (offset === 1) {
      expressionsSql = `ORDINAL(${expressionsSql})`;
    } else if (offset !== undefined) {
      this.unsupported(`Unsupported array offset: ${offset}`);
    }

    if (expression.args.safe) {
      expressionsSql = `SAFE_${expressionsSql}`;
    }

    return `${this.sql(thisNode)}[${expressionsSql}]`;
  }

  inUnnestOp (expression: UnnestExpr): string {
    return this.sql(expression);
  }

  versionSql (expression: VersionExpr): string {
    if (expression.name === 'TIMESTAMP') {
      expression.setArgKey('this', 'SYSTEM_TIME');
    }
    return super.versionSql(expression);
  }

  containsSql (expression: ContainsExpr): string {
    const _thisNode = expression.args.this;
    const _expr = expression.args.expression;
    let thisNode: string | Expression | undefined = (_thisNode instanceof Expression || typeof _thisNode === 'string') ? _thisNode : undefined;
    let expr: string | Expression | undefined = (_expr instanceof Expression || typeof _expr === 'string') ? _expr : undefined;

    if (thisNode instanceof LowerExpr && expr instanceof LowerExpr) {
      const tn = thisNode.args.this;
      const ex = expr.args.this;
      thisNode = (tn instanceof Expression || typeof tn === 'string') ? tn : thisNode;
      expr = (ex instanceof Expression || typeof ex === 'string') ? ex : expr;
    }

    return this.func('CONTAINS_SUBSTR', [
      thisNode,
      expr,
      expression.args.jsonScope,
    ]);
  }

  castSql (expression: CastExpr, options: { safePrefix?: string } = {}): string {
    const { safePrefix } = options;
    const thisNode = expression.args.this;

    // This ensures that inline type-annotated ARRAY literals like ARRAY<INT64>[1, 2, 3]
    // are roundtripped unaffected. The inner check excludes ARRAY(SELECT ...) expressions.
    if (thisNode instanceof ArrayExpr) {
      const elem = seqGet(thisNode.args.expressions || [], 0);
      if (!(elem instanceof Expression && elem.find(QueryExpr))) {
        return `${this.sql(expression, 'to')}${this.sql(thisNode)}`;
      }
    }

    return super.castSql(expression, { safePrefix });
  }

  declareItemSql (expression: DeclareItemExpr): string {
    const variables = this.expressions(expression, { key: 'this' });

    let defaultValue = this.sql(expression, 'default');
    defaultValue = defaultValue ? ` DEFAULT ${defaultValue}` : '';

    let kind = this.sql(expression, 'kind');
    kind = kind ? ` ${kind}` : '';

    return `${variables}${kind}${defaultValue}`;
  }
}

export class BigQueryJsonPathTokenizer extends JsonPathTokenizer {
  static VAR_TOKENS = new Set([TokenType.DASH, TokenType.VAR]);
}

export class BigQuery extends Dialect {
  public static WEEK_OFFSET = -1;
  public static UNNEST_COLUMN_ONLY = true;
  public static SUPPORTS_USER_DEFINED_TYPES = false;
  public static SUPPORTS_SEMI_ANTI_JOIN = false;
  public static LOG_BASE_FIRST = false;
  public static HEX_LOWERCASE = true;
  public static FORCE_EARLY_ALIAS_REF_EXPANSION = true;
  public static EXPAND_ONLY_GROUP_ALIAS_REF = true;
  public static PRESERVE_ORIGINAL_NAMES = true;
  public static HEX_STRING_IS_INTEGER_TYPE = true;
  public static BYTE_STRING_IS_BYTES_TYPE = true;
  public static UUID_IS_STRING_TYPE = true;
  public static ANNOTATE_ALL_SCOPES = true;
  public static PROJECTION_ALIASES_SHADOW_SOURCE_NAMES = true;
  public static TABLES_REFERENCEABLE_AS_COLUMNS = true;
  public static SUPPORTS_STRUCT_STAR_EXPANSION = true;
  public static EXCLUDES_PSEUDOCOLUMNS_FROM_STAR = true;
  public static QUERY_RESULTS_ARE_STRUCTS = true;
  public static JSON_EXTRACT_SCALAR_SCALAR_ONLY = true;
  public static LEAST_GREATEST_IGNORES_NULLS = false;
  public static DEFAULT_NULL_TYPE = DataTypeExprKind.BIGINT;
  public static PRIORITIZE_NON_LITERAL_TYPES = true;

  public static INITCAP_DEFAULT_DELIMITER_CHARS = ' \t\n\r\f\v\\[\\](){}/|<>!?@"^#$&~_,.:;*%+\\-';

  public static NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE;
  public static NORMALIZE_FUNCTIONS = false as const;

  public static TIME_MAPPING = {
    '%x': '%m/%d/%y',
    '%D': '%m/%d/%y',
    '%E6S': '%S.%f',
    '%e': '%-d',
    '%F': '%Y-%m-%d',
    '%T': '%H:%M:%S',
    '%c': '%a %b %e %H:%M:%S %Y',
  };

  public static INVERSE_TIME_MAPPING = {
    '%H:%M:%S.%f': '%H:%M:%E6S',
  };

  public static FORMAT_MAPPING: Record<string, string> = {
    DD: '%d',
    MM: '%m',
    MON: '%b',
    MONTH: '%B',
    YYYY: '%Y',
    YY: '%y',
    HH: '%I',
    HH12: '%I',
    HH24: '%H',
    MI: '%M',
    SS: '%S',
    SSSSS: '%f',
    TZH: '%z',
  };

  // The _PARTITIONTIME and _PARTITIONDATE pseudo-columns are not returned by a SELECT * statement
  // https://cloud.google.com/bigquery/docs/querying-partitioned-tables#query_an_ingestion-time_partitioned_table
  // https://cloud.google.com/bigquery/docs/querying-wildcard-tables#scanning_a_range_of_tables_using_table_suffix
  // https://cloud.google.com/bigquery/docs/query-cloud-storage-data#query_the_file_name_pseudo-column
  public static PSEUDOCOLUMNS: Set<string> = new Set([
    '_PARTITIONTIME',
    '_PARTITIONDATE',
    '_TABLE_SUFFIX',
    '_FILE_NAME',
    '_DBT_MAX_PARTITION',
  ]);

  // All set operations require either a DISTINCT or ALL specifier
  public static SET_OP_DISTINCT_BY_DEFAULT: Partial<Record<ExpressionKey, boolean>> = {};

  // https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#percentile_cont
  public static COERCES_TO: Record<string, Set<string>> = (() => {
    const base: Record<string, Set<string>> = {};
    for (const [k, v] of TypeAnnotator.COERCES_TO) {
      base[k] = new Set(v as Iterable<string>);
    }
    base[DataTypeExprKind.BIGDECIMAL] = new Set([DataTypeExprKind.DOUBLE]);
    base[DataTypeExprKind.DECIMAL]?.add(DataTypeExprKind.BIGDECIMAL);
    base[DataTypeExprKind.BIGINT]?.add(DataTypeExprKind.BIGDECIMAL);
    if (!base[DataTypeExprKind.VARCHAR]) base[DataTypeExprKind.VARCHAR] = new Set();
    for (const t of [
      DataTypeExprKind.DATE,
      DataTypeExprKind.DATETIME,
      DataTypeExprKind.TIME,
      DataTypeExprKind.TIMESTAMP,
      DataTypeExprKind.TIMESTAMPTZ,
    ]) base[DataTypeExprKind.VARCHAR].add(t);
    return base;
  })();

  public static EXPRESSION_METADATA: ExpressionMetadata = new Map(BIGQUERY_EXPRESSION_METADATA);

  normalizeIdentifier<E extends Expression> (expression: E): E {
    if (
      expression instanceof IdentifierExpr
      && this._constructor.NORMALIZATION_STRATEGY === NormalizationStrategy.CASE_INSENSITIVE
    ) {
      let parent = expression.parent;
      while (parent instanceof DotExpr) {
        parent = parent.parent;
      }

      // In BigQuery, CTEs are case-insensitive, but UDF and table names are case-sensitive
      // by default. The following check uses a heuristic to detect tables based on whether
      // they are qualified. This should generally be correct, because tables in BigQuery
      // must be qualified with at least a dataset, unless @@dataset_id is set.
      const caseSensitive = (
        parent instanceof UserDefinedFunctionExpr
        || (
          parent instanceof TableExpr
          && parent.db
          && (parent.meta['quotedTable'] || !parent.meta['maybeColumn'])
        )
        || expression.meta['isTable']
      );

      if (!caseSensitive) {
        expression.setArgKey('this', (expression.args.this as string).toLowerCase());
      }

      return expression;
    }

    return super.normalizeIdentifier(expression);
  }

  public static Tokenizer = BigQueryTokenizer;
  public static Parser = BigQueryParser;
  public static Generator = BigQueryGenerator;
  public static JsonPathTokenizer = BigQueryJsonPathTokenizer;
}

Dialect.register(Dialects.BIGQUERY, BigQuery);
