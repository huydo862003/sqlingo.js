import { cache } from '../port_internals';
import {
  Generator,
} from '../generator';
import {
  Parser, binaryRangeParser,
} from '../parser';
import type { TokenPair } from '../tokens';
import {
  Tokenizer, TokenType,
} from '../tokens';
import type {
  BracketExpr,
  LateralExpr,
  ArrayContainsExpr,
  IsAsciiExpr,
  RespectNullsExpr,
  IgnoreNullsExpr,
  AlterSetExpr,
  SchemaCommentPropertyExpr,
  UnnestExpr,
  GeneratedAsRowColumnConstraintExpr,
} from '../expressions';
import {
  DataTypeExprKind,
  CastExpr,
  Expression,
  DataTypeExpr,
  DateAddExpr,
  DateSubExpr,
  DateDiffExpr,
  AnyValueExpr,
  RoundExpr,
  SubstringExpr,
  IdentifierExpr,
  BooleanExpr,
  ParenExpr,
  DotExpr,
  ComputedColumnConstraintExpr,
  DivExpr,
  select,
  CurrentTimestampExpr,
  GenerateSeriesExpr,
  ArrayExpr,
  SelectExpr,
  StrToTimeExpr,
  TimeToStrExpr,
  TryCastExpr,
  ArrayPrependExpr,
  BitwiseAndAggExpr,
  BitwiseOrAggExpr,
  BitwiseXorAggExpr,
  BitwiseXorExpr,
  CurrentVersionExpr,
  GetbitExpr,
  LevenshteinExpr,
  StrToDateExpr,
  UnixToTimeExpr,
  Sha2Expr,
  Sha2DigestExpr,
  TsOrDsAddExpr,
  TsOrDsDiffExpr,
  IntDivExpr,
  GroupConcatExpr,
  LogicalOrExpr,
  LogicalAndExpr,
  RandExpr,
  RegexpReplaceExpr,
  RegexpILikeExpr,
  RegexpLikeExpr,
  StrPositionExpr,
  TimeStrToTimeExpr,
  TimestampTruncExpr,
  VarianceExpr,
  VariancePopExpr,
  CountIfExpr,
  TimeFromPartsExpr,
  TimestampFromPartsExpr,
  DateStrToDateExpr,
  ExplodeExpr,
  ArrayAppendExpr,
  ArrayConcatExpr,
  ArrayFilterExpr,
  JsonExtractExpr,
  JsonExtractScalarExpr,
  XorExpr,
  MergeExpr,
  MinExpr,
  MaxExpr,
  PercentileContExpr,
  PercentileDiscExpr,
  JsonObjectAggExpr,
  TrimExpr,
  StructExtractExpr,
  ColumnDefExpr,
  CurrentDateExpr,
  CurrentUserExpr,
  JsonArrayAggExpr,
  JsonbObjectAggExpr,
  WidthBucketExpr,
  PlaceholderExpr,
  VariadicExpr,
  CurrentSchemaExpr,
  BitwiseNotExpr,
  IntervalExpr,
  ArrayOverlapsExpr,
  ExplodingGenerateSeriesExpr,
  AutoIncrementColumnConstraintExpr,
  GeneratedAsIdentityColumnConstraintExpr,
  ColumnConstraintExpr,
  NotNullColumnConstraintExpr,
  LiteralExpr,
  CaseExpr,
  IsExpr,
  CoalesceExpr,
  AnyExpr,
  JsonbExtractExpr,
  JsonbExtractScalarExpr,
  JsonbContainsExpr,
  JsonPathKeyExpr,
  JsonPathRootExpr,
  JsonPathSubscriptExpr,
  UuidExpr,
  TimeToUnixExpr,
  UnicodeExpr,
  PowExpr,
  MatchAgainstExpr,
  ColumnExpr,
  ExtractExpr,
  JsonbExistsExpr,
  null_,
  toInterval,
  SetConfigPropertyExpr,
  LengthExpr,
  ParseJsonExpr,
  LastDayExpr,
  MapFromEntriesExpr,
  PivotExpr,
  PartitionedByPropertyExpr,
  ToCharExpr,
  CommentColumnConstraintExpr,
  PropertiesLocation,
  TransientPropertyExpr,
  VolatilePropertyExpr,
  InOutColumnConstraintExpr,
  GenerateDateArrayExpr,
  JoinExpr,
  FromExpr,
  TableExpr,
  isType,
} from '../expressions';
import { seqGet } from '../helper';
import {
  eliminateSemiAndAntiJoins, eliminateQualify,
  addWithinGroupForPercentiles, preprocess,
} from '../transforms';
import { annotateTypes } from '../optimizer';
import {
  anyValueToMaxSql,
  arrayConcatSql,
  arrayAppendSql,
  binaryFromFunction,
  boolXorSql,
  buildFormattedTime,
  buildJsonExtractPath,
  buildTimestampTrunc,
  countIfToSum,
  dateStrToDateSql,
  filterArrayUsingUnnest,
  getBitSql,
  groupConcatSql,
  maxOrGreatest,
  mergeWithoutTargetSql,
  minOrLeast,
  noTrycastSql,
  regexpReplaceGlobalModifier,
  renameFunc,
  sha256Sql,
  sha2DigestSql,
  strPositionSql,
  structExtractSql,
  timeStrToTimeSql,
  timestampTruncSql,
  trimSql,
  noParenCurrentDateSql,
  jsonExtractSegments,
  jsonPathKeyOnlyName,
  noLastDaySql,
  noMapFromEntriesSql,
  noPivotSql,
  inlineArraySql,
  tsOrDsAddCast,
  Dialect, Dialects,
} from './dialect';

const DATE_DIFF_FACTOR: Record<string, string> = {
  MICROSECOND: ' * 1000000',
  MILLISECOND: ' * 1000',
  SECOND: '',
  MINUTE: ' / 60',
  HOUR: ' / 3600',
  DAY: ' / 86400',
};

function dateAddSql (kind: string) {
  return (self: Generator, expression: DateAddExpr | DateSubExpr | TsOrDsAddExpr): string => {
    let expr = expression;
    if (expr instanceof TsOrDsAddExpr) {
      expr = tsOrDsAddCast(expr);
    }

    const thisSql = self.sql(expr, 'this');
    const unit = expr.args.unit;

    let e = expr.args.expression && self.simplifyUnlessLiteral(expr.args.expression);
    if (e instanceof LiteralExpr) {
      e.setArgKey('isString', true);
    } else if (e && e.isNumber) {
      e = LiteralExpr.string(e.args.this);
    } else {
      self.unsupported('Cannot add non-literal');
    }

    return `${thisSql} ${kind} ${self.sql(new IntervalExpr({
      this: e,
      unit,
    }))}`;
  };
}

function dateDiffSql (self: Generator, expression: DateDiffExpr): string {
  const unit = expression.text('unit').toUpperCase();
  const factor = DATE_DIFF_FACTOR[unit];

  const end = `CAST(${self.sql(expression, 'this')} AS TIMESTAMP)`;
  const start = `CAST(${self.sql(expression, 'expression')} AS TIMESTAMP)`;

  if (factor !== undefined) {
    return `CAST(EXTRACT(epoch FROM ${end} - ${start})${factor} AS BIGINT)`;
  }

  const age = `AGE(${end}, ${start})`;
  let unitSql: string;

  if (unit === 'WEEK') {
    unitSql = `EXTRACT(days FROM (${end} - {start})) / 7`;
  } else if (unit === 'MONTH') {
    unitSql = `EXTRACT(year FROM ${age}) * 12 + EXTRACT(month FROM ${age})`;
  } else if (unit === 'QUARTER') {
    unitSql = `EXTRACT(year FROM ${age}) * 4 + EXTRACT(month FROM ${age}) / 3`;
  } else if (unit === 'YEAR') {
    unitSql = `EXTRACT(year FROM ${age})`;
  } else {
    unitSql = age;
  }

  return `CAST(${unitSql} AS BIGINT)`;
}

function substringSql (self: Generator, expression: SubstringExpr): string {
  const thisSql = self.sql(expression, 'this');
  const start = self.sql(expression, 'start');
  const length = self.sql(expression, 'length');

  const fromPart = start ? ` FROM ${start}` : '';
  const forPart = length ? ` FOR ${length}` : '';

  return `SUBSTRING(${thisSql}${fromPart}${forPart})`;
}

function autoIncrementToSerial (expression: Expression): Expression {
  const auto = expression.find(AutoIncrementColumnConstraintExpr);

  if (auto && expression instanceof ColumnDefExpr) {
    const constraints = expression.args.constraints || [];
    expression.setArgKey('constraints', constraints.filter((c) => c !== auto.parent));

    const kind = expression.args.kind;
    if (kind instanceof DataTypeExpr) {
      if (kind.isType(DataTypeExprKind.INT)) {
        kind.replace(new DataTypeExpr({ this: DataTypeExprKind.SERIAL }));
      } else if (kind.isType(DataTypeExprKind.SMALLINT)) {
        kind.replace(new DataTypeExpr({ this: DataTypeExprKind.SMALLSERIAL }));
      } else if (kind.isType(DataTypeExprKind.BIGINT)) {
        kind.replace(new DataTypeExpr({ this: DataTypeExprKind.BIGSERIAL }));
      }
    }
  }

  return expression;
}

function serialToGenerated (expression: Expression): Expression {
  if (!(expression instanceof ColumnDefExpr)) return expression;

  const kind = expression.args.kind;
  if (!(kind instanceof DataTypeExpr)) return expression;

  let dataType: DataTypeExpr | undefined;
  if (kind.isType(DataTypeExprKind.SERIAL)) {
    dataType = new DataTypeExpr({ this: DataTypeExprKind.INT });
  } else if (kind.isType(DataTypeExprKind.SMALLSERIAL)) {
    dataType = new DataTypeExpr({ this: DataTypeExprKind.SMALLINT });
  } else if (kind.isType(DataTypeExprKind.BIGSERIAL)) {
    dataType = new DataTypeExpr({ this: DataTypeExprKind.BIGINT });
  }

  if (dataType) {
    kind.replace(dataType);
    const constraints = expression.args.constraints || [];
    const generated = new ColumnConstraintExpr({ kind: new GeneratedAsIdentityColumnConstraintExpr({ this: false }) });
    const notNull = new ColumnConstraintExpr({ kind: new NotNullColumnConstraintExpr({}) });

    if (!constraints.some((c) => c instanceof ColumnConstraintExpr && c.args.kind instanceof NotNullColumnConstraintExpr)) {
      constraints.unshift(notNull);
    }
    if (!constraints.some((c) => c instanceof ColumnConstraintExpr && c.args.kind instanceof GeneratedAsIdentityColumnConstraintExpr)) {
      constraints.unshift(generated);
    }
    expression.setArgKey('constraints', constraints);
  }

  return expression;
}

function buildGenerateSeries (args: Expression[]): ExplodingGenerateSeriesExpr {
  const step = seqGet(args, 2);
  if (step) {
    if (step instanceof LiteralExpr && step.isString) {
      args[2] = toInterval(step.args.this);
    } else if (step instanceof IntervalExpr && !step.args.unit) {
      args[2] = toInterval(step.args.this?.args.this as string | LiteralExpr);
    }
  }

  return ExplodingGenerateSeriesExpr.fromArgList(args);
}

function buildToTimestamp (args: Expression[]): UnixToTimeExpr | StrToTimeExpr {
  if (args.length === 1) {
    return UnixToTimeExpr.fromArgList(args);
  }
  return buildFormattedTime(StrToTimeExpr, { dialect: 'postgres' })(args);
}

function jsonExtractSql (name: string, op: string) {
  return (self: Generator, expression: JsonExtractExpr | JsonExtractScalarExpr): string => {
    const onlyJsonTypes = expression.args.onlyJsonTypes;
    return jsonExtractSegments(name, {
      quotedIndex: !onlyJsonTypes,
      op,
    })(self, expression);
  };
}

function buildRegexpReplace (args: Expression[]): RegexpReplaceExpr {
  let regexpReplace: RegexpReplaceExpr | undefined;

  if (3 < args.length) {
    const last = args[args.length - 1];
    if (!(last instanceof LiteralExpr && last.isNumber)) {
      // In TS we skip the complex type annotation check unless necessary,
      // but we assume if it's a string literal, it's the modifiers
      if (last instanceof LiteralExpr && last.isString) {
        regexpReplace = RegexpReplaceExpr.fromArgList(args.slice(0, -1));
        regexpReplace.setArgKey('modifiers', last);
      }
    }
  }

  regexpReplace = regexpReplace || RegexpReplaceExpr.fromArgList(args);
  regexpReplace.setArgKey('singleReplace', true);
  return regexpReplace;
}

function unixToTimeSql (self: Generator, expression: UnixToTimeExpr): string {
  const scale = expression.args.scale;
  const timestamp = expression.args.this;

  if (scale === undefined || scale === UnixToTimeExpr.SECONDS) {
    return self.func('TO_TIMESTAMP', [timestamp, self.formatTime(expression)]);
  }

  const div = new DivExpr({
    this: timestamp,
    expression: self.func('POW', [LiteralExpr.number(10), scale]),
  });

  return self.func('TO_TIMESTAMP', [div, self.formatTime(expression)]);
}

function buildLevenshteinLessEqual (args: Expression[]): LevenshteinExpr {
  const maxDist = args.pop();

  return new LevenshteinExpr({
    this: seqGet(args, 0),
    expression: seqGet(args, 1),
    insCost: seqGet(args, 2),
    delCost: seqGet(args, 3),
    subCost: seqGet(args, 4),
    maxDist: maxDist,
  });
}

function levenshteinSql (self: Generator, expression: LevenshteinExpr): string {
  const name = expression.args.maxDist ? 'LEVENSHTEIN_LESS_EQUAL' : 'LEVENSHTEIN';
  return renameFunc(name)(self, expression);
}

function versionedAnyValueSql (self: Generator, expression: AnyValueExpr): string {
  if (self.dialect.version.major < 16) {
    return anyValueToMaxSql(self, expression);
  }
  return renameFunc('ANY_VALUE')(self, expression);
}

function roundSql (self: Generator, expression: RoundExpr): string {
  const thisSql = self.sql(expression, 'this');
  const decimals = self.sql(expression, 'decimals');

  if (!decimals) {
    return self.func('ROUND', [thisSql]);
  }

  let currentThis = thisSql;
  // If the input is double precision, we must cast to decimal in Postgres
  if (expression.args.this instanceof Expression && expression.args.this.isType(DataTypeExprKind.DOUBLE)) {
    const decimalType = DataTypeExpr.build(DataTypeExprKind.DECIMAL, { expressions: expression.args.expressions });
    currentThis = self.sql(new CastExpr({
      this: thisSql,
      to: decimalType,
    }));
  }

  return self.func('ROUND', [currentThis, decimals]);
}

export class PostgresTokenizer extends Tokenizer {
  static BIT_STRINGS: TokenPair[] = [
    ['b\'', '\''],
    ['B\'', '\''],
    '0b',
  ];

  static HEX_STRINGS: TokenPair[] = [
    ['x\'', '\''],
    ['X\'', '\''],
    '0x',
  ];

  static BYTE_STRINGS: TokenPair[] = [['e\'', '\''], ['E\'', '\'']];
  static BYTE_STRING_ESCAPES = ['\'', '\\'];
  static HEREDOC_STRINGS: TokenPair[] = ['$'];

  static HEREDOC_TAG_IS_IDENTIFIER = true;
  static HEREDOC_STRING_ALTERNATIVE = TokenType.PARAMETER;

  static ORIGINAL_KEYWORDS: Record<string, TokenType> = (() => {
    const {
      '/*+': _1, DIV: _2, ...kw
    } = Tokenizer.KEYWORDS;
    return {
      ...kw,
      '~': TokenType.RLIKE,
      '@@': TokenType.DAT,
      '@>': TokenType.AT_GT,
      '<@': TokenType.LT_AT,
      '?&': TokenType.QMARK_AMP,
      '?|': TokenType.QMARK_PIPE,
      '#-': TokenType.HASH_DASH,
      '|/': TokenType.PIPE_SLASH,
      '||/': TokenType.DPIPE_SLASH,
      'BEGIN': TokenType.BEGIN,
      'BIGSERIAL': TokenType.BIGSERIAL,
      'CONSTRAINT TRIGGER': TokenType.COMMAND,
      'CSTRING': TokenType.PSEUDO_TYPE,
      'DECLARE': TokenType.COMMAND,
      'DO': TokenType.COMMAND,
      'EXEC': TokenType.COMMAND,
      'HSTORE': TokenType.HSTORE,
      'INT8': TokenType.BIGINT,
      'MONEY': TokenType.MONEY,
      'NAME': TokenType.NAME,
      'OID': TokenType.OBJECT_IDENTIFIER,
      'ONLY': TokenType.ONLY,
      'POINT': TokenType.POINT,
      'REFRESH': TokenType.COMMAND,
      'REINDEX': TokenType.COMMAND,
      'RESET': TokenType.COMMAND,
      'SERIAL': TokenType.SERIAL,
      'SMALLSERIAL': TokenType.SMALLSERIAL,
      'TEMP': TokenType.TEMPORARY,
      'REGCLASS': TokenType.OBJECT_IDENTIFIER,
      'REGCOLLATION': TokenType.OBJECT_IDENTIFIER,
      'REGCONFIG': TokenType.OBJECT_IDENTIFIER,
      'REGDICTIONARY': TokenType.OBJECT_IDENTIFIER,
      'REGNAMESPACE': TokenType.OBJECT_IDENTIFIER,
      'REGOPER': TokenType.OBJECT_IDENTIFIER,
      'REGOPERATOR': TokenType.OBJECT_IDENTIFIER,
      'REGPROC': TokenType.OBJECT_IDENTIFIER,
      'REGPROCEDURE': TokenType.OBJECT_IDENTIFIER,
      'REGROLE': TokenType.OBJECT_IDENTIFIER,
      'REGTYPE': TokenType.OBJECT_IDENTIFIER,
      'FLOAT': TokenType.DOUBLE,
      'XML': TokenType.XML,
      'VARIADIC': TokenType.VARIADIC,
      'INOUT': TokenType.INOUT,
    };
  })();

  static SINGLE_TOKENS: Record<string, TokenType> = {
    ...Tokenizer.SINGLE_TOKENS,
    $: TokenType.HEREDOC_STRING,
  };

  static VAR_SINGLE_TOKENS = new Set(['$']);
}

class PostgresParser extends Parser {
  static SUPPORTS_OMITTED_INTERVAL_SPAN_UNIT = true;

  @cache
  static get PROPERTY_PARSERS (): Record<string, (self: Parser, ...args: unknown[]) => Expression | Expression[] | undefined> {
    return (() => {
      const parsers: Record<string, (self: Parser, ...args: unknown[]) => Expression | Expression[] | undefined> = {
        ...Parser.PROPERTY_PARSERS,
        SET: (self: Parser) => self.expression(SetConfigPropertyExpr, { this: (self as PostgresParser).parseSet() }),
      };
      delete parsers['INPUT'];
      return parsers;
    })();
  }

  @cache
  static get PLACEHOLDER_PARSERS (): Partial<Record<TokenType, (self: Parser) => Expression | undefined>> {
    return {
      ...Parser.PLACEHOLDER_PARSERS,
      [TokenType.PLACEHOLDER]: (self: Parser) => self.expression(PlaceholderExpr, { jdbc: true }),
      [TokenType.MOD]: (self: Parser) => (self as PostgresParser).parseQueryParameter(),
    };
  }

  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> {
    return {
      ...Parser.FUNCTIONS,
      ARRAY_PREPEND: (args: Expression[]) => new ArrayPrependExpr({
        this: seqGet(args, 1),
        expression: seqGet(args, 0),
      }),
      BIT_AND: BitwiseAndAggExpr.fromArgList,
      BIT_OR: BitwiseOrAggExpr.fromArgList,
      BIT_XOR: BitwiseXorAggExpr.fromArgList,
      VERSION: CurrentVersionExpr.fromArgList,
      DATE_TRUNC: buildTimestampTrunc,
      DIV: (args: Expression[]) => new CastExpr({
        this: binaryFromFunction(IntDivExpr)(args),
        to: DataTypeExpr.build('decimal'),
      }),
      GENERATE_SERIES: buildGenerateSeries,
      GET_BIT: (args: Expression[]) => new GetbitExpr({
        this: seqGet(args, 0),
        expression: seqGet(args, 1),
        zeroIsMsb: true,
      }),
      JSON_EXTRACT_PATH: buildJsonExtractPath(JsonExtractExpr),
      JSON_EXTRACT_PATH_TEXT: buildJsonExtractPath(JsonExtractScalarExpr),
      LENGTH: (args: Expression[]) => new LengthExpr({
        this: seqGet(args, 0),
        encoding: seqGet(args, 1),
      }),
      MAKE_TIME: TimeFromPartsExpr.fromArgList,
      MAKE_TIMESTAMP: TimestampFromPartsExpr.fromArgList,
      NOW: CurrentTimestampExpr.fromArgList,
      REGEXP_REPLACE: buildRegexpReplace,
      TO_CHAR: buildFormattedTime(TimeToStrExpr, { dialect: 'postgres' }),
      TO_DATE: buildFormattedTime(StrToDateExpr, { dialect: 'postgres' }),
      TO_TIMESTAMP: buildToTimestamp,
      UNNEST: ExplodeExpr.fromArgList,
      SHA256: (args: Expression[]) => new Sha2Expr({
        this: seqGet(args, 0),
        length: LiteralExpr.number(256),
      }),
      SHA384: (args: Expression[]) => new Sha2Expr({
        this: seqGet(args, 0),
        length: LiteralExpr.number(384),
      }),
      SHA512: (args: Expression[]) => new Sha2Expr({
        this: seqGet(args, 0),
        length: LiteralExpr.number(512),
      }),
      LEVENSHTEIN_LESS_EQUAL: buildLevenshteinLessEqual,
      JSON_OBJECT_AGG: JsonObjectAggExpr.fromArgList,
      JSONB_OBJECT_AGG: JsonbObjectAggExpr.fromArgList,
      WIDTH_BUCKET: (args: Expression[]) => args.length === 2
        ? new WidthBucketExpr({
          this: seqGet(args, 0),
          threshold: seqGet(args, 1),
        })
        : WidthBucketExpr.fromArgList(args),
    };
  }

  @cache
  static get NO_PAREN_FUNCTION_PARSERS (): Partial<Record<string, (self: Parser) => Expression | undefined>> {
    return {
      ...Parser.NO_PAREN_FUNCTION_PARSERS,
      VARIADIC: (self: Parser) => self.expression(VariadicExpr, { this: (self as PostgresParser).parseBitwise() }),
    };
  }

  @cache
  static get NO_PAREN_FUNCTIONS (): Partial<Record<TokenType, typeof Expression>> {
    return {
      ...Parser.NO_PAREN_FUNCTIONS,
      [TokenType.CURRENT_SCHEMA]: CurrentSchemaExpr,
    };
  }

  @cache
  static get FUNCTION_PARSERS (): Partial<Record<string, (self: Parser) => Expression | undefined>> {
    return {
      ...Parser.FUNCTION_PARSERS,
      DATE_PART: (self: Parser) => (self as PostgresParser).parseDatePart(),
      JSON_AGG: (self: Parser) => self.expression(JsonArrayAggExpr, {
        this: (self as PostgresParser).parseLambda(),
        order: (self as PostgresParser).parseOrder(),
      }),
      JSONB_EXISTS: (self: Parser) => (self as PostgresParser).parseJsonbExists(),
    };
  }

  @cache
  static get BITWISE (): Partial<Record<TokenType, typeof Expression>> {
    return {
      ...Parser.BITWISE,
      [TokenType.HASH]: BitwiseXorExpr,
    };
  }

  static EXPONENT = {
    [TokenType.CARET]: PowExpr,
  };

  @cache
  static get RANGE_PARSERS (): Partial<Record<TokenType, (self: Parser, this_: Expression) => Expression | undefined>> {
    return {
      ...Parser.RANGE_PARSERS,
      [TokenType.DAMP]: binaryRangeParser(ArrayOverlapsExpr),
      [TokenType.DAT]: (self: Parser, thisNode: Expression) => self.expression(MatchAgainstExpr, {
        this: (self as PostgresParser).parseBitwise(),
        expressions: [thisNode],
      }),
    };
  }

  @cache
  static get STATEMENT_PARSERS (): Partial<Record<TokenType, (self: Parser) => Expression | undefined>> {
    return {
      ...Parser.STATEMENT_PARSERS,
      [TokenType.END]: (self: Parser) => (self as PostgresParser).parseCommitOrRollback(),
    };
  }

  @cache
  static get UNARY_PARSERS (): Partial<Record<TokenType, (self: Parser) => Expression | undefined>> {
    return {
      ...Parser.UNARY_PARSERS,
      [TokenType.RLIKE]: (self: Parser) => self.expression(BitwiseNotExpr, {
        this: (self as PostgresParser).parseUnary(),
      }),
    };
  }

  static JSON_ARROWS_REQUIRE_JSON_TYPE = true;
  @cache
  static get COLUMN_OPERATORS (): Partial<Record<TokenType, undefined | ((self: Parser, this_?: Expression, to?: Expression) => Expression)>> {
    return {
      ...Parser.COLUMN_OPERATORS,
      [TokenType.ARROW]: (self: Parser, thisNode?: Expression, path?: Expression) => self.validateExpression(
        buildJsonExtractPath(JsonExtractExpr, { arrowReqJsonType: (self.constructor as typeof PostgresParser).JSON_ARROWS_REQUIRE_JSON_TYPE })([thisNode, path]),
      ),
      [TokenType.DARROW]: (self: Parser, thisNode?: Expression, path?: Expression) => self.validateExpression(
        buildJsonExtractPath(JsonExtractScalarExpr, { arrowReqJsonType: (self.constructor as typeof PostgresParser).JSON_ARROWS_REQUIRE_JSON_TYPE })([thisNode, path]),
      ),
    };
  }

  static ARG_MODE_TOKENS = new Set([
    TokenType.IN,
    TokenType.OUT,
    TokenType.INOUT,
    TokenType.VARIADIC,
  ]);

  parseParameterMode (): TokenType | undefined {
    if (!this.matchSet((this.constructor as typeof PostgresParser).ARG_MODE_TOKENS, { advance: false }) || !this.next) {
      return undefined;
    }

    const modeToken = this.curr;

    // Check Pattern 1: MODE TYPE
    const isFollowedByBuiltinType = this.tryParse(
      () => {
        this.advance();
        return this.parseTypes({
          checkFunc: false,
          allowIdentifiers: false,
        });
      },
      { retreat: true },
    );

    if (isFollowedByBuiltinType) {
      return undefined;
    }

    // Check Pattern 2: MODE NAME TYPE
    if (!this.next || !(this.constructor as typeof PostgresParser).ID_VAR_TOKENS.has(this.next.tokenType)) {
      return undefined;
    }

    const isFollowedByAnyType = this.tryParse(
      () => {
        this.advance(2);
        return this.parseTypes({
          checkFunc: false,
          allowIdentifiers: true,
        });
      },
      { retreat: true },
    );

    if (isFollowedByAnyType) {
      return modeToken?.tokenType;
    }

    return undefined;
  }

  createModeConstraint (paramMode: TokenType): InOutColumnConstraintExpr {
    return this.expression(InOutColumnConstraintExpr, {
      input: paramMode === TokenType.IN || paramMode === TokenType.INOUT,
      output: paramMode === TokenType.OUT || paramMode === TokenType.INOUT,
      variadic: paramMode === TokenType.VARIADIC,
    });
  }

  parseFunctionParameter (): Expression | undefined {
    const paramMode = this.parseParameterMode();

    if (paramMode) {
      this.advance();
    }

    const paramName = this.parseIdVar();
    const columnDef = this.parseColumnDef(paramName, { computedColumn: false });

    if (paramMode && columnDef instanceof ColumnDefExpr) {
      const constraint = this.createModeConstraint(paramMode);
      if (!columnDef.args.constraints) {
        columnDef.setArgKey('constraints', []);
      }
      columnDef.args.constraints?.unshift(new ColumnConstraintExpr({ kind: constraint }));
    }

    return columnDef;
  }

  parseQueryParameter (): Expression | undefined {
    const thisNode = this.match(TokenType.L_PAREN, { advance: false })
      ? this.parseWrapped(() => this.parseIdVar())
      : undefined;

    this.matchTextSeq('S');
    return this.expression(PlaceholderExpr, { this: thisNode });
  }

  parseDatePart (): Expression {
    let part = this.parseType();
    this.match(TokenType.COMMA);
    const value = this.parseBitwise();

    if (part && (part instanceof ColumnExpr || part instanceof LiteralExpr)) {
      part = new IdentifierExpr({ this: part.name });
    }

    return this.expression(ExtractExpr, {
      this: part,
      expression: value,
    });
  }

  parseUniqueKey (): Expression | undefined {
    return undefined;
  }

  parseJsonbExists (): JsonbExistsExpr {
    return this.expression(JsonbExistsExpr, {
      this: this.parseBitwise(),
      path: this.match(TokenType.COMMA) && this.dialect.toJsonPath(this.parseBitwise()),
    });
  }

  parseGeneratedAsIdentity (): GeneratedAsIdentityColumnConstraintExpr | ComputedColumnConstraintExpr | GeneratedAsRowColumnConstraintExpr {
    let thisNode = super.parseGeneratedAsIdentity();

    if (this.matchTextSeq('STORED')) {
      thisNode = this.expression(ComputedColumnConstraintExpr, { this: thisNode.args.expression });
    }

    return thisNode;
  }

  parseUserDefinedType (identifier: IdentifierExpr): Expression | undefined {
    let udtType: IdentifierExpr | DotExpr = identifier;

    while (this.match(TokenType.DOT)) {
      const part = this.parseIdVar();
      if (part) {
        udtType = new DotExpr({
          this: udtType,
          expression: part,
        });
      }
    }

    return DataTypeExpr.build(udtType, { udt: true });
  }
}

class PostgresGenerator extends Generator {
  static SINGLE_STRING_INTERVAL = true;
  static RENAME_TABLE_WITH_DB = false;
  static LOCKING_READS_SUPPORTED = true;
  static JOIN_HINTS = false;
  static TABLE_HINTS = false;
  static QUERY_HINTS = false;
  static NVL2_SUPPORTED = false;
  static PARAMETER_TOKEN = '$';
  static NAMED_PLACEHOLDER_TOKEN = '%';
  static TABLESAMPLE_SIZE_IS_ROWS = false;
  static TABLESAMPLE_SEED_KEYWORD = 'REPEATABLE';
  static SUPPORTS_SELECT_INTO = true;
  static JSON_TYPE_REQUIRED_FOR_EXTRACTION = true;
  static SUPPORTS_UNLOGGED_TABLES = true;
  static LIKE_PROPERTY_INSIDE_SCHEMA = true;
  static MULTI_ARG_DISTINCT = false;
  static CAN_IMPLEMENT_ARRAY_ANY = true;
  static SUPPORTS_WINDOW_EXCLUDE = true;
  static COPY_HAS_INTO_KEYWORD = false;
  static ARRAY_CONCAT_IS_VAR_LEN = false;
  static SUPPORTS_MEDIAN = false;
  static ARRAY_SIZE_DIM_REQUIRED = true;
  static SUPPORTS_BETWEEN_FLAGS = true;
  static INOUT_SEPARATOR = '';

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
    return {
      ...Generator.TYPE_MAPPING,
      [DataTypeExprKind.TINYINT]: 'SMALLINT',
      [DataTypeExprKind.FLOAT]: 'REAL',
      [DataTypeExprKind.DOUBLE]: 'DOUBLE PRECISION',
      [DataTypeExprKind.BINARY]: 'BYTEA',
      [DataTypeExprKind.VARBINARY]: 'BYTEA',
      [DataTypeExprKind.ROWVERSION]: 'BYTEA',
      [DataTypeExprKind.DATETIME]: 'TIMESTAMP',
      [DataTypeExprKind.TIMESTAMPNTZ]: 'TIMESTAMP',
      [DataTypeExprKind.BLOB]: 'BYTEA',
    };
  }

  lateralSql (expression: LateralExpr): string {
    let sql = super.lateralSql(expression);

    if (expression.args.crossApply !== undefined) {
      sql = `${sql} ON TRUE`;
    }

    return sql;
  }

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (self: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (self: Generator, e: any) => string>([
      ...Generator.TRANSFORMS,
      [AnyValueExpr, versionedAnyValueSql],
      [ArrayConcatExpr, arrayConcatSql('ARRAY_CAT')],
      [ArrayFilterExpr, filterArrayUsingUnnest],
      [ArrayAppendExpr, arrayAppendSql('ARRAY_APPEND')],
      [ArrayPrependExpr, arrayAppendSql('ARRAY_PREPEND', { swapParams: true })],
      [BitwiseAndAggExpr, renameFunc('BIT_AND')],
      [BitwiseOrAggExpr, renameFunc('BIT_OR')],
      [BitwiseXorExpr, (self: Generator, e: BitwiseXorExpr) => self.binary(e, '#')],
      [BitwiseXorAggExpr, renameFunc('BIT_XOR')],
      [ColumnDefExpr, preprocess([autoIncrementToSerial, serialToGenerated])],
      [CurrentDateExpr, noParenCurrentDateSql],
      [CurrentTimestampExpr, () => 'CURRENT_TIMESTAMP'],
      [CurrentUserExpr, () => 'CURRENT_USER'],
      [CurrentVersionExpr, renameFunc('VERSION')],
      [DateAddExpr, dateAddSql('+')],
      [DateDiffExpr, dateDiffSql],
      [DateStrToDateExpr, dateStrToDateSql],
      [DateSubExpr, dateAddSql('-')],
      [ExplodeExpr, renameFunc('UNNEST')],
      [ExplodingGenerateSeriesExpr, renameFunc('GENERATE_SERIES')],
      [GetbitExpr, getBitSql],
      [
        GroupConcatExpr,
        (self: Generator, e: GroupConcatExpr) =>
          groupConcatSql(self, e, {
            funcName: 'STRING_AGG',
            withinGroup: false,
          }),
      ],
      [IntDivExpr, renameFunc('DIV')],
      [
        JsonArrayAggExpr,
        (self: Generator, e: JsonArrayAggExpr) => {
          const thisSql = self.sql(e, 'this');
          const orderSql = self.sql(e, 'order');
          const inner = orderSql ? `${thisSql} ${orderSql}` : thisSql;
          return `JSON_AGG(${inner})`;
        },
      ],
      [JsonExtractExpr, jsonExtractSql('JSON_EXTRACT_PATH', '->')],
      [JsonExtractScalarExpr, jsonExtractSql('JSON_EXTRACT_PATH_TEXT', '->>')],
      [JsonbExtractExpr, (self: Generator, e: JsonbExtractExpr) => self.binary(e, '#>')],
      [JsonbExtractScalarExpr, (self: Generator, e: JsonbExtractScalarExpr) => self.binary(e, '#>>')],
      [JsonbContainsExpr, (self: Generator, e: JsonbContainsExpr) => self.binary(e, '?')],
      [
        ParseJsonExpr,
        (self: Generator, e: ParseJsonExpr) =>
          self.sql(new CastExpr({
            this: e.args.this,
            to: DataTypeExpr.build('json'),
          })),
      ],
      [JsonPathKeyExpr, jsonPathKeyOnlyName],
      [JsonPathRootExpr, () => ''],
      [
        JsonPathSubscriptExpr,
        (self: Generator, e: JsonPathSubscriptExpr) => {
          const thisVal = e.args.this;
          const part = typeof thisVal === 'number' || typeof thisVal === 'boolean' ? String(thisVal) : (thisVal instanceof Expression ? thisVal : (thisVal ?? ''));
          return self.jsonPathPart(part);
        },
      ],
      [LastDayExpr, noLastDaySql],
      [LogicalOrExpr, renameFunc('BOOL_OR')],
      [LogicalAndExpr, renameFunc('BOOL_AND')],
      [MaxExpr, maxOrGreatest],
      [MapFromEntriesExpr, noMapFromEntriesSql],
      [MinExpr, minOrLeast],
      [MergeExpr, mergeWithoutTargetSql],
      [PartitionedByPropertyExpr, (self: Generator, e: PartitionedByPropertyExpr) => `PARTITION BY ${self.sql(e, 'this')}`],
      [PercentileContExpr, preprocess([addWithinGroupForPercentiles])],
      [PercentileDiscExpr, preprocess([addWithinGroupForPercentiles])],
      [PivotExpr, noPivotSql],
      [RandExpr, renameFunc('RANDOM')],
      [RegexpLikeExpr, (self: Generator, e: RegexpLikeExpr) => self.binary(e, '~')],
      [RegexpILikeExpr, (self: Generator, e: RegexpILikeExpr) => self.binary(e, '~*')],
      [
        RegexpReplaceExpr,
        (self: Generator, e: RegexpReplaceExpr) =>
          self.func('REGEXP_REPLACE', [
            e.args.this,
            e.args.expression,
            e.args.replacement,
            e.args.position,
            e.args.occurrence,
            regexpReplaceGlobalModifier(e),
          ]),
      ],
      [RoundExpr, roundSql],
      [SelectExpr, preprocess([eliminateSemiAndAntiJoins, eliminateQualify])],
      [Sha2Expr, sha256Sql],
      [Sha2DigestExpr, sha2DigestSql],
      [StrPositionExpr, (self: Generator, e: StrPositionExpr) => strPositionSql(self, e, { funcName: 'POSITION' })],
      [StrToDateExpr, (self: Generator, e: StrToDateExpr) => self.func('TO_DATE', [e.args.this, self.formatTime(e)])],
      [StrToTimeExpr, (self: Generator, e: StrToTimeExpr) => self.func('TO_TIMESTAMP', [e.args.this, self.formatTime(e)])],
      [StructExtractExpr, structExtractSql],
      [SubstringExpr, substringSql],
      [TimeFromPartsExpr, renameFunc('MAKE_TIME')],
      [TimestampFromPartsExpr, renameFunc('MAKE_TIMESTAMP')],
      [TimestampTruncExpr, timestampTruncSql({ zone: true })],
      [TimeStrToTimeExpr, timeStrToTimeSql],
      [TimeToStrExpr, (self: Generator, e: TimeToStrExpr) => self.func('TO_CHAR', [e.args.this, self.formatTime(e)])],
      [
        ToCharExpr,
        (self: Generator, e: ToCharExpr) =>
          e.args.format ? self.functionFallbackSql(e) : (self as PostgresGenerator).toCharSql(e),
      ],
      [TrimExpr, trimSql],
      [TryCastExpr, noTrycastSql],
      [TsOrDsAddExpr, dateAddSql('+')],
      [TsOrDsDiffExpr, dateDiffSql],
      [UnixToTimeExpr, unixToTimeSql],
      [UuidExpr, () => 'GEN_RANDOM_UUID()'],
      [
        TimeToUnixExpr,
        (self: Generator, e: TimeToUnixExpr) =>
          self.func('DATE_PART', [LiteralExpr.string('epoch'), e.args.this]),
      ],
      [VariancePopExpr, renameFunc('VAR_POP')],
      [VarianceExpr, renameFunc('VAR_SAMP')],
      [XorExpr, boolXorSql],
      [UnicodeExpr, renameFunc('ASCII')],
      [LevenshteinExpr, levenshteinSql],
      [JsonObjectAggExpr, renameFunc('JSON_OBJECT_AGG')],
      [JsonbObjectAggExpr, renameFunc('JSONB_OBJECT_AGG')],
      [CountIfExpr, countIfToSum],
    ]);
    transforms.delete(CommentColumnConstraintExpr);
    return transforms;
  }

  @cache
  static get PROPERTIES_LOCATION () {
    return {
      ...Generator.PROPERTIES_LOCATION,
      [PartitionedByPropertyExpr.constructor.name]: PropertiesLocation.POST_SCHEMA,
      [TransientPropertyExpr.constructor.name]: PropertiesLocation.UNSUPPORTED,
      [VolatilePropertyExpr.constructor.name]: PropertiesLocation.UNSUPPORTED,
    };
  }

  schemaCommentPropertySql (_expression: SchemaCommentPropertyExpr): string {
    this.unsupported('Table comments are not supported in the CREATE statement');
    return '';
  }

  commentColumnConstraintSql (_expression: CommentColumnConstraintExpr): string {
    this.unsupported('Column comments are not supported in the CREATE statement');
    return '';
  }

  columnDefSql (expression: ColumnDefExpr, options: { sep?: string } = {}): string {
    const { sep = ' ' } = options;
    const paramConstraint = expression.find(InOutColumnConstraintExpr);

    if (paramConstraint) {
      const modeSql = this.sql(paramConstraint);
      paramConstraint.pop();
      const baseSql = super.columnDefSql(expression, { sep });
      return `${modeSql} ${baseSql}`;
    }

    return super.columnDefSql(expression, { sep });
  }

  unnestSql (expression: UnnestExpr): string {
    if (expression.args.expressions?.length === 1) {
      const arg = expression.args.expressions[0];
      if (arg instanceof GenerateDateArrayExpr) {
        let generateSeries: Expression = new GenerateSeriesExpr({ ...arg.args });
        if (expression.parent instanceof FromExpr || expression.parent instanceof JoinExpr) {
          generateSeries = select('value::date')
            .from(new TableExpr({ this: generateSeries }).as('_t', { table: ['value'] }))
            .subquery(expression.args.alias instanceof Expression ? expression.args.alias : (typeof expression.args.alias === 'string' ? expression.args.alias : '_unnested_generate_series'));
        }
        return this.sql(generateSeries);
      }

      const thisNode = arg instanceof Expression ? annotateTypes(arg, { dialect: this.dialect }) : arg;
      if (isType(thisNode, 'array<json>')) {
        let current = thisNode;
        while (current instanceof CastExpr) {
          const inner = current.args.this;
          if (inner instanceof Expression) {
            current = inner;
          } else {
            break;
          }
        }

        const argAsJson = this.sql(new CastExpr({
          this: current,
          to: DataTypeExpr.build('json'),
        }));
        let alias = this.sql(expression, 'alias');
        alias = alias ? ` AS ${alias}` : '';

        if (expression.args.offset) {
          this.unsupported('Unsupported JSON_ARRAY_ELEMENTS with offset');
        }

        return `JSON_ARRAY_ELEMENTS(${argAsJson})${alias}`;
      }
    }

    return super.unnestSql(expression);
  }

  bracketSql (expression: BracketExpr): string {
    if (expression.args.this instanceof ArrayExpr) {
      expression.setArgKey('this', new ParenExpr({ this: expression.args.this }));
    }

    return super.bracketSql(expression);
  }

  matchAgainstSql (expression: MatchAgainstExpr): string {
    const thisSql = this.sql(expression, 'this');
    const expressions = (expression.args.expressions ?? []).map((e) => `${this.sql(e)} @@ ${thisSql}`);
    const sql = expressions.join(' OR ');
    return 1 < expressions.length ? `(${sql})` : sql;
  }

  alterSetSql (expression: AlterSetExpr): string {
    let exprs = this.expressions(expression, { flat: true });
    exprs = exprs ? `(${exprs})` : '';

    let accessMethod = this.sql(expression, 'accessMethod');
    accessMethod = accessMethod ? `ACCESS METHOD ${accessMethod}` : '';
    let tablespace = this.sql(expression, 'tablespace');
    tablespace = tablespace ? `TABLESPACE ${tablespace}` : '';
    const option = this.sql(expression, 'option');

    return `SET ${exprs}${accessMethod}${tablespace}${option}`;
  }

  dataTypeSql (expression: DataTypeExpr): string {
    if (expression.isType(DataTypeExprKind.ARRAY)) {
      if (expression.args.expressions && 0 < expression.args.expressions.length) {
        const values = this.expressions(expression, {
          key: 'values',
          flat: true,
        });
        return `${this.expressions(expression, { flat: true })}[${values}]`;
      }
      return 'ARRAY';
    }

    if (
      expression.isType([DataTypeExprKind.DOUBLE, DataTypeExprKind.FLOAT])
      && expression.args.expressions
      && 0 < expression.args.expressions.length
    ) {
      return `FLOAT(${this.expressions(expression, { flat: true })})`;
    }

    return super.dataTypeSql(expression);
  }

  castSql (expression: CastExpr, options: { safePrefix?: string } = {}): string {
    const { safePrefix } = options;
    const thisNode = expression.args.this;

    if (thisNode instanceof IntDivExpr && expression.args.to instanceof DataTypeExpr && expression.args.to.isType(DataTypeExprKind.DECIMAL)) {
      return this.sql(thisNode);
    }

    return super.castSql(expression, { safePrefix });
  }

  arraySql (expression: ArrayExpr): string {
    const exprs = expression.args.expressions || [];
    const funcName = this.normalizeFunc('ARRAY');

    if (exprs[0] instanceof SelectExpr) {
      return `${funcName}(${this.sql(exprs[0])})`;
    }

    return `${funcName}${inlineArraySql(this, expression)}`;
  }

  computedColumnConstraintSql (expression: ComputedColumnConstraintExpr): string {
    return `GENERATED ALWAYS AS (${this.sql(expression, 'this')}) STORED`;
  }

  isAsciiSql (expression: IsAsciiExpr): string {
    return `(${this.sql(expression.args.this)} ~ '^[[:ascii:]]*$')`;
  }

  ignoreNullsSql (expression: IgnoreNullsExpr): string {
    this.unsupported('PostgreSQL does not support IGNORE NULLS.');
    return this.sql(expression.args.this);
  }

  respectNullsSql (expression: RespectNullsExpr): string {
    this.unsupported('PostgreSQL does not support RESPECT NULLS.');
    return this.sql(expression.args.this);
  }

  currentSchemaSql (expression: CurrentSchemaExpr): string {
    if (expression.args.this) {
      this.unsupported('Unsupported arg \'this\' for CURRENT_SCHEMA');
    }
    return 'CURRENT_SCHEMA';
  }

  intervalSql (expression: IntervalExpr): string {
    const unit = expression.text('unit').toLowerCase();
    const thisNode = expression.args.this;

    if (unit.startsWith('quarter') && thisNode instanceof LiteralExpr) {
      thisNode.setArgKey('this', (parseInt(thisNode.args.this ?? '0') * 3).toString());
      expression.args.unit?.replace(new IdentifierExpr({ this: 'MONTH' }));
    }

    return super.intervalSql(expression);
  }

  placeholderSql (expression: PlaceholderExpr): string {
    if (expression.args.jdbc) {
      return '?';
    }

    const thisPart = expression.args.this ? `(${expression.name})` : '';
    return `${(this.constructor as typeof PostgresGenerator).NAMED_PLACEHOLDER_TOKEN}${thisPart}s`;
  }

  arrayContainsSql (expression: ArrayContainsExpr): string {
    const value = expression.args.expression;
    const array = expression.args.this;

    if (!value) return '';

    const coalesceExpr = new CoalesceExpr({
      this: value.eq(new AnyExpr({ this: new ParenExpr({ this: array }) })),
      expressions: [new BooleanExpr({ this: false })],
    });

    const caseExpr = new CaseExpr({ ifs: [] })
      .when(new IsExpr({
        this: value,
        expression: null_(),
      }), null_())
      .else(coalesceExpr);

    return this.sql(caseExpr);
  }
}

export class Postgres extends Dialect {
  static INDEX_OFFSET = 1;
  static TYPED_DIVISION = true;
  static CONCAT_COALESCE = true;
  static NULL_ORDERING = 'nulls_are_large' as const;
  static TIME_FORMAT = '\'YYYY-MM-DD HH24:MI:SS\'';
  static TABLESAMPLE_SIZE_IS_PERCENT = true;
  static TABLES_REFERENCEABLE_AS_COLUMNS = true;

  static DEFAULT_FUNCTIONS_COLUMN_NAMES = new Map([[ExplodingGenerateSeriesExpr.name, 'generate_series']]);

  static TIME_MAPPING = {
    d: '%u',
    D: '%u',
    dd: '%d',
    DD: '%d',
    ddd: '%j',
    DDD: '%j',
    FMDD: '%-d',
    FMDDD: '%-j',
    FMHH12: '%-I',
    FMHH24: '%-H',
    FMMI: '%-M',
    FMMM: '%-m',
    FMSS: '%-S',
    HH12: '%I',
    HH24: '%H',
    mi: '%M',
    MI: '%M',
    mm: '%m',
    MM: '%m',
    OF: '%z',
    ss: '%S',
    SS: '%S',
    TMDay: '%A',
    TMDy: '%a',
    TMMon: '%b',
    TMMonth: '%B',
    TZ: '%Z',
    US: '%f',
    ww: '%U',
    WW: '%U',
    yy: '%y',
    YY: '%y',
    yyyy: '%Y',
    YYYY: '%Y',
  };

  static Tokenizer = PostgresTokenizer;
  static Parser = PostgresParser;
  static Generator = PostgresGenerator;
}

Dialect.register(Dialects.POSTGRES, Postgres);
