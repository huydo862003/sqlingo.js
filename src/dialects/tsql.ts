import {
  Generator,
} from '../generator';
import {
  Parser, buildExtractJsonWithPath, buildCoalesce,
} from '../parser';
import {
  Tokenizer, TokenType,
} from '../tokens';
import type {
  DropExpr,
  CoalesceExpr,
  AlterSetExpr,
  SetItemExpr,
  LateralExpr,
  ConstraintExpr,
  VersionExpr,
  AlterExpr,
  CommandExpr,
  ReturningExpr,
  DPipeExpr,
  IsAsciiExpr,
  ExpressionValue,
  ColumnDefExpr,
} from '../expressions';
import {
  Expression,
  DataTypeExpr,
  DataTypeExprKind,
  CreateExpr,
  CreateExprKind,
  SchemaExpr,
  SelectExpr,
  TableExpr,
  IntoExpr,
  LiteralExpr,
  ColumnExpr,
  alias,
  var_,
  cast,
  func,
  Atan2Expr,
  ArrayToStringExpr,
  AutoIncrementColumnConstraintExpr,
  CeilExpr,
  ChrExpr,
  DateAddExpr,
  DateDiffExpr,
  DateStrToDateExpr,
  GeneratedAsIdentityColumnConstraintExpr,
  GroupConcatExpr,
  IfExpr,
  JsonExtractExpr,
  JsonExtractScalarExpr,
  LastDayExpr,
  LnExpr,
  MaxExpr,
  Md5Expr,
  MinExpr,
  NumberToStrExpr,
  RepeatExpr,
  CurrentSchemaExpr,
  ShaExpr,
  Sha1DigestExpr,
  Sha2Expr,
  StddevExpr,
  StrPositionExpr,
  TemporaryPropertyExpr,
  TimeStrToTimeExpr,
  TimeToStrExpr,
  TrimExpr,
  TsOrDsAddExpr,
  TsOrDsDiffExpr,
  TsOrDsToDateExpr,
  TimestampTruncExpr,
  TruncExpr,
  UuidExpr,
  DateFromPartsExpr,
  AnyValueExpr,
  CurrentTimestampExpr,
  CurrentTimestampLtzExpr,
  CurrentUserExpr,
  CurrentDateExpr,
  CountExpr,
  SplitPartExpr,
  TimestampFromPartsExpr,
  TimeFromPartsExpr,
  ScopeResolutionExpr,
  CastExpr,
  EqExpr,
  AliasExpr,
  CommitExpr,
  RollbackExpr,
  TransactionExpr,
  DateExpr,
  TimeExpr,
  TimestampExpr,
  ExtractExpr,
  PropertiesExpr,
  ReturnsPropertyExpr,
  ConvertExpr,
  QueryOptionExpr,
  XmlKeyValueOptionExpr,
  JsonArrayAggExpr,
  LeftExpr,
  RightExpr,
  LengthExpr,
  PowExpr,
  UserDefinedFunctionExpr,
  IdentifierExpr,
  VarExpr,
  NullExpr,
  BooleanExpr,
  IsExpr,
  ParameterExpr,
  LikePropertyExpr,
  VolatilePropertyExpr,
  PropertiesLocation,
  CteExpr,
  JsonPathKeyExpr,
  JsonPathRootExpr,
  JsonPathSubscriptExpr,
  DeleteExpr,
  InsertExpr,
  IntersectExpr,
  ExceptExpr,
  MergeExpr,
  SubqueryExpr,
  UnionExpr,
  UpdateExpr,
  FetchExpr,
  LimitExpr,
  OffsetExpr,
  ValuesExpr,
  AlterRenameExpr,
  PartitionExpr,
  PartitionRangeExpr,
  DeclareItemExpr,
  UniqueColumnConstraintExpr,
  OrderExpr,
  DistinctExpr,
  NeqExpr,
  InExpr,
  AddExpr,
  DropExprKind,
} from '../expressions';
import {
  ensureIterable, seqGet,
} from '../helper';
import {
  eliminateDistinctOn, eliminateQualify, eliminateSemiAndAntiJoins, preprocess, unnestGenerateDateArrayUsingRecursiveCte,
} from '../transforms';
import { EXPRESSION_METADATA } from '../typing';
import { narrowInstanceOf } from '../port_internals';
import {
  buildDateDelta,
  dateDeltaSql,
  dateStrToDateSql,
  generatedAsIdentityColumnConstraintSql,
  anyValueToMaxSql,
  maxOrGreatest,
  minOrLeast,
  strPositionSql,
  timeStrToTimeSql,
  trimSql,
  mapDatePart,

  Dialect, NormalizationStrategy, Dialects,
  renameFunc,
} from './dialect';

const FULL_FORMAT_TIME_MAPPING: Record<string, string> = {
  weekday: '%A',
  dw: '%A',
  w: '%A',
  month: '%B',
  mm: '%B',
  m: '%B',
};

const DATE_DELTA_INTERVAL: Record<string, string> = {
  year: 'year',
  yyyy: 'year',
  yy: 'year',
  quarter: 'quarter',
  qq: 'quarter',
  q: 'quarter',
  month: 'month',
  mm: 'month',
  m: 'month',
  week: 'week',
  ww: 'week',
  wk: 'week',
  day: 'day',
  dd: 'day',
  d: 'day',
};

const DATE_PART_UNMAPPING: Record<string, string> = {
  WEEKISO: 'ISO_WEEK',
  DAYOFWEEK: 'WEEKDAY',
  TIMEZONE_MINUTE: 'TZOFFSET',
};

const TRANSPILE_SAFE_NUMBER_FMT = new Set(['N', 'C']);

const DATE_FMT_RE = /([dD]{1,2})|([mM]{1,2})|([yY]{1,4})|([hH]{1,2})|([sS]{1,2})/;

const OPTIONS_THAT_REQUIRE_EQUAL = new Set([
  'MAX_GRANT_PERCENT',
  'MIN_GRANT_PERCENT',
  'LABEL',
]);

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const BIT_TYPES: Set<any> = new Set([
  EqExpr,
  NeqExpr,
  IsExpr,
  InExpr,
  SelectExpr,
  AliasExpr,
]);

const OPTIONS: Record<string, (string | string[])[] | null> = {
  DISABLE_OPTIMIZED_PLAN_FORCING: [],
  FAST: [],
  IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX: [],
  LABEL: [],
  MAXDOP: [],
  MAXRECURSION: [],
  MAX_GRANT_PERCENT: [],
  MIN_GRANT_PERCENT: [],
  NO_PERFORMANCE_SPOOL: [],
  QUERYTRACEON: [],
  RECOMPILE: [],
  CONCAT: ['UNION'],
  DISABLE: ['EXTERNALPUSHDOWN', 'SCALEOUTEXECUTION'],
  EXPAND: ['VIEWS'],
  FORCE: [
    'EXTERNALPUSHDOWN',
    'ORDER',
    'SCALEOUTEXECUTION',
  ],
  HASH: [
    'GROUP',
    'JOIN',
    'UNION',
  ],
  KEEP: ['PLAN'],
  KEEPFIXED: ['PLAN'],
  LOOP: ['JOIN'],
  MERGE: ['JOIN', 'UNION'],
  OPTIMIZE: [['FOR', 'UNKNOWN']],
  ORDER: ['GROUP'],
  PARAMETERIZATION: ['FORCED', 'SIMPLE'],
  ROBUST: ['PLAN'],
  USE: ['PLAN'],
};

const XML_OPTIONS: Record<string, (string | string[])[] | null> = {
  AUTO: [],
  EXPLICIT: [],
  TYPE: [],
  ELEMENTS: ['XSINIL', 'ABSENT'],
  BINARY: ['BASE64'],
};

function buildBuiltinFormattedTime (
  fullFormatMapping: boolean,
): (args: Expression[]) => TimeToStrExpr {
  return (args: Expression[]) => {
    let fmt = seqGet(args, 0);
    if (fmt) {
      const fmtName = (fmt as LiteralExpr).name?.toLowerCase() ?? '';
      const mapping: Record<string, string> = fullFormatMapping
        ? {
          ...TSQL.TIME_MAPPING,
          ...FULL_FORMAT_TIME_MAPPING,
        }
        : { ...TSQL.TIME_MAPPING };
      const mapped = mapping[fmtName];
      if (mapped) {
        fmt = LiteralExpr.string(mapped);
      }
    }

    let thisExpr = seqGet(args, 1);
    if (thisExpr) {
      thisExpr = cast(thisExpr, DataTypeExprKind.DATETIME2);
    }

    return new TimeToStrExpr({
      this: thisExpr,
      format: fmt,
    });
  };
}

function buildFormat (args: Expression[]): NumberToStrExpr | TimeToStrExpr {
  const thisExpr = seqGet(args, 0);
  const fmt = seqGet(args, 1) as LiteralExpr | undefined;
  const culture = seqGet(args, 2);

  const fmtName = fmt?.name ?? '';
  const isNumberFmt = fmtName && (TRANSPILE_SAFE_NUMBER_FMT.has(fmtName) || !DATE_FMT_RE.test(fmtName));

  if (isNumberFmt) {
    return new NumberToStrExpr({
      this: thisExpr,
      format: fmt?.name,
      culture,
    });
  }

  let mappedFmt: Expression | undefined = fmt;
  if (fmt) {
    const mapped = fmtName.length === 1
      ? (TSQL.FORMAT_TIME_MAPPING as Record<string, string>)[fmtName]
      : (TSQL.TIME_MAPPING as Record<string, string>)[fmtName];
    if (mapped) {
      mappedFmt = LiteralExpr.string(mapped);
    }
  }

  return new TimeToStrExpr({
    this: thisExpr,
    format: mappedFmt,
    culture,
  });
}

function buildEomonth (args: Expression[]): LastDayExpr {
  const date = new TsOrDsToDateExpr({ this: seqGet(args, 0) });
  const monthLag = seqGet(args, 1);

  let thisExpr: Expression;
  if (monthLag === undefined) {
    thisExpr = date;
  } else {
    const unit = DATE_DELTA_INTERVAL['month'];
    thisExpr = new DateAddExpr({
      this: date,
      expression: monthLag,
      unit: unit ? var_(unit) : undefined,
    });
  }

  return new LastDayExpr({ this: thisExpr });
}

function buildHashbytes (args: Expression[]): Expression {
  const kind = args[0] as LiteralExpr;
  const data = args[1];
  const kindStr = kind?.isString ? (kind.name ?? '').toUpperCase() : '';

  if (kindStr === 'MD5') {
    args.splice(0, 1);
    return new Md5Expr({ this: data });
  }
  if (kindStr === 'SHA' || kindStr === 'SHA1') {
    args.splice(0, 1);
    return new ShaExpr({ this: data });
  }
  if (kindStr === 'SHA2_256') {
    return new Sha2Expr({
      this: data,
      length: LiteralExpr.number(256),
    });
  }
  if (kindStr === 'SHA2_512') {
    return new Sha2Expr({
      this: data,
      length: LiteralExpr.number(512),
    });
  }

  return func('HASHBYTES', ...args);
}

function buildDatetimefromparts (args: Expression[]): TimestampFromPartsExpr {
  return new TimestampFromPartsExpr({
    year: seqGet(args, 0),
    month: seqGet(args, 1),
    day: seqGet(args, 2),
    hour: seqGet(args, 3),
    min: seqGet(args, 4),
    sec: seqGet(args, 5),
    milli: seqGet(args, 6),
  });
}

function buildTimefromparts (args: Expression[]): TimeFromPartsExpr {
  const fractions = seqGet(args, 3);
  return new TimeFromPartsExpr({
    hour: seqGet(args, 0),
    min: seqGet(args, 1),
    sec: seqGet(args, 2),
    fractions: fractions ? [fractions] : undefined,
    precision: seqGet(args, 4),
  });
}

function buildWithArgAsText<T extends Expression> (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  Klass: new (args: any) => T,
): (args: Expression[]) => T {
  return (args: Expression[]): T => {
    let thisExpr = seqGet(args, 0);

    if (thisExpr && !(thisExpr as LiteralExpr).isString) {
      thisExpr = cast(thisExpr, DataTypeExprKind.TEXT);
    }

    const expression = seqGet(args, 1);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const kwargs: any = { this: thisExpr };

    if (expression) {
      kwargs['expression'] = expression;
    }

    return new Klass(kwargs);
  };
}

function buildParsename (args: Expression[]): SplitPartExpr | Expression {
  if (args.length === 2 && args.every((arg) => (arg as LiteralExpr).isString || (arg as LiteralExpr).isNumber)) {
    const thisExpr = args[0] as LiteralExpr;
    const partIndex = args[1] as LiteralExpr;
    const splitCount = thisExpr.name.split('.').length;
    if (splitCount <= 4) {
      return new SplitPartExpr({
        this: thisExpr,
        delimiter: LiteralExpr.string('.'),
        partIndex: LiteralExpr.number(splitCount + 1 - Number(partIndex.name)),
      });
    }
  }

  return func('PARSENAME', ...args);
}

function buildJsonQuery (args: Expression[], options: { dialect: Dialect }): JsonExtractExpr {
  if (args.length === 1) {
    args.push(LiteralExpr.string('$'));
  }
  return buildExtractJsonWithPath(JsonExtractExpr)(args, options);
}

function buildDatetrunc (args: Expression[]): TimestampTruncExpr {
  const unit = seqGet(args, 0);
  let thisExpr = seqGet(args, 1);

  if (thisExpr && (thisExpr as LiteralExpr).isString) {
    thisExpr = cast(thisExpr, DataTypeExprKind.DATETIME2);
  }

  return new TimestampTruncExpr({
    this: thisExpr,
    unit,
  });
}

function qualifyDerivedTableOutputs (expression: Expression): Expression {
  return expression;
}

function tableName (table: TableExpr): string {
  const parts = [
    table.catalog,
    table.db,
    table.name,
  ].filter(Boolean);
  return parts.join('.');
}

function formatSql (self: Generator, expression: NumberToStrExpr | TimeToStrExpr): string {
  const fmt = expression.args.format as LiteralExpr | undefined;
  let fmtSql: string;

  if (!(expression instanceof NumberToStrExpr)) {
    if (fmt?.isString) {
      const mappedFmt = TSQL.INVERSE_TIME_MAPPING[fmt.name] ?? fmt.name;
      fmtSql = self.sql(LiteralExpr.string(mappedFmt));
    } else {
      fmtSql = self.formatTime?.(expression) ?? (fmt ? self.sql(fmt) : '');
    }
  } else {
    fmtSql = fmt ? self.sql(fmt) : '';
  }

  return self.func('FORMAT', [
    expression.args.this,
    fmtSql,
    expression.args.culture,
  ]);
}

function stringAggSql (self: Generator, expression: GroupConcatExpr): string {
  let thisExpr = expression.args.this;
  const distinct = expression.find(DistinctExpr);
  if (distinct) {
    self.unsupported('T-SQL STRING_AGG doesn\'t support DISTINCT.');
    thisExpr = distinct.pop().args.expressions?.[0];
  }

  let order = '';
  if (expression.args.this instanceof OrderExpr) {
    const orderExpr = expression.args.this as OrderExpr;
    if (orderExpr.args.this) {
      thisExpr = orderExpr.args.this.pop();
    }
    const orderSql = self.sql(orderExpr);
    order = ` WITHIN GROUP (${orderSql.slice(1)})`;
  }

  const separator: Expression = (expression.args.separator as Expression | undefined) ?? LiteralExpr.string(',');
  return `STRING_AGG(${self.formatArgs([thisExpr, separator])})${order}`;
}

function jsonExtractSql (self: Generator, expression: JsonExtractExpr | JsonExtractScalarExpr): string {
  const jsonQuery = self.func('JSON_QUERY', [expression.args.this, expression.args.expression]);
  const jsonValue = self.func('JSON_VALUE', [expression.args.this, expression.args.expression]);
  return self.func('ISNULL', [jsonQuery, jsonValue]);
}

export class TSQLTokenizer extends Tokenizer {
  static IDENTIFIERS: (string | [string, string])[] = [['[', ']'], '"'];
  static QUOTES = ['\'', '"'];
  static HEX_STRINGS: [string, string][] = [['0x', ''], ['0X', '']];
  static VAR_SINGLE_TOKENS = new Set([
    '@',
    '$',
    '#',
  ]);

  static ORIGINAL_KEYWORDS = (() => {
    const keywords: Record<string, TokenType> = {
      ...Tokenizer.KEYWORDS,
      'CLUSTERED INDEX': TokenType.INDEX,
      'DATETIME2': TokenType.DATETIME2,
      'DATETIMEOFFSET': TokenType.TIMESTAMPTZ,
      'DECLARE': TokenType.DECLARE,
      'EXEC': TokenType.COMMAND,
      'FOR SYSTEM_TIME': TokenType.TIMESTAMP_SNAPSHOT,
      'GO': TokenType.COMMAND,
      'IMAGE': TokenType.IMAGE,
      'MONEY': TokenType.MONEY,
      'NONCLUSTERED INDEX': TokenType.INDEX,
      'NTEXT': TokenType.TEXT,
      'OPTION': TokenType.OPTION,
      'OUTPUT': TokenType.RETURNING,
      'PRINT': TokenType.COMMAND,
      'PROC': TokenType.PROCEDURE,
      'REAL': TokenType.FLOAT,
      'ROWVERSION': TokenType.ROWVERSION,
      'SMALLDATETIME': TokenType.SMALLDATETIME,
      'SMALLMONEY': TokenType.SMALLMONEY,
      'SQL_VARIANT': TokenType.VARIANT,
      'SYSTEM_USER': TokenType.CURRENT_USER,
      'TOP': TokenType.TOP,
      'TIMESTAMP': TokenType.ROWVERSION,
      'TINYINT': TokenType.UTINYINT,
      'UNIQUEIDENTIFIER': TokenType.UUID,
      'UPDATE STATISTICS': TokenType.COMMAND,
      'XML': TokenType.XML,
    };
    delete keywords['/*+'];
    return keywords;
  })();

  static COMMANDS = new Set([...Array.from(Tokenizer.COMMANDS), TokenType.END]);
}

export class TSQLParser extends Parser {
  static SET_REQUIRES_ASSIGNMENT_DELIMITER = false;
  static LOG_DEFAULTS_TO_LN = true;
  static STRING_ALIASES = true;
  static NO_PAREN_IF_COMMANDS = false;

  static #QUERY_MODIFIER_PARSERS: Partial<Record<TokenType, (self: Parser) => [string, Expression | Expression[] | undefined]>> | undefined = undefined;
  static get QUERY_MODIFIER_PARSERS (): Partial<Record<TokenType, (self: Parser) => [string, Expression | Expression[] | undefined]>> {
    return TSQLParser.#QUERY_MODIFIER_PARSERS ??= {
      ...Parser.QUERY_MODIFIER_PARSERS,
      [TokenType.OPTION]: (self: Parser) => ['options', (self as TSQLParser).parseOptions()],
      [TokenType.FOR]: (self: Parser) => ['for', (self as TSQLParser).parseFor()],
    };
  }

  // T-SQL does not allow BEGIN to be used as an identifier
  static #ID_VAR_TOKENS: undefined = undefined;
  static get ID_VAR_TOKENS () {
    return TSQLParser.#ID_VAR_TOKENS ??= new Set([...Array.from(Parser.ID_VAR_TOKENS)].filter((t) => t !== TokenType.BEGIN));
  }

  static #ALIAS_TOKENS: undefined = undefined;
  static get ALIAS_TOKENS () {
    return TSQLParser.#ALIAS_TOKENS ??= new Set([...Array.from(Parser.ALIAS_TOKENS)].filter((t) => t !== TokenType.BEGIN));
  }

  static #TABLE_ALIAS_TOKENS: undefined = undefined;
  static get TABLE_ALIAS_TOKENS () {
    return TSQLParser.#TABLE_ALIAS_TOKENS ??= new Set([...Array.from(Parser.TABLE_ALIAS_TOKENS)].filter((t) => t !== TokenType.BEGIN));
  }

  static #COMMENT_TABLE_ALIAS_TOKENS: undefined = undefined;
  static get COMMENT_TABLE_ALIAS_TOKENS () {
    return TSQLParser.#COMMENT_TABLE_ALIAS_TOKENS ??= new Set([...Array.from(Parser.COMMENT_TABLE_ALIAS_TOKENS)].filter((t) => t !== TokenType.BEGIN));
  }

  static #UPDATE_ALIAS_TOKENS: undefined = undefined;
  static get UPDATE_ALIAS_TOKENS () {
    return TSQLParser.#UPDATE_ALIAS_TOKENS ??= new Set([...Array.from(Parser.UPDATE_ALIAS_TOKENS)].filter((t) => t !== TokenType.BEGIN));
  }

  static #FUNCTIONS: undefined = undefined;
  static get FUNCTIONS () {
    return TSQLParser.#FUNCTIONS ??= {
      ...Parser.FUNCTIONS,
      ATN2: Atan2Expr.fromArgList,
      CHARINDEX: (args: Expression[]) => new StrPositionExpr({
        this: seqGet(args, 1),
        substr: seqGet(args, 0),
        position: seqGet(args, 2),
      }),
      COUNT: (args: Expression[]) => new CountExpr({
        this: seqGet(args, 0),
        expressions: args.slice(1),
        bigInt: false,
      }),
      COUNT_BIG: (args: Expression[]) => new CountExpr({
        this: seqGet(args, 0),
        expressions: args.slice(1),
        bigInt: true,
      }),
      DATEADD: buildDateDelta(DateAddExpr, DATE_DELTA_INTERVAL),
      DATEDIFF: buildDateDelta(DateDiffExpr, DATE_DELTA_INTERVAL),
      DATEDIFF_BIG: (args: Expression[]) => {
        const expr = buildDateDelta(DateDiffExpr, DATE_DELTA_INTERVAL)(args);
        expr.setArgKey('bigInt', true);
        return expr;
      },
      DATENAME: buildBuiltinFormattedTime(true),
      DATETIMEFROMPARTS: buildDatetimefromparts,
      EOMONTH: buildEomonth,
      FORMAT: buildFormat,
      GETDATE: CurrentTimestampExpr.fromArgList,
      HASHBYTES: buildHashbytes,
      ISNULL: (args: Expression[]) => buildCoalesce(args, { isNull: true }),
      JSON_QUERY: buildJsonQuery,
      JSON_VALUE: buildExtractJsonWithPath(JsonExtractScalarExpr),
      LEN: buildWithArgAsText(LengthExpr),
      LEFT: buildWithArgAsText(LeftExpr),
      NEWID: UuidExpr.fromArgList,
      RIGHT: buildWithArgAsText(RightExpr),
      PARSENAME: buildParsename,
      REPLICATE: RepeatExpr.fromArgList,
      SCHEMA_NAME: CurrentSchemaExpr.fromArgList,
      SQUARE: (args: Expression[]) => new PowExpr({
        this: seqGet(args, 0),
        expression: LiteralExpr.number(2),
      }),
      SYSDATETIME: CurrentTimestampExpr.fromArgList,
      SUSER_NAME: CurrentUserExpr.fromArgList,
      SUSER_SNAME: CurrentUserExpr.fromArgList,
      SYSDATETIMEOFFSET: CurrentTimestampLtzExpr.fromArgList,
      SYSTEM_USER: CurrentUserExpr.fromArgList,
      TIMEFROMPARTS: buildTimefromparts,
      DATETRUNC: buildDatetrunc,
    };
  }

  static JOIN_HINTS = new Set([
    'LOOP',
    'HASH',
    'MERGE',
    'REMOTE',
  ]);

  static PROCEDURE_OPTIONS: Record<string, string[]> = {
    ENCRYPTION: [],
    RECOMPILE: [],
    SCHEMABINDING: [],
    NATIVE_COMPILATION: [],
    EXECUTE: [],
  };

  static COLUMN_DEFINITION_MODES = new Set([
    'OUT',
    'OUTPUT',
    'READONLY',
  ]);

  static #RETURNS_TABLE_TOKENS: undefined = undefined;
  static get RETURNS_TABLE_TOKENS () {
    return TSQLParser.#RETURNS_TABLE_TOKENS ??= new Set(
      [...Array.from(Parser.ID_VAR_TOKENS)].filter((t) =>
        t !== TokenType.TABLE && !Parser.TYPE_TOKENS.has(t)),
    );
  }

  static #STATEMENT_PARSERS: undefined = undefined;
  static get STATEMENT_PARSERS () {
    return TSQLParser.#STATEMENT_PARSERS ??= {
      ...Parser.STATEMENT_PARSERS,
      [TokenType.DECLARE]: (self: Parser) => (self as TSQLParser).parseDeclare(),
    };
  }

  static #RANGE_PARSERS: undefined = undefined;
  static get RANGE_PARSERS () {
    return TSQLParser.#RANGE_PARSERS ??= {
      ...Parser.RANGE_PARSERS,
      [TokenType.DCOLON]: (self: Parser, thisNode: Expression) => (self as TSQLParser).expression(ScopeResolutionExpr, {
        this: thisNode,
        expression: self.parseFunction() || self.parseVar({ anyToken: true }),
      }),
    };
  }

  static #NO_PAREN_FUNCTION_PARSERS: Record<string, (self: Parser) => Expression> | undefined = undefined;
  static get NO_PAREN_FUNCTION_PARSERS (): Record<string, (self: Parser) => Expression> {
    return TSQLParser.#NO_PAREN_FUNCTION_PARSERS ??= {
      ...Parser.NO_PAREN_FUNCTION_PARSERS,
      NEXT: (self: Parser) => (self as TSQLParser).parseNextValueFor()!,
    };
  }

  static #FUNCTION_PARSERS: Record<string, (self: Parser) => Expression> | undefined = undefined;
  static get FUNCTION_PARSERS (): Record<string, (self: Parser) => Expression> {
    return TSQLParser.#FUNCTION_PARSERS ??= {
      ...Parser.FUNCTION_PARSERS,
      JSON_ARRAYAGG: (self: Parser) => {
        const p = self as TSQLParser;
        return p.expression(JsonArrayAggExpr, {
          this: p.parseBitwise(),
          order: p.parseOrder(),
          nullHandling: p.parseOnHandling('NULL', ['NULL', 'ABSENT']),
        });
      },
      DATEPART: (self: Parser) => (self as TSQLParser).parseDatepart(),
    };
  }

  static #COLUMN_OPERATORS: undefined = undefined;
  static get COLUMN_OPERATORS () {
    return TSQLParser.#COLUMN_OPERATORS ??= {
      ...Parser.COLUMN_OPERATORS,
      [TokenType.DCOLON]: (self: Parser, thisNode?: Expression, to?: Expression) =>
        to instanceof DataTypeExpr && to.args.this !== DataTypeExprKind.USERDEFINED
          ? self.expression(CastExpr, {
            this: thisNode,
            to,
          })
          : self.expression(ScopeResolutionExpr, {
            this: thisNode,
            expression: to,
          }),
    };
  }

  static SET_OP_MODIFIERS = new Set(['offset']);

  static ODBC_DATETIME_LITERALS: Record<string, typeof Expression> = {
    d: DateExpr,
    t: TimeExpr,
    ts: TimestampExpr,
  };

  parseDatepart (): ExtractExpr {
    const thisNode = this.parseVar({ tokens: new Set([TokenType.IDENTIFIER]) });
    const expression = this.match(TokenType.COMMA) && this.parseBitwise();
    const name = mapDatePart(thisNode, { dialect: this.dialect });

    return this.expression(ExtractExpr, {
      this: name,
      expression,
    });
  }

  parseAlterTableSet (): AlterSetExpr {
    return this.parseWrapped(() => super.parseAlterTableSet());
  }

  parseWrappedSelect (options: { table?: boolean } = {}): Expression | undefined {
    if (this.match(TokenType.MERGE)) {
      const comments = this.prevComments;
      const merge = this.parseMerge();
      merge.addComments(comments, { prepend: true });
      return merge;
    }

    return super.parseWrappedSelect(options);
  }

  parseDcolon (): Expression | undefined {
    if (this.matchSet(this._constructor.TYPE_TOKENS, { advance: false })) {
      return this.parseTypes();
    }

    return this.parseFunction() || this.parseTypes();
  }

  parseOptions (): Expression[] | undefined {
    if (!this.match(TokenType.OPTION)) {
      return undefined;
    }

    const parseOption = (): Expression | undefined => {
      const option = this.parseVarFromOptions(OPTIONS);
      if (!option) return undefined;

      this.match(TokenType.EQ);
      return this.expression(QueryOptionExpr, {
        this: option,
        expression: this.parsePrimaryOrVar(),
      });
    };

    return this.parseWrappedCsv(parseOption);
  }

  parseXmlKeyValueOption (): XmlKeyValueOptionExpr {
    const thisNode = this.parsePrimaryOrVar();
    let expression: Expression | undefined;

    if (this.match(TokenType.L_PAREN, { advance: false })) {
      expression = this.parseWrapped(() => this.parseString());
    } else {
      expression = undefined;
    }

    return new XmlKeyValueOptionExpr({
      this: thisNode,
      expression,
    });
  }

  parseFor (): Expression[] | undefined {
    if (!this.matchPair(TokenType.FOR, TokenType.XML)) {
      return undefined;
    }

    const parseForXml = (): Expression | undefined => {
      return this.expression(QueryOptionExpr, {
        this: this.parseVarFromOptions(XML_OPTIONS, { raiseUnmatched: false })
          || this.parseXmlKeyValueOption(),
      });
    };

    return this.parseCsv(parseForXml);
  }

  parseProjections (): Expression[] {
    /**
     * T-SQL supports alias = expression in SELECT.
     * Converts EQ projections into Aliases.
     */
    return super.parseProjections().map((projection) => {
      if (projection instanceof EqExpr && projection.args.this instanceof ColumnExpr) {
        return alias(projection.args.expression as Expression, projection.args.this.args.this as IdentifierExpr, { copy: false });
      }
      return projection;
    });
  }

  parseCommitOrRollback (): CommitExpr | RollbackExpr {
    const rollback = this.prev?.tokenType === TokenType.ROLLBACK;

    this.matchTexts(['TRAN', 'TRANSACTION']);
    const thisNode = this.parseIdVar();

    if (rollback) {
      return this.expression(RollbackExpr, { this: thisNode });
    }

    let durability: boolean | undefined = undefined;
    if (this.matchPair(TokenType.WITH, TokenType.L_PAREN)) {
      this.matchTextSeq('DELAYED_DURABILITY');
      this.match(TokenType.EQ);

      if (this.matchTextSeq('OFF')) {
        durability = false;
      } else {
        this.match(TokenType.ON);
        durability = true;
      }

      this.matchRParen();
    }

    return this.expression(CommitExpr, {
      this: thisNode,
      durability,
    });
  }

  parseTransaction (): TransactionExpr | CommandExpr {
    if (this.matchTexts(['TRAN', 'TRANSACTION'])) {
      const transactionName = this.parseIdVar();
      const transaction = this.expression(TransactionExpr, { this: transactionName });
      if (this.matchTextSeq(['WITH', 'MARK'])) {
        transaction.setArgKey('mark', this.parseString());
      }
      return transaction;
    }

    return this.parseAsCommand(this.prev);
  }

  parseReturns (): ReturnsPropertyExpr {
    const table = this.parseIdVar({
      anyToken: false,
      tokens: (this._constructor as unknown as typeof TSQLParser).RETURNS_TABLE_TOKENS,
    });
    const returns = super.parseReturns();
    returns.setArgKey('table', table);
    return returns;
  }

  parseConvert (strict: boolean, options: { safe?: boolean } = {}): Expression | undefined {
    const { safe } = options;
    const thisNode = this.parseTypes();
    this.match(TokenType.COMMA);
    const args = [thisNode, ...this.parseCsv(() => this.parseAssignment())];
    const convert = ConvertExpr.fromArgList(args);
    convert.setArgKey('safe', safe);
    return convert;
  }

  parseColumnDef (thisNode?: Expression, options: { computedColumn?: boolean } = {}): Expression | undefined {
    const { computedColumn = true } = options;
    const columnDef = super.parseColumnDef(thisNode, { computedColumn });
    if (!columnDef) return undefined;

    if (this.match(TokenType.EQ)) {
      columnDef.setArgKey('default', this.parseDisjunction());
    }
    if (this.matchTexts((this._constructor as unknown as typeof TSQLParser).COLUMN_DEFINITION_MODES)) {
      columnDef.setArgKey('output', this.prev?.text);
    }
    return columnDef;
  }

  parseUserDefinedFunction (options: { kind?: TokenType } = {}): Expression | undefined {
    const { kind } = options;
    const thisNode = super.parseUserDefinedFunction(options);

    if (
      kind === TokenType.FUNCTION
      || thisNode instanceof UserDefinedFunctionExpr
      || this.match(TokenType.ALIAS, { advance: false })
    ) {
      return thisNode;
    }

    let expressions: Expression[] | undefined;
    if (!this.match(TokenType.WITH, { advance: false })) {
      expressions = this.parseCsv(() => this.parseFunctionParameter());
    }

    return this.expression(UserDefinedFunctionExpr, {
      this: thisNode,
      expressions,
    });
  }

  parseInto (): IntoExpr | undefined {
    const into = super.parseInto();

    if (into instanceof IntoExpr) {
      const table = into.find(TableExpr);
      if (table instanceof TableExpr) {
        const tableIdentifier = table.args.this;
        if (narrowInstanceOf(tableIdentifier, Expression)?.getArgKey('temporary')) {
          into.setArgKey('temporary', true);
        }
      }
    }

    return into;
  }

  parseIdVar (options: {
    anyToken?: boolean;
    tokens?: Set<TokenType>;
  } = {}): Expression | undefined {
    const {
      anyToken = true, tokens,
    } = options;
    const isTemporary = this.match(TokenType.HASH);
    const isGlobal = isTemporary && this.match(TokenType.HASH);

    const thisNode = super.parseIdVar({
      anyToken,
      tokens,
    });
    if (thisNode) {
      if (isGlobal) {
        thisNode.setArgKey('global', true);
      } else if (isTemporary) {
        thisNode.setArgKey('temporary', true);
      }
    }

    return thisNode;
  }

  parseCreate (): CreateExpr | CommandExpr {
    const create = super.parseCreate();

    if (create instanceof CreateExpr) {
      const table = create.args.this instanceof SchemaExpr ? create.args.this.args.this : create.args.this;
      if (table instanceof TableExpr && narrowInstanceOf(table.args.this, Expression)?.getArgKey('temporary')) {
        if (!create.args.properties) {
          create.setArgKey('properties', new PropertiesExpr({ expressions: [] }));
        }
        (create.args.properties as PropertiesExpr).append('expressions', new TemporaryPropertyExpr());
      }
    }

    return create;
  }

  parseIf (): Expression | undefined {
    const index = this.index;

    if (this.matchTextSeq('OBJECT_ID')) {
      this.parseWrappedCsv(() => this.parseString());
      if (this.matchTextSeq([
        'IS',
        'NOT',
        'NULL',
      ]) && this.match(TokenType.DROP)) {
        return this.parseDrop({ exists: true });
      }
      this.retreat(index);
    }

    return super.parseIf();
  }

  parseUnique (): UniqueColumnConstraintExpr {
    let thisNode: ExpressionValue | undefined;
    if (this.matchTexts(['CLUSTERED', 'NONCLUSTERED'])) {
      const parsers = this._constructor.CONSTRAINT_PARSERS;
      thisNode = seqGet(ensureIterable(parsers[this.prev?.text.toUpperCase() ?? '']?.(this)), 0) as ExpressionValue;
    } else {
      thisNode = this.parseSchema({ this: this.parseIdVar({ anyToken: false }) }) ?? new VarExpr({});
    }

    return this.expression(UniqueColumnConstraintExpr, { this: thisNode });
  }

  parseUpdate (): UpdateExpr {
    const expression = super.parseUpdate();
    expression.setArgKey('options', this.parseOptions());
    return expression;
  }

  parsePartition (): PartitionExpr | undefined {
    if (!this.matchTextSeq([
      'WITH',
      '(',
      'PARTITIONS',
    ])) {
      return undefined;
    }

    const parseRange = (): Expression => {
      const low = this.parseBitwise();
      const high = this.matchTextSeq('TO') ? this.parseBitwise() : undefined;

      return high
        ? this.expression(PartitionRangeExpr, {
          this: low,
          expression: high,
        })
        : (low ?? this.expression(PartitionRangeExpr, {}));
    };

    const expressions = this.parseWrappedCsv(parseRange);
    const partition = this.expression(PartitionExpr, { expressions });

    this.matchRParen();

    return partition;
  }

  parseDeclareitem (): DeclareItemExpr | undefined {
    const varNode = this.parseIdVar();
    if (!varNode) return undefined;

    this.match(TokenType.ALIAS);
    return this.expression(DeclareItemExpr, {
      this: varNode,
      kind: this.match(TokenType.TABLE) ? this.parseSchema() : this.parseTypes(),
      default: this.match(TokenType.EQ) ? this.parseBitwise() : undefined,
    });
  }

  parseAlterTableAlter (): Expression | undefined {
    const expression = super.parseAlterTableAlter();

    if (expression) {
      const collation = expression.getArgKey('collate');
      if (collation instanceof ColumnExpr && collation.args.this instanceof IdentifierExpr) {
        const identifier = collation.args.this;
        collation.setArgKey('this', new VarExpr({ this: identifier.name }));
      }
    }

    return expression;
  }

  parsePrimaryKeyPart (): Expression | undefined {
    return this.parseOrdered();
  }
}

export class TSQLGenerator extends Generator {
  static LIMIT_IS_TOP = true;
  static QUERY_HINTS = false;
  static RETURNING_END = false;
  static NVL2_SUPPORTED = false;
  static ALTER_TABLE_INCLUDE_COLUMN_KEYWORD = false;
  static LIMIT_FETCH = 'FETCH';
  static COMPUTED_COLUMN_WITH_TYPE = false;
  static CTE_RECURSIVE_KEYWORD_REQUIRED = false;
  static ENSURE_BOOLS = true;
  static NULL_ORDERING_SUPPORTED = undefined;
  static SUPPORTS_SINGLE_ARG_CONCAT = false;
  static TABLESAMPLE_SEED_KEYWORD = 'REPEATABLE';
  static SUPPORTS_SELECT_INTO = true;
  static JSON_PATH_BRACKETED_KEY_SUPPORTED = false;
  static SUPPORTS_TO_NUMBER = false;
  static SET_OP_MODIFIERS = false;
  static COPY_PARAMS_EQ_REQUIRED = true;
  static PARSE_JSON_NAME = undefined;
  static EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = false;
  static ALTER_SET_WRAPPED = true;
  static ALTER_SET_TYPE = '';

  static EXPRESSIONS_WITHOUT_NESTED_CTES = new Set([
    CreateExpr,
    DeleteExpr,
    InsertExpr,
    IntersectExpr,
    ExceptExpr,
    MergeExpr,
    SelectExpr,
    SubqueryExpr,
    UnionExpr,
    UpdateExpr,
  ]);

  static SUPPORTED_JSON_PATH_PARTS = new Set([
    JsonPathKeyExpr,
    JsonPathRootExpr,
    JsonPathSubscriptExpr,
  ]);

  static TYPE_MAPPING = (() => {
    const mapping = new Map<DataTypeExprKind, string>([
      ...(Generator.TYPE_MAPPING.entries() as unknown as [DataTypeExprKind, string][]),
      [DataTypeExprKind.BOOLEAN, 'BIT'],
      [DataTypeExprKind.DATETIME2, 'DATETIME2'],
      [DataTypeExprKind.DECIMAL, 'NUMERIC'],
      [DataTypeExprKind.DOUBLE, 'FLOAT'],
      [DataTypeExprKind.INT, 'INTEGER'],
      [DataTypeExprKind.ROWVERSION, 'ROWVERSION'],
      [DataTypeExprKind.TEXT, 'VARCHAR(MAX)'],
      [DataTypeExprKind.TIMESTAMP, 'DATETIME2'],
      [DataTypeExprKind.TIMESTAMPNTZ, 'DATETIME2'],
      [DataTypeExprKind.TIMESTAMPTZ, 'DATETIMEOFFSET'],
      [DataTypeExprKind.SMALLDATETIME, 'SMALLDATETIME'],
      [DataTypeExprKind.UTINYINT, 'TINYINT'],
      [DataTypeExprKind.VARIANT, 'SQL_VARIANT'],
      [DataTypeExprKind.UUID, 'UNIQUEIDENTIFIER'],
    ]);

    mapping.delete(DataTypeExprKind.NCHAR);
    mapping.delete(DataTypeExprKind.NVARCHAR);

    return mapping;
  })();

  static ORIGINAL_TRANSFORMS = (() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (self: Generator, e: any) => string>(([
      ...Generator.TRANSFORMS.entries(),
      [AnyValueExpr, anyValueToMaxSql],
      [Atan2Expr, renameFunc('ATN2')],
      [ArrayToStringExpr, renameFunc('STRING_AGG')],
      [AutoIncrementColumnConstraintExpr, () => 'IDENTITY'],
      [CeilExpr, renameFunc('CEILING')],
      [ChrExpr, renameFunc('CHAR')],
      [DateAddExpr, dateDeltaSql('DATEADD')],
      [CteExpr, preprocess([qualifyDerivedTableOutputs])],
      [CurrentDateExpr, renameFunc('GETDATE')],
      [CurrentTimestampExpr, renameFunc('GETDATE')],
      [CurrentTimestampLtzExpr, renameFunc('SYSDATETIMEOFFSET')],
      [DateStrToDateExpr, dateStrToDateSql],
      [GeneratedAsIdentityColumnConstraintExpr, generatedAsIdentityColumnConstraintSql],
      [GroupConcatExpr, stringAggSql],
      [IfExpr, renameFunc('IIF')],
      [JsonExtractExpr, jsonExtractSql],
      [JsonExtractScalarExpr, jsonExtractSql],
      [LastDayExpr, (self: Generator, e: LastDayExpr) => self.func('EOMONTH', [e.args.this])],
      [LnExpr, renameFunc('LOG')],
      [MaxExpr, maxOrGreatest],
      [Md5Expr, (self: Generator, e: Md5Expr) => self.func('HASHBYTES', [LiteralExpr.string('MD5'), e.args.this])],
      [MinExpr, minOrLeast],
      [NumberToStrExpr, formatSql],
      [RepeatExpr, renameFunc('REPLICATE')],
      [CurrentSchemaExpr, renameFunc('SCHEMA_NAME')],
      [
        SelectExpr,
        preprocess([
          eliminateDistinctOn,
          eliminateSemiAndAntiJoins,
          eliminateQualify,
          unnestGenerateDateArrayUsingRecursiveCte,
        ]),
      ],
      [StddevExpr, renameFunc('STDEV')],
      [
        StrPositionExpr,
        (self: Generator, e: StrPositionExpr) =>
          strPositionSql(self, e, {
            funcName: 'CHARINDEX',
            supportsPosition: true,
          }),
      ],
      [SubqueryExpr, preprocess([qualifyDerivedTableOutputs])],
      [ShaExpr, (self: Generator, e: ShaExpr) => self.func('HASHBYTES', [LiteralExpr.string('SHA1'), e.args.this])],
      [Sha1DigestExpr, (self: Generator, e: Sha1DigestExpr) => self.func('HASHBYTES', [LiteralExpr.string('SHA1'), e.args.this])],
      [
        Sha2Expr,
        (self: Generator, e: Sha2Expr) =>
          self.func('HASHBYTES', [LiteralExpr.string(`SHA2_${e.args.length || 256}`), e.args.this]),
      ],
      [TemporaryPropertyExpr, () => ''],
      [TimeStrToTimeExpr, timeStrToTimeSql],
      [TimeToStrExpr, formatSql],
      [TrimExpr, trimSql],
      [TsOrDsAddExpr, dateDeltaSql('DATEADD', { cast: true })],
      [TsOrDsDiffExpr, dateDeltaSql('DATEDIFF')],
      [TimestampTruncExpr, (self: Generator, e: TimestampTruncExpr) => self.func('DATETRUNC', [e.args.unit, e.args.this])],
      [
        TruncExpr,
        (self: Generator, e: TruncExpr) =>
          self.func('ROUND', [
            e.args.this,
            e.args.decimals || LiteralExpr.number(0),
            LiteralExpr.number(1),
          ]),
      ],
      [UuidExpr, () => 'NEWID()'],
      [DateFromPartsExpr, renameFunc('DATEFROMPARTS')],
    ]));

    transforms.delete(ReturnsPropertyExpr);

    return transforms;
  })();

  static PROPERTIES_LOCATION = new Map<typeof Expression, PropertiesLocation>([...Generator.PROPERTIES_LOCATION.entries(), [VolatilePropertyExpr, PropertiesLocation.UNSUPPORTED]]);

  scopeResolution (rhs: string, scopeName: string): string {
    return `${scopeName}::${rhs}`;
  }

  selectSql (expression: SelectExpr): string {
    const limit = expression.args.limit;
    let offset = expression.args.offset;

    // T-SQL requires OFFSET in order to use FETCH
    if (limit instanceof FetchExpr && !offset) {
      offset = new OffsetExpr({ expression: LiteralExpr.number(0) });
      expression.setArgKey('offset', offset);
    }

    if (offset) {
      if (!expression.args.order) {
        // T-SQL OFFSET requires an ORDER BY. Use a no-op subquery if none exists.
        //
        expression.orderBy(new SelectExpr({ expressions: [new NullExpr()] }).subquery(), { copy: false });
      }

      if (limit instanceof LimitExpr) {
        // TOP and OFFSET can't be combined; replace TOP with FETCH
        limit.replace(new FetchExpr({
          direction: 'FIRST',
          count: limit.args.expression,
        }));
      }
    }

    return super.selectSql(expression);
  }

  convertSql (expression: ConvertExpr): string {
    const name = expression.args.safe ? 'TRY_CONVERT' : 'CONVERT';
    return this.func(name, [
      expression.args.this,
      expression.args.expression,
      expression.args.style,
    ]);
  }

  queryoptionSql (expression: QueryOptionExpr): string {
    const option = this.sql(expression, 'this');
    const value = this.sql(expression, 'expression');

    if (value) {
      // T-SQL options like MAXDOP don't use '=', but others like LABEL do.
      const optionalEqualSign = OPTIONS_THAT_REQUIRE_EQUAL.has(option) ? '= ' : '';
      return `${option} ${optionalEqualSign}${value}`;
    }
    return option;
  }

  lateralOp (expression: LateralExpr): string {
    const crossApply = expression.args.crossApply;
    if (crossApply === true) {
      return 'CROSS APPLY';
    }
    if (crossApply === false) {
      return 'OUTER APPLY';
    }

    this.unsupported('LATERAL clause is not supported.');
    return 'LATERAL';
  }

  splitpartSql (expression: SplitPartExpr): string {
    const thisNode = expression.args.this;
    const splitCount = (thisNode instanceof LiteralExpr) ? thisNode.name.split('.').length : 0;
    const delimiter = expression.args.delimiter;
    const partIndex = expression.args.partIndex;

    if (
      !(thisNode instanceof LiteralExpr && delimiter instanceof LiteralExpr && partIndex instanceof LiteralExpr)
      || (delimiter && delimiter.name !== '.')
      || !partIndex
      || 4 < splitCount
    ) {
      this.unsupported(
        'SPLIT_PART can be transpiled to PARSENAME only for \'.\' delimiter and literal values',
      );
      return '';
    }

    const reversedIndex = splitCount + 1 - Number(partIndex.name);
    return this.func('PARSENAME', [thisNode, LiteralExpr.number(reversedIndex)]);
  }

  extractSql (expression: ExtractExpr): string {
    const part = expression.args.this;
    const partName = (part instanceof LiteralExpr || part instanceof IdentifierExpr)
      ? part.name.toUpperCase()
      : '';
    const name = DATE_PART_UNMAPPING[partName] || part;

    return this.func('DATEPART', [name, expression.args.expression]);
  }

  timefrompartsSql (expression: TimeFromPartsExpr): string {
    const nano = expression.args.nano;
    if (nano !== undefined) {
      nano.pop();
      this.unsupported('Specifying nanoseconds is not supported in TIMEFROMPARTS.');
    }

    if (expression.args.fractions === undefined) {
      expression.setArgKey('fractions', LiteralExpr.number(0));
    }
    if (expression.args.precision === undefined) {
      expression.setArgKey('precision', LiteralExpr.number(0));
    }

    return renameFunc('TIMEFROMPARTS')(this, expression);
  }

  timestampfrompartsSql (expression: TimestampFromPartsExpr): string {
    const zone = expression.args.zone;
    if (zone !== undefined) {
      zone.pop();
      this.unsupported('Time zone is not supported in DATETIMEFROMPARTS.');
    }

    const nano = expression.args.nano;
    if (nano !== undefined) {
      nano.pop();
      this.unsupported('Specifying nanoseconds is not supported in DATETIMEFROMPARTS.');
    }

    if (expression.args.milli === undefined) {
      expression.setArgKey('milli', LiteralExpr.number(0));
    }

    return renameFunc('DATETIMEFROMPARTS')(this, expression);
  }

  setItemSql (expression: SetItemExpr): string {
    const thisNode = expression.args.this;
    if (thisNode instanceof EqExpr && !(thisNode.args.this instanceof ParameterExpr)) {
      // T-SQL does not use '=' in SET command for system options (e.g., SET NOCOUNT ON),
      // except when the LHS is a variable (@var = val).
      return `${this.sql(thisNode.args.this)} ${this.sql(thisNode.args.expression)}`;
    }

    return super.setItemSql(expression);
  }

  booleanSql (expression: BooleanExpr): string {
    const parent = expression.parent;
    const valuesOrSelect = expression.findAncestor<ValuesExpr | SelectExpr>(ValuesExpr, SelectExpr);

    // T-SQL does not have a native BOOLEAN type; it uses BIT (0/1).
    // In predicates, we use (1=1) or (1=0).
    //
    if (
      (parent && BIT_TYPES.has(parent.constructor))
      || (valuesOrSelect instanceof ValuesExpr)
    ) {
      return expression.args.this ? '1' : '0';
    }

    return expression.args.this ? '(1 = 1)' : '(1 = 0)';
  }

  isSql (expression: IsExpr): string {
    if (expression.args.expression instanceof BooleanExpr) {
      return this.binary(expression, '=');
    }
    return this.binary(expression, 'IS');
  }

  createableSql (expression: CreateExpr): string {
    let sql = this.sql(expression, 'this');
    const properties = expression.args.properties;

    // T-SQL temporary tables must start with #.
    // We automatically prepend # if a TemporaryProperty is found.
    if (
      sql[0] !== '#'
      && properties?.args?.expressions?.some((prop) => prop instanceof TemporaryPropertyExpr)
    ) {
      sql = sql.startsWith('[') ? `[#${sql.slice(1)}` : `#${sql}`;
    }

    return sql;
  }

  createSql (expression: CreateExpr): string {
    const kind = expression.args.kind;
    const exists = expression.args.exists;
    expression.setArgKey('exists', undefined);

    const likeProperty = expression.find(LikePropertyExpr);
    let ctasExpression = likeProperty ? likeProperty.args.this : expression.args.expression;

    if (kind === CreateExprKind.VIEW) {
      expression.args.this?.setArgKey('catalog', undefined);
      const withClause = expression.args.with;
      if (ctasExpression && withClause) {
        // CREATE VIEW requires WITH after the query
        ctasExpression.setArgKey('with', withClause.pop());
      }
    }

    const table = expression.find(TableExpr);

    let sql: string;
    // Convert CTAS (Create Table As Select) to T-SQL's SELECT .. INTO ..
    if (kind === CreateExprKind.TABLE && ctasExpression) {
      if (ctasExpression instanceof SelectExpr) {
        ctasExpression = ctasExpression.subquery();
      }

      const properties = expression.args.properties || new PropertiesExpr({ expressions: [] });
      const isTemp = (properties.args.expressions ?? []).some((p) => p instanceof TemporaryPropertyExpr);

      const selectInto = new SelectExpr({ expressions: [LiteralExpr.string('*')] })
        .from(alias(ctasExpression, 'temp', { table: true }));

      selectInto.setArgKey('into', new IntoExpr({
        this: table,
        temporary: isTemp,
      }));

      if (likeProperty) {
        selectInto.limit(0, { copy: false });
      }

      sql = this.sql(selectInto);
    } else {
      sql = super.createSql(expression);
    }

    if (exists) {
      const idLiteral = this.sql(LiteralExpr.string(table ? tableName(table) : ''));
      const sqlWithCtes = this.prependCtes(expression, sql);
      const sqlLiteral = this.sql(LiteralExpr.string(sqlWithCtes));

      if (kind === CreateExprKind.SCHEMA) {
        return `IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ${idLiteral}) EXEC(${sqlLiteral})`;
      } else if (kind === CreateExprKind.TABLE) {
        const where = [
          `TABLE_NAME = ${this.sql(LiteralExpr.string((table as TableExpr).name))}`,
          (table as TableExpr).db ? `TABLE_SCHEMA = ${this.sql(LiteralExpr.string((table as TableExpr).db))}` : null,
          (table as TableExpr).catalog ? `TABLE_CATALOG = ${this.sql(LiteralExpr.string((table as TableExpr).catalog))}` : null,
        ].filter(Boolean).join(' AND ');

        return `IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE ${where}) EXEC(${sqlLiteral})`;
      } else if (kind === CreateExprKind.INDEX) {
        const indexName = this.sql(LiteralExpr.string(expression.args.this?.text('this') || ''));
        return `IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = object_id(${idLiteral}) AND name = ${indexName}) EXEC(${sqlLiteral})`;
      }
    } else if (expression.args.replace) {
      sql = sql.replace('CREATE OR REPLACE ', 'CREATE OR ALTER ');
    }

    return this.prependCtes(expression, sql);
  }

  intoSql (expression: IntoExpr): string {
    if (expression.args.temporary) {
      const table = expression.find(TableExpr);
      if (table && table.args.this instanceof IdentifierExpr) {
        table.args.this.setArgKey('temporary', true);
      }
    }
    return `${this.seg('INTO')} ${this.sql(expression, 'this')}`;
  }

  countSql (expression: CountExpr): string {
    const funcName = expression.args.bigInt ? 'COUNT_BIG' : 'COUNT';
    return renameFunc(funcName)(this, expression);
  }

  datediffSql (expression: DateDiffExpr): string {
    const funcName = expression.args.bigInt ? 'DATEDIFF_BIG' : 'DATEDIFF';
    return dateDeltaSql(funcName)(this, expression);
  }

  offsetSql (expression: OffsetExpr): string {
    return `${super.offsetSql(expression)} ROWS`;
  }

  versionSql (expression: VersionExpr): string {
    const name = expression.name === 'TIMESTAMP' ? 'SYSTEM_TIME' : expression.name;
    const kind = expression.text('kind');
    const expr = expression.args.expression;
    let exprSql = '';

    if (kind === 'FROM' || kind === 'BETWEEN') {
      const args = expr?.args.expressions as ExpressionValue[] ?? [];
      const sep = kind === 'FROM' ? 'TO' : 'AND';
      exprSql = `${this.sql(seqGet(args, 0))} ${sep} ${this.sql(seqGet(args, 1))}`;
    } else {
      exprSql = this.sql(expr);
    }

    return `FOR ${name} ${kind}${exprSql ? ' ' + exprSql : ''}`;
  }

  returnspropertySql (expression: ReturnsPropertyExpr): string {
    const table = expression.args.table;
    const tablePart = table ? `${table} ` : '';
    return `RETURNS ${tablePart}${this.sql(expression, 'this')}`;
  }

  returningSql (expression: ReturningExpr): string {
    const into = this.sql(expression, 'into');
    const intoPart = into ? this.seg(`INTO ${into}`) : '';
    return `${this.seg('OUTPUT')} ${this.expressions(expression, { flat: true })}${intoPart}`;
  }

  transactionSql (expression: TransactionExpr): string {
    const thisPart = expression.args.this ? ` ${this.sql(expression, 'this')}` : '';
    const mark = expression.args.mark ? ` WITH MARK ${this.sql(expression, 'mark')}` : '';
    return `BEGIN TRANSACTION${thisPart}${mark}`;
  }

  commitSql (expression: CommitExpr): string {
    const thisPart = expression.args.this ? ` ${this.sql(expression, 'this')}` : '';
    const durability = expression.args.durability !== undefined
      ? ` WITH (DELAYED_DURABILITY = ${expression.args.durability ? 'ON' : 'OFF'})`
      : '';
    return `COMMIT TRANSACTION${thisPart}${durability}`;
  }

  rollbackSql (expression: RollbackExpr): string {
    const thisPart = expression.args.this ? ` ${this.sql(expression, 'this')}` : '';
    return `ROLLBACK TRANSACTION${thisPart}`;
  }

  identifierSql (expression: IdentifierExpr): string {
    let identifier = super.identifierSql(expression);
    if (expression.args.global) {
      identifier = `##${identifier}`;
    } else if (expression.args.temporary) {
      identifier = `#${identifier}`;
    }
    return identifier;
  }

  constraintSql (expression: ConstraintExpr): string {
    return `CONSTRAINT ${this.sql(expression, 'this')} ${this.expressions(expression, {
      flat: true,
      sep: ' ',
    })}`;
  }

  lengthSql (expression: LengthExpr): string {
    return this.uncastText(expression, 'LEN');
  }

  rightSql (expression: RightExpr): string {
    return this.uncastText(expression, 'RIGHT');
  }

  leftSql (expression: LeftExpr): string {
    return this.uncastText(expression, 'LEFT');
  }

  private uncastText (expression: Expression, name: string): string {
    const thisNode = expression.args.this;
    const thisSql = (thisNode instanceof CastExpr && thisNode.isType(DataTypeExprKind.TEXT))
      ? this.sql(thisNode, 'this')
      : this.sql(thisNode);
    const expressionSql = this.sql(expression, 'expression');
    return this.func(name, [thisSql, expressionSql || undefined]);
  }

  partitionSql (expression: PartitionExpr): string {
    return `WITH (PARTITIONS(${this.expressions(expression, { flat: true })}))`;
  }

  alterSql (expression: AlterExpr): string {
    const action = seqGet(expression.args.actions ?? [], 0);
    if (action instanceof AlterRenameExpr) {
      return `EXEC sp_rename '${this.sql(expression, 'this')}', '${(action.args.this)?.name ?? ''}'`;
    }
    return super.alterSql(expression);
  }

  dropSql (expression: DropExpr): string {
    if (expression.args.kind === DropExprKind.VIEW) {
      expression.args.this?.setArgKey('catalog', undefined);
    }
    return super.dropSql(expression);
  }

  optionsModifier (expression: Expression): string {
    const options = this.expressions(expression, { key: 'options' });
    return options ? ` OPTION${this.wrap(options)}` : '';
  }

  dpipeSql (expression: DPipeExpr): string {
    // Converts || concatenation into + for T-SQL
    const flattened = [...expression.flatten()].reduce((acc: Expression, curr: Expression) =>
      new AddExpr({
        this: acc,
        expression: curr,
      }));
    return this.sql(flattened);
  }

  isAsciiSql (expression: IsAsciiExpr): string {
    // T-SQL implementation using PATINDEX and hex range
    return `(PATINDEX(CONVERT(VARCHAR(MAX), 0x255b5e002d7f5d25) COLLATE Latin1_General_BIN, ${this.sql(expression.args.this)}) = 0)`;
  }

  columnDefSql (expression: ColumnDefExpr, options: { sep?: string } = {}): string {
    const thisSql = super.columnDefSql(expression, options);
    const defaultValue = expression.args.default ? ` = ${this.sql(expression, 'default')}` : '';
    const output = expression.args.output ? ` ${this.sql(expression, 'output')}` : '';
    return `${thisSql}${defaultValue}${output}`;
  }

  coalesceSql (expression: CoalesceExpr): string {
    const funcName = expression.args.isNull ? 'ISNULL' : 'COALESCE';
    return renameFunc(funcName)(this, expression);
  }
}

export class TSQL extends Dialect {
  static SUPPORTS_SEMI_ANTI_JOIN = false;
  static LOG_BASE_FIRST = false;
  static TYPED_DIVISION = true;
  static CONCAT_COALESCE = true;
  static NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE;
  static ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN = false;

  static TIME_FORMAT = '\'yyyy-mm-dd hh:mm:ss\'';

  static EXPRESSION_METADATA = { ...EXPRESSION_METADATA };

  static DATE_PART_MAPPING = {
    ...Dialect.DATE_PART_MAPPING,
    QQ: 'QUARTER',
    M: 'MONTH',
    Y: 'DAYOFYEAR',
    WW: 'WEEK',
    N: 'MINUTE',
    SS: 'SECOND',
    MCS: 'MICROSECOND',
    TZOFFSET: 'TIMEZONE_MINUTE',
    TZ: 'TIMEZONE_MINUTE',
    ISO_WEEK: 'WEEKISO',
    ISOWK: 'WEEKISO',
    ISOWW: 'WEEKISO',
  };

  static TIME_MAPPING = {
    year: '%Y',
    dayofyear: '%j',
    day: '%d',
    dy: '%d',
    y: '%Y',
    week: '%W',
    ww: '%W',
    wk: '%W',
    isowk: '%V',
    isoww: '%V',
    isoWeek: '%V',
    hour: '%h',
    hh: '%I',
    minute: '%M',
    mi: '%M',
    n: '%M',
    second: '%S',
    ss: '%S',
    s: '%-S',
    millisecond: '%f',
    ms: '%f',
    weekday: '%w',
    dw: '%w',
    month: '%m',
    mm: '%M',
    m: '%-M',
    Y: '%Y',
    YYYY: '%Y',
    YY: '%y',
    MMMM: '%B',
    MMM: '%b',
    MM: '%m',
    M: '%-m',
    dddd: '%A',
    dd: '%d',
    d: '%-d',
    HH: '%H',
    H: '%-H',
    h: '%-I',
    ffffff: '%f',
    yyyy: '%Y',
    yy: '%y',
  };

  static CONVERT_FORMAT_MAPPING = {
    0: '%b %d %Y %-I:%M%p',
    1: '%m/%d/%y',
    2: '%y.%m.%d',
    3: '%d/%m/%y',
    4: '%d.%m.%y',
    5: '%d-%m-%y',
    6: '%d %b %y',
    7: '%b %d, %y',
    8: '%H:%M:%S',
    9: '%b %d %Y %-I:%M:%S:%f%p',
    10: 'mm-dd-yy',
    11: 'yy/mm/dd',
    12: 'yymmdd',
    13: '%d %b %Y %H:%M:ss:%f',
    14: '%H:%M:%S:%f',
    20: '%Y-%m-%d %H:%M:%S',
    21: '%Y-%m-%d %H:%M:%S.%f',
    22: '%m/%d/%y %-I:%M:%S %p',
    23: '%Y-%m-%d',
    24: '%H:%M:%S',
    25: '%Y-%m-%d %H:%M:%S.%f',
    100: '%b %d %Y %-I:%M%p',
    101: '%m/%d/%Y',
    102: '%Y.%m.%d',
    103: '%d/%m/%Y',
    104: '%d.%m.%Y',
    105: '%d-%m-%Y',
    106: '%d %b %Y',
    107: '%b %d, %Y',
    108: '%H:%M:%S',
    109: '%b %d %Y %-I:%M:%S:%f%p',
    110: '%m-%d-%Y',
    111: '%Y/%m/%d',
    112: '%Y%m%d',
    113: '%d %b %Y %H:%M:%S:%f',
    114: '%H:%M:%S:%f',
    120: '%Y-%m-%d %H:%M:%S',
    121: '%Y-%m-%d %H:%M:%S.%f',
    126: '%Y-%m-%dT%H:%M:%S.%f',
  };

  static FORMAT_TIME_MAPPING = {
    y: '%B %Y',
    d: '%m/%d/%Y',
    H: '%-H',
    h: '%-I',
    s: '%Y-%m-%d %H:%M:%S',
    D: '%A,%B,%Y',
    f: '%A,%B,%Y %-I:%M %p',
    F: '%A,%B,%Y %-I:%M:%S %p',
    g: '%m/%d/%Y %-I:%M %p',
    G: '%m/%d/%Y %-I:%M:%S %p',
    M: '%B %-d',
    m: '%B %-d',
    O: '%Y-%m-%dT%H:%M:%S',
    u: '%Y-%M-%D %H:%M:%S%z',
    U: '%A, %B %D, %Y %H:%M:%S%z',
    T: '%-I:%M:%S %p',
    t: '%-I:%M',
    Y: '%a %Y',
  };

  public static Tokenizer = TSQLTokenizer;
  public static Parser = TSQLParser;
  public static Generator = TSQLGenerator;
}

Dialect.register(Dialects.TSQL, TSQL);
