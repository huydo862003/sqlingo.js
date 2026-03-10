import { cache } from '../port_internals';
import {
  Generator,
} from '../generator';
import {
  Parser, buildCoalesce,
} from '../parser';
import type { TokenPair } from '../tokens';
import {
  Tokenizer, TokenType,
} from '../tokens';
import type {
  Expression,
  ColumnDefExpr,
  IntervalExpr,
  CoalesceExpr,
  HintExpr,
  IsAsciiExpr,
  OffsetExpr,
} from '../expressions';
import {
  IdentifierExpr,
  JsonExistsExpr,
  TimeToStrExpr,
  PropertiesLocation,
  VolatilePropertyExpr,
  PseudocolumnExpr,
  ConvertToCharsetExpr,
  EuclideanDistanceExpr,
  PowExpr,
  LiteralExpr,
  StrToTimeExpr,
  AnonymousExpr,
  StrToDateExpr,
  PriorExpr,
  CurrentTimestampExpr,
  SystimestampExpr,
  TemporaryPropertyExpr,
  ForcePropertyExpr,
  QueryOptionExpr,
  DateStrToDateExpr,
  RandExpr,
  IntoExpr,
  TableExpr,
  TableAliasExpr,
  DataTypeExprKind,
  DateTruncExpr,
  ILikeExpr,
  LogicalOrExpr,
  LogicalAndExpr,
  ModExpr,
  SelectExpr,
  StrPositionExpr,
  SubstringExpr,
  TableSampleExpr,
  ToCharExpr,
  ToNumberExpr,
  TrimExpr,
  UnicodeExpr,
  UnixToTimeExpr,
  UtcTimestampExpr,
  UtcTimeExpr,
  InOutColumnConstraintExpr,
  IntervalSpanExpr,
  SubqueryExpr,
  literal,
  JsonArrayAggExpr,
  JsonArrayExpr,
} from '../expressions';
import { seqGet } from '../helper';
import {
  eliminateDistinctOn, eliminateQualify, preprocess,
} from '../transforms';
import {
  buildFormattedTime,
  buildTimeToStrOrToChar,
  buildTrunc,
  noIlikeSql,
  strPositionSql,
  toNumberWithNlsParam,
  trimSql,
  Dialect, NormalizationStrategy, Dialects,
  renameFunc,
  NullOrdering,
} from './dialect';

function trimSqlEx (this: Generator, expression: TrimExpr): string {
  const position = expression.args.position;

  if (position && ['LEADING', 'TRAILING'].includes(position.toString().toUpperCase())) {
    return this.trimSql(expression);
  }

  return trimSql.call(this, expression);
}

function buildToTimestamp (args: Expression[]): StrToTimeExpr | AnonymousExpr {
  if (args.length === 1) {
    return new AnonymousExpr({
      this: 'TO_TIMESTAMP',
      expressions: args,
    });
  }

  return buildFormattedTime(StrToTimeExpr, { dialect: Dialects.ORACLE })(args);
}

export class OracleTokenizer extends Tokenizer {
  @cache
  static get VAR_SINGLE_TOKENS () {
    return new Set([
      '@',
      '$',
      '#',
    ]);
  }

  @cache
  static get UNICODE_STRINGS (): TokenPair[] {
    const quotes = Tokenizer.QUOTES as string[];
    const results: [string, string][] = [];
    for (const q of quotes) {
      for (const prefix of ['U', 'u']) {
        results.push([prefix + q, q]);
      }
    }
    return results;
  }

  static NESTED_COMMENTS = false;

  @cache
  static get ORIGINAL_KEYWORDS (): Record<string, TokenType> {
    return {
      ...Tokenizer.KEYWORDS,
      '(+)': TokenType.JOIN_MARKER,
      'BINARY_DOUBLE': TokenType.DOUBLE,
      'BINARY_FLOAT': TokenType.FLOAT,
      'BULK COLLECT INTO': TokenType.BULK_COLLECT_INTO,
      'COLUMNS': TokenType.COLUMN,
      'MATCH_RECOGNIZE': TokenType.MATCH_RECOGNIZE,
      'MINUS': TokenType.EXCEPT,
      'NVARCHAR2': TokenType.NVARCHAR,
      'ORDER SIBLINGS BY': TokenType.ORDER_SIBLINGS_BY,
      'SAMPLE': TokenType.TABLE_SAMPLE,
      'START': TokenType.BEGIN,
      'TOP': TokenType.TOP,
      'VARCHAR2': TokenType.VARCHAR,
      'SYSTIMESTAMP': TokenType.SYSTIMESTAMP,
    };
  }
}

export class OracleParser extends Parser {
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
    const noParenFunctions = {
      [TokenType.SYSTIMESTAMP]: SystimestampExpr,
      ...Parser.NO_PAREN_FUNCTIONS,
    };
    delete noParenFunctions[TokenType.LOCALTIME];
    return noParenFunctions;
  }

  @cache
  static get WINDOW_BEFORE_PAREN_TOKENS () {
    return new Set([TokenType.OVER, TokenType.KEEP]);
  }

  static VALUES_FOLLOWED_BY_PAREN = false;

  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> {
    return {
      ...Parser.FUNCTIONS,
      CONVERT: (args: Expression[]) => ConvertToCharsetExpr.fromArgList(args),
      L2_DISTANCE: (args: Expression[]) => EuclideanDistanceExpr.fromArgList(args),
      NVL: (args: Expression[]) => buildCoalesce(args, { isNvl: true }),
      SQUARE: (args: Expression[]) => new PowExpr({
        this: seqGet(args, 0),
        expression: literal(2),
      }),
      TO_CHAR: buildTimeToStrOrToChar,
      TO_TIMESTAMP: buildToTimestamp,
      TO_DATE: buildFormattedTime(StrToDateExpr, { dialect: Dialects.ORACLE }),
      TRUNC: (args: Expression[], { dialect }: { dialect: Dialect }) => buildTrunc(args, {
        dialect,
        dateTruncUnabbreviate: false,
        defaultDateTruncUnit: 'DD',
      }),
    };
  }

  @cache
  static get NO_PAREN_FUNCTION_PARSERS (): Record<string, (this: Parser) => Expression | undefined> {
    return {
      ...Parser.NO_PAREN_FUNCTION_PARSERS,
      NEXT: function (this: Parser) {
        return this.parseNextValueFor();
      },
      PRIOR: function (this: Parser) {
        return this.expression(PriorExpr, { this: this.parseBitwise() });
      },
      SYSDATE: function (this: Parser) {
        return this.expression(CurrentTimestampExpr, { sysdate: true });
      },
      DBMS_RANDOM: function (this: Parser) {
        return (this as OracleParser).parseDbmsRandom();
      },
    };
  }

  @cache
  static get FUNCTION_PARSERS (): Record<string, (this: Parser) => Expression | undefined> {
    return {
      ...Parser.FUNCTION_PARSERS,
      CONVERT: function (this: Parser) {
        // Oracle CONVERT(expr, dest_charset[, source_charset]) is different from SQL CONVERT
        // Parse as regular function arguments instead of using parseConvert
        const args = this.parseCsv(() => this.parseBitwise());
        return ConvertToCharsetExpr.fromArgList(args);
      },
      JSON_ARRAY: function (this: Parser) {
        return (this as OracleParser).parseJsonArray(
          JsonArrayExpr,
          { expressions: this.parseCsv(() => this.parseFormatJson(this.parseBitwise())) },
        );
      },
      JSON_ARRAYAGG: function (this: Parser) {
        return (this as OracleParser).parseJsonArray(
          JsonArrayAggExpr,
          {
            this: this.parseFormatJson(this.parseBitwise()),
            order: this.parseOrder(),
          },
        );
      },
      JSON_EXISTS: function (this: Parser) {
        return (this as OracleParser).parseJsonExists();
      },
    };
  }

  @cache
  static get PROPERTY_PARSERS (): Record<string, (this: Parser) => Expression | undefined> {
    return {
      ...Parser.PROPERTY_PARSERS,
      GLOBAL: function (this: Parser) {
        return (this.matchTextSeq('TEMPORARY') || undefined)
          && this.expression(TemporaryPropertyExpr, { this: 'GLOBAL' });
      },
      PRIVATE: function (this: Parser) {
        return (this.matchTextSeq('TEMPORARY') || undefined)
          && this.expression(TemporaryPropertyExpr, { this: 'PRIVATE' });
      },
      FORCE: function (this: Parser) {
        return this.expression(ForcePropertyExpr, {});
      },
    };
  }

  @cache
  static get QUERY_MODIFIER_PARSERS (): Partial<Record<TokenType, (this: Parser) => [string, Expression | Expression[] | undefined]>> {
    return {
      ...Parser.QUERY_MODIFIER_PARSERS,
      [TokenType.ORDER_SIBLINGS_BY]: function (this: Parser) {
        return ['order', this.parseOrder()];
      },
      [TokenType.WITH]: function (this: Parser) {
        return ['options', (this as OracleParser).parseQueryRestrictions()];
      },
    };
  }

  @cache
  static get TYPE_LITERAL_PARSERS (): Partial<Record<DataTypeExprKind, (this: Parser, thisArg?: Expression, _?: unknown) => Expression>> {
    return {
      [DataTypeExprKind.DATE]: function (this: Parser, thisExpr?: Expression) {
        return this.expression(DateStrToDateExpr, { this: thisExpr });
      },
      [DataTypeExprKind.TIMESTAMP]: function (this: Parser, thisExpr?: Expression) {
        return buildToTimestamp(
          [thisExpr ?? literal(''), literal('%Y-%m-%d %H:%M:%S.%f')],
        );
      },
    };
  }

  @cache
  static get DISTINCT_TOKENS () {
    return new Set([TokenType.DISTINCT, TokenType.UNIQUE]);
  }

  @cache
  static get QUERY_RESTRICTIONS () {
    return {
      WITH: [['READ', 'ONLY'], ['CHECK', 'OPTION']],
    };
  }

  public parseDbmsRandom (): Expression | undefined {
    if (this.matchTextSeq(['.', 'VALUE'])) {
      let lower: Expression | undefined;
      let upper: Expression | undefined;
      if (this.match(TokenType.L_PAREN, { advance: false })) {
        const lowerUpper = this.parseWrappedCsv(() => this.parseBitwise());
        if (lowerUpper.length === 2) {
          lower = lowerUpper[0];
          upper = lowerUpper[1];
        }
      }

      return new RandExpr({
        lower,
        upper,
      });
    }

    this.retreat(this.index - 1);
    return undefined;
  }

  public parseJsonArray (exprType: typeof Expression, options: Record<string, unknown> = {}): Expression {
    return this.expression(
      exprType,
      {
        nullHandling: this.parseOnHandling('NULL', ['NULL', 'ABSENT']),
        returnType: this.matchTextSeq('RETURNING') && this.parseType(),
        strict: this.matchTextSeq('STRICT'),
        ...options,
      },
    );
  }

  public parseQueryRestrictions (): Expression | undefined {
    const kind = this.parseVarFromOptions(OracleParser.QUERY_RESTRICTIONS, { raiseUnmatched: false });

    if (!kind) {
      return undefined;
    }

    return this.expression(
      QueryOptionExpr,
      {
        this: kind,
        expression: this.match(TokenType.CONSTRAINT) && this.parseField(),
      },
    );
  }

  public parseJsonExists (): JsonExistsExpr {
    const thisExpr = this.parseFormatJson(this.parseBitwise());
    this.match(TokenType.COMMA);
    return this.expression(
      JsonExistsExpr,
      {
        this: thisExpr,
        path: this.dialect.toJsonPath(this.parseBitwise()),
        passing: (this.matchTextSeq('PASSING') || undefined)
          && this.parseCsv(() => this.parseAlias(this.parseBitwise())),
        onCondition: this.parseOnCondition(),
      },
    );
  }

  public parseInto (): IntoExpr | undefined {
    const bulkCollect = this.match(TokenType.BULK_COLLECT_INTO);
    if (!bulkCollect && !this.match(TokenType.INTO)) {
      return undefined;
    }

    const index = this.index;

    const expressions = this.parseExpressions();
    if (expressions.length === 1) {
      this.retreat(index);
      this.match(TokenType.TABLE);
      return this.expression(IntoExpr, {
        this: this.parseTable({ schema: true }),
        bulkCollect,
      });
    }

    return this.expression(IntoExpr, {
      bulkCollect,
      expressions,
    });
  }

  public parseHintFunctionCall (): Expression | undefined {
    if (!this.curr || !this.next || this.next.tokenType !== TokenType.L_PAREN) {
      return undefined;
    }

    const thisText = this.curr.text;
    this.advance(2);
    const args = this.parseHintArgs();
    const expr = this.expression(AnonymousExpr, {
      this: thisText,
      expressions: args,
    });
    this.matchRParen(expr);
    return expr;
  }

  public parseHintArgs (): Expression[] {
    const args: Expression[] = [];
    let result = this.parseVar();

    while (result) {
      args.push(result);
      result = this.parseVar();
    }

    return args;
  }

  public parseConnectWithPrior (): Expression | undefined {
    return this.parseAssignment();
  }

  public parseColumnOps (thisExpr: Expression | undefined): Expression | undefined {
    const result = super.parseColumnOps(thisExpr);

    if (!result) {
      return result;
    }

    const index = this.index;
    const intervalSpan = this.parseIntervalSpan(result);
    if (intervalSpan.args.unit instanceof IntervalSpanExpr) {
      return intervalSpan;
    }

    this.retreat(index);
    return result;
  }

  public parseInsertTable (): Expression | undefined {
    const thisExpr = this.parseTableParts({ schema: true });

    if (thisExpr instanceof TableExpr) {
      const aliasName = this.parseIdVar({ anyToken: false });
      if (aliasName instanceof IdentifierExpr) {
        thisExpr.setArgKey('alias', new TableAliasExpr({ this: aliasName }));
      }

      thisExpr.setArgKey('partition', this.parsePartition());

      return this.parseSchema({ this: thisExpr });
    }

    return thisExpr;
  }

  // port from _Dialect metaclass logic
  @cache
  static get TABLE_ALIAS_TOKENS (): Set<TokenType> {
    return new Set([...Parser.TABLE_ALIAS_TOKENS, TokenType.STRAIGHT_JOIN]);
  }
}
export class OracleGenerator extends Generator {
  @cache
  static get AFTER_HAVING_MODIFIER_TRANSFORMS () {
    const modifiers = new Map(super.AFTER_HAVING_MODIFIER_TRANSFORMS);
    [
      'cluster',
      'distribute',
      'sort',
    ].forEach((m) => modifiers.delete(m));
    return modifiers;
  }

  // port from _Dialect metaclass logic
  static readonly SELECT_KINDS: string[] = [];
  // port from _Dialect metaclass logic
  static TRY_SUPPORTED = false;
  // port from _Dialect metaclass logic
  static SUPPORTS_UESCAPE = false;
  static LOCKING_READS_SUPPORTED = true;
  static JOIN_HINTS = false;
  static TABLE_HINTS = false;
  static DATA_TYPE_SPECIFIERS_ALLOWED = true;
  static ALTER_TABLE_INCLUDE_COLUMN_KEYWORD = false;
  static LIMIT_FETCH = 'FETCH';
  static TABLESAMPLE_KEYWORDS = 'SAMPLE';
  static LAST_DAY_SUPPORTS_DATE_PART = false;
  static SUPPORTS_SELECT_INTO = true;
  static TZ_TO_WITH_TIME_ZONE = true;
  static SUPPORTS_WINDOW_EXCLUDE = true;
  static QUERY_HINT_SEP = ' ';
  static SUPPORTS_DECODE_CASE = true;

  @cache
  static get TYPE_MAPPING (): Map<DataTypeExprKind | string, string> {
    const mapping = new Map(Generator.TYPE_MAPPING);
    mapping.set(DataTypeExprKind.TINYINT, 'SMALLINT');
    mapping.set(DataTypeExprKind.SMALLINT, 'SMALLINT');
    mapping.set(DataTypeExprKind.INT, 'INT');
    mapping.set(DataTypeExprKind.BIGINT, 'INT');
    mapping.set(DataTypeExprKind.DECIMAL, 'NUMBER');
    mapping.set(DataTypeExprKind.DOUBLE, 'DOUBLE PRECISION');
    mapping.set(DataTypeExprKind.VARCHAR, 'VARCHAR2');
    mapping.set(DataTypeExprKind.NVARCHAR, 'NVARCHAR2');
    mapping.set(DataTypeExprKind.NCHAR, 'NCHAR');
    mapping.set(DataTypeExprKind.TEXT, 'CLOB');
    mapping.set(DataTypeExprKind.TIMETZ, 'TIME');
    mapping.set(DataTypeExprKind.TIMESTAMPNTZ, 'TIMESTAMP');
    mapping.set(DataTypeExprKind.TIMESTAMPTZ, 'TIMESTAMP');
    mapping.set(DataTypeExprKind.BINARY, 'BLOB');
    mapping.set(DataTypeExprKind.VARBINARY, 'BLOB');
    mapping.set(DataTypeExprKind.ROWVERSION, 'BLOB');
    mapping.delete(DataTypeExprKind.BLOB);
    return mapping;
  }

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (this: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const m = new Map<typeof Expression, (this: Generator, e: any) => string>(Generator.TRANSFORMS);
    m.set(DateStrToDateExpr, function (this: Generator, e: DateStrToDateExpr) {
      return this.func('TO_DATE', [e.args.this, literal('YYYY-MM-DD')]);
    });
    m.set(DateTruncExpr, function (this: Generator, e: DateTruncExpr) {
      return this.func('TRUNC', [e.args.this, e.args.unit]);
    });
    m.set(EuclideanDistanceExpr, renameFunc('L2_DISTANCE'));
    m.set(ILikeExpr, noIlikeSql);
    m.set(LogicalOrExpr, renameFunc('MAX'));
    m.set(LogicalAndExpr, renameFunc('MIN'));
    m.set(ModExpr, renameFunc('MOD'));
    m.set(RandExpr, renameFunc('DBMS_RANDOM.VALUE'));
    m.set(SelectExpr, preprocess([eliminateDistinctOn, eliminateQualify]));
    m.set(StrPositionExpr, function (this: Generator, e: StrPositionExpr) {
      return (
        strPositionSql.call(this, e, {
          funcName: 'INSTR',
          supportsPosition: true,
          supportsOccurrence: true,
        })
      );
    });
    m.set(StrToTimeExpr, function (this: Generator, e: StrToTimeExpr) {
      return this.func('TO_TIMESTAMP', [e.args.this, this.formatTime(e)]);
    });
    m.set(StrToDateExpr, function (this: Generator, e: StrToDateExpr) {
      return this.func('TO_DATE', [e.args.this, this.formatTime(e)]);
    });
    m.set(SubqueryExpr, function (this: Generator, e: SubqueryExpr) {
      return this.subquerySql(e, { sep: ' ' });
    });
    m.set(SubstringExpr, renameFunc('SUBSTR'));
    m.set(TableExpr, function (this: Generator, e: TableExpr) {
      return this.tableSql(e, { sep: ' ' });
    });
    m.set(TableSampleExpr, function (this: Generator, e: TableSampleExpr) {
      return this.tableSampleSql(e);
    });
    m.set(TemporaryPropertyExpr, (e: TemporaryPropertyExpr) => `${e.args.this || 'GLOBAL'} TEMPORARY`);
    m.set(TimeToStrExpr, function (this: Generator, e: TimeToStrExpr) {
      return this.func('TO_CHAR', [e.args.this, this.formatTime(e)]);
    });
    m.set(ToCharExpr, function (this: Generator, e: ToCharExpr) {
      return this.functionFallbackSql(e);
    });
    m.set(ToNumberExpr, toNumberWithNlsParam);
    m.set(TrimExpr, trimSqlEx);
    m.set(UnicodeExpr, function (this: Generator, e: UnicodeExpr) {
      return `ASCII(UNISTR(${this.sql(e.args.this)}))`;
    });
    m.set(UnixToTimeExpr, function (this: Generator, e: UnixToTimeExpr) {
      return `TO_DATE('1970-01-01', 'YYYY-MM-DD') + (${this.sql(e, 'this')} / 86400)`;
    });
    m.set(UtcTimestampExpr, renameFunc('UTC_TIMESTAMP'));
    m.set(UtcTimeExpr, renameFunc('UTC_TIME'));
    m.set(SystimestampExpr, () => 'SYSTIMESTAMP');
    return m;
  }

  @cache
  static get PROPERTIES_LOCATION (): Map<typeof Expression, PropertiesLocation> {
    const m = new Map(Generator.PROPERTIES_LOCATION);
    m.set(VolatilePropertyExpr, PropertiesLocation.UNSUPPORTED);
    return m;
  }

  public currentTimestampSql (expression: CurrentTimestampExpr): string {
    if (expression.args.sysdate) {
      return 'SYSDATE';
    }

    const thisExpr = expression.args.this;
    return thisExpr ? this.func('CURRENT_TIMESTAMP', [thisExpr]) : 'CURRENT_TIMESTAMP';
  }

  public offsetSql (expression: OffsetExpr): string {
    return `${super.offsetSql(expression)} ROWS`;
  }

  public addColumnSql (expression: Expression): string {
    return `ADD ${this.sql(expression)}`;
  }

  public queryOptionSql (expression: QueryOptionExpr): string {
    const option = this.sql(expression, 'this');
    let value = this.sql(expression, 'expression');
    value = value ? ` CONSTRAINT ${value}` : '';

    return `${option}${value}`;
  }

  public coalesceSql (expression: CoalesceExpr): string {
    const funcName = expression.args.isNvl ? 'NVL' : 'COALESCE';
    return renameFunc(funcName).call(this, expression);
  }

  public intoSql (expression: IntoExpr): string {
    const into = !expression.args.bulkCollect ? 'INTO' : 'BULK COLLECT INTO';
    if (expression.args.this) {
      return `${this.seg(into)} ${this.sql(expression, 'this')}`;
    }

    return `${this.seg(into)} ${this.expressions(expression)}`;
  }

  public hintSql (expression: HintExpr): string {
    const expressions: string[] = [];

    for (const e of expression.args.expressions || []) {
      if (e instanceof AnonymousExpr) {
        const formattedArgs = this.formatArgs(e.args.expressions || [], { sep: ' ' });
        expressions.push(`${this.sql(e, 'this')}(${formattedArgs})`);
      } else {
        expressions.push(this.sql(e));
      }
    }

    return ` /*+ ${this.expressions(undefined, {
      sqls: expressions,
      sep: this._constructor.QUERY_HINT_SEP,
    }).trim()} */`;
  }

  public isAsciiSql (expression: IsAsciiExpr): string {
    return `NVL(REGEXP_LIKE(${this.sql(expression.args.this)}, '^[' || CHR(1) || '-' || CHR(127) || ']*$'), TRUE)`;
  }

  public intervalSql (expression: IntervalExpr): string {
    return `${expression.args.this instanceof LiteralExpr ? 'INTERVAL ' : ''}${this.sql(expression, 'this')} ${this.sql(expression, 'unit')}`;
  }

  public columnDefSql (expression: ColumnDefExpr, options: { sep?: string } = {}): string {
    let { sep = ' ' } = options;
    const paramConstraint = expression.find(InOutColumnConstraintExpr);
    if (paramConstraint) {
      sep = ` ${this.sql(paramConstraint)} `;
      paramConstraint.parent?.pop();
    }
    return super.columnDefSql(expression, { sep });
  }
}

export class Oracle extends Dialect {
  static DIALECT_NAME = Dialects.ORACLE;
  static ALIAS_POST_TABLESAMPLE = true;
  static LOCKING_READS_SUPPORTED = true;
  static TABLESAMPLE_SIZE_IS_PERCENT = true;

  @cache
  static get NULL_ORDERING () {
    return NullOrdering.NULLS_ARE_LARGE;
  }

  static ON_CONDITION_EMPTY_BEFORE_ERROR = false;
  static ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN = false;
  static DISABLES_ALIAS_REF_EXPANSION = true;

  @cache
  static get NORMALIZATION_STRATEGY () {
    return NormalizationStrategy.UPPERCASE;
  }

  @cache
  static get TIME_MAPPING () {
    return {
      D: '%u',
      DAY: '%A',
      DD: '%d',
      DDD: '%j',
      DY: '%a',
      HH: '%I',
      HH12: '%I',
      HH24: '%H',
      IW: '%V',
      MI: '%M',
      MM: '%m',
      MON: '%b',
      MONTH: '%B',
      SS: '%S',
      WW: '%W',
      YY: '%y',
      YYYY: '%Y',
      FF6: '%f',
    };
  }

  @cache
  static get PSEUDOCOLUMNS () {
    return new Set([
      'ROWNUM',
      'ROWID',
      'OBJECT_ID',
      'OBJECT_VALUE',
      'LEVEL',
    ]);
  }

  public canQuote (identifier: IdentifierExpr, options: { identify?: string | boolean } = {}): boolean {
    const { identify = 'safe' } = options;

    return (
      identifier.args.quoted || !(identifier.parent instanceof PseudocolumnExpr)
    ) && super.canQuote(identifier, { identify });
  }

  static Tokenizer = OracleTokenizer;
  static Parser = OracleParser;
  static Generator = OracleGenerator;
}

Dialect.register(Dialects.ORACLE, Oracle);
