import {
  Generator, unsupportedArgs,
} from '../generator';
import type {
  GeneratedAsRowColumnConstraintExpr, PrimaryKeyColumnConstraintExpr, PrimaryKeyExpr,
  ArrayExpr,
  ArrayContainsAllExpr,
  DPipeExpr,
  ExtractExpr,
  TimestampTruncExpr,
  AtTimeZoneExpr,
  IsAsciiExpr,
  IgnoreNullsExpr,
  AlterRenameExpr,
  AlterColumnExpr,
  FuncExpr,
} from '../expressions';
import {
  AlterIndexExpr, ColumnExpr, PartitionExpr, PartitionListExpr, PartitionRangeExpr,
  BitwiseAndAggExpr,
  BitwiseCountExpr,
  BitwiseOrAggExpr,
  BitwiseXorAggExpr,
  ConvertTimezoneExpr,
  CurrentDateExpr,
  DateTruncExpr,
  EqExpr,
  JsonArrayContainsExpr,
  ShowExpr,
  SoundexExpr,
  TimeToStrExpr,
  TsOrDsToDateExpr,
  Expression,
  StrToDateExpr,
  StrToTimeExpr,
  UnixToTimeExpr,
  CastExpr,
  MaxExpr,
  MinExpr,
  StrPositionExpr,
  DateAddExpr,
  DateSubExpr,
  DataTypeExpr,
  DataTypeExprKind,
  LiteralExpr,
  TryCastExpr,
  TableSampleExpr,
  PivotExpr,
  ILikeExpr,
  DivExpr,
  func,
  AndExpr,
  XorExpr,
  OrExpr,
  DayExpr,
  DayOfMonthExpr,
  DayOfWeekExpr,
  DayOfYearExpr,
  NumberToStrExpr,
  LengthExpr,
  TimeFromPartsExpr,
  MonthExpr,
  CurrentSchemaExpr,
  TimestampDiffExpr,
  DateDiffExpr,
  VarExpr,
  CurrentVersionExpr,
  WeekExpr,
  WeekOfYearExpr,
  YearExpr,
  AnonymousExpr,
  SetItemExpr,
  LockPropertyExpr,
  PartitionByRangePropertyExpr,
  PartitionByListPropertyExpr,
  SetItemExprKind,
  ZeroFillColumnConstraintExpr,
  GeneratedAsIdentityColumnConstraintExpr,
  ComputedColumnConstraintExpr,
  ColumnPrefixExpr,
  IndexColumnConstraintExpr,
  IndexConstraintOptionExpr,
  ArrayAggExpr,
  ChrExpr,
  IntervalExpr,
  PropertiesLocation,
  TransientPropertyExpr,
  VolatilePropertyExpr,
  PartitionedByPropertyExpr,
  SelectExpr,
  GroupConcatExpr,
  LogicalOrExpr,
  LogicalAndExpr,
  NullSafeEqExpr,
  NullSafeNeqExpr,
  JsonExtractScalarExpr,
  StuffExpr,
  SessionUserExpr,
  TimestampAddExpr,
  TimestampSubExpr,
  TimeStrToUnixExpr,
  TimeStrToTimeExpr,
  TrimExpr,
  TruncExpr,
  TsOrDsAddExpr,
  TsOrDsDiffExpr,
  UnicodeExpr,
  UtcTimestampExpr,
  UtcTimeExpr,
  DateStrToDateExpr,
} from '../expressions';
import {
  EXPRESSION_METADATA as BASE_EXPRESSION_METADATA, type ExpressionMetadata,
} from '../typing';
import type { TokenPair } from '../tokens';
import {
  Tokenizer, TokenType,
} from '../tokens';
import { seqGet } from '../helper';
import { Parser } from '../parser';
import {
  eliminateDistinctOn, eliminateFullOuterJoin, eliminateQualify, eliminateSemiAndAntiJoins, preprocess, unnestGenerateDateArrayUsingRecursiveCte,
} from '../transforms';
import {
  isnullToIsNull,
  lengthOrCharLengthSql,
  maxOrGreatest,
  minOrLeast,
  noIlikeSql,
  noParenCurrentDateSql,
  noPivotSql,
  noTablesampleSql,
  noTrycastSql,
  strPositionSql,
  trimSql,
  arrowJsonExtractSql,
  dateAddIntervalSql,
  dateStrToDateSql,
  timeStrToTimeSql,
  unitToVar,
  renameFunc,
  NormalizationStrategy,
  Dialect,
  buildDateDeltaWithInterval,
  Dialects,
  buildFormattedTime,
  buildDateDelta,
} from './dialect';

/**
 * A higher-order function that returns a parser for MySQL SHOW statements.
 * @param args - Arguments to pass to the internal MySQL SHOW parser.
 * @param kwargs - Keyword arguments to pass to the internal MySQL SHOW parser.
 * @returns A function that takes a MySQL Parser and returns a ShowExpr.
 */

export function showParser (
  thisArg: string,
  options: {
    target?: boolean | string;
    full?: boolean;
    global?: boolean;
  } = {},
): (self: Parser) => ShowExpr {
  return (self: Parser): ShowExpr => {
    return (self as MySQLParser).parseShowMysql(thisArg, options);
  };
}

/**
 * Transpiles the DATE_TRUNC expression into MySQL-compatible SQL.
 * Since MySQL lacks a native DATE_TRUNC, this uses string concatenation and conversion.
 * @param self - The MySQL Generator instance.
 * @param expression - The DateTruncExpr node to transpile.
 * @returns The generated SQL string.
 */
export function dateTruncSql (self: Generator, expression: DateTruncExpr): string {
  const expr = self.sql(expression, 'this');
  const unit = expression.text('unit').toUpperCase();

  let concat: string;
  let dateFormat: string;

  if (unit === 'WEEK') {
    concat = `CONCAT(YEAR(${expr}), ' ', WEEK(${expr}, 1), ' 1')`;
    dateFormat = '%Y %u %w';
  } else if (unit === 'MONTH') {
    concat = `CONCAT(YEAR(${expr}), ' ', MONTH(${expr}), ' 1')`;
    dateFormat = '%Y %c %e';
  } else if (unit === 'QUARTER') {
    concat = `CONCAT(YEAR(${expr}), ' ', QUARTER(${expr}) * 3 - 2, ' 1')`;
    dateFormat = '%Y %c %e';
  } else if (unit === 'YEAR') {
    concat = `CONCAT(YEAR(${expr}), ' 1 1')`;
    dateFormat = '%Y %c %e';
  } else {
    if (unit !== 'DAY') {
      self.unsupported(`Unexpected interval unit: ${unit}`);
    }
    return self.func('DATE', [expr]);
  }

  return self.func('STR_TO_DATE', [concat, `'${dateFormat}'`]);
}

/**
 * All specifiers for time parts (as opposed to date parts)
 * Reference: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format
 */
export const TIME_SPECIFIERS = new Set([
  'f',
  'H',
  'h',
  'I',
  'i',
  'k',
  'l',
  'p',
  'r',
  'S',
  's',
  'T',
]);

/**
 * Checks if a MySQL date format string contains any time-specific specifiers.
 */
function hasTimeSpecifier (dateFormat: string): boolean {
  let i = 0;
  const length = dateFormat.length;

  while (i < length) {
    if (dateFormat[i] === '%') {
      i += 1;
      if (i < length && TIME_SPECIFIERS.has(dateFormat[i])) {
        return true;
      }
    }
    i += 1;
  }
  return false;
}

/**
 * Parser builder for STR_TO_DATE. Decides whether to return a StrToDateExpr or StrToTimeExpr
 * based on the presence of time specifiers in the format string.
 */
export function buildStrToDate (args: [Expression, ...(string | Expression | undefined)[]]): StrToDateExpr | StrToTimeExpr {
  const mysqlDateFormat = seqGet(args, 1);
  const dateFormat = MySQL.formatTime(mysqlDateFormat);
  const thisArg = seqGet(args, 0) as Expression;

  if (mysqlDateFormat && hasTimeSpecifier(mysqlDateFormat instanceof Expression ? mysqlDateFormat.name : mysqlDateFormat)) {
    return new StrToTimeExpr({
      this: thisArg,
      format: dateFormat || '',
    });
  }

  return new StrToDateExpr({
    this: thisArg,
    format: dateFormat,
  });
}

/**
 * Generator for STR_TO_DATE.
 */
export function strToDateSql (
  self: Generator,
  expression: StrToDateExpr | StrToTimeExpr | TsOrDsToDateExpr,
): string {
  return self.func('STR_TO_DATE', [expression.args.this, self.formatTime(expression)]);
}

/**
 * Generator for UNIX_TO_TIME. Handles optional scale for sub-second precision.
 */
export function unixToTimeSql (self: Generator, expression: UnixToTimeExpr): string {
  const scale = expression.args.scale;
  const timestamp = expression.args.this;

  if (scale === undefined || scale === UnixToTimeExpr.SECONDS) {
    return self.func('FROM_UNIXTIME', [timestamp, self.formatTime(expression)!]);
  }

  return self.func(
    'FROM_UNIXTIME',
    [
      new DivExpr({
        this: timestamp,
        expression: func('POW', '10', scale.toString()),
      }),
      self.formatTime(expression),
    ],
  );
}

/**
 * Higher-order function to generate MySQL DATE_ADD or DATE_SUB calls.
 * @param kind - Either 'ADD' or 'SUB'.
 * @returns A function that generates the MySQL-specific DATE logic.
 */
export function dateAddSql (kind: string): (self: Generator, expression: DateAddExpr) => string {
  return (self: Generator, expression: DateAddExpr): string => {
    return self.func(
      `DATE_${kind}`,
      [
        expression.args.this,
        new IntervalExpr({
          this: expression.args.expression,
          unit: unitToVar(expression),
        }),
      ],
    );
  };
}

/**
 * Handles converting a Timestamp or DateString to a Date in MySQL.
 * @param self - The MySQL Generator.
 * @param expression - The TsOrDsToDateExpr node.
 */
export function tsOrDsToDateSql (self: Generator, expression: TsOrDsToDateExpr): string {
  const timeFormat = expression.args.format;
  return timeFormat
    ? strToDateSql(self, expression)
    : self.func('DATE', [expression.args.this]);
}

/**
 * Optimization that removes redundant TsOrDsToDate wrappers when the parent
 * function can handle raw types.
 */
export function removeTsOrDsToDate<T extends FuncExpr> (
  toSql?: (self: Generator, expression: T) => string,
  args: string[] = ['this'],
): (self: Generator, expression: T) => string {
  return (self: Generator, expression: T): string => {
    for (const argKey of args) {
      const arg = expression.getArgKey(argKey);
      if (arg instanceof TsOrDsToDateExpr && !arg.args.format) {
        expression.setArgKey(argKey, arg.args.this);
      }
    }

    return toSql ? toSql(self, expression) : self.functionFallbackSql(expression);
  };
}

class MySQLTokenizer extends Tokenizer {
  public QUOTES = ['\'', '"'];
  public COMMENTS: TokenPair[] = [
    '--',
    '#',
    ['/*', '*/'],
  ];

  public IDENTIFIERS = ['`'];
  public STRING_ESCAPES = [
    '\'',
    '"',
    '\\',
  ];

  public BIT_STRINGS: [string, string][] = [
    ['b\'', '\''],
    ['B\'', '\''],
    ['0b', ''],
  ];

  public HEX_STRINGS: [string, string][] = [
    ['x\'', '\''],
    ['X\'', '\''],
    ['0x', ''],
  ];

  /**
     * Special characters that are recognized after an escape character (\) in MySQL.
     * Reference: https://dev.mysql.com/doc/refman/8.4/en/string-literals.html
     */
  public ESCAPE_FOLLOW_CHARS = [
    '0',
    'b',
    'n',
    'r',
    't',
    'Z',
    '%',
    '_',
  ];

  public NESTED_COMMENTS = false;

  static ORIGINAL_KEYWORDS: Record<string, TokenType> = {
    ...Tokenizer.KEYWORDS,
    'BLOB': TokenType.BLOB,
    'CHARSET': TokenType.CHARACTER_SET,
    'DISTINCTROW': TokenType.DISTINCT,
    'EXPLAIN': TokenType.DESCRIBE,
    'FORCE': TokenType.FORCE,
    'IGNORE': TokenType.IGNORE,
    'KEY': TokenType.KEY,
    'LOCK TABLES': TokenType.COMMAND,
    'LONGBLOB': TokenType.LONGBLOB,
    'LONGTEXT': TokenType.LONGTEXT,
    'MEDIUMBLOB': TokenType.MEDIUMBLOB,
    'MEDIUMINT': TokenType.MEDIUMINT,
    'MEDIUMTEXT': TokenType.MEDIUMTEXT,
    'MEMBER OF': TokenType.MEMBER_OF,
    'MOD': TokenType.MOD,
    'SEPARATOR': TokenType.SEPARATOR,
    'SERIAL': TokenType.SERIAL,
    'SIGNED': TokenType.BIGINT,
    'SIGNED INTEGER': TokenType.BIGINT,
    'SOUNDS LIKE': TokenType.SOUNDS_LIKE,
    'START': TokenType.BEGIN,
    'TIMESTAMP': TokenType.TIMESTAMPTZ,
    'TINYBLOB': TokenType.TINYBLOB,
    'TINYTEXT': TokenType.TINYTEXT,
    'UNLOCK TABLES': TokenType.COMMAND,
    'UNSIGNED': TokenType.UBIGINT,
    'UNSIGNED INTEGER': TokenType.UBIGINT,
    'YEAR': TokenType.YEAR,
    '_ARMSCII8': TokenType.INTRODUCER,
    '_ASCII': TokenType.INTRODUCER,
    '_BIG5': TokenType.INTRODUCER,
    '_BINARY': TokenType.INTRODUCER,
    '_CP1250': TokenType.INTRODUCER,
    '_CP1251': TokenType.INTRODUCER,
    '_CP1256': TokenType.INTRODUCER,
    '_CP1257': TokenType.INTRODUCER,
    '_CP850': TokenType.INTRODUCER,
    '_CP852': TokenType.INTRODUCER,
    '_CP866': TokenType.INTRODUCER,
    '_CP932': TokenType.INTRODUCER,
    '_DEC8': TokenType.INTRODUCER,
    '_EUCJPMS': TokenType.INTRODUCER,
    '_EUCKR': TokenType.INTRODUCER,
    '_GB18030': TokenType.INTRODUCER,
    '_GB2312': TokenType.INTRODUCER,
    '_GBK': TokenType.INTRODUCER,
    '_GEOSTD8': TokenType.INTRODUCER,
    '_GREEK': TokenType.INTRODUCER,
    '_HEBREW': TokenType.INTRODUCER,
    '_HP8': TokenType.INTRODUCER,
    '_KEYBCS2': TokenType.INTRODUCER,
    '_KOI8R': TokenType.INTRODUCER,
    '_KOI8U': TokenType.INTRODUCER,
    '_LATIN1': TokenType.INTRODUCER,
    '_LATIN2': TokenType.INTRODUCER,
    '_LATIN5': TokenType.INTRODUCER,
    '_LATIN7': TokenType.INTRODUCER,
    '_MACCE': TokenType.INTRODUCER,
    '_MACROMAN': TokenType.INTRODUCER,
    '_SJIS': TokenType.INTRODUCER,
    '_SWE7': TokenType.INTRODUCER,
    '_TIS620': TokenType.INTRODUCER,
    '_UCS2': TokenType.INTRODUCER,
    '_UJIS': TokenType.INTRODUCER,
    '_UTF8': TokenType.INTRODUCER,
    '_UTF16': TokenType.INTRODUCER,
    '_UTF16LE': TokenType.INTRODUCER,
    '_UTF32': TokenType.INTRODUCER,
    '_UTF8MB3': TokenType.INTRODUCER,
    '_UTF8MB4': TokenType.INTRODUCER,
    '@@': TokenType.SESSION_PARAMETER,
  };

  public COMMANDS: Set<TokenType> = (() => {
    const commands = new Set([...Tokenizer.COMMANDS, TokenType.REPLACE]);
    commands.delete(TokenType.SHOW);
    return commands;
  })();
};

class MySQLParser extends Parser {
  public static FUNC_TOKENS: Set<TokenType> = new Set([
    ...Parser.FUNC_TOKENS,
    TokenType.DATABASE,
    TokenType.MOD,
    TokenType.SCHEMA,
    TokenType.VALUES,
    TokenType.CHARACTER_SET,
  ]);

  public static CONJUNCTION = {
    ...Parser.CONJUNCTION,
    [TokenType.DAMP]: AndExpr,
    [TokenType.XOR]: XorExpr,
  };

  public static DISJUNCTION = {
    ...Parser.DISJUNCTION,
    [TokenType.DPIPE]: OrExpr,
  };

  public static TABLE_ALIAS_TOKENS: Set<TokenType> = (() => {
    const tokens = new Set(Parser.TABLE_ALIAS_TOKENS);
    for (const hint of Parser.TABLE_INDEX_HINT_TOKENS) {
      tokens.delete(hint);
    }
    return tokens;
  })();

  public static RANGE_PARSERS = {
    ...Parser.RANGE_PARSERS,
    [TokenType.SOUNDS_LIKE]: (self: Parser, thisArg: Expression): EqExpr =>
      self.expression(EqExpr, {
        this: self.expression(SoundexExpr, { this: thisArg }),
        expression: self.expression(SoundexExpr, { this: self.parseTerm() }),
      }),
    [TokenType.MEMBER_OF]: (self: Parser, thisArg: Expression): JsonArrayContainsExpr =>
      self.expression(JsonArrayContainsExpr, {
        this: thisArg,
        expression: self.parseWrapped(self.parseExpression.bind(self)),
      }),
  };

  public static FUNCTIONS: typeof Parser.FUNCTIONS = {
    ...Parser.FUNCTIONS,
    BIT_AND: BitwiseAndAggExpr.fromArgList,
    BIT_OR: BitwiseOrAggExpr.fromArgList,
    BIT_XOR: BitwiseXorAggExpr.fromArgList,
    BIT_COUNT: BitwiseCountExpr.fromArgList,
    CONVERT_TZ: (args: Expression[]): ConvertTimezoneExpr =>
      new ConvertTimezoneExpr({
        sourceTz: seqGet(args, 1),
        targetTz: seqGet(args, 2),
        timestamp: seqGet(args, 0),
      }),
    CURDATE: CurrentDateExpr.fromArgList,
    DATE: (args: Expression[]): TsOrDsToDateExpr =>
      new TsOrDsToDateExpr({ this: seqGet(args, 0) }),
    DATE_ADD: (args: Expression[]) => buildDateDeltaWithInterval(DateAddExpr)(args)!,
    DATE_FORMAT: buildFormattedTime(TimeToStrExpr, { dialect: Dialects.MYSQL }),
    DATE_SUB: (args: Expression[]) => buildDateDeltaWithInterval(DateSubExpr)(args)!,
    DAY: (args: Expression[]): DayExpr =>
      new DayExpr({ this: new TsOrDsToDateExpr({ this: seqGet(args, 0) }) }),
    DAYOFMONTH: (args: Expression[]): DayOfMonthExpr =>
      new DayOfMonthExpr({ this: new TsOrDsToDateExpr({ this: seqGet(args, 0) }) }),
    DAYOFWEEK: (args: Expression[]): DayOfWeekExpr =>
      new DayOfWeekExpr({ this: new TsOrDsToDateExpr({ this: seqGet(args, 0) }) }),
    DAYOFYEAR: (args: Expression[]): DayOfYearExpr =>
      new DayOfYearExpr({ this: new TsOrDsToDateExpr({ this: seqGet(args, 0) }) }),
    FORMAT: NumberToStrExpr.fromArgList,
    FROM_UNIXTIME: buildFormattedTime(UnixToTimeExpr, { dialect: Dialects.MYSQL }),
    ISNULL: isnullToIsNull,
    LENGTH: (args: Expression[]): LengthExpr =>
      new LengthExpr({
        this: seqGet(args, 0),
        binary: true,
      }),
    MAKETIME: TimeFromPartsExpr.fromArgList,
    MONTH: (args: Expression[]): MonthExpr =>
      new MonthExpr({ this: new TsOrDsToDateExpr({ this: seqGet(args, 0) }) }),
    MONTHNAME: (args: Expression[]): TimeToStrExpr =>
      new TimeToStrExpr({
        this: new TsOrDsToDateExpr({ this: seqGet(args, 0) }),
        format: new LiteralExpr({
          this: '%B',
          isString: true,
        }),
      }),
    SCHEMA: CurrentSchemaExpr.fromArgList,
    DATABASE: CurrentSchemaExpr.fromArgList,
    STR_TO_DATE: buildStrToDate as (args: Expression[]) => Expression,
    TIMESTAMPDIFF: buildDateDelta(TimestampDiffExpr),
    TO_DAYS: (args: Expression[]) => {
      const diff = new DateDiffExpr({
        this: new TsOrDsToDateExpr({ this: seqGet(args, 0) }),
        expression: new TsOrDsToDateExpr({
          this: new LiteralExpr({
            this: '0000-01-01',
            isString: true,
          }),
        }),
        unit: new VarExpr({ this: 'DAY' }),
      });
      return diff.add(1); // Standardized parenthesized expression with offset
    },
    VERSION: CurrentVersionExpr.fromArgList,
    WEEK: (args: Expression[]): WeekExpr =>
      new WeekExpr({
        this: new TsOrDsToDateExpr({ this: seqGet(args, 0)! }),
        mode: seqGet(args, 1),
      }),
    WEEKOFYEAR: (args: Expression[]): WeekOfYearExpr =>
      new WeekOfYearExpr({ this: new TsOrDsToDateExpr({ this: seqGet(args, 0)! }) }),
    YEAR: (args: Expression[]): YearExpr =>
      new YearExpr({ this: new TsOrDsToDateExpr({ this: seqGet(args, 0)! }) }),
  };

  public static FUNCTION_PARSERS: Partial<Record<string, (self: Parser) => Expression | undefined>> = {
    ...Parser.FUNCTION_PARSERS,
    GROUP_CONCAT: (self: Parser) => self.parseGroupConcat(),
    VALUES: (self: Parser): AnonymousExpr =>
      self.expression(AnonymousExpr, {
        this: 'VALUES',
        expressions: [self.parseIdVar()],
      }),
    JSON_VALUE: (self: Parser): Expression => self.parseJsonValue(),
    SUBSTR: (self: Parser): Expression => self.parseSubstring(),
  };

  public static STATEMENT_PARSERS = {
    ...Parser.STATEMENT_PARSERS,
    [TokenType.SHOW]: (self: Parser) => self.parseShow(),
  };

  public static SHOW_PARSERS: typeof Parser.SHOW_PARSERS = ({
    'BINARY LOGS': showParser('BINARY LOGS'),
    'MASTER LOGS': showParser('BINARY LOGS'),
    'BINLOG EVENTS': showParser('BINLOG EVENTS'),
    'CHARACTER SET': showParser('CHARACTER SET'),
    'CHARSET': showParser('CHARACTER SET'),
    'COLLATION': showParser('COLLATION'),
    'FULL COLUMNS': showParser('COLUMNS', {
      target: 'FROM',
      full: true,
    }),
    'COLUMNS': showParser('COLUMNS', { target: 'FROM' }),
    'CREATE DATABASE': showParser('CREATE DATABASE', { target: true }),
    'CREATE EVENT': showParser('CREATE EVENT', { target: true }),
    'CREATE FUNCTION': showParser('CREATE FUNCTION', { target: true }),
    'CREATE PROCEDURE': showParser('CREATE PROCEDURE', { target: true }),
    'CREATE TABLE': showParser('CREATE TABLE', { target: true }),
    'CREATE TRIGGER': showParser('CREATE TRIGGER', { target: true }),
    'CREATE VIEW': showParser('CREATE VIEW', { target: true }),
    'DATABASES': showParser('DATABASES'),
    'SCHEMAS': showParser('DATABASES'),
    'ENGINE': showParser('ENGINE', { target: true }),
    'STORAGE ENGINES': showParser('ENGINES'),
    'ENGINES': showParser('ENGINES'),
    'ERRORS': showParser('ERRORS'),
    'EVENTS': showParser('EVENTS'),
    'FUNCTION CODE': showParser('FUNCTION CODE', { target: true }),
    'FUNCTION STATUS': showParser('FUNCTION STATUS'),
    'GRANTS': showParser('GRANTS', { target: 'FOR' }),
    'INDEX': showParser('INDEX', { target: 'FROM' }),
    'MASTER STATUS': showParser('MASTER STATUS'),
    'OPEN TABLES': showParser('OPEN TABLES'),
    'PLUGINS': showParser('PLUGINS'),
    'PROCEDURE CODE': showParser('PROCEDURE CODE', { target: true }),
    'PROCEDURE STATUS': showParser('PROCEDURE STATUS'),
    'PRIVILEGES': showParser('PRIVILEGES'),
    'FULL PROCESSLIST': showParser('PROCESSLIST', { full: true }),
    'PROCESSLIST': showParser('PROCESSLIST'),
    'PROFILE': showParser('PROFILE'),
    'PROFILES': showParser('PROFILES'),
    'RELAYLOG EVENTS': showParser('RELAYLOG EVENTS'),
    'REPLICAS': showParser('REPLICAS'),
    'SLAVE HOSTS': showParser('REPLICAS'),
    'REPLICA STATUS': showParser('REPLICA STATUS'),
    'SLAVE STATUS': showParser('REPLICA STATUS'),
    'GLOBAL STATUS': showParser('STATUS', { global: true }),
    'SESSION STATUS': showParser('STATUS'),
    'STATUS': showParser('STATUS'),
    'TABLE STATUS': showParser('TABLE STATUS'),
    'FULL TABLES': showParser('TABLES', { full: true }),
    'TABLES': showParser('TABLES'),
    'TRIGGERS': showParser('TRIGGERS'),
    'GLOBAL VARIABLES': showParser('VARIABLES', { global: true }),
    'SESSION VARIABLES': showParser('VARIABLES'),
    'VARIABLES': showParser('VARIABLES'),
    'WARNINGS': showParser('WARNINGS'),
  });

  /**
   * Parsers for table and column properties.
   */
  public static PROPERTY_PARSERS = {
    ...Parser.PROPERTY_PARSERS,
    'LOCK': (self: Parser) => self.parsePropertyAssignment(LockPropertyExpr),
    'PARTITION BY': (self: Parser) => (self as MySQLParser).parsePartitionProperty(),
  };

  /**
   * Parsers for SET statement items.
   */
  public static SET_PARSERS = {
    ...Parser.SET_PARSERS,
    'PERSIST': (self: Parser) => (self as MySQLParser).parseSetItemAssignment({ kind: SetItemExprKind.PERSIST }),
    'PERSIST_ONLY': (self: Parser) => self.parseSetItemAssignment({ kind: 'PERSIST_ONLY' }),
    'CHARACTER SET': (self: Parser): Expression => (self as MySQLParser).parseSetItemCharset('CHARACTER SET'),
    'CHARSET': (self: Parser): Expression => (self as MySQLParser).parseSetItemCharset('CHARACTER SET'),
    'NAMES': (self: Parser) => (self as MySQLParser).parseSetItemNames(),
  };

  /**
   * Parsers for column and table constraints.
   */
  public static CONSTRAINT_PARSERS = {
    ...Parser.CONSTRAINT_PARSERS,
    FULLTEXT: (self: Parser) => (self as MySQLParser).parseIndexConstraint('FULLTEXT'),
    INDEX: (self: Parser) => (self as MySQLParser).parseIndexConstraint(),
    KEY: (self: Parser) => (self as MySQLParser).parseIndexConstraint(),
    SPATIAL: (self: Parser) => (self as MySQLParser).parseIndexConstraint('SPATIAL'),
    ZEROFILL: (self: Parser) =>
      self.expression(ZeroFillColumnConstraintExpr),
  };

  /**
   * Parsers for ALTER TABLE actions.
   */
  public static ALTER_PARSERS = {
    ...Parser.ALTER_PARSERS,
    MODIFY: (self: Parser) => self.parseAlterTableAlter(),
  };

  /**
   * Parsers for ALTER TABLE ALTER sub-actions.
   */
  public static ALTER_ALTER_PARSERS = {
    ...Parser.ALTER_ALTER_PARSERS,
    INDEX: (self: Parser) => (self as MySQLParser).parseAlterTableAlterIndex(),
  };

  /**
   * List of constraints that do not require an explicit name in the schema definition.
   */
  public static SCHEMA_UNNAMED_CONSTRAINTS: Set<string> = new Set([
    ...Parser.SCHEMA_UNNAMED_CONSTRAINTS,
    'FULLTEXT',
    'INDEX',
    'KEY',
    'SPATIAL',
  ]);

  public static PROFILE_TYPES = {
    ALL: [],
    CPU: [],
    IPC: [],
    MEMORY: [],
    SOURCE: [],
    SWAPS: [],
    BLOCK: ['IO'],
    CONTEXT: ['SWITCHES'],
    PAGE: ['FAULTS'],
  };

  public static TYPE_TOKENS: Set<TokenType> = new Set([...Parser.TYPE_TOKENS, TokenType.SET]);

  public static ENUM_TYPE_TOKENS: Set<TokenType> = new Set([...Parser.ENUM_TYPE_TOKENS, TokenType.SET]);

  /**
   * Modifiers that can appear in a MySQL SELECT statement.
   */
  public static OPERATION_MODIFIERS: Set<string> = new Set([
    'HIGH_PRIORITY',
    'STRAIGHT_JOIN',
    'SQL_SMALL_RESULT',
    'SQL_BIG_RESULT',
    'SQL_BUFFER_RESULT',
    'SQL_NO_CACHE',
    'SQL_CALC_FOUND_ROWS',
  ]);

  public static LOG_DEFAULTS_TO_LN = true;
  public static STRING_ALIASES = true;
  public static VALUES_FOLLOWED_BY_PAREN = false;
  public static SUPPORTS_PARTITION_SELECTION = true;

  /**
   * Handles MySQL's GENERATED ALWAYS AS logic, including VIRTUAL vs STORED persistence.
   */
  public parseGeneratedAsIdentity ():
    | GeneratedAsIdentityColumnConstraintExpr
    | ComputedColumnConstraintExpr
    | GeneratedAsRowColumnConstraintExpr {
    let thisExpr = super.parseGeneratedAsIdentity();

    if (this.matchTexts(['STORED', 'VIRTUAL'])) {
      const persisted = this.prev?.text.toUpperCase() === 'STORED';

      if (thisExpr instanceof ComputedColumnConstraintExpr) {
        thisExpr.setArgKey('persisted', persisted);
      } else if (thisExpr instanceof GeneratedAsIdentityColumnConstraintExpr) {
        // Pivot to a ComputedColumnConstraint if persistence is explicitly specified
        thisExpr = this.expression(ComputedColumnConstraintExpr, {
          this: thisExpr.args.expression,
          persisted: persisted,
        });
      }
    }

    return thisExpr;
  }

  /**
   * Parses MySQL-specific primary key parts which allow column prefixes (e.g. KEY(col(10))).
   */
  public parsePrimaryKeyPart (): Expression | undefined {
    const thisExpr = this.parseIdVar();
    if (!this.match(TokenType.L_PAREN)) {
      return thisExpr;
    }

    const expression = this.parseNumber();
    this.matchRParen();

    return this.expression(ColumnPrefixExpr, {
      this: thisExpr,
      expression: expression,
    });
  }

  /**
   * Parses MySQL index constraints, including support for KEY_BLOCK_SIZE,
   * custom parsers, and visibility toggles.
   */
  protected parseIndexConstraint (kind?: string): IndexColumnConstraintExpr {
    if (kind) {
      this.matchTexts(['INDEX', 'KEY']);
    }

    const thisExpr = this.parseIdVar({ anyToken: false });
    const indexType = this.match(TokenType.USING) && this.advanceAny() && this.prev?.text;
    const expressions = this.parseWrappedCsv(this.parseOrdered.bind(this));

    const options: IndexConstraintOptionExpr[] = [];
    while (true) {
      let opt: IndexConstraintOptionExpr | undefined = undefined;

      if (this.matchTextSeq(['KEY_BLOCK_SIZE'])) {
        this.match(TokenType.EQ);
        opt = new IndexConstraintOptionExpr({ keyBlockSize: this.parseNumber() });
      } else if (this.match(TokenType.USING)) {
        opt = new IndexConstraintOptionExpr({ using: this.advanceAny() && this.prev?.text });
      } else if (this.matchTextSeq(['WITH', 'PARSER'])) {
        opt = new IndexConstraintOptionExpr({ parser: this.parseVar({ anyToken: true }) });
      } else if (this.match(TokenType.COMMENT)) {
        opt = new IndexConstraintOptionExpr({ comment: this.parseString() });
      } else if (this.matchTextSeq(['VISIBLE'])) {
        opt = new IndexConstraintOptionExpr({ visible: true });
      } else if (this.matchTextSeq(['INVISIBLE'])) {
        opt = new IndexConstraintOptionExpr({ visible: false });
      } else if (this.matchTextSeq(['ENGINE_ATTRIBUTE'])) {
        this.match(TokenType.EQ);
        opt = new IndexConstraintOptionExpr({ engineAttr: this.parseString() });
      } else if (this.matchTextSeq(['SECONDARY_ENGINE_ATTRIBUTE'])) {
        this.match(TokenType.EQ);
        opt = new IndexConstraintOptionExpr({ secondaryEngineAttr: this.parseString() });
      }

      if (!opt) break;
      options.push(opt);
    }

    return this.expression(IndexColumnConstraintExpr, {
      this: thisExpr,
      expressions: expressions,
      kind: kind,
      indexType: indexType,
      options: options,
    });
  }

  /**
   * Core internal parser for the varied MySQL SHOW statement variants.
   */
  public parseShowMysql (
    thisArg: string,
    options: {
      target?: boolean | string;
      full?: boolean;
      global?: boolean;
    } = {},
  ): ShowExpr {
    const {
      target = false,
      full,
      global,
    } = options;
    const json = this.matchTextSeq(['JSON']);
    let targetId: Expression | undefined = undefined;

    if (target) {
      if (typeof target === 'string') {
        this.matchTextSeq(target.split(' '));
      }
      targetId = this.parseIdVar();
    }

    const log = this.matchTextSeq(['IN']) ? this.parseString() : undefined;
    let position: Expression | undefined = undefined;
    let db: Expression | undefined = undefined;

    if (thisArg === 'BINLOG EVENTS' || thisArg === 'RELAYLOG EVENTS') {
      position = this.matchTextSeq(['FROM']) ? this.parseNumber() : undefined;
    } else {
      if (this.match(TokenType.FROM)) {
        db = this.parseIdVar();
      } else if (this.match(TokenType.DOT)) {
        db = targetId;
        targetId = this.parseIdVar();
      }
    }

    const channel = this.matchTextSeq(['FOR', 'CHANNEL']) ? this.parseIdVar() : undefined;
    const like = this.matchTextSeq(['LIKE']) ? this.parseString() : undefined;
    const where = this.parseWhere();

    let types: Expression[] | undefined = undefined;
    let query: Expression | undefined = undefined;
    let offset: Expression | undefined = undefined;
    let limit: Expression | undefined = undefined;

    if (thisArg === 'PROFILE') {
      types = this.parseCsv(() => this.parseVarFromOptions(MySQLParser.PROFILE_TYPES));
      query = this.matchTextSeq(['FOR', 'QUERY']) ? this.parseNumber() : undefined;
      offset = this.matchTextSeq(['OFFSET']) ? this.parseNumber() : undefined;
      limit = this.matchTextSeq(['LIMIT']) ? this.parseNumber() : undefined;
    } else {
      [offset, limit] = this.parseOldstyleLimit();
    }

    let mutex: boolean | undefined = undefined;
    if (this.matchTextSeq(['MUTEX'])) mutex = true;
    if (this.matchTextSeq(['STATUS'])) mutex = false;

    const forTable = this.matchTextSeq(['FOR', 'TABLE']) ? this.parseIdVar() : undefined;
    const forGroup = this.matchTextSeq(['FOR', 'GROUP']) ? this.parseString() : undefined;
    const forUser = this.matchTextSeq(['FOR', 'USER']) ? this.parseString() : undefined;
    const forRole = this.matchTextSeq(['FOR', 'ROLE']) ? this.parseString() : undefined;
    const intoOutfile = this.matchTextSeq(['INTO', 'OUTFILE']) ? this.parseString() : undefined;

    return this.expression(ShowExpr, {
      this: thisArg,
      target: targetId,
      full: full,
      log: log,
      position: position,
      db: db,
      channel: channel,
      like: like,
      where: where,
      types: types,
      query: query,
      offset: offset,
      limit: limit,
      mutex: mutex,
      forTable: forTable,
      forGroup: forGroup,
      forUser: forUser,
      forRole: forRole,
      intoOutfile: intoOutfile,
      json: json,
      global: global,
    });
  }

  protected parseOldstyleLimit (): [Expression | undefined, Expression | undefined] {
    let limit: Expression | undefined = undefined;
    let offset: Expression | undefined = undefined;

    if (this.matchTextSeq(['LIMIT'])) {
      const parts = this.parseCsv(this.parseNumber.bind(this));
      if (parts.length === 1) {
        limit = parts[0];
      } else if (parts.length === 2) {
        limit = parts[1];
        offset = parts[0];
      }
    }

    return [offset, limit];
  }

  protected parseSetItemCharset (kind: string): Expression {
    const thisExpr = this.parseString() || this.parseUnquotedField();
    return this.expression(SetItemExpr, {
      this: thisExpr,
      kind: kind,
    });
  }

  protected parseSetItemNames (): Expression {
    const charset = this.parseString() || this.parseUnquotedField();
    let collate: Expression | undefined = undefined;

    if (this.matchTextSeq(['COLLATE'])) {
      collate = this.parseString() || this.parseUnquotedField();
    }

    return this.expression(SetItemExpr, {
      this: charset,
      collate: collate,
      kind: 'NAMES',
    });
  }

  /**
   * Overrides core type parsing to handle MySQL's unique 'BINARY' modifier
   * which can act as a cast without parentheses.
   */
  public parseType (options: {
    parseInterval?: boolean;
    fallbackToIdentifier?: boolean;
  } = {}): Expression | undefined {
    const {
      parseInterval = true,
      fallbackToIdentifier = false,
    } = options;
    if (this.match(TokenType.BINARY, { advance: false })) {
      const dataType = this.parseTypes({
        checkFunc: true,
        allowIdentifiers: false,
      });

      if (dataType instanceof DataTypeExpr) {
        return this.expression(CastExpr, {
          this: this.parseColumn(),
          to: dataType,
        });
      }
    }

    return super.parseType({
      parseInterval,
      fallbackToIdentifier,
    });
  }

  protected parseAlterTableAlterIndex (): AlterIndexExpr {
    const index = this.parseField({ anyToken: true });
    let visible: boolean | undefined = undefined;

    if (this.matchTextSeq(['VISIBLE'])) {
      visible = true;
    } else if (this.matchTextSeq(['INVISIBLE'])) {
      visible = false;
    }

    return this.expression(AlterIndexExpr, {
      this: index,
      visible: visible,
    });
  }

  /**
   * Parses MySQL partitioning properties for RANGE and LIST schemes.
   */
  protected parsePartitionProperty (): Expression | Expression[] | undefined {
    let partitionCls: typeof Expression | undefined = undefined;
    let valueParser: (() => Expression | undefined) | undefined = undefined;

    if (this.matchTextSeq(['RANGE'])) {
      partitionCls = PartitionByRangePropertyExpr;
      valueParser = this.parsePartitionRangeValue.bind(this);
    } else if (this.matchTextSeq(['LIST'])) {
      partitionCls = PartitionByListPropertyExpr;
      valueParser = this.parsePartitionListValue.bind(this);
    }

    if (!partitionCls || !valueParser) {
      return undefined;
    }

    const partitionExpressions = this.parseWrappedCsv(this.parseAssignment.bind(this));

    // For Doris and Starrocks compatibility check
    if (!this.matchTextSeq(['(', 'PARTITION'], { advance: false })) {
      return partitionExpressions;
    }

    const createExpressions = this.parseWrappedCsv(valueParser);

    return this.expression(partitionCls, {
      partitionExpressions: partitionExpressions,
      createExpressions: createExpressions,
    });
  }

  protected parsePartitionRangeValue (): Expression | undefined {
    this.matchTextSeq(['PARTITION']);
    const name = this.parseIdVar();

    if (!this.matchTextSeq([
      'VALUES',
      'LESS',
      'THAN',
    ])) {
      return name;
    }

    let values = this.parseWrappedCsv(this.parseExpression.bind(this));

    if (
      values.length === 1
      && values[0] instanceof ColumnExpr
      && values[0].name.toUpperCase() === 'MAXVALUE'
    ) {
      values = [new VarExpr({ this: 'MAXVALUE' })];
    }

    const partRange = this.expression(PartitionRangeExpr, {
      this: name,
      expressions: values,
    });
    return this.expression(PartitionExpr, { expressions: [partRange] });
  }

  protected parsePartitionListValue (): PartitionExpr {
    this.matchTextSeq(['PARTITION']);
    const name = this.parseIdVar();
    this.matchTextSeq(['VALUES', 'IN']);
    const values = this.parseWrappedCsv(this.parseExpression.bind(this));

    const partList = this.expression(PartitionListExpr, {
      this: name,
      expressions: values,
    });
    return this.expression(PartitionExpr, { expressions: [partList] });
  }

  public parsePrimaryKey (
    options: {
      wrappedOptional?: boolean;
      inProps?: boolean;
      namedPrimaryKey?: boolean;
    } = {},
  ): PrimaryKeyColumnConstraintExpr | PrimaryKeyExpr {
    const {
      wrappedOptional = false,
      inProps = false,
      namedPrimaryKey: _namedPrimaryKey = false,
    } = options;
    // MySQL always supports named primary keys in this context
    return super.parsePrimaryKey({
      wrappedOptional,
      inProps,
      namedPrimaryKey: true,
    });
  }
}

class MySQLGenerator extends Generator {
  public static INTERVAL_ALLOWS_PLURAL_FORM: boolean = false;
  public static LOCKING_READS_SUPPORTED: boolean = true;
  public static NULL_ORDERING_SUPPORTED: boolean | undefined = undefined;
  public static JOIN_HINTS: boolean = false;
  public static TABLE_HINTS: boolean = true;
  public static DUPLICATE_KEY_UPDATE_WITH_SET: boolean = false;
  public static QUERY_HINT_SEP: string = ' ';
  public static VALUES_AS_TABLE: boolean = false;
  public static NVL2_SUPPORTED: boolean = false;
  public static LAST_DAY_SUPPORTS_DATE_PART: boolean = false;
  public static JSON_TYPE_REQUIRED_FOR_EXTRACTION: boolean = true;
  public static JSON_PATH_BRACKETED_KEY_SUPPORTED: boolean = false;
  public static JSON_KEY_VALUE_PAIR_SEP: string = ',';
  public static SUPPORTS_TO_NUMBER: boolean = false;
  public static PARSE_JSON_NAME: string | undefined = undefined;
  public static PAD_FILL_PATTERN_IS_REQUIRED: boolean = true;
  public static WRAP_DERIVED_VALUES: boolean = false;
  public static VARCHAR_REQUIRES_SIZE: boolean = true;
  public static SUPPORTS_MEDIAN: boolean = false;
  public static UPDATE_STATEMENT_SUPPORTS_FROM: boolean = false;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  public static ORIGINAL_TRANSFORMS = new Map<typeof Expression, (self: Generator, e: any) => string>([
    ...Generator.TRANSFORMS,
    [ArrayAggExpr, renameFunc('GROUP_CONCAT')],
    [BitwiseAndAggExpr, renameFunc('BIT_AND')],
    [BitwiseOrAggExpr, renameFunc('BIT_OR')],
    [BitwiseXorAggExpr, renameFunc('BIT_XOR')],
    [BitwiseCountExpr, renameFunc('BIT_COUNT')],
    [ChrExpr, (self: Generator, e: ChrExpr): string => self.chrSql(e, { name: 'CHAR' })],
    [CurrentDateExpr, noParenCurrentDateSql],
    [CurrentVersionExpr, renameFunc('VERSION')],
    [
      DateDiffExpr,
      removeTsOrDsToDate(
        (self: Generator, e: DateDiffExpr): string => self.func('DATEDIFF', [e.args.this, e.args.expression]),
        ['this', 'expression'],
      ),
    ],
    [DateAddExpr, removeTsOrDsToDate(dateAddSql('ADD'))],
    [DateStrToDateExpr, dateStrToDateSql],
    [DateSubExpr, removeTsOrDsToDate(dateAddSql('SUB'))],
    [DateTruncExpr, dateTruncSql],
    [DayExpr, removeTsOrDsToDate()],
    [DayOfMonthExpr, removeTsOrDsToDate(renameFunc('DAYOFMONTH'))],
    [DayOfWeekExpr, removeTsOrDsToDate(renameFunc('DAYOFWEEK'))],
    [DayOfYearExpr, removeTsOrDsToDate(renameFunc('DAYOFYEAR'))],
    [
      GroupConcatExpr,
      (self: Generator, e: GroupConcatExpr): string =>
        `GROUP_CONCAT(${self.sql(e, 'this')} SEPARATOR ${self.sql(e, 'separator') || '\',\''})`,
    ],
    [ILikeExpr, noIlikeSql],
    [JsonExtractScalarExpr, arrowJsonExtractSql],
    [LengthExpr, lengthOrCharLengthSql],
    [LogicalOrExpr, renameFunc('MAX')],
    [LogicalAndExpr, renameFunc('MIN')],
    [MaxExpr, maxOrGreatest],
    [MinExpr, minOrLeast],
    [MonthExpr, removeTsOrDsToDate()],
    [NullSafeEqExpr, (self: Generator, e: NullSafeEqExpr): string => self.binary(e, '<=>')],
    [NullSafeNeqExpr, (self: Generator, e: NullSafeNeqExpr): string => `NOT ${self.binary(e, '<=>')}`],
    [NumberToStrExpr, renameFunc('FORMAT')],
    [PivotExpr, noPivotSql],
    [
      SelectExpr,
      preprocess([
        eliminateDistinctOn,
        eliminateSemiAndAntiJoins,
        eliminateQualify,
        eliminateFullOuterJoin,
        unnestGenerateDateArrayUsingRecursiveCte,
      ]),
    ],
    [
      StrPositionExpr,
      (self: Generator, e: StrPositionExpr): string =>
        strPositionSql(self, e, {
          funcName: 'LOCATE',
          supportsPosition: true,
        }),
    ],
    [StrToDateExpr, strToDateSql],
    [StrToTimeExpr, strToDateSql],
    [StuffExpr, renameFunc('INSERT')],
    [SessionUserExpr, () => 'SESSION_USER()'],
    [TableSampleExpr, noTablesampleSql],
    [TimeFromPartsExpr, renameFunc('MAKETIME')],
    [TimestampAddExpr, dateAddIntervalSql('DATE', 'ADD')],
    [
      TimestampDiffExpr,
      (self: Generator, e: TimestampDiffExpr): string =>
        self.func('TIMESTAMPDIFF', [
          unitToVar(e)!,
          e.args.expression,
          e.args.this,
        ]),
    ],
    [TimestampSubExpr, dateAddIntervalSql('DATE', 'SUB')],
    [TimeStrToUnixExpr, renameFunc('UNIX_TIMESTAMP')],
    [
      TimeStrToTimeExpr,
      (self: Generator, e: TimeStrToTimeExpr): string =>
        timeStrToTimeSql(self, e, { includePrecision: !e.args.zone }),
    ],
    [
      TimeToStrExpr,
      removeTsOrDsToDate((self: Generator, e: TimeToStrExpr) =>
        self.func('DATE_FORMAT', [e.args.this, self.formatTime(e)!])),
    ],
    [TrimExpr, trimSql],
    [TruncExpr, renameFunc('TRUNCATE')],
    [TryCastExpr, noTrycastSql],
    [TsOrDsAddExpr, dateAddSql('ADD')],
    [TsOrDsDiffExpr, (self: Generator, e: TsOrDsDiffExpr): string => self.func('DATEDIFF', [e.args.this, e.args.expression])],
    [TsOrDsToDateExpr, tsOrDsToDateSql],
    [UnicodeExpr, (self: Generator, e: UnicodeExpr): string => `ORD(CONVERT(${self.sql(e.args.this)} USING utf32))`],
    [UnixToTimeExpr, unixToTimeSql],
    [WeekExpr, removeTsOrDsToDate()],
    [WeekOfYearExpr, removeTsOrDsToDate(renameFunc('WEEKOFYEAR'))],
    [YearExpr, removeTsOrDsToDate()],
    [UtcTimestampExpr, renameFunc('UTC_TIMESTAMP')],
    [UtcTimeExpr, renameFunc('UTC_TIME')],
  ]);

  /**
   * Maps unsigned types to their standard MySQL counterparts.
   * MySQL adds the 'UNSIGNED' attribute during generation based on the DataTypeExpr.
   */
  public static UNSIGNED_TYPE_MAPPING: Record<string, string> = {
    [DataTypeExprKind.UBIGINT]: 'BIGINT',
    [DataTypeExprKind.UINT]: 'INT',
    [DataTypeExprKind.UMEDIUMINT]: 'MEDIUMINT',
    [DataTypeExprKind.USMALLINT]: 'SMALLINT',
    [DataTypeExprKind.UTINYINT]: 'TINYINT',
    [DataTypeExprKind.UDECIMAL]: 'DECIMAL',
    [DataTypeExprKind.UDOUBLE]: 'DOUBLE',
  };

  /**
   * Standardizes various timestamp types to MySQL's DATETIME or TIMESTAMP.
   */
  public static TIMESTAMP_TYPE_MAPPING: Record<string, string> = {
    [DataTypeExprKind.DATETIME2]: 'DATETIME',
    [DataTypeExprKind.SMALLDATETIME]: 'DATETIME',
    [DataTypeExprKind.TIMESTAMP]: 'DATETIME',
    [DataTypeExprKind.TIMESTAMPNTZ]: 'DATETIME',
    [DataTypeExprKind.TIMESTAMPTZ]: 'TIMESTAMP',
    [DataTypeExprKind.TIMESTAMPLTZ]: 'TIMESTAMP',
  };

  public static TYPE_MAPPING = (() => {
    const mapping = new Map(Generator.TYPE_MAPPING);

    for (const [k, v] of Object.entries(MySQLGenerator.UNSIGNED_TYPE_MAPPING)) {
      mapping.set(k, v);
    }
    for (const [k, v] of Object.entries(MySQLGenerator.TIMESTAMP_TYPE_MAPPING)) {
      mapping.set(k, v);
    }

    // Remove types that MySQL handles natively or via special attributes
    mapping.delete(DataTypeExprKind.MEDIUMTEXT);
    mapping.delete(DataTypeExprKind.LONGTEXT);
    mapping.delete(DataTypeExprKind.TINYTEXT);
    mapping.delete(DataTypeExprKind.BLOB);
    mapping.delete(DataTypeExprKind.MEDIUMBLOB);
    mapping.delete(DataTypeExprKind.LONGBLOB);
    mapping.delete(DataTypeExprKind.TINYBLOB);

    return mapping;
  })();

  public static PROPERTIES_LOCATION = (() => {
    const map = new Map(Generator.PROPERTIES_LOCATION);
    map.set(TransientPropertyExpr, PropertiesLocation.UNSUPPORTED);
    map.set(VolatilePropertyExpr, PropertiesLocation.UNSUPPORTED);
    map.set(PartitionedByPropertyExpr, PropertiesLocation.UNSUPPORTED);
    map.set(PartitionByRangePropertyExpr, PropertiesLocation.POST_SCHEMA);
    map.set(PartitionByListPropertyExpr, PropertiesLocation.POST_SCHEMA);
    return map;
  })();

  public static LIMIT_FETCH: string = 'LIMIT';
  public static LIMIT_ONLY_LITERALS: boolean = true;

  /**
   * MySQL CAST targets for character-based types.
   */
  public static CHAR_CAST_MAPPING: Record<string, string> = [
    DataTypeExprKind.LONGTEXT,
    DataTypeExprKind.LONGBLOB,
    DataTypeExprKind.MEDIUMBLOB,
    DataTypeExprKind.MEDIUMTEXT,
    DataTypeExprKind.TEXT,
    DataTypeExprKind.TINYBLOB,
    DataTypeExprKind.TINYTEXT,
    DataTypeExprKind.VARCHAR,
  ].reduce((acc, type) => ({
    ...acc,
    [type]: 'CHAR',
  }), {});

  /**
   * MySQL CAST targets for integer-based types.
   */
  public static SIGNED_CAST_MAPPING: Record<string, string> = [
    DataTypeExprKind.BIGINT,
    DataTypeExprKind.BOOLEAN,
    DataTypeExprKind.INT,
    DataTypeExprKind.SMALLINT,
    DataTypeExprKind.TINYINT,
    DataTypeExprKind.MEDIUMINT,
  ].reduce((acc, type) => ({
    ...acc,
    [type]: 'SIGNED',
  }), {});

  /**
   * MySQL is restricted in which types it can use as a CAST target.
   * Reference: https://dev.mysql.com/doc/refman/8.0/en/cast-functions.html#function_cast
   */
  public static CAST_MAPPING: Record<string, string> = {
    ...MySQLGenerator.CHAR_CAST_MAPPING,
    ...MySQLGenerator.SIGNED_CAST_MAPPING,
    [DataTypeExprKind.UBIGINT]: 'UNSIGNED',
  };

  /**
   * Types that require specific function-like syntax for timestamp manipulation.
   */
  public static TIMESTAMP_FUNC_TYPES: Set<string> = new Set([DataTypeExprKind.TIMESTAMPTZ, DataTypeExprKind.TIMESTAMPLTZ]);

  /**
   * Comprehensive list of MySQL reserved keywords for identifier quoting.
   * Reference: https://dev.mysql.com/doc/refman/8.0/en/keywords.html
   */
  public static RESERVED_KEYWORDS: Set<string> = new Set([
    'accessible',
    'add',
    'all',
    'alter',
    'analyze',
    'and',
    'as',
    'asc',
    'asensitive',
    'before',
    'between',
    'bigint',
    'binary',
    'blob',
    'both',
    'by',
    'call',
    'cascade',
    'case',
    'change',
    'char',
    'character',
    'check',
    'collate',
    'column',
    'condition',
    'constraint',
    'continue',
    'convert',
    'create',
    'cross',
    'cube',
    'cume_dist',
    'current_date',
    'current_time',
    'current_timestamp',
    'current_user',
    'cursor',
    'database',
    'databases',
    'day_hour',
    'day_microsecond',
    'day_minute',
    'day_second',
    'dec',
    'decimal',
    'declare',
    'default',
    'delayed',
    'delete',
    'dense_rank',
    'desc',
    'describe',
    'deterministic',
    'distinct',
    'distinctrow',
    'div',
    'double',
    'drop',
    'dual',
    'each',
    'else',
    'elseif',
    'empty',
    'enclosed',
    'escaped',
    'except',
    'exists',
    'exit',
    'explain',
    'false',
    'fetch',
    'first_value',
    'float',
    'float4',
    'float8',
    'for',
    'force',
    'foreign',
    'from',
    'fulltext',
    'function',
    'generated',
    'get',
    'grant',
    'group',
    'grouping',
    'groups',
    'having',
    'high_priority',
    'hour_microsecond',
    'hour_minute',
    'hour_second',
    'if',
    'ignore',
    'in',
    'index',
    'infile',
    'inner',
    'inout',
    'insensitive',
    'insert',
    'int',
    'int1',
    'int2',
    'int3',
    'int4',
    'int8',
    'integer',
    'intersect',
    'interval',
    'into',
    'io_after_gtids',
    'io_before_gtids',
    'is',
    'iterate',
    'join',
    'json_table',
    'key',
    'keys',
    'kill',
    'lag',
    'last_value',
    'lateral',
    'lead',
    'leading',
    'leave',
    'left',
    'like',
    'limit',
    'linear',
    'lines',
    'load',
    'localtime',
    'localtimestamp',
    'lock',
    'long',
    'longblob',
    'longtext',
    'loop',
    'low_priority',
    'master_bind',
    'master_ssl_verify_server_cert',
    'match',
    'maxvalue',
    'mediumblob',
    'mediumint',
    'mediumtext',
    'middleint',
    'minute_microsecond',
    'minute_second',
    'mod',
    'modifies',
    'natural',
    'not',
    'no_write_to_binlog',
    'nth_value',
    'ntile',
    'undefined',
    'numeric',
    'of',
    'on',
    'optimize',
    'optimizer_costs',
    'option',
    'optionally',
    'or',
    'order',
    'out',
    'outer',
    'outfile',
    'over',
    'partition',
    'percent_rank',
    'precision',
    'primary',
    'procedure',
    'purge',
    'range',
    'rank',
    'read',
    'reads',
    'read_write',
    'real',
    'recursive',
    'references',
    'regexp',
    'release',
    'rename',
    'repeat',
    'replace',
    'require',
    'resignal',
    'restrict',
    'return',
    'revoke',
    'right',
    'rlike',
    'row',
    'rows',
    'row_number',
    'schema',
    'schemas',
    'second_microsecond',
    'select',
    'sensitive',
    'separator',
    'set',
    'show',
    'signal',
    'smallint',
    'spatial',
    'specific',
    'sql',
    'sqlexception',
    'sqlstate',
    'sqlwarning',
    'sql_big_result',
    'sql_calc_found_rows',
    'sql_small_result',
    'ssl',
    'starting',
    'stored',
    'straight_join',
    'system',
    'table',
    'terminated',
    'then',
    'tinyblob',
    'tinyint',
    'tinytext',
    'to',
    'trailing',
    'trigger',
    'true',
    'undo',
    'union',
    'unique',
    'unlock',
    'unsigned',
    'update',
    'usage',
    'use',
    'using',
    'utc_date',
    'utc_time',
    'utc_timestamp',
    'values',
    'varbinary',
    'varchar',
    'varcharacter',
    'varying',
    'virtual',
    'when',
    'where',
    'while',
    'window',
    'with',
    'write',
    'xor',
    'year_month',
    'zerofill',
  ]);

  public computedColumnConstraintSql (expression: ComputedColumnConstraintExpr): string {
    const persisted = expression.args.persisted ? 'STORED' : 'VIRTUAL';
    return `GENERATED ALWAYS AS (${this.sql(expression.args.this?.unnest())}) ${persisted}`;
  }

  public arraySql (expression: ArrayExpr): string {
    this.unsupported('Arrays are not supported by MySQL');
    return this.functionFallbackSql(expression);
  }

  public arrayContainsAllSql (expression: ArrayContainsAllExpr): string {
    this.unsupported('Array operations are not supported by MySQL');
    return this.functionFallbackSql(expression);
  }

  public dpipeSql (expression: DPipeExpr): string {
    return this.func('CONCAT', [...expression.flatten()]);
  }

  public extractSql (expression: ExtractExpr): string {
    const unit = expression.name;
    if (unit && unit.toLowerCase() === 'epoch') {
      return this.func('UNIX_TIMESTAMP', [expression.args.expression as Expression]);
    }

    return super.extractSql(expression);
  }

  public dataTypeSql (expression: DataTypeExpr): string {
    if (
      (this.constructor as typeof MySQLGenerator).VARCHAR_REQUIRES_SIZE
      && expression.isType(DataTypeExprKind.VARCHAR)
      && !expression.args.expressions?.length
    ) {
      // `VARCHAR` must always have a size - if it doesn't, we always generate `TEXT`
      return 'TEXT';
    }

    let result = super.dataTypeSql(expression);
    if (expression.args.this as string in MySQLGenerator.UNSIGNED_TYPE_MAPPING) {
      result = `${result} UNSIGNED`;
    }

    return result;
  }

  public jsonArrayContainsSql (expression: JsonArrayContainsExpr): string {
    return `${this.sql(expression, 'this')} MEMBER OF(${this.sql(expression, 'expression')})`;
  }

  public castSql (expression: CastExpr, _options: { safePrefix?: string } = {}): string {
    const toExpr = expression.args.to;
    if (toExpr instanceof DataTypeExpr) {
      const toThis = toExpr.args.this as string;
      if (MySQLGenerator.TIMESTAMP_FUNC_TYPES.has(toThis)) {
        return this.func('TIMESTAMP', [expression.args.this as Expression]);
      }
      const to = MySQLGenerator.CAST_MAPPING[toThis];
      if (to) {
        toExpr.setArgKey('this', to);
      }
    }
    return super.castSql(expression);
  }

  public showSql (expression: ShowExpr): string {
    const thisName = ` ${expression.name}`;
    const full = expression.args.full ? ' FULL' : '';
    const global_ = expression.args.global ? ' GLOBAL' : '';

    let target = this.sql(expression, 'target');
    target = target ? ` ${target}` : '';

    if (['COLUMNS', 'INDEX'].includes(expression.name)) {
      target = ` FROM${target}`;
    } else if (expression.name === 'GRANTS') {
      target = ` FOR${target}`;
    } else if (['LINKS', 'PARTITIONS'].includes(expression.name)) {
      target = target ? ` ON${target}` : '';
    } else if (expression.name === 'PROJECTIONS') {
      target = target ? ` ON TABLE${target}` : '';
    }

    const db = this.prefixedSql('FROM', expression, 'db');
    const like = this.prefixedSql('LIKE', expression, 'like');
    const where = this.sql(expression, 'where');

    let types = this.expressions(expression, { key: 'types' });
    types = types ? ` ${types}` : types;
    const query = this.prefixedSql('FOR QUERY', expression, 'query');

    let offset = '';
    let limit = '';
    if (expression.name === 'PROFILE') {
      offset = this.prefixedSql('OFFSET', expression, 'offset');
      limit = this.prefixedSql('LIMIT', expression, 'limit');
    } else {
      limit = this.oldStyleLimitSql(expression);
    }

    const log = this.prefixedSql('IN', expression, 'log');
    const position = this.prefixedSql('FROM', expression, 'position');
    const channel = this.prefixedSql('FOR CHANNEL', expression, 'channel');

    let mutexOrStatus = '';
    if (expression.name === 'ENGINE') {
      mutexOrStatus = expression.args.mutex ? ' MUTEX' : ' STATUS';
    }

    const forTable = this.prefixedSql('FOR TABLE', expression, 'forTable');
    const forGroup = this.prefixedSql('FOR GROUP', expression, 'forGroup');
    const forUser = this.prefixedSql('FOR USER', expression, 'forUser');
    const forRole = this.prefixedSql('FOR ROLE', expression, 'forRole');
    const intoOutfile = this.prefixedSql('INTO OUTFILE', expression, 'intoOutfile');
    const json = expression.args.json ? ' JSON' : '';

    return `SHOW${full}${global_}${thisName}${json}${target}${forTable}${types}${db}${query}${log}${position}${channel}${mutexOrStatus}${like}${where}${offset}${limit}${forGroup}${forUser}${forRole}${intoOutfile}`;
  }

  /**
   * MySQL doesn't use the TO keyword in ALTER ... RENAME.
   */
  public alterRenameSql (expression: AlterRenameExpr, _options: { includeTo?: boolean } = {}): string {
    return super.alterRenameSql(expression, { includeTo: false });
  }

  /**
   * MySQL uses MODIFY COLUMN for changing column data types.
   */
  public alterColumnSql (expression: AlterColumnExpr): string {
    const dtype = this.sql(expression, 'dtype');
    if (!dtype) {
      return super.alterColumnSql(expression);
    }

    const thisExpr = this.sql(expression, 'this');
    return `MODIFY COLUMN ${thisExpr} ${dtype}`;
  }

  protected prefixedSql (prefix: string, expression: Expression, arg: string): string {
    const sql = this.sql(expression, arg);
    return sql ? ` ${prefix} ${sql}` : '';
  }

  protected oldStyleLimitSql (expression: ShowExpr): string {
    const limit = this.sql(expression, 'limit');
    const offset = this.sql(expression, 'offset');
    if (limit) {
      const limitOffset = offset ? `${offset}, ${limit}` : limit;
      return ` LIMIT ${limitOffset}`;
    }
    return '';
  }

  /**
   * Simulates TIMESTAMP_TRUNC using TIMESTAMPDIFF and DATE_ADD math.
   */
  public timestampTruncSql (expression: TimestampTruncExpr): string {
    const unit = expression.args.unit;
    const startTs = '\'0000-01-01 00:00:00\'';

    // Calculate diff between 0000-01-01 and target, then add that interval back to the base
    const timestampDiff = buildDateDelta(TimestampDiffExpr)([
      unit as Expression,
      new LiteralExpr({
        this: startTs,
        isString: true,
      }),
      expression.args.this as Expression,
    ]);
    const interval = new IntervalExpr({
      this: timestampDiff,
      unit: unit,
    });
    const dateAdd = buildDateDeltaWithInterval(DateAddExpr)([
      new LiteralExpr({
        this: startTs,
        isString: true,
      }),
      interval,
    ]);

    return this.sql(dateAdd);
  }

  public converttimezoneSql (expression: ConvertTimezoneExpr): string {
    const fromTz = expression.args.sourceTz;
    const toTz = expression.args.targetTz;
    const dt = expression.args.timestamp;

    return this.func('CONVERT_TZ', [
      dt,
      fromTz,
      toTz,
    ] as Expression[]);
  }

  public attimezoneSql (expression: AtTimeZoneExpr): string {
    this.unsupported('AT TIME ZONE is not supported by MySQL');
    return this.sql(expression.args.this);
  }

  public isasciiSql (expression: IsAsciiExpr): string {
    return `REGEXP_LIKE(${this.sql(expression.args.this)}, '^[[:ascii:]]*$')`;
  }

  public ignoreundefinedsSql (expression: IgnoreNullsExpr): string {
    this.unsupported('MySQL does not support IGNORE NULLS.');
    return this.sql(expression.args.this);
  }

  @unsupportedArgs('this')
  public currentschemaSql (_expression: CurrentSchemaExpr): string {
    return this.func('SCHEMA', []);
  }

  public partitionSql (expression: PartitionExpr): string {
    const parent = expression.parent;
    if (parent instanceof PartitionByRangePropertyExpr || parent instanceof PartitionByListPropertyExpr) {
      return this.expressions(expression, { flat: true });
    }
    return super.partitionSql(expression);
  }

  public partitionByRangeOrListSql (
    expression: PartitionByRangePropertyExpr | PartitionByListPropertyExpr,
    kind: string,
  ): string {
    const partitions = this.expressions(expression, {
      key: 'partitionExpressions',
      flat: true,
    });
    const create = this.expressions(expression, {
      key: 'createExpressions',
      flat: true,
    });
    return `PARTITION BY ${kind} (${partitions}) (${create})`;
  }

  public partitionByRangePropertySql (expression: PartitionByRangePropertyExpr): string {
    return this.partitionByRangeOrListSql(expression, 'RANGE');
  }

  public partitionByListPropertySql (expression: PartitionByListPropertyExpr): string {
    return this.partitionByRangeOrListSql(expression, 'LIST');
  }

  public partitionListSql (expression: PartitionListExpr): string {
    const name = this.sql(expression, 'this');
    const values = this.expressions(expression, { flat: true });
    return `PARTITION ${name} VALUES IN (${values})`;
  }

  public partitionRangeSql (expression: PartitionRangeExpr): string {
    const name = this.sql(expression, 'this');
    const values = this.expressions(expression, { flat: true });
    return `PARTITION ${name} VALUES LESS THAN (${values})`;
  }
}

export class MySQL extends Dialect {
  public static PROMOTE_TO_INFERRED_DATETIME_TYPE: boolean = true;

  // MySQL allows identifiers to start with digits if they are quoted or in specific contexts
  // Reference: https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
  public static IDENTIFIERS_CAN_START_WITH_DIGIT: boolean = true;

  /**
   * We default to treating all identifiers as case-sensitive, since it matches MySQL's
   * behavior on Linux systems. For MacOS and Windows systems, one can override this
   * setting by specifying `dialect="mysql, normalization_strategy = lowercase"`.
   * * Reference: https://dev.mysql.com/doc/refman/8.2/en/identifier-case-sensitivity.html
   */
  public static NORMALIZATION_STRATEGY: NormalizationStrategy = NormalizationStrategy.CASE_SENSITIVE;

  public static TIME_FORMAT: string = '\'%Y-%m-%d %T\'';
  public static DPIPE_IS_STRING_CONCAT: boolean = false;
  public static SUPPORTS_USER_DEFINED_TYPES: boolean = false;
  public static SUPPORTS_SEMI_ANTI_JOIN: boolean = false;
  public static SAFE_DIVISION: boolean = true;
  public static SAFE_TO_ELIMINATE_DOUBLE_NEGATION: boolean = false;
  public static LEAST_GREATEST_IGNORES_NULLS: boolean = false;

  public static EXPRESSION_METADATA: ExpressionMetadata = { ...BASE_EXPRESSION_METADATA };

  /**
   * MySQL-specific time format mapping.
   * Reference: https://prestodb.io/docs/current/functions/datetime.html#mysql-date-functions
   */
  public static TIME_MAPPING: Record<string, string> = {
    '%M': '%B',
    '%c': '%-m',
    '%e': '%-d',
    '%h': '%I',
    '%i': '%M',
    '%s': '%S',
    '%u': '%W',
    '%k': '%-H',
    '%l': '%-I',
    '%T': '%H:%M:%S',
    '%W': '%A',
  };

  /**
   * Valid interval units supported by MySQL.
   * Includes standard units plus MySQL-specific compound units.
   */
  public static VALID_INTERVAL_UNITS: Set<string> = new Set([
    ...Dialect.VALID_INTERVAL_UNITS,
    'SECOND_MICROSECOND',
    'MINUTE_MICROSECOND',
    'MINUTE_SECOND',
    'HOUR_MICROSECOND',
    'HOUR_SECOND',
    'HOUR_MINUTE',
    'DAY_MICROSECOND',
    'DAY_SECOND',
    'DAY_MINUTE',
    'DAY_HOUR',
    'YEAR_MONTH',
  ]);

  static Tokenizer = MySQLTokenizer;
  static Parser = MySQLParser;
  static Generator = MySQLGenerator;
}
Dialect.register(Dialects.MYSQL, MySQL);
