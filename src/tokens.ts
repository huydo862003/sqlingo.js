// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/tokens.py

import type { DialectType } from './dialects';
import { Dialect } from './dialects';
import { TokenError } from './errors';
import {
  inTrie, newTrie, TrieResult, type TrieNode,
} from './trie';

/**
 * Represents a syntax token's tag.
 *
 */
export enum TokenType {
  L_PAREN = 'L_PAREN',
  R_PAREN = 'R_PAREN',
  L_BRACKET = 'L_BRACKET',
  R_BRACKET = 'R_BRACKET',
  L_BRACE = 'L_BRACE',
  R_BRACE = 'R_BRACE',
  COMMA = 'COMMA',
  DOT = 'DOT',
  DASH = 'DASH',
  PLUS = 'PLUS',
  COLON = 'COLON',
  DOTCOLON = 'DOTCOLON',
  DCOLON = 'DCOLON',
  DCOLONDOLLAR = 'DCOLONDOLLAR',
  DCOLONPERCENT = 'DCOLONPERCENT',
  DCOLONQMARK = 'DCOLONQMARK',
  DQMARK = 'DQMARK',
  SEMICOLON = 'SEMICOLON',
  STAR = 'STAR',
  BACKSLASH = 'BACKSLASH',
  SLASH = 'SLASH',
  LT = 'LT',
  LTE = 'LTE',
  GT = 'GT',
  GTE = 'GTE',
  NOT = 'NOT',
  EQ = 'EQ',
  NEQ = 'NEQ',
  NULLSAFE_EQ = 'NULLSAFE_EQ',
  COLON_EQ = 'COLON_EQ',
  COLON_GT = 'COLON_GT',
  NCOLON_GT = 'NCOLON_GT',
  AND = 'AND',
  OR = 'OR',
  AMP = 'AMP',
  DPIPE = 'DPIPE',
  PIPE_GT = 'PIPE_GT',
  PIPE = 'PIPE',
  PIPE_SLASH = 'PIPE_SLASH',
  DPIPE_SLASH = 'DPIPE_SLASH',
  CARET = 'CARET',
  CARET_AT = 'CARET_AT',
  TILDE = 'TILDE',
  ARROW = 'ARROW',
  DARROW = 'DARROW',
  FARROW = 'FARROW',
  HASH = 'HASH',
  HASH_ARROW = 'HASH_ARROW',
  DHASH_ARROW = 'DHASH_ARROW',
  LR_ARROW = 'LR_ARROW',
  DAT = 'DAT',
  LT_AT = 'LT_AT',
  AT_GT = 'AT_GT',
  DOLLAR = 'DOLLAR',
  PARAMETER = 'PARAMETER',
  SESSION = 'SESSION',
  SESSION_PARAMETER = 'SESSION_PARAMETER',
  SESSION_USER = 'SESSION_USER',
  DAMP = 'DAMP',
  AMP_LT = 'AMP_LT',
  AMP_GT = 'AMP_GT',
  ADJACENT = 'ADJACENT',
  XOR = 'XOR',
  DSTAR = 'DSTAR',
  QMARK_AMP = 'QMARK_AMP',
  QMARK_PIPE = 'QMARK_PIPE',
  HASH_DASH = 'HASH_DASH',
  EXCLAMATION = 'EXCLAMATION',

  URI_START = 'URI_START',

  BLOCK_START = 'BLOCK_START',
  BLOCK_END = 'BLOCK_END',

  SPACE = 'SPACE',
  BREAK = 'BREAK',

  STRING = 'STRING',
  NUMBER = 'NUMBER',
  IDENTIFIER = 'IDENTIFIER',
  DATABASE = 'DATABASE',
  COLUMN = 'COLUMN',
  COLUMN_DEF = 'COLUMN_DEF',
  SCHEMA = 'SCHEMA',
  TABLE = 'TABLE',
  WAREHOUSE = 'WAREHOUSE',
  STAGE = 'STAGE',
  STREAMLIT = 'STREAMLIT',
  VAR = 'VAR',
  BIT_STRING = 'BIT_STRING',
  HEX_STRING = 'HEX_STRING',
  BYTE_STRING = 'BYTE_STRING',
  NATIONAL_STRING = 'NATIONAL_STRING',
  RAW_STRING = 'RAW_STRING',
  HEREDOC_STRING = 'HEREDOC_STRING',
  UNICODE_STRING = 'UNICODE_STRING',

  // types
  BIT = 'BIT',
  BOOLEAN = 'BOOLEAN',
  TINYINT = 'TINYINT',
  UTINYINT = 'UTINYINT',
  SMALLINT = 'SMALLINT',
  USMALLINT = 'USMALLINT',
  MEDIUMINT = 'MEDIUMINT',
  UMEDIUMINT = 'UMEDIUMINT',
  INT = 'INT',
  UINT = 'UINT',
  BIGINT = 'BIGINT',
  UBIGINT = 'UBIGINT',
  BIGNUM = 'BIGNUM',
  INT128 = 'INT128',
  UINT128 = 'UINT128',
  INT256 = 'INT256',
  UINT256 = 'UINT256',
  FLOAT = 'FLOAT',
  DOUBLE = 'DOUBLE',
  UDOUBLE = 'UDOUBLE',
  DECIMAL = 'DECIMAL',
  DECIMAL32 = 'DECIMAL32',
  DECIMAL64 = 'DECIMAL64',
  DECIMAL128 = 'DECIMAL128',
  DECIMAL256 = 'DECIMAL256',
  DECFLOAT = 'DECFLOAT',
  UDECIMAL = 'UDECIMAL',
  BIGDECIMAL = 'BIGDECIMAL',
  CHAR = 'CHAR',
  NCHAR = 'NCHAR',
  VARCHAR = 'VARCHAR',
  NVARCHAR = 'NVARCHAR',
  BPCHAR = 'BPCHAR',
  TEXT = 'TEXT',
  MEDIUMTEXT = 'MEDIUMTEXT',
  LONGTEXT = 'LONGTEXT',
  BLOB = 'BLOB',
  MEDIUMBLOB = 'MEDIUMBLOB',
  LONGBLOB = 'LONGBLOB',
  TINYBLOB = 'TINYBLOB',
  TINYTEXT = 'TINYTEXT',
  NAME = 'NAME',
  BINARY = 'BINARY',
  VARBINARY = 'VARBINARY',
  JSON = 'JSON',
  JSONB = 'JSONB',
  TIME = 'TIME',
  TIMETZ = 'TIMETZ',
  TIME_NS = 'TIME_NS',
  TIMESTAMP = 'TIMESTAMP',
  TIMESTAMPTZ = 'TIMESTAMPTZ',
  TIMESTAMPLTZ = 'TIMESTAMPLTZ',
  TIMESTAMPNTZ = 'TIMESTAMPNTZ',
  TIMESTAMP_S = 'TIMESTAMP_S',
  TIMESTAMP_MS = 'TIMESTAMP_MS',
  TIMESTAMP_NS = 'TIMESTAMP_NS',
  DATETIME = 'DATETIME',
  DATETIME2 = 'DATETIME2',
  DATETIME64 = 'DATETIME64',
  SMALLDATETIME = 'SMALLDATETIME',
  DATE = 'DATE',
  DATE32 = 'DATE32',
  INT4RANGE = 'INT4RANGE',
  INT4MULTIRANGE = 'INT4MULTIRANGE',
  INT8RANGE = 'INT8RANGE',
  INT8MULTIRANGE = 'INT8MULTIRANGE',
  NUMRANGE = 'NUMRANGE',
  NUMMULTIRANGE = 'NUMMULTIRANGE',
  TSRANGE = 'TSRANGE',
  TSMULTIRANGE = 'TSMULTIRANGE',
  TSTZRANGE = 'TSTZRANGE',
  TSTZMULTIRANGE = 'TSTZMULTIRANGE',
  DATERANGE = 'DATERANGE',
  DATEMULTIRANGE = 'DATEMULTIRANGE',
  UUID = 'UUID',
  GEOGRAPHY = 'GEOGRAPHY',
  GEOGRAPHYPOINT = 'GEOGRAPHYPOINT',
  NULLABLE = 'NULLABLE',
  GEOMETRY = 'GEOMETRY',
  POINT = 'POINT',
  RING = 'RING',
  LINESTRING = 'LINESTRING',
  LOCALTIME = 'LOCALTIME',
  LOCALTIMESTAMP = 'LOCALTIMESTAMP',
  SYSTIMESTAMP = 'SYSTIMESTAMP',
  MULTILINESTRING = 'MULTILINESTRING',
  POLYGON = 'POLYGON',
  MULTIPOLYGON = 'MULTIPOLYGON',
  HLLSKETCH = 'HLLSKETCH',
  HSTORE = 'HSTORE',
  SUPER = 'SUPER',
  SERIAL = 'SERIAL',
  SMALLSERIAL = 'SMALLSERIAL',
  BIGSERIAL = 'BIGSERIAL',
  XML = 'XML',
  YEAR = 'YEAR',
  USERDEFINED = 'USERDEFINED',
  MONEY = 'MONEY',
  SMALLMONEY = 'SMALLMONEY',
  ROWVERSION = 'ROWVERSION',
  IMAGE = 'IMAGE',
  VARIANT = 'VARIANT',
  OBJECT = 'OBJECT',
  INET = 'INET',
  IPADDRESS = 'IPADDRESS',
  IPPREFIX = 'IPPREFIX',
  IPV4 = 'IPV4',
  IPV6 = 'IPV6',
  ENUM = 'ENUM',
  ENUM8 = 'ENUM8',
  ENUM16 = 'ENUM16',
  FIXEDSTRING = 'FIXEDSTRING',
  LOWCARDINALITY = 'LOWCARDINALITY',
  NESTED = 'NESTED',
  AGGREGATEFUNCTION = 'AGGREGATEFUNCTION',
  SIMPLEAGGREGATEFUNCTION = 'SIMPLEAGGREGATEFUNCTION',
  TDIGEST = 'TDIGEST',
  UNKNOWN = 'UNKNOWN',
  VECTOR = 'VECTOR',
  DYNAMIC = 'DYNAMIC',
  VOID = 'VOID',

  // keywords
  ALIAS = 'ALIAS',
  ALTER = 'ALTER',
  ALL = 'ALL',
  ANTI = 'ANTI',
  ANY = 'ANY',
  APPLY = 'APPLY',
  ARRAY = 'ARRAY',
  ASC = 'ASC',
  ASOF = 'ASOF',
  ATTACH = 'ATTACH',
  AUTO_INCREMENT = 'AUTO_INCREMENT',
  BEGIN = 'BEGIN',
  BETWEEN = 'BETWEEN',
  BULK_COLLECT_INTO = 'BULK_COLLECT_INTO',
  CACHE = 'CACHE',
  CASE = 'CASE',
  CHARACTER_SET = 'CHARACTER_SET',
  CLUSTER_BY = 'CLUSTER_BY',
  COLLATE = 'COLLATE',
  COMMAND = 'COMMAND',
  COMMENT = 'COMMENT',
  COMMIT = 'COMMIT',
  CONNECT_BY = 'CONNECT_BY',
  CONSTRAINT = 'CONSTRAINT',
  COPY = 'COPY',
  CREATE = 'CREATE',
  CROSS = 'CROSS',
  CUBE = 'CUBE',
  CURRENT_DATE = 'CURRENT_DATE',
  CURRENT_DATETIME = 'CURRENT_DATETIME',
  CURRENT_SCHEMA = 'CURRENT_SCHEMA',
  CURRENT_TIME = 'CURRENT_TIME',
  CURRENT_TIMESTAMP = 'CURRENT_TIMESTAMP',
  CURRENT_USER = 'CURRENT_USER',
  CURRENT_ROLE = 'CURRENT_ROLE',
  CURRENT_CATALOG = 'CURRENT_CATALOG',
  DECLARE = 'DECLARE',
  DEFAULT = 'DEFAULT',
  DELETE = 'DELETE',
  DESC = 'DESC',
  DESCRIBE = 'DESCRIBE',
  DETACH = 'DETACH',
  DICTIONARY = 'DICTIONARY',
  DISTINCT = 'DISTINCT',
  DISTRIBUTE_BY = 'DISTRIBUTE_BY',
  DIV = 'DIV',
  DROP = 'DROP',
  ELSE = 'ELSE',
  END = 'END',
  ESCAPE = 'ESCAPE',
  EXCEPT = 'EXCEPT',
  EXECUTE = 'EXECUTE',
  EXISTS = 'EXISTS',
  FALSE = 'FALSE',
  FETCH = 'FETCH',
  FILE = 'FILE',
  FILE_FORMAT = 'FILE_FORMAT',
  FILTER = 'FILTER',
  FINAL = 'FINAL',
  FIRST = 'FIRST',
  FOR = 'FOR',
  FORCE = 'FORCE',
  FOREIGN_KEY = 'FOREIGN_KEY',
  FORMAT = 'FORMAT',
  FROM = 'FROM',
  FULL = 'FULL',
  FUNCTION = 'FUNCTION',
  GET = 'GET',
  GLOB = 'GLOB',
  GLOBAL = 'GLOBAL',
  GRANT = 'GRANT',
  GROUP_BY = 'GROUP_BY',
  GROUPING_SETS = 'GROUPING_SETS',
  HAVING = 'HAVING',
  HINT = 'HINT',
  IGNORE = 'IGNORE',
  ILIKE = 'ILIKE',
  IN = 'IN',
  INDEX = 'INDEX',
  INDEXED_BY = 'INDEXED_BY',
  INNER = 'INNER',
  INSERT = 'INSERT',
  INSTALL = 'INSTALL',
  INTERSECT = 'INTERSECT',
  INTERVAL = 'INTERVAL',
  INTO = 'INTO',
  INTRODUCER = 'INTRODUCER',
  IRLIKE = 'IRLIKE',
  IS = 'IS',
  ISNULL = 'ISNULL',
  JOIN = 'JOIN',
  JOIN_MARKER = 'JOIN_MARKER',
  KEEP = 'KEEP',
  KEY = 'KEY',
  KILL = 'KILL',
  LANGUAGE = 'LANGUAGE',
  LATERAL = 'LATERAL',
  LEFT = 'LEFT',
  LIKE = 'LIKE',
  LIMIT = 'LIMIT',
  LIST = 'LIST',
  LOAD = 'LOAD',
  LOCK = 'LOCK',
  MAP = 'MAP',
  MATCH = 'MATCH',
  MATCH_CONDITION = 'MATCH_CONDITION',
  MATCH_RECOGNIZE = 'MATCH_RECOGNIZE',
  MEMBER_OF = 'MEMBER_OF',
  MERGE = 'MERGE',
  MOD = 'MOD',
  MODEL = 'MODEL',
  NATURAL = 'NATURAL',
  NEXT = 'NEXT',
  NOTHING = 'NOTHING',
  NOTNULL = 'NOTNULL',
  NULL = 'NULL',
  OBJECT_IDENTIFIER = 'OBJECT_IDENTIFIER',
  OFFSET = 'OFFSET',
  ON = 'ON',
  ONLY = 'ONLY',
  OPERATOR = 'OPERATOR',
  ORDER_BY = 'ORDER_BY',
  ORDER_SIBLINGS_BY = 'ORDER_SIBLINGS_BY',
  ORDERED = 'ORDERED',
  ORDINALITY = 'ORDINALITY',
  OUT = 'OUT',
  INOUT = 'INOUT',
  OUTER = 'OUTER',
  OVER = 'OVER',
  OVERLAPS = 'OVERLAPS',
  OVERWRITE = 'OVERWRITE',
  PARTITION = 'PARTITION',
  PARTITION_BY = 'PARTITION_BY',
  PERCENT = 'PERCENT',
  PIVOT = 'PIVOT',
  PLACEHOLDER = 'PLACEHOLDER',
  POSITIONAL = 'POSITIONAL',
  PRAGMA = 'PRAGMA',
  PREWHERE = 'PREWHERE',
  PRIMARY_KEY = 'PRIMARY_KEY',
  PROCEDURE = 'PROCEDURE',
  PROPERTIES = 'PROPERTIES',
  PSEUDO_TYPE = 'PSEUDO_TYPE',
  PUT = 'PUT',
  QUALIFY = 'QUALIFY',
  QUOTE = 'QUOTE',
  QDCOLON = 'QDCOLON',
  RANGE = 'RANGE',
  RECURSIVE = 'RECURSIVE',
  REFRESH = 'REFRESH',
  RENAME = 'RENAME',
  REPLACE = 'REPLACE',
  RETURNING = 'RETURNING',
  REVOKE = 'REVOKE',
  REFERENCES = 'REFERENCES',
  RIGHT = 'RIGHT',
  RLIKE = 'RLIKE',
  ROLLBACK = 'ROLLBACK',
  ROLLUP = 'ROLLUP',
  ROW = 'ROW',
  ROWS = 'ROWS',
  SELECT = 'SELECT',
  SEMI = 'SEMI',
  SEPARATOR = 'SEPARATOR',
  SEQUENCE = 'SEQUENCE',
  SERDE_PROPERTIES = 'SERDE_PROPERTIES',
  SET = 'SET',
  SETTINGS = 'SETTINGS',
  SHOW = 'SHOW',
  SIMILAR_TO = 'SIMILAR_TO',
  SOME = 'SOME',
  SORT_BY = 'SORT_BY',
  SOUNDS_LIKE = 'SOUNDS_LIKE',
  START_WITH = 'START_WITH',
  STORAGE_INTEGRATION = 'STORAGE_INTEGRATION',
  STRAIGHT_JOIN = 'STRAIGHT_JOIN',
  STRUCT = 'STRUCT',
  SUMMARIZE = 'SUMMARIZE',
  TABLE_SAMPLE = 'TABLE_SAMPLE',
  TAG = 'TAG',
  TEMPORARY = 'TEMPORARY',
  TOP = 'TOP',
  THEN = 'THEN',
  TRUE = 'TRUE',
  TRUNCATE = 'TRUNCATE',
  UNCACHE = 'UNCACHE',
  UNION = 'UNION',
  UNNEST = 'UNNEST',
  UNPIVOT = 'UNPIVOT',
  UPDATE = 'UPDATE',
  USE = 'USE',
  USING = 'USING',
  VALUES = 'VALUES',
  VARIADIC = 'VARIADIC',
  VIEW = 'VIEW',
  SEMANTIC_VIEW = 'SEMANTIC_VIEW',
  VOLATILE = 'VOLATILE',
  WHEN = 'WHEN',
  WHERE = 'WHERE',
  WINDOW = 'WINDOW',
  WITH = 'WITH',
  UNIQUE = 'UNIQUE',
  UTC_DATE = 'UTC_DATE',
  UTC_TIME = 'UTC_TIME',
  UTC_TIMESTAMP = 'UTC_TIMESTAMP',
  VERSION_SNAPSHOT = 'VERSION_SNAPSHOT',
  TIMESTAMP_SNAPSHOT = 'TIMESTAMP_SNAPSHOT',
  OPTION = 'OPTION',
  SINK = 'SINK',
  SOURCE = 'SOURCE',
  ANALYZE = 'ANALYZE',
  NAMESPACE = 'NAMESPACE',
  EXPORT = 'EXPORT',

  // sentinel
  HIVE_TOKEN_STREAM = 'HIVE_TOKEN_STREAM',
}

/**
 * Represents a single token in the SQL lexical analysis.
 *
 */
export class Token {
  tokenType: TokenType;
  text: string;
  line: number;
  col: number;
  start: number;
  end: number;
  comments: string[];

  /**
   * Creates a token representing a numeric literal.
   *
   * @example Token.number(42)
   */
  static number (number: number): Token {
    return new Token(TokenType.NUMBER, String(number));
  }

  /**
   * Creates a token representing a string literal.
   *
   * @example Token.string("hello")
   */
  static string (string: string): Token {
    return new Token(TokenType.STRING, string);
  }

  /**
   * Creates a token representing an identifier.
   *
   * @example Token.identifier("users")
   */
  static identifier (identifier: string): Token {
    return new Token(TokenType.IDENTIFIER, identifier);
  }

  constructor (
    tokenType: TokenType,
    text: string,
    line: number = 1,
    col: number = 1,
    start: number = 0,
    end: number = 0,
    comments: string[] = [],
  ) {
    this.tokenType = tokenType;
    this.text = text;
    this.line = line;
    this.col = col;
    this.start = start;
    this.end = end;
    this.comments = comments;
  }
}

/**
 * Token pair type for denoting opening/closing delimiters.
 */
type TokenPair = string | [string, string];

/**
 * Base tokenizer class for SQL lexical analysis.
 *
 * Tokenizer = sqlglot's _Tokenizer + Tokenizer
 *
 */
export class Tokenizer {
  /**
   * Singular token texts to token types
   *
   */
  static SINGLE_TOKENS: Record<string, TokenType> = {
    '(': TokenType.L_PAREN,
    ')': TokenType.R_PAREN,
    '[': TokenType.L_BRACKET,
    ']': TokenType.R_BRACKET,
    '{': TokenType.L_BRACE,
    '}': TokenType.R_BRACE,
    '&': TokenType.AMP,
    '^': TokenType.CARET,
    ':': TokenType.COLON,
    ',': TokenType.COMMA,
    '.': TokenType.DOT,
    '-': TokenType.DASH,
    '=': TokenType.EQ,
    '>': TokenType.GT,
    '<': TokenType.LT,
    '%': TokenType.MOD,
    '!': TokenType.NOT,
    '|': TokenType.PIPE,
    '+': TokenType.PLUS,
    ';': TokenType.SEMICOLON,
    '/': TokenType.SLASH,
    '\\': TokenType.BACKSLASH,
    '*': TokenType.STAR,
    '~': TokenType.TILDE,
    '?': TokenType.PLACEHOLDER,
    '@': TokenType.PARAMETER,
    '#': TokenType.HASH,
    // Used for breaking a var like x'y' but nothing else the token type doesn't matter
    '\'': TokenType.UNKNOWN,
    '`': TokenType.UNKNOWN,
    '"': TokenType.UNKNOWN,
  };

  /**
   * Bit string prefixes.
   *
   * @example [["b'", "'"], ["B'", "'"]]
   */
  static BIT_STRINGS: TokenPair[] = [];

  /**
   * Byte string prefixes.
   *
   * @example [["b'", "'"], ["B'", "'"]]
   */
  static BYTE_STRINGS: TokenPair[] = [];

  /**
   * Hex string prefixes.
   *
   * @example [["x'", "'"], ["X'", "'"], ["0x", ""]]
   */
  static HEX_STRINGS: TokenPair[] = [];

  /**
   * Raw string prefixes.
   *
   * @example [["r'", "'"], ["R'", "'"]]
   */
  static RAW_STRINGS: TokenPair[] = [];

  /**
   * Heredoc string prefixes.
   *
   * @example [["$$", "$$"]]
   */
  static HEREDOC_STRINGS: TokenPair[] = [];

  /**
   * Identifier quote delimiters.
   *
   * @example ['"', '`', ['[', ']']]
   */
  static IDENTIFIERS: TokenPair[] = ['"'];

  /**
   * String quote delimiters.
   *
   * @example ["'"]
   */
  static QUOTES: TokenPair[] = ['\''];

  /**
   * Unicode string prefixes.
   *
   * @example [["u'", "'"], ["U'", "'"]]
   */
  static UNICODE_STRINGS: TokenPair[] = [];

  /**
   * Characters that can be escaped in strings.
   *
   * @example ["'", "\\"]
   */
  static STRING_ESCAPES: string[] = ['\''];

  /**
   * Characters that can be escaped in byte strings.
   *
   * @example ["\\"]
   */
  static BYTE_STRING_ESCAPES (): string[] {
    return this.STRING_ESCAPES;
  }

  /**
   * Set of single-character tokens that can appear in variable names.
   *
   * Used to break variable-like tokens when these characters are encountered.
   *
   * @example new Set(['@', '$'])
   */
  static VAR_SINGLE_TOKENS: Set<string> = new Set();

  /**
   * Characters that can follow an escape character.
   *
   * @example ["n", "t", "r", "\\"]
   */
  static ESCAPE_FOLLOW_CHARS: string[] = [];

  /**
   * Characters that can be escaped in identifiers.
   *
   * @example ['"']
   */
  static IDENTIFIER_ESCAPES: string[] = [];

  /**
   * Whether the heredoc tags follow the same lexical rules as unquoted identifiers.
   *
   */
  static HEREDOC_TAG_IS_IDENTIFIER = false;

  /**
   * Token that we'll generate as a fallback if the heredoc prefix doesn't correspond to a heredoc.
   *
   */
  static HEREDOC_STRING_ALTERNATIVE = TokenType.VAR;

  /**
   * Whether string escape characters function as such when placed within raw strings.
   *
   */
  static STRING_ESCAPES_ALLOWED_IN_RAW_STRINGS = true;

  /**
   * Whether nested comments are supported.
   *
   */
  static NESTED_COMMENTS = true;

  /**
   * The string that marks the start of a hint comment.
   *
   */
  static HINT_START = '/*+';

  /**
   * Set of token types that can precede a hint.
   *
   */
  static TOKENS_PRECEDING_HINT = new Set([
    TokenType.SELECT,
    TokenType.INSERT,
    TokenType.UPDATE,
    TokenType.DELETE,
  ]);

  protected static commentsCache = new WeakMap<typeof Tokenizer, Record<string, string | undefined>>();

  /**
   * Dictionary mapping comment start delimiters to end delimiters (or null for single-line comments).
   *
   * Includes Jinja comments ({# #}) by default.
   *
   * @example { '--': null, '{#': '#}' }
   */
  static _COMMENTS (): Record<string, string | undefined> {
    let cached = this.commentsCache.get(this);
    if (!cached) {
      cached = {};

      // Convert COMMENTS array to dictionary format
      for (const comment of this.COMMENTS) {
        if (typeof comment === 'string') {
          // Single-line comment (e.g., '--')
          cached[comment] = undefined;
        } else {
          // Multi-line comment (e.g., ['/*', '*/'])
          cached[comment[0]] = comment[1];
        }
      }

      // Ensure Jinja comments are tokenized correctly in all dialects
      cached['{#'] = '#}';

      this.commentsCache.set(this, cached);
    }
    return cached;
  }

  protected static formatStringsCache = new WeakMap<typeof Tokenizer, Record<string, [string, TokenType]>>();

  /**
   * Dictionary mapping format string prefixes to [closing delimiter, token type] tuples.
   */
  static _FORMAT_STRINGS (): Record<string, [string, TokenType]> {
    let cached = this.formatStringsCache.get(this);
    if (!cached) {
      // Generate national strings by prefixing 'n' or 'N' to all quotes
      const nationalStrings: Record<string, [string, TokenType]> = {};
      for (const [start, end] of Object.entries(this._QUOTES())) {
        for (const prefix of ['n', 'N']) {
          nationalStrings[prefix + start] = [end, TokenType.STRING];
        }
      }

      cached = {
        ...nationalStrings,
        ...this._quotesToFormat(TokenType.STRING, this.BIT_STRINGS),
        ...this._quotesToFormat(TokenType.STRING, this.BYTE_STRINGS),
        ...this._quotesToFormat(TokenType.STRING, this.HEX_STRINGS),
        ...this._quotesToFormat(TokenType.STRING, this.RAW_STRINGS),
        ...this._quotesToFormat(TokenType.STRING, this.HEREDOC_STRINGS),
        ...this._quotesToFormat(TokenType.STRING, this.UNICODE_STRINGS),
      };
      this.formatStringsCache.set(this, cached);
    }
    return cached;
  }

  protected static identifiersCache = new WeakMap<typeof Tokenizer, Record<string, string>>();

  /**
   * Dictionary mapping opening identifier delimiters to closing delimiters.
   *
   * @example { '"': '"', '`': '`' }
   */
  static _IDENTIFIERS (): Record<string, string> {
    let cached = this.identifiersCache.get(this);
    if (!cached) {
      cached = this._convertQuotes(this.IDENTIFIERS);
      this.identifiersCache.set(this, cached);
    }
    return cached;
  }

  protected static identifierEscapesCache = new WeakMap<typeof Tokenizer, Set<string>>();

  /**
   * Set of characters that can be escaped in identifiers.
   *
   * @example new Set(['"'])
   */
  static _IDENTIFIER_ESCAPES (): Set<string> {
    let cached = this.identifierEscapesCache.get(this);
    if (!cached) {
      cached = new Set(this.IDENTIFIER_ESCAPES);
      this.identifierEscapesCache.set(this, cached);
    }
    return cached;
  }

  protected static quotesCache = new WeakMap<typeof Tokenizer, Record<string, string>>();

  /**
   * Dictionary mapping opening quote delimiters to closing delimiters.
   *
   * @example { "'": "'", '"': '"' }
   */
  static _QUOTES (): Record<string, string> {
    let cached = this.quotesCache.get(this);
    if (!cached) {
      cached = this._convertQuotes(this.QUOTES);
      this.quotesCache.set(this, cached);
    }
    return cached;
  }

  protected static stringEscapesCache = new WeakMap<typeof Tokenizer, Set<string>>();

  /**
   * Set of characters that can be escaped in strings.
   *
   * @example new Set(["'", "\\"])
   */
  static _STRING_ESCAPES (): Set<string> {
    let cached = this.stringEscapesCache.get(this);
    if (!cached) {
      cached = new Set(this.STRING_ESCAPES);
      this.stringEscapesCache.set(this, cached);
    }
    return cached;
  }

  protected static byteStringEscapesCache = new WeakMap<typeof Tokenizer, Set<string>>();

  /**
   * Set of characters that can be escaped in byte strings.
   *
   * @example new Set(["\\"])
   */
  static _BYTE_STRING_ESCAPES (): Set<string> {
    let cached = this.byteStringEscapesCache.get(this);
    if (!cached) {
      cached = new Set(this.BYTE_STRING_ESCAPES());
      this.byteStringEscapesCache.set(this, cached);
    }
    return cached;
  }

  protected static escapeFollowCharsCache = new WeakMap<typeof Tokenizer, Set<string>>();

  /**
   * Set of characters that can follow an escape character.
   *
   * @example new Set(["n", "t", "r"])
   */
  static _ESCAPE_FOLLOW_CHARS (): Set<string> {
    let cached = this.escapeFollowCharsCache.get(this);
    if (!cached) {
      cached = new Set(this.ESCAPE_FOLLOW_CHARS);
      this.escapeFollowCharsCache.set(this, cached);
    }
    return cached;
  }

  protected static keywordTrieCache = new WeakMap<typeof Tokenizer, TrieNode>();

  /**
   * Trie structure for efficient keyword matching.
   *
   * Contains keywords that have spaces or contain single-token characters,
   * allowing for efficient prefix-based scanning of multi-character keywords.
   */
  static _KEYWORD_TRIE (): TrieNode {
    let cached = this.keywordTrieCache.get(this);
    if (!cached) {
      const singleTokenChars = Object.keys(this.SINGLE_TOKENS);
      const keys = [
        ...Object.keys(this.KEYWORDS()),
        ...Object.keys(this._COMMENTS()),
        ...Object.keys(this._QUOTES()),
        ...Object.keys(this._FORMAT_STRINGS()),
      ];

      cached = newTrie(
        keys
          .filter((key) => key.includes(' ') || singleTokenChars.some((c) => key.includes(c)))
          .map((key) => Array.from(key.toUpperCase())),
      );

      this.keywordTrieCache.set(this, cached);
    }
    return cached;
  }

  protected static keywordsCache = new WeakMap<typeof Tokenizer, Record<string, TokenType>>();

  /**
   * Map of SQL keywords and operators to their corresponding token types.
   *
   * Lazily computed and cached per class.
   *
   * @example { 'SELECT': TokenType.SELECT, 'FROM': TokenType.FROM }
   */
  static KEYWORDS (): Record<string, TokenType> {
    let cached = this.keywordsCache.get(this);
    if (!cached) {
      cached = {
        '{%': TokenType.BLOCK_START,
        '{%+': TokenType.BLOCK_START,
        '{%-': TokenType.BLOCK_START,
        '%}': TokenType.BLOCK_END,
        '+%}': TokenType.BLOCK_END,
        '-%}': TokenType.BLOCK_END,
        '{{+': TokenType.BLOCK_START,
        '{{-': TokenType.BLOCK_START,
        '+}}': TokenType.BLOCK_END,
        '-}}': TokenType.BLOCK_END,
        [this.HINT_START]: TokenType.HINT,
        '&<': TokenType.AMP_LT,
        '&>': TokenType.AMP_GT,
        '==': TokenType.EQ,
        '::': TokenType.DCOLON,
        '?::': TokenType.QDCOLON,
        '||': TokenType.DPIPE,
        '|>': TokenType.PIPE_GT,
        '>=': TokenType.GTE,
        '<=': TokenType.LTE,
        '<>': TokenType.NEQ,
        '!=': TokenType.NEQ,
        ':=': TokenType.COLON_EQ,
        '<=>': TokenType.NULLSAFE_EQ,
        '->': TokenType.ARROW,
        '->>': TokenType.DARROW,
        '=>': TokenType.FARROW,
        '#>': TokenType.HASH_ARROW,
        '#>>': TokenType.DHASH_ARROW,
        '<->': TokenType.LR_ARROW,
        '&&': TokenType.DAMP,
        '??': TokenType.DQMARK,
        '~~~': TokenType.GLOB,
        '~~': TokenType.LIKE,
        '~~*': TokenType.ILIKE,
        '~*': TokenType.IRLIKE,
        '-|-': TokenType.ADJACENT,
        'ALL': TokenType.ALL,
        'AND': TokenType.AND,
        'ANTI': TokenType.ANTI,
        'ANY': TokenType.ANY,
        'ASC': TokenType.ASC,
        'AS': TokenType.ALIAS,
        'ASOF': TokenType.ASOF,
        'AUTOINCREMENT': TokenType.AUTO_INCREMENT,
        'AUTO_INCREMENT': TokenType.AUTO_INCREMENT,
        'BEGIN': TokenType.BEGIN,
        'BETWEEN': TokenType.BETWEEN,
        'CACHE': TokenType.CACHE,
        'UNCACHE': TokenType.UNCACHE,
        'CASE': TokenType.CASE,
        'CHARACTER SET': TokenType.CHARACTER_SET,
        'CLUSTER BY': TokenType.CLUSTER_BY,
        'COLLATE': TokenType.COLLATE,
        'COLUMN': TokenType.COLUMN,
        'COMMIT': TokenType.COMMIT,
        'CONNECT BY': TokenType.CONNECT_BY,
        'CONSTRAINT': TokenType.CONSTRAINT,
        'COPY': TokenType.COPY,
        'CREATE': TokenType.CREATE,
        'CROSS': TokenType.CROSS,
        'CUBE': TokenType.CUBE,
        'CURRENT_DATE': TokenType.CURRENT_DATE,
        'CURRENT_SCHEMA': TokenType.CURRENT_SCHEMA,
        'CURRENT_TIME': TokenType.CURRENT_TIME,
        'CURRENT_TIMESTAMP': TokenType.CURRENT_TIMESTAMP,
        'CURRENT_USER': TokenType.CURRENT_USER,
        'CURRENT_CATALOG': TokenType.CURRENT_CATALOG,
        'DATABASE': TokenType.DATABASE,
        'DEFAULT': TokenType.DEFAULT,
        'DELETE': TokenType.DELETE,
        'DESC': TokenType.DESC,
        'DESCRIBE': TokenType.DESCRIBE,
        'DISTINCT': TokenType.DISTINCT,
        'DISTRIBUTE BY': TokenType.DISTRIBUTE_BY,
        'DIV': TokenType.DIV,
        'DROP': TokenType.DROP,
        'ELSE': TokenType.ELSE,
        'END': TokenType.END,
        'ENUM': TokenType.ENUM,
        'ESCAPE': TokenType.ESCAPE,
        'EXCEPT': TokenType.EXCEPT,
        'EXECUTE': TokenType.EXECUTE,
        'EXISTS': TokenType.EXISTS,
        'FALSE': TokenType.FALSE,
        'FETCH': TokenType.FETCH,
        'FILTER': TokenType.FILTER,
        'FILE': TokenType.FILE,
        'FIRST': TokenType.FIRST,
        'FULL': TokenType.FULL,
        'FUNCTION': TokenType.FUNCTION,
        'FOR': TokenType.FOR,
        'FOREIGN KEY': TokenType.FOREIGN_KEY,
        'FORMAT': TokenType.FORMAT,
        'FROM': TokenType.FROM,
        'GEOGRAPHY': TokenType.GEOGRAPHY,
        'GEOMETRY': TokenType.GEOMETRY,
        'GLOB': TokenType.GLOB,
        'GROUP BY': TokenType.GROUP_BY,
        'GROUPING SETS': TokenType.GROUPING_SETS,
        'HAVING': TokenType.HAVING,
        'ILIKE': TokenType.ILIKE,
        'IN': TokenType.IN,
        'INDEX': TokenType.INDEX,
        'INET': TokenType.INET,
        'INNER': TokenType.INNER,
        'INSERT': TokenType.INSERT,
        'INTERVAL': TokenType.INTERVAL,
        'INTERSECT': TokenType.INTERSECT,
        'INTO': TokenType.INTO,
        'IS': TokenType.IS,
        'ISNULL': TokenType.ISNULL,
        'JOIN': TokenType.JOIN,
        'KEEP': TokenType.KEEP,
        'KILL': TokenType.KILL,
        'LATERAL': TokenType.LATERAL,
        'LEFT': TokenType.LEFT,
        'LIKE': TokenType.LIKE,
        'LIMIT': TokenType.LIMIT,
        'LOAD': TokenType.LOAD,
        'LOCALTIME': TokenType.LOCALTIME,
        'LOCALTIMESTAMP': TokenType.LOCALTIMESTAMP,
        'LOCK': TokenType.LOCK,
        'MERGE': TokenType.MERGE,
        'NAMESPACE': TokenType.NAMESPACE,
        'NATURAL': TokenType.NATURAL,
        'NEXT': TokenType.NEXT,
        'NOT': TokenType.NOT,
        'NOTNULL': TokenType.NOTNULL,
        'NULL': TokenType.NULL,
        'OBJECT': TokenType.OBJECT,
        'OFFSET': TokenType.OFFSET,
        'ON': TokenType.ON,
        'OR': TokenType.OR,
        'XOR': TokenType.XOR,
        'ORDER BY': TokenType.ORDER_BY,
        'ORDINALITY': TokenType.ORDINALITY,
        'OUT': TokenType.OUT,
        'OUTER': TokenType.OUTER,
        'OVER': TokenType.OVER,
        'OVERLAPS': TokenType.OVERLAPS,
        'OVERWRITE': TokenType.OVERWRITE,
        'PARTITION': TokenType.PARTITION,
        'PARTITION BY': TokenType.PARTITION_BY,
        'PARTITIONED BY': TokenType.PARTITION_BY,
        'PARTITIONED_BY': TokenType.PARTITION_BY,
        'PERCENT': TokenType.PERCENT,
        'PIVOT': TokenType.PIVOT,
        'PRAGMA': TokenType.PRAGMA,
        'PRIMARY KEY': TokenType.PRIMARY_KEY,
        'PROCEDURE': TokenType.PROCEDURE,
        'OPERATOR': TokenType.OPERATOR,
        'QUALIFY': TokenType.QUALIFY,
        'RANGE': TokenType.RANGE,
        'RECURSIVE': TokenType.RECURSIVE,
        'REGEXP': TokenType.RLIKE,
        'RENAME': TokenType.RENAME,
        'REPLACE': TokenType.REPLACE,
        'RETURNING': TokenType.RETURNING,
        'REFERENCES': TokenType.REFERENCES,
        'RIGHT': TokenType.RIGHT,
        'RLIKE': TokenType.RLIKE,
        'ROLLBACK': TokenType.ROLLBACK,
        'ROLLUP': TokenType.ROLLUP,
        'ROW': TokenType.ROW,
        'ROWS': TokenType.ROWS,
        'SCHEMA': TokenType.SCHEMA,
        'SELECT': TokenType.SELECT,
        'SEMI': TokenType.SEMI,
        'SESSION': TokenType.SESSION,
        'SESSION_USER': TokenType.SESSION_USER,
        'SET': TokenType.SET,
        'SETTINGS': TokenType.SETTINGS,
        'SHOW': TokenType.SHOW,
        'SIMILAR TO': TokenType.SIMILAR_TO,
        'SOME': TokenType.SOME,
        'SORT BY': TokenType.SORT_BY,
        'START WITH': TokenType.START_WITH,
        'STRAIGHT_JOIN': TokenType.STRAIGHT_JOIN,
        'TABLE': TokenType.TABLE,
        'TABLESAMPLE': TokenType.TABLE_SAMPLE,
        'TEMP': TokenType.TEMPORARY,
        'TEMPORARY': TokenType.TEMPORARY,
        'THEN': TokenType.THEN,
        'TRUE': TokenType.TRUE,
        'TRUNCATE': TokenType.TRUNCATE,
        'UNION': TokenType.UNION,
        'UNKNOWN': TokenType.UNKNOWN,
        'UNNEST': TokenType.UNNEST,
        'UNPIVOT': TokenType.UNPIVOT,
        'UPDATE': TokenType.UPDATE,
        'USE': TokenType.USE,
        'USING': TokenType.USING,
        'UUID': TokenType.UUID,
        'VALUES': TokenType.VALUES,
        'VIEW': TokenType.VIEW,
        'VOLATILE': TokenType.VOLATILE,
        'WHEN': TokenType.WHEN,
        'WHERE': TokenType.WHERE,
        'WINDOW': TokenType.WINDOW,
        'WITH': TokenType.WITH,
        'APPLY': TokenType.APPLY,
        'ARRAY': TokenType.ARRAY,
        'BIT': TokenType.BIT,
        'BOOL': TokenType.BOOLEAN,
        'BOOLEAN': TokenType.BOOLEAN,
        'BYTE': TokenType.TINYINT,
        'MEDIUMINT': TokenType.MEDIUMINT,
        'INT1': TokenType.TINYINT,
        'TINYINT': TokenType.TINYINT,
        'INT16': TokenType.SMALLINT,
        'SHORT': TokenType.SMALLINT,
        'SMALLINT': TokenType.SMALLINT,
        'HUGEINT': TokenType.INT128,
        'UHUGEINT': TokenType.UINT128,
        'INT2': TokenType.SMALLINT,
        'INTEGER': TokenType.INT,
        'INT': TokenType.INT,
        'INT4': TokenType.INT,
        'INT32': TokenType.INT,
        'INT64': TokenType.BIGINT,
        'INT128': TokenType.INT128,
        'INT256': TokenType.INT256,
        'LONG': TokenType.BIGINT,
        'BIGINT': TokenType.BIGINT,
        'INT8': TokenType.TINYINT,
        'UINT': TokenType.UINT,
        'UINT128': TokenType.UINT128,
        'UINT256': TokenType.UINT256,
        'DEC': TokenType.DECIMAL,
        'DECIMAL': TokenType.DECIMAL,
        'DECIMAL32': TokenType.DECIMAL32,
        'DECIMAL64': TokenType.DECIMAL64,
        'DECIMAL128': TokenType.DECIMAL128,
        'DECIMAL256': TokenType.DECIMAL256,
        'DECFLOAT': TokenType.DECFLOAT,
        'BIGDECIMAL': TokenType.BIGDECIMAL,
        'BIGNUMERIC': TokenType.BIGDECIMAL,
        'BIGNUM': TokenType.BIGNUM,
        'LIST': TokenType.LIST,
        'MAP': TokenType.MAP,
        'NULLABLE': TokenType.NULLABLE,
        'NUMBER': TokenType.DECIMAL,
        'NUMERIC': TokenType.DECIMAL,
        'FIXED': TokenType.DECIMAL,
        'REAL': TokenType.FLOAT,
        'FLOAT': TokenType.FLOAT,
        'FLOAT4': TokenType.FLOAT,
        'FLOAT8': TokenType.DOUBLE,
        'DOUBLE': TokenType.DOUBLE,
        'DOUBLE PRECISION': TokenType.DOUBLE,
        'JSON': TokenType.JSON,
        'JSONB': TokenType.JSONB,
        'CHAR': TokenType.CHAR,
        'CHARACTER': TokenType.CHAR,
        'CHAR VARYING': TokenType.VARCHAR,
        'CHARACTER VARYING': TokenType.VARCHAR,
        'NCHAR': TokenType.NCHAR,
        'VARCHAR': TokenType.VARCHAR,
        'VARCHAR2': TokenType.VARCHAR,
        'NVARCHAR': TokenType.NVARCHAR,
        'NVARCHAR2': TokenType.NVARCHAR,
        'BPCHAR': TokenType.BPCHAR,
        'STR': TokenType.TEXT,
        'STRING': TokenType.TEXT,
        'TEXT': TokenType.TEXT,
        'LONGTEXT': TokenType.LONGTEXT,
        'MEDIUMTEXT': TokenType.MEDIUMTEXT,
        'TINYTEXT': TokenType.TINYTEXT,
        'CLOB': TokenType.TEXT,
        'LONGVARCHAR': TokenType.TEXT,
        'BINARY': TokenType.BINARY,
        'BLOB': TokenType.VARBINARY,
        'LONGBLOB': TokenType.LONGBLOB,
        'MEDIUMBLOB': TokenType.MEDIUMBLOB,
        'TINYBLOB': TokenType.TINYBLOB,
        'BYTEA': TokenType.VARBINARY,
        'VARBINARY': TokenType.VARBINARY,
        'TIME': TokenType.TIME,
        'TIMETZ': TokenType.TIMETZ,
        'TIME_NS': TokenType.TIME_NS,
        'TIMESTAMP': TokenType.TIMESTAMP,
        'TIMESTAMPTZ': TokenType.TIMESTAMPTZ,
        'TIMESTAMPLTZ': TokenType.TIMESTAMPLTZ,
        'TIMESTAMP_LTZ': TokenType.TIMESTAMPLTZ,
        'TIMESTAMPNTZ': TokenType.TIMESTAMPNTZ,
        'TIMESTAMP_NTZ': TokenType.TIMESTAMPNTZ,
        'DATE': TokenType.DATE,
        'DATETIME': TokenType.DATETIME,
        'INT4RANGE': TokenType.INT4RANGE,
        'INT4MULTIRANGE': TokenType.INT4MULTIRANGE,
        'INT8RANGE': TokenType.INT8RANGE,
        'INT8MULTIRANGE': TokenType.INT8MULTIRANGE,
        'NUMRANGE': TokenType.NUMRANGE,
        'NUMMULTIRANGE': TokenType.NUMMULTIRANGE,
        'TSRANGE': TokenType.TSRANGE,
        'TSMULTIRANGE': TokenType.TSMULTIRANGE,
        'TSTZRANGE': TokenType.TSTZRANGE,
        'TSTZMULTIRANGE': TokenType.TSTZMULTIRANGE,
        'DATERANGE': TokenType.DATERANGE,
        'DATEMULTIRANGE': TokenType.DATEMULTIRANGE,
        'UNIQUE': TokenType.UNIQUE,
        'VECTOR': TokenType.VECTOR,
        'STRUCT': TokenType.STRUCT,
        'SEQUENCE': TokenType.SEQUENCE,
        'VARIANT': TokenType.VARIANT,
        'ALTER': TokenType.ALTER,
        'ANALYZE': TokenType.ANALYZE,
        'CALL': TokenType.COMMAND,
        'COMMENT': TokenType.COMMENT,
        'EXPLAIN': TokenType.COMMAND,
        'GRANT': TokenType.GRANT,
        'REVOKE': TokenType.REVOKE,
        'OPTIMIZE': TokenType.COMMAND,
        'PREPARE': TokenType.COMMAND,
        'VACUUM': TokenType.COMMAND,
        'USER-DEFINED': TokenType.USERDEFINED,
        'FOR VERSION': TokenType.VERSION_SNAPSHOT,
        'FOR TIMESTAMP': TokenType.TIMESTAMP_SNAPSHOT,
      };
      this.keywordsCache.set(this, cached);
    }
    return cached;
  }

  /**
   * Map of whitespace characters to their token types.
   *
   */
  static WHITE_SPACE: Record<string, TokenType> = {
    ' ': TokenType.SPACE,
    '\t': TokenType.SPACE,
    '\n': TokenType.BREAK,
    '\r': TokenType.BREAK,
  };

  /**
   * Set of command token types.
   *
   */
  static COMMANDS = new Set([
    TokenType.COMMAND,
    TokenType.EXECUTE,
    TokenType.FETCH,
    TokenType.SHOW,
    TokenType.RENAME,
  ]);

  /**
   * Set of token types that can precede a command.
   *
   */
  static COMMAND_PREFIX_TOKENS = new Set([TokenType.SEMICOLON, TokenType.BEGIN]);

  /**
   * Handle numeric literals like in hive (3L = BIGINT).
   *
   */
  static NUMERIC_LITERALS: Record<string, string> = {};

  /**
   * Array of comment delimiters.
   */
  static COMMENTS: Array<string | [string, string]> = ['--', ['/*', '*/']];

  /** The SQL string being tokenized. */
  sql = '';
  /** The length of the SQL string. */
  size = 0;
  /** Array of tokens produced by tokenization. */
  tokens: Token[] = [];
  dialect: Dialect;
  /** Starting position of the current token. */
  private _start = 0;
  /** Current position in the SQL string. */
  private _current = 0;
  /** Current line number (1-indexed). */
  private _line = 1;
  /** Current column number. */
  private _col = 0;
  /** Accumulated comments for the next token. */
  private _comments: string[] = [];
  /** Current character being processed. */
  private _char = '';
  /** Whether we've reached the end of the SQL string. */
  private _end = false;
  /** The next character to be processed. */
  private _peek = '';
  /** Line number of the previously added token. */
  private _prev_token_line = -1;

  constructor (dialect?: DialectType) {
    this.dialect = Dialect.getOrRaise(dialect);
    this.reset();
  }

  /**
   * Reset the tokenizer state.
   */
  reset (): void {
    this.sql = '';
    this.size = 0;
    this.tokens = [];
    this._start = 0;
    this._current = 0;
    this._line = 1;
    this._col = 0;
    this._comments = [];
    this._char = '';
    this._end = false;
    this._peek = '';
    this._prev_token_line = -1;
  }

  /**
   * Returns a list of tokens corresponding to the SQL string.
   *
   * @param sql - The SQL string to tokenize
   * @returns Array of tokens
   * @throws {Error} If tokenization fails, with context around the error position
   */
  tokenize (sql: string): Token[] {
    this.reset();
    this.sql = sql;
    this.size = sql.length;

    try {
      this._scan();
    } catch {
      const start = Math.max(this._current - 50, 0);
      const end = Math.min(this._current + 50, this.size - 1);
      const context = this.sql.slice(start, end);
      throw new TokenError(`Error tokenizing '${context}'`);
    }

    return this.tokens;
  }

  /**
   * Main scanning loop that processes the SQL string character by character.
   *
   * @param until - Optional callback to stop scanning early
   */
  private _scan (until?: () => boolean): void {
    while (this.size && !this._end) {
      let current = this._current;

      // Skip spaces here rather than iteratively calling advance() for performance reasons
      while (current < this.size) {
        const char = this.sql[current];

        if (this._isWhitespace(char) && (char === ' ' || char === '\t')) {
          current++;
        } else {
          break;
        }
      }

      const offset = current > this._current ? current - this._current : 1;

      this._start = current;
      this._advance({ i: offset });

      if (!this._isWhitespace(this._char)) {
        if (this._isDigit(this._char)) {
          this._scanNumber();
        } else if (this._char in (this._constructor)._IDENTIFIERS()) {
          this._scanIdentifier((this._constructor)._IDENTIFIERS()[this._char]);
        } else {
          this._scanKeywords();
        }
      }

      if (until?.()) {
        break;
      }
    }

    if (this.tokens.length && this._comments.length) {
      this.tokens[this.tokens.length - 1].comments.push(...this._comments);
    }
  }

  private _chars (size: number): string {
    if (size === 1) {
      return this._char;
    }

    const start = this._current - 1;
    const end = start + size;

    return end <= this.size ? this.sql.slice(start, end) : '';
  }

  /**
   * Advances the current position in the SQL string.
   *
   * @param opts.i - Number of characters to advance
   * @param opts.alnum - If true, fast-forward through alphanumeric characters
   */
  private _advance (opts: { i?: number;
    alnum?: boolean; } = {}): void {
    const {
      i = 1, alnum = false,
    } = opts;
    const constructor = this._constructor;
    if (constructor.WHITE_SPACE[this._char] === TokenType.BREAK) {
      // Ensures we don't count an extra line if we get a \r\n line break sequence
      if (!(this._char === '\r' && this._peek === '\n')) {
        this._col = i;
        this._line += 1;
      }
    } else {
      this._col += i;
    }

    this._current += i;
    this._end = this._current >= this.size;
    this._char = this.sql[this._current - 1];
    this._peek = this._end ? '' : this.sql[this._current];

    if (alnum && this._isAlnum(this._char)) {
      // Here we use local variables instead of attributes for better performance
      let {
        _col, _current, _end, _peek,
      } = this;

      while (this._isAlnum(_peek)) {
        _col += 1;
        _current += 1;
        _end = _current >= this.size;
        _peek = _end ? '' : this.sql[_current];
      }

      this._col = _col;
      this._current = _current;
      this._end = _end;
      this._peek = _peek;
      this._char = this.sql[_current - 1];
    }
  }

  private get _text (): string {
    return this.sql.slice(this._start, this._current);
  }

  /**
   * Adds a token to the tokens array.
   *
   * Handles special cases like attaching comments and parsing command strings.
   *
   * @param tokenType - The type of token to add
   * @param text - Optional text override (defaults to current token text)
   */
  private _add (tokenType: TokenType, text?: string): void {
    this._prev_token_line = this._line;

    if (this._comments.length && tokenType === TokenType.SEMICOLON && this.tokens.length) {
      this.tokens[this.tokens.length - 1].comments.push(...this._comments);
      this._comments = [];
    }

    this.tokens.push(
      new Token(
        tokenType,
        text ?? this._text,
        this._line,
        this._col,
        this._start,
        this._current - 1,
        this._comments,
      ),
    );
    this._comments = [];

    const constructor = this._constructor;
    // If we have either a semicolon or a begin token before the command's token, we'll parse
    // whatever follows the command's token as a string
    if (
      constructor.COMMANDS.has(tokenType)
      && this._peek !== ';'
      && (this.tokens.length === 1 || constructor.COMMAND_PREFIX_TOKENS.has(this.tokens[this.tokens.length - 2].tokenType))
    ) {
      const start = this._current;
      const tokensLength = this.tokens.length;
      this._scan(() => this._peek === ';');
      this.tokens = this.tokens.slice(0, tokensLength);
      const commandText = this.sql.slice(start, this._current).trim();
      if (commandText) {
        this._add(TokenType.STRING, commandText);
      }
    }
  }

  private _scanKeywords (): void {
    let size = 0;
    let word: string | undefined = undefined;
    let chars = this._text;
    let char = chars;
    let prevSpace = false;
    let skip = false;
    const constructor = this._constructor;
    let trie = constructor._KEYWORD_TRIE();
    let singleToken = char in this._constructor.SINGLE_TOKENS;

    while (chars) {
      let result: TrieResult;
      if (skip) {
        result = TrieResult.PREFIX;
      } else {
        [result, trie] = inTrie(trie, Array.from(char.toUpperCase()));
      }

      if (result === TrieResult.FAILED) {
        break;
      }
      if (result === TrieResult.EXISTS) {
        word = chars;
      }

      const end = this._current + size;
      size += 1;

      if (end < this.size) {
        char = this.sql[end];
        singleToken = singleToken || char in this._constructor.SINGLE_TOKENS;
        const isSpace = this._isWhitespace(char);

        if (!isSpace || !prevSpace) {
          if (isSpace) {
            char = ' ';
          }
          chars += char;
          prevSpace = isSpace;
          skip = false;
        } else {
          skip = true;
        }
      } else {
        char = '';
        break;
      }
    }

    if (word) {
      if (this._scanString(word)) {
        return;
      }
      if (this._scanComment(word)) {
        return;
      }
      if (prevSpace || singleToken || !char) {
        this._advance({ i: size - 1 });
        const upper = word.toUpperCase();
        this._add(constructor.KEYWORDS()[upper], upper);
        return;
      }
    }

    const type = this._constructor.SINGLE_TOKENS[this._char];
    if (type !== undefined) {
      this._add(type, this._char);
      return;
    }

    this._scanVar();
  }

  /**
   * Scans a comment (single-line or multi-line with nesting support).
   *
   * @param commentStart - The comment start delimiter
   * @returns True if a comment was scanned
   */
  private _scanComment (commentStart: string): boolean {
    const constructor = this._constructor;
    if (!(commentStart in constructor._COMMENTS())) {
      return false;
    }

    const commentStartLine = this._line;
    const commentStartSize = commentStart.length;
    const commentEnd = constructor._COMMENTS()[commentStart];

    if (commentEnd) {
      // Skip the comment's start delimiter
      this._advance({ i: commentStartSize });

      let commentCount = 1;
      const commentEndSize = commentEnd.length;

      while (!this._end) {
        if (this._chars(commentEndSize) === commentEnd) {
          commentCount -= 1;
          if (!commentCount) {
            break;
          }
        }

        this._advance({ alnum: true });

        // Nested comments are allowed by some dialects
        if (
          constructor.NESTED_COMMENTS
          && !this._end
          && this._chars(commentStartSize) === commentStart
        ) {
          this._advance({ i: commentStartSize });
          commentCount += 1;
        }
      }

      this._comments.push(this._text.slice(commentStartSize, -commentEndSize + 1));
      this._advance({ i: commentEndSize - 1 });
    } else {
      while (!this._end && constructor.WHITE_SPACE[this._peek] !== TokenType.BREAK) {
        this._advance({
          i: 1,
          alnum: true,
        });
      }
      this._comments.push(this._text.slice(commentStartSize));
    }

    if (
      commentStart === constructor.HINT_START
      && this.tokens.length
      && constructor.TOKENS_PRECEDING_HINT.has(this.tokens[this.tokens.length - 1].tokenType)
    ) {
      this._add(TokenType.HINT);
    }

    // Leading comment is attached to the succeeding token, whilst trailing comment to the preceding.
    if (commentStartLine === this._prev_token_line) {
      this.tokens[this.tokens.length - 1].comments.push(...this._comments);
      this._comments = [];
      this._prev_token_line = this._line;
    }

    return true;
  }

  /**
   * Scans a numeric literal, including decimals, scientific notation, and type suffixes.
   */
  private _scanNumber (): void {
    const constructor = this._constructor;
    if (this._char === '0') {
      const peek = this._peek.toUpperCase();
      if (peek === 'B') {
        return constructor.BIT_STRINGS.length ? this._scanBits() : this._add(TokenType.NUMBER);
      } else if (peek === 'X') {
        return constructor.HEX_STRINGS.length ? this._scanHex() : this._add(TokenType.NUMBER);
      }
    }

    let decimal = false;
    let scientific = 0;

    while (true) {
      if (this._isDigit(this._peek)) {
        this._advance();
      } else if (this._peek === '.' && !decimal) {
        if (this.tokens.length && this.tokens[this.tokens.length - 1].tokenType === TokenType.PARAMETER) {
          return this._add(TokenType.NUMBER);
        }
        decimal = true;
        this._advance();
      } else if (['-', '+'].includes(this._peek) && scientific === 1) {
        // Only consume +/- if followed by a digit
        if (this._current + 1 < this.size && this._isDigit(this.sql[this._current + 1])) {
          scientific += 1;
          this._advance();
        } else {
          return this._add(TokenType.NUMBER);
        }
      } else if (this._peek.toUpperCase() === 'E' && !scientific) {
        scientific += 1;
        this._advance();
      } else if (this._peek === '_' && this.dialect._constructor.NUMBERS_CAN_BE_UNDERSCORE_SEPARATED) {
        this._advance();
      } else if (this._isIdentifierChar(this._peek)) {
        const numberText = this._text;
        let literal = '';

        while (this._peek.trim() && !this._constructor.SINGLE_TOKENS[this._peek]) {
          literal += this._peek;
          this._advance();
        }

        const tokenType = constructor.KEYWORDS()[constructor.NUMERIC_LITERALS[literal.toUpperCase()] || ''];

        if (tokenType) {
          this._add(TokenType.NUMBER, numberText);
          this._add(TokenType.DCOLON, '::');
          return this._add(tokenType, literal);
        }

        this._advance({ i: -literal.length });
        return this._add(TokenType.NUMBER, numberText);
      } else {
        return this._add(TokenType.NUMBER);
      }
    }
  }

  private _scanBits (): void {
    this._advance();
    const value = this._extractValue();
    // If `value` can't be converted to a binary, fallback to tokenizing it as an identifier
    if (!Number.isNaN(parseInt(value, 2))) {
      this._add(TokenType.BIT_STRING, value.slice(2)); // Drop the 0b
    } else {
      this._add(TokenType.IDENTIFIER);
    }
  }

  private _scanHex (): void {
    this._advance();
    const value = this._extractValue();
    // If `value` can't be converted to a hex, fallback to tokenizing it as an identifier
    if (!Number.isNaN(parseInt(value, 16))) {
      this._add(TokenType.HEX_STRING, value.slice(2)); // Drop the 0x
    } else {
      this._add(TokenType.IDENTIFIER);
    }
  }

  private _extractValue (): string {
    while (true) {
      const char = this._peek.trim();
      if (char && !this._constructor.SINGLE_TOKENS[char]) {
        this._advance({
          i: 1,
          alnum: true,
        });
      } else {
        break;
      }
    }

    return this._text;
  }

  /**
   * Scans various string types including quoted strings, format strings, and heredocs.
   *
   * @param start - The string start delimiter
   * @returns True if a string was scanned
   */
  private _scanString (start: string): boolean {
    const constructor = this._constructor;
    let base: number | undefined = undefined;
    let tokenType = TokenType.STRING;

    let end: string;
    if (start in constructor._QUOTES()) {
      end = constructor._QUOTES()[start];
    } else if (start in constructor._FORMAT_STRINGS()) {
      [end, tokenType] = constructor._FORMAT_STRINGS()[start];

      if (tokenType === TokenType.HEX_STRING) {
        base = 16;
      } else if (tokenType === TokenType.BIT_STRING) {
        base = 2;
      } else if (tokenType === TokenType.HEREDOC_STRING) {
        this._advance();

        let tag: string;
        if (this._char === end) {
          tag = '';
        } else {
          tag = this._extractString(end, undefined, true, !constructor.HEREDOC_TAG_IS_IDENTIFIER);
        }

        if (
          tag
          && constructor.HEREDOC_TAG_IS_IDENTIFIER
          && (this._end || this._isDigit(tag) || this._isWhitespace(tag))
        ) {
          if (!this._end) {
            this._advance({ i: -1 });
          }

          this._advance({ i: -tag.length });
          this._add(constructor.HEREDOC_STRING_ALTERNATIVE);
          return true;
        }

        end = `${start}${tag}${end}`;
      }
    } else {
      return false;
    }

    this._advance({ i: start.length });
    const text = this._extractString(
      end,
      tokenType === TokenType.BYTE_STRING ? constructor._BYTE_STRING_ESCAPES() : constructor._STRING_ESCAPES(),
      tokenType === TokenType.RAW_STRING,
    );

    if (base && text && Number.isNaN(parseInt(text, base))) {
      throw new Error(`Numeric string contains invalid characters from ${this._line}:${this._start}`);
    }

    this._add(tokenType, text);
    return true;
  }

  private _scanIdentifier (identifierEnd: string): void {
    this._advance();
    const constructor = this._constructor;
    const escapes = new Set([...Array.from(constructor._IDENTIFIER_ESCAPES()), identifierEnd]);
    const text = this._extractString(identifierEnd, escapes);
    this._add(TokenType.IDENTIFIER, text);
  }

  private _scanVar (): void {
    const constructor = this._constructor;
    while (true) {
      const char = this._peek.trim();
      if (char && (constructor.VAR_SINGLE_TOKENS.has(char) || !this._constructor.SINGLE_TOKENS[char])) {
        this._advance({
          i: 1,
          alnum: true,
        });
      } else {
        break;
      }
    }

    this._add(
      this.tokens.length && this.tokens[this.tokens.length - 1].tokenType === TokenType.PARAMETER
        ? TokenType.VAR
        : constructor.KEYWORDS()[this._text.toUpperCase()] || TokenType.VAR,
    );
  }

  /**
   * Extracts string content between delimiters, handling escape sequences.
   *
   * @param delimiter - The closing delimiter to look for
   * @param escapes - Set of escapable characters (null uses default string escapes)
   * @param rawString - If true, treat as raw string with minimal escape processing
   * @param raiseUnmatched - If true, throw error on unmatched delimiter
   * @returns The extracted string content
   * @throws {Error} If delimiter is unmatched and raiseUnmatched is true
   */
  private _extractString (
    delimiter: string,
    escapes: Set<string> | undefined = undefined,
    rawString: boolean = false,
    raiseUnmatched: boolean = true,
  ): string {
    const constructor = this._constructor;
    let text = '';
    const delimSize = delimiter.length;
    escapes = escapes === undefined ? constructor._STRING_ESCAPES() : escapes;

    while (true) {
      if (
        !rawString
        && this.dialect._constructor.UNESCAPED_SEQUENCES
        && this._peek
        && escapes.has(this._char)
      ) {
        const unescaped_sequence = this.dialect._constructor.UNESCAPED_SEQUENCES[this._char + this._peek];
        if (unescaped_sequence) {
          this._advance({ i: 2 });
          text += unescaped_sequence;
          continue;
        }
      }

      const isValidCustomEscape
        = constructor.ESCAPE_FOLLOW_CHARS.length
          && this._char === '\\'
          && !constructor.ESCAPE_FOLLOW_CHARS.includes(this._peek);

      if (
        (constructor.STRING_ESCAPES_ALLOWED_IN_RAW_STRINGS || !rawString)
        && escapes.has(this._char)
        && (this._peek === delimiter || escapes.has(this._peek) || isValidCustomEscape)
        && (!(this._char in constructor._QUOTES()) || this._char === this._peek)
      ) {
        if (this._peek === delimiter) {
          text += this._peek;
        } else if (isValidCustomEscape && this._char !== this._peek) {
          text += this._peek;
        } else {
          text += this._char + this._peek;
        }

        if (this._current + 1 < this.size) {
          this._advance({ i: 2 });
        } else {
          throw new TokenError(`Missing ${delimiter} from ${this._line}:${this._current}`);
        }
      } else {
        if (this._chars(delimSize) === delimiter) {
          if (delimSize > 1) {
            this._advance({ i: delimSize - 1 });
          }
          break;
        }

        if (this._end) {
          if (!raiseUnmatched) {
            return text + this._char;
          }

          throw new TokenError(`Missing ${delimiter} from ${this._line}:${this._start}`);
        }

        const current = this._current - 1;
        this._advance({
          i: 1,
          alnum: true,
        });
        text += this.sql.slice(current, this._current - 1);
      }
    }

    return text;
  }

  private static _convertQuotes (arr: TokenPair[]): Record<string, string> {
    const res: Record<string, string> = {};
    for (const item of arr) {
      const key = typeof item === 'string' ? item : item[0];
      const value = typeof item === 'string' ? item : item[1];
      res[key] = value;
    }
    return res;
  }

  private static _quotesToFormat (
    tokenType: TokenType,
    arr: TokenPair[],
  ): Record<string, [string, TokenType]> {
    const quotes = this._convertQuotes(arr);
    const result: Record<string, [string, TokenType]> = {};
    for (const [k, v] of Object.entries(quotes)) {
      result[k] = [v, tokenType];
    }
    return result;
  }

  // Helper methods

  private _isWhitespace (char: string): boolean {
    return /\s/.test(char);
  }

  private _isDigit (char: string): boolean {
    return /\d/.test(char);
  }

  private _isAlnum (char: string): boolean {
    return /[a-zA-Z0-9]/.test(char);
  }

  private _isIdentifierChar (char: string): boolean {
    return /[a-zA-Z_]/.test(char);
  }

  private get _constructor (): typeof Tokenizer {
    return this.constructor as typeof Tokenizer;
  }
}
