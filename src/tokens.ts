// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/tokens.py

import { Dialect } from './dialects/dialect';
import { TokenError } from './errors';
import {
  inTrie, newTrie, TrieResult, type TrieNode,
} from './trie';

/**
 * Represents a syntax token's tag.
 */
export enum TokenType {
  L_PAREN = 'lParen',
  R_PAREN = 'rParen',
  L_BRACKET = 'lBracket',
  R_BRACKET = 'rBracket',
  L_BRACE = 'lBrace',
  R_BRACE = 'rBrace',
  COMMA = 'comma',
  DOT = 'dot',
  DASH = 'dash',
  PLUS = 'plus',
  COLON = 'colon',
  DOTCOLON = 'dotcolon',
  DCOLON = 'dcolon',
  DCOLONDOLLAR = 'dcolondollar',
  DCOLONPERCENT = 'dcolonpercent',
  DCOLONQMARK = 'dcolonqmark',
  DQMARK = 'dqmark',
  SEMICOLON = 'semicolon',
  STAR = 'star',
  BACKSLASH = 'backslash',
  SLASH = 'slash',
  LT = 'lt',
  LTE = 'lte',
  GT = 'gt',
  GTE = 'gte',
  NOT = 'not',
  EQ = 'eq',
  NEQ = 'neq',
  NULLSAFE_EQ = 'nullsafeEq',
  COLON_EQ = 'colonEq',
  COLON_GT = 'colonGt',
  NCOLON_GT = 'ncolonGt',
  AND = 'and',
  OR = 'or',
  AMP = 'amp',
  DPIPE = 'dpipe',
  PIPE_GT = 'pipeGt',
  PIPE = 'pipe',
  PIPE_SLASH = 'pipeSlash',
  DPIPE_SLASH = 'dpipeSlash',
  CARET = 'caret',
  CARET_AT = 'caretAt',
  TILDE = 'tilde',
  ARROW = 'arrow',
  DARROW = 'darrow',
  FARROW = 'farrow',
  HASH = 'hash',
  HASH_ARROW = 'hashArrow',
  DHASH_ARROW = 'dhashArrow',
  LR_ARROW = 'lrArrow',
  DAT = 'dat',
  LT_AT = 'ltAt',
  AT_GT = 'atGt',
  DOLLAR = 'dollar',
  PARAMETER = 'parameter',
  SESSION = 'session',
  SESSION_PARAMETER = 'sessionParameter',
  SESSION_USER = 'sessionUser',
  DAMP = 'damp',
  AMP_LT = 'ampLt',
  AMP_GT = 'ampGt',
  ADJACENT = 'adjacent',
  XOR = 'xor',
  DSTAR = 'dstar',
  QMARK_AMP = 'qmarkAmp',
  QMARK_PIPE = 'qmarkPipe',
  HASH_DASH = 'hashDash',
  EXCLAMATION = 'exclamation',

  URI_START = 'uriStart',

  BLOCK_START = 'blockStart',
  BLOCK_END = 'blockEnd',

  SPACE = 'space',
  BREAK = 'break',

  STRING = 'string',
  NUMBER = 'number',
  IDENTIFIER = 'identifier',
  DATABASE = 'database',
  COLUMN = 'column',
  COLUMN_DEF = 'columnDef',
  SCHEMA = 'schema',
  TABLE = 'table',
  WAREHOUSE = 'warehouse',
  STAGE = 'stage',
  STREAMLIT = 'streamlit',
  VAR = 'var',
  BIT_STRING = 'bitString',
  HEX_STRING = 'hexString',
  BYTE_STRING = 'byteString',
  NATIONAL_STRING = 'nationalString',
  RAW_STRING = 'rawString',
  HEREDOC_STRING = 'heredocString',
  UNICODE_STRING = 'unicodeString',

  // types
  BIT = 'bit',
  BOOLEAN = 'boolean',
  TINYINT = 'tinyint',
  UTINYINT = 'utinyint',
  SMALLINT = 'smallint',
  USMALLINT = 'usmallint',
  MEDIUMINT = 'mediumint',
  UMEDIUMINT = 'umediumint',
  INT = 'int',
  UINT = 'uint',
  BIGINT = 'bigint',
  UBIGINT = 'ubigint',
  BIGNUM = 'bignum',
  INT128 = 'int128',
  UINT128 = 'uint128',
  INT256 = 'int256',
  UINT256 = 'uint256',
  FLOAT = 'float',
  DOUBLE = 'double',
  UDOUBLE = 'udouble',
  DECIMAL = 'decimal',
  DECIMAL32 = 'decimal32',
  DECIMAL64 = 'decimal64',
  DECIMAL128 = 'decimal128',
  DECIMAL256 = 'decimal256',
  DECFLOAT = 'decfloat',
  UDECIMAL = 'udecimal',
  BIGDECIMAL = 'bigdecimal',
  CHAR = 'char',
  NCHAR = 'nchar',
  VARCHAR = 'varchar',
  NVARCHAR = 'nvarchar',
  BPCHAR = 'bpchar',
  TEXT = 'text',
  MEDIUMTEXT = 'mediumtext',
  LONGTEXT = 'longtext',
  BLOB = 'blob',
  MEDIUMBLOB = 'mediumblob',
  LONGBLOB = 'longblob',
  TINYBLOB = 'tinyblob',
  TINYTEXT = 'tinytext',
  NAME = 'name',
  BINARY = 'binary',
  VARBINARY = 'varbinary',
  JSON = 'json',
  JSONB = 'jsonb',
  TIME = 'time',
  TIMETZ = 'timetz',
  TIME_NS = 'timeNs',
  TIMESTAMP = 'timestamp',
  TIMESTAMPTZ = 'timestamptz',
  TIMESTAMPLTZ = 'timestampltz',
  TIMESTAMPNTZ = 'timestampntz',
  TIMESTAMP_S = 'timestampS',
  TIMESTAMP_MS = 'timestampMs',
  TIMESTAMP_NS = 'timestampNs',
  DATETIME = 'datetime',
  DATETIME2 = 'datetime2',
  DATETIME64 = 'datetime64',
  SMALLDATETIME = 'smalldatetime',
  DATE = 'date',
  DATE32 = 'date32',
  INT4RANGE = 'int4range',
  INT4MULTIRANGE = 'int4multirange',
  INT8RANGE = 'int8range',
  INT8MULTIRANGE = 'int8multirange',
  NUMRANGE = 'numrange',
  NUMMULTIRANGE = 'nummultirange',
  TSRANGE = 'tsrange',
  TSMULTIRANGE = 'tsmultirange',
  TSTZRANGE = 'tstzrange',
  TSTZMULTIRANGE = 'tstzmultirange',
  DATERANGE = 'daterange',
  DATEMULTIRANGE = 'datemultirange',
  UUID = 'uuid',
  GEOGRAPHY = 'geography',
  GEOGRAPHYPOINT = 'geographypoint',
  NULLABLE = 'nullable',
  GEOMETRY = 'geometry',
  POINT = 'point',
  RING = 'ring',
  LINESTRING = 'linestring',
  LOCALTIME = 'localtime',
  LOCALTIMESTAMP = 'localtimestamp',
  SYSTIMESTAMP = 'systimestamp',
  MULTILINESTRING = 'multilinestring',
  POLYGON = 'polygon',
  MULTIPOLYGON = 'multipolygon',
  HLLSKETCH = 'hllsketch',
  HSTORE = 'hstore',
  SUPER = 'super',
  SERIAL = 'serial',
  SMALLSERIAL = 'smallserial',
  BIGSERIAL = 'bigserial',
  XML = 'xml',
  YEAR = 'year',
  USERDEFINED = 'userdefined',
  MONEY = 'money',
  SMALLMONEY = 'smallmoney',
  ROWVERSION = 'rowversion',
  IMAGE = 'image',
  VARIANT = 'variant',
  OBJECT = 'object',
  INET = 'inet',
  IPADDRESS = 'ipaddress',
  IPPREFIX = 'ipprefix',
  IPV4 = 'ipv4',
  IPV6 = 'ipv6',
  ENUM = 'enum',
  ENUM8 = 'enum8',
  ENUM16 = 'enum16',
  FIXEDSTRING = 'fixedstring',
  LOWCARDINALITY = 'lowcardinality',
  NESTED = 'nested',
  AGGREGATEFUNCTION = 'aggregatefunction',
  SIMPLEAGGREGATEFUNCTION = 'simpleaggregatefunction',
  TDIGEST = 'tdigest',
  UNKNOWN = 'unknown',
  VECTOR = 'vector',
  DYNAMIC = 'dynamic',
  VOID = 'void',

  // keywords
  ALIAS = 'alias',
  ALTER = 'alter',
  ALL = 'all',
  ANTI = 'anti',
  ANY = 'any',
  APPLY = 'apply',
  ARRAY = 'array',
  ASC = 'asc',
  ASOF = 'asof',
  ATTACH = 'attach',
  AUTO_INCREMENT = 'autoIncrement',
  BEGIN = 'begin',
  BETWEEN = 'between',
  BULK_COLLECT_INTO = 'bulkCollectInto',
  CACHE = 'cache',
  CASE = 'case',
  CHARACTER_SET = 'characterSet',
  CLUSTER_BY = 'clusterBy',
  COLLATE = 'collate',
  COMMAND = 'command',
  COMMENT = 'comment',
  COMMIT = 'commit',
  CONNECT_BY = 'connectBy',
  CONSTRAINT = 'constraint',
  COPY = 'copy',
  CREATE = 'create',
  CROSS = 'cross',
  CUBE = 'cube',
  CURRENT_DATE = 'currentDate',
  CURRENT_DATETIME = 'currentDatetime',
  CURRENT_SCHEMA = 'currentSchema',
  CURRENT_TIME = 'currentTime',
  CURRENT_TIMESTAMP = 'currentTimestamp',
  CURRENT_USER = 'currentUser',
  CURRENT_ROLE = 'currentRole',
  CURRENT_CATALOG = 'currentCatalog',
  DECLARE = 'declare',
  DEFAULT = 'default',
  DELETE = 'delete',
  DESC = 'desc',
  DESCRIBE = 'describe',
  DETACH = 'detach',
  DICTIONARY = 'dictionary',
  DISTINCT = 'distinct',
  DISTRIBUTE_BY = 'distributeBy',
  DIV = 'div',
  DROP = 'drop',
  ELSE = 'else',
  END = 'end',
  ESCAPE = 'escape',
  EXCEPT = 'except',
  EXECUTE = 'execute',
  EXISTS = 'exists',
  FALSE = 'false',
  FETCH = 'fetch',
  FILE = 'file',
  FILE_FORMAT = 'fileFormat',
  FILTER = 'filter',
  FINAL = 'final',
  FIRST = 'first',
  FOR = 'for',
  FORCE = 'force',
  FOREIGN_KEY = 'foreignKey',
  FORMAT = 'format',
  FROM = 'from',
  FULL = 'full',
  FUNCTION = 'function',
  GET = 'get',
  GLOB = 'glob',
  GLOBAL = 'global',
  GRANT = 'grant',
  GROUP_BY = 'groupBy',
  GROUPING_SETS = 'groupingSets',
  HAVING = 'having',
  HINT = 'hint',
  IGNORE = 'ignore',
  ILIKE = 'ilike',
  IN = 'in',
  INDEX = 'index',
  INDEXED_BY = 'indexedBy',
  INNER = 'inner',
  INSERT = 'insert',
  INSTALL = 'install',
  INTERSECT = 'intersect',
  INTERVAL = 'interval',
  INTO = 'into',
  INTRODUCER = 'introducer',
  IRLIKE = 'irlike',
  IS = 'is',
  ISNULL = 'isnull',
  JOIN = 'join',
  JOIN_MARKER = 'joinMarker',
  KEEP = 'keep',
  KEY = 'key',
  KILL = 'kill',
  LANGUAGE = 'language',
  LATERAL = 'lateral',
  LEFT = 'left',
  LIKE = 'like',
  LIMIT = 'limit',
  LIST = 'list',
  LOAD = 'load',
  LOCK = 'lock',
  MAP = 'map',
  MATCH = 'match',
  MATCH_CONDITION = 'matchCondition',
  MATCH_RECOGNIZE = 'matchRecognize',
  MEMBER_OF = 'memberOf',
  MERGE = 'merge',
  MOD = 'mod',
  MODEL = 'model',
  NATURAL = 'natural',
  NEXT = 'next',
  NOTHING = 'nothing',
  NOTNULL = 'notnull',
  NULL = 'null',
  OBJECT_IDENTIFIER = 'objectIdentifier',
  OFFSET = 'offset',
  ON = 'on',
  ONLY = 'only',
  OPERATOR = 'operator',
  ORDER_BY = 'orderBy',
  ORDER_SIBLINGS_BY = 'orderSiblingsBy',
  ORDERED = 'ordered',
  ORDINALITY = 'ordinality',
  OUT = 'out',
  INOUT = 'inout',
  OUTER = 'outer',
  OVER = 'over',
  OVERLAPS = 'overlaps',
  OVERWRITE = 'overwrite',
  PARTITION = 'partition',
  PARTITION_BY = 'partitionBy',
  PERCENT = 'percent',
  PIVOT = 'pivot',
  PLACEHOLDER = 'placeholder',
  POSITIONAL = 'positional',
  PRAGMA = 'pragma',
  PREWHERE = 'prewhere',
  PRIMARY_KEY = 'primaryKey',
  PROCEDURE = 'procedure',
  PROPERTIES = 'properties',
  PSEUDO_TYPE = 'pseudoType',
  PUT = 'put',
  QUALIFY = 'qualify',
  QUOTE = 'quote',
  QDCOLON = 'qdcolon',
  RANGE = 'range',
  RECURSIVE = 'recursive',
  REFRESH = 'refresh',
  RENAME = 'rename',
  REPLACE = 'replace',
  RETURNING = 'returning',
  REVOKE = 'revoke',
  REFERENCES = 'references',
  RIGHT = 'right',
  RLIKE = 'rlike',
  ROLLBACK = 'rollback',
  ROLLUP = 'rollup',
  ROW = 'row',
  ROWS = 'rows',
  SELECT = 'select',
  SEMI = 'semi',
  SEPARATOR = 'separator',
  SEQUENCE = 'sequence',
  SERDE_PROPERTIES = 'serdeProperties',
  SET = 'set',
  SETTINGS = 'settings',
  SHOW = 'show',
  SIMILAR_TO = 'similarTo',
  SOME = 'some',
  SORT_BY = 'sortBy',
  SOUNDS_LIKE = 'soundsLike',
  START_WITH = 'startWith',
  STORAGE_INTEGRATION = 'storageIntegration',
  STRAIGHT_JOIN = 'straightJoin',
  STRUCT = 'struct',
  SUMMARIZE = 'summarize',
  TABLE_SAMPLE = 'tableSample',
  TAG = 'tag',
  TEMPORARY = 'temporary',
  TOP = 'top',
  THEN = 'then',
  TRUE = 'true',
  TRUNCATE = 'truncate',
  UNCACHE = 'uncache',
  UNION = 'union',
  UNNEST = 'unnest',
  UNPIVOT = 'unpivot',
  UPDATE = 'update',
  USE = 'use',
  USING = 'using',
  VALUES = 'values',
  VARIADIC = 'variadic',
  VIEW = 'view',
  SEMANTIC_VIEW = 'semanticView',
  VOLATILE = 'volatile',
  WHEN = 'when',
  WHERE = 'where',
  WINDOW = 'window',
  WITH = 'with',
  UNIQUE = 'unique',
  UTC_DATE = 'utcDate',
  UTC_TIME = 'utcTime',
  UTC_TIMESTAMP = 'utcTimestamp',
  VERSION_SNAPSHOT = 'versionSnapshot',
  TIMESTAMP_SNAPSHOT = 'timestampSnapshot',
  OPTION = 'option',
  SINK = 'sink',
  SOURCE = 'source',
  ANALYZE = 'analyze',
  NAMESPACE = 'namespace',
  EXPORT = 'export',

  // sentinel
  HIVE_TOKEN_STREAM = 'hiveTokenStream',
}

/**
 * Represents a single token in the SQL lexical analysis.
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

export interface TokenizerOptions {
  dialect?: Dialect | string;
  [index: string]: unknown;
}

/**
 * Token pair type for denoting opening/closing delimiters.
 */
export type TokenPair = string | [string, string];

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
  static get BYTE_STRING_ESCAPES (): string[] {
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
   * @example { '--': undefined, '{#': '#}' }
   */
  static get _COMMENTS (): Record<string, string | undefined> {
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
  static get _FORMAT_STRINGS (): Record<string, [string, TokenType]> {
    let cached = this.formatStringsCache.get(this);
    if (!cached) {
      // Generate national strings by prefixing 'n' or 'N' to all quotes
      const nationalStrings: Record<string, [string, TokenType]> = {};
      for (const [start, end] of Object.entries(this._QUOTES)) {
        for (const prefix of ['n', 'N']) {
          nationalStrings[prefix + start] = [end, TokenType.STRING];
        }
      }

      cached = {
        ...nationalStrings,
        ...this.quotesToFormat(TokenType.STRING, this.BIT_STRINGS),
        ...this.quotesToFormat(TokenType.STRING, this.BYTE_STRINGS),
        ...this.quotesToFormat(TokenType.STRING, this.HEX_STRINGS),
        ...this.quotesToFormat(TokenType.STRING, this.RAW_STRINGS),
        ...this.quotesToFormat(TokenType.STRING, this.HEREDOC_STRINGS),
        ...this.quotesToFormat(TokenType.STRING, this.UNICODE_STRINGS),
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
  static get _IDENTIFIERS (): Record<string, string> {
    let cached = this.identifiersCache.get(this);
    if (!cached) {
      cached = this.convertQuotes(this.IDENTIFIERS);
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
  static get _IDENTIFIER_ESCAPES (): Set<string> {
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
  static get _QUOTES (): Record<string, string> {
    let cached = this.quotesCache.get(this);
    if (!cached) {
      cached = this.convertQuotes(this.QUOTES);
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
  static get _STRING_ESCAPES (): Set<string> {
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
  static get _BYTE_STRING_ESCAPES (): Set<string> {
    let cached = this.byteStringEscapesCache.get(this);
    if (!cached) {
      cached = new Set(this.BYTE_STRING_ESCAPES);
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
  static get _ESCAPE_FOLLOW_CHARS (): Set<string> {
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
  static get _KEYWORD_TRIE (): TrieNode {
    let cached = this.keywordTrieCache.get(this);
    if (!cached) {
      const singleTokenChars = Object.keys(this.SINGLE_TOKENS);
      const keys = [
        ...Object.keys(this.KEYWORDS),
        ...Object.keys(this._COMMENTS),
        ...Object.keys(this._QUOTES),
        ...Object.keys(this._FORMAT_STRINGS),
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
   * Override this in subclasses to add or change keywords for a dialect.
   * The `KEYWORDS` getter will return this value (with caching).
   */
  static ORIGINAL_KEYWORDS: Record<string, TokenType> = {
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

  /**
   * Map of SQL keywords and operators to their corresponding token types.
   *
   * Lazily computed and cached per class. Delegates to `ORIGINAL_KEYWORDS`.
   *
   * @final Do not override this getter in subclasses; override `ORIGINAL_KEYWORDS` instead.
   * @example { 'SELECT': TokenType.SELECT, 'FROM': TokenType.FROM }
   */
  static get KEYWORDS (): Record<string, TokenType> {
    let cached = this.keywordsCache.get(this);
    if (!cached) {
      cached = this.ORIGINAL_KEYWORDS;
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
  static COMMENTS: (string | [string, string])[] = ['--', ['/*', '*/']];

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
  private line = 1;
  /** Current column number. */
  private _col = 0;
  /** Accumulated comments for the next token. */
  private comments: string[] = [];
  /** Current character being processed. */
  private char = '';
  /** Whether we've reached the end of the SQL string. */
  private _end = false;
  /** The next character to be processed. */
  private peek = '';
  /** Line number of the previously added token. */
  private prevTokenLine = -1;

  constructor (options: TokenizerOptions = {}) {
    const { dialect } = options;
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
    this.line = 1;
    this._col = 0;
    this.comments = [];
    this.char = '';
    this._end = false;
    this.peek = '';
    this.prevTokenLine = -1;
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
      this.scan();
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
  private scan (until?: () => boolean): void {
    while (this.size && !this._end) {
      let current = this._current;

      // Skip spaces here rather than iteratively calling advance() for performance reasons
      while (current < this.size) {
        const char = this.sql[current];

        if (this.isWhitespace(char) && (char === ' ' || char === '\t')) {
          current++;
        } else {
          break;
        }
      }

      const offset = this._current < current
        ? current - this._current
        : 1;

      this._start = current;
      this.advance({ i: offset });

      if (!this.isWhitespace(this.char)) {
        if (this.isDigit(this.char)) {
          this.scanNumber();
        } else if (this.char in (this._constructor)._IDENTIFIERS) {
          this.scanIdentifier((this._constructor)._IDENTIFIERS[this.char]);
        } else {
          this.scanKeywords();
        }
      }

      if (until?.()) {
        break;
      }
    }

    if (this.tokens.length && this.comments.length) {
      this.tokens[this.tokens.length - 1].comments.push(...this.comments);
    }
  }

  private chars (size: number): string {
    if (size === 1) {
      return this.char;
    }

    const start = this._current - 1;
    const end = start + size;

    return end <= this.size
      ? this.sql.slice(start, end)
      : '';
  }

  /**
   * Advances the current position in the SQL string.
   *
   * @param opts.i - Number of characters to advance
   * @param opts.alnum - If true, fast-forward through alphanumeric characters
   */
  private advance (opts: {
    i?: number;
    alnum?: boolean;
  } = {}): void {
    const {
      i = 1, alnum = false,
    } = opts;
    const constructor = this._constructor;
    if (constructor.WHITE_SPACE[this.char] === TokenType.BREAK) {
      // Ensures we don't count an extra line if we get a \r\n line break sequence
      if (!(this.char === '\r' && this.peek === '\n')) {
        this._col = i;
        this.line += 1;
      }
    } else {
      this._col += i;
    }

    this._current += i;
    this._end = this.size <= this._current;
    this.char = this.sql[this._current - 1];
    this.peek = this._end
      ? ''
      : this.sql[this._current];

    if (alnum && this.isAlnum(this.char)) {
      // Here we use local variables instead of attributes for better performance
      let {
        _col, _current, _end, peek: _peek,
      } = this;

      while (this.isAlnum(_peek)) {
        _col += 1;
        _current += 1;
        _end = this.size <= _current;
        _peek = _end
          ? ''
          : this.sql[_current];
      }

      this._col = _col;
      this._current = _current;
      this._end = _end;
      this.peek = _peek;
      this.char = this.sql[_current - 1];
    }
  }

  private get text (): string {
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
  private add (tokenType: TokenType, text?: string): void {
    this.prevTokenLine = this.line;

    if (this.comments.length && tokenType === TokenType.SEMICOLON && this.tokens.length) {
      this.tokens[this.tokens.length - 1].comments.push(...this.comments);
      this.comments = [];
    }

    this.tokens.push(
      new Token(
        tokenType,
        text ?? this.text,
        this.line,
        this._col,
        this._start,
        this._current - 1,
        this.comments,
      ),
    );
    this.comments = [];

    const constructor = this._constructor;
    // If we have either a semicolon or a begin token before the command's token, we'll parse
    // whatever follows the command's token as a string
    if (
      constructor.COMMANDS.has(tokenType)
      && this.peek !== ';'
      && (this.tokens.length === 1 || constructor.COMMAND_PREFIX_TOKENS.has(this.tokens[this.tokens.length - 2].tokenType))
    ) {
      const start = this._current;
      const tokensLength = this.tokens.length;
      this.scan(() => this.peek === ';');
      this.tokens = this.tokens.slice(0, tokensLength);
      const commandText = this.sql.slice(start, this._current).trim();
      if (commandText) {
        this.add(TokenType.STRING, commandText);
      }
    }
  }

  private scanKeywords (): void {
    let size = 0;
    let word: string | undefined = undefined;
    let chars = this.text;
    let char = chars;
    let prevSpace = false;
    let skip = false;
    const constructor = this._constructor;
    let trie = constructor._KEYWORD_TRIE;
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
        const isSpace = this.isWhitespace(char);

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
      if (this.scanString(word)) {
        return;
      }
      if (this.scanComment(word)) {
        return;
      }
      if (prevSpace || singleToken || !char) {
        this.advance({ i: size - 1 });
        const upper = word.toUpperCase();
        this.add(constructor.KEYWORDS[upper], upper);
        return;
      }
    }

    const type = this._constructor.SINGLE_TOKENS[this.char];
    if (type !== undefined) {
      this.add(type, this.char);
      return;
    }

    this.scanVar();
  }

  /**
   * Scans a comment (single-line or multi-line with nesting support).
   *
   * @param commentStart - The comment start delimiter
   * @returns True if a comment was scanned
   */
  private scanComment (commentStart: string): boolean {
    const constructor = this._constructor;
    if (!(commentStart in constructor._COMMENTS)) {
      return false;
    }

    const commentStartLine = this.line;
    const commentStartSize = commentStart.length;
    const commentEnd = constructor._COMMENTS[commentStart];

    if (commentEnd) {
      // Skip the comment's start delimiter
      this.advance({ i: commentStartSize });

      let commentCount = 1;
      const commentEndSize = commentEnd.length;

      while (!this._end) {
        if (this.chars(commentEndSize) === commentEnd) {
          commentCount -= 1;
          if (!commentCount) {
            break;
          }
        }

        this.advance({ alnum: true });

        // Nested comments are allowed by some dialects
        if (
          constructor.NESTED_COMMENTS
          && !this._end
          && this.chars(commentStartSize) === commentStart
        ) {
          this.advance({ i: commentStartSize });
          commentCount += 1;
        }
      }

      this.comments.push(this.text.slice(commentStartSize, -commentEndSize + 1));
      this.advance({ i: commentEndSize - 1 });
    } else {
      while (!this._end && constructor.WHITE_SPACE[this.peek] !== TokenType.BREAK) {
        this.advance({
          i: 1,
          alnum: true,
        });
      }
      this.comments.push(this.text.slice(commentStartSize));
    }

    if (
      commentStart === constructor.HINT_START
      && this.tokens.length
      && constructor.TOKENS_PRECEDING_HINT.has(this.tokens[this.tokens.length - 1].tokenType)
    ) {
      this.add(TokenType.HINT);
    }

    // Leading comment is attached to the succeeding token, whilst trailing comment to the preceding.
    if (commentStartLine === this.prevTokenLine) {
      this.tokens[this.tokens.length - 1].comments.push(...this.comments);
      this.comments = [];
      this.prevTokenLine = this.line;
    }

    return true;
  }

  /**
   * Scans a numeric literal, including decimals, scientific notation, and type suffixes.
   */
  private scanNumber (): void {
    const constructor = this._constructor;
    if (this.char === '0') {
      const peek = this.peek.toUpperCase();
      if (peek === 'B') {
        return constructor.BIT_STRINGS.length
          ? this.scanBits()
          : this.add(TokenType.NUMBER);
      } else if (peek === 'X') {
        return constructor.HEX_STRINGS.length
          ? this.scanHex()
          : this.add(TokenType.NUMBER);
      }
    }

    let decimal = false;
    let scientific = 0;

    while (true) {
      if (this.isDigit(this.peek)) {
        this.advance();
      } else if (this.peek === '.' && !decimal) {
        if (this.tokens.length && this.tokens[this.tokens.length - 1].tokenType === TokenType.PARAMETER) {
          return this.add(TokenType.NUMBER);
        }
        decimal = true;
        this.advance();
      } else if (['-', '+'].includes(this.peek) && scientific === 1) {
        // Only consume +/- if followed by a digit
        if (this._current + 1 < this.size && this.isDigit(this.sql[this._current + 1])) {
          scientific += 1;
          this.advance();
        } else {
          return this.add(TokenType.NUMBER);
        }
      } else if (this.peek.toUpperCase() === 'E' && !scientific) {
        scientific += 1;
        this.advance();
      } else if (this.peek === '_' && this.dialect._constructor.NUMBERS_CAN_BE_UNDERSCORE_SEPARATED) {
        this.advance();
      } else if (this.isIdentifierChar(this.peek)) {
        const numberText = this.text;
        let literal = '';

        while (this.peek.trim() && !this._constructor.SINGLE_TOKENS[this.peek]) {
          literal += this.peek;
          this.advance();
        }

        const tokenType = constructor.KEYWORDS[constructor.NUMERIC_LITERALS[literal.toUpperCase()] || ''];

        if (tokenType) {
          this.add(TokenType.NUMBER, numberText);
          this.add(TokenType.DCOLON, '::');
          return this.add(tokenType, literal);
        }

        this.advance({ i: -literal.length });
        return this.add(TokenType.NUMBER, numberText);
      } else {
        return this.add(TokenType.NUMBER);
      }
    }
  }

  private scanBits (): void {
    this.advance();
    const value = this.extractValue();
    // If `value` can't be converted to a binary, fallback to tokenizing it as an identifier
    if (!Number.isNaN(parseInt(value, 2))) {
      this.add(TokenType.BIT_STRING, value.slice(2)); // Drop the 0b
    } else {
      this.add(TokenType.IDENTIFIER);
    }
  }

  private scanHex (): void {
    this.advance();
    const value = this.extractValue();
    // If `value` can't be converted to a hex, fallback to tokenizing it as an identifier
    if (!Number.isNaN(parseInt(value, 16))) {
      this.add(TokenType.HEX_STRING, value.slice(2)); // Drop the 0x
    } else {
      this.add(TokenType.IDENTIFIER);
    }
  }

  private extractValue (): string {
    while (true) {
      const char = this.peek.trim();
      if (char && !this._constructor.SINGLE_TOKENS[char]) {
        this.advance({
          i: 1,
          alnum: true,
        });
      } else {
        break;
      }
    }

    return this.text;
  }

  /**
   * Scans various string types including quoted strings, format strings, and heredocs.
   *
   * @param start - The string start delimiter
   * @returns True if a string was scanned
   */
  private scanString (start: string): boolean {
    const constructor = this._constructor;
    let base: number | undefined = undefined;
    let tokenType = TokenType.STRING;

    let end: string;
    if (start in constructor._QUOTES) {
      end = constructor._QUOTES[start];
    } else if (start in constructor._FORMAT_STRINGS) {
      [end, tokenType] = constructor._FORMAT_STRINGS[start];

      if (tokenType === TokenType.HEX_STRING) {
        base = 16;
      } else if (tokenType === TokenType.BIT_STRING) {
        base = 2;
      } else if (tokenType === TokenType.HEREDOC_STRING) {
        this.advance();

        let tag: string;
        if (this.char === end) {
          tag = '';
        } else {
          tag = this.extractString(end, undefined, {
            rawString: true,
            raiseUnmatched: !constructor.HEREDOC_TAG_IS_IDENTIFIER,
          });
        }

        if (
          tag
          && constructor.HEREDOC_TAG_IS_IDENTIFIER
          && (this._end || this.isDigit(tag) || this.isWhitespace(tag))
        ) {
          if (!this._end) {
            this.advance({ i: -1 });
          }

          this.advance({ i: -tag.length });
          this.add(constructor.HEREDOC_STRING_ALTERNATIVE);
          return true;
        }

        end = `${start}${tag}${end}`;
      }
    } else {
      return false;
    }

    this.advance({ i: start.length });
    const text = this.extractString(
      end,
      tokenType === TokenType.BYTE_STRING
        ? constructor._BYTE_STRING_ESCAPES
        : constructor._STRING_ESCAPES,
      { rawString: tokenType === TokenType.RAW_STRING },
    );

    if (base && text && Number.isNaN(parseInt(text, base))) {
      throw new Error(`Numeric string contains invalid characters from ${this.line}:${this._start}`);
    }

    this.add(tokenType, text);
    return true;
  }

  private scanIdentifier (identifierEnd: string): void {
    this.advance();
    const constructor = this._constructor;
    const escapes = new Set([...Array.from(constructor._IDENTIFIER_ESCAPES), identifierEnd]);
    const text = this.extractString(identifierEnd, escapes);
    this.add(TokenType.IDENTIFIER, text);
  }

  private scanVar (): void {
    const constructor = this._constructor;
    while (true) {
      const char = this.peek.trim();
      if (char && (constructor.VAR_SINGLE_TOKENS.has(char) || !this._constructor.SINGLE_TOKENS[char])) {
        this.advance({
          i: 1,
          alnum: true,
        });
      } else {
        break;
      }
    }

    this.add(
      this.tokens.length && this.tokens[this.tokens.length - 1].tokenType === TokenType.PARAMETER
        ? TokenType.VAR
        : constructor.KEYWORDS[this.text.toUpperCase()] || TokenType.VAR,
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
  private extractString (
    delimiter: string,
    escapes: Set<string> | undefined = undefined,
    options: { rawString?: boolean;
      raiseUnmatched?: boolean; } = {},
  ): string {
    const {
      rawString = false, raiseUnmatched = true,
    } = options;
    const constructor = this._constructor;
    let text = '';
    const delimSize = delimiter.length;
    escapes = escapes === undefined
      ? constructor._STRING_ESCAPES
      : escapes;

    while (true) {
      if (
        !rawString
        && this.dialect._constructor.UNESCAPED_SEQUENCES
        && this.peek
        && escapes.has(this.char)
      ) {
        const unescapedSequence = this.dialect._constructor.UNESCAPED_SEQUENCES[this.char + this.peek];
        if (unescapedSequence) {
          this.advance({ i: 2 });
          text += unescapedSequence;
          continue;
        }
      }

      const isValidCustomEscape =
        constructor.ESCAPE_FOLLOW_CHARS.length
        && this.char === '\\'
        && !constructor.ESCAPE_FOLLOW_CHARS.includes(this.peek);

      if (
        (constructor.STRING_ESCAPES_ALLOWED_IN_RAW_STRINGS || !rawString)
        && escapes.has(this.char)
        && (this.peek === delimiter || escapes.has(this.peek) || isValidCustomEscape)
        && (!(this.char in constructor._QUOTES) || this.char === this.peek)
      ) {
        if (this.peek === delimiter) {
          text += this.peek;
        } else if (isValidCustomEscape && this.char !== this.peek) {
          text += this.peek;
        } else {
          text += this.char + this.peek;
        }

        if (this._current + 1 < this.size) {
          this.advance({ i: 2 });
        } else {
          throw new TokenError(`Missing ${delimiter} from ${this.line}:${this._current}`);
        }
      } else {
        if (this.chars(delimSize) === delimiter) {
          if (1 < delimSize) {
            this.advance({ i: delimSize - 1 });
          }
          break;
        }

        if (this._end) {
          if (!raiseUnmatched) {
            return text + this.char;
          }

          throw new TokenError(`Missing ${delimiter} from ${this.line}:${this._start}`);
        }

        const current = this._current - 1;
        this.advance({
          i: 1,
          alnum: true,
        });
        text += this.sql.slice(current, this._current - 1);
      }
    }

    return text;
  }

  private static convertQuotes (arr: Iterable<TokenPair>): Record<string, string> {
    const res: Record<string, string> = {};
    for (const item of arr) {
      const key = typeof item === 'string'
        ? item
        : item[0];
      const value = typeof item === 'string'
        ? item
        : item[1];
      res[key] = value;
    }
    return res;
  }

  private static quotesToFormat (
    tokenType: TokenType,
    arr: TokenPair[],
  ): Record<string, [string, TokenType]> {
    const quotes = this.convertQuotes(arr);
    const result: Record<string, [string, TokenType]> = {};
    for (const [k, v] of Object.entries(quotes)) {
      result[k] = [v, tokenType];
    }
    return result;
  }

  // Helper methods

  private isWhitespace (char: string): boolean {
    return /\s/.test(char);
  }

  private isDigit (char: string): boolean {
    return /\d/.test(char);
  }

  private isAlnum (char: string): boolean {
    return /[a-zA-Z0-9]/.test(char);
  }

  private isIdentifierChar (char: string): boolean {
    return /[a-zA-Z_]/.test(char);
  }

  private get _constructor (): typeof Tokenizer {
    return this.constructor as typeof Tokenizer;
  }
}
