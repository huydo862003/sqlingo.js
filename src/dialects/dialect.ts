// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/dialects/dialect.py

import type { Expression } from '../expressions';
import {
  ExceptExpr, IntersectExpr, UnionExpr,
} from '../expressions';
import { Tokenizer } from '../tokens';
import {
  newTrie, type TrieNode,
} from '../trie';

/**
 * Base unescaped sequences that are common across dialects.
 */
const BASE_UNESCAPED_SEQUENCES: Record<string, string> = {
  '\\a': '\x07',
  '\\b': '\b',
  '\\f': '\f',
  '\\n': '\n',
  '\\r': '\r',
  '\\t': '\t',
  '\\v': '\v',
  '\\\\': '\\',
};

/**
 * Dialects supported by SQLGlot.
 * @enum
 */
export enum Dialects {
  DIALECT = '',

  ATHENA = 'athena',
  BIGQUERY = 'bigquery',
  CLICKHOUSE = 'clickhouse',
  DATABRICKS = 'databricks',
  DORIS = 'doris',
  DREMIO = 'dremio',
  DRILL = 'drill',
  DRUID = 'druid',
  DUCKDB = 'duckdb',
  DUNE = 'dune',
  FABRIC = 'fabric',
  HIVE = 'hive',
  MATERIALIZE = 'materialize',
  MYSQL = 'mysql',
  ORACLE = 'oracle',
  POSTGRES = 'postgres',
  PRESTO = 'presto',
  PRQL = 'prql',
  REDSHIFT = 'redshift',
  RISINGWAVE = 'risingwave',
  SNOWFLAKE = 'snowflake',
  SOLR = 'solr',
  SPARK = 'spark',
  SPARK2 = 'spark2',
  SQLITE = 'sqlite',
  STARROCKS = 'starrocks',
  TABLEAU = 'tableau',
  TERADATA = 'teradata',
  TRINO = 'trino',
  TSQL = 'tsql',
  EXASOL = 'exasol',
}

/**
 * Specifies the strategy according to which identifiers should be normalized.
 */
export enum NormalizationStrategy {
  /** Unquoted identifiers are lowercased. */
  LOWERCASE = 'LOWERCASE',
  /** Unquoted identifiers are uppercased. */
  UPPERCASE = 'UPPERCASE',
  /** Always case-sensitive, regardless of quotes. */
  CASE_SENSITIVE = 'CASE_SENSITIVE',
  /** Always case-insensitive (lowercase), regardless of quotes. */
  CASE_INSENSITIVE = 'CASE_INSENSITIVE',
  /** Always case-insensitive (uppercase), regardless of quotes. */
  CASE_INSENSITIVE_UPPERCASE = 'CASE_INSENSITIVE_UPPERCASE',
}

export type DialectType = string | Dialect | typeof Dialect;

/**
 * Base dialect class for SQL parsing and generation.
 *
 * Dialect = sqlglot's _Dialect (metaclass) + Dialect
 */
export class Dialect {
  // Registry of dialect classes (simulates metaclass _Dialect's classes)
  protected static _registry: Map<string, typeof Dialect> = new Map();

  /** The base index offset for arrays. */
  static INDEX_OFFSET = 0;

  /** First day of the week in DATE_TRUNC(week). Defaults to 0 (Monday). -1 would be Sunday. */
  static WEEK_OFFSET = 0;

  /** Whether `UNNEST` table aliases are treated as column aliases. */
  static UNNEST_COLUMN_ONLY = false;

  /** Whether the table alias comes after tablesample. */
  static ALIAS_POST_TABLESAMPLE = false;

  /** Whether a size in the table sample clause represents percentage. */
  static TABLESAMPLE_SIZE_IS_PERCENT = false;

  /** Specifies the strategy according to which identifiers should be normalized. */
  static NORMALIZATION_STRATEGY = NormalizationStrategy.LOWERCASE;

  /** Whether an unquoted identifier can start with a digit. */
  static IDENTIFIERS_CAN_START_WITH_DIGIT = false;

  /** Whether the DPIPE token (`||`) is a string concatenation operator. */
  static DPIPE_IS_STRING_CONCAT = true;

  /** Whether `CONCAT`'s arguments must be strings. */
  static STRICT_STRING_CONCAT = false;

  /** Whether user-defined data types are supported. */
  static SUPPORTS_USER_DEFINED_TYPES = true;

  /** Whether `SEMI` or `ANTI` joins are supported. */
  static SUPPORTS_SEMI_ANTI_JOIN = true;

  /** Whether the old-style outer join (+) syntax is supported. */
  static get SUPPORTS_COLUMN_JOIN_MARKS (): boolean {
    return '(+)' in this.tokenizerClass.KEYWORDS;
  }

  /**
   * Determines how function names are going to be normalized.
   * Possible values:
   *   "upper" or true: Convert names to uppercase.
   *   "lower": Convert names to lowercase.
   *   false: Disables function name normalization.
   */
  static NORMALIZE_FUNCTIONS: false | 'upper' | 'lower' = 'upper';

  /**
   * Whether the name of the function should be preserved inside the node's metadata.
   */
  static PRESERVE_ORIGINAL_NAMES = false;

  /**
   * Whether the base comes first in the `LOG` function.
   * Possible values: true, false, undefined (two arguments are not supported by `LOG`)
   */
  static LOG_BASE_FIRST: boolean | undefined = true;

  /**
   * Default `NULL` ordering method to use if not explicitly set.
   * Possible values: "nulls_are_small", "nulls_are_large", "nulls_are_last"
   */
  static NULL_ORDERING: 'nulls_are_small' | 'nulls_are_large' | 'nulls_are_last' = 'nulls_are_small';

  /**
   * Whether the behavior of `a / b` depends on the types of `a` and `b`.
   * false means `a / b` is always float division.
   * true means `a / b` is integer division if both `a` and `b` are integers.
   */
  static TYPED_DIVISION = false;

  /** Whether division by zero throws an error (false) or returns NULL (true). */
  static SAFE_DIVISION = false;

  /** A `NULL` arg in `CONCAT` yields `NULL` by default, but in some dialects it yields an empty string. */
  static CONCAT_COALESCE = false;

  /** Whether the `HEX` function returns a lowercase hexadecimal string. */
  static HEX_LOWERCASE = false;

  static DATE_FORMAT = '\'%Y-%m-%d\'';
  static DATEINT_FORMAT = '\'%Y%m%d\'';
  static TIME_FORMAT = '\'%Y-%m-%d %H:%M:%S\'';

  /** Associates this dialect's time formats with their equivalent Python `strftime` formats. */
  static TIME_MAPPING: Record<string, string> = {};

  /**
   * Helper which is used for parsing the special syntax `CAST(x AS DATE FORMAT 'yyyy')`.
   * If empty, the corresponding trie will be constructed off of `TIME_MAPPING`.
   */
  static FORMAT_MAPPING: Record<string, string> = {};

  /** Mapping of an escaped sequence (`\\n`) to its unescaped version (`\n`). */
  static UNESCAPED_SEQUENCES: Record<string, string> = {};

  /**
   * Columns that are auto-generated by the engine corresponding to this dialect.
   * For example, such columns may be excluded from `SELECT *` queries.
   */
  static PSEUDOCOLUMNS: Set<string> = new Set();

  /**
   * Some dialects allow you to reference a CTE column alias in the HAVING clause.
   */
  static PREFER_CTE_ALIAS_COLUMN = false;

  /** Separator of COPY statement parameters. */
  static COPY_PARAMS_ARE_CSV = true;

  /**
   * Whether alias reference expansion should run before column qualification.
   */
  static FORCE_EARLY_ALIAS_REF_EXPANSION = false;

  /** Whether alias reference expansion before qualification should only happen for the GROUP BY clause. */
  static EXPAND_ONLY_GROUP_ALIAS_REF = false;

  /** Whether to annotate all scopes during optimization. */
  static ANNOTATE_ALL_SCOPES = false;

  /** Whether alias reference expansion is disabled for this dialect. */
  static DISABLES_ALIAS_REF_EXPANSION = false;

  /** Whether alias references are allowed in JOIN ... ON clauses. */
  static SUPPORTS_ALIAS_REFS_IN_JOIN_CONDITIONS = false;

  /** Whether ORDER BY ALL is supported. */
  static SUPPORTS_ORDER_BY_ALL = false;

  /** Whether projection alias names can shadow table/source names in GROUP BY and HAVING clauses. */
  static PROJECTION_ALIASES_SHADOW_SOURCE_NAMES = false;

  /** Whether table names can be referenced as columns (treated as structs). */
  static TABLES_REFERENCEABLE_AS_COLUMNS = false;

  /** Whether the dialect supports expanding struct fields using star notation. */
  static SUPPORTS_STRUCT_STAR_EXPANSION = false;

  /** Whether pseudocolumns should be excluded from star expansion (SELECT *). */
  static EXCLUDES_PSEUDOCOLUMNS_FROM_STAR = false;

  /** Whether query results are typed as structs in metadata for type inference. */
  static QUERY_RESULTS_ARE_STRUCTS = false;

  /** Whether struct field access requires parentheses around the expression. */
  static REQUIRES_PARENTHESIZED_STRUCT_ACCESS = false;

  /** Whether NULL/VOID is supported as a valid data type. */
  static SUPPORTS_NULL_TYPE = false;

  /** Whether COALESCE in comparisons has non-standard NULL semantics. */
  static COALESCE_COMPARISON_NON_STANDARD = false;

  /** Whether the ARRAY constructor is context-sensitive. */
  static HAS_DISTINCT_ARRAY_CONSTRUCTORS = false;

  /** Whether expressions like x::INT[5] should be parsed as fixed-size array defs/casts. */
  static SUPPORTS_FIXED_SIZE_ARRAYS = false;

  /** Whether failing to parse a JSON path expression will log a warning. */
  static STRICT_JSON_PATH_SYNTAX = true;

  /** Whether empty ON condition should error before attempting to parse. */
  static ON_CONDITION_EMPTY_BEFORE_ERROR = true;

  /** Whether ArrayAgg needs to filter NULL values. */
  static ARRAY_AGG_INCLUDES_NULLS: boolean | undefined = true;

  /** Whether Array update functions return NULL when the input array is NULL. */
  static ARRAY_FUNCS_PROPAGATES_NULLS = false;

  /**
   * This flag is used in the optimizer's canonicalize rule and determines whether x will be promoted
   * to the literal's type in x::DATE < '2020-01-01 12:05:03' (i.e., DATETIME). When false, the literal
   * is cast to x's type to match it instead.
   */
  static PROMOTE_TO_INFERRED_DATETIME_TYPE = false;

  /** Whether the DEFAULT keyword is supported in the VALUES clause. */
  static SUPPORTS_VALUES_DEFAULT = true;

  /** Whether number literals can include underscores for better readability. */
  static NUMBERS_CAN_BE_UNDERSCORE_SEPARATED = false;

  /** Whether hex strings such as x'CC' evaluate to integer or binary/blob type. */
  static HEX_STRING_IS_INTEGER_TYPE = false;

  /** The default value for the capturing group. */
  static REGEXP_EXTRACT_DEFAULT_GROUP = 0;

  /** Whether REGEXP_EXTRACT returns NULL when the position arg exceeds the string length. */
  static REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL = true;

  /**
   * Whether a set operation uses DISTINCT by default. This is undefined when either DISTINCT or ALL
   * must be explicitly specified.
   */
  static SET_OP_DISTINCT_BY_DEFAULT: Map<typeof Expression, boolean | undefined> = new Map([
    [ExceptExpr, true],
    [IntersectExpr, true],
    [UnionExpr, true],
  ]);

  /**
   * Helper for dialects that use a different name for the same creatable kind.
   * For example, the Clickhouse equivalent of CREATE SCHEMA is CREATE DATABASE.
   */
  static CREATABLE_KIND_MAPPING: Record<string, string> = {};

  /**
   * Hive by default does not update the schema of existing partitions when a column is changed.
   * The CASCADE clause is used to indicate that the change should be propagated to all existing partitions.
   * The Spark dialect, while derived from Hive, does not support the CASCADE clause.
   */
  static ALTER_TABLE_SUPPORTS_CASCADE = false;

  /** Whether ADD is present for each column added by ALTER TABLE. */
  static ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN = true;

  /**
   * Whether the value/LHS of the TRY_CAST(<value> AS <type>) should strictly be a
   * STRING type (Snowflake's case) or can be of any type.
   */
  static TRY_CAST_REQUIRES_STRING: boolean | undefined = undefined;

  /**
   * Whether the double negation can be applied.
   * Not safe with MySQL and SQLite due to type coercion (may not return boolean).
   */
  static SAFE_TO_ELIMINATE_DOUBLE_NEGATION = true;

  /** Whether the INITCAP function supports custom delimiter characters as the second argument. */
  static INITCAP_SUPPORTS_CUSTOM_DELIMITERS = true;

  /** Default delimiter characters for INITCAP function: whitespace and non-alphanumeric characters. */
  static INITCAP_DEFAULT_DELIMITER_CHARS = ' \t\n\r\f\v!"#$%&\'()*+,\\-./:;<=>?@\\[\\]^_`{|}~';

  /** Whether byte string literals (ex: BigQuery's b'...') are typed as BYTES/BINARY. */
  static BYTE_STRING_IS_BYTES_TYPE = false;

  /** Whether a UUID is considered a string or a UUID type. */
  static UUID_IS_STRING_TYPE = false;

  /** Whether JSON_EXTRACT_SCALAR returns undefined if a non-scalar value is selected. */
  static JSON_EXTRACT_SCALAR_SCALAR_ONLY = false;

  /**
   * Maps function expressions to their default output column name(s).
   * For example, in Postgres, generate_series function outputs a column named "generate_series" by default.
   */
  static DEFAULT_FUNCTIONS_COLUMN_NAMES: Record<string, string | string[]> = {};

  /** The default type of NULL for producing the correct projection type. */
  static DEFAULT_NULL_TYPE = 'UNKNOWN';

  /**
   * Whether LEAST/GREATEST functions ignore NULL values, e.g:
   * - BigQuery, Snowflake, MySQL, Presto/Trino: LEAST(1, NULL, 2) -> NULL
   * - Spark, Postgres, DuckDB, TSQL: LEAST(1, NULL, 2) -> 1
   */
  static LEAST_GREATEST_IGNORES_NULLS = true;

  /** Whether to prioritize non-literal types over literals during type annotation. */
  static PRIORITIZE_NON_LITERAL_TYPES = false;

  // --- Autofilled by metaclass in Python, set as instance properties in TypeScript ---

  // Tokenizer class reference
  static tokenizerClass: typeof Tokenizer = Tokenizer;
  // TODO: add jsonpathTokenizerClass
  // TODO: add parserClass

  protected static timeTrieCache = new WeakMap<typeof Dialect, TrieNode>();

  /**
   * Cache a trie of time format keys for efficient matching.
   */
  protected static TIME_TRIE (): TrieNode {
    let cached = this.timeTrieCache.get(this);
    if (!cached) {
      cached = newTrie(Object.keys(this.TIME_MAPPING).map((k) => Array.from(k)));
      this.timeTrieCache.set(this, cached);
    }
    return cached;
  }

  protected static formatTrieCache = new WeakMap<typeof Dialect, TrieNode>();

  protected static FORMAT_TRIE (): TrieNode {
    let cached = this.formatTrieCache.get(this);
    if (!cached) {
      const mapping = Object.keys(this.FORMAT_MAPPING).length > 0 ? this.FORMAT_MAPPING : this.TIME_MAPPING;
      cached = newTrie(Object.keys(mapping).map((k) => Array.from(k)));
      this.formatTrieCache.set(this, cached);
    }
    return cached;
  }

  protected static inverseTimeMappingCache = new WeakMap<typeof Dialect, Record<string, string>>();

  protected static INVERSE_TIME_MAPPING (): Record<string, string> {
    let cached = this.inverseTimeMappingCache.get(this);
    if (!cached) {
      cached = Object.fromEntries(Object.entries(this.TIME_MAPPING).map(([k, v]) => [v, k]));
      this.inverseTimeMappingCache.set(this, cached);
    }
    return cached;
  }

  protected static inverseTimeTrieCache = new WeakMap<typeof Dialect, TrieNode>();

  protected static INVERSE_TIME_TRIE (): TrieNode {
    let cached = this.inverseTimeTrieCache.get(this);
    if (!cached) {
      cached = newTrie(Object.keys(this.INVERSE_TIME_MAPPING()).map((k) => Array.from(k)));
      this.inverseTimeTrieCache.set(this, cached);
    }
    return cached;
  }

  protected static inverseFormatMappingCache = new WeakMap<typeof Dialect, Record<string, string>>();

  protected static INVERSE_FORMAT_MAPPING (): Record<string, string> {
    let cached = this.inverseFormatMappingCache.get(this);
    if (!cached) {
      cached = Object.fromEntries(Object.entries(this.FORMAT_MAPPING).map(([k, v]) => [v, k]));
      this.inverseFormatMappingCache.set(this, cached);
    }
    return cached;
  }

  protected static inverseFormatTrieCache = new WeakMap<typeof Dialect, TrieNode>();

  protected static INVERSE_FORMAT_TRIE (): TrieNode {
    let cached = this.inverseFormatTrieCache.get(this);
    if (!cached) {
      cached = newTrie(Object.keys(this.INVERSE_FORMAT_MAPPING()).map((k) => Array.from(k)));
      this.inverseFormatTrieCache.set(this, cached);
    }
    return cached;
  }

  protected static inverseCreatableKindMappingCache = new WeakMap<typeof Dialect, Record<string, string>>();

  /**
   * Cache the inverse of CREATABLE_KIND_MAPPING.
   * Maps normalized creatable kind names back to their original names.
   */
  protected static INVERSE_CREATABLE_KIND_MAPPING_COMPUTED (): Record<string, string> {
    let cached = this.inverseCreatableKindMappingCache.get(this);
    if (!cached) {
      cached = Object.fromEntries(Object.entries(this.CREATABLE_KIND_MAPPING).map(([k, v]) => [v, k]));
      this.inverseCreatableKindMappingCache.set(this, cached);
    }
    return cached;
  }

  protected static unescapedSequencesCache = new WeakMap<typeof Dialect, Record<string, string>>();

  /**
   * Caches unescaped sequences, merging BASE_UNESCAPED_SEQUENCES
   * with dialect-specific sequences when escape sequences are supported.
   */
  protected static UNESCAPED_SEQUENCES_COMPUTED (): Record<string, string> {
    let cached = this.unescapedSequencesCache.get(this);
    if (!cached) {
      if (this.STRINGS_SUPPORT_ESCAPED_SEQUENCES || this.BYTE_STRINGS_SUPPORT_ESCAPED_SEQUENCES) {
        cached = {
          ...BASE_UNESCAPED_SEQUENCES,
          ...this.UNESCAPED_SEQUENCES,
        };
      } else {
        cached = this.UNESCAPED_SEQUENCES;
      }
      this.unescapedSequencesCache.set(this, cached);
    }
    return cached;
  }

  protected static escapedSequencesCache = new WeakMap<typeof Dialect, Record<string, string>>();

  /**
   * Caches escaped sequences by inverting UNESCAPED_SEQUENCES_COMPUTED.
   */
  protected static ESCAPED_SEQUENCES_COMPUTED (): Record<string, string> {
    let cached = this.escapedSequencesCache.get(this);
    if (!cached) {
      cached = Object.fromEntries(
        Object.entries(this.UNESCAPED_SEQUENCES_COMPUTED()).map(([k, v]) => [v, k]),
      );
      this.escapedSequencesCache.set(this, cached);
    }
    return cached;
  }

  protected static validIntervalUnitsCache = new WeakMap<typeof Dialect, Set<string>>();

  protected static VALID_INTERVAL_UNITS_COMPUTED (): Set<string> {
    let cached = this.validIntervalUnitsCache.get(this);
    if (!cached) {
      cached = new Set([
        ...this.VALID_INTERVAL_UNITS,
        ...Object.keys(this.DATE_PART_MAPPING),
        ...Object.values(this.DATE_PART_MAPPING),
      ]);
      this.validIntervalUnitsCache.set(this, cached);
    }
    return cached;
  }

  /** Mapping of escaped sequences. */
  static ESCAPED_SEQUENCES: Record<string, string> = {};

  protected static quoteDelimitersCache = new WeakMap<typeof Dialect, [string, string]>();

  /**
   * Extracts quote delimiters from the tokenizer class.
   * Returns [start, end] delimiter pair for string literals.
   */
  protected static QUOTE_DELIMITERS (): [string, string] {
    let cached = this.quoteDelimitersCache.get(this);
    if (!cached) {
      const quotes = Object.entries(this.tokenizerClass.QUOTES)[0];
      cached = quotes as [string, string];
      this.quoteDelimitersCache.set(this, cached);
    }
    return cached;
  }

  /** Delimiters for string literals. */
  static get QUOTE_START (): string {
    return this.QUOTE_DELIMITERS()[0];
  }

  static get QUOTE_END (): string {
    return this.QUOTE_DELIMITERS()[1];
  }

  protected static identifierDelimitersCache = new WeakMap<typeof Dialect, [string, string]>();

  /**
   * Extracts identifier delimiters from the tokenizer class.
   * Returns [start, end] delimiter pair for identifiers.
   */
  protected static IDENTIFIER_DELIMITERS (): [string, string] {
    let cached = this.identifierDelimitersCache.get(this);
    if (!cached) {
      const identifiers = Object.entries(this.tokenizerClass.IDENTIFIERS)[0];
      cached = identifiers as [string, string];
      this.identifierDelimitersCache.set(this, cached);
    }
    return cached;
  }

  /** Delimiters for identifiers. */
  static get IDENTIFIER_START (): string {
    return this.IDENTIFIER_DELIMITERS()[0];
  }

  static get IDENTIFIER_END (): string {
    return this.IDENTIFIER_DELIMITERS()[1];
  }

  /** Valid interval units. */
  static VALID_INTERVAL_UNITS: Set<string> = new Set();

  /** Delimiters for bit literals. */
  static get BIT_START (): string | undefined {
    return this.tokenizerClass.BIT_STRINGS[0]?.[0];
  }

  static get BIT_END (): string | undefined {
    return this.tokenizerClass.BIT_STRINGS[0]?.[1];
  }

  /** Delimiters for hex literals. */
  static get HEX_START (): string | undefined {
    return this.tokenizerClass.HEX_STRINGS[0]?.[0];
  }

  static get HEX_END (): string | undefined {
    return this.tokenizerClass.HEX_STRINGS[0]?.[1];
  }

  /** Delimiters for byte literals. */
  static get BYTE_START (): string | undefined {
    return this.tokenizerClass.BYTE_STRINGS[0]?.[0];
  }

  static get BYTE_END (): string | undefined {
    return this.tokenizerClass.BYTE_STRINGS[0]?.[1];
  }

  /** Delimiters for unicode literals. */
  static get UNICODE_START (): string | undefined {
    return this.tokenizerClass.UNICODE_STRINGS[0]?.[0];
  }

  static get UNICODE_END (): string | undefined {
    return this.tokenizerClass.UNICODE_STRINGS[0]?.[1];
  }

  /** Date part mapping for normalization. */
  static DATE_PART_MAPPING: Record<string, string> = {
    'Y': 'YEAR',
    'YY': 'YEAR',
    'YYY': 'YEAR',
    'YYYY': 'YEAR',
    'YR': 'YEAR',
    'YEARS': 'YEAR',
    'YRS': 'YEAR',
    'MM': 'MONTH',
    'MON': 'MONTH',
    'MONS': 'MONTH',
    'MONTHS': 'MONTH',
    'D': 'DAY',
    'DD': 'DAY',
    'DAYS': 'DAY',
    'DAYOFMONTH': 'DAY',
    'DAY OF WEEK': 'DAYOFWEEK',
    'WEEKDAY': 'DAYOFWEEK',
    'DOW': 'DAYOFWEEK',
    'DW': 'DAYOFWEEK',
    'WEEKDAY_ISO': 'DAYOFWEEKISO',
    'DOW_ISO': 'DAYOFWEEKISO',
    'DW_ISO': 'DAYOFWEEKISO',
    'DAYOFWEEK_ISO': 'DAYOFWEEKISO',
    'DAY OF YEAR': 'DAYOFYEAR',
    'DOY': 'DAYOFYEAR',
    'DY': 'DAYOFYEAR',
    'W': 'WEEK',
    'WK': 'WEEK',
    'WEEKOFYEAR': 'WEEK',
    'WOY': 'WEEK',
    'WY': 'WEEK',
    'WEEK_ISO': 'WEEKISO',
    'WEEKOFYEARISO': 'WEEKISO',
    'WEEKOFYEAR_ISO': 'WEEKISO',
    'Q': 'QUARTER',
    'QTR': 'QUARTER',
    'QTRS': 'QUARTER',
    'QUARTERS': 'QUARTER',
    'H': 'HOUR',
    'HH': 'HOUR',
    'HR': 'HOUR',
    'HOURS': 'HOUR',
    'HRS': 'HOUR',
    'M': 'MINUTE',
    'MI': 'MINUTE',
    'MIN': 'MINUTE',
    'MINUTES': 'MINUTE',
    'MINS': 'MINUTE',
    'S': 'SECOND',
    'SEC': 'SECOND',
    'SECONDS': 'SECOND',
    'SECS': 'SECOND',
    'MS': 'MILLISECOND',
    'MSEC': 'MILLISECOND',
    'MSECS': 'MILLISECOND',
    'MSECOND': 'MILLISECOND',
    'MSECONDS': 'MILLISECOND',
    'MILLISEC': 'MILLISECOND',
    'MILLISECS': 'MILLISECOND',
    'MILLISECON': 'MILLISECOND',
    'MILLISECONDS': 'MILLISECOND',
    'US': 'MICROSECOND',
    'USEC': 'MICROSECOND',
    'USECS': 'MICROSECOND',
    'MICROSEC': 'MICROSECOND',
    'MICROSECS': 'MICROSECOND',
    'USECOND': 'MICROSECOND',
    'USECONDS': 'MICROSECOND',
    'MICROSECONDS': 'MICROSECOND',
    'NS': 'NANOSECOND',
    'NSEC': 'NANOSECOND',
    'NANOSEC': 'NANOSECOND',
    'NSECOND': 'NANOSECOND',
    'NSECONDS': 'NANOSECOND',
    'NANOSECS': 'NANOSECOND',
    'EPOCH_SECOND': 'EPOCH',
    'EPOCH_SECONDS': 'EPOCH',
    'EPOCH_MILLISECONDS': 'EPOCH_MILLISECOND',
    'EPOCH_MICROSECONDS': 'EPOCH_MICROSECOND',
    'EPOCH_NANOSECONDS': 'EPOCH_NANOSECOND',
    'TZH': 'TIMEZONE_HOUR',
    'TZM': 'TIMEZONE_MINUTE',
    'DEC': 'DECADE',
    'DECS': 'DECADE',
    'DECADES': 'DECADE',
    'MIL': 'MILLENNIUM',
    'MILS': 'MILLENNIUM',
    'MILLENIA': 'MILLENNIUM',
    'C': 'CENTURY',
    'CENT': 'CENTURY',
    'CENTS': 'CENTURY',
    'CENTURIES': 'CENTURY',
  };

  /** Specifies what types a given type can be coerced into. */
  static COERCES_TO: Record<string, Set<string>> = {};

  /** Specifies type inference & validation rules for expressions. */
  static EXPRESSION_METADATA: Record<string, unknown> = {};

  /** Determines the supported Dialect instance settings. */
  static SUPPORTED_SETTINGS: Set<string> = new Set(['normalization_strategy', 'version']);

  /** Whether strings support escaped sequences. */
  static get STRINGS_SUPPORT_ESCAPED_SEQUENCES (): boolean {
    return this.tokenizerClass.STRING_ESCAPES.includes('\\');
  }

  /** Whether byte strings support escaped sequences. */
  static get BYTE_STRINGS_SUPPORT_ESCAPED_SEQUENCES (): boolean {
    return this.tokenizerClass.BYTE_STRING_ESCAPES().includes('\\');
  }

  /** Inverse mapping of CREATABLE_KIND_MAPPING. */
  static INVERSE_CREATABLE_KIND_MAPPING: Record<string, string> = {};

  /**
   * Register a dialect class.
   */
  static register (name: string, dialectClass: typeof Dialect): void {
    this._registry.set(name.toLowerCase(), dialectClass);
  }

  /**
   * Get a dialect class by name.
   */
  static get (name: string, fallback?: typeof Dialect): typeof Dialect | undefined {
    return this._registry.get(name.toLowerCase()) || fallback;
  }

  /**
   * Look up a dialect in the global dialect registry and return it if it exists.
   *
   * @param dialect - The target dialect. If this is a string, it can be optionally followed by
   *   additional key-value pairs that are separated by commas and are used to specify dialect settings.
   *
   * @example
   * ```ts
   * const dialect = Dialect.getOrRaise("duckdb");
   * const dialect2 = Dialect.getOrRaise("mysql, normalization_strategy = case_sensitive");
   * ```
   */
  static getOrRaise (dialect?: DialectType): Dialect {
    if (!dialect) {
      return new this();
    }

    if (typeof dialect === 'function') {
      return new dialect();
    }

    if (dialect instanceof Dialect) {
      return dialect;
    }

    if (typeof dialect === 'string') {
      const parts = dialect.split(',');
      const dialectName = parts[0].trim();
      const kwargs: Record<string, boolean | string> = {};

      for (let i = 1; i < parts.length; i++) {
        const kv = parts[i].split('=');
        const key = kv[0].trim();
        let value: boolean | string = true;

        if (kv.length === 2) {
          const val = kv[1].trim().toLowerCase();
          if (val === 'true') {
            value = true;
          } else if (val === 'false') {
            value = false;
          } else {
            value = kv[1].trim();
          }
        }

        kwargs[key] = value;
      }

      const DialectClass = this.get(dialectName);
      if (!DialectClass) {
        throw new Error(`Unknown dialect: '${dialectName}'`);
      }

      return new DialectClass(kwargs);
    }

    throw new Error(`Invalid dialect type: '${typeof dialect}'`);
  }

  version: [number, number, number];
  normalizationStrategy: NormalizationStrategy;
  settings: Record<string, boolean | string>;

  constructor (kwargs: Record<string, boolean | string | number> = {}) {
    const versionStr = String(kwargs.version ?? Number.MAX_SAFE_INTEGER);
    const parts = versionStr.split('.');
    while (parts.length < 3) {
      parts.push('0');
    }
    this.version = [
      parseInt(parts[0]),
      parseInt(parts[1]),
      parseInt(parts[2]),
    ];

    const normalizationStrategy = kwargs.normalization_strategy;
    if (normalizationStrategy === undefined) {
      this.normalizationStrategy = this._constructor.NORMALIZATION_STRATEGY;
    } else {
      this.normalizationStrategy = NormalizationStrategy[String(normalizationStrategy).toUpperCase() as keyof typeof NormalizationStrategy];
    }

    // Remove version and normalization_strategy from settings
    const {
      version: _version, normalization_strategy: _ns, ...settings
    } = kwargs;
    this.settings = settings as Record<string, boolean | string>;
  }

  equals (other: unknown): boolean {
    if (this === other) {
      return true;
    }
    if (other instanceof Dialect) {
      return this.constructor === other.constructor;
    }
    return false;
  }

  hash (): string {
    return this.constructor.name.toLowerCase();
  }

  /**
   * Checks if text contains any case sensitive characters, based on the dialect's rules.
   */
  caseSensitive (text: string): boolean {
    if (this.normalizationStrategy === NormalizationStrategy.CASE_INSENSITIVE) {
      return false;
    }

    const unsafe = this.normalizationStrategy === NormalizationStrategy.UPPERCASE
      ? (char: string) => char === char.toLowerCase() && char !== char.toUpperCase()
      : (char: string) => char === char.toUpperCase() && char !== char.toLowerCase();

    return Array.from(text).some(unsafe);
  }

  /**
   * Tokenize SQL string into tokens.
   */
  tokenize (sql: string): ReturnType<Tokenizer['tokenize']> {
    return this.tokenizer().tokenize(sql);
  }

  /**
   * Get a tokenizer instance for this dialect.
   */
  tokenizer (): Tokenizer {
    return new this._constructor.tokenizerClass(this);
  }

  /**
   * Generate SQL from an expression tree.
   */
  generate (expression: Expression, options?: { copy?: boolean;
    [key: string]: unknown; }): string {
  }

  /**
   * Get or create a generator instance for this dialect.
   */
  generator (options?: Record<string, unknown>) {
  }

  // TODO: Port these methods when classes are available
  // normalizeIdentifier(expression: E): E
  // canQuote(identifier: Identifier, identify?: string | boolean): boolean
  // quoteIdentifier(expression: E, identify?: boolean): E
  // toJsonPath(path?: Expression): Expression | undefined
  // parse(sql: string, opts?: unknown): Expression[]
  // parseInto(expressionType: unknown, sql: string, opts?: unknown): Expression[]
  // transpile(sql: string, opts?: unknown): string[]
  // jsonpathTokenizer(opts?: unknown): JSONPathTokenizer
  // parser(opts?: unknown): Parser
  // generateValuesAliases(expression: Values): Identifier[]

  get _constructor (): typeof Dialect {
    return this.constructor as typeof Dialect;
  }
}

// Register the base Dialect
Dialect.register('', Dialect);
Dialect.register('dialect', Dialect);
