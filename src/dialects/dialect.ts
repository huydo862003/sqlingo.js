// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/dialects/dialect.py

import type {
  DateSubExpr,
  JsonbExtractScalarExpr,
  JsonExtractScalarExpr,
  TimeSubExpr,
  TimestampSubExpr,
  TsOrDsDiffExpr,
  ApproxDistinctExpr,
  ILikeExpr,
  CurrentDateExpr,
  WithExpr,
  TableSampleExpr,
  PivotExpr,
  CommentColumnConstraintExpr,
  MapFromEntriesExpr,
  PropertyExpr,
  StrPositionExpr,
  StructExtractExpr,
  ArrayAppendExpr,
  ArrayPrependExpr,
  ArrayConcatExpr,
  MapExpr,
  VarMapExpr,
  MonthsBetweenExpr,
  UnixToStrExpr,
  StrToUnixExpr,
  RegexpReplaceExpr,
  RegexpExtractAllExpr,
  FuncExpr,
  ConcatWsExpr,
  ConcatExpr,
  StrToTimeExpr,
  TrimExpr,
  CountIfExpr,
  MaxExpr,
  MinExpr,
  EncodeExpr,
  DecodeExpr,
  DateStrToDateExpr,
  TimeStrToTimeExpr,
  RightExpr,
  LeftExpr,
  DatetimeExpr,
  TimeExpr,
  TimestampExpr,
  Sha2Expr,
  DatetimeDiffExpr,
  TimestampDiffExpr,
  MakeIntervalExpr,
  GroupConcatExpr,
  ArrayCompactExpr,
  GetbitExpr,
  TimeUnitExpr,
  GeneratedAsIdentityColumnConstraintExpr,
  ArgMaxExpr,
  ArgMinExpr,
  MergeExpr,
  AnyValueExpr,
  XorExpr,
  JsonExtractExprArgs,
  RegexpExtractExprArgs,
  GenerateDateArrayExpr,
  ToNumberExpr,
  Sha2DigestExpr,
  ExpressionValue,
} from '../expressions';
import {
  cache,
  assertIsInstanceOf, isInstanceOf,
} from '../port_internals';
import {
  SelectExpr,
  JsonbExtractExpr,
  DatetimeSubExpr,
  RegexpExtractExpr,
  PosexplodeExpr,
  TableAliasExpr,
  ExplodeExpr,
  KwargExpr,
  ArrayRemoveExpr,
  JsonPathPartExpr,
  LimitExpr,
  PlaceholderExpr,
  JoinExprKind,
  UpdateExpr,
  InsertExpr,
  TupleExpr,
  WhenExpr,
  GenerateSeriesExpr,
  DateDiffExpr,
  DayExpr,
  LastDayExpr,
  DivExpr,
  ParenExpr,
  SAFE_IDENTIFIER_RE,
  Expression,
  AddExpr,
  alias,
  AliasExpr,
  AndExpr,
  AnonymousExpr,
  ArrayExpr,
  ArrayFilterExpr,
  AtTimeZoneExpr,
  BitwiseAndExpr,
  BitwiseRightShiftExpr,
  CastExpr,
  ColumnExpr,
  DataTypeExpr,
  DataTypeExprKind,
  DateAddExpr,
  DateTruncExpr,
  DatetimeAddExpr,
  DPipeExpr,
  EqExpr,
  EscapeExpr,
  ExpressionKey,
  GtExpr,
  GteExpr,
  IdentifierExpr,
  IfExpr,
  IntervalExpr,
  IsExpr,
  JoinExpr,
  JsonExtractExpr,
  JsonPathExpr,
  JsonPathKeyExpr,
  JsonPathRootExpr,
  JsonPathSubscriptExpr,
  JsonPathWildcardExpr,
  LambdaExpr,
  LateralExpr,
  LengthExpr,
  LikeExpr,
  LiteralExpr,
  LowerExpr,
  LtExpr,
  LteExpr,
  NeqExpr,
  NotExpr,
  NullExpr,
  OrderExpr,
  OrExpr,
  ParseJsonExpr,
  QueryExpr,
  ReplaceExpr,
  select,
  SubExpr,
  SubstringExpr,
  TimeAddExpr,
  TimestampAddExpr,
  TimestampFromPartsExpr,
  TimestampTruncExpr,
  TimeToStrExpr,
  ToCharExpr,
  toIdentifier,
  TruncExpr,
  TsOrDsAddExpr,
  UnnestExpr,
  VarExpr,
  WithinGroupExpr,
  cast,
  var_,
  InExpr,
  CoalesceExpr,
  DistinctExpr,
  DataTypeParamExpr,
  StarExpr,
  isType,
  null_,
} from '../expressions';
import { annotateTypes } from '../optimizer/annotate_types';
import type { TokenizerOptions } from '../tokens';
import {
  TokenType, Tokenizer,
} from '../tokens';
import type { ParseOptions } from '../parser';
import { Parser } from '../parser';
import {
  newTrie, type TrieNode,
} from '../trie';
import {
  JsonPathTokenizer, parse as parseJsonPath,
} from '../jsonpath';
import type {
  GeneratorOptions, TranspileOptions,
} from '../generator';
import {
  Generator, unsupportedArgs,
} from '../generator';
import {
  ensureIterable,
  isInt,
  seqGet,
  suggestClosestMatchAndFail, toBool,
} from '../helper';
import {
  formatTime, subsecondPrecision, TIMEZONES,
} from '../time';
import type { ExpressionMetadata } from '../typing';

// Type aliases for common expression type unions
export type DateAddOrDiff =
  | DateAddExpr
  | DateDiffExpr
  | DateSubExpr
  | TsOrDsAddExpr
  | TsOrDsDiffExpr;

export type DateAddOrSub =
  | DateAddExpr
  | TsOrDsAddExpr
  | DateSubExpr;

export type JsonExtractType =
  | JsonExtractExpr
  | JsonExtractScalarExpr
  | JsonbExtractExpr
  | JsonbExtractScalarExpr;

export type DatetimeDelta =
  | DateAddExpr
  | DatetimeAddExpr
  | DatetimeSubExpr
  | TimeAddExpr
  | TimeSubExpr
  | TimestampAddExpr
  | TimestampSubExpr
  | TsOrDsAddExpr;

export const DATETIME_ADD = [
  DateAddExpr,
  TimeAddExpr,
  DatetimeAddExpr,
  TsOrDsAddExpr,
  TimestampAddExpr,
] as const;

// Type aliases for dialect configuration properties
export enum NormalizeFunctions {
  NONE = '',
  UPPER = 'upper',
  LOWER = 'lower',
}

export enum NullOrdering {
  NULLS_ARE_SMALL = 'nulls_are_small',
  NULLS_ARE_LARGE = 'nulls_are_large',
  NULLS_ARE_LAST = 'nulls_are_last',
}

export interface DialectOptions {
  version?: number | string;
  normalizationStrategy?: NormalizationStrategy;
  [index: string]: boolean | string | number | undefined;
}

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

export const PLUGIN_GROUP_NAME = 'sqlglot.dialects';

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
  SINGLESTORE = 'SINGLESTORE',
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
  LOWERCASE = 'lowercase',
  /** Unquoted identifiers are uppercased. */
  UPPERCASE = 'uppercase',
  /** Always case-sensitive, regardless of quotes. */
  CASE_SENSITIVE = 'caseSensitive',
  /** Always case-insensitive (lowercase), regardless of quotes. */
  CASE_INSENSITIVE = 'caseInsensitive',
  /** Always case-insensitive (uppercase), regardless of quotes. */
  CASE_INSENSITIVE_UPPERCASE = 'caseInsensitiveUppercase',
}

export type DialectType = string | Dialect | typeof Dialect;

/**
 * Base dialect class for SQL parsing and generation.
 *
 * Dialect = sqlglot's _Dialect (metaclass) + Dialect
 */
export class Dialect {
  static DIALECT_NAME = Dialects.DIALECT;

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
    return !!this.tokenizerClass.KEYWORDS['(+)'];
  }

  /** Separator of COPY statement parameters. */
  static COPY_PARAMS_ARE_CSV = true;

  /**
   * Determines how function names are going to be normalized.
   * Possible values:
   *   NormalizeFunctions.UPPER or true: Convert names to uppercase.
   *   NormalizeFunctions.LOWER: Convert names to lowercase.
   *   false: Disables function name normalization.
   */
  static NORMALIZE_FUNCTIONS: NormalizeFunctions = NormalizeFunctions.UPPER;

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
   * Possible values: NullOrdering.NULLS_ARE_SMALL, NullOrdering.NULLS_ARE_LARGE, NullOrdering.NULLS_ARE_LAST
   */
  static NULL_ORDERING: NullOrdering = NullOrdering.NULLS_ARE_SMALL;

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
  @cache
  static get TIME_MAPPING (): Record<string, string> { return {}; }

  /**
   * Helper which is used for parsing the special syntax `CAST(x AS DATE FORMAT 'yyyy')`.
   * If empty, the corresponding trie will be constructed off of `TIME_MAPPING`.
   */
  @cache
  static get FORMAT_MAPPING (): Record<string, string> { return {}; }

  static #UNESCAPED_SEQUENCES = new WeakMap<typeof Dialect, Record<string, string>>();

  static get UNESCAPED_SEQUENCES (): Record<string, string> {
    let cached = this.#UNESCAPED_SEQUENCES.get(this);
    if (!cached) {
      if (this.STRINGS_SUPPORT_ESCAPED_SEQUENCES || this.BYTE_STRINGS_SUPPORT_ESCAPED_SEQUENCES) {
        cached = {
          ...BASE_UNESCAPED_SEQUENCES,
          ...this.UNESCAPED_SEQUENCES,
        };
      } else {
        cached = this.UNESCAPED_SEQUENCES;
      }
      this.#UNESCAPED_SEQUENCES.set(this, cached);
    }
    return cached;
  }

  /**
   * Columns that are auto-generated by the engine corresponding to this dialect.
   * For example, such columns may be excluded from `SELECT *` queries.
   */
  @cache
  static get PSEUDOCOLUMNS (): Set<string> { return new Set(); }

  /**
   * Some dialects allow you to reference a CTE column alias in the HAVING clause.
   */
  static PREFER_CTE_ALIAS_COLUMN = false;

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

  @cache
  static get SET_OP_DISTINCT_BY_DEFAULT (): Partial<Record<ExpressionKey, boolean>> {
    return {
      [ExpressionKey.EXCEPT]: true,
      [ExpressionKey.INTERSECT]: true,
      [ExpressionKey.UNION]: true,
    };
  }

  /**
   * Helper for dialects that use a different name for the same creatable kind.
   * For example, the Clickhouse equivalent of CREATE SCHEMA is CREATE DATABASE.
   */
  @cache
  static get CREATABLE_KIND_MAPPING (): Record<string, string> { return {}; }

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

  /**
   * Whether the INITCAP function supports custom delimiter characters as the second argument.
   */
  static get INITCAP_SUPPORTS_CUSTOM_DELIMITERS (): boolean {
    return [
      Dialects.DIALECT,
      Dialects.BIGQUERY,
      Dialects.SNOWFLAKE,
    ].includes(this.DIALECT_NAME);
  }

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
  @cache
  static get DEFAULT_FUNCTIONS_COLUMN_NAMES (): Map<string, string | string[]> { return new Map(); }

  @cache
  static get DEFAULT_NULL_TYPE (): DataTypeExprKind {
    return DataTypeExprKind.UNKNOWN;
  }

  @cache
  static get UNMERGABLE_ARGS (): Set<string> {
    return new Set(
      Array.from(SelectExpr.availableArgs).filter(
        (arg) => ![
          'expressions',
          'from',
          'joins',
          'where',
          'order',
          'hint',
        ].includes(arg),
      ),
    );
  }

  /**
   * Whether LEAST/GREATEST functions ignore NULL values, e.g:
   * - BigQuery, Snowflake, MySQL, Presto/Trino: LEAST(1, NULL, 2) -> NULL
   * - Spark, Postgres, DuckDB, TSQL: LEAST(1, NULL, 2) -> 1
   */
  static LEAST_GREATEST_IGNORES_NULLS = true;

  /** Whether to prioritize non-literal types over literals during type annotation. */
  static PRIORITIZE_NON_LITERAL_TYPES = false;

  static get TRY_SUPPORTED (): boolean {
    const TRY_SUPPORTED_DIALECTS = [
      Dialects.DIALECT,
      Dialects.ATHENA,
      Dialects.PRESTO,
      Dialects.TRINO,
      Dialects.DUCKDB,
    ];

    return TRY_SUPPORTED_DIALECTS.includes(this.DIALECT_NAME);
  }

  /**
   * Whether UESCAPE clause is supported in string literals.
   * Returns false for non-Athena/Presto/Trino/DuckDB dialects.
   */
  static get SUPPORTS_UESCAPE (): boolean {
    return [
      Dialects.DIALECT,
      Dialects.ATHENA,
      Dialects.PRESTO,
      Dialects.TRINO,
      Dialects.DUCKDB,
    ].includes(this.DIALECT_NAME);
  }

  // --- Autofilled by metaclass in Python, set as instance properties in TypeScript ---
  static #tokenizerClass = new WeakMap<typeof Dialect, typeof Tokenizer>();
  static get tokenizerClass (): typeof Tokenizer {
    const cached = this.#tokenizerClass.get(this);
    if (cached) return cached;
    if (Object.prototype.hasOwnProperty.call(this, 'Tokenizer')) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return (this as any).Tokenizer;
    }
    const base = Object.getPrototypeOf(this);
    const baseTokenizer = base?.tokenizerClass as typeof Tokenizer;
    if (!baseTokenizer) {
      this.#tokenizerClass.set(this, Tokenizer);
      return Tokenizer;
    }
    class DerivedTokenizer extends baseTokenizer {}
    this.#tokenizerClass.set(this, DerivedTokenizer);
    return DerivedTokenizer;
  }

  static #jsonpathTokenizerClass = new WeakMap<typeof Dialect, typeof JsonPathTokenizer>();
  static get jsonpathTokenizerClass (): typeof JsonPathTokenizer {
    const cached = this.#jsonpathTokenizerClass.get(this);
    if (cached) return cached;
    if (Object.prototype.hasOwnProperty.call(this, 'JsonPathTokenizer')) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return (this as any).JsonPathTokenizer;
    }
    const base = Object.getPrototypeOf(this);
    const baseJsonpathTokenizer = base?.JsonPathTokenizer as typeof JsonPathTokenizer;
    if (!baseJsonpathTokenizer) {
      this.#jsonpathTokenizerClass.set(this, JsonPathTokenizer);
      return JsonPathTokenizer;
    }
    class DerivedJsonPathTokenizer extends baseJsonpathTokenizer {}
    this.#jsonpathTokenizerClass.set(this, DerivedJsonPathTokenizer);
    return DerivedJsonPathTokenizer;
  }

  static #parserClass = new WeakMap<typeof Dialect, typeof Parser>();

  // NOTE: These logic should be handled in the respective dialect files:
  // - https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/dialects/dialect.py#L317-L380
  static get parserClass (): typeof Parser {
    const cached = this.#parserClass.get(this);
    if (cached) return cached;

    if (Object.prototype.hasOwnProperty.call(this, 'Parser')) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return (this as any).Parser;
    }

    const base = Object.getPrototypeOf(this);
    const baseParser = base?.parserClass as typeof Parser;

    if (!baseParser) {
      this.#parserClass.set(this, Parser);
      return Parser;
    }

    class DerivedParser extends baseParser {}
    this.#parserClass.set(this, DerivedParser);
    return DerivedParser;
  }

  static #generatorClass = new WeakMap<typeof Dialect, typeof Generator>();

  // NOTE: These logic should be handled in the respective dialect files:
  // - https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/dialects/dialect.py#L300-L326
  static get generatorClass (): typeof Generator {
    const cached = this.#generatorClass.get(this);
    if (cached) return cached;

    if (Object.prototype.hasOwnProperty.call(this, 'Generator')) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return (this as any).Generator;
    }

    const base = Object.getPrototypeOf(this);
    const baseGenerator = base?.generatorClass as typeof Generator;

    if (!baseGenerator) {
      this.#generatorClass.set(this, Generator);
      return Generator;
    }

    class DerivedGenerator extends baseGenerator {}
    this.#generatorClass.set(this, DerivedGenerator);
    return DerivedGenerator;
  }

  @cache
  static get TIME_TRIE (): TrieNode {
    return newTrie(Object.keys(this.TIME_MAPPING).map((k) => Array.from(k)));
  }

  @cache
  static get FORMAT_TRIE (): TrieNode {
    const mapping = 0 < Object.keys(this.FORMAT_MAPPING).length
      ? this.FORMAT_MAPPING
      : this.TIME_MAPPING;
    return newTrie(Object.keys(mapping).map((k) => Array.from(k)));
  }

  @cache
  static get INVERSE_TIME_MAPPING (): Record<string, string> {
    return Object.fromEntries(Object.entries(this.TIME_MAPPING).map(([k, v]) => [v, k]));
  }

  @cache
  static get INVERSE_TIME_TRIE (): TrieNode {
    return newTrie(Object.keys(this.INVERSE_TIME_MAPPING).map((k) => Array.from(k)));
  }

  @cache
  static get INVERSE_FORMAT_MAPPING (): Record<string, string> {
    return Object.fromEntries(Object.entries(this.FORMAT_MAPPING).map(([k, v]) => [v, k]));
  }

  @cache
  static get INVERSE_CREATABLE_KIND_MAPPING (): Record<string, string> {
    return Object.fromEntries(Object.entries(this.CREATABLE_KIND_MAPPING).map(([k, v]) => [v, k]));
  }

  @cache
  static get ESCAPED_SEQUENCES (): Record<string, string> {
    return Object.fromEntries(
      Object.entries(this.UNESCAPED_SEQUENCES).map(([k, v]) => [v, k]),
    );
  }

  /** Delimiters for string literals. */
  static get QUOTE_START (): string {
    return Object.entries(this.tokenizerClass._QUOTES)[0]?.[0];
  }

  static get QUOTE_END (): string {
    return Object.entries(this.tokenizerClass._QUOTES)[0]?.[1];
  }

  /** Delimiters for identifiers. */
  static get IDENTIFIER_START (): string {
    return Object.entries(this.tokenizerClass._IDENTIFIERS)[0]?.[0];
  }

  static get IDENTIFIER_END (): string {
    return Object.entries(this.tokenizerClass._IDENTIFIERS)[0]?.[1];
  }

  private static getStartEnd (tokenType: TokenType): [string | undefined, string | undefined] {
    const result = Object.entries(this.tokenizerClass._FORMAT_STRINGS).find(
      ([_, [__, type]]) => type === tokenType,
    );

    if (result) {
      const [start, [end]] = result;
      return [start, end];
    }

    return [undefined, undefined];
  }

  /** Delimiters for bit literals. */
  static get BIT_START (): string | undefined {
    return this.getStartEnd(TokenType.BIT_STRING)[0];
  }

  static get BIT_END (): string | undefined {
    return this.getStartEnd(TokenType.BIT_STRING)[1];
  }

  /** Delimiters for hex literals. */
  static get HEX_START (): string | undefined {
    return this.getStartEnd(TokenType.HEX_STRING)[0];
  }

  static get HEX_END (): string | undefined {
    return this.getStartEnd(TokenType.HEX_STRING)[1];
  }

  /** Delimiters for byte literals. */
  static get BYTE_START (): string | undefined {
    return this.getStartEnd(TokenType.BYTE_STRING)[0];
  }

  static get BYTE_END (): string | undefined {
    return this.getStartEnd(TokenType.BYTE_STRING)[1];
  }

  /** Delimiters for unicode literals. */
  static get UNICODE_START (): string | undefined {
    return this.getStartEnd(TokenType.UNICODE_STRING)[0];
  }

  static get UNICODE_END (): string | undefined {
    return this.getStartEnd(TokenType.UNICODE_STRING)[1];
  }

  /** Date part mapping for normalization. */
  @cache
  static get DATE_PART_MAPPING (): Record<string, string> { return {
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
  }; }

  /** Specifies what types a given type can be coerced into. */
  @cache
  static get COERCES_TO (): Record<string, Set<string>> { return {}; }

  /** Specifies type inference & validation rules for expressions. */
  @cache
  static get EXPRESSION_METADATA (): ExpressionMetadata {
    return new Map();
  }

  /** Determines the supported Dialect instance settings. */
  @cache
  static get SUPPORTED_SETTINGS (): Set<string> { return new Set(['normalizationStrategy', 'version']); }

  /**
   * Extracts quote delimiters from the tokenizer class.
   * Returns [start, end] delimiter pair for string literals.
   */
  @cache
  static get QUOTE_DELIMITERS (): [string, string] {
    return Object.entries(this.tokenizerClass.QUOTES)[0] as [string, string];
  }

  /**
   * Extracts identifier delimiters from the tokenizer class.
   * Returns [start, end] delimiter pair for identifiers.
   */
  @cache
  static get IDENTIFIER_DELIMITERS (): [string, string] {
    return Object.entries(this.tokenizerClass.IDENTIFIERS)[0] as [string, string];
  }

  /** Valid interval units. */
  @cache
  static get BASE_VALID_INTERVAL_UNITS (): Set<string> { return new Set<string>(); } // NOTE: Corresponds to VALID_INTERVAL_UNITS in sqlglot python version

  @cache
  static get VALID_INTERVAL_UNITS (): Set<string> {
    const mapping = this.DATE_PART_MAPPING;
    return new Set([
      ...this.BASE_VALID_INTERVAL_UNITS,
      ...Object.keys(mapping),
      ...Object.values(mapping),
    ]);
  }

  /** Whether strings support escaped sequences. */
  static get STRINGS_SUPPORT_ESCAPED_SEQUENCES (): boolean {
    return this.tokenizerClass.STRING_ESCAPES.includes('\\');
  }

  /** Whether byte strings support escaped sequences. */
  static get BYTE_STRINGS_SUPPORT_ESCAPED_SEQUENCES (): boolean {
    return this.tokenizerClass.BYTE_STRING_ESCAPES.includes('\\');
  }

  /**
   * Compare this dialect class with another value for equality.
   *
   * @param other - Value to compare with
   * @returns true if equal, false otherwise
   */
  static equals (other: unknown): boolean {
    if (this === other) {
      return true;
    }
    if (typeof other === 'string') {
      return this === this.get(other);
    }
    if (other instanceof Dialect) {
      return this === other.constructor;
    }
    return false;
  }

  private static registry: Map<string, typeof Dialect> = new Map();
  // If a subclass wants `Dialect.get` or `Dialect.getOrRaise` to recognize it, it should call this method
  static register (name: string, dialect: typeof Dialect) {
    this.registry.set(name, dialect);
  }

  /**
   * Get a dialect class by name.
   */
  static get (name: string, fallback?: typeof Dialect): typeof Dialect | undefined {
    return this.registry.get(name) || fallback;
  }

  /**
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

    // Is a Dialect class
    if (typeof dialect === 'function' && dialect.prototype instanceof Dialect) {
      return new dialect();
    }

    // Is already a Dialect instance
    if (dialect instanceof Dialect) {
      return dialect;
    }

    // Handle String parsing ("mysql, normalizationStrategy = case_sensitive")
    if (typeof dialect === 'string') {
      let dialectName: string;
      const kwargs: Record<string, string | number | boolean> = {};

      try {
        const [name, ...kvStrings] = dialect.split(',');
        dialectName = name.trim();

        for (const kvString of kvStrings) {
          const parts = kvString.split('=');
          const key = parts[0].trim();
          let value: string | boolean = true;

          if (parts.length === 2) {
            value = parts[1].trim();
          }

          kwargs[key] = toBool(value) ?? false;
        }
      } catch {
        throw new Error(
          `Invalid dialect format: '${dialect}'. `
          + 'Please use the correct format: \'dialect [, k1 = v2 [, ...]]\'.',
        );
      }

      const ResultClass = this.get(dialectName);

      if (!ResultClass) {
        const allDialects = new Set(Object.keys(Dialects));
        suggestClosestMatchAndFail('dialect', dialectName, Array.from(allDialects));
      }

      return new ResultClass(kwargs);
    }

    throw new Error(`Invalid dialect type for '${dialect}': '${typeof dialect}'.`);
  }

  static formatTime (expression?: string | Expression): Expression | undefined {
    if (typeof expression === 'string') {
      return LiteralExpr.string(
        formatTime(
          expression.slice(1, -1),
          this.TIME_MAPPING,
          this.TIME_TRIE,
        ),
      );
    }

    if (expression instanceof Expression && expression.isString) {
      return LiteralExpr.string(
        formatTime(expression.args.this as string, this.TIME_MAPPING, this.TIME_TRIE),
      );
    }

    return expression;
  }

  version: {
    major: number;
    minor: number;
    patch: number;
  };

  normalizationStrategy: NormalizationStrategy;
  settings: Record<string, boolean | number | string | undefined>;

  constructor (options: DialectOptions = {}) {
    const {
      version = Number.MAX_SAFE_INTEGER,
      normalizationStrategy,
    } = options;

    const parts = version.toString().split('.');
    while (parts.length < 3) {
      parts.push('0');
    }
    this.version = {
      major: parseInt(parts[0]),
      minor: parseInt(parts[1]),
      patch: parseInt(parts[2]),
    };

    this.normalizationStrategy = normalizationStrategy ?? this._constructor.NORMALIZATION_STRATEGY;

    this.settings = options;

    for (const unsupportedSetting of Object.keys(options)) {
      if (!this._constructor.SUPPORTED_SETTINGS.has(unsupportedSetting)) {
        suggestClosestMatchAndFail(
          'setting',
          unsupportedSetting,
          [...this._constructor.SUPPORTED_SETTINGS],
        );
      }
    }
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
   * Transforms an identifier in a way that resembles how it'd be resolved by this dialect.
   *
   * For example, an identifier like `FoO` would be resolved as `foo` in Postgres, because it
   * lowercases all unquoted identifiers. On the other hand, Snowflake uppercases them, so
   * it would resolve it as `FOO`. If it was quoted, it'd need to be treated as case-sensitive,
   * and so any normalization would be prohibited in order to avoid "breaking" the identifier.
   *
   * There are also dialects like Spark, which are case-insensitive even when quotes are
   * present, and dialects like MySQL, whose resolution rules match those employed by the
   * underlying operating system, for example they may always be case-sensitive in Linux.
   *
   * Finally, the normalization behavior of some engines can even be controlled through flags,
   * like in Redshift's case, where users can explicitly set enable_case_sensitive_identifier.
   *
   * SQLGlot aims to understand and handle all of these different behaviors gracefully, so
   * that it can analyze queries in the optimizer and successfully capture their semantics.
   */
  normalizeIdentifier<E extends Expression> (expression: E): E {
    if (
      expression instanceof IdentifierExpr
      && this.normalizationStrategy !== NormalizationStrategy.CASE_SENSITIVE
      && (
        !expression.args.quoted
        || this.normalizationStrategy === NormalizationStrategy.CASE_INSENSITIVE
        || this.normalizationStrategy === NormalizationStrategy.CASE_INSENSITIVE_UPPERCASE
      )
    ) {
      const shouldUppercase = (
        this.normalizationStrategy === NormalizationStrategy.UPPERCASE
        || this.normalizationStrategy === NormalizationStrategy.CASE_INSENSITIVE_UPPERCASE
      );
      const thisValue = expression.args.this;
      if (typeof thisValue === 'string') {
        const normalized = shouldUppercase
          ? thisValue.toUpperCase()
          : thisValue.toLowerCase();
        expression.setArgKey('this', normalized);
      }
    }

    return expression;
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
   * Checks if an identifier can be quoted.
   *
   * @param identifier - The identifier to check
   * @param identify - `true`: always quote; `"safe"`: only if case-insensitive; `"unsafe"`: only if case-sensitive
   */
  canQuote (
    identifier: IdentifierExpr,
    options: {
      identify?: string | boolean;
    } = {},
  ): boolean {
    const { identify = 'safe' } = options;

    if (identifier.args.quoted) return true;
    if (!identify) return false;
    if (identify === true) return true;

    const name = identifier.name ?? '';
    const isSafe = !this.caseSensitive(name) && SAFE_IDENTIFIER_RE.test(name);

    if (identify === 'safe') return isSafe;
    if (identify === 'unsafe') return !isSafe;

    throw new Error(`Unexpected argument for identify: '${identify}'`);
  }

  /**
   * Adds quotes to a given expression if it is an identifier.
   *
   * @param expression - The expression of interest
   * @param identify - If `false`, only quote if the identifier is deemed "unsafe"
   */
  quoteIdentifier<E extends Expression> (
    expression: E,
    options: {
      identify?: boolean;
    } = {},
  ): E {
    const { identify = true } = options;

    if (expression instanceof IdentifierExpr) {
      expression.setArgKey('quoted', this.canQuote(expression, { identify: identify || 'unsafe' }));
    }
    return expression;
  }

  toJsonPath (path?: Expression): Expression | undefined {
    if (path instanceof LiteralExpr) {
      let pathText = path.name;

      if (path.isNumber) {
        pathText = `[${pathText}]`;
      }

      try {
        return parseJsonPath(pathText, { dialect: this });
      } catch (e) {
        const isStrict = this._constructor.STRICT_JSON_PATH_SYNTAX;
        const trimmedPath = pathText.trimStart();
        const hasModePrefix = trimmedPath.startsWith('lax') || trimmedPath.startsWith('strict');

        if (isStrict && !hasModePrefix) {
          console.warn(`Invalid JSON path syntax. ${e instanceof Error ? e.message : String(e)}`);
        }
      }
    }

    return path;
  }

  /**
   * Parse SQL string into expression tree.
   */
  parse (sql: string, opts?: ParseOptions): (Expression | undefined)[] {
    return this.parser(opts).parse(this.tokenize(sql));
  }

  parser (opts?: ParseOptions): Parser {
    return new this._constructor.parserClass({
      dialect: this,
      ...opts,
    });
  }

  /**
   * Parse SQL string into specific expression type.
   */
  parseInto<T extends Expression> (
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expressionType: string | (new (args: any) => T) | string[] | (new (args: any) => T)[],
    sql: string,
    opts?: ParseOptions,
  ): (Expression | undefined)[] {
    return this.parser({
      into: expressionType,
      ...opts,
    }).parse(this.tokenize(sql));
  }

  /**
   * Generate SQL from an expression tree.
   */
  generate (expression: Expression, options: GeneratorOptions & { copy?: boolean } = {}): string {
    const {
      copy = true, ...restOptions
    } = options;
    return this.generator(restOptions).generate(expression, { copy });
  }

  /**
   * Get or create a generator instance for this dialect.
   */
  generator (options: GeneratorOptions = {}): Generator {
    return new this._constructor.generatorClass({
      dialect: this,
      ...options,
    });
  }

  /**
   * Tokenize SQL string into tokens.
   */
  tokenize (sql: string, options: TokenizerOptions = {}): ReturnType<Tokenizer['tokenize']> {
    return this.tokenizer(options).tokenize(sql);
  }

  /**
   * Get a tokenizer instance for this dialect.
   */
  tokenizer (options: TokenizerOptions = {}): Tokenizer {
    return new this._constructor.tokenizerClass({
      dialect: this,
      ...options,
    });
  }

  /**
   * Get a jsonpath tokenizer instance for this dialect.
   */
  jsonpathTokenizer (options: TokenizerOptions = {}): Tokenizer {
    return new this._constructor.jsonpathTokenizerClass({
      dialect: this,
      ...options,
    });
  }

  /**
   * Parse SQL and generate it back in this dialect.
   */
  transpile (sql: string, options: TranspileOptions = {}): string[] {
    return this.parse(sql, options).map((expression) =>
      expression
        ? this.generate(expression, {
          copy: false,
          ...options,
        })
        : '');
  }

  /**
   * Generate column aliases for a VALUES expression.
   * By default, generates _col_0, _col_1, etc.
   */
  generateValuesAliases (expression: Expression): IdentifierExpr[] {
    const firstRow = expression.args.expressions?.[0];
    if (!(firstRow instanceof Expression)) {
      return [];
    }

    const firstRowExpr = firstRow as Expression;
    return (firstRowExpr.args.expressions ?? []).map((_: unknown, i: number) => toIdentifier(`_col_${i}`));
  }

  get _constructor (): typeof Dialect {
    return this.constructor as typeof Dialect;
  }
}

// Register the base Dialect
Dialect.register(Dialects.DIALECT, Dialect);

/**
 * Creates a function that renames a function call.
 */
export function renameFunc (name: string): (this: Generator, expression: Expression) => string {
  return function (this: Generator, expression: Expression): string {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const flatten = (arr: any[]): any[] => arr.reduce((acc, val) =>
      Array.isArray(val) ? acc.concat(flatten(val)) : acc.concat(val), []);
    return this.func(name, flatten(Object.values(expression.args)));
  };
}

/**
 * Generate APPROX_COUNT_DISTINCT SQL (with unsupported accuracy parameter).
 */
export function approxCountDistinctSql (this: Generator, expression: ApproxDistinctExpr): string {
  unsupportedArgs.call(this, expression, 'accuracy');
  return this.func('APPROX_COUNT_DISTINCT', [expression.args.this]);
}

/**
 * Creates an IF function generator with custom name and optional false value.
 */
export function ifSql (
  name: string = 'IF',
  falseValue?: Expression | string,
): (this: Generator, expression: IfExpr) => string {
  return function (this: Generator, expression: IfExpr): string {
    return this.func(
      name,
      [
        expression.args.this,
        expression.args.true,
        expression.args.false || falseValue,
      ],
    );
  };
}

/**
 * Generate arrow-based JSON extract (-> or ->>).
 */
export function arrowJsonExtractSql (this: Generator, expression: JsonExtractType): string {
  const thisArg = expression.args.this;

  if (
    this._constructor.JSON_TYPE_REQUIRED_FOR_EXTRACTION
    && thisArg instanceof LiteralExpr
    && thisArg.isString
  ) {
    const jsonType = new DataTypeExpr({ this: DataTypeExprKind.JSON });
    thisArg.replace(cast(thisArg.copy(), jsonType));
  }

  const operator = expression instanceof JsonExtractExpr ? '->' : '->>';
  return this.binary(expression, operator);
}

/**
 * Generate inline array syntax: [elem1, elem2, ...]
 */
export function inlineArraySql (this: Generator, expression: Expression): string {
  return `[${this.expressions(expression, {
    dynamic: true,
    newLine: true,
    skipFirst: true,
    skipLast: true,
  })}]`;
}

/**
 * Generate inline array unless it contains a query.
 */
export function inlineArrayUnlessQuery (this: Generator, expression: Expression): string {
  const elem = seqGet(expression.args.expressions ?? [], 0);
  if (elem instanceof Expression && elem?.find?.(QueryExpr)) {
    return this.func('ARRAY', [elem]);
  }
  return inlineArraySql.call(this, expression);
}

/**
 * Transpile ILIKE to LIKE with LOWER().
 */
export function noIlikeSql (this: Generator, expression: ILikeExpr): string {
  const likeExpr = new LikeExpr({
    this: new LowerExpr({ this: expression.args.this }),
    expression: new LowerExpr({ this: expression.args.expression }),
  });
  return this.likeSql(likeExpr);
}

/**
 * Generate CURRENT_DATE without parentheses.
 */
export function noParenCurrentDateSql (this: Generator, expression: CurrentDateExpr): string {
  const zone = this.sql(expression, 'this');
  return zone ? `CURRENT_DATE AT TIME ZONE ${zone}` : 'CURRENT_DATE';
}

/**
 * Emit unsupported warning for recursive CTEs.
 */
export function noRecursiveCteSql (this: Generator, expression: WithExpr): string {
  if (expression.args.recursive) {
    this.unsupported('Recursive CTEs are unsupported');
    expression.args.recursive = false;
  }
  return this.withSql(expression);
}

/**
 * Emit unsupported warning for TABLESAMPLE.
 */
export function noTablesampleSql (this: Generator, expression: TableSampleExpr): string {
  this.unsupported('TABLESAMPLE unsupported');
  return this.sql(expression.args.this);
}

/**
 * Emit unsupported warning for PIVOT.
 */
export function noPivotSql (this: Generator, _expression: PivotExpr): string {
  this.unsupported('PIVOT unsupported');
  return '';
}

/**
 * Transpile TRY_CAST to CAST.
 */
export function noTrycastSql (this: Generator, expression: Expression): string {
  return this.castSql(expression);
}

/**
 * Emit unsupported warning for comment column constraints.
 */
export function noCommentColumnConstraintSql (this: Generator, _expression: CommentColumnConstraintExpr): string {
  this.unsupported('CommentColumnConstraint unsupported');
  return '';
}

/**
 * Emit unsupported warning for MAP_FROM_ENTRIES.
 */
export function noMapFromEntriesSql (this: Generator, _expression: MapFromEntriesExpr): string {
  this.unsupported('MAP_FROM_ENTRIES unsupported');
  return '';
}

/**
 * Generate property SQL: key=value.
 */
export function propertySql (this: Generator, expression: PropertyExpr): string {
  return `${this.propertyName(expression, { stringKey: true })}=${this.sql(expression, 'value')}`;
}

/**
 * Generate STRPOS/POSITION SQL with optional parameters.
 */
export function strPositionSql (
  this: Generator,
  expression: StrPositionExpr,
  options: {
    funcName?: string;
    supportsPosition?: boolean;
    supportsOccurrence?: boolean;
    useAnsiPosition?: boolean;
  } = {},
): string {
  const {
    funcName = 'STRPOS',
    supportsPosition = false,
    supportsOccurrence = false,
    useAnsiPosition = true,
  } = options;

  let string = expression.args.this;
  const substr = expression.args.substr;
  let position = expression.args.position;
  const occurrence = expression.args.occurrence;
  const zero = LiteralExpr.number(0);
  const one = LiteralExpr.number(1);

  if (supportsOccurrence && occurrence && supportsPosition && !position) {
    position = one;
  }

  const transpilePosition = position && !supportsPosition;
  if (transpilePosition) {
    string = new SubstringExpr({
      this: string,
      start: position,
    });
  }

  let func: Expression;
  if (funcName === 'POSITION' && useAnsiPosition) {
    func = new AnonymousExpr({
      this: funcName,
      expressions: [
        new InExpr({
          this: substr,
          field: string,
        }),
      ],
    });
  } else {
    const args = (funcName === 'LOCATE' || funcName === 'CHARINDEX')
      ? [substr, string]
      : [string, substr];

    if (supportsPosition && position !== undefined) {
      args.push(position);
    }

    if (occurrence) {
      if (supportsOccurrence) {
        args.push(occurrence);
      } else {
        this.unsupported(`${funcName} does not support the occurrence parameter.`);
      }
    }
    func = new AnonymousExpr({
      this: funcName,
      expressions: args.filter((x): x is Expression => x !== undefined),
    });
  }

  if (transpilePosition) {
    const funcWithOffset = new SubExpr({
      this: new AddExpr({
        this: func,
        expression: position,
      }),
      expression: one,
    });

    const funcWrapped = new IfExpr({
      this: func.eq(zero),
      true: zero,
      false: funcWithOffset,
    });

    return this.sql(funcWrapped);
  }

  return this.sql(func);
}

/**
 * Generate struct extract: struct.field.
 */
export function structExtractSql (this: Generator, expression: StructExtractExpr): string {
  return `${this.sql(expression, 'this')}.${this.sql(toIdentifier(expression.args.expression?.name ?? ''))}`;
}

/**
 * Creates array append/prepend function with NULL handling.
 */
export function arrayAppendSql (
  name: string,
  options: {
    swapParams?: boolean;
  } = {},
): (this: Generator, expression: ArrayAppendExpr | ArrayPrependExpr) => string {
  const { swapParams = false } = options;
  return function (this: Generator, expression: ArrayAppendExpr | ArrayPrependExpr): string {
    let thisArg = expression.args.this;
    const element = expression.args.expression;

    let args = swapParams ? [element, thisArg] : [thisArg, element];
    const funcSql = this.func(name, args);

    const sourceNullPropagation = Boolean(expression.args.nullPropagation);
    const targetNullPropagation = this.dialect._constructor.ARRAY_FUNCS_PROPAGATES_NULLS;

    // No transpilation needed when source and target have matching NULL semantics
    if (sourceNullPropagation === targetNullPropagation) {
      return funcSql;
    }

    // Source propagates NULLs, target doesn't: wrap in conditional to return NULL explicitly
    if (sourceNullPropagation) {
      return this.sql(
        new IfExpr({
          this: new IsExpr({
            this: thisArg,
            expression: null_(),
          }),
          true: null_(),
          false: funcSql,
        }),
      );
    }

    // Source doesn't propagate NULLs, target does: use COALESCE to convert NULL to empty array
    const coalesceThis = thisArg ?? new ArrayExpr({ expressions: [] });
    thisArg = new CoalesceExpr({
      expressions: [coalesceThis, new ArrayExpr({ expressions: [] })],
    });

    args = swapParams ? [element, thisArg] : [thisArg, element];
    return this.func(name, args);
  };
}

export function arrayConcatSql (
  name: string,
): (this: Generator, expression: ArrayConcatExpr) => string {
  function buildFuncCall (this: Generator, funcName: string, args: Expression[]): string {
    if (this._constructor.ARRAY_CONCAT_IS_VAR_LEN) {
      return this.func(funcName, args);
    }

    if (args.length === 1) {
      return this.func(funcName, [args[0], new ArrayExpr({ expressions: [] })]);
    }

    // Binary nesting: ARRAY_CAT(a, ARRAY_CAT(b, c))
    let result = this.func(funcName, [args[args.length - 2], args[args.length - 1]]);
    for (let i = args.length - 3; 0 <= i; i--) {
      result = `${funcName}(${this.sql(args[i])}, ${result})`;
    }
    return result;
  }

  return function (this: Generator, expression: ArrayConcatExpr): string {
    const thisArg = expression.args.this;
    const exprs = expression.args.expressions || [];
    const allArgs = [...(thisArg ? [thisArg] : []), ...exprs] as Expression[];

    const sourceNullPropagation = Boolean(expression.args.nullPropagation);
    const targetNullPropagation = this.dialect._constructor.ARRAY_FUNCS_PROPAGATES_NULLS;

    if (
      sourceNullPropagation === targetNullPropagation
      || thisArg instanceof ArrayExpr
      || exprs.length === 0
    ) {
      return buildFuncCall.call(this, name, allArgs);
    }

    if (sourceNullPropagation) {
      // Build OR-chain: a IS NULL OR b IS NULL
      const nullChecks = allArgs.map(
        (arg) => new IsExpr({
          this: arg.copy(),
          expression: null_(),
        }),
      );

      const combinedCheck = nullChecks.reduce(
        (acc, curr) => new OrExpr({
          this: acc,
          expression: curr,
        }),
      );

      return this.sql(
        new IfExpr({
          this: combinedCheck,
          true: null_(),
          false: buildFuncCall.call(this, name, allArgs),
        }),
      );
    }

    // Convert NULL -> empty array
    const wrappedArgs = allArgs.map(
      (arg) => new CoalesceExpr({
        expressions: [arg.copy(), new ArrayExpr({ expressions: [] })],
      }),
    );

    return buildFuncCall.call(this, name, wrappedArgs);
  };
}

export function varMapSql (
  this: Generator,
  expression: MapExpr | VarMapExpr,
  mapFuncName: string = 'MAP',
): string {
  const keys = expression.args.keys;
  const values = expression.args.values;

  if (!(keys instanceof ArrayExpr) || !(values instanceof ArrayExpr)) {
    this.unsupported('Cannot convert array columns into map.');
    return this.func(mapFuncName, [...ensureIterable(keys), ...ensureIterable(values)] as (Expression | string | undefined)[]);
  }

  const args: string[] = [];
  const keyExprs = keys.args.expressions;
  const valueExprs = values.args.expressions;

  for (let i = 0; i < Math.min(keyExprs?.length || 0, valueExprs?.length || 0); i++) {
    args.push(this.sql(keyExprs?.[i]));
    args.push(this.sql(valueExprs?.[i]));
  }

  return this.func(mapFuncName, args);
}

export function monthsBetweenSql (this: Generator, expression: MonthsBetweenExpr): string {
  const date1 = expression.args.this;
  const date2 = expression.args.expression;

  const date1Cast = cast(date1, DataTypeExprKind.DATE);
  const date2Cast = cast(date2, DataTypeExprKind.DATE);

  const wholeMonths = new DateDiffExpr({
    this: date1Cast,
    expression: date2Cast,
    unit: var_('month'),
  });

  const day1 = new DayExpr({ this: date1Cast.copy() });
  const day2 = new DayExpr({ this: date2Cast.copy() });

  const lastDay1 = new LastDayExpr({ this: date1Cast.copy() });
  const lastDay2 = new LastDayExpr({ this: date2Cast.copy() });

  const dayOfLastDay1 = new DayExpr({ this: lastDay1 });
  const dayOfLastDay2 = new DayExpr({ this: lastDay2 });

  const isLastDay1 = new EqExpr({
    this: day1.copy(),
    expression: dayOfLastDay1,
  });
  const isLastDay2 = new EqExpr({
    this: day2.copy(),
    expression: dayOfLastDay2,
  });
  const bothLastDay = new AndExpr({
    this: isLastDay1,
    expression: isLastDay2,
  });

  const fractional = new DivExpr({
    this: new ParenExpr({
      this: new SubExpr({
        this: day1.copy(),
        expression: day2.copy(),
      }),
    }),
    expression: LiteralExpr.number('31.0'),
  });

  const fractionalWithCheck = new IfExpr({
    this: bothLastDay,
    true: LiteralExpr.number('0'),
    false: fractional,
  });

  return this.sql(new AddExpr({
    this: wholeMonths,
    expression: fractionalWithCheck,
  }));
}

export function buildFormattedTime<T extends Expression> (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ExpClass: new (args: any) => T,
  options: {
    dialect: string;
    defaultValue?: boolean | string;
  },
// eslint-disable-next-line @typescript-eslint/no-explicit-any
): (args: any) => T {
  const {
    dialect, defaultValue,
  } = options;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (args: any) => {
    return new ExpClass({
      this: seqGet(args, 0),
      format: Dialect.get(dialect)?.formatTime(
        seqGet(args, 1) || (defaultValue === true ? Dialect.get(dialect)?.TIME_FORMAT : undefined),
      ),
    });
  };
}

export function timeFormat (
  dialect?: string,
): (this: Generator, expression: UnixToStrExpr | StrToUnixExpr) => string | undefined {
  return function (this: Generator, expression: UnixToStrExpr | StrToUnixExpr) {
    const format = this.formatTime(expression);
    return format !== Dialect.getOrRaise(dialect)?._constructor.TIME_FORMAT ? format : undefined;
  };
}

export function buildDateDelta<T extends Expression> (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ExpClass: new (args: any) => T,
  unitMapping?: Record<string, string>,
  options: {
    defaultUnit?: string;
    supportsTimezone?: boolean;
  } = {},
// eslint-disable-next-line @typescript-eslint/no-explicit-any
): (args: any) => T {
  const {
    defaultUnit = 'DAY',
    supportsTimezone = false,
  } = options;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (args: any) => {
    const unitBased = 3 <= args.length;
    const hasTimezone = args.length === 4;
    const thisArg = unitBased ? args[2] : seqGet(args, 0);
    let unit = undefined;

    if (unitBased || defaultUnit) {
      unit = unitBased ? args[0] : LiteralExpr.string(defaultUnit);
      const unitName = unit.name?.toLowerCase();
      if (unitMapping && unitName && unitMapping[unitName]) {
        unit = var_(unitMapping[unitName]);
      }
    }

    const expression = new ExpClass({
      this: thisArg,
      expression: seqGet(args, 1),
      unit,
    });
    if (supportsTimezone && hasTimezone) {
      expression.setArgKey('zone', args[args.length - 1]);
    }
    return expression;
  };
}

export function buildDateDeltaWithInterval<T extends Expression> (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ExpClass: new (args: any) => T,
// eslint-disable-next-line @typescript-eslint/no-explicit-any
): (args: any) => T {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (args: any) => {
    if (args.length < 2) throw new Error(`Expected at least 2 arguments but got ${args.length}`);
    const interval = args[1];

    if (!(interval instanceof IntervalExpr)) {
      throw new Error(`INTERVAL expression expected but got '${interval}'`);
    }

    return new ExpClass({
      this: args[0],
      expression: interval.args.this,
      unit: unitToStr(interval),
    });
  };
}

export function dateTruncToTime (args: (Expression | undefined)[]): DateTruncExpr | TimestampTruncExpr {
  const unit = seqGet(args, 0);
  const thisExpr = seqGet(args, 1);
  if (thisExpr instanceof CastExpr && thisExpr.isType('date')) {
    return new DateTruncExpr({
      unit: unit,
      this: thisExpr,
    });
  }
  return new TimestampTruncExpr({
    this: thisExpr,
    unit: unit,
  });
}

export function dateAddIntervalSql (
  dataType: string,
  kind: string,
): (this: Generator, expression: TimeUnitExpr) => string {
  return function (this: Generator, expression: TimeUnitExpr) {
    const thisArg = this.sql(expression, 'this');
    const interval = new IntervalExpr({
      this: expression.args.expression,
      unit: unitToVar(expression),
    });
    return `${dataType}_${kind}(${thisArg}, ${this.sql(interval)})`;
  };
}

export function timestampTruncSql (
  options: {
    func?: string;
    zone?: boolean;
  } = {},
): (this: Generator, expression: TimestampTruncExpr) => string {
  const {
    func = 'DATE_TRUNC', zone = false,
  } = options;
  return function (this: Generator, expression: TimestampTruncExpr): string {
    const args = [unitToStr(expression), expression.args.this];
    if (zone) {
      args.push(expression.args.zone);
    }
    return this.func(func, args);
  };
}

export function noTimestampSql (this: Generator, expression: TimestampExpr): string {
  const zone = expression.args.zone;
  if (!zone) {
    const annotated = annotateTypes(expression, { dialect: this.dialect }).type;
    const targetType: DataTypeExpr | DataTypeExprKind = isInstanceOf(annotated, DataTypeExpr) ? annotated : DataTypeExprKind.TIMESTAMP;
    return this.sql(cast(expression.args.this || '', targetType));
  }

  if (TIMEZONES.has(zone.name?.toLowerCase())) {
    return this.sql(
      new AtTimeZoneExpr({
        this: cast(expression.args.this || '', DataTypeExprKind.TIMESTAMP),
        zone: zone,
      }),
    );
  }
  return this.func('TIMESTAMP', [expression.args.this, zone]);
}

export function noTimeSql (this: Generator, expression: TimeExpr): string {
  const thisArg = cast(expression.args.this || '', DataTypeExprKind.TIMESTAMPTZ);
  const expr = cast(
    new AtTimeZoneExpr({
      this: thisArg,
      zone: expression.args.zone,
    }),
    DataTypeExprKind.TIME,
  );
  return this.sql(expr);
}

export function noDatetimeSql (this: Generator, expression: DatetimeExpr): string {
  const thisArg = expression.args.this;
  const expr = expression.args.expression;

  if (expr && TIMEZONES.has(expr.name?.toLowerCase() || '')) {
    const tsTz = cast(thisArg, DataTypeExprKind.TIMESTAMPTZ);
    const ts = cast(new AtTimeZoneExpr({
      this: tsTz,
      zone: expr,
    }), DataTypeExprKind.TIMESTAMP);
    return this.sql(ts);
  }

  const date = cast(thisArg, DataTypeExprKind.DATE);
  const time = cast(expr || '', DataTypeExprKind.TIME);

  return this.sql(cast(new AddExpr({
    this: date,
    expression: time,
  }), DataTypeExprKind.TIMESTAMP));
}

export function leftToSubstringSql (this: Generator, expression: LeftExpr): string {
  return this.sql(
    new SubstringExpr({
      this: expression.args.this,
      start: LiteralExpr.number(1),
      length: expression.args.expression,
    }),
  );
}

export function rightToSubstringSql (this: Generator, expression: RightExpr): string {
  return this.sql(
    new SubstringExpr({
      this: expression.args.this,
      start: new SubExpr({
        this: new LengthExpr({ this: expression.args.this }),
        expression: new ParenExpr({
          this: new SubExpr({
            this: expression.args.expression,
            expression: LiteralExpr.number(1),
          }),
        }),
      }),
    }),
  );
}

export function timeStrToTimeSql (
  this: Generator,
  expression: TimeStrToTimeExpr,
  options: {
    includePrecision?: boolean;
  } = {},
): string {
  const { includePrecision = false } = options;

  let datatype = DataTypeExpr.build(
    expression.args.zone ? DataTypeExprKind.TIMESTAMPTZ : DataTypeExprKind.TIMESTAMP,
  );

  if (expression.args.this instanceof LiteralExpr && includePrecision) {
    const precision = subsecondPrecision(expression.args.this.name);
    if (0 < precision && datatype) {
      datatype = DataTypeExpr.build(
        datatype.args.this,
        {
          expressions: new DataTypeParamExpr({
            this: LiteralExpr.number(precision),
          }),
        },
      );
    }
  }

  return this.sql(cast(expression.args.this, datatype, { dialect: this.dialect }));
}

export function dateStrToDateSql (this: Generator, expression: DateStrToDateExpr): string {
  const thisArg = expression.args.this;
  const castArg: string | Expression = (thisArg instanceof Expression || typeof thisArg === 'string') ? thisArg : '';
  return this.sql(cast(castArg, DataTypeExprKind.DATE));
}

export function encodeDecodeSql (
  this: Generator,
  expression: EncodeExpr | DecodeExpr,
  name: string,
  options: { replace?: boolean } = {},
): string {
  const {
    replace = true,
  } = options;
  const charset = expression.args.charset;
  if (charset && !['utf-8', 'utf8'].includes(charset.name?.toLowerCase())) {
    this.unsupported(`Expected utf-8 character set, got ${charset}.`);
  }

  return this.func(name, [expression.args.this, replace ? expression.getArgKey('replace') as Expression | undefined : undefined]);
}

export function minOrLeast (this: Generator, expression: MinExpr): string {
  const name = 0 < (expression.args.expressions?.length ?? 0) ? 'LEAST' : 'MIN';
  return renameFunc(name).call(this, expression);
}

export function maxOrGreatest (this: Generator, expression: MaxExpr): string {
  const name = 0 < (expression.args.expressions?.length ?? 0) ? 'GREATEST' : 'MAX';
  return renameFunc(name).call(this, expression);
}

export function countIfToSum (this: Generator, expression: CountIfExpr): string {
  let cond: ExpressionValue | undefined = expression.args.this;

  if (cond instanceof DistinctExpr) {
    cond = cond.args.expressions?.[0];
    this.unsupported('DISTINCT is not supported when converting COUNT_IF to SUM');
  }

  return this.func('sum', cond
    ? [
      new IfExpr({
        this: cond,
        true: LiteralExpr.number(1),
        false: LiteralExpr.number(0),
      }),
    ]
    : []);
}

export function trimSql (this: Generator, expression: TrimExpr, options: { defaultTrimType?: string } = {}): string {
  const { defaultTrimType = '' } = options;

  const target = this.sql(expression, 'this');
  const trimType = this.sql(expression, 'position') || defaultTrimType;
  const removeChars = this.sql(expression, 'expression');
  const collation = this.sql(expression, 'collation');

  if (!removeChars) {
    return this.trimSql(expression);
  }

  const typePart = trimType ? `${trimType} ` : '';
  const charPart = removeChars ? `${removeChars} ` : '';
  const fromPart = typePart || charPart ? 'FROM ' : '';
  const collPart = collation ? ` COLLATE ${collation}` : '';

  return `TRIM(${typePart}${charPart}${fromPart}${target}${collPart})`;
}

export function strToTimeSql (this: Generator, expression: StrToTimeExpr): string {
  return this.func('STRPTIME', [expression.args.this, this.formatTime(expression)]);
}

export function concatToDPipeSql (this: Generator, expression: ConcatExpr): string {
  return this.sql(
    (expression.args.expressions ?? []).reduce((acc, curr) => new DPipeExpr({
      this: acc,
      expression: curr,
    })),
  );
}

export function concatWsToDPipeSql (this: Generator, expression: ConcatWsExpr): string {
  const [delim, ...rest] = expression.args.expressions ?? [];
  return this.sql(
    rest.reduce((acc, curr) =>
      new DPipeExpr({
        this: acc,
        expression: new DPipeExpr({
          this: delim,
          expression: curr,
        }),
      })),
  );
}

export function regexpExtractSql (
  this: Generator,
  expression: RegexpExtractExpr | RegexpExtractAllExpr,
): string {
  let group = expression.args.group;

  if (group && group.name === String(this.dialect._constructor.REGEXP_EXTRACT_DEFAULT_GROUP)) {
    group = undefined;
  }

  return this.func((expression._constructor as typeof FuncExpr).sqlName(), [
    expression.args.this,
    expression.args.expression,
    group,
  ]);
}

export function regexpReplaceSql (this: Generator, expression: RegexpReplaceExpr): string {
  return this.func('REGEXP_REPLACE', [
    expression.args.this,
    expression.args.expression,
    expression.args.replacement,
  ]);
}
export function pivotColumnNames (aggregations: Expression[], options: { dialect: DialectType }): string[] {
  const { dialect } = options;
  const names: string[] = [];
  for (const agg of aggregations) {
    if (agg instanceof AliasExpr) {
      names.push(agg.alias);
    } else {
      const aggAllUnquoted = agg.transform((node) => {
        if (node instanceof IdentifierExpr) {
          return new IdentifierExpr({
            this: node.name,
            quoted: false,
          });
        }
        return node;
      });
      names.push(aggAllUnquoted.sql({
        dialect,
        normalizeFunctions: NormalizeFunctions.LOWER,
      }));
    }
  }
  return names;
}

export function binaryFromFunction<T extends Expression> (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ExprType: new (args: any) => T,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
): (args: any[]) => T {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (args: any[]) => new ExprType({
    this: seqGet(args, 0),
    expression: seqGet(args, 1),
  });
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function buildTimestampTrunc (args: any[]): TimestampTruncExpr {
  return new TimestampTruncExpr({
    this: seqGet(args, 1),
    unit: seqGet(args, 0),
  });
}

export function buildTrunc (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  args: any[],
  options: {
    dialect: DialectType;
    dateTruncUnabbreviate?: boolean;
    defaultDateTruncUnit?: string;
    dateTruncRequiresPart?: boolean;
  },
): DateTruncExpr | TruncExpr | AnonymousExpr {
  const {
    dialect, dateTruncUnabbreviate = true, defaultDateTruncUnit, dateTruncRequiresPart = true,
  } = options;
  let thisArg = seqGet(args, 0);
  let second = seqGet(args, 1);

  if (thisArg && !thisArg.type) {
    thisArg = annotateTypes(thisArg, { dialect });
  }
  if (second && !second.type) {
    second = annotateTypes(second, { dialect });
  }

  const isTemporal = thisArg?.isType(...DataTypeExpr.TEMPORAL_TYPES);
  const isText = second?.isType(...DataTypeExpr.TEXT_TYPES);

  if ((isTemporal && (second || defaultDateTruncUnit)) || isText) {
    const unit = second || LiteralExpr.string(defaultDateTruncUnit);
    return new DateTruncExpr({
      this: thisArg,
      unit: unit,
      unabbreviate: dateTruncUnabbreviate,
    });
  }

  const isNumeric = thisArg?.isType(...DataTypeExpr.NUMERIC_TYPES) || second?.isType(...DataTypeExpr.NUMERIC_TYPES);
  if (isNumeric || (!dateTruncRequiresPart && !second)) {
    return new TruncExpr({
      this: thisArg,
      decimals: second,
    });
  }

  return new AnonymousExpr({
    this: 'TRUNC',
    expressions: args,
  });
}

export function anyValueToMaxSql (this: Generator, expression: AnyValueExpr): string {
  return this.func('MAX', [expression.args.this]);
}

export function boolXorSql (this: Generator, expression: XorExpr): string {
  const a = this.sql(expression.left);
  const b = this.sql(expression.right);
  return `(${a} AND (NOT ${b})) OR ((NOT ${a}) AND {b})`;
}

export function isParseJson (expression: unknown): boolean {
  return expression instanceof ParseJsonExpr || (expression instanceof CastExpr && expression.isType('json'));
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function isnullToIsNull (args: any[]): Expression {
  return new ParenExpr({
    this: new IsExpr({
      this: seqGet(args, 0),
      expression: null_(),
    }),
  });
}

export function generatedAsIdentityColumnConstraintSql (
  this: Generator,
  expression: GeneratedAsIdentityColumnConstraintExpr,
): string {
  const start = this.sql(expression, 'start') || '1';
  const increment = this.sql(expression, 'increment') || '1';
  return `IDENTITY(${start}, ${increment})`;
}

export function argMaxOrMinNoCount (name: string): (this: Generator, expression: ArgMaxExpr | ArgMinExpr) => string {
  return function (this: Generator, expression: ArgMaxExpr | ArgMinExpr): string {
    return this.func(name, [expression.args.this, expression.args.expression]);
  };
}

export function tsOrDsAddCast (expression: TsOrDsAddExpr): TsOrDsAddExpr {
  if (!expression.args.this) return expression;
  let thisArg = expression.args.this.copy();
  const returnType = expression.returnType;
  assertIsInstanceOf(returnType, DataTypeExpr);

  if (returnType.isType(DataTypeExprKind.DATE)) {
    thisArg = cast(thisArg, DataTypeExprKind.TIMESTAMP);
  }

  expression.args.this?.replace(cast(thisArg, returnType));
  return expression;
}

export function dateDeltaSql (
  name: string,
  options: {
    cast?: boolean;
  } = {},
): (this: Generator, expression: DateAddExpr | DateDiffExpr | DateSubExpr | TsOrDsAddExpr | TsOrDsDiffExpr) => string {
  const { cast = false } = options;
  return function (this: Generator, expression): string {
    let expr = expression;
    if (cast && expression instanceof TsOrDsAddExpr) {
      expr = tsOrDsAddCast(expression);
    }

    return this.func(name, [
      unitToVar(expr),
      expr.args.expression,
      expr.args.this,
    ]);
  };
}

export function dateDeltaToBinaryIntervalOp (
  options: {
    cast?: boolean;
  } = {},
): (this: Generator, expression: DatetimeAddExpr | DatetimeSubExpr) => string {
  const { cast: shouldCast = true } = options;
  return function (this: Generator, expression: DatetimeAddExpr | DatetimeSubExpr): string {
    let thisArg = expression.args.this;
    const unit = unitToVar(expression);
    const op = expression instanceof DatetimeAddExpr ? '+' : '-';

    let toType: DataTypeExpr | DataTypeExprKind | undefined = undefined;
    if (shouldCast) {
      if (expression instanceof TsOrDsAddExpr) {
        const rt = expression.returnType;
        assertIsInstanceOf(rt, DataTypeExpr);
        toType = rt;
      } else if (thisArg?.isString) {
        toType = (expression instanceof DatetimeAddExpr || expression instanceof DatetimeSubExpr)
          ? DataTypeExprKind.DATETIME
          : DataTypeExprKind.DATE;
      }
    }

    thisArg = toType ? cast(thisArg, toType) : thisArg;
    const expr = expression.args.expression;
    const interval = expr instanceof IntervalExpr
      ? expr
      : new IntervalExpr({
        this: expr,
        unit,
      });

    return `${this.sql(thisArg)} ${op} ${this.sql(interval)}`;
  };
}

export function unitToStr (expression: TsOrDsDiffExpr | DateAddOrSub | DateTruncExpr | TimeUnitExpr, options: {
  defaultValue?: string;
} = {}): Expression | undefined {
  const { defaultValue = 'DAY' } = options;
  const unit = expression.args.unit;
  if (!unit) {
    return defaultValue ? LiteralExpr.string(defaultValue) : undefined;
  }

  if (unit instanceof PlaceholderExpr || !(unit instanceof VarExpr || unit instanceof LiteralExpr)) {
    return unit;
  }

  return LiteralExpr.string(unit.name);
}

export function unitToVar (expression: TimeUnitExpr, options: {
  defaultValue?: string;
} = {}): Expression | undefined {
  const { defaultValue = 'DAY' } = options;
  const unit = expression.args.unit;

  if (unit instanceof VarExpr || unit instanceof PlaceholderExpr || unit instanceof ColumnExpr) {
    return unit;
  }

  const value = unit?.name || defaultValue;
  return value ? var_(value) : undefined;
}

export function mapDatePart (part: string | Expression | undefined, options: { dialect?: DialectType } = {}): Expression | undefined {
  const partName = part instanceof Expression ? part.name : part || '';
  const { dialect } = options;
  const mapped = (part && !(part instanceof ColumnExpr && part.parts.length !== 1))
    ? Dialect.getOrRaise(dialect)._constructor.DATE_PART_MAPPING?.[partName.toUpperCase()]
    : undefined;

  if (mapped) {
    if (typeof part === 'string') {
      return LiteralExpr.string(mapped);
    }

    return part?.isString ? LiteralExpr.string(mapped) : var_(mapped);
  }

  if (typeof part === 'string') {
    return LiteralExpr.string(part);
  }
  return part;
}

export function noLastDaySql (this: Generator, expression: LastDayExpr): string {
  const truncCurrDate = new AnonymousExpr({
    this: 'date_trunc',
    expressions: [LiteralExpr.string('month'), ...(expression.args.this ? [expression.args.this] : [])],
  });
  const plusOneMonth = new AnonymousExpr({
    this: 'date_add',
    expressions: [
      truncCurrDate,
      LiteralExpr.number(1),
      LiteralExpr.string('month'),
    ],
  });
  const minusOneDay = new AnonymousExpr({
    this: 'date_sub',
    expressions: [
      plusOneMonth,
      LiteralExpr.number(1),
      LiteralExpr.string('day'),
    ],
  });

  return this.sql(cast(minusOneDay, DataTypeExprKind.DATE));
}

export function mergeWithoutTargetSql (this: Generator, expression: MergeExpr): string {
  const alias = expression.args.this?.args.alias;
  const normalize = (id: Expression | undefined) => id ? this.dialect.normalizeIdentifier(id).name : undefined;

  const thisThis = expression.args.this?.args.this;
  const targets = new Set([normalize(isInstanceOf(thisThis, Expression) ? thisThis : undefined)]);
  if (isInstanceOf(alias, Expression)) targets.add(typeof alias.args.this === 'string' ? alias.args.this : normalize(isInstanceOf(alias.args.this, Expression) ? alias.args.this : undefined));

  for (const when of expression.args.whens?.args.expressions || []) {
    if (!isInstanceOf(when, WhenExpr)) continue;
    const then = when.args.then;
    if (then instanceof UpdateExpr) {
      for (const equals of then.findAll(EqExpr)) {
        const lhs = equals.args.this;
        if (lhs instanceof ColumnExpr && targets.has(normalize(typeof lhs.args.table === 'string' ? toIdentifier(lhs.args.table) : lhs.args.table))) {
          lhs.replace(new ColumnExpr({ this: lhs.args.this }));
        }
      }
    } else if (then instanceof InsertExpr) {
      const columnList = then.args.this;
      if (columnList instanceof TupleExpr) {
        for (const colVal of columnList.args.expressions || []) {
          if (!(colVal instanceof Expression)) continue;
          const col = colVal;
          const tableArg = col.getArgKey('table');
          if (
            (col.args.this instanceof IdentifierExpr || col.args.this instanceof StarExpr)
            && tableArg
            && targets.has(tableArg instanceof Expression ? normalize(tableArg) : tableArg.toString())) {
            col.replace(new ColumnExpr({ this: col.args.this }));
          }
        }
      }
    }
  }

  return this.mergeSql(expression);
}

export function buildJsonExtractPath<T extends JsonExtractExpr | JsonExtractScalarExpr | JsonbExtractExpr | JsonbExtractScalarExpr> (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ExprType: (typeof JsonExtractExpr | typeof JsonExtractScalarExpr | typeof JsonbExtractExpr | typeof JsonbExtractScalarExpr) & (new (arg: any) => T),
  options: {
    zeroBasedIndexing?: boolean;
    arrowReqJsonType?: boolean;
    jsonType?: string;
  } = {},
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
): (args: any) => T {
  const {
    zeroBasedIndexing = true, arrowReqJsonType = false, jsonType,
  } = options;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (args: any) => {
    const segments: JsonPathPartExpr[] = [new JsonPathRootExpr({})];
    for (const arg of args.slice(1)) {
      if (!(arg instanceof LiteralExpr)) {
        return ExprType.fromArgList(args);
      }

      const text = arg.name;
      if (isInt(text) && (!arrowReqJsonType || !arg.isString)) {
        const index = parseInt(text);
        segments.push(new JsonPathSubscriptExpr({ this: zeroBasedIndexing ? index : index - 1 }));
      } else {
        segments.push(new JsonPathKeyExpr({ this: text }));
      }
    }

    args.splice(2);
    const kwargs: JsonExtractExprArgs & Record<string, unknown> = {
      this: seqGet(args, 0),
      expression: new JsonPathExpr({ expressions: segments }),
    };

    if (!(ExprType.prototype instanceof JsonbExtractExpr)) {
      kwargs.onlyJsonTypes = arrowReqJsonType;
    }
    if (jsonType !== undefined) kwargs.jsonType = jsonType;

    return new ExprType(kwargs) as T;
  };
}

export function jsonExtractSegments (
  name: string,
  options: {
    quotedIndex?: boolean;
    op?: string;
  } = {},
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
): (this: Generator, expression: any) => string {
  const {
    quotedIndex = true, op,
  } = options;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return function (this: Generator, expression: any): string {
    const path = expression.args.expression;
    if (!(path instanceof JsonPathExpr)) {
      return renameFunc(name).call(this, expression);
    }

    const segments: string[] = [];
    for (const segment of path.args.expressions ?? []) {
      let segmentSql = this.sql(segment);
      if (segmentSql) {
        if (segment instanceof JsonPathPartExpr && (quotedIndex || !(segment instanceof JsonPathSubscriptExpr))) {
          if (path.args.escape) segmentSql = this.escapeStr(segmentSql);
          segmentSql = `${this.dialect._constructor.QUOTE_START}${segmentSql}${this.dialect._constructor.QUOTE_END}`;
        }
        segments.push(segmentSql);
      }
    }

    if (op) return [this.sql(expression.args.this), ...segments].join(` ${op} `);
    return this.func(name, [expression.args.this, ...segments]);
  };
}

export function jsonPathKeyOnlyName (this: Generator, expression: JsonPathKeyExpr): string {
  if (expression.args.this instanceof JsonPathWildcardExpr) {
    this.unsupported('Unsupported wildcard in JsonPathKey expression');
  }
  return expression.name;
}

export function filterArrayUsingUnnest (this: Generator, expression: ArrayFilterExpr | ArrayRemoveExpr): string {
  let cond = expression.args.expression;
  let aliasExpr: Expression = LiteralExpr.string('_u');

  if (cond instanceof LambdaExpr && (cond.args.expressions?.length ?? 0) === 1) {
    const firstExpr = cond.args.expressions?.[0];
    if (firstExpr) aliasExpr = firstExpr;
    cond = cond.args.this;
  } else if (expression instanceof ArrayRemoveExpr) {
    cond = new NeqExpr({
      this: aliasExpr,
      expression: expression.args.expression,
    });
  }

  const unnest = new UnnestExpr({ expressions: [...(expression.args.this ? [expression.args.this] : [])] });
  const filtered = select(aliasExpr)
    .from(alias(unnest, undefined, { table: [aliasExpr as string | IdentifierExpr] }))
    .where(cond);
  return this.sql(new ArrayExpr({ expressions: [filtered] }));
}

export function arrayCompactSql (this: Generator, expression: ArrayCompactExpr): string {
  const lambdaId = new IdentifierExpr({
    this: '_u',
    quoted: false,
  });
  const cond = new IsExpr({
    this: lambdaId,
    expression: null_(),
  }).not();
  return this.sql(new ArrayFilterExpr({
    this: expression.args.this,
    expression: new LambdaExpr({
      this: cond,
      expressions: [lambdaId],
    }),
  }));
}

export function removeFromArrayUsingFilter (this: Generator, expression: ArrayRemoveExpr): string {
  const lambdaId = new IdentifierExpr({
    this: '_u',
    quoted: false,
  });
  const cond = new NeqExpr({
    this: lambdaId,
    expression: expression.args.expression,
  });
  const filterSql = this.sql(new ArrayFilterExpr({
    this: expression.args.this,
    expression: new LambdaExpr({
      this: cond,
      expressions: [lambdaId],
    }),
  }));

  if (expression.args.nullPropagation && !this.dialect._constructor.ARRAY_FUNCS_PROPAGATES_NULLS) {
    const val = expression.args.expression;
    if ((val instanceof LiteralExpr && !(val instanceof NullExpr)) || val instanceof ArrayExpr) {
      return filterSql;
    }
    return this.sql(new IfExpr({
      this: new IsExpr({
        this: val,
        expression: null_(),
      }),
      true: null_(),
      false: filterSql,
    }));
  }
  return filterSql;
}

export function toNumberWithNlsParam (this: Generator, expression: ToNumberExpr): string {
  return this.func(
    'TO_NUMBER',
    [
      expression.args.this,
      expression.args.format,
      expression.args.nlsparam,
    ],
  );
}

export function buildDefaultDecimalType (precision?: number, scale?: number): (dtype: DataTypeExpr) => DataTypeExpr {
  return (dtype: DataTypeExpr) => {
    if (0 < (dtype.args.expressions?.length ?? 0) || precision === undefined) return dtype;
    const params = scale !== undefined ? `${precision}, ${scale}` : `${precision}`;
    return DataTypeExpr.build(`DECIMAL(${params})`) ?? dtype;
  };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function buildTimestampFromParts (args: any[]): Expression {
  if (args.length === 2) {
    return new AnonymousExpr({
      this: 'TIMESTAMP_FROM_PARTS',
      expressions: args,
    });
  }
  return TimestampFromPartsExpr.fromArgList(args);
}

export function sha256Sql (this: Generator, expression: Sha2Expr): string {
  return this.func(`SHA${expression.text('length') || '256'}`, [expression.args.this]);
}

export function sha2DigestSql (this: Generator, expression: Sha2DigestExpr): string {
  return this.func(`SHA${expression.text('length') || '256'}`, [expression.args.this]);
}

export function sequenceSql (this: Generator, e: Expression): string {
  const expression = e as GenerateSeriesExpr | GenerateDateArrayExpr;
  let start = expression.args.start;
  let end = expression.args.end;
  const step = expression.args.step;
  const targetType = (start instanceof CastExpr ? start.args.to : (end instanceof CastExpr ? end.args.to : undefined));

  if (start !== undefined && end !== undefined && isType(targetType, ['date', 'timestamp'])) {
    assertIsInstanceOf(targetType, DataTypeExpr);
    if (start instanceof CastExpr && targetType === start.to) end = cast(end, targetType);
    else start = cast(start, targetType);

    if (isInstanceOf(expression, GenerateSeriesExpr) && expression.args.isEndExclusive) {
      const stepVal = step || LiteralExpr.number(1);
      end = new ParenExpr({
        this: new SubExpr({
          this: end,
          expression: stepVal,
        }),
      });
      const seqCall = new AnonymousExpr({
        this: 'SEQUENCE',
        expressions: [
          start,
          end,
          ...(step ? [step] : []),
        ],
      });
      const zero = LiteralExpr.number(0);
      const shouldEmpty = new OrExpr({
        this: new EqExpr({
          this: stepVal.copy(),
          expression: zero.copy(),
        }),
        expression: new OrExpr({
          this: new AndExpr({
            this: new GtExpr({
              this: stepVal.copy(),
              expression: zero.copy(),
            }),
            expression: new GteExpr({
              this: start.copy(),
              expression: end.copy(),
            }),
          }),
          expression: new AndExpr({
            this: new LtExpr({
              this: stepVal.copy(),
              expression: zero.copy(),
            }),
            expression: new LteExpr({
              this: start.copy(),
              expression: end.copy(),
            }),
          }),
        }),
      });
      return this.sql(new IfExpr({
        this: shouldEmpty,
        true: new ArrayExpr({ expressions: [] }),
        false: seqCall,
      }));
    }
  }
  return this.func('SEQUENCE', [
    start,
    end,
    step,
  ]);
}

export function buildLike<T extends Expression> (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ExprType: new (args: any) => T,
  options: {
    notLike?: boolean;
  } = {},
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
): (args: any[]) => Expression {
  const { notLike = false } = options;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (args: any[]) => {
    let likeExpr: Expression = new ExprType({
      this: seqGet(args, 0),
      expression: seqGet(args, 1),
    });
    const escape = seqGet(args, 2);
    if (escape) likeExpr = new EscapeExpr({
      this: likeExpr,
      expression: escape,
    });
    return notLike ? new NotExpr({ this: likeExpr }) : likeExpr;
  };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function buildRegexpExtract<T extends Expression> (ExprType: (typeof RegexpExtractExpr) & (new (args: any) => T)): (args: any[], options: { dialect: Dialect }) => Expression {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (args: any[], { dialect }: { dialect: Dialect }) => {
    const kwargs: RegexpExtractExprArgs = {
      this: seqGet(args, 0),
      expression: seqGet(args, 1),
      group: seqGet(args, 2) || LiteralExpr.number(dialect._constructor.REGEXP_EXTRACT_DEFAULT_GROUP),
      parameters: seqGet(args, 3),
    };
    if (ExprType === RegexpExtractExpr) {
      kwargs.nullIfPosOverflow = dialect._constructor.REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL;
    }
    return new ExprType(kwargs);
  };
}

export function explodeToUnnestSql (this: Generator, expression: LateralExpr): string {
  const thisArg = expression.args.this;
  const aliasExpr = expression.args.alias;
  let crossJoinExpr;

  if (thisArg instanceof PosexplodeExpr && aliasExpr) {
    assertIsInstanceOf(aliasExpr, TableAliasExpr);
    const [pos, ...cols] = aliasExpr.args.columns || [];
    assertIsInstanceOf(pos, IdentifierExpr);
    const validCols = cols.filter((c) => c instanceof Expression) as Expression[];
    const lateralSubquery = select([alias(pos.sub(1), pos), ...validCols])
      .from(new UnnestExpr({
        expressions: [...(thisArg.args.this ? [thisArg.args.this] : [])],
        offset: true,
        alias: new TableAliasExpr({
          this: aliasExpr.args.this as IdentifierExpr | undefined,
          columns: [...validCols, pos] as IdentifierExpr[],
        }),
      }));
    crossJoinExpr = new LateralExpr({ this: lateralSubquery.subquery() });
  }
  if (thisArg instanceof ExplodeExpr) {
    crossJoinExpr = new UnnestExpr({
      expressions: [...(thisArg.args.this ? [thisArg.args.this] : [])],
      alias: aliasExpr,
    });
  }

  if (crossJoinExpr) {
    return this.sql(new JoinExpr({
      this: crossJoinExpr,
      kind: JoinExprKind.CROSS,
    }));
  }

  return this.lateralSql(expression);
}

export function timestampDiffSql (this: Generator, expression: DatetimeDiffExpr | TimestampDiffExpr): string {
  return this.func('TIMESTAMPDIFF', [
    expression.unit,
    expression.args.expression,
    expression.args.this,
  ]);
}

export function noMakeIntervalSql (this: Generator, expression: MakeIntervalExpr, options: {
  sep?: string;
} = {}): string {
  const { sep = ', ' } = options;
  const args = Object.entries(expression.args).map(([unit, val]) => `${val instanceof KwargExpr ? val.args.expression : val} ${unit}`);
  return `INTERVAL '${args.join(sep)}'`;
}

export function lengthOrCharLengthSql (this: Generator, expression: LengthExpr): string {
  return this.func(expression.args.binary ? 'LENGTH' : 'CHAR_LENGTH', [expression.args.this]);
}

export function groupConcatSql (this: Generator, expression: GroupConcatExpr, options: {
  funcName?: string;
  sep?: string;
  withinGroup?: boolean;
  onOverflow?: boolean;
} = {}): string {
  const {
    funcName = 'LISTAGG', sep = ',', withinGroup = true, onOverflow = false,
  } = options;
  let thisArg: Expression | undefined = expression.args.this;
  const separator = this.sql(expression.args.separator || (sep ? LiteralExpr.string(sep) : undefined));
  const overflow = onOverflow && this.sql(expression, 'on_overflow') ? ` ON OVERFLOW ${this.sql(expression, 'on_overflow')}` : '';

  let limit = undefined;
  if (thisArg instanceof LimitExpr) {
    limit = thisArg;
    thisArg = limit.args.this?.pop();
  }

  const order = thisArg?.find(OrderExpr);
  if (order?.args.this) thisArg = order.args.this.pop();

  const formattedArgs = [thisArg, separator ? `${separator}${overflow}` : (overflow || undefined)].filter(Boolean).join(', ');
  let listagg: Expression = new AnonymousExpr({
    this: funcName,
    expressions: [formattedArgs],
  });
  let modifiers = this.sql(limit);

  if (order) {
    if (withinGroup) listagg = new WithinGroupExpr({
      this: listagg,
      expression: order,
    });
    else modifiers = `${this.sql(order)}${modifiers}`;
  }

  if (modifiers) listagg.setArgKey('expressions', [`${formattedArgs}${modifiers}`]);
  return this.sql(listagg);
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function buildTimeToStrOrToChar (args: any[], { dialect }: { dialect: DialectType }): TimeToStrExpr | ToCharExpr {
  if (args.length === 2) {
    const thisArg = args[0];
    if (!thisArg.type) annotateTypes(thisArg, { dialect: dialect });
    if (thisArg.isType(...DataTypeExpr.TEMPORAL_TYPES)) {
      return buildFormattedTime(TimeToStrExpr, {
        dialect: dialect.constructor.name.toLowerCase(),
        defaultValue: true,
      })(args);
    }
  }
  return ToCharExpr.fromArgList(args);
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function buildReplaceWithOptionalReplacement (args: any[]): ReplaceExpr {
  return new ReplaceExpr({
    this: seqGet(args, 0),
    expression: seqGet(args, 1),
    replacement: seqGet(args, 2) || LiteralExpr.string(''),
  });
}

export function regexpReplaceGlobalModifier (expression: RegexpReplaceExpr): Expression | undefined {
  const modifiers = expression.args.modifiers;
  if (!expression.args.singleReplace && (!expression.args.occurrence || (expression.args.occurrence.isInteger && expression.args.occurrence.toValue() === 0))) {
    if (!modifiers || modifiers.isString) {
      return LiteralExpr.string((modifiers?.name || '') + 'g');
    }
  }
  return modifiers;
}

export function getBitSql (this: Generator, expression: GetbitExpr): string {
  const value = expression.args.this;
  const pos = expression.args.expression;

  if (!expression.args.zeroIsMsb && expression.isType([...DataTypeExpr.SIGNED_INTEGER_TYPES, ...DataTypeExpr.UNSIGNED_INTEGER_TYPES])) {
    const shifted = new BitwiseRightShiftExpr({
      this: value,
      expression: pos,
    });
    const masked = new BitwiseAndExpr({
      this: shifted,
      expression: LiteralExpr.number(1),
    });
    return this.sql(masked);
  }
  return this.func('GET_BIT', [value, pos]);
}
