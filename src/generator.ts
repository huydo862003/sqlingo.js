// https://github.com/tobymao/sqlglot/blob/main/sqlglot/generator.py

import {
  cache,
  assertIsInstanceOf, isInstanceOf,
} from './port_internals';
import type {
  AddConstraintExpr,
  AddPartitionExpr,
  AliasesExpr,
  AlterColumnExpr,
  AlterDistStyleExpr,
  AlterExpr,
  AlterIndexExpr,
  AlterSessionExpr,
  AlterSetExpr,
  AlterSortKeyExpr,
  AnalyzeDeleteExpr,
  AnalyzeExpr,
  AnalyzeHistogramExpr,
  AnalyzeListChainedRowsExpr,
  AnalyzeSampleExpr,
  AnalyzeStatisticsExpr,
  AnalyzeValidateExpr,
  AnonymousAggFuncExpr,
  AnonymousExpr,
  AnyValueExpr,
  ApplyExpr,
  ArrayAggExpr,
  ArrayAnyExpr,
  AtIndexExpr,
  AttachExpr,
  AttachOptionExpr,
  AutoIncrementColumnConstraintExpr,
  BetweenExpr,
  BitStringExpr,
  BitwiseAndExpr,
  BitwiseLeftShiftExpr,
  BitwiseNotExpr,
  BitwiseOrExpr,
  BitwiseRightShiftExpr,
  BitwiseXorExpr,
  BoolandExpr,
  BoolorExpr,
  BuildPropertyExpr,
  ByteStringExpr,
  CteExpr,
  CacheExpr,
  ChangesExpr,
  CharacterSetExpr,
  CheckColumnConstraintExpr,
  CheckExpr,
  CloneExpr,
  CollateExpr,
  ColumnConstraintExpr,
  ColumnPositionExpr,
  ColumnPrefixExpr,
  ColumnsExpr,
  CombinedAggFuncExpr,
  CombinedParameterizedAggExpr,
  CommentExpr,
  CommitExpr,
  ComprehensionExpr,
  CompressColumnConstraintExpr,
  ConcatExpr,
  ConditionalInsertExpr,
  ConnectExpr,
  ConstraintExpr,
  ConvertExpr,
  CopyExpr,
  CopyParameterExpr,
  CredentialsExpr,
  CubeExpr,
  DPipeExpr,
  DataTypeParamExpr,
  DateFromUnixDateExpr,
  DeclareExpr,
  DeclareItemExpr,
  DecodeCaseExpr,
  DetachExpr,
  DictSubPropertyExpr,
  DirectoryStageExpr,
  DistanceExpr,
  DistributeExpr,
  DropPartitionExpr,
  EscapeExpr,
  ExistsExpr,
  ExplodingGenerateSeriesExpr,
  ExportExpr,
  ExtractExpr,
  FeaturesAtTimeExpr,
  ForInExpr,
  ForeignKeyExpr,
  FormatJsonExpr,
  FormatPhraseExpr,
  FromTimeZoneExpr,
  GteExpr,
  GtExpr,
  GapFillExpr,
  GenerateEmbeddingExpr,
  GeneratedAsIdentityColumnConstraintExpr,
  GeneratedAsRowColumnConstraintExpr,
  GetExtractExpr,
  GlobExpr,
  GrantExpr,
  GrantPrincipalExpr,
  GrantPrivilegeExpr,
  GroupingSetsExpr,
  HeredocExpr,
  HexExpr,
  HexStringExpr,
  HintExpr,
  IgnoreNullsExpr,
  InExpr,
  InOutColumnConstraintExpr,
  IndexColumnConstraintExpr,
  IndexConstraintOptionExpr,
  IndexExpr,
  IndexParametersExpr,
  InitcapExpr,
  InputOutputFormatExpr,
  InstallExpr,
  IntDivExpr,
  IntroducerExpr,
  JsonArrayAggExpr,
  JsonArrayExpr,
  JsonCastExpr,
  JsonColumnDefExpr,
  JsonExistsExpr,
  JsonExpr,
  JsonExtractQuoteExpr,
  JsonKeyValueExpr,
  JsonObjectAggExpr,
  JsonPathExpr,
  JsonSchemaExpr,
  JsonTableExpr,
  JsonValueExpr,
  JoinHintExpr,
  KwargExpr,
  LteExpr,
  LtExpr,
  LastDayExpr,
  LimitOptionsExpr,
  LoadDataExpr,
  LocaltimeExpr,
  LocaltimestampExpr,
  LockExpr,
  LogExpr,
  LowerHexExpr,
  MlForecastExpr,
  MlTranslateExpr,
  MaskingPolicyColumnConstraintExpr,
  MatchAgainstExpr,
  MatchExpr,
  MatchRecognizeExpr,
  MatchRecognizeMeasureExpr,
  MedianExpr,
  MergeExpr,
  MergeTreeTtlActionExpr,
  ModExpr,
  ModelAttributeExpr,
  NeqExpr,
  NationalExpr,
  NextValueForExpr,
  NotNullColumnConstraintExpr,
  NullSafeEqExpr,
  NullSafeNeqExpr,
  Nvl2Expr,
  ObjectIdentifierExpr,
  OffsetExpr,
  OnClusterExpr,
  OnConditionExpr,
  OnConflictExpr,
  OpclassExpr,
  OpenJsonColumnDefExpr,
  OpenJsonExpr,
  OrderedExpr,
  OverflowTruncateBehaviorExpr,
  OverlapsExpr,
  OverlayExpr,
  PadExpr,
  ParameterExpr,
  ParameterizedAggExpr,
  ParseJsonExpr,
  PartitionByRangePropertyDynamicExpr,
  PartitionByRangePropertyExpr,
  PartitionExpr,
  PartitionRangeExpr,
  PeriodForSystemTimeConstraintExpr,
  PivotAliasExpr,
  PlaceholderExpr,
  PragmaExpr,
  PredictExpr,
  PrimaryKeyColumnConstraintExpr,
  PriorExpr,
  PseudoTypeExpr,
  PseudocolumnExpr,
  QualifyExpr,
  QueryBandExpr,
  QueryExpr,
  QueryOptionExpr,
  QueryTransformExpr,
  RawStringExpr,
  RecursiveWithSearchExpr,
  ReferenceExpr,
  RefreshExpr,
  RenameColumnExpr,
  RespectNullsExpr,
  ReturningExpr,
  RevokeExpr,
  RollbackExpr,
  RollupExpr,
  RollupIndexExpr,
  SafeDivideExpr,
  ScopeResolutionExpr,
  SemanticViewExpr,
  SemicolonExpr,
  SessionParameterExpr,
  SetItemExpr,
  ShowExpr,
  SimilarToExpr,
  SliceExpr,
  SortExpr,
  SpaceExpr,
  StringExpr,
  SummarizeExpr,
  TableFromRowsExpr,
  TagExpr,
  ToArrayExpr,
  ToCharExpr,
  ToDoubleExpr,
  ToNumberExpr,
  TransactionExpr,
  TranslateCharactersExpr,
  TrimExpr,
  TruncateTableExpr,
  TryExpr,
  UncacheExpr,
  UnicodeStringExpr,
  UniqueColumnConstraintExpr,
  UnixDateExpr,
  UnixSecondsExpr,
  UnpivotColumnsExpr,
  UseExpr,
  UserDefinedFunctionExpr,
  UuidExpr,
  ValuesExpr,
  VectorSearchExpr,
  VersionExpr,
  WatermarkColumnConstraintExpr,
  WeekStartExpr,
  WhenExpr,
  WhensExpr,
  WindowSpecExpr,
  WithFillExpr,
  WithinGroupExpr,
  XmlElementExpr,
  XmlKeyValueOptionExpr,
  XmlNamespaceExpr,
  XmlTableExpr,
  KillExpr,
  IndexTableHintExpr,
  HistoricalDataExpr,
  JsonPathKeyExpr,
  JsonPathSubscriptExpr,
  TableSampleExpr,
  UniqueKeyPropertyExpr,
  XorExpr,
  NotExpr,
  EqExpr,
  IsExpr,
  OrExpr,
  AndExpr,
  AddExpr,
  SubExpr,
  MulExpr,
  ExpressionValue,
} from './expressions';
import {
  DistinctExpr,
  FilterExpr,
  JsonExtractExpr,
  ConnectorExpr,
  AlterRenameExpr,
  BracketExpr,
  DateAddExpr,
  RandExpr,
  WindowExpr,
  WithTableHintExpr,
  Expression, LiteralExpr, TableExpr, IntoExpr, literal,
  FuncExpr, PropertyExpr,
  CaseExpr, IfExpr, NullExpr, BooleanExpr,
  CastExpr, DivExpr, DataTypeExpr, DataTypeExprKind,
  JoinExpr, SelectExpr, StarExpr, FromExpr, SubqueryExpr, TableAliasExpr,
  PropertiesExpr, PropertiesLocation,
  ArrayFilterExpr, ArraySizeExpr,
  ParenExpr, LikeExpr, ILikeExpr, AllExpr, AnyExpr, TupleExpr, ConditionExpr,
  BinaryExpr, UnionExpr,
  IdentifierExpr, ColumnExpr,
  alias, toIdentifier, cast, or, and,
  DirectoryExpr, PropertyEqExpr, AtTimeZoneExpr,
  StructExpr,
  TsOrDsToTimeExpr, TsOrDsToTimestampExpr, TsOrDsToDatetimeExpr, TsOrDsToDateExpr,
  StrToTimeExpr, TryCastExpr,
  TimestampDiffExpr,
  ConvertTimezoneExpr,
  GenerateSeriesExpr, UnnestExpr,
  PercentileContExpr,
  JsonPathPartExpr, JsonPathWildcardExpr,
  AliasExpr, VarExpr,
  var_,
  ComputedColumnConstraintExpr,
  NullifExpr,
  CoalesceExpr,
  ReturnExpr,
  LateralExpr,
  FetchExpr,
  LimitExpr,
  PivotExpr,
  ConcatWsExpr,
  DotExpr,
  PartitionBoundSpecExpr,
  IntervalExpr,
  ColumnDefExpr,
  SchemaExpr,
  JsonObjectExpr,
  IntervalSpanExpr,
  HavingMaxExpr,
  OrderExpr,
  PutExpr,
  InsertExpr,
  UpdateExpr,
  AdjacentExpr,
  AllowedValuesPropertyExpr,
  AnalyzeColumnsExpr,
  AnalyzeWithExpr,
  ArrayContainsAllExpr,
  ArrayOverlapsExpr,
  AutoRefreshPropertyExpr,
  BackupPropertyExpr,
  CaseSpecificColumnConstraintExpr,
  CeilExpr,
  CharacterSetColumnConstraintExpr,
  CharacterSetPropertyExpr,
  ClusteredColumnConstraintExpr,
  CollateColumnConstraintExpr,
  CommentColumnConstraintExpr,
  ConnectByRootExpr,
  ConvertToCharsetExpr,
  CopyGrantsPropertyExpr,
  CredentialsPropertyExpr,
  CurrentCatalogExpr,
  CurrentDateExpr,
  CurrentTimeExpr,
  CurrentTimestampExpr,
  DateFormatColumnConstraintExpr,
  DefaultColumnConstraintExpr,
  DynamicPropertyExpr,
  EmptyPropertyExpr,
  EncodeColumnConstraintExpr,
  EnviromentPropertyExpr,
  EphemeralColumnConstraintExpr,
  ExcludeColumnConstraintExpr,
  ExceptExpr,
  ExecuteAsPropertyExpr,
  ExternalPropertyExpr,
  ExtendsLeftExpr,
  ExtendsRightExpr,
  FloorExpr,
  ForcePropertyExpr,
  GetExpr,
  GlobalPropertyExpr,
  HeapPropertyExpr,
  IcebergPropertyExpr,
  InheritsPropertyExpr,
  InlineLengthColumnConstraintExpr,
  InputModelPropertyExpr,
  IntersectExpr,
  Int64Expr,
  JsonbContainsAllTopKeysExpr,
  JsonbContainsAnyTopKeysExpr,
  JsonbDeleteAtPathExpr,
  LanguagePropertyExpr,
  LocationPropertyExpr,
  LogPropertyExpr,
  MaterializedPropertyExpr,
  NetFuncExpr,
  NonClusteredColumnConstraintExpr,
  NoPrimaryIndexPropertyExpr,
  NotForReplicationColumnConstraintExpr,
  OnCommitPropertyExpr,
  OnPropertyExpr,
  OnUpdateColumnConstraintExpr,
  OperatorExpr,
  OutputModelPropertyExpr,
  PartitionByTruncateExpr,
  PartitionedByBucketExpr,
  PathColumnConstraintExpr,
  PivotAnyExpr,
  PositionalColumnExpr,
  ProjectionPolicyColumnConstraintExpr,
  RemoteWithConnectionModelPropertyExpr,
  ReturnsPropertyExpr,
  SafeFuncExpr,
  SamplePropertyExpr,
  SecurePropertyExpr,
  SecurityPropertyExpr,
  SessionUserExpr,
  SetConfigPropertyExpr,
  SetPropertyExpr,
  SettingsPropertyExpr,
  SharingPropertyExpr,
  SqlReadWritePropertyExpr,
  SqlSecurityPropertyExpr,
  StabilityPropertyExpr,
  StreamExpr,
  StreamingTablePropertyExpr,
  StrictPropertyExpr,
  SwapTableExpr,
  TableColumnExpr,
  TagsExpr,
  TemporaryPropertyExpr,
  TitleColumnConstraintExpr,
  ToMapExpr,
  ToTablePropertyExpr,
  TransformModelPropertyExpr,
  TransientPropertyExpr,
  UnloggedPropertyExpr,
  UppercaseColumnConstraintExpr,
  UsingDataExpr,
  UsingTemplatePropertyExpr,
  UtcDateExpr,
  UtcTimeExpr,
  UtcTimestampExpr,
  VariadicExpr,
  VarMapExpr,
  ViewAttributePropertyExpr,
  VolatilePropertyExpr,
  WithJournalTablePropertyExpr,
  WithOperatorExpr,
  WithProcedureOptionsExpr,
  WithSchemaBindingPropertyExpr,
  ZeroFillColumnConstraintExpr,
  AlgorithmPropertyExpr,
  AutoIncrementPropertyExpr,
  BlockCompressionPropertyExpr,
  ChecksumPropertyExpr,
  CollatePropertyExpr,
  ClusterExpr,
  ClusteredByPropertyExpr,
  DistributedByPropertyExpr,
  DuplicateKeyPropertyExpr,
  DataBlocksizePropertyExpr,
  DataDeletionPropertyExpr,
  DefinerPropertyExpr,
  DictRangeExpr,
  DictPropertyExpr,
  DistKeyPropertyExpr,
  DistStylePropertyExpr,
  EncodePropertyExpr,
  EnginePropertyExpr,
  FallbackPropertyExpr,
  FileFormatPropertyExpr,
  FreespacePropertyExpr,
  IncludePropertyExpr,
  IsolatedLoadingPropertyExpr,
  JournalPropertyExpr,
  LikePropertyExpr,
  LockPropertyExpr,
  LockingPropertyExpr,
  MergeBlockRatioPropertyExpr,
  PartitionedByPropertyExpr,
  PartitionedOfPropertyExpr,
  PrimaryKeyExpr,
  RefreshTriggerPropertyExpr,
  RollupPropertyExpr,
  RowFormatPropertyExpr,
  RowFormatDelimitedPropertyExpr,
  RowFormatSerdePropertyExpr,
  SchemaCommentPropertyExpr,
  SerdePropertiesExpr,
  SetExpr,
  SequencePropertiesExpr,
  SortKeyPropertyExpr,
  StorageHandlerPropertyExpr,
  WithSystemVersioningPropertyExpr,
  WithDataPropertyExpr,
  MergeTreeTtlExpr,
  CommandExpr,
  CreateExpr,
  DescribeExpr,
  DeleteExpr,
  DropExpr,
  GroupExpr,
  HavingExpr,
  WhereExpr,
  WithExpr,
  MultitableInsertsExpr,
  SetOperationExpr,
  NegExpr,
  UNWRAPPED_QUERIES,
  subquery,
  true_,
  union,
  JoinExprKind,
  AggFuncExpr,
  maybeCopy,
  null_,
  case_,
  wrap,
  not,
  paren,
  func,
  array,
  StrToDateExpr,
  TimeToStrExpr,
} from './expressions';
import { formatTime } from './time';
import type { ParseOptions } from './parser';
import {
  Dialect, type DialectType, mapDatePart, unitToStr,
  concatToDPipeSql, NullOrdering, NormalizeFunctions,
} from './dialects/dialect';
import {
  ErrorLevel, UnsupportedError, concatMessages,
} from './errors';
import {
  TokenType,
} from './tokens';
import { simplify } from './optimizer/simplify';
import {
  applyIndexOffset, csv, nameSequence,
  seqGet,
} from './helper';
import { ALL_JSON_PATH_PARTS } from './jsonpath/expressions';
import { annotateTypes } from './optimizer/annotate_types';
import type { TrieNode } from './trie';
import { TSQL } from './dialects/tsql';
import {
  ensureBools, moveCtesToTopLevel,
} from './transforms';

export interface GeneratorOptions extends ParseOptions {
  pretty?: boolean;
  identify?: string | boolean;
  normalize?: boolean;
  pad?: number;
  indent?: number;
  normalizeFunctions?: string | boolean;
  unsupportedLevel?: ErrorLevel;
  maxUnsupported?: number;
  leadingComma?: boolean;
  maxTextWidth?: number;
  comments?: boolean;
  dialect?: DialectType;
  [key: string]: unknown;
}

export interface TranspileOptions extends ParseOptions {
  write?: DialectType;
  identity?: boolean;
  errorLevel?: ErrorLevel;
  pretty?: boolean;
  [key: string]: unknown;
}

// Constants
const ESCAPED_UNICODE_RE = /\\(\d+)/g;

export function unsupportedArgs<T extends Expression> (
  this: Generator,
  expression: T,
  ...args: (string | [string, string])[]
): void {
  const expressionName = expression._constructor.name;
  const dialectName = this.dialect._constructor.name;
  for (const arg of args) {
    const [argName, diagnostic] = typeof arg === 'string' ? [arg, undefined] : arg;
    if (expression.getArgKey(argName)) {
      this.unsupported(diagnostic ?? `Argument '${argName}' is not supported for expression '${expressionName}' when targeting ${dialectName}.`);
    }
  }
}

/**
 * Generator converts a given syntax tree to the corresponding SQL string.
 *
 * Args:
 *   pretty: Whether to format the produced SQL string. Default: False.
 *   identify: Determines when an identifier should be quoted. Possible values are:
 *     False (default): Never quote, except in cases where it's mandatory by the dialect.
 *     True: Always quote except for specials cases.
 *     'safe': Only quote identifiers that are case insensitive.
 *   normalize: Whether to normalize identifiers to lowercase. Default: False.
 *   pad: The pad size in a formatted string. Default: 2.
 *   indent: The indentation size in a formatted string. Default: 2.
 *   normalizeFunctions: How to normalize function names. Possible values are:
 *     "upper" or True (default): Convert names to uppercase.
 *     "lower": Convert names to lowercase.
 *     False: Disables function name normalization.
 *   unsupportedLevel: Determines the generator's behavior when it encounters unsupported expressions.
 *     Default ErrorLevel.WARN.
 *   maxUnsupported: Maximum number of unsupported messages to include in a raised UnsupportedError.
 *     This is only relevant if unsupported_level is ErrorLevel.RAISE. Default: 3
 *   leadingComma: Whether the comma is leading or trailing in select expressions.
 *     This is only relevant when generating in pretty mode. Default: False
 *   maxTextWidth: The max number of characters in a segment before creating new lines in pretty mode.
 *     Default: 80
 *   comments: Whether to preserve comments in the output SQL code. Default: True
 */
export class Generator {
  // Static feature flags

  // Whether null ordering is supported in order by
  // True: Full Support, None: No support, False: No support for certain cases
  // such as window specifications, aggregate functions etc
  static NULL_ORDERING_SUPPORTED?: boolean | null = true;

  // Whether ignore nulls is inside the agg or outside.
  // FIRST(x IGNORE NULLS) OVER vs FIRST (x) IGNORE NULLS OVER
  static IGNORE_NULLS_IN_FUNC = false;
  static RESPECT_IGNORE_NULLS_UNSUPPORTED_EXPRESSIONS: (typeof Expression)[] = [];

  // Whether locking reads (i.e. SELECT ... FOR UPDATE/SHARE) are supported
  static LOCKING_READS_SUPPORTED = false;

  // Whether the EXCEPT and INTERSECT operations can return duplicates
  static EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = true;

  // Wrap derived values in parens, usually standard but spark doesn't support it
  static WRAP_DERIVED_VALUES = true;

  // Whether create function uses an AS before the RETURN
  static CREATE_FUNCTION_RETURN_AS = true;

  // Whether MERGE ... WHEN MATCHED BY SOURCE is allowed
  static MATCHED_BY_SOURCE = true;

  // Whether the INTERVAL expression works only with values like '1 day'
  static SINGLE_STRING_INTERVAL = false;

  // Whether the plural form of date parts like day (i.e. "days") is supported in INTERVALs
  static INTERVAL_ALLOWS_PLURAL_FORM = true;

  // Whether limit and fetch are supported (possible values: "ALL", "LIMIT", "FETCH")
  static LIMIT_FETCH = 'ALL';

  // Whether limit and fetch allows expresions or just limits
  static LIMIT_ONLY_LITERALS = false;

  // Whether a table is allowed to be renamed with a db
  static RENAME_TABLE_WITH_DB = true;

  // The separator for grouping sets and rollups
  static GROUPINGS_SEP = ',';

  // The string used for creating an index on a table
  static INDEX_ON = 'ON';

  // Separator for IN/OUT parameter mode (Oracle uses " " for "IN OUT", PostgreSQL uses "" for "INOUT")
  static INOUT_SEPARATOR = ' ';

  // Whether join hints should be generated
  static JOIN_HINTS = true;

  // Whether directed joins are supported
  static DIRECTED_JOINS = false;

  // Whether table hints should be generated
  static TABLE_HINTS = true;

  // Whether query hints should be generated
  static QUERY_HINTS = true;

  // What kind of separator to use for query hints
  static QUERY_HINT_SEP = ', ';

  // Whether comparing against booleans (e.g. x IS TRUE) is supported
  static IS_BOOL_ALLOWED = true;

  // Whether to include the "SET" keyword in the "INSERT ... ON DUPLICATE KEY UPDATE" statement
  static DUPLICATE_KEY_UPDATE_WITH_SET = true;

  // Whether to generate the limit as TOP <value> instead of LIMIT <value>
  static LIMIT_IS_TOP = false;

  // Whether to generate INSERT INTO ... RETURNING or INSERT INTO RETURNING ...
  static RETURNING_END = true;

  // Whether to generate an unquoted value for EXTRACT's date part argument
  static EXTRACT_ALLOWS_QUOTES = true;

  // Whether TIMETZ / TIMESTAMPTZ will be generated using the "WITH TIME ZONE" syntax
  static TZ_TO_WITH_TIME_ZONE = false;

  // Whether the NVL2 function is supported
  static NVL2_SUPPORTED = true;

  // https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
  static SELECT_KINDS: string[] = ['STRUCT', 'VALUE'];

  // Whether VALUES statements can be used as derived tables.
  // MySQL 5 and Redshift do not allow this, so when False, it will convert
  // SELECT * VALUES into SELECT UNION
  static VALUES_AS_TABLE = true;

  // Whether the word COLUMN is included when adding a column with ALTER TABLE
  static ALTER_TABLE_INCLUDE_COLUMN_KEYWORD = true;

  // UNNEST WITH ORDINALITY (presto) instead of UNNEST WITH OFFSET (bigquery)
  static UNNEST_WITH_ORDINALITY = true;

  // Whether FILTER (WHERE cond) can be used for conditional aggregation
  static AGGREGATE_FILTER_SUPPORTED = true;

  // Whether JOIN sides (LEFT, RIGHT) are supported in conjunction with SEMI/ANTI join kinds
  static SEMI_ANTI_JOIN_WITH_SIDE = true;

  // Whether to include the type of a computed column in the CREATE DDL
  static COMPUTED_COLUMN_WITH_TYPE = true;

  // Whether CREATE TABLE .. COPY .. is supported. False means we'll generate CLONE instead of COPY
  static SUPPORTS_TABLE_COPY = true;

  // Whether parentheses are required around the table sample's expression
  static TABLESAMPLE_REQUIRES_PARENS = true;

  // Whether a table sample clause's size needs to be followed by the ROWS keyword
  static TABLESAMPLE_SIZE_IS_ROWS = true;
  static TABLESAMPLE_SIZE_IS_PERCENT = false;

  // The keyword(s) to use when generating a sample clause
  static TABLESAMPLE_KEYWORDS = 'TABLESAMPLE';

  // Whether the TABLESAMPLE clause supports a method name, like BERNOULLI
  static TABLESAMPLE_WITH_METHOD = true;

  // The keyword to use when specifying the seed of a sample clause
  static TABLESAMPLE_SEED_KEYWORD = 'SEED';

  // Whether COLLATE is a function instead of a binary operator
  static COLLATE_IS_FUNC = false;

  // Whether data types support additional specifiers like e.g. CHAR or BYTE (oracle)
  static DATA_TYPE_SPECIFIERS_ALLOWED = false;

  // Whether conditions require booleans WHERE x = 0 vs WHERE x
  static ENSURE_BOOLS = false;

  // Whether the "RECURSIVE" keyword is required when defining recursive CTEs
  static CTE_RECURSIVE_KEYWORD_REQUIRED = true;

  // Whether CONCAT requires >1 arguments
  static SUPPORTS_SINGLE_ARG_CONCAT = true;

  // Whether LAST_DAY function supports a date part argument
  static LAST_DAY_SUPPORTS_DATE_PART = true;

  // Whether named columns are allowed in table aliases
  static SUPPORTS_TABLE_ALIAS_COLUMNS = true;

  // Whether UNPIVOT aliases are Identifiers (False means they're Literals)
  static UNPIVOT_ALIASES_ARE_IDENTIFIERS = true;

  // What delimiter to use for separating JSON key/value pairs
  static JSON_KEY_VALUE_PAIR_SEP = ':';

  // INSERT OVERWRITE TABLE x override
  static INSERT_OVERWRITE = ' OVERWRITE TABLE';

  // Whether the SELECT .. INTO syntax is used instead of CTAS
  static SUPPORTS_SELECT_INTO = false;

  // Whether UNLOGGED tables can be created
  static SUPPORTS_UNLOGGED_TABLES = false;

  // Whether the CREATE TABLE LIKE statement is supported
  static SUPPORTS_CREATE_TABLE_LIKE = true;

  // Whether the LikeProperty needs to be specified inside of the schema clause
  static LIKE_PROPERTY_INSIDE_SCHEMA = false;

  // Whether DISTINCT can be followed by multiple args in an AggFunc. If not, it will be
  // transpiled into a series of CASE-WHEN-ELSE, ultimately using a tuple conseisting of the args
  static MULTI_ARG_DISTINCT = true;

  // Whether the JSON extraction operators expect a value of type JSON
  static JSON_TYPE_REQUIRED_FOR_EXTRACTION = false;

  // Whether bracketed keys like ["foo"] are supported in JSON paths
  static JSON_PATH_BRACKETED_KEY_SUPPORTED = true;

  // Whether to escape keys using single quotes in JSON paths
  static JSON_PATH_SINGLE_QUOTE_ESCAPE = false;

  // The JsonPathPart expressions supported by this dialect
  @cache
  static get SUPPORTED_JSON_PATH_PARTS (): Set<typeof Expression> {
    return new Set([...ALL_JSON_PATH_PARTS]);
  }

  // Whether any(f(x) for x in array) can be implemented by this dialect
  static CAN_IMPLEMENT_ARRAY_ANY = false;

  // Whether the function TO_NUMBER is supported
  static SUPPORTS_TO_NUMBER = true;

  // Whether EXCLUDE in window specification is supported
  static SUPPORTS_WINDOW_EXCLUDE = false;

  // Whether or not set op modifiers apply to the outer set op or select.
  // SELECT * FROM x UNION SELECT * FROM y LIMIT 1
  // True means limit 1 happens after the set op, False means it it happens on y.
  static SET_OP_MODIFIERS = true;

  // Whether parameters from COPY statement are wrapped in parentheses
  static COPY_PARAMS_ARE_WRAPPED = true;

  // Whether values of params are set with "=" token or empty space
  static COPY_PARAMS_EQ_REQUIRED = false;

  // Whether COPY statement has INTO keyword
  static COPY_HAS_INTO_KEYWORD = true;

  // Whether the conditional TRY(expression) function is supported
  static TRY_SUPPORTED = true;

  // Whether the UESCAPE syntax in unicode strings is supported
  static SUPPORTS_UESCAPE = true;

  // Function used to replace escaped unicode codes in unicode strings
  static UNICODE_SUBSTITUTE?: string | ((substring: string, ...args: string[]) => string);

  // The keyword to use when generating a star projection with excluded columns
  static STAR_EXCEPT = 'EXCEPT';

  // The HEX function name
  static HEX_FUNC = 'HEX';

  // The keywords to use when prefixing & separating WITH based properties
  static WITH_PROPERTIES_PREFIX = 'WITH';

  // Whether to quote the generated expression of exp.JsonPath
  static QUOTE_JSON_PATH = true;

  // Whether the text pattern/fill (3rd) parameter of RPAD()/LPAD() is optional (defaults to space)
  static PAD_FILL_PATTERN_IS_REQUIRED = false;

  // Whether a projection can explode into multiple rows, e.g. by unnesting an array.
  static SUPPORTS_EXPLODING_PROJECTIONS = true;

  // Whether ARRAY_CONCAT can be generated with varlen args or if it should be reduced to 2-arg version
  static ARRAY_CONCAT_IS_VAR_LEN = true;

  // Whether CONVERT_TIMEZONE() is supported; if not, it will be generated as exp.AtTimeZone
  static SUPPORTS_CONVERT_TIMEZONE = false;

  // Whether MEDIAN(expr) is supported; if not, it will be generated as PERCENTILE_CONT(expr, 0.5)
  static SUPPORTS_MEDIAN = true;

  // Whether UNIX_SECONDS(timestamp) is supported
  static SUPPORTS_UNIX_SECONDS = false;

  // Whether to wrap <props> in `AlterSet`, e.g., ALTER ... SET (<props>)
  static ALTER_SET_WRAPPED = false;

  // Whether to normalize the date parts in EXTRACT(<date_part> FROM <expr>) into a common representation
  // For instance, to extract the day of week in ISO semantics, one can use IsoDOW, DAYOFWEEKISO etc depending on the dialect.
  static NORMALIZE_EXTRACT_DATE_PARTS = false;

  // The name to generate for the JsonPath expression. If `None`, only `this` will be generated
  static PARSE_JSON_NAME?: string | undefined = 'PARSE_JSON';

  // The function name of the exp.ArraySize expression
  static ARRAY_SIZE_NAME = 'ARRAY_LENGTH';

  // The syntax to use when altering the type of a column
  static ALTER_SET_TYPE = 'SET DATA TYPE';

  // Whether exp.ArraySize should generate the dimension arg too (valid for Postgres & DuckDB)
  // None -> Doesn't support it at all
  // False (DuckDB) -> Has backwards-compatible support, but preferably generated without
  // True (Postgres) -> Explicitly requires it
  static ARRAY_SIZE_DIM_REQUIRED?: boolean;

  // Whether a multi-argument DECODE(...) function is supported. If not, a CASE expression is generated
  static SUPPORTS_DECODE_CASE = true;
  static ON_CONDITION_EMPTY_BEFORE_ERROR = false;

  // Whether SYMMETRIC and ASYMMETRIC flags are supported with BETWEEN expression
  static SUPPORTS_BETWEEN_FLAGS = false;

  // Whether LIKE and ILIKE support quantifiers such as LIKE ANY/ALL/SOME
  static SUPPORTS_LIKE_QUANTIFIERS = true;

  // Prefix which is appended to exp.Table expressions in MATCH AGAINST
  static MATCH_AGAINST_TABLE_PREFIX?: string;

  // Whether to include the VARIABLE keyword for SET assignments
  static SET_ASSIGNMENT_REQUIRES_VARIABLE_KEYWORD = false;

  // Whether FROM is supported in UPDATE statements or if joins must be generated instead, e.g:
  // Supported (Postgres, Doris etc): UPDATE t1 SET t1.a = t2.b FROM t2
  // Unsupported (MySQL, SingleStore): UPDATE t1 JOIN t2 ON TRUE SET t1.a = t2.b
  static UPDATE_STATEMENT_SUPPORTS_FROM = true;

  // Whether SELECT *, ... EXCLUDE requires wrapping in a subquery for transpilation.
  static STAR_EXCLUDE_REQUIRES_DERIVED_TABLE = true;

  static AFTER_HAVING_MODIFIER_TRANSFORMS: Record<string, (this: Generator, e: Expression) => string> = {
    cluster: function (this: Generator, e) {
      return this.sql(e, 'cluster');
    },
    distribute: function (this: Generator, e) {
      return this.sql(e, 'distribute');
    },
    sort: function (this: Generator, e) {
      return this.sql(e, 'sort');
    },
    windows: function (this: Generator, e) {
      return e.getArgKey('windows')
        ? this.seg('WINDOW ') + this.expressions(e, {
          key: 'windows',
          flat: true,
        })
        : '';
    },
    qualify: function (this: Generator, e) {
      return this.sql(e, 'qualify');
    },
  };

  static SAFE_JSON_PATH_KEY_RE = /^[a-zA-Z_$][a-zA-Z0-9_$]*$/;

  static SENTINEL_LINE_BREAK = '__SQLGLOT__LB__';

  static TIME_PART_SINGULARS: Record<string, string> = {
    MICROSECONDS: 'MICROSECOND',
    SECONDS: 'SECOND',
    MINUTES: 'MINUTE',
    HOURS: 'HOUR',
    DAYS: 'DAY',
    WEEKS: 'WEEK',
    MONTHS: 'MONTH',
    QUARTERS: 'QUARTER',
    YEARS: 'YEAR',
  };

  @cache
  static get UNWRAPPED_INTERVAL_VALUES (): Set<typeof Expression> {
    return new Set<typeof Expression>([
      ColumnExpr,
      LiteralExpr,
      NegExpr,
      ParenExpr,
    ]);
  }

  @cache
  static get WITH_SEPARATED_COMMENTS (): Set<typeof Expression> {
    return new Set<typeof Expression>([
      CommandExpr,
      CreateExpr,
      DescribeExpr,
      DeleteExpr,
      DropExpr,
      FromExpr,
      InsertExpr,
      JoinExpr,
      MultitableInsertsExpr,
      OrderExpr,
      GroupExpr,
      HavingExpr,
      SelectExpr,
      SetOperationExpr,
      UpdateExpr,
      WhereExpr,
      WithExpr,
    ]);
  }

  @cache
  static get EXCLUDE_COMMENTS (): Set<typeof Expression> {
    return new Set<typeof Expression>([BinaryExpr, SetOperationExpr]);
  }

  @cache
  static get TYPE_MAPPING (): Map<DataTypeExprKind | string, string> {
    return new Map<DataTypeExprKind | string, string>([
      [DataTypeExprKind.DATETIME2, 'TIMESTAMP'],
      [DataTypeExprKind.NCHAR, 'CHAR'],
      [DataTypeExprKind.NVARCHAR, 'VARCHAR'],
      [DataTypeExprKind.MEDIUMTEXT, 'TEXT'],
      [DataTypeExprKind.LONGTEXT, 'TEXT'],
      [DataTypeExprKind.TINYTEXT, 'TEXT'],
      [DataTypeExprKind.BLOB, 'VARBINARY'],
      [DataTypeExprKind.MEDIUMBLOB, 'BLOB'],
      [DataTypeExprKind.LONGBLOB, 'BLOB'],
      [DataTypeExprKind.TINYBLOB, 'BLOB'],
      [DataTypeExprKind.INET, 'INET'],
      [DataTypeExprKind.ROWVERSION, 'VARBINARY'],
      [DataTypeExprKind.SMALLDATETIME, 'TIMESTAMP'],
    ]);
  }

  @cache
  static get UNSUPPORTED_TYPES (): Set<DataTypeExprKind> {
    return new Set<DataTypeExprKind>();
  }

  static STRUCT_DELIMITER = ['<', '>'];
  @cache
  static get PARAMETERIZABLE_TEXT_TYPES (): Set<DataTypeExprKind> {
    return new Set<DataTypeExprKind>([
      DataTypeExprKind.NVARCHAR,
      DataTypeExprKind.VARCHAR,
      DataTypeExprKind.CHAR,
      DataTypeExprKind.NCHAR,
    ]);
  }

  static PARAMETER_TOKEN = '@';
  static NAMED_PLACEHOLDER_TOKEN = ':';

  static RESERVED_KEYWORDS = new Set<string>();
  @cache
  static get TOKEN_MAPPING (): Partial<Record<TokenType, string>> {
    return {};
  }

  // Expressions that need to have all CTEs under them bubbled up to them
  static EXPRESSIONS_WITHOUT_NESTED_CteS: Set<typeof Expression> = new Set();

  // Creatables where the expression (e.g. AS SELECT ...) precedes the schema-level properties
  static EXPRESSION_PRECEDES_PROPERTIES_CREATABLES: Set<string> = new Set();

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (this: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return new Map<typeof Expression, (this: Generator, e: any) => string>(
      [
        [
          AdjacentExpr,
          function (this: Generator, e: BinaryExpr) {
            return this.binary(e, '-|-');
          },
        ],
        [
          AllowedValuesPropertyExpr,
          function (this: Generator, e) {
            return `ALLOWED_VALUES ${this.expressions(e, { flat: true })}`;
          },
        ],
        [
          AnalyzeColumnsExpr,
          function (this: Generator, e) {
            return this.sql(e, 'this');
          },
        ],
        [
          AnalyzeWithExpr,
          function (this: Generator, e) {
            return this.expressions(e, {
              prefix: 'WITH ',
              sep: ' ',
            });
          },
        ],
        [
          ArrayContainsAllExpr,
          function (this: Generator, e) {
            return this.binary(e, '@>');
          },
        ],
        [
          ArrayOverlapsExpr,
          function (this: Generator, e) {
            return this.binary(e, '&&');
          },
        ],
        [
          AutoRefreshPropertyExpr,
          function (this: Generator, e) {
            return `AUTO REFRESH ${this.sql(e, 'this')}`;
          },
        ],
        [
          BackupPropertyExpr,
          function (this: Generator, e) {
            return `BACKUP ${this.sql(e, 'this')}`;
          },
        ],
        [CaseSpecificColumnConstraintExpr, (e: Expression) => `${e.getArgKey('not') ? 'NOT ' : ''}CASESPECIFIC`],
        [
          CeilExpr,
          function (this: Generator, e) {
            return this.ceilFloor(e);
          },
        ],
        [
          CharacterSetColumnConstraintExpr,
          function (this: Generator, e) {
            return `CHARACTER SET ${this.sql(e, 'this')}`;
          },
        ],
        [
          CharacterSetPropertyExpr,
          function (this: Generator, e: Expression) {
            return `${e.getArgKey('default') ? 'DEFAULT ' : ''}CHARACTER SET=${this.sql(e, 'this')}`;
          },
        ],
        [
          ClusteredColumnConstraintExpr,
          function (this: Generator, e) {
            return `CLUSTERED (${this.expressions(e, {
              key: 'this',
              indent: false,
            })})`;
          },
        ],
        [
          CollateColumnConstraintExpr,
          function (this: Generator, e) {
            return `COLLATE ${this.sql(e, 'this')}`;
          },
        ],
        [
          CommentColumnConstraintExpr,
          function (this: Generator, e) {
            return `COMMENT ${this.sql(e, 'this')}`;
          },
        ],
        [
          ConnectByRootExpr,
          function (this: Generator, e) {
            return `CONNECT_BY_ROOT ${this.sql(e, 'this')}`;
          },
        ],
        [
          ConvertToCharsetExpr,
          function (this: Generator, e: ConvertToCharsetExpr) {
            return this.func('CONVERT', [
              e.args.this,
              e.args.dest,
              e.args.source,
            ]);
          },
        ],
        [CopyGrantsPropertyExpr, () => 'COPY GRANTS'],
        [
          CredentialsPropertyExpr,
          function (this: Generator, e) {
            return `CREDENTIALS=(${this.expressions(e, {
              key: 'expressions',
              sep: ' ',
            })})`;
          },
        ],
        [CurrentCatalogExpr, () => 'CURRENT_CATALOG'],
        [SessionUserExpr, () => 'SESSION_USER'],
        [
          DateFormatColumnConstraintExpr,
          function (this: Generator, e) {
            return `FORMAT ${this.sql(e, 'this')}`;
          },
        ],
        [
          DefaultColumnConstraintExpr,
          function (this: Generator, e) {
            return `DEFAULT ${this.sql(e, 'this')}`;
          },
        ],
        [DynamicPropertyExpr, () => 'DYNAMIC'],
        [EmptyPropertyExpr, () => 'EMPTY'],
        [
          EncodeColumnConstraintExpr,
          function (this: Generator, e) {
            return `ENCODE ${this.sql(e, 'this')}`;
          },
        ],
        [
          EnviromentPropertyExpr,
          function (this: Generator, e) {
            return `ENVIRONMENT (${this.expressions(e, { flat: true })})`;
          },
        ],
        [
          EphemeralColumnConstraintExpr,
          function (this: Generator, e: Expression) {
            return `EPHEMERAL${e.args.this ? ' ' + this.sql(e, 'this') : ''}`;
          },
        ],
        [
          ExcludeColumnConstraintExpr,
          function (this: Generator, e) {
            return `EXCLUDE ${this.sql(e, 'this').trimStart()}`;
          },
        ],
        [
          ExecuteAsPropertyExpr,
          function (this: Generator, e) {
            return this.nakedProperty(e);
          },
        ],
        [
          ExceptExpr,
          function (this: Generator, e) {
            return this.setOperations(e);
          },
        ],
        [ExternalPropertyExpr, () => 'EXTERNAL'],
        [
          FloorExpr,
          function (this: Generator, e) {
            return this.ceilFloor(e);
          },
        ],
        [
          GetExpr,
          function (this: Generator, e) {
            return this.getPutSql(e);
          },
        ],
        [GlobalPropertyExpr, () => 'GLOBAL'],
        [HeapPropertyExpr, () => 'HEAP'],
        [IcebergPropertyExpr, () => 'ICEBERG'],
        [
          InheritsPropertyExpr,
          function (this: Generator, e) {
            return `INHERITS (${this.expressions(e, { flat: true })})`;
          },
        ],
        [
          InlineLengthColumnConstraintExpr,
          function (this: Generator, e) {
            return `INLINE LENGTH ${this.sql(e, 'this')}`;
          },
        ],
        [
          InputModelPropertyExpr,
          function (this: Generator, e) {
            return `INPUT${this.sql(e, 'this')}`;
          },
        ],
        [
          IntersectExpr,
          function (this: Generator, e) {
            return this.setOperations(e);
          },
        ],
        [
          IntervalSpanExpr,
          function (this: Generator, e) {
            return `${this.sql(e, 'this')} TO ${this.sql(e, 'expression')}`;
          },
        ],
        [
          Int64Expr,
          function (this: Generator, e: Int64Expr) {
            return this.sql(cast(e.args.this || '', DataTypeExprKind.BIGINT));
          },
        ],
        [
          JsonbContainsAnyTopKeysExpr,
          function (this: Generator, e) {
            return this.binary(e, '?|');
          },
        ],
        [
          JsonbContainsAllTopKeysExpr,
          function (this: Generator, e) {
            return this.binary(e, '?&');
          },
        ],
        [
          JsonbDeleteAtPathExpr,
          function (this: Generator, e) {
            return this.binary(e, '#-');
          },
        ],
        [
          LanguagePropertyExpr,
          function (this: Generator, e) {
            return this.nakedProperty(e);
          },
        ],
        [
          LocationPropertyExpr,
          function (this: Generator, e) {
            return this.nakedProperty(e);
          },
        ],
        [LogPropertyExpr, (e: Expression) => `${e.getArgKey('no') ? 'NO ' : ''}LOG`],
        [MaterializedPropertyExpr, () => 'MATERIALIZED'],
        [
          NetFuncExpr,
          function (this: Generator, e) {
            return `NET.${this.sql(e, 'this')}`;
          },
        ],
        [
          NonClusteredColumnConstraintExpr,
          function (this: Generator, e) {
            return `NONCLUSTERED (${this.expressions(e, {
              key: 'this',
              indent: false,
            })})`;
          },
        ],
        [NoPrimaryIndexPropertyExpr, () => 'NO PRIMARY INDEX'],
        [NotForReplicationColumnConstraintExpr, () => 'NOT FOR REPLICATION'],
        [OnCommitPropertyExpr, (e: Expression) => `ON COMMIT ${e.getArgKey('delete') ? 'DELETE' : 'PRESERVE'} ROWS`],
        [
          OnPropertyExpr,
          function (this: Generator, e) {
            return `ON ${this.sql(e, 'this')}`;
          },
        ],
        [
          OnUpdateColumnConstraintExpr,
          function (this: Generator, e) {
            return `ON UPDATE ${this.sql(e, 'this')}`;
          },
        ],
        [
          OperatorExpr,
          function (this: Generator, e) {
            return this.binary(e, '');
          },
        ],
        [
          OutputModelPropertyExpr,
          function (this: Generator, e) {
            return `OUTPUT${this.sql(e, 'this')}`;
          },
        ],
        [
          ExtendsLeftExpr,
          function (this: Generator, e) {
            return this.binary(e, '&<');
          },
        ],
        [
          ExtendsRightExpr,
          function (this: Generator, e) {
            return this.binary(e, '&>');
          },
        ],
        [
          PathColumnConstraintExpr,
          function (this: Generator, e) {
            return `PATH ${this.sql(e, 'this')}`;
          },
        ],
        [
          PartitionedByBucketExpr,
          function (this: Generator, e: PartitionedByBucketExpr) {
            return this.func('BUCKET', [e.args.this, e.args.expression]);
          },
        ],
        [
          PartitionByTruncateExpr,
          function (this: Generator, e: PartitionByTruncateExpr) {
            return this.func('TRUNCATE', [e.args.this, e.args.expression]);
          },
        ],
        [
          PivotAnyExpr,
          function (this: Generator, e) {
            return `ANY${this.sql(e, 'this')}`;
          },
        ],
        [
          PositionalColumnExpr,
          function (this: Generator, e) {
            return `#${this.sql(e, 'this')}`;
          },
        ],
        [
          ProjectionPolicyColumnConstraintExpr,
          function (this: Generator, e) {
            return `PROJECTION POLICY ${this.sql(e, 'this')}`;
          },
        ],
        [ZeroFillColumnConstraintExpr, () => 'ZEROFILL'],
        [
          PutExpr,
          function (this: Generator, e) {
            return this.getPutSql(e);
          },
        ],
        [
          RemoteWithConnectionModelPropertyExpr,
          function (this: Generator, e) {
            return `REMOTE WITH CONNECTION ${this.sql(e, 'this')}`;
          },
        ],
        [
          ReturnsPropertyExpr,
          function (this: Generator, e: ReturnsPropertyExpr) {
            return e.getArgKey('null') ? 'RETURNS NULL ON NULL INPUT' : this.nakedProperty(e);
          },
        ],
        [
          SafeFuncExpr,
          function (this: Generator, e) {
            return `SAFE.${this.sql(e, 'this')}`;
          },
        ],
        [
          SamplePropertyExpr,
          function (this: Generator, e) {
            return `SAMPLE BY ${this.sql(e, 'this')}`;
          },
        ],
        [SecurePropertyExpr, () => 'SECURE'],
        [
          SecurityPropertyExpr,
          function (this: Generator, e) {
            return `SECURITY ${this.sql(e, 'this')}`;
          },
        ],
        [
          SetConfigPropertyExpr,
          function (this: Generator, e) {
            return this.sql(e, 'this');
          },
        ],
        [SetPropertyExpr, (e: Expression) => `${e.getArgKey('multi') ? 'MULTI' : ''}SET`],
        [
          SettingsPropertyExpr,
          function (this: Generator, e) {
            return `SETTINGS${this.seg('')}${this.expressions(e)}`;
          },
        ],
        [
          SharingPropertyExpr,
          function (this: Generator, e) {
            return `SHARING=${this.sql(e, 'this')}`;
          },
        ],
        [SqlReadWritePropertyExpr, (e: Expression) => e.name],
        [
          SqlSecurityPropertyExpr,
          function (this: Generator, e) {
            return `SQL SECURITY ${this.sql(e, 'this')}`;
          },
        ],
        [StabilityPropertyExpr, (e: Expression) => e.name],
        [
          StreamExpr,
          function (this: Generator, e) {
            return `STREAM ${this.sql(e, 'this')}`;
          },
        ],
        [StreamingTablePropertyExpr, () => 'STREAMING'],
        [StrictPropertyExpr, () => 'STRICT'],
        [
          SwapTableExpr,
          function (this: Generator, e) {
            return `SWAP WITH ${this.sql(e, 'this')}`;
          },
        ],
        [
          TableColumnExpr,
          function (this: Generator, e: TableColumnExpr) {
            return this.sql(e.args.this);
          },
        ],
        [
          TagsExpr,
          function (this: Generator, e) {
            return `TAG (${this.expressions(e, { flat: true })})`;
          },
        ],
        [TemporaryPropertyExpr, () => 'TEMPORARY'],
        [
          TitleColumnConstraintExpr,
          function (this: Generator, e) {
            return `TITLE ${this.sql(e, 'this')}`;
          },
        ],
        [
          ToMapExpr,
          function (this: Generator, e) {
            return `MAP ${this.sql(e, 'this')}`;
          },
        ],
        [
          ToTablePropertyExpr,
          function (this: Generator, e: Expression) {
            return `TO ${this.sql(e.args.this as string | Expression)}`;
          },
        ],
        [
          TransformModelPropertyExpr,
          function (this: Generator, e: TransformModelPropertyExpr) {
            return this.func('TRANSFORM', e.args.expressions || []);
          },
        ],
        [TransientPropertyExpr, () => 'TRANSIENT'],
        [
          UnionExpr,
          function (this: Generator, e) {
            return this.setOperations(e);
          },
        ],
        [UnloggedPropertyExpr, () => 'UNLOGGED'],
        [
          UsingTemplatePropertyExpr,
          function (this: Generator, e) {
            return `USING TEMPLATE ${this.sql(e, 'this')}`;
          },
        ],
        [
          UsingDataExpr,
          function (this: Generator, e) {
            return `USING DATA ${this.sql(e, 'this')}`;
          },
        ],
        [UppercaseColumnConstraintExpr, () => 'UPPERCASE'],
        [
          UtcDateExpr,
          function (this: Generator, _e) {
            return this.sql(new CurrentDateExpr({ this: LiteralExpr.string('UTC') }));
          },
        ],
        [
          UtcTimeExpr,
          function (this: Generator, _e) {
            return this.sql(new CurrentTimeExpr({ this: LiteralExpr.string('UTC') }));
          },
        ],
        [
          UtcTimestampExpr,
          function (this: Generator, _e) {
            return this.sql(new CurrentTimestampExpr({ this: LiteralExpr.string('UTC') }));
          },
        ],
        [
          VariadicExpr,
          function (this: Generator, e) {
            return `VARIADIC ${this.sql(e, 'this')}`;
          },
        ],
        [
          VarMapExpr,
          function (this: Generator, e: VarMapExpr) {
            return this.func('MAP', e.keys.flatMap((k, i) => [k, e.values[i]]));
          },
        ],
        [
          ViewAttributePropertyExpr,
          function (this: Generator, e) {
            return `WITH ${this.sql(e, 'this')}`;
          },
        ],
        [VolatilePropertyExpr, () => 'VOLATILE'],
        [
          WithJournalTablePropertyExpr,
          function (this: Generator, e) {
            return `WITH JOURNAL TABLE=${this.sql(e, 'this')}`;
          },
        ],
        [
          WithProcedureOptionsExpr,
          function (this: Generator, e) {
            return `WITH ${this.expressions(e, { flat: true })}`;
          },
        ],
        [
          WithSchemaBindingPropertyExpr,
          function (this: Generator, e) {
            return `WITH SCHEMA ${this.sql(e, 'this')}`;
          },
        ],
        [
          WithOperatorExpr,
          function (this: Generator, e) {
            return `${this.sql(e, 'this')} WITH ${this.sql(e, 'op')}`;
          },
        ],
        [ForcePropertyExpr, () => 'FORCE'],
      ],
    );
  }

  /**
   * @final Do not override this getter in subclasses; override `ORIGINAL_TRANSFORMS` instead.
   */
  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get TRANSFORMS (): Map<typeof Expression, (this: Generator, e: any) => string> {
    const transforms = this.ORIGINAL_TRANSFORMS;
    for (const part of Array.from(ALL_JSON_PATH_PARTS).filter((cls) => !this.SUPPORTED_JSON_PATH_PARTS.has(cls))) {
      transforms.delete(part);
    }
    return transforms;
  }

  @cache
  static get PROPERTIES_LOCATION (): Map<typeof Expression, PropertiesLocation> {
    return new Map<typeof Expression, PropertiesLocation>([
      [AllowedValuesPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [AlgorithmPropertyExpr, PropertiesLocation.POST_CREATE],
      [AutoIncrementPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [AutoRefreshPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [BackupPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [BlockCompressionPropertyExpr, PropertiesLocation.POST_NAME],
      [CharacterSetPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [ChecksumPropertyExpr, PropertiesLocation.POST_NAME],
      [CollatePropertyExpr, PropertiesLocation.POST_SCHEMA],
      [CopyGrantsPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [ClusterExpr, PropertiesLocation.POST_SCHEMA],
      [ClusteredByPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [DistributedByPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [DuplicateKeyPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [DataBlocksizePropertyExpr, PropertiesLocation.POST_NAME],
      [DataDeletionPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [DefinerPropertyExpr, PropertiesLocation.POST_CREATE],
      [DictRangeExpr, PropertiesLocation.POST_SCHEMA],
      [DictPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [DynamicPropertyExpr, PropertiesLocation.POST_CREATE],
      [DistKeyPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [DistStylePropertyExpr, PropertiesLocation.POST_SCHEMA],
      [EmptyPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [EncodePropertyExpr, PropertiesLocation.POST_EXPRESSION],
      [EnginePropertyExpr, PropertiesLocation.POST_SCHEMA],
      [EnviromentPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [ExecuteAsPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [ExternalPropertyExpr, PropertiesLocation.POST_CREATE],
      [FallbackPropertyExpr, PropertiesLocation.POST_NAME],
      [FileFormatPropertyExpr, PropertiesLocation.POST_WITH],
      [FreespacePropertyExpr, PropertiesLocation.POST_NAME],
      [GlobalPropertyExpr, PropertiesLocation.POST_CREATE],
      [HeapPropertyExpr, PropertiesLocation.POST_WITH],
      [InheritsPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [IcebergPropertyExpr, PropertiesLocation.POST_CREATE],
      [IncludePropertyExpr, PropertiesLocation.POST_SCHEMA],
      [InputModelPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [IsolatedLoadingPropertyExpr, PropertiesLocation.POST_NAME],
      [JournalPropertyExpr, PropertiesLocation.POST_NAME],
      [LanguagePropertyExpr, PropertiesLocation.POST_SCHEMA],
      [LikePropertyExpr, PropertiesLocation.POST_SCHEMA],
      [LocationPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [LockPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [LockingPropertyExpr, PropertiesLocation.POST_ALIAS],
      [LogPropertyExpr, PropertiesLocation.POST_NAME],
      [MaterializedPropertyExpr, PropertiesLocation.POST_CREATE],
      [MergeBlockRatioPropertyExpr, PropertiesLocation.POST_NAME],
      [NoPrimaryIndexPropertyExpr, PropertiesLocation.POST_EXPRESSION],
      [OnPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [OnCommitPropertyExpr, PropertiesLocation.POST_EXPRESSION],
      [OrderExpr, PropertiesLocation.POST_SCHEMA],
      [OutputModelPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [PartitionedByPropertyExpr, PropertiesLocation.POST_WITH],
      [PartitionedOfPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [PrimaryKeyExpr, PropertiesLocation.POST_SCHEMA],
      [PropertyExpr, PropertiesLocation.POST_WITH],
      [RefreshTriggerPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [RemoteWithConnectionModelPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [ReturnsPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [RollupPropertyExpr, PropertiesLocation.UNSUPPORTED],
      [RowFormatPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [RowFormatDelimitedPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [RowFormatSerdePropertyExpr, PropertiesLocation.POST_SCHEMA],
      [SamplePropertyExpr, PropertiesLocation.POST_SCHEMA],
      [SchemaCommentPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [SecurePropertyExpr, PropertiesLocation.POST_CREATE],
      [SecurityPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [SerdePropertiesExpr, PropertiesLocation.POST_SCHEMA],
      [SetExpr, PropertiesLocation.POST_SCHEMA],
      [SettingsPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [SetPropertyExpr, PropertiesLocation.POST_CREATE],
      [SetConfigPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [SharingPropertyExpr, PropertiesLocation.POST_EXPRESSION],
      [SequencePropertiesExpr, PropertiesLocation.POST_EXPRESSION],
      [SortKeyPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [SqlReadWritePropertyExpr, PropertiesLocation.POST_SCHEMA],
      [SqlSecurityPropertyExpr, PropertiesLocation.POST_CREATE],
      [StabilityPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [StorageHandlerPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [StreamingTablePropertyExpr, PropertiesLocation.POST_CREATE],
      [StrictPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [TagsExpr, PropertiesLocation.POST_WITH],
      [TemporaryPropertyExpr, PropertiesLocation.POST_CREATE],
      [ToTablePropertyExpr, PropertiesLocation.POST_SCHEMA],
      [TransientPropertyExpr, PropertiesLocation.POST_CREATE],
      [TransformModelPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [MergeTreeTtlExpr, PropertiesLocation.POST_SCHEMA],
      [UnloggedPropertyExpr, PropertiesLocation.POST_CREATE],
      [UsingTemplatePropertyExpr, PropertiesLocation.POST_SCHEMA],
      [ViewAttributePropertyExpr, PropertiesLocation.POST_SCHEMA],
      [VolatilePropertyExpr, PropertiesLocation.POST_CREATE],
      [WithDataPropertyExpr, PropertiesLocation.POST_EXPRESSION],
      [WithJournalTablePropertyExpr, PropertiesLocation.POST_NAME],
      [WithProcedureOptionsExpr, PropertiesLocation.POST_SCHEMA],
      [WithSchemaBindingPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [WithSystemVersioningPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [ForcePropertyExpr, PropertiesLocation.POST_CREATE],
    ]);
  }

  // Instance properties
  protected pretty: boolean;
  protected identify: string | boolean;
  protected normalize: boolean;
  protected pad: number;
  protected indentAmount: number;
  protected normalizeFunctions: string | boolean;
  protected unsupportedLevel: ErrorLevel;
  protected maxUnsupported: number;
  protected leadingComma: boolean;
  protected maxTextWidth: number;
  protected comments: boolean;
  dialect: Dialect;
  protected unsupportedMessages: string[];
  protected escapedQuoteEnd: string;
  private nextName: () => string;
  protected escapedByteQuoteEnd: string;
  protected escapedIdentifierEnd: string;
  protected identifierStart: string;
  protected identifierEnd: string;
  quoteJsonPathKeyUsingBrackets: boolean;

  constructor (options: GeneratorOptions = {}) {
    this.pretty = options.pretty ?? false;
    this.identify = options.identify ?? false;
    this.normalize = options.normalize ?? false;
    this.pad = options.pad ?? 2;
    this.indentAmount = options.indent ?? 2;
    this.unsupportedLevel = options.unsupportedLevel ?? ErrorLevel.WARN;
    this.maxUnsupported = options.maxUnsupported ?? 3;
    this.leadingComma = options.leadingComma ?? false;
    this.maxTextWidth = options.maxTextWidth ?? 80;
    this.comments = options.comments ?? true;

    // Get dialect and prioritize option over dialect default
    this.dialect = Dialect.getOrRaise(options.dialect);
    const dialectClass = this.dialect._constructor;
    this.normalizeFunctions = options.normalizeFunctions ?? dialectClass.NORMALIZE_FUNCTIONS ?? NormalizeFunctions.UPPER;

    // Initialize escaped delimiters
    const stringEscapes = dialectClass.tokenizerClass.STRING_ESCAPES;
    const escapeChar = stringEscapes && 0 < stringEscapes.length ? stringEscapes[0] : '';
    this.escapedQuoteEnd = escapeChar + this.dialect._constructor.QUOTE_END;
    this.escapedByteQuoteEnd = this.dialect._constructor.BYTE_END
      ? escapeChar + this.dialect._constructor.BYTE_END
      : '';

    // Initialize identifier delimiters
    this.escapedIdentifierEnd = this.dialect._constructor.IDENTIFIER_END + this.dialect._constructor.IDENTIFIER_END;
    this.identifierStart = this.dialect._constructor.IDENTIFIER_START;
    this.identifierEnd = this.dialect._constructor.IDENTIFIER_END;

    this.unsupportedMessages = [];
    this.nextName = nameSequence('_t');
    this.quoteJsonPathKeyUsingBrackets = true;
  }

  get _constructor (): typeof Generator {
    return this.constructor as typeof Generator;
  }

  /**
   * Main generate method - converts an expression tree to SQL string.
   */
  generate (expression: Expression, options: { copy?: boolean } = {}): string {
    const { copy = true } = options;
    let expr = copy
      ? expression.copy()
      : expression;

    expr = this.preprocess(expr);

    this.unsupportedMessages = [];

    let sql = this.sql(expr).trim();

    if (this.pretty) {
      sql = sql.replaceAll(this._constructor.SENTINEL_LINE_BREAK, '\n');
    }

    if (this.unsupportedLevel === ErrorLevel.IGNORE) {
      return sql;
    }

    if (this.unsupportedLevel === ErrorLevel.WARN) {
      for (const msg of this.unsupportedMessages) {
        console.warn(msg);
      }
    } else if (this.unsupportedLevel === ErrorLevel.RAISE && 0 < this.unsupportedMessages.length) {
      throw new UnsupportedError(concatMessages(this.unsupportedMessages, this.maxUnsupported));
    }

    return sql;
  }

  protected preprocess (expression: Expression): Expression {
    expression = this.moveCtesToTopLevel(expression);

    if (this._constructor.ENSURE_BOOLS) {
      expression = ensureBools(expression);
    }

    return expression;
  }

  protected moveCtesToTopLevel<E extends Expression> (expression: E): E {
    if (
      !expression.parent
      && (this.constructor as typeof Generator).EXPRESSIONS_WITHOUT_NESTED_CteS.has(expression._constructor)
      && [...expression.findAll(WithExpr)].some((node) => node.parent !== expression)
      && isInstanceOf(expression, SelectExpr)
    ) {
      assertIsInstanceOf(expression, SelectExpr);
      return moveCtesToTopLevel(expression) as E;
    }
    return expression;
  }

  /**
   * Record an unsupported expression/feature.
   */
  unsupported (message: string): void {
    if (this.unsupportedLevel === ErrorLevel.IMMEDIATE) {
      throw new UnsupportedError(message);
    }
    this.unsupportedMessages.push(message);
  }

  /**
   * Generate a separator (space or newline based on pretty mode).
   */
  sep (separator = ' '): string {
    if (this.pretty) {
      return `${separator.trim()}\n`;
    }
    return separator;
  }

  /**
   * Generate a segment with separator.
   */
  seg (sql: string, separator = ' '): string {
    if (!sql) {
      return '';
    }
    return `${this.sep(separator)}${sql}`;
  }

  sanitizeComment (comment: string): string {
    let result = comment;

    // Add space at start if first char is not whitespace
    if (result[0] && result[0].trim() !== '') {
      result = ' ' + result;
    }

    // Add space at end if last char is not whitespace
    if (result[result.length - 1] && result[result.length - 1].trim() !== '') {
      result = result + ' ';
    }

    // If dialect doesn't support nested comments, replace */ with * /
    if (!this.dialect._constructor.tokenizerClass.NESTED_COMMENTS) {
      result = result.replace(/\*\//g, '* /');
    }

    return result;
  }

  wrap (expression: Expression | string): string {
    let thisSql: string;

    // Check if the expression is one that should not be accessed via the 'this' key
    if (typeof expression !== 'string' && UNWRAPPED_QUERIES.some((cls) => expression instanceof cls)) {
      thisSql = this.sql(expression);
    } else {
      thisSql = this.sql(expression, 'this');
    }

    if (!thisSql) {
      return '()';
    }

    thisSql = this.indent(thisSql, {
      level: 1,
      pad: 0,
    });

    return `(${this.sep('')}${thisSql}${this.seg(')', '')})`;
  }

  /**
   * Temporarily disable identifier quoting, execute func, then restore.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  noIdentify<T extends (...args: any[]) => string> (
    func: T,
    ...args: Parameters<T>
  ): string {
    const original = this.identify;
    this.identify = false;
    const result = func(...args);
    this.identify = original;
    return result;
  }

  /**
   * Normalize a function name based on settings.
   */
  normalizeFunc (name: string): string {
    if (this.normalizeFunctions === NormalizeFunctions.UPPER || this.normalizeFunctions === true) {
      return name.toUpperCase();
    }
    if (this.normalizeFunctions === NormalizeFunctions.LOWER) {
      return name.toLowerCase();
    }
    return name;
  }

  protected indent (
    sql: string,
    options: {
      skipFirst?: boolean;
      skipLast?: boolean;
      level?: number;
      pad?: number;
    } = {},
  ) {
    const {
      skipFirst = false, skipLast = false, pad = this.pad, level = 0,
    } = options;
    if (!this.pretty || !sql) {
      return sql;
    }
    const lines = sql.split('\n');

    return lines
      .map((line, i) => {
        if ((skipFirst && i === 0) || (skipLast && i === lines.length - 1)) {
          return line;
        }

        const indentation = ' '.repeat(level * this.indentAmount + pad);
        return `${indentation}${line}`;
      })
      .join('\n');
  }

  /**
   * Core SQL generation method with auto-discovery.
   *
   * @param expression - Expression to generate SQL for (or string/undefined)
   * @param key - Optional key to extract from expression.args
   * @param comment - Whether to include comments (default: true)
   */
  sql (
    expression?: Expression | string | number | boolean,
    key?: string,
    options: { comment?: boolean } = {},
  ): string {
    const { comment = true } = options;

    // Handle undefined/null early
    if (!expression) {
      return '';
    }

    // Handle string literals
    if (typeof expression === 'number' || typeof expression === 'string' || typeof expression === 'boolean') {
      return expression.toString();
    }

    // Handle key extraction
    if (key) {
      const value = expression.getArgKey(key);
      if (value) {
        if (typeof value === 'string' || value instanceof Expression) {
          return this.sql(value);
        }
      }
      return '';
    }

    // Check TRANSFORMS
    const transform = this._constructor.TRANSFORMS.get(expression._constructor);

    let sql = '';
    if (transform instanceof Function) {
      sql = transform.call(this, expression);
    } else if (expression instanceof Expression) {
      const expHandlerName = `${expression._constructor.key}Sql`;

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const handler = (this as any)[expHandlerName];
      if (handler instanceof Function) {
        sql = handler(expression);
      } else if (expression instanceof FuncExpr) {
        sql = this.functionFallbackSql(expression);
      } else if (expression instanceof PropertyExpr) {
        sql = this.propertySql(expression);
      } else {
        throw new Error(`Unsupported expression type ${expression._constructor.name}`);
      }
    } else {
      throw new Error(`Expected an Expression. Received ${typeof expression}: ${expression}`);
    }

    return this.comments && comment ? this.maybeComment(sql, expression) : sql;
  }

  /**
   * Add comment to SQL if present.
   */
  protected maybeComment (
    sql: string,
    expression?: Expression,
    comments?: string[],
    separated = false,
  ): string {
    const commentsToUse = this.comments
      ? (comments !== undefined ? comments : expression?.comments)
      : undefined;

    if (!commentsToUse || commentsToUse.length === 0) {
      return sql;
    }

    if (expression && this._constructor.EXCLUDE_COMMENTS.has(expression._constructor)) {
      return sql;
    }

    const commentsSql = commentsToUse
      .filter((c) => c)
      .map((c) => `/*${this.sanitizeComment(c)}*/`)
      .join(' ');

    if (!commentsSql) {
      return sql;
    }

    const commentsSqlWithLineBreaks = this.replaceLineBreaks(commentsSql);

    if (separated || (expression && this._constructor.WITH_SEPARATED_COMMENTS.has(expression._constructor))) {
      return (!sql || sql[0] === ' ' || sql[0] === '\n')
        ? `${this.sep()}${commentsSqlWithLineBreaks}${sql}`
        : `${commentsSqlWithLineBreaks}${this.sep()}${sql}`;
    }

    return `${sql} ${commentsSqlWithLineBreaks}`;
  }

  /**
   * Generate SQL for UNCACHE TABLE.
   */
  uncacheSql (expression: UncacheExpr): string {
    const table = this.sql(expression, 'this');
    const existsSql = expression.args.exists ? ' IF EXISTS' : '';
    return `UNCACHE TABLE${existsSql} ${table}`;
  }

  /**
   * Generate SQL for CACHE TABLE.
   */
  cacheSql (expression: CacheExpr): string {
    const lazy = expression.args.lazy ? ' LAZY' : '';
    const table = this.sql(expression, 'this');
    const options = expression.args.options as Expression[] | undefined;
    const optionsSql = options
      ? ` OPTIONS(${this.sql(options[0])} = ${this.sql(options[1])})`
      : '';
    let sql = this.sql(expression, 'expression');
    sql = sql ? ` AS${this.sep()}${sql}` : '';
    sql = `CACHE${lazy} TABLE ${table}${optionsSql}${sql}`;
    return this.prependCtes(expression, sql);
  }

  /**
   * Generate SQL for CHARACTER SET.
   */
  characterSetSql (expression: CharacterSetExpr): string {
    if (expression.parent instanceof CastExpr) {
      return `CHAR CHARACTER SET ${this.sql(expression, 'this')}`;
    }
    const defaultStr = expression.args.default ? 'DEFAULT ' : '';
    return `${defaultStr}CHARACTER SET=${this.sql(expression, 'this')}`;
  }

  /**
   * Generate parts of a column reference (catalog.db.table.column).
   */
  protected columnParts (expression: ColumnExpr): string {
    const parts = [
      expression.args.catalog,
      expression.args.db,
      expression.args.table,
      expression.args.this,
    ].filter(Boolean);

    return parts.map((part) => this.sql(part as string | Expression)).join('.');
  }

  columnSql (expression: ColumnExpr): string {
    const joinMark = expression.args.joinMark ? ' (+)' : '';

    if (joinMark && !this.dialect._constructor.SUPPORTS_COLUMN_JOIN_MARKS) {
      this.unsupported('Outer join syntax using the (+) operator is not supported.');
      return this.columnParts(expression);
    }

    return `${this.columnParts(expression)}${joinMark}`;
  }

  pseudocolumnSql (expression: PseudocolumnExpr): string {
    return this.columnSql(expression);
  }

  columnPositionSql (expression: ColumnPositionExpr): string {
    const thisStr = this.sql(expression, 'this');
    const thisFormatted = thisStr ? ` ${thisStr}` : '';
    const position = this.sql(expression, 'position');
    return `${position}${thisFormatted}`;
  }

  columnDefSql (expression: ColumnDefExpr, options: { sep?: string } = {}): string {
    const { sep = ' ' } = options;
    const column = this.sql(expression, 'this');
    let kind = this.sql(expression, 'kind');
    const constraints = this.expressions(expression, {
      key: 'constraints',
      sep: ' ',
      flat: true,
    });
    const exists = expression.args.exists ? 'IF NOT EXISTS ' : '';
    kind = kind ? `${sep}${kind}` : '';
    const constraintsPart = constraints ? ` ${constraints}` : '';
    let position = this.sql(expression, 'position');
    position = position ? ` ${position}` : '';

    // Check for ComputedColumnConstraint
    const hasComputedColumn = 0 < Array.from(expression.findAll(ComputedColumnConstraintExpr)).length;
    if (hasComputedColumn && !this._constructor.COMPUTED_COLUMN_WITH_TYPE) {
      kind = '';
    }

    return `${exists}${column}${kind}${constraintsPart}${position}`;
  }

  columnConstraintSql (expression: ColumnConstraintExpr): string {
    const thisStr = this.sql(expression, 'this');
    const kindSql = this.sql(expression, 'kind').trim();
    return thisStr ? `CONSTRAINT ${thisStr} ${kindSql}` : kindSql;
  }

  computedColumnConstraintSql (expression: ComputedColumnConstraintExpr): string {
    const thisStr = this.sql(expression, 'this');
    let persisted: string;
    if (expression.args.notNull) {
      persisted = ' PERSISTED NOT NULL';
    } else if (expression.args.persisted) {
      persisted = ' PERSISTED';
    } else {
      persisted = '';
    }
    return `AS ${thisStr}${persisted}`;
  }

  autoIncrementColumnConstraintSql (_expression: AutoIncrementColumnConstraintExpr): string {
    return this.tokenSql(TokenType.AUTO_INCREMENT);
  }

  compressColumnConstraintSql (expression: CompressColumnConstraintExpr): string {
    let thisStr: string;
    if (Array.isArray(expression.args.this)) {
      thisStr = this.wrap(this.expressions(expression, {
        key: 'this',
        flat: true,
      }));
    } else {
      thisStr = this.sql(expression, 'this');
    }
    return `COMPRESS ${thisStr}`;
  }

  generatedAsIdentityColumnConstraintSql (expression: GeneratedAsIdentityColumnConstraintExpr): string {
    let thisStr = '';
    if (expression.args.this !== undefined) {
      const onNull = expression.args.onNull ? ' ON NULL' : '';
      thisStr = expression.args.this ? ' ALWAYS' : ` BY DEFAULT${onNull}`;
    }

    const start = expression.args.start;
    const startStr = start ? `START WITH ${start}` : '';
    const increment = expression.args.increment;
    const incrementStr = increment ? ` INCREMENT BY ${increment}` : '';
    const minvalue = expression.args.minvalue;
    const minvalueStr = minvalue ? ` MINVALUE ${minvalue}` : '';
    const maxvalue = expression.args.maxvalue;
    const maxvalueStr = maxvalue ? ` MAXVALUE ${maxvalue}` : '';
    const cycle = expression.args.cycle;
    let cycleSql = '';

    if (cycle !== undefined) {
      cycleSql = `${!cycle ? ' NO' : ''} CYCLE`;
      cycleSql = !startStr && !incrementStr ? cycleSql.trim() : cycleSql;
    }

    let sequenceOpts = '';
    if (startStr || incrementStr || cycleSql) {
      sequenceOpts = `${startStr}${incrementStr}${minvalueStr}${maxvalueStr}${cycleSql}`;
      sequenceOpts = ` (${sequenceOpts.trim()})`;
    }

    let expr = this.sql(expression, 'expression');
    expr = expr ? `(${expr})` : 'IDENTITY';

    return `GENERATED${thisStr} AS ${expr}${sequenceOpts}`;
  }

  generatedAsRowColumnConstraintSql (expression: GeneratedAsRowColumnConstraintExpr): string {
    const start = expression.args.start ? 'START' : 'END';
    const hidden = expression.args.hidden ? ' HIDDEN' : '';
    return `GENERATED ALWAYS AS ROW ${start}${hidden}`;
  }

  periodForSystemTimeConstraintSql (expression: PeriodForSystemTimeConstraintExpr): string {
    return `PERIOD FOR SYSTEM_TIME (${this.sql(expression, 'this')}, ${this.sql(expression, 'expression')})`;
  }

  notNullColumnConstraintSql (expression: NotNullColumnConstraintExpr): string {
    return expression.args.allowNull ? 'NULL' : 'NOT NULL';
  }

  primaryKeyColumnConstraintSql (expression: PrimaryKeyColumnConstraintExpr): string {
    const desc = expression.args.desc;
    if (desc !== undefined) {
      return `PRIMARY KEY${desc ? ' DESC' : ' ASC'}`;
    }
    let options = this.expressions(expression, {
      key: 'options',
      flat: true,
      sep: ' ',
    });
    options = options ? ` ${options}` : '';
    return `PRIMARY KEY${options}`;
  }

  uniqueColumnConstraintSql (expression: UniqueColumnConstraintExpr): string {
    let thisStr = this.sql(expression, 'this');
    thisStr = thisStr ? ` ${thisStr}` : '';
    const indexType = expression.args.indexType;
    const indexTypeStr = indexType ? ` USING ${indexType}` : '';
    let onConflict = this.sql(expression, 'onConflict');
    onConflict = onConflict ? ` ${onConflict}` : '';
    const nullsSql = expression.args.nulls ? ' NULLS NOT DISTINCT' : '';
    let options = this.expressions(expression, {
      key: 'options',
      flat: true,
      sep: ' ',
    });
    options = options ? ` ${options}` : '';
    return `UNIQUE${nullsSql}${thisStr}${indexTypeStr}${onConflict}${options}`;
  }

  inOutColumnConstraintSql (expression: InOutColumnConstraintExpr): string {
    const input = expression.args.input;
    const output = expression.args.output;
    const variadic = expression.args.variadic;

    // VARIADIC is mutually exclusive with IN/OUT/INOUT
    if (variadic) {
      return 'VARIADIC';
    }

    if (input && output) {
      return `IN${this._constructor.INOUT_SEPARATOR}OUT`;
    }
    if (input) {
      return 'IN';
    }
    if (output) {
      return 'OUT';
    }

    return '';
  }

  createableSql (expression: Expression, _locations: unknown): string {
    return this.sql(expression, 'this');
  }

  createSql (expression: CreateExpr): string {
    let kind = this.sql(expression, 'kind');
    kind = this.dialect._constructor.INVERSE_CREATABLE_KIND_MAPPING[kind] ?? kind;

    const properties = expression.args.properties;
    if (properties) {
      assertIsInstanceOf(properties, PropertiesExpr);
    }
    const propertiesLocs = properties ? this.locateProperties(properties) : new Map<string, Expression[]>();

    const thisSql = this.createableSql(expression, propertiesLocs);

    let propertiesSql = '';
    const postSchema = propertiesLocs.get(PropertiesLocation.POST_SCHEMA);
    const postWith = propertiesLocs.get(PropertiesLocation.POST_WITH);

    if (postSchema || postWith) {
      const propsAst = new PropertiesExpr({
        expressions: [...(postSchema ?? []), ...(postWith ?? [])],
      });
      propsAst.parent = expression;
      propertiesSql = this.sql(propsAst);

      if (postSchema) {
        propertiesSql = this.sep() + propertiesSql;
      } else if (!this.pretty) {
        propertiesSql = ` ${propertiesSql}`;
      }
    }

    const begin = expression.args.begin ? ' BEGIN' : '';
    const end = expression.args.end ? ' END' : '';

    let expressionSql = this.sql(expression, 'expression');
    if (expressionSql) {
      expressionSql = `${begin}${this.sep()}${expressionSql}${end}`;

      if (this._constructor.CREATE_FUNCTION_RETURN_AS || !(expression.args.expression instanceof ReturnExpr)) {
        let postaliasPropsSql = '';
        const postAlias = propertiesLocs.get(PropertiesLocation.POST_ALIAS);
        if (postAlias) {
          postaliasPropsSql = this.properties(
            new PropertiesExpr({ expressions: postAlias }),
            { wrapped: false },
          );
        }
        postaliasPropsSql = postaliasPropsSql ? ` ${postaliasPropsSql}` : '';
        expressionSql = ` AS${postaliasPropsSql}${expressionSql}`;
      }
    }

    let postindexPropsSql = '';
    const postIndex = propertiesLocs.get(PropertiesLocation.POST_INDEX);
    if (postIndex) {
      postindexPropsSql = this.properties(
        new PropertiesExpr({ expressions: postIndex }),
        {
          wrapped: false,
          prefix: ' ',
        },
      );
    }

    let indexes = this.expressions(expression, {
      key: 'indexes',
      indent: false,
      sep: ' ',
    });
    indexes = indexes ? ` ${indexes}` : '';
    const indexSql = indexes + postindexPropsSql;

    const replace = expression.args.replace ? ' OR REPLACE' : '';
    const refresh = expression.args.refresh ? ' OR REFRESH' : '';
    const unique = expression.args.unique ? ' UNIQUE' : '';

    const clustered = expression.args.clustered;
    let clusteredSql = '';
    if (clustered === undefined) {
      clusteredSql = '';
    } else if (clustered) {
      clusteredSql = ' CLUSTERED COLUMNSTORE';
    } else {
      clusteredSql = ' NONCLUSTERED COLUMNSTORE';
    }

    let postcreatePropsSql = '';
    const postCreate = propertiesLocs.get(PropertiesLocation.POST_CREATE);
    if (postCreate) {
      postcreatePropsSql = this.properties(
        new PropertiesExpr({ expressions: postCreate }),
        {
          sep: ' ',
          prefix: ' ',
          wrapped: false,
        },
      );
    }

    const modifiers = `${clusteredSql}${replace}${refresh}${unique}${postcreatePropsSql}`;

    let postexpressionPropsSql = '';
    const postExpression = propertiesLocs.get(PropertiesLocation.POST_EXPRESSION);
    if (postExpression) {
      postexpressionPropsSql = this.properties(
        new PropertiesExpr({ expressions: postExpression }),
        {
          sep: ' ',
          prefix: ' ',
          wrapped: false,
        },
      );
    }

    const concurrently = expression.args.concurrently ? ' CONCURRENTLY' : '';
    const existsSql = expression.args.exists ? ' IF NOT EXISTS' : '';
    const noSchemaBinding = expression.args.noSchemaBinding ? ' WITH NO SCHEMA BINDING' : '';

    let clone = this.sql(expression, 'clone');
    clone = clone ? ` ${clone}` : '';

    let propertiesExpression: string;
    if (this._constructor.EXPRESSION_PRECEDES_PROPERTIES_CREATABLES.has(kind)) {
      propertiesExpression = `${expressionSql}${propertiesSql}`;
    } else {
      propertiesExpression = `${propertiesSql}${expressionSql}`;
    }

    expressionSql = `CREATE${modifiers} ${kind}${concurrently}${existsSql} ${thisSql}${propertiesExpression}${postexpressionPropsSql}${indexSql}${noSchemaBinding}${clone}`;

    return this.prependCtes(expression, expressionSql);
  }

  sequencePropertiesSql (expression: SequencePropertiesExpr): string {
    let start = this.sql(expression, 'start');
    start = start ? `START WITH ${start}` : '';
    let increment = this.sql(expression, 'increment');
    increment = increment ? ` INCREMENT BY ${increment}` : '';
    let minvalue = this.sql(expression, 'minvalue');
    minvalue = minvalue ? ` MINVALUE ${minvalue}` : '';
    let maxvalue = this.sql(expression, 'maxvalue');
    maxvalue = maxvalue ? ` MAXVALUE ${maxvalue}` : '';
    let owned = this.sql(expression, 'owned');
    owned = owned ? ` OWNED BY ${owned}` : '';

    const cache = expression.args.cache;
    let cacheStr = '';
    if (cache === undefined) {
      cacheStr = '';
    } else if (cache === true) {
      cacheStr = ' CACHE';
    } else {
      cacheStr = ` CACHE ${cache}`;
    }

    const options = this.expressions(expression, {
      key: 'options',
      flat: true,
      sep: ' ',
    });
    const optionsPart = options ? ` ${options}` : '';

    return `${start}${increment}${minvalue}${maxvalue}${cacheStr}${optionsPart}${owned}`.trimStart();
  }

  cloneSql (expression: CloneExpr): string {
    const thisStr = this.sql(expression, 'this');
    const shallow = expression.args.shallow ? 'ShaLLOW ' : '';
    const keyword = (expression.args.copy && this._constructor.SUPPORTS_TABLE_COPY) ? 'COPY' : 'CLONE';
    return `${shallow}${keyword} ${thisStr}`;
  }

  describeSql (expression: DescribeExpr): string {
    const style = expression.args.style;
    const stylePart = style ? ` ${style}` : '';
    let partition = this.sql(expression, 'partition');
    partition = partition ? ` ${partition}` : '';
    let format = this.sql(expression, 'format');
    format = format ? ` ${format}` : '';
    const asJson = expression.args.asJson ? ' AS JSON' : '';

    return `DESCRIBE${stylePart}${format} ${this.sql(expression, 'this')}${partition}${asJson}`;
  }

  heredocSql (expression: HeredocExpr): string {
    const tag = this.sql(expression, 'tag');
    return `$${tag}$${this.sql(expression, 'this')}$${tag}$`;
  }

  protected prependCtes (expression: Expression, sql: string): string {
    const withSql = this.sql(expression, 'with');
    if (withSql) {
      return `${withSql}${this.sep()}${sql}`;
    }
    return sql;
  }

  withSql (expression: WithExpr): string {
    const sql = this.expressions(expression, { flat: true });
    const recursive = (this._constructor.CTE_RECURSIVE_KEYWORD_REQUIRED && expression.args.recursive)
      ? 'RECURSIVE '
      : '';
    let search = this.sql(expression, 'search');
    search = search ? ` ${search}` : '';

    return `WITH ${recursive}${sql}${search}`;
  }

  cteSql (expression: CteExpr): string {
    const alias = expression.args.alias;
    if (alias) {
      alias.addComments?.(expression.popComments());
    }

    const aliasSql = this.sql(expression, 'alias');

    const materialized = expression.args.materialized;
    let materializedStr = '';
    if (materialized === false) {
      materializedStr = 'NOT MATERIALIZED ';
    } else if (materialized) {
      materializedStr = 'MATERIALIZED ';
    }

    const keyExpressions = this.expressions(expression, {
      key: 'keyExpressions',
      flat: true,
    });
    const keyPart = keyExpressions ? ` USING KEY (${keyExpressions})` : '';

    return `${aliasSql}${keyPart} AS ${materializedStr}${this.wrap(expression)}`;
  }

  tableAliasSql (expression: TableAliasExpr): string {
    let alias = this.sql(expression, 'this');
    let columns = this.expressions(expression, {
      key: 'columns',
      flat: true,
    });
    columns = columns ? `(${columns})` : '';

    if (columns && !this._constructor.SUPPORTS_TABLE_ALIAS_COLUMNS) {
      columns = '';
      this.unsupported('Named columns are not supported in table alias.');
    }

    if (!alias && !this.dialect._constructor.UNNEST_COLUMN_ONLY) {
      alias = this.nextName();
    }

    return `${alias}${columns}`;
  }

  bitStringSql (expression: BitStringExpr): string {
    const thisStr = this.sql(expression, 'this');
    if (this.dialect._constructor.BIT_START) {
      return `${this.dialect._constructor.BIT_START}${thisStr}${this.dialect._constructor.BIT_END}`;
    }
    return `${parseInt(thisStr, 2)}`;
  }

  hexStringSql (expression: HexStringExpr, options: { binaryFunctionRepr?: string } = {}): string {
    const { binaryFunctionRepr } = options;
    const thisStr = this.sql(expression, 'this');
    const isIntegerType = expression.args.isInteger;

    if ((isIntegerType && !this.dialect._constructor.HEX_STRING_IS_INTEGER_TYPE)
      || (!this.dialect._constructor.HEX_START && !binaryFunctionRepr)) {
      return `${parseInt(thisStr, 16)}`;
    }

    if (!isIntegerType) {
      if (binaryFunctionRepr) {
        return this.func(binaryFunctionRepr, [literal(thisStr)]);
      }
      if (this.dialect._constructor.HEX_STRING_IS_INTEGER_TYPE) {
        this.unsupported('Unsupported transpilation from BINARY/BLOB hex string');
      }
    }

    return `${this.dialect._constructor.HEX_START}${thisStr}${this.dialect._constructor.HEX_END}`;
  }

  byteStringSql (expression: ByteStringExpr): string {
    const thisStr = this.sql(expression, 'this');
    if (this.dialect._constructor.BYTE_START) {
      const escapedByteString = this.escapeStr(
        thisStr,
        {
          escapeBackslash: false,
          delimiter: this.dialect._constructor.BYTE_END,
          escapedDelimiter: this.escapedByteQuoteEnd,
          isByteString: true,
        },
      );
      const isBytes = expression.args.isBytes || false;
      const delimitedByteString = `${this.dialect._constructor.BYTE_START}${escapedByteString}${this.dialect._constructor.BYTE_END}`;

      if (isBytes && !this.dialect._constructor.BYTE_STRING_IS_BYTES_TYPE) {
        return this.sql(cast(delimitedByteString, 'BINARY', { dialect: this.dialect }));
      }
      if (!isBytes && this.dialect._constructor.BYTE_STRING_IS_BYTES_TYPE) {
        return this.sql(cast(delimitedByteString, 'VARCHAR', { dialect: this.dialect }));
      }

      return delimitedByteString;
    }
    return thisStr;
  }

  unicodeStringSql (expression: UnicodeStringExpr): string {
    const thisSql = this.sql(expression, 'this');
    const escape = expression.args.escape;

    let escapeSubstitute: string;
    let leftQuote: string;
    let rightQuote: string | undefined;

    if (this.dialect._constructor.UNICODE_START) {
      escapeSubstitute = '\\$1';
      leftQuote = this.dialect._constructor.UNICODE_START;
      rightQuote = this.dialect._constructor.UNICODE_END;
    } else {
      escapeSubstitute = '\\u$1';
      leftQuote = this.dialect._constructor.QUOTE_START;
      rightQuote = this.dialect._constructor.QUOTE_END;
    }

    let escapePattern: RegExp;
    let escapeSql: string;

    if (escape) {
      escapePattern = new RegExp(`${escape.name}(\\d+)`, 'g');
      escapeSql = this._constructor.SUPPORTS_UESCAPE ? ` UESCAPE ${this.sql(escape)}` : '';
    } else {
      escapePattern = ESCAPED_UNICODE_RE;
      escapeSql = '';
    }

    let resultThis = thisSql;
    if (!this.dialect._constructor.UNICODE_START || (escape && !this._constructor.SUPPORTS_UESCAPE)) {
      const replacement = this._constructor.UNICODE_SUBSTITUTE || escapeSubstitute;
      if (typeof replacement === 'function') {
        resultThis = resultThis.replace(escapePattern, replacement);
      } else {
        resultThis = resultThis.replace(escapePattern, replacement);
      }
    }

    return `${leftQuote}${resultThis}${rightQuote}${escapeSql}`;
  }

  rawStringSql (expression: RawStringExpr): string {
    let string = expression.args.this?.toString() || '';

    if (this.dialect._constructor.tokenizerClass.STRING_ESCAPES.includes('\\')) {
      string = string?.replace(/\\/g, '\\\\');
    }

    string = this.escapeStr(string, { escapeBackslash: false });
    return `${this.dialect._constructor.QUOTE_START}${string}${this.dialect._constructor.QUOTE_END}`;
  }

  dataTypeParamSql (expression: DataTypeParamExpr): string {
    const thisStr = this.sql(expression, 'this');
    const specifier = this.sql(expression, 'expression');
    const specifierStr = specifier && this._constructor.DATA_TYPE_SPECIFIERS_ALLOWED
      ? ` ${specifier}`
      : '';
    return `${thisStr}${specifierStr}`;
  }

  dataTypeSql (expression: DataTypeExpr): string {
    let nested = '';
    let values = '';

    const exprNested = expression.args.nested;
    const interior = exprNested && this.pretty
      ? this.expressions(expression, {
        dynamic: true,
        newLine: true,
        skipFirst: true,
        skipLast: true,
      })
      : this.expressions(expression, { flat: true });

    const typeValue = expression.args.this as DataTypeExprKind;
    if (this._constructor.UNSUPPORTED_TYPES.has(typeValue)) {
      this.unsupported(`Data type ${typeValue} is not supported when targeting ${this.dialect._constructor.name}`);
    }

    let typeSql: string;
    if (typeValue === DataTypeExprKind.USERDEFINED && expression.args.kind) {
      typeSql = this.sql(expression, 'kind');
    } else {
      const typeMapping = this._constructor.TYPE_MAPPING.get(typeValue);
      typeSql = typeMapping !== undefined ? typeMapping : typeValue;
    }

    if (interior) {
      if (exprNested) {
        const structDelim = this._constructor.STRUCT_DELIMITER;
        nested = `${structDelim[0]}${interior}${structDelim[1]}`;
        if (expression.args.values !== undefined) {
          const delimiters = typeValue === DataTypeExprKind.ARRAY ? ['[', ']'] : ['(', ')'];
          const valuesStr = this.expressions(expression, {
            key: 'values',
            flat: true,
          });
          values = `${delimiters[0]}${valuesStr}${delimiters[1]}`;
        }
      } else if (typeValue === DataTypeExprKind.INTERVAL) {
        nested = ` ${interior}`;
      } else {
        nested = `(${interior})`;
      }
    }

    typeSql = `${typeSql}${nested}${values}`;
    if (
      this._constructor.TZ_TO_WITH_TIME_ZONE
      && (typeValue === DataTypeExprKind.TIMETZ || typeValue === DataTypeExprKind.TIMESTAMPTZ)) {
      typeSql = `${typeSql} WITH TIME ZONE`;
    }

    return typeSql;
  }

  directorySql (expression: DirectoryExpr): string {
    const local = expression.args.local ? 'LOCAL ' : '';
    let rowFormat = this.sql(expression, 'rowFormat');
    rowFormat = rowFormat ? ` ${rowFormat}` : '';
    return `${local}DIRECTORY ${this.sql(expression, 'this')}${rowFormat}`;
  }

  deleteSql (expression: DeleteExpr): string {
    const thisStr = this.sql(expression, 'this');
    const thisClause = thisStr ? ` FROM ${thisStr}` : '';
    let using = this.expressions(expression, { key: 'using' });
    using = using ? ` USING ${using}` : '';
    let cluster = this.sql(expression, 'cluster');
    cluster = cluster ? ` ${cluster}` : '';
    const where = this.sql(expression, 'where');
    const returning = this.sql(expression, 'returning');
    const order = this.sql(expression, 'order');
    const limit = this.sql(expression, 'limit');
    let tables = this.expressions(expression, { key: 'tables' });
    tables = tables ? ` ${tables}` : '';
    let expressionSql: string;
    if (this._constructor.RETURNING_END) {
      expressionSql = `${thisClause}${using}${cluster}${where}${returning}${order}${limit}`;
    } else {
      expressionSql = `${returning}${thisClause}${using}${cluster}${where}${order}${limit}`;
    }
    return this.prependCtes(expression, `DELETE${tables}${expressionSql}`);
  }

  dropSql (expression: DropExpr): string {
    const thisStr = this.sql(expression, 'this');
    let expressions = this.expressions(expression, { flat: true });
    expressions = expressions ? ` (${expressions})` : '';
    const kind = expression.args.kind?.toString();
    const kindStr = kind ? this.dialect._constructor.INVERSE_CREATABLE_KIND_MAPPING?.[kind] || kind : kind;
    const existsSql = expression.args.exists ? ' IF EXISTS ' : ' ';
    const concurrentlySql = expression.args.concurrently ? ' CONCURRENTLY' : '';
    let onCluster = this.sql(expression, 'cluster');
    onCluster = onCluster ? ` ${onCluster}` : '';
    const temporary = expression.args.temporary ? ' TEMPORARY' : '';
    const materialized = expression.args.materialized ? ' MATERIALIZED' : '';
    const cascade = expression.args.cascade ? ' CASCADE' : '';
    const constraints = expression.args.constraints ? ' CONSTRAINTS' : '';
    const purge = expression.args.purge ? ' PURGE' : '';
    return `DROP${temporary}${materialized} ${kindStr}${concurrentlySql}${existsSql}${thisStr}${onCluster}${expressions}${cascade}${constraints}${purge}`;
  }

  setOperation (expression: SetOperationExpr): string {
    const opType = expression._constructor;
    const opName = opType.key.toUpperCase();

    let distinct = expression.args.distinct;
    if (
      distinct === false
      && (expression instanceof ExceptExpr || expression instanceof IntersectExpr)
      && !this._constructor.EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE
    ) {
      this.unsupported(`${opName} ALL is not supported`);
    }

    const defaultDistinct = this.dialect._constructor.SET_OP_DISTINCT_BY_DEFAULT?.[opType.key];

    if (distinct === undefined) {
      distinct = defaultDistinct;
      if (distinct === undefined) {
        this.unsupported(`${opName} requires DISTINCT or ALL to be specified`);
      }
    }

    let distinctOrAll: string;
    if (distinct === defaultDistinct) {
      distinctOrAll = '';
    } else {
      distinctOrAll = distinct ? ' DISTINCT' : ' ALL';
    }

    const sideKind = [expression.args.side, expression.args.kind].filter(Boolean).join(' ');
    const sideKindStr = sideKind ? `${sideKind} ` : '';

    const byName = expression.args.byName ? ' BY NAME' : '';
    let on = this.expressions(expression, {
      key: 'on',
      flat: true,
    });
    on = on ? ` ON (${on})` : '';

    return `${sideKindStr}${opName}${distinctOrAll}${byName}${on}`;
  }

  setOperations (expression: SetOperationExpr): string {
    if (!this._constructor.SET_OP_MODIFIERS) {
      const limit = expression.args.limit;
      const order = expression.args.order;

      if (limit || order) {
        const selectExpr = this.moveCtesToTopLevel(
          subquery(expression, '_l_0', { copy: false }).select('*', { copy: false }),
        );

        let select = selectExpr;

        if (limit) {
          select = select.limit(typeof limit === 'number' ? limit : limit.pop(), { copy: false });
        }

        if (order) {
          select = select.orderBy(order.pop(), { copy: false });
        }

        return this.sql(select);
      }
    }

    const sqls: string[] = [];
    const stack: (string | Expression)[] = [expression];

    while (0 < stack.length) {
      const node = stack.pop();

      if (node instanceof SetOperationExpr) {
        if (node.args.expression) stack.push(node.args.expression);
        stack.push(
          this.maybeComment(
            this.setOperation(node),
            node,
            node.comments,
            true,
          ),
        );
        if (node.args.this) stack.push(node.args.this);
      } else {
        const nodeStr = typeof node === 'string' ? node : this.sql(node);
        sqls.push(nodeStr);
      }
    }

    let self = sqls.join(this.sep());
    self = this.queryModifiers(expression, self);
    return this.prependCtes(expression, self);
  }

  fetchSql (expression: FetchExpr): string {
    const direction = expression.args.direction;
    const directionStr = direction ? ` ${direction}` : '';
    const count = this.sql(expression, 'count');
    const countStr = count ? ` ${count}` : '';
    const limitOptions = this.sql(expression, 'limitOptions');
    const limitOptionsStr = limitOptions ? `${limitOptions}` : ' ROWS ONLY';
    return `${this.seg('FETCH')}${directionStr}${countStr}${limitOptionsStr}`;
  }

  limitOptionsSql (expression: LimitOptionsExpr): string {
    const percent = expression.args.percent ? ' PERCENT' : '';
    const rows = expression.args.rows ? ' ROWS' : '';
    let withTies = expression.args.withTies ? ' WITH TIES' : '';

    if (!withTies && rows) {
      withTies = ' ONLY';
    }

    return `${percent}${rows}${withTies}`;
  }

  filterSql (expression: FilterExpr): string {
    const dialect = this._constructor;
    if (dialect.AGGREGATE_FILTER_SUPPORTED) {
      const thisStr = this.sql(expression, 'this');
      const where = this.sql(expression, 'expression').trim();
      return `${thisStr} FILTER(${where})`;
    }

    const agg = expression.args.this;
    const aggArg = agg ? agg.args.this : undefined;
    const cond = expression.args.expression?.args.this;

    if (aggArg instanceof Expression && cond instanceof Expression) {
      aggArg.replace(new IfExpr({
        this: cond.copy(),
        true: aggArg.copy(),
      }));
    }
    return this.sql(agg);
  }

  hintSql (expression: HintExpr): string {
    if (!this._constructor.QUERY_HINTS) {
      this.unsupported('Hints are not supported');
      return '';
    }
    return ` /*+ ${this.expressions(expression, { sep: this._constructor.QUERY_HINT_SEP }).trim()} */`;
  }

  indexParametersSql (expression: IndexParametersExpr): string {
    let using = this.sql(expression, 'using');
    using = using ? ` USING ${using}` : '';
    const columns = this.expressions(expression, {
      key: 'columns',
      flat: true,
    });
    const columnsStr = columns ? `(${columns})` : '';
    const partitionByRaw = this.expressions(expression, {
      key: 'partitionBy',
      flat: true,
    });
    const partitionBy = partitionByRaw ? ` PARTITION BY ${partitionByRaw}` : '';
    const where = this.sql(expression, 'where');
    let include = this.expressions(expression, {
      key: 'include',
      flat: true,
    });
    if (include) {
      include = ` INCLUDE (${include})`;
    }
    const withStorageRaw = this.expressions(expression, {
      key: 'withStorage',
      flat: true,
    });
    const withStorage = withStorageRaw ? ` WITH (${withStorageRaw})` : '';
    const tablespaceRaw = this.sql(expression, 'tablespace');
    const tablespace = tablespaceRaw ? ` USING INDEX TABLESPACE ${tablespaceRaw}` : '';
    const onRaw = this.sql(expression, 'on');
    const on = onRaw ? ` ON ${onRaw}` : '';

    return `${using}${columnsStr}${include}${withStorage}${tablespace}${partitionBy}${where}${on}`;
  }

  indexSql (expression: IndexExpr): string {
    const unique = expression.args.unique ? 'UNIQUE ' : '';
    const primary = expression.args.primary ? 'PRIMARY ' : '';
    const amp = expression.args.amp ? 'AMP ' : '';

    let name = this.sql(expression, 'this');
    name = name ? `${name} ` : '';

    let table = this.sql(expression, 'table');
    table = table ? `${this._constructor.INDEX_ON} ${table}` : '';

    const index = !table ? 'INDEX ' : '';

    const params = this.sql(expression, 'params');

    return `${unique}${primary}${amp}${index}${name}${table}${params}`;
  }

  identifierSql (expression: IdentifierExpr): string {
    let text = expression.name;
    const lower = text.toLowerCase();
    text = this.normalize && !expression.args.quoted ? lower : text;
    text = text.replaceAll(this.identifierEnd, this.escapedIdentifierEnd);

    if (
      expression.args.quoted
      || this.dialect.canQuote(expression, { identify: this.identify })
      || this._constructor.RESERVED_KEYWORDS.has(lower)
      || (!this.dialect._constructor.IDENTIFIERS_CAN_START_WITH_DIGIT && /^\d/.test(text))
    ) {
      text = `${this.identifierStart}${text}${this.identifierEnd}`;
    }
    return text;
  }

  hexSql (expression: HexExpr): string {
    const hexFunc = this._constructor.HEX_FUNC;
    let text = this.func(hexFunc, [this.sql(expression, 'this')]);
    if (this.dialect._constructor.HEX_LOWERCASE) {
      text = this.func('LOWER', [text]);
    }
    return text;
  }

  lowerHexSql (expression: LowerHexExpr): string {
    const hexFunc = this._constructor.HEX_FUNC;
    let text = this.func(hexFunc, [this.sql(expression, 'this')]);
    if (!this.dialect._constructor.HEX_LOWERCASE) {
      text = this.func('LOWER', [text]);
    }
    return text;
  }

  inputOutputFormatSql (expression: InputOutputFormatExpr): string {
    const inputFormat = this.sql(expression, 'inputFormat');
    const inputFormatStr = inputFormat ? `INPUTFORMAT ${inputFormat}` : '';
    const outputFormat = this.sql(expression, 'outputFormat');
    const outputFormatStr = outputFormat ? `OUTPUTFORMAT ${outputFormat}` : '';
    return [inputFormatStr, outputFormatStr].join(this.sep());
  }

  nationalSql (expression: NationalExpr, options: { prefix?: string } = {}): string {
    const { prefix = 'N' } = options;
    const string = this.sql(literal(expression.name));
    return `${prefix}${string}`;
  }

  partitionSql (expression: PartitionExpr): string {
    const partitionKeyword = expression.args.subpartition ? 'SUBPARTITION' : 'PARTITION';
    return `${partitionKeyword}(${this.expressions(expression, { flat: true })})`;
  }

  propertiesSql (expression: PropertiesExpr): string {
    const rootProperties = [];
    const withProperties = [];

    for (const p of expression.args.expressions || []) {
      const pLoc = this._constructor.PROPERTIES_LOCATION.get(p._constructor);

      if (pLoc === PropertiesLocation.POST_WITH) {
        withProperties.push(p);
      } else if (pLoc === PropertiesLocation.POST_SCHEMA) {
        rootProperties.push(p);
      }
    }

    const rootPropsAst = new PropertiesExpr({ expressions: rootProperties });
    rootPropsAst.parent = expression.parent;

    const withPropsAst = new PropertiesExpr({ expressions: withProperties });
    withPropsAst.parent = expression.parent;

    const rootProps = this.rootProperties(rootPropsAst);
    let withProps = this.withProperties(withPropsAst);

    if (rootProps && withProps && !this.pretty) {
      withProps = ` ${withProps}`;
    }

    return rootProps + withProps;
  }

  rootProperties (properties: PropertiesExpr): string {
    if (0 < (properties.args.expressions || []).length) {
      return this.expressions(properties, {
        indent: false,
        sep: ' ',
      });
    }
    return '';
  }

  properties (
    properties: PropertiesExpr,
    options: {
      prefix?: string;
      sep?: string;
      suffix?: string;
      wrapped?: boolean;
    } = {},
  ): string {
    const {
      prefix = '',
      sep = ', ',
      suffix = '',
      wrapped = true,
    } = options;
    if (0 < (properties.args.expressions || []).length) {
      const expressions = this.expressions(properties, {
        sep,
        indent: false,
      });
      if (expressions) {
        const wrappedExpr = wrapped ? this.wrap(expressions) : expressions;
        return `${prefix}${prefix.trim() ? ' ' : ''}${wrappedExpr}${suffix}`;
      }
    }
    return '';
  }

  withProperties (properties: PropertiesExpr): string {
    return this.properties(properties, {
      prefix: this.seg(this._constructor.WITH_PROPERTIES_PREFIX, ''),
      sep: '',
    });
  }

  locateProperties (properties: PropertiesExpr): Map<PropertiesLocation, Expression[]> {
    const propertiesLocs = new Map<PropertiesLocation, Expression[]>();
    const expressions = properties.args.expressions || [];
    for (const p of expressions) {
      const pLoc = this._constructor.PROPERTIES_LOCATION.get(p._constructor) || PropertiesLocation.UNSUPPORTED;
      if (pLoc !== PropertiesLocation.UNSUPPORTED) {
        if (!propertiesLocs.has(pLoc)) {
          propertiesLocs.set(pLoc, []);
        }
        propertiesLocs.get(pLoc)?.push(p);
      } else {
        this.unsupported(`Unsupported property ${p._constructor.key}`);
      }
    }
    return propertiesLocs;
  }

  propertyName (
    expression: PropertyExpr,
    options: {
      stringKey?: boolean;
    } = {},
  ): string {
    const { stringKey = false } = options;

    if (expression.args.this instanceof DotExpr) {
      return this.sql(expression, 'this');
    }
    return stringKey ? `'${expression.name}'` : expression.name;
  }

  propertySql (expression: PropertyExpr): string {
    const propertyCls = expression._constructor;
    if (propertyCls === PropertyExpr) {
      return `${this.propertyName(expression)}=${this.sql(expression, 'value')}`;
    }

    const propertyName = PropertiesExpr.PROPERTY_TO_NAME?.[propertyCls.key];
    if (!propertyName) {
      this.unsupported(`Unsupported property ${expression._constructor.key}`);
    }

    return `${propertyName}=${this.sql(expression, 'this')}`;
  }

  likePropertySql (expression: LikePropertyExpr): string {
    if (this._constructor.SUPPORTS_CREATE_TABLE_LIKE) {
      const options = (expression.args.expressions || [])
        .map((e) => `${e.name} ${this.sql(e, 'value')}`)
        .join(' ');
      const optionsStr = options ? ` ${options}` : '';
      return `LIKE ${this.sql(expression, 'this')}${optionsStr}`;
    }
    return this.propertySql(expression);
  }

  fallbackPropertySql (expression: FallbackPropertyExpr): string {
    const no = expression.args.no ? 'NO ' : '';
    const protection = expression.args.protection ? ' PROTECTION' : '';
    return `${no}FALLBACK${protection}`;
  }

  journalPropertySql (expression: JournalPropertyExpr): string {
    const no = expression.args.no ? 'NO ' : '';
    const local = expression.args.local;
    const localStr = local ? `${local} ` : '';
    const dual = expression.args.dual ? 'DUAL ' : '';
    const before = expression.args.before ? 'BEFORE ' : '';
    const after = expression.args.after ? 'AFTER ' : '';
    return `${no}${localStr}${dual}${before}${after}JOURNAL`;
  }

  freeSpacePropertySql (expression: FreespacePropertyExpr): string {
    const freespace = this.sql(expression, 'this');
    const percent = expression.args.percent ? ' PERCENT' : '';
    return `FREESPACE=${freespace}${percent}`;
  }

  checksumPropertySql (expression: ChecksumPropertyExpr): string {
    let property: string;
    if (expression.args.default) {
      property = 'DEFAULT';
    } else if (expression.args.on) {
      property = 'ON';
    } else {
      property = 'OFF';
    }
    return `CHECKSUM=${property}`;
  }

  mergeBlockRatioPropertySql (expression: MergeBlockRatioPropertyExpr): string {
    if (expression.args.no) {
      return 'NO MERGEBLOCKRATIO';
    }
    if (expression.args.default) {
      return 'DEFAULT MERGEBLOCKRATIO';
    }
    const percent = expression.args.percent ? ' PERCENT' : '';
    return `MERGEBLOCKRATIO=${this.sql(expression, 'this')}${percent}`;
  }

  dataBlockSizePropertySql (expression: DataBlocksizePropertyExpr): string {
    const defaultVal = expression.args.default;
    const minimum = expression.args.minimum;
    const maximum = expression.args.maximum;
    if (defaultVal || minimum || maximum) {
      let prop: string;
      if (defaultVal) {
        prop = 'DEFAULT';
      } else if (minimum) {
        prop = 'MINIMUM';
      } else {
        prop = 'MAXIMUM';
      }
      return `${prop} DATABLOCKSIZE`;
    }
    const units = expression.args.units;
    const unitsStr = units ? ` ${units}` : '';
    return `DATABLOCKSIZE=${this.sql(expression, 'size')}${unitsStr}`;
  }

  blockCompressionPropertySql (expression: BlockCompressionPropertyExpr): string {
    const autotemp = expression.args.autotemp;
    const always = expression.args.always;
    const defaultVal = expression.args.default;
    const manual = expression.args.manual;
    const never = expression.args.never;

    let prop: string;
    if (autotemp !== undefined) {
      prop = `AUTOTEMP ${this.sql(autotemp)}`;
    } else if (always) {
      prop = 'ALWAYS';
    } else if (defaultVal) {
      prop = 'DEFAULT';
    } else if (manual) {
      prop = 'MANUAL';
    } else if (never) {
      prop = 'NEVER';
    } else {
      prop = '';
    }
    return `BLOCKCOMPRESSION=${prop}`;
  }

  isolatedLoadingPropertySql (expression: IsolatedLoadingPropertyExpr): string {
    const no = expression.args.no;
    const noStr = no ? ' NO' : '';
    const concurrent = expression.args.concurrent;
    const concurrentStr = concurrent ? ' CONCURRENT' : '';
    let target = this.sql(expression, 'target');
    target = target ? ` ${target}` : '';
    return `WITH${noStr}${concurrentStr} ISOLATED LOADING${target}`;
  }

  partitionBoundSpecSql (expression: PartitionBoundSpecExpr): string {
    if (Array.isArray(expression.args.this)) {
      return `IN (${this.expressions(expression, {
        key: 'this',
        flat: true,
      })})`;
    }
    if (expression.args.this) {
      const modulus = this.sql(expression, 'this');
      const remainder = this.sql(expression, 'expression');
      return `WITH (MODULUS ${modulus}, REMAINDER ${remainder})`;
    }

    const fromExpressions = this.expressions(expression, {
      key: 'fromExpressions',
      flat: true,
    });
    const toExpressions = this.expressions(expression, {
      key: 'toExpressions',
      flat: true,
    });
    return `FROM (${fromExpressions}) TO (${toExpressions})`;
  }

  partitionedOfPropertySql (expression: PartitionedOfPropertyExpr): string {
    const thisStr = this.sql(expression, 'this');

    let forValuesOrDefault: Expression | string = expression.args.expression ?? '';
    if (forValuesOrDefault instanceof PartitionBoundSpecExpr) {
      forValuesOrDefault = ` FOR VALUES ${this.sql(forValuesOrDefault)}`;
    } else {
      forValuesOrDefault = ' DEFAULT';
    }

    return `PARTITION OF ${thisStr}${forValuesOrDefault}`;
  }

  lockingPropertySql (expression: LockingPropertyExpr): string {
    const kind = expression.args.kind;
    const thisStr = expression.args.this ? ` ${this.sql(expression, 'this')}` : '';
    const forOrIn = expression.args.forOrIn;
    const forOrInStr = forOrIn ? ` ${forOrIn}` : '';
    const lockType = expression.args.lockType;
    const override = expression.args.override ? ' OVERRIDE' : '';
    return `LOCKING ${kind}${thisStr}${forOrInStr} ${lockType}${override}`;
  }

  withDataPropertySql (expression: WithDataPropertyExpr): string {
    const dataSql = `WITH ${expression.args.no ? 'NO ' : ''}DATA`;
    const statistics = expression.args.statistics;
    let statisticsSql = '';
    if (statistics !== undefined) {
      statisticsSql = ` AND ${!statistics ? 'NO ' : ''}STATISTICS`;
    }
    return `${dataSql}${statisticsSql}`;
  }

  withSystemVersioningPropertySql (expression: WithSystemVersioningPropertyExpr): string {
    let thisStr = this.sql(expression, 'this');
    thisStr = thisStr ? `HISTORY_TABLE=${thisStr}` : '';
    let dataConsistency = this.sql(expression, 'dataConsistency');
    dataConsistency = dataConsistency ? `DATA_CONSISTENCY_CHECK=${dataConsistency}` : '';
    let retentionPeriod = this.sql(expression, 'retentionPeriod');
    retentionPeriod = retentionPeriod ? `HISTORY_RETENTION_PERIOD=${retentionPeriod}` : '';

    let onSql: string;
    if (thisStr) {
      onSql = this.func('ON', [
        thisStr,
        dataConsistency,
        retentionPeriod,
      ]);
    } else {
      onSql = expression.args.on ? 'ON' : 'OFF';
    }

    return `SYSTEM_VERSIONING=${onSql}`;
  }

  insertSql (expression: InsertExpr): string {
    const hint = this.sql(expression, 'hint');
    const overwrite = expression.args.overwrite;
    const isDirectory = expression.args.this instanceof DirectoryExpr;
    let keyword: string;
    if (isDirectory) {
      keyword = overwrite ? ' OVERWRITE' : ' INTO';
    } else {
      keyword = overwrite ? this._constructor.INSERT_OVERWRITE : ' INTO';
    }

    const stored = this.sql(expression, 'stored');
    const storedStr = stored ? ` ${stored}` : '';
    const alternative = expression.args.alternative as string | undefined;
    const alternativeStr = alternative ? ` OR ${alternative}` : '';
    const ignore = expression.args.ignore ? ' IGNORE' : '';
    const isFunction = expression.args.isFunction;
    if (isFunction) {
      keyword = `${keyword} FUNCTION`;
    }
    const thisStr = `${keyword} ${this.sql(expression, 'this')}`;
    const exists = expression.args.exists ? ' IF EXISTS' : '';
    const where = this.sql(expression, 'where');
    const whereStr = where ? `${this.sep()}REPLACE WHERE ${where}` : '';
    let expressionSql = `${this.sep()}${this.sql(expression, 'expression')}`;
    const onConflict = this.sql(expression, 'conflict');
    const onConflictStr = onConflict ? ` ${onConflict}` : '';
    const byName = expression.args.byName ? ' BY NAME' : '';
    const defaultValues = expression.args.default ? 'DEFAULT VALUES' : '';
    const returning = this.sql(expression, 'returning');

    if (this._constructor.RETURNING_END) {
      expressionSql = `${expressionSql}${onConflictStr}${defaultValues}${returning}`;
    } else {
      expressionSql = `${returning}${expressionSql}${onConflictStr}`;
    }

    const partition = this.sql(expression, 'partition');
    const partitionStr = partition ? ` ${partition}` : '';
    const settings = this.sql(expression, 'settings');
    const settingsStr = settings ? ` ${settings}` : '';
    const source = this.sql(expression, 'source');
    const sourceStr = source ? `TABLE ${source}` : '';

    const sql = `INSERT${hint}${alternativeStr}${ignore}${thisStr}${storedStr}${byName}${exists}${partitionStr}${settingsStr}${whereStr}${expressionSql}${sourceStr}`;
    return this.prependCtes(expression, sql);
  }

  introducerSql (expression: IntroducerExpr): string {
    const thisStr = this.sql(expression, 'this');
    return `_${thisStr} ${this.sql(expression, 'expression')}`;
  }

  killSql (expression: KillExpr): string {
    const kind = expression.args.kind ? `${expression.args.kind} ` : '';
    return `KILL ${kind}${this.sql(expression, 'this')}`;
  }

  pseudoTypeSql (expression: PseudoTypeExpr): string {
    return expression.name;
  }

  objectIdentifierSql (expression: ObjectIdentifierExpr): string {
    return expression.name;
  }

  onConflictSql (expression: OnConflictExpr): string {
    const conflict = expression.args.duplicate ? 'ON DUPLICATE KEY' : 'ON CONFLICT';

    let constraint = this.sql(expression, 'constraint');
    constraint = constraint ? ` ON CONSTRAINT ${constraint}` : '';

    let conflictKeys = this.expressions(expression, {
      key: 'conflictKeys',
      flat: true,
    });
    if (conflictKeys) {
      conflictKeys = `(${conflictKeys})`;
    }

    const indexPredicate = this.sql(expression, 'indexPredicate');
    conflictKeys = `${conflictKeys}${indexPredicate} `;

    const action = this.sql(expression, 'action');

    let expressions = this.expressions(expression, { flat: true });
    if (expressions) {
      const setKeyword = this._constructor.DUPLICATE_KEY_UPDATE_WITH_SET ? 'SET ' : '';
      expressions = ` ${setKeyword}${expressions}`;
    }

    const where = this.sql(expression, 'where');

    return `${conflict}${constraint}${conflictKeys}${action}${expressions}${where}`;
  }

  returningSql (expression: ReturningExpr): string {
    return this.opExpressions('RETURNING', expression);
  }

  rowFormatDelimitedPropertySql (expression: RowFormatDelimitedPropertyExpr): string {
    const fields = this.sql(expression, 'fields');
    const fieldsStr = fields ? ` FIELDS TERMINATED BY ${fields}` : '';
    const collectionItems = this.sql(expression, 'collectionItems');
    const collectionItemsStr = collectionItems ? ` COLLECTION ITEMS TERMINATED BY ${collectionItems}` : '';
    const mapKeys = this.sql(expression, 'mapKeys');
    const mapKeysStr = mapKeys ? ` MAP KEYS TERMINATED BY ${mapKeys}` : '';
    const lines = this.sql(expression, 'lines');
    const linesStr = lines ? ` LINES TERMINATED BY ${lines}` : '';
    const nullStr = this.sql(expression, 'null');
    const nullDef = nullStr ? ` NULL DEFINED AS ${nullStr}` : '';
    return `ROW FORMAT DELIMITED${fieldsStr}${collectionItemsStr}${mapKeysStr}${linesStr}${nullDef}`;
  }

  withTableHintSql (expression: WithTableHintExpr): string {
    return `WITH (${this.expressions(expression, { flat: true })})`;
  }

  indexTableHintSql (expression: IndexTableHintExpr): string {
    const thisSql = `${this.sql(expression, 'this')} INDEX`;
    let target = this.sql(expression, 'target');
    target = target ? ` FOR ${target}` : '';

    return `${thisSql}${target} (${this.expressions(expression, { flat: true })})`;
  }

  historicalDataSql (expression: HistoricalDataExpr): string {
    const thisSql = this.sql(expression, 'this');
    const kind = this.sql(expression, 'kind');
    const expr = this.sql(expression, 'expression');

    return `${thisSql} (${kind} => ${expr})`;
  }

  tableParts (expression: TableExpr): string {
    const catalog = this.sql(expression, 'catalog');
    const db = this.sql(expression, 'db');
    const tableExpr = this.sql(expression, 'this');
    const tableArray: string[] = [];

    if (catalog) {
      tableArray.push(catalog);
    }
    if (db) {
      tableArray.push(db);
    }
    if (tableExpr) {
      tableArray.push(tableExpr);
    }

    return tableArray.join('.');
  }

  tableSql (
    expression: TableExpr,
    options: { sep?: string } = {},
  ): string {
    const { sep = ' AS ' } = options;

    const table = this.tableParts(expression);
    const only = expression.args.only ? 'ONLY ' : '';
    let partition = this.sql(expression, 'partition');
    partition = partition ? ` ${partition}` : '';
    let version = this.sql(expression, 'version');
    version = version ? ` ${version}` : '';
    let alias = this.sql(expression, 'alias');
    alias = alias ? `${sep}${alias}` : '';

    const sample = this.sql(expression, 'sample');
    let samplePreAlias: string;
    let samplePostAlias: string;
    if (this.dialect._constructor.ALIAS_POST_TABLESAMPLE) {
      samplePreAlias = sample;
      samplePostAlias = '';
    } else {
      samplePreAlias = '';
      samplePostAlias = sample;
    }

    let hints = this.expressions(expression, {
      key: 'hints',
      sep: ' ',
    });
    hints = hints && this._constructor.TABLE_HINTS ? ` ${hints}` : '';
    const pivots = this.expressions(expression, {
      key: 'pivots',
      sep: '',
      flat: true,
    });
    const joins = this.indent(
      this.expressions(expression, {
        key: 'joins',
        sep: '',
        flat: true,
      }),
      { skipFirst: true },
    );
    const laterals = this.expressions(expression, {
      key: 'laterals',
      sep: '',
    });

    let fileFormat = this.sql(expression, 'format');
    if (fileFormat) {
      const pattern = this.sql(expression, 'pattern');
      const patternStr = pattern ? `, PATTERN => ${pattern}` : '';
      fileFormat = ` (FILE_FORMAT => ${fileFormat}${patternStr})`;
    }

    let ordinality = expression.args.ordinality || '';
    if (ordinality) {
      ordinality = ` WITH ORDINALITY${alias}`;
      alias = '';
    }

    const when = this.sql(expression, 'when');
    let tableStr = table;
    if (when) {
      tableStr = `${table} ${when}`;
    }

    let changes = this.sql(expression, 'changes');
    changes = changes ? ` ${changes}` : '';

    const rowsFrom = this.expressions(expression, { key: 'rowsFrom' });
    if (rowsFrom) {
      tableStr = `ROWS FROM ${this.wrap(rowsFrom)}`;
    }

    const indexed = expression.args.indexed;
    let indexedStr: string;
    if (indexed !== undefined) {
      indexedStr = indexed ? ` INDEXED BY ${this.sql(indexed)}` : ' NOT INDEXED';
    } else {
      indexedStr = '';
    }

    return `${only}${tableStr}${changes}${partition}${version}${fileFormat}${samplePreAlias}${alias}${indexedStr}${hints}${pivots}${samplePostAlias}${joins}${laterals}${ordinality}`;
  }

  tableFromRowsSql (expression: TableFromRowsExpr): string {
    const table = this.func('TABLE', [expression.args.this]);
    let alias = this.sql(expression, 'alias');
    alias = alias ? ` AS ${alias}` : '';
    const sample = this.sql(expression, 'sample');
    const pivots = this.expressions(expression, {
      key: 'pivots',
      sep: '',
      flat: true,
    });
    const joins = this.indent(
      this.expressions(expression, {
        key: 'joins',
        sep: '',
        flat: true,
      }),
      { skipFirst: true },
    );
    return `${table}${alias}${pivots}${sample}${joins}`;
  }

  pivotSql (expression: PivotExpr): string {
    const expressions = this.expressions(expression, { flat: true });
    const direction = expression.args.unpivot ? 'UNPIVOT' : 'PIVOT';

    const group = this.sql(expression, 'group');

    if (expression.args.this) {
      const thisStr = this.sql(expression, 'this');
      let sql: string;
      if (!expressions) {
        sql = `UNPIVOT ${thisStr}`;
      } else {
        const on = `${this.seg('ON')} ${expressions}`;
        let into = this.sql(expression, 'into');
        into = into ? `${this.seg('INTO')} ${into}` : '';
        let using = this.expressions(expression, {
          key: 'using',
          flat: true,
        });
        using = using ? `${this.seg('USING')} ${using}` : '';
        sql = `${direction} ${thisStr}${on}${into}${using}${group}`;
      }
      return this.prependCtes(expression, sql);
    }

    let alias = this.sql(expression, 'alias');
    alias = alias ? ` AS ${alias}` : '';

    const fields = this.expressions(
      expression,
      {
        key: 'fields',
        sep: ' ',
        dynamic: true,
        newLine: true,
        skipFirst: true,
        skipLast: true,
      },
    );

    const includeNulls = expression.args.includeNulls;
    let nulls: string;
    if (includeNulls !== undefined) {
      nulls = includeNulls ? ' INCLUDE NULLS ' : ' EXCLUDE NULLS ';
    } else {
      nulls = '';
    }

    let defaultOnNull = this.sql(expression, 'defaultOnNull');
    defaultOnNull = defaultOnNull ? ` DEFAULT ON NULL (${defaultOnNull})` : '';
    const sql = `${this.seg(direction)}${nulls}(${expressions} FOR ${fields}${defaultOnNull}${group})${alias}`;
    return this.prependCtes(expression, sql);
  }

  versionSql (expression: VersionExpr): string {
    const thisStr = `FOR ${expression.name}`;
    const kind = expression.text?.('kind') || '';
    const expr = this.sql(expression, 'expression');
    return `${thisStr} ${kind} ${expr}`;
  }

  tupleSql (expression: TupleExpr): string {
    return `(${this.expressions(expression, {
      dynamic: true,
      newLine: true,
      skipFirst: true,
      skipLast: true,
    })})`;
  }

  updateFromJoinsSql (expression: UpdateExpr): [string, string] {
    const fromExpr = expression.args.from;
    if ((this.constructor as typeof Generator).UPDATE_STATEMENT_SUPPORTS_FROM || !fromExpr) {
      return ['', this.sql(expression, 'from')];
    }

    // Qualify unqualified columns in SET clause with the target table
    const targetTable = expression.args.this;
    if (targetTable instanceof TableExpr) {
      const targetName = toIdentifier(targetTable.aliasOrName);
      for (const eq of expression.args.expressions || []) {
        const col = eq.args.this;
        if (col instanceof ColumnExpr && !col.table) {
          col.setArgKey('table', targetName);
        }
      }
    }

    const table = fromExpr.args.this;
    const nestedJoins: Expression[] = (isInstanceOf(table, Expression) ? table.args.joins : undefined) || [];
    if (0 < nestedJoins.length && isInstanceOf(table, Expression)) {
      table.setArgKey('joins', undefined);
    }

    let joinSql = isInstanceOf(table, Expression)
      ? this.sql(new JoinExpr({
        this: table,
        on: true_(),
      }))
      : '';
    for (const nested of nestedJoins) {
      if (!nested.getArgKey('on') && !nested.getArgKey('using')) {
        nested.setArgKey('on', true_());
      }
      joinSql += this.sql(nested);
    }

    return [joinSql, ''];
  }

  updateSql (expression: UpdateExpr): string {
    const thisStr = this.sql(expression, 'this');
    const [joinSql, fromSql] = this.updateFromJoinsSql(expression);
    const setSql = this.expressions(expression, { flat: true });
    const whereSql = this.sql(expression, 'where');
    const returning = this.sql(expression, 'returning');
    const order = this.sql(expression, 'order');
    const limit = this.sql(expression, 'limit');
    let expressionSql: string;
    if (this._constructor.RETURNING_END) {
      expressionSql = `${fromSql}${whereSql}${returning}`;
    } else {
      expressionSql = `${returning}${fromSql}${whereSql}`;
    }
    let options = this.expressions(expression, { key: 'options' });
    options = options ? ` OPTION(${options})` : '';
    const sql = `UPDATE ${thisStr}${joinSql} SET ${setSql}${expressionSql}${order}${limit}${options}`;
    return this.prependCtes(expression, sql);
  }

  valuesSql (
    expression: ValuesExpr,
    options: { valuesAsTable?: boolean } = {},
  ): string {
    const { valuesAsTable = true } = options;
    const shouldUseValuesAsTable = valuesAsTable && this._constructor.VALUES_AS_TABLE;

    // The VALUES clause is still valid in an `INSERT INTO ..` statement, for example
    if (shouldUseValuesAsTable || !expression.findAncestor<FromExpr | JoinExpr>(FromExpr, JoinExpr)) {
      const args = this.expressions(expression);
      const alias = this.sql(expression, 'alias');
      let values = `VALUES${this.seg('')}${args}`;

      const shouldWrap = this._constructor.WRAP_DERIVED_VALUES
        && (alias || [FromExpr, TableExpr].some((cls) => expression instanceof cls));

      if (shouldWrap) {
        values = `(${values})`;
      }

      values = this.queryModifiers(expression, values);
      return alias ? `${values} AS ${alias}` : values;
    }

    // Converts `VALUES...` expression into a series of select unions.
    const aliasNode = isInstanceOf(expression.args.alias, TableAliasExpr) ? expression.args.alias : undefined;
    const columnNames = aliasNode?.columns;

    const selects: QueryExpr[] = [];

    const expressions = expression.args.expressions;
    if (expressions) {
      for (let i = 0; i < expressions.length; i++) {
        const tup = expressions[i];
        let row = tup.args.expressions as Expression[] || [];

        if (i === 0 && columnNames && 0 < columnNames.length) {
          row = row.map((value: Expression, idx: number) => {
            const colName = columnNames[idx];
            return alias(value, colName instanceof IdentifierExpr ? colName : (colName as Expression).name, { copy: false });
          });
        }

        selects.push(new SelectExpr({ expressions: row }));
      }
    }

    if (selects.length === 0) {
      return '';
    }

    if (this.pretty) {
      const query = selects.reduce(
        (x: QueryExpr, y: QueryExpr): QueryExpr => union(
          [x, y],
          {
            distinct: false,
            copy: false,
          },
        ) ?? x,
      );
      const aliasThis = aliasNode?.args.this;
      const aliasExpr = aliasThis instanceof Expression ? aliasThis : undefined;
      return this.subquerySql(query.subquery(aliasExpr, { copy: false }));
    }

    const aliasStr = aliasNode ? ` AS ${this.sql(aliasNode, 'this')}` : '';
    const unions = selects.map((s) => this.sql(s)).join(' UNION ALL ');
    return `(${unions})${aliasStr}`;
  }

  varSql (expression: VarExpr): string {
    return this.sql(expression, 'this');
  }

  intoSql (expression: IntoExpr): string {
    unsupportedArgs.call(this, expression, 'expressions');
    const temporary = expression.args.temporary ? ' TEMPORARY' : '';
    const unlogged = expression.args.unlogged ? ' UNLOGGED' : '';

    const modifier = temporary || unlogged;

    return `${this.seg('INTO')}${modifier} ${this.sql(expression, 'this')}`;
  }

  fromSql (expression: FromExpr): string {
    return `${this.seg('FROM')} ${this.sql(expression, 'this')}`;
  }

  groupingSetsSql (expression: GroupingSetsExpr): string {
    const groupingSets = this.expressions(expression, { indent: false });
    return `GROUPING SETS ${this.wrap(groupingSets)}`;
  }

  /**
   * Generate SQL for ROLLUP.
   */
  rollupSql (expression: RollupExpr): string {
    const expressions = this.expressions(expression, { indent: false });
    return expressions ? `ROLLUP ${this.wrap(expressions)}` : 'WITH ROLLUP';
  }

  rollupIndexSql (expression: RollupIndexExpr): string {
    const thisStr = this.sql(expression, 'this');
    const columns = this.expressions(expression, { flat: true });
    const fromIndex = this.sql(expression, 'fromIndex');
    const fromStr = fromIndex ? ` FROM ${fromIndex}` : '';
    const properties = expression.args.properties;
    if (properties) {
      assertIsInstanceOf(properties, PropertiesExpr);
    }
    const propertiesStr = properties
      ? ` ${this.properties(properties, { prefix: 'PROPERTIES' })}`
      : '';
    return `${thisStr}(${columns})${fromStr}${propertiesStr}`;
  }

  rollupPropertySql (expression: RollupPropertyExpr): string {
    return `ROLLUP (${this.expressions(expression, { flat: true })})`;
  }

  cubeSql (expression: CubeExpr): string {
    const expressions = this.expressions(expression, { indent: false });
    return expressions ? `CUBE ${this.wrap(expressions)}` : 'WITH CUBE';
  }

  groupSql (expression: GroupExpr): string {
    const groupByAll = expression.args.all;
    let modifier = '';
    if (groupByAll === true) {
      modifier = ' ALL';
    } else if (groupByAll === false) {
      modifier = ' DISTINCT';
    }

    const groupBy = this.opExpressions(`GROUP BY${modifier}`, expression);

    const groupingSets = this.expressions(expression, { key: 'groupingSets' });
    const cube = this.expressions(expression, { key: 'cube' });
    const rollup = this.expressions(expression, { key: 'rollup' });

    const groupings = csv(
      [
        groupingSets ? this.seg(groupingSets) : '',
        cube ? this.seg(cube) : '',
        rollup ? this.seg(rollup) : '',
        expression.args.totals ? this.seg('WITH TOTALS') : '',
      ],
      { sep: this._constructor.GROUPINGS_SEP },
    );

    let result = groupBy;
    if (expression.args.expressions?.length
      && groupings
      && groupings.trim() !== 'WITH CUBE'
      && groupings.trim() !== 'WITH ROLLUP') {
      result = `${result}${this._constructor.GROUPINGS_SEP}`;
    }

    return `${result}${groupings}`;
  }

  havingSql (expression: HavingExpr): string {
    const thisStr = this.indent(this.sql(expression, 'this'));
    return `${this.seg('HAVING')}${this.sep()}${thisStr}`;
  }

  connectSql (expression: ConnectExpr): string {
    let start = this.sql(expression, 'start');
    start = start ? this.seg(`START WITH ${start}`) : '';
    const nocycle = expression.args.nocycle ? ' NOCYCLE' : '';
    const connect = this.sql(expression, 'connect');
    const connectPart = this.seg(`CONNECT BY${nocycle} ${connect}`);
    return start + connectPart;
  }

  priorSql (expression: PriorExpr): string {
    return `PRIOR ${this.sql(expression, 'this')}`;
  }

  joinSql (expression: JoinExpr): string {
    const genClass = this._constructor;
    let side: string | undefined;

    if (!genClass.SEMI_ANTI_JOIN_WITH_SIDE && (expression.args.kind === JoinExprKind.SEMI || expression.args.kind === JoinExprKind.ANTI)) {
      side = undefined;
    } else {
      side = expression.args.side;
    }

    const opSql = [
      expression.args.method,
      expression.args.global ? 'GLOBAL' : undefined,
      side,
      expression.args.kind,
      (expression.args.hint && genClass.JOIN_HINTS) ? expression.args.hint : undefined,
      (expression.args.directed && genClass.DIRECTED_JOINS) ? 'DIRECTED' : undefined,
    ].filter((op) => op).join(' ');

    let matchCond = this.sql(expression, 'matchCondition');
    matchCond = matchCond ? ` MATCH_CONDITION (${matchCond})` : '';

    let onSql = this.sql(expression, 'on');
    const using = expression.args.using;

    if (!onSql && using) {
      onSql = using.map((col) => this.sql(col)).join(', ');
    }

    const thisExpr = expression.args.this;
    let thisSql = this.sql(thisExpr);

    const exprs = this.expressions(expression);
    if (exprs) {
      thisSql = `${thisSql},${this.seg(exprs)}`;
    }

    if (onSql) {
      onSql = this.indent(onSql, { skipFirst: true });
      const space = this.pretty ? this.seg(' '.repeat(this.pad)) : ' ';
      if (using) {
        onSql = `${space}USING (${onSql})`;
      } else {
        onSql = `${space}ON ${onSql}`;
      }
    } else if (!opSql) {
      // Check for Lateral with cross_apply
      if (thisExpr instanceof LateralExpr && thisExpr.args.crossApply !== undefined) {
        return ` ${thisSql}`;
      }
      return `, ${thisSql}`;
    }

    const finalOp = opSql !== 'STRAIGHT_JOIN' ? (opSql ? `${opSql} JOIN` : 'JOIN') : opSql;

    const pivots = this.expressions(expression, {
      key: 'pivots',
      sep: '',
      flat: true,
    });
    return `${this.seg(finalOp)} ${thisSql}${matchCond}${onSql}${pivots}`;
  }

  lambdaSql (
    expression: Expression,
    options: {
      arrowSep?: string;
      wrap?: boolean;
    } = {},
  ): string {
    const {
      arrowSep = '->', wrap = true,
    } = options;

    let args = this.expressions(expression, { flat: true });
    args = (wrap && 1 < args.split(',').length) ? `(${args})` : args;
    return `${args} ${arrowSep} ${this.sql(expression, 'this')}`;
  }

  lateralOp (expression: LateralExpr): string {
    const crossApply = expression.args.crossApply;

    let op = '';
    if (crossApply === true) {
      op = 'INNER JOIN ';
    } else if (crossApply === false) {
      op = 'LEFT JOIN ';
    }

    return `${op}LATERAL`;
  }

  lateralSql (expression: LateralExpr): string {
    const thisStr = this.sql(expression, 'this');

    if (expression.args.view) {
      const alias = expression.args.alias;
      const columns = this.expressions(alias, {
        key: 'columns',
        flat: true,
      });
      const table = alias?.name ? ` ${alias.name}` : '';
      const columnsStr = columns ? ` AS ${columns}` : '';
      const outer = expression.args.outer ? ' OUTER' : '';
      const opSql = this.seg(`LATERAL VIEW${outer}`);
      return `${opSql}${this.sep()}${thisStr}${table}${columnsStr}`;
    }

    let alias = this.sql(expression, 'alias');
    alias = alias ? ` AS ${alias}` : '';

    let ordinality = expression.args.ordinality || '';
    if (ordinality) {
      ordinality = ` WITH ORDINALITY${alias}`;
      alias = '';
    }

    return `${this.lateralOp(expression)} ${thisStr}${alias}${ordinality}`;
  }

  limitSql (expression: LimitExpr, options: { top?: boolean } = {}): string {
    const { top = false } = options;

    const thisSql = this.sql(expression, 'this');

    const args = ['offset', 'expression']
      .map((k) => expression.getArgKey(k))
      .filter(Boolean)
      .map((e) => this._constructor.LIMIT_ONLY_LITERALS ? this.simplifyUnlessLiteral(e as Expression) : e as Expression);

    let argsSql = args.map((e) => this.sql(e)).join(', ');

    // Handle parentheses for TOP if non-numeric expressions are present
    if (top && args.some((e) => !e.isNumber)) {
      argsSql = `(${argsSql})`;
    }

    let expressions = this.expressions(expression, { flat: true });
    const limitOptions = this.sql(expression, 'limitOptions');
    expressions = expressions ? ` BY ${expressions}` : '';

    const keyword = top ? 'TOP' : 'LIMIT';

    return `${thisSql}${this.seg(keyword)} ${argsSql}${limitOptions}${expressions}`;
  }

  offsetSql (expression: OffsetExpr): string {
    const thisStr = this.sql(expression, 'this');
    let value = expression.args.expression as Expression;
    value = this._constructor.LIMIT_ONLY_LITERALS
      ? this.simplifyUnlessLiteral(value)
      : value;

    const expressions = this.expressions(expression, { flat: true });
    const exprPart = expressions ? ` BY ${expressions}` : '';

    return `${thisStr}${this.seg('OFFSET')} ${this.sql(value)}${exprPart}`;
  }

  setItemSql (expression: SetItemExpr): string {
    let kind = this.sql(expression, 'kind');
    if (!this._constructor.SET_ASSIGNMENT_REQUIRES_VARIABLE_KEYWORD && kind === 'VARIABLE') {
      kind = '';
    } else {
      kind = kind ? `${kind} ` : '';
    }

    const thisStr = this.sql(expression, 'this');
    const expressions = this.expressions(expression);
    let collate = this.sql(expression, 'collate');
    collate = collate ? ` COLLATE ${collate}` : '';
    const global = expression.args.global ? 'GLOBAL ' : '';

    return `${global}${kind}${thisStr}${expressions}${collate}`;
  }

  setSql (expression: SetExpr): string {
    const expressions = ` ${this.expressions(expression, { flat: true })}`;
    const tag = expression.args.tag ? ' TAG' : '';
    return `${expression.args.unset ? 'UNSET' : 'SET'}${tag}${expressions}`;
  }

  queryBandSql (expression: QueryBandExpr): string {
    const thisStr = this.sql(expression, 'this');
    const update = expression.args.update ? ' UPDATE' : '';
    const scopeRaw = this.sql(expression, 'scope');
    const scope = scopeRaw ? ` FOR ${scopeRaw}` : '';

    return `QUERY_BAND = ${thisStr}${update}${scope}`;
  }

  pragmaSql (expression: PragmaExpr): string {
    return `PRAGMA ${this.sql(expression, 'this')}`;
  }

  lockSql (expression: LockExpr): string {
    if (!this._constructor.LOCKING_READS_SUPPORTED) {
      this.unsupported('Locking reads using \'FOR UPDATE/SHARE\' are not supported');
      return '';
    }

    const update = expression.args.update;
    const key = expression.args.key;
    let lockType: string;

    if (update) {
      lockType = key ? 'FOR NO KEY UPDATE' : 'FOR UPDATE';
    } else {
      lockType = key ? 'FOR KEY SHARE' : 'FOR SHARE';
    }

    const expressions = this.expressions(expression, { flat: true });
    const exprPart = expressions ? ` OF ${expressions}` : '';
    const wait = expression.args.wait;

    let waitPart = '';
    if (wait !== undefined) {
      if (wait instanceof LiteralExpr) {
        waitPart = ` WAIT ${this.sql(wait)}`;
      } else {
        waitPart = wait ? ' NOWAIT' : ' SKIP LOCKED';
      }
    }

    return `${lockType}${exprPart}${waitPart || ''}`;
  }

  literalSql (expression: LiteralExpr): string {
    let text = expression.args.this || '';
    if (expression.isString) {
      text = `${this.dialect._constructor.QUOTE_START}${this.escapeStr(text)}${this.dialect._constructor.QUOTE_END}`;
    }
    return text;
  }

  escapeStr (
    text: string,
    options: {
      escapeBackslash?: boolean;
      delimiter?: string;
      escapedDelimiter?: string;
      isByteString?: boolean;
    } = {},
  ): string {
    const {
      escapeBackslash = true,
      delimiter,
      escapedDelimiter,
      isByteString = false,
    } = options;

    const supportsEscapeSequences = isByteString
      ? this.dialect._constructor.BYTE_STRINGS_SUPPORT_ESCAPED_SEQUENCES
      : this.dialect._constructor.STRINGS_SUPPORT_ESCAPED_SEQUENCES;

    if (supportsEscapeSequences) {
      text = Array.from(text)
        .map((ch) => {
          const escaped = this.dialect._constructor.ESCAPED_SEQUENCES[ch];
          if (escaped !== undefined) {
            return escapeBackslash || ch !== '\\' ? escaped : ch;
          }
          return ch;
        })
        .join('');
    }

    const delim = delimiter || this.dialect._constructor.QUOTE_END;

    return this.replaceLineBreaks(text).replaceAll(delim, escapedDelimiter || this.escapedQuoteEnd);
  }

  loadDataSql (expression: LoadDataExpr): string {
    const local = expression.args.local ? ' LOCAL' : '';
    const inpath = ` INPATH ${this.sql(expression, 'inpath')}`;
    const overwrite = expression.args.overwrite ? ' OVERWRITE' : '';
    const thisStr = ` INTO TABLE ${this.sql(expression, 'this')}`;
    const partition = this.sql(expression, 'partition');
    const partitionStr = partition ? ` ${partition}` : '';
    const inputFormat = this.sql(expression, 'inputFormat');
    const inputFormatStr = inputFormat ? ` INPUTFORMAT ${inputFormat}` : '';
    const serde = this.sql(expression, 'serde');
    const serdeStr = serde ? ` SERDE ${serde}` : '';
    return `LOAD DATA${local}${inpath}${overwrite}${thisStr}${partitionStr}${inputFormatStr}${serdeStr}`;
  }

  nullSql (..._args: unknown[]): string {
    return 'NULL';
  }

  booleanSql (expression: BooleanExpr): string {
    return expression.args.this ? 'TRUE' : 'FALSE';
  }

  boolandSql (expression: BoolandExpr): string {
    return `((${this.sql(expression, 'this')}) AND (${this.sql(expression, 'expression')}))`;
  }

  boolorSql (expression: BoolorExpr): string {
    return `((${this.sql(expression, 'this')}) OR (${this.sql(expression, 'expression')}))`;
  }

  orderSql (expression: OrderExpr, options: { flat?: boolean } = {}): string {
    const { flat = false } = options;
    let thisStr = this.sql(expression, 'this');
    thisStr = thisStr ? `${thisStr} ` : thisStr;
    const siblings = expression.args.siblings ? 'SIBLINGS ' : '';
    return this.opExpressions(`${thisStr}ORDER ${siblings}BY`, expression, { flat: flat || !!thisStr });
  }

  withFillSql (expression: WithFillExpr): string {
    let fromSql = this.sql(expression, 'from');
    fromSql = fromSql ? ` FROM ${fromSql}` : '';
    let toSql = this.sql(expression, 'to');
    toSql = toSql ? ` TO ${toSql}` : '';
    let stepSql = this.sql(expression, 'step');
    stepSql = stepSql ? ` STEP ${stepSql}` : '';

    const interpolate = expression.args.interpolate;
    const interpolatedValues = interpolate?.map((e) => {
      const isAlias = e instanceof AliasExpr;
      return isAlias
        ? `${this.sql(e, 'alias')} AS ${this.sql(e, 'this')}`
        : this.sql(e, 'this');
    }) || [];

    const interpolatePart = interpolatedValues.length
      ? ` INTERPOLATE (${interpolatedValues.join(', ')})`
      : '';

    return `WITH FILL${fromSql}${toSql}${stepSql}${interpolatePart}`;
  }

  clusterSql (expression: ClusterExpr): string {
    return this.opExpressions('CLUSTER BY', expression);
  }

  distributeSql (expression: DistributeExpr): string {
    return this.opExpressions('DISTRIBUTE BY', expression);
  }

  sortSql (expression: SortExpr): string {
    return this.opExpressions('SORT BY', expression);
  }

  orderedSql (expression: OrderedExpr): string {
    const desc = expression.args.desc;
    const asc = !desc;

    const nullsFirst = expression.args.nullsFirst;
    const nullsLast = !nullsFirst;
    const nullsAreLarge = this.dialect._constructor.NULL_ORDERING === NullOrdering.NULLS_ARE_LARGE;
    const nullsAreSmall = this.dialect._constructor.NULL_ORDERING === NullOrdering.NULLS_ARE_SMALL;
    const nullsAreLast = this.dialect._constructor.NULL_ORDERING === NullOrdering.NULLS_ARE_LAST;

    let thisStr = this.sql(expression, 'this');

    const sortOrder = desc ? ' DESC' : (desc === false ? ' ASC' : '');
    let nullsSortChange = '';

    if (nullsFirst && ((asc && nullsAreLarge) || (desc && nullsAreSmall) || nullsAreLast)) {
      nullsSortChange = ' NULLS FIRST';
    } else if (nullsLast && ((asc && nullsAreSmall) || (desc && nullsAreLarge)) && !nullsAreLast) {
      nullsSortChange = ' NULLS LAST';
    }

    // If the NULLS FIRST/LAST clause is unsupported, we add another sort key to simulate it
    if (nullsSortChange && !this._constructor.NULL_ORDERING_SUPPORTED) {
      const window = expression.findAncestor<WindowExpr | SelectExpr>(WindowExpr, SelectExpr);

      if (window instanceof WindowExpr && window.args.spec) {
        this.unsupported(
          `'${nullsSortChange.trim()}' translation not supported in window functions`,
        );
        nullsSortChange = '';
      } else if (
        this._constructor.NULL_ORDERING_SUPPORTED === false
        && ((asc && nullsSortChange === ' NULLS LAST') || (desc && nullsSortChange === ' NULLS FIRST'))
      ) {
        // BigQuery does not allow these ordering/nulls combinations when used under
        // an aggregation func or under a window containing one
        let ancestor: Expression | undefined = expression.findAncestor<AggFuncExpr | WindowExpr | SelectExpr>(AggFuncExpr, WindowExpr, SelectExpr);

        if (ancestor instanceof WindowExpr) {
          ancestor = ancestor.args.this;
        }

        if (ancestor instanceof AggFuncExpr) {
          this.unsupported(
            `'${nullsSortChange.trim()}' translation not supported for aggregate functions with ${sortOrder} sort order`,
          );
          nullsSortChange = '';
        }
      } else if (this._constructor.NULL_ORDERING_SUPPORTED === null) {
        if (expression.args.this?.isInteger) {
          this.unsupported(
            `'${nullsSortChange.trim()}' translation not supported with positional ordering`,
          );
        } else if (!(expression.args.this instanceof RandExpr)) {
          const nullSortOrder = nullsSortChange === ' NULLS FIRST' ? ' DESC' : '';
          thisStr = `CASE WHEN ${thisStr} IS NULL THEN 1 ELSE 0 END${nullSortOrder}, ${thisStr}`;
        }
        nullsSortChange = '';
      }
    }

    let withFill = this.sql(expression, 'withFill');
    withFill = withFill ? ` ${withFill}` : '';

    return `${thisStr}${sortOrder}${nullsSortChange}${withFill}`;
  }

  matchRecognizeMeasureSql (expression: MatchRecognizeMeasureExpr): string {
    const windowFrameRaw = this.sql(expression, 'windowFrame');
    const windowFrame = windowFrameRaw ? `${windowFrameRaw} ` : '';

    const thisStr = this.sql(expression, 'this');

    return `${windowFrame}${thisStr}`;
  }

  matchRecognizeSql (expression: MatchRecognizeExpr): string {
    const partition = this.partitionBySql(expression);
    const order = this.sql(expression, 'order');
    const measuresRaw = this.expressions(expression, { key: 'measures' });
    const measures = measuresRaw ? this.seg(`MEASURES${this.seg(measuresRaw)}`) : '';
    const rowsRaw = this.sql(expression, 'rows');
    const rows = rowsRaw ? this.seg(rowsRaw) : '';
    const afterRaw = this.sql(expression, 'after');
    const after = afterRaw ? this.seg(afterRaw) : '';
    const patternRaw = this.sql(expression, 'pattern');
    const pattern = patternRaw ? this.seg(`PATTERN (${patternRaw})`) : '';
    const definitionSqls = (expression.args.define || []).map((definition: Expression) =>
      `${this.sql(definition, 'alias')} AS ${this.sql(definition, 'this')}`);
    const definitions = this.expressions(undefined, { sqls: definitionSqls });
    const define = definitions ? this.seg(`DEFINE${this.seg(definitions)}`) : '';
    const body = [
      partition,
      order,
      measures,
      rows,
      after,
      pattern,
      define,
    ].join('');
    const aliasRaw = this.sql(expression, 'alias');
    const aliasStr = aliasRaw ? ` ${aliasRaw}` : '';
    return `${this.seg('MATCH_RECOGNIZE')} ${this.wrap(body)}${aliasStr}`;
  }

  /**
   * Helper methods.
   */
  protected queryModifiers (
    expression: Expression,
    ...sqls: string[]
  ): string {
    let limit = expression.getArgKey('limit');

    if (limit !== undefined && !(limit instanceof FetchExpr || limit instanceof LimitExpr)) return '';

    // Convert between LIMIT and FETCH based on dialect preference
    const limitFetch = this._constructor.LIMIT_FETCH;
    if (limitFetch === 'LIMIT' && limit instanceof FetchExpr) {
      limit = new LimitExpr({
        expression: maybeCopy(limit.args.count),
      });
    } else if (limitFetch === 'FETCH' && limit instanceof LimitExpr) {
      limit = new FetchExpr({
        direction: 'FIRST',
        count: maybeCopy(limit.args.expression),
      });
    }

    return csv(
      [
        ...sqls,
        ...(expression.args.joins?.map((join) => this.sql(join)) ?? []),
        this.sql(expression, 'match'),
        ...(expression.args.laterals?.map((lateral) => this.sql(lateral)) ?? []),
        this.sql(expression, 'prewhere'),
        this.sql(expression, 'where'),
        this.sql(expression, 'connect'),
        this.sql(expression, 'group'),
        this.sql(expression, 'having'),
        ...Object.values(this._constructor.AFTER_HAVING_MODIFIER_TRANSFORMS).map((gen) => gen.call(this, expression)),
        this.sql(expression, 'order'),
        ...this.offsetLimitModifiers(expression, { fetch: limit instanceof FetchExpr }, limit),
        ...this.afterLimitModifiers(expression),
        this.optionsModifier(expression),
        this.forModifiers(expression),
      ],
      { sep: '' },
    );
  }

  optionsModifier (expression: Expression): string {
    const options = this.expressions(expression, { key: 'options' });
    return options ? ` ${options}` : '';
  }

  forModifiers (expression: Expression): string {
    const forModifiers = this.expressions(expression, { key: 'for_' });
    return forModifiers ? `${this.sep()}FOR XML${this.seg(forModifiers)}` : '';
  }

  queryOptionSql (_expression: QueryOptionExpr): string {
    this.unsupported('Unsupported query option.');
    return '';
  }

  afterLimitModifiers (expression: Expression): string[] {
    let locks = this.expressions(expression, {
      key: 'locks',
      sep: ' ',
    });
    locks = locks ? ` ${locks}` : '';
    return [locks, this.sql(expression, 'sample')];
  }

  /**
   * Generate SQL for SELECT.
   */
  selectSql (expression: SelectExpr): string {
    const into = expression.args.into;
    if (!this._constructor.SUPPORTS_SELECT_INTO && into) {
      into.pop();
    }

    const hint = this.sql(expression, 'hint');
    let distinct = this.sql(expression, 'distinct');
    distinct = distinct ? ` ${distinct}` : '';
    let kind = this.sql(expression, 'kind');

    const limit = expression.args.limit;
    let top = '';
    if (this._constructor.LIMIT_IS_TOP && limit instanceof LimitExpr) {
      top = this.limitSql(limit, { top: true });
      limit.pop();
    }

    let expressions = this.expressions(expression);

    const genClass = this._constructor;
    if (kind) {
      if (genClass.SELECT_KINDS.includes(kind)) {
        kind = ` AS ${kind}`;
      } else {
        if (kind === 'STRUCT') {
          // Rebuild expressions as Struct with PropertyEQ for aliases
          const exprs = expression.args.expressions as Expression[] || [];
          const structExprs = exprs.map((e) => {
            if (e instanceof AliasExpr) {
              return new PropertyEqExpr({
                this: e.args.alias instanceof IdentifierExpr ? e.args.alias : new IdentifierExpr({ this: e.args.alias?.toString() || '' }),
                expression: e.args.this,
              });
            }
            return e;
          });
          expressions = this.expressions(undefined, {
            sqls: [new StructExpr({ expressions: structExprs })],
          });
        }
        kind = '';
      }
    }

    const operationModifiers = this.expressions(expression, {
      key: 'operationModifiers',
      sep: ' ',
    });
    const operationModifiersPart = operationModifiers ? `${this.sep()}${operationModifiers}` : '';

    const topDistinct = genClass.LIMIT_IS_TOP ? `${distinct}${hint}${top}` : `${top}${hint}${distinct}`;
    expressions = expressions ? `${this.sep()}${expressions}` : expressions;

    let sql = this.queryModifiers(
      expression,
      `SELECT${topDistinct}${operationModifiersPart}${kind}${expressions}`,
      this.sql(expression, 'into', { comment: false }),
      this.sql(expression, 'from', { comment: false }),
    );

    sql = this.prependCtes(expression, sql);

    if (!genClass.SUPPORTS_SELECT_INTO && into) {
      assertIsInstanceOf(into, IntoExpr);
      let tableKind = '';
      if (into.args.temporary) {
        tableKind = ' TEMPORARY';
      } else if (genClass.SUPPORTS_UNLOGGED_TABLES && into.args.unlogged) {
        tableKind = ' UNLOGGED';
      }
      sql = `CREATE${tableKind} TABLE ${this.sql(into.args.this)} AS ${sql}`;
    }

    return sql;
  }

  /**
   * Generate SQL for schema.
   */
  schemaSql (expression: SchemaExpr): string {
    const thisStr = this.sql(expression, 'this');
    const sql = this.schemaColumnsSql(expression);
    return (thisStr && sql) ? `${thisStr} ${sql}` : (thisStr || sql);
  }

  /**
   * Generate SQL for schema columns.
   */
  schemaColumnsSql (expression: SchemaExpr): string {
    if (expression.args.expressions?.length) {
      return `(${this.sep('')}${this.expressions(expression)}${this.seg(')', '')}`;
    }
    return '';
  }

  starSql (expression: StarExpr): string {
    let except = this.expressions(expression, {
      key: 'except',
      flat: true,
    });
    except = except ? `${this.seg(this._constructor.STAR_EXCEPT)} (${except})` : '';
    let replace = this.expressions(expression, {
      key: 'replace',
      flat: true,
    });
    replace = replace ? `${this.seg('REPLACE')} (${replace})` : '';
    let rename = this.expressions(expression, {
      key: 'rename',
      flat: true,
    });
    rename = rename ? `${this.seg('RENAME')} (${rename})` : '';
    return `*${except}${replace}${rename}`;
  }

  parameterSql (expression: ParameterExpr): string {
    const thisStr = this.sql(expression, 'this');
    return `${this._constructor.PARAMETER_TOKEN}${thisStr}`;
  }

  sessionParameterSql (expression: SessionParameterExpr): string {
    const thisStr = this.sql(expression, 'this');
    let kind = expression.text('kind');
    if (kind) {
      kind = `${kind}.`;
    }
    return `@@${kind}${thisStr}`;
  }

  placeholderSql (expression: PlaceholderExpr): string {
    return expression.args.this
      ? `${this._constructor.NAMED_PLACEHOLDER_TOKEN}${expression.name}`
      : '?';
  }

  subquerySql (expression: SubqueryExpr, options: { sep?: string } = {}): string {
    const { sep = ' AS ' } = options;
    let alias = this.sql(expression, 'alias');
    alias = alias ? `${sep}${alias}` : '';
    const sample = this.sql(expression, 'sample');

    if (this.dialect._constructor.ALIAS_POST_TABLESAMPLE && sample) {
      alias = `${sample}${alias}`;
      expression.setArgKey('sample', undefined);
    }

    const pivots = this.expressions(expression, {
      key: 'pivots',
      sep: '',
      flat: true,
    });
    const sql = this.queryModifiers(expression, this.wrap(expression), alias, pivots);
    return this.prependCtes(expression, sql);
  }

  qualifySql (expression: QualifyExpr): string {
    const thisStr = this.indent(this.sql(expression, 'this'));
    return `${this.seg('QUALIFY')}${this.sep()}${thisStr}`;
  }

  unnestSql (expression: UnnestExpr): string {
    const args = this.expressions(expression, { flat: true });

    const alias = expression.args.alias;
    const offset = expression.args.offset;

    if (this._constructor.UNNEST_WITH_ORDINALITY) {
      if (alias && offset instanceof IdentifierExpr) {
        assertIsInstanceOf(alias, TableAliasExpr);
        // Append offset to alias columns
        if (alias.args.columns) {
          alias.args.columns.push(offset);
        } else {
          alias.args.columns = [offset];
        }
      }
    }

    let aliasStr = '';
    if (alias && this.dialect._constructor.UNNEST_COLUMN_ONLY) {
      assertIsInstanceOf(alias, TableAliasExpr);
      const columns = alias.columns;
      aliasStr = columns && columns[0] ? this.sql(columns[0]) : '';
    } else {
      aliasStr = this.sql(alias);
    }

    aliasStr = aliasStr ? ` AS ${aliasStr}` : aliasStr;
    let suffix = '';
    if (this._constructor.UNNEST_WITH_ORDINALITY) {
      suffix = offset ? ` WITH ORDINALITY${aliasStr}` : aliasStr;
    } else {
      if (offset instanceof Expression) {
        suffix = `${aliasStr} WITH OFFSET AS ${this.sql(offset)}`;
      } else if (offset) {
        suffix = `${aliasStr} WITH OFFSET`;
      } else {
        suffix = aliasStr;
      }
    }

    return `UNNEST(${args})${suffix}`;
  }

  preWhereSql (_expression: Expression): string {
    return '';
  }

  whereSql (expression: WhereExpr): string {
    const thisStr = this.indent(this.sql(expression, 'this'));
    return `${this.seg('WHERE')}${this.sep()}${thisStr}`;
  }

  windowSql (expression: WindowExpr): string {
    let thisStr = this.sql(expression, 'this');
    const partition = this.partitionBySql(expression);
    const order = expression.args.order;
    if (order) {
      assertIsInstanceOf(order, OrderExpr);
    }
    const orderStr = order ? this.orderSql(order, { flat: true }) : '';
    const spec = this.sql(expression, 'spec');
    const alias = this.sql(expression, 'alias');
    const over = this.sql(expression, 'over') || 'OVER';

    thisStr = `${thisStr} ${expression.argKey === 'windows' ? 'AS' : over}`;

    const first = expression.args.first;
    let firstStr = '';
    if (first !== undefined) {
      firstStr = first ? 'FIRST' : 'LAST';
    }

    if (!partition && !orderStr && !spec && alias) {
      return `${thisStr} ${alias}`;
    }

    const args = this.formatArgs(
      [
        alias,
        firstStr,
        partition,
        orderStr,
        spec,
      ].filter((arg) => arg),
      { sep: ' ' },
    );

    return `${thisStr} (${args})`;
  }

  partitionBySql (expression: WindowExpr | MatchRecognizeExpr): string {
    const partition = this.expressions(expression, {
      key: 'partitionBy',
      flat: true,
    });
    return partition ? `PARTITION BY ${partition}` : '';
  }

  windowSpecSql (expression: WindowSpecExpr): string {
    const kind = this.sql(expression, 'kind');
    const start = csv(
      [this.sql(expression, 'start'), this.sql(expression, 'startSide')],
      { sep: ' ' },
    );
    const end = csv(
      [this.sql(expression, 'end'), this.sql(expression, 'endSide')],
      { sep: ' ' },
    ) || 'CURRENT ROW';

    let windowSpec = `${kind} BETWEEN ${start} AND ${end}`;

    const exclude = this.sql(expression, 'exclude');
    if (exclude) {
      if (this._constructor.SUPPORTS_WINDOW_EXCLUDE) {
        windowSpec += ` EXCLUDE ${exclude}`;
      } else {
        this.unsupported('EXCLUDE clause is not supported in the WINDOW clause');
      }
    }

    return windowSpec;
  }

  withinGroupSql (expression: WithinGroupExpr): string {
    const thisStr = this.sql(expression, 'this');
    let expressionSql = this.sql(expression, 'expression');
    expressionSql = expressionSql.substring(1); // order has a leading space
    return `${thisStr} WITHIN GROUP (${expressionSql})`;
  }

  betweenSql (expression: BetweenExpr): string {
    const thisStr = this.sql(expression, 'this');
    const low = this.sql(expression, 'low');
    const high = this.sql(expression, 'high');
    const symmetric = expression.args.symmetric;

    if (symmetric && !this._constructor.SUPPORTS_BETWEEN_FLAGS) {
      return `(${thisStr} BETWEEN ${low} AND ${high} OR ${thisStr} BETWEEN ${high} AND ${low})`;
    }

    const flag = symmetric
      ? ' SYMMETRIC'
      : (symmetric === false && this._constructor.SUPPORTS_BETWEEN_FLAGS)
        ? ' ASYMMETRIC'
        : '';

    return `${thisStr} BETWEEN${flag} ${low} AND ${high}`;
  }

  bracketOffsetExpressions (
    expression: BracketExpr,
    options: { indexOffset?: number } = {},
  ): Expression[] {
    const { indexOffset } = options;

    const offset = (indexOffset !== undefined ? indexOffset : this.dialect._constructor.INDEX_OFFSET) - (expression.args.offset || 0);
    // Call apply_index_offset helper (assumed to exist)
    const bracketThis = expression.args.this instanceof Expression ? expression.args.this : new Expression({});
    return applyIndexOffset(bracketThis, expression.args.expressions || [], offset, { dialect: this.dialect });
  }

  bracketSql (expression: BracketExpr): string {
    const expressions = this.bracketOffsetExpressions(expression);
    const expressionsSql = expressions.map((e) => this.sql(e)).join(', ');
    return `${this.sql(expression, 'this')}[${expressionsSql}]`;
  }

  allSql (expression: AllExpr): string {
    let thisStr = this.sql(expression, 'this');
    const thisExpr = expression.args.this;
    if (thisExpr && !(thisExpr instanceof TupleExpr || thisExpr instanceof ParenExpr)) {
      thisStr = this.wrap(thisStr);
    }
    return `ALL ${thisStr}`;
  }

  anySql (expression: AnyExpr): string {
    let thisStr = this.sql(expression, 'this');
    const thisExpr = expression.args.this;

    // UNWRAPPED_QUERIES are Select and SetOperation expressions
    if (UNWRAPPED_QUERIES.some((cls) => thisExpr instanceof cls) || thisExpr instanceof ParenExpr) {
      if (UNWRAPPED_QUERIES.some((cls) => thisExpr instanceof cls)) {
        thisStr = this.wrap(thisStr);
      }
      return `ANY${thisStr}`;
    }

    return `ANY ${thisStr}`;
  }

  existsSql (expression: ExistsExpr): string {
    return `EXISTS${this.wrap(expression)}`;
  }

  caseSql (expression: CaseExpr): string {
    const thisStr = this.sql(expression, 'this');
    const statements: string[] = [thisStr ? `CASE ${thisStr}` : 'CASE'];

    const ifs = expression.args.ifs as Expression[] | undefined;
    if (ifs) {
      for (const e of ifs) {
        statements.push(`WHEN ${this.sql(e, 'this')}`);
        statements.push(`THEN ${this.sql(e, 'true')}`);
      }
    }

    const defaultStr = this.sql(expression, 'default');
    if (defaultStr) {
      statements.push(`ELSE ${defaultStr}`);
    }

    statements.push('END');

    if (this.pretty && this.tooWide(statements)) {
      return this.indent(statements.join('\n'), {
        skipFirst: true,
        skipLast: true,
      });
    }

    return statements.join(' ');
  }

  constraintSql (expression: ConstraintExpr): string {
    const thisStr = this.sql(expression, 'this');
    const expressions = this.expressions(expression, { flat: true });
    return `CONSTRAINT ${thisStr} ${expressions}`;
  }

  nextValueForSql (expression: NextValueForExpr): string {
    const order = expression.args.order;
    if (order) {
      assertIsInstanceOf(order, OrderExpr);
    }
    const orderStr = order ? ` OVER (${this.orderSql(order, { flat: true })})` : '';
    return `NEXT VALUE FOR ${this.sql(expression, 'this')}${orderStr}`;
  }

  extractSql (expression: ExtractExpr): string {
    let thisExpr = expression.args.this as Expression;

    if (this._constructor.NORMALIZE_EXTRACT_DATE_PARTS) {
      thisExpr = mapDatePart(thisExpr, { dialect: this.dialect }) || thisExpr;
    }

    const thisSql = this._constructor.EXTRACT_ALLOWS_QUOTES ? this.sql(thisExpr) : thisExpr.name;
    const expressionSql = this.sql(expression, 'expression');

    return `EXTRACT(${thisSql} FROM ${expressionSql})`;
  }

  trimSql (expression: TrimExpr): string {
    const trimType = this.sql(expression, 'position');

    let funcName: string;
    if (trimType === 'LEADING') {
      funcName = 'LtRIM';
    } else if (trimType === 'TRAILING') {
      funcName = 'RTRIM';
    } else {
      funcName = 'TRIM';
    }

    return this.func(funcName, [expression.args.this, expression.args.expression]);
  }

  convertConcatArgs (expression: ConcatExpr | ConcatWsExpr): Expression[] {
    let args = expression.args.expressions || [];
    if (expression instanceof ConcatWsExpr) {
      args = args.slice(1); // Skip the delimiter
    }

    if (this.dialect._constructor.STRICT_STRING_CONCAT && expression.args.safe) {
      args = args.map((e) => cast(e, DataTypeExprKind.TEXT, { dialect: this.dialect }));
    }

    if (!this.dialect._constructor.CONCAT_COALESCE && expression.args.coalesce) {
      const wrapWithCoalesce = (e: Expression): Expression => {
        if (!e.type) {
          e = annotateTypes(e, { dialect: this.dialect });
        }

        if (e.isString || (e.isType && e.isType(DataTypeExprKind.ARRAY))) {
          return e;
        }

        return new CoalesceExpr({
          this: e,
          expressions: [
            new LiteralExpr({
              this: '',
              isString: true,
            }),
          ],
        });
      };

      args = args.map(wrapWithCoalesce);
    }

    return args;
  }

  concatSql (expression: ConcatExpr): string {
    if (this.dialect._constructor.CONCAT_COALESCE && !expression.args.coalesce) {
      // Dialect's CONCAT function coalesces NULLs to empty strings, but the expression does not.
      // Transpile to double pipe operators, which typically returns NULL if any args are NULL
      // instead of coalescing them to empty string.
      return concatToDPipeSql.call(this, expression);
    }

    const expressions = this.convertConcatArgs(expression);

    // Some dialects don't allow a single-argument CONCAT call
    if (!this._constructor.SUPPORTS_SINGLE_ARG_CONCAT && expressions.length === 1) {
      return this.sql(expressions[0]);
    }

    return this.func('CONCAT', expressions);
  }

  concatWsSql (expression: ConcatWsExpr): string {
    const expressions = expression.args.expressions || [];
    const delimiter = expressions[0];
    const args = this.convertConcatArgs(expression);
    return this.func('CONCAT_WS', [delimiter, ...args]);
  }

  checkSql (expression: CheckExpr): string {
    const thisStr = this.sql(expression, 'this');
    return `CHECK (${thisStr})`;
  }

  foreignKeySql (expression: ForeignKeyExpr): string {
    let expressions = this.expressions(expression, { flat: true });
    expressions = expressions ? ` (${expressions})` : '';
    let reference = this.sql(expression, 'reference');
    reference = reference ? ` ${reference}` : '';
    let deleteSql = this.sql(expression, 'delete');
    deleteSql = deleteSql ? ` ON DELETE ${deleteSql}` : '';
    let update = this.sql(expression, 'update');
    update = update ? ` ON UPDATE ${update}` : '';
    let options = this.expressions(expression, {
      key: 'options',
      flat: true,
      sep: ' ',
    });
    options = options ? ` ${options}` : '';
    return `FOREIGN KEY${expressions}${reference}${deleteSql}${update}${options}`;
  }

  primaryKeySql (expression: PrimaryKeyExpr): string {
    let thisStr = this.sql(expression, 'this');
    thisStr = thisStr ? ` ${thisStr}` : '';
    const expressions = this.expressions(expression, { flat: true });
    const include = this.sql(expression, 'include');
    let options = this.expressions(expression, {
      key: 'options',
      flat: true,
      sep: ' ',
    });
    options = options ? ` ${options}` : '';
    return `PRIMARY KEY${thisStr} (${expressions})${include}${options}`;
  }

  ifSql (expression: IfExpr): string {
    return this.caseSql(new CaseExpr({
      ifs: [expression],
      default: expression.args.false,
    }));
  }

  matchAgainstSql (expression: MatchAgainstExpr): string {
    let exprs: (string | Expression)[] = expression.args.expressions || [];

    if (this._constructor.MATCH_AGAINST_TABLE_PREFIX) {
      exprs = exprs.map((expr) => {
        if (expr instanceof TableExpr) {
          return `TABLE ${this.sql(expr)}`;
        }
        return expr;
      });
    } else {
      exprs = expression.args.expressions || [];
    }

    const modifier = expression.args.modifier;
    const modifierPart = modifier ? ` ${modifier}` : '';

    return `${this.func('MATCH', exprs)} AGAINST(${this.sql(expression, 'this')}${modifierPart})`;
  }

  jsonKeyValueSql (expression: JsonKeyValueExpr): string {
    return `${this.sql(expression, 'this')}${this._constructor.JSON_KEY_VALUE_PAIR_SEP} ${this.sql(expression, 'expression')}`;
  }

  jsonPathSql (expression: JsonPathExpr): string {
    let path = this.expressions(expression, {
      sep: '',
      flat: true,
    }).replace(/^\.+/, '');

    if (expression.args.escape) {
      path = this.escapeStr(path);
    }

    if (this._constructor.QUOTE_JSON_PATH) {
      path = `${this.dialect._constructor.QUOTE_START}${path}${this.dialect._constructor.QUOTE_END}`;
    }

    return path;
  }

  jsonPathPart (expression?: ExpressionValue<JsonPathPartExpr>): string {
    if (expression === undefined) {
      return '';
    }

    if (expression instanceof JsonPathPartExpr) {
      const transform = this._constructor.TRANSFORMS.get(expression._constructor);
      if (typeof transform !== 'function') {
        this.unsupported(`Unsupported JsonPathPart type ${expression._constructor.name}`);
        return '';
      }
      return transform.call(this, expression);
    }
    if (typeof expression === 'number') {
      return String(expression);
    }
    if (this.quoteJsonPathKeyUsingBrackets && this._constructor.JSON_PATH_SINGLE_QUOTE_ESCAPE) {
      const escaped = expression.toString().replaceAll('\'', '\\\'');
      return `\\'${escaped}\\'`;
    }
    const escaped = expression.toString().replace(/"/g, '\\"');
    return `"${escaped}"`;
  }

  formatJsonSql (expression: FormatJsonExpr): string {
    return `${this.sql(expression, 'this')} FORMAT JSON`;
  }

  formatPhraseSql (expression: FormatPhraseExpr): string {
    const thisStr = this.sql(expression, 'this');
    const fmt = this.sql(expression, 'format');
    return `${thisStr} (FORMAT ${fmt})`;
  }

  jsonObjectSql (expression: JsonObjectExpr | JsonObjectAggExpr): string {
    const nullHandling = expression.args.nullHandling;
    const nullHandlingStr = nullHandling ? ` ${nullHandling}` : '';

    const uniqueKeys = expression.args.uniqueKeys;
    let uniqueKeysStr = '';
    if (uniqueKeys !== undefined && uniqueKeys !== null) {
      uniqueKeysStr = ` ${uniqueKeys ? 'WITH' : 'WITHOUT'} UNIQUE KEYS`;
    }

    const returnType = this.sql(expression, 'returnType');
    const returnTypeStr = returnType ? ` RETURNING ${returnType}` : '';
    const encoding = this.sql(expression, 'encoding');
    const encodingStr = encoding ? ` ENCODING ${encoding}` : '';

    const funcName = expression instanceof JsonObjectExpr ? 'JSON_OBJECT' : 'JSON_OBJECTAGG';
    return this.func(
      funcName,
      expression.args.expressions || [],
      { suffix: `${nullHandlingStr}${uniqueKeysStr}${returnTypeStr}${encodingStr})` },
    );
  }

  jsonObjectAggSql (expression: JsonObjectAggExpr): string {
    return this.jsonObjectSql(expression);
  }

  jsonArraySql (expression: JsonArrayExpr): string {
    const nullHandling = expression.args.nullHandling;
    const nullHandlingStr = nullHandling ? ` ${nullHandling}` : '';
    const returnType = this.sql(expression, 'returnType');
    const returnTypeStr = returnType ? ` RETURNING ${returnType}` : '';
    const strict = expression.args.strict ? ' STRICT' : '';
    return this.func(
      'JSON_ARRAY',
      expression.args.expressions || [],
      { suffix: `${nullHandlingStr}${returnTypeStr}${strict})` },
    );
  }

  jsonArrayAggSql (expression: JsonArrayAggExpr): string {
    const thisStr = this.sql(expression, 'this');
    const order = this.sql(expression, 'order');
    const nullHandling = expression.args.nullHandling;
    const nullHandlingStr = nullHandling ? ` ${nullHandling}` : '';
    const returnType = this.sql(expression, 'returnType');
    const returnTypeStr = returnType ? ` RETURNING ${returnType}` : '';
    const strict = expression.args.strict ? ' STRICT' : '';
    return this.func(
      'JSON_ARRAYAGG',
      [thisStr],
      { suffix: `${order}${nullHandlingStr}${returnTypeStr}${strict})` },
    );
  }

  jsonColumnDefSql (expression: JsonColumnDefExpr): string {
    const path = this.sql(expression, 'path');
    const pathStr = path ? ` PATH ${path}` : '';
    const nestedSchema = this.sql(expression, 'nestedSchema');

    if (nestedSchema) {
      return `NESTED${pathStr} ${nestedSchema}`;
    }

    const thisStr = this.sql(expression, 'this');
    const kind = this.sql(expression, 'kind');
    const kindStr = kind ? ` ${kind}` : '';

    const ordinality = expression.args.ordinality ? ' FOR ORDINALITY' : '';
    return `${thisStr}${kindStr}${pathStr}${ordinality}`;
  }

  jsonSchemaSql (expression: JsonSchemaExpr): string {
    return this.func('COLUMNS', expression.args.expressions || []);
  }

  jsonTableSql (expression: JsonTableExpr): string {
    const thisStr = this.sql(expression, 'this');
    const path = this.sql(expression, 'path');
    const pathStr = path ? `, ${path}` : '';
    const errorHandling = expression.args.errorHandling;
    const errorHandlingStr = errorHandling ? ` ${errorHandling}` : '';
    const emptyHandling = expression.args.emptyHandling;
    const emptyHandlingStr = emptyHandling ? ` ${emptyHandling}` : '';
    const schema = this.sql(expression, 'schema');
    return this.func(
      'JSON_TABLE',
      [thisStr],
      { suffix: `${pathStr}${errorHandlingStr}${emptyHandlingStr} ${schema})` },
    );
  }

  openJsonColumnDefSql (expression: OpenJsonColumnDefExpr): string {
    const thisStr = this.sql(expression, 'this');
    const kind = this.sql(expression, 'kind');
    const path = this.sql(expression, 'path');
    const pathStr = path ? ` ${path}` : '';
    const asJson = expression.args.asJson ? ' AS JSON' : '';
    return `${thisStr} ${kind}${pathStr}${asJson}`;
  }

  openJsonSql (expression: OpenJsonExpr): string {
    const thisStr = this.sql(expression, 'this');
    const path = this.sql(expression, 'path');
    const pathStr = path ? `, ${path}` : '';
    const expressions = this.expressions(expression);
    const withStr = expressions
      ? ` WITH (${this.seg(this.indent(expressions), '')}${this.seg(')', '')}`
      : '';
    return `OPENJSON(${thisStr}${pathStr})${withStr}`;
  }

  inSql (expression: InExpr): string {
    const query = expression.args.query;
    const unnest = expression.args.unnest;
    const field = expression.args.field;
    const isGlobal = expression.args.isGlobal ? ' GLOBAL' : '';

    let inSql: string;

    if (query) {
      inSql = this.sql(query);
    } else if (unnest) {
      inSql = this.inUnnestOp(unnest);
    } else if (field) {
      inSql = this.sql(field);
    } else {
      const options = {
        dynamic: true,
        newLine: true,
        skipFirst: true,
        skipLast: true,
      };
      inSql = `(${this.expressions(expression, options)})`;
    }

    return `${this.sql(expression, 'this')}${isGlobal} IN ${inSql}`;
  }

  inUnnestOp (unnest: Expression): string {
    return `(SELECT ${this.sql(unnest)})`;
  }

  intervalSql (expression: IntervalExpr): string {
    const unitExpression = expression.args.unit;
    let unit = unitExpression ? this.sql(unitExpression) : '';
    if (!this._constructor.INTERVAL_ALLOWS_PLURAL_FORM) {
      unit = this._constructor.TIME_PART_SINGULARS[unit] || unit;
    }
    const unitStr = unit ? ` ${unit}` : '';

    if (this._constructor.SINGLE_STRING_INTERVAL) {
      const thisName = expression.args.this instanceof Expression ? expression.args.this?.name || '' : expression.args.this;
      if (thisName) {
        if (unitExpression instanceof IntervalSpanExpr) {
          return `INTERVAL '${thisName}'${unitStr}`;
        }
        return `INTERVAL '${thisName}${unitStr}'`;
      }
      return `INTERVAL${unitStr}`;
    }

    let thisStr = this.sql(expression, 'this');
    if (thisStr) {
      const unwrapped = expression.args.this instanceof Expression && this._constructor.UNWRAPPED_INTERVAL_VALUES.has(expression.args.this._constructor);
      thisStr = unwrapped ? ` ${thisStr}` : ` (${thisStr})`;
    }

    return `INTERVAL${thisStr}${unitStr}`;
  }

  returnSql (expression: ReturnExpr): string {
    return `RETURN ${this.sql(expression, 'this')}`;
  }

  referenceSql (expression: ReferenceExpr): string {
    const thisStr = this.sql(expression, 'this');
    let expressions = this.expressions(expression, { flat: true });
    expressions = expressions ? `(${expressions})` : '';
    let options = this.expressions(expression, {
      key: 'options',
      flat: true,
      sep: ' ',
    });
    options = options ? ` ${options}` : '';
    return `REFERENCES ${thisStr}${expressions}${options}`;
  }

  anonymousSql (expression: AnonymousExpr): string {
    const parent = expression.parent;
    const isQualified = parent instanceof DotExpr && parent.args.expression === expression;
    return this.func(
      this.sql(expression, 'this'),
      expression.args.expressions || [],
      { normalize: !isQualified },
    );
  }

  parenSql (expression: ParenExpr): string {
    const sql = this.seg(this.indent(this.sql(expression, 'this')), '');
    return `(${sql}${this.seg(')', '')}`;
  }

  negSql (expression: NegExpr): string {
    const thisSql = this.sql(expression, 'this');
    // Avoid converting "- -5" to "--5" which is a comment
    const sep = thisSql[0] === '-'
      ? ' '
      : '';
    return `-${sep}${thisSql}`;
  }

  notSql (expression: NotExpr): string {
    return `NOT ${this.sql(expression, 'this')}`;
  }

  aliasSql (expression: AliasExpr): string {
    let alias = this.sql(expression, 'alias');
    alias = alias ? ` AS ${alias}` : '';
    return `${this.sql(expression, 'this')}${alias}`;
  }

  pivotAliasSql (expression: PivotAliasExpr): string {
    const alias = expression.args.alias;

    const parent = expression.parent;
    const pivot = parent && parent.parent;

    if (pivot instanceof PivotExpr && pivot.unpivot) {
      const identifierAlias = alias instanceof IdentifierExpr;
      const literalAlias = alias instanceof LiteralExpr;

      if (identifierAlias && !this._constructor.UNPIVOT_ALIASES_ARE_IDENTIFIERS) {
        alias.replace(new LiteralExpr({
          this: (alias as Expression).name,
          isString: true,
        }));
      } else if (!identifierAlias && literalAlias && this._constructor.UNPIVOT_ALIASES_ARE_IDENTIFIERS) {
        alias.replace(new IdentifierExpr({
          this: (alias as Expression).name,
        }));
      }
    }

    return this.aliasSql(expression);
  }

  aliasesSql (expression: AliasesExpr): string {
    return `${this.sql(expression, 'this')} AS (${this.expressions(expression, { flat: true })})`;
  }

  atIndexSql (expression: AtIndexExpr): string {
    const thisStr = this.sql(expression, 'this');
    const index = this.sql(expression, 'expression');
    return `${thisStr} AT ${index}`;
  }

  atTimeZoneSql (expression: AtTimeZoneExpr): string {
    const thisStr = this.sql(expression, 'this');
    const zone = this.sql(expression, 'zone');
    return `${thisStr} AT TIME ZONE ${zone}`;
  }

  fromTimeZoneSql (expression: FromTimeZoneExpr): string {
    const thisStr = this.sql(expression, 'this');
    const zone = this.sql(expression, 'zone');
    return `${thisStr} AT TIME ZONE ${zone} AT TIME ZONE 'UTC'`;
  }

  addSql (expression: AddExpr): string {
    return this.binary(expression, '+');
  }

  andSql (expression: AndExpr, stack?: (string | Expression)[]): string {
    return this.connectorSql(expression, 'AND', stack);
  }

  orSql (expression: OrExpr, stack?: (string | Expression)[]): string {
    return this.connectorSql(expression, 'OR', stack);
  }

  xorSql (expression: XorExpr, stack?: (string | Expression)[]): string {
    return this.connectorSql(expression, 'XOR', stack);
  }

  connectorSql (
    expression: ConnectorExpr,
    op: string,
    stack?: (string | Expression)[],
  ): string {
    if (stack !== undefined) {
      if (expression.args.expressions && 0 < expression.args.expressions.length) {
        stack.push(this.expressions(expression, { sep: ` ${op} ` }));
      } else {
        stack.push(expression.right || '');
        if (expression.comments && this.comments) {
          for (const comment of expression.comments) {
            if (comment) {
              op += ` /*${this.sanitizeComment(comment)}*/`;
            }
          }
        }
        stack.push(op, expression.left || '');
      }
      return op;
    }

    const localStack: (string | Expression)[] = [expression];
    const sqls: string[] = [];
    const ops = new Set<string>();

    while (0 < localStack.length) {
      const node = localStack.pop()!;

      if (node instanceof ConnectorExpr) {
        const methodName = `${node._constructor.key}Sql` as keyof this;
        const method = this[methodName];

        if (typeof method === 'function') {
          ops.add(method.call(this, node, localStack));
        }
      } else {
        const sql = this.sql(node);
        const lastSql = sqls[sqls.length - 1];

        if (0 < sqls.length && ops.has(lastSql)) {
          sqls[sqls.length - 1] += ` ${sql}`;
        } else {
          sqls.push(sql);
        }
      }
    }

    const sep = this.pretty && this.tooWide(sqls) ? '\n' : ' ';
    return sqls.join(sep);
  }

  bitwiseAndSql (expression: BitwiseAndExpr): string {
    return this.binary(expression, '&');
  }

  bitwiseLeftShiftSql (expression: BitwiseLeftShiftExpr): string {
    return this.binary(expression, '<<');
  }

  bitwiseNotSql (expression: BitwiseNotExpr): string {
    return `~${this.sql(expression, 'this')}`;
  }

  bitwiseOrSql (expression: BitwiseOrExpr): string {
    return this.binary(expression, '|');
  }

  bitwiseRightShiftSql (expression: BitwiseRightShiftExpr): string {
    return this.binary(expression, '>>');
  }

  bitwiseXorSql (expression: BitwiseXorExpr): string {
    return this.binary(expression, '^');
  }

  castSql (expression: Expression, options: { safePrefix?: string } = {}): string {
    const { safePrefix } = options;
    let formatSql = this.sql(expression, 'format');
    formatSql = formatSql ? ` FORMAT ${formatSql}` : '';
    let toSql = this.sql(expression, 'to');
    toSql = toSql ? ` ${toSql}` : '';
    let action = this.sql(expression, 'action');
    action = action ? ` ${action}` : '';
    let defaultSql = this.sql(expression, 'default');
    defaultSql = defaultSql ? ` DEFAULT ${defaultSql} ON CONVERSION ERROR` : '';
    return `${safePrefix || ''}CAST(${this.sql(expression, 'this')} AS${toSql}${defaultSql}${formatSql}${action})`;
  }

  strToTimeSql (expression: StrToTimeExpr): string {
    return this.func('STR_TO_TIME', [expression.args.this ?? '', expression.args.format]);
  }

  currentDateSql (expression: CurrentDateExpr): string {
    const zone = this.sql(expression, 'this');
    return zone ? `CURRENT_DATE(${zone})` : 'CURRENT_DATE';
  }

  collateSql (expression: CollateExpr): string {
    if (this._constructor.COLLATE_IS_FUNC) {
      return this.functionFallbackSql(expression);
    }
    return this.binary(expression, 'COLLATE');
  }

  commandSql (expression: CommandExpr): string {
    const thisStr = this.sql(expression, 'this');
    const exprText = expression.text('expression').trim();
    return `${thisStr} ${exprText}`;
  }

  commentSql (expression: CommentExpr): string {
    const thisStr = this.sql(expression, 'this');
    const kind = expression.args.kind;
    const materialized = expression.args.materialized ? ' MATERIALIZED' : '';
    const existsSql = expression.args.exists ? ' IF EXISTS ' : ' ';
    const expressionSql = this.sql(expression, 'expression');
    return `COMMENT${existsSql}ON${materialized} ${kind} ${thisStr} IS ${expressionSql}`;
  }

  mergeTreeTtlActionSql (expression: MergeTreeTtlActionExpr): string {
    const thisStr = this.sql(expression, 'this');
    const del = expression.args.delete ? ' DELETE' : '';
    const recompress = this.sql(expression, 'recompress');
    const recompressStr = recompress ? ` RECOMPRESS ${recompress}` : '';
    const toDisk = this.sql(expression, 'toDisk');
    const toDiskStr = toDisk ? ` TO DISK ${toDisk}` : '';
    const toVolume = this.sql(expression, 'toVolume');
    const toVolumeStr = toVolume ? ` TO VOLUME ${toVolume}` : '';
    return `${thisStr}${del}${recompressStr}${toDiskStr}${toVolumeStr}`;
  }

  mergeTreeTtlSql (expression: MergeTreeTtlExpr): string {
    const where = this.sql(expression, 'where');
    const group = this.sql(expression, 'group');
    const aggregates = this.expressions(expression, { key: 'aggregates' });
    const aggregatesStr = aggregates ? `${this.seg('SET')}${this.seg(aggregates)}` : '';
    if (!where && !group && !aggregatesStr && expression.args.expressions?.length === 1) {
      return `TTL ${this.expressions(expression, { flat: true })}`;
    }
    return `TTL${this.seg(this.expressions(expression))}${where}${group}${aggregatesStr}`;
  }

  transactionSql (expression: TransactionExpr): string {
    const modes = this.expressions(expression, { key: 'modes' });
    const modesStr = modes ? ` ${modes}` : '';
    return `BEGIN${modesStr}`;
  }

  commitSql (expression: CommitExpr): string {
    const chain = expression.args.chain;
    let chainStr = '';
    if (chain !== undefined) {
      chainStr = chain ? ' AND CHAIN' : ' AND NO CHAIN';
    }
    return `COMMIT${chainStr}`;
  }

  rollbackSql (expression: RollbackExpr): string {
    const savepoint = expression.args.savepoint;
    const savepointStr = savepoint ? ` TO ${savepoint}` : '';
    return `ROLLBACK${savepointStr}`;
  }

  alterColumnSql (expression: AlterColumnExpr): string {
    const thisStr = this.sql(expression, 'this');

    const dtype = this.sql(expression, 'dtype');
    if (dtype) {
      let collate = this.sql(expression, 'collate');
      collate = collate ? ` COLLATE ${collate}` : '';
      let using = this.sql(expression, 'using');
      using = using ? ` USING ${using}` : '';
      const alterSetType = this._constructor.ALTER_SET_TYPE;
      const alterSetTypeStr = alterSetType ? `${alterSetType} ` : '';
      return `ALTER COLUMN ${thisStr} ${alterSetTypeStr}${dtype}${collate}${using}`;
    }

    const defaultVal = this.sql(expression, 'default');
    if (defaultVal) {
      return `ALTER COLUMN ${thisStr} SET DEFAULT ${defaultVal}`;
    }

    const comment = this.sql(expression, 'comment');
    if (comment) {
      return `ALTER COLUMN ${thisStr} COMMENT ${comment}`;
    }

    const visible = expression.args.visible;
    if (visible) {
      return `ALTER COLUMN ${thisStr} SET ${visible}`;
    }

    const allowNull = expression.args.allowNull;
    const drop = expression.args.drop;

    if (!drop && !allowNull) {
      this.unsupported('Unsupported ALTER COLUMN syntax');
    }

    if (allowNull !== undefined) {
      const keyword = drop ? 'DROP' : 'SET';
      return `ALTER COLUMN ${thisStr} ${keyword} NOT NULL`;
    }

    return `ALTER COLUMN ${thisStr} DROP DEFAULT`;
  }

  alterIndexSql (expression: AlterIndexExpr): string {
    const thisStr = this.sql(expression, 'this');
    const visible = expression.args.visible;
    const visibleSql = visible ? 'VISIBLE' : 'INVISIBLE';
    return `ALTER INDEX ${thisStr} ${visibleSql}`;
  }

  alterDistStyleSql (expression: AlterDistStyleExpr): string {
    let thisStr = this.sql(expression, 'this');
    if (!(expression.args.this instanceof VarExpr)) {
      thisStr = `KEY DISTKEY ${thisStr}`;
    }
    return `ALTER DISTSTYLE ${thisStr}`;
  }

  alterSortKeySql (expression: AlterSortKeyExpr): string {
    const compound = expression.args.compound ? ' COMPOUND' : '';
    const thisStr = this.sql(expression, 'this');
    let expressions = this.expressions(expression, { flat: true });
    expressions = expressions ? `(${expressions})` : '';
    return `ALTER${compound} SORTKEY ${thisStr || expressions}`;
  }

  alterRenameSql (expression: Expression, options: { includeTo?: boolean } = {}): string {
    const { includeTo = true } = options;
    let expr = expression;
    if (!this._constructor.RENAME_TABLE_WITH_DB) {
      // Remove db from tables
      expr = expression.transform((n) => {
        if (n instanceof TableExpr) {
          return new TableExpr({ this: n.args.this });
        }
        return n;
      }).assertIs(AlterRenameExpr);
    }
    const thisStr = this.sql(expr, 'this');
    const toKw = includeTo ? ' TO' : '';
    return `RENAME${toKw} ${thisStr}`;
  }

  renameColumnSql (expression: RenameColumnExpr): string {
    const exists = expression.args.exists ? ' IF EXISTS' : '';
    const oldColumn = this.sql(expression, 'this');
    const newColumn = this.sql(expression, 'to');
    return `RENAME COLUMN${exists} ${oldColumn} TO ${newColumn}`;
  }

  alterSetSql (expression: AlterSetExpr): string {
    let exprs = this.expressions(expression, { flat: true });
    if (this._constructor.ALTER_SET_WRAPPED) {
      exprs = `(${exprs})`;
    }
    return `SET ${exprs}`;
  }

  alterSql (expression: AlterExpr): string {
    const actions = expression.args.actions || [];

    let actionsSql: string;
    if (!(this.dialect._constructor.ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN
      && actions[0] instanceof ColumnDefExpr)) {
      actionsSql = this.expressions(expression, {
        key: 'actions',
        flat: true,
      });
      actionsSql = `ADD ${actionsSql}`;
    } else {
      const actionsList: string[] = [];
      for (const action of actions) {
        const actionExpr = action as Expression;
        let actionSql: string;
        if (actionExpr instanceof ColumnDefExpr || actionExpr instanceof SchemaExpr) {
          actionSql = this.addColumnSql(actionExpr);
        } else {
          actionSql = this.sql(actionExpr);
          if (actionExpr instanceof SelectExpr || actionExpr instanceof UnionExpr) {
            actionSql = `AS ${actionSql}`;
          }
        }
        actionsList.push(actionSql);
      }
      actionsSql = this.formatArgs(actionsList).replace(/^\n+/, '');
    }

    const exists = expression.args.exists ? ' IF EXISTS' : '';
    let onCluster = this.sql(expression, 'cluster');
    onCluster = onCluster ? ` ${onCluster}` : '';
    const only = expression.args.only ? ' ONLY' : '';
    let options = this.expressions(expression, { key: 'options' });
    options = options ? `, ${options}` : '';
    const kind = this.sql(expression, 'kind');
    const notValid = expression.args.notValid ? ' NOT VALID' : '';
    const check = expression.args.check ? ' WITH CHECK' : '';
    const cascade = expression.args.cascade && this.dialect._constructor.ALTER_TABLE_SUPPORTS_CASCADE
      ? ' CASCADE'
      : '';
    let thisStr = this.sql(expression, 'this');
    thisStr = thisStr ? ` ${thisStr}` : '';

    return `ALTER ${kind}${exists}${only}${thisStr}${onCluster}${check}${this.sep()}${actionsSql}${notValid}${options}${cascade}`;
  }

  alterSessionSql (expression: AlterSessionExpr): string {
    const itemsSql = this.expressions(expression, { flat: true });
    const keyword = expression.args.unset ? 'UNSET' : 'SET';
    return `${keyword} ${itemsSql}`;
  }

  addColumnSql (expression: Expression): string {
    const sql = this.sql(expression);
    let columnText: string;
    if (expression instanceof SchemaExpr) {
      columnText = ' COLUMNS';
    } else if (expression instanceof ColumnDefExpr
      && this._constructor.ALTER_TABLE_INCLUDE_COLUMN_KEYWORD) {
      columnText = ' COLUMN';
    } else {
      columnText = '';
    }
    return `ADD${columnText} ${sql}`;
  }

  dropPartitionSql (expression: DropPartitionExpr): string {
    const expressions = this.expressions(expression);
    const exists = expression.args.exists ? ' IF EXISTS ' : ' ';
    return `DROP${exists}${expressions}`;
  }

  addConstraintSql (expression: AddConstraintExpr): string {
    return `ADD ${this.expressions(expression, { indent: false })}`;
  }

  addPartitionSql (expression: AddPartitionExpr): string {
    const exists = expression.args.exists ? 'IF NOT EXISTS ' : '';
    let location = this.sql(expression, 'location');
    location = location ? ` ${location}` : '';
    return `ADD ${exists}${this.sql(expression, 'this')}${location}`;
  }

  distinctSql (expression: DistinctExpr): string {
    let thisSql = this.expressions(expression, { flat: true });

    // Handle dialects that don't support multiple arguments in DISTINCT
    // by wrapping them in a CASE statement logic
    if (!this._constructor.MULTI_ARG_DISTINCT && 1 < (expression.args.expressions?.length ?? 0)) {
      let caseAst = case_();

      for (const arg of expression.args.expressions || []) {
        caseAst = caseAst.when(arg.is(null_()), null_());
      }

      thisSql = this.sql(caseAst.else(`(${thisSql})`));
    }

    thisSql = thisSql ? ` ${thisSql}` : '';

    let on = this.sql(expression, 'on');
    on = on ? ` ON ${on}` : '';

    return `DISTINCT${thisSql}${on}`;
  }

  ignoreNullsSql (expression: IgnoreNullsExpr): string {
    return this.embedIgnoreNulls(expression, 'IGNORE NULLS');
  }

  respectNullsSql (expression: RespectNullsExpr): string {
    return this.embedIgnoreNulls(expression, 'RESPECT NULLS');
  }

  havingMaxSql (expression: HavingMaxExpr): string {
    const thisStr = this.sql(expression, 'this');
    const expressionSql = this.sql(expression, 'expression');
    const kind = expression.args.max ? 'MAX' : 'MIN';
    return `${thisStr} HAVING ${kind} ${expressionSql}`;
  }

  intDivSql (expression: IntDivExpr): string {
    return this.sql(new CastExpr({
      this: new DivExpr({
        this: expression.args.this,
        expression: expression.args.expression,
      }),
      to: new DataTypeExpr({ this: DataTypeExprKind.INT }),
    }));
  }

  dPipeSql (expression: DPipeExpr): string {
    if (this.dialect._constructor.STRICT_STRING_CONCAT && expression.args.safe) {
      const flatParts = Array.from(expression.flatten())
        .map((e) => cast(e, DataTypeExprKind.TEXT, { copy: false }));
      return this.func('CONCAT', flatParts);
    }
    return this.binary(expression, '||');
  }

  divSql (expression: DivExpr): string {
    const l = expression.left;
    const r = expression.right;

    // Handle SAFE_DIVISION by wrapping the divisor in NULLIF(r, 0)
    if (!this.dialect._constructor.SAFE_DIVISION && expression.args.safe) {
      r?.replace(
        new NullifExpr({
          this: r.copy(),
          expression: LiteralExpr.number(0),
        }),
      );
    }

    // Handle TYPED_DIVISION: Cast to DOUBLE if neither side is a real type
    if (this.dialect._constructor.TYPED_DIVISION && !expression.args.typed) {
      if (!l?.isType(Array.from(DataTypeExpr.REAL_TYPES)) && !r?.isType(Array.from(DataTypeExpr.REAL_TYPES))) {
        l?.replace(cast(l.copy(), DataTypeExprKind.DOUBLE));
      }
    } else if (!this.dialect._constructor.TYPED_DIVISION && expression.args.typed) {
      // Handle non-TYPED_DIVISION: Cast result to BIGINT if both sides are integers
      if (l?.isType(Array.from(DataTypeExpr.INTEGER_TYPES)) && r?.isType(Array.from(DataTypeExpr.INTEGER_TYPES))) {
        return this.sql(
          cast(
            l.div(r),
            DataTypeExprKind.BIGINT,
          ),
        );
      }
    }

    return this.binary(expression, '/');
  }

  safeDivideSql (expression: SafeDivideExpr): string {
    const n = wrap(expression.args.this, BinaryExpr);
    const d = wrap(expression.args.expression, BinaryExpr);
    if (!n || !d) return '';
    return this.sql(
      new IfExpr({
        this: d.neq(0),
        true: n.div(d),
        false: null_(),
      }),
    );
  }

  overlapsSql (expression: OverlapsExpr): string {
    return this.binary(expression, 'OVERLAPS');
  }

  distanceSql (expression: DistanceExpr): string {
    return this.binary(expression, '<->');
  }

  dotSql (expression: DotExpr): string {
    return `${this.sql(expression, 'this')}.${this.sql(expression, 'expression')}`;
  }

  eqSql (expression: EqExpr): string {
    return this.binary(expression, '=');
  }

  propertyEqSql (expression: PropertyEqExpr): string {
    return this.binary(expression, ':=');
  }

  escapeSql (expression: EscapeExpr): string {
    return this.binary(expression, 'ESCAPE');
  }

  globSql (expression: GlobExpr): string {
    return this.binary(expression, 'GLOB');
  }

  gtSql (expression: GtExpr): string {
    return this.binary(expression, '>');
  }

  gteSql (expression: GteExpr): string {
    return this.binary(expression, '>=');
  }

  isSql (expression: IsExpr): string {
    if (!this._constructor.IS_BOOL_ALLOWED && expression.args.expression instanceof BooleanExpr) {
      const thisArg = expression.args.this;
      return this.sql(
        expression.args.expression.args.this
          ? thisArg
          : not(thisArg instanceof Expression ? thisArg : typeof thisArg === 'string' ? thisArg : undefined),
      );
    }

    return this.binary(expression, 'IS');
  }

  likeSql (expression: LikeExpr | ILikeExpr): string {
    const thisExpr = expression.args.this;
    const rhs = expression.args.expression;

    let expClass: typeof Expression;
    let op: string;

    if (expression instanceof LikeExpr) {
      expClass = LikeExpr;
      op = 'LIKE';
    } else {
      expClass = ILikeExpr;
      op = 'ILIKE';
    }

    // Check if we are dealing with 'LIKE ANY' or 'LIKE ALL' when the dialect doesn't support it
    if ((rhs instanceof AllExpr || rhs instanceof AnyExpr) && !this._constructor.SUPPORTS_LIKE_QUANTIFIERS) {
      let exprs: ExpressionValue[] | undefined = rhs.args.this instanceof Expression ? [rhs.args.this.unnest()] : [];

      if (exprs instanceof TupleExpr) {
        exprs = exprs.args.expressions;
      } else if (exprs && !Array.isArray(exprs)) {
        // Handle cases where unnest might return a single expression
        exprs = [exprs];
      }

      const connective = rhs instanceof AnyExpr ? or : and;

      // Build the expanded expression: (this LIKE expr1 OR this LIKE expr2...)
      let likeExpr: Expression = new expClass({
        this: thisExpr,
        expression: exprs?.[0],
      });

      for (let i = 1; i < (exprs?.length || 0); i++) {
        likeExpr = connective([
          likeExpr,
          new expClass({
            this: thisExpr,
            expression: exprs?.[i] || 0,
          }),
        ]);
      }

      const parent = expression.parent;

      // Wrap in parentheses if the expansion happens within another condition to maintain precedence
      if (parent instanceof ConditionExpr && !(parent instanceof likeExpr._constructor)) {
        likeExpr = paren(likeExpr, { copy: false });
      }

      return this.sql(likeExpr);
    }

    return this.binary(expression, op);
  }

  iLikeSql (expression: ILikeExpr): string {
    return this.likeSql(expression);
  }

  matchSql (expression: MatchExpr): string {
    return this.binary(expression, 'MATCH');
  }

  similarToSql (expression: SimilarToExpr): string {
    return this.binary(expression, 'SIMILAR TO');
  }

  ltSql (expression: LtExpr): string {
    return this.binary(expression, '<');
  }

  lteSql (expression: LteExpr): string {
    return this.binary(expression, '<=');
  }

  modSql (expression: ModExpr): string {
    return this.binary(expression, '%');
  }

  mulSql (expression: MulExpr): string {
    return this.binary(expression, '*');
  }

  neqSql (expression: NeqExpr): string {
    return this.binary(expression, '<>');
  }

  nullSafeEqSql (expression: NullSafeEqExpr): string {
    return this.binary(expression, 'IS NOT DISTINCT FROM');
  }

  nullSafeNeqSql (expression: NullSafeNeqExpr): string {
    return this.binary(expression, 'IS DISTINCT FROM');
  }

  subSql (expression: SubExpr): string {
    return this.binary(expression, '-');
  }

  tryCastSql (expression: TryCastExpr): string {
    return this.castSql(expression, { safePrefix: 'TRY_' });
  }

  jsonCastSql (expression: JsonCastExpr): string {
    return this.castSql(expression);
  }

  trySql (expression: TryExpr): string {
    if (!this._constructor.TRY_SUPPORTED) {
      this.unsupported('Unsupported TRY function');
      return this.sql(expression, 'this');
    }
    return this.func('TRY', [expression.args.this]);
  }

  logSql (expression: LogExpr): string {
    let thisExpr: Expression | undefined = expression.args.this;
    let exprArg: Expression | undefined = expression.args.expression;
    if (this.dialect._constructor.LOG_BASE_FIRST === false) {
      [thisExpr, exprArg] = [exprArg, thisExpr];
    } else if (this.dialect._constructor.LOG_BASE_FIRST === undefined && exprArg) {
      if (!thisExpr) return '';
      const baseName = thisExpr.name;
      if (baseName === '2' || baseName === '10') {
        return this.func(`LOG${baseName}`, [exprArg]);
      }
      this.unsupported(`Unsupported logarithm with base ${this.sql(thisExpr)}`);
    }
    return this.func('LOG', [thisExpr, exprArg]);
  }

  useSql (expression: UseExpr): string {
    const kind = this.sql(expression, 'kind');
    const kindStr = kind ? ` ${kind}` : '';
    const thisStr = this.sql(expression, 'this') || this.expressions(expression, { flat: true });
    const thisResult = thisStr ? ` ${thisStr}` : '';
    return `USE${kindStr}${thisResult}`;
  }

  binary (expression: BinaryExpr, op: string): string {
    const sqls: string[] = [];
    const stack: (string | Expression)[] = [expression];

    while (0 < stack.length) {
      const node = stack.pop()!;

      if (node instanceof BinaryExpr) {
        const opFunc = node.args.operator;
        if (opFunc) {
          op = `OPERATOR(${this.sql(opFunc)})`;
        }

        stack.push(node.right || '');
        stack.push(` ${this.maybeComment(op, undefined, node.comments)} `);
        stack.push(node.left || '');
      } else {
        sqls.push(this.sql(node));
      }
    }

    return sqls.join('');
  }

  ceilFloor (expression: CeilExpr | FloorExpr): string {
    const toClause = this.sql(expression, 'to');

    if (toClause) {
      return `${(expression._constructor as typeof FuncExpr).sqlName}(${this.sql(expression, 'this')} TO ${toClause})`;
    }

    return this.functionFallbackSql(expression);
  }

  functionFallbackSql (expression: FuncExpr | JsonExtractExpr): string {
    const args: (Expression | string)[] = [];

    for (const key of expression._constructor.availableArgs) {
      const argValue = expression.getArgKey(key);

      if (Array.isArray(argValue)) {
        for (const value of argValue) {
          if (value instanceof Expression || typeof value === 'string') args.push(value);
        }
      } else if ((argValue !== undefined && argValue instanceof Expression) || typeof argValue === 'string') {
        args.push(argValue);
      }
    }

    let name: string | undefined;
    if (this.dialect._constructor.PRESERVE_ORIGINAL_NAMES) {
      name = (expression.meta?.name as string | undefined) || (expression._constructor as typeof FuncExpr).sqlName();
    } else {
      name = (expression._constructor as typeof FuncExpr).sqlName();
    }

    return this.func(name, args);
  }

  func (
    name: string,
    args: (ExpressionValue | undefined)[],
    options: {
      prefix?: string;
      suffix?: string;
      normalize?: boolean;
    } = {},
  ): string {
    const {
      prefix = '(', suffix = ')', normalize = true,
    } = options;
    const funcName = normalize ? this.normalizeFunc(name) : name;
    return `${funcName}${prefix}${this.formatArgs(args)}${suffix}`;
  }

  formatArgs (
    args: (string | number | boolean | Expression | undefined)[],
    options: { sep?: string } = {},
  ): string {
    const { sep = ', ' } = options;
    const argSqls = args
      .filter((arg) => arg !== undefined && typeof arg !== 'boolean')
      .map((arg) => this.sql(arg));

    if (this.pretty && this.tooWide(argSqls)) {
      const joined = argSqls.join(`\n${sep.trim()}\n`);
      return this.indent(`\n${joined}\n`, {
        skipFirst: true,
        skipLast: true,
      });
    }

    return argSqls.join(sep);
  }

  tooWide (args: string[]): boolean {
    const totalLength = args.reduce((acc, arg) => acc + arg.length, 0);
    return this.maxTextWidth < totalLength;
  }

  expressions (
    expression?: Expression | undefined,
    options: {
      sqls?: (string | Expression)[];
      key?: string;
      flat?: boolean;
      indent?: boolean;
      skipFirst?: boolean;
      skipLast?: boolean;
      sep?: string;
      prefix?: string;
      dynamic?: boolean;
      newLine?: boolean;
    } = {},
  ): string {
    const {
      flat = false,
      indent = true,
      skipFirst = false,
      skipLast = false,
      sep = ', ',
      prefix = '',
      dynamic = false,
      newLine = false,
      key = 'expressions',
      sqls,
    } = options;

    const exprs = expression ? expression.getArgKey(key) : sqls;

    if (!exprs || (Array.isArray(exprs) && exprs.length === 0)) {
      return '';
    }

    const exprArray = (Array.isArray(exprs) ? exprs : [exprs]).filter((e): e is Expression | string => e instanceof Expression || typeof e === 'string');

    if (flat) {
      return exprArray
        .map((e) => this.sql(e))
        .filter((sql) => sql)
        .join(sep);
    }

    const numSqls = exprArray.length;
    const resultSqls: string[] = [];

    for (let i = 0; i < numSqls; i++) {
      const e = exprArray[i];
      const sql = this.sql(e, undefined, { comment: false });
      if (!sql) continue;

      const comments = e instanceof Expression ? this.maybeComment('', e) : '';

      if (this.pretty) {
        if (this.leadingComma) {
          resultSqls.push(`${0 < i ? sep : ''}${prefix}${sql}${comments}`);
        } else {
          const separator = i + 1 < numSqls ? (comments ? sep.trimEnd() : sep) : '';
          resultSqls.push(`${prefix}${sql}${separator}${comments}`);
        }
      } else {
        resultSqls.push(`${prefix}${sql}${comments}${i + 1 < numSqls ? sep : ''}`);
      }
    }

    let resultSql: string;

    if (this.pretty && (!dynamic || this.tooWide(resultSqls))) {
      if (newLine) {
        resultSqls.unshift('');
        resultSqls.push('');
      }
      resultSql = resultSqls.map((s) => s.trimEnd()).join('\n');
    } else {
      resultSql = resultSqls.join('');
    }

    return indent
      ? this.indent(resultSql, {
        skipFirst,
        skipLast,
      })
      : resultSql;
  }

  opExpressions (op: string, expression: Expression, options: { flat?: boolean } = {}): string {
    const { flat = false } = options;
    const expressionsSql = this.expressions(expression, { flat });
    if (flat) {
      return `${op} ${expressionsSql}`;
    }
    return `${this.seg(op)}${expressionsSql
      ? this.sep()
      : ''}${expressionsSql}`;
  }

  nakedProperty (expression: PropertyExpr): string {
    const propertyName = PropertiesExpr.PROPERTY_TO_NAME[expression._constructor.name];
    if (!propertyName) {
      this.unsupported(`Unsupported property ${expression._constructor.name}`);
    }
    return `${propertyName || ''} ${this.sql(expression, 'this')}`;
  }

  tagSql (expression: TagExpr): string {
    const prefix = expression.args.prefix || '';
    const postfix = expression.args.postfix || '';
    return `${prefix}${this.sql(expression.args.this)}${postfix}`;
  }

  protected tokenSql (tokenType: TokenType): string {
    return this._constructor.TOKEN_MAPPING[tokenType] ?? tokenType;
  }

  userDefinedFunctionSql (expression: UserDefinedFunctionExpr): string {
    const thisStr = this.sql(expression, 'this');
    let expressions = this.noIdentify(this.expressions.bind(this), expression);
    if (expression.args.wrapped) {
      expressions = this.wrap(expressions);
    } else {
      expressions = expressions ? ` ${expressions}` : '';
    }
    return expressions.trim() !== '' ? `${thisStr}${expressions}` : thisStr;
  }

  joinHintSql (expression: JoinHintExpr): string {
    const thisStr = this.sql(expression, 'this');
    const expressions = this.expressions(expression, { flat: true });
    return `${thisStr}(${expressions})`;
  }

  kwargSql (expression: KwargExpr): string {
    return this.binary(expression, '=>');
  }

  whenSql (expression: WhenExpr): string {
    const matched = expression.args.matched ? 'MATCHED' : 'NOT MATCHED';
    const source = this._constructor.MATCHED_BY_SOURCE && expression.args.source ? ' BY SOURCE' : '';
    const condition = this.sql(expression, 'condition');
    const conditionStr = condition ? ` AND ${condition}` : '';

    const thenExpression = expression.args.then;
    let then = '';
    if (thenExpression instanceof InsertExpr) {
      let thisStr = this.sql(thenExpression, 'this');
      thisStr = thisStr ? `INSERT ${thisStr}` : 'INSERT';
      const thenExpr = this.sql(thenExpression, 'expression');
      then = thenExpr ? `${thisStr} VALUES ${thenExpr}` : thisStr;
    } else if (thenExpression instanceof UpdateExpr) {
      if (thenExpression.getArgKey('expressions') instanceof StarExpr) {
        then = `UPDATE ${this.sql(thenExpression, 'expressions')}`;
      } else {
        const expressionsSql = this.expressions(thenExpression);
        then = expressionsSql ? `UPDATE SET${this.sep()}${expressionsSql}` : 'UPDATE';
      }
    } else {
      then = this.sql(thenExpression);
    }
    return `WHEN ${matched}${source}${conditionStr} THEN ${then}`;
  }

  whensSql (expression: WhensExpr): string {
    return this.expressions(expression, {
      sep: ' ',
      indent: false,
    });
  }

  mergeSql (expression: MergeExpr): string {
    const table = expression.args.this;
    let tableAlias = '';

    if (isInstanceOf(table, TableExpr)) {
      const hints: Expression[] = table.args.hints || [];
      if (0 < hints.length && table.alias && hints[0] instanceof WithTableHintExpr) {
        // T-SQL syntax is MERGE ... <target_table> [WITH (<merge_hint>)] [[AS] table_alias]
        const aliasRaw = table.args.alias;
        const aliasExpr = isInstanceOf(aliasRaw, Expression) ? aliasRaw.pop() : undefined;
        tableAlias = aliasExpr ? ` AS ${this.sql(aliasExpr)}` : '';
      }
    }

    const thisStr = this.sql(table);
    const using = `USING ${this.sql(expression, 'using')}`;
    let whens = this.sql(expression, 'whens');

    let on = this.sql(expression, 'on');
    on = on ? `ON ${on}` : '';

    if (!on) {
      const usingCond = this.expressions(expression, { key: 'usingCond' });
      on = usingCond ? `USING (${usingCond})` : '';
    }

    const returning = this.sql(expression, 'returning');
    if (returning) {
      whens = `${whens}${returning}`;
    }

    const sep = this.sep();

    return this.prependCtes(
      expression,
      `MERGE INTO ${thisStr}${tableAlias}${sep}${using}${sep}${on}${sep}${whens}`,
    );
  }

  toCharSql (expression: ToCharExpr): string {
    unsupportedArgs.call(this, expression, 'format');
    return this.sql(cast(expression.args.this, DataTypeExprKind.TEXT));
  }

  toNumberSql (expression: ToNumberExpr): string {
    if (!this._constructor.SUPPORTS_TO_NUMBER) {
      this.unsupported('Unsupported TO_NUMBER function');
      return this.sql(cast(expression.args.this as Expression, DataTypeExprKind.DOUBLE));
    }
    const fmt = expression.args.format;
    if (!fmt) {
      this.unsupported('Conversion format is required for TO_NUMBER');
      return this.sql(cast(expression.args.this as Expression, DataTypeExprKind.DOUBLE));
    }
    return this.func('TO_NUMBER', [expression.args.this, fmt]);
  }

  dictPropertySql (expression: DictPropertyExpr): string {
    const thisStr = this.sql(expression, 'this');
    const kind = this.sql(expression, 'kind');
    const settingsSql = this.expressions(expression, {
      key: 'settings',
      sep: ' ',
    });
    const args = settingsSql
      ? `(${this.sep('')}${settingsSql}${this.seg(')', '')}`
      : '()';
    return `${thisStr}(${kind}${args})`;
  }

  dictRangeSql (expression: DictRangeExpr): string {
    const thisStr = this.sql(expression, 'this');
    const max = this.sql(expression, 'max');
    const min = this.sql(expression, 'min');
    return `${thisStr}(MIN ${min} MAX ${max})`;
  }

  dictSubPropertySql (expression: DictSubPropertyExpr): string {
    return `${this.sql(expression, 'this')} ${this.sql(expression, 'value')}`;
  }

  duplicateKeyPropertySql (expression: DuplicateKeyPropertyExpr): string {
    return `DUPLICATE KEY (${this.expressions(expression, { flat: true })})`;
  }

  distributedByPropertySql (expression: DistributedByPropertyExpr): string {
    let expressions = this.expressions(expression, { flat: true });
    expressions = expressions ? ` ${this.wrap(expressions)}` : '';
    const buckets = this.sql(expression, 'buckets');
    const kind = this.sql(expression, 'kind');
    const bucketsStr = buckets ? ` BUCKETS ${buckets}` : '';
    const order = this.sql(expression, 'order');
    return `DISTRIBUTED BY ${kind}${expressions}${bucketsStr}${order}`;
  }

  onClusterSql (_expression: OnClusterExpr): string {
    return '';
  }

  clusteredByPropertySql (expression: ClusteredByPropertyExpr): string {
    const expressions = this.expressions(expression, {
      key: 'expressions',
      flat: true,
    });
    let sortedBy = this.expressions(expression, {
      key: 'sortedBy',
      flat: true,
    });
    sortedBy = sortedBy ? ` SORTED BY (${sortedBy})` : '';
    const buckets = this.sql(expression, 'buckets');
    return `CLUSTERED BY (${expressions})${sortedBy} INTO ${buckets} BUCKETS`;
  }

  anyValueSql (expression: AnyValueExpr): string {
    const thisStr = this.sql(expression, 'this');
    const having = this.sql(expression, 'having');
    if (having) {
      const max = expression.args.max;
      return this.func('ANY_VALUE', [`${thisStr} HAVING ${max ? 'MAX' : 'MIN'} ${having}`]);
    }
    return this.func('ANY_VALUE', [expression.args.this]);
  }

  queryTransformSql (expression: QueryTransformExpr): string {
    const transform = this.func('TRANSFORM', expression.args.expressions || []);
    let rowFormatBefore = this.sql(expression, 'rowFormatBefore');
    rowFormatBefore = rowFormatBefore ? ` ${rowFormatBefore}` : '';
    let recordWriter = this.sql(expression, 'recordWriter');
    recordWriter = recordWriter ? ` RECORDWRITER ${recordWriter}` : '';
    const using = ` USING ${this.sql(expression, 'commandScript')}`;
    let schema = this.sql(expression, 'schema');
    schema = schema ? ` AS ${schema}` : '';
    let rowFormatAfter = this.sql(expression, 'rowFormatAfter');
    rowFormatAfter = rowFormatAfter ? ` ${rowFormatAfter}` : '';
    let recordReader = this.sql(expression, 'recordReader');
    recordReader = recordReader ? ` RECORDREADER ${recordReader}` : '';
    return `${transform}${rowFormatBefore}${recordWriter}${using}${schema}${rowFormatAfter}${recordReader}`;
  }

  indexConstraintOptionSql (expression: IndexConstraintOptionExpr): string {
    const keyBlockSize = this.sql(expression, 'keyBlockSize');
    if (keyBlockSize) {
      return `KEY_BLOCK_SIZE = ${keyBlockSize}`;
    }

    const using = this.sql(expression, 'using');
    if (using) {
      return `USING ${using}`;
    }

    const parser = this.sql(expression, 'parser');
    if (parser) {
      return `WITH PARSER ${parser}`;
    }

    const comment = this.sql(expression, 'comment');
    if (comment) {
      return `COMMENT ${comment}`;
    }

    const visible = expression.args.visible;
    if (visible !== undefined) {
      return visible ? 'VISIBLE' : 'INVISIBLE';
    }

    const engineAttr = this.sql(expression, 'engineAttr');
    if (engineAttr) {
      return `ENGINE_ATTRIBUTE = ${engineAttr}`;
    }

    const secondaryEngineAttr = this.sql(expression, 'secondaryEngineAttr');
    if (secondaryEngineAttr) {
      return `SECONDARY_ENGINE_ATTRIBUTE = ${secondaryEngineAttr}`;
    }

    this.unsupported('Unsupported index constraint option.');
    return '';
  }

  checkColumnConstraintSql (expression: CheckColumnConstraintExpr): string {
    const enforced = expression.args.enforced ? ' ENFORCED' : '';
    return `CHECK (${this.sql(expression, 'this')})${enforced}`;
  }

  indexColumnConstraintSql (expression: IndexColumnConstraintExpr): string {
    const kind = this.sql(expression, 'kind');
    const kindStr = kind ? `${kind} INDEX` : 'INDEX';
    let thisStr = this.sql(expression, 'this');
    thisStr = thisStr ? ` ${thisStr}` : '';
    let indexType = this.sql(expression, 'indexType');
    indexType = indexType ? ` USING ${indexType}` : '';
    let expressions = this.expressions(expression, { flat: true });
    expressions = expressions ? ` (${expressions})` : '';
    let options = this.expressions(expression, {
      key: 'options',
      flat: true,
      sep: ' ',
    });
    options = options ? ` ${options}` : '';
    return `${kindStr}${thisStr}${indexType}${expressions}${options}`;
  }

  nvl2Sql (expression: Nvl2Expr): string {
    if (this._constructor.NVL2_SUPPORTED) {
      return this.functionFallbackSql(expression);
    }

    const nvl2This = expression.args.this;
    if (!nvl2This) return '';
    const caseExpr = new CaseExpr({})
      .when(
        nvl2This.is(null_()).not({ copy: false }),
        expression.args.true ?? '',
        { copy: false },
      );

    const elseCond = expression.args.false;
    if (elseCond) {
      caseExpr.else(elseCond, { copy: false });
    }

    return this.sql(caseExpr);
  }

  comprehensionSql (expression: ComprehensionExpr): string {
    const thisStr = this.sql(expression, 'this');
    const expr = this.sql(expression, 'expression');
    const position = this.sql(expression, 'position');
    const positionStr = position ? `, ${position}` : '';
    const iterator = this.sql(expression, 'iterator');
    const condition = this.sql(expression, 'condition');
    const conditionStr = condition ? ` IF ${condition}` : '';
    return `${thisStr} FOR ${expr}${positionStr} IN ${iterator}${conditionStr}`;
  }

  columnPrefixSql (expression: ColumnPrefixExpr): string {
    return `${this.sql(expression, 'this')}(${this.sql(expression, 'expression')})`;
  }

  opclassSql (expression: OpclassExpr): string {
    return `${this.sql(expression, 'this')} ${this.sql(expression, 'expression')}`;
  }

  mlSql (expression: FuncExpr, name: string): string {
    const model = `MODEL ${this.sql(expression, 'this')}`;
    const exprNode = expression.args.expression;
    let exprSql: string | undefined;
    if (exprNode) {
      const raw = this.sql(expression, 'expression');
      exprSql = !(exprNode instanceof SubqueryExpr) ? `TABLE ${raw}` : raw;
    }
    const parameters = this.sql(expression, 'paramsStruct') || undefined;
    return this.func(name, [
      model,
      exprSql,
      parameters,
    ]);
  }

  predictSql (expression: PredictExpr): string {
    return this.mlSql(expression, 'PREDICT');
  }

  generateEmbeddingSql (expression: GenerateEmbeddingExpr): string {
    const name = expression.args.isText ? 'GENERATE_TEXT_EMBEDDING' : 'GENERATE_EMBEDDING';
    return this.mlSql(expression, name);
  }

  mlTranslateSql (expression: MlTranslateExpr): string {
    return this.mlSql(expression, 'TRANSLATE');
  }

  mlForecastSql (expression: MlForecastExpr): string {
    return this.mlSql(expression, 'FORECAST');
  }

  featuresAtTimeSql (expression: FeaturesAtTimeExpr): string {
    const thisNode = expression.args.this;
    let thisStr = this.sql(expression, 'this');
    if (thisNode instanceof TableExpr) {
      thisStr = `TABLE ${thisStr}`;
    }
    return this.func(
      'FEATURES_AT_TIME',
      [
        thisStr,
        expression.args.time,
        ...(expression.args.numRows || [undefined]),
        ...(expression.args.ignoreFeatureNulls || [undefined]),
      ],
    );
  }

  vectorSearchSql (expression: VectorSearchExpr): string {
    let thisStr = this.sql(expression, 'this');
    if (expression.args.this instanceof TableExpr) {
      thisStr = `TABLE ${thisStr}`;
    }
    const queryTable = expression.args.queryTable;
    let queryTableStr = queryTable ? this.sql(queryTable) : undefined;
    if (queryTable instanceof TableExpr) {
      queryTableStr = `TABLE ${queryTableStr}`;
    }
    return this.func(
      'VECTOR_SEARCH',
      [
        thisStr,
        expression.args.columnToSearch,
        queryTableStr,
        expression.args.queryColumnToSearch,
        expression.args.topK,
        expression.args.distanceType,
        ...(expression.args.options || [undefined]),
      ],
    );
  }

  forInSql (expression: ForInExpr): string {
    const thisStr = this.sql(expression, 'this');
    const expressionSql = this.sql(expression, 'expression');
    return `FOR ${thisStr} DO ${expressionSql}`;
  }

  refreshSql (expression: RefreshExpr): string {
    const thisStr = this.sql(expression, 'this');
    const isLiteral = expression.args.this instanceof LiteralExpr;
    const kind = isLiteral ? '' : `${expression.args.kind || ''} `;
    return `REFRESH ${kind}${thisStr}`;
  }

  toArraySql (expression: ToArrayExpr): string {
    let arg = expression.args.this;
    if (!arg) return '';
    if (!arg.type) {
      arg = annotateTypes(arg, { dialect: this.dialect });
    }

    if (arg.isType(DataTypeExprKind.ARRAY)) {
      return this.sql(arg);
    }

    const condForNull = arg.is(null_());
    return this.sql(func('IF', condForNull, null_(), array(arg, { copy: false })));
  }

  tsOrDsToTimeSql (expression: TsOrDsToTimeExpr): string {
    const thisArg = expression.args.this;
    if (!thisArg) return '';
    const timeFormat = this.formatTime(expression);
    if (timeFormat) {
      return this.sql(
        cast(
          new StrToTimeExpr({
            this: thisArg,
            format: expression.args.format || timeFormat,
          }),
          DataTypeExprKind.TIME,
        ),
      );
    }
    if (expression.args.this instanceof TsOrDsToTimeExpr
      || thisArg.isType(DataTypeExprKind.TIME)) {
      return this.sql(thisArg);
    }
    return this.sql(cast(thisArg, DataTypeExprKind.TIME));
  }

  tsOrDsToTimestampSql (expression: TsOrDsToTimestampExpr): string {
    const thisArg = expression.args.this;
    if (!thisArg) return '';
    if (thisArg instanceof TsOrDsToTimestampExpr
      || thisArg.isType(DataTypeExprKind.TIMESTAMP)) {
      return this.sql(thisArg);
    }
    return this.sql(cast(thisArg, DataTypeExprKind.TIMESTAMP));
  }

  tsOrDsToDatetimeSql (expression: TsOrDsToDatetimeExpr): string {
    const thisArg = expression.args.this;
    if (!thisArg) return '';
    if (expression.args.this instanceof TsOrDsToDatetimeExpr
      || thisArg.isType(DataTypeExprKind.DATETIME)) {
      return this.sql(thisArg);
    }
    return this.sql(cast(thisArg, DataTypeExprKind.DATETIME));
  }

  tsOrDsToDateSql (expression: TsOrDsToDateExpr): string {
    const thisArg = expression.args.this;
    if (!thisArg) return '';
    const timeFormat = this.formatTime(expression);
    const safe = expression.args.safe;
    if (timeFormat && ![this.dialect._constructor.TIME_FORMAT, this.dialect._constructor.DATE_FORMAT].includes(timeFormat)) {
      return this.sql(
        cast(
          new StrToTimeExpr({
            this: thisArg,
            format: timeFormat,
            safe,
          }),
          DataTypeExprKind.DATE,
        ),
      );
    }
    if (expression.args.this instanceof TsOrDsToDateExpr
      || thisArg.isType(DataTypeExprKind.DATE)) {
      return this.sql(thisArg);
    }
    if (safe) {
      return this.sql(
        new TryCastExpr({
          this: thisArg,
          to: new DataTypeExpr({ this: DataTypeExprKind.DATE }),
        }),
      );
    }
    return this.sql(cast(thisArg, DataTypeExprKind.DATE));
  }

  unixDateSql (expression: UnixDateExpr): string {
    const startDate = cast(LiteralExpr.string('1970-01-01'), DataTypeExprKind.DATE);
    return this.func(
      'DATEDIFF',
      [
        expression.args.this,
        startDate,
        'day',
      ],
    );
  }

  lastDaySql (expression: LastDayExpr): string {
    if (this._constructor.LAST_DAY_SUPPORTS_DATE_PART) {
      return this.functionFallbackSql(expression);
    }
    const unit = expression.text('unit');
    const unitStr = unit ? this.sql(unit) : '';
    if (unitStr && unitStr !== 'MONTH') {
      this.unsupported('Date parts are not supported in LAST_DAY.');
    }
    return this.func('LAST_DAY', [expression.args.this]);
  }

  dateAddSql (expression: DateAddExpr): string {
    return this.func(
      'DATE_ADD',
      [
        expression.args.this,
        expression.args.expression,
        unitToStr(expression),
      ],
    );
  }

  arrayAnySql (expression: ArrayAnyExpr): string {
    if (this._constructor.CAN_IMPLEMENT_ARRAY_ANY) {
      const filtered = new ArrayFilterExpr({
        this: expression.args.this,
        expression: expression.args.expression,
      });
      const filteredNotEmpty = new ArraySizeExpr({ this: filtered }).neq(0);
      const originalIsEmpty = new ArraySizeExpr({ this: expression.args.this }).eq(0);
      return this.sql(paren(originalIsEmpty.or(filteredNotEmpty)));
    }

    // SQLGlot's executor supports ARRAY_ANY, so we don't wanna warn for the SQLGlot dialect
    if (!(this.dialect._constructor !== Dialect)) {
      this.unsupported('ARRAY_ANY is unsupported');
    }

    return this.functionFallbackSql(expression);
  }

  structSql (expression: StructExpr): string {
    expression.setArgKey(
      'expressions',
      (expression.args.expressions ?? []).map((e) => {
        if (e instanceof PropertyEqExpr) {
          const thisArg = e.args.this;
          const aliasName = (thisArg as Expression | undefined)?.isString ? e.name : isInstanceOf(thisArg, IdentifierExpr) ? thisArg : typeof thisArg === 'string' ? thisArg : undefined;
          const exprArg = e.args.expression;
          return alias(
            (exprArg instanceof Expression || typeof exprArg === 'string') ? exprArg : '',
            aliasName,
          );
        }
        return e;
      }),
    );

    return this.functionFallbackSql(expression);
  }

  partitionRangeSql (expression: PartitionRangeExpr): string {
    const low = this.sql(expression, 'this');
    const high = this.sql(expression, 'expression');
    return `${low} TO ${high}`;
  }

  truncateTableSql (expression: TruncateTableExpr): string {
    const target = expression.args.isDatabase ? 'DATABASE' : 'TABLE';
    const tables = ` ${this.expressions(expression)}`;
    const exists = expression.args.exists ? ' IF EXISTS' : '';
    let onCluster = this.sql(expression, 'cluster');
    onCluster = onCluster ? ` ${onCluster}` : '';
    let identity = this.sql(expression, 'identity');
    identity = identity ? ` ${identity} IDENTITY` : '';
    let option = this.sql(expression, 'option');
    option = option ? ` ${option}` : '';
    let partition = this.sql(expression, 'partition');
    partition = partition ? ` ${partition}` : '';
    return `TRUNCATE ${target}${exists}${tables}${onCluster}${identity}${option}${partition}`;
  }

  convertSql (expression: ConvertExpr): string {
    const to = expression.args.this;
    const value = expression.args.expression;
    const style = expression.args.style;
    const safe = expression.args.safe;
    const strict = expression.getArgKey('strict');

    if (!to || !value) {
      return '';
    }

    assertIsInstanceOf(to, DataTypeExpr);
    let finalTo: DataTypeExpr = to;

    // Retrieve length of datatype and override to default if not specified
    if (!seqGet(to.args.expressions ?? [], 0) && this._constructor.PARAMETERIZABLE_TEXT_TYPES.has(to.args.this as DataTypeExprKind)) {
      finalTo = DataTypeExpr.build(to.args.this, {
        expressions: [LiteralExpr.number(30)],
        nested: false,
      }) ?? finalTo;
    }

    let transformed: Expression | undefined = undefined;
    const castClass = strict ? CastExpr : TryCastExpr;

    // Check whether a conversion with format (T-SQL calls this 'style') is applicable
    if (style instanceof LiteralExpr && style.isInteger) {
      const styleValue = style.name;
      const convertedStyle = TSQL.INVERSE_FORMAT_MAPPING[styleValue];

      if (!convertedStyle) {
        this.unsupported(`Unsupported T-SQL 'style' value: ${styleValue}`);
      }

      const fmt = LiteralExpr.string(convertedStyle);

      if (finalTo.args.this === DataTypeExprKind.DATE) {
        transformed = new StrToDateExpr({
          this: value,
          format: fmt,
        });
      } else if (
        finalTo.args.this === DataTypeExprKind.DATETIME
        || finalTo.args.this === DataTypeExprKind.DATETIME2
      ) {
        transformed = new StrToTimeExpr({
          this: value,
          format: fmt,
        });
      } else if (this._constructor.PARAMETERIZABLE_TEXT_TYPES.has(finalTo.args.this as DataTypeExprKind)) {
        transformed = new castClass({
          this: new TimeToStrExpr({
            this: value,
            format: fmt,
          }),
          to: finalTo,
          safe: safe,
        });
      } else if (finalTo.args.this === DataTypeExprKind.TEXT) {
        transformed = new TimeToStrExpr({
          this: value,
          format: fmt,
        });
      }
    }

    if (!transformed) {
      transformed = new castClass({
        this: value,
        to: finalTo,
        safe: safe,
      });
    }

    return this.sql(transformed);
  }

  jsonPathKeySql (expression: JsonPathKeyExpr): string {
    const thisVal = expression.args.this;
    if (thisVal instanceof JsonPathWildcardExpr) {
      const part = this.jsonPathPart(thisVal);
      return part ? `.${part}` : '';
    }
    if (typeof thisVal === 'string' && this._constructor.SAFE_JSON_PATH_KEY_RE.test(thisVal)) {
      return `.${thisVal}`;
    }
    const part = this.jsonPathPart(typeof thisVal === 'string' ? thisVal : (thisVal instanceof Expression ? thisVal.name ?? '' : ''));
    return (this.quoteJsonPathKeyUsingBrackets && this._constructor.JSON_PATH_BRACKETED_KEY_SUPPORTED)
      ? `[${part}]`
      : `.${part}`;
  }

  jsonPathSubscriptSql (expression: JsonPathSubscriptExpr): string {
    const thisVal = expression.args.this;
    const part = this.jsonPathPart(typeof thisVal === 'number' ? String(thisVal) : this.sql(thisVal as Expression));
    return part ? `[${part}]` : '';
  }

  simplifyUnlessLiteral<E extends Expression> (expression: E): E {
    if (!(expression instanceof LiteralExpr)) {
      expression = simplify(expression, { dialect: this.dialect });
    }

    return expression;
  }

  embedIgnoreNulls (expression: IgnoreNullsExpr | RespectNullsExpr, text: string): string {
    const thisExpr = expression.args.this;

    // Check if the current expression is in the unsupported list for the dialect
    if (this._constructor.RESPECT_IGNORE_NULLS_UNSUPPORTED_EXPRESSIONS.some((cls) => thisExpr instanceof cls)) {
      this.unsupported(
        `RESPECT/IGNORE NULLS is not supported for ${thisExpr?._constructor.key} in ${this.dialect.constructor.name}`,
      );
      return this.sql(thisExpr);
    }

    if (this._constructor.IGNORE_NULLS_IN_FUNC && !expression.meta?.inline) {
      // Sort modifiers: HavingMax -> Order -> Limit
      const mods = [
        ...expression.findAll<HavingMaxExpr | OrderExpr | LimitExpr>([
          HavingMaxExpr,
          OrderExpr,
          LimitExpr,
        ]),
      ].sort((a, b) => {
        const getPriority = (x: Expression) => {
          if (x instanceof HavingMaxExpr) return 0;
          if (x instanceof OrderExpr) return 1;
          return 2;
        };
        return getPriority(a) - getPriority(b);
      });

      if (0 < mods.length) {
        const mod = mods[0];
        const newThis = new expression._constructor({ this: mod.args.this?.copy() });

        newThis.meta = {
          ...newThis.meta,
          inline: true,
        };
        mod.args.this?.replace(newThis);

        return this.sql(expression.args.this);
      }

      const aggFunc = expression.find(AggFuncExpr);
      if (aggFunc) {
        // Inject the modifier text inside the function parentheses
        const aggFuncSql = this.sql(aggFunc, undefined, { comment: false }).slice(0, -1) + ` ${text})`;
        return this.maybeComment(aggFuncSql, undefined, aggFunc.comments);
      }
    }

    return `${this.sql(expression, 'this')} ${text}`;
  }

  replaceLineBreaks (string: string): string {
    if (this.pretty) {
      return string.replace(/\n/g, this._constructor.SENTINEL_LINE_BREAK);
    }
    return string;
  }

  copyParameterSql (expression: CopyParameterExpr): string {
    const option = this.sql(expression, 'this');

    if (expression.args.expressions && 0 < expression.args.expressions.length) {
      const upper = option.toUpperCase();

      const sep = upper === 'FILE_FORMAT' ? ' ' : ', ';

      const op = (upper === 'COPY_OPTIONS' || upper === 'FORMAT_OPTIONS') ? ' ' : ' = ';

      const values = this.expressions(expression, {
        flat: true,
        sep,
      });
      return `${option}${op}${values}`;
    }

    const value = this.sql(expression, 'expression');

    if (!value) return option;

    const op = this._constructor.COPY_PARAMS_EQ_REQUIRED ? ' = ' : ' ';

    return `${option}${op}${value}`;
  }

  credentialsSql (expression: CredentialsExpr): string {
    const credExpr = expression.args.credentials;
    let credentials = '';
    if (credExpr instanceof LiteralExpr) {
      const credStr = this.sql(expression, 'credentials');
      credentials = credStr ? `CREDENTIALS ${credStr}` : '';
    } else {
      const credStr = this.expressions(expression, {
        key: 'credentials',
        flat: true,
        sep: ' ',
      });
      credentials = credExpr !== undefined && credExpr !== null ? `CREDENTIALS = (${credStr})` : '';
    }

    const storage = this.sql(expression, 'storage');
    const storageStr = storage ? `STORAGE_INTEGRATION = ${storage}` : '';

    const encryption = this.expressions(expression, {
      key: 'encryption',
      flat: true,
      sep: ' ',
    });
    const encryptionStr = encryption ? ` ENCRYPTION = (${encryption})` : '';

    const iamRole = this.sql(expression, 'iamRole');
    const iamRoleStr = iamRole ? `IAM_ROLE ${iamRole}` : '';

    const region = this.sql(expression, 'region');
    const regionStr = region ? ` REGION ${region}` : '';

    return `${credentials}${storageStr}${encryptionStr}${iamRoleStr}${regionStr}`;
  }

  copySql (expression: CopyExpr): string {
    const dialect = this._constructor;
    let thisStr = this.sql(expression, 'this');
    thisStr = dialect.COPY_HAS_INTO_KEYWORD ? ` INTO ${thisStr}` : ` ${thisStr}`;

    let credentials = this.sql(expression, 'credentials');
    credentials = credentials ? this.seg(credentials) : '';
    const files = this.expressions(expression, {
      key: 'files',
      flat: true,
    });
    const kind = (files && expression.args.kind) ? this.seg('FROM') : (files ? this.seg('TO') : '');

    const copyParamsAreCsv = this.dialect._constructor.COPY_PARAMS_ARE_CSV || false;
    const sep = copyParamsAreCsv ? ', ' : ' ';
    let params = this.expressions(
      expression,
      {
        key: 'params',
        sep,
        newLine: true,
        skipLast: true,
        skipFirst: true,
        indent: dialect.COPY_PARAMS_ARE_WRAPPED,
      },
    );

    if (params) {
      if (dialect.COPY_PARAMS_ARE_WRAPPED) {
        params = ` WITH (${params})`;
      } else if (!this.pretty && (files || credentials)) {
        params = ` ${params}`;
      }
    }

    return `COPY${thisStr}${kind} ${files}${credentials}${params}`;
  }

  semicolonSql (_expression: SemicolonExpr): string {
    return '';
  }

  dataDeletionPropertySql (expression: DataDeletionPropertyExpr): string {
    const onStr = expression.args.on ? 'ON' : 'OFF';
    const filterCol = this.sql(expression, 'filterColumn');
    const filterColStr = filterCol ? `FILTER_COLUMN=${filterCol}` : undefined;
    const retentionPeriod = this.sql(expression, 'retentionPeriod');
    const retentionPeriodStr = retentionPeriod ? `RETENTION_PERIOD=${retentionPeriod}` : undefined;
    if (filterColStr || retentionPeriodStr) {
      return `DATA_DELETION=${this.func('ON', [filterColStr, retentionPeriodStr])}`;
    }
    return `DATA_DELETION=${onStr}`;
  }

  gapFillSql (expression: GapFillExpr): string {
    let thisSql = this.sql(expression, 'this');
    thisSql = `TABLE ${thisSql}`;

    const otherArgs = Object.entries(expression.args)
      .filter(([k]) => k !== 'this')
      .map(([_, v]) => v as string | Expression);

    return this.func('GAP_FILL', [thisSql, ...otherArgs]);
  }

  scopeResolution (rhs: string, scopeName: string): string {
    return this.func('SCOPE_RESOLUTION', [scopeName || undefined, rhs]);
  }

  scopeResolutionSql (expression: ScopeResolutionExpr): string {
    const thisStr = this.sql(expression, 'this');
    const exprNode = expression.args.expression;
    let expr: string;
    if (exprNode instanceof FuncExpr) {
      const exprThis = this.sql(exprNode, 'this');
      const exprArgs = this.formatArgs(exprNode.args.expressions || []);
      expr = `${exprThis}(${exprArgs})`;
    } else {
      expr = this.sql(expression, 'expression');
    }
    return this.scopeResolution(expr, thisStr);
  }

  parseJsonSql (expression: ParseJsonExpr): string {
    const parseName = this._constructor.PARSE_JSON_NAME;
    if (parseName === undefined || parseName === null) {
      return this.sql(expression.args.this);
    }
    return this.func(
      parseName,
      [expression.args.this, expression.args.expression],
    );
  }

  randSql (expression: RandExpr): string {
    const lower = this.sql(expression, 'lower');
    const upper = this.sql(expression, 'upper');
    if (lower && upper) {
      return `(${upper} - ${lower}) * ${this.func('RAND', [expression.args.this])} + ${lower}`;
    }
    return this.func('RAND', [expression.args.this]);
  }

  changesSql (expression: ChangesExpr): string {
    const information = this.sql(expression, 'information');
    const informationStr = `INFORMATION => ${information}`;
    const atBefore = this.sql(expression, 'atBefore');
    const atBeforeStr = atBefore ? `${this.seg('')}${atBefore}` : '';
    const end = this.sql(expression, 'end');
    const endStr = end ? `${this.seg('')}${end}` : '';
    return `CHANGES (${informationStr})${atBeforeStr}${endStr}`;
  }

  padSql (expression: PadExpr): string {
    const prefix = expression.args.isLeft ? 'L' : 'R';

    let fillPattern = this.sql(expression, 'fillPattern');
    if (!fillPattern && this._constructor.PAD_FILL_PATTERN_IS_REQUIRED) {
      fillPattern = '\' \'';
    }

    return this.func(`${prefix}PAD`, [
      expression.args.this,
      expression.args.expression,
      fillPattern,
    ]);
  }

  summarizeSql (expression: SummarizeExpr): string {
    const table = expression.args.table ? ' TABLE' : '';
    return `SUMMARIZE${table} ${this.sql(expression.args.this as Expression)}`;
  }

  explodingGenerateSeriesSql (expression: ExplodingGenerateSeriesExpr): string {
    const generateSeries = new GenerateSeriesExpr({ ...expression.args });

    let parent = expression.parent;
    if (parent instanceof AliasExpr || parent instanceof TableAliasExpr) {
      parent = parent.parent;
    }

    if (this._constructor.SUPPORTS_EXPLODING_PROJECTIONS && !(parent instanceof TableExpr) && !(parent instanceof UnnestExpr)) {
      return this.sql(new UnnestExpr({ expressions: [generateSeries] }));
    }

    if (parent instanceof SelectExpr) {
      this.unsupported('GenerateSeries projection unnesting is not supported.');
    }

    return this.sql(generateSeries);
  }

  convertTimezoneSql (expression: ConvertTimezoneExpr): string {
    if (this._constructor.SUPPORTS_CONVERT_TIMEZONE) {
      return this.functionFallbackSql(expression);
    }

    const sourceTz = expression.args.sourceTz;
    const targetTz = expression.args.targetTz;
    let timestamp = expression.args.timestamp;

    if (sourceTz && timestamp) {
      timestamp = new AtTimeZoneExpr({
        this: cast(timestamp, DataTypeExprKind.TIMESTAMPNTZ),
        zone: sourceTz,
      });
    }

    const expr = new AtTimeZoneExpr({
      this: timestamp,
      zone: targetTz,
    });

    return this.sql(expr);
  }

  jsonSql (expression: JsonExpr): string {
    const thisStr = this.sql(expression, 'this');
    const thisResult = thisStr ? ` ${thisStr}` : '';

    const _with = expression.args.with;

    let withSql = '';
    if (_with === undefined || _with === null) {
      withSql = '';
    } else if (!_with) {
      withSql = ' WITHOUT';
    } else {
      withSql = ' WITH';
    }

    const uniqueSql = expression.args.unique ? ' UNIQUE KEYS' : '';

    return `JSON${thisResult}${withSql}${uniqueSql}`;
  }

  jsonValueSql (expression: JsonValueExpr): string {
    const path = this.sql(expression, 'path');
    let returning = this.sql(expression, 'returning');
    returning = returning ? ` RETURNING ${returning}` : '';

    let onCondition = this.sql(expression, 'onCondition');
    onCondition = onCondition ? ` ${onCondition}` : '';

    return this.func('JSON_VALUE', [expression.args.this, `${path}${returning}${onCondition}`]);
  }

  conditionalInsertSql (expression: ConditionalInsertExpr): string {
    const else_ = expression.args.else ? 'ELSE ' : '';
    const condition = this.sql(expression, 'expression');
    const conditionStr = condition ? `WHEN ${condition} THEN ` : else_;
    const insert = this.sql(expression, 'this').substring('INSERT'.length)
      .trim();
    return `${conditionStr}${insert}`;
  }

  multitableInsertsSql (expression: MultitableInsertsExpr): string {
    const kind = this.sql(expression, 'kind');
    const expressions = this.seg(this.expressions(expression, { sep: ' ' }));
    const res = `INSERT ${kind}${expressions}${this.seg(this.sql(expression, 'source'))}`;
    return res;
  }

  onConditionSql (expression: OnConditionExpr): string {
    const empty = expression.args.empty;
    let emptyStr = '';
    if (empty !== undefined && empty !== null) {
      if (typeof empty === 'object' && 'key' in empty) {
        emptyStr = `DEFAULT ${this.sql(empty)} ON EMPTY`;
      } else {
        emptyStr = this.sql(expression, 'empty');
      }
    }

    const error = expression.args.error;
    let errorStr = '';
    if (error !== undefined && error !== null) {
      if (typeof error === 'object' && 'key' in error) {
        errorStr = `DEFAULT ${this.sql(error)} ON ERROR`;
      } else {
        errorStr = this.sql(expression, 'error');
      }
    }

    if (errorStr && emptyStr) {
      if (this._constructor.ON_CONDITION_EMPTY_BEFORE_ERROR) {
        errorStr = `${emptyStr} ${errorStr}`;
      } else {
        errorStr = `${errorStr} ${emptyStr}`;
      }
      emptyStr = '';
    }

    const nullStr = this.sql(expression, 'null');

    return `${emptyStr}${errorStr}${nullStr}`;
  }

  jsonExtractQuoteSql (expression: JsonExtractQuoteExpr): string {
    const scalar = expression.args.scalar ? ' ON SCALAR STRING' : '';
    return `${this.sql(expression, 'option')} QUOTES${scalar}`;
  }

  jsonExistsSql (expression: JsonExistsExpr): string {
    const thisStr = this.sql(expression, 'this');
    let path = this.sql(expression, 'path');

    const passing = this.expressions(expression, { key: 'passing' });
    const passingStr = passing ? ` PASSING ${passing}` : '';

    let onCondition = this.sql(expression, 'onCondition');
    onCondition = onCondition ? ` ${onCondition}` : '';

    path = `${path}${passingStr}${onCondition}`;

    return this.func('JSON_EXISTS', [thisStr, path]);
  }

  addArrayAggNullFilter (
    arrayAggSql: string,
    arrayAggExpr: ArrayAggExpr,
    columnExpr: Expression,
  ): string {
    // Add a NULL FILTER on the column to mimic the results going from a dialect that excludes nulls
    // on ARRAY_AGG (e.g Spark) to one that doesn't (e.g. DuckDB)
    if (
      !(this.dialect._constructor.ARRAY_AGG_INCLUDES_NULLS && arrayAggExpr.args.nullsExcluded)
    ) {
      return arrayAggSql;
    }

    const parent = arrayAggExpr.parent;
    if (parent instanceof FilterExpr) {
      const parentCond = parent.args.this;
      if (!parentCond) return arrayAggSql;
      parentCond.replace(
        parentCond.and(columnExpr.is(null_()).not()),
      );
    } else if (columnExpr.find(ColumnExpr)) {
      // Do not add the filter if the input is not a column (e.g. literal, struct etc)
      // DISTINCT is already present in the agg function, do not propagate it to FILTER as well
      const thisSql = columnExpr instanceof DistinctExpr
        ? this.expressions(columnExpr)
        : this.sql(columnExpr);

      arrayAggSql = `${arrayAggSql} FILTER(WHERE ${thisSql} IS NOT NULL)`;
    }

    return arrayAggSql;
  }

  arrayAggSql (expression: ArrayAggExpr): string {
    const arrayAgg = this.functionFallbackSql(expression);
    const arrayAggThis = expression.args.this instanceof Expression ? expression.args.this : undefined;
    if (!arrayAggThis) return arrayAgg;
    return this.addArrayAggNullFilter(arrayAgg, expression, arrayAggThis);
  }

  sliceSql (expression: SliceExpr): string {
    const step = this.sql(expression, 'step');
    const end = this.sql(typeof expression.args.expression === 'number' ? expression.args.expression.toString() : expression.args.expression);
    const begin = this.sql(expression.args.this);

    const sql = step ? `${end}:${step}` : end;
    return sql ? `${begin}:${sql}` : `${begin}:`;
  }

  applySql (expression: ApplyExpr): string {
    const thisStr = this.sql(expression, 'this');
    const expr = this.sql(expression, 'expression');

    return `${thisStr} APPLY(${expr})`;
  }

  grantOrRevokeSql (
    expression: GrantExpr | RevokeExpr,
    options: {
      keyword?: string;
      preposition?: string;
      grantOptionPrefix?: string;
      grantOptionSuffix?: string;
    } = {},
  ): string {
    const {
      keyword,
      preposition,
      grantOptionPrefix = '',
      grantOptionSuffix = '',
    } = options;

    const privilegesSql = this.expressions(expression, {
      key: 'privileges',
      flat: true,
    });

    const kind = this.sql(expression, 'kind');
    const kindStr = kind ? ` ${kind}` : '';

    let securable = this.sql(expression, 'securable');
    securable = securable ? ` ${securable}` : '';

    const principals = this.expressions(expression, {
      key: 'principals',
      flat: true,
    });

    let grantOptionPrefixStr = grantOptionPrefix;
    let grantOptionSuffixStr = grantOptionSuffix;
    if (!expression.args.grantOption) {
      grantOptionPrefixStr = grantOptionSuffixStr = '';
    }

    const cascade = this.sql(expression, 'cascade');
    const cascadeStr = cascade ? ` ${cascade}` : '';

    return `${keyword} ${grantOptionPrefixStr}${privilegesSql} ON${kindStr}${securable} ${preposition} ${principals}${grantOptionSuffixStr}${cascadeStr}`;
  }

  grantSql (expression: GrantExpr): string {
    return this.grantOrRevokeSql(
      expression,
      {
        keyword: 'GRANT',
        preposition: 'TO',
        grantOptionSuffix: ' WITH GRANT OPTION',
      },
    );
  }

  revokeSql (expression: RevokeExpr): string {
    return this.grantOrRevokeSql(
      expression,
      {
        keyword: 'REVOKE',
        preposition: 'FROM',
        grantOptionPrefix: 'GRANT OPTION FOR ',
      },
    );
  }

  grantPrivilegeSql (expression: GrantPrivilegeExpr): string {
    const thisStr = this.sql(expression, 'this');
    const columns = this.expressions(expression, { flat: true });
    const columnsStr = columns ? `(${columns})` : '';

    return `${thisStr}${columnsStr}`;
  }

  grantPrincipalSql (expression: GrantPrincipalExpr): string {
    const thisStr = this.sql(expression, 'this');

    const kind = this.sql(expression, 'kind');
    const kindStr = kind ? `${kind} ` : '';

    return `${kindStr}${thisStr}`;
  }

  columnsSql (expression: ColumnsExpr): string {
    const func = this.functionFallbackSql(expression);
    const unpack = expression.args.unpack;
    return unpack ? `*${func}` : func;
  }

  overlaySql (expression: OverlayExpr): string {
    const thisStr = this.sql(expression, 'this');
    const expr = this.sql(expression, 'expression');
    const fromSql = this.sql(expression, 'from');
    const forSql = this.sql(expression, 'for');
    const forStr = forSql ? ` FOR ${forSql}` : '';
    return `OVERLAY(${thisStr} PLACING ${expr} FROM ${fromSql}${forStr})`;
  }

  toDoubleSql (expression: ToDoubleExpr): string {
    unsupportedArgs.call(this, expression, 'format');
    const castCls = expression.args.safe ? TryCastExpr : CastExpr;
    return this.sql(new castCls({
      this: expression.args.this,
      to: new DataTypeExpr({ this: DataTypeExprKind.DOUBLE }),
    }));
  }

  stringSql (expression: StringExpr): string {
    let thisExpr: Expression = LiteralExpr.string(expression.args.this);
    const zone = expression.args.zone;

    if (zone) {
      // This is a BigQuery specific argument for STRING(<timestamp_expr>, <time_zone>)
      // BigQuery stores timestamps internally as UTC, so ConvertTimezone is used with UTC
      // set for source_tz to transpile the time conversion before the STRING cast
      thisExpr = new ConvertTimezoneExpr({
        sourceTz: new LiteralExpr({
          this: 'UTC',
          isString: true,
        }),
        targetTz: zone,
        timestamp: thisExpr,
      });
    }

    return this.sql(cast(thisExpr, 'VARCHAR', { dialect: this.dialect }));
  }

  medianSql (expression: MedianExpr): string {
    if (!this._constructor.SUPPORTS_MEDIAN) {
      return this.sql(
        new PercentileContExpr({
          this: expression.args.this as Expression,
          expression: LiteralExpr.number(0.5),
        }),
      );
    }
    return this.functionFallbackSql(expression);
  }

  overflowTruncateBehaviorSql (expression: OverflowTruncateBehaviorExpr): string {
    const filler = this.sql(expression, 'this');
    const fillerStr = filler ? ` ${filler}` : '';
    const withCount = expression.args.withCount ? 'WITH COUNT' : 'WITHOUT COUNT';
    return `TRUNCATE${fillerStr} ${withCount}`;
  }

  unixSecondsSql (expression: UnixSecondsExpr): string {
    if (this._constructor.SUPPORTS_UNIX_SECONDS) {
      return this.functionFallbackSql(expression);
    }
    const startTs = cast(LiteralExpr.string('1970-01-01 00:00:00+00'), DataTypeExprKind.TIMESTAMPTZ);
    return this.sql(
      new TimestampDiffExpr({
        this: expression.args.this,
        expression: startTs,
        unit: new VarExpr({ this: 'SECONDS' }),
      }),
    );
  }

  arraySizeSql (expression: ArraySizeExpr): string {
    let dim = expression.args.expression;

    // For dialects that don't support the dimension arg, we can safely transpile its default value (1st dimension)
    if (dim && this._constructor.ARRAY_SIZE_DIM_REQUIRED === undefined) {
      if (!(dim.isInteger && dim.name === '1')) {
        this.unsupported('Cannot transpile dimension argument for ARRAY_LENGTH');
      }
      dim = undefined;
    }

    // If dimension is required but not specified, default initialize it
    if (this._constructor.ARRAY_SIZE_DIM_REQUIRED && !dim) {
      dim = new LiteralExpr({
        this: '1',
        isString: false,
      });
    }

    return this.func(this._constructor.ARRAY_SIZE_NAME, [expression.args.this, dim]);
  }

  attachSql (expression: AttachExpr): string {
    const thisStr = this.sql(expression, 'this');
    const existsSql = expression.args.exists ? ' IF NOT EXISTS' : '';
    let expressions = this.expressions(expression);
    expressions = expressions ? ` (${expressions})` : '';
    return `ATTACH${existsSql} ${thisStr}${expressions}`;
  }

  detachSql (expression: DetachExpr): string {
    const thisStr = this.sql(expression, 'this');
    const existsSql = expression.args.exists ? ' DATABASE IF EXISTS' : '';
    return `DETACH${existsSql} ${thisStr}`;
  }

  attachOptionSql (expression: AttachOptionExpr): string {
    const thisStr = this.sql(expression, 'this');
    let value = this.sql(expression, 'expression');
    value = value ? ` ${value}` : '';
    return `${thisStr}${value}`;
  }

  watermarkColumnConstraintSql (expression: WatermarkColumnConstraintExpr): string {
    return `WATERMARK FOR ${this.sql(expression, 'this')} AS ${this.sql(expression, 'expression')}`;
  }

  encodePropertySql (expression: EncodePropertyExpr): string {
    const encode = expression.args.key ? 'KEY ENCODE' : 'ENCODE';
    let encodeSql = `${encode} ${this.sql(expression, 'this')}`;

    const properties = expression.args.properties;
    if (properties) {
      assertIsInstanceOf(properties, PropertiesExpr);
      encodeSql = `${encodeSql} ${this.properties(properties)}`;
    }

    return encodeSql;
  }

  includePropertySql (expression: IncludePropertyExpr): string {
    const thisStr = this.sql(expression, 'this');
    let include = `INCLUDE ${thisStr}`;

    const columnDef = this.sql(expression, 'columnDef');
    if (columnDef) {
      include = `${include} ${columnDef}`;
    }

    const alias = this.sql(expression, 'alias');
    if (alias) {
      include = `${include} AS ${alias}`;
    }

    return include;
  }

  xmlElementSql (expression: XmlElementExpr): string {
    const prefix = expression.args.evalname ? 'EVALNAME' : 'NAME';
    const name = `${prefix} ${this.sql(expression, 'this')}`;
    return this.func('XmlELEMENT', [name, ...(expression.args.expressions || [])]);
  }

  xmlKeyValueOptionSql (expression: XmlKeyValueOptionExpr): string {
    const thisStr = this.sql(expression, 'this');
    const expr = this.sql(expression, 'expression');
    const exprStr = expr ? `(${expr})` : '';
    return `${thisStr}${exprStr}`;
  }

  partitionByRangePropertySql (expression: PartitionByRangePropertyExpr): string {
    const partitions = this.expressions(expression, { key: 'partitionExpressions' });
    const create = this.expressions(expression, { key: 'createExpressions' });
    return `PARTITION BY RANGE ${this.wrap(partitions)} ${this.wrap(create)}`;
  }

  partitionByRangePropertyDynamicSql (expression: PartitionByRangePropertyDynamicExpr): string {
    const start = this.sql(expression, 'start');
    const end = this.sql(expression, 'end');

    const every = expression.args.every;
    if (every instanceof IntervalExpr && every.args.this?.isString) {
      every.args.this.replace(LiteralExpr.number(every.name));
    }

    const everySql = this.sql(every as Expression);
    return `START(${start}) END(${end}) EVERY(${everySql})`;
  }

  unpivotColumnsSql (expression: UnpivotColumnsExpr): string {
    const name = this.sql(expression, 'this');
    const values = this.expressions(expression, { flat: true });

    return `NAME ${name} VALUE ${values}`;
  }

  analyzeSampleSql (expression: AnalyzeSampleExpr): string {
    const kind = this.sql(expression, 'kind');
    const sample = this.sql(expression, 'sample');
    return `SAMPLE ${sample} ${kind}`;
  }

  analyzeStatisticsSql (expression: AnalyzeStatisticsExpr): string {
    const kind = this.sql(expression, 'kind');
    let option = this.sql(expression, 'option');
    option = option ? ` ${option}` : '';
    let thisStr = this.sql(expression, 'this');
    thisStr = thisStr ? ` ${thisStr}` : '';
    let columns = this.expressions(expression);
    columns = columns ? ` ${columns}` : '';
    return `${kind}${option} STATISTICS${thisStr}${columns}`;
  }

  analyzeHistogramSql (expression: AnalyzeHistogramExpr): string {
    const thisStr = this.sql(expression, 'this');
    const columns = this.expressions(expression);
    let innerExpression = this.sql(expression, 'expression');
    innerExpression = innerExpression ? ` ${innerExpression}` : '';
    let updateOptions = this.sql(expression, 'updateOptions');
    updateOptions = updateOptions ? ` ${updateOptions} UPDATE` : '';
    return `${thisStr} HISTOGRAM ON ${columns}${innerExpression}${updateOptions}`;
  }

  analyzeDeleteSql (expression: AnalyzeDeleteExpr): string {
    let kind = this.sql(expression, 'kind');
    kind = kind ? ` ${kind}` : '';
    return `DELETE${kind} STATISTICS`;
  }

  analyzeListChainedRowsSql (expression: AnalyzeListChainedRowsExpr): string {
    const innerExpression = this.sql(expression, 'expression');
    return `LIST CHAINED ROWS${innerExpression}`;
  }

  analyzeValidateSql (expression: AnalyzeValidateExpr): string {
    const kind = this.sql(expression, 'kind');
    let thisStr = this.sql(expression, 'this');
    thisStr = thisStr ? ` ${thisStr}` : '';
    const innerExpression = this.sql(expression, 'expression');
    return `VALIDATE ${kind}${thisStr}${innerExpression}`;
  }

  analyzeSql (expression: AnalyzeExpr): string {
    let options = this.expressions(expression, {
      key: 'options',
      sep: ' ',
    });
    options = options ? ` ${options}` : '';
    let kind = this.sql(expression, 'kind');
    kind = kind ? ` ${kind}` : '';
    let thisStr = this.sql(expression, 'this');
    thisStr = thisStr ? ` ${thisStr}` : '';
    let mode = this.sql(expression, 'mode');
    mode = mode ? ` ${mode}` : '';
    let properties = this.sql(expression, 'properties');
    properties = properties ? ` ${properties}` : '';
    let partition = this.sql(expression, 'partition');
    partition = partition ? ` ${partition}` : '';
    let innerExpression = this.sql(expression, 'expression');
    innerExpression = innerExpression ? ` ${innerExpression}` : '';
    return `ANALYZE${options}${kind}${thisStr}${mode}${properties}${partition}${innerExpression}`;
  }

  xmlTableSql (expression: XmlTableExpr): string {
    const thisStr = this.sql(expression, 'this');
    const namespaces = this.expressions(expression, { key: 'namespaces' });
    const namespacesStr = namespaces ? `XmlNAMESPACES(${namespaces}), ` : '';
    const passing = this.expressions(expression, { key: 'passing' });
    const passingStr = passing ? `${this.sep()}PASSING${this.seg(passing)}` : '';
    const columns = this.expressions(expression, { key: 'columns' });
    const columnsStr = columns ? `${this.sep()}COLUMNS${this.seg(columns)}` : '';
    const byRef = expression.args.byRef ? `${this.sep()}RETURNING SEQUENCE BY REF` : '';
    return `XmlTABLE(${this.sep('')}${this.indent(namespacesStr + thisStr + passingStr + byRef + columnsStr)}${this.seg(')', '')})`;
  }

  xmlNamespaceSql (expression: XmlNamespaceExpr): string {
    const thisStr = this.sql(expression, 'this');
    return expression.args.this instanceof AliasExpr ? thisStr : `DEFAULT ${thisStr}`;
  }

  exportSql (expression: ExportExpr): string {
    const thisStr = this.sql(expression, 'this');
    const connection = this.sql(expression, 'connection');
    const connectionStr = connection ? `WITH CONNECTION ${connection} ` : '';
    const options = this.sql(expression, 'options');
    return `EXPORT DATA ${connectionStr}${options} AS ${thisStr}`;
  }

  declareSql (expression: DeclareExpr): string {
    return `DECLARE ${this.expressions(expression, { flat: true })}`;
  }

  declareItemSql (expression: DeclareItemExpr): string {
    const variable = this.sql(expression, 'this');
    const defaultVal = this.sql(expression, 'default');
    const defaultStr = defaultVal ? ` = ${defaultVal}` : '';

    let kind = this.sql(expression, 'kind');
    if (expression.args.kind instanceof SchemaExpr) {
      kind = `TABLE ${kind}`;
    }

    return `${variable} AS ${kind}${defaultStr}`;
  }

  recursiveWithSearchSql (expression: RecursiveWithSearchExpr): string {
    const kind = this.sql(expression, 'kind');
    const thisStr = this.sql(expression, 'this');
    const set = this.sql(expression, 'expression');
    const using = this.sql(expression, 'using');
    const usingStr = using ? ` USING ${using}` : '';

    const kindSql = kind === 'CYCLE' ? kind : `SEARCH ${kind} FIRST BY`;

    return `${kindSql} ${thisStr} SET ${set}${usingStr}`;
  }

  parameterizedAggSql (expression: ParameterizedAggExpr): string {
    const params = this.expressions(expression, {
      key: 'params',
      flat: true,
    });
    const name = expression.name || '';
    return this.func(name, expression.args.expressions || []) + `(${params})`;
  }

  anonymousAggFuncSql (expression: AnonymousAggFuncExpr): string {
    const name = expression.name || '';
    return this.func(name, expression.args.expressions || []);
  }

  combinedAggFuncSql (expression: CombinedAggFuncExpr): string {
    return this.anonymousAggFuncSql(expression);
  }

  combinedParameterizedAggSql (expression: CombinedParameterizedAggExpr): string {
    return this.parameterizedAggSql(expression);
  }

  showSql (_expression: ShowExpr): string {
    this.unsupported('Unsupported SHOW statement');
    return '';
  }

  installSql (_expression: InstallExpr): string {
    this.unsupported('Unsupported INSTALL statement');
    return '';
  }

  getPutSql (expression: PutExpr | GetExpr): string {
    const props = expression.args.properties;
    if (props) {
      assertIsInstanceOf(props, PropertiesExpr);
    }
    const propsSql = props
      ? this.properties(props, {
        prefix: ' ',
        sep: ' ',
        wrapped: false,
      })
      : '';
    const thisStr = this.sql(expression, 'this');
    const target = this.sql(expression, 'target');

    if (expression instanceof PutExpr) {
      return `PUT ${thisStr} ${target}${propsSql}`;
    } else {
      return `GET ${target} ${thisStr}${propsSql}`;
    }
  }

  translateCharactersSql (expression: TranslateCharactersExpr): string {
    const thisStr = this.sql(expression, 'this');
    const expr = this.sql(expression, 'expression');
    const withError = expression.args.withError ? ' WITH ERROR' : '';
    return `TRANSLATE(${thisStr} USING ${expr}${withError})`;
  }

  decodeCaseSql (expression: DecodeCaseExpr): string {
    if (this._constructor.SUPPORTS_DECODE_CASE) {
      return this.func('DECODE', expression.args.expressions ?? []);
    }

    const [baseExpression, ...restExpressions] = expression.args.expressions ?? [];

    const ifs: IfExpr[] = [];
    for (let i = 0; i < restExpressions.length - 1; i += 2) {
      const search = restExpressions[i];
      const result = restExpressions[i + 1];

      if (search instanceof LiteralExpr) {
        ifs.push(new IfExpr({
          this: baseExpression.eq(search),
          true: result,
        }));
      } else if (search instanceof NullExpr) {
        ifs.push(new IfExpr({
          this: baseExpression.is(null_()),
          true: result,
        }));
      } else {
        let finalSearch = search;
        if (finalSearch instanceof BinaryExpr) {
          finalSearch = paren(finalSearch);
        }

        const cond = or(
          [baseExpression.eq(finalSearch), and([baseExpression.is(null_()), finalSearch.is(null_())], { copy: false })],
          { copy: false },
        );
        ifs.push(new IfExpr({
          this: cond,
          true: result,
        }));
      }
    }

    const defaultExpr = restExpressions.length % 2 === 1 ? restExpressions[restExpressions.length - 1] : undefined;
    const caseExpr = new CaseExpr({
      ifs,
      default: defaultExpr,
    });

    return this.sql(caseExpr);
  }

  semanticViewSql (expression: SemanticViewExpr): string {
    const thisSql = this.sql(expression, 'this');
    const thisSeg = this.seg(thisSql, '');

    const dimensionsExpr = this.expressions(expression, {
      key: 'dimensions',
      dynamic: true,
      skipFirst: true,
      skipLast: true,
    });
    const dimensions = dimensionsExpr ? this.seg(`DIMENSIONS ${dimensionsExpr}`) : '';

    const metricsExpr = this.expressions(expression, {
      key: 'metrics',
      dynamic: true,
      skipFirst: true,
      skipLast: true,
    });
    const metrics = metricsExpr ? this.seg(`METRICS ${metricsExpr}`) : '';

    const factsExpr = this.expressions(expression, {
      key: 'facts',
      dynamic: true,
      skipFirst: true,
      skipLast: true,
    });
    const facts = factsExpr ? this.seg(`FACTS ${factsExpr}`) : '';

    const whereExpr = this.sql(expression, 'where');
    const where = whereExpr ? this.seg(`WHERE ${whereExpr}`) : '';

    const body = this.indent(thisSeg + metrics + dimensions + facts + where, {
      skipFirst: true,
    });

    return `SEMANTIC_VIEW(${body}${this.seg(')', '')}`;
  }

  getExtractSql (expression: GetExtractExpr): string {
    let thisExpr = expression.args.this;
    const expr = expression.args.expression;

    if (!thisExpr) return '';
    if (!thisExpr.type || !expression.type) {
      thisExpr = annotateTypes(thisExpr, { dialect: this.dialect });
    }

    if (thisExpr.isType([DataTypeExprKind.ARRAY, DataTypeExprKind.MAP])) {
      return this.sql(new BracketExpr({
        this: thisExpr,
        expressions: expr ? [expr] : [],
      }));
    }

    return this.sql(
      new JsonExtractExpr({
        this: thisExpr,
        expression: this.dialect.toJsonPath(expr),
      }),
    );
  }

  dateFromUnixDateSql (expression: DateFromUnixDateExpr): string {
    return this.sql(
      new DateAddExpr({
        this: cast(LiteralExpr.string('1970-01-01'), DataTypeExprKind.DATE),
        expression: expression.args.this as Expression,
        unit: var_('DAY'),
      }),
    );
  }

  spaceSql (expression: SpaceExpr): string {
    return this.func('REPEAT', [literal(' '), expression.args.this]);
  }

  buildPropertySql (expression: BuildPropertyExpr): string {
    return `BUILD ${this.sql(expression, 'this')}`;
  }

  refreshTriggerPropertySql (expression: RefreshTriggerPropertyExpr): string {
    const method = this.sql(expression, 'method');
    const kind = expression.args.kind;
    if (!kind) {
      return `REFRESH ${method}`;
    }

    const every = this.sql(expression, 'every');
    const unit = this.sql(expression, 'unit');
    const everyStr = every ? ` EVERY ${every} ${unit}` : '';
    let starts = this.sql(expression, 'starts');
    starts = starts ? ` STARTS ${starts}` : '';

    return `REFRESH ${method} ON ${kind}${everyStr}${starts}`;
  }

  modelAttributeSql (_expression: ModelAttributeExpr): string {
    this.unsupported('The model!attribute syntax is not supported');
    return '';
  }

  directoryStageSql (expression: DirectoryStageExpr): string {
    return this.func('DIRECTORY', [expression.args.this]);
  }

  uuidSql (expression: UuidExpr): string {
    const uuidFuncSql = this.func('UUID', []);
    const isString = expression.args.isString;
    if (isString && !this.dialect._constructor.UUID_IS_STRING_TYPE) {
      return this.sql(cast(var_(uuidFuncSql) as Expression, DataTypeExprKind.VARCHAR));
    }
    return uuidFuncSql;
  }

  initcapSql (expression: InitcapExpr): string {
    let delimiters = expression.args.expression;

    if (delimiters) {
      // Do not generate delimiters arg if we are round-tripping from default delimiters
      if (
        delimiters.isString
        && delimiters.args.this === this.dialect._constructor.INITCAP_DEFAULT_DELIMITER_CHARS
      ) {
        delimiters = undefined;
      } else if (!this.dialect._constructor.INITCAP_SUPPORTS_CUSTOM_DELIMITERS) {
        this.unsupported('INITCAP does not support custom delimiters');
        delimiters = undefined;
      }
    }

    return this.func('INITCAP', [expression.args.this, delimiters]);
  }

  localtimeSql (expression: LocaltimeExpr): string {
    const thisArg = expression.args.this;
    return thisArg ? this.func('LOCALTIME', [thisArg]) : 'LOCALTIME';
  }

  localtimestampSql (expression: LocaltimestampExpr): string {
    const thisArg = expression.args.this;
    return thisArg ? this.func('LOCALTIMESTAMP', [thisArg]) : 'LOCALTIMESTAMP';
  }

  weekStartSql (expression: WeekStartExpr): string {
    const thisExpr = expression.args.this;
    const thisName = thisExpr?.name.toUpperCase();
    if (this.dialect._constructor.WEEK_OFFSET === -1 && thisName === 'SUNDAY') {
      return 'WEEK';
    }
    return this.func('WEEK', [thisExpr]);
  }

  chrSql (expression: Expression, options: { name?: string } = {}): string {
    const { name = 'CHR' } = options;
    const thisStr = this.expressions(expression);
    const charset = this.sql(expression, 'charset');
    const using = charset ? ` USING ${charset}` : '';
    return this.func(name, [thisStr + using]);
  }

  blockSql (expression: Expression): string {
    const expressions = this.expressions(expression, {
      sep: '; ',
      flat: true,
    });
    return expressions ? `${expressions}` : '';
  }

  tableSampleSql (expression: TableSampleExpr, options: { tablesampleKeyword?: string } = {}): string {
    const { tablesampleKeyword } = options;
    let method = this.sql(expression, 'method');
    method = method && this._constructor.TABLESAMPLE_WITH_METHOD ? `${method} ` : '';
    const numerator = this.sql(expression, 'bucketNumerator');
    const denominator = this.sql(expression, 'bucketDenominator');
    let field = this.sql(expression, 'bucketField');
    field = field ? ` ON ${field}` : '';
    const bucket = numerator ? `BUCKET ${numerator} OUT OF ${denominator}${field}` : '';
    let seed = this.sql(expression, 'seed');
    seed = seed ? ` ${this._constructor.TABLESAMPLE_SEED_KEYWORD} (${seed})` : '';

    let size = this.sql(expression, 'size');
    if (size && this._constructor.TABLESAMPLE_SIZE_IS_ROWS) {
      size = `${size} ROWS`;
    }

    let percent = this.sql(expression, 'percent');
    if (percent && !this.dialect._constructor.TABLESAMPLE_SIZE_IS_PERCENT) {
      percent = `${percent} PERCENT`;
    }

    let expr = `${bucket}${percent}${size}`;
    if (this._constructor.TABLESAMPLE_REQUIRES_PARENS) {
      expr = `(${expr})`;
    }

    const keyword = tablesampleKeyword || this._constructor.TABLESAMPLE_KEYWORDS;
    return ` ${keyword} ${method}${expr}${seed}`;
  }

  maskingPolicyColumnConstraintSql (expression: MaskingPolicyColumnConstraintExpr): string {
    const thisStr = this.sql(expression, 'this');
    let expressions = this.expressions(expression, { flat: true });
    expressions = expressions ? ` USING (${expressions})` : '';
    return `MASKING POLICY ${thisStr}${expressions}`;
  }

  uniqueKeyPropertySql (expression: UniqueKeyPropertyExpr, options: { prefix?: string } = {}): string {
    const { prefix = 'UNIQUE KEY' } = options;
    return `${prefix} (${this.expressions(expression, { flat: true })})`;
  }

  /**
   * Format the time expression using the dialect's inverse time mapping.
   */
  formatTime (
    expression: Expression,
    inverseTimeMapping?: Record<string, string>,
    inverseTimeTrie?: TrieNode,
  ): string | undefined {
    const dialectCls = this.dialect._constructor;
    const mapping = inverseTimeMapping ?? dialectCls.INVERSE_TIME_MAPPING;
    const trie = inverseTimeTrie ?? dialectCls.INVERSE_TIME_TRIE;
    return formatTime(
      this.sql(expression, 'format'),
      mapping ?? {},
      trie,
    );
  }

  ceilSql (expression: CeilExpr): string {
    return this.ceilFloor(expression);
  }

  floorSql (expression: FloorExpr): string {
    return this.ceilFloor(expression);
  }

  offsetLimitModifiers (expression: Expression, options: { fetch: boolean }, limit: Expression | undefined): string[] {
    const { fetch } = options;
    return [fetch ? this.sql(expression, 'offset') : this.sql(limit), fetch ? this.sql(limit) : this.sql(expression, 'offset')];
  }
}

export function generate (expression: Expression, opts?: GeneratorOptions): string {
  const generator = new Generator(opts);
  return generator.generate(expression);
}
