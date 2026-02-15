// https://github.com/tobymao/sqlglot/blob/main/sqlglot/expressions.py

import { createHash } from 'crypto';
import { DateTime } from 'luxon';
import {
  Dialect, type DialectType,
} from './dialects/dialect';
import { Token } from './tokens';
import {
  ensureList, splitNumWords,
} from './helper';
import {
  type Merge,
  multiInherit,
  type RequiredMap,
} from './port_internals';
import { traverseScope } from './optimizer/scope';
import {
  ErrorLevel, ParseError,
} from './errors';
import {
  parseOne, type ParseOptions,
} from './parser';

export const SQLGLOT_META = 'sqlglot.meta';
export const SQLGLOT_ANONYMOUS = 'sqlglot.anonymous';
export const TABLE_PARTS = [
  'this',
  'db',
  'catalog',
] as const;
export const COLUMN_PARTS = [
  'this',
  'table',
  'db',
  'catalog',
] as const;
export const POSITION_META_KEYS = [
  'line',
  'col',
  'start',
  'end',
] as const;

/**
 * Convert a value to boolean
 * @param value - Value to convert
 * @returns Boolean value
 */
function toBool (value: unknown): boolean {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const lower = value.toLowerCase().trim();
    return lower === 'true' || lower === '1' || lower === 'yes';
  }
  return Boolean(value);
}

/** Expression key enum */
export enum ExpressionKey {
  ABS = 'abs',
  ACOS = 'acos',
  ACOSH = 'acosh',
  ADD = 'add',
  ADD_CONSTRAINT = 'add_constraint',
  ADD_MONTHS = 'add_months',
  ADD_PARTITION = 'add_partition',
  ADJACENT = 'adjacent',
  AGG_FUNC = 'agg_func',
  AI_AGG = 'ai_agg',
  AI_CLASSIFY = 'ai_classify',
  AI_SUMMARIZE_AGG = 'ai_summarize_agg',
  ALGORITHM_PROPERTY = 'algorithm_property',
  ALIAS = 'alias',
  ALIASES = 'aliases',
  ALL = 'all',
  ALLOWED_VALUES_PROPERTY = 'allowed_values_property',
  ALTER = 'alter',
  ALTER_COLUMN = 'alter_column',
  ALTER_DIST_STYLE = 'alter_dist_style',
  ALTER_INDEX = 'alter_index',
  ALTER_RENAME = 'alter_rename',
  ALTER_SESSION = 'alter_session',
  ALTER_SET = 'alter_set',
  ALTER_SORT_KEY = 'alter_sort_key',
  ANALYZE = 'analyze',
  ANALYZE_COLUMNS = 'analyze_columns',
  ANALYZE_DELETE = 'analyze_delete',
  ANALYZE_HISTOGRAM = 'analyze_histogram',
  ANALYZE_LIST_CHAINED_ROWS = 'analyze_list_chained_rows',
  ANALYZE_SAMPLE = 'analyze_sample',
  ANALYZE_STATISTICS = 'analyze_statistics',
  ANALYZE_VALIDATE = 'analyze_validate',
  ANALYZE_WITH = 'analyze_with',
  AND = 'and',
  ANONYMOUS = 'anonymous',
  ANONYMOUS_AGG_FUNC = 'anonymous_agg_func',
  ANY = 'any',
  ANY_VALUE = 'any_value',
  APPLY = 'apply',
  APPROXIMATE_SIMILARITY = 'approximate_similarity',
  APPROX_DISTINCT = 'approx_distinct',
  APPROX_PERCENTILE_ACCUMULATE = 'approx_percentile_accumulate',
  APPROX_PERCENTILE_COMBINE = 'approx_percentile_combine',
  APPROX_PERCENTILE_ESTIMATE = 'approx_percentile_estimate',
  APPROX_QUANTILE = 'approx_quantile',
  APPROX_QUANTILES = 'approx_quantiles',
  APPROX_TOP_K = 'approx_top_k',
  APPROX_TOP_K_ACCUMULATE = 'approx_top_k_accumulate',
  APPROX_TOP_K_COMBINE = 'approx_top_k_combine',
  APPROX_TOP_K_ESTIMATE = 'approx_top_k_estimate',
  APPROX_TOP_SUM = 'approx_top_sum',
  ARG_MAX = 'arg_max',
  ARG_MIN = 'arg_min',
  ARRAY = 'array',
  ARRAYS_ZIP = 'arrays_zip',
  ARRAY_AGG = 'array_agg',
  ARRAY_ALL = 'array_all',
  ARRAY_ANY = 'array_any',
  ARRAY_APPEND = 'array_append',
  ARRAY_COMPACT = 'array_compact',
  ARRAY_CONCAT = 'array_concat',
  ARRAY_CONCAT_AGG = 'array_concat_agg',
  ARRAY_CONSTRUCT_COMPACT = 'array_construct_compact',
  ARRAY_CONTAINS = 'array_contains',
  ARRAY_CONTAINS_ALL = 'array_contains_all',
  ARRAY_FILTER = 'array_filter',
  ARRAY_FIRST = 'array_first',
  ARRAY_INSERT = 'array_insert',
  ARRAY_INTERSECT = 'array_intersect',
  ARRAY_LAST = 'array_last',
  ARRAY_OVERLAPS = 'array_overlaps',
  ARRAY_PREPEND = 'array_prepend',
  ARRAY_REMOVE = 'array_remove',
  ARRAY_REMOVE_AT = 'array_remove_at',
  ARRAY_REVERSE = 'array_reverse',
  ARRAY_SIZE = 'array_size',
  ARRAY_SLICE = 'array_slice',
  ARRAY_SORT = 'array_sort',
  ARRAY_SUM = 'array_sum',
  ARRAY_TO_STRING = 'array_to_string',
  ARRAY_UNION_AGG = 'array_union_agg',
  ARRAY_UNIQUE_AGG = 'array_unique_agg',
  ASCII = 'ascii',
  ASIN = 'asin',
  ASINH = 'asinh',
  ATAN = 'atan',
  ATAN2 = 'atan2',
  ATANH = 'atanh',
  ATTACH = 'attach',
  ATTACH_OPTION = 'attach_option',
  AT_INDEX = 'at_index',
  AT_TIME_ZONE = 'at_time_zone',
  AUTO_INCREMENT_COLUMN_CONSTRAINT = 'auto_increment_column_constraint',
  AUTO_INCREMENT_PROPERTY = 'auto_increment_property',
  AUTO_REFRESH_PROPERTY = 'auto_refresh_property',
  AVG = 'avg',
  BACKUP_PROPERTY = 'backup_property',
  BASE64_DECODE_BINARY = 'base64_decode_binary',
  BASE64_DECODE_STRING = 'base64_decode_string',
  BASE64_ENCODE = 'base64_encode',
  BETWEEN = 'between',
  BINARY = 'binary',
  BITMAP_BIT_POSITION = 'bitmap_bit_position',
  BITMAP_BUCKET_NUMBER = 'bitmap_bucket_number',
  BITMAP_CONSTRUCT_AGG = 'bitmap_construct_agg',
  BITMAP_COUNT = 'bitmap_count',
  BITMAP_OR_AGG = 'bitmap_or_agg',
  BITWISE_AND = 'bitwise_and',
  BITWISE_AND_AGG = 'bitwise_and_agg',
  BITWISE_COUNT = 'bitwise_count',
  BITWISE_LEFT_SHIFT = 'bitwise_left_shift',
  BITWISE_NOT = 'bitwise_not',
  BITWISE_OR = 'bitwise_or',
  BITWISE_OR_AGG = 'bitwise_or_agg',
  BITWISE_RIGHT_SHIFT = 'bitwise_right_shift',
  BITWISE_XOR = 'bitwise_xor',
  BITWISE_XOR_AGG = 'bitwise_xor_agg',
  BIT_LENGTH = 'bit_length',
  BIT_STRING = 'bit_string',
  BLOCK_COMPRESSION_PROPERTY = 'block_compression_property',
  BOOLAND = 'booland',
  BOOLEAN = 'boolean',
  BOOLNOT = 'boolnot',
  BOOLOR = 'boolor',
  BOOLXOR_AGG = 'boolxor_agg',
  BRACKET = 'bracket',
  BUILD_PROPERTY = 'build_property',
  BYTE_LENGTH = 'byte_length',
  BYTE_STRING = 'byte_string',
  CACHE = 'cache',
  CASE = 'case',
  CASE_SPECIFIC_COLUMN_CONSTRAINT = 'case_specific_column_constraint',
  CAST = 'cast',
  CAST_TO_STR_TYPE = 'cast_to_str_type',
  CBRT = 'cbrt',
  CEIL = 'ceil',
  CHANGES = 'changes',
  CHARACTER_SET = 'character_set',
  CHARACTER_SET_COLUMN_CONSTRAINT = 'character_set_column_constraint',
  CHARACTER_SET_PROPERTY = 'character_set_property',
  CHECK = 'check',
  CHECKSUM_PROPERTY = 'checksum_property',
  CHECK_COLUMN_CONSTRAINT = 'check_column_constraint',
  CHECK_JSON = 'check_json',
  CHECK_XML = 'check_xml',
  CHR = 'chr',
  CLONE = 'clone',
  CLUSTER = 'cluster',
  CLUSTERED_BY_PROPERTY = 'clustered_by_property',
  CLUSTERED_COLUMN_CONSTRAINT = 'clustered_column_constraint',
  COALESCE = 'coalesce',
  CODE_POINTS_TO_BYTES = 'code_points_to_bytes',
  CODE_POINTS_TO_STRING = 'code_points_to_string',
  COLLATE = 'collate',
  COLLATE_COLUMN_CONSTRAINT = 'collate_column_constraint',
  COLLATE_PROPERTY = 'collate_property',
  COLLATION = 'collation',
  COLUMN = 'column',
  COLUMNS = 'columns',
  COLUMN_CONSTRAINT = 'column_constraint',
  COLUMN_CONSTRAINT_KIND = 'column_constraint_kind',
  COLUMN_DEF = 'column_def',
  COLUMN_POSITION = 'column_position',
  COLUMN_PREFIX = 'column_prefix',
  COMBINED_AGG_FUNC = 'combined_agg_func',
  COMBINED_PARAMETERIZED_AGG = 'combined_parameterized_agg',
  COMMAND = 'command',
  COMMENT = 'comment',
  COMMENT_COLUMN_CONSTRAINT = 'comment_column_constraint',
  COMMIT = 'commit',
  COMPREHENSION = 'comprehension',
  COMPRESS = 'compress',
  COMPRESS_COLUMN_CONSTRAINT = 'compress_column_constraint',
  COMPUTED_COLUMN_CONSTRAINT = 'computed_column_constraint',
  CONCAT = 'concat',
  CONCAT_WS = 'concat_ws',
  CONDITION = 'condition',
  CONDITIONAL_INSERT = 'conditional_insert',
  CONNECT = 'connect',
  CONNECTOR = 'connector',
  CONNECT_BY_ROOT = 'connect_by_root',
  CONSTRAINT = 'constraint',
  CONTAINS = 'contains',
  CONVERT = 'convert',
  CONVERT_TIMEZONE = 'convert_timezone',
  CONVERT_TO_CHARSET = 'convert_to_charset',
  COPY = 'copy',
  COPY_GRANTS_PROPERTY = 'copy_grants_property',
  COPY_PARAMETER = 'copy_parameter',
  CORR = 'corr',
  COS = 'cos',
  COSH = 'cosh',
  COSINE_DISTANCE = 'cosine_distance',
  COT = 'cot',
  COTH = 'coth',
  COUNT = 'count',
  COUNT_IF = 'count_if',
  COVAR_POP = 'covar_pop',
  COVAR_SAMP = 'covar_samp',
  CREATE = 'create',
  CREDENTIALS = 'credentials',
  CREDENTIALS_PROPERTY = 'credentials_property',
  CSC = 'csc',
  CSCH = 'csch',
  CTE = 'cte',
  CUBE = 'cube',
  CUME_DIST = 'cume_dist',
  CURRENT_ACCOUNT = 'current_account',
  CURRENT_ACCOUNT_NAME = 'current_account_name',
  CURRENT_AVAILABLE_ROLES = 'current_available_roles',
  CURRENT_CATALOG = 'current_catalog',
  CURRENT_CLIENT = 'current_client',
  CURRENT_DATABASE = 'current_database',
  CURRENT_DATE = 'current_date',
  CURRENT_DATETIME = 'current_datetime',
  CURRENT_IP_ADDRESS = 'current_ip_address',
  CURRENT_ORGANIZATION_NAME = 'current_organization_name',
  CURRENT_ORGANIZATION_USER = 'current_organization_user',
  CURRENT_REGION = 'current_region',
  CURRENT_ROLE = 'current_role',
  CURRENT_ROLE_TYPE = 'current_role_type',
  CURRENT_SCHEMA = 'current_schema',
  CURRENT_SCHEMAS = 'current_schemas',
  CURRENT_SECONDARY_ROLES = 'current_secondary_roles',
  CURRENT_SESSION = 'current_session',
  CURRENT_STATEMENT = 'current_statement',
  CURRENT_TIME = 'current_time',
  CURRENT_TIMESTAMP = 'current_timestamp',
  CURRENT_TIMESTAMP_LTZ = 'current_timestamp_l_t_z',
  CURRENT_TIMEZONE = 'current_timezone',
  CURRENT_TRANSACTION = 'current_transaction',
  CURRENT_USER = 'current_user',
  CURRENT_VERSION = 'current_version',
  CURRENT_WAREHOUSE = 'current_warehouse',
  DATA_BLOCKSIZE_PROPERTY = 'data_blocksize_property',
  DATA_DELETION_PROPERTY = 'data_deletion_property',
  DATA_TYPE = 'data_type',
  DATA_TYPE_PARAM = 'data_type_param',
  DATE = 'date',
  DATETIME = 'datetime',
  DATETIME_ADD = 'datetime_add',
  DATETIME_DIFF = 'datetime_diff',
  DATETIME_SUB = 'datetime_sub',
  DATETIME_TRUNC = 'datetime_trunc',
  DATE_ADD = 'date_add',
  DATE_BIN = 'date_bin',
  DATE_DIFF = 'date_diff',
  DATE_FORMAT_COLUMN_CONSTRAINT = 'date_format_column_constraint',
  DATE_FROM_PARTS = 'date_from_parts',
  DATE_FROM_UNIX_DATE = 'date_from_unix_date',
  DATE_STR_TO_DATE = 'date_str_to_date',
  DATE_SUB = 'date_sub',
  DATE_TO_DATE_STR = 'date_to_date_str',
  DATE_TO_DI = 'date_to_di',
  DATE_TRUNC = 'date_trunc',
  DAY = 'day',
  DAYNAME = 'dayname',
  DAY_OF_MONTH = 'day_of_month',
  DAY_OF_WEEK = 'day_of_week',
  DAY_OF_WEEK_ISO = 'day_of_week_iso',
  DAY_OF_YEAR = 'day_of_year',
  DDL = 'ddl',
  DECLARE = 'declare',
  DECLARE_ITEM = 'declare_item',
  DECODE = 'decode',
  DECODE_CASE = 'decode_case',
  DECOMPRESS_BINARY = 'decompress_binary',
  DECOMPRESS_STRING = 'decompress_string',
  DECRYPT = 'decrypt',
  DECRYPT_RAW = 'decrypt_raw',
  DEFAULT_COLUMN_CONSTRAINT = 'default_column_constraint',
  DEFINER_PROPERTY = 'definer_property',
  DEGREES = 'degrees',
  DELETE = 'delete',
  DENSE_RANK = 'dense_rank',
  DERIVED_TABLE = 'derived_table',
  DESCRIBE = 'describe',
  DETACH = 'detach',
  DICT_PROPERTY = 'dict_property',
  DICT_RANGE = 'dict_range',
  DICT_SUB_PROPERTY = 'dict_sub_property',
  DIRECTORY = 'directory',
  DIRECTORY_STAGE = 'directory_stage',
  DISTANCE = 'distance',
  DISTINCT = 'distinct',
  DISTRIBUTE = 'distribute',
  DISTRIBUTED_BY_PROPERTY = 'distributed_by_property',
  DIST_KEY_PROPERTY = 'dist_key_property',
  DIST_STYLE_PROPERTY = 'dist_style_property',
  DIV = 'div',
  DI_TO_DATE = 'di_to_date',
  DML = 'dml',
  DOT = 'dot',
  DOT_PRODUCT = 'dot_product',
  DROP = 'drop',
  DROP_PARTITION = 'drop_partition',
  DUPLICATE_KEY_PROPERTY = 'duplicate_key_property',
  DYNAMIC_PROPERTY = 'dynamic_property',
  D_PIPE = 'd_pipe',
  ELT = 'elt',
  EMPTY_PROPERTY = 'empty_property',
  ENCODE = 'encode',
  ENCODE_COLUMN_CONSTRAINT = 'encode_column_constraint',
  ENCODE_PROPERTY = 'encode_property',
  ENCRYPT = 'encrypt',
  ENCRYPT_RAW = 'encrypt_raw',
  ENDS_WITH = 'ends_with',
  ENGINE_PROPERTY = 'engine_property',
  ENVIROMENT_PROPERTY = 'enviroment_property',
  EPHEMERAL_COLUMN_CONSTRAINT = 'ephemeral_column_constraint',
  EQ = 'eq',
  EQUAL_NULL = 'equal_null',
  ESCAPE = 'escape',
  EUCLIDEAN_DISTANCE = 'euclidean_distance',
  EXCEPT = 'except',
  EXCLUDE_COLUMN_CONSTRAINT = 'exclude_column_constraint',
  EXECUTE_AS_PROPERTY = 'execute_as_property',
  EXISTS = 'exists',
  EXP = 'exp',
  EXPLODE = 'explode',
  EXPLODE_OUTER = 'explode_outer',
  EXPLODING_GENERATE_SERIES = 'exploding_generate_series',
  EXPORT = 'export',
  EXPRESSION = 'expression',
  EXTENDS_LEFT = 'extends_left',
  EXTENDS_RIGHT = 'extends_right',
  EXTERNAL_PROPERTY = 'external_property',
  EXTRACT = 'extract',
  FACTORIAL = 'factorial',
  FALLBACK_PROPERTY = 'fallback_property',
  FARM_FINGERPRINT = 'farm_fingerprint',
  FEATURES_AT_TIME = 'features_at_time',
  FETCH = 'fetch',
  FILE_FORMAT_PROPERTY = 'file_format_property',
  FILTER = 'filter',
  FINAL = 'final',
  FIRST = 'first',
  FIRST_VALUE = 'first_value',
  FLATTEN = 'flatten',
  FLOAT64 = 'float64',
  FLOOR = 'floor',
  FORCE_PROPERTY = 'force_property',
  FOREIGN_KEY = 'foreign_key',
  FORMAT = 'format',
  FORMAT_JSON = 'format_json',
  FORMAT_PHRASE = 'format_phrase',
  FOR_IN = 'for_in',
  FREESPACE_PROPERTY = 'freespace_property',
  FROM = 'from',
  FROM_BASE = 'from_base',
  FROM_BASE32 = 'from_base32',
  FROM_BASE64 = 'from_base64',
  FROM_ISO8601_TIMESTAMP = 'from_iso8601_timestamp',
  FROM_TIME_ZONE = 'from_time_zone',
  FUNC = 'func',
  GAP_FILL = 'gap_fill',
  GENERATED_AS_IDENTITY_COLUMN_CONSTRAINT = 'generated_as_identity_column_constraint',
  GENERATED_AS_ROW_COLUMN_CONSTRAINT = 'generated_as_row_column_constraint',
  GENERATE_DATE_ARRAY = 'generate_date_array',
  GENERATE_EMBEDDING = 'generate_embedding',
  GENERATE_SERIES = 'generate_series',
  GENERATE_TIMESTAMP_ARRAY = 'generate_timestamp_array',
  GENERATOR = 'generator',
  GET = 'get',
  GETBIT = 'getbit',
  GET_EXTRACT = 'get_extract',
  GLOB = 'glob',
  GLOBAL_PROPERTY = 'global_property',
  GRANT = 'grant',
  GRANT_PRINCIPAL = 'grant_principal',
  GRANT_PRIVILEGE = 'grant_privilege',
  GREATEST = 'greatest',
  GROUP = 'group',
  GROUPING = 'grouping',
  GROUPING_ID = 'grouping_id',
  GROUPING_SETS = 'grouping_sets',
  GROUP_CONCAT = 'group_concat',
  GT = 'gt',
  GTE = 'gte',
  HASH_AGG = 'hash_agg',
  HAVING = 'having',
  HAVING_MAX = 'having_max',
  HEAP_PROPERTY = 'heap_property',
  HEREDOC = 'heredoc',
  HEX = 'hex',
  HEX_DECODE_STRING = 'hex_decode_string',
  HEX_ENCODE = 'hex_encode',
  HEX_STRING = 'hex_string',
  HINT = 'hint',
  HISTORICAL_DATA = 'historical_data',
  HLL = 'hll',
  HOST = 'host',
  HOUR = 'hour',
  ICEBERG_PROPERTY = 'iceberg_property',
  IDENTIFIER = 'identifier',
  IF = 'if',
  IGNORE_NULLS = 'ignore_nulls',
  IN = 'in',
  INCLUDE_PROPERTY = 'include_property',
  INDEX = 'index',
  INDEX_COLUMN_CONSTRAINT = 'index_column_constraint',
  INDEX_CONSTRAINT_OPTION = 'index_constraint_option',
  INDEX_PARAMETERS = 'index_parameters',
  INDEX_TABLE_HINT = 'index_table_hint',
  INHERITS_PROPERTY = 'inherits_property',
  INITCAP = 'initcap',
  INLINE = 'inline',
  INLINE_LENGTH_COLUMN_CONSTRAINT = 'inline_length_column_constraint',
  INPUT_MODEL_PROPERTY = 'input_model_property',
  INPUT_OUTPUT_FORMAT = 'input_output_format',
  INSERT = 'insert',
  INSTALL = 'install',
  INT64 = 'int64',
  INTERSECT = 'intersect',
  INTERVAL = 'interval',
  INTERVAL_OP = 'interval_op',
  INTERVAL_SPAN = 'interval_span',
  INTO = 'into',
  INTRODUCER = 'introducer',
  INT_DIV = 'int_div',
  IN_OUT_COLUMN_CONSTRAINT = 'in_out_column_constraint',
  IS = 'is',
  ISOLATED_LOADING_PROPERTY = 'isolated_loading_property',
  IS_ARRAY = 'is_array',
  IS_ASCII = 'is_ascii',
  IS_INF = 'is_inf',
  IS_NAN = 'is_nan',
  IS_NULL_VALUE = 'is_null_value',
  ILIKE = 'ilike',
  JAROWINKLER_SIMILARITY = 'jarowinkler_similarity',
  JOIN = 'join',
  JOIN_HINT = 'join_hint',
  JOURNAL_PROPERTY = 'journal_property',
  JSON = 'json',
  JSON_ARRAY = 'json_array',
  JSON_ARRAY_AGG = 'json_array_agg',
  JSON_ARRAY_APPEND = 'json_array_append',
  JSON_ARRAY_CONTAINS = 'json_array_contains',
  JSON_ARRAY_INSERT = 'json_array_insert',
  JSON_BOOL = 'json_bool',
  JSONB_CONTAINS = 'jsonb_contains',
  JSONB_CONTAINS_ALL_TOP_KEYS = 'jsonb_contains_all_top_keys',
  JSONB_CONTAINS_ANY_TOP_KEYS = 'jsonb_contains_any_top_keys',
  JSONB_DELETE_AT_PATH = 'jsonb_delete_at_path',
  JSONB_EXISTS = 'jsonb_exists',
  JSONB_EXTRACT = 'jsonb_extract',
  JSONB_EXTRACT_SCALAR = 'jsonb_extract_scalar',
  JSONB_OBJECT_AGG = 'jsonb_object_agg',
  JSON_CAST = 'json_cast',
  JSON_COLUMN_DEF = 'json_column_def',
  JSON_EXISTS = 'json_exists',
  JSON_EXTRACT = 'json_extract',
  JSON_EXTRACT_ARRAY = 'json_extract_array',
  JSON_EXTRACT_QUOTE = 'json_extract_quote',
  JSON_EXTRACT_SCALAR = 'json_extract_scalar',
  JSON_FORMAT = 'json_format',
  JSON_KEYS = 'json_keys',
  JSON_KEYS_AT_DEPTH = 'json_keys_at_depth',
  JSON_KEY_VALUE = 'json_key_value',
  JSON_OBJECT = 'json_object',
  JSON_OBJECT_AGG = 'json_object_agg',
  JSON_PATH = 'json_path',
  JSON_PATH_FILTER = 'json_path_filter',
  JSON_PATH_KEY = 'json_path_key',
  JSON_PATH_PART = 'json_path_part',
  JSON_PATH_RECURSIVE = 'json_path_recursive',
  JSON_PATH_ROOT = 'json_path_root',
  JSON_PATH_SCRIPT = 'json_path_script',
  JSON_PATH_SELECTOR = 'json_path_selector',
  JSON_PATH_SLICE = 'json_path_slice',
  JSON_PATH_SUBSCRIPT = 'json_path_subscript',
  JSON_PATH_UNION = 'json_path_union',
  JSON_PATH_WILDCARD = 'json_path_wildcard',
  JSON_REMOVE = 'json_remove',
  JSON_SCHEMA = 'json_schema',
  JSON_SET = 'json_set',
  JSON_STRIP_NULLS = 'json_strip_nulls',
  JSON_TABLE = 'json_table',
  JSON_TYPE = 'json_type',
  JSON_VALUE = 'json_value',
  JSON_VALUE_ARRAY = 'json_value_array',
  JUSTIFY_DAYS = 'justify_days',
  JUSTIFY_HOURS = 'justify_hours',
  JUSTIFY_INTERVAL = 'justify_interval',
  KILL = 'kill',
  KURTOSIS = 'kurtosis',
  KWARG = 'kwarg',
  LAG = 'lag',
  LAMBDA = 'lambda',
  LANGUAGE_PROPERTY = 'language_property',
  LAST = 'last',
  LAST_DAY = 'last_day',
  LAST_VALUE = 'last_value',
  LATERAL = 'lateral',
  LAX_BOOL = 'lax_bool',
  LAX_FLOAT64 = 'lax_float64',
  LAX_INT64 = 'lax_int64',
  LAX_STRING = 'lax_string',
  LEAD = 'lead',
  LEAST = 'least',
  LEFT = 'left',
  LENGTH = 'length',
  LEVENSHTEIN = 'levenshtein',
  LIKE = 'like',
  LIKE_PROPERTY = 'like_property',
  LIMIT = 'limit',
  LIMIT_OPTIONS = 'limit_options',
  LIST = 'list',
  LITERAL = 'literal',
  LN = 'ln',
  LOAD_DATA = 'load_data',
  LOCALTIME = 'localtime',
  LOCALTIMESTAMP = 'localtimestamp',
  LOCATION = 'location',
  LOCATION_PROPERTY = 'location_property',
  LOCK = 'lock',
  LOCKING_PROPERTY = 'locking_property',
  LOCKING_STATEMENT = 'locking_statement',
  LOCK_PROPERTY = 'lock_property',
  LOG = 'log',
  LOGICAL_AND = 'logical_and',
  LOGICAL_OR = 'logical_or',
  LOG_PROPERTY = 'log_property',
  LOWER = 'lower',
  LOWER_HEX = 'lower_hex',
  LT = 'lt',
  LTE = 'lte',
  MAKE_INTERVAL = 'make_interval',
  MANHATTAN_DISTANCE = 'manhattan_distance',
  MAP = 'map',
  MAP_CAT = 'map_cat',
  MAP_CONTAINS_KEY = 'map_contains_key',
  MAP_DELETE = 'map_delete',
  MAP_FROM_ENTRIES = 'map_from_entries',
  MAP_INSERT = 'map_insert',
  MAP_KEYS = 'map_keys',
  MAP_PICK = 'map_pick',
  MAP_SIZE = 'map_size',
  MASKING_POLICY_COLUMN_CONSTRAINT = 'masking_policy_column_constraint',
  MATCH = 'match',
  MATCH_AGAINST = 'match_against',
  MATCH_RECOGNIZE = 'match_recognize',
  MATCH_RECOGNIZE_MEASURE = 'match_recognize_measure',
  MATERIALIZED_PROPERTY = 'materialized_property',
  MAX = 'max',
  MD5 = 'md5',
  MD5_DIGEST = 'md5_digest',
  MD5_NUMBER_LOWER64 = 'md5_number_lower64',
  MD5_NUMBER_UPPER64 = 'md5_number_upper64',
  MEDIAN = 'median',
  MERGE = 'merge',
  MERGE_BLOCK_RATIO_PROPERTY = 'merge_block_ratio_property',
  MERGE_TREE_TTL = 'merge_tree_ttl',
  MERGE_TREE_TTL_ACTION = 'merge_tree_ttl_action',
  MIN = 'min',
  MINHASH = 'minhash',
  MINHASH_COMBINE = 'minhash_combine',
  MINUTE = 'minute',
  ML_FORECAST = 'ml_forecast',
  ML_TRANSLATE = 'ml_translate',
  MOD = 'mod',
  MODE = 'mode',
  MODEL_ATTRIBUTE = 'model_attribute',
  MONTH = 'month',
  MONTHNAME = 'monthname',
  MONTHS_BETWEEN = 'months_between',
  MUL = 'mul',
  MULTITABLE_INSERTS = 'multitable_inserts',
  NATIONAL = 'national',
  NEG = 'neg',
  NEQ = 'neq',
  NET_FUNC = 'net_func',
  NEXT_DAY = 'next_day',
  NEXT_VALUE_FOR = 'next_value_for',
  NON_CLUSTERED_COLUMN_CONSTRAINT = 'non_clustered_column_constraint',
  NORMAL = 'normal',
  NORMALIZE = 'normalize',
  NOT = 'not',
  NOT_FOR_REPLICATION_COLUMN_CONSTRAINT = 'not_for_replication_column_constraint',
  NOT_NULL_COLUMN_CONSTRAINT = 'not_null_column_constraint',
  NO_PRIMARY_INDEX_PROPERTY = 'no_primary_index_property',
  NTH_VALUE = 'nth_value',
  NTILE = 'ntile',
  NULL = 'null',
  NULLIF = 'nullif',
  NULL_SAFE_EQ = 'null_safe_eq',
  NULL_SAFE_NEQ = 'null_safe_neq',
  NUMBER_TO_STR = 'number_to_str',
  NVL2 = 'nvl2',
  OBJECT_AGG = 'object_agg',
  OBJECT_IDENTIFIER = 'object_identifier',
  OBJECT_INSERT = 'object_insert',
  OFFSET = 'offset',
  ON_CLUSTER = 'on_cluster',
  ON_COMMIT_PROPERTY = 'on_commit_property',
  ON_CONDITION = 'on_condition',
  ON_CONFLICT = 'on_conflict',
  ON_PROPERTY = 'on_property',
  ON_UPDATE_COLUMN_CONSTRAINT = 'on_update_column_constraint',
  OPCLASS = 'opclass',
  OPEN_JSON = 'open_json',
  OPEN_JSON_COLUMN_DEF = 'open_json_column_def',
  OPERATOR = 'operator',
  OR = 'or',
  ORDER = 'order',
  ORDERED = 'ordered',
  OUTPUT_MODEL_PROPERTY = 'output_model_property',
  OVERFLOW_TRUNCATE_BEHAVIOR = 'overflow_truncate_behavior',
  OVERLAPS = 'overlaps',
  OVERLAY = 'overlay',
  PAD = 'pad',
  PARAMETER = 'parameter',
  PARAMETERIZED_AGG = 'parameterized_agg',
  PAREN = 'paren',
  PARSE_BIGNUMERIC = 'parse_bignumeric',
  PARSE_DATETIME = 'parse_datetime',
  PARSE_IP = 'parse_ip',
  PARSE_JSON = 'parse_json',
  PARSE_NUMERIC = 'parse_numeric',
  PARSE_TIME = 'parse_time',
  PARSE_URL = 'parse_url',
  PARTITION = 'partition',
  PARTITIONED_BY_BUCKET = 'partitioned_by_bucket',
  PARTITIONED_BY_PROPERTY = 'partitioned_by_property',
  PARTITIONED_OF_PROPERTY = 'partitioned_of_property',
  PARTITION_BOUND_SPEC = 'partition_bound_spec',
  PARTITION_BY_LIST_PROPERTY = 'partition_by_list_property',
  PARTITION_BY_RANGE_PROPERTY = 'partition_by_range_property',
  PARTITION_BY_RANGE_PROPERTY_DYNAMIC = 'partition_by_range_property_dynamic',
  PARTITION_BY_TRUNCATE = 'partition_by_truncate',
  PARTITION_ID = 'partition_id',
  PARTITION_LIST = 'partition_list',
  PARTITION_RANGE = 'partition_range',
  PATH_COLUMN_CONSTRAINT = 'path_column_constraint',
  PERCENTILE_CONT = 'percentile_cont',
  PERCENTILE_DISC = 'percentile_disc',
  PERCENT_RANK = 'percent_rank',
  PERIOD_FOR_SYSTEM_TIME_CONSTRAINT = 'period_for_system_time_constraint',
  PI = 'pi',
  PIVOT = 'pivot',
  PIVOT_ALIAS = 'pivot_alias',
  PIVOT_ANY = 'pivot_any',
  PLACEHOLDER = 'placeholder',
  POSEXPLODE = 'posexplode',
  POSEXPLODE_OUTER = 'posexplode_outer',
  POSITIONAL_COLUMN = 'positional_column',
  POW = 'pow',
  PRAGMA = 'pragma',
  PREDICATE = 'predicate',
  PREDICT = 'predict',
  PREVIOUS_DAY = 'previous_day',
  PRE_WHERE = 'pre_where',
  PRIMARY_KEY = 'primary_key',
  PRIMARY_KEY_COLUMN_CONSTRAINT = 'primary_key_column_constraint',
  PRIOR = 'prior',
  PROJECTION_DEF = 'projection_def',
  PROJECTION_POLICY_COLUMN_CONSTRAINT = 'projection_policy_column_constraint',
  PROPERTIES = 'properties',
  PROPERTY = 'property',
  PROPERTY_EQ = 'property_eq',
  PSEUDOCOLUMN = 'pseudocolumn',
  PSEUDO_TYPE = 'pseudo_type',
  PUT = 'put',
  QUALIFY = 'qualify',
  QUANTILE = 'quantile',
  QUARTER = 'quarter',
  QUERY = 'query',
  QUERY_BAND = 'query_band',
  QUERY_OPTION = 'query_option',
  QUERY_TRANSFORM = 'query_transform',
  RADIANS = 'radians',
  RAND = 'rand',
  RANDN = 'randn',
  RANDSTR = 'randstr',
  RANGE_BUCKET = 'range_bucket',
  RANGE_N = 'range_n',
  RANK = 'rank',
  RAW_STRING = 'raw_string',
  READ_CSV = 'read_csv',
  READ_PARQUET = 'read_parquet',
  RECURSIVE_WITH_SEARCH = 'recursive_with_search',
  REDUCE = 'reduce',
  REFERENCE = 'reference',
  REFRESH = 'refresh',
  REFRESH_TRIGGER_PROPERTY = 'refresh_trigger_property',
  REGEXP_COUNT = 'regexp_count',
  REGEXP_EXTRACT = 'regexp_extract',
  REGEXP_EXTRACT_ALL = 'regexp_extract_all',
  REGEXP_FULL_MATCH = 'regexp_full_match',
  REGEXP_INSTR = 'regexp_instr',
  REGEXP_ILIKE = 'regexp_ilike',
  REGEXP_LIKE = 'regexp_like',
  REGEXP_REPLACE = 'regexp_replace',
  REGEXP_SPLIT = 'regexp_split',
  REGR_AVGX = 'regr_avgx',
  REGR_AVGY = 'regr_avgy',
  REGR_COUNT = 'regr_count',
  REGR_INTERCEPT = 'regr_intercept',
  REGR_R2 = 'regr_r2',
  REGR_SLOPE = 'regr_slope',
  REGR_SXX = 'regr_sxx',
  REGR_SXY = 'regr_sxy',
  REGR_SYY = 'regr_syy',
  REGR_VALX = 'regr_valx',
  REGR_VALY = 'regr_valy',
  REG_DOMAIN = 'reg_domain',
  REMOTE_WITH_CONNECTION_MODEL_PROPERTY = 'remote_with_connection_model_property',
  RENAME_COLUMN = 'rename_column',
  REPEAT = 'repeat',
  REPLACE = 'replace',
  REPLACE_PARTITION = 'replace_partition',
  RESPECT_NULLS = 'respect_nulls',
  RETURN = 'return',
  RETURNING = 'returning',
  RETURNS_PROPERTY = 'returns_property',
  REVERSE = 'reverse',
  REVOKE = 'revoke',
  RIGHT = 'right',
  ROLLBACK = 'rollback',
  ROLLUP = 'rollup',
  ROLLUP_INDEX = 'rollup_index',
  ROLLUP_PROPERTY = 'rollup_property',
  ROUND = 'round',
  ROW_FORMAT_DELIMITED_PROPERTY = 'row_format_delimited_property',
  ROW_FORMAT_PROPERTY = 'row_format_property',
  ROW_FORMAT_SERDE_PROPERTY = 'row_format_serde_property',
  ROW_NUMBER = 'row_number',
  RTRIMMED_LENGTH = 'rtrimmed_length',
  SAFE_ADD = 'safe_add',
  SAFE_CONVERT_BYTES_TO_STRING = 'safe_convert_bytes_to_string',
  SAFE_DIVIDE = 'safe_divide',
  SAFE_FUNC = 'safe_func',
  SAFE_MULTIPLY = 'safe_multiply',
  SAFE_NEGATE = 'safe_negate',
  SAFE_SUBTRACT = 'safe_subtract',
  SAMPLE_PROPERTY = 'sample_property',
  SCHEMA = 'schema',
  SCHEMA_COMMENT_PROPERTY = 'schema_comment_property',
  SCOPE_RESOLUTION = 'scope_resolution',
  SEARCH = 'search',
  SEARCH_IP = 'search_ip',
  SEC = 'sec',
  SECH = 'sech',
  SECOND = 'second',
  SECURE_PROPERTY = 'secure_property',
  SECURITY_PROPERTY = 'security_property',
  SELECT = 'select',
  SEMANTIC_VIEW = 'semantic_view',
  SEMICOLON = 'semicolon',
  SEQ1 = 'seq1',
  SEQ2 = 'seq2',
  SEQ4 = 'seq4',
  SEQ8 = 'seq8',
  SEQUENCE_PROPERTIES = 'sequence_properties',
  SERDE_PROPERTIES = 'serde_properties',
  SESSION_PARAMETER = 'session_parameter',
  SESSION_USER = 'session_user',
  SET = 'set',
  SETTINGS_PROPERTY = 'settings_property',
  SET_CONFIG_PROPERTY = 'set_config_property',
  SET_ITEM = 'set_item',
  SET_OPERATION = 'set_operation',
  SET_PROPERTY = 'set_property',
  SHA = 'sha',
  SHA1_DIGEST = 'sha1_digest',
  SHA2 = 'sha2',
  SHA2_DIGEST = 'sha2_digest',
  SHARING_PROPERTY = 'sharing_property',
  SHOW = 'show',
  SIGN = 'sign',
  SIMILAR_TO = 'similar_to',
  SIN = 'sin',
  SINH = 'sinh',
  SKEWNESS = 'skewness',
  SLICE = 'slice',
  SORT = 'sort',
  SORT_ARRAY = 'sort_array',
  SORT_KEY_PROPERTY = 'sort_key_property',
  SOUNDEX = 'soundex',
  SOUNDEX_P123 = 'soundex_p123',
  SPACE = 'space',
  SPLIT = 'split',
  SPLIT_PART = 'split_part',
  SQL_READ_WRITE_PROPERTY = 'sql_read_write_property',
  SQL_SECURITY_PROPERTY = 'sql_security_property',
  SQRT = 'sqrt',
  STABILITY_PROPERTY = 'stability_property',
  STANDARD_HASH = 'standard_hash',
  STAR = 'star',
  STARTS_WITH = 'starts_with',
  STAR_MAP = 'star_map',
  STDDEV = 'stddev',
  STDDEV_POP = 'stddev_pop',
  STDDEV_SAMP = 'stddev_samp',
  STORAGE_HANDLER_PROPERTY = 'storage_handler_property',
  STREAM = 'stream',
  STREAMING_TABLE_PROPERTY = 'streaming_table_property',
  STRICT_PROPERTY = 'strict_property',
  STRING = 'string',
  STRING_TO_ARRAY = 'string_to_array',
  STRUCT = 'struct',
  STRUCT_EXTRACT = 'struct_extract',
  STR_POSITION = 'str_position',
  STR_TO_DATE = 'str_to_date',
  STR_TO_MAP = 'str_to_map',
  STR_TO_TIME = 'str_to_time',
  STR_TO_UNIX = 'str_to_unix',
  STUFF = 'stuff',
  ST_DISTANCE = 'st_distance',
  ST_POINT = 'st_point',
  SUB = 'sub',
  SUBQUERY = 'subquery',
  SUBQUERY_PREDICATE = 'subquery_predicate',
  SUBSTRING = 'substring',
  SUBSTRING_INDEX = 'substring_index',
  SUM = 'sum',
  SUMMARIZE = 'summarize',
  SWAP_TABLE = 'swap_table',
  SYSTIMESTAMP = 'systimestamp',
  TABLE = 'table',
  TABLE_ALIAS = 'table_alias',
  TABLE_COLUMN = 'table_column',
  TABLE_FROM_ROWS = 'table_from_rows',
  TABLE_SAMPLE = 'table_sample',
  TAG = 'tag',
  TAGS = 'tags',
  TAN = 'tan',
  TANH = 'tanh',
  TEMPORARY_PROPERTY = 'temporary_property',
  TIME = 'time',
  TIMESTAMP = 'timestamp',
  TIMESTAMP_ADD = 'timestamp_add',
  TIMESTAMP_DIFF = 'timestamp_diff',
  TIMESTAMP_FROM_PARTS = 'timestamp_from_parts',
  TIMESTAMP_LTZ_FROM_PARTS = 'timestamp_ltz_from_parts',
  TIMESTAMP_SUB = 'timestamp_sub',
  TIMESTAMP_TRUNC = 'timestamp_trunc',
  TIMESTAMP_TZ_FROM_PARTS = 'timestamp_tz_from_parts',
  TIME_ADD = 'time_add',
  TIME_DIFF = 'time_diff',
  TIME_FROM_PARTS = 'time_from_parts',
  TIME_SLICE = 'time_slice',
  TIME_STR_TO_DATE = 'time_str_to_date',
  TIME_STR_TO_TIME = 'time_str_to_time',
  TIME_STR_TO_UNIX = 'time_str_to_unix',
  TIME_SUB = 'time_sub',
  TIME_TO_STR = 'time_to_str',
  TIME_TO_TIME_STR = 'time_to_time_str',
  TIME_TO_UNIX = 'time_to_unix',
  TIME_TRUNC = 'time_trunc',
  TIME_UNIT = 'time_unit',
  TITLE_COLUMN_CONSTRAINT = 'title_column_constraint',
  TO_ARRAY = 'to_array',
  TO_BASE32 = 'to_base32',
  TO_BASE64 = 'to_base64',
  TO_BINARY = 'to_binary',
  TO_BOOLEAN = 'to_boolean',
  TO_CHAR = 'to_char',
  TO_CODE_POINTS = 'to_code_points',
  TO_DAYS = 'to_days',
  TO_DECFLOAT = 'to_decfloat',
  TO_DOUBLE = 'to_double',
  TO_FILE = 'to_file',
  TO_MAP = 'to_map',
  TO_NUMBER = 'to_number',
  TO_TABLE_PROPERTY = 'to_table_property',
  TRANSACTION = 'transaction',
  TRANSFORM = 'transform',
  TRANSFORM_MODEL_PROPERTY = 'transform_model_property',
  TRANSIENT_PROPERTY = 'transient_property',
  TRANSLATE = 'translate',
  TRANSLATE_CHARACTERS = 'translate_characters',
  TRIM = 'trim',
  TRUNC = 'trunc',
  TRUNCATE_TABLE = 'truncate_table',
  TRY = 'try',
  TRY_BASE64_DECODE_BINARY = 'try_base64_decode_binary',
  TRY_BASE64_DECODE_STRING = 'try_base64_decode_string',
  TRY_CAST = 'try_cast',
  TRY_HEX_DECODE_BINARY = 'try_hex_decode_binary',
  TRY_HEX_DECODE_STRING = 'try_hex_decode_string',
  TRY_TO_DECFLOAT = 'try_to_decfloat',
  TS_OR_DI_TO_DI = 'ts_or_di_to_di',
  TS_OR_DS_ADD = 'ts_or_ds_add',
  TS_OR_DS_DIFF = 'ts_or_ds_diff',
  TS_OR_DS_TO_DATE = 'ts_or_ds_to_date',
  TS_OR_DS_TO_DATETIME = 'ts_or_ds_to_datetime',
  TS_OR_DS_TO_DATE_STR = 'ts_or_ds_to_date_str',
  TS_OR_DS_TO_TIME = 'ts_or_ds_to_time',
  TS_OR_DS_TO_TIMESTAMP = 'ts_or_ds_to_timestamp',
  TUPLE = 'tuple',
  TYPE = 'type',
  TYPEOF = 'typeof',
  UDTF = 'udtf',
  UNARY = 'unary',
  UNCACHE = 'uncache',
  UNHEX = 'unhex',
  UNICODE = 'unicode',
  UNICODE_STRING = 'unicode_string',
  UNIFORM = 'uniform',
  UNION = 'union',
  UNIQUE_COLUMN_CONSTRAINT = 'unique_column_constraint',
  UNIQUE_KEY_PROPERTY = 'unique_key_property',
  UNIX_DATE = 'unix_date',
  UNIX_MICROS = 'unix_micros',
  UNIX_MILLIS = 'unix_millis',
  UNIX_SECONDS = 'unix_seconds',
  UNIX_TO_STR = 'unix_to_str',
  UNIX_TO_TIME = 'unix_to_time',
  UNIX_TO_TIME_STR = 'unix_to_time_str',
  UNLOGGED_PROPERTY = 'unlogged_property',
  UNNEST = 'unnest',
  UNPIVOT_COLUMNS = 'unpivot_columns',
  UPDATE = 'update',
  UPPER = 'upper',
  UPPERCASE_COLUMN_CONSTRAINT = 'uppercase_column_constraint',
  USE = 'use',
  USER_DEFINED_FUNCTION = 'user_defined_function',
  USING_DATA = 'using_data',
  USING_TEMPLATE_PROPERTY = 'using_template_property',
  UTC_DATE = 'utc_date',
  UTC_TIME = 'utc_time',
  UTC_TIMESTAMP = 'utc_timestamp',
  UUID = 'uuid',
  VALUES = 'values',
  VAR = 'var',
  VARIADIC = 'variadic',
  VARIANCE = 'variance',
  VARIANCE_POP = 'variance_pop',
  VAR_MAP = 'var_map',
  VECTOR_SEARCH = 'vector_search',
  VERSION = 'version',
  VIEW_ATTRIBUTE_PROPERTY = 'view_attribute_property',
  VOLATILE_PROPERTY = 'volatile_property',
  WATERMARK_COLUMN_CONSTRAINT = 'watermark_column_constraint',
  WEEK = 'week',
  WEEK_OF_YEAR = 'week_of_year',
  WEEK_START = 'week_start',
  WHEN = 'when',
  WHENS = 'whens',
  WHERE = 'where',
  WIDTH_BUCKET = 'width_bucket',
  WINDOW = 'window',
  WINDOW_SPEC = 'window_spec',
  WITH = 'with',
  WITHIN_GROUP = 'within_group',
  WITH_DATA_PROPERTY = 'with_data_property',
  WITH_FILL = 'with_fill',
  WITH_JOURNAL_TABLE_PROPERTY = 'with_journal_table_property',
  WITH_OPERATOR = 'with_operator',
  WITH_PROCEDURE_OPTIONS = 'with_procedure_options',
  WITH_SCHEMA_BINDING_PROPERTY = 'with_schema_binding_property',
  WITH_SYSTEM_VERSIONING_PROPERTY = 'with_system_versioning_property',
  WITH_TABLE_HINT = 'with_table_hint',
  XML_ELEMENT = 'xml_element',
  XML_GET = 'xml_get',
  XML_KEY_VALUE_OPTION = 'xml_key_value_option',
  XML_NAMESPACE = 'xml_namespace',
  XML_TABLE = 'xml_table',
  XOR = 'xor',
  YEAR = 'year',
  YEAR_OF_WEEK = 'year_of_week',
  YEAR_OF_WEEK_ISO = 'year_of_week_iso',
  ZERO_FILL_COLUMN_CONSTRAINT = 'zero_fill_column_constraint',
  ZIPF = 'zipf',
}

export type ExpressionValue = Token | Expression | string | boolean | number | undefined;
export type ExpressionValueList<T extends ExpressionValue = ExpressionValue> = T[];

/**
 * Base arguments that all Expression classes can accept.
 */
export interface BaseExpressionArgs {
  this?: ExpressionValue;
  expression?: ExpressionValue;
  expressions?: (Expression | string | number | boolean | Token)[];
  alias?: TableAliasExpr | IdentifierExpr | string;
  isString?: boolean;
  to?: Expression;
  from?: FromExpr;
  joins?: JoinExpr[];
}

/**
 * Base class for all SQL expressions in the AST.
 *
 * Expressions form a tree structure where each node can have:
 * - args: Named arguments (properties and child expressions)
 * - parent: Parent expression in the tree
 * - key: Expression type identifier
 *
 * @example
 * const col = new ColumnExpr({ this: new IdentifierExpr({ this: 'name' }) });
 */
export class Expression {
  protected static requiredArgsCache = new WeakMap<typeof Expression, Set<string>>();

  /**
   * Get required arguments for this expression class (cached).
   * Computed from argTypes where value is true.
   */
  static requiredArgs (): Set<string> {
    let cached = this.requiredArgsCache.get(this);
    if (!cached) {
      cached = new Set<string>();
      for (const [key, required] of Object.entries(this.argTypes)) {
        if (required) {
          cached.add(key);
        }
      }
      this.requiredArgsCache.set(this, cached);
    }
    return cached;
  }

  /** The key identifying this expression type */
  key: ExpressionKey = ExpressionKey.EXPRESSION;

  /** Arguments/properties of this expression (child nodes, flags, etc.) */
  args: BaseExpressionArgs = {};

  /** Parent expression in the AST tree */
  parent?: Expression;

  /** The argument key this expression is stored under in its parent */
  argKey?: string;

  /** The index if this expression is in an array argument */
  index?: number;

  /** Comments associated with this expression */
  comments?: string[];

  /** Cached data type of this expression */
  private _type?: DataTypeExpr;

  /** Metadata attached to this expression */
  private _meta?: Record<string, unknown>;

  /** Cached hash value for this expression */
  private _hash?: string;

  /** Static arg types definition */
  // NOTE: In sqlglot, `this` is `true`,
  // but some subclasses of `Expression`
  // does not have `this`, so I set it to false
  static argTypes: RequiredMap<BaseExpressionArgs> = {};

  /** Set of required argument names */

  constructor (args: BaseExpressionArgs) {
    this.args = args;
    for (const [argKey, value] of Object.entries(args)) {
      this._setParent(argKey, value);
    }
  }

  * [Symbol.iterator] (): Iterator<this['args']['expressions'] extends (infer U)[] | undefined ? U : never> {
    if ('expressions' in (this.constructor as typeof Expression).argTypes) {
      if (Array.isArray(this.args.expressions)) {
        for (const e of this.args.expressions) {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          yield e as any;
        }
      }
      return;
    }
    throw new Error(`'${this.constructor.name}' object is not iterable`);
  }

  get this (): ExpressionValue {
    return this.args.this;
  }

  get expression (): ExpressionValue {
    return this.args.expression;
  }

  get expressions (): (Expression | string | number | boolean | Token)[] {
    const exprs = this.args.expressions;
    return Array.isArray(exprs)
      ? exprs
      : [];
  }

  /**
   * Extract text value from a named argument
   * @param key - The argument key to extract text from
   * @returns The text value, or empty string if not found
   */
  text (key: string): string {
    const field = (this.args as Record<string, ExpressionValue | ExpressionValueList>)[key];
    if (typeof field === 'string') {
      return field;
    }
    if (field instanceof IdentifierExpr || field instanceof LiteralExpr || field instanceof VarExpr) {
      return typeof field.this === 'string'
        ? field.this
        : '';
    }
    if (field instanceof StarExpr || field instanceof NullExpr) {
      return field.name;
    }
    return '';
  }

  /**
   * Check if this expression is a string literal
   * @returns True if string literal
   */
  get isString (): boolean {
    return this instanceof LiteralExpr && this.args.isString === true;
  }

  /**
   * Check if this expression is a number literal
   * @returns True if number literal
   */
  get isNumber (): boolean {
    return (this instanceof LiteralExpr && !this.args.isString)
      || (this instanceof NegExpr && (this.this as Expression).isNumber);
  }

  /**
   * Returns a JavaScript value equivalent of the SQL node
   * @throws Error if the expression cannot be converted
   */
  toValue (): ExpressionValue {
    if (this instanceof LiteralExpr) {
      const value = this.this;
      if (this.isString) {
        return value;
      }
      if (typeof value === 'string') {
        return Number(value);
      }
      return value;
    }
    throw new Error(`${this.constructor.name} cannot be converted to a JavaScript value.`);
  }

  /**
   * Check if this expression is an integer literal
   * @returns True if integer literal
   */
  get isInt (): boolean {
    if (!this.isNumber) {
      return false;
    }
    try {
      const value = this.toValue();
      return Number.isInteger(value);
    } catch {
      return false;
    }
  }

  /**
   * Check if this expression is a star (*) expression
   * @returns True if star expression
   */
  get isStar (): boolean {
    return this instanceof StarExpr
      || (this instanceof ColumnExpr && this.this instanceof StarExpr);
  }

  /**
   * Get the alias of this expression
   * @returns The alias name, or empty string if no alias
   */
  get alias (): string {
    if (this.args.alias instanceof TableAliasExpr) {
      return this.args.alias.name;
    }
    return this.text('alias');
  }

  /**
   * Get column names from table alias
   * @returns Array of column names
   */
  get aliasColumnNames (): string[] {
    const tableAlias = this.args.alias;
    if (!(tableAlias instanceof TableAliasExpr)) {
      return [];
    }
    const columns = tableAlias.args.columns;
    if (Array.isArray(columns)) {
      return columns.map((c: unknown) => (c instanceof Expression
        ? c.name
        : ''));
    }
    return [];
  }

  /**
   * Get the name of this expression (extracted from 'this' argument)
   * @returns The expression name
   */
  get name (): string {
    return this.text('this');
  }

  /**
   * Get the alias if present, otherwise the name
   * @returns Alias or name
   */
  get aliasOrName (): string {
    return this.alias || this.name;
  }

  /**
   * Get the output name (alias or name)
   * @returns The output name for this expression
   */
  get outputName (): string {
    return '';
  }

  /**
   * Get the data type of this expression
   * @returns DataType expression or undefined
   */
  get type (): DataTypeExpr | undefined {
    if (this instanceof CastExpr) {
      return this._type || (this.args.to as DataTypeExpr | undefined);
    }
    return this._type;
  }

  /**
   * Set the data type for this expression
   * @param dtype - Data type (string or DataTypeExpr)
   */
  set type (dtype: DataTypeExpr | DataTypeExprKind | undefined) {
    if (dtype && !(dtype instanceof DataTypeExpr)) {
      dtype = DataTypeExpr.build(dtype);
    }
    this._type = dtype as DataTypeExpr | undefined;
  }

  /**
   * Check if this expression has any of the specified data types
   * @param dtypes - Data type names to check
   * @returns True if expression has one of the specified types
   */
  isType (
    dtypes: (DataTypeExprKind | DataTypeExpr | IdentifierExpr | DotExpr)[],
    _options?: { checkNullable?: boolean },
  ): boolean {
    if (!this._type) {
      return false;
    }
    return this._type.isType(dtypes);
  }

  /**
   * Check if this expression is a leaf node (has no child expressions)
   * @returns True if this expression has no Expression or list children
   */
  get isLeaf (): boolean {
    return !Object.values(this.args).some((v) =>
      (v instanceof Expression || Array.isArray(v)) && v);
  }

  /**
   * Get metadata dictionary for this expression
   * @returns Metadata object
   */
  get meta (): Record<string, unknown> {
    if (!this._meta) {
      this._meta = {};
    }
    return this._meta;
  }

  copy (): this {
    const root = new (this.constructor as new () => this)();
    const stack: [Expression, Expression][] = [[this, root]];

    while (0 < stack.length) {
      const [node, copy] = stack.pop()!;
      if (node.comments) {
        copy.comments = [...node.comments];
      }
      if (node._type) {
        copy._type = node._type.copy() as DataTypeExpr;
      }
      if (node._meta) {
        copy._meta = { ...node._meta };
      }
      if (node._hash) {
        copy._hash = node._hash;
      }

      for (const [k, vs] of Object.entries(node.args)) {
        if (vs instanceof Expression) {
          const childCopy = new (vs.constructor as new () => Expression)();
          stack.push([vs, childCopy]);
          copy.setArgKey(k, childCopy);
        } else if (Array.isArray(vs)) {
          (copy.args as Record<string, ExpressionValue | ExpressionValueList>)[k] = [];
          for (const v of vs) {
            if (v instanceof Expression) {
              const childCopy = new (v.constructor as new () => Expression)();
              stack.push([v, childCopy]);
              copy.append(k, childCopy);
            } else {
              copy.append(k, v);
            }
          }
        } else {
          (copy.args as Record<string, ExpressionValue | ExpressionValueList>)[k] = vs;
        }
      }
    }
    return root;
  }

  /**
   * Add comments to this expression
   * @param comments - Array of comment strings to add
   * @param options
   * @param options.prepend - If true, prepend comments instead of appending
   */
  addComments (comments?: string[], options?: { prepend?: boolean }): void {
    const prepend = options?.prepend ?? false;
    if (!this.comments) {
      this.comments = [];
    }

    if (comments) {
      for (const comment of comments) {
        const [_, ...meta] = comment.split(SQLGLOT_META);

        if (0 < meta.length) {
          for (const kv of meta.join('').split(',')) {
            const [key, ...valueParts] = kv.split('=');
            const value = 0 < valueParts.length
              ? valueParts[0].trim()
              : true;
            this.meta[key.trim()] = toBool(value);
          }
        }

        if (!prepend) {
          this.comments.push(comment);
        }
      }

      if (prepend) {
        this.comments = [...comments, ...this.comments];
      }
    }
  }

  /**
   * Remove and return all comments from this expression
   * @returns Array of comment strings
   */
  popComments (): string[] {
    const comments = this.comments || [];
    this.comments = undefined;
    return comments;
  }

  /**
   * Appends value to arg_key if it's a list or sets it as a new list
   * @param argKey - Name of the list expression arg
   * @param value - Value to append to the list
   */
  append (argKey: string, value: ExpressionValue): void {
    const args = this.args as Record<string, ExpressionValue | ExpressionValueList>;
    if (!Array.isArray(args[argKey])) {
      args[argKey] = [];
    }
    this._setParent(argKey, value);
    const values = args[argKey] as unknown[];
    if (value instanceof Expression) {
      value.index = values.length;
    }
    values.push(value);
  }

  /**
   * Sets arg_key to value
   * @param argKey - Name of the expression arg
   * @param value - Value to set the arg to
   * @param index - If the arg is a list, this specifies what position to add the value in it
   * @param options
   * @param options.overwrite - If an index is given, determines whether to overwrite the list entry
   */
  setArgKey (
    argKey: string,
    value: ExpressionValue | ExpressionValueList,
    index?: number,
    options?: { overwrite?: boolean },
  ): void {
    const overwrite = options?.overwrite ?? true;
    // Clear hash cache up the tree
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let expression: Expression | undefined = this;
    while (expression && expression._hash !== undefined) {
      expression._hash = undefined;
      expression = expression.parent;
    }

    const args = this.args as Record<string, ExpressionValue | ExpressionValueList>;

    if (index !== undefined) {
      const expressions = (args[argKey] || []) as ExpressionValueList;

      if (expressions[index] === undefined) {
        return;
      }

      if (value === undefined) {
        expressions.splice(index, 1);
        for (let i = index; i < expressions.length; i++) {
          const v = expressions[i];
          if (v instanceof Expression && v.index !== undefined) {
            v.index = v.index - 1;
          }
        }
        return;
      }

      if (Array.isArray(value)) {
        expressions.splice(index, 1);
        expressions.splice(index, 0, ...value);
      } else if (overwrite) {
        expressions[index] = value;
      } else {
        expressions.splice(index, 0, value);
      }

      value = expressions;
    } else if (value === undefined) {
      delete args[argKey];
      return;
    }

    args[argKey] = value;
    this._setParent(argKey, value, index);
  }

  private _setParent (argKey: string, value: unknown, index?: number): void {
    if (value instanceof Expression) {
      value.parent = this;
      value.argKey = argKey;
      value.index = index;
    } else if (Array.isArray(value)) {
      value.forEach((v, i) => {
        if (v instanceof Expression) {
          v.parent = this;
          v.argKey = argKey;
          v.index = i;
        }
      });
    }
  }

  /**
   * Get the depth of this expression in the tree (distance from root)
   * @returns Depth level (0 = root)
   */
  get depth (): number {
    let depth = 0;
    let node: Expression | undefined = this.parent;
    while (node) {
      depth++;
      node = node.parent;
    }
    return depth;
  }

  * iterExpressions (options?: { reverse?: boolean }): Generator<Expression> {
    const reverse = options?.reverse ?? false;
    const argValues = reverse
      ? Object.values(this.args).reverse()
      : Object.values(this.args);
    for (const value of argValues) {
      if (value instanceof Expression) {
        yield value;
      } else if (Array.isArray(value)) {
        const items = reverse
          ? [...value].reverse()
          : value;
        for (const item of items) {
          if (item instanceof Expression) {
            yield item;
          }
        }
      }
    }
  }

  /**
   * Find the first expression of specified type(s) in the tree
   * @param expressionTypes - Array of expression class constructors to match
   * @param options - Options object
   * @param options.bfs - Use breadth-first search (default: true)
   * @returns First matching expression or undefined
   */
  find<T extends Expression>(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expressionTypes: (new (args: any) => T) | (new (args: any) => T)[],
    options?: { bfs?: boolean },
  ): T | undefined {
    for (const expr of this.findAll(expressionTypes, options)) {
      return expr;
    }
    return undefined;
  }

  /**
   * Returns a generator object which visits all nodes in this tree and only
   * yields those that match at least one of the specified expression types.
   *
   * @param expressionTypes - the expression type(s) to match
   * @param options - Options object
   * @param options.bfs - whether to search the AST using the BFS algorithm (DFS is used if false)
   * @returns The generator object
   */
  * findAll<T extends Expression>(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expressionTypes: (new (args: any) => T) | (new (args: any) => T)[],
    options?: { bfs?: boolean },
  ): Generator<T> {
    const types = ensureList(expressionTypes);

    const bfs = options?.bfs ?? true;
    for (const expression of this.walk({ bfs })) {
      if (types.some((type) => expression instanceof type)) {
        yield expression as T;
      }
    }
  }

  /**
   * Find the nearest ancestor expression of specified type(s)
   * @param expressionTypes - Array of expression class constructors to match
   * @returns First matching ancestor or undefined
   */
  findAncestor<T extends Expression>(
    ...expressionTypes: (new (...args: never[]) => T)[]
  ): T | undefined {
    let node: Expression | undefined = this.parent;
    while (node) {
      if (expressionTypes.some((type) => node instanceof type)) {
        return node as unknown as T;
      }
      node = node.parent;
    }
    return undefined;
  }

  /**
   * Returns the parent select statement.
   */
  get parentSelect (): SelectExpr | undefined {
    return this.findAncestor(SelectExpr);
  }

  /**
   * Returns if the parent is the same class as itself.
   */
  get sameParent (): boolean {
    return this.parent?.constructor === this.constructor;
  }

  sameParentAs (other: Expression): boolean {
    return this.parent === other.parent;
  }

  root (): Expression {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let node: Expression = this;
    while (node.parent) {
      node = node.parent;
    }
    return node;
  }

  /**
   * Generator that walks the expression tree using BFS or DFS.
   * @param options - Options object
   * @param options.bfs - whether to use breadth-first search (default: true), DFS if false
   * @param options.prune - optional function to determine if a node's children should be pruned
   * @returns Generator yielding all expressions in the tree
   */
  * walk (options?: { bfs?: boolean;
    prune?: (node: Expression) => boolean; }): Generator<Expression> {
    const bfs = options?.bfs ?? true;
    const prune = options?.prune;
    if (bfs) {
      yield* this.bfs({ prune });
    } else {
      yield* this.dfs({ prune });
    }
  }

  /**
   * Returns a generator object which visits all nodes in this tree in
   * the DFS (Depth-first) order.
   *
   * @param options - Options object
   * @param options.prune - optional function to determine if a node's children should be pruned
   * @returns The generator object
   */
  * dfs (options?: { prune?: (node: Expression) => boolean }): Generator<Expression> {
    const prune = options?.prune;
    const stack: Expression[] = [this];

    while (0 < stack.length) {
      const node = stack.pop()!;

      yield node;

      if (prune?.(node)) {
        continue;
      }

      for (const v of node.iterExpressions({ reverse: true })) {
        stack.push(v);
      }
    }
  }

  /**
   * Returns a generator object which visits all nodes in this tree in
   * the BFS (Breadth-first) order.
   *
   * @param options - Options object
   * @param options.prune - optional function to determine if a node's children should be pruned
   * @returns The generator object
   */
  * bfs (options?: { prune?: (node: Expression) => boolean }): Generator<Expression> {
    const prune = options?.prune;
    const queue: Expression[] = [this];

    while (0 < queue.length) {
      const node = queue.shift()!;

      yield node;

      if (prune?.(node)) {
        continue;
      }

      for (const v of node.iterExpressions()) {
        queue.push(v);
      }
    }
  }

  /**
   * Returns the first non-parenthesis child or self.
   */
  unnest (): Expression {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let expression: Expression = this;
    while (expression instanceof ParenExpr) {
      const thisArg = expression.this;
      if (thisArg instanceof Expression) {
        expression = thisArg;
      } else {
        break;
      }
    }
    return expression;
  }

  /**
   * Returns the inner expression if this is an Alias.
   */
  unalias (): Expression {
    if (this instanceof AliasExpr) {
      const thisArg = this.this;
      if (thisArg instanceof Expression) {
        return thisArg;
      }
    }
    return this;
  }

  /**
   * Returns unnested operands as a tuple.
   */
  unnestOperands (): Expression[] {
    return Array.from(this.iterExpressions()).map((arg) => arg.unnest());
  }

  /**
   * Returns a generator which yields child nodes whose parents are the same class.
   * A AND B AND C -> [A, B, C]
   *
   * @param options - Options object
   * @param options.unnest - whether to unwrap parentheses (default: true)
   */
  * flatten (options?: { unnest?: boolean }): Generator<Expression> {
    const unnest = options?.unnest ?? true;
    for (const node of this.dfs({ prune: (n) => n.parent !== undefined && n.constructor !== this.constructor })) {
      if (node.constructor !== this.constructor) {
        if (unnest && !(node instanceof SubqueryExpr)) {
          yield node.unnest();
        } else {
          yield node;
        }
      }
    }
  }

  sql (options: {
    dialect?: DialectType;
    [index: string]: unknown;
  } = {}): string {
    const {
      dialect, ...restOptions
    } = options;
    const dialectInstance = Dialect.getOrRaise(dialect);
    return dialectInstance.generate(this, restOptions);
  }

  /**
   * Visits all tree nodes (excluding already transformed ones)
   * and applies the given transformation function to each node.
   *
   * @param func - a function which takes a node and kwargs object, and returns a
   *               new transformed node or the same node without modifications. If the function
   *               returns undefined, then the corresponding node will be removed from the
   *               syntax tree.
   * @param options - Options object
   * @param options.copy - if set to true a new tree instance is constructed, otherwise the tree is
   *                       modified in place (default: true)
   * @returns The transformed tree
   */
  transform (
    func: (node: Expression, options: Record<string, unknown>) => Expression | undefined,
    options: {
      copy?: boolean;
      options?: Record<string, unknown>;
    } = {},
  ): Expression {
    const {
      copy = true, ...restOptions
    } = options;

    let root: Expression | undefined;
    let newNode: Expression | undefined;

    const startNode = copy
      ? this.copy()
      : this;

    for (const node of startNode.dfs({ prune: (n) => n !== newNode })) {
      const parent = node.parent;
      const argKey = node.argKey;
      const index = node.index;

      newNode = func(node, {
        ...restOptions,
        copy,
      });

      if (!root) {
        root = newNode;
      } else if (parent && argKey && newNode !== node) {
        parent.setArgKey(argKey, newNode, index);
      }
    }

    if (!root) {
      throw new Error('Transform failed: no root node');
    }

    return root.assertIs(Expression);
  }

  /**
   * Swap out this expression with a new expression.
   *
   * For example:
   *   const tree = new SelectExpr(...).select("x").from("tbl");
   *   tree.find([ColumnExpr]).replaceWith(new ColumnExpr({this: "y"}));
   *   tree.sql() // 'SELECT y FROM tbl'
   *
   * @param expression - new node (or undefined to remove)
   * @returns The new expression
   */
  replace<E extends Expression>(expression: E): E;
  replace<E extends Expression>(expression: E | undefined): E | undefined;
  replace (expression: undefined): undefined;
  replace<E extends Expression>(expression: E | undefined): E | undefined {
    const parent = this.parent;

    if (!parent || parent === expression) {
      return expression;
    }

    const key = this.argKey;
    if (!key) {
      return expression;
    }

    const value = (parent.args as Record<string, ExpressionValue | ExpressionValueList>)[key];

    if (Array.isArray(expression) && value instanceof Expression) {
      // We are trying to replace an Expression with a list, so it's assumed that
      // the intention was to really replace the parent of this expression.
      value.parent?.replace(expression);
    } else {
      parent.setArgKey(key, expression, this.index);
    }

    if (expression !== this as unknown) {
      this.parent = undefined;
      this.argKey = undefined;
      this.index = undefined;
    }

    return expression;
  }

  /**
   * Remove this expression from its AST.
   *
   * @returns The popped expression
   */
  pop (): this {
    this.replace(undefined);
    return this;
  }

  /**
   * Assert that this Expression is an instance of the specified type.
   *
   * If it is NOT an instance of type, this raises an assertion error.
   * Otherwise, this returns this expression.
   *
   * This is useful for type security in chained expressions:
   *
   * @example
   * ```typescript
   * parse_one("SELECT x from y").assertIs(SelectExpr).select("z").sql()
   * // 'SELECT x, z FROM y'
   * ```
   *
   * @param type - the class constructor to check against
   * @returns This expression, typed as the specified type
   */
  assertIs<T extends Expression>(type: new (...args: never[]) => T): T {
    if (!(this instanceof type)) {
      throw new Error(`${this.constructor.name} is not ${type.name}.`);
    }
    return this as T;
  }

  /**
   * Checks if this expression is valid (e.g. all mandatory args are set).
   *
   * @param args - a sequence of values that were used to instantiate a Func expression.
   *               This is used to check that the provided arguments don't exceed the function
   *               argument limit.
   * @returns A list of error messages for all possible errors that were found
   */
  errorMessages (args?: unknown[]): string[] {
    const errors: string[] = [];

    // Check for required arguments
    const constructor = this.constructor as typeof Expression;
    if (constructor.argTypes) {
      for (const key of constructor.requiredArgs()) {
        const v = (this.args as Record<string, ExpressionValue | ExpressionValueList>)[key];
        if (v === undefined || (Array.isArray(v) && v.length === 0)) {
          errors.push(`Required keyword: '${key}' missing for ${this.constructor.name}`);
        }
      }
    }

    // Check for too many arguments in Func expressions
    if (args && this instanceof FuncExpr) {
      const argTypeCount = constructor.argTypes
        ? Object.keys(constructor.argTypes).length
        : 0;
      // Check if this function accepts variable-length arguments
      // (e.g., CONCAT, COALESCE can take any number of arguments)
      const isVarLen = (constructor as typeof FuncExpr).isVarLenArgs || false;
      if (argTypeCount < args.length && !isVarLen) {
        errors.push(
          `The number of provided arguments (${args.length}) is greater than `
          + `the maximum number of supported arguments (${argTypeCount})`,
        );
      }
    }

    return errors;
  }

  // TODO: implement load and dump

  /**
   * AND this condition with one or multiple expressions.
   *
   * @example
   * ```typescript
   * condition("x=1").and([condition("y=1")]).sql()
   * // 'x = 1 AND y = 1'
   *
   * // Without copying or wrapping
   * condition("x=1").and([condition("y=1")], { copy: false, wrap: false })
   * ```
   *
   * @param expressions - The expressions to AND with this condition
   * @param options - Options object
   * @param options.dialect - The dialect to use for parsing
   * @param options.copy - Whether to copy the involved expressions (default: true)
   * @param options.wrap - Whether to wrap operands in Parens to avoid precedence issues (default:
   * true)
   * @returns The new AND condition
   */
  and (
    expressions: Expression | string | (Expression | string)[],
    options: {
      dialect?: DialectType;
      copy?: boolean;
      wrap?: boolean;
    } = {},
  ): ConditionExpr {
    const {
      copy = true, wrap = true, ...restOptions
    } = options;
    const expressionList = ensureList(expressions);
    return and([this, ...expressionList], {
      ...restOptions,
      copy,
      wrap,
    });
  }

  /**
   * OR this condition with one or multiple expressions.
   *
   * @example
   * ```typescript
   * condition("x=1").or([condition("y=1")]).sql()
   * // 'x = 1 OR y = 1'
   *
   * // Without copying or wrapping
   * condition("x=1").or([condition("y=1")], { copy: false, wrap: false })
   * ```
   *
   * @param expressions - The expressions to OR with this condition
   * @param options - Options object
   * @param options.dialect - The dialect to use for parsing
   * @param options.copy - Whether to copy the involved expressions (default: true)
   * @param options.wrap - Whether to wrap operands in Parens to avoid precedence issues (default:
   * true)
   * @returns The new OR condition
   */
  or (
    expressions: Expression | string | (Expression | string)[],
    options: {
      dialect?: DialectType;
      copy?: boolean;
      wrap?: boolean;
    } = {},
  ): ConditionExpr {
    const {
      copy = true, wrap = true, ...restOptions
    } = options;
    const expressionList = ensureList(expressions);
    return or([this, ...expressionList], {
      ...restOptions,
      copy,
      wrap,
    });
  }

  /**
   * Wrap this condition with NOT.
   *
   * @example
   * ```typescript
   * condition("x=1").negate().sql()
   * // 'NOT x = 1'
   *
   * // Without copying
   * condition("x=1").negate({ copy: false })
   * ```
   *
   * @param options - Options object
   * @param options.copy - Whether to copy this object (default: true)
   * @returns The new NOT instance
   */
  not (options: { copy?: boolean } = {}): NotExpr {
    const {
      copy = true, ...restOptions
    } = options;
    return not(this, {
      ...restOptions,
      copy,
    });
  }

  /**
   * Update this expression with positions from a token or other expression.
   *
   * @param other - a token or expression to update this expression with
   * @param positions - position values to use if other is not provided
   * @param positions.line - the line number
   * @param positions.col - column number
   * @param positions.start - start char index
   * @param positions.end - end char index
   * @returns The updated expression
   */
  updatePositions (
    other?: Token | Expression,
    positions?: {
      line?: number;
      col?: number;
      start?: number;
      end?: number;
    },
  ): this {
    if (!other) {
      this.meta.line = positions?.line;
      this.meta.col = positions?.col;
      this.meta.start = positions?.start;
      this.meta.end = positions?.end;
    } else if (other instanceof Expression) {
      for (const key of POSITION_META_KEYS) {
        this.meta[key] = other.meta[key];
      }
    } else {
      // Copy from token-like object
      this.meta.line = other.line;
      this.meta.col = other.col;
      this.meta.start = other.start;
      this.meta.end = other.end;
    }
    return this;
  }

  /**
   * Create an alias for this expression.
   *
   * @example
   * ```typescript
   * column("x").as("y")  // "x AS y"
   * column("x").as("y", { quoted: true })  // "x AS "y""
   * ```
   *
   * @param alias - the alias name (string or Identifier expression)
   * @param options - Options object
   * @param options.quoted - whether to quote the alias
   * @param options.dialect - the dialect to use for parsing
   * @param options.copy - whether to copy this expression (default: true)
   * @param options.wrap - whether to wrap in parentheses (default: true)
   * @returns The Alias expression
   */
  as (
    _alias: string | IdentifierExpr,
    options: {
      quoted?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      wrap?: boolean;
    } = {},
  ): AliasExpr {
    const {
      copy = true, ...restOptions
    } = options;
    const aliasName = typeof _alias === 'string'
      ? _alias
      : _alias.name;
    return alias(this, aliasName, {
      ...restOptions,
      copy,
    }) as AliasExpr; // NOTE: This is unsafe, needs verification
  }

  /**
   * Create a binary operation expression.
   * Internal helper for operator methods.
   *
   * @param klass - the binary expression class constructor
   * @param _other - the right-hand side operand
   * @param options - Options object
   * @param options.reverse - whether to reverse the operands (default: false)
   * @returns The binary expression
   */
  protected binop<T extends Expression>(
    klass: new (arg: {
      this: Expression;
      expression: Expression;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      [index: string]: any | undefined;
    }) => T,
    _other: unknown,
    options?: { reverse?: boolean },
  ): T {
    const reverse = options?.reverse ?? false;
    let self: Expression = this.copy();
    let other = convert(_other, true);
    if (!(self instanceof klass) && !(_other instanceof klass)) {
      const wrappedSelf = _wrap(self, BinaryExpr);
      const wrappedOther = _wrap(other, BinaryExpr);
      if (wrappedSelf) self = wrappedSelf;
      if (wrappedOther) other = wrappedOther;
    }
    if (reverse) {
      return new klass({
        this: other,
        expression: self,
      });
    }
    return new klass({
      this: self,
      expression: other,
    });
  }

  hash (): string {
    if (this._hash !== undefined) {
      return this._hash;
    }

    const nodes: Expression[] = [];
    const queue: Expression[] = [this];

    while (0 < queue.length) {
      const node = queue.shift()!;
      nodes.push(node);
      for (const child of node.iterExpressions()) {
        if (child._hash === undefined) {
          queue.push(child);
        }
      }
    }

    for (let i = nodes.length - 1; 0 <= i; i--) {
      const node = nodes[i];
      let hash = this._hashString(node.key);

      if (node instanceof LiteralExpr || node instanceof IdentifierExpr) {
        const sortedEntries = Object.entries(node.args).sort();
        for (const [k, v] of sortedEntries) {
          if (v) {
            hash = this._hashString(hash + k + v.toString());
          }
        }
      } else {
        const sortedEntries = Object.entries(node.args);
        for (const [k, v] of sortedEntries) {
          if (Array.isArray(v)) {
            for (const x of v) {
              if (x !== undefined && x !== false) {
                const hashValue = typeof x === 'string'
                  ? x.toLowerCase()
                  : x;
                hash = this._hashString(hash + k + hashValue);
              } else {
                hash = this._hashString(hash + k);
              }
            }
          } else if (v !== undefined && v !== false) {
            const hashValue = typeof v === 'string'
              ? v.toLowerCase()
              : v;
            hash = this._hashString(hash + k + hashValue);
          }
        }
      }
      node._hash = hash;
    }
    return this._hash || '';
  }

  equals (other: unknown): boolean {
    if (this === other) return true;
    if (!(other instanceof Expression)) return false;
    if (this.constructor !== other.constructor) return false;
    return this.hash() === other.hash();
  }

  private _hashString (str: string): string {
    const hash = createHash('sha256');
    hash.update(str);
    return hash.digest('hex');
  }

  toString (): string {
    return this.sql();
  }

  /**
   * Create an IN expression.
   *
   * @param expressions - The values to check against
   * @param options - Options object
   * @param options.query - Optional subquery expression
   * @param options.unnest - Optional unnest expression(s)
   * @param options.copy - Whether to copy this expression (default: true)
   * @returns The IN expression
   */
  in (
    expressions: unknown[],
    query?: string | Expression,
    options: {
      unnest?: string | Expression | (string | Expression)[];
      copy?: boolean;
    } = {},
  ): InExpr {
    const {
      copy = true, unnest, ...restOptions
    } = options;

    let subquery = query
      ? maybeParse(query)
      : undefined;

    // NOTE: The original sqlglot doesn't check that subquery is a QueryExpr. However, after a quick scan, only QueryExpr has a `subquery` method, so I added this check
    if (!(subquery instanceof SubqueryExpr) && subquery instanceof QueryExpr) {
      subquery = subquery.subquery(undefined, { copy: false });
    }

    return new InExpr({
      this: maybeCopy(this, copy),
      expressions: expressions.map((e) => convert(e, copy)),
      query: subquery,
      unnest: unnest
        ? new UnnestExpr({
          expressions: ensureList(unnest).map((e) => maybeParse(e, {
            ...restOptions,
            copy,
          })),
        })
        : undefined,
    });
  }

  /**
   * Create a BETWEEN expression.
   *
   * @param low - The lower bound
   * @param high - The upper bound
   * @param options - Options object
   * @param options.copy - Whether to copy this expression (default: true)
   * @param options.symmetric - Whether this is a symmetric between (optional)
   * @returns The BETWEEN expression
   */
  between (
    low: unknown,
    high: unknown,
    options: {
      copy?: boolean;
      symmetric?: boolean;
    } = {},
  ): BetweenExpr {
    const {
      copy = true, symmetric,
    } = options;

    const between = new BetweenExpr({
      this: maybeCopy(this, copy),
      low: convert(low, copy),
      high: convert(high, copy),
    });

    if (symmetric !== undefined) {
      between.setArgKey('symmetric', symmetric);
    }

    return between;
  }

  /**
   * Create an IS expression.
   */
  is (other: string | Expression): IsExpr {
    return this.binop(IsExpr, other);
  }

  /**
   * Create a LIKE expression.
   */
  like (other: string | Expression): LikeExpr {
    return this.binop(LikeExpr, other);
  }

  /**
   * Create an ILIKE expression.
   */
  ilike (other: string | Expression): ILikeExpr {
    return this.binop(ILikeExpr, other);
  }

  /**
   * Create an EQ (equals) expression.
   */
  eq (other: unknown): EQExpr {
    return this.binop(EQExpr, other);
  }

  /**
   * Create a NEQ (not equals) expression.
   */
  neq (other: unknown): NEQExpr {
    return this.binop(NEQExpr, other);
  }

  /**
   * Create a REGEXP_LIKE expression.
   */
  rlike (other: string | Expression): RegexpLikeExpr {
    return this.binop(RegexpLikeExpr, other);
  }

  /**
   * Create a DIV expression with optional typed and safe flags.
   */
  div (other: string | Expression, options?: { typed?: boolean;
    safe?: boolean; }): DivExpr {
    const div = this.binop(DivExpr, other);
    div.setArgKey('typed', options?.typed ?? false);
    div.setArgKey('safe', options?.safe ?? false);
    return div;
  }

  /**
   * Create an ascending ORDER BY expression.
   */
  asc (nullsFirst = true): OrderedExpr {
    return new OrderedExpr({
      this: this.copy(),
      nullsFirst: convert(nullsFirst, false),
    });
  }

  /**
   * Create a descending ORDER BY expression.
   */
  desc (nullsFirst = false): OrderedExpr {
    return new OrderedExpr({
      this: this.copy(),
      desc: convert(true, false),
      nullsFirst: convert(nullsFirst, false),
    });
  }

  // Comparison operators

  /**
   * Create an LT (less than) expression.
   */
  lt (other: unknown): LTExpr {
    return this.binop(LTExpr, other);
  }

  /**
   * Create an LTE (less than or equal) expression.
   */
  lte (other: unknown): LTEExpr {
    return this.binop(LTEExpr, other);
  }

  /**
   * Create a GT (greater than) expression.
   */
  gt (other: unknown): GTExpr {
    return this.binop(GTExpr, other);
  }

  /**
   * Create a GTE (greater than or equal) expression.
   */
  gte (other: unknown): GTEExpr {
    return this.binop(GTEExpr, other);
  }

  // Arithmetic operators

  /**
   * Create an ADD expression.
   */
  add (other: unknown): AddExpr {
    return this.binop(AddExpr, other);
  }

  /**
   * Create an ADD expression (reversed operands).
   */
  radd (other: unknown): AddExpr {
    return this.binop(AddExpr, other, { reverse: true });
  }

  /**
   * Create a SUB expression.
   */
  sub (other: unknown): SubExpr {
    return this.binop(SubExpr, other);
  }

  /**
   * Create a SUB expression (reversed operands).
   */
  rsub (other: unknown): SubExpr {
    return this.binop(SubExpr, other, { reverse: true });
  }

  /**
   * Create a MUL expression.
   */
  mul (other: unknown): MulExpr {
    return this.binop(MulExpr, other);
  }

  /**
   * Create a MUL expression (reversed operands).
   */
  rmul (other: unknown): MulExpr {
    return this.binop(MulExpr, other, { reverse: true });
  }

  /**
   * Create a DIV expression (reversed operands).
   */
  rdiv (other: unknown): DivExpr {
    return this.binop(DivExpr, other, { reverse: true });
  }

  /**
   * Create an INTDIV expression.
   */
  floorDiv (other: unknown): IntDivExpr {
    return this.binop(IntDivExpr, other);
  }

  /**
   * Create an INTDIV expression (reversed operands).
   */
  rfloorDiv (other: unknown): IntDivExpr {
    return this.binop(IntDivExpr, other, { reverse: true });
  }

  /**
   * Create a MOD expression.
   */
  mod (other: unknown): ModExpr {
    return this.binop(ModExpr, other);
  }

  /**
   * Create a MOD expression (reversed operands).
   */
  rmod (other: unknown): ModExpr {
    return this.binop(ModExpr, other, { reverse: true });
  }

  /**
   * Create a POW expression.
   */
  pow (other: unknown): PowExpr {
    return this.binop(PowExpr, other);
  }

  /**
   * Create a POW expression (reversed operands).
   */
  rpow (other: unknown): PowExpr {
    return this.binop(PowExpr, other, { reverse: true });
  }

  get _constructor (): typeof Expression {
    return this.constructor as typeof Expression;
  }
}

export type ConditionExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class ConditionExpr extends Expression {
  key = ExpressionKey.CONDITION;

  static argTypes: RequiredMap<ConditionExprArgs> = {
    ...super.argTypes,
  };

  declare args: ConditionExprArgs;

  constructor (args: ConditionExprArgs) {
    super(args);
  }
}

export type PredicateExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class PredicateExpr extends Expression {
  key = ExpressionKey.PREDICATE;

  static argTypes: RequiredMap<PredicateExprArgs> = {
    ...super.argTypes,
  };

  declare args: PredicateExprArgs;

  constructor (args: PredicateExprArgs) {
    super(args);
  }
}

export type DerivedTableExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class DerivedTableExpr extends Expression {
  key = ExpressionKey.DERIVED_TABLE;

  static argTypes: RequiredMap<DerivedTableExprArgs> = {
    ...super.argTypes,
  };

  declare args: DerivedTableExprArgs;

  constructor (args: DerivedTableExprArgs) {
    super(args);
  }

  /**
   * Gets the select expressions from the derived table.
   * Returns the select expressions if this is a QueryExpr, otherwise returns an empty array.
   *
   * @returns Array of Expression objects representing the SELECT clause expressions
   */
  get selects (): Expression[] {
    return this.this instanceof QueryExpr
      ? this.this.selects
      : [];
  }

  /**
   * Gets the output names of all select expressions in the derived table.
   * Maps each select expression to its output name (alias or column name).
   *
   * @returns Array of strings representing the names of the selected columns
   */
  get namedSelects (): string[] {
    return this.selects.map((s) => s.outputName);
  }
}

export type QueryExprArgs = Merge<[
  BaseExpressionArgs,
  { with?: WithExpr },
]>;

export class QueryExpr extends Expression {
  key = ExpressionKey.QUERY;

  static argTypes: RequiredMap<QueryExprArgs> = {
    ...super.argTypes,
  };

  declare args: QueryExprArgs;

  constructor (args: QueryExprArgs) {
    super(args);
  }

  /**
   * Returns a `Subquery` that wraps around this query.
   *
   * Example:
   *     const subquery = select().select("x").from("tbl").subquery();
   *     select().select("x").from(subquery).sql();
   *     // 'SELECT x FROM (SELECT x FROM tbl)'
   *
   * @param alias - An optional alias for the subquery (string or Expression)
   * @param options - Options object with `copy` property
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns A Subquery expression wrapping this query
   */
  subquery (
    alias?: string | Expression,
    options: {
      copy?: boolean;
      [index: string]: unknown;
    } = {},
  ): SubqueryExpr {
    const { copy = true } = options;
    const instance = maybeCopy(this, copy);
    let aliasExpr: TableAliasExpr | undefined;

    if (!(alias instanceof Expression)) {
      aliasExpr = new TableAliasExpr({
        this: alias
          ? toIdentifier(alias)
          : undefined,
      });
    }

    return new SubqueryExpr({
      this: instance,
      alias: aliasExpr,
    });
  }

  /**
   * Adds a LIMIT clause to this query.
   *
   * Example:
   *     select("1").union(select("1")).limit(1).sql();
   *     // 'SELECT 1 UNION SELECT 1 LIMIT 1'
   *
   * @param expression - The SQL code string to parse.
   *                     This can also be an integer.
   *                     If a `Limit` instance is passed, it will be used as-is.
   *                     If another `Expression` instance is passed, it will be wrapped in a
   *                     `Limit`.
   * @param options - Options object
   * @param options.dialect - The dialect used to parse the input expression
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns A limited query expression
   */
  limit (
    expression: string | number | Expression,
    options: {
      dialect?: DialectType;
      copy?: boolean;
      [index: string]: unknown;
    } = {},
  ): this {
    const {
      copy = true, ...restOptions
    } = options;
    return _applyBuilder(expression, {
      instance: this,
      arg: 'limit',
      into: LimitExpr,
      prefix: 'LIMIT',
      intoArg: 'expression',
      ...restOptions,
      copy,
    }) as this;
  }

  /**
   * Set the OFFSET expression.
   *
   * Example:
   *     select().from("tbl").select("x").offset(10).sql();
   *     // 'SELECT x FROM tbl OFFSET 10'
   *
   * @param expression - The SQL code string to parse.
   *                     This can also be an integer.
   *                     If a `Offset` instance is passed, this is used as-is.
   *                     If another `Expression` instance is passed, it will be wrapped in a
   *                     `Offset`.
   * @param options - Options object
   * @param options.dialect - The dialect used to parse the input expression
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns The modified query expression
   */
  offset (
    expression: string | number | Expression,
    options: {
      dialect?: DialectType;
      copy?: boolean;
      [index: string]: unknown;
    } = {},
  ): this {
    const {
      copy = true, ...restOptions
    } = options;
    return _applyBuilder(expression, {
      instance: this,
      arg: 'offset',
      into: OffsetExpr,
      prefix: 'OFFSET',
      intoArg: 'expression',
      ...restOptions,
      copy,
    }) as this;
  }

  /**
   * Set the ORDER BY expression.
   *
   * Example:
   *     select().from("tbl").select("x").orderBy("x DESC").sql();
   *     // 'SELECT x FROM tbl ORDER BY x DESC'
   *
   * @param expressions - The SQL code strings to parse.
   *                      If a `Group` instance is passed, this is used as-is.
   *                      If another `Expression` instance is passed, it will be wrapped in a
   *                      `Order`.
   * @param options - Options object
   * @param options.append - If `true`, add to any existing expressions. Otherwise, this flattens
   * all the `Order` expression into a single expression. Default is `true`.
   * @param options.dialect - The dialect used to parse the input expression
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns The modified query expression
   */
  orderBy (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [index: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, copy = true, ...restOptions
    } = options;
    const expressionList = ensureList(expressions);
    return _applyChildListBuilder(expressionList, {
      instance: this,
      arg: 'order',
      prefix: 'ORDER BY',
      into: OrderExpr,
      ...restOptions,
      append,
      copy,
    }) as this;
  }

  /**
   * Returns a list of all the CTEs attached to this query.
   *
   * @returns Array of CTE expressions
   */
  get ctes (): CTEExpr[] {
    const withExpr = this.args.with;
    return withExpr?.$expressions || []; // sqlglot uses `Expression.expressions`, but I used $expressions for type safety
  }

  /**
   * Returns the query's projections.
   * Subclasses must implement this property.
   *
   * @returns Array of Expression objects representing the SELECT clause projections
   */
  get selects (): Expression[] {
    throw new Error('Query objects must implement `selects`');
  }

  /**
   * Returns the output names of the query's projections.
   * Subclasses must implement this property.
   *
   * @returns Array of strings representing the names of the projected columns
   */
  get namedSelects (): string[] {
    throw new Error('Query objects must implement `namedSelects`');
  }

  /**
   * Append to or set the SELECT expressions.
   *
   * Example:
   *     select().select(["x", "y"]).sql();
   *     // 'SELECT x, y'
   *
   * @param expressions - The SQL code strings to parse.
   *                      If an `Expression` instance is passed, it will be used as-is.
   * @param options - Options object
   * @param options.append - If `true`, add to any existing expressions. Otherwise, this resets the
   * expressions. Default is `true`.
   * @param options.dialect - The dialect used to parse the input expressions
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns The modified query expression
   */
  select (
    _expressions?: string | Expression | (string | Expression | undefined)[],
    _options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [index: string]: unknown;
    } = {},
  ): this {
    throw new Error('Query objects must implement `select`');
  }

  /**
   * Append to or set the WHERE expressions.
   *
   * Examples:
   *     select().select(["x"]).from("tbl").where(["x = 'a' OR x < 'b'"]).sql();
   *     // "SELECT x FROM tbl WHERE x = 'a' OR x < 'b'"
   *
   * @param expressions - The SQL code strings to parse.
   *                      If an `Expression` instance is passed, it will be used as-is.
   *                      Multiple expressions are combined with an AND operator.
   * @param options - Options object
   * @param options.append - If `true`, AND the new expressions to any existing expression.
   * Otherwise, this resets the expression. Default is `true`.
   * @param options.dialect - The dialect used to parse the input expressions
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns The modified expression
   */
  where (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [index: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, copy = true, ...restOptions
    } = options;
    const processedExpressions = ensureList(expressions)
      .filter((expr): expr is string | Expression => typeof expr === 'string' || expr instanceof Expression)
      .map((expr): string | Expression =>
        expr instanceof WhereExpr
          ? expr.$this
          : expr);

    return _applyConjunctionBuilder(processedExpressions, {
      instance: this,
      arg: 'where',
      into: WhereExpr,
      ...restOptions,
      append,
      copy,
    }) as this;
  }

  /**
   * Append to or set the common table expressions.
   *
   * Example:
   *     select().with("tbl2", "SELECT * FROM tbl").select(["x"]).from("tbl2").sql();
   *     // 'WITH tbl2 AS (SELECT * FROM tbl) SELECT x FROM tbl2'
   *
   * @param alias - The SQL code string to parse as the table name.
   *                If an `Expression` instance is passed, this is used as-is.
   * @param as - The SQL code string to parse as the table expression.
   *             If an `Expression` instance is passed, it will be used as-is.
   * @param options - Options object
   * @param options.recursive - Set the RECURSIVE part of the expression. Defaults to `false`.
   * @param options.materialized - Set the MATERIALIZED part of the expression
   * @param options.append - If `true`, add to any existing expressions. Otherwise, this resets the
   * expressions. Default is `true`.
   * @param options.dialect - The dialect used to parse the input expression
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @param options.scalar - If `true`, this is a scalar common table expression
   * @returns The modified expression
   */
  with (
    alias: string | IdentifierExpr,
    as: string | QueryExpr,
    options: {
      recursive?: boolean;
      materialized?: boolean;
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      scalar?: boolean;
      [index: string]: unknown;
    } = {},
  ): this {
    const {
      recursive = false, append = true, copy = true, ...restOptions
    } = options;
    return _applyCteBuilder({
      instance: this,
      alias,
      as,
      ...restOptions,
      recursive,
      append,
      copy,
    }) as this;
  }

  /**
   * Builds a UNION expression.
   *
   * Example:
   *     select("1").union([select("1")]).sql();
   *     // 'SELECT 1 UNION SELECT 1'
   *
   * @param expressions - The SQL code strings to parse.
   *                      If an `Expression` instance is passed, it will be used as-is.
   * @param options - Options object
   * @param options.distinct - If `true`, uses UNION DISTINCT. Otherwise uses UNION ALL. Default is
   * `true`.
   * @param options.dialect - The dialect used to parse the input expressions
   * @returns A Union expression
   */
  union (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      distinct?: boolean;
      dialect?: DialectType;
      [index: string]: unknown;
    } = {},
  ): UnionExpr {
    return union([this, ...ensureList(expressions)], {
      ...options,
      distinct: options.distinct ?? true,
    });
  }

  /**
   * Builds an INTERSECT expression.
   *
   * Example:
   *     select("1").intersect([select("1")]).sql();
   *     // 'SELECT 1 INTERSECT SELECT 1'
   *
   * @param expressions - The SQL code strings to parse.
   *                      If an `Expression` instance is passed, it will be used as-is.
   * @param options - Options object
   * @param options.distinct - If `true`, uses INTERSECT DISTINCT. Otherwise uses INTERSECT ALL.
   * Default is `true`.
   * @param options.dialect - The dialect used to parse the input expressions
   * @returns An Intersect expression
   */
  intersect (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      distinct?: boolean;
      dialect?: DialectType;
      [index: string]: unknown;
    } = {},
  ): IntersectExpr {
    return intersect([this, ...ensureList(expressions)], {
      ...options,
      distinct: options.distinct ?? true,
    });
  }

  /**
   * Builds an EXCEPT expression.
   *
   * Example:
   *     select("1").except([select("2")]).sql();
   *     // 'SELECT 1 EXCEPT SELECT 2'
   *
   * @param expressions - The SQL code strings to parse.
   *                      If an `Expression` instance is passed, it will be used as-is.
   * @param options - Options object
   * @param options.distinct - If `true`, uses EXCEPT DISTINCT. Otherwise uses EXCEPT ALL. Default
   * is `true`.
   * @param options.dialect - The dialect used to parse the input expressions
   * @returns An Except expression
   */
  except (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      distinct?: boolean;
      dialect?: DialectType;
      [index: string]: unknown;
    } = {},
  ): ExceptExpr {
    return except([this, ...ensureList(expressions)], {
      ...options,
      distinct: options.distinct ?? true,
    });
  }

  // NOTE: sqlglot does not have this
  // However, I think this is a sensible assumption
  unnest (): QueryExpr {
    return super.unnest() as QueryExpr;
  }
}

export type UDTFExprArgs = Merge<[
  DerivedTableExprArgs,
  { alias?: TableAliasExpr },
]>;

export class UDTFExpr extends DerivedTableExpr {
  key = ExpressionKey.UDTF;

  static argTypes: RequiredMap<UDTFExprArgs> = {};

  declare args: UDTFExprArgs;

  constructor (args: UDTFExprArgs) {
    super(args);
  }

  get selects (): Expression[] {
    const alias = this.args.alias;
    return alias
      ? alias.columns
      : [];
  }

  get $alias (): TableAliasExpr | undefined {
    return this.args.alias;
  }
}

export type CacheExprArgs = Merge<[
  BaseExpressionArgs,
  {
    lazy?: Expression;
    options?: Expression[];
    this: Expression;
    expression?: ExpressionValue;
  },
]>;

export class CacheExpr extends Expression {
  key = ExpressionKey.CACHE;

  /**
   * Defines the arguments (properties and child expressions) for Cache expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<CacheExprArgs> = {
    ...super.argTypes,
    this: true,
    lazy: false,
    options: false,
    expression: false,
  };

  declare args: CacheExprArgs;

  constructor (args: CacheExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $lazy (): Expression | undefined {
    return this.args.lazy;
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }
}

export type UncacheExprArgs = Merge<[
  BaseExpressionArgs,
  {
    exists?: boolean;
    this: Expression;
  },
]>;

export class UncacheExpr extends Expression {
  key = ExpressionKey.UNCACHE;

  /**
   * Defines the arguments (properties and child expressions) for Uncache expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<UncacheExprArgs> = {
    ...super.argTypes,
    this: true,
    exists: false,
  };

  declare args: UncacheExprArgs;

  constructor (args: UncacheExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $exists (): boolean | undefined {
    return this.args.exists;
  }
}

/**
 * Enumeration of valid kind values for Refresh expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum RefreshExprKind {
  INCREMENTAL = 'INCREMENTAL',
  FULL = 'FULL',
}

export type RefreshExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind: RefreshExprKind;
    this: Expression;
  },
]>;

export class RefreshExpr extends Expression {
  key = ExpressionKey.REFRESH;

  /**
   * Defines the arguments (properties and child expressions) for Refresh expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<RefreshExprArgs> = {
    ...super.argTypes,
    this: true,
    kind: true,
  };

  declare args: RefreshExprArgs;

  constructor (args: RefreshExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): RefreshExprKind {
    return this.args.kind;
  }
}

export type DDLExprArgs = Merge<[
  BaseExpressionArgs,
  {
    with?: WithExpr; // NOTE: sqlglot does not have this, but based on usage, I added this
    expression?: SelectExpr; // NOTE: sqlglot does not have this, but based on usage, I added this
  },
]>;

export class DDLExpr extends Expression {
  key = ExpressionKey.DDL;

  static argTypes: RequiredMap<DDLExprArgs> = {
    ...super.argTypes,
    with: false,
    expression: false,
  };

  declare args: DDLExprArgs;

  constructor (args: DDLExprArgs) {
    super(args);
  }

  /**
   * Returns a list of all the CTEs attached to this statement.
   *
   * @returns Array of CTE expressions
   */
  get ctes (): CTEExpr[] {
    const withExpr = this.args.with;
    return withExpr?.$expressions || []; // NOTE: The original sqlglot uses `Expression.expressions`
  }

  /**
   * If this statement contains a query (e.g. a CTAS), this returns the query's projections.
   *
   * @returns Array of Expression objects representing the SELECT clause projections
   */
  get selects (): Expression[] {
    const expr = this.expression;
    return (expr instanceof QueryExpr)
      ? expr.selects
      : [];
  }

  /**
   * If this statement contains a query (e.g. a CTAS), this returns the output
   * names of the query's projections.
   *
   * @returns Array of strings representing the names of the projected columns
   */
  get namedSelects (): string[] {
    const expr = this.expression;
    return (expr instanceof QueryExpr)
      ? expr.namedSelects
      : [];
  }

  get $with (): WithExpr | undefined {
    return this.args.with;
  }
}

export type LockingStatementExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class LockingStatementExpr extends Expression {
  key = ExpressionKey.LOCKING_STATEMENT;

  static argTypes: RequiredMap<BaseExpressionArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: LockingStatementExprArgs;

  constructor (args: LockingStatementExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type DMLExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class DMLExpr extends Expression {
  key = ExpressionKey.DML;

  static argTypes: RequiredMap<DMLExprArgs> = {
    ...super.argTypes,
  };

  declare args: DMLExprArgs;

  constructor (args: DMLExprArgs) {
    super(args);
  }

  /**
   * Set the RETURNING expression. Not supported by all dialects.
   *
   * Example:
   *     delete("tbl").returning("*", { dialect: "postgres" }).sql();
   *     // 'DELETE FROM tbl RETURNING *'
   *
   * @param expression - The SQL code string to parse.
   *                     If an `Expression` instance is passed, it will be used as-is.
   * @param options - Options object
   * @param options.dialect - The dialect used to parse the input expression
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns The modified expression
   */
  returning (
    expression: string | Expression,
    options: {
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      copy = true, ...restOptions
    } = options;
    return _applyBuilder(expression, {
      instance: this,
      arg: 'returning',
      prefix: 'RETURNING',
      into: ReturningExpr,
      ...restOptions,
      copy,
    }) as this;
  }
}

/**
 * Enumeration of valid kind values for Create expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum CreateExprKind {
  TABLE = 'TABLE',
  VIEW = 'VIEW',
  INDEX = 'INDEX',
  SCHEMA = 'SCHEMA',
  DATABASE = 'DATABASE',
  FUNCTION = 'FUNCTION',
  PROCEDURE = 'PROCEDURE',
  TRIGGER = 'TRIGGER',
  SEQUENCE = 'SEQUENCE',
}

export type CreateExprArgs = Merge<[
  DDLExprArgs,
  {
    with?: WithExpr;
    kind: CreateExprKind;
    exists?: boolean;
    properties?: PropertiesExpr;
    replace?: boolean;
    refresh?: Expression;
    unique?: boolean;
    indexes?: Expression[];
    noSchemaBinding?: Expression;
    begin?: Expression;
    end?: Expression;
    clone?: Expression;
    concurrently?: Expression;
    clustered?: Expression;
    this: Expression;
    expression?: SelectExpr;
  },
]>;

export class CreateExpr extends DDLExpr {
  key = ExpressionKey.CREATE;

  /**
   * Defines the arguments (properties and child expressions) for Create expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<CreateExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
    with: false,
    kind: true,
    exists: false,
    properties: false,
    replace: false,
    refresh: false,
    unique: false,
    indexes: false,
    noSchemaBinding: false,
    begin: false,
    end: false,
    clone: false,
    concurrently: false,
    clustered: false,
  };

  declare args: CreateExprArgs;

  constructor (args: CreateExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $with (): WithExpr | undefined {
    return this.args.with;
  }

  get $kind (): CreateExprKind {
    return this.args.kind;
  }

  get kind (): CreateExprKind {
    return this.args.kind;
  }

  get $exists (): boolean | undefined {
    return this.args.exists;
  }

  get $properties (): PropertiesExpr | undefined {
    return this.args.properties;
  }

  get $replace (): boolean | undefined {
    return this.args.replace;
  }

  get $refresh (): Expression | undefined {
    return this.args.refresh;
  }

  get $unique (): boolean | undefined {
    return this.args.unique;
  }

  get $indexes (): Expression[] | undefined {
    return this.args.indexes;
  }

  get $noSchemaBinding (): Expression | undefined {
    return this.args.noSchemaBinding;
  }

  get $begin (): Expression | undefined {
    return this.args.begin;
  }

  get $end (): Expression | undefined {
    return this.args.end;
  }

  get $clone (): Expression | undefined {
    return this.args.clone;
  }

  get $concurrently (): Expression | undefined {
    return this.args.concurrently;
  }

  get $clustered (): Expression | undefined {
    return this.args.clustered;
  }
}

export type SequencePropertiesExprArgs = Merge<[
  BaseExpressionArgs,
  {
    increment?: Expression;
    minvalue?: string;
    maxvalue?: string;
    cache?: Expression;
    start?: Expression;
    owned?: Expression;
    options?: Expression[];
  },
]>;

export class SequencePropertiesExpr extends Expression {
  key = ExpressionKey.SEQUENCE_PROPERTIES;

  /**
   * Defines the arguments (properties and child expressions) for SequenceProperties expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<SequencePropertiesExprArgs> = {
    ...super.argTypes,
    increment: false,
    minvalue: false,
    maxvalue: false,
    cache: false,
    start: false,
    owned: false,
    options: false,
  };

  declare args: SequencePropertiesExprArgs;

  constructor (args: SequencePropertiesExprArgs) {
    super(args);
  }

  get $increment (): Expression | undefined {
    return this.args.increment;
  }

  get $minvalue (): string | undefined {
    return this.args.minvalue;
  }

  get $maxvalue (): string | undefined {
    return this.args.maxvalue;
  }

  get $cache (): Expression | undefined {
    return this.args.cache;
  }

  get $start (): Expression | undefined {
    return this.args.start;
  }

  get $owned (): Expression | undefined {
    return this.args.owned;
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }
}

export type TruncateTableExprArgs = Merge<[
  BaseExpressionArgs,
  {
    isDatabase?: string;
    exists?: boolean;
    only?: boolean;
    cluster?: Expression;
    identity?: Expression;
    option?: Expression;
    partition?: Expression;
    expressions: Expression[];
  },
]>;

export class TruncateTableExpr extends Expression {
  key = ExpressionKey.TRUNCATE_TABLE;

  /**
   * Defines the arguments (properties and child expressions) for TruncateTable expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<TruncateTableExprArgs> = {
    ...super.argTypes,
    expressions: true,
    isDatabase: false,
    exists: false,
    only: false,
    cluster: false,
    identity: false,
    option: false,
    partition: false,
  };

  declare args: TruncateTableExprArgs;

  constructor (args: TruncateTableExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $isDatabase (): string | undefined {
    return this.args.isDatabase;
  }

  get $exists (): boolean | undefined {
    return this.args.exists;
  }

  get $only (): boolean | undefined {
    return this.args.only;
  }

  get $cluster (): Expression | undefined {
    return this.args.cluster;
  }

  get $identity (): Expression | undefined {
    return this.args.identity;
  }

  get $option (): Expression | undefined {
    return this.args.option;
  }

  get $partition (): Expression | undefined {
    return this.args.partition;
  }
}

export type CloneExprArgs = Merge<[
  BaseExpressionArgs,
  {
    shallow?: Expression;
    copy?: unknown;
    this: Expression;
  },
]>;

export class CloneExpr extends Expression {
  key = ExpressionKey.CLONE;

  /**
   * Defines the arguments (properties and child expressions) for Clone expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<CloneExprArgs> = {
    ...super.argTypes,
    this: true,
    shallow: false,
    copy: false,
  };

  declare args: CloneExprArgs;

  constructor (args: CloneExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $shallow (): Expression | undefined {
    return this.args.shallow;
  }

  get $copy (): unknown {
    return this.args.copy;
  }
}

/**
 * Valid kind values for DESCRIBE statements
 */
export enum DescribeExprKind {
  TABLE = 'TABLE',
  VIEW = 'VIEW',
  SCHEMA = 'SCHEMA',
  FUNCTION = 'FUNCTION',
  PROCEDURE = 'PROCEDURE',
}
export type DescribeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    style?: Expression;
    kind?: DescribeExprKind;
    partition?: Expression;
    format?: string;
    asJson?: Expression;
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class DescribeExpr extends Expression {
  key = ExpressionKey.DESCRIBE;

  /**
   * Defines the arguments (properties and child expressions) for Describe expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<DescribeExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
    style: false,
    kind: false,
    partition: false,
    format: false,
    asJson: false,
  };

  declare args: DescribeExprArgs;

  constructor (args: DescribeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $style (): Expression | undefined {
    return this.args.style;
  }

  get $kind (): DescribeExprKind | undefined {
    return this.args.kind;
  }

  get $partition (): Expression | undefined {
    return this.args.partition;
  }

  get $format (): string | undefined {
    return this.args.format;
  }

  get $asJson (): Expression | undefined {
    return this.args.asJson;
  }
}

export type AttachExprArgs = Merge<[
  BaseExpressionArgs,
  {
    exists?: boolean;
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class AttachExpr extends Expression {
  key = ExpressionKey.ATTACH;

  /**
   * Defines the arguments (properties and child expressions) for Attach expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<AttachExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
    exists: false,
  };

  declare args: AttachExprArgs;

  constructor (args: AttachExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $exists (): boolean | undefined {
    return this.args.exists;
  }
}

export type DetachExprArgs = Merge<[
  BaseExpressionArgs,
  {
    exists?: boolean;
    this: Expression;
  },
]>;

export class DetachExpr extends Expression {
  key = ExpressionKey.DETACH;

  /**
   * Defines the arguments (properties and child expressions) for Detach expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<DetachExprArgs> = {
    ...super.argTypes,
    this: true,
    exists: false,
  };

  declare args: DetachExprArgs;

  constructor (args: DetachExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $exists (): boolean | undefined {
    return this.args.exists;
  }
}

export type InstallExprArgs = Merge<[
  BaseExpressionArgs,
  {
    from?: Expression;
    force?: Expression;
    this: Expression;
  },
]>;

export class InstallExpr extends Expression {
  key = ExpressionKey.INSTALL;

  /**
   * Defines the arguments (properties and child expressions) for Install expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<InstallExprArgs> = {
    ...super.argTypes,
    this: true,
    from: false,
    force: false,
  };

  declare args: InstallExprArgs;

  constructor (args: InstallExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $from (): Expression | undefined {
    return this.args.from;
  }

  get $force (): Expression | undefined {
    return this.args.force;
  }
}

export type SummarizeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    table?: Expression;
    this: Expression;
  },
]>;

export class SummarizeExpr extends Expression {
  key = ExpressionKey.SUMMARIZE;

  /**
   * Defines the arguments (properties and child expressions) for Summarize expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<SummarizeExprArgs> = {
    ...super.argTypes,
    this: true,
    table: false,
  };

  declare args: SummarizeExprArgs;

  constructor (args: SummarizeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $table (): Expression | undefined {
    return this.args.table;
  }
}

/**
 * Valid kind values for KILL statements
 */
export enum KillExprKind {
  CONNECTION = 'CONNECTION',
  QUERY = 'QUERY',
}

export type KillExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: KillExprKind;
    this: Expression;
  },
]>;

export class KillExpr extends Expression {
  key = ExpressionKey.KILL;

  /**
   * Defines the arguments (properties and child expressions) for Kill expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<KillExprArgs> = {
    ...super.argTypes,
    this: true,
    kind: false,
  };

  declare args: KillExprArgs;

  constructor (args: KillExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): KillExprKind | undefined {
    return this.args.kind;
  }
}

export type PragmaExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class PragmaExpr extends Expression {
  key = ExpressionKey.PRAGMA;

  static argTypes: RequiredMap<PragmaExprArgs> = {
    ...super.argTypes,
  };

  declare args: PragmaExprArgs;

  constructor (args: PragmaExprArgs) {
    super(args);
  }
}

export type DeclareExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions: Expression[] },
]>;

export class DeclareExpr extends Expression {
  key = ExpressionKey.DECLARE;

  static argTypes: RequiredMap<DeclareExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: DeclareExprArgs;

  constructor (args: DeclareExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

/**
 * Valid kind values for DECLARE items
 */
export enum DeclareItemExprKind {
  CURSOR = 'CURSOR',
  VARIABLE = 'VARIABLE',
  TABLE = 'TABLE',
  CONSTANT = 'CONSTANT',
}

export type DeclareItemExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: DeclareItemExprKind;
    default?: Expression;
    this: Expression;
  },
]>;

export class DeclareItemExpr extends Expression {
  key = ExpressionKey.DECLARE_ITEM;

  /**
   * Defines the arguments (properties and child expressions) for DeclareItem expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<DeclareItemExprArgs> = {
    ...super.argTypes,
    this: true,
    kind: false,
    default: false,
  };

  declare args: DeclareItemExprArgs;

  constructor (args: DeclareItemExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): DeclareItemExprKind | undefined {
    return this.args.kind;
  }

  get $default (): Expression | undefined {
    return this.args.default;
  }
}

export type SetExprArgs = Merge<[
  BaseExpressionArgs,
  {
    unset?: Expression;
    tag?: Expression;
    expressions?: Expression[];
  },
]>;

export class SetExpr extends Expression {
  key = ExpressionKey.SET;

  /**
   * Defines the arguments (properties and child expressions) for Set expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<SetExprArgs> = {
    ...super.argTypes,
    expressions: false,
    unset: false,
    tag: false,
  };

  declare args: SetExprArgs;

  constructor (args: SetExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $unset (): Expression | undefined {
    return this.args.unset;
  }

  get $tag (): Expression | undefined {
    return this.args.tag;
  }
}

export type HeredocExprArgs = Merge<[
  BaseExpressionArgs,
  {
    tag?: Expression;
    this: Expression;
  },
]>;

export class HeredocExpr extends Expression {
  key = ExpressionKey.HEREDOC;

  /**
   * Defines the arguments (properties and child expressions) for Heredoc expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<HeredocExprArgs> = {
    ...super.argTypes,
    this: true,
    tag: false,
  };

  declare args: HeredocExprArgs;

  constructor (args: HeredocExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $tag (): Expression | undefined {
    return this.args.tag;
  }
}

/**
 * Valid kind values for SET items
 */
export enum SetItemExprKind {
  SESSION = 'SESSION',
  GLOBAL = 'GLOBAL',
  LOCAL = 'LOCAL',
  PERSIST = 'PERSIST',
  PERSIST_ONLY = 'PERSIST_ONLY',
}

export type SetItemExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: SetItemExprKind;
    collate?: string;
    global?: boolean;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class SetItemExpr extends Expression {
  key = ExpressionKey.SET_ITEM;

  /**
   * Defines the arguments (properties and child expressions) for SetItem expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<SetItemExprArgs> = {
    ...super.argTypes,
    this: false,
    expressions: false,
    kind: false,
    collate: false,
    global: false,
  };

  declare args: SetItemExprArgs;

  constructor (args: SetItemExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $kind (): SetItemExprKind | undefined {
    return this.args.kind;
  }

  get $collate (): string | undefined {
    return this.args.collate;
  }

  get $global (): boolean | undefined {
    return this.args.global;
  }
}

export type QueryBandExprArgs = Merge<[
  BaseExpressionArgs,
  {
    scope?: Expression;
    update?: Expression;
    this: Expression;
  },
]>;

export class QueryBandExpr extends Expression {
  key = ExpressionKey.QUERY_BAND;

  /**
   * Defines the arguments (properties and child expressions) for QueryBand expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<QueryBandExprArgs> = {
    ...super.argTypes,
    this: true,
    scope: false,
    update: false,
  };

  declare args: QueryBandExprArgs;

  constructor (args: QueryBandExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $scope (): Expression | undefined {
    return this.args.scope;
  }

  get $update (): Expression | undefined {
    return this.args.update;
  }
}

export type ShowExprArgs = Merge<[
  BaseExpressionArgs,
  {
    history?: Expression;
    terse?: Expression;
    target?: Expression;
    offset?: boolean;
    startsWith?: Expression;
    limit?: number | Expression;
    from?: Expression;
    like?: Expression;
    where?: Expression;
    db?: string;
    scope?: Expression;
    scopeKind?: string;
    full?: Expression;
    mutex?: Expression;
    query?: Expression;
    channel?: Expression;
    global?: boolean;
    log?: Expression;
    position?: Expression;
    types?: Expression[];
    privileges?: Expression[];
    forTable?: Expression;
    forGroup?: Expression;
    forUser?: Expression;
    forRole?: Expression;
    intoOutfile?: Expression;
    json?: Expression;
    this: Expression;
  },
]>;

export class ShowExpr extends Expression {
  key = ExpressionKey.SHOW;

  /**
   * Defines the arguments (properties and child expressions) for Show expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ShowExprArgs> = {
    ...super.argTypes,
    this: true,
    history: false,
    terse: false,
    target: false,
    offset: false,
    startsWith: false,
    limit: false,
    from: false,
    like: false,
    where: false,
    db: false,
    scope: false,
    scopeKind: false,
    full: false,
    mutex: false,
    query: false,
    channel: false,
    global: false,
    log: false,
    position: false,
    types: false,
    privileges: false,
    forTable: false,
    forGroup: false,
    forUser: false,
    forRole: false,
    intoOutfile: false,
    json: false,
  };

  declare args: ShowExprArgs;

  constructor (args: ShowExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $history (): Expression | undefined {
    return this.args.history;
  }

  get $terse (): Expression | undefined {
    return this.args.terse;
  }

  get $target (): Expression | undefined {
    return this.args.target;
  }

  get $offset (): boolean | undefined {
    return this.args.offset;
  }

  get $startsWith (): Expression | undefined {
    return this.args.startsWith;
  }

  get $limit (): number | Expression | undefined {
    return this.args.limit;
  }

  get $from (): Expression | undefined {
    return this.args.from;
  }

  get $like (): Expression | undefined {
    return this.args.like;
  }

  get $where (): Expression | undefined {
    return this.args.where;
  }

  get $db (): string | undefined {
    return this.args.db;
  }

  get $scope (): Expression | undefined {
    return this.args.scope;
  }

  get $scopeKind (): string | undefined {
    return this.args.scopeKind;
  }

  get $full (): Expression | undefined {
    return this.args.full;
  }

  get $mutex (): Expression | undefined {
    return this.args.mutex;
  }

  get $query (): Expression | undefined {
    return this.args.query;
  }

  get $channel (): Expression | undefined {
    return this.args.channel;
  }

  get $global (): boolean | undefined {
    return this.args.global;
  }

  get $log (): Expression | undefined {
    return this.args.log;
  }

  get $position (): Expression | undefined {
    return this.args.position;
  }

  get $types (): Expression[] | undefined {
    return this.args.types;
  }

  get $privileges (): Expression[] | undefined {
    return this.args.privileges;
  }

  get $forTable (): Expression | undefined {
    return this.args.forTable;
  }

  get $forGroup (): Expression | undefined {
    return this.args.forGroup;
  }

  get $forUser (): Expression | undefined {
    return this.args.forUser;
  }

  get $forRole (): Expression | undefined {
    return this.args.forRole;
  }

  get $intoOutfile (): Expression | undefined {
    return this.args.intoOutfile;
  }

  get $json (): Expression | undefined {
    return this.args.json;
  }
}

export type UserDefinedFunctionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    wrapped?: Expression;
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class UserDefinedFunctionExpr extends Expression {
  key = ExpressionKey.USER_DEFINED_FUNCTION;

  /**
   * Defines the arguments (properties and child expressions) for UserDefinedFunction expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<UserDefinedFunctionExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
    wrapped: false,
  };

  declare args: UserDefinedFunctionExprArgs;

  constructor (args: UserDefinedFunctionExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $wrapped (): Expression | undefined {
    return this.args.wrapped;
  }
}

export type CharacterSetExprArgs = Merge<[
  BaseExpressionArgs,
  {
    default?: Expression;
    this: Expression;
  },
]>;

export class CharacterSetExpr extends Expression {
  key = ExpressionKey.CHARACTER_SET;

  /**
   * Defines the arguments (properties and child expressions) for CharacterSet expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<CharacterSetExprArgs> = {
    ...super.argTypes,
    this: true,
    default: false,
  };

  declare args: CharacterSetExprArgs;

  constructor (args: CharacterSetExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $default (): Expression | undefined {
    return this.args.default;
  }
}

/**
 * Enumeration of valid kind values for RecursiveWithSearch expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum RecursiveWithSearchExprKind {
  BREADTH = 'BREADTH',
  DEPTH = 'DEPTH',
}

export type RecursiveWithSearchExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind: RecursiveWithSearchExprKind;
    using?: string;
    this: Expression;
    expression: Expression;
  },
]>;

export class RecursiveWithSearchExpr extends Expression {
  key = ExpressionKey.RECURSIVE_WITH_SEARCH;

  /**
   * Defines the arguments (properties and child expressions) for RecursiveWithSearch expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<RecursiveWithSearchExprArgs> = {
    ...super.argTypes,
    kind: true,
    this: true,
    expression: true,
    using: false,
  };

  declare args: RecursiveWithSearchExprArgs;

  constructor (args: RecursiveWithSearchExprArgs) {
    super(args);
  }

  get $kind (): RecursiveWithSearchExprKind {
    return this.args.kind;
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $using (): string | undefined {
    return this.args.using;
  }
}

export type WithExprArgs = Merge<[
  BaseExpressionArgs,
  {
    recursive?: boolean;
    search?: Expression;
    expressions: CTEExpr[];
  },
]>;

export class WithExpr extends Expression {
  key = ExpressionKey.WITH;

  /**
   * Defines the arguments (properties and child expressions) for With expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<WithExprArgs> = {
    ...super.argTypes,
    expressions: true,
    recursive: false,
    search: false,
  };

  declare args: WithExprArgs;

  constructor (args: WithExprArgs) {
    super(args);
  }

  get $expressions (): CTEExpr[] {
    return this.args.expressions;
  }

  get $recursive (): boolean | undefined {
    return this.args.recursive;
  }

  get $search (): Expression | undefined {
    return this.args.search;
  }

  get recursive (): boolean | undefined {
    return this.args.recursive;
  }
}

export type WithinGroupExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;
export class WithinGroupExpr extends Expression {
  key = ExpressionKey.WITHIN_GROUP;

  /**
   * Defines the arguments (properties and child expressions) for WithinGroup expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<BaseExpressionArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: WithinGroupExprArgs;

  constructor (args: WithinGroupExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }
}

export type ProjectionDefExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;
export class ProjectionDefExpr extends Expression {
  key = ExpressionKey.PROJECTION_DEF;

  /**
   * Defines the arguments (properties and child expressions) for ProjectionDef expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ProjectionDefExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: ProjectionDefExprArgs;

  constructor (args: ProjectionDefExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type TableAliasExprArgs = Merge<[
  BaseExpressionArgs,
  {
    columns?: Expression[];
    this?: IdentifierExpr;
  },
]>;

export class TableAliasExpr extends Expression {
  key = ExpressionKey.TABLE_ALIAS;

  /**
   * Defines the arguments (properties and child expressions) for TableAlias expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<TableAliasExprArgs> = {
    ...super.argTypes,
    this: false,
    columns: false,
  };

  declare args: TableAliasExprArgs;

  constructor (args: TableAliasExprArgs) {
    super(args);
  }

  get $this (): IdentifierExpr | undefined {
    return this.args.this;
  }

  get $columns (): Expression[] | undefined {
    return this.args.columns;
  }

  get columns (): Expression[] {
    return this.args.columns || [];
  }
}

export type ColumnPositionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    position: Expression;
    this?: Expression;
  },
]>;

export class ColumnPositionExpr extends Expression {
  key = ExpressionKey.COLUMN_POSITION;

  /**
   * Defines the arguments (properties and child expressions) for ColumnPosition expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ColumnPositionExprArgs> = {
    ...super.argTypes,
    this: false,
    position: true,
  };

  declare args: ColumnPositionExprArgs;

  constructor (args: ColumnPositionExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $position (): Expression {
    return this.args.position;
  }
}

/**
 * Enumeration of valid kind values for ColumnDef expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum ColumnDefExprKind {
  GENERATED = 'GENERATED',
  STORED = 'STORED',
  VIRTUAL = 'VIRTUAL',
  DEFAULT = 'DEFAULT',
}

export type ColumnDefExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: ColumnDefExprKind;
    constraints?: ColumnConstraintExpr[];
    exists?: boolean;
    position?: Expression;
    default?: Expression;
    output?: Expression;
    this: Expression;
  },
]>;

export class ColumnDefExpr extends Expression {
  key = ExpressionKey.COLUMN_DEF;

  /**
   * Defines the arguments (properties and child expressions) for ColumnDef expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ColumnDefExprArgs> = {
    ...super.argTypes,
    this: true,
    kind: false,
    constraints: false,
    exists: false,
    position: false,
    default: false,
    output: false,
  };

  declare args: ColumnDefExprArgs;

  constructor (args: ColumnDefExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): ColumnDefExprKind | undefined {
    return this.args.kind;
  }

  get $constraints (): Expression[] | undefined {
    return this.args.constraints;
  }

  get $exists (): boolean | undefined {
    return this.args.exists;
  }

  get $position (): Expression | undefined {
    return this.args.position;
  }

  get $default (): Expression | undefined {
    return this.args.default;
  }

  get $output (): Expression | undefined {
    return this.args.output;
  }

  /**
   * Gets the data type of the column definition
   * @returns The DataType expression or undefined
   */
  get kind (): ColumnDefExprKind | undefined {
    return this.args.kind;
  }

  /**
   * Gets the column constraints
   * @returns Array of ColumnConstraint expressions
   */
  get constraints (): ColumnConstraintExpr[] {
    return this.args.constraints || [];
  }
}

export type AlterColumnExprArgs = Merge<[
  BaseExpressionArgs,
  {
    dtype?: DataTypeExpr;
    collate?: string;
    using?: string;
    default?: Expression;
    drop?: Expression;
    comment?: string;
    allowNull?: Expression;
    visible?: Expression;
    renameTo?: string;
    this: Expression;
  },
]>;

export class AlterColumnExpr extends Expression {
  key = ExpressionKey.ALTER_COLUMN;

  /**
   * Defines the arguments (properties and child expressions) for AlterColumn expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<AlterColumnExprArgs> = {
    ...super.argTypes,
    this: true,
    dtype: false,
    collate: false,
    using: false,
    default: false,
    drop: false,
    comment: false,
    allowNull: false,
    visible: false,
    renameTo: false,
  };

  declare args: AlterColumnExprArgs;

  constructor (args: AlterColumnExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $dtype (): DataTypeExpr | undefined {
    return this.args.dtype;
  }

  get $collate (): string | undefined {
    return this.args.collate;
  }

  get $using (): string | undefined {
    return this.args.using;
  }

  get $default (): Expression | undefined {
    return this.args.default;
  }

  get $drop (): Expression | undefined {
    return this.args.drop;
  }

  get $comment (): string | undefined {
    return this.args.comment;
  }

  get $allowNull (): Expression | undefined {
    return this.args.allowNull;
  }

  get $visible (): Expression | undefined {
    return this.args.visible;
  }

  get $renameTo (): string | undefined {
    return this.args.renameTo;
  }
}

export type AlterIndexExprArgs = Merge<[
  BaseExpressionArgs,
  {
    visible: Expression;
    this: Expression;
  },
]>;

export class AlterIndexExpr extends Expression {
  key = ExpressionKey.ALTER_INDEX;

  /**
   * Defines the arguments (properties and child expressions) for AlterIndex expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<AlterIndexExprArgs> = {
    ...super.argTypes,
    this: true,
    visible: true,
  };

  declare args: AlterIndexExprArgs;

  constructor (args: AlterIndexExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $visible (): Expression {
    return this.args.visible;
  }
}

export type AlterDistStyleExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class AlterDistStyleExpr extends Expression {
  key = ExpressionKey.ALTER_DIST_STYLE;

  static argTypes: RequiredMap<AlterDistStyleExprArgs> = {
    ...super.argTypes,
  };

  declare args: AlterDistStyleExprArgs;

  constructor (args: AlterDistStyleExprArgs) {
    super(args);
  }
}

export type AlterSortKeyExprArgs = Merge<[
  BaseExpressionArgs,
  {
    compound?: Expression;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class AlterSortKeyExpr extends Expression {
  key = ExpressionKey.ALTER_SORT_KEY;

  /**
   * Defines the arguments (properties and child expressions) for AlterSortKey expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<AlterSortKeyExprArgs> = {
    ...super.argTypes,
    this: false,
    expressions: false,
    compound: false,
  };

  declare args: AlterSortKeyExprArgs;

  constructor (args: AlterSortKeyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $compound (): Expression | undefined {
    return this.args.compound;
  }
}

export type AlterSetExprArgs = Merge<[
  BaseExpressionArgs,
  {
    option?: Expression;
    tablespace?: Expression;
    accessMethod?: string;
    fileFormat?: string;
    copyOptions?: Expression[];
    tag?: Expression;
    location?: Expression;
    serde?: Expression;
    expressions?: Expression[];
  },
]>;

export class AlterSetExpr extends Expression {
  key = ExpressionKey.ALTER_SET;

  /**
   * Defines the arguments (properties and child expressions) for AlterSet expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<AlterSetExprArgs> = {
    ...super.argTypes,
    expressions: false,
    option: false,
    tablespace: false,
    accessMethod: false,
    fileFormat: false,
    copyOptions: false,
    tag: false,
    location: false,
    serde: false,
  };

  declare args: AlterSetExprArgs;

  constructor (args: AlterSetExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $option (): Expression | undefined {
    return this.args.option;
  }

  get $tablespace (): Expression | undefined {
    return this.args.tablespace;
  }

  get $accessMethod (): string | undefined {
    return this.args.accessMethod;
  }

  get $fileFormat (): string | undefined {
    return this.args.fileFormat;
  }

  get $copyOptions (): Expression[] | undefined {
    return this.args.copyOptions;
  }

  get $tag (): Expression | undefined {
    return this.args.tag;
  }

  get $location (): Expression | undefined {
    return this.args.location;
  }

  get $serde (): Expression | undefined {
    return this.args.serde;
  }
}

export type RenameColumnExprArgs = Merge<[
  BaseExpressionArgs,
  {
    to: Expression;
    exists?: boolean;
    this: Expression;
  },
]>;

export class RenameColumnExpr extends Expression {
  key = ExpressionKey.RENAME_COLUMN;

  /**
   * Defines the arguments (properties and child expressions) for RenameColumn expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<RenameColumnExprArgs> = {
    ...super.argTypes,
    this: true,
    to: true,
    exists: false,
  };

  declare args: RenameColumnExprArgs;

  constructor (args: RenameColumnExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $to (): Expression {
    return this.args.to;
  }

  get $exists (): boolean | undefined {
    return this.args.exists;
  }
}

export type AlterRenameExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class AlterRenameExpr extends Expression {
  key = ExpressionKey.ALTER_RENAME;

  static argTypes: RequiredMap<AlterRenameExprArgs> = {
    ...super.argTypes,
  };

  declare args: AlterRenameExprArgs;

  constructor (args: AlterRenameExprArgs) {
    super(args);
  }
}

export type SwapTableExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class SwapTableExpr extends Expression {
  key = ExpressionKey.SWAP_TABLE;

  static argTypes: RequiredMap<SwapTableExprArgs> = {
    ...super.argTypes,
  };

  declare args: SwapTableExprArgs;

  constructor (args: SwapTableExprArgs) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for Comment expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum CommentExprKind {
  TABLE = 'TABLE',
  COLUMN = 'COLUMN',
  VIEW = 'VIEW',
}

export type CommentExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind: CommentExprKind;
    exists?: boolean;
    materialized?: boolean;
    this: Expression;
    expression: Expression;
  },
]>;

export class CommentExpr extends Expression {
  key = ExpressionKey.COMMENT;

  /**
   * Defines the arguments (properties and child expressions) for Comment expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<CommentExprArgs> = {
    ...super.argTypes,
    this: true,
    kind: true,
    expression: true,
    exists: false,
    materialized: false,
  };

  declare args: CommentExprArgs;

  constructor (args: CommentExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): CommentExprKind {
    return this.args.kind;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $exists (): boolean | undefined {
    return this.args.exists;
  }

  get $materialized (): boolean | undefined {
    return this.args.materialized;
  }
}

export type ComprehensionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    position?: Expression;
    iterator: Expression;
    condition?: Expression;
    this: Expression;
    expression: Expression;
  },
]>;

export class ComprehensionExpr extends Expression {
  key = ExpressionKey.COMPREHENSION;

  /**
   * Defines the arguments (properties and child expressions) for Comprehension expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ComprehensionExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    position: false,
    iterator: true,
    condition: false,
  };

  declare args: ComprehensionExprArgs;

  constructor (args: ComprehensionExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $position (): Expression | undefined {
    return this.args.position;
  }

  get $iterator (): Expression {
    return this.args.iterator;
  }

  get $condition (): Expression | undefined {
    return this.args.condition;
  }
}

export type MergeTreeTTLActionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    delete?: Expression;
    recompress?: Expression[];
    toDisk?: Expression;
    toVolume?: Expression;
    this: Expression;
  },
]>;

export class MergeTreeTTLActionExpr extends Expression {
  key = ExpressionKey.MERGE_TREE_TTL_ACTION;

  /**
   * Defines the arguments (properties and child expressions) for MergeTreeTTLAction expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<MergeTreeTTLActionExprArgs> = {
    ...super.argTypes,
    this: true,
    delete: false,
    recompress: false,
    toDisk: false,
    toVolume: false,
  };

  declare args: MergeTreeTTLActionExprArgs;

  constructor (args: MergeTreeTTLActionExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $delete (): Expression | undefined {
    return this.args.delete;
  }

  get $recompress (): Expression[] | undefined {
    return this.args.recompress;
  }

  get $toDisk (): Expression | undefined {
    return this.args.toDisk;
  }

  get $toVolume (): Expression | undefined {
    return this.args.toVolume;
  }
}

export type MergeTreeTTLExprArgs = Merge<[
  BaseExpressionArgs,
  {
    where?: Expression;
    group?: Expression;
    aggregates?: Expression[];
    expressions: Expression[];
  },
]>;

export class MergeTreeTTLExpr extends Expression {
  key = ExpressionKey.MERGE_TREE_TTL;

  /**
   * Defines the arguments (properties and child expressions) for MergeTreeTTL expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<MergeTreeTTLExprArgs> = {
    ...super.argTypes,
    expressions: true,
    where: false,
    group: false,
    aggregates: false,
  };

  declare args: MergeTreeTTLExprArgs;

  constructor (args: MergeTreeTTLExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $where (): Expression | undefined {
    return this.args.where;
  }

  get $group (): Expression | undefined {
    return this.args.group;
  }

  get $aggregates (): Expression[] | undefined {
    return this.args.aggregates;
  }
}

export type IndexConstraintOptionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    keyBlockSize?: number | Expression;
    using?: string;
    parser?: Expression;
    comment?: string;
    visible?: Expression;
    engineAttr?: string;
    secondaryEngineAttr?: string;
  },
]>;

export class IndexConstraintOptionExpr extends Expression {
  key = ExpressionKey.INDEX_CONSTRAINT_OPTION;

  /**
   * Defines the arguments (properties and child expressions) for IndexConstraintOption expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<IndexConstraintOptionExprArgs> = {
    ...super.argTypes,
    keyBlockSize: false,
    using: false,
    parser: false,
    comment: false,
    visible: false,
    engineAttr: false,
    secondaryEngineAttr: false,
  };

  declare args: IndexConstraintOptionExprArgs;

  constructor (args: IndexConstraintOptionExprArgs) {
    super(args);
  }

  get $keyBlockSize (): number | Expression | undefined {
    return this.args.keyBlockSize;
  }

  get $using (): string | undefined {
    return this.args.using;
  }

  get $parser (): Expression | undefined {
    return this.args.parser;
  }

  get $comment (): string | undefined {
    return this.args.comment;
  }

  get $visible (): Expression | undefined {
    return this.args.visible;
  }

  get $engineAttr (): string | undefined {
    return this.args.engineAttr;
  }

  get $secondaryEngineAttr (): string | undefined {
    return this.args.secondaryEngineAttr;
  }
}

/**
 * Enumeration of valid kind values for ColumnConstraint expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum ColumnConstraintExprKind {
  PRIMARY_KEY = 'PRIMARY_KEY',
  UNIQUE = 'UNIQUE',
  NOT_NULL = 'NOT_NULL',
  CHECK = 'CHECK',
  DEFAULT = 'DEFAULT',
  FOREIGN_KEY = 'FOREIGN_KEY',
}

export type ColumnConstraintExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind: ColumnConstraintExprKind;
    this?: Expression;
  },
]>;

export class ColumnConstraintExpr extends Expression {
  key = ExpressionKey.COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for ColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ColumnConstraintExprArgs> = {
    ...super.argTypes,
    this: false,
    kind: true,
  };

  declare args: ColumnConstraintExprArgs;

  constructor (args: ColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $kind (): ColumnConstraintExprKind {
    return this.args.kind;
  }

  /**
   * Gets the kind of column constraint
   * @returns The ColumnConstraintKind expression
   */
  get kind (): ColumnConstraintExprKind {
    return this.args.kind;
  }
}

export type ColumnConstraintKindExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class ColumnConstraintKindExpr extends Expression {
  key = ExpressionKey.COLUMN_CONSTRAINT_KIND;

  static argTypes: RequiredMap<ColumnConstraintKindExprArgs> = {
    ...super.argTypes,
  };

  declare args: ColumnConstraintKindExprArgs;

  constructor (args: ColumnConstraintKindExprArgs) {
    super(args);
  }
}

export type WithOperatorExprArgs = Merge<[
  BaseExpressionArgs,
  {
    op: Expression;
    this: Expression;
  },
]>;

export class WithOperatorExpr extends Expression {
  key = ExpressionKey.WITH_OPERATOR;

  /**
   * Defines the arguments (properties and child expressions) for WithOperator expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<WithOperatorExprArgs> = {
    ...super.argTypes,
    this: true,
    op: true,
  };

  declare args: WithOperatorExprArgs;

  constructor (args: WithOperatorExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $op (): Expression {
    return this.args.op;
  }
}

export type WatermarkColumnConstraintExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class WatermarkColumnConstraintExpr extends Expression {
  key = ExpressionKey.WATERMARK_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<BaseExpressionArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: WatermarkColumnConstraintExprArgs;

  constructor (args: WatermarkColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type ConstraintExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expressions: Expression[];
  },
]>;

export class ConstraintExpr extends Expression {
  key = ExpressionKey.CONSTRAINT;

  static argTypes: RequiredMap<BaseExpressionArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
  };

  declare args: ConstraintExprArgs;

  constructor (args: ConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type DeleteExprArgs = Merge<[
  DMLExprArgs,
  {
    with?: Expression;
    using?: string;
    where?: Expression;
    returning?: Expression;
    order?: Expression;
    limit?: number | Expression;
    tables?: Expression[];
    cluster?: Expression;
    this?: Expression;
  },
]>;

export class DeleteExpr extends DMLExpr {
  key = ExpressionKey.DELETE;

  /**
   * Defines the arguments (properties and child expressions) for Delete expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<DeleteExprArgs> = {
    ...super.argTypes,
    with: false,
    this: false,
    using: false,
    where: false,
    returning: false,
    order: false,
    limit: false,
    tables: false,
    cluster: false,
  };

  declare args: DeleteExprArgs;

  constructor (args: DeleteExprArgs) {
    super(args);
  }

  /**
   * Create a DELETE expression or replace the table on an existing DELETE expression.
   *
   * Example:
   *     delete("tbl").sql();
   *     // 'DELETE FROM tbl'
   *
   * @param table - The table from which to delete
   * @param options - Options object
   * @param options.dialect - The dialect used to parse the input expression
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns The modified expression
   */
  delete (
    table: string | Expression,
    options: {
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      copy = true, ...restOptions
    } = options;
    return _applyBuilder(table, {
      instance: this,
      arg: 'this',
      into: TableExpr,
      ...restOptions,
      copy,
    }) as this;
  }

  /**
   * Append to or set the WHERE expressions.
   *
   * Example:
   *     delete("tbl").where(["x = 'a' OR x < 'b'"]).sql();
   *     // "DELETE FROM tbl WHERE x = 'a' OR x < 'b'"
   *
   * @param expressions - The SQL code strings to parse.
   *                      If an `Expression` instance is passed, it will be used as-is.
   *                      Multiple expressions are combined with an AND operator.
   * @param options - Options object
   * @param options.append - If `true`, AND the new expressions to any existing expression.
   * Otherwise, this resets the expression. Default is `true`.
   * @param options.dialect - The dialect used to parse the input expressions
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns The modified expression
   */
  where (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [index: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, copy = true, ...restOptions
    } = options;
    return _applyConjunctionBuilder(ensureList(expressions), {
      instance: this,
      arg: 'where',
      into: WhereExpr,
      ...restOptions,
      append,
      copy,
    }) as this;
  }

  get $with (): Expression | undefined {
    return this.args.with;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $using (): string | undefined {
    return this.args.using;
  }

  get $where (): Expression | undefined {
    return this.args.where;
  }

  get $returning (): Expression | undefined {
    return this.args.returning;
  }

  get $order (): Expression | undefined {
    return this.args.order;
  }

  get $limit (): number | Expression | undefined {
    return this.args.limit;
  }

  get $tables (): Expression[] | undefined {
    return this.args.tables;
  }

  get $cluster (): Expression | undefined {
    return this.args.cluster;
  }
}

/**
 * Valid kind values for DROP statements
 */
export enum DropExprKind {
  TABLE = 'TABLE',
  VIEW = 'VIEW',
  INDEX = 'INDEX',
  SCHEMA = 'SCHEMA',
  DATABASE = 'DATABASE',
  FUNCTION = 'FUNCTION',
  PROCEDURE = 'PROCEDURE',
  TRIGGER = 'TRIGGER',
  SEQUENCE = 'SEQUENCE',
  CONSTRAINT = 'CONSTRAINT',
  COLUMN = 'COLUMN',
}

export type DropExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: DropExprKind;
    exists?: boolean;
    temporary?: boolean;
    materialized?: boolean;
    cascade?: Expression;
    constraints?: Expression[];
    purge?: Expression;
    cluster?: Expression;
    concurrently?: Expression;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class DropExpr extends Expression {
  key = ExpressionKey.DROP;

  /**
   * Defines the arguments (properties and child expressions) for Drop expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<DropExprArgs> = {
    ...super.argTypes,
    this: false,
    kind: false,
    expressions: false,
    exists: false,
    temporary: false,
    materialized: false,
    cascade: false,
    constraints: false,
    purge: false,
    cluster: false,
    concurrently: false,
  };

  declare args: DropExprArgs;

  constructor (args: DropExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $kind (): DropExprKind | undefined {
    return this.args.kind;
  }

  /**
   * Gets the kind of DROP statement
   * @returns The kind as an uppercase string, or undefined
   */
  get kind (): DropExprKind | undefined {
    return this.args.kind;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $exists (): boolean | undefined {
    return this.args.exists;
  }

  get $temporary (): boolean | undefined {
    return this.args.temporary;
  }

  get $materialized (): boolean | undefined {
    return this.args.materialized;
  }

  get $cascade (): Expression | undefined {
    return this.args.cascade;
  }

  get $constraints (): Expression[] | undefined {
    return this.args.constraints;
  }

  get $purge (): Expression | undefined {
    return this.args.purge;
  }

  get $cluster (): Expression | undefined {
    return this.args.cluster;
  }

  get $concurrently (): Expression | undefined {
    return this.args.concurrently;
  }
}

export type ExportExprArgs = Merge<[
  BaseExpressionArgs,
  {
    connection?: Expression;
    options: Expression[];
    this: Expression;
  },
]>;

export class ExportExpr extends Expression {
  key = ExpressionKey.EXPORT;

  /**
   * Defines the arguments (properties and child expressions) for Export expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ExportExprArgs> = {
    ...super.argTypes,
    this: true,
    connection: false,
    options: true,
  };

  declare args: ExportExprArgs;

  constructor (args: ExportExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $connection (): Expression | undefined {
    return this.args.connection;
  }

  get $options (): Expression[] {
    return this.args.options;
  }
}

export type FilterExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class FilterExpr extends Expression {
  key = ExpressionKey.FILTER;

  /**
   * Defines the arguments (properties and child expressions) for Filter expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<FilterExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: FilterExprArgs;

  constructor (args: FilterExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type CheckExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class CheckExpr extends Expression {
  key = ExpressionKey.CHECK;

  static argTypes: RequiredMap<CheckExprArgs> = {
    ...super.argTypes,
  };

  declare args: CheckExprArgs;

  constructor (args: CheckExprArgs) {
    super(args);
  }
}

export type ChangesExprArgs = Merge<[
  BaseExpressionArgs,
  {
    information: string;
    atBefore?: Expression;
    end?: Expression;
  },
]>;

export class ChangesExpr extends Expression {
  key = ExpressionKey.CHANGES;

  /**
   * Defines the arguments (properties and child expressions) for Changes expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ChangesExprArgs> = {
    ...super.argTypes,
    information: true,
    atBefore: false,
    end: false,
  };

  declare args: ChangesExprArgs;

  constructor (args: ChangesExprArgs) {
    super(args);
  }

  get $information (): string {
    return this.args.information;
  }

  get $atBefore (): Expression | undefined {
    return this.args.atBefore;
  }

  get $end (): Expression | undefined {
    return this.args.end;
  }
}

export type ConnectExprArgs = Merge<[
  BaseExpressionArgs,
  {
    start?: Expression;
    connect: Expression;
    nocycle?: Expression;
  },
]>;

export class ConnectExpr extends Expression {
  key = ExpressionKey.CONNECT;

  /**
   * Defines the arguments (properties and child expressions) for Connect expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ConnectExprArgs> = {
    ...super.argTypes,
    start: false,
    connect: true,
    nocycle: false,
  };

  declare args: ConnectExprArgs;

  constructor (args: ConnectExprArgs) {
    super(args);
  }

  get $start (): Expression | undefined {
    return this.args.start;
  }

  get $connect (): Expression {
    return this.args.connect;
  }

  get $nocycle (): Expression | undefined {
    return this.args.nocycle;
  }
}

export type CopyParameterExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression?: Expression;
    expressions?: Expression[];
  },
]>;

export class CopyParameterExpr extends Expression {
  key = ExpressionKey.COPY_PARAMETER;

  /**
   * Defines the arguments (properties and child expressions) for CopyParameter expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<CopyParameterExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
    expressions: false,
  };

  declare args: CopyParameterExprArgs;

  constructor (args: CopyParameterExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

export type CredentialsExprArgs = Merge<[
  BaseExpressionArgs,
  {
    credentials?: Expression[];
    encryption?: Expression;
    storage?: Expression;
    iamRole?: Expression;
    region?: Expression;
  },
]>;

export class CredentialsExpr extends Expression {
  key = ExpressionKey.CREDENTIALS;

  /**
   * Defines the arguments (properties and child expressions) for Credentials expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<CredentialsExprArgs> = {
    ...super.argTypes,
    credentials: false,
    encryption: false,
    storage: false,
    iamRole: false,
    region: false,
  };

  declare args: CredentialsExprArgs;

  constructor (args: CredentialsExprArgs) {
    super(args);
  }

  get $credentials (): Expression[] | undefined {
    return this.args.credentials;
  }

  get $encryption (): Expression | undefined {
    return this.args.encryption;
  }

  get $storage (): Expression | undefined {
    return this.args.storage;
  }

  get $iamRole (): Expression | undefined {
    return this.args.iamRole;
  }

  get $region (): Expression | undefined {
    return this.args.region;
  }
}

export type PriorExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class PriorExpr extends Expression {
  key = ExpressionKey.PRIOR;

  static argTypes: RequiredMap<PriorExprArgs> = {
    ...super.argTypes,
  };

  declare args: PriorExprArgs;

  constructor (args: PriorExprArgs) {
    super(args);
  }
}

export type DirectoryExprArgs = Merge<[
  BaseExpressionArgs,
  {
    local?: Expression;
    rowFormat?: string;
    this: Expression;
  },
]>;

export class DirectoryExpr extends Expression {
  key = ExpressionKey.DIRECTORY;

  /**
   * Defines the arguments (properties and child expressions) for Directory expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<DirectoryExprArgs> = {
    ...super.argTypes,
    this: true,
    local: false,
    rowFormat: false,
  };

  declare args: DirectoryExprArgs;

  constructor (args: DirectoryExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $local (): Expression | undefined {
    return this.args.local;
  }

  get $rowFormat (): string | undefined {
    return this.args.rowFormat;
  }
}

export type DirectoryStageExprArgs = Merge<[
  BaseExpressionArgs,
]>;
export class DirectoryStageExpr extends Expression {
  key = ExpressionKey.DIRECTORY_STAGE;

  static argTypes: RequiredMap<DirectoryStageExprArgs> = {
    ...super.argTypes,
  };

  declare args: DirectoryStageExprArgs;

  constructor (args: DirectoryStageExprArgs) {
    super(args);
  }
}

export type ForeignKeyExprArgs = Merge<[
  BaseExpressionArgs,
  {
    reference?: Expression;
    delete?: Expression;
    update?: Expression;
    options?: Expression[];
    expressions?: Expression[];
  },
]>;

export class ForeignKeyExpr extends Expression {
  key = ExpressionKey.FOREIGN_KEY;

  /**
   * Defines the arguments (properties and child expressions) for ForeignKey expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ForeignKeyExprArgs> = {
    ...super.argTypes,
    expressions: false,
    reference: false,
    delete: false,
    update: false,
    options: false,
  };

  declare args: ForeignKeyExprArgs;

  constructor (args: ForeignKeyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $reference (): Expression | undefined {
    return this.args.reference;
  }

  get $delete (): Expression | undefined {
    return this.args.delete;
  }

  get $update (): Expression | undefined {
    return this.args.update;
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }
}

export type ColumnPrefixExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class ColumnPrefixExpr extends Expression {
  key = ExpressionKey.COLUMN_PREFIX;

  /**
   * Defines the arguments (properties and child expressions) for ColumnPrefix expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<BaseExpressionArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: ColumnPrefixExprArgs;

  constructor (args: ColumnPrefixExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type PrimaryKeyExprArgs = Merge<[
  BaseExpressionArgs,
  {
    options?: Expression[];
    include?: Expression;
    this?: Expression;
    expressions: Expression[];
  },
]>;

export class PrimaryKeyExpr extends Expression {
  key = ExpressionKey.PRIMARY_KEY;

  /**
   * Defines the arguments (properties and child expressions) for PrimaryKey expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<PrimaryKeyExprArgs> = {
    ...super.argTypes,
    this: false,
    expressions: true,
    options: false,
    include: false,
  };

  declare args: PrimaryKeyExprArgs;

  constructor (args: PrimaryKeyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }

  get $include (): Expression | undefined {
    return this.args.include;
  }
}

export type IntoExprArgs = Merge<[
  BaseExpressionArgs,
  {
    temporary?: boolean;
    unlogged?: Expression;
    bulkCollect?: Expression;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class IntoExpr extends Expression {
  key = ExpressionKey.INTO;

  /**
   * Defines the arguments (properties and child expressions) for Into expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<IntoExprArgs> = {
    ...super.argTypes,
    this: false,
    temporary: false,
    unlogged: false,
    bulkCollect: false,
    expressions: false,
  };

  declare args: IntoExprArgs;

  constructor (args: IntoExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $temporary (): boolean | undefined {
    return this.args.temporary;
  }

  get $unlogged (): Expression | undefined {
    return this.args.unlogged;
  }

  get $bulkCollect (): Expression | undefined {
    return this.args.bulkCollect;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

export type FromExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression; // NOTE: sqlglot does not have this, but based on usage, I added it;
  },
]>;

export class FromExpr extends Expression {
  key = ExpressionKey.FROM;

  static argTypes: RequiredMap<FromExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: FromExprArgs;

  constructor (args: FromExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  /**
   * Gets the name of the FROM expression
   * @returns The name of the expression
   */
  get name (): string {
    return this.$this?.name || '';
  }

  /**
   * Gets the alias or name of the FROM expression
   * @returns The alias if it exists, otherwise the name
   */
  get aliasOrName (): string {
    return this.$this?.aliasOrName || '';
  }
}

export type HavingExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class HavingExpr extends Expression {
  key = ExpressionKey.HAVING;

  static argTypes: RequiredMap<HavingExprArgs> = {
    ...super.argTypes,
  };

  declare args: HavingExprArgs;

  constructor (args: HavingExprArgs) {
    super(args);
  }
}

export type HintExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions: (Expression | string)[] },
]>;
export class HintExpr extends Expression {
  key = ExpressionKey.HINT;

  static argTypes: RequiredMap<BaseExpressionArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: HintExprArgs;

  constructor (args: HintExprArgs) {
    super(args);
  }

  get $expressions (): (Expression | string)[] {
    return this.args.expressions;
  }
}

export type JoinHintExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: string;
    expressions: Expression[];
  },
]>;

export class JoinHintExpr extends Expression {
  key = ExpressionKey.JOIN_HINT;

  static argTypes: RequiredMap<BaseExpressionArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
  };

  declare args: JoinHintExprArgs;

  constructor (args: JoinHintExprArgs) {
    super(args);
  }

  get $this (): string {
    return this.args.this;
  }

  get $expressions (): (Expression | string)[] {
    return this.args.expressions;
  }
}

export type IdentifierExprArgs = Merge<[
  BaseExpressionArgs,
  {
    quoted?: boolean;
    global?: boolean;
    temporary?: boolean;
    this: string;
  },
]>;

export class IdentifierExpr extends Expression {
  key = ExpressionKey.IDENTIFIER;

  /**
   * Defines the arguments (properties and child expressions) for Identifier expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<IdentifierExprArgs> = {
    ...super.argTypes,
    this: true,
    quoted: false,
    global: false,
    temporary: false,
  };

  declare args: IdentifierExprArgs;

  constructor (args: IdentifierExprArgs) {
    super(args);
  }

  get outputName (): string {
    return this.name;
  }

  get $this (): string {
    return this.args.this;
  }

  get $quoted (): boolean | undefined {
    return this.args.quoted;
  }

  get $global (): boolean | undefined {
    return this.args.global;
  }

  get $temporary (): boolean | undefined {
    return this.args.temporary;
  }
}

export type OpclassExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class OpclassExpr extends Expression {
  key = ExpressionKey.OPCLASS;

  static argTypes: RequiredMap<BaseExpressionArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: OpclassExprArgs;

  constructor (args: OpclassExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type IndexExprArgs = Merge<[
  BaseExpressionArgs,
  {
    table?: Expression;
    unique?: boolean;
    primary?: boolean;
    amp?: Expression;
    params?: Expression[];
    this?: Expression;
  },
]>;

export class IndexExpr extends Expression {
  key = ExpressionKey.INDEX;

  /**
   * Defines the arguments (properties and child expressions) for Index expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<IndexExprArgs> = {
    ...super.argTypes,
    this: false,
    table: false,
    unique: false,
    primary: false,
    amp: false,
    params: false,
  };

  declare args: IndexExprArgs;

  constructor (args: IndexExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $table (): Expression | undefined {
    return this.args.table;
  }

  get $unique (): boolean | undefined {
    return this.args.unique;
  }

  get $primary (): boolean | undefined {
    return this.args.primary;
  }

  get $amp (): Expression | undefined {
    return this.args.amp;
  }

  get $params (): Expression[] | undefined {
    return this.args.params;
  }
}

export type IndexParametersExprArgs = Merge<[
  BaseExpressionArgs,
  {
    using?: string;
    include?: Expression;
    columns?: Expression[];
    withStorage?: Expression;
    partitionBy?: Expression;
    tablespace?: Expression;
    where?: Expression;
    on?: Expression;
  },
]>;

export class IndexParametersExpr extends Expression {
  key = ExpressionKey.INDEX_PARAMETERS;

  /**
   * Defines the arguments (properties and child expressions) for IndexParameters expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<IndexParametersExprArgs> = {
    ...super.argTypes,
    using: false,
    include: false,
    columns: false,
    withStorage: false,
    partitionBy: false,
    tablespace: false,
    where: false,
    on: false,
  };

  declare args: IndexParametersExprArgs;

  constructor (args: IndexParametersExprArgs) {
    super(args);
  }

  get $using (): string | undefined {
    return this.args.using;
  }

  get $include (): Expression | undefined {
    return this.args.include;
  }

  get $columns (): Expression[] | undefined {
    return this.args.columns;
  }

  get $withStorage (): Expression | undefined {
    return this.args.withStorage;
  }

  get $partitionBy (): Expression | undefined {
    return this.args.partitionBy;
  }

  get $tablespace (): Expression | undefined {
    return this.args.tablespace;
  }

  get $where (): Expression | undefined {
    return this.args.where;
  }

  get $on (): Expression | undefined {
    return this.args.on;
  }
}

export type ConditionalInsertExprArgs = Merge<[
  BaseExpressionArgs,
  {
    else?: Expression;
    this: Expression;
    expression?: Expression;
  },
]>;

export class ConditionalInsertExpr extends Expression {
  key = ExpressionKey.CONDITIONAL_INSERT;

  /**
   * Defines the arguments (properties and child expressions) for ConditionalInsert expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ConditionalInsertExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
    else: false,
  };

  declare args: ConditionalInsertExprArgs;

  constructor (args: ConditionalInsertExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $else (): Expression | undefined {
    return this.args.else;
  }
}

/**
 * Enumeration of valid kind values for MultitableInserts expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum MultitableInsertsExprKind {
  CONDITIONAL = 'CONDITIONAL',
  UNCONDITIONAL = 'UNCONDITIONAL',
}

export type MultitableInsertsExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind: MultitableInsertsExprKind;
    source: Expression;
    expressions: Expression[];
  },
]>;

export class MultitableInsertsExpr extends Expression {
  key = ExpressionKey.MULTITABLE_INSERTS;

  /**
   * Defines the arguments (properties and child expressions) for MultitableInserts expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<MultitableInsertsExprArgs> = {
    ...super.argTypes,
    expressions: true,
    kind: true,
    source: true,
  };

  declare args: MultitableInsertsExprArgs;

  constructor (args: MultitableInsertsExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $kind (): MultitableInsertsExprKind {
    return this.args.kind;
  }

  get $source (): Expression {
    return this.args.source;
  }
}

export type OnConflictExprArgs = Merge<[
  BaseExpressionArgs,
  {
    duplicate?: Expression;
    action?: Expression;
    conflictKeys?: Expression[];
    indexPredicate?: Expression;
    constraint?: Expression;
    where?: Expression;
    expressions?: Expression[];
  },
]>;

export class OnConflictExpr extends Expression {
  key = ExpressionKey.ON_CONFLICT;

  /**
   * Defines the arguments (properties and child expressions) for OnConflict expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<OnConflictExprArgs> = {
    ...super.argTypes,
    duplicate: false,
    expressions: false,
    action: false,
    conflictKeys: false,
    indexPredicate: false,
    constraint: false,
    where: false,
  };

  declare args: OnConflictExprArgs;

  constructor (args: OnConflictExprArgs) {
    super(args);
  }

  get $duplicate (): Expression | undefined {
    return this.args.duplicate;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $action (): Expression | undefined {
    return this.args.action;
  }

  get $conflictKeys (): Expression[] | undefined {
    return this.args.conflictKeys;
  }

  get $indexPredicate (): Expression | undefined {
    return this.args.indexPredicate;
  }

  get $constraint (): Expression | undefined {
    return this.args.constraint;
  }

  get $where (): Expression | undefined {
    return this.args.where;
  }
}

export type OnConditionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    error?: Expression;
    empty?: Expression;
    null?: Expression;
  },
]>;

export class OnConditionExpr extends Expression {
  key = ExpressionKey.ON_CONDITION;

  /**
   * Defines the arguments (properties and child expressions) for OnCondition expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<OnConditionExprArgs> = {
    ...super.argTypes,
    error: false,
    empty: false,
    null: false,
  };

  declare args: OnConditionExprArgs;

  constructor (args: OnConditionExprArgs) {
    super(args);
  }

  get $error (): Expression | undefined {
    return this.args.error;
  }

  get $empty (): Expression | undefined {
    return this.args.empty;
  }

  get $null (): Expression | undefined {
    return this.args.null;
  }
}

export type ReturningExprArgs = Merge<[
  BaseExpressionArgs,
  {
    into?: Expression;
    expressions: Expression[];
  },
]>;

export class ReturningExpr extends Expression {
  key = ExpressionKey.RETURNING;

  /**
   * Defines the arguments (properties and child expressions) for Returning expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ReturningExprArgs> = {
    ...super.argTypes,
    expressions: true,
    into: false,
  };

  declare args: ReturningExprArgs;

  constructor (args: ReturningExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $into (): Expression | undefined {
    return this.args.into;
  }
}

export type IntroducerExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class IntroducerExpr extends Expression {
  key = ExpressionKey.INTRODUCER;

  static argTypes: RequiredMap<BaseExpressionArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: IntroducerExprArgs;

  constructor (args: IntroducerExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type NationalExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class NationalExpr extends Expression {
  key = ExpressionKey.NATIONAL;

  static argTypes: RequiredMap<NationalExprArgs> = {
    ...super.argTypes,
  };

  declare args: NationalExprArgs;

  constructor (args: NationalExprArgs) {
    super(args);
  }
}

export type LoadDataExprArgs = Merge<[
  BaseExpressionArgs,
  {
    local?: Expression;
    overwrite?: Expression;
    inpath: Expression;
    partition?: Expression;
    inputFormat?: string;
    serde?: Expression;
    this: Expression;
  },
]>;

export class LoadDataExpr extends Expression {
  key = ExpressionKey.LOAD_DATA;

  /**
   * Defines the arguments (properties and child expressions) for LoadData expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<LoadDataExprArgs> = {
    ...super.argTypes,
    this: true,
    local: false,
    overwrite: false,
    inpath: true,
    partition: false,
    inputFormat: false,
    serde: false,
  };

  declare args: LoadDataExprArgs;

  constructor (args: LoadDataExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $local (): Expression | undefined {
    return this.args.local;
  }

  get $overwrite (): Expression | undefined {
    return this.args.overwrite;
  }

  get $inpath (): Expression {
    return this.args.inpath;
  }

  get $partition (): Expression | undefined {
    return this.args.partition;
  }

  get $inputFormat (): string | undefined {
    return this.args.inputFormat;
  }

  get $serde (): Expression | undefined {
    return this.args.serde;
  }
}

export type PartitionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    subpartition?: Expression;
    expressions: Expression[];
  },
]>;

export class PartitionExpr extends Expression {
  key = ExpressionKey.PARTITION;

  /**
   * Defines the arguments (properties and child expressions) for Partition expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<PartitionExprArgs> = {
    ...super.argTypes,
    expressions: true,
    subpartition: false,
  };

  declare args: PartitionExprArgs;

  constructor (args: PartitionExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $subpartition (): Expression | undefined {
    return this.args.subpartition;
  }
}

export type PartitionRangeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression?: Expression;
    expressions?: Expression[];
  },
]>;

export class PartitionRangeExpr extends Expression {
  key = ExpressionKey.PARTITION_RANGE;

  static argTypes: RequiredMap<PartitionRangeExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
    expressions: false,
  };

  declare args: PartitionRangeExprArgs;

  constructor (args: PartitionRangeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

export type PartitionIdExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class PartitionIdExpr extends Expression {
  key = ExpressionKey.PARTITION_ID;

  static argTypes: RequiredMap<PartitionIdExprArgs> = {
    ...super.argTypes,
  };

  declare args: PartitionIdExprArgs;

  constructor (args: PartitionIdExprArgs) {
    super(args);
  }
}

export type FetchExprArgs = Merge<[
  BaseExpressionArgs,
  {
    direction?: Expression;
    count?: Expression;
    limitOptions?: Expression[];
  },
]>;

export class FetchExpr extends Expression {
  key = ExpressionKey.FETCH;

  /**
   * Defines the arguments (properties and child expressions) for Fetch expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<FetchExprArgs> = {
    ...super.argTypes,
    direction: false,
    count: false,
    limitOptions: false,
  };

  declare args: FetchExprArgs;

  constructor (args: FetchExprArgs) {
    super(args);
  }

  get $direction (): Expression | undefined {
    return this.args.direction;
  }

  get $count (): Expression | undefined {
    return this.args.count;
  }

  get $limitOptions (): Expression[] | undefined {
    return this.args.limitOptions;
  }
}

/**
 * Valid kind values for GRANT statements
 */
export enum GrantExprKind {
  GRANT = 'GRANT',
  REVOKE = 'REVOKE',
}

export type GrantExprArgs = Merge<[
  BaseExpressionArgs,
  {
    privileges: Expression[];
    kind?: GrantExprKind;
    securable: Expression;
    principals: Expression[];
    grantOption?: Expression;
  },
]>;

export class GrantExpr extends Expression {
  key = ExpressionKey.GRANT;

  /**
   * Defines the arguments (properties and child expressions) for Grant expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<GrantExprArgs> = {
    ...super.argTypes,
    privileges: true,
    kind: false,
    securable: true,
    principals: true,
    grantOption: false,
  };

  declare args: GrantExprArgs;

  constructor (args: GrantExprArgs) {
    super(args);
  }

  get $privileges (): Expression[] {
    return this.args.privileges;
  }

  get $kind (): GrantExprKind | undefined {
    return this.args.kind;
  }

  get $securable (): Expression {
    return this.args.securable;
  }

  get $principals (): Expression[] {
    return this.args.principals;
  }

  get $grantOption (): Expression | undefined {
    return this.args.grantOption;
  }
}

export type RevokeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    cascade?: Expression;
    privileges: Expression[];
    kind?: string;
    securable: Expression;
    principals: Expression[];
    grantOption?: Expression;
  },
]>;

export class RevokeExpr extends Expression {
  key = ExpressionKey.REVOKE;

  /**
   * Defines the arguments (properties and child expressions) for Revoke expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   * Extends Grant's arg_types with additional cascade field.
   */
  static argTypes: RequiredMap<RevokeExprArgs> = {
    ...super.argTypes,
    privileges: true,
    kind: false,
    securable: true,
    principals: true,
    grantOption: false,
    cascade: false,
  };

  declare args: RevokeExprArgs;

  constructor (args: RevokeExprArgs) {
    super(args);
  }

  get $privileges (): Expression[] {
    return this.args.privileges;
  }

  get $kind (): string | undefined {
    return this.args.kind;
  }

  get $securable (): Expression {
    return this.args.securable;
  }

  get $principals (): Expression[] {
    return this.args.principals;
  }

  get $grantOption (): Expression | undefined {
    return this.args.grantOption;
  }

  get $cascade (): Expression | undefined {
    return this.args.cascade;
  }
}

export type GroupExprArgs = Merge<[
  BaseExpressionArgs,
  {
    groupingSets?: Expression[];
    cube?: Expression;
    rollup?: Expression;
    totals?: Expression[];
    all?: Expression;
    expressions?: Expression[];
  },
]>;

export class GroupExpr extends Expression {
  key = ExpressionKey.GROUP;

  /**
   * Defines the arguments (properties and child expressions) for Group expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<GroupExprArgs> = {
    ...super.argTypes,
    expressions: false,
    groupingSets: false,
    cube: false,
    rollup: false,
    totals: false,
    all: false,
  };

  declare args: GroupExprArgs;

  constructor (args: GroupExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $groupingSets (): Expression[] | undefined {
    return this.args.groupingSets;
  }

  get $cube (): Expression | undefined {
    return this.args.cube;
  }

  get $rollup (): Expression | undefined {
    return this.args.rollup;
  }

  get $totals (): Expression[] | undefined {
    return this.args.totals;
  }

  get $all (): Expression | undefined {
    return this.args.all;
  }
}

export type CubeExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions?: Expression[] },
]>;

export class CubeExpr extends Expression {
  key = ExpressionKey.CUBE;

  static argTypes: RequiredMap<CubeExprArgs> = {
    ...super.argTypes,
    expressions: false,
  };

  declare args: CubeExprArgs;

  constructor (args: CubeExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

export type RollupExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions?: Expression[] },
]>;

export class RollupExpr extends Expression {
  key = ExpressionKey.ROLLUP;

  static argTypes: RequiredMap<RollupExprArgs> = {
    ...super.argTypes,
    expressions: false,
  };

  declare args: RollupExprArgs;

  constructor (args: RollupExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

export type GroupingSetsExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions: Expression[] },
]>;

export class GroupingSetsExpr extends Expression {
  key = ExpressionKey.GROUPING_SETS;

  static argTypes: RequiredMap<GroupingSetsExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: GroupingSetsExprArgs;

  constructor (args: GroupingSetsExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type LambdaExprArgs = Merge<[
  BaseExpressionArgs,
  {
    colon?: Expression;
    this: Expression;
    expressions: Expression[];
  },
]>;

export class LambdaExpr extends Expression {
  key = ExpressionKey.LAMBDA;

  /**
   * Defines the arguments (properties and child expressions) for Lambda expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<LambdaExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
    colon: false,
  };

  declare args: LambdaExprArgs;

  constructor (args: LambdaExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $colon (): Expression | undefined {
    return this.args.colon;
  }
}

export type LimitExprArgs = Merge<[
  BaseExpressionArgs,
  {
    offset?: boolean;
    limitOptions?: Expression[];
    this?: Expression;
    expression: Expression;
    expressions?: Expression[];
  },
]>;

export class LimitExpr extends Expression {
  key = ExpressionKey.LIMIT;

  /**
   * Defines the arguments (properties and child expressions) for Limit expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<LimitExprArgs> = {
    ...super.argTypes,
    this: false,
    expression: true,
    offset: false,
    limitOptions: false,
    expressions: false,
  };

  declare args: LimitExprArgs;

  constructor (args: LimitExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $offset (): boolean | undefined {
    return this.args.offset;
  }

  get $limitOptions (): Expression[] | undefined {
    return this.args.limitOptions;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

export type LimitOptionsExprArgs = Merge<[
  BaseExpressionArgs,
  {
    percent?: Expression;
    rows?: Expression[];
    withTies?: Expression[];
  },
]>;

export class LimitOptionsExpr extends Expression {
  key = ExpressionKey.LIMIT_OPTIONS;

  /**
   * Defines the arguments (properties and child expressions) for LimitOptions expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<LimitOptionsExprArgs> = {
    ...super.argTypes,
    percent: false,
    rows: false,
    withTies: false,
  };

  declare args: LimitOptionsExprArgs;

  constructor (args: LimitOptionsExprArgs) {
    super(args);
  }

  get $percent (): Expression | undefined {
    return this.args.percent;
  }

  get $rows (): Expression[] | undefined {
    return this.args.rows;
  }

  get $withTies (): Expression[] | undefined {
    return this.args.withTies;
  }
}

/**
 * Enumeration of valid kind values for JOIN expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum JoinExprKind {
  INNER = 'INNER',
  LEFT = 'LEFT',
  RIGHT = 'RIGHT',
  FULL = 'FULL',
  CROSS = 'CROSS',
  SEMI = 'SEMI',
  ANTI = 'ANTI',
}

/**
 * Represents a JOIN clause in SQL.
 *
 * @example
 * // INNER JOIN users ON users.id = orders.user_id
 * const join = new JoinExpr({
 *   this: usersTable,
 *   on: joinCondition,
 *   kind: JoinExprKind.INNER
 * });
 */
export type JoinExprArgs = Merge<[
  BaseExpressionArgs,
  {
    on?: Expression;
    side?: Expression;
    kind?: JoinExprKind;
    using?: string;
    method?: string;
    global?: boolean;
    hint?: Expression;
    matchCondition?: Expression;
    directed?: Expression;
    pivots?: Expression[];
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class JoinExpr extends Expression {
  key = ExpressionKey.JOIN;

  /**
   * Defines the arguments (properties and child expressions) for Join expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JoinExprArgs> = {
    ...super.argTypes,
    this: true,
    on: false,
    side: false,
    kind: false,
    using: false,
    method: false,
    global: false,
    hint: false,
    matchCondition: false,
    directed: false,
    expressions: false,
    pivots: false,
  };

  declare args: JoinExprArgs;

  constructor (args: JoinExprArgs) {
    super(args);
  }

  /**
   * Append to or set the ON expressions.
   *
   * @example
   * sqlglot.parseOne("JOIN x", Join).on("y = 1").sql()
   * // 'JOIN x ON y = 1'
   *
   * @param expressions - the SQL code strings to parse.
   *   If an Expression instance is passed, it will be used as-is.
   *   Multiple expressions are combined with an AND operator.
   * @param options - Configuration options
   * @param options.append - if true, AND the new expressions to any existing expression. Otherwise,
   * this resets the expression.
   * @param options.dialect - the dialect used to parse the input expressions.
   * @param options.copy - if false, modify this expression instance in-place.
   * @returns The modified Join expression.
   */
  on (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      append, dialect, copy, ...restOptions
    } = options;
    const expressionList = ensureList(expressions);
    const join = _applyConjunctionBuilder(expressionList, {
      instance: this,
      arg: 'on',
      append,
      dialect,
      copy,
      ...restOptions,
    }) as this;

    if (join.$kind === JoinExprKind.CROSS) {
      join.setArgKey('kind', undefined);
    }

    return join;
  }

  /**
   * Append to or set the USING expressions.
   *
   * @example
   * sqlglot.parseOne("JOIN x", Join).using("foo", "bla").sql()
   * // 'JOIN x USING (foo, bla)'
   *
   * @param expressions - the SQL code strings to parse.
   *   If an Expression instance is passed, it will be used as-is.
   * @param options - Configuration options
   * @param options.append - if true, concatenate the new expressions to the existing "using" list.
   * Otherwise, this resets the expression.
   * @param options.dialect - the dialect used to parse the input expressions.
   * @param options.copy - if false, modify this expression instance in-place.
   * @returns The modified Join expression.
   */
  using (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      append, dialect, copy, ...restOptions
    } = options;
    const expressionList = ensureList(expressions);
    const join = _applyListBuilder(expressionList, {
      instance: this,
      arg: 'using',
      append,
      dialect,
      copy,
      ...restOptions,
    }) as this;

    if (join.$kind === JoinExprKind.CROSS) {
      join.setArgKey('kind', undefined);
    }

    return join;
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $on (): Expression | undefined {
    return this.args.on;
  }

  get $side (): Expression | undefined {
    return this.args.side;
  }

  get $kind (): JoinExprKind | undefined {
    return this.args.kind;
  }

  get $using (): string | undefined {
    return this.args.using;
  }

  get $method (): string | undefined {
    return this.args.method;
  }

  get $global (): boolean | undefined {
    return this.args.global;
  }

  get $hint (): Expression | undefined {
    return this.args.hint;
  }

  get $matchCondition (): Expression | undefined {
    return this.args.matchCondition;
  }

  get $directed (): Expression | undefined {
    return this.args.directed;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $pivots (): Expression[] | undefined {
    return this.args.pivots;
  }

  get method (): string {
    return this.text('method').toUpperCase();
  }

  get kind (): string {
    return this.text('kind').toUpperCase();
  }

  get side (): string {
    return this.text('side').toUpperCase();
  }

  get hint (): string {
    return this.text('hint').toUpperCase();
  }

  get aliasOrName (): string {
    return this.$this.aliasOrName;
  }

  get isSemiOrAntiJoin (): boolean {
    return [JoinExprKind.SEMI, JoinExprKind.ANTI].includes(this.$kind!);
  }
}

export type MatchRecognizeMeasureExprArgs = Merge<[
  BaseExpressionArgs,
  {
    windowFrame?: Expression;
    this: Expression;
  },
]>;

export class MatchRecognizeMeasureExpr extends Expression {
  key = ExpressionKey.MATCH_RECOGNIZE_MEASURE;

  /**
   * Defines the arguments (properties and child expressions) for MatchRecognizeMeasure expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<MatchRecognizeMeasureExprArgs> = {
    ...super.argTypes,
    this: true,
    windowFrame: false,
  };

  declare args: MatchRecognizeMeasureExprArgs;

  constructor (args: MatchRecognizeMeasureExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $windowFrame (): Expression | undefined {
    return this.args.windowFrame;
  }
}

export type MatchRecognizeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    partitionBy?: Expression;
    order?: Expression;
    measures?: Expression[];
    rows?: Expression[];
    after?: Expression;
    pattern?: Expression;
    define?: Expression;
    alias?: TableAliasExpr;
  },
]>;

export class MatchRecognizeExpr extends Expression {
  key = ExpressionKey.MATCH_RECOGNIZE;

  /**
   * Defines the arguments (properties and child expressions) for MatchRecognize expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<MatchRecognizeExprArgs> = {
    ...super.argTypes,
    partitionBy: false,
    order: false,
    measures: false,
    rows: false,
    after: false,
    pattern: false,
    define: false,
    alias: false,
  };

  declare args: MatchRecognizeExprArgs;

  constructor (args: MatchRecognizeExprArgs) {
    super(args);
  }

  get $partitionBy (): Expression | undefined {
    return this.args.partitionBy;
  }

  get $order (): Expression | undefined {
    return this.args.order;
  }

  get $measures (): Expression[] | undefined {
    return this.args.measures;
  }

  get $rows (): Expression[] | undefined {
    return this.args.rows;
  }

  get $after (): Expression | undefined {
    return this.args.after;
  }

  get $pattern (): Expression | undefined {
    return this.args.pattern;
  }

  get $define (): Expression | undefined {
    return this.args.define;
  }

  get $alias (): TableAliasExpr | undefined {
    return this.args.alias;
  }
}

export type FinalExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class FinalExpr extends Expression {
  key = ExpressionKey.FINAL;

  static argTypes: RequiredMap<FinalExprArgs> = {
    ...super.argTypes,
  };

  declare args: FinalExprArgs;

  constructor (args: FinalExprArgs) {
    super(args);
  }
}

export type OffsetExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression: Expression;
    expressions?: Expression[];
  },
]>;

export class OffsetExpr extends Expression {
  key = ExpressionKey.OFFSET;

  static argTypes: RequiredMap<BaseExpressionArgs> = {
    ...super.argTypes,
    this: false,
    expression: true,
    expressions: false,
  };

  declare args: OffsetExprArgs;

  constructor (args: OffsetExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

export type OrderExprArgs = Merge<[
  BaseExpressionArgs,
  {
    siblings?: Expression[];
    this?: Expression;
    expressions: Expression[];
  },
]>;

export class OrderExpr extends Expression {
  key = ExpressionKey.ORDER;

  /**
   * Defines the arguments (properties and child expressions) for Order expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<OrderExprArgs> = {
    ...super.argTypes,
    this: false,
    expressions: true,
    siblings: false,
  };

  declare args: OrderExprArgs;

  constructor (args: OrderExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $siblings (): Expression[] | undefined {
    return this.args.siblings;
  }
}

export type WithFillExprArgs = Merge<[
  BaseExpressionArgs,
  {
    from?: Expression;
    to?: Expression;
    step?: Expression;
    interpolate?: Expression;
  },
]>;

export class WithFillExpr extends Expression {
  key = ExpressionKey.WITH_FILL;

  /**
   * Defines the arguments (properties and child expressions) for WithFill expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<WithFillExprArgs> = {
    ...super.argTypes,
    from: false,
    to: false,
    step: false,
    interpolate: false,
  };

  declare args: WithFillExprArgs;

  constructor (args: WithFillExprArgs) {
    super(args);
  }

  get $from (): Expression | undefined {
    return this.args.from;
  }

  get $to (): Expression | undefined {
    return this.args.to;
  }

  get $step (): Expression | undefined {
    return this.args.step;
  }

  get $interpolate (): Expression | undefined {
    return this.args.interpolate;
  }
}

export type OrderedExprArgs = Merge<[
  BaseExpressionArgs,
  {
    desc?: Expression;
    nullsFirst: Expression;
    withFill?: Expression;
    this: Expression;
  },
]>;

export class OrderedExpr extends Expression {
  key = ExpressionKey.ORDERED;

  /**
   * Defines the arguments (properties and child expressions) for Ordered expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<OrderedExprArgs> = {
    ...super.argTypes,
    this: true,
    desc: false,
    nullsFirst: true,
    withFill: false,
  };

  declare args: OrderedExprArgs;

  constructor (args: OrderedExprArgs) {
    super(args);
  }

  get name (): string {
    return this.$this?.name || '';
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $desc (): Expression | undefined {
    return this.args.desc;
  }

  get $nullsFirst (): Expression {
    return this.args.nullsFirst;
  }

  get $withFill (): Expression | undefined {
    return this.args.withFill;
  }
}

export type PropertyExprArgs = Merge<[
  BaseExpressionArgs,
  {
    value?: string | Expression;
    this?: Expression | string; // NOTE: In argTypes, we set this to true
  },
]>;

export class PropertyExpr extends Expression {
  key = ExpressionKey.PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for Property expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<PropertyExprArgs> = {
    ...super.argTypes,
    this: true, // NOTE: sqlglot sets this to true, although some subclasses set this to false
    value: false,
  };

  declare args: PropertyExprArgs;

  constructor (args: PropertyExprArgs | BaseExpressionArgs) {
    super(args);
  }

  get $this (): Expression | string | undefined {
    return this.args.this;
  }

  get $value (): string | Expression | undefined {
    return this.args.value;
  }
}

export type GrantPrivilegeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class GrantPrivilegeExpr extends Expression {
  key = ExpressionKey.GRANT_PRIVILEGE;

  static argTypes: RequiredMap<GrantPrivilegeExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
  };

  declare args: GrantPrivilegeExprArgs;

  constructor (args: GrantPrivilegeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

/**
 * Valid kind values for GRANT principals
 */
export enum GrantPrincipalExprKind {
  USER = 'USER',
  ROLE = 'ROLE',
  GROUP = 'GROUP',
  PUBLIC = 'PUBLIC',
}
export type GrantPrincipalExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: GrantPrincipalExprKind;
    this: Expression;
  },
]>;

export class GrantPrincipalExpr extends Expression {
  key = ExpressionKey.GRANT_PRINCIPAL;

  /**
   * Defines the arguments (properties and child expressions) for GrantPrincipal expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<GrantPrincipalExprArgs> = {
    ...super.argTypes,
    this: true,
    kind: false,
  };

  declare args: GrantPrincipalExprArgs;

  constructor (args: GrantPrincipalExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): GrantPrincipalExprKind | undefined {
    return this.args.kind;
  }
}

export type AllowedValuesPropertyExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions: Expression[] },
]>;

export class AllowedValuesPropertyExpr extends Expression {
  key = ExpressionKey.ALLOWED_VALUES_PROPERTY;

  static argTypes: RequiredMap<AllowedValuesPropertyExprArgs> = {
    expressions: true,
  };

  declare args: AllowedValuesPropertyExprArgs;

  constructor (args: AllowedValuesPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type PartitionByRangePropertyDynamicExprArgs = Merge<[
  BaseExpressionArgs,
  {
    start: Expression;
    end: Expression;
    every: Expression;
    this?: Expression;
  },
]>;

export class PartitionByRangePropertyDynamicExpr extends Expression {
  key = ExpressionKey.PARTITION_BY_RANGE_PROPERTY_DYNAMIC;

  /**
   * Defines the arguments (properties and child expressions) for PartitionByRangePropertyDynamic
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<PartitionByRangePropertyDynamicExprArgs> = {
    ...super.argTypes,
    this: false,
    start: true,
    end: true,
    every: true,
  };

  declare args: PartitionByRangePropertyDynamicExprArgs;

  constructor (args: PartitionByRangePropertyDynamicExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $start (): Expression {
    return this.args.start;
  }

  get $end (): Expression {
    return this.args.end;
  }

  get $every (): Expression {
    return this.args.every;
  }
}

export type RollupIndexExprArgs = Merge<[
  BaseExpressionArgs,
  {
    fromIndex?: Expression;
    properties?: Expression[];
    this: Expression;
    expressions: Expression[];
  },
]>;

export class RollupIndexExpr extends Expression {
  key = ExpressionKey.ROLLUP_INDEX;

  /**
   * Defines the arguments (properties and child expressions) for RollupIndex expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<RollupIndexExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
    fromIndex: false,
    properties: false,
  };

  declare args: RollupIndexExprArgs;

  constructor (args: RollupIndexExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $fromIndex (): Expression | undefined {
    return this.args.fromIndex;
  }

  get $properties (): Expression[] | undefined {
    return this.args.properties;
  }
}

export type PartitionListExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expressions: Expression[];
  },
]>;

export class PartitionListExpr extends Expression {
  key = ExpressionKey.PARTITION_LIST;

  static argTypes: RequiredMap<PartitionListExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
  };

  declare args: PartitionListExprArgs;

  constructor (args: PartitionListExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type PartitionBoundSpecExprArgs = Merge<[
  BaseExpressionArgs,
  {
    fromExpressions?: Expression[];
    toExpressions?: Expression[];
    this?: Expression;
    expression?: Expression;
  },
]>;

export class PartitionBoundSpecExpr extends Expression {
  key = ExpressionKey.PARTITION_BOUND_SPEC;

  /**
   * Defines the arguments (properties and child expressions) for PartitionBoundSpec expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<PartitionBoundSpecExprArgs> = {
    ...super.argTypes,
    this: false,
    expression: false,
    fromExpressions: false,
    toExpressions: false,
  };

  declare args: PartitionBoundSpecExprArgs;

  constructor (args: PartitionBoundSpecExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $fromExpressions (): Expression[] | undefined {
    return this.args.fromExpressions;
  }

  get $toExpressions (): Expression[] | undefined {
    return this.args.toExpressions;
  }
}

export type QueryTransformExprArgs = Merge<[
  BaseExpressionArgs,
  {
    commandScript: Expression;
    schema?: Expression;
    rowFormatBefore?: string;
    recordWriter?: Expression;
    rowFormatAfter?: string;
    recordReader?: Expression;
  },
]>;

export class QueryTransformExpr extends Expression {
  key = ExpressionKey.QUERY_TRANSFORM;

  /**
   * Defines the arguments (properties and child expressions) for QueryTransform expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<QueryTransformExprArgs> = {
    ...super.argTypes,
    commandScript: true,
    schema: false,
    rowFormatBefore: false,
    recordWriter: false,
    rowFormatAfter: false,
    recordReader: false,
  };

  declare args: QueryTransformExprArgs;

  constructor (args: QueryTransformExprArgs) {
    super(args);
  }

  get $commandScript (): Expression {
    return this.args.commandScript;
  }

  get $schema (): Expression | undefined {
    return this.args.schema;
  }

  get $rowFormatBefore (): string | undefined {
    return this.args.rowFormatBefore;
  }

  get $recordWriter (): Expression | undefined {
    return this.args.recordWriter;
  }

  get $rowFormatAfter (): string | undefined {
    return this.args.rowFormatAfter;
  }

  get $recordReader (): Expression | undefined {
    return this.args.recordReader;
  }
}

export type SemanticViewExprArgs = Merge<[
  BaseExpressionArgs,
  {
    metrics?: Expression[];
    dimensions?: Expression[];
    facts?: Expression[];
    where?: Expression;
  },
]>;

export class SemanticViewExpr extends Expression {
  key = ExpressionKey.SEMANTIC_VIEW;

  /**
   * Defines the arguments (properties and child expressions) for SemanticView expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<SemanticViewExprArgs> = {
    ...super.argTypes,
    metrics: false,
    dimensions: false,
    facts: false,
    where: false,
  };

  declare args: SemanticViewExprArgs;

  constructor (args: SemanticViewExprArgs) {
    super(args);
  }

  get $metrics (): Expression[] | undefined {
    return this.args.metrics;
  }

  get $dimensions (): Expression[] | undefined {
    return this.args.dimensions;
  }

  get $facts (): Expression[] | undefined {
    return this.args.facts;
  }

  get $where (): Expression | undefined {
    return this.args.where;
  }
}

export type LocationExprArgs = Merge<[
  BaseExpressionArgs,
]>;
export class LocationExpr extends Expression {
  key = ExpressionKey.LOCATION;

  static argTypes: RequiredMap<LocationExprArgs> = {};

  declare args: LocationExprArgs;

  constructor (args: LocationExprArgs) {
    super(args);
  }
}

export type QualifyExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class QualifyExpr extends Expression {
  key = ExpressionKey.QUALIFY;

  static argTypes: RequiredMap<QualifyExprArgs> = {
    ...super.argTypes,
  };

  declare args: QualifyExprArgs;

  constructor (args: QualifyExprArgs) {
    super(args);
  }
}

export type InputOutputFormatExprArgs = Merge<[
  BaseExpressionArgs,
  {
    inputFormat?: string;
    outputFormat?: string;
  },
]>;

export class InputOutputFormatExpr extends Expression {
  key = ExpressionKey.INPUT_OUTPUT_FORMAT;

  /**
   * Defines the arguments (properties and child expressions) for InputOutputFormat expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<InputOutputFormatExprArgs> = {
    ...super.argTypes,
    inputFormat: false,
    outputFormat: false,
  };

  declare args: InputOutputFormatExprArgs;

  constructor (args: InputOutputFormatExprArgs) {
    super(args);
  }

  get $inputFormat (): string | undefined {
    return this.args.inputFormat;
  }

  get $outputFormat (): string | undefined {
    return this.args.outputFormat;
  }
}

export type ReturnExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class ReturnExpr extends Expression {
  key = ExpressionKey.RETURN;

  static argTypes: RequiredMap<ReturnExprArgs> = {
    ...super.argTypes,
  };

  declare args: ReturnExprArgs;

  constructor (args: ReturnExprArgs) {
    super(args);
  }
}

export type ReferenceExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expressions?: Expression[];
    options?: Expression[];
  },
]>;

export class ReferenceExpr extends Expression {
  key = ExpressionKey.REFERENCE;

  static argTypes: RequiredMap<ReferenceExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
    options: false,
  };

  declare args: ReferenceExprArgs;

  constructor (args: ReferenceExprArgs) {
    super(args);
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

export type TupleExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions?: Expression[] },
]>;

export class TupleExpr extends Expression {
  key = ExpressionKey.TUPLE;

  static argTypes: RequiredMap<TupleExprArgs> = {
    ...super.argTypes,
    expressions: false,
  };

  declare args: TupleExprArgs;

  constructor (args: TupleExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  in (
    expressions: unknown[],
    query?: Expression | string,
    options: {
      unnest?: Expression | string | (Expression | string)[];
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): InExpr {
    const {
      copy = true, unnest, ...restOptions
    } = options;
    return new InExpr({
      this: maybeCopy(this, copy),
      expressions: expressions.map((e) => convert(e, copy)),
      query: query
        ? maybeParse(query, {
          ...restOptions,
          copy,
        })
        : undefined,
      unnest: unnest
        ? new UnnestExpr({
          expressions: ensureList(unnest).map((e) => maybeParse(e, {
            ...restOptions,
            copy,
          })),
        })
        : undefined,
    });
  }
}

export const QUERY_MODIFIERS = {
  match: false,
  laterals: false,
  joins: false,
  connect: false,
  pivots: false,
  prewhere: false,
  where: false,
  group: false,
  having: false,
  qualify: false,
  windows: false,
  distribute: false,
  sort: false,
  cluster: false,
  order: false,
  limit: false,
  offset: false,
  locks: false,
  sample: false,
  settings: false,
  format: false,
  options: false,
} as const;

// https://learn.microsoft.com/en-us/sql/t-sql/queries/option-clause-transact-sql?view=sql-server-ver16
// https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query?view=sql-server-ver16
export type QueryOptionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class QueryOptionExpr extends Expression {
  key = ExpressionKey.QUERY_OPTION;

  static argTypes: RequiredMap<QueryOptionExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: QueryOptionExprArgs;

  constructor (args: QueryOptionExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }
}

// https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-ver16
export type WithTableHintExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions: Expression[] },
]>;

export class WithTableHintExpr extends Expression {
  key = ExpressionKey.WITH_TABLE_HINT;

  static argTypes: RequiredMap<WithTableHintExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: WithTableHintExprArgs;

  constructor (args: WithTableHintExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

// https://dev.mysql.com/doc/refman/8.0/en/index-hints.html
export type IndexTableHintExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: string;
    expressions?: Expression[];
    target?: string;
  },
]>;

export class IndexTableHintExpr extends Expression {
  key = ExpressionKey.INDEX_TABLE_HINT;

  static argTypes: RequiredMap<IndexTableHintExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
    target: false,
  };

  declare args: IndexTableHintExprArgs;

  constructor (args: IndexTableHintExprArgs) {
    super(args);
  }

  get $this (): string {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $target (): string | undefined {
    return this.args.target;
  }
}

/**
 * Enumeration of valid kind values for HistoricalData expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum HistoricalDataExprKind {
  SYSTEM_TIME = 'SYSTEM_TIME',
  TRANSACTION_TIME = 'TRANSACTION_TIME',
}

// https://docs.snowflake.com/en/sql-reference/constructs/at-before
export type HistoricalDataExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    kind: HistoricalDataExprKind;
    expression: Expression;
  },
]>;

export class HistoricalDataExpr extends Expression {
  key = ExpressionKey.HISTORICAL_DATA;

  static argTypes: RequiredMap<HistoricalDataExprArgs> = {
    ...super.argTypes,
    this: true,
    kind: true,
    expression: true,
  };

  declare args: HistoricalDataExprArgs;

  constructor (args: HistoricalDataExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): HistoricalDataExprKind {
    return this.args.kind;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

// https://docs.snowflake.com/en/sql-reference/sql/put
export type PutExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    target: Expression;
    properties?: Expression[];
  },
]>;

export class PutExpr extends Expression {
  key = ExpressionKey.PUT;

  static argTypes: RequiredMap<PutExprArgs> = {
    ...super.argTypes,
    this: true,
    target: true,
    properties: false,
  };

  declare args: PutExprArgs;

  constructor (args: PutExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $target (): Expression {
    return this.args.target;
  }

  get $properties (): Expression[] | undefined {
    return this.args.properties;
  }
}

// https://docs.snowflake.com/en/sql-reference/sql/get
export type GetExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    target: Expression;
    properties?: Expression[];
  },
]>;

export class GetExpr extends Expression {
  key = ExpressionKey.GET;

  static argTypes: RequiredMap<GetExprArgs> = {
    ...super.argTypes,
    this: true,
    target: true,
    properties: false,
  };

  declare args: GetExprArgs;

  constructor (args: GetExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $target (): Expression {
    return this.args.target;
  }

  get $properties (): Expression[] | undefined {
    return this.args.properties;
  }
}

export type TableExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: IdentifierExpr | DotExpr;
    db?: IdentifierExpr;
    catalog?: IdentifierExpr;
    alias?: TableAliasExpr | IdentifierExpr;
    laterals?: Expression[];
    joins?: JoinExpr[];
    pivots?: Expression[];
    hints?: Expression[];
    systemTime?: Expression;
    version?: Expression;
    format?: string;
    pattern?: Expression;
    ordinality?: boolean;
    when?: Expression;
    only?: boolean;
    partition?: Expression;
    changes?: Expression[];
    rowsFrom?: number | Expression;
    sample?: number | Expression;
    indexed?: Expression;
  },
]>;

export class TableExpr extends Expression {
  key = ExpressionKey.TABLE;

  /**
   * Defines the arguments (properties and child expressions) for Table expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   *
   * @see {@link https://docs.sqlglot.com/sqlglot/expressions.html#Table | SQLGlot Table Documentation}
   */
  static argTypes: RequiredMap<TableExprArgs> = {
    ...super.argTypes,
    this: false,
    alias: false,
    db: false,
    catalog: false,
    laterals: false,
    joins: false,
    pivots: false,
    hints: false,
    systemTime: false,
    version: false,
    format: false,
    pattern: false,
    ordinality: false,
    when: false,
    only: false,
    partition: false,
    changes: false,
    rowsFrom: false,
    sample: false,
    indexed: false,
  };

  declare args: TableExprArgs;

  constructor (args: TableExprArgs) {
    super(args);
  }

  get $this (): IdentifierExpr | DotExpr | undefined {
    return this.args.this;
  }

  get $alias (): TableAliasExpr | IdentifierExpr | undefined {
    return this.args.alias;
  }

  get $db (): IdentifierExpr | undefined {
    return this.args.db;
  }

  get $catalog (): IdentifierExpr | undefined {
    return this.args.catalog;
  }

  get $laterals (): Expression[] | undefined {
    return this.args.laterals;
  }

  get $joins (): JoinExpr[] | undefined {
    return this.args.joins;
  }

  get $pivots (): Expression[] | undefined {
    return this.args.pivots;
  }

  get $hints (): Expression[] | undefined {
    return this.args.hints;
  }

  get $systemTime (): Expression | undefined {
    return this.args.systemTime;
  }

  get $version (): Expression | undefined {
    return this.args.version;
  }

  get $format (): string | undefined {
    return this.args.format;
  }

  get $pattern (): Expression | undefined {
    return this.args.pattern;
  }

  get $ordinality (): boolean | undefined {
    return this.args.ordinality;
  }

  get $when (): Expression | undefined {
    return this.args.when;
  }

  get $only (): boolean | undefined {
    return this.args.only;
  }

  get $partition (): Expression | undefined {
    return this.args.partition;
  }

  get $changes (): Expression[] | undefined {
    return this.args.changes;
  }

  get $rowsFrom (): number | Expression | undefined {
    return this.args.rowsFrom;
  }

  get $sample (): number | Expression | undefined {
    return this.args.sample;
  }

  get $indexed (): Expression | undefined {
    return this.args.indexed;
  }

  /**
   * Returns the name of the table.
   * If `this` is missing or is a Func, returns empty string.
   */
  get name (): string {
    const thisArg = this.args.this;
    if (!thisArg || thisArg instanceof FuncExpr) {
      return '';
    }
    return thisArg.name || '';
  }

  /**
   * Returns the database name as a string.
   */
  get db (): string {
    return this.text('db');
  }

  /**
   * Returns the catalog name as a string.
   */
  get catalog (): string {
    return this.text('catalog');
  }

  /**
   * Returns all Select expressions that reference this table.
   */
  get selects (): Expression[] {
    return [];
  }

  /**
   * Returns a list of named selects.
   */
  get namedSelects (): string[] {
    return [];
  }

  /**
   * Returns the parts of a table in order: [catalog, db, this].
   * Flattens Dot expressions into their constituent parts.
   */
  get parts (): IdentifierExpr[] | [...Expression[], ColumnExpr] {
    const parts: Expression[] = [];

    for (const arg of [
      'catalog',
      'db',
      'this',
    ] as const) {
      const part = this.args[arg];

      if (part instanceof DotExpr) {
        parts.push(...part.flatten());
      } else if (part instanceof IdentifierExpr) {
        parts.push(part);
      }
    }

    return parts as IdentifierExpr[] | [...Expression[], ColumnExpr];
  }

  /**
   * Converts this table to a Column expression.
   */
  toColumn (options: { copy?: boolean } = {}): ColumnExpr | DotExpr | AliasExpr {
    const { copy = true } = options;

    const parts = this.parts;
    const lastPart = parts[parts.length - 1];

    let col: ColumnExpr | DotExpr | AliasExpr;
    if (lastPart instanceof IdentifierExpr) {
      // Build column from parts (reversed for catalog.db.table order)
      const columnParts = parts.slice(0, 4).reverse();
      const fields = parts.slice(4);
      col = column({
        col: columnParts[0] as IdentifierExpr,
        table: columnParts[1] as IdentifierExpr | undefined,
        db: columnParts[2] as IdentifierExpr | undefined,
        catalog: columnParts[3] as IdentifierExpr | undefined,
      }, {
        fields: fields as IdentifierExpr[],
        copy,
      });
    } else {
      // If last part is a function or array wrapped in Table
      col = lastPart as ColumnExpr;
    }

    const aliasArg = this.args.alias;
    if (aliasArg) {
      col = alias(col, aliasArg.$this, { copy });
    }

    return col;
  }
}

export type VarExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class VarExpr extends Expression {
  key = ExpressionKey.VAR;

  static argTypes: RequiredMap<VarExprArgs> = {
    ...super.argTypes,
  };

  declare args: VarExprArgs;

  constructor (args: VarExprArgs) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for Version expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum VersionExprKind {
  SNAPSHOT = 'SNAPSHOT',
  TIMESTAMP = 'TIMESTAMP',
  VERSION = 'VERSION',
}

/**
 * Time travel expressions for Iceberg, BigQuery, DuckDB, etc.
 * @see {@link https://trino.io/docs/current/connector/iceberg.html | Trino Iceberg}
 * @see {@link https://www.databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html | Delta Time Travel}
 * @see {@link https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#for_system_time_as_of | BigQuery System Time}
 * @see {@link https://learn.microsoft.com/en-us/sql/relational-databases/tables/querying-data-in-a-system-versioned-temporal-table | SQL Server Temporal Tables}
 */
export type VersionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    kind: VersionExprKind;
    expression?: Expression;
  },
]>;

export class VersionExpr extends Expression {
  key = ExpressionKey.VERSION;

  static argTypes: RequiredMap<VersionExprArgs> = {
    ...super.argTypes,
    this: true,
    kind: true,
    expression: false,
  };

  declare args: VersionExprArgs;

  constructor (args: VersionExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): VersionExprKind {
    return this.args.kind;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }
}

export type SchemaExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class SchemaExpr extends Expression {
  key = ExpressionKey.SCHEMA;

  static argTypes: RequiredMap<SchemaExprArgs> = {
    ...super.argTypes,
    this: false,
    expressions: false,
  };

  declare args: SchemaExprArgs;

  constructor (args: SchemaExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

/**
 * Lock expressions for SELECT ... FOR UPDATE
 * @see {@link https://dev.mysql.com/doc/refman/8.0/en/select.html | MySQL SELECT}
 * @see {@link https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SELECT.html | Oracle SELECT}
 */
export type LockExprArgs = Merge<[
  BaseExpressionArgs,
  {
    update: Expression;
    expressions?: Expression[];
    wait?: Expression;
    key?: Expression;
  },
]>;

export class LockExpr extends Expression {
  key = ExpressionKey.LOCK;

  static argTypes: RequiredMap<LockExprArgs> = {
    ...super.argTypes,
    update: true,
    expressions: false,
    wait: false,
    key: false,
  };

  declare args: LockExprArgs;

  constructor (args: LockExprArgs) {
    super(args);
  }

  get $update (): Expression {
    return this.args.update;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $wait (): Expression | undefined {
    return this.args.wait;
  }

  get $key (): Expression | undefined {
    return this.args.key;
  }
}

export type TableSampleExprArgs = Merge<[
  BaseExpressionArgs,
  {
    expressions?: Expression[];
    method?: string;
    bucketNumerator?: Expression;
    bucketDenominator?: Expression;
    bucketField?: Expression;
    percent?: Expression;
    rows?: Expression[];
    size?: number | Expression;
    seed?: Expression;
  },
]>;

export class TableSampleExpr extends Expression {
  key = ExpressionKey.TABLE_SAMPLE;

  /**
   * Defines the arguments (properties and child expressions) for TableSample expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<TableSampleExprArgs> = {
    ...super.argTypes,
    expressions: false,
    method: false,
    bucketNumerator: false,
    bucketDenominator: false,
    bucketField: false,
    percent: false,
    rows: false,
    size: false,
    seed: false,
  };

  declare args: TableSampleExprArgs;

  constructor (args: TableSampleExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $method (): string | undefined {
    return this.args.method;
  }

  get $bucketNumerator (): Expression | undefined {
    return this.args.bucketNumerator;
  }

  get $bucketDenominator (): Expression | undefined {
    return this.args.bucketDenominator;
  }

  get $bucketField (): Expression | undefined {
    return this.args.bucketField;
  }

  get $percent (): Expression | undefined {
    return this.args.percent;
  }

  get $rows (): Expression[] | undefined {
    return this.args.rows;
  }

  get $size (): number | Expression | undefined {
    return this.args.size;
  }

  get $seed (): Expression | undefined {
    return this.args.seed;
  }
}

export type TagExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    prefix?: Expression;
    postfix?: Expression;
  },
]>;

export class TagExpr extends Expression {
  key = ExpressionKey.TAG;

  /**
   * Defines the arguments (properties and child expressions) for Tag expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<TagExprArgs> = {
    ...super.argTypes,
    this: false,
    prefix: false,
    postfix: false,
  };

  declare args: TagExprArgs;

  constructor (args: TagExprArgs) {
    super(args);
  }

  get this (): Expression | undefined {
    return this.args.this;
  }

  get $prefix (): Expression | undefined {
    return this.args.prefix;
  }

  get $postfix (): Expression | undefined {
    return this.args.postfix;
  }
}

export type PivotExprArgs = Merge<[
  BaseExpressionArgs,
  {
    fields?: Expression[];
    unpivot?: boolean;
    using?: string;
    group?: Expression;
    columns?: Expression[];
    includeNulls?: Expression[];
    defaultOnNull?: Expression;
    into?: Expression;
    with?: WithExpr;
  },
]>;

export class PivotExpr extends Expression {
  key = ExpressionKey.PIVOT;

  /**
   * Defines the arguments (properties and child expressions) for Pivot expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<PivotExprArgs> = {
    ...super.argTypes,
    fields: false,
    unpivot: false,
    using: false,
    group: false,
    columns: false,
    includeNulls: false,
    defaultOnNull: false,
    into: false,
    with: false,
  };

  declare args: PivotExprArgs;

  constructor (args: PivotExprArgs) {
    super(args);
  }

  get $fields (): Expression[] | undefined {
    return this.args.fields;
  }

  get $unpivot (): boolean | undefined {
    return this.args.unpivot;
  }

  get $using (): string | undefined {
    return this.args.using;
  }

  get $group (): Expression | undefined {
    return this.args.group;
  }

  get $columns (): Expression[] | undefined {
    return this.args.columns;
  }

  get $includeNulls (): Expression[] | undefined {
    return this.args.includeNulls;
  }

  get $defaultOnNull (): Expression | undefined {
    return this.args.defaultOnNull;
  }

  get $into (): Expression | undefined {
    return this.args.into;
  }

  get $with (): WithExpr | undefined {
    return this.args.with;
  }

  /**
   * Returns true if this is an UNPIVOT operation.
   */
  get unpivot (): boolean {
    return !!this.args.unpivot;
  }

  /**
   * Returns the pivot fields.
   */
  get fields (): Expression[] {
    return this.args.fields || [];
  }
}

export type UnpivotColumnsExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expressions: Expression[];
  },
]>;

export class UnpivotColumnsExpr extends Expression {
  key = ExpressionKey.UNPIVOT_COLUMNS;

  static argTypes: RequiredMap<UnpivotColumnsExprArgs> = {
    this: true,
    expressions: true,
  };

  declare args: UnpivotColumnsExprArgs;

  constructor (args: UnpivotColumnsExprArgs) {
    super(args);
  }
}

/**
 * Valid kind values for window frame specifications
 */
export enum WindowSpecExprKind {
  ROWS = 'ROWS',
  RANGE = 'RANGE',
  GROUPS = 'GROUPS',
}

export type WindowSpecExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: WindowSpecExprKind;
    start?: Expression;
    startSide?: Expression;
    end?: Expression;
    endSide?: Expression;
    exclude?: Expression;
  },
]>;

export class WindowSpecExpr extends Expression {
  key = ExpressionKey.WINDOW_SPEC;

  /**
   * Defines the arguments (properties and child expressions) for WindowSpec expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<WindowSpecExprArgs> = {
    ...super.argTypes,
    kind: false,
    start: false,
    startSide: false,
    end: false,
    endSide: false,
    exclude: false,
  };

  declare args: WindowSpecExprArgs;

  constructor (args: WindowSpecExprArgs) {
    super(args);
  }

  get $kind (): WindowSpecExprKind | undefined {
    return this.args.kind;
  }

  get $start (): Expression | undefined {
    return this.args.start;
  }

  get $startSide (): Expression | undefined {
    return this.args.startSide;
  }

  get $end (): Expression | undefined {
    return this.args.end;
  }

  get $endSide (): Expression | undefined {
    return this.args.endSide;
  }

  get $exclude (): Expression | undefined {
    return this.args.exclude;
  }
}

export type PreWhereExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class PreWhereExpr extends Expression {
  key = ExpressionKey.PRE_WHERE;

  static argTypes: RequiredMap<PreWhereExprArgs> = {
    ...super.argTypes,
  };

  declare args: PreWhereExprArgs;

  constructor (args: PreWhereExprArgs) {
    super(args);
  }
}

export type WhereExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: string | Expression; // NOTE: sqlglot does not have this, but based on Subquery.where(), I added this;
  },
]>;

export class WhereExpr extends Expression {
  key = ExpressionKey.WHERE;

  static argTypes: RequiredMap<WhereExprArgs> = {
    ...super.argTypes,
    this: true, // NOTE: sqlglot does not have this, but based on Subquery.where(), I added this
  };

  declare args: WhereExprArgs;

  constructor (args: WhereExprArgs) {
    super(args);
  }

  get $this (): string | Expression {
    return this.args.this;
  }
}

export type StarExprArgs = Merge<[
  BaseExpressionArgs,
  {
    except?: Expression;
    replace?: boolean;
    rename?: string;
  },
]>;

export class StarExpr extends Expression {
  key = ExpressionKey.STAR;

  /**
   * Defines the arguments (properties and child expressions) for Star expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<StarExprArgs> = {
    ...super.argTypes,
    except: false,
    replace: false,
    rename: false,
  };

  declare args: StarExprArgs;

  constructor (args: StarExprArgs) {
    super(args);
  }

  get $except (): Expression | undefined {
    return this.args.except;
  }

  get $replace (): boolean | undefined {
    return this.args.replace;
  }

  get $rename (): string | undefined {
    return this.args.rename;
  }

  /**
   * Returns the name of this star expression.
   */
  get name (): string {
    return '*';
  }

  /**
   * Returns the output name of this star expression.
   */
  get outputName (): string {
    return this.name;
  }
}

export type DataTypeParamExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class DataTypeParamExpr extends Expression {
  key = ExpressionKey.DATA_TYPE_PARAM;

  static argTypes: RequiredMap<DataTypeParamExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: DataTypeParamExprArgs;

  constructor (args: DataTypeParamExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  /**
   * Returns the name from the 'this' expression.
   */
  get name (): string {
    return this.args.this.name;
  }
}

/**
 * Valid kind values for DataType expressions (SQL data types)
 */
export enum DataTypeExprKind {
  ARRAY = 'ARRAY',
  AGGREGATEFUNCTION = 'AGGREGATEFUNCTION',
  SIMPLEAGGREGATEFUNCTION = 'SIMPLEAGGREGATEFUNCTION',
  BIGDECIMAL = 'BIGDECIMAL',
  BIGINT = 'BIGINT',
  BIGNUM = 'BIGNUM',
  BIGSERIAL = 'BIGSERIAL',
  BINARY = 'BINARY',
  BIT = 'BIT',
  BLOB = 'BLOB',
  BOOLEAN = 'BOOLEAN',
  BPCHAR = 'BPCHAR',
  CHAR = 'CHAR',
  DATE = 'DATE',
  DATE32 = 'DATE32',
  DATEMULTIRANGE = 'DATEMULTIRANGE',
  DATERANGE = 'DATERANGE',
  DATETIME = 'DATETIME',
  DATETIME2 = 'DATETIME2',
  DATETIME64 = 'DATETIME64',
  DECIMAL = 'DECIMAL',
  DECIMAL32 = 'DECIMAL32',
  DECIMAL64 = 'DECIMAL64',
  DECIMAL128 = 'DECIMAL128',
  DECIMAL256 = 'DECIMAL256',
  DECFLOAT = 'DECFLOAT',
  DOUBLE = 'DOUBLE',
  DYNAMIC = 'DYNAMIC',
  ENUM = 'ENUM',
  ENUM8 = 'ENUM8',
  ENUM16 = 'ENUM16',
  FILE = 'FILE',
  FIXEDSTRING = 'FIXEDSTRING',
  FLOAT = 'FLOAT',
  GEOGRAPHY = 'GEOGRAPHY',
  GEOGRAPHYPOINT = 'GEOGRAPHYPOINT',
  GEOMETRY = 'GEOMETRY',
  POINT = 'POINT',
  RING = 'RING',
  LINESTRING = 'LINESTRING',
  MULTILINESTRING = 'MULTILINESTRING',
  POLYGON = 'POLYGON',
  MULTIPOLYGON = 'MULTIPOLYGON',
  HLLSKETCH = 'HLLSKETCH',
  HSTORE = 'HSTORE',
  IMAGE = 'IMAGE',
  INET = 'INET',
  INT = 'INT',
  INT128 = 'INT128',
  INT256 = 'INT256',
  INT4MULTIRANGE = 'INT4MULTIRANGE',
  INT4RANGE = 'INT4RANGE',
  INT8MULTIRANGE = 'INT8MULTIRANGE',
  INT8RANGE = 'INT8RANGE',
  INTERVAL = 'INTERVAL',
  IPADDRESS = 'IPADDRESS',
  IPPREFIX = 'IPPREFIX',
  IPV4 = 'IPV4',
  IPV6 = 'IPV6',
  JSON = 'JSON',
  JSONB = 'JSONB',
  LIST = 'LIST',
  LONGBLOB = 'LONGBLOB',
  LONGTEXT = 'LONGTEXT',
  LOWCARDINALITY = 'LOWCARDINALITY',
  MAP = 'MAP',
  MEDIUMBLOB = 'MEDIUMBLOB',
  MEDIUMINT = 'MEDIUMINT',
  MEDIUMTEXT = 'MEDIUMTEXT',
  MONEY = 'MONEY',
  NAME = 'NAME',
  NCHAR = 'NCHAR',
  NESTED = 'NESTED',
  NOTHING = 'NOTHING',
  NULL = 'NULL',
  NUMMULTIRANGE = 'NUMMULTIRANGE',
  NUMRANGE = 'NUMRANGE',
  NVARCHAR = 'NVARCHAR',
  OBJECT = 'OBJECT',
  RANGE = 'RANGE',
  ROWVERSION = 'ROWVERSION',
  SERIAL = 'SERIAL',
  SET = 'SET',
  SMALLDATETIME = 'SMALLDATETIME',
  SMALLINT = 'SMALLINT',
  SMALLMONEY = 'SMALLMONEY',
  SMALLSERIAL = 'SMALLSERIAL',
  STRUCT = 'STRUCT',
  SUPER = 'SUPER',
  TEXT = 'TEXT',
  TINYBLOB = 'TINYBLOB',
  TINYTEXT = 'TINYTEXT',
  TIME = 'TIME',
  TIMETZ = 'TIMETZ',
  TIME_NS = 'TIME_NS',
  TIMESTAMP = 'TIMESTAMP',
  TIMESTAMPNTZ = 'TIMESTAMPNTZ',
  TIMESTAMPLTZ = 'TIMESTAMPLTZ',
  TIMESTAMPTZ = 'TIMESTAMPTZ',
  TIMESTAMP_S = 'TIMESTAMP_S',
  TIMESTAMP_MS = 'TIMESTAMP_MS',
  TIMESTAMP_NS = 'TIMESTAMP_NS',
  TINYINT = 'TINYINT',
  TSMULTIRANGE = 'TSMULTIRANGE',
  TSRANGE = 'TSRANGE',
  TSTZMULTIRANGE = 'TSTZMULTIRANGE',
  TSTZRANGE = 'TSTZRANGE',
  UBIGINT = 'UBIGINT',
  UINT = 'UINT',
  UINT128 = 'UINT128',
  UINT256 = 'UINT256',
  UMEDIUMINT = 'UMEDIUMINT',
  UDECIMAL = 'UDECIMAL',
  UDOUBLE = 'UDOUBLE',
  UNION = 'UNION',
  UNKNOWN = 'UNKNOWN',
  USERDEFINED = 'USER-DEFINED',
  USMALLINT = 'USMALLINT',
  UTINYINT = 'UTINYINT',
  UUID = 'UUID',
  VARBINARY = 'VARBINARY',
  VARCHAR = 'VARCHAR',
  VARIANT = 'VARIANT',
  VECTOR = 'VECTOR',
  XML = 'XML',
  YEAR = 'YEAR',
  TDIGEST = 'TDIGEST',
}

export type DataTypeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: DataTypeExprKind;
    expressions?: Expression[];
    nested?: boolean;
    values?: Expression[];
    prefix?: boolean | string;
    kind?: DataTypeExprKind | DotExpr | IdentifierExpr;
    nullable?: Expression;
  },
]>;

export class DataTypeExpr extends Expression {
  key = ExpressionKey.DATA_TYPE;

  /**
   * Defines the arguments (properties and child expressions) for DataType expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<DataTypeExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
    nested: false,
    values: false,
    prefix: false,
    kind: false,
    nullable: false,
  };

  static STRUCT_TYPES = new Set<DataTypeExprKind>([
    DataTypeExprKind.FILE,
    DataTypeExprKind.NESTED,
    DataTypeExprKind.OBJECT,
    DataTypeExprKind.STRUCT,
    DataTypeExprKind.UNION,
  ]);

  static ARRAY_TYPES = new Set<DataTypeExprKind>([DataTypeExprKind.ARRAY, DataTypeExprKind.LIST]);

  static NESTED_TYPES = new Set<DataTypeExprKind>([
    ...DataTypeExpr.STRUCT_TYPES,
    ...DataTypeExpr.ARRAY_TYPES,
    DataTypeExprKind.MAP,
  ]);

  static TEXT_TYPES = new Set<DataTypeExprKind>([
    DataTypeExprKind.CHAR,
    DataTypeExprKind.NCHAR,
    DataTypeExprKind.NVARCHAR,
    DataTypeExprKind.TEXT,
    DataTypeExprKind.VARCHAR,
    DataTypeExprKind.NAME,
  ]);

  static SIGNED_INTEGER_TYPES = new Set<DataTypeExprKind>([
    DataTypeExprKind.BIGINT,
    DataTypeExprKind.INT,
    DataTypeExprKind.INT128,
    DataTypeExprKind.INT256,
    DataTypeExprKind.MEDIUMINT,
    DataTypeExprKind.SMALLINT,
    DataTypeExprKind.TINYINT,
  ]);

  static UNSIGNED_INTEGER_TYPES = new Set<DataTypeExprKind>([
    DataTypeExprKind.UBIGINT,
    DataTypeExprKind.UINT,
    DataTypeExprKind.UINT128,
    DataTypeExprKind.UINT256,
    DataTypeExprKind.UMEDIUMINT,
    DataTypeExprKind.USMALLINT,
    DataTypeExprKind.UTINYINT,
  ]);

  static INTEGER_TYPES = new Set<DataTypeExprKind>([
    ...DataTypeExpr.SIGNED_INTEGER_TYPES,
    ...DataTypeExpr.UNSIGNED_INTEGER_TYPES,
    DataTypeExprKind.BIT,
  ]);

  static FLOAT_TYPES = new Set<DataTypeExprKind>([DataTypeExprKind.DOUBLE, DataTypeExprKind.FLOAT]);

  static REAL_TYPES = new Set<DataTypeExprKind>([
    ...DataTypeExpr.FLOAT_TYPES,
    DataTypeExprKind.BIGDECIMAL,
    DataTypeExprKind.DECIMAL,
    DataTypeExprKind.DECIMAL32,
    DataTypeExprKind.DECIMAL64,
    DataTypeExprKind.DECIMAL128,
    DataTypeExprKind.DECIMAL256,
    DataTypeExprKind.DECFLOAT,
    DataTypeExprKind.MONEY,
    DataTypeExprKind.SMALLMONEY,
    DataTypeExprKind.UDECIMAL,
    DataTypeExprKind.UDOUBLE,
  ]);

  static NUMERIC_TYPES = new Set<DataTypeExprKind>([...DataTypeExpr.INTEGER_TYPES, ...DataTypeExpr.REAL_TYPES]);

  static TEMPORAL_TYPES = new Set<DataTypeExprKind>([
    DataTypeExprKind.DATE,
    DataTypeExprKind.DATE32,
    DataTypeExprKind.DATETIME,
    DataTypeExprKind.DATETIME2,
    DataTypeExprKind.DATETIME64,
    DataTypeExprKind.SMALLDATETIME,
    DataTypeExprKind.TIME,
    DataTypeExprKind.TIMESTAMP,
    DataTypeExprKind.TIMESTAMPNTZ,
    DataTypeExprKind.TIMESTAMPLTZ,
    DataTypeExprKind.TIMESTAMPTZ,
    DataTypeExprKind.TIMESTAMP_MS,
    DataTypeExprKind.TIMESTAMP_NS,
    DataTypeExprKind.TIMESTAMP_S,
    DataTypeExprKind.TIMETZ,
  ]);

  declare args: DataTypeExprArgs;

  constructor (args: DataTypeExprArgs) {
    super(args);
  }

  /**
   * Constructs a DataTypeExpr object.
   *
   * @param dtype - The data type of interest.
   * @param dialect - The dialect to use for parsing dtype, in case it's a string.
   * @param udt - When set to true, dtype will be used as-is if it can't be parsed into a DataTypeExpr, thus creating a user-defined type.
   * @param copy - Whether to copy the data type.
   * @param kwargs - Additional arguments to pass in the constructor of DataTypeExpr.
   * @returns The constructed DataTypeExpr object.
   */
  static build (
    dtype: DataTypeExprKind | DataTypeExpr | IdentifierExpr | DotExpr,
    options: {
      dialect?: DialectType;
      udt?: boolean;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): DataTypeExpr {
    const {
      udt = false, copy = true, dialect, ...kwargs
    } = options;

    let _dataTypeExp;

    if (typeof dtype === 'string') {
      if (dtype === DataTypeExprKind.UNKNOWN) {
        return new DataTypeExpr({
          ...kwargs,
          this: DataTypeExprKind.UNKNOWN,
        });
      }

      try {
        _dataTypeExp = parseOne(dtype, {
          read: dialect,
          into: DataTypeExpr,
          errorLevel: ErrorLevel.IGNORE,
        });
      } catch (e) {
        if (!(e instanceof ParseError)) {
          throw e;
        }
        if (udt) {
          return new DataTypeExpr({
            ...options,
            this: DataTypeExprKind.USERDEFINED,
            kind: dtype,
          });
        }
      }
    }

    if ((dtype instanceof IdentifierExpr || dtype instanceof DotExpr) && udt) {
      return new DataTypeExpr({
        ...kwargs,
        this: DataTypeExprKind.USERDEFINED,
        kind: dtype,
      });
    }

    if (typeof dtype === 'string' && Object.values(DataTypeExprKind).includes(dtype as DataTypeExprKind)) {
      return new DataTypeExpr({
        ...kwargs,
        this: dtype,
      });
    }

    if (dtype instanceof DataTypeExpr) {
      return maybeCopy(dtype, copy);
    }

    throw new Error(`Invalid data type: ${typeof dtype}. Expected string, DataTypeExprKind, or DataTypeExpr`);

    // There's a return here in sqlglot but unreachable
  }

  /**
   * Checks whether this DataType matches one of the provided data types. Nested types or precision
   * will be compared using "structural equivalence" semantics, so e.g. array<int> != array<float>.
   *
   * @param dtypes - The data types to compare this DataType to.
   * @param options - Options for the comparison.
   * @param options.checkNullable - Whether to take the NULLABLE type constructor into account for the comparison.
   *                                 If false, it means that NULLABLE<INT> is equivalent to INT.
   * @returns True, if and only if there is a type in dtypes which is equal to this DataType.
   */
  isType (
    dtypes: (DataTypeExprKind | DataTypeExpr | IdentifierExpr | DotExpr)[],
    options?: { checkNullable?: boolean },
  ): boolean {
    const checkNullable = options?.checkNullable ?? false;
    const selfIsNullable = this.args.nullable;

    for (const dtype of dtypes) {
      const otherType = DataTypeExpr.build(dtype, {
        copy: false,
        udt: true,
      });
      const otherIsNullable = otherType.args.nullable;

      let matches: boolean;

      if (
        otherType.args.expressions
        || (checkNullable && (selfIsNullable || otherIsNullable))
        || this.args.this === DataTypeExprKind.USERDEFINED
        || otherType.args.this === DataTypeExprKind.USERDEFINED
      ) {
        matches = this.equals(otherType);
      } else {
        matches = this.args.this === otherType.args.this;
      }

      if (matches) {
        return true;
      }
    }

    return false;
  }

  get $this (): DataTypeExprKind {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $nested (): boolean | undefined {
    return this.args.nested;
  }

  get $values (): Expression[] | undefined {
    return this.args.values;
  }

  get $prefix (): boolean | string | undefined {
    return this.args.prefix;
  }

  get $kind (): DataTypeExprKind | DotExpr | IdentifierExpr | undefined {
    return this.args.kind;
  }

  get $nullable (): Expression | undefined {
    return this.args.nullable;
  }
}

export type TypeExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class TypeExpr extends Expression {
  key = ExpressionKey.TYPE;

  static argTypes: RequiredMap<TypeExprArgs> = {};

  declare args: TypeExprArgs;

  constructor (args: TypeExprArgs) {
    super(args);
  }
}

export type CommandExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: string;
    expression?: string;
  },
]>;

export class CommandExpr extends Expression {
  key = ExpressionKey.COMMAND;

  static argTypes: RequiredMap<CommandExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: CommandExprArgs;

  constructor (args: CommandExprArgs) {
    super(args);
  }

  get $this (): string {
    return this.args.this;
  }

  get $expression (): string | undefined {
    return this.args.expression;
  }
}

export type TransactionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    modes?: Expression[];
    mark?: Expression;
  },
]>;

export class TransactionExpr extends Expression {
  key = ExpressionKey.TRANSACTION;

  static argTypes: RequiredMap<TransactionExprArgs> = {
    ...super.argTypes,
    this: false,
    modes: false,
    mark: false,
  };

  declare args: TransactionExprArgs;

  constructor (args: TransactionExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $modes (): Expression[] | undefined {
    return this.args.modes;
  }

  get $mark (): Expression | undefined {
    return this.args.mark;
  }
}

export type CommitExprArgs = Merge<[
  BaseExpressionArgs,
  {
    chain?: Expression;
    this?: Expression;
    durability?: Expression;
  },
]>;

export class CommitExpr extends Expression {
  key = ExpressionKey.COMMIT;

  static argTypes: RequiredMap<CommitExprArgs> = {
    ...super.argTypes,
    chain: false,
    this: false,
    durability: false,
  };

  declare args: CommitExprArgs;

  constructor (args: CommitExprArgs) {
    super(args);
  }

  get $chain (): Expression | undefined {
    return this.args.chain;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $durability (): Expression | undefined {
    return this.args.durability;
  }
}

export type RollbackExprArgs = Merge<[
  BaseExpressionArgs,
  {
    savepoint?: Expression;
    this?: Expression;
  },
]>;

export class RollbackExpr extends Expression {
  key = ExpressionKey.ROLLBACK;

  static argTypes: RequiredMap<RollbackExprArgs> = {
    ...super.argTypes,
    savepoint: false,
    this: false,
  };

  declare args: RollbackExprArgs;

  constructor (args: RollbackExprArgs) {
    super(args);
  }

  get $savepoint (): Expression | undefined {
    return this.args.savepoint;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

/**
 * Enumeration of valid kind values for Alter expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum AlterExprKind {
  TABLE = 'TABLE',
  VIEW = 'VIEW',
  SCHEMA = 'SCHEMA',
  DATABASE = 'DATABASE',
  INDEX = 'INDEX',
  COLUMN = 'COLUMN',
}

export type AlterExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    kind: AlterExprKind;
    actions: Expression[];
    exists?: boolean;
    only?: boolean;
    options?: Expression[];
    cluster?: Expression;
    notValid?: Expression;
    check?: Expression;
    cascade?: Expression;
  },
]>;

export class AlterExpr extends Expression {
  key = ExpressionKey.ALTER;

  /**
   * Defines the arguments (properties and child expressions) for Alter expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<AlterExprArgs> = {
    ...super.argTypes,
    this: false,
    kind: true,
    actions: true,
    exists: false,
    only: false,
    options: false,
    cluster: false,
    notValid: false,
    check: false,
    cascade: false,
  };

  declare args: AlterExprArgs;

  constructor (args: AlterExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $kind (): AlterExprKind {
    return this.args.kind;
  }

  get $actions (): Expression[] {
    return this.args.actions;
  }

  get $exists (): boolean | undefined {
    return this.args.exists;
  }

  get $only (): boolean | undefined {
    return this.args.only;
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }

  get $cluster (): Expression | undefined {
    return this.args.cluster;
  }

  get $notValid (): Expression | undefined {
    return this.args.notValid;
  }

  get $check (): Expression | undefined {
    return this.args.check;
  }

  get $cascade (): Expression | undefined {
    return this.args.cascade;
  }

  /**
   * Returns the kind in uppercase.
   */
  get kind (): string | undefined {
    const kind = this.args.kind;
    return kind ? kind.toUpperCase() : undefined;
  }

  /**
   * Returns the actions array.
   */
  get actions (): Expression[] {
    return this.args.actions || [];
  }
}

export type AlterSessionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    expressions: Expression[];
    unset?: Expression;
  },
]>;

export class AlterSessionExpr extends Expression {
  key = ExpressionKey.ALTER_SESSION;

  static argTypes: RequiredMap<AlterSessionExprArgs> = {
    ...super.argTypes,
    expressions: true,
    unset: false,
  };

  declare args: AlterSessionExprArgs;

  constructor (args: AlterSessionExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $unset (): Expression | undefined {
    return this.args.unset;
  }
}

/**
 * Valid kind values for ANALYZE statements
 */
export enum AnalyzeExprKind {
  STATISTICS = 'STATISTICS',
  COMPUTE = 'COMPUTE',
  TABLE = 'TABLE',
}

export type AnalyzeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: AnalyzeExprKind;
    this?: Expression;
    options?: Expression[];
    mode?: Expression;
    partition?: Expression;
    expression?: Expression;
    properties?: Expression[];
  },
]>;

export class AnalyzeExpr extends Expression {
  key = ExpressionKey.ANALYZE;

  /**
   * Defines the arguments (properties and child expressions) for Analyze expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<AnalyzeExprArgs> = {
    ...super.argTypes,
    kind: false,
    this: false,
    options: false,
    mode: false,
    partition: false,
    expression: false,
    properties: false,
  };

  declare args: AnalyzeExprArgs;

  constructor (args: AnalyzeExprArgs) {
    super(args);
  }

  get $kind (): AnalyzeExprKind | undefined {
    return this.args.kind;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }

  get $mode (): Expression | undefined {
    return this.args.mode;
  }

  get $partition (): Expression | undefined {
    return this.args.partition;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $properties (): Expression[] | undefined {
    return this.args.properties;
  }
}

/**
 * Enumeration of valid kind values for AnalyzeStatistics expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum AnalyzeStatisticsExprKind {
  ALL = 'ALL',
  DEFAULT = 'DEFAULT',
  COLUMNS = 'COLUMNS',
}

export type AnalyzeStatisticsExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind: AnalyzeStatisticsExprKind;
    option?: Expression;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class AnalyzeStatisticsExpr extends Expression {
  key = ExpressionKey.ANALYZE_STATISTICS;

  /**
   * Defines the arguments (properties and child expressions) for AnalyzeStatistics expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<AnalyzeStatisticsExprArgs> = {
    ...super.argTypes,
    kind: true,
    option: false,
    this: false,
    expressions: false,
  };

  declare args: AnalyzeStatisticsExprArgs;

  constructor (args: AnalyzeStatisticsExprArgs) {
    super(args);
  }

  get $kind (): AnalyzeStatisticsExprKind {
    return this.args.kind;
  }

  get $option (): Expression | undefined {
    return this.args.option;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

export type AnalyzeHistogramExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expressions: Expression[];
    expression?: Expression;
    updateOptions?: Expression[];
  },
]>;

export class AnalyzeHistogramExpr extends Expression {
  key = ExpressionKey.ANALYZE_HISTOGRAM;

  static argTypes: RequiredMap<AnalyzeHistogramExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
    expression: false,
    updateOptions: false,
  };

  declare args: AnalyzeHistogramExprArgs;

  constructor (args: AnalyzeHistogramExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $updateOptions (): Expression[] | undefined {
    return this.args.updateOptions;
  }
}

/**
 * Enumeration of valid kind values for AnalyzeSample expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum AnalyzeSampleExprKind {
  PERCENT = 'PERCENT',
  ROWS = 'ROWS',
}

export type AnalyzeSampleExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind: AnalyzeSampleExprKind;
    sample: number | Expression;
  },
]>;

export class AnalyzeSampleExpr extends Expression {
  key = ExpressionKey.ANALYZE_SAMPLE;

  /**
   * Defines the arguments (properties and child expressions) for AnalyzeSample expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<AnalyzeSampleExprArgs> = {
    ...super.argTypes,
    kind: true,
    sample: true,
  };

  declare args: AnalyzeSampleExprArgs;

  constructor (args: AnalyzeSampleExprArgs) {
    super(args);
  }

  get $kind (): AnalyzeSampleExprKind {
    return this.args.kind;
  }

  get $sample (): number | Expression {
    return this.args.sample;
  }
}

export type AnalyzeListChainedRowsExprArgs = Merge<[
  BaseExpressionArgs,
  { expression?: Expression },
]>;

export class AnalyzeListChainedRowsExpr extends Expression {
  key = ExpressionKey.ANALYZE_LIST_CHAINED_ROWS;

  static argTypes: RequiredMap<AnalyzeListChainedRowsExprArgs> = {
    ...super.argTypes,
    expression: false,
  };

  declare args: AnalyzeListChainedRowsExprArgs;

  constructor (args: AnalyzeListChainedRowsExprArgs) {
    super(args);
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }
}

/**
 * Valid kind values for ANALYZE DELETE statements
 */
export enum AnalyzeDeleteExprKind {
  STATISTICS = 'STATISTICS',
}
export type AnalyzeDeleteExprArgs = Merge<[
  BaseExpressionArgs,
  { kind?: AnalyzeDeleteExprKind },
]>;

export class AnalyzeDeleteExpr extends Expression {
  key = ExpressionKey.ANALYZE_DELETE;

  static argTypes: RequiredMap<AnalyzeDeleteExprArgs> = {
    ...super.argTypes,
    kind: false,
  };

  declare args: AnalyzeDeleteExprArgs;

  constructor (args: AnalyzeDeleteExprArgs) {
    super(args);
  }

  get $kind (): AnalyzeDeleteExprKind | undefined {
    return this.args.kind;
  }
}

export type AnalyzeWithExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions: Expression[] },
]>;

export class AnalyzeWithExpr extends Expression {
  key = ExpressionKey.ANALYZE_WITH;

  static argTypes: RequiredMap<AnalyzeWithExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: AnalyzeWithExprArgs;

  constructor (args: AnalyzeWithExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

/**
 * Enumeration of valid kind values for AnalyzeValidate expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum AnalyzeValidateExprKind {
  REF_UPDATE = 'REF_UPDATE',
  STRUCTURE = 'STRUCTURE',
}

export type AnalyzeValidateExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind: AnalyzeValidateExprKind;
    this?: Expression;
    expression?: Expression;
  },
]>;

export class AnalyzeValidateExpr extends Expression {
  key = ExpressionKey.ANALYZE_VALIDATE;

  static argTypes: RequiredMap<AnalyzeValidateExprArgs> = {
    ...super.argTypes,
    kind: true,
    this: false,
    expression: false,
  };

  declare args: AnalyzeValidateExprArgs;

  constructor (args: AnalyzeValidateExprArgs) {
    super(args);
  }

  get $kind (): AnalyzeValidateExprKind {
    return this.args.kind;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }
}

export type AnalyzeColumnsExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class AnalyzeColumnsExpr extends Expression {
  key = ExpressionKey.ANALYZE_COLUMNS;

  static argTypes: RequiredMap<AnalyzeColumnsExprArgs> = {
    ...super.argTypes,
  };

  declare args: AnalyzeColumnsExprArgs;

  constructor (args: AnalyzeColumnsExprArgs) {
    super(args);
  }
}

export type UsingDataExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class UsingDataExpr extends Expression {
  key = ExpressionKey.USING_DATA;

  static argTypes: RequiredMap<UsingDataExprArgs> = {
    ...super.argTypes,
  };

  declare args: UsingDataExprArgs;

  constructor (args: UsingDataExprArgs) {
    super(args);
  }
}

export type AddConstraintExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions: Expression[] },
]>;

export class AddConstraintExpr extends Expression {
  key = ExpressionKey.ADD_CONSTRAINT;

  static argTypes: RequiredMap<AddConstraintExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: AddConstraintExprArgs;

  constructor (args: AddConstraintExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type AddPartitionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    exists?: boolean;
    location?: Expression;
  },
]>;

export class AddPartitionExpr extends Expression {
  key = ExpressionKey.ADD_PARTITION;

  static argTypes: RequiredMap<AddPartitionExprArgs> = {
    ...super.argTypes,
    this: true,
    exists: false,
    location: false,
  };

  declare args: AddPartitionExprArgs;

  constructor (args: AddPartitionExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $exists (): boolean | undefined {
    return this.args.exists;
  }

  get $location (): Expression | undefined {
    return this.args.location;
  }
}

export type AttachOptionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class AttachOptionExpr extends Expression {
  key = ExpressionKey.ATTACH_OPTION;

  static argTypes: RequiredMap<AttachOptionExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: AttachOptionExprArgs;

  constructor (args: AttachOptionExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }
}

export type DropPartitionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    expressions: Expression[];
    exists?: boolean;
  },
]>;

export class DropPartitionExpr extends Expression {
  key = ExpressionKey.DROP_PARTITION;

  static argTypes: RequiredMap<DropPartitionExprArgs> = {
    ...super.argTypes,
    expressions: true,
    exists: false,
  };

  declare args: DropPartitionExprArgs;

  constructor (args: DropPartitionExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $exists (): boolean | undefined {
    return this.args.exists;
  }
}

export type ReplacePartitionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    expression: Expression;
    source: Expression;
  },
]>;

export class ReplacePartitionExpr extends Expression {
  key = ExpressionKey.REPLACE_PARTITION;

  static argTypes: RequiredMap<ReplacePartitionExprArgs> = {
    ...super.argTypes,
    expression: true,
    source: true,
  };

  declare args: ReplacePartitionExprArgs;

  constructor (args: ReplacePartitionExprArgs) {
    super(args);
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $source (): Expression {
    return this.args.source;
  }
}

export type AliasExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    alias?: string | IdentifierExpr;
  },
]>;

export class AliasExpr extends Expression {
  key = ExpressionKey.ALIAS;

  static argTypes: RequiredMap<AliasExprArgs> = {
    ...super.argTypes,
    this: true,
    alias: false,
  };

  declare args: AliasExprArgs;

  constructor (args: AliasExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $alias (): string | IdentifierExpr | undefined {
    return this.args.alias;
  }

  get outputName (): string {
    if (typeof this.args.alias === 'string') {
      return this.args.alias;
    }
    return this.args.alias?.name || '';
  }
}

export type PivotAnyExprArgs = Merge<[
  BaseExpressionArgs,
  { this?: Expression },
]>;

/**
 * Represents Snowflake's ANY [ ORDER BY ... ] syntax
 * https://docs.snowflake.com/en/sql-reference/constructs/pivot
 */
export class PivotAnyExpr extends Expression {
  key = ExpressionKey.PIVOT_ANY;

  static argTypes: RequiredMap<PivotAnyExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: PivotAnyExprArgs;

  constructor (args: PivotAnyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type AliasesExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expressions: Expression[];
  },
]>;

export class AliasesExpr extends Expression {
  key = ExpressionKey.ALIASES;

  static argTypes: RequiredMap<AliasesExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
  };

  declare args: AliasesExprArgs;

  constructor (args: AliasesExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get aliases (): Expression[] {
    return this.args.expressions;
  }
}

export type AtIndexExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

/**
 * https://docs.aws.amazon.com/redshift/latest/dg/query-super.html
 */
export class AtIndexExpr extends Expression {
  key = ExpressionKey.AT_INDEX;

  static argTypes: RequiredMap<AtIndexExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: AtIndexExprArgs;

  constructor (args: AtIndexExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type AtTimeZoneExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    zone: Expression;
  },
]>;

export class AtTimeZoneExpr extends Expression {
  key = ExpressionKey.AT_TIME_ZONE;

  static argTypes: RequiredMap<AtTimeZoneExprArgs> = {
    ...super.argTypes,
    this: true,
    zone: true,
  };

  declare args: AtTimeZoneExprArgs;

  constructor (args: AtTimeZoneExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $zone (): Expression {
    return this.args.zone;
  }
}

export type FromTimeZoneExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    zone: Expression;
  },
]>;

export class FromTimeZoneExpr extends Expression {
  key = ExpressionKey.FROM_TIME_ZONE;

  static argTypes: RequiredMap<FromTimeZoneExprArgs> = {
    ...super.argTypes,
    this: true,
    zone: true,
  };

  declare args: FromTimeZoneExprArgs;

  constructor (args: FromTimeZoneExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $zone (): Expression {
    return this.args.zone;
  }
}

export type FormatPhraseExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    format: Expression;
  },
]>;

/**
 * Format override for a column in Teradata.
 * Can be expanded to additional dialects as needed.
 *
 * https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Types-and-Literals/Data-Type-Formats-and-Format-Phrases/FORMAT
 */
export class FormatPhraseExpr extends Expression {
  key = ExpressionKey.FORMAT_PHRASE;

  static argTypes: RequiredMap<FormatPhraseExprArgs> = {
    ...super.argTypes,
    this: true,
    format: true,
  };

  declare args: FormatPhraseExprArgs;

  constructor (args: FormatPhraseExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $format (): Expression {
    return this.args.format;
  }
}

export type DistinctExprArgs = Merge<[
  BaseExpressionArgs,
  {
    expressions?: Expression[];
    on?: Expression;
  },
]>;

export class DistinctExpr extends Expression {
  key = ExpressionKey.DISTINCT;

  static argTypes: RequiredMap<DistinctExprArgs> = {
    ...super.argTypes,
    expressions: false,
    on: false,
  };

  declare args: DistinctExprArgs;

  constructor (args: DistinctExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $on (): Expression | undefined {
    return this.args.on;
  }
}

export type ForInExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

/**
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#for-in
 */
export class ForInExpr extends Expression {
  key = ExpressionKey.FOR_IN;

  static argTypes: RequiredMap<ForInExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: ForInExprArgs;

  constructor (args: ForInExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type TimeUnitExprArgs = Merge<[
  BaseExpressionArgs,
  { unit?: VarExpr | IntervalSpanExpr },
]>;

/**
 * Automatically converts unit arg into a var.
 */
export class TimeUnitExpr extends Expression {
  key = ExpressionKey.TIME_UNIT;

  static argTypes: RequiredMap<TimeUnitExprArgs> = {
    ...super.argTypes,
    unit: false,
  };

  static UNABBREVIATED_UNIT_NAME: Record<string, string> = {
    D: 'DAY',
    H: 'HOUR',
    M: 'MINUTE',
    MS: 'MILLISECOND',
    NS: 'NANOSECOND',
    Q: 'QUARTER',
    S: 'SECOND',
    US: 'MICROSECOND',
    W: 'WEEK',
    Y: 'YEAR',
  };

  static isVarLike (expr: Expression): expr is VarExpr | ColumnExpr | LiteralExpr {
    return expr instanceof VarExpr || expr instanceof ColumnExpr || expr instanceof LiteralExpr;
  }

  declare args: TimeUnitExprArgs;

  constructor (args: TimeUnitExprArgs) {
    const unit = args.unit;

    if (
      unit
      && TimeUnitExpr.isVarLike(unit)
      && !(unit instanceof ColumnExpr && unit.parts.length !== 1)
    ) {
      args.unit = new VarExpr({
        this: (TimeUnitExpr.UNABBREVIATED_UNIT_NAME[unit.name] || unit.name).toUpperCase(),
      });
    } else if (unit instanceof WeekExpr) {
      const thisArg = unit.args.this;
      if (thisArg) {
        unit.setArgKey('this', new VarExpr({ this: thisArg.name.toUpperCase() }));
      }
    }

    super(args);
  }

  get unit (): VarExpr | IntervalSpanExpr | undefined {
    return this.$unit;
  }

  get $unit (): VarExpr | IntervalSpanExpr | undefined {
    return this.args.unit;
  }
}

export type IgnoreNullsExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class IgnoreNullsExpr extends Expression {
  key = ExpressionKey.IGNORE_NULLS;

  static argTypes: RequiredMap<IgnoreNullsExprArgs> = {
    ...super.argTypes,
  };

  declare args: IgnoreNullsExprArgs;

  constructor (args: IgnoreNullsExprArgs) {
    super(args);
  }
}

export type RespectNullsExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class RespectNullsExpr extends Expression {
  key = ExpressionKey.RESPECT_NULLS;

  static argTypes: RequiredMap<RespectNullsExprArgs> = {
    ...super.argTypes,
  };

  declare args: RespectNullsExprArgs;

  constructor (args: RespectNullsExprArgs) {
    super(args);
  }
}

/**
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate-function-calls#max_min_clause
 */
export type HavingMaxExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression: Expression;
    max: Expression;
  },
]>;

export class HavingMaxExpr extends Expression {
  key = ExpressionKey.HAVING_MAX;

  static argTypes: RequiredMap<HavingMaxExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    max: true,
  };

  declare args: HavingMaxExprArgs;

  constructor (args: HavingMaxExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $max (): Expression {
    return this.args.max;
  }
}

export type TranslateCharactersExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression: Expression;
    withError?: Expression;
  },
]>;

export class TranslateCharactersExpr extends Expression {
  key = ExpressionKey.TRANSLATE_CHARACTERS;

  static argTypes: RequiredMap<TranslateCharactersExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    withError: false,
  };

  declare args: TranslateCharactersExprArgs;

  constructor (args: TranslateCharactersExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $withError (): Expression | undefined {
    return this.args.withError;
  }
}

export type PositionalColumnExprArgs = Merge<[
  BaseExpressionArgs,
]>;
export class PositionalColumnExpr extends Expression {
  key = ExpressionKey.POSITIONAL_COLUMN;

  static argTypes: RequiredMap<PositionalColumnExprArgs> = {};

  declare args: PositionalColumnExprArgs;

  constructor (args: PositionalColumnExprArgs) {
    super(args);
  }
}

export type OverflowTruncateBehaviorExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    withCount: Expression;
  },
]>;

export class OverflowTruncateBehaviorExpr extends Expression {
  key = ExpressionKey.OVERFLOW_TRUNCATE_BEHAVIOR;

  static argTypes: RequiredMap<OverflowTruncateBehaviorExprArgs> = {
    this: false,
    withCount: true,
  };

  declare args: OverflowTruncateBehaviorExprArgs;

  constructor (args: OverflowTruncateBehaviorExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $withCount (): Expression {
    return this.args.withCount;
  }
}

export type JSONExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    with?: Expression;
    unique?: boolean;
  },
]>;

export class JSONExpr extends Expression {
  key = ExpressionKey.JSON;

  /**
   * Defines the arguments (properties and child expressions) for JSON expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JSONExprArgs> = {
    ...super.argTypes,
    this: false,
    with: false,
    unique: false,
  };

  declare args: JSONExprArgs;

  constructor (args: JSONExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $with (): Expression | undefined {
    return this.args.with;
  }

  get $unique (): boolean | undefined {
    return this.args.unique;
  }
}

export type JSONPathExprArgs = Merge<[
  BaseExpressionArgs,
  {
    expressions: Expression[];
    escape?: Expression;
  },
]>;

export class JSONPathExpr extends Expression {
  key = ExpressionKey.JSON_PATH;

  static argTypes: RequiredMap<JSONPathExprArgs> = {
    ...super.argTypes,
    expressions: true,
    escape: false,
  };

  declare args: JSONPathExprArgs;

  constructor (args: JSONPathExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $escape (): Expression | undefined {
    return this.args.escape;
  }

  get outputName (): string {
    const lastSegment = this.args.expressions[this.args.expressions.length - 1];
    const thisValue = lastSegment.args.this;
    return typeof thisValue === 'string' ? thisValue : '';
  }
}

export type JSONPathPartExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class JSONPathPartExpr extends Expression {
  key = ExpressionKey.JSON_PATH_PART;

  static argTypes: RequiredMap<JSONPathPartExprArgs> = {};

  declare args: JSONPathPartExprArgs;

  constructor (args: JSONPathPartExprArgs) {
    super(args);
  }
}

export type FormatJsonExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class FormatJsonExpr extends Expression {
  key = ExpressionKey.FORMAT_JSON;

  static argTypes: RequiredMap<FormatJsonExprArgs> = {};

  declare args: FormatJsonExprArgs;

  constructor (args: FormatJsonExprArgs) {
    super(args);
  }
}

export type JSONKeyValueExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class JSONKeyValueExpr extends Expression {
  key = ExpressionKey.JSON_KEY_VALUE;

  static argTypes: RequiredMap<JSONKeyValueExprArgs> = {
    this: true,
    expression: true,
  };

  declare args: JSONKeyValueExprArgs;

  constructor (args: JSONKeyValueExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

/**
 * Valid kind values for JSON column definitions
 */
export enum JSONColumnDefExprKind {
  PATH = 'PATH',
  EXISTS = 'EXISTS',
  VALUE = 'VALUE',
  QUERY = 'QUERY',
}

export type JSONColumnDefExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    kind?: JSONColumnDefExprKind;
    path?: Expression;
    nestedSchema?: Expression;
    ordinality?: boolean;
  },
]>;

export class JSONColumnDefExpr extends Expression {
  key = ExpressionKey.JSON_COLUMN_DEF;

  /**
   * Defines the arguments (properties and child expressions) for JSONColumnDef expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JSONColumnDefExprArgs> = {
    ...super.argTypes,
    this: false,
    kind: false,
    path: false,
    nestedSchema: false,
    ordinality: false,
  };

  declare args: JSONColumnDefExprArgs;

  constructor (args: JSONColumnDefExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $kind (): JSONColumnDefExprKind | undefined {
    return this.args.kind;
  }

  get $path (): Expression | undefined {
    return this.args.path;
  }

  get $nestedSchema (): Expression | undefined {
    return this.args.nestedSchema;
  }

  get $ordinality (): boolean | undefined {
    return this.args.ordinality;
  }
}

export type JSONSchemaExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions: Expression[] },
]>;

export class JSONSchemaExpr extends Expression {
  key = ExpressionKey.JSON_SCHEMA;

  static argTypes: RequiredMap<JSONSchemaExprArgs> = {
    expressions: true,
  };

  declare args: JSONSchemaExprArgs;

  constructor (args: JSONSchemaExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type JSONValueExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    path: Expression;
    returning?: Expression;
    onCondition?: Expression;
  },
]>;

export class JSONValueExpr extends Expression {
  key = ExpressionKey.JSON_VALUE;

  /**
   * Defines the arguments (properties and child expressions) for JSONValue expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JSONValueExprArgs> = {
    ...super.argTypes,
    this: true,
    path: true,
    returning: false,
    onCondition: false,
  };

  declare args: JSONValueExprArgs;

  constructor (args: JSONValueExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $path (): Expression {
    return this.args.path;
  }

  get $returning (): Expression | undefined {
    return this.args.returning;
  }

  get $onCondition (): Expression | undefined {
    return this.args.onCondition;
  }
}

/**
 * Enumeration of valid kind values for OpenJSONColumnDef expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum OpenJSONColumnDefExprKind {
  PATH = 'PATH',
  EXISTS = 'EXISTS',
  VALUE = 'VALUE',
  QUERY = 'QUERY',
}

export type OpenJSONColumnDefExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    kind: OpenJSONColumnDefExprKind;
    path?: Expression;
    asJson?: Expression;
  },
]>;

export class OpenJSONColumnDefExpr extends Expression {
  key = ExpressionKey.OPEN_JSON_COLUMN_DEF;

  /**
   * Defines the arguments (properties and child expressions) for OpenJSONColumnDef expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<OpenJSONColumnDefExprArgs> = {
    ...super.argTypes,
    this: true,
    kind: true,
    path: false,
    asJson: false,
  };

  declare args: OpenJSONColumnDefExprArgs;

  constructor (args: OpenJSONColumnDefExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): OpenJSONColumnDefExprKind {
    return this.args.kind;
  }

  get $path (): Expression | undefined {
    return this.args.path;
  }

  get $asJson (): Expression | undefined {
    return this.args.asJson;
  }
}

export type JSONExtractQuoteExprArgs = Merge<[
  BaseExpressionArgs,
  {
    option: Expression;
    scalar?: boolean;
  },
]>;

export class JSONExtractQuoteExpr extends Expression {
  key = ExpressionKey.JSON_EXTRACT_QUOTE;

  /**
   * Defines the arguments (properties and child expressions) for JSONExtractQuote expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JSONExtractQuoteExprArgs> = {
    ...super.argTypes,
    option: true,
    scalar: false,
  };

  declare args: JSONExtractQuoteExprArgs;

  constructor (args: JSONExtractQuoteExprArgs) {
    super(args);
  }

  get $option (): Expression {
    return this.args.option;
  }

  get $scalar (): boolean | undefined {
    return this.args.scalar;
  }
}

export type ScopeResolutionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression: Expression;
  },
]>;

export class ScopeResolutionExpr extends Expression {
  key = ExpressionKey.SCOPE_RESOLUTION;

  static argTypes: RequiredMap<ScopeResolutionExprArgs> = {
    this: false,
    expression: true,
  };

  declare args: ScopeResolutionExprArgs;

  constructor (args: ScopeResolutionExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type SliceExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression | number;
    step?: Expression;
  },
]>;

export class SliceExpr extends Expression {
  key = ExpressionKey.SLICE;

  static argTypes: RequiredMap<SliceExprArgs> = {
    this: false,
    expression: false,
    step: false,
  };

  declare args: SliceExprArgs;

  constructor (args: SliceExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $step (): Expression | undefined {
    return this.args.step;
  }
}

export type StreamExprArgs = Merge<[
  BaseExpressionArgs,
]>;
export class StreamExpr extends Expression {
  key = ExpressionKey.STREAM;

  static argTypes: RequiredMap<StreamExprArgs> = {};

  declare args: StreamExprArgs;

  constructor (args: StreamExprArgs) {
    super(args);
  }
}

export type ModelAttributeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class ModelAttributeExpr extends Expression {
  key = ExpressionKey.MODEL_ATTRIBUTE;

  static argTypes: RequiredMap<ModelAttributeExprArgs> = {
    this: true,
    expression: true,
  };

  declare args: ModelAttributeExprArgs;

  constructor (args: ModelAttributeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type WeekStartExprArgs = Merge<[
  BaseExpressionArgs,
]>;
export class WeekStartExpr extends Expression {
  key = ExpressionKey.WEEK_START;

  static argTypes: RequiredMap<WeekStartExprArgs> = {};

  declare args: WeekStartExprArgs;

  constructor (args: WeekStartExprArgs) {
    super(args);
  }
}

export type XMLNamespaceExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class XMLNamespaceExpr extends Expression {
  key = ExpressionKey.XML_NAMESPACE;

  static argTypes: RequiredMap<XMLNamespaceExprArgs> = {
    ...super.argTypes,
  };

  declare args: XMLNamespaceExprArgs;

  constructor (args: XMLNamespaceExprArgs) {
    super(args);
  }
}

export type XMLKeyValueOptionExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class XMLKeyValueOptionExpr extends Expression {
  key = ExpressionKey.XML_KEY_VALUE_OPTION;

  static argTypes: RequiredMap<XMLKeyValueOptionExprArgs> = {
    this: true,
    expression: false,
  };

  declare args: XMLKeyValueOptionExprArgs;

  constructor (args: XMLKeyValueOptionExprArgs) {
    super(args);
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $this (): ExpressionValue {
    return this.args.this;
  }
}

/**
 * Valid kind values for USE statements
 */
export enum UseExprKind {
  DATABASE = 'DATABASE',
  SCHEMA = 'SCHEMA',
  WAREHOUSE = 'WAREHOUSE',
  ROLE = 'ROLE',
  CATALOG = 'CATALOG',
}
export type UseExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: UseExprKind;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class UseExpr extends Expression {
  key = ExpressionKey.USE;

  static argTypes: RequiredMap<UseExprArgs> = {
    kind: false,
    this: false,
    expressions: false,
  };

  declare args: UseExprArgs;

  constructor (args: UseExprArgs) {
    super(args);
  }

  get $kind (): UseExprKind | undefined {
    return this.args.kind;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type WhenExprArgs = Merge<[
  BaseExpressionArgs,
  {
    matched: Expression;
    source?: Expression;
    condition?: Expression;
    then: Expression;
  },
]>;

export class WhenExpr extends Expression {
  key = ExpressionKey.WHEN;

  /**
   * Defines the arguments (properties and child expressions) for When expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<WhenExprArgs> = {
    ...super.argTypes,
    matched: true,
    source: false,
    condition: false,
    then: true,
  };

  declare args: WhenExprArgs;

  constructor (args: WhenExprArgs) {
    super(args);
  }

  get $matched (): Expression {
    return this.args.matched;
  }

  get $source (): Expression | undefined {
    return this.args.source;
  }

  get $condition (): Expression | undefined {
    return this.args.condition;
  }

  get $then (): Expression {
    return this.args.then;
  }
}

export type WhensExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions: Expression[] },
]>;

export class WhensExpr extends Expression {
  key = ExpressionKey.WHENS;

  static argTypes: RequiredMap<WhensExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: WhensExprArgs;

  constructor (args: WhensExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type SemicolonExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class SemicolonExpr extends Expression {
  key = ExpressionKey.SEMICOLON;

  static argTypes: RequiredMap<SemicolonExprArgs> = {};

  declare args: SemicolonExprArgs;

  constructor (args: SemicolonExprArgs) {
    super(args);
  }
}

export type TableColumnExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class TableColumnExpr extends Expression {
  key = ExpressionKey.TABLE_COLUMN;

  static argTypes: RequiredMap<TableColumnExprArgs> = {
    ...super.argTypes,
  };

  declare args: TableColumnExprArgs;

  constructor (args: TableColumnExprArgs) {
    super(args);
  }
}

export type VariadicExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class VariadicExpr extends Expression {
  key = ExpressionKey.VARIADIC;

  static argTypes: RequiredMap<VariadicExprArgs> = {
    ...super.argTypes,
  };

  declare args: VariadicExprArgs;

  constructor (args: VariadicExprArgs) {
    super(args);
  }
}

export type CTEExprArgs = Merge<[
  BaseExpressionArgs,
  {
    scalar?: boolean;
    materialized?: boolean;
    keyExpressions?: Expression[];
    // NOTE: sqlglot named it alias but we name it aliases
    aliases: Expression[];
    this: Expression;
  },
]>;

export class CTEExpr extends DerivedTableExpr {
  key = ExpressionKey.CTE;

  /**
   * Defines the arguments (properties and child expressions) for CTE expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<CTEExprArgs> = {
    ...super.argTypes,
    this: true,
    aliases: true,
    scalar: false,
    materialized: false,
    keyExpressions: false,
  };

  declare args: CTEExprArgs;

  constructor (args: CTEExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $aliases (): Expression[] {
    return this.args.aliases;
  }

  get $scalar (): boolean | undefined {
    return this.args.scalar;
  }

  get $materialized (): boolean | undefined {
    return this.args.materialized;
  }

  get $keyExpressions (): Expression[] | undefined {
    return this.args.keyExpressions;
  }
}

export type BitStringExprArgs = Merge<[
  ConditionExprArgs,
]>;

export class BitStringExpr extends ConditionExpr {
  key = ExpressionKey.BIT_STRING;

  static argTypes: RequiredMap<BitStringExprArgs> = {
    ...super.argTypes,
  };

  declare args: BitStringExprArgs;

  constructor (args: BitStringExprArgs) {
    super(args);
  }
}

export type HexStringExprArgs = Merge<[
  BaseExpressionArgs,
  {
    isInteger?: boolean;
    this: Expression;
  },
]>;

export class HexStringExpr extends ConditionExpr {
  key = ExpressionKey.HEX_STRING;

  /**
   * Defines the arguments (properties and child expressions) for HexString expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<HexStringExprArgs> = {
    ...super.argTypes,
    this: true,
    isInteger: false,
  };

  declare args: HexStringExprArgs;

  constructor (args: HexStringExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $isInteger (): boolean | undefined {
    return this.args.isInteger;
  }
}

export type ByteStringExprArgs = Merge<[
  ConditionExprArgs,
  {
    isBytes?: boolean;
    this: Expression;
  },
]>;

export class ByteStringExpr extends ConditionExpr {
  key = ExpressionKey.BYTE_STRING;

  /**
   * Defines the arguments (properties and child expressions) for ByteString expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ByteStringExprArgs> = {
    ...super.argTypes,
    this: true,
    isBytes: false,
  };

  declare args: ByteStringExprArgs;

  constructor (args: ByteStringExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $isBytes (): boolean | undefined {
    return this.args.isBytes;
  }
}

export type RawStringExprArgs = Merge<[
  ConditionExprArgs,
]>;

export class RawStringExpr extends ConditionExpr {
  key = ExpressionKey.RAW_STRING;

  static argTypes: RequiredMap<RawStringExprArgs> = {
    ...super.argTypes,
  };

  declare args: RawStringExprArgs;

  constructor (args: RawStringExprArgs) {
    super(args);
  }
}

export type UnicodeStringExprArgs = Merge<[
  ConditionExprArgs,
  {
    escape?: Expression;
    this: Expression;
  },
]>;

export class UnicodeStringExpr extends ConditionExpr {
  key = ExpressionKey.UNICODE_STRING;

  /**
   * Defines the arguments (properties and child expressions) for UnicodeString expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<UnicodeStringExprArgs> = {
    ...super.argTypes,
    this: true,
    escape: false,
  };

  declare args: UnicodeStringExprArgs;

  constructor (args: UnicodeStringExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $escape (): Expression | undefined {
    return this.args.escape;
  }
}

/**
 * Represents a column reference (optionally qualified with table name).
 *
 * @example
 * // users.id
 * const col = column('id', 'users');
 */
export type ColumnExprArgs = Merge<[
  ConditionExprArgs,
  {
    table?: IdentifierExpr;
    db?: IdentifierExpr;
    catalog?: IdentifierExpr;
    this: IdentifierExpr | StarExpr; // NOTE: sqlglot does not define `this` to also have type `StarExpr`, but based on the column function, I think it should also have this type
    joinMark?: Expression;
  },
]>;

export class ColumnExpr extends ConditionExpr {
  key = ExpressionKey.COLUMN;

  /**
   * Defines the arguments (properties and child expressions) for Column expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ColumnExprArgs> = {
    ...super.argTypes,
    this: true,
    table: false,
    db: false,
    catalog: false,
    joinMark: false,
  };

  declare args: ColumnExprArgs;

  constructor (args: ColumnExprArgs) {
    super(args);
  }

  /**
   * Gets the table name as a string
   * @returns The table name
   */
  get table (): string {
    return this.text('table');
  }

  /**
   * Gets the database name as a string
   * @returns The database name
   */
  get db (): string {
    return this.text('db');
  }

  /**
   * Gets the catalog name as a string
   * @returns The catalog name
   */
  get catalog (): string {
    return this.text('catalog');
  }

  /**
   * Gets the output name of the column
   * @returns The column name
   */
  get outputName (): string {
    return this.name;
  }

  /**
   * Return the parts of a column in order catalog, db, table, name.
   * @returns Array of Identifier expressions for each part that exists
   */
  get parts (): [] | [...IdentifierExpr[], StarExpr] {
    const result = [];
    for (const part of [
      'catalog',
      'db',
      'table',
      'this',
    ] as const) {
      const value = this.args[part];
      if (value) {
        result.push(value);
      }
    }
    return result as [] | [...IdentifierExpr[], StarExpr];
  }

  toDot (options: { includeDots?: boolean } = {}): DotExpr | IdentifierExpr | StarExpr {
    const { includeDots = true } = options;
    const parts: Expression[] = this.parts;
    let parent = this.parent;

    if (includeDots) {
      while (parent instanceof DotExpr) {
        parts.push(parent.$expression);
        parent = parent.parent;
      }
    }

    return 1 < parts.length ? DotExpr.build(parts.map((p) => p.copy())) : parts[0] as IdentifierExpr | StarExpr;
  }

  get $this (): IdentifierExpr | StarExpr {
    return this.args.this;
  }

  get $table (): IdentifierExpr | undefined {
    return this.args.table;
  }

  get $db (): IdentifierExpr | undefined {
    return this.args.db;
  }

  get $catalog (): IdentifierExpr | undefined {
    return this.args.catalog;
  }

  get $joinMark (): Expression | undefined {
    return this.args.joinMark;
  }
}

export type PseudocolumnExprArgs = Merge<[
  ColumnExprArgs,
]>;

export class PseudocolumnExpr extends ColumnExpr {
  key = ExpressionKey.PSEUDOCOLUMN;

  static argTypes: RequiredMap<PseudocolumnExprArgs> = {
    ...super.argTypes,
  };

  declare args: PseudocolumnExprArgs;

  constructor (args: PseudocolumnExprArgs) {
    super(args);
  }

  get $this (): IdentifierExpr | StarExpr {
    return this.args.this;
  }
}

export type AutoIncrementColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class AutoIncrementColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.AUTO_INCREMENT_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<AutoIncrementColumnConstraintExprArgs> = {
    ...super.argTypes,
  };

  declare args: AutoIncrementColumnConstraintExprArgs;

  constructor (args: AutoIncrementColumnConstraintExprArgs) {
    super(args);
  }
}

export type ZeroFillColumnConstraintExprArgs = Merge<[
  ColumnConstraintExprArgs,
]>;

export class ZeroFillColumnConstraintExpr extends ColumnConstraintExpr {
  key = ExpressionKey.ZERO_FILL_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<ZeroFillColumnConstraintExprArgs> = {
    ...super.argTypes,
    kind: true, // sqlglot does not have this, but i thought it was a mistake
  };

  declare args: ZeroFillColumnConstraintExprArgs;

  constructor (args: ZeroFillColumnConstraintExprArgs) {
    super(args);
  }
}

export type PeriodForSystemTimeConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class PeriodForSystemTimeConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.PERIOD_FOR_SYSTEM_TIME_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for PeriodForSystemTimeConstraint
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<BaseExpressionArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: PeriodForSystemTimeConstraintExprArgs;

  constructor (args: PeriodForSystemTimeConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type CaseSpecificColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  { not: Expression },
]>;

export class CaseSpecificColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.CASE_SPECIFIC_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for CaseSpecificColumnConstraint
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<CaseSpecificColumnConstraintExprArgs> = {
    ...super.argTypes,
    not: true,
  };

  declare args: CaseSpecificColumnConstraintExprArgs;

  constructor (args: CaseSpecificColumnConstraintExprArgs) {
    super(args);
  }

  get $not (): Expression {
    return this.args.not;
  }
}

export type CharacterSetColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  { this: Expression },
]>;
export class CharacterSetColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.CHARACTER_SET_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<BaseExpressionArgs> = {
    this: true,
  };

  declare args: CharacterSetColumnConstraintExprArgs;

  constructor (args: CharacterSetColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type CheckColumnConstraintExprArgs = Merge<[
  BaseExpressionArgs,
  {
    enforced?: Expression;
    this: Expression;
  },
]>;

export class CheckColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.CHECK_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for CheckColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<CheckColumnConstraintExprArgs> = {
    ...super.argTypes,
    this: true,
    enforced: false,
  };

  declare args: CheckColumnConstraintExprArgs;

  constructor (args: CheckColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $enforced (): Expression | undefined {
    return this.args.enforced;
  }
}

export type ClusteredColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class ClusteredColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.CLUSTERED_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<ClusteredColumnConstraintExprArgs> = {
    ...super.argTypes,
  };

  declare args: ClusteredColumnConstraintExprArgs;

  constructor (args: ClusteredColumnConstraintExprArgs) {
    super(args);
  }
}

export type CollateColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class CollateColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.COLLATE_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<CollateColumnConstraintExprArgs> = {
    ...super.argTypes,
  };

  declare args: CollateColumnConstraintExprArgs;

  constructor (args: CollateColumnConstraintExprArgs) {
    super(args);
  }
}

export type CommentColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class CommentColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.COMMENT_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<CommentColumnConstraintExprArgs> = {
    ...super.argTypes,
  };

  declare args: CommentColumnConstraintExprArgs;

  constructor (args: CommentColumnConstraintExprArgs) {
    super(args);
  }
}

export type CompressColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  { this?: Expression },
]>;

export class CompressColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.COMPRESS_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<BaseExpressionArgs> = {
    this: false,
  };

  declare args: CompressColumnConstraintExprArgs;

  constructor (args: CompressColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type DateFormatColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  { this: Expression },
]>;

export class DateFormatColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.DATE_FORMAT_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for DateFormatColumnConstraint
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<BaseExpressionArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: DateFormatColumnConstraintExprArgs;

  constructor (args: DateFormatColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type DefaultColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class DefaultColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.DEFAULT_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<DefaultColumnConstraintExprArgs> = {
    ...super.argTypes,
  };

  declare args: DefaultColumnConstraintExprArgs;

  constructor (args: DefaultColumnConstraintExprArgs) {
    super(args);
  }
}

export type EncodeColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class EncodeColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.ENCODE_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<EncodeColumnConstraintExprArgs> = {
    ...super.argTypes,
  };

  declare args: EncodeColumnConstraintExprArgs;

  constructor (args: EncodeColumnConstraintExprArgs) {
    super(args);
  }
}

export type ExcludeColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class ExcludeColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.EXCLUDE_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<ExcludeColumnConstraintExprArgs> = {
    ...super.argTypes,
  };

  declare args: ExcludeColumnConstraintExprArgs;

  constructor (args: ExcludeColumnConstraintExprArgs) {
    super(args);
  }
}

export type EphemeralColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  { this?: Expression },
]>;

export class EphemeralColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.EPHEMERAL_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<BaseExpressionArgs> = {
    this: false,
  };

  declare args: EphemeralColumnConstraintExprArgs;

  constructor (args: EphemeralColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type GeneratedAsIdentityColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    onNull?: Expression;
    start?: Expression;
    increment?: Expression;
    minvalue?: string;
    maxvalue?: string;
    cycle?: Expression;
    order?: boolean;
    this?: boolean;
    expression?: Expression;
  },
]>;

export class GeneratedAsIdentityColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.GENERATED_AS_IDENTITY_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for
   * GeneratedAsIdentityColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   * Note: this: true -> ALWAYS, this: false -> BY DEFAULT
   */
  static argTypes: RequiredMap<GeneratedAsIdentityColumnConstraintExprArgs> = {
    ...super.argTypes,
    this: false,
    expression: false,
    onNull: false,
    start: false,
    increment: false,
    minvalue: false,
    maxvalue: false,
    cycle: false,
    order: false,
  };

  declare args: GeneratedAsIdentityColumnConstraintExprArgs;

  constructor (args: GeneratedAsIdentityColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): boolean | undefined {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $onNull (): Expression | undefined {
    return this.args.onNull;
  }

  get $start (): Expression | undefined {
    return this.args.start;
  }

  get $increment (): Expression | undefined {
    return this.args.increment;
  }

  get $minvalue (): string | undefined {
    return this.args.minvalue;
  }

  get $maxvalue (): string | undefined {
    return this.args.maxvalue;
  }

  get $cycle (): Expression | undefined {
    return this.args.cycle;
  }

  get $order (): boolean | undefined {
    return this.args.order;
  }
}

export type GeneratedAsRowColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    start?: Expression;
    hidden?: Expression;
  },
]>;

export class GeneratedAsRowColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.GENERATED_AS_ROW_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for GeneratedAsRowColumnConstraint
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<GeneratedAsRowColumnConstraintExprArgs> = {
    ...super.argTypes,
    start: false,
    hidden: false,
  };

  declare args: GeneratedAsRowColumnConstraintExprArgs;

  constructor (args: GeneratedAsRowColumnConstraintExprArgs) {
    super(args);
  }

  get $start (): Expression | undefined {
    return this.args.start;
  }

  get $hidden (): Expression | undefined {
    return this.args.hidden;
  }
}

/**
 * Valid kind values for index column constraints
 */
export enum IndexColumnConstraintExprKind {
  PRIMARY = 'PRIMARY',
  UNIQUE = 'UNIQUE',
  FULLTEXT = 'FULLTEXT',
  SPATIAL = 'SPATIAL',
}

export type IndexColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    kind?: IndexColumnConstraintExprKind;
    indexType?: DataTypeExpr;
    options?: Expression[];
    granularity?: Expression;
    this?: Expression;
    expressions?: Expression[];
    expression?: Expression;
  },
]>;

export class IndexColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.INDEX_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for IndexColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<IndexColumnConstraintExprArgs> = {
    ...super.argTypes,
    this: false,
    expressions: false,
    kind: false,
    indexType: false,
    options: false,
    expression: false,
    granularity: false,
  };

  declare args: IndexColumnConstraintExprArgs;

  constructor (args: IndexColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $kind (): IndexColumnConstraintExprKind | undefined {
    return this.args.kind;
  }

  get $indexType (): DataTypeExpr | undefined {
    return this.args.indexType;
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $granularity (): Expression | undefined {
    return this.args.granularity;
  }
}

export type InlineLengthColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class InlineLengthColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.INLINE_LENGTH_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<InlineLengthColumnConstraintExprArgs> = {
    ...super.argTypes,
  };

  declare args: InlineLengthColumnConstraintExprArgs;

  constructor (args: InlineLengthColumnConstraintExprArgs) {
    super(args);
  }
}

export type NonClusteredColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class NonClusteredColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.NON_CLUSTERED_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<NonClusteredColumnConstraintExprArgs> = {
    ...super.argTypes,
  };

  declare args: NonClusteredColumnConstraintExprArgs;

  constructor (args: NonClusteredColumnConstraintExprArgs) {
    super(args);
  }
}

export type NotForReplicationColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class NotForReplicationColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.NOT_FOR_REPLICATION_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<NotForReplicationColumnConstraintExprArgs> = {};

  declare args: NotForReplicationColumnConstraintExprArgs;

  constructor (args: NotForReplicationColumnConstraintExprArgs) {
    super(args);
  }
}

export type MaskingPolicyColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class MaskingPolicyColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.MASKING_POLICY_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<MaskingPolicyColumnConstraintExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
  };

  declare args: MaskingPolicyColumnConstraintExprArgs;

  constructor (args: MaskingPolicyColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

export type NotNullColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  { allowNull?: Expression },
]>;

export class NotNullColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.NOT_NULL_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for NotNullColumnConstraint
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<NotNullColumnConstraintExprArgs> = {
    ...super.argTypes,
    allowNull: false,
  };

  declare args: NotNullColumnConstraintExprArgs;

  constructor (args: NotNullColumnConstraintExprArgs) {
    super(args);
  }

  get $allowNull (): Expression | undefined {
    return this.args.allowNull;
  }
}

export type OnUpdateColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class OnUpdateColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.ON_UPDATE_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<OnUpdateColumnConstraintExprArgs> = {
    ...super.argTypes,
  };

  declare args: OnUpdateColumnConstraintExprArgs;

  constructor (args: OnUpdateColumnConstraintExprArgs) {
    super(args);
  }
}

export type PrimaryKeyColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    desc?: Expression;
    options?: Expression[];
  },
]>;

export class PrimaryKeyColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.PRIMARY_KEY_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for PrimaryKeyColumnConstraint
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<PrimaryKeyColumnConstraintExprArgs> = {
    ...super.argTypes,
    desc: false,
    options: false,
  };

  declare args: PrimaryKeyColumnConstraintExprArgs;

  constructor (args: PrimaryKeyColumnConstraintExprArgs) {
    super(args);
  }

  get $desc (): Expression | undefined {
    return this.args.desc;
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }
}

export type TitleColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class TitleColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.TITLE_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<TitleColumnConstraintExprArgs> = {
    ...super.argTypes,
  };

  declare args: TitleColumnConstraintExprArgs;

  constructor (args: TitleColumnConstraintExprArgs) {
    super(args);
  }
}

export type UniqueColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    indexType?: DataTypeExpr;
    onConflict?: Expression;
    nulls?: Expression[];
    options?: Expression[];
    this?: Expression;
  },
]>;

export class UniqueColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.UNIQUE_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for UniqueColumnConstraint
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<UniqueColumnConstraintExprArgs> = {
    ...super.argTypes,
    this: false,
    indexType: false,
    onConflict: false,
    nulls: false,
    options: false,
  };

  declare args: UniqueColumnConstraintExprArgs;

  constructor (args: UniqueColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $indexType (): DataTypeExpr | undefined {
    return this.args.indexType;
  }

  get $onConflict (): Expression | undefined {
    return this.args.onConflict;
  }

  get $nulls (): Expression[] | undefined {
    return this.args.nulls;
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }
}

export type UppercaseColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class UppercaseColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.UPPERCASE_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<UppercaseColumnConstraintExprArgs> = {};

  declare args: UppercaseColumnConstraintExprArgs;

  constructor (args: UppercaseColumnConstraintExprArgs) {
    super(args);
  }
}

export type PathColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class PathColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.PATH_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<PathColumnConstraintExprArgs> = {
    ...super.argTypes,
  };

  declare args: PathColumnConstraintExprArgs;

  constructor (args: PathColumnConstraintExprArgs) {
    super(args);
  }
}

export type ProjectionPolicyColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class ProjectionPolicyColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.PROJECTION_POLICY_COLUMN_CONSTRAINT;

  static argTypes: RequiredMap<ProjectionPolicyColumnConstraintExprArgs> = {
    ...super.argTypes,
  };

  declare args: ProjectionPolicyColumnConstraintExprArgs;

  constructor (args: ProjectionPolicyColumnConstraintExprArgs) {
    super(args);
  }
}

export type ComputedColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    persisted?: boolean;
    notNull?: boolean;
    dataType?: DataTypeExpr;
    this: Expression;
  },
]>;

export class ComputedColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.COMPUTED_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for ComputedColumnConstraint
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ComputedColumnConstraintExprArgs> = {
    ...super.argTypes,
    this: true,
    persisted: false,
    notNull: false,
    dataType: false,
  };

  declare args: ComputedColumnConstraintExprArgs;

  constructor (args: ComputedColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $persisted (): boolean | undefined {
    return this.args.persisted;
  }

  get $notNull (): boolean | undefined {
    return this.args.notNull;
  }

  get $dataType (): DataTypeExpr | undefined {
    return this.args.dataType;
  }
}

export type InOutColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    input?: Expression;
    output?: Expression;
    variadic?: Expression;
  },
]>;

export class InOutColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.IN_OUT_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for InOutColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<InOutColumnConstraintExprArgs> = {
    ...super.argTypes,
    input: false,
    output: false,
    variadic: false,
  };

  declare args: InOutColumnConstraintExprArgs;

  constructor (args: InOutColumnConstraintExprArgs) {
    super(args);
  }

  get $input (): Expression | undefined {
    return this.args.input;
  }

  get $output (): Expression | undefined {
    return this.args.output;
  }

  get $variadic (): Expression | undefined {
    return this.args.variadic;
  }
}

/**
 * Enumeration of valid kind values for Copy expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum CopyExprKind {
  LOCAL = 'LOCAL',
  REMOTE = 'REMOTE',
}

export type CopyExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind: CopyExprKind;
    files?: Expression[];
    credentials?: Expression[];
    format?: string;
    params?: Expression[];
    this: Expression;
  },
]>;

export class CopyExpr extends DMLExpr {
  key = ExpressionKey.COPY;

  /**
   * Defines the arguments (properties and child expressions) for Copy expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<CopyExprArgs> = {
    ...super.argTypes,
    this: true,
    kind: true,
    files: false,
    credentials: false,
    format: false,
    params: false,
  };

  declare args: CopyExprArgs;

  constructor (args: CopyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): CopyExprKind {
    return this.args.kind;
  }

  get $files (): Expression[] | undefined {
    return this.args.files;
  }

  get $credentials (): Expression[] | undefined {
    return this.args.credentials;
  }

  get $format (): string | undefined {
    return this.args.format;
  }

  get $params (): Expression[] | undefined {
    return this.args.params;
  }
}

export type InsertExprArgs = Merge<[
  DMLExprArgs,
  DDLExprArgs,
  {
    hint?: Expression;
    with?: WithExpr;
    isFunction?: boolean;
    conflict?: Expression;
    returning?: Expression;
    overwrite?: boolean;
    exists?: boolean;
    alternative?: Expression;
    where?: Expression;
    ignore?: Expression;
    byName?: string;
    stored?: Expression;
    partition?: Expression;
    settings?: Expression[];
    source?: Expression;
    default?: Expression;
    this?: Expression;
    expression?: SelectExpr;
  },
]>;

export class InsertExpr extends multiInherit(DDLExpr, DMLExpr, Expression) {
  key = ExpressionKey.INSERT;

  /**
   * Defines the arguments (properties and child expressions) for Insert expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<InsertExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    hint: false,
    with: false,
    isFunction: false,
    this: false,
    expression: false,
    conflict: false,
    returning: false,
    overwrite: false,
    exists: false,
    alternative: false,
    where: false,
    ignore: false,
    byName: false,
    stored: false,
    partition: false,
    settings: false,
    source: false,
    default: false,
  };

  declare args: InsertExprArgs;

  constructor (args: InsertExprArgs) {
    super(args);
  }

  /**
   * Append to or set the common table expressions.
   *
   * @example
   * insert("SELECT x FROM cte", "t").with("cte", "SELECT * FROM tbl").sql()
   * // 'WITH cte AS (SELECT * FROM tbl) INSERT INTO t SELECT x FROM cte'
   *
   * @param alias - the SQL code string to parse as the table name.
   *   If an Expression instance is passed, this is used as-is.
   * @param as - the SQL code string to parse as the table expression.
   *   If an Expression instance is passed, it will be used as-is.
   * @param options - Configuration options
   * @param options.recursive - set the RECURSIVE part of the expression. Defaults to false.
   * @param options.materialized - set the MATERIALIZED part of the expression.
   * @param options.append - if true, add to any existing expressions. Otherwise, this resets the
   * expressions.
   * @param options.dialect - the dialect used to parse the input expression.
   * @param options.copy - if false, modify this expression instance in-place.
   * @returns The modified expression.
   */
  with (
    alias: string | IdentifierExpr,
    as: string | QueryExpr,
    options: {
      recursive?: boolean;
      materialized?: boolean;
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      recursive, materialized, append, dialect, copy, ...restOptions
    } = options;
    return _applyCteBuilder({
      instance: this,
      alias,
      as,
      recursive,
      materialized,
      append,
      dialect,
      copy,
      ...restOptions,
    }) as this;
  }

  get $hint (): Expression | undefined {
    return this.args.hint;
  }

  get $with (): WithExpr | undefined {
    return this.args.with;
  }

  get $isFunction (): boolean | undefined {
    return this.args.isFunction;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $conflict (): Expression | undefined {
    return this.args.conflict;
  }

  get $returning (): Expression | undefined {
    return this.args.returning;
  }

  get $overwrite (): boolean | undefined {
    return this.args.overwrite;
  }

  get $exists (): boolean | undefined {
    return this.args.exists;
  }

  get $alternative (): Expression | undefined {
    return this.args.alternative;
  }

  get $where (): Expression | undefined {
    return this.args.where;
  }

  get $ignore (): Expression | undefined {
    return this.args.ignore;
  }

  get $byName (): string | undefined {
    return this.args.byName;
  }

  get $stored (): Expression | undefined {
    return this.args.stored;
  }

  get $partition (): Expression | undefined {
    return this.args.partition;
  }

  get $settings (): Expression[] | undefined {
    return this.args.settings;
  }

  get $source (): Expression | undefined {
    return this.args.source;
  }

  get $default (): Expression | undefined {
    return this.args.default;
  }
}

/**
 * Represents a literal value (string, number, boolean).
 *
 * @example
 * const str = new LiteralExpr({ this: 'hello', isString: true });
 * const num = new LiteralExpr({ this: '42', isString: false });
 */
export type LiteralExprArgs = Merge<[
  BaseExpressionArgs,
  {
    isString: boolean;
    this: string;
  },
]>;

export class LiteralExpr extends ConditionExpr {
  key = ExpressionKey.LITERAL;

  /**
   * Defines the arguments (properties and child expressions) for Literal expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<LiteralExprArgs> = {
    ...super.argTypes,
    this: true,
    isString: true,
  };

  declare args: LiteralExprArgs;

  /**
   * Create a numeric literal expression
   * @param number - The number value
   * @returns A literal expression (or negative expression for negative numbers)
   */
  static number (number: number | string): LiteralExpr | NegExpr {
    let expr: LiteralExpr | NegExpr = new LiteralExpr({
      this: String(number),
      isString: false,
    });

    const numValue = typeof number === 'number'
      ? number
      : parseFloat(String(number));

    if (!isNaN(numValue) && numValue < 0) {
      expr = new LiteralExpr({
        this: String(Math.abs(numValue)),
        isString: false,
      });
      expr = new NegExpr({ this: expr });
    }

    return expr;
  }

  /**
   * Create a string literal expression
   * @param string - The string value
   * @returns A literal expression
   */
  static string (string: unknown): LiteralExpr {
    return new LiteralExpr({
      this: String(string),
      isString: true,
    });
  }

  constructor (args: LiteralExprArgs) {
    super(args);
  }

  get outputName (): string {
    return this.name;
  }

  /**
   * Convert the literal to a Javascript value.
   * Returns a number (int or float) for numeric literals, or string for string literals.
   */
  toValue (): number | string {
    if (this.isNumber) {
      const parsed = parseInt(this.this as string, 10);
      if (!isNaN(parsed)) {
        return parsed;
      }
      const floatParsed = parseFloat(this.this as string);
      if (!isNaN(floatParsed)) {
        return floatParsed;
      }
    }
    return this.this as string;
  }

  get $this (): string {
    return this.args.this;
  }

  get $isString (): boolean {
    return this.args.isString;
  }
}

export type ClusterExprArgs = Merge<[
  OrderExprArgs,
]>;

export class ClusterExpr extends OrderExpr {
  key = ExpressionKey.CLUSTER;

  static argTypes: RequiredMap<ClusterExprArgs> = {
    ...super.argTypes,
  };

  declare args: ClusterExprArgs;

  constructor (args: ClusterExprArgs) {
    super(args);
  }
}

export type DistributeExprArgs = Merge<[
  OrderExprArgs,
]>;

export class DistributeExpr extends OrderExpr {
  key = ExpressionKey.DISTRIBUTE;

  static argTypes: RequiredMap<DistributeExprArgs> = {
    ...super.argTypes,
  };

  declare args: DistributeExprArgs;

  constructor (args: DistributeExprArgs) {
    super(args);
  }
}

export type SortExprArgs = Merge<[
  OrderExprArgs,
]>;

export class SortExpr extends OrderExpr {
  key = ExpressionKey.SORT;

  static argTypes: RequiredMap<SortExprArgs> = {
    ...super.argTypes,
  };

  declare args: SortExprArgs;

  constructor (args: SortExprArgs) {
    super(args);
  }
}

export type AlgorithmPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class AlgorithmPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ALGORITHM_PROPERTY;

  static argTypes: RequiredMap<AlgorithmPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: AlgorithmPropertyExprArgs;

  constructor (args: AlgorithmPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type AutoIncrementPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class AutoIncrementPropertyExpr extends PropertyExpr {
  key = ExpressionKey.AUTO_INCREMENT_PROPERTY;

  static argTypes: RequiredMap<AutoIncrementPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: AutoIncrementPropertyExprArgs;

  constructor (args: AutoIncrementPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type AutoRefreshPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class AutoRefreshPropertyExpr extends PropertyExpr {
  key = ExpressionKey.AUTO_REFRESH_PROPERTY;

  static argTypes: RequiredMap<AutoRefreshPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: AutoRefreshPropertyExprArgs;

  constructor (args: AutoRefreshPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type BackupPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class BackupPropertyExpr extends PropertyExpr {
  key = ExpressionKey.BACKUP_PROPERTY;

  static argTypes: RequiredMap<BackupPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: BackupPropertyExprArgs;

  constructor (args: BackupPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type BuildPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class BuildPropertyExpr extends PropertyExpr {
  key = ExpressionKey.BUILD_PROPERTY;

  static argTypes: RequiredMap<BuildPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: BuildPropertyExprArgs;

  constructor (args: BuildPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type BlockCompressionPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    autotemp?: Expression;
    always?: Expression[];
    default?: Expression;
    manual?: Expression;
    never?: Expression;
  },
]>;

export class BlockCompressionPropertyExpr extends PropertyExpr {
  key = ExpressionKey.BLOCK_COMPRESSION_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for BlockCompressionProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<BlockCompressionPropertyExprArgs> = {
    ...super.argTypes,
    autotemp: false,
    always: false,
    default: false,
    manual: false,
    never: false,
  };

  declare args: BlockCompressionPropertyExprArgs;

  constructor (args: BlockCompressionPropertyExprArgs) {
    super(args);
  }

  get $autotemp (): Expression | undefined {
    return this.args.autotemp;
  }

  get $always (): Expression[] | undefined {
    return this.args.always;
  }

  get $default (): Expression | undefined {
    return this.args.default;
  }

  get $manual (): Expression | undefined {
    return this.args.manual;
  }

  get $never (): Expression | undefined {
    return this.args.never;
  }
}

export type CharacterSetPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    default: Expression;
    this: Expression;
  },
]>;

export class CharacterSetPropertyExpr extends PropertyExpr {
  key = ExpressionKey.CHARACTER_SET_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for CharacterSetProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<CharacterSetPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
    default: true,
  };

  declare args: CharacterSetPropertyExprArgs;

  constructor (args: CharacterSetPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $default (): Expression {
    return this.args.default;
  }
}

export type ChecksumPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    on?: Expression;
    default?: Expression;
  },
]>;

export class ChecksumPropertyExpr extends PropertyExpr {
  key = ExpressionKey.CHECKSUM_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for ChecksumProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ChecksumPropertyExprArgs> = {
    ...super.argTypes,
    on: false,
    default: false,
  };

  declare args: ChecksumPropertyExprArgs;

  constructor (args: ChecksumPropertyExprArgs) {
    super(args);
  }

  get $on (): Expression | undefined {
    return this.args.on;
  }

  get $default (): Expression | undefined {
    return this.args.default;
  }
}

export type CollatePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    default?: Expression;
    this: Expression;
  },
]>;

export class CollatePropertyExpr extends PropertyExpr {
  key = ExpressionKey.COLLATE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for CollateProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<CollatePropertyExprArgs> = {
    ...super.argTypes,
    this: true,
    default: false,
  };

  declare args: CollatePropertyExprArgs;

  constructor (args: CollatePropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $default (): Expression | undefined {
    return this.args.default;
  }
}

export type CopyGrantsPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class CopyGrantsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.COPY_GRANTS_PROPERTY;

  static argTypes: RequiredMap<CopyGrantsPropertyExprArgs> = {
    ...super.argTypes,
    value: true,
  };

  declare args: CopyGrantsPropertyExprArgs;

  constructor (args: CopyGrantsPropertyExprArgs) {
    super(args);
  }
}

export type DataBlocksizePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    size?: number | Expression;
    units?: Expression[];
    minimum?: Expression;
    maximum?: Expression;
    default?: Expression;
  },
]>;

export class DataBlocksizePropertyExpr extends PropertyExpr {
  key = ExpressionKey.DATA_BLOCKSIZE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for DataBlocksizeProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<DataBlocksizePropertyExprArgs> = {
    ...super.argTypes,
    size: false,
    units: false,
    minimum: false,
    maximum: false,
    default: false,
  };

  declare args: DataBlocksizePropertyExprArgs;

  constructor (args: DataBlocksizePropertyExprArgs) {
    super(args);
  }

  get $size (): number | Expression | undefined {
    return this.args.size;
  }

  get $units (): Expression[] | undefined {
    return this.args.units;
  }

  get $minimum (): Expression | undefined {
    return this.args.minimum;
  }

  get $maximum (): Expression | undefined {
    return this.args.maximum;
  }

  get $default (): Expression | undefined {
    return this.args.default;
  }
}

export type DataDeletionPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    on: Expression;
    filterColumn?: Expression;
    retentionPeriod?: Expression;
  },
]>;

export class DataDeletionPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DATA_DELETION_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for DataDeletionProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<DataDeletionPropertyExprArgs> = {
    ...super.argTypes,
    on: true,
    filterColumn: false,
    retentionPeriod: false,
  };

  declare args: DataDeletionPropertyExprArgs;

  constructor (args: DataDeletionPropertyExprArgs) {
    super(args);
  }

  get $on (): Expression {
    return this.args.on;
  }

  get $filterColumn (): Expression | undefined {
    return this.args.filterColumn;
  }

  get $retentionPeriod (): Expression | undefined {
    return this.args.retentionPeriod;
  }
}

export type DefinerPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: string },
]>;

export class DefinerPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DEFINER_PROPERTY;

  static argTypes: RequiredMap<DefinerPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: DefinerPropertyExprArgs;

  constructor (args: DefinerPropertyExprArgs) {
    super(args);
  }

  get $this (): string {
    return this.args.this;
  }
}

export type DistKeyPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class DistKeyPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DIST_KEY_PROPERTY;

  static argTypes: RequiredMap<DistKeyPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: DistKeyPropertyExprArgs;

  constructor (args: DistKeyPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

/**
 * Enumeration of valid kind values for DistributedByProperty expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum DistributedByPropertyExprKind {
  HASH = 'HASH',
  RANGE = 'RANGE',
  LIST = 'LIST',
  REPLICATE = 'REPLICATE',
}

export type DistributedByPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    kind: DistributedByPropertyExprKind;
    buckets?: Expression[];
    order?: Expression;
    expressions?: Expression[];
  },
]>;

export class DistributedByPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DISTRIBUTED_BY_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for DistributedByProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<DistributedByPropertyExprArgs> = {
    ...super.argTypes,
    expressions: false,
    kind: true,
    buckets: false,
    order: false,
  };

  declare args: DistributedByPropertyExprArgs;

  constructor (args: DistributedByPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $kind (): DistributedByPropertyExprKind {
    return this.args.kind;
  }

  get $buckets (): Expression[] | undefined {
    return this.args.buckets;
  }

  get $order (): Expression | undefined {
    return this.args.order;
  }
}

export type DistStylePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class DistStylePropertyExpr extends PropertyExpr {
  key = ExpressionKey.DIST_STYLE_PROPERTY;

  static argTypes: RequiredMap<DistStylePropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: DistStylePropertyExprArgs;

  constructor (args: DistStylePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type DuplicateKeyPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { expressions: Expression[] },
]>;

export class DuplicateKeyPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DUPLICATE_KEY_PROPERTY;

  static argTypes: RequiredMap<DuplicateKeyPropertyExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: DuplicateKeyPropertyExprArgs;

  constructor (args: DuplicateKeyPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type EnginePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class EnginePropertyExpr extends PropertyExpr {
  key = ExpressionKey.ENGINE_PROPERTY;

  static argTypes: RequiredMap<EnginePropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: EnginePropertyExprArgs;

  constructor (args: EnginePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type HeapPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class HeapPropertyExpr extends PropertyExpr {
  key = ExpressionKey.HEAP_PROPERTY;

  static argTypes: RequiredMap<HeapPropertyExprArgs> = {
    ...super.argTypes, // NOTE: sqlglot assisns `{}`
  };

  declare args: HeapPropertyExprArgs;

  constructor (args: HeapPropertyExprArgs) {
    super(args);
  }
}

export type ToTablePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class ToTablePropertyExpr extends PropertyExpr {
  key = ExpressionKey.TO_TABLE_PROPERTY;

  static argTypes: RequiredMap<ToTablePropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: ToTablePropertyExprArgs;

  constructor (args: ToTablePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type ExecuteAsPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class ExecuteAsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.EXECUTE_AS_PROPERTY;

  static argTypes: RequiredMap<ExecuteAsPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: ExecuteAsPropertyExprArgs;

  constructor (args: ExecuteAsPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type ExternalPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this?: Expression;
  },
]>;

export class ExternalPropertyExpr extends PropertyExpr {
  key = ExpressionKey.EXTERNAL_PROPERTY;

  static argTypes: RequiredMap<ExternalPropertyExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: ExternalPropertyExprArgs;

  constructor (args: ExternalPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type FallbackPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    no: Expression;
    protection?: Expression;
  },
]>;

export class FallbackPropertyExpr extends PropertyExpr {
  key = ExpressionKey.FALLBACK_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for FallbackProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<FallbackPropertyExprArgs> = {
    ...super.argTypes,
    no: true,
    protection: false,
  };

  declare args: FallbackPropertyExprArgs;

  constructor (args: FallbackPropertyExprArgs) {
    super(args);
  }

  get $no (): Expression {
    return this.args.no;
  }

  get $protection (): Expression | undefined {
    return this.args.protection;
  }
}

export type FileFormatPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    hiveFormat?: string;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class FileFormatPropertyExpr extends PropertyExpr {
  key = ExpressionKey.FILE_FORMAT_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for FileFormatProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<FileFormatPropertyExprArgs> = {
    ...super.argTypes,
    expressions: false,
    hiveFormat: false,
    this: false,
  };

  declare args: FileFormatPropertyExprArgs;

  constructor (args: FileFormatPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $hiveFormat (): string | undefined {
    return this.args.hiveFormat;
  }
}

export type CredentialsPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { expressions: Expression[] },
]>;

export class CredentialsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.CREDENTIALS_PROPERTY;

  static argTypes: RequiredMap<CredentialsPropertyExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: CredentialsPropertyExprArgs;

  constructor (args: CredentialsPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type FreespacePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    this: Expression;
    percent?: Expression;
  },
]>;

export class FreespacePropertyExpr extends PropertyExpr {
  key = ExpressionKey.FREESPACE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for FreespaceProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<FreespacePropertyExprArgs> = {
    ...super.argTypes,
    this: true,
    percent: false,
  };

  declare args: FreespacePropertyExprArgs;

  constructor (args: FreespacePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $percent (): Expression | undefined {
    return this.args.percent;
  }
}

export type GlobalPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class GlobalPropertyExpr extends PropertyExpr {
  key = ExpressionKey.GLOBAL_PROPERTY;

  static argTypes: RequiredMap<GlobalPropertyExprArgs> = {
    ...super.argTypes,
    value: true,
  };

  declare args: GlobalPropertyExprArgs;

  constructor (args: GlobalPropertyExprArgs) {
    super(args);
  }
}

export type IcebergPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class IcebergPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ICEBERG_PROPERTY;

  static argTypes: RequiredMap<IcebergPropertyExprArgs> = {
    ...super.argTypes,
    value: true,
  };

  declare args: IcebergPropertyExprArgs;

  constructor (args: IcebergPropertyExprArgs) {
    super(args);
  }
}

export type InheritsPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { expressions?: Expression[] },
]>;

export class InheritsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.INHERITS_PROPERTY;

  static argTypes: RequiredMap<InheritsPropertyExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: InheritsPropertyExprArgs;

  constructor (args: InheritsPropertyExprArgs) {
    super(args);
  }
}

export type InputModelPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class InputModelPropertyExpr extends PropertyExpr {
  key = ExpressionKey.INPUT_MODEL_PROPERTY;

  static argTypes: RequiredMap<InputModelPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: InputModelPropertyExprArgs;

  constructor (args: InputModelPropertyExprArgs) {
    super(args);
  }
}

export type OutputModelPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class OutputModelPropertyExpr extends PropertyExpr {
  key = ExpressionKey.OUTPUT_MODEL_PROPERTY;

  static argTypes: RequiredMap<OutputModelPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: OutputModelPropertyExprArgs;

  constructor (args: OutputModelPropertyExprArgs) {
    super(args);
  }
}

export type IsolatedLoadingPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    no?: Expression;
    concurrent?: Expression;
    target?: Expression;
  },
]>;

export class IsolatedLoadingPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ISOLATED_LOADING_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for IsolatedLoadingProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<IsolatedLoadingPropertyExprArgs> = {
    ...super.argTypes,
    no: false,
    concurrent: false,
    target: false,
  };

  declare args: IsolatedLoadingPropertyExprArgs;

  constructor (args: IsolatedLoadingPropertyExprArgs) {
    super(args);
  }

  get $no (): Expression | undefined {
    return this.args.no;
  }

  get $concurrent (): Expression | undefined {
    return this.args.concurrent;
  }

  get $target (): Expression | undefined {
    return this.args.target;
  }
}

export type JournalPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    no?: Expression;
    dual?: Expression;
    before?: Expression;
    local?: Expression;
    after?: Expression;
  },
]>;

export class JournalPropertyExpr extends PropertyExpr {
  key = ExpressionKey.JOURNAL_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for JournalProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JournalPropertyExprArgs> = {
    ...super.argTypes,
    no: false,
    dual: false,
    before: false,
    local: false,
    after: false,
  };

  declare args: JournalPropertyExprArgs;

  constructor (args: JournalPropertyExprArgs) {
    super(args);
  }

  get $no (): Expression | undefined {
    return this.args.no;
  }

  get $dual (): Expression | undefined {
    return this.args.dual;
  }

  get $before (): Expression | undefined {
    return this.args.before;
  }

  get $local (): Expression | undefined {
    return this.args.local;
  }

  get $after (): Expression | undefined {
    return this.args.after;
  }
}

export type LanguagePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class LanguagePropertyExpr extends PropertyExpr {
  key = ExpressionKey.LANGUAGE_PROPERTY;

  static argTypes: RequiredMap<LanguagePropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: LanguagePropertyExprArgs;

  constructor (args: LanguagePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type EnviromentPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { expressions: Expression[] },
]>;

export class EnviromentPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ENVIROMENT_PROPERTY;

  static argTypes: RequiredMap<EnviromentPropertyExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: EnviromentPropertyExprArgs;

  constructor (args: EnviromentPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type ClusteredByPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    expressions: Expression[];
    sortedBy?: string;
    buckets: Expression[];
  },
]>;

export class ClusteredByPropertyExpr extends PropertyExpr {
  key = ExpressionKey.CLUSTERED_BY_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for ClusteredByProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ClusteredByPropertyExprArgs> = {
    ...super.argTypes,
    expressions: true,
    sortedBy: false,
    buckets: true,
  };

  declare args: ClusteredByPropertyExprArgs;

  constructor (args: ClusteredByPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $sortedBy (): string | undefined {
    return this.args.sortedBy;
  }

  get $buckets (): Expression[] {
    return this.args.buckets;
  }
}

/**
 * Enumeration of valid kind values for DictProperty expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum DictPropertyExprKind {
  FLAT = 'FLAT',
  HASHED = 'HASHED',
  RANGE_HASHED = 'RANGE_HASHED',
  CACHE = 'CACHE',
  DIRECT = 'DIRECT',
  IP_TRIE = 'IP_TRIE',
  POLYGON = 'POLYGON',
}

export type DictPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    this: Expression;
    kind: DictPropertyExprKind;
    settings?: Expression[];
  },
]>;

export class DictPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DICT_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for DictProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<DictPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
    kind: true,
    settings: false,
  };

  declare args: DictPropertyExprArgs;

  constructor (args: DictPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): DictPropertyExprKind {
    return this.args.kind;
  }

  get $settings (): Expression[] | undefined {
    return this.args.settings;
  }
}

export type DictSubPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class DictSubPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DICT_SUB_PROPERTY;

  static argTypes: RequiredMap<DictSubPropertyExprArgs> = {
    ...super.argTypes,
  };

  declare args: DictSubPropertyExprArgs;

  constructor (args: DictSubPropertyExprArgs) {
    super(args);
  }
}

export type DictRangeExprArgs = Merge<[
  PropertyExprArgs,
  {
    min: Expression;
    max: Expression;
    this: Expression;
  },
]>;

export class DictRangeExpr extends PropertyExpr {
  key = ExpressionKey.DICT_RANGE;

  /**
   * Defines the arguments (properties and child expressions) for DictRange expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<DictRangeExprArgs> = {
    ...super.argTypes,
    this: true,
    min: true,
    max: true,
  };

  declare args: DictRangeExprArgs;

  constructor (args: DictRangeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $min (): Expression {
    return this.args.min;
  }

  get $max (): Expression {
    return this.args.max;
  }
}

export type DynamicPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class DynamicPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DYNAMIC_PROPERTY;

  static argTypes: RequiredMap<DynamicPropertyExprArgs> = {
    ...super.argTypes,
    value: true,
  };

  declare args: DynamicPropertyExprArgs;

  constructor (args: DynamicPropertyExprArgs) {
    super(args);
  }
}

export type OnClusterExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class OnClusterExpr extends PropertyExpr {
  key = ExpressionKey.ON_CLUSTER;

  static argTypes: RequiredMap<OnClusterExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: OnClusterExprArgs;

  constructor (args: OnClusterExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type EmptyPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class EmptyPropertyExpr extends PropertyExpr {
  key = ExpressionKey.EMPTY_PROPERTY;

  static argTypes: RequiredMap<EmptyPropertyExprArgs> = {
    ...super.argTypes,
    value: true,
  };

  declare args: EmptyPropertyExprArgs;

  constructor (args: EmptyPropertyExprArgs) {
    super(args);
  }
}

export type LikePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class LikePropertyExpr extends PropertyExpr {
  key = ExpressionKey.LIKE_PROPERTY;

  static argTypes: RequiredMap<LikePropertyExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
  };

  declare args: LikePropertyExprArgs;

  constructor (args: LikePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

export type LocationPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class LocationPropertyExpr extends PropertyExpr {
  key = ExpressionKey.LOCATION_PROPERTY;

  static argTypes: RequiredMap<LocationPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: LocationPropertyExprArgs;

  constructor (args: LocationPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type LockPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class LockPropertyExpr extends PropertyExpr {
  key = ExpressionKey.LOCK_PROPERTY;

  static argTypes: RequiredMap<LockPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: LockPropertyExprArgs;

  constructor (args: LockPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

/**
 * Enumeration of valid kind values for LockingProperty expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum LockingPropertyExprKind {
  DEFAULT = 'DEFAULT',
  ALL = 'ALL',
  FALLBACK = 'FALLBACK',
  LOCKING = 'LOCKING',
}

export type LockingPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    kind: LockingPropertyExprKind;
    forOrIn?: Expression;
    lockType: DataTypeExpr;
    override?: Expression;
    this?: Expression;
  },
]>;

export class LockingPropertyExpr extends PropertyExpr {
  key = ExpressionKey.LOCKING_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for LockingProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<LockingPropertyExprArgs> = {
    ...super.argTypes,
    kind: true,
    forOrIn: false,
    lockType: true,
    override: false,
  };

  declare args: LockingPropertyExprArgs;

  constructor (args: LockingPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get $kind (): LockingPropertyExprKind {
    return this.args.kind;
  }

  get $forOrIn (): Expression | undefined {
    return this.args.forOrIn;
  }

  get $lockType (): Expression {
    return this.args.lockType;
  }

  get $override (): Expression | undefined {
    return this.args.override;
  }
}

export type LogPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { no: Expression },
]>;

export class LogPropertyExpr extends PropertyExpr {
  key = ExpressionKey.LOG_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for LogProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<LogPropertyExprArgs> = {
    ...super.argTypes,
    no: true,
  };

  declare args: LogPropertyExprArgs;

  constructor (args: LogPropertyExprArgs) {
    super(args);
  }

  get $no (): Expression {
    return this.args.no;
  }
}

export type MaterializedPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this?: Expression;
  },
]>;

export class MaterializedPropertyExpr extends PropertyExpr {
  key = ExpressionKey.MATERIALIZED_PROPERTY;

  static argTypes: RequiredMap<MaterializedPropertyExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: MaterializedPropertyExprArgs;

  constructor (args: MaterializedPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type MergeBlockRatioPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    no?: Expression;
    default?: Expression;
    percent?: Expression;
    this?: Expression;
  },
]>;

export class MergeBlockRatioPropertyExpr extends PropertyExpr {
  key = ExpressionKey.MERGE_BLOCK_RATIO_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for MergeBlockRatioProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<MergeBlockRatioPropertyExprArgs> = {
    ...super.argTypes,
    this: false,
    no: false,
    default: false,
    percent: false,
  };

  declare args: MergeBlockRatioPropertyExprArgs;

  constructor (args: MergeBlockRatioPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $no (): Expression | undefined {
    return this.args.no;
  }

  get $default (): Expression | undefined {
    return this.args.default;
  }

  get $percent (): Expression | undefined {
    return this.args.percent;
  }
}

export type NoPrimaryIndexPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class NoPrimaryIndexPropertyExpr extends PropertyExpr {
  key = ExpressionKey.NO_PRIMARY_INDEX_PROPERTY;

  static argTypes: RequiredMap<NoPrimaryIndexPropertyExprArgs> = {
    ...super.argTypes,
    value: true,
  };

  declare args: NoPrimaryIndexPropertyExprArgs;

  constructor (args: NoPrimaryIndexPropertyExprArgs) {
    super(args);
  }
}

export type OnPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;
export class OnPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ON_PROPERTY;

  static argTypes: RequiredMap<OnPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: OnPropertyExprArgs;

  constructor (args: OnPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type OnCommitPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { delete?: boolean },
]>;

export class OnCommitPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ON_COMMIT_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for OnCommitProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<OnCommitPropertyExprArgs> = {
    ...super.argTypes,
    delete: false,
  };

  declare args: OnCommitPropertyExprArgs;

  constructor (args: OnCommitPropertyExprArgs) {
    super(args);
  }

  get $delete (): boolean | undefined {
    return this.args.delete;
  }
}

export type PartitionedByPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class PartitionedByPropertyExpr extends PropertyExpr {
  key = ExpressionKey.PARTITIONED_BY_PROPERTY;

  static argTypes: RequiredMap<PartitionedByPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: PartitionedByPropertyExprArgs;

  constructor (args: PartitionedByPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type PartitionedByBucketExprArgs = Merge<[
  PropertyExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class PartitionedByBucketExpr extends PropertyExpr {
  key = ExpressionKey.PARTITIONED_BY_BUCKET;

  static argTypes: RequiredMap<PartitionedByBucketExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: PartitionedByBucketExprArgs;

  constructor (args: PartitionedByBucketExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type PartitionByTruncateExprArgs = Merge<[
  PropertyExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class PartitionByTruncateExpr extends PropertyExpr {
  key = ExpressionKey.PARTITION_BY_TRUNCATE;

  static argTypes: RequiredMap<PartitionByTruncateExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: PartitionByTruncateExprArgs;

  constructor (args: PartitionByTruncateExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type PartitionByRangePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    partitionExpressions: Expression[];
    createExpressions: Expression[];
  },
]>;

export class PartitionByRangePropertyExpr extends PropertyExpr {
  key = ExpressionKey.PARTITION_BY_RANGE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for PartitionByRangeProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<PartitionByRangePropertyExprArgs> = {
    ...super.argTypes,
    partitionExpressions: true,
    createExpressions: true,
  };

  declare args: PartitionByRangePropertyExprArgs;

  constructor (args: PartitionByRangePropertyExprArgs) {
    super(args);
  }

  get $partitionExpressions (): Expression[] {
    return this.args.partitionExpressions;
  }

  get $createExpressions (): Expression[] {
    return this.args.createExpressions;
  }
}

export type RollupPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { expressions: Expression[] },
]>;

export class RollupPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ROLLUP_PROPERTY;

  static argTypes: RequiredMap<RollupPropertyExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: RollupPropertyExprArgs;

  constructor (args: RollupPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type PartitionByListPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    partitionExpressions: Expression[];
    createExpressions: Expression[];
  },
]>;

export class PartitionByListPropertyExpr extends PropertyExpr {
  key = ExpressionKey.PARTITION_BY_LIST_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for PartitionByListProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<PartitionByListPropertyExprArgs> = {
    ...super.argTypes,
    partitionExpressions: true,
    createExpressions: true,
  };

  declare args: PartitionByListPropertyExprArgs;

  constructor (args: PartitionByListPropertyExprArgs) {
    super(args);
  }

  get $partitionExpressions (): Expression[] {
    return this.args.partitionExpressions;
  }

  get $createExpressions (): Expression[] {
    return this.args.createExpressions;
  }
}

/**
 * Valid kind values for refresh trigger properties
 */
export enum RefreshTriggerPropertyExprKind {
  ON_COMMIT = 'ON_COMMIT',
  ON_DEMAND = 'ON_DEMAND',
  START_WITH = 'START_WITH',
  NEXT = 'NEXT',
}
export type RefreshTriggerPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    method?: string;
    kind?: RefreshTriggerPropertyExprKind;
    every?: Expression;
    unit?: Expression;
    starts?: Expression[];
  },
]>;

export class RefreshTriggerPropertyExpr extends PropertyExpr {
  key = ExpressionKey.REFRESH_TRIGGER_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for RefreshTriggerProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<RefreshTriggerPropertyExprArgs> = {
    ...super.argTypes,
    method: false,
    kind: false,
    every: false,
    unit: false,
    starts: false,
  };

  declare args: RefreshTriggerPropertyExprArgs;

  constructor (args: RefreshTriggerPropertyExprArgs) {
    super(args);
  }

  get $method (): string | undefined {
    return this.args.method;
  }

  get $kind (): RefreshTriggerPropertyExprKind | undefined {
    return this.args.kind;
  }

  get $every (): Expression | undefined {
    return this.args.every;
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  get $starts (): Expression[] | undefined {
    return this.args.starts;
  }
}

export type UniqueKeyPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { expressions: Expression[] },
]>;

export class UniqueKeyPropertyExpr extends PropertyExpr {
  key = ExpressionKey.UNIQUE_KEY_PROPERTY;

  static argTypes: RequiredMap<UniqueKeyPropertyExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: UniqueKeyPropertyExprArgs;

  constructor (args: UniqueKeyPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type PartitionedOfPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class PartitionedOfPropertyExpr extends PropertyExpr {
  key = ExpressionKey.PARTITIONED_OF_PROPERTY;

  static argTypes: RequiredMap<PartitionedOfPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: PartitionedOfPropertyExprArgs;

  constructor (args: PartitionedOfPropertyExprArgs) {
    super(args);
  }
}

export type StreamingTablePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class StreamingTablePropertyExpr extends PropertyExpr {
  key = ExpressionKey.STREAMING_TABLE_PROPERTY;

  static argTypes: RequiredMap<StreamingTablePropertyExprArgs> = {
    ...super.argTypes,
    value: true,
  };

  declare args: StreamingTablePropertyExprArgs;

  constructor (args: StreamingTablePropertyExprArgs) {
    super(args);
  }
}

export type RemoteWithConnectionModelPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this: Expression;
  },
]>;

export class RemoteWithConnectionModelPropertyExpr extends PropertyExpr {
  key = ExpressionKey.REMOTE_WITH_CONNECTION_MODEL_PROPERTY;

  static argTypes: RequiredMap<RemoteWithConnectionModelPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: RemoteWithConnectionModelPropertyExprArgs;

  constructor (args: RemoteWithConnectionModelPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $value (): string | undefined {
    return this.args.value;
  }
}

export type ReturnsPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this?: Expression;
    isTable?: Expression;
    table?: Expression;
    null?: Expression;
  },
]>;

export class ReturnsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.RETURNS_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for ReturnsProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ReturnsPropertyExprArgs> = {
    ...super.argTypes,
    this: false,
    isTable: false,
    table: false,
    null: false,
  };

  declare args: ReturnsPropertyExprArgs;

  constructor (args: ReturnsPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $isTable (): Expression | undefined {
    return this.args.isTable;
  }

  get $table (): Expression | undefined {
    return this.args.table;
  }

  get $null (): Expression | undefined {
    return this.args.null;
  }
}

export type StrictPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class StrictPropertyExpr extends PropertyExpr {
  key = ExpressionKey.STRICT_PROPERTY;

  static argTypes: RequiredMap<StrictPropertyExprArgs> = {
    ...super.argTypes,
    value: true,
  };

  declare args: StrictPropertyExprArgs;

  constructor (args: StrictPropertyExprArgs) {
    super(args);
  }
}

export type RowFormatPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class RowFormatPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ROW_FORMAT_PROPERTY;

  static argTypes: RequiredMap<RowFormatPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: RowFormatPropertyExprArgs;

  constructor (args: RowFormatPropertyExprArgs) {
    super(args);
  }
}

export type RowFormatDelimitedPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    fields?: Expression[];
    escaped?: Expression;
    collectionItems?: Expression[];
    mapKeys?: Expression[];
    lines?: Expression[];
    null?: Expression;
    serde?: Expression;
  },
]>;

export class RowFormatDelimitedPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ROW_FORMAT_DELIMITED_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for RowFormatDelimitedProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<RowFormatDelimitedPropertyExprArgs> = {
    ...super.argTypes,
    fields: false,
    escaped: false,
    collectionItems: false,
    mapKeys: false,
    lines: false,
    null: false,
    serde: false,
  };

  declare args: RowFormatDelimitedPropertyExprArgs;

  constructor (args: RowFormatDelimitedPropertyExprArgs) {
    super(args);
  }

  get $fields (): Expression[] | undefined {
    return this.args.fields;
  }

  get $escaped (): Expression | undefined {
    return this.args.escaped;
  }

  get $collectionItems (): Expression[] | undefined {
    return this.args.collectionItems;
  }

  get $mapKeys (): Expression[] | undefined {
    return this.args.mapKeys;
  }

  get $lines (): Expression[] | undefined {
    return this.args.lines;
  }

  get $null (): Expression | undefined {
    return this.args.null;
  }

  get $serde (): Expression | undefined {
    return this.args.serde;
  }
}

export type RowFormatSerdePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    this: Expression;
    serdeProperties?: Expression[];
  },
]>;

export class RowFormatSerdePropertyExpr extends PropertyExpr {
  key = ExpressionKey.ROW_FORMAT_SERDE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for RowFormatSerdeProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<RowFormatSerdePropertyExprArgs> = {
    ...super.argTypes,
    this: true,
    serdeProperties: false,
  };

  declare args: RowFormatSerdePropertyExprArgs;

  constructor (args: RowFormatSerdePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $serdeProperties (): Expression[] | undefined {
    return this.args.serdeProperties;
  }
}

export type SamplePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class SamplePropertyExpr extends PropertyExpr {
  key = ExpressionKey.SAMPLE_PROPERTY;

  static argTypes: RequiredMap<SamplePropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: SamplePropertyExprArgs;

  constructor (args: SamplePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type SecurityPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class SecurityPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SECURITY_PROPERTY;

  static argTypes: RequiredMap<SecurityPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: SecurityPropertyExprArgs;

  constructor (args: SecurityPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type SchemaCommentPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class SchemaCommentPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SCHEMA_COMMENT_PROPERTY;

  static argTypes: RequiredMap<SchemaCommentPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: SchemaCommentPropertyExprArgs;

  constructor (args: SchemaCommentPropertyExprArgs) {
    super(args);
  }
}

export type SerdePropertiesExprArgs = Merge<[
  PropertyExprArgs,
  {
    expressions: Expression[];
    with?: Expression;
  },
]>;

export class SerdePropertiesExpr extends PropertyExpr {
  key = ExpressionKey.SERDE_PROPERTIES;

  /**
   * Defines the arguments (properties and child expressions) for SerdeProperties expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<SerdePropertiesExprArgs> = {
    ...super.argTypes,
    expressions: true,
    with: false,
  };

  declare args: SerdePropertiesExprArgs;

  constructor (args: SerdePropertiesExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $with (): Expression | undefined {
    return this.args.with;
  }
}

export type SetPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { multi: Expression },
]>;

export class SetPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SET_PROPERTY;

  static argTypes: RequiredMap<SetPropertyExprArgs> = {
    ...super.argTypes,
    multi: true,
  };

  declare args: SetPropertyExprArgs;

  constructor (args: SetPropertyExprArgs) {
    super(args);
  }

  get $multi (): Expression {
    return this.args.multi;
  }
}

export type SharingPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this?: Expression;
  },
]>;

export class SharingPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SHARING_PROPERTY;

  static argTypes: RequiredMap<SharingPropertyExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: SharingPropertyExprArgs;

  constructor (args: SetPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type SetConfigPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class SetConfigPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SET_CONFIG_PROPERTY;

  static argTypes: RequiredMap<SetConfigPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: SetConfigPropertyExprArgs;

  constructor (args: SetConfigPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type SettingsPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { expressions: Expression[] },
]>;

export class SettingsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SETTINGS_PROPERTY;

  static argTypes: RequiredMap<SettingsPropertyExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: SettingsPropertyExprArgs;

  constructor (args: SettingsPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type SortKeyPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    this: Expression;
    compound?: Expression;
  },
]>;

export class SortKeyPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SORT_KEY_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for SortKeyProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<SortKeyPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
    compound: false,
  };

  declare args: SortKeyPropertyExprArgs;

  constructor (args: SortKeyPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $compound (): Expression | undefined {
    return this.args.compound;
  }
}

export type SqlReadWritePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class SqlReadWritePropertyExpr extends PropertyExpr {
  key = ExpressionKey.SQL_READ_WRITE_PROPERTY;

  static argTypes: RequiredMap<SqlReadWritePropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: SqlReadWritePropertyExprArgs;

  constructor (args: SqlReadWritePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type SqlSecurityPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class SqlSecurityPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SQL_SECURITY_PROPERTY;

  static argTypes: RequiredMap<SqlSecurityPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: SqlSecurityPropertyExprArgs;

  constructor (args: SqlSecurityPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type StabilityPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class StabilityPropertyExpr extends PropertyExpr {
  key = ExpressionKey.STABILITY_PROPERTY;

  static argTypes: RequiredMap<StabilityPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: StabilityPropertyExprArgs;

  constructor (args: StabilityPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type StorageHandlerPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class StorageHandlerPropertyExpr extends PropertyExpr {
  key = ExpressionKey.STORAGE_HANDLER_PROPERTY;

  static argTypes: RequiredMap<StorageHandlerPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: StorageHandlerPropertyExprArgs;

  constructor (args: StorageHandlerPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type TemporaryPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this?: Expression;
  },
]>;

export class TemporaryPropertyExpr extends PropertyExpr {
  key = ExpressionKey.TEMPORARY_PROPERTY;

  static argTypes: RequiredMap<TemporaryPropertyExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: TemporaryPropertyExprArgs;

  constructor (args: TemporaryPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type SecurePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class SecurePropertyExpr extends PropertyExpr {
  key = ExpressionKey.SECURE_PROPERTY;

  static argTypes: RequiredMap<SecurePropertyExprArgs> = {
    ...super.argTypes,
    value: true,
  };

  declare args: SecurePropertyExprArgs;

  constructor (args: SecurePropertyExprArgs) {
    super(args);
  }
}

export type TagsExprArgs = Merge<[
  PropertyExprArgs,
  ColumnConstraintKindExprArgs,
  {
    expressions: Expression[];
    this?: Expression;
    value?: string | Expression;
  },
]>;

export class TagsExpr extends multiInherit(ColumnConstraintKindExpr, PropertyExpr) {
  key = ExpressionKey.TAGS;

  static argTypes: RequiredMap<TagsExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    expressions: true,
  };

  declare args: TagsExprArgs;

  constructor (args: TagsExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type TransformModelPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { expressions: Expression[] },
]>;

export class TransformModelPropertyExpr extends PropertyExpr {
  key = ExpressionKey.TRANSFORM_MODEL_PROPERTY;

  static argTypes: RequiredMap<TransformModelPropertyExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: TransformModelPropertyExprArgs;

  constructor (args: TransformModelPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type TransientPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this?: Expression;
  },
]>;

export class TransientPropertyExpr extends PropertyExpr {
  key = ExpressionKey.TRANSIENT_PROPERTY;

  static argTypes: RequiredMap<TransientPropertyExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: TransientPropertyExprArgs;

  constructor (args: TransientPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type UnloggedPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class UnloggedPropertyExpr extends PropertyExpr {
  key = ExpressionKey.UNLOGGED_PROPERTY;

  static argTypes: RequiredMap<UnloggedPropertyExprArgs> = {
    ...super.argTypes,
    value: true,
  };

  declare args: UnloggedPropertyExprArgs;

  constructor (args: UnloggedPropertyExprArgs) {
    super(args);
  }
}

export type UsingTemplatePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class UsingTemplatePropertyExpr extends PropertyExpr {
  key = ExpressionKey.USING_TEMPLATE_PROPERTY;

  static argTypes: RequiredMap<UsingTemplatePropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: UsingTemplatePropertyExprArgs;

  constructor (args: UsingTemplatePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type ViewAttributePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class ViewAttributePropertyExpr extends PropertyExpr {
  key = ExpressionKey.VIEW_ATTRIBUTE_PROPERTY;

  static argTypes: RequiredMap<ViewAttributePropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: ViewAttributePropertyExprArgs;

  constructor (args: ViewAttributePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type VolatilePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this?: Expression;
  },
]>;

export class VolatilePropertyExpr extends PropertyExpr {
  key = ExpressionKey.VOLATILE_PROPERTY;

  static argTypes: RequiredMap<VolatilePropertyExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: VolatilePropertyExprArgs;

  constructor (args: VolatilePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type WithDataPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    no: Expression;
    statistics?: Expression[];
  },
]>;

export class WithDataPropertyExpr extends PropertyExpr {
  key = ExpressionKey.WITH_DATA_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for WithDataProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<WithDataPropertyExprArgs> = {
    ...super.argTypes,
    no: true,
    statistics: false,
  };

  declare args: WithDataPropertyExprArgs;

  constructor (args: WithDataPropertyExprArgs) {
    super(args);
  }

  get $no (): Expression {
    return this.args.no;
  }

  get $statistics (): Expression[] | undefined {
    return this.args.statistics;
  }
}

export type WithJournalTablePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class WithJournalTablePropertyExpr extends PropertyExpr {
  key = ExpressionKey.WITH_JOURNAL_TABLE_PROPERTY;

  static argTypes: RequiredMap<WithJournalTablePropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: WithJournalTablePropertyExprArgs;

  constructor (args: WithJournalTablePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type WithSchemaBindingPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this: Expression },
]>;

export class WithSchemaBindingPropertyExpr extends PropertyExpr {
  key = ExpressionKey.WITH_SCHEMA_BINDING_PROPERTY;

  static argTypes: RequiredMap<WithSchemaBindingPropertyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: WithSchemaBindingPropertyExprArgs;

  constructor (args: WithSchemaBindingPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type WithSystemVersioningPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    on?: Expression;
    dataConsistency?: Expression;
    retentionPeriod?: Expression;
    with: Expression;
    this?: Expression;
  },
]>;

export class WithSystemVersioningPropertyExpr extends PropertyExpr {
  key = ExpressionKey.WITH_SYSTEM_VERSIONING_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for WithSystemVersioningProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<WithSystemVersioningPropertyExprArgs> = {
    ...super.argTypes,
    on: false,
    dataConsistency: false,
    retentionPeriod: false,
    with: true,
  };

  declare args: WithSystemVersioningPropertyExprArgs;

  constructor (args: WithSystemVersioningPropertyExprArgs) {
    super(args);
  }

  get $on (): Expression | undefined {
    return this.args.on;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $dataConsistency (): Expression | undefined {
    return this.args.dataConsistency;
  }

  get $retentionPeriod (): Expression | undefined {
    return this.args.retentionPeriod;
  }

  get $with (): Expression {
    return this.args.with;
  }
}

export type WithProcedureOptionsExprArgs = Merge<[
  PropertyExprArgs,
  { expressions: Expression[] },
]>;

export class WithProcedureOptionsExpr extends PropertyExpr {
  key = ExpressionKey.WITH_PROCEDURE_OPTIONS;

  static argTypes: RequiredMap<WithProcedureOptionsExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: WithProcedureOptionsExprArgs;

  constructor (args: WithProcedureOptionsExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type EncodePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    this: Expression;
    properties?: Expression[];
    key?: Expression;
  },
]>;

export class EncodePropertyExpr extends PropertyExpr {
  key = ExpressionKey.ENCODE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for EncodeProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   * Note: The 'key' argument can be accessed via this.args.key (no getter to avoid conflict with
   * Expression.key).
   */
  static argTypes: RequiredMap<EncodePropertyExprArgs> = {
    ...super.argTypes,
    this: true,
    properties: false,
    key: false,
  };

  declare args: EncodePropertyExprArgs;

  constructor (args: EncodePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $properties (): Expression[] | undefined {
    return this.args.properties;
  }

  get $key (): Expression | undefined {
    return this.args.key;
  }
}

export type IncludePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this: Expression;
    alias?: string | TableAliasExpr | IdentifierExpr;
    columnDef?: Expression;
  },
]>;

export class IncludePropertyExpr extends PropertyExpr {
  key = ExpressionKey.INCLUDE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for IncludeProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   * Note: The 'alias' argument can be accessed via this.args.alias (no getter to avoid conflict
   * with Expression.alias).
   */
  static argTypes: RequiredMap<IncludePropertyExprArgs> = {
    ...super.argTypes,
    this: true,
    alias: false,
    columnDef: false,
  };

  declare args: IncludePropertyExprArgs;

  constructor (args: IncludePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $alias (): string | TableAliasExpr | IdentifierExpr | undefined {
    return this.args.alias;
  }

  get $columnDef (): Expression | undefined {
    return this.args.columnDef;
  }
}

export type ForcePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class ForcePropertyExpr extends PropertyExpr {
  key = ExpressionKey.FORCE_PROPERTY;

  static argTypes: RequiredMap<ForcePropertyExprArgs> = {
    ...super.argTypes,
    value: true,
  };

  declare args: ForcePropertyExprArgs;

  constructor (args: ForcePropertyExprArgs) {
    super(args);
  }
}

/**
 * Enumeration of CREATE property locations
 * Form: schema specified
 *   create [POST_CREATE]
 *     table a [POST_NAME]
 *     (b int) [POST_SCHEMA]
 *     with ([POST_WITH])
 *     index (b) [POST_INDEX]
 *
 * Form: alias selection
 *   create [POST_CREATE]
 *     table a [POST_NAME]
 *     as [POST_ALIAS] (select * from b) [POST_EXPRESSION]
 *     index (c) [POST_INDEX]
 */
export enum PropertiesLocation {
  POST_CREATE = 'POST_CREATE',
  POST_NAME = 'POST_NAME',
  POST_SCHEMA = 'POST_SCHEMA',
  POST_WITH = 'POST_WITH',
  POST_ALIAS = 'POST_ALIAS',
  POST_EXPRESSION = 'POST_EXPRESSION',
  POST_INDEX = 'POST_INDEX',
  UNSUPPORTED = 'UNSUPPORTED',
}

export type PropertiesExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions: Expression[] },
]>;

export class PropertiesExpr extends Expression {
  key = ExpressionKey.PROPERTIES;

  static argTypes: RequiredMap<PropertiesExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: PropertiesExprArgs;

  constructor (args: PropertiesExprArgs) {
    super(args);
  }

  static NAME_TO_PROPERTY = {
    'ALGORITHM': AlgorithmPropertyExpr,
    'AUTO_INCREMENT': AutoIncrementPropertyExpr,
    'CHARACTER SET': CharacterSetPropertyExpr,
    'CLUSTERED_BY': ClusteredByPropertyExpr,
    'COLLATE': CollatePropertyExpr,
    'COMMENT': SchemaCommentPropertyExpr,
    'CREDENTIALS': CredentialsPropertyExpr,
    'DEFINER': DefinerPropertyExpr,
    'DISTKEY': DistKeyPropertyExpr,
    'DISTRIBUTED_BY': DistributedByPropertyExpr,
    'DISTSTYLE': DistStylePropertyExpr,
    'ENGINE': EnginePropertyExpr,
    'EXECUTE AS': ExecuteAsPropertyExpr,
    'FORMAT': FileFormatPropertyExpr,
    'LANGUAGE': LanguagePropertyExpr,
    'LOCATION': LocationPropertyExpr,
    'LOCK': LockPropertyExpr,
    'PARTITIONED_BY': PartitionedByPropertyExpr,
    'RETURNS': ReturnsPropertyExpr,
    'ROW_FORMAT': RowFormatPropertyExpr,
    'SORTKEY': SortKeyPropertyExpr,
    'ENCODE': EncodePropertyExpr,
    'INCLUDE': IncludePropertyExpr,
  } as const;

  static PROPERTY_TO_NAME: Record<string, string> = Object.fromEntries(
    Object.entries(PropertiesExpr.NAME_TO_PROPERTY).map(([k, v]) => [v.name, k]),
  );

  /**
   * Creates a Properties expression from a dictionary of property key-value pairs.
   *
   * @param propertiesDict - Dictionary mapping property names to their values
   * @returns A Properties expression containing the property expressions
   */
  static fromDict (propertiesDict: Record<string, unknown>): PropertiesExpr {
    const expressions: Expression[] = [];

    for (const [key, value] of Object.entries(propertiesDict)) {
      const propertyClass = PropertiesExpr.NAME_TO_PROPERTY[key.toUpperCase() as keyof typeof PropertiesExpr.NAME_TO_PROPERTY];
      if (propertyClass) {
        // @ts-expect-error "sqlglot is intrinsically type-unsafe here"
        expressions.push(new propertyClass({ this: convert(value) }));
      } else {
        expressions.push(new PropertyExpr({
          this: LiteralExpr.string(key),
          value: convert(value),
        }));
      }
    }

    return new PropertiesExpr({ expressions });
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

/**
 * Enumeration of valid kind values for SetOperation expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum SetOperationExprKind {
  UNION = 'UNION',
  INTERSECT = 'INTERSECT',
  EXCEPT = 'EXCEPT',
}

export type SetOperationExprArgs = Merge<[
  QueryExprArgs,
  {
    this: QueryExpr;
    expression: QueryExpr;
    distinct?: boolean;
    byName?: string;
    side?: string;
    kind?: SetOperationExprKind;
    on?: Expression;
    match?: Expression;
    laterals?: Expression[];
    joins?: JoinExpr[];
    connect?: Expression;
    pivots?: Expression[];
    prewhere?: Expression;
    where?: Expression;
    group?: Expression;
    having?: Expression;
    qualify?: Expression;
    windows?: Expression[];
    distribute?: Expression;
    sort?: Expression;
    cluster?: Expression;
    order?: Expression;
    limit?: number | Expression;
    offset?: number | Expression;
    locks?: Expression[];
    sample?: number | Expression;
  },
]>;

export class SetOperationExpr extends QueryExpr {
  key = ExpressionKey.SET_OPERATION;

  static argTypes: RequiredMap<SetOperationExprArgs> = {
    ...super.argTypes,
    with: false,
    this: true,
    expression: true,
    distinct: false,
    byName: false,
    side: false,
    kind: false,
    on: false,
    ...QUERY_MODIFIERS,
  };

  declare args: SetOperationExprArgs;

  constructor (args: SetOperationExprArgs) {
    super(args);
  }

  get $with (): Expression | undefined {
    return this.args.with;
  }

  get $this (): QueryExpr {
    return this.args.this;
  }

  get $expression (): QueryExpr {
    return this.args.expression;
  }

  get $distinct (): boolean | undefined {
    return this.args.distinct;
  }

  get $byName (): string | undefined {
    return this.args.byName;
  }

  get $side (): string | undefined {
    return this.args.side;
  }

  get $kind (): SetOperationExprKind | undefined {
    return this.args.kind;
  }

  get $on (): Expression | undefined {
    return this.args.on;
  }

  /**
   * Applies select expressions to both sides of the set operation.
   */
  select (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      copy = true, ...restOptions
    } = options;
    const expressionList = ensureList(expressions);
    const self = maybeCopy(this, copy);
    self.$this.unnest().select(expressionList, {
      ...restOptions,
      copy: false,
    });
    self.$expression.unnest().select(expressionList, {
      ...restOptions,
      copy: false,
      append: options?.append ?? true,
    });

    return self;
  }

  /**
   * Returns named selects from the underlying query.
   * Walks up the SetOperation chain to find the base query.
   */
  get namedSelects (): string[] {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let expression: QueryExpr = this;
    while (expression instanceof SetOperationExpr) {
      expression = expression.args.this.unnest();
    }
    return expression.namedSelects;
  }

  /**
   * Returns true if either side of the set operation is a star select.
   */
  get isStar (): boolean {
    const leftIsStar = this.args.this.isStar;
    const rightIsStar = this.args.expression.isStar;
    return leftIsStar || rightIsStar;
  }

  /**
   * Returns selects from the underlying query.
   * Walks up the SetOperation chain to find the base query.
   */
  get selects (): Expression[] {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let expression: QueryExpr = this;
    while (expression instanceof SetOperationExpr) {
      expression = expression.args.this.unnest();
    }
    return expression.selects;
  }

  /**
   * Returns the left side of the set operation.
   */
  get left (): QueryExpr {
    return this.args.this;
  }

  /**
   * Returns the right side of the set operation.
   */
  get right (): QueryExpr {
    return this.args.expression;
  }

  /**
   * Returns the kind of set operation as uppercase string.
   */
  get kind (): SetOperationExprKind | undefined {
    return this.args.kind;
  }

  /**
   * Returns the side as uppercase string.
   */
  get side (): string {
    return this.text('side').toUpperCase();
  }
}

export type UpdateExprArgs = Merge<[
  DMLExprArgs,
  {
    with?: Expression;
    this?: Expression;
    expressions?: Expression[];
    from?: Expression;
    where?: Expression;
    returning?: Expression;
    order?: Expression;
    limit?: number | Expression;
    options?: Expression[];
  },
]>;

export class UpdateExpr extends DMLExpr {
  key = ExpressionKey.UPDATE;

  static argTypes: RequiredMap<UpdateExprArgs> = {
    ...super.argTypes,
    with: false,
    this: false,
    expressions: false,
    from: false,
    where: false,
    returning: false,
    order: false,
    limit: false,
    options: false,
  };

  declare args: UpdateExprArgs;

  constructor (args: UpdateExprArgs) {
    super(args);
  }

  get $with (): Expression | undefined {
    return this.args.with;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $from (): Expression | undefined {
    return this.args.from;
  }

  get $where (): Expression | undefined {
    return this.args.where;
  }

  get $returning (): Expression | undefined {
    return this.args.returning;
  }

  get $order (): Expression | undefined {
    return this.args.order;
  }

  get $limit (): number | Expression | undefined {
    return this.args.limit;
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }

  /**
   * Set the table to update.
   *
   * @example
   * update().table("my_table").set("x = 1").sql()
   * // 'UPDATE my_table SET x = 1'
   *
   * @param expression - The SQL code string to parse or Expression instance
   * @param options - Options for parsing and copying
   * @returns The modified Update expression
   */
  table (
    expression: string | Expression,
    options: {
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      dialect, copy = true,
    } = options;
    return _applyBuilder(expression, {
      instance: this,
      arg: 'this',
      into: TableExpr,
      prefix: undefined,
      dialect,
      copy,
    });
  }

  /**
   * Append to or set the SET expressions.
   *
   * @example
   * update().table("my_table").setExpressions("x = 1").sql()
   * // 'UPDATE my_table SET x = 1'
   *
   * @param expressions - The SQL code strings to parse or Expression instances
   * @param options - Options for parsing, appending, and copying
   * @returns The modified Update expression
   */
  set (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      append = true, dialect, copy = true,
    } = options;
    const expressionList = ensureList(expressions);
    return _applyListBuilder(expressionList, {
      instance: this,
      arg: 'expressions',
      append,
      into: Expression,
      prefix: undefined,
      dialect,
      copy,
    });
  }

  /**
   * Append to or set the WHERE expressions.
   *
   * @example
   * update().table("tbl").set("x = 1").where("x = 'a' OR x < 'b'").sql()
   * // "UPDATE tbl SET x = 1 WHERE x = 'a' OR x < 'b'"
   *
   * @param expressions - The SQL code strings to parse or Expression instances
   * @param options - Options for parsing, appending, and copying
   * @returns The modified Update expression
   */
  where (
    expressions: string | Expression | undefined | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      append = true, dialect, copy = true,
    } = options;
    return _applyConjunctionBuilder(expressions, {
      instance: this,
      arg: 'where',
      append,
      into: WhereExpr,
      dialect,
      copy,
    });
  }

  /**
   * Set the FROM expression.
   *
   * @example
   * update().table("my_table").set("x = 1").from("baz").sql()
   * // 'UPDATE my_table SET x = 1 FROM baz'
   *
   * @param expression - The SQL code string to parse or Expression instance
   * @param options - Options for parsing and copying
   * @returns The modified Update expression
   */
  from (
    expression?: string | Expression,
    options: {
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      dialect, copy = true,
    } = options;
    if (!expression) {
      return this;
    }

    return _applyBuilder(expression, {
      instance: this,
      arg: 'from',
      into: FromExpr,
      prefix: undefined,
      dialect,
      copy,
    });
  }

  /**
   * Append to or set the common table expressions.
   *
   * @example
   * update().table("my_table").set(["x = 1"]).from("baz").with("baz", "SELECT id FROM foo").sql()
   * // 'WITH baz AS (SELECT id FROM foo) UPDATE my_table SET x = 1 FROM baz'
   *
   * @param alias - The SQL code string to parse as the table name
   * @param as - The SQL code string to parse as the table expression
   * @param options - Options object
   * @returns The modified Update expression
   */
  with (
    alias: string | IdentifierExpr,
    as: string | QueryExpr,
    options: {
      recursive?: boolean;
      materialized?: boolean;
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      scalar?: boolean;
    } = {},
  ): this {
    const {
      recursive = false, append = true, copy = true, ...restOptions
    } = options;
    return _applyCteBuilder({
      instance: this,
      alias,
      as,
      ...restOptions,
      recursive,
      append,
      copy,
    });
  }
}

/**
 * Enumeration of valid kind values for SELECT expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum SelectExprKind {
  STRUCT = 'STRUCT',
  VALUE = 'VALUE',
  OBJECT = 'OBJECT',
  ALL = 'ALL',
  DISTINCT = 'DISTINCT',
}
/**
 * Represents a SELECT statement.
 *
 * @example
 * // SELECT col1, col2 FROM table WHERE id > 10
 * const select = new SelectExpr({
 *   expressions: [col1, col2],
 *   from: fromExpr,
 *   where: whereCondition
 * });
 */
export type SelectExprArgs = Merge<[
  QueryExprArgs,
  {
    with?: WithExpr;
    kind?: SelectExprKind;
    expressions?: Expression[];
    hint?: Expression;
    distinct?: boolean;
    into?: Expression;
    from?: FromExpr;
    operationModifiers?: Expression[];
    match?: Expression;
    laterals?: Expression[];
    joins?: JoinExpr[];
    connect?: Expression;
    pivots?: Expression[];
    prewhere?: Expression;
    where?: Expression;
    group?: Expression;
    having?: Expression;
    qualify?: Expression;
    windows?: Expression[];
    distribute?: Expression;
    sort?: Expression;
    cluster?: Expression;
    order?: Expression;
    limit?: number | Expression;
    offset?: number | Expression;
    locks?: Expression[];
    sample?: number | Expression;
  },
]>;

export class SelectExpr extends QueryExpr {
  key = ExpressionKey.SELECT;

  static argTypes: RequiredMap<SelectExprArgs> = {
    ...super.argTypes,
    with: false,
    kind: false,
    expressions: false,
    hint: false,
    distinct: false,
    into: false,
    from: false,
    operationModifiers: false,
    ...QUERY_MODIFIERS,
  };

  declare args: SelectExprArgs;

  constructor (args: SelectExprArgs) {
    super(args);
  }

  get $with (): Expression | undefined {
    return this.args.with;
  }

  get $kind (): SelectExprKind | undefined {
    return this.args.kind;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $hint (): Expression | undefined {
    return this.args.hint;
  }

  get $distinct (): boolean | undefined {
    return this.args.distinct;
  }

  get $into (): Expression | undefined {
    return this.args.into;
  }

  get $from (): FromExpr | undefined {
    return this.args.from;
  }

  get $operationModifiers (): Expression[] | undefined {
    return this.args.operationModifiers;
  }

  get $match (): Expression | undefined {
    return this.args.match;
  }

  get $laterals (): Expression[] | undefined {
    return this.args.laterals;
  }

  get $joins (): JoinExpr[] | undefined {
    return this.args.joins;
  }

  get $connect (): Expression | undefined {
    return this.args.connect;
  }

  get $pivots (): Expression[] | undefined {
    return this.args.pivots;
  }

  get $prewhere (): Expression | undefined {
    return this.args.prewhere;
  }

  get $where (): Expression | undefined {
    return this.args.where;
  }

  get $group (): Expression | undefined {
    return this.args.group;
  }

  get $having (): Expression | undefined {
    return this.args.having;
  }

  get $qualify (): Expression | undefined {
    return this.args.qualify;
  }

  get $windows (): Expression[] | undefined {
    return this.args.windows;
  }

  get $distribute (): Expression | undefined {
    return this.args.distribute;
  }

  get $sort (): Expression | undefined {
    return this.args.sort;
  }

  get $cluster (): Expression | undefined {
    return this.args.cluster;
  }

  get $order (): Expression | undefined {
    return this.args.order;
  }

  get $limit (): number | Expression | undefined {
    return this.args.limit;
  }

  get $offset (): number | Expression | undefined {
    return this.args.offset;
  }

  get $locks (): Expression[] | undefined {
    return this.args.locks;
  }

  get $sample (): number | Expression | undefined {
    return this.args.sample;
  }

  /**
   * Returns a list of output names from the select expressions.
   */
  get namedSelects (): string[] {
    const selects: string[] = [];

    for (const e of (this.$expressions || [])) {
      if (e.aliasOrName) {
        selects.push(e.outputName);
      } else if (e instanceof AliasesExpr) {
        const aliases = e.args.expressions || [];
        selects.push(...aliases.map((a) => a instanceof Expression ? a.name : '').filter((n) => n));
      }
    }

    return selects;
  }

  /**
   * Returns true if any expression is a star expression.
   */
  get isStar (): boolean {
    return this.expressions.some((expression) => typeof expression === 'object' && 'isStar' in expression && expression.isStar);
  }

  /**
   * Returns the SELECT expressions.
   */
  get selects (): Expression[] {
    return this.$expressions || [];
  }

  /**
   * Set the FROM expression.
   *
   * @example
   * select().from("tbl").select(["x"]).sql()
   * // 'SELECT x FROM tbl'
   */
  from (
    expression: string | Expression | undefined,
    options: {
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      dialect, copy = true, ...restOptions
    } = options;
    return _applyBuilder(expression, {
      ...restOptions,
      instance: this,
      arg: 'from',
      into: FromExpr,
      prefix: 'FROM',
      dialect,
      copy,
    });
  }

  /**
   * Set the GROUP BY expression.
   *
   * @example
   * select().from("tbl").select(["x", "COUNT(1)"]).groupBy(["x"]).sql()
   * // 'SELECT x, COUNT(1) FROM tbl GROUP BY x'
   */
  groupBy (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, dialect, copy = true, ...restOptions
    } = options;
    const expressionList = ensureList(expressions);
    if (expressionList.length === 0) {
      return copy ? (this.copy() as this) : this;
    }

    return _applyChildListBuilder(expressionList, {
      ...restOptions,
      instance: this,
      arg: 'group',
      append,
      copy,
      prefix: 'GROUP BY',
      into: GroupExpr,
      dialect,
    });
  }

  /**
   * Set the SORT BY expression.
   *
   * @example
   * select().from("tbl").select(["x"]).sortBy(["x DESC"]).sql({ dialect: "hive" })
   * // 'SELECT x FROM tbl SORT BY x DESC'
   */
  sortBy (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, dialect, copy = true, ...restOptions
    } = options;
    return _applyChildListBuilder(expressions, {
      ...restOptions,
      instance: this,
      arg: 'sort',
      append,
      copy,
      prefix: 'SORT BY',
      into: SortExpr,
      dialect,
    });
  }

  /**
   * Set the CLUSTER BY expression.
   *
   * @example
   * select().from("tbl").select(["x"]).clusterBy(["x"]).sql({ dialect: "hive" })
   * // 'SELECT x FROM tbl CLUSTER BY x'
   */
  clusterBy (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, dialect, copy = true, ...restOptions
    } = options;
    return _applyChildListBuilder(expressions, {
      ...restOptions,
      instance: this,
      arg: 'cluster',
      append,
      copy,
      prefix: 'CLUSTER BY',
      into: ClusterExpr,
      dialect,
    });
  }

  /**
   * Append to or set the SELECT expressions.
   *
   * @example
   * select().select(["x", "y"]).from("tbl").sql()
   * // 'SELECT x, y FROM tbl'
   */
  select (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, dialect, copy = true, ...restOptions
    } = options;
    return _applyListBuilder(expressions, {
      ...restOptions,
      instance: this,
      arg: 'expressions',
      append,
      dialect,
      into: Expression,
      copy,
    });
  }

  /**
   * Append to or set the LATERAL expressions.
   *
   * @example
   * select().select(["x"]).lateral(["OUTER explode(y) tbl2 AS z"]).from("tbl").sql()
   * // 'SELECT x FROM tbl LATERAL VIEW OUTER EXPLODE(y) tbl2 AS z'
   */
  lateral (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, dialect, copy = true, ...restOptions
    } = options;
    return _applyListBuilder(expressions, {
      ...restOptions,
      instance: this,
      arg: 'laterals',
      append,
      into: LateralExpr,
      prefix: 'LATERAL VIEW',
      dialect,
      copy,
    });
  }

  /**
   * Append to or set the JOIN expressions.
   *
   * @example
   * select().select(["*"]).from("tbl").join("tbl2", { on: "tbl1.y = tbl2.y" }).sql()
   * // 'SELECT * FROM tbl JOIN tbl2 ON tbl1.y = tbl2.y'
   *
   * @example
   * select().select(["1"]).from("a").join("b", { using: ["x", "y", "z"] }).sql()
   * // 'SELECT 1 FROM a JOIN b USING (x, y, z)'
   *
   * @example
   * select().select(["*"]).from("tbl").join("tbl2", { on: "tbl1.y = tbl2.y", joinType: "left outer" }).sql()
   * // 'SELECT * FROM tbl LEFT OUTER JOIN tbl2 ON tbl1.y = tbl2.y'
   */
  join (
    expression: string | Expression,
    options: {
      on?: string | Expression | (string | Expression)[];
      using?: string | Expression | (string | Expression)[];
      append?: boolean;
      joinType?: string;
      joinAlias?: string | IdentifierExpr;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      on, using: usingOpt, append = true, joinType, joinAlias, dialect, copy = true, ..._restOptions
    } = options;
    const parseArgs = {
      dialect,
    };

    let expr: Expression;
    try {
      expr = maybeParse(expression, {
        ...parseArgs,
        into: JoinExpr,
        prefix: 'JOIN',
      });
    } catch {
      expr = maybeParse(expression, {
        ...parseArgs,
        into: Expression,
      });
    }

    let join = expr instanceof JoinExpr ? expr : new JoinExpr({ this: expr });

    // If joining a Select, wrap it in a subquery
    if (join.args.this instanceof SelectExpr) {
      join.args.this.replace(join.args.this.subquery());
    }

    // Set join type (method, side, kind)
    if (joinType) {
      const [
        method,
        side,
        kind,
      ] = maybeParse(joinType, {
        ...parseArgs,
        into: 'JOIN_TYPE', // FIXME: What is this in sqlglot??
      });
      if (method instanceof Token) {
        join.setArgKey('method', method.text);
      }
      if (side instanceof Token) {
        join.setArgKey('side', side.text);
      }
      if (kind instanceof Token) {
        join.setArgKey('kind', kind.text);
      }
    }

    // Set ON condition
    if (on) {
      const onExpr = and(ensureList(on), {
        dialect,
        copy,
      });
      join.setArgKey('on', onExpr);
    }

    // Set USING
    if (usingOpt) {
      join = _applyListBuilder(ensureList(usingOpt), {
        instance: join,
        arg: 'using',
        append,
        copy,
        into: IdentifierExpr,
      }) as JoinExpr;
    }

    // Set join alias
    if (joinAlias) {
      join.setArgKey('this', alias(join.args.this as Expression, joinAlias, { table: true }));
    }

    return _applyListBuilder([join], {
      instance: this,
      arg: 'joins',
      append,
      copy,
    });
  }

  /**
   * Append to or set the WINDOW expressions.
   *
   * @example
   * select().select(["x"]).from("tbl").window(["w AS (PARTITION BY x)"]).sql()
   * // 'SELECT x FROM tbl WINDOW w AS (PARTITION BY x)'
   */
  window (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, dialect, copy = true, ...restOptions
    } = options;
    return _applyListBuilder(expressions, {
      ...restOptions,
      instance: this,
      arg: 'windows',
      append,
      into: WindowExpr,
      dialect,
      copy,
    });
  }

  /**
   * Append to or set the QUALIFY expressions.
   *
   * @example
   * select().select(["x"]).from("tbl").qualify(["ROW_NUMBER() OVER (PARTITION BY x) = 1"]).sql()
   * // 'SELECT x FROM tbl QUALIFY ROW_NUMBER() OVER (PARTITION BY x) = 1'
   */
  qualify (
    expressions: string | Expression | undefined | (string | Expression)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, dialect, copy = true, ...restOptions
    } = options;
    return _applyConjunctionBuilder(expressions, {
      ...restOptions,
      instance: this,
      arg: 'qualify',
      append,
      into: QualifyExpr,
      dialect,
      copy,
    });
  }

  /**
   * Set the DISTINCT clause.
   *
   * @example
   * select().from("tbl").select(["x"]).distinct().sql()
   * // 'SELECT DISTINCT x FROM tbl'
   */
  distinct (
    ons?: (string | Expression | undefined)[],
    options: {
      distinct?: boolean;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      distinct: distinctValue = true, copy = true, ..._restOptions
    } = options;
    const instance = maybeCopy(this, copy);

    if (ons && 0 < ons.length) {
      const onExprs = ons.filter((on): on is string | Expression => on !== undefined)
        .map((on) => maybeParse(on, {
          copy,
          into: Expression,
        }));
      const tupleExpr = new TupleExpr({ expressions: onExprs });
      instance.setArgKey('distinct', distinctValue ? new DistinctExpr({ on: tupleExpr }) : undefined);
    } else {
      instance.setArgKey('distinct', distinctValue ? new DistinctExpr({}) : undefined);
    }

    return instance as this;
  }

  /**
   * Convert this expression to a CREATE TABLE AS statement.
   *
   * @example
   * select().select(["*"]).from("tbl").ctas("x").sql()
   * // 'CREATE TABLE x AS SELECT * FROM tbl'
   */
  ctas (
    table: string | Expression,
    options: {
      properties?: Record<string, unknown>;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): CreateExpr {
    const {
      properties, dialect, copy = true,
    } = options;
    const instance = maybeCopy(this, copy);
    const tableExpr = maybeParse(table, {
      dialect,
      into: TableExpr,
    });

    let propertiesExpr: PropertiesExpr | undefined;
    if (properties) {
      propertiesExpr = PropertiesExpr.fromDict(properties);
    }

    return new CreateExpr({
      this: tableExpr,
      kind: CreateExprKind.TABLE,
      expression: instance,
      properties: propertiesExpr,
    });
  }

  /**
   * Set the locking read mode for this expression.
   *
   * @example
   * select().select(["x"]).from("tbl").where(["x = 'a'"]).lock().sql({ dialect: "mysql" })
   * // "SELECT x FROM tbl WHERE x = 'a' FOR UPDATE"
   *
   * @example
   * select().select(["x"]).from("tbl").where(["x = 'a'"]).lock({ update: false }).sql({ dialect: "mysql" })
   * // "SELECT x FROM tbl WHERE x = 'a' FOR SHARE"
   */
  lock (
    options: {
      update?: boolean;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      update = true, copy = true,
    } = options;
    const inst = maybeCopy(this, copy);
    inst.setArgKey('locks', [
      new LockExpr({
        update: new LiteralExpr({
          this: String(Number(update)),
          isString: false,
        }),
      }),
    ]);
    return inst as this;
  }

  /**
   * Set hints for this expression.
   *
   * @example
   * select().select(["x"]).from("tbl").hint(["BROADCAST(y)"]).sql({ dialect: "spark" })
   * // 'SELECT /*+ BROADCAST(y) *\/ x FROM tbl'
   */
  hint (
    hints: (string | Expression)[],
    options: {
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      dialect, copy = true,
    } = options;
    const hintExprs = hints.map((h) =>
      maybeParse(h, {
        dialect,
        into: Expression,
      }));
    const inst = maybeCopy(this, copy);
    inst.setArgKey('hint', new HintExpr({ expressions: hintExprs }));
    return inst as this;
  }
}

export type SubqueryExprArgs = Merge<[
  DerivedTableExprArgs,
  QueryExprArgs,
  {
    with?: WithExpr;
    alias?: TableAliasExpr;
    this: QueryExpr;
  },
]>;

export class SubqueryExpr extends multiInherit(DerivedTableExpr, QueryExpr) {
  key = ExpressionKey.SUBQUERY;

  static argTypes: RequiredMap<SubqueryExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    alias: false,
    with: false,
  };

  declare args: SubqueryExprArgs;

  constructor (args: SubqueryExprArgs) {
    super(args);
  }

  get $alias (): TableAliasExpr | undefined {
    return this.args.alias;
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $with (): WithExpr | undefined {
    return this.args.with;
  }

  /**
   * Returns the first non-subquery expression.
   */
  unnest (): QueryExpr {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let expression: QueryExpr = this;
    while (expression instanceof SubqueryExpr) {
      expression = expression.args.this || expression;
      if (!expression || expression === this) break;
    }
    return expression;
  }

  /**
   * Returns the outermost wrapper subquery.
   */
  unwrap (): SubqueryExpr {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let expression: SubqueryExpr = this;
    while (expression.sameParent && expression.isWrapper) {
      const parent = expression.parent;
      if (!(parent instanceof SubqueryExpr)) break;
      expression = parent;
    }
    return expression;
  }

  /**
   * Append to or set the SELECT expressions on the unnested query.
   */
  select (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      copy = true, ...restOptions
    } = options;
    const instance = maybeCopy(this, copy);
    const unnested = instance?.unnest();

    if (unnested instanceof SelectExpr) {
      unnested.select(expressions, {
        ...restOptions,
        copy: false,
      });
    }

    return instance;
  }

  /**
   * Whether this Subquery acts as a simple wrapper around another expression.
   *
   * SELECT * FROM (((SELECT * FROM t)))
   *               ^
   *               This corresponds to a "wrapper" Subquery node
   */
  get isWrapper (): boolean {
    return Object.entries(this.args).every(
      ([k, v]) => k === 'this' || v === undefined,
    );
  }

  /**
   * Returns true if the inner query is a star expression.
   */
  get isStar (): boolean {
    const thisArg = this.args.this;
    return thisArg ? thisArg.isStar : false;
  }

  /**
   * Returns the alias of this subquery.
   */
  get outputName (): string {
    return this.alias;
  }
}

export type WindowExprArgs = Merge<[
  ConditionExprArgs,
  {
    this: Expression;
    partitionBy?: Expression;
    order?: Expression;
    spec?: Expression;
    alias?: TableAliasExpr;
    over?: Expression;
    first?: Expression;
  },
]>;

export class WindowExpr extends ConditionExpr {
  key = ExpressionKey.WINDOW;

  /**
   * Defines the arguments (properties and child expressions) for Window expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<WindowExprArgs> = {
    ...super.argTypes,
    this: true,
    partitionBy: false,
    order: false,
    spec: false,
    alias: false,
    over: false,
    first: false,
  };

  declare args: WindowExprArgs;

  constructor (args: WindowExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $partitionBy (): Expression | undefined {
    return this.args.partitionBy;
  }

  get $alias (): TableAliasExpr | undefined {
    return this.args.alias;
  }

  get $order (): Expression | undefined {
    return this.args.order;
  }

  get $spec (): Expression | undefined {
    return this.args.spec;
  }

  get $over (): Expression | undefined {
    return this.args.over;
  }

  get $first (): Expression | undefined {
    return this.args.first;
  }
}

export type ParameterExprArgs = Merge<[
  ConditionExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class ParameterExpr extends ConditionExpr {
  key = ExpressionKey.PARAMETER;

  static argTypes: RequiredMap<ParameterExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: ParameterExprArgs;

  constructor (args: ParameterExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }
}

/**
 * Valid kind values for session parameters
 */
export enum SessionParameterExprKind {
  SESSION = 'SESSION',
  GLOBAL = 'GLOBAL',
  LOCAL = 'LOCAL',
  VARIABLE = 'VARIABLE',
}
export type SessionParameterExprArgs = Merge<[
  ConditionExprArgs,
  {
    this: Expression;
    kind?: SessionParameterExprKind;
  },
]>;

export class SessionParameterExpr extends ConditionExpr {
  key = ExpressionKey.SESSION_PARAMETER;

  static argTypes: RequiredMap<SessionParameterExprArgs> = {
    ...super.argTypes,
    this: true,
    kind: false,
  };

  declare args: SessionParameterExprArgs;

  constructor (args: SessionParameterExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): SessionParameterExprKind | undefined {
    return this.args.kind;
  }
}

/**
 * Valid kind values for placeholders
 */
export enum PlaceholderExprKind {
  POSITIONAL = 'POSITIONAL',
  NAMED = 'NAMED',
  QUESTION = 'QUESTION',
  NUMERIC = 'NUMERIC',
  DOLLAR = 'DOLLAR',
}
export type PlaceholderExprArgs = Merge<[
  ConditionExprArgs,
  {
    this?: Expression;
    kind?: PlaceholderExprKind;
    widget?: Expression;
    jdbc?: boolean;
  },
]>;

export class PlaceholderExpr extends ConditionExpr {
  key = ExpressionKey.PLACEHOLDER;

  /**
   * Defines the arguments (properties and child expressions) for Placeholder expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<PlaceholderExprArgs> = {
    ...super.argTypes,
    this: false,
    kind: false,
    widget: false,
    jdbc: false,
  };

  declare args: PlaceholderExprArgs;

  constructor (args: PlaceholderExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $kind (): PlaceholderExprKind | undefined {
    return this.args.kind;
  }

  get $widget (): Expression | undefined {
    return this.args.widget;
  }

  get $jdbc (): boolean | undefined {
    return this.args.jdbc;
  }

  /**
   * Returns the name of this placeholder.
   */
  get name (): string {
    return this.args.this?.name || '?';
  }
}

export type NullExprArgs = Merge<[
  ConditionExprArgs,
]>;

export class NullExpr extends ConditionExpr {
  key = ExpressionKey.NULL;

  static argTypes: RequiredMap<NullExprArgs> = {
    ...super.argTypes,
  };

  declare args: NullExprArgs;

  constructor (args: NullExprArgs) {
    super(args);
  }

  /**
   * Returns the name of this undefined expression.
   */
  get name (): string {
    return 'NULL';
  }

  /**
   * Converts this to a Python undefined value.
   */
  toValue (): undefined {
    return undefined;
  }
}

export type BooleanExprArgs = Merge<[
  ConditionExprArgs,
]>;

export class BooleanExpr extends ConditionExpr {
  key = ExpressionKey.BOOLEAN;

  static argTypes: RequiredMap<BooleanExprArgs> = {
    ...super.argTypes,
  };

  declare args: BooleanExprArgs;

  constructor (args: BooleanExprArgs) {
    super(args);
  }

  /**
   * Converts this to a Python boolean value.
   */
  toPy (): boolean {
    return Boolean(this.args.this);
  }
}

export type PseudoTypeExprArgs = Merge<[
  DataTypeExprArgs,
  { this: DataTypeExprKind },
]>;

export class PseudoTypeExpr extends DataTypeExpr {
  key = ExpressionKey.PSEUDO_TYPE;

  static argTypes: RequiredMap<PseudoTypeExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: PseudoTypeExprArgs;

  constructor (args: PseudoTypeExprArgs) {
    super(args);
  }

  get $this (): DataTypeExprKind {
    return this.args.this;
  }
}

export type ObjectIdentifierExprArgs = Merge<[
  DataTypeExprArgs,
  { this: DataTypeExprKind },
]>;

export class ObjectIdentifierExpr extends DataTypeExpr {
  key = ExpressionKey.OBJECT_IDENTIFIER;

  static argTypes: RequiredMap<ObjectIdentifierExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: ObjectIdentifierExprArgs;

  constructor (args: ObjectIdentifierExprArgs) {
    super(args);
  }

  get $this (): DataTypeExprKind {
    return this.args.this;
  }
}

export type BinaryExprArgs = Merge<[
  ConditionExprArgs,
  {
    this?: Expression; // NOTE: We set `this: true` in argTypes
    expression?: Expression; // NOTE: We set `expression: true` in argTypes

  },
]>;

export class BinaryExpr extends ConditionExpr {
  key = ExpressionKey.BINARY;

  static argTypes: RequiredMap<BinaryExprArgs> = {
    ...super.argTypes,
    this: true, // NOTE: sqlglot sets this to true, although XorExpr sets to false
    expression: true, // NOTE: sqlglot sets this to true, although XorExpr sets to false

  };

  declare args: BinaryExprArgs;

  constructor (args: BinaryExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  get left (): Expression | undefined {
    return this.args.this;
  }

  get right (): Expression | undefined {
    return this.args.expression;
  }
}

export type UnaryExprArgs = Merge<[
  BaseExpressionArgs,
]>;
export class UnaryExpr extends Expression {
  key = ExpressionKey.UNARY;

  static argTypes: RequiredMap<UnaryExprArgs> = {};

  declare args: UnaryExprArgs;

  constructor (args: UnaryExprArgs) {
    super(args);
  }
}

export type PivotAliasExprArgs = Merge<[
  AliasExprArgs,
]>;

export class PivotAliasExpr extends AliasExpr {
  key = ExpressionKey.PIVOT_ALIAS;

  static argTypes: RequiredMap<PivotAliasExprArgs> = {
    ...super.argTypes,
  };

  declare args: PivotAliasExprArgs;

  constructor (args: PivotAliasExprArgs) {
    super(args);
  }
}

export type BracketExprArgs = Merge<[
  ConditionExprArgs,
  {
    this: Expression;
    expressions: Expression[];
    offset?: boolean;
    safe?: boolean;
    returnsListForMaps?: Expression[];
  },
]>;

/**
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#array_subscript_operator
 */
export class BracketExpr extends ConditionExpr {
  key = ExpressionKey.BRACKET;

  static argTypes: RequiredMap<BracketExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
    offset: false,
    safe: false,
    returnsListForMaps: false,
  };

  declare args: BracketExprArgs;

  constructor (args: BracketExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $offset (): boolean | undefined {
    return this.args.offset;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }

  get $returnsListForMaps (): Expression[] | undefined {
    return this.args.returnsListForMaps;
  }

  get outputName (): string {
    if (this.args.expressions.length === 1) {
      return this.args.expressions[0].outputName;
    }
    return super.outputName;
  }
}

export type IntervalOpExprArgs = Merge<[
  TimeUnitExprArgs,
  {
    unit?: VarExpr | IntervalSpanExpr;
    expression: Expression;
  },
]>;

export class IntervalOpExpr extends TimeUnitExpr {
  key = ExpressionKey.INTERVAL_OP;

  static argTypes: RequiredMap<IntervalOpExprArgs> = {
    ...super.argTypes,
    unit: false,
    expression: true,
  };

  declare args: IntervalOpExprArgs;

  constructor (args: IntervalOpExprArgs) {
    super(args);
  }

  interval (): IntervalExpr {
    return new IntervalExpr({
      this: this.args.expression.copy(),
      unit: this.unit?.copy(),
    });
  }

  get $unit (): VarExpr | IntervalSpanExpr | undefined {
    return this.args.unit;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

/**
 * https://www.oracletutorial.com/oracle-basics/oracle-interval/
 * https://trino.io/docs/current/language/types.html#interval-day-to-second
 * https://docs.databricks.com/en/sql/language-manual/data-types/interval-type.html
 */
export type IntervalSpanExprArgs = Merge<[
  DataTypeExprArgs,
  {
    this: DataTypeExprKind;
    expression: Expression;
  },
]>;

export class IntervalSpanExpr extends DataTypeExpr {
  key = ExpressionKey.INTERVAL_SPAN;

  static argTypes: RequiredMap<IntervalSpanExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: IntervalSpanExprArgs;

  constructor (args: IntervalSpanExprArgs) {
    super(args);
  }

  get $this (): DataTypeExprKind {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type IntervalExprArgs = Merge<[
  TimeUnitExprArgs,
  {
    this?: Expression;
    unit?: VarExpr | IntervalSpanExpr;
  },
]>;

export class IntervalExpr extends TimeUnitExpr {
  key = ExpressionKey.INTERVAL;

  static argTypes: RequiredMap<IntervalExprArgs> = {
    ...super.argTypes,
    this: false,
    unit: false,
  };

  declare args: IntervalExprArgs;

  constructor (args: IntervalExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $unit (): VarExpr | IntervalSpanExpr | undefined {
    return this.args.unit;
  }
}

// Global registry for function classes (populated by self-registration)
const _functionRegistry = new Map<string, typeof FuncExpr>();
const _allFunctions = new Set<typeof FuncExpr>();

/**
 * The base class for all function expressions.
 *
 * Attributes:
 *   isVarLenArgs: if set to true the last argument defined in argTypes will be
 *     treated as a variable length argument and the argument's value will be stored as a list.
 *   sqlNames: the SQL name (1st item in the list) and aliases (subsequent items) for this
 *     function expression. These values are used to map this node to a name during parsing as
 *     well as to provide the function's name during SQL string generation. By default the SQL
 *     name is set to the expression's class name transformed to snake case.
 */
export type FuncExprArgs = Merge<[
  ConditionExprArgs,
]>;

export class FuncExpr extends ConditionExpr {
  key = ExpressionKey.FUNC;

  static argTypes: RequiredMap<FuncExprArgs> = {
    ...super.argTypes,
  };

  declare args: FuncExprArgs;

  constructor (args: FuncExprArgs) {
    super(args);
  }

  static isVarLenArgs = false;

  /**
   * Create a function instance from a list of arguments
   */
  static fromArgList<T extends typeof FuncExpr> (this: T, args: Expression[]): InstanceType<T> {
    const allArgKeys = Object.keys(this.argTypes);

    if (this.isVarLenArgs) {
      const nonVarLenArgKeys = allArgKeys.slice(0, -1);
      const numNonVar = nonVarLenArgKeys.length;

      const argsDict: Record<string, Expression | Expression[]> = {};
      for (let i = 0; i < nonVarLenArgKeys.length; i++) {
        argsDict[nonVarLenArgKeys[i]] = args[i];
      }
      argsDict[allArgKeys[allArgKeys.length - 1]] = args.slice(numNonVar);

      return new this(argsDict as FuncExprArgs) as InstanceType<T>;
    } else {
      const argsDict: Record<string, Expression> = {};
      for (let i = 0; i < allArgKeys.length; i++) {
        argsDict[allArgKeys[i]] = args[i];
      }

      return new this(argsDict as FuncExprArgs) as InstanceType<T>;
    }
  }

  static _sqlNames: string[] = [];
  /**
   * Get the SQL names for this function class
   * @returns Array of SQL names (primary name first, then aliases)
   */
  static sqlNames (): string[] {
    if (this === FuncExpr) {
      throw new Error('SQL name is only supported by concrete function implementations');
    }

    if (!Object.hasOwn(this, '_sqlNames')) {
      const className = this.name.replace(/Expr$/, '');
      const snakeCase = className
        .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2')
        .replace(/([a-z\d])([A-Z])/g, '$1_$2')
        .toLowerCase();

      this._sqlNames = [snakeCase];
    }

    return this._sqlNames;
  }

  /**
   * Get the primary SQL name for this function
   * @returns The primary SQL name (first item from sqlNames)
   */
  static sqlName (): string {
    const sqlNames = this.sqlNames();
    if (!sqlNames.length) {
      throw new Error(`Expected non-empty 'sql_names' for Func: ${this.name}.`);
    }
    return sqlNames[0];
  }

  /**
   * Get default parser mappings for this function
   * @returns Object mapping SQL names to the fromArgList parser
   */
  static defaultParserMappings (): Record<string, (args: Expression[]) => FuncExpr> {
    const mappings: Record<string, (args: Expression[]) => FuncExpr> = {};
    for (const name of this.sqlNames()) {
      mappings[name] = this.fromArgList.bind(this);
    }
    return mappings;
  }

  /**
   * Register this function class in the global registry
   * Called automatically by subclasses using static initialization blocks
   */
  static register (): void {
    // Don't register base classes
    if (this === FuncExpr || this === AggFuncExpr) {
      return;
    }

    // Add to all functions set
    _allFunctions.add(this);

    // Register by all SQL names
    const sqlNames = this.sqlNames();
    for (const name of sqlNames) {
      _functionRegistry.set(name.toUpperCase(), this);
    }
  }
}

export type JSONPathFilterExprArgs = Merge<[
  JSONPathPartExprArgs,
  { this: string },
]>;
export class JSONPathFilterExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_FILTER;

  static argTypes: RequiredMap<JSONPathFilterExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: JSONPathFilterExprArgs;

  constructor (args: JSONPathFilterExprArgs) {
    super(args);
  }

  get $this (): string {
    return this.args.this;
  }
}

export type JSONPathKeyExprArgs = Merge<[
  JSONPathPartExprArgs,
  { this: string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | JSONPathSliceExpr | number | false },
]>;
export class JSONPathKeyExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_KEY;

  static argTypes: RequiredMap<JSONPathKeyExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: JSONPathKeyExprArgs;

  constructor (args: JSONPathKeyExprArgs) {
    super(args);
  }

  get $this (): string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | JSONPathSliceExpr | number | false {
    return this.args.this;
  }
}

export type JSONPathRecursiveExprArgs = Merge<[
  JSONPathPartExprArgs,
  { this?: string | Expression },
]>;

export class JSONPathRecursiveExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_RECURSIVE;

  static argTypes: RequiredMap<JSONPathRecursiveExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: JSONPathRecursiveExprArgs;

  constructor (args: JSONPathRecursiveExprArgs) {
    super(args);
  }

  get $this (): string | Expression | undefined {
    return this.args.this;
  }
}

export type JSONPathRootExprArgs = Merge<[
  JSONPathPartExprArgs,
]>;

export class JSONPathRootExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_ROOT;

  static argTypes: RequiredMap<JSONPathRootExprArgs> = {};

  declare args: JSONPathRootExprArgs;

  constructor (args: JSONPathRootExprArgs) {
    super(args);
  }
}

export type JSONPathScriptExprArgs = Merge<[
  JSONPathPartExprArgs,
  { this: string },
]>;

export class JSONPathScriptExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_SCRIPT;

  static argTypes: RequiredMap<JSONPathScriptExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: JSONPathScriptExprArgs;

  constructor (args: JSONPathScriptExprArgs) {
    super(args);
  }

  get $this (): string {
    return this.args.this;
  }
}

export type JSONPathSliceExprArgs = Merge<[
  JSONPathPartExprArgs,
  {
    start?: string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | number | false;
    end?: string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | number | false;
    step?: string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | number | false;
  },
]>;

export class JSONPathSliceExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_SLICE;

  /**
   * Defines the arguments (properties and child expressions) for JSONPathSlice expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JSONPathSliceExprArgs> = {
    ...super.argTypes,
    start: false,
    end: false,
    step: false,
  };

  declare args: JSONPathSliceExprArgs;

  constructor (args: JSONPathSliceExprArgs) {
    super(args);
  }

  get $start (): string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | number | false | undefined {
    return this.args.start;
  }

  get $end (): string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | number | false | undefined {
    return this.args.end;
  }

  get $step (): string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | JSONPathSliceExpr | number | false | undefined {
    return this.args.step;
  }
}

export type JSONPathSelectorExprArgs = Merge<[
  JSONPathPartExprArgs,
  { this: string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | JSONPathSliceExpr | number | false },
]>;

export class JSONPathSelectorExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_SELECTOR;

  static argTypes: RequiredMap<JSONPathSelectorExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: JSONPathSelectorExprArgs;

  constructor (args: JSONPathSelectorExprArgs) {
    super(args);
  }

  get $this (): string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | JSONPathSliceExpr | number | false {
    return this.args.this;
  }
}

export type JSONPathSubscriptExprArgs = Merge<[
  JSONPathPartExprArgs,
  { this: string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | JSONPathSliceExpr | number | false },
]>;

export class JSONPathSubscriptExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_SUBSCRIPT;

  static argTypes: RequiredMap<JSONPathSubscriptExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: JSONPathSubscriptExprArgs;

  constructor (args: JSONPathSubscriptExprArgs) {
    super(args);
  }

  get $this (): string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | JSONPathSliceExpr | number | false {
    return this.args.this;
  }
}

export type JSONPathUnionExprArgs = Merge<[
  JSONPathPartExprArgs,
  { expressions: (string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | JSONPathSliceExpr | number | false)[] },
]>;

export class JSONPathUnionExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_UNION;

  static argTypes: RequiredMap<JSONPathUnionExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: JSONPathUnionExprArgs;

  constructor (args: JSONPathUnionExprArgs) {
    super(args);
  }

  get $expressions (): (string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | JSONPathSliceExpr | number | false)[] {
    return this.args.expressions;
  }
}

export type JSONPathWildcardExprArgs = Merge<[
  JSONPathPartExprArgs,
]>;

export class JSONPathWildcardExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_WILDCARD;

  static argTypes: RequiredMap<JSONPathWildcardExprArgs> = {};

  declare args: JSONPathWildcardExprArgs;

  constructor (args: JSONPathWildcardExprArgs) {
    super(args);
  }
}

export type MergeExprArgs = Merge<[
  DMLExprArgs,
  {
    using: Expression;
    on?: Expression;
    usingCond?: string;
    whens: WhensExpr;
    with?: Expression;
    returning?: Expression;
  },
]>;

export class MergeExpr extends DMLExpr {
  key = ExpressionKey.MERGE;

  /**
   * Defines the arguments (properties and child expressions) for Merge expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<MergeExprArgs> = {
    ...super.argTypes,
    using: true,
    on: false,
    usingCond: false,
    whens: true,
    with: false,
    returning: false,
  };

  declare args: MergeExprArgs;

  constructor (args: MergeExprArgs) {
    super(args);
  }

  get $using (): Expression {
    return this.args.using;
  }

  get $on (): Expression | undefined {
    return this.args.on;
  }

  get $usingCond (): string | undefined {
    return this.args.usingCond;
  }

  get $whens (): WhensExpr {
    return this.args.whens;
  }

  get $with (): Expression | undefined {
    return this.args.with;
  }

  get $returning (): Expression | undefined {
    return this.args.returning;
  }
}

export type LateralExprArgs = Merge<[
  BaseExpressionArgs,
  {
    view?: Expression;
    outer?: Expression;
    crossApply?: boolean;
    ordinality?: boolean;
    alias?: TableAliasExpr;
    this: Expression;
  },
]>;

export class LateralExpr extends UDTFExpr {
  key = ExpressionKey.LATERAL;

  /**
   * Defines the arguments (properties and child expressions) for Lateral expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<LateralExprArgs> = {
    ...super.argTypes,
    this: true,
    view: false,
    outer: false,
    alias: false,
    crossApply: false,
    ordinality: false,
  };

  declare args: LateralExprArgs;

  constructor (args: LateralExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $view (): Expression | undefined {
    return this.args.view;
  }

  get $outer (): Expression | undefined {
    return this.args.outer;
  }

  get $alias (): TableAliasExpr | undefined {
    return this.args.alias;
  }

  get $crossApply (): boolean | undefined {
    return this.args.crossApply;
  }

  get $ordinality (): boolean | undefined {
    return this.args.ordinality;
  }
}

export type TableFromRowsExprArgs = Merge<[
  UDTFExprArgs,
  {
    joins?: JoinExpr[];
    pivots?: Expression[];
    sample?: number | Expression;
    alias?: TableAliasExpr;
    this: Expression;
  },
]>;

export class TableFromRowsExpr extends UDTFExpr {
  key = ExpressionKey.TABLE_FROM_ROWS;

  /**
   * Defines the arguments (properties and child expressions) for TableFromRows expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<TableFromRowsExprArgs> = {
    ...super.argTypes,
    this: true,
    alias: false,
    joins: false,
    pivots: false,
    sample: false,
  };

  declare args: TableFromRowsExprArgs;

  constructor (args: TableFromRowsExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $alias (): TableAliasExpr | undefined {
    return this.args.alias;
  }

  get $joins (): JoinExpr[] | undefined {
    return this.args.joins;
  }

  get $pivots (): Expression[] | undefined {
    return this.args.pivots;
  }

  get $sample (): number | Expression | undefined {
    return this.args.sample;
  }
}

export type UnionExprArgs = Merge<[
  SetOperationExprArgs,
]>;

export class UnionExpr extends SetOperationExpr {
  key = ExpressionKey.UNION;

  static argTypes: RequiredMap<UnionExprArgs> = {
    ...super.argTypes,
  };

  declare args: UnionExprArgs;

  constructor (args: UnionExprArgs) {
    super(args);
  }
}

export type ExceptExprArgs = Merge<[
  SetOperationExprArgs,
]>;

export class ExceptExpr extends SetOperationExpr {
  key = ExpressionKey.EXCEPT;

  static argTypes: RequiredMap<ExceptExprArgs> = {
    ...super.argTypes,
  };

  declare args: ExceptExprArgs;

  constructor (args: ExceptExprArgs) {
    super(args);
  }
}

export type IntersectExprArgs = Merge<[
  SetOperationExprArgs,
]>;

export class IntersectExpr extends SetOperationExpr {
  key = ExpressionKey.INTERSECT;

  static argTypes: RequiredMap<IntersectExprArgs> = {
    ...super.argTypes,
  };

  declare args: IntersectExprArgs;

  constructor (args: IntersectExprArgs) {
    super(args);
  }
}

/**
 * VALUES clause with DuckDB support for ORDER BY, LIMIT, OFFSET
 * @see {@link https://duckdb.org/docs/stable/sql/query_syntax/limit | DuckDB LIMIT}
 */
export type ValuesExprArgs = Merge<[
  UDTFExprArgs,
  {
    expressions: Expression[];
    alias?: TableAliasExpr;
    order?: Expression;
    limit?: number | Expression;
    offset?: number | Expression;
  },
]>;

export class ValuesExpr extends UDTFExpr {
  key = ExpressionKey.VALUES;

  static argTypes: RequiredMap<ValuesExprArgs> = {
    ...super.argTypes,
    expressions: true,
    alias: false,
    order: false,
    limit: false,
    offset: false,
  };

  declare args: ValuesExprArgs;

  constructor (args: ValuesExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $alias (): TableAliasExpr | undefined {
    return this.args.alias;
  }

  get $order (): Expression | undefined {
    return this.args.order;
  }

  get $limit (): number | Expression | undefined {
    return this.args.limit;
  }

  get $offset (): number | Expression | undefined {
    return this.args.offset;
  }
}

export type SubqueryPredicateExprArgs = Merge<[
  PredicateExprArgs,
]>;

export class SubqueryPredicateExpr extends PredicateExpr {
  key = ExpressionKey.SUBQUERY_PREDICATE;

  static argTypes: RequiredMap<SubqueryPredicateExprArgs> = {
    ...super.argTypes,
  };

  declare args: SubqueryPredicateExprArgs;

  constructor (args: SubqueryPredicateExprArgs) {
    super(args);
  }
}

export type AddExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class AddExpr extends BinaryExpr {
  key = ExpressionKey.ADD;

  static argTypes: RequiredMap<AddExprArgs> = {
    ...super.argTypes,
  };

  declare args: AddExprArgs;

  constructor (args: AddExprArgs) {
    super(args);
  }
}

export type ConnectorExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class ConnectorExpr extends BinaryExpr {
  key = ExpressionKey.CONNECTOR;

  static argTypes: RequiredMap<ConnectorExprArgs> = {
    ...super.argTypes,
  };

  declare args: ConnectorExprArgs;

  constructor (args: ConnectorExprArgs) {
    super(args);
  }
}

export type BitwiseAndExprArgs = Merge<[
  BinaryExprArgs,
  {
    this: Expression;
    expression: Expression;
    padside?: Expression;
  },
]>;

export class BitwiseAndExpr extends BinaryExpr {
  key = ExpressionKey.BITWISE_AND;

  static argTypes: RequiredMap<BitwiseAndExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    padside: false,
  };

  declare args: BitwiseAndExprArgs;

  constructor (args: BitwiseAndExprArgs) {
    super(args);
  }

  get $padside (): Expression | undefined {
    return this.args.padside;
  }
}

export type BitwiseLeftShiftExprArgs = Merge<[
  BinaryExprArgs,
  {
    this: Expression;
    expression: Expression;
    requiresInt128?: Expression;
  },
]>;

export class BitwiseLeftShiftExpr extends BinaryExpr {
  key = ExpressionKey.BITWISE_LEFT_SHIFT;

  static argTypes: RequiredMap<BitwiseLeftShiftExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    requiresInt128: false,
  };

  declare args: BitwiseLeftShiftExprArgs;

  constructor (args: BitwiseLeftShiftExprArgs) {
    super(args);
  }

  get $requiresInt128 (): Expression | undefined {
    return this.args.requiresInt128;
  }
}

export type BitwiseOrExprArgs = Merge<[
  BinaryExprArgs,
  {
    this: Expression;
    expression: Expression;
    padside?: Expression;
  },
]>;

export class BitwiseOrExpr extends BinaryExpr {
  key = ExpressionKey.BITWISE_OR;

  static argTypes: RequiredMap<BitwiseOrExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    padside: false,
  };

  declare args: BitwiseOrExprArgs;

  constructor (args: BitwiseOrExprArgs) {
    super(args);
  }

  get $padside (): Expression | undefined {
    return this.args.padside;
  }
}

export type BitwiseRightShiftExprArgs = Merge<[
  BinaryExprArgs,
  {
    this: Expression;
    expression: Expression;
    requiresInt128?: Expression;
  },
]>;

export class BitwiseRightShiftExpr extends BinaryExpr {
  key = ExpressionKey.BITWISE_RIGHT_SHIFT;

  static argTypes: RequiredMap<BitwiseRightShiftExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    requiresInt128: false,
  };

  declare args: BitwiseRightShiftExprArgs;

  constructor (args: BitwiseRightShiftExprArgs) {
    super(args);
  }

  get $requiresInt128 (): Expression | undefined {
    return this.args.requiresInt128;
  }
}

export type BitwiseXorExprArgs = Merge<[
  BinaryExprArgs,
  {
    this: Expression;
    expression: Expression;
    padside?: Expression;
  },
]>;

export class BitwiseXorExpr extends BinaryExpr {
  key = ExpressionKey.BITWISE_XOR;

  static argTypes: RequiredMap<BitwiseXorExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    padside: false,
  };

  declare args: BitwiseXorExprArgs;

  constructor (args: BitwiseXorExprArgs) {
    super(args);
  }

  get $padside (): Expression | undefined {
    return this.args.padside;
  }
}

export type DivExprArgs = Merge<[
  BinaryExprArgs,
  {
    this: Expression;
    expression: Expression;
    typed?: DataTypeExpr;
    safe?: boolean;
  },
]>;

export class DivExpr extends BinaryExpr {
  key = ExpressionKey.DIV;

  static argTypes: RequiredMap<DivExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    typed: false,
    safe: false,
  };

  declare args: DivExprArgs;

  constructor (args: DivExprArgs) {
    super(args);
  }

  get $typed (): DataTypeExpr | undefined {
    return this.args.typed;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }
}

export type OverlapsExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class OverlapsExpr extends BinaryExpr {
  key = ExpressionKey.OVERLAPS;

  static argTypes: RequiredMap<OverlapsExprArgs> = {
    ...super.argTypes,
  };

  declare args: OverlapsExprArgs;

  constructor (args: OverlapsExprArgs) {
    super(args);
  }
}

export type ExtendsLeftExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class ExtendsLeftExpr extends BinaryExpr {
  key = ExpressionKey.EXTENDS_LEFT;

  static argTypes: RequiredMap<ExtendsLeftExprArgs> = {
    ...super.argTypes,
  };

  declare args: ExtendsLeftExprArgs;

  constructor (args: ExtendsLeftExprArgs) {
    super(args);
  }
}

export type ExtendsRightExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class ExtendsRightExpr extends BinaryExpr {
  key = ExpressionKey.EXTENDS_RIGHT;

  static argTypes: RequiredMap<ExtendsRightExprArgs> = {
    ...super.argTypes,
  };

  declare args: ExtendsRightExprArgs;

  constructor (args: ExtendsRightExprArgs) {
    super(args);
  }
}

export type DotExprArgs = Merge<[
  BinaryExprArgs,
  {
    expression: Expression;
  },
]>;

export class DotExpr extends BinaryExpr {
  key = ExpressionKey.DOT;

  static argTypes: RequiredMap<DotExprArgs> = {
    ...super.argTypes,
    expression: true,
  };

  declare args: DotExprArgs;

  constructor (args: DotExprArgs) {
    super(args);
  }

  get isStar (): boolean {
    return this.args.expression.isStar;
  }

  get name (): string {
    return this.args.expression.name;
  }

  get outputName (): string {
    return this.name;
  }

  /**
   * Build a Dot object with a sequence of expressions.
   */
  static build (expressions: Expression[]): DotExpr {
    if (expressions.length < 2) {
      throw new Error('Dot requires >= 2 expressions.');
    }

    return expressions.reduce(
      (x, y) => new DotExpr({
        this: x,
        expression: y,
      }),
    ) as DotExpr;
  }

  /**
   * Return the parts of a table / column in order catalog, db, table.
   */
  get parts (): Expression[] {
    const [thisExpr, ...restParts] = this.flatten();
    const parts = [...restParts];

    parts.reverse();

    for (const arg of COLUMN_PARTS) {
      const part = (thisExpr.args as Record<string, ExpressionValue | ExpressionValueList>)[arg];

      if (part instanceof Expression) {
        parts.push(part);
      }
    }

    parts.reverse();
    return parts;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type DPipeExprArgs = Merge<[
  BinaryExprArgs,
  {
    this: Expression;
    expression: Expression;
    safe?: boolean;
  },
]>;

export class DPipeExpr extends BinaryExpr {
  key = ExpressionKey.D_PIPE;

  static argTypes: RequiredMap<DPipeExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    safe: false,
  };

  declare args: DPipeExprArgs;

  constructor (args: DPipeExprArgs) {
    super(args);
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }
}

export type EQExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class EQExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.EQ;

  static argTypes: RequiredMap<EQExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: EQExprArgs;

  constructor (args: EQExprArgs) {
    super(args);
  }
}

export type NullSafeEQExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class NullSafeEQExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.NULL_SAFE_EQ;

  static argTypes: RequiredMap<NullSafeEQExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: NullSafeEQExprArgs;

  constructor (args: NullSafeEQExprArgs) {
    super(args);
  }
}

export type NullSafeNEQExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class NullSafeNEQExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.NULL_SAFE_NEQ;

  static argTypes: RequiredMap<NullSafeNEQExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: NullSafeNEQExprArgs;

  constructor (args: NullSafeNEQExprArgs) {
    super(args);
  }
}

export type PropertyEQExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class PropertyEQExpr extends BinaryExpr {
  key = ExpressionKey.PROPERTY_EQ;

  static argTypes: RequiredMap<PropertyEQExprArgs> = {
    ...super.argTypes,
  };

  declare args: PropertyEQExprArgs;

  constructor (args: PropertyEQExprArgs) {
    super(args);
  }
}

export type DistanceExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class DistanceExpr extends BinaryExpr {
  key = ExpressionKey.DISTANCE;

  static argTypes: RequiredMap<DistanceExprArgs> = {
    ...super.argTypes,
  };

  declare args: DistanceExprArgs;

  constructor (args: DistanceExprArgs) {
    super(args);
  }
}

export type EscapeExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class EscapeExpr extends BinaryExpr {
  key = ExpressionKey.ESCAPE;

  static argTypes: RequiredMap<EscapeExprArgs> = {
    ...super.argTypes,
  };

  declare args: EscapeExprArgs;

  constructor (args: EscapeExprArgs) {
    super(args);
  }
}

export type GlobExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class GlobExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.GLOB;

  static argTypes: RequiredMap<GlobExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: GlobExprArgs;

  constructor (args: GlobExprArgs) {
    super(args);
  }
}

export type GTExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class GTExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.GT;

  static argTypes: RequiredMap<GTExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: GTExprArgs;

  constructor (args: GTExprArgs) {
    super(args);
  }
}

export type GTEExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class GTEExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.GTE;

  static argTypes: RequiredMap<GTEExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: GTEExprArgs;

  constructor (args: GTEExprArgs) {
    super(args);
  }
}

export type ILikeExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class ILikeExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.ILIKE;

  static argTypes: RequiredMap<ILikeExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: ILikeExprArgs;

  constructor (args: ILikeExprArgs) {
    super(args);
  }
}

export type IntDivExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class IntDivExpr extends BinaryExpr {
  key = ExpressionKey.INT_DIV;

  static argTypes: RequiredMap<IntDivExprArgs> = {
    ...super.argTypes,
  };

  declare args: IntDivExprArgs;

  constructor (args: IntDivExprArgs) {
    super(args);
  }
}

export type IsExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class IsExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.IS;

  static argTypes: RequiredMap<IsExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: IsExprArgs;

  constructor (args: IsExprArgs) {
    super(args);
  }
}

export type KwargExprArgs = Merge<[
  BinaryExprArgs,
]>;

/**
 * Kwarg in special functions like func(kwarg => y).
 */
export class KwargExpr extends BinaryExpr {
  key = ExpressionKey.KWARG;

  static argTypes: RequiredMap<KwargExprArgs> = {
    ...super.argTypes,
  };

  declare args: KwargExprArgs;

  constructor (args: KwargExprArgs) {
    super(args);
  }
}

export type LikeExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class LikeExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.LIKE;

  static argTypes: RequiredMap<LikeExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: LikeExprArgs;

  constructor (args: LikeExprArgs) {
    super(args);
  }
}

export type MatchExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class MatchExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.MATCH;

  static argTypes: RequiredMap<MatchExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: MatchExprArgs;

  constructor (args: MatchExprArgs) {
    super(args);
  }
}

export type LTExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class LTExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.LT;

  static argTypes: RequiredMap<LTExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: LTExprArgs;

  constructor (args: LTExprArgs) {
    super(args);
  }
}

export type LTEExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class LTEExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.LTE;

  static argTypes: RequiredMap<LTEExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: LTEExprArgs;

  constructor (args: LTEExprArgs) {
    super(args);
  }
}

export type ModExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class ModExpr extends BinaryExpr {
  key = ExpressionKey.MOD;

  static argTypes: RequiredMap<ModExprArgs> = {
    ...super.argTypes,
  };

  declare args: ModExprArgs;

  constructor (args: ModExprArgs) {
    super(args);
  }
}

export type MulExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class MulExpr extends BinaryExpr {
  key = ExpressionKey.MUL;

  static argTypes: RequiredMap<MulExprArgs> = {
    ...super.argTypes,
  };

  declare args: MulExprArgs;

  constructor (args: MulExprArgs) {
    super(args);
  }
}

export type NEQExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class NEQExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.NEQ;

  static argTypes: RequiredMap<NEQExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: NEQExprArgs;

  constructor (args: NEQExprArgs) {
    super(args);
  }
}

export type OperatorExprArgs = Merge<[
  BinaryExprArgs,
  {
    this: Expression;
    operator: Expression;
    expression: Expression;
  },
]>;

export class OperatorExpr extends BinaryExpr {
  key = ExpressionKey.OPERATOR;

  static argTypes: RequiredMap<OperatorExprArgs> = {
    ...super.argTypes,
    this: true,
    operator: true,
    expression: true,
  };

  declare args: OperatorExprArgs;

  constructor (args: OperatorExprArgs) {
    super(args);
  }

  get $operator (): Expression {
    return this.args.operator;
  }
}

export type SimilarToExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class SimilarToExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.SIMILAR_TO;

  static argTypes: RequiredMap<SimilarToExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: SimilarToExprArgs;

  constructor (args: SimilarToExprArgs) {
    super(args);
  }
}

export type SubExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class SubExpr extends BinaryExpr {
  key = ExpressionKey.SUB;

  static argTypes: RequiredMap<SubExprArgs> = {
    ...super.argTypes,
  };

  declare args: SubExprArgs;

  constructor (args: SubExprArgs) {
    super(args);
  }
}

export type AdjacentExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class AdjacentExpr extends BinaryExpr {
  key = ExpressionKey.ADJACENT;

  static argTypes: RequiredMap<AdjacentExprArgs> = {
    ...super.argTypes,
  };

  declare args: AdjacentExprArgs;

  constructor (args: AdjacentExprArgs) {
    super(args);
  }
}

export type BitwiseNotExprArgs = Merge<[
  UnaryExprArgs,
]>;

export class BitwiseNotExpr extends UnaryExpr {
  key = ExpressionKey.BITWISE_NOT;

  static argTypes: RequiredMap<BitwiseNotExprArgs> = {
    ...super.argTypes,
  };

  declare args: BitwiseNotExprArgs;

  constructor (args: BitwiseNotExprArgs) {
    super(args);
  }
}

export type NotExprArgs = Merge<[
  UnaryExprArgs,
]>;

export class NotExpr extends UnaryExpr {
  key = ExpressionKey.NOT;

  static argTypes: RequiredMap<NotExprArgs> = {
    ...super.argTypes,
  };

  declare args: NotExprArgs;

  constructor (args: NotExprArgs) {
    super(args);
  }
}

export type ParenExprArgs = Merge<[
  UnaryExprArgs,
  { this: Expression },
]>;

export class ParenExpr extends UnaryExpr {
  key = ExpressionKey.PAREN;

  static argTypes: RequiredMap<ParenExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: ParenExprArgs;

  constructor (args: ParenExprArgs) {
    super(args);
  }

  get outputName (): string {
    return this.args.this.name;
  }
}

export type NegExprArgs = Merge<[
  UnaryExprArgs,
  { this: UnaryExpr }, // NOTE: sqlglot does not have this
]>;

export class NegExpr extends UnaryExpr {
  key = ExpressionKey.NEG;

  static argTypes: RequiredMap<NegExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: NegExprArgs;

  constructor (args: NegExprArgs) {
    super(args);
  }

  toValue (): ExpressionValue {
    if (this.isNumber) {
      return (this.args.this.toValue() as number) * -1;
    }
    return super.toValue();
  }
}

export type BetweenExprArgs = Merge<[
  PredicateExprArgs,
  {
    this: Expression;
    low: Expression;
    high: Expression;
    symmetric?: Expression;
  },
]>;

export class BetweenExpr extends PredicateExpr {
  key = ExpressionKey.BETWEEN;

  static argTypes: RequiredMap<BetweenExprArgs> = {
    ...super.argTypes,
    this: true,
    low: true,
    high: true,
    symmetric: false,
  };

  declare args: BetweenExprArgs;

  constructor (args: BetweenExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $low (): Expression {
    return this.args.low;
  }

  get $high (): Expression {
    return this.args.high;
  }

  get $symmetric (): Expression | undefined {
    return this.args.symmetric;
  }
}

export type InExprArgs = Merge<[
  PredicateExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
    query?: Expression;
    unnest?: UnnestExpr;
    field?: Expression;
    isGlobal?: boolean;
  },
]>;

export class InExpr extends PredicateExpr {
  key = ExpressionKey.IN;

  static argTypes: RequiredMap<InExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
    query: false,
    unnest: false,
    field: false,
    isGlobal: false,
  };

  declare args: InExprArgs;

  constructor (args: InExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $query (): Expression | undefined {
    return this.args.query;
  }

  get $unnest (): UnnestExpr | undefined {
    return this.args.unnest;
  }

  get $field (): Expression | undefined {
    return this.args.field;
  }

  get $isGlobal (): boolean | undefined {
    return this.args.isGlobal;
  }
}

/**
 * Function returns NULL instead of error
 * https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/functions-reference#safe_prefix
 */
export type SafeFuncExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SafeFuncExpr extends FuncExpr {
  key = ExpressionKey.SAFE_FUNC;

  static argTypes: RequiredMap<SafeFuncExprArgs> = {
    ...super.argTypes,
  };

  declare args: SafeFuncExprArgs;

  constructor (args: SafeFuncExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TypeofExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TypeofExpr extends FuncExpr {
  key = ExpressionKey.TYPEOF;

  static argTypes: RequiredMap<TypeofExprArgs> = {
    ...super.argTypes,
  };

  declare args: TypeofExprArgs;

  constructor (args: TypeofExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AcosExprArgs = Merge<[
  FuncExprArgs,
]>;

export class AcosExpr extends FuncExpr {
  key = ExpressionKey.ACOS;

  static argTypes: RequiredMap<AcosExprArgs> = {
    ...super.argTypes,
  };

  declare args: AcosExprArgs;

  constructor (args: AcosExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AcoshExprArgs = Merge<[
  FuncExprArgs,
]>;

export class AcoshExpr extends FuncExpr {
  key = ExpressionKey.ACOSH;

  static argTypes: RequiredMap<AcoshExprArgs> = {
    ...super.argTypes,
  };

  declare args: AcoshExprArgs;

  constructor (args: AcoshExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AsinExprArgs = Merge<[
  FuncExprArgs,
]>;

export class AsinExpr extends FuncExpr {
  key = ExpressionKey.ASIN;

  static argTypes: RequiredMap<AsinExprArgs> = {
    ...super.argTypes,
  };

  declare args: AsinExprArgs;

  constructor (args: AsinExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AsinhExprArgs = Merge<[
  FuncExprArgs,
]>;

export class AsinhExpr extends FuncExpr {
  key = ExpressionKey.ASINH;

  static argTypes: RequiredMap<AsinhExprArgs> = {
    ...super.argTypes,
  };

  declare args: AsinhExprArgs;

  constructor (args: AsinhExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AtanExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class AtanExpr extends FuncExpr {
  key = ExpressionKey.ATAN;

  static argTypes: RequiredMap<AtanExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: AtanExprArgs;

  constructor (args: AtanExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type AtanhExprArgs = Merge<[
  FuncExprArgs,
]>;

export class AtanhExpr extends FuncExpr {
  key = ExpressionKey.ATANH;

  static argTypes: RequiredMap<AtanhExprArgs> = {
    ...super.argTypes,
  };

  declare args: AtanhExprArgs;

  constructor (args: AtanhExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Atan2ExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class Atan2Expr extends FuncExpr {
  key = ExpressionKey.ATAN2;

  static argTypes: RequiredMap<Atan2ExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: Atan2ExprArgs;

  constructor (args: Atan2ExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type CotExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CotExpr extends FuncExpr {
  key = ExpressionKey.COT;

  static argTypes: RequiredMap<CotExprArgs> = {
    ...super.argTypes,
  };

  declare args: CotExprArgs;

  constructor (args: CotExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CothExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CothExpr extends FuncExpr {
  key = ExpressionKey.COTH;

  static argTypes: RequiredMap<CothExprArgs> = {
    ...super.argTypes,
  };

  declare args: CothExprArgs;

  constructor (args: CothExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CosExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CosExpr extends FuncExpr {
  key = ExpressionKey.COS;

  static argTypes: RequiredMap<CosExprArgs> = {
    ...super.argTypes,
  };

  declare args: CosExprArgs;

  constructor (args: CosExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CscExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CscExpr extends FuncExpr {
  key = ExpressionKey.CSC;

  static argTypes: RequiredMap<CscExprArgs> = {
    ...super.argTypes,
  };

  declare args: CscExprArgs;

  constructor (args: CscExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CschExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CschExpr extends FuncExpr {
  key = ExpressionKey.CSCH;

  static argTypes: RequiredMap<CschExprArgs> = {
    ...super.argTypes,
  };

  declare args: CschExprArgs;

  constructor (args: CschExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SecExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SecExpr extends FuncExpr {
  key = ExpressionKey.SEC;

  static argTypes: RequiredMap<SecExprArgs> = {
    ...super.argTypes,
  };

  declare args: SecExprArgs;

  constructor (args: SecExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SechExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SechExpr extends FuncExpr {
  key = ExpressionKey.SECH;

  static argTypes: RequiredMap<SechExprArgs> = {
    ...super.argTypes,
  };

  declare args: SechExprArgs;

  constructor (args: SechExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SinExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SinExpr extends FuncExpr {
  key = ExpressionKey.SIN;

  static argTypes: RequiredMap<SinExprArgs> = {
    ...super.argTypes,
  };

  declare args: SinExprArgs;

  constructor (args: SinExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SinhExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SinhExpr extends FuncExpr {
  key = ExpressionKey.SINH;

  static argTypes: RequiredMap<SinhExprArgs> = {
    ...super.argTypes,
  };

  declare args: SinhExprArgs;

  constructor (args: SinhExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TanExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TanExpr extends FuncExpr {
  key = ExpressionKey.TAN;

  static argTypes: RequiredMap<TanExprArgs> = {
    ...super.argTypes,
  };

  declare args: TanExprArgs;

  constructor (args: TanExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TanhExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TanhExpr extends FuncExpr {
  key = ExpressionKey.TANH;

  static argTypes: RequiredMap<TanhExprArgs> = {
    ...super.argTypes,
  };

  declare args: TanhExprArgs;

  constructor (args: TanhExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DegreesExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DegreesExpr extends FuncExpr {
  key = ExpressionKey.DEGREES;

  static argTypes: RequiredMap<DegreesExprArgs> = {
    ...super.argTypes,
  };

  declare args: DegreesExprArgs;

  constructor (args: DegreesExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CoshExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CoshExpr extends FuncExpr {
  key = ExpressionKey.COSH;

  static argTypes: RequiredMap<CoshExprArgs> = {
    ...super.argTypes,
  };

  declare args: CoshExprArgs;

  constructor (args: CoshExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CosineDistanceExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class CosineDistanceExpr extends FuncExpr {
  key = ExpressionKey.COSINE_DISTANCE;

  static argTypes: RequiredMap<CosineDistanceExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: CosineDistanceExprArgs;

  constructor (args: CosineDistanceExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type DotProductExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class DotProductExpr extends FuncExpr {
  key = ExpressionKey.DOT_PRODUCT;

  static argTypes: RequiredMap<DotProductExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: DotProductExprArgs;

  constructor (args: DotProductExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type EuclideanDistanceExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class EuclideanDistanceExpr extends FuncExpr {
  key = ExpressionKey.EUCLIDEAN_DISTANCE;

  static argTypes: RequiredMap<EuclideanDistanceExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: EuclideanDistanceExprArgs;

  constructor (args: EuclideanDistanceExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ManhattanDistanceExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class ManhattanDistanceExpr extends FuncExpr {
  key = ExpressionKey.MANHATTAN_DISTANCE;

  static argTypes: RequiredMap<ManhattanDistanceExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: ManhattanDistanceExprArgs;

  constructor (args: ManhattanDistanceExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type JarowinklerSimilarityExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class JarowinklerSimilarityExpr extends FuncExpr {
  key = ExpressionKey.JAROWINKLER_SIMILARITY;

  static argTypes: RequiredMap<JarowinklerSimilarityExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: JarowinklerSimilarityExprArgs;

  constructor (args: JarowinklerSimilarityExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type AggFuncExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class AggFuncExpr extends FuncExpr {
  key = ExpressionKey.AGG_FUNC;

  static argTypes: RequiredMap<AggFuncExprArgs> = {
    ...super.argTypes,
  };

  declare args: AggFuncExprArgs;

  constructor (args: AggFuncExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitwiseCountExprArgs = Merge<[
  FuncExprArgs,
]>;

export class BitwiseCountExpr extends FuncExpr {
  key = ExpressionKey.BITWISE_COUNT;

  static argTypes: RequiredMap<BitwiseCountExprArgs> = {
    ...super.argTypes,
  };

  declare args: BitwiseCountExprArgs;

  constructor (args: BitwiseCountExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitmapBucketNumberExprArgs = Merge<[
  FuncExprArgs,
]>;

export class BitmapBucketNumberExpr extends FuncExpr {
  key = ExpressionKey.BITMAP_BUCKET_NUMBER;

  static argTypes: RequiredMap<BitmapBucketNumberExprArgs> = {
    ...super.argTypes,
  };

  declare args: BitmapBucketNumberExprArgs;

  constructor (args: BitmapBucketNumberExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitmapCountExprArgs = Merge<[
  FuncExprArgs,
]>;

export class BitmapCountExpr extends FuncExpr {
  key = ExpressionKey.BITMAP_COUNT;

  static argTypes: RequiredMap<BitmapCountExprArgs> = {
    ...super.argTypes,
  };

  declare args: BitmapCountExprArgs;

  constructor (args: BitmapCountExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitmapBitPositionExprArgs = Merge<[
  FuncExprArgs,
]>;

export class BitmapBitPositionExpr extends FuncExpr {
  key = ExpressionKey.BITMAP_BIT_POSITION;

  static argTypes: RequiredMap<BitmapBitPositionExprArgs> = {
    ...super.argTypes,
  };

  declare args: BitmapBitPositionExprArgs;

  constructor (args: BitmapBitPositionExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ByteLengthExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ByteLengthExpr extends FuncExpr {
  key = ExpressionKey.BYTE_LENGTH;

  static argTypes: RequiredMap<ByteLengthExprArgs> = {
    ...super.argTypes,
  };

  declare args: ByteLengthExprArgs;

  constructor (args: ByteLengthExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BoolnotExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    roundInput?: Expression;
  },
]>;

export class BoolnotExpr extends FuncExpr {
  key = ExpressionKey.BOOLNOT;

  static argTypes: RequiredMap<BoolnotExprArgs> = {
    ...super.argTypes,
    this: true,
    roundInput: false,
  };

  declare args: BoolnotExprArgs;

  constructor (args: BoolnotExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $roundInput (): Expression | undefined {
    return this.args.roundInput;
  }

  static {
    this.register();
  }
}

export type BoolandExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    roundInput?: Expression;
  },
]>;

export class BoolandExpr extends FuncExpr {
  key = ExpressionKey.BOOLAND;

  static argTypes: RequiredMap<BoolandExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    roundInput: false,
  };

  declare args: BoolandExprArgs;

  constructor (args: BoolandExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $roundInput (): Expression | undefined {
    return this.args.roundInput;
  }

  static {
    this.register();
  }
}

export type BoolorExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    roundInput?: Expression;
  },
]>;

export class BoolorExpr extends FuncExpr {
  key = ExpressionKey.BOOLOR;

  static argTypes: RequiredMap<BoolorExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    roundInput: false,
  };

  declare args: BoolorExprArgs;

  constructor (args: BoolorExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $roundInput (): Expression | undefined {
    return this.args.roundInput;
  }

  static {
    this.register();
  }
}

/**
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#bool_for_json
 */
export type JSONBoolExprArgs = Merge<[
  FuncExprArgs,
]>;

export class JSONBoolExpr extends FuncExpr {
  key = ExpressionKey.JSON_BOOL;

  static argTypes: RequiredMap<JSONBoolExprArgs> = {
    ...super.argTypes,
  };

  declare args: JSONBoolExprArgs;

  constructor (args: JSONBoolExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayRemoveExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    nullPropagation?: boolean;
  },
]>;

export class ArrayRemoveExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_REMOVE;

  static argTypes: RequiredMap<ArrayRemoveExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    nullPropagation: false,
  };

  declare args: ArrayRemoveExprArgs;

  constructor (args: ArrayRemoveExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $nullPropagation (): boolean | undefined {
    return this.args.nullPropagation;
  }

  static {
    this.register();
  }
}

export type AbsExprArgs = Merge<[
  FuncExprArgs,
]>;

export class AbsExpr extends FuncExpr {
  key = ExpressionKey.ABS;

  static argTypes: RequiredMap<AbsExprArgs> = {
    ...super.argTypes,
  };

  declare args: AbsExprArgs;

  constructor (args: AbsExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ApproxTopKEstimateExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class ApproxTopKEstimateExpr extends FuncExpr {
  key = ExpressionKey.APPROX_TOP_K_ESTIMATE;

  static argTypes: RequiredMap<ApproxTopKEstimateExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: ApproxTopKEstimateExprArgs;

  constructor (args: ApproxTopKEstimateExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type FarmFingerprintExprArgs = Merge<[
  FuncExprArgs,
  { expressions: Expression[] },
]>;

export class FarmFingerprintExpr extends FuncExpr {
  key = ExpressionKey.FARM_FINGERPRINT;

  static isVarLenArgs = true;

  static _sqlNames = ['FARM_FINGERPRINT', 'FARMFINGERPRINT64'];

  static argTypes: RequiredMap<FarmFingerprintExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: FarmFingerprintExprArgs;

  constructor (args: FarmFingerprintExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type FlattenExprArgs = Merge<[
  FuncExprArgs,
]>;

export class FlattenExpr extends FuncExpr {
  key = ExpressionKey.FLATTEN;

  static argTypes: RequiredMap<FlattenExprArgs> = {
    ...super.argTypes,
  };

  declare args: FlattenExprArgs;

  constructor (args: FlattenExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Float64ExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class Float64Expr extends FuncExpr {
  key = ExpressionKey.FLOAT64;

  static argTypes: RequiredMap<Float64ExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: Float64ExprArgs;

  constructor (args: Float64ExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

/**
 * https://spark.apache.org/docs/latest/api/sql/index.html#transform
 */
export type TransformExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class TransformExpr extends FuncExpr {
  key = ExpressionKey.TRANSFORM;

  static argTypes: RequiredMap<TransformExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: TransformExprArgs;

  constructor (args: TransformExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type TranslateExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    from: Expression;
    to: Expression;
  },
]>;

export class TranslateExpr extends FuncExpr {
  key = ExpressionKey.TRANSLATE;

  static argTypes: RequiredMap<TranslateExprArgs> = {
    ...super.argTypes,
    this: true,
    from: true,
    to: true,
  };

  declare args: TranslateExprArgs;

  constructor (args: TranslateExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $from (): Expression {
    return this.args.from;
  }

  get $to (): Expression {
    return this.args.to;
  }

  static {
    this.register();
  }
}

export type AnonymousExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class AnonymousExpr extends FuncExpr {
  key = ExpressionKey.ANONYMOUS;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<AnonymousExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
  };

  declare args: AnonymousExprArgs;

  constructor (args: AnonymousExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get name (): string {
    return typeof this.args.this === 'string' ? this.args.this : this.args.this.name;
  }

  static {
    this.register();
  }
}

export type ApplyExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class ApplyExpr extends FuncExpr {
  key = ExpressionKey.APPLY;

  static argTypes: RequiredMap<ApplyExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: ApplyExprArgs;

  constructor (args: ApplyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ArrayExprArgs = Merge<[
  FuncExprArgs,
  {
    expressions?: (string | number | boolean | Token | Expression)[];
    bracketNotation?: Expression;
    structNameInheritance?: string;
  },
]>;

export class ArrayExpr extends FuncExpr {
  key = ExpressionKey.ARRAY;

  static isVarLenArgs = true;

  /**
   * Defines the arguments (properties and child expressions) for Array expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ArrayExprArgs> = {
    ...super.argTypes,
    expressions: false,
    bracketNotation: false,
    structNameInheritance: false,
  };

  declare args: ArrayExprArgs;

  constructor (args: ArrayExprArgs) {
    super(args);
  }

  get $expressions (): (string | number | boolean | Token | Expression)[] | undefined {
    return this.args.expressions;
  }

  get $bracketNotation (): Expression | undefined {
    return this.args.bracketNotation;
  }

  get $structNameInheritance (): string | undefined {
    return this.args.structNameInheritance;
  }

  static {
    this.register();
  }
}

export type AsciiExprArgs = Merge<[
  FuncExprArgs,
]>;

export class AsciiExpr extends FuncExpr {
  key = ExpressionKey.ASCII;

  static argTypes: RequiredMap<AsciiExprArgs> = {
    ...super.argTypes,
  };

  declare args: AsciiExprArgs;

  constructor (args: AsciiExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToArrayExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ToArrayExpr extends FuncExpr {
  key = ExpressionKey.TO_ARRAY;

  static argTypes: RequiredMap<ToArrayExprArgs> = {
    ...super.argTypes,
  };

  declare args: ToArrayExprArgs;

  constructor (args: ToArrayExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToBooleanExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    safe?: boolean;
  },
]>;

export class ToBooleanExpr extends FuncExpr {
  key = ExpressionKey.TO_BOOLEAN;

  static argTypes: RequiredMap<ToBooleanExprArgs> = {
    ...super.argTypes,
    this: true,
    safe: false,
  };

  declare args: ToBooleanExprArgs;

  constructor (args: ToBooleanExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }

  static {
    this.register();
  }
}

export type ListExprArgs = Merge<[
  FuncExprArgs,
  { expressions?: Expression[] },
]>;

export class ListExpr extends FuncExpr {
  key = ExpressionKey.LIST;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<ListExprArgs> = {
    ...super.argTypes,
    expressions: false,
  };

  declare args: ListExprArgs;

  constructor (args: ListExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

/**
 * String pad, kind True -> LPAD, False -> RPAD
 */
export type PadExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    fillPattern?: Expression;
    isLeft: boolean;
  },
]>;

export class PadExpr extends FuncExpr {
  key = ExpressionKey.PAD;

  static argTypes: RequiredMap<PadExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    fillPattern: false,
    isLeft: true,
  };

  declare args: PadExprArgs;

  constructor (args: PadExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $fillPattern (): Expression | undefined {
    return this.args.fillPattern;
  }

  get $isLeft (): boolean {
    return this.args.isLeft;
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/to_char
 * https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/TO_CHAR-number.html
 */
export type ToCharExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    format?: Expression;
    nlsparam?: Expression;
    isNumeric?: Expression;
  },
]>;

export class ToCharExpr extends FuncExpr {
  key = ExpressionKey.TO_CHAR;

  static argTypes: RequiredMap<ToCharExprArgs> = {
    ...super.argTypes,
    this: true,
    format: false,
    nlsparam: false,
    isNumeric: false,
  };

  declare args: ToCharExprArgs;

  constructor (args: ToCharExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $format (): Expression | undefined {
    return this.args.format;
  }

  get $nlsparam (): Expression | undefined {
    return this.args.nlsparam;
  }

  get $isNumeric (): Expression | undefined {
    return this.args.isNumeric;
  }

  static {
    this.register();
  }
}

export type ToCodePointsExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ToCodePointsExpr extends FuncExpr {
  key = ExpressionKey.TO_CODE_POINTS;

  static argTypes: RequiredMap<ToCodePointsExprArgs> = {
    ...super.argTypes,
  };

  declare args: ToCodePointsExprArgs;

  constructor (args: ToCodePointsExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/to_decimal
 * https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/TO_NUMBER.html
 */
export type ToNumberExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    format?: Expression;
    nlsparam?: Expression;
    precision?: Expression;
    scale?: Expression;
    safe?: boolean;
    safeName?: string;
  },
]>;

export class ToNumberExpr extends FuncExpr {
  key = ExpressionKey.TO_NUMBER;

  static argTypes: RequiredMap<ToNumberExprArgs> = {
    ...super.argTypes,
    this: true,
    format: false,
    nlsparam: false,
    precision: false,
    scale: false,
    safe: false,
    safeName: false,
  };

  declare args: ToNumberExprArgs;

  constructor (args: ToNumberExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $format (): Expression | undefined {
    return this.args.format;
  }

  get $nlsparam (): Expression | undefined {
    return this.args.nlsparam;
  }

  get $precision (): Expression | undefined {
    return this.args.precision;
  }

  get $scale (): Expression | undefined {
    return this.args.scale;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }

  get $safeName (): string | undefined {
    return this.args.safeName;
  }

  static {
    this.register();
  }
}

export type ToDoubleExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    format?: Expression;
    safe?: boolean;
  },
]>;

export class ToDoubleExpr extends FuncExpr {
  key = ExpressionKey.TO_DOUBLE;

  /**
   * Defines the arguments (properties and child expressions) for ToDouble expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ToDoubleExprArgs> = {
    ...super.argTypes,
    this: true,
    format: false,
    safe: false,
  };

  declare args: ToDoubleExprArgs;

  constructor (args: ToDoubleExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $format (): Expression | undefined {
    return this.args.format;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }

  static {
    this.register();
  }
}

export type ToDecfloatExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    format?: Expression;
  },
]>;

export class ToDecfloatExpr extends FuncExpr {
  key = ExpressionKey.TO_DECFLOAT;

  static argTypes: RequiredMap<ToDecfloatExprArgs> = {
    ...super.argTypes,
    this: true,
    format: false,
  };

  declare args: ToDecfloatExprArgs;

  constructor (args: ToDecfloatExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $format (): Expression | undefined {
    return this.args.format;
  }

  static {
    this.register();
  }
}

export type TryToDecfloatExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    format?: Expression;
  },
]>;

export class TryToDecfloatExpr extends FuncExpr {
  key = ExpressionKey.TRY_TO_DECFLOAT;

  static argTypes: RequiredMap<TryToDecfloatExprArgs> = {
    ...super.argTypes,
    this: true,
    format: false,
  };

  declare args: TryToDecfloatExprArgs;

  constructor (args: TryToDecfloatExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $format (): Expression | undefined {
    return this.args.format;
  }

  static {
    this.register();
  }
}

export type ToFileExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    path?: Expression;
    safe?: boolean;
  },
]>;

export class ToFileExpr extends FuncExpr {
  key = ExpressionKey.TO_FILE;

  /**
   * Defines the arguments (properties and child expressions) for ToFile expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ToFileExprArgs> = {
    ...super.argTypes,
    this: true,
    path: false,
    safe: false,
  };

  declare args: ToFileExprArgs;

  constructor (args: ToFileExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $path (): Expression | undefined {
    return this.args.path;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }

  static {
    this.register();
  }
}

export type CodePointsToBytesExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CodePointsToBytesExpr extends FuncExpr {
  key = ExpressionKey.CODE_POINTS_TO_BYTES;

  static argTypes: RequiredMap<CodePointsToBytesExprArgs> = {
    ...super.argTypes,
  };

  declare args: CodePointsToBytesExprArgs;

  constructor (args: CodePointsToBytesExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ColumnsExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    unpack?: Expression;
  },
]>;

export class ColumnsExpr extends FuncExpr {
  key = ExpressionKey.COLUMNS;

  static argTypes: RequiredMap<ColumnsExprArgs> = {
    ...super.argTypes,
    this: true,
    unpack: false,
  };

  declare args: ColumnsExprArgs;

  constructor (args: ColumnsExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $unpack (): Expression | undefined {
    return this.args.unpack;
  }

  static {
    this.register();
  }
}

export type ConvertExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    style?: Expression;
    safe?: boolean;
  },
]>;

export class ConvertExpr extends FuncExpr {
  key = ExpressionKey.CONVERT;

  /**
   * Defines the arguments (properties and child expressions) for Convert expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ConvertExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    style: false,
    safe: false,
  };

  declare args: ConvertExprArgs;

  constructor (args: ConvertExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $style (): Expression | undefined {
    return this.args.style;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }

  static {
    this.register();
  }
}

export type ConvertToCharsetExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    dest: Expression;
    source?: Expression;
  },
]>;

export class ConvertToCharsetExpr extends FuncExpr {
  key = ExpressionKey.CONVERT_TO_CHARSET;

  /**
   * Defines the arguments (properties and child expressions) for ConvertToCharset expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ConvertToCharsetExprArgs> = {
    ...super.argTypes,
    this: true,
    dest: true,
    source: false,
  };

  declare args: ConvertToCharsetExprArgs;

  constructor (args: ConvertToCharsetExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $dest (): Expression {
    return this.args.dest;
  }

  get $source (): Expression | undefined {
    return this.args.source;
  }

  static {
    this.register();
  }
}

export type ConvertTimezoneExprArgs = Merge<[
  FuncExprArgs,
  {
    sourceTz?: Expression;
    targetTz: Expression;
    timestamp: Expression;
    options?: Expression[];
  },
]>;

export class ConvertTimezoneExpr extends FuncExpr {
  key = ExpressionKey.CONVERT_TIMEZONE;

  /**
   * Defines the arguments (properties and child expressions) for ConvertTimezone expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ConvertTimezoneExprArgs> = {
    ...super.argTypes,
    sourceTz: false,
    targetTz: true,
    timestamp: true,
    options: false,
  };

  declare args: ConvertTimezoneExprArgs;

  constructor (args: ConvertTimezoneExprArgs) {
    super(args);
  }

  get $sourceTz (): Expression | undefined {
    return this.args.sourceTz;
  }

  get $targetTz (): Expression {
    return this.args.targetTz;
  }

  get $timestamp (): Expression {
    return this.args.timestamp;
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }

  static {
    this.register();
  }
}

export type CodePointsToStringExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CodePointsToStringExpr extends FuncExpr {
  key = ExpressionKey.CODE_POINTS_TO_STRING;

  static argTypes: RequiredMap<CodePointsToStringExprArgs> = {
    ...super.argTypes,
  };

  declare args: CodePointsToStringExprArgs;

  constructor (args: CodePointsToStringExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type GenerateSeriesExprArgs = Merge<[
  FuncExprArgs,
  {
    start: Expression;
    end: Expression;
    step?: Expression;
    isEndExclusive?: Expression;
  },
]>;

export class GenerateSeriesExpr extends FuncExpr {
  key = ExpressionKey.GENERATE_SERIES;

  static argTypes: RequiredMap<GenerateSeriesExprArgs> = {
    ...super.argTypes,
    start: true,
    end: true,
    step: false,
    isEndExclusive: false,
  };

  declare args: GenerateSeriesExprArgs;

  constructor (args: GenerateSeriesExprArgs) {
    super(args);
  }

  get $start (): Expression {
    return this.args.start;
  }

  get $end (): Expression {
    return this.args.end;
  }

  get $step (): Expression | undefined {
    return this.args.step;
  }

  get $isEndExclusive (): Expression | undefined {
    return this.args.isEndExclusive;
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/generator
 */
export type GeneratorExprArgs = Merge<[
  FuncExprArgs,
  UDTFExprArgs,
  {
    rowcount?: Expression;
    timelimit?: Expression;
  },
]>;

export class GeneratorExpr extends multiInherit(FuncExpr, UDTFExpr) {
  key = ExpressionKey.GENERATOR;

  static argTypes: RequiredMap<GeneratorExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    rowcount: false,
    timelimit: false,
  };

  declare args: GeneratorExprArgs;

  constructor (args: GeneratorExprArgs) {
    super(args);
  }

  get $rowcount (): Expression | undefined {
    return this.args.rowcount;
  }

  get $timelimit (): Expression | undefined {
    return this.args.timelimit;
  }

  static {
    this.register();
  }
}

export type AIClassifyExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    categories: Expression;
    config?: Expression;
  },
]>;

export class AIClassifyExpr extends FuncExpr {
  key = ExpressionKey.AI_CLASSIFY;

  static _sqlNames = ['AI_CLASSIFY'];

  static argTypes: RequiredMap<AIClassifyExprArgs> = {
    ...super.argTypes,
    this: true,
    categories: true,
    config: false,
  };

  declare args: AIClassifyExprArgs;

  constructor (args: AIClassifyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $categories (): Expression {
    return this.args.categories;
  }

  get $config (): Expression | undefined {
    return this.args.config;
  }

  static {
    this.register();
  }
}

export type ArrayAllExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class ArrayAllExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_ALL;

  static argTypes: RequiredMap<ArrayAllExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: ArrayAllExprArgs;

  constructor (args: ArrayAllExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

/**
 * Represents Python's `any(f(x) for x in array)`, where `array` is `this` and `f` is `expression`
 */
export type ArrayAnyExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class ArrayAnyExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_ANY;

  static argTypes: RequiredMap<ArrayAnyExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: ArrayAnyExprArgs;

  constructor (args: ArrayAnyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ArrayAppendExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    nullPropagation?: boolean;
  },
]>;

export class ArrayAppendExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_APPEND;

  static argTypes: RequiredMap<ArrayAppendExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    nullPropagation: false,
  };

  declare args: ArrayAppendExprArgs;

  constructor (args: ArrayAppendExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $nullPropagation (): boolean | undefined {
    return this.args.nullPropagation;
  }

  static {
    this.register();
  }
}

export type ArrayPrependExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    nullPropagation?: boolean;
  },
]>;

export class ArrayPrependExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_PREPEND;

  static argTypes: RequiredMap<ArrayPrependExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    nullPropagation: false,
  };

  declare args: ArrayPrependExprArgs;

  constructor (args: ArrayPrependExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $nullPropagation (): boolean | undefined {
    return this.args.nullPropagation;
  }

  static {
    this.register();
  }
}

export type ArrayConcatExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
    nullPropagation?: boolean;
  },
]>;

export class ArrayConcatExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_CONCAT;

  static _sqlNames = ['ARRAY_CONCAT', 'ARRAY_CAT'];

  static isVarLenArgs = true;

  static argTypes: RequiredMap<ArrayConcatExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
    nullPropagation: false,
  };

  declare args: ArrayConcatExprArgs;

  constructor (args: ArrayConcatExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $nullPropagation (): boolean | undefined {
    return this.args.nullPropagation;
  }

  static {
    this.register();
  }
}

export type ArrayCompactExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ArrayCompactExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_COMPACT;

  static argTypes: RequiredMap<ArrayCompactExprArgs> = {
    ...super.argTypes,
  };

  declare args: ArrayCompactExprArgs;

  constructor (args: ArrayCompactExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayInsertExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    position: Expression;
    expression: Expression;
    offset?: Expression;
  },
]>;

export class ArrayInsertExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_INSERT;

  static argTypes: RequiredMap<ArrayInsertExprArgs> = {
    ...super.argTypes,
    this: true,
    position: true,
    expression: true,
    offset: false,
  };

  declare args: ArrayInsertExprArgs;

  constructor (args: ArrayInsertExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $position (): Expression {
    return this.args.position;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $offset (): Expression | undefined {
    return this.args.offset;
  }

  static {
    this.register();
  }
}

export type ArrayRemoveAtExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    position: Expression;
  },
]>;

export class ArrayRemoveAtExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_REMOVE_AT;

  static argTypes: RequiredMap<ArrayRemoveAtExprArgs> = {
    ...super.argTypes,
    this: true,
    position: true,
  };

  declare args: ArrayRemoveAtExprArgs;

  constructor (args: ArrayRemoveAtExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $position (): Expression {
    return this.args.position;
  }

  static {
    this.register();
  }
}

export type ArrayConstructCompactExprArgs = Merge<[
  FuncExprArgs,
  { expressions?: Expression[] },
]>;

export class ArrayConstructCompactExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_CONSTRUCT_COMPACT;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<ArrayConstructCompactExprArgs> = {
    ...super.argTypes,
    expressions: false,
  };

  declare args: ArrayConstructCompactExprArgs;

  constructor (args: ArrayConstructCompactExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type ArrayContainsExprArgs = Merge<[
  BinaryExprArgs,
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    ensureVariant?: Expression;
  },
]>;

export class ArrayContainsExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.ARRAY_CONTAINS;

  static _sqlNames = ['ARRAY_CONTAINS', 'ARRAY_HAS'];

  static argTypes: RequiredMap<ArrayContainsExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    ensureVariant: false,
  };

  declare args: ArrayContainsExprArgs;

  constructor (args: ArrayContainsExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $ensureVariant (): Expression | undefined {
    return this.args.ensureVariant;
  }

  static {
    this.register();
  }
}

export type ArrayContainsAllExprArgs = Merge<[
  BinaryExprArgs,
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class ArrayContainsAllExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.ARRAY_CONTAINS_ALL;

  static _sqlNames = ['ARRAY_CONTAINS_ALL', 'ARRAY_HAS_ALL'];

  static argTypes: RequiredMap<ArrayContainsAllExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: ArrayContainsAllExprArgs;

  constructor (args: ArrayContainsAllExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ArrayFilterExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class ArrayFilterExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_FILTER;

  static _sqlNames = ['FILTER', 'ARRAY_FILTER'];

  static argTypes: RequiredMap<ArrayFilterExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: ArrayFilterExprArgs;

  constructor (args: ArrayFilterExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ArrayFirstExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ArrayFirstExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_FIRST;

  static argTypes: RequiredMap<ArrayFirstExprArgs> = {
    ...super.argTypes,
  };

  declare args: ArrayFirstExprArgs;

  constructor (args: ArrayFirstExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayLastExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ArrayLastExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_LAST;

  static argTypes: RequiredMap<ArrayLastExprArgs> = {
    ...super.argTypes,
  };

  declare args: ArrayLastExprArgs;

  constructor (args: ArrayLastExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayReverseExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ArrayReverseExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_REVERSE;

  static argTypes: RequiredMap<ArrayReverseExprArgs> = {
    ...super.argTypes,
  };

  declare args: ArrayReverseExprArgs;

  constructor (args: ArrayReverseExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArraySliceExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    start: Expression;
    end?: Expression;
    step?: Expression;
  },
]>;

export class ArraySliceExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_SLICE;

  static argTypes: RequiredMap<ArraySliceExprArgs> = {
    ...super.argTypes,
    this: true,
    start: true,
    end: false,
    step: false,
  };

  declare args: ArraySliceExprArgs;

  constructor (args: ArraySliceExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $start (): Expression {
    return this.args.start;
  }

  get $end (): Expression | undefined {
    return this.args.end;
  }

  get $step (): Expression | undefined {
    return this.args.step;
  }

  static {
    this.register();
  }
}

export type ArrayToStringExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    null?: Expression;
  },
]>;

export class ArrayToStringExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_TO_STRING;

  static _sqlNames = ['ARRAY_TO_STRING', 'ARRAY_JOIN'];

  static argTypes: RequiredMap<ArrayToStringExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    null: false,
  };

  declare args: ArrayToStringExprArgs;

  constructor (args: ArrayToStringExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $null (): Expression | undefined {
    return this.args.null;
  }

  static {
    this.register();
  }
}

export type ArrayIntersectExprArgs = Merge<[
  FuncExprArgs,
  { expressions: Expression[] },
]>;

export class ArrayIntersectExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_INTERSECT;

  static isVarLenArgs = true;

  static _sqlNames = ['ARRAY_INTERSECT', 'ARRAY_INTERSECTION'];

  static argTypes: RequiredMap<ArrayIntersectExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: ArrayIntersectExprArgs;

  constructor (args: ArrayIntersectExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type StPointExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    null?: Expression;
  },
]>;

export class StPointExpr extends FuncExpr {
  key = ExpressionKey.ST_POINT;

  static _sqlNames = ['ST_POINT', 'ST_MAKEPOINT'];

  static argTypes: RequiredMap<StPointExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    null: false,
  };

  declare args: StPointExprArgs;

  constructor (args: StPointExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $null (): Expression | undefined {
    return this.args.null;
  }

  static {
    this.register();
  }
}

export type StDistanceExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    useSpheroid?: Expression;
  },
]>;

export class StDistanceExpr extends FuncExpr {
  key = ExpressionKey.ST_DISTANCE;

  static argTypes: RequiredMap<StDistanceExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    useSpheroid: false,
  };

  declare args: StDistanceExprArgs;

  constructor (args: StDistanceExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $useSpheroid (): Expression | undefined {
    return this.args.useSpheroid;
  }

  static {
    this.register();
  }
}

/**
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#string
 */
export type StringExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    zone?: Expression;
  },
]>;

export class StringExpr extends FuncExpr {
  key = ExpressionKey.STRING;

  static argTypes: RequiredMap<StringExprArgs> = {
    ...super.argTypes,
    this: true,
    zone: false,
  };

  declare args: StringExprArgs;

  constructor (args: StringExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  static {
    this.register();
  }
}

export type StringToArrayExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
    null?: Expression;
  },
]>;

export class StringToArrayExpr extends FuncExpr {
  key = ExpressionKey.STRING_TO_ARRAY;

  static _sqlNames = [
    'STRING_TO_ARRAY',
    'SPLIT_BY_STRING',
    'STRTOK_TO_ARRAY',
  ];

  static argTypes: RequiredMap<StringToArrayExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
    null: false,
  };

  declare args: StringToArrayExprArgs;

  constructor (args: StringToArrayExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $null (): Expression | undefined {
    return this.args.null;
  }

  static {
    this.register();
  }
}

export type ArrayOverlapsExprArgs = Merge<[
  BinaryExprArgs,
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class ArrayOverlapsExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.ARRAY_OVERLAPS;

  static argTypes: RequiredMap<ArrayOverlapsExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: ArrayOverlapsExprArgs;

  constructor (args: ArrayOverlapsExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ArraySizeExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class ArraySizeExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_SIZE;

  static _sqlNames = ['ARRAY_SIZE', 'ARRAY_LENGTH'];

  static argTypes: RequiredMap<ArraySizeExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: ArraySizeExprArgs;

  constructor (args: ArraySizeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ArraySortExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class ArraySortExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_SORT;

  static argTypes: RequiredMap<ArraySortExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: ArraySortExprArgs;

  constructor (args: ArraySortExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ArraySumExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class ArraySumExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_SUM;

  static argTypes: RequiredMap<ArraySumExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: ArraySumExprArgs;

  constructor (args: ArraySumExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ArraysZipExprArgs = Merge<[
  FuncExprArgs,
  { expressions?: Expression[] },
]>;

export class ArraysZipExpr extends FuncExpr {
  key = ExpressionKey.ARRAYS_ZIP;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<ArraysZipExprArgs> = {
    ...super.argTypes,
    expressions: false,
  };

  declare args: ArraysZipExprArgs;

  constructor (args: ArraysZipExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type CaseExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    ifs: Expression[];
    default?: Expression;
  },
]>;

export class CaseExpr extends FuncExpr {
  key = ExpressionKey.CASE;

  /**
   * Defines the arguments (properties and child expressions) for Case expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<CaseExprArgs> = {
    ...super.argTypes,
    this: false,
    ifs: true,
    default: false,
  };

  declare args: CaseExprArgs;

  constructor (args: CaseExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $ifs (): Expression[] {
    return this.args.ifs;
  }

  get $default (): Expression | undefined {
    return this.args.default;
  }

  when (
    condition: string | Expression,
    then: string | Expression,
    copy = true,
    options?: { dialect?: DialectType;
      prefix?: string; },
  ): CaseExpr {
    const instance = maybeCopy(this, copy);
    instance.append(
      'ifs',
      new IfExpr({
        this: maybeParse(condition, options),
        true: maybeParse(then, options),
      }),
    );
    return instance;
  }

  else (
    condition: string | Expression,
    copy = true,
    options?: { dialect?: DialectType;
      prefix?: string; },
  ): CaseExpr {
    const instance = maybeCopy(this, copy);
    instance.setArgKey('default', maybeParse(condition, options));
    return instance;
  }

  static {
    this.register();
  }
}

export type CastExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    to: DataTypeExpr;
    format?: string;
    safe?: boolean;
    action?: Expression;
    default?: Expression;
  },
]>;

export class CastExpr extends FuncExpr {
  key = ExpressionKey.CAST;

  /**
   * Defines the arguments (properties and child expressions) for Cast expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<CastExprArgs> = {
    ...super.argTypes,
    this: true,
    to: true,
    format: false,
    safe: false,
    action: false,
    default: false,
  };

  declare args: CastExprArgs;

  constructor (args: CastExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $to (): DataTypeExpr {
    return this.args.to;
  }

  get $format (): string | undefined {
    return this.args.format;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }

  get $action (): Expression | undefined {
    return this.args.action;
  }

  get $default (): Expression | undefined {
    return this.args.default;
  }

  get name (): string {
    return this.$this.name || '';
  }

  get to (): Expression {
    return this.$to;
  }

  get outputName (): string {
    return this.name;
  }

  isType (
    dtypes: (DataTypeExprKind | DataTypeExpr | IdentifierExpr | DotExpr)[],
    _options?: { checkNullable?: boolean },
  ): boolean {
    const toExpr = this.$to;
    if (!toExpr) return false;
    if (toExpr instanceof DataTypeExpr) {
      return toExpr.isType(dtypes);
    }
    return false;
  }

  static {
    this.register();
  }
}

export type JustifyDaysExprArgs = Merge<[
  FuncExprArgs,
]>;

export class JustifyDaysExpr extends FuncExpr {
  key = ExpressionKey.JUSTIFY_DAYS;

  static argTypes: RequiredMap<JustifyDaysExprArgs> = {
    ...super.argTypes,
  };

  declare args: JustifyDaysExprArgs;

  constructor (args: JustifyDaysExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JustifyHoursExprArgs = Merge<[
  FuncExprArgs,
]>;

export class JustifyHoursExpr extends FuncExpr {
  key = ExpressionKey.JUSTIFY_HOURS;

  static argTypes: RequiredMap<JustifyHoursExprArgs> = {
    ...super.argTypes,
  };

  declare args: JustifyHoursExprArgs;

  constructor (args: JustifyHoursExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JustifyIntervalExprArgs = Merge<[
  FuncExprArgs,
]>;

export class JustifyIntervalExpr extends FuncExpr {
  key = ExpressionKey.JUSTIFY_INTERVAL;

  static argTypes: RequiredMap<JustifyIntervalExprArgs> = {
    ...super.argTypes,
  };

  declare args: JustifyIntervalExprArgs;

  constructor (args: JustifyIntervalExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TryExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TryExpr extends FuncExpr {
  key = ExpressionKey.TRY;

  static argTypes: RequiredMap<TryExprArgs> = {
    ...super.argTypes,
  };

  declare args: TryExprArgs;

  constructor (args: TryExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CastToStrTypeExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    to: DataTypeExpr;
  },
]>;

export class CastToStrTypeExpr extends FuncExpr {
  key = ExpressionKey.CAST_TO_STR_TYPE;

  static argTypes: RequiredMap<CastToStrTypeExprArgs> = {
    ...super.argTypes,
    this: true,
    to: true,
  };

  declare args: CastToStrTypeExprArgs;

  constructor (args: CastToStrTypeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $to (): DataTypeExpr {
    return this.args.to;
  }

  static {
    this.register();
  }
}

export type CheckJsonExprArgs = Merge<[
  FuncExprArgs,
  { this: Expression },
]>;

export class CheckJsonExpr extends FuncExpr {
  key = ExpressionKey.CHECK_JSON;

  static argTypes: RequiredMap<CheckJsonExprArgs> = {
    ...super.argTypes,
    this: true,
  };

  declare args: CheckJsonExprArgs;

  constructor (args: CheckJsonExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type CheckXmlExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    disableAutoConvert?: Expression;
  },
]>;

export class CheckXmlExpr extends FuncExpr {
  key = ExpressionKey.CHECK_XML;

  static argTypes: RequiredMap<CheckXmlExprArgs> = {
    ...super.argTypes,
    this: true,
    disableAutoConvert: false,
  };

  declare args: CheckXmlExprArgs;

  constructor (args: CheckXmlExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $disableAutoConvert (): Expression | undefined {
    return this.args.disableAutoConvert;
  }

  static {
    this.register();
  }
}

export type CollateExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
]>;

export class CollateExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.COLLATE;

  static argTypes: RequiredMap<CollateExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: CollateExprArgs;

  constructor (args: CollateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CollationExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CollationExpr extends FuncExpr {
  key = ExpressionKey.COLLATION;

  static argTypes: RequiredMap<CollationExprArgs> = {
    ...super.argTypes,
  };

  declare args: CollationExprArgs;

  constructor (args: CollationExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CeilExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    decimals?: Expression;
    to?: Expression;
  },
]>;

export class CeilExpr extends FuncExpr {
  key = ExpressionKey.CEIL;

  static _sqlNames = ['CEIL', 'CEILING'];

  static argTypes: RequiredMap<CeilExprArgs> = {
    ...super.argTypes,
    this: true,
    decimals: false,
    to: false,
  };

  declare args: CeilExprArgs;

  // Auto-register this class when the module loads
  static {
    this.register();
  }

  constructor (args: CeilExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $decimals (): Expression | undefined {
    return this.args.decimals;
  }

  get $to (): Expression | undefined {
    return this.args.to;
  }
}

export type CoalesceExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
    isNvl?: boolean;
    isNull?: boolean;
  },
]>;

export class CoalesceExpr extends FuncExpr {
  key = ExpressionKey.COALESCE;

  static _sqlNames = [
    'COALESCE',
    'IFNULL',
    'NVL',
  ];

  static isVarLenArgs = true;

  static argTypes: RequiredMap<CoalesceExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
    isNvl: false,
    isNull: false,
  };

  declare args: CoalesceExprArgs;

  constructor (args: CoalesceExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $isNvl (): boolean | undefined {
    return this.args.isNvl;
  }

  get $isNull (): boolean | undefined {
    return this.args.isNull;
  }

  static {
    this.register();
  }
}

export type ChrExprArgs = Merge<[
  FuncExprArgs,
  {
    expressions: Expression[];
    charset?: string;
  },
]>;

export class ChrExpr extends FuncExpr {
  key = ExpressionKey.CHR;

  static _sqlNames = ['CHR', 'CHAR'];

  static isVarLenArgs = true;

  static argTypes: RequiredMap<ChrExprArgs> = {
    ...super.argTypes,
    expressions: true,
    charset: false,
  };

  declare args: ChrExprArgs;

  constructor (args: ChrExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $charset (): string | undefined {
    return this.args.charset;
  }

  static {
    this.register();
  }
}

export type ConcatExprArgs = Merge<[
  FuncExprArgs,
  {
    expressions: Expression[];
    safe?: boolean;
    coalesce?: boolean;
  },
]>;

export class ConcatExpr extends FuncExpr {
  key = ExpressionKey.CONCAT;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<ConcatExprArgs> = {
    ...super.argTypes,
    expressions: true,
    safe: false,
    coalesce: false,
  };

  declare args: ConcatExprArgs;

  constructor (args: ConcatExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }

  get $coalesce (): boolean | undefined {
    return this.args.coalesce;
  }

  static {
    this.register();
  }
}

export type ContainsExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    jsonScope?: Expression;
  },
]>;

export class ContainsExpr extends FuncExpr {
  key = ExpressionKey.CONTAINS;

  static argTypes: RequiredMap<ContainsExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    jsonScope: false,
  };

  declare args: ContainsExprArgs;

  constructor (args: ContainsExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $jsonScope (): Expression | undefined {
    return this.args.jsonScope;
  }

  static {
    this.register();
  }
}

export type ConnectByRootExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ConnectByRootExpr extends FuncExpr {
  key = ExpressionKey.CONNECT_BY_ROOT;

  static argTypes: RequiredMap<ConnectByRootExprArgs> = {
    ...super.argTypes,
  };

  declare args: ConnectByRootExprArgs;

  constructor (args: ConnectByRootExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CbrtExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CbrtExpr extends FuncExpr {
  key = ExpressionKey.CBRT;

  static argTypes: RequiredMap<CbrtExprArgs> = {
    ...super.argTypes,
  };

  declare args: CbrtExprArgs;

  constructor (args: CbrtExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentAccountExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentAccountExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ACCOUNT;

  static argTypes: RequiredMap<CurrentAccountExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentAccountExprArgs;

  constructor (args: CurrentAccountExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentAccountNameExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentAccountNameExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ACCOUNT_NAME;

  static argTypes: RequiredMap<CurrentAccountNameExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentAccountNameExprArgs;

  constructor (args: CurrentAccountNameExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentAvailableRolesExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentAvailableRolesExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_AVAILABLE_ROLES;

  static argTypes: RequiredMap<CurrentAvailableRolesExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentAvailableRolesExprArgs;

  constructor (args: CurrentAvailableRolesExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentClientExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentClientExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_CLIENT;

  static argTypes: RequiredMap<CurrentClientExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentClientExprArgs;

  constructor (args: CurrentClientExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentIpAddressExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentIpAddressExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_IP_ADDRESS;

  static argTypes: RequiredMap<CurrentIpAddressExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentIpAddressExprArgs;

  constructor (args: CurrentIpAddressExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentDatabaseExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentDatabaseExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_DATABASE;

  static argTypes: RequiredMap<CurrentDatabaseExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentDatabaseExprArgs;

  constructor (args: CurrentDatabaseExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentSchemasExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class CurrentSchemasExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_SCHEMAS;

  static argTypes: RequiredMap<CurrentSchemasExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: CurrentSchemasExprArgs;

  constructor (args: CurrentSchemasExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type CurrentSecondaryRolesExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentSecondaryRolesExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_SECONDARY_ROLES;

  static argTypes: RequiredMap<CurrentSecondaryRolesExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentSecondaryRolesExprArgs;

  constructor (args: CurrentSecondaryRolesExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentSessionExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentSessionExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_SESSION;

  static argTypes: RequiredMap<CurrentSessionExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentSessionExprArgs;

  constructor (args: CurrentSessionExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentStatementExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentStatementExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_STATEMENT;

  static argTypes: RequiredMap<CurrentStatementExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentStatementExprArgs;

  constructor (args: CurrentStatementExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentVersionExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentVersionExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_VERSION;

  static argTypes: RequiredMap<CurrentVersionExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentVersionExprArgs;

  constructor (args: CurrentVersionExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentTransactionExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentTransactionExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_TRANSACTION;

  static argTypes: RequiredMap<CurrentTransactionExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentTransactionExprArgs;

  constructor (args: CurrentTransactionExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentWarehouseExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentWarehouseExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_WAREHOUSE;

  static argTypes: RequiredMap<CurrentWarehouseExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentWarehouseExprArgs;

  constructor (args: CurrentWarehouseExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentDateExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class CurrentDateExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_DATE;

  static argTypes: RequiredMap<CurrentDateExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: CurrentDateExprArgs;

  constructor (args: CurrentDateExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type CurrentDatetimeExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class CurrentDatetimeExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_DATETIME;

  static argTypes: RequiredMap<CurrentDatetimeExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: CurrentDatetimeExprArgs;

  constructor (args: CurrentDatetimeExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type CurrentTimeExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class CurrentTimeExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_TIME;

  static argTypes: RequiredMap<CurrentTimeExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: CurrentTimeExprArgs;

  constructor (args: CurrentTimeExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type LocaltimeExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class LocaltimeExpr extends FuncExpr {
  key = ExpressionKey.LOCALTIME;

  static argTypes: RequiredMap<LocaltimeExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: LocaltimeExprArgs;

  constructor (args: LocaltimeExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type LocaltimestampExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class LocaltimestampExpr extends FuncExpr {
  key = ExpressionKey.LOCALTIMESTAMP;

  static argTypes: RequiredMap<LocaltimestampExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: LocaltimestampExprArgs;

  constructor (args: LocaltimestampExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type SystimestampExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class SystimestampExpr extends FuncExpr {
  key = ExpressionKey.SYSTIMESTAMP;

  static argTypes: RequiredMap<SystimestampExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: SystimestampExprArgs;

  constructor (args: SystimestampExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type CurrentTimestampExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    sysdate?: Expression;
  },
]>;

export class CurrentTimestampExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_TIMESTAMP;

  static argTypes: RequiredMap<CurrentTimestampExprArgs> = {
    ...super.argTypes,
    this: false,
    sysdate: false,
  };

  declare args: CurrentTimestampExprArgs;

  constructor (args: CurrentTimestampExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $sysdate (): Expression | undefined {
    return this.args.sysdate;
  }

  static {
    this.register();
  }
}

export type CurrentTimestampLTZExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentTimestampLTZExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_TIMESTAMP_LTZ;

  static argTypes: RequiredMap<CurrentTimestampLTZExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentTimestampLTZExprArgs;

  constructor (args: CurrentTimestampLTZExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentTimezoneExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentTimezoneExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_TIMEZONE;

  static argTypes: RequiredMap<CurrentTimezoneExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentTimezoneExprArgs;

  constructor (args: CurrentTimezoneExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentOrganizationNameExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentOrganizationNameExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ORGANIZATION_NAME;

  static argTypes: RequiredMap<CurrentOrganizationNameExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentOrganizationNameExprArgs;

  constructor (args: CurrentOrganizationNameExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentSchemaExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class CurrentSchemaExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_SCHEMA;

  static argTypes: RequiredMap<CurrentSchemaExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: CurrentSchemaExprArgs;

  constructor (args: CurrentSchemaExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type CurrentUserExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class CurrentUserExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_USER;

  static argTypes: RequiredMap<CurrentUserExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: CurrentUserExprArgs;

  constructor (args: CurrentUserExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type CurrentCatalogExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentCatalogExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_CATALOG;

  static argTypes: RequiredMap<CurrentCatalogExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentCatalogExprArgs;

  constructor (args: CurrentCatalogExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentRegionExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentRegionExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_REGION;

  static argTypes: RequiredMap<CurrentRegionExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentRegionExprArgs;

  constructor (args: CurrentRegionExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentRoleExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentRoleExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ROLE;

  static argTypes: RequiredMap<CurrentRoleExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentRoleExprArgs;

  constructor (args: CurrentRoleExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentRoleTypeExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentRoleTypeExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ROLE_TYPE;

  static argTypes: RequiredMap<CurrentRoleTypeExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentRoleTypeExprArgs;

  constructor (args: CurrentRoleTypeExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentOrganizationUserExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentOrganizationUserExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ORGANIZATION_USER;

  static argTypes: RequiredMap<CurrentOrganizationUserExprArgs> = {
    ...super.argTypes,
  };

  declare args: CurrentOrganizationUserExprArgs;

  constructor (args: CurrentOrganizationUserExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SessionUserExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SessionUserExpr extends FuncExpr {
  key = ExpressionKey.SESSION_USER;

  static argTypes: RequiredMap<SessionUserExprArgs> = {
    ...super.argTypes,
  };

  declare args: SessionUserExprArgs;

  constructor (args: SessionUserExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UtcDateExprArgs = Merge<[
  FuncExprArgs,
]>;

export class UtcDateExpr extends FuncExpr {
  key = ExpressionKey.UTC_DATE;

  static argTypes: RequiredMap<UtcDateExprArgs> = {
    ...super.argTypes,
  };

  declare args: UtcDateExprArgs;

  constructor (args: UtcDateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UtcTimeExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class UtcTimeExpr extends FuncExpr {
  key = ExpressionKey.UTC_TIME;

  static argTypes: RequiredMap<UtcTimeExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: UtcTimeExprArgs;

  constructor (args: UtcTimeExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type UtcTimestampExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class UtcTimestampExpr extends FuncExpr {
  key = ExpressionKey.UTC_TIMESTAMP;

  static argTypes: RequiredMap<UtcTimestampExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: UtcTimestampExprArgs;

  constructor (args: UtcTimestampExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type DateAddExprArgs = Merge<[
  FuncExprArgs,
  IntervalOpExprArgs,
  {
    this: Expression;
    expression: Expression;
    unit?: Expression;
  },
]>;

export class DateAddExpr extends multiInherit(FuncExpr, IntervalOpExpr) {
  key = ExpressionKey.DATE_ADD;

  static argTypes: RequiredMap<DateAddExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  };

  declare args: DateAddExprArgs;

  constructor (args: DateAddExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  static {
    this.register();
  }
}

export type DateBinExprArgs = Merge<[
  FuncExprArgs,
  IntervalOpExprArgs,
  {
    this: Expression;
    expression: Expression;
    unit?: Expression;
    zone?: Expression;
    origin?: Expression;
  },
]>;

export class DateBinExpr extends multiInherit(FuncExpr, IntervalOpExpr) {
  key = ExpressionKey.DATE_BIN;

  static argTypes: RequiredMap<DateBinExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
    zone: false,
    origin: false,
  };

  declare args: DateBinExprArgs;

  constructor (args: DateBinExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  get $origin (): Expression | undefined {
    return this.args.origin;
  }

  static {
    this.register();
  }
}

export type DateSubExprArgs = Merge<[
  FuncExprArgs,
  IntervalOpExprArgs,
  {
    this: Expression;
    expression: Expression;
    unit?: Expression;
  },
]>;

export class DateSubExpr extends multiInherit(FuncExpr, IntervalOpExpr) {
  key = ExpressionKey.DATE_SUB;

  static argTypes: RequiredMap<DateSubExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  };

  declare args: DateSubExprArgs;

  constructor (args: DateSubExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  static {
    this.register();
  }
}

export type DateDiffExprArgs = Merge<[
  FuncExprArgs,
  TimeUnitExprArgs,
  {
    this: Expression;
    expression: Expression;
    unit?: Expression;
    zone?: Expression;
    bigInt?: Expression;
    datePartBoundary?: Expression;
  },
]>;

export class DateDiffExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.DATE_DIFF;

  static _sqlNames = ['DATEDIFF', 'DATE_DIFF'];

  static argTypes: RequiredMap<DateDiffExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
    zone: false,
    bigInt: false,
    datePartBoundary: false,
  };

  declare args: DateDiffExprArgs;

  constructor (args: DateDiffExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  get $bigInt (): Expression | undefined {
    return this.args.bigInt;
  }

  get $datePartBoundary (): Expression | undefined {
    return this.args.datePartBoundary;
  }

  static {
    this.register();
  }
}

export type DateTruncExprArgs = Merge<[
  FuncExprArgs,
  {
    unit: Expression;
    this: Expression;
    zone?: Expression;
    inputTypePreserved?: DataTypeExpr;
    unabbreviate?: boolean;
  },
]>;

export class DateTruncExpr extends FuncExpr {
  key = ExpressionKey.DATE_TRUNC;

  static argTypes: RequiredMap<DateTruncExprArgs> = {
    ...super.argTypes,
    unit: true,
    this: true,
    zone: false,
    inputTypePreserved: false,
  };

  declare args: DateTruncExprArgs;

  constructor (args: DateTruncExprArgs) {
    const unabbreviate = args.unabbreviate ?? true;
    const unit = args.unit;

    if (
      TimeUnitExpr.isVarLike(unit)
      && !(unit instanceof ColumnExpr && unit.parts.length !== 1)
    ) {
      let unitName = unit.name.toUpperCase();
      if (unabbreviate && unitName in TimeUnitExpr.UNABBREVIATED_UNIT_NAME) {
        unitName = TimeUnitExpr.UNABBREVIATED_UNIT_NAME[unitName];
      }
      args.unit = LiteralExpr.string(unitName);
    }

    delete args.unabbreviate;
    super(args);
  }

  get $unit (): Expression {
    return this.args.unit;
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  get $inputTypePreserved (): DataTypeExpr | undefined {
    return this.args.inputTypePreserved;
  }

  get unit (): Expression {
    return this.$unit;
  }

  static {
    this.register();
  }
}

export type DatetimeExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class DatetimeExpr extends FuncExpr {
  key = ExpressionKey.DATETIME;

  static argTypes: RequiredMap<DatetimeExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: DatetimeExprArgs;

  constructor (args: DatetimeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type DatetimeAddExprArgs = Merge<[
  FuncExprArgs,
  IntervalOpExprArgs,
  {
    this: Expression;
    expression: Expression;
    unit?: Expression;
  },
]>;

export class DatetimeAddExpr extends multiInherit(FuncExpr, IntervalOpExpr) {
  key = ExpressionKey.DATETIME_ADD;

  static argTypes: RequiredMap<DatetimeAddExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  };

  declare args: DatetimeAddExprArgs;

  constructor (args: DatetimeAddExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  static {
    this.register();
  }
}

export type DatetimeSubExprArgs = Merge<[
  FuncExprArgs,
  IntervalOpExprArgs,
  {
    this: Expression;
    expression: Expression;
    unit?: Expression;
  },
]>;

export class DatetimeSubExpr extends multiInherit(FuncExpr, IntervalOpExpr) {
  key = ExpressionKey.DATETIME_SUB;

  static argTypes: RequiredMap<DatetimeSubExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  };

  declare args: DatetimeSubExprArgs;

  constructor (args: DatetimeSubExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  static {
    this.register();
  }
}

export type DatetimeDiffExprArgs = Merge<[
  FuncExprArgs,
  TimeUnitExprArgs,
  {
    this: Expression;
    expression: Expression;
    unit?: Expression;
  },
]>;

export class DatetimeDiffExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.DATETIME_DIFF;

  static argTypes: RequiredMap<DatetimeDiffExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  };

  declare args: DatetimeDiffExprArgs;

  constructor (args: DatetimeDiffExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  static {
    this.register();
  }
}

export type DatetimeTruncExprArgs = Merge<[
  FuncExprArgs,
  TimeUnitExprArgs,
  {
    this: Expression;
    unit: Expression;
    zone?: Expression;
  },
]>;

export class DatetimeTruncExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.DATETIME_TRUNC;

  static argTypes: RequiredMap<DatetimeTruncExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    unit: true,
    zone: false,
  };

  declare args: DatetimeTruncExprArgs;

  constructor (args: DatetimeTruncExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $unit (): Expression {
    return this.args.unit;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  static {
    this.register();
  }
}

export type DateFromUnixDateExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DateFromUnixDateExpr extends FuncExpr {
  key = ExpressionKey.DATE_FROM_UNIX_DATE;

  static argTypes: RequiredMap<DateFromUnixDateExprArgs> = {
    ...super.argTypes,
  };

  declare args: DateFromUnixDateExprArgs;

  constructor (args: DateFromUnixDateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DayOfWeekExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DayOfWeekExpr extends FuncExpr {
  key = ExpressionKey.DAY_OF_WEEK;

  static argTypes: RequiredMap<DayOfWeekExprArgs> = {
    ...super.argTypes,
  };

  declare args: DayOfWeekExprArgs;

  constructor (args: DayOfWeekExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DayOfWeekIsoExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DayOfWeekIsoExpr extends FuncExpr {
  key = ExpressionKey.DAY_OF_WEEK_ISO;

  static argTypes: RequiredMap<DayOfWeekIsoExprArgs> = {
    ...super.argTypes,
  };

  declare args: DayOfWeekIsoExprArgs;

  constructor (args: DayOfWeekIsoExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DayOfMonthExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DayOfMonthExpr extends FuncExpr {
  key = ExpressionKey.DAY_OF_MONTH;

  static argTypes: RequiredMap<DayOfMonthExprArgs> = {
    ...super.argTypes,
  };

  declare args: DayOfMonthExprArgs;

  constructor (args: DayOfMonthExprArgs) {
    super(args);
  }

  static _sqlNames = ['DAY_OF_MONTH', 'DAYOFMONTH'];

  static {
    this.register();
  }
}

export type DayOfYearExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DayOfYearExpr extends FuncExpr {
  key = ExpressionKey.DAY_OF_YEAR;

  static argTypes: RequiredMap<DayOfYearExprArgs> = {
    ...super.argTypes,
  };

  declare args: DayOfYearExprArgs;

  constructor (args: DayOfYearExprArgs) {
    super(args);
  }

  static _sqlNames = ['DAY_OF_YEAR', 'DAYOFYEAR'];

  static {
    this.register();
  }
}

export type DaynameExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    abbreviated?: Expression;
  },
]>;

export class DaynameExpr extends FuncExpr {
  key = ExpressionKey.DAYNAME;

  static argTypes: RequiredMap<DaynameExprArgs> = {
    ...super.argTypes,
    this: true,
    abbreviated: false,
  };

  declare args: DaynameExprArgs;

  constructor (args: DaynameExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $abbreviated (): Expression | undefined {
    return this.args.abbreviated;
  }

  static {
    this.register();
  }
}

export type ToDaysExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ToDaysExpr extends FuncExpr {
  key = ExpressionKey.TO_DAYS;

  static argTypes: RequiredMap<ToDaysExprArgs> = {
    ...super.argTypes,
  };

  declare args: ToDaysExprArgs;

  constructor (args: ToDaysExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type WeekOfYearExprArgs = Merge<[
  FuncExprArgs,
]>;

export class WeekOfYearExpr extends FuncExpr {
  key = ExpressionKey.WEEK_OF_YEAR;

  static argTypes: RequiredMap<WeekOfYearExprArgs> = {
    ...super.argTypes,
  };

  declare args: WeekOfYearExprArgs;

  constructor (args: WeekOfYearExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type YearOfWeekExprArgs = Merge<[
  FuncExprArgs,
]>;

export class YearOfWeekExpr extends FuncExpr {
  key = ExpressionKey.YEAR_OF_WEEK;

  static argTypes: RequiredMap<YearOfWeekExprArgs> = {
    ...super.argTypes,
  };

  declare args: YearOfWeekExprArgs;

  constructor (args: YearOfWeekExprArgs) {
    super(args);
  }

  static _sqlNames = ['YEAR_OF_WEEK', 'YEAROFWEEK'];

  static {
    this.register();
  }
}

export type YearOfWeekIsoExprArgs = Merge<[
  FuncExprArgs,
]>;

export class YearOfWeekIsoExpr extends FuncExpr {
  key = ExpressionKey.YEAR_OF_WEEK_ISO;

  static argTypes: RequiredMap<YearOfWeekIsoExprArgs> = {
    ...super.argTypes,
  };

  declare args: YearOfWeekIsoExprArgs;

  constructor (args: YearOfWeekIsoExprArgs) {
    super(args);
  }

  static _sqlNames = ['YEAR_OF_WEEK_ISO', 'YEAROFWEEKISO'];

  static {
    this.register();
  }
}

export type MonthsBetweenExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    roundoff?: Expression;
  },
]>;

export class MonthsBetweenExpr extends FuncExpr {
  key = ExpressionKey.MONTHS_BETWEEN;

  static argTypes: RequiredMap<MonthsBetweenExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    roundoff: false,
  };

  declare args: MonthsBetweenExprArgs;

  static {
    this.register();
  }

  constructor (args: MonthsBetweenExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $roundoff (): Expression | undefined {
    return this.args.roundoff;
  }
}

export type MakeIntervalExprArgs = Merge<[
  FuncExprArgs,
  {
    year?: Expression;
    month?: Expression;
    week?: Expression;
    day?: Expression;
    hour?: Expression;
    minute?: Expression;
    second?: Expression;
  },
]>;

export class MakeIntervalExpr extends FuncExpr {
  key = ExpressionKey.MAKE_INTERVAL;

  /**
   * Defines the arguments (properties and child expressions) for MakeInterval expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<MakeIntervalExprArgs> = {
    ...super.argTypes,
    year: false,
    month: false,
    week: false,
    day: false,
    hour: false,
    minute: false,
    second: false,
  };

  declare args: MakeIntervalExprArgs;

  constructor (args: MakeIntervalExprArgs) {
    super(args);
  }

  get $year (): Expression | undefined {
    return this.args.year;
  }

  get $month (): Expression | undefined {
    return this.args.month;
  }

  get $week (): Expression | undefined {
    return this.args.week;
  }

  get $day (): Expression | undefined {
    return this.args.day;
  }

  get $hour (): Expression | undefined {
    return this.args.hour;
  }

  get $minute (): Expression | undefined {
    return this.args.minute;
  }

  get $second (): Expression | undefined {
    return this.args.second;
  }

  static {
    this.register();
  }
}

export type LastDayExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    unit?: Expression;
  },
]>;

export class LastDayExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.LAST_DAY;

  static argTypes: RequiredMap<LastDayExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    unit: false,
  };

  declare args: LastDayExprArgs;

  constructor (args: LastDayExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  static {
    this.register();
  }
}

export type PreviousDayExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class PreviousDayExpr extends FuncExpr {
  key = ExpressionKey.PREVIOUS_DAY;

  static argTypes: RequiredMap<PreviousDayExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: PreviousDayExprArgs;

  constructor (args: PreviousDayExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type LaxBoolExprArgs = Merge<[
  FuncExprArgs,
]>;

export class LaxBoolExpr extends FuncExpr {
  key = ExpressionKey.LAX_BOOL;

  static argTypes: RequiredMap<LaxBoolExprArgs> = {
    ...super.argTypes,
  };

  declare args: LaxBoolExprArgs;

  constructor (args: LaxBoolExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LaxFloat64ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class LaxFloat64Expr extends FuncExpr {
  key = ExpressionKey.LAX_FLOAT64;

  static argTypes: RequiredMap<LaxFloat64ExprArgs> = {
    ...super.argTypes,
  };

  declare args: LaxFloat64ExprArgs;

  constructor (args: LaxFloat64ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LaxInt64ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class LaxInt64Expr extends FuncExpr {
  key = ExpressionKey.LAX_INT64;

  static argTypes: RequiredMap<LaxInt64ExprArgs> = {
    ...super.argTypes,
  };

  declare args: LaxInt64ExprArgs;

  constructor (args: LaxInt64ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LaxStringExprArgs = Merge<[
  FuncExprArgs,
]>;

export class LaxStringExpr extends FuncExpr {
  key = ExpressionKey.LAX_STRING;

  static argTypes: RequiredMap<LaxStringExprArgs> = {
    ...super.argTypes,
  };

  declare args: LaxStringExprArgs;

  constructor (args: LaxStringExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ExtractExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class ExtractExpr extends FuncExpr {
  key = ExpressionKey.EXTRACT;

  static argTypes: RequiredMap<ExtractExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: ExtractExprArgs;

  constructor (args: ExtractExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ExistsExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;
export class ExistsExpr extends multiInherit(FuncExpr, SubqueryPredicateExpr) {
  key = ExpressionKey.EXISTS;

  static argTypes: RequiredMap<ExistsExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: ExistsExprArgs;

  constructor (args: ExistsExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type EltExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions: Expression[];
  },
]>;

export class EltExpr extends FuncExpr {
  key = ExpressionKey.ELT;

  static isVarLenArgs = true;
  static argTypes: RequiredMap<EltExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
  };

  declare args: EltExprArgs;

  constructor (args: EltExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type TimestampExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    zone?: Expression;
    withTz?: Expression;
  },
]>;

export class TimestampExpr extends FuncExpr {
  key = ExpressionKey.TIMESTAMP;

  /**
   * Defines the arguments (properties and child expressions) for Timestamp expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<TimestampExprArgs> = {
    ...super.argTypes,
    this: false,
    zone: false,
    withTz: false,
  };

  declare args: TimestampExprArgs;

  constructor (args: TimestampExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  get $withTz (): Expression | undefined {
    return this.args.withTz;
  }

  static {
    this.register();
  }
}

export type TimestampAddExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    unit?: Expression;
  },
]>;

export class TimestampAddExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIMESTAMP_ADD;

  static argTypes: RequiredMap<TimestampAddExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  };

  declare args: TimestampAddExprArgs;

  constructor (args: TimestampAddExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  static {
    this.register();
  }
}

export type TimestampSubExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    unit?: Expression;
  },
]>;

export class TimestampSubExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIMESTAMP_SUB;

  static argTypes: RequiredMap<TimestampSubExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  };

  declare args: TimestampSubExprArgs;

  constructor (args: TimestampSubExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  static {
    this.register();
  }
}

export type TimestampDiffExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    unit?: Expression;
  },
]>;

export class TimestampDiffExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIMESTAMP_DIFF;

  static argTypes: RequiredMap<TimestampDiffExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  };

  declare args: TimestampDiffExprArgs;

  constructor (args: TimestampDiffExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  static {
    this.register();
  }
}

export type TimestampTruncExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    unit: Expression;
    zone?: Expression;
    inputTypePreserved?: DataTypeExpr;
  },
]>;

export class TimestampTruncExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIMESTAMP_TRUNC;

  /**
   * Defines the arguments (properties and child expressions) for TimestampTrunc expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<TimestampTruncExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    unit: true,
    zone: false,
    inputTypePreserved: false,
  };

  declare args: TimestampTruncExprArgs;

  constructor (args: TimestampTruncExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $unit (): Expression {
    return this.args.unit;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  get $inputTypePreserved (): DataTypeExpr | undefined {
    return this.args.inputTypePreserved;
  }

  static {
    this.register();
  }
}

/**
 * Valid kind values for time slice expressions
 */
export enum TimeSliceExprKind {
  START = 'START',
  END = 'END',
}
export type TimeSliceExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    unit: Expression;
    kind?: TimeSliceExprKind;
  },
]>;

export class TimeSliceExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIME_SLICE;

  /**
   * Defines the arguments (properties and child expressions) for TimeSlice expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<TimeSliceExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: true,
    kind: false,
  };

  declare args: TimeSliceExprArgs;

  constructor (args: TimeSliceExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $unit (): Expression {
    return this.args.unit;
  }

  get $kind (): TimeSliceExprKind | undefined {
    return this.args.kind;
  }

  static {
    this.register();
  }
}

export type TimeAddExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    unit?: Expression;
  },
]>;

export class TimeAddExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIME_ADD;

  static argTypes: RequiredMap<TimeAddExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  };

  declare args: TimeAddExprArgs;

  constructor (args: TimeAddExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  static {
    this.register();
  }
}

export type TimeSubExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    unit?: Expression;
  },
]>;

export class TimeSubExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIME_SUB;

  static argTypes: RequiredMap<TimeSubExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  };

  declare args: TimeSubExprArgs;

  constructor (args: TimeSubExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  static {
    this.register();
  }
}

export type TimeDiffExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    unit?: Expression;
  },
]>;

export class TimeDiffExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIME_DIFF;

  static argTypes: RequiredMap<TimeDiffExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  };

  declare args: TimeDiffExprArgs;

  constructor (args: TimeDiffExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  static {
    this.register();
  }
}

export type TimeTruncExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    unit: Expression;
    zone?: Expression;
  },
]>;

export class TimeTruncExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIME_TRUNC;

  /**
   * Defines the arguments (properties and child expressions) for TimeTrunc expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<TimeTruncExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    unit: true,
    zone: false,
  };

  declare args: TimeTruncExprArgs;

  constructor (args: TimeTruncExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $unit (): Expression {
    return this.args.unit;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  static {
    this.register();
  }
}

export type DateFromPartsExprArgs = Merge<[
  FuncExprArgs,
  {
    year: Expression;
    month?: Expression;
    day?: Expression;
    allowOverflow?: Expression;
  },
]>;

export class DateFromPartsExpr extends FuncExpr {
  key = ExpressionKey.DATE_FROM_PARTS;

  /**
   * Defines the arguments (properties and child expressions) for DateFromParts expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<DateFromPartsExprArgs> = {
    ...super.argTypes,
    year: true,
    month: false,
    day: false,
    allowOverflow: false,
  };

  declare args: DateFromPartsExprArgs;

  constructor (args: DateFromPartsExprArgs) {
    super(args);
  }

  get $year (): Expression {
    return this.args.year;
  }

  get $month (): Expression | undefined {
    return this.args.month;
  }

  get $day (): Expression | undefined {
    return this.args.day;
  }

  get $allowOverflow (): Expression | undefined {
    return this.args.allowOverflow;
  }

  static {
    this.register();
  }
}

export type TimeFromPartsExprArgs = Merge<[
  FuncExprArgs,
  {
    hour: Expression;
    min: Expression;
    sec: Expression;
    nano?: Expression;
    fractions?: Expression[];
    precision?: number | Expression;
    overflow?: Expression;
  },
]>;

export class TimeFromPartsExpr extends FuncExpr {
  key = ExpressionKey.TIME_FROM_PARTS;

  /**
   * Defines the arguments (properties and child expressions) for TimeFromParts expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<TimeFromPartsExprArgs> = {
    ...super.argTypes,
    hour: true,
    min: true,
    sec: true,
    nano: false,
    fractions: false,
    precision: false,
    overflow: false,
  };

  declare args: TimeFromPartsExprArgs;

  constructor (args: TimeFromPartsExprArgs) {
    super(args);
  }

  get $hour (): Expression {
    return this.args.hour;
  }

  get $min (): Expression {
    return this.args.min;
  }

  get $sec (): Expression {
    return this.args.sec;
  }

  get $nano (): Expression | undefined {
    return this.args.nano;
  }

  get $fractions (): Expression[] | undefined {
    return this.args.fractions;
  }

  get $precision (): number | Expression | undefined {
    return this.args.precision;
  }

  get $overflow (): Expression | undefined {
    return this.args.overflow;
  }

  static {
    this.register();
  }
}

export type DateStrToDateExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DateStrToDateExpr extends FuncExpr {
  key = ExpressionKey.DATE_STR_TO_DATE;

  static argTypes: RequiredMap<DateStrToDateExprArgs> = {
    ...super.argTypes,
  };

  declare args: DateStrToDateExprArgs;

  constructor (args: DateStrToDateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateToDateStrExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DateToDateStrExpr extends FuncExpr {
  key = ExpressionKey.DATE_TO_DATE_STR;

  static argTypes: RequiredMap<DateToDateStrExprArgs> = {
    ...super.argTypes,
  };

  declare args: DateToDateStrExprArgs;

  constructor (args: DateToDateStrExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateToDiExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DateToDiExpr extends FuncExpr {
  key = ExpressionKey.DATE_TO_DI;

  static argTypes: RequiredMap<DateToDiExprArgs> = {
    ...super.argTypes,
  };

  declare args: DateToDiExprArgs;

  constructor (args: DateToDiExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
    zone?: Expression;
  },
]>;

export class DateExpr extends FuncExpr {
  key = ExpressionKey.DATE;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<DateExprArgs> = {
    ...super.argTypes,
    this: false,
    expressions: false,
    zone: false,
  };

  declare args: DateExprArgs;

  constructor (args: DateExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  static {
    this.register();
  }
}

export type DayExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DayExpr extends FuncExpr {
  key = ExpressionKey.DAY;

  static argTypes: RequiredMap<DayExprArgs> = {
    ...super.argTypes,
  };

  declare args: DayExprArgs;

  constructor (args: DayExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DecodeExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    charset: string;
    replace?: boolean;
  },
]>;

export class DecodeExpr extends FuncExpr {
  key = ExpressionKey.DECODE;

  /**
   * Defines the arguments (properties and child expressions) for Decode expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<DecodeExprArgs> = {
    ...super.argTypes,
    this: true,
    charset: true,
    replace: false,
  };

  declare args: DecodeExprArgs;

  constructor (args: DecodeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $charset (): string {
    return this.args.charset;
  }

  get $replace (): boolean | undefined | undefined {
    return this.args.replace;
  }

  static {
    this.register();
  }
}

export type DecodeCaseExprArgs = Merge<[
  FuncExprArgs,
  { expressions: Expression[] },
]>;

export class DecodeCaseExpr extends FuncExpr {
  key = ExpressionKey.DECODE_CASE;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<DecodeCaseExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: DecodeCaseExprArgs;

  constructor (args: DecodeCaseExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type DecryptExprArgs = Merge<[
  FuncExprArgs,
  {
    passphrase: Expression;
    aad?: Expression;
    encryptionMethod?: string;
    safe?: boolean;
  },
]>;

export class DecryptExpr extends FuncExpr {
  key = ExpressionKey.DECRYPT;

  /**
   * Defines the arguments (properties and child expressions) for Decrypt expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<DecryptExprArgs> = {
    ...super.argTypes,
    passphrase: true,
    aad: false,
    encryptionMethod: false,
    safe: false,
  };

  declare args: DecryptExprArgs;

  constructor (args: DecryptExprArgs) {
    super(args);
  }

  get $passphrase (): Expression {
    return this.args.passphrase;
  }

  get $aad (): Expression | undefined {
    return this.args.aad;
  }

  get $encryptionMethod (): string | undefined {
    return this.args.encryptionMethod;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }

  static {
    this.register();
  }
}

export type DecryptRawExprArgs = Merge<[
  FuncExprArgs,
  {
    key: unknown;
    iv: Expression;
    aad?: Expression;
    encryptionMethod?: string;
    aead?: Expression;
    safe?: boolean;
  },
]>;

export class DecryptRawExpr extends FuncExpr {
  key = ExpressionKey.DECRYPT_RAW;

  /**
   * Defines the arguments (properties and child expressions) for DecryptRaw expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<DecryptRawExprArgs> = {
    ...super.argTypes,
    key: true,
    iv: true,
    aad: false,
    encryptionMethod: false,
    aead: false,
    safe: false,
  };

  declare args: DecryptRawExprArgs;

  constructor (args: DecryptRawExprArgs) {
    super(args);
  }

  get $key (): unknown {
    return this.args.key;
  }

  get $iv (): Expression {
    return this.args.iv;
  }

  get $aad (): Expression | undefined {
    return this.args.aad;
  }

  get $encryptionMethod (): string | undefined {
    return this.args.encryptionMethod;
  }

  get $aead (): Expression | undefined {
    return this.args.aead;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }

  static {
    this.register();
  }
}

export type DiToDateExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DiToDateExpr extends FuncExpr {
  key = ExpressionKey.DI_TO_DATE;

  static argTypes: RequiredMap<DiToDateExprArgs> = {
    ...super.argTypes,
  };

  declare args: DiToDateExprArgs;

  constructor (args: DiToDateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type EncodeExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    charset: string;
  },
]>;

export class EncodeExpr extends FuncExpr {
  key = ExpressionKey.ENCODE;

  static argTypes: RequiredMap<EncodeExprArgs> = {
    ...super.argTypes,
    this: true,
    charset: true,
  };

  declare args: EncodeExprArgs;

  constructor (args: EncodeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $charset (): string {
    return this.args.charset;
  }

  static {
    this.register();
  }
}

export type EncryptExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    passphrase: Expression;
    aad?: Expression;
    encryptionMethod?: string;
  },
]>;

export class EncryptExpr extends FuncExpr {
  key = ExpressionKey.ENCRYPT;

  /**
   * Defines the arguments (properties and child expressions) for Encrypt expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<EncryptExprArgs> = {
    ...super.argTypes,
    this: true,
    passphrase: true,
    aad: false,
    encryptionMethod: false,
  };

  declare args: EncryptExprArgs;

  constructor (args: EncryptExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $passphrase (): Expression {
    return this.args.passphrase;
  }

  get $aad (): Expression | undefined {
    return this.args.aad;
  }

  get $encryptionMethod (): string | undefined {
    return this.args.encryptionMethod;
  }

  static {
    this.register();
  }
}

export type EncryptRawExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    key: unknown;
    iv: Expression;
    aad?: Expression;
    encryptionMethod?: string;
  },
]>;

export class EncryptRawExpr extends FuncExpr {
  key = ExpressionKey.ENCRYPT_RAW;

  /**
   * Defines the arguments (properties and child expressions) for EncryptRaw expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<EncryptRawExprArgs> = {
    ...super.argTypes,
    this: true,
    key: true,
    iv: true,
    aad: false,
    encryptionMethod: false,
  };

  declare args: EncryptRawExprArgs;

  constructor (args: EncryptRawExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $key (): unknown {
    return this.args.key;
  }

  get $iv (): Expression {
    return this.args.iv;
  }

  get $aad (): Expression | undefined {
    return this.args.aad;
  }

  get $encryptionMethod (): string | undefined {
    return this.args.encryptionMethod;
  }

  static {
    this.register();
  }
}

export type EqualNullExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class EqualNullExpr extends FuncExpr {
  key = ExpressionKey.EQUAL_NULL;

  static argTypes: RequiredMap<EqualNullExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: EqualNullExprArgs;

  constructor (args: EqualNullExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ExpExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ExpExpr extends FuncExpr {
  key = ExpressionKey.EXP;

  static argTypes: RequiredMap<ExpExprArgs> = {
    ...super.argTypes,
  };

  declare args: ExpExprArgs;

  constructor (args: ExpExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FactorialExprArgs = Merge<[
  FuncExprArgs,
]>;

export class FactorialExpr extends FuncExpr {
  key = ExpressionKey.FACTORIAL;

  static argTypes: RequiredMap<FactorialExprArgs> = {
    ...super.argTypes,
  };

  declare args: FactorialExprArgs;

  constructor (args: FactorialExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ExplodeExprArgs = Merge<[
  FuncExprArgs,
  UDTFExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class ExplodeExpr extends multiInherit(FuncExpr, UDTFExpr) {
  key = ExpressionKey.EXPLODE;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<ExplodeExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expressions: false,
  };

  declare args: ExplodeExprArgs;

  constructor (args: ExplodeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type InlineExprArgs = Merge<[
  FuncExprArgs,
]>;

export class InlineExpr extends FuncExpr {
  key = ExpressionKey.INLINE;

  static argTypes: RequiredMap<InlineExprArgs> = {
    ...super.argTypes,
  };

  declare args: InlineExprArgs;

  constructor (args: InlineExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnnestExprArgs = Merge<[
  FuncExprArgs,
  UDTFExprArgs,
  {
    expressions: Expression[];
    alias?: TableAliasExpr;
    offset?: boolean | Expression;
    explodeArray?: Expression;
  },
]>;

export class UnnestExpr extends multiInherit(FuncExpr, UDTFExpr) {
  key = ExpressionKey.UNNEST;

  static argTypes: RequiredMap<UnnestExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    expressions: true,
    alias: false,
    offset: false,
    explodeArray: false,
  };

  declare args: UnnestExprArgs;

  constructor (args: UnnestExprArgs) {
    super(args);
  }

  get selects (): Expression[] {
    const columns = super.selects;
    const offset = this.args.offset;
    if (offset) {
      const offsetCol = offset === true ? toIdentifier('offset') : offset;
      return [...columns, offsetCol];
    }
    return columns;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $alias (): TableAliasExpr | undefined {
    return this.args.alias;
  }

  get $offset (): boolean | Expression | undefined {
    return this.args.offset;
  }

  get $explodeArray (): Expression | undefined {
    return this.args.explodeArray;
  }

  static {
    this.register();
  }
}

export type FloorExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    decimals?: Expression[];
    to?: Expression;
  },
]>;

export class FloorExpr extends FuncExpr {
  key = ExpressionKey.FLOOR;

  /**
   * Defines the arguments (properties and child expressions) for Floor expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<FloorExprArgs> = {
    ...super.argTypes,
    this: true,
    decimals: false,
    to: false,
  };

  declare args: FloorExprArgs;

  constructor (args: FloorExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $decimals (): Expression[] | undefined {
    return this.args.decimals;
  }

  get $to (): Expression | undefined {
    return this.args.to;
  }

  static {
    this.register();
  }
}

export type FromBase32ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class FromBase32Expr extends FuncExpr {
  key = ExpressionKey.FROM_BASE32;

  static argTypes: RequiredMap<FromBase32ExprArgs> = {
    ...super.argTypes,
  };

  declare args: FromBase32ExprArgs;

  constructor (args: FromBase32ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FromBase64ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class FromBase64Expr extends FuncExpr {
  key = ExpressionKey.FROM_BASE64;

  static argTypes: RequiredMap<FromBase64ExprArgs> = {
    ...super.argTypes,
  };

  declare args: FromBase64ExprArgs;

  constructor (args: FromBase64ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToBase32ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ToBase32Expr extends FuncExpr {
  key = ExpressionKey.TO_BASE32;

  static argTypes: RequiredMap<ToBase32ExprArgs> = {
    ...super.argTypes,
  };

  declare args: ToBase32ExprArgs;

  constructor (args: ToBase32ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToBase64ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ToBase64Expr extends FuncExpr {
  key = ExpressionKey.TO_BASE64;

  static argTypes: RequiredMap<ToBase64ExprArgs> = {
    ...super.argTypes,
  };

  declare args: ToBase64ExprArgs;

  constructor (args: ToBase64ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToBinaryExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    format?: string;
    safe?: boolean;
  },
]>;

export class ToBinaryExpr extends FuncExpr {
  key = ExpressionKey.TO_BINARY;

  /**
   * Defines the arguments (properties and child expressions) for ToBinary expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ToBinaryExprArgs> = {
    ...super.argTypes,
    this: true,
    format: false,
    safe: false,
  };

  declare args: ToBinaryExprArgs;

  constructor (args: ToBinaryExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $format (): string | undefined {
    return this.args.format;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }

  static {
    this.register();
  }
}

export type Base64DecodeBinaryExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    alphabet?: Expression;
  },
]>;

export class Base64DecodeBinaryExpr extends FuncExpr {
  key = ExpressionKey.BASE64_DECODE_BINARY;

  static argTypes: RequiredMap<Base64DecodeBinaryExprArgs> = {
    ...super.argTypes,
    this: true,
    alphabet: false,
  };

  declare args: Base64DecodeBinaryExprArgs;

  constructor (args: Base64DecodeBinaryExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $alphabet (): Expression | undefined {
    return this.args.alphabet;
  }

  static {
    this.register();
  }
}

export type Base64DecodeStringExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    alphabet?: Expression;
  },
]>;

export class Base64DecodeStringExpr extends FuncExpr {
  key = ExpressionKey.BASE64_DECODE_STRING;

  static argTypes: RequiredMap<Base64DecodeStringExprArgs> = {
    ...super.argTypes,
    this: true,
    alphabet: false,
  };

  declare args: Base64DecodeStringExprArgs;

  constructor (args: Base64DecodeStringExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $alphabet (): Expression | undefined {
    return this.args.alphabet;
  }

  static {
    this.register();
  }
}

export type Base64EncodeExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    maxLineLength?: number | Expression;
    alphabet?: Expression;
  },
]>;

export class Base64EncodeExpr extends FuncExpr {
  key = ExpressionKey.BASE64_ENCODE;

  /**
   * Defines the arguments (properties and child expressions) for Base64Encode expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<Base64EncodeExprArgs> = {
    ...super.argTypes,
    this: true,
    maxLineLength: false,
    alphabet: false,
  };

  declare args: Base64EncodeExprArgs;

  constructor (args: Base64EncodeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $maxLineLength (): number | Expression | undefined {
    return this.args.maxLineLength;
  }

  get $alphabet (): Expression | undefined {
    return this.args.alphabet;
  }

  static {
    this.register();
  }
}

export type TryBase64DecodeBinaryExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    alphabet?: Expression;
  },
]>;

export class TryBase64DecodeBinaryExpr extends FuncExpr {
  key = ExpressionKey.TRY_BASE64_DECODE_BINARY;

  static argTypes: RequiredMap<TryBase64DecodeBinaryExprArgs> = {
    ...super.argTypes,
    this: true,
    alphabet: false,
  };

  declare args: TryBase64DecodeBinaryExprArgs;

  constructor (args: TryBase64DecodeBinaryExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $alphabet (): Expression | undefined {
    return this.args.alphabet;
  }

  static {
    this.register();
  }
}

export type TryBase64DecodeStringExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    alphabet?: Expression;
  },
]>;

export class TryBase64DecodeStringExpr extends FuncExpr {
  key = ExpressionKey.TRY_BASE64_DECODE_STRING;

  static argTypes: RequiredMap<TryBase64DecodeStringExprArgs> = {
    ...super.argTypes,
    this: true,
    alphabet: false,
  };

  declare args: TryBase64DecodeStringExprArgs;

  constructor (args: TryBase64DecodeStringExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $alphabet (): Expression | undefined {
    return this.args.alphabet;
  }

  static {
    this.register();
  }
}

export type TryHexDecodeBinaryExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TryHexDecodeBinaryExpr extends FuncExpr {
  key = ExpressionKey.TRY_HEX_DECODE_BINARY;

  static argTypes: RequiredMap<TryHexDecodeBinaryExprArgs> = {
    ...super.argTypes,
  };

  declare args: TryHexDecodeBinaryExprArgs;

  constructor (args: TryHexDecodeBinaryExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TryHexDecodeStringExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TryHexDecodeStringExpr extends FuncExpr {
  key = ExpressionKey.TRY_HEX_DECODE_STRING;

  static argTypes: RequiredMap<TryHexDecodeStringExprArgs> = {
    ...super.argTypes,
  };

  declare args: TryHexDecodeStringExprArgs;

  constructor (args: TryHexDecodeStringExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FromISO8601TimestampExprArgs = Merge<[
  FuncExprArgs,
]>;

export class FromISO8601TimestampExpr extends FuncExpr {
  key = ExpressionKey.FROM_ISO8601_TIMESTAMP;

  static argTypes: RequiredMap<FromISO8601TimestampExprArgs> = {
    ...super.argTypes,
  };

  declare args: FromISO8601TimestampExprArgs;

  constructor (args: FromISO8601TimestampExprArgs) {
    super(args);
  }

  static _sqlNames = ['FROM_ISO8601_TIMESTAMP'];

  static {
    this.register();
  }
}

export type GapFillExprArgs = Merge<[
  FuncExprArgs,
  {
    tsColumn: Expression;
    bucketWidth: Expression;
    partitioningColumns?: Expression[];
    valueColumns?: Expression[];
    origin?: Expression;
    ignoreNulls?: Expression[];
  },
]>;

export class GapFillExpr extends FuncExpr {
  key = ExpressionKey.GAP_FILL;

  /**
   * Defines the arguments (properties and child expressions) for GapFill expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<GapFillExprArgs> = {
    ...super.argTypes,
    tsColumn: true,
    bucketWidth: true,
    partitioningColumns: false,
    valueColumns: false,
    origin: false,
    ignoreNulls: false,
  };

  declare args: GapFillExprArgs;

  static {
    this.register();
  }

  constructor (args: GapFillExprArgs) {
    super(args);
  }

  get $tsColumn (): Expression {
    return this.args.tsColumn;
  }

  get $bucketWidth (): Expression {
    return this.args.bucketWidth;
  }

  get $partitioningColumns (): Expression[] | undefined {
    return this.args.partitioningColumns;
  }

  get $valueColumns (): Expression[] | undefined {
    return this.args.valueColumns;
  }

  get $origin (): Expression | undefined {
    return this.args.origin;
  }

  get $ignoreNulls (): Expression[] | undefined {
    return this.args.ignoreNulls;
  }
}

export type GenerateDateArrayExprArgs = Merge<[
  FuncExprArgs,
  {
    start: Expression;
    end: Expression;
    step?: Expression;
  },
]>;

export class GenerateDateArrayExpr extends FuncExpr {
  key = ExpressionKey.GENERATE_DATE_ARRAY;

  /**
   * Defines the arguments (properties and child expressions) for GenerateDateArray expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<GenerateDateArrayExprArgs> = {
    ...super.argTypes,
    start: true,
    end: true,
    step: false,
  };

  declare args: GenerateDateArrayExprArgs;

  constructor (args: GenerateDateArrayExprArgs) {
    super(args);
  }

  get $start (): Expression {
    return this.args.start;
  }

  get $end (): Expression {
    return this.args.end;
  }

  get $step (): Expression | undefined {
    return this.args.step;
  }

  static {
    this.register();
  }
}

export type GenerateTimestampArrayExprArgs = Merge<[
  FuncExprArgs,
  {
    start: Expression;
    end: Expression;
    step: Expression;
  },
]>;

export class GenerateTimestampArrayExpr extends FuncExpr {
  key = ExpressionKey.GENERATE_TIMESTAMP_ARRAY;

  /**
   * Defines the arguments (properties and child expressions) for GenerateTimestampArray
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<GenerateTimestampArrayExprArgs> = {
    ...super.argTypes,
    start: true,
    end: true,
    step: true,
  };

  declare args: GenerateTimestampArrayExprArgs;

  constructor (args: GenerateTimestampArrayExprArgs) {
    super(args);
  }

  get $start (): Expression {
    return this.args.start;
  }

  get $end (): Expression {
    return this.args.end;
  }

  get $step (): Expression {
    return this.args.step;
  }

  static {
    this.register();
  }
}

export type GetExtractExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class GetExtractExpr extends FuncExpr {
  key = ExpressionKey.GET_EXTRACT;

  static argTypes: RequiredMap<GetExtractExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: GetExtractExprArgs;

  constructor (args: GetExtractExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type GetbitExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    zeroIsMsb?: Expression;
  },
]>;

export class GetbitExpr extends FuncExpr {
  key = ExpressionKey.GETBIT;

  static argTypes: RequiredMap<GetbitExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    zeroIsMsb: false,
  };

  declare args: GetbitExprArgs;

  constructor (args: GetbitExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $zeroIsMsb (): Expression | undefined {
    return this.args.zeroIsMsb;
  }

  static {
    this.register();
  }
}

export type GreatestExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
    ignoreNulls: boolean;
  },
]>;

export class GreatestExpr extends FuncExpr {
  key = ExpressionKey.GREATEST;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<GreatestExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
    ignoreNulls: true,
  };

  declare args: GreatestExprArgs;

  constructor (args: GreatestExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $ignoreNulls (): boolean {
    return this.args.ignoreNulls;
  }

  static {
    this.register();
  }
}

export type HexExprArgs = Merge<[
  FuncExprArgs,
]>;

export class HexExpr extends FuncExpr {
  key = ExpressionKey.HEX;

  static argTypes: RequiredMap<HexExprArgs> = {
    ...super.argTypes,
  };

  declare args: HexExprArgs;

  constructor (args: HexExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type HexDecodeStringExprArgs = Merge<[
  FuncExprArgs,
]>;

export class HexDecodeStringExpr extends FuncExpr {
  key = ExpressionKey.HEX_DECODE_STRING;

  static argTypes: RequiredMap<HexDecodeStringExprArgs> = {
    ...super.argTypes,
  };

  declare args: HexDecodeStringExprArgs;

  constructor (args: HexDecodeStringExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type HexEncodeExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    case?: Expression;
  },
]>;

export class HexEncodeExpr extends FuncExpr {
  key = ExpressionKey.HEX_ENCODE;

  static argTypes: RequiredMap<HexEncodeExprArgs> = {
    ...super.argTypes,
    this: true,
    case: false,
  };

  declare args: HexEncodeExprArgs;

  constructor (args: HexEncodeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $case (): Expression | undefined {
    return this.args.case;
  }

  static {
    this.register();
  }
}

export type HourExprArgs = Merge<[
  FuncExprArgs,
]>;

export class HourExpr extends FuncExpr {
  key = ExpressionKey.HOUR;

  static argTypes: RequiredMap<HourExprArgs> = {
    ...super.argTypes,
  };

  declare args: HourExprArgs;

  constructor (args: HourExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MinuteExprArgs = Merge<[
  FuncExprArgs,
]>;

export class MinuteExpr extends FuncExpr {
  key = ExpressionKey.MINUTE;

  static argTypes: RequiredMap<MinuteExprArgs> = {
    ...super.argTypes,
  };

  declare args: MinuteExprArgs;

  constructor (args: MinuteExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SecondExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SecondExpr extends FuncExpr {
  key = ExpressionKey.SECOND;

  static argTypes: RequiredMap<SecondExprArgs> = {
    ...super.argTypes,
  };

  declare args: SecondExprArgs;

  constructor (args: SecondExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CompressExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    method?: string;
  },
]>;

export class CompressExpr extends FuncExpr {
  key = ExpressionKey.COMPRESS;

  static argTypes: RequiredMap<CompressExprArgs> = {
    ...super.argTypes,
    this: true,
    method: false,
  };

  declare args: CompressExprArgs;

  constructor (args: CompressExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $method (): string | undefined {
    return this.args.method;
  }

  static {
    this.register();
  }
}

export type DecompressBinaryExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    method: string;
  },
]>;

export class DecompressBinaryExpr extends FuncExpr {
  key = ExpressionKey.DECOMPRESS_BINARY;

  static argTypes: RequiredMap<DecompressBinaryExprArgs> = {
    ...super.argTypes,
    this: true,
    method: true,
  };

  declare args: DecompressBinaryExprArgs;

  constructor (args: DecompressBinaryExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $method (): string {
    return this.args.method;
  }

  static {
    this.register();
  }
}

export type DecompressStringExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    method: string;
  },
]>;

export class DecompressStringExpr extends FuncExpr {
  key = ExpressionKey.DECOMPRESS_STRING;

  static argTypes: RequiredMap<DecompressStringExprArgs> = {
    ...super.argTypes,
    this: true,
    method: true,
  };

  declare args: DecompressStringExprArgs;

  constructor (args: DecompressStringExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $method (): string {
    return this.args.method;
  }

  static {
    this.register();
  }
}

export type IfExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    true: Expression;
    false?: Expression;
  },
]>;

export class IfExpr extends FuncExpr {
  key = ExpressionKey.IF;

  static _sqlNames = ['IF', 'IIF'];

  /**
   * Defines the arguments (properties and child expressions) for If expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<IfExprArgs> = {
    ...super.argTypes,
    this: true,
    true: true,
    false: false,
  };

  declare args: IfExprArgs;

  constructor (args: IfExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $true (): Expression {
    return this.args.true;
  }

  get $false (): Expression | undefined {
    return this.args.false;
  }

  static {
    this.register();
  }
}

export type NullifExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class NullifExpr extends FuncExpr {
  key = ExpressionKey.NULLIF;

  static argTypes: RequiredMap<NullifExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: NullifExprArgs;

  constructor (args: NullifExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type InitcapExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class InitcapExpr extends FuncExpr {
  key = ExpressionKey.INITCAP;

  static argTypes: RequiredMap<InitcapExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: InitcapExprArgs;

  constructor (args: InitcapExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type IsAsciiExprArgs = Merge<[
  FuncExprArgs,
]>;

export class IsAsciiExpr extends FuncExpr {
  key = ExpressionKey.IS_ASCII;

  static argTypes: RequiredMap<IsAsciiExprArgs> = {
    ...super.argTypes,
  };

  declare args: IsAsciiExprArgs;

  constructor (args: IsAsciiExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type IsNanExprArgs = Merge<[
  FuncExprArgs,
]>;

export class IsNanExpr extends FuncExpr {
  key = ExpressionKey.IS_NAN;

  static argTypes: RequiredMap<IsNanExprArgs> = {
    ...super.argTypes,
  };

  declare args: IsNanExprArgs;

  constructor (args: IsNanExprArgs) {
    super(args);
  }

  static _sqlNames = ['IS_NAN', 'ISNAN'];

  static {
    this.register();
  }
}

export type Int64ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class Int64Expr extends FuncExpr {
  key = ExpressionKey.INT64;

  static argTypes: RequiredMap<Int64ExprArgs> = {
    ...super.argTypes,
  };

  declare args: Int64ExprArgs;

  constructor (args: Int64ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type IsInfExprArgs = Merge<[
  FuncExprArgs,
]>;

export class IsInfExpr extends FuncExpr {
  key = ExpressionKey.IS_INF;

  static argTypes: RequiredMap<IsInfExprArgs> = {
    ...super.argTypes,
  };

  declare args: IsInfExprArgs;

  constructor (args: IsInfExprArgs) {
    super(args);
  }

  static _sqlNames = ['IS_INF', 'ISINF'];

  static {
    this.register();
  }
}

export type IsNullValueExprArgs = Merge<[
  FuncExprArgs,
]>;

export class IsNullValueExpr extends FuncExpr {
  key = ExpressionKey.IS_NULL_VALUE;

  static argTypes: RequiredMap<IsNullValueExprArgs> = {
    ...super.argTypes,
  };

  declare args: IsNullValueExprArgs;

  constructor (args: IsNullValueExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type IsArrayExprArgs = Merge<[
  FuncExprArgs,
]>;

export class IsArrayExpr extends FuncExpr {
  key = ExpressionKey.IS_ARRAY;

  static argTypes: RequiredMap<IsArrayExprArgs> = {
    ...super.argTypes,
  };

  declare args: IsArrayExprArgs;

  constructor (args: IsArrayExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FormatExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class FormatExpr extends FuncExpr {
  key = ExpressionKey.FORMAT;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<FormatExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
  };

  declare args: FormatExprArgs;

  constructor (args: FormatExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type JSONKeysExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
    expressions?: Expression[];
  },
]>;

export class JSONKeysExpr extends FuncExpr {
  key = ExpressionKey.JSON_KEYS;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<JSONKeysExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
    expressions: false,
  };

  declare args: JSONKeysExprArgs;

  constructor (args: JSONKeysExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type JSONKeysAtDepthExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
    mode?: Expression;
  },
]>;

export class JSONKeysAtDepthExpr extends FuncExpr {
  key = ExpressionKey.JSON_KEYS_AT_DEPTH;

  static argTypes: RequiredMap<JSONKeysAtDepthExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
    mode: false,
  };

  declare args: JSONKeysAtDepthExprArgs;

  constructor (args: JSONKeysAtDepthExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $mode (): Expression | undefined {
    return this.args.mode;
  }

  static {
    this.register();
  }
}

export type JSONObjectExprArgs = Merge<[
  FuncExprArgs,
  {
    nullHandling?: Expression;
    uniqueKeys?: Expression[];
    returnType?: DataTypeExpr;
    encoding?: Expression;
  },
]>;

export class JSONObjectExpr extends FuncExpr {
  key = ExpressionKey.JSON_OBJECT;

  /**
   * Defines the arguments (properties and child expressions) for JSONObject expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JSONObjectExprArgs> = {
    ...super.argTypes,
    nullHandling: false,
    uniqueKeys: false,
    returnType: false,
    encoding: false,
  };

  declare args: JSONObjectExprArgs;

  constructor (args: JSONObjectExprArgs) {
    super(args);
  }

  get $nullHandling (): Expression | undefined {
    return this.args.nullHandling;
  }

  get $uniqueKeys (): Expression[] | undefined {
    return this.args.uniqueKeys;
  }

  get $returnType (): DataTypeExpr | undefined {
    return this.args.returnType;
  }

  get $encoding (): Expression | undefined {
    return this.args.encoding;
  }

  static {
    this.register();
  }
}

export type JSONArrayExprArgs = Merge<[
  FuncExprArgs,
  {
    nullHandling?: Expression;
    returnType?: DataTypeExpr;
    strict?: Expression;
  },
]>;

export class JSONArrayExpr extends FuncExpr {
  key = ExpressionKey.JSON_ARRAY;

  /**
   * Defines the arguments (properties and child expressions) for JSONArray expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JSONArrayExprArgs> = {
    ...super.argTypes,
    nullHandling: false,
    returnType: false,
    strict: false,
  };

  declare args: JSONArrayExprArgs;

  constructor (args: JSONArrayExprArgs) {
    super(args);
  }

  get $nullHandling (): Expression | undefined {
    return this.args.nullHandling;
  }

  get $returnType (): DataTypeExpr | undefined {
    return this.args.returnType;
  }

  get $strict (): Expression | undefined {
    return this.args.strict;
  }

  static {
    this.register();
  }
}

export type JSONExistsExprArgs = Merge<[
  FuncExprArgs,
  {
    path: Expression;
    passing?: Expression;
    onCondition?: Expression;
    fromDcolonqmark?: Expression;
  },
]>;

export class JSONExistsExpr extends FuncExpr {
  key = ExpressionKey.JSON_EXISTS;

  /**
   * Defines the arguments (properties and child expressions) for JSONExists expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JSONExistsExprArgs> = {
    ...super.argTypes,
    path: true,
    passing: false,
    onCondition: false,
    fromDcolonqmark: false,
  };

  declare args: JSONExistsExprArgs;

  constructor (args: JSONExistsExprArgs) {
    super(args);
  }

  get $path (): Expression {
    return this.args.path;
  }

  get $passing (): Expression | undefined {
    return this.args.passing;
  }

  get $onCondition (): Expression | undefined {
    return this.args.onCondition;
  }

  get $fromDcolonqmark (): Expression | undefined {
    return this.args.fromDcolonqmark;
  }

  static {
    this.register();
  }
}

export type JSONSetExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions: Expression[];
  },
]>;

export class JSONSetExpr extends FuncExpr {
  key = ExpressionKey.JSON_SET;

  static isVarLenArgs = true;
  static _sqlNames = ['JSON_SET'];

  static argTypes: RequiredMap<JSONSetExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
  };

  declare args: JSONSetExprArgs;

  constructor (args: JSONSetExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type JSONStripNullsExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
    includeArrays?: Expression;
    removeEmpty?: Expression;
  },
]>;

export class JSONStripNullsExpr extends FuncExpr {
  key = ExpressionKey.JSON_STRIP_NULLS;

  static _sqlNames = ['JSON_STRIP_NULLS'];

  /**
   * Defines the arguments (properties and child expressions) for JSONStripNulls expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JSONStripNullsExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
    includeArrays: false,
    removeEmpty: false,
  };

  declare args: JSONStripNullsExprArgs;

  constructor (args: JSONStripNullsExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $includeArrays (): Expression | undefined {
    return this.args.includeArrays;
  }

  get $removeEmpty (): Expression | undefined {
    return this.args.removeEmpty;
  }

  static {
    this.register();
  }
}

export type JSONValueArrayExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class JSONValueArrayExpr extends FuncExpr {
  key = ExpressionKey.JSON_VALUE_ARRAY;

  static argTypes: RequiredMap<JSONValueArrayExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: JSONValueArrayExprArgs;

  constructor (args: JSONValueArrayExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type JSONRemoveExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions: Expression[];
  },
]>;

export class JSONRemoveExpr extends FuncExpr {
  key = ExpressionKey.JSON_REMOVE;

  static isVarLenArgs = true;
  static _sqlNames = ['JSON_REMOVE'];

  static argTypes: RequiredMap<JSONRemoveExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
  };

  declare args: JSONRemoveExprArgs;

  constructor (args: JSONRemoveExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type JSONTableExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    schema: Expression;
    path?: Expression | string;
    errorHandling?: Expression | string;
    emptyHandling?: Expression | string;
  },
]>;

export class JSONTableExpr extends FuncExpr {
  key = ExpressionKey.JSON_TABLE;

  /**
   * Defines the arguments (properties and child expressions) for JSONTable expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JSONTableExprArgs> = {
    ...super.argTypes,
    this: true,
    schema: true,
    path: false,
    errorHandling: false,
    emptyHandling: false,
  };

  declare args: JSONTableExprArgs;

  constructor (args: JSONTableExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $schema (): Expression {
    return this.args.schema;
  }

  get $path (): Expression | string | undefined {
    return this.args.path;
  }

  get $errorHandling (): Expression | string | undefined {
    return this.args.errorHandling;
  }

  get $emptyHandling (): Expression | string | undefined {
    return this.args.emptyHandling;
  }

  static {
    this.register();
  }
}

export type JSONTypeExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class JSONTypeExpr extends FuncExpr {
  key = ExpressionKey.JSON_TYPE;

  static _sqlNames = ['JSON_TYPE'];

  static argTypes: RequiredMap<JSONTypeExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: JSONTypeExprArgs;

  constructor (args: JSONTypeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ObjectInsertExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    key: Expression;
    value: Expression;
    updateFlag?: Expression;
  },
]>;

export class ObjectInsertExpr extends FuncExpr {
  key = ExpressionKey.OBJECT_INSERT;

  /**
   * Defines the arguments (properties and child expressions) for ObjectInsert expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ObjectInsertExprArgs> = {
    ...super.argTypes,
    this: true,
    key: true,
    value: true,
    updateFlag: false,
  };

  declare args: ObjectInsertExprArgs;

  constructor (args: ObjectInsertExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $key (): Expression {
    return this.args.key;
  }

  get $value (): Expression {
    return this.args.value;
  }

  get $updateFlag (): Expression | undefined {
    return this.args.updateFlag;
  }

  static {
    this.register();
  }
}

export type OpenJSONExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    path?: Expression;
    expressions?: Expression[];
  },
]>;

export class OpenJSONExpr extends FuncExpr {
  key = ExpressionKey.OPEN_JSON;

  static argTypes: RequiredMap<OpenJSONExprArgs> = {
    ...super.argTypes,
    this: true,
    path: false,
    expressions: false,
  };

  declare args: OpenJSONExprArgs;

  constructor (args: OpenJSONExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $path (): Expression | undefined {
    return this.args.path;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type JSONBContainsExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
]>;

export class JSONBContainsExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.JSONB_CONTAINS;

  static _sqlNames = ['JSONB_CONTAINS'];

  static argTypes: RequiredMap<JSONBContainsExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: JSONBContainsExprArgs;

  constructor (args: JSONBContainsExprArgs) {
    super(args);
  }
}

export type JSONBContainsAnyTopKeysExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
]>;

export class JSONBContainsAnyTopKeysExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.JSONB_CONTAINS_ANY_TOP_KEYS;

  static argTypes: RequiredMap<JSONBContainsAnyTopKeysExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: JSONBContainsAnyTopKeysExprArgs;

  constructor (args: JSONBContainsAnyTopKeysExprArgs) {
    super(args);
  }
}

export type JSONBContainsAllTopKeysExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
]>;

export class JSONBContainsAllTopKeysExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.JSONB_CONTAINS_ALL_TOP_KEYS;

  static argTypes: RequiredMap<JSONBContainsAllTopKeysExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: JSONBContainsAllTopKeysExprArgs;

  constructor (args: JSONBContainsAllTopKeysExprArgs) {
    super(args);
  }
}

export type JSONBExistsExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    path: Expression;
  },
]>;

export class JSONBExistsExpr extends FuncExpr {
  key = ExpressionKey.JSONB_EXISTS;

  static _sqlNames = ['JSONB_EXISTS'];

  static argTypes: RequiredMap<JSONBExistsExprArgs> = {
    ...super.argTypes,
    this: true,
    path: true,
  };

  declare args: JSONBExistsExprArgs;

  constructor (args: JSONBExistsExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $path (): Expression {
    return this.args.path;
  }

  static {
    this.register();
  }
}

export type JSONBDeleteAtPathExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
]>;

export class JSONBDeleteAtPathExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.JSONB_DELETE_AT_PATH;

  static argTypes: RequiredMap<JSONBDeleteAtPathExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: JSONBDeleteAtPathExprArgs;

  constructor (args: JSONBDeleteAtPathExprArgs) {
    super(args);
  }
}

export type JSONExtractExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
  {
    onlyJsonTypes?: Expression;
    expressions?: Expression[];
    variantExtract?: Expression;
    jsonQuery?: Expression;
    option?: Expression;
    quote?: Expression;
    onCondition?: Expression;
    requiresJson?: Expression;
    expression: Expression;
  },
]>;

export class JSONExtractExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.JSON_EXTRACT;

  static isVarLenArgs = true;
  static _sqlNames = ['JSON_EXTRACT'];

  /**
   * Defines the arguments (properties and child expressions) for JSONExtract expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JSONExtractExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    onlyJsonTypes: false,
    expressions: false,
    variantExtract: false,
    jsonQuery: false,
    option: false,
    quote: false,
    onCondition: false,
    requiresJson: false,
    expression: true,
  };

  declare args: JSONExtractExprArgs;

  constructor (args: JSONExtractExprArgs) {
    super(args);
  }

  get $onlyJsonTypes (): Expression | undefined {
    return this.args.onlyJsonTypes;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $variantExtract (): Expression | undefined {
    return this.args.variantExtract;
  }

  get $jsonQuery (): Expression | undefined {
    return this.args.jsonQuery;
  }

  get $option (): Expression | undefined {
    return this.args.option;
  }

  get $quote (): Expression | undefined {
    return this.args.quote;
  }

  get $onCondition (): Expression | undefined {
    return this.args.onCondition;
  }

  get $requiresJson (): Expression | undefined {
    return this.args.requiresJson;
  }

  get outputName (): string {
    return !this.args.expressions ? this.$expression.outputName : '';
  }
}

export type JSONExtractArrayExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class JSONExtractArrayExpr extends FuncExpr {
  key = ExpressionKey.JSON_EXTRACT_ARRAY;

  static _sqlNames = ['JSON_EXTRACT_ARRAY'];

  static argTypes: RequiredMap<JSONExtractArrayExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: JSONExtractArrayExprArgs;

  constructor (args: JSONExtractArrayExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type JSONExtractScalarExprArgs = Merge<[
  BinaryExprArgs,
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    onlyJsonTypes?: Expression;
    expressions?: Expression[];
    jsonType?: Expression;
    scalarOnly?: boolean;
  },
]>;

export class JSONExtractScalarExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.JSON_EXTRACT_SCALAR;

  static isVarLenArgs = true;
  static _sqlNames = ['JSON_EXTRACT_SCALAR'];

  /**
   * Defines the arguments (properties and child expressions) for JSONExtractScalar expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JSONExtractScalarExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    expression: true,
    onlyJsonTypes: false,
    expressions: false,
    jsonType: false,
    scalarOnly: false,
    this: true,
  };

  declare args: JSONExtractScalarExprArgs;

  constructor (args: JSONExtractScalarExprArgs) {
    super(args);
  }

  get $onlyJsonTypes (): Expression | undefined {
    return this.args.onlyJsonTypes;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $jsonType (): Expression | undefined {
    return this.args.jsonType;
  }

  get $scalarOnly (): boolean | undefined {
    return this.args.scalarOnly;
  }

  get outputName (): string {
    return this.$expression.outputName;
  }

  get $expression (): Expression {
    return this.args.expression;
  }
}

export type JSONBExtractExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
]>;

export class JSONBExtractExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.JSONB_EXTRACT;

  static _sqlNames = ['JSONB_EXTRACT'];

  static argTypes: RequiredMap<JSONBExtractExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: JSONBExtractExprArgs;

  constructor (args: JSONBExtractExprArgs) {
    super(args);
  }
}

export type JSONBExtractScalarExprArgs = Merge<[
  BinaryExprArgs,
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    jsonType?: Expression;
  },
]>;

export class JSONBExtractScalarExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.JSONB_EXTRACT_SCALAR;

  static _sqlNames = ['JSONB_EXTRACT_SCALAR'];

  static argTypes: RequiredMap<JSONBExtractScalarExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    jsonType: false,
  };

  declare args: JSONBExtractScalarExprArgs;

  constructor (args: JSONBExtractScalarExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $jsonType (): Expression | undefined {
    return this.args.jsonType;
  }
}

export type JSONFormatExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    options?: Expression[];
    isJson?: Expression;
    toJson?: Expression;
  },
]>;

export class JSONFormatExpr extends FuncExpr {
  key = ExpressionKey.JSON_FORMAT;

  static _sqlNames = ['JSON_FORMAT'];

  /**
   * Defines the arguments (properties and child expressions) for JSONFormat expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JSONFormatExprArgs> = {
    ...super.argTypes,
    this: false,
    options: false,
    isJson: false,
    toJson: false,
  };

  declare args: JSONFormatExprArgs;

  constructor (args: JSONFormatExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }

  get $isJson (): Expression | undefined {
    return this.args.isJson;
  }

  get $toJson (): Expression | undefined {
    return this.args.toJson;
  }

  static {
    this.register();
  }
}

export type JSONArrayAppendExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions: Expression[];
  },
]>;

export class JSONArrayAppendExpr extends FuncExpr {
  key = ExpressionKey.JSON_ARRAY_APPEND;

  static isVarLenArgs = true;
  static _sqlNames = ['JSON_ARRAY_APPEND'];

  static argTypes: RequiredMap<JSONArrayAppendExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
  };

  declare args: JSONArrayAppendExprArgs;

  constructor (args: JSONArrayAppendExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type JSONArrayContainsExprArgs = Merge<[
  BinaryExprArgs,
  {
    this: Expression;
    jsonType?: Expression;
    expression: Expression;
  },
]>;

export class JSONArrayContainsExpr extends multiInherit(BinaryExpr, PredicateExpr, FuncExpr) {
  key = ExpressionKey.JSON_ARRAY_CONTAINS;

  static _sqlNames = ['JSON_ARRAY_CONTAINS'];

  static argTypes: RequiredMap<JSONArrayContainsExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    jsonType: false,
  };

  declare args: JSONArrayContainsExprArgs;

  constructor (args: JSONArrayContainsExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $jsonType (): Expression | undefined {
    return this.args.jsonType;
  }
}

export type JSONArrayInsertExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions: Expression[];
  },
]>;

export class JSONArrayInsertExpr extends FuncExpr {
  key = ExpressionKey.JSON_ARRAY_INSERT;

  static isVarLenArgs = true;
  static _sqlNames = ['JSON_ARRAY_INSERT'];

  static argTypes: RequiredMap<JSONArrayInsertExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
  };

  declare args: JSONArrayInsertExprArgs;

  constructor (args: JSONArrayInsertExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type ParseBignumericExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ParseBignumericExpr extends FuncExpr {
  key = ExpressionKey.PARSE_BIGNUMERIC;

  static argTypes: RequiredMap<ParseBignumericExprArgs> = {
    ...super.argTypes,
  };

  declare args: ParseBignumericExprArgs;

  constructor (args: ParseBignumericExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ParseNumericExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ParseNumericExpr extends FuncExpr {
  key = ExpressionKey.PARSE_NUMERIC;

  static argTypes: RequiredMap<ParseNumericExprArgs> = {
    ...super.argTypes,
  };

  declare args: ParseNumericExprArgs;

  constructor (args: ParseNumericExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ParseJSONExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
    safe?: boolean;
  },
]>;

export class ParseJSONExpr extends FuncExpr {
  key = ExpressionKey.PARSE_JSON;

  static _sqlNames = ['PARSE_JSON', 'JSON_PARSE'];

  static argTypes: RequiredMap<ParseJSONExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
    safe: false,
  };

  declare args: ParseJSONExprArgs;

  constructor (args: ParseJSONExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }

  static {
    this.register();
  }
}

export type ParseUrlExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    partToExtract?: Expression;
    key?: unknown;
    permissive?: Expression;
  },
]>;

export class ParseUrlExpr extends FuncExpr {
  key = ExpressionKey.PARSE_URL;

  /**
   * Defines the arguments (properties and child expressions) for ParseUrl expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ParseUrlExprArgs> = {
    ...super.argTypes,
    this: true,
    partToExtract: false,
    key: false,
    permissive: false,
  };

  declare args: ParseUrlExprArgs;

  constructor (args: ParseUrlExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $partToExtract (): Expression | undefined {
    return this.args.partToExtract;
  }

  get $key (): unknown {
    return this.args.key;
  }

  get $permissive (): Expression | undefined {
    return this.args.permissive;
  }

  static {
    this.register();
  }
}

export type ParseIpExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    type: Expression;
    permissive?: Expression;
  },
]>;

export class ParseIpExpr extends FuncExpr {
  key = ExpressionKey.PARSE_IP;

  static argTypes: RequiredMap<ParseIpExprArgs> = {
    ...super.argTypes,
    this: true,
    type: true,
    permissive: false,
  };

  declare args: ParseIpExprArgs;

  constructor (args: ParseIpExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $type (): Expression {
    return this.args.type;
  }

  get $permissive (): Expression | undefined {
    return this.args.permissive;
  }

  static {
    this.register();
  }
}

export type ParseTimeExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    format: string;
  },
]>;

export class ParseTimeExpr extends FuncExpr {
  key = ExpressionKey.PARSE_TIME;

  static argTypes: RequiredMap<ParseTimeExprArgs> = {
    ...super.argTypes,
    this: true,
    format: true,
  };

  declare args: ParseTimeExprArgs;

  constructor (args: ParseTimeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $format (): string {
    return this.args.format;
  }

  static {
    this.register();
  }
}

export type ParseDatetimeExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    format?: string;
    zone?: Expression;
  },
]>;

export class ParseDatetimeExpr extends FuncExpr {
  key = ExpressionKey.PARSE_DATETIME;

  /**
   * Defines the arguments (properties and child expressions) for ParseDatetime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ParseDatetimeExprArgs> = {
    ...super.argTypes,
    this: true,
    format: false,
    zone: false,
  };

  declare args: ParseDatetimeExprArgs;

  constructor (args: ParseDatetimeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $format (): string | undefined {
    return this.args.format;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  static {
    this.register();
  }
}

export type LeastExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
    ignoreNulls: boolean;
  },
]>;

export class LeastExpr extends FuncExpr {
  key = ExpressionKey.LEAST;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<LeastExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
    ignoreNulls: true,
  };

  declare args: LeastExprArgs;

  constructor (args: LeastExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $ignoreNulls (): boolean {
    return this.args.ignoreNulls;
  }

  static {
    this.register();
  }
}

export type LeftExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class LeftExpr extends FuncExpr {
  key = ExpressionKey.LEFT;

  static argTypes: RequiredMap<LeftExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: LeftExprArgs;

  constructor (args: LeftExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type RightExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class RightExpr extends FuncExpr {
  key = ExpressionKey.RIGHT;

  static argTypes: RequiredMap<RightExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: RightExprArgs;

  constructor (args: RightExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ReverseExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ReverseExpr extends FuncExpr {
  key = ExpressionKey.REVERSE;

  static argTypes: RequiredMap<ReverseExprArgs> = {
    ...super.argTypes,
  };

  declare args: ReverseExprArgs;

  constructor (args: ReverseExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LengthExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    binary?: Expression;
    encoding?: Expression;
  },
]>;

export class LengthExpr extends FuncExpr {
  key = ExpressionKey.LENGTH;

  static _sqlNames = [
    'LENGTH',
    'LEN',
    'CHAR_LENGTH',
    'CHARACTER_LENGTH',
  ];

  /**
   * Defines the arguments (properties and child expressions) for Length expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<LengthExprArgs> = {
    ...super.argTypes,
    this: true,
    binary: false,
    encoding: false,
  };

  declare args: LengthExprArgs;

  constructor (args: LengthExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $binary (): Expression | undefined {
    return this.args.binary;
  }

  get $encoding (): Expression | undefined {
    return this.args.encoding;
  }

  static {
    this.register();
  }
}

export type RtrimmedLengthExprArgs = Merge<[
  FuncExprArgs,
]>;

export class RtrimmedLengthExpr extends FuncExpr {
  key = ExpressionKey.RTRIMMED_LENGTH;

  static argTypes: RequiredMap<RtrimmedLengthExprArgs> = {
    ...super.argTypes,
  };

  declare args: RtrimmedLengthExprArgs;

  constructor (args: RtrimmedLengthExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitLengthExprArgs = Merge<[
  FuncExprArgs,
]>;

export class BitLengthExpr extends FuncExpr {
  key = ExpressionKey.BIT_LENGTH;

  static argTypes: RequiredMap<BitLengthExprArgs> = {
    ...super.argTypes,
  };

  declare args: BitLengthExprArgs;

  constructor (args: BitLengthExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LevenshteinExprArgs = Merge<[
  FuncExprArgs,
  {
    insCost?: Expression;
    delCost?: Expression;
    subCost?: Expression;
    maxDist?: Expression;
  },
]>;

export class LevenshteinExpr extends FuncExpr {
  key = ExpressionKey.LEVENSHTEIN;

  /**
   * Defines the arguments (properties and child expressions) for Levenshtein expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<LevenshteinExprArgs> = {
    ...super.argTypes,
    insCost: false,
    delCost: false,
    subCost: false,
    maxDist: false,
  };

  declare args: LevenshteinExprArgs;

  constructor (args: LevenshteinExprArgs) {
    super(args);
  }

  get $insCost (): Expression | undefined {
    return this.args.insCost;
  }

  get $delCost (): Expression | undefined {
    return this.args.delCost;
  }

  get $subCost (): Expression | undefined {
    return this.args.subCost;
  }

  get $maxDist (): Expression | undefined {
    return this.args.maxDist;
  }

  static {
    this.register();
  }
}

export type LnExprArgs = Merge<[
  FuncExprArgs,
]>;

export class LnExpr extends FuncExpr {
  key = ExpressionKey.LN;

  static argTypes: RequiredMap<LnExprArgs> = {
    ...super.argTypes,
  };

  declare args: LnExprArgs;

  constructor (args: LnExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LogExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class LogExpr extends FuncExpr {
  key = ExpressionKey.LOG;

  static argTypes: RequiredMap<LogExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: LogExprArgs;

  constructor (args: LogExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type LowerExprArgs = Merge<[
  FuncExprArgs,
]>;

export class LowerExpr extends FuncExpr {
  key = ExpressionKey.LOWER;

  static argTypes: RequiredMap<LowerExprArgs> = {
    ...super.argTypes,
  };

  declare args: LowerExprArgs;

  constructor (args: LowerExprArgs) {
    super(args);
  }

  static _sqlNames = ['LOWER', 'LCASE'];

  static {
    this.register();
  }
}

export type MapExprArgs = Merge<[
  FuncExprArgs,
  {
    keys?: Expression[];
    values?: Expression[];
  },
]>;

export class MapExpr extends FuncExpr {
  key = ExpressionKey.MAP;

  /**
   * Defines the arguments (properties and child expressions) for Map expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<MapExprArgs> = {
    ...super.argTypes,
    keys: false,
    values: false,
  };

  declare args: MapExprArgs;

  static {
    this.register();
  }

  constructor (args: MapExprArgs) {
    super(args);
  }

  get $keys (): Expression[] | undefined {
    return this.args.keys;
  }

  get $values (): Expression[] | undefined {
    return this.args.values;
  }

  get keys (): (string | number | boolean | Token | Expression)[] {
    const keysArg = this.args.keys;
    return keysArg?.[0]?.args?.expressions || [];
  }

  get values (): (string | number | boolean | Token | Expression)[] {
    const valuesArg = this.args.values;
    return valuesArg?.[0]?.args?.expressions || [];
  }
}

export type ToMapExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ToMapExpr extends FuncExpr {
  key = ExpressionKey.TO_MAP;

  static argTypes: RequiredMap<ToMapExprArgs> = {
    ...super.argTypes,
  };

  declare args: ToMapExprArgs;

  constructor (args: ToMapExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MapFromEntriesExprArgs = Merge<[
  FuncExprArgs,
]>;

export class MapFromEntriesExpr extends FuncExpr {
  key = ExpressionKey.MAP_FROM_ENTRIES;

  static argTypes: RequiredMap<MapFromEntriesExprArgs> = {
    ...super.argTypes,
  };

  declare args: MapFromEntriesExprArgs;

  constructor (args: MapFromEntriesExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MapCatExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class MapCatExpr extends FuncExpr {
  key = ExpressionKey.MAP_CAT;

  static argTypes: RequiredMap<MapCatExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: MapCatExprArgs;

  constructor (args: MapCatExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type MapContainsKeyExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    key: Expression;
  },
]>;

export class MapContainsKeyExpr extends FuncExpr {
  key = ExpressionKey.MAP_CONTAINS_KEY;

  static argTypes: RequiredMap<MapContainsKeyExprArgs> = {
    ...super.argTypes,
    this: true,
    key: true,
  };

  declare args: MapContainsKeyExprArgs;

  constructor (args: MapContainsKeyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $key (): Expression {
    return this.args.key;
  }

  static {
    this.register();
  }
}

export type MapDeleteExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions: Expression[];
  },
]>;

export class MapDeleteExpr extends FuncExpr {
  key = ExpressionKey.MAP_DELETE;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<MapDeleteExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
  };

  declare args: MapDeleteExprArgs;

  constructor (args: MapDeleteExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type MapInsertExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    key?: Expression;
    value: string;
    updateFlag?: Expression;
  },
]>;

export class MapInsertExpr extends FuncExpr {
  key = ExpressionKey.MAP_INSERT;

  /**
   * Defines the arguments (properties and child expressions) for MapInsert expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<MapInsertExprArgs> = {
    ...super.argTypes,
    this: true,
    key: false,
    value: true,
    updateFlag: false,
  };

  declare args: MapInsertExprArgs;

  constructor (args: MapInsertExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $key (): Expression | undefined {
    return this.args.key;
  }

  get $value (): string {
    return this.args.value;
  }

  get $updateFlag (): Expression | undefined {
    return this.args.updateFlag;
  }

  static {
    this.register();
  }
}

export type MapKeysExprArgs = Merge<[
  FuncExprArgs,
]>;

export class MapKeysExpr extends FuncExpr {
  key = ExpressionKey.MAP_KEYS;

  static argTypes: RequiredMap<MapKeysExprArgs> = {
    ...super.argTypes,
  };

  declare args: MapKeysExprArgs;

  constructor (args: MapKeysExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MapPickExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions: Expression[];
  },
]>;

export class MapPickExpr extends FuncExpr {
  key = ExpressionKey.MAP_PICK;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<MapPickExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
  };

  declare args: MapPickExprArgs;

  constructor (args: MapPickExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type MapSizeExprArgs = Merge<[
  FuncExprArgs,
]>;

export class MapSizeExpr extends FuncExpr {
  key = ExpressionKey.MAP_SIZE;

  static argTypes: RequiredMap<MapSizeExprArgs> = {
    ...super.argTypes,
  };

  declare args: MapSizeExprArgs;

  constructor (args: MapSizeExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StarMapExprArgs = Merge<[
  FuncExprArgs,
]>;

export class StarMapExpr extends FuncExpr {
  key = ExpressionKey.STAR_MAP;

  static argTypes: RequiredMap<StarMapExprArgs> = {
    ...super.argTypes,
  };

  declare args: StarMapExprArgs;

  constructor (args: StarMapExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type VarMapExprArgs = Merge<[
  FuncExprArgs,
  {
    keys: ArrayExpr;
    values: ArrayExpr;
  },
]>;

export class VarMapExpr extends FuncExpr {
  key = ExpressionKey.VAR_MAP;

  static isVarLenArgs = true;
  /**
   * Defines the arguments (properties and child expressions) for VarMap expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<VarMapExprArgs> = {
    ...super.argTypes,
    keys: true,
    values: true,
  };

  declare args: VarMapExprArgs;

  constructor (args: VarMapExprArgs) {
    super(args);
  }

  get $keys (): ArrayExpr {
    return this.args.keys;
  }

  get $values (): ArrayExpr {
    return this.args.values;
  }

  get keys (): (string | number | boolean | Token | Expression)[] {
    const keysArg = this.args.keys;
    return keysArg.expressions || [];
  }

  get values (): (string | number | boolean | Token | Expression)[] {
    const valuesArg = this.args.values;
    return valuesArg.expressions || [];
  }

  static {
    this.register();
  }
}

export type MatchAgainstExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions: Expression[];
    modifier?: Expression;
  },
]>;

export class MatchAgainstExpr extends FuncExpr {
  key = ExpressionKey.MATCH_AGAINST;

  static argTypes: RequiredMap<MatchAgainstExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
    modifier: false,
  };

  declare args: MatchAgainstExprArgs;

  constructor (args: MatchAgainstExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $modifier (): Expression | undefined {
    return this.args.modifier;
  }

  static {
    this.register();
  }
}

export type MD5ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class MD5Expr extends FuncExpr {
  key = ExpressionKey.MD5;

  static _sqlNames = ['MD5'];

  static argTypes: RequiredMap<MD5ExprArgs> = {
    ...super.argTypes,
  };

  declare args: MD5ExprArgs;

  constructor (args: MD5ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MD5DigestExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class MD5DigestExpr extends FuncExpr {
  key = ExpressionKey.MD5_DIGEST;

  static isVarLenArgs = true;
  static _sqlNames = ['MD5_DIGEST'];

  static argTypes: RequiredMap<MD5DigestExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
  };

  declare args: MD5DigestExprArgs;

  constructor (args: MD5DigestExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type MD5NumberLower64ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class MD5NumberLower64Expr extends FuncExpr {
  key = ExpressionKey.MD5_NUMBER_LOWER64;

  static argTypes: RequiredMap<MD5NumberLower64ExprArgs> = {
    ...super.argTypes,
  };

  declare args: MD5NumberLower64ExprArgs;

  constructor (args: MD5NumberLower64ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MD5NumberUpper64ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class MD5NumberUpper64Expr extends FuncExpr {
  key = ExpressionKey.MD5_NUMBER_UPPER64;

  static argTypes: RequiredMap<MD5NumberUpper64ExprArgs> = {
    ...super.argTypes,
  };

  declare args: MD5NumberUpper64ExprArgs;

  constructor (args: MD5NumberUpper64ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MonthExprArgs = Merge<[
  FuncExprArgs,
]>;

export class MonthExpr extends FuncExpr {
  key = ExpressionKey.MONTH;

  static argTypes: RequiredMap<MonthExprArgs> = {
    ...super.argTypes,
  };

  declare args: MonthExprArgs;

  constructor (args: MonthExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MonthnameExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    abbreviated?: Expression;
  },
]>;

export class MonthnameExpr extends FuncExpr {
  key = ExpressionKey.MONTHNAME;

  static argTypes: RequiredMap<MonthnameExprArgs> = {
    ...super.argTypes,
    this: true,
    abbreviated: false,
  };

  declare args: MonthnameExprArgs;

  constructor (args: MonthnameExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $abbreviated (): Expression | undefined {
    return this.args.abbreviated;
  }

  static {
    this.register();
  }
}

export type AddMonthsExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    preserveEndOfMonth?: Expression;
  },
]>;

export class AddMonthsExpr extends FuncExpr {
  key = ExpressionKey.ADD_MONTHS;

  static argTypes: RequiredMap<AddMonthsExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    preserveEndOfMonth: false,
  };

  declare args: AddMonthsExprArgs;

  constructor (args: AddMonthsExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $preserveEndOfMonth (): Expression | undefined {
    return this.args.preserveEndOfMonth;
  }

  static {
    this.register();
  }
}

export type Nvl2ExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    true: Expression;
    false?: Expression;
  },
]>;

export class Nvl2Expr extends FuncExpr {
  key = ExpressionKey.NVL2;

  /**
   * Defines the arguments (properties and child expressions) for Nvl2 expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<Nvl2ExprArgs> = {
    ...super.argTypes,
    this: true,
    true: true,
    false: false,
  };

  declare args: Nvl2ExprArgs;

  constructor (args: Nvl2ExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $true (): Expression {
    return this.args.true;
  }

  get $false (): Expression | undefined {
    return this.args.false;
  }

  static {
    this.register();
  }
}

export type NormalizeExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    form?: Expression;
    isCasefold?: Expression;
  },
]>;

export class NormalizeExpr extends FuncExpr {
  key = ExpressionKey.NORMALIZE;

  /**
   * Defines the arguments (properties and child expressions) for Normalize expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<NormalizeExprArgs> = {
    ...super.argTypes,
    this: true,
    form: false,
    isCasefold: false,
  };

  declare args: NormalizeExprArgs;

  constructor (args: NormalizeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $form (): Expression | undefined {
    return this.args.form;
  }

  get $isCasefold (): Expression | undefined {
    return this.args.isCasefold;
  }

  static {
    this.register();
  }
}

export type NormalExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    stddev: Expression;
    gen: Expression;
  },
]>;

export class NormalExpr extends FuncExpr {
  key = ExpressionKey.NORMAL;

  /**
   * Defines the arguments (properties and child expressions) for Normal expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<NormalExprArgs> = {
    ...super.argTypes,
    this: true,
    stddev: true,
    gen: true,
  };

  declare args: NormalExprArgs;

  constructor (args: NormalExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $stddev (): Expression {
    return this.args.stddev;
  }

  get $gen (): Expression {
    return this.args.gen;
  }

  static {
    this.register();
  }
}

export type NetFuncExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class NetFuncExpr extends FuncExpr {
  key = ExpressionKey.NET_FUNC;

  static argTypes: RequiredMap<NetFuncExprArgs> = {
    ...super.argTypes,
  };

  declare args: NetFuncExprArgs;

  constructor (args: NetFuncExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type HostExprArgs = Merge<[
  FuncExprArgs,
]>;

export class HostExpr extends FuncExpr {
  key = ExpressionKey.HOST;

  static argTypes: RequiredMap<HostExprArgs> = {
    ...super.argTypes,
  };

  declare args: HostExprArgs;

  constructor (args: HostExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegDomainExprArgs = Merge<[
  FuncExprArgs,
]>;

export class RegDomainExpr extends FuncExpr {
  key = ExpressionKey.REG_DOMAIN;

  static argTypes: RequiredMap<RegDomainExprArgs> = {
    ...super.argTypes,
  };

  declare args: RegDomainExprArgs;

  constructor (args: RegDomainExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type OverlayExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    from: Expression;
    for?: Expression;
  },
]>;

export class OverlayExpr extends FuncExpr {
  key = ExpressionKey.OVERLAY;

  /**
   * Defines the arguments (properties and child expressions) for Overlay expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<OverlayExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    from: true,
    for: false,
  };

  declare args: OverlayExprArgs;

  constructor (args: OverlayExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $from (): Expression {
    return this.args.from;
  }

  get $for (): Expression | undefined {
    return this.args.for;
  }

  static {
    this.register();
  }
}

export type PredictExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    paramsStruct?: Expression;
  },
]>;

export class PredictExpr extends FuncExpr {
  key = ExpressionKey.PREDICT;

  static argTypes: RequiredMap<PredictExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    paramsStruct: false,
  };

  declare args: PredictExprArgs;

  constructor (args: PredictExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $paramsStruct (): Expression | undefined {
    return this.args.paramsStruct;
  }

  static {
    this.register();
  }
}

export type MLTranslateExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    paramsStruct: Expression;
  },
]>;

export class MLTranslateExpr extends FuncExpr {
  key = ExpressionKey.ML_TRANSLATE;

  static argTypes: RequiredMap<MLTranslateExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    paramsStruct: true,
  };

  declare args: MLTranslateExprArgs;

  constructor (args: MLTranslateExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $paramsStruct (): Expression {
    return this.args.paramsStruct;
  }

  static {
    this.register();
  }
}

export type FeaturesAtTimeExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    time?: Expression;
    numRows?: Expression[];
    ignoreFeatureNulls?: Expression[];
  },
]>;

export class FeaturesAtTimeExpr extends FuncExpr {
  key = ExpressionKey.FEATURES_AT_TIME;

  /**
   * Defines the arguments (properties and child expressions) for FeaturesAtTime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<FeaturesAtTimeExprArgs> = {
    ...super.argTypes,
    this: true,
    time: false,
    numRows: false,
    ignoreFeatureNulls: false,
  };

  declare args: FeaturesAtTimeExprArgs;

  constructor (args: FeaturesAtTimeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $time (): Expression | undefined {
    return this.args.time;
  }

  get $numRows (): Expression[] | undefined {
    return this.args.numRows;
  }

  get $ignoreFeatureNulls (): Expression[] | undefined {
    return this.args.ignoreFeatureNulls;
  }

  static {
    this.register();
  }
}

export type GenerateEmbeddingExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    paramsStruct?: Expression;
    isText?: string;
  },
]>;

export class GenerateEmbeddingExpr extends FuncExpr {
  key = ExpressionKey.GENERATE_EMBEDDING;

  /**
   * Defines the arguments (properties and child expressions) for GenerateEmbedding expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<GenerateEmbeddingExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    paramsStruct: false,
    isText: false,
  };

  declare args: GenerateEmbeddingExprArgs;

  constructor (args: GenerateEmbeddingExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $paramsStruct (): Expression | undefined {
    return this.args.paramsStruct;
  }

  get $isText (): string | undefined {
    return this.args.isText;
  }

  static {
    this.register();
  }
}

export type MLForecastExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
    paramsStruct?: Expression;
  },
]>;

export class MLForecastExpr extends FuncExpr {
  key = ExpressionKey.ML_FORECAST;

  static argTypes: RequiredMap<MLForecastExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
    paramsStruct: false,
  };

  declare args: MLForecastExprArgs;

  constructor (args: MLForecastExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $paramsStruct (): Expression | undefined {
    return this.args.paramsStruct;
  }

  static {
    this.register();
  }
}

export type VectorSearchExprArgs = Merge<[
  FuncExprArgs,
  {
    columnToSearch: Expression;
    queryTable: Expression;
    queryColumnToSearch?: Expression;
    topK?: Expression;
    distanceType?: DataTypeExpr;
    options?: Expression[];
  },
]>;

export class VectorSearchExpr extends FuncExpr {
  key = ExpressionKey.VECTOR_SEARCH;

  /**
   * Defines the arguments (properties and child expressions) for VectorSearch expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<VectorSearchExprArgs> = {
    ...super.argTypes,
    columnToSearch: true,
    queryTable: true,
    queryColumnToSearch: false,
    topK: false,
    distanceType: false,
    options: false,
  };

  declare args: VectorSearchExprArgs;

  constructor (args: VectorSearchExprArgs) {
    super(args);
  }

  get $columnToSearch (): Expression {
    return this.args.columnToSearch;
  }

  get $queryTable (): Expression {
    return this.args.queryTable;
  }

  get $queryColumnToSearch (): Expression | undefined {
    return this.args.queryColumnToSearch;
  }

  get $topK (): Expression | undefined {
    return this.args.topK;
  }

  get $distanceType (): DataTypeExpr | undefined {
    return this.args.distanceType;
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }

  static {
    this.register();
  }
}

export type PiExprArgs = Merge<[
  FuncExprArgs,
]>;

export class PiExpr extends FuncExpr {
  key = ExpressionKey.PI;

  static argTypes: RequiredMap<PiExprArgs> = {
    ...super.argTypes,
  };

  declare args: PiExprArgs;

  constructor (args: PiExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type PowExprArgs = Merge<[
  BinaryExprArgs,
]>;
export class PowExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.POW;

  static _sqlNames = ['POWER', 'POW'];

  static argTypes: RequiredMap<PowExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: PowExprArgs;

  constructor (args: PowExprArgs) {
    super(args);
  }
}

export type ApproxPercentileEstimateExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    percentile: Expression;
  },
]>;

export class ApproxPercentileEstimateExpr extends FuncExpr {
  key = ExpressionKey.APPROX_PERCENTILE_ESTIMATE;

  static argTypes: RequiredMap<ApproxPercentileEstimateExprArgs> = {
    ...super.argTypes,
    this: true,
    percentile: true,
  };

  declare args: ApproxPercentileEstimateExprArgs;

  constructor (args: ApproxPercentileEstimateExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $percentile (): Expression {
    return this.args.percentile;
  }

  static {
    this.register();
  }
}

export type QuarterExprArgs = Merge<[
  FuncExprArgs,
]>;

export class QuarterExpr extends FuncExpr {
  key = ExpressionKey.QUARTER;

  static argTypes: RequiredMap<QuarterExprArgs> = {
    ...super.argTypes,
  };

  declare args: QuarterExprArgs;

  constructor (args: QuarterExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RandExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    lower?: Expression;
    upper?: Expression;
  },
]>;

export class RandExpr extends FuncExpr {
  key = ExpressionKey.RAND;

  static _sqlNames = ['RAND', 'RANDOM'];

  /**
   * Defines the arguments (properties and child expressions) for Rand expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<RandExprArgs> = {
    ...super.argTypes,
    this: false,
    lower: false,
    upper: false,
  };

  declare args: RandExprArgs;

  constructor (args: RandExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $lower (): Expression | undefined {
    return this.args.lower;
  }

  get $upper (): Expression | undefined {
    return this.args.upper;
  }

  static {
    this.register();
  }
}

export type RandnExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class RandnExpr extends FuncExpr {
  key = ExpressionKey.RANDN;

  static argTypes: RequiredMap<RandnExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: RandnExprArgs;

  constructor (args: RandnExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type RandstrExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    generator?: Expression;
  },
]>;

export class RandstrExpr extends FuncExpr {
  key = ExpressionKey.RANDSTR;

  static argTypes: RequiredMap<RandstrExprArgs> = {
    ...super.argTypes,
    this: true,
    generator: false,
  };

  declare args: RandstrExprArgs;

  constructor (args: RandstrExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $generator (): Expression | undefined {
    return this.args.generator;
  }

  static {
    this.register();
  }
}

export type RangeNExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions: Expression[];
    each?: Expression;
  },
]>;

export class RangeNExpr extends FuncExpr {
  key = ExpressionKey.RANGE_N;

  static argTypes: RequiredMap<RangeNExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
    each: false,
  };

  declare args: RangeNExprArgs;

  constructor (args: RangeNExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $each (): Expression | undefined {
    return this.args.each;
  }

  static {
    this.register();
  }
}

export type RangeBucketExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class RangeBucketExpr extends FuncExpr {
  key = ExpressionKey.RANGE_BUCKET;

  static argTypes: RequiredMap<RangeBucketExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: RangeBucketExprArgs;

  constructor (args: RangeBucketExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ReadCSVExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class ReadCSVExpr extends FuncExpr {
  key = ExpressionKey.READ_CSV;

  static isVarLenArgs = true;
  static _sqlNames = ['READ_CSV'];

  static argTypes: RequiredMap<ReadCSVExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
  };

  declare args: ReadCSVExprArgs;

  constructor (args: ReadCSVExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type ReadParquetExprArgs = Merge<[
  FuncExprArgs,
  { expressions: Expression[] },
]>;

export class ReadParquetExpr extends FuncExpr {
  key = ExpressionKey.READ_PARQUET;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<ReadParquetExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: ReadParquetExprArgs;

  constructor (args: ReadParquetExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type ReduceExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    initial: Expression;
    merge: Expression;
    finish?: Expression;
  },
]>;

export class ReduceExpr extends FuncExpr {
  key = ExpressionKey.REDUCE;

  /**
   * Defines the arguments (properties and child expressions) for Reduce expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ReduceExprArgs> = {
    ...super.argTypes,
    this: true,
    initial: true,
    merge: true,
    finish: false,
  };

  declare args: ReduceExprArgs;

  constructor (args: ReduceExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $initial (): Expression {
    return this.args.initial;
  }

  get $merge (): Expression {
    return this.args.merge;
  }

  get $finish (): Expression | undefined {
    return this.args.finish;
  }

  static {
    this.register();
  }
}

export type RegexpExtractExprArgs = Merge<[
  FuncExprArgs,
  {
    position?: Expression;
    occurrence?: Expression;
    parameters?: Expression[];
    group?: Expression;
    nullIfPosOverflow?: Expression;
  },
]>;

export class RegexpExtractExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_EXTRACT;

  /**
   * Defines the arguments (properties and child expressions) for RegexpExtract expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<RegexpExtractExprArgs> = {
    ...super.argTypes,
    position: false,
    occurrence: false,
    parameters: false,
    group: false,
    nullIfPosOverflow: false,
  };

  declare args: RegexpExtractExprArgs;

  constructor (args: RegexpExtractExprArgs) {
    super(args);
  }

  get $position (): Expression | undefined {
    return this.args.position;
  }

  get $occurrence (): Expression | undefined {
    return this.args.occurrence;
  }

  get $parameters (): Expression[] | undefined {
    return this.args.parameters;
  }

  get $group (): Expression | undefined {
    return this.args.group;
  }

  get $nullIfPosOverflow (): Expression | undefined {
    return this.args.nullIfPosOverflow;
  }

  static {
    this.register();
  }
}

export type RegexpExtractAllExprArgs = Merge<[
  FuncExprArgs,
  {
    group?: Expression;
    parameters?: Expression[];
    position?: Expression;
    occurrence?: Expression;
  },
]>;

export class RegexpExtractAllExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_EXTRACT_ALL;

  /**
   * Defines the arguments (properties and child expressions) for RegexpExtractAll expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<RegexpExtractAllExprArgs> = {
    ...super.argTypes,
    group: false,
    parameters: false,
    position: false,
    occurrence: false,
  };

  declare args: RegexpExtractAllExprArgs;

  constructor (args: RegexpExtractAllExprArgs) {
    super(args);
  }

  get $group (): Expression | undefined {
    return this.args.group;
  }

  get $parameters (): Expression[] | undefined {
    return this.args.parameters;
  }

  get $position (): Expression | undefined {
    return this.args.position;
  }

  get $occurrence (): Expression | undefined {
    return this.args.occurrence;
  }

  static {
    this.register();
  }
}

export type RegexpReplaceExprArgs = Merge<[
  FuncExprArgs,
  {
    replacement?: boolean;
    position?: Expression;
    occurrence?: Expression;
    modifiers?: Expression[];
    singleReplace?: Expression;
  },
]>;

export class RegexpReplaceExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_REPLACE;

  /**
   * Defines the arguments (properties and child expressions) for RegexpReplace expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<RegexpReplaceExprArgs> = {
    ...super.argTypes,
    replacement: false,
    position: false,
    occurrence: false,
    modifiers: false,
    singleReplace: false,
  };

  declare args: RegexpReplaceExprArgs;

  constructor (args: RegexpReplaceExprArgs) {
    super(args);
  }

  get $replacement (): boolean | undefined {
    return this.args.replacement;
  }

  get $position (): Expression | undefined {
    return this.args.position;
  }

  get $occurrence (): Expression | undefined {
    return this.args.occurrence;
  }

  get $modifiers (): Expression[] | undefined {
    return this.args.modifiers;
  }

  get $singleReplace (): Expression | undefined {
    return this.args.singleReplace;
  }

  static {
    this.register();
  }
}

export type RegexpLikeExprArgs = Merge<[
  BinaryExprArgs,
  {
    this: Expression;
    expression: Expression;
    flag?: Expression;
  },
]>;

export class RegexpLikeExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.REGEXP_LIKE;

  static argTypes: RequiredMap<RegexpLikeExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    flag: false,
  };

  declare args: RegexpLikeExprArgs;

  constructor (args: RegexpLikeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $flag (): Expression | undefined {
    return this.args.flag;
  }
}

export type RegexpILikeExprArgs = Merge<[
  BinaryExprArgs,
  {
    this: Expression;
    expression: Expression;
    flag?: Expression;
  },
]>;

export class RegexpILikeExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.REGEXP_ILIKE;

  static argTypes: RequiredMap<RegexpILikeExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    flag: false,
  };

  declare args: RegexpILikeExprArgs;

  constructor (args: RegexpILikeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $flag (): Expression | undefined {
    return this.args.flag;
  }
}

export type RegexpFullMatchExprArgs = Merge<[
  BinaryExprArgs,
  {
    this: Expression;
    expression: Expression;
    options?: Expression[];
  },
]>;

export class RegexpFullMatchExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.REGEXP_FULL_MATCH;

  static argTypes: RequiredMap<RegexpFullMatchExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    options: false,
  };

  declare args: RegexpFullMatchExprArgs;

  constructor (args: RegexpFullMatchExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }
}

export type RegexpInstrExprArgs = Merge<[
  FuncExprArgs,
  {
    position?: Expression;
    occurrence?: Expression;
    option?: Expression;
    parameters?: Expression[];
    group?: Expression;
  },
]>;

export class RegexpInstrExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_INSTR;

  /**
   * Defines the arguments (properties and child expressions) for RegexpInstr expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<RegexpInstrExprArgs> = {
    ...super.argTypes,
    position: false,
    occurrence: false,
    option: false,
    parameters: false,
    group: false,
  };

  declare args: RegexpInstrExprArgs;

  constructor (args: RegexpInstrExprArgs) {
    super(args);
  }

  get $position (): Expression | undefined {
    return this.args.position;
  }

  get $occurrence (): Expression | undefined {
    return this.args.occurrence;
  }

  get $option (): Expression | undefined {
    return this.args.option;
  }

  get $parameters (): Expression[] | undefined {
    return this.args.parameters;
  }

  get $group (): Expression | undefined {
    return this.args.group;
  }

  static {
    this.register();
  }
}

export type RegexpSplitExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    limit?: number | Expression;
  },
]>;

export class RegexpSplitExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_SPLIT;

  static argTypes: RequiredMap<RegexpSplitExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    limit: false,
  };

  declare args: RegexpSplitExprArgs;

  constructor (args: RegexpSplitExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $limit (): number | Expression | undefined {
    return this.args.limit;
  }

  static {
    this.register();
  }
}

export type RegexpCountExprArgs = Merge<[
  FuncExprArgs,
  {
    position?: Expression;
    parameters?: Expression[];
  },
]>;

export class RegexpCountExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_COUNT;

  /**
   * Defines the arguments (properties and child expressions) for RegexpCount expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<RegexpCountExprArgs> = {
    ...super.argTypes,
    position: false,
    parameters: false,
  };

  declare args: RegexpCountExprArgs;

  constructor (args: RegexpCountExprArgs) {
    super(args);
  }

  get $position (): Expression | undefined {
    return this.args.position;
  }

  get $parameters (): Expression[] | undefined {
    return this.args.parameters;
  }

  static {
    this.register();
  }
}

export type RepeatExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    times: Expression[];
  },
]>;

export class RepeatExpr extends FuncExpr {
  key = ExpressionKey.REPEAT;

  static argTypes: RequiredMap<RepeatExprArgs> = {
    ...super.argTypes,
    this: true,
    times: true,
  };

  declare args: RepeatExprArgs;

  constructor (args: RepeatExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $times (): Expression[] {
    return this.args.times;
  }

  static {
    this.register();
  }
}

export type ReplaceExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    replacement?: boolean;
  },
]>;

export class ReplaceExpr extends FuncExpr {
  key = ExpressionKey.REPLACE;

  static argTypes: RequiredMap<ReplaceExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    replacement: false,
  };

  declare args: ReplaceExprArgs;

  constructor (args: ReplaceExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $replacement (): boolean | undefined {
    return this.args.replacement;
  }

  static {
    this.register();
  }
}

export type RadiansExprArgs = Merge<[
  FuncExprArgs,
]>;

export class RadiansExpr extends FuncExpr {
  key = ExpressionKey.RADIANS;

  static argTypes: RequiredMap<RadiansExprArgs> = {
    ...super.argTypes,
  };

  declare args: RadiansExprArgs;

  constructor (args: RadiansExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RoundExprArgs = Merge<[
  FuncExprArgs,
  {
    decimals?: Expression[];
    truncate?: Expression;
    castsNonIntegerDecimals?: Expression[];
  },
]>;

export class RoundExpr extends FuncExpr {
  key = ExpressionKey.ROUND;

  /**
   * Defines the arguments (properties and child expressions) for Round expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<RoundExprArgs> = {
    ...super.argTypes,
    decimals: false,
    truncate: false,
    castsNonIntegerDecimals: false,
  };

  declare args: RoundExprArgs;

  constructor (args: RoundExprArgs) {
    super(args);
  }

  get $decimals (): Expression[] | undefined {
    return this.args.decimals;
  }

  get $truncate (): Expression | undefined {
    return this.args.truncate;
  }

  get $castsNonIntegerDecimals (): Expression[] | undefined {
    return this.args.castsNonIntegerDecimals;
  }

  static {
    this.register();
  }
}

export type TruncExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    decimals?: Expression;
  },
]>;

export class TruncExpr extends FuncExpr {
  key = ExpressionKey.TRUNC;

  static _sqlNames = ['TRUNC', 'TRUNCATE'];

  static argTypes: RequiredMap<TruncExprArgs> = {
    ...super.argTypes,
    this: true,
    decimals: false,
  };

  declare args: TruncExprArgs;

  constructor (args: TruncExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $decimals (): Expression | undefined {
    return this.args.decimals;
  }

  static {
    this.register();
  }
}

export type RowNumberExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class RowNumberExpr extends FuncExpr {
  key = ExpressionKey.ROW_NUMBER;

  static argTypes: RequiredMap<RowNumberExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: RowNumberExprArgs;

  constructor (args: RowNumberExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type Seq1ExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class Seq1Expr extends FuncExpr {
  key = ExpressionKey.SEQ1;

  static argTypes: RequiredMap<Seq1ExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: Seq1ExprArgs;

  constructor (args: Seq1ExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type Seq2ExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class Seq2Expr extends FuncExpr {
  key = ExpressionKey.SEQ2;

  static argTypes: RequiredMap<Seq2ExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: Seq2ExprArgs;

  constructor (args: Seq2ExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type Seq4ExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class Seq4Expr extends FuncExpr {
  key = ExpressionKey.SEQ4;

  static argTypes: RequiredMap<Seq4ExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: Seq4ExprArgs;

  constructor (args: Seq4ExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type Seq8ExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class Seq8Expr extends FuncExpr {
  key = ExpressionKey.SEQ8;

  static argTypes: RequiredMap<Seq8ExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: Seq8ExprArgs;

  constructor (args: Seq8ExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type SafeAddExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class SafeAddExpr extends FuncExpr {
  key = ExpressionKey.SAFE_ADD;

  static argTypes: RequiredMap<SafeAddExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: SafeAddExprArgs;

  constructor (args: SafeAddExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type SafeDivideExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class SafeDivideExpr extends FuncExpr {
  key = ExpressionKey.SAFE_DIVIDE;

  static argTypes: RequiredMap<SafeDivideExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: SafeDivideExprArgs;

  constructor (args: SafeDivideExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type SafeMultiplyExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class SafeMultiplyExpr extends FuncExpr {
  key = ExpressionKey.SAFE_MULTIPLY;

  static argTypes: RequiredMap<SafeMultiplyExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: SafeMultiplyExprArgs;

  constructor (args: SafeMultiplyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type SafeNegateExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SafeNegateExpr extends FuncExpr {
  key = ExpressionKey.SAFE_NEGATE;

  static argTypes: RequiredMap<SafeNegateExprArgs> = {
    ...super.argTypes,
  };

  declare args: SafeNegateExprArgs;

  constructor (args: SafeNegateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SafeSubtractExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class SafeSubtractExpr extends FuncExpr {
  key = ExpressionKey.SAFE_SUBTRACT;

  static argTypes: RequiredMap<SafeSubtractExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: SafeSubtractExprArgs;

  constructor (args: SafeSubtractExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type SafeConvertBytesToStringExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SafeConvertBytesToStringExpr extends FuncExpr {
  key = ExpressionKey.SAFE_CONVERT_BYTES_TO_STRING;

  static argTypes: RequiredMap<SafeConvertBytesToStringExprArgs> = {
    ...super.argTypes,
  };

  declare args: SafeConvertBytesToStringExprArgs;

  constructor (args: SafeConvertBytesToStringExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SHAExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SHAExpr extends FuncExpr {
  key = ExpressionKey.SHA;

  static argTypes: RequiredMap<SHAExprArgs> = {
    ...super.argTypes,
  };

  declare args: SHAExprArgs;

  constructor (args: SHAExprArgs) {
    super(args);
  }

  static _sqlNames = ['SHA', 'SHA1'];

  static {
    this.register();
  }
}

export type SHA2ExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    length?: number | Expression;
  },
]>;

export class SHA2Expr extends FuncExpr {
  key = ExpressionKey.SHA2;

  static _sqlNames = ['SHA2'];

  static argTypes: RequiredMap<SHA2ExprArgs> = {
    ...super.argTypes,
    this: true,
    length: false,
  };

  declare args: SHA2ExprArgs;

  static {
    this.register();
  }

  constructor (args: SHA2ExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $length (): number | Expression | undefined {
    return this.args.length;
  }
}

export type SHA1DigestExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SHA1DigestExpr extends FuncExpr {
  key = ExpressionKey.SHA1_DIGEST;

  static argTypes: RequiredMap<SHA1DigestExprArgs> = {
    ...super.argTypes,
  };

  declare args: SHA1DigestExprArgs;

  constructor (args: SHA1DigestExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SHA2DigestExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    length?: number | Expression;
  },
]>;

export class SHA2DigestExpr extends FuncExpr {
  key = ExpressionKey.SHA2_DIGEST;

  static argTypes: RequiredMap<SHA2DigestExprArgs> = {
    ...super.argTypes,
    this: true,
    length: false,
  };

  declare args: SHA2DigestExprArgs;

  constructor (args: SHA2DigestExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $length (): number | Expression | undefined {
    return this.args.length;
  }

  static {
    this.register();
  }
}

export type SignExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SignExpr extends FuncExpr {
  key = ExpressionKey.SIGN;

  static argTypes: RequiredMap<SignExprArgs> = {
    ...super.argTypes,
  };

  declare args: SignExprArgs;

  constructor (args: SignExprArgs) {
    super(args);
  }

  static _sqlNames = ['SIGN', 'SIGNUM'];

  static {
    this.register();
  }
}

export type SortArrayExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    asc?: Expression;
    nullsFirst?: Expression;
  },
]>;

export class SortArrayExpr extends FuncExpr {
  key = ExpressionKey.SORT_ARRAY;

  /**
   * Defines the arguments (properties and child expressions) for SortArray expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<SortArrayExprArgs> = {
    ...super.argTypes,
    this: true,
    asc: false,
    nullsFirst: false,
  };

  declare args: SortArrayExprArgs;

  static {
    this.register();
  }

  constructor (args: SortArrayExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $asc (): Expression | undefined {
    return this.args.asc;
  }

  get $nullsFirst (): Expression | undefined {
    return this.args.nullsFirst;
  }
}

export type SoundexExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SoundexExpr extends FuncExpr {
  key = ExpressionKey.SOUNDEX;

  static argTypes: RequiredMap<SoundexExprArgs> = {
    ...super.argTypes,
  };

  declare args: SoundexExprArgs;

  constructor (args: SoundexExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SoundexP123ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SoundexP123Expr extends FuncExpr {
  key = ExpressionKey.SOUNDEX_P123;

  static argTypes: RequiredMap<SoundexP123ExprArgs> = {
    ...super.argTypes,
  };

  declare args: SoundexP123ExprArgs;

  constructor (args: SoundexP123ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SplitExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    limit?: number | Expression;
  },
]>;

export class SplitExpr extends FuncExpr {
  key = ExpressionKey.SPLIT;

  static argTypes: RequiredMap<SplitExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    limit: false,
  };

  declare args: SplitExprArgs;

  constructor (args: SplitExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $limit (): number | Expression | undefined {
    return this.args.limit;
  }

  static {
    this.register();
  }
}

export type SplitPartExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    delimiter?: number | Expression;
    partIndex?: Expression;
  },
]>;

export class SplitPartExpr extends FuncExpr {
  key = ExpressionKey.SPLIT_PART;

  /**
   * Defines the arguments (properties and child expressions) for SplitPart expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<SplitPartExprArgs> = {
    ...super.argTypes,
    this: true,
    delimiter: false,
    partIndex: false,
  };

  declare args: SplitPartExprArgs;

  constructor (args: SplitPartExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $delimiter (): number | Expression | undefined {
    return this.args.delimiter;
  }

  get $partIndex (): Expression | undefined {
    return this.args.partIndex;
  }

  static {
    this.register();
  }
}

export type SubstringExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    start?: Expression;
    length?: number | Expression;
  },
]>;

export class SubstringExpr extends FuncExpr {
  key = ExpressionKey.SUBSTRING;
  static _sqlNames = ['SUBSTRING', 'SUBSTR'];

  /**
   * Defines the arguments (properties and child expressions) for Substring expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<SubstringExprArgs> = {
    ...super.argTypes,
    this: true,
    start: false,
    length: false,
  };

  declare args: SubstringExprArgs;

  constructor (args: SubstringExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $start (): Expression | undefined {
    return this.args.start;
  }

  get $length (): number | Expression | undefined {
    return this.args.length;
  }

  static {
    this.register();
  }
}

export type SubstringIndexExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    delimiter: number | Expression;
    count: Expression;
  },
]>;

export class SubstringIndexExpr extends FuncExpr {
  key = ExpressionKey.SUBSTRING_INDEX;

  /**
   * Defines the arguments (properties and child expressions) for SubstringIndex expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<SubstringIndexExprArgs> = {
    ...super.argTypes,
    this: true,
    delimiter: true,
    count: true,
  };

  declare args: SubstringIndexExprArgs;

  constructor (args: SubstringIndexExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $delimiter (): number | Expression {
    return this.args.delimiter;
  }

  get $count (): Expression {
    return this.args.count;
  }

  static {
    this.register();
  }
}

export type StandardHashExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class StandardHashExpr extends FuncExpr {
  key = ExpressionKey.STANDARD_HASH;

  static argTypes: RequiredMap<StandardHashExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: StandardHashExprArgs;

  constructor (args: StandardHashExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type StartsWithExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class StartsWithExpr extends FuncExpr {
  key = ExpressionKey.STARTS_WITH;
  static _sqlNames = ['STARTS_WITH', 'STARTSWITH'];

  static argTypes: RequiredMap<StartsWithExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: StartsWithExprArgs;

  constructor (args: StartsWithExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type EndsWithExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class EndsWithExpr extends FuncExpr {
  key = ExpressionKey.ENDS_WITH;
  static _sqlNames = ['ENDS_WITH', 'ENDSWITH'];

  static argTypes: RequiredMap<EndsWithExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: EndsWithExprArgs;

  constructor (args: EndsWithExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type StrPositionExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    substr: Expression;
    position?: Expression;
    occurrence?: Expression;
  },
]>;

export class StrPositionExpr extends FuncExpr {
  key = ExpressionKey.STR_POSITION;

  /**
   * Defines the arguments (properties and child expressions) for StrPosition expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<StrPositionExprArgs> = {
    ...super.argTypes,
    this: true,
    substr: true,
    position: false,
    occurrence: false,
  };

  declare args: StrPositionExprArgs;

  constructor (args: StrPositionExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $substr (): Expression {
    return this.args.substr;
  }

  get $position (): Expression | undefined {
    return this.args.position;
  }

  get $occurrence (): Expression | undefined {
    return this.args.occurrence;
  }

  static {
    this.register();
  }
}

export type SearchExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    jsonScope?: Expression;
    analyzer?: Expression;
    analyzerOptions?: Expression[];
    searchMode?: Expression;
  },
]>;

export class SearchExpr extends FuncExpr {
  key = ExpressionKey.SEARCH;

  /**
   * Defines the arguments (properties and child expressions) for Search expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<SearchExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    jsonScope: false,
    analyzer: false,
    analyzerOptions: false,
    searchMode: false,
  };

  declare args: SearchExprArgs;

  constructor (args: SearchExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $jsonScope (): Expression | undefined {
    return this.args.jsonScope;
  }

  get $analyzer (): Expression | undefined {
    return this.args.analyzer;
  }

  get $analyzerOptions (): Expression[] | undefined {
    return this.args.analyzerOptions;
  }

  get $searchMode (): Expression | undefined {
    return this.args.searchMode;
  }

  static {
    this.register();
  }
}

export type SearchIpExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class SearchIpExpr extends FuncExpr {
  key = ExpressionKey.SEARCH_IP;

  static argTypes: RequiredMap<SearchIpExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: SearchIpExprArgs;

  constructor (args: SearchIpExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type StrToDateExprArgs = Merge<[
  FuncExprArgs,
  {
    format?: string;
    safe?: boolean;
    this: Expression;
  },
]>;

export class StrToDateExpr extends FuncExpr {
  key = ExpressionKey.STR_TO_DATE;

  /**
   * Defines the arguments (properties and child expressions) for StrToDate expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<StrToDateExprArgs> = {
    ...super.argTypes,
    format: false,
    safe: false,
    this: true,
  };

  declare args: StrToDateExprArgs;

  constructor (args: StrToDateExprArgs) {
    super(args);
  }

  get $format (): string | undefined {
    return this.args.format;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type StrToTimeExprArgs = Merge<[
  FuncExprArgs,
  {
    format: string;
    zone?: Expression;
    safe?: boolean;
    targetType?: DataTypeExpr;
    this: Expression;
  },
]>;

export class StrToTimeExpr extends FuncExpr {
  key = ExpressionKey.STR_TO_TIME;

  /**
   * Defines the arguments (properties and child expressions) for StrToTime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<StrToTimeExprArgs> = {
    ...super.argTypes,
    format: true,
    zone: false,
    safe: false,
    targetType: false,
    this: true,
  };

  declare args: StrToTimeExprArgs;

  constructor (args: StrToTimeExprArgs) {
    super(args);
  }

  get $format (): string {
    return this.args.format;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }

  get $targetType (): DataTypeExpr | undefined {
    return this.args.targetType;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type StrToUnixExprArgs = Merge<[
  FuncExprArgs,
  {
    format?: string;
    this?: Expression;
  },
]>;

export class StrToUnixExpr extends FuncExpr {
  key = ExpressionKey.STR_TO_UNIX;

  static argTypes: RequiredMap<StrToUnixExprArgs> = {
    format: false,
    this: false,
  };

  declare args: StrToUnixExprArgs;

  constructor (args: StrToUnixExprArgs) {
    super(args);
  }

  get $format (): string | undefined {
    return this.args.format;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type StrToMapExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    pairDelim?: Expression;
    keyValueDelim?: string;
    duplicateResolutionCallback?: Expression;
  },
]>;

export class StrToMapExpr extends FuncExpr {
  key = ExpressionKey.STR_TO_MAP;

  /**
   * Defines the arguments (properties and child expressions) for StrToMap expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<StrToMapExprArgs> = {
    ...super.argTypes,
    this: true,
    pairDelim: false,
    keyValueDelim: false,
    duplicateResolutionCallback: false,
  };

  declare args: StrToMapExprArgs;

  constructor (args: StrToMapExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $pairDelim (): Expression | undefined {
    return this.args.pairDelim;
  }

  get $keyValueDelim (): string | undefined {
    return this.args.keyValueDelim;
  }

  get $duplicateResolutionCallback (): Expression | undefined {
    return this.args.duplicateResolutionCallback;
  }

  static {
    this.register();
  }
}

export type NumberToStrExprArgs = Merge<[
  FuncExprArgs,
  {
    format: string;
    culture?: Expression;
    this: Expression;
  },
]>;

export class NumberToStrExpr extends FuncExpr {
  key = ExpressionKey.NUMBER_TO_STR;

  /**
   * Defines the arguments (properties and child expressions) for NumberToStr expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<NumberToStrExprArgs> = {
    ...super.argTypes,
    format: true,
    culture: false,
    this: true,
  };

  declare args: NumberToStrExprArgs;

  constructor (args: NumberToStrExprArgs) {
    super(args);
  }

  get $format (): string {
    return this.args.format;
  }

  get $culture (): Expression | undefined {
    return this.args.culture;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type FromBaseExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class FromBaseExpr extends FuncExpr {
  key = ExpressionKey.FROM_BASE;

  static argTypes: RequiredMap<FromBaseExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: FromBaseExprArgs;

  constructor (args: FromBaseExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type SpaceExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SpaceExpr extends FuncExpr {
  key = ExpressionKey.SPACE;

  static argTypes: RequiredMap<SpaceExprArgs> = {
    ...super.argTypes,
  };

  declare args: SpaceExprArgs;

  constructor (args: SpaceExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StructExprArgs = Merge<[
  FuncExprArgs,
  { expressions?: Expression[] },
]>;

export class StructExpr extends FuncExpr {
  key = ExpressionKey.STRUCT;

  static isVarLenArgs = true;
  static argTypes: RequiredMap<StructExprArgs> = {
    ...super.argTypes,
    expressions: false,
  };

  declare args: StructExprArgs;

  constructor (args: StructExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type StructExtractExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class StructExtractExpr extends FuncExpr {
  key = ExpressionKey.STRUCT_EXTRACT;

  static argTypes: RequiredMap<StructExtractExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: StructExtractExprArgs;

  constructor (args: StructExtractExprArgs) {
    super(args);
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type StuffExprArgs = Merge<[
  FuncExprArgs,
  {
    start: Expression;
    length: number | Expression;
    this: Expression;
    expression: Expression;
  },
]>;

export class StuffExpr extends FuncExpr {
  key = ExpressionKey.STUFF;
  static _sqlNames = ['STUFF', 'INSERT'];

  /**
   * Defines the arguments (properties and child expressions) for Stuff expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<StuffExprArgs> = {
    ...super.argTypes,
    start: true,
    length: true,
    this: true,
    expression: true,
  };

  declare args: StuffExprArgs;

  constructor (args: StuffExprArgs) {
    super(args);
  }

  get $start (): Expression {
    return this.args.start;
  }

  get $length (): number | Expression {
    return this.args.length;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type SqrtExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SqrtExpr extends FuncExpr {
  key = ExpressionKey.SQRT;

  static argTypes: RequiredMap<SqrtExprArgs> = {
    ...super.argTypes,
  };

  declare args: SqrtExprArgs;

  constructor (args: SqrtExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeExprArgs = Merge<[
  FuncExprArgs,
  {
    zone?: Expression;
    this?: Expression;
  },
]>;

export class TimeExpr extends FuncExpr {
  key = ExpressionKey.TIME;

  static argTypes: RequiredMap<TimeExprArgs> = {
    zone: false,
    this: false,
  };

  declare args: TimeExprArgs;

  constructor (args: TimeExprArgs) {
    super(args);
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type TimeToStrExprArgs = Merge<[
  FuncExprArgs,
  {
    format: string;
    culture?: Expression;
    zone?: Expression;
    this: Expression;
  },
]>;

export class TimeToStrExpr extends FuncExpr {
  key = ExpressionKey.TIME_TO_STR;

  /**
   * Defines the arguments (properties and child expressions) for TimeToStr expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<TimeToStrExprArgs> = {
    ...super.argTypes,
    format: true,
    culture: false,
    zone: false,
    this: true,
  };

  declare args: TimeToStrExprArgs;

  constructor (args: TimeToStrExprArgs) {
    super(args);
  }

  get $format (): string {
    return this.args.format;
  }

  get $culture (): Expression | undefined {
    return this.args.culture;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type TimeToTimeStrExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TimeToTimeStrExpr extends FuncExpr {
  key = ExpressionKey.TIME_TO_TIME_STR;

  static argTypes: RequiredMap<TimeToTimeStrExprArgs> = {
    ...super.argTypes,
  };

  declare args: TimeToTimeStrExprArgs;

  constructor (args: TimeToTimeStrExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeToUnixExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TimeToUnixExpr extends FuncExpr {
  key = ExpressionKey.TIME_TO_UNIX;

  static argTypes: RequiredMap<TimeToUnixExprArgs> = {
    ...super.argTypes,
  };

  declare args: TimeToUnixExprArgs;

  constructor (args: TimeToUnixExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeStrToDateExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TimeStrToDateExpr extends FuncExpr {
  key = ExpressionKey.TIME_STR_TO_DATE;

  static argTypes: RequiredMap<TimeStrToDateExprArgs> = {
    ...super.argTypes,
  };

  declare args: TimeStrToDateExprArgs;

  constructor (args: TimeStrToDateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeStrToTimeExprArgs = Merge<[
  FuncExprArgs,
  {
    zone?: Expression;
    this: Expression;
  },
]>;

export class TimeStrToTimeExpr extends FuncExpr {
  key = ExpressionKey.TIME_STR_TO_TIME;

  static argTypes: RequiredMap<TimeStrToTimeExprArgs> = {
    zone: false,
    this: true,
  };

  declare args: TimeStrToTimeExprArgs;

  constructor (args: TimeStrToTimeExprArgs) {
    super(args);
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type TimeStrToUnixExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TimeStrToUnixExpr extends FuncExpr {
  key = ExpressionKey.TIME_STR_TO_UNIX;

  static argTypes: RequiredMap<TimeStrToUnixExprArgs> = {
    ...super.argTypes,
  };

  declare args: TimeStrToUnixExprArgs;

  constructor (args: TimeStrToUnixExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export enum TrimPosition {
  LEADING = 'LEADING',
  TRAILING = 'TRAILING',
}

export type TrimExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
    position?: TrimPosition;
    collation?: Expression;
  },
]>;

export class TrimExpr extends FuncExpr {
  key = ExpressionKey.TRIM;

  /**
   * Defines the arguments (properties and child expressions) for Trim expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<TrimExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
    position: false,
    collation: false,
  };

  declare args: TrimExprArgs;

  constructor (args: TrimExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $position (): TrimPosition | undefined {
    return this.args.position;
  }

  get $collation (): Expression | undefined {
    return this.args.collation;
  }

  static {
    this.register();
  }
}

export type TsOrDsAddExprArgs = Merge<[
  FuncExprArgs,
  {
    unit?: Expression;
    returnType?: DataTypeExpr;
    this: Expression;
    expression: Expression;
  },
]>;

export class TsOrDsAddExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TS_OR_DS_ADD;

  /**
   * Defines the arguments (properties and child expressions) for TsOrDsAdd expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<TsOrDsAddExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    unit: false,
    returnType: false,
    this: true,
    expression: true,
  };

  declare args: TsOrDsAddExprArgs;

  constructor (args: TsOrDsAddExprArgs) {
    super(args);
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  get $returnType (): DataTypeExpr | undefined {
    return this.args.returnType;
  }

  get returnType (): DataTypeExpr {
    const returnTypeArg = this.args.returnType;
    if (returnTypeArg instanceof DataTypeExpr) {
      return returnTypeArg;
    }
    return DataTypeExpr.build(DataTypeExprKind.DATE);
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type TsOrDsDiffExprArgs = Merge<[
  FuncExprArgs,
  {
    unit?: Expression;
    this: Expression;
    expression: Expression;
  },
]>;

export class TsOrDsDiffExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TS_OR_DS_DIFF;

  static argTypes: RequiredMap<TsOrDsDiffExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    unit: false,
    this: true,
    expression: true,
  };

  declare args: TsOrDsDiffExprArgs;

  constructor (args: TsOrDsDiffExprArgs) {
    super(args);
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type TsOrDsToDateStrExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TsOrDsToDateStrExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_TO_DATE_STR;

  static argTypes: RequiredMap<TsOrDsToDateStrExprArgs> = {
    ...super.argTypes,
  };

  declare args: TsOrDsToDateStrExprArgs;

  constructor (args: TsOrDsToDateStrExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TsOrDsToDateExprArgs = Merge<[
  FuncExprArgs,
  {
    format?: string;
    safe?: boolean;
    this: Expression;
  },
]>;

export class TsOrDsToDateExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_TO_DATE;

  /**
   * Defines the arguments (properties and child expressions) for TsOrDsToDate expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<TsOrDsToDateExprArgs> = {
    ...super.argTypes,
    format: false,
    safe: false,
    this: true,
  };

  declare args: TsOrDsToDateExprArgs;

  constructor (args: TsOrDsToDateExprArgs) {
    super(args);
  }

  get $format (): string | undefined {
    return this.args.format;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type TsOrDsToDatetimeExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TsOrDsToDatetimeExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_TO_DATETIME;

  static argTypes: RequiredMap<TsOrDsToDatetimeExprArgs> = {
    ...super.argTypes,
  };

  declare args: TsOrDsToDatetimeExprArgs;

  constructor (args: TsOrDsToDatetimeExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TsOrDsToTimeExprArgs = Merge<[
  FuncExprArgs,
  {
    format?: string;
    safe?: boolean;
    this: Expression;
  },
]>;

export class TsOrDsToTimeExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_TO_TIME;

  /**
   * Defines the arguments (properties and child expressions) for TsOrDsToTime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<TsOrDsToTimeExprArgs> = {
    ...super.argTypes,
    format: false,
    safe: false,
    this: true,
  };

  declare args: TsOrDsToTimeExprArgs;

  constructor (args: TsOrDsToTimeExprArgs) {
    super(args);
  }

  get $format (): string | undefined {
    return this.args.format;
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type TsOrDsToTimestampExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TsOrDsToTimestampExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_TO_TIMESTAMP;

  static argTypes: RequiredMap<TsOrDsToTimestampExprArgs> = {
    ...super.argTypes,
  };

  declare args: TsOrDsToTimestampExprArgs;

  constructor (args: TsOrDsToTimestampExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TsOrDiToDiExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TsOrDiToDiExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DI_TO_DI;

  static argTypes: RequiredMap<TsOrDiToDiExprArgs> = {
    ...super.argTypes,
  };

  declare args: TsOrDiToDiExprArgs;

  constructor (args: TsOrDiToDiExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnhexExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class UnhexExpr extends FuncExpr {
  key = ExpressionKey.UNHEX;

  static argTypes: RequiredMap<UnhexExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: UnhexExprArgs;

  constructor (args: UnhexExprArgs) {
    super(args);
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type UnicodeExprArgs = Merge<[
  FuncExprArgs,
]>;

export class UnicodeExpr extends FuncExpr {
  key = ExpressionKey.UNICODE;

  static argTypes: RequiredMap<UnicodeExprArgs> = {
    ...super.argTypes,
  };

  declare args: UnicodeExprArgs;

  constructor (args: UnicodeExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UniformExprArgs = Merge<[
  FuncExprArgs,
  {
    gen?: Expression;
    seed?: Expression;
    this: Expression;
    expression: Expression;
  },
]>;

export class UniformExpr extends FuncExpr {
  key = ExpressionKey.UNIFORM;

  /**
   * Defines the arguments (properties and child expressions) for Uniform expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<UniformExprArgs> = {
    ...super.argTypes,
    gen: false,
    seed: false,
    this: true,
    expression: true,
  };

  declare args: UniformExprArgs;

  constructor (args: UniformExprArgs) {
    super(args);
  }

  get $gen (): Expression | undefined {
    return this.args.gen;
  }

  get $seed (): Expression | undefined {
    return this.args.seed;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type UnixDateExprArgs = Merge<[
  FuncExprArgs,
]>;

export class UnixDateExpr extends FuncExpr {
  key = ExpressionKey.UNIX_DATE;

  static argTypes: RequiredMap<UnixDateExprArgs> = {
    ...super.argTypes,
  };

  declare args: UnixDateExprArgs;

  constructor (args: UnixDateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnixToStrExprArgs = Merge<[
  FuncExprArgs,
  {
    format?: string;
    this: Expression;
  },
]>;

export class UnixToStrExpr extends FuncExpr {
  key = ExpressionKey.UNIX_TO_STR;

  static argTypes: RequiredMap<UnixToStrExprArgs> = {
    ...super.argTypes,
    format: false,
    this: true,
  };

  declare args: UnixToStrExprArgs;

  constructor (args: UnixToStrExprArgs) {
    super(args);
  }

  get $format (): string | undefined {
    return this.args.format;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type UnixToTimeExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    scale?: number | Expression;
    zone?: Expression;
    hours?: Expression[];
    minutes?: Expression[];
    format?: string;
    targetType?: DataTypeExpr;
  },
]>;

export class UnixToTimeExpr extends FuncExpr {
  key = ExpressionKey.UNIX_TO_TIME;

  /**
   * Defines the arguments (properties and child expressions) for UnixToTime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<UnixToTimeExprArgs> = {
    ...super.argTypes,
    this: true,
    scale: false,
    zone: false,
    hours: false,
    minutes: false,
    format: false,
    targetType: false,
  };

  declare args: UnixToTimeExprArgs;

  constructor (args: UnixToTimeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $scale (): number | Expression | undefined {
    return this.args.scale;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  get $hours (): Expression[] | undefined {
    return this.args.hours;
  }

  get $minutes (): Expression[] | undefined {
    return this.args.minutes;
  }

  get $format (): string | undefined {
    return this.args.format;
  }

  get $targetType (): DataTypeExpr | undefined {
    return this.args.targetType;
  }

  static SECONDS = LiteralExpr.number(0);
  static DECIS = LiteralExpr.number(1);
  static CENTIS = LiteralExpr.number(2);
  static MILLIS = LiteralExpr.number(3);
  static DECIMILLIS = LiteralExpr.number(4);
  static CENTIMILLIS = LiteralExpr.number(5);
  static MICROS = LiteralExpr.number(6);
  static DECIMICROS = LiteralExpr.number(7);
  static CENTIMICROS = LiteralExpr.number(8);
  static NANOS = LiteralExpr.number(9);

  static {
    this.register();
  }
}

export type UnixToTimeStrExprArgs = Merge<[
  FuncExprArgs,
]>;

export class UnixToTimeStrExpr extends FuncExpr {
  key = ExpressionKey.UNIX_TO_TIME_STR;

  static argTypes: RequiredMap<UnixToTimeStrExprArgs> = {
    ...super.argTypes,
  };

  declare args: UnixToTimeStrExprArgs;

  constructor (args: UnixToTimeStrExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnixSecondsExprArgs = Merge<[
  FuncExprArgs,
]>;

export class UnixSecondsExpr extends FuncExpr {
  key = ExpressionKey.UNIX_SECONDS;

  static argTypes: RequiredMap<UnixSecondsExprArgs> = {
    ...super.argTypes,
  };

  declare args: UnixSecondsExprArgs;

  constructor (args: UnixSecondsExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnixMicrosExprArgs = Merge<[
  FuncExprArgs,
]>;

export class UnixMicrosExpr extends FuncExpr {
  key = ExpressionKey.UNIX_MICROS;

  static argTypes: RequiredMap<UnixMicrosExprArgs> = {
    ...super.argTypes,
  };

  declare args: UnixMicrosExprArgs;

  constructor (args: UnixMicrosExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnixMillisExprArgs = Merge<[
  FuncExprArgs,
]>;

export class UnixMillisExpr extends FuncExpr {
  key = ExpressionKey.UNIX_MILLIS;

  static argTypes: RequiredMap<UnixMillisExprArgs> = {
    ...super.argTypes,
  };

  declare args: UnixMillisExprArgs;

  constructor (args: UnixMillisExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UuidExprArgs = Merge<[
  FuncExprArgs,
  {
    name?: Expression;
    isString?: boolean;
    this?: Expression;
  },
]>;

export class UuidExpr extends FuncExpr {
  key = ExpressionKey.UUID;

  static _sqlNames = [
    'UUID',
    'GEN_RANDOM_UUID',
    'GENERATE_UUID',
    'UUID_STRING',
  ];

  /**
   * Defines the arguments (properties and child expressions) for Uuid expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<UuidExprArgs> = {
    ...super.argTypes,
    name: false,
    isString: false,
    this: false,
  };

  declare args: UuidExprArgs;

  constructor (args: UuidExprArgs) {
    super(args);
  }

  get $name (): Expression | undefined {
    return this.args.name;
  }

  get $isString (): boolean | undefined {
    return this.args.isString;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

const TIMESTAMP_PARTS = {
  year: false,
  month: false,
  day: false,
  hour: false,
  min: false,
  sec: false,
  nano: false,
} as const;

export type TimestampFromPartsExprArgs = Merge<[
  FuncExprArgs,
  {
    year?: Expression;
    month?: Expression;
    day?: Expression;
    hour?: Expression;
    min?: Expression;
    sec?: Expression;
    nano?: Expression;
    zone?: Expression;
    milli?: Expression;
    this?: Expression;
    expression?: Expression;
  },
]>;

export class TimestampFromPartsExpr extends FuncExpr {
  key = ExpressionKey.TIMESTAMP_FROM_PARTS;
  static _sqlNames = ['TIMESTAMP_FROM_PARTS', 'TIMESTAMPFROMPARTS'];

  /**
   * Defines the arguments (properties and child expressions) for TimestampFromParts expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<TimestampFromPartsExprArgs> = {
    ...super.argTypes,
    ...TIMESTAMP_PARTS,
    zone: false,
    milli: false,
    this: false,
    expression: false,
  };

  declare args: TimestampFromPartsExprArgs;

  constructor (args: TimestampFromPartsExprArgs) {
    super(args);
  }

  get $year (): Expression | undefined {
    return this.args.year;
  }

  get $month (): Expression | undefined {
    return this.args.month;
  }

  get $day (): Expression | undefined {
    return this.args.day;
  }

  get $hour (): Expression | undefined {
    return this.args.hour;
  }

  get $min (): Expression | undefined {
    return this.args.min;
  }

  get $sec (): Expression | undefined {
    return this.args.sec;
  }

  get $nano (): Expression | undefined {
    return this.args.nano;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  get $milli (): Expression | undefined {
    return this.args.milli;
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type TimestampLtzFromPartsExprArgs = Merge<[
  FuncExprArgs,
  {
    year?: Expression;
    month?: Expression;
    day?: Expression;
    hour?: Expression;
    min?: Expression;
    sec?: Expression;
    nano?: Expression;
  },
]>;

export class TimestampLtzFromPartsExpr extends FuncExpr {
  key = ExpressionKey.TIMESTAMP_LTZ_FROM_PARTS;

  static _sqlNames = ['TIMESTAMP_LTZ_FROM_PARTS', 'TIMESTAMPLTZFROMPARTS'];

  static argTypes: RequiredMap<TimestampLtzFromPartsExprArgs> = {
    ...super.argTypes,
    ...TIMESTAMP_PARTS,
  };

  declare args: TimestampLtzFromPartsExprArgs;

  static {
    this.register();
  }

  constructor (args: TimestampLtzFromPartsExprArgs) {
    super(args);
  }

  get $year (): Expression | undefined {
    return this.args.year;
  }

  get $month (): Expression | undefined {
    return this.args.month;
  }

  get $day (): Expression | undefined {
    return this.args.day;
  }

  get $hour (): Expression | undefined {
    return this.args.hour;
  }

  get $min (): Expression | undefined {
    return this.args.min;
  }

  get $sec (): Expression | undefined {
    return this.args.sec;
  }

  get $nano (): Expression | undefined {
    return this.args.nano;
  }
}

export type TimestampTzFromPartsExprArgs = Merge<[
  FuncExprArgs,
  {
    year?: Expression;
    month?: Expression;
    day?: Expression;
    hour?: Expression;
    min?: Expression;
    sec?: Expression;
    nano?: Expression;
    zone?: Expression;
  },
]>;

export class TimestampTzFromPartsExpr extends FuncExpr {
  key = ExpressionKey.TIMESTAMP_TZ_FROM_PARTS;
  static _sqlNames = ['TIMESTAMP_TZ_FROM_PARTS', 'TIMESTAMPTZFROMPARTS'];

  static argTypes: RequiredMap<TimestampTzFromPartsExprArgs> = {
    ...super.argTypes,
    ...TIMESTAMP_PARTS,
    zone: false,
  };

  declare args: TimestampTzFromPartsExprArgs;

  constructor (args: TimestampTzFromPartsExprArgs) {
    super(args);
  }

  get $year (): Expression | undefined {
    return this.args.year;
  }

  get $month (): Expression | undefined {
    return this.args.month;
  }

  get $day (): Expression | undefined {
    return this.args.day;
  }

  get $hour (): Expression | undefined {
    return this.args.hour;
  }

  get $min (): Expression | undefined {
    return this.args.min;
  }

  get $sec (): Expression | undefined {
    return this.args.sec;
  }

  get $nano (): Expression | undefined {
    return this.args.nano;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  static {
    this.register();
  }
}

export type UpperExprArgs = Merge<[
  FuncExprArgs,
]>;

export class UpperExpr extends FuncExpr {
  key = ExpressionKey.UPPER;

  static argTypes: RequiredMap<UpperExprArgs> = {
    ...super.argTypes,
  };

  declare args: UpperExprArgs;

  constructor (args: UpperExprArgs) {
    super(args);
  }

  static _sqlNames = ['UPPER', 'UCASE'];

  static {
    this.register();
  }
}

export type CorrExprArgs = Merge<[
  BinaryExprArgs,
  AggFuncExprArgs,
  {
    nullOnZeroVariance?: Expression;
    this: Expression;
    expression: Expression;
  },
]>;

export class CorrExpr extends multiInherit(BinaryExpr, AggFuncExpr) {
  key = ExpressionKey.CORR;

  static argTypes: RequiredMap<CorrExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    nullOnZeroVariance: false,
    this: true,
    expression: true,
  };

  declare args: CorrExprArgs;

  constructor (args: CorrExprArgs) {
    super(args);
  }

  get $nullOnZeroVariance (): Expression | undefined {
    return this.args.nullOnZeroVariance;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type WidthBucketExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    minValue?: string;
    maxValue?: string;
    numBuckets?: Expression[];
    threshold?: Expression;
  },
]>;

export class WidthBucketExpr extends FuncExpr {
  key = ExpressionKey.WIDTH_BUCKET;

  /**
   * Defines the arguments (properties and child expressions) for WidthBucket expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<WidthBucketExprArgs> = {
    ...super.argTypes,
    this: true,
    minValue: false,
    maxValue: false,
    numBuckets: false,
    threshold: false,
  };

  declare args: WidthBucketExprArgs;

  constructor (args: WidthBucketExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $minValue (): string | undefined {
    return this.args.minValue;
  }

  get $maxValue (): string | undefined {
    return this.args.maxValue;
  }

  get $numBuckets (): Expression[] | undefined {
    return this.args.numBuckets;
  }

  get $threshold (): Expression | undefined {
    return this.args.threshold;
  }

  static {
    this.register();
  }
}

export type WeekExprArgs = Merge<[
  FuncExprArgs,
  {
    mode?: Expression;
    this: Expression;
  },
]>;

export class WeekExpr extends FuncExpr {
  key = ExpressionKey.WEEK;

  static argTypes: RequiredMap<WeekExprArgs> = {
    ...super.argTypes,
    mode: false,
    this: true,
  };

  declare args: WeekExprArgs;

  constructor (args: WeekExprArgs) {
    super(args);
  }

  get $mode (): Expression | undefined {
    return this.args.mode;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type NextDayExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class NextDayExpr extends FuncExpr {
  key = ExpressionKey.NEXT_DAY;

  static argTypes: RequiredMap<NextDayExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: NextDayExprArgs;

  constructor (args: NextDayExprArgs) {
    super(args);
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $this (): ExpressionValue {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type XMLElementExprArgs = Merge<[
  FuncExprArgs,
  {
    evalname?: string;
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class XMLElementExpr extends FuncExpr {
  key = ExpressionKey.XML_ELEMENT;

  static _sqlNames = ['XMLELEMENT'];

  static argTypes: RequiredMap<XMLElementExprArgs> = {
    ...super.argTypes,
    evalname: false,
    this: true,
    expressions: false,
  };

  declare args: XMLElementExprArgs;

  constructor (args: XMLElementExprArgs) {
    super(args);
  }

  get $evalname (): string | undefined {
    return this.args.evalname;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type XMLGetExprArgs = Merge<[
  FuncExprArgs,
  {
    instance?: Expression;
    this: Expression;
    expression: Expression;
  },
]>;

export class XMLGetExpr extends FuncExpr {
  key = ExpressionKey.XML_GET;
  static _sqlNames = ['XMLGET'];

  static argTypes: RequiredMap<XMLGetExprArgs> = {
    ...super.argTypes,
    instance: false,
    this: true,
    expression: true,
  };

  declare args: XMLGetExprArgs;

  constructor (args: XMLGetExprArgs) {
    super(args);
  }

  get $instance (): Expression | undefined {
    return this.args.instance;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type XMLTableExprArgs = Merge<[
  FuncExprArgs,
  {
    this: Expression;
    namespaces?: Expression[];
    passing?: Expression;
    columns?: Expression[];
    byRef?: Expression;
  },
]>;

export class XMLTableExpr extends FuncExpr {
  key = ExpressionKey.XML_TABLE;

  /**
   * Defines the arguments (properties and child expressions) for XMLTable expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<XMLTableExprArgs> = {
    ...super.argTypes,
    this: true,
    namespaces: false,
    passing: false,
    columns: false,
    byRef: false,
  };

  declare args: XMLTableExprArgs;

  constructor (args: XMLTableExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $namespaces (): Expression[] | undefined {
    return this.args.namespaces;
  }

  get $passing (): Expression | undefined {
    return this.args.passing;
  }

  get $columns (): Expression[] | undefined {
    return this.args.columns;
  }

  get $byRef (): Expression | undefined {
    return this.args.byRef;
  }

  static {
    this.register();
  }
}

export type YearExprArgs = Merge<[
  FuncExprArgs,
]>;

export class YearExpr extends FuncExpr {
  key = ExpressionKey.YEAR;

  static argTypes: RequiredMap<YearExprArgs> = {
    ...super.argTypes,
  };

  declare args: YearExprArgs;

  constructor (args: YearExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ZipfExprArgs = Merge<[
  FuncExprArgs,
  {
    elementcount: Expression;
    gen: Expression;
    this: Expression;
  },
]>;

export class ZipfExpr extends FuncExpr {
  key = ExpressionKey.ZIPF;

  /**
   * Defines the arguments (properties and child expressions) for Zipf expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ZipfExprArgs> = {
    ...super.argTypes,
    elementcount: true,
    gen: true,
    this: true,
  };

  declare args: ZipfExprArgs;

  constructor (args: ZipfExprArgs) {
    super(args);
  }

  get $elementcount (): Expression {
    return this.args.elementcount;
  }

  get $gen (): Expression {
    return this.args.gen;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type NextValueForExprArgs = Merge<[
  FuncExprArgs,
  {
    order?: Expression;
    this: Expression;
  },
]>;

export class NextValueForExpr extends FuncExpr {
  key = ExpressionKey.NEXT_VALUE_FOR;

  static argTypes: RequiredMap<NextValueForExprArgs> = {
    ...super.argTypes,
    order: false,
    this: true,
  };

  declare args: NextValueForExprArgs;

  constructor (args: NextValueForExprArgs) {
    super(args);
  }

  get $order (): Expression | undefined {
    return this.args.order;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type AllExprArgs = Merge<[
  SubqueryPredicateExprArgs,
]>;

export class AllExpr extends SubqueryPredicateExpr {
  key = ExpressionKey.ALL;

  static argTypes: RequiredMap<AllExprArgs> = {
    ...super.argTypes,
  };

  declare args: AllExprArgs;

  constructor (args: AllExprArgs) {
    super(args);
  }
}

export type AnyExprArgs = Merge<[
  SubqueryPredicateExprArgs,
]>;

export class AnyExpr extends SubqueryPredicateExpr {
  key = ExpressionKey.ANY;

  static argTypes: RequiredMap<AnyExprArgs> = {
    ...super.argTypes,
  };

  declare args: AnyExprArgs;

  constructor (args: AnyExprArgs) {
    super(args);
  }
}

export type BitwiseAndAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class BitwiseAndAggExpr extends AggFuncExpr {
  key = ExpressionKey.BITWISE_AND_AGG;

  static argTypes: RequiredMap<BitwiseAndAggExprArgs> = {
    ...super.argTypes,
  };

  declare args: BitwiseAndAggExprArgs;

  constructor (args: BitwiseAndAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitwiseOrAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class BitwiseOrAggExpr extends AggFuncExpr {
  key = ExpressionKey.BITWISE_OR_AGG;

  static argTypes: RequiredMap<BitwiseOrAggExprArgs> = {
    ...super.argTypes,
  };

  declare args: BitwiseOrAggExprArgs;

  constructor (args: BitwiseOrAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitwiseXorAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class BitwiseXorAggExpr extends AggFuncExpr {
  key = ExpressionKey.BITWISE_XOR_AGG;

  static argTypes: RequiredMap<BitwiseXorAggExprArgs> = {
    ...super.argTypes,
  };

  declare args: BitwiseXorAggExprArgs;

  constructor (args: BitwiseXorAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BoolxorAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class BoolxorAggExpr extends AggFuncExpr {
  key = ExpressionKey.BOOLXOR_AGG;

  static argTypes: RequiredMap<BoolxorAggExprArgs> = {
    ...super.argTypes,
  };

  declare args: BoolxorAggExprArgs;

  constructor (args: BoolxorAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitmapConstructAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class BitmapConstructAggExpr extends AggFuncExpr {
  key = ExpressionKey.BITMAP_CONSTRUCT_AGG;

  static argTypes: RequiredMap<BitmapConstructAggExprArgs> = {
    ...super.argTypes,
  };

  declare args: BitmapConstructAggExprArgs;

  constructor (args: BitmapConstructAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitmapOrAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class BitmapOrAggExpr extends AggFuncExpr {
  key = ExpressionKey.BITMAP_OR_AGG;

  static argTypes: RequiredMap<BitmapOrAggExprArgs> = {
    ...super.argTypes,
  };

  declare args: BitmapOrAggExprArgs;

  constructor (args: BitmapOrAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ParameterizedAggExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expressions: Expression[];
    params: Expression[];
  },
]>;

export class ParameterizedAggExpr extends AggFuncExpr {
  key = ExpressionKey.PARAMETERIZED_AGG;

  static argTypes: RequiredMap<ParameterizedAggExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
    params: true,
  };

  declare args: ParameterizedAggExprArgs;

  constructor (args: ParameterizedAggExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $params (): Expression[] {
    return this.args.params;
  }

  static {
    this.register();
  }
}

export type ArgMaxExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    count?: Expression;
  },
]>;

export class ArgMaxExpr extends AggFuncExpr {
  key = ExpressionKey.ARG_MAX;

  static _sqlNames = [
    'ARG_MAX',
    'ARGMAX',
    'MAX_BY',
  ];

  static argTypes: RequiredMap<ArgMaxExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    count: false,
  };

  declare args: ArgMaxExprArgs;

  constructor (args: ArgMaxExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $count (): Expression | undefined {
    return this.args.count;
  }

  static {
    this.register();
  }
}

export type ArgMinExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    count?: Expression;
  },
]>;

export class ArgMinExpr extends AggFuncExpr {
  key = ExpressionKey.ARG_MIN;

  static _sqlNames = [
    'ARG_MIN',
    'ARGMIN',
    'MIN_BY',
  ];

  static argTypes: RequiredMap<ArgMinExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    count: false,
  };

  declare args: ArgMinExprArgs;

  constructor (args: ArgMinExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $count (): Expression | undefined {
    return this.args.count;
  }

  static {
    this.register();
  }
}

export type ApproxTopKExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
    counters?: Expression;
  },
]>;

export class ApproxTopKExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_TOP_K;

  static argTypes: RequiredMap<ApproxTopKExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
    counters: false,
  };

  declare args: ApproxTopKExprArgs;

  constructor (args: ApproxTopKExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  get $counters (): Expression | undefined {
    return this.args.counters;
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/approx_top_k_accumulate
 * https://spark.apache.org/docs/preview/api/sql/index.html#approx_top_k_accumulate
 */
export type ApproxTopKAccumulateExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class ApproxTopKAccumulateExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_TOP_K_ACCUMULATE;

  static argTypes: RequiredMap<ApproxTopKAccumulateExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: ApproxTopKAccumulateExprArgs;

  constructor (args: ApproxTopKAccumulateExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/approx_top_k_combine
 */
export type ApproxTopKCombineExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class ApproxTopKCombineExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_TOP_K_COMBINE;

  static argTypes: RequiredMap<ApproxTopKCombineExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: ApproxTopKCombineExprArgs;

  constructor (args: ApproxTopKCombineExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ApproxTopSumExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
    count: Expression;
  },
]>;

export class ApproxTopSumExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_TOP_SUM;

  static argTypes: RequiredMap<ApproxTopSumExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
    count: true,
  };

  declare args: ApproxTopSumExprArgs;

  constructor (args: ApproxTopSumExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $count (): Expression {
    return this.args.count;
  }

  static {
    this.register();
  }
}

export type ApproxQuantilesExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class ApproxQuantilesExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_QUANTILES;

  static argTypes: RequiredMap<ApproxQuantilesExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: ApproxQuantilesExprArgs;

  constructor (args: ApproxQuantilesExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/approx_percentile_combine
 */
export type ApproxPercentileCombineExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class ApproxPercentileCombineExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_PERCENTILE_COMBINE;

  static argTypes: RequiredMap<ApproxPercentileCombineExprArgs> = {
    ...super.argTypes,
  };

  declare args: ApproxPercentileCombineExprArgs;

  constructor (args: ApproxPercentileCombineExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/minhash
 */
export type MinhashExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expressions: Expression[];
  },
]>;

export class MinhashExpr extends AggFuncExpr {
  key = ExpressionKey.MINHASH;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<MinhashExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
  };

  declare args: MinhashExprArgs;

  constructor (args: MinhashExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/minhash_combine
 */
export type MinhashCombineExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class MinhashCombineExpr extends AggFuncExpr {
  key = ExpressionKey.MINHASH_COMBINE;

  static argTypes: RequiredMap<MinhashCombineExprArgs> = {
    ...super.argTypes,
  };

  declare args: MinhashCombineExprArgs;

  constructor (args: MinhashCombineExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/approximate_similarity
 */
export type ApproximateSimilarityExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class ApproximateSimilarityExpr extends AggFuncExpr {
  key = ExpressionKey.APPROXIMATE_SIMILARITY;

  static _sqlNames = ['APPROXIMATE_SIMILARITY', 'APPROXIMATE_JACCARD_INDEX'];

  static argTypes: RequiredMap<ApproximateSimilarityExprArgs> = {
    ...super.argTypes,
  };

  declare args: ApproximateSimilarityExprArgs;

  constructor (args: ApproximateSimilarityExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type GroupingExprArgs = Merge<[
  AggFuncExprArgs,
  { expressions: Expression[] },
]>;

export class GroupingExpr extends AggFuncExpr {
  key = ExpressionKey.GROUPING;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<GroupingExprArgs> = {
    ...super.argTypes,
    expressions: true,
  };

  declare args: GroupingExprArgs;

  constructor (args: GroupingExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type GroupingIdExprArgs = Merge<[
  AggFuncExprArgs,
  { expressions?: Expression[] },
]>;

export class GroupingIdExpr extends AggFuncExpr {
  key = ExpressionKey.GROUPING_ID;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<GroupingIdExprArgs> = {
    ...super.argTypes,
    expressions: false,
  };

  declare args: GroupingIdExprArgs;

  constructor (args: GroupingIdExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type AnonymousAggFuncExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class AnonymousAggFuncExpr extends AggFuncExpr {
  key = ExpressionKey.ANONYMOUS_AGG_FUNC;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<AnonymousAggFuncExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
  };

  declare args: AnonymousAggFuncExprArgs;

  constructor (args: AnonymousAggFuncExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/hash_agg
 */
export type HashAggExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class HashAggExpr extends AggFuncExpr {
  key = ExpressionKey.HASH_AGG;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<HashAggExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
  };

  declare args: HashAggExprArgs;

  constructor (args: HashAggExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/hll
 * https://docs.aws.amazon.com/redshift/latest/dg/r_HLL_function.html
 */
export type HllExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class HllExpr extends AggFuncExpr {
  key = ExpressionKey.HLL;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<HllExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
  };

  declare args: HllExprArgs;

  constructor (args: HllExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type ApproxDistinctExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    accuracy?: Expression;
  },
]>;

export class ApproxDistinctExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_DISTINCT;

  static _sqlNames = ['APPROX_DISTINCT', 'APPROX_COUNT_DISTINCT'];

  static argTypes: RequiredMap<ApproxDistinctExprArgs> = {
    ...super.argTypes,
    this: true,
    accuracy: false,
  };

  declare args: ApproxDistinctExprArgs;

  constructor (args: ApproxDistinctExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $accuracy (): Expression | undefined {
    return this.args.accuracy;
  }

  static {
    this.register();
  }
}

/**
 * Postgres' GENERATE_SERIES function returns a row set, i.e. it implicitly explodes when it's
 * used in a projection, so this expression is a helper that facilitates transpilation to other
 * dialects. For example, we'd generate UNNEST(GENERATE_SERIES(...)) in DuckDB
 */
export type ExplodingGenerateSeriesExprArgs = Merge<[
  GenerateSeriesExprArgs,
]>;

export class ExplodingGenerateSeriesExpr extends GenerateSeriesExpr {
  key = ExpressionKey.EXPLODING_GENERATE_SERIES;

  static {
    this.register();
  }
}

export type ArrayAggExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    nullsExcluded?: boolean;
  },
]>;

export class ArrayAggExpr extends AggFuncExpr {
  key = ExpressionKey.ARRAY_AGG;

  static argTypes: RequiredMap<ArrayAggExprArgs> = {
    ...super.argTypes,
    this: true,
    nullsExcluded: false,
  };

  declare args: ArrayAggExprArgs;

  constructor (args: ArrayAggExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $nullsExcluded (): boolean | undefined {
    return this.args.nullsExcluded;
  }

  static {
    this.register();
  }
}

export type ArrayUniqueAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class ArrayUniqueAggExpr extends AggFuncExpr {
  key = ExpressionKey.ARRAY_UNIQUE_AGG;

  static argTypes: RequiredMap<ArrayUniqueAggExprArgs> = {
    ...super.argTypes,
  };

  declare args: ArrayUniqueAggExprArgs;

  constructor (args: ArrayUniqueAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AIAggExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class AIAggExpr extends AggFuncExpr {
  key = ExpressionKey.AI_AGG;

  static _sqlNames = ['AI_AGG'];

  static argTypes: RequiredMap<AIAggExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: AIAggExprArgs;

  constructor (args: AIAggExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type AISummarizeAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class AISummarizeAggExpr extends AggFuncExpr {
  key = ExpressionKey.AI_SUMMARIZE_AGG;

  static _sqlNames = ['AI_SUMMARIZE_AGG'];

  static argTypes: RequiredMap<AISummarizeAggExprArgs> = {
    ...super.argTypes,
  };

  declare args: AISummarizeAggExprArgs;

  constructor (args: AISummarizeAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayConcatAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class ArrayConcatAggExpr extends AggFuncExpr {
  key = ExpressionKey.ARRAY_CONCAT_AGG;

  static argTypes: RequiredMap<ArrayConcatAggExprArgs> = {
    ...super.argTypes,
  };

  declare args: ArrayConcatAggExprArgs;

  constructor (args: ArrayConcatAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayUnionAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class ArrayUnionAggExpr extends AggFuncExpr {
  key = ExpressionKey.ARRAY_UNION_AGG;

  static argTypes: RequiredMap<ArrayUnionAggExprArgs> = {
    ...super.argTypes,
  };

  declare args: ArrayUnionAggExprArgs;

  constructor (args: ArrayUnionAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AvgExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class AvgExpr extends AggFuncExpr {
  key = ExpressionKey.AVG;

  static argTypes: RequiredMap<AvgExprArgs> = {
    ...super.argTypes,
  };

  declare args: AvgExprArgs;

  constructor (args: AvgExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AnyValueExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class AnyValueExpr extends AggFuncExpr {
  key = ExpressionKey.ANY_VALUE;

  static argTypes: RequiredMap<AnyValueExprArgs> = {
    ...super.argTypes,
  };

  declare args: AnyValueExprArgs;

  constructor (args: AnyValueExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LagExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    offset?: Expression;
    default?: Expression;
  },
]>;

export class LagExpr extends AggFuncExpr {
  key = ExpressionKey.LAG;

  static argTypes: RequiredMap<LagExprArgs> = {
    ...super.argTypes,
    this: true,
    offset: false,
    default: false,
  };

  declare args: LagExprArgs;

  constructor (args: LagExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $offset (): Expression | undefined {
    return this.args.offset;
  }

  get $default (): Expression | undefined {
    return this.args.default;
  }

  static {
    this.register();
  }
}

export type LeadExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    offset?: Expression;
    default?: Expression;
  },
]>;

export class LeadExpr extends AggFuncExpr {
  key = ExpressionKey.LEAD;

  static argTypes: RequiredMap<LeadExprArgs> = {
    ...super.argTypes,
    this: true,
    offset: false,
    default: false,
  };

  declare args: LeadExprArgs;

  constructor (args: LeadExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $offset (): Expression | undefined {
    return this.args.offset;
  }

  get $default (): Expression | undefined {
    return this.args.default;
  }

  static {
    this.register();
  }
}

export type FirstExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class FirstExpr extends AggFuncExpr {
  key = ExpressionKey.FIRST;

  static argTypes: RequiredMap<FirstExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: FirstExprArgs;

  constructor (args: FirstExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type LastExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class LastExpr extends AggFuncExpr {
  key = ExpressionKey.LAST;

  static argTypes: RequiredMap<LastExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: LastExprArgs;

  constructor (args: LastExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type FirstValueExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class FirstValueExpr extends AggFuncExpr {
  key = ExpressionKey.FIRST_VALUE;

  static argTypes: RequiredMap<FirstValueExprArgs> = {
    ...super.argTypes,
  };

  declare args: FirstValueExprArgs;

  constructor (args: FirstValueExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LastValueExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class LastValueExpr extends AggFuncExpr {
  key = ExpressionKey.LAST_VALUE;

  static argTypes: RequiredMap<LastValueExprArgs> = {
    ...super.argTypes,
  };

  declare args: LastValueExprArgs;

  constructor (args: LastValueExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type NthValueExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    offset: Expression;
    fromFirst?: Expression;
  },
]>;

export class NthValueExpr extends AggFuncExpr {
  key = ExpressionKey.NTH_VALUE;

  static argTypes: RequiredMap<NthValueExprArgs> = {
    ...super.argTypes,
    this: true,
    offset: true,
    fromFirst: false,
  };

  declare args: NthValueExprArgs;

  constructor (args: NthValueExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $offset (): Expression {
    return this.args.offset;
  }

  get $fromFirst (): Expression | undefined {
    return this.args.fromFirst;
  }

  static {
    this.register();
  }
}

export type ObjectAggExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class ObjectAggExpr extends AggFuncExpr {
  key = ExpressionKey.OBJECT_AGG;

  static argTypes: RequiredMap<ObjectAggExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: ObjectAggExprArgs;

  constructor (args: ObjectAggExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type TryCastExprArgs = Merge<[
  CastExprArgs,
  { requiresString?: Expression },
]>;

export class TryCastExpr extends CastExpr {
  key = ExpressionKey.TRY_CAST;

  static argTypes: RequiredMap<TryCastExprArgs> = {
    ...super.argTypes,
    requiresString: false,
  };

  declare args: TryCastExprArgs;

  constructor (args: TryCastExprArgs) {
    super(args);
  }

  get $requiresString (): Expression | undefined {
    return this.args.requiresString;
  }
}

export type JSONCastExprArgs = Merge<[
  CastExprArgs,
]>;

export class JSONCastExpr extends CastExpr {
  key = ExpressionKey.JSON_CAST;

  declare args: JSONCastExprArgs;

  constructor (args: JSONCastExprArgs) {
    super(args);
  }
}

export type ConcatWsExprArgs = Merge<[
  ConcatExprArgs,
]>;

export class ConcatWsExpr extends ConcatExpr {
  key = ExpressionKey.CONCAT_WS;

  static _sqlNames = ['CONCAT_WS'];

  static argTypes: RequiredMap<ConcatWsExprArgs> = {
    ...super.argTypes,
  };

  declare args: ConcatWsExprArgs;

  constructor (args: ConcatWsExprArgs) {
    super(args);
  }
}

export type CountExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
    bigInt?: boolean;
  },
]>;

export class CountExpr extends AggFuncExpr {
  key = ExpressionKey.COUNT;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<CountExprArgs> = {
    ...super.argTypes,
    this: false,
    expressions: false,
    bigInt: false,
  };

  declare args: CountExprArgs;

  constructor (args: CountExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $bigInt (): boolean | undefined {
    return this.args.bigInt;
  }

  static {
    this.register();
  }
}

export type CountIfExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class CountIfExpr extends AggFuncExpr {
  key = ExpressionKey.COUNT_IF;

  static argTypes: RequiredMap<CountIfExprArgs> = {
    ...super.argTypes,
  };

  declare args: CountIfExprArgs;

  constructor (args: CountIfExprArgs) {
    super(args);
  }

  static _sqlNames = ['COUNT_IF', 'COUNTIF'];

  static {
    this.register();
  }
}

export type DenseRankExprArgs = Merge<[
  AggFuncExprArgs,
  { expressions?: Expression[] },
]>;

export class DenseRankExpr extends AggFuncExpr {
  key = ExpressionKey.DENSE_RANK;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<DenseRankExprArgs> = {
    ...super.argTypes,
    expressions: false,
  };

  declare args: DenseRankExprArgs;

  constructor (args: DenseRankExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type ExplodeOuterExprArgs = Merge<[
  ExplodeExprArgs,
]>;
export class ExplodeOuterExpr extends ExplodeExpr {
  key = ExpressionKey.EXPLODE_OUTER;

  static argTypes: RequiredMap<ExplodeOuterExprArgs> = {
    ...super.argTypes,
  };

  declare args: ExplodeOuterExprArgs;

  constructor (args: ExplodeOuterExprArgs) {
    super(args);
  }
}

export type PosexplodeExprArgs = Merge<[
  ExplodeExprArgs,
]>;
export class PosexplodeExpr extends ExplodeExpr {
  key = ExpressionKey.POSEXPLODE;

  static argTypes: RequiredMap<PosexplodeExprArgs> = {
    ...super.argTypes,
  };

  declare args: PosexplodeExprArgs;

  constructor (args: PosexplodeExprArgs) {
    super(args);
  }
}

export type GroupConcatExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    separator?: Expression;
    onOverflow?: Expression;
  },
]>;

export class GroupConcatExpr extends AggFuncExpr {
  key = ExpressionKey.GROUP_CONCAT;

  /**
   * Defines the arguments (properties and child expressions) for GroupConcat expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<GroupConcatExprArgs> = {
    ...super.argTypes,
    this: true,
    separator: false,
    onOverflow: false,
  };

  declare args: GroupConcatExprArgs;

  constructor (args: GroupConcatExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $separator (): Expression | undefined {
    return this.args.separator;
  }

  get $onOverflow (): Expression | undefined {
    return this.args.onOverflow;
  }

  static {
    this.register();
  }
}

export type LowerHexExprArgs = Merge<[
  HexExprArgs,
]>;
export class LowerHexExpr extends HexExpr {
  key = ExpressionKey.LOWER_HEX;

  static argTypes: RequiredMap<LowerHexExprArgs> = {};

  declare args: LowerHexExprArgs;

  constructor (args: LowerHexExprArgs) {
    super(args);
  }
}

export type AndExprArgs = Merge<[
  ConnectorExprArgs,
]>;
export class AndExpr extends multiInherit(ConnectorExpr, FuncExpr) {
  key = ExpressionKey.AND;

  static argTypes: RequiredMap<AndExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: AndExprArgs;

  constructor (args: AndExprArgs) {
    super(args);
  }
}

export type OrExprArgs = Merge<[
  ConnectorExprArgs,
]>;
export class OrExpr extends multiInherit(ConnectorExpr, FuncExpr) {
  key = ExpressionKey.OR;

  static argTypes: RequiredMap<OrExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: OrExprArgs;

  constructor (args: OrExprArgs) {
    super(args);
  }
}

export type XorExprArgs = Merge<[
  ConnectorExprArgs,
  FuncExpr,
  {
    this?: Expression;
    expression?: Expression;
    expressions?: Expression[];
    roundInput?: Expression;
  },
]>;

export class XorExpr extends multiInherit(ConnectorExpr, FuncExpr) {
  key = ExpressionKey.XOR;

  static isVarLenArgs = true;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: false,
    expression: false,
    expressions: false,
    roundInput: false,
  } as RequiredMap<XorExprArgs>;

  declare args: XorExprArgs;

  constructor (args: XorExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $roundInput (): Expression | undefined {
    return this.args.roundInput;
  }
}

export type JSONObjectAggExprArgs = Merge<[
  AggFuncExprArgs,
  {
    nullHandling?: Expression;
    uniqueKeys?: Expression[];
    returnType?: DataTypeExpr;
    encoding?: Expression;
  },
]>;

export class JSONObjectAggExpr extends AggFuncExpr {
  key = ExpressionKey.JSON_OBJECT_AGG;

  /**
   * Defines the arguments (properties and child expressions) for JSONObjectAgg expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JSONObjectAggExprArgs> = {
    ...super.argTypes,
    nullHandling: false,
    uniqueKeys: false,
    returnType: false,
    encoding: false,
  };

  declare args: JSONObjectAggExprArgs;

  constructor (args: JSONObjectAggExprArgs) {
    super(args);
  }

  get $nullHandling (): Expression | undefined {
    return this.args.nullHandling;
  }

  get $uniqueKeys (): Expression[] | undefined {
    return this.args.uniqueKeys;
  }

  get $returnType (): DataTypeExpr | undefined {
    return this.args.returnType;
  }

  get $encoding (): Expression | undefined {
    return this.args.encoding;
  }

  static {
    this.register();
  }
}

export type JSONBObjectAggExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class JSONBObjectAggExpr extends AggFuncExpr {
  key = ExpressionKey.JSONB_OBJECT_AGG;

  static argTypes: RequiredMap<JSONBObjectAggExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: JSONBObjectAggExprArgs;

  constructor (args: JSONBObjectAggExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type JSONArrayAggExprArgs = Merge<[
  AggFuncExprArgs,
  {
    order?: Expression;
    nullHandling?: Expression;
    returnType?: DataTypeExpr;
    strict?: Expression;
  },
]>;

export class JSONArrayAggExpr extends AggFuncExpr {
  key = ExpressionKey.JSON_ARRAY_AGG;

  /**
   * Defines the arguments (properties and child expressions) for JSONArrayAgg expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<JSONArrayAggExprArgs> = {
    ...super.argTypes,
    order: false,
    nullHandling: false,
    returnType: false,
    strict: false,
  };

  declare args: JSONArrayAggExprArgs;

  constructor (args: JSONArrayAggExprArgs) {
    super(args);
  }

  get $order (): Expression | undefined {
    return this.args.order;
  }

  get $nullHandling (): Expression | undefined {
    return this.args.nullHandling;
  }

  get $returnType (): DataTypeExpr | undefined {
    return this.args.returnType;
  }

  get $strict (): Expression | undefined {
    return this.args.strict;
  }

  static {
    this.register();
  }
}

export type LogicalOrExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class LogicalOrExpr extends AggFuncExpr {
  key = ExpressionKey.LOGICAL_OR;

  static argTypes: RequiredMap<LogicalOrExprArgs> = {
    ...super.argTypes,
  };

  declare args: LogicalOrExprArgs;

  constructor (args: LogicalOrExprArgs) {
    super(args);
  }

  static _sqlNames = [
    'LOGICAL_OR',
    'BOOL_OR',
    'BOOLOR_AGG',
  ];

  static {
    this.register();
  }
}

export type LogicalAndExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class LogicalAndExpr extends AggFuncExpr {
  key = ExpressionKey.LOGICAL_AND;

  static argTypes: RequiredMap<LogicalAndExprArgs> = {
    ...super.argTypes,
  };

  declare args: LogicalAndExprArgs;

  constructor (args: LogicalAndExprArgs) {
    super(args);
  }

  static _sqlNames = [
    'LOGICAL_AND',
    'BOOL_AND',
    'BOOLAND_AGG',
  ];

  static {
    this.register();
  }
}

export type MaxExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class MaxExpr extends AggFuncExpr {
  key = ExpressionKey.MAX;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<MaxExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
  };

  declare args: MaxExprArgs;

  constructor (args: MaxExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type MedianExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class MedianExpr extends AggFuncExpr {
  key = ExpressionKey.MEDIAN;

  static argTypes: RequiredMap<MedianExprArgs> = {
    ...super.argTypes,
  };

  declare args: MedianExprArgs;

  constructor (args: MedianExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ModeExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    deterministic?: Expression;
  },
]>;

export class ModeExpr extends AggFuncExpr {
  key = ExpressionKey.MODE;

  static argTypes: RequiredMap<ModeExprArgs> = {
    ...super.argTypes,
    this: false,
    deterministic: false,
  };

  declare args: ModeExprArgs;

  constructor (args: ModeExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $deterministic (): Expression | undefined {
    return this.args.deterministic;
  }

  static {
    this.register();
  }
}

export type MinExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class MinExpr extends AggFuncExpr {
  key = ExpressionKey.MIN;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<MinExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
  };

  declare args: MinExprArgs;

  constructor (args: MinExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type NtileExprArgs = Merge<[
  AggFuncExprArgs,
  { this?: Expression },
]>;

export class NtileExpr extends AggFuncExpr {
  key = ExpressionKey.NTILE;

  static argTypes: RequiredMap<NtileExprArgs> = {
    ...super.argTypes,
    this: false,
  };

  declare args: NtileExprArgs;

  constructor (args: NtileExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type PercentileContExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class PercentileContExpr extends AggFuncExpr {
  key = ExpressionKey.PERCENTILE_CONT;

  static argTypes: RequiredMap<PercentileContExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: PercentileContExprArgs;

  constructor (args: PercentileContExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type PercentileDiscExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression?: Expression;
  },
]>;

export class PercentileDiscExpr extends AggFuncExpr {
  key = ExpressionKey.PERCENTILE_DISC;

  static argTypes: RequiredMap<PercentileDiscExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: false,
  };

  declare args: PercentileDiscExprArgs;

  constructor (args: PercentileDiscExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): ExpressionValue {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type PercentRankExprArgs = Merge<[
  AggFuncExprArgs,
  { expressions?: Expression[] },
]>;

export class PercentRankExpr extends AggFuncExpr {
  key = ExpressionKey.PERCENT_RANK;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<PercentRankExprArgs> = {
    ...super.argTypes,
    expressions: false,
  };

  declare args: PercentRankExprArgs;

  constructor (args: PercentRankExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type QuantileExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    quantile: Expression;
  },
]>;

export class QuantileExpr extends AggFuncExpr {
  key = ExpressionKey.QUANTILE;

  static argTypes: RequiredMap<QuantileExprArgs> = {
    ...super.argTypes,
    this: true,
    quantile: true,
  };

  declare args: QuantileExprArgs;

  constructor (args: QuantileExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $quantile (): Expression {
    return this.args.quantile;
  }

  static {
    this.register();
  }
}

export type ApproxPercentileAccumulateExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class ApproxPercentileAccumulateExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_PERCENTILE_ACCUMULATE;

  static argTypes: RequiredMap<ApproxPercentileAccumulateExprArgs> = {
    ...super.argTypes,
  };

  declare args: ApproxPercentileAccumulateExprArgs;

  constructor (args: ApproxPercentileAccumulateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RankExprArgs = Merge<[
  AggFuncExprArgs,
  { expressions?: Expression[] },
]>;

export class RankExpr extends AggFuncExpr {
  key = ExpressionKey.RANK;

  static isVarLenArgs = true;

  static argTypes: RequiredMap<RankExprArgs> = {
    ...super.argTypes,
    expressions: false,
  };

  declare args: RankExprArgs;

  constructor (args: RankExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type RegrValxExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class RegrValxExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_VALX;

  static argTypes: RequiredMap<RegrValxExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: RegrValxExprArgs;

  constructor (args: RegrValxExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type RegrValyExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class RegrValyExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_VALY;

  static argTypes: RequiredMap<RegrValyExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: RegrValyExprArgs;

  constructor (args: RegrValyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type RegrAvgyExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class RegrAvgyExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_AVGY;

  static argTypes: RequiredMap<RegrAvgyExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: RegrAvgyExprArgs;

  constructor (args: RegrAvgyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type RegrAvgxExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class RegrAvgxExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_AVGX;

  static argTypes: RequiredMap<RegrAvgxExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: RegrAvgxExprArgs;

  constructor (args: RegrAvgxExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type RegrCountExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class RegrCountExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_COUNT;

  static argTypes: RequiredMap<RegrCountExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: RegrCountExprArgs;

  constructor (args: RegrCountExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type RegrInterceptExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class RegrInterceptExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_INTERCEPT;

  static argTypes: RequiredMap<RegrInterceptExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: RegrInterceptExprArgs;

  constructor (args: RegrInterceptExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type RegrR2ExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class RegrR2Expr extends AggFuncExpr {
  key = ExpressionKey.REGR_R2;

  static argTypes: RequiredMap<RegrR2ExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: RegrR2ExprArgs;

  constructor (args: RegrR2ExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type RegrSxxExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class RegrSxxExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_SXX;

  static argTypes: RequiredMap<RegrSxxExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: RegrSxxExprArgs;

  constructor (args: RegrSxxExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type RegrSxyExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class RegrSxyExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_SXY;

  static argTypes: RequiredMap<RegrSxyExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: RegrSxyExprArgs;

  constructor (args: RegrSxyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type RegrSyyExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class RegrSyyExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_SYY;

  static argTypes: RequiredMap<RegrSyyExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: RegrSyyExprArgs;

  constructor (args: RegrSyyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type RegrSlopeExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class RegrSlopeExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_SLOPE;

  static argTypes: RequiredMap<RegrSlopeExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: RegrSlopeExprArgs;

  constructor (args: RegrSlopeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type SumExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class SumExpr extends AggFuncExpr {
  key = ExpressionKey.SUM;

  static argTypes: RequiredMap<SumExprArgs> = {
    ...super.argTypes,
  };

  declare args: SumExprArgs;

  constructor (args: SumExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StddevExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class StddevExpr extends AggFuncExpr {
  key = ExpressionKey.STDDEV;

  static argTypes: RequiredMap<StddevExprArgs> = {
    ...super.argTypes,
  };

  declare args: StddevExprArgs;

  constructor (args: StddevExprArgs) {
    super(args);
  }

  static _sqlNames = ['STDDEV', 'STDEV'];

  static {
    this.register();
  }
}

export type StddevPopExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class StddevPopExpr extends AggFuncExpr {
  key = ExpressionKey.STDDEV_POP;

  static argTypes: RequiredMap<StddevPopExprArgs> = {
    ...super.argTypes,
  };

  declare args: StddevPopExprArgs;

  constructor (args: StddevPopExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StddevSampExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class StddevSampExpr extends AggFuncExpr {
  key = ExpressionKey.STDDEV_SAMP;

  static argTypes: RequiredMap<StddevSampExprArgs> = {
    ...super.argTypes,
  };

  declare args: StddevSampExprArgs;

  constructor (args: StddevSampExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CumeDistExprArgs = Merge<[
  AggFuncExprArgs,
  { expressions?: Expression[] },
]>;

export class CumeDistExpr extends AggFuncExpr {
  key = ExpressionKey.CUME_DIST;

  static isVarLenArgs = true;
  static argTypes: RequiredMap<CumeDistExprArgs> = {
    expressions: false,
  };

  declare args: CumeDistExprArgs;

  constructor (args: CumeDistExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type VarianceExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class VarianceExpr extends AggFuncExpr {
  key = ExpressionKey.VARIANCE;

  static argTypes: RequiredMap<VarianceExprArgs> = {
    ...super.argTypes,
  };

  declare args: VarianceExprArgs;

  constructor (args: VarianceExprArgs) {
    super(args);
  }

  static _sqlNames = [
    'VARIANCE',
    'VARIANCE_SAMP',
    'VAR_SAMP',
  ];

  static {
    this.register();
  }
}

export type VariancePopExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class VariancePopExpr extends AggFuncExpr {
  key = ExpressionKey.VARIANCE_POP;

  static argTypes: RequiredMap<VariancePopExprArgs> = {
    ...super.argTypes,
  };

  declare args: VariancePopExprArgs;

  constructor (args: VariancePopExprArgs) {
    super(args);
  }

  static _sqlNames = ['VARIANCE_POP', 'VAR_POP'];

  static {
    this.register();
  }
}

export type KurtosisExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class KurtosisExpr extends AggFuncExpr {
  key = ExpressionKey.KURTOSIS;

  static argTypes: RequiredMap<KurtosisExprArgs> = {
    ...super.argTypes,
  };

  declare args: KurtosisExprArgs;

  constructor (args: KurtosisExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SkewnessExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class SkewnessExpr extends AggFuncExpr {
  key = ExpressionKey.SKEWNESS;

  static argTypes: RequiredMap<SkewnessExprArgs> = {
    ...super.argTypes,
  };

  declare args: SkewnessExprArgs;

  constructor (args: SkewnessExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CovarSampExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class CovarSampExpr extends AggFuncExpr {
  key = ExpressionKey.COVAR_SAMP;

  static argTypes: RequiredMap<CovarSampExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: CovarSampExprArgs;

  constructor (args: CovarSampExprArgs) {
    super(args);
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

export type CovarPopExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this: Expression;
    expression: Expression;
  },
]>;

export class CovarPopExpr extends AggFuncExpr {
  key = ExpressionKey.COVAR_POP;

  static argTypes: RequiredMap<CovarPopExprArgs> = {
    ...super.argTypes,
    this: true,
    expression: true,
  };

  declare args: CovarPopExprArgs;

  constructor (args: CovarPopExprArgs) {
    super(args);
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $this (): Expression {
    return this.args.this;
  }

  static {
    this.register();
  }
}

/**
 * https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators
 */
export type CombinedAggFuncExprArgs = Merge<[
  AnonymousAggFuncExprArgs,
  {
    this: Expression;
    expressions?: Expression[];
  },
]>;

export class CombinedAggFuncExpr extends AnonymousAggFuncExpr {
  key = ExpressionKey.COMBINED_AGG_FUNC;

  static argTypes: RequiredMap<CombinedAggFuncExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: false,
  };

  declare args: CombinedAggFuncExprArgs;

  constructor (args: CombinedAggFuncExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }
}

export type CombinedParameterizedAggExprArgs = Merge<[
  ParameterizedAggExprArgs,
  {
    this: Expression;
    expressions: Expression[];
    params: Expression[];
  },
]>;

export class CombinedParameterizedAggExpr extends ParameterizedAggExpr {
  key = ExpressionKey.COMBINED_PARAMETERIZED_AGG;

  static argTypes: RequiredMap<CombinedParameterizedAggExprArgs> = {
    ...super.argTypes,
    this: true,
    expressions: true,
    params: true,
  };

  declare args: CombinedParameterizedAggExprArgs;

  constructor (args: CombinedParameterizedAggExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $params (): Expression[] {
    return this.args.params;
  }

  static {
    this.register();
  }
}

export type PosexplodeOuterExprArgs = Merge<[
  ExplodeExprArgs,
]>;
export class PosexplodeOuterExpr extends multiInherit(PosexplodeExpr, ExplodeOuterExpr) {
  key = ExpressionKey.POSEXPLODE_OUTER;

  static argTypes: RequiredMap<PosexplodeOuterExprArgs> = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  };

  declare args: PosexplodeOuterExprArgs;

  constructor (args: PosexplodeOuterExprArgs) {
    super(args);
  }
}

export type ApproxQuantileExprArgs = Merge<[
  QuantileExprArgs,
  {
    quantile: Expression;
    accuracy?: Expression;
    weight?: Expression;
    errorTolerance?: Expression;
  },
]>;

export class ApproxQuantileExpr extends QuantileExpr {
  key = ExpressionKey.APPROX_QUANTILE;

  /**
   * Defines the arguments (properties and child expressions) for ApproxQuantile expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes: RequiredMap<ApproxQuantileExprArgs> = {
    ...super.argTypes,
    quantile: true,
    accuracy: false,
    weight: false,
    errorTolerance: false,
  };

  declare args: ApproxQuantileExprArgs;

  constructor (args: ApproxQuantileExprArgs) {
    super(args);
  }

  get $quantile (): Expression {
    return this.args.quantile;
  }

  get $accuracy (): Expression | undefined {
    return this.args.accuracy;
  }

  get $weight (): Expression | undefined {
    return this.args.weight;
  }

  get $errorTolerance (): Expression | undefined {
    return this.args.errorTolerance;
  }
}

/**
 * Create a column expression (optionally qualified with table name).
 *
 * @param name - Column name
 * @param table - Optional table name
 * @returns Column expression
 *
 * @example
 * // Simple column: name
 * const col = column('name');
 *
 * @example
 * // Qualified column: users.id
 * const col = column('id', 'users');
 */
/**
 * Build a Column.
 *
 * Example:
 *     column('col', 'table').sql()
 *     // 'table.col'
 *
 *     column('col', 'table', { fields: ['field1', 'field2'] }).sql()
 *     // 'table.col.field1.field2'
 *
 * @param col - Column name (can be string, Identifier, or Star)
 * @param table - Table name
 * @param options - Options object
 * @param options.db - Database name
 * @param options.catalog - Catalog name
 * @param options.fields - Additional fields using dots
 * @param options.quoted - Whether to force quotes on the column's identifiers
 * @param options.copy - Whether to copy identifiers if passed in
 * @returns The new Column or Dot instance
 */
export function column (
  columnRef: {
    col: string | IdentifierExpr;
    table?: string | IdentifierExpr;
    db?: string | IdentifierExpr;
    catalog?: string | IdentifierExpr;
  },
  options: {
    fields?: (string | IdentifierExpr)[];
    quoted?: boolean;
    copy?: boolean;
  } = {},
): ColumnExpr | DotExpr {
  const {
    col, table, db, catalog,
  } = columnRef;

  const {
    fields, quoted, copy = true,
  } = options;

  let colIdent: IdentifierExpr | StarExpr;
  if (col instanceof StarExpr) {
    colIdent = col;
  } else {
    colIdent = toIdentifier(col, {
      quoted,
      copy,
    });
  }

  const columnExpr: ColumnExpr = new ColumnExpr({
    this: colIdent,
    table: table !== undefined
      ? toIdentifier(table, {
        quoted,
        copy,
      })
      : undefined,
    db: db !== undefined
      ? toIdentifier(db, {
        quoted,
        copy,
      })
      : undefined,
    catalog: catalog !== undefined
      ? toIdentifier(catalog, {
        quoted,
        copy,
      })
      : undefined,
  });

  if (fields && 0 < fields.length) {
    const fieldIdents = fields.map((field) => toIdentifier(field, {
      quoted,
      copy,
    })).filter((f): f is IdentifierExpr => f !== undefined);
    return DotExpr.build([columnExpr, ...fieldIdents]);
  }

  return columnExpr;
}

/**
 * Create a table expression (optionally qualified with database and catalog).
 *
 * @param name - Table name
 * @param db - Optional database name
 * @param catalog - Optional catalog name
 * @returns Table expression
 *
 * @example
 * // Simple table: users
 * const tbl = table('users');
 *
 * @example
 * // Fully qualified: catalog.database.table
 * const tbl = table('users', 'mydb', 'mycatalog');
 */
/**
 * Build a Table.
 *
 * Example:
 *     table_('users', { quoted: true }).sql()
 *     // '"users"'
 *
 *     table_('users', { db: 'mydb', catalog: 'mycatalog' }).sql()
 *     // 'mycatalog.mydb.users'
 *
 * @param tableName - Table name
 * @param options - Options object
 * @param options.db - Database name
 * @param options.catalog - Catalog name
 * @param options.quoted - Whether to force quotes on the table's identifiers
 * @param options.alias - Table's alias
 * @returns The new Table instance
 */
export function table_ (
  tableName: string | IdentifierExpr,
  options: {
    db?: string | IdentifierExpr;
    catalog?: string | IdentifierExpr;
    quoted?: boolean;
    alias?: string | IdentifierExpr;
  } = {},
): TableExpr {
  const {
    db, catalog, quoted, alias: aliasName,
  } = options;

  return new TableExpr({
    this: tableName ? toIdentifier(tableName, { quoted }) : undefined,
    db: db ? toIdentifier(db, { quoted }) : undefined,
    catalog: catalog ? toIdentifier(catalog, { quoted }) : undefined,
    alias: aliasName ? new TableAliasExpr({ this: toIdentifier(aliasName) }) : undefined,
  });
}

export function table (name: string, db?: string, catalog?: string): TableExpr {
  const args: TableExprArgs = { this: new IdentifierExpr({ this: name }) };
  if (db) {
    args.db = new IdentifierExpr({ this: db });
  }
  if (catalog) {
    args.catalog = new IdentifierExpr({ this: catalog });
  }
  return new TableExpr(args);
}

/**
 * Create an AND expression from multiple conditions.
 * Automatically chains multiple conditions with AND.
 *
 * @param conditions - Conditions to AND together (nulls are filtered out)
 * @param options - Options object
 * @param options.dialect - The dialect to use for parsing
 * @param options.copy - Whether to copy expressions (handled by caller)
 * @param options.wrap - Whether to wrap in Parens
 * @returns AND expression or single condition if only one provided
 *
 * @example
 * // WHERE a = 1 AND b = 2 AND c = 3
 * const condition = and([cond1, cond2, cond3]);
 *
 * @example
 * // With options
 * const condition = and([cond1, cond2], { wrap: true });
 */
/**
 * Combine multiple conditions with an AND logical operator.
 *
 * Example:
 *     and(["x=1", and(["y=1", "z=1"])]).sql()
 *     // 'x = 1 AND (y = 1 AND z = 1)'
 *
 * @param expressions - The SQL code strings to parse. If an Expression instance is passed, this is used as-is.
 * @param options - Options object
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy expressions (only applies to Expressions)
 * @param options.wrap - Whether to wrap the operands in Parens
 * @returns The new condition
 */
export function and (
  expressions?: string | Expression | (string | Expression | undefined)[],
  options: {
    dialect?: DialectType;
    copy?: boolean;
    wrap?: boolean;
    [key: string]: unknown;
  } = {},
): ConditionExpr {
  const {
    dialect, copy = true, wrap = true, ...opts
  } = options;
  return _combine(expressions, AndExpr, {
    dialect,
    copy,
    wrap,
    ...opts,
  }) as ConditionExpr;
}

/**
 * Combine multiple conditions with an OR logical operator.
 *
 * Example:
 *     or(["x=1", or(["y=1", "z=1"])]).sql()
 *     // 'x = 1 OR (y = 1 OR z = 1)'
 *
 * @param expressions - The SQL code strings to parse. If an Expression instance is passed, this is used as-is.
 * @param options - Options object
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy expressions (only applies to Expressions)
 * @param options.wrap - Whether to wrap the operands in Parens
 * @returns The new condition
 */
export function or (
  expressions?: string | Expression | (string | Expression | undefined)[],
  options: {
    dialect?: DialectType;
    copy?: boolean;
    wrap?: boolean;
    [key: string]: unknown;
  } = {},
): ConditionExpr {
  const {
    dialect, copy = true, wrap = true, ...opts
  } = options;
  return _combine(expressions, OrExpr, {
    dialect,
    copy,
    wrap,
    ...opts,
  }) as ConditionExpr;
}

/**
 * Combine multiple conditions with an XOR logical operator.
 *
 * Example:
 *     xor(["x=1", xor(["y=1", "z=1"])]).sql()
 *     // 'x = 1 XOR (y = 1 XOR z = 1)'
 *
 * @param expressions - The SQL code strings to parse. If an Expression instance is passed, this is used as-is.
 * @param options - Options object
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy expressions (only applies to Expressions)
 * @param options.wrap - Whether to wrap the operands in Parens
 * @returns The new condition
 */
export function xor (
  expressions?: string | Expression | (string | Expression | undefined)[],
  options: {
    dialect?: DialectType;
    copy?: boolean;
    wrap?: boolean;
    [key: string]: unknown;
  } = {},
): ConditionExpr {
  const {
    dialect, copy = true, wrap = true, ...opts
  } = options;
  return _combine(expressions, XorExpr, {
    dialect,
    copy,
    wrap,
    ...opts,
  }) as ConditionExpr;
}

/**
 * Wrap a condition with a NOT operator.
 *
 * Example:
 *     not_("this_suit='black'").sql()
 *     // "NOT this_suit = 'black'"
 *
 * @param expression - The SQL code string to parse. If an Expression instance is passed, this is used as-is.
 * @param options - Options object
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy the expression
 * @returns The new condition
 */
export function not (
  expression: string | Expression,
  options: {
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): NotExpr {
  const thisExpr = condition(expression, options);
  return new NotExpr({ this: _wrap(thisExpr, ConnectorExpr) || thisExpr });
}

/**
 * Wrap an expression in parentheses.
 *
 * Example:
 *     paren("5 + 3").sql()
 *     // '(5 + 3)'
 *
 * @param expression - The SQL code string to parse.
 *                     If an Expression instance is passed, this is used as-is.
 * @param options - Options object
 * @param options.copy - Whether to copy the expression or not
 * @returns The wrapped expression
 */
export function paren (
  expression: string | Expression,
  options: {
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): ParenExpr {
  const {
    copy = true, ...opts
  } = options;
  return new ParenExpr({
    this: maybeParse(expression, {
      copy,
      ...opts,
    }),
  });
}

const SAFE_IDENTIFIER_RE = /^[_a-zA-Z][\w]*$/;

/**
 * Builds an identifier.
 *
 * Example:
 *     toIdentifier("my_column").sql()
 *     // 'my_column'
 *     toIdentifier("column name", { quoted: true }).sql()
 *     // '"column name"'
 *
 * @param name - The name to turn into an identifier
 * @param options - Options object
 * @param options.quoted - Whether to force quote the identifier
 * @param options.copy - Whether to copy name if it's an Identifier
 * @returns The identifier ast node or undefined if name is undefined
 */
export function toIdentifier (
  name: string | IdentifierExpr,
  options: {
    quoted?: boolean;
    copy?: boolean;
  } = {},
): IdentifierExpr {
  const {
    quoted, copy = true,
  } = options;

  if (name instanceof IdentifierExpr) {
    return maybeCopy(name, copy) as IdentifierExpr;
  }

  return new IdentifierExpr({
    this: name,
    quoted: quoted !== undefined ? quoted : !SAFE_IDENTIFIER_RE.test(name),
  });

  // throw new Error(`Name needs to be a string or an Identifier, got: ${name?.constructor?.name}`);
}

/**
 * Parses a given string into an identifier.
 *
 * Example:
 *     parseIdentifier("my_table").sql()
 *     // 'my_table'
 *
 * @param name - The name to parse into an identifier
 * @param options - Options object
 * @param options.dialect - The dialect to parse against
 * @returns The identifier ast node
 */
export function parseIdentifier (
  name: string | IdentifierExpr,
  options: {
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): IdentifierExpr {
  const {
    dialect, ...opts
  } = options;
  try {
    return maybeParse(name, {
      dialect,
      into: IdentifierExpr,
      ...opts,
    }) as IdentifierExpr;
  } catch {
    return toIdentifier(name) as IdentifierExpr;
  }
}

/**
 * Matches interval strings like "1 day" or "5.5 months"
 * Captures: (number, unit)
 */
export const INTERVAL_STRING_RE = /\s*(-?[0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]+)\s*/;

/**
 * Matches day-time interval strings that contain:
 * - A number of days (possibly negative or with decimals)
 * - At least one space
 * - Portions of a time-like signature, potentially negative
 *   - Standard format                   [-]h+:m+:s+[.f+]
 *   - Just minutes/seconds/frac seconds [-]m+:s+.f+
 *   - Just hours, minutes, maybe colon  [-]h+:m+[:]
 *   - Just hours, maybe colon           [-]h+[:]
 *   - Just colon                        :
 */
export const INTERVAL_DAY_TIME_RE = /\s*-?\s*\d+(?:\.\d+)?\s+(?:-?(?:\d+:)?\d+:\d+(?:\.\d+)?|-?(?:\d+:){1,2}|:)\s*/;

/**
 * Builds an interval expression from a string like '1 day' or '5 months'.
 *
 * Example:
 *     toInterval("1 day").sql()
 *     // 'INTERVAL 1 DAY'
 *
 * @param interval - The interval string or Literal expression
 * @returns The interval expression
 */
export function toInterval (
  interval: string | LiteralExpr,
): IntervalExpr {
  let intervalStr: string;

  if (interval instanceof LiteralExpr) {
    if (!interval.args.isString) {
      throw new Error('Invalid interval string.');
    }
    intervalStr = interval.args.this;
  } else {
    intervalStr = interval;
  }

  const parsed = maybeParse(`INTERVAL ${intervalStr}`);
  if (!(parsed instanceof IntervalExpr)) {
    throw new Error('Failed to parse interval expression');
  }
  return parsed;
}

/**
 * Create an Alias expression.
 *
 * Example:
 *     alias_('foo', 'bar').sql()
 *     // 'foo AS bar'
 *
 *     alias_('(select 1, 2)', 'bar', { table: ['a', 'b'] }).sql()
 *     // '(SELECT 1, 2) AS bar(a, b)'
 *
 * @param expression - The SQL code string to parse.
 *                     If an Expression instance is passed, this is used as-is.
 * @param aliasName - The alias name to use. If the name has special characters it is quoted.
 * @param options - Options object
 * @param options.table - Whether to create a table alias, can also be a list of columns
 * @param options.quoted - Whether to quote the alias
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy the expression
 * @returns The aliased expression
 */
export function alias<E extends Expression> (
  expression: string | E,
  aliasName: string | IdentifierExpr | undefined,
  options: {
    table?: boolean | (string | IdentifierExpr)[];
    quoted?: boolean;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): E | AliasExpr {
  const {
    table: tableOpt, quoted, dialect, copy = true, ...opts
  } = options;

  const exp = maybeParse(expression, {
    dialect,
    copy,
    ...opts,
  });
  const aliasIdent = aliasName !== undefined ? toIdentifier(aliasName, { quoted }) : undefined;

  if (tableOpt) {
    const tableAlias = new TableAliasExpr({ this: aliasIdent });
    exp.setArgKey('alias', tableAlias);

    if (Array.isArray(tableOpt)) {
      for (const column of tableOpt) {
        const columnIdent = toIdentifier(column, { quoted });
        if (columnIdent) {
          tableAlias.append('columns', columnIdent);
        }
      }
    }

    return exp;
  }

  // We don't set the "alias" arg for Window expressions, because that would add an IDENTIFIER node in
  // the AST, representing a "named_window" construct (eg. bigquery). What we want is an ALIAS node
  // for the complete Window expression.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  if ((exp.constructor as any).argTypes['alias'] !== undefined && !(exp instanceof WindowExpr)) {
    if (aliasIdent) {
      exp.setArgKey('alias', aliasIdent);
    }
    return exp;
  }

  return new AliasExpr({
    this: exp,
    alias: aliasIdent,
  });
}

/**
 * Build a subquery expression that's selected from.
 *
 * Example:
 *     subquery('select x from tbl', 'bar').select(['x']).sql()
 *     // 'SELECT x FROM (SELECT x FROM tbl) AS bar'
 *
 * @param expression - The SQL code string to parse.
 *                     If an Expression instance is passed, this is used as-is.
 * @param aliasName - The alias name to use
 * @param options - Options object
 * @param options.dialect - The dialect used to parse the input expression
 * @returns A new Select instance with the subquery expression included
 */
export function subquery (
  expression: string | Expression,
  aliasName?: string | IdentifierExpr,
  options: {
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): SelectExpr {
  const {
    dialect, ...opts
  } = options;
  const parsed = maybeParse(expression, {
    dialect,
    ...opts,
  });
  if (!(parsed instanceof QueryExpr)) {
    throw new Error('The input sql is not a QueryExpr');
  }
  const subqueryExpr = parsed.subquery(aliasName, opts);
  return new SelectExpr({}).from(subqueryExpr, {
    dialect,
    ...opts,
  });
}

/**
 * Create a SELECT expression.
 *
 * @param columns - Columns to select (strings or expressions)
 * @returns SELECT expression
 *
 * @example
 * // SELECT col1, col2
 * const sel = select('col1', 'col2');
 *
 * @example
 * // SELECT users.id, users.name
 * const sel = select(
 *   column('id', 'users'),
 *   column('name', 'users')
 * );
 */
export function select (
  expressions?: string | Expression | (string | Expression | undefined)[],
  options: {
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): SelectExpr {
  return new SelectExpr({}).select(expressions, options);
}

/**
 * Initializes a syntax tree from a FROM expression.
 *
 * Example:
 *     from("tbl").select("col1", "col2").sql()
 *     // 'SELECT col1, col2 FROM tbl'
 *
 * @param expression - The SQL code string to parse as the FROM expression of a
 *                     SELECT statement. If an Expression instance is passed, this is used as-is.
 * @param options - Options object
 * @param options.dialect - The dialect used to parse the input expression
 * @returns The syntax tree for the SELECT statement
 */
export function from (
  expression: string | Expression,
  options: {
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): SelectExpr {
  return new SelectExpr({}).from(expression, options);
}

/**
 * Create a CASE expression
 * @param conditions - Condition-value pairs
 * @param defaultValue - Default value
 * @returns CASE expression
 */
/**
 * Initialize a CASE statement.
 *
 * Example:
 *     case_().when("a = 1", "foo").else_("bar")
 *
 * @param expression - Optionally, the input expression (not all dialects support this)
 * @param options - Extra options for parsing expression
 * @returns A Case expression
 */
export function case_ (
  expression?: string | Expression,
  options: {
    [key: string]: unknown;
  } = {},
): CaseExpr {
  let thisExpr: Expression | undefined;
  if (expression !== undefined) {
    thisExpr = maybeParse(expression, options);
  }
  return new CaseExpr({
    this: thisExpr,
    ifs: [],
  });
}

/**
 * Create a CAST expression.
 *
 * @param expr - Expression to cast
 * @param toType - Target data type (string or DataTypeExpr)
 * @returns CAST expression
 *
 * @example
 * // CAST(col AS VARCHAR)
 * const casted = cast(column('col'), 'VARCHAR');
 *
 * @example
 * // CAST(value AS INTEGER)
 * const casted = cast(literal('123'), DataTypeExpr.build('INTEGER'));
 */
/**
 * Cast an expression to a data type.
 *
 * Example:
 *     cast('x + 1', 'int').sql()
 *     // 'CAST(x + 1 AS INT)'
 *
 * @param expression - The expression to cast
 * @param to - The datatype to cast to
 * @param options - Options object
 * @param options.copy - Whether to copy the supplied expressions
 * @param options.dialect - The target dialect. This is used to prevent a re-cast in the following scenario:
 *                          - The expression to be cast is already a Cast expression
 *                          - The existing cast is to a type that is logically equivalent to new type
 *
 *                          For example, if expression='CAST(x as DATETIME)' and to=Type.TIMESTAMP,
 *                          but in the target dialect DATETIME is mapped to TIMESTAMP, then we will NOT return
 *                          CAST(x (as DATETIME) as TIMESTAMP) and instead just return the original expression
 *                          CAST(x as DATETIME).
 *
 *                          This is to prevent it being output as a double cast CAST(x (as TIMESTAMP) as TIMESTAMP)
 *                          once the DATETIME -> TIMESTAMP mapping is applied in the target dialect generator.
 * @returns The new Cast instance
 */
export function cast (
  expression: string | Expression,
  to: DataTypeExprKind,
  options: {
    copy?: boolean;
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): CastExpr {
  const {
    copy = true, dialect, ...opts
  } = options;

  const expr = maybeParse(expression, {
    copy,
    dialect,
    ...opts,
  });
  const dataType = DataTypeExpr.build(to, {
    copy,
    dialect,
    ...opts,
  });

  // Don't re-cast if the expression is already a cast to the correct type
  if (expr instanceof CastExpr) {
    // TODO: In Python, this uses dialect-specific type mapping:
    // target_dialect = Dialect.get_or_raise(dialect)
    // type_mapping = target_dialect.generator_class.TYPE_MAPPING
    // types_are_equivalent = type_mapping.get(existing_cast_type, existing_cast_type.value) == type_mapping.get(new_cast_type, new_cast_type.value)
    //
    // For now, we use a simpler check until TYPE_MAPPING is available in TypeScript

    if (expr.isType([dataType])) {
      return expr;
    }

    const existingCastType = expr.args.to?.args.this;
    const newCastType = dataType.args.this;

    if (existingCastType === newCastType) {
      return expr;
    }
  }

  const castExpr = new CastExpr({
    this: expr,
    to: dataType,
  });
  castExpr.type = dataType;

  return castExpr;
}

/**
 * Build VALUES statement.
 *
 * Example:
 *     values([[1, '2']]).sql()
 *     // "VALUES (1, '2')"
 *
 * @param valuesList - Values statements that will be converted to SQL (array of tuples/arrays)
 * @param options - Options object
 * @param options.alias - Optional alias
 * @param options.columns - Optional list of ordered column names. If provided then an alias is also required.
 * @returns The Values expression object
 */
export function values (
  valuesList: unknown[][],
  options: {
    alias?: string;
    columns?: (string | IdentifierExpr)[];
  } = {},
): ValuesExpr {
  const {
    alias: aliasName, columns,
  } = options;

  if (columns && !aliasName) {
    throw new Error('Alias is required when providing columns');
  }

  const expressions = valuesList.map((tup) => convert(tup));

  let alias: TableAliasExpr | undefined;
  if (columns) {
    alias = new TableAliasExpr({
      this: aliasName !== undefined ? toIdentifier(aliasName) : undefined,
      columns: columns.map((col) => toIdentifier(col)).filter((c): c is IdentifierExpr => c !== undefined),
    });
  } else if (aliasName) {
    alias = new TableAliasExpr({ this: toIdentifier(aliasName) });
  }

  return new ValuesExpr({
    expressions,
    alias,
  });
}

/**
 * Create a function expression
 * @param name - Function name
 * @param args - Function arguments
 * @returns Function expression
 */
export function func (name: string, ...args: Expression[]): FuncExpr {
  return new FuncExpr({
    this: name,
    expressions: args,
  });
}

/**
 * Create a UNION expression.
 *
 * @param left - Left query
 * @param right - Right query
 * @param distinct - Whether to use UNION DISTINCT (default: false for UNION ALL)
 * @returns UNION expression
 *
 * @example
 * // SELECT ... UNION ALL SELECT ...
 * const un = union(query1, query2);
 *
 * @example
 * // SELECT ... UNION DISTINCT SELECT ...
 * const un = union(query1, query2, true);
 */
/**
 * Helper function to build set operations (UNION, INTERSECT, EXCEPT) by chaining expressions.
 * @param expressions - The expressions to combine
 * @param setOperation - The set operation class constructor
 * @param options - Options including distinct, dialect, copy, etc.
 * @returns The chained set operation expression
 */
function _applySetOperation<S extends Expression> (
  expressions: (string | Expression)[],
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  setOperation: new (args: any) => S,
  options: {
    distinct?: boolean;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): S {
  const {
    distinct = true, dialect, copy = true, ...opts
  } = options;

  const parsedExpressions = expressions.map((e) =>
    maybeParse(e, {
      dialect,
      copy,
      ...opts,
    }));

  return parsedExpressions.reduce((left, right) =>
    new setOperation({
      this: left,
      expression: right,
      distinct,
      ...opts,
    })) as S;
}

/**
 * Initializes a syntax tree for the `UNION` operation.
 *
 * Example:
 *     union(["SELECT * FROM foo", "SELECT * FROM bla"]).sql();
 *     // 'SELECT * FROM foo UNION SELECT * FROM bla'
 *
 * @param expressions - The SQL code strings, corresponding to the `UNION`'s operands.
 *                      If `Expression` instances are passed, they will be used as-is.
 * @param options - Options object
 * @param options.distinct - Set the DISTINCT flag if and only if this is true. Default is `true`.
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy the expression. Default is `true`.
 * @returns The new Union instance
 */
export function union (
  expressions?: string | Expression | (string | Expression | undefined)[],
  options: {
    distinct?: boolean;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): UnionExpr {
  const expressionList = ensureList(expressions).filter((e): e is string | Expression => e !== undefined);
  if (expressionList.length < 2) {
    throw new Error('At least two expressions are required by `union`.');
  }
  return _applySetOperation(expressionList, UnionExpr, options);
}

/**
 * Initializes a syntax tree for the `INTERSECT` operation.
 *
 * Example:
 *     intersectExpr(["SELECT * FROM foo", "SELECT * FROM bla"]).sql();
 *     // 'SELECT * FROM foo INTERSECT SELECT * FROM bla'
 *
 * @param expressions - The SQL code strings, corresponding to the `INTERSECT`'s operands.
 *                      If `Expression` instances are passed, they will be used as-is.
 * @param options - Options object
 * @param options.distinct - Set the DISTINCT flag if and only if this is true. Default is `true`.
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy the expression. Default is `true`.
 * @returns The new Intersect instance
 */
export function intersect (
  expressions?: string | Expression | (string | Expression | undefined)[],
  options: {
    distinct?: boolean;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): IntersectExpr {
  const expressionList = ensureList(expressions).filter((e): e is string | Expression => e !== undefined);
  if (expressionList.length < 2) {
    throw new Error('At least two expressions are required by `intersect`.');
  }
  return _applySetOperation(expressionList, IntersectExpr, options);
}

/**
 * Initializes a syntax tree for the `EXCEPT` operation.
 *
 * Example:
 *     exceptExpr(["SELECT * FROM foo", "SELECT * FROM bla"]).sql();
 *     // 'SELECT * FROM foo EXCEPT SELECT * FROM bla'
 *
 * @param expressions - The SQL code strings, corresponding to the `EXCEPT`'s operands.
 *                      If `Expression` instances are passed, they will be used as-is.
 * @param options - Options object
 * @param options.distinct - Set the DISTINCT flag if and only if this is true. Default is `true`.
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy the expression. Default is `true`.
 * @returns The new Except instance
 */
export function except (
  expressions?: string | Expression | (string | Expression | undefined)[],
  options: {
    distinct?: boolean;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): ExceptExpr {
  const expressionList = ensureList(expressions).filter((e): e is string | Expression => e !== undefined);
  if (expressionList.length < 2) {
    throw new Error('At least two expressions are required by `except`.');
  }
  return _applySetOperation(expressionList, ExceptExpr, options);
}

/**
 * Builds an INSERT statement.
 *
 * Example:
 *     insert("VALUES (1, 2, 3)", "tbl").sql()
 *     // 'INSERT INTO tbl VALUES (1, 2, 3)'
 *
 * @param expression - The SQL string or expression of the INSERT statement
 * @param into - The table to insert data to
 * @param options - Options object
 * @param options.columns - Optionally the table's column names
 * @param options.overwrite - Whether to INSERT OVERWRITE or not
 * @param options.returning - SQL conditional parsed into a RETURNING statement
 * @param options.dialect - The dialect used to parse the input expressions
 * @param options.copy - Whether to copy the expression
 * @returns The syntax tree for the INSERT statement
 */
export function insert (
  expression: string | SelectExpr,
  into: string | TableExpr,
  options: {
    columns?: (string | IdentifierExpr)[];
    overwrite?: boolean;
    returning?: string | Expression;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): InsertExpr {
  const {
    columns, overwrite, returning, dialect, copy = true, ...opts
  } = options;

  const expr = maybeParse(expression, {
    dialect,
    copy,
    ...opts,
  });

  let thisExpr: TableExpr | SchemaExpr = maybeParse(into, {
    into: TableExpr,
    dialect,
    copy,
    ...opts,
  });

  if (columns) {
    thisExpr = new SchemaExpr({
      this: thisExpr,
      expressions: columns.map((c) =>
        typeof c === 'string' ? toIdentifier(c) : c),
    });
  }

  let insertExpr = new InsertExpr({
    this: thisExpr,
    expression: expr,
    overwrite,
  });

  if (returning) {
    insertExpr = insertExpr.returning(returning, {
      dialect,
      copy: false,
      ...opts,
    }) as InsertExpr;
  }

  return insertExpr;
}

/**
 * Builds a DELETE statement.
 *
 * Example:
 *     delete_("my_table", { where: "id > 1" }).sql()
 *     // 'DELETE FROM my_table WHERE id > 1'
 *
 * @param table - The table to delete from
 * @param options - Options object
 * @param options.where - SQL conditional parsed into a WHERE statement
 * @param options.returning - SQL conditional parsed into a RETURNING statement
 * @param options.dialect - The dialect used to parse the input expressions
 * @returns The syntax tree for the DELETE statement
 */
export function delete_ (
  table: string | Expression,
  options: {
    where?: string | Expression;
    returning?: string | Expression;
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): DeleteExpr {
  const {
    where, returning, dialect, ...opts
  } = options;

  let deleteExpr = new DeleteExpr({}).delete(table, {
    dialect,
    copy: false,
    ...opts,
  }) as DeleteExpr;

  if (where) {
    deleteExpr = deleteExpr.where(where, {
      dialect,
      copy: false,
      ...opts,
    }) as DeleteExpr;
  }

  if (returning) {
    deleteExpr = deleteExpr.returning(returning, {
      dialect,
      copy: false,
      ...opts,
    }) as DeleteExpr;
  }

  return deleteExpr;
}

/**
 * Creates an UPDATE statement.
 *
 * Example:
 *     update("my_table", { properties: { x: 1, y: "2" }, where: "id > 1" }).sql()
 *     // "UPDATE my_table SET x = 1, y = '2' WHERE id > 1"
 *
 * @param table - The table to update
 * @param options - Options object
 * @param options.properties - Dictionary of properties to SET
 * @param options.where - SQL conditional parsed into a WHERE statement
 * @param options.from - SQL statement parsed into a FROM statement
 * @param options.with - Dictionary of CTE aliases / select statements
 * @param options.dialect - The dialect used to parse the input expressions
 * @returns The syntax tree for the UPDATE statement
 */
export function update<T> (
  table: string | TableExpr,
  options: {
    properties?: Record<string, unknown>;
    where?: string | Expression;
    from?: string | Expression;
    with?: Record<string, string | Expression>;
    dialect?: DialectType;
  } & { [K in keyof T]: K extends 'dialect' ? unknown : ExpressionValue | ExpressionValueList },
): UpdateExpr;
export function update (
  table: string | TableExpr,
  options: {
    properties?: Record<string, unknown>;
    where?: string | Expression;
    from?: string | Expression;
    with?: Record<string, string | Expression>;
    dialect?: DialectType;
  } & { [key: string]: ExpressionValue | ExpressionValueList } = {},
): UpdateExpr {
  const {
    properties, where, from: fromExpr, with: withCtes, dialect, ...opts
  } = options;

  const updateExpr = new UpdateExpr({
    this: maybeParse(table, {
      into: TableExpr,
      dialect,
    }),
  });

  if (properties) {
    updateExpr.setArgKey('expressions', Object.entries(properties).map(([k, v]) =>
      new EQExpr({
        this: maybeParse(k, {
          dialect,
          ...opts,
        }),
        expression: convert(v),
      })));
  }

  if (fromExpr) {
    updateExpr.setArgKey('from', maybeParse(fromExpr, {
      into: FromExpr,
      dialect,
      prefix: 'FROM',
      ...opts,
    }));
  }

  if (where) {
    let whereExpr = where;
    if (where instanceof ConditionExpr) {
      whereExpr = new WhereExpr({ this: where });
    }
    updateExpr.setArgKey('where', maybeParse(whereExpr, {
      into: WhereExpr,
      dialect,
      prefix: 'WHERE',
      ...opts,
    }));
  }

  if (withCtes) {
    const cteList = Object.entries(withCtes).map(([aliasName, qry]) =>
      alias(new CTEExpr({
        this: maybeParse(qry, {
          dialect,
          ...opts,
        }),
        aliases: [],
      }), aliasName, { table: true })) as CTEExpr[];

    updateExpr.setArgKey('with', new WithExpr({ expressions: cteList }));
  }

  return updateExpr;
}

/**
 * Builds a MERGE statement.
 *
 * Example:
 *     merge(["WHEN MATCHED THEN UPDATE..."], {
 *       into: "my_table",
 *       using: "source_table",
 *       on: "my_table.id = source_table.id"
 *     }).sql()
 *
 * @param whenExprs - The WHEN clauses specifying actions for matched and unmatched rows
 * @param options - Options object
 * @param options.into - The target table to merge data into
 * @param options.using - The source table to merge data from
 * @param options.on - The join condition for the merge
 * @param options.returning - The columns to return from the merge
 * @param options.dialect - The dialect used to parse the input expressions
 * @param options.copy - Whether to copy the expression
 * @returns The syntax tree for the MERGE statement
 */
export function merge (
  whenExprs: (string | Expression)[],
  options: {
    into: string | Expression;
    using: string | Expression;
    on: string | Expression;
    returning?: string | Expression;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  },
): MergeExpr {
  const {
    into, using: usingExpr, on, returning, dialect, copy = true, ...opts
  } = options;

  const expressions: Expression[] = [];
  for (const whenExpr of whenExprs) {
    const expr = maybeParse(whenExpr, {
      dialect,
      copy,
      into: WhensExpr,
      ...opts,
    });
    if (expr instanceof WhenExpr) {
      expressions.push(expr);
    } else if ('expressions' in expr.args) {
      expressions.push(...(expr.args.expressions as Expression[]));
    }
  }

  let mergeExpr = new MergeExpr({
    this: maybeParse(into, {
      dialect,
      copy,
      ...opts,
    }),
    using: maybeParse(usingExpr, {
      dialect,
      copy,
      ...opts,
    }),
    on: maybeParse(on, {
      dialect,
      copy,
      ...opts,
    }),
    whens: new WhensExpr({ expressions }),
  });

  if (returning) {
    mergeExpr = mergeExpr.returning(returning, {
      dialect,
      copy: false,
      ...opts,
    }) as MergeExpr;
  }

  const usingClause = mergeExpr.args.using;
  if (usingClause instanceof AliasExpr) {
    usingClause.replace(alias(usingClause.$this!, usingClause.args.alias as string, { table: true }));
  }

  return mergeExpr;
}

/**
 * Initialize a logical condition expression.
 *
 * Example:
 *     condition("x=1").sql()
 *     // 'x = 1'
 *
 * This is helpful for composing larger logical syntax trees:
 *     const where = condition("x=1")
 *     where = where.and("y=1")
 *     Select().from("tbl").select("*").where(where).sql()
 *     // 'SELECT * FROM tbl WHERE x = 1 AND y = 1'
 *
 * @param expression - The SQL code string to parse. If an Expression instance is passed, this is used as-is.
 * @param options - Options object
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy the expression
 * @returns The new Condition instance
 */
export function condition (
  expression: string | Expression,
  options: {
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): ConditionExpr {
  return maybeParse(expression, {
    into: ConditionExpr,
    ...options,
  }) as ConditionExpr;
}

/**
 * Parse SQL text into an expression if needed
 * @param sqlOrExpression - SQL text or expression
 * @param options - Parsing options
 * @returns Expression
 */
export function maybeParse<RetT extends Expression> (
  sqlOrExpression: string | number | boolean | RetT | undefined,
  options?: ParseOptions<RetT> & {
    prefix?: string;
    copy?: boolean;
  },
): RetT {
  // If it's already an Expression
  if (sqlOrExpression instanceof Expression) {
    if (options?.copy) {
      return sqlOrExpression.copy();
    }
    return sqlOrExpression;
  }

  // SQL cannot be None/null
  if (sqlOrExpression === undefined) {
    throw new ParseError('SQL cannot be null or undefined');
  }

  // Convert to string and optionally add prefix
  let _sql = String(sqlOrExpression);
  if (options?.prefix) {
    _sql = `${options.prefix} ${_sql}`;
  }

  // Extract prefix and copy from options, pass the rest to parseOne
  const {
    dialect, ...parseOptions
  } = options || {};

  // Parse the SQL string
  return parseOne<RetT>(_sql, {
    ...parseOptions,
    read: dialect || parseOptions.read,
  });
}

/**
 * Convert SQL text to a column expression
 * @param sql - SQL text
 * @param dialect - SQL dialect
 * @returns Column expression
 */
/**
 * Create a table expression from a `[catalog].[schema].[table]` sql path.
 * Catalog and schema are optional. If a table is passed in then that table is returned.
 *
 * Example:
 *     to_table("catalog.schema.table").sql()
 *     // 'catalog.schema.table'
 *
 * @param sqlPath - A `[catalog].[schema].[table]` string or TableExpr instance
 * @param options - Options object
 * @param options.dialect - The source dialect according to which the table name will be parsed
 * @param options.copy - Whether to copy a table if it is passed in
 * @returns A table expression
 */
export function toTable<T> (
  sqlPath: string | TableExpr,
  options?: {
    dialect?: DialectType;
    copy?: boolean;
  } & { [K in keyof T]: K extends 'dialect' ? unknown : ExpressionValue | ExpressionValueList },
): TableExpr;
export function toTable (
  sqlPath: string | TableExpr,
  options: {
    dialect?: DialectType;
    copy?: boolean;
  } & { [key: string]: ExpressionValue | ExpressionValueList } = {},
): TableExpr {
  const {
    dialect, copy = true, ...opts
  } = options;

  if (sqlPath instanceof TableExpr) {
    return maybeCopy(sqlPath, copy) as TableExpr;
  }

  try {
    const parsed = maybeParse(sqlPath, {
      into: TableExpr,
      dialect,
      ...opts,
    });
    for (const [k, v] of Object.entries(opts)) {
      parsed.setArgKey(k, v);
    }
    return parsed as TableExpr;
  } catch {
    const [
      catalog,
      db,
      name,
    ] = splitNumWords(sqlPath, '.', 3);

    if (!name) {
      throw new Error(`Invalid table path: ${sqlPath}`);
    }

    const tableExpr = table(name, db, catalog);
    for (const [k, v] of Object.entries(opts)) {
      tableExpr.setArgKey(k, v);
    }
    return tableExpr;
  }
}

/**
 * Create a column from a `[table].[column]` sql path. Table is optional.
 * If a column is passed in then that column is returned.
 *
 * Example:
 *     to_column("table.column").sql()
 *     // 'table.column'
 *
 * @param sqlPath - A `[table].[column]` string or ColumnExpr instance
 * @param options - Options object
 * @param options.quoted - Whether or not to force quote identifiers
 * @param options.dialect - The source dialect according to which the column name will be parsed
 * @param options.copy - Whether to copy a column if it is passed in
 * @returns A column expression
 */
export function toColumn<T> (
  sqlPath: string | ColumnExpr,
  options?: {
    quoted?: boolean;
    dialect?: DialectType;
    copy?: boolean;
  } & { [K in keyof T]: K extends 'dialect' ? unknown : ExpressionValue | ExpressionValueList },
): ColumnExpr;
export function toColumn (
  sqlPath: string | ColumnExpr,
  options: {
    quoted?: boolean;
    dialect?: DialectType;
    copy?: boolean;
  } & { [key: string]: ExpressionValue | ExpressionValueList } = {},
): ColumnExpr {
  const {
    quoted, dialect, copy = true, ...opts
  } = options;

  if (sqlPath instanceof ColumnExpr) {
    return maybeCopy(sqlPath, copy) as ColumnExpr;
  }

  try {
    const col = maybeParse(sqlPath, {
      into: ColumnExpr,
      dialect,
      ...opts,
    }) as ColumnExpr;
    for (const [k, v] of Object.entries(opts)) {
      col.setArgKey(k, v);
    }

    if (quoted) {
      for (const identifier of col.findAll(IdentifierExpr)) {
        identifier.setArgKey('quoted', true);
      }
    }

    return col;
  } catch {
    const parts = sqlPath.split('.').reverse();
    const [name, tableName] = parts;
    const args: ColumnExprArgs = {
      this: toIdentifier(name, { quoted }) as IdentifierExpr,
    };
    if (tableName) {
      args.table = toIdentifier(tableName, { quoted }) as IdentifierExpr;
    }
    const col = new ColumnExpr(args);
    for (const [k, v] of Object.entries(opts)) {
      col.setArgKey(k, v);
    }
    return col;
  }
}

/**
 * Find all table references in an expression tree.
 * Walks the entire tree and collects all TableExpr instances.
 *
 * @param expr - Expression to search
 * @returns Array of table expressions found in the tree
 *
 * @example
 * // Find all tables in a query
 * const tables = findTables(select('*'));
 * // Returns [TableExpr('users'), TableExpr('orders'), ...]
 */
/**
 * Find all tables referenced in a query.
 *
 * @param expression - The query to find the tables in
 * @returns A set of all the tables
 */
export function findTables (expression: Expression): Set<TableExpr> {
  const tables = new Set<TableExpr>();
  for (const scope of traverseScope(expression)) {
    for (const table of scope.tables) {
      if (table.name && !scope.cteSources.has(table.name)) {
        tables.add(table);
      }
    }
  }
  return tables;
}

/**
 * Maybe copy an expression if copy is true
 * @param instance - Expression instance to potentially copy
 * @param copy - Whether to copy the instance
 * @returns The instance or a copy of it
 */
export function maybeCopy<E extends Expression | undefined> (instance: E, copy = true): E {
  if (copy && instance) {
    return instance.copy() as E;
  }
  return instance;
}

/**
 * Generate a textual representation of an Expression tree
 * @param node - The node to convert to string
 * @param verbose - Include additional metadata like _id, _comments
 * @param level - Current indentation level
 * @param reprStr - Whether to use repr for strings
 * @returns String representation of the expression tree
 */
function _toS (node: unknown, verbose = false, level = 0, reprStr = false): string {
  let indent = '\n' + ('  '.repeat(level + 1));
  let delim = `,${indent}`;

  if (node instanceof Expression) {
    const args: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(node.args)) {
      if ((v !== undefined && (!Array.isArray(v) || 0 < v.length)) || verbose) {
        args[k] = v;
      }
    }

    if ((node.type || verbose) && !(node instanceof DataTypeExpr)) {
      args._type = node.type;
    }
    if (node.comments || verbose) {
      args._comments = node.comments;
    }

    if (verbose) {
      args._id = node.hash; // Use _hash as a proxy for id
    }

    // Inline leaves for a more compact representation
    if (node.isLeaf) {
      indent = '';
      delim = ', ';
    }

    const isReprStr = node.isString || (node instanceof IdentifierExpr && node.$quoted);
    const items = Object.entries(args)
      .map(([k, v]) => `${k}=${_toS(v, verbose, level + 1, isReprStr)}`)
      .join(delim);

    return `${node.constructor.name}(${indent}${items})`;
  }

  if (Array.isArray(node)) {
    const items = node.map((i) => _toS(i, verbose, level + 1)).join(delim);
    return `[${indent}${items}]`;
  }

  // We use the representation of the string to avoid stripping out important whitespace
  if (reprStr && typeof node === 'string') {
    node = JSON.stringify(node);
  }

  // Indent multiline strings to match the current level
  const str = String(node).replace(/^\n+|\n+$/g, '');
  return str.split('\n').join(indent);
}

/**
 * Check if an expression is the wrong type
 * @param expression - The expression to check
 * @param into - The expected expression class
 * @returns True if the expression is wrong type
 */
function _isWrongExpression (expression: unknown, into: typeof Expression): expression is Expression {
  return expression instanceof Expression && !(expression instanceof into);
}

/**
 * Apply a builder function that sets a single argument on an instance
 * @param options - Options object
 * @returns The modified instance
 */
function _applyBuilder<RetT extends Expression, ArgT extends Expression> (expression: undefined | ArgT | string | number, options: {
  instance: RetT;
  arg: string;
  copy?: boolean;
  prefix?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  into?: new (args: any) => ArgT;
  dialect?: DialectType;
  intoArg?: string;
  [key: string]: unknown;
}): RetT {
  const {
    instance,
    arg,
    copy = true,
    prefix,
    into,
    dialect,
    intoArg = 'this',
    ...opts
  } = options;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  if (into && _isWrongExpression(expression, into as any)) {
    expression = new into({ [intoArg]: expression });
  }

  const inst = maybeCopy(instance, copy)!;
  expression = maybeParse(expression, {
    prefix,
    into,
    dialect,
    ...opts,
  });

  inst.setArgKey(arg, expression);
  return inst;
}

/**
 * Apply a builder function that sets a list of child expressions
 * @param options - Options object
 * @returns The modified instance
 */
function _applyChildListBuilder<ArgT extends Expression, IntoT extends Expression, RetT extends Expression> (
  expressions: string | ArgT | undefined | (string | ArgT | undefined)[],
  options: {
    instance: RetT;
    arg: string;
    append?: boolean;
    copy?: boolean;
    prefix?: string;
    into?: new (args: { expressions: ArgT[] }) => IntoT;
    dialect?: DialectType;
    properties?: Record<string, ExpressionValue | ExpressionValueList>;
    [key: string]: unknown;
  },
): RetT {
  const {
    instance,
    arg,
    append = true,
    copy = true,
    prefix,
    into,
    dialect,
    properties: initialProperties,
    ...opts
  } = options;

  const inst = maybeCopy(instance, copy)!;
  const parsed: Expression[] = [];
  const properties: Record<string, unknown> = initialProperties || {};

  const expressionList = ensureList(expressions);
  for (const expression of expressionList) {
    if (expression === undefined) {
      continue;
    }

    let expr: string | IntoT | ArgT = expression;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    if (into && _isWrongExpression(expr, into as any)) {
      expr = new into({ expressions: [expr] });
    }

    const parsedExpr = maybeParse(expr, {
      into,
      dialect,
      prefix,
      ...opts,
    });

    for (const [k, v] of Object.entries(parsedExpr.args)) {
      if (k === 'expressions') {
        parsed.push(...(v as Expression[]));
      } else {
        properties[k] = v;
      }
    }
  }

  const existing = (inst.args as Record<string, ExpressionValue | ExpressionValueList>)[arg] as Expression | undefined;
  let allExpressions = parsed;
  if (append && existing && existing.args.expressions) {
    allExpressions = [...(existing.args.expressions as Expression[]), ...parsed];
  }

  const child = into
    ? new into({ expressions: allExpressions as ArgT[] })
    : new Expression({ expressions: allExpressions });
  for (const [k, v] of Object.entries(properties)) {
    child.setArgKey(k, v as ExpressionValue | ExpressionValueList);
  }
  inst.setArgKey(arg, child);

  return inst;
}

/**
 * Apply a builder function that sets a flat list of expressions
 * @param expressions - Array of expressions to add
 * @param options - Options object
 * @returns The modified instance
 */
function _applyListBuilder<ArgT extends Expression, RetT extends Expression> (
  expressions: string | ArgT | undefined | (string | ArgT | undefined)[],
  options: {
    instance: RetT;
    arg: string;
    append?: boolean;
    copy?: boolean;
    prefix?: string;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    into?: new (args: any) => ArgT;
    dialect?: DialectType;
    [key: string]: unknown;
  },
): RetT {
  const {
    instance,
    arg,
    append = true,
    copy = true,
    prefix,
    into,
    dialect,
    ...opts
  } = options;

  const inst = maybeCopy(instance, copy)!;

  const expressionList = ensureList(expressions);
  const parsedExpressions = expressionList
    .filter((expr) => expr !== undefined)
    .map((expr) =>
      maybeParse(expr, {
        into,
        prefix,
        dialect,
        ...opts,
      }));

  const existing = (inst.args as Record<string, ExpressionValue | ExpressionValueList>)[arg] as Expression[] | undefined;
  if (append && existing) {
    inst.setArgKey(arg, [...existing, ...parsedExpressions]);
  } else {
    inst.setArgKey(arg, parsedExpressions);
  }

  return inst;
}

/**
 * Apply a conjunction builder (combines expressions with AND)
 * @param expressions - Array of expressions to combine with AND
 * @param options - Options object
 * @returns The modified instance
 */
function _applyConjunctionBuilder<E extends Expression> (
  expressions: string | Expression | undefined | (string | Expression | undefined)[],
  options: {
    instance: E;
    arg: string;
    into?: typeof Expression;
    append?: boolean;
    copy?: boolean;
    dialect?: DialectType;
    [key: string]: unknown;
  },
): E {
  const {
    instance,
    arg,
    into,
    append = true,
    copy = true,
    dialect,
    ...opts
  } = options;

  // Filter out undefined and empty strings
  const expressionList = ensureList(expressions);
  const filteredExpressions = expressionList.filter(
    (expr) => expr !== undefined && expr !== '',
  );

  if (filteredExpressions.length === 0) {
    return instance;
  }

  const inst = maybeCopy(instance, copy)!;

  const existing = (inst.args as Record<string, ExpressionValue | ExpressionValueList>)[arg] as Expression | undefined;
  let allExpressions = [...filteredExpressions];

  if (append && existing !== undefined) {
    const existingExpr = into && 'this' in existing.args ? existing.args.this as Expression : existing;
    allExpressions = [existingExpr, ...filteredExpressions];
  }

  // Create AND conjunction of all expressions
  let combined: Expression | undefined;
  if (0 < allExpressions.length) {
    combined = allExpressions
      .map((expr) => maybeParse(expr, {
        dialect,
        copy,
        ...opts,
      }))
      .reduce((left, right) =>
        new AndExpr({
          this: left,
          expression: right,
        }));
  }

  const node = into && combined ? new into({ this: combined }) : combined;

  if (node) {
    inst.setArgKey(arg, node);
  }

  return inst;
}

/**
 * Apply a CTE builder
 * @param options - Options object
 * @returns The modified instance
 */
function _applyCteBuilder<E extends Expression> (options: {
  instance: E;
  alias: string | IdentifierExpr | TableAliasExpr;
  as: string | QueryExpr;
  recursive?: boolean;
  materialized?: boolean;
  append?: boolean;
  dialect?: DialectType;
  copy?: boolean;
  scalar?: boolean;
  [key: string]: unknown;
}): E {
  const {
    instance,
    alias,
    as: asExpr,
    recursive,
    materialized,
    append = true,
    dialect,
    copy = true,
    scalar,
    ...opts
  } = options;

  const aliasExpression = maybeParse(alias, {
    dialect,
    into: TableAliasExpr,
    ...opts,
  });

  let asExpression = maybeParse(asExpr, {
    dialect,
    copy,
    ...opts,
  });

  // Scalar CTE must be wrapped in a subquery
  if (scalar && !(asExpression instanceof SubqueryExpr)) {
    asExpression = new SubqueryExpr({ this: asExpression });
  }

  const cte = new CTEExpr({
    this: asExpression,
    aliases: [aliasExpression],
    materialized,
    scalar,
  });

  return _applyChildListBuilder([cte], {
    instance,
    arg: 'with',
    append,
    copy,
    into: WithExpr,
    properties: recursive ? { recursive } : {},
  });
}

/**
 * Combine expressions with a connector operator
 * @param expressions - Expressions to combine
 * @param operator - The connector operator class (AndExpr, OrExpr, etc.)
 * @param options - Options object
 * @returns Combined expression
 */
function _combine<T extends ConnectorExpr> (
  expressions: string | Expression | undefined | (string | Expression | undefined)[],
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  operator: new (args: any) => T,
  options: {
    dialect?: DialectType;
    copy?: boolean;
    wrap?: boolean;
    [key: string]: unknown;
  } = {},
): Expression {
  const {
    dialect, copy = true, wrap = true, ...opts
  } = options;

  const expressionList = ensureList(expressions);
  const conditions = expressionList
    .filter((expr) => expr !== undefined && expr !== '')
    .map((expr) => maybeParse(expr, {
      dialect,
      copy,
      ...opts,
    }));

  const [first, ...rest] = conditions;

  let result = first;
  if (0 < rest.length && wrap) {
    result = _wrap(result, ConnectorExpr) || result;
  }

  for (const expr of rest) {
    result = new operator({
      this: result,
      expression: wrap ? _wrap(expr, ConnectorExpr) || expr : expr,
    });
  }

  return result;
}

/**
 * Wrap an expression in parentheses if it's of a certain type
 * @param expression - Expression to potentially wrap
 * @param kind - The expression class to check against
 * @returns The expression wrapped in parentheses if it matches the kind, otherwise the original
 * expression
 */
function _wrap (expression: Expression | undefined, kind: typeof Expression): Expression | ParenExpr | undefined {
  if (expression instanceof kind) {
    return new ParenExpr({ this: expression });
  }
  return expression;
}

/**
 * Returns a Null expression
 * @returns A Null expression
 */
export function null_ (): NullExpr {
  return new NullExpr({});
}

/**
 * Returns a true Boolean expression
 * @returns A true Boolean expression
 */
export function true_ (): BooleanExpr {
  return new BooleanExpr({ this: true });
}

/**
 * Returns a false Boolean expression
 * @returns A false Boolean expression
 */
export function false_ (): BooleanExpr {
  return new BooleanExpr({ this: false });
}

/**
 * Convert a JavaScript value into an expression object.
 * Raises an error if a conversion is not possible.
 *
 * @param value - A JavaScript value
 * @param copy - Whether to copy `value` (only applies to Expressions and collections)
 * @returns The equivalent expression object
 */
export function convert (value: unknown, copy = false): Expression {
  // Handle Expression instances
  if (value instanceof Expression) {
    const result = maybeCopy(value, copy);
    if (result) {
      return result;
    }
  }

  // Handle strings
  if (typeof value === 'string') {
    return LiteralExpr.string(value);
  }

  // Handle booleans
  if (typeof value === 'boolean') {
    return new BooleanExpr({ this: value });
  }

  // Handle null, undefined, or NaN
  if (value === undefined || (typeof value === 'number' && isNaN(value))) {
    return null_();
  }

  // Handle numbers
  if (typeof value === 'number') {
    return LiteralExpr.number(value);
  }

  // Handle Luxon DateTime objects (datetime.datetime in Python)
  // Luxon provides proper timezone support similar to Python's datetime with tzinfo
  if (DateTime.isDateTime(value)) {
    // Format datetime similar to Python's isoformat(sep=" ")
    // Python: "2024-01-15 10:30:45" (no milliseconds)
    const datetimeStr = value.toFormat('yyyy-MM-dd HH:mm:ss');
    const datetimeLiteral = LiteralExpr.string(datetimeStr);

    // Extract timezone similar to Python's str(value.tzinfo)
    // This returns IANA timezone names like "America/Los_Angeles"
    let tz: LiteralExpr | undefined;
    if (value.zoneName && value.zoneName !== 'UTC') {
      tz = LiteralExpr.string(value.zoneName);
    }

    return new TimeStrToTimeExpr({
      this: datetimeLiteral,
      zone: tz,
    });
  }

  // Handle native JavaScript Date objects (fallback)
  if (value instanceof Date) {
    // Convert to Luxon DateTime for consistent handling
    const dt = DateTime.fromJSDate(value);
    const datetimeStr = dt.toFormat('yyyy-MM-dd HH:mm:ss');
    const datetimeLiteral = LiteralExpr.string(datetimeStr);

    return new TimeStrToTimeExpr({
      this: datetimeLiteral,
      zone: undefined, // Native Date doesn't have timezone info
    });
  }

  // Handle arrays
  if (Array.isArray(value)) {
    return new ArrayExpr({ expressions: value.map((v) => convert(v, copy)) });
  }

  // Handle objects as structs
  if (value !== null && typeof value === 'object') {
    const entries = Object.entries(value);
    return new StructExpr({
      expressions: entries.map(([k, v]) =>
        new PropertyEQExpr({
          this: toIdentifier(k),
          expression: convert(v, copy),
        })),
    });
  }

  throw new Error(`Cannot convert ${value}`);
}

/**
 * Build a SQL variable.
 *
 * Example:
 *     var_('x').sql()
 *     // 'x'
 *
 * @param name - The name of the var or an expression whose name will become the var
 * @returns The new variable node
 */
export function var_ (name: string | Expression | undefined): VarExpr {
  if (!name) {
    throw new Error('Cannot convert empty name into var.');
  }

  if (name instanceof Expression) {
    name = name.name;
  }

  return new VarExpr({ this: name });
}

/**
 * Returns an array.
 *
 * Example:
 *     array([1, 'x']).sql()
 *     // 'ARRAY(1, x)'
 *
 * @param expressions - The expressions to add to the array
 * @param options - Options object
 * @param options.copy - Whether to copy the argument expressions
 * @param options.dialect - The source dialect
 * @returns An array expression
 */
export function array (
  expressions?: string | Expression | (string | Expression | undefined)[],
  options: {
    copy?: boolean;
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): ArrayExpr {
  const {
    copy = true, dialect, ...opts
  } = options;

  const expressionList = ensureList(expressions);
  return new ArrayExpr({
    expressions: expressionList.map((expr) => maybeParse(expr, {
      copy,
      dialect,
      ...opts,
    })),
  });
}

/**
 * Returns a tuple.
 *
 * Example:
 *     tuple_([1, 'x']).sql()
 *     // '(1, x)'
 *
 * @param expressions - The expressions to add to the tuple
 * @param options - Options object
 * @param options.copy - Whether to copy the argument expressions
 * @param options.dialect - The source dialect
 * @returns A tuple expression
 */
export function tuple (
  expressions?: string | Expression | (string | Expression | undefined)[],
  options: {
    copy?: boolean;
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): TupleExpr {
  const {
    copy = true, dialect, ...opts
  } = options;

  const expressionList = ensureList(expressions);
  return new TupleExpr({
    expressions: expressionList.map((expr) => maybeParse(expr, {
      copy,
      dialect,
      ...opts,
    })),
  });
}

/**
 * Build ALTER TABLE... RENAME... expression
 *
 * Example:
 *     renameTable('old_table', 'new_table').sql()
 *     // 'ALTER TABLE old_table RENAME TO new_table'
 *
 * @param oldName - The old name of the table
 * @param newName - The new name of the table
 * @param options - Options object
 * @param options.dialect - The dialect to parse the table
 * @returns Alter table expression
 */
export function renameTable (
  oldName: string | TableExpr,
  newName: string | TableExpr,
  options: {
    dialect?: DialectType;
  } = {},
): AlterExpr {
  const { dialect } = options;

  const oldTable = toTable(oldName, { dialect });
  const newTable = toTable(newName, { dialect });

  return new AlterExpr({
    this: oldTable,
    kind: AlterExprKind.TABLE,
    actions: [new AlterRenameExpr({ this: newTable })],
  });
}

/**
 * Build ALTER TABLE... RENAME COLUMN... expression
 *
 * Example:
 *     renameColumn('my_table', 'old_col', 'new_col').sql()
 *     // 'ALTER TABLE my_table RENAME COLUMN old_col TO new_col'
 *
 * @param tableName - Name of the table
 * @param oldColumnName - The old name of the column
 * @param newColumnName - The new name of the column
 * @param options - Options object
 * @param options.exists - Whether to add the IF EXISTS clause
 * @param options.dialect - The dialect to parse the table/column
 * @returns Alter table expression
 */
export function renameColumn (
  tableName: string | TableExpr,
  oldColumnName: string | ColumnExpr,
  newColumnName: string | ColumnExpr,
  options: {
    exists?: boolean;
    dialect?: DialectType;
  } = {},
): AlterExpr {
  const {
    exists, dialect,
  } = options;

  const tableExpr = toTable(tableName, { dialect });
  const oldColumn = toColumn(oldColumnName, { dialect });
  const newColumn = toColumn(newColumnName, { dialect });

  return new AlterExpr({
    this: tableExpr,
    kind: AlterExprKind.TABLE,
    actions: [
      new RenameColumnExpr({
        this: oldColumn,
        to: newColumn,
        exists,
      }),
    ],
  });
}

/**
 * Return all table names referenced through columns in an expression.
 *
 * Example:
 *     columnTableNames(parse('a.b AND c.d AND c.e'))
 *     // Set(['a', 'c'])
 *
 * @param expression - Expression to find table names
 * @param exclude - A table name to exclude
 * @returns A set of unique table names
 */
export function columnTableNames (
  expression: Expression,
  exclude = '',
): Set<string> {
  const tableNames = new Set<string>();

  for (const col of expression.findAll(ColumnExpr)) {
    const tableName = col.table;
    if (tableName && tableName !== exclude) {
      tableNames.add(tableName);
    }
  }

  return tableNames;
}

/**
 * Get the full name of a table as a string.
 *
 * Example:
 *     tableName(parse('select * from a.b.c').find(TableExpr))
 *     // 'a.b.c'
 *
 * @param tableExpr - Table expression node or string
 * @param options - Options object
 * @param options.dialect - The dialect to generate the table name for
 * @param options.identify - Whether to always quote identifiers
 * @returns The table name
 */
export function tableName (
  tableExpr: TableExpr | string,
  options: {
    dialect?: DialectType;
    identify?: boolean;
  } = {},
): string {
  const {
    dialect, identify = false,
  } = options;

  const table = maybeParse(tableExpr, {
    into: TableExpr,
    dialect,
  });

  if (!table) {
    throw new Error(`Cannot parse ${tableExpr}`);
  }

  return (table as TableExpr).parts
    .map((part) => {
      if (identify || !SAFE_IDENTIFIER_RE.test(part.name)) {
        return part.sql({
          dialect,
          identify: true,
          copy: false,
          comments: false,
        });
      }
      return part.name;
    })
    .join('.');
}

/**
 * Replace children of an expression with the result of a function.
 *
 * @param expression - The expression whose children to replace
 * @param fun - Function to apply to each child node
 * @param args - Additional arguments to pass to the function
 * @param kwargs - Additional keyword arguments to pass to the function
 */
export function replaceChildren (
  expression: Expression,
  fun: (child: Expression, ...args: unknown[]) => Expression | Expression[],
  ...args: unknown[]
): void {
  for (const [key, value] of Object.entries(expression.args)) {
    const isListArg = Array.isArray(value);
    const childNodes = isListArg ? value : [value];
    const newChildNodes: Expression[] = [];

    for (const childNode of childNodes) {
      if (childNode instanceof Expression) {
        const result = fun(childNode, ...args);
        const resultArray = Array.isArray(result) ? result : [result];
        newChildNodes.push(...resultArray);
      } else {
        newChildNodes.push(childNode);
      }
    }

    if (isListArg) {
      expression.setArgKey(key, newChildNodes);
    } else {
      expression.setArgKey(key, newChildNodes[0]);
    }
  }
}

/**
 * Replace an entire tree with the result of function calls on each node.
 *
 * This will be traversed in reverse DFS, so leaves first.
 * If new nodes are created as a result of function calls, they will also be traversed.
 *
 * @param expression - The root expression
 * @param fun - Function to apply to each node
 * @param prune - Optional function to determine if a subtree should be pruned
 * @returns The transformed expression
 */
export function replaceTree (
  expression: Expression,
  fun: (node: Expression) => Expression,
  prune?: (node: Expression) => boolean,
): Expression {
  const stack = Array.from(expression.dfs({ prune }));
  let newNode = expression;

  while (0 < stack.length) {
    const node = stack.pop()!;
    newNode = fun(node);

    if (newNode !== node) {
      node.replace(newNode);

      if (newNode instanceof Expression) {
        stack.push(newNode);
      }
    }
  }

  return newNode;
}

/**
 * Returns a case normalized table name without quotes.
 *
 * Example:
 *     normalizeTableName('`A-B`.c', { dialect: 'bigquery' })
 *     // 'A-B.c'
 *
 * @param tableExpr - The table to normalize
 * @param options - Options object
 * @param options.dialect - The dialect to use for normalization rules
 * @param options.copy - Whether to copy the expression
 * @returns Normalized table name
 */
export function normalizeTableName (
  tableExpr: string | TableExpr,
  options: {
    dialect?: DialectType;
    copy?: boolean;
  } = {},
): string {
  const {
    dialect, copy = true,
  } = options;

  // TODO: Import normalizeIdentifiers from optimizer when available
  // For now, use simple normalization
  const table = toTable(tableExpr, {
    dialect,
    copy,
  });

  return table.parts.map((p) => p.name).join('.');
}

/**
 * Replace all tables in expression according to the mapping.
 *
 * Example:
 *     replaceTables(parse('select * from a.b'), { 'a.b': 'c' }).sql()
 *     // 'SELECT * FROM c'
 *
 * @param expression - Expression node to be transformed and replaced
 * @param mapping - Mapping of table names
 * @param options - Options object
 * @param options.dialect - The dialect of the mapping table
 * @param options.copy - Whether to copy the expression
 * @returns The mapped expression
 */
export function replaceTables<T extends Expression> (
  expression: T,
  mapping: Record<string, string>,
  options: {
    dialect?: DialectType;
    copy?: boolean;
  } = {},
): T {
  const {
    dialect, copy = true,
  } = options;

  const normalizedMapping: Record<string, string> = {};
  for (const [key, value] of Object.entries(mapping)) {
    normalizedMapping[normalizeTableName(key, { dialect })] = value;
  }

  function replaceTablesTransform (node: Expression): Expression {
    if (node instanceof TableExpr && node.meta['replace'] !== false) {
      const original = normalizeTableName(node, { dialect });
      const newName = normalizedMapping[original];

      if (newName) {
        const newTable = toTable(newName, { dialect });
        // Copy over other args except table parts
        for (const [key, value] of Object.entries(node.args)) {
          if (![
            'this',
            'db',
            'catalog',
          ].includes(key)) {
            newTable.setArgKey(key, value);
          }
        }
        newTable.addComments([original]);
        return newTable;
      }
    }
    return node;
  }

  return expression.transform(replaceTablesTransform, { copy }) as T;
}

/**
 * Replace placeholders in an expression.
 *
 * Example:
 *     replacePlaceholders(
 *       parse('select * from :tbl where ? = ?'),
 *       toIdentifier('str_col'), 'b', { tbl: toIdentifier('foo') }
 *     ).sql()
 *     // "SELECT * FROM foo WHERE str_col = 'b'"
 *
 * @param expression - Expression node to be transformed and replaced
 * @param args - Positional values that will substitute unnamed placeholders in order
 * @returns The mapped expression
 */
export function replacePlaceholders (
  expression: Expression,
  ...args: unknown[]
): Expression {
  // Separate positional args from the last arg if it's an object (kwargs)
  let positionalArgs: unknown[];
  let kwargs: Record<string, unknown> = {};

  if (0 < args.length && typeof args[args.length - 1] === 'object' && args[args.length - 1] != undefined && !Array.isArray(args[args.length - 1]) && !(args[args.length - 1] instanceof Expression)) {
    kwargs = args[args.length - 1] as Record<string, unknown>;
    positionalArgs = args.slice(0, -1);
  } else {
    positionalArgs = args;
  }

  let argIndex = 0;

  function replacePlaceholder (node: Expression): Expression {
    if (node instanceof PlaceholderExpr) {
      if (typeof node.args.this === 'string') {
        const newName = kwargs[node.args.this];
        if (newName !== undefined) {
          return convert(newName);
        }
      } else {
        if (argIndex < positionalArgs.length) {
          return convert(positionalArgs[argIndex++]);
        }
      }
    }
    return node;
  }

  return expression.transform(replacePlaceholder);
}

/**
 * Transforms an expression by expanding all referenced sources into subqueries.
 *
 * Example:
 *     expand(parse('select * from x AS z'), { x: parse('select * from y') }).sql()
 *     // 'SELECT * FROM (SELECT * FROM y) AS z'
 *
 * @param expression - The expression to expand
 * @param sources - A dict of name to query or a callable that provides a query on demand
 * @param options - Options object
 * @param options.dialect - The dialect of the sources dict or the callable
 * @param options.copy - Whether to copy the expression during transformation
 * @returns The transformed expression
 */
export function expand (
  expression: Expression,
  sources: Record<string, QueryExpr | (() => QueryExpr)>,
  options: {
    dialect?: DialectType;
    copy?: boolean;
  } = {},
): Expression {
  const {
    dialect, copy = true,
  } = options;

  const normalizedSources: Record<string, QueryExpr | (() => QueryExpr)> = {};
  for (const [key, value] of Object.entries(sources)) {
    normalizedSources[normalizeTableName(key, { dialect })] = value;
  }

  function expandTransform (node: Expression): Expression {
    if (node instanceof TableExpr) {
      const name = normalizeTableName(node, { dialect });
      const source = normalizedSources[name];

      if (source) {
        const parsedSource = typeof source === 'function' ? source() : source;
        const aliasName = node.args.alias || name;
        const subqueryExpr = parsedSource.subquery(aliasName);
        subqueryExpr.comments = [`source: ${name}`];

        return subqueryExpr.transform(expandTransform, { copy: false });
      }
    }
    return node;
  }

  return expression.transform(expandTransform, { copy });
}

/** Query expression types that don't need to be wrapped in parentheses */
export const UNWRAPPED_QUERIES = [SelectExpr, SetOperationExpr] as const;

/** Percentile function classes */
export const PERCENTILES = [PercentileContExpr, PercentileDiscExpr] as const;

/** Non-null constant expression types */
export const NONNULL_CONSTANTS = [LiteralExpr, BooleanExpr] as const;

/** All constant expression types (including NULL) */
export const CONSTANTS = [
  LiteralExpr,
  BooleanExpr,
  NullExpr,
] as const;

/** Map of SQL function names to their expression classes (auto-populated by self-registration) */
export const FUNCTION_BY_NAME: ReadonlyMap<string, typeof FuncExpr> = _functionRegistry;

/** Set of all FuncExpr subclasses (auto-populated by self-registration) */
export const ALL_FUNCTIONS: ReadonlySet<typeof FuncExpr> = _allFunctions;

/**
 * Set of JSON path part expression classes
 */
export const JSON_PATH_PARTS = new Set<typeof Expression>([
  JSONPathFilterExpr,
  JSONPathKeyExpr,
  JSONPathRecursiveExpr,
  JSONPathRootExpr,
  JSONPathScriptExpr,
  JSONPathSliceExpr,
  JSONPathSubscriptExpr,
  JSONPathUnionExpr,
  JSONPathWildcardExpr,
]);
