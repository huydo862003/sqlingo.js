// https://github.com/tobymao/sqlglot/blob/main/sqlglot/expressions.py

import { createHash } from 'crypto';
import {
  Dialect, type DialectType,
} from './dialects/dialect';
import type { Token } from './tokens';
import { ensureList } from './helper';
import {
  multiInherit, type RequiredMap,
} from './port_internals';
import {
  ErrorLevel, ParseError, parseOne,
} from '.';

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

export type ExpressionValue = Expression | string | boolean | number | undefined | null;
export type ExpressionValueList<T extends ExpressionValue = ExpressionValue> = T[];

/**
 * Base arguments that all Expression classes can accept.
 */
export interface BaseExpressionArgs {
  this?: ExpressionValue;
  expression?: Expression;
  expressions?: (Expression | string)[];
  alias?: TableAliasExpr | IdentifierExpr | string;
  isString?: boolean;
  to?: DataTypeExpr;
  [key: string]: ExpressionValueList | ExpressionValue | undefined;
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
  static argTypes = {} satisfies RequiredMap<BaseExpressionArgs>;

  /** Set of required argument names */

  constructor (args: BaseExpressionArgs) {
    this.args = args;
    for (const [argKey, value] of Object.entries(args)) {
      this._setParent(argKey, value);
    }
  }

  get this (): ExpressionValue {
    return this.args.this;
  }

  get expression (): Expression | undefined {
    return this.args.expression;
  }

  get expressions (): (string | Expression)[] {
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
    const field = this.args[key];
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
  set type (dtype: DataTypeExpr | string | undefined) {
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
  isType (...dtypes: Array<DataTypeExprKind | DataTypeExpr>): boolean {
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
    const stack: Array<[Expression, Expression]> = [[this, root]];

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
          copy.set(k, childCopy);
        } else if (Array.isArray(vs)) {
          copy.args[k] = [];
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
          copy.args[k] = vs;
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
    if (!Array.isArray(this.args[argKey])) {
      this.args[argKey] = [];
    }
    this._setParent(argKey, value);
    const values = this.args[argKey] as unknown[];
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
  set (
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

    if (index !== undefined) {
      const expressions = (this.args[argKey] || []) as ExpressionValueList;

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
      delete this.args[argKey];
      return;
    }

    this.args[argKey] = value;
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
    expressionTypes: Array<new (...args: never[]) => T>,
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
    expressionTypes: Array<new (...args: never[]) => T>,
    options?: { bfs?: boolean },
  ): Generator<T> {
    const bfs = options?.bfs ?? true;
    for (const expression of this.walk({ bfs })) {
      if (expressionTypes.some((type) => expression instanceof type)) {
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
    ...expressionTypes: Array<new (...args: never[]) => T>
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

  sql (dialect?: DialectType, opts?: Record<string, unknown>): string {
    const dialectInstance = Dialect.getOrRaise(dialect);
    return dialectInstance.generate(this, opts);
  }

  /**
   * Visits all tree nodes (excluding already transformed ones)
   * and applies the given transformation function to each node.
   *
   * @param func - a function which takes a node and kwargs object, and returns a
   *               new transformed node or the same node without modifications. If the function
   *               returns null/undefined, then the corresponding node will be removed from the
   *               syntax tree.
   * @param options - Options object
   * @param options.copy - if set to true a new tree instance is constructed, otherwise the tree is
   *                       modified in place (default: true)
   * @returns The transformed tree
   */
  transform (
    func: (node: Expression, options: Record<string, unknown>) => Expression | undefined,
    options?: { copy?: boolean;
      options?: Record<string, unknown>; },
  ): Expression {
    const copy = options?.copy ?? true;

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
        ...options,
        copy,
      });

      if (!root) {
        root = newNode;
      } else if (parent && argKey && newNode !== node) {
        parent.set(argKey, newNode, index);
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

    const value = parent.args[key];

    if (Array.isArray(expression) && value instanceof Expression) {
      // We are trying to replace an Expression with a list, so it's assumed that
      // the intention was to really replace the parent of this expression.
      value.parent?.replace(expression);
    } else {
      parent.set(key, expression, this.index);
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
        const v = this.args[key];
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
    expressions: (Expression | undefined)[],
    options?: {
      dialect?: DialectType;
      copy?: boolean;
      wrap?: boolean;
    },
  ): Expression | undefined {
    const copy = options?.copy ?? true;
    const wrap = options?.wrap ?? true;
    return and(expressions, {
      ...options,
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
    expressions: (Expression | undefined)[],
    options?: { dialect?: DialectType;
      copy?: boolean;
      wrap?: boolean; },
  ): Expression | undefined {
    const copy = options?.copy ?? true;
    const wrap = options?.wrap ?? true;
    return or(expressions, {
      ...options,
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
  not (options?: { copy?: boolean }): NotExpr | undefined {
    const copy = options?.copy ?? true;
    return or([this], {
      ...options,
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
    options?: {
      quoted?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      wrap?: boolean;
    },
  ): AliasExpr {
    const copy = options?.copy ?? true;
    const aliasName = typeof _alias === 'string'
      ? _alias
      : _alias.name;
    return alias(this, aliasName, {
      ...options,
      copy,
    });
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
    klass: new (arg: BaseExpressionArgs) => T,
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
    options?: {
      unnest?: string | Expression | Array<string | Expression>;
      copy?: boolean;
    },
  ): InExpr {
    const copy = options?.copy ?? true;
    const unnest = options?.unnest;

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
            ...options,
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
    options?: {
      copy?: boolean;
      symmetric?: boolean;
    },
  ): BetweenExpr {
    const copy = options?.copy ?? true;

    const between = new BetweenExpr({
      this: maybeCopy(this, copy),
      low: convert(low, copy),
      high: convert(high, copy),
    });

    if (options?.symmetric !== undefined) {
      between.set('symmetric', options.symmetric);
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
    div.set('typed', options?.typed ?? false);
    div.set('safe', options?.safe ?? false);
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
}

export type ConditionExprArgs = BaseExpressionArgs;

export class ConditionExpr extends Expression {
  key = ExpressionKey.CONDITION;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ConditionExprArgs>;

  declare args: ConditionExprArgs;
  constructor (args: ConditionExprArgs) {
    super(args);
  }
}

export type PredicateExprArgs = BaseExpressionArgs;

export class PredicateExpr extends Expression {
  key = ExpressionKey.PREDICATE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<PredicateExprArgs>;

  declare args: PredicateExprArgs;
  constructor (args: PredicateExprArgs) {
    super(args);
  }
}

export type DerivedTableExprArgs = BaseExpressionArgs;

export class DerivedTableExpr extends Expression {
  key = ExpressionKey.DERIVED_TABLE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DerivedTableExprArgs>;

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

export type QueryExprArgs = {
  with?: WithExpr;
} & BaseExpressionArgs;

export class QueryExpr extends Expression {
  key = ExpressionKey.QUERY;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<QueryExprArgs>;

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
  subquery (alias?: string | Expression, options: { copy?: boolean } = {}): SubqueryExpr {
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
    } = {},
  ): this {
    return _applyBuilder(expression, {
      instance: this,
      arg: 'limit',
      into: LimitExpr,
      prefix: 'LIMIT',
      intoArg: 'expression',
      ...options,
      copy: options.copy ?? true,
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
    } = {},
  ): this {
    return _applyBuilder(expression, {
      instance: this,
      arg: 'offset',
      into: OffsetExpr,
      prefix: 'OFFSET',
      intoArg: 'expression',
      ...options,
      copy: options.copy ?? true,
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
    expressions: Array<string | Expression>,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    return _applyChildListBuilder(expressions, {
      instance: this,
      arg: 'order',
      prefix: 'ORDER BY',
      into: OrderExpr,
      ...options,
      append: options.append ?? true,
      copy: options.copy ?? true,
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
    _expressions: Array<string | Expression>,
    _options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
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
    expressions: Array<string | Expression>,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const processedExpressions = expressions.map((expr) =>
      expr instanceof WhereExpr
        ? expr.this
        : expr);

    return _applyConjunctionBuilder(processedExpressions as (string | Expression)[], {
      instance: this,
      arg: 'where',
      into: WhereExpr,
      ...options,
      append: options.append ?? true,
      copy: options.copy ?? true,
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
    alias: string | Expression,
    as: string | Expression,
    options: {
      recursive?: boolean;
      materialized?: boolean;
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      scalar?: boolean;
    } = {},
  ): this {
    return _applyCteBuilder({
      instance: this,
      alias,
      as,
      ...options,
      recursive: options.recursive ?? false,
      append: options.append ?? true,
      copy: options.copy ?? true,
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
    expressions: Array<string | Expression>,
    options: {
      distinct?: boolean;
      dialect?: DialectType;
    } = {},
  ): UnionExpr {
    return union([this, ...expressions], {
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
    expressions: Array<string | Expression>,
    options: {
      distinct?: boolean;
      dialect?: DialectType;
    } = {},
  ): IntersectExpr {
    return intersect([this, ...expressions], {
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
    expressions: Array<string | Expression>,
    options: {
      distinct?: boolean;
      dialect?: DialectType;
    } = {},
  ): ExceptExpr {
    return except([this, ...expressions], {
      ...options,
      distinct: options.distinct ?? true,
    });
  }
}

export type UDTFExprArgs = {
  alias?: TableAliasExpr;
} & DerivedTableExprArgs;

export class UDTFExpr extends DerivedTableExpr {
  key = ExpressionKey.UDTF;

  static argTypes = {} satisfies RequiredMap<UDTFExprArgs>;

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

export type CacheExprArgs = {
  lazy?: Expression;
  options?: Expression[];
  this: Expression;
  expression?: ExpressionValue;
} & BaseExpressionArgs;

export class CacheExpr extends Expression {
  key = ExpressionKey.CACHE;

  /**
   * Defines the arguments (properties and child expressions) for Cache expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    lazy: false,
    options: false,
    expression: false,
  } satisfies RequiredMap<CacheExprArgs>;

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

  get $expression (): Expression | undefined {
    return this.args.expression;
  }
}

export type UncacheExprArgs = {
  exists?: Expression[];
  this: Expression;
} & BaseExpressionArgs;

export class UncacheExpr extends Expression {
  key = ExpressionKey.UNCACHE;

  /**
   * Defines the arguments (properties and child expressions) for Uncache expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    exists: false,
  } satisfies RequiredMap<UncacheExprArgs>;

  declare args: UncacheExprArgs;
  constructor (args: UncacheExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $exists (): Expression[] | undefined {
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

export type RefreshExprArgs = {
  kind: RefreshExprKind;
  this: Expression;
} & BaseExpressionArgs;

export class RefreshExpr extends Expression {
  key = ExpressionKey.REFRESH;

  /**
   * Defines the arguments (properties and child expressions) for Refresh expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    kind: true,
  } satisfies RequiredMap<RefreshExprArgs>;

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

export type DDLExprArgs = {
  with?: WithExpr;
  expression?: SelectExpr;
} & BaseExpressionArgs;

export class DDLExpr extends Expression {
  key = ExpressionKey.DDL;

  static argTypes = {} satisfies RequiredMap<DDLExprArgs>;

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

export type LockingStatementExprArgs = {
  this: Expression;
  expression: Expression;
} & BaseExpressionArgs;

export class LockingStatementExpr extends Expression {
  key = ExpressionKey.LOCKING_STATEMENT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<BaseExpressionArgs>;

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

export type DMLExprArgs = BaseExpressionArgs;

export class DMLExpr extends Expression {
  key = ExpressionKey.DML;

  static argTypes = {} satisfies RequiredMap<DMLExprArgs>;

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
    return _applyBuilder(expression, {
      instance: this,
      arg: 'returning',
      prefix: 'RETURNING',
      into: ReturningExpr,
      ...options,
      dialect: options.dialect,
      copy: options.copy ?? true,
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

export type CreateExprArgs = {
  with?: WithExpr;
  kind: CreateExprKind;
  exists?: Expression[];
  properties?: Expression[];
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
  expression?: Expression;
} & DDLExprArgs;

export class CreateExpr extends DDLExpr {
  key = ExpressionKey.CREATE;

  /**
   * Defines the arguments (properties and child expressions) for Create expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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
  } satisfies RequiredMap<CreateExprArgs>;

  declare args: CreateExprArgs;
  constructor (args: CreateExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
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

  get $exists (): Expression[] | undefined {
    return this.args.exists;
  }

  get $properties (): Expression[] | undefined {
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

export type SequencePropertiesExprArgs = {
  increment?: Expression;
  minvalue?: string;
  maxvalue?: string;
  cache?: Expression;
  start?: Expression;
  owned?: Expression;
  options?: Expression[];
} & BaseExpressionArgs;

export class SequencePropertiesExpr extends Expression {
  key = ExpressionKey.SEQUENCE_PROPERTIES;

  /**
   * Defines the arguments (properties and child expressions) for SequenceProperties expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    increment: false,
    minvalue: false,
    maxvalue: false,
    cache: false,
    start: false,
    owned: false,
    options: false,
  } satisfies RequiredMap<SequencePropertiesExprArgs>;

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

export type TruncateTableExprArgs = {
  isDatabase?: string;
  exists?: Expression[];
  only?: Expression;
  cluster?: Expression;
  identity?: Expression;
  option?: Expression;
  partition?: Expression;
  expressions: Expression[];
} & BaseExpressionArgs;

export class TruncateTableExpr extends Expression {
  key = ExpressionKey.TRUNCATE_TABLE;

  /**
   * Defines the arguments (properties and child expressions) for TruncateTable expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    expressions: true,
    isDatabase: false,
    exists: false,
    only: false,
    cluster: false,
    identity: false,
    option: false,
    partition: false,
  } satisfies RequiredMap<TruncateTableExprArgs>;

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

  get $exists (): Expression[] | undefined {
    return this.args.exists;
  }

  get $only (): Expression | undefined {
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

export type CloneExprArgs = {
  shallow?: Expression;
  copy?: unknown;
  this: Expression;
} & BaseExpressionArgs;

export class CloneExpr extends Expression {
  key = ExpressionKey.CLONE;

  /**
   * Defines the arguments (properties and child expressions) for Clone expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    shallow: false,
    copy: false,
  } satisfies RequiredMap<CloneExprArgs>;

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
export type DescribeExprArgs = {
  style?: Expression;
  kind?: DescribeExprKind;
  partition?: Expression;
  format?: string;
  asJson?: Expression;
  this: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class DescribeExpr extends Expression {
  key = ExpressionKey.DESCRIBE;

  /**
   * Defines the arguments (properties and child expressions) for Describe expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
    style: false,
    kind: false,
    partition: false,
    format: false,
    asJson: false,
  } satisfies RequiredMap<DescribeExprArgs>;

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

export type AttachExprArgs = {
  exists?: Expression[];
  this: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class AttachExpr extends Expression {
  key = ExpressionKey.ATTACH;

  /**
   * Defines the arguments (properties and child expressions) for Attach expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
    exists: false,
  } satisfies RequiredMap<AttachExprArgs>;

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

  get $exists (): Expression[] | undefined {
    return this.args.exists;
  }
}

export type DetachExprArgs = {
  exists?: Expression[];
  this: Expression;
} & BaseExpressionArgs;

export class DetachExpr extends Expression {
  key = ExpressionKey.DETACH;

  /**
   * Defines the arguments (properties and child expressions) for Detach expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    exists: false,
  } satisfies RequiredMap<DetachExprArgs>;

  declare args: DetachExprArgs;
  constructor (args: DetachExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $exists (): Expression[] | undefined {
    return this.args.exists;
  }
}

export type InstallExprArgs = {
  from?: Expression;
  force?: Expression;
  this: Expression;
} & BaseExpressionArgs;

export class InstallExpr extends Expression {
  key = ExpressionKey.INSTALL;

  /**
   * Defines the arguments (properties and child expressions) for Install expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    from: false,
    force: false,
  } satisfies RequiredMap<InstallExprArgs>;

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

export type SummarizeExprArgs = {
  table?: Expression;
  this: Expression;
} & BaseExpressionArgs;

export class SummarizeExpr extends Expression {
  key = ExpressionKey.SUMMARIZE;

  /**
   * Defines the arguments (properties and child expressions) for Summarize expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    table: false,
  } satisfies RequiredMap<SummarizeExprArgs>;

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

export type KillExprArgs = {
  kind?: KillExprKind;
  this: Expression;
} & BaseExpressionArgs;

export class KillExpr extends Expression {
  key = ExpressionKey.KILL;

  /**
   * Defines the arguments (properties and child expressions) for Kill expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    kind: false,
  } satisfies RequiredMap<KillExprArgs>;

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

export type PragmaExprArgs = BaseExpressionArgs;

export class PragmaExpr extends Expression {
  key = ExpressionKey.PRAGMA;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<PragmaExprArgs>;

  declare args: PragmaExprArgs;
  constructor (args: PragmaExprArgs) {
    super(args);
  }
}

export type DeclareExprArgs = {
  expressions: Expression[];
} & BaseExpressionArgs;

export class DeclareExpr extends Expression {
  key = ExpressionKey.DECLARE;

  static argTypes = { expressions: true } satisfies RequiredMap<BaseExpressionArgs>;

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

export type DeclareItemExprArgs = {
  kind?: DeclareItemExprKind;
  default?: Expression;
  this: Expression;
} & BaseExpressionArgs;

export class DeclareItemExpr extends Expression {
  key = ExpressionKey.DECLARE_ITEM;

  /**
   * Defines the arguments (properties and child expressions) for DeclareItem expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    kind: false,
    default: false,
  } satisfies RequiredMap<DeclareItemExprArgs>;

  declare args: DeclareItemExprArgs;
  constructor (args: DeclareItemExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): DeclareItemExprArgs | undefined {
    return this.args.kind;
  }

  get $default (): Expression | undefined {
    return this.args.default;
  }
}

export type SetExprArgs = {
  unset?: Expression;
  tag?: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class SetExpr extends Expression {
  key = ExpressionKey.SET;

  /**
   * Defines the arguments (properties and child expressions) for Set expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    expressions: false,
    unset: false,
    tag: false,
  } satisfies RequiredMap<SetExprArgs>;

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

export type HeredocExprArgs = {
  tag?: Expression;
  this: Expression;
} & BaseExpressionArgs;

export class HeredocExpr extends Expression {
  key = ExpressionKey.HEREDOC;

  /**
   * Defines the arguments (properties and child expressions) for Heredoc expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    tag: false,
  } satisfies RequiredMap<HeredocExprArgs>;

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

export type SetItemExprArgs = {
  kind?: SetItemExprKind;
  collate?: string;
  global?: boolean;
  this?: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class SetItemExpr extends Expression {
  key = ExpressionKey.SET_ITEM;

  /**
   * Defines the arguments (properties and child expressions) for SetItem expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    expressions: false,
    kind: false,
    collate: false,
    global: false,
  } satisfies RequiredMap<SetItemExprArgs>;

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

export type QueryBandExprArgs = {
  scope?: Expression;
  update?: Expression;
  this: Expression;
} & BaseExpressionArgs;

export class QueryBandExpr extends Expression {
  key = ExpressionKey.QUERY_BAND;

  /**
   * Defines the arguments (properties and child expressions) for QueryBand expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    scope: false,
    update: false,
  } satisfies RequiredMap<QueryBandExprArgs>;

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

export type ShowExprArgs = {
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
} & BaseExpressionArgs;

export class ShowExpr extends Expression {
  key = ExpressionKey.SHOW;

  /**
   * Defines the arguments (properties and child expressions) for Show expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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
  } satisfies RequiredMap<ShowExprArgs>;

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

export type UserDefinedFunctionExprArgs = {
  wrapped?: Expression;
  this: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class UserDefinedFunctionExpr extends Expression {
  key = ExpressionKey.USER_DEFINED_FUNCTION;

  /**
   * Defines the arguments (properties and child expressions) for UserDefinedFunction expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
    wrapped: false,
  } satisfies RequiredMap<UserDefinedFunctionExprArgs>;

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

export type CharacterSetExprArgs = {
  default?: Expression;
  this: Expression;
} & BaseExpressionArgs;

export class CharacterSetExpr extends Expression {
  key = ExpressionKey.CHARACTER_SET;

  /**
   * Defines the arguments (properties and child expressions) for CharacterSet expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    default: false,
  } satisfies RequiredMap<CharacterSetExprArgs>;

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

export type RecursiveWithSearchExprArgs = {
  kind: RecursiveWithSearchExprKind;
  using?: string;
  this: Expression;
  expression: Expression;
} & BaseExpressionArgs;

export class RecursiveWithSearchExpr extends Expression {
  key = ExpressionKey.RECURSIVE_WITH_SEARCH;

  /**
   * Defines the arguments (properties and child expressions) for RecursiveWithSearch expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    kind: true,
    this: true,
    expression: true,
    using: false,
  } satisfies RequiredMap<RecursiveWithSearchExprArgs>;

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

export type WithExprArgs = {
  recursive?: boolean;
  search?: Expression;
  expressions: CTEExpr[];
} & BaseExpressionArgs;

export class WithExpr extends Expression {
  key = ExpressionKey.WITH;

  /**
   * Defines the arguments (properties and child expressions) for With expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    expressions: true,
    recursive: false,
    search: false,
  } satisfies RequiredMap<WithExprArgs>;

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

export type WithinGroupExprArgs = {
  this: Expression;
  expression?: Expression;
} & BaseExpressionArgs;
export class WithinGroupExpr extends Expression {
  key = ExpressionKey.WITHIN_GROUP;

  /**
   * Defines the arguments (properties and child expressions) for WithinGroup expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<BaseExpressionArgs>;

  declare args: WithinGroupExprArgs;
  constructor (args: WithinGroupExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }
}

export type ProjectionDefExprArgs = {
  this: Expression;
  expression: Expression;
} & BaseExpressionArgs;
export class ProjectionDefExpr extends Expression {
  key = ExpressionKey.PROJECTION_DEF;

  /**
   * Defines the arguments (properties and child expressions) for ProjectionDef expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<BaseExpressionArgs>;

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

export type TableAliasExprArgs = {
  columns?: Expression[];
  this?: Expression;
} & BaseExpressionArgs;

export class TableAliasExpr extends Expression {
  key = ExpressionKey.TABLE_ALIAS;

  /**
   * Defines the arguments (properties and child expressions) for TableAlias expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    columns: false,
  } satisfies RequiredMap<TableAliasExprArgs>;

  declare args: TableAliasExprArgs;

  constructor (args: TableAliasExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $columns (): Expression[] | undefined {
    return this.args.columns;
  }

  get columns (): Expression[] {
    return this.args.columns || [];
  }
}

export type ColumnPositionExprArgs = {
  position: Expression;
  this?: Expression;
} & BaseExpressionArgs;

export class ColumnPositionExpr extends Expression {
  key = ExpressionKey.COLUMN_POSITION;

  /**
   * Defines the arguments (properties and child expressions) for ColumnPosition expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    position: true,
  } satisfies RequiredMap<ColumnPositionExprArgs>;

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

export type ColumnDefExprArgs = {
  kind?: ColumnDefExprKind;
  constraints?: ColumnConstraintExpr[];
  exists?: Expression[];
  position?: Expression;
  default?: Expression;
  output?: Expression;
  this: Expression;
} & BaseExpressionArgs;

export class ColumnDefExpr extends Expression {
  key = ExpressionKey.COLUMN_DEF;

  /**
   * Defines the arguments (properties and child expressions) for ColumnDef expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    kind: false,
    constraints: false,
    exists: false,
    position: false,
    default: false,
    output: false,
  } satisfies RequiredMap<ColumnDefExprArgs>;

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

  get $exists (): Expression[] | undefined {
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

export type AlterColumnExprArgs = {
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
} & BaseExpressionArgs;

export class AlterColumnExpr extends Expression {
  key = ExpressionKey.ALTER_COLUMN;

  /**
   * Defines the arguments (properties and child expressions) for AlterColumn expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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
  } satisfies RequiredMap<AlterColumnExprArgs>;

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

export type AlterIndexExprArgs = {
  visible: Expression;
  this: Expression;
} & BaseExpressionArgs;

export class AlterIndexExpr extends Expression {
  key = ExpressionKey.ALTER_INDEX;

  /**
   * Defines the arguments (properties and child expressions) for AlterIndex expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    visible: true,
  } satisfies RequiredMap<AlterIndexExprArgs>;

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

export type AlterDistStyleExprArgs = BaseExpressionArgs;

export class AlterDistStyleExpr extends Expression {
  key = ExpressionKey.ALTER_DIST_STYLE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AlterDistStyleExprArgs>;

  declare args: AlterDistStyleExprArgs;
  constructor (args: AlterDistStyleExprArgs) {
    super(args);
  }
}

export type AlterSortKeyExprArgs = {
  compound?: Expression;
  this?: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class AlterSortKeyExpr extends Expression {
  key = ExpressionKey.ALTER_SORT_KEY;

  /**
   * Defines the arguments (properties and child expressions) for AlterSortKey expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    expressions: false,
    compound: false,
  } satisfies RequiredMap<AlterSortKeyExprArgs>;

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

export type AlterSetExprArgs = {
  option?: Expression;
  tablespace?: Expression;
  accessMethod?: string;
  fileFormat?: string;
  copyOptions?: Expression[];
  tag?: Expression;
  location?: Expression;
  serde?: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class AlterSetExpr extends Expression {
  key = ExpressionKey.ALTER_SET;

  /**
   * Defines the arguments (properties and child expressions) for AlterSet expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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
  } satisfies RequiredMap<AlterSetExprArgs>;

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

export type RenameColumnExprArgs = {
  to: Expression;
  exists?: Expression[];
  this: Expression;
} & BaseExpressionArgs;

export class RenameColumnExpr extends Expression {
  key = ExpressionKey.RENAME_COLUMN;

  /**
   * Defines the arguments (properties and child expressions) for RenameColumn expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    to: true,
    exists: false,
  } satisfies RequiredMap<RenameColumnExprArgs>;

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

  get $exists (): Expression[] | undefined {
    return this.args.exists;
  }
}

export type AlterRenameExprArgs = BaseExpressionArgs;

export class AlterRenameExpr extends Expression {
  key = ExpressionKey.ALTER_RENAME;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AlterRenameExprArgs>;

  declare args: AlterRenameExprArgs;
  constructor (args: AlterRenameExprArgs) {
    super(args);
  }
}

export type SwapTableExprArgs = BaseExpressionArgs;

export class SwapTableExpr extends Expression {
  key = ExpressionKey.SWAP_TABLE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SwapTableExprArgs>;

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

export type CommentExprArgs = {
  kind: CommentExprKind;
  exists?: Expression[];
  materialized?: boolean;
  this: Expression;
  expression: Expression;
} & BaseExpressionArgs;

export class CommentExpr extends Expression {
  key = ExpressionKey.COMMENT;

  /**
   * Defines the arguments (properties and child expressions) for Comment expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    kind: true,
    expression: true,
    exists: false,
    materialized: false,
  } satisfies RequiredMap<CommentExprArgs>;

  declare args: CommentExprArgs;
  constructor (args: CommentExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): string {
    return this.args.kind;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get $exists (): Expression[] | undefined {
    return this.args.exists;
  }

  get $materialized (): boolean | undefined {
    return this.args.materialized;
  }
}

export type ComprehensionExprArgs = {
  position?: Expression;
  iterator: Expression;
  condition?: Expression;
  this: Expression;
  expression: Expression;
} & BaseExpressionArgs;

export class ComprehensionExpr extends Expression {
  key = ExpressionKey.COMPREHENSION;

  /**
   * Defines the arguments (properties and child expressions) for Comprehension expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    position: false,
    iterator: true,
    condition: false,
  } satisfies RequiredMap<ComprehensionExprArgs>;

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

export type MergeTreeTTLActionExprArgs = {
  delete?: Expression;
  recompress?: Expression[];
  toDisk?: Expression;
  toVolume?: Expression;
  this: Expression;
} & BaseExpressionArgs;

export class MergeTreeTTLActionExpr extends Expression {
  key = ExpressionKey.MERGE_TREE_TTL_ACTION;

  /**
   * Defines the arguments (properties and child expressions) for MergeTreeTTLAction expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    delete: false,
    recompress: false,
    toDisk: false,
    toVolume: false,
  } satisfies RequiredMap<MergeTreeTTLActionExprArgs>;

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

export type MergeTreeTTLExprArgs = {
  where?: Expression;
  group?: Expression;
  aggregates?: Expression[];
  expressions: Expression[];
} & BaseExpressionArgs;

export class MergeTreeTTLExpr extends Expression {
  key = ExpressionKey.MERGE_TREE_TTL;

  /**
   * Defines the arguments (properties and child expressions) for MergeTreeTTL expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    expressions: true,
    where: false,
    group: false,
    aggregates: false,
  } satisfies RequiredMap<MergeTreeTTLExprArgs>;

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

export type IndexConstraintOptionExprArgs = {
  keyBlockSize?: number | Expression;
  using?: string;
  parser?: Expression;
  comment?: string;
  visible?: Expression;
  engineAttr?: string;
  secondaryEngineAttr?: string;
} & BaseExpressionArgs;

export class IndexConstraintOptionExpr extends Expression {
  key = ExpressionKey.INDEX_CONSTRAINT_OPTION;

  /**
   * Defines the arguments (properties and child expressions) for IndexConstraintOption expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    keyBlockSize: false,
    using: false,
    parser: false,
    comment: false,
    visible: false,
    engineAttr: false,
    secondaryEngineAttr: false,
  } satisfies RequiredMap<IndexConstraintOptionExprArgs>;

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

export type ColumnConstraintExprArgs = {
  kind: ColumnConstraintExprKind;
  this?: Expression;
} & BaseExpressionArgs;

export class ColumnConstraintExpr extends Expression {
  key = ExpressionKey.COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for ColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    kind: true,
  } satisfies RequiredMap<ColumnConstraintExprArgs>;

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

export type ColumnConstraintKindExprArgs = BaseExpressionArgs;

export class ColumnConstraintKindExpr extends Expression {
  key = ExpressionKey.COLUMN_CONSTRAINT_KIND;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ColumnConstraintKindExprArgs>;

  declare args: ColumnConstraintKindExprArgs;
  constructor (args: ColumnConstraintKindExprArgs) {
    super(args);
  }
}

export type WithOperatorExprArgs = {
  op: Expression;
  this: Expression;
} & BaseExpressionArgs;

export class WithOperatorExpr extends Expression {
  key = ExpressionKey.WITH_OPERATOR;

  /**
   * Defines the arguments (properties and child expressions) for WithOperator expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    op: true,
  } satisfies RequiredMap<WithOperatorExprArgs>;

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

export type WatermarkColumnConstraintExprArgs = {
  this: Expression;
  expression: Expression;
} & BaseExpressionArgs;

export class WatermarkColumnConstraintExpr extends Expression {
  key = ExpressionKey.WATERMARK_COLUMN_CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<BaseExpressionArgs>;

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

export type ConstraintExprArgs = {
  this: Expression;
  expressions: Expression[];
} & BaseExpressionArgs;

export class ConstraintExpr extends Expression {
  key = ExpressionKey.CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
  } satisfies RequiredMap<BaseExpressionArgs>;

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

export type DeleteExprArgs = {
  with?: Expression;
  using?: string;
  where?: Expression;
  returning?: Expression;
  order?: Expression;
  limit?: number | Expression;
  tables?: Expression[];
  cluster?: Expression;
  this?: Expression;
} & DMLExprArgs;

export class DeleteExpr extends DMLExpr {
  key = ExpressionKey.DELETE;

  /**
   * Defines the arguments (properties and child expressions) for Delete expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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
  } satisfies RequiredMap<DeleteExprArgs>;

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
    return _applyBuilder(table, {
      instance: this,
      arg: 'this',
      into: TableExpr,
      ...options,
      dialect: options.dialect,
      copy: options.copy ?? true,
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
    expressions: Array<string | Expression>,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    return _applyConjunctionBuilder(expressions, {
      instance: this,
      arg: 'where',
      into: WhereExpr,
      ...options,
      append: options.append ?? true,
      copy: options.copy ?? true,
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

export type DropExprArgs = {
  kind?: DropExprKind;
  exists?: Expression[];
  temporary?: boolean;
  materialized?: boolean;
  cascade?: Expression;
  constraints?: Expression[];
  purge?: Expression;
  cluster?: Expression;
  concurrently?: Expression;
  this?: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class DropExpr extends Expression {
  key = ExpressionKey.DROP;

  /**
   * Defines the arguments (properties and child expressions) for Drop expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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
  } satisfies RequiredMap<DropExprArgs>;

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

  get $exists (): Expression[] | undefined {
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

export type ExportExprArgs = {
  connection?: Expression;
  options: Expression[];
  this: Expression;
} & BaseExpressionArgs;

export class ExportExpr extends Expression {
  key = ExpressionKey.EXPORT;

  /**
   * Defines the arguments (properties and child expressions) for Export expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    connection: false,
    options: true,
  } satisfies RequiredMap<ExportExprArgs>;

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

export type FilterExprArgs = {
  this: Expression;
  expression: Expression;
} & BaseExpressionArgs;

export class FilterExpr extends Expression {
  key = ExpressionKey.FILTER;

  /**
   * Defines the arguments (properties and child expressions) for Filter expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<FilterExprArgs>;

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

export type CheckExprArgs = BaseExpressionArgs;

export class CheckExpr extends Expression {
  key = ExpressionKey.CHECK;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CheckExprArgs>;

  declare args: CheckExprArgs;

  constructor (args: CheckExprArgs) {
    super(args);
  }
}

export type ChangesExprArgs = {
  information: string;
  atBefore?: Expression;
  end?: Expression;
} & BaseExpressionArgs;

export class ChangesExpr extends Expression {
  key = ExpressionKey.CHANGES;

  /**
   * Defines the arguments (properties and child expressions) for Changes expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    information: true,
    atBefore: false,
    end: false,
  } satisfies RequiredMap<ChangesExprArgs>;

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

export type ConnectExprArgs = {
  start?: Expression;
  connect: Expression;
  nocycle?: Expression;
} & BaseExpressionArgs;

export class ConnectExpr extends Expression {
  key = ExpressionKey.CONNECT;

  /**
   * Defines the arguments (properties and child expressions) for Connect expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    start: false,
    connect: true,
    nocycle: false,
  } satisfies RequiredMap<ConnectExprArgs>;

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

export type CopyParameterExprArgs = {
  this: Expression;
  expression?: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class CopyParameterExpr extends Expression {
  key = ExpressionKey.COPY_PARAMETER;

  /**
   * Defines the arguments (properties and child expressions) for CopyParameter expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
    expressions: false,
  } satisfies RequiredMap<CopyParameterExprArgs>;

  declare args: CopyParameterExprArgs;

  constructor (args: CopyParameterExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

export type CredentialsExprArgs = {
  credentials?: Expression[];
  encryption?: Expression;
  storage?: Expression;
  iamRole?: Expression;
  region?: Expression;
} & BaseExpressionArgs;

export class CredentialsExpr extends Expression {
  key = ExpressionKey.CREDENTIALS;

  /**
   * Defines the arguments (properties and child expressions) for Credentials expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    credentials: false,
    encryption: false,
    storage: false,
    iamRole: false,
    region: false,
  } satisfies RequiredMap<CredentialsExprArgs>;

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

export type PriorExprArgs = BaseExpressionArgs;

export class PriorExpr extends Expression {
  key = ExpressionKey.PRIOR;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<PriorExprArgs>;

  declare args: PriorExprArgs;

  constructor (args: PriorExprArgs) {
    super(args);
  }
}

export type DirectoryExprArgs = {
  local?: Expression;
  rowFormat?: string;
  this: Expression;
} & BaseExpressionArgs;

export class DirectoryExpr extends Expression {
  key = ExpressionKey.DIRECTORY;

  /**
   * Defines the arguments (properties and child expressions) for Directory expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    local: false,
    rowFormat: false,
  } satisfies RequiredMap<DirectoryExprArgs>;

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

export type DirectoryStageExprArgs = BaseExpressionArgs;
export class DirectoryStageExpr extends Expression {
  key = ExpressionKey.DIRECTORY_STAGE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DirectoryStageExprArgs>;

  declare args: DirectoryStageExprArgs;

  constructor (args: DirectoryStageExprArgs) {
    super(args);
  }
}

export type ForeignKeyExprArgs = {
  reference?: Expression;
  delete?: Expression;
  update?: Expression;
  options?: Expression[];
  expressions?: Expression[];
} & BaseExpressionArgs;

export class ForeignKeyExpr extends Expression {
  key = ExpressionKey.FOREIGN_KEY;

  /**
   * Defines the arguments (properties and child expressions) for ForeignKey expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    expressions: false,
    reference: false,
    delete: false,
    update: false,
    options: false,
  } satisfies RequiredMap<ForeignKeyExprArgs>;

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

export type ColumnPrefixExprArgs = {
  this: Expression;
  expression: Expression;
} & BaseExpressionArgs;

export class ColumnPrefixExpr extends Expression {
  key = ExpressionKey.COLUMN_PREFIX;

  /**
   * Defines the arguments (properties and child expressions) for ColumnPrefix expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<BaseExpressionArgs>;

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

export type PrimaryKeyExprArgs = {
  options?: Expression[];
  include?: Expression;
  this?: Expression;
  expressions: Expression[];
} & BaseExpressionArgs;

export class PrimaryKeyExpr extends Expression {
  key = ExpressionKey.PRIMARY_KEY;

  /**
   * Defines the arguments (properties and child expressions) for PrimaryKey expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    expressions: true,
    options: false,
    include: false,
  } satisfies RequiredMap<PrimaryKeyExprArgs>;

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

export type IntoExprArgs = {
  temporary?: boolean;
  unlogged?: Expression;
  bulkCollect?: Expression;
  this?: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class IntoExpr extends Expression {
  key = ExpressionKey.INTO;

  /**
   * Defines the arguments (properties and child expressions) for Into expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    temporary: false,
    unlogged: false,
    bulkCollect: false,
    expressions: false,
  } satisfies RequiredMap<IntoExprArgs>;

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

export type FromExprArgs = BaseExpressionArgs;

export class FromExpr extends Expression {
  key = ExpressionKey.FROM;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<FromExprArgs>;

  declare args: FromExprArgs;

  constructor (args: FromExprArgs) {
    super(args);
  }

  /**
   * Gets the name of the FROM expression
   * @returns The name of the expression
   */
  get name (): string {
    return this.this?.name || '';
  }

  /**
   * Gets the alias or name of the FROM expression
   * @returns The alias if it exists, otherwise the name
   */
  get aliasOrName (): string {
    return this.this?.aliasOrName || '';
  }
}

export type HavingExprArgs = BaseExpressionArgs;

export class HavingExpr extends Expression {
  key = ExpressionKey.HAVING;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<HavingExprArgs>;

  declare args: HavingExprArgs;

  constructor (args: HavingExprArgs) {
    super(args);
  }
}

export type HintExprArgs = {
  expressions: Expression[];
} & BaseExpressionArgs;
export class HintExpr extends Expression {
  key = ExpressionKey.HINT;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<BaseExpressionArgs>;

  declare args: HintExprArgs;

  constructor (args: HintExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type JoinHintExprArgs = {
  this: Expression;
  expressions: Expression[];
} & BaseExpressionArgs;

export class JoinHintExpr extends Expression {
  key = ExpressionKey.JOIN_HINT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
  } satisfies RequiredMap<BaseExpressionArgs>;

  declare args: JoinHintExprArgs;

  constructor (args: JoinHintExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type IdentifierExprArgs = {
  quoted?: boolean;
  global?: boolean;
  temporary?: boolean;
  this: Expression;
} & BaseExpressionArgs;

export class IdentifierExpr extends Expression {
  key = ExpressionKey.IDENTIFIER;

  /**
   * Defines the arguments (properties and child expressions) for Identifier expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    quoted: false,
    global: false,
    temporary: false,
  } satisfies RequiredMap<IdentifierExprArgs>;

  declare args: IdentifierExprArgs;

  constructor (args: IdentifierExprArgs) {
    super(args);
  }

  get outputName (): string {
    return this.name;
  }

  get $this (): Expression {
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

export type OpclassExprArgs = {
  this: Expression;
  expression: Expression;
} & BaseExpressionArgs;

export class OpclassExpr extends Expression {
  key = ExpressionKey.OPCLASS;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<BaseExpressionArgs>;

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

export type IndexExprArgs = {
  table?: Expression;
  unique?: boolean;
  primary?: boolean;
  amp?: Expression;
  params?: Expression[];
  this?: Expression;
} & BaseExpressionArgs;

export class IndexExpr extends Expression {
  key = ExpressionKey.INDEX;

  /**
   * Defines the arguments (properties and child expressions) for Index expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    table: false,
    unique: false,
    primary: false,
    amp: false,
    params: false,
  } satisfies RequiredMap<IndexExprArgs>;

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

export type IndexParametersExprArgs = {
  using?: string;
  include?: Expression;
  columns?: Expression[];
  withStorage?: Expression;
  partitionBy?: Expression;
  tablespace?: Expression;
  where?: Expression;
  on?: Expression;
} & BaseExpressionArgs;

export class IndexParametersExpr extends Expression {
  key = ExpressionKey.INDEX_PARAMETERS;

  /**
   * Defines the arguments (properties and child expressions) for IndexParameters expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    using: false,
    include: false,
    columns: false,
    withStorage: false,
    partitionBy: false,
    tablespace: false,
    where: false,
    on: false,
  } satisfies RequiredMap<IndexParametersExprArgs>;

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

export type ConditionalInsertExprArgs = {
  else?: Expression;
  this: Expression;
  expression?: Expression;
} & BaseExpressionArgs;

export class ConditionalInsertExpr extends Expression {
  key = ExpressionKey.CONDITIONAL_INSERT;

  /**
   * Defines the arguments (properties and child expressions) for ConditionalInsert expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
    else: false,
  } satisfies RequiredMap<ConditionalInsertExprArgs>;

  declare args: ConditionalInsertExprArgs;

  constructor (args: ConditionalInsertExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
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

export type MultitableInsertsExprArgs = {
  kind: MultitableInsertsExprKind;
  source: Expression;
  expressions: Expression[];
} & BaseExpressionArgs;

export class MultitableInsertsExpr extends Expression {
  key = ExpressionKey.MULTITABLE_INSERTS;

  /**
   * Defines the arguments (properties and child expressions) for MultitableInserts expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    expressions: true,
    kind: true,
    source: true,
  } satisfies RequiredMap<MultitableInsertsExprArgs>;

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

export type OnConflictExprArgs = {
  duplicate?: Expression;
  action?: Expression;
  conflictKeys?: Expression[];
  indexPredicate?: Expression;
  constraint?: Expression;
  where?: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class OnConflictExpr extends Expression {
  key = ExpressionKey.ON_CONFLICT;

  /**
   * Defines the arguments (properties and child expressions) for OnConflict expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    duplicate: false,
    expressions: false,
    action: false,
    conflictKeys: false,
    indexPredicate: false,
    constraint: false,
    where: false,
  } satisfies RequiredMap<OnConflictExprArgs>;

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

export type OnConditionExprArgs = {
  error?: Expression;
  empty?: Expression;
  null?: Expression;
} & BaseExpressionArgs;

export class OnConditionExpr extends Expression {
  key = ExpressionKey.ON_CONDITION;

  /**
   * Defines the arguments (properties and child expressions) for OnCondition expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    error: false,
    empty: false,
    null: false,
  } satisfies RequiredMap<OnConditionExprArgs>;

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

export type ReturningExprArgs = {
  into?: Expression;
  expressions: Expression[];
} & BaseExpressionArgs;

export class ReturningExpr extends Expression {
  key = ExpressionKey.RETURNING;

  /**
   * Defines the arguments (properties and child expressions) for Returning expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    expressions: true,
    into: false,
  } satisfies RequiredMap<ReturningExprArgs>;

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

export type IntroducerExprArgs = {
  this: Expression;
  expression: Expression;
} & BaseExpressionArgs;

export class IntroducerExpr extends Expression {
  key = ExpressionKey.INTRODUCER;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<BaseExpressionArgs>;

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

export type NationalExprArgs = BaseExpressionArgs;

export class NationalExpr extends Expression {
  key = ExpressionKey.NATIONAL;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<NationalExprArgs>;

  declare args: NationalExprArgs;

  constructor (args: NationalExprArgs) {
    super(args);
  }
}

export type LoadDataExprArgs = {
  local?: Expression;
  overwrite?: Expression;
  inpath: Expression;
  partition?: Expression;
  inputFormat?: string;
  serde?: Expression;
  this: Expression;
} & BaseExpressionArgs;

export class LoadDataExpr extends Expression {
  key = ExpressionKey.LOAD_DATA;

  /**
   * Defines the arguments (properties and child expressions) for LoadData expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    local: false,
    overwrite: false,
    inpath: true,
    partition: false,
    inputFormat: false,
    serde: false,
  } satisfies RequiredMap<LoadDataExprArgs>;

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

export type PartitionExprArgs = {
  subpartition?: Expression;
  expressions: Expression[];
} & BaseExpressionArgs;

export class PartitionExpr extends Expression {
  key = ExpressionKey.PARTITION;

  /**
   * Defines the arguments (properties and child expressions) for Partition expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    expressions: true,
    subpartition: false,
  } satisfies RequiredMap<PartitionExprArgs>;

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

export type PartitionRangeExprArgs = {
  this: Expression;
  expression?: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class PartitionRangeExpr extends Expression {
  key = ExpressionKey.PARTITION_RANGE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
    expressions: false,
  } satisfies RequiredMap<PartitionRangeExprArgs>;

  declare args: PartitionRangeExprArgs;

  constructor (args: PartitionRangeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

export type PartitionIdExprArgs = BaseExpressionArgs;

export class PartitionIdExpr extends Expression {
  key = ExpressionKey.PARTITION_ID;
  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<PartitionIdExprArgs>;

  declare args: PartitionIdExprArgs;

  constructor (args: PartitionIdExprArgs) {
    super(args);
  }
}

export type FetchExprArgs = {
  direction?: Expression;
  count?: Expression;
  limitOptions?: Expression[];
} & BaseExpressionArgs;

export class FetchExpr extends Expression {
  key = ExpressionKey.FETCH;

  /**
   * Defines the arguments (properties and child expressions) for Fetch expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    direction: false,
    count: false,
    limitOptions: false,
  } satisfies RequiredMap<FetchExprArgs>;

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

export type GrantExprArgs = {
  privileges: Expression[];
  kind?: GrantExprKind;
  securable: Expression;
  principals: Expression[];
  grantOption?: Expression;
} & BaseExpressionArgs;

export class GrantExpr extends Expression {
  key = ExpressionKey.GRANT;

  /**
   * Defines the arguments (properties and child expressions) for Grant expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    privileges: true,
    kind: false,
    securable: true,
    principals: true,
    grantOption: false,
  } satisfies RequiredMap<GrantExprArgs>;

  declare args: GrantExprArgs;

  constructor (args: GrantExprArgs) {
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
}

export type RevokeExprArgs = {
  cascade?: Expression;
  privileges: Expression[];
  kind?: string;
  securable: Expression;
  principals: Expression[];
  grantOption?: Expression;
} & BaseExpressionArgs;

export class RevokeExpr extends Expression {
  key = ExpressionKey.REVOKE;

  /**
   * Defines the arguments (properties and child expressions) for Revoke expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   * Extends Grant's arg_types with additional cascade field.
   */
  static argTypes = {
    ...super.argTypes,
    privileges: true,
    kind: false,
    securable: true,
    principals: true,
    grantOption: false,
    cascade: false,
  } satisfies RequiredMap<RevokeExprArgs>;

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

export type GroupExprArgs = {
  groupingSets?: Expression[];
  cube?: Expression;
  rollup?: Expression;
  totals?: Expression[];
  all?: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class GroupExpr extends Expression {
  key = ExpressionKey.GROUP;

  /**
   * Defines the arguments (properties and child expressions) for Group expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    expressions: false,
    groupingSets: false,
    cube: false,
    rollup: false,
    totals: false,
    all: false,
  } satisfies RequiredMap<GroupExprArgs>;

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

export type CubeExprArgs = {
  expressions?: Expression[];
} & BaseExpressionArgs;

export class CubeExpr extends Expression {
  key = ExpressionKey.CUBE;

  static argTypes = {
    ...super.argTypes,
    expressions: false,
  } satisfies RequiredMap<CubeExprArgs>;

  declare args: CubeExprArgs;
  constructor (args: CubeExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

export type RollupExprArgs = {
  expressions?: Expression[];
} & BaseExpressionArgs;

export class RollupExpr extends Expression {
  key = ExpressionKey.ROLLUP;

  static argTypes = {
    ...super.argTypes,
    expressions: false,
  } satisfies RequiredMap<RollupExprArgs>;

  declare args: RollupExprArgs;

  constructor (args: RollupExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }
}

export type GroupingSetsExprArgs = {
  expressions: Expression[];
} & BaseExpressionArgs;

export class GroupingSetsExpr extends Expression {
  key = ExpressionKey.GROUPING_SETS;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<GroupingSetsExprArgs>;

  declare args: GroupingSetsExprArgs;
  constructor (args: GroupingSetsExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type LambdaExprArgs = {
  colon?: Expression;
  this: Expression;
  expressions: Expression[];
} & BaseExpressionArgs;

export class LambdaExpr extends Expression {
  key = ExpressionKey.LAMBDA;

  /**
   * Defines the arguments (properties and child expressions) for Lambda expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
    colon: false,
  } satisfies RequiredMap<LambdaExprArgs>;

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

export type LimitExprArgs = {
  offset?: boolean;
  limitOptions?: Expression[];
  this?: Expression;
  expression: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class LimitExpr extends Expression {
  key = ExpressionKey.LIMIT;

  /**
   * Defines the arguments (properties and child expressions) for Limit expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    expression: true,
    offset: false,
    limitOptions: false,
    expressions: false,
  } satisfies RequiredMap<LimitExprArgs>;

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

export type LimitOptionsExprArgs = {
  percent?: Expression;
  rows?: Expression[];
  withTies?: Expression[];
} & BaseExpressionArgs;

export class LimitOptionsExpr extends Expression {
  key = ExpressionKey.LIMIT_OPTIONS;

  /**
   * Defines the arguments (properties and child expressions) for LimitOptions expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    percent: false,
    rows: false,
    withTies: false,
  } satisfies RequiredMap<LimitOptionsExprArgs>;

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
export type JoinExprArgs = {
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
} & BaseExpressionArgs;

export class JoinExpr extends Expression {
  key = ExpressionKey.JOIN;

  /**
   * Defines the arguments (properties and child expressions) for Join expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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
  } satisfies RequiredMap<JoinExprArgs>;

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
    expressions: Array<string | Expression>,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const join = _applyConjunctionBuilder(expressions, {
      instance: this,
      arg: 'on',
      append: options.append,
      dialect: options.dialect,
      copy: options.copy,
      ...options,
    }) as this;

    if (join.$kind === JoinExprKind.CROSS) {
      join.set('kind', undefined);
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
    expressions: Array<string | Expression>,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const join = _applyListBuilder(expressions, {
      instance: this,
      arg: 'using',
      append: options.append,
      dialect: options.dialect,
      copy: options.copy,
      ...options,
    }) as this;

    if (join.$kind === JoinExprKind.CROSS) {
      join.set('kind', undefined);
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

export type MatchRecognizeMeasureExprArgs = {
  windowFrame?: Expression;
  this: Expression;
} & BaseExpressionArgs;

export class MatchRecognizeMeasureExpr extends Expression {
  key = ExpressionKey.MATCH_RECOGNIZE_MEASURE;

  /**
   * Defines the arguments (properties and child expressions) for MatchRecognizeMeasure expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    windowFrame: false,
  } satisfies RequiredMap<MatchRecognizeMeasureExprArgs>;

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

export type MatchRecognizeExprArgs = {
  partitionBy?: Expression;
  order?: Expression;
  measures?: Expression[];
  rows?: Expression[];
  after?: Expression;
  pattern?: Expression;
  define?: Expression;
  alias?: TableAliasExpr;
} & BaseExpressionArgs;

export class MatchRecognizeExpr extends Expression {
  key = ExpressionKey.MATCH_RECOGNIZE;

  /**
   * Defines the arguments (properties and child expressions) for MatchRecognize expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    partitionBy: false,
    order: false,
    measures: false,
    rows: false,
    after: false,
    pattern: false,
    define: false,
    alias: false,
  } satisfies RequiredMap<MatchRecognizeExprArgs>;

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

export type FinalExprArgs = BaseExpressionArgs;

export class FinalExpr extends Expression {
  key = ExpressionKey.FINAL;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<FinalExprArgs>;

  declare args: FinalExprArgs;

  constructor (args: FinalExprArgs) {
    super(args);
  }
}

export type OffsetExprArgs = {
  this?: Expression;
  expression: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class OffsetExpr extends Expression {
  key = ExpressionKey.OFFSET;

  static argTypes = {
    ...super.argTypes,
    this: false,
    expression: true,
    expressions: false,
  } satisfies RequiredMap<BaseExpressionArgs>;

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

export type OrderExprArgs = {
  siblings?: Expression[];
  this?: Expression;
  expressions: Expression[];
} & BaseExpressionArgs;

export class OrderExpr extends Expression {
  key = ExpressionKey.ORDER;

  /**
   * Defines the arguments (properties and child expressions) for Order expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    expressions: true,
    siblings: false,
  } satisfies RequiredMap<OrderExprArgs>;

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

export type WithFillExprArgs = {
  from?: Expression;
  to?: Expression;
  step?: Expression;
  interpolate?: Expression;
} & BaseExpressionArgs;

export class WithFillExpr extends Expression {
  key = ExpressionKey.WITH_FILL;

  /**
   * Defines the arguments (properties and child expressions) for WithFill expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    from: false,
    to: false,
    step: false,
    interpolate: false,
  } satisfies RequiredMap<WithFillExprArgs>;

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

export type OrderedExprArgs = {
  desc?: Expression;
  nullsFirst: Expression;
  withFill?: Expression;
  this: Expression;
} & BaseExpressionArgs;

export class OrderedExpr extends Expression {
  key = ExpressionKey.ORDERED;

  /**
   * Defines the arguments (properties and child expressions) for Ordered expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    desc: false,
    nullsFirst: true,
    withFill: false,
  } satisfies RequiredMap<OrderedExprArgs>;

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

export type PropertyExprArgs = {
  value: string | Expression;
  this: Expression;
} & BaseExpressionArgs;

export class PropertyExpr extends Expression {
  key = ExpressionKey.PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for Property expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    value: true,
  } satisfies RequiredMap<PropertyExprArgs>;

  declare args: PropertyExprArgs;

  constructor (args: PropertyExprArgs | BaseExpressionArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $value (): string | Expression {
    return this.args.value;
  }
}

export type GrantPrivilegeExprArgs = {
  this: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class GrantPrivilegeExpr extends Expression {
  key = ExpressionKey.GRANT_PRIVILEGE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
  } satisfies RequiredMap<BaseExpressionArgs>;

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
export type GrantPrincipalExprArgs = {
  kind?: GrantPrincipalExprKind;
  this: Expression;
} & BaseExpressionArgs;

export class GrantPrincipalExpr extends Expression {
  key = ExpressionKey.GRANT_PRINCIPAL;

  /**
   * Defines the arguments (properties and child expressions) for GrantPrincipal expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    kind: false,
  } satisfies RequiredMap<GrantPrincipalExprArgs>;

  declare args: GrantPrincipalExprArgs;

  constructor (args: GrantPrincipalExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): string | undefined {
    return this.args.kind;
  }
}

export type AllowedValuesPropertyExprArgs = {
  expressions: Expression[];
} & BaseExpressionArgs;

export class AllowedValuesPropertyExpr extends Expression {
  key = ExpressionKey.ALLOWED_VALUES_PROPERTY;

  static argTypes = {
    expressions: true,
  } satisfies RequiredMap<AllowedValuesPropertyExprArgs>;

  declare args: AllowedValuesPropertyExprArgs;

  constructor (args: AllowedValuesPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type PartitionByRangePropertyDynamicExprArgs = {

  start: Expression;
  end: Expression;
  every: Expression;
  this?: Expression;
} & BaseExpressionArgs;

export class PartitionByRangePropertyDynamicExpr extends Expression {
  key = ExpressionKey.PARTITION_BY_RANGE_PROPERTY_DYNAMIC;

  /**
   * Defines the arguments (properties and child expressions) for PartitionByRangePropertyDynamic
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    start: true,
    end: true,
    every: true,
  } satisfies RequiredMap<PartitionByRangePropertyDynamicExprArgs>;

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

export type RollupIndexExprArgs = {
  fromIndex?: Expression;
  properties?: Expression[];
  this: Expression;
  expressions: Expression[];
} & BaseExpressionArgs;

export class RollupIndexExpr extends Expression {
  key = ExpressionKey.ROLLUP_INDEX;

  /**
   * Defines the arguments (properties and child expressions) for RollupIndex expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
    fromIndex: false,
    properties: false,
  } satisfies RequiredMap<RollupIndexExprArgs>;

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

export type PartitionListExprArgs = {
  this: Expression;
  expressions: Expression[];
} & BaseExpressionArgs;

export class PartitionListExpr extends Expression {
  key = ExpressionKey.PARTITION_LIST;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
  } satisfies RequiredMap<PartitionListExprArgs>;

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

export type PartitionBoundSpecExprArgs = {
  fromExpressions?: Expression[];
  toExpressions?: Expression[];
  this?: Expression;
  expression?: Expression;
} & BaseExpressionArgs;

export class PartitionBoundSpecExpr extends Expression {
  key = ExpressionKey.PARTITION_BOUND_SPEC;

  /**
   * Defines the arguments (properties and child expressions) for PartitionBoundSpec expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    expression: false,
    fromExpressions: false,
    toExpressions: false,
  } satisfies RequiredMap<PartitionBoundSpecExprArgs>;

  declare args: PartitionBoundSpecExprArgs;

  constructor (args: PartitionBoundSpecExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  get $fromExpressions (): Expression[] | undefined {
    return this.args.fromExpressions;
  }

  get $toExpressions (): Expression[] | undefined {
    return this.args.toExpressions;
  }
}

export type QueryTransformExprArgs = {
  commandScript: Expression;
  schema?: Expression;
  rowFormatBefore?: string;
  recordWriter?: Expression;
  rowFormatAfter?: string;
  recordReader?: Expression;
} & BaseExpressionArgs;

export class QueryTransformExpr extends Expression {
  key = ExpressionKey.QUERY_TRANSFORM;

  /**
   * Defines the arguments (properties and child expressions) for QueryTransform expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    commandScript: true,
    schema: false,
    rowFormatBefore: false,
    recordWriter: false,
    rowFormatAfter: false,
    recordReader: false,
  } satisfies RequiredMap<QueryTransformExprArgs>;

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

export type SemanticViewExprArgs = { metrics?: Expression[];
  dimensions?: Expression[];
  facts?: Expression[];
  where?: Expression; } & BaseExpressionArgs;

export class SemanticViewExpr extends Expression {
  key = ExpressionKey.SEMANTIC_VIEW;

  /**
   * Defines the arguments (properties and child expressions) for SemanticView expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    metrics: false,
    dimensions: false,
    facts: false,
    where: false,
  } satisfies RequiredMap<SemanticViewExprArgs>;

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

export type LocationExprArgs = BaseExpressionArgs;
export class LocationExpr extends Expression {
  key = ExpressionKey.LOCATION;
  static argTypes = {} satisfies RequiredMap<LocationExprArgs>;

  declare args: LocationExprArgs;
  constructor (args: LocationExprArgs) {
    super(args);
  }
}

export type QualifyExprArgs = BaseExpressionArgs;

export class QualifyExpr extends Expression {
  key = ExpressionKey.QUALIFY;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<QualifyExprArgs>;

  declare args: QualifyExprArgs;

  constructor (args: QualifyExprArgs) {
    super(args);
  }
}

export type InputOutputFormatExprArgs = {
  inputFormat?: string;
  outputFormat?: string;
} & BaseExpressionArgs;

export class InputOutputFormatExpr extends Expression {
  key = ExpressionKey.INPUT_OUTPUT_FORMAT;

  /**
   * Defines the arguments (properties and child expressions) for InputOutputFormat expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    inputFormat: false,
    outputFormat: false,
  } satisfies RequiredMap<InputOutputFormatExprArgs>;

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

export type ReturnExprArgs = BaseExpressionArgs;

export class ReturnExpr extends Expression {
  key = ExpressionKey.RETURN;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ReturnExprArgs>;

  declare args: ReturnExprArgs;

  constructor (args: ReturnExprArgs) {
    super(args);
  }
}

export type ReferenceExprArgs = {
  this: Expression;
  expressions: Expression[];
  options?: Expression[];
} & BaseExpressionArgs;

export class ReferenceExpr extends Expression {
  key = ExpressionKey.REFERENCE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
    options: false,
  } satisfies RequiredMap<ReferenceExprArgs>;

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

export type TupleExprArgs = {
  expressions?: Expression[];
} & BaseExpressionArgs;

export class TupleExpr extends Expression {
  key = ExpressionKey.TUPLE;

  static argTypes = {
    ...super.argTypes,
    expressions: false,
  } satisfies RequiredMap<TupleExprArgs>;

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
    options?: {
      unnest: Expression | string | (Expression | string)[];
      copy: boolean;
      [key: string]: unknown;
    },
  ): InExpr {
    const copy = options?.copy ?? true;
    return new InExpr({
      this: maybeCopy(this, copy),
      expressions: expressions.map((e) => convert(e, copy)),
      query: query
        ? maybeParse(query, {
          ...options,
          copy,
        })
        : undefined,
      unnest: options?.unnest
        ? new UnnestExpr({
          expressions: ensureList(options.unnest).map((e) => maybeParse(e, {
            ...options,
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
export type QueryOptionExprArgs = {
  this: Expression;
  expression?: Expression;
} & BaseExpressionArgs;

export class QueryOptionExpr extends Expression {
  key = ExpressionKey.QUERY_OPTION;

  static argTypes: Record<string, boolean> = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<QueryOptionExprArgs>;

  declare args: QueryOptionExprArgs;

  constructor (args: QueryOptionExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }
}

// https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-ver16
export type WithTableHintExprArgs = {
  expressions: Expression[];
} & BaseExpressionArgs;

export class WithTableHintExpr extends Expression {
  key = ExpressionKey.WITH_TABLE_HINT;

  static argTypes: Record<string, boolean> = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<WithTableHintExprArgs>;

  declare args: WithTableHintExprArgs;

  constructor (args: WithTableHintExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

// https://dev.mysql.com/doc/refman/8.0/en/index-hints.html
export type IndexTableHintExprArgs = {
  this: Expression;
  expressions?: Expression[];
  target?: Expression;
} & BaseExpressionArgs;

export class IndexTableHintExpr extends Expression {
  key = ExpressionKey.INDEX_TABLE_HINT;

  static argTypes: Record<string, boolean> = {
    ...super.argTypes,
    this: true,
    expressions: false,
    target: false,
  } satisfies RequiredMap<IndexTableHintExprArgs>;

  declare args: IndexTableHintExprArgs;

  constructor (args: IndexTableHintExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  get $target (): Expression | undefined {
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
export type HistoricalDataExprArgs = {
  this: Expression;
  kind: HistoricalDataExprKind;
  expression: Expression;
} & BaseExpressionArgs;

export class HistoricalDataExpr extends Expression {
  key = ExpressionKey.HISTORICAL_DATA;

  static argTypes: Record<string, boolean> = {
    ...super.argTypes,
    this: true,
    kind: true,
    expression: true,
  } satisfies RequiredMap<HistoricalDataExprArgs>;

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
export type PutExprArgs = {
  this: Expression;
  target: Expression;
  properties?: Expression[];
} & BaseExpressionArgs;

export class PutExpr extends Expression {
  key = ExpressionKey.PUT;

  static argTypes: Record<string, boolean> = {
    ...super.argTypes,
    this: true,
    target: true,
    properties: false,
  } satisfies RequiredMap<PutExprArgs>;

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
export type GetExprArgs = {
  this: Expression;
  target: Expression;
  properties?: Expression[];
} & BaseExpressionArgs;

export class GetExpr extends Expression {
  key = ExpressionKey.GET;

  static argTypes: Record<string, boolean> = {
    ...super.argTypes,
    this: true,
    target: true,
    properties: false,
  } satisfies RequiredMap<GetExprArgs>;

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

export type TableExprArgs = {
  db?: string | IdentifierExpr;
  catalog?: string | IdentifierExpr;
  laterals?: Expression[];
  joins?: Expression[];
  pivots?: Expression[];
  hints?: Expression[];
  systemTime?: Expression;
  version?: Expression;
  format?: string;
  pattern?: Expression;
  ordinality?: boolean;
  when?: Expression;
  only?: Expression;
  partition?: Expression;
  changes?: Expression[];
  rowsFrom?: number | Expression;
  sample?: number | Expression;
  indexed?: Expression;
} & BaseExpressionArgs;

export class TableExpr extends Expression {
  key = ExpressionKey.TABLE;

  /**
   * Defines the arguments (properties and child expressions) for Table expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   *
   * @see {@link https://docs.sqlglot.com/sqlglot/expressions.html#Table | SQLGlot Table Documentation}
   */
  static argTypes: Record<string, boolean> = {
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
  } satisfies RequiredMap<TableExprArgs>;

  declare args: TableExprArgs;

  constructor (args: TableExprArgs) {
    super(args);
  }

  get $this (): ExpressionValue {
    return this.args.this;
  }

  get $alias (): TableAliasExpr | IdentifierExpr | string | undefined {
    return this.args.alias;
  }

  get $db (): string | IdentifierExpr | undefined {
    return this.args.db;
  }

  get $catalog (): string | IdentifierExpr | undefined {
    return this.args.catalog;
  }

  get $laterals (): Expression[] | undefined {
    return this.args.laterals;
  }

  get $joins (): Expression[] | undefined {
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

  get $only (): Expression | undefined {
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
    if (thisArg instanceof IdentifierExpr || thisArg instanceof TableExpr) {
      return thisArg.name || '';
    }
    return '';
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
  get parts (): Expression[] {
    const parts: Expression[] = [];

    for (const arg of [
      'catalog',
      'db',
      'this',
    ] as const) {
      const part = this.args[arg];

      if (part instanceof DotExpr) {
        parts.push(...part.flatten());
      } else if (part instanceof Expression) {
        parts.push(part);
      }
    }

    return parts;
  }

  /**
   * Converts this table to a Column expression.
   */
  toColumn (copy: boolean = true): Expression {
    const parts = this.parts;
    const lastPart = parts[parts.length - 1];

    let col: Expression;
    if (lastPart instanceof IdentifierExpr) {
      // Build column from parts (reversed for catalog.db.table order)
      const columnParts = parts.slice(0, 4).reverse();
      const fields = parts.slice(4);
      col = column(...columnParts, fields, copy);
    } else {
      // If last part is a function or array wrapped in Table
      col = lastPart;
    }

    const aliasArg = this.args.alias;
    if (aliasArg) {
      const aliasName = typeof aliasArg === 'string'
        ? aliasArg
        : aliasArg instanceof TableAliasExpr || aliasArg instanceof IdentifierExpr
          ? aliasArg.this
          : aliasArg;
      col = alias(col, aliasName, { copy });
    }

    return col;
  }
}

export type VarExprArgs = BaseExpressionArgs;

export class VarExpr extends Expression {
  key = ExpressionKey.VAR;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<VarExprArgs>;

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
export type VersionExprArgs = {
  this: Expression;
  kind: VersionExprKind;
  expression?: Expression;
} & BaseExpressionArgs;

export class VersionExpr extends Expression {
  key = ExpressionKey.VERSION;

  static argTypes = {
    ...super.argTypes,
    this: true,
    kind: true,
    expression: false,
  } satisfies RequiredMap<VersionExprArgs>;

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

  get $expression (): Expression | undefined {
    return this.args.expression;
  }
}

export type SchemaExprArgs = {
  this?: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class SchemaExpr extends Expression {
  key = ExpressionKey.SCHEMA;

  static argTypes = {
    ...super.argTypes,
    this: false,
    expressions: false,
  } satisfies RequiredMap<SchemaExprArgs>;

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
export type LockExprArgs = {
  update: Expression;
  expressions?: Expression[];
  wait?: Expression;
  key?: Expression;
} & BaseExpressionArgs;

export class LockExpr extends Expression {
  key = ExpressionKey.LOCK;

  static argTypes = {
    ...super.argTypes,
    update: true,
    expressions: false,
    wait: false,
    key: false,
  } satisfies RequiredMap<LockExprArgs>;

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

export type TableSampleExprArgs = {
  expressions?: Expression[];
  method?: string;
  bucketNumerator?: Expression;
  bucketDenominator?: Expression;
  bucketField?: Expression;
  percent?: Expression;
  rows?: Expression[];
  size?: number | Expression;
  seed?: Expression;
} & BaseExpressionArgs;

export class TableSampleExpr extends Expression {
  key = ExpressionKey.TABLE_SAMPLE;

  /**
   * Defines the arguments (properties and child expressions) for TableSample expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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
  } satisfies RequiredMap<TableSampleExprArgs>;

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

export type TagExprArgs = {
  this?: Expression;
  prefix?: Expression;
  postfix?: Expression;
} & BaseExpressionArgs;

export class TagExpr extends Expression {
  key = ExpressionKey.TAG;

  /**
   * Defines the arguments (properties and child expressions) for Tag expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    prefix: false,
    postfix: false,
  } satisfies RequiredMap<TagExprArgs>;

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

export type PivotExprArgs = {
  fields?: Expression[];
  unpivot?: boolean;
  using?: string;
  group?: Expression;
  columns?: Expression[];
  includeNulls?: Expression[];
  defaultOnNull?: Expression;
  into?: Expression;
  with?: WithExpr;
} & BaseExpressionArgs;

export class PivotExpr extends Expression {
  key = ExpressionKey.PIVOT;

  /**
   * Defines the arguments (properties and child expressions) for Pivot expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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
  } satisfies RequiredMap<PivotExprArgs>;

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

export type UnpivotColumnsExprArgs = {
  this: Expression;
  expressions: Expression[];
} & BaseExpressionArgs;

export class UnpivotColumnsExpr extends Expression {
  key = ExpressionKey.UNPIVOT_COLUMNS;
  static argTypes = {
    this: true,
    expressions: true,
  } satisfies RequiredMap<UnpivotColumnsExprArgs>;

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

export type WindowSpecExprArgs = {
  kind?: WindowSpecExprKind;
  start?: Expression;
  startSide?: Expression;
  end?: Expression;
  endSide?: Expression;
  exclude?: Expression;
} & BaseExpressionArgs;

export class WindowSpecExpr extends Expression {
  key = ExpressionKey.WINDOW_SPEC;

  /**
   * Defines the arguments (properties and child expressions) for WindowSpec expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    kind: false,
    start: false,
    startSide: false,
    end: false,
    endSide: false,
    exclude: false,
  } satisfies RequiredMap<WindowSpecExprArgs>;

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

export type PreWhereExprArgs = BaseExpressionArgs;

export class PreWhereExpr extends Expression {
  key = ExpressionKey.PRE_WHERE;
  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<PreWhereExprArgs>;

  declare args: PreWhereExprArgs;

  constructor (args: PreWhereExprArgs) {
    super(args);
  }
}

export type WhereExprArgs = BaseExpressionArgs;

export class WhereExpr extends Expression {
  key = ExpressionKey.WHERE;
  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<WhereExprArgs>;

  declare args: WhereExprArgs;

  constructor (args: WhereExprArgs) {
    super(args);
  }
}

export type StarExprArgs = { except?: Expression;
  replace?: boolean;
  rename?: string; } & BaseExpressionArgs;

export class StarExpr extends Expression {
  key = ExpressionKey.STAR;

  /**
   * Defines the arguments (properties and child expressions) for Star expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    except: false,
    replace: false,
    rename: false,
  } satisfies RequiredMap<StarExprArgs>;

  declare args: StarExprArgs;

  constructor (args: StarExprArgs) {
    super(args);
  }

  get $except (): Expression | undefined {
    return this.args.except;
  }

  get $replace (): Expression | undefined {
    return this.args.replace;
  }

  get $rename (): Expression | undefined {
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

export type DataTypeParamExprArgs = {
  this: Expression;
  expression?: Expression;
} & BaseExpressionArgs;

export class DataTypeParamExpr extends Expression {
  key = ExpressionKey.DATA_TYPE_PARAM;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<DataTypeParamExprArgs>;

  declare args: DataTypeParamExprArgs;

  constructor (args: DataTypeParamExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
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

export type DataTypeExprArgs = {
  this: DataTypeExprKind;
  expressions?: Expression[];
  nested?: Expression;
  values?: Expression[];
  prefix?: Expression;
  kind?: DataTypeExprKind;
  nullable?: Expression;
} & BaseExpressionArgs;

export class DataTypeExpr extends Expression {
  key = ExpressionKey.DATA_TYPE;

  /**
   * Defines the arguments (properties and child expressions) for DataType expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
    nested: false,
    values: false,
    prefix: false,
    kind: false,
    nullable: false,
  } satisfies RequiredMap<DataTypeExprArgs>;

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
      dialect?: string;
      udt?: boolean;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): DataTypeExpr {
    const {
      udt = false, copy = true, dialect, ...kwargs
    } = options;

    let dataTypeExp;

    if (typeof dtype === 'string') {
      if (dtype === DataTypeExprKind.UNKNOWN) {
        return new DataTypeExpr({
          ...kwargs,
          this: DataTypeExprKind.UNKNOWN,
        });
      }

      try {
        dataTypeExp = parseOne(dtype, {
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
    dtypes: Array<DataTypeExprKind | DataTypeExpr | IdentifierExpr | DotExpr>,
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

  get $nested (): Expression | undefined {
    return this.args.nested;
  }

  get $values (): Expression[] | undefined {
    return this.args.values;
  }

  get $prefix (): Expression | undefined {
    return this.args.prefix;
  }

  get $kind (): DataTypeExprKind | undefined {
    return this.args.kind;
  }

  get $nullable (): Expression | undefined {
    return this.args.nullable;
  }
}

export type TypeExprArgs = BaseExpressionArgs;
export class TypeExpr extends Expression {
  key = ExpressionKey.TYPE;
  static argTypes = {} satisfies RequiredMap<TypeExprArgs>;

  declare args: TypeExprArgs;
  constructor (args: TypeExprArgs) {
    super(args);
  }
}

export type CommandExprArgs = {
  this: Expression;
  expression?: Expression;
} & BaseExpressionArgs;

export class CommandExpr extends Expression {
  key = ExpressionKey.COMMAND;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<CommandExprArgs>;

  declare args: CommandExprArgs;

  constructor (args: CommandExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }
}

export type TransactionExprArgs = {
  this?: Expression;
  modes?: Expression[];
  mark?: Expression;
} & BaseExpressionArgs;

export class TransactionExpr extends Expression {
  key = ExpressionKey.TRANSACTION;

  static argTypes = {
    ...super.argTypes,
    this: false,
    modes: false,
    mark: false,
  } satisfies RequiredMap<TransactionExprArgs>;

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

export type CommitExprArgs = {
  chain?: Expression;
  this?: Expression;
  durability?: Expression;
} & BaseExpressionArgs;

export class CommitExpr extends Expression {
  key = ExpressionKey.COMMIT;

  static argTypes = {
    ...super.argTypes,
    chain: false,
    this: false,
    durability: false,
  } satisfies RequiredMap<CommitExprArgs>;

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

export type RollbackExprArgs = {
  savepoint?: Expression;
  this?: Expression;
} & BaseExpressionArgs;

export class RollbackExpr extends Expression {
  key = ExpressionKey.ROLLBACK;

  static argTypes = {
    ...super.argTypes,
    savepoint: false,
    this: false,
  } satisfies RequiredMap<RollbackExprArgs>;

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

export type AlterExprArgs = {
  this?: Expression;
  kind: AlterExprKind;
  actions: Expression[];
  exists?: Expression;
  only?: Expression;
  options?: Expression[];
  cluster?: Expression;
  notValid?: Expression;
  check?: Expression;
  cascade?: Expression;
} & BaseExpressionArgs;

export class AlterExpr extends Expression {
  key = ExpressionKey.ALTER;

  /**
   * Defines the arguments (properties and child expressions) for Alter expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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
  } satisfies RequiredMap<AlterExprArgs>;

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

  get $exists (): Expression | undefined {
    return this.args.exists;
  }

  get $only (): Expression | undefined {
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

export type AlterSessionExprArgs = {
  expressions: Expression[];
  unset?: Expression;
} & BaseExpressionArgs;

export class AlterSessionExpr extends Expression {
  key = ExpressionKey.ALTER_SESSION;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
    unset: false,
  } satisfies RequiredMap<AlterSessionExprArgs>;

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

export type AnalyzeExprArgs = {
  kind?: AnalyzeExprKind;
  this?: Expression;
  options?: Expression[];
  mode?: Expression;
  partition?: Expression;
  expression?: Expression;
  properties?: Expression[];
} & BaseExpressionArgs;

export class AnalyzeExpr extends Expression {
  key = ExpressionKey.ANALYZE;

  /**
   * Defines the arguments (properties and child expressions) for Analyze expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    kind: false,
    this: false,
    options: false,
    mode: false,
    partition: false,
    expression: false,
    properties: false,
  } satisfies RequiredMap<AnalyzeExprArgs>;

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

  get $expression (): Expression | undefined {
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

export type AnalyzeStatisticsExprArgs = {
  kind: AnalyzeStatisticsExprKind;
  option?: Expression;
  this?: Expression;
  expressions?: Expression[];
} & BaseExpressionArgs;

export class AnalyzeStatisticsExpr extends Expression {
  key = ExpressionKey.ANALYZE_STATISTICS;

  /**
   * Defines the arguments (properties and child expressions) for AnalyzeStatistics expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    kind: true,
    option: false,
    this: false,
    expressions: false,
  } satisfies RequiredMap<AnalyzeStatisticsExprArgs>;

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

export type AnalyzeHistogramExprArgs = {
  this: Expression;
  expressions: Expression[];
  expression?: Expression;
  updateOptions?: Expression[];
} & BaseExpressionArgs;

export class AnalyzeHistogramExpr extends Expression {
  key = ExpressionKey.ANALYZE_HISTOGRAM;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
    expression: false,
    updateOptions: false,
  } satisfies RequiredMap<AnalyzeHistogramExprArgs>;

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

  get $expression (): Expression | undefined {
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

export type AnalyzeSampleExprArgs = { kind: AnalyzeSampleExprKind;
  sample: number | Expression; } & BaseExpressionArgs;

export class AnalyzeSampleExpr extends Expression {
  key = ExpressionKey.ANALYZE_SAMPLE;

  /**
   * Defines the arguments (properties and child expressions) for AnalyzeSample expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    kind: true,
    sample: true,
  } satisfies RequiredMap<AnalyzeSampleExprArgs>;

  declare args: AnalyzeSampleExprArgs;

  constructor (args: AnalyzeSampleExprArgs) {
    super(args);
  }

  get $kind (): string {
    return this.args.kind;
  }

  get $sample (): number | Expression {
    return this.args.sample;
  }
}

export type AnalyzeListChainedRowsExprArgs = {
  expression?: Expression;
} & BaseExpressionArgs;

export class AnalyzeListChainedRowsExpr extends Expression {
  key = ExpressionKey.ANALYZE_LIST_CHAINED_ROWS;

  static argTypes = {
    ...super.argTypes,
    expression: false,
  } satisfies RequiredMap<AnalyzeListChainedRowsExprArgs>;

  declare args: AnalyzeListChainedRowsExprArgs;

  constructor (args: AnalyzeListChainedRowsExprArgs) {
    super(args);
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }
}

/**
 * Valid kind values for ANALYZE DELETE statements
 */
export enum AnalyzeDeleteExprKind {
  STATISTICS = 'STATISTICS',
}
export type AnalyzeDeleteExprArgs = { kind?: AnalyzeDeleteExprKind } & BaseExpressionArgs;

export class AnalyzeDeleteExpr extends Expression {
  key = ExpressionKey.ANALYZE_DELETE;

  static argTypes = {
    ...super.argTypes,
    kind: false,
  } satisfies RequiredMap<AnalyzeDeleteExprArgs>;

  declare args: AnalyzeDeleteExprArgs;

  constructor (args: AnalyzeDeleteExprArgs) {
    super(args);
  }

  get $kind (): AnalyzeDeleteExprKind | undefined {
    return this.args.kind;
  }
}

export type AnalyzeWithExprArgs = {
  expressions: Expression[];
} & BaseExpressionArgs;

export class AnalyzeWithExpr extends Expression {
  key = ExpressionKey.ANALYZE_WITH;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<AnalyzeWithExprArgs>;

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

export type AnalyzeValidateExprArgs = {
  kind: AnalyzeValidateExprKind;
  this?: Expression;
  expression?: Expression;
} & BaseExpressionArgs;

export class AnalyzeValidateExpr extends Expression {
  key = ExpressionKey.ANALYZE_VALIDATE;

  static argTypes = {
    ...super.argTypes,
    kind: true,
    this: false,
    expression: false,
  } satisfies RequiredMap<AnalyzeValidateExprArgs>;

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

  get $expression (): Expression | undefined {
    return this.args.expression;
  }
}

export type AnalyzeColumnsExprArgs = BaseExpressionArgs;

export class AnalyzeColumnsExpr extends Expression {
  key = ExpressionKey.ANALYZE_COLUMNS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AnalyzeColumnsExprArgs>;

  declare args: AnalyzeColumnsExprArgs;
  constructor (args: AnalyzeColumnsExprArgs) {
    super(args);
  }
}

export type UsingDataExprArgs = BaseExpressionArgs;

export class UsingDataExpr extends Expression {
  key = ExpressionKey.USING_DATA;
  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<UsingDataExprArgs>;

  declare args: UsingDataExprArgs;

  constructor (args: UsingDataExprArgs) {
    super(args);
  }
}

export type AddConstraintExprArgs = {
  expressions: Expression[];
} & BaseExpressionArgs;

export class AddConstraintExpr extends Expression {
  key = ExpressionKey.ADD_CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<AddConstraintExprArgs>;

  declare args: AddConstraintExprArgs;

  constructor (args: AddConstraintExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type AddPartitionExprArgs = {
  this: Expression;
  exists?: Expression;
  location?: Expression;
} & BaseExpressionArgs;

export class AddPartitionExpr extends Expression {
  key = ExpressionKey.ADD_PARTITION;

  static argTypes = {
    ...super.argTypes,
    this: true,
    exists: false,
    location: false,
  } satisfies RequiredMap<AddPartitionExprArgs>;

  declare args: AddPartitionExprArgs;

  constructor (args: AddPartitionExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $exists (): Expression | undefined {
    return this.args.exists;
  }

  get $location (): Expression | undefined {
    return this.args.location;
  }
}

export type AttachOptionExprArgs = {
  this: Expression;
  expression?: Expression;
} & BaseExpressionArgs;

export class AttachOptionExpr extends Expression {
  key = ExpressionKey.ATTACH_OPTION;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<AttachOptionExprArgs>;

  declare args: AttachOptionExprArgs;

  constructor (args: AttachOptionExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }
}

export type DropPartitionExprArgs = {
  expressions: Expression[];
  exists?: Expression;
} & BaseExpressionArgs;

export class DropPartitionExpr extends Expression {
  key = ExpressionKey.DROP_PARTITION;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
    exists: false,
  } satisfies RequiredMap<DropPartitionExprArgs>;

  declare args: DropPartitionExprArgs;

  constructor (args: DropPartitionExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }

  get $exists (): Expression | undefined {
    return this.args.exists;
  }
}

export type ReplacePartitionExprArgs = {
  expression: Expression;
  source: Expression;
} & BaseExpressionArgs;

export class ReplacePartitionExpr extends Expression {
  key = ExpressionKey.REPLACE_PARTITION;

  static argTypes = {
    ...super.argTypes,
    expression: true,
    source: true,
  } satisfies RequiredMap<ReplacePartitionExprArgs>;

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

export type AliasExprArgs = {
  this: Expression;
  alias?: string | IdentifierExpr;
} & BaseExpressionArgs;

export class AliasExpr extends Expression {
  key = ExpressionKey.ALIAS;

  static argTypes = {
    ...super.argTypes,
    this: true,
    alias: false,
  } satisfies RequiredMap<AliasExprArgs>;

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

export type PivotAnyExprArgs = {
  this?: Expression;
} & BaseExpressionArgs;

/**
 * Represents Snowflake's ANY [ ORDER BY ... ] syntax
 * https://docs.snowflake.com/en/sql-reference/constructs/pivot
 */
export class PivotAnyExpr extends Expression {
  key = ExpressionKey.PIVOT_ANY;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<PivotAnyExprArgs>;

  declare args: PivotAnyExprArgs;

  constructor (args: PivotAnyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type AliasesExprArgs = {
  this: Expression;
  expressions: Expression[];
} & BaseExpressionArgs;

export class AliasesExpr extends Expression {
  key = ExpressionKey.ALIASES;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
  } satisfies RequiredMap<AliasesExprArgs>;

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

export type AtIndexExprArgs = {
  this: Expression;
  expression: Expression;
} & BaseExpressionArgs;

/**
 * https://docs.aws.amazon.com/redshift/latest/dg/query-super.html
 */
export class AtIndexExpr extends Expression {
  key = ExpressionKey.AT_INDEX;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<AtIndexExprArgs>;

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

export type AtTimeZoneExprArgs = {
  this: Expression;
  zone: Expression;
} & BaseExpressionArgs;

export class AtTimeZoneExpr extends Expression {
  key = ExpressionKey.AT_TIME_ZONE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    zone: true,
  } satisfies RequiredMap<AtTimeZoneExprArgs>;

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

export type FromTimeZoneExprArgs = {
  this: Expression;
  zone: Expression;
} & BaseExpressionArgs;

export class FromTimeZoneExpr extends Expression {
  key = ExpressionKey.FROM_TIME_ZONE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    zone: true,
  } satisfies RequiredMap<FromTimeZoneExprArgs>;

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

export type FormatPhraseExprArgs = {
  this: Expression;
  format: Expression;
} & BaseExpressionArgs;

/**
 * Format override for a column in Teradata.
 * Can be expanded to additional dialects as needed.
 *
 * https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Types-and-Literals/Data-Type-Formats-and-Format-Phrases/FORMAT
 */
export class FormatPhraseExpr extends Expression {
  key = ExpressionKey.FORMAT_PHRASE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    format: true,
  } satisfies RequiredMap<FormatPhraseExprArgs>;

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

export type DistinctExprArgs = {
  expressions?: Expression[];
  on?: Expression;
} & BaseExpressionArgs;

export class DistinctExpr extends Expression {
  key = ExpressionKey.DISTINCT;

  static argTypes = {
    ...super.argTypes,
    expressions: false,
    on: false,
  } satisfies RequiredMap<DistinctExprArgs>;

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

export type ForInExprArgs = {
  this: Expression;
  expression: Expression;
} & BaseExpressionArgs;

/**
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#for-in
 */
export class ForInExpr extends Expression {
  key = ExpressionKey.FOR_IN;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<ForInExprArgs>;

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

export type TimeUnitExprArgs = { unit?: VarExpr | IntervalSpanExpr } & BaseExpressionArgs;

/**
 * Automatically converts unit arg into a var.
 */
export class TimeUnitExpr extends Expression {
  key = ExpressionKey.TIME_UNIT;

  static argTypes = {
    ...super.argTypes,
    unit: false,
  } satisfies RequiredMap<TimeUnitExprArgs>;

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
        unit.set('this', new VarExpr({ this: thisArg.name.toUpperCase() }));
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

export type IgnoreNullsExprArgs = BaseExpressionArgs;

export class IgnoreNullsExpr extends Expression {
  key = ExpressionKey.IGNORE_NULLS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<IgnoreNullsExprArgs>;

  declare args: IgnoreNullsExprArgs;

  constructor (args: IgnoreNullsExprArgs) {
    super(args);
  }
}

export type RespectNullsExprArgs = BaseExpressionArgs;

export class RespectNullsExpr extends Expression {
  key = ExpressionKey.RESPECT_NULLS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<RespectNullsExprArgs>;

  declare args: RespectNullsExprArgs;

  constructor (args: RespectNullsExprArgs) {
    super(args);
  }
}

/**
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate-function-calls#max_min_clause
 */
export type HavingMaxExprArgs = {
  this: Expression;
  expression: Expression;
  max: Expression;
} & BaseExpressionArgs;

export class HavingMaxExpr extends Expression {
  key = ExpressionKey.HAVING_MAX;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    max: true,
  } satisfies RequiredMap<HavingMaxExprArgs>;

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

export type TranslateCharactersExprArgs = {
  this: Expression;
  expression: Expression;
  withError?: Expression;
} & BaseExpressionArgs;

export class TranslateCharactersExpr extends Expression {
  key = ExpressionKey.TRANSLATE_CHARACTERS;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    withError: false,
  } satisfies RequiredMap<TranslateCharactersExprArgs>;

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

export type PositionalColumnExprArgs = BaseExpressionArgs;
export class PositionalColumnExpr extends Expression {
  key = ExpressionKey.POSITIONAL_COLUMN;
  static argTypes = {} satisfies RequiredMap<PositionalColumnExprArgs>;

  declare args: PositionalColumnExprArgs;
  constructor (args: PositionalColumnExprArgs) {
    super(args);
  }
}

export type OverflowTruncateBehaviorExprArgs = {
  this?: Expression;
  withCount: Expression;
} & BaseExpressionArgs;

export class OverflowTruncateBehaviorExpr extends Expression {
  key = ExpressionKey.OVERFLOW_TRUNCATE_BEHAVIOR;

  static argTypes = {
    this: false,
    withCount: true,
  } satisfies RequiredMap<OverflowTruncateBehaviorExprArgs>;

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

export type JSONExprArgs = {
  this?: Expression;
  with?: Expression;
  unique?: boolean;
} & BaseExpressionArgs;

export class JSONExpr extends Expression {
  key = ExpressionKey.JSON;

  /**
   * Defines the arguments (properties and child expressions) for JSON expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    with: false,
    unique: false,
  } satisfies RequiredMap<JSONExprArgs>;

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

  get $unique (): Expression | undefined {
    return this.args.unique;
  }
}

export type JSONPathExprArgs = {
  expressions: Expression[];
  escape?: Expression;
} & BaseExpressionArgs;

export class JSONPathExpr extends Expression {
  key = ExpressionKey.JSON_PATH;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
    escape: false,
  } satisfies RequiredMap<JSONPathExprArgs>;

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

export type JSONPathPartExprArgs = BaseExpressionArgs;
export class JSONPathPartExpr extends Expression {
  key = ExpressionKey.JSON_PATH_PART;
  static argTypes = {} satisfies RequiredMap<JSONPathPartExprArgs>;

  declare args: JSONPathPartExprArgs;
  constructor (args: JSONPathPartExprArgs) {
    super(args);
  }
}

export type FormatJsonExprArgs = BaseExpressionArgs;
export class FormatJsonExpr extends Expression {
  key = ExpressionKey.FORMAT_JSON;
  static argTypes = {} satisfies RequiredMap<FormatJsonExprArgs>;

  declare args: FormatJsonExprArgs;
  constructor (args: FormatJsonExprArgs) {
    super(args);
  }
}

export type JSONKeyValueExprArgs = {
  this: Expression;
  expression: Expression;
} & BaseExpressionArgs;

export class JSONKeyValueExpr extends Expression {
  key = ExpressionKey.JSON_KEY_VALUE;

  static argTypes = {
    this: true,
    expression: true,
  } satisfies RequiredMap<JSONKeyValueExprArgs>;

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
export type JSONColumnDefExprArgs = { kind?: JSONColumnDefExprKind;
  path?: Expression;
  nestedSchema?: Expression;
  ordinality?: boolean; } & BaseExpressionArgs;

export class JSONColumnDefExpr extends Expression {
  key = ExpressionKey.JSON_COLUMN_DEF;

  /**
   * Defines the arguments (properties and child expressions) for JSONColumnDef expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    kind: false,
    path: false,
    nestedSchema: false,
    ordinality: false,
  } satisfies RequiredMap<JSONColumnDefExprArgs>;

  declare args: JSONColumnDefExprArgs;

  constructor (args: JSONColumnDefExprArgs) {
    super(args);
  }

  get $kind (): string | undefined {
    return this.args.kind;
  }

  get $path (): Expression | undefined {
    return this.args.path;
  }

  get $nestedSchema (): Expression | undefined {
    return this.args.nestedSchema;
  }

  get $ordinality (): Expression | undefined {
    return this.args.ordinality;
  }
}

export type JSONSchemaExprArgs = {
  expressions: Expression[];
} & BaseExpressionArgs;

export class JSONSchemaExpr extends Expression {
  key = ExpressionKey.JSON_SCHEMA;

  static argTypes = {
    expressions: true,
  } satisfies RequiredMap<JSONSchemaExprArgs>;

  declare args: JSONSchemaExprArgs;

  constructor (args: JSONSchemaExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type JSONValueExprArgs = { path: Expression;
  returning?: Expression;
  onCondition?: Expression; } & BaseExpressionArgs;

export class JSONValueExpr extends Expression {
  key = ExpressionKey.JSON_VALUE;

  /**
   * Defines the arguments (properties and child expressions) for JSONValue expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    path: true,
    returning: false,
    onCondition: false,
  } satisfies RequiredMap<JSONValueExprArgs>;

  declare args: JSONValueExprArgs;

  constructor (args: JSONValueExprArgs) {
    super(args);
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

export type OpenJSONColumnDefExprArgs = {
  this: Expression;
  kind: OpenJSONColumnDefExprKind;
  path?: Expression;
  asJson?: Expression;
} & BaseExpressionArgs;

export class OpenJSONColumnDefExpr extends Expression {
  key = ExpressionKey.OPEN_JSON_COLUMN_DEF;

  /**
   * Defines the arguments (properties and child expressions) for OpenJSONColumnDef expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    kind: true,
    path: false,
    asJson: false,
  } satisfies RequiredMap<OpenJSONColumnDefExprArgs>;

  declare args: OpenJSONColumnDefExprArgs;

  constructor (args: OpenJSONColumnDefExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): string {
    return this.args.kind;
  }

  get $path (): Expression | undefined {
    return this.args.path;
  }

  get $asJson (): Expression | undefined {
    return this.args.asJson;
  }
}

export type JSONExtractQuoteExprArgs = { option: Expression;
  scalar?: Expression; } & BaseExpressionArgs;

export class JSONExtractQuoteExpr extends Expression {
  key = ExpressionKey.JSON_EXTRACT_QUOTE;

  /**
   * Defines the arguments (properties and child expressions) for JSONExtractQuote expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    option: true,
    scalar: false,
  } satisfies RequiredMap<JSONExtractQuoteExprArgs>;

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

export type ScopeResolutionExprArgs = BaseExpressionArgs;
export class ScopeResolutionExpr extends Expression {
  key = ExpressionKey.SCOPE_RESOLUTION;
  static argTypes = {} satisfies RequiredMap<ScopeResolutionExprArgs>;

  declare args: ScopeResolutionExprArgs;
  constructor (args: ScopeResolutionExprArgs) {
    super(args);
  }
}

export type SliceExprArgs = { step?: Expression } & BaseExpressionArgs;

export class SliceExpr extends Expression {
  key = ExpressionKey.SLICE;

  static argTypes = { step: false } satisfies RequiredMap<SliceExprArgs>;

  declare args: SliceExprArgs;

  constructor (args: SliceExprArgs) {
    super(args);
  }

  get $step (): Expression | undefined {
    return this.args.step;
  }
}

export type StreamExprArgs = BaseExpressionArgs;
export class StreamExpr extends Expression {
  key = ExpressionKey.STREAM;
  static argTypes = {} satisfies RequiredMap<StreamExprArgs>;

  declare args: StreamExprArgs;
  constructor (args: StreamExprArgs) {
    super(args);
  }
}

export type ModelAttributeExprArgs = BaseExpressionArgs;
export class ModelAttributeExpr extends Expression {
  key = ExpressionKey.MODEL_ATTRIBUTE;
  static argTypes = {} satisfies RequiredMap<ModelAttributeExprArgs>;

  declare args: ModelAttributeExprArgs;
  constructor (args: ModelAttributeExprArgs) {
    super(args);
  }
}

export type WeekStartExprArgs = BaseExpressionArgs;
export class WeekStartExpr extends Expression {
  key = ExpressionKey.WEEK_START;
  static argTypes = {} satisfies RequiredMap<WeekStartExprArgs>;

  declare args: WeekStartExprArgs;
  constructor (args: WeekStartExprArgs) {
    super(args);
  }
}

export type XMLNamespaceExprArgs = BaseExpressionArgs;
export class XMLNamespaceExpr extends Expression {
  key = ExpressionKey.XML_NAMESPACE;
  static argTypes = {} satisfies RequiredMap<XMLNamespaceExprArgs>;

  declare args: XMLNamespaceExprArgs;
  constructor (args: XMLNamespaceExprArgs) {
    super(args);
  }
}

export type XMLKeyValueOptionExprArgs = BaseExpressionArgs;
export class XMLKeyValueOptionExpr extends Expression {
  key = ExpressionKey.XML_KEY_VALUE_OPTION;
  static argTypes = {} satisfies RequiredMap<XMLKeyValueOptionExprArgs>;

  declare args: XMLKeyValueOptionExprArgs;
  constructor (args: XMLKeyValueOptionExprArgs) {
    super(args);
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
export type UseExprArgs = { kind?: UseExprKind } & BaseExpressionArgs;

export class UseExpr extends Expression {
  key = ExpressionKey.USE;

  static argTypes = { kind: false } satisfies RequiredMap<UseExprArgs>;

  declare args: UseExprArgs;

  constructor (args: UseExprArgs) {
    super(args);
  }

  get $kind (): string | undefined {
    return this.args.kind;
  }
}

export type WhenExprArgs = { matched: Expression;
  source?: Expression;
  condition?: Expression;
  then: Expression; } & BaseExpressionArgs;

export class WhenExpr extends Expression {
  key = ExpressionKey.WHEN;

  /**
   * Defines the arguments (properties and child expressions) for When expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    matched: true,
    source: false,
    condition: false,
    then: true,
  } satisfies RequiredMap<WhenExprArgs>;

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

export type WhensExprArgs = BaseExpressionArgs;
export class WhensExpr extends Expression {
  key = ExpressionKey.WHENS;
  static argTypes = {} satisfies RequiredMap<WhensExprArgs>;

  declare args: WhensExprArgs;
  constructor (args: WhensExprArgs) {
    super(args);
  }
}

export type SemicolonExprArgs = BaseExpressionArgs;
export class SemicolonExpr extends Expression {
  key = ExpressionKey.SEMICOLON;
  static argTypes = {} satisfies RequiredMap<SemicolonExprArgs>;

  declare args: SemicolonExprArgs;
  constructor (args: SemicolonExprArgs) {
    super(args);
  }
}

export type TableColumnExprArgs = BaseExpressionArgs;
export class TableColumnExpr extends Expression {
  key = ExpressionKey.TABLE_COLUMN;
  static argTypes = {} satisfies RequiredMap<TableColumnExprArgs>;

  declare args: TableColumnExprArgs;
  constructor (args: TableColumnExprArgs) {
    super(args);
  }
}

export type VariadicExprArgs = BaseExpressionArgs;

export class VariadicExpr extends Expression {
  key = ExpressionKey.VARIADIC;

  static argTypes = {} satisfies RequiredMap<VariadicExprArgs>;

  declare args: VariadicExprArgs;
  constructor (args: VariadicExprArgs) {
    super(args);
  }
}

export type CTEExprArgs = {
  scalar?: boolean;
  materialized?: boolean;
  keyExpressions?: Expression[];
  alias: Expression[];
  this: Expression;
} & BaseExpressionArgs;

export class CTEExpr extends DerivedTableExpr {
  key = ExpressionKey.CTE;

  /**
   * Defines the arguments (properties and child expressions) for CTE expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    alias: true,
    scalar: false,
    materialized: false,
    keyExpressions: false,
  } satisfies RequiredMap<CTEExprArgs>;

  declare args: CTEExprArgs;
  constructor (args: CTEExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $alias (): Expression[] {
    return this.args.alias;
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

export type BitStringExprArgs = ConditionExprArgs;

export class BitStringExpr extends ConditionExpr {
  key = ExpressionKey.BIT_STRING;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<BitStringExprArgs>;

  declare args: BitStringExprArgs;
  constructor (args: BitStringExprArgs) {
    super(args);
  }
}

export type HexStringExprArgs = {
  isInteger?: boolean;
  this: Expression;
} & BaseExpressionArgs;

export class HexStringExpr extends ConditionExpr {
  key = ExpressionKey.HEX_STRING;

  /**
   * Defines the arguments (properties and child expressions) for HexString expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    isInteger: false,
  } satisfies RequiredMap<HexStringExprArgs>;

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

export type ByteStringExprArgs = {
  isBytes?: boolean;
  this: Expression;
} & ConditionExprArgs;

export class ByteStringExpr extends ConditionExpr {
  key = ExpressionKey.BYTE_STRING;

  /**
   * Defines the arguments (properties and child expressions) for ByteString expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    isBytes: false,
  } satisfies RequiredMap<ByteStringExprArgs>;

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

export type RawStringExprArgs = ConditionExprArgs;

export class RawStringExpr extends ConditionExpr {
  key = ExpressionKey.RAW_STRING;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<RawStringExprArgs>;

  declare args: RawStringExprArgs;
  constructor (args: RawStringExprArgs) {
    super(args);
  }
}

export type UnicodeStringExprArgs = {
  escape?: Expression;
  this: Expression;
} & ConditionExprArgs;

export class UnicodeStringExpr extends ConditionExpr {
  key = ExpressionKey.UNICODE_STRING;

  /**
   * Defines the arguments (properties and child expressions) for UnicodeString expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    escape: false,
  } satisfies RequiredMap<UnicodeStringExprArgs>;

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
export type ColumnExprArgs = {
  table?: IdentifierExpr;
  db?: IdentifierExpr;
  catalog?: IdentifierExpr;
  this: IdentifierExpr;
  joinMark?: Expression;
} & ConditionExprArgs;

export class ColumnExpr extends ConditionExpr {
  key = ExpressionKey.COLUMN;

  /**
   * Defines the arguments (properties and child expressions) for Column expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    table: false,
    db: false,
    catalog: false,
    joinMark: false,
  } satisfies RequiredMap<ColumnExprArgs>;

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
  get parts (): IdentifierExpr[] {
    const result: IdentifierExpr[] = [];
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
    return result;
  }

  toDot () {
    // TODO
  }

  get $this (): IdentifierExpr {
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

export type PseudocolumnExprArgs = ColumnExprArgs;

export class PseudocolumnExpr extends ColumnExpr {
  key = ExpressionKey.PSEUDOCOLUMN;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<PseudocolumnExprArgs>;

  declare args: PseudocolumnExprArgs;
  constructor (args: PseudocolumnExprArgs) {
    super(args);
  }

  get $this (): IdentifierExpr {
    return this.args.this;
  }
}

export type AutoIncrementColumnConstraintExprArgs = ColumnConstraintKindExprArgs;

export class AutoIncrementColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.AUTO_INCREMENT_COLUMN_CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AutoIncrementColumnConstraintExprArgs>;

  declare args: AutoIncrementColumnConstraintExprArgs;
  constructor (args: AutoIncrementColumnConstraintExprArgs) {
    super(args);
  }
}

export type ZeroFillColumnConstraintExprArgs = ColumnConstraintExprArgs;

export class ZeroFillColumnConstraintExpr extends ColumnConstraintExpr {
  key = ExpressionKey.ZERO_FILL_COLUMN_CONSTRAINT;

  static argTypes = {
    kind: true, // sqlglot does not have this, but i thought it was a mistake
  } satisfies RequiredMap<
    ZeroFillColumnConstraintExprArgs
  >;

  declare args: ZeroFillColumnConstraintExprArgs;

  constructor (args: ZeroFillColumnConstraintExprArgs) {
    super(args);
  }
}

export type PeriodForSystemTimeConstraintExprArgs = {
  this: Expression;
  expression: Expression;
} & ColumnConstraintKindExprArgs;

export class PeriodForSystemTimeConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.PERIOD_FOR_SYSTEM_TIME_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for PeriodForSystemTimeConstraint
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<BaseExpressionArgs>;

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

export type CaseSpecificColumnConstraintExprArgs = {
  not: Expression;
} & ColumnConstraintKindExprArgs;

export class CaseSpecificColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.CASE_SPECIFIC_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for CaseSpecificColumnConstraint
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    not: true,
  } satisfies RequiredMap<CaseSpecificColumnConstraintExprArgs>;

  declare args: CaseSpecificColumnConstraintExprArgs;

  constructor (args: CaseSpecificColumnConstraintExprArgs) {
    super(args);
  }

  get $not (): Expression {
    return this.args.not;
  }
}

export type CharacterSetColumnConstraintExprArgs = {
  this: Expression;
} & BaseExpressionArgs;
export class CharacterSetColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.CHARACTER_SET_COLUMN_CONSTRAINT;

  static argTypes = {
    this: true,
  } satisfies RequiredMap<BaseExpressionArgs>;

  declare args: CharacterSetColumnConstraintExprArgs;
  constructor (args: CharacterSetColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type CheckColumnConstraintExprArgs = {
  enforced?: Expression;
  this: Expression;
} & BaseExpressionArgs;

export class CheckColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.CHECK_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for CheckColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    enforced: false,
  } satisfies RequiredMap<CheckColumnConstraintExprArgs>;

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

export type ClusteredColumnConstraintExprArgs = ColumnConstraintKindExprArgs;

export class ClusteredColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.CLUSTERED_COLUMN_CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ClusteredColumnConstraintExprArgs>;

  declare args: ClusteredColumnConstraintExprArgs;

  constructor (args: ClusteredColumnConstraintExprArgs) {
    super(args);
  }
}

export type CollateColumnConstraintExprArgs = ColumnConstraintKindExprArgs;

export class CollateColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.COLLATE_COLUMN_CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CollateColumnConstraintExprArgs>;

  declare args: CollateColumnConstraintExprArgs;

  constructor (args: CollateColumnConstraintExprArgs) {
    super(args);
  }
}

export type CommentColumnConstraintExprArgs = ColumnConstraintKindExprArgs;

export class CommentColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.COMMENT_COLUMN_CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CommentColumnConstraintExprArgs>;

  declare args: CommentColumnConstraintExprArgs;

  constructor (args: CommentColumnConstraintExprArgs) {
    super(args);
  }
}

export type CompressColumnConstraintExprArgs = {
  this?: Expression;
} & ColumnConstraintKindExprArgs;

export class CompressColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.COMPRESS_COLUMN_CONSTRAINT;

  static argTypes = {
    this: false,
  } satisfies RequiredMap<BaseExpressionArgs>;

  declare args: CompressColumnConstraintExprArgs;

  constructor (args: CompressColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type DateFormatColumnConstraintExprArgs = {
  this: Expression;
} & ColumnConstraintKindExprArgs;

export class DateFormatColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.DATE_FORMAT_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for DateFormatColumnConstraint
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<BaseExpressionArgs>;

  declare args: DateFormatColumnConstraintExprArgs;
  constructor (args: DateFormatColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type DefaultColumnConstraintExprArgs = ColumnConstraintKindExprArgs;

export class DefaultColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.DEFAULT_COLUMN_CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DefaultColumnConstraintExprArgs>;

  declare args: DefaultColumnConstraintExprArgs;

  constructor (args: DefaultColumnConstraintExprArgs) {
    super(args);
  }
}

export type EncodeColumnConstraintExprArgs = ColumnConstraintKindExprArgs;

export class EncodeColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.ENCODE_COLUMN_CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<EncodeColumnConstraintExprArgs>;

  declare args: EncodeColumnConstraintExprArgs;

  constructor (args: EncodeColumnConstraintExprArgs) {
    super(args);
  }
}

export type ExcludeColumnConstraintExprArgs = ColumnConstraintKindExprArgs;

export class ExcludeColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.EXCLUDE_COLUMN_CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ExcludeColumnConstraintExprArgs>;

  declare args: ExcludeColumnConstraintExprArgs;

  constructor (args: ExcludeColumnConstraintExprArgs) {
    super(args);
  }
}

export type EphemeralColumnConstraintExprArgs = {
  this?: Expression;
} & ColumnConstraintKindExprArgs;

export class EphemeralColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.EPHEMERAL_COLUMN_CONSTRAINT;

  static argTypes = {
    this: false,
  } satisfies RequiredMap<BaseExpressionArgs>;

  declare args: EphemeralColumnConstraintExprArgs;
  constructor (args: EphemeralColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type GeneratedAsIdentityColumnConstraintExprArgs = {
  onNull?: Expression;
  start?: Expression;
  increment?: Expression;
  minvalue?: string;
  maxvalue?: string;
  cycle?: Expression;
  order?: Expression;
  this?: Expression;
  expression?: Expression;
} & ColumnConstraintKindExprArgs;

export class GeneratedAsIdentityColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.GENERATED_AS_IDENTITY_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for
   * GeneratedAsIdentityColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   * Note: this: true -> ALWAYS, this: false -> BY DEFAULT
   */
  static argTypes = {
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
  } satisfies RequiredMap<GeneratedAsIdentityColumnConstraintExprArgs>;

  declare args: GeneratedAsIdentityColumnConstraintExprArgs;

  constructor (args: GeneratedAsIdentityColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
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

  get $order (): Expression | undefined {
    return this.args.order;
  }
}

export type GeneratedAsRowColumnConstraintExprArgs = {
  start?: Expression;
  hidden?: Expression;
} & ColumnConstraintKindExprArgs;

export class GeneratedAsRowColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.GENERATED_AS_ROW_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for GeneratedAsRowColumnConstraint
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    start: false,
    hidden: false,
  } satisfies RequiredMap<GeneratedAsRowColumnConstraintExprArgs>;

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

export type IndexColumnConstraintExprArgs = {
  kind?: IndexColumnConstraintExprKind;
  indexType?: DataTypeExpr;
  options?: Expression[];
  granularity?: Expression;
  this?: Expression;
  expressions?: Expression[];
  expression?: Expression;
} & ColumnConstraintKindExprArgs;

export class IndexColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.INDEX_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for IndexColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    expressions: false,
    kind: false,
    indexType: false,
    options: false,
    expression: false,
    granularity: false,
  } satisfies RequiredMap<IndexColumnConstraintExprArgs>;

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

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  get $granularity (): Expression | undefined {
    return this.args.granularity;
  }
}

export type InlineLengthColumnConstraintExprArgs = ColumnConstraintKindExprArgs;

export class InlineLengthColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.INLINE_LENGTH_COLUMN_CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<InlineLengthColumnConstraintExprArgs>;

  declare args: InlineLengthColumnConstraintExprArgs;

  constructor (args: InlineLengthColumnConstraintExprArgs) {
    super(args);
  }
}

export type NonClusteredColumnConstraintExprArgs = ColumnConstraintKindExprArgs;

export class NonClusteredColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.NON_CLUSTERED_COLUMN_CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<NonClusteredColumnConstraintExprArgs>;

  declare args: NonClusteredColumnConstraintExprArgs;

  constructor (args: NonClusteredColumnConstraintExprArgs) {
    super(args);
  }
}

export type NotForReplicationColumnConstraintExprArgs = ColumnConstraintKindExprArgs;

export class NotForReplicationColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.NOT_FOR_REPLICATION_COLUMN_CONSTRAINT;

  static argTypes = {} satisfies RequiredMap<NotForReplicationColumnConstraintExprArgs>;

  declare args: NotForReplicationColumnConstraintExprArgs;

  constructor (args: NotForReplicationColumnConstraintExprArgs) {
    super(args);
  }
}

export type MaskingPolicyColumnConstraintExprArgs = {
  this: Expression;
  expressions?: Expression[];
} & ColumnConstraintKindExprArgs;

export class MaskingPolicyColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.MASKING_POLICY_COLUMN_CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
  } satisfies RequiredMap<MaskingPolicyColumnConstraintExprArgs>;

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

export type NotNullColumnConstraintExprArgs = {
  allowNull?: Expression;
} & ColumnConstraintKindExprArgs;

export class NotNullColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.NOT_NULL_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for NotNullColumnConstraint
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    allowNull: false,
  } satisfies RequiredMap<NotNullColumnConstraintExprArgs>;

  declare args: NotNullColumnConstraintExprArgs;

  constructor (args: NotNullColumnConstraintExprArgs) {
    super(args);
  }

  get $allowNull (): Expression | undefined {
    return this.args.allowNull;
  }
}

export type OnUpdateColumnConstraintExprArgs = ColumnConstraintKindExprArgs;

export class OnUpdateColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.ON_UPDATE_COLUMN_CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<OnUpdateColumnConstraintExprArgs>;

  declare args: OnUpdateColumnConstraintExprArgs;

  constructor (args: OnUpdateColumnConstraintExprArgs) {
    super(args);
  }
}

export type PrimaryKeyColumnConstraintExprArgs = {
  desc?: Expression;
  options?: Expression[];
} & ColumnConstraintKindExprArgs;

export class PrimaryKeyColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.PRIMARY_KEY_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for PrimaryKeyColumnConstraint
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    desc: false,
    options: false,
  } satisfies RequiredMap<PrimaryKeyColumnConstraintExprArgs>;

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

export type TitleColumnConstraintExprArgs = ColumnConstraintKindExprArgs;

export class TitleColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.TITLE_COLUMN_CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TitleColumnConstraintExprArgs>;

  declare args: TitleColumnConstraintExprArgs;

  constructor (args: TitleColumnConstraintExprArgs) {
    super(args);
  }
}

export type UniqueColumnConstraintExprArgs = {
  indexType?: DataTypeExpr;
  onConflict?: Expression;
  nulls?: Expression[];
  options?: Expression[];
  this?: Expression;
} & ColumnConstraintKindExprArgs;

export class UniqueColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.UNIQUE_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for UniqueColumnConstraint
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    indexType: false,
    onConflict: false,
    nulls: false,
    options: false,
  } satisfies RequiredMap<UniqueColumnConstraintExprArgs>;

  declare args: UniqueColumnConstraintExprArgs;

  constructor (args: UniqueColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $indexType (): Expression | undefined {
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

export type UppercaseColumnConstraintExprArgs = ColumnConstraintKindExprArgs;

export class UppercaseColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.UPPERCASE_COLUMN_CONSTRAINT;

  static argTypes = {} satisfies RequiredMap<
    UppercaseColumnConstraintExprArgs
  >;

  declare args: UppercaseColumnConstraintExprArgs;

  constructor (args: UppercaseColumnConstraintExprArgs) {
    super(args);
  }
}

export type PathColumnConstraintExprArgs = ColumnConstraintKindExprArgs;

export class PathColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.PATH_COLUMN_CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<PathColumnConstraintExprArgs>;

  declare args: PathColumnConstraintExprArgs;

  constructor (args: PathColumnConstraintExprArgs) {
    super(args);
  }
}

export type ProjectionPolicyColumnConstraintExprArgs = ColumnConstraintKindExprArgs;

export class ProjectionPolicyColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.PROJECTION_POLICY_COLUMN_CONSTRAINT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ProjectionPolicyColumnConstraintExprArgs>;

  declare args: ProjectionPolicyColumnConstraintExprArgs;

  constructor (args: ProjectionPolicyColumnConstraintExprArgs) {
    super(args);
  }
}

export type ComputedColumnConstraintExprArgs = {
  persisted?: Expression;
  notNull?: Expression;
  dataType?: DataTypeExpr;
  this: Expression;
} & ColumnConstraintKindExprArgs;

export class ComputedColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.COMPUTED_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for ComputedColumnConstraint
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    persisted: false,
    notNull: false,
    dataType: false,
  } satisfies RequiredMap<ComputedColumnConstraintExprArgs>;

  declare args: ComputedColumnConstraintExprArgs;

  constructor (args: ComputedColumnConstraintExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $persisted (): Expression | undefined {
    return this.args.persisted;
  }

  get $notNull (): Expression | undefined {
    return this.args.notNull;
  }

  get $dataType (): Expression | undefined {
    return this.args.dataType;
  }
}

export type InOutColumnConstraintExprArgs = {
  input?: Expression;
  output?: Expression;
  variadic?: Expression;
} & ColumnConstraintKindExprArgs;

export class InOutColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.IN_OUT_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for InOutColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    input: false,
    output: false,
    variadic: false,
  } satisfies RequiredMap<InOutColumnConstraintExprArgs>;

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

export type CopyExprArgs = {
  kind: CopyExprKind;
  files?: Expression[];
  credentials?: Expression[];
  format?: string;
  params?: Expression[];
  this: Expression;
} & BaseExpressionArgs;

export class CopyExpr extends DMLExpr {
  key = ExpressionKey.COPY;

  /**
   * Defines the arguments (properties and child expressions) for Copy expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    kind: true,
    files: false,
    credentials: false,
    format: false,
    params: false,
  } satisfies RequiredMap<CopyExprArgs>;

  declare args: CopyExprArgs;

  constructor (args: CopyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $kind (): string {
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

export type InsertExprArgs = {
  hint?: Expression;
  with?: WithExpr;
  isFunction?: boolean;
  conflict?: Expression;
  returning?: Expression;
  overwrite?: Expression;
  exists?: Expression[];
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
} & DMLExprArgs & DDLExprArgs;

export class InsertExpr extends multiInherit(DMLExpr, DDLExpr, Expression) {
  key = ExpressionKey.INSERT;

  /**
   * Defines the arguments (properties and child expressions) for Insert expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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
  } satisfies RequiredMap<InsertExprArgs>;

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
  withWith (
    alias: string | Expression,
    as: string | Expression,
    options: {
      recursive?: boolean;
      materialized?: boolean;
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    return _applyCteBuilder({
      instance: this,
      alias,
      as,
      recursive: options.recursive,
      materialized: options.materialized,
      append: options.append,
      dialect: options.dialect,
      copy: options.copy,
      ...options,
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

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  get $conflict (): Expression | undefined {
    return this.args.conflict;
  }

  get $returning (): Expression | undefined {
    return this.args.returning;
  }

  get $overwrite (): Expression | undefined {
    return this.args.overwrite;
  }

  get $exists (): Expression[] | undefined {
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
 * Represents a literal value (string, number, boolean, null).
 *
 * @example
 * const str = new LiteralExpr({ this: 'hello', isString: true });
 * const num = new LiteralExpr({ this: '42', isString: false });
 */
export type LiteralExprArgs = {
  isString: boolean;
  this: string;
} & BaseExpressionArgs;

export class LiteralExpr extends ConditionExpr {
  key = ExpressionKey.LITERAL;

  /**
   * Defines the arguments (properties and child expressions) for Literal expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    isString: true,
  } satisfies RequiredMap<LiteralExprArgs>;

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
  static string (string: string | unknown): LiteralExpr {
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

export type ClusterExprArgs = OrderExprArgs;

export class ClusterExpr extends OrderExpr {
  key = ExpressionKey.CLUSTER;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ClusterExprArgs>;

  declare args: ClusterExprArgs;

  constructor (args: ClusterExprArgs) {
    super(args);
  }
}

export type DistributeExprArgs = OrderExprArgs;

export class DistributeExpr extends OrderExpr {
  key = ExpressionKey.DISTRIBUTE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DistributeExprArgs>;

  declare args: DistributeExprArgs;

  constructor (args: DistributeExprArgs) {
    super(args);
  }
}

export type SortExprArgs = OrderExprArgs;

export class SortExpr extends OrderExpr {
  key = ExpressionKey.SORT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SortExprArgs>;

  declare args: SortExprArgs;

  constructor (args: SortExprArgs) {
    super(args);
  }
}

export type AlgorithmPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class AlgorithmPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ALGORITHM_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<AlgorithmPropertyExprArgs>;

  declare args: AlgorithmPropertyExprArgs;

  constructor (args: AlgorithmPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type AutoIncrementPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class AutoIncrementPropertyExpr extends PropertyExpr {
  key = ExpressionKey.AUTO_INCREMENT_PROPERTY;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AutoIncrementPropertyExprArgs>;

  declare args: AutoIncrementPropertyExprArgs;

  constructor (args: AutoIncrementPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type AutoRefreshPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class AutoRefreshPropertyExpr extends PropertyExpr {
  key = ExpressionKey.AUTO_REFRESH_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<AutoRefreshPropertyExprArgs>;

  declare args: AutoRefreshPropertyExprArgs;

  constructor (args: AutoRefreshPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type BackupPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class BackupPropertyExpr extends PropertyExpr {
  key = ExpressionKey.BACKUP_PROPERTY;

  static argTypes = {
    this: true,
  } satisfies RequiredMap<BackupPropertyExprArgs>;

  declare args: BackupPropertyExprArgs;

  constructor (args: BackupPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type BuildPropertyExprArgs = { this: Expression } & PropertyExprArgs;

export class BuildPropertyExpr extends PropertyExpr {
  key = ExpressionKey.BUILD_PROPERTY;

  static argTypes = {
    this: true,
  } satisfies RequiredMap<BuildPropertyExprArgs>;

  declare args: BuildPropertyExprArgs;

  constructor (args: BuildPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type BlockCompressionPropertyExprArgs = {
  autotemp?: Expression;
  always?: Expression[];
  default?: Expression;
  manual?: Expression;
  never?: Expression;
} & PropertyExprArgs;

export class BlockCompressionPropertyExpr extends PropertyExpr {
  key = ExpressionKey.BLOCK_COMPRESSION_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for BlockCompressionProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    autotemp: false,
    always: false,
    default: false,
    manual: false,
    never: false,
  } satisfies RequiredMap<BlockCompressionPropertyExprArgs>;

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

export type CharacterSetPropertyExprArgs = {
  value?: string;
  default: Expression;
  this: Expression;
} & PropertyExprArgs;

export class CharacterSetPropertyExpr extends PropertyExpr {
  key = ExpressionKey.CHARACTER_SET_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for CharacterSetProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    default: true,
  } satisfies RequiredMap<CharacterSetPropertyExprArgs>;

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

export type ChecksumPropertyExprArgs = {
  on?: Expression;
  default?: Expression;
} & PropertyExprArgs;

export class ChecksumPropertyExpr extends PropertyExpr {
  key = ExpressionKey.CHECKSUM_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for ChecksumProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    on: false,
    default: false,
  } satisfies RequiredMap<ChecksumPropertyExprArgs>;

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

export type CollatePropertyExprArgs = {
  value?: string;
  default?: Expression;
  this: Expression;
} & PropertyExprArgs;

export class CollatePropertyExpr extends PropertyExpr {
  key = ExpressionKey.COLLATE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for CollateProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    default: false,
  } satisfies RequiredMap<CollatePropertyExprArgs>;

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

export type CopyGrantsPropertyExprArgs = PropertyExprArgs;

export class CopyGrantsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.COPY_GRANTS_PROPERTY;

  static argTypes = {} satisfies RequiredMap<CopyGrantsPropertyExprArgs>;

  declare args: CopyGrantsPropertyExprArgs;

  constructor (args: CopyGrantsPropertyExprArgs) {
    super(args);
  }
}

export type DataBlocksizePropertyExprArgs = {
  size?: number | Expression;
  units?: Expression[];
  minimum?: Expression;
  maximum?: Expression;
  default?: Expression;
} & PropertyExprArgs;

export class DataBlocksizePropertyExpr extends PropertyExpr {
  key = ExpressionKey.DATA_BLOCKSIZE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for DataBlocksizeProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    size: false,
    units: false,
    minimum: false,
    maximum: false,
    default: false,
  } satisfies RequiredMap<DataBlocksizePropertyExprArgs>;

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

export type DataDeletionPropertyExprArgs = {
  on: Expression;
  filterColumn?: Expression;
  retentionPeriod?: Expression;
} & PropertyExprArgs;

export class DataDeletionPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DATA_DELETION_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for DataDeletionProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    on: true,
    filterColumn: false,
    retentionPeriod: false,
  } satisfies RequiredMap<DataDeletionPropertyExprArgs>;

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

export type DefinerPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class DefinerPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DEFINER_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<DefinerPropertyExprArgs>;

  declare args: DefinerPropertyExprArgs;

  constructor (args: DefinerPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type DistKeyPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class DistKeyPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DIST_KEY_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<DistKeyPropertyExprArgs>;

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

export type DistributedByPropertyExprArgs = {
  kind: DistributedByPropertyExprKind;
  buckets?: Expression[];
  order?: Expression;
  expressions?: Expression[];
} & PropertyExprArgs;

export class DistributedByPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DISTRIBUTED_BY_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for DistributedByProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    expressions: false,
    kind: true,
    buckets: false,
    order: false,
  } satisfies RequiredMap<DistributedByPropertyExprArgs>;

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

  get $kind (): string {
    return this.args.kind;
  }

  get $buckets (): Expression[] | undefined {
    return this.args.buckets;
  }

  get $order (): Expression | undefined {
    return this.args.order;
  }
}

export type DistStylePropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class DistStylePropertyExpr extends PropertyExpr {
  key = ExpressionKey.DIST_STYLE_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<DistStylePropertyExprArgs>;

  declare args: DistStylePropertyExprArgs;

  constructor (args: DistStylePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type DuplicateKeyPropertyExprArgs = {
  expressions: Expression[];
} & PropertyExprArgs;

export class DuplicateKeyPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DUPLICATE_KEY_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<DuplicateKeyPropertyExprArgs>;

  declare args: DuplicateKeyPropertyExprArgs;

  constructor (args: DuplicateKeyPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type EnginePropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class EnginePropertyExpr extends PropertyExpr {
  key = ExpressionKey.ENGINE_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<EnginePropertyExprArgs>;

  declare args: EnginePropertyExprArgs;

  constructor (args: EnginePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type HeapPropertyExprArgs = PropertyExprArgs;

export class HeapPropertyExpr extends PropertyExpr {
  key = ExpressionKey.HEAP_PROPERTY;

  static argTypes = {
    ...super.argTypes, // NOTE: sqlglot assisns `{}`
  } satisfies RequiredMap<HeapPropertyExprArgs>;

  declare args: HeapPropertyExprArgs;

  constructor (args: HeapPropertyExprArgs) {
    super(args);
  }
}

export type ToTablePropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class ToTablePropertyExpr extends PropertyExpr {
  key = ExpressionKey.TO_TABLE_PROPERTY;

  static argTypes = {
    this: true,
  } satisfies RequiredMap<ToTablePropertyExprArgs>;

  declare args: ToTablePropertyExprArgs;

  constructor (args: ToTablePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type ExecuteAsPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class ExecuteAsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.EXECUTE_AS_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<ExecuteAsPropertyExprArgs>;

  declare args: ExecuteAsPropertyExprArgs;

  constructor (args: ExecuteAsPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type ExternalPropertyExprArgs = {
  this?: Expression;
} & PropertyExprArgs;

export class ExternalPropertyExpr extends PropertyExpr {
  key = ExpressionKey.EXTERNAL_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<ExternalPropertyExprArgs>;

  declare args: ExternalPropertyExprArgs;

  constructor (args: ExternalPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type FallbackPropertyExprArgs = {
  no: Expression;
  protection?: Expression;
} & PropertyExprArgs;

export class FallbackPropertyExpr extends PropertyExpr {
  key = ExpressionKey.FALLBACK_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for FallbackProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    no: true,
    protection: false,
  } satisfies RequiredMap<FallbackPropertyExprArgs>;

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

export type FileFormatPropertyExprArgs = {
  hiveFormat?: string;
  this?: Expression;
  expressions?: Expression[];
} & PropertyExprArgs;

export class FileFormatPropertyExpr extends PropertyExpr {
  key = ExpressionKey.FILE_FORMAT_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for FileFormatProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    expressions: false,
    hiveFormat: false,
  } satisfies RequiredMap<FileFormatPropertyExprArgs>;

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

export type CredentialsPropertyExprArgs = {
  expressions: Expression[];
} & PropertyExprArgs;

export class CredentialsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.CREDENTIALS_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<CredentialsPropertyExprArgs>;

  declare args: CredentialsPropertyExprArgs;

  constructor (args: CredentialsPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type FreespacePropertyExprArgs = {
  this: Expression;
  percent?: Expression;
} & PropertyExprArgs;

export class FreespacePropertyExpr extends PropertyExpr {
  key = ExpressionKey.FREESPACE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for FreespaceProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    percent: false,
  } satisfies RequiredMap<FreespacePropertyExprArgs>;

  declare args: FreespacePropertyExprArgs;

  constructor (args: FreespacePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }

  get $percent (): Expression | undefined {
    return this.args.percent;
  }
}

export type GlobalPropertyExprArgs = PropertyExprArgs;

export class GlobalPropertyExpr extends PropertyExpr {
  key = ExpressionKey.GLOBAL_PROPERTY;

  static argTypes = {} satisfies RequiredMap<GlobalPropertyExprArgs>;

  declare args: GlobalPropertyExprArgs;

  constructor (args: GlobalPropertyExprArgs) {
    super(args);
  }
}

export type IcebergPropertyExprArgs = PropertyExprArgs;

export class IcebergPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ICEBERG_PROPERTY;

  static argTypes = {} satisfies RequiredMap<IcebergPropertyExprArgs>;

  declare args: IcebergPropertyExprArgs;

  constructor (args: IcebergPropertyExprArgs) {
    super(args);
  }
}

export type InheritsPropertyExprArgs = {
  expressions?: Expression[];
} & PropertyExprArgs;

export class InheritsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.INHERITS_PROPERTY;
  static argTypes = {
    expressions: true,
  } satisfies RequiredMap<InheritsPropertyExprArgs>;

  declare args: InheritsPropertyExprArgs;

  constructor (args: InheritsPropertyExprArgs) {
    super(args);
  }
}

export type InputModelPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class InputModelPropertyExpr extends PropertyExpr {
  key = ExpressionKey.INPUT_MODEL_PROPERTY;

  static argTypes = {
    this: true,
  } satisfies RequiredMap<InputModelPropertyExprArgs>;

  declare args: InputModelPropertyExprArgs;

  constructor (args: InputModelPropertyExprArgs) {
    super(args);
  }
}

export type OutputModelPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class OutputModelPropertyExpr extends PropertyExpr {
  key = ExpressionKey.OUTPUT_MODEL_PROPERTY;

  static argTypes = {
    this: true,
  } satisfies RequiredMap<OutputModelPropertyExprArgs>;

  declare args: OutputModelPropertyExprArgs;

  constructor (args: OutputModelPropertyExprArgs) {
    super(args);
  }
}

export type IsolatedLoadingPropertyExprArgs = {
  no?: Expression;
  concurrent?: Expression;
  target?: Expression;
} & PropertyExprArgs;

export class IsolatedLoadingPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ISOLATED_LOADING_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for IsolatedLoadingProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    no: false,
    concurrent: false,
    target: false,
  } satisfies RequiredMap<IsolatedLoadingPropertyExprArgs>;

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

export type JournalPropertyExprArgs = {
  no?: Expression;
  dual?: Expression;
  before?: Expression;
  local?: Expression;
  after?: Expression;
} & PropertyExprArgs;

export class JournalPropertyExpr extends PropertyExpr {
  key = ExpressionKey.JOURNAL_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for JournalProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    no: false,
    dual: false,
    before: false,
    local: false,
    after: false,
  } satisfies RequiredMap<JournalPropertyExprArgs>;

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

export type LanguagePropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class LanguagePropertyExpr extends PropertyExpr {
  key = ExpressionKey.LANGUAGE_PROPERTY;
  static argTypes = {
    this: true,
  } satisfies RequiredMap<LanguagePropertyExprArgs>;

  declare args: LanguagePropertyExprArgs;

  constructor (args: LanguagePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type EnviromentPropertyExprArgs = {
  expressions: Expression[];
} & PropertyExprArgs;

export class EnviromentPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ENVIROMENT_PROPERTY;

  static argTypes = {
    expressions: true,
  } satisfies RequiredMap<EnviromentPropertyExprArgs>;

  declare args: EnviromentPropertyExprArgs;

  constructor (args: EnviromentPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type ClusteredByPropertyExprArgs = {
  expressions: Expression[];
  sortedBy?: string;
  buckets: Expression[];
} & PropertyExprArgs;

export class ClusteredByPropertyExpr extends PropertyExpr {
  key = ExpressionKey.CLUSTERED_BY_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for ClusteredByProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    expressions: true,
    sortedBy: false,
    buckets: true,
  } satisfies RequiredMap<ClusteredByPropertyExprArgs>;

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

export type DictPropertyExprArgs = {
  this: Expression;
  kind: DictPropertyExprKind;
  settings?: Expression[];
} & PropertyExprArgs;

export class DictPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DICT_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for DictProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    kind: true,
    settings: false,
  } satisfies RequiredMap<DictPropertyExprArgs>;

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

export type DictSubPropertyExprArgs = PropertyExprArgs;

export class DictSubPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DICT_SUB_PROPERTY;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DictSubPropertyExprArgs>;

  declare args: DictSubPropertyExprArgs;

  constructor (args: DictSubPropertyExprArgs) {
    super(args);
  }
}

export type DictRangeExprArgs = {
  min: Expression;
  max: Expression;
  this: Expression;
} & PropertyExprArgs;

export class DictRangeExpr extends PropertyExpr {
  key = ExpressionKey.DICT_RANGE;

  /**
   * Defines the arguments (properties and child expressions) for DictRange expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    min: true,
    max: true,
  } satisfies RequiredMap<DictRangeExprArgs>;

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

export type DynamicPropertyExprArgs = PropertyExprArgs;

export class DynamicPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DYNAMIC_PROPERTY;

  static argTypes = {} satisfies RequiredMap<DynamicPropertyExprArgs>;

  declare args: DynamicPropertyExprArgs;

  constructor (args: DynamicPropertyExprArgs) {
    super(args);
  }
}

export type OnClusterExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class OnClusterExpr extends PropertyExpr {
  key = ExpressionKey.ON_CLUSTER;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<OnClusterExprArgs>;

  declare args: OnClusterExprArgs;

  constructor (args: OnClusterExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type EmptyPropertyExprArgs = PropertyExprArgs;

export class EmptyPropertyExpr extends PropertyExpr {
  key = ExpressionKey.EMPTY_PROPERTY;

  static argTypes = {} satisfies RequiredMap<EmptyPropertyExprArgs>;

  declare args: EmptyPropertyExprArgs;

  constructor (args: EmptyPropertyExprArgs) {
    super(args);
  }
}

export type LikePropertyExprArgs = {
  this: Expression;
  expressions?: Expression[];
} & PropertyExprArgs;

export class LikePropertyExpr extends PropertyExpr {
  key = ExpressionKey.LIKE_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
  } satisfies RequiredMap<LikePropertyExprArgs>;

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

export type LocationPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class LocationPropertyExpr extends PropertyExpr {
  key = ExpressionKey.LOCATION_PROPERTY;

  static argTypes = {
    this: true,
  } satisfies RequiredMap<LocationPropertyExprArgs>;

  declare args: LocationPropertyExprArgs;

  constructor (args: LocationPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type LockPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class LockPropertyExpr extends PropertyExpr {
  key = ExpressionKey.LOCK_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<LockPropertyExprArgs>;

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

export type LockingPropertyExprArgs = {
  kind: LockingPropertyExprKind;
  forOrIn?: Expression;
  lockType: DataTypeExpr;
  override?: Expression;
  this?: Expression;
} & PropertyExprArgs;

export class LockingPropertyExpr extends PropertyExpr {
  key = ExpressionKey.LOCKING_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for LockingProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    kind: true,
    forOrIn: false,
    lockType: true,
    override: false,
  } satisfies RequiredMap<LockingPropertyExprArgs>;

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

export type LogPropertyExprArgs = {
  no: Expression;
} & PropertyExprArgs;

export class LogPropertyExpr extends PropertyExpr {
  key = ExpressionKey.LOG_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for LogProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    no: true,
  } satisfies RequiredMap<LogPropertyExprArgs>;

  declare args: LogPropertyExprArgs;

  constructor (args: LogPropertyExprArgs) {
    super(args);
  }

  get $no (): Expression {
    return this.args.no;
  }
}

export type MaterializedPropertyExprArgs = {
  this?: Expression;
} & BaseExpressionArgs;

export class MaterializedPropertyExpr extends PropertyExpr {
  key = ExpressionKey.MATERIALIZED_PROPERTY;

  static argTypes = {
    this: false,
  } satisfies RequiredMap<MaterializedPropertyExprArgs>;

  declare args: MaterializedPropertyExprArgs;

  constructor (args: MaterializedPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type MergeBlockRatioPropertyExprArgs = {
  value?: string;
  no?: Expression;
  default?: Expression;
  percent?: Expression;
  this?: Expression;
} & PropertyExprArgs;

export class MergeBlockRatioPropertyExpr extends PropertyExpr {
  key = ExpressionKey.MERGE_BLOCK_RATIO_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for MergeBlockRatioProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    no: false,
    default: false,
    percent: false,
  } satisfies RequiredMap<MergeBlockRatioPropertyExprArgs>;

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

export type NoPrimaryIndexPropertyExprArgs = PropertyExprArgs;

export class NoPrimaryIndexPropertyExpr extends PropertyExpr {
  key = ExpressionKey.NO_PRIMARY_INDEX_PROPERTY;

  static argTypes = {} satisfies RequiredMap<NoPrimaryIndexPropertyExprArgs>;

  declare args: NoPrimaryIndexPropertyExprArgs;

  constructor (args: NoPrimaryIndexPropertyExprArgs) {
    super(args);
  }
}

export type OnPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;
export class OnPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ON_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<OnPropertyExprArgs>;

  declare args: OnPropertyExprArgs;

  constructor (args: OnPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type OnCommitPropertyExprArgs = {
  delete?: Expression;
} & PropertyExprArgs;

export class OnCommitPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ON_COMMIT_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for OnCommitProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    delete: false,
  } satisfies RequiredMap<OnCommitPropertyExprArgs>;

  declare args: OnCommitPropertyExprArgs;

  constructor (args: OnCommitPropertyExprArgs) {
    super(args);
  }

  get $delete (): Expression | undefined {
    return this.args.delete;
  }
}

export type PartitionedByPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class PartitionedByPropertyExpr extends PropertyExpr {
  key = ExpressionKey.PARTITIONED_BY_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<PartitionedByPropertyExprArgs>;

  declare args: PartitionedByPropertyExprArgs;

  constructor (args: PartitionedByPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type PartitionedByBucketExprArgs = {
  this: Expression;
  expression: Expression;
} & PropertyExprArgs;

export class PartitionedByBucketExpr extends PropertyExpr {
  key = ExpressionKey.PARTITIONED_BY_BUCKET;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<PartitionedByBucketExprArgs>;

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

export type PartitionByTruncateExprArgs = {
  this: Expression;
  expression: Expression;
} & PropertyExprArgs;

export class PartitionByTruncateExpr extends PropertyExpr {
  key = ExpressionKey.PARTITION_BY_TRUNCATE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<BaseExpressionArgs>;

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

export type PartitionByRangePropertyExprArgs = {
  partitionExpressions: Expression[];
  createExpressions: Expression[];
} & PropertyExprArgs;

export class PartitionByRangePropertyExpr extends PropertyExpr {
  key = ExpressionKey.PARTITION_BY_RANGE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for PartitionByRangeProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    partitionExpressions: true,
    createExpressions: true,
  } satisfies RequiredMap<PartitionByRangePropertyExprArgs>;

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

export type RollupPropertyExprArgs = {
  expressions: Expression[];
} & PropertyExprArgs;

export class RollupPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ROLLUP_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<RollupPropertyExprArgs>;

  declare args: RollupPropertyExprArgs;

  constructor (args: RollupPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type PartitionByListPropertyExprArgs = {
  partitionExpressions: Expression[];
  createExpressions: Expression[];
} & PropertyExprArgs;

export class PartitionByListPropertyExpr extends PropertyExpr {
  key = ExpressionKey.PARTITION_BY_LIST_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for PartitionByListProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    partitionExpressions: true,
    createExpressions: true,
  } satisfies RequiredMap<PartitionByListPropertyExprArgs>;

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
export type RefreshTriggerPropertyExprArgs = {
  method?: string;
  kind?: RefreshTriggerPropertyExprKind;
  every?: Expression;
  unit?: Expression;
  starts?: Expression[];
} & PropertyExprArgs;

export class RefreshTriggerPropertyExpr extends PropertyExpr {
  key = ExpressionKey.REFRESH_TRIGGER_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for RefreshTriggerProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    method: false,
    kind: false,
    every: false,
    unit: false,
    starts: false,
  } satisfies RequiredMap<RefreshTriggerPropertyExprArgs>;

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

export type UniqueKeyPropertyExprArgs = {
  expressions: Expression[];
} & PropertyExprArgs;

export class UniqueKeyPropertyExpr extends PropertyExpr {
  key = ExpressionKey.UNIQUE_KEY_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<UniqueKeyPropertyExprArgs>;

  declare args: UniqueKeyPropertyExprArgs;

  constructor (args: UniqueKeyPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type PartitionedOfPropertyExprArgs = {
  this: Expression;
  expression: Expression;
} & PropertyExprArgs;

export class PartitionedOfPropertyExpr extends PropertyExpr {
  key = ExpressionKey.PARTITIONED_OF_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<PartitionedOfPropertyExprArgs>;

  declare args: PartitionedOfPropertyExprArgs;

  constructor (args: PartitionedOfPropertyExprArgs) {
    super(args);
  }
}

export type StreamingTablePropertyExprArgs = PropertyExprArgs;

export class StreamingTablePropertyExpr extends PropertyExpr {
  key = ExpressionKey.STREAMING_TABLE_PROPERTY;

  static argTypes = {} satisfies RequiredMap<StreamingTablePropertyExprArgs>;

  declare args: StreamingTablePropertyExprArgs;

  constructor (args: StreamingTablePropertyExprArgs) {
    super(args);
  }
}

export type RemoteWithConnectionModelPropertyExprArgs = PropertyExprArgs;

export class RemoteWithConnectionModelPropertyExpr extends PropertyExpr {
  key = ExpressionKey.REMOTE_WITH_CONNECTION_MODEL_PROPERTY;

  static argTypes = {} satisfies RequiredMap<RemoteWithConnectionModelPropertyExprArgs>;

  declare args: RemoteWithConnectionModelPropertyExprArgs;

  constructor (args: RemoteWithConnectionModelPropertyExprArgs) {
    super(args);
  }
}

export type ReturnsPropertyExprArgs = {
  this?: Expression;
  isTable?: Expression;
  table?: Expression;
  null?: Expression;
} & PropertyExprArgs;

export class ReturnsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.RETURNS_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for ReturnsProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    isTable: false,
    table: false,
    null: false,
  } satisfies RequiredMap<ReturnsPropertyExprArgs>;

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

export type StrictPropertyExprArgs = PropertyExprArgs;

export class StrictPropertyExpr extends PropertyExpr {
  key = ExpressionKey.STRICT_PROPERTY;

  static argTypes = {} satisfies RequiredMap<StrictPropertyExprArgs>;

  declare args: StrictPropertyExprArgs;

  constructor (args: StrictPropertyExprArgs) {
    super(args);
  }
}

export type RowFormatPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class RowFormatPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ROW_FORMAT_PROPERTY;

  static argTypes = {
    this: true,
  } satisfies RequiredMap<RowFormatPropertyExprArgs>;

  declare args: RowFormatPropertyExprArgs;

  constructor (args: RowFormatPropertyExprArgs) {
    super(args);
  }
}

export type RowFormatDelimitedPropertyExprArgs = {
  fields?: Expression[];
  escaped?: Expression;
  collectionItems?: Expression[];
  mapKeys?: Expression[];
  lines?: Expression[];
  null?: Expression;
  serde?: Expression;
} & PropertyExprArgs;

export class RowFormatDelimitedPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ROW_FORMAT_DELIMITED_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for RowFormatDelimitedProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    fields: false,
    escaped: false,
    collectionItems: false,
    mapKeys: false,
    lines: false,
    null: false,
    serde: false,
  } satisfies RequiredMap<RowFormatDelimitedPropertyExprArgs>;

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

export type RowFormatSerdePropertyExprArgs = {
  this: Expression;
  serdeProperties?: Expression[];
} & PropertyExprArgs;

export class RowFormatSerdePropertyExpr extends PropertyExpr {
  key = ExpressionKey.ROW_FORMAT_SERDE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for RowFormatSerdeProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    serdeProperties: false,
  } satisfies RequiredMap<RowFormatSerdePropertyExprArgs>;

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

export type SamplePropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class SamplePropertyExpr extends PropertyExpr {
  key = ExpressionKey.SAMPLE_PROPERTY;

  static argTypes = {
    this: true,
  } satisfies RequiredMap<SamplePropertyExprArgs>;

  declare args: SamplePropertyExprArgs;

  constructor (args: SamplePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type SecurityPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class SecurityPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SECURITY_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<SecurityPropertyExprArgs>;

  declare args: SecurityPropertyExprArgs;

  constructor (args: SecurityPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type SchemaCommentPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class SchemaCommentPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SCHEMA_COMMENT_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<SchemaCommentPropertyExprArgs>;

  declare args: SchemaCommentPropertyExprArgs;

  constructor (args: SchemaCommentPropertyExprArgs) {
    super(args);
  }
}

export type SerdePropertiesExprArgs = {
  expressions: Expression[];
  with?: Expression;
} & PropertyExprArgs;

export class SerdePropertiesExpr extends PropertyExpr {
  key = ExpressionKey.SERDE_PROPERTIES;

  /**
   * Defines the arguments (properties and child expressions) for SerdeProperties expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    expressions: true,
    with: false,
  } satisfies RequiredMap<SerdePropertiesExprArgs>;

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

export type SetPropertyExprArgs = {
  multi: Expression;
} & PropertyExprArgs;

export class SetPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SET_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    multi: true,
  } satisfies RequiredMap<SetPropertyExprArgs>;

  declare args: SetPropertyExprArgs;

  constructor (args: SetPropertyExprArgs) {
    super(args);
  }

  get $multi (): Expression {
    return this.args.multi;
  }
}

export type SharingPropertyExprArgs = {
  this?: Expression;
} & PropertyExprArgs;

export class SharingPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SHARING_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<SharingPropertyExprArgs>;

  declare args: SharingPropertyExprArgs;

  constructor (args: SetPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type SetConfigPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class SetConfigPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SET_CONFIG_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<SetConfigPropertyExprArgs>;

  declare args: SetConfigPropertyExprArgs;

  constructor (args: SetConfigPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type SettingsPropertyExprArgs = {
  expressions: Expression[];
} & PropertyExprArgs;

export class SettingsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SETTINGS_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<SettingsPropertyExprArgs>;

  declare args: SettingsPropertyExprArgs;

  constructor (args: SettingsPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type SortKeyPropertyExprArgs = {
  this: Expression;
  compound?: Expression;
} & PropertyExprArgs;

export class SortKeyPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SORT_KEY_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for SortKeyProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    compound: false,
  } satisfies RequiredMap<SortKeyPropertyExprArgs>;

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

export type SqlReadWritePropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class SqlReadWritePropertyExpr extends PropertyExpr {
  key = ExpressionKey.SQL_READ_WRITE_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<SqlReadWritePropertyExprArgs>;

  declare args: SqlReadWritePropertyExprArgs;

  constructor (args: SqlReadWritePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type SqlSecurityPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class SqlSecurityPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SQL_SECURITY_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<SqlSecurityPropertyExprArgs>;

  declare args: SqlSecurityPropertyExprArgs;

  constructor (args: SqlSecurityPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type StabilityPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class StabilityPropertyExpr extends PropertyExpr {
  key = ExpressionKey.STABILITY_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<StabilityPropertyExprArgs>;

  declare args: StabilityPropertyExprArgs;

  constructor (args: StabilityPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type StorageHandlerPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class StorageHandlerPropertyExpr extends PropertyExpr {
  key = ExpressionKey.STORAGE_HANDLER_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<StorageHandlerPropertyExprArgs>;

  declare args: StorageHandlerPropertyExprArgs;

  constructor (args: StorageHandlerPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type TemporaryPropertyExprArgs = {
  this?: Expression;
} & PropertyExprArgs;

export class TemporaryPropertyExpr extends PropertyExpr {
  key = ExpressionKey.TEMPORARY_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<TemporaryPropertyExprArgs>;

  declare args: TemporaryPropertyExprArgs;

  constructor (args: TemporaryPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type SecurePropertyExprArgs = PropertyExprArgs;

export class SecurePropertyExpr extends PropertyExpr {
  key = ExpressionKey.SECURE_PROPERTY;

  static argTypes = {} satisfies RequiredMap<SecurePropertyExprArgs>;

  declare args: SecurePropertyExprArgs;

  constructor (args: SecurePropertyExprArgs) {
    super(args);
  }
}

export type TagsExprArgs = {
  expressions: Expression[];
} & PropertyExprArgs & ColumnConstraintKindExprArgs;

export class TagsExpr extends multiInherit(Expression, PropertyExpr, ColumnConstraintKindExpr) {
  key = ExpressionKey.TAGS;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<TagsExprArgs>;

  declare args: TagsExprArgs;

  constructor (args: TagsExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type TransformModelPropertyExprArgs = {
  expressions: Expression[];
} & PropertyExprArgs;

export class TransformModelPropertyExpr extends PropertyExpr {
  key = ExpressionKey.TRANSFORM_MODEL_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<TransformModelPropertyExprArgs>;

  declare args: TransformModelPropertyExprArgs;

  constructor (args: TransformModelPropertyExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type TransientPropertyExprArgs = {
  this?: Expression;
} & PropertyExprArgs;

export class TransientPropertyExpr extends PropertyExpr {
  key = ExpressionKey.TRANSIENT_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<TransientPropertyExprArgs>;

  declare args: TransientPropertyExprArgs;

  constructor (args: TransientPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type UnloggedPropertyExprArgs = PropertyExprArgs;

export class UnloggedPropertyExpr extends PropertyExpr {
  key = ExpressionKey.UNLOGGED_PROPERTY;

  static argTypes = {} satisfies RequiredMap<UnloggedPropertyExprArgs>;

  declare args: UnloggedPropertyExprArgs;

  constructor (args: UnloggedPropertyExprArgs) {
    super(args);
  }
}

export type UsingTemplatePropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class UsingTemplatePropertyExpr extends PropertyExpr {
  key = ExpressionKey.USING_TEMPLATE_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<UsingTemplatePropertyExprArgs>;

  declare args: UsingTemplatePropertyExprArgs;

  constructor (args: UsingTemplatePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type ViewAttributePropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class ViewAttributePropertyExpr extends PropertyExpr {
  key = ExpressionKey.VIEW_ATTRIBUTE_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<ViewAttributePropertyExprArgs>;

  declare args: ViewAttributePropertyExprArgs;

  constructor (args: ViewAttributePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type VolatilePropertyExprArgs = {
  this?: Expression;
} & PropertyExprArgs;

export class VolatilePropertyExpr extends PropertyExpr {
  key = ExpressionKey.VOLATILE_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<VolatilePropertyExprArgs>;

  declare args: VolatilePropertyExprArgs;

  constructor (args: VolatilePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type WithDataPropertyExprArgs = {
  no: Expression;
  statistics?: Expression[];
} & PropertyExprArgs;

export class WithDataPropertyExpr extends PropertyExpr {
  key = ExpressionKey.WITH_DATA_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for WithDataProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    no: true,
    statistics: false,
  } satisfies RequiredMap<WithDataPropertyExprArgs>;

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

export type WithJournalTablePropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class WithJournalTablePropertyExpr extends PropertyExpr {
  key = ExpressionKey.WITH_JOURNAL_TABLE_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<WithJournalTablePropertyExprArgs>;

  declare args: WithJournalTablePropertyExprArgs;

  constructor (args: WithJournalTablePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type WithSchemaBindingPropertyExprArgs = {
  this: Expression;
} & PropertyExprArgs;

export class WithSchemaBindingPropertyExpr extends PropertyExpr {
  key = ExpressionKey.WITH_SCHEMA_BINDING_PROPERTY;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<WithSchemaBindingPropertyExprArgs>;

  declare args: WithSchemaBindingPropertyExprArgs;

  constructor (args: WithSchemaBindingPropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type WithSystemVersioningPropertyExprArgs = {
  on?: Expression;
  dataConsistency?: Expression;
  retentionPeriod?: Expression;
  with: Expression;
} & PropertyExprArgs;

export class WithSystemVersioningPropertyExpr extends PropertyExpr {
  key = ExpressionKey.WITH_SYSTEM_VERSIONING_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for WithSystemVersioningProperty
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    on: false,
    dataConsistency: false,
    retentionPeriod: false,
    with: true,
  } satisfies RequiredMap<WithSystemVersioningPropertyExprArgs>;

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

export type WithProcedureOptionsExprArgs = {
  expressions: Expression[];
} & PropertyExprArgs;

export class WithProcedureOptionsExpr extends PropertyExpr {
  key = ExpressionKey.WITH_PROCEDURE_OPTIONS;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<WithProcedureOptionsExprArgs>;

  declare args: WithProcedureOptionsExprArgs;

  constructor (args: WithProcedureOptionsExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type EncodePropertyExprArgs = {
  this: Expression;
  properties?: Expression[];
  key?: Expression;
} & PropertyExprArgs;

export class EncodePropertyExpr extends PropertyExpr {
  key = ExpressionKey.ENCODE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for EncodeProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   * Note: The 'key' argument can be accessed via this.args.key (no getter to avoid conflict with
   * Expression.key).
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    properties: false,
    key: false,
  } satisfies RequiredMap<EncodePropertyExprArgs>;

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

export type IncludePropertyExprArgs = {
  this: Expression;
  alias?: Expression;
  columnDef?: Expression;
} & PropertyExprArgs;

export class IncludePropertyExpr extends PropertyExpr {
  key = ExpressionKey.INCLUDE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for IncludeProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   * Note: The 'alias' argument can be accessed via this.args.alias (no getter to avoid conflict
   * with Expression.alias).
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    alias: false,
    columnDef: false,
  } satisfies RequiredMap<IncludePropertyExprArgs>;

  declare args: IncludePropertyExprArgs;

  constructor (args: IncludePropertyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $alias (): Expression | undefined {
    return this.args.alias;
  }

  get $columnDef (): Expression | undefined {
    return this.args.columnDef;
  }
}

export type ForcePropertyExprArgs = PropertyExprArgs;

export class ForcePropertyExpr extends PropertyExpr {
  key = ExpressionKey.FORCE_PROPERTY;

  static argTypes = {} satisfies RequiredMap<ForcePropertyExprArgs>;

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

export type PropertiesExprArgs = {
  expressions: Expression[];
} & BaseExpressionArgs;

export class PropertiesExpr extends Expression {
  key = ExpressionKey.PROPERTIES;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<PropertiesExprArgs>;

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

export type SetOperationExprArgs = {
  this: QueryExpr;
  expression: QueryExpr;
  distinct?: boolean;
  byName?: string;
  side?: string;
  kind?: SetOperationExprKind;
  on?: Expression;
  match?: Expression;
  laterals?: Expression[];
  joins?: Expression[];
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
} & QueryExprArgs;

export class SetOperationExpr extends QueryExpr {
  key = ExpressionKey.SET_OPERATION;

  static argTypes = {
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
  } satisfies RequiredMap<SetOperationExprArgs>;

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
    expressions: Array<string | Expression>,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const self = maybeCopy(this, options.copy ?? true);
    self.this.unnest().select(expressions, {
      ...options,
      copy: false,
    });
    self.expression.unnest().select(expressions, {
      ...options,
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
    let expression: Expression = this;
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
    let expression: Expression = this;
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

export type UpdateExprArgs = {
  with?: Expression;
  this?: Expression;
  expressions?: Expression[];
  from?: Expression;
  where?: Expression;
  returning?: Expression;
  order?: Expression;
  limit?: number | Expression;
  options?: Expression[];
} & DMLExprArgs;

export class UpdateExpr extends DMLExpr {
  key = ExpressionKey.UPDATE;

  static argTypes = {
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
  } satisfies RequiredMap<UpdateExprArgs>;

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
    return _applyBuilder(expression, {
      instance: this,
      arg: 'this',
      into: TableExpr,
      prefix: undefined,
      dialect: options.dialect,
      copy: options.copy ?? true,
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
    expressions: Array<string | Expression>,
    options?: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    },
  ): this {
    return _applyListBuilder(expressions, {
      instance: this,
      arg: 'expressions',
      append: options?.append ?? true,
      into: Expression,
      prefix: undefined,
      dialect: options?.dialect,
      copy: options?.copy ?? true,
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
    expressions: Array<string | Expression | undefined>,
    options?: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    },
  ): this {
    return _applyConjunctionBuilder(expressions, {
      instance: this,
      arg: 'where',
      append: options?.append ?? true,
      into: WhereExpr,
      dialect: options?.dialect,
      copy: options?.copy ?? true,
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
    if (!expression) {
      return this;
    }

    return _applyBuilder(expression, {
      instance: this,
      arg: 'from',
      into: FromExpr,
      prefix: undefined,
      dialect: options.dialect,
      copy: options.copy ?? true,
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
    alias: string | Expression,
    as: string | Expression,
    options: {
      recursive?: boolean;
      materialized?: boolean;
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      scalar?: boolean;
    } = {},
  ): this {
    return _applyCteBuilder({
      instance: this,
      alias,
      as,
      ...options,
      recursive: options.recursive ?? false,
      append: options.append ?? true,
      copy: options.copy ?? true,
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
export type SelectExprArgs = {
  with?: Expression;
  kind?: SelectExprKind;
  expressions?: Expression[];
  hint?: Expression;
  distinct?: boolean;
  into?: Expression;
  from?: Expression;
  operationModifiers?: Expression[];
  match?: Expression;
  laterals?: Expression[];
  joins?: Expression[];
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
} & QueryExprArgs;

export class SelectExpr extends QueryExpr {
  key = ExpressionKey.SELECT;

  static argTypes = {
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
  } satisfies RequiredMap<SelectExprArgs>;

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

  get $from (): Expression | undefined {
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

  get $joins (): Expression[] | undefined {
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

    for (const e of this.expressions) {
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
    return this.expressions.some((expression) => expression.isStar);
  }

  /**
   * Returns the SELECT expressions.
   */
  get selects (): Expression[] {
    return this.expressions;
  }

  /**
   * Set the FROM expression.
   *
   * @example
   * select().from("tbl").select(["x"]).sql()
   * // 'SELECT x FROM tbl'
   */
  from (
    expression: string | Expression,
    options: {
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    return _applyBuilder(expression, {
      ...options,
      instance: this,
      arg: 'from',
      into: FromExpr,
      prefix: 'FROM',
      dialect: options.dialect,
      copy: options.copy ?? true,
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
    expressions: Array<string | Expression | undefined>,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    if (expressions.length === 0) {
      return options.copy ?? true ? (this.copy() as this) : this;
    }

    return _applyChildListBuilder(expressions, {
      ...options,
      instance: this,
      arg: 'group',
      append: options.append ?? true,
      copy: options.copy ?? true,
      prefix: 'GROUP BY',
      into: GroupExpr,
      dialect: options.dialect,
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
    expressions: Array<string | Expression | undefined>,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    return _applyChildListBuilder(expressions, {
      ...options,
      instance: this,
      arg: 'sort',
      append: options.append ?? true,
      copy: options.copy ?? true,
      prefix: 'SORT BY',
      into: SortExpr,
      dialect: options.dialect,
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
    expressions: Array<string | Expression | undefined>,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    return _applyChildListBuilder(expressions, {
      ...options,
      instance: this,
      arg: 'cluster',
      append: options.append ?? true,
      copy: options.copy ?? true,
      prefix: 'CLUSTER BY',
      into: ClusterExpr,
      dialect: options.dialect,
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
    expressions: Array<string | Expression | undefined>,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    return _applyListBuilder(expressions, {
      ...options,
      instance: this,
      arg: 'expressions',
      append: options.append ?? true,
      dialect: options.dialect,
      into: Expression,
      copy: options.copy ?? true,
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
    expressions: Array<string | Expression | undefined>,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    return _applyListBuilder(expressions, {
      ...options,
      instance: this,
      arg: 'laterals',
      append: options.append ?? true,
      into: LateralExpr,
      prefix: 'LATERAL VIEW',
      dialect: options.dialect,
      copy: options.copy ?? true,
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
      on?: string | Expression | Array<string | Expression>;
      using?: string | Expression | Array<string | Expression>;
      append?: boolean;
      joinType?: string;
      joinAlias?: string | Expression;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const parseArgs = {
      dialect: options.dialect,
    };

    let expr: Expression;
    try {
      expr = maybeParse(expression, JoinExpr, {
        ...parseArgs,
        prefix: 'JOIN',
      });
    } catch {
      expr = maybeParse(expression, Expression, parseArgs);
    }

    let join = expr instanceof JoinExpr ? expr : new JoinExpr({ this: expr });

    // If joining a Select, wrap it in a subquery
    if (join.args.this instanceof SelectExpr) {
      join.args.this.replace(join.args.this.subquery());
    }

    // Set join type (method, side, kind)
    if (options?.joinType) {
      const [
        method,
        side,
        kind,
      ] = maybeParse(options.joinType, {
        ...parseArgs,
        into: 'JOIN_TYPE',
      });
      if (method) {
        join.set('method', method.text);
      }
      if (side) {
        join.set('side', side.text);
      }
      if (kind) {
        join.set('kind', kind.text);
      }
    }

    // Set ON condition
    if (options?.on) {
      const onExpr = and(...ensureList(options.on), {
        dialect: options.dialect,
        copy: options.copy ?? true,
      });
      join.set('on', onExpr);
    }

    // Set USING
    if (options?.using) {
      join = _applyListBuilder(ensureList(options.using), {
        instance: join,
        arg: 'using',
        append: options.append ?? true,
        copy: options.copy ?? true,
        into: IdentifierExpr,
      }) as JoinExpr;
    }

    // Set join alias
    if (options?.joinAlias) {
      join.set('this', alias(join.args.this, options?.joinAlias, { table: true }));
    }

    return _applyListBuilder([join], {
      instance: this,
      arg: 'joins',
      append: options.append ?? true,
      copy: options.copy ?? true,
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
    expressions: Array<string | Expression | undefined>,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    return _applyListBuilder(expressions, {
      ...options,
      instance: this,
      arg: 'windows',
      append: options.append ?? true,
      into: WindowExpr,
      dialect: options.dialect,
      copy: options.copy ?? true,
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
    expressions: Array<string | Expression | undefined>,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    return _applyConjunctionBuilder(expressions, {
      ...options,
      instance: this,
      arg: 'qualify',
      append: options.append ?? true,
      into: QualifyExpr,
      dialect: options.dialect,
      copy: options.copy ?? true,
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
    ons?: Array<string | Expression | undefined>,
    options: {
      distinct?: boolean;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const instance = maybeCopy(this, options.copy ?? true);
    const distinctValue = options.distinct ?? true;

    if (ons && 0 < ons.length) {
      const onExprs = ons.filter((on): on is string | Expression => on !== undefined)
        .map((on) => maybeParse(on, Expression, { copy: options.copy ?? true }));
      const tupleExpr = new TupleExpr({ expressions: onExprs });
      instance.set('distinct', distinctValue ? new DistinctExpr({ on: tupleExpr }) : undefined);
    } else {
      instance.set('distinct', distinctValue ? new DistinctExpr({}) : undefined);
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
    const instance = maybeCopy(this, options.copy ?? true);
    const tableExpr = maybeParse(table, TableExpr, { dialect: options.dialect });

    let propertiesExpr: PropertiesExpr | undefined;
    if (options.properties) {
      propertiesExpr = PropertiesExpr.fromDict(options.properties);
    }

    return new CreateExpr({
      this: tableExpr,
      kind: 'TABLE',
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
    const inst = maybeCopy(this, options.copy ?? true);
    inst.set('locks', [new LockExpr({ update: new LiteralExpr({ this: options.update ?? true }) })]);
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
    hints: Array<string | Expression>,
    options: {
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const hintExprs = hints.map((h) =>
      maybeParse(h, Expression, { dialect: options.dialect }));
    const inst = maybeCopy(this, options.copy ?? true);
    inst.set('hint', new HintExpr({ expressions: hintExprs }));
    return inst as this;
  }
}

export type SubqueryExprArgs = {
  with?: WithExpr;
  alias?: TableAliasExpr;
  this: Expression;
} & DerivedTableExprArgs & QueryExprArgs;

export class SubqueryExpr extends multiInherit(DerivedTableExpr, QueryExpr) {
  key = ExpressionKey.SUBQUERY;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    alias: false,
    with: false,
  } satisfies RequiredMap<SubqueryExprArgs>;

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
  unnest (): Expression {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let expression: Expression = this;
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
    expressions: Array<string | Expression | undefined>,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const instance = maybeCopy(this, options.copy ?? true);
    const unnested = instance?.unnest();

    if (unnested instanceof SelectExpr) {
      unnested.select(expressions, {
        ...options,
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
      ([k, v]) => k === 'this' || v === null || v === undefined,
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

export type WindowExprArgs = {
  this: Expression;
  partitionBy?: Expression;
  order?: Expression;
  spec?: Expression;
  alias?: TableAliasExpr;
  over?: Expression;
  first?: Expression;
} & ConditionExprArgs;

export class WindowExpr extends ConditionExpr {
  key = ExpressionKey.WINDOW;

  /**
   * Defines the arguments (properties and child expressions) for Window expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    partitionBy: false,
    order: false,
    spec: false,
    alias: false,
    over: false,
    first: false,
  } satisfies RequiredMap<WindowExprArgs>;

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

export type ParameterExprArgs = {
  this: Expression;
  expression?: Expression;
} & ConditionExprArgs;

export class ParameterExpr extends ConditionExpr {
  key = ExpressionKey.PARAMETER;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<ParameterExprArgs>;

  declare args: ParameterExprArgs;

  constructor (args: ParameterExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
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
export type SessionParameterExprArgs = {
  this: Expression;
  kind?: SessionParameterExprKind;
} & ConditionExprArgs;

export class SessionParameterExpr extends ConditionExpr {
  key = ExpressionKey.SESSION_PARAMETER;

  static argTypes = {
    ...super.argTypes,
    this: true,
    kind: false,
  } satisfies RequiredMap<SessionParameterExprArgs>;

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
export type PlaceholderExprArgs = {
  this?: Expression;
  kind?: PlaceholderExprKind;
  widget?: Expression;
  jdbc?: boolean;
} & ConditionExprArgs;

export class PlaceholderExpr extends ConditionExpr {
  key = ExpressionKey.PLACEHOLDER;

  /**
   * Defines the arguments (properties and child expressions) for Placeholder expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    kind: false,
    widget: false,
    jdbc: false,
  } satisfies RequiredMap<PlaceholderExprArgs>;

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

export type NullExprArgs = ConditionExprArgs;

export class NullExpr extends ConditionExpr {
  key = ExpressionKey.NULL;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<NullExprArgs>;

  declare args: NullExprArgs;

  constructor (args: NullExprArgs) {
    super(args);
  }

  /**
   * Returns the name of this null expression.
   */
  get name (): string {
    return 'NULL';
  }

  /**
   * Converts this to a Python null value.
   */
  toValue (): null {
    return null;
  }
}

export type BooleanExprArgs = ConditionExprArgs;

export class BooleanExpr extends ConditionExpr {
  key = ExpressionKey.BOOLEAN;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<BooleanExprArgs>;

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

export type PseudoTypeExprArgs = {
  this: DataTypeExprKind;
} & DataTypeExprArgs;

export class PseudoTypeExpr extends DataTypeExpr {
  key = ExpressionKey.PSEUDO_TYPE;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<PseudoTypeExprArgs>;

  declare args: PseudoTypeExprArgs;

  constructor (args: PseudoTypeExprArgs) {
    super(args);
  }

  get $this (): DataTypeExprKind {
    return this.args.this;
  }
}

export type ObjectIdentifierExprArgs = {
  this: DataTypeExprKind;
} & DataTypeExprArgs;

export class ObjectIdentifierExpr extends DataTypeExpr {
  key = ExpressionKey.OBJECT_IDENTIFIER;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<ObjectIdentifierExprArgs>;

  declare args: ObjectIdentifierExprArgs;

  constructor (args: ObjectIdentifierExprArgs) {
    super(args);
  }

  get $this (): DataTypeExprKind {
    return this.args.this;
  }
}

export type BinaryExprArgs = {
  this: Expression;
  expression: Expression;
} & ConditionExprArgs;

export class BinaryExpr extends ConditionExpr {
  key = ExpressionKey.BINARY;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<BinaryExprArgs>;

  declare args: BinaryExprArgs;

  constructor (args: BinaryExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression {
    return this.args.expression;
  }

  get left (): Expression {
    return this.args.this;
  }

  get right (): Expression {
    return this.args.expression;
  }
}

export type UnaryExprArgs = BaseExpressionArgs;
export class UnaryExpr extends Expression {
  key = ExpressionKey.UNARY;
  static argTypes = {} satisfies RequiredMap<UnaryExprArgs>;

  declare args: UnaryExprArgs;
  constructor (args: UnaryExprArgs) {
    super(args);
  }
}

export type PivotAliasExprArgs = AliasExprArgs;

export class PivotAliasExpr extends AliasExpr {
  key = ExpressionKey.PIVOT_ALIAS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<PivotAliasExprArgs>;

  declare args: PivotAliasExprArgs;
  constructor (args: PivotAliasExprArgs) {
    super(args);
  }
}

export type BracketExprArgs = {
  this: Expression;
  expressions: Expression[];
  offset?: boolean;
  safe?: boolean;
  returnsListForMaps?: Expression[];
} & ConditionExprArgs;

/**
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#array_subscript_operator
 */
export class BracketExpr extends ConditionExpr {
  key = ExpressionKey.BRACKET;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
    offset: false,
    safe: false,
    returnsListForMaps: false,
  } satisfies RequiredMap<BracketExprArgs>;

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

export type IntervalOpExprArgs = {
  unit?: VarExpr | IntervalSpanExpr;
  expression: Expression;
} & TimeUnitExprArgs;

export class IntervalOpExpr extends TimeUnitExpr {
  key = ExpressionKey.INTERVAL_OP;

  static argTypes = {
    ...super.argTypes,
    unit: false,
    expression: true,
  } satisfies RequiredMap<IntervalOpExprArgs>;

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
export type IntervalSpanExprArgs = {
  this: DataTypeExprKind;
  expression: Expression;
} & DataTypeExprArgs;

export class IntervalSpanExpr extends DataTypeExpr {
  key = ExpressionKey.INTERVAL_SPAN;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<IntervalSpanExprArgs>;

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

export type IntervalExprArgs = {
  this?: Expression;
  unit?: VarExpr | IntervalSpanExpr;
} & TimeUnitExprArgs;

export class IntervalExpr extends TimeUnitExpr {
  key = ExpressionKey.INTERVAL;

  static argTypes = {
    ...super.argTypes,
    this: false,
    unit: false,
  } satisfies RequiredMap<IntervalExprArgs>;

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
export type FuncExprArgs = ConditionExprArgs;

export class FuncExpr extends ConditionExpr {
  key = ExpressionKey.FUNC;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<FuncExprArgs>;

  declare args: FuncExprArgs;

  constructor (args: FuncExprArgs) {
    super(args);
  }

  static isVarLenArgs = false;

  /**
   * Create a function instance from a list of arguments
   */
  static fromArgList (args: Expression[]): FuncExpr {
    const allArgKeys = Object.keys(this.argTypes);

    if (this.isVarLenArgs) {
      const nonVarLenArgKeys = allArgKeys.slice(0, -1);
      const numNonVar = nonVarLenArgKeys.length;

      const argsDict: Record<string, Expression | Expression[]> = {};
      for (let i = 0; i < nonVarLenArgKeys.length; i++) {
        argsDict[nonVarLenArgKeys[i]] = args[i];
      }
      argsDict[allArgKeys[allArgKeys.length - 1]] = args.slice(numNonVar);

      return new this(argsDict as FuncExprArgs);
    } else {
      const argsDict: Record<string, Expression> = {};
      for (let i = 0; i < allArgKeys.length; i++) {
        argsDict[allArgKeys[i]] = args[i];
      }

      return new this(argsDict as FuncExprArgs);
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

export type JSONPathFilterExprArgs = {
  this: Expression;
} & BaseExpressionArgs;
export class JSONPathFilterExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_FILTER;
  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<JSONPathFilterExprArgs>;

  declare args: JSONPathFilterExprArgs;
  constructor (args: JSONPathFilterExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type JSONPathKeyExprArgs = {
  this: Expression;
} & BaseExpressionArgs;
export class JSONPathKeyExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_KEY;
  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<JSONPathKeyExprArgs>;

  declare args: JSONPathKeyExprArgs;
  constructor (args: JSONPathKeyExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type JSONPathRecursiveExprArgs = {
  this?: Expression;
} & BaseExpressionArgs;
export class JSONPathRecursiveExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_RECURSIVE;
  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<JSONPathRecursiveExprArgs>;

  declare args: JSONPathRecursiveExprArgs;
  constructor (args: JSONPathRecursiveExprArgs) {
    super(args);
  }

  get $this (): Expression | undefined {
    return this.args.this;
  }
}

export type JSONPathRootExprArgs = BaseExpressionArgs;
export class JSONPathRootExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_ROOT;
  static argTypes = {} satisfies RequiredMap<JSONPathRootExprArgs>;

  declare args: JSONPathRootExprArgs;
  constructor (args: JSONPathRootExprArgs) {
    super(args);
  }
}

export type JSONPathScriptExprArgs = {
  this: Expression;
} & BaseExpressionArgs;
export class JSONPathScriptExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_SCRIPT;
  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<JSONPathScriptExprArgs>;

  declare args: JSONPathScriptExprArgs;
  constructor (args: JSONPathScriptExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type JSONPathSliceExprArgs = { start?: Expression;
  end?: Expression;
  step?: Expression; } & BaseExpressionArgs;

export class JSONPathSliceExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_SLICE;

  /**
   * Defines the arguments (properties and child expressions) for JSONPathSlice expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    start: false,
    end: false,
    step: false,
  } satisfies RequiredMap<JSONPathSliceExprArgs>;

  declare args: JSONPathSliceExprArgs;

  constructor (args: JSONPathSliceExprArgs) {
    super(args);
  }

  get $start (): Expression | undefined {
    return this.args.start;
  }

  get $end (): Expression | undefined {
    return this.args.end;
  }

  get $step (): Expression | undefined {
    return this.args.step;
  }
}

export type JSONPathSelectorExprArgs = {
  this: Expression;
} & BaseExpressionArgs;
export class JSONPathSelectorExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_SELECTOR;
  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<JSONPathSelectorExprArgs>;

  declare args: JSONPathSelectorExprArgs;
  constructor (args: JSONPathSelectorExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type JSONPathSubscriptExprArgs = {
  this: Expression;
} & BaseExpressionArgs;
export class JSONPathSubscriptExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_SUBSCRIPT;
  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<JSONPathSubscriptExprArgs>;

  declare args: JSONPathSubscriptExprArgs;
  constructor (args: JSONPathSubscriptExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }
}

export type JSONPathUnionExprArgs = {
  expressions: Expression[];
} & BaseExpressionArgs;

export class JSONPathUnionExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_UNION;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<JSONPathUnionExprArgs>;

  declare args: JSONPathUnionExprArgs;

  constructor (args: JSONPathUnionExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] {
    return this.args.expressions;
  }
}

export type JSONPathWildcardExprArgs = BaseExpressionArgs;
export class JSONPathWildcardExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_WILDCARD;
  static argTypes = {} satisfies RequiredMap<JSONPathWildcardExprArgs>;

  declare args: JSONPathWildcardExprArgs;
  constructor (args: JSONPathWildcardExprArgs) {
    super(args);
  }
}

export type MergeExprArgs = { using: string;
  on?: Expression;
  usingCond?: string;
  whens: Expression[];
  with?: Expression;
  returning?: Expression; } & BaseExpressionArgs;

export class MergeExpr extends DMLExpr {
  key = ExpressionKey.MERGE;

  /**
   * Defines the arguments (properties and child expressions) for Merge expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    using: true,
    on: false,
    usingCond: false,
    whens: true,
    with: false,
    returning: false,
  } satisfies RequiredMap<MergeExprArgs>;

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

  get $usingCond (): Expression | undefined {
    return this.args.usingCond;
  }

  get $whens (): Expression[] {
    return this.args.whens;
  }

  get $with (): Expression | undefined {
    return this.args.with;
  }

  get $returning (): Expression | undefined {
    return this.args.returning;
  }
}

export type LateralExprArgs = {
  view?: Expression;
  outer?: Expression;
  crossApply?: boolean;
  ordinality?: boolean;
  alias?: TableAliasExpr;
  this: Expression;
} & BaseExpressionArgs;

export class LateralExpr extends UDTFExpr {
  key = ExpressionKey.LATERAL;

  /**
   * Defines the arguments (properties and child expressions) for Lateral expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    view: false,
    outer: false,
    alias: false,
    crossApply: false,
    ordinality: false,
  } satisfies RequiredMap<LateralExprArgs>;

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

export type TableFromRowsExprArgs = {
  joins?: Expression[];
  pivots?: Expression[];
  sample?: number | Expression;
  alias?: TableAliasExpr;
  this: Expression;
} & UDTFExprArgs;

export class TableFromRowsExpr extends UDTFExpr {
  key = ExpressionKey.TABLE_FROM_ROWS;

  /**
   * Defines the arguments (properties and child expressions) for TableFromRows expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    alias: false,
    joins: false,
    pivots: false,
    sample: false,
  } satisfies RequiredMap<TableFromRowsExprArgs>;

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

  get $joins (): Expression[] | undefined {
    return this.args.joins;
  }

  get $pivots (): Expression[] | undefined {
    return this.args.pivots;
  }

  get $sample (): number | Expression | undefined {
    return this.args.sample;
  }
}

export type UnionExprArgs = SetOperationExprArgs;

export class UnionExpr extends SetOperationExpr {
  key = ExpressionKey.UNION;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<UnionExprArgs>;

  declare args: UnionExprArgs;

  constructor (args: UnionExprArgs) {
    super(args);
  }
}

export type ExceptExprArgs = SetOperationExprArgs;

export class ExceptExpr extends SetOperationExpr {
  key = ExpressionKey.EXCEPT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ExceptExprArgs>;

  declare args: ExceptExprArgs;

  constructor (args: ExceptExprArgs) {
    super(args);
  }
}

export type IntersectExprArgs = SetOperationExprArgs;

export class IntersectExpr extends SetOperationExpr {
  key = ExpressionKey.INTERSECT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<IntersectExprArgs>;

  declare args: IntersectExprArgs;

  constructor (args: IntersectExprArgs) {
    super(args);
  }
}

/**
 * VALUES clause with DuckDB support for ORDER BY, LIMIT, OFFSET
 * @see {@link https://duckdb.org/docs/stable/sql/query_syntax/limit | DuckDB LIMIT}
 */
export type ValuesExprArgs = {
  expressions: Expression[];
  alias?: TableAliasExpr;
  order?: Expression;
  limit?: number | Expression;
  offset?: number | Expression;
} & UDTFExprArgs;

export class ValuesExpr extends UDTFExpr {
  key = ExpressionKey.VALUES;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
    alias: false,
    order: false,
    limit: false,
    offset: false,
  } satisfies RequiredMap<ValuesExprArgs>;

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

export type SubqueryPredicateExprArgs = PredicateExprArgs;

export class SubqueryPredicateExpr extends PredicateExpr {
  key = ExpressionKey.SUBQUERY_PREDICATE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SubqueryPredicateExprArgs>;

  declare args: SubqueryPredicateExprArgs;

  constructor (args: SubqueryPredicateExprArgs) {
    super(args);
  }
}

export type AddExprArgs = BinaryExprArgs;

export class AddExpr extends BinaryExpr {
  key = ExpressionKey.ADD;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AddExprArgs>;

  declare args: AddExprArgs;

  constructor (args: AddExprArgs) {
    super(args);
  }
}

export type ConnectorExprArgs = BinaryExprArgs;

export class ConnectorExpr extends BinaryExpr {
  key = ExpressionKey.CONNECTOR;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ConnectorExprArgs>;

  declare args: ConnectorExprArgs;

  constructor (args: ConnectorExprArgs) {
    super(args);
  }
}

export type BitwiseAndExprArgs = {
  this: Expression;
  expression: Expression;
  padside?: Expression;
} & BinaryExprArgs;

export class BitwiseAndExpr extends BinaryExpr {
  key = ExpressionKey.BITWISE_AND;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    padside: false,
  } satisfies RequiredMap<BitwiseAndExprArgs>;

  declare args: BitwiseAndExprArgs;

  constructor (args: BitwiseAndExprArgs) {
    super(args);
  }

  get $padside (): Expression | undefined {
    return this.args.padside;
  }
}

export type BitwiseLeftShiftExprArgs = {
  this: Expression;
  expression: Expression;
  requiresInt128?: Expression;
} & BinaryExprArgs;

export class BitwiseLeftShiftExpr extends BinaryExpr {
  key = ExpressionKey.BITWISE_LEFT_SHIFT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    requiresInt128: false,
  } satisfies RequiredMap<BitwiseLeftShiftExprArgs>;

  declare args: BitwiseLeftShiftExprArgs;

  constructor (args: BitwiseLeftShiftExprArgs) {
    super(args);
  }

  get $requiresInt128 (): Expression | undefined {
    return this.args.requiresInt128;
  }
}

export type BitwiseOrExprArgs = {
  this: Expression;
  expression: Expression;
  padside?: Expression;
} & BinaryExprArgs;

export class BitwiseOrExpr extends BinaryExpr {
  key = ExpressionKey.BITWISE_OR;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    padside: false,
  } satisfies RequiredMap<BitwiseOrExprArgs>;

  declare args: BitwiseOrExprArgs;

  constructor (args: BitwiseOrExprArgs) {
    super(args);
  }

  get $padside (): Expression | undefined {
    return this.args.padside;
  }
}

export type BitwiseRightShiftExprArgs = {
  this: Expression;
  expression: Expression;
  requiresInt128?: Expression;
} & BinaryExprArgs;

export class BitwiseRightShiftExpr extends BinaryExpr {
  key = ExpressionKey.BITWISE_RIGHT_SHIFT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    requiresInt128: false,
  } satisfies RequiredMap<BitwiseRightShiftExprArgs>;

  declare args: BitwiseRightShiftExprArgs;

  constructor (args: BitwiseRightShiftExprArgs) {
    super(args);
  }

  get $requiresInt128 (): Expression | undefined {
    return this.args.requiresInt128;
  }
}

export type BitwiseXorExprArgs = {
  this: Expression;
  expression: Expression;
  padside?: Expression;
} & BinaryExprArgs;

export class BitwiseXorExpr extends BinaryExpr {
  key = ExpressionKey.BITWISE_XOR;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    padside: false,
  } satisfies RequiredMap<BitwiseXorExprArgs>;

  declare args: BitwiseXorExprArgs;

  constructor (args: BitwiseXorExprArgs) {
    super(args);
  }

  get $padside (): Expression | undefined {
    return this.args.padside;
  }
}

export type DivExprArgs = {
  this: Expression;
  expression: Expression;
  typed?: DataTypeExpr;
  safe?: boolean;
} & BinaryExprArgs;

export class DivExpr extends BinaryExpr {
  key = ExpressionKey.DIV;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    typed: false,
    safe: false,
  } satisfies RequiredMap<DivExprArgs>;

  declare args: DivExprArgs;

  constructor (args: DivExprArgs) {
    super(args);
  }

  get $typed (): Expression | undefined {
    return this.args.typed;
  }

  get $safe (): Expression | undefined {
    return this.args.safe;
  }
}

export type OverlapsExprArgs = BinaryExprArgs;

export class OverlapsExpr extends BinaryExpr {
  key = ExpressionKey.OVERLAPS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<OverlapsExprArgs>;

  declare args: OverlapsExprArgs;

  constructor (args: OverlapsExprArgs) {
    super(args);
  }
}

export type ExtendsLeftExprArgs = BinaryExprArgs;

export class ExtendsLeftExpr extends BinaryExpr {
  key = ExpressionKey.EXTENDS_LEFT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ExtendsLeftExprArgs>;

  declare args: ExtendsLeftExprArgs;

  constructor (args: ExtendsLeftExprArgs) {
    super(args);
  }
}

export type ExtendsRightExprArgs = BinaryExprArgs;

export class ExtendsRightExpr extends BinaryExpr {
  key = ExpressionKey.EXTENDS_RIGHT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ExtendsRightExprArgs>;

  declare args: ExtendsRightExprArgs;

  constructor (args: ExtendsRightExprArgs) {
    super(args);
  }
}

export type DotExprArgs = BinaryExprArgs;

export class DotExpr extends BinaryExpr {
  key = ExpressionKey.DOT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DotExprArgs>;

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
    );
  }

  /**
   * Return the parts of a table / column in order catalog, db, table.
   */
  get parts (): Expression[] {
    const [thisExpr, ...restParts] = this.flatten();
    const parts = [...restParts];

    parts.reverse();

    for (const arg of COLUMN_PARTS) {
      const part = thisExpr.args[arg];

      if (part instanceof Expression) {
        parts.push(part);
      }
    }

    parts.reverse();
    return parts;
  }
}

export type DPipeExprArgs = {
  this: Expression;
  expression: Expression;
  safe?: boolean;
} & BinaryExprArgs;

export class DPipeExpr extends BinaryExpr {
  key = ExpressionKey.D_PIPE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    safe: false,
  } satisfies RequiredMap<DPipeExprArgs>;

  declare args: DPipeExprArgs;

  constructor (args: DPipeExprArgs) {
    super(args);
  }

  get $safe (): boolean | undefined {
    return this.args.safe;
  }
}

export type EQExprArgs = BinaryExprArgs & PredicateExprArgs;

export class EQExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.EQ;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<EQExprArgs>;

  declare args: EQExprArgs;

  constructor (args: EQExprArgs) {
    super(args);
  }
}

export type NullSafeEQExprArgs = BinaryExprArgs & PredicateExprArgs;

export class NullSafeEQExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.NULL_SAFE_EQ;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<NullSafeEQExprArgs>;

  declare args: NullSafeEQExprArgs;

  constructor (args: NullSafeEQExprArgs) {
    super(args);
  }
}

export type NullSafeNEQExprArgs = BinaryExprArgs & PredicateExprArgs;

export class NullSafeNEQExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.NULL_SAFE_NEQ;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<NullSafeNEQExprArgs>;

  declare args: NullSafeNEQExprArgs;

  constructor (args: NullSafeNEQExprArgs) {
    super(args);
  }
}

export type PropertyEQExprArgs = BinaryExprArgs;

export class PropertyEQExpr extends BinaryExpr {
  key = ExpressionKey.PROPERTY_EQ;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<PropertyEQExprArgs>;

  declare args: PropertyEQExprArgs;

  constructor (args: PropertyEQExprArgs) {
    super(args);
  }
}

export type DistanceExprArgs = BinaryExprArgs;

export class DistanceExpr extends BinaryExpr {
  key = ExpressionKey.DISTANCE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DistanceExprArgs>;

  declare args: DistanceExprArgs;

  constructor (args: DistanceExprArgs) {
    super(args);
  }
}

export type EscapeExprArgs = BinaryExprArgs;

export class EscapeExpr extends BinaryExpr {
  key = ExpressionKey.ESCAPE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<EscapeExprArgs>;

  declare args: EscapeExprArgs;

  constructor (args: EscapeExprArgs) {
    super(args);
  }
}

export type GlobExprArgs = BinaryExprArgs & PredicateExprArgs;

export class GlobExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.GLOB;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<GlobExprArgs>;

  declare args: GlobExprArgs;

  constructor (args: GlobExprArgs) {
    super(args);
  }
}

export type GTExprArgs = BinaryExprArgs & PredicateExprArgs;

export class GTExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.GT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<GTExprArgs>;

  declare args: GTExprArgs;

  constructor (args: GTExprArgs) {
    super(args);
  }
}

export type GTEExprArgs = BinaryExprArgs & PredicateExprArgs;

export class GTEExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.GTE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<GTEExprArgs>;

  declare args: GTEExprArgs;

  constructor (args: GTEExprArgs) {
    super(args);
  }
}

export type ILikeExprArgs = BinaryExprArgs & PredicateExprArgs;

export class ILikeExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.ILIKE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ILikeExprArgs>;

  declare args: ILikeExprArgs;

  constructor (args: ILikeExprArgs) {
    super(args);
  }
}

export type IntDivExprArgs = BinaryExprArgs;

export class IntDivExpr extends BinaryExpr {
  key = ExpressionKey.INT_DIV;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<IntDivExprArgs>;

  declare args: IntDivExprArgs;

  constructor (args: IntDivExprArgs) {
    super(args);
  }
}

export type IsExprArgs = BinaryExprArgs & PredicateExprArgs;

export class IsExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.IS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<IsExprArgs>;

  declare args: IsExprArgs;

  constructor (args: IsExprArgs) {
    super(args);
  }
}

export type KwargExprArgs = BinaryExprArgs;

/**
 * Kwarg in special functions like func(kwarg => y).
 */
export class KwargExpr extends BinaryExpr {
  key = ExpressionKey.KWARG;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<KwargExprArgs>;

  declare args: KwargExprArgs;

  constructor (args: KwargExprArgs) {
    super(args);
  }
}

export type LikeExprArgs = BinaryExprArgs & PredicateExprArgs;

export class LikeExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.LIKE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<LikeExprArgs>;

  declare args: LikeExprArgs;

  constructor (args: LikeExprArgs) {
    super(args);
  }
}

export type MatchExprArgs = BinaryExprArgs & PredicateExprArgs;

export class MatchExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.MATCH;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<MatchExprArgs>;

  declare args: MatchExprArgs;

  constructor (args: MatchExprArgs) {
    super(args);
  }
}

export type LTExprArgs = BinaryExprArgs & PredicateExprArgs;

export class LTExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.LT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<LTExprArgs>;

  declare args: LTExprArgs;

  constructor (args: LTExprArgs) {
    super(args);
  }
}

export type LTEExprArgs = BinaryExprArgs & PredicateExprArgs;

export class LTEExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.LTE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<LTEExprArgs>;

  declare args: LTEExprArgs;

  constructor (args: LTEExprArgs) {
    super(args);
  }
}

export type ModExprArgs = BinaryExprArgs;

export class ModExpr extends BinaryExpr {
  key = ExpressionKey.MOD;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ModExprArgs>;

  declare args: ModExprArgs;

  constructor (args: ModExprArgs) {
    super(args);
  }
}

export type MulExprArgs = BinaryExprArgs;

export class MulExpr extends BinaryExpr {
  key = ExpressionKey.MUL;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<MulExprArgs>;

  declare args: MulExprArgs;

  constructor (args: MulExprArgs) {
    super(args);
  }
}

export type NEQExprArgs = BinaryExprArgs & PredicateExprArgs;

export class NEQExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.NEQ;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<NEQExprArgs>;

  declare args: NEQExprArgs;

  constructor (args: NEQExprArgs) {
    super(args);
  }
}

export type OperatorExprArgs = {
  this: Expression;
  operator: Expression;
  expression: Expression;
} & BinaryExprArgs;

export class OperatorExpr extends BinaryExpr {
  key = ExpressionKey.OPERATOR;

  static argTypes = {
    ...super.argTypes,
    this: true,
    operator: true,
    expression: true,
  } satisfies RequiredMap<OperatorExprArgs>;

  declare args: OperatorExprArgs;

  constructor (args: OperatorExprArgs) {
    super(args);
  }

  get $operator (): Expression {
    return this.args.operator;
  }
}

export type SimilarToExprArgs = BinaryExprArgs & PredicateExprArgs;

export class SimilarToExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  key = ExpressionKey.SIMILAR_TO;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SimilarToExprArgs>;

  declare args: SimilarToExprArgs;

  constructor (args: SimilarToExprArgs) {
    super(args);
  }
}

export type SubExprArgs = BinaryExprArgs;

export class SubExpr extends BinaryExpr {
  key = ExpressionKey.SUB;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SubExprArgs>;

  declare args: SubExprArgs;

  constructor (args: SubExprArgs) {
    super(args);
  }
}

export type AdjacentExprArgs = BinaryExprArgs;

export class AdjacentExpr extends BinaryExpr {
  key = ExpressionKey.ADJACENT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AdjacentExprArgs>;

  declare args: AdjacentExprArgs;

  constructor (args: AdjacentExprArgs) {
    super(args);
  }
}

export type BitwiseNotExprArgs = UnaryExprArgs;

export class BitwiseNotExpr extends UnaryExpr {
  key = ExpressionKey.BITWISE_NOT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<BitwiseNotExprArgs>;

  declare args: BitwiseNotExprArgs;

  constructor (args: BitwiseNotExprArgs) {
    super(args);
  }
}

export type NotExprArgs = UnaryExprArgs;

export class NotExpr extends UnaryExpr {
  key = ExpressionKey.NOT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<NotExprArgs>;

  declare args: NotExprArgs;

  constructor (args: NotExprArgs) {
    super(args);
  }
}

export type ParenExprArgs = UnaryExprArgs;

export class ParenExpr extends UnaryExpr {
  key = ExpressionKey.PAREN;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ParenExprArgs>;

  declare args: ParenExprArgs;

  constructor (args: ParenExprArgs) {
    super(args);
  }

  get outputName (): string {
    return this.args.this.name;
  }
}

export type NegExprArgs = UnaryExprArgs;

export class NegExpr extends UnaryExpr {
  key = ExpressionKey.NEG;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<NegExprArgs>;

  declare args: NegExprArgs;

  constructor (args: NegExprArgs) {
    super(args);
  }

  toValue (): number {
    if (this.isNumber) {
      return this.args.this.toValue() * -1;
    }
    return super.toValue();
  }
}

export type BetweenExprArgs = {
  this: Expression;
  low: Expression;
  high: Expression;
  symmetric?: Expression;
} & PredicateExprArgs;

export class BetweenExpr extends PredicateExpr {
  key = ExpressionKey.BETWEEN;

  static argTypes = {
    ...super.argTypes,
    this: true,
    low: true,
    high: true,
    symmetric: false,
  } satisfies RequiredMap<BetweenExprArgs>;

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

export type InExprArgs = {
  this: Expression;
  expressions?: Expression[];
  query?: Expression;
  unnest?: UnnestExpr;
  field?: Expression;
  isGlobal?: boolean;
} & PredicateExprArgs;

export class InExpr extends PredicateExpr {
  key = ExpressionKey.IN;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
    query: false,
    unnest: false,
    field: false,
    isGlobal: false,
  } satisfies RequiredMap<InExprArgs>;

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
export type SafeFuncExprArgs = FuncExprArgs;

export class SafeFuncExpr extends FuncExpr {
  key = ExpressionKey.SAFE_FUNC;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SafeFuncExprArgs>;

  declare args: SafeFuncExprArgs;

  constructor (args: SafeFuncExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TypeofExprArgs = FuncExprArgs;

export class TypeofExpr extends FuncExpr {
  key = ExpressionKey.TYPEOF;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TypeofExprArgs>;

  declare args: TypeofExprArgs;

  constructor (args: TypeofExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AcosExprArgs = FuncExprArgs;

export class AcosExpr extends FuncExpr {
  key = ExpressionKey.ACOS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AcosExprArgs>;

  declare args: AcosExprArgs;

  constructor (args: AcosExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AcoshExprArgs = FuncExprArgs;

export class AcoshExpr extends FuncExpr {
  key = ExpressionKey.ACOSH;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AcoshExprArgs>;

  declare args: AcoshExprArgs;

  constructor (args: AcoshExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AsinExprArgs = FuncExprArgs;

export class AsinExpr extends FuncExpr {
  key = ExpressionKey.ASIN;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AsinExprArgs>;

  declare args: AsinExprArgs;

  constructor (args: AsinExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AsinhExprArgs = FuncExprArgs;

export class AsinhExpr extends FuncExpr {
  key = ExpressionKey.ASINH;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AsinhExprArgs>;

  declare args: AsinhExprArgs;

  constructor (args: AsinhExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AtanExprArgs = {
  this: Expression;
  expression?: Expression;
} & FuncExprArgs;

export class AtanExpr extends FuncExpr {
  key = ExpressionKey.ATAN;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<AtanExprArgs>;

  declare args: AtanExprArgs;

  constructor (args: AtanExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type AtanhExprArgs = FuncExprArgs;

export class AtanhExpr extends FuncExpr {
  key = ExpressionKey.ATANH;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AtanhExprArgs>;

  declare args: AtanhExprArgs;

  constructor (args: AtanhExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Atan2ExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class Atan2Expr extends FuncExpr {
  key = ExpressionKey.ATAN2;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<Atan2ExprArgs>;

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

export type CotExprArgs = FuncExprArgs;

export class CotExpr extends FuncExpr {
  key = ExpressionKey.COT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CotExprArgs>;

  declare args: CotExprArgs;

  constructor (args: CotExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CothExprArgs = FuncExprArgs;

export class CothExpr extends FuncExpr {
  key = ExpressionKey.COTH;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CothExprArgs>;

  declare args: CothExprArgs;

  constructor (args: CothExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CosExprArgs = FuncExprArgs;

export class CosExpr extends FuncExpr {
  key = ExpressionKey.COS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CosExprArgs>;

  declare args: CosExprArgs;

  constructor (args: CosExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CscExprArgs = FuncExprArgs;

export class CscExpr extends FuncExpr {
  key = ExpressionKey.CSC;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CscExprArgs>;

  declare args: CscExprArgs;

  constructor (args: CscExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CschExprArgs = FuncExprArgs;

export class CschExpr extends FuncExpr {
  key = ExpressionKey.CSCH;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CschExprArgs>;

  declare args: CschExprArgs;

  constructor (args: CschExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SecExprArgs = FuncExprArgs;

export class SecExpr extends FuncExpr {
  key = ExpressionKey.SEC;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SecExprArgs>;

  declare args: SecExprArgs;

  constructor (args: SecExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SechExprArgs = FuncExprArgs;

export class SechExpr extends FuncExpr {
  key = ExpressionKey.SECH;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SechExprArgs>;

  declare args: SechExprArgs;

  constructor (args: SechExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SinExprArgs = FuncExprArgs;

export class SinExpr extends FuncExpr {
  key = ExpressionKey.SIN;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SinExprArgs>;

  declare args: SinExprArgs;

  constructor (args: SinExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SinhExprArgs = FuncExprArgs;

export class SinhExpr extends FuncExpr {
  key = ExpressionKey.SINH;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SinhExprArgs>;

  declare args: SinhExprArgs;

  constructor (args: SinhExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TanExprArgs = FuncExprArgs;

export class TanExpr extends FuncExpr {
  key = ExpressionKey.TAN;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TanExprArgs>;

  declare args: TanExprArgs;

  constructor (args: TanExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TanhExprArgs = FuncExprArgs;

export class TanhExpr extends FuncExpr {
  key = ExpressionKey.TANH;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TanhExprArgs>;

  declare args: TanhExprArgs;

  constructor (args: TanhExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DegreesExprArgs = FuncExprArgs;

export class DegreesExpr extends FuncExpr {
  key = ExpressionKey.DEGREES;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DegreesExprArgs>;

  declare args: DegreesExprArgs;

  constructor (args: DegreesExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CoshExprArgs = FuncExprArgs;

export class CoshExpr extends FuncExpr {
  key = ExpressionKey.COSH;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CoshExprArgs>;

  declare args: CoshExprArgs;

  constructor (args: CoshExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CosineDistanceExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class CosineDistanceExpr extends FuncExpr {
  key = ExpressionKey.COSINE_DISTANCE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<CosineDistanceExprArgs>;

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

export type DotProductExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class DotProductExpr extends FuncExpr {
  key = ExpressionKey.DOT_PRODUCT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<DotProductExprArgs>;

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

export type EuclideanDistanceExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class EuclideanDistanceExpr extends FuncExpr {
  key = ExpressionKey.EUCLIDEAN_DISTANCE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<EuclideanDistanceExprArgs>;

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

export type ManhattanDistanceExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class ManhattanDistanceExpr extends FuncExpr {
  key = ExpressionKey.MANHATTAN_DISTANCE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<ManhattanDistanceExprArgs>;

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

export type JarowinklerSimilarityExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class JarowinklerSimilarityExpr extends FuncExpr {
  key = ExpressionKey.JAROWINKLER_SIMILARITY;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<JarowinklerSimilarityExprArgs>;

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

export type AggFuncExprArgs = BaseExpressionArgs;

export class AggFuncExpr extends FuncExpr {
  key = ExpressionKey.AGG_FUNC;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AggFuncExprArgs>;

  declare args: AggFuncExprArgs;

  constructor (args: AggFuncExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitwiseCountExprArgs = FuncExprArgs;

export class BitwiseCountExpr extends FuncExpr {
  key = ExpressionKey.BITWISE_COUNT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<BitwiseCountExprArgs>;

  declare args: BitwiseCountExprArgs;

  constructor (args: BitwiseCountExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitmapBucketNumberExprArgs = FuncExprArgs;

export class BitmapBucketNumberExpr extends FuncExpr {
  key = ExpressionKey.BITMAP_BUCKET_NUMBER;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<BitmapBucketNumberExprArgs>;

  declare args: BitmapBucketNumberExprArgs;

  constructor (args: BitmapBucketNumberExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitmapCountExprArgs = FuncExprArgs;

export class BitmapCountExpr extends FuncExpr {
  key = ExpressionKey.BITMAP_COUNT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<BitmapCountExprArgs>;

  declare args: BitmapCountExprArgs;

  constructor (args: BitmapCountExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitmapBitPositionExprArgs = FuncExprArgs;

export class BitmapBitPositionExpr extends FuncExpr {
  key = ExpressionKey.BITMAP_BIT_POSITION;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<BitmapBitPositionExprArgs>;

  declare args: BitmapBitPositionExprArgs;

  constructor (args: BitmapBitPositionExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ByteLengthExprArgs = FuncExprArgs;

export class ByteLengthExpr extends FuncExpr {
  key = ExpressionKey.BYTE_LENGTH;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ByteLengthExprArgs>;

  declare args: ByteLengthExprArgs;

  constructor (args: ByteLengthExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BoolnotExprArgs = {
  this: Expression;
  roundInput?: Expression;
} & FuncExprArgs;

export class BoolnotExpr extends FuncExpr {
  key = ExpressionKey.BOOLNOT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    roundInput: false,
  } satisfies RequiredMap<BoolnotExprArgs>;

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

export type BoolandExprArgs = {
  this: Expression;
  expression: Expression;
  roundInput?: Expression;
} & FuncExprArgs;

export class BoolandExpr extends FuncExpr {
  key = ExpressionKey.BOOLAND;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    roundInput: false,
  } satisfies RequiredMap<BoolandExprArgs>;

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

export type BoolorExprArgs = {
  this: Expression;
  expression: Expression;
  roundInput?: Expression;
} & FuncExprArgs;

export class BoolorExpr extends FuncExpr {
  key = ExpressionKey.BOOLOR;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    roundInput: false,
  } satisfies RequiredMap<BoolorExprArgs>;

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
export type JSONBoolExprArgs = FuncExprArgs;

export class JSONBoolExpr extends FuncExpr {
  key = ExpressionKey.JSON_BOOL;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<JSONBoolExprArgs>;

  declare args: JSONBoolExprArgs;

  constructor (args: JSONBoolExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayRemoveExprArgs = {
  this: Expression;
  expression: Expression;
  nullPropagation?: Expression;
} & FuncExprArgs;

export class ArrayRemoveExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_REMOVE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    nullPropagation: false,
  } satisfies RequiredMap<ArrayRemoveExprArgs>;

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

  get $nullPropagation (): Expression | undefined {
    return this.args.nullPropagation;
  }

  static {
    this.register();
  }
}

export type AbsExprArgs = FuncExprArgs;

export class AbsExpr extends FuncExpr {
  key = ExpressionKey.ABS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AbsExprArgs>;

  declare args: AbsExprArgs;

  constructor (args: AbsExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ApproxTopKEstimateExprArgs = {
  this: Expression;
  expression?: Expression;
} & FuncExprArgs;

export class ApproxTopKEstimateExpr extends FuncExpr {
  key = ExpressionKey.APPROX_TOP_K_ESTIMATE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<ApproxTopKEstimateExprArgs>;

  declare args: ApproxTopKEstimateExprArgs;

  constructor (args: ApproxTopKEstimateExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type FarmFingerprintExprArgs = {
  expressions: Expression[];
} & FuncExprArgs;

export class FarmFingerprintExpr extends FuncExpr {
  key = ExpressionKey.FARM_FINGERPRINT;

  static isVarLenArgs = true;

  static _sqlNames = ['FARM_FINGERPRINT', 'FARMFINGERPRINT64'];

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<FarmFingerprintExprArgs>;

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

  static sqlNames = ['FARM_FINGERPRINT', 'FARMFINGERPRINT64'];
}

export type FlattenExprArgs = FuncExprArgs;

export class FlattenExpr extends FuncExpr {
  key = ExpressionKey.FLATTEN;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<FlattenExprArgs>;

  declare args: FlattenExprArgs;

  constructor (args: FlattenExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Float64ExprArgs = {
  this: Expression;
  expression?: Expression;
} & FuncExprArgs;

export class Float64Expr extends FuncExpr {
  key = ExpressionKey.FLOAT64;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<Float64ExprArgs>;

  declare args: Float64ExprArgs;

  constructor (args: Float64ExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

/**
 * https://spark.apache.org/docs/latest/api/sql/index.html#transform
 */
export type TransformExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class TransformExpr extends FuncExpr {
  key = ExpressionKey.TRANSFORM;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<TransformExprArgs>;

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

export type TranslateExprArgs = {
  this: Expression;
  from: Expression;
  to: Expression;
} & FuncExprArgs;

export class TranslateExpr extends FuncExpr {
  key = ExpressionKey.TRANSLATE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    from: true,
    to: true,
  } satisfies RequiredMap<TranslateExprArgs>;

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

export type AnonymousExprArgs = {
  this: Expression;
  expressions?: Expression[];
} & FuncExprArgs;

export class AnonymousExpr extends FuncExpr {
  key = ExpressionKey.ANONYMOUS;

  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
  } satisfies RequiredMap<AnonymousExprArgs>;

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

export type ApplyExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class ApplyExpr extends FuncExpr {
  key = ExpressionKey.APPLY;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<ApplyExprArgs>;

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

export type ArrayExprArgs = {
  expressions?: Expression[];
  bracketNotation?: Expression;
  structNameInheritance?: string;
} & FuncExprArgs;

export class ArrayExpr extends FuncExpr {
  key = ExpressionKey.ARRAY;

  static isVarLenArgs = true;

  /**
   * Defines the arguments (properties and child expressions) for Array expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    expressions: false,
    bracketNotation: false,
    structNameInheritance: false,
  } satisfies RequiredMap<ArrayExprArgs>;

  declare args: ArrayExprArgs;

  constructor (args: ArrayExprArgs) {
    super(args);
  }

  get $expressions (): Expression[] | undefined {
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

export type AsciiExprArgs = FuncExprArgs;

export class AsciiExpr extends FuncExpr {
  key = ExpressionKey.ASCII;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AsciiExprArgs>;

  declare args: AsciiExprArgs;

  constructor (args: AsciiExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToArrayExprArgs = FuncExprArgs;

export class ToArrayExpr extends FuncExpr {
  key = ExpressionKey.TO_ARRAY;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ToArrayExprArgs>;

  declare args: ToArrayExprArgs;

  constructor (args: ToArrayExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToBooleanExprArgs = {
  this: Expression;
  safe?: boolean;
} & FuncExprArgs;

export class ToBooleanExpr extends FuncExpr {
  key = ExpressionKey.TO_BOOLEAN;

  static argTypes = {
    ...super.argTypes,
    this: true,
    safe: false,
  } satisfies RequiredMap<ToBooleanExprArgs>;

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

export type ListExprArgs = {
  expressions?: Expression[];
} & FuncExprArgs;

export class ListExpr extends FuncExpr {
  key = ExpressionKey.LIST;

  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    expressions: false,
  } satisfies RequiredMap<ListExprArgs>;

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
export type PadExprArgs = {
  this: Expression;
  expression: Expression;
  fillPattern?: Expression;
  isLeft: boolean;
} & FuncExprArgs;

export class PadExpr extends FuncExpr {
  key = ExpressionKey.PAD;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    fillPattern: false,
    isLeft: true,
  } satisfies RequiredMap<PadExprArgs>;

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
export type ToCharExprArgs = {
  this: Expression;
  format?: Expression;
  nlsparam?: Expression;
  isNumeric?: Expression;
} & FuncExprArgs;

export class ToCharExpr extends FuncExpr {
  key = ExpressionKey.TO_CHAR;

  static argTypes = {
    ...super.argTypes,
    this: true,
    format: false,
    nlsparam: false,
    isNumeric: false,
  } satisfies RequiredMap<ToCharExprArgs>;

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

export type ToCodePointsExprArgs = FuncExprArgs;

export class ToCodePointsExpr extends FuncExpr {
  key = ExpressionKey.TO_CODE_POINTS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ToCodePointsExprArgs>;

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
export type ToNumberExprArgs = {
  this: Expression;
  format?: Expression;
  nlsparam?: Expression;
  precision?: Expression;
  scale?: Expression;
  safe?: boolean;
  safeName?: string;
} & FuncExprArgs;

export class ToNumberExpr extends FuncExpr {
  key = ExpressionKey.TO_NUMBER;

  static argTypes = {
    ...super.argTypes,
    this: true,
    format: false,
    nlsparam: false,
    precision: false,
    scale: false,
    safe: false,
    safeName: false,
  } satisfies RequiredMap<ToNumberExprArgs>;

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

export type ToDoubleExprArgs = {
  this: Expression;
  format?: Expression;
  safe?: boolean;
} & FuncExprArgs;

export class ToDoubleExpr extends FuncExpr {
  key = ExpressionKey.TO_DOUBLE;

  /**
   * Defines the arguments (properties and child expressions) for ToDouble expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    format: false,
    safe: false,
  } satisfies RequiredMap<ToDoubleExprArgs>;

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

export type ToDecfloatExprArgs = {
  this: Expression;
  format?: Expression;
} & FuncExprArgs;

export class ToDecfloatExpr extends FuncExpr {
  key = ExpressionKey.TO_DECFLOAT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    format: false,
  } satisfies RequiredMap<ToDecfloatExprArgs>;

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

export type TryToDecfloatExprArgs = {
  this: Expression;
  format?: Expression;
} & FuncExprArgs;

export class TryToDecfloatExpr extends FuncExpr {
  key = ExpressionKey.TRY_TO_DECFLOAT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    format: false,
  } satisfies RequiredMap<TryToDecfloatExprArgs>;

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

export type ToFileExprArgs = {
  this: Expression;
  path?: Expression;
  safe?: boolean;
} & FuncExprArgs;

export class ToFileExpr extends FuncExpr {
  key = ExpressionKey.TO_FILE;

  /**
   * Defines the arguments (properties and child expressions) for ToFile expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    path: false,
    safe: false,
  } satisfies RequiredMap<ToFileExprArgs>;

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

export type CodePointsToBytesExprArgs = FuncExprArgs;

export class CodePointsToBytesExpr extends FuncExpr {
  key = ExpressionKey.CODE_POINTS_TO_BYTES;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CodePointsToBytesExprArgs>;

  declare args: CodePointsToBytesExprArgs;

  constructor (args: CodePointsToBytesExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ColumnsExprArgs = {
  this: Expression;
  unpack?: Expression;
} & FuncExprArgs;

export class ColumnsExpr extends FuncExpr {
  key = ExpressionKey.COLUMNS;

  static argTypes = {
    ...super.argTypes,
    this: true,
    unpack: false,
  } satisfies RequiredMap<ColumnsExprArgs>;

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

export type ConvertExprArgs = {
  this: Expression;
  expression: Expression;
  style?: Expression;
  safe?: boolean;
} & FuncExprArgs;

export class ConvertExpr extends FuncExpr {
  key = ExpressionKey.CONVERT;

  /**
   * Defines the arguments (properties and child expressions) for Convert expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    style: false,
    safe: false,
  } satisfies RequiredMap<ConvertExprArgs>;

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

export type ConvertToCharsetExprArgs = {
  this: Expression;
  dest: Expression;
  source?: Expression;
} & FuncExprArgs;

export class ConvertToCharsetExpr extends FuncExpr {
  key = ExpressionKey.CONVERT_TO_CHARSET;

  /**
   * Defines the arguments (properties and child expressions) for ConvertToCharset expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    dest: true,
    source: false,
  } satisfies RequiredMap<ConvertToCharsetExprArgs>;

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

export type ConvertTimezoneExprArgs = {
  sourceTz?: Expression;
  targetTz: Expression;
  timestamp: Expression;
  options?: Expression[];
} & FuncExprArgs;

export class ConvertTimezoneExpr extends FuncExpr {
  key = ExpressionKey.CONVERT_TIMEZONE;

  /**
   * Defines the arguments (properties and child expressions) for ConvertTimezone expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    sourceTz: false,
    targetTz: true,
    timestamp: true,
    options: false,
  } satisfies RequiredMap<ConvertTimezoneExprArgs>;

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

export type CodePointsToStringExprArgs = FuncExprArgs;

export class CodePointsToStringExpr extends FuncExpr {
  key = ExpressionKey.CODE_POINTS_TO_STRING;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CodePointsToStringExprArgs>;

  declare args: CodePointsToStringExprArgs;

  constructor (args: CodePointsToStringExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type GenerateSeriesExprArgs = {
  start: Expression;
  end: Expression;
  step?: Expression;
  isEndExclusive?: Expression;
} & FuncExprArgs;

export class GenerateSeriesExpr extends FuncExpr {
  key = ExpressionKey.GENERATE_SERIES;

  static argTypes = {
    ...super.argTypes,
    start: true,
    end: true,
    step: false,
    isEndExclusive: false,
  } satisfies RequiredMap<GenerateSeriesExprArgs>;

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
export type GeneratorExprArgs = {
  rowcount?: Expression;
  timelimit?: Expression;
} & FuncExprArgs & UDTFExprArgs;

export class GeneratorExpr extends multiInherit(FuncExpr, UDTFExpr) {
  key = ExpressionKey.GENERATOR;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    rowcount: false,
    timelimit: false,
  } satisfies RequiredMap<GeneratorExprArgs>;

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

export type AIClassifyExprArgs = {
  this: Expression;
  categories: Expression;
  config?: Expression;
} & FuncExprArgs;

export class AIClassifyExpr extends FuncExpr {
  key = ExpressionKey.AI_CLASSIFY;

  static _sqlNames = ['AI_CLASSIFY'];

  static argTypes = {
    ...super.argTypes,
    this: true,
    categories: true,
    config: false,
  } satisfies RequiredMap<AIClassifyExprArgs>;

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

  static sqlNames = ['AI_CLASSIFY'];
}

export type ArrayAllExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class ArrayAllExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_ALL;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<ArrayAllExprArgs>;

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
export type ArrayAnyExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class ArrayAnyExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_ANY;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<ArrayAnyExprArgs>;

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

export type ArrayAppendExprArgs = {
  this: Expression;
  expression: Expression;
  nullPropagation?: Expression;
} & FuncExprArgs;

export class ArrayAppendExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_APPEND;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    nullPropagation: false,
  } satisfies RequiredMap<ArrayAppendExprArgs>;

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

  get $nullPropagation (): Expression | undefined {
    return this.args.nullPropagation;
  }

  static {
    this.register();
  }
}

export type ArrayPrependExprArgs = {
  this: Expression;
  expression: Expression;
  nullPropagation?: Expression;
} & FuncExprArgs;

export class ArrayPrependExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_PREPEND;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    nullPropagation: false,
  } satisfies RequiredMap<ArrayPrependExprArgs>;

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

  get $nullPropagation (): Expression | undefined {
    return this.args.nullPropagation;
  }

  static {
    this.register();
  }
}

export type ArrayConcatExprArgs = {
  this: Expression;
  expressions?: Expression[];
  nullPropagation?: Expression;
} & FuncExprArgs;

export class ArrayConcatExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_CONCAT;

  static _sqlNames = ['ARRAY_CONCAT', 'ARRAY_CAT'];

  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
    nullPropagation: false,
  } satisfies RequiredMap<ArrayConcatExprArgs>;

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

  get $nullPropagation (): Expression | undefined {
    return this.args.nullPropagation;
  }

  static {
    this.register();
  }

  static sqlNames = ['ARRAY_CONCAT', 'ARRAY_CAT'];
}

export type ArrayCompactExprArgs = FuncExprArgs;

export class ArrayCompactExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_COMPACT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ArrayCompactExprArgs>;

  declare args: ArrayCompactExprArgs;

  constructor (args: ArrayCompactExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayInsertExprArgs = {
  this: Expression;
  position: Expression;
  expression: Expression;
  offset?: Expression;
} & FuncExprArgs;

export class ArrayInsertExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_INSERT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    position: true,
    expression: true,
    offset: false,
  } satisfies RequiredMap<ArrayInsertExprArgs>;

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

export type ArrayRemoveAtExprArgs = {
  this: Expression;
  position: Expression;
} & FuncExprArgs;

export class ArrayRemoveAtExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_REMOVE_AT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    position: true,
  } satisfies RequiredMap<ArrayRemoveAtExprArgs>;

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

export type ArrayConstructCompactExprArgs = {
  expressions?: Expression[];
} & FuncExprArgs;

export class ArrayConstructCompactExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_CONSTRUCT_COMPACT;

  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    expressions: false,
  } satisfies RequiredMap<ArrayConstructCompactExprArgs>;

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

export type ArrayContainsExprArgs = {
  this: Expression;
  expression: Expression;
  ensureVariant?: Expression;
} & BinaryExprArgs & FuncExprArgs;

export class ArrayContainsExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.ARRAY_CONTAINS;

  static _sqlNames = ['ARRAY_CONTAINS', 'ARRAY_HAS'];

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    ensureVariant: false,
  } satisfies RequiredMap<ArrayContainsExprArgs>;

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

export type ArrayContainsAllExprArgs = {
  this: Expression;
  expression: Expression;
} & BinaryExprArgs & FuncExprArgs;

export class ArrayContainsAllExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.ARRAY_CONTAINS_ALL;

  static _sqlNames = ['ARRAY_CONTAINS_ALL', 'ARRAY_HAS_ALL'];

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<ArrayContainsAllExprArgs>;

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

export type ArrayFilterExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class ArrayFilterExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_FILTER;

  static _sqlNames = ['FILTER', 'ARRAY_FILTER'];

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<ArrayFilterExprArgs>;

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

  static sqlNames = ['FILTER', 'ARRAY_FILTER'];
}

export type ArrayFirstExprArgs = FuncExprArgs;

export class ArrayFirstExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_FIRST;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ArrayFirstExprArgs>;

  declare args: ArrayFirstExprArgs;

  constructor (args: ArrayFirstExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayLastExprArgs = FuncExprArgs;

export class ArrayLastExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_LAST;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ArrayLastExprArgs>;

  declare args: ArrayLastExprArgs;

  constructor (args: ArrayLastExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayReverseExprArgs = FuncExprArgs;

export class ArrayReverseExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_REVERSE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ArrayReverseExprArgs>;

  declare args: ArrayReverseExprArgs;

  constructor (args: ArrayReverseExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArraySliceExprArgs = {
  this: Expression;
  start: Expression;
  end?: Expression;
  step?: Expression;
} & FuncExprArgs;

export class ArraySliceExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_SLICE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    start: true,
    end: false,
    step: false,
  } satisfies RequiredMap<ArraySliceExprArgs>;

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

export type ArrayToStringExprArgs = {
  this: Expression;
  expression: Expression;
  null?: Expression;
} & FuncExprArgs;

export class ArrayToStringExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_TO_STRING;

  static _sqlNames = ['ARRAY_TO_STRING', 'ARRAY_JOIN'];

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    null: false,
  } satisfies RequiredMap<ArrayToStringExprArgs>;

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

  static sqlNames = ['ARRAY_TO_STRING', 'ARRAY_JOIN'];
}

export type ArrayIntersectExprArgs = {
  expressions: Expression[];
} & FuncExprArgs;

export class ArrayIntersectExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_INTERSECT;

  static isVarLenArgs = true;

  static _sqlNames = ['ARRAY_INTERSECT', 'ARRAY_INTERSECTION'];

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<ArrayIntersectExprArgs>;

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

  static sqlNames = ['ARRAY_INTERSECT', 'ARRAY_INTERSECTION'];
}

export type StPointExprArgs = {
  this: Expression;
  expression: Expression;
  null?: Expression;
} & FuncExprArgs;

export class StPointExpr extends FuncExpr {
  key = ExpressionKey.ST_POINT;

  static _sqlNames = ['ST_POINT', 'ST_MAKEPOINT'];

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    null: false,
  } satisfies RequiredMap<StPointExprArgs>;

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

  static sqlNames = ['ST_POINT', 'ST_MAKEPOINT'];
}

export type StDistanceExprArgs = {
  this: Expression;
  expression: Expression;
  useSpheroid?: Expression;
} & FuncExprArgs;

export class StDistanceExpr extends FuncExpr {
  key = ExpressionKey.ST_DISTANCE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    useSpheroid: false,
  } satisfies RequiredMap<StDistanceExprArgs>;

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
export type StringExprArgs = {
  this: Expression;
  zone?: Expression;
} & FuncExprArgs;

export class StringExpr extends FuncExpr {
  key = ExpressionKey.STRING;

  static argTypes = {
    ...super.argTypes,
    this: true,
    zone: false,
  } satisfies RequiredMap<StringExprArgs>;

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

export type StringToArrayExprArgs = {
  this: Expression;
  expression?: Expression;
  null?: Expression;
} & FuncExprArgs;

export class StringToArrayExpr extends FuncExpr {
  key = ExpressionKey.STRING_TO_ARRAY;

  static _sqlNames = [
    'STRING_TO_ARRAY',
    'SPLIT_BY_STRING',
    'STRTOK_TO_ARRAY',
  ];

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
    null: false,
  } satisfies RequiredMap<StringToArrayExprArgs>;

  declare args: StringToArrayExprArgs;

  constructor (args: StringToArrayExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  get $null (): Expression | undefined {
    return this.args.null;
  }

  static {
    this.register();
  }

  static sqlNames = [
    'STRING_TO_ARRAY',
    'SPLIT_BY_STRING',
    'STRTOK_TO_ARRAY',
  ];
}

export type ArrayOverlapsExprArgs = {
  this: Expression;
  expression: Expression;
} & BinaryExprArgs & FuncExprArgs;

export class ArrayOverlapsExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.ARRAY_OVERLAPS;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<ArrayOverlapsExprArgs>;

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

export type ArraySizeExprArgs = {
  this: Expression;
  expression?: Expression;
} & FuncExprArgs;

export class ArraySizeExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_SIZE;

  static _sqlNames = ['ARRAY_SIZE', 'ARRAY_LENGTH'];

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<ArraySizeExprArgs>;

  declare args: ArraySizeExprArgs;

  constructor (args: ArraySizeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }

  static sqlNames = ['ARRAY_SIZE', 'ARRAY_LENGTH'];
}

export type ArraySortExprArgs = {
  this: Expression;
  expression?: Expression;
} & FuncExprArgs;

export class ArraySortExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_SORT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<ArraySortExprArgs>;

  declare args: ArraySortExprArgs;

  constructor (args: ArraySortExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ArraySumExprArgs = {
  this: Expression;
  expression?: Expression;
} & FuncExprArgs;

export class ArraySumExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_SUM;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<ArraySumExprArgs>;

  declare args: ArraySumExprArgs;

  constructor (args: ArraySumExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ArraysZipExprArgs = {
  expressions?: Expression[];
} & FuncExprArgs;

export class ArraysZipExpr extends FuncExpr {
  key = ExpressionKey.ARRAYS_ZIP;

  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    expressions: false,
  } satisfies RequiredMap<ArraysZipExprArgs>;

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

export type CaseExprArgs = {
  this?: Expression;
  ifs: Expression[];
  default?: Expression;
} & FuncExprArgs;

export class CaseExpr extends FuncExpr {
  key = ExpressionKey.CASE;

  /**
   * Defines the arguments (properties and child expressions) for Case expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    ifs: true,
    default: false,
  } satisfies RequiredMap<CaseExprArgs>;

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
    instance.set('default', maybeParse(condition, options));
    return instance;
  }

  static {
    this.register();
  }
}

export type CastExprArgs = {
  this: Expression;
  to: Expression;
  format?: string;
  safe?: boolean;
  action?: Expression;
  default?: Expression;
} & FuncExprArgs;

export class CastExpr extends FuncExpr {
  key = ExpressionKey.CAST;

  /**
   * Defines the arguments (properties and child expressions) for Cast expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    to: true,
    format: false,
    safe: false,
    action: false,
    default: false,
  } satisfies RequiredMap<CastExprArgs>;

  declare args: CastExprArgs;

  constructor (args: CastExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $to (): Expression {
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

  isType (...dtypes: Array<DataTypeExprKind | DataTypeExpr>): boolean {
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

export type JustifyDaysExprArgs = FuncExprArgs;

export class JustifyDaysExpr extends FuncExpr {
  key = ExpressionKey.JUSTIFY_DAYS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<JustifyDaysExprArgs>;

  declare args: JustifyDaysExprArgs;

  constructor (args: JustifyDaysExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JustifyHoursExprArgs = FuncExprArgs;

export class JustifyHoursExpr extends FuncExpr {
  key = ExpressionKey.JUSTIFY_HOURS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<JustifyHoursExprArgs>;

  declare args: JustifyHoursExprArgs;

  constructor (args: JustifyHoursExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JustifyIntervalExprArgs = FuncExprArgs;

export class JustifyIntervalExpr extends FuncExpr {
  key = ExpressionKey.JUSTIFY_INTERVAL;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<JustifyIntervalExprArgs>;

  declare args: JustifyIntervalExprArgs;

  constructor (args: JustifyIntervalExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TryExprArgs = FuncExprArgs;

export class TryExpr extends FuncExpr {
  key = ExpressionKey.TRY;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TryExprArgs>;

  declare args: TryExprArgs;

  constructor (args: TryExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CastToStrTypeExprArgs = {
  this: Expression;
  to: Expression;
} & FuncExprArgs;

export class CastToStrTypeExpr extends FuncExpr {
  key = ExpressionKey.CAST_TO_STR_TYPE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    to: true,
  } satisfies RequiredMap<CastToStrTypeExprArgs>;

  declare args: CastToStrTypeExprArgs;

  constructor (args: CastToStrTypeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $to (): Expression {
    return this.args.to;
  }

  static {
    this.register();
  }
}

export type CheckJsonExprArgs = {
  this: Expression;
} & FuncExprArgs;

export class CheckJsonExpr extends FuncExpr {
  key = ExpressionKey.CHECK_JSON;

  static argTypes = {
    ...super.argTypes,
    this: true,
  } satisfies RequiredMap<CheckJsonExprArgs>;

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

export type CheckXmlExprArgs = {
  this: Expression;
  disableAutoConvert?: Expression;
} & FuncExprArgs;

export class CheckXmlExpr extends FuncExpr {
  key = ExpressionKey.CHECK_XML;

  static argTypes = {
    ...super.argTypes,
    this: true,
    disableAutoConvert: false,
  } satisfies RequiredMap<CheckXmlExprArgs>;

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

export type CollateExprArgs = BinaryExprArgs & FuncExprArgs;

export class CollateExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.COLLATE;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  } satisfies RequiredMap<CollateExprArgs>;

  declare args: CollateExprArgs;

  constructor (args: CollateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CollationExprArgs = FuncExprArgs;

export class CollationExpr extends FuncExpr {
  key = ExpressionKey.COLLATION;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CollationExprArgs>;

  declare args: CollationExprArgs;

  constructor (args: CollationExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CeilExprArgs = {
  this: Expression;
  decimals?: Expression;
  to?: Expression;
} & FuncExprArgs;

export class CeilExpr extends FuncExpr {
  key = ExpressionKey.CEIL;

  static sqlNames = ['CEIL', 'CEILING'];

  static argTypes = {
    ...super.argTypes,
    this: true,
    decimals: false,
    to: false,
  } satisfies RequiredMap<CeilExprArgs>;

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

export type CoalesceExprArgs = {
  this: Expression;
  expressions?: Expression[];
  isNvl?: Expression;
  isNull?: Expression;
} & FuncExprArgs;

export class CoalesceExpr extends FuncExpr {
  key = ExpressionKey.COALESCE;

  static sqlNames = [
    'COALESCE',
    'IFNULL',
    'NVL',
  ];

  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
    isNvl: false,
    isNull: false,
  } satisfies RequiredMap<CoalesceExprArgs>;

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

  get $isNvl (): Expression | undefined {
    return this.args.isNvl;
  }

  get $isNull (): Expression | undefined {
    return this.args.isNull;
  }

  static {
    this.register();
  }
}

export type ChrExprArgs = {
  expressions: Expression[];
  charset?: string;
} & FuncExprArgs;

export class ChrExpr extends FuncExpr {
  key = ExpressionKey.CHR;

  static sqlNames = ['CHR', 'CHAR'];

  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
    charset: false,
  } satisfies RequiredMap<ChrExprArgs>;

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

export type ConcatExprArgs = {
  expressions: Expression[];
  safe?: boolean;
  coalesce?: Expression;
} & FuncExprArgs;

export class ConcatExpr extends FuncExpr {
  key = ExpressionKey.CONCAT;

  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
    safe: false,
    coalesce: false,
  } satisfies RequiredMap<ConcatExprArgs>;

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

  get $coalesce (): Expression | undefined {
    return this.args.coalesce;
  }

  static {
    this.register();
  }
}

export type ContainsExprArgs = {
  this: Expression;
  expression: Expression;
  jsonScope?: Expression;
} & FuncExprArgs;

export class ContainsExpr extends FuncExpr {
  key = ExpressionKey.CONTAINS;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    jsonScope: false,
  } satisfies RequiredMap<ContainsExprArgs>;

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

export type ConnectByRootExprArgs = FuncExprArgs;

export class ConnectByRootExpr extends FuncExpr {
  key = ExpressionKey.CONNECT_BY_ROOT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ConnectByRootExprArgs>;

  declare args: ConnectByRootExprArgs;

  constructor (args: ConnectByRootExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CbrtExprArgs = FuncExprArgs;

export class CbrtExpr extends FuncExpr {
  key = ExpressionKey.CBRT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CbrtExprArgs>;

  declare args: CbrtExprArgs;

  constructor (args: CbrtExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentAccountExprArgs = FuncExprArgs;

export class CurrentAccountExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ACCOUNT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentAccountExprArgs>;

  declare args: CurrentAccountExprArgs;

  constructor (args: CurrentAccountExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentAccountNameExprArgs = FuncExprArgs;

export class CurrentAccountNameExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ACCOUNT_NAME;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentAccountNameExprArgs>;

  declare args: CurrentAccountNameExprArgs;

  constructor (args: CurrentAccountNameExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentAvailableRolesExprArgs = FuncExprArgs;

export class CurrentAvailableRolesExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_AVAILABLE_ROLES;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentAvailableRolesExprArgs>;

  declare args: CurrentAvailableRolesExprArgs;

  constructor (args: CurrentAvailableRolesExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentClientExprArgs = FuncExprArgs;

export class CurrentClientExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_CLIENT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentClientExprArgs>;

  declare args: CurrentClientExprArgs;

  constructor (args: CurrentClientExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentIpAddressExprArgs = FuncExprArgs;

export class CurrentIpAddressExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_IP_ADDRESS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentIpAddressExprArgs>;

  declare args: CurrentIpAddressExprArgs;

  constructor (args: CurrentIpAddressExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentDatabaseExprArgs = FuncExprArgs;

export class CurrentDatabaseExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_DATABASE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentDatabaseExprArgs>;

  declare args: CurrentDatabaseExprArgs;

  constructor (args: CurrentDatabaseExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentSchemasExprArgs = {
  this?: Expression;
} & FuncExprArgs;

export class CurrentSchemasExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_SCHEMAS;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<CurrentSchemasExprArgs>;

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

export type CurrentSecondaryRolesExprArgs = FuncExprArgs;

export class CurrentSecondaryRolesExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_SECONDARY_ROLES;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentSecondaryRolesExprArgs>;

  declare args: CurrentSecondaryRolesExprArgs;

  constructor (args: CurrentSecondaryRolesExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentSessionExprArgs = FuncExprArgs;

export class CurrentSessionExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_SESSION;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentSessionExprArgs>;

  declare args: CurrentSessionExprArgs;

  constructor (args: CurrentSessionExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentStatementExprArgs = FuncExprArgs;

export class CurrentStatementExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_STATEMENT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentStatementExprArgs>;

  declare args: CurrentStatementExprArgs;

  constructor (args: CurrentStatementExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentVersionExprArgs = FuncExprArgs;

export class CurrentVersionExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_VERSION;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentVersionExprArgs>;

  declare args: CurrentVersionExprArgs;

  constructor (args: CurrentVersionExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentTransactionExprArgs = FuncExprArgs;

export class CurrentTransactionExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_TRANSACTION;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentTransactionExprArgs>;

  declare args: CurrentTransactionExprArgs;

  constructor (args: CurrentTransactionExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentWarehouseExprArgs = FuncExprArgs;

export class CurrentWarehouseExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_WAREHOUSE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentWarehouseExprArgs>;

  declare args: CurrentWarehouseExprArgs;

  constructor (args: CurrentWarehouseExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentDateExprArgs = {
  this?: Expression;
} & FuncExprArgs;

export class CurrentDateExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_DATE;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<CurrentDateExprArgs>;

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

export type CurrentDatetimeExprArgs = {
  this?: Expression;
} & FuncExprArgs;

export class CurrentDatetimeExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_DATETIME;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<CurrentDatetimeExprArgs>;

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

export type CurrentTimeExprArgs = {
  this?: Expression;
} & FuncExprArgs;

export class CurrentTimeExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_TIME;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<CurrentTimeExprArgs>;

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

export type LocaltimeExprArgs = {
  this?: Expression;
} & FuncExprArgs;

export class LocaltimeExpr extends FuncExpr {
  key = ExpressionKey.LOCALTIME;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<LocaltimeExprArgs>;

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

export type LocaltimestampExprArgs = {
  this?: Expression;
} & FuncExprArgs;

export class LocaltimestampExpr extends FuncExpr {
  key = ExpressionKey.LOCALTIMESTAMP;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<LocaltimestampExprArgs>;

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

export type SystimestampExprArgs = {
  this?: Expression;
} & FuncExprArgs;

export class SystimestampExpr extends FuncExpr {
  key = ExpressionKey.SYSTIMESTAMP;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<SystimestampExprArgs>;

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

export type CurrentTimestampExprArgs = {
  this?: Expression;
  sysdate?: Expression;
} & FuncExprArgs;

export class CurrentTimestampExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_TIMESTAMP;

  static argTypes = {
    ...super.argTypes,
    this: false,
    sysdate: false,
  } satisfies RequiredMap<CurrentTimestampExprArgs>;

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

export type CurrentTimestampLTZExprArgs = FuncExprArgs;

export class CurrentTimestampLTZExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_TIMESTAMP_LTZ;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentTimestampLTZExprArgs>;

  declare args: CurrentTimestampLTZExprArgs;

  constructor (args: CurrentTimestampLTZExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentTimezoneExprArgs = FuncExprArgs;

export class CurrentTimezoneExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_TIMEZONE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentTimezoneExprArgs>;

  declare args: CurrentTimezoneExprArgs;

  constructor (args: CurrentTimezoneExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentOrganizationNameExprArgs = FuncExprArgs;

export class CurrentOrganizationNameExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ORGANIZATION_NAME;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentOrganizationNameExprArgs>;

  declare args: CurrentOrganizationNameExprArgs;

  constructor (args: CurrentOrganizationNameExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentSchemaExprArgs = {
  this?: Expression;
} & FuncExprArgs;

export class CurrentSchemaExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_SCHEMA;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<CurrentSchemaExprArgs>;

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

export type CurrentUserExprArgs = {
  this?: Expression;
} & FuncExprArgs;

export class CurrentUserExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_USER;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<CurrentUserExprArgs>;

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

export type CurrentCatalogExprArgs = FuncExprArgs;

export class CurrentCatalogExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_CATALOG;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentCatalogExprArgs>;

  declare args: CurrentCatalogExprArgs;

  constructor (args: CurrentCatalogExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentRegionExprArgs = FuncExprArgs;

export class CurrentRegionExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_REGION;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentRegionExprArgs>;

  declare args: CurrentRegionExprArgs;

  constructor (args: CurrentRegionExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentRoleExprArgs = FuncExprArgs;

export class CurrentRoleExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ROLE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentRoleExprArgs>;

  declare args: CurrentRoleExprArgs;

  constructor (args: CurrentRoleExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentRoleTypeExprArgs = FuncExprArgs;

export class CurrentRoleTypeExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ROLE_TYPE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentRoleTypeExprArgs>;

  declare args: CurrentRoleTypeExprArgs;

  constructor (args: CurrentRoleTypeExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentOrganizationUserExprArgs = FuncExprArgs;

export class CurrentOrganizationUserExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ORGANIZATION_USER;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CurrentOrganizationUserExprArgs>;

  declare args: CurrentOrganizationUserExprArgs;

  constructor (args: CurrentOrganizationUserExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SessionUserExprArgs = FuncExprArgs;

export class SessionUserExpr extends FuncExpr {
  key = ExpressionKey.SESSION_USER;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SessionUserExprArgs>;

  declare args: SessionUserExprArgs;

  constructor (args: SessionUserExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UtcDateExprArgs = FuncExprArgs;

export class UtcDateExpr extends FuncExpr {
  key = ExpressionKey.UTC_DATE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<UtcDateExprArgs>;

  declare args: UtcDateExprArgs;

  constructor (args: UtcDateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UtcTimeExprArgs = {
  this?: Expression;
} & FuncExprArgs;

export class UtcTimeExpr extends FuncExpr {
  key = ExpressionKey.UTC_TIME;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<UtcTimeExprArgs>;

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

export type UtcTimestampExprArgs = {
  this?: Expression;
} & FuncExprArgs;

export class UtcTimestampExpr extends FuncExpr {
  key = ExpressionKey.UTC_TIMESTAMP;

  static argTypes = {
    ...super.argTypes,
    this: false,
  } satisfies RequiredMap<UtcTimestampExprArgs>;

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

export type DateAddExprArgs = {
  this: Expression;
  expression: Expression;
  unit?: Expression;
} & FuncExprArgs & IntervalOpExprArgs;

export class DateAddExpr extends multiInherit(FuncExpr, IntervalOpExpr) {
  key = ExpressionKey.DATE_ADD;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  } satisfies RequiredMap<DateAddExprArgs>;

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

export type DateBinExprArgs = {
  this: Expression;
  expression: Expression;
  unit?: Expression;
  zone?: Expression;
  origin?: Expression;
} & FuncExprArgs & IntervalOpExprArgs;

export class DateBinExpr extends multiInherit(FuncExpr, IntervalOpExpr) {
  key = ExpressionKey.DATE_BIN;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
    zone: false,
    origin: false,
  } satisfies RequiredMap<DateBinExprArgs>;

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

export type DateSubExprArgs = {
  this: Expression;
  expression: Expression;
  unit?: Expression;
} & FuncExprArgs & IntervalOpExprArgs;

export class DateSubExpr extends multiInherit(FuncExpr, IntervalOpExpr) {
  key = ExpressionKey.DATE_SUB;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  } satisfies RequiredMap<DateSubExprArgs>;

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

export type DateDiffExprArgs = {
  this: Expression;
  expression: Expression;
  unit?: Expression;
  zone?: Expression;
  bigInt?: Expression;
  datePartBoundary?: Expression;
} & FuncExprArgs & TimeUnitExprArgs;

export class DateDiffExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.DATE_DIFF;

  static sqlNames = ['DATEDIFF', 'DATE_DIFF'];

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
    zone: false,
    bigInt: false,
    datePartBoundary: false,
  } satisfies RequiredMap<DateDiffExprArgs>;

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

export type DateTruncExprArgs = {
  unit: Expression;
  this: Expression;
  zone?: Expression;
  inputTypePreserved?: DataTypeExpr;
  unabbreviate?: boolean;
} & FuncExprArgs;

export class DateTruncExpr extends FuncExpr {
  key = ExpressionKey.DATE_TRUNC;

  static argTypes = {
    ...super.argTypes,
    unit: true,
    this: true,
    zone: false,
    inputTypePreserved: false,
  } satisfies RequiredMap<DateTruncExprArgs>;

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

export type DatetimeExprArgs = {
  this: Expression;
  expression?: Expression;
} & FuncExprArgs;

export class DatetimeExpr extends FuncExpr {
  key = ExpressionKey.DATETIME;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<DatetimeExprArgs>;

  declare args: DatetimeExprArgs;

  constructor (args: DatetimeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type DatetimeAddExprArgs = {
  this: Expression;
  expression: Expression;
  unit?: Expression;
} & FuncExprArgs & IntervalOpExprArgs;

export class DatetimeAddExpr extends multiInherit(FuncExpr, IntervalOpExpr) {
  key = ExpressionKey.DATETIME_ADD;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  } satisfies RequiredMap<DatetimeAddExprArgs>;

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

export type DatetimeSubExprArgs = {
  this: Expression;
  expression: Expression;
  unit?: Expression;
} & FuncExprArgs & IntervalOpExprArgs;

export class DatetimeSubExpr extends multiInherit(FuncExpr, IntervalOpExpr) {
  key = ExpressionKey.DATETIME_SUB;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  } satisfies RequiredMap<DatetimeSubExprArgs>;

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

export type DatetimeDiffExprArgs = {
  this: Expression;
  expression: Expression;
  unit?: Expression;
} & FuncExprArgs & TimeUnitExprArgs;

export class DatetimeDiffExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.DATETIME_DIFF;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  } satisfies RequiredMap<DatetimeDiffExprArgs>;

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

export type DatetimeTruncExprArgs = {
  this: Expression;
  unit: Expression;
  zone?: Expression;
} & FuncExprArgs & TimeUnitExprArgs;

export class DatetimeTruncExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.DATETIME_TRUNC;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    unit: true,
    zone: false,
  } satisfies RequiredMap<DatetimeTruncExprArgs>;

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

export type DateFromUnixDateExprArgs = FuncExprArgs;

export class DateFromUnixDateExpr extends FuncExpr {
  key = ExpressionKey.DATE_FROM_UNIX_DATE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DateFromUnixDateExprArgs>;

  declare args: DateFromUnixDateExprArgs;

  constructor (args: DateFromUnixDateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DayOfWeekExprArgs = FuncExprArgs;

export class DayOfWeekExpr extends FuncExpr {
  key = ExpressionKey.DAY_OF_WEEK;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DayOfWeekExprArgs>;

  declare args: DayOfWeekExprArgs;

  constructor (args: DayOfWeekExprArgs) {
    super(args);
  }

  static {
    this.register();
  }

  static sqlNames = ['DAY_OF_WEEK', 'DAYOFWEEK'];
}

export type DayOfWeekIsoExprArgs = FuncExprArgs;

export class DayOfWeekIsoExpr extends FuncExpr {
  key = ExpressionKey.DAY_OF_WEEK_ISO;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DayOfWeekIsoExprArgs>;

  declare args: DayOfWeekIsoExprArgs;

  constructor (args: DayOfWeekIsoExprArgs) {
    super(args);
  }

  static {
    this.register();
  }

  static sqlNames = ['DAYOFWEEK_ISO', 'ISODOW'];
}

export type DayOfMonthExprArgs = FuncExprArgs;

export class DayOfMonthExpr extends FuncExpr {
  key = ExpressionKey.DAY_OF_MONTH;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DayOfMonthExprArgs>;

  declare args: DayOfMonthExprArgs;

  constructor (args: DayOfMonthExprArgs) {
    super(args);
  }

  static sqlNames = ['DAY_OF_MONTH', 'DAYOFMONTH'];

  static {
    this.register();
  }
}

export type DayOfYearExprArgs = FuncExprArgs;

export class DayOfYearExpr extends FuncExpr {
  key = ExpressionKey.DAY_OF_YEAR;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DayOfYearExprArgs>;

  declare args: DayOfYearExprArgs;

  constructor (args: DayOfYearExprArgs) {
    super(args);
  }

  static sqlNames = ['DAY_OF_YEAR', 'DAYOFYEAR'];

  static {
    this.register();
  }
}

export type DaynameExprArgs = {
  this: Expression;
  abbreviated?: Expression;
} & FuncExprArgs;

export class DaynameExpr extends FuncExpr {
  key = ExpressionKey.DAYNAME;

  static argTypes = {
    ...super.argTypes,
    this: true,
    abbreviated: false,
  } satisfies RequiredMap<DaynameExprArgs>;

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

export type ToDaysExprArgs = FuncExprArgs;

export class ToDaysExpr extends FuncExpr {
  key = ExpressionKey.TO_DAYS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ToDaysExprArgs>;

  declare args: ToDaysExprArgs;

  constructor (args: ToDaysExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type WeekOfYearExprArgs = FuncExprArgs;

export class WeekOfYearExpr extends FuncExpr {
  key = ExpressionKey.WEEK_OF_YEAR;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<WeekOfYearExprArgs>;

  declare args: WeekOfYearExprArgs;

  constructor (args: WeekOfYearExprArgs) {
    super(args);
  }

  static {
    this.register();
  }

  static sqlNames = ['WEEK_OF_YEAR', 'WEEKOFYEAR'];
}

export type YearOfWeekExprArgs = FuncExprArgs;

export class YearOfWeekExpr extends FuncExpr {
  key = ExpressionKey.YEAR_OF_WEEK;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<YearOfWeekExprArgs>;

  declare args: YearOfWeekExprArgs;

  constructor (args: YearOfWeekExprArgs) {
    super(args);
  }

  static sqlNames = ['YEAR_OF_WEEK', 'YEAROFWEEK'];

  static {
    this.register();
  }
}

export type YearOfWeekIsoExprArgs = FuncExprArgs;

export class YearOfWeekIsoExpr extends FuncExpr {
  key = ExpressionKey.YEAR_OF_WEEK_ISO;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<YearOfWeekIsoExprArgs>;

  declare args: YearOfWeekIsoExprArgs;

  constructor (args: YearOfWeekIsoExprArgs) {
    super(args);
  }

  static sqlNames = ['YEAR_OF_WEEK_ISO', 'YEAROFWEEKISO'];

  static {
    this.register();
  }
}

export type MonthsBetweenExprArgs = {
  this: Expression;
  expression: Expression;
  roundoff?: Expression;
} & FuncExprArgs;

export class MonthsBetweenExpr extends FuncExpr {
  key = ExpressionKey.MONTHS_BETWEEN;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    roundoff: false,
  } satisfies RequiredMap<MonthsBetweenExprArgs>;

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

export type MakeIntervalExprArgs = { year?: Expression;
  month?: Expression;
  week?: Expression;
  day?: Expression;
  hour?: Expression;
  minute?: Expression;
  second?: Expression; } & FuncExprArgs;

export class MakeIntervalExpr extends FuncExpr {
  key = ExpressionKey.MAKE_INTERVAL;

  /**
   * Defines the arguments (properties and child expressions) for MakeInterval expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    year: false,
    month: false,
    week: false,
    day: false,
    hour: false,
    minute: false,
    second: false,
  } satisfies RequiredMap<MakeIntervalExprArgs>;

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

export type LastDayExprArgs = {
  this: Expression;
  unit?: Expression;
} & FuncExprArgs;

export class LastDayExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.LAST_DAY;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    unit: false,
  } satisfies RequiredMap<LastDayExprArgs>;

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

  static sqlNames = ['LAST_DAY', 'LAST_DAY_OF_MONTH'];
}

export type PreviousDayExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class PreviousDayExpr extends FuncExpr {
  key = ExpressionKey.PREVIOUS_DAY;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<PreviousDayExprArgs>;

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

export type LaxBoolExprArgs = FuncExprArgs;

export class LaxBoolExpr extends FuncExpr {
  key = ExpressionKey.LAX_BOOL;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<LaxBoolExprArgs>;

  declare args: LaxBoolExprArgs;

  constructor (args: LaxBoolExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LaxFloat64ExprArgs = FuncExprArgs;

export class LaxFloat64Expr extends FuncExpr {
  key = ExpressionKey.LAX_FLOAT64;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<LaxFloat64ExprArgs>;

  declare args: LaxFloat64ExprArgs;

  constructor (args: LaxFloat64ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LaxInt64ExprArgs = FuncExprArgs;

export class LaxInt64Expr extends FuncExpr {
  key = ExpressionKey.LAX_INT64;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<LaxInt64ExprArgs>;

  declare args: LaxInt64ExprArgs;

  constructor (args: LaxInt64ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LaxStringExprArgs = FuncExprArgs;

export class LaxStringExpr extends FuncExpr {
  key = ExpressionKey.LAX_STRING;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<LaxStringExprArgs>;

  declare args: LaxStringExprArgs;

  constructor (args: LaxStringExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ExtractExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class ExtractExpr extends FuncExpr {
  key = ExpressionKey.EXTRACT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<ExtractExprArgs>;

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

export type ExistsExprArgs = {
  this: Expression;
  expression?: Expression;
} & FuncExprArgs;
export class ExistsExpr extends multiInherit(FuncExpr, SubqueryPredicateExpr) {
  key = ExpressionKey.EXISTS;
  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<ExistsExprArgs>;

  declare args: ExistsExprArgs;
  constructor (args: ExistsExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type EltExprArgs = {
  this: Expression;
  expressions: Expression[];
} & FuncExprArgs;

export class EltExpr extends FuncExpr {
  key = ExpressionKey.ELT;
    static isVarLenArgs = true;
  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
  } satisfies RequiredMap<EltExprArgs>;

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

export type TimestampExprArgs = {
  this?: Expression;
  zone?: Expression;
  withTz?: Expression;
} & FuncExprArgs;

export class TimestampExpr extends FuncExpr {
  key = ExpressionKey.TIMESTAMP;

  /**
   * Defines the arguments (properties and child expressions) for Timestamp expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    zone: false,
    withTz: false,
  } satisfies RequiredMap<TimestampExprArgs>;

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

export type TimestampAddExprArgs = {
  this: Expression;
  expression: Expression;
  unit?: Expression;
} & FuncExprArgs;

export class TimestampAddExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIMESTAMP_ADD;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  } satisfies RequiredMap<TimestampAddExprArgs>;

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

export type TimestampSubExprArgs = {
  this: Expression;
  expression: Expression;
  unit?: Expression;
} & FuncExprArgs;

export class TimestampSubExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIMESTAMP_SUB;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  } satisfies RequiredMap<TimestampSubExprArgs>;

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

export type TimestampDiffExprArgs = {
  this: Expression;
  expression: Expression;
  unit?: Expression;
} & FuncExprArgs;

export class TimestampDiffExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIMESTAMP_DIFF;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  } satisfies RequiredMap<TimestampDiffExprArgs>;

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

  static sqlNames = ['TIMESTAMPDIFF', 'TIMESTAMP_DIFF'];
}

export type TimestampTruncExprArgs = {
  this: Expression;
  unit: Expression;
  zone?: Expression;
  inputTypePreserved?: DataTypeExpr;
} & FuncExprArgs;

export class TimestampTruncExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIMESTAMP_TRUNC;

  /**
   * Defines the arguments (properties and child expressions) for TimestampTrunc expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    unit: true,
    zone: false,
    inputTypePreserved: false,
  } satisfies RequiredMap<TimestampTruncExprArgs>;

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

  get $inputTypePreserved (): Expression | undefined {
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
export type TimeSliceExprArgs = {
  this: Expression;
  expression: Expression;
  unit: Expression;
  kind?: TimeSliceExprKind;
} & FuncExprArgs;

export class TimeSliceExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIME_SLICE;

  /**
   * Defines the arguments (properties and child expressions) for TimeSlice expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: true,
    kind: false,
  } satisfies RequiredMap<TimeSliceExprArgs>;

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

  get $kind (): string | undefined {
    return this.args.kind;
  }

  static {
    this.register();
  }
}

export type TimeAddExprArgs = {
  this: Expression;
  expression: Expression;
  unit?: Expression;
} & FuncExprArgs;

export class TimeAddExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIME_ADD;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  } satisfies RequiredMap<TimeAddExprArgs>;

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

export type TimeSubExprArgs = {
  this: Expression;
  expression: Expression;
  unit?: Expression;
} & FuncExprArgs;

export class TimeSubExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIME_SUB;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  } satisfies RequiredMap<TimeSubExprArgs>;

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

export type TimeDiffExprArgs = {
  this: Expression;
  expression: Expression;
  unit?: Expression;
} & FuncExprArgs;

export class TimeDiffExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIME_DIFF;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expression: true,
    unit: false,
  } satisfies RequiredMap<TimeDiffExprArgs>;

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

export type TimeTruncExprArgs = {
  this: Expression;
  unit: Expression;
  zone?: Expression;
} & FuncExprArgs;

export class TimeTruncExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TIME_TRUNC;

  /**
   * Defines the arguments (properties and child expressions) for TimeTrunc expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    unit: true,
    zone: false,
  } satisfies RequiredMap<TimeTruncExprArgs>;

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

export type DateFromPartsExprArgs = { year: Expression;
  month?: Expression;
  day?: Expression;
  allowOverflow?: Expression; } & FuncExprArgs;

export class DateFromPartsExpr extends FuncExpr {
  key = ExpressionKey.DATE_FROM_PARTS;

  /**
   * Defines the arguments (properties and child expressions) for DateFromParts expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    year: true,
    month: false,
    day: false,
    allowOverflow: false,
  } satisfies RequiredMap<DateFromPartsExprArgs>;

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

  static sqlNames = ['DATE_FROM_PARTS', 'DATEFROMPARTS'];
}

export type TimeFromPartsExprArgs = { hour: Expression;
  min: Expression;
  sec: Expression;
  nano?: Expression;
  fractions?: Expression[];
  precision?: number | Expression;
  overflow?: Expression; } & FuncExprArgs;

export class TimeFromPartsExpr extends FuncExpr {
  key = ExpressionKey.TIME_FROM_PARTS;

  /**
   * Defines the arguments (properties and child expressions) for TimeFromParts expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    hour: true,
    min: true,
    sec: true,
    nano: false,
    fractions: false,
    precision: false,
    overflow: false,
  } satisfies RequiredMap<TimeFromPartsExprArgs>;

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

  get $precision (): Expression | undefined {
    return this.args.precision;
  }

  get $overflow (): Expression | undefined {
    return this.args.overflow;
  }

  static {
    this.register();
  }

  static sqlNames = ['TIME_FROM_PARTS', 'TIMEFROMPARTS'];
}

export type DateStrToDateExprArgs = FuncExprArgs;

export class DateStrToDateExpr extends FuncExpr {
  key = ExpressionKey.DATE_STR_TO_DATE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DateStrToDateExprArgs>;

  declare args: DateStrToDateExprArgs;

  constructor (args: DateStrToDateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateToDateStrExprArgs = FuncExprArgs;

export class DateToDateStrExpr extends FuncExpr {
  key = ExpressionKey.DATE_TO_DATE_STR;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DateToDateStrExprArgs>;

  declare args: DateToDateStrExprArgs;

  constructor (args: DateToDateStrExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateToDiExprArgs = FuncExprArgs;

export class DateToDiExpr extends FuncExpr {
  key = ExpressionKey.DATE_TO_DI;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DateToDiExprArgs>;

  declare args: DateToDiExprArgs;

  constructor (args: DateToDiExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateExprArgs = {
  this?: Expression;
  expressions?: Expression[];
  zone?: Expression;
} & FuncExprArgs;

export class DateExpr extends FuncExpr {
  key = ExpressionKey.DATE;
  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: false,
    expressions: false,
    zone: false,
  } satisfies RequiredMap<DateExprArgs>;

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

export type DayExprArgs = FuncExprArgs;

export class DayExpr extends FuncExpr {
  key = ExpressionKey.DAY;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DayExprArgs>;

  declare args: DayExprArgs;

  constructor (args: DayExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DecodeExprArgs = {
  this: Expression;
  charset: string;
  replace?: boolean;
} & FuncExprArgs;

export class DecodeExpr extends FuncExpr {
  key = ExpressionKey.DECODE;

  /**
   * Defines the arguments (properties and child expressions) for Decode expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    charset: true,
    replace: false,
  } satisfies RequiredMap<DecodeExprArgs>;

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

export type DecodeCaseExprArgs = {
  expressions: Expression[];
} & FuncExprArgs;

export class DecodeCaseExpr extends FuncExpr {
  key = ExpressionKey.DECODE_CASE;
  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<DecodeCaseExprArgs>;

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

export type DecryptExprArgs = {
  passphrase: Expression;
  aad?: Expression;
  encryptionMethod?: string;
  safe?: boolean;
} & FuncExpr;

export class DecryptExpr extends FuncExpr {
  key = ExpressionKey.DECRYPT;

  /**
   * Defines the arguments (properties and child expressions) for Decrypt expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    passphrase: true,
    aad: false,
    encryptionMethod: false,
    safe: false,
  } satisfies RequiredMap<DecryptExprArgs>;

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

  get $encryptionMethod (): Expression | undefined {
    return this.args.encryptionMethod;
  }

  get $safe (): Expression | undefined {
    return this.args.safe;
  }

  static {
    this.register();
  }
}

export type DecryptRawExprArgs = { key: unknown;
  iv: Expression;
  aad?: Expression;
  encryptionMethod?: string;
  aead?: Expression;
  safe?: boolean; } & FuncExprArgs;

export class DecryptRawExpr extends FuncExpr {
  key = ExpressionKey.DECRYPT_RAW;

  /**
   * Defines the arguments (properties and child expressions) for DecryptRaw expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    key: true,
    iv: true,
    aad: false,
    encryptionMethod: false,
    aead: false,
    safe: false,
  } satisfies RequiredMap<DecryptRawExprArgs>;

  declare args: DecryptRawExprArgs;

  constructor (args: DecryptRawExprArgs) {
    super(args);
  }

  get $key (): Expression {
    return this.args.key;
  }

  get $iv (): Expression {
    return this.args.iv;
  }

  get $aad (): Expression | undefined {
    return this.args.aad;
  }

  get $encryptionMethod (): Expression | undefined {
    return this.args.encryptionMethod;
  }

  get $aead (): Expression | undefined {
    return this.args.aead;
  }

  get $safe (): Expression | undefined {
    return this.args.safe;
  }

  static {
    this.register();
  }
}

export type DiToDateExprArgs = FuncExprArgs;

export class DiToDateExpr extends FuncExpr {
  key = ExpressionKey.DI_TO_DATE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<DiToDateExprArgs>;

  declare args: DiToDateExprArgs;

  constructor (args: DiToDateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type EncodeExprArgs = {
  this: Expression;
  charset: string;
} & FuncExprArgs;

export class EncodeExpr extends FuncExpr {
  key = ExpressionKey.ENCODE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    charset: true,
  } satisfies RequiredMap<EncodeExprArgs>;

  declare args: EncodeExprArgs;

  constructor (args: EncodeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $charset (): Expression {
    return this.args.charset;
  }

  static {
    this.register();
  }
}

export type EncryptExprArgs = {
  this: Expression;
  passphrase: Expression;
  aad?: Expression;
  encryptionMethod?: string;
} & FuncExprArgs;

export class EncryptExpr extends FuncExpr {
  key = ExpressionKey.ENCRYPT;

  /**
   * Defines the arguments (properties and child expressions) for Encrypt expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    passphrase: true,
    aad: false,
    encryptionMethod: false,
  } satisfies RequiredMap<EncryptExprArgs>;

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

  get $encryptionMethod (): Expression | undefined {
    return this.args.encryptionMethod;
  }

  static {
    this.register();
  }
}

export type EncryptRawExprArgs = {
  this: Expression;
  key: unknown;
  iv: Expression;
  aad?: Expression;
  encryptionMethod?: string;
} & FuncExprArgs;

export class EncryptRawExpr extends FuncExpr {
  key = ExpressionKey.ENCRYPT_RAW;

  /**
   * Defines the arguments (properties and child expressions) for EncryptRaw expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    key: true,
    iv: true,
    aad: false,
    encryptionMethod: false,
  } satisfies RequiredMap<EncryptRawExprArgs>;

  declare args: EncryptRawExprArgs;

  constructor (args: EncryptRawExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $key (): Expression {
    return this.args.key;
  }

  get $iv (): Expression {
    return this.args.iv;
  }

  get $aad (): Expression | undefined {
    return this.args.aad;
  }

  get $encryptionMethod (): Expression | undefined {
    return this.args.encryptionMethod;
  }

  static {
    this.register();
  }
}

export type EqualNullExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class EqualNullExpr extends FuncExpr {
  key = ExpressionKey.EQUAL_NULL;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<EqualNullExprArgs>;

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

export type ExpExprArgs = FuncExprArgs;

export class ExpExpr extends FuncExpr {
  key = ExpressionKey.EXP;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ExpExprArgs>;

  declare args: ExpExprArgs;

  constructor (args: ExpExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FactorialExprArgs = FuncExprArgs;

export class FactorialExpr extends FuncExpr {
  key = ExpressionKey.FACTORIAL;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<FactorialExprArgs>;

  declare args: FactorialExprArgs;

  constructor (args: FactorialExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ExplodeExprArgs = {
  this: Expression;
  expressions?: Expression[];
} & FuncExprArgs;
export class ExplodeExpr extends multiInherit(FuncExpr, UDTFExpr) {
  key = ExpressionKey.EXPLODE;
    static isVarLenArgs = true;
  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    this: true,
    expressions: false,
  } satisfies RequiredMap<ExplodeExprArgs>;

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

export type InlineExprArgs = FuncExprArgs;

export class InlineExpr extends FuncExpr {
  key = ExpressionKey.INLINE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<InlineExprArgs>;

  declare args: InlineExprArgs;

  constructor (args: InlineExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnnestExprArgs = {
  expressions: Expression[];
  alias?: TableAliasExpr;
  offset?: boolean | Expression;
  explodeArray?: Expression;
} & FuncExprArgs & UDTFExprArgs;

export class UnnestExpr extends multiInherit(FuncExpr, UDTFExpr) {
  key = ExpressionKey.UNNEST;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    expressions: true,
    alias: false,
    offset: false,
    explodeArray: false,
  } satisfies RequiredMap<UnnestExprArgs>;

  declare args: UnnestExprArgs;

  constructor (args: UnnestExprArgs) {
    super(args);
  }

  get selects (): Expression[] {
    const columns = super.selects;
    const offset = this.args.offset;
    if (offset) {
      const offsetCol = offset === true ? IdentifierExpr.build('offset') : offset;
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

export type FloorExprArgs = {
  this: Expression;
  decimals?: Expression[];
  to?: Expression;
} & FuncExprArgs;

export class FloorExpr extends FuncExpr {
  key = ExpressionKey.FLOOR;

  /**
   * Defines the arguments (properties and child expressions) for Floor expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    decimals: false,
    to: false,
  } satisfies RequiredMap<FloorExprArgs>;

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

export type FromBase32ExprArgs = FuncExprArgs;

export class FromBase32Expr extends FuncExpr {
  key = ExpressionKey.FROM_BASE32;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<FromBase32ExprArgs>;

  declare args: FromBase32ExprArgs;

  constructor (args: FromBase32ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FromBase64ExprArgs = FuncExprArgs;

export class FromBase64Expr extends FuncExpr {
  key = ExpressionKey.FROM_BASE64;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<FromBase64ExprArgs>;

  declare args: FromBase64ExprArgs;

  constructor (args: FromBase64ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToBase32ExprArgs = FuncExprArgs;

export class ToBase32Expr extends FuncExpr {
  key = ExpressionKey.TO_BASE32;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ToBase32ExprArgs>;

  declare args: ToBase32ExprArgs;

  constructor (args: ToBase32ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToBase64ExprArgs = FuncExprArgs;

export class ToBase64Expr extends FuncExpr {
  key = ExpressionKey.TO_BASE64;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ToBase64ExprArgs>;

  declare args: ToBase64ExprArgs;

  constructor (args: ToBase64ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToBinaryExprArgs = {
  this: Expression;
  format?: string;
  safe?: boolean;
} & FuncExprArgs;

export class ToBinaryExpr extends FuncExpr {
  key = ExpressionKey.TO_BINARY;

  /**
   * Defines the arguments (properties and child expressions) for ToBinary expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    format: false,
    safe: false,
  } satisfies RequiredMap<ToBinaryExprArgs>;

  declare args: ToBinaryExprArgs;

  constructor (args: ToBinaryExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $format (): Expression | undefined {
    return this.args.format;
  }

  get $safe (): Expression | undefined {
    return this.args.safe;
  }

  static {
    this.register();
  }
}

export type Base64DecodeBinaryExprArgs = {
  this: Expression;
  alphabet?: Expression;
} & FuncExprArgs;

export class Base64DecodeBinaryExpr extends FuncExpr {
  key = ExpressionKey.BASE64_DECODE_BINARY;

  static argTypes = {
    ...super.argTypes,
    this: true,
    alphabet: false,
  } satisfies RequiredMap<Base64DecodeBinaryExprArgs>;

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

export type Base64DecodeStringExprArgs = {
  this: Expression;
  alphabet?: Expression;
} & FuncExprArgs;

export class Base64DecodeStringExpr extends FuncExpr {
  key = ExpressionKey.BASE64_DECODE_STRING;

  static argTypes = {
    ...super.argTypes,
    this: true,
    alphabet: false,
  } satisfies RequiredMap<Base64DecodeStringExprArgs>;

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

export type Base64EncodeExprArgs = {
  this: Expression;
  maxLineLength?: number | Expression;
  alphabet?: Expression;
} & FuncExprArgs;

export class Base64EncodeExpr extends FuncExpr {
  key = ExpressionKey.BASE64_ENCODE;

  /**
   * Defines the arguments (properties and child expressions) for Base64Encode expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    maxLineLength: false,
    alphabet: false,
  } satisfies RequiredMap<Base64EncodeExprArgs>;

  declare args: Base64EncodeExprArgs;

  constructor (args: Base64EncodeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $maxLineLength (): Expression | undefined {
    return this.args.maxLineLength;
  }

  get $alphabet (): Expression | undefined {
    return this.args.alphabet;
  }

  static {
    this.register();
  }
}

export type TryBase64DecodeBinaryExprArgs = {
  this: Expression;
  alphabet?: Expression;
} & FuncExprArgs;

export class TryBase64DecodeBinaryExpr extends FuncExpr {
  key = ExpressionKey.TRY_BASE64_DECODE_BINARY;

  static argTypes = {
    ...super.argTypes,
    this: true,
    alphabet: false,
  } satisfies RequiredMap<TryBase64DecodeBinaryExprArgs>;

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

export type TryBase64DecodeStringExprArgs = {
  this: Expression;
  alphabet?: Expression;
} & FuncExprArgs;

export class TryBase64DecodeStringExpr extends FuncExpr {
  key = ExpressionKey.TRY_BASE64_DECODE_STRING;

  static argTypes = {
    ...super.argTypes,
    this: true,
    alphabet: false,
  } satisfies RequiredMap<TryBase64DecodeStringExprArgs>;

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

export type TryHexDecodeBinaryExprArgs = FuncExprArgs;

export class TryHexDecodeBinaryExpr extends FuncExpr {
  key = ExpressionKey.TRY_HEX_DECODE_BINARY;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TryHexDecodeBinaryExprArgs>;

  declare args: TryHexDecodeBinaryExprArgs;

  constructor (args: TryHexDecodeBinaryExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TryHexDecodeStringExprArgs = FuncExprArgs;

export class TryHexDecodeStringExpr extends FuncExpr {
  key = ExpressionKey.TRY_HEX_DECODE_STRING;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TryHexDecodeStringExprArgs>;

  declare args: TryHexDecodeStringExprArgs;

  constructor (args: TryHexDecodeStringExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FromISO8601TimestampExprArgs = FuncExprArgs;

export class FromISO8601TimestampExpr extends FuncExpr {
  key = ExpressionKey.FROM_ISO8601_TIMESTAMP;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<FromISO8601TimestampExprArgs>;

  declare args: FromISO8601TimestampExprArgs;

  constructor (args: FromISO8601TimestampExprArgs) {
    super(args);
  }

  static sqlNames = ['FROM_ISO8601_TIMESTAMP'];

  static {
    this.register();
  }
}

export type GapFillExprArgs = {
  tsColumn: Expression;
  bucketWidth: Expression;
  partitioningColumns?: Expression[];
  valueColumns?: Expression[];
  origin?: Expression;
  ignoreNulls?: Expression[];
} & FuncExprArgs;

export class GapFillExpr extends FuncExpr {
  key = ExpressionKey.GAP_FILL;

  /**
   * Defines the arguments (properties and child expressions) for GapFill expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    tsColumn: true,
    bucketWidth: true,
    partitioningColumns: false,
    valueColumns: false,
    origin: false,
    ignoreNulls: false,
  } satisfies RequiredMap<GapFillExprArgs>;

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

export type GenerateDateArrayExprArgs = { start: Expression;
  end: Expression;
  step?: Expression; } & FuncExprArgs;

export class GenerateDateArrayExpr extends FuncExpr {
  key = ExpressionKey.GENERATE_DATE_ARRAY;

  /**
   * Defines the arguments (properties and child expressions) for GenerateDateArray expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    start: true,
    end: true,
    step: false,
  } satisfies RequiredMap<GenerateDateArrayExprArgs>;

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

export type GenerateTimestampArrayExprArgs = { start: Expression;
  end: Expression;
  step: Expression; } & FuncExprArgs;

export class GenerateTimestampArrayExpr extends FuncExpr {
  key = ExpressionKey.GENERATE_TIMESTAMP_ARRAY;

  /**
   * Defines the arguments (properties and child expressions) for GenerateTimestampArray
   * expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    start: true,
    end: true,
    step: true,
  } satisfies RequiredMap<GenerateTimestampArrayExprArgs>;

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

export type GetExtractExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class GetExtractExpr extends FuncExpr {
  key = ExpressionKey.GET_EXTRACT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<GetExtractExprArgs>;

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

export type GetbitExprArgs = {
  this: Expression;
  expression: Expression;
  zeroIsMsb?: Expression;
} & FuncExprArgs;

export class GetbitExpr extends FuncExpr {
  key = ExpressionKey.GETBIT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    zeroIsMsb: false,
  } satisfies RequiredMap<GetbitExprArgs>;

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

  static sqlNames = ['GETBIT', 'GET_BIT'];
}

export type GreatestExprArgs = {
  this: Expression;
  expressions?: Expression[];
  ignoreNulls: Expression[];
} & FuncExprArgs;

export class GreatestExpr extends FuncExpr {
  key = ExpressionKey.GREATEST;
  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
    ignoreNulls: true,
  } satisfies RequiredMap<GreatestExprArgs>;

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

  get $ignoreNulls (): Expression[] {
    return this.args.ignoreNulls;
  }

  static {
    this.register();
  }
}

export type HexExprArgs = FuncExprArgs;

export class HexExpr extends FuncExpr {
  key = ExpressionKey.HEX;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<HexExprArgs>;

  declare args: HexExprArgs;

  constructor (args: HexExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type HexDecodeStringExprArgs = FuncExprArgs;

export class HexDecodeStringExpr extends FuncExpr {
  key = ExpressionKey.HEX_DECODE_STRING;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<HexDecodeStringExprArgs>;

  declare args: HexDecodeStringExprArgs;

  constructor (args: HexDecodeStringExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type HexEncodeExprArgs = {
  this: Expression;
  case?: Expression;
} & FuncExprArgs;

export class HexEncodeExpr extends FuncExpr {
  key = ExpressionKey.HEX_ENCODE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    case: false,
  } satisfies RequiredMap<HexEncodeExprArgs>;

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

export type HourExprArgs = FuncExprArgs;

export class HourExpr extends FuncExpr {
  key = ExpressionKey.HOUR;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<HourExprArgs>;

  declare args: HourExprArgs;

  constructor (args: HourExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MinuteExprArgs = FuncExprArgs;

export class MinuteExpr extends FuncExpr {
  key = ExpressionKey.MINUTE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<MinuteExprArgs>;

  declare args: MinuteExprArgs;

  constructor (args: MinuteExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SecondExprArgs = FuncExprArgs;

export class SecondExpr extends FuncExpr {
  key = ExpressionKey.SECOND;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SecondExprArgs>;

  declare args: SecondExprArgs;

  constructor (args: SecondExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CompressExprArgs = {
  this: Expression;
  method?: string;
} & FuncExprArgs;

export class CompressExpr extends FuncExpr {
  key = ExpressionKey.COMPRESS;

  static argTypes = {
    ...super.argTypes,
    this: true,
    method: false,
  } satisfies RequiredMap<CompressExprArgs>;

  declare args: CompressExprArgs;

  constructor (args: CompressExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $method (): Expression | undefined {
    return this.args.method;
  }

  static {
    this.register();
  }
}

export type DecompressBinaryExprArgs = {
  this: Expression;
  method: string;
} & FuncExprArgs;

export class DecompressBinaryExpr extends FuncExpr {
  key = ExpressionKey.DECOMPRESS_BINARY;

  static argTypes = {
    ...super.argTypes,
    this: true,
    method: true,
  } satisfies RequiredMap<DecompressBinaryExprArgs>;

  declare args: DecompressBinaryExprArgs;

  constructor (args: DecompressBinaryExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $method (): Expression {
    return this.args.method;
  }

  static {
    this.register();
  }
}

export type DecompressStringExprArgs = {
  this: Expression;
  method: string;
} & FuncExprArgs;

export class DecompressStringExpr extends FuncExpr {
  key = ExpressionKey.DECOMPRESS_STRING;

  static argTypes = {
    ...super.argTypes,
    this: true,
    method: true,
  } satisfies RequiredMap<DecompressStringExprArgs>;

  declare args: DecompressStringExprArgs;

  constructor (args: DecompressStringExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $method (): Expression {
    return this.args.method;
  }

  static {
    this.register();
  }
}

export type IfExprArgs = {
  this: Expression;
  true: Expression;
  false?: Expression;
} & FuncExprArgs;

export class IfExpr extends FuncExpr {
  key = ExpressionKey.IF;

  static sqlNames = ['IF', 'IIF'];

  /**
   * Defines the arguments (properties and child expressions) for If expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    true: true,
    false: false,
  } satisfies RequiredMap<IfExprArgs>;

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

export type NullifExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class NullifExpr extends FuncExpr {
  key = ExpressionKey.NULLIF;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<NullifExprArgs>;

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

export type InitcapExprArgs = {
  this: Expression;
  expression?: Expression;
} & FuncExprArgs;

export class InitcapExpr extends FuncExpr {
  key = ExpressionKey.INITCAP;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<InitcapExprArgs>;

  declare args: InitcapExprArgs;

  constructor (args: InitcapExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type IsAsciiExprArgs = FuncExprArgs;

export class IsAsciiExpr extends FuncExpr {
  key = ExpressionKey.IS_ASCII;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<IsAsciiExprArgs>;

  declare args: IsAsciiExprArgs;

  constructor (args: IsAsciiExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type IsNanExprArgs = FuncExprArgs;

export class IsNanExpr extends FuncExpr {
  key = ExpressionKey.IS_NAN;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<IsNanExprArgs>;

  declare args: IsNanExprArgs;

  constructor (args: IsNanExprArgs) {
    super(args);
  }

  static sqlNames = ['IS_NAN', 'ISNAN'];

  static {
    this.register();
  }
}

export type Int64ExprArgs = FuncExprArgs;

export class Int64Expr extends FuncExpr {
  key = ExpressionKey.INT64;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<Int64ExprArgs>;

  declare args: Int64ExprArgs;

  constructor (args: Int64ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type IsInfExprArgs = FuncExprArgs;

export class IsInfExpr extends FuncExpr {
  key = ExpressionKey.IS_INF;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<IsInfExprArgs>;

  declare args: IsInfExprArgs;

  constructor (args: IsInfExprArgs) {
    super(args);
  }

  static sqlNames = ['IS_INF', 'ISINF'];

  static {
    this.register();
  }
}

export type IsNullValueExprArgs = FuncExprArgs;

export class IsNullValueExpr extends FuncExpr {
  key = ExpressionKey.IS_NULL_VALUE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<IsNullValueExprArgs>;

  declare args: IsNullValueExprArgs;

  constructor (args: IsNullValueExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type IsArrayExprArgs = FuncExprArgs;

export class IsArrayExpr extends FuncExpr {
  key = ExpressionKey.IS_ARRAY;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<IsArrayExprArgs>;

  declare args: IsArrayExprArgs;

  constructor (args: IsArrayExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FormatExprArgs = {
  this: Expression;
  expressions?: Expression[];
} & FuncExprArgs;

export class FormatExpr extends FuncExpr {
  key = ExpressionKey.FORMAT;
  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
  } satisfies RequiredMap<FormatExprArgs>;

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

export type JSONKeysExprArgs = {
  this: Expression;
  expression?: Expression;
  expressions?: Expression[];
} & FuncExprArgs;

export class JSONKeysExpr extends FuncExpr {
  key = ExpressionKey.JSON_KEYS;
  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
    expressions: false,
  } satisfies RequiredMap<JSONKeysExprArgs>;

  declare args: JSONKeysExprArgs;

  constructor (args: JSONKeysExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  get $expressions (): Expression[] | undefined {
    return this.args.expressions;
  }

  static {
    this.register();
  }

  static sqlNames = ['JSON_KEYS'];
}

export type JSONKeysAtDepthExprArgs = {
  this: Expression;
  expression?: Expression;
  mode?: Expression;
} & FuncExprArgs;

export class JSONKeysAtDepthExpr extends FuncExpr {
  key = ExpressionKey.JSON_KEYS_AT_DEPTH;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
    mode: false,
  } satisfies RequiredMap<JSONKeysAtDepthExprArgs>;

  declare args: JSONKeysAtDepthExprArgs;

  constructor (args: JSONKeysAtDepthExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  get $mode (): Expression | undefined {
    return this.args.mode;
  }

  static {
    this.register();
  }
}

export type JSONObjectExprArgs = { nullHandling?: Expression;
  uniqueKeys?: Expression[];
  returnType?: DataTypeExpr;
  encoding?: Expression; } & FuncExprArgs;

export class JSONObjectExpr extends FuncExpr {
  key = ExpressionKey.JSON_OBJECT;

  /**
   * Defines the arguments (properties and child expressions) for JSONObject expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    nullHandling: false,
    uniqueKeys: false,
    returnType: false,
    encoding: false,
  } satisfies RequiredMap<JSONObjectExprArgs>;

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

  get $returnType (): Expression | undefined {
    return this.args.returnType;
  }

  get $encoding (): Expression | undefined {
    return this.args.encoding;
  }

  static {
    this.register();
  }
}

export type JSONArrayExprArgs = { nullHandling?: Expression;
  returnType?: DataTypeExpr;
  strict?: Expression; } & FuncExprArgs;

export class JSONArrayExpr extends FuncExpr {
  key = ExpressionKey.JSON_ARRAY;

  /**
   * Defines the arguments (properties and child expressions) for JSONArray expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    nullHandling: false,
    returnType: false,
    strict: false,
  } satisfies RequiredMap<JSONArrayExprArgs>;

  declare args: JSONArrayExprArgs;

  constructor (args: JSONArrayExprArgs) {
    super(args);
  }

  get $nullHandling (): Expression | undefined {
    return this.args.nullHandling;
  }

  get $returnType (): Expression | undefined {
    return this.args.returnType;
  }

  get $strict (): Expression | undefined {
    return this.args.strict;
  }

  static {
    this.register();
  }
}

export type JSONExistsExprArgs = { path: Expression;
  passing?: Expression;
  onCondition?: Expression;
  fromDcolonqmark?: Expression; } & FuncExprArgs;

export class JSONExistsExpr extends FuncExpr {
  key = ExpressionKey.JSON_EXISTS;

  /**
   * Defines the arguments (properties and child expressions) for JSONExists expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    path: true,
    passing: false,
    onCondition: false,
    fromDcolonqmark: false,
  } satisfies RequiredMap<JSONExistsExprArgs>;

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

export type JSONSetExprArgs = {
  this: Expression;
  expressions: Expression[];
} & FuncExprArgs;

export class JSONSetExpr extends FuncExpr {
  key = ExpressionKey.JSON_SET;
  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
  } satisfies RequiredMap<JSONSetExprArgs>;

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

  static sqlNames = ['JSON_SET'];
}

export type JSONStripNullsExprArgs = { includeArrays?: Expression[];
  removeEmpty?: Expression; } & FuncExprArgs;

export class JSONStripNullsExpr extends FuncExpr {
  key = ExpressionKey.JSON_STRIP_NULLS;

  /**
   * Defines the arguments (properties and child expressions) for JSONStripNulls expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    includeArrays: false,
    removeEmpty: false,
  } satisfies RequiredMap<JSONStripNullsExprArgs>;

  declare args: JSONStripNullsExprArgs;

  constructor (args: JSONStripNullsExprArgs) {
    super(args);
  }

  get $includeArrays (): Expression[] | undefined {
    return this.args.includeArrays;
  }

  get $removeEmpty (): Expression | undefined {
    return this.args.removeEmpty;
  }

  static {
    this.register();
  }

  static sqlNames = ['JSON_STRIP_NULLS'];
}

export type JSONValueArrayExprArgs = {
  this: Expression;
  expression?: Expression;
} & FuncExprArgs;

export class JSONValueArrayExpr extends FuncExpr {
  key = ExpressionKey.JSON_VALUE_ARRAY;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<JSONValueArrayExprArgs>;

  declare args: JSONValueArrayExprArgs;

  constructor (args: JSONValueArrayExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type JSONRemoveExprArgs = {
  this: Expression;
  expressions: Expression[];
} & FuncExprArgs;

export class JSONRemoveExpr extends FuncExpr {
  key = ExpressionKey.JSON_REMOVE;
  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
  } satisfies RequiredMap<JSONRemoveExprArgs>;

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

  static sqlNames = ['JSON_REMOVE'];
}

export type JSONTableExprArgs = { schema: Expression;
  path?: Expression;
  errorHandling?: Expression;
  emptyHandling?: Expression; } & FuncExprArgs;

export class JSONTableExpr extends FuncExpr {
  key = ExpressionKey.JSON_TABLE;

  /**
   * Defines the arguments (properties and child expressions) for JSONTable expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    schema: true,
    path: false,
    errorHandling: false,
    emptyHandling: false,
  } satisfies RequiredMap<JSONTableExprArgs>;

  declare args: JSONTableExprArgs;

  constructor (args: JSONTableExprArgs) {
    super(args);
  }

  get $schema (): Expression {
    return this.args.schema;
  }

  get $path (): Expression | undefined {
    return this.args.path;
  }

  get $errorHandling (): Expression | undefined {
    return this.args.errorHandling;
  }

  get $emptyHandling (): Expression | undefined {
    return this.args.emptyHandling;
  }

  static {
    this.register();
  }
}

export type JSONTypeExprArgs = {
  this: Expression;
  expression?: Expression;
} & FuncExprArgs;

export class JSONTypeExpr extends FuncExpr {
  key = ExpressionKey.JSON_TYPE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<JSONTypeExprArgs>;

  declare args: JSONTypeExprArgs;

  constructor (args: JSONTypeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }

  static sqlNames = ['JSON_TYPE'];
}

export type ObjectInsertExprArgs = { key: unknown;
  value: string;
  updateFlag?: Expression; } & FuncExprArgs;

export class ObjectInsertExpr extends FuncExpr {
  key = ExpressionKey.OBJECT_INSERT;

  /**
   * Defines the arguments (properties and child expressions) for ObjectInsert expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    key: true,
    value: true,
    updateFlag: false,
  } satisfies RequiredMap<ObjectInsertExprArgs>;

  declare args: ObjectInsertExprArgs;

  constructor (args: ObjectInsertExprArgs) {
    super(args);
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

export type OpenJSONExprArgs = {
  this: Expression;
  path?: Expression;
  expressions?: Expression[];
} & FuncExprArgs;

export class OpenJSONExpr extends FuncExpr {
  key = ExpressionKey.OPEN_JSON;

  static argTypes = {
    ...super.argTypes,
    this: true,
    path: false,
    expressions: false,
  } satisfies RequiredMap<OpenJSONExprArgs>;

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

export type JSONBContainsExprArgs = BinaryExprArgs;
export class JSONBContainsExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.JSONB_CONTAINS;
  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  } satisfies RequiredMap<JSONBContainsExprArgs>;

  declare args: JSONBContainsExprArgs;
  constructor (args: JSONBContainsExprArgs) {
    super(args);
  }
}

export type JSONBContainsAnyTopKeysExprArgs = BinaryExprArgs;
export class JSONBContainsAnyTopKeysExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.JSONB_CONTAINS_ANY_TOP_KEYS;
  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  } satisfies RequiredMap<JSONBContainsAnyTopKeysExprArgs>;

  declare args: JSONBContainsAnyTopKeysExprArgs;
  constructor (args: JSONBContainsAnyTopKeysExprArgs) {
    super(args);
  }
}

export type JSONBContainsAllTopKeysExprArgs = BinaryExprArgs;
export class JSONBContainsAllTopKeysExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.JSONB_CONTAINS_ALL_TOP_KEYS;
  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  } satisfies RequiredMap<JSONBContainsAllTopKeysExprArgs>;

  declare args: JSONBContainsAllTopKeysExprArgs;
  constructor (args: JSONBContainsAllTopKeysExprArgs) {
    super(args);
  }
}

export type JSONBExistsExprArgs = {
  this: Expression;
  path: Expression;
} & FuncExprArgs;

export class JSONBExistsExpr extends FuncExpr {
  key = ExpressionKey.JSONB_EXISTS;

  static argTypes = {
    ...super.argTypes,
    this: true,
    path: true,
  } satisfies RequiredMap<JSONBExistsExprArgs>;

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

  static sqlNames = ['JSONB_EXISTS'];
}

export type JSONBDeleteAtPathExprArgs = BinaryExprArgs;
export class JSONBDeleteAtPathExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.JSONB_DELETE_AT_PATH;
  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  } satisfies RequiredMap<JSONBDeleteAtPathExprArgs>;

  declare args: JSONBDeleteAtPathExprArgs;
  constructor (args: JSONBDeleteAtPathExprArgs) {
    super(args);
  }
}

export type JSONExtractExprArgs = { onlyJsonTypes?: Expression[];
  variantExtract?: string;
  jsonQuery?: Expression;
  option?: Expression;
  quote?: Expression;
  onCondition?: Expression;
  requiresJson?: Expression; } & BinaryExprArgs;

export class JSONExtractExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.JSON_EXTRACT;
    static isVarLenArgs = true;
  /**
   * Defines the arguments (properties and child expressions) for JSONExtract expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    onlyJsonTypes: false,
    variantExtract: false,
    jsonQuery: false,
    option: false,
    quote: false,
    onCondition: false,
    requiresJson: false,
  } satisfies RequiredMap<JSONExtractExprArgs>;

  declare args: JSONExtractExprArgs;

  constructor (args: JSONExtractExprArgs) {
    super(args);
  }

  get $onlyJsonTypes (): Expression[] | undefined {
    return this.args.onlyJsonTypes;
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
    return !this.args.expressions ? this.expression.outputName : '';
  }
}

export type JSONExtractArrayExprArgs = {
  this: Expression;
  expression?: Expression;
} & FuncExprArgs;

export class JSONExtractArrayExpr extends FuncExpr {
  key = ExpressionKey.JSON_EXTRACT_ARRAY;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<JSONExtractArrayExprArgs>;

  declare args: JSONExtractArrayExprArgs;

  constructor (args: JSONExtractArrayExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }

  static sqlNames = ['JSON_EXTRACT_ARRAY'];
}

export type JSONExtractScalarExprArgs = { onlyJsonTypes?: Expression[];
  jsonType?: Expression;
  scalarOnly?: Expression; } & BinaryExprArgs;

export class JSONExtractScalarExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.JSON_EXTRACT_SCALAR;
    static isVarLenArgs = true;
  /**
   * Defines the arguments (properties and child expressions) for JSONExtractScalar expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    onlyJsonTypes: false,
    jsonType: false,
    scalarOnly: false,
  } satisfies RequiredMap<JSONExtractScalarExprArgs>;

  declare args: JSONExtractScalarExprArgs;

  constructor (args: JSONExtractScalarExprArgs) {
    super(args);
  }

  get $onlyJsonTypes (): Expression[] | undefined {
    return this.args.onlyJsonTypes;
  }

  get $jsonType (): Expression | undefined {
    return this.args.jsonType;
  }

  get $scalarOnly (): boolean | undefined {
    return this.args.scalarOnly;
  }

  get outputName (): string {
    return this.expression.outputName;
  }
}

export type JSONBExtractExprArgs = BinaryExprArgs;
export class JSONBExtractExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.JSONB_EXTRACT;
  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  } satisfies RequiredMap<JSONBExtractExprArgs>;

  declare args: JSONBExtractExprArgs;
  constructor (args: JSONBExtractExprArgs) {
    super(args);
  }
}

export type JSONBExtractScalarExprArgs = { jsonType?: Expression } & BinaryExprArgs;

export class JSONBExtractScalarExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.JSONB_EXTRACT_SCALAR;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    jsonType: false,
  } satisfies RequiredMap<JSONBExtractScalarExprArgs>;

  declare args: JSONBExtractScalarExprArgs;

  constructor (args: JSONBExtractScalarExprArgs) {
    super(args);
  }

  get $jsonType (): Expression | undefined {
    return this.args.jsonType;
  }
}

export type JSONFormatExprArgs = {
  this?: Expression;
  options?: Expression[];
  isJson?: Expression;
  toJson?: Expression;
} & FuncExprArgs;

export class JSONFormatExpr extends FuncExpr {
  key = ExpressionKey.JSON_FORMAT;

  /**
   * Defines the arguments (properties and child expressions) for JSONFormat expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: false,
    options: false,
    isJson: false,
    toJson: false,
  } satisfies RequiredMap<JSONFormatExprArgs>;

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

  static sqlNames = ['JSON_FORMAT'];
}

export type JSONArrayAppendExprArgs = {
  this: Expression;
  expressions: Expression[];
} & FuncExprArgs;

export class JSONArrayAppendExpr extends FuncExpr {
  key = ExpressionKey.JSON_ARRAY_APPEND;
  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
  } satisfies RequiredMap<JSONArrayAppendExprArgs>;

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

  static sqlNames = ['JSON_ARRAY_APPEND'];
}

export type JSONArrayContainsExprArgs = { jsonType?: Expression } & BinaryExprArgs;

export class JSONArrayContainsExpr extends multiInherit(BinaryExpr, PredicateExpr, FuncExpr) {
  key = ExpressionKey.JSON_ARRAY_CONTAINS;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    jsonType: false,
  } satisfies RequiredMap<JSONArrayContainsExprArgs>;

  declare args: JSONArrayContainsExprArgs;

  constructor (args: JSONArrayContainsExprArgs) {
    super(args);
  }

  get $jsonType (): Expression | undefined {
    return this.args.jsonType;
  }
}

export type JSONArrayInsertExprArgs = {
  this: Expression;
  expressions: Expression[];
} & FuncExprArgs;

export class JSONArrayInsertExpr extends FuncExpr {
  key = ExpressionKey.JSON_ARRAY_INSERT;
  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
  } satisfies RequiredMap<JSONArrayInsertExprArgs>;

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

  static sqlNames = ['JSON_ARRAY_INSERT'];
}

export type ParseBignumericExprArgs = FuncExprArgs;

export class ParseBignumericExpr extends FuncExpr {
  key = ExpressionKey.PARSE_BIGNUMERIC;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ParseBignumericExprArgs>;

  declare args: ParseBignumericExprArgs;

  constructor (args: ParseBignumericExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ParseNumericExprArgs = FuncExprArgs;

export class ParseNumericExpr extends FuncExpr {
  key = ExpressionKey.PARSE_NUMERIC;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ParseNumericExprArgs>;

  declare args: ParseNumericExprArgs;

  constructor (args: ParseNumericExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ParseJSONExprArgs = {
  this: Expression;
  expression?: Expression;
  safe?: boolean;
} & FuncExprArgs;

export class ParseJSONExpr extends FuncExpr {
  key = ExpressionKey.PARSE_JSON;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
    safe: false,
  } satisfies RequiredMap<ParseJSONExprArgs>;

  declare args: ParseJSONExprArgs;

  constructor (args: ParseJSONExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  get $safe (): Expression | undefined {
    return this.args.safe;
  }

  static {
    this.register();
  }

  static sqlNames = ['PARSE_JSON', 'JSON_PARSE'];
}

export type ParseUrlExprArgs = {
  this: Expression;
  partToExtract?: Expression;
  key?: unknown;
  permissive?: Expression;
} & FuncExprArgs;

export class ParseUrlExpr extends FuncExpr {
  key = ExpressionKey.PARSE_URL;

  /**
   * Defines the arguments (properties and child expressions) for ParseUrl expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    partToExtract: false,
    key: false,
    permissive: false,
  } satisfies RequiredMap<ParseUrlExprArgs>;

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

  get $key (): Expression | undefined {
    return this.args.key;
  }

  get $permissive (): Expression | undefined {
    return this.args.permissive;
  }

  static {
    this.register();
  }
}

export type ParseIpExprArgs = {
  this: Expression;
  type: Expression;
  permissive?: Expression;
} & FuncExprArgs;

export class ParseIpExpr extends FuncExpr {
  key = ExpressionKey.PARSE_IP;

  static argTypes = {
    ...super.argTypes,
    this: true,
    type: true,
    permissive: false,
  } satisfies RequiredMap<ParseIpExprArgs>;

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

export type ParseTimeExprArgs = {
  this: Expression;
  format: string;
} & FuncExprArgs;

export class ParseTimeExpr extends FuncExpr {
  key = ExpressionKey.PARSE_TIME;

  static argTypes = {
    ...super.argTypes,
    this: true,
    format: true,
  } satisfies RequiredMap<ParseTimeExprArgs>;

  declare args: ParseTimeExprArgs;

  constructor (args: ParseTimeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $format (): Expression {
    return this.args.format;
  }

  static {
    this.register();
  }
}

export type ParseDatetimeExprArgs = {
  this: Expression;
  format?: string;
  zone?: Expression;
} & FuncExprArgs;

export class ParseDatetimeExpr extends FuncExpr {
  key = ExpressionKey.PARSE_DATETIME;

  /**
   * Defines the arguments (properties and child expressions) for ParseDatetime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    format: false,
    zone: false,
  } satisfies RequiredMap<ParseDatetimeExprArgs>;

  declare args: ParseDatetimeExprArgs;

  constructor (args: ParseDatetimeExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $format (): Expression | undefined {
    return this.args.format;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  static {
    this.register();
  }
}

export type LeastExprArgs = {
  this: Expression;
  expressions?: Expression[];
  ignoreNulls: Expression[];
} & FuncExprArgs;

export class LeastExpr extends FuncExpr {
  key = ExpressionKey.LEAST;
  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
    ignoreNulls: true,
  } satisfies RequiredMap<LeastExprArgs>;

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

  get $ignoreNulls (): Expression[] {
    return this.args.ignoreNulls;
  }

  static {
    this.register();
  }
}

export type LeftExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class LeftExpr extends FuncExpr {
  key = ExpressionKey.LEFT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<LeftExprArgs>;

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

export type RightExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class RightExpr extends FuncExpr {
  key = ExpressionKey.RIGHT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<RightExprArgs>;

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

export type ReverseExprArgs = FuncExprArgs;

export class ReverseExpr extends FuncExpr {
  key = ExpressionKey.REVERSE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ReverseExprArgs>;

  declare args: ReverseExprArgs;

  constructor (args: ReverseExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LengthExprArgs = {
  this: Expression;
  binary?: Expression;
  encoding?: Expression;
} & FuncExprArgs;

export class LengthExpr extends FuncExpr {
  key = ExpressionKey.LENGTH;

  /**
   * Defines the arguments (properties and child expressions) for Length expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    binary: false,
    encoding: false,
  } satisfies RequiredMap<LengthExprArgs>;

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

  static sqlNames = [
    'LENGTH',
    'LEN',
    'CHAR_LENGTH',
    'CHARACTER_LENGTH',
  ];
}

export type RtrimmedLengthExprArgs = FuncExprArgs;

export class RtrimmedLengthExpr extends FuncExpr {
  key = ExpressionKey.RTRIMMED_LENGTH;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<RtrimmedLengthExprArgs>;

  declare args: RtrimmedLengthExprArgs;

  constructor (args: RtrimmedLengthExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitLengthExprArgs = FuncExprArgs;

export class BitLengthExpr extends FuncExpr {
  key = ExpressionKey.BIT_LENGTH;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<BitLengthExprArgs>;

  declare args: BitLengthExprArgs;

  constructor (args: BitLengthExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LevenshteinExprArgs = { insCost?: Expression;
  delCost?: Expression;
  subCost?: Expression;
  maxDist?: Expression; } & FuncExprArgs;

export class LevenshteinExpr extends FuncExpr {
  key = ExpressionKey.LEVENSHTEIN;

  /**
   * Defines the arguments (properties and child expressions) for Levenshtein expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    insCost: false,
    delCost: false,
    subCost: false,
    maxDist: false,
  } satisfies RequiredMap<LevenshteinExprArgs>;

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

export type LnExprArgs = FuncExprArgs;

export class LnExpr extends FuncExpr {
  key = ExpressionKey.LN;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<LnExprArgs>;

  declare args: LnExprArgs;

  constructor (args: LnExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LogExprArgs = {
  this: Expression;
  expression?: Expression;
} & FuncExprArgs;

export class LogExpr extends FuncExpr {
  key = ExpressionKey.LOG;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<LogExprArgs>;

  declare args: LogExprArgs;

  constructor (args: LogExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type LowerExprArgs = FuncExprArgs;

export class LowerExpr extends FuncExpr {
  key = ExpressionKey.LOWER;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<LowerExprArgs>;

  declare args: LowerExprArgs;

  constructor (args: LowerExprArgs) {
    super(args);
  }

  static sqlNames = ['LOWER', 'LCASE'];

  static {
    this.register();
  }
}

export type MapExprArgs = { keys?: Expression[];
  values?: Expression[]; } & FuncExprArgs;

export class MapExpr extends FuncExpr {
  key = ExpressionKey.MAP;

  /**
   * Defines the arguments (properties and child expressions) for Map expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    keys: false,
    values: false,
  } satisfies RequiredMap<MapExprArgs>;

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

  get keys (): Expression[] {
    const keysArg = this.args.keys;
    return keysArg?.[0]?.args?.expressions || [];
  }

  get values (): Expression[] {
    const valuesArg = this.args.values;
    return valuesArg?.[0]?.args?.expressions || [];
  }
}

export type ToMapExprArgs = FuncExprArgs;

export class ToMapExpr extends FuncExpr {
  key = ExpressionKey.TO_MAP;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ToMapExprArgs>;

  declare args: ToMapExprArgs;

  constructor (args: ToMapExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MapFromEntriesExprArgs = FuncExprArgs;

export class MapFromEntriesExpr extends FuncExpr {
  key = ExpressionKey.MAP_FROM_ENTRIES;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<MapFromEntriesExprArgs>;

  declare args: MapFromEntriesExprArgs;

  constructor (args: MapFromEntriesExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MapCatExprArgs = {
  this: Expression;
  expression: Expression;
} & FuncExprArgs;

export class MapCatExpr extends FuncExpr {
  key = ExpressionKey.MAP_CAT;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<MapCatExprArgs>;

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

export type MapContainsKeyExprArgs = {
  this: Expression;
  key: unknown;
} & FuncExprArgs;

export class MapContainsKeyExpr extends FuncExpr {
  key = ExpressionKey.MAP_CONTAINS_KEY;

  static argTypes = {
    ...super.argTypes,
    this: true,
    key: true,
  } satisfies RequiredMap<MapContainsKeyExprArgs>;

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

export type MapDeleteExprArgs = {
  this: Expression;
  expressions: Expression[];
} & FuncExprArgs;

export class MapDeleteExpr extends FuncExpr {
  key = ExpressionKey.MAP_DELETE;
  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
  } satisfies RequiredMap<MapDeleteExprArgs>;

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

export type MapInsertExprArgs = {
  this: Expression;
  key?: unknown;
  value: string;
  updateFlag?: Expression;
} & FuncExprArgs;

export class MapInsertExpr extends FuncExpr {
  key = ExpressionKey.MAP_INSERT;

  /**
   * Defines the arguments (properties and child expressions) for MapInsert expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    key: false,
    value: true,
    updateFlag: false,
  } satisfies RequiredMap<MapInsertExprArgs>;

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

export type MapKeysExprArgs = FuncExprArgs;

export class MapKeysExpr extends FuncExpr {
  key = ExpressionKey.MAP_KEYS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<MapKeysExprArgs>;

  declare args: MapKeysExprArgs;

  constructor (args: MapKeysExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MapPickExprArgs = {
  this: Expression;
  expressions: Expression[];
} & FuncExprArgs;

export class MapPickExpr extends FuncExpr {
  key = ExpressionKey.MAP_PICK;
  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
  } satisfies RequiredMap<MapPickExprArgs>;

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

export type MapSizeExprArgs = FuncExprArgs;

export class MapSizeExpr extends FuncExpr {
  key = ExpressionKey.MAP_SIZE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<MapSizeExprArgs>;

  declare args: MapSizeExprArgs;

  constructor (args: MapSizeExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StarMapExprArgs = FuncExprArgs;

export class StarMapExpr extends FuncExpr {
  key = ExpressionKey.STAR_MAP;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<StarMapExprArgs>;

  declare args: StarMapExprArgs;

  constructor (args: StarMapExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type VarMapExprArgs = { keys: Expression[];
  values: Expression[]; } & FuncExprArgs;

export class VarMapExpr extends FuncExpr {
  key = ExpressionKey.VAR_MAP;
    static isVarLenArgs = true;
  /**
   * Defines the arguments (properties and child expressions) for VarMap expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    keys: true,
    values: true,
  } satisfies RequiredMap<VarMapExprArgs>;

  declare args: VarMapExprArgs;

  constructor (args: VarMapExprArgs) {
    super(args);
  }

  get $keys (): Expression[] {
    return this.args.keys;
  }

  get $values (): Expression[] {
    return this.args.values;
  }

  get keys (): Expression[] {
    const keysArg = this.args.keys;
    return keysArg?.[0]?.args?.expressions || [];
  }

  get values (): Expression[] {
    const valuesArg = this.args.values;
    return valuesArg?.[0]?.args?.expressions || [];
  }

  static {
    this.register();
  }
}

export type MatchAgainstExprArgs = { modifier?: Expression } & FuncExprArgs;

export class MatchAgainstExpr extends FuncExpr {
  key = ExpressionKey.MATCH_AGAINST;

  static argTypes = { modifier: false } satisfies RequiredMap<MatchAgainstExprArgs>;

  declare args: MatchAgainstExprArgs;

  constructor (args: MatchAgainstExprArgs) {
    super(args);
  }

  get $modifier (): Expression | undefined {
    return this.args.modifier;
  }

  static {
    this.register();
  }
}

export type MD5ExprArgs = FuncExprArgs;

export class MD5Expr extends FuncExpr {
  key = ExpressionKey.MD5;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<MD5ExprArgs>;

  declare args: MD5ExprArgs;

  constructor (args: MD5ExprArgs) {
    super(args);
  }

  static sqlNames = ['MD5'];

  static {
    this.register();
  }
}

export type MD5DigestExprArgs = FuncExprArgs;

export class MD5DigestExpr extends FuncExpr {
  key = ExpressionKey.MD5_DIGEST;
    static isVarLenArgs = true;
  static argTypes = {} satisfies RequiredMap<MD5DigestExprArgs>;

  declare args: MD5DigestExprArgs;

  constructor (args: MD5DigestExprArgs) {
    super(args);
  }

  static {
    this.register();
  }

  static sqlNames = ['MD5_DIGEST'];
}

export type MD5NumberLower64ExprArgs = FuncExprArgs;

export class MD5NumberLower64Expr extends FuncExpr {
  key = ExpressionKey.MD5_NUMBER_LOWER64;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<MD5NumberLower64ExprArgs>;

  declare args: MD5NumberLower64ExprArgs;

  constructor (args: MD5NumberLower64ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MD5NumberUpper64ExprArgs = FuncExprArgs;

export class MD5NumberUpper64Expr extends FuncExpr {
  key = ExpressionKey.MD5_NUMBER_UPPER64;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<MD5NumberUpper64ExprArgs>;

  declare args: MD5NumberUpper64ExprArgs;

  constructor (args: MD5NumberUpper64ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MonthExprArgs = FuncExprArgs;

export class MonthExpr extends FuncExpr {
  key = ExpressionKey.MONTH;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<MonthExprArgs>;

  declare args: MonthExprArgs;

  constructor (args: MonthExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MonthnameExprArgs = { abbreviated?: Expression } & FuncExprArgs;

export class MonthnameExpr extends FuncExpr {
  key = ExpressionKey.MONTHNAME;

  static argTypes = { abbreviated: false } satisfies RequiredMap<MonthnameExprArgs>;

  declare args: MonthnameExprArgs;

  constructor (args: MonthnameExprArgs) {
    super(args);
  }

  get $abbreviated (): Expression | undefined {
    return this.args.abbreviated;
  }

  static {
    this.register();
  }
}

export type AddMonthsExprArgs = { preserveEndOfMonth?: Expression } & FuncExprArgs;

export class AddMonthsExpr extends FuncExpr {
  key = ExpressionKey.ADD_MONTHS;

  static argTypes = { preserveEndOfMonth: false } satisfies RequiredMap<AddMonthsExprArgs>;

  declare args: AddMonthsExprArgs;

  constructor (args: AddMonthsExprArgs) {
    super(args);
  }

  get $preserveEndOfMonth (): Expression | undefined {
    return this.args.preserveEndOfMonth;
  }

  static {
    this.register();
  }
}

export type Nvl2ExprArgs = { true: Expression;
  false?: Expression; } & FuncExprArgs;

export class Nvl2Expr extends FuncExpr {
  key = ExpressionKey.NVL2;

  /**
   * Defines the arguments (properties and child expressions) for Nvl2 expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    true: true,
    false: false,
  } satisfies RequiredMap<Nvl2ExprArgs>;

  declare args: Nvl2ExprArgs;

  constructor (args: Nvl2ExprArgs) {
    super(args);
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

export type NormalizeExprArgs = { form?: Expression;
  isCasefold?: Expression; } & FuncExprArgs;

export class NormalizeExpr extends FuncExpr {
  key = ExpressionKey.NORMALIZE;

  /**
   * Defines the arguments (properties and child expressions) for Normalize expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    form: false,
    isCasefold: false,
  } satisfies RequiredMap<NormalizeExprArgs>;

  declare args: NormalizeExprArgs;

  constructor (args: NormalizeExprArgs) {
    super(args);
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

export type NormalExprArgs = { stddev: Expression;
  gen: Expression; } & FuncExprArgs;

export class NormalExpr extends FuncExpr {
  key = ExpressionKey.NORMAL;

  /**
   * Defines the arguments (properties and child expressions) for Normal expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    stddev: true,
    gen: true,
  } satisfies RequiredMap<NormalExprArgs>;

  declare args: NormalExprArgs;

  constructor (args: NormalExprArgs) {
    super(args);
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

export type NetFuncExprArgs = BaseExpressionArgs;

export class NetFuncExpr extends FuncExpr {
  key = ExpressionKey.NET_FUNC;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<NetFuncExprArgs>;

  declare args: NetFuncExprArgs;

  constructor (args: NetFuncExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type HostExprArgs = FuncExprArgs;

export class HostExpr extends FuncExpr {
  key = ExpressionKey.HOST;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<HostExprArgs>;

  declare args: HostExprArgs;

  constructor (args: HostExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegDomainExprArgs = FuncExprArgs;

export class RegDomainExpr extends FuncExpr {
  key = ExpressionKey.REG_DOMAIN;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<RegDomainExprArgs>;

  declare args: RegDomainExprArgs;

  constructor (args: RegDomainExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type OverlayExprArgs = { from: Expression;
  for?: Expression; } & FuncExprArgs;

export class OverlayExpr extends FuncExpr {
  key = ExpressionKey.OVERLAY;

  /**
   * Defines the arguments (properties and child expressions) for Overlay expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    from: true,
    for: false,
  } satisfies RequiredMap<OverlayExprArgs>;

  declare args: OverlayExprArgs;

  constructor (args: OverlayExprArgs) {
    super(args);
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

export type PredictExprArgs = { paramsStruct?: Expression } & FuncExprArgs;

export class PredictExpr extends FuncExpr {
  key = ExpressionKey.PREDICT;

  static argTypes = { paramsStruct: false } satisfies RequiredMap<PredictExprArgs>;

  declare args: PredictExprArgs;

  constructor (args: PredictExprArgs) {
    super(args);
  }

  get $paramsStruct (): Expression | undefined {
    return this.args.paramsStruct;
  }

  static {
    this.register();
  }
}

export type MLTranslateExprArgs = { paramsStruct: Expression } & FuncExprArgs;

export class MLTranslateExpr extends FuncExpr {
  key = ExpressionKey.ML_TRANSLATE;

  static argTypes = { paramsStruct: true } satisfies RequiredMap<MLTranslateExprArgs>;

  declare args: MLTranslateExprArgs;

  constructor (args: MLTranslateExprArgs) {
    super(args);
  }

  get $paramsStruct (): Expression {
    return this.args.paramsStruct;
  }

  static {
    this.register();
  }
}

export type FeaturesAtTimeExprArgs = { time?: Expression;
  numRows?: Expression[];
  ignoreFeatureNulls?: Expression[]; } & FuncExprArgs;

export class FeaturesAtTimeExpr extends FuncExpr {
  key = ExpressionKey.FEATURES_AT_TIME;

  /**
   * Defines the arguments (properties and child expressions) for FeaturesAtTime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    time: false,
    numRows: false,
    ignoreFeatureNulls: false,
  } satisfies RequiredMap<FeaturesAtTimeExprArgs>;

  declare args: FeaturesAtTimeExprArgs;

  constructor (args: FeaturesAtTimeExprArgs) {
    super(args);
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

export type GenerateEmbeddingExprArgs = { paramsStruct?: Expression;
  isText?: string; } & FuncExprArgs;

export class GenerateEmbeddingExpr extends FuncExpr {
  key = ExpressionKey.GENERATE_EMBEDDING;

  /**
   * Defines the arguments (properties and child expressions) for GenerateEmbedding expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    paramsStruct: false,
    isText: false,
  } satisfies RequiredMap<GenerateEmbeddingExprArgs>;

  declare args: GenerateEmbeddingExprArgs;

  constructor (args: GenerateEmbeddingExprArgs) {
    super(args);
  }

  get $paramsStruct (): Expression | undefined {
    return this.args.paramsStruct;
  }

  get $isText (): Expression | undefined {
    return this.args.isText;
  }

  static {
    this.register();
  }
}

export type MLForecastExprArgs = { paramsStruct?: Expression } & FuncExprArgs;

export class MLForecastExpr extends FuncExpr {
  key = ExpressionKey.ML_FORECAST;

  static argTypes = { paramsStruct: false } satisfies RequiredMap<MLForecastExprArgs>;

  declare args: MLForecastExprArgs;

  constructor (args: MLForecastExprArgs) {
    super(args);
  }

  get $paramsStruct (): Expression | undefined {
    return this.args.paramsStruct;
  }

  static {
    this.register();
  }
}

export type VectorSearchExprArgs = { columnToSearch: Expression;
  queryTable: Expression;
  queryColumnToSearch?: Expression;
  topK?: Expression;
  distanceType?: DataTypeExpr;
  options?: Expression[]; } & FuncExprArgs;

export class VectorSearchExpr extends FuncExpr {
  key = ExpressionKey.VECTOR_SEARCH;

  /**
   * Defines the arguments (properties and child expressions) for VectorSearch expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    columnToSearch: true,
    queryTable: true,
    queryColumnToSearch: false,
    topK: false,
    distanceType: false,
    options: false,
  } satisfies RequiredMap<VectorSearchExprArgs>;

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

  get $distanceType (): Expression | undefined {
    return this.args.distanceType;
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }

  static {
    this.register();
  }
}

export type PiExprArgs = FuncExprArgs;

export class PiExpr extends FuncExpr {
  key = ExpressionKey.PI;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<PiExprArgs>;

  declare args: PiExprArgs;

  constructor (args: PiExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type PowExprArgs = BinaryExprArgs;
export class PowExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.POW;
  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  } satisfies RequiredMap<PowExprArgs>;

  declare args: PowExprArgs;
  constructor (args: PowExprArgs) {
    super(args);
  }
}

export type ApproxPercentileEstimateExprArgs = { percentile: Expression } & FuncExprArgs;

export class ApproxPercentileEstimateExpr extends FuncExpr {
  key = ExpressionKey.APPROX_PERCENTILE_ESTIMATE;

  static argTypes = { percentile: true } satisfies RequiredMap<ApproxPercentileEstimateExprArgs>;

  declare args: ApproxPercentileEstimateExprArgs;

  constructor (args: ApproxPercentileEstimateExprArgs) {
    super(args);
  }

  get $percentile (): Expression {
    return this.args.percentile;
  }

  static {
    this.register();
  }
}

export type QuarterExprArgs = FuncExprArgs;

export class QuarterExpr extends FuncExpr {
  key = ExpressionKey.QUARTER;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<QuarterExprArgs>;

  declare args: QuarterExprArgs;

  constructor (args: QuarterExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RandExprArgs = { lower?: Expression;
  upper?: Expression; } & FuncExprArgs;

export class RandExpr extends FuncExpr {
  key = ExpressionKey.RAND;

  /**
   * Defines the arguments (properties and child expressions) for Rand expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    lower: false,
    upper: false,
  } satisfies RequiredMap<RandExprArgs>;

  declare args: RandExprArgs;

  constructor (args: RandExprArgs) {
    super(args);
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

  static sqlNames = ['RAND', 'RANDOM'];
}

export type RandnExprArgs = FuncExprArgs;

export class RandnExpr extends FuncExpr {
  key = ExpressionKey.RANDN;

  static argTypes = {} satisfies RequiredMap<RandnExprArgs>;

  declare args: RandnExprArgs;

  constructor (args: RandnExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RandstrExprArgs = { generator?: Expression } & FuncExprArgs;

export class RandstrExpr extends FuncExpr {
  key = ExpressionKey.RANDSTR;

  static argTypes = { generator: false } satisfies RequiredMap<RandstrExprArgs>;

  declare args: RandstrExprArgs;

  constructor (args: RandstrExprArgs) {
    super(args);
  }

  get $generator (): Expression | undefined {
    return this.args.generator;
  }

  static {
    this.register();
  }
}

export type RangeNExprArgs = { each?: Expression } & FuncExprArgs;

export class RangeNExpr extends FuncExpr {
  key = ExpressionKey.RANGE_N;

  static argTypes = { each: false } satisfies RequiredMap<RangeNExprArgs>;

  declare args: RangeNExprArgs;

  constructor (args: RangeNExprArgs) {
    super(args);
  }

  get $each (): Expression | undefined {
    return this.args.each;
  }

  static {
    this.register();
  }
}

export type RangeBucketExprArgs = FuncExprArgs;

export class RangeBucketExpr extends FuncExpr {
  key = ExpressionKey.RANGE_BUCKET;

  static argTypes = {} satisfies RequiredMap<RangeBucketExprArgs>;

  declare args: RangeBucketExprArgs;

  constructor (args: RangeBucketExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ReadCSVExprArgs = FuncExprArgs;

export class ReadCSVExpr extends FuncExpr {
  key = ExpressionKey.READ_CSV;
    static isVarLenArgs = true;
  static argTypes = {} satisfies RequiredMap<ReadCSVExprArgs>;

  declare args: ReadCSVExprArgs;

  constructor (args: ReadCSVExprArgs) {
    super(args);
  }

  static {
    this.register();
  }

  static sqlNames = ['READ_CSV'];
}

export type ReadParquetExprArgs = FuncExprArgs;

export class ReadParquetExpr extends FuncExpr {
  key = ExpressionKey.READ_PARQUET;
    static isVarLenArgs = true;
  static argTypes = {} satisfies RequiredMap<ReadParquetExprArgs>;

  declare args: ReadParquetExprArgs;

  constructor (args: ReadParquetExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ReduceExprArgs = { initial: Expression;
  merge: Expression;
  finish?: Expression; } & FuncExprArgs;

export class ReduceExpr extends FuncExpr {
  key = ExpressionKey.REDUCE;

  /**
   * Defines the arguments (properties and child expressions) for Reduce expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    initial: true,
    merge: true,
    finish: false,
  } satisfies RequiredMap<ReduceExprArgs>;

  declare args: ReduceExprArgs;

  constructor (args: ReduceExprArgs) {
    super(args);
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

export type RegexpExtractExprArgs = { position?: Expression;
  occurrence?: Expression;
  parameters?: Expression[];
  group?: Expression;
  nullIfPosOverflow?: Expression; } & FuncExprArgs;

export class RegexpExtractExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_EXTRACT;

  /**
   * Defines the arguments (properties and child expressions) for RegexpExtract expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    position: false,
    occurrence: false,
    parameters: false,
    group: false,
    nullIfPosOverflow: false,
  } satisfies RequiredMap<RegexpExtractExprArgs>;

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

export type RegexpExtractAllExprArgs = { group?: Expression;
  parameters?: Expression[];
  position?: Expression;
  occurrence?: Expression; } & FuncExprArgs;

export class RegexpExtractAllExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_EXTRACT_ALL;

  /**
   * Defines the arguments (properties and child expressions) for RegexpExtractAll expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    group: false,
    parameters: false,
    position: false,
    occurrence: false,
  } satisfies RequiredMap<RegexpExtractAllExprArgs>;

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

export type RegexpReplaceExprArgs = { replacement?: boolean;
  position?: Expression;
  occurrence?: Expression;
  modifiers?: Expression[];
  singleReplace?: Expression; } & FuncExprArgs;

export class RegexpReplaceExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_REPLACE;

  /**
   * Defines the arguments (properties and child expressions) for RegexpReplace expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    replacement: false,
    position: false,
    occurrence: false,
    modifiers: false,
    singleReplace: false,
  } satisfies RequiredMap<RegexpReplaceExprArgs>;

  declare args: RegexpReplaceExprArgs;

  constructor (args: RegexpReplaceExprArgs) {
    super(args);
  }

  get $replacement (): Expression | undefined {
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

export type RegexpLikeExprArgs = { flag?: Expression } & BinaryExprArgs;

export class RegexpLikeExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.REGEXP_LIKE;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    flag: false,
  } satisfies RequiredMap<RegexpLikeExprArgs>;

  declare args: RegexpLikeExprArgs;

  constructor (args: RegexpLikeExprArgs) {
    super(args);
  }

  get $flag (): Expression | undefined {
    return this.args.flag;
  }
}

export type RegexpILikeExprArgs = { flag?: Expression } & BinaryExprArgs;

export class RegexpILikeExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.REGEXP_ILIKE;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    flag: false,
  } satisfies RequiredMap<RegexpILikeExprArgs>;

  declare args: RegexpILikeExprArgs;

  constructor (args: RegexpILikeExprArgs) {
    super(args);
  }

  get $flag (): Expression | undefined {
    return this.args.flag;
  }
}

export type RegexpFullMatchExprArgs = { options?: Expression[] } & BinaryExprArgs;

export class RegexpFullMatchExpr extends multiInherit(BinaryExpr, FuncExpr) {
  key = ExpressionKey.REGEXP_FULL_MATCH;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    options: false,
  } satisfies RequiredMap<RegexpFullMatchExprArgs>;

  declare args: RegexpFullMatchExprArgs;

  constructor (args: RegexpFullMatchExprArgs) {
    super(args);
  }

  get $options (): Expression[] | undefined {
    return this.args.options;
  }
}

export type RegexpInstrExprArgs = { position?: Expression;
  occurrence?: Expression;
  option?: Expression;
  parameters?: Expression[];
  group?: Expression; } & FuncExprArgs;

export class RegexpInstrExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_INSTR;

  /**
   * Defines the arguments (properties and child expressions) for RegexpInstr expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    position: false,
    occurrence: false,
    option: false,
    parameters: false,
    group: false,
  } satisfies RequiredMap<RegexpInstrExprArgs>;

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

export type RegexpSplitExprArgs = { limit?: number | Expression } & FuncExprArgs;

export class RegexpSplitExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_SPLIT;

  static argTypes = { limit: false } satisfies RequiredMap<RegexpSplitExprArgs>;

  declare args: RegexpSplitExprArgs;

  constructor (args: RegexpSplitExprArgs) {
    super(args);
  }

  get $limit (): number | Expression | undefined {
    return this.args.limit;
  }

  static {
    this.register();
  }
}

export type RegexpCountExprArgs = {
  position?: Expression;
  parameters?: Expression[];
} & FuncExprArgs;

export class RegexpCountExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_COUNT;

  /**
   * Defines the arguments (properties and child expressions) for RegexpCount expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    position: false,
    parameters: false,
  } satisfies RequiredMap<RegexpCountExprArgs>;

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

export type RepeatExprArgs = { times: Expression[] } & FuncExprArgs;

export class RepeatExpr extends FuncExpr {
  key = ExpressionKey.REPEAT;

  static argTypes = { times: true } satisfies RequiredMap<RepeatExprArgs>;

  declare args: RepeatExprArgs;

  constructor (args: RepeatExprArgs) {
    super(args);
  }

  get $times (): Expression[] {
    return this.args.times;
  }

  static {
    this.register();
  }
}

export type ReplaceExprArgs = { replacement?: boolean } & FuncExprArgs;

export class ReplaceExpr extends FuncExpr {
  key = ExpressionKey.REPLACE;

  static argTypes = { replacement: false } satisfies RequiredMap<ReplaceExprArgs>;

  declare args: ReplaceExprArgs;

  constructor (args: ReplaceExprArgs) {
    super(args);
  }

  get $replacement (): Expression | undefined {
    return this.args.replacement;
  }

  static {
    this.register();
  }
}

export type RadiansExprArgs = FuncExprArgs;

export class RadiansExpr extends FuncExpr {
  key = ExpressionKey.RADIANS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<RadiansExprArgs>;

  declare args: RadiansExprArgs;

  constructor (args: RadiansExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RoundExprArgs = { decimals?: Expression[];
  truncate?: Expression;
  castsNonIntegerDecimals?: Expression[]; } & FuncExprArgs;

export class RoundExpr extends FuncExpr {
  key = ExpressionKey.ROUND;

  /**
   * Defines the arguments (properties and child expressions) for Round expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    decimals: false,
    truncate: false,
    castsNonIntegerDecimals: false,
  } satisfies RequiredMap<RoundExprArgs>;

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

export type TruncExprArgs = { decimals?: Expression[] } & FuncExprArgs;

export class TruncExpr extends FuncExpr {
  key = ExpressionKey.TRUNC;

  static argTypes = { decimals: false } satisfies RequiredMap<TruncExprArgs>;

  declare args: TruncExprArgs;

  constructor (args: TruncExprArgs) {
    super(args);
  }

  get $decimals (): Expression[] | undefined {
    return this.args.decimals;
  }

  static {
    this.register();
  }

  static sqlNames = ['TRUNC', 'TRUNCATE'];
}

export type RowNumberExprArgs = FuncExprArgs;

export class RowNumberExpr extends FuncExpr {
  key = ExpressionKey.ROW_NUMBER;

  static argTypes = {} satisfies RequiredMap<RowNumberExprArgs>;

  declare args: RowNumberExprArgs;

  constructor (args: RowNumberExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Seq1ExprArgs = FuncExprArgs;

export class Seq1Expr extends FuncExpr {
  key = ExpressionKey.SEQ1;

  static argTypes = {} satisfies RequiredMap<Seq1ExprArgs>;

  declare args: Seq1ExprArgs;

  constructor (args: Seq1ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Seq2ExprArgs = FuncExprArgs;

export class Seq2Expr extends FuncExpr {
  key = ExpressionKey.SEQ2;

  static argTypes = {} satisfies RequiredMap<Seq2ExprArgs>;

  declare args: Seq2ExprArgs;

  constructor (args: Seq2ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Seq4ExprArgs = FuncExprArgs;

export class Seq4Expr extends FuncExpr {
  key = ExpressionKey.SEQ4;

  static argTypes = {} satisfies RequiredMap<Seq4ExprArgs>;

  declare args: Seq4ExprArgs;

  constructor (args: Seq4ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Seq8ExprArgs = FuncExprArgs;

export class Seq8Expr extends FuncExpr {
  key = ExpressionKey.SEQ8;

  static argTypes = {} satisfies RequiredMap<Seq8ExprArgs>;

  declare args: Seq8ExprArgs;

  constructor (args: Seq8ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SafeAddExprArgs = FuncExprArgs;

export class SafeAddExpr extends FuncExpr {
  key = ExpressionKey.SAFE_ADD;

  static argTypes = {} satisfies RequiredMap<SafeAddExprArgs>;

  declare args: SafeAddExprArgs;

  constructor (args: SafeAddExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SafeDivideExprArgs = FuncExprArgs;

export class SafeDivideExpr extends FuncExpr {
  key = ExpressionKey.SAFE_DIVIDE;

  static argTypes = {} satisfies RequiredMap<SafeDivideExprArgs>;

  declare args: SafeDivideExprArgs;

  constructor (args: SafeDivideExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SafeMultiplyExprArgs = FuncExprArgs;

export class SafeMultiplyExpr extends FuncExpr {
  key = ExpressionKey.SAFE_MULTIPLY;

  static argTypes = {} satisfies RequiredMap<SafeMultiplyExprArgs>;

  declare args: SafeMultiplyExprArgs;

  constructor (args: SafeMultiplyExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SafeNegateExprArgs = FuncExprArgs;

export class SafeNegateExpr extends FuncExpr {
  key = ExpressionKey.SAFE_NEGATE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SafeNegateExprArgs>;

  declare args: SafeNegateExprArgs;

  constructor (args: SafeNegateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SafeSubtractExprArgs = FuncExprArgs;

export class SafeSubtractExpr extends FuncExpr {
  key = ExpressionKey.SAFE_SUBTRACT;

  static argTypes = {} satisfies RequiredMap<SafeSubtractExprArgs>;

  declare args: SafeSubtractExprArgs;

  constructor (args: SafeSubtractExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SafeConvertBytesToStringExprArgs = FuncExprArgs;

export class SafeConvertBytesToStringExpr extends FuncExpr {
  key = ExpressionKey.SAFE_CONVERT_BYTES_TO_STRING;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SafeConvertBytesToStringExprArgs>;

  declare args: SafeConvertBytesToStringExprArgs;

  constructor (args: SafeConvertBytesToStringExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SHAExprArgs = FuncExprArgs;

export class SHAExpr extends FuncExpr {
  key = ExpressionKey.SHA;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SHAExprArgs>;

  declare args: SHAExprArgs;

  constructor (args: SHAExprArgs) {
    super(args);
  }

  static sqlNames = ['SHA', 'SHA1'];

  static {
    this.register();
  }
}

export type SHA2ExprArgs = { length?: number | Expression } & FuncExprArgs;

export class SHA2Expr extends FuncExpr {
  key = ExpressionKey.SHA2;

  static argTypes = { length: false } satisfies RequiredMap<SHA2ExprArgs>;

  declare args: SHA2ExprArgs;

  static {
    this.register();
  }

  static sqlNames = ['SHA2'];
  constructor (args: SHA2ExprArgs) {
    super(args);
  }

  get $length (): Expression | undefined {
    return this.args.length;
  }
}

export type SHA1DigestExprArgs = FuncExprArgs;

export class SHA1DigestExpr extends FuncExpr {
  key = ExpressionKey.SHA1_DIGEST;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SHA1DigestExprArgs>;

  declare args: SHA1DigestExprArgs;

  constructor (args: SHA1DigestExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SHA2DigestExprArgs = { length?: number | Expression } & FuncExprArgs;

export class SHA2DigestExpr extends FuncExpr {
  key = ExpressionKey.SHA2_DIGEST;

  static argTypes = { length: false } satisfies RequiredMap<SHA2DigestExprArgs>;

  declare args: SHA2DigestExprArgs;

  constructor (args: SHA2DigestExprArgs) {
    super(args);
  }

  get $length (): Expression | undefined {
    return this.args.length;
  }

  static {
    this.register();
  }
}

export type SignExprArgs = FuncExprArgs;

export class SignExpr extends FuncExpr {
  key = ExpressionKey.SIGN;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SignExprArgs>;

  declare args: SignExprArgs;

  constructor (args: SignExprArgs) {
    super(args);
  }

  static sqlNames = ['SIGN', 'SIGNUM'];

  static {
    this.register();
  }
}

export type SortArrayExprArgs = { asc?: Expression;
  nullsFirst?: Expression; } & FuncExprArgs;

export class SortArrayExpr extends FuncExpr {
  key = ExpressionKey.SORT_ARRAY;

  /**
   * Defines the arguments (properties and child expressions) for SortArray expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    asc: false,
    nullsFirst: false,
  } satisfies RequiredMap<SortArrayExprArgs>;

  declare args: SortArrayExprArgs;

  static {
    this.register();
  }

  constructor (args: SortArrayExprArgs) {
    super(args);
  }

  get $asc (): Expression | undefined {
    return this.args.asc;
  }

  get $nullsFirst (): Expression | undefined {
    return this.args.nullsFirst;
  }
}

export type SoundexExprArgs = FuncExprArgs;

export class SoundexExpr extends FuncExpr {
  key = ExpressionKey.SOUNDEX;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SoundexExprArgs>;

  declare args: SoundexExprArgs;

  constructor (args: SoundexExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SoundexP123ExprArgs = FuncExprArgs;

export class SoundexP123Expr extends FuncExpr {
  key = ExpressionKey.SOUNDEX_P123;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SoundexP123ExprArgs>;

  declare args: SoundexP123ExprArgs;

  constructor (args: SoundexP123ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SplitExprArgs = { limit?: number | Expression } & FuncExprArgs;

export class SplitExpr extends FuncExpr {
  key = ExpressionKey.SPLIT;

  static argTypes = { limit: false } satisfies RequiredMap<SplitExprArgs>;

  declare args: SplitExprArgs;

  constructor (args: SplitExprArgs) {
    super(args);
  }

  get $limit (): Expression | undefined {
    return this.args.limit;
  }

  static {
    this.register();
  }
}

export type SplitPartExprArgs = { delimiter?: number | Expression;
  partIndex?: Expression; } & FuncExprArgs;

export class SplitPartExpr extends FuncExpr {
  key = ExpressionKey.SPLIT_PART;

  /**
   * Defines the arguments (properties and child expressions) for SplitPart expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    delimiter: false,
    partIndex: false,
  } satisfies RequiredMap<SplitPartExprArgs>;

  declare args: SplitPartExprArgs;

  constructor (args: SplitPartExprArgs) {
    super(args);
  }

  get $delimiter (): Expression | undefined {
    return this.args.delimiter;
  }

  get $partIndex (): Expression | undefined {
    return this.args.partIndex;
  }

  static {
    this.register();
  }
}

export type SubstringExprArgs = { start?: Expression;
  length?: number | Expression; } & FuncExprArgs;

export class SubstringExpr extends FuncExpr {
  key = ExpressionKey.SUBSTRING;

  /**
   * Defines the arguments (properties and child expressions) for Substring expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    start: false,
    length: false,
  } satisfies RequiredMap<SubstringExprArgs>;

  declare args: SubstringExprArgs;

  constructor (args: SubstringExprArgs) {
    super(args);
  }

  get $start (): Expression | undefined {
    return this.args.start;
  }

  get $length (): Expression | undefined {
    return this.args.length;
  }

  static {
    this.register();
  }

  static sqlNames = ['SUBSTRING', 'SUBSTR'];
}

export type SubstringIndexExprArgs = { delimiter: number | Expression;
  count: Expression; } & FuncExprArgs;

export class SubstringIndexExpr extends FuncExpr {
  key = ExpressionKey.SUBSTRING_INDEX;

  /**
   * Defines the arguments (properties and child expressions) for SubstringIndex expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    delimiter: true,
    count: true,
  } satisfies RequiredMap<SubstringIndexExprArgs>;

  declare args: SubstringIndexExprArgs;

  constructor (args: SubstringIndexExprArgs) {
    super(args);
  }

  get $delimiter (): Expression {
    return this.args.delimiter;
  }

  get $count (): Expression {
    return this.args.count;
  }

  static {
    this.register();
  }
}

export type StandardHashExprArgs = FuncExprArgs;

export class StandardHashExpr extends FuncExpr {
  key = ExpressionKey.STANDARD_HASH;

  static argTypes = {} satisfies RequiredMap<StandardHashExprArgs>;

  declare args: StandardHashExprArgs;

  constructor (args: StandardHashExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StartsWithExprArgs = FuncExprArgs;

export class StartsWithExpr extends FuncExpr {
  key = ExpressionKey.STARTS_WITH;

  static argTypes = {} satisfies RequiredMap<StartsWithExprArgs>;

  declare args: StartsWithExprArgs;

  constructor (args: StartsWithExprArgs) {
    super(args);
  }

  static {
    this.register();
  }

  static sqlNames = ['STARTS_WITH', 'STARTSWITH'];
}

export type EndsWithExprArgs = FuncExprArgs;

export class EndsWithExpr extends FuncExpr {
  key = ExpressionKey.ENDS_WITH;

  static argTypes = {} satisfies RequiredMap<EndsWithExprArgs>;

  declare args: EndsWithExprArgs;

  constructor (args: EndsWithExprArgs) {
    super(args);
  }

  static {
    this.register();
  }

  static sqlNames = ['ENDS_WITH', 'ENDSWITH'];
}

export type StrPositionExprArgs = { substr: Expression;
  position?: Expression;
  occurrence?: Expression; } & FuncExprArgs;

export class StrPositionExpr extends FuncExpr {
  key = ExpressionKey.STR_POSITION;

  /**
   * Defines the arguments (properties and child expressions) for StrPosition expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    substr: true,
    position: false,
    occurrence: false,
  } satisfies RequiredMap<StrPositionExprArgs>;

  declare args: StrPositionExprArgs;

  constructor (args: StrPositionExprArgs) {
    super(args);
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

export type SearchExprArgs = { jsonScope?: Expression;
  analyzer?: Expression;
  analyzerOptions?: Expression[];
  searchMode?: Expression; } & FuncExprArgs;

export class SearchExpr extends FuncExpr {
  key = ExpressionKey.SEARCH;

  /**
   * Defines the arguments (properties and child expressions) for Search expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    jsonScope: false,
    analyzer: false,
    analyzerOptions: false,
    searchMode: false,
  } satisfies RequiredMap<SearchExprArgs>;

  declare args: SearchExprArgs;

  constructor (args: SearchExprArgs) {
    super(args);
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

export type SearchIpExprArgs = FuncExprArgs;

export class SearchIpExpr extends FuncExpr {
  key = ExpressionKey.SEARCH_IP;

  static argTypes = {} satisfies RequiredMap<SearchIpExprArgs>;

  declare args: SearchIpExprArgs;

  constructor (args: SearchIpExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StrToDateExprArgs = { format?: string;
  safe?: boolean; } & FuncExprArgs;

export class StrToDateExpr extends FuncExpr {
  key = ExpressionKey.STR_TO_DATE;

  /**
   * Defines the arguments (properties and child expressions) for StrToDate expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    format: false,
    safe: false,
  } satisfies RequiredMap<StrToDateExprArgs>;

  declare args: StrToDateExprArgs;

  constructor (args: StrToDateExprArgs) {
    super(args);
  }

  get $format (): Expression | undefined {
    return this.args.format;
  }

  get $safe (): Expression | undefined {
    return this.args.safe;
  }

  static {
    this.register();
  }
}

export type StrToTimeExprArgs = { format: string;
  zone?: Expression;
  safe?: boolean;
  targetType?: DataTypeExpr; } & FuncExprArgs;

export class StrToTimeExpr extends FuncExpr {
  key = ExpressionKey.STR_TO_TIME;

  /**
   * Defines the arguments (properties and child expressions) for StrToTime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    format: true,
    zone: false,
    safe: false,
    targetType: false,
  } satisfies RequiredMap<StrToTimeExprArgs>;

  declare args: StrToTimeExprArgs;

  constructor (args: StrToTimeExprArgs) {
    super(args);
  }

  get $format (): Expression {
    return this.args.format;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  get $safe (): Expression | undefined {
    return this.args.safe;
  }

  get $targetType (): Expression | undefined {
    return this.args.targetType;
  }

  static {
    this.register();
  }
}

export type StrToUnixExprArgs = { format?: string } & FuncExprArgs;

export class StrToUnixExpr extends FuncExpr {
  key = ExpressionKey.STR_TO_UNIX;

  static argTypes = { format: false } satisfies RequiredMap<StrToUnixExprArgs>;

  declare args: StrToUnixExprArgs;

  constructor (args: StrToUnixExprArgs) {
    super(args);
  }

  get $format (): Expression | undefined {
    return this.args.format;
  }

  static {
    this.register();
  }
}

export type StrToMapExprArgs = { pairDelim?: Expression;
  keyValueDelim?: string;
  duplicateResolutionCallback?: Expression; } & FuncExprArgs;

export class StrToMapExpr extends FuncExpr {
  key = ExpressionKey.STR_TO_MAP;

  /**
   * Defines the arguments (properties and child expressions) for StrToMap expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    pairDelim: false,
    keyValueDelim: false,
    duplicateResolutionCallback: false,
  } satisfies RequiredMap<StrToMapExprArgs>;

  declare args: StrToMapExprArgs;

  constructor (args: StrToMapExprArgs) {
    super(args);
  }

  get $pairDelim (): Expression | undefined {
    return this.args.pairDelim;
  }

  get $keyValueDelim (): Expression | undefined {
    return this.args.keyValueDelim;
  }

  get $duplicateResolutionCallback (): Expression | undefined {
    return this.args.duplicateResolutionCallback;
  }

  static {
    this.register();
  }
}

export type NumberToStrExprArgs = { format: string;
  culture?: Expression; } & FuncExprArgs;

export class NumberToStrExpr extends FuncExpr {
  key = ExpressionKey.NUMBER_TO_STR;

  /**
   * Defines the arguments (properties and child expressions) for NumberToStr expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    format: true,
    culture: false,
  } satisfies RequiredMap<NumberToStrExprArgs>;

  declare args: NumberToStrExprArgs;

  constructor (args: NumberToStrExprArgs) {
    super(args);
  }

  get $format (): Expression {
    return this.args.format;
  }

  get $culture (): Expression | undefined {
    return this.args.culture;
  }

  static {
    this.register();
  }
}

export type FromBaseExprArgs = FuncExprArgs;

export class FromBaseExpr extends FuncExpr {
  key = ExpressionKey.FROM_BASE;

  static argTypes = {} satisfies RequiredMap<FromBaseExprArgs>;

  declare args: FromBaseExprArgs;

  constructor (args: FromBaseExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SpaceExprArgs = FuncExprArgs;

export class SpaceExpr extends FuncExpr {
  key = ExpressionKey.SPACE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SpaceExprArgs>;

  declare args: SpaceExprArgs;

  constructor (args: SpaceExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StructExprArgs = FuncExprArgs;

export class StructExpr extends FuncExpr {
  key = ExpressionKey.STRUCT;
    static isVarLenArgs = true;
  static argTypes = {} satisfies RequiredMap<StructExprArgs>;

  declare args: StructExprArgs;

  constructor (args: StructExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StructExtractExprArgs = FuncExprArgs;

export class StructExtractExpr extends FuncExpr {
  key = ExpressionKey.STRUCT_EXTRACT;

  static argTypes = {} satisfies RequiredMap<StructExtractExprArgs>;

  declare args: StructExtractExprArgs;

  constructor (args: StructExtractExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StuffExprArgs = { start: Expression;
  length: number | Expression; } & FuncExprArgs;

export class StuffExpr extends FuncExpr {
  key = ExpressionKey.STUFF;

  /**
   * Defines the arguments (properties and child expressions) for Stuff expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    start: true,
    length: true,
  } satisfies RequiredMap<StuffExprArgs>;

  declare args: StuffExprArgs;

  constructor (args: StuffExprArgs) {
    super(args);
  }

  get $start (): Expression {
    return this.args.start;
  }

  get $length (): Expression {
    return this.args.length;
  }

  static {
    this.register();
  }

  static sqlNames = ['STUFF', 'INSERT'];
}

export type SqrtExprArgs = FuncExprArgs;

export class SqrtExpr extends FuncExpr {
  key = ExpressionKey.SQRT;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SqrtExprArgs>;

  declare args: SqrtExprArgs;

  constructor (args: SqrtExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeExprArgs = { zone?: Expression } & FuncExprArgs;

export class TimeExpr extends FuncExpr {
  key = ExpressionKey.TIME;

  static argTypes = { zone: false } satisfies RequiredMap<TimeExprArgs>;

  declare args: TimeExprArgs;

  constructor (args: TimeExprArgs) {
    super(args);
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  static {
    this.register();
  }
}

export type TimeToStrExprArgs = { format: string;
  culture?: Expression;
  zone?: Expression; } & FuncExprArgs;

export class TimeToStrExpr extends FuncExpr {
  key = ExpressionKey.TIME_TO_STR;

  /**
   * Defines the arguments (properties and child expressions) for TimeToStr expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    format: true,
    culture: false,
    zone: false,
  } satisfies RequiredMap<TimeToStrExprArgs>;

  declare args: TimeToStrExprArgs;

  constructor (args: TimeToStrExprArgs) {
    super(args);
  }

  get $format (): Expression {
    return this.args.format;
  }

  get $culture (): Expression | undefined {
    return this.args.culture;
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  static {
    this.register();
  }
}

export type TimeToTimeStrExprArgs = FuncExprArgs;

export class TimeToTimeStrExpr extends FuncExpr {
  key = ExpressionKey.TIME_TO_TIME_STR;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TimeToTimeStrExprArgs>;

  declare args: TimeToTimeStrExprArgs;

  constructor (args: TimeToTimeStrExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeToUnixExprArgs = FuncExprArgs;

export class TimeToUnixExpr extends FuncExpr {
  key = ExpressionKey.TIME_TO_UNIX;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TimeToUnixExprArgs>;

  declare args: TimeToUnixExprArgs;

  constructor (args: TimeToUnixExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeStrToDateExprArgs = FuncExprArgs;

export class TimeStrToDateExpr extends FuncExpr {
  key = ExpressionKey.TIME_STR_TO_DATE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TimeStrToDateExprArgs>;

  declare args: TimeStrToDateExprArgs;

  constructor (args: TimeStrToDateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeStrToTimeExprArgs = { zone?: Expression } & FuncExprArgs;

export class TimeStrToTimeExpr extends FuncExpr {
  key = ExpressionKey.TIME_STR_TO_TIME;

  static argTypes = { zone: false } satisfies RequiredMap<TimeStrToTimeExprArgs>;

  declare args: TimeStrToTimeExprArgs;

  constructor (args: TimeStrToTimeExprArgs) {
    super(args);
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  static {
    this.register();
  }
}

export type TimeStrToUnixExprArgs = FuncExprArgs;

export class TimeStrToUnixExpr extends FuncExpr {
  key = ExpressionKey.TIME_STR_TO_UNIX;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TimeStrToUnixExprArgs>;

  declare args: TimeStrToUnixExprArgs;

  constructor (args: TimeStrToUnixExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TrimExprArgs = { position?: Expression;
  collation?: Expression; } & FuncExprArgs;

export class TrimExpr extends FuncExpr {
  key = ExpressionKey.TRIM;

  /**
   * Defines the arguments (properties and child expressions) for Trim expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    position: false,
    collation: false,
  } satisfies RequiredMap<TrimExprArgs>;

  declare args: TrimExprArgs;

  constructor (args: TrimExprArgs) {
    super(args);
  }

  get $position (): Expression | undefined {
    return this.args.position;
  }

  get $collation (): Expression | undefined {
    return this.args.collation;
  }

  static {
    this.register();
  }
}

export type TsOrDsAddExprArgs = { unit?: Expression;
  returnType?: DataTypeExpr; } & FuncExprArgs;

export class TsOrDsAddExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TS_OR_DS_ADD;

  /**
   * Defines the arguments (properties and child expressions) for TsOrDsAdd expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    unit: false,
    returnType: false,
  } satisfies RequiredMap<TsOrDsAddExprArgs>;

  declare args: TsOrDsAddExprArgs;

  constructor (args: TsOrDsAddExprArgs) {
    super(args);
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  get $returnType (): Expression | undefined {
    return this.args.returnType;
  }

  get returnType (): DataTypeExpr {
    const returnTypeArg = this.args.returnType;
    if (returnTypeArg instanceof DataTypeExpr) {
      return returnTypeArg;
    }
    return DataTypeExpr.build(DataTypeExprKind.DATE);
  }

  static {
    this.register();
  }
}

export type TsOrDsDiffExprArgs = { unit?: Expression } & FuncExprArgs;

export class TsOrDsDiffExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  key = ExpressionKey.TS_OR_DS_DIFF;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    unit: false,
  } satisfies RequiredMap<TsOrDsDiffExprArgs>;

  declare args: TsOrDsDiffExprArgs;

  constructor (args: TsOrDsDiffExprArgs) {
    super(args);
  }

  get $unit (): Expression | undefined {
    return this.args.unit;
  }

  static {
    this.register();
  }
}

export type TsOrDsToDateStrExprArgs = FuncExprArgs;

export class TsOrDsToDateStrExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_TO_DATE_STR;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TsOrDsToDateStrExprArgs>;

  declare args: TsOrDsToDateStrExprArgs;

  constructor (args: TsOrDsToDateStrExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TsOrDsToDateExprArgs = { format?: string;
  safe?: boolean; } & FuncExprArgs;

export class TsOrDsToDateExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_TO_DATE;

  /**
   * Defines the arguments (properties and child expressions) for TsOrDsToDate expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    format: false,
    safe: false,
  } satisfies RequiredMap<TsOrDsToDateExprArgs>;

  declare args: TsOrDsToDateExprArgs;

  constructor (args: TsOrDsToDateExprArgs) {
    super(args);
  }

  get $format (): Expression | undefined {
    return this.args.format;
  }

  get $safe (): Expression | undefined {
    return this.args.safe;
  }

  static {
    this.register();
  }
}

export type TsOrDsToDatetimeExprArgs = FuncExprArgs;

export class TsOrDsToDatetimeExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_TO_DATETIME;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TsOrDsToDatetimeExprArgs>;

  declare args: TsOrDsToDatetimeExprArgs;

  constructor (args: TsOrDsToDatetimeExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TsOrDsToTimeExprArgs = { format?: string;
  safe?: boolean; } & FuncExprArgs;

export class TsOrDsToTimeExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_TO_TIME;

  /**
   * Defines the arguments (properties and child expressions) for TsOrDsToTime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    format: false,
    safe: false,
  } satisfies RequiredMap<TsOrDsToTimeExprArgs>;

  declare args: TsOrDsToTimeExprArgs;

  constructor (args: TsOrDsToTimeExprArgs) {
    super(args);
  }

  get $format (): Expression | undefined {
    return this.args.format;
  }

  get $safe (): Expression | undefined {
    return this.args.safe;
  }

  static {
    this.register();
  }
}

export type TsOrDsToTimestampExprArgs = FuncExprArgs;

export class TsOrDsToTimestampExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_TO_TIMESTAMP;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TsOrDsToTimestampExprArgs>;

  declare args: TsOrDsToTimestampExprArgs;

  constructor (args: TsOrDsToTimestampExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TsOrDiToDiExprArgs = FuncExprArgs;

export class TsOrDiToDiExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DI_TO_DI;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TsOrDiToDiExprArgs>;

  declare args: TsOrDiToDiExprArgs;

  constructor (args: TsOrDiToDiExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnhexExprArgs = FuncExprArgs;

export class UnhexExpr extends FuncExpr {
  key = ExpressionKey.UNHEX;

  static argTypes = {} satisfies RequiredMap<UnhexExprArgs>;

  declare args: UnhexExprArgs;

  constructor (args: UnhexExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnicodeExprArgs = FuncExprArgs;

export class UnicodeExpr extends FuncExpr {
  key = ExpressionKey.UNICODE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<UnicodeExprArgs>;

  declare args: UnicodeExprArgs;

  constructor (args: UnicodeExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UniformExprArgs = { gen?: Expression;
  seed?: Expression; } & FuncExprArgs;

export class UniformExpr extends FuncExpr {
  key = ExpressionKey.UNIFORM;

  /**
   * Defines the arguments (properties and child expressions) for Uniform expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    gen: false,
    seed: false,
  } satisfies RequiredMap<UniformExprArgs>;

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

  static {
    this.register();
  }
}

export type UnixDateExprArgs = FuncExprArgs;

export class UnixDateExpr extends FuncExpr {
  key = ExpressionKey.UNIX_DATE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<UnixDateExprArgs>;

  declare args: UnixDateExprArgs;

  constructor (args: UnixDateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnixToStrExprArgs = { format?: string } & FuncExprArgs;

export class UnixToStrExpr extends FuncExpr {
  key = ExpressionKey.UNIX_TO_STR;

  static argTypes = { format: false } satisfies RequiredMap<UnixToStrExprArgs>;

  declare args: UnixToStrExprArgs;

  constructor (args: UnixToStrExprArgs) {
    super(args);
  }

  get $format (): Expression | undefined {
    return this.args.format;
  }

  static {
    this.register();
  }
}

export type UnixToTimeExprArgs = { scale?: number | Expression;
  zone?: Expression;
  hours?: Expression[];
  minutes?: Expression[];
  format?: string;
  targetType?: DataTypeExpr; } & FuncExprArgs;

export class UnixToTimeExpr extends FuncExpr {
  key = ExpressionKey.UNIX_TO_TIME;

  /**
   * Defines the arguments (properties and child expressions) for UnixToTime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    scale: false,
    zone: false,
    hours: false,
    minutes: false,
    format: false,
    targetType: false,
  } satisfies RequiredMap<UnixToTimeExprArgs>;

  declare args: UnixToTimeExprArgs;

  constructor (args: UnixToTimeExprArgs) {
    super(args);
  }

  get $scale (): Expression | undefined {
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

  get $format (): Expression | undefined {
    return this.args.format;
  }

  get $targetType (): Expression | undefined {
    return this.args.targetType;
  }

  static {
    this.register();
  }
}

export type UnixToTimeStrExprArgs = FuncExprArgs;

export class UnixToTimeStrExpr extends FuncExpr {
  key = ExpressionKey.UNIX_TO_TIME_STR;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<UnixToTimeStrExprArgs>;

  declare args: UnixToTimeStrExprArgs;

  constructor (args: UnixToTimeStrExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnixSecondsExprArgs = FuncExprArgs;

export class UnixSecondsExpr extends FuncExpr {
  key = ExpressionKey.UNIX_SECONDS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<UnixSecondsExprArgs>;

  declare args: UnixSecondsExprArgs;

  constructor (args: UnixSecondsExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnixMicrosExprArgs = FuncExprArgs;

export class UnixMicrosExpr extends FuncExpr {
  key = ExpressionKey.UNIX_MICROS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<UnixMicrosExprArgs>;

  declare args: UnixMicrosExprArgs;

  constructor (args: UnixMicrosExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnixMillisExprArgs = FuncExprArgs;

export class UnixMillisExpr extends FuncExpr {
  key = ExpressionKey.UNIX_MILLIS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<UnixMillisExprArgs>;

  declare args: UnixMillisExprArgs;

  constructor (args: UnixMillisExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UuidExprArgs = { name?: unknown;
  isString?: unknown; } & FuncExprArgs;

export class UuidExpr extends FuncExpr {
  key = ExpressionKey.UUID;

  /**
   * Defines the arguments (properties and child expressions) for Uuid expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    name: false,
    isString: false,
  } satisfies RequiredMap<UuidExprArgs>;

  declare args: UuidExprArgs;

  constructor (args: UuidExprArgs) {
    super(args);
  }

  get $name (): Expression | undefined {
    return this.args.name;
  }

  get $isString (): Expression | undefined {
    return this.args.isString;
  }

  static {
    this.register();
  }

  static sqlNames = [
    'UUID',
    'GEN_RANDOM_UUID',
    'GENERATE_UUID',
    'UUID_STRING',
  ];
}

export type TimestampFromPartsExprArgs = { zone?: Expression;
  milli?: Expression; } & FuncExprArgs;

export class TimestampFromPartsExpr extends FuncExpr {
  key = ExpressionKey.TIMESTAMP_FROM_PARTS;

  /**
   * Defines the arguments (properties and child expressions) for TimestampFromParts expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    zone: false,
    milli: false,
  } satisfies RequiredMap<TimestampFromPartsExprArgs>;

  declare args: TimestampFromPartsExprArgs;

  constructor (args: TimestampFromPartsExprArgs) {
    super(args);
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  get $milli (): Expression | undefined {
    return this.args.milli;
  }

  static {
    this.register();
  }

  static sqlNames = ['TIMESTAMP_FROM_PARTS', 'TIMESTAMPFROMPARTS'];
}

export type TimestampLtzFromPartsExprArgs = { zone?: Expression } & FuncExprArgs;

export class TimestampLtzFromPartsExpr extends FuncExpr {
  key = ExpressionKey.TIMESTAMP_LTZ_FROM_PARTS;

  static sqlNames = ['TIMESTAMP_LTZ_FROM_PARTS', 'TIMESTAMPLTZFROMPARTS'];

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TimestampLtzFromPartsExprArgs>;

  declare args: TimestampLtzFromPartsExprArgs;

  static {
    this.register();
  }

  constructor (args: TimestampLtzFromPartsExprArgs) {
    super(args);
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }
}

export type TimestampTzFromPartsExprArgs = { zone?: Expression } & FuncExprArgs;

export class TimestampTzFromPartsExpr extends FuncExpr {
  key = ExpressionKey.TIMESTAMP_TZ_FROM_PARTS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<TimestampTzFromPartsExprArgs>;

  declare args: TimestampTzFromPartsExprArgs;

  constructor (args: TimestampTzFromPartsExprArgs) {
    super(args);
  }

  get $zone (): Expression | undefined {
    return this.args.zone;
  }

  static {
    this.register();
  }

  static sqlNames = ['TIMESTAMP_TZ_FROM_PARTS', 'TIMESTAMPTZFROMPARTS'];
}

export type UpperExprArgs = FuncExprArgs;

export class UpperExpr extends FuncExpr {
  key = ExpressionKey.UPPER;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<UpperExprArgs>;

  declare args: UpperExprArgs;

  constructor (args: UpperExprArgs) {
    super(args);
  }

  static sqlNames = ['UPPER', 'UCASE'];

  static {
    this.register();
  }
}

export type CorrExprArgs = { nullOnZeroVariance?: Expression } & BinaryExprArgs;

export class CorrExpr extends multiInherit(BinaryExpr, AggFuncExpr) {
  key = ExpressionKey.CORR;

  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
    nullOnZeroVariance: false,
  } satisfies RequiredMap<CorrExprArgs>;

  declare args: CorrExprArgs;

  constructor (args: CorrExprArgs) {
    super(args);
  }

  get $nullOnZeroVariance (): Expression | undefined {
    return this.args.nullOnZeroVariance;
  }
}

export type WidthBucketExprArgs = { minValue?: string;
  maxValue?: string;
  numBuckets?: Expression[];
  threshold?: Expression; } & FuncExprArgs;

export class WidthBucketExpr extends FuncExpr {
  key = ExpressionKey.WIDTH_BUCKET;

  /**
   * Defines the arguments (properties and child expressions) for WidthBucket expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    minValue: false,
    maxValue: false,
    numBuckets: false,
    threshold: false,
  } satisfies RequiredMap<WidthBucketExprArgs>;

  declare args: WidthBucketExprArgs;

  constructor (args: WidthBucketExprArgs) {
    super(args);
  }

  get $minValue (): Expression | undefined {
    return this.args.minValue;
  }

  get $maxValue (): Expression | undefined {
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

export type WeekExprArgs = { mode?: Expression } & FuncExprArgs;

export class WeekExpr extends FuncExpr {
  key = ExpressionKey.WEEK;

  static argTypes = { mode: false } satisfies RequiredMap<WeekExprArgs>;

  declare args: WeekExprArgs;

  constructor (args: WeekExprArgs) {
    super(args);
  }

  get $mode (): Expression | undefined {
    return this.args.mode;
  }

  static {
    this.register();
  }
}

export type NextDayExprArgs = FuncExprArgs;

export class NextDayExpr extends FuncExpr {
  key = ExpressionKey.NEXT_DAY;

  static argTypes = {} satisfies RequiredMap<NextDayExprArgs>;

  declare args: NextDayExprArgs;

  constructor (args: NextDayExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type XMLElementExprArgs = { evalname?: string } & FuncExprArgs;

export class XMLElementExpr extends FuncExpr {
  key = ExpressionKey.XML_ELEMENT;

  static argTypes = { evalname: false } satisfies RequiredMap<XMLElementExprArgs>;

  declare args: XMLElementExprArgs;

  constructor (args: XMLElementExprArgs) {
    super(args);
  }

  get $evalname (): Expression | undefined {
    return this.args.evalname;
  }

  static {
    this.register();
  }

  static sqlNames = ['XMLELEMENT'];
}

export type XMLGetExprArgs = { instance?: Expression } & FuncExprArgs;

export class XMLGetExpr extends FuncExpr {
  key = ExpressionKey.XML_GET;

  static argTypes = { instance: false } satisfies RequiredMap<XMLGetExprArgs>;

  declare args: XMLGetExprArgs;

  constructor (args: XMLGetExprArgs) {
    super(args);
  }

  get $instance (): Expression | undefined {
    return this.args.instance;
  }

  static {
    this.register();
  }

  static sqlNames = ['XMLGET'];
}

export type XMLTableExprArgs = { namespaces?: Expression[];
  passing?: Expression;
  columns?: Expression[];
  byRef?: Expression; } & FuncExprArgs;

export class XMLTableExpr extends FuncExpr {
  key = ExpressionKey.XML_TABLE;

  /**
   * Defines the arguments (properties and child expressions) for XMLTable expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    namespaces: false,
    passing: false,
    columns: false,
    byRef: false,
  } satisfies RequiredMap<XMLTableExprArgs>;

  declare args: XMLTableExprArgs;

  constructor (args: XMLTableExprArgs) {
    super(args);
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

export type YearExprArgs = FuncExprArgs;

export class YearExpr extends FuncExpr {
  key = ExpressionKey.YEAR;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<YearExprArgs>;

  declare args: YearExprArgs;

  constructor (args: YearExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ZipfExprArgs = { elementcount: Expression;
  gen: Expression; } & FuncExprArgs;

export class ZipfExpr extends FuncExpr {
  key = ExpressionKey.ZIPF;

  /**
   * Defines the arguments (properties and child expressions) for Zipf expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    elementcount: true,
    gen: true,
  } satisfies RequiredMap<ZipfExprArgs>;

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

  static {
    this.register();
  }
}

export type NextValueForExprArgs = { order?: Expression } & FuncExprArgs;

export class NextValueForExpr extends FuncExpr {
  key = ExpressionKey.NEXT_VALUE_FOR;

  static argTypes = { order: false } satisfies RequiredMap<NextValueForExprArgs>;

  declare args: NextValueForExprArgs;

  constructor (args: NextValueForExprArgs) {
    super(args);
  }

  get $order (): Expression | undefined {
    return this.args.order;
  }

  static {
    this.register();
  }
}

export type AllExprArgs = SubqueryPredicateExprArgs;

export class AllExpr extends SubqueryPredicateExpr {
  key = ExpressionKey.ALL;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AllExprArgs>;

  declare args: AllExprArgs;

  constructor (args: AllExprArgs) {
    super(args);
  }
}

export type AnyExprArgs = SubqueryPredicateExprArgs;

export class AnyExpr extends SubqueryPredicateExpr {
  key = ExpressionKey.ANY;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AnyExprArgs>;

  declare args: AnyExprArgs;

  constructor (args: AnyExprArgs) {
    super(args);
  }
}

export type BitwiseAndAggExprArgs = AggFuncExprArgs;

export class BitwiseAndAggExpr extends AggFuncExpr {
  key = ExpressionKey.BITWISE_AND_AGG;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<BitwiseAndAggExprArgs>;

  declare args: BitwiseAndAggExprArgs;

  constructor (args: BitwiseAndAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitwiseOrAggExprArgs = AggFuncExprArgs;

export class BitwiseOrAggExpr extends AggFuncExpr {
  key = ExpressionKey.BITWISE_OR_AGG;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<BitwiseOrAggExprArgs>;

  declare args: BitwiseOrAggExprArgs;

  constructor (args: BitwiseOrAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitwiseXorAggExprArgs = AggFuncExprArgs;

export class BitwiseXorAggExpr extends AggFuncExpr {
  key = ExpressionKey.BITWISE_XOR_AGG;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<BitwiseXorAggExprArgs>;

  declare args: BitwiseXorAggExprArgs;

  constructor (args: BitwiseXorAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BoolxorAggExprArgs = AggFuncExprArgs;

export class BoolxorAggExpr extends AggFuncExpr {
  key = ExpressionKey.BOOLXOR_AGG;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<BoolxorAggExprArgs>;

  declare args: BoolxorAggExprArgs;

  constructor (args: BoolxorAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitmapConstructAggExprArgs = AggFuncExprArgs;

export class BitmapConstructAggExpr extends AggFuncExpr {
  key = ExpressionKey.BITMAP_CONSTRUCT_AGG;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<BitmapConstructAggExprArgs>;

  declare args: BitmapConstructAggExprArgs;

  constructor (args: BitmapConstructAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitmapOrAggExprArgs = AggFuncExprArgs;

export class BitmapOrAggExpr extends AggFuncExpr {
  key = ExpressionKey.BITMAP_OR_AGG;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<BitmapOrAggExprArgs>;

  declare args: BitmapOrAggExprArgs;

  constructor (args: BitmapOrAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ParameterizedAggExprArgs = {
  this: Expression;
  expressions: Expression[];
  params: Expression[];
} & AggFuncExprArgs;

export class ParameterizedAggExpr extends AggFuncExpr {
  key = ExpressionKey.PARAMETERIZED_AGG;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
    params: true,
  } satisfies RequiredMap<ParameterizedAggExprArgs>;

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

export type ArgMaxExprArgs = {
  this: Expression;
  expression: Expression;
  count?: Expression;
} & AggFuncExprArgs;

export class ArgMaxExpr extends AggFuncExpr {
  key = ExpressionKey.ARG_MAX;

  static _sqlNames = [
    'ARG_MAX',
    'ARGMAX',
    'MAX_BY',
  ];

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    count: false,
  } satisfies RequiredMap<ArgMaxExprArgs>;

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

  static sqlNames = [
    'ARG_MAX',
    'ARGMAX',
    'MAX_BY',
  ];
}

export type ArgMinExprArgs = {
  this: Expression;
  expression: Expression;
  count?: Expression;
} & AggFuncExprArgs;

export class ArgMinExpr extends AggFuncExpr {
  key = ExpressionKey.ARG_MIN;

  static _sqlNames = [
    'ARG_MIN',
    'ARGMIN',
    'MIN_BY',
  ];

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    count: false,
  } satisfies RequiredMap<ArgMinExprArgs>;

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

  static sqlNames = [
    'ARG_MIN',
    'ARGMIN',
    'MIN_BY',
  ];
}

export type ApproxTopKExprArgs = {
  this: Expression;
  expression?: Expression;
  counters?: Expression;
} & AggFuncExprArgs;

export class ApproxTopKExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_TOP_K;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
    counters: false,
  } satisfies RequiredMap<ApproxTopKExprArgs>;

  declare args: ApproxTopKExprArgs;

  constructor (args: ApproxTopKExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
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
export type ApproxTopKAccumulateExprArgs = {
  this: Expression;
  expression?: Expression;
} & AggFuncExprArgs;

export class ApproxTopKAccumulateExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_TOP_K_ACCUMULATE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<ApproxTopKAccumulateExprArgs>;

  declare args: ApproxTopKAccumulateExprArgs;

  constructor (args: ApproxTopKAccumulateExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/approx_top_k_combine
 */
export type ApproxTopKCombineExprArgs = {
  this: Expression;
  expression?: Expression;
} & AggFuncExprArgs;

export class ApproxTopKCombineExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_TOP_K_COMBINE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<ApproxTopKCombineExprArgs>;

  declare args: ApproxTopKCombineExprArgs;

  constructor (args: ApproxTopKCombineExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type ApproxTopSumExprArgs = {
  this: Expression;
  expression: Expression;
  count: Expression;
} & AggFuncExprArgs;

export class ApproxTopSumExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_TOP_SUM;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
    count: true,
  } satisfies RequiredMap<ApproxTopSumExprArgs>;

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

export type ApproxQuantilesExprArgs = {
  this: Expression;
  expression?: Expression;
} & AggFuncExprArgs;

export class ApproxQuantilesExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_QUANTILES;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<ApproxQuantilesExprArgs>;

  declare args: ApproxQuantilesExprArgs;

  constructor (args: ApproxQuantilesExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/approx_percentile_combine
 */
export type ApproxPercentileCombineExprArgs = AggFuncExprArgs;

export class ApproxPercentileCombineExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_PERCENTILE_COMBINE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ApproxPercentileCombineExprArgs>;

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
export type MinhashExprArgs = {
  this: Expression;
  expressions: Expression[];
} & AggFuncExprArgs;

export class MinhashExpr extends AggFuncExpr {
  key = ExpressionKey.MINHASH;

  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
  } satisfies RequiredMap<MinhashExprArgs>;

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
export type MinhashCombineExprArgs = AggFuncExprArgs;

export class MinhashCombineExpr extends AggFuncExpr {
  key = ExpressionKey.MINHASH_COMBINE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<MinhashCombineExprArgs>;

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
export type ApproximateSimilarityExprArgs = AggFuncExprArgs;

export class ApproximateSimilarityExpr extends AggFuncExpr {
  key = ExpressionKey.APPROXIMATE_SIMILARITY;

  static _sqlNames = ['APPROXIMATE_SIMILARITY', 'APPROXIMATE_JACCARD_INDEX'];

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ApproximateSimilarityExprArgs>;

  declare args: ApproximateSimilarityExprArgs;

  constructor (args: ApproximateSimilarityExprArgs) {
    super(args);
  }

  static {
    this.register();
  }

  static sqlNames = ['APPROXIMATE_SIMILARITY', 'APPROXIMATE_JACCARD_INDEX'];
}

export type GroupingExprArgs = {
  expressions: Expression[];
} & AggFuncExprArgs;

export class GroupingExpr extends AggFuncExpr {
  key = ExpressionKey.GROUPING;

  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    expressions: true,
  } satisfies RequiredMap<GroupingExprArgs>;

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

export type GroupingIdExprArgs = {
  expressions?: Expression[];
} & AggFuncExprArgs;

export class GroupingIdExpr extends AggFuncExpr {
  key = ExpressionKey.GROUPING_ID;

  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    expressions: false,
  } satisfies RequiredMap<GroupingIdExprArgs>;

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

export type AnonymousAggFuncExprArgs = {
  this: Expression;
  expressions?: Expression[];
} & AggFuncExprArgs;

export class AnonymousAggFuncExpr extends AggFuncExpr {
  key = ExpressionKey.ANONYMOUS_AGG_FUNC;

  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
  } satisfies RequiredMap<AnonymousAggFuncExprArgs>;

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
export type HashAggExprArgs = {
  this: Expression;
  expressions?: Expression[];
} & AggFuncExprArgs;

export class HashAggExpr extends AggFuncExpr {
  key = ExpressionKey.HASH_AGG;

  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
  } satisfies RequiredMap<HashAggExprArgs>;

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
export type HllExprArgs = {
  this: Expression;
  expressions?: Expression[];
} & AggFuncExprArgs;

export class HllExpr extends AggFuncExpr {
  key = ExpressionKey.HLL;

  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
  } satisfies RequiredMap<HllExprArgs>;

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

export type ApproxDistinctExprArgs = {
  this: Expression;
  accuracy?: Expression;
} & AggFuncExprArgs;

export class ApproxDistinctExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_DISTINCT;

  static _sqlNames = ['APPROX_DISTINCT', 'APPROX_COUNT_DISTINCT'];

  static argTypes = {
    ...super.argTypes,
    this: true,
    accuracy: false,
  } satisfies RequiredMap<ApproxDistinctExprArgs>;

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

  static sqlNames = ['APPROX_DISTINCT', 'APPROX_COUNT_DISTINCT'];
}

/**
 * Postgres' GENERATE_SERIES function returns a row set, i.e. it implicitly explodes when it's
 * used in a projection, so this expression is a helper that facilitates transpilation to other
 * dialects. For example, we'd generate UNNEST(GENERATE_SERIES(...)) in DuckDB
 */
export type ExplodingGenerateSeriesExprArgs = GenerateSeriesExprArgs;

export class ExplodingGenerateSeriesExpr extends GenerateSeriesExpr {
  key = ExpressionKey.EXPLODING_GENERATE_SERIES;

  static {
    this.register();
  }
}

export type ArrayAggExprArgs = {
  this: Expression;
  nullsExcluded?: Expression;
} & AggFuncExprArgs;

export class ArrayAggExpr extends AggFuncExpr {
  key = ExpressionKey.ARRAY_AGG;

  static argTypes = {
    ...super.argTypes,
    this: true,
    nullsExcluded: false,
  } satisfies RequiredMap<ArrayAggExprArgs>;

  declare args: ArrayAggExprArgs;

  constructor (args: ArrayAggExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $nullsExcluded (): Expression | undefined {
    return this.args.nullsExcluded;
  }

  static {
    this.register();
  }
}

export type ArrayUniqueAggExprArgs = AggFuncExprArgs;

export class ArrayUniqueAggExpr extends AggFuncExpr {
  key = ExpressionKey.ARRAY_UNIQUE_AGG;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ArrayUniqueAggExprArgs>;

  declare args: ArrayUniqueAggExprArgs;

  constructor (args: ArrayUniqueAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AIAggExprArgs = {
  this: Expression;
  expression: Expression;
} & AggFuncExprArgs;

export class AIAggExpr extends AggFuncExpr {
  key = ExpressionKey.AI_AGG;

  static _sqlNames = ['AI_AGG'];

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<AIAggExprArgs>;

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

  static sqlNames = ['AI_AGG'];
}

export type AISummarizeAggExprArgs = AggFuncExprArgs;

export class AISummarizeAggExpr extends AggFuncExpr {
  key = ExpressionKey.AI_SUMMARIZE_AGG;

  static _sqlNames = ['AI_SUMMARIZE_AGG'];

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AISummarizeAggExprArgs>;

  declare args: AISummarizeAggExprArgs;

  constructor (args: AISummarizeAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }

  static sqlNames = ['AI_SUMMARIZE_AGG'];
}

export type ArrayConcatAggExprArgs = AggFuncExprArgs;

export class ArrayConcatAggExpr extends AggFuncExpr {
  key = ExpressionKey.ARRAY_CONCAT_AGG;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ArrayConcatAggExprArgs>;

  declare args: ArrayConcatAggExprArgs;

  constructor (args: ArrayConcatAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayUnionAggExprArgs = AggFuncExprArgs;

export class ArrayUnionAggExpr extends AggFuncExpr {
  key = ExpressionKey.ARRAY_UNION_AGG;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ArrayUnionAggExprArgs>;

  declare args: ArrayUnionAggExprArgs;

  constructor (args: ArrayUnionAggExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AvgExprArgs = AggFuncExprArgs;

export class AvgExpr extends AggFuncExpr {
  key = ExpressionKey.AVG;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AvgExprArgs>;

  declare args: AvgExprArgs;

  constructor (args: AvgExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AnyValueExprArgs = AggFuncExprArgs;

export class AnyValueExpr extends AggFuncExpr {
  key = ExpressionKey.ANY_VALUE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<AnyValueExprArgs>;

  declare args: AnyValueExprArgs;

  constructor (args: AnyValueExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LagExprArgs = {
  this: Expression;
  offset?: Expression;
  default?: Expression;
} & AggFuncExprArgs;

export class LagExpr extends AggFuncExpr {
  key = ExpressionKey.LAG;

  static argTypes = {
    ...super.argTypes,
    this: true,
    offset: false,
    default: false,
  } satisfies RequiredMap<LagExprArgs>;

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

export type LeadExprArgs = {
  this: Expression;
  offset?: Expression;
  default?: Expression;
} & AggFuncExprArgs;

export class LeadExpr extends AggFuncExpr {
  key = ExpressionKey.LEAD;

  static argTypes = {
    ...super.argTypes,
    this: true,
    offset: false,
    default: false,
  } satisfies RequiredMap<LeadExprArgs>;

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

export type FirstExprArgs = {
  this: Expression;
  expression?: Expression;
} & AggFuncExprArgs;

export class FirstExpr extends AggFuncExpr {
  key = ExpressionKey.FIRST;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<FirstExprArgs>;

  declare args: FirstExprArgs;

  constructor (args: FirstExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type LastExprArgs = {
  this: Expression;
  expression?: Expression;
} & AggFuncExprArgs;

export class LastExpr extends AggFuncExpr {
  key = ExpressionKey.LAST;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: false,
  } satisfies RequiredMap<LastExprArgs>;

  declare args: LastExprArgs;

  constructor (args: LastExprArgs) {
    super(args);
  }

  get $this (): Expression {
    return this.args.this;
  }

  get $expression (): Expression | undefined {
    return this.args.expression;
  }

  static {
    this.register();
  }
}

export type FirstValueExprArgs = AggFuncExprArgs;

export class FirstValueExpr extends AggFuncExpr {
  key = ExpressionKey.FIRST_VALUE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<FirstValueExprArgs>;

  declare args: FirstValueExprArgs;

  constructor (args: FirstValueExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LastValueExprArgs = AggFuncExprArgs;

export class LastValueExpr extends AggFuncExpr {
  key = ExpressionKey.LAST_VALUE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<LastValueExprArgs>;

  declare args: LastValueExprArgs;

  constructor (args: LastValueExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type NthValueExprArgs = {
  this: Expression;
  offset: Expression;
  fromFirst?: Expression;
} & AggFuncExprArgs;

export class NthValueExpr extends AggFuncExpr {
  key = ExpressionKey.NTH_VALUE;

  static argTypes = {
    ...super.argTypes,
    this: true,
    offset: true,
    fromFirst: false,
  } satisfies RequiredMap<NthValueExprArgs>;

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

export type ObjectAggExprArgs = {
  this: Expression;
  expression: Expression;
} & AggFuncExprArgs;

export class ObjectAggExpr extends AggFuncExpr {
  key = ExpressionKey.OBJECT_AGG;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<ObjectAggExprArgs>;

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

export type TryCastExprArgs = {
  requiresString?: Expression;
} & CastExprArgs;

export class TryCastExpr extends CastExpr {
  key = ExpressionKey.TRY_CAST;

  static argTypes = {
    ...super.argTypes,
    requiresString: false,
  } satisfies RequiredMap<TryCastExprArgs>;

  declare args: TryCastExprArgs;

  constructor (args: TryCastExprArgs) {
    super(args);
  }

  get $requiresString (): Expression | undefined {
    return this.args.requiresString;
  }
}

export type JSONCastExprArgs = CastExprArgs;

export class JSONCastExpr extends CastExpr {
  key = ExpressionKey.JSON_CAST;

  declare args: JSONCastExprArgs;

  constructor (args: JSONCastExprArgs) {
    super(args);
  }
}

export type ConcatWsExprArgs = ConcatExprArgs;

export class ConcatWsExpr extends ConcatExpr {
  key = ExpressionKey.CONCAT_WS;

  static sqlNames = ['CONCAT_WS'];

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ConcatWsExprArgs>;

  declare args: ConcatWsExprArgs;

  constructor (args: ConcatWsExprArgs) {
    super(args);
  }
}

export type CountExprArgs = {
  this?: Expression;
  expressions?: Expression[];
  bigInt?: Expression;
} & AggFuncExprArgs;

export class CountExpr extends AggFuncExpr {
  key = ExpressionKey.COUNT;

  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    this: false,
    expressions: false,
    bigInt: false,
  } satisfies RequiredMap<CountExprArgs>;

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

  get $bigInt (): Expression | undefined {
    return this.args.bigInt;
  }

  static {
    this.register();
  }
}

export type CountIfExprArgs = AggFuncExprArgs;

export class CountIfExpr extends AggFuncExpr {
  key = ExpressionKey.COUNT_IF;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<CountIfExprArgs>;

  declare args: CountIfExprArgs;

  constructor (args: CountIfExprArgs) {
    super(args);
  }

  static sqlNames = ['COUNT_IF', 'COUNTIF'];

  static {
    this.register();
  }
}

export type DenseRankExprArgs = {
  expressions?: Expression[];
} & AggFuncExprArgs;

export class DenseRankExpr extends AggFuncExpr {
  key = ExpressionKey.DENSE_RANK;
  static isVarLenArgs = true;

  static argTypes = {
    ...super.argTypes,
    expressions: false,
  } satisfies RequiredMap<DenseRankExprArgs>;

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

export type ExplodeOuterExprArgs = BaseExpressionArgs;
export class ExplodeOuterExpr extends ExplodeExpr {
  key = ExpressionKey.EXPLODE_OUTER;
  static argTypes = {} satisfies RequiredMap<ExplodeOuterExprArgs>;

  declare args: ExplodeOuterExprArgs;
  constructor (args: ExplodeOuterExprArgs) {
    super(args);
  }
}

export type PosexplodeExprArgs = BaseExpressionArgs;
export class PosexplodeExpr extends ExplodeExpr {
  key = ExpressionKey.POSEXPLODE;
  static argTypes = {} satisfies RequiredMap<PosexplodeExprArgs>;

  declare args: PosexplodeExprArgs;
  constructor (args: PosexplodeExprArgs) {
    super(args);
  }
}

export type GroupConcatExprArgs = {
  this: Expression;
  separator?: Expression;
  onOverflow?: Expression;
} & AggFuncExprArgs;

export class GroupConcatExpr extends AggFuncExpr {
  key = ExpressionKey.GROUP_CONCAT;

  /**
   * Defines the arguments (properties and child expressions) for GroupConcat expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    this: true,
    separator: false,
    onOverflow: false,
  } satisfies RequiredMap<GroupConcatExprArgs>;

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

export type LowerHexExprArgs = BaseExpressionArgs;
export class LowerHexExpr extends HexExpr {
  key = ExpressionKey.LOWER_HEX;
  static argTypes = {} satisfies RequiredMap<LowerHexExprArgs>;

  declare args: LowerHexExprArgs;
  constructor (args: LowerHexExprArgs) {
    super(args);
  }
}

export type AndExprArgs = ConnectorExprArgs;
export class AndExpr extends multiInherit(ConnectorExpr, FuncExpr) {
  key = ExpressionKey.AND;
  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  } satisfies RequiredMap<AndExprArgs>;

  declare args: AndExprArgs;
  constructor (args: AndExprArgs) {
    super(args);
  }
}

export type OrExprArgs = ConnectorExprArgs;
export class OrExpr extends multiInherit(ConnectorExpr, FuncExpr) {
  key = ExpressionKey.OR;
  static argTypes = {
    // @ts-expect-error - super.argTypes not accessible in multiInherit classes
    ...super.argTypes,
  } satisfies RequiredMap<OrExprArgs>;

  declare args: OrExprArgs;
  constructor (args: OrExprArgs) {
    super(args);
  }
}

export type XorExprArgs = {
  this?: Expression;
  expression?: Expression;
  expressions?: Expression[];
  roundInput?: Expression;
} & ConnectorExprArgs;

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
  } satisfies RequiredMap<XorExprArgs>;

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

export type JSONObjectAggExprArgs = { nullHandling?: Expression;
  uniqueKeys?: Expression[];
  returnType?: DataTypeExpr;
  encoding?: Expression; } & AggFuncExprArgs;

export class JSONObjectAggExpr extends AggFuncExpr {
  key = ExpressionKey.JSON_OBJECT_AGG;

  /**
   * Defines the arguments (properties and child expressions) for JSONObjectAgg expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    nullHandling: false,
    uniqueKeys: false,
    returnType: false,
    encoding: false,
  } satisfies RequiredMap<JSONObjectAggExprArgs>;

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

  get $returnType (): Expression | undefined {
    return this.args.returnType;
  }

  get $encoding (): Expression | undefined {
    return this.args.encoding;
  }

  static {
    this.register();
  }
}

export type JSONBObjectAggExprArgs = {
  this: Expression;
  expression: Expression;
} & AggFuncExprArgs;

export class JSONBObjectAggExpr extends AggFuncExpr {
  key = ExpressionKey.JSONB_OBJECT_AGG;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expression: true,
  } satisfies RequiredMap<JSONBObjectAggExprArgs>;

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

export type JSONArrayAggExprArgs = { order?: Expression;
  nullHandling?: Expression;
  returnType?: DataTypeExpr;
  strict?: Expression; } & AggFuncExprArgs;

export class JSONArrayAggExpr extends AggFuncExpr {
  key = ExpressionKey.JSON_ARRAY_AGG;

  /**
   * Defines the arguments (properties and child expressions) for JSONArrayAgg expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    order: false,
    nullHandling: false,
    returnType: false,
    strict: false,
  } satisfies RequiredMap<JSONArrayAggExprArgs>;

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

  get $returnType (): Expression | undefined {
    return this.args.returnType;
  }

  get $strict (): Expression | undefined {
    return this.args.strict;
  }

  static {
    this.register();
  }
}

export type LogicalOrExprArgs = AggFuncExprArgs;

export class LogicalOrExpr extends AggFuncExpr {
  key = ExpressionKey.LOGICAL_OR;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<LogicalOrExprArgs>;

  declare args: LogicalOrExprArgs;

  constructor (args: LogicalOrExprArgs) {
    super(args);
  }

  static sqlNames = [
    'LOGICAL_OR',
    'BOOL_OR',
    'BOOLOR_AGG',
  ];

  static {
    this.register();
  }
}

export type LogicalAndExprArgs = AggFuncExprArgs;

export class LogicalAndExpr extends AggFuncExpr {
  key = ExpressionKey.LOGICAL_AND;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<LogicalAndExprArgs>;

  declare args: LogicalAndExprArgs;

  constructor (args: LogicalAndExprArgs) {
    super(args);
  }

  static sqlNames = [
    'LOGICAL_AND',
    'BOOL_AND',
    'BOOLAND_AGG',
  ];

  static {
    this.register();
  }
}

export type MaxExprArgs = AggFuncExprArgs;

export class MaxExpr extends AggFuncExpr {
  key = ExpressionKey.MAX;
    static isVarLenArgs = true;
  static argTypes = {} satisfies RequiredMap<MaxExprArgs>;

  declare args: MaxExprArgs;

  constructor (args: MaxExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MedianExprArgs = AggFuncExprArgs;

export class MedianExpr extends AggFuncExpr {
  key = ExpressionKey.MEDIAN;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<MedianExprArgs>;

  declare args: MedianExprArgs;

  constructor (args: MedianExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ModeExprArgs = { deterministic?: Expression } & AggFuncExprArgs;

export class ModeExpr extends AggFuncExpr {
  key = ExpressionKey.MODE;

  static argTypes = { deterministic: false } satisfies RequiredMap<ModeExprArgs>;

  declare args: ModeExprArgs;

  constructor (args: ModeExprArgs) {
    super(args);
  }

  get $deterministic (): Expression | undefined {
    return this.args.deterministic;
  }

  static {
    this.register();
  }
}

export type MinExprArgs = AggFuncExprArgs;

export class MinExpr extends AggFuncExpr {
  key = ExpressionKey.MIN;
    static isVarLenArgs = true;
  static argTypes = {} satisfies RequiredMap<MinExprArgs>;

  declare args: MinExprArgs;

  constructor (args: MinExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type NtileExprArgs = AggFuncExprArgs;

export class NtileExpr extends AggFuncExpr {
  key = ExpressionKey.NTILE;

  static argTypes = {} satisfies RequiredMap<NtileExprArgs>;

  declare args: NtileExprArgs;

  constructor (args: NtileExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type PercentileContExprArgs = AggFuncExprArgs;

export class PercentileContExpr extends AggFuncExpr {
  key = ExpressionKey.PERCENTILE_CONT;

  static argTypes = {} satisfies RequiredMap<PercentileContExprArgs>;

  declare args: PercentileContExprArgs;

  constructor (args: PercentileContExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type PercentileDiscExprArgs = AggFuncExprArgs;

export class PercentileDiscExpr extends AggFuncExpr {
  key = ExpressionKey.PERCENTILE_DISC;

  static argTypes = {} satisfies RequiredMap<PercentileDiscExprArgs>;

  declare args: PercentileDiscExprArgs;

  constructor (args: PercentileDiscExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type PercentRankExprArgs = AggFuncExprArgs;

export class PercentRankExpr extends AggFuncExpr {
  key = ExpressionKey.PERCENT_RANK;
    static isVarLenArgs = true;
  static argTypes = {} satisfies RequiredMap<PercentRankExprArgs>;

  declare args: PercentRankExprArgs;

  constructor (args: PercentRankExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type QuantileExprArgs = { quantile: Expression } & AggFuncExprArgs;

export class QuantileExpr extends AggFuncExpr {
  key = ExpressionKey.QUANTILE;

  static argTypes = { quantile: true } satisfies RequiredMap<QuantileExprArgs>;

  declare args: QuantileExprArgs;

  constructor (args: QuantileExprArgs) {
    super(args);
  }

  get $quantile (): Expression {
    return this.args.quantile;
  }

  static {
    this.register();
  }
}

export type ApproxPercentileAccumulateExprArgs = AggFuncExprArgs;

export class ApproxPercentileAccumulateExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_PERCENTILE_ACCUMULATE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<ApproxPercentileAccumulateExprArgs>;

  declare args: ApproxPercentileAccumulateExprArgs;

  constructor (args: ApproxPercentileAccumulateExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RankExprArgs = AggFuncExprArgs;

export class RankExpr extends AggFuncExpr {
  key = ExpressionKey.RANK;
    static isVarLenArgs = true;
  static argTypes = {} satisfies RequiredMap<RankExprArgs>;

  declare args: RankExprArgs;

  constructor (args: RankExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrValxExprArgs = AggFuncExprArgs;

export class RegrValxExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_VALX;

  static argTypes = {} satisfies RequiredMap<RegrValxExprArgs>;

  declare args: RegrValxExprArgs;

  constructor (args: RegrValxExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrValyExprArgs = AggFuncExprArgs;

export class RegrValyExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_VALY;

  static argTypes = {} satisfies RequiredMap<RegrValyExprArgs>;

  declare args: RegrValyExprArgs;

  constructor (args: RegrValyExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrAvgyExprArgs = AggFuncExprArgs;

export class RegrAvgyExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_AVGY;

  static argTypes = {} satisfies RequiredMap<RegrAvgyExprArgs>;

  declare args: RegrAvgyExprArgs;

  constructor (args: RegrAvgyExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrAvgxExprArgs = AggFuncExprArgs;

export class RegrAvgxExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_AVGX;

  static argTypes = {} satisfies RequiredMap<RegrAvgxExprArgs>;

  declare args: RegrAvgxExprArgs;

  constructor (args: RegrAvgxExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrCountExprArgs = AggFuncExprArgs;

export class RegrCountExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_COUNT;

  static argTypes = {} satisfies RequiredMap<RegrCountExprArgs>;

  declare args: RegrCountExprArgs;

  constructor (args: RegrCountExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrInterceptExprArgs = AggFuncExprArgs;

export class RegrInterceptExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_INTERCEPT;

  static argTypes = {} satisfies RequiredMap<RegrInterceptExprArgs>;

  declare args: RegrInterceptExprArgs;

  constructor (args: RegrInterceptExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrR2ExprArgs = AggFuncExprArgs;

export class RegrR2Expr extends AggFuncExpr {
  key = ExpressionKey.REGR_R2;

  static argTypes = {} satisfies RequiredMap<RegrR2ExprArgs>;

  declare args: RegrR2ExprArgs;

  constructor (args: RegrR2ExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrSxxExprArgs = AggFuncExprArgs;

export class RegrSxxExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_SXX;

  static argTypes = {} satisfies RequiredMap<RegrSxxExprArgs>;

  declare args: RegrSxxExprArgs;

  constructor (args: RegrSxxExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrSxyExprArgs = AggFuncExprArgs;

export class RegrSxyExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_SXY;

  static argTypes = {} satisfies RequiredMap<RegrSxyExprArgs>;

  declare args: RegrSxyExprArgs;

  constructor (args: RegrSxyExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrSyyExprArgs = AggFuncExprArgs;

export class RegrSyyExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_SYY;

  static argTypes = {} satisfies RequiredMap<RegrSyyExprArgs>;

  declare args: RegrSyyExprArgs;

  constructor (args: RegrSyyExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrSlopeExprArgs = AggFuncExprArgs;

export class RegrSlopeExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_SLOPE;

  static argTypes = {} satisfies RequiredMap<RegrSlopeExprArgs>;

  declare args: RegrSlopeExprArgs;

  constructor (args: RegrSlopeExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SumExprArgs = AggFuncExprArgs;

export class SumExpr extends AggFuncExpr {
  key = ExpressionKey.SUM;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SumExprArgs>;

  declare args: SumExprArgs;

  constructor (args: SumExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StddevExprArgs = AggFuncExprArgs;

export class StddevExpr extends AggFuncExpr {
  key = ExpressionKey.STDDEV;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<StddevExprArgs>;

  declare args: StddevExprArgs;

  constructor (args: StddevExprArgs) {
    super(args);
  }

  static sqlNames = ['STDDEV', 'STDEV'];

  static {
    this.register();
  }
}

export type StddevPopExprArgs = AggFuncExprArgs;

export class StddevPopExpr extends AggFuncExpr {
  key = ExpressionKey.STDDEV_POP;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<StddevPopExprArgs>;

  declare args: StddevPopExprArgs;

  constructor (args: StddevPopExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StddevSampExprArgs = AggFuncExprArgs;

export class StddevSampExpr extends AggFuncExpr {
  key = ExpressionKey.STDDEV_SAMP;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<StddevSampExprArgs>;

  declare args: StddevSampExprArgs;

  constructor (args: StddevSampExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CumeDistExprArgs = AggFuncExprArgs;

export class CumeDistExpr extends AggFuncExpr {
  key = ExpressionKey.CUME_DIST;
    static isVarLenArgs = true;
  static argTypes = {} satisfies RequiredMap<CumeDistExprArgs>;

  declare args: CumeDistExprArgs;

  constructor (args: CumeDistExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type VarianceExprArgs = AggFuncExprArgs;

export class VarianceExpr extends AggFuncExpr {
  key = ExpressionKey.VARIANCE;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<VarianceExprArgs>;

  declare args: VarianceExprArgs;

  constructor (args: VarianceExprArgs) {
    super(args);
  }

  static sqlNames = [
    'VARIANCE',
    'VARIANCE_SAMP',
    'VAR_SAMP',
  ];

  static {
    this.register();
  }
}

export type VariancePopExprArgs = AggFuncExprArgs;

export class VariancePopExpr extends AggFuncExpr {
  key = ExpressionKey.VARIANCE_POP;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<VariancePopExprArgs>;

  declare args: VariancePopExprArgs;

  constructor (args: VariancePopExprArgs) {
    super(args);
  }

  static sqlNames = ['VARIANCE_POP', 'VAR_POP'];

  static {
    this.register();
  }
}

export type KurtosisExprArgs = AggFuncExprArgs;

export class KurtosisExpr extends AggFuncExpr {
  key = ExpressionKey.KURTOSIS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<KurtosisExprArgs>;

  declare args: KurtosisExprArgs;

  constructor (args: KurtosisExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SkewnessExprArgs = AggFuncExprArgs;

export class SkewnessExpr extends AggFuncExpr {
  key = ExpressionKey.SKEWNESS;

  static argTypes = {
    ...super.argTypes,
  } satisfies RequiredMap<SkewnessExprArgs>;

  declare args: SkewnessExprArgs;

  constructor (args: SkewnessExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CovarSampExprArgs = AggFuncExprArgs;

export class CovarSampExpr extends AggFuncExpr {
  key = ExpressionKey.COVAR_SAMP;

  static argTypes = {} satisfies RequiredMap<CovarSampExprArgs>;

  declare args: CovarSampExprArgs;

  constructor (args: CovarSampExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CovarPopExprArgs = AggFuncExprArgs;

export class CovarPopExpr extends AggFuncExpr {
  key = ExpressionKey.COVAR_POP;

  static argTypes = {} satisfies RequiredMap<CovarPopExprArgs>;

  declare args: CovarPopExprArgs;

  constructor (args: CovarPopExprArgs) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators
 */
export type CombinedAggFuncExprArgs = {
  this: Expression;
  expressions?: Expression[];
} & AnonymousAggFuncExprArgs;

export class CombinedAggFuncExpr extends AnonymousAggFuncExpr {
  key = ExpressionKey.COMBINED_AGG_FUNC;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: false,
  } satisfies RequiredMap<CombinedAggFuncExprArgs>;

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

export type CombinedParameterizedAggExprArgs = {
  this: Expression;
  expressions: Expression[];
  params: Expression[];
} & ParameterizedAggExprArgs;

export class CombinedParameterizedAggExpr extends ParameterizedAggExpr {
  key = ExpressionKey.COMBINED_PARAMETERIZED_AGG;

  static argTypes = {
    ...super.argTypes,
    this: true,
    expressions: true,
    params: true,
  } satisfies RequiredMap<CombinedParameterizedAggExprArgs>;

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

export type PosexplodeOuterExprArgs = BaseExpressionArgs;
export class PosexplodeOuterExpr extends PosexplodeExpr {
  key = ExpressionKey.POSEXPLODE_OUTER;
  static argTypes = {} satisfies RequiredMap<PosexplodeOuterExprArgs>;

  declare args: PosexplodeOuterExprArgs;
  constructor (args: PosexplodeOuterExprArgs) {
    super(args);
  }
}

export type ApproxQuantileExprArgs = { quantile: Expression;
  accuracy?: Expression;
  weight?: Expression;
  errorTolerance?: Expression; } & QuantileExprArgs;

export class ApproxQuantileExpr extends QuantileExpr {
  key = ExpressionKey.APPROX_QUANTILE;

  /**
   * Defines the arguments (properties and child expressions) for ApproxQuantile expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ...super.argTypes,
    quantile: true,
    accuracy: false,
    weight: false,
    errorTolerance: false,
  } satisfies RequiredMap<ApproxQuantileExprArgs>;

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
export function column (name: string, table?: string): ColumnExpr {
  const args: ColumnExprArgs = { this: new IdentifierExpr({ this: name }) };
  if (table) {
    args.table = new IdentifierExpr({ this: table });
  }
  return new ColumnExpr(args);
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
 * Create an alias expression.
 *
 * @param expr - Expression to alias
 * @param alias - Alias name
 * @returns Alias expression
 *
 * @example
 * // SELECT col AS alias
 * const aliased = alias(column('col'), 'alias');
 */
/**
 * Create an ALIAS expression.
 *
 * @param expr - The expression to alias
 * @param alias - The alias name
 * @param options - Options object
 * @param options.quoted - Whether to quote the alias
 * @param options.dialect - The dialect to use for parsing
 * @param options.wrap - Whether to wrap in parentheses
 * @returns ALIAS expression
 */
export function alias (
  expr: Expression,
  alias: string,
  options?: {
    quoted?: boolean;
    dialect?: DialectType;
    wrap?: boolean;
    copy?: boolean;
  },
): AliasExpr {
  void options; // Mark as intentionally unused until implemented
  return new AliasExpr({
    this: expr,
    alias: new IdentifierExpr({ this: alias }),
  });
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
export function and (
  conditions: (Expression | undefined)[],
  _options?: { dialect?: DialectType;
    copy?: boolean;
    wrap?: boolean; },
): Expression | undefined {
  const validConditions = conditions.filter((c): c is Expression => c !== undefined);

  if (validConditions.length === 0) {
    return undefined;
  }

  if (validConditions.length === 1) {
    return validConditions[0];
  }

  // Chain: a AND b AND c becomes And(And(a, b), c)
  let result: Expression = new AndExpr({
    this: validConditions[0],
    expression: validConditions[1],
  });

  for (let i = 2; i < validConditions.length; i++) {
    result = new AndExpr({
      this: result,
      expression: validConditions[i],
    });
  }

  return result;
}

/**
 * Create an OR expression from multiple conditions.
 * Automatically chains multiple conditions with OR.
 *
 * @param conditions - Conditions to OR together (nulls are filtered out)
 * @param options - Options object
 * @param options.dialect - The dialect to use for parsing
 * @param options.copy - Whether to copy expressions (handled by caller)
 * @param options.wrap - Whether to wrap in Parens
 * @returns OR expression or single condition if only one provided
 *
 * @example
 * // WHERE a = 1 OR b = 2 OR c = 3
 * const condition = or([cond1, cond2, cond3]);
 *
 * @example
 * // With options
 * const condition = or([cond1, cond2], { wrap: true });
 */
export function or (
  conditions: (Expression | undefined)[],
  _options?: { dialect?: DialectType;
    copy?: boolean;
    wrap?: boolean; },
): Expression | undefined {
  const validConditions = conditions.filter((c): c is Expression => c != null);

  if (validConditions.length === 0) {
    return undefined;
  }

  if (validConditions.length === 1) {
    return validConditions[0];
  }

  // Chain: a OR b OR c becomes Or(Or(a, b), c)
  let result: Expression = new OrExpr({
    this: validConditions[0],
    expression: validConditions[1],
  });

  for (let i = 2; i < validConditions.length; i++) {
    result = new OrExpr({
      this: result,
      expression: validConditions[i],
    });
  }

  return result;
}

/**
 * Create a NOT expression
 * @param expr - Expression to negate
 * @returns NOT expression
 */
export function not (expr: Expression): NotExpr {
  return new NotExpr({ this: expr });
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
export function select (...columns: (string | Expression)[]): SelectExpr {
  const expressions = columns.map((col) =>
    typeof col === 'string'
      ? column(col)
      : col);
  return new SelectExpr({ expressions });
}

/**
 * Create a FROM expression
 * @param table - Table or expression
 * @returns FROM expression
 */
export function from (table: string | Expression): FromExpr {
  const tableExpr = typeof table === 'string'
    ? new TableExpr({ this: new IdentifierExpr({ this: table }) })
    : table;
  return new FromExpr({ expressions: [tableExpr] });
}

/**
 * Create a CASE expression
 * @param conditions - Condition-value pairs
 * @param defaultValue - Default value
 * @returns CASE expression
 */
export function case_ (conditions?: Expression[], defaultValue?: Expression): CaseExpr {
  return new CaseExpr({
    ifs: conditions || [],
    default: defaultValue,
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
export function cast (expr: Expression, toType: DataTypeExpr | string): CastExpr {
  const dataType = typeof toType === 'string'
    ? DataTypeExpr.build(toType)
    : toType;
  return new CastExpr({
    this: expr,
    to: dataType,
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
 * Create a subquery expression
 * @param query - Query expression
 * @param alias - Optional alias
 * @returns Subquery expression
 */
export function subqueryExpr (query: Expression, alias?: string): SubqueryExpr {
  const args: SubqueryExprArgs = { this: query };
  if (alias) {
    args.alias = new IdentifierExpr({ this: alias });
  }
  return new SubqueryExpr(args);
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
  expressions: Array<string | Expression>,
  setOperation: typeof SetOperationExpr,
  options: {
    distinct?: boolean;
    dialect?: DialectType;
    copy?: boolean;
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
  expressions: Array<string | Expression>,
  options: {
    distinct?: boolean;
    dialect?: DialectType;
    copy?: boolean;
  } = {},
): UnionExpr {
  if (expressions.length < 2) {
    throw new Error('At least two expressions are required by `unionExpr`.');
  }
  return _applySetOperation(expressions, UnionExpr, options);
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
  expressions: Array<string | Expression>,
  options: {
    distinct?: boolean;
    dialect?: DialectType;
    copy?: boolean;
  } = {},
): IntersectExpr {
  if (expressions.length < 2) {
    throw new Error('At least two expressions are required by `intersectExpr`.');
  }
  return _applySetOperation(expressions, IntersectExpr, options);
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
  expressions: Array<string | Expression>,
  options: {
    distinct?: boolean;
    dialect?: DialectType;
    copy?: boolean;
  } = {},
): ExceptExpr {
  if (expressions.length < 2) {
    throw new Error('At least two expressions are required by `exceptExpr`.');
  }
  return _applySetOperation(expressions, ExceptExpr, options);
}

/**
 * Create an INSERT expression
 * @param table - Target table
 * @param values - Values to insert
 * @returns INSERT expression
 */
export function insert (table: Expression, values?: Expression): InsertExpr {
  return new InsertExpr({
    this: table,
    expression: values,
  });
}

/**
 * Create a DELETE expression
 * @param table - Target table
 * @param where - Optional WHERE condition
 * @returns DELETE expression
 */
export function delete_ (table: Expression, where?: Expression): DeleteExpr {
  const args: DeleteExprArgs = { this: table };
  if (where) {
    args.where = where;
  }
  return new DeleteExpr(args);
}

/**
 * Create a MERGE expression
 * @param target - Target table
 * @param using - Source table
 * @param on - Join condition
 * @returns MERGE expression
 */
export function mergeExpr (target: Expression, using: Expression, on: Expression): MergeExpr {
  return new MergeExpr({
    this: target,
    using: using.name || '',
    on,
    whens: [],
  });
}

/**
 * Create a condition expression from SQL text
 * @param sql - SQL condition text
 * @returns Condition expression (placeholder for now)
 */
export function condition (sql: string): Expression {
  return new ConditionExpr({ this: sql });
}

/**
 * Parse SQL text into an expression if needed
 * @param sqlOrExpression - SQL text or expression
 * @param options - Parsing options
 * @returns Expression
 */
export function maybeParse (
  sqlOrExpression: string | number | boolean | Expression | null | undefined,
  options?: {
    into?: typeof Expression;
    dialect?: DialectType;
    prefix?: string;
    copy?: boolean;
  },
): Expression {
  // If it's already an Expression
  if (sqlOrExpression instanceof Expression) {
    if (options?.copy) {
      return sqlOrExpression.copy();
    }
    return sqlOrExpression;
  }

  // SQL cannot be None/null
  if (sqlOrExpression === null || sqlOrExpression === undefined) {
    throw new Error('SQL cannot be null or undefined');
  }

  // Convert to string and optionally add prefix
  let _sql = String(sqlOrExpression);
  if (options?.prefix) {
    _sql = `${options.prefix} ${_sql}`;
  }

  // TODO: Implement actual SQL parsing when parser is available

  throw new Error('SQL parsing not yet implemented. Parser module required.');
}

/**
 * Convert SQL text to a column expression
 * @param sql - SQL text
 * @param dialect - SQL dialect
 * @returns Column expression
 */
export function toColumn (sql: string, _dialect?: DialectType): ColumnExpr {
  return column(sql);
}

/**
 * Convert SQL text to an identifier expression
 * @param name - Identifier name
 * @param quoted - Whether to quote the identifier
 * @returns Identifier expression
 */
export function toIdentifier (name: string, quoted = false): IdentifierExpr {
  return new IdentifierExpr({
    this: name,
    quoted,
  });
}

/**
 * Convert SQL text to a table expression
 * @param sql - SQL text
 * @param dialect - SQL dialect
 * @returns Table expression
 */
export function toTable (sql: string, _dialect?: DialectType): TableExpr {
  return table(sql);
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
export function findTables (expr: Expression): TableExpr[] {
  const tables: TableExpr[] = [];
  for (const node of expr.walk()) {
    if (node instanceof TableExpr) {
      tables.push(node);
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
export function maybeCopy (instance: Expression | undefined, copy = true): Expression | undefined {
  if (copy && instance) {
    return instance.copy();
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
  const indent = '\n' + ('  '.repeat(level + 1));
  let delim = `,${indent}`;

  if (node instanceof Expression) {
    const args: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(node.args)) {
      if ((v !== null && v !== undefined && (!Array.isArray(v) || 0 < v.length)) || verbose) {
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
      delim = ', ';
    }

    const isReprStr = node.isString || (node instanceof IdentifierExpr && node.$quoted);
    const items = Object.entries(args)
      .map(([k, v]) => `${k}=${_toS(v, verbose, level + 1, isReprStr)}`)
      .join(delim);

    return `${node.constructor.name}(${items
      ? indent + items
      : ''})`;
  }

  if (Array.isArray(node)) {
    const items = node.map((i) => _toS(i, verbose, level + 1)).join(delim);
    return `[${items
      ? indent + items
      : ''}]`;
  }

  // Use JSON.stringify for strings if reprStr is true
  if (reprStr && typeof node === 'string') {
    return JSON.stringify(node);
  }

  // Indent multiline strings to match the current level
  const str = String(node).trim();
  return str.split('\n').join(indent);
}

/**
 * Check if an expression is the wrong type
 * @param expression - The expression to check
 * @param into - The expected expression class
 * @returns True if the expression is wrong type
 */
function _isWrongExpression (expression: unknown, into: typeof Expression): boolean {
  return expression instanceof Expression && !(expression instanceof into);
}

/**
 * Apply a builder function that sets a single argument on an instance
 * @param options - Options object
 * @returns The modified instance
 */
function _applyBuilder (expression: Expression | string | number, options: {
  instance: Expression;
  arg: string;
  copy?: boolean;
  prefix?: string;
  into?: typeof Expression;
  dialect?: DialectType;
  intoArg?: string;
}): Expression {
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

  if (into && _isWrongExpression(expression, into)) {
    expression = new into({ [intoArg]: expression });
  }

  const inst = maybeCopy(instance, copy)!;
  expression = maybeParse(expression, {
    prefix,
    into,
    dialect,
    ...opts,
  });

  inst.set(arg, expression);
  return inst;
}

/**
 * Apply a builder function that sets a list of child expressions
 * @param options - Options object
 * @returns The modified instance
 */
function _applyChildListBuilder (
  expressions: Array<string | Expression>,
  options: {
    instance: Expression;
    arg: string;
    append?: boolean;
    copy?: boolean;
    prefix?: string;
    into?: typeof Expression;
    dialect?: DialectType;
    properties?: Record<string, ExpressionValue | ExpressionValueList>;
  },
): Expression {
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

  for (const expression of expressions) {
    let expr = expression;
    if (into && _isWrongExpression(expr, into)) {
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

  const existing = inst.args[arg] as Expression | undefined;
  let allExpressions = parsed;
  if (append && existing && existing.args.expressions) {
    allExpressions = [...(existing.args.expressions as Expression[]), ...parsed];
  }

  const child = into
    ? new into({ expressions: allExpressions })
    : new Expression({ expressions: allExpressions });
  for (const [k, v] of Object.entries(properties)) {
    child.set(k, v as ExpressionValue | ExpressionValueList);
  }
  inst.set(arg, child);

  return inst;
}

/**
 * Apply a builder function that sets a flat list of expressions
 * @param expressions - Array of expressions to add
 * @param options - Options object
 * @returns The modified instance
 */
function _applyListBuilder (
  expressions: Array<string | Expression>,
  options: {
    instance: Expression;
    arg: string;
    append?: boolean;
    copy?: boolean;
    prefix?: string;
    into?: typeof Expression;
    dialect?: DialectType;
  },
): Expression {
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

  const parsedExpressions = expressions
    .filter((expr) => expr !== null && expr !== undefined)
    .map((expr) =>
      maybeParse(expr, {
        into,
        prefix,
        dialect,
        ...opts,
      }));

  const existing = inst.args[arg] as Expression[] | undefined;
  if (append && existing) {
    inst.set(arg, [...existing, ...parsedExpressions]);
  } else {
    inst.set(arg, parsedExpressions);
  }

  return inst;
}

/**
 * Apply a conjunction builder (combines expressions with AND)
 * @param expressions - Array of expressions to combine with AND
 * @param options - Options object
 * @returns The modified instance
 */
function _applyConjunctionBuilder (
  expressions: Array<string | Expression>,
  options: {
    instance: Expression;
    arg: string;
    into?: typeof Expression;
    append?: boolean;
    copy?: boolean;
    dialect?: DialectType;
  },
): Expression {
  const {
    instance,
    arg,
    into,
    append = true,
    copy = true,
    dialect,
    ...opts
  } = options;

  const inst = maybeCopy(instance, copy)!;

  // Parse all expressions
  const parsedExpressions = expressions
    .filter((expr) => expr !== undefined)
    .map((expr) =>
      maybeParse(expr, {
        into,
        dialect,
        ...opts,
      }));

  let combined: Expression | undefined;
  if (0 < parsedExpressions.length) {
    combined = parsedExpressions.reduce((left, right) =>
      new AndExpr({
        this: left,
        expression: right,
      }));
  }

  const existing = inst.args[arg] as Expression | undefined;
  if (append && existing && combined) {
    combined = new AndExpr({
      this: existing,
      expression: combined,
    });
  }

  if (combined && into) {
    combined = new into({ this: combined });
  }

  if (combined) {
    inst.set(arg, combined);
  }

  return inst;
}

/**
 * Apply a CTE builder
 * @param options - Options object
 * @returns The modified instance
 */
function _applyCteBuilder (options: {
  instance: Expression;
  alias: string | Expression;
  as: string | Expression;
  recursive?: boolean;
  materialized?: boolean;
  append?: boolean;
  dialect?: DialectType;
  copy?: boolean;
  scalar?: boolean;
}): Expression {
  const {
    instance,
    alias,
    as: asExpr,
    recursive = false,
    materialized,
    append = true,
    dialect,
    copy = true,
    scalar,
    ...opts
  } = options;

  const inst = maybeCopy(instance, copy)!;

  // Parse alias
  const aliasExpression = typeof alias === 'string'
    ? new TableAliasExpr({ this: toIdentifier(alias) })
    : alias;

  const asExpression = maybeParse(asExpr, {
    dialect,
    ...opts,
  });

  const cte = new CTEExpr({
    this: asExpression,
    alias: aliasExpression,
    scalar,
    materialized,
  });
  // Get or create the WITH expression
  let withExpr = inst.args['with'] as WithExpr | undefined;
  if (!withExpr) {
    withExpr = new WithExpr({ recursive });
  }

  const existingCtes = (withExpr.expressions || []) as CTEExpr[];

  const ctes = append
    ? [...existingCtes, cte]
    : [cte];

  withExpr.set('expressions', ctes);
  if (recursive !== undefined) {
    withExpr.set('recursive', recursive);
  }

  inst.set('with', withExpr);

  return inst;
}

/**
 * Wrap an expression in parentheses if it's of a certain type
 * @param expression - Expression to potentially wrap
 * @param kind - The expression class to check against
 * @returns The expression wrapped in parentheses if it matches the kind, otherwise the original
 * expression
 */
function _wrap (expression: Expression | undefined, kind: typeof Expression): Expression | undefined {
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
  if (value === undefined || value === null || (typeof value === 'number' && isNaN(value))) {
    return null_();
  }

  // Handle numbers
  if (typeof value === 'number') {
    return LiteralExpr.number(value);
  }

  // Handle Date objects (datetime.datetime)
  if (value instanceof Date) {
    const datetimeLiteral = LiteralExpr.string(
      value.toISOString().replace('T', ' ')
        .replace(/\.\d{3}Z$/, ''),
    );

    let tz: LiteralExpr | undefined;
    const timezoneOffset = value.getTimezoneOffset();
    if (timezoneOffset !== 0) {
      const hours = Math.floor(Math.abs(timezoneOffset) / 60);
      const minutes = Math.abs(timezoneOffset) % 60;
      const sign = 0 < timezoneOffset
        ? '-'
        : '+';
      tz = LiteralExpr.string(
        `${sign}${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}`,
      );
    }

    return new TimeStrToTimeExpr({
      this: datetimeLiteral,
      zone: tz,
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
