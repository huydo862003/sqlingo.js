// https://github.com/tobymao/sqlglot/blob/main/sqlglot/expressions.py

import { createHash } from 'crypto';
import { Dialect, type DialectType } from './dialects/dialect';
import type { Token } from './tokens';
import { ensureList } from './helper';

export const SQLGLOT_META = 'sqlglot.meta';
export const SQLGLOT_ANONYMOUS = 'sqlglot.anonymous';
export const TABLE_PARTS = ['this', 'db', 'catalog'] as const;
export const COLUMN_PARTS = ['this', 'table', 'db', 'catalog'] as const;
export const POSITION_META_KEYS = ['line', 'col', 'start', 'end'] as const;

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

export type ExpressionValue = Expression | string | boolean | number | undefined;
export type ExpressionValueList<T extends ExpressionValue = ExpressionValue> = T[];

/**
 * Base arguments that all Expression classes can accept.
 */
export interface BaseExpressionArgs {
  this?: ExpressionValue;
  expression?: Expression;
  expressions?: (Expression | string)[];
  [key: string]: ExpressionValueList | ExpressionValue;
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
  static argTypes: Record<string, boolean> = { this: true };

  /** Set of required argument names */

  constructor (args: BaseExpressionArgs = {}) {
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
    return Array.isArray(exprs) ? exprs : [];
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
      return typeof field.this === 'string' ? field.this : '';
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
    return (this instanceof LiteralExpr && !this.args.isString) || (this instanceof NegExpr && (this.this as Expression).isNumber);
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
    return this instanceof StarExpr || (this instanceof ColumnExpr && this.this instanceof StarExpr);
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
      return columns.map((c: unknown) => (c instanceof Expression ? c.name : ''));
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
  isType (...dtypes: string[]): boolean {
    if (!this._type) {
      return false;
    }
    return this._type.isType(...dtypes);
  }

  /**
   * Check if this expression is a leaf node (has no child expressions)
   * @returns True if this expression has no Expression or list children
   */
  get isLeaf (): boolean {
    return !Object.values(this.args).some((v) =>
      (v instanceof Expression || Array.isArray(v)) && v,
    );
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

    while (stack.length > 0) {
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

        if (meta.length > 0) {
          for (const kv of meta.join('').split(',')) {
            const [key, ...valueParts] = kv.split('=');
            const value = valueParts.length > 0 ? valueParts[0].trim() : true;
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
  set (argKey: string, value: ExpressionValue | ExpressionValueList, index?: number, options?: { overwrite?: boolean }): void {
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
    const argValues = reverse ? Object.values(this.args).reverse() : Object.values(this.args);
    for (const value of argValues) {
      if (value instanceof Expression) {
        yield value;
      } else if (Array.isArray(value)) {
        const items = reverse ? [...value].reverse() : value;
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
  * walk (options?: { bfs?: boolean; prune?: (node: Expression) => boolean }): Generator<Expression> {
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

    while (stack.length > 0) {
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

    while (queue.length > 0) {
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
   * Note: Renamed from Python's unnest() to avoid conflict with InExpr.unnest property
   */
  unnestExpr (): Expression {
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
    return Array.from(this.iterExpressions()).map((arg) => arg.unnestExpr());
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
          yield node.unnestExpr();
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
   *               returns null/undefined, then the corresponding node will be removed from the syntax tree.
   * @param options - Options object
   * @param options.copy - if set to true a new tree instance is constructed, otherwise the tree is
   *                       modified in place (default: true)
   * @returns The transformed tree
   */
  transform (
    func: (node: Expression, options: Record<string, unknown>) => Expression | undefined,
    options?: { copy?: boolean; options?: Record<string, unknown> },
  ): Expression {
    const copy = options?.copy ?? true;

    let root: Expression | undefined = undefined;
    let newNode: Expression | undefined = undefined;

    const startNode = copy ? this.copy() : this;

    for (const node of startNode.dfs({ prune: (n) => n !== newNode })) {
      const parent = node.parent;
      const argKey = node.argKey;
      const index = node.index;

      newNode = func(node, { ...options, copy });

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
   * Note: Renamed from Python's replace() to avoid conflict with subclass replace properties
   *
   * For example:
   *   const tree = new SelectExpr(...).select("x").from("tbl");
   *   tree.find([ColumnExpr]).replaceWith(new ColumnExpr({this: "y"}));
   *   tree.sql() // 'SELECT y FROM tbl'
   *
   * @param expression - new node (or undefined to remove)
   * @returns The new expression
   */
  replaceWith<E extends Expression>(expression: E): E;
  replaceWith (expression: undefined): undefined;
  replaceWith<E extends Expression>(expression: E | undefined): E | undefined {
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
      value.parent?.replaceWith(expression);
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
    this.replaceWith(undefined);
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
   *               This is used to check that the provided arguments don't exceed the function argument limit.
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
      const argTypeCount = constructor.argTypes ? Object.keys(constructor.argTypes).length : 0;
      // Check if this function accepts variable-length arguments
      // (e.g., CONCAT, COALESCE can take any number of arguments)
      const isVarLen = (constructor as typeof FuncExpr).isVarLenArgs || false;
      if (args.length > argTypeCount && !isVarLen) {
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
   * @param options.wrap - Whether to wrap operands in Parens to avoid precedence issues (default: true)
   * @returns The new AND condition
   */
  andExpr (
    expressions: (Expression | undefined)[],
    options?: {
      dialect?: DialectType;
      copy?: boolean;
      wrap?: boolean;
      [key: string]: unknown;
    },
  ): Expression | undefined {
    const copy = options?.copy ?? true;
    const wrap = options?.wrap ?? true;
    return andExpr(expressions, { ...options, copy, wrap });
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
   * @param options.wrap - Whether to wrap operands in Parens to avoid precedence issues (default: true)
   * @returns The new OR condition
   */
  orExpr (
    expressions: (Expression | undefined)[],
    options?: { dialect?: DialectType; copy?: boolean; wrap?: boolean; [key: string]: unknown },
  ): Expression | undefined {
    const copy = options?.copy ?? true;
    const wrap = options?.wrap ?? true;
    return orExpr(expressions, { ...options, copy, wrap });
  }

  /**
   * Wrap this condition with NOT.
   * Note: Renamed from Python's not_() to negate() to avoid conflict with subclass not properties
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
  notExpr (options?: { copy?: boolean }): NotExpr | undefined {
    const copy = options?.copy ?? true;
    return orExpr([this], { ...options, copy });
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
  asExpr (
    alias: string | IdentifierExpr,
    options?: {
      quoted?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      wrap?: boolean;
      [key: string]: unknown;
    },
  ): AliasExpr {
    const copy = options?.copy ?? true;
    const aliasName = typeof alias === 'string' ? alias : alias.name;
    return aliasExpr(this, aliasName, { ...options, copy });
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
      return new klass({ this: other, expression: self });
    }
    return new klass({ this: self, expression: other });
  }

  hash (): string {
    if (this._hash !== undefined) {
      return this._hash;
    }

    const nodes: Expression[] = [];
    const queue: Expression[] = [this];

    while (queue.length > 0) {
      const node = queue.shift()!;
      nodes.push(node);
      for (const child of node.iterExpressions()) {
        if (child._hash === undefined) {
          queue.push(child);
        }
      }
    }

    for (let i = nodes.length - 1; i >= 0; i--) {
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
                const hashValue = typeof x === 'string' ? x.toLowerCase() : x;
                hash = this._hashString(hash + k + hashValue);
              } else {
                hash = this._hashString(hash + k);
              }
            }
          } else if (v !== undefined && v !== false) {
            const hashValue = typeof v === 'string' ? v.toLowerCase() : v;
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
  inExpr (
    expressions: unknown[],
    options?: {
      query?: string | Expression;
      unnest?: string | Expression | Array<string | Expression>;
      copy?: boolean;
      [key: string]: unknown;
    },
  ): InExpr {
    const copy = options?.copy ?? true;
    const unnest = options?.unnest;

    let subquery = options?.query ? maybeParse(options.query as string | Expression) : undefined;

    if (subquery && !(subquery instanceof SubqueryExpr)) {
      subquery = subquery.subquery({ copy: false });
    }

    return new InExpr({
      this: maybeCopy(this, copy),
      expressions: expressions.map((e) => convert(e, copy)),
      query: subquery,
      unnest: unnest
        ? new UnnestExpr({ expressions: ensureList(unnest).map((e) => maybeParse(e, { ...options, copy })) })
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
  betweenExpr (
    low: unknown,
    high: unknown,
    options?: {
      copy?: boolean;
      symmetric?: boolean;
      [key: string]: unknown;
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
  isExpr (other: string | Expression): IsExpr {
    return this.binop(IsExpr, other);
  }

  /**
   * Create a LIKE expression.
   */
  likeExpr (other: string | Expression): LikeExpr {
    return this.binop(LikeExpr, other);
  }

  /**
   * Create an ILIKE expression.
   */
  ilikeExpr (other: string | Expression): ILikeExpr {
    return this.binop(ILikeExpr, other);
  }

  /**
   * Create an EQ (equals) expression.
   */
  eqExpr (other: unknown): EQExpr {
    return this.binop(EQExpr, other);
  }

  /**
   * Create a NEQ (not equals) expression.
   */
  neqExpr (other: unknown): NEQExpr {
    return this.binop(NEQExpr, other);
  }

  /**
   * Create a REGEXP_LIKE expression.
   */
  rlikeExpr (other: string | Expression): RegexpLikeExpr {
    return this.binop(RegexpLikeExpr, other);
  }

  /**
   * Create a DIV expression with optional typed and safe flags.
   */
  divExpr (other: string | Expression, typed = false, safe = false): DivExpr {
    const div = this.binop(DivExpr, other);
    div.set('typed', typed);
    div.set('safe', safe);
    return div;
  }

  /**
   * Create an ascending ORDER BY expression.
   */
  ascExpr (nullsFirst = true): OrderedExpr {
    return new OrderedExpr({
      this: this.copy(),
      nullsFirst: convert(nullsFirst, false),
    });
  }

  /**
   * Create a descending ORDER BY expression.
   */
  descExpr (nullsFirst = false): OrderedExpr {
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
  ltExpr (other: unknown): LTExpr {
    return this.binop(LTExpr, other);
  }

  /**
   * Create an LTE (less than or equal) expression.
   */
  lteExpr (other: unknown): LTEExpr {
    return this.binop(LTEExpr, other);
  }

  /**
   * Create a GT (greater than) expression.
   */
  gtExpr (other: unknown): GTExpr {
    return this.binop(GTExpr, other);
  }

  /**
   * Create a GTE (greater than or equal) expression.
   */
  gteExpr (other: unknown): GTEExpr {
    return this.binop(GTEExpr, other);
  }

  // Arithmetic operators

  /**
   * Create an ADD expression.
   */
  addExpr (other: unknown): AddExpr {
    return this.binop(AddExpr, other);
  }

  /**
   * Create an ADD expression (reversed operands).
   */
  raddExpr (other: unknown): AddExpr {
    return this.binop(AddExpr, other, { reverse: true });
  }

  /**
   * Create a SUB expression.
   */
  subExpr (other: unknown): SubExpr {
    return this.binop(SubExpr, other);
  }

  /**
   * Create a SUB expression (reversed operands).
   */
  rsubExpr (other: unknown): SubExpr {
    return this.binop(SubExpr, other, { reverse: true });
  }

  /**
   * Create a MUL expression.
   */
  mulExpr (other: unknown): MulExpr {
    return this.binop(MulExpr, other);
  }

  /**
   * Create a MUL expression (reversed operands).
   */
  rmulExpr (other: unknown): MulExpr {
    return this.binop(MulExpr, other, { reverse: true });
  }

  /**
   * Create a DIV expression.
   */
  truedivExpr (other: unknown): DivExpr {
    return this.binop(DivExpr, other);
  }

  /**
   * Create a DIV expression (reversed operands).
   */
  rtruedivExpr (other: unknown): DivExpr {
    return this.binop(DivExpr, other, { reverse: true });
  }

  /**
   * Create an INTDIV expression.
   */
  floordivExpr (other: unknown): IntDivExpr {
    return this.binop(IntDivExpr, other);
  }

  /**
   * Create an INTDIV expression (reversed operands).
   */
  rfloordivExpr (other: unknown): IntDivExpr {
    return this.binop(IntDivExpr, other, { reverse: true });
  }

  /**
   * Create a MOD expression.
   */
  modExpr (other: unknown): ModExpr {
    return this.binop(ModExpr, other);
  }

  /**
   * Create a MOD expression (reversed operands).
   */
  rmodExpr (other: unknown): ModExpr {
    return this.binop(ModExpr, other, { reverse: true });
  }

  /**
   * Create a POW expression.
   */
  powExpr (other: unknown): PowExpr {
    return this.binop(PowExpr, other);
  }

  /**
   * Create a POW expression (reversed operands).
   */
  rpowExpr (other: unknown): PowExpr {
    return this.binop(PowExpr, other, { reverse: true });
  }
}

export class ConditionExpr extends Expression {
  key = ExpressionKey.CONDITION;
}

export class PredicateExpr extends Expression {
  key = ExpressionKey.PREDICATE;
}

export class DerivedTableExpr extends Expression {
  key = ExpressionKey.DERIVED_TABLE;

  /**
   * Gets the select expressions from the derived table.
   * Returns the select expressions if this is a QueryExpr, otherwise returns an empty array.
   *
   * @returns Array of Expression objects representing the SELECT clause expressions
   */
  get selects (): Expression[] {
    return this.this instanceof QueryExpr ? this.this.selects : [];
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

export class QueryExpr extends Expression {
  key = ExpressionKey.QUERY;

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
      aliasExpr = new TableAliasExpr({ this: alias ? toIdentifier(alias) : undefined });
    }

    return new SubqueryExpr({ this: instance, alias: aliasExpr });
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
   *                     If another `Expression` instance is passed, it will be wrapped in a `Limit`.
   * @param options - Options object
   * @param options.dialect - The dialect used to parse the input expression
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns A limited query expression
   */
  limit (expression: string | number | Expression, options: { dialect?: DialectType; copy?: boolean; [key: string]: unknown } = {}): this {
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
   *                     If another `Expression` instance is passed, it will be wrapped in a `Offset`.
   * @param options - Options object
   * @param options.dialect - The dialect used to parse the input expression
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns The modified query expression
   */
  offset (expression: string | number | Expression, options: { dialect?: DialectType; copy?: boolean; [key: string]: unknown } = {}): this {
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
   *                      If another `Expression` instance is passed, it will be wrapped in a `Order`.
   * @param options - Options object
   * @param options.append - If `true`, add to any existing expressions. Otherwise, this flattens all the `Order` expression into a single expression. Default is `true`.
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
      [key: string]: unknown;
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
    const withExpr = this.args['with'] as WithExpr | undefined;
    return (withExpr?.expressions as CTEExpr[]) || [];
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
   * @param options.append - If `true`, add to any existing expressions. Otherwise, this resets the expressions. Default is `true`.
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
      [key: string]: unknown;
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
   * @param options.append - If `true`, AND the new expressions to any existing expression. Otherwise, this resets the expression. Default is `true`.
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
      [key: string]: unknown;
    } = {},
  ): this {
    const processedExpressions = expressions.map((expr) =>
      expr instanceof WhereExpr ? expr.this : expr,
    );

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
   * @param options.append - If `true`, add to any existing expressions. Otherwise, this resets the expressions. Default is `true`.
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
      [key: string]: unknown;
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
   * @param options.distinct - If `true`, uses UNION DISTINCT. Otherwise uses UNION ALL. Default is `true`.
   * @param options.dialect - The dialect used to parse the input expressions
   * @returns A Union expression
   */
  union (
    expressions: Array<string | Expression>,
    options: {
      distinct?: boolean;
      dialect?: DialectType;
      [key: string]: unknown;
    } = {},
  ): UnionExpr {
    return unionExpr([this, ...expressions], {
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
   * @param options.distinct - If `true`, uses INTERSECT DISTINCT. Otherwise uses INTERSECT ALL. Default is `true`.
   * @param options.dialect - The dialect used to parse the input expressions
   * @returns An Intersect expression
   */
  intersect (
    expressions: Array<string | Expression>,
    options: {
      distinct?: boolean;
      dialect?: DialectType;
      [key: string]: unknown;
    } = {},
  ): IntersectExpr {
    return intersectExpr([this, ...expressions], {
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
   * @param options.distinct - If `true`, uses EXCEPT DISTINCT. Otherwise uses EXCEPT ALL. Default is `true`.
   * @param options.dialect - The dialect used to parse the input expressions
   * @returns An Except expression
   */
  except (
    expressions: Array<string | Expression>,
    options: {
      distinct?: boolean;
      dialect?: DialectType;
      [key: string]: unknown;
    } = {},
  ): ExceptExpr {
    return exceptExpr([this, ...expressions], {
      ...options,
      distinct: options.distinct ?? true,
    });
  }
}

export class UDTFExpr extends DerivedTableExpr {
  key = ExpressionKey.UDTF;

  get selects (): Expression[] {
    const alias = this.args['alias'];
    // @ts-expect-error "Alias should contain 'columns'"
    return alias ? alias.columns : [];
  }
}

export type CacheExprArgs = { lazy?: Expression; options?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class CacheExpr extends Expression {
  key = ExpressionKey.CACHE;

  /**
   * Defines the arguments (properties and child expressions) for Cache expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    this: true,
    lazy: false,
    options: false,
    expression: false,
  };

  constructor (args: CacheExprArgs = {}) {
    super(args);
  }

  get lazy (): Expression {
    return this.args.lazy as Expression;
  }

  get options (): Expression[] {
    return (this.args.options || []) as Expression[];
  }
}

export type UncacheExprArgs = { exists?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class UncacheExpr extends Expression {
  key = ExpressionKey.UNCACHE;

  /**
   * Defines the arguments (properties and child expressions) for Uncache expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    this: true,
    exists: false,
  };

  constructor (args: UncacheExprArgs = {}) {
    super(args);
  }

  get exists (): Expression {
    return this.args.exists as Expression;
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

export type RefreshExprArgs = { kind: RefreshExprKind; [key: string]: unknown } & BaseExpressionArgs;

export class RefreshExpr extends Expression {
  key = ExpressionKey.REFRESH;

  /**
   * Defines the arguments (properties and child expressions) for Refresh expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    this: true,
    kind: true,
  };

  constructor (args: RefreshExprArgs) {
    super(args);
  }

  get kind (): RefreshExprKind | undefined {
    return this.args.kind as RefreshExprKind | undefined;
  }
}

export class DDLExpr extends Expression {
  key = ExpressionKey.DDL;

  /**
   * Returns a list of all the CTEs attached to this statement.
   *
   * @returns Array of CTE expressions
   */
  get ctes (): CTEExpr[] {
    const withExpr = this.args['with'] as WithExpr | undefined;
    return (withExpr?.expressions as CTEExpr[]) || [];
  }

  /**
   * If this statement contains a query (e.g. a CTAS), this returns the query's projections.
   *
   * @returns Array of Expression objects representing the SELECT clause projections
   */
  get selects (): Expression[] {
    const expr = this.expression;
    return (expr instanceof QueryExpr) ? expr.selects : [];
  }

  /**
   * If this statement contains a query (e.g. a CTAS), this returns the output
   * names of the query's projections.
   *
   * @returns Array of strings representing the names of the projected columns
   */
  get namedSelects (): string[] {
    const expr = this.expression;
    return (expr instanceof QueryExpr) ? expr.namedSelects : [];
  }
}

export class LockingStatementExpr extends Expression {
  key = ExpressionKey.LOCKING_STATEMENT;
  static argTypes = {
    this: true,
    expression: true,
  };
}

export class DMLExpr extends Expression {
  key = ExpressionKey.DML;

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
      [key: string]: unknown;
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

export type CreateExprArgs = { with?: Expression; kind: CreateExprKind; exists?: Expression; properties?: Expression[]; replace?: boolean; refresh?: Expression; unique?: boolean; indexes?: Expression[]; noSchemaBinding?: Expression; begin?: Expression; end?: Expression; clone?: Expression; concurrently?: Expression; clustered?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class CreateExpr extends DDLExpr {
  key = ExpressionKey.CREATE;

  /**
   * Defines the arguments (properties and child expressions) for Create expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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

  constructor (args: CreateExprArgs) {
    super(args);
  }

  get with (): Expression {
    return this.args['with'] as Expression;
  }

  get kind (): CreateExprKind | undefined {
    return this.args.kind as CreateExprKind | undefined;
  }

  get exists (): Expression {
    return this.args.exists as Expression;
  }

  get properties (): Expression[] {
    return (this.args.properties || []) as Expression[];
  }

  get replace (): boolean {
    return this.args.replace as boolean;
  }

  get refresh (): Expression {
    return this.args.refresh as Expression;
  }

  get unique (): boolean {
    return this.args.unique as boolean;
  }

  get indexes (): Expression[] {
    return (this.args.indexes || []) as Expression[];
  }

  get noSchemaBinding (): Expression {
    return this.args.noSchemaBinding as Expression;
  }

  get begin (): Expression {
    return this.args.begin as Expression;
  }

  get end (): Expression {
    return this.args.end as Expression;
  }

  get clone (): Expression {
    return this.args.clone as Expression;
  }

  get concurrently (): Expression {
    return this.args.concurrently as Expression;
  }

  get clustered (): Expression {
    return this.args.clustered as Expression;
  }
}

export type SequencePropertiesExprArgs = { increment?: Expression; minvalue?: string; maxvalue?: string; cache?: Expression; start?: Expression; owned?: Expression; options?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class SequencePropertiesExpr extends Expression {
  key = ExpressionKey.SEQUENCE_PROPERTIES;

  /**
   * Defines the arguments (properties and child expressions) for SequenceProperties expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    increment: false,
    minvalue: false,
    maxvalue: false,
    cache: false,
    start: false,
    owned: false,
    options: false,
  };

  constructor (args: SequencePropertiesExprArgs = {}) {
    super(args);
  }

  get increment (): Expression {
    return this.args.increment as Expression;
  }

  get minvalue (): string {
    return this.args.minvalue as string;
  }

  get maxvalue (): string {
    return this.args.maxvalue as string;
  }

  get cache (): Expression {
    return this.args.cache as Expression;
  }

  get start (): Expression {
    return this.args.start as Expression;
  }

  get owned (): Expression {
    return this.args.owned as Expression;
  }

  get options (): Expression[] {
    return (this.args.options || []) as Expression[];
  }
}

export type TruncateTableExprArgs = { isDatabase?: string; exists?: Expression; only?: Expression; cluster?: Expression; identity?: Expression; option?: Expression; partition?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TruncateTableExpr extends Expression {
  key = ExpressionKey.TRUNCATE_TABLE;

  /**
   * Defines the arguments (properties and child expressions) for TruncateTable expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    expressions: true,
    isDatabase: false,
    exists: false,
    only: false,
    cluster: false,
    identity: false,
    option: false,
    partition: false,
  };

  constructor (args: TruncateTableExprArgs = {}) {
    super(args);
  }

  get isDatabase (): string {
    return this.args.isDatabase as string;
  }

  get exists (): Expression {
    return this.args.exists as Expression;
  }

  get only (): Expression {
    return this.args.only as Expression;
  }

  get cluster (): Expression {
    return this.args.cluster as Expression;
  }

  get identity (): Expression {
    return this.args.identity as Expression;
  }

  get option (): Expression {
    return this.args.option as Expression;
  }

  get partition (): Expression {
    return this.args.partition as Expression;
  }
}

export type CloneExprArgs = { shallow?: Expression; copy?: unknown; [key: string]: unknown } & BaseExpressionArgs;

export class CloneExpr extends Expression {
  key = ExpressionKey.CLONE;

  /**
   * Defines the arguments (properties and child expressions) for Clone expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    this: true,
    shallow: false,
    copy: false,
  };

  constructor (args: CloneExprArgs = {}) {
    super(args);
  }

  get shallow (): Expression {
    return this.args.shallow as Expression;
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
export type DescribeExprArgs = { style?: Expression; kind?: DescribeExprKind; partition?: Expression; format?: string; asJson?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DescribeExpr extends Expression {
  key = ExpressionKey.DESCRIBE;

  /**
   * Defines the arguments (properties and child expressions) for Describe expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    style: false,
    kind: false,
    partition: false,
    format: false,
    asJson: false,
  };

  constructor (args: DescribeExprArgs = {}) {
    super(args);
  }

  get style (): Expression {
    return this.args.style as Expression;
  }

  get kind (): DescribeExprKind | undefined {
    return this.args.kind as DescribeExprKind | undefined;
  }

  get partition (): Expression {
    return this.args.partition as Expression;
  }

  get format (): string {
    return this.args.format as string;
  }

  get asJson (): Expression {
    return this.args.asJson as Expression;
  }
}

export type AttachExprArgs = { exists?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class AttachExpr extends Expression {
  key = ExpressionKey.ATTACH;

  /**
   * Defines the arguments (properties and child expressions) for Attach expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    exists: false,
  };

  constructor (args: AttachExprArgs = {}) {
    super(args);
  }

  get exists (): Expression {
    return this.args.exists as Expression;
  }
}

export type DetachExprArgs = { exists?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DetachExpr extends Expression {
  key = ExpressionKey.DETACH;

  /**
   * Defines the arguments (properties and child expressions) for Detach expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    exists: false,
  };

  constructor (args: DetachExprArgs = {}) {
    super(args);
  }

  get exists (): Expression {
    return this.args.exists as Expression;
  }
}

export type InstallExprArgs = { from?: Expression; force?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class InstallExpr extends Expression {
  key = ExpressionKey.INSTALL;

  /**
   * Defines the arguments (properties and child expressions) for Install expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    from: false,
    force: false,
  };

  constructor (args: InstallExprArgs = {}) {
    super(args);
  }

  get from (): Expression {
    return this.args.from as Expression;
  }

  get force (): Expression {
    return this.args.force as Expression;
  }
}

export type SummarizeExprArgs = { table?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class SummarizeExpr extends Expression {
  key = ExpressionKey.SUMMARIZE;

  /**
   * Defines the arguments (properties and child expressions) for Summarize expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    table: false,
  };

  constructor (args: SummarizeExprArgs = {}) {
    super(args);
  }

  get table (): Expression {
    return this.args.table as Expression;
  }
}

/**
 * Valid kind values for KILL statements
 */
export enum KillExprKind {
  CONNECTION = 'CONNECTION',
  QUERY = 'QUERY',
}
export type KillExprArgs = { kind?: KillExprKind; [key: string]: unknown } & BaseExpressionArgs;

export class KillExpr extends Expression {
  key = ExpressionKey.KILL;

  /**
   * Defines the arguments (properties and child expressions) for Kill expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: false,
  };

  constructor (args: KillExprArgs = {}) {
    super(args);
  }

  get kind (): KillExprKind | undefined {
    return this.args.kind as KillExprKind | undefined;
  }
}

export class PragmaExpr extends Expression {
  key = ExpressionKey.PRAGMA;
}

export class DeclareExpr extends Expression {
  key = ExpressionKey.DECLARE;
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
export type DeclareItemExprArgs = { kind?: DeclareItemExprKind; default?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DeclareItemExpr extends Expression {
  key = ExpressionKey.DECLARE_ITEM;

  /**
   * Defines the arguments (properties and child expressions) for DeclareItem expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: false,
    default: false,
  };

  constructor (args: DeclareItemExprArgs = {}) {
    super(args);
  }

  get kind (): DeclareItemExprKind | undefined {
    return this.args.kind as DeclareItemExprKind | undefined;
  }

  get default (): Expression {
    return this.args['default'] as Expression;
  }
}

export type SetExprArgs = { unset?: Expression; tag?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class SetExpr extends Expression {
  key = ExpressionKey.SET;

  /**
   * Defines the arguments (properties and child expressions) for Set expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unset: false,
    tag: false,
  };

  constructor (args: SetExprArgs = {}) {
    super(args);
  }

  get unset (): Expression {
    return this.args.unset as Expression;
  }

  get tag (): Expression {
    return this.args.tag as Expression;
  }
}

export type HeredocExprArgs = { tag?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class HeredocExpr extends Expression {
  key = ExpressionKey.HEREDOC;

  /**
   * Defines the arguments (properties and child expressions) for Heredoc expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    tag: false,
  };

  constructor (args: HeredocExprArgs = {}) {
    super(args);
  }

  get tag (): Expression {
    return this.args.tag as Expression;
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
export type SetItemExprArgs = { kind?: SetItemExprKind; collate?: string; global?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class SetItemExpr extends Expression {
  key = ExpressionKey.SET_ITEM;

  /**
   * Defines the arguments (properties and child expressions) for SetItem expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: false,
    collate: false,
    global: false,
  };

  constructor (args: SetItemExprArgs = {}) {
    super(args);
  }

  get kind (): SetItemExprKind | undefined {
    return this.args.kind as SetItemExprKind | undefined;
  }

  get collate (): string {
    return this.args.collate as string;
  }

  get global (): boolean {
    return this.args['global'] as boolean;
  }
}

export type QueryBandExprArgs = { scope?: Expression; update?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class QueryBandExpr extends Expression {
  key = ExpressionKey.QUERY_BAND;

  /**
   * Defines the arguments (properties and child expressions) for QueryBand expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    scope: false,
    update: false,
  };

  constructor (args: QueryBandExprArgs = {}) {
    super(args);
  }

  get scope (): Expression {
    return this.args.scope as Expression;
  }

  get update (): Expression {
    return this.args.update as Expression;
  }
}

export type ShowExprArgs = { history?: Expression; terse?: Expression; target?: Expression; offset?: boolean; startsWith?: Expression; limit?: number | Expression; from?: Expression; like?: Expression; where?: Expression; db?: string; scope?: Expression; scopeKind?: string; full?: Expression; mutex?: Expression; query?: Expression; channel?: Expression; global?: boolean; log?: Expression; position?: Expression; types?: Expression[]; privileges?: Expression[]; forTable?: Expression; forGroup?: Expression; forUser?: Expression; forRole?: Expression; intoOutfile?: Expression; json?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ShowExpr extends Expression {
  key = ExpressionKey.SHOW;

  /**
   * Defines the arguments (properties and child expressions) for Show expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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

  constructor (args: ShowExprArgs = {}) {
    super(args);
  }

  get history (): Expression {
    return this.args.history as Expression;
  }

  get terse (): Expression {
    return this.args.terse as Expression;
  }

  get target (): Expression {
    return this.args.target as Expression;
  }

  get offset (): boolean {
    return this.args.offset as boolean;
  }

  get startsWith (): Expression {
    return this.args.startsWith as Expression;
  }

  get limit (): number | Expression {
    return this.args.limit as number | Expression;
  }

  get from (): Expression {
    return this.args.from as Expression;
  }

  get like (): Expression {
    return this.args.like as Expression;
  }

  get where (): Expression {
    return this.args.where as Expression;
  }

  get db (): string {
    return this.args.db as string;
  }

  get scope (): Expression {
    return this.args.scope as Expression;
  }

  get scopeKind (): string {
    return this.args.scopeKind as string;
  }

  get full (): Expression {
    return this.args.full as Expression;
  }

  get mutex (): Expression {
    return this.args.mutex as Expression;
  }

  get query (): Expression {
    return this.args.query as Expression;
  }

  get channel (): Expression {
    return this.args.channel as Expression;
  }

  get global (): boolean {
    return this.args['global'] as boolean;
  }

  get log (): Expression {
    return this.args.log as Expression;
  }

  get position (): Expression {
    return this.args.position as Expression;
  }

  get types (): Expression[] {
    return (this.args.types || []) as Expression[];
  }

  get privileges (): Expression[] {
    return (this.args.privileges || []) as Expression[];
  }

  get forTable (): Expression {
    return this.args.forTable as Expression;
  }

  get forGroup (): Expression {
    return this.args.forGroup as Expression;
  }

  get forUser (): Expression {
    return this.args.forUser as Expression;
  }

  get forRole (): Expression {
    return this.args.forRole as Expression;
  }

  get intoOutfile (): Expression {
    return this.args.intoOutfile as Expression;
  }

  get json (): Expression {
    return this.args.json as Expression;
  }
}

export type UserDefinedFunctionExprArgs = { wrapped?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class UserDefinedFunctionExpr extends Expression {
  key = ExpressionKey.USER_DEFINED_FUNCTION;

  /**
   * Defines the arguments (properties and child expressions) for UserDefinedFunction expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    wrapped: false,
  };

  constructor (args: UserDefinedFunctionExprArgs = {}) {
    super(args);
  }

  get wrapped (): Expression {
    return this.args.wrapped as Expression;
  }
}

export type CharacterSetExprArgs = { default?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class CharacterSetExpr extends Expression {
  key = ExpressionKey.CHARACTER_SET;

  /**
   * Defines the arguments (properties and child expressions) for CharacterSet expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    default: false,
  };

  constructor (args: CharacterSetExprArgs = {}) {
    super(args);
  }

  get default (): Expression {
    return this.args['default'] as Expression;
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

export type RecursiveWithSearchExprArgs = { kind: RecursiveWithSearchExprKind; using?: string; [key: string]: unknown } & BaseExpressionArgs;

export class RecursiveWithSearchExpr extends Expression {
  key = ExpressionKey.RECURSIVE_WITH_SEARCH;

  /**
   * Defines the arguments (properties and child expressions) for RecursiveWithSearch expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: true,
    using: false,
  };

  constructor (args: RecursiveWithSearchExprArgs) {
    super(args);
  }

  get kind (): RecursiveWithSearchExprKind | undefined {
    return this.args.kind as RecursiveWithSearchExprKind | undefined;
  }

  get using (): string {
    return this.args.using as string;
  }
}

export type WithExprArgs = { recursive?: boolean; search?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class WithExpr extends Expression {
  key = ExpressionKey.WITH;

  /**
   * Defines the arguments (properties and child expressions) for With expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    recursive: false,
    search: false,
  };

  constructor (args: WithExprArgs = {}) {
    super(args);
  }

  get recursive (): boolean {
    return this.args.recursive as boolean;
  }

  get search (): Expression {
    return this.args.search as Expression;
  }
}

export class WithinGroupExpr extends Expression {
  key = ExpressionKey.WITHIN_GROUP;
}

export class ProjectionDefExpr extends Expression {
  key = ExpressionKey.PROJECTION_DEF;
}

export type TableAliasExprArgs = { columns?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class TableAliasExpr extends Expression {
  key = ExpressionKey.TABLE_ALIAS;

  /**
   * Defines the arguments (properties and child expressions) for TableAlias expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    columns: false,
  };

  constructor (args: TableAliasExprArgs = {}) {
    super(args);
  }

  get columns (): Expression[] {
    return (this.args.columns || []) as Expression[];
  }
}

export type ColumnPositionExprArgs = { position: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ColumnPositionExpr extends Expression {
  key = ExpressionKey.COLUMN_POSITION;

  /**
   * Defines the arguments (properties and child expressions) for ColumnPosition expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    position: true,
  };

  constructor (args: ColumnPositionExprArgs) {
    super(args);
  }

  get position (): Expression {
    return this.args.position as Expression;
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

export type ColumnDefExprArgs = { kind?: ColumnDefExprKind; constraints?: Expression[]; exists?: Expression; position?: Expression; default?: Expression; output?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ColumnDefExpr extends Expression {
  key = ExpressionKey.COLUMN_DEF;

  /**
   * Defines the arguments (properties and child expressions) for ColumnDef expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: false,
    constraints: false,
    exists: false,
    position: false,
    default: false,
    output: false,
  };

  constructor (args: ColumnDefExprArgs = {}) {
    super(args);
  }

  get kind (): ColumnDefExprKind | undefined {
    return this.args.kind as ColumnDefExprKind | undefined;
  }

  get constraints (): Expression[] {
    return (this.args.constraints || []) as Expression[];
  }

  get exists (): Expression {
    return this.args.exists as Expression;
  }

  get position (): Expression {
    return this.args.position as Expression;
  }

  get default (): Expression {
    return this.args['default'] as Expression;
  }

  get output (): Expression {
    return this.args.output as Expression;
  }
}

export type AlterColumnExprArgs = { dtype?: DataTypeExpr; collate?: string; using?: string; default?: Expression; drop?: Expression; comment?: string; allowNull?: Expression; visible?: Expression; renameTo?: string; [key: string]: unknown } & BaseExpressionArgs;

export class AlterColumnExpr extends Expression {
  key = ExpressionKey.ALTER_COLUMN;

  /**
   * Defines the arguments (properties and child expressions) for AlterColumn expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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

  constructor (args: AlterColumnExprArgs = {}) {
    super(args);
  }

  get dtype (): DataTypeExpr {
    return this.args.dtype as DataTypeExpr;
  }

  get collate (): string {
    return this.args.collate as string;
  }

  get using (): string {
    return this.args.using as string;
  }

  get default (): Expression {
    return this.args['default'] as Expression;
  }

  get drop (): Expression {
    return this.args.drop as Expression;
  }

  get comment (): string {
    return this.args.comment as string;
  }

  get allowNull (): Expression {
    return this.args.allowNull as Expression;
  }

  get visible (): Expression {
    return this.args.visible as Expression;
  }

  get renameTo (): string {
    return this.args.renameTo as string;
  }
}

export type AlterIndexExprArgs = { visible: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class AlterIndexExpr extends Expression {
  key = ExpressionKey.ALTER_INDEX;

  /**
   * Defines the arguments (properties and child expressions) for AlterIndex expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    visible: true,
  };

  constructor (args: AlterIndexExprArgs) {
    super(args);
  }

  get visible (): Expression {
    return this.args.visible as Expression;
  }
}

export class AlterDistStyleExpr extends Expression {
  key = ExpressionKey.ALTER_DIST_STYLE;
}

export type AlterSortKeyExprArgs = { compound?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class AlterSortKeyExpr extends Expression {
  key = ExpressionKey.ALTER_SORT_KEY;

  /**
   * Defines the arguments (properties and child expressions) for AlterSortKey expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    compound: false,
  };

  constructor (args: AlterSortKeyExprArgs = {}) {
    super(args);
  }

  get compound (): Expression {
    return this.args.compound as Expression;
  }
}

export type AlterSetExprArgs = { option?: Expression; tablespace?: Expression; accessMethod?: string; fileFormat?: string; copyOptions?: Expression[]; tag?: Expression; location?: Expression; serde?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class AlterSetExpr extends Expression {
  key = ExpressionKey.ALTER_SET;

  /**
   * Defines the arguments (properties and child expressions) for AlterSet expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    option: false,
    tablespace: false,
    accessMethod: false,
    fileFormat: false,
    copyOptions: false,
    tag: false,
    location: false,
    serde: false,
  };

  constructor (args: AlterSetExprArgs = {}) {
    super(args);
  }

  get option (): Expression {
    return this.args.option as Expression;
  }

  get tablespace (): Expression {
    return this.args.tablespace as Expression;
  }

  get accessMethod (): string {
    return this.args.accessMethod as string;
  }

  get fileFormat (): string {
    return this.args.fileFormat as string;
  }

  get copyOptions (): Expression[] {
    return (this.args.copyOptions || []) as Expression[];
  }

  get tag (): Expression {
    return this.args.tag as Expression;
  }

  get location (): Expression {
    return this.args.location as Expression;
  }

  get serde (): Expression {
    return this.args.serde as Expression;
  }
}

export type RenameColumnExprArgs = { to: Expression; exists?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class RenameColumnExpr extends Expression {
  key = ExpressionKey.RENAME_COLUMN;

  /**
   * Defines the arguments (properties and child expressions) for RenameColumn expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    to: true,
    exists: false,
  };

  constructor (args: RenameColumnExprArgs) {
    super(args);
  }

  get to (): Expression {
    return this.args.to as Expression;
  }

  get exists (): Expression {
    return this.args.exists as Expression;
  }
}

export class AlterRenameExpr extends Expression {
  key = ExpressionKey.ALTER_RENAME;
}

export class SwapTableExpr extends Expression {
  key = ExpressionKey.SWAP_TABLE;
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

export type CommentExprArgs = { kind: CommentExprKind; exists?: Expression; materialized?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class CommentExpr extends Expression {
  key = ExpressionKey.COMMENT;

  /**
   * Defines the arguments (properties and child expressions) for Comment expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: true,
    exists: false,
    materialized: false,
  };

  constructor (args: CommentExprArgs) {
    super(args);
  }

  get kind (): CommentExprKind | undefined {
    return this.args.kind as CommentExprKind | undefined;
  }

  get exists (): Expression {
    return this.args.exists as Expression;
  }

  get materialized (): boolean {
    return this.args.materialized as boolean;
  }
}

export type ComprehensionExprArgs = { position?: Expression; iterator: Expression; condition?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ComprehensionExpr extends Expression {
  key = ExpressionKey.COMPREHENSION;

  /**
   * Defines the arguments (properties and child expressions) for Comprehension expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    position: false,
    iterator: true,
    condition: false,
  };

  constructor (args: ComprehensionExprArgs) {
    super(args);
  }

  get position (): Expression {
    return this.args.position as Expression;
  }

  get iterator (): Expression {
    return this.args.iterator as Expression;
  }

  get condition (): Expression {
    return this.args.condition as Expression;
  }
}

export type MergeTreeTTLActionExprArgs = { delete?: Expression; recompress?: Expression[]; toDisk?: Expression; toVolume?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class MergeTreeTTLActionExpr extends Expression {
  key = ExpressionKey.MERGE_TREE_TTL_ACTION;

  /**
   * Defines the arguments (properties and child expressions) for MergeTreeTTLAction expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    delete: false,
    recompress: false,
    toDisk: false,
    toVolume: false,
  };

  constructor (args: MergeTreeTTLActionExprArgs = {}) {
    super(args);
  }

  get delete (): Expression {
    return this.args['delete'] as Expression;
  }

  get recompress (): Expression[] {
    return (this.args.recompress || []) as Expression[];
  }

  get toDisk (): Expression {
    return this.args.toDisk as Expression;
  }

  get toVolume (): Expression {
    return this.args.toVolume as Expression;
  }
}

export type MergeTreeTTLExprArgs = { where?: Expression; group?: Expression; aggregates?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class MergeTreeTTLExpr extends Expression {
  key = ExpressionKey.MERGE_TREE_TTL;

  /**
   * Defines the arguments (properties and child expressions) for MergeTreeTTL expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    where: false,
    group: false,
    aggregates: false,
  };

  constructor (args: MergeTreeTTLExprArgs = {}) {
    super(args);
  }

  get where (): Expression {
    return this.args.where as Expression;
  }

  get group (): Expression {
    return this.args.group as Expression;
  }

  get aggregates (): Expression[] {
    return (this.args.aggregates || []) as Expression[];
  }
}

export type IndexConstraintOptionExprArgs = { keyBlockSize?: number | Expression; using?: string; parser?: Expression; comment?: string; visible?: Expression; engineAttr?: string; secondaryEngineAttr?: string; [key: string]: unknown } & BaseExpressionArgs;

export class IndexConstraintOptionExpr extends Expression {
  key = ExpressionKey.INDEX_CONSTRAINT_OPTION;

  /**
   * Defines the arguments (properties and child expressions) for IndexConstraintOption expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    keyBlockSize: false,
    using: false,
    parser: false,
    comment: false,
    visible: false,
    engineAttr: false,
    secondaryEngineAttr: false,
  };

  constructor (args: IndexConstraintOptionExprArgs = {}) {
    super(args);
  }

  get keyBlockSize (): number | Expression {
    return this.args.keyBlockSize as number | Expression;
  }

  get using (): string {
    return this.args.using as string;
  }

  get parser (): Expression {
    return this.args.parser as Expression;
  }

  get comment (): string {
    return this.args.comment as string;
  }

  get visible (): Expression {
    return this.args.visible as Expression;
  }

  get engineAttr (): string {
    return this.args.engineAttr as string;
  }

  get secondaryEngineAttr (): string {
    return this.args.secondaryEngineAttr as string;
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

export type ColumnConstraintExprArgs = { kind: ColumnConstraintExprKind; [key: string]: unknown } & BaseExpressionArgs;

export class ColumnConstraintExpr extends Expression {
  key = ExpressionKey.COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for ColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: true,
  };

  constructor (args: ColumnConstraintExprArgs) {
    super(args);
  }

  get kind (): ColumnConstraintExprKind | undefined {
    return this.args.kind as ColumnConstraintExprKind | undefined;
  }
}

export class ColumnConstraintKindExpr extends Expression {
  key = ExpressionKey.COLUMN_CONSTRAINT_KIND;
}

export type WithOperatorExprArgs = { op: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class WithOperatorExpr extends Expression {
  key = ExpressionKey.WITH_OPERATOR;

  /**
   * Defines the arguments (properties and child expressions) for WithOperator expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    op: true,
  };

  constructor (args: WithOperatorExprArgs) {
    super(args);
  }

  get op (): Expression {
    return this.args.op as Expression;
  }
}

export class WatermarkColumnConstraintExpr extends Expression {
  key = ExpressionKey.WATERMARK_COLUMN_CONSTRAINT;
}

export class ConstraintExpr extends Expression {
  key = ExpressionKey.CONSTRAINT;
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
export type DropExprArgs = { kind?: DropExprKind; exists?: Expression; temporary?: boolean; materialized?: boolean; cascade?: Expression; constraints?: Expression[]; purge?: Expression; cluster?: Expression; concurrently?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DropExpr extends Expression {
  key = ExpressionKey.DROP;

  /**
   * Defines the arguments (properties and child expressions) for Drop expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: false,
    exists: false,
    temporary: false,
    materialized: false,
    cascade: false,
    constraints: false,
    purge: false,
    cluster: false,
    concurrently: false,
  };

  constructor (args: DropExprArgs = {}) {
    super(args);
  }

  get kind (): DropExprKind | undefined {
    return this.args.kind as DropExprKind | undefined;
  }

  get exists (): Expression {
    return this.args.exists as Expression;
  }

  get temporary (): boolean {
    return this.args.temporary as boolean;
  }

  get materialized (): boolean {
    return this.args.materialized as boolean;
  }

  get cascade (): Expression {
    return this.args.cascade as Expression;
  }

  get constraints (): Expression[] {
    return (this.args.constraints || []) as Expression[];
  }

  get purge (): Expression {
    return this.args.purge as Expression;
  }

  get cluster (): Expression {
    return this.args.cluster as Expression;
  }

  get concurrently (): Expression {
    return this.args.concurrently as Expression;
  }
}

export type ExportExprArgs = { connection?: Expression; options: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class ExportExpr extends Expression {
  key = ExpressionKey.EXPORT;

  /**
   * Defines the arguments (properties and child expressions) for Export expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    connection: false,
    options: true,
  };

  constructor (args: ExportExprArgs) {
    super(args);
  }

  get connection (): Expression {
    return this.args.connection as Expression;
  }

  get options (): Expression[] {
    return (this.args.options || []) as Expression[];
  }
}

export class FilterExpr extends Expression {
  key = ExpressionKey.FILTER;
}

export class CheckExpr extends Expression {
  key = ExpressionKey.CHECK;
}

export type ChangesExprArgs = { information: string; atBefore?: Expression; end?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ChangesExpr extends Expression {
  key = ExpressionKey.CHANGES;

  /**
   * Defines the arguments (properties and child expressions) for Changes expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    information: true,
    atBefore: false,
    end: false,
  };

  constructor (args: ChangesExprArgs) {
    super(args);
  }

  get information (): string {
    return this.args.information as string;
  }

  get atBefore (): Expression {
    return this.args.atBefore as Expression;
  }

  get end (): Expression {
    return this.args.end as Expression;
  }
}

export type ConnectExprArgs = { start?: Expression; connect: Expression; nocycle?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ConnectExpr extends Expression {
  key = ExpressionKey.CONNECT;

  /**
   * Defines the arguments (properties and child expressions) for Connect expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    start: false,
    connect: true,
    nocycle: false,
  };

  constructor (args: ConnectExprArgs) {
    super(args);
  }

  get start (): Expression {
    return this.args.start as Expression;
  }

  get connect (): Expression {
    return this.args.connect as Expression;
  }

  get nocycle (): Expression {
    return this.args.nocycle as Expression;
  }
}

export class CopyParameterExpr extends Expression {
  key = ExpressionKey.COPY_PARAMETER;
}

export type CredentialsExprArgs = { credentials?: Expression[]; encryption?: Expression; storage?: Expression; iamRole?: Expression; region?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class CredentialsExpr extends Expression {
  key = ExpressionKey.CREDENTIALS;

  /**
   * Defines the arguments (properties and child expressions) for Credentials expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    credentials: false,
    encryption: false,
    storage: false,
    iamRole: false,
    region: false,
  };

  constructor (args: CredentialsExprArgs = {}) {
    super(args);
  }

  get credentials (): Expression[] {
    return (this.args.credentials || []) as Expression[];
  }

  get encryption (): Expression {
    return this.args.encryption as Expression;
  }

  get storage (): Expression {
    return this.args.storage as Expression;
  }

  get iamRole (): Expression {
    return this.args.iamRole as Expression;
  }

  get region (): Expression {
    return this.args.region as Expression;
  }
}

export class PriorExpr extends Expression {
  key = ExpressionKey.PRIOR;
}

export type DirectoryExprArgs = { local?: Expression; rowFormat?: string; [key: string]: unknown } & BaseExpressionArgs;

export class DirectoryExpr extends Expression {
  key = ExpressionKey.DIRECTORY;

  /**
   * Defines the arguments (properties and child expressions) for Directory expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    local: false,
    rowFormat: false,
  };

  constructor (args: DirectoryExprArgs = {}) {
    super(args);
  }

  get local (): Expression {
    return this.args.local as Expression;
  }

  get rowFormat (): string {
    return this.args.rowFormat as string;
  }
}

export class DirectoryStageExpr extends Expression {
  key = ExpressionKey.DIRECTORY_STAGE;
}

export type ForeignKeyExprArgs = { reference?: Expression; delete?: Expression; update?: Expression; options?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class ForeignKeyExpr extends Expression {
  key = ExpressionKey.FOREIGN_KEY;

  /**
   * Defines the arguments (properties and child expressions) for ForeignKey expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    reference: false,
    delete: false,
    update: false,
    options: false,
  };

  constructor (args: ForeignKeyExprArgs = {}) {
    super(args);
  }

  get reference (): Expression {
    return this.args.reference as Expression;
  }

  get delete (): Expression {
    return this.args['delete'] as Expression;
  }

  get update (): Expression {
    return this.args.update as Expression;
  }

  get options (): Expression[] {
    return (this.args.options || []) as Expression[];
  }
}

export class ColumnPrefixExpr extends Expression {
  key = ExpressionKey.COLUMN_PREFIX;
}

export type PrimaryKeyExprArgs = { options?: Expression[]; include?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class PrimaryKeyExpr extends Expression {
  key = ExpressionKey.PRIMARY_KEY;

  /**
   * Defines the arguments (properties and child expressions) for PrimaryKey expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    options: false,
    include: false,
  };

  constructor (args: PrimaryKeyExprArgs = {}) {
    super(args);
  }

  get options (): Expression[] {
    return (this.args.options || []) as Expression[];
  }

  get include (): Expression {
    return this.args.include as Expression;
  }
}

export type IntoExprArgs = { temporary?: boolean; unlogged?: Expression; bulkCollect?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class IntoExpr extends Expression {
  key = ExpressionKey.INTO;

  /**
   * Defines the arguments (properties and child expressions) for Into expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    temporary: false,
    unlogged: false,
    bulkCollect: false,
  };

  constructor (args: IntoExprArgs = {}) {
    super(args);
  }

  get temporary (): boolean {
    return this.args.temporary as boolean;
  }

  get unlogged (): Expression {
    return this.args.unlogged as Expression;
  }

  get bulkCollect (): Expression {
    return this.args.bulkCollect as Expression;
  }
}

export class FromExpr extends Expression {
  key = ExpressionKey.FROM;
}

export class HavingExpr extends Expression {
  key = ExpressionKey.HAVING;
}

export class HintExpr extends Expression {
  key = ExpressionKey.HINT;
}

export class JoinHintExpr extends Expression {
  key = ExpressionKey.JOIN_HINT;
}

export type IdentifierExprArgs = { quoted?: boolean; global?: boolean; temporary?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class IdentifierExpr extends Expression {
  key = ExpressionKey.IDENTIFIER;

  /**
   * Defines the arguments (properties and child expressions) for Identifier expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    quoted: false,
    global: false,
    temporary: false,
  };

  constructor (args: IdentifierExprArgs = {}) {
    super(args);
  }

  get quoted (): boolean {
    return this.args.quoted as boolean;
  }

  get global (): boolean {
    return this.args['global'] as boolean;
  }

  get temporary (): boolean {
    return this.args.temporary as boolean;
  }
}

export class OpclassExpr extends Expression {
  key = ExpressionKey.OPCLASS;
}

export type IndexExprArgs = { table?: Expression; unique?: boolean; primary?: boolean; amp?: Expression; params?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class IndexExpr extends Expression {
  key = ExpressionKey.INDEX;

  /**
   * Defines the arguments (properties and child expressions) for Index expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    table: false,
    unique: false,
    primary: false,
    amp: false,
    params: false,
  };

  constructor (args: IndexExprArgs = {}) {
    super(args);
  }

  get table (): Expression {
    return this.args.table as Expression;
  }

  get unique (): boolean {
    return this.args.unique as boolean;
  }

  get primary (): boolean {
    return this.args.primary as boolean;
  }

  get amp (): Expression {
    return this.args.amp as Expression;
  }

  get params (): Expression[] {
    return (this.args.params || []) as Expression[];
  }
}

export type IndexParametersExprArgs = { using?: string; include?: Expression; columns?: Expression[]; withStorage?: Expression; partitionBy?: Expression; tablespace?: Expression; where?: Expression; on?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class IndexParametersExpr extends Expression {
  key = ExpressionKey.INDEX_PARAMETERS;

  /**
   * Defines the arguments (properties and child expressions) for IndexParameters expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    using: false,
    include: false,
    columns: false,
    withStorage: false,
    partitionBy: false,
    tablespace: false,
    where: false,
    on: false,
  };

  constructor (args: IndexParametersExprArgs = {}) {
    super(args);
  }

  get using (): string {
    return this.args.using as string;
  }

  get include (): Expression {
    return this.args.include as Expression;
  }

  get columns (): Expression[] {
    return (this.args.columns || []) as Expression[];
  }

  get withStorage (): Expression {
    return this.args.withStorage as Expression;
  }

  get partitionBy (): Expression {
    return this.args.partitionBy as Expression;
  }

  get tablespace (): Expression {
    return this.args.tablespace as Expression;
  }

  get where (): Expression {
    return this.args.where as Expression;
  }

  get on (): Expression {
    return this.args.on as Expression;
  }
}

export type ConditionalInsertExprArgs = { else?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ConditionalInsertExpr extends Expression {
  key = ExpressionKey.CONDITIONAL_INSERT;

  /**
   * Defines the arguments (properties and child expressions) for ConditionalInsert expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    else: false,
  };

  constructor (args: ConditionalInsertExprArgs = {}) {
    super(args);
  }

  get else (): Expression {
    return this.args['else'] as Expression;
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

export type MultitableInsertsExprArgs = { kind: MultitableInsertsExprKind; source: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class MultitableInsertsExpr extends Expression {
  key = ExpressionKey.MULTITABLE_INSERTS;

  /**
   * Defines the arguments (properties and child expressions) for MultitableInserts expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: true,
    source: true,
  };

  constructor (args: MultitableInsertsExprArgs) {
    super(args);
  }

  get kind (): MultitableInsertsExprKind | undefined {
    return this.args.kind as MultitableInsertsExprKind | undefined;
  }

  get source (): Expression {
    return this.args.source as Expression;
  }
}

export type OnConflictExprArgs = { duplicate?: Expression; action?: Expression; conflictKeys?: Expression[]; indexPredicate?: Expression; constraint?: Expression; where?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class OnConflictExpr extends Expression {
  key = ExpressionKey.ON_CONFLICT;

  /**
   * Defines the arguments (properties and child expressions) for OnConflict expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    duplicate: false,
    action: false,
    conflictKeys: false,
    indexPredicate: false,
    constraint: false,
    where: false,
  };

  constructor (args: OnConflictExprArgs = {}) {
    super(args);
  }

  get duplicate (): Expression {
    return this.args.duplicate as Expression;
  }

  get action (): Expression {
    return this.args.action as Expression;
  }

  get conflictKeys (): Expression[] {
    return (this.args.conflictKeys || []) as Expression[];
  }

  get indexPredicate (): Expression {
    return this.args.indexPredicate as Expression;
  }

  get constraint (): Expression {
    return this.args.constraint as Expression;
  }

  get where (): Expression {
    return this.args.where as Expression;
  }
}

export type OnConditionExprArgs = { error?: Expression; empty?: Expression; null?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class OnConditionExpr extends Expression {
  key = ExpressionKey.ON_CONDITION;

  /**
   * Defines the arguments (properties and child expressions) for OnCondition expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    error: false,
    empty: false,
    null: false,
  };

  constructor (args: OnConditionExprArgs = {}) {
    super(args);
  }

  get error (): Expression {
    return this.args.error as Expression;
  }

  get empty (): Expression {
    return this.args.empty as Expression;
  }

  get null (): Expression {
    return this.args.null as Expression;
  }
}

export type ReturningExprArgs = { into?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ReturningExpr extends Expression {
  key = ExpressionKey.RETURNING;

  /**
   * Defines the arguments (properties and child expressions) for Returning expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    into: false,
  };

  constructor (args: ReturningExprArgs = {}) {
    super(args);
  }

  get into (): Expression {
    return this.args.into as Expression;
  }
}

export class IntroducerExpr extends Expression {
  key = ExpressionKey.INTRODUCER;
}

export class NationalExpr extends Expression {
  key = ExpressionKey.NATIONAL;
}

export type LoadDataExprArgs = { local?: Expression; overwrite?: Expression; inpath: Expression; partition?: Expression; inputFormat?: string; serde?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class LoadDataExpr extends Expression {
  key = ExpressionKey.LOAD_DATA;

  /**
   * Defines the arguments (properties and child expressions) for LoadData expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    local: false,
    overwrite: false,
    inpath: true,
    partition: false,
    inputFormat: false,
    serde: false,
  };

  constructor (args: LoadDataExprArgs) {
    super(args);
  }

  get local (): Expression {
    return this.args.local as Expression;
  }

  get overwrite (): Expression {
    return this.args.overwrite as Expression;
  }

  get inpath (): Expression {
    return this.args.inpath as Expression;
  }

  get partition (): Expression {
    return this.args.partition as Expression;
  }

  get inputFormat (): string {
    return this.args.inputFormat as string;
  }

  get serde (): Expression {
    return this.args.serde as Expression;
  }
}

export type PartitionExprArgs = { subpartition?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class PartitionExpr extends Expression {
  key = ExpressionKey.PARTITION;

  /**
   * Defines the arguments (properties and child expressions) for Partition expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    subpartition: false,
  };

  constructor (args: PartitionExprArgs = {}) {
    super(args);
  }

  get subpartition (): Expression {
    return this.args.subpartition as Expression;
  }
}

export class PartitionRangeExpr extends Expression {
  key = ExpressionKey.PARTITION_RANGE;
}

export class PartitionIdExpr extends Expression {
  key = ExpressionKey.PARTITION_ID;
}

export type FetchExprArgs = { direction?: Expression; count?: Expression; limitOptions?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class FetchExpr extends Expression {
  key = ExpressionKey.FETCH;

  /**
   * Defines the arguments (properties and child expressions) for Fetch expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    direction: false,
    count: false,
    limitOptions: false,
  };

  constructor (args: FetchExprArgs = {}) {
    super(args);
  }

  get direction (): Expression {
    return this.args.direction as Expression;
  }

  get count (): Expression {
    return this.args.count as Expression;
  }

  get limitOptions (): Expression[] {
    return (this.args.limitOptions || []) as Expression[];
  }
}

/**
 * Valid kind values for GRANT statements
 */
export enum GrantExprKind {
  GRANT = 'GRANT',
  REVOKE = 'REVOKE',
}
export type GrantExprArgs = { privileges: Expression[]; kind?: GrantExprKind; securable: Expression; principals: Expression[]; grantOption?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class GrantExpr extends Expression {
  key = ExpressionKey.GRANT;

  /**
   * Defines the arguments (properties and child expressions) for Grant expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    privileges: true,
    kind: false,
    securable: true,
    principals: true,
    grantOption: false,
  };

  constructor (args: GrantExprArgs) {
    super(args);
  }

  get privileges (): Expression[] {
    return (this.args.privileges || []) as Expression[];
  }

  get kind (): GrantExprKind | undefined {
    return this.args.kind as GrantExprKind | undefined;
  }

  get securable (): Expression {
    return this.args.securable as Expression;
  }

  get principals (): Expression[] {
    return (this.args.principals || []) as Expression[];
  }

  get grantOption (): Expression {
    return this.args.grantOption as Expression;
  }
}

export type RevokeExprArgs = { cascade?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class RevokeExpr extends Expression {
  key = ExpressionKey.REVOKE;

  /**
   * Defines the arguments (properties and child expressions) for Revoke expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    cascade: false,
  };

  constructor (args: RevokeExprArgs = {}) {
    super(args);
  }

  get cascade (): Expression {
    return this.args.cascade as Expression;
  }
}

export type GroupExprArgs = { groupingSets?: Expression[]; cube?: Expression; rollup?: Expression; totals?: Expression[]; all?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class GroupExpr extends Expression {
  key = ExpressionKey.GROUP;

  /**
   * Defines the arguments (properties and child expressions) for Group expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    groupingSets: false,
    cube: false,
    rollup: false,
    totals: false,
    all: false,
  };

  constructor (args: GroupExprArgs = {}) {
    super(args);
  }

  get groupingSets (): Expression[] {
    return (this.args.groupingSets || []) as Expression[];
  }

  get cube (): Expression {
    return this.args.cube as Expression;
  }

  get rollup (): Expression {
    return this.args.rollup as Expression;
  }

  get totals (): Expression[] {
    return (this.args.totals || []) as Expression[];
  }

  get all (): Expression {
    return this.args.all as Expression;
  }
}

export class CubeExpr extends Expression {
  key = ExpressionKey.CUBE;
}

export class RollupExpr extends Expression {
  key = ExpressionKey.ROLLUP;
}

export class GroupingSetsExpr extends Expression {
  key = ExpressionKey.GROUPING_SETS;
}

export type LambdaExprArgs = { colon?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class LambdaExpr extends Expression {
  key = ExpressionKey.LAMBDA;

  /**
   * Defines the arguments (properties and child expressions) for Lambda expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    colon: false,
  };

  constructor (args: LambdaExprArgs = {}) {
    super(args);
  }

  get colon (): Expression {
    return this.args.colon as Expression;
  }
}

export type LimitExprArgs = { offset?: boolean; limitOptions?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class LimitExpr extends Expression {
  key = ExpressionKey.LIMIT;

  /**
   * Defines the arguments (properties and child expressions) for Limit expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    offset: false,
    limitOptions: false,
  };

  constructor (args: LimitExprArgs = {}) {
    super(args);
  }

  get offset (): boolean {
    return this.args.offset as boolean;
  }

  get limitOptions (): Expression[] {
    return (this.args.limitOptions || []) as Expression[];
  }
}

export type LimitOptionsExprArgs = { percent?: Expression; rows?: Expression[]; withTies?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class LimitOptionsExpr extends Expression {
  key = ExpressionKey.LIMIT_OPTIONS;

  /**
   * Defines the arguments (properties and child expressions) for LimitOptions expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    percent: false,
    rows: false,
    withTies: false,
  };

  constructor (args: LimitOptionsExprArgs = {}) {
    super(args);
  }

  get percent (): Expression {
    return this.args.percent as Expression;
  }

  get rows (): Expression[] {
    return (this.args.rows || []) as Expression[];
  }

  get withTies (): Expression[] {
    return (this.args.withTies || []) as Expression[];
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
export type JoinExprArgs = { on?: Expression; side?: Expression; kind?: JoinExprKind; using?: string; method?: string; global?: boolean; hint?: Expression; matchCondition?: Expression; directed?: Expression; pivots?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class JoinExpr extends Expression {
  key = ExpressionKey.JOIN;

  /**
   * Defines the arguments (properties and child expressions) for Join expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    on: false,
    side: false,
    kind: false,
    using: false,
    method: false,
    global: false,
    hint: false,
    matchCondition: false,
    directed: false,
    pivots: false,
  };

  constructor (args: JoinExprArgs = {}) {
    super(args);
  }

  get on (): Expression {
    return this.args.on as Expression;
  }

  get side (): Expression {
    return this.args.side as Expression;
  }

  get kind (): JoinExprKind | undefined {
    return this.args.kind as JoinExprKind | undefined;
  }

  get using (): string {
    return this.args.using as string;
  }

  get method (): string {
    return this.args.method as string;
  }

  get global (): boolean {
    return this.args['global'] as boolean;
  }

  get hint (): Expression {
    return this.args.hint as Expression;
  }

  get matchCondition (): Expression {
    return this.args.matchCondition as Expression;
  }

  get directed (): Expression {
    return this.args.directed as Expression;
  }

  get pivots (): Expression[] {
    return (this.args.pivots || []) as Expression[];
  }
}

export type MatchRecognizeMeasureExprArgs = { windowFrame?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class MatchRecognizeMeasureExpr extends Expression {
  key = ExpressionKey.MATCH_RECOGNIZE_MEASURE;

  /**
   * Defines the arguments (properties and child expressions) for MatchRecognizeMeasure expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    windowFrame: false,
  };

  constructor (args: MatchRecognizeMeasureExprArgs = {}) {
    super(args);
  }

  get windowFrame (): Expression {
    return this.args.windowFrame as Expression;
  }
}

export type MatchRecognizeExprArgs = { partitionBy?: Expression; order?: Expression; measures?: Expression[]; rows?: Expression[]; after?: Expression; pattern?: Expression; define?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class MatchRecognizeExpr extends Expression {
  key = ExpressionKey.MATCH_RECOGNIZE;

  /**
   * Defines the arguments (properties and child expressions) for MatchRecognize expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    partitionBy: false,
    order: false,
    measures: false,
    rows: false,
    after: false,
    pattern: false,
    define: false,
  };

  constructor (args: MatchRecognizeExprArgs = {}) {
    super(args);
  }

  get partitionBy (): Expression {
    return this.args.partitionBy as Expression;
  }

  get order (): Expression {
    return this.args.order as Expression;
  }

  get measures (): Expression[] {
    return (this.args.measures || []) as Expression[];
  }

  get rows (): Expression[] {
    return (this.args.rows || []) as Expression[];
  }

  get after (): Expression {
    return this.args.after as Expression;
  }

  get pattern (): Expression {
    return this.args.pattern as Expression;
  }

  get define (): Expression {
    return this.args.define as Expression;
  }
}

export class FinalExpr extends Expression {
  key = ExpressionKey.FINAL;
}

export class OffsetExpr extends Expression {
  key = ExpressionKey.OFFSET;
}

export type OrderExprArgs = { siblings?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class OrderExpr extends Expression {
  key = ExpressionKey.ORDER;

  /**
   * Defines the arguments (properties and child expressions) for Order expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    siblings: false,
  };

  constructor (args: OrderExprArgs = {}) {
    super(args);
  }

  get siblings (): Expression[] {
    return (this.args.siblings || []) as Expression[];
  }
}

export type WithFillExprArgs = { from?: Expression; to?: Expression; step?: Expression; interpolate?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class WithFillExpr extends Expression {
  key = ExpressionKey.WITH_FILL;

  /**
   * Defines the arguments (properties and child expressions) for WithFill expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    from: false,
    to: false,
    step: false,
    interpolate: false,
  };

  constructor (args: WithFillExprArgs = {}) {
    super(args);
  }

  get from (): Expression {
    return this.args.from as Expression;
  }

  get to (): Expression {
    return this.args.to as Expression;
  }

  get step (): Expression {
    return this.args.step as Expression;
  }

  get interpolate (): Expression {
    return this.args.interpolate as Expression;
  }
}

export type OrderedExprArgs = { desc?: Expression; nullsFirst: Expression; withFill?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class OrderedExpr extends Expression {
  key = ExpressionKey.ORDERED;

  /**
   * Defines the arguments (properties and child expressions) for Ordered expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    desc: false,
    nullsFirst: true,
    withFill: false,
  };

  constructor (args: OrderedExprArgs) {
    super(args);
  }

  get desc (): Expression {
    return this.args.desc as Expression;
  }

  get nullsFirst (): Expression {
    return this.args.nullsFirst as Expression;
  }

  get withFill (): Expression {
    return this.args.withFill as Expression;
  }
}

export type PropertyExprArgs = { value: string; [key: string]: unknown } & BaseExpressionArgs;

export class PropertyExpr extends Expression {
  key = ExpressionKey.PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for Property expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: true,
  };

  constructor (args: PropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }
}

export class GrantPrivilegeExpr extends Expression {
  key = ExpressionKey.GRANT_PRIVILEGE;
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
export type GrantPrincipalExprArgs = { kind?: GrantPrincipalExprKind; [key: string]: unknown } & BaseExpressionArgs;

export class GrantPrincipalExpr extends Expression {
  key = ExpressionKey.GRANT_PRINCIPAL;

  /**
   * Defines the arguments (properties and child expressions) for GrantPrincipal expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: false,
  };

  constructor (args: GrantPrincipalExprArgs = {}) {
    super(args);
  }

  get kind (): GrantPrincipalExprKind | undefined {
    return this.args.kind as GrantPrincipalExprKind | undefined;
  }
}

export class AllowedValuesPropertyExpr extends Expression {
  key = ExpressionKey.ALLOWED_VALUES_PROPERTY;
}

export type PartitionByRangePropertyDynamicExprArgs = { start: Expression; end: Expression; every: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class PartitionByRangePropertyDynamicExpr extends Expression {
  key = ExpressionKey.PARTITION_BY_RANGE_PROPERTY_DYNAMIC;

  /**
   * Defines the arguments (properties and child expressions) for PartitionByRangePropertyDynamic expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    start: true,
    end: true,
    every: true,
  };

  constructor (args: PartitionByRangePropertyDynamicExprArgs) {
    super(args);
  }

  get start (): Expression {
    return this.args.start as Expression;
  }

  get end (): Expression {
    return this.args.end as Expression;
  }

  get every (): Expression {
    return this.args.every as Expression;
  }
}

export type RollupIndexExprArgs = { fromIndex?: Expression; properties?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class RollupIndexExpr extends Expression {
  key = ExpressionKey.ROLLUP_INDEX;

  /**
   * Defines the arguments (properties and child expressions) for RollupIndex expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    fromIndex: false,
    properties: false,
  };

  constructor (args: RollupIndexExprArgs = {}) {
    super(args);
  }

  get fromIndex (): Expression {
    return this.args.fromIndex as Expression;
  }

  get properties (): Expression[] {
    return (this.args.properties || []) as Expression[];
  }
}

export class PartitionListExpr extends Expression {
  key = ExpressionKey.PARTITION_LIST;
}

export type PartitionBoundSpecExprArgs = { fromExpressions?: Expression[]; toExpressions?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class PartitionBoundSpecExpr extends Expression {
  key = ExpressionKey.PARTITION_BOUND_SPEC;

  /**
   * Defines the arguments (properties and child expressions) for PartitionBoundSpec expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    fromExpressions: false,
    toExpressions: false,
  };

  constructor (args: PartitionBoundSpecExprArgs = {}) {
    super(args);
  }

  get fromExpressions (): Expression[] {
    return (this.args.fromExpressions || []) as Expression[];
  }

  get toExpressions (): Expression[] {
    return (this.args.toExpressions || []) as Expression[];
  }
}

export type QueryTransformExprArgs = { commandScript: Expression; schema?: Expression; rowFormatBefore?: string; recordWriter?: Expression; rowFormatAfter?: string; recordReader?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class QueryTransformExpr extends Expression {
  key = ExpressionKey.QUERY_TRANSFORM;

  /**
   * Defines the arguments (properties and child expressions) for QueryTransform expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    commandScript: true,
    schema: false,
    rowFormatBefore: false,
    recordWriter: false,
    rowFormatAfter: false,
    recordReader: false,
  };

  constructor (args: QueryTransformExprArgs) {
    super(args);
  }

  get commandScript (): Expression {
    return this.args.commandScript as Expression;
  }

  get schema (): Expression {
    return this.args.schema as Expression;
  }

  get rowFormatBefore (): string {
    return this.args.rowFormatBefore as string;
  }

  get recordWriter (): Expression {
    return this.args.recordWriter as Expression;
  }

  get rowFormatAfter (): string {
    return this.args.rowFormatAfter as string;
  }

  get recordReader (): Expression {
    return this.args.recordReader as Expression;
  }
}

export type SemanticViewExprArgs = { metrics?: Expression[]; dimensions?: Expression[]; facts?: Expression[]; where?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class SemanticViewExpr extends Expression {
  key = ExpressionKey.SEMANTIC_VIEW;

  /**
   * Defines the arguments (properties and child expressions) for SemanticView expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    metrics: false,
    dimensions: false,
    facts: false,
    where: false,
  };

  constructor (args: SemanticViewExprArgs = {}) {
    super(args);
  }

  get metrics (): Expression[] {
    return (this.args.metrics || []) as Expression[];
  }

  get dimensions (): Expression[] {
    return (this.args.dimensions || []) as Expression[];
  }

  get facts (): Expression[] {
    return (this.args.facts || []) as Expression[];
  }

  get where (): Expression {
    return this.args.where as Expression;
  }
}

export class PropertiesExpr extends Expression {
  key = ExpressionKey.PROPERTIES;
}

export class LocationExpr extends Expression {
  key = ExpressionKey.LOCATION;
}

export class QualifyExpr extends Expression {
  key = ExpressionKey.QUALIFY;
}

export type InputOutputFormatExprArgs = { inputFormat?: string; outputFormat?: string; [key: string]: unknown } & BaseExpressionArgs;

export class InputOutputFormatExpr extends Expression {
  key = ExpressionKey.INPUT_OUTPUT_FORMAT;

  /**
   * Defines the arguments (properties and child expressions) for InputOutputFormat expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    inputFormat: false,
    outputFormat: false,
  };

  constructor (args: InputOutputFormatExprArgs = {}) {
    super(args);
  }

  get inputFormat (): string {
    return this.args.inputFormat as string;
  }

  get outputFormat (): string {
    return this.args.outputFormat as string;
  }
}

export class ReturnExpr extends Expression {
  key = ExpressionKey.RETURN;
}

export type ReferenceExprArgs = { options?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class ReferenceExpr extends Expression {
  key = ExpressionKey.REFERENCE;

  /**
   * Defines the arguments (properties and child expressions) for Reference expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    options: false,
  };

  constructor (args: ReferenceExprArgs = {}) {
    super(args);
  }

  get options (): Expression[] {
    return (this.args.options || []) as Expression[];
  }
}

export class TupleExpr extends Expression {
  key = ExpressionKey.TUPLE;
}

export class QueryOptionExpr extends Expression {
  key = ExpressionKey.QUERY_OPTION;
}

export class WithTableHintExpr extends Expression {
  key = ExpressionKey.WITH_TABLE_HINT;
}

export type IndexTableHintExprArgs = { target?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class IndexTableHintExpr extends Expression {
  key = ExpressionKey.INDEX_TABLE_HINT;

  /**
   * Defines the arguments (properties and child expressions) for IndexTableHint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    target: false,
  };

  constructor (args: IndexTableHintExprArgs = {}) {
    super(args);
  }

  get target (): Expression {
    return this.args.target as Expression;
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

export type HistoricalDataExprArgs = { kind: HistoricalDataExprKind; [key: string]: unknown } & BaseExpressionArgs;

export class HistoricalDataExpr extends Expression {
  key = ExpressionKey.HISTORICAL_DATA;

  /**
   * Defines the arguments (properties and child expressions) for HistoricalData expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: true,
  };

  constructor (args: HistoricalDataExprArgs) {
    super(args);
  }

  get kind (): HistoricalDataExprKind | undefined {
    return this.args.kind as HistoricalDataExprKind | undefined;
  }
}

export type PutExprArgs = { target: Expression; properties?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class PutExpr extends Expression {
  key = ExpressionKey.PUT;

  /**
   * Defines the arguments (properties and child expressions) for Put expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    target: true,
    properties: false,
  };

  constructor (args: PutExprArgs) {
    super(args);
  }

  get target (): Expression {
    return this.args.target as Expression;
  }

  get properties (): Expression[] {
    return (this.args.properties || []) as Expression[];
  }
}

export type GetExprArgs = { target: Expression; properties?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class GetExpr extends Expression {
  key = ExpressionKey.GET;

  /**
   * Defines the arguments (properties and child expressions) for Get expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    target: true,
    properties: false,
  };

  constructor (args: GetExprArgs) {
    super(args);
  }

  get target (): Expression {
    return this.args.target as Expression;
  }

  get properties (): Expression[] {
    return (this.args.properties || []) as Expression[];
  }
}

export type TableExprArgs = { db?: string; catalog?: string; laterals?: Expression[]; joins?: Expression[]; pivots?: Expression[]; hints?: Expression[]; systemTime?: Expression; version?: Expression; format?: string; pattern?: Expression; ordinality?: boolean; when?: Expression; only?: Expression; partition?: Expression; changes?: Expression[]; rowsFrom?: number | Expression; sample?: number | Expression; indexed?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TableExpr extends Expression {
  key = ExpressionKey.TABLE;

  /**
   * Defines the arguments (properties and child expressions) for Table expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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

  constructor (args: TableExprArgs = {}) {
    super(args);
  }

  get db (): string {
    return this.args.db as string;
  }

  get catalog (): string {
    return this.args.catalog as string;
  }

  get laterals (): Expression[] {
    return (this.args.laterals || []) as Expression[];
  }

  get joins (): Expression[] {
    return (this.args.joins || []) as Expression[];
  }

  get pivots (): Expression[] {
    return (this.args.pivots || []) as Expression[];
  }

  get hints (): Expression[] {
    return (this.args.hints || []) as Expression[];
  }

  get systemTime (): Expression {
    return this.args.systemTime as Expression;
  }

  get version (): Expression {
    return this.args.version as Expression;
  }

  get format (): string {
    return this.args.format as string;
  }

  get pattern (): Expression {
    return this.args.pattern as Expression;
  }

  get ordinality (): boolean {
    return this.args.ordinality as boolean;
  }

  get when (): Expression {
    return this.args.when as Expression;
  }

  get only (): Expression {
    return this.args.only as Expression;
  }

  get partition (): Expression {
    return this.args.partition as Expression;
  }

  get changes (): Expression[] {
    return (this.args.changes || []) as Expression[];
  }

  get rowsFrom (): number | Expression {
    return this.args.rowsFrom as number | Expression;
  }

  get sample (): number | Expression {
    return this.args.sample as number | Expression;
  }

  get indexed (): Expression {
    return this.args.indexed as Expression;
  }
}

export class VarExpr extends Expression {
  key = ExpressionKey.VAR;
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

export type VersionExprArgs = { kind: VersionExprKind; [key: string]: unknown } & BaseExpressionArgs;

export class VersionExpr extends Expression {
  key = ExpressionKey.VERSION;

  /**
   * Defines the arguments (properties and child expressions) for Version expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: true,
  };

  constructor (args: VersionExprArgs) {
    super(args);
  }

  get kind (): VersionExprKind | undefined {
    return this.args.kind as VersionExprKind | undefined;
  }
}

export class SchemaExpr extends Expression {
  key = ExpressionKey.SCHEMA;
}

export type LockExprArgs = { update: Expression; wait?: Expression; key?: unknown; [key: string]: unknown } & BaseExpressionArgs;

export class LockExpr extends Expression {
  key = ExpressionKey.LOCK;

  /**
   * Defines the arguments (properties and child expressions) for Lock expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    update: true,
    wait: false,
    key: false,
  };

  constructor (args: LockExprArgs) {
    super(args);
  }

  get update (): Expression {
    return this.args.update as Expression;
  }

  get wait (): Expression {
    return this.args.wait as Expression;
  }
}

export type TableSampleExprArgs = { method?: string; bucketNumerator?: Expression; bucketDenominator?: Expression; bucketField?: Expression; percent?: Expression; rows?: Expression[]; size?: number | Expression; seed?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TableSampleExpr extends Expression {
  key = ExpressionKey.TABLE_SAMPLE;

  /**
   * Defines the arguments (properties and child expressions) for TableSample expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    method: false,
    bucketNumerator: false,
    bucketDenominator: false,
    bucketField: false,
    percent: false,
    rows: false,
    size: false,
    seed: false,
  };

  constructor (args: TableSampleExprArgs = {}) {
    super(args);
  }

  get method (): string {
    return this.args.method as string;
  }

  get bucketNumerator (): Expression {
    return this.args.bucketNumerator as Expression;
  }

  get bucketDenominator (): Expression {
    return this.args.bucketDenominator as Expression;
  }

  get bucketField (): Expression {
    return this.args.bucketField as Expression;
  }

  get percent (): Expression {
    return this.args.percent as Expression;
  }

  get rows (): Expression[] {
    return (this.args.rows || []) as Expression[];
  }

  get size (): number | Expression {
    return this.args.size as number | Expression;
  }

  get seed (): Expression {
    return this.args.seed as Expression;
  }
}

export type TagExprArgs = { prefix?: Expression; postfix?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TagExpr extends Expression {
  key = ExpressionKey.TAG;

  /**
   * Defines the arguments (properties and child expressions) for Tag expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    prefix: false,
    postfix: false,
  };

  constructor (args: TagExprArgs = {}) {
    super(args);
  }

  get prefix (): Expression {
    return this.args.prefix as Expression;
  }

  get postfix (): Expression {
    return this.args.postfix as Expression;
  }
}

export type PivotExprArgs = { fields?: Expression[]; unpivot?: Expression; using?: string; group?: Expression; columns?: Expression[]; includeNulls?: Expression[]; defaultOnNull?: Expression; into?: Expression; with?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class PivotExpr extends Expression {
  key = ExpressionKey.PIVOT;

  /**
   * Defines the arguments (properties and child expressions) for Pivot expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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

  constructor (args: PivotExprArgs = {}) {
    super(args);
  }

  get fields (): Expression[] {
    return (this.args.fields || []) as Expression[];
  }

  get unpivot (): Expression {
    return this.args.unpivot as Expression;
  }

  get using (): string {
    return this.args.using as string;
  }

  get group (): Expression {
    return this.args.group as Expression;
  }

  get columns (): Expression[] {
    return (this.args.columns || []) as Expression[];
  }

  get includeNulls (): Expression[] {
    return (this.args.includeNulls || []) as Expression[];
  }

  get defaultOnNull (): Expression {
    return this.args.defaultOnNull as Expression;
  }

  get into (): Expression {
    return this.args.into as Expression;
  }

  get with (): Expression {
    return this.args['with'] as Expression;
  }
}

export class UnpivotColumnsExpr extends Expression {
  key = ExpressionKey.UNPIVOT_COLUMNS;
}

/**
 * Valid kind values for window frame specifications
 */
export enum WindowSpecExprKind {
  ROWS = 'ROWS',
  RANGE = 'RANGE',
  GROUPS = 'GROUPS',
}
export type WindowSpecExprArgs = { kind?: WindowSpecExprKind; start?: Expression; startSide?: Expression; end?: Expression; endSide?: Expression; exclude?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class WindowSpecExpr extends Expression {
  key = ExpressionKey.WINDOW_SPEC;

  /**
   * Defines the arguments (properties and child expressions) for WindowSpec expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: false,
    start: false,
    startSide: false,
    end: false,
    endSide: false,
    exclude: false,
  };

  constructor (args: WindowSpecExprArgs = {}) {
    super(args);
  }

  get kind (): WindowSpecExprKind | undefined {
    return this.args.kind as WindowSpecExprKind | undefined;
  }

  get start (): Expression {
    return this.args.start as Expression;
  }

  get startSide (): Expression {
    return this.args.startSide as Expression;
  }

  get end (): Expression {
    return this.args.end as Expression;
  }

  get endSide (): Expression {
    return this.args.endSide as Expression;
  }

  get exclude (): Expression {
    return this.args.exclude as Expression;
  }
}

export class PreWhereExpr extends Expression {
  key = ExpressionKey.PRE_WHERE;
}

export class WhereExpr extends Expression {
  key = ExpressionKey.WHERE;
}

export type StarExprArgs = { except?: Expression; replace?: boolean; rename?: string; [key: string]: unknown } & BaseExpressionArgs;

export class StarExpr extends Expression {
  key = ExpressionKey.STAR;

  /**
   * Defines the arguments (properties and child expressions) for Star expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    except: false,
    replace: false,
    rename: false,
  };

  constructor (args: StarExprArgs = {}) {
    super(args);
  }

  get except (): Expression {
    return this.args.except as Expression;
  }

  get replace (): boolean {
    return this.args.replace as boolean;
  }

  get rename (): string {
    return this.args.rename as string;
  }
}

export class DataTypeParamExpr extends Expression {
  key = ExpressionKey.DATA_TYPE_PARAM;
}

/**
 * Valid kind values for DataType expressions (SQL data types)
 */
export enum DataTypeExprKind {
  CHAR = 'CHAR',
  VARCHAR = 'VARCHAR',
  TEXT = 'TEXT',
  INT = 'INT',
  BIGINT = 'BIGINT',
  FLOAT = 'FLOAT',
  DOUBLE = 'DOUBLE',
  DECIMAL = 'DECIMAL',
  BOOLEAN = 'BOOLEAN',
  DATE = 'DATE',
  DATETIME = 'DATETIME',
  TIMESTAMP = 'TIMESTAMP',
  ARRAY = 'ARRAY',
  MAP = 'MAP',
  STRUCT = 'STRUCT',
  JSON = 'JSON',
}

export type DataTypeExprArgs = { nested?: Expression; values?: Expression[]; prefix?: Expression; kind?: DataTypeExprKind | string; nullable?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DataTypeExpr extends Expression {
  key = ExpressionKey.DATA_TYPE;

  /**
   * Defines the arguments (properties and child expressions) for DataType expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    nested: false,
    values: false,
    prefix: false,
    kind: false,
    nullable: false,
  };

  constructor (args: DataTypeExprArgs = {}) {
    super(args);
  }

  /**
   * Build a DataTypeExpr from a string type name
   * @param dtype - Data type name
   * @returns DataTypeExpr instance
   */
  static build (dtype: string): DataTypeExpr {
    return new DataTypeExpr({ this: dtype });
  }

  get nested (): Expression {
    return this.args.nested as Expression;
  }

  get values (): Expression[] {
    return (this.args.values || []) as Expression[];
  }

  get prefix (): Expression {
    return this.args.prefix as Expression;
  }

  get kind (): DataTypeExprKind | string | undefined {
    return this.args.kind as DataTypeExprKind | string | undefined;
  }

  get nullable (): Expression {
    return this.args.nullable as Expression;
  }
}

export class TypeExpr extends Expression {
  key = ExpressionKey.TYPE;
}

export class CommandExpr extends Expression {
  key = ExpressionKey.COMMAND;
}

export type TransactionExprArgs = { modes?: Expression[]; mark?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TransactionExpr extends Expression {
  key = ExpressionKey.TRANSACTION;

  /**
   * Defines the arguments (properties and child expressions) for Transaction expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    modes: false,
    mark: false,
  };

  constructor (args: TransactionExprArgs = {}) {
    super(args);
  }

  get modes (): Expression[] {
    return (this.args.modes || []) as Expression[];
  }

  get mark (): Expression {
    return this.args.mark as Expression;
  }
}

export type CommitExprArgs = { chain?: Expression; durability?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class CommitExpr extends Expression {
  key = ExpressionKey.COMMIT;

  /**
   * Defines the arguments (properties and child expressions) for Commit expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    chain: false,
    durability: false,
  };

  constructor (args: CommitExprArgs = {}) {
    super(args);
  }

  get chain (): Expression {
    return this.args.chain as Expression;
  }

  get durability (): Expression {
    return this.args.durability as Expression;
  }
}

export type RollbackExprArgs = { savepoint?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class RollbackExpr extends Expression {
  key = ExpressionKey.ROLLBACK;

  /**
   * Defines the arguments (properties and child expressions) for Rollback expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    savepoint: false,
  };

  constructor (args: RollbackExprArgs = {}) {
    super(args);
  }

  get savepoint (): Expression {
    return this.args.savepoint as Expression;
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

export type AlterExprArgs = { kind: AlterExprKind; actions: Expression[]; exists?: Expression; only?: Expression; options?: Expression[]; cluster?: Expression; notValid?: Expression; check?: Expression; cascade?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class AlterExpr extends Expression {
  key = ExpressionKey.ALTER;

  /**
   * Defines the arguments (properties and child expressions) for Alter expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
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

  constructor (args: AlterExprArgs) {
    super(args);
  }

  get kind (): AlterExprKind | undefined {
    return this.args.kind as AlterExprKind | undefined;
  }

  get actions (): Expression[] {
    return (this.args.actions || []) as Expression[];
  }

  get exists (): Expression {
    return this.args.exists as Expression;
  }

  get only (): Expression {
    return this.args.only as Expression;
  }

  get options (): Expression[] {
    return (this.args.options || []) as Expression[];
  }

  get cluster (): Expression {
    return this.args.cluster as Expression;
  }

  get notValid (): Expression {
    return this.args.notValid as Expression;
  }

  get check (): Expression {
    return this.args.check as Expression;
  }

  get cascade (): Expression {
    return this.args.cascade as Expression;
  }
}

export type AlterSessionExprArgs = { unset?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class AlterSessionExpr extends Expression {
  key = ExpressionKey.ALTER_SESSION;

  /**
   * Defines the arguments (properties and child expressions) for AlterSession expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unset: false,
  };

  constructor (args: AlterSessionExprArgs = {}) {
    super(args);
  }

  get unset (): Expression {
    return this.args.unset as Expression;
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
export type AnalyzeExprArgs = { kind?: AnalyzeExprKind; options?: Expression[]; mode?: Expression; partition?: Expression; properties?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class AnalyzeExpr extends Expression {
  key = ExpressionKey.ANALYZE;

  /**
   * Defines the arguments (properties and child expressions) for Analyze expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: false,
    options: false,
    mode: false,
    partition: false,
    properties: false,
  };

  constructor (args: AnalyzeExprArgs = {}) {
    super(args);
  }

  get kind (): AnalyzeExprKind | undefined {
    return this.args.kind as AnalyzeExprKind | undefined;
  }

  get options (): Expression[] {
    return (this.args.options || []) as Expression[];
  }

  get mode (): Expression {
    return this.args.mode as Expression;
  }

  get partition (): Expression {
    return this.args.partition as Expression;
  }

  get properties (): Expression[] {
    return (this.args.properties || []) as Expression[];
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

export type AnalyzeStatisticsExprArgs = { kind: AnalyzeStatisticsExprKind; option?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class AnalyzeStatisticsExpr extends Expression {
  key = ExpressionKey.ANALYZE_STATISTICS;

  /**
   * Defines the arguments (properties and child expressions) for AnalyzeStatistics expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: true,
    option: false,
  };

  constructor (args: AnalyzeStatisticsExprArgs) {
    super(args);
  }

  get kind (): AnalyzeStatisticsExprKind | undefined {
    return this.args.kind as AnalyzeStatisticsExprKind | undefined;
  }

  get option (): Expression {
    return this.args.option as Expression;
  }
}

export type AnalyzeHistogramExprArgs = { updateOptions?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class AnalyzeHistogramExpr extends Expression {
  key = ExpressionKey.ANALYZE_HISTOGRAM;

  /**
   * Defines the arguments (properties and child expressions) for AnalyzeHistogram expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    updateOptions: false,
  };

  constructor (args: AnalyzeHistogramExprArgs = {}) {
    super(args);
  }

  get updateOptions (): Expression[] {
    return (this.args.updateOptions || []) as Expression[];
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

export type AnalyzeSampleExprArgs = { kind: AnalyzeSampleExprKind; sample: number | Expression; [key: string]: unknown } & BaseExpressionArgs;

export class AnalyzeSampleExpr extends Expression {
  key = ExpressionKey.ANALYZE_SAMPLE;

  /**
   * Defines the arguments (properties and child expressions) for AnalyzeSample expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: true,
    sample: true,
  };

  constructor (args: AnalyzeSampleExprArgs) {
    super(args);
  }

  get kind (): AnalyzeSampleExprKind | undefined {
    return this.args.kind as AnalyzeSampleExprKind | undefined;
  }

  get sample (): number | Expression {
    return this.args.sample as number | Expression;
  }
}

export class AnalyzeListChainedRowsExpr extends Expression {
  key = ExpressionKey.ANALYZE_LIST_CHAINED_ROWS;
}

/**
 * Valid kind values for ANALYZE DELETE statements
 */
export enum AnalyzeDeleteExprKind {
  STATISTICS = 'STATISTICS',
}
export type AnalyzeDeleteExprArgs = { kind?: AnalyzeDeleteExprKind; [key: string]: unknown } & BaseExpressionArgs;

export class AnalyzeDeleteExpr extends Expression {
  key = ExpressionKey.ANALYZE_DELETE;

  /**
   * Defines the arguments (properties and child expressions) for AnalyzeDelete expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: false,
  };

  constructor (args: AnalyzeDeleteExprArgs = {}) {
    super(args);
  }

  get kind (): AnalyzeDeleteExprKind | undefined {
    return this.args.kind as AnalyzeDeleteExprKind | undefined;
  }
}

export class AnalyzeWithExpr extends Expression {
  key = ExpressionKey.ANALYZE_WITH;
}

/**
 * Enumeration of valid kind values for AnalyzeValidate expressions.
 * Used to specify the variant or subtype of the expression.
 */
export enum AnalyzeValidateExprKind {
  REF_UPDATE = 'REF_UPDATE',
  STRUCTURE = 'STRUCTURE',
}

export type AnalyzeValidateExprArgs = { kind: AnalyzeValidateExprKind; [key: string]: unknown } & BaseExpressionArgs;

export class AnalyzeValidateExpr extends Expression {
  key = ExpressionKey.ANALYZE_VALIDATE;

  /**
   * Defines the arguments (properties and child expressions) for AnalyzeValidate expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: true,
  };

  constructor (args: AnalyzeValidateExprArgs) {
    super(args);
  }

  get kind (): AnalyzeValidateExprKind | undefined {
    return this.args.kind as AnalyzeValidateExprKind | undefined;
  }
}

export class AnalyzeColumnsExpr extends Expression {
  key = ExpressionKey.ANALYZE_COLUMNS;
}

export class UsingDataExpr extends Expression {
  key = ExpressionKey.USING_DATA;
}

export class AddConstraintExpr extends Expression {
  key = ExpressionKey.ADD_CONSTRAINT;
}

export type AddPartitionExprArgs = { exists?: Expression; location?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class AddPartitionExpr extends Expression {
  key = ExpressionKey.ADD_PARTITION;

  /**
   * Defines the arguments (properties and child expressions) for AddPartition expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    exists: false,
    location: false,
  };

  constructor (args: AddPartitionExprArgs = {}) {
    super(args);
  }

  get exists (): Expression {
    return this.args.exists as Expression;
  }

  get location (): Expression {
    return this.args.location as Expression;
  }
}

export class AttachOptionExpr extends Expression {
  key = ExpressionKey.ATTACH_OPTION;
}

export type DropPartitionExprArgs = { exists?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DropPartitionExpr extends Expression {
  key = ExpressionKey.DROP_PARTITION;

  /**
   * Defines the arguments (properties and child expressions) for DropPartition expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    exists: false,
  };

  constructor (args: DropPartitionExprArgs = {}) {
    super(args);
  }

  get exists (): Expression {
    return this.args.exists as Expression;
  }
}

export type ReplacePartitionExprArgs = { source: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ReplacePartitionExpr extends Expression {
  key = ExpressionKey.REPLACE_PARTITION;

  /**
   * Defines the arguments (properties and child expressions) for ReplacePartition expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    source: true,
  };

  constructor (args: ReplacePartitionExprArgs) {
    super(args);
  }

  get source (): Expression {
    return this.args.source as Expression;
  }
}

export class AliasExpr extends Expression {
  key = ExpressionKey.ALIAS;
}

export class PivotAnyExpr extends Expression {
  key = ExpressionKey.PIVOT_ANY;
}

export class AliasesExpr extends Expression {
  key = ExpressionKey.ALIASES;
}

export class AtIndexExpr extends Expression {
  key = ExpressionKey.AT_INDEX;
}

export type AtTimeZoneExprArgs = { zone: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class AtTimeZoneExpr extends Expression {
  key = ExpressionKey.AT_TIME_ZONE;

  /**
   * Defines the arguments (properties and child expressions) for AtTimeZone expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    zone: true,
  };

  constructor (args: AtTimeZoneExprArgs) {
    super(args);
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }
}

export type FromTimeZoneExprArgs = { zone: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class FromTimeZoneExpr extends Expression {
  key = ExpressionKey.FROM_TIME_ZONE;

  /**
   * Defines the arguments (properties and child expressions) for FromTimeZone expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    zone: true,
  };

  constructor (args: FromTimeZoneExprArgs) {
    super(args);
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }
}

export type FormatPhraseExprArgs = { format: string; [key: string]: unknown } & BaseExpressionArgs;

export class FormatPhraseExpr extends Expression {
  key = ExpressionKey.FORMAT_PHRASE;

  /**
   * Defines the arguments (properties and child expressions) for FormatPhrase expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: true,
  };

  constructor (args: FormatPhraseExprArgs) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }
}

export type DistinctExprArgs = { on?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DistinctExpr extends Expression {
  key = ExpressionKey.DISTINCT;

  /**
   * Defines the arguments (properties and child expressions) for Distinct expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    on: false,
  };

  constructor (args: DistinctExprArgs = {}) {
    super(args);
  }

  get on (): Expression {
    return this.args.on as Expression;
  }
}

export class ForInExpr extends Expression {
  key = ExpressionKey.FOR_IN;
}

export type TimeUnitExprArgs = { unit?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TimeUnitExpr extends Expression {
  key = ExpressionKey.TIME_UNIT;

  /**
   * Defines the arguments (properties and child expressions) for TimeUnit expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
  };

  constructor (args: TimeUnitExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }
}

export class IgnoreNullsExpr extends Expression {
  key = ExpressionKey.IGNORE_NULLS;
}

export class RespectNullsExpr extends Expression {
  key = ExpressionKey.RESPECT_NULLS;
}

export type HavingMaxExprArgs = { max: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class HavingMaxExpr extends Expression {
  key = ExpressionKey.HAVING_MAX;

  /**
   * Defines the arguments (properties and child expressions) for HavingMax expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    max: true,
  };

  constructor (args: HavingMaxExprArgs) {
    super(args);
  }

  get max (): Expression {
    return this.args.max as Expression;
  }
}

export type TranslateCharactersExprArgs = { withError?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TranslateCharactersExpr extends Expression {
  key = ExpressionKey.TRANSLATE_CHARACTERS;

  /**
   * Defines the arguments (properties and child expressions) for TranslateCharacters expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    withError: false,
  };

  constructor (args: TranslateCharactersExprArgs = {}) {
    super(args);
  }

  get withError (): Expression {
    return this.args.withError as Expression;
  }
}

export class PositionalColumnExpr extends Expression {
  key = ExpressionKey.POSITIONAL_COLUMN;
}

export type OverflowTruncateBehaviorExprArgs = { withCount: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class OverflowTruncateBehaviorExpr extends Expression {
  key = ExpressionKey.OVERFLOW_TRUNCATE_BEHAVIOR;

  /**
   * Defines the arguments (properties and child expressions) for OverflowTruncateBehavior expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    withCount: true,
  };

  constructor (args: OverflowTruncateBehaviorExprArgs) {
    super(args);
  }

  get withCount (): Expression {
    return this.args.withCount as Expression;
  }
}

export type JSONExprArgs = { with?: Expression; unique?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class JSONExpr extends Expression {
  key = ExpressionKey.JSON;

  /**
   * Defines the arguments (properties and child expressions) for JSON expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    with: false,
    unique: false,
  };

  constructor (args: JSONExprArgs = {}) {
    super(args);
  }

  get with (): Expression {
    return this.args['with'] as Expression;
  }

  get unique (): boolean {
    return this.args.unique as boolean;
  }
}

export type JSONPathExprArgs = { escape?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONPathExpr extends Expression {
  key = ExpressionKey.JSON_PATH;

  /**
   * Defines the arguments (properties and child expressions) for JSONPath expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    escape: false,
  };

  constructor (args: JSONPathExprArgs = {}) {
    super(args);
  }

  get escape (): Expression {
    return this.args.escape as Expression;
  }
}

export class JSONPathPartExpr extends Expression {
  key = ExpressionKey.JSON_PATH_PART;
}

export class FormatJsonExpr extends Expression {
  key = ExpressionKey.FORMAT_JSON;
}

export class JSONKeyValueExpr extends Expression {
  key = ExpressionKey.JSON_KEY_VALUE;
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
export type JSONColumnDefExprArgs = { kind?: JSONColumnDefExprKind; path?: Expression; nestedSchema?: Expression; ordinality?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class JSONColumnDefExpr extends Expression {
  key = ExpressionKey.JSON_COLUMN_DEF;

  /**
   * Defines the arguments (properties and child expressions) for JSONColumnDef expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: false,
    path: false,
    nestedSchema: false,
    ordinality: false,
  };

  constructor (args: JSONColumnDefExprArgs = {}) {
    super(args);
  }

  get kind (): JSONColumnDefExprKind | undefined {
    return this.args.kind as JSONColumnDefExprKind | undefined;
  }

  get path (): Expression {
    return this.args.path as Expression;
  }

  get nestedSchema (): Expression {
    return this.args.nestedSchema as Expression;
  }

  get ordinality (): boolean {
    return this.args.ordinality as boolean;
  }
}

export class JSONSchemaExpr extends Expression {
  key = ExpressionKey.JSON_SCHEMA;
}

export type JSONValueExprArgs = { path: Expression; returning?: Expression; onCondition?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONValueExpr extends Expression {
  key = ExpressionKey.JSON_VALUE;

  /**
   * Defines the arguments (properties and child expressions) for JSONValue expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    path: true,
    returning: false,
    onCondition: false,
  };

  constructor (args: JSONValueExprArgs) {
    super(args);
  }

  get path (): Expression {
    return this.args.path as Expression;
  }

  get returning (): Expression {
    return this.args.returning as Expression;
  }

  get onCondition (): Expression {
    return this.args.onCondition as Expression;
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

export type OpenJSONColumnDefExprArgs = { kind: OpenJSONColumnDefExprKind; path?: Expression; asJson?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class OpenJSONColumnDefExpr extends Expression {
  key = ExpressionKey.OPEN_JSON_COLUMN_DEF;

  /**
   * Defines the arguments (properties and child expressions) for OpenJSONColumnDef expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: true,
    path: false,
    asJson: false,
  };

  constructor (args: OpenJSONColumnDefExprArgs) {
    super(args);
  }

  get kind (): OpenJSONColumnDefExprKind | undefined {
    return this.args.kind as OpenJSONColumnDefExprKind | undefined;
  }

  get path (): Expression {
    return this.args.path as Expression;
  }

  get asJson (): Expression {
    return this.args.asJson as Expression;
  }
}

export type JSONExtractQuoteExprArgs = { option: Expression; scalar?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONExtractQuoteExpr extends Expression {
  key = ExpressionKey.JSON_EXTRACT_QUOTE;

  /**
   * Defines the arguments (properties and child expressions) for JSONExtractQuote expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    option: true,
    scalar: false,
  };

  constructor (args: JSONExtractQuoteExprArgs) {
    super(args);
  }

  get option (): Expression {
    return this.args.option as Expression;
  }

  get scalar (): Expression {
    return this.args.scalar as Expression;
  }
}

export class ScopeResolutionExpr extends Expression {
  key = ExpressionKey.SCOPE_RESOLUTION;
}

export type SliceExprArgs = { step?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class SliceExpr extends Expression {
  key = ExpressionKey.SLICE;

  /**
   * Defines the arguments (properties and child expressions) for Slice expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    step: false,
  };

  constructor (args: SliceExprArgs = {}) {
    super(args);
  }

  get step (): Expression {
    return this.args.step as Expression;
  }
}

export class StreamExpr extends Expression {
  key = ExpressionKey.STREAM;
}

export class ModelAttributeExpr extends Expression {
  key = ExpressionKey.MODEL_ATTRIBUTE;
}

export class WeekStartExpr extends Expression {
  key = ExpressionKey.WEEK_START;
}

export class XMLNamespaceExpr extends Expression {
  key = ExpressionKey.XML_NAMESPACE;
}

export class XMLKeyValueOptionExpr extends Expression {
  key = ExpressionKey.XML_KEY_VALUE_OPTION;
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
export type UseExprArgs = { kind?: UseExprKind; [key: string]: unknown } & BaseExpressionArgs;

export class UseExpr extends Expression {
  key = ExpressionKey.USE;

  /**
   * Defines the arguments (properties and child expressions) for Use expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: false,
  };

  constructor (args: UseExprArgs = {}) {
    super(args);
  }

  get kind (): UseExprKind | undefined {
    return this.args.kind as UseExprKind | undefined;
  }
}

export type WhenExprArgs = { matched: Expression; source?: Expression; condition?: Expression; then: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class WhenExpr extends Expression {
  key = ExpressionKey.WHEN;

  /**
   * Defines the arguments (properties and child expressions) for When expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    matched: true,
    source: false,
    condition: false,
    then: true,
  };

  constructor (args: WhenExprArgs) {
    super(args);
  }

  get matched (): Expression {
    return this.args.matched as Expression;
  }

  get source (): Expression {
    return this.args.source as Expression;
  }

  get condition (): Expression {
    return this.args.condition as Expression;
  }

  get then (): Expression {
    return this.args.then as Expression;
  }
}

export class WhensExpr extends Expression {
  key = ExpressionKey.WHENS;
}

export class SemicolonExpr extends Expression {
  key = ExpressionKey.SEMICOLON;
}

export class TableColumnExpr extends Expression {
  key = ExpressionKey.TABLE_COLUMN;
}

export class VariadicExpr extends Expression {
  key = ExpressionKey.VARIADIC;
}

export type CTEExprArgs = { scalar?: boolean; materialized?: boolean; keyExpressions?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class CTEExpr extends DerivedTableExpr {
  key = ExpressionKey.CTE;

  /**
   * Defines the arguments (properties and child expressions) for CTE expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    scalar: false,
    materialized: false,
    keyExpressions: false,
  };

  constructor (args: CTEExprArgs = {}) {
    super(args);
  }

  get scalar (): Expression {
    return this.args.scalar as Expression;
  }

  get materialized (): boolean {
    return this.args.materialized as boolean;
  }

  get keyExpressions (): Expression[] {
    return (this.args.keyExpressions || []) as Expression[];
  }
}

export class BitStringExpr extends Expression {
  key = ExpressionKey.BIT_STRING;
}

export type HexStringExprArgs = { isInteger?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class HexStringExpr extends Expression {
  key = ExpressionKey.HEX_STRING;

  /**
   * Defines the arguments (properties and child expressions) for HexString expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    isInteger: false,
  };

  constructor (args: HexStringExprArgs = {}) {
    super(args);
  }

  get isInteger (): Expression {
    return this.args.isInteger as Expression;
  }
}

export type ByteStringExprArgs = { isBytes?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class ByteStringExpr extends Expression {
  key = ExpressionKey.BYTE_STRING;

  /**
   * Defines the arguments (properties and child expressions) for ByteString expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    isBytes: false,
  };

  constructor (args: ByteStringExprArgs = {}) {
    super(args);
  }

  get isBytes (): Expression[] {
    return (this.args.isBytes || []) as Expression[];
  }
}

export class RawStringExpr extends Expression {
  key = ExpressionKey.RAW_STRING;
}

export type UnicodeStringExprArgs = { escape?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class UnicodeStringExpr extends Expression {
  key = ExpressionKey.UNICODE_STRING;

  /**
   * Defines the arguments (properties and child expressions) for UnicodeString expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    escape: false,
  };

  constructor (args: UnicodeStringExprArgs = {}) {
    super(args);
  }

  get escape (): Expression {
    return this.args.escape as Expression;
  }
}

/**
 * Represents a column reference (optionally qualified with table name).
 *
 * @example
 * // users.id
 * const col = columnExpr('id', 'users');
 */
export type ColumnExprArgs = { table?: Expression; db?: string; catalog?: string; joinMark?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ColumnExpr extends Expression {
  key = ExpressionKey.COLUMN;

  /**
   * Defines the arguments (properties and child expressions) for Column expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    table: false,
    db: false,
    catalog: false,
    joinMark: false,
  };

  constructor (args: ColumnExprArgs = {}) {
    super(args);
  }

  get table (): Expression {
    return this.args.table as Expression;
  }

  get db (): string {
    return this.args.db as string;
  }

  get catalog (): string {
    return this.args.catalog as string;
  }

  get joinMark (): Expression {
    return this.args.joinMark as Expression;
  }
}

export class AutoIncrementColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.AUTO_INCREMENT_COLUMN_CONSTRAINT;
}

export class ZeroFillColumnConstraintExpr extends ColumnConstraintExpr {
  key = ExpressionKey.ZERO_FILL_COLUMN_CONSTRAINT;
}

export class PeriodForSystemTimeConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.PERIOD_FOR_SYSTEM_TIME_CONSTRAINT;
}

export type CaseSpecificColumnConstraintExprArgs = { not: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class CaseSpecificColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.CASE_SPECIFIC_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for CaseSpecificColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    not: true,
  };

  constructor (args: CaseSpecificColumnConstraintExprArgs) {
    super(args);
  }

  get not (): Expression {
    return this.args.not as Expression;
  }
}

export class CharacterSetColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.CHARACTER_SET_COLUMN_CONSTRAINT;
}

export type CheckColumnConstraintExprArgs = { enforced?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class CheckColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.CHECK_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for CheckColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    enforced: false,
  };

  constructor (args: CheckColumnConstraintExprArgs = {}) {
    super(args);
  }

  get enforced (): Expression {
    return this.args.enforced as Expression;
  }
}

export class ClusteredColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.CLUSTERED_COLUMN_CONSTRAINT;
}

export class CollateColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.COLLATE_COLUMN_CONSTRAINT;
}

export class CommentColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.COMMENT_COLUMN_CONSTRAINT;
}

export class CompressColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.COMPRESS_COLUMN_CONSTRAINT;
}

export class DateFormatColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.DATE_FORMAT_COLUMN_CONSTRAINT;
}

export class DefaultColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.DEFAULT_COLUMN_CONSTRAINT;
}

export class EncodeColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.ENCODE_COLUMN_CONSTRAINT;
}

export class ExcludeColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.EXCLUDE_COLUMN_CONSTRAINT;
}

export class EphemeralColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.EPHEMERAL_COLUMN_CONSTRAINT;
}

export type GeneratedAsIdentityColumnConstraintExprArgs = { onNull?: Expression; start?: Expression; increment?: Expression; minvalue?: string; maxvalue?: string; cycle?: Expression; order?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class GeneratedAsIdentityColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.GENERATED_AS_IDENTITY_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for GeneratedAsIdentityColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    onNull: false,
    start: false,
    increment: false,
    minvalue: false,
    maxvalue: false,
    cycle: false,
    order: false,
  };

  constructor (args: GeneratedAsIdentityColumnConstraintExprArgs = {}) {
    super(args);
  }

  get onNull (): Expression {
    return this.args.onNull as Expression;
  }

  get start (): Expression {
    return this.args.start as Expression;
  }

  get increment (): Expression {
    return this.args.increment as Expression;
  }

  get minvalue (): string {
    return this.args.minvalue as string;
  }

  get maxvalue (): string {
    return this.args.maxvalue as string;
  }

  get cycle (): Expression {
    return this.args.cycle as Expression;
  }

  get order (): Expression {
    return this.args.order as Expression;
  }
}

export type GeneratedAsRowColumnConstraintExprArgs = { start?: Expression; hidden?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class GeneratedAsRowColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.GENERATED_AS_ROW_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for GeneratedAsRowColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    start: false,
    hidden: false,
  };

  constructor (args: GeneratedAsRowColumnConstraintExprArgs = {}) {
    super(args);
  }

  get start (): Expression {
    return this.args.start as Expression;
  }

  get hidden (): Expression {
    return this.args.hidden as Expression;
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
export type IndexColumnConstraintExprArgs = { kind?: IndexColumnConstraintExprKind; indexType?: DataTypeExpr; options?: Expression[]; granularity?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class IndexColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.INDEX_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for IndexColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: false,
    indexType: false,
    options: false,
    granularity: false,
  };

  constructor (args: IndexColumnConstraintExprArgs = {}) {
    super(args);
  }

  get kind (): IndexColumnConstraintExprKind | undefined {
    return this.args.kind as IndexColumnConstraintExprKind | undefined;
  }

  get indexType (): DataTypeExpr {
    return this.args.indexType as DataTypeExpr;
  }

  get options (): Expression[] {
    return (this.args.options || []) as Expression[];
  }

  get granularity (): Expression {
    return this.args.granularity as Expression;
  }
}

export class InlineLengthColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.INLINE_LENGTH_COLUMN_CONSTRAINT;
}

export class NonClusteredColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.NON_CLUSTERED_COLUMN_CONSTRAINT;
}

export class NotForReplicationColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.NOT_FOR_REPLICATION_COLUMN_CONSTRAINT;
}

export class MaskingPolicyColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.MASKING_POLICY_COLUMN_CONSTRAINT;
}

export type NotNullColumnConstraintExprArgs = { allowNull?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class NotNullColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.NOT_NULL_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for NotNullColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    allowNull: false,
  };

  constructor (args: NotNullColumnConstraintExprArgs = {}) {
    super(args);
  }

  get allowNull (): Expression {
    return this.args.allowNull as Expression;
  }
}

export class OnUpdateColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.ON_UPDATE_COLUMN_CONSTRAINT;
}

export type PrimaryKeyColumnConstraintExprArgs = { desc?: Expression; options?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class PrimaryKeyColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.PRIMARY_KEY_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for PrimaryKeyColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    desc: false,
    options: false,
  };

  constructor (args: PrimaryKeyColumnConstraintExprArgs = {}) {
    super(args);
  }

  get desc (): Expression {
    return this.args.desc as Expression;
  }

  get options (): Expression[] {
    return (this.args.options || []) as Expression[];
  }
}

export class TitleColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.TITLE_COLUMN_CONSTRAINT;
}

export type UniqueColumnConstraintExprArgs = { indexType?: DataTypeExpr; onConflict?: Expression; nulls?: Expression[]; options?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class UniqueColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.UNIQUE_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for UniqueColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    indexType: false,
    onConflict: false,
    nulls: false,
    options: false,
  };

  constructor (args: UniqueColumnConstraintExprArgs = {}) {
    super(args);
  }

  get indexType (): DataTypeExpr {
    return this.args.indexType as DataTypeExpr;
  }

  get onConflict (): Expression {
    return this.args.onConflict as Expression;
  }

  get nulls (): Expression[] {
    return (this.args.nulls || []) as Expression[];
  }

  get options (): Expression[] {
    return (this.args.options || []) as Expression[];
  }
}

export class UppercaseColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.UPPERCASE_COLUMN_CONSTRAINT;
}

export class PathColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.PATH_COLUMN_CONSTRAINT;
}

export class ProjectionPolicyColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.PROJECTION_POLICY_COLUMN_CONSTRAINT;
}

export type ComputedColumnConstraintExprArgs = { persisted?: Expression; notNull?: Expression; dataType?: DataTypeExpr; [key: string]: unknown } & BaseExpressionArgs;

export class ComputedColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.COMPUTED_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for ComputedColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    persisted: false,
    notNull: false,
    dataType: false,
  };

  constructor (args: ComputedColumnConstraintExprArgs = {}) {
    super(args);
  }

  get persisted (): Expression {
    return this.args.persisted as Expression;
  }

  get notNull (): Expression {
    return this.args.notNull as Expression;
  }

  get dataType (): DataTypeExpr {
    return this.args.dataType as DataTypeExpr;
  }
}

export type InOutColumnConstraintExprArgs = { input?: Expression; output?: Expression; variadic?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class InOutColumnConstraintExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.IN_OUT_COLUMN_CONSTRAINT;

  /**
   * Defines the arguments (properties and child expressions) for InOutColumnConstraint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    input: false,
    output: false,
    variadic: false,
  };

  constructor (args: InOutColumnConstraintExprArgs = {}) {
    super(args);
  }

  get input (): Expression {
    return this.args.input as Expression;
  }

  get output (): Expression {
    return this.args.output as Expression;
  }

  get variadic (): Expression {
    return this.args.variadic as Expression;
  }
}

export type DeleteExprArgs = { with?: Expression; using?: string; where?: Expression; returning?: Expression; order?: Expression; limit?: number | Expression; tables?: Expression[]; cluster?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DeleteExpr extends DMLExpr {
  key = ExpressionKey.DELETE;

  /**
   * Defines the arguments (properties and child expressions) for Delete expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    with: false,
    using: false,
    where: false,
    returning: false,
    order: false,
    limit: false,
    tables: false,
    cluster: false,
  };

  constructor (args: DeleteExprArgs = {}) {
    super(args);
  }

  get with (): Expression {
    return this.args['with'] as Expression;
  }

  get using (): string {
    return this.args.using as string;
  }

  get where (): Expression {
    return this.args.where as Expression;
  }

  get returning (): Expression {
    return this.args.returning as Expression;
  }

  get order (): Expression {
    return this.args.order as Expression;
  }

  get limit (): number | Expression {
    return this.args.limit as number | Expression;
  }

  get tables (): Expression[] {
    return (this.args.tables || []) as Expression[];
  }

  get cluster (): Expression {
    return this.args.cluster as Expression;
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

export type CopyExprArgs = { kind: CopyExprKind; files?: Expression[]; credentials?: Expression[]; format?: string; params?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class CopyExpr extends DMLExpr {
  key = ExpressionKey.COPY;

  /**
   * Defines the arguments (properties and child expressions) for Copy expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: true,
    files: false,
    credentials: false,
    format: false,
    params: false,
  };

  constructor (args: CopyExprArgs) {
    super(args);
  }

  get kind (): CopyExprKind | undefined {
    return this.args.kind as CopyExprKind | undefined;
  }

  get files (): Expression[] {
    return (this.args.files || []) as Expression[];
  }

  get credentials (): Expression[] {
    return (this.args.credentials || []) as Expression[];
  }

  get format (): string {
    return this.args.format as string;
  }

  get params (): Expression[] {
    return (this.args.params || []) as Expression[];
  }
}

export type InsertExprArgs = { hint?: Expression; with?: Expression; isFunction?: Expression; conflict?: Expression; returning?: Expression; overwrite?: Expression; exists?: Expression; alternative?: Expression; where?: Expression; ignore?: Expression; byName?: string; stored?: Expression; partition?: Expression; settings?: Expression[]; source?: Expression; default?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class InsertExpr extends DDLExpr {
  key = ExpressionKey.INSERT;

  /**
   * Defines the arguments (properties and child expressions) for Insert expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    hint: false,
    with: false,
    isFunction: false,
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

  constructor (args: InsertExprArgs = {}) {
    super(args);
  }

  get hint (): Expression {
    return this.args.hint as Expression;
  }

  get with (): Expression {
    return this.args['with'] as Expression;
  }

  get isFunction (): Expression {
    return this.args.isFunction as Expression;
  }

  get conflict (): Expression {
    return this.args.conflict as Expression;
  }

  get returning (): Expression {
    return this.args.returning as Expression;
  }

  get overwrite (): Expression {
    return this.args.overwrite as Expression;
  }

  get exists (): Expression {
    return this.args.exists as Expression;
  }

  get alternative (): Expression {
    return this.args.alternative as Expression;
  }

  get where (): Expression {
    return this.args.where as Expression;
  }

  get ignore (): Expression {
    return this.args.ignore as Expression;
  }

  get byName (): string {
    return this.args.byName as string;
  }

  get stored (): Expression {
    return this.args.stored as Expression;
  }

  get partition (): Expression {
    return this.args.partition as Expression;
  }

  get settings (): Expression[] {
    return (this.args.settings || []) as Expression[];
  }

  get source (): Expression {
    return this.args.source as Expression;
  }

  get default (): Expression {
    return this.args['default'] as Expression;
  }
}

/**
 * Represents a literal value (string, number, boolean, null).
 *
 * @example
 * const str = new LiteralExpr({ this: 'hello', isString: true });
 * const num = new LiteralExpr({ this: '42', isString: false });
 */
export type LiteralExprArgs = { isString: unknown; [key: string]: unknown } & BaseExpressionArgs;

export class LiteralExpr extends Expression {
  key = ExpressionKey.LITERAL;

  /**
   * Defines the arguments (properties and child expressions) for Literal expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    isString: true,
  };

  /**
   * Create a numeric literal expression
   * @param number - The number value
   * @returns A literal expression (or negative expression for negative numbers)
   */
  static number (number: number | string): LiteralExpr | NegExpr {
    let expr: LiteralExpr | NegExpr = new LiteralExpr({ this: String(number), isString: false });

    try {
      const numValue = typeof number === 'number' ? number : parseFloat(String(number));

      if (!isNaN(numValue) && numValue < 0) {
        expr = new LiteralExpr({ this: String(Math.abs(numValue)), isString: false });
        expr = new NegExpr({ this: expr });
      }
    } catch {
      // Keep original expression if parsing fails
    }

    return expr;
  }

  /**
   * Create a string literal expression
   * @param string - The string value
   * @returns A literal expression
   */
  static string (string: string | unknown): LiteralExpr {
    return new LiteralExpr({ this: String(string), isString: true });
  }

  constructor (args: LiteralExprArgs) {
    super(args);
  }
}

export class ClusterExpr extends OrderExpr {
  key = ExpressionKey.CLUSTER;
}

export class DistributeExpr extends OrderExpr {
  key = ExpressionKey.DISTRIBUTE;
}

export class SortExpr extends OrderExpr {
  key = ExpressionKey.SORT;
}

export class AlgorithmPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ALGORITHM_PROPERTY;
}

export class AutoIncrementPropertyExpr extends PropertyExpr {
  key = ExpressionKey.AUTO_INCREMENT_PROPERTY;
}

export class AutoRefreshPropertyExpr extends PropertyExpr {
  key = ExpressionKey.AUTO_REFRESH_PROPERTY;
}

export class BackupPropertyExpr extends PropertyExpr {
  key = ExpressionKey.BACKUP_PROPERTY;
}

export class BuildPropertyExpr extends PropertyExpr {
  key = ExpressionKey.BUILD_PROPERTY;
}

export type BlockCompressionPropertyExprArgs = { value?: string; autotemp?: Expression; always?: Expression[]; default?: Expression; manual?: Expression; never?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class BlockCompressionPropertyExpr extends PropertyExpr {
  key = ExpressionKey.BLOCK_COMPRESSION_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for BlockCompressionProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    autotemp: false,
    always: false,
    default: false,
    manual: false,
    never: false,
  };

  constructor (args: BlockCompressionPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get autotemp (): Expression {
    return this.args.autotemp as Expression;
  }

  get always (): Expression[] {
    return (this.args.always || []) as Expression[];
  }

  get default (): Expression {
    return this.args['default'] as Expression;
  }

  get manual (): Expression {
    return this.args.manual as Expression;
  }

  get never (): Expression {
    return this.args.never as Expression;
  }
}

export type CharacterSetPropertyExprArgs = { value?: string; default: Expression; [key: string]: unknown } & PropertyExprArgs;

export class CharacterSetPropertyExpr extends PropertyExpr {
  key = ExpressionKey.CHARACTER_SET_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for CharacterSetProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    default: true,
  };

  constructor (args: CharacterSetPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get default (): Expression {
    return this.args['default'] as Expression;
  }
}

export type ChecksumPropertyExprArgs = { value?: string; on?: Expression; default?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class ChecksumPropertyExpr extends PropertyExpr {
  key = ExpressionKey.CHECKSUM_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for ChecksumProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    on: false,
    default: false,
  };

  constructor (args: ChecksumPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get on (): Expression {
    return this.args.on as Expression;
  }

  get default (): Expression {
    return this.args['default'] as Expression;
  }
}

export type CollatePropertyExprArgs = { value?: string; default?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class CollatePropertyExpr extends PropertyExpr {
  key = ExpressionKey.COLLATE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for CollateProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    default: false,
  };

  constructor (args: CollatePropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get default (): Expression {
    return this.args['default'] as Expression;
  }
}

export class CopyGrantsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.COPY_GRANTS_PROPERTY;
}

export type DataBlocksizePropertyExprArgs = { value?: string; size?: number | Expression; units?: Expression[]; minimum?: Expression; maximum?: Expression; default?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class DataBlocksizePropertyExpr extends PropertyExpr {
  key = ExpressionKey.DATA_BLOCKSIZE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for DataBlocksizeProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    size: false,
    units: false,
    minimum: false,
    maximum: false,
    default: false,
  };

  constructor (args: DataBlocksizePropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get size (): number | Expression {
    return this.args.size as number | Expression;
  }

  get units (): Expression[] {
    return (this.args.units || []) as Expression[];
  }

  get minimum (): Expression {
    return this.args.minimum as Expression;
  }

  get maximum (): Expression {
    return this.args.maximum as Expression;
  }

  get default (): Expression {
    return this.args['default'] as Expression;
  }
}

export type DataDeletionPropertyExprArgs = { value?: string; on: Expression; filterColumn?: Expression; retentionPeriod?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class DataDeletionPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DATA_DELETION_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for DataDeletionProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    on: true,
    filterColumn: false,
    retentionPeriod: false,
  };

  constructor (args: DataDeletionPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get on (): Expression {
    return this.args.on as Expression;
  }

  get filterColumn (): Expression {
    return this.args.filterColumn as Expression;
  }

  get retentionPeriod (): Expression {
    return this.args.retentionPeriod as Expression;
  }
}

export class DefinerPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DEFINER_PROPERTY;
}

export class DistKeyPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DIST_KEY_PROPERTY;
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

export type DistributedByPropertyExprArgs = { value?: string; kind: DistributedByPropertyExprKind; buckets?: Expression[]; order?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class DistributedByPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DISTRIBUTED_BY_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for DistributedByProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    kind: true,
    buckets: false,
    order: false,
  };

  constructor (args: DistributedByPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get kind (): DistributedByPropertyExprKind | undefined {
    return this.args.kind as DistributedByPropertyExprKind | undefined;
  }

  get buckets (): Expression[] {
    return (this.args.buckets || []) as Expression[];
  }

  get order (): Expression {
    return this.args.order as Expression;
  }
}

export class DistStylePropertyExpr extends PropertyExpr {
  key = ExpressionKey.DIST_STYLE_PROPERTY;
}

export class DuplicateKeyPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DUPLICATE_KEY_PROPERTY;
}

export class EnginePropertyExpr extends PropertyExpr {
  key = ExpressionKey.ENGINE_PROPERTY;
}

export class HeapPropertyExpr extends PropertyExpr {
  key = ExpressionKey.HEAP_PROPERTY;
}

export class ToTablePropertyExpr extends PropertyExpr {
  key = ExpressionKey.TO_TABLE_PROPERTY;
}

export class ExecuteAsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.EXECUTE_AS_PROPERTY;
}

export class ExternalPropertyExpr extends PropertyExpr {
  key = ExpressionKey.EXTERNAL_PROPERTY;
}

export type FallbackPropertyExprArgs = { value?: string; no: Expression; protection?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class FallbackPropertyExpr extends PropertyExpr {
  key = ExpressionKey.FALLBACK_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for FallbackProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    no: true,
    protection: false,
  };

  constructor (args: FallbackPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get no (): Expression {
    return this.args.no as Expression;
  }

  get protection (): Expression {
    return this.args.protection as Expression;
  }
}

export type FileFormatPropertyExprArgs = { value?: string; hiveFormat?: string; [key: string]: unknown } & PropertyExprArgs;

export class FileFormatPropertyExpr extends PropertyExpr {
  key = ExpressionKey.FILE_FORMAT_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for FileFormatProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    hiveFormat: false,
  };

  constructor (args: FileFormatPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get hiveFormat (): string {
    return this.args.hiveFormat as string;
  }
}

export class CredentialsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.CREDENTIALS_PROPERTY;
}

export type FreespacePropertyExprArgs = { value?: string; percent?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class FreespacePropertyExpr extends PropertyExpr {
  key = ExpressionKey.FREESPACE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for FreespaceProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    percent: false,
  };

  constructor (args: FreespacePropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get percent (): Expression {
    return this.args.percent as Expression;
  }
}

export class GlobalPropertyExpr extends PropertyExpr {
  key = ExpressionKey.GLOBAL_PROPERTY;
}

export class IcebergPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ICEBERG_PROPERTY;
}

export class InheritsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.INHERITS_PROPERTY;
}

export class InputModelPropertyExpr extends PropertyExpr {
  key = ExpressionKey.INPUT_MODEL_PROPERTY;
}

export class OutputModelPropertyExpr extends PropertyExpr {
  key = ExpressionKey.OUTPUT_MODEL_PROPERTY;
}

export type IsolatedLoadingPropertyExprArgs = { value?: string; no?: Expression; concurrent?: Expression; target?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class IsolatedLoadingPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ISOLATED_LOADING_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for IsolatedLoadingProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    no: false,
    concurrent: false,
    target: false,
  };

  constructor (args: IsolatedLoadingPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get no (): Expression {
    return this.args.no as Expression;
  }

  get concurrent (): Expression {
    return this.args.concurrent as Expression;
  }

  get target (): Expression {
    return this.args.target as Expression;
  }
}

export type JournalPropertyExprArgs = { value?: string; no?: Expression; dual?: Expression; before?: Expression; local?: Expression; after?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class JournalPropertyExpr extends PropertyExpr {
  key = ExpressionKey.JOURNAL_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for JournalProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    no: false,
    dual: false,
    before: false,
    local: false,
    after: false,
  };

  constructor (args: JournalPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get no (): Expression {
    return this.args.no as Expression;
  }

  get dual (): Expression {
    return this.args.dual as Expression;
  }

  get before (): Expression {
    return this.args.before as Expression;
  }

  get local (): Expression {
    return this.args.local as Expression;
  }

  get after (): Expression {
    return this.args.after as Expression;
  }
}

export class LanguagePropertyExpr extends PropertyExpr {
  key = ExpressionKey.LANGUAGE_PROPERTY;
}

export class EnviromentPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ENVIROMENT_PROPERTY;
}

export type ClusteredByPropertyExprArgs = { value?: string; sortedBy?: string; buckets: Expression[]; [key: string]: unknown } & PropertyExprArgs;

export class ClusteredByPropertyExpr extends PropertyExpr {
  key = ExpressionKey.CLUSTERED_BY_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for ClusteredByProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    sortedBy: false,
    buckets: true,
  };

  constructor (args: ClusteredByPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get sortedBy (): string {
    return this.args.sortedBy as string;
  }

  get buckets (): Expression[] {
    return (this.args.buckets || []) as Expression[];
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

export type DictPropertyExprArgs = { value?: string; kind: DictPropertyExprKind; settings?: Expression[]; [key: string]: unknown } & PropertyExprArgs;

export class DictPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DICT_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for DictProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    kind: true,
    settings: false,
  };

  constructor (args: DictPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get kind (): DictPropertyExprKind | undefined {
    return this.args.kind as DictPropertyExprKind | undefined;
  }

  get settings (): Expression[] {
    return (this.args.settings || []) as Expression[];
  }
}

export class DictSubPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DICT_SUB_PROPERTY;
}

export type DictRangeExprArgs = { value?: string; min: Expression; max: Expression; [key: string]: unknown } & PropertyExprArgs;

export class DictRangeExpr extends PropertyExpr {
  key = ExpressionKey.DICT_RANGE;

  /**
   * Defines the arguments (properties and child expressions) for DictRange expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    min: true,
    max: true,
  };

  constructor (args: DictRangeExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get min (): Expression {
    return this.args.min as Expression;
  }

  get max (): Expression {
    return this.args.max as Expression;
  }
}

export class DynamicPropertyExpr extends PropertyExpr {
  key = ExpressionKey.DYNAMIC_PROPERTY;
}

export class OnClusterExpr extends PropertyExpr {
  key = ExpressionKey.ON_CLUSTER;
}

export class EmptyPropertyExpr extends PropertyExpr {
  key = ExpressionKey.EMPTY_PROPERTY;
}

export class LikePropertyExpr extends PropertyExpr {
  key = ExpressionKey.LIKE_PROPERTY;
}

export class LocationPropertyExpr extends PropertyExpr {
  key = ExpressionKey.LOCATION_PROPERTY;
}

export class LockPropertyExpr extends PropertyExpr {
  key = ExpressionKey.LOCK_PROPERTY;
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

export type LockingPropertyExprArgs = { value?: string; kind: LockingPropertyExprKind; forOrIn?: Expression; lockType: DataTypeExpr; override?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class LockingPropertyExpr extends PropertyExpr {
  key = ExpressionKey.LOCKING_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for LockingProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    kind: true,
    forOrIn: false,
    lockType: true,
    override: false,
  };

  constructor (args: LockingPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get kind (): LockingPropertyExprKind | undefined {
    return this.args.kind as LockingPropertyExprKind | undefined;
  }

  get forOrIn (): Expression {
    return this.args.forOrIn as Expression;
  }

  get lockType (): DataTypeExpr {
    return this.args.lockType as DataTypeExpr;
  }

  get override (): Expression {
    return this.args.override as Expression;
  }
}

export type LogPropertyExprArgs = { value?: string; no: Expression; [key: string]: unknown } & PropertyExprArgs;

export class LogPropertyExpr extends PropertyExpr {
  key = ExpressionKey.LOG_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for LogProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    no: true,
  };

  constructor (args: LogPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get no (): Expression {
    return this.args.no as Expression;
  }
}

export class MaterializedPropertyExpr extends PropertyExpr {
  key = ExpressionKey.MATERIALIZED_PROPERTY;
}

export type MergeBlockRatioPropertyExprArgs = { value?: string; no?: Expression; default?: Expression; percent?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class MergeBlockRatioPropertyExpr extends PropertyExpr {
  key = ExpressionKey.MERGE_BLOCK_RATIO_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for MergeBlockRatioProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    no: false,
    default: false,
    percent: false,
  };

  constructor (args: MergeBlockRatioPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get no (): Expression {
    return this.args.no as Expression;
  }

  get default (): Expression {
    return this.args['default'] as Expression;
  }

  get percent (): Expression {
    return this.args.percent as Expression;
  }
}

export class NoPrimaryIndexPropertyExpr extends PropertyExpr {
  key = ExpressionKey.NO_PRIMARY_INDEX_PROPERTY;
}

export class OnPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ON_PROPERTY;
}

export type OnCommitPropertyExprArgs = { value?: string; delete?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class OnCommitPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ON_COMMIT_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for OnCommitProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    delete: false,
  };

  constructor (args: OnCommitPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get delete (): Expression {
    return this.args['delete'] as Expression;
  }
}

export class PartitionedByPropertyExpr extends PropertyExpr {
  key = ExpressionKey.PARTITIONED_BY_PROPERTY;
}

export class PartitionedByBucketExpr extends PropertyExpr {
  key = ExpressionKey.PARTITIONED_BY_BUCKET;
}

export class PartitionByTruncateExpr extends PropertyExpr {
  key = ExpressionKey.PARTITION_BY_TRUNCATE;
}

export type PartitionByRangePropertyExprArgs = { value?: string; partitionExpressions: Expression[]; createExpressions: Expression[]; [key: string]: unknown } & PropertyExprArgs;

export class PartitionByRangePropertyExpr extends PropertyExpr {
  key = ExpressionKey.PARTITION_BY_RANGE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for PartitionByRangeProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    partitionExpressions: true,
    createExpressions: true,
  };

  constructor (args: PartitionByRangePropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get partitionExpressions (): Expression[] {
    return (this.args.partitionExpressions || []) as Expression[];
  }

  get createExpressions (): Expression[] {
    return (this.args.createExpressions || []) as Expression[];
  }
}

export class RollupPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ROLLUP_PROPERTY;
}

export type PartitionByListPropertyExprArgs = { value?: string; partitionExpressions: Expression[]; createExpressions: Expression[]; [key: string]: unknown } & PropertyExprArgs;

export class PartitionByListPropertyExpr extends PropertyExpr {
  key = ExpressionKey.PARTITION_BY_LIST_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for PartitionByListProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    partitionExpressions: true,
    createExpressions: true,
  };

  constructor (args: PartitionByListPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get partitionExpressions (): Expression[] {
    return (this.args.partitionExpressions || []) as Expression[];
  }

  get createExpressions (): Expression[] {
    return (this.args.createExpressions || []) as Expression[];
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
export type RefreshTriggerPropertyExprArgs = { value?: string; method?: string; kind?: RefreshTriggerPropertyExprKind; every?: Expression; unit?: Expression; starts?: Expression[]; [key: string]: unknown } & PropertyExprArgs;

export class RefreshTriggerPropertyExpr extends PropertyExpr {
  key = ExpressionKey.REFRESH_TRIGGER_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for RefreshTriggerProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    method: false,
    kind: false,
    every: false,
    unit: false,
    starts: false,
  };

  constructor (args: RefreshTriggerPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get method (): string {
    return this.args.method as string;
  }

  get kind (): RefreshTriggerPropertyExprKind | undefined {
    return this.args.kind as RefreshTriggerPropertyExprKind | undefined;
  }

  get every (): Expression {
    return this.args.every as Expression;
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }

  get starts (): Expression[] {
    return (this.args.starts || []) as Expression[];
  }
}

export class UniqueKeyPropertyExpr extends PropertyExpr {
  key = ExpressionKey.UNIQUE_KEY_PROPERTY;
}

export class PartitionedOfPropertyExpr extends PropertyExpr {
  key = ExpressionKey.PARTITIONED_OF_PROPERTY;
}

export class StreamingTablePropertyExpr extends PropertyExpr {
  key = ExpressionKey.STREAMING_TABLE_PROPERTY;
}

export class RemoteWithConnectionModelPropertyExpr extends PropertyExpr {
  key = ExpressionKey.REMOTE_WITH_CONNECTION_MODEL_PROPERTY;
}

export type ReturnsPropertyExprArgs = { value?: string; isTable?: Expression; table?: Expression; null?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class ReturnsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.RETURNS_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for ReturnsProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    isTable: false,
    table: false,
    null: false,
  };

  constructor (args: ReturnsPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get isTable (): Expression {
    return this.args.isTable as Expression;
  }

  get table (): Expression {
    return this.args.table as Expression;
  }

  get null (): Expression {
    return this.args.null as Expression;
  }
}

export class StrictPropertyExpr extends PropertyExpr {
  key = ExpressionKey.STRICT_PROPERTY;
}

export class RowFormatPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ROW_FORMAT_PROPERTY;
}

export type RowFormatDelimitedPropertyExprArgs = { value?: string; fields?: Expression[]; escaped?: Expression; collectionItems?: Expression[]; mapKeys?: Expression[]; lines?: Expression[]; null?: Expression; serde?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class RowFormatDelimitedPropertyExpr extends PropertyExpr {
  key = ExpressionKey.ROW_FORMAT_DELIMITED_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for RowFormatDelimitedProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    fields: false,
    escaped: false,
    collectionItems: false,
    mapKeys: false,
    lines: false,
    null: false,
    serde: false,
  };

  constructor (args: RowFormatDelimitedPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get fields (): Expression[] {
    return (this.args.fields || []) as Expression[];
  }

  get escaped (): Expression {
    return this.args.escaped as Expression;
  }

  get collectionItems (): Expression[] {
    return (this.args.collectionItems || []) as Expression[];
  }

  get mapKeys (): Expression[] {
    return (this.args.mapKeys || []) as Expression[];
  }

  get lines (): Expression[] {
    return (this.args.lines || []) as Expression[];
  }

  get null (): Expression {
    return this.args.null as Expression;
  }

  get serde (): Expression {
    return this.args.serde as Expression;
  }
}

export type RowFormatSerdePropertyExprArgs = { value?: string; serdeProperties?: Expression[]; [key: string]: unknown } & PropertyExprArgs;

export class RowFormatSerdePropertyExpr extends PropertyExpr {
  key = ExpressionKey.ROW_FORMAT_SERDE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for RowFormatSerdeProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    serdeProperties: false,
  };

  constructor (args: RowFormatSerdePropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get serdeProperties (): Expression[] {
    return (this.args.serdeProperties || []) as Expression[];
  }
}

export class SamplePropertyExpr extends PropertyExpr {
  key = ExpressionKey.SAMPLE_PROPERTY;
}

export class SecurityPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SECURITY_PROPERTY;
}

export class SchemaCommentPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SCHEMA_COMMENT_PROPERTY;
}

export type SerdePropertiesExprArgs = { value?: string; with?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class SerdePropertiesExpr extends PropertyExpr {
  key = ExpressionKey.SERDE_PROPERTIES;

  /**
   * Defines the arguments (properties and child expressions) for SerdeProperties expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    with: false,
  };

  constructor (args: SerdePropertiesExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get with (): Expression {
    return this.args['with'] as Expression;
  }
}

export type SetPropertyExprArgs = { value?: string; multi: Expression; [key: string]: unknown } & PropertyExprArgs;

export class SetPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SET_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for SetProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    multi: true,
  };

  constructor (args: SetPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get multi (): Expression {
    return this.args.multi as Expression;
  }
}

export class SharingPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SHARING_PROPERTY;
}

export class SetConfigPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SET_CONFIG_PROPERTY;
}

export class SettingsPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SETTINGS_PROPERTY;
}

export type SortKeyPropertyExprArgs = { value?: string; compound?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class SortKeyPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SORT_KEY_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for SortKeyProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    compound: false,
  };

  constructor (args: SortKeyPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get compound (): Expression {
    return this.args.compound as Expression;
  }
}

export class SqlReadWritePropertyExpr extends PropertyExpr {
  key = ExpressionKey.SQL_READ_WRITE_PROPERTY;
}

export class SqlSecurityPropertyExpr extends PropertyExpr {
  key = ExpressionKey.SQL_SECURITY_PROPERTY;
}

export class StabilityPropertyExpr extends PropertyExpr {
  key = ExpressionKey.STABILITY_PROPERTY;
}

export class StorageHandlerPropertyExpr extends PropertyExpr {
  key = ExpressionKey.STORAGE_HANDLER_PROPERTY;
}

export class TemporaryPropertyExpr extends PropertyExpr {
  key = ExpressionKey.TEMPORARY_PROPERTY;
}

export class SecurePropertyExpr extends PropertyExpr {
  key = ExpressionKey.SECURE_PROPERTY;
}

export class TagsExpr extends ColumnConstraintKindExpr {
  key = ExpressionKey.TAGS;
}

export class TransformModelPropertyExpr extends PropertyExpr {
  key = ExpressionKey.TRANSFORM_MODEL_PROPERTY;
}

export class TransientPropertyExpr extends PropertyExpr {
  key = ExpressionKey.TRANSIENT_PROPERTY;
}

export class UnloggedPropertyExpr extends PropertyExpr {
  key = ExpressionKey.UNLOGGED_PROPERTY;
}

export class UsingTemplatePropertyExpr extends PropertyExpr {
  key = ExpressionKey.USING_TEMPLATE_PROPERTY;
}

export class ViewAttributePropertyExpr extends PropertyExpr {
  key = ExpressionKey.VIEW_ATTRIBUTE_PROPERTY;
}

export class VolatilePropertyExpr extends PropertyExpr {
  key = ExpressionKey.VOLATILE_PROPERTY;
}

export type WithDataPropertyExprArgs = { value?: string; no: Expression; statistics?: Expression[]; [key: string]: unknown } & PropertyExprArgs;

export class WithDataPropertyExpr extends PropertyExpr {
  key = ExpressionKey.WITH_DATA_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for WithDataProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    no: true,
    statistics: false,
  };

  constructor (args: WithDataPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get no (): Expression {
    return this.args.no as Expression;
  }

  get statistics (): Expression[] {
    return (this.args.statistics || []) as Expression[];
  }
}

export class WithJournalTablePropertyExpr extends PropertyExpr {
  key = ExpressionKey.WITH_JOURNAL_TABLE_PROPERTY;
}

export class WithSchemaBindingPropertyExpr extends PropertyExpr {
  key = ExpressionKey.WITH_SCHEMA_BINDING_PROPERTY;
}

export type WithSystemVersioningPropertyExprArgs = { value?: string; on?: Expression; dataConsistency?: Expression; retentionPeriod?: Expression; with: Expression; [key: string]: unknown } & PropertyExprArgs;

export class WithSystemVersioningPropertyExpr extends PropertyExpr {
  key = ExpressionKey.WITH_SYSTEM_VERSIONING_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for WithSystemVersioningProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    on: false,
    dataConsistency: false,
    retentionPeriod: false,
    with: true,
  };

  constructor (args: WithSystemVersioningPropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get on (): Expression {
    return this.args.on as Expression;
  }

  get dataConsistency (): Expression {
    return this.args.dataConsistency as Expression;
  }

  get retentionPeriod (): Expression {
    return this.args.retentionPeriod as Expression;
  }

  get with (): Expression {
    return this.args['with'] as Expression;
  }
}

export class WithProcedureOptionsExpr extends PropertyExpr {
  key = ExpressionKey.WITH_PROCEDURE_OPTIONS;
}

export type EncodePropertyExprArgs = { value?: string; properties?: Expression[]; key?: unknown; [key: string]: unknown } & PropertyExprArgs;

export class EncodePropertyExpr extends PropertyExpr {
  key = ExpressionKey.ENCODE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for EncodeProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    properties: false,
    key: false,
  };

  constructor (args: EncodePropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get properties (): Expression[] {
    return (this.args.properties || []) as Expression[];
  }
}

export type IncludePropertyExprArgs = { value?: string; columnDef?: Expression; [key: string]: unknown } & PropertyExprArgs;

export class IncludePropertyExpr extends PropertyExpr {
  key = ExpressionKey.INCLUDE_PROPERTY;

  /**
   * Defines the arguments (properties and child expressions) for IncludeProperty expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    value: false,
    columnDef: false,
  };

  constructor (args: IncludePropertyExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get columnDef (): Expression {
    return this.args.columnDef as Expression;
  }
}

export class ForcePropertyExpr extends PropertyExpr {
  key = ExpressionKey.FORCE_PROPERTY;
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

export type SetOperationExprArgs = { with?: Expression; distinct?: boolean; byName?: string; side?: Expression; kind?: SetOperationExprKind; on?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class SetOperationExpr extends QueryExpr {
  key = ExpressionKey.SET_OPERATION;

  /**
   * Defines the arguments (properties and child expressions) for SetOperation expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    with: false,
    distinct: false,
    byName: false,
    side: false,
    kind: false,
    on: false,
  };

  constructor (args: SetOperationExprArgs = {}) {
    super(args);
  }

  get with (): Expression {
    return this.args['with'] as Expression;
  }

  get distinct (): boolean {
    return this.args.distinct as boolean;
  }

  get byName (): string {
    return this.args.byName as string;
  }

  get side (): Expression {
    return this.args.side as Expression;
  }

  get kind (): SetOperationExprKind | undefined {
    return this.args.kind as SetOperationExprKind | undefined;
  }

  get on (): Expression {
    return this.args.on as Expression;
  }
}

export type UpdateExprArgs = { with?: Expression; from?: Expression; where?: Expression; returning?: Expression; order?: Expression; limit?: number | Expression; options?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class UpdateExpr extends DMLExpr {
  key = ExpressionKey.UPDATE;

  /**
   * Defines the arguments (properties and child expressions) for Update expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    with: false,
    from: false,
    where: false,
    returning: false,
    order: false,
    limit: false,
    options: false,
  };

  constructor (args: UpdateExprArgs = {}) {
    super(args);
  }

  get with (): Expression {
    return this.args['with'] as Expression;
  }

  get from (): Expression {
    return this.args.from as Expression;
  }

  get where (): Expression {
    return this.args.where as Expression;
  }

  get returning (): Expression {
    return this.args.returning as Expression;
  }

  get order (): Expression {
    return this.args.order as Expression;
  }

  get limit (): number | Expression {
    return this.args.limit as number | Expression;
  }

  get options (): Expression[] {
    return (this.args.options || []) as Expression[];
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
export type SelectExprArgs = { with?: Expression; kind?: SelectExprKind; hint?: Expression; distinct?: boolean; into?: Expression; from?: Expression; operationModifiers?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class SelectExpr extends QueryExpr {
  key = ExpressionKey.SELECT;

  /**
   * Defines the arguments (properties and child expressions) for Select expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    with: false,
    kind: false,
    hint: false,
    distinct: false,
    into: false,
    from: false,
    operationModifiers: false,
  };

  constructor (args: SelectExprArgs = {}) {
    super(args);
  }

  get with (): Expression {
    return this.args['with'] as Expression;
  }

  get kind (): SelectExprKind | undefined {
    return this.args.kind as SelectExprKind | undefined;
  }

  get hint (): Expression {
    return this.args.hint as Expression;
  }

  get distinct (): boolean {
    return this.args.distinct as boolean;
  }

  get into (): Expression {
    return this.args.into as Expression;
  }

  get from (): Expression {
    return this.args.from as Expression;
  }

  get operationModifiers (): Expression[] {
    return (this.args.operationModifiers || []) as Expression[];
  }
}

export type SubqueryExprArgs = { with?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class SubqueryExpr extends DerivedTableExpr {
  key = ExpressionKey.SUBQUERY;

  /**
   * Defines the arguments (properties and child expressions) for Subquery expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    with: false,
  };

  constructor (args: SubqueryExprArgs = {}) {
    super(args);
  }

  get with (): Expression {
    return this.args['with'] as Expression;
  }
}

export type WindowExprArgs = { partitionBy?: Expression; order?: Expression; spec?: Expression; over?: Expression; first?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class WindowExpr extends Expression {
  key = ExpressionKey.WINDOW;

  /**
   * Defines the arguments (properties and child expressions) for Window expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    partitionBy: false,
    order: false,
    spec: false,
    over: false,
    first: false,
  };

  constructor (args: WindowExprArgs = {}) {
    super(args);
  }

  get partitionBy (): Expression {
    return this.args.partitionBy as Expression;
  }

  get order (): Expression {
    return this.args.order as Expression;
  }

  get spec (): Expression {
    return this.args.spec as Expression;
  }

  get over (): Expression {
    return this.args.over as Expression;
  }

  get first (): Expression {
    return this.args.first as Expression;
  }
}

export class ParameterExpr extends Expression {
  key = ExpressionKey.PARAMETER;
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
export type SessionParameterExprArgs = { kind?: SessionParameterExprKind; [key: string]: unknown } & BaseExpressionArgs;

export class SessionParameterExpr extends Expression {
  key = ExpressionKey.SESSION_PARAMETER;

  /**
   * Defines the arguments (properties and child expressions) for SessionParameter expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: false,
  };

  constructor (args: SessionParameterExprArgs = {}) {
    super(args);
  }

  get kind (): SessionParameterExprKind | undefined {
    return this.args.kind as SessionParameterExprKind | undefined;
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
export type PlaceholderExprArgs = { kind?: PlaceholderExprKind; widget?: Expression; jdbc?: string; [key: string]: unknown } & BaseExpressionArgs;

export class PlaceholderExpr extends Expression {
  key = ExpressionKey.PLACEHOLDER;

  /**
   * Defines the arguments (properties and child expressions) for Placeholder expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    kind: false,
    widget: false,
    jdbc: false,
  };

  constructor (args: PlaceholderExprArgs = {}) {
    super(args);
  }

  get kind (): PlaceholderExprKind | undefined {
    return this.args.kind as PlaceholderExprKind | undefined;
  }

  get widget (): Expression {
    return this.args.widget as Expression;
  }

  get jdbc (): string {
    return this.args.jdbc as string;
  }
}

export class NullExpr extends Expression {
  key = ExpressionKey.NULL;
}

export class BooleanExpr extends Expression {
  key = ExpressionKey.BOOLEAN;
}

export class PseudoTypeExpr extends DataTypeExpr {
  key = ExpressionKey.PSEUDO_TYPE;
}

export class ObjectIdentifierExpr extends DataTypeExpr {
  key = ExpressionKey.OBJECT_IDENTIFIER;
}

export class BinaryExpr extends Expression {
  key = ExpressionKey.BINARY;
}

export class UnaryExpr extends Expression {
  key = ExpressionKey.UNARY;
}

export class PivotAliasExpr extends AliasExpr {
  key = ExpressionKey.PIVOT_ALIAS;
}

export type BracketExprArgs = { offset?: boolean; safe?: boolean; returnsListForMaps?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class BracketExpr extends Expression {
  key = ExpressionKey.BRACKET;

  /**
   * Defines the arguments (properties and child expressions) for Bracket expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    offset: false,
    safe: false,
    returnsListForMaps: false,
  };

  constructor (args: BracketExprArgs = {}) {
    super(args);
  }

  get offset (): boolean {
    return this.args.offset as boolean;
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }

  get returnsListForMaps (): Expression[] {
    return (this.args.returnsListForMaps || []) as Expression[];
  }
}

export type IntervalOpExprArgs = { unit?: Expression; [key: string]: unknown } & TimeUnitExprArgs;

export class IntervalOpExpr extends TimeUnitExpr {
  key = ExpressionKey.INTERVAL_OP;

  /**
   * Defines the arguments (properties and child expressions) for IntervalOp expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
  };

  constructor (args: IntervalOpExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }
}

export class IntervalSpanExpr extends DataTypeExpr {
  key = ExpressionKey.INTERVAL_SPAN;
}

export type IntervalExprArgs = { unit?: Expression; [key: string]: unknown } & TimeUnitExprArgs;

export class IntervalExpr extends TimeUnitExpr {
  key = ExpressionKey.INTERVAL;

  /**
   * Defines the arguments (properties and child expressions) for Interval expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
  };

  constructor (args: IntervalExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }
}

// Global registry for function classes (populated by self-registration)
const _functionRegistry = new Map<string, typeof FuncExpr>();
const _allFunctions = new Set<typeof FuncExpr>();

export class FuncExpr extends Expression {
  key = ExpressionKey.FUNC;

  /**
   * If set to true, the last argument defined in argTypes will be treated as a
   * variable length argument and the argument's value will be stored as a list.
   * This is used for functions like CONCAT, COALESCE that can accept any number of arguments.
   */
  static isVarLenArgs = false;

  /**
   * SQL names for this function (first is primary, rest are aliases)
   * Override this in subclasses to specify custom SQL names
   * If not specified, defaults to snake_case version of class name
   */
  static sqlNames?: string[];

  /**
   * Get the SQL names for this function class
   * @returns Array of SQL names (primary name first, then aliases)
   */
  static getSqlNames (): string[] {
    if (this.sqlNames) {
      return this.sqlNames;
    }

    // Auto-generate from class name: convert camelCase to SNAKE_CASE
    // e.g., CoalesceExpr -> COALESCE, ArraySizeExpr -> ARRAY_SIZE
    const className = this.name.replace(/Expr$/, ''); // Remove Expr suffix
    const snakeCase = className
      .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2')
      .replace(/([a-z\d])([A-Z])/g, '$1_$2')
      .toUpperCase();

    return [snakeCase];
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
    const sqlNames = this.getSqlNames();
    for (const name of sqlNames) {
      _functionRegistry.set(name.toUpperCase(), this);
    }
  }
}

export class JSONPathFilterExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_FILTER;
}

export class JSONPathKeyExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_KEY;
}

export class JSONPathRecursiveExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_RECURSIVE;
}

export class JSONPathRootExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_ROOT;
}

export class JSONPathScriptExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_SCRIPT;
}

export type JSONPathSliceExprArgs = { start?: Expression; end?: Expression; step?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONPathSliceExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_SLICE;

  /**
   * Defines the arguments (properties and child expressions) for JSONPathSlice expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    start: false,
    end: false,
    step: false,
  };

  constructor (args: JSONPathSliceExprArgs = {}) {
    super(args);
  }

  get start (): Expression {
    return this.args.start as Expression;
  }

  get end (): Expression {
    return this.args.end as Expression;
  }

  get step (): Expression {
    return this.args.step as Expression;
  }
}

export class JSONPathSelectorExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_SELECTOR;
}

export class JSONPathSubscriptExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_SUBSCRIPT;
}

export class JSONPathUnionExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_UNION;
}

export class JSONPathWildcardExpr extends JSONPathPartExpr {
  key = ExpressionKey.JSON_PATH_WILDCARD;
}

export type MergeExprArgs = { using: string; on?: Expression; usingCond?: string; whens: Expression[]; with?: Expression; returning?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class MergeExpr extends DMLExpr {
  key = ExpressionKey.MERGE;

  /**
   * Defines the arguments (properties and child expressions) for Merge expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    using: true,
    on: false,
    usingCond: false,
    whens: true,
    with: false,
    returning: false,
  };

  constructor (args: MergeExprArgs) {
    super(args);
  }

  get using (): string {
    return this.args.using as string;
  }

  get on (): Expression {
    return this.args.on as Expression;
  }

  get usingCond (): string {
    return this.args.usingCond as string;
  }

  get whens (): Expression[] {
    return (this.args.whens || []) as Expression[];
  }

  get with (): Expression {
    return this.args['with'] as Expression;
  }

  get returning (): Expression {
    return this.args.returning as Expression;
  }
}

export class PseudocolumnExpr extends ColumnExpr {
  key = ExpressionKey.PSEUDOCOLUMN;
}

export type LateralExprArgs = { view?: Expression; outer?: Expression; crossApply?: boolean; ordinality?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class LateralExpr extends UDTFExpr {
  key = ExpressionKey.LATERAL;

  /**
   * Defines the arguments (properties and child expressions) for Lateral expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    view: false,
    outer: false,
    crossApply: false,
    ordinality: false,
  };

  constructor (args: LateralExprArgs = {}) {
    super(args);
  }

  get view (): Expression {
    return this.args.view as Expression;
  }

  get outer (): Expression {
    return this.args.outer as Expression;
  }

  get crossApply (): boolean {
    return this.args.crossApply as boolean;
  }

  get ordinality (): boolean {
    return this.args.ordinality as boolean;
  }
}

export type TableFromRowsExprArgs = { joins?: Expression[]; pivots?: Expression[]; sample?: number | Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TableFromRowsExpr extends UDTFExpr {
  key = ExpressionKey.TABLE_FROM_ROWS;

  /**
   * Defines the arguments (properties and child expressions) for TableFromRows expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    joins: false,
    pivots: false,
    sample: false,
  };

  constructor (args: TableFromRowsExprArgs = {}) {
    super(args);
  }

  get joins (): Expression[] {
    return (this.args.joins || []) as Expression[];
  }

  get pivots (): Expression[] {
    return (this.args.pivots || []) as Expression[];
  }

  get sample (): number | Expression {
    return this.args.sample as number | Expression;
  }
}

export class UnionExpr extends SetOperationExpr {
  key = ExpressionKey.UNION;
}

export class ExceptExpr extends SetOperationExpr {
  key = ExpressionKey.EXCEPT;
}

export class IntersectExpr extends SetOperationExpr {
  key = ExpressionKey.INTERSECT;
}

export type ValuesExprArgs = { order?: Expression; limit?: number | Expression; offset?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class ValuesExpr extends UDTFExpr {
  key = ExpressionKey.VALUES;

  /**
   * Defines the arguments (properties and child expressions) for Values expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    order: false,
    limit: false,
    offset: false,
  };

  constructor (args: ValuesExprArgs = {}) {
    super(args);
  }

  get order (): Expression {
    return this.args.order as Expression;
  }

  get limit (): number | Expression {
    return this.args.limit as number | Expression;
  }

  get offset (): boolean {
    return this.args.offset as boolean;
  }
}

export class SubqueryPredicateExpr extends PredicateExpr {
  key = ExpressionKey.SUBQUERY_PREDICATE;
}

export class AddExpr extends BinaryExpr {
  key = ExpressionKey.ADD;
}

export class ConnectorExpr extends BinaryExpr {
  key = ExpressionKey.CONNECTOR;
}

export type BitwiseAndExprArgs = { padside?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class BitwiseAndExpr extends BinaryExpr {
  key = ExpressionKey.BITWISE_AND;

  /**
   * Defines the arguments (properties and child expressions) for BitwiseAnd expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    padside: false,
  };

  constructor (args: BitwiseAndExprArgs = {}) {
    super(args);
  }

  get padside (): Expression {
    return this.args.padside as Expression;
  }
}

export type BitwiseLeftShiftExprArgs = { requiresInt128?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class BitwiseLeftShiftExpr extends BinaryExpr {
  key = ExpressionKey.BITWISE_LEFT_SHIFT;

  /**
   * Defines the arguments (properties and child expressions) for BitwiseLeftShift expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    requiresInt128: false,
  };

  constructor (args: BitwiseLeftShiftExprArgs = {}) {
    super(args);
  }

  get requiresInt128 (): Expression {
    return this.args.requiresInt128 as Expression;
  }
}

export type BitwiseOrExprArgs = { padside?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class BitwiseOrExpr extends BinaryExpr {
  key = ExpressionKey.BITWISE_OR;

  /**
   * Defines the arguments (properties and child expressions) for BitwiseOr expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    padside: false,
  };

  constructor (args: BitwiseOrExprArgs = {}) {
    super(args);
  }

  get padside (): Expression {
    return this.args.padside as Expression;
  }
}

export type BitwiseRightShiftExprArgs = { requiresInt128?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class BitwiseRightShiftExpr extends BinaryExpr {
  key = ExpressionKey.BITWISE_RIGHT_SHIFT;

  /**
   * Defines the arguments (properties and child expressions) for BitwiseRightShift expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    requiresInt128: false,
  };

  constructor (args: BitwiseRightShiftExprArgs = {}) {
    super(args);
  }

  get requiresInt128 (): Expression {
    return this.args.requiresInt128 as Expression;
  }
}

export type BitwiseXorExprArgs = { padside?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class BitwiseXorExpr extends BinaryExpr {
  key = ExpressionKey.BITWISE_XOR;

  /**
   * Defines the arguments (properties and child expressions) for BitwiseXor expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    padside: false,
  };

  constructor (args: BitwiseXorExprArgs = {}) {
    super(args);
  }

  get padside (): Expression {
    return this.args.padside as Expression;
  }
}

export type DivExprArgs = { typed?: DataTypeExpr; safe?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class DivExpr extends BinaryExpr {
  key = ExpressionKey.DIV;

  /**
   * Defines the arguments (properties and child expressions) for Div expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    typed: false,
    safe: false,
  };

  constructor (args: DivExprArgs = {}) {
    super(args);
  }

  get typed (): DataTypeExpr {
    return this.args.typed as DataTypeExpr;
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }
}

export class OverlapsExpr extends BinaryExpr {
  key = ExpressionKey.OVERLAPS;
}

export class ExtendsLeftExpr extends BinaryExpr {
  key = ExpressionKey.EXTENDS_LEFT;
}

export class ExtendsRightExpr extends BinaryExpr {
  key = ExpressionKey.EXTENDS_RIGHT;
}

export class DotExpr extends BinaryExpr {
  key = ExpressionKey.DOT;
}

export type DPipeExprArgs = { safe?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class DPipeExpr extends BinaryExpr {
  key = ExpressionKey.D_PIPE;

  /**
   * Defines the arguments (properties and child expressions) for DPipe expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    safe: false,
  };

  constructor (args: DPipeExprArgs = {}) {
    super(args);
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }
}

export class EQExpr extends BinaryExpr {
  key = ExpressionKey.EQ;
}

export class NullSafeEQExpr extends BinaryExpr {
  key = ExpressionKey.NULL_SAFE_EQ;
}

export class NullSafeNEQExpr extends BinaryExpr {
  key = ExpressionKey.NULL_SAFE_NEQ;
}

export class PropertyEQExpr extends BinaryExpr {
  key = ExpressionKey.PROPERTY_EQ;
}

export class DistanceExpr extends BinaryExpr {
  key = ExpressionKey.DISTANCE;
}

export class EscapeExpr extends BinaryExpr {
  key = ExpressionKey.ESCAPE;
}

export class GlobExpr extends BinaryExpr {
  key = ExpressionKey.GLOB;
}

export class GTExpr extends BinaryExpr {
  key = ExpressionKey.GT;
}

export class GTEExpr extends BinaryExpr {
  key = ExpressionKey.GTE;
}

export class ILikeExpr extends BinaryExpr {
  key = ExpressionKey.ILIKE;
}

export class IntDivExpr extends BinaryExpr {
  key = ExpressionKey.INT_DIV;
}

export class IsExpr extends BinaryExpr {
  key = ExpressionKey.IS;
}

export class KwargExpr extends BinaryExpr {
  key = ExpressionKey.KWARG;
}

export class LikeExpr extends BinaryExpr {
  key = ExpressionKey.LIKE;
}

export class MatchExpr extends BinaryExpr {
  key = ExpressionKey.MATCH;
}

export class LTExpr extends BinaryExpr {
  key = ExpressionKey.LT;
}

export class LTEExpr extends BinaryExpr {
  key = ExpressionKey.LTE;
}

export class ModExpr extends BinaryExpr {
  key = ExpressionKey.MOD;
}

export class MulExpr extends BinaryExpr {
  key = ExpressionKey.MUL;
}

export class NEQExpr extends BinaryExpr {
  key = ExpressionKey.NEQ;
}

export type OperatorExprArgs = { operator: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class OperatorExpr extends BinaryExpr {
  key = ExpressionKey.OPERATOR;

  /**
   * Defines the arguments (properties and child expressions) for Operator expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    operator: true,
  };

  constructor (args: OperatorExprArgs) {
    super(args);
  }

  get operator (): Expression {
    return this.args.operator as Expression;
  }
}

export class SimilarToExpr extends BinaryExpr {
  key = ExpressionKey.SIMILAR_TO;
}

export class SubExpr extends BinaryExpr {
  key = ExpressionKey.SUB;
}

export class AdjacentExpr extends BinaryExpr {
  key = ExpressionKey.ADJACENT;
}

export class BitwiseNotExpr extends UnaryExpr {
  key = ExpressionKey.BITWISE_NOT;
}

export class NotExpr extends UnaryExpr {
  key = ExpressionKey.NOT;
}

export class ParenExpr extends UnaryExpr {
  key = ExpressionKey.PAREN;
}

export class NegExpr extends UnaryExpr {
  key = ExpressionKey.NEG;
}

export type BetweenExprArgs = { low: Expression; high: Expression; symmetric?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class BetweenExpr extends PredicateExpr {
  key = ExpressionKey.BETWEEN;

  /**
   * Defines the arguments (properties and child expressions) for Between expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    low: true,
    high: true,
    symmetric: false,
  };

  constructor (args: BetweenExprArgs) {
    super(args);
  }

  get low (): Expression {
    return this.args.low as Expression;
  }

  get high (): Expression {
    return this.args.high as Expression;
  }

  get symmetric (): Expression {
    return this.args.symmetric as Expression;
  }
}

export type InExprArgs = { query?: Expression; unnest?: UnnestExpr; field?: Expression; isGlobal?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class InExpr extends PredicateExpr {
  key = ExpressionKey.IN;

  /**
   * Defines the arguments (properties and child expressions) for In expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    query: false,
    unnest: false,
    field: false,
    isGlobal: false,
  };

  constructor (args: InExprArgs = {}) {
    super(args);
  }

  get query (): Expression {
    return this.args.query as Expression;
  }

  get unnest (): boolean {
    return this.args.unnest as boolean;
  }

  get field (): Expression {
    return this.args.field as Expression;
  }

  get isGlobal (): Expression {
    return this.args.isGlobal as Expression;
  }
}

export class SafeFuncExpr extends FuncExpr {
  key = ExpressionKey.SAFE_FUNC;
}

export class TypeofExpr extends FuncExpr {
  key = ExpressionKey.TYPEOF;
}

export class AcosExpr extends FuncExpr {
  key = ExpressionKey.ACOS;
}

export class AcoshExpr extends FuncExpr {
  key = ExpressionKey.ACOSH;
}

export class AsinExpr extends FuncExpr {
  key = ExpressionKey.ASIN;
}

export class AsinhExpr extends FuncExpr {
  key = ExpressionKey.ASINH;
}

export class AtanExpr extends FuncExpr {
  key = ExpressionKey.ATAN;
}

export class AtanhExpr extends FuncExpr {
  key = ExpressionKey.ATANH;
}

export class Atan2Expr extends FuncExpr {
  key = ExpressionKey.ATAN2;
}

export class CotExpr extends FuncExpr {
  key = ExpressionKey.COT;
}

export class CothExpr extends FuncExpr {
  key = ExpressionKey.COTH;
}

export class CosExpr extends FuncExpr {
  key = ExpressionKey.COS;
}

export class CscExpr extends FuncExpr {
  key = ExpressionKey.CSC;
}

export class CschExpr extends FuncExpr {
  key = ExpressionKey.CSCH;
}

export class SecExpr extends FuncExpr {
  key = ExpressionKey.SEC;
}

export class SechExpr extends FuncExpr {
  key = ExpressionKey.SECH;
}

export class SinExpr extends FuncExpr {
  key = ExpressionKey.SIN;
}

export class SinhExpr extends FuncExpr {
  key = ExpressionKey.SINH;
}

export class TanExpr extends FuncExpr {
  key = ExpressionKey.TAN;
}

export class TanhExpr extends FuncExpr {
  key = ExpressionKey.TANH;
}

export class DegreesExpr extends FuncExpr {
  key = ExpressionKey.DEGREES;
}

export class CoshExpr extends FuncExpr {
  key = ExpressionKey.COSH;
}

export class CosineDistanceExpr extends FuncExpr {
  key = ExpressionKey.COSINE_DISTANCE;
}

export class DotProductExpr extends FuncExpr {
  key = ExpressionKey.DOT_PRODUCT;
}

export class EuclideanDistanceExpr extends FuncExpr {
  key = ExpressionKey.EUCLIDEAN_DISTANCE;
}

export class ManhattanDistanceExpr extends FuncExpr {
  key = ExpressionKey.MANHATTAN_DISTANCE;
}

export class JarowinklerSimilarityExpr extends FuncExpr {
  key = ExpressionKey.JAROWINKLER_SIMILARITY;
}

export class AggFuncExpr extends FuncExpr {
  key = ExpressionKey.AGG_FUNC;
}

export class BitwiseCountExpr extends FuncExpr {
  key = ExpressionKey.BITWISE_COUNT;
}

export class BitmapBucketNumberExpr extends FuncExpr {
  key = ExpressionKey.BITMAP_BUCKET_NUMBER;
}

export class BitmapCountExpr extends FuncExpr {
  key = ExpressionKey.BITMAP_COUNT;
}

export class BitmapBitPositionExpr extends FuncExpr {
  key = ExpressionKey.BITMAP_BIT_POSITION;
}

export class ByteLengthExpr extends FuncExpr {
  key = ExpressionKey.BYTE_LENGTH;
}

export type BoolnotExprArgs = { roundInput?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class BoolnotExpr extends FuncExpr {
  key = ExpressionKey.BOOLNOT;

  /**
   * Defines the arguments (properties and child expressions) for Boolnot expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    roundInput: false,
  };

  constructor (args: BoolnotExprArgs = {}) {
    super(args);
  }

  get roundInput (): Expression {
    return this.args.roundInput as Expression;
  }
}

export type BoolandExprArgs = { roundInput?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class BoolandExpr extends FuncExpr {
  key = ExpressionKey.BOOLAND;

  /**
   * Defines the arguments (properties and child expressions) for Booland expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    roundInput: false,
  };

  constructor (args: BoolandExprArgs = {}) {
    super(args);
  }

  get roundInput (): Expression {
    return this.args.roundInput as Expression;
  }
}

export type BoolorExprArgs = { roundInput?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class BoolorExpr extends FuncExpr {
  key = ExpressionKey.BOOLOR;

  /**
   * Defines the arguments (properties and child expressions) for Boolor expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    roundInput: false,
  };

  constructor (args: BoolorExprArgs = {}) {
    super(args);
  }

  get roundInput (): Expression {
    return this.args.roundInput as Expression;
  }
}

export class JSONBoolExpr extends FuncExpr {
  key = ExpressionKey.JSON_BOOL;
}

export type ArrayRemoveExprArgs = { nullPropagation?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ArrayRemoveExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_REMOVE;

  /**
   * Defines the arguments (properties and child expressions) for ArrayRemove expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    nullPropagation: false,
  };

  constructor (args: ArrayRemoveExprArgs = {}) {
    super(args);
  }

  get nullPropagation (): Expression {
    return this.args.nullPropagation as Expression;
  }
}

export class AbsExpr extends FuncExpr {
  key = ExpressionKey.ABS;
}

export class ApproxTopKEstimateExpr extends FuncExpr {
  key = ExpressionKey.APPROX_TOP_K_ESTIMATE;
}

export class FarmFingerprintExpr extends FuncExpr {
  key = ExpressionKey.FARM_FINGERPRINT;
}

export class FlattenExpr extends FuncExpr {
  key = ExpressionKey.FLATTEN;
}

export class Float64Expr extends FuncExpr {
  key = ExpressionKey.FLOAT64;
}

export class TransformExpr extends FuncExpr {
  key = ExpressionKey.TRANSFORM;
}

export type TranslateExprArgs = { from: Expression; to: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TranslateExpr extends FuncExpr {
  key = ExpressionKey.TRANSLATE;

  /**
   * Defines the arguments (properties and child expressions) for Translate expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    from: true,
    to: true,
  };

  constructor (args: TranslateExprArgs) {
    super(args);
  }

  get from (): Expression {
    return this.args.from as Expression;
  }

  get to (): Expression {
    return this.args.to as Expression;
  }
}

export class AnonymousExpr extends FuncExpr {
  key = ExpressionKey.ANONYMOUS;
}

export class ApplyExpr extends FuncExpr {
  key = ExpressionKey.APPLY;
}

export type ArrayExprArgs = { bracketNotation?: Expression; structNameInheritance?: string; [key: string]: unknown } & BaseExpressionArgs;

export class ArrayExpr extends FuncExpr {
  key = ExpressionKey.ARRAY;

  /**
   * Defines the arguments (properties and child expressions) for Array expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    bracketNotation: false,
    structNameInheritance: false,
  };

  constructor (args: ArrayExprArgs = {}) {
    super(args);
  }

  get bracketNotation (): Expression {
    return this.args.bracketNotation as Expression;
  }

  get structNameInheritance (): string {
    return this.args.structNameInheritance as string;
  }
}

export class AsciiExpr extends FuncExpr {
  key = ExpressionKey.ASCII;
}

export class ToArrayExpr extends FuncExpr {
  key = ExpressionKey.TO_ARRAY;
}

export type ToBooleanExprArgs = { safe?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class ToBooleanExpr extends FuncExpr {
  key = ExpressionKey.TO_BOOLEAN;

  /**
   * Defines the arguments (properties and child expressions) for ToBoolean expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    safe: false,
  };

  constructor (args: ToBooleanExprArgs = {}) {
    super(args);
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }
}

export class ListExpr extends FuncExpr {
  key = ExpressionKey.LIST;
}

export type PadExprArgs = { fillPattern?: Expression; isLeft: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class PadExpr extends FuncExpr {
  key = ExpressionKey.PAD;

  /**
   * Defines the arguments (properties and child expressions) for Pad expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    fillPattern: false,
    isLeft: true,
  };

  constructor (args: PadExprArgs) {
    super(args);
  }

  get fillPattern (): Expression {
    return this.args.fillPattern as Expression;
  }

  get isLeft (): Expression {
    return this.args.isLeft as Expression;
  }
}

export type ToCharExprArgs = { format?: string; nlsparam?: Expression; isNumeric?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ToCharExpr extends FuncExpr {
  key = ExpressionKey.TO_CHAR;

  /**
   * Defines the arguments (properties and child expressions) for ToChar expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: false,
    nlsparam: false,
    isNumeric: false,
  };

  constructor (args: ToCharExprArgs = {}) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }

  get nlsparam (): Expression {
    return this.args.nlsparam as Expression;
  }

  get isNumeric (): Expression {
    return this.args.isNumeric as Expression;
  }
}

export class ToCodePointsExpr extends FuncExpr {
  key = ExpressionKey.TO_CODE_POINTS;
}

export type ToNumberExprArgs = { format?: string; nlsparam?: Expression; precision?: number | Expression; scale?: number | Expression; safe?: boolean; safeName?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class ToNumberExpr extends FuncExpr {
  key = ExpressionKey.TO_NUMBER;

  /**
   * Defines the arguments (properties and child expressions) for ToNumber expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: false,
    nlsparam: false,
    precision: false,
    scale: false,
    safe: false,
    safeName: false,
  };

  constructor (args: ToNumberExprArgs = {}) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }

  get nlsparam (): Expression {
    return this.args.nlsparam as Expression;
  }

  get precision (): number | Expression {
    return this.args.precision as number | Expression;
  }

  get scale (): number | Expression {
    return this.args.scale as number | Expression;
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }

  get safeName (): boolean {
    return this.args.safeName as boolean;
  }
}

export type ToDoubleExprArgs = { format?: string; safe?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class ToDoubleExpr extends FuncExpr {
  key = ExpressionKey.TO_DOUBLE;

  /**
   * Defines the arguments (properties and child expressions) for ToDouble expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: false,
    safe: false,
  };

  constructor (args: ToDoubleExprArgs = {}) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }
}

export type ToDecfloatExprArgs = { format?: string; [key: string]: unknown } & BaseExpressionArgs;

export class ToDecfloatExpr extends FuncExpr {
  key = ExpressionKey.TO_DECFLOAT;

  /**
   * Defines the arguments (properties and child expressions) for ToDecfloat expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: false,
  };

  constructor (args: ToDecfloatExprArgs = {}) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }
}

export type TryToDecfloatExprArgs = { format?: string; [key: string]: unknown } & BaseExpressionArgs;

export class TryToDecfloatExpr extends FuncExpr {
  key = ExpressionKey.TRY_TO_DECFLOAT;

  /**
   * Defines the arguments (properties and child expressions) for TryToDecfloat expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: false,
  };

  constructor (args: TryToDecfloatExprArgs = {}) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }
}

export type ToFileExprArgs = { path?: Expression; safe?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class ToFileExpr extends FuncExpr {
  key = ExpressionKey.TO_FILE;

  /**
   * Defines the arguments (properties and child expressions) for ToFile expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    path: false,
    safe: false,
  };

  constructor (args: ToFileExprArgs = {}) {
    super(args);
  }

  get path (): Expression {
    return this.args.path as Expression;
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }
}

export class CodePointsToBytesExpr extends FuncExpr {
  key = ExpressionKey.CODE_POINTS_TO_BYTES;
}

export type ColumnsExprArgs = { unpack?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ColumnsExpr extends FuncExpr {
  key = ExpressionKey.COLUMNS;

  /**
   * Defines the arguments (properties and child expressions) for Columns expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unpack: false,
  };

  constructor (args: ColumnsExprArgs = {}) {
    super(args);
  }

  get unpack (): Expression {
    return this.args.unpack as Expression;
  }
}

export type ConvertExprArgs = { style?: Expression; safe?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class ConvertExpr extends FuncExpr {
  key = ExpressionKey.CONVERT;

  /**
   * Defines the arguments (properties and child expressions) for Convert expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    style: false,
    safe: false,
  };

  constructor (args: ConvertExprArgs = {}) {
    super(args);
  }

  get style (): Expression {
    return this.args.style as Expression;
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }
}

export type ConvertToCharsetExprArgs = { dest: Expression; source?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ConvertToCharsetExpr extends FuncExpr {
  key = ExpressionKey.CONVERT_TO_CHARSET;

  /**
   * Defines the arguments (properties and child expressions) for ConvertToCharset expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    dest: true,
    source: false,
  };

  constructor (args: ConvertToCharsetExprArgs) {
    super(args);
  }

  get dest (): Expression {
    return this.args.dest as Expression;
  }

  get source (): Expression {
    return this.args.source as Expression;
  }
}

export type ConvertTimezoneExprArgs = { sourceTz?: Expression; targetTz: Expression; timestamp: Expression; options?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class ConvertTimezoneExpr extends FuncExpr {
  key = ExpressionKey.CONVERT_TIMEZONE;

  /**
   * Defines the arguments (properties and child expressions) for ConvertTimezone expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    sourceTz: false,
    targetTz: true,
    timestamp: true,
    options: false,
  };

  constructor (args: ConvertTimezoneExprArgs) {
    super(args);
  }

  get sourceTz (): Expression {
    return this.args.sourceTz as Expression;
  }

  get targetTz (): Expression {
    return this.args.targetTz as Expression;
  }

  get timestamp (): Expression {
    return this.args.timestamp as Expression;
  }

  get options (): Expression[] {
    return (this.args.options || []) as Expression[];
  }
}

export class CodePointsToStringExpr extends FuncExpr {
  key = ExpressionKey.CODE_POINTS_TO_STRING;
}

export type GenerateSeriesExprArgs = { start: Expression; end: Expression; step?: Expression; isEndExclusive?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class GenerateSeriesExpr extends FuncExpr {
  key = ExpressionKey.GENERATE_SERIES;

  /**
   * Defines the arguments (properties and child expressions) for GenerateSeries expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    start: true,
    end: true,
    step: false,
    isEndExclusive: false,
  };

  constructor (args: GenerateSeriesExprArgs) {
    super(args);
  }

  get start (): Expression {
    return this.args.start as Expression;
  }

  get end (): Expression {
    return this.args.end as Expression;
  }

  get step (): Expression {
    return this.args.step as Expression;
  }

  get isEndExclusive (): Expression {
    return this.args.isEndExclusive as Expression;
  }
}

export type GeneratorExprArgs = { rowcount?: Expression; timelimit?: number | Expression; [key: string]: unknown } & BaseExpressionArgs;

export class GeneratorExpr extends FuncExpr {
  key = ExpressionKey.GENERATOR;

  /**
   * Defines the arguments (properties and child expressions) for Generator expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    rowcount: false,
    timelimit: false,
  };

  constructor (args: GeneratorExprArgs = {}) {
    super(args);
  }

  get rowcount (): Expression {
    return this.args.rowcount as Expression;
  }

  get timelimit (): number | Expression {
    return this.args.timelimit as number | Expression;
  }
}

export type AIClassifyExprArgs = { categories: Expression[]; config?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class AIClassifyExpr extends FuncExpr {
  key = ExpressionKey.AI_CLASSIFY;

  /**
   * Defines the arguments (properties and child expressions) for AIClassify expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    categories: true,
    config: false,
  };

  constructor (args: AIClassifyExprArgs) {
    super(args);
  }

  get categories (): Expression[] {
    return (this.args.categories || []) as Expression[];
  }

  get config (): Expression {
    return this.args.config as Expression;
  }
}

export class ArrayAllExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_ALL;
}

export class ArrayAnyExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_ANY;
}

export type ArrayAppendExprArgs = { nullPropagation?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ArrayAppendExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_APPEND;

  /**
   * Defines the arguments (properties and child expressions) for ArrayAppend expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    nullPropagation: false,
  };

  constructor (args: ArrayAppendExprArgs = {}) {
    super(args);
  }

  get nullPropagation (): Expression {
    return this.args.nullPropagation as Expression;
  }
}

export type ArrayPrependExprArgs = { nullPropagation?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ArrayPrependExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_PREPEND;

  /**
   * Defines the arguments (properties and child expressions) for ArrayPrepend expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    nullPropagation: false,
  };

  constructor (args: ArrayPrependExprArgs = {}) {
    super(args);
  }

  get nullPropagation (): Expression {
    return this.args.nullPropagation as Expression;
  }
}

export type ArrayConcatExprArgs = { nullPropagation?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ArrayConcatExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_CONCAT;
  static sqlNames = ['ARRAY_CONCAT', 'ARRAY_CAT'];

  /**
   * Defines the arguments (properties and child expressions) for ArrayConcat expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    nullPropagation: false,
  };

  constructor (args: ArrayConcatExprArgs = {}) {
    super(args);
  }

  get nullPropagation (): Expression {
    return this.args.nullPropagation as Expression;
  }
}

export class ArrayCompactExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_COMPACT;
}

export type ArrayInsertExprArgs = { position: Expression; offset?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class ArrayInsertExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_INSERT;

  /**
   * Defines the arguments (properties and child expressions) for ArrayInsert expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    position: true,
    offset: false,
  };

  constructor (args: ArrayInsertExprArgs) {
    super(args);
  }

  get position (): Expression {
    return this.args.position as Expression;
  }

  get offset (): boolean {
    return this.args.offset as boolean;
  }
}

export type ArrayRemoveAtExprArgs = { position: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ArrayRemoveAtExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_REMOVE_AT;

  /**
   * Defines the arguments (properties and child expressions) for ArrayRemoveAt expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    position: true,
  };

  constructor (args: ArrayRemoveAtExprArgs) {
    super(args);
  }

  get position (): Expression {
    return this.args.position as Expression;
  }
}

export class ArrayConstructCompactExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_CONSTRUCT_COMPACT;
}

export type ArrayContainsExprArgs = { ensureVariant?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ArrayContainsExpr extends BinaryExpr {
  key = ExpressionKey.ARRAY_CONTAINS;
  static sqlNames = ['ARRAY_CONTAINS', 'ARRAY_HAS'];

  /**
   * Defines the arguments (properties and child expressions) for ArrayContains expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ensureVariant: false,
  };

  constructor (args: ArrayContainsExprArgs = {}) {
    super(args);
  }

  get ensureVariant (): Expression {
    return this.args.ensureVariant as Expression;
  }
}

export class ArrayContainsAllExpr extends BinaryExpr {
  key = ExpressionKey.ARRAY_CONTAINS_ALL;
  static sqlNames = ['ARRAY_CONTAINS_ALL', 'ARRAY_HAS_ALL'];
}

export class ArrayFilterExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_FILTER;
  static sqlNames = ['FILTER', 'ARRAY_FILTER'];
}

export class ArrayFirstExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_FIRST;
}

export class ArrayLastExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_LAST;
}

export class ArrayReverseExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_REVERSE;
}

export type ArraySliceExprArgs = { start: Expression; end?: Expression; step?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ArraySliceExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_SLICE;

  /**
   * Defines the arguments (properties and child expressions) for ArraySlice expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    start: true,
    end: false,
    step: false,
  };

  constructor (args: ArraySliceExprArgs) {
    super(args);
  }

  get start (): Expression {
    return this.args.start as Expression;
  }

  get end (): Expression {
    return this.args.end as Expression;
  }

  get step (): Expression {
    return this.args.step as Expression;
  }
}

export type ArrayToStringExprArgs = { null?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ArrayToStringExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_TO_STRING;
  static sqlNames = ['ARRAY_TO_STRING', 'ARRAY_JOIN'];

  /**
   * Defines the arguments (properties and child expressions) for ArrayToString expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    null: false,
  };

  constructor (args: ArrayToStringExprArgs = {}) {
    super(args);
  }

  get null (): Expression {
    return this.args.null as Expression;
  }
}

export class ArrayIntersectExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_INTERSECT;
}

export type StPointExprArgs = { null?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class StPointExpr extends FuncExpr {
  key = ExpressionKey.ST_POINT;

  /**
   * Defines the arguments (properties and child expressions) for StPoint expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    null: false,
  };

  constructor (args: StPointExprArgs = {}) {
    super(args);
  }

  get null (): Expression {
    return this.args.null as Expression;
  }
}

export type StDistanceExprArgs = { useSpheroid?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class StDistanceExpr extends FuncExpr {
  key = ExpressionKey.ST_DISTANCE;

  /**
   * Defines the arguments (properties and child expressions) for StDistance expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    useSpheroid: false,
  };

  constructor (args: StDistanceExprArgs = {}) {
    super(args);
  }

  get useSpheroid (): Expression {
    return this.args.useSpheroid as Expression;
  }
}

export type StringExprArgs = { zone?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class StringExpr extends FuncExpr {
  key = ExpressionKey.STRING;

  /**
   * Defines the arguments (properties and child expressions) for String expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    zone: false,
  };

  constructor (args: StringExprArgs = {}) {
    super(args);
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }
}

export type StringToArrayExprArgs = { null?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class StringToArrayExpr extends FuncExpr {
  key = ExpressionKey.STRING_TO_ARRAY;

  /**
   * Defines the arguments (properties and child expressions) for StringToArray expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    null: false,
  };

  constructor (args: StringToArrayExprArgs = {}) {
    super(args);
  }

  get null (): Expression {
    return this.args.null as Expression;
  }
}

export class ArrayOverlapsExpr extends BinaryExpr {
  key = ExpressionKey.ARRAY_OVERLAPS;
}

export class ArraySizeExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_SIZE;
  static sqlNames = ['ARRAY_SIZE', 'ARRAY_LENGTH'];
}

export class ArraySortExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_SORT;
}

export class ArraySumExpr extends FuncExpr {
  key = ExpressionKey.ARRAY_SUM;
}

export class ArraysZipExpr extends FuncExpr {
  key = ExpressionKey.ARRAYS_ZIP;
}

export type CaseExprArgs = { ifs: Expression[]; default?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class CaseExpr extends FuncExpr {
  key = ExpressionKey.CASE;

  /**
   * Defines the arguments (properties and child expressions) for Case expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ifs: true,
    default: false,
  };

  constructor (args: CaseExprArgs) {
    super(args);
  }

  get ifs (): Expression[] {
    return (this.args.ifs || []) as Expression[];
  }

  get default (): Expression {
    return this.args['default'] as Expression;
  }
}

export type CastExprArgs = { to: Expression; format?: string; safe?: boolean; action?: Expression; default?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class CastExpr extends FuncExpr {
  key = ExpressionKey.CAST;

  /**
   * Defines the arguments (properties and child expressions) for Cast expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    to: true,
    format: false,
    safe: false,
    action: false,
    default: false,
  };

  constructor (args: CastExprArgs) {
    super(args);
  }

  get to (): Expression {
    return this.args.to as Expression;
  }

  get format (): string {
    return this.args.format as string;
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }

  get action (): Expression {
    return this.args.action as Expression;
  }

  get default (): Expression {
    return this.args['default'] as Expression;
  }
}

export class JustifyDaysExpr extends FuncExpr {
  key = ExpressionKey.JUSTIFY_DAYS;
}

export class JustifyHoursExpr extends FuncExpr {
  key = ExpressionKey.JUSTIFY_HOURS;
}

export class JustifyIntervalExpr extends FuncExpr {
  key = ExpressionKey.JUSTIFY_INTERVAL;
}

export class TryExpr extends FuncExpr {
  key = ExpressionKey.TRY;
}

export type CastToStrTypeExprArgs = { to: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class CastToStrTypeExpr extends FuncExpr {
  key = ExpressionKey.CAST_TO_STR_TYPE;

  /**
   * Defines the arguments (properties and child expressions) for CastToStrType expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    to: true,
  };

  constructor (args: CastToStrTypeExprArgs) {
    super(args);
  }

  get to (): Expression {
    return this.args.to as Expression;
  }
}

export class CheckJsonExpr extends FuncExpr {
  key = ExpressionKey.CHECK_JSON;
}

export type CheckXmlExprArgs = { disableAutoConvert?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class CheckXmlExpr extends FuncExpr {
  key = ExpressionKey.CHECK_XML;

  /**
   * Defines the arguments (properties and child expressions) for CheckXml expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    disableAutoConvert: false,
  };

  constructor (args: CheckXmlExprArgs = {}) {
    super(args);
  }

  get disableAutoConvert (): Expression {
    return this.args.disableAutoConvert as Expression;
  }
}

export class CollateExpr extends BinaryExpr {
  key = ExpressionKey.COLLATE;
}

export class CollationExpr extends FuncExpr {
  key = ExpressionKey.COLLATION;
}

export type CeilExprArgs = { decimals?: Expression[]; to?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class CeilExpr extends FuncExpr {
  key = ExpressionKey.CEIL;
  static sqlNames = ['CEIL', 'CEILING'];

  /**
   * Defines the arguments (properties and child expressions) for Ceil expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    decimals: false,
    to: false,
  };

  // Auto-register this class when the module loads
  static {
    this.register();
  }

  constructor (args: CeilExprArgs = {}) {
    super(args);
  }

  get decimals (): Expression[] {
    return (this.args.decimals || []) as Expression[];
  }

  get to (): Expression {
    return this.args.to as Expression;
  }
}

export type CoalesceExprArgs = { isNvl?: Expression; isNull?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class CoalesceExpr extends FuncExpr {
  key = ExpressionKey.COALESCE;
  static sqlNames = ['COALESCE', 'IFNULL', 'NVL'];

  /**
   * Defines the arguments (properties and child expressions) for Coalesce expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    isNvl: false,
    isNull: false,
  };

  constructor (args: CoalesceExprArgs = {}) {
    super(args);
  }

  get isNvl (): Expression {
    return this.args.isNvl as Expression;
  }

  get isNull (): Expression {
    return this.args.isNull as Expression;
  }
}

export type ChrExprArgs = { charset?: string; [key: string]: unknown } & BaseExpressionArgs;

export class ChrExpr extends FuncExpr {
  key = ExpressionKey.CHR;
  static sqlNames = ['CHR', 'CHAR'];

  /**
   * Defines the arguments (properties and child expressions) for Chr expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    charset: false,
  };

  constructor (args: ChrExprArgs = {}) {
    super(args);
  }

  get charset (): string {
    return this.args.charset as string;
  }
}

export type ConcatExprArgs = { safe?: boolean; coalesce?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ConcatExpr extends FuncExpr {
  key = ExpressionKey.CONCAT;

  /**
   * Defines the arguments (properties and child expressions) for Concat expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    safe: false,
    coalesce: false,
  };

  constructor (args: ConcatExprArgs = {}) {
    super(args);
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }

  get coalesce (): Expression {
    return this.args.coalesce as Expression;
  }
}

export type ContainsExprArgs = { jsonScope?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ContainsExpr extends FuncExpr {
  key = ExpressionKey.CONTAINS;

  /**
   * Defines the arguments (properties and child expressions) for Contains expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    jsonScope: false,
  };

  constructor (args: ContainsExprArgs = {}) {
    super(args);
  }

  get jsonScope (): Expression {
    return this.args.jsonScope as Expression;
  }
}

export class ConnectByRootExpr extends FuncExpr {
  key = ExpressionKey.CONNECT_BY_ROOT;
}

export class CbrtExpr extends FuncExpr {
  key = ExpressionKey.CBRT;
}

export class CurrentAccountExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ACCOUNT;
}

export class CurrentAccountNameExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ACCOUNT_NAME;
}

export class CurrentAvailableRolesExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_AVAILABLE_ROLES;
}

export class CurrentClientExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_CLIENT;
}

export class CurrentIpAddressExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_IP_ADDRESS;
}

export class CurrentDatabaseExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_DATABASE;
}

export class CurrentSchemasExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_SCHEMAS;
}

export class CurrentSecondaryRolesExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_SECONDARY_ROLES;
}

export class CurrentSessionExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_SESSION;
}

export class CurrentStatementExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_STATEMENT;
}

export class CurrentVersionExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_VERSION;
}

export class CurrentTransactionExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_TRANSACTION;
}

export class CurrentWarehouseExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_WAREHOUSE;
}

export class CurrentDateExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_DATE;
}

export class CurrentDatetimeExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_DATETIME;
}

export class CurrentTimeExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_TIME;
}

export class LocaltimeExpr extends FuncExpr {
  key = ExpressionKey.LOCALTIME;
}

export class LocaltimestampExpr extends FuncExpr {
  key = ExpressionKey.LOCALTIMESTAMP;
}

export class SystimestampExpr extends FuncExpr {
  key = ExpressionKey.SYSTIMESTAMP;
}

export type CurrentTimestampExprArgs = { sysdate?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class CurrentTimestampExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_TIMESTAMP;

  /**
   * Defines the arguments (properties and child expressions) for CurrentTimestamp expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    sysdate: false,
  };

  constructor (args: CurrentTimestampExprArgs = {}) {
    super(args);
  }

  get sysdate (): Expression {
    return this.args.sysdate as Expression;
  }
}

export class CurrentTimestampLTZExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_TIMESTAMP_LTZ;
}

export class CurrentTimezoneExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_TIMEZONE;
}

export class CurrentOrganizationNameExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ORGANIZATION_NAME;
}

export class CurrentSchemaExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_SCHEMA;
}

export class CurrentUserExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_USER;
}

export class CurrentCatalogExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_CATALOG;
}

export class CurrentRegionExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_REGION;
}

export class CurrentRoleExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ROLE;
}

export class CurrentRoleTypeExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ROLE_TYPE;
}

export class CurrentOrganizationUserExpr extends FuncExpr {
  key = ExpressionKey.CURRENT_ORGANIZATION_USER;
}

export class SessionUserExpr extends FuncExpr {
  key = ExpressionKey.SESSION_USER;
}

export class UtcDateExpr extends FuncExpr {
  key = ExpressionKey.UTC_DATE;
}

export class UtcTimeExpr extends FuncExpr {
  key = ExpressionKey.UTC_TIME;
}

export class UtcTimestampExpr extends FuncExpr {
  key = ExpressionKey.UTC_TIMESTAMP;
}

export type DateAddExprArgs = { unit?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DateAddExpr extends FuncExpr {
  key = ExpressionKey.DATE_ADD;

  /**
   * Defines the arguments (properties and child expressions) for DateAdd expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
  };

  constructor (args: DateAddExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }
}

export type DateBinExprArgs = { unit?: Expression; zone?: Expression; origin?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DateBinExpr extends FuncExpr {
  key = ExpressionKey.DATE_BIN;

  /**
   * Defines the arguments (properties and child expressions) for DateBin expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
    zone: false,
    origin: false,
  };

  constructor (args: DateBinExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }

  get origin (): Expression {
    return this.args.origin as Expression;
  }
}

export type DateSubExprArgs = { unit?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DateSubExpr extends FuncExpr {
  key = ExpressionKey.DATE_SUB;

  /**
   * Defines the arguments (properties and child expressions) for DateSub expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
  };

  constructor (args: DateSubExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }
}

export type DateDiffExprArgs = { unit?: Expression; zone?: Expression; bigInt?: Expression; datePartBoundary?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DateDiffExpr extends FuncExpr {
  key = ExpressionKey.DATE_DIFF;
  static sqlNames = ['DATEDIFF', 'DATE_DIFF'];

  /**
   * Defines the arguments (properties and child expressions) for DateDiff expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
    zone: false,
    bigInt: false,
    datePartBoundary: false,
  };

  constructor (args: DateDiffExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }

  get bigInt (): Expression {
    return this.args.bigInt as Expression;
  }

  get datePartBoundary (): Expression {
    return this.args.datePartBoundary as Expression;
  }
}

export type DateTruncExprArgs = { unit: Expression; zone?: Expression; inputTypePreserved?: DataTypeExpr; [key: string]: unknown } & BaseExpressionArgs;

export class DateTruncExpr extends FuncExpr {
  key = ExpressionKey.DATE_TRUNC;

  /**
   * Defines the arguments (properties and child expressions) for DateTrunc expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: true,
    zone: false,
    inputTypePreserved: false,
  };

  constructor (args: DateTruncExprArgs) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }

  get inputTypePreserved (): DataTypeExpr {
    return this.args.inputTypePreserved as DataTypeExpr;
  }
}

export class DatetimeExpr extends FuncExpr {
  key = ExpressionKey.DATETIME;
}

export type DatetimeAddExprArgs = { unit?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DatetimeAddExpr extends FuncExpr {
  key = ExpressionKey.DATETIME_ADD;

  /**
   * Defines the arguments (properties and child expressions) for DatetimeAdd expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
  };

  constructor (args: DatetimeAddExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }
}

export type DatetimeSubExprArgs = { unit?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DatetimeSubExpr extends FuncExpr {
  key = ExpressionKey.DATETIME_SUB;

  /**
   * Defines the arguments (properties and child expressions) for DatetimeSub expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
  };

  constructor (args: DatetimeSubExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }
}

export type DatetimeDiffExprArgs = { unit?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DatetimeDiffExpr extends FuncExpr {
  key = ExpressionKey.DATETIME_DIFF;

  /**
   * Defines the arguments (properties and child expressions) for DatetimeDiff expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
  };

  constructor (args: DatetimeDiffExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }
}

export type DatetimeTruncExprArgs = { unit: Expression; zone?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DatetimeTruncExpr extends FuncExpr {
  key = ExpressionKey.DATETIME_TRUNC;

  /**
   * Defines the arguments (properties and child expressions) for DatetimeTrunc expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: true,
    zone: false,
  };

  constructor (args: DatetimeTruncExprArgs) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }
}

export class DateFromUnixDateExpr extends FuncExpr {
  key = ExpressionKey.DATE_FROM_UNIX_DATE;
}

export class DayOfWeekExpr extends FuncExpr {
  key = ExpressionKey.DAY_OF_WEEK;

  static {
    this.register();
  }

  static sqlNames = ['DAY_OF_WEEK', 'DAYOFWEEK'];
}

export class DayOfWeekIsoExpr extends FuncExpr {
  key = ExpressionKey.DAY_OF_WEEK_ISO;

  static {
    this.register();
  }
}

export class DayOfMonthExpr extends FuncExpr {
  key = ExpressionKey.DAY_OF_MONTH;
  static sqlNames = ['DAY_OF_MONTH', 'DAYOFMONTH'];
}

export class DayOfYearExpr extends FuncExpr {
  key = ExpressionKey.DAY_OF_YEAR;
  static sqlNames = ['DAY_OF_YEAR', 'DAYOFYEAR'];
}

export type DaynameExprArgs = { abbreviated?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DaynameExpr extends FuncExpr {
  key = ExpressionKey.DAYNAME;

  /**
   * Defines the arguments (properties and child expressions) for Dayname expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    abbreviated: false,
  };

  static {
    this.register();
  }

  static {
    this.register();
  }

  constructor (args: DaynameExprArgs = {}) {
    super(args);
  }

  get abbreviated (): Expression {
    return this.args.abbreviated as Expression;
  }
}

export class ToDaysExpr extends FuncExpr {
  key = ExpressionKey.TO_DAYS;
}

export class WeekOfYearExpr extends FuncExpr {
  key = ExpressionKey.WEEK_OF_YEAR;

  static {
    this.register();
  }

  static sqlNames = ['WEEK_OF_YEAR', 'WEEKOFYEAR'];
}

export class YearOfWeekExpr extends FuncExpr {
  key = ExpressionKey.YEAR_OF_WEEK;
  static sqlNames = ['YEAR_OF_WEEK', 'YEAROFWEEK'];
}

export class YearOfWeekIsoExpr extends FuncExpr {
  key = ExpressionKey.YEAR_OF_WEEK_ISO;
  static sqlNames = ['YEAR_OF_WEEK_ISO', 'YEAROFWEEKISO'];
}

export type MonthsBetweenExprArgs = { roundoff?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class MonthsBetweenExpr extends FuncExpr {
  key = ExpressionKey.MONTHS_BETWEEN;

  /**
   * Defines the arguments (properties and child expressions) for MonthsBetween expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    roundoff: false,
  };

  static {
    this.register();
  }

  static {
    this.register();
  }

  constructor (args: MonthsBetweenExprArgs = {}) {
    super(args);
  }

  get roundoff (): Expression {
    return this.args.roundoff as Expression;
  }
}

export type MakeIntervalExprArgs = { year?: Expression; month?: Expression; week?: Expression; day?: Expression; hour?: Expression; minute?: Expression; second?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class MakeIntervalExpr extends FuncExpr {
  key = ExpressionKey.MAKE_INTERVAL;

  /**
   * Defines the arguments (properties and child expressions) for MakeInterval expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    year: false,
    month: false,
    week: false,
    day: false,
    hour: false,
    minute: false,
    second: false,
  };

  constructor (args: MakeIntervalExprArgs = {}) {
    super(args);
  }

  get year (): Expression {
    return this.args.year as Expression;
  }

  get month (): Expression {
    return this.args.month as Expression;
  }

  get week (): Expression {
    return this.args.week as Expression;
  }

  get day (): Expression {
    return this.args.day as Expression;
  }

  get hour (): Expression {
    return this.args.hour as Expression;
  }

  get minute (): Expression {
    return this.args.minute as Expression;
  }

  get second (): Expression {
    return this.args.second as Expression;
  }
}

export type LastDayExprArgs = { unit?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class LastDayExpr extends FuncExpr {
  key = ExpressionKey.LAST_DAY;

  /**
   * Defines the arguments (properties and child expressions) for LastDay expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
  };

  constructor (args: LastDayExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }
}

export class PreviousDayExpr extends FuncExpr {
  key = ExpressionKey.PREVIOUS_DAY;
}

export class LaxBoolExpr extends FuncExpr {
  key = ExpressionKey.LAX_BOOL;
}

export class LaxFloat64Expr extends FuncExpr {
  key = ExpressionKey.LAX_FLOAT64;
}

export class LaxInt64Expr extends FuncExpr {
  key = ExpressionKey.LAX_INT64;
}

export class LaxStringExpr extends FuncExpr {
  key = ExpressionKey.LAX_STRING;
}

export class ExtractExpr extends FuncExpr {
  key = ExpressionKey.EXTRACT;
}

export class ExistsExpr extends FuncExpr {
  key = ExpressionKey.EXISTS;
}

export class EltExpr extends FuncExpr {
  key = ExpressionKey.ELT;
}

export type TimestampExprArgs = { zone?: Expression; withTz?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TimestampExpr extends FuncExpr {
  key = ExpressionKey.TIMESTAMP;

  /**
   * Defines the arguments (properties and child expressions) for Timestamp expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    zone: false,
    withTz: false,
  };

  constructor (args: TimestampExprArgs = {}) {
    super(args);
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }

  get withTz (): Expression {
    return this.args.withTz as Expression;
  }
}

export type TimestampAddExprArgs = { unit?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TimestampAddExpr extends FuncExpr {
  key = ExpressionKey.TIMESTAMP_ADD;

  /**
   * Defines the arguments (properties and child expressions) for TimestampAdd expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
  };

  constructor (args: TimestampAddExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }
}

export type TimestampSubExprArgs = { unit?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TimestampSubExpr extends FuncExpr {
  key = ExpressionKey.TIMESTAMP_SUB;

  /**
   * Defines the arguments (properties and child expressions) for TimestampSub expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
  };

  constructor (args: TimestampSubExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }
}

export type TimestampDiffExprArgs = { unit?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TimestampDiffExpr extends FuncExpr {
  key = ExpressionKey.TIMESTAMP_DIFF;

  /**
   * Defines the arguments (properties and child expressions) for TimestampDiff expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
  };

  constructor (args: TimestampDiffExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }
}

export type TimestampTruncExprArgs = { unit: Expression; zone?: Expression; inputTypePreserved?: DataTypeExpr; [key: string]: unknown } & BaseExpressionArgs;

export class TimestampTruncExpr extends FuncExpr {
  key = ExpressionKey.TIMESTAMP_TRUNC;

  /**
   * Defines the arguments (properties and child expressions) for TimestampTrunc expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: true,
    zone: false,
    inputTypePreserved: false,
  };

  constructor (args: TimestampTruncExprArgs) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }

  get inputTypePreserved (): DataTypeExpr {
    return this.args.inputTypePreserved as DataTypeExpr;
  }
}

/**
 * Valid kind values for time slice expressions
 */
export enum TimeSliceExprKind {
  START = 'START',
  END = 'END',
}
export type TimeSliceExprArgs = { unit: Expression; kind?: TimeSliceExprKind; [key: string]: unknown } & BaseExpressionArgs;

export class TimeSliceExpr extends FuncExpr {
  key = ExpressionKey.TIME_SLICE;

  /**
   * Defines the arguments (properties and child expressions) for TimeSlice expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: true,
    kind: false,
  };

  constructor (args: TimeSliceExprArgs) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }

  get kind (): TimeSliceExprKind | undefined {
    return this.args.kind as TimeSliceExprKind | undefined;
  }
}

export type TimeAddExprArgs = { unit?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TimeAddExpr extends FuncExpr {
  key = ExpressionKey.TIME_ADD;

  /**
   * Defines the arguments (properties and child expressions) for TimeAdd expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
  };

  constructor (args: TimeAddExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }
}

export type TimeSubExprArgs = { unit?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TimeSubExpr extends FuncExpr {
  key = ExpressionKey.TIME_SUB;

  /**
   * Defines the arguments (properties and child expressions) for TimeSub expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
  };

  constructor (args: TimeSubExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }
}

export type TimeDiffExprArgs = { unit?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TimeDiffExpr extends FuncExpr {
  key = ExpressionKey.TIME_DIFF;

  /**
   * Defines the arguments (properties and child expressions) for TimeDiff expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
  };

  constructor (args: TimeDiffExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }
}

export type TimeTruncExprArgs = { unit: Expression; zone?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TimeTruncExpr extends FuncExpr {
  key = ExpressionKey.TIME_TRUNC;

  /**
   * Defines the arguments (properties and child expressions) for TimeTrunc expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: true,
    zone: false,
  };

  constructor (args: TimeTruncExprArgs) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }
}

export type DateFromPartsExprArgs = { year: Expression; month?: Expression; day?: Expression; allowOverflow?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DateFromPartsExpr extends FuncExpr {
  key = ExpressionKey.DATE_FROM_PARTS;

  /**
   * Defines the arguments (properties and child expressions) for DateFromParts expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    year: true,
    month: false,
    day: false,
    allowOverflow: false,
  };

  constructor (args: DateFromPartsExprArgs) {
    super(args);
  }

  get year (): Expression {
    return this.args.year as Expression;
  }

  get month (): Expression {
    return this.args.month as Expression;
  }

  get day (): Expression {
    return this.args.day as Expression;
  }

  get allowOverflow (): Expression {
    return this.args.allowOverflow as Expression;
  }
}

export type TimeFromPartsExprArgs = { hour: Expression; min: Expression; sec: Expression; nano?: Expression; fractions?: Expression[]; precision?: number | Expression; overflow?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TimeFromPartsExpr extends FuncExpr {
  key = ExpressionKey.TIME_FROM_PARTS;

  /**
   * Defines the arguments (properties and child expressions) for TimeFromParts expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    hour: true,
    min: true,
    sec: true,
    nano: false,
    fractions: false,
    precision: false,
    overflow: false,
  };

  constructor (args: TimeFromPartsExprArgs) {
    super(args);
  }

  get hour (): Expression {
    return this.args.hour as Expression;
  }

  get min (): Expression {
    return this.args.min as Expression;
  }

  get sec (): Expression {
    return this.args.sec as Expression;
  }

  get nano (): Expression {
    return this.args.nano as Expression;
  }

  get fractions (): Expression[] {
    return (this.args.fractions || []) as Expression[];
  }

  get precision (): number | Expression {
    return this.args.precision as number | Expression;
  }

  get overflow (): Expression {
    return this.args.overflow as Expression;
  }
}

export class DateStrToDateExpr extends FuncExpr {
  key = ExpressionKey.DATE_STR_TO_DATE;
}

export class DateToDateStrExpr extends FuncExpr {
  key = ExpressionKey.DATE_TO_DATE_STR;
}

export class DateToDiExpr extends FuncExpr {
  key = ExpressionKey.DATE_TO_DI;
}

export type DateExprArgs = { zone?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class DateExpr extends FuncExpr {
  key = ExpressionKey.DATE;

  /**
   * Defines the arguments (properties and child expressions) for Date expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    zone: false,
  };

  constructor (args: DateExprArgs = {}) {
    super(args);
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }
}

export class DayExpr extends FuncExpr {
  key = ExpressionKey.DAY;
}

export type DecodeExprArgs = { charset: string; replace?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class DecodeExpr extends FuncExpr {
  key = ExpressionKey.DECODE;

  /**
   * Defines the arguments (properties and child expressions) for Decode expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    charset: true,
    replace: false,
  };

  constructor (args: DecodeExprArgs) {
    super(args);
  }

  get charset (): string {
    return this.args.charset as string;
  }

  get replace (): boolean {
    return this.args.replace as boolean;
  }
}

export class DecodeCaseExpr extends FuncExpr {
  key = ExpressionKey.DECODE_CASE;
}

export type DecryptExprArgs = { passphrase: Expression; aad?: Expression; encryptionMethod?: string; safe?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class DecryptExpr extends FuncExpr {
  key = ExpressionKey.DECRYPT;

  /**
   * Defines the arguments (properties and child expressions) for Decrypt expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    passphrase: true,
    aad: false,
    encryptionMethod: false,
    safe: false,
  };

  constructor (args: DecryptExprArgs) {
    super(args);
  }

  get passphrase (): Expression {
    return this.args.passphrase as Expression;
  }

  get aad (): Expression {
    return this.args.aad as Expression;
  }

  get encryptionMethod (): string {
    return this.args.encryptionMethod as string;
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }
}

export type DecryptRawExprArgs = { key: unknown; iv: Expression; aad?: Expression; encryptionMethod?: string; aead?: Expression; safe?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class DecryptRawExpr extends FuncExpr {
  key = ExpressionKey.DECRYPT_RAW;

  /**
   * Defines the arguments (properties and child expressions) for DecryptRaw expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    key: true,
    iv: true,
    aad: false,
    encryptionMethod: false,
    aead: false,
    safe: false,
  };

  constructor (args: DecryptRawExprArgs) {
    super(args);
  }

  get iv (): Expression {
    return this.args.iv as Expression;
  }

  get aad (): Expression {
    return this.args.aad as Expression;
  }

  get encryptionMethod (): string {
    return this.args.encryptionMethod as string;
  }

  get aead (): Expression {
    return this.args.aead as Expression;
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }
}

export class DiToDateExpr extends FuncExpr {
  key = ExpressionKey.DI_TO_DATE;
}

export type EncodeExprArgs = { charset: string; [key: string]: unknown } & BaseExpressionArgs;

export class EncodeExpr extends FuncExpr {
  key = ExpressionKey.ENCODE;

  /**
   * Defines the arguments (properties and child expressions) for Encode expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    charset: true,
  };

  constructor (args: EncodeExprArgs) {
    super(args);
  }

  get charset (): string {
    return this.args.charset as string;
  }
}

export type EncryptExprArgs = { passphrase: Expression; aad?: Expression; encryptionMethod?: string; [key: string]: unknown } & BaseExpressionArgs;

export class EncryptExpr extends FuncExpr {
  key = ExpressionKey.ENCRYPT;

  /**
   * Defines the arguments (properties and child expressions) for Encrypt expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    passphrase: true,
    aad: false,
    encryptionMethod: false,
  };

  constructor (args: EncryptExprArgs) {
    super(args);
  }

  get passphrase (): Expression {
    return this.args.passphrase as Expression;
  }

  get aad (): Expression {
    return this.args.aad as Expression;
  }

  get encryptionMethod (): string {
    return this.args.encryptionMethod as string;
  }
}

export type EncryptRawExprArgs = { key: unknown; iv: Expression; aad?: Expression; encryptionMethod?: string; [key: string]: unknown } & BaseExpressionArgs;

export class EncryptRawExpr extends FuncExpr {
  key = ExpressionKey.ENCRYPT_RAW;

  /**
   * Defines the arguments (properties and child expressions) for EncryptRaw expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    key: true,
    iv: true,
    aad: false,
    encryptionMethod: false,
  };

  constructor (args: EncryptRawExprArgs) {
    super(args);
  }

  get iv (): Expression {
    return this.args.iv as Expression;
  }

  get aad (): Expression {
    return this.args.aad as Expression;
  }

  get encryptionMethod (): string {
    return this.args.encryptionMethod as string;
  }
}

export class EqualNullExpr extends FuncExpr {
  key = ExpressionKey.EQUAL_NULL;
}

export class ExpExpr extends FuncExpr {
  key = ExpressionKey.EXP;
}

export class FactorialExpr extends FuncExpr {
  key = ExpressionKey.FACTORIAL;
}

export class ExplodeExpr extends FuncExpr {
  key = ExpressionKey.EXPLODE;
}

export class InlineExpr extends FuncExpr {
  key = ExpressionKey.INLINE;
}

export type UnnestExprArgs = { offset?: boolean; explodeArray?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class UnnestExpr extends FuncExpr {
  key = ExpressionKey.UNNEST;

  /**
   * Defines the arguments (properties and child expressions) for Unnest expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    offset: false,
    explodeArray: false,
  };

  constructor (args: UnnestExprArgs = {}) {
    super(args);
  }

  get offset (): boolean {
    return this.args.offset as boolean;
  }

  get explodeArray (): Expression {
    return this.args.explodeArray as Expression;
  }
}

export type FloorExprArgs = { decimals?: Expression[]; to?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class FloorExpr extends FuncExpr {
  key = ExpressionKey.FLOOR;

  /**
   * Defines the arguments (properties and child expressions) for Floor expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    decimals: false,
    to: false,
  };

  constructor (args: FloorExprArgs = {}) {
    super(args);
  }

  get decimals (): Expression[] {
    return (this.args.decimals || []) as Expression[];
  }

  get to (): Expression {
    return this.args.to as Expression;
  }
}

export class FromBase32Expr extends FuncExpr {
  key = ExpressionKey.FROM_BASE32;
}

export class FromBase64Expr extends FuncExpr {
  key = ExpressionKey.FROM_BASE64;
}

export class ToBase32Expr extends FuncExpr {
  key = ExpressionKey.TO_BASE32;
}

export class ToBase64Expr extends FuncExpr {
  key = ExpressionKey.TO_BASE64;
}

export type ToBinaryExprArgs = { format?: string; safe?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class ToBinaryExpr extends FuncExpr {
  key = ExpressionKey.TO_BINARY;

  /**
   * Defines the arguments (properties and child expressions) for ToBinary expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: false,
    safe: false,
  };

  constructor (args: ToBinaryExprArgs = {}) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }
}

export type Base64DecodeBinaryExprArgs = { alphabet?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class Base64DecodeBinaryExpr extends FuncExpr {
  key = ExpressionKey.BASE64_DECODE_BINARY;

  /**
   * Defines the arguments (properties and child expressions) for Base64DecodeBinary expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    alphabet: false,
  };

  constructor (args: Base64DecodeBinaryExprArgs = {}) {
    super(args);
  }

  get alphabet (): Expression {
    return this.args.alphabet as Expression;
  }
}

export type Base64DecodeStringExprArgs = { alphabet?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class Base64DecodeStringExpr extends FuncExpr {
  key = ExpressionKey.BASE64_DECODE_STRING;

  /**
   * Defines the arguments (properties and child expressions) for Base64DecodeString expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    alphabet: false,
  };

  constructor (args: Base64DecodeStringExprArgs = {}) {
    super(args);
  }

  get alphabet (): Expression {
    return this.args.alphabet as Expression;
  }
}

export type Base64EncodeExprArgs = { maxLineLength?: number | Expression; alphabet?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class Base64EncodeExpr extends FuncExpr {
  key = ExpressionKey.BASE64_ENCODE;

  /**
   * Defines the arguments (properties and child expressions) for Base64Encode expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    maxLineLength: false,
    alphabet: false,
  };

  constructor (args: Base64EncodeExprArgs = {}) {
    super(args);
  }

  get maxLineLength (): number | Expression {
    return this.args.maxLineLength as number | Expression;
  }

  get alphabet (): Expression {
    return this.args.alphabet as Expression;
  }
}

export type TryBase64DecodeBinaryExprArgs = { alphabet?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TryBase64DecodeBinaryExpr extends FuncExpr {
  key = ExpressionKey.TRY_BASE64_DECODE_BINARY;

  /**
   * Defines the arguments (properties and child expressions) for TryBase64DecodeBinary expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    alphabet: false,
  };

  constructor (args: TryBase64DecodeBinaryExprArgs = {}) {
    super(args);
  }

  get alphabet (): Expression {
    return this.args.alphabet as Expression;
  }
}

export type TryBase64DecodeStringExprArgs = { alphabet?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TryBase64DecodeStringExpr extends FuncExpr {
  key = ExpressionKey.TRY_BASE64_DECODE_STRING;

  /**
   * Defines the arguments (properties and child expressions) for TryBase64DecodeString expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    alphabet: false,
  };

  constructor (args: TryBase64DecodeStringExprArgs = {}) {
    super(args);
  }

  get alphabet (): Expression {
    return this.args.alphabet as Expression;
  }
}

export class TryHexDecodeBinaryExpr extends FuncExpr {
  key = ExpressionKey.TRY_HEX_DECODE_BINARY;
}

export class TryHexDecodeStringExpr extends FuncExpr {
  key = ExpressionKey.TRY_HEX_DECODE_STRING;
}

export class FromISO8601TimestampExpr extends FuncExpr {
  key = ExpressionKey.FROM_ISO8601_TIMESTAMP;
  static sqlNames = ['FROM_ISO8601_TIMESTAMP'];
}

export type GapFillExprArgs = { tsColumn: Expression; bucketWidth: Expression; partitioningColumns?: Expression[]; valueColumns?: Expression[]; origin?: Expression; ignoreNulls?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class GapFillExpr extends FuncExpr {
  key = ExpressionKey.GAP_FILL;

  /**
   * Defines the arguments (properties and child expressions) for GapFill expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    tsColumn: true,
    bucketWidth: true,
    partitioningColumns: false,
    valueColumns: false,
    origin: false,
    ignoreNulls: false,
  };

  static {
    this.register();
  }

  constructor (args: GapFillExprArgs) {
    super(args);
  }

  get tsColumn (): Expression {
    return this.args.tsColumn as Expression;
  }

  get bucketWidth (): Expression {
    return this.args.bucketWidth as Expression;
  }

  get partitioningColumns (): Expression[] {
    return (this.args.partitioningColumns || []) as Expression[];
  }

  get valueColumns (): Expression[] {
    return (this.args.valueColumns || []) as Expression[];
  }

  get origin (): Expression {
    return this.args.origin as Expression;
  }

  get ignoreNulls (): Expression[] {
    return (this.args.ignoreNulls || []) as Expression[];
  }
}

export type GenerateDateArrayExprArgs = { start: Expression; end: Expression; step?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class GenerateDateArrayExpr extends FuncExpr {
  key = ExpressionKey.GENERATE_DATE_ARRAY;

  /**
   * Defines the arguments (properties and child expressions) for GenerateDateArray expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    start: true,
    end: true,
    step: false,
  };

  constructor (args: GenerateDateArrayExprArgs) {
    super(args);
  }

  get start (): Expression {
    return this.args.start as Expression;
  }

  get end (): Expression {
    return this.args.end as Expression;
  }

  get step (): Expression {
    return this.args.step as Expression;
  }
}

export type GenerateTimestampArrayExprArgs = { start: Expression; end: Expression; step: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class GenerateTimestampArrayExpr extends FuncExpr {
  key = ExpressionKey.GENERATE_TIMESTAMP_ARRAY;

  /**
   * Defines the arguments (properties and child expressions) for GenerateTimestampArray expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    start: true,
    end: true,
    step: true,
  };

  constructor (args: GenerateTimestampArrayExprArgs) {
    super(args);
  }

  get start (): Expression {
    return this.args.start as Expression;
  }

  get end (): Expression {
    return this.args.end as Expression;
  }

  get step (): Expression {
    return this.args.step as Expression;
  }
}

export class GetExtractExpr extends FuncExpr {
  key = ExpressionKey.GET_EXTRACT;
}

export type GetbitExprArgs = { zeroIsMsb?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class GetbitExpr extends FuncExpr {
  key = ExpressionKey.GETBIT;

  /**
   * Defines the arguments (properties and child expressions) for Getbit expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    zeroIsMsb: false,
  };

  constructor (args: GetbitExprArgs = {}) {
    super(args);
  }

  get zeroIsMsb (): Expression {
    return this.args.zeroIsMsb as Expression;
  }
}

export type GreatestExprArgs = { ignoreNulls: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class GreatestExpr extends FuncExpr {
  key = ExpressionKey.GREATEST;

  /**
   * Defines the arguments (properties and child expressions) for Greatest expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ignoreNulls: true,
  };

  constructor (args: GreatestExprArgs) {
    super(args);
  }

  get ignoreNulls (): Expression[] {
    return (this.args.ignoreNulls || []) as Expression[];
  }
}

export class HexExpr extends FuncExpr {
  key = ExpressionKey.HEX;
}

export class HexDecodeStringExpr extends FuncExpr {
  key = ExpressionKey.HEX_DECODE_STRING;
}

export type HexEncodeExprArgs = { case?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class HexEncodeExpr extends FuncExpr {
  key = ExpressionKey.HEX_ENCODE;

  /**
   * Defines the arguments (properties and child expressions) for HexEncode expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    case: false,
  };

  constructor (args: HexEncodeExprArgs = {}) {
    super(args);
  }

  get case (): Expression {
    return this.args['case'] as Expression;
  }
}

export class HourExpr extends FuncExpr {
  key = ExpressionKey.HOUR;
}

export class MinuteExpr extends FuncExpr {
  key = ExpressionKey.MINUTE;
}

export class SecondExpr extends FuncExpr {
  key = ExpressionKey.SECOND;
}

export type CompressExprArgs = { method?: string; [key: string]: unknown } & BaseExpressionArgs;

export class CompressExpr extends FuncExpr {
  key = ExpressionKey.COMPRESS;

  /**
   * Defines the arguments (properties and child expressions) for Compress expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    method: false,
  };

  constructor (args: CompressExprArgs = {}) {
    super(args);
  }

  get method (): string {
    return this.args.method as string;
  }
}

export type DecompressBinaryExprArgs = { method: string; [key: string]: unknown } & BaseExpressionArgs;

export class DecompressBinaryExpr extends FuncExpr {
  key = ExpressionKey.DECOMPRESS_BINARY;

  /**
   * Defines the arguments (properties and child expressions) for DecompressBinary expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    method: true,
  };

  constructor (args: DecompressBinaryExprArgs) {
    super(args);
  }

  get method (): string {
    return this.args.method as string;
  }
}

export type DecompressStringExprArgs = { method: string; [key: string]: unknown } & BaseExpressionArgs;

export class DecompressStringExpr extends FuncExpr {
  key = ExpressionKey.DECOMPRESS_STRING;

  /**
   * Defines the arguments (properties and child expressions) for DecompressString expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    method: true,
  };

  constructor (args: DecompressStringExprArgs) {
    super(args);
  }

  get method (): string {
    return this.args.method as string;
  }
}

export type IfExprArgs = { true: Expression; false?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class IfExpr extends FuncExpr {
  key = ExpressionKey.IF;
  static sqlNames = ['IF', 'IIF'];

  /**
   * Defines the arguments (properties and child expressions) for If expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    true: true,
    false: false,
  };

  constructor (args: IfExprArgs) {
    super(args);
  }

  get true (): Expression {
    return this.args.true as Expression;
  }

  get false (): Expression {
    return this.args.false as Expression;
  }
}

export class NullifExpr extends FuncExpr {
  key = ExpressionKey.NULLIF;
}

export class InitcapExpr extends FuncExpr {
  key = ExpressionKey.INITCAP;
}

export class IsAsciiExpr extends FuncExpr {
  key = ExpressionKey.IS_ASCII;
}

export class IsNanExpr extends FuncExpr {
  key = ExpressionKey.IS_NAN;
  static sqlNames = ['IS_NAN', 'ISNAN'];

  static {
    this.register();
  }
}

export class Int64Expr extends FuncExpr {
  key = ExpressionKey.INT64;
}

export class IsInfExpr extends FuncExpr {
  key = ExpressionKey.IS_INF;
  static sqlNames = ['IS_INF', 'ISINF'];

  static {
    this.register();
  }
}

export class IsNullValueExpr extends FuncExpr {
  key = ExpressionKey.IS_NULL_VALUE;
}

export class IsArrayExpr extends FuncExpr {
  key = ExpressionKey.IS_ARRAY;
}

export class FormatExpr extends FuncExpr {
  key = ExpressionKey.FORMAT;
}

export class JSONKeysExpr extends FuncExpr {
  key = ExpressionKey.JSON_KEYS;
}

export type JSONKeysAtDepthExprArgs = { mode?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONKeysAtDepthExpr extends FuncExpr {
  key = ExpressionKey.JSON_KEYS_AT_DEPTH;

  /**
   * Defines the arguments (properties and child expressions) for JSONKeysAtDepth expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    mode: false,
  };

  constructor (args: JSONKeysAtDepthExprArgs = {}) {
    super(args);
  }

  get mode (): Expression {
    return this.args.mode as Expression;
  }
}

export type JSONObjectExprArgs = { nullHandling?: Expression; uniqueKeys?: Expression[]; returnType?: DataTypeExpr; encoding?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONObjectExpr extends FuncExpr {
  key = ExpressionKey.JSON_OBJECT;

  /**
   * Defines the arguments (properties and child expressions) for JSONObject expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    nullHandling: false,
    uniqueKeys: false,
    returnType: false,
    encoding: false,
  };

  constructor (args: JSONObjectExprArgs = {}) {
    super(args);
  }

  get nullHandling (): Expression {
    return this.args.nullHandling as Expression;
  }

  get uniqueKeys (): Expression[] {
    return (this.args.uniqueKeys || []) as Expression[];
  }

  get returnType (): DataTypeExpr {
    return this.args.returnType as DataTypeExpr;
  }

  get encoding (): Expression {
    return this.args.encoding as Expression;
  }
}

export type JSONArrayExprArgs = { nullHandling?: Expression; returnType?: DataTypeExpr; strict?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONArrayExpr extends FuncExpr {
  key = ExpressionKey.JSON_ARRAY;

  /**
   * Defines the arguments (properties and child expressions) for JSONArray expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    nullHandling: false,
    returnType: false,
    strict: false,
  };

  constructor (args: JSONArrayExprArgs = {}) {
    super(args);
  }

  get nullHandling (): Expression {
    return this.args.nullHandling as Expression;
  }

  get returnType (): DataTypeExpr {
    return this.args.returnType as DataTypeExpr;
  }

  get strict (): Expression {
    return this.args.strict as Expression;
  }
}

export type JSONExistsExprArgs = { path: Expression; passing?: Expression; onCondition?: Expression; fromDcolonqmark?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONExistsExpr extends FuncExpr {
  key = ExpressionKey.JSON_EXISTS;

  /**
   * Defines the arguments (properties and child expressions) for JSONExists expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    path: true,
    passing: false,
    onCondition: false,
    fromDcolonqmark: false,
  };

  constructor (args: JSONExistsExprArgs) {
    super(args);
  }

  get path (): Expression {
    return this.args.path as Expression;
  }

  get passing (): Expression {
    return this.args.passing as Expression;
  }

  get onCondition (): Expression {
    return this.args.onCondition as Expression;
  }

  get fromDcolonqmark (): Expression {
    return this.args.fromDcolonqmark as Expression;
  }
}

export class JSONSetExpr extends FuncExpr {
  key = ExpressionKey.JSON_SET;
}

export type JSONStripNullsExprArgs = { includeArrays?: Expression[]; removeEmpty?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONStripNullsExpr extends FuncExpr {
  key = ExpressionKey.JSON_STRIP_NULLS;

  /**
   * Defines the arguments (properties and child expressions) for JSONStripNulls expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    includeArrays: false,
    removeEmpty: false,
  };

  constructor (args: JSONStripNullsExprArgs = {}) {
    super(args);
  }

  get includeArrays (): Expression[] {
    return (this.args.includeArrays || []) as Expression[];
  }

  get removeEmpty (): Expression {
    return this.args.removeEmpty as Expression;
  }
}

export class JSONValueArrayExpr extends FuncExpr {
  key = ExpressionKey.JSON_VALUE_ARRAY;
}

export class JSONRemoveExpr extends FuncExpr {
  key = ExpressionKey.JSON_REMOVE;
}

export type JSONTableExprArgs = { schema: Expression; path?: Expression; errorHandling?: Expression; emptyHandling?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONTableExpr extends FuncExpr {
  key = ExpressionKey.JSON_TABLE;

  /**
   * Defines the arguments (properties and child expressions) for JSONTable expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    schema: true,
    path: false,
    errorHandling: false,
    emptyHandling: false,
  };

  constructor (args: JSONTableExprArgs) {
    super(args);
  }

  get schema (): Expression {
    return this.args.schema as Expression;
  }

  get path (): Expression {
    return this.args.path as Expression;
  }

  get errorHandling (): Expression {
    return this.args.errorHandling as Expression;
  }

  get emptyHandling (): Expression {
    return this.args.emptyHandling as Expression;
  }
}

export class JSONTypeExpr extends FuncExpr {
  key = ExpressionKey.JSON_TYPE;
}

export type ObjectInsertExprArgs = { key: unknown; value: string; updateFlag?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ObjectInsertExpr extends FuncExpr {
  key = ExpressionKey.OBJECT_INSERT;

  /**
   * Defines the arguments (properties and child expressions) for ObjectInsert expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    key: true,
    value: true,
    updateFlag: false,
  };

  constructor (args: ObjectInsertExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get updateFlag (): Expression {
    return this.args.updateFlag as Expression;
  }
}

export type OpenJSONExprArgs = { path?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class OpenJSONExpr extends FuncExpr {
  key = ExpressionKey.OPEN_JSON;

  /**
   * Defines the arguments (properties and child expressions) for OpenJSON expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    path: false,
  };

  constructor (args: OpenJSONExprArgs = {}) {
    super(args);
  }

  get path (): Expression {
    return this.args.path as Expression;
  }
}

export class JSONBContainsExpr extends BinaryExpr {
  key = ExpressionKey.JSONB_CONTAINS;
}

export class JSONBContainsAnyTopKeysExpr extends BinaryExpr {
  key = ExpressionKey.JSONB_CONTAINS_ANY_TOP_KEYS;
}

export class JSONBContainsAllTopKeysExpr extends BinaryExpr {
  key = ExpressionKey.JSONB_CONTAINS_ALL_TOP_KEYS;
}

export type JSONBExistsExprArgs = { path: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONBExistsExpr extends FuncExpr {
  key = ExpressionKey.JSONB_EXISTS;

  /**
   * Defines the arguments (properties and child expressions) for JSONBExists expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    path: true,
  };

  constructor (args: JSONBExistsExprArgs) {
    super(args);
  }

  get path (): Expression {
    return this.args.path as Expression;
  }
}

export class JSONBDeleteAtPathExpr extends BinaryExpr {
  key = ExpressionKey.JSONB_DELETE_AT_PATH;
}

export type JSONExtractExprArgs = { onlyJsonTypes?: Expression[]; variantExtract?: string; jsonQuery?: Expression; option?: Expression; quote?: Expression; onCondition?: Expression; requiresJson?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONExtractExpr extends BinaryExpr {
  key = ExpressionKey.JSON_EXTRACT;

  /**
   * Defines the arguments (properties and child expressions) for JSONExtract expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    onlyJsonTypes: false,
    variantExtract: false,
    jsonQuery: false,
    option: false,
    quote: false,
    onCondition: false,
    requiresJson: false,
  };

  constructor (args: JSONExtractExprArgs = {}) {
    super(args);
  }

  get onlyJsonTypes (): Expression[] {
    return (this.args.onlyJsonTypes || []) as Expression[];
  }

  get variantExtract (): string {
    return this.args.variantExtract as string;
  }

  get jsonQuery (): Expression {
    return this.args.jsonQuery as Expression;
  }

  get option (): Expression {
    return this.args.option as Expression;
  }

  get quote (): Expression {
    return this.args.quote as Expression;
  }

  get onCondition (): Expression {
    return this.args.onCondition as Expression;
  }

  get requiresJson (): Expression {
    return this.args.requiresJson as Expression;
  }
}

export class JSONExtractArrayExpr extends FuncExpr {
  key = ExpressionKey.JSON_EXTRACT_ARRAY;
}

export type JSONExtractScalarExprArgs = { onlyJsonTypes?: Expression[]; jsonType?: Expression; scalarOnly?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONExtractScalarExpr extends BinaryExpr {
  key = ExpressionKey.JSON_EXTRACT_SCALAR;

  /**
   * Defines the arguments (properties and child expressions) for JSONExtractScalar expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    onlyJsonTypes: false,
    jsonType: false,
    scalarOnly: false,
  };

  constructor (args: JSONExtractScalarExprArgs = {}) {
    super(args);
  }

  get onlyJsonTypes (): Expression[] {
    return (this.args.onlyJsonTypes || []) as Expression[];
  }

  get jsonType (): Expression {
    return this.args.jsonType as Expression;
  }

  get scalarOnly (): Expression {
    return this.args.scalarOnly as Expression;
  }
}

export class JSONBExtractExpr extends BinaryExpr {
  key = ExpressionKey.JSONB_EXTRACT;
}

export type JSONBExtractScalarExprArgs = { jsonType?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONBExtractScalarExpr extends BinaryExpr {
  key = ExpressionKey.JSONB_EXTRACT_SCALAR;

  /**
   * Defines the arguments (properties and child expressions) for JSONBExtractScalar expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    jsonType: false,
  };

  constructor (args: JSONBExtractScalarExprArgs = {}) {
    super(args);
  }

  get jsonType (): Expression {
    return this.args.jsonType as Expression;
  }
}

export type JSONFormatExprArgs = { options?: Expression[]; isJson?: Expression; toJson?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONFormatExpr extends FuncExpr {
  key = ExpressionKey.JSON_FORMAT;

  /**
   * Defines the arguments (properties and child expressions) for JSONFormat expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    options: false,
    isJson: false,
    toJson: false,
  };

  constructor (args: JSONFormatExprArgs = {}) {
    super(args);
  }

  get options (): Expression[] {
    return (this.args.options || []) as Expression[];
  }

  get isJson (): Expression {
    return this.args.isJson as Expression;
  }

  get toJson (): Expression {
    return this.args.toJson as Expression;
  }
}

export class JSONArrayAppendExpr extends FuncExpr {
  key = ExpressionKey.JSON_ARRAY_APPEND;
}

export type JSONArrayContainsExprArgs = { jsonType?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONArrayContainsExpr extends BinaryExpr {
  key = ExpressionKey.JSON_ARRAY_CONTAINS;

  /**
   * Defines the arguments (properties and child expressions) for JSONArrayContains expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    jsonType: false,
  };

  constructor (args: JSONArrayContainsExprArgs = {}) {
    super(args);
  }

  get jsonType (): Expression {
    return this.args.jsonType as Expression;
  }
}

export class JSONArrayInsertExpr extends FuncExpr {
  key = ExpressionKey.JSON_ARRAY_INSERT;
}

export class ParseBignumericExpr extends FuncExpr {
  key = ExpressionKey.PARSE_BIGNUMERIC;
}

export class ParseNumericExpr extends FuncExpr {
  key = ExpressionKey.PARSE_NUMERIC;
}

export type ParseJSONExprArgs = { safe?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class ParseJSONExpr extends FuncExpr {
  key = ExpressionKey.PARSE_JSON;

  /**
   * Defines the arguments (properties and child expressions) for ParseJSON expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    safe: false,
  };

  constructor (args: ParseJSONExprArgs = {}) {
    super(args);
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }
}

export type ParseUrlExprArgs = { partToExtract?: Expression; key?: unknown; permissive?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ParseUrlExpr extends FuncExpr {
  key = ExpressionKey.PARSE_URL;

  /**
   * Defines the arguments (properties and child expressions) for ParseUrl expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    partToExtract: false,
    key: false,
    permissive: false,
  };

  constructor (args: ParseUrlExprArgs = {}) {
    super(args);
  }

  get partToExtract (): Expression {
    return this.args.partToExtract as Expression;
  }

  get permissive (): Expression {
    return this.args.permissive as Expression;
  }
}

export type ParseIpExprArgs = { permissive?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ParseIpExpr extends FuncExpr {
  key = ExpressionKey.PARSE_IP;

  /**
   * Defines the arguments (properties and child expressions) for ParseIp expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    permissive: false,
  };

  constructor (args: ParseIpExprArgs = {}) {
    super(args);
  }

  get permissive (): Expression {
    return this.args.permissive as Expression;
  }
}

export type ParseTimeExprArgs = { format: string; [key: string]: unknown } & BaseExpressionArgs;

export class ParseTimeExpr extends FuncExpr {
  key = ExpressionKey.PARSE_TIME;

  /**
   * Defines the arguments (properties and child expressions) for ParseTime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: true,
  };

  constructor (args: ParseTimeExprArgs) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }
}

export type ParseDatetimeExprArgs = { format?: string; zone?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ParseDatetimeExpr extends FuncExpr {
  key = ExpressionKey.PARSE_DATETIME;

  /**
   * Defines the arguments (properties and child expressions) for ParseDatetime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: false,
    zone: false,
  };

  constructor (args: ParseDatetimeExprArgs = {}) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }
}

export type LeastExprArgs = { ignoreNulls: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class LeastExpr extends FuncExpr {
  key = ExpressionKey.LEAST;

  /**
   * Defines the arguments (properties and child expressions) for Least expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    ignoreNulls: true,
  };

  constructor (args: LeastExprArgs) {
    super(args);
  }

  get ignoreNulls (): Expression[] {
    return (this.args.ignoreNulls || []) as Expression[];
  }
}

export class LeftExpr extends FuncExpr {
  key = ExpressionKey.LEFT;
}

export class RightExpr extends FuncExpr {
  key = ExpressionKey.RIGHT;
}

export class ReverseExpr extends FuncExpr {
  key = ExpressionKey.REVERSE;
}

export type LengthExprArgs = { binary?: Expression; encoding?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class LengthExpr extends FuncExpr {
  key = ExpressionKey.LENGTH;

  /**
   * Defines the arguments (properties and child expressions) for Length expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    binary: false,
    encoding: false,
  };

  constructor (args: LengthExprArgs = {}) {
    super(args);
  }

  get binary (): Expression {
    return this.args.binary as Expression;
  }

  get encoding (): Expression {
    return this.args.encoding as Expression;
  }
}

export class RtrimmedLengthExpr extends FuncExpr {
  key = ExpressionKey.RTRIMMED_LENGTH;
}

export class BitLengthExpr extends FuncExpr {
  key = ExpressionKey.BIT_LENGTH;
}

export type LevenshteinExprArgs = { insCost?: Expression; delCost?: Expression; subCost?: Expression; maxDist?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class LevenshteinExpr extends FuncExpr {
  key = ExpressionKey.LEVENSHTEIN;

  /**
   * Defines the arguments (properties and child expressions) for Levenshtein expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    insCost: false,
    delCost: false,
    subCost: false,
    maxDist: false,
  };

  constructor (args: LevenshteinExprArgs = {}) {
    super(args);
  }

  get insCost (): Expression {
    return this.args.insCost as Expression;
  }

  get delCost (): Expression {
    return this.args.delCost as Expression;
  }

  get subCost (): Expression {
    return this.args.subCost as Expression;
  }

  get maxDist (): Expression {
    return this.args.maxDist as Expression;
  }
}

export class LnExpr extends FuncExpr {
  key = ExpressionKey.LN;
}

export class LogExpr extends FuncExpr {
  key = ExpressionKey.LOG;
}

export class LowerExpr extends FuncExpr {
  key = ExpressionKey.LOWER;
  static sqlNames = ['LOWER', 'LCASE'];
}

export type MapExprArgs = { keys?: Expression[]; values?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class MapExpr extends FuncExpr {
  key = ExpressionKey.MAP;

  /**
   * Defines the arguments (properties and child expressions) for Map expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    keys: false,
    values: false,
  };

  static {
    this.register();
  }

  constructor (args: MapExprArgs = {}) {
    super(args);
  }

  get keys (): Expression[] {
    return (this.args.keys || []) as Expression[];
  }

  get values (): Expression[] {
    return (this.args.values || []) as Expression[];
  }
}

export class ToMapExpr extends FuncExpr {
  key = ExpressionKey.TO_MAP;
}

export class MapFromEntriesExpr extends FuncExpr {
  key = ExpressionKey.MAP_FROM_ENTRIES;
}

export class MapCatExpr extends FuncExpr {
  key = ExpressionKey.MAP_CAT;
}

export type MapContainsKeyExprArgs = { key: unknown; [key: string]: unknown } & BaseExpressionArgs;

export class MapContainsKeyExpr extends FuncExpr {
  key = ExpressionKey.MAP_CONTAINS_KEY;

  /**
   * Defines the arguments (properties and child expressions) for MapContainsKey expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    key: true,
  };

  constructor (args: MapContainsKeyExprArgs) {
    super(args);
  }
}

export class MapDeleteExpr extends FuncExpr {
  key = ExpressionKey.MAP_DELETE;
}

export type MapInsertExprArgs = { key?: unknown; value: string; updateFlag?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class MapInsertExpr extends FuncExpr {
  key = ExpressionKey.MAP_INSERT;

  /**
   * Defines the arguments (properties and child expressions) for MapInsert expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    key: false,
    value: true,
    updateFlag: false,
  };

  constructor (args: MapInsertExprArgs) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }

  get updateFlag (): Expression {
    return this.args.updateFlag as Expression;
  }
}

export class MapKeysExpr extends FuncExpr {
  key = ExpressionKey.MAP_KEYS;
}

export class MapPickExpr extends FuncExpr {
  key = ExpressionKey.MAP_PICK;
}

export class MapSizeExpr extends FuncExpr {
  key = ExpressionKey.MAP_SIZE;
}

export class StarMapExpr extends FuncExpr {
  key = ExpressionKey.STAR_MAP;
}

export type VarMapExprArgs = { keys: Expression[]; values: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class VarMapExpr extends FuncExpr {
  key = ExpressionKey.VAR_MAP;

  /**
   * Defines the arguments (properties and child expressions) for VarMap expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    keys: true,
    values: true,
  };

  constructor (args: VarMapExprArgs) {
    super(args);
  }

  get keys (): Expression[] {
    return (this.args.keys || []) as Expression[];
  }

  get values (): Expression[] {
    return (this.args.values || []) as Expression[];
  }
}

export type MatchAgainstExprArgs = { modifier?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class MatchAgainstExpr extends FuncExpr {
  key = ExpressionKey.MATCH_AGAINST;

  /**
   * Defines the arguments (properties and child expressions) for MatchAgainst expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    modifier: false,
  };

  constructor (args: MatchAgainstExprArgs = {}) {
    super(args);
  }

  get modifier (): Expression {
    return this.args.modifier as Expression;
  }
}

export class MD5Expr extends FuncExpr {
  key = ExpressionKey.MD5;
  static sqlNames = ['MD5'];

  static {
    this.register();
  }
}

export class MD5DigestExpr extends FuncExpr {
  key = ExpressionKey.MD5_DIGEST;
}

export class MD5NumberLower64Expr extends FuncExpr {
  key = ExpressionKey.MD5_NUMBER_LOWER64;
}

export class MD5NumberUpper64Expr extends FuncExpr {
  key = ExpressionKey.MD5_NUMBER_UPPER64;
}

export class MonthExpr extends FuncExpr {
  key = ExpressionKey.MONTH;
}

export type MonthnameExprArgs = { abbreviated?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class MonthnameExpr extends FuncExpr {
  key = ExpressionKey.MONTHNAME;

  /**
   * Defines the arguments (properties and child expressions) for Monthname expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    abbreviated: false,
  };

  constructor (args: MonthnameExprArgs = {}) {
    super(args);
  }

  get abbreviated (): Expression {
    return this.args.abbreviated as Expression;
  }
}

export type AddMonthsExprArgs = { preserveEndOfMonth?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class AddMonthsExpr extends FuncExpr {
  key = ExpressionKey.ADD_MONTHS;

  /**
   * Defines the arguments (properties and child expressions) for AddMonths expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    preserveEndOfMonth: false,
  };

  constructor (args: AddMonthsExprArgs = {}) {
    super(args);
  }

  get preserveEndOfMonth (): Expression {
    return this.args.preserveEndOfMonth as Expression;
  }
}

export type Nvl2ExprArgs = { true: Expression; false?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class Nvl2Expr extends FuncExpr {
  key = ExpressionKey.NVL2;

  /**
   * Defines the arguments (properties and child expressions) for Nvl2 expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    true: true,
    false: false,
  };

  constructor (args: Nvl2ExprArgs) {
    super(args);
  }

  get true (): Expression {
    return this.args.true as Expression;
  }

  get false (): Expression {
    return this.args.false as Expression;
  }
}

export type NormalizeExprArgs = { form?: Expression; isCasefold?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class NormalizeExpr extends FuncExpr {
  key = ExpressionKey.NORMALIZE;

  /**
   * Defines the arguments (properties and child expressions) for Normalize expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    form: false,
    isCasefold: false,
  };

  constructor (args: NormalizeExprArgs = {}) {
    super(args);
  }

  get form (): Expression {
    return this.args.form as Expression;
  }

  get isCasefold (): Expression {
    return this.args.isCasefold as Expression;
  }
}

export type NormalExprArgs = { stddev: Expression; gen: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class NormalExpr extends FuncExpr {
  key = ExpressionKey.NORMAL;

  /**
   * Defines the arguments (properties and child expressions) for Normal expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    stddev: true,
    gen: true,
  };

  constructor (args: NormalExprArgs) {
    super(args);
  }

  get stddev (): Expression {
    return this.args.stddev as Expression;
  }

  get gen (): Expression {
    return this.args.gen as Expression;
  }
}

export class NetFuncExpr extends FuncExpr {
  key = ExpressionKey.NET_FUNC;
}

export class HostExpr extends FuncExpr {
  key = ExpressionKey.HOST;
}

export class RegDomainExpr extends FuncExpr {
  key = ExpressionKey.REG_DOMAIN;
}

export type OverlayExprArgs = { from: Expression; for?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class OverlayExpr extends FuncExpr {
  key = ExpressionKey.OVERLAY;

  /**
   * Defines the arguments (properties and child expressions) for Overlay expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    from: true,
    for: false,
  };

  constructor (args: OverlayExprArgs) {
    super(args);
  }

  get from (): Expression {
    return this.args.from as Expression;
  }

  get for (): Expression {
    return this.args['for'] as Expression;
  }
}

export type PredictExprArgs = { paramsStruct?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class PredictExpr extends FuncExpr {
  key = ExpressionKey.PREDICT;

  /**
   * Defines the arguments (properties and child expressions) for Predict expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    paramsStruct: false,
  };

  constructor (args: PredictExprArgs = {}) {
    super(args);
  }

  get paramsStruct (): Expression {
    return this.args.paramsStruct as Expression;
  }
}

export type MLTranslateExprArgs = { paramsStruct: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class MLTranslateExpr extends FuncExpr {
  key = ExpressionKey.ML_TRANSLATE;

  /**
   * Defines the arguments (properties and child expressions) for MLTranslate expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    paramsStruct: true,
  };

  constructor (args: MLTranslateExprArgs) {
    super(args);
  }

  get paramsStruct (): Expression {
    return this.args.paramsStruct as Expression;
  }
}

export type FeaturesAtTimeExprArgs = { time?: Expression; numRows?: Expression[]; ignoreFeatureNulls?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class FeaturesAtTimeExpr extends FuncExpr {
  key = ExpressionKey.FEATURES_AT_TIME;

  /**
   * Defines the arguments (properties and child expressions) for FeaturesAtTime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    time: false,
    numRows: false,
    ignoreFeatureNulls: false,
  };

  constructor (args: FeaturesAtTimeExprArgs = {}) {
    super(args);
  }

  get time (): Expression {
    return this.args.time as Expression;
  }

  get numRows (): Expression[] {
    return (this.args.numRows || []) as Expression[];
  }

  get ignoreFeatureNulls (): Expression[] {
    return (this.args.ignoreFeatureNulls || []) as Expression[];
  }
}

export type GenerateEmbeddingExprArgs = { paramsStruct?: Expression; isText?: string; [key: string]: unknown } & BaseExpressionArgs;

export class GenerateEmbeddingExpr extends FuncExpr {
  key = ExpressionKey.GENERATE_EMBEDDING;

  /**
   * Defines the arguments (properties and child expressions) for GenerateEmbedding expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    paramsStruct: false,
    isText: false,
  };

  constructor (args: GenerateEmbeddingExprArgs = {}) {
    super(args);
  }

  get paramsStruct (): Expression {
    return this.args.paramsStruct as Expression;
  }

  get isText (): string {
    return this.args.isText as string;
  }
}

export type MLForecastExprArgs = { paramsStruct?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class MLForecastExpr extends FuncExpr {
  key = ExpressionKey.ML_FORECAST;

  /**
   * Defines the arguments (properties and child expressions) for MLForecast expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    paramsStruct: false,
  };

  constructor (args: MLForecastExprArgs = {}) {
    super(args);
  }

  get paramsStruct (): Expression {
    return this.args.paramsStruct as Expression;
  }
}

export type VectorSearchExprArgs = { columnToSearch: Expression; queryTable: Expression; queryColumnToSearch?: Expression; topK?: Expression; distanceType?: DataTypeExpr; options?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class VectorSearchExpr extends FuncExpr {
  key = ExpressionKey.VECTOR_SEARCH;

  /**
   * Defines the arguments (properties and child expressions) for VectorSearch expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    columnToSearch: true,
    queryTable: true,
    queryColumnToSearch: false,
    topK: false,
    distanceType: false,
    options: false,
  };

  constructor (args: VectorSearchExprArgs) {
    super(args);
  }

  get columnToSearch (): Expression {
    return this.args.columnToSearch as Expression;
  }

  get queryTable (): Expression {
    return this.args.queryTable as Expression;
  }

  get queryColumnToSearch (): Expression {
    return this.args.queryColumnToSearch as Expression;
  }

  get topK (): Expression {
    return this.args.topK as Expression;
  }

  get distanceType (): DataTypeExpr {
    return this.args.distanceType as DataTypeExpr;
  }

  get options (): Expression[] {
    return (this.args.options || []) as Expression[];
  }
}

export class PiExpr extends FuncExpr {
  key = ExpressionKey.PI;
}

export class PowExpr extends BinaryExpr {
  key = ExpressionKey.POW;
}

export type ApproxPercentileEstimateExprArgs = { percentile: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ApproxPercentileEstimateExpr extends FuncExpr {
  key = ExpressionKey.APPROX_PERCENTILE_ESTIMATE;

  /**
   * Defines the arguments (properties and child expressions) for ApproxPercentileEstimate expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    percentile: true,
  };

  constructor (args: ApproxPercentileEstimateExprArgs) {
    super(args);
  }

  get percentile (): Expression {
    return this.args.percentile as Expression;
  }
}

export class QuarterExpr extends FuncExpr {
  key = ExpressionKey.QUARTER;
}

export type RandExprArgs = { lower?: Expression; upper?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class RandExpr extends FuncExpr {
  key = ExpressionKey.RAND;

  /**
   * Defines the arguments (properties and child expressions) for Rand expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    lower: false,
    upper: false,
  };

  constructor (args: RandExprArgs = {}) {
    super(args);
  }

  get lower (): Expression {
    return this.args.lower as Expression;
  }

  get upper (): Expression {
    return this.args.upper as Expression;
  }
}

export class RandnExpr extends FuncExpr {
  key = ExpressionKey.RANDN;
}

export type RandstrExprArgs = { generator?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class RandstrExpr extends FuncExpr {
  key = ExpressionKey.RANDSTR;

  /**
   * Defines the arguments (properties and child expressions) for Randstr expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    generator: false,
  };

  constructor (args: RandstrExprArgs = {}) {
    super(args);
  }

  get generator (): Expression {
    return this.args.generator as Expression;
  }
}

export type RangeNExprArgs = { each?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class RangeNExpr extends FuncExpr {
  key = ExpressionKey.RANGE_N;

  /**
   * Defines the arguments (properties and child expressions) for RangeN expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    each: false,
  };

  constructor (args: RangeNExprArgs = {}) {
    super(args);
  }

  get each (): Expression {
    return this.args.each as Expression;
  }
}

export class RangeBucketExpr extends FuncExpr {
  key = ExpressionKey.RANGE_BUCKET;
}

export class ReadCSVExpr extends FuncExpr {
  key = ExpressionKey.READ_CSV;
}

export class ReadParquetExpr extends FuncExpr {
  key = ExpressionKey.READ_PARQUET;
}

export type ReduceExprArgs = { initial: Expression; merge: Expression; finish?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ReduceExpr extends FuncExpr {
  key = ExpressionKey.REDUCE;

  /**
   * Defines the arguments (properties and child expressions) for Reduce expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    initial: true,
    merge: true,
    finish: false,
  };

  constructor (args: ReduceExprArgs) {
    super(args);
  }

  get initial (): Expression {
    return this.args.initial as Expression;
  }

  get merge (): Expression {
    return this.args.merge as Expression;
  }

  get finish (): Expression {
    return this.args.finish as Expression;
  }
}

export type RegexpExtractExprArgs = { position?: Expression; occurrence?: Expression; parameters?: Expression[]; group?: Expression; nullIfPosOverflow?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class RegexpExtractExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_EXTRACT;

  /**
   * Defines the arguments (properties and child expressions) for RegexpExtract expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    position: false,
    occurrence: false,
    parameters: false,
    group: false,
    nullIfPosOverflow: false,
  };

  constructor (args: RegexpExtractExprArgs = {}) {
    super(args);
  }

  get position (): Expression {
    return this.args.position as Expression;
  }

  get occurrence (): Expression {
    return this.args.occurrence as Expression;
  }

  get parameters (): Expression[] {
    return (this.args.parameters || []) as Expression[];
  }

  get group (): Expression {
    return this.args.group as Expression;
  }

  get nullIfPosOverflow (): Expression {
    return this.args.nullIfPosOverflow as Expression;
  }
}

export type RegexpExtractAllExprArgs = { group?: Expression; parameters?: Expression[]; position?: Expression; occurrence?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class RegexpExtractAllExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_EXTRACT_ALL;

  /**
   * Defines the arguments (properties and child expressions) for RegexpExtractAll expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    group: false,
    parameters: false,
    position: false,
    occurrence: false,
  };

  constructor (args: RegexpExtractAllExprArgs = {}) {
    super(args);
  }

  get group (): Expression {
    return this.args.group as Expression;
  }

  get parameters (): Expression[] {
    return (this.args.parameters || []) as Expression[];
  }

  get position (): Expression {
    return this.args.position as Expression;
  }

  get occurrence (): Expression {
    return this.args.occurrence as Expression;
  }
}

export type RegexpReplaceExprArgs = { replacement?: boolean; position?: Expression; occurrence?: Expression; modifiers?: Expression[]; singleReplace?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class RegexpReplaceExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_REPLACE;

  /**
   * Defines the arguments (properties and child expressions) for RegexpReplace expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    replacement: false,
    position: false,
    occurrence: false,
    modifiers: false,
    singleReplace: false,
  };

  constructor (args: RegexpReplaceExprArgs = {}) {
    super(args);
  }

  get replacement (): boolean {
    return this.args.replacement as boolean;
  }

  get position (): Expression {
    return this.args.position as Expression;
  }

  get occurrence (): Expression {
    return this.args.occurrence as Expression;
  }

  get modifiers (): Expression[] {
    return (this.args.modifiers || []) as Expression[];
  }

  get singleReplace (): Expression {
    return this.args.singleReplace as Expression;
  }
}

export type RegexpLikeExprArgs = { flag?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class RegexpLikeExpr extends BinaryExpr {
  key = ExpressionKey.REGEXP_LIKE;

  /**
   * Defines the arguments (properties and child expressions) for RegexpLike expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    flag: false,
  };

  constructor (args: RegexpLikeExprArgs = {}) {
    super(args);
  }

  get flag (): Expression {
    return this.args.flag as Expression;
  }
}

export type RegexpILikeExprArgs = { flag?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class RegexpILikeExpr extends BinaryExpr {
  key = ExpressionKey.REGEXP_ILIKE;

  /**
   * Defines the arguments (properties and child expressions) for RegexpILike expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    flag: false,
  };

  constructor (args: RegexpILikeExprArgs = {}) {
    super(args);
  }

  get flag (): Expression {
    return this.args.flag as Expression;
  }
}

export type RegexpFullMatchExprArgs = { options?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class RegexpFullMatchExpr extends BinaryExpr {
  key = ExpressionKey.REGEXP_FULL_MATCH;

  /**
   * Defines the arguments (properties and child expressions) for RegexpFullMatch expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    options: false,
  };

  constructor (args: RegexpFullMatchExprArgs = {}) {
    super(args);
  }

  get options (): Expression[] {
    return (this.args.options || []) as Expression[];
  }
}

export type RegexpInstrExprArgs = { position?: Expression; occurrence?: Expression; option?: Expression; parameters?: Expression[]; group?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class RegexpInstrExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_INSTR;

  /**
   * Defines the arguments (properties and child expressions) for RegexpInstr expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    position: false,
    occurrence: false,
    option: false,
    parameters: false,
    group: false,
  };

  constructor (args: RegexpInstrExprArgs = {}) {
    super(args);
  }

  get position (): Expression {
    return this.args.position as Expression;
  }

  get occurrence (): Expression {
    return this.args.occurrence as Expression;
  }

  get option (): Expression {
    return this.args.option as Expression;
  }

  get parameters (): Expression[] {
    return (this.args.parameters || []) as Expression[];
  }

  get group (): Expression {
    return this.args.group as Expression;
  }
}

export type RegexpSplitExprArgs = { limit?: number | Expression; [key: string]: unknown } & BaseExpressionArgs;

export class RegexpSplitExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_SPLIT;

  /**
   * Defines the arguments (properties and child expressions) for RegexpSplit expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    limit: false,
  };

  constructor (args: RegexpSplitExprArgs = {}) {
    super(args);
  }

  get limit (): number | Expression {
    return this.args.limit as number | Expression;
  }
}

export type RegexpCountExprArgs = { position?: Expression; parameters?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class RegexpCountExpr extends FuncExpr {
  key = ExpressionKey.REGEXP_COUNT;

  /**
   * Defines the arguments (properties and child expressions) for RegexpCount expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    position: false,
    parameters: false,
  };

  constructor (args: RegexpCountExprArgs = {}) {
    super(args);
  }

  get position (): Expression {
    return this.args.position as Expression;
  }

  get parameters (): Expression[] {
    return (this.args.parameters || []) as Expression[];
  }
}

export type RepeatExprArgs = { times: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class RepeatExpr extends FuncExpr {
  key = ExpressionKey.REPEAT;

  /**
   * Defines the arguments (properties and child expressions) for Repeat expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    times: true,
  };

  constructor (args: RepeatExprArgs) {
    super(args);
  }

  get times (): Expression[] {
    return (this.args.times || []) as Expression[];
  }
}

export type ReplaceExprArgs = { replacement?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class ReplaceExpr extends FuncExpr {
  key = ExpressionKey.REPLACE;

  /**
   * Defines the arguments (properties and child expressions) for Replace expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    replacement: false,
  };

  constructor (args: ReplaceExprArgs = {}) {
    super(args);
  }

  get replacement (): boolean {
    return this.args.replacement as boolean;
  }
}

export class RadiansExpr extends FuncExpr {
  key = ExpressionKey.RADIANS;
}

export type RoundExprArgs = { decimals?: Expression[]; truncate?: Expression; castsNonIntegerDecimals?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class RoundExpr extends FuncExpr {
  key = ExpressionKey.ROUND;

  /**
   * Defines the arguments (properties and child expressions) for Round expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    decimals: false,
    truncate: false,
    castsNonIntegerDecimals: false,
  };

  constructor (args: RoundExprArgs = {}) {
    super(args);
  }

  get decimals (): Expression[] {
    return (this.args.decimals || []) as Expression[];
  }

  get truncate (): Expression {
    return this.args.truncate as Expression;
  }

  get castsNonIntegerDecimals (): Expression[] {
    return (this.args.castsNonIntegerDecimals || []) as Expression[];
  }
}

export type TruncExprArgs = { decimals?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class TruncExpr extends FuncExpr {
  key = ExpressionKey.TRUNC;

  /**
   * Defines the arguments (properties and child expressions) for Trunc expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    decimals: false,
  };

  constructor (args: TruncExprArgs = {}) {
    super(args);
  }

  get decimals (): Expression[] {
    return (this.args.decimals || []) as Expression[];
  }
}

export class RowNumberExpr extends FuncExpr {
  key = ExpressionKey.ROW_NUMBER;
}

export class Seq1Expr extends FuncExpr {
  key = ExpressionKey.SEQ1;
}

export class Seq2Expr extends FuncExpr {
  key = ExpressionKey.SEQ2;
}

export class Seq4Expr extends FuncExpr {
  key = ExpressionKey.SEQ4;
}

export class Seq8Expr extends FuncExpr {
  key = ExpressionKey.SEQ8;
}

export class SafeAddExpr extends FuncExpr {
  key = ExpressionKey.SAFE_ADD;
}

export class SafeDivideExpr extends FuncExpr {
  key = ExpressionKey.SAFE_DIVIDE;
}

export class SafeMultiplyExpr extends FuncExpr {
  key = ExpressionKey.SAFE_MULTIPLY;
}

export class SafeNegateExpr extends FuncExpr {
  key = ExpressionKey.SAFE_NEGATE;
}

export class SafeSubtractExpr extends FuncExpr {
  key = ExpressionKey.SAFE_SUBTRACT;
}

export class SafeConvertBytesToStringExpr extends FuncExpr {
  key = ExpressionKey.SAFE_CONVERT_BYTES_TO_STRING;
}

export class SHAExpr extends FuncExpr {
  key = ExpressionKey.SHA;
  static sqlNames = ['SHA', 'SHA1'];
}

export type SHA2ExprArgs = { length?: number | Expression; [key: string]: unknown } & BaseExpressionArgs;

export class SHA2Expr extends FuncExpr {
  key = ExpressionKey.SHA2;

  /**
   * Defines the arguments (properties and child expressions) for SHA2 expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    length: false,
  };

  static {
    this.register();
  }

  constructor (args: SHA2ExprArgs = {}) {
    super(args);
  }

  get length (): number | Expression {
    return this.args.length as number | Expression;
  }
}

export class SHA1DigestExpr extends FuncExpr {
  key = ExpressionKey.SHA1_DIGEST;
}

export type SHA2DigestExprArgs = { length?: number | Expression; [key: string]: unknown } & BaseExpressionArgs;

export class SHA2DigestExpr extends FuncExpr {
  key = ExpressionKey.SHA2_DIGEST;

  /**
   * Defines the arguments (properties and child expressions) for SHA2Digest expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    length: false,
  };

  constructor (args: SHA2DigestExprArgs = {}) {
    super(args);
  }

  get length (): number | Expression {
    return this.args.length as number | Expression;
  }
}

export class SignExpr extends FuncExpr {
  key = ExpressionKey.SIGN;
  static sqlNames = ['SIGN', 'SIGNUM'];
}

export type SortArrayExprArgs = { asc?: Expression; nullsFirst?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class SortArrayExpr extends FuncExpr {
  key = ExpressionKey.SORT_ARRAY;

  /**
   * Defines the arguments (properties and child expressions) for SortArray expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    asc: false,
    nullsFirst: false,
  };

  static {
    this.register();
  }

  constructor (args: SortArrayExprArgs = {}) {
    super(args);
  }

  get asc (): Expression {
    return this.args.asc as Expression;
  }

  get nullsFirst (): Expression {
    return this.args.nullsFirst as Expression;
  }
}

export class SoundexExpr extends FuncExpr {
  key = ExpressionKey.SOUNDEX;
}

export class SoundexP123Expr extends FuncExpr {
  key = ExpressionKey.SOUNDEX_P123;
}

export type SplitExprArgs = { limit?: number | Expression; [key: string]: unknown } & BaseExpressionArgs;

export class SplitExpr extends FuncExpr {
  key = ExpressionKey.SPLIT;

  /**
   * Defines the arguments (properties and child expressions) for Split expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    limit: false,
  };

  constructor (args: SplitExprArgs = {}) {
    super(args);
  }

  get limit (): number | Expression {
    return this.args.limit as number | Expression;
  }
}

export type SplitPartExprArgs = { delimiter?: number | Expression; partIndex?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class SplitPartExpr extends FuncExpr {
  key = ExpressionKey.SPLIT_PART;

  /**
   * Defines the arguments (properties and child expressions) for SplitPart expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    delimiter: false,
    partIndex: false,
  };

  constructor (args: SplitPartExprArgs = {}) {
    super(args);
  }

  get delimiter (): number | Expression {
    return this.args.delimiter as number | Expression;
  }

  get partIndex (): Expression {
    return this.args.partIndex as Expression;
  }
}

export type SubstringExprArgs = { start?: Expression; length?: number | Expression; [key: string]: unknown } & BaseExpressionArgs;

export class SubstringExpr extends FuncExpr {
  key = ExpressionKey.SUBSTRING;

  /**
   * Defines the arguments (properties and child expressions) for Substring expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    start: false,
    length: false,
  };

  constructor (args: SubstringExprArgs = {}) {
    super(args);
  }

  get start (): Expression {
    return this.args.start as Expression;
  }

  get length (): number | Expression {
    return this.args.length as number | Expression;
  }
}

export type SubstringIndexExprArgs = { delimiter: number | Expression; count: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class SubstringIndexExpr extends FuncExpr {
  key = ExpressionKey.SUBSTRING_INDEX;

  /**
   * Defines the arguments (properties and child expressions) for SubstringIndex expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    delimiter: true,
    count: true,
  };

  constructor (args: SubstringIndexExprArgs) {
    super(args);
  }

  get delimiter (): number | Expression {
    return this.args.delimiter as number | Expression;
  }

  get count (): Expression {
    return this.args.count as Expression;
  }
}

export class StandardHashExpr extends FuncExpr {
  key = ExpressionKey.STANDARD_HASH;
}

export class StartsWithExpr extends FuncExpr {
  key = ExpressionKey.STARTS_WITH;
}

export class EndsWithExpr extends FuncExpr {
  key = ExpressionKey.ENDS_WITH;
}

export type StrPositionExprArgs = { substr: Expression; position?: Expression; occurrence?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class StrPositionExpr extends FuncExpr {
  key = ExpressionKey.STR_POSITION;

  /**
   * Defines the arguments (properties and child expressions) for StrPosition expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    substr: true,
    position: false,
    occurrence: false,
  };

  constructor (args: StrPositionExprArgs) {
    super(args);
  }

  get substr (): Expression {
    return this.args.substr as Expression;
  }

  get position (): Expression {
    return this.args.position as Expression;
  }

  get occurrence (): Expression {
    return this.args.occurrence as Expression;
  }
}

export type SearchExprArgs = { jsonScope?: Expression; analyzer?: Expression; analyzerOptions?: Expression[]; searchMode?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class SearchExpr extends FuncExpr {
  key = ExpressionKey.SEARCH;

  /**
   * Defines the arguments (properties and child expressions) for Search expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    jsonScope: false,
    analyzer: false,
    analyzerOptions: false,
    searchMode: false,
  };

  constructor (args: SearchExprArgs = {}) {
    super(args);
  }

  get jsonScope (): Expression {
    return this.args.jsonScope as Expression;
  }

  get analyzer (): Expression {
    return this.args.analyzer as Expression;
  }

  get analyzerOptions (): Expression[] {
    return (this.args.analyzerOptions || []) as Expression[];
  }

  get searchMode (): Expression {
    return this.args.searchMode as Expression;
  }
}

export class SearchIpExpr extends FuncExpr {
  key = ExpressionKey.SEARCH_IP;
}

export type StrToDateExprArgs = { format?: string; safe?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class StrToDateExpr extends FuncExpr {
  key = ExpressionKey.STR_TO_DATE;

  /**
   * Defines the arguments (properties and child expressions) for StrToDate expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: false,
    safe: false,
  };

  constructor (args: StrToDateExprArgs = {}) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }
}

export type StrToTimeExprArgs = { format: string; zone?: Expression; safe?: boolean; targetType?: DataTypeExpr; [key: string]: unknown } & BaseExpressionArgs;

export class StrToTimeExpr extends FuncExpr {
  key = ExpressionKey.STR_TO_TIME;

  /**
   * Defines the arguments (properties and child expressions) for StrToTime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: true,
    zone: false,
    safe: false,
    targetType: false,
  };

  constructor (args: StrToTimeExprArgs) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }

  get targetType (): DataTypeExpr {
    return this.args.targetType as DataTypeExpr;
  }
}

export type StrToUnixExprArgs = { format?: string; [key: string]: unknown } & BaseExpressionArgs;

export class StrToUnixExpr extends FuncExpr {
  key = ExpressionKey.STR_TO_UNIX;

  /**
   * Defines the arguments (properties and child expressions) for StrToUnix expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: false,
  };

  constructor (args: StrToUnixExprArgs = {}) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }
}

export type StrToMapExprArgs = { pairDelim?: Expression; keyValueDelim?: string; duplicateResolutionCallback?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class StrToMapExpr extends FuncExpr {
  key = ExpressionKey.STR_TO_MAP;

  /**
   * Defines the arguments (properties and child expressions) for StrToMap expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    pairDelim: false,
    keyValueDelim: false,
    duplicateResolutionCallback: false,
  };

  constructor (args: StrToMapExprArgs = {}) {
    super(args);
  }

  get pairDelim (): Expression {
    return this.args.pairDelim as Expression;
  }

  get keyValueDelim (): string {
    return this.args.keyValueDelim as string;
  }

  get duplicateResolutionCallback (): Expression {
    return this.args.duplicateResolutionCallback as Expression;
  }
}

export type NumberToStrExprArgs = { format: string; culture?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class NumberToStrExpr extends FuncExpr {
  key = ExpressionKey.NUMBER_TO_STR;

  /**
   * Defines the arguments (properties and child expressions) for NumberToStr expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: true,
    culture: false,
  };

  constructor (args: NumberToStrExprArgs) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }

  get culture (): Expression {
    return this.args.culture as Expression;
  }
}

export class FromBaseExpr extends FuncExpr {
  key = ExpressionKey.FROM_BASE;
}

export class SpaceExpr extends FuncExpr {
  key = ExpressionKey.SPACE;
}

export class StructExpr extends FuncExpr {
  key = ExpressionKey.STRUCT;
}

export class StructExtractExpr extends FuncExpr {
  key = ExpressionKey.STRUCT_EXTRACT;
}

export type StuffExprArgs = { start: Expression; length: number | Expression; [key: string]: unknown } & BaseExpressionArgs;

export class StuffExpr extends FuncExpr {
  key = ExpressionKey.STUFF;

  /**
   * Defines the arguments (properties and child expressions) for Stuff expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    start: true,
    length: true,
  };

  constructor (args: StuffExprArgs) {
    super(args);
  }

  get start (): Expression {
    return this.args.start as Expression;
  }

  get length (): number | Expression {
    return this.args.length as number | Expression;
  }
}

export class SqrtExpr extends FuncExpr {
  key = ExpressionKey.SQRT;
}

export type TimeExprArgs = { zone?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TimeExpr extends FuncExpr {
  key = ExpressionKey.TIME;

  /**
   * Defines the arguments (properties and child expressions) for Time expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    zone: false,
  };

  constructor (args: TimeExprArgs = {}) {
    super(args);
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }
}

export type TimeToStrExprArgs = { format: string; culture?: Expression; zone?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TimeToStrExpr extends FuncExpr {
  key = ExpressionKey.TIME_TO_STR;

  /**
   * Defines the arguments (properties and child expressions) for TimeToStr expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: true,
    culture: false,
    zone: false,
  };

  constructor (args: TimeToStrExprArgs) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }

  get culture (): Expression {
    return this.args.culture as Expression;
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }
}

export class TimeToTimeStrExpr extends FuncExpr {
  key = ExpressionKey.TIME_TO_TIME_STR;
}

export class TimeToUnixExpr extends FuncExpr {
  key = ExpressionKey.TIME_TO_UNIX;
}

export class TimeStrToDateExpr extends FuncExpr {
  key = ExpressionKey.TIME_STR_TO_DATE;
}

export type TimeStrToTimeExprArgs = { zone?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TimeStrToTimeExpr extends FuncExpr {
  key = ExpressionKey.TIME_STR_TO_TIME;

  /**
   * Defines the arguments (properties and child expressions) for TimeStrToTime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    zone: false,
  };

  constructor (args: TimeStrToTimeExprArgs = {}) {
    super(args);
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }
}

export class TimeStrToUnixExpr extends FuncExpr {
  key = ExpressionKey.TIME_STR_TO_UNIX;
}

export type TrimExprArgs = { position?: Expression; collation?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TrimExpr extends FuncExpr {
  key = ExpressionKey.TRIM;

  /**
   * Defines the arguments (properties and child expressions) for Trim expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    position: false,
    collation: false,
  };

  constructor (args: TrimExprArgs = {}) {
    super(args);
  }

  get position (): Expression {
    return this.args.position as Expression;
  }

  get collation (): Expression {
    return this.args.collation as Expression;
  }
}

export type TsOrDsAddExprArgs = { unit?: Expression; returnType?: DataTypeExpr; [key: string]: unknown } & BaseExpressionArgs;

export class TsOrDsAddExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_ADD;

  /**
   * Defines the arguments (properties and child expressions) for TsOrDsAdd expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
    returnType: false,
  };

  constructor (args: TsOrDsAddExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }

  get returnType (): DataTypeExpr {
    return this.args.returnType as DataTypeExpr;
  }
}

export type TsOrDsDiffExprArgs = { unit?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TsOrDsDiffExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_DIFF;

  /**
   * Defines the arguments (properties and child expressions) for TsOrDsDiff expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    unit: false,
  };

  constructor (args: TsOrDsDiffExprArgs = {}) {
    super(args);
  }

  get unit (): Expression {
    return this.args.unit as Expression;
  }
}

export class TsOrDsToDateStrExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_TO_DATE_STR;
}

export type TsOrDsToDateExprArgs = { format?: string; safe?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class TsOrDsToDateExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_TO_DATE;

  /**
   * Defines the arguments (properties and child expressions) for TsOrDsToDate expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: false,
    safe: false,
  };

  constructor (args: TsOrDsToDateExprArgs = {}) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }
}

export class TsOrDsToDatetimeExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_TO_DATETIME;
}

export type TsOrDsToTimeExprArgs = { format?: string; safe?: boolean; [key: string]: unknown } & BaseExpressionArgs;

export class TsOrDsToTimeExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_TO_TIME;

  /**
   * Defines the arguments (properties and child expressions) for TsOrDsToTime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: false,
    safe: false,
  };

  constructor (args: TsOrDsToTimeExprArgs = {}) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }

  get safe (): boolean {
    return this.args.safe as boolean;
  }
}

export class TsOrDsToTimestampExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DS_TO_TIMESTAMP;
}

export class TsOrDiToDiExpr extends FuncExpr {
  key = ExpressionKey.TS_OR_DI_TO_DI;
}

export class UnhexExpr extends FuncExpr {
  key = ExpressionKey.UNHEX;
}

export class UnicodeExpr extends FuncExpr {
  key = ExpressionKey.UNICODE;
}

export type UniformExprArgs = { gen?: Expression; seed?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class UniformExpr extends FuncExpr {
  key = ExpressionKey.UNIFORM;

  /**
   * Defines the arguments (properties and child expressions) for Uniform expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    gen: false,
    seed: false,
  };

  constructor (args: UniformExprArgs = {}) {
    super(args);
  }

  get gen (): Expression {
    return this.args.gen as Expression;
  }

  get seed (): Expression {
    return this.args.seed as Expression;
  }
}

export class UnixDateExpr extends FuncExpr {
  key = ExpressionKey.UNIX_DATE;
}

export type UnixToStrExprArgs = { format?: string; [key: string]: unknown } & BaseExpressionArgs;

export class UnixToStrExpr extends FuncExpr {
  key = ExpressionKey.UNIX_TO_STR;

  /**
   * Defines the arguments (properties and child expressions) for UnixToStr expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    format: false,
  };

  constructor (args: UnixToStrExprArgs = {}) {
    super(args);
  }

  get format (): string {
    return this.args.format as string;
  }
}

export type UnixToTimeExprArgs = { scale?: number | Expression; zone?: Expression; hours?: Expression[]; minutes?: Expression[]; format?: string; targetType?: DataTypeExpr; [key: string]: unknown } & BaseExpressionArgs;

export class UnixToTimeExpr extends FuncExpr {
  key = ExpressionKey.UNIX_TO_TIME;

  /**
   * Defines the arguments (properties and child expressions) for UnixToTime expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    scale: false,
    zone: false,
    hours: false,
    minutes: false,
    format: false,
    targetType: false,
  };

  constructor (args: UnixToTimeExprArgs = {}) {
    super(args);
  }

  get scale (): number | Expression {
    return this.args.scale as number | Expression;
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }

  get hours (): Expression[] {
    return (this.args.hours || []) as Expression[];
  }

  get minutes (): Expression[] {
    return (this.args.minutes || []) as Expression[];
  }

  get format (): string {
    return this.args.format as string;
  }

  get targetType (): DataTypeExpr {
    return this.args.targetType as DataTypeExpr;
  }
}

export class UnixToTimeStrExpr extends FuncExpr {
  key = ExpressionKey.UNIX_TO_TIME_STR;
}

export class UnixSecondsExpr extends FuncExpr {
  key = ExpressionKey.UNIX_SECONDS;
}

export class UnixMicrosExpr extends FuncExpr {
  key = ExpressionKey.UNIX_MICROS;
}

export class UnixMillisExpr extends FuncExpr {
  key = ExpressionKey.UNIX_MILLIS;
}

export type UuidExprArgs = { name?: unknown; isString?: unknown; [key: string]: unknown } & BaseExpressionArgs;

export class UuidExpr extends FuncExpr {
  key = ExpressionKey.UUID;

  /**
   * Defines the arguments (properties and child expressions) for Uuid expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    name: false,
    isString: false,
  };

  constructor (args: UuidExprArgs = {}) {
    super(args);
  }
}

export type TimestampFromPartsExprArgs = { zone?: Expression; milli?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TimestampFromPartsExpr extends FuncExpr {
  key = ExpressionKey.TIMESTAMP_FROM_PARTS;

  /**
   * Defines the arguments (properties and child expressions) for TimestampFromParts expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    zone: false,
    milli: false,
  };

  constructor (args: TimestampFromPartsExprArgs = {}) {
    super(args);
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }

  get milli (): Expression {
    return this.args.milli as Expression;
  }
}

export type TimestampLtzFromPartsExprArgs = { zone?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TimestampLtzFromPartsExpr extends FuncExpr {
  key = ExpressionKey.TIMESTAMP_LTZ_FROM_PARTS;
  static sqlNames = ['TIMESTAMP_LTZ_FROM_PARTS', 'TIMESTAMPLTZFROMPARTS'];

  /**
   * Defines the arguments (properties and child expressions) for TimestampLtzFromParts expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    zone: false,
  };

  static {
    this.register();
  }

  constructor (args: TimestampLtzFromPartsExprArgs = {}) {
    super(args);
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }
}

export type TimestampTzFromPartsExprArgs = { zone?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class TimestampTzFromPartsExpr extends FuncExpr {
  key = ExpressionKey.TIMESTAMP_TZ_FROM_PARTS;

  /**
   * Defines the arguments (properties and child expressions) for TimestampTzFromParts expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    zone: false,
  };

  constructor (args: TimestampTzFromPartsExprArgs = {}) {
    super(args);
  }

  get zone (): Expression {
    return this.args.zone as Expression;
  }
}

export class UpperExpr extends FuncExpr {
  key = ExpressionKey.UPPER;
  static sqlNames = ['UPPER', 'UCASE'];
}

export type CorrExprArgs = { nullOnZeroVariance?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class CorrExpr extends BinaryExpr {
  key = ExpressionKey.CORR;

  /**
   * Defines the arguments (properties and child expressions) for Corr expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    nullOnZeroVariance: false,
  };

  constructor (args: CorrExprArgs = {}) {
    super(args);
  }

  get nullOnZeroVariance (): Expression {
    return this.args.nullOnZeroVariance as Expression;
  }
}

export type WidthBucketExprArgs = { minValue?: string; maxValue?: string; numBuckets?: Expression[]; threshold?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class WidthBucketExpr extends FuncExpr {
  key = ExpressionKey.WIDTH_BUCKET;

  /**
   * Defines the arguments (properties and child expressions) for WidthBucket expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    minValue: false,
    maxValue: false,
    numBuckets: false,
    threshold: false,
  };

  constructor (args: WidthBucketExprArgs = {}) {
    super(args);
  }

  get minValue (): string {
    return this.args.minValue as string;
  }

  get maxValue (): string {
    return this.args.maxValue as string;
  }

  get numBuckets (): Expression[] {
    return (this.args.numBuckets || []) as Expression[];
  }

  get threshold (): Expression {
    return this.args.threshold as Expression;
  }
}

export type WeekExprArgs = { mode?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class WeekExpr extends FuncExpr {
  key = ExpressionKey.WEEK;

  /**
   * Defines the arguments (properties and child expressions) for Week expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    mode: false,
  };

  constructor (args: WeekExprArgs = {}) {
    super(args);
  }

  get mode (): Expression {
    return this.args.mode as Expression;
  }
}

export class NextDayExpr extends FuncExpr {
  key = ExpressionKey.NEXT_DAY;
}

export type XMLElementExprArgs = { evalname?: string; [key: string]: unknown } & BaseExpressionArgs;

export class XMLElementExpr extends FuncExpr {
  key = ExpressionKey.XML_ELEMENT;

  /**
   * Defines the arguments (properties and child expressions) for XMLElement expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    evalname: false,
  };

  constructor (args: XMLElementExprArgs = {}) {
    super(args);
  }

  get evalname (): string {
    return this.args.evalname as string;
  }
}

export type XMLGetExprArgs = { instance?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class XMLGetExpr extends FuncExpr {
  key = ExpressionKey.XML_GET;

  /**
   * Defines the arguments (properties and child expressions) for XMLGet expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    instance: false,
  };

  constructor (args: XMLGetExprArgs = {}) {
    super(args);
  }

  get instance (): Expression {
    return this.args.instance as Expression;
  }
}

export type XMLTableExprArgs = { namespaces?: Expression[]; passing?: Expression; columns?: Expression[]; byRef?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class XMLTableExpr extends FuncExpr {
  key = ExpressionKey.XML_TABLE;

  /**
   * Defines the arguments (properties and child expressions) for XMLTable expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    namespaces: false,
    passing: false,
    columns: false,
    byRef: false,
  };

  constructor (args: XMLTableExprArgs = {}) {
    super(args);
  }

  get namespaces (): Expression[] {
    return (this.args.namespaces || []) as Expression[];
  }

  get passing (): Expression {
    return this.args.passing as Expression;
  }

  get columns (): Expression[] {
    return (this.args.columns || []) as Expression[];
  }

  get byRef (): Expression {
    return this.args.byRef as Expression;
  }
}

export class YearExpr extends FuncExpr {
  key = ExpressionKey.YEAR;
}

export type ZipfExprArgs = { elementcount: Expression; gen: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ZipfExpr extends FuncExpr {
  key = ExpressionKey.ZIPF;

  /**
   * Defines the arguments (properties and child expressions) for Zipf expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    elementcount: true,
    gen: true,
  };

  constructor (args: ZipfExprArgs) {
    super(args);
  }

  get elementcount (): Expression {
    return this.args.elementcount as Expression;
  }

  get gen (): Expression {
    return this.args.gen as Expression;
  }
}

export type NextValueForExprArgs = { order?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class NextValueForExpr extends FuncExpr {
  key = ExpressionKey.NEXT_VALUE_FOR;

  /**
   * Defines the arguments (properties and child expressions) for NextValueFor expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    order: false,
  };

  constructor (args: NextValueForExprArgs = {}) {
    super(args);
  }

  get order (): Expression {
    return this.args.order as Expression;
  }
}

export class AllExpr extends SubqueryPredicateExpr {
  key = ExpressionKey.ALL;
}

export class AnyExpr extends SubqueryPredicateExpr {
  key = ExpressionKey.ANY;
}

export class BitwiseAndAggExpr extends AggFuncExpr {
  key = ExpressionKey.BITWISE_AND_AGG;
}

export class BitwiseOrAggExpr extends AggFuncExpr {
  key = ExpressionKey.BITWISE_OR_AGG;
}

export class BitwiseXorAggExpr extends AggFuncExpr {
  key = ExpressionKey.BITWISE_XOR_AGG;
}

export class BoolxorAggExpr extends AggFuncExpr {
  key = ExpressionKey.BOOLXOR_AGG;
}

export class BitmapConstructAggExpr extends AggFuncExpr {
  key = ExpressionKey.BITMAP_CONSTRUCT_AGG;
}

export class BitmapOrAggExpr extends AggFuncExpr {
  key = ExpressionKey.BITMAP_OR_AGG;
}

export type ParameterizedAggExprArgs = { params: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class ParameterizedAggExpr extends AggFuncExpr {
  key = ExpressionKey.PARAMETERIZED_AGG;

  /**
   * Defines the arguments (properties and child expressions) for ParameterizedAgg expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    params: true,
  };

  constructor (args: ParameterizedAggExprArgs) {
    super(args);
  }

  get params (): Expression[] {
    return (this.args.params || []) as Expression[];
  }
}

export type ArgMaxExprArgs = { count?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ArgMaxExpr extends AggFuncExpr {
  key = ExpressionKey.ARG_MAX;
  static sqlNames = ['ARG_MAX', 'ARGMAX', 'MAX_BY'];

  /**
   * Defines the arguments (properties and child expressions) for ArgMax expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    count: false,
  };

  constructor (args: ArgMaxExprArgs = {}) {
    super(args);
  }

  get count (): Expression {
    return this.args.count as Expression;
  }
}

export type ArgMinExprArgs = { count?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ArgMinExpr extends AggFuncExpr {
  key = ExpressionKey.ARG_MIN;
  static sqlNames = ['ARG_MIN', 'ARGMIN', 'MIN_BY'];

  /**
   * Defines the arguments (properties and child expressions) for ArgMin expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    count: false,
  };

  constructor (args: ArgMinExprArgs = {}) {
    super(args);
  }

  get count (): Expression {
    return this.args.count as Expression;
  }
}

export type ApproxTopKExprArgs = { counters?: Expression[]; [key: string]: unknown } & BaseExpressionArgs;

export class ApproxTopKExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_TOP_K;

  /**
   * Defines the arguments (properties and child expressions) for ApproxTopK expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    counters: false,
  };

  constructor (args: ApproxTopKExprArgs = {}) {
    super(args);
  }

  get counters (): Expression[] {
    return (this.args.counters || []) as Expression[];
  }
}

export class ApproxTopKAccumulateExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_TOP_K_ACCUMULATE;
}

export class ApproxTopKCombineExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_TOP_K_COMBINE;
}

export type ApproxTopSumExprArgs = { count: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ApproxTopSumExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_TOP_SUM;

  /**
   * Defines the arguments (properties and child expressions) for ApproxTopSum expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    count: true,
  };

  constructor (args: ApproxTopSumExprArgs) {
    super(args);
  }

  get count (): Expression {
    return this.args.count as Expression;
  }
}

export class ApproxQuantilesExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_QUANTILES;
}

export class ApproxPercentileCombineExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_PERCENTILE_COMBINE;
}

export class MinhashExpr extends AggFuncExpr {
  key = ExpressionKey.MINHASH;
}

export class MinhashCombineExpr extends AggFuncExpr {
  key = ExpressionKey.MINHASH_COMBINE;
}

export class ApproximateSimilarityExpr extends AggFuncExpr {
  key = ExpressionKey.APPROXIMATE_SIMILARITY;
  static sqlNames = ['APPROXIMATE_SIMILARITY', 'APPROXIMATE_JACCARD_INDEX'];

  static {
    this.register();
  }
}

export class GroupingExpr extends AggFuncExpr {
  key = ExpressionKey.GROUPING;
}

export class GroupingIdExpr extends AggFuncExpr {
  key = ExpressionKey.GROUPING_ID;
}

export class AnonymousAggFuncExpr extends AggFuncExpr {
  key = ExpressionKey.ANONYMOUS_AGG_FUNC;
}

export class HashAggExpr extends AggFuncExpr {
  key = ExpressionKey.HASH_AGG;
}

export class HllExpr extends AggFuncExpr {
  key = ExpressionKey.HLL;
}

export type ApproxDistinctExprArgs = { accuracy?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ApproxDistinctExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_DISTINCT;
  static sqlNames = ['APPROX_DISTINCT', 'APPROX_COUNT_DISTINCT'];

  /**
   * Defines the arguments (properties and child expressions) for ApproxDistinct expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    accuracy: false,
  };

  constructor (args: ApproxDistinctExprArgs = {}) {
    super(args);
  }

  get accuracy (): Expression {
    return this.args.accuracy as Expression;
  }
}

export class ExplodingGenerateSeriesExpr extends GenerateSeriesExpr {
  key = ExpressionKey.EXPLODING_GENERATE_SERIES;
}

export type ArrayAggExprArgs = { nullsExcluded?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ArrayAggExpr extends AggFuncExpr {
  key = ExpressionKey.ARRAY_AGG;

  /**
   * Defines the arguments (properties and child expressions) for ArrayAgg expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    nullsExcluded: false,
  };

  constructor (args: ArrayAggExprArgs = {}) {
    super(args);
  }

  get nullsExcluded (): Expression {
    return this.args.nullsExcluded as Expression;
  }
}

export class ArrayUniqueAggExpr extends AggFuncExpr {
  key = ExpressionKey.ARRAY_UNIQUE_AGG;
}

export class AIAggExpr extends AggFuncExpr {
  key = ExpressionKey.AI_AGG;
}

export class AISummarizeAggExpr extends AggFuncExpr {
  key = ExpressionKey.AI_SUMMARIZE_AGG;
  static sqlNames = ['AI_SUMMARIZE_AGG'];

  static {
    this.register();
  }
}

export class ArrayConcatAggExpr extends AggFuncExpr {
  key = ExpressionKey.ARRAY_CONCAT_AGG;
}

export class ArrayUnionAggExpr extends AggFuncExpr {
  key = ExpressionKey.ARRAY_UNION_AGG;
}

export class AvgExpr extends AggFuncExpr {
  key = ExpressionKey.AVG;
}

export class AnyValueExpr extends AggFuncExpr {
  key = ExpressionKey.ANY_VALUE;
}

export type LagExprArgs = { offset?: boolean; default?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class LagExpr extends AggFuncExpr {
  key = ExpressionKey.LAG;

  /**
   * Defines the arguments (properties and child expressions) for Lag expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    offset: false,
    default: false,
  };

  constructor (args: LagExprArgs = {}) {
    super(args);
  }

  get offset (): boolean {
    return this.args.offset as boolean;
  }

  get default (): Expression {
    return this.args['default'] as Expression;
  }
}

export type LeadExprArgs = { offset?: boolean; default?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class LeadExpr extends AggFuncExpr {
  key = ExpressionKey.LEAD;

  /**
   * Defines the arguments (properties and child expressions) for Lead expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    offset: false,
    default: false,
  };

  constructor (args: LeadExprArgs = {}) {
    super(args);
  }

  get offset (): boolean {
    return this.args.offset as boolean;
  }

  get default (): Expression {
    return this.args['default'] as Expression;
  }
}

export class FirstExpr extends AggFuncExpr {
  key = ExpressionKey.FIRST;
}

export class LastExpr extends AggFuncExpr {
  key = ExpressionKey.LAST;
}

export class FirstValueExpr extends AggFuncExpr {
  key = ExpressionKey.FIRST_VALUE;
}

export class LastValueExpr extends AggFuncExpr {
  key = ExpressionKey.LAST_VALUE;
}

export type NthValueExprArgs = { offset: boolean; fromFirst?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class NthValueExpr extends AggFuncExpr {
  key = ExpressionKey.NTH_VALUE;

  /**
   * Defines the arguments (properties and child expressions) for NthValue expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    offset: true,
    fromFirst: false,
  };

  constructor (args: NthValueExprArgs) {
    super(args);
  }

  get offset (): boolean {
    return this.args.offset as boolean;
  }

  get fromFirst (): Expression {
    return this.args.fromFirst as Expression;
  }
}

export class ObjectAggExpr extends AggFuncExpr {
  key = ExpressionKey.OBJECT_AGG;
}

export type TryCastExprArgs = { to?: unknown; format?: unknown; safe?: unknown; action?: unknown; default?: unknown; requiresString?: Expression; [key: string]: unknown } & CastExprArgs;

export class TryCastExpr extends CastExpr {
  key = ExpressionKey.TRY_CAST;

  /**
   * Defines the arguments (properties and child expressions) for TryCast expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    to: false,
    format: false,
    safe: false,
    action: false,
    default: false,
    requiresString: false,
  };

  constructor (args: TryCastExprArgs) {
    super(args);
  }

  get requiresString (): Expression {
    return this.args.requiresString as Expression;
  }
}

export class JSONCastExpr extends CastExpr {
  key = ExpressionKey.JSON_CAST;
}

export class ConcatWsExpr extends ConcatExpr {
  key = ExpressionKey.CONCAT_WS;
}

export type CountExprArgs = { bigInt?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class CountExpr extends AggFuncExpr {
  key = ExpressionKey.COUNT;

  /**
   * Defines the arguments (properties and child expressions) for Count expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    bigInt: false,
  };

  constructor (args: CountExprArgs = {}) {
    super(args);
  }

  get bigInt (): Expression {
    return this.args.bigInt as Expression;
  }
}

export class CountIfExpr extends AggFuncExpr {
  key = ExpressionKey.COUNT_IF;
  static sqlNames = ['COUNT_IF', 'COUNTIF'];

  static {
    this.register();
  }
}

export class DenseRankExpr extends AggFuncExpr {
  key = ExpressionKey.DENSE_RANK;
}

export class ExplodeOuterExpr extends ExplodeExpr {
  key = ExpressionKey.EXPLODE_OUTER;
}

export class PosexplodeExpr extends ExplodeExpr {
  key = ExpressionKey.POSEXPLODE;
}

export type GroupConcatExprArgs = { separator?: Expression; onOverflow?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class GroupConcatExpr extends AggFuncExpr {
  key = ExpressionKey.GROUP_CONCAT;

  /**
   * Defines the arguments (properties and child expressions) for GroupConcat expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    separator: false,
    onOverflow: false,
  };

  constructor (args: GroupConcatExprArgs = {}) {
    super(args);
  }

  get separator (): Expression {
    return this.args.separator as Expression;
  }

  get onOverflow (): Expression {
    return this.args.onOverflow as Expression;
  }
}

export class LowerHexExpr extends HexExpr {
  key = ExpressionKey.LOWER_HEX;
}

export class AndExpr extends ConnectorExpr {
  key = ExpressionKey.AND;
}

export class OrExpr extends ConnectorExpr {
  key = ExpressionKey.OR;
}

export type XorExprArgs = { roundInput?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class XorExpr extends ConnectorExpr {
  key = ExpressionKey.XOR;

  /**
   * Defines the arguments (properties and child expressions) for Xor expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    roundInput: false,
  };

  constructor (args: XorExprArgs = {}) {
    super(args);
  }

  get roundInput (): Expression {
    return this.args.roundInput as Expression;
  }
}

export type JSONObjectAggExprArgs = { nullHandling?: Expression; uniqueKeys?: Expression[]; returnType?: DataTypeExpr; encoding?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONObjectAggExpr extends AggFuncExpr {
  key = ExpressionKey.JSON_OBJECT_AGG;

  /**
   * Defines the arguments (properties and child expressions) for JSONObjectAgg expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    nullHandling: false,
    uniqueKeys: false,
    returnType: false,
    encoding: false,
  };

  constructor (args: JSONObjectAggExprArgs = {}) {
    super(args);
  }

  get nullHandling (): Expression {
    return this.args.nullHandling as Expression;
  }

  get uniqueKeys (): Expression[] {
    return (this.args.uniqueKeys || []) as Expression[];
  }

  get returnType (): DataTypeExpr {
    return this.args.returnType as DataTypeExpr;
  }

  get encoding (): Expression {
    return this.args.encoding as Expression;
  }
}

export class JSONBObjectAggExpr extends AggFuncExpr {
  key = ExpressionKey.JSONB_OBJECT_AGG;
}

export type JSONArrayAggExprArgs = { order?: Expression; nullHandling?: Expression; returnType?: DataTypeExpr; strict?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class JSONArrayAggExpr extends AggFuncExpr {
  key = ExpressionKey.JSON_ARRAY_AGG;

  /**
   * Defines the arguments (properties and child expressions) for JSONArrayAgg expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    order: false,
    nullHandling: false,
    returnType: false,
    strict: false,
  };

  constructor (args: JSONArrayAggExprArgs = {}) {
    super(args);
  }

  get order (): Expression {
    return this.args.order as Expression;
  }

  get nullHandling (): Expression {
    return this.args.nullHandling as Expression;
  }

  get returnType (): DataTypeExpr {
    return this.args.returnType as DataTypeExpr;
  }

  get strict (): Expression {
    return this.args.strict as Expression;
  }
}

export class LogicalOrExpr extends AggFuncExpr {
  key = ExpressionKey.LOGICAL_OR;
  static sqlNames = ['LOGICAL_OR', 'BOOL_OR', 'BOOLOR_AGG'];

  static {
    this.register();
  }
}

export class LogicalAndExpr extends AggFuncExpr {
  key = ExpressionKey.LOGICAL_AND;
  static sqlNames = ['LOGICAL_AND', 'BOOL_AND', 'BOOLAND_AGG'];

  static {
    this.register();
  }
}

export class MaxExpr extends AggFuncExpr {
  key = ExpressionKey.MAX;
}

export class MedianExpr extends AggFuncExpr {
  key = ExpressionKey.MEDIAN;
}

export type ModeExprArgs = { deterministic?: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class ModeExpr extends AggFuncExpr {
  key = ExpressionKey.MODE;

  /**
   * Defines the arguments (properties and child expressions) for Mode expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    deterministic: false,
  };

  constructor (args: ModeExprArgs = {}) {
    super(args);
  }

  get deterministic (): Expression {
    return this.args.deterministic as Expression;
  }
}

export class MinExpr extends AggFuncExpr {
  key = ExpressionKey.MIN;
}

export class NtileExpr extends AggFuncExpr {
  key = ExpressionKey.NTILE;
}

export class PercentileContExpr extends AggFuncExpr {
  key = ExpressionKey.PERCENTILE_CONT;
}

export class PercentileDiscExpr extends AggFuncExpr {
  key = ExpressionKey.PERCENTILE_DISC;
}

export class PercentRankExpr extends AggFuncExpr {
  key = ExpressionKey.PERCENT_RANK;
}

export type QuantileExprArgs = { quantile: Expression; [key: string]: unknown } & BaseExpressionArgs;

export class QuantileExpr extends AggFuncExpr {
  key = ExpressionKey.QUANTILE;

  /**
   * Defines the arguments (properties and child expressions) for Quantile expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    quantile: true,
  };

  constructor (args: QuantileExprArgs) {
    super(args);
  }

  get quantile (): Expression {
    return this.args.quantile as Expression;
  }
}

export class ApproxPercentileAccumulateExpr extends AggFuncExpr {
  key = ExpressionKey.APPROX_PERCENTILE_ACCUMULATE;
}

export class RankExpr extends AggFuncExpr {
  key = ExpressionKey.RANK;
}

export class RegrValxExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_VALX;
}

export class RegrValyExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_VALY;
}

export class RegrAvgyExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_AVGY;
}

export class RegrAvgxExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_AVGX;
}

export class RegrCountExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_COUNT;
}

export class RegrInterceptExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_INTERCEPT;
}

export class RegrR2Expr extends AggFuncExpr {
  key = ExpressionKey.REGR_R2;
}

export class RegrSxxExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_SXX;
}

export class RegrSxyExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_SXY;
}

export class RegrSyyExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_SYY;
}

export class RegrSlopeExpr extends AggFuncExpr {
  key = ExpressionKey.REGR_SLOPE;
}

export class SumExpr extends AggFuncExpr {
  key = ExpressionKey.SUM;
}

export class StddevExpr extends AggFuncExpr {
  key = ExpressionKey.STDDEV;
  static sqlNames = ['STDDEV', 'STDEV'];

  static {
    this.register();
  }
}

export class StddevPopExpr extends AggFuncExpr {
  key = ExpressionKey.STDDEV_POP;
}

export class StddevSampExpr extends AggFuncExpr {
  key = ExpressionKey.STDDEV_SAMP;
}

export class CumeDistExpr extends AggFuncExpr {
  key = ExpressionKey.CUME_DIST;
}

export class VarianceExpr extends AggFuncExpr {
  key = ExpressionKey.VARIANCE;
  static sqlNames = ['VARIANCE', 'VARIANCE_SAMP', 'VAR_SAMP'];

  static {
    this.register();
  }
}

export class VariancePopExpr extends AggFuncExpr {
  key = ExpressionKey.VARIANCE_POP;
  static sqlNames = ['VARIANCE_POP', 'VAR_POP'];

  static {
    this.register();
  }
}

export class KurtosisExpr extends AggFuncExpr {
  key = ExpressionKey.KURTOSIS;
}

export class SkewnessExpr extends AggFuncExpr {
  key = ExpressionKey.SKEWNESS;
}

export class CovarSampExpr extends AggFuncExpr {
  key = ExpressionKey.COVAR_SAMP;
}

export class CovarPopExpr extends AggFuncExpr {
  key = ExpressionKey.COVAR_POP;
}

export class CombinedAggFuncExpr extends AnonymousAggFuncExpr {
  key = ExpressionKey.COMBINED_AGG_FUNC;
}

export type CombinedParameterizedAggExprArgs = { params: Expression[]; [key: string]: unknown } & ParameterizedAggExprArgs;

export class CombinedParameterizedAggExpr extends ParameterizedAggExpr {
  key = ExpressionKey.COMBINED_PARAMETERIZED_AGG;

  /**
   * Defines the arguments (properties and child expressions) for CombinedParameterizedAgg expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    params: true,
  };

  constructor (args: CombinedParameterizedAggExprArgs) {
    super(args);
  }

  get params (): Expression[] {
    return (this.args.params || []) as Expression[];
  }
}

export class PosexplodeOuterExpr extends PosexplodeExpr {
  key = ExpressionKey.POSEXPLODE_OUTER;
}

export type ApproxQuantileExprArgs = { quantile: Expression; accuracy?: Expression; weight?: Expression; errorTolerance?: Expression; [key: string]: unknown } & QuantileExprArgs;

export class ApproxQuantileExpr extends QuantileExpr {
  key = ExpressionKey.APPROX_QUANTILE;

  /**
   * Defines the arguments (properties and child expressions) for ApproxQuantile expressions.
   * Each key represents an argument name, and the boolean indicates if it's required.
   */
  static argTypes = {
    quantile: true,
    accuracy: false,
    weight: false,
    errorTolerance: false,
  };

  constructor (args: ApproxQuantileExprArgs) {
    super(args);
  }

  get quantile (): Expression {
    return this.args.quantile as Expression;
  }

  get accuracy (): Expression {
    return this.args.accuracy as Expression;
  }

  get weight (): Expression {
    return this.args.weight as Expression;
  }

  get errorTolerance (): Expression {
    return this.args.errorTolerance as Expression;
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
 * const col = columnExpr('name');
 *
 * @example
 * // Qualified column: users.id
 * const col = columnExpr('id', 'users');
 */
export function columnExpr (name: string, table?: string): ColumnExpr {
  const args: Record<string, unknown> = {
    this: new IdentifierExpr({ this: name }),
  };
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
 * const table = tableExpr('users');
 *
 * @example
 * // Fully qualified: catalog.database.table
 * const table = tableExpr('users', 'mydb', 'mycatalog');
 */
export function tableExpr (name: string, db?: string, catalog?: string): TableExpr {
  const args: Record<string, unknown> = {
    this: new IdentifierExpr({ this: name }),
  };
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
 * const aliased = aliasExpr(columnExpr('col'), 'alias');
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
export function aliasExpr (
  expr: Expression,
  alias: string,
  options?: { quoted?: boolean; dialect?: DialectType; wrap?: boolean; [key: string]: unknown },
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
 * const condition = andExpr([cond1, cond2, cond3]);
 *
 * @example
 * // With options
 * const condition = andExpr([cond1, cond2], { wrap: true });
 */
export function andExpr (
  conditions: (Expression | undefined)[],
  _options?: { dialect?: DialectType; copy?: boolean; wrap?: boolean; [key: string]: unknown },
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
 * const condition = orExpr([cond1, cond2, cond3]);
 *
 * @example
 * // With options
 * const condition = orExpr([cond1, cond2], { wrap: true });
 */
export function orExpr (
  conditions: (Expression | undefined)[],
  _options?: { dialect?: DialectType; copy?: boolean; wrap?: boolean; [key: string]: unknown },
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
export function notExpr (expr: Expression): NotExpr {
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
 * const select = selectExpr('col1', 'col2');
 *
 * @example
 * // SELECT users.id, users.name
 * const select = selectExpr(
 *   columnExpr('id', 'users'),
 *   columnExpr('name', 'users')
 * );
 */
export function selectExpr (...columns: (string | Expression)[]): SelectExpr {
  const expressions = columns.map((col) =>
    typeof col === 'string' ? columnExpr(col) : col,
  );
  return new SelectExpr({ expressions });
}

/**
 * Create a FROM expression
 * @param table - Table or expression
 * @returns FROM expression
 */
export function fromExpr (table: string | Expression): FromExpr {
  const tableExpr = typeof table === 'string' ? new TableExpr({ this: new IdentifierExpr({ this: table }) }) : table;
  return new FromExpr({ expressions: [tableExpr] });
}

/**
 * Create a CASE expression
 * @param conditions - Condition-value pairs
 * @param defaultValue - Default value
 * @returns CASE expression
 */
export function caseExpr (conditions?: Expression[], defaultValue?: Expression): CaseExpr {
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
 * const casted = castExpr(columnExpr('col'), 'VARCHAR');
 *
 * @example
 * // CAST(value AS INTEGER)
 * const casted = castExpr(literalExpr('123'), DataTypeExpr.build('INTEGER'));
 */
export function castExpr (expr: Expression, toType: DataTypeExpr | string): CastExpr {
  const dataType = typeof toType === 'string' ? DataTypeExpr.build(toType) : toType;
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
export function funcExpr (name: string, ...args: Expression[]): FuncExpr {
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
  const args: Record<string, unknown> = { this: query };
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
 * const union = unionExpr(query1, query2);
 *
 * @example
 * // SELECT ... UNION DISTINCT SELECT ...
 * const union = unionExpr(query1, query2, true);
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
    [key: string]: unknown;
  } = {},
): S {
  const { distinct = true, dialect, copy = true, ...opts } = options;

  const parsedExpressions = expressions.map((e) =>
    maybeParse(e, { dialect, copy, ...opts }),
  );

  return parsedExpressions.reduce((left, right) =>
    new setOperation({
      this: left,
      expression: right,
      distinct,
      ...opts,
    }),
  ) as S;
}

/**
 * Initializes a syntax tree for the `UNION` operation.
 *
 * Example:
 *     unionExpr(["SELECT * FROM foo", "SELECT * FROM bla"]).sql();
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
export function unionExpr (
  expressions: Array<string | Expression>,
  options: {
    distinct?: boolean;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
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
export function intersectExpr (
  expressions: Array<string | Expression>,
  options: {
    distinct?: boolean;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
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
export function exceptExpr (
  expressions: Array<string | Expression>,
  options: {
    distinct?: boolean;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
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
export function insertExpr (table: Expression, values?: Expression): InsertExpr {
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
export function deleteExpr (table: Expression, where?: Expression): DeleteExpr {
  const args: Record<string, unknown> = { this: table };
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
export function conditionExpr (sql: string): Expression {
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
  let sql = String(sqlOrExpression);
  if (options?.prefix) {
    sql = `${options.prefix} ${sql}`;
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
  return columnExpr(sql);
}

/**
 * Convert SQL text to an identifier expression
 * @param name - Identifier name
 * @param quoted - Whether to quote the identifier
 * @returns Identifier expression
 */
export function toIdentifier (name: string, quoted = false): IdentifierExpr {
  return new IdentifierExpr({ this: name, quoted });
}

/**
 * Convert SQL text to a table expression
 * @param sql - SQL text
 * @param dialect - SQL dialect
 * @returns Table expression
 */
export function toTable (sql: string, _dialect?: DialectType): TableExpr {
  return tableExpr(sql);
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
 * const tables = findTables(selectExpr);
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
      if ((v !== null && v !== undefined && (!Array.isArray(v) || v.length > 0)) || verbose) {
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

    const isReprStr = node.isString || (node instanceof IdentifierExpr && node.quoted);
    const items = Object.entries(args)
      .map(([k, v]) => `${k}=${_toS(v, verbose, level + 1, isReprStr)}`)
      .join(delim);

    return `${node.constructor.name}(${items ? indent + items : ''})`;
  }

  if (Array.isArray(node)) {
    const items = node.map((i) => _toS(i, verbose, level + 1)).join(delim);
    return `[${items ? indent + items : ''}]`;
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
  [key: string]: unknown;
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
    [key: string]: unknown;
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

  const child = into ? new into({ expressions: allExpressions }) : new Expression({ expressions: allExpressions });
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
    [key: string]: unknown;
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
      }),
    );

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
    [key: string]: unknown;
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
      }),
    );

  let combined: Expression | undefined;
  if (parsedExpressions.length > 0) {
    combined = parsedExpressions.reduce((left, right) =>
      new AndExpr({ this: left, expression: right }),
    );
  }

  const existing = inst.args[arg] as Expression | undefined;
  if (append && existing && combined) {
    combined = new AndExpr({ this: existing, expression: combined });
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
  [key: string]: unknown;
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

  const asExpression = maybeParse(asExpr, { dialect, ...opts });

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

  const ctes = append ? [...existingCtes, cte] : [cte];

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
 * @returns The expression wrapped in parentheses if it matches the kind, otherwise the original expression
 */
export function _wrap (expression: Expression | undefined, kind: typeof Expression): Expression | undefined {
  if (expression instanceof kind) {
    return new ParenExpr({ this: expression });
  }
  return expression;
}

/**
 * Returns a Null expression
 * @returns A Null expression
 */
export function nullExpr (): NullExpr {
  return new NullExpr({});
}

/**
 * Returns a true Boolean expression
 * @returns A true Boolean expression
 */
export function trueExpr (): BooleanExpr {
  return new BooleanExpr({ this: true });
}

/**
 * Returns a false Boolean expression
 * @returns A false Boolean expression
 */
export function falseExpr (): BooleanExpr {
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
    return nullExpr();
  }

  // Handle numbers
  if (typeof value === 'number') {
    return LiteralExpr.number(value);
  }

  // Handle Date objects (datetime.datetime)
  if (value instanceof Date) {
    const datetimeLiteral = LiteralExpr.string(
      value.toISOString().replace('T', ' ').replace(/\.\d{3}Z$/, ''),
    );

    let tz: LiteralExpr | undefined;
    const timezoneOffset = value.getTimezoneOffset();
    if (timezoneOffset !== 0) {
      const hours = Math.floor(Math.abs(timezoneOffset) / 60);
      const minutes = Math.abs(timezoneOffset) % 60;
      const sign = timezoneOffset > 0 ? '-' : '+';
      tz = LiteralExpr.string(
        `${sign}${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}`,
      );
    }

    return new TimeStrToTimeExpr({ this: datetimeLiteral, zone: tz });
  }

  // Handle arrays
  if (Array.isArray(value)) {
    return new ArrayExpr({
      expressions: value.map((v) => convert(v, copy)),
    });
  }

  // Handle objects as structs
  if (value !== null && typeof value === 'object') {
    const entries = Object.entries(value);
    return new StructExpr({
      expressions: entries.map(([k, v]) =>
        new PropertyEQExpr({
          this: toIdentifier(k),
          expression: convert(v, copy),
        }),
      ),
    });
  }

  throw new Error(`Cannot convert ${value}`);
}

/** Percentile function classes */
export const PERCENTILES = [PercentileContExpr, PercentileDiscExpr] as const;

/** Non-null constant expression types */
export const NONNULL_CONSTANTS = [LiteralExpr, BooleanExpr] as const;

/** All constant expression types (including NULL) */
export const CONSTANTS = [LiteralExpr, BooleanExpr, NullExpr] as const;

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
