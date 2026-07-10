import {
  describe, test, expect,
} from 'vitest';
import {
  parseOne, ParseError,
  UnsupportedError,
} from '../../../src/index';
import type {
  Expression,
  SelectExpr,
} from '../../../src/expressions';
import {
  AggFuncExpr, AlterExpr, AlterSessionExpr, AnonymousExpr,
  BooleanExpr,
  column,
  ColumnExpr, DateTruncExpr, DirectoryStageExpr, EqExpr, ExplodeExpr,
  ExtractExpr, GetExpr, GetExtractExpr, IdentifierExpr, LiteralExpr,
  PlaceholderExpr, PutExpr, SearchExpr, SearchIpExpr, select, SetItemExpr,
  SwapTableExpr, TableExpr, TruncExpr, TsOrDsToDateExpr, VarExpr,
  WindowExpr,
} from '../../../src/expressions';
import {
  annotateTypes,
  normalizeIdentifiers,
  quoteIdentifiers,
} from '../../../src/optimizer';
import {
  Validator,
} from './validator';

class TestSnowflake extends Validator {
  override dialect = 'snowflake' as const;

  testSnowflake () {
    this.validateIdentity(
      'SELECT * FROM x ASOF JOIN y OFFSET MATCH_CONDITION (x.a > y.a)',
      'SELECT * FROM x ASOF JOIN y AS OFFSET MATCH_CONDITION (x.a > y.a)',
    );
    this.validateIdentity(
      'SELECT * FROM x ASOF JOIN y LIMIT MATCH_CONDITION (x.a > y.a)',
      'SELECT * FROM x ASOF JOIN y AS LIMIT MATCH_CONDITION (x.a > y.a)',
    );

    this.validateIdentity('SELECT session');
    this.validateIdentity('x::nvarchar()', 'CAST(x AS VARCHAR)');

    let ast = this.parseOne('DATEADD(DAY, n, d)');

    ast.setArgKey('unit', LiteralExpr.string('MONTH'));
    expect(ast.sql({
      dialect: 'snowflake',
    })).toBe('DATEADD(MONTH, n, d)');

    this.validateIdentity('SELECT DATE_PART(EPOCH_MILLISECOND, CURRENT_TIMESTAMP()) AS a');
    this.validateIdentity('SELECT GET(a, b)');
    this.validateIdentity('SELECT HASH_AGG(a, b, c, d)');
    this.validateIdentity('SELECT GREATEST(1, 2, 3, NULL)');
    this.validateIdentity('SELECT GREATEST_IGNORE_NULLS(1, 2, 3, NULL)');
    this.validateIdentity('SELECT LEAST(5, NULL, 7, 3)');
    this.validateIdentity('SELECT LEAST_IGNORE_NULLS(5, NULL, 7, 3)');
    this.validateIdentity('SELECT MAX(x)');
    this.validateIdentity('SELECT COUNT(x)');
    this.validateIdentity('SELECT MIN(amount)');
    this.validateIdentity('SELECT MODE(x)');
    this.validateIdentity('SELECT MODE(status) OVER (PARTITION BY region) FROM orders');
    this.validateIdentity('SELECT TAN(x)');
    this.validateIdentity('SELECT COS(x)');
    this.validateIdentity('SELECT SINH(1.5)');
    this.validateIdentity('SELECT MOD(x, y)', 'SELECT x % y');
    this.validateIdentity('SELECT ROUND(x)');
    this.validateIdentity('SELECT ROUND(123.456, -1)');
    this.validateIdentity('SELECT ROUND(123.456, 2, \'HALF_AWAY_FROM_ZERO\')');

    this.validateIdentity('SELECT FLOOR(x)');
    this.validateIdentity('SELECT FLOOR(135.135, 1)');
    this.validateIdentity('SELECT FLOOR(x, -1)');
    this.validateIdentity(
      'SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary) FROM employees',
    );
    // Ensures we don't fail when generating ParseJSON with the `safe` arg set to `true`
    expect(
      this.validateIdentity('SELECT TRY_PARSE_JSON(\'{"x: 1}\')').sql(),
    ).toBe('SELECT PARSE_JSON(\'{"x: 1}\')');

    this.validateIdentity(
      'SELECT APPROX_TOP_K(col) FROM t',
      'SELECT APPROX_TOP_K(col, 1) FROM t',
    );
    this.validateIdentity('SELECT APPROX_TOP_K(category, 3) FROM t');
    this.validateIdentity('APPROX_TOP_K(C4, 3, 5)').assertIs(AggFuncExpr);

    this.validateIdentity('SELECT MINHASH(5, col)');
    this.validateIdentity('SELECT MINHASH(5, col1, col2)');
    this.validateIdentity('SELECT MINHASH(5, *)');
    this.validateIdentity('SELECT MINHASH_COMBINE(minhash_col)');
    this.validateIdentity('SELECT APPROXIMATE_SIMILARITY(minhash_col)');
    this.validateIdentity(
      'SELECT APPROXIMATE_JACCARD_INDEX(minhash_col)',
      'SELECT APPROXIMATE_SIMILARITY(minhash_col)',
    );
    this.validateIdentity('SELECT APPROX_PERCENTILE_ACCUMULATE(col)');
    this.validateIdentity('SELECT APPROX_PERCENTILE_ESTIMATE(state, 0.5)');
    this.validateIdentity('SELECT APPROX_TOP_K_ACCUMULATE(col, 10)');
    this.validateIdentity('SELECT APPROX_TOP_K_COMBINE(state, 2)');
    this.validateIdentity('SELECT APPROX_TOP_K_COMBINE(state)');
    this.validateIdentity('SELECT APPROX_TOP_K_ESTIMATE(state_column, 4)');
    this.validateIdentity('SELECT APPROX_TOP_K_ESTIMATE(state_column)');
    this.validateIdentity('SELECT APPROX_PERCENTILE_COMBINE(state_column)');
    this.validateIdentity('SELECT EQUAL_NULL(1, 2)');
    this.validateIdentity('SELECT EXP(1)');
    this.validateIdentity('SELECT FACTORIAL(5)');
    this.validateIdentity('SELECT BIT_LENGTH(\'abc\')');
    this.validateIdentity('SELECT BIT_LENGTH(x\'A1B2\')');
    this.validateAll(
      'SELECT BITMAP_BIT_POSITION(10)',
      {
        write: {
          'duckdb': 'SELECT (CASE WHEN 10 > 0 THEN 10 - 1 ELSE ABS(10) END) % 32768',
          'snowflake': 'SELECT BITMAP_BIT_POSITION(10)',
        },
      },
    );
    this.validateIdentity('SELECT BITMAP_BUCKET_NUMBER(32769)');
    this.validateIdentity('SELECT BITMAP_CONSTRUCT_AGG(value)');
    this.validateAll(
      'SELECT BITMAP_CONSTRUCT_AGG(v) FROM t',
      {
        write: {
          'snowflake': 'SELECT BITMAP_CONSTRUCT_AGG(v) FROM t',
          'duckdb': 'SELECT (SELECT CASE WHEN l IS NULL OR LENGTH(l) = 0 THEN NULL WHEN LENGTH(l) <> LENGTH(LIST_FILTER(l, __v -> __v BETWEEN 0 AND 32767)) THEN NULL WHEN LENGTH(l) < 5 THEN UNHEX(PRINTF(\'%04X\', LENGTH(l)) || h || REPEAT(\'00\', GREATEST(0, 4 - LENGTH(l)) * 2)) ELSE UNHEX(\'08000000000000000000\' || h) END FROM (SELECT l, COALESCE(LIST_REDUCE(LIST_TRANSFORM(l, __x -> PRINTF(\'%02X%02X\', CAST(__x AS INT) & 255, (CAST(__x AS INT) >> 8) & 255)), (__a, __b) -> __a || __b, \'\'), \'\') AS h FROM (SELECT LIST_SORT(LIST_DISTINCT(LIST(v) FILTER(WHERE NOT v IS NULL))) AS l))) FROM t',
        },
      },
    );
    this.validateIdentity(
      'SELECT BITMAP_COUNT(BITMAP_CONSTRUCT_AGG(value)) FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT(1, 2, 3, 5)))',
      'SELECT BITMAP_COUNT(BITMAP_CONSTRUCT_AGG(value)) FROM TABLE(FLATTEN(INPUT => [1, 2, 3, 5]))',
    );
    this.validateIdentity('SELECT BOOLAND(1, -2)');
    this.validateIdentity('SELECT BOOLXOR(2, 0)');
    this.validateIdentity('SELECT BOOLOR(1, 0)');
    this.validateIdentity('SELECT TO_BOOLEAN(\'true\')');
    this.validateIdentity('SELECT TO_BOOLEAN(1)');
    this.validateIdentity('SELECT IS_NULL_VALUE(GET_PATH(payload, \'field\'))');
    this.validateIdentity('SELECT RTRIMMED_LENGTH(\' ABCD \')');
    this.validateIdentity('SELECT HEX_DECODE_STRING(\'48656C6C6F\')');
    this.validateIdentity('SELECT HEX_ENCODE(\'Hello World\')');
    this.validateIdentity('SELECT HEX_ENCODE(\'Hello World\', 1)');
    this.validateIdentity('SELECT HEX_ENCODE(\'Hello World\', 0)');
    this.validateIdentity('SELECT IFNULL(col1, col2)', 'SELECT COALESCE(col1, col2)');
    this.validateIdentity('SELECT NEXT_DAY(\'2025-10-15\', \'FRIDAY\')');
    this.validateIdentity('SELECT NVL2(col1, col2, col3)');
    this.validateIdentity('SELECT NVL(col1, col2)', 'SELECT COALESCE(col1, col2)');
    this.validateIdentity('SELECT CHR(8364)');
    this.validateIdentity('SELECT CHECK_JSON(\'{"key": "value"}\')');
    this.validateIdentity(
      'SELECT CHECK_XML(\'<root><key attribute="attr">value</key></root>\')',
    );
    this.validateIdentity(
      'SELECT CHECK_XML(\'<root><key attribute="attr">value</key></root>\', TRUE)',
    );
    this.validateIdentity('SELECT COMPRESS(\'Hello World\', \'ZLIB\')');
    this.validateIdentity('SELECT DECOMPRESS_BINARY(\'compressed_data\', \'SNAPPY\')');
    this.validateIdentity('SELECT DECOMPRESS_STRING(\'compressed_data\', \'ZSTD\')');
    this.validateIdentity('SELECT LPAD(\'Hello\', 10, \'*\')');
    this.validateIdentity('SELECT LPAD(tbl.bin_col, 10)');
    this.validateIdentity('SELECT RPAD(\'Hello\', 10, \'*\')');
    this.validateIdentity('SELECT RPAD(tbl.bin_col, 10)');

    this.validateAll(
      'SELECT RPAD(\'test\', 10, \'ab\')',
      {
        write: {
          'snowflake': 'SELECT RPAD(\'test\', 10, \'ab\')',
          'duckdb': 'SELECT RPAD(\'test\', 10, \'ab\')',
        },
      },
    );
    this.validateAll(
      'SELECT RPAD(\'data\', 8)',
      {
        write: {
          'snowflake': 'SELECT RPAD(\'data\', 8)',
          'duckdb': 'SELECT RPAD(\'data\', 8, \' \')',
          'postgres': 'SELECT RPAD(\'data\', 8)',
        },
      },
    );
    this.validateAll(
      'SELECT RPAD(\'exact\', 5, \'*\')',
      {
        write: {
          'snowflake': 'SELECT RPAD(\'exact\', 5, \'*\')',
          'duckdb': 'SELECT RPAD(\'exact\', 5, \'*\')',
        },
      },
    );

    ast = this.validateIdentity(
      'SELECT RPAD(TO_BINARY(\'Hi\', \'UTF8\'), 10, TO_BINARY(\'_\', \'UTF8\'))',
    );
    let annotated = annotateTypes(ast, {
      dialect: 'snowflake',
    });

    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('SELECT ENCODE(\'Hi\') || REPEAT(ENCODE(\'_\'), GREATEST(0, 10 - OCTET_LENGTH(ENCODE(\'Hi\'))))');

    this.validateIdentity('SELECT SOUNDEX(column_name)');
    this.validateIdentity('SELECT SOUNDEX_P123(column_name)');
    this.validateIdentity('SELECT ABS(x)');
    this.validateIdentity('SELECT ASIN(0.5)');
    this.validateIdentity('SELECT ASINH(0.5)');
    this.validateIdentity('SELECT ATAN(0.5)');
    this.validateIdentity('SELECT ATAN2(0.5, 0.3)');
    this.validateIdentity('SELECT ATANH(0.5)');
    this.validateIdentity('SELECT CBRT(27.0)');
    this.validateIdentity('SELECT POW(2, 3)', 'SELECT POWER(2, 3)');
    this.validateIdentity('SELECT POW(2.5, 3.0)', 'SELECT POWER(2.5, 3.0)');
    this.validateIdentity('SELECT SQUARE(2.5)', 'SELECT POWER(2.5, 2)');
    this.validateIdentity('SELECT SIGN(x)');
    this.validateIdentity('SELECT COSH(1.5)');
    this.validateIdentity('SELECT TANH(0.5)');
    this.validateIdentity('SELECT JAROWINKLER_SIMILARITY(\'hello\', \'world\')');
    this.validateIdentity('SELECT TRANSLATE(column_name, \'abc\', \'123\')');
    this.validateIdentity('SELECT UNICODE(column_name)');
    this.validateIdentity('SELECT WIDTH_BUCKET(col, 0, 100, 10)');
    this.validateIdentity('SELECT SPLIT_PART(\'11.22.33\', \'.\', 1)');
    this.validateIdentity('SELECT PI()');
    this.validateIdentity('SELECT DEGREES(PI() / 3)');
    this.validateIdentity('SELECT DEGREES(1)');
    this.validateIdentity('SELECT RADIANS(180)');
    this.validateAll(
      'SELECT REGR_VALX(y, x)',
      {
        write: {
          'snowflake': 'SELECT REGR_VALX(y, x)',
          'duckdb': 'SELECT CASE WHEN y IS NULL THEN CAST(NULL AS DOUBLE) ELSE x END',
        },
      },
    );
    this.validateAll(
      'SELECT REGR_VALY(y, x)',
      {
        write: {
          'snowflake': 'SELECT REGR_VALY(y, x)',
          'duckdb': 'SELECT CASE WHEN x IS NULL THEN CAST(NULL AS DOUBLE) ELSE y END',
        },
      },
    );
    this.validateIdentity('SELECT REGR_AVGX(y, x)');
    this.validateIdentity('SELECT REGR_AVGY(y, x)');
    this.validateIdentity('SELECT REGR_COUNT(y, x)');
    this.validateIdentity('SELECT REGR_INTERCEPT(y, x)');
    this.validateIdentity('SELECT REGR_R2(y, x)');
    this.validateIdentity('SELECT REGR_SXX(y, x)');
    this.validateIdentity('SELECT REGR_SXY(y, x)');
    this.validateIdentity('SELECT REGR_SYY(y, x)');
    this.validateIdentity('SELECT REGR_SLOPE(y, x)');

    this.validateAll(
      'SELECT IS_ARRAY(PARSE_JSON(\'[1,2,3]\'))',
      {
        write: {
          'snowflake': 'SELECT IS_ARRAY(PARSE_JSON(\'[1,2,3]\'))',
          'duckdb': 'SELECT JSON_TYPE(JSON(\'[1,2,3]\')) = \'ARRAY\'',
        },
      },
    );
    this.validateAll(
      'SELECT IFF(x > 5, 10, 20)',
      {
        write: {
          'snowflake': 'SELECT IFF(x > 5, 10, 20)',
          'duckdb': 'SELECT CASE WHEN x > 5 THEN 10 ELSE 20 END',
        },
      },
    );
    this.validateAll(
      'SELECT IFF(col IS NULL, 0, col)',
      {
        write: {
          'snowflake': 'SELECT IFF(col IS NULL, 0, col)',
          'duckdb': 'SELECT CASE WHEN col IS NULL THEN 0 ELSE col END',
        },
      },
    );
    this.validateAll(
      'SELECT VAR_SAMP(x)',
      {
        write: {
          'snowflake': 'SELECT VARIANCE(x)',
          'duckdb': 'SELECT VARIANCE(x)',
          'postgres': 'SELECT VAR_SAMP(x)',
        },
      },
    );
    this.validateAll(
      'SELECT GREATEST(1, 2)',
      {
        write: {
          'snowflake': 'SELECT GREATEST(1, 2)',
          'duckdb': 'SELECT CASE WHEN 1 IS NULL OR 2 IS NULL THEN NULL ELSE GREATEST(1, 2) END',
        },
      },
    );
    this.validateAll(
      'SELECT GREATEST_IGNORE_NULLS(1, 2)',
      {
        write: {
          'snowflake': 'SELECT GREATEST_IGNORE_NULLS(1, 2)',
          'duckdb': 'SELECT GREATEST(1, 2)',
        },
      },
    );
    this.validateAll(
      'SELECT LEAST(1, 2)',
      {
        write: {
          'snowflake': 'SELECT LEAST(1, 2)',
          'duckdb': 'SELECT CASE WHEN 1 IS NULL OR 2 IS NULL THEN NULL ELSE LEAST(1, 2) END',
        },
      },
    );
    this.validateAll(
      'SELECT LEAST_IGNORE_NULLS(1, 2)',
      {
        write: {
          'snowflake': 'SELECT LEAST_IGNORE_NULLS(1, 2)',
          'duckdb': 'SELECT LEAST(1, 2)',
        },
      },
    );
    this.validateAll(
      'SELECT VAR_POP(x)',
      {
        write: {
          'snowflake': 'SELECT VARIANCE_POP(x)',
          'duckdb': 'SELECT VAR_POP(x)',
          'postgres': 'SELECT VAR_POP(x)',
        },
      },
    );
    this.validateAll(
      'SELECT SKEW(a)',
      {
        write: {
          'snowflake': 'SELECT SKEW(a)',
          'duckdb': 'SELECT SKEWNESS(a)',
          'spark': 'SELECT SKEWNESS(a)',
          'trino': 'SELECT SKEWNESS(a)',
        },
        read: {
          'duckdb': 'SELECT SKEWNESS(a)',
          'spark': 'SELECT SKEWNESS(a)',
          'trino': 'SELECT SKEWNESS(a)',
        },
      },
    );
    this.validateIdentity('SELECT RANDOM()');
    this.validateIdentity('SELECT RANDOM(123)');
    this.validateIdentity('SELECT RANDSTR(123, 456)');
    this.validateIdentity('SELECT RANDSTR(123, RANDOM())');
    this.validateIdentity('SELECT NORMAL(0, 1, RANDOM())');

    this.validateAll(
      'IS_NULL_VALUE(x)',
      {
        write: {
          'duckdb': 'JSON_TYPE(x) = \'NULL\'',
          'snowflake': 'IS_NULL_VALUE(x)',
        },
      },
    );
    // Test RANDSTR transpilation to DuckDB
    this.validateAll(
      'SELECT RANDSTR(10, 123)',
      {
        write: {
          'snowflake': 'SELECT RANDSTR(10, 123)',
          'duckdb': 'SELECT (SELECT LISTAGG(SUBSTRING(\'0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz\', 1 + CAST(FLOOR(random_value * 62) AS INT), 1), \'\') FROM (SELECT (ABS(HASH(i + 123)) % 1000) / 1000.0 AS random_value FROM RANGE(10) AS t(i)))',
        },
      },
    );
    this.validateAll(
      'SELECT RANDSTR(10, RANDOM(123))',
      {
        write: {
          'snowflake': 'SELECT RANDSTR(10, RANDOM(123))',
          'duckdb': 'SELECT (SELECT LISTAGG(SUBSTRING(\'0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz\', 1 + CAST(FLOOR(random_value * 62) AS INT), 1), \'\') FROM (SELECT (ABS(HASH(i + 123)) % 1000) / 1000.0 AS random_value FROM RANGE(10) AS t(i)))',
        },
      },
    );
    this.validateAll(
      'SELECT RANDSTR(10, RANDOM())',
      {
        write: {
          'snowflake': 'SELECT RANDSTR(10, RANDOM())',
          'duckdb': 'SELECT (SELECT LISTAGG(SUBSTRING(\'0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz\', 1 + CAST(FLOOR(random_value * 62) AS INT), 1), \'\') FROM (SELECT (ABS(HASH(i + RANDOM())) % 1000) / 1000.0 AS random_value FROM RANGE(10) AS t(i)))',
        },
      },
    );

    this.validateAll(
      'SELECT BOOLNOT(0)',
      {
        write: {
          'snowflake': 'SELECT BOOLNOT(0)',
          'duckdb': 'SELECT NOT (ROUND(0, 0))',
        },
      },
    );

    this.validateAll(
      'SELECT ZIPF(1, 10, 1234)',
      {
        write: {
          'duckdb': 'SELECT (WITH rand AS (SELECT (ABS(HASH(1234)) % 1000000) / 1000000.0 AS r), weights AS (SELECT i, 1.0 / POWER(i, 1) AS w FROM RANGE(1, 10 + 1) AS t(i)), cdf AS (SELECT i, SUM(w) OVER (ORDER BY i NULLS FIRST) / SUM(w) OVER () AS p FROM weights) SELECT MIN(i) FROM cdf WHERE p >= (SELECT r FROM rand))',
          'snowflake': 'SELECT ZIPF(1, 10, 1234)',
        },
      },
    );

    this.validateAll(
      'SELECT ZIPF(2, 100, RANDOM())',
      {
        write: {
          'duckdb': 'SELECT (WITH rand AS (SELECT RANDOM() AS r), weights AS (SELECT i, 1.0 / POWER(i, 2) AS w FROM RANGE(1, 100 + 1) AS t(i)), cdf AS (SELECT i, SUM(w) OVER (ORDER BY i NULLS FIRST) / SUM(w) OVER () AS p FROM weights) SELECT MIN(i) FROM cdf WHERE p >= (SELECT r FROM rand))',
          'snowflake': 'SELECT ZIPF(2, 100, RANDOM())',
        },
      },
    );

    this.validateIdentity('SELECT GROUPING_ID(a, b) AS g_id FROM x GROUP BY ROLLUP (a, b)');
    this.validateIdentity('PARSE_URL(\'https://example.com/path\')');
    this.validateIdentity('PARSE_URL(\'https://example.com/path\', 1)');
    this.validateIdentity('SELECT XMLGET(object_col, \'level2\')');
    this.validateIdentity('SELECT XMLGET(object_col, \'level3\', 1)');
    this.validateIdentity('SELECT {*} FROM my_table');
    this.validateIdentity('SELECT {my_table.*} FROM my_table');
    this.validateIdentity('SELECT {* ILIKE \'col1%\'} FROM my_table');
    this.validateIdentity('SELECT {* EXCLUDE (col1)} FROM my_table');
    this.validateIdentity('SELECT {* EXCLUDE (col1, col2)} FROM my_table');
    this.validateIdentity('SELECT a, b, COUNT(*) FROM x GROUP BY ALL LIMIT 100');
    this.validateIdentity('STRTOK_TO_ARRAY(\'a b c\')');
    this.validateIdentity('STRTOK_TO_ARRAY(\'a.b.c\', \'.\')');
    this.validateIdentity('GET(a, b)');
    this.validateIdentity('INSERT INTO test VALUES (x\'48FAF43B0AFCEF9B63EE3A93EE2AC2\')');
    this.validateIdentity('SELECT STAR(tbl, exclude := [foo])');
    this.validateIdentity('SELECT CAST([1, 2, 3] AS VECTOR(FLOAT, 3))');
    this.validateIdentity('SELECT VECTOR_COSINE_SIMILARITY(a, b)');
    this.validateIdentity('SELECT VECTOR_INNER_PRODUCT(a, b)');
    this.validateIdentity('SELECT VECTOR_L1_DISTANCE(a, b)');
    this.validateIdentity('SELECT VECTOR_L2_DISTANCE(a, b)');
    this.validateIdentity('SELECT CONNECT_BY_ROOT test AS test_column_alias');
    (this.validateIdentity('SELECT number') as SelectExpr).selects[0].assertIs(ColumnExpr);
    this.validateIdentity('INTERVAL \'4 years, 5 months, 3 hours\'');
    this.validateIdentity('ALTER TABLE table1 CLUSTER BY (name DESC)');
    this.validateIdentity('SELECT rename, replace');
    this.validateIdentity('SELECT TIMEADD(HOUR, 2, CAST(\'09:05:03\' AS TIME))');
    this.validateIdentity('SELECT CAST(OBJECT_CONSTRUCT(\'a\', 1) AS MAP(VARCHAR, INT))');
    this.validateIdentity(
      'SELECT MAP_CAT(CAST(col AS MAP(VARCHAR, VARCHAR)), CAST(col AS MAP(VARCHAR, VARCHAR)))',
    );
    this.validateAll(
      'SELECT MAP_CAT(CAST(m1 AS MAP(VARCHAR, INT)), CAST(m2 AS MAP(VARCHAR, INT)))',
      {
        write: {
          'duckdb': 'SELECT CASE WHEN CAST(m1 AS MAP(TEXT, INT)) IS NULL OR CAST(m2 AS MAP(TEXT, INT)) IS NULL THEN NULL ELSE MAP_FROM_ENTRIES(LIST_FILTER(LIST_TRANSFORM(LIST_DISTINCT(LIST_CONCAT(MAP_KEYS(CAST(m1 AS MAP(TEXT, INT))), MAP_KEYS(CAST(m2 AS MAP(TEXT, INT))))), __k -> STRUCT_PACK(key := __k, value := COALESCE(CAST(m2 AS MAP(TEXT, INT))[__k], CAST(m1 AS MAP(TEXT, INT))[__k]))), __x -> NOT __x.value IS NULL)) END',
          'snowflake': 'SELECT MAP_CAT(CAST(m1 AS MAP(VARCHAR, INT)), CAST(m2 AS MAP(VARCHAR, INT)))',
        },
      },
    );
    this.validateAll(
      'SELECT MAP_CAT(CAST(OBJECT_CONSTRUCT() AS MAP(VARCHAR, INT)), CAST(OBJECT_CONSTRUCT(\'a\', 1) AS MAP(VARCHAR, INT)))',
      {
        write: {
          'duckdb': 'SELECT CASE WHEN CAST(MAP() AS MAP(TEXT, INT)) IS NULL OR CAST({\'a\': 1} AS MAP(TEXT, INT)) IS NULL THEN NULL ELSE MAP_FROM_ENTRIES(LIST_FILTER(LIST_TRANSFORM(LIST_DISTINCT(LIST_CONCAT(MAP_KEYS(CAST(MAP() AS MAP(TEXT, INT))), MAP_KEYS(CAST({\'a\': 1} AS MAP(TEXT, INT))))), __k -> STRUCT_PACK(key := __k, value := COALESCE(CAST({\'a\': 1} AS MAP(TEXT, INT))[__k], CAST(MAP() AS MAP(TEXT, INT))[__k]))), __x -> NOT __x.value IS NULL)) END',
          'snowflake': 'SELECT MAP_CAT(CAST(OBJECT_CONSTRUCT() AS MAP(VARCHAR, INT)), CAST(OBJECT_CONSTRUCT(\'a\', 1) AS MAP(VARCHAR, INT)))',
        },
      },
    );
    this.validateIdentity('SELECT MAP_CONTAINS_KEY(\'k1\', CAST(col AS MAP(VARCHAR, VARCHAR)))');
    this.validateIdentity('SELECT MAP_DELETE(CAST(col AS MAP(VARCHAR, VARCHAR)), \'k1\')');
    this.validateIdentity('SELECT MAP_INSERT(CAST(col AS MAP(VARCHAR, VARCHAR)), \'b\', \'2\')');
    this.validateIdentity('SELECT MAP_KEYS(CAST(col AS MAP(VARCHAR, VARCHAR)))');
    this.validateIdentity('SELECT MAP_PICK(CAST(col AS MAP(VARCHAR, VARCHAR)), \'a\', \'c\')');
    this.validateIdentity('SELECT MAP_SIZE(CAST(col AS MAP(VARCHAR, VARCHAR)))');
    this.validateIdentity('SELECT CAST(OBJECT_CONSTRUCT(\'a\', 1) AS OBJECT(a CHAR NOT NULL))');
    this.validateIdentity('SELECT CAST([1, 2, 3] AS ARRAY(INT))');
    this.validateIdentity('SELECT CAST(obj AS OBJECT(x CHAR) RENAME FIELDS)');
    this.validateIdentity('SELECT CAST(obj AS OBJECT(x CHAR, y VARCHAR) ADD FIELDS)');
    (this.validateIdentity('SELECT TO_TIMESTAMP(123.4)') as SelectExpr).selects[0].assertIs(AnonymousExpr);
    this.validateIdentity('SELECT TO_TIMESTAMP(x) FROM t');
    this.validateIdentity('SELECT TO_TIMESTAMP_NTZ(x) FROM t');
    this.validateIdentity('SELECT TO_TIMESTAMP_LTZ(x) FROM t');
    this.validateIdentity('SELECT TO_TIMESTAMP_TZ(x) FROM t');
    this.validateIdentity('TO_DECIMAL(expr)', 'TO_NUMBER(expr)');
    this.validateIdentity('TO_DECIMAL(expr, fmt)', 'TO_NUMBER(expr, fmt)');
    this.validateIdentity('TO_DECIMAL(expr, fmt, precision, scale)', 'TO_NUMBER(expr, fmt, precision, scale)');
    this.validateIdentity('TO_NUMBER(expr)');
    this.validateIdentity('TO_NUMBER(expr, fmt)');
    this.validateIdentity('TO_NUMBER(expr, fmt, precision, scale)');
    this.validateIdentity('TO_DECFLOAT(\'123.456\')');
    this.validateIdentity('TO_DECFLOAT(\'1,234.56\', \'999,999.99\')');
    this.validateIdentity('TRY_TO_DECFLOAT(\'123.456\')');
    this.validateIdentity('TRY_TO_DECFLOAT(\'1,234.56\', \'999,999.99\')');
    this.validateAll(
      'TRY_TO_BOOLEAN(\'true\')',
      {
        write: {
          'snowflake': 'TRY_TO_BOOLEAN(\'true\')',
          'duckdb': 'CASE WHEN UPPER(CAST(\'true\' AS TEXT)) = \'ON\' THEN TRUE WHEN UPPER(CAST(\'true\' AS TEXT)) = \'OFF\' THEN FALSE ELSE TRY_CAST(\'true\' AS BOOLEAN) END',
        },
      },
    );

    this.validateIdentity('TRY_TO_DECIMAL(\'123.45\')', 'TRY_TO_NUMBER(\'123.45\')');
    this.validateIdentity('TRY_TO_DECIMAL(\'123.45\', \'999.99\')', 'TRY_TO_NUMBER(\'123.45\', \'999.99\')');
    this.validateIdentity('TRY_TO_DECIMAL(\'123.45\', \'999.99\', 10, 2)', 'TRY_TO_NUMBER(\'123.45\', \'999.99\', 10, 2)');
    this.validateAll(
      'TRY_TO_DOUBLE(\'123.456\')',
      {
        write: {
          'snowflake': 'TRY_TO_DOUBLE(\'123.456\')',
          'duckdb': 'TRY_CAST(\'123.456\' AS DOUBLE)',
        },
      },
    );
    this.validateIdentity('TRY_TO_DOUBLE(\'123.456\', \'999.99\')');
    this.validateAll(
      'TRY_TO_DOUBLE(\'-4.56E-03\', \'S9.99EEEE\')',
      {
        write: {
          'snowflake': 'TRY_TO_DOUBLE(\'-4.56E-03\', \'S9.99EEEE\')',
          'duckdb': UnsupportedError,
        },
      },
    );
    this.validateIdentity('TO_FILE(object_col)');
    this.validateIdentity('TO_FILE(\'file.csv\')');
    this.validateIdentity('TO_FILE(\'file.csv\', \'relativepath/\')');
    this.validateIdentity('TRY_TO_FILE(object_col)');
    this.validateIdentity('TRY_TO_FILE(\'file.csv\')');
    this.validateIdentity('TRY_TO_FILE(\'file.csv\', \'relativepath/\')');
    this.validateIdentity('TRY_TO_NUMBER(\'123.45\')');
    this.validateIdentity('TRY_TO_NUMBER(\'123.45\', \'999.99\')');
    this.validateIdentity('TRY_TO_NUMBER(\'123.45\', \'999.99\', 10, 2)');
    this.validateIdentity('TO_NUMERIC(\'123.45\')', 'TO_NUMBER(\'123.45\')');
    this.validateIdentity('TO_NUMERIC(\'123.45\', \'999.99\')', 'TO_NUMBER(\'123.45\', \'999.99\')');
    this.validateIdentity('TO_NUMERIC(\'123.45\', \'999.99\', 10, 2)', 'TO_NUMBER(\'123.45\', \'999.99\', 10, 2)');
    this.validateIdentity('TRY_TO_NUMERIC(\'123.45\')', 'TRY_TO_NUMBER(\'123.45\')');
    this.validateIdentity('TRY_TO_NUMERIC(\'123.45\', \'999.99\')', 'TRY_TO_NUMBER(\'123.45\', \'999.99\')');
    this.validateIdentity('TRY_TO_NUMERIC(\'123.45\', \'999.99\', 10, 2)', 'TRY_TO_NUMBER(\'123.45\', \'999.99\', 10, 2)');
    this.validateAll(
      'TRY_TO_TIME(\'12:30:00\')',
      {
        write: {
          'snowflake': 'TRY_CAST(\'12:30:00\' AS TIME)',
          'duckdb': 'TRY_CAST(\'12:30:00\' AS TIME)',
        },
      },
    );
    this.validateIdentity('TRY_TO_TIME(\'12:30:00\', \'AUTO\')');
    this.validateAll(
      'TRY_TO_TIMESTAMP(\'2024-01-15 12:30:00\')',
      {
        write: {
          'snowflake': 'TRY_CAST(\'2024-01-15 12:30:00\' AS TIMESTAMP)',
          'duckdb': 'TRY_CAST(\'2024-01-15 12:30:00\' AS TIMESTAMP)',
        },
      },
    );
    this.validateIdentity('TRY_TO_TIMESTAMP(\'2024-01-15 12:30:00\', \'AUTO\')');
    this.validateIdentity('ALTER TABLE authors ADD CONSTRAINT c1 UNIQUE (id, email)');
    this.validateIdentity('RM @parquet_stage', undefined, {
      checkCommandWarning: true,
    });
    this.validateIdentity('REMOVE @parquet_stage', undefined, {
      checkCommandWarning: true,
    });
    this.validateIdentity('SELECT TIMESTAMP_FROM_PARTS(2024, 5, 9, 14, 30, 45)');
    this.validateIdentity('SELECT TIMESTAMP_FROM_PARTS(2024, 5, 9, 14, 30, 45, 123)');
    this.validateIdentity('SELECT TIMESTAMP_LTZ_FROM_PARTS(2013, 4, 5, 12, 00, 00)');
    this.validateIdentity('SELECT TIMESTAMP_TZ_FROM_PARTS(2013, 4, 5, 12, 00, 00)');
    this.validateIdentity(
      'SELECT TIMESTAMP_TZ_FROM_PARTS(2013, 4, 5, 12, 00, 00, 0, \'America/Los_Angeles\')',
    );
    this.validateIdentity(
      'SELECT TIMESTAMP_FROM_PARTS(CAST(\'2024-05-09\' AS DATE), CAST(\'14:30:45\' AS TIME))',
    );
    this.validateIdentity(
      'SELECT TIMESTAMP_NTZ_FROM_PARTS(TO_DATE(\'2013-04-05\'), TO_TIME(\'12:00:00\'))',
      'SELECT TIMESTAMP_FROM_PARTS(CAST(\'2013-04-05\' AS DATE), CAST(\'12:00:00\' AS TIME))',
    );
    this.validateIdentity(
      'SELECT TIMESTAMP_NTZ_FROM_PARTS(2013, 4, 5, 12, 00, 00, 987654321)',
      'SELECT TIMESTAMP_FROM_PARTS(2013, 4, 5, 12, 00, 00, 987654321)',
    );

    this.validateIdentity('SELECT DATE_FROM_PARTS(1977, 8, 7)');
    this.validateIdentity('SELECT GET_PATH(v, \'attr[0].name\') FROM vartab');
    this.validateIdentity('SELECT TO_ARRAY(CAST(x AS ARRAY))');
    this.validateIdentity('SELECT TO_ARRAY(CAST([\'test\'] AS VARIANT))');
    this.validateIdentity('SELECT ARRAY_UNIQUE_AGG(x)');
    this.validateIdentity('SELECT ARRAY_APPEND([1, 2, 3], 4)');
    this.validateIdentity('SELECT ARRAY_CAT([1, 2], [3, 4])');
    this.validateIdentity('SELECT ARRAY_PREPEND([2, 3, 4], 1)');
    this.validateIdentity('SELECT ARRAY_REMOVE([1, 2, 3], 2)');
    this.validateIdentity('SELECT ARRAYS_ZIP([1, 2, 3])');
    this.validateIdentity('SELECT ARRAYS_ZIP([1, 2, 3], [\'a\', \'b\', \'c\'], [10, 20, 30])');
    this.validateIdentity('SELECT AI_AGG(review, \'Summarize the reviews\')');
    this.validateIdentity('SELECT AI_SUMMARIZE_AGG(review)');
    this.validateIdentity('SELECT AI_CLASSIFY(\'text\', [\'travel\', \'cooking\'])');
    this.validateIdentity('SELECT OBJECT_CONSTRUCT()');
    this.validateIdentity('SELECT CURRENT_ACCOUNT()');
    this.validateIdentity('SELECT CURRENT_ACCOUNT_NAME()');
    this.validateIdentity('SELECT CURRENT_AVAILABLE_ROLES()');
    this.validateIdentity('SELECT CURRENT_CLIENT()');
    this.validateIdentity('SELECT CURRENT_IP_ADDRESS()');
    this.validateIdentity('SELECT CURRENT_DATABASE()');
    this.validateIdentity('SELECT CURRENT_SCHEMAS()');
    this.validateIdentity('SELECT CURRENT_SECONDARY_ROLES()');
    this.validateIdentity('SELECT CURRENT_SESSION()');
    this.validateIdentity('SELECT CURRENT_STATEMENT()');
    this.validateIdentity('SELECT CURRENT_VERSION()');
    this.validateIdentity('SELECT CURRENT_TRANSACTION()');
    this.validateIdentity('SELECT CURRENT_WAREHOUSE()');
    this.validateIdentity('SELECT CURRENT_ORGANIZATION_USER()');
    this.validateIdentity('SELECT CURRENT_REGION()');
    this.validateIdentity('SELECT CURRENT_ROLE()');
    this.validateIdentity('SELECT CURRENT_ROLE_TYPE()');
    this.validateIdentity('SELECT DAY(CURRENT_TIMESTAMP())');
    this.validateIdentity('SELECT DAYOFMONTH(CURRENT_TIMESTAMP())');
    this.validateIdentity('SELECT DAYOFYEAR(CURRENT_TIMESTAMP())');
    this.validateIdentity('SELECT MONTH(CURRENT_TIMESTAMP())');
    this.validateIdentity('SELECT QUARTER(CURRENT_TIMESTAMP())');
    this.validateIdentity('SELECT WEEK(CURRENT_TIMESTAMP())');
    this.validateIdentity('SELECT WEEKISO(CURRENT_TIMESTAMP())');
    this.validateIdentity('WEEKOFYEAR(tstamp)', 'WEEK(tstamp)');
    this.validateIdentity('SELECT YEAR(CURRENT_TIMESTAMP())');
    this.validateIdentity('SELECT YEAROFWEEK(CURRENT_TIMESTAMP())');
    this.validateIdentity('SELECT YEAROFWEEKISO(CURRENT_TIMESTAMP())');
    this.validateAll(
      'SELECT DAYOFWEEKISO(\'2024-01-15\'::DATE)',
      {
        write: {
          'snowflake': 'SELECT DAYOFWEEKISO(CAST(\'2024-01-15\' AS DATE))',
          'duckdb': 'SELECT ISODOW(CAST(\'2024-01-15\' AS DATE))',
        },
      },
    );
    this.validateAll(
      'SELECT YEAROFWEEK(\'2024-12-31\'::DATE)',
      {
        write: {
          'snowflake': 'SELECT YEAROFWEEK(CAST(\'2024-12-31\' AS DATE))',
          'duckdb': 'SELECT EXTRACT(ISOYEAR FROM CAST(\'2024-12-31\' AS DATE))',
        },
      },
    );
    this.validateAll(
      'SELECT YEAROFWEEKISO(\'2024-12-31\'::DATE)',
      {
        write: {
          'snowflake': 'SELECT YEAROFWEEKISO(CAST(\'2024-12-31\' AS DATE))',
          'duckdb': 'SELECT EXTRACT(ISOYEAR FROM CAST(\'2024-12-31\' AS DATE))',
        },
      },
    );
    this.validateAll(
      'SELECT WEEKISO(\'2024-01-15\'::DATE)',
      {
        write: {
          'snowflake': 'SELECT WEEKISO(CAST(\'2024-01-15\' AS DATE))',
          'duckdb': 'SELECT WEEKOFYEAR(CAST(\'2024-01-15\' AS DATE))',
        },
      },
    );
    this.validateIdentity('SELECT SUM(amount) FROM mytable GROUP BY ALL');
    this.validateIdentity('SELECT STDDEV(x)');
    this.validateIdentity('SELECT STDDEV(x) OVER (PARTITION BY 1)');
    this.validateIdentity('SELECT STDDEV_POP(x)');
    this.validateIdentity('SELECT STDDEV_POP(x) OVER (PARTITION BY 1)');
    this.validateIdentity('SELECT STDDEV_SAMP(x)', 'SELECT STDDEV(x)');
    this.validateIdentity('SELECT STDDEV_SAMP(x) OVER (PARTITION BY 1)', 'SELECT STDDEV(x) OVER (PARTITION BY 1)');
    this.validateIdentity('SELECT KURTOSIS(x)');
    this.validateIdentity('SELECT KURTOSIS(x) OVER (PARTITION BY 1)');
    this.validateIdentity('WITH x AS (SELECT 1 AS foo) SELECT foo FROM IDENTIFIER(\'x\')');
    this.validateIdentity('WITH x AS (SELECT 1 AS foo) SELECT IDENTIFIER(\'foo\') FROM x');
    this.validateIdentity('INITCAP(\'iqamqinterestedqinqthisqtopic\', \'q\')');
    this.validateIdentity('OBJECT_CONSTRUCT(*)');
    this.validateIdentity('SELECT CAST(\'2021-01-01\' AS DATE) + INTERVAL \'1 DAY\'');
    this.validateIdentity('SELECT HLL(*)');
    this.validateIdentity('SELECT HLL(a)');
    this.validateIdentity('SELECT HLL(DISTINCT t.a)');
    this.validateIdentity('SELECT HLL(a, b, c)');
    this.validateIdentity('SELECT HLL(DISTINCT a, b, c)');
    this.validateIdentity('$x');  // parameter
    this.validateIdentity('a$b');  // valid snowflake identifier
    this.validateIdentity('SELECT REGEXP_LIKE(a, b, c)');
    this.validateIdentity('CREATE TABLE foo (bar DOUBLE AUTOINCREMENT START 0 INCREMENT 1)');
    this.validateIdentity('COMMENT IF EXISTS ON TABLE foo IS \'bar\'');
    this.validateIdentity('SELECT CONVERT_TIMEZONE(\'UTC\', \'America/Los_Angeles\', col)');
    this.validateIdentity('SELECT CURRENT_ORGANIZATION_NAME()');
    this.validateIdentity('ALTER TABLE a SWAP WITH b');
    this.validateIdentity('SELECT MATCH_CONDITION');
    this.validateIdentity('SELECT OBJECT_AGG(key, value) FROM tbl');
    this.validateIdentity('1 /* /* */');
    this.validateIdentity('TO_TIMESTAMP(col, fmt)');
    this.validateIdentity('SELECT TO_CHAR(CAST(\'12:05:05\' AS TIME))');
    this.validateIdentity('SELECT TRIM(COALESCE(TO_CHAR(CAST(c AS TIME)), \'\')) FROM t');
    this.validateIdentity('SELECT GET_PATH(PARSE_JSON(foo), \'bar\')');
    this.validateIdentity('SELECT PARSE_IP(\'192.168.1.1\', \'INET\')');
    this.validateIdentity('SELECT PARSE_IP(\'192.168.1.1\', \'INET\', 0)');
    this.validateIdentity('SELECT GET_PATH(foo, \'bar\')');
    this.validateIdentity('SELECT a, exclude, b FROM xxx');
    this.validateIdentity('SELECT ARRAY_SORT(x, TRUE, FALSE)');
    this.validateIdentity('SELECT BOOLXOR_AGG(col) FROM tbl');
    this.validateIdentity(
      'SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY col) OVER (PARTITION BY category)',
    );
    (this.validateIdentity(
      'SELECT DATEADD(DAY, -7, DATEADD(t.m, 1, CAST(\'2023-01-03\' AS DATE))) FROM (SELECT \'month\' AS m) AS t',
    ) as any).selects[0].args.this.args.unit.assertIs(ColumnExpr);
    this.validateIdentity('SELECT STRTOK(\'hello world\')', 'SELECT SPLIT_PART(\'hello world\', \' \', 1)');
    this.validateIdentity('SELECT STRTOK(\'hello world\', \' \')', 'SELECT SPLIT_PART(\'hello world\', \' \', 1)');
    this.validateIdentity('SELECT STRTOK(\'hello world\', \' \', 2)', 'SELECT SPLIT_PART(\'hello world\', \' \', 2)');
    (this.validateIdentity('SELECT FILE_URL FROM DIRECTORY(@mystage) WHERE SIZE > 100000') as any).args.from?.args.this.args.this.assertIs(DirectoryStageExpr).args.this.assertIs(VarExpr);
    this.validateIdentity(
      'SELECT AI_CLASSIFY(\'text\', [\'travel\', \'cooking\'], OBJECT_CONSTRUCT(\'output_mode\', \'multi\'))',
    );
    this.validateIdentity(
      'SELECT * FROM table AT (TIMESTAMP => \'2024-07-24\') UNPIVOT(a FOR b IN (c)) AS pivot_table',
    );
    this.validateIdentity(
      'SELECT * FROM quarterly_sales PIVOT(SUM(amount) FOR quarter IN (\'2023_Q1\', \'2023_Q2\', \'2023_Q3\', \'2023_Q4\', \'2024_Q1\') DEFAULT ON NULL (0)) ORDER BY empid',
    );
    this.validateIdentity(
      'SELECT * FROM quarterly_sales PIVOT(SUM(amount) FOR quarter IN (SELECT DISTINCT quarter FROM ad_campaign_types_by_quarter WHERE television = TRUE ORDER BY quarter)) ORDER BY empid',
    );
    this.validateIdentity(
      'SELECT * FROM quarterly_sales PIVOT(SUM(amount) FOR quarter IN (ANY ORDER BY quarter)) ORDER BY empid',
    );
    this.validateIdentity(
      'SELECT * FROM quarterly_sales PIVOT(SUM(amount) FOR quarter IN (ANY)) ORDER BY empid',
    );
    this.validateIdentity(
      'MERGE INTO my_db AS ids USING (SELECT new_id FROM my_model WHERE NOT col IS NULL) AS new_ids ON ids.type = new_ids.type AND ids.source = new_ids.source WHEN NOT MATCHED THEN INSERT VALUES (new_ids.new_id)',
    );
    this.validateIdentity('INSERT OVERWRITE TABLE t SELECT 1', 'INSERT OVERWRITE INTO t SELECT 1');
    this.validateIdentity(
      'DESCRIBE TABLE "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."WEB_SITE" type=stage',
    );
    this.validateIdentity(
      'SELECT * FROM DATA AS DATA_L ASOF JOIN DATA AS DATA_R MATCH_CONDITION (DATA_L.VAL > DATA_R.VAL) ON DATA_L.ID = DATA_R.ID',
    );
    this.validateIdentity(
      'SELECT TO_TIMESTAMP(\'2025-01-16T14:45:30.123+0500\', \'yyyy-mm-DDThh24:mi:ss.ff9tzhtzm\')',
    );
    this.validateIdentity(
      'SELECT * REPLACE (CAST(col AS TEXT) AS scol) FROM t',
      'SELECT * REPLACE (CAST(col AS VARCHAR) AS scol) FROM t',
    );
    this.validateIdentity(
      'GET(value, \'foo\')::VARCHAR',
      'CAST(GET(value, \'foo\') AS VARCHAR)',
    );
    this.validateIdentity(
      'SELECT 1 put',
      'SELECT 1 AS put',
    );
    this.validateIdentity(
      'SELECT 1 get',
      'SELECT 1 AS get',
    );
    this.validateIdentity(
      'WITH t (SELECT 1 AS c) SELECT c FROM t',
      'WITH t AS (SELECT 1 AS c) SELECT c FROM t',
    );
    this.validateIdentity(
      'GET_PATH(json_data, \'$id\')',
      'GET_PATH(json_data, \'["$id"]\')',
    );
    this.validateIdentity(
      'CAST(x AS GEOGRAPHY)',
      'TO_GEOGRAPHY(x)',
    );
    this.validateIdentity(
      'CAST(x AS GEOMETRY)',
      'TO_GEOMETRY(x)',
    );
    this.validateIdentity(
      'transform(x, a int -> a + a + 1)',
      'TRANSFORM(x, a -> CAST(a AS INT) + CAST(a AS INT) + 1)',
    );
    this.validateIdentity(
      'SELECT * FROM s WHERE c NOT IN (1, 2, 3)',
      'SELECT * FROM s WHERE NOT c IN (1, 2, 3)',
    );
    this.validateIdentity(
      'SELECT * FROM s WHERE c NOT IN (SELECT * FROM t)',
      'SELECT * FROM s WHERE c <> ALL (SELECT * FROM t)',
    );
    this.validateIdentity(
      'SELECT * FROM t1 INNER JOIN t2 USING (t1.col)',
      'SELECT * FROM t1 INNER JOIN t2 USING (col)',
    );
    this.validateIdentity(
      'CURRENT_TIMESTAMP - INTERVAL \'1 w\' AND (1 = 1)',
      'CURRENT_TIMESTAMP() - INTERVAL \'1 WEEK\' AND (1 = 1)',
    );
    this.validateIdentity(
      `REGEXP_REPLACE('target', 'pattern', '
')`,
      'REGEXP_REPLACE(\'target\', \'pattern\', \'\\n\')',
    );
    this.validateIdentity(
      'SELECT a:from::STRING, a:from || \' test\' ',
      'SELECT CAST(GET_PATH(a, \'from\') AS VARCHAR), GET_PATH(a, \'from\') || \' test\'',
    );
    this.validateIdentity(
      'SELECT a:select',
      'SELECT GET_PATH(a, \'select\')',
    );
    this.validateIdentity('x:from', 'GET_PATH(x, \'from\')');
    this.validateIdentity(
      'value:values::string::int',
      'CAST(CAST(GET_PATH(value, \'values\') AS VARCHAR) AS INT)',
    );
    this.validateIdentity(
      'SELECT GET_PATH(PARSE_JSON(\'{"y": [{"z": 1}]}\'), \'y[0]:z\')',
      'SELECT GET_PATH(PARSE_JSON(\'{"y": [{"z": 1}]}\'), \'y[0].z\')',
    );
    this.validateIdentity(
      'SELECT p FROM t WHERE p:val NOT IN (\'2\')',
      'SELECT p FROM t WHERE NOT GET_PATH(p, \'val\') IN (\'2\')',
    );
    this.validateIdentity(
      'SELECT PARSE_JSON(\'{"x": "hello"}\'):x LIKE \'hello\'',
      'SELECT GET_PATH(PARSE_JSON(\'{"x": "hello"}\'), \'x\') LIKE \'hello\'',
    );
    this.validateIdentity(
      'SELECT data:x LIKE \'hello\' FROM some_table',
      'SELECT GET_PATH(data, \'x\') LIKE \'hello\' FROM some_table',
    );
    this.validateIdentity(
      'SELECT SUM({ fn CONVERT(123, SQL_DOUBLE) })',
      'SELECT SUM(CAST(123 AS DOUBLE))',
    );
    this.validateIdentity(
      'SELECT SUM({ fn CONVERT(123, SQL_VARCHAR) })',
      'SELECT SUM(CAST(123 AS VARCHAR))',
    );
    this.validateIdentity(
      'SELECT TIMESTAMPFROMPARTS(d, t)',
      'SELECT TIMESTAMP_FROM_PARTS(d, t)',
    );
    this.validateIdentity(
      'SELECT v:attr[0].name FROM vartab',
      'SELECT GET_PATH(v, \'attr[0].name\') FROM vartab',
    );
    this.validateIdentity(
      'SELECT v:"fruit" FROM vartab',
      'SELECT GET_PATH(v, \'fruit\') FROM vartab',
    );
    this.validateIdentity(
      'v:attr[0]:name',
      'GET_PATH(v, \'attr[0].name\')',
    );
    this.validateIdentity(
      'a.x:from.b:c.d::int',
      'CAST(GET_PATH(a.x, \'from.b.c.d\') AS INT)',
    );
    this.validateIdentity(
      'SELECT PARSE_JSON(\'{"food":{"fruit":"banana"}}\'):food.fruit::VARCHAR',
      'SELECT CAST(GET_PATH(PARSE_JSON(\'{"food":{"fruit":"banana"}}\'), \'food.fruit\') AS VARCHAR)',
    );
    this.validateIdentity(
      'SELECT * FROM t, UNNEST(x) WITH ORDINALITY',
      'SELECT * FROM t, TABLE(FLATTEN(INPUT => x)) AS _t0(seq, key, path, index, value, this)',
    );
    this.validateIdentity(
      'CREATE TABLE foo (ID INT COMMENT $$some comment$$)',
      'CREATE TABLE foo (ID INT COMMENT \'some comment\')',
    );
    this.validateIdentity(
      'SELECT state, city, SUM(retail_price * quantity) AS gross_revenue FROM sales GROUP BY ALL',
    );
    this.validateIdentity(
      'SELECT * FROM foo window',
      'SELECT * FROM foo AS window',
    );
    this.validateIdentity(
      'SELECT RLIKE(a, $$regular expression with \\ characters: \\d{2}-\\d{3}-\\d{4}$$, \'i\') FROM log_source',
      'SELECT REGEXP_LIKE(a, \'regular expression with \\\\ characters: \\\\d{2}-\\\\d{3}-\\\\d{4}\', \'i\') FROM log_source',
    );
    this.validateIdentity(
      'SELECT $$a \' \\ \\t \\x21 z $ $$',
      'SELECT \'a \\\' \\\\ \\\\t \\\\x21 z $ \'',
    );
    this.validateIdentity(
      'SELECT {\'test\': \'best\'}::VARIANT',
      'SELECT CAST(OBJECT_CONSTRUCT(\'test\', \'best\') AS VARIANT)',
    );
    this.validateIdentity(
      'SELECT {fn DAYNAME(\'2022-5-13\')}',
      'SELECT DAYNAME(\'2022-5-13\')',
    );
    this.validateIdentity(
      'SELECT {fn LOG(5)}',
      'SELECT LN(5)',
    );
    this.validateIdentity(
      'SELECT {fn CEILING(5.3)}',
      'SELECT CEIL(5.3)',
    );
    this.validateIdentity(
      'SELECT CEIL(3.14)',
    );
    this.validateIdentity(
      'SELECT CEIL(3.14, 1)',
    );
    this.validateIdentity(
      'CAST(x AS BYTEINT)',
      'CAST(x AS INT)',
    );
    this.validateIdentity(
      'CAST(x AS CHAR VARYING)',
      'CAST(x AS VARCHAR)',
    );
    this.validateIdentity(
      'CAST(x AS CHARACTER VARYING)',
      'CAST(x AS VARCHAR)',
    );
    this.validateIdentity(
      'CAST(x AS NCHAR VARYING)',
      'CAST(x AS VARCHAR)',
    );
    this.validateIdentity(
      'CREATE OR REPLACE TEMPORARY TABLE x (y NUMBER IDENTITY(0, 1))',
      'CREATE OR REPLACE TEMPORARY TABLE x (y DECIMAL(38, 0) AUTOINCREMENT START 0 INCREMENT 1)',
    );
    this.validateIdentity(
      'CREATE TEMPORARY TABLE x (y NUMBER AUTOINCREMENT(0, 1))',
      'CREATE TEMPORARY TABLE x (y DECIMAL(38, 0) AUTOINCREMENT START 0 INCREMENT 1)',
    );
    this.validateIdentity(
      'CREATE OR REPLACE TABLE x (y NUMBER(38, 0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 ORDER)',
      'CREATE OR REPLACE TABLE x (y DECIMAL(38, 0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 ORDER)',
    );
    this.validateIdentity(
      'CREATE OR REPLACE TABLE x (y NUMBER(38, 0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 NOORDER)',
      'CREATE OR REPLACE TABLE x (y DECIMAL(38, 0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 NOORDER)',
    );
    this.validateIdentity(
      'CREATE TABLE x (y NUMBER IDENTITY START 0 INCREMENT 1)',
      'CREATE TABLE x (y DECIMAL(38, 0) AUTOINCREMENT START 0 INCREMENT 1)',
    );
    this.validateIdentity(
      'ALTER TABLE foo ADD COLUMN id INT identity(1, 1)',
      'ALTER TABLE foo ADD id INT AUTOINCREMENT START 1 INCREMENT 1',
    );
    this.validateIdentity(
      'SELECT DAYOFWEEK(\'2016-01-02T23:39:20.123-07:00\'::TIMESTAMP)',
      'SELECT DAYOFWEEK(CAST(\'2016-01-02T23:39:20.123-07:00\' AS TIMESTAMP))',
    );
    this.validateIdentity(
      'SELECT * FROM xxx WHERE col ilike \'%Don\'\'t%\'',
      'SELECT * FROM xxx WHERE col ILIKE \'%Don\\\'t%\'',
    );
    this.validateIdentity(
      'SELECT * EXCLUDE a, b FROM xxx',
      'SELECT * EXCLUDE (a), b FROM xxx',
    );
    this.validateIdentity(
      'SELECT * RENAME a AS b, c AS d FROM xxx',
      'SELECT * RENAME (a AS b), c AS d FROM xxx',
    );

    // Support for optional trailing commas after tables in from clause
    this.validateIdentity(
      'SELECT * FROM xxx, yyy, zzz,',
      'SELECT * FROM xxx, yyy, zzz',
    );
    this.validateIdentity(
      'SELECT * FROM xxx, yyy, zzz, WHERE foo = bar',
      'SELECT * FROM xxx, yyy, zzz WHERE foo = bar',
    );
    this.validateIdentity(
      'SELECT * FROM xxx, yyy, zzz',
      'SELECT * FROM xxx, yyy, zzz',
    );

    this.validateAll(
      'SELECT LTRIM(RTRIM(col)) FROM t1',
      {
        write: {
          'duckdb': 'SELECT LTRIM(RTRIM(col)) FROM t1',
          'snowflake': 'SELECT LTRIM(RTRIM(col)) FROM t1',
        },
      },
    );
    this.validateAll(
      'SELECT value[\'x\'] AS x FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'x\', \'x\')])) AS _t0(seq, key, path, index, value, this)',
      {
        read: {
          'bigquery': 'SELECT x FROM UNNEST([STRUCT(\'x\' AS x)])',
          'snowflake': 'SELECT value[\'x\'] AS x FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'x\', \'x\')])) AS _t0(seq, key, path, index, value, this)',
        },
      },
    );
    this.validateAll(
      'SELECT value[\'x\'] AS x, value[\'y\'] AS y, value[\'z\'] AS z FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'x\', 1, \'y\', 2, \'z\', 3)])) AS _t0(seq, key, path, index, value, this)',
      {
        read: {
          'bigquery': 'SELECT x, y, z FROM UNNEST([STRUCT(1 AS x, 2 AS y, 3 AS z)])',
          'snowflake': 'SELECT value[\'x\'] AS x, value[\'y\'] AS y, value[\'z\'] AS z FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'x\', 1, \'y\', 2, \'z\', 3)])) AS _t0(seq, key, path, index, value, this)',
        },
      },
    );
    this.validateAll(
      'SELECT u1[\'x\'] AS x, u2[\'y\'] AS y FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'x\', 1)])) AS _t0(seq, key, path, index, u1, this) CROSS JOIN TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'y\', 2)])) AS _t1(seq, key, path, index, u2, this)',
      {
        read: {
          'bigquery': 'SELECT u1.x, u2.y FROM UNNEST([STRUCT(1 AS x)]) AS u1, UNNEST([STRUCT(2 AS y)]) AS u2',
          'snowflake': 'SELECT u1[\'x\'] AS x, u2[\'y\'] AS y FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'x\', 1)])) AS _t0(seq, key, path, index, u1, this) CROSS JOIN TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'y\', 2)])) AS _t1(seq, key, path, index, u2, this)',
        },
      },
    );
    this.validateAll(
      'SELECT t.id, value[\'name\'] AS name, value[\'age\'] AS age FROM t CROSS JOIN TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'name\', \'John\', \'age\', 30)])) AS _t0(seq, key, path, index, value, this)',
      {
        read: {
          'bigquery': 'SELECT t.id, name, age FROM t, UNNEST([STRUCT(\'John\' AS name, 30 AS age)])',
          'snowflake': 'SELECT t.id, value[\'name\'] AS name, value[\'age\'] AS age FROM t CROSS JOIN TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'name\', \'John\', \'age\', 30)])) AS _t0(seq, key, path, index, value, this)',
        },
      },
    );
    this.validateAll(
      'SELECT value FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'x\', 1)])) AS _t0(seq, key, path, index, value, this)',
      {
        read: {
          'bigquery': 'SELECT value FROM UNNEST([STRUCT(1 AS x)]) AS value',
          'snowflake': 'SELECT value FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'x\', 1)])) AS _t0(seq, key, path, index, value, this)',
        },
      },
    );
    this.validateAll(
      'SELECT t.col1, value[\'field1\'] AS field1, other_col, value[\'field2\'] AS field2 FROM t CROSS JOIN TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'field1\', \'a\', \'field2\', \'b\')])) AS _t0(seq, key, path, index, value, this)',
      {
        read: {
          'bigquery': 'SELECT t.col1, field1, other_col, field2 FROM t, UNNEST([STRUCT(\'a\' AS field1, \'b\' AS field2)])',
          'snowflake': 'SELECT t.col1, value[\'field1\'] AS field1, other_col, value[\'field2\'] AS field2 FROM t CROSS JOIN TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'field1\', \'a\', \'field2\', \'b\')])) AS _t0(seq, key, path, index, value, this)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM (SELECT value[\'x\'] AS x FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'x\', \'value\')])) AS _t0(seq, key, path, index, value, this))',
      {
        read: {
          'bigquery': 'SELECT * FROM (SELECT x FROM UNNEST([STRUCT(\'value\' AS x)]))',
          'snowflake': 'SELECT * FROM (SELECT value[\'x\'] AS x FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'x\', \'value\')])) AS _t0(seq, key, path, index, value, this))',
        },
      },
    );
    this.validateAll(
      'SELECT value FROM TABLE(FLATTEN(INPUT => [1, 2, 3])) AS _t0(seq, key, path, index, value, this)',
      {
        read: {
          'bigquery': 'SELECT value FROM UNNEST([1, 2, 3]) AS value',
          'snowflake': 'SELECT value FROM TABLE(FLATTEN(INPUT => [1, 2, 3])) AS _t0(seq, key, path, index, value, this)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM t1 AS t1 CROSS JOIN t2 AS t2 LEFT JOIN t3 AS t3 ON t1.a = t3.i',
      {
        read: {
          'bigquery': 'SELECT * FROM t1 AS t1, t2 AS t2 LEFT JOIN t3 AS t3 ON t1.a = t3.i',
          'snowflake': 'SELECT * FROM t1 AS t1 CROSS JOIN t2 AS t2 LEFT JOIN t3 AS t3 ON t1.a = t3.i',
        },
      },
    );
    this.validateAll(
      'SELECT value[\'x\'] AS x, yval, zval FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'x\', \'x\', \'y\', [\'y1\', \'y2\', \'y3\'], \'z\', [\'z1\', \'z2\', \'z3\'])])) AS _t0(seq, key, path, index, value, this) CROSS JOIN TABLE(FLATTEN(INPUT => value[\'y\'])) AS _t1(seq, key, path, index, yval, this) CROSS JOIN TABLE(FLATTEN(INPUT => value[\'z\'])) AS _t2(seq, key, path, index, zval, this)',
      {
        read: {
          'bigquery': 'SELECT x, yval, zval FROM UNNEST([STRUCT(\'x\' AS x, [\'y1\', \'y2\', \'y3\'] AS y, [\'z1\', \'z2\', \'z3\'] AS z)]), UNNEST(y) AS yval, UNNEST(z) AS zval',
          'snowflake': 'SELECT value[\'x\'] AS x, yval, zval FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'x\', \'x\', \'y\', [\'y1\', \'y2\', \'y3\'], \'z\', [\'z1\', \'z2\', \'z3\'])])) AS _t0(seq, key, path, index, value, this) CROSS JOIN TABLE(FLATTEN(INPUT => value[\'y\'])) AS _t1(seq, key, path, index, yval, this) CROSS JOIN TABLE(FLATTEN(INPUT => value[\'z\'])) AS _t2(seq, key, path, index, zval, this)',
        },
      },
    );
    this.validateAll(
      'SELECT _u[\'foo\'] AS foo, bar, baz FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'foo\', \'x\', \'bars\', [\'y\', \'z\'], \'bazs\', [\'w\'])])) AS _t0(seq, key, path, index, _u, this) CROSS JOIN TABLE(FLATTEN(INPUT => _u[\'bars\'])) AS _t1(seq, key, path, index, bar, this) CROSS JOIN TABLE(FLATTEN(INPUT => _u[\'bazs\'])) AS _t2(seq, key, path, index, baz, this)',
      {
        read: {
          'bigquery': 'SELECT _u.foo, bar, baz FROM UNNEST([struct(\'x\' AS foo, [\'y\', \'z\'] AS bars, [\'w\'] AS bazs)]) AS _u, UNNEST(_u.bars) AS bar, UNNEST(_u.bazs) AS baz',
        },
      },
    );
    this.validateAll(
      'SELECT _u, _u[\'foo\'] AS foo, _u[\'bar\'] AS bar FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'foo\', \'x\', \'bar\', \'y\')])) AS _t0(seq, key, path, index, _u, this)',
      {
        read: {
          'bigquery': 'select _u, _u.foo, _u.bar from unnest([struct(\'x\' as foo, \'y\' AS bar)]) as _u',
        },
      },
    );
    this.validateAll(
      'SELECT _u[\'foo\'][0].bar FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'foo\', [OBJECT_CONSTRUCT(\'bar\', 1)])])) AS _t0(seq, key, path, index, _u, this)',
      {
        read: {
          'bigquery': 'select _u.foo[0].bar from unnest([struct([struct(1 as bar)] as foo)]) as _u',
        },
      },
    );
    this.validateAll(
      'SELECT ARRAY_INTERSECTION([1, 2], [2, 3])',
      {
        write: {
          'snowflake': 'SELECT ARRAY_INTERSECTION([1, 2], [2, 3])',
          'starrocks': 'SELECT ARRAY_INTERSECT([1, 2], [2, 3])',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE test_table (id NUMERIC NOT NULL AUTOINCREMENT)',
      {
        write: {
          'duckdb': 'CREATE TABLE test_table (id DECIMAL(38, 0) NOT NULL)',
          'snowflake': 'CREATE TABLE test_table (id DECIMAL(38, 0) NOT NULL AUTOINCREMENT)',
        },
      },
    );
    this.validateAll(
      'SELECT TO_TIMESTAMP(\'2025-01-16 14:45:30.123\', \'yyyy-mm-DD hh24:mi:ss.ff6\')',
      {
        write: {
          '': 'SELECT STR_TO_TIME(\'2025-01-16 14:45:30.123\', \'%Y-%m-%d %H:%M:%S.%f\')',
          'snowflake': 'SELECT TO_TIMESTAMP(\'2025-01-16 14:45:30.123\', \'yyyy-mm-DD hh24:mi:ss.ff6\')',
        },
      },
    );
    this.validateAll(
      'ARRAY_CONSTRUCT_COMPACT(1, null, 2)',
      {
        write: {
          'spark': 'ARRAY_COMPACT(ARRAY(1, NULL, 2))',
          'snowflake': 'ARRAY_CONSTRUCT_COMPACT(1, NULL, 2)',
        },
      },
    );
    this.validateAll(
      'ARRAY_COMPACT(arr)',
      {
        read: {
          'spark': 'ARRAY_COMPACT(arr)',
          'databricks': 'ARRAY_COMPACT(arr)',
          'snowflake': 'ARRAY_COMPACT(arr)',
        },
        write: {
          'spark': 'ARRAY_COMPACT(arr)',
          'databricks': 'ARRAY_COMPACT(arr)',
        },
      },
    );
    this.validateAll(
      'OBJECT_CONSTRUCT_KEEP_NULL(\'key_1\', \'one\', \'key_2\', NULL)',
      {
        read: {
          'bigquery': 'JSON_OBJECT([\'key_1\', \'key_2\'], [\'one\', NULL])',
          'duckdb': 'JSON_OBJECT(\'key_1\', \'one\', \'key_2\', NULL)',
        },
        write: {
          'bigquery': 'JSON_OBJECT(\'key_1\', \'one\', \'key_2\', NULL)',
          'duckdb': 'JSON_OBJECT(\'key_1\', \'one\', \'key_2\', NULL)',
          'snowflake': 'OBJECT_CONSTRUCT_KEEP_NULL(\'key_1\', \'one\', \'key_2\', NULL)',
        },
      },
    );
    // Test simple case - uses MAKE_TIME (values within normal ranges)
    this.validateAll(
      'SELECT TIME_FROM_PARTS(12, 34, 56)',
      {
        write: {
          'duckdb': 'SELECT MAKE_TIME(12, 34, 56)',
          'snowflake': 'SELECT TIME_FROM_PARTS(12, 34, 56)',
        },
      },
    );
    // Test with nanoseconds - uses INTERVAL arithmetic
    this.validateAll(
      'SELECT TIME_FROM_PARTS(12, 34, 56, 987654321)',
      {
        write: {
          'duckdb': 'SELECT CAST(\'00:00:00\' AS TIME) + INTERVAL ((12 * 3600) + (34 * 60) + 56 + (987654321 / 1000000000.0)) SECOND',
          'snowflake': 'SELECT TIME_FROM_PARTS(12, 34, 56, 987654321)',
        },
      },
    );
    // Test overflow normalization - documented Snowflake feature with INTERVAL arithmetic
    this.validateAll(
      'SELECT TIME_FROM_PARTS(0, 100, 0)',
      {
        write: {
          'duckdb': 'SELECT CAST(\'00:00:00\' AS TIME) + INTERVAL ((0 * 3600) + (100 * 60) + 0) SECOND',
          'snowflake': 'SELECT TIME_FROM_PARTS(0, 100, 0)',
        },
      },
    );
    this.validateIdentity(
      'SELECT TIMESTAMPNTZFROMPARTS(2013, 4, 5, 12, 00, 00)',
      'SELECT TIMESTAMP_FROM_PARTS(2013, 4, 5, 12, 00, 00)',
    );
    this.validateAll(
      'SELECT TIMESTAMP_FROM_PARTS(2013, 4, 5, 12, 00, 00)',
      {
        read: {
          'duckdb': 'SELECT MAKE_TIMESTAMP(2013, 4, 5, 12, 00, 00)',
          'snowflake': 'SELECT TIMESTAMP_NTZ_FROM_PARTS(2013, 4, 5, 12, 00, 00)',
        },
        write: {
          'duckdb': 'SELECT MAKE_TIMESTAMP(2013, 4, 5, 12, 00, 00)',
          'snowflake': 'SELECT TIMESTAMP_FROM_PARTS(2013, 4, 5, 12, 00, 00)',
        },
      },
    );
    this.validateAll(
      'SELECT TIMESTAMP_FROM_PARTS(TO_DATE(\'2023-06-15\'), TO_TIME(\'14:30:45\'))',
      {
        write: {
          'duckdb': 'SELECT CAST(\'2023-06-15\' AS DATE) + CAST(\'14:30:45\' AS TIME)',
          'snowflake': 'SELECT TIMESTAMP_FROM_PARTS(CAST(\'2023-06-15\' AS DATE), CAST(\'14:30:45\' AS TIME))',
        },
      },
    );
    this.validateAll(
      'SELECT TIMESTAMP_NTZ_FROM_PARTS(TO_DATE(\'2023-06-15\'), TO_TIME(\'14:30:45\'))',
      {
        write: {
          'duckdb': 'SELECT CAST(\'2023-06-15\' AS DATE) + CAST(\'14:30:45\' AS TIME)',
          'snowflake': 'SELECT TIMESTAMP_FROM_PARTS(CAST(\'2023-06-15\' AS DATE), CAST(\'14:30:45\' AS TIME))',
        },
      },
    );
    this.validateAll(
      'SELECT TIMESTAMP_LTZ_FROM_PARTS(2023, 6, 15, 14, 30, 45)',
      {
        write: {
          'duckdb': 'SELECT CAST(MAKE_TIMESTAMP(2023, 6, 15, 14, 30, 45) AS TIMESTAMPTZ)',
          'snowflake': 'SELECT TIMESTAMP_LTZ_FROM_PARTS(2023, 6, 15, 14, 30, 45)',
        },
      },
    );
    this.validateAll(
      'SELECT TIMESTAMP_TZ_FROM_PARTS(2023, 6, 15, 14, 30, 45, 0, \'America/Los_Angeles\')',
      {
        write: {
          'duckdb': 'SELECT MAKE_TIMESTAMP(2023, 6, 15, 14, 30, 45) AT TIME ZONE \'America/Los_Angeles\'',
          'snowflake': 'SELECT TIMESTAMP_TZ_FROM_PARTS(2023, 6, 15, 14, 30, 45, 0, \'America/Los_Angeles\')',
        },
      },
    );
    this.validateAll(
      'WITH vartab(v) AS (select parse_json(\'[{"attr": [{"name": "banana"}]}]\')) SELECT GET_PATH(v, \'[0].attr[0].name\') FROM vartab',
      {
        write: {
          'bigquery': 'WITH vartab AS (SELECT PARSE_JSON(\'[{"attr": [{"name": "banana"}]}]\') AS v) SELECT JSON_EXTRACT(v, \'$[0].attr[0].name\') FROM vartab',
          'duckdb': 'WITH vartab(v) AS (SELECT JSON(\'[{"attr": [{"name": "banana"}]}]\')) SELECT v -> \'$[0].attr[0].name\' FROM vartab',
          'mysql': 'WITH vartab(v) AS (SELECT \'[{"attr": [{"name": "banana"}]}]\') SELECT JSON_EXTRACT(v, \'$[0].attr[0].name\') FROM vartab',
          'presto': 'WITH vartab(v) AS (SELECT JSON_PARSE(\'[{"attr": [{"name": "banana"}]}]\')) SELECT JSON_EXTRACT(v, \'$[0].attr[0].name\') FROM vartab',
          'snowflake': 'WITH vartab(v) AS (SELECT PARSE_JSON(\'[{"attr": [{"name": "banana"}]}]\')) SELECT GET_PATH(v, \'[0].attr[0].name\') FROM vartab',
          'tsql': 'WITH vartab(v) AS (SELECT \'[{"attr": [{"name": "banana"}]}]\') SELECT ISNULL(JSON_QUERY(v, \'$[0].attr[0].name\'), JSON_VALUE(v, \'$[0].attr[0].name\')) FROM vartab',
        },
      },
    );
    this.validateAll(
      'WITH vartab(v) AS (select parse_json(\'{"attr": [{"name": "banana"}]}\')) SELECT GET_PATH(v, \'attr[0].name\') FROM vartab',
      {
        write: {
          'bigquery': 'WITH vartab AS (SELECT PARSE_JSON(\'{"attr": [{"name": "banana"}]}\') AS v) SELECT JSON_EXTRACT(v, \'$.attr[0].name\') FROM vartab',
          'duckdb': 'WITH vartab(v) AS (SELECT JSON(\'{"attr": [{"name": "banana"}]}\')) SELECT v -> \'$.attr[0].name\' FROM vartab',
          'mysql': 'WITH vartab(v) AS (SELECT \'{"attr": [{"name": "banana"}]}\') SELECT JSON_EXTRACT(v, \'$.attr[0].name\') FROM vartab',
          'presto': 'WITH vartab(v) AS (SELECT JSON_PARSE(\'{"attr": [{"name": "banana"}]}\')) SELECT JSON_EXTRACT(v, \'$.attr[0].name\') FROM vartab',
          'snowflake': 'WITH vartab(v) AS (SELECT PARSE_JSON(\'{"attr": [{"name": "banana"}]}\')) SELECT GET_PATH(v, \'attr[0].name\') FROM vartab',
          'tsql': 'WITH vartab(v) AS (SELECT \'{"attr": [{"name": "banana"}]}\') SELECT ISNULL(JSON_QUERY(v, \'$.attr[0].name\'), JSON_VALUE(v, \'$.attr[0].name\')) FROM vartab',
        },
      },
    );
    this.validateAll(
      'SELECT PARSE_JSON(\'{"fruit":"banana"}\'):fruit',
      {
        write: {
          'bigquery': 'SELECT JSON_EXTRACT(PARSE_JSON(\'{"fruit":"banana"}\'), \'$.fruit\')',
          'databricks': 'SELECT PARSE_JSON(\'{"fruit":"banana"}\'):fruit',
          'duckdb': 'SELECT JSON(\'{"fruit":"banana"}\') -> \'$.fruit\'',
          'mysql': 'SELECT JSON_EXTRACT(\'{"fruit":"banana"}\', \'$.fruit\')',
          'presto': 'SELECT JSON_EXTRACT(JSON_PARSE(\'{"fruit":"banana"}\'), \'$.fruit\')',
          'snowflake': 'SELECT GET_PATH(PARSE_JSON(\'{"fruit":"banana"}\'), \'fruit\')',
          'spark': 'SELECT GET_JSON_OBJECT(\'{"fruit":"banana"}\', \'$.fruit\')',
          'tsql': 'SELECT ISNULL(JSON_QUERY(\'{"fruit":"banana"}\', \'$.fruit\'), JSON_VALUE(\'{"fruit":"banana"}\', \'$.fruit\'))',
        },
      },
    );
    this.validateAll(
      'SELECT TO_ARRAY([\'test\'])',
      {
        write: {
          'snowflake': 'SELECT TO_ARRAY([\'test\'])',
          'spark': 'SELECT ARRAY(\'test\')',
        },
      },
    );
    this.validateAll(
      'SELECT TO_ARRAY([\'test\'])',
      {
        write: {
          'snowflake': 'SELECT TO_ARRAY([\'test\'])',
          'spark': 'SELECT ARRAY(\'test\')',
        },
      },
    );
    this.validateAll(
      // We need to qualify the columns in this query because "value" would be ambiguous
      'WITH t(x, "value") AS (SELECT [1, 2, 3], 1) SELECT IFF(_u.pos = _u_2.pos_2, _u_2."value", NULL) AS "value" FROM t CROSS JOIN TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, (GREATEST(ARRAY_SIZE(t.x)) - 1) + 1))) AS _u(seq, key, path, index, pos, this) CROSS JOIN TABLE(FLATTEN(INPUT => t.x)) AS _u_2(seq, key, path, pos_2, "value", this) WHERE _u.pos = _u_2.pos_2 OR (_u.pos > (ARRAY_SIZE(t.x) - 1) AND _u_2.pos_2 = (ARRAY_SIZE(t.x) - 1))',
      {
        read: {
          'duckdb': 'WITH t(x, "value") AS (SELECT [1,2,3], 1) SELECT UNNEST(t.x) AS "value" FROM t',
        },
      },
    );
    this.validateAll(
      'SELECT { \'Manitoba\': \'Winnipeg\', \'foo\': \'bar\' } AS province_capital',
      {
        write: {
          'duckdb': 'SELECT {\'Manitoba\': \'Winnipeg\', \'foo\': \'bar\'} AS province_capital',
          'snowflake': 'SELECT OBJECT_CONSTRUCT(\'Manitoba\', \'Winnipeg\', \'foo\', \'bar\') AS province_capital',
          'spark': 'SELECT STRUCT(\'Winnipeg\' AS Manitoba, \'bar\' AS foo) AS province_capital',
        },
      },
    );
    this.validateAll(
      'SELECT COLLATE(\'B\', \'und:ci\')',
      {
        write: {
          'bigquery': 'SELECT COLLATE(\'B\', \'und:ci\')',
          'snowflake': 'SELECT COLLATE(\'B\', \'und:ci\')',
        },
      },
    );

    this.validateAll(
      'SELECT To_BOOLEAN(\'T\')',
      {
        write: {
          'duckdb': 'SELECT CASE WHEN UPPER(CAST(\'T\' AS TEXT)) = \'ON\' THEN TRUE WHEN UPPER(CAST(\'T\' AS TEXT)) = \'OFF\' THEN FALSE WHEN ISNAN(TRY_CAST(\'T\' AS REAL)) OR ISINF(TRY_CAST(\'T\' AS REAL)) THEN ERROR(\'TO_BOOLEAN: Non-numeric values NaN and INF are not supported\') ELSE CAST(\'T\' AS BOOLEAN) END',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM x START WITH a = b CONNECT BY c = PRIOR d',
      {
        read: {
          'oracle': 'SELECT * FROM x START WITH a = b CONNECT BY c = PRIOR d',
        },
        write: {
          'oracle': 'SELECT * FROM x START WITH a = b CONNECT BY c = PRIOR d',
          'snowflake': 'SELECT * FROM x START WITH a = b CONNECT BY c = PRIOR d',
        },
      },
    );
    this.validateAll(
      'SELECT INSERT(a, 0, 0, \'b\')',
      {
        read: {
          'mysql': 'SELECT INSERT(a, 0, 0, \'b\')',
          'snowflake': 'SELECT INSERT(a, 0, 0, \'b\')',
          'tsql': 'SELECT STUFF(a, 0, 0, \'b\')',
        },
        write: {
          'mysql': 'SELECT INSERT(a, 0, 0, \'b\')',
          'snowflake': 'SELECT INSERT(a, 0, 0, \'b\')',
          'tsql': 'SELECT STUFF(a, 0, 0, \'b\')',
        },
      },
    );
    this.validateAll(
      'ARRAY_GENERATE_RANGE(0, 3)',
      {
        write: {
          'bigquery': 'GENERATE_ARRAY(0, 3 - 1)',
          'postgres': 'GENERATE_SERIES(0, 3 - 1)',
          'presto': 'SEQUENCE(0, 3 - 1)',
          'snowflake': 'ARRAY_GENERATE_RANGE(0, (3 - 1) + 1)',
        },
      },
    );
    this.validateAll(
      'ARRAY_GENERATE_RANGE(0, 3 + 1)',
      {
        read: {
          'bigquery': 'GENERATE_ARRAY(0, 3)',
          'postgres': 'GENERATE_SERIES(0, 3)',
          'presto': 'SEQUENCE(0, 3)',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_PART(\'year\', TIMESTAMP \'2020-01-01\')',
      {
        write: {
          'hive': 'SELECT EXTRACT(year FROM CAST(\'2020-01-01\' AS TIMESTAMP))',
          'snowflake': 'SELECT DATE_PART(\'year\', CAST(\'2020-01-01\' AS TIMESTAMP))',
          'spark': 'SELECT EXTRACT(year FROM CAST(\'2020-01-01\' AS TIMESTAMP))',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM (VALUES (0) foo(bar))',
      {
        write: {
          'snowflake': 'SELECT * FROM (VALUES (0)) AS foo(bar)',
        },
      },
    );
    this.validateAll(
      'OBJECT_CONSTRUCT(\'a\', b, \'c\', d)',
      {
        read: {
          '': 'STRUCT(b as a, d as c)',
        },
        write: {
          'duckdb': '{\'a\': b, \'c\': d}',
          'snowflake': 'OBJECT_CONSTRUCT(\'a\', b, \'c\', d)',
          '': 'STRUCT(b AS a, d AS c)',
        },
      },
    );
    this.validateIdentity('OBJECT_CONSTRUCT(a, b, c, d)');

    this.validateAll(
      'SELECT i, p, o FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1',
      {
        write: {
          '': 'SELECT i, p, o FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o NULLS LAST) = 1',
          'databricks': 'SELECT i, p, o FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o NULLS LAST) = 1',
          'hive': 'SELECT i, p, o FROM (SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o NULLS LAST) AS _w FROM qt) AS _t WHERE _w = 1',
          'presto': 'SELECT i, p, o FROM (SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS _w FROM qt) AS _t WHERE _w = 1',
          'snowflake': 'SELECT i, p, o FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1',
          'spark': 'SELECT i, p, o FROM (SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o NULLS LAST) AS _w FROM qt) AS _t WHERE _w = 1',
          'sqlite': 'SELECT i, p, o FROM (SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o NULLS LAST) AS _w FROM qt) AS _t WHERE _w = 1',
          'trino': 'SELECT i, p, o FROM (SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS _w FROM qt) AS _t WHERE _w = 1',
        },
      },
    );

    this.validateAll(
      'SELECT NTH_VALUE(is_deleted, 2) FROM FIRST IGNORE NULLS OVER (PARTITION BY id) AS nth_is_deleted FROM my_table',
      {
        write: {
          'snowflake': 'SELECT NTH_VALUE(is_deleted, 2) FROM FIRST IGNORE NULLS OVER (PARTITION BY id) AS nth_is_deleted FROM my_table',
          'duckdb': 'SELECT NTH_VALUE(is_deleted, 2 IGNORE NULLS) OVER (PARTITION BY id) AS nth_is_deleted FROM my_table',
        },
      },
    );

    this.validateAll(
      'SELECT NTH_VALUE(is_deleted, 2) FROM LAST RESPECT NULLS OVER (PARTITION BY id) AS nth_is_deleted FROM my_table',
      {
        write: {
          'snowflake': 'SELECT NTH_VALUE(is_deleted, 2) FROM LAST RESPECT NULLS OVER (PARTITION BY id) AS nth_is_deleted FROM my_table',
          'duckdb': 'SELECT NTH_VALUE(is_deleted, 2 RESPECT NULLS) OVER (PARTITION BY id) AS nth_is_deleted FROM my_table',
        },
      },
    );

    this.validateAll(
      'SELECT NTH_VALUE(is_deleted, 2) OVER (PARTITION BY id) AS nth_is_deleted FROM my_table',
      {
        write: {
          'snowflake': 'SELECT NTH_VALUE(is_deleted, 2) OVER (PARTITION BY id) AS nth_is_deleted FROM my_table',
          'duckdb': 'SELECT NTH_VALUE(is_deleted, 2) OVER (PARTITION BY id) AS nth_is_deleted FROM my_table',
        },
      },
    );

    this.validateAll(
      'SELECT BOOLOR_AGG(c1), BOOLOR_AGG(c2) FROM test',
      {
        write: {
          '': 'SELECT LOGICAL_OR(c1), LOGICAL_OR(c2) FROM test',
          'duckdb': 'SELECT BOOL_OR(CAST(c1 AS BOOLEAN)), BOOL_OR(CAST(c2 AS BOOLEAN)) FROM test',
          'oracle': 'SELECT MAX(c1), MAX(c2) FROM test',
          'postgres': 'SELECT BOOL_OR(c1), BOOL_OR(c2) FROM test',
          'snowflake': 'SELECT BOOLOR_AGG(c1), BOOLOR_AGG(c2) FROM test',
          'spark': 'SELECT BOOL_OR(c1), BOOL_OR(c2) FROM test',
          'sqlite': 'SELECT MAX(c1), MAX(c2) FROM test',
        },
      },
    );
    this.validateAll(
      'SELECT BOOLAND_AGG(c1), BOOLAND_AGG(c2) FROM test',
      {
        write: {
          '': 'SELECT LOGICAL_AND(c1), LOGICAL_AND(c2) FROM test',
          'duckdb': 'SELECT BOOL_AND(CAST(c1 AS BOOLEAN)), BOOL_AND(CAST(c2 AS BOOLEAN)) FROM test',
          'oracle': 'SELECT MIN(c1), MIN(c2) FROM test',
          'postgres': 'SELECT BOOL_AND(c1), BOOL_AND(c2) FROM test',
          'snowflake': 'SELECT BOOLAND_AGG(c1), BOOLAND_AGG(c2) FROM test',
          'spark': 'SELECT BOOL_AND(c1), BOOL_AND(c2) FROM test',
          'sqlite': 'SELECT MIN(c1), MIN(c2) FROM test',
          'mysql': 'SELECT MIN(c1), MIN(c2) FROM test',
        },
      },
    );
    this.validateAll(
      'SELECT BOOLXOR_AGG(c1) FROM test',
      {
        write: {
          'duckdb': 'SELECT COUNT_IF(CAST(c1 AS BOOLEAN)) = 1 FROM test',
          'snowflake': 'SELECT BOOLXOR_AGG(c1) FROM test',
        },
      },
    );
    for (const suffix of [
      '',
      ' OVER ()',
    ]) {
      this.validateAll(
        `SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)${suffix}`,
        {
          read: {
            'postgres': `SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)${suffix}`,
          },
          write: {
            '': `SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x NULLS LAST)${suffix}`,
            'duckdb': `SELECT QUANTILE_CONT(x, 0.5 ORDER BY x)${suffix}`,
            'postgres': `SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)${suffix}`,
            'snowflake': `SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)${suffix}`,
          },
        },
      );
      for (const func of [
        'COVAR_POP',
        'COVAR_SAMP',
      ]) {
        this.validateAll(
          `SELECT ${func}(y, x)${suffix}`,
          {
            write: {
              '': `SELECT ${func}(y, x)${suffix}`,
              'duckdb': `SELECT ${func}(y, x)${suffix}`,
              'postgres': `SELECT ${func}(y, x)${suffix}`,
              'snowflake': `SELECT ${func}(y, x)${suffix}`,
            },
          },
        );
        this.validateAll(
          'TO_CHAR(x, y)',
          {
            read: {
              '': 'TO_CHAR(x, y)',
              'snowflake': 'TO_VARCHAR(x, y)',
            },
            write: {
              '': 'CAST(x AS TEXT)',
              'databricks': 'TO_CHAR(x, y)',
              'drill': 'TO_CHAR(x, y)',
              'oracle': 'TO_CHAR(x, y)',
              'postgres': 'TO_CHAR(x, y)',
              'snowflake': 'TO_CHAR(x, y)',
              'teradata': 'TO_CHAR(x, y)',
            },
          },
        );
        for (const toFunc of [
          'TO_CHAR',
          'TO_VARCHAR',
        ]) {
          this.validateIdentity(
            `${toFunc}(foo::DATE, 'yyyy')`,
            'TO_CHAR(CAST(foo AS DATE), \'yyyy\')',
          );
          this.validateAll(
            `${toFunc}(foo::TIMESTAMP, 'YYYY-MM')`,
            {
              write: {
                'snowflake': 'TO_CHAR(CAST(foo AS TIMESTAMP), \'yyyy-mm\')',
                'duckdb': 'STRFTIME(CAST(foo AS TIMESTAMP), \'%Y-%m\')',
              },
            },
          );
          this.validateAll(
            'SQUARE(x)',
            {
              write: {
                'bigquery': 'POWER(x, 2)',
                'clickhouse': 'POWER(x, 2)',
                'databricks': 'POWER(x, 2)',
                'drill': 'POW(x, 2)',
                'duckdb': 'POWER(x, 2)',
                'hive': 'POWER(x, 2)',
                'mysql': 'POWER(x, 2)',
                'oracle': 'POWER(x, 2)',
                'postgres': 'POWER(x, 2)',
                'presto': 'POWER(x, 2)',
                'redshift': 'POWER(x, 2)',
                'snowflake': 'POWER(x, 2)',
                'spark': 'POWER(x, 2)',
                'sqlite': 'POWER(x, 2)',
                'starrocks': 'POWER(x, 2)',
                'teradata': 'x ** 2',
                'trino': 'POWER(x, 2)',
                'tsql': 'POWER(x, 2)',
              },
            },
          );
          this.validateAll(
            'POWER(x, 2)',
            {
              read: {
                'oracle': 'SQUARE(x)',
                'snowflake': 'SQUARE(x)',
                'tsql': 'SQUARE(x)',
              },
            },
          );
          this.validateAll(
            'DIV0(foo, bar)',
            {
              write: {
                'snowflake': 'IFF(bar = 0 AND NOT foo IS NULL, 0, foo / bar)',
                'sqlite': 'IIF(bar = 0 AND NOT foo IS NULL, 0, CAST(foo AS REAL) / bar)',
                'presto': 'IF(bar = 0 AND NOT foo IS NULL, 0, CAST(foo AS DOUBLE) / bar)',
                'spark': 'IF(bar = 0 AND NOT foo IS NULL, 0, foo / bar)',
                'hive': 'IF(bar = 0 AND NOT foo IS NULL, 0, foo / bar)',
                'duckdb': 'CASE WHEN bar = 0 AND NOT foo IS NULL THEN 0 ELSE foo / bar END',
              },
            },
          );
          this.validateAll(
            'DIV0(a - b, c - d)',
            {
              write: {
                'snowflake': 'IFF((c - d) = 0 AND NOT (a - b) IS NULL, 0, (a - b) / (c - d))',
                'sqlite': 'IIF((c - d) = 0 AND NOT (a - b) IS NULL, 0, CAST((a - b) AS REAL) / (c - d))',
                'presto': 'IF((c - d) = 0 AND NOT (a - b) IS NULL, 0, CAST((a - b) AS DOUBLE) / (c - d))',
                'spark': 'IF((c - d) = 0 AND NOT (a - b) IS NULL, 0, (a - b) / (c - d))',
                'hive': 'IF((c - d) = 0 AND NOT (a - b) IS NULL, 0, (a - b) / (c - d))',
                'duckdb': 'CASE WHEN (c - d) = 0 AND NOT (a - b) IS NULL THEN 0 ELSE (a - b) / (c - d) END',
              },
            },
          );
          this.validateAll(
            'DIV0NULL(foo, bar)',
            {
              write: {
                'snowflake': 'IFF(bar = 0 OR bar IS NULL, 0, foo / bar)',
                'sqlite': 'IIF(bar = 0 OR bar IS NULL, 0, CAST(foo AS REAL) / bar)',
                'presto': 'IF(bar = 0 OR bar IS NULL, 0, CAST(foo AS DOUBLE) / bar)',
                'spark': 'IF(bar = 0 OR bar IS NULL, 0, foo / bar)',
                'hive': 'IF(bar = 0 OR bar IS NULL, 0, foo / bar)',
                'duckdb': 'CASE WHEN bar = 0 OR bar IS NULL THEN 0 ELSE foo / bar END',
              },
            },
          );
          this.validateAll(
            'DIV0NULL(a - b, c - d)',
            {
              write: {
                'snowflake': 'IFF((c - d) = 0 OR (c - d) IS NULL, 0, (a - b) / (c - d))',
                'sqlite': 'IIF((c - d) = 0 OR (c - d) IS NULL, 0, CAST((a - b) AS REAL) / (c - d))',
                'presto': 'IF((c - d) = 0 OR (c - d) IS NULL, 0, CAST((a - b) AS DOUBLE) / (c - d))',
                'spark': 'IF((c - d) = 0 OR (c - d) IS NULL, 0, (a - b) / (c - d))',
                'hive': 'IF((c - d) = 0 OR (c - d) IS NULL, 0, (a - b) / (c - d))',
                'duckdb': 'CASE WHEN (c - d) = 0 OR (c - d) IS NULL THEN 0 ELSE (a - b) / (c - d) END',
              },
            },
          );
          this.validateAll(
            'ZEROIFNULL(foo)',
            {
              write: {
                'snowflake': 'IFF(foo IS NULL, 0, foo)',
                'sqlite': 'IIF(foo IS NULL, 0, foo)',
                'presto': 'IF(foo IS NULL, 0, foo)',
                'spark': 'IF(foo IS NULL, 0, foo)',
                'hive': 'IF(foo IS NULL, 0, foo)',
                'duckdb': 'CASE WHEN foo IS NULL THEN 0 ELSE foo END',
              },
            },
          );
          this.validateAll(
            'NULLIFZERO(foo)',
            {
              write: {
                'snowflake': 'IFF(foo = 0, NULL, foo)',
                'sqlite': 'IIF(foo = 0, NULL, foo)',
                'presto': 'IF(foo = 0, NULL, foo)',
                'spark': 'IF(foo = 0, NULL, foo)',
                'hive': 'IF(foo = 0, NULL, foo)',
                'duckdb': 'CASE WHEN foo = 0 THEN NULL ELSE foo END',
              },
            },
          );
          this.validateAll(
            'SELECT * EXCLUDE (a, b) REPLACE (c AS d, E AS F) FROM xxx',
            {
              read: {
                'duckdb': 'SELECT * EXCLUDE (a, b) REPLACE (c AS d, E AS F) FROM xxx',
              },
              write: {
                'snowflake': 'SELECT * EXCLUDE (a, b) REPLACE (c AS d, E AS F) FROM xxx',
                'duckdb': 'SELECT * EXCLUDE (a, b) REPLACE (c AS d, E AS F) FROM xxx',
              },
            },
          );
          this.validateAll(
            'SELECT PARSE_JSON(\'{"a": {"b c": "foo"}}\'):a:"b c"',
            {
              write: {
                'duckdb': 'SELECT JSON(\'{"a": {"b c": "foo"}}\') -> \'$.a."b c"\'',
                'mysql': 'SELECT JSON_EXTRACT(\'{"a": {"b c": "foo"}}\', \'$.a."b c"\')',
                'snowflake': 'SELECT GET_PATH(PARSE_JSON(\'{"a": {"b c": "foo"}}\'), \'a["b c"]\')',
              },
            },
          );
          this.validateAll(
            'SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10',
            {
              write: {
                'bigquery': 'SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a NULLS LAST LIMIT 10',
                'snowflake': 'SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10',
              },
            },
          );
          this.validateAll(
            'SELECT a FROM test AS t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY Z) = 1',
            {
              write: {
                'bigquery': 'SELECT a FROM test AS t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY Z NULLS LAST) = 1',
                'snowflake': 'SELECT a FROM test AS t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY Z) = 1',
              },
            },
          );
          this.validateAll(
            'SELECT TO_TIMESTAMP(col, \'DD-MM-YYYY HH12:MI:SS\') FROM t',
            {
              write: {
                'bigquery': 'SELECT PARSE_TIMESTAMP(\'%d-%m-%Y %I:%M:%S\', col) FROM t',
                'duckdb': 'SELECT STRPTIME(col, \'%d-%m-%Y %I:%M:%S\') FROM t',
                'snowflake': 'SELECT TO_TIMESTAMP(col, \'DD-mm-yyyy hh12:mi:ss\') FROM t',
                'spark': 'SELECT TO_TIMESTAMP(col, \'dd-MM-yyyy hh:mm:ss\') FROM t',
              },
            },
          );
          this.validateAll(
            'SELECT TO_TIMESTAMP(1659981729)',
            {
              write: {
                'bigquery': 'SELECT TIMESTAMP_SECONDS(1659981729)',
                'snowflake': 'SELECT TO_TIMESTAMP(1659981729)',
                'spark': 'SELECT CAST(FROM_UNIXTIME(1659981729) AS TIMESTAMP)',
                'redshift': 'SELECT (TIMESTAMP \'epoch\' + 1659981729 * INTERVAL \'1 SECOND\')',
              },
            },
          );
          this.validateAll(
            'SELECT TO_TIMESTAMP(1659981729000, 3)',
            {
              write: {
                'bigquery': 'SELECT TIMESTAMP_MILLIS(1659981729000)',
                'snowflake': 'SELECT TO_TIMESTAMP(1659981729000, 3)',
                'spark': 'SELECT TIMESTAMP_MILLIS(1659981729000)',
                'redshift': 'SELECT (TIMESTAMP \'epoch\' + (1659981729000 / POWER(10, 3)) * INTERVAL \'1 SECOND\')',
              },
            },
          );
          this.validateAll(
            'SELECT TO_TIMESTAMP(16599817290000, 4)',
            {
              write: {
                'bigquery': 'SELECT TIMESTAMP_SECONDS(CAST(16599817290000 / POWER(10, 4) AS INT64))',
                'snowflake': 'SELECT TO_TIMESTAMP(16599817290000, 4)',
                'spark': 'SELECT TIMESTAMP_SECONDS(16599817290000 / POWER(10, 4))',
                'redshift': 'SELECT (TIMESTAMP \'epoch\' + (16599817290000 / POWER(10, 4)) * INTERVAL \'1 SECOND\')',
              },
            },
          );
          this.validateAll(
            'SELECT TO_TIMESTAMP(\'1659981729\')',
            {
              write: {
                'snowflake': 'SELECT TO_TIMESTAMP(\'1659981729\')',
                'spark': 'SELECT CAST(FROM_UNIXTIME(\'1659981729\') AS TIMESTAMP)',
              },
            },
          );
          this.validateAll(
            'SELECT TO_TIMESTAMP(1659981729000000000, 9)',
            {
              write: {
                'bigquery': 'SELECT TIMESTAMP_SECONDS(CAST(1659981729000000000 / POWER(10, 9) AS INT64))',
                'duckdb': 'SELECT TO_TIMESTAMP(1659981729000000000 / POWER(10, 9)) AT TIME ZONE \'UTC\'',
                'presto': 'SELECT FROM_UNIXTIME(CAST(1659981729000000000 AS DOUBLE) / POW(10, 9))',
                'snowflake': 'SELECT TO_TIMESTAMP(1659981729000000000, 9)',
                'spark': 'SELECT TIMESTAMP_SECONDS(1659981729000000000 / POWER(10, 9))',
                'redshift': 'SELECT (TIMESTAMP \'epoch\' + (1659981729000000000 / POWER(10, 9)) * INTERVAL \'1 SECOND\')',
              },
            },
          );
          this.validateAll(
            'SELECT TO_TIMESTAMP(\'2013-04-05 01:02:03\')',
            {
              write: {
                'bigquery': 'SELECT CAST(\'2013-04-05 01:02:03\' AS DATETIME)',
                'snowflake': 'SELECT CAST(\'2013-04-05 01:02:03\' AS TIMESTAMP)',
                'spark': 'SELECT CAST(\'2013-04-05 01:02:03\' AS TIMESTAMP)',
              },
            },
          );
          this.validateAll(
            'SELECT TO_TIMESTAMP(\'04/05/2013 01:02:03\', \'mm/DD/yyyy hh24:mi:ss\')',
            {
              read: {
                'bigquery': 'SELECT PARSE_TIMESTAMP(\'%m/%d/%Y %H:%M:%S\', \'04/05/2013 01:02:03\')',
                'duckdb': 'SELECT STRPTIME(\'04/05/2013 01:02:03\', \'%m/%d/%Y %H:%M:%S\')',
              },
              write: {
                'bigquery': 'SELECT PARSE_TIMESTAMP(\'%m/%d/%Y %T\', \'04/05/2013 01:02:03\')',
                'snowflake': 'SELECT TO_TIMESTAMP(\'04/05/2013 01:02:03\', \'mm/DD/yyyy hh24:mi:ss\')',
                'spark': 'SELECT TO_TIMESTAMP(\'04/05/2013 01:02:03\', \'MM/dd/yyyy HH:mm:ss\')',
              },
            },
          );
          this.validateAll(
            'TO_TIMESTAMP(\'2024-01-15 3:00 AM\', \'YYYY-MM-DD HH12:MI PM\')',
            {
              write: {
                'duckdb': 'STRPTIME(\'2024-01-15 3:00 AM\', \'%Y-%m-%d %I:%M %p\')',
                'snowflake': 'TO_TIMESTAMP(\'2024-01-15 3:00 AM\', \'yyyy-mm-DD hh12:mi pm\')',
              },
            },
          );
          this.validateAll(
            'TO_TIMESTAMP(\'2024-01-15 3:00 PM\', \'YYYY-MM-DD HH12:MI AM\')',
            {
              write: {
                'duckdb': 'STRPTIME(\'2024-01-15 3:00 PM\', \'%Y-%m-%d %I:%M %p\')',
                'snowflake': 'TO_TIMESTAMP(\'2024-01-15 3:00 PM\', \'yyyy-mm-DD hh12:mi pm\')',
              },
            },
          );
          this.validateAll(
            'TO_TIMESTAMP(\'2024-01-15 3:00 PM\', \'YYYY-MM-DD HH12:MI PM\')',
            {
              write: {
                'duckdb': 'STRPTIME(\'2024-01-15 3:00 PM\', \'%Y-%m-%d %I:%M %p\')',
                'snowflake': 'TO_TIMESTAMP(\'2024-01-15 3:00 PM\', \'yyyy-mm-DD hh12:mi pm\')',
              },
            },
          );
          this.validateAll(
            'TO_TIMESTAMP(\'2024-01-15 3:00 AM\', \'YYYY-MM-DD HH12:MI AM\')',
            {
              write: {
                'duckdb': 'STRPTIME(\'2024-01-15 3:00 AM\', \'%Y-%m-%d %I:%M %p\')',
                'snowflake': 'TO_TIMESTAMP(\'2024-01-15 3:00 AM\', \'yyyy-mm-DD hh12:mi pm\')',
              },
            },
          );
          this.validateAll(
            'SELECT IFF(TRUE, \'true\', \'false\')',
            {
              write: {
                'snowflake': 'SELECT IFF(TRUE, \'true\', \'false\')',
                'spark': 'SELECT IF(TRUE, \'true\', \'false\')',
              },
            },
          );
          this.validateAll(
            'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname',
            {
              write: {
                'duckdb': 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname',
                'postgres': 'SELECT fname, lname, age FROM person ORDER BY age DESC, fname ASC, lname',
                'presto': 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname',
                'hive': 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname NULLS LAST',
                'spark': 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname NULLS LAST',
                'snowflake': 'SELECT fname, lname, age FROM person ORDER BY age DESC, fname ASC, lname',
              },
            },
          );
          this.validateAll(
            'SELECT ARRAY_AGG(DISTINCT a)',
            {
              write: {
                'spark': 'SELECT COLLECT_LIST(DISTINCT a)',
                'snowflake': 'SELECT ARRAY_AGG(DISTINCT a)',
                'duckdb': 'SELECT ARRAY_AGG(DISTINCT a) FILTER(WHERE a IS NOT NULL)',
                'presto': 'SELECT ARRAY_AGG(DISTINCT a) FILTER(WHERE a IS NOT NULL)',
              },
            },
          );
          this.validateAll(
            'SELECT ARRAY_AGG(col) WITHIN GROUP (ORDER BY sort_col)',
            {
              write: {
                'snowflake': 'SELECT ARRAY_AGG(col) WITHIN GROUP (ORDER BY sort_col)',
                'duckdb': 'SELECT ARRAY_AGG(col ORDER BY sort_col) FILTER(WHERE col IS NOT NULL)',
              },
            },
          );
          this.validateAll(
            'SELECT ARRAY_AGG(DISTINCT col) WITHIN GROUP (ORDER BY col DESC)',
            {
              write: {
                'snowflake': 'SELECT ARRAY_AGG(DISTINCT col) WITHIN GROUP (ORDER BY col DESC)',
                'duckdb': 'SELECT ARRAY_AGG(DISTINCT col ORDER BY col DESC NULLS FIRST) FILTER(WHERE col IS NOT NULL)',
              },
            },
          );
          this.validateAll(
            'ARRAY_TO_STRING(x, \'\')',
            {
              read: {
                'duckdb': 'ARRAY_TO_STRING(x, \'\')',
              },
              write: {
                'spark': 'ARRAY_JOIN(x, \'\')',
                'snowflake': 'ARRAY_TO_STRING(x, \'\')',
                'duckdb': 'ARRAY_TO_STRING(x, \'\')',
              },
            },
          );
          this.validateAll(
            'TO_ARRAY(x)',
            {
              write: {
                'spark': 'IF(x IS NULL, NULL, ARRAY(x))',
                'snowflake': 'TO_ARRAY(x)',
              },
            },
          );
          this.validateAll(
            'SELECT * FROM a INTERSECT ALL SELECT * FROM b',
            {
              write: {
                'snowflake': UnsupportedError,
              },
            },
          );
          this.validateAll(
            'SELECT * FROM a EXCEPT ALL SELECT * FROM b',
            {
              write: {
                'snowflake': UnsupportedError,
              },
            },
          );
          this.validateAll(
            'SELECT ARRAY_UNION_AGG(a)',
            {
              write: {
                'snowflake': 'SELECT ARRAY_UNION_AGG(a)',
              },
            },
          );
          this.validateAll(
            'SELECT $$a$$',
            {
              write: {
                'snowflake': 'SELECT \'a\'',
              },
            },
          );
          this.validateAll(
            'SELECT RLIKE(a, b)',
            {
              write: {
                'hive': 'SELECT a RLIKE b',
                'snowflake': 'SELECT REGEXP_LIKE(a, b)',
                'spark': 'SELECT a RLIKE b',
              },
            },
          );
          this.validateAll(
            '\'foo\' REGEXP \'bar\'',
            {
              write: {
                'snowflake': 'REGEXP_LIKE(\'foo\', \'bar\')',
                'postgres': '\'foo\' ~ \'bar\'',
                'mysql': 'REGEXP_LIKE(\'foo\', \'bar\')',
                'bigquery': 'REGEXP_CONTAINS(\'foo\', \'bar\')',
              },
            },
          );
          this.validateAll(
            '\'foo\' NOT REGEXP \'bar\'',
            {
              write: {
                'snowflake': 'NOT REGEXP_LIKE(\'foo\', \'bar\')',
                'postgres': 'NOT \'foo\' ~ \'bar\'',
                'mysql': 'NOT REGEXP_LIKE(\'foo\', \'bar\')',
                'bigquery': 'NOT REGEXP_CONTAINS(\'foo\', \'bar\')',
              },
            },
          );
          this.validateAll(
            'SELECT a FROM test pivot',
            {
              write: {
                'snowflake': 'SELECT a FROM test AS pivot',
              },
            },
          );
          this.validateAll(
            'SELECT a FROM test unpivot',
            {
              write: {
                'snowflake': 'SELECT a FROM test AS unpivot',
              },
            },
          );
          this.validateAll(
            'trim(date_column, \'UTC\')',
            {
              write: {
                'bigquery': 'TRIM(date_column, \'UTC\')',
                'snowflake': 'TRIM(date_column, \'UTC\')',
                'postgres': 'TRIM(\'UTC\' FROM date_column)',
              },
            },
          );
          this.validateAll(
            'trim(date_column)',
            {
              write: {
                'snowflake': 'TRIM(date_column)',
                'bigquery': 'TRIM(date_column)',
              },
            },
          );
          this.validateAll(
            'DECODE(x, a, b, c, d, e)',
            {
              write: {
                'duckdb': 'CASE WHEN x = a OR (x IS NULL AND a IS NULL) THEN b WHEN x = c OR (x IS NULL AND c IS NULL) THEN d ELSE e END',
                'snowflake': 'DECODE(x, a, b, c, d, e)',
              },
            },
          );
          this.validateAll(
            'DECODE(TRUE, a.b = \'value\', \'value\')',
            {
              write: {
                'duckdb': 'CASE WHEN TRUE = (a.b = \'value\') OR (TRUE IS NULL AND (a.b = \'value\') IS NULL) THEN \'value\' END',
                'snowflake': 'DECODE(TRUE, a.b = \'value\', \'value\')',
              },
            },
          );
          this.validateAll(
            'SELECT BOOLAND(1, -2)',
            {
              read: {
                'snowflake': 'SELECT BOOLAND(1, -2)',
              },
              write: {
                'snowflake': 'SELECT BOOLAND(1, -2)',
                'duckdb': 'SELECT ((ROUND(1, 0)) AND (ROUND(-2, 0)))',
              },
            },
          );
          this.validateAll(
            'SELECT BOOLOR(1, 0)',
            {
              write: {
                'snowflake': 'SELECT BOOLOR(1, 0)',
                'duckdb': 'SELECT ((ROUND(1, 0)) OR (ROUND(0, 0)))',
              },
            },
          );
          this.validateAll(
            'SELECT BOOLXOR(2, 0.3)',
            {
              read: {
                'snowflake': 'SELECT BOOLXOR(2, 0.3)',
              },
              write: {
                'snowflake': 'SELECT BOOLXOR(2, 0.3)',
                'duckdb': 'SELECT (ROUND(2, 0) AND (NOT ROUND(0.3, 0))) OR ((NOT ROUND(2, 0)) AND ROUND(0.3, 0))',
              },
            },
          );
          this.validateAll(
            'SELECT APPROX_PERCENTILE(a, 0.5) FROM t',
            {
              read: {
                'trino': 'SELECT APPROX_PERCENTILE(a, 1, 0.5, 0.001) FROM t',
                'presto': 'SELECT APPROX_PERCENTILE(a, 1, 0.5, 0.001) FROM t',
              },
              write: {
                'trino': 'SELECT APPROX_PERCENTILE(a, 0.5) FROM t',
                'presto': 'SELECT APPROX_PERCENTILE(a, 0.5) FROM t',
                'snowflake': 'SELECT APPROX_PERCENTILE(a, 0.5) FROM t',
              },
            },
          );

          this.validateAll(
            'SELECT OBJECT_INSERT(OBJECT_INSERT(OBJECT_INSERT(OBJECT_CONSTRUCT(\'key5\', \'value5\'), \'key1\', 5), \'key2\', 2.2), \'key3\', \'value3\')',
            {
              write: {
                'snowflake': 'SELECT OBJECT_INSERT(OBJECT_INSERT(OBJECT_INSERT(OBJECT_CONSTRUCT(\'key5\', \'value5\'), \'key1\', 5), \'key2\', 2.2), \'key3\', \'value3\')',
                'duckdb': 'SELECT STRUCT_INSERT(STRUCT_INSERT(STRUCT_INSERT({\'key5\': \'value5\'}, key1 := 5), key2 := 2.2), key3 := \'value3\')',
              },
            },
          );

          this.validateAll(
            'SELECT OBJECT_INSERT(OBJECT_INSERT(OBJECT_INSERT(OBJECT_CONSTRUCT(), \'key1\', 5), \'key2\', 2.2), \'key3\', \'value3\')',
            {
              write: {
                'snowflake': 'SELECT OBJECT_INSERT(OBJECT_INSERT(OBJECT_INSERT(OBJECT_CONSTRUCT(), \'key1\', 5), \'key2\', 2.2), \'key3\', \'value3\')',
                'duckdb': 'SELECT STRUCT_INSERT(STRUCT_INSERT(STRUCT_PACK(key1 := 5), key2 := 2.2), key3 := \'value3\')',
              },
            },
          );

          this.validateIdentity(
            'SELECT ARRAY_CONSTRUCT(\'foo\')::VARIANT[0]',
            'SELECT CAST([\'foo\'] AS VARIANT)[0]',
          );

          this.validateAll(
            'SELECT CONVERT_TIMEZONE(\'America/New_York\', \'2024-08-06 09:10:00.000\')',
            {
              write: {
                'snowflake': 'SELECT CONVERT_TIMEZONE(\'America/New_York\', \'2024-08-06 09:10:00.000\')',
                'spark': 'SELECT CONVERT_TIMEZONE(\'America/New_York\', \'2024-08-06 09:10:00.000\')',
                'databricks': 'SELECT CONVERT_TIMEZONE(\'America/New_York\', \'2024-08-06 09:10:00.000\')',
                'redshift': 'SELECT CONVERT_TIMEZONE(\'America/New_York\', \'2024-08-06 09:10:00.000\')',
              },
            },
          );

          this.validateAll(
            'SELECT CONVERT_TIMEZONE(\'America/Los_Angeles\', \'America/New_York\', \'2024-08-06 09:10:00.000\')',
            {
              write: {
                'snowflake': 'SELECT CONVERT_TIMEZONE(\'America/Los_Angeles\', \'America/New_York\', \'2024-08-06 09:10:00.000\')',
                'spark': 'SELECT CONVERT_TIMEZONE(\'America/Los_Angeles\', \'America/New_York\', \'2024-08-06 09:10:00.000\')',
                'databricks': 'SELECT CONVERT_TIMEZONE(\'America/Los_Angeles\', \'America/New_York\', \'2024-08-06 09:10:00.000\')',
                'redshift': 'SELECT CONVERT_TIMEZONE(\'America/Los_Angeles\', \'America/New_York\', \'2024-08-06 09:10:00.000\')',
                'mysql': 'SELECT CONVERT_TZ(\'2024-08-06 09:10:00.000\', \'America/Los_Angeles\', \'America/New_York\')',
                'duckdb': 'SELECT CAST(\'2024-08-06 09:10:00.000\' AS TIMESTAMP) AT TIME ZONE \'America/Los_Angeles\' AT TIME ZONE \'America/New_York\'',
              },
            },
          );

          this.validateIdentity(
            'SELECT UUID_STRING(), UUID_STRING(\'fe971b24-9572-4005-b22f-351e9c09274d\', \'foo\')',
          );

          this.validateAll(
            'UUID_STRING(\'fe971b24-9572-4005-b22f-351e9c09274d\', \'foo\')',
            {
              read: {
                'snowflake': 'UUID_STRING(\'fe971b24-9572-4005-b22f-351e9c09274d\', \'foo\')',
              },
              write: {
                'hive': 'UUID()',
                'spark2': 'UUID()',
                'spark': 'UUID()',
                'databricks': 'UUID()',
                'duckdb': 'UUID()',
                'presto': 'UUID()',
                'trino': 'UUID()',
                'postgres': 'GEN_RANDOM_UUID()',
                'bigquery': 'GENERATE_UUID()',
              },
            },
          );
          this.validateIdentity('TRY_TO_TIMESTAMP(foo)').assertIs(AnonymousExpr);
          this.validateIdentity('TRY_TO_TIMESTAMP(\'12345\')').assertIs(AnonymousExpr);
          this.validateAll(
            'SELECT TRY_TO_TIMESTAMP(\'2024-01-15 12:30:00.000\')',
            {
              write: {
                'snowflake': 'SELECT TRY_CAST(\'2024-01-15 12:30:00.000\' AS TIMESTAMP)',
                'duckdb': 'SELECT TRY_CAST(\'2024-01-15 12:30:00.000\' AS TIMESTAMP)',
              },
            },
          );
          this.validateAll(
            'SELECT TRY_TO_TIMESTAMP(\'invalid\')',
            {
              write: {
                'snowflake': 'SELECT TRY_CAST(\'invalid\' AS TIMESTAMP)',
                'duckdb': 'SELECT TRY_CAST(\'invalid\' AS TIMESTAMP)',
              },
            },
          );
          this.validateAll(
            'SELECT TRY_TO_TIMESTAMP(\'04/05/2013 01:02:03\', \'mm/DD/yyyy hh24:mi:ss\')',
            {
              write: {
                'snowflake': 'SELECT TRY_TO_TIMESTAMP(\'04/05/2013 01:02:03\', \'mm/DD/yyyy hh24:mi:ss\')',
                'duckdb': 'SELECT CAST(TRY_STRPTIME(\'04/05/2013 01:02:03\', \'%m/%d/%Y %H:%M:%S\') AS TIMESTAMP)',
              },
            },
          );

          this.validateAll(
            'EDITDISTANCE(col1, col2)',
            {
              write: {
                'duckdb': 'LEVENSHTEIN(col1, col2)',
                'snowflake': 'EDITDISTANCE(col1, col2)',
              },
            },
          );
          this.validateAll(
            'EDITDISTANCE(col1, col2, 3)',
            {
              write: {
                'bigquery': 'EDIT_DISTANCE(col1, col2, max_distance => 3)',
                'duckdb': 'CASE WHEN LEVENSHTEIN(col1, col2) IS NULL OR 3 IS NULL THEN NULL ELSE LEAST(LEVENSHTEIN(col1, col2), 3) END',
                'postgres': 'LEVENSHTEIN_LESS_EQUAL(col1, col2, 3)',
                'snowflake': 'EDITDISTANCE(col1, col2, 3)',
              },
            },
          );

          this.validateIdentity('MINHASH(100, col1)');
          this.validateIdentity('MINHASH(100, col1, col2)');
          this.validateAll(
            'MINHASH(4, col1)',
            {
              write: {
                'duckdb': '(SELECT JSON_OBJECT(\'state\', LIST(min_h ORDER BY seed NULLS FIRST), \'type\', \'minhash\', \'version\', 1) FROM (SELECT seed, LIST_MIN(LIST_TRANSFORM(vals, __v -> HASH(CAST(__v AS TEXT) || CAST(seed AS TEXT)))) AS min_h FROM (SELECT LIST(col1) AS vals), RANGE(0, 4) AS t(seed)))',
                'snowflake': 'MINHASH(4, col1)',
              },
            },
          );

          this.validateIdentity('MINHASH_COMBINE(sig_col)');
          this.validateAll(
            'MINHASH_COMBINE(sig_col)',
            {
              write: {
                'duckdb': '(SELECT JSON_OBJECT(\'state\', LIST(min_h ORDER BY idx NULLS FIRST), \'type\', \'minhash\', \'version\', 1) FROM (SELECT pos AS idx, MIN(val) AS min_h FROM UNNEST(LIST(sig_col)) AS _(sig) JOIN UNNEST(CAST(sig -> \'$.state\' AS UBIGINT[])) WITH ORDINALITY AS t(val, pos) ON TRUE GROUP BY pos))',
                'snowflake': 'MINHASH_COMBINE(sig_col)',
              },
            },
          );

          this.validateIdentity('APPROXIMATE_SIMILARITY(sig_col)');
          this.validateAll(
            'APPROXIMATE_SIMILARITY(sig_col)',
            {
              write: {
                'duckdb': '(SELECT CAST(SUM(CASE WHEN num_distinct = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) FROM (SELECT pos, COUNT(DISTINCT h) AS num_distinct FROM (SELECT h, pos FROM UNNEST(LIST(sig_col)) AS _(sig) JOIN UNNEST(CAST(sig -> \'$.state\' AS UBIGINT[])) WITH ORDINALITY AS s(h, pos) ON TRUE) GROUP BY pos))',
                'snowflake': 'APPROXIMATE_SIMILARITY(sig_col)',
              },
            },
          );

          this.validateIdentity('SELECT BITNOT(a)');
          this.validateIdentity('SELECT BIT_NOT(a)', 'SELECT BITNOT(a)');
          this.validateAll(
            'SELECT BITNOT(-1)',
            {
              write: {
                'duckdb': 'SELECT ~(-1)',
                'snowflake': 'SELECT BITNOT(-1)',
              },
            },
          );
          this.validateIdentity('SELECT BITAND(a, b)');
          this.validateIdentity('SELECT BITAND(a, b, \'LEFT\')');
          this.validateIdentity('SELECT BIT_AND(a, b)', 'SELECT BITAND(a, b)');
          this.validateIdentity('SELECT BIT_AND(a, b, \'LEFT\')', 'SELECT BITAND(a, b, \'LEFT\')');
          this.validateIdentity('SELECT BITOR(a, b)');
          this.validateIdentity('SELECT BITOR(a, b, \'LEFT\')');
          this.validateIdentity('SELECT BIT_OR(a, b)', 'SELECT BITOR(a, b)');
          this.validateIdentity('SELECT BIT_OR(a, b, \'RIGHT\')', 'SELECT BITOR(a, b, \'RIGHT\')');
          this.validateIdentity('SELECT BITXOR(a, b)');
          this.validateIdentity('SELECT BITXOR(a, b, \'LEFT\')');
          this.validateIdentity('SELECT BIT_XOR(a, b)', 'SELECT BITXOR(a, b)');
          this.validateIdentity('SELECT BIT_XOR(a, b, \'LEFT\')', 'SELECT BITXOR(a, b, \'LEFT\')');

          // duckdb has an order of operations precedence issue with bitshift and bitwise operators
          this.validateAll(
            'SELECT BITOR(BITSHIFTLEFT(5, 16), BITSHIFTLEFT(3, 8))',
            {
              write: {
                'duckdb': 'SELECT (CAST(5 AS INT128) << 16) | (CAST(3 AS INT128) << 8)',
              },
            },
          );
          this.validateAll(
            'SELECT BITAND(BITSHIFTLEFT(255, 4), BITSHIFTLEFT(15, 2))',
            {
              write: {
                'snowflake': 'SELECT BITAND(BITSHIFTLEFT(255, 4), BITSHIFTLEFT(15, 2))',
                'duckdb': 'SELECT (CAST(255 AS INT128) << 4) & (CAST(15 AS INT128) << 2)',
              },
            },
          );
          this.validateAll(
            'SELECT BITSHIFTLEFT(255, 4)',
            {
              write: {
                'snowflake': 'SELECT BITSHIFTLEFT(255, 4)',
                'duckdb': 'SELECT CAST(255 AS INT128) << 4',
              },
            },
          );
          this.validateAll(
            'SELECT BITSHIFTRIGHT(255, 4)',
            {
              write: {
                'snowflake': 'SELECT BITSHIFTRIGHT(255, 4)',
                'duckdb': 'SELECT CAST(255 AS INT128) >> 4',
              },
            },
          );
          this.validateAll(
            'SELECT BITSHIFTLEFT(X\'002A\'::BINARY, 1)',
            {
              write: {
                'snowflake': 'SELECT BITSHIFTLEFT(CAST(x\'002A\' AS BINARY), 1)',
                'duckdb': 'SELECT CAST(CAST(CAST(UNHEX(\'002A\') AS BLOB) AS BIT) << 1 AS BLOB)',
              },
            },
          );
          this.validateAll(
            'SELECT BITSHIFTRIGHT(X\'002A\'::BINARY, 1)',
            {
              write: {
                'snowflake': 'SELECT BITSHIFTRIGHT(CAST(x\'002A\' AS BINARY), 1)',
                'duckdb': 'SELECT CAST(CAST(CAST(UNHEX(\'002A\') AS BLOB) AS BIT) >> 1 AS BLOB)',
              },
            },
          );

          this.validateAll(
            'OCTET_LENGTH(\'A\')',
            {
              read: {
                'bigquery': 'BYTE_LENGTH(\'A\')',
                'snowflake': 'OCTET_LENGTH(\'A\')',
              },
            },
          );

          this.validateIdentity('CREATE TABLE t (id INT PRIMARY KEY AUTOINCREMENT)');

          this.validateAll(
            'SELECT HEX_DECODE_BINARY(\'65\')',
            {
              write: {
                'bigquery': 'SELECT FROM_HEX(\'65\')',
                'duckdb': 'SELECT UNHEX(\'65\')',
                'snowflake': 'SELECT HEX_DECODE_BINARY(\'65\')',
              },
            },
          );

          this.validateAll(
            'DAYOFWEEKISO(foo)',
            {
              read: {
                'snowflake': 'DAYOFWEEKISO(foo)',
                'presto': 'DAY_OF_WEEK(foo)',
                'trino': 'DAY_OF_WEEK(foo)',
              },
              write: {
                'duckdb': 'ISODOW(foo)',
              },
            },
          );

          this.validateAll(
            'DAYOFWEEKISO(foo)',
            {
              read: {
                'presto': 'DOW(foo)',
                'trino': 'DOW(foo)',
              },
            },
          );

          this.validateAll(
            'DAYOFYEAR(foo)',
            {
              read: {
                'presto': 'DOY(foo)',
                'trino': 'DOY(foo)',
              },
              write: {
                'snowflake': 'DAYOFYEAR(foo)',
              },
            },
          );

          this.validateIdentity('TO_JSON(OBJECT_CONSTRUCT(\'name\', \'Alice\'))');

          expect(() => {
            parseOne(
              'SELECT id, PRIOR name AS parent_name, name FROM tree CONNECT BY NOCYCLE PRIOR id = parent_id',
              {
                dialect: 'snowflake',
              },
            );
          }).toThrow(ParseError);
          this.validateAll(
            'SELECT CAST(1 AS DOUBLE), CAST(1 AS DOUBLE)',
            {
              read: {
                'bigquery': 'SELECT CAST(1 AS BIGDECIMAL), CAST(1 AS BIGNUMERIC)',
              },
              write: {
                'snowflake': 'SELECT CAST(1 AS DOUBLE), CAST(1 AS DOUBLE)',
              },
            },
          );

          this.validateAll(
            'SELECT DATE_PART(WEEKISO, CAST(\'2013-12-25\' AS DATE))',
            {
              read: {
                'bigquery': 'SELECT EXTRACT(ISOWEEK FROM CAST(\'2013-12-25\' AS DATE))',
                'snowflake': 'SELECT DATE_PART(WEEKISO, CAST(\'2013-12-25\' AS DATE))',
              },
              write: {
                'duckdb': 'SELECT CAST(STRFTIME(CAST(\'2013-12-25\' AS DATE), \'%V\') AS INT)',
              },
            },
          );
          // DATE_PART/EXTRACT with specifiers not supported in DuckDB
          this.validateAll(
            'SELECT DATE_PART(YEAROFWEEK, CAST(\'2026-01-06\' AS DATE))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(YEAROFWEEK, CAST(\'2026-01-06\' AS DATE))',
                'duckdb': 'SELECT CAST(STRFTIME(CAST(\'2026-01-06\' AS DATE), \'%G\') AS INT)',
              },
            },
          );
          this.validateAll(
            'SELECT DATE_PART(YEAROFWEEKISO, CAST(\'2026-01-06\' AS DATE))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(YEAROFWEEKISO, CAST(\'2026-01-06\' AS DATE))',
                'duckdb': 'SELECT CAST(STRFTIME(CAST(\'2026-01-06\' AS DATE), \'%G\') AS INT)',
              },
            },
          );
          this.validateAll(
            'SELECT DATE_PART(NANOSECOND, CAST(\'2026-01-06 11:45:00.123456789\' AS TIMESTAMPNTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(NANOSECOND, CAST(\'2026-01-06 11:45:00.123456789\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT CAST(STRFTIME(CAST(CAST(\'2026-01-06 11:45:00.123456789\' AS TIMESTAMP) AS TIMESTAMP_NS), \'%n\') AS BIGINT)',
              },
            },
          );
          // TIMESTAMP_NTZ tests - using NTZ for consistent behavior across timezones
          this.validateAll(
            'SELECT EXTRACT(YEAR FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(YEAR, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT EXTRACT(YEAR FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(QUARTER FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(QUARTER, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT EXTRACT(QUARTER FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(MONTH FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(MONTH, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT EXTRACT(MONTH FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(WEEK FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(WEEK, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT EXTRACT(WEEK FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(WEEKISO FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(WEEKISO, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT CAST(STRFTIME(CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP), \'%V\') AS INT)',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(DAY FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(DAY, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT EXTRACT(DAY FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(DAYOFMONTH FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(DAY, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT EXTRACT(DAY FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(DAYOFWEEK FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(DAYOFWEEK, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT EXTRACT(DAYOFWEEK FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(DAYOFWEEKISO FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(DAYOFWEEKISO, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT EXTRACT(ISODOW FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(DAYOFYEAR FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(DAYOFYEAR, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT EXTRACT(DAYOFYEAR FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(YEAROFWEEK FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(YEAROFWEEK, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT CAST(STRFTIME(CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP), \'%G\') AS INT)',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(YEAROFWEEKISO FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(YEAROFWEEKISO, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT CAST(STRFTIME(CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP), \'%G\') AS INT)',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(HOUR FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(HOUR, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT EXTRACT(HOUR FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(MINUTE FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(MINUTE, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT EXTRACT(MINUTE FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(SECOND FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(SECOND, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT EXTRACT(SECOND FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(NANOSECOND FROM CAST(\'2026-01-06 11:45:00.123456789\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(NANOSECOND, CAST(\'2026-01-06 11:45:00.123456789\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT CAST(STRFTIME(CAST(CAST(\'2026-01-06 11:45:00.123456789\' AS TIMESTAMP) AS TIMESTAMP_NS), \'%n\') AS BIGINT)',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(EPOCH_SECOND FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(EPOCH_SECOND, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT CAST(EPOCH(CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP)) AS BIGINT)',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(EPOCH_MILLISECOND FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(EPOCH_MILLISECOND, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT EPOCH_MS(CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(EPOCH_MICROSECOND FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(EPOCH_MICROSECOND, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT EPOCH_US(CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(EPOCH_NANOSECOND FROM CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP_NTZ))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(EPOCH_NANOSECOND, CAST(\'2026-01-06 11:45:00\' AS TIMESTAMPNTZ))',
                'duckdb': 'SELECT EPOCH_NS(CAST(\'2026-01-06 11:45:00\' AS TIMESTAMP))',
              },
            },
          );
          // EXTRACT from DATE - exhaustive tests
          this.validateAll(
            'SELECT EXTRACT(YEAR FROM CAST(\'2026-01-06\' AS DATE))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(YEAR, CAST(\'2026-01-06\' AS DATE))',
                'duckdb': 'SELECT EXTRACT(YEAR FROM CAST(\'2026-01-06\' AS DATE))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(QUARTER FROM CAST(\'2026-01-06\' AS DATE))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(QUARTER, CAST(\'2026-01-06\' AS DATE))',
                'duckdb': 'SELECT EXTRACT(QUARTER FROM CAST(\'2026-01-06\' AS DATE))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(MONTH FROM CAST(\'2026-01-06\' AS DATE))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(MONTH, CAST(\'2026-01-06\' AS DATE))',
                'duckdb': 'SELECT EXTRACT(MONTH FROM CAST(\'2026-01-06\' AS DATE))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(WEEK FROM CAST(\'2026-01-06\' AS DATE))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(WEEK, CAST(\'2026-01-06\' AS DATE))',
                'duckdb': 'SELECT EXTRACT(WEEK FROM CAST(\'2026-01-06\' AS DATE))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(WEEKISO FROM CAST(\'2026-01-06\' AS DATE))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(WEEKISO, CAST(\'2026-01-06\' AS DATE))',
                'duckdb': 'SELECT CAST(STRFTIME(CAST(\'2026-01-06\' AS DATE), \'%V\') AS INT)',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(DAY FROM CAST(\'2026-01-06\' AS DATE))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(DAY, CAST(\'2026-01-06\' AS DATE))',
                'duckdb': 'SELECT EXTRACT(DAY FROM CAST(\'2026-01-06\' AS DATE))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(DAYOFMONTH FROM CAST(\'2026-01-06\' AS DATE))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(DAY, CAST(\'2026-01-06\' AS DATE))',
                'duckdb': 'SELECT EXTRACT(DAY FROM CAST(\'2026-01-06\' AS DATE))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(DAYOFWEEK FROM CAST(\'2026-01-06\' AS DATE))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(DAYOFWEEK, CAST(\'2026-01-06\' AS DATE))',
                'duckdb': 'SELECT EXTRACT(DAYOFWEEK FROM CAST(\'2026-01-06\' AS DATE))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(DAYOFWEEKISO FROM CAST(\'2026-01-06\' AS DATE))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(DAYOFWEEKISO, CAST(\'2026-01-06\' AS DATE))',
                'duckdb': 'SELECT EXTRACT(ISODOW FROM CAST(\'2026-01-06\' AS DATE))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(DAYOFYEAR FROM CAST(\'2026-01-06\' AS DATE))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(DAYOFYEAR, CAST(\'2026-01-06\' AS DATE))',
                'duckdb': 'SELECT EXTRACT(DAYOFYEAR FROM CAST(\'2026-01-06\' AS DATE))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(YEAROFWEEK FROM CAST(\'2026-01-06\' AS DATE))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(YEAROFWEEK, CAST(\'2026-01-06\' AS DATE))',
                'duckdb': 'SELECT CAST(STRFTIME(CAST(\'2026-01-06\' AS DATE), \'%G\') AS INT)',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(YEAROFWEEKISO FROM CAST(\'2026-01-06\' AS DATE))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(YEAROFWEEKISO, CAST(\'2026-01-06\' AS DATE))',
                'duckdb': 'SELECT CAST(STRFTIME(CAST(\'2026-01-06\' AS DATE), \'%G\') AS INT)',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(HOUR FROM CAST(\'11:45:00.123456789\' AS TIME))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(HOUR, CAST(\'11:45:00.123456789\' AS TIME))',
                'duckdb': 'SELECT EXTRACT(HOUR FROM CAST(\'11:45:00.123456789\' AS TIME))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(MINUTE FROM CAST(\'11:45:00.123456789\' AS TIME))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(MINUTE, CAST(\'11:45:00.123456789\' AS TIME))',
                'duckdb': 'SELECT EXTRACT(MINUTE FROM CAST(\'11:45:00.123456789\' AS TIME))',
              },
            },
          );
          this.validateAll(
            'SELECT EXTRACT(SECOND FROM CAST(\'11:45:00.123456789\' AS TIME))',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(SECOND, CAST(\'11:45:00.123456789\' AS TIME))',
                'duckdb': 'SELECT EXTRACT(SECOND FROM CAST(\'11:45:00.123456789\' AS TIME))',
              },
            },
          );

          this.validateAll(
            'SELECT ST_MAKEPOINT(10, 20)',
            {
              write: {
                'snowflake': 'SELECT ST_MAKEPOINT(10, 20)',
                'starrocks': 'SELECT ST_POINT(10, 20)',
              },
            },
          );

          this.validateAll(
            'LAST_DAY(CAST(\'2023-04-15\' AS DATE))',
            {
              write: {
                'snowflake': 'LAST_DAY(CAST(\'2023-04-15\' AS DATE))',
                'duckdb': 'LAST_DAY(CAST(\'2023-04-15\' AS DATE))',
              },
            },
          );

          this.validateAll(
            'LAST_DAY(CAST(\'2023-04-15\' AS DATE), MONTH)',
            {
              write: {
                'snowflake': 'LAST_DAY(CAST(\'2023-04-15\' AS DATE), MONTH)',
                'duckdb': 'LAST_DAY(CAST(\'2023-04-15\' AS DATE))',
              },
            },
          );

          this.validateAll(
            'LAST_DAY(CAST(\'2024-06-15\' AS DATE), YEAR)',
            {
              write: {
                'snowflake': 'LAST_DAY(CAST(\'2024-06-15\' AS DATE), YEAR)',
                'duckdb': 'MAKE_DATE(EXTRACT(YEAR FROM CAST(\'2024-06-15\' AS DATE)), 12, 31)',
              },
            },
          );

          this.validateAll(
            'LAST_DAY(CAST(\'2024-01-15\' AS DATE), QUARTER)',
            {
              write: {
                'snowflake': 'LAST_DAY(CAST(\'2024-01-15\' AS DATE), QUARTER)',
                'duckdb': 'LAST_DAY(MAKE_DATE(EXTRACT(YEAR FROM CAST(\'2024-01-15\' AS DATE)), EXTRACT(QUARTER FROM CAST(\'2024-01-15\' AS DATE)) * 3, 1))',
              },
            },
          );

          this.validateAll(
            'LAST_DAY(CAST(\'2025-12-15\' AS DATE), WEEK)',
            {
              write: {
                'snowflake': 'LAST_DAY(CAST(\'2025-12-15\' AS DATE), WEEK)',
                'duckdb': 'CAST(CAST(\'2025-12-15\' AS DATE) + INTERVAL ((7 - EXTRACT(DAYOFWEEK FROM CAST(\'2025-12-15\' AS DATE))) % 7) DAY AS DATE)',
              },
            },
          );

          this.validateAll(
            'SELECT ST_DISTANCE(a, b)',
            {
              write: {
                'snowflake': 'SELECT ST_DISTANCE(a, b)',
                'starrocks': 'SELECT ST_DISTANCE_SPHERE(ST_X(a), ST_Y(a), ST_X(b), ST_Y(b))',
              },
            },
          );

          this.validateAll(
            'SELECT DATE_PART(DAYOFWEEKISO, foo)',
            {
              read: {
                'snowflake': 'SELECT DATE_PART(WEEKDAY_ISO, foo)',
              },
              write: {
                'snowflake': 'SELECT DATE_PART(DAYOFWEEKISO, foo)',
                'duckdb': 'SELECT EXTRACT(ISODOW FROM foo)',
              },
            },
          );

          this.validateAll(
            'SELECT DATE_PART(DAYOFWEEK_ISO, foo)',
            {
              write: {
                'snowflake': 'SELECT DATE_PART(DAYOFWEEKISO, foo)',
                'duckdb': 'SELECT EXTRACT(ISODOW FROM foo)',
              },
            },
          );
          this.validateIdentity('ALTER TABLE foo ADD col1 VARCHAR(512), col2 VARCHAR(512)');
          this.validateIdentity(
            'ALTER TABLE foo ADD col1 VARCHAR NOT NULL TAG (key1=\'value_1\'), col2 VARCHAR NOT NULL TAG (key2=\'value_2\')',
          );
          this.validateIdentity('ALTER TABLE foo ADD IF NOT EXISTS col1 INT, col2 INT');
          this.validateIdentity('ALTER TABLE foo ADD IF NOT EXISTS col1 INT, IF NOT EXISTS col2 INT');
          this.validateIdentity('ALTER TABLE foo ADD col1 INT, IF NOT EXISTS col2 INT');
          this.validateIdentity('ALTER TABLE IF EXISTS foo ADD IF NOT EXISTS col1 INT');
          // ADD_MONTHS - Basic integer months with type preservation
          this.validateAll(
            'SELECT ADD_MONTHS(\'2023-01-31\', 1)',
            {
              write: {
                'duckdb': 'SELECT CASE WHEN LAST_DAY(CAST(\'2023-01-31\' AS TIMESTAMP)) = CAST(\'2023-01-31\' AS TIMESTAMP) THEN LAST_DAY(CAST(\'2023-01-31\' AS TIMESTAMP) + INTERVAL 1 MONTH) ELSE CAST(\'2023-01-31\' AS TIMESTAMP) + INTERVAL 1 MONTH END',
                'snowflake': 'SELECT ADD_MONTHS(\'2023-01-31\', 1)',
              },
            },
          );
          this.validateAll(
            'SELECT ADD_MONTHS(\'2023-01-31\'::date, 1)',
            {
              write: {
                'duckdb': 'SELECT CAST(CASE WHEN LAST_DAY(CAST(\'2023-01-31\' AS DATE)) = CAST(\'2023-01-31\' AS DATE) THEN LAST_DAY(CAST(\'2023-01-31\' AS DATE) + INTERVAL 1 MONTH) ELSE CAST(\'2023-01-31\' AS DATE) + INTERVAL 1 MONTH END AS DATE)',
                'snowflake': 'SELECT ADD_MONTHS(CAST(\'2023-01-31\' AS DATE), 1)',
              },
            },
          );
          this.validateAll(
            'SELECT ADD_MONTHS(\'2023-01-31\'::timestamptz, 1)',
            {
              write: {
                'duckdb': 'SELECT CAST(CASE WHEN LAST_DAY(CAST(\'2023-01-31\' AS TIMESTAMPTZ)) = CAST(\'2023-01-31\' AS TIMESTAMPTZ) THEN LAST_DAY(CAST(\'2023-01-31\' AS TIMESTAMPTZ) + INTERVAL 1 MONTH) ELSE CAST(\'2023-01-31\' AS TIMESTAMPTZ) + INTERVAL 1 MONTH END AS TIMESTAMPTZ)',
                'snowflake': 'SELECT ADD_MONTHS(CAST(\'2023-01-31\' AS TIMESTAMPTZ), 1)',
              },
            },
          );

          // ADD_MONTHS - Float month values (rounded to integer)
          this.validateAll(
            'SELECT ADD_MONTHS(\'2016-05-15\'::DATE, 2.7)',
            {
              write: {
                'duckdb': 'SELECT CAST(CASE WHEN LAST_DAY(CAST(\'2016-05-15\' AS DATE)) = CAST(\'2016-05-15\' AS DATE) THEN LAST_DAY(CAST(\'2016-05-15\' AS DATE) + TO_MONTHS(CAST(ROUND(2.7) AS INT))) ELSE CAST(\'2016-05-15\' AS DATE) + TO_MONTHS(CAST(ROUND(2.7) AS INT)) END AS DATE)',
                'snowflake': 'SELECT ADD_MONTHS(CAST(\'2016-05-15\' AS DATE), 2.7)',
              },
            },
          );
          this.validateAll(
            'SELECT ADD_MONTHS(\'2016-05-15\'::DATE, -2.3)',
            {
              write: {
                'duckdb': 'SELECT CAST(CASE WHEN LAST_DAY(CAST(\'2016-05-15\' AS DATE)) = CAST(\'2016-05-15\' AS DATE) THEN LAST_DAY(CAST(\'2016-05-15\' AS DATE) + TO_MONTHS(CAST(ROUND(-2.3) AS INT))) ELSE CAST(\'2016-05-15\' AS DATE) + TO_MONTHS(CAST(ROUND(-2.3) AS INT)) END AS DATE)',
                'snowflake': 'SELECT ADD_MONTHS(CAST(\'2016-05-15\' AS DATE), -2.3)',
              },
            },
          );

          // ADD_MONTHS - Decimal month values (rounded to integer)
          this.validateAll(
            'SELECT ADD_MONTHS(\'2016-05-15\'::DATE, 3.2::DECIMAL(10,2))',
            {
              write: {
                'duckdb': 'SELECT CAST(CASE WHEN LAST_DAY(CAST(\'2016-05-15\' AS DATE)) = CAST(\'2016-05-15\' AS DATE) THEN LAST_DAY(CAST(\'2016-05-15\' AS DATE) + TO_MONTHS(CAST(ROUND(CAST(3.2 AS DECIMAL(10, 2))) AS INT))) ELSE CAST(\'2016-05-15\' AS DATE) + TO_MONTHS(CAST(ROUND(CAST(3.2 AS DECIMAL(10, 2))) AS INT)) END AS DATE)',
                'snowflake': 'SELECT ADD_MONTHS(CAST(\'2016-05-15\' AS DATE), CAST(3.2 AS DECIMAL(10, 2)))',
              },
            },
          );

          // ADD_MONTHS - End-of-month preservation (Snowflake semantic)
          this.validateAll(
            'SELECT ADD_MONTHS(\'2016-02-29\'::DATE, 1)',
            {
              write: {
                'duckdb': 'SELECT CAST(CASE WHEN LAST_DAY(CAST(\'2016-02-29\' AS DATE)) = CAST(\'2016-02-29\' AS DATE) THEN LAST_DAY(CAST(\'2016-02-29\' AS DATE) + INTERVAL 1 MONTH) ELSE CAST(\'2016-02-29\' AS DATE) + INTERVAL 1 MONTH END AS DATE)',
                'snowflake': 'SELECT ADD_MONTHS(CAST(\'2016-02-29\' AS DATE), 1)',
              },
            },
          );
          this.validateAll(
            'SELECT ADD_MONTHS(\'2016-05-31\'::DATE, 1)',
            {
              write: {
                'duckdb': 'SELECT CAST(CASE WHEN LAST_DAY(CAST(\'2016-05-31\' AS DATE)) = CAST(\'2016-05-31\' AS DATE) THEN LAST_DAY(CAST(\'2016-05-31\' AS DATE) + INTERVAL 1 MONTH) ELSE CAST(\'2016-05-31\' AS DATE) + INTERVAL 1 MONTH END AS DATE)',
                'snowflake': 'SELECT ADD_MONTHS(CAST(\'2016-05-31\' AS DATE), 1)',
              },
            },
          );
          this.validateAll(
            'SELECT ADD_MONTHS(\'2016-05-31\'::DATE, -1)',
            {
              write: {
                'duckdb': 'SELECT CAST(CASE WHEN LAST_DAY(CAST(\'2016-05-31\' AS DATE)) = CAST(\'2016-05-31\' AS DATE) THEN LAST_DAY(CAST(\'2016-05-31\' AS DATE) + INTERVAL (-1) MONTH) ELSE CAST(\'2016-05-31\' AS DATE) + INTERVAL (-1) MONTH END AS DATE)',
                'snowflake': 'SELECT ADD_MONTHS(CAST(\'2016-05-31\' AS DATE), -1)',
              },
            },
          );

          // ADD_MONTHS - Mid-month dates (end-of-month logic should not trigger)
          this.validateAll(
            'SELECT ADD_MONTHS(\'2016-05-15\'::DATE, 1)',
            {
              write: {
                'duckdb': 'SELECT CAST(CASE WHEN LAST_DAY(CAST(\'2016-05-15\' AS DATE)) = CAST(\'2016-05-15\' AS DATE) THEN LAST_DAY(CAST(\'2016-05-15\' AS DATE) + INTERVAL 1 MONTH) ELSE CAST(\'2016-05-15\' AS DATE) + INTERVAL 1 MONTH END AS DATE)',
                'snowflake': 'SELECT ADD_MONTHS(CAST(\'2016-05-15\' AS DATE), 1)',
              },
            },
          );

          // ADD_MONTHS - NULL handling
          this.validateAll(
            'SELECT ADD_MONTHS(NULL::DATE, 2)',
            {
              write: {
                'duckdb': 'SELECT CAST(CASE WHEN LAST_DAY(CAST(NULL AS DATE)) = CAST(NULL AS DATE) THEN LAST_DAY(CAST(NULL AS DATE) + INTERVAL 2 MONTH) ELSE CAST(NULL AS DATE) + INTERVAL 2 MONTH END AS DATE)',
                'snowflake': 'SELECT ADD_MONTHS(CAST(NULL AS DATE), 2)',
              },
            },
          );
          this.validateAll(
            'SELECT ADD_MONTHS(\'2016-05-15\'::DATE, NULL)',
            {
              write: {
                'duckdb': 'SELECT CAST(CASE WHEN LAST_DAY(CAST(\'2016-05-15\' AS DATE)) = CAST(\'2016-05-15\' AS DATE) THEN LAST_DAY(CAST(\'2016-05-15\' AS DATE) + INTERVAL (NULL) MONTH) ELSE CAST(\'2016-05-15\' AS DATE) + INTERVAL (NULL) MONTH END AS DATE)',
                'snowflake': 'SELECT ADD_MONTHS(CAST(\'2016-05-15\' AS DATE), NULL)',
              },
            },
          );

          // ADD_MONTHS - Zero months
          this.validateAll(
            'SELECT ADD_MONTHS(\'2016-05-15\'::DATE, 0)',
            {
              write: {
                'duckdb': 'SELECT CAST(CASE WHEN LAST_DAY(CAST(\'2016-05-15\' AS DATE)) = CAST(\'2016-05-15\' AS DATE) THEN LAST_DAY(CAST(\'2016-05-15\' AS DATE) + INTERVAL 0 MONTH) ELSE CAST(\'2016-05-15\' AS DATE) + INTERVAL 0 MONTH END AS DATE)',
                'snowflake': 'SELECT ADD_MONTHS(CAST(\'2016-05-15\' AS DATE), 0)',
              },
            },
          );

          this.validateIdentity('SELECT HOUR(CAST(\'08:50:57\' AS TIME))');
          this.validateIdentity('SELECT MINUTE(CAST(\'08:50:57\' AS TIME))');
          this.validateIdentity('SELECT SECOND(CAST(\'08:50:57\' AS TIME))');
          this.validateIdentity('SELECT HOUR(CAST(\'2024-05-09 08:50:57\' AS TIMESTAMP))');
          this.validateIdentity('SELECT MONTHNAME(CAST(\'2024-05-09\' AS DATE))');
          this.validateAll(
            'SELECT DAYNAME(TO_DATE(\'2025-01-15\'))',
            {
              write: {
                'duckdb': 'SELECT STRFTIME(CAST(\'2025-01-15\' AS DATE), \'%a\')',
                'snowflake': 'SELECT DAYNAME(CAST(\'2025-01-15\' AS DATE))',
              },
            },
          );
          this.validateAll(
            'SELECT DAYNAME(TO_TIMESTAMP(\'2025-02-28 10:30:45\'))',
            {
              write: {
                'duckdb': 'SELECT STRFTIME(CAST(\'2025-02-28 10:30:45\' AS TIMESTAMP), \'%a\')',
                'snowflake': 'SELECT DAYNAME(CAST(\'2025-02-28 10:30:45\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT MONTHNAME(TO_DATE(\'2025-01-15\'))',
            {
              write: {
                'duckdb': 'SELECT STRFTIME(CAST(\'2025-01-15\' AS DATE), \'%b\')',
                'snowflake': 'SELECT MONTHNAME(CAST(\'2025-01-15\' AS DATE))',
              },
            },
          );
          this.validateAll(
            'SELECT MONTHNAME(TO_TIMESTAMP(\'2025-02-28 10:30:45\'))',
            {
              write: {
                'duckdb': 'SELECT STRFTIME(CAST(\'2025-02-28 10:30:45\' AS TIMESTAMP), \'%b\')',
                'snowflake': 'SELECT MONTHNAME(CAST(\'2025-02-28 10:30:45\' AS TIMESTAMP))',
              },
            },
          );
          this.validateIdentity('SELECT PREVIOUS_DAY(CAST(\'2024-05-09\' AS DATE), \'MONDAY\')');
          this.validateIdentity('SELECT TIME_FROM_PARTS(14, 30, 45)');
          this.validateIdentity('SELECT TIME_FROM_PARTS(14, 30, 45, 123)');

          this.validateIdentity(
            'SELECT MONTHS_BETWEEN(CAST(\'2019-03-15\' AS DATE), CAST(\'2019-02-15\' AS DATE))',
          );
          this.validateIdentity(
            'SELECT MONTHS_BETWEEN(CAST(\'2019-03-01 02:00:00\' AS TIMESTAMP), CAST(\'2019-02-15 01:00:00\' AS TIMESTAMP))',
          );

          this.validateIdentity(
            'SELECT TIME_SLICE(CAST(\'2024-05-09 08:50:57.891\' AS TIMESTAMP), 15, \'MINUTE\')',
          );
          this.validateIdentity('SELECT TIME_SLICE(CAST(\'2024-05-09\' AS DATE), 1, \'DAY\')');
          this.validateIdentity(
            'SELECT TIME_SLICE(CAST(\'2024-05-09 08:50:57.891\' AS TIMESTAMP), 1, \'HOUR\', \'start\')',
          );

          // TIME_SLICE transpilation to DuckDB
          this.validateAll(
            'SELECT TIME_SLICE(TIMESTAMP \'2024-03-15 14:37:42\', 1, \'HOUR\')',
            {
              write: {
                'snowflake': 'SELECT TIME_SLICE(CAST(\'2024-03-15 14:37:42\' AS TIMESTAMP), 1, \'HOUR\')',
                'duckdb': 'SELECT TIME_BUCKET(INTERVAL 1 HOUR, CAST(\'2024-03-15 14:37:42\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT TIME_SLICE(TIMESTAMP \'2024-03-15 14:37:42\', 1, \'HOUR\', \'END\')',
            {
              write: {
                'snowflake': 'SELECT TIME_SLICE(CAST(\'2024-03-15 14:37:42\' AS TIMESTAMP), 1, \'HOUR\', \'END\')',
                'duckdb': 'SELECT TIME_BUCKET(INTERVAL 1 HOUR, CAST(\'2024-03-15 14:37:42\' AS TIMESTAMP)) + INTERVAL 1 HOUR',
              },
            },
          );
          this.validateAll(
            'SELECT TIME_SLICE(DATE \'2024-03-15\', 1, \'DAY\')',
            {
              write: {
                'snowflake': 'SELECT TIME_SLICE(CAST(\'2024-03-15\' AS DATE), 1, \'DAY\')',
                'duckdb': 'SELECT TIME_BUCKET(INTERVAL 1 DAY, CAST(\'2024-03-15\' AS DATE))',
              },
            },
          );
          this.validateAll(
            'SELECT TIME_SLICE(DATE \'2024-03-15\', 1, \'DAY\', \'END\')',
            {
              write: {
                'snowflake': 'SELECT TIME_SLICE(CAST(\'2024-03-15\' AS DATE), 1, \'DAY\', \'END\')',
                'duckdb': 'SELECT CAST(TIME_BUCKET(INTERVAL 1 DAY, CAST(\'2024-03-15\' AS DATE)) + INTERVAL 1 DAY AS DATE)',
              },
            },
          );
          this.validateAll(
            'SELECT TIME_SLICE(TIMESTAMP \'2024-03-15 14:37:42\', 15, \'MINUTE\')',
            {
              write: {
                'snowflake': 'SELECT TIME_SLICE(CAST(\'2024-03-15 14:37:42\' AS TIMESTAMP), 15, \'MINUTE\')',
                'duckdb': 'SELECT TIME_BUCKET(INTERVAL 15 MINUTE, CAST(\'2024-03-15 14:37:42\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT TIME_SLICE(TIMESTAMP \'2024-03-15 14:37:42\', 1, \'QUARTER\')',
            {
              write: {
                'snowflake': 'SELECT TIME_SLICE(CAST(\'2024-03-15 14:37:42\' AS TIMESTAMP), 1, \'QUARTER\')',
                'duckdb': 'SELECT TIME_BUCKET(INTERVAL 1 QUARTER, CAST(\'2024-03-15 14:37:42\' AS TIMESTAMP))',
              },
            },
          );
          this.validateAll(
            'SELECT TIME_SLICE(DATE \'2024-03-15\', 1, \'WEEK\', \'END\')',
            {
              write: {
                'snowflake': 'SELECT TIME_SLICE(CAST(\'2024-03-15\' AS DATE), 1, \'WEEK\', \'END\')',
                'duckdb': 'SELECT CAST(TIME_BUCKET(INTERVAL 1 WEEK, CAST(\'2024-03-15\' AS DATE)) + INTERVAL 1 WEEK AS DATE)',
              },
            },
          );

          for (const join of [
            'FULL OUTER',
            'LEFT',
            'RIGHT',
            'LEFT OUTER',
            'RIGHT OUTER',
            'INNER',
          ]) {
            this.validateAll(
              `SELECT * FROM t1 ${join} JOIN t2`,
              {
                read: {
                  'snowflake': `SELECT * FROM t1 ${join} JOIN t2`,
                },
                write: {
                  'duckdb': 'SELECT * FROM t1, t2',
                },
              },
            );

            this.validateIdentity(
              'SELECT * EXCLUDE foo RENAME bar AS baz FROM tbl',
              'SELECT * EXCLUDE (foo) RENAME (bar AS baz) FROM tbl',
            );

            this.validateAll(
              'WITH foo AS (SELECT [1] AS arr_1) SELECT (SELECT unnested_arr FROM TABLE(FLATTEN(INPUT => arr_1)) AS _t0(seq, key, path, index, unnested_arr, this)) AS f FROM foo',
              {
                read: {
                  'bigquery': 'WITH foo AS (SELECT [1] AS arr_1) SELECT (SELECT unnested_arr FROM UNNEST(arr_1) AS unnested_arr) AS f FROM foo',
                },
              },
            );

            this.validateIdentity('SELECT LIKE(col, \'pattern\')', 'SELECT col LIKE \'pattern\'');
            this.validateIdentity('SELECT ILIKE(col, \'pattern\')', 'SELECT col ILIKE \'pattern\'');
            this.validateIdentity('SELECT LIKE(col, \'pattern\', \'\\\\\')', 'SELECT col LIKE \'pattern\' ESCAPE \'\\\\\'');
            this.validateIdentity('SELECT ILIKE(col, \'pattern\', \'\\\\\')', 'SELECT col ILIKE \'pattern\' ESCAPE \'\\\\\'');
            this.validateIdentity('SELECT LIKE(col, \'pattern\', \'!\')', 'SELECT col LIKE \'pattern\' ESCAPE \'!\'');
            this.validateIdentity('SELECT ILIKE(col, \'pattern\', \'!\')', 'SELECT col ILIKE \'pattern\' ESCAPE \'!\'');

            const expr = this.validateIdentity('SELECT BASE64_ENCODE(\'Hello World\')');

            annotated = annotateTypes(expr, {
              dialect: 'snowflake',
            });
            expect(annotated.sql({
              dialect: 'duckdb',
            })).toBe('SELECT TO_BASE64(ENCODE(\'Hello World\'))');
            this.validateAll(
              'SELECT BASE64_ENCODE(x)',
              {
                write: {
                  'duckdb': 'SELECT TO_BASE64(x)',
                  'snowflake': 'SELECT BASE64_ENCODE(x)',
                },
              },
            );
            this.validateAll(
              'SELECT BASE64_ENCODE(x, 76)',
              {
                write: {
                  'duckdb': 'SELECT RTRIM(REGEXP_REPLACE(TO_BASE64(x), \'(.{76})\', \'\\1\' || CHR(10), \'g\'), CHR(10))',
                  'snowflake': 'SELECT BASE64_ENCODE(x, 76)',
                },
              },
            );
            this.validateAll(
              'SELECT BASE64_ENCODE(x, 76, \'+/=\')',
              {
                write: {
                  'duckdb': 'SELECT RTRIM(REGEXP_REPLACE(TO_BASE64(x), \'(.{76})\', \'\\1\' || CHR(10), \'g\'), CHR(10))',
                  'snowflake': 'SELECT BASE64_ENCODE(x, 76, \'+/=\')',
                },
              },
            );

            this.validateAll(
              'SELECT BASE64_DECODE_STRING(\'U25vd2ZsYWtl\')',
              {
                write: {
                  'snowflake': 'SELECT BASE64_DECODE_STRING(\'U25vd2ZsYWtl\')',
                  'duckdb': 'SELECT DECODE(FROM_BASE64(\'U25vd2ZsYWtl\'))',
                },
              },
            );
            this.validateAll(
              'SELECT BASE64_DECODE_STRING(\'U25vd2ZsYWtl\', \'-_+\')',
              {
                write: {
                  'snowflake': 'SELECT BASE64_DECODE_STRING(\'U25vd2ZsYWtl\', \'-_+\')',
                  'duckdb': 'SELECT DECODE(FROM_BASE64(REPLACE(REPLACE(REPLACE(\'U25vd2ZsYWtl\', \'-\', \'+\'), \'_\', \'/\'), \'+\', \'=\')))',
                },
              },
            );
            this.validateAll(
              'SELECT BASE64_DECODE_BINARY(x)',
              {
                write: {
                  'snowflake': 'SELECT BASE64_DECODE_BINARY(x)',
                  'duckdb': 'SELECT FROM_BASE64(x)',
                },
              },
            );
            this.validateAll(
              'SELECT BASE64_DECODE_BINARY(x, \'-_+\')',
              {
                write: {
                  'snowflake': 'SELECT BASE64_DECODE_BINARY(x, \'-_+\')',
                  'duckdb': 'SELECT FROM_BASE64(REPLACE(REPLACE(REPLACE(x, \'-\', \'+\'), \'_\', \'/\'), \'+\', \'=\'))',
                },
              },
            );

            this.validateIdentity('SELECT TRY_HEX_DECODE_BINARY(\'48656C6C6F\')');

            this.validateIdentity('SELECT TRY_HEX_DECODE_STRING(\'48656C6C6F\')');

            this.validateAll(
              'SELECT ARRAY_CONTAINS(CAST(\'1\' AS VARIANT), [\'1\'])',
              {
                read: {
                  'presto': 'SELECT CONTAINS(ARRAY[\'1\'], \'1\')',
                  'snowflake': 'SELECT ARRAY_CONTAINS(CAST(\'1\' AS VARIANT), [\'1\'])',
                },
              },
            );
            this.validateAll(
              'SELECT ARRAY_CONTAINS(CAST(CAST(\'2020-10-10\' AS DATE) AS VARIANT), [CAST(\'2020-10-10\' AS DATE)])',
              {
                read: {
                  'presto': 'SELECT CONTAINS(ARRAY[DATE \'2020-10-10\'], DATE \'2020-10-10\')',
                  'snowflake': 'SELECT ARRAY_CONTAINS(CAST(CAST(\'2020-10-10\' AS DATE) AS VARIANT), [CAST(\'2020-10-10\' AS DATE)])',
                },
              },
            );
            this.validateIdentity('SELECT ARRAY_CONTAINS(1, [1])');

            this.validateAll(
              'SELECT x\'ABCD\'',
              {
                write: {
                  'snowflake': 'SELECT x\'ABCD\'',
                  'duckdb': 'SELECT UNHEX(\'ABCD\')',
                },
              },
            );

            this.validateAll(
              'SET a = 1',
              {
                write: {
                  'snowflake': 'SET a = 1',
                  'bigquery': 'SET a = 1',
                  'duckdb': 'SET VARIABLE a = 1',
                },
              },
            );
            this.validateAll(
              'CAST(6.43 AS FLOAT)',
              {
                write: {
                  'snowflake': 'CAST(6.43 AS DOUBLE)',
                  'duckdb': 'CAST(6.43 AS DOUBLE)',
                },
              },
            );
            this.validateAll(
              'UNIFORM(1, 10, RANDOM(5))',
              {
                write: {
                  'snowflake': 'UNIFORM(1, 10, RANDOM(5))',
                  'databricks': 'UNIFORM(1, 10, 5)',
                  'duckdb': 'CAST(FLOOR(1 + RANDOM() * (10 - 1 + 1)) AS BIGINT)',
                },
              },
            );
            this.validateAll(
              'UNIFORM(1, 10, RANDOM())',
              {
                write: {
                  'snowflake': 'UNIFORM(1, 10, RANDOM())',
                  'databricks': 'UNIFORM(1, 10)',
                  'duckdb': 'CAST(FLOOR(1 + RANDOM() * (10 - 1 + 1)) AS BIGINT)',
                },
              },
            );
            this.validateAll(
              'UNIFORM(1, 10, 5)',
              {
                write: {
                  'snowflake': 'UNIFORM(1, 10, 5)',
                  'databricks': 'UNIFORM(1, 10, 5)',
                  'duckdb': 'CAST(FLOOR(1 + (ABS(HASH(5)) % 1000000) / 1000000.0 * (10 - 1 + 1)) AS BIGINT)',
                },
              },
            );
            this.validateAll(
              'NORMAL(0, 1, 42)',
              {
                write: {
                  'snowflake': 'NORMAL(0, 1, 42)',
                  'duckdb': '0 + (1 * SQRT(-2 * LN(GREATEST((ABS(HASH(42)) % 1000000) / 1000000.0, 1e-10))) * COS(2 * PI() * (ABS(HASH(42 + 1)) % 1000000) / 1000000.0))',
                },
              },
            );
            this.validateAll(
              'NORMAL(10.5, 2.5, RANDOM())',
              {
                write: {
                  'snowflake': 'NORMAL(10.5, 2.5, RANDOM())',
                  'duckdb': '10.5 + (2.5 * SQRT(-2 * LN(GREATEST(RANDOM(), 1e-10))) * COS(2 * PI() * RANDOM()))',
                },
              },
            );
            this.validateAll(
              'NORMAL(10.5, 2.5, RANDOM(5))',
              {
                write: {
                  'snowflake': 'NORMAL(10.5, 2.5, RANDOM(5))',
                  'duckdb': '10.5 + (2.5 * SQRT(-2 * LN(GREATEST((ABS(HASH(5)) % 1000000) / 1000000.0, 1e-10))) * COS(2 * PI() * (ABS(HASH(5 + 1)) % 1000000) / 1000000.0))',
                },
              },
            );
            this.validateAll(
              'SYSDATE()',
              {
                write: {
                  'snowflake': 'SYSDATE()',
                  'duckdb': 'CURRENT_TIMESTAMP AT TIME ZONE \'UTC\'',
                },
              },
            );
            this.validateIdentity('SYSTIMESTAMP()', 'CURRENT_TIMESTAMP()');
            this.validateIdentity('GETDATE()', 'CURRENT_TIMESTAMP()');
            this.validateIdentity('LOCALTIMESTAMP', 'CURRENT_TIMESTAMP');
            this.validateIdentity('LOCALTIMESTAMP()', 'CURRENT_TIMESTAMP()');
            this.validateIdentity('LOCALTIMESTAMP(3)', 'CURRENT_TIMESTAMP(3)');

            this.validateAll(
              'SELECT CURRENT_TIME(4)',
              {
                write: {
                  'snowflake': 'SELECT CURRENT_TIME(4)',
                  'duckdb': 'SELECT LOCALTIME',
                },
              },
            );

            this.validateAll(
              'SELECT CURRENT_TIME',
              {
                write: {
                  'snowflake': 'SELECT CURRENT_TIME',
                  'duckdb': 'SELECT LOCALTIME',
                },
              },
            );
            this.validateAll(
              'SELECT DATE_FROM_PARTS(2026, 1, 100)',
              {
                write: {
                  'snowflake': 'SELECT DATE_FROM_PARTS(2026, 1, 100)',
                  'duckdb': 'SELECT CAST(MAKE_DATE(2026, 1, 1) + INTERVAL (1 - 1) MONTH + INTERVAL (100 - 1) DAY AS DATE)',
                },
              },
            );
            this.validateAll(
              'SELECT DATE_FROM_PARTS(2026, 14, 32)',
              {
                write: {
                  'snowflake': 'SELECT DATE_FROM_PARTS(2026, 14, 32)',
                  'duckdb': 'SELECT CAST(MAKE_DATE(2026, 1, 1) + INTERVAL (14 - 1) MONTH + INTERVAL (32 - 1) DAY AS DATE)',
                },
              },
            );
            this.validateAll(
              'SELECT DATE_FROM_PARTS(2026, 0, 0)',
              {
                write: {
                  'snowflake': 'SELECT DATE_FROM_PARTS(2026, 0, 0)',
                  'duckdb': 'SELECT CAST(MAKE_DATE(2026, 1, 1) + INTERVAL (0 - 1) MONTH + INTERVAL (0 - 1) DAY AS DATE)',
                },
              },
            );
            this.validateAll(
              'SELECT DATE_FROM_PARTS(2026, -14, -32)',
              {
                write: {
                  'snowflake': 'SELECT DATE_FROM_PARTS(2026, -14, -32)',
                  'duckdb': 'SELECT CAST(MAKE_DATE(2026, 1, 1) + INTERVAL (-14 - 1) MONTH + INTERVAL (-32 - 1) DAY AS DATE)',
                },
              },
            );
            this.validateAll(
              'SELECT DATE_FROM_PARTS(2024, 1, 60)',
              {
                write: {
                  'snowflake': 'SELECT DATE_FROM_PARTS(2024, 1, 60)',
                  'duckdb': 'SELECT CAST(MAKE_DATE(2024, 1, 1) + INTERVAL (1 - 1) MONTH + INTERVAL (60 - 1) DAY AS DATE)',
                },
              },
            );
            this.validateAll(
              'SELECT DATE_FROM_PARTS(2026, NULL, 100)',
              {
                write: {
                  'snowflake': 'SELECT DATE_FROM_PARTS(2026, NULL, 100)',
                  'duckdb': 'SELECT CAST(MAKE_DATE(2026, 1, 1) + INTERVAL (NULL - 1) MONTH + INTERVAL (100 - 1) DAY AS DATE)',
                },
              },
            );
            this.validateAll(
              'SELECT DATE_FROM_PARTS(2024 + 2, 1 + 2, 2 + 3)',
              {
                write: {
                  'snowflake': 'SELECT DATE_FROM_PARTS(2024 + 2, 1 + 2, 2 + 3)',
                  'duckdb': 'SELECT CAST(MAKE_DATE(2024 + 2, 1, 1) + INTERVAL ((1 + 2) - 1) MONTH + INTERVAL ((2 + 3) - 1) DAY AS DATE)',
                },
              },
            );

            this.validateAll(
              'SELECT DATE_FROM_PARTS(year, month, date)',
              {
                write: {
                  'snowflake': 'SELECT DATE_FROM_PARTS(year, month, date)',
                  'duckdb': 'SELECT CAST(MAKE_DATE(year, 1, 1) + INTERVAL (month - 1) MONTH + INTERVAL (date - 1) DAY AS DATE)',
                },
              },
            );

            this.validateAll(
              'EQUAL_NULL(a, b)',
              {
                write: {
                  'snowflake': 'EQUAL_NULL(a, b)',
                  'duckdb': 'a IS NOT DISTINCT FROM b',
                },
              },
            );

          }
        }
      }
    }
  }

  testNullTreatment () {
    this.validateAll(
      'SELECT FIRST_VALUE(TABLE1.COLUMN1) OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1',
      {
        write: {
          'snowflake': 'SELECT FIRST_VALUE(TABLE1.COLUMN1) OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1',
        },
      },
    );
    this.validateAll(
      'SELECT FIRST_VALUE(TABLE1.COLUMN1 RESPECT NULLS) OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1',
      {
        write: {
          'snowflake': 'SELECT FIRST_VALUE(TABLE1.COLUMN1) RESPECT NULLS OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1',
        },
      },
    );
    this.validateAll(
      'SELECT FIRST_VALUE(TABLE1.COLUMN1) RESPECT NULLS OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1',
      {
        write: {
          'snowflake': 'SELECT FIRST_VALUE(TABLE1.COLUMN1) RESPECT NULLS OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1',
        },
      },
    );
    this.validateAll(
      'SELECT FIRST_VALUE(TABLE1.COLUMN1 IGNORE NULLS) OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1',
      {
        write: {
          'snowflake': 'SELECT FIRST_VALUE(TABLE1.COLUMN1) IGNORE NULLS OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1',
        },
      },
    );
    this.validateAll(
      'SELECT FIRST_VALUE(TABLE1.COLUMN1) IGNORE NULLS OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1',
      {
        write: {
          'snowflake': 'SELECT FIRST_VALUE(TABLE1.COLUMN1) IGNORE NULLS OVER (PARTITION BY RANDOM_COLUMN1, RANDOM_COLUMN2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS MY_ALIAS FROM TABLE1',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM foo WHERE \'str\' IN (SELECT value FROM TABLE(FLATTEN(INPUT => vals)) AS _u(seq, key, path, index, value, this))',
      {
        read: {
          'bigquery': 'SELECT * FROM foo WHERE \'str\' IN UNNEST(vals)',
        },
        write: {
          'snowflake': 'SELECT * FROM foo WHERE \'str\' IN (SELECT value FROM TABLE(FLATTEN(INPUT => vals)) AS _u(seq, key, path, index, value, this))',
        },
      },
    );

  }

  testStagedFiles () {
    // Ensure we don't treat staged file paths as identifiers (i.e. they're not normalized)
    const stagedFile = parseOne('SELECT * FROM @foo', {
      read: 'snowflake',
    });

    expect(normalizeIdentifiers(stagedFile, {
      dialect: 'snowflake',
    }).sql({
      dialect: 'snowflake',
    })).toBe(stagedFile.sql({
      dialect: 'snowflake',
    }));

    this.validateIdentity('SELECT * FROM @"mystage"');
    this.validateIdentity('SELECT * FROM @"myschema"."mystage"/file.gz');
    this.validateIdentity('SELECT * FROM @"my_DB"."schEMA1".mystage/file.gz');
    this.validateIdentity('SELECT metadata$filename FROM @s1/');
    this.validateIdentity('SELECT * FROM @~');
    this.validateIdentity('SELECT * FROM @~/some/path/to/file.csv');
    this.validateIdentity('SELECT * FROM @mystage');
    this.validateIdentity('SELECT * FROM \'@mystage\'');
    this.validateIdentity('SELECT * FROM @namespace.mystage/path/to/file.json.gz');
    this.validateIdentity('SELECT * FROM @namespace.%table_name/path/to/file.json.gz');
    this.validateIdentity('SELECT * FROM \'@external/location\' (FILE_FORMAT => \'path.to.csv\')');
    this.validateIdentity('PUT file:///dir/tmp.csv @%table', undefined, {
      checkCommandWarning: true,
    });
    this.validateIdentity('SELECT * FROM (SELECT a FROM @foo)');
    this.validateIdentity(
      'SELECT * FROM (SELECT * FROM \'@external/location\' (FILE_FORMAT => \'path.to.csv\'))',
    );
    this.validateIdentity(
      'SELECT * FROM @foo/bar (FILE_FORMAT => ds_sandbox.test.my_csv_format, PATTERN => \'test\') AS bla',
    );
    this.validateIdentity(
      'SELECT t.$1, t.$2 FROM @mystage1 (FILE_FORMAT => \'myformat\', PATTERN => \'.*data.*[.]csv.gz\') AS t',
    );
    this.validateIdentity(
      'SELECT parse_json($1):a.b FROM @mystage2/data1.json.gz',
      'SELECT GET_PATH(PARSE_JSON($1), \'a.b\') FROM @mystage2/data1.json.gz',
    );
    this.validateIdentity(
      'SELECT * FROM @mystage t (c1)',
      'SELECT * FROM @mystage AS t(c1)',
    );
    this.validateIdentity(
      'SELECT * FROM @foo/bar (PATTERN => \'test\', FILE_FORMAT => ds_sandbox.test.my_csv_format) AS bla',
      'SELECT * FROM @foo/bar (FILE_FORMAT => ds_sandbox.test.my_csv_format, PATTERN => \'test\') AS bla',
    );

    this.validateIdentity(
      'SELECT * FROM @test.public.thing/location/somefile.csv( FILE_FORMAT => \'fmt\' )',
      'SELECT * FROM @test.public.thing/location/somefile.csv (FILE_FORMAT => \'fmt\')',
    );

  }

  testSample () {
    this.validateIdentity('SELECT * FROM testtable TABLESAMPLE BERNOULLI (20.3)');
    this.validateIdentity('SELECT * FROM testtable TABLESAMPLE SYSTEM (3) SEED (82)');
    this.validateIdentity(
      'SELECT a FROM test PIVOT(SUM(x) FOR y IN (\'z\', \'q\')) AS x TABLESAMPLE BERNOULLI (0.1)',
    );
    this.validateIdentity(
      'SELECT i, j FROM table1 AS t1 INNER JOIN table2 AS t2 TABLESAMPLE BERNOULLI (50) WHERE t2.j = t1.i',
    );
    this.validateIdentity(
      'SELECT * FROM (SELECT * FROM t1 JOIN t2 ON t1.a = t2.c) TABLESAMPLE BERNOULLI (1)',
    );
    this.validateIdentity(
      'SELECT * FROM testtable TABLESAMPLE (10 ROWS)',
      'SELECT * FROM testtable TABLESAMPLE BERNOULLI (10 ROWS)',
    );
    this.validateIdentity(
      'SELECT * FROM testtable TABLESAMPLE (100)',
      'SELECT * FROM testtable TABLESAMPLE BERNOULLI (100)',
    );
    this.validateIdentity(
      'SELECT * FROM testtable SAMPLE (10)',
      'SELECT * FROM testtable TABLESAMPLE BERNOULLI (10)',
    );
    this.validateIdentity(
      'SELECT * FROM testtable SAMPLE ROW (0)',
      'SELECT * FROM testtable TABLESAMPLE ROW (0)',
    );
    this.validateIdentity(
      'SELECT a FROM test SAMPLE BLOCK (0.5) SEED (42)',
      'SELECT a FROM test TABLESAMPLE BLOCK (0.5) SEED (42)',
    );
    this.validateIdentity(
      'SELECT user_id, value FROM table_name SAMPLE BERNOULLI ($s) SEED (0)',
      'SELECT user_id, value FROM table_name TABLESAMPLE BERNOULLI ($s) SEED (0)',
    );

    this.validateAll(
      'SELECT * FROM example TABLESAMPLE BERNOULLI (3) SEED (82)',
      {
        read: {
          'duckdb': 'SELECT * FROM example TABLESAMPLE BERNOULLI (3 PERCENT) REPEATABLE (82)',
        },
        write: {
          'databricks': 'SELECT * FROM example TABLESAMPLE (3 PERCENT) REPEATABLE (82)',
          'duckdb': 'SELECT * FROM example TABLESAMPLE BERNOULLI (3 PERCENT) REPEATABLE (82)',
          'snowflake': 'SELECT * FROM example TABLESAMPLE BERNOULLI (3) SEED (82)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM test AS _tmp TABLESAMPLE (5)',
      {
        write: {
          'postgres': 'SELECT * FROM test AS _tmp TABLESAMPLE BERNOULLI (5)',
          'snowflake': 'SELECT * FROM test AS _tmp TABLESAMPLE BERNOULLI (5)',
        },
      },
    );
    this.validateAll(
      `
            SELECT i, j
                FROM
                     table1 AS t1 SAMPLE (25)     -- 25% of rows in table1
                         INNER JOIN
                     table2 AS t2 SAMPLE (50)     -- 50% of rows in table2
                WHERE t2.j = t1.i`,
      {
        write: {
          'snowflake': 'SELECT i, j FROM table1 AS t1 TABLESAMPLE BERNOULLI (25) /* 25% of rows in table1 */ INNER JOIN table2 AS t2 TABLESAMPLE BERNOULLI (50) /* 50% of rows in table2 */ WHERE t2.j = t1.i',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM testtable SAMPLE BLOCK (0.012) REPEATABLE (99992)',
      {
        write: {
          'snowflake': 'SELECT * FROM testtable TABLESAMPLE BLOCK (0.012) SEED (99992)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM (SELECT * FROM t1 join t2 on t1.a = t2.c) SAMPLE (1)',
      {
        write: {
          'snowflake': 'SELECT * FROM (SELECT * FROM t1 JOIN t2 ON t1.a = t2.c) TABLESAMPLE BERNOULLI (1)',
          'spark': 'SELECT * FROM (SELECT * FROM t1 JOIN t2 ON t1.a = t2.c) TABLESAMPLE (1 PERCENT)',
        },
      },
    );
    this.validateAll(
      'TO_DOUBLE(expr)',
      {
        write: {
          'snowflake': 'TO_DOUBLE(expr)',
          'duckdb': 'CAST(expr AS DOUBLE)',
        },
      },
    );
    this.validateAll(
      'TO_DOUBLE(expr, fmt)',
      {
        write: {
          'snowflake': 'TO_DOUBLE(expr, fmt)',
          'duckdb': UnsupportedError,
        },
      },
    );

  }

  testTimestamps () {
    this.validateIdentity('SELECT CAST(\'12:00:00\' AS TIME)');
    this.validateIdentity('SELECT DATE_PART(month, a)');

    (this.validateIdentity(
      'SELECT DATE_PART(year FROM CAST(\'2024-04-08\' AS DATE))',
      'SELECT DATE_PART(year, CAST(\'2024-04-08\' AS DATE))',
    ).args.expressions?.[0] as Expression).assertIs(ExtractExpr);
    (this.validateIdentity(
      'SELECT DATE_PART(\'month\' FROM CAST(\'2024-04-08\' AS DATE))',
      'SELECT DATE_PART(\'month\', CAST(\'2024-04-08\' AS DATE))',
    ).args.expressions?.[0] as Expression).assertIs(ExtractExpr);
    (this.validateIdentity('SELECT DATE_PART(day FROM a)', 'SELECT DATE_PART(day, a)').args.expressions?.[0] as Expression).assertIs(ExtractExpr);

    for (const dataType of [
      'TIMESTAMP',
      'TIMESTAMPLTZ',
      'TIMESTAMPNTZ',
    ]) {
      this.validateIdentity(`CAST(a AS ${dataType})`);

      this.validateIdentity('CAST(a AS TIMESTAMP_NTZ)', 'CAST(a AS TIMESTAMPNTZ)');
      this.validateIdentity('CAST(a AS TIMESTAMP_LTZ)', 'CAST(a AS TIMESTAMPLTZ)');

      this.validateAll(
        'SELECT a::TIMESTAMP_LTZ(9)',
        {
          write: {
            'snowflake': 'SELECT CAST(a AS TIMESTAMPLTZ(9))',
          },
        },
      );
      this.validateAll(
        'SELECT a::TIMESTAMPLTZ',
        {
          write: {
            'snowflake': 'SELECT CAST(a AS TIMESTAMPLTZ)',
          },
        },
      );
      this.validateAll(
        'SELECT a::TIMESTAMP WITH LOCAL TIME ZONE',
        {
          write: {
            'snowflake': 'SELECT CAST(a AS TIMESTAMPLTZ)',
          },
        },
      );
      this.validateAll(
        'SELECT EXTRACT(\'month\', a)',
        {
          write: {
            'snowflake': 'SELECT DATE_PART(\'month\', a)',
          },
        },
      );
      this.validateAll(
        'SELECT DATE_PART(\'month\', a)',
        {
          write: {
            'snowflake': 'SELECT DATE_PART(\'month\', a)',
          },
        },
      );
      this.validateAll(
        'SELECT DATE_PART(month, a::DATETIME)',
        {
          write: {
            'snowflake': 'SELECT DATE_PART(month, CAST(a AS DATETIME))',
          },
        },
      );
      this.validateAll(
        'SELECT DATE_PART(epoch_second, foo) as ddate from table_name',
        {
          write: {
            'snowflake': 'SELECT DATE_PART(EPOCH_SECOND, foo) AS ddate FROM table_name',
            'duckdb': 'SELECT CAST(EPOCH(foo) AS BIGINT) AS ddate FROM table_name',
            'presto': 'SELECT TO_UNIXTIME(CAST(foo AS TIMESTAMP)) AS ddate FROM table_name',
          },
        },
      );
      this.validateAll(
        'SELECT DATE_PART(epoch_milliseconds, foo) as ddate from table_name',
        {
          write: {
            'snowflake': 'SELECT DATE_PART(EPOCH_MILLISECOND, foo) AS ddate FROM table_name',
            'duckdb': 'SELECT EPOCH_MS(foo) AS ddate FROM table_name',
            'presto': 'SELECT TO_UNIXTIME(CAST(foo AS TIMESTAMP)) * 1000 AS ddate FROM table_name',
          },
        },
      );
      this.validateAll(
        'DATEADD(DAY, 5, CAST(\'2008-12-25\' AS DATE))',
        {
          read: {
            'snowflake': 'TIMESTAMPADD(DAY, 5, CAST(\'2008-12-25\' AS DATE))',
          },
          write: {
            'bigquery': 'DATE_ADD(CAST(\'2008-12-25\' AS DATE), INTERVAL 5 DAY)',
            'snowflake': 'DATEADD(DAY, 5, CAST(\'2008-12-25\' AS DATE))',
          },
        },
      );
      this.validateIdentity(
        'DATEDIFF(DAY, CAST(\'2007-12-25\' AS DATE), CAST(\'2008-12-25\' AS DATE))',
      );
      this.validateIdentity(
        'TIMEDIFF(DAY, CAST(\'2007-12-25\' AS DATE), CAST(\'2008-12-25\' AS DATE))',
        'DATEDIFF(DAY, CAST(\'2007-12-25\' AS DATE), CAST(\'2008-12-25\' AS DATE))',
      );
      this.validateIdentity(
        'TIMESTAMPDIFF(DAY, CAST(\'2007-12-25\' AS DATE), CAST(\'2008-12-25\' AS DATE))',
        'DATEDIFF(DAY, CAST(\'2007-12-25\' AS DATE), CAST(\'2008-12-25\' AS DATE))',
      );

      // Test DATEDIFF with WEEK unit - week boundary crossing
      this.validateAll(
        'DATEDIFF(WEEK, \'2024-12-13\', \'2024-12-17\')',
        {
          write: {
            'duckdb': 'DATE_DIFF(\'WEEK\', DATE_TRUNC(\'WEEK\', CAST(\'2024-12-13\' AS DATE)), DATE_TRUNC(\'WEEK\', CAST(\'2024-12-17\' AS DATE)))',
            'snowflake': 'DATEDIFF(WEEK, \'2024-12-13\', \'2024-12-17\')',
          },
        },
      );
      this.validateAll(
        'DATEDIFF(WEEK, \'2024-12-15\', \'2024-12-16\')',
        {
          write: {
            'duckdb': 'DATE_DIFF(\'WEEK\', DATE_TRUNC(\'WEEK\', CAST(\'2024-12-15\' AS DATE)), DATE_TRUNC(\'WEEK\', CAST(\'2024-12-16\' AS DATE)))',
            'snowflake': 'DATEDIFF(WEEK, \'2024-12-15\', \'2024-12-16\')',
          },
        },
      );

      // Test DATEDIFF with other date parts - should not use DATE_TRUNC
      this.validateAll(
        'DATEDIFF(YEAR, \'2020-01-15\', \'2023-06-20\')',
        {
          write: {
            'duckdb': 'DATE_DIFF(\'YEAR\', CAST(\'2020-01-15\' AS DATE), CAST(\'2023-06-20\' AS DATE))',
            'snowflake': 'DATEDIFF(YEAR, \'2020-01-15\', \'2023-06-20\')',
          },
        },
      );
      this.validateAll(
        'DATEDIFF(MONTH, \'2020-01-15\', \'2023-06-20\')',
        {
          write: {
            'duckdb': 'DATE_DIFF(\'MONTH\', CAST(\'2020-01-15\' AS DATE), CAST(\'2023-06-20\' AS DATE))',
            'snowflake': 'DATEDIFF(MONTH, \'2020-01-15\', \'2023-06-20\')',
          },
        },
      );
      this.validateAll(
        'DATEDIFF(QUARTER, \'2020-01-15\', \'2023-06-20\')',
        {
          write: {
            'duckdb': 'DATE_DIFF(\'QUARTER\', CAST(\'2020-01-15\' AS DATE), CAST(\'2023-06-20\' AS DATE))',
            'snowflake': 'DATEDIFF(QUARTER, \'2020-01-15\', \'2023-06-20\')',
          },
        },
      );

      // Test DATEDIFF with NANOSECOND - DuckDB uses EPOCH_NS since DATE_DIFF doesn't support NANOSECOND
      this.validateAll(
        'DATEDIFF(NANOSECOND, \'2023-01-01 10:00:00.000000000\', \'2023-01-01 10:00:00.123456789\')',
        {
          write: {
            'duckdb': 'EPOCH_NS(CAST(\'2023-01-01 10:00:00.123456789\' AS TIMESTAMP_NS)) - EPOCH_NS(CAST(\'2023-01-01 10:00:00.000000000\' AS TIMESTAMP_NS))',
            'snowflake': 'DATEDIFF(NANOSECOND, \'2023-01-01 10:00:00.000000000\', \'2023-01-01 10:00:00.123456789\')',
          },
        },
      );

      // Test DATEDIFF with NANOSECOND on columns
      this.validateAll(
        'DATEDIFF(NANOSECOND, start_time, end_time)',
        {
          write: {
            'duckdb': 'EPOCH_NS(CAST(end_time AS TIMESTAMP_NS)) - EPOCH_NS(CAST(start_time AS TIMESTAMP_NS))',
            'snowflake': 'DATEDIFF(NANOSECOND, start_time, end_time)',
          },
        },
      );

      // Test DATEADD with NANOSECOND - DuckDB uses MAKE_TIMESTAMP_NS since INTERVAL doesn't support NANOSECOND
      this.validateAll(
        'DATEADD(NANOSECOND, 123456789, \'2023-01-01 10:00:00.000000000\')',
        {
          write: {
            'duckdb': 'MAKE_TIMESTAMP_NS(EPOCH_NS(CAST(\'2023-01-01 10:00:00.000000000\' AS TIMESTAMP_NS)) + 123456789)',
            'snowflake': 'DATEADD(NANOSECOND, 123456789, \'2023-01-01 10:00:00.000000000\')',
          },
        },
      );

      // Test DATEADD with NANOSECOND on columns
      this.validateAll(
        'DATEADD(NANOSECOND, nano_offset, timestamp_col)',
        {
          write: {
            'duckdb': 'MAKE_TIMESTAMP_NS(EPOCH_NS(CAST(timestamp_col AS TIMESTAMP_NS)) + nano_offset)',
            'snowflake': 'DATEADD(NANOSECOND, nano_offset, timestamp_col)',
          },
        },
      );

      // Test negative NANOSECOND values (subtraction)
      this.validateAll(
        'DATEADD(NANOSECOND, -123456789, \'2023-01-01 10:00:00.500000000\')',
        {
          write: {
            'duckdb': 'MAKE_TIMESTAMP_NS(EPOCH_NS(CAST(\'2023-01-01 10:00:00.500000000\' AS TIMESTAMP_NS)) + -123456789)',
            'snowflake': 'DATEADD(NANOSECOND, -123456789, \'2023-01-01 10:00:00.500000000\')',
          },
        },
      );

      // Test TIMESTAMPDIFF with NANOSECOND - Snowflake parser converts to DATEDIFF
      this.validateAll(
        'TIMESTAMPDIFF(NANOSECOND, \'2023-01-01 10:00:00.000000000\', \'2023-01-01 10:00:00.123456789\')',
        {
          write: {
            'duckdb': 'EPOCH_NS(CAST(\'2023-01-01 10:00:00.123456789\' AS TIMESTAMP_NS)) - EPOCH_NS(CAST(\'2023-01-01 10:00:00.000000000\' AS TIMESTAMP_NS))',
            'snowflake': 'DATEDIFF(NANOSECOND, \'2023-01-01 10:00:00.000000000\', \'2023-01-01 10:00:00.123456789\')',
          },
        },
      );

      // Test TIMESTAMPADD with NANOSECOND - Snowflake parser converts to DATEADD
      this.validateAll(
        'TIMESTAMPADD(NANOSECOND, 123456789, \'2023-01-01 10:00:00.000000000\')',
        {
          write: {
            'duckdb': 'MAKE_TIMESTAMP_NS(EPOCH_NS(CAST(\'2023-01-01 10:00:00.000000000\' AS TIMESTAMP_NS)) + 123456789)',
            'snowflake': 'DATEADD(NANOSECOND, 123456789, \'2023-01-01 10:00:00.000000000\')',
          },
        },
      );

      this.validateIdentity('DATEADD(y, 5, x)', 'DATEADD(YEAR, 5, x)');
      this.validateIdentity('DATEADD(y, 5, x)', 'DATEADD(YEAR, 5, x)');
      this.validateIdentity('DATE_PART(yyy, x)', 'DATE_PART(YEAR, x)');
      this.validateIdentity('DATE_TRUNC(yr, x)', 'DATE_TRUNC(\'YEAR\', x)');
      this.validateAll(
        'DATE_TRUNC(\'YEAR\', CAST(\'2024-06-15\' AS DATE))',
        {
          write: {
            'snowflake': 'DATE_TRUNC(\'YEAR\', CAST(\'2024-06-15\' AS DATE))',
            'duckdb': 'DATE_TRUNC(\'YEAR\', CAST(\'2024-06-15\' AS DATE))',
          },
        },
      );
      this.validateAll(
        'DATE_TRUNC(\'HOUR\', CAST(\'2026-01-01 00:00:00\' AS TIMESTAMP))',
        {
          write: {
            'snowflake': 'DATE_TRUNC(\'HOUR\', CAST(\'2026-01-01 00:00:00\' AS TIMESTAMP))',
            'duckdb': 'DATE_TRUNC(\'HOUR\', CAST(\'2026-01-01 00:00:00\' AS TIMESTAMP))',
          },
        },
      );
      // Snowflake's DATE_TRUNC return type matches type of the expresison
      // DuckDB's DATE_TRUNC return type matches type of granularity part
      // In Snowflake --> DuckDB, DATE_TRUNC(date_part, timestamp) should be cast to timestamp to preserve Snowflake behavior
      this.validateAll(
        'DATE_TRUNC(YEAR, TIMESTAMP \'2026-01-01 00:00:00\')',
        {
          write: {
            'snowflake': 'DATE_TRUNC(\'YEAR\', CAST(\'2026-01-01 00:00:00\' AS TIMESTAMP))',
            'duckdb': 'CAST(DATE_TRUNC(\'YEAR\', CAST(\'2026-01-01 00:00:00\' AS TIMESTAMP)) AS TIMESTAMP)',
          },
        },
      );
      this.validateAll(
        'DATE_TRUNC(MONTH, CAST(\'2024-06-15 14:23:45\' AS TIMESTAMPTZ))',
        {
          write: {
            'snowflake': 'DATE_TRUNC(\'MONTH\', CAST(\'2024-06-15 14:23:45\' AS TIMESTAMPTZ))',
            'duckdb': 'CAST(DATE_TRUNC(\'MONTH\', CAST(\'2024-06-15 14:23:45\' AS TIMESTAMPTZ)) AS TIMESTAMPTZ)',
          },
        },
      );
      this.validateAll(
        'DATE_TRUNC(\'WEEK\', CURRENT_DATE)',
        {
          write: {
            'snowflake': 'DATE_TRUNC(\'WEEK\', CURRENT_DATE)',
            'duckdb': 'DATE_TRUNC(\'WEEK\', CURRENT_DATE)',
          },
        },
      );

      // In Snowflake --> DuckDB, DATE_TRUNC(time_part, date) should be cast to date to preserve Snowflake behavior
      this.validateAll(
        'DATE_TRUNC(\'HOUR\', CAST(\'2026-01-01\' AS DATE))',
        {
          write: {
            'snowflake': 'DATE_TRUNC(\'HOUR\', CAST(\'2026-01-01\' AS DATE))',
            'duckdb': 'CAST(DATE_TRUNC(\'HOUR\', CAST(\'2026-01-01\' AS DATE)) AS DATE)',
          },
        },
      );

      // DuckDB does not support DATE_TRUNC(time_part, time), so we add a dummy date to generate DATE_TRUNC(time_part, date) --> DATE in DuckDB
      // Then it is casted to a time (HH:MM:SS) to match Snowflake
      this.validateAll(
        'DATE_TRUNC(\'HOUR\', CAST(\'14:23:45.123456\' AS TIME))',
        {
          write: {
            'snowflake': 'DATE_TRUNC(\'HOUR\', CAST(\'14:23:45.123456\' AS TIME))',
            'duckdb': 'CAST(DATE_TRUNC(\'HOUR\', CAST(\'1970-01-01\' AS DATE) + CAST(\'14:23:45.123456\' AS TIME)) AS TIME)',
          },
        },
      );

      this.validateAll(
        'DATE(x)',
        {
          write: {
            'duckdb': 'CAST(x AS DATE)',
            'snowflake': 'TO_DATE(x)',
          },
        },
      );

      this.validateAll(
        'DATE(\'01-01-2000\', \'MM-DD-YYYY\')',
        {
          write: {
            'snowflake': 'TO_DATE(\'01-01-2000\', \'mm-DD-yyyy\')',
            'duckdb': 'CAST(STRPTIME(\'01-01-2000\', \'%m-%d-%Y\') AS DATE)',
          },
        },
      );

      this.validateIdentity('SELECT TO_TIME(x) FROM t');
      this.validateAll(
        'SELECT TO_TIME(\'12:05:00\')',
        {
          write: {
            'bigquery': 'SELECT CAST(\'12:05:00\' AS TIME)',
            'snowflake': 'SELECT CAST(\'12:05:00\' AS TIME)',
            'duckdb': 'SELECT CAST(\'12:05:00\' AS TIME)',
          },
        },
      );
      this.validateAll(
        'SELECT TO_TIME(\'2024-01-15 14:30:00\'::TIMESTAMP)',
        {
          write: {
            'bigquery': 'SELECT TIME(CAST(\'2024-01-15 14:30:00\' AS DATETIME))',
            'snowflake': 'SELECT TO_TIME(CAST(\'2024-01-15 14:30:00\' AS TIMESTAMP))',
            'duckdb': 'SELECT CAST(CAST(\'2024-01-15 14:30:00\' AS TIMESTAMP) AS TIME)',
          },
        },
      );
      this.validateAll(
        'SELECT TO_TIME(CONVERT_TIMEZONE(\'UTC\', \'US/Pacific\', \'2024-08-06 09:10:00.000\')) AS pst_time',
        {
          write: {
            'snowflake': 'SELECT TO_TIME(CONVERT_TIMEZONE(\'UTC\', \'US/Pacific\', \'2024-08-06 09:10:00.000\')) AS pst_time',
            'duckdb': 'SELECT CAST(CAST(\'2024-08-06 09:10:00.000\' AS TIMESTAMP) AT TIME ZONE \'UTC\' AT TIME ZONE \'US/Pacific\' AS TIME) AS pst_time',
          },
        },
      );
      this.validateAll(
        'SELECT TO_TIME(\'11.15.00\', \'hh24.mi.ss\')',
        {
          write: {
            'snowflake': 'SELECT TO_TIME(\'11.15.00\', \'hh24.mi.ss\')',
            'duckdb': 'SELECT CAST(STRPTIME(\'11.15.00\', \'%H.%M.%S\') AS TIME)',
          },
        },
      );
      this.validateAll(
        'SELECT TO_TIME(\'093000\', \'HH24MISS\')',
        {
          write: {
            'duckdb': 'SELECT CAST(STRPTIME(\'093000\', \'%H%M%S\') AS TIME)',
            'snowflake': 'SELECT TO_TIME(\'093000\', \'hh24miss\')',
          },
        },
      );
      this.validateAll(
        'SELECT TRY_TO_TIME(\'093000\', \'HH24MISS\')',
        {
          write: {
            'snowflake': 'SELECT TRY_TO_TIME(\'093000\', \'hh24miss\')',
            'duckdb': 'SELECT TRY_CAST(TRY_STRPTIME(\'093000\', \'%H%M%S\') AS TIME)',
          },
        },
      );
      this.validateAll(
        'SELECT TRY_TO_TIME(\'11.15.00\')',
        {
          write: {
            'snowflake': 'SELECT TRY_CAST(\'11.15.00\' AS TIME)',
            'duckdb': 'SELECT TRY_CAST(\'11.15.00\' AS TIME)',
          },
        },
      );
      this.validateAll(
        'SELECT TRY_TO_TIME(\'11.15.00\', \'hh24.mi.ss\')',
        {
          write: {
            'snowflake': 'SELECT TRY_TO_TIME(\'11.15.00\', \'hh24.mi.ss\')',
            'duckdb': 'SELECT TRY_CAST(TRY_STRPTIME(\'11.15.00\', \'%H.%M.%S\') AS TIME)',
          },
        },
      );

    }
  }

  testToDate () {
    this.validateIdentity('TO_DATE(\'12345\')').assertIs(AnonymousExpr);

    this.validateIdentity('TO_DATE(x)').assertIs(TsOrDsToDateExpr);

    this.validateAll(
      'TO_DATE(\'01-01-2000\', \'MM-DD-YYYY\')',
      {
        write: {
          'snowflake': 'TO_DATE(\'01-01-2000\', \'mm-DD-yyyy\')',
          'duckdb': 'CAST(STRPTIME(\'01-01-2000\', \'%m-%d-%Y\') AS DATE)',
        },
      },
    );

    this.validateAll(
      'TO_DATE(x, \'MM-DD-YYYY\')',
      {
        write: {
          'snowflake': 'TO_DATE(x, \'mm-DD-yyyy\')',
          'duckdb': 'CAST(STRPTIME(x, \'%m-%d-%Y\') AS DATE)',
        },
      },
    );

    this.validateIdentity(
      'SELECT TO_DATE(\'2019-02-28\') + INTERVAL \'1 day, 1 year\'',
      'SELECT CAST(\'2019-02-28\' AS DATE) + INTERVAL \'1 day, 1 year\'',
    );

    this.validateIdentity('TRY_TO_DATE(x)').assertIs(TsOrDsToDateExpr);

    this.validateAll(
      'TRY_TO_DATE(\'2024-01-31\')',
      {
        write: {
          'snowflake': 'TRY_CAST(\'2024-01-31\' AS DATE)',
          'duckdb': 'TRY_CAST(\'2024-01-31\' AS DATE)',
        },
      },
    );
    this.validateIdentity('TRY_TO_DATE(\'2024-01-31\', \'AUTO\')');

    this.validateAll(
      'TRY_TO_DATE(\'01-01-2000\', \'MM-DD-YYYY\')',
      {
        write: {
          'snowflake': 'TRY_TO_DATE(\'01-01-2000\', \'mm-DD-yyyy\')',
          'duckdb': 'CAST(CAST(TRY_STRPTIME(\'01-01-2000\', \'%m-%d-%Y\') AS TIMESTAMP) AS DATE)',
        },
      },
    );

    for (const i of [
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
    ]) {
      const fractionalFormat = 'ff' + String(i);
      let duckDbFormat = '%n';

      if (i == 3) {
        duckDbFormat = '%g';
      } else if (i == 6) {
        duckDbFormat = '%f';
      }

      this.validateAll(
        `TRY_TO_DATE('2013-04-28T20:57:01', 'yyyy-mm-DDThh24:mi:ss.${fractionalFormat}')`,
        {
          write: {
            'snowflake': `TRY_TO_DATE('2013-04-28T20:57:01', 'yyyy-mm-DDThh24:mi:ss.${fractionalFormat}')`,
            'duckdb': `CAST(CAST(TRY_STRPTIME('2013-04-28T20:57:01', '%Y-%m-%dT%H:%M:%S.${duckDbFormat}') AS TIMESTAMP) AS DATE)`,
          },
        },
      );
    }

    this.validateAll(
      'TRY_TO_DATE(\'2013-04-28T20:57:01.888\', \'yyyy-mm-DDThh24:mi:ss.ff\')',
      {
        write: {
          'snowflake': 'TRY_TO_DATE(\'2013-04-28T20:57:01.888\', \'yyyy-mm-DDThh24:mi:ss.ff9\')',
          'duckdb': 'CAST(CAST(TRY_STRPTIME(\'2013-04-28T20:57:01.888\', \'%Y-%m-%dT%H:%M:%S.%n\') AS TIMESTAMP) AS DATE)',
        },
      },
    );

    const tzToFormat = {
      'tzh:tzm': '+07:00',
      'tzhtzm': '+0700',
      'tzh': '+07',
    };

    for (const [
      tzFormat,
      tz,
    ] of Object.entries(tzToFormat)) {
      this.validateAll(
        `TRY_TO_DATE('2013-04-28 20:57 ${tz}', 'YYYY-MM-DD HH24:MI ${tzFormat}')`,
        {
          write: {
            'snowflake': `TRY_TO_DATE('2013-04-28 20:57 ${tz}', 'yyyy-mm-DD hh24:mi ${tzFormat}')`,
            'duckdb': `CAST(CAST(TRY_STRPTIME('2013-04-28 20:57 ${tz}', '%Y-%m-%d %H:%M %z') AS TIMESTAMP) AS DATE)`,
          },
        },
      );

      this.validateAll(
        'TRY_TO_DATE(\'2013-04-28T20:57\', \'YYYY-MM-DD"T"HH24:MI:SS\')',
        {
          write: {
            'snowflake': 'TRY_TO_DATE(\'2013-04-28T20:57\', \'yyyy-mm-DDThh24:mi:ss\')',
            'duckdb': 'CAST(CAST(TRY_STRPTIME(\'2013-04-28T20:57\', \'%Y-%m-%dT%H:%M:%S\') AS TIMESTAMP) AS DATE)',
          },
        },
      );

    }
  }

  testTrunc () {
    // Numeric truncation identity
    this.validateIdentity('TRUNC(3.14159, 2)').assertIs(TruncExpr);
    this.validateIdentity('TRUNC(price, 0)').assertIs(TruncExpr);
    this.validateIdentity('TRUNC(3.14159)').assertIs(TruncExpr);

    // Single-arg TRUNC is always numeric in Snowflake (date trunc requires unit)
    this.validateIdentity('TRUNC(col)').assertIs(TruncExpr);

    // Date truncation with typed column and unit
    // (parse_one because DateTrunc generates as DATE_TRUNC, not TRUNC)
    this.parseOne('TRUNC(CAST(x AS DATE), \'MONTH\')').assertIs(DateTruncExpr);
    this.parseOne('TRUNC(CAST(x AS TIMESTAMP), \'MONTH\')').assertIs(DateTruncExpr);
    this.parseOne('TRUNC(CAST(x AS DATETIME), \'MONTH\')').assertIs(DateTruncExpr);

    // Fallback to Anonymous when type cannot be determined
    this.validateIdentity('TRUNC(foo, bar)').assertIs(AnonymousExpr);

    // Cross-dialect numeric truncation transpilation
    this.validateAll(
      'TRUNC(3.14159, 2)',
      {
        write: {
          'snowflake': 'TRUNC(3.14159, 2)',
          'oracle': 'TRUNC(3.14159, 2)',
          'postgres': 'TRUNC(3.14159, 2)',
          'mysql': 'TRUNCATE(3.14159, 2)',
          'tsql': 'ROUND(3.14159, 2, 1)',
          'bigquery': 'TRUNC(3.14159, 2)',
          'duckdb': 'TRUNC(3.14159)',
          'presto': 'TRUNCATE(3.14159, 2)',
          'clickhouse': 'trunc(3.14159, 2)',
          'spark': 'CAST(3.14159 AS BIGINT)',
        },
      },
    );

    // Single-argument numeric TRUNC transpilation
    this.validateAll(
      'TRUNC(3.14159)',
      {
        write: {
          'snowflake': 'TRUNC(3.14159)',
          'oracle': 'TRUNC(3.14159)',
          'postgres': 'TRUNC(3.14159)',
          'mysql': 'TRUNCATE(3.14159)',
          'tsql': 'ROUND(3.14159, 0, 1)',
        },
      },
    );

    // Read numeric TRUNC from other dialects
    this.validateAll(
      'TRUNC(price, 2)',
      {
        read: {
          'mysql': 'TRUNCATE(price, 2)',
          'oracle': 'TRUNC(price, 2)',
          'postgres': 'TRUNC(price, 2)',
        },
        write: {
          'snowflake': 'TRUNC(price, 2)',
        },
      },
    );

  }

  testSemiStructuredTypes () {
    this.validateIdentity('SELECT CAST(a AS VARIANT)');
    this.validateIdentity('SELECT CAST(a AS ARRAY)');

    this.validateAll(
      'SELECT a::VARIANT',
      {
        write: {
          'snowflake': 'SELECT CAST(a AS VARIANT)',
          'tsql': 'SELECT CAST(a AS SQL_VARIANT)',
        },
      },
    );
    this.validateAll(
      'ARRAY_CONSTRUCT(0, 1, 2)',
      {
        write: {
          'snowflake': '[0, 1, 2]',
          'bigquery': '[0, 1, 2]',
          'duckdb': '[0, 1, 2]',
          'presto': 'ARRAY[0, 1, 2]',
          'spark': 'ARRAY(0, 1, 2)',
        },
      },
    );
    this.validateAll(
      'ARRAYS_ZIP([1, 2], [3, 4], [4, 5])',
      {
        write: {
          'snowflake': 'ARRAYS_ZIP([1, 2], [3, 4], [4, 5])',
          'duckdb': 'CASE WHEN [1, 2] IS NULL OR [3, 4] IS NULL OR [4, 5] IS NULL THEN NULL WHEN LENGTH([1, 2]) = 0 AND LENGTH([3, 4]) = 0 AND LENGTH([4, 5]) = 0 THEN [{\'$1\': NULL, \'$2\': NULL, \'$3\': NULL}] ELSE LIST_TRANSFORM(RANGE(0, CASE WHEN LENGTH([1, 2]) IS NULL OR LENGTH([3, 4]) IS NULL OR LENGTH([4, 5]) IS NULL THEN NULL ELSE GREATEST(LENGTH([1, 2]), LENGTH([3, 4]), LENGTH([4, 5])) END), __i -> {\'$1\': COALESCE([1, 2], [])[__i + 1], \'$2\': COALESCE([3, 4], [])[__i + 1], \'$3\': COALESCE([4, 5], [])[__i + 1]}) END',
        },
      },
    );
    this.validateAll(
      'ARRAYS_ZIP([1, 2, 3])',
      {
        write: {
          'snowflake': 'ARRAYS_ZIP([1, 2, 3])',
          'duckdb': 'CASE WHEN [1, 2, 3] IS NULL THEN NULL WHEN LENGTH([1, 2, 3]) = 0 THEN [{\'$1\': NULL}] ELSE LIST_TRANSFORM(RANGE(0, LENGTH([1, 2, 3])), __i -> {\'$1\': COALESCE([1, 2, 3], [])[__i + 1]}) END',
        },
      },
    );
    this.validateAll(
      'SELECT a::OBJECT',
      {
        write: {
          'snowflake': 'SELECT CAST(a AS OBJECT)',
        },
      },
    );

  }

  testNextDay () {
    this.validateAll(
      'SELECT NEXT_DAY(CAST(\'2024-01-01\' AS DATE), \'Monday\')',
      {
        write: {
          'snowflake': 'SELECT NEXT_DAY(CAST(\'2024-01-01\' AS DATE), \'Monday\')',
          'duckdb': 'SELECT CAST(CAST(\'2024-01-01\' AS DATE) + INTERVAL ((((1 - ISODOW(CAST(\'2024-01-01\' AS DATE))) + 6) % 7) + 1) DAY AS DATE)',
        },
      },
    );

    this.validateAll(
      'SELECT NEXT_DAY(CAST(\'2024-01-05\' AS DATE), \'Friday\')',
      {
        write: {
          'snowflake': 'SELECT NEXT_DAY(CAST(\'2024-01-05\' AS DATE), \'Friday\')',
          'duckdb': 'SELECT CAST(CAST(\'2024-01-05\' AS DATE) + INTERVAL ((((5 - ISODOW(CAST(\'2024-01-05\' AS DATE))) + 6) % 7) + 1) DAY AS DATE)',
        },
      },
    );

    this.validateAll(
      'SELECT NEXT_DAY(CAST(\'2024-01-05\' AS DATE), \'WE\')',
      {
        write: {
          'snowflake': 'SELECT NEXT_DAY(CAST(\'2024-01-05\' AS DATE), \'WE\')',
          'duckdb': 'SELECT CAST(CAST(\'2024-01-05\' AS DATE) + INTERVAL ((((3 - ISODOW(CAST(\'2024-01-05\' AS DATE))) + 6) % 7) + 1) DAY AS DATE)',
        },
      },
    );

    this.validateAll(
      'SELECT NEXT_DAY(CAST(\'2024-01-01 10:30:45\' AS TIMESTAMP), \'Friday\')',
      {
        write: {
          'snowflake': 'SELECT NEXT_DAY(CAST(\'2024-01-01 10:30:45\' AS TIMESTAMP), \'Friday\')',
          'duckdb': 'SELECT CAST(CAST(\'2024-01-01 10:30:45\' AS TIMESTAMP) + INTERVAL ((((5 - ISODOW(CAST(\'2024-01-01 10:30:45\' AS TIMESTAMP))) + 6) % 7) + 1) DAY AS DATE)',
        },
      },
    );

    this.validateAll(
      'SELECT NEXT_DAY(CAST(\'2024-01-01\' AS DATE), day_column)',
      {
        write: {
          'snowflake': 'SELECT NEXT_DAY(CAST(\'2024-01-01\' AS DATE), day_column)',
          'duckdb': 'SELECT CAST(CAST(\'2024-01-01\' AS DATE) + INTERVAL ((((CASE WHEN STARTS_WITH(UPPER(day_column), \'MO\') THEN 1 WHEN STARTS_WITH(UPPER(day_column), \'TU\') THEN 2 WHEN STARTS_WITH(UPPER(day_column), \'WE\') THEN 3 WHEN STARTS_WITH(UPPER(day_column), \'TH\') THEN 4 WHEN STARTS_WITH(UPPER(day_column), \'FR\') THEN 5 WHEN STARTS_WITH(UPPER(day_column), \'SA\') THEN 6 WHEN STARTS_WITH(UPPER(day_column), \'SU\') THEN 7 END - ISODOW(CAST(\'2024-01-01\' AS DATE))) + 6) % 7) + 1) DAY AS DATE)',
        },
      },
    );

  }

  testPreviousDay () {
    this.validateAll(
      'SELECT PREVIOUS_DAY(DATE \'2024-01-15\', \'Monday\')',
      {
        write: {
          'duckdb': 'SELECT CAST(CAST(\'2024-01-15\' AS DATE) - INTERVAL ((((ISODOW(CAST(\'2024-01-15\' AS DATE)) - 1) + 6) % 7) + 1) DAY AS DATE)',
          'snowflake': 'SELECT PREVIOUS_DAY(CAST(\'2024-01-15\' AS DATE), \'Monday\')',
        },
      },
    );

    this.validateAll(
      'SELECT PREVIOUS_DAY(DATE \'2024-01-15\', \'Fr\')',
      {
        write: {
          'duckdb': 'SELECT CAST(CAST(\'2024-01-15\' AS DATE) - INTERVAL ((((ISODOW(CAST(\'2024-01-15\' AS DATE)) - 5) + 6) % 7) + 1) DAY AS DATE)',
          'snowflake': 'SELECT PREVIOUS_DAY(CAST(\'2024-01-15\' AS DATE), \'Fr\')',
        },
      },
    );

    this.validateAll(
      'SELECT PREVIOUS_DAY(TIMESTAMP \'2024-01-15 10:30:45\', \'Monday\')',
      {
        write: {
          'duckdb': 'SELECT CAST(CAST(\'2024-01-15 10:30:45\' AS TIMESTAMP) - INTERVAL ((((ISODOW(CAST(\'2024-01-15 10:30:45\' AS TIMESTAMP)) - 1) + 6) % 7) + 1) DAY AS DATE)',
          'snowflake': 'SELECT PREVIOUS_DAY(CAST(\'2024-01-15 10:30:45\' AS TIMESTAMP), \'Monday\')',
        },
      },
    );

    this.validateAll(
      'SELECT PREVIOUS_DAY(DATE \'2024-01-15\', day_column)',
      {
        write: {
          'duckdb': 'SELECT CAST(CAST(\'2024-01-15\' AS DATE) - INTERVAL ((((ISODOW(CAST(\'2024-01-15\' AS DATE)) - CASE WHEN STARTS_WITH(UPPER(day_column), \'MO\') THEN 1 WHEN STARTS_WITH(UPPER(day_column), \'TU\') THEN 2 WHEN STARTS_WITH(UPPER(day_column), \'WE\') THEN 3 WHEN STARTS_WITH(UPPER(day_column), \'TH\') THEN 4 WHEN STARTS_WITH(UPPER(day_column), \'FR\') THEN 5 WHEN STARTS_WITH(UPPER(day_column), \'SA\') THEN 6 WHEN STARTS_WITH(UPPER(day_column), \'SU\') THEN 7 END) + 6) % 7) + 1) DAY AS DATE)',
          'snowflake': 'SELECT PREVIOUS_DAY(CAST(\'2024-01-15\' AS DATE), day_column)',
        },
      },
    );

  }

  testHistoricalData () {
    this.validateIdentity('SELECT * FROM my_table AT (STATEMENT => $query_id_var)');
    this.validateIdentity('SELECT * FROM my_table AT (OFFSET => -60 * 5)');
    this.validateIdentity('SELECT * FROM my_table BEFORE (STATEMENT => $query_id_var)');
    this.validateIdentity('SELECT * FROM my_table BEFORE (OFFSET => -60 * 5)');
    this.validateIdentity('CREATE SCHEMA restored_schema CLONE my_schema AT (OFFSET => -3600)');
    this.validateIdentity(
      'CREATE TABLE restored_table CLONE my_table AT (TIMESTAMP => CAST(\'Sat, 09 May 2015 01:01:00 +0300\' AS TIMESTAMPTZ))',
    );
    this.validateIdentity(
      'CREATE DATABASE restored_db CLONE my_db BEFORE (STATEMENT => \'8e5d0ca9-005e-44e6-b858-a8f5b37c5726\')',
    );
    this.validateIdentity(
      'SELECT * FROM my_table AT (TIMESTAMP => TO_TIMESTAMP(1432669154242, 3))',
    );
    this.validateIdentity(
      'SELECT * FROM my_table AT (OFFSET => -60 * 5) AS T WHERE T.flag = \'valid\'',
    );
    this.validateIdentity(
      'SELECT * FROM my_table AT (STATEMENT => \'8e5d0ca9-005e-44e6-b858-a8f5b37c5726\')',
    );
    this.validateIdentity(
      'SELECT * FROM my_table BEFORE (STATEMENT => \'8e5d0ca9-005e-44e6-b858-a8f5b37c5726\')',
    );
    this.validateIdentity(
      'SELECT * FROM my_table AT (TIMESTAMP => \'Fri, 01 May 2015 16:20:00 -0700\'::timestamp)',
      'SELECT * FROM my_table AT (TIMESTAMP => CAST(\'Fri, 01 May 2015 16:20:00 -0700\' AS TIMESTAMP))',
    );
    this.validateIdentity(
      'SELECT * FROM my_table AT(TIMESTAMP => \'Fri, 01 May 2015 16:20:00 -0700\'::timestamp_tz)',
      'SELECT * FROM my_table AT (TIMESTAMP => CAST(\'Fri, 01 May 2015 16:20:00 -0700\' AS TIMESTAMPTZ))',
    );
    this.validateIdentity(
      'SELECT * FROM my_table BEFORE (TIMESTAMP => \'Fri, 01 May 2015 16:20:00 -0700\'::timestamp_tz);',
      'SELECT * FROM my_table BEFORE (TIMESTAMP => CAST(\'Fri, 01 May 2015 16:20:00 -0700\' AS TIMESTAMPTZ))',
    );
    this.validateIdentity(
      `
            SELECT oldt.* , newt.*
            FROM my_table BEFORE(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726') AS oldt
            FULL OUTER JOIN my_table AT(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726') AS newt
            ON oldt.id = newt.id
            WHERE oldt.id IS NULL OR newt.id IS NULL;
            `,
      'SELECT oldt.*, newt.* FROM my_table BEFORE (STATEMENT => \'8e5d0ca9-005e-44e6-b858-a8f5b37c5726\') AS oldt FULL OUTER JOIN my_table AT (STATEMENT => \'8e5d0ca9-005e-44e6-b858-a8f5b37c5726\') AS newt ON oldt.id = newt.id WHERE oldt.id IS NULL OR newt.id IS NULL',
    );

    // Make sure that the historical data keywords can still be used as aliases
    for (const historicalDataPrefix of [
      'AT',
      'BEFORE',
      'END',
      'CHANGES',
    ]) {
      for (const schemaSuffix of [
        '',
        '(col)',
      ]) {
        this.validateIdentity(
          `SELECT * FROM foo ${historicalDataPrefix}${schemaSuffix}`,
          `SELECT * FROM foo AS ${historicalDataPrefix}${schemaSuffix}`,
        );

      }
    }
  }

  testDdl () {
    for (const constraintPrefix of [
      'WITH ',
      '',
    ]) {
      this.validateIdentity(
        `CREATE TABLE t (id INT ${constraintPrefix}MASKING POLICY p.q.r)`,
        'CREATE TABLE t (id INT MASKING POLICY p.q.r)',
      );
      this.validateIdentity(
        `CREATE TABLE t (id INT ${constraintPrefix}MASKING POLICY p USING (c1, c2, c3))`,
        'CREATE TABLE t (id INT MASKING POLICY p USING (c1, c2, c3))',
      );
      this.validateIdentity(
        `CREATE TABLE t (id INT ${constraintPrefix}PROJECTION POLICY p.q.r)`,
        'CREATE TABLE t (id INT PROJECTION POLICY p.q.r)',
      );
      this.validateIdentity(
        `CREATE TABLE t (id INT ${constraintPrefix}TAG (key1='value_1', key2='value_2'))`,
        'CREATE TABLE t (id INT TAG (key1=\'value_1\', key2=\'value_2\'))',
      );

      this.validateIdentity('CREATE OR REPLACE TABLE foo COPY GRANTS USING TEMPLATE (SELECT 1)');
      this.validateIdentity('USE SECONDARY ROLES ALL');
      this.validateIdentity('USE SECONDARY ROLES NONE');
      this.validateIdentity('USE SECONDARY ROLES a, b, c');
      this.validateIdentity('CREATE SECURE VIEW table1 AS (SELECT a FROM table2)');
      this.validateIdentity('CREATE OR REPLACE VIEW foo (uid) COPY GRANTS AS (SELECT 1)');
      this.validateIdentity('CREATE TABLE geospatial_table (id INT, g GEOGRAPHY)');
      this.validateIdentity('CREATE MATERIALIZED VIEW a COMMENT=\'...\' AS SELECT 1 FROM x');
      this.validateIdentity('CREATE DATABASE mytestdb_clone CLONE mytestdb');
      this.validateIdentity('CREATE SCHEMA mytestschema_clone CLONE testschema');
      this.validateIdentity('CREATE TABLE IDENTIFIER(\'foo\') (COLUMN1 VARCHAR, COLUMN2 VARCHAR)');
      this.validateIdentity('CREATE TABLE IDENTIFIER($foo) (col1 VARCHAR, col2 VARCHAR)');
      this.validateIdentity('CREATE TAG cost_center ALLOWED_VALUES \'a\', \'b\'');
      (this.validateIdentity('CREATE WAREHOUSE x').args.this as Expression).assertIs(IdentifierExpr);
      (this.validateIdentity('CREATE STREAMLIT x').args.this as Expression).assertIs(IdentifierExpr);
      (this.validateIdentity(
        'CREATE TEMPORARY STAGE stage1 FILE_FORMAT=(TYPE=PARQUET)',
      ).args.this as Expression).assertIs(TableExpr);
      this.validateIdentity(
        'CREATE STAGE stage1 FILE_FORMAT=\'format1\'',
        'CREATE STAGE stage1 FILE_FORMAT=(FORMAT_NAME=\'format1\')',
      );
      this.validateIdentity('CREATE STAGE stage1 FILE_FORMAT=(FORMAT_NAME=stage1.format1)');
      this.validateIdentity('CREATE STAGE stage1 FILE_FORMAT=(FORMAT_NAME=\'stage1.format1\')');
      this.validateIdentity(
        'CREATE STAGE stage1 FILE_FORMAT=schema1.format1',
        'CREATE STAGE stage1 FILE_FORMAT=(FORMAT_NAME=schema1.format1)',
      );
      expect(() => {
        this.parseOne('CREATE STAGE stage1 FILE_FORMAT=123', {
          read: 'snowflake',
        });
      }).toThrow(ParseError);
      this.validateIdentity(
        'CREATE STAGE s1 URL=\'s3://bucket-123\' FILE_FORMAT=(TYPE=\'JSON\') CREDENTIALS=(aws_key_id=\'test\' aws_secret_key=\'test\')',
      );
      (this.validateIdentity(
        'CREATE OR REPLACE TAG IF NOT EXISTS cost_center COMMENT=\'cost_center tag\'',
      ).args.this as Expression).assertIs(IdentifierExpr);
      (this.validateIdentity(
        'CREATE TEMPORARY FILE FORMAT fileformat1 TYPE=PARQUET COMPRESSION=auto',
      ).args.this as Expression).assertIs(TableExpr);
      this.validateIdentity(
        'CREATE DYNAMIC TABLE product (pre_tax_profit, taxes, after_tax_profit) TARGET_LAG=\'20 minutes\' WAREHOUSE=mywh AS SELECT revenue - cost, (revenue - cost) * tax_rate, (revenue - cost) * (1.0 - tax_rate) FROM staging_table',
      );
      this.validateIdentity(
        'ALTER TABLE db_name.schmaName.tblName ADD COLUMN_1 VARCHAR NOT NULL TAG (key1=\'value_1\')',
      );
      this.validateIdentity(
        'DROP FUNCTION my_udf (OBJECT(city VARCHAR, zipcode DECIMAL(38, 0), val ARRAY(BOOLEAN)))',
      );
      this.validateIdentity(
        'CREATE TABLE orders_clone_restore CLONE orders AT (TIMESTAMP => TO_TIMESTAMP_TZ(\'04/05/2013 01:02:03\', \'mm/dd/yyyy hh24:mi:ss\'))',
      );
      this.validateIdentity(
        'CREATE TABLE orders_clone_restore CLONE orders BEFORE (STATEMENT => \'8e5d0ca9-005e-44e6-b858-a8f5b37c5726\')',
      );
      this.validateIdentity(
        'CREATE SCHEMA mytestschema_clone_restore CLONE testschema BEFORE (TIMESTAMP => TO_TIMESTAMP(40 * 365 * 86400))',
      );
      this.validateIdentity(
        'CREATE OR REPLACE TABLE EXAMPLE_DB.DEMO.USERS (ID DECIMAL(38, 0) NOT NULL, PRIMARY KEY (ID), FOREIGN KEY (CITY_CODE) REFERENCES EXAMPLE_DB.DEMO.CITIES (CITY_CODE))',
      );
      this.validateIdentity(
        'CREATE ICEBERG TABLE my_iceberg_table (amount ARRAY(INT)) CATALOG=\'SNOWFLAKE\' EXTERNAL_VOLUME=\'my_external_volume\' BASE_LOCATION=\'my/relative/path/from/extvol\'',
      );
      this.validateIdentity(
        'CREATE OR REPLACE FUNCTION ibis_udfs.public.object_values("obj" OBJECT) RETURNS ARRAY LANGUAGE JAVASCRIPT RETURNS NULL ON NULL INPUT AS \' return Object.values(obj) \'',
      );
      this.validateIdentity(
        'CREATE OR REPLACE FUNCTION ibis_udfs.public.object_values("obj" OBJECT) RETURNS ARRAY LANGUAGE JAVASCRIPT STRICT AS \' return Object.values(obj) \'',
      );
      this.validateIdentity(
        'CREATE OR REPLACE TABLE TEST (SOME_REF DECIMAL(38, 0) NOT NULL FOREIGN KEY REFERENCES SOME_OTHER_TABLE (ID))',
      );
      this.validateIdentity(
        'CREATE OR REPLACE FUNCTION my_udf(location OBJECT(city VARCHAR, zipcode DECIMAL(38, 0), val ARRAY(BOOLEAN))) RETURNS VARCHAR AS $$ SELECT \'foo\' $$',
        'CREATE OR REPLACE FUNCTION my_udf(location OBJECT(city VARCHAR, zipcode DECIMAL(38, 0), val ARRAY(BOOLEAN))) RETURNS VARCHAR AS \' SELECT \\\'foo\\\' \'',
      );
      this.validateIdentity(
        'CREATE OR REPLACE FUNCTION my_udtf(foo BOOLEAN) RETURNS TABLE(col1 ARRAY(INT)) AS $$ WITH t AS (SELECT CAST([1, 2, 3] AS ARRAY(INT)) AS c) SELECT c FROM t $$',
        'CREATE OR REPLACE FUNCTION my_udtf(foo BOOLEAN) RETURNS TABLE (col1 ARRAY(INT)) AS \' WITH t AS (SELECT CAST([1, 2, 3] AS ARRAY(INT)) AS c) SELECT c FROM t \'',
      );
      this.validateIdentity(
        'CREATE SEQUENCE seq1 WITH START=1, INCREMENT=1 ORDER',
        'CREATE SEQUENCE seq1 START WITH 1 INCREMENT BY 1 ORDER',
      );
      this.validateIdentity(
        'CREATE SEQUENCE seq1 WITH START=1 INCREMENT=1 ORDER',
        'CREATE SEQUENCE seq1 START WITH 1 INCREMENT BY 1 ORDER',
      );
      this.validateIdentity(
        `create external table et2(
  col1 date as (parse_json(metadata$external_table_partition):COL1::date),
  col2 varchar as (parse_json(metadata$external_table_partition):COL2::varchar),
  col3 number as (parse_json(metadata$external_table_partition):COL3::number))
  partition by (col1,col2,col3)
  location=@s2/logs/
  partition_type = user_specified
  file_format = (type = parquet compression = gzip binary_as_text = false)`,
        'CREATE EXTERNAL TABLE et2 (col1 DATE AS (CAST(GET_PATH(PARSE_JSON(metadata$external_table_partition), \'COL1\') AS DATE)), col2 VARCHAR AS (CAST(GET_PATH(PARSE_JSON(metadata$external_table_partition), \'COL2\') AS VARCHAR)), col3 DECIMAL(38, 0) AS (CAST(GET_PATH(PARSE_JSON(metadata$external_table_partition), \'COL3\') AS DECIMAL(38, 0)))) PARTITION BY (col1, col2, col3) LOCATION=@s2/logs/ partition_type=user_specified FILE_FORMAT=(type=parquet compression=gzip binary_as_text=FALSE)',
      );

      this.validateAll(
        'CREATE TABLE orders_clone CLONE orders',
        {
          read: {
            'bigquery': 'CREATE TABLE orders_clone CLONE orders',
          },
          write: {
            'bigquery': 'CREATE TABLE orders_clone CLONE orders',
            'snowflake': 'CREATE TABLE orders_clone CLONE orders',
          },
        },
      );
      this.validateAll(
        'CREATE OR REPLACE TRANSIENT TABLE a (id INT)',
        {
          read: {
            'postgres': 'CREATE OR REPLACE TRANSIENT TABLE a (id INT)',
            'snowflake': 'CREATE OR REPLACE TRANSIENT TABLE a (id INT)',
          },
          write: {
            'postgres': 'CREATE OR REPLACE TABLE a (id INT)',
            'mysql': 'CREATE OR REPLACE TABLE a (id INT)',
            'snowflake': 'CREATE OR REPLACE TRANSIENT TABLE a (id INT)',
          },
        },
      );
      this.validateAll(
        'CREATE TABLE a (b INT)',
        {
          read: {
            'teradata': 'CREATE MULTISET TABLE a (b INT)',
          },
          write: {
            'snowflake': 'CREATE TABLE a (b INT)',
          },
        },
      );

      this.validateIdentity('CREATE TABLE a TAG (key1=\'value_1\', key2=\'value_2\')');
      this.validateAll(
        'CREATE TABLE a TAG (key1=\'value_1\')',
        {
          read: {
            'snowflake': 'CREATE TABLE a WITH TAG (key1=\'value_1\')',
          },
        },
      );

      for (const action of [
        'SET',
        'DROP',
      ]) {
        this.validateAll(
          `
           ALTER TABLE a
           ALTER COLUMN my_column ${action} NOT NULL;`,
          {
            write: {
              'snowflake': `ALTER TABLE a ALTER COLUMN my_column ${action} NOT NULL`,
              'duckdb': `ALTER TABLE a ALTER COLUMN my_column ${action} NOT NULL`,
              'postgres': `ALTER TABLE a ALTER COLUMN my_column ${action} NOT NULL`,
            },
          },
        );

      }
    }
  }

  testUserDefinedFunctions () {
    this.validateAll(
      'CREATE FUNCTION a(x DATE, y BIGINT) RETURNS ARRAY LANGUAGE JAVASCRIPT AS $$ SELECT 1 $$',
      {
        write: {
          'snowflake': 'CREATE FUNCTION a(x DATE, y BIGINT) RETURNS ARRAY LANGUAGE JAVASCRIPT AS \' SELECT 1 \'',
        },
      },
    );
    this.validateAll(
      'CREATE FUNCTION a() RETURNS TABLE (b INT) AS \'SELECT 1\'',
      {
        write: {
          'snowflake': 'CREATE FUNCTION a() RETURNS TABLE (b INT) AS \'SELECT 1\'',
          'bigquery': 'CREATE TABLE FUNCTION a() RETURNS TABLE <b INT64> AS SELECT 1',
        },
      },
    );
    this.validateAll(
      'CREATE FUNCTION a() RETURNS INT IMMUTABLE AS \'SELECT 1\'',
      {
        write: {
          'snowflake': 'CREATE FUNCTION a() RETURNS INT IMMUTABLE AS \'SELECT 1\'',
        },
      },
    );

  }

  testStoredProcedures () {
    this.validateIdentity('CALL a.b.c(x, y)', undefined, {
      checkCommandWarning: true,
    });
    this.validateIdentity(
      'CREATE PROCEDURE a.b.c(x INT, y VARIANT) RETURNS OBJECT EXECUTE AS CALLER AS \'BEGIN SELECT 1; END;\'',
    );

  }

  testTableFunction () {
    this.validateIdentity('SELECT * FROM TABLE(\'MYTABLE\')');
    this.validateIdentity('SELECT * FROM TABLE($MYVAR)');
    this.validateIdentity('SELECT * FROM TABLE(?)');
    this.validateIdentity('SELECT * FROM TABLE(:BINDING)');
    this.validateIdentity('SELECT * FROM TABLE($MYVAR) WHERE COL1 = 10');
    this.validateIdentity('SELECT * FROM TABLE(\'t1\') AS f');
    this.validateIdentity('SELECT * FROM (TABLE(\'t1\') CROSS JOIN TABLE(\'t2\'))');
    this.validateIdentity('SELECT * FROM TABLE(\'t1\'), LATERAL (SELECT * FROM t2)');
    this.validateIdentity('SELECT * FROM TABLE(\'t1\') UNION ALL SELECT * FROM TABLE(\'t2\')');
    this.validateIdentity('SELECT * FROM TABLE(\'t1\') TABLESAMPLE BERNOULLI (20.3)');
    this.validateIdentity('SELECT * FROM TABLE(\'MYDB."MYSCHEMA"."MYTABLE"\')');
    this.validateIdentity(
      'SELECT * FROM TABLE($$MYDB. "MYSCHEMA"."MYTABLE"$$)',
      'SELECT * FROM TABLE(\'MYDB. "MYSCHEMA"."MYTABLE"\')',
    );

  }

  testFlatten () {
    expect(select(new ExplodeExpr({
      this: column({
        col: 'x',
      }),
    }).as('y', {
      quoted: true,
    })).sql({
      dialect: 'snowflake',
      pretty: true,
    })).toBe(
      `SELECT
  IFF(_u.pos = _u_2.pos_2, _u_2."y", NULL) AS "y"
FROM TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, (
  GREATEST(ARRAY_SIZE(x)) - 1
) + 1))) AS _u(seq, key, path, index, pos, this)
CROSS JOIN TABLE(FLATTEN(INPUT => x)) AS _u_2(seq, key, path, pos_2, "y", this)
WHERE
  _u.pos = _u_2.pos_2
  OR (
    _u.pos > (
      ARRAY_SIZE(x) - 1
    ) AND _u_2.pos_2 = (
      ARRAY_SIZE(x) - 1
    )
  )`,
    );

    this.validateAll(
      `
            select
              dag_report.acct_id,
              dag_report.report_date,
              dag_report.report_uuid,
              dag_report.airflow_name,
              dag_report.dag_id,
              f.value::varchar as operator
            from cs.telescope.dag_report,
            table(flatten(input=>split(operators, ','))) f
            `,
      {
        write: {
          'snowflake': `SELECT
  dag_report.acct_id,
  dag_report.report_date,
  dag_report.report_uuid,
  dag_report.airflow_name,
  dag_report.dag_id,
  CAST(f.value AS VARCHAR) AS operator
FROM cs.telescope.dag_report, TABLE(FLATTEN(input => SPLIT(operators, ','))) AS f`,
        },
        pretty: true,
      },
    );
    this.validateAll(
      `
       SELECT
         uc.user_id,
         uc.start_ts AS ts,
         CASE
           WHEN uc.start_ts::DATE >= '2023-01-01' AND uc.country_code IN ('US') AND uc.user_id NOT IN (
             SELECT DISTINCT
               _id
             FROM
               users,
               LATERAL FLATTEN(INPUT => PARSE_JSON(flags)) datasource
             WHERE datasource.value:name = 'something'
           )
             THEN 'Sample1'
             ELSE 'Sample2'
         END AS entity
       FROM user_countries AS uc
       LEFT JOIN (
         SELECT user_id, MAX(IFF(service_entity IS NULL,1,0)) AS le_null
         FROM accepted_user_agreements
         GROUP BY 1
       ) AS aua
         ON uc.user_id = aua.user_id
       `,
      {
        write: {
          'snowflake': `SELECT
  uc.user_id,
  uc.start_ts AS ts,
  CASE
    WHEN CAST(uc.start_ts AS DATE) >= '2023-01-01'
    AND uc.country_code IN ('US')
    AND uc.user_id <> ALL (
      SELECT DISTINCT
        _id
      FROM users, LATERAL IFF(_u.pos = _u_2.pos_2, _u_2.entity, NULL) AS datasource(SEQ, KEY, PATH, INDEX, VALUE, THIS)
      WHERE
        GET_PATH(datasource.value, 'name') = 'something'
    )
    THEN 'Sample1'
    ELSE 'Sample2'
  END AS entity
FROM user_countries AS uc
LEFT JOIN (
  SELECT
    user_id,
    MAX(IFF(service_entity IS NULL, 1, 0)) AS le_null
  FROM accepted_user_agreements
  GROUP BY
    1
) AS aua
  ON uc.user_id = aua.user_id
CROSS JOIN TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, (
  GREATEST(ARRAY_SIZE(INPUT => PARSE_JSON(flags))) - 1
) + 1))) AS _u(seq, key, path, index, pos, this)
CROSS JOIN TABLE(FLATTEN(INPUT => PARSE_JSON(flags))) AS _u_2(seq, key, path, pos_2, entity, this)
WHERE
  _u.pos = _u_2.pos_2
  OR (
    _u.pos > (
      ARRAY_SIZE(INPUT => PARSE_JSON(flags)) - 1
    )
    AND _u_2.pos_2 = (
      ARRAY_SIZE(INPUT => PARSE_JSON(flags)) - 1
    )
  )`,
        },
        pretty: true,
      },
    );

    // All examples from https://docs.snowflake.com/en/sql-reference/functions/flatten.html#syntax
    this.validateAll(
      'SELECT * FROM TABLE(FLATTEN(input => parse_json(\'[1, ,77]\'))) f',
      {
        write: {
          'snowflake': 'SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON(\'[1, ,77]\'))) AS f',
        },
      },
    );

    this.validateAll(
      'SELECT * FROM TABLE(FLATTEN(input => parse_json(\'{"a":1, "b":[77,88]}\'), outer => true)) f',
      {
        write: {
          'snowflake': 'SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON(\'{"a":1, "b":[77,88]}\'), outer => TRUE)) AS f',
        },
      },
    );

    this.validateAll(
      'SELECT * FROM TABLE(FLATTEN(input => parse_json(\'{"a":1, "b":[77,88]}\'), path => \'b\')) f',
      {
        write: {
          'snowflake': 'SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON(\'{"a":1, "b":[77,88]}\'), path => \'b\')) AS f',
        },
      },
    );

    this.validateAll(
      'SELECT * FROM TABLE(FLATTEN(input => parse_json(\'[]\'))) f',
      {
        write: {
          'snowflake': 'SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON(\'[]\'))) AS f',
        },
      },
    );

    this.validateAll(
      'SELECT * FROM TABLE(FLATTEN(input => parse_json(\'[]\'), outer => true)) f',
      {
        write: {
          'snowflake': 'SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON(\'[]\'), outer => TRUE)) AS f',
        },
      },
    );

    this.validateAll(
      'SELECT * FROM TABLE(FLATTEN(input => parse_json(\'{"a":1, "b":[77,88], "c": {"d":"X"}}\'))) f',
      {
        write: {
          'snowflake': 'SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON(\'{"a":1, "b":[77,88], "c": {"d":"X"}}\'))) AS f',
        },
      },
    );

    this.validateAll(
      'SELECT * FROM TABLE(FLATTEN(input => parse_json(\'{"a":1, "b":[77,88], "c": {"d":"X"}}\'), recursive => true)) f',
      {
        write: {
          'snowflake': 'SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON(\'{"a":1, "b":[77,88], "c": {"d":"X"}}\'), recursive => TRUE)) AS f',
        },
      },
    );

    this.validateAll(
      'SELECT * FROM TABLE(FLATTEN(input => parse_json(\'{"a":1, "b":[77,88], "c": {"d":"X"}}\'), recursive => true, mode => \'object\')) f',
      {
        write: {
          'snowflake': 'SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON(\'{"a":1, "b":[77,88], "c": {"d":"X"}}\'), recursive => TRUE, mode => \'object\')) AS f',
        },
      },
    );

    this.validateAll(
      `
       SELECT id as "ID",
         f.value AS "Contact",
         f1.value:type AS "Type",
         f1.value:content AS "Details"
       FROM persons p,
         lateral flatten(input => p.c, path => 'contact') f,
         lateral flatten(input => f.value:business) f1
       `,
      {
        write: {
          'snowflake': `SELECT
  id AS "ID",
  f.value AS "Contact",
  GET_PATH(f1.value, 'type') AS "Type",
  GET_PATH(f1.value, 'content') AS "Details"
FROM persons AS p, LATERAL FLATTEN(input => p.c, path => 'contact') AS f(SEQ, KEY, PATH, INDEX, VALUE, THIS), LATERAL FLATTEN(input => GET_PATH(f.value, 'business')) AS f1(SEQ, KEY, PATH, INDEX, VALUE, THIS)`,
        },
        pretty: true,
      },
    );

    this.validateAll(
      `
            SELECT id as "ID",
              value AS "Contact"
            FROM persons p,
              lateral flatten(input => p.c, path => 'contact')
            `,
      {
        write: {
          'snowflake': `SELECT
  id AS "ID",
  value AS "Contact"
FROM persons AS p, LATERAL FLATTEN(input => p.c, path => 'contact') AS _flattened(SEQ, KEY, PATH, INDEX, VALUE, THIS)`,
        },
        pretty: true,
      },
    );

  }

  testMinus () {
    this.validateAll(
      'SELECT 1 EXCEPT SELECT 1',
      {
        read: {
          'oracle': 'SELECT 1 MINUS SELECT 1',
          'snowflake': 'SELECT 1 MINUS SELECT 1',
        },
      },
    );

  }

  testValues () {
    const selectExpr = select('*').from('values (map([\'a\'], [1]))');

    expect(selectExpr.sql({
      dialect: 'snowflake',
    })).toBe('SELECT * FROM (SELECT OBJECT_CONSTRUCT(\'a\', 1))');

    this.validateAll(
      'SELECT "c0", "c1" FROM (VALUES (1, 2), (3, 4)) AS "t0"("c0", "c1")',
      {
        read: {
          'spark': 'SELECT `c0`, `c1` FROM (VALUES (1, 2), (3, 4)) AS `t0`(`c0`, `c1`)',
        },
      },
    );
    this.validateAll(
      'SELECT $1 AS "_1" FROM VALUES (\'a\'), (\'b\')',
      {
        write: {
          'snowflake': 'SELECT $1 AS "_1" FROM (VALUES (\'a\'), (\'b\'))',
          'spark': 'SELECT ${1} AS `_1` FROM VALUES (\'a\'), (\'b\')',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM (SELECT OBJECT_CONSTRUCT(\'a\', 1) AS x) AS t',
      {
        read: {
          'duckdb': 'SELECT * FROM (VALUES ({\'a\': 1})) AS t(x)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM (SELECT OBJECT_CONSTRUCT(\'a\', 1) AS x UNION ALL SELECT OBJECT_CONSTRUCT(\'a\', 2)) AS t',
      {
        read: {
          'duckdb': 'SELECT * FROM (VALUES ({\'a\': 1}), ({\'a\': 2})) AS t(x)',
        },
      },
    );

  }

  testDescribe () {
    this.validateIdentity('DESCRIBE SEMANTIC VIEW TPCDS_SEMANTIC_VIEW_SM');
    this.validateIdentity(
      'DESC SEMANTIC VIEW TPCDS_SEMANTIC_VIEW_SM',
      'DESCRIBE SEMANTIC VIEW TPCDS_SEMANTIC_VIEW_SM',
    );

    this.validateAll(
      'DESCRIBE TABLE db.table',
      {
        write: {
          'snowflake': 'DESCRIBE TABLE db.table',
          'spark': 'DESCRIBE db.table',
        },
      },
    );
    this.validateAll(
      'DESCRIBE db.table',
      {
        write: {
          'snowflake': 'DESCRIBE TABLE db.table',
          'spark': 'DESCRIBE db.table',
        },
      },
    );
    this.validateAll(
      'DESC TABLE db.table',
      {
        write: {
          'snowflake': 'DESCRIBE TABLE db.table',
          'spark': 'DESCRIBE db.table',
        },
      },
    );
    this.validateAll(
      'DESC VIEW db.table',
      {
        write: {
          'snowflake': 'DESCRIBE VIEW db.table',
          'spark': 'DESCRIBE db.table',
        },
      },
    );
    this.validateAll(
      'ENDSWITH(\'abc\', \'c\')',
      {
        read: {
          'bigquery': 'ENDS_WITH(\'abc\', \'c\')',
          'clickhouse': 'endsWith(\'abc\', \'c\')',
          'databricks': 'ENDSWITH(\'abc\', \'c\')',
          'duckdb': 'ENDS_WITH(\'abc\', \'c\')',
          'presto': 'ENDS_WITH(\'abc\', \'c\')',
          'spark': 'ENDSWITH(\'abc\', \'c\')',
        },
        write: {
          'bigquery': 'ENDS_WITH(\'abc\', \'c\')',
          'clickhouse': 'endsWith(\'abc\', \'c\')',
          'databricks': 'ENDSWITH(\'abc\', \'c\')',
          'duckdb': 'ENDS_WITH(\'abc\', \'c\')',
          'presto': 'ENDS_WITH(\'abc\', \'c\')',
          'snowflake': 'ENDSWITH(\'abc\', \'c\')',
          'spark': 'ENDSWITH(\'abc\', \'c\')',
        },
      },
    );

  }

  testParseLikeAny () {
    for (const keyword of [
      'LIKE',
      'ILIKE',
    ]) {
      const ast = this.validateIdentity(`a ${keyword} ANY FUN('foo')`);

      ast.sql();  // check that this doesn't raise
    }
  }

  testRegexpSubstr () {
    this.validateAll(
      'REGEXP_SUBSTR(subject, pattern, pos, occ, params, group)',
      {
        write: {
          'bigquery': 'REGEXP_EXTRACT(subject, pattern, pos, occ)',
          'hive': 'REGEXP_EXTRACT(subject, pattern, group)',
          'presto': 'REGEXP_EXTRACT(subject, pattern, "group")',
          'snowflake': 'REGEXP_SUBSTR(subject, pattern, pos, occ, params, group)',
          'spark': 'REGEXP_EXTRACT(subject, pattern, group)',
        },
      },
    );
    this.validateAll(
      'REGEXP_SUBSTR(subject, pattern)',
      {
        read: {
          'bigquery': 'REGEXP_EXTRACT(subject, pattern)',
        },
        write: {
          'bigquery': 'REGEXP_EXTRACT(subject, pattern)',
          'snowflake': 'REGEXP_SUBSTR(subject, pattern)',
        },
      },
    );
    this.validateAll(
      'REGEXP_SUBSTR(subject, pattern, 1, 1, \'c\', 1)',
      {
        read: {
          'hive': 'REGEXP_EXTRACT(subject, pattern)',
          'spark2': 'REGEXP_EXTRACT(subject, pattern)',
          'spark': 'REGEXP_EXTRACT(subject, pattern)',
          'databricks': 'REGEXP_EXTRACT(subject, pattern)',
        },
        write: {
          'hive': 'REGEXP_EXTRACT(subject, pattern)',
          'spark2': 'REGEXP_EXTRACT(subject, pattern)',
          'spark': 'REGEXP_EXTRACT(subject, pattern)',
          'databricks': 'REGEXP_EXTRACT(subject, pattern)',
          'snowflake': 'REGEXP_SUBSTR(subject, pattern, 1, 1, \'c\', 1)',
        },
      },
    );
    this.validateAll(
      'REGEXP_SUBSTR(subject, pattern, 1, 1, \'c\', group)',
      {
        read: {
          'duckdb': 'REGEXP_EXTRACT(subject, pattern, group)',
          'hive': 'REGEXP_EXTRACT(subject, pattern, group)',
          'presto': 'REGEXP_EXTRACT(subject, pattern, group)',
          'snowflake': 'REGEXP_SUBSTR(subject, pattern, 1, 1, \'c\', group)',
          'spark': 'REGEXP_EXTRACT(subject, pattern, group)',
        },
      },
    );

    this.validateIdentity(
      'REGEXP_SUBSTR_ALL(subject, pattern, pos, occ, param, group)',
      'REGEXP_SUBSTR_ALL(subject, pattern, pos, occ, param, group)',
    );

    this.validateIdentity('SELECT SEARCH((play, line), \'dream\')');
    this.validateIdentity('SELECT SEARCH(line, \'king\', ANALYZER => \'UNICODE_ANALYZER\')');
    this.validateIdentity('SELECT SEARCH(character, \'king queen\', SEARCH_MODE => \'AND\')');
    this.validateIdentity(
      'SELECT SEARCH(line, \'king\', ANALYZER => \'UNICODE_ANALYZER\', SEARCH_MODE => \'OR\')',
    );

    // AST validation tests - verify argument mapping
    let ast = this.validateIdentity('SELECT SEARCH(line, \'king\')');

    let searchAst = ast.find(SearchExpr);

    expect(Object.keys(searchAst!.args).filter((k) => (searchAst!.args as Record<string, unknown>)[k] !== undefined)).toEqual([
      'this',
      'expression',
    ]);

    expect(searchAst?.getArgKey('analyzer')).toBeUndefined();
    expect(searchAst?.getArgKey('searchMode')).toBeUndefined();

    ast = this.validateIdentity('SELECT SEARCH(line, \'king\', ANALYZER => \'UNICODE_ANALYZER\')');
    searchAst = ast.find(SearchExpr);
    expect(searchAst?.getArgKey('analyzer')).toBeDefined();
    expect(searchAst?.getArgKey('searchMode')).toBeUndefined();

    ast = this.validateIdentity('SELECT SEARCH(character, \'king queen\', SEARCH_MODE => \'AND\')');
    searchAst = ast.find(SearchExpr);
    expect(searchAst?.getArgKey('analyzer')).toBeUndefined();
    expect(searchAst?.getArgKey('searchMode')).toBeDefined();

    // Test with arguments in different order (searchMode first, then analyzer)
    ast = this.validateIdentity(
      'SELECT SEARCH(line, \'king\', SEARCH_MODE => \'AND\', ANALYZER => \'PATTERN_ANALYZER\')',
      'SELECT SEARCH(line, \'king\', ANALYZER => \'PATTERN_ANALYZER\', SEARCH_MODE => \'AND\')',
    );
    searchAst = ast.find(SearchExpr);
    expect(Object.keys(searchAst!.args).filter((k) => (searchAst!.args as Record<string, unknown>)[k] !== undefined)).toEqual([
      'this',
      'expression',
      'searchMode',
      'analyzer',
    ]);
    expect(searchAst?.getArgKey('analyzer')).toBeDefined();
    expect(searchAst?.getArgKey('searchMode')).toBeDefined();

    (this.validateIdentity('SELECT SEARCH_IP(col, \'192.168.0.0\')') as SelectExpr).selects[0].assertIs(
      SearchIpExpr,
    );

    this.validateIdentity('SELECT REGEXP_COUNT(\'hello world\', \'l \')');
    this.validateIdentity('SELECT REGEXP_COUNT(\'hello world\', \'l\', 1)');
    this.validateIdentity('SELECT REGEXP_COUNT(\'hello world\', \'l\', 1, \'i\')');
  }

  testRegexpReplace () {
    this.validateAll(
      'REGEXP_REPLACE(subject, pattern)',
      {
        write: {
          'bigquery': 'REGEXP_REPLACE(subject, pattern, \'\')',
          'duckdb': 'REGEXP_REPLACE(subject, pattern, \'\', \'g\')',
          'hive': 'REGEXP_REPLACE(subject, pattern, \'\')',
          'snowflake': 'REGEXP_REPLACE(subject, pattern, \'\')',
          'spark': 'REGEXP_REPLACE(subject, pattern, \'\')',
        },
      },
    );
    this.validateAll(
      'REGEXP_REPLACE(subject, pattern, replacement)',
      {
        read: {
          'bigquery': 'REGEXP_REPLACE(subject, pattern, replacement)',
          'duckdb': 'REGEXP_REPLACE(subject, pattern, replacement)',
          'hive': 'REGEXP_REPLACE(subject, pattern, replacement)',
          'spark': 'REGEXP_REPLACE(subject, pattern, replacement)',
        },
        write: {
          'bigquery': 'REGEXP_REPLACE(subject, pattern, replacement)',
          'duckdb': 'REGEXP_REPLACE(subject, pattern, replacement, \'g\')',
          'postgres': 'REGEXP_REPLACE(subject, pattern, replacement, \'g\')',
          'hive': 'REGEXP_REPLACE(subject, pattern, replacement)',
          'snowflake': 'REGEXP_REPLACE(subject, pattern, replacement)',
          'spark': 'REGEXP_REPLACE(subject, pattern, replacement)',
        },
      },
    );
    this.validateAll(
      'REGEXP_REPLACE(subject, pattern, replacement, position)',
      {
        read: {
          'spark': 'REGEXP_REPLACE(subject, pattern, replacement, position)',
        },
        write: {
          'bigquery': 'REGEXP_REPLACE(subject, pattern, replacement)',
          'duckdb': 'REGEXP_REPLACE(subject, pattern, replacement, \'g\')',
          'postgres': 'REGEXP_REPLACE(subject, pattern, replacement, position, \'g\')',
          'hive': 'REGEXP_REPLACE(subject, pattern, replacement)',
          'snowflake': 'REGEXP_REPLACE(subject, pattern, replacement, position)',
          'spark': 'REGEXP_REPLACE(subject, pattern, replacement, position)',
        },
      },
    );
    this.validateAll(
      'REGEXP_REPLACE(subject, pattern, replacement, position, occurrence, \'c\')',
      {
        write: {
          'bigquery': 'REGEXP_REPLACE(subject, pattern, replacement)',
          'duckdb': 'REGEXP_REPLACE(subject, pattern, replacement, \'c\')',
          'postgres': 'REGEXP_REPLACE(subject, pattern, replacement, position, occurrence, \'c\')',
          'hive': 'REGEXP_REPLACE(subject, pattern, replacement)',
          'snowflake': 'REGEXP_REPLACE(subject, pattern, replacement, position, occurrence, \'c\')',
          'spark': 'REGEXP_REPLACE(subject, pattern, replacement, position)',
        },
      },
    );

    this.validateAll(
      'REGEXP_REPLACE(subject, pattern, replacement, 1, 0, \'c\')',
      {
        write: {
          'snowflake': 'REGEXP_REPLACE(subject, pattern, replacement, 1, 0, \'c\')',
          'duckdb': 'REGEXP_REPLACE(subject, pattern, replacement, \'cg\')',
          'postgres': 'REGEXP_REPLACE(subject, pattern, replacement, 1, 0, \'cg\')',
        },
      },
    );

  }

  testReplace () {
    this.validateAll(
      'REPLACE(subject, pattern)',
      {
        write: {
          'bigquery': 'REPLACE(subject, pattern, \'\')',
          'duckdb': 'REPLACE(subject, pattern, \'\')',
          'hive': 'REPLACE(subject, pattern, \'\')',
          'snowflake': 'REPLACE(subject, pattern, \'\')',
          'spark': 'REPLACE(subject, pattern, \'\')',
        },
      },
    );
    this.validateAll(
      'REPLACE(subject, pattern, replacement)',
      {
        read: {
          'bigquery': 'REPLACE(subject, pattern, replacement)',
          'duckdb': 'REPLACE(subject, pattern, replacement)',
          'hive': 'REPLACE(subject, pattern, replacement)',
          'spark': 'REPLACE(subject, pattern, replacement)',
        },
        write: {
          'bigquery': 'REPLACE(subject, pattern, replacement)',
          'duckdb': 'REPLACE(subject, pattern, replacement)',
          'hive': 'REPLACE(subject, pattern, replacement)',
          'snowflake': 'REPLACE(subject, pattern, replacement)',
          'spark': 'REPLACE(subject, pattern, replacement)',
        },
      },
    );

  }

  testMatchRecognize () {
    for (const windowFrame of [
      '',
      'FINAL ',
      'RUNNING ',
    ]) {
      for (const row of [
        'ONE ROW PER MATCH',
        'ALL ROWS PER MATCH',
        'ALL ROWS PER MATCH SHOW EMPTY MATCHES',
        'ALL ROWS PER MATCH OMIT EMPTY MATCHES',
        'ALL ROWS PER MATCH WITH UNMATCHED ROWS',
      ]) {
        for (const after of [
          'AFTER MATCH SKIP',
          'AFTER MATCH SKIP PAST LAST ROW',
          'AFTER MATCH SKIP TO NEXT ROW',
          'AFTER MATCH SKIP TO FIRST x',
          'AFTER MATCH SKIP TO LAST x',
        ]) {
          this.validateIdentity(
            `SELECT
  *
FROM x
MATCH_RECOGNIZE (
  PARTITION BY a, b
  ORDER BY
    x DESC
  MEASURES
    ${windowFrame}y AS b
  ${row}
  ${after}
  PATTERN (^ S1 S2*? ( {{- S3 -}} S4 )+ | PERMUTE(S1, S2){{1,2}} $)
  DEFINE
    x AS y
)`,
            undefined,
            {
              pretty: true,
            },
          );

        }
      }
    }
  }

  testShowUsers () {
    this.validateIdentity('SHOW USERS');
    this.validateIdentity('SHOW TERSE USERS');
    this.validateIdentity('SHOW USERS LIKE \'_foo%\' STARTS WITH \'bar\' LIMIT 5 FROM \'baz\'');

  }

  testShowDatabases () {
    this.validateIdentity('SHOW TERSE DATABASES');
    this.validateIdentity(
      'SHOW TERSE DATABASES HISTORY LIKE \'foo\' STARTS WITH \'bla\' LIMIT 5 FROM \'bob\' WITH PRIVILEGES USAGE, MODIFY',
    );

    const ast = parseOne('SHOW DATABASES IN ACCOUNT', {
      read: 'snowflake',
    });

    expect(ast.args.this).toBe('DATABASES');
    expect(ast.getArgKey('scopeKind')).toBe('ACCOUNT');

  }

  testShowFileFormats () {
    this.validateIdentity('SHOW FILE FORMATS');
    this.validateIdentity('SHOW FILE FORMATS LIKE \'foo\' IN DATABASE db1');
    this.validateIdentity('SHOW FILE FORMATS LIKE \'foo\' IN SCHEMA db1.schema1');

    const ast = parseOne('SHOW FILE FORMATS IN ACCOUNT', {
      read: 'snowflake',
    });

    expect(ast.args.this).toBe('FILE FORMATS');
    expect(ast.getArgKey('scopeKind')).toBe('ACCOUNT');

  }

  testShowFunctions () {
    this.validateIdentity('SHOW FUNCTIONS');
    this.validateIdentity('SHOW FUNCTIONS LIKE \'foo\' IN CLASS bla');

    const ast = parseOne('SHOW FUNCTIONS IN ACCOUNT', {
      read: 'snowflake',
    });

    expect(ast.args.this).toBe('FUNCTIONS');
    expect(ast.getArgKey('scopeKind')).toBe('ACCOUNT');

  }

  testShowProcedures () {
    this.validateIdentity('SHOW PROCEDURES');
    this.validateIdentity('SHOW PROCEDURES LIKE \'foo\' IN APPLICATION app');
    this.validateIdentity('SHOW PROCEDURES LIKE \'foo\' IN APPLICATION PACKAGE pkg');

    const ast = parseOne('SHOW PROCEDURES IN ACCOUNT', {
      read: 'snowflake',
    });

    expect(ast.args.this).toBe('PROCEDURES');
    expect(ast.getArgKey('scopeKind')).toBe('ACCOUNT');

  }

  testShowStages () {
    this.validateIdentity('SHOW STAGES');
    this.validateIdentity('SHOW STAGES LIKE \'foo\' IN DATABASE db1');
    this.validateIdentity('SHOW STAGES LIKE \'foo\' IN SCHEMA db1.schema1');

    const ast = parseOne('SHOW STAGES IN ACCOUNT', {
      read: 'snowflake',
    });

    expect(ast.args.this).toBe('STAGES');
    expect(ast.getArgKey('scopeKind')).toBe('ACCOUNT');

  }

  testShowWarehouses () {
    this.validateIdentity('SHOW WAREHOUSES');
    this.validateIdentity('SHOW WAREHOUSES LIKE \'foo\' WITH PRIVILEGES USAGE, MODIFY');

    const ast = parseOne('SHOW WAREHOUSES', {
      read: 'snowflake',
    });

    expect(ast.args.this).toBe('WAREHOUSES');

  }

  testShowSchemas () {
    this.validateIdentity(
      'show terse schemas in database db1 starts with \'a\' limit 10 from \'b\'',
      'SHOW TERSE SCHEMAS IN DATABASE db1 STARTS WITH \'a\' LIMIT 10 FROM \'b\'',
    );

    const ast = parseOne('SHOW SCHEMAS IN DATABASE db1', {
      read: 'snowflake',
    });

    expect(ast.getArgKey('scopeKind')).toBe('DATABASE');
    expect(ast.find(TableExpr)?.sql({
      dialect: 'snowflake',
    })).toBe('db1');

  }

  testShowObjects () {
    this.validateIdentity(
      'show terse objects in schema db1.schema1 starts with \'a\' limit 10 from \'b\'',
      'SHOW TERSE OBJECTS IN SCHEMA db1.schema1 STARTS WITH \'a\' LIMIT 10 FROM \'b\'',
    );
    this.validateIdentity(
      'show terse objects in db1.schema1 starts with \'a\' limit 10 from \'b\'',
      'SHOW TERSE OBJECTS IN SCHEMA db1.schema1 STARTS WITH \'a\' LIMIT 10 FROM \'b\'',
    );

    const ast = parseOne('SHOW OBJECTS IN db1.schema1', {
      read: 'snowflake',
    });

    expect(ast.getArgKey('scopeKind')).toBe('SCHEMA');
    expect(ast.find(TableExpr)?.sql({
      dialect: 'snowflake',
    })).toBe('db1.schema1');
  }

  testShowColumns () {
    this.validateIdentity('SHOW COLUMNS');
    this.validateIdentity('SHOW COLUMNS IN TABLE dt_test');
    this.validateIdentity('SHOW COLUMNS LIKE \'_foo%\' IN TABLE dt_test');
    this.validateIdentity('SHOW COLUMNS IN VIEW');
    this.validateIdentity('SHOW COLUMNS LIKE \'_foo%\' IN VIEW dt_test');

    const ast = parseOne('SHOW COLUMNS LIKE \'_testing%\' IN dt_test', {
      read: 'snowflake',
    });

    expect(ast.find(TableExpr)?.sql({
      dialect: 'snowflake',
    })).toBe('dt_test');
    expect(ast.find(LiteralExpr)?.sql({
      dialect: 'snowflake',
    })).toBe('\'_testing%\'');

  }

  testShowTables () {
    this.validateIdentity(
      'SHOW TABLES LIKE \'line%\' IN tpch.public',
      'SHOW TABLES LIKE \'line%\' IN SCHEMA tpch.public',
    );
    this.validateIdentity(
      'SHOW TABLES HISTORY IN tpch.public',
      'SHOW TABLES HISTORY IN SCHEMA tpch.public',
    );
    this.validateIdentity(
      'show terse tables in schema db1.schema1 starts with \'a\' limit 10 from \'b\'',
      'SHOW TERSE TABLES IN SCHEMA db1.schema1 STARTS WITH \'a\' LIMIT 10 FROM \'b\'',
    );
    this.validateIdentity(
      'show terse tables in db1.schema1 starts with \'a\' limit 10 from \'b\'',
      'SHOW TERSE TABLES IN SCHEMA db1.schema1 STARTS WITH \'a\' LIMIT 10 FROM \'b\'',
    );

    const ast = parseOne('SHOW TABLES IN db1.schema1', {
      read: 'snowflake',
    });

    expect(ast.find(TableExpr)?.sql({
      dialect: 'snowflake',
    })).toBe('db1.schema1');
  }

  testShowPrimaryKeys () {
    this.validateIdentity('SHOW PRIMARY KEYS');
    this.validateIdentity('SHOW PRIMARY KEYS IN ACCOUNT');
    this.validateIdentity('SHOW PRIMARY KEYS IN DATABASE');
    this.validateIdentity('SHOW PRIMARY KEYS IN DATABASE foo');
    this.validateIdentity('SHOW PRIMARY KEYS IN TABLE');
    this.validateIdentity('SHOW PRIMARY KEYS IN TABLE foo');
    this.validateIdentity(
      'SHOW PRIMARY KEYS IN "TEST"."PUBLIC"."foo"',
      'SHOW PRIMARY KEYS IN TABLE "TEST"."PUBLIC"."foo"',
    );
    this.validateIdentity(
      'SHOW TERSE PRIMARY KEYS IN "TEST"."PUBLIC"."foo"',
      'SHOW PRIMARY KEYS IN TABLE "TEST"."PUBLIC"."foo"',
    );

    const ast = parseOne('SHOW PRIMARY KEYS IN "TEST"."PUBLIC"."foo"', {
      read: 'snowflake',
    });

    expect(ast.find(TableExpr)?.sql({
      dialect: 'snowflake',
    })).toBe('"TEST"."PUBLIC"."foo"');

  }

  testShowViews () {
    this.validateIdentity('SHOW TERSE VIEWS');
    this.validateIdentity('SHOW VIEWS');
    this.validateIdentity('SHOW VIEWS LIKE \'foo%\'');
    this.validateIdentity('SHOW VIEWS IN ACCOUNT');
    this.validateIdentity('SHOW VIEWS IN DATABASE');
    this.validateIdentity('SHOW VIEWS IN DATABASE foo');
    this.validateIdentity('SHOW VIEWS IN SCHEMA foo');
    this.validateIdentity(
      'SHOW VIEWS IN foo',
      'SHOW VIEWS IN SCHEMA foo',
    );

    const ast = parseOne('SHOW VIEWS IN db1.schema1', {
      read: 'snowflake',
    });

    expect(ast.find(TableExpr)?.sql({
      dialect: 'snowflake',
    })).toBe('db1.schema1');

  }

  testShowUniqueKeys () {
    this.validateIdentity('SHOW UNIQUE KEYS');
    this.validateIdentity('SHOW UNIQUE KEYS IN ACCOUNT');
    this.validateIdentity('SHOW UNIQUE KEYS IN DATABASE');
    this.validateIdentity('SHOW UNIQUE KEYS IN DATABASE foo');
    this.validateIdentity('SHOW UNIQUE KEYS IN TABLE');
    this.validateIdentity('SHOW UNIQUE KEYS IN TABLE foo');
    this.validateIdentity(
      'SHOW UNIQUE KEYS IN "TEST"."PUBLIC"."foo"',
      'SHOW UNIQUE KEYS IN SCHEMA "TEST"."PUBLIC"."foo"',
    );
    this.validateIdentity(
      'SHOW TERSE UNIQUE KEYS IN "TEST"."PUBLIC"."foo"',
      'SHOW UNIQUE KEYS IN SCHEMA "TEST"."PUBLIC"."foo"',
    );

    const ast = parseOne('SHOW UNIQUE KEYS IN "TEST"."PUBLIC"."foo"', {
      read: 'snowflake',
    });

    expect(ast.find(TableExpr)?.sql({
      dialect: 'snowflake',
    })).toBe('"TEST"."PUBLIC"."foo"');

  }

  testShowImportedKeys () {
    this.validateIdentity('SHOW IMPORTED KEYS');
    this.validateIdentity('SHOW IMPORTED KEYS IN ACCOUNT');
    this.validateIdentity('SHOW IMPORTED KEYS IN DATABASE');
    this.validateIdentity('SHOW IMPORTED KEYS IN DATABASE foo');
    this.validateIdentity('SHOW IMPORTED KEYS IN TABLE');
    this.validateIdentity('SHOW IMPORTED KEYS IN TABLE foo');
    this.validateIdentity(
      'SHOW IMPORTED KEYS IN "TEST"."PUBLIC"."foo"',
      'SHOW IMPORTED KEYS IN SCHEMA "TEST"."PUBLIC"."foo"',
    );
    this.validateIdentity(
      'SHOW TERSE IMPORTED KEYS IN "TEST"."PUBLIC"."foo"',
      'SHOW IMPORTED KEYS IN SCHEMA "TEST"."PUBLIC"."foo"',
    );

    const ast = parseOne('SHOW IMPORTED KEYS IN "TEST"."PUBLIC"."foo"', {
      read: 'snowflake',
    });

    expect(ast.find(TableExpr)?.sql({
      dialect: 'snowflake',
    })).toBe('"TEST"."PUBLIC"."foo"');

  }

  testShowSequences () {
    this.validateIdentity('SHOW TERSE SEQUENCES');
    this.validateIdentity('SHOW SEQUENCES');
    this.validateIdentity('SHOW SEQUENCES LIKE \'_foo%\' IN ACCOUNT');
    this.validateIdentity('SHOW SEQUENCES LIKE \'_foo%\' IN DATABASE');
    this.validateIdentity('SHOW SEQUENCES LIKE \'_foo%\' IN DATABASE foo');
    this.validateIdentity('SHOW SEQUENCES LIKE \'_foo%\' IN SCHEMA');
    this.validateIdentity('SHOW SEQUENCES LIKE \'_foo%\' IN SCHEMA foo');
    this.validateIdentity(
      'SHOW SEQUENCES LIKE \'_foo%\' IN foo',
      'SHOW SEQUENCES LIKE \'_foo%\' IN SCHEMA foo',
    );

    const ast = parseOne('SHOW SEQUENCES IN dt_test', {
      read: 'snowflake',
    });

    expect(ast.getArgKey('scopeKind')).toBe('SCHEMA');

  }

  testStorageIntegration () {
    (this.validateIdentity(
      `CREATE STORAGE INTEGRATION s3_int
TYPE=EXTERNAL_STAGE
STORAGE_PROVIDER='S3'
STORAGE_AWS_ROLE_ARN='arn:aws:iam::001234567890:role/myrole'
ENABLED=TRUE
STORAGE_ALLOWED_LOCATIONS=('s3://mybucket1/path1/', 's3://mybucket2/path2/')`,
      undefined,
      {
        pretty: true,
      },
    ).args.this as Expression).assertIs(IdentifierExpr);

  }

  testSwap () {
    const ast = parseOne('ALTER TABLE a SWAP WITH b', {
      read: 'snowflake',
    });

    expect(ast).toBeInstanceOf(AlterExpr);
    expect((ast.getArgKey('actions') as Expression[])[0]).toBeInstanceOf(SwapTableExpr);

  }

  testTryCast () {
    this.validateAll('TRY_CAST(\'foo\' AS VARCHAR)', {
      read: {
        'hive': 'CAST(\'foo\' AS STRING)',
      },
    });
    this.validateAll('CAST(5 + 5 AS VARCHAR)', {
      read: {
        'hive': 'CAST(5 + 5 AS STRING)',
      },
    });
    this.validateAll(
      'CAST(TRY_CAST(\'2020-01-01\' AS DATE) AS VARCHAR)',
      {
        read: {
          'hive': 'CAST(CAST(\'2020-01-01\' AS DATE) AS STRING)',
          'snowflake': 'CAST(TRY_CAST(\'2020-01-01\' AS DATE) AS VARCHAR)',
        },
      },
    );
    this.validateAll(
      'TRY_CAST(\'val\' AS VARCHAR)',
      {
        read: {
          'hive': 'CAST(\'val\' AS STRING)',
          'snowflake': 'TRY_CAST(\'val\' AS VARCHAR)',
        },
      },
    );
    this.validateIdentity('SELECT TRY_CAST(x AS DOUBLE)');
    this.validateIdentity('SELECT TRY_CAST(FOO() AS TEXT)', 'SELECT TRY_CAST(FOO() AS VARCHAR)');

    const expr = parseOne('SELECT CAST(t.x AS STRING) FROM t', {
      read: 'hive',
    });

    for (const valueType of [
      'string',
      'int',
    ]) {
      const func = valueType === 'string' ? 'TRY_CAST' : 'CAST';

      const annotated = annotateTypes(expr, {
        schema: {
          't': {
            'x': valueType,
          },
        },
      });

      expect(annotated.sql({
        dialect: 'snowflake',
      })).toBe(`SELECT ${func}(t.x AS VARCHAR) FROM t`);

    }
  }

  testDecfloat () {
    this.validateAll(
      'SELECT CAST(1.5 AS DECFLOAT)',
      {
        write: {
          'snowflake': 'SELECT CAST(1.5 AS DECFLOAT)',
          'duckdb': 'SELECT CAST(1.5 AS DECIMAL(38, 5))',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE t (x DECFLOAT)',
      {
        write: {
          'snowflake': 'CREATE TABLE t (x DECFLOAT)',
          'duckdb': 'CREATE TABLE t (x DECIMAL(38, 5))',
        },
      },
    );

  }

  testCopy () {
    this.validateIdentity('COPY INTO test (c1) FROM (SELECT $1.c1 FROM @mystage)');
    this.validateIdentity(
      'COPY INTO temp FROM @random_stage/path/ FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER=\'|\' NULL_IF=(\'str1\', \'str2\') FIELD_OPTIONALLY_ENCLOSED_BY=\'"\' TIMESTAMP_FORMAT=\'TZHTZM YYYY-MM-DD HH24:MI:SS.FF9\' DATE_FORMAT=\'TZHTZM YYYY-MM-DD HH24:MI:SS.FF9\' BINARY_FORMAT=BASE64) VALIDATION_MODE = \'RETURN_3_ROWS\'',
    );
    this.validateIdentity(
      'COPY INTO load1 FROM @%load1/data1/ CREDENTIALS = (AWS_KEY_ID=\'id\' AWS_SECRET_KEY=\'key\' AWS_TOKEN=\'token\') FILES = (\'test1.csv\', \'test2.csv\') FORCE = TRUE',
    );
    this.validateIdentity(
      'COPY INTO mytable FROM \'azure://myaccount.blob.core.windows.net/mycontainer/data/files\' CREDENTIALS = (AZURE_SAS_TOKEN=\'token\') ENCRYPTION = (TYPE=\'AZURE_CSE\' MASTER_KEY=\'kPx...\') FILE_FORMAT = (FORMAT_NAME=my_csv_format)',
    );
    this.validateIdentity(
      'COPY INTO mytable (col1, col2) FROM \'s3://mybucket/data/files\' STORAGE_INTEGRATION = "storage" ENCRYPTION = (TYPE=\'NONE\' MASTER_KEY=\'key\') FILES = (\'file1\', \'file2\') PATTERN = \'pattern\' FILE_FORMAT = (FORMAT_NAME=my_csv_format NULL_IF=(\'\')) PARSE_HEADER = TRUE',
    );
    this.validateIdentity(
      'COPY INTO @my_stage/result/data FROM (SELECT * FROM orderstiny) FILE_FORMAT = (TYPE=\'csv\')',
    );
    this.validateIdentity('COPY INTO mytable FILE_FORMAT = (TYPE=\'csv\')');
    this.validateIdentity(
      'COPY INTO MY_DATABASE.MY_SCHEMA.MY_TABLE FROM @MY_DATABASE.MY_SCHEMA.MY_STAGE/my_path FILE_FORMAT = (FORMAT_NAME=MY_DATABASE.MY_SCHEMA.MY_FILE_FORMAT)',
    );
    this.validateAll(
      `COPY INTO 's3://example/data.csv'
    FROM EXTRA.EXAMPLE.TABLE
    CREDENTIALS = ()
    FILE_FORMAT = (TYPE = CSV COMPRESSION = NONE NULL_IF = ('') FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    HEADER = TRUE
    OVERWRITE = TRUE
    SINGLE = TRUE
            `,
      {
        write: {
          '': `COPY INTO 's3://example/data.csv'
FROM EXTRA.EXAMPLE.TABLE
CREDENTIALS = () WITH (
  FILE_FORMAT = (TYPE=CSV COMPRESSION=NONE NULL_IF=(
    ''
  ) FIELD_OPTIONALLY_ENCLOSED_BY='"'),
  HEADER TRUE,
  OVERWRITE TRUE,
  SINGLE TRUE
)`,
          'snowflake': `COPY INTO 's3://example/data.csv'
FROM EXTRA.EXAMPLE.TABLE
CREDENTIALS = ()
FILE_FORMAT = (TYPE=CSV COMPRESSION=NONE NULL_IF=(
  ''
) FIELD_OPTIONALLY_ENCLOSED_BY='"')
HEADER = TRUE
OVERWRITE = TRUE
SINGLE = TRUE`,
        },
        pretty: true,
      },
    );
    this.validateAll(
      `COPY INTO 's3://example/data.csv'
    FROM EXTRA.EXAMPLE.TABLE
    STORAGE_INTEGRATION = S3_INTEGRATION
    FILE_FORMAT = (TYPE=CSV COMPRESSION=NONE NULL_IF=('') FIELD_OPTIONALLY_ENCLOSED_BY='"')
    HEADER = TRUE
    OVERWRITE = TRUE
    SINGLE = TRUE
            `,
      {
        write: {
          '': 'COPY INTO \'s3://example/data.csv\' FROM EXTRA.EXAMPLE.TABLE STORAGE_INTEGRATION = S3_INTEGRATION WITH (FILE_FORMAT = (TYPE=CSV COMPRESSION=NONE NULL_IF=(\'\') FIELD_OPTIONALLY_ENCLOSED_BY=\'"\'), HEADER TRUE, OVERWRITE TRUE, SINGLE TRUE)',
          'snowflake': 'COPY INTO \'s3://example/data.csv\' FROM EXTRA.EXAMPLE.TABLE STORAGE_INTEGRATION = S3_INTEGRATION FILE_FORMAT = (TYPE=CSV COMPRESSION=NONE NULL_IF=(\'\') FIELD_OPTIONALLY_ENCLOSED_BY=\'"\') HEADER = TRUE OVERWRITE = TRUE SINGLE = TRUE',
        },
      },
    );

    const copyAst = parseOne('COPY INTO \'s3://example/contacts.csv\' FROM db.tbl STORAGE_INTEGRATION = PROD_S3_SIDETRADE_INTEGRATION FILE_FORMAT = (FORMAT_NAME=my_csv_format TYPE=CSV COMPRESSION=NONE NULL_IF=(\'\') FIELD_OPTIONALLY_ENCLOSED_BY=\'"\') MATCH_BY_COLUMN_NAME = CASE_SENSITIVE OVERWRITE = TRUE SINGLE = TRUE INCLUDE_METADATA = (col1 = METADATA$START_SCAN_TIME)', {
      read: 'snowflake',
    });

    expect(
      quoteIdentifiers(copyAst).sql({
        dialect: 'snowflake',
      }),
      'COPY INTO \'s3://example/contacts.csv\' FROM "db"."tbl" STORAGE_INTEGRATION = "PROD_S3_SIDETRADE_INTEGRATION" FILE_FORMAT = (FORMAT_NAME="my_csv_format" TYPE=CSV COMPRESSION=NONE NULL_IF=(\'\') FIELD_OPTIONALLY_ENCLOSED_BY=\'"\') MATCH_BY_COLUMN_NAME = CASE_SENSITIVE OVERWRITE = TRUE SINGLE = TRUE INCLUDE_METADATA = ("col1" = "METADATA$START_SCAN_TIME")',
    );

  }

  testPutToStage () {
    this.validateIdentity('PUT \'file:///dir/tmp.csv\' @"my_DB"."schEMA1"."MYstage"');

    // PUT with file path and stage ref containing spaces (wrapped in single quotes)
    let ast = parseOne('PUT \'file://my file.txt\' \'@s1/my folder\'', {
      read: 'snowflake',
    });

    expect(ast).toBeInstanceOf(PutExpr);
    expect((ast.args.this as Expression).equals(LiteralExpr.string('file://my file.txt'))).toBe(true);
    expect((ast.getArgKey('target') as Expression).equals(new VarExpr({
      this: '\'@s1/my folder\'',
    }))).toBe(true);
    expect(ast.sql({
      dialect: 'snowflake',
    })).toBe('PUT \'file://my file.txt\' \'@s1/my folder\'');

    // expression with additional properties
    ast = parseOne('PUT \'file:///tmp/my.txt\' @stage1/folder PARALLEL = 1 AUTO_COMPRESS=false source_compression=gzip OVERWRITE=TRUE', {
      read: 'snowflake',
    });
    expect(ast).toBeInstanceOf(PutExpr);
    expect((ast.args.this as Expression).equals(LiteralExpr.string('file:///tmp/my.txt'))).toBe(true);
    expect((ast.getArgKey('target') as Expression).equals(new VarExpr({
      this: '@stage1/folder',
    }))).toBe(true);
    const properties = ast.getArgKey('properties') as any;

    const propsDict: Record<string, unknown> = {};

    for (const prop of properties.args.expressions) {
      propsDict[prop.args.this.args.this] = (prop.getArgKey('value') as Expression).args.this;
    }
    expect(propsDict).toEqual({
      'PARALLEL': '1',
      'AUTO_COMPRESS': false,
      'source_compression': 'gzip',
      'OVERWRITE': true,
    });

    // validate identity for different args and properties
    this.validateIdentity('PUT \'file:///dir/tmp.csv\' @s1/test');

    // the unquoted URI variant is not fully supported yet
    this.validateIdentity('PUT file:///dir/tmp.csv @%table', undefined, {
      checkCommandWarning: true,
    });
    this.validateIdentity(
      'PUT file:///dir/tmp.csv @s1/test PARALLEL=1 AUTO_COMPRESS=FALSE source_compression=gzip OVERWRITE=TRUE',
      undefined,
      {
        checkCommandWarning: true,
      },
    );

  }

  testGetFromStage () {
    this.validateIdentity('GET @"my_DB"."schEMA1"."MYstage" \'file:///dir/tmp.csv\'');
    this.validateIdentity('GET @s1/test \'file:///dir/tmp.csv\'').assertIs(GetExpr);

    // GET with file path and stage ref containing spaces (wrapped in single quotes)
    let ast = parseOne('GET \'@s1/my folder\' \'file://my file.txt\'', {
      read: 'snowflake',
    });

    expect(ast).toBeInstanceOf(GetExpr);
    expect((ast.getArgKey('target') as Expression).equals(new VarExpr({
      this: '\'@s1/my folder\'',
    }))).toBe(true);
    expect((ast.args.this as Expression).equals(new LiteralExpr({
      this: 'file://my file.txt',
      isString: true,
    }))).toBe(true);
    expect(ast.sql({
      dialect: 'snowflake',
    })).toBe('GET \'@s1/my folder\' \'file://my file.txt\'');

    // expression with additional properties
    ast = parseOne('GET @stage1/folder \'file:///tmp/my.txt\' PARALLEL = 1', {
      read: 'snowflake',
    });
    expect(ast).toBeInstanceOf(GetExpr);
    expect((ast.getArgKey('target') as Expression).equals(new VarExpr({
      this: '@stage1/folder',
    }))).toBe(true);
    expect((ast.args.this as Expression).equals(new LiteralExpr({
      this: 'file:///tmp/my.txt',
      isString: true,
    }))).toBe(true);
    const properties = ast.getArgKey('properties') as Expression;
    const propsDict: Record<string, string> = {};

    for (const prop of (properties.getArgKey('expressions') as Expression[])) {
      propsDict[(prop.args.this as any).args.this] = (prop.getArgKey('value') as Expression).args.this as string;
    }
    expect(propsDict).toEqual({
      PARALLEL: '1',
    });

    // the unquoted URI variant is not fully supported yet
    this.validateIdentity('GET @%table file:///dir/tmp.csv', undefined, {
      checkCommandWarning: true,
    });
    this.validateIdentity(
      'GET @s1/test file:///dir/tmp.csv PARALLEL=1',
      undefined,
      {
        checkCommandWarning: true,
      },
    );
  }

  testQueryingSemiStructuredData () {
    this.validateIdentity('SELECT $1');
    this.validateIdentity('SELECT $1.elem');

    this.validateIdentity('SELECT $1:a.b', 'SELECT GET_PATH($1, \'a.b\')');
    this.validateIdentity('SELECT t.$23:a.b', 'SELECT GET_PATH(t.$23, \'a.b\')');
    this.validateIdentity('SELECT t.$17:a[0].b[0].c', 'SELECT GET_PATH(t.$17, \'a[0].b[0].c\')');

    this.validateAll(
      `
            SELECT col:"customer's department"
            `,
      {
        write: {
          'snowflake': 'SELECT GET_PATH(col, \'["customer\\\'s department"]\')',
          'postgres': 'SELECT JSON_EXTRACT_PATH(col, \'customer\'\'s department\')',
        },
      },
    );

  }

  testAlterSetUnset () {
    this.validateIdentity('ALTER TABLE tbl SET DATA_RETENTION_TIME_IN_DAYS=1');
    this.validateIdentity('ALTER TABLE tbl SET DEFAULT_DDL_COLLATION=\'test\'');
    this.validateIdentity('ALTER TABLE foo SET COMMENT=\'bar\'');
    this.validateIdentity('ALTER TABLE foo SET CHANGE_TRACKING=FALSE');
    this.validateIdentity('ALTER TABLE table1 SET TAG foo.bar = \'baz\'');
    this.validateIdentity('ALTER TABLE IF EXISTS foo SET TAG a = \'a\', b = \'b\', c = \'c\'');
    this.validateIdentity(
      'ALTER TABLE tbl SET STAGE_FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER=\'|\' NULL_IF=(\'\') FIELD_OPTIONALLY_ENCLOSED_BY=\'"\' TIMESTAMP_FORMAT=\'TZHTZM YYYY-MM-DD HH24:MI:SS.FF9\' DATE_FORMAT=\'TZHTZM YYYY-MM-DD HH24:MI:SS.FF9\' BINARY_FORMAT=BASE64)',
    );
    this.validateIdentity(
      'ALTER TABLE tbl SET STAGE_COPY_OPTIONS = (ON_ERROR=SKIP_FILE SIZE_LIMIT=5 PURGE=TRUE MATCH_BY_COLUMN_NAME=CASE_SENSITIVE)',
    );

    this.validateIdentity('ALTER TABLE foo UNSET TAG a, b, c');
    this.validateIdentity('ALTER TABLE foo UNSET DATA_RETENTION_TIME_IN_DAYS, CHANGE_TRACKING');

  }

  testAlterSession () {
    let expr = this.validateIdentity(
      'ALTER SESSION SET autocommit = FALSE, QUERY_TAG = \'qtag\', JSON_INDENT = 1',
    );

    expect(expr.find(AlterSessionExpr)!.equals(new AlterSessionExpr({
      expressions: [
        new SetItemExpr(
          {
            this: new EqExpr({
              this: new ColumnExpr({
                this: new IdentifierExpr({
                  this: 'autocommit',
                  quoted: false,
                }),
              }),
              expression: new BooleanExpr({
                this: false,
              }),
            }),
          },
        ),
        new SetItemExpr(
          {
            this: new EqExpr({
              this: new ColumnExpr({
                this: new IdentifierExpr({
                  this: 'QUERY_TAG',
                  quoted: false,
                }),
              }),
              expression: new LiteralExpr({
                this: 'qtag',
                isString: true,
              }),
            }),
          },
        ),
        new SetItemExpr(
          {
            this: new EqExpr({
              this: new ColumnExpr({
                this: new IdentifierExpr({
                  this: 'JSON_INDENT',
                  quoted: false,
                }),
              }),
              expression: new LiteralExpr({
                this: '1',
                isString: false,
              }),
            }),
          },
        ),
      ],
      unset: false,
    }))).toBe(true);

    expr = this.validateIdentity('ALTER SESSION UNSET autocommit, QUERY_TAG');

    expect(expr.find(AlterSessionExpr)!.equals(new AlterSessionExpr({
      expressions: [
        new SetItemExpr({
          this: new IdentifierExpr({
            this: 'autocommit',
            quoted: false,
          }),
        }),
        new SetItemExpr({
          this: new IdentifierExpr({
            this: 'QUERY_TAG',
            quoted: false,
          }),
        }),
      ],
      unset: true,
    }))).toBe(true);

  }

  testFromChanges () {
    this.validateIdentity(
      'SELECT C1 FROM t1 CHANGES (INFORMATION => APPEND_ONLY) AT (STREAM => \'s1\') END (TIMESTAMP => $ts2)',
    );
    this.validateIdentity(
      'SELECT C1 FROM t1 CHANGES (INFORMATION => APPEND_ONLY) BEFORE (STATEMENT => \'STMT_ID\') END (TIMESTAMP => $ts2)',
    );
    this.validateIdentity(
      'SELECT 1 FROM some_table CHANGES (INFORMATION => APPEND_ONLY) AT (TIMESTAMP => TO_TIMESTAMP_TZ(\'2024-07-01 00:00:00+00:00\')) END (TIMESTAMP => TO_TIMESTAMP_TZ(\'2024-07-01 14:28:59.999999+00:00\'))',
      'SELECT 1 FROM some_table CHANGES (INFORMATION => APPEND_ONLY) AT (TIMESTAMP => CAST(\'2024-07-01 00:00:00+00:00\' AS TIMESTAMPTZ)) END (TIMESTAMP => CAST(\'2024-07-01 14:28:59.999999+00:00\' AS TIMESTAMPTZ))',
    );

  }

  testGrant () {
    const grantCmds = [
      'GRANT SELECT ON FUTURE TABLES IN DATABASE d1 TO ROLE r1',
      'GRANT INSERT, DELETE ON FUTURE TABLES IN SCHEMA d1.s1 TO ROLE r2',
      'GRANT SELECT ON ALL TABLES IN SCHEMA mydb.myschema to ROLE analyst',
      'GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA mydb.myschema TO ROLE role1',
      'GRANT CREATE MATERIALIZED VIEW ON SCHEMA mydb.myschema TO DATABASE ROLE mydb.dr1',
    ];

    for (const sql of grantCmds) {
      this.validateIdentity(sql, undefined, {
        checkCommandWarning: true,
      });

      this.validateIdentity(
        'GRANT ALL PRIVILEGES ON FUNCTION mydb.myschema.ADD5(number) TO ROLE analyst',
      );

    }
  }

  testRevoke () {
    const revokeCmds = [
      'REVOKE SELECT ON FUTURE TABLES IN DATABASE d1 FROM ROLE r1',
      'REVOKE INSERT, DELETE ON FUTURE TABLES IN SCHEMA d1.s1 FROM ROLE r2',
      'REVOKE SELECT ON ALL TABLES IN SCHEMA mydb.myschema FROM ROLE analyst',
      'REVOKE SELECT, INSERT ON FUTURE TABLES IN SCHEMA mydb.myschema FROM ROLE role1',
      'REVOKE CREATE MATERIALIZED VIEW ON SCHEMA mydb.myschema FROM DATABASE ROLE mydb.dr1',
    ];

    for (const sql of revokeCmds) {
      this.validateIdentity(sql, undefined, {
        checkCommandWarning: true,
      });

      this.validateIdentity(
        'REVOKE ALL PRIVILEGES ON FUNCTION mydb.myschema.ADD5(number) FROM ROLE analyst',
      );

    }
  }

  testWindowFunctionArg () {
    const query = 'SELECT * FROM TABLE(db.schema.FUNC(a) OVER ())';

    const ast = this.parseOne(query);

    const window = ast.find(WindowExpr);

    expect(ast.sql({
      dialect: 'snowflake',
    })).toBe(query);
    expect(Array.from(ast.findAll(ColumnExpr)).length).toBe(1);
    expect(window?.args.this?.sql({
      dialect: 'snowflake',
    })).toBe('db.schema.FUNC(a)');

  }

  testOffsetWithoutLimit () {
    this.validateAll(
      'SELECT 1 ORDER BY 1 LIMIT NULL OFFSET 0',
      {
        read: {
          'trino': 'SELECT 1 ORDER BY 1 OFFSET 0',
        },
      },
    );

  }

  testListagg () {
    this.validateIdentity('LISTAGG(data[\'some_field\'], \',\')');

    for (const distinct of [
      '',
      'DISTINCT ',
    ]) {
      this.validateAll(
        `SELECT LISTAGG(${distinct}col, '|SEPARATOR|') WITHIN GROUP (ORDER BY col2) FROM t`,
        {
          read: {
            'trino': `SELECT LISTAGG(${distinct}col, '|SEPARATOR|') WITHIN GROUP (ORDER BY col2) FROM t`,
            'duckdb': `SELECT LISTAGG(${distinct}col, '|SEPARATOR|' ORDER BY col2) FROM t`,
          },
          write: {
            'snowflake': `SELECT LISTAGG(${distinct}col, '|SEPARATOR|') WITHIN GROUP (ORDER BY col2) FROM t`,
            'trino': `SELECT LISTAGG(${distinct}col, '|SEPARATOR|') WITHIN GROUP (ORDER BY col2) FROM t`,
            'duckdb': `SELECT LISTAGG(${distinct}col, '|SEPARATOR|' ORDER BY col2) FROM t`,
          },
        },
      );

    }
  }

  testRelyOptions () {
    for (const option of [
      'NORELY',
      'RELY',
    ]) {
      this.validateIdentity(
        `CREATE TABLE t (col1 INT PRIMARY KEY ${option}, col2 INT UNIQUE ${option}, col3 INT NOT NULL FOREIGN KEY REFERENCES other_t (id) ${option})`,
      );
      this.validateIdentity(
        `CREATE TABLE t (col1 INT, col2 INT, col3 INT, PRIMARY KEY (col1) ${option}, UNIQUE (col1, col2) ${option}, FOREIGN KEY (col3) REFERENCES other_t (id) ${option})`,
      );

    }
  }

  testParameter () {
    const expr = this.validateIdentity('SELECT :1');

    expect(expr.find(PlaceholderExpr)!.equals(new PlaceholderExpr({
      this: '1',
    }))).toBe(true);
    this.validateIdentity('SELECT :1, :2');
    this.validateIdentity('SELECT :1 + :2');

  }

  testMaxByMinBy () {
    const maxBy = this.validateIdentity('MAX_BY(DISTINCT selected_col, filtered_col)');
    const minBy = this.validateIdentity('MIN_BY(DISTINCT selected_col, filtered_col)');

    for (const node of [
      maxBy,
      minBy,
    ]) {
      expect((node.args.this as any).getArgKey('expressions').length).toBe(1);
      expect(node.args.expression).toBeInstanceOf(ColumnExpr);

      // Test 3-argument case (returns array)
      const maxBy3 = this.validateIdentity('MAX_BY(selected_col, filtered_col, 5)');
      const minBy3 = this.validateIdentity('MIN_BY(selected_col, filtered_col, 3)');

      for (const node of [
        maxBy3,
        minBy3,
      ]) {
        expect(node.getArgKey('count')).toBeDefined();

        this.validateAll(
          'SELECT MAX_BY(a, b) FROM t',
          {
            write: {
              'snowflake': 'SELECT MAX_BY(a, b) FROM t',
              'duckdb': 'SELECT ARG_MAX(a, b) FROM t',
            },
          },
        );
        this.validateAll(
          'SELECT MIN_BY(a, b) FROM t',
          {
            write: {
              'snowflake': 'SELECT MIN_BY(a, b) FROM t',
              'duckdb': 'SELECT ARG_MIN(a, b) FROM t',
            },
          },
        );

      }
    }
  }

  testCreateViewCopyGrants () {
    // for normal views, 'COPY GRANTS' goes *after* the column list. ref: https://docs.snowflake.com/en/sql-reference/sql/create-view#syntax
    this.validateIdentity(
      'CREATE OR REPLACE VIEW FOO (A, B) COPY GRANTS AS SELECT A, B FROM TBL',
    );

    // for materialized views, 'COPY GRANTS' must go *before* the column list or an error will be thrown. ref: https://docs.snowflake.com/en/sql-reference/sql/create-materialized-view#syntax
    this.validateIdentity(
      'CREATE OR REPLACE MATERIALIZED VIEW FOO COPY GRANTS (A, B) AS SELECT A, B FROM TBL',
    );

    // check that only 'COPY GRANTS' goes before the column list and other properties still go after
    this.validateIdentity(
      'CREATE OR REPLACE MATERIALIZED VIEW FOO COPY GRANTS (A, B) COMMENT=\'foo\' TAG (a=\'b\') AS SELECT A, B FROM TBL',
    );

    // no COPY GRANTS
    this.validateIdentity('CREATE OR REPLACE VIEW FOO (A, B) AS SELECT A, B FROM TBL');
    this.validateIdentity(
      'CREATE OR REPLACE MATERIALIZED VIEW FOO (A, B) AS SELECT A, B FROM TBL',
    );

  }

  testSemanticView () {
    for (const [
      dimensions,
      metrics,
      whereClause,
      facts,
    ] of [
        [
          undefined,
          undefined,
          undefined,
          undefined,
        ],
        [
          undefined,
          undefined,
          undefined,
          'a.a',
        ],
        [
          'DATE_PART(\'year\', a.b)',
          undefined,
          undefined,
          undefined,
        ],
        [
          undefined,
          'a.b, a.c',
          undefined,
          undefined,
        ],
        [
          undefined,
          undefined,
          undefined,
          'a.d, a.e',
        ],
        [
          'a.b, a.c',
          'a.b, a.c',
          undefined,
          undefined,
        ],
        [
          'a.b',
          'a.b, a.c',
          'a.c > 5',
          undefined,
        ],
        [
          'a.b',
          undefined,
          'a.c > 5',
          'a.d',
        ],
      ]) {
      const dimensionsString = dimensions ? ` DIMENSIONS ${dimensions}` : '';
      const metricsString = metrics ? ` METRICS ${metrics}` : '';
      const factString = facts ? ` FACTS ${facts}` : '';
      const whereString = whereClause ? ` WHERE ${whereClause}` : '';

      this.validateIdentity(
        `SELECT * FROM SEMANTIC_VIEW(tbl${metricsString}${dimensionsString}${factString}${whereString}) ORDER BY foo`,
      );
      this.validateIdentity(
        `SELECT * FROM SEMANTIC_VIEW(tbl${dimensionsString}${factString}${metricsString}${whereString})`,
        `SELECT * FROM SEMANTIC_VIEW(tbl${metricsString}${dimensionsString}${factString}${whereString})`,
      );

      this.validateIdentity(
        'SELECT * FROM SEMANTIC_VIEW(foo METRICS a.b, a.c DIMENSIONS a.b, a.c WHERE a.b > \'1995-01-01\')',
        `SELECT
  *
FROM SEMANTIC_VIEW(
  foo
  METRICS a.b, a.c
  DIMENSIONS a.b, a.c
  WHERE a.b > '1995-01-01'
)`,
        {
          pretty: true,
        },
      );
    }
  }

  testGetExtract () {
    this.validateAll(
      'SELECT GET([4, 5, 6], 1)',
      {
        write: {
          'snowflake': 'SELECT GET([4, 5, 6], 1)',
          'duckdb': 'SELECT [4, 5, 6][2]',
        },
      },
    );

    this.validateAll(
      'SELECT GET(col::MAP(INTEGER, VARCHAR), 1)',
      {
        write: {
          'snowflake': 'SELECT GET(CAST(col AS MAP(INT, VARCHAR)), 1)',
          'duckdb': 'SELECT CAST(col AS MAP(INT, TEXT))[1]',
        },
      },
    );

    this.validateAll(
      'SELECT GET(v, \'field\')',
      {
        write: {
          'snowflake': 'SELECT GET(v, \'field\')',
          'duckdb': 'SELECT v -> \'$.field\'',
        },
      },
    );

    this.validateIdentity('GET(foo, bar)').assertIs(GetExtractExpr);

  }

  testCreateSequence () {
    this.validateIdentity(
      'CREATE SEQUENCE seq  START=5 comment = \'foo\' INCREMENT=10',
      'CREATE SEQUENCE seq COMMENT=\'foo\' START WITH 5 INCREMENT BY 10',
    );
    this.validateAll(
      'CREATE SEQUENCE seq WITH START=1 INCREMENT=1',
      {
        write: {
          'snowflake': 'CREATE SEQUENCE seq START WITH 1 INCREMENT BY 1',
          'duckdb': 'CREATE SEQUENCE seq START WITH 1 INCREMENT BY 1',
        },
      },
    );

  }

  testBitAggs () {
    const bitAndFuncs = [
      'BITANDAGG',
      'BITAND_AGG',
      'BIT_AND_AGG',
      'BIT_ANDAGG',
    ];
    const bitOrFuncs = [
      'BITORAGG',
      'BITOR_AGG',
      'BIT_OR_AGG',
      'BIT_ORAGG',
    ];
    const bitXorFuncs = [
      'BITXORAGG',
      'BITXOR_AGG',
      'BIT_XOR_AGG',
      'BIT_XORAGG',
    ];

    for (const bitFunc of [
      bitAndFuncs,
      bitOrFuncs,
      bitXorFuncs,
    ]) {
      for (const name of bitFunc) {
        this.validateIdentity(`${name}(x)`, `${bitFunc[0]}(x)`);

      }
    }
  }

  testBitmapOrAgg () {
    this.validateIdentity('BITMAP_OR_AGG(x)');

  }

  testMd5Functions () {
    this.validateIdentity('MD5_HEX(col)', 'MD5(col)');
    this.validateIdentity('MD5(col)');
    this.validateIdentity('MD5_BINARY(col)');
    this.validateIdentity('MD5_NUMBER_LOWER64(col)');
    this.validateIdentity('MD5_NUMBER_UPPER64(col)');

  }

  testSha1 () {
    this.validateAll(
      'SHA1(x)',
      {
        write: {
          'snowflake': 'SHA1(x)',
          'duckdb': 'SHA1(x)',
        },
      },
    );

    let expr = this.validateIdentity('SHA1(\'text\')');
    let annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });

    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('SHA1(\'text\')');
    expect(annotated.sql({
      dialect: 'snowflake',
    })).toBe('SHA1(\'text\')');

    this.validateAll(
      'SHA1(X\'002A\'::BINARY)',
      {
        write: {
          'snowflake': 'SHA1(CAST(x\'002A\' AS BINARY))',
          'duckdb': 'SHA1(CAST(UNHEX(\'002A\') AS BLOB))',
        },
      },
    );
    expr = this.validateIdentity('SHA1(123)');
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'snowflake',
    })).toBe('SHA1(123)');
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('SHA1(CAST(123 AS TEXT))');

    expr = this.validateIdentity('SHA1(DATE \'2024-01-15\')', 'SHA1(CAST(\'2024-01-15\' AS DATE))');
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'snowflake',
    })).toBe('SHA1(CAST(\'2024-01-15\' AS DATE))');
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('SHA1(CAST(CAST(\'2024-01-15\' AS DATE) AS TEXT))');

  }

  testModelAttribute () {
    this.validateIdentity('SELECT model!mladmin');
    this.validateIdentity('SELECT model!PREDICT(1)');
    this.validateIdentity('SELECT m!PREDICT(INPUT_DATA => {*}) AS p FROM tbl');
    this.validateIdentity('SELECT m!PREDICT(INPUT_DATA => {tbl.*}) AS p FROM tbl');
    this.validateIdentity('x.y.z!PREDICT(foo, bar, baz, bla)');
    this.validateIdentity(
      'SELECT * FROM TABLE(model_trained_with_labeled_data!DETECT_ANOMALIES(INPUT_DATA => TABLE(view_with_data_to_analyze), TIMESTAMP_COLNAME => \'date\', TARGET_COLNAME => \'sales\', CONFIG_OBJECT => OBJECT_CONSTRUCT(\'prediction_interval\', 0.99)))',
    );

  }

  testSetItemKindAttribute () {
    let expr = parseOne('ALTER SESSION SET autocommit = FALSE', {
      read: 'snowflake',
    });

    let setItem = expr.find(SetItemExpr);

    expect(setItem).toBeDefined();
    expect(setItem?.getArgKey('kind')).toBeNullable();

    expr = parseOne('SET a = 1', {
      read: 'snowflake',
    });
    setItem = expr.find(SetItemExpr);
    expect(setItem).toBeDefined();
    expect(setItem?.getArgKey('kind')).toBe('VARIABLE');

  }

  testRound () {
    this.validateAll(
      'SELECT ROUND(2.25) AS value',
      {
        write: {
          'snowflake': 'SELECT ROUND(2.25) AS value',
          'duckdb': 'SELECT ROUND(2.25) AS value',
        },
      },
    );

    this.validateAll(
      'SELECT ROUND(2.25, 1) AS value',
      {
        write: {
          'snowflake': 'SELECT ROUND(2.25, 1) AS value',
          'duckdb': 'SELECT ROUND(2.25, 1) AS value',
        },
      },
    );

    this.validateAll(
      'SELECT ROUND(EXPR => 2.25, SCALE => 1) AS value',
      {
        write: {
          'snowflake': 'SELECT ROUND(2.25, 1) AS value',
          'duckdb': 'SELECT ROUND(2.25, 1) AS value',
        },
      },
    );

    this.validateAll(
      'SELECT ROUND(SCALE => 1, EXPR => 2.25) AS value',
      {
        write: {
          'snowflake': 'SELECT ROUND(2.25, 1) AS value',
          'duckdb': 'SELECT ROUND(2.25, 1) AS value',
        },
      },
    );

    this.validateAll(
      'SELECT ROUND(2.25, 1, \'HALF_AWAY_FROM_ZERO\') AS value',
      {
        write: {
          'snowflake': 'SELECT ROUND(2.25, 1, \'HALF_AWAY_FROM_ZERO\') AS value',
          'duckdb': 'SELECT ROUND(2.25, 1) AS value',
        },
      },
    );

    this.validateAll(
      'SELECT ROUND(EXPR => 2.25, SCALE => 1, ROUNDING_MODE => \'HALF_AWAY_FROM_ZERO\') AS value',
      {
        write: {
          'snowflake': 'SELECT ROUND(2.25, 1, \'HALF_AWAY_FROM_ZERO\') AS value',
          'duckdb': 'SELECT ROUND(2.25, 1) AS value',
        },
      },
    );

    this.validateAll(
      'SELECT ROUND(2.25, 1, \'HALF_TO_EVEN\') AS value',
      {
        write: {
          'snowflake': 'SELECT ROUND(2.25, 1, \'HALF_TO_EVEN\') AS value',
          'duckdb': 'SELECT ROUND_EVEN(2.25, 1) AS value',
        },
      },
    );

    this.validateAll(
      'SELECT ROUND(ROUNDING_MODE => \'HALF_TO_EVEN\', EXPR => 2.25, SCALE => 1) AS value',
      {
        write: {
          'snowflake': 'SELECT ROUND(2.25, 1, \'HALF_TO_EVEN\') AS value',
          'duckdb': 'SELECT ROUND_EVEN(2.25, 1) AS value',
        },
      },
    );

    this.validateAll(
      'SELECT ROUND(SCALE => 1, EXPR => 2.25, , ROUNDING_MODE => \'HALF_TO_EVEN\') AS value',
      {
        write: {
          'snowflake': 'SELECT ROUND(2.25, 1, \'HALF_TO_EVEN\') AS value',
          'duckdb': 'SELECT ROUND_EVEN(2.25, 1) AS value',
        },
      },
    );

    this.validateAll(
      'SELECT ROUND(EXPR => 2.25, SCALE => 1, ROUNDING_MODE => \'HALF_TO_EVEN\') AS value',
      {
        write: {
          'snowflake': 'SELECT ROUND(2.25, 1, \'HALF_TO_EVEN\') AS value',
          'duckdb': 'SELECT ROUND_EVEN(2.25, 1) AS value',
        },
      },
    );

    this.validateAll(
      'SELECT ROUND(2.256, 1.8) AS value',
      {
        write: {
          'snowflake': 'SELECT ROUND(2.256, 1.8) AS value',
          'duckdb': 'SELECT ROUND(2.256, CAST(1.8 AS INT)) AS value',
        },
      },
    );

    this.validateAll(
      'SELECT ROUND(2.256, CAST(1.8 AS DECIMAL(38, 0))) AS value',
      {
        write: {
          'snowflake': 'SELECT ROUND(2.256, CAST(1.8 AS DECIMAL(38, 0))) AS value',
          'duckdb': 'SELECT ROUND(2.256, CAST(CAST(1.8 AS DECIMAL(38, 0)) AS INT)) AS value',
        },
      },
    );

  }

  testGetBit () {
    this.validateAll(
      'SELECT GETBIT(11, 1)',
      {
        write: {
          'snowflake': 'SELECT GETBIT(11, 1)',
          'databricks': 'SELECT GETBIT(11, 1)',
          'redshift': 'SELECT GETBIT(11, 1)',
        },
      },
    );
    const expr = this.validateIdentity('GETBIT(11, 1)');
    const annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });

    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('(11 >> 1) & 1');
    expect(annotated.sql({
      dialect: 'postgres',
    })).toBe('11 >> 1 & 1');

  }

  testToBinary () {
    let expr = this.validateIdentity('TO_BINARY(\'48454C50\', \'HEX\')');
    let annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });

    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('UNHEX(\'48454C50\')');

    expr = this.validateIdentity('TO_BINARY(\'48454C50\')');
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('UNHEX(\'48454C50\')');

    expr = this.validateIdentity('TO_BINARY(\'TEST\', \'UTF-8\')');
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('ENCODE(\'TEST\')');

    expr = this.validateIdentity('TO_BINARY(\'SEVMUA==\', \'BASE64\')');
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('FROM_BASE64(\'SEVMUA==\')');

    expr = this.validateIdentity('TRY_TO_BINARY(\'48454C50\', \'HEX\')');
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('TRY(UNHEX(\'48454C50\'))');

    expr = this.validateIdentity('TRY_TO_BINARY(\'48454C50\')');
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('TRY(UNHEX(\'48454C50\'))');

    expr = this.validateIdentity('TRY_TO_BINARY(\'Hello\', \'UTF-8\')');
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('TRY(ENCODE(\'Hello\'))');

    expr = this.validateIdentity('TRY_TO_BINARY(\'SGVsbG8=\', \'BASE64\')');
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('TRY(FROM_BASE64(\'SGVsbG8=\'))');

    expr = this.validateIdentity('TRY_TO_BINARY(\'Hello\', \'UTF-16\')');
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('NULL');

  }

  testReverse () {
    // Test REVERSE with TO_BINARY (BLOB type) - UTF-8 format
    let expr = this.validateIdentity('REVERSE(TO_BINARY(\'ABC\', \'UTF-8\'))');
    let annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });

    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('CAST(REVERSE(CAST(ENCODE(\'ABC\') AS TEXT)) AS BLOB)');

    // Test REVERSE with TO_BINARY - HEX format
    expr = this.validateIdentity('REVERSE(TO_BINARY(\'414243\', \'HEX\'))');
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('CAST(REVERSE(CAST(UNHEX(\'414243\') AS TEXT)) AS BLOB)');

    // Test REVERSE with HEX_DECODE_BINARY
    expr = this.validateIdentity('REVERSE(HEX_DECODE_BINARY(\'414243\'))');
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('CAST(REVERSE(CAST(UNHEX(\'414243\') AS TEXT)) AS BLOB)');

    // Test REVERSE with VARCHAR (should not add casts)
    expr = this.validateIdentity('REVERSE(\'ABC\')');
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('REVERSE(\'ABC\')');

  }

  testFloatInterval () {
    // Test TIMEADD with float interval value - DuckDB INTERVAL requires integers
    let expr = this.validateIdentity('TIMEADD(HOUR, 2.5, CAST(\'10:30:00\' AS TIME))');
    let annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });

    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('CAST(\'10:30:00\' AS TIME) + INTERVAL (CAST(ROUND(2.5) AS INT)) HOUR');

    // Test DATEADD with decimal interval value
    expr = this.validateIdentity(
      'DATEADD(HOUR, CAST(3.8 AS DECIMAL(10, 2)), CAST(\'2024-01-01 10:00:00\' AS TIMESTAMP))',
    );
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('CAST(\'2024-01-01 10:00:00\' AS TIMESTAMP) + INTERVAL (CAST(ROUND(CAST(3.8 AS DECIMAL(10, 2))) AS INT)) HOUR');

    // Test TIMESTAMPADD with float interval value - Snowflake parser converts to DATEADD
    expr = this.parseOne(
      'TIMESTAMPADD(MINUTE, 30.9, CAST(\'2024-01-01 10:00:00\' AS TIMESTAMP))',
      {
        dialect: 'snowflake',
      },
    );
    expect(expr.sql({
      dialect: 'snowflake',
    })).toBe('DATEADD(MINUTE, 30.9, CAST(\'2024-01-01 10:00:00\' AS TIMESTAMP))');
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('CAST(\'2024-01-01 10:00:00\' AS TIMESTAMP) + INTERVAL (CAST(ROUND(30.9) AS INT)) MINUTE');

  }

  testTranspileBitwiseOps () {
    // Binary bitwise operations
    let expr = this.parseOne('SELECT BITOR(x\'FF\', x\'0F\')', {
      dialect: 'snowflake',
    });
    let annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });

    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('SELECT CAST(CAST(UNHEX(\'FF\') AS BIT) | CAST(UNHEX(\'0F\') AS BIT) AS BLOB)');
    expect(annotated.sql({
      dialect: 'snowflake',
    })).toBe('SELECT BITOR(x\'FF\', x\'0F\')');

    expr = this.parseOne('SELECT BITAND(x\'FF\', x\'0F\')', {
      dialect: 'snowflake',
    });
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('SELECT CAST(CAST(UNHEX(\'FF\') AS BIT) & CAST(UNHEX(\'0F\') AS BIT) AS BLOB)');
    expect(annotated.sql({
      dialect: 'snowflake',
    })).toBe('SELECT BITAND(x\'FF\', x\'0F\')');

    expr = this.parseOne('SELECT BITXOR(x\'FF\', x\'0F\')', {
      dialect: 'snowflake',
    });
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('SELECT CAST(XOR(CAST(UNHEX(\'FF\') AS BIT), CAST(UNHEX(\'0F\') AS BIT)) AS BLOB)');
    expect(annotated.sql({
      dialect: 'snowflake',
    })).toBe('SELECT BITXOR(x\'FF\', x\'0F\')');

    expr = this.parseOne('SELECT BITNOT(x\'FF\')', {
      dialect: 'snowflake',
    });
    annotated = annotateTypes(expr, {
      dialect: 'snowflake',
    });
    expect(annotated.sql({
      dialect: 'duckdb',
    })).toBe('SELECT CAST(~CAST(UNHEX(\'FF\') AS BIT) AS BLOB)');
    expect(annotated.sql({
      dialect: 'snowflake',
    })).toBe('SELECT BITNOT(x\'FF\')');

  }

  testQuoting () {
    expect(parseOne('select a, B from DUAL', {
      read: 'snowflake',
    }).sql({
      dialect: 'snowflake',
      identify: 'safe',
    })).toBe('SELECT a, "B" FROM DUAL');

  }

  testFloor () {
    this.validateAll(
      'SELECT FLOOR(1.753, 2)',
      {
        write: {
          'duckdb': 'SELECT ROUND(FLOOR(1.753 * POWER(10, 2)) / POWER(10, 2), 2)',
        },
      },
    );
    this.validateAll(
      'SELECT FLOOR(123.45, -1)',
      {
        write: {
          'duckdb': 'SELECT ROUND(FLOOR(123.45 * POWER(10, -1)) / POWER(10, -1), -1)',
        },
      },
    );
    this.validateAll(
      'SELECT FLOOR(a + b, 2)',
      {
        write: {
          'duckdb': 'SELECT ROUND(FLOOR((a + b) * POWER(10, 2)) / POWER(10, 2), 2)',
        },
      },
    );
    this.validateAll(
      'SELECT FLOOR(1.234, 1.5)',
      {
        write: {
          'duckdb': 'SELECT ROUND(FLOOR(1.234 * POWER(10, CAST(1.5 AS INT))) / POWER(10, CAST(1.5 AS INT)), CAST(1.5 AS INT))',
        },
      },
    );

  }

  testSeqFunctions () {
    // SEQ1 - 1-byte sequences
    this.validateAll(
      'SELECT SEQ1() FROM test',
      {
        write: {
          'duckdb': 'SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 256 FROM test',
          'snowflake': 'SELECT SEQ1() FROM test',
        },
      },
    );
    this.validateAll(
      'SELECT SEQ1(0) FROM test',
      {
        write: {
          'duckdb': 'SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 256 FROM test',
          'snowflake': 'SELECT SEQ1(0) FROM test',
        },
      },
    );
    // 1 means it's signed parameter, which affects wrap-around behavior
    this.validateAll(
      'SELECT SEQ1(1) FROM test',
      {
        write: {
          'duckdb': 'SELECT (CASE WHEN (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 256 >= 128 THEN (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 256 - 256 ELSE (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 256 END) FROM test',
          'snowflake': 'SELECT SEQ1(1) FROM test',
        },
      },
    );

    // SEQ2 - 2-byte sequences
    this.validateAll(
      'SELECT SEQ2() FROM test',
      {
        write: {
          'duckdb': 'SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 65536 FROM test',
          'snowflake': 'SELECT SEQ2() FROM test',
        },
      },
    );
    this.validateAll(
      'SELECT SEQ2(0) FROM test',
      {
        write: {
          'duckdb': 'SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 65536 FROM test',
          'snowflake': 'SELECT SEQ2(0) FROM test',
        },
      },
    );
    this.validateAll(
      'SELECT SEQ2(1) FROM test',
      {
        write: {
          'duckdb': 'SELECT (CASE WHEN (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 65536 >= 32768 THEN (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 65536 - 65536 ELSE (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 65536 END) FROM test',
          'snowflake': 'SELECT SEQ2(1) FROM test',
        },
      },
    );

    // SEQ4 - 4-byte sequences
    this.validateAll(
      'SELECT SEQ4() FROM test',
      {
        write: {
          'duckdb': 'SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 4294967296 FROM test',
          'snowflake': 'SELECT SEQ4() FROM test',
        },
      },
    );
    this.validateAll(
      'SELECT SEQ4(0) FROM test',
      {
        write: {
          'duckdb': 'SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 4294967296 FROM test',
          'snowflake': 'SELECT SEQ4(0) FROM test',
        },
      },
    );
    this.validateAll(
      'SELECT SEQ4(1) FROM test',
      {
        write: {
          'duckdb': 'SELECT (CASE WHEN (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 4294967296 >= 2147483648 THEN (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 4294967296 - 4294967296 ELSE (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 4294967296 END) FROM test',
          'snowflake': 'SELECT SEQ4(1) FROM test',
        },
      },
    );

    // SEQ8 - 8-byte sequences
    this.validateAll(
      'SELECT SEQ8() FROM test',
      {
        write: {
          'duckdb': 'SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 18446744073709551616 FROM test',
          'snowflake': 'SELECT SEQ8() FROM test',
        },
      },
    );
    this.validateAll(
      'SELECT SEQ8(0) FROM test',
      {
        write: {
          'duckdb': 'SELECT (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 18446744073709551616 FROM test',
          'snowflake': 'SELECT SEQ8(0) FROM test',
        },
      },
    );
    this.validateAll(
      'SELECT SEQ8(1) FROM test',
      {
        write: {
          'duckdb': 'SELECT (CASE WHEN (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 18446744073709551616 >= 9223372036854775808 THEN (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 18446744073709551616 - 18446744073709551616 ELSE (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % 18446744073709551616 END) FROM test',
          'snowflake': 'SELECT SEQ8(1) FROM test',
        },
      },
    );

  }

  testGenerator () {
    // Basic ROWCOUNT transpilation
    this.validateAll(
      'SELECT 1 FROM TABLE(GENERATOR(ROWCOUNT => 5))',
      {
        write: {
          'duckdb': 'SELECT 1 FROM RANGE(5)',
          'snowflake': 'SELECT 1 FROM TABLE(GENERATOR(ROWCOUNT => 5))',
        },
      },
    );

    // GENERATOR with SEQ functions - the common use case
    this.validateAll(
      'SELECT SEQ8() FROM TABLE(GENERATOR(ROWCOUNT => 5))',
      {
        write: {
          'duckdb': 'SELECT range % 18446744073709551616 FROM RANGE(5)',
          'snowflake': 'SELECT SEQ8() FROM TABLE(GENERATOR(ROWCOUNT => 5))',
        },
      },
    );

    // GENERATOR with JOIN in parenthesized construct - preserves joins
    this.validateAll(
      'SELECT * FROM (TABLE(GENERATOR(ROWCOUNT => 5)) JOIN other ON 1 = 1)',
      {
        write: {
          'duckdb': 'SELECT * FROM (RANGE(5) JOIN other ON 1 = 1)',
          'snowflake': 'SELECT * FROM (TABLE(GENERATOR(ROWCOUNT => 5)) JOIN other ON 1 = 1)',
        },
      },
    );

  }

  testCeil () {
    this.validateAll(
      'SELECT CEIL(1.753, 2)',
      {
        write: {
          'duckdb': 'SELECT ROUND(CEIL(1.753 * POWER(10, 2)) / POWER(10, 2), 2)',
        },
      },
    );
    this.validateAll(
      'SELECT CEIL(123.45, -1)',
      {
        write: {
          'duckdb': 'SELECT ROUND(CEIL(123.45 * POWER(10, -1)) / POWER(10, -1), -1)',
        },
      },
    );
    this.validateAll(
      'SELECT CEIL(a + b, 2)',
      {
        write: {
          'duckdb': 'SELECT ROUND(CEIL((a + b) * POWER(10, 2)) / POWER(10, 2), 2)',
        },
      },
    );
    this.validateAll(
      'SELECT CEIL(1.234, 1.5)',
      {
        write: {
          'duckdb': 'SELECT ROUND(CEIL(1.234 * POWER(10, CAST(1.5 AS INT))) / POWER(10, CAST(1.5 AS INT)), CAST(1.5 AS INT))',
        },
      },
    );

  }

  testCorr () {
    this.validateAll(
      'SELECT CORR(a, b)',
      {
        read: {
          'snowflake': 'SELECT CORR(a, b)',
          'postgres': 'SELECT CORR(a, b)',
        },
        write: {
          'snowflake': 'SELECT CORR(a, b)',
          'postgres': 'SELECT CORR(a, b)',
          'duckdb': 'SELECT CASE WHEN ISNAN(CORR(a, b)) THEN NULL ELSE CORR(a, b) END',
        },
      },
    );
    this.validateAll(
      'SELECT CORR(a, b) OVER (PARTITION BY c)',
      {
        read: {
          'snowflake': 'SELECT CORR(a, b) OVER (PARTITION BY c)',
          'postgres': 'SELECT CORR(a, b) OVER (PARTITION BY c)',
        },
        write: {
          'snowflake': 'SELECT CORR(a, b) OVER (PARTITION BY c)',
          'postgres': 'SELECT CORR(a, b) OVER (PARTITION BY c)',
          'duckdb': 'SELECT CASE WHEN ISNAN(CORR(a, b) OVER (PARTITION BY c)) THEN NULL ELSE CORR(a, b) OVER (PARTITION BY c) END',
        },
      },
    );

    this.validateAll(
      'SELECT CORR(a, b) FILTER(WHERE c > 0)',
      {
        write: {
          'duckdb': 'SELECT CASE WHEN ISNAN(CORR(a, b) FILTER(WHERE c > 0)) THEN NULL ELSE CORR(a, b) FILTER(WHERE c > 0) END',
        },
      },
    );
    this.validateAll(
      'SELECT CORR(a, b) FILTER(WHERE c > 0) OVER (PARTITION BY d)',
      {
        write: {
          'duckdb': 'SELECT CASE WHEN ISNAN(CORR(a, b) FILTER(WHERE c > 0) OVER (PARTITION BY d)) THEN NULL ELSE CORR(a, b) FILTER(WHERE c > 0) OVER (PARTITION BY d) END',
        },
      },
    );

  }

  testEncryptionFunctions () {
    // ENCRYPT
    this.validateIdentity('ENCRYPT(value, \'passphrase\')');
    this.validateIdentity('ENCRYPT(value, \'passphrase\', \'aad\')');
    this.validateIdentity('ENCRYPT(value, \'passphrase\', \'aad\', \'AES-GCM\')');

    // ENCRYPT_RAW
    this.validateIdentity('ENCRYPT_RAW(value, key, iv)');
    this.validateIdentity('ENCRYPT_RAW(value, key, iv, aad)');
    this.validateIdentity('ENCRYPT_RAW(value, key, iv, aad, \'AES-GCM\')');

    // DECRYPT
    this.validateIdentity('DECRYPT(encrypted, \'passphrase\')');
    this.validateIdentity('DECRYPT(encrypted, \'passphrase\', \'aad\')');
    this.validateIdentity('DECRYPT(encrypted, \'passphrase\', \'aad\', \'AES-GCM\')');

    // DECRYPT_RAW
    this.validateIdentity('DECRYPT_RAW(encrypted, key, iv)');
    this.validateIdentity('DECRYPT_RAW(encrypted, key, iv, aad)');
    this.validateIdentity('DECRYPT_RAW(encrypted, key, iv, aad, \'AES-GCM\')');
    this.validateIdentity('DECRYPT_RAW(encrypted, key, iv, aad, \'AES-GCM\', aead)');

    // TRY_DECRYPT (parses as Decrypt with safe=true)
    this.validateIdentity('TRY_DECRYPT(encrypted, \'passphrase\')');
    this.validateIdentity('TRY_DECRYPT(encrypted, \'passphrase\', \'aad\')');
    this.validateIdentity('TRY_DECRYPT(encrypted, \'passphrase\', \'aad\', \'AES-GCM\')');

    // TRY_DECRYPT_RAW (parses as DecryptRaw with safe=true)
    this.validateIdentity('TRY_DECRYPT_RAW(encrypted, key, iv)');
    this.validateIdentity('TRY_DECRYPT_RAW(encrypted, key, iv, aad)');
    this.validateIdentity('TRY_DECRYPT_RAW(encrypted, key, iv, aad, \'AES-GCM\')');
    this.validateIdentity('TRY_DECRYPT_RAW(encrypted, key, iv, aad, \'AES-GCM\', aead)');

  }

  testUpdateStatement () {
    this.validateIdentity('UPDATE test SET t = 1 FROM t1');
    this.validateIdentity('UPDATE test SET t = 1 FROM t2 JOIN t3 ON t2.id = t3.id');
    this.validateIdentity(
      'UPDATE test SET t = 1 FROM (SELECT id FROM test2) AS t2 JOIN test3 AS t3 ON t2.id = t3.id',
    );

    this.validateIdentity(
      'UPDATE sometesttable u FROM (SELECT 5195 AS new_count, \'01bee1e5-0000-d31e-0000-e80ef02b9f27\' query_id ) b SET qry_hash_count = new_count WHERE u.sample_query_id  = b.query_id',
      'UPDATE sometesttable AS u SET qry_hash_count = new_count FROM (SELECT 5195 AS new_count, \'01bee1e5-0000-d31e-0000-e80ef02b9f27\' AS query_id) AS b WHERE u.sample_query_id = b.query_id',
    );

  }

  testTypeSensitiveBitshiftTranspilation () {
    let ast = annotateTypes(this.parseOne('SELECT BITSHIFTLEFT(X\'FF\', 4)'), {
      dialect: 'snowflake',
    });

    expect(ast.sql({
      dialect: 'duckdb',
    })).toBe('SELECT CAST(CAST(UNHEX(\'FF\') AS BIT) << 4 AS BLOB)');

    ast = annotateTypes(this.parseOne('SELECT BITSHIFTRIGHT(X\'FF\', 4)'), {
      dialect: 'snowflake',
    });
    expect(ast.sql({
      dialect: 'duckdb',
    })).toBe('SELECT CAST(CAST(UNHEX(\'FF\') AS BIT) >> 4 AS BLOB)');

  }

  testArrayFlatten () {
    // String array flattening
    this.validateAll(
      'SELECT ARRAY_FLATTEN([[\'a\', \'b\'], [\'c\', \'d\', \'e\']])',
      {
        write: {
          'snowflake': 'SELECT ARRAY_FLATTEN([[\'a\', \'b\'], [\'c\', \'d\', \'e\']])',
          'duckdb': 'SELECT FLATTEN([[\'a\', \'b\'], [\'c\', \'d\', \'e\']])',
          'starrocks': 'SELECT ARRAY_FLATTEN([[\'a\', \'b\'], [\'c\', \'d\', \'e\']])',
        },
      },
    );

    // Nested arrays (single level flattening)
    this.validateAll(
      'SELECT ARRAY_FLATTEN([[[1, 2], [3]], [[4], [5]]])',
      {
        write: {
          'snowflake': 'SELECT ARRAY_FLATTEN([[[1, 2], [3]], [[4], [5]]])',
          'duckdb': 'SELECT FLATTEN([[[1, 2], [3]], [[4], [5]]])',
        },
      },
    );

    // Array with NULL elements
    this.validateAll(
      'SELECT ARRAY_FLATTEN([[1, NULL, 3], [4]])',
      {
        write: {
          'snowflake': 'SELECT ARRAY_FLATTEN([[1, NULL, 3], [4]])',
          'duckdb': 'SELECT FLATTEN([[1, NULL, 3], [4]])',
        },
      },
    );

    // Empty arrays
    this.validateAll(
      'SELECT ARRAY_FLATTEN([[]])',
      {
        write: {
          'snowflake': 'SELECT ARRAY_FLATTEN([[]])',
          'duckdb': 'SELECT FLATTEN([[]])',
        },
      },
    );

  }

  testSpace () {
    // Integer literal
    this.validateAll(
      'SELECT SPACE(5)',
      {
        write: {
          'snowflake': 'SELECT REPEAT(\' \', 5)',
          'duckdb': 'SELECT REPEAT(\' \', CAST(5 AS BIGINT))',
        },
      },
    );

    // Float literal (tests rounding behavior)
    this.validateAll(
      'SELECT SPACE(3.7)',
      {
        write: {
          'snowflake': 'SELECT REPEAT(\' \', 3.7)',
          'duckdb': 'SELECT REPEAT(\' \', CAST(3.7 AS BIGINT))',
        },
      },
    );

    // NULL value
    this.validateAll(
      'SELECT SPACE(NULL)',
      {
        write: {
          'snowflake': 'SELECT REPEAT(\' \', NULL)',
          'duckdb': 'SELECT REPEAT(\' \', CAST(NULL AS BIGINT))',
        },
      },
    );

  }

  testDirectedJoins () {
    this.validateIdentity('SELECT * FROM a CROSS DIRECTED JOIN b USING (id)');
    this.validateIdentity('SELECT * FROM a INNER DIRECTED JOIN b USING (id)');
    this.validateIdentity('SELECT * FROM a NATURAL INNER DIRECTED JOIN b USING (id)');

    for (const joinSide of [
      'LEFT',
      'RIGHT',
      'FULL',
    ]) {
      for (const outer of [
        '',
        ' OUTER',
      ]) {
        for (const natural of [
          '',
          'NATURAL ',
        ]) {
          const prefix = natural + joinSide + outer + ' DIRECTED';

          this.validateIdentity(`SELECT * FROM a ${prefix} JOIN b USING (id)`);
        }
      }
    }
  }

}

describe('TestSnowflake', () => {
  const validator = new TestSnowflake();

  test('test snowflake', () => {
    validator.testSnowflake();
  });

  test('test null treatment', () => {
    validator.testNullTreatment();
  });

  test('test staged files', () => {
    validator.testStagedFiles();
  });

  test('test sample', () => {
    validator.testSample();
  });

  test('test timestamps', () => {
    validator.testTimestamps();
  });

  test('test to date', () => {
    validator.testToDate();
  });

  test('test trunc', () => {
    validator.testTrunc();
  });

  test('test semi structured types', () => {
    validator.testSemiStructuredTypes();
  });

  test('test next day', () => {
    validator.testNextDay();
  });

  test('test previous day', () => {
    validator.testPreviousDay();
  });

  test('test historical data', () => {
    validator.testHistoricalData();
  });

  test('test ddl', () => {
    validator.testDdl();
  });

  test('test user defined functions', () => {
    validator.testUserDefinedFunctions();
  });

  test('test stored procedures', () => {
    validator.testStoredProcedures();
  });

  test('test table function', () => {
    validator.testTableFunction();
  });

  test('test flatten', () => {
    validator.testFlatten();
  });

  test('test minus', () => {
    validator.testMinus();
  });

  test('test values', () => {
    validator.testValues();
  });

  test('test describe', () => {
    validator.testDescribe();
  });

  test('test parse like any', () => {
    validator.testParseLikeAny();
  });

  test('test regexp substr', () => {
    validator.testRegexpSubstr();
  });

  test('test regexp replace', () => {
    validator.testRegexpReplace();
  });

  test('test replace', () => {
    validator.testReplace();
  });

  test('test match recognize', () => {
    validator.testMatchRecognize();
  });

  test('test show users', () => {
    validator.testShowUsers();
  });

  test('test show databases', () => {
    validator.testShowDatabases();
  });

  test('test show file formats', () => {
    validator.testShowFileFormats();
  });

  test('test show functions', () => {
    validator.testShowFunctions();
  });

  test('test show procedures', () => {
    validator.testShowProcedures();
  });

  test('test show stages', () => {
    validator.testShowStages();
  });

  test('test show warehouses', () => {
    validator.testShowWarehouses();
  });

  test('test show schemas', () => {
    validator.testShowSchemas();
  });

  test('test show objects', () => {
    validator.testShowObjects();
  });

  test('test show columns', () => {
    validator.testShowColumns();
  });

  test('test show tables', () => {
    validator.testShowTables();
  });

  test('test show primary keys', () => {
    validator.testShowPrimaryKeys();
  });

  test('test show views', () => {
    validator.testShowViews();
  });

  test('test show unique keys', () => {
    validator.testShowUniqueKeys();
  });

  test('test show imported keys', () => {
    validator.testShowImportedKeys();
  });

  test('test show sequences', () => {
    validator.testShowSequences();
  });

  test('test storage integration', () => {
    validator.testStorageIntegration();
  });

  test('test swap', () => {
    validator.testSwap();
  });

  test('test try cast', () => {
    validator.testTryCast();
  });

  test('test decfloat', () => {
    validator.testDecfloat();
  });

  test('test copy', () => {
    validator.testCopy();
  });

  test('test put to stage', () => {
    validator.testPutToStage();
  });

  test('test get from stage', () => {
    validator.testGetFromStage();
  });

  test('test querying semi structured data', () => {
    validator.testQueryingSemiStructuredData();
  });

  test('test alter set unset', () => {
    validator.testAlterSetUnset();
  });

  test('test alter session', () => {
    validator.testAlterSession();
  });

  test('test from changes', () => {
    validator.testFromChanges();
  });

  test('test grant', () => {
    validator.testGrant();
  });

  test('test revoke', () => {
    validator.testRevoke();
  });

  test('test window function arg', () => {
    validator.testWindowFunctionArg();
  });

  test('test offset without limit', () => {
    validator.testOffsetWithoutLimit();
  });

  test('test listagg', () => {
    validator.testListagg();
  });

  test('test rely options', () => {
    validator.testRelyOptions();
  });

  test('test parameter', () => {
    validator.testParameter();
  });

  test('test max by min by', () => {
    validator.testMaxByMinBy();
  });

  test('test create view copy grants', () => {
    validator.testCreateViewCopyGrants();
  });

  test('test semantic view', () => {
    validator.testSemanticView();
  });

  test('test get extract', () => {
    validator.testGetExtract();
  });

  test('test create sequence', () => {
    validator.testCreateSequence();
  });

  test('test bit aggs', () => {
    validator.testBitAggs();
  });

  test('test bitmap or agg', () => {
    validator.testBitmapOrAgg();
  });

  test('test md5 functions', () => {
    validator.testMd5Functions();
  });

  test('test sha1', () => {
    validator.testSha1();
  });

  test('test model attribute', () => {
    validator.testModelAttribute();
  });

  test('test set item kind attribute', () => {
    validator.testSetItemKindAttribute();
  });

  test('test round', () => {
    validator.testRound();
  });

  test('test get bit', () => {
    validator.testGetBit();
  });

  test('test to binary', () => {
    validator.testToBinary();
  });

  test('test reverse', () => {
    validator.testReverse();
  });

  test('test float interval', () => {
    validator.testFloatInterval();
  });

  test('test transpile bitwise ops', () => {
    validator.testTranspileBitwiseOps();
  });

  test('test quoting', () => {
    validator.testQuoting();
  });

  test('test floor', () => {
    validator.testFloor();
  });

  test('test seq functions', () => {
    validator.testSeqFunctions();
  });

  test('test generator', () => {
    validator.testGenerator();
  });

  test('test ceil', () => {
    validator.testCeil();
  });

  test('test corr', () => {
    validator.testCorr();
  });

  test('test encryption functions', () => {
    validator.testEncryptionFunctions();
  });

  test('test update statement', () => {
    validator.testUpdateStatement();
  });

  test('test type sensitive bitshift transpilation', () => {
    validator.testTypeSensitiveBitshiftTranspilation();
  });

  test('test array flatten', () => {
    validator.testArrayFlatten();
  });

  test('test space', () => {
    validator.testSpace();
  });

  test('test directed joins', () => {
    validator.testDirectedJoins();
  });
});
