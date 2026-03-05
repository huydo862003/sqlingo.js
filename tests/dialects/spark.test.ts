import {
  describe, test, expect,
} from 'vitest';
import {
  ApproxQuantileExpr, DistinctExpr, LiteralExpr, DateTruncExpr, RefreshExpr,
} from '../../src/expressions';
import { narrowInstanceOf } from '../../src/port_internals';
import { parseOne } from '../../src/index';
import {
  Validator, UnsupportedError,
} from './validator';

class TestSpark extends Validator {
  override dialect = 'spark' as const;

  testDdl () {
    this.validateIdentity('DAYOFWEEK(TO_DATE(x))');
    this.validateIdentity('DAYOFMONTH(TO_DATE(x))');
    this.validateIdentity('DAYOFYEAR(TO_DATE(x))');
    this.validateIdentity('WEEKOFYEAR(TO_DATE(x))');
    this.validateIdentity('SELECT MODE(category)');
    this.validateIdentity('SELECT MODE(price, TRUE) AS deterministic_mode FROM products');
    this.validateIdentity('SELECT MODE() WITHIN GROUP (ORDER BY status) FROM orders');
    this.validateIdentity('DROP NAMESPACE my_catalog.my_namespace');
    this.validateIdentity('CREATE NAMESPACE my_catalog.my_namespace');
    this.validateIdentity('INSERT OVERWRITE TABLE db1.tb1 TABLE db2.tb2');
    this.validateIdentity('CREATE TABLE foo AS WITH t AS (SELECT 1 AS col) SELECT col FROM t');
    this.validateIdentity('CREATE TEMPORARY VIEW test AS SELECT 1');
    this.validateIdentity('CREATE TABLE foo (col VARCHAR(50))');
    this.validateIdentity('CREATE TABLE foo (col STRUCT<struct_col_a: VARCHAR((50))>)');
    this.validateIdentity('CREATE TABLE foo (col STRING) CLUSTERED BY (col) INTO 10 BUCKETS');
    this.validateIdentity(
      'CREATE TABLE foo (col STRING) CLUSTERED BY (col) SORTED BY (col) INTO 10 BUCKETS',
    );
    this.validateIdentity('TRUNCATE TABLE t1 PARTITION(age = 10, name = \'test\', address)');

    this.validateAll(
      'CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:string>)',
      {
        write: {
          duckdb: 'CREATE TABLE db.example_table (col_a STRUCT(struct_col_a INT, struct_col_b TEXT))',
          presto: 'CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b VARCHAR))',
          hive: 'CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRING>)',
          spark: 'CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRING>)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:struct<nested_col_a:string, nested_col_b:string>>)',
      {
        write: {
          bigquery: 'CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)',
          duckdb: 'CREATE TABLE db.example_table (col_a STRUCT(struct_col_a INT, struct_col_b STRUCT(nested_col_a TEXT, nested_col_b TEXT)))',
          presto: 'CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b ROW(nested_col_a VARCHAR, nested_col_b VARCHAR)))',
          hive: 'CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRUCT<nested_col_a: STRING, nested_col_b: STRING>>)',
          spark: 'CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRUCT<nested_col_a: STRING, nested_col_b: STRING>>)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE db.example_table (col_a array<int>, col_b array<array<int>>)',
      {
        write: {
          bigquery: 'CREATE TABLE db.example_table (col_a ARRAY<INT64>, col_b ARRAY<ARRAY<INT64>>)',
          duckdb: 'CREATE TABLE db.example_table (col_a INT[], col_b INT[][])',
          presto: 'CREATE TABLE db.example_table (col_a ARRAY(INTEGER), col_b ARRAY(ARRAY(INTEGER)))',
          hive: 'CREATE TABLE db.example_table (col_a ARRAY<INT>, col_b ARRAY<ARRAY<INT>>)',
          spark: 'CREATE TABLE db.example_table (col_a ARRAY<INT>, col_b ARRAY<ARRAY<INT>>)',
          snowflake: 'CREATE TABLE db.example_table (col_a ARRAY, col_b ARRAY)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE x USING ICEBERG PARTITIONED BY (MONTHS(y)) LOCATION \'s3://z\'',
      {
        write: {
          duckdb: 'CREATE TABLE x',
          presto: 'CREATE TABLE x WITH (format=\'ICEBERG\', PARTITIONED_BY=ARRAY[\'MONTHS(y)\'])',
          hive: 'CREATE TABLE x STORED AS ICEBERG PARTITIONED BY (MONTHS(y)) LOCATION \'s3://z\'',
          spark: 'CREATE TABLE x USING ICEBERG PARTITIONED BY (MONTHS(y)) LOCATION \'s3://z\'',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE test STORED AS PARQUET AS SELECT 1',
      {
        write: {
          duckdb: 'CREATE TABLE test AS SELECT 1',
          presto: 'CREATE TABLE test WITH (format=\'PARQUET\') AS SELECT 1',
          trino: 'CREATE TABLE test WITH (format=\'PARQUET\') AS SELECT 1',
          athena: 'CREATE TABLE test WITH (format=\'PARQUET\') AS SELECT 1', // note: lowercase format property is important for Athena
          hive: 'CREATE TABLE test STORED AS PARQUET AS SELECT 1',
          spark: 'CREATE TABLE test STORED AS PARQUET AS SELECT 1',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE blah (col_a INT) COMMENT "Test comment: blah" PARTITIONED BY (date STRING) USING ICEBERG TBLPROPERTIES(\'x\' = \'1\')',
      {
        write: {
          duckdb: `CREATE TABLE blah (
  col_a INT
)`, // Partition columns should exist in table
          presto: `CREATE TABLE blah (
  col_a INTEGER,
  date VARCHAR
)
COMMENT 'Test comment: blah'
WITH (
  PARTITIONED_BY=ARRAY['date'],
  format='ICEBERG',
  x='1'
)`,
          hive: `CREATE TABLE blah (
  col_a INT
)
COMMENT 'Test comment: blah'
PARTITIONED BY (
  date STRING
)
STORED AS ICEBERG
TBLPROPERTIES (
  'x'='1'
)`,
          spark: `CREATE TABLE blah (
  col_a INT,
  date STRING
)
COMMENT 'Test comment: blah'
PARTITIONED BY (
  date
)
USING ICEBERG
TBLPROPERTIES (
  'x'='1'
)`,
        },
        pretty: true,
      },
    );

    this.validateAll(
      'CACHE TABLE testCache OPTIONS (\'storageLevel\' \'DISK_ONLY\') SELECT * FROM testData',
      {
        write: {
          spark: 'CACHE TABLE testCache OPTIONS(\'storageLevel\' = \'DISK_ONLY\') AS SELECT * FROM testData',
        },
      },
    );
    this.validateAll(
      'ALTER TABLE StudentInfo ADD COLUMNS (LastName STRING, DOB TIMESTAMP)',
      {
        write: {
          spark: 'ALTER TABLE StudentInfo ADD COLUMNS (LastName STRING, DOB TIMESTAMP)',
        },
      },
    );
    this.validateAll(
      'ALTER TABLE db.example ALTER COLUMN col_a TYPE BIGINT',
      {
        write: {
          spark: 'ALTER TABLE db.example ALTER COLUMN col_a TYPE BIGINT',
          hive: 'ALTER TABLE db.example CHANGE COLUMN col_a col_a BIGINT',
        },
      },
    );
    this.validateAll(
      'ALTER TABLE db.example CHANGE COLUMN col_a col_a BIGINT',
      {
        write: {
          spark: 'ALTER TABLE db.example ALTER COLUMN col_a TYPE BIGINT',
          hive: 'ALTER TABLE db.example CHANGE COLUMN col_a col_a BIGINT',
        },
      },
    );
    this.validateAll(
      'ALTER TABLE db.example RENAME COLUMN col_a TO col_b',
      {
        write: {
          spark: 'ALTER TABLE db.example RENAME COLUMN col_a TO col_b',
          hive: UnsupportedError,
        },
      },
    );
    this.validateAll(
      'ALTER TABLE StudentInfo DROP COLUMNS (LastName, DOB)',
      {
        write: {
          spark: 'ALTER TABLE StudentInfo DROP COLUMNS (LastName, DOB)',
        },
      },
    );
    this.validateIdentity('ALTER VIEW StudentInfoView AS SELECT * FROM StudentInfo');
    this.validateIdentity('ALTER VIEW StudentInfoView AS SELECT LastName FROM StudentInfo');
    this.validateIdentity('ALTER VIEW StudentInfoView RENAME TO StudentInfoViewRenamed');
    this.validateIdentity(
      'ALTER VIEW StudentInfoView SET TBLPROPERTIES (\'key1\'=\'val1\', \'key2\'=\'val2\')',
    );
    this.validateIdentity(
      'ALTER VIEW StudentInfoView UNSET TBLPROPERTIES (\'key1\', \'key2\')',
      undefined,
      { checkCommandWarning: true },
    );
  }

  testToDate () {
    this.validateAll(
      'TO_DATE(x, \'yyyy-MM-dd\')',
      {
        write: {
          duckdb: 'TRY_CAST(x AS DATE)',
          hive: 'TO_DATE(x)',
          presto: 'CAST(CAST(x AS TIMESTAMP) AS DATE)',
          spark: 'TO_DATE(x)',
          snowflake: 'TRY_TO_DATE(x, \'yyyy-mm-DD\')',
          databricks: 'TO_DATE(x)',
        },
      },
    );
    this.validateAll(
      'TO_DATE(x, \'yyyy\')',
      {
        write: {
          duckdb: 'CAST(CAST(TRY_STRPTIME(x, \'%Y\') AS TIMESTAMP) AS DATE)',
          hive: 'TO_DATE(x, \'yyyy\')',
          presto: 'CAST(DATE_PARSE(x, \'%Y\') AS DATE)',
          spark: 'TO_DATE(x, \'yyyy\')',
          snowflake: 'TRY_TO_DATE(x, \'yyyy\')',
          databricks: 'TO_DATE(x, \'yyyy\')',
        },
      },
    );
  }

  testHint () {
    this.validateAll(
      'SELECT /*+ COALESCE(3) */ * FROM x',
      {
        write: {
          spark: 'SELECT /*+ COALESCE(3) */ * FROM x',
          bigquery: 'SELECT * FROM x',
        },
      },
    );
    this.validateAll(
      'SELECT /*+ COALESCE(3), REPARTITION(1) */ * FROM x',
      {
        write: {
          spark: 'SELECT /*+ COALESCE(3), REPARTITION(1) */ * FROM x',
          bigquery: 'SELECT * FROM x',
        },
      },
    );
    this.validateAll(
      'SELECT /*+ BROADCAST(table) */ cola FROM table',
      {
        write: {
          spark: 'SELECT /*+ BROADCAST(table) */ cola FROM table',
          bigquery: 'SELECT cola FROM table',
        },
      },
    );
    this.validateAll(
      'SELECT /*+ BROADCASTJOIN(table) */ cola FROM table',
      {
        write: {
          spark: 'SELECT /*+ BROADCASTJOIN(table) */ cola FROM table',
          bigquery: 'SELECT cola FROM table',
        },
      },
    );
    this.validateAll(
      'SELECT /*+ MAPJOIN(table) */ cola FROM table',
      {
        write: {
          spark: 'SELECT /*+ MAPJOIN(table) */ cola FROM table',
          bigquery: 'SELECT cola FROM table',
        },
      },
    );
    this.validateAll(
      'SELECT /*+ MERGE(table) */ cola FROM table',
      {
        write: {
          spark: 'SELECT /*+ MERGE(table) */ cola FROM table',
          bigquery: 'SELECT cola FROM table',
        },
      },
    );
    this.validateAll(
      'SELECT /*+ SHUFFLEMERGE(table) */ cola FROM table',
      {
        write: {
          spark: 'SELECT /*+ SHUFFLEMERGE(table) */ cola FROM table',
          bigquery: 'SELECT cola FROM table',
        },
      },
    );
    this.validateAll(
      'SELECT /*+ MERGEJOIN(table) */ cola FROM table',
      {
        write: {
          spark: 'SELECT /*+ MERGEJOIN(table) */ cola FROM table',
          bigquery: 'SELECT cola FROM table',
        },
      },
    );
    this.validateAll(
      'SELECT /*+ SHUFFLE_HASH(table) */ cola FROM table',
      {
        write: {
          spark: 'SELECT /*+ SHUFFLE_HASH(table) */ cola FROM table',
          bigquery: 'SELECT cola FROM table',
        },
      },
    );
    this.validateAll(
      'SELECT /*+ SHUFFLE_REPLICATE_NL(table) */ cola FROM table',
      {
        write: {
          spark: 'SELECT /*+ SHUFFLE_REPLICATE_NL(table) */ cola FROM table',
          bigquery: 'SELECT cola FROM table',
        },
      },
    );
  }

  testSpark () {
    expect(
      parseOne('REFRESH TABLE t', { read: 'spark' }).sql({ dialect: 'spark' }),
    ).toBe('REFRESH TABLE t');
    parseOne('REFRESH TABLE t', { read: 'spark' }).assertIs(RefreshExpr);

    // Spark TRUNC is date-only, should parse to DateTrunc (not numeric Trunc)
    this.validateIdentity('TRUNC(date_col, \'MM\')').assertIs(DateTruncExpr);

    // Numeric TRUNC from other dialects - Spark has no native support, uses CAST to BIGINT
    this.validateAll(
      'CAST(3.14159 AS BIGINT)',
      { read: { postgres: 'TRUNC(3.14159, 2)' } },
    );

    this.validateIdentity('SELECT APPROX_TOP_K_ACCUMULATE(col, 10)');
    this.validateIdentity('SELECT APPROX_TOP_K_ACCUMULATE(col)');
    this.validateIdentity('SELECT BITMAP_BIT_POSITION(10)');
    this.validateIdentity('SELECT BITMAP_CONSTRUCT_AGG(value)');
    this.validateIdentity('ALTER TABLE foo ADD PARTITION(event = \'click\')');
    this.validateIdentity('ALTER TABLE foo ADD IF NOT EXISTS PARTITION(event = \'click\')');
    this.validateIdentity('IF(cond, foo AS bar, bla AS baz)');
    this.validateIdentity('any_value(col, true)', 'ANY_VALUE(col) IGNORE NULLS');
    this.validateIdentity('first(col, true)', 'FIRST(col) IGNORE NULLS');
    this.validateIdentity('first_value(col, true)', 'FIRST_VALUE(col) IGNORE NULLS');
    this.validateIdentity('last(col, true)', 'LAST(col) IGNORE NULLS');
    this.validateIdentity('last_value(col, true)', 'LAST_VALUE(col) IGNORE NULLS');
    this.validateIdentity('DESCRIBE EXTENDED db.tbl');
    this.validateIdentity('SELECT * FROM test TABLESAMPLE (50 PERCENT)');
    this.validateIdentity('SELECT * FROM test TABLESAMPLE (5 ROWS)');
    this.validateIdentity('SELECT * FROM test TABLESAMPLE (BUCKET 4 OUT OF 10)');
    this.validateIdentity('REFRESH \'hdfs://path/to/table\'');
    this.validateIdentity('REFRESH TABLE tempDB.view1');
    this.validateIdentity('SELECT CASE WHEN a = NULL THEN 1 ELSE 2 END');
    this.validateIdentity('SELECT * FROM t1 SEMI JOIN t2 ON t1.x = t2.x');
    this.validateIdentity('SELECT TRANSFORM(ARRAY(1, 2, 3), x -> x + 1)');
    this.validateIdentity('SELECT TRANSFORM(ARRAY(1, 2, 3), (x, i) -> x + i)');
    this.validateIdentity('REFRESH TABLE a.b.c');
    this.validateIdentity('INTERVAL \'-86\' DAYS');
    this.validateIdentity('TRIM(\'    SparkSQL   \')');
    this.validateIdentity('TRIM(BOTH \'SL\' FROM \'SSparkSQLS\')');
    this.validateIdentity('TRIM(LEADING \'SL\' FROM \'SSparkSQLS\')');
    this.validateIdentity('TRIM(TRAILING \'SL\' FROM \'SSparkSQLS\')');
    this.validateIdentity('SPLIT(str, pattern, lim)');
    this.validateIdentity(
      'SELECT * FROM t1, t2',
      'SELECT * FROM t1 CROSS JOIN t2',
    );
    this.validateIdentity(
      'SELECT 1 limit',
      'SELECT 1 AS limit',
    );
    this.validateIdentity(
      'SELECT 1 offset',
      'SELECT 1 AS offset',
    );
    this.validateIdentity(
      'SELECT UNIX_TIMESTAMP()',
      'SELECT UNIX_TIMESTAMP(CURRENT_TIMESTAMP())',
    );
    this.validateIdentity(
      'SELECT CAST(\'2023-01-01\' AS TIMESTAMP) + INTERVAL 23 HOUR + 59 MINUTE + 59 SECONDS',
      'SELECT CAST(\'2023-01-01\' AS TIMESTAMP) + INTERVAL \'23\' HOUR + INTERVAL \'59\' MINUTE + INTERVAL \'59\' SECONDS',
    );
    this.validateIdentity(
      'SELECT CAST(\'2023-01-01\' AS TIMESTAMP) + INTERVAL \'23\' HOUR + \'59\' MINUTE + \'59\' SECONDS',
      'SELECT CAST(\'2023-01-01\' AS TIMESTAMP) + INTERVAL \'23\' HOUR + INTERVAL \'59\' MINUTE + INTERVAL \'59\' SECONDS',
    );
    this.validateIdentity(
      'SELECT INTERVAL \'5\' HOURS \'30\' MINUTES \'5\' SECONDS \'6\' MILLISECONDS \'7\' MICROSECONDS',
      'SELECT INTERVAL \'5\' HOURS + INTERVAL \'30\' MINUTES + INTERVAL \'5\' SECONDS + INTERVAL \'6\' MILLISECONDS + INTERVAL \'7\' MICROSECONDS',
    );
    this.validateIdentity(
      'SELECT INTERVAL 5 HOURS 30 MINUTES 5 SECONDS 6 MILLISECONDS 7 MICROSECONDS',
      'SELECT INTERVAL \'5\' HOURS + INTERVAL \'30\' MINUTES + INTERVAL \'5\' SECONDS + INTERVAL \'6\' MILLISECONDS + INTERVAL \'7\' MICROSECONDS',
    );
    this.validateIdentity(
      'SELECT REGEXP_REPLACE(\'100-200\', r\'([^0-9])\', \'\')',
      'SELECT REGEXP_REPLACE(\'100-200\', \'([^0-9])\', \'\')',
    );
    this.validateIdentity(
      'SELECT REGEXP_REPLACE(\'100-200\', R\'([^0-9])\', \'\')',
      'SELECT REGEXP_REPLACE(\'100-200\', \'([^0-9])\', \'\')',
    );
    this.validateIdentity(
      'SELECT STR_TO_MAP(\'a:1,b:2,c:3\')',
      'SELECT STR_TO_MAP(\'a:1,b:2,c:3\', \',\', \':\')',
    );

    this.validateAll(
      'SELECT * FROM parquet.`name.parquet`',
      {
        read: {
          duckdb: 'SELECT * FROM READ_PARQUET(\'name.parquet\')',
          spark: 'SELECT * FROM parquet.`name.parquet`',
        },
      },
    );
    this.validateAll(
      'SELECT TO_JSON(STRUCT(\'blah\' AS x)) AS y',
      {
        write: {
          presto: 'SELECT JSON_FORMAT(CAST(CAST(ROW(\'blah\') AS ROW(x VARCHAR)) AS JSON)) AS y',
          spark: 'SELECT TO_JSON(STRUCT(\'blah\' AS x)) AS y',
          trino: 'SELECT JSON_FORMAT(CAST(CAST(ROW(\'blah\') AS ROW(x VARCHAR)) AS JSON)) AS y',
        },
      },
    );
    this.validateAll(
      'SELECT TRY_ELEMENT_AT(ARRAY(1, 2, 3), 2)',
      {
        read: {
          databricks: 'SELECT TRY_ELEMENT_AT(ARRAY(1, 2, 3), 2)',
          presto: 'SELECT ELEMENT_AT(ARRAY[1, 2, 3], 2)',
        },
        write: {
          'databricks': 'SELECT TRY_ELEMENT_AT(ARRAY(1, 2, 3), 2)',
          'spark': 'SELECT TRY_ELEMENT_AT(ARRAY(1, 2, 3), 2)',
          'duckdb': 'SELECT [1, 2, 3][2]',
          'duckdb, version=1.1.0': 'SELECT ([1, 2, 3])[2]',
          'presto': 'SELECT ELEMENT_AT(ARRAY[1, 2, 3], 2)',
        },
      },
    );

    this.validateAll(
      'SELECT id_column, name, age FROM test_table LATERAL VIEW INLINE(struc_column) explode_view AS name, age',
      {
        write: {
          presto: 'SELECT id_column, name, age FROM test_table CROSS JOIN UNNEST(struc_column) AS explode_view(name, age)',
          spark: 'SELECT id_column, name, age FROM test_table LATERAL VIEW INLINE(struc_column) explode_view AS name, age',
        },
      },
    );
    this.validateAll(
      'SELECT ARRAY_AGG(x) FILTER (WHERE x = 5) FROM (SELECT 1 UNION ALL SELECT NULL) AS t(x)',
      {
        write: {
          duckdb: 'SELECT ARRAY_AGG(x) FILTER(WHERE x = 5 AND NOT x IS NULL) FROM (SELECT 1 UNION ALL SELECT NULL) AS t(x)',
          spark: 'SELECT COLLECT_LIST(x) FILTER(WHERE x = 5) FROM (SELECT 1 UNION ALL SELECT NULL) AS t(x)',
        },
      },
    );
    this.validateAll(
      'SELECT ARRAY_AGG(1)',
      {
        write: {
          duckdb: 'SELECT ARRAY_AGG(1)',
          spark: 'SELECT COLLECT_LIST(1)',
        },
      },
    );
    this.validateAll(
      'SELECT ARRAY_AGG(DISTINCT STRUCT(\'a\'))',
      {
        write: {
          duckdb: 'SELECT ARRAY_AGG(DISTINCT {\'col1\': \'a\'})',
          spark: 'SELECT COLLECT_LIST(DISTINCT STRUCT(\'a\' AS col1))',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_FORMAT(DATE \'2020-01-01\', \'EEEE\') AS weekday',
      {
        write: {
          presto: 'SELECT DATE_FORMAT(CAST(CAST(\'2020-01-01\' AS DATE) AS TIMESTAMP), \'%W\') AS weekday',
          spark: 'SELECT DATE_FORMAT(CAST(\'2020-01-01\' AS DATE), \'EEEE\') AS weekday',
        },
      },
    );
    this.validateAll(
      'SELECT TRY_ELEMENT_AT(MAP(1, \'a\', 2, \'b\'), 2)',
      {
        read: {
          databricks: 'SELECT TRY_ELEMENT_AT(MAP(1, \'a\', 2, \'b\'), 2)',
        },
        write: {
          'databricks': 'SELECT TRY_ELEMENT_AT(MAP(1, \'a\', 2, \'b\'), 2)',
          'duckdb': 'SELECT MAP([1, 2], [\'a\', \'b\'])[2]',
          'duckdb, version=1.1.0': 'SELECT (MAP([1, 2], [\'a\', \'b\'])[2])[1]',
          'spark': 'SELECT TRY_ELEMENT_AT(MAP(1, \'a\', 2, \'b\'), 2)',
        },
      },
    );
    this.validateAll(
      'SELECT SPLIT(\'123|789\', \'\\\\|\')',
      {
        read: {
          duckdb: 'SELECT STR_SPLIT_REGEX(\'123|789\', \'\\|\')',
          presto: 'SELECT REGEXP_SPLIT(\'123|789\', \'\\|\')',
        },
        write: {
          duckdb: 'SELECT STR_SPLIT_REGEX(\'123|789\', \'\\|\')',
          presto: 'SELECT REGEXP_SPLIT(\'123|789\', \'\\|\')',
          spark: 'SELECT SPLIT(\'123|789\', \'\\\\|\')',
        },
      },
    );
    this.validateAll(
      'WITH tbl AS (SELECT 1 AS id, \'eggy\' AS name UNION ALL SELECT NULL AS id, \'jake\' AS name) SELECT COUNT(DISTINCT id, name) AS cnt FROM tbl',
      {
        write: {
          clickhouse: 'WITH tbl AS (SELECT 1 AS id, \'eggy\' AS name UNION ALL SELECT NULL AS id, \'jake\' AS name) SELECT COUNT(DISTINCT id, name) AS cnt FROM tbl',
          databricks: 'WITH tbl AS (SELECT 1 AS id, \'eggy\' AS name UNION ALL SELECT NULL AS id, \'jake\' AS name) SELECT COUNT(DISTINCT id, name) AS cnt FROM tbl',
          doris: 'WITH tbl AS (SELECT 1 AS id, \'eggy\' AS name UNION ALL SELECT NULL AS id, \'jake\' AS `name`) SELECT COUNT(DISTINCT id, `name`) AS cnt FROM tbl',
          duckdb: 'WITH tbl AS (SELECT 1 AS id, \'eggy\' AS name UNION ALL SELECT NULL AS id, \'jake\' AS name) SELECT COUNT(DISTINCT CASE WHEN id IS NULL THEN NULL WHEN name IS NULL THEN NULL ELSE (id, name) END) AS cnt FROM tbl',
          hive: 'WITH tbl AS (SELECT 1 AS id, \'eggy\' AS name UNION ALL SELECT NULL AS id, \'jake\' AS name) SELECT COUNT(DISTINCT id, name) AS cnt FROM tbl',
          mysql: 'WITH tbl AS (SELECT 1 AS id, \'eggy\' AS name UNION ALL SELECT NULL AS id, \'jake\' AS name) SELECT COUNT(DISTINCT id, name) AS cnt FROM tbl',
          postgres: 'WITH tbl AS (SELECT 1 AS id, \'eggy\' AS name UNION ALL SELECT NULL AS id, \'jake\' AS name) SELECT COUNT(DISTINCT CASE WHEN id IS NULL THEN NULL WHEN name IS NULL THEN NULL ELSE (id, name) END) AS cnt FROM tbl',
          presto: 'WITH tbl AS (SELECT 1 AS id, \'eggy\' AS name UNION ALL SELECT NULL AS id, \'jake\' AS name) SELECT COUNT(DISTINCT CASE WHEN id IS NULL THEN NULL WHEN name IS NULL THEN NULL ELSE (id, name) END) AS cnt FROM tbl',
          snowflake: 'WITH tbl AS (SELECT 1 AS id, \'eggy\' AS name UNION ALL SELECT NULL AS id, \'jake\' AS name) SELECT COUNT(DISTINCT id, name) AS cnt FROM tbl',
          spark: 'WITH tbl AS (SELECT 1 AS id, \'eggy\' AS name UNION ALL SELECT NULL AS id, \'jake\' AS name) SELECT COUNT(DISTINCT id, name) AS cnt FROM tbl',
        },
      },
    );
    this.validateAll(
      'SELECT TO_UTC_TIMESTAMP(\'2016-08-31\', \'Asia/Seoul\')',
      {
        write: {
          bigquery: 'SELECT DATETIME(TIMESTAMP(CAST(\'2016-08-31\' AS DATETIME), \'Asia/Seoul\'), \'UTC\')',
          duckdb: 'SELECT CAST(\'2016-08-31\' AS TIMESTAMP) AT TIME ZONE \'Asia/Seoul\' AT TIME ZONE \'UTC\'',
          postgres: 'SELECT CAST(\'2016-08-31\' AS TIMESTAMP) AT TIME ZONE \'Asia/Seoul\' AT TIME ZONE \'UTC\'',
          presto: 'SELECT WITH_TIMEZONE(CAST(\'2016-08-31\' AS TIMESTAMP), \'Asia/Seoul\') AT TIME ZONE \'UTC\'',
          redshift: 'SELECT CAST(\'2016-08-31\' AS TIMESTAMP) AT TIME ZONE \'Asia/Seoul\' AT TIME ZONE \'UTC\'',
          snowflake: 'SELECT CONVERT_TIMEZONE(\'Asia/Seoul\', \'UTC\', CAST(\'2016-08-31\' AS TIMESTAMP))',
          spark: 'SELECT TO_UTC_TIMESTAMP(CAST(\'2016-08-31\' AS TIMESTAMP), \'Asia/Seoul\')',
        },
      },
    );
    this.validateAll(
      'SELECT FROM_UTC_TIMESTAMP(\'2016-08-31\', \'Asia/Seoul\')',
      {
        write: {
          presto: 'SELECT AT_TIMEZONE(CAST(\'2016-08-31\' AS TIMESTAMP), \'Asia/Seoul\')',
          spark: 'SELECT FROM_UTC_TIMESTAMP(CAST(\'2016-08-31\' AS TIMESTAMP), \'Asia/Seoul\')',
        },
      },
    );
    this.validateAll(
      'foo.bar',
      {
        read: {
          '': 'STRUCT_EXTRACT(foo, bar)',
        },
      },
    );
    this.validateAll(
      'MAP(1, 2, 3, 4)',
      {
        write: {
          spark: 'MAP(1, 2, 3, 4)',
          trino: 'MAP(ARRAY[1, 3], ARRAY[2, 4])',
        },
      },
    );
    this.validateAll(
      'MAP()',
      {
        read: {
          spark: 'MAP()',
          trino: 'MAP()',
        },
        write: {
          trino: 'MAP(ARRAY[], ARRAY[])',
        },
      },
    );
    this.validateAll(
      'SELECT STR_TO_MAP(\'a:1,b:2,c:3\', \',\', \':\')',
      {
        read: {
          presto: 'SELECT SPLIT_TO_MAP(\'a:1,b:2,c:3\', \',\', \':\')',
          spark: 'SELECT STR_TO_MAP(\'a:1,b:2,c:3\', \',\', \':\')',
        },
        write: {
          presto: 'SELECT SPLIT_TO_MAP(\'a:1,b:2,c:3\', \',\', \':\')',
          spark: 'SELECT STR_TO_MAP(\'a:1,b:2,c:3\', \',\', \':\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(MONTH, CAST(\'1996-10-30\' AS TIMESTAMP), CAST(\'1997-02-28 10:30:00\' AS TIMESTAMP))',
      {
        read: {
          duckdb: 'SELECT DATEDIFF(\'month\', CAST(\'1996-10-30\' AS TIMESTAMPTZ), CAST(\'1997-02-28 10:30:00\' AS TIMESTAMPTZ))',
        },
        write: {
          spark: 'SELECT DATEDIFF(MONTH, CAST(\'1996-10-30\' AS TIMESTAMP), CAST(\'1997-02-28 10:30:00\' AS TIMESTAMP))',
          spark2: 'SELECT CAST(MONTHS_BETWEEN(CAST(\'1997-02-28 10:30:00\' AS TIMESTAMP), CAST(\'1996-10-30\' AS TIMESTAMP)) AS INT)',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(week, \'2020-01-01\', \'2020-12-31\')',
      {
        write: {
          bigquery: 'SELECT DATE_DIFF(CAST(\'2020-12-31\' AS DATE), CAST(\'2020-01-01\' AS DATE), WEEK)',
          duckdb: 'SELECT DATE_DIFF(\'WEEK\', CAST(\'2020-01-01\' AS DATE), CAST(\'2020-12-31\' AS DATE))',
          hive: 'SELECT CAST(DATEDIFF(\'2020-12-31\', \'2020-01-01\') / 7 AS INT)',
          postgres: 'SELECT CAST(EXTRACT(days FROM (CAST(CAST(\'2020-12-31\' AS DATE) AS TIMESTAMP) - CAST(CAST(\'2020-01-01\' AS DATE) AS TIMESTAMP))) / 7 AS BIGINT)',
          redshift: 'SELECT DATEDIFF(WEEK, CAST(\'2020-01-01\' AS DATE), CAST(\'2020-12-31\' AS DATE))',
          snowflake: 'SELECT DATEDIFF(WEEK, TO_DATE(\'2020-01-01\'), TO_DATE(\'2020-12-31\'))',
          spark: 'SELECT DATEDIFF(WEEK, \'2020-01-01\', \'2020-12-31\')',
        },
      },
    );
    this.validateAll(
      'SELECT MONTHS_BETWEEN(\'1997-02-28 10:30:00\', \'1996-10-30\')',
      {
        write: {
          duckdb: 'SELECT DATE_DIFF(\'MONTH\', CAST(\'1996-10-30\' AS DATE), CAST(\'1997-02-28 10:30:00\' AS DATE)) + CASE WHEN DAY(CAST(\'1997-02-28 10:30:00\' AS DATE)) = DAY(LAST_DAY(CAST(\'1997-02-28 10:30:00\' AS DATE))) AND DAY(CAST(\'1996-10-30\' AS DATE)) = DAY(LAST_DAY(CAST(\'1996-10-30\' AS DATE))) THEN 0 ELSE (DAY(CAST(\'1997-02-28 10:30:00\' AS DATE)) - DAY(CAST(\'1996-10-30\' AS DATE))) / 31.0 END',
          hive: 'SELECT MONTHS_BETWEEN(\'1997-02-28 10:30:00\', \'1996-10-30\')',
          spark: 'SELECT MONTHS_BETWEEN(\'1997-02-28 10:30:00\', \'1996-10-30\')',
        },
      },
    );
    this.validateAll(
      'SELECT MONTHS_BETWEEN(\'1997-02-28 10:30:00\', \'1996-10-30\', FALSE)',
      {
        write: {
          duckdb: 'SELECT DATE_DIFF(\'MONTH\', CAST(\'1996-10-30\' AS DATE), CAST(\'1997-02-28 10:30:00\' AS DATE)) + CASE WHEN DAY(CAST(\'1997-02-28 10:30:00\' AS DATE)) = DAY(LAST_DAY(CAST(\'1997-02-28 10:30:00\' AS DATE))) AND DAY(CAST(\'1996-10-30\' AS DATE)) = DAY(LAST_DAY(CAST(\'1996-10-30\' AS DATE))) THEN 0 ELSE (DAY(CAST(\'1997-02-28 10:30:00\' AS DATE)) - DAY(CAST(\'1996-10-30\' AS DATE))) / 31.0 END',
          hive: 'SELECT MONTHS_BETWEEN(\'1997-02-28 10:30:00\', \'1996-10-30\')',
          spark: 'SELECT MONTHS_BETWEEN(\'1997-02-28 10:30:00\', \'1996-10-30\', FALSE)',
        },
      },
    );
    this.validateAll(
      'SELECT TO_TIMESTAMP(\'2016-12-31 00:12:00\')',
      {
        write: {
          '': 'SELECT CAST(\'2016-12-31 00:12:00\' AS TIMESTAMP)',
          'duckdb': 'SELECT CAST(\'2016-12-31 00:12:00\' AS TIMESTAMP)',
          'spark': 'SELECT CAST(\'2016-12-31 00:12:00\' AS TIMESTAMP)',
        },
      },
    );
    this.validateAll(
      'SELECT TO_TIMESTAMP(x, \'zZ\')',
      {
        write: {
          '': 'SELECT STR_TO_TIME(x, \'%Z%z\')',
          'duckdb': 'SELECT STRPTIME(x, \'%Z%z\')',
        },
      },
    );
    this.validateAll(
      'SELECT TO_TIMESTAMP(\'2016-12-31\', \'yyyy-MM-dd\')',
      {
        read: {
          duckdb: 'SELECT STRPTIME(\'2016-12-31\', \'%Y-%m-%d\')',
        },
        write: {
          '': 'SELECT STR_TO_TIME(\'2016-12-31\', \'%Y-%m-%d\')',
          'duckdb': 'SELECT STRPTIME(\'2016-12-31\', \'%Y-%m-%d\')',
          'spark': 'SELECT TO_TIMESTAMP(\'2016-12-31\', \'yyyy-MM-dd\')',
        },
      },
    );
    this.validateAll(
      'SELECT RLIKE(\'John Doe\', \'John.*\')',
      {
        write: {
          bigquery: 'SELECT REGEXP_CONTAINS(\'John Doe\', \'John.*\')',
          hive: 'SELECT \'John Doe\' RLIKE \'John.*\'',
          postgres: 'SELECT \'John Doe\' ~ \'John.*\'',
          snowflake: 'SELECT REGEXP_LIKE(\'John Doe\', \'John.*\')',
          spark: 'SELECT \'John Doe\' RLIKE \'John.*\'',
        },
      },
    );
    this.validateAll(
      'UNHEX(MD5(x))',
      {
        write: {
          bigquery: 'FROM_HEX(TO_HEX(MD5(x)))',
          spark: 'UNHEX(MD5(x))',
        },
      },
    );
    this.validateAll('SELECT * FROM ((VALUES 1))', { write: { spark: 'SELECT * FROM (VALUES (1))' } });
    this.validateAll(
      'SELECT CAST(STRUCT(\'fooo\') AS STRUCT<a: VARCHAR(2)>)',
      { write: { spark: 'SELECT CAST(STRUCT(\'fooo\' AS col1) AS STRUCT<a: STRING>)' } },
    );
    this.validateAll(
      'SELECT CAST(123456 AS VARCHAR(3))',
      {
        write: {
          '': 'SELECT TRY_CAST(123456 AS TEXT)',
          'databricks': 'SELECT TRY_CAST(123456 AS STRING)',
          'spark': 'SELECT CAST(123456 AS STRING)',
          'spark2': 'SELECT CAST(123456 AS STRING)',
        },
      },
    );
    this.validateAll(
      'SELECT TRY_CAST(\'a\' AS INT)',
      {
        write: {
          '': 'SELECT TRY_CAST(\'a\' AS INT)',
          'databricks': 'SELECT TRY_CAST(\'a\' AS INT)',
          'spark': 'SELECT TRY_CAST(\'a\' AS INT)',
          'spark2': 'SELECT CAST(\'a\' AS INT)',
        },
      },
    );
    this.validateAll(
      'SELECT piv.Q1 FROM (SELECT * FROM produce PIVOT(SUM(sales) FOR quarter IN (\'Q1\', \'Q2\'))) AS piv',
      {
        read: {
          snowflake: 'SELECT piv.Q1 FROM produce PIVOT(SUM(sales) FOR quarter IN (\'Q1\', \'Q2\')) piv',
        },
      },
    );
    this.validateAll(
      'SELECT piv.Q1 FROM (SELECT * FROM (SELECT * FROM produce) PIVOT(SUM(sales) FOR quarter IN (\'Q1\', \'Q2\'))) AS piv',
      {
        read: {
          snowflake: 'SELECT piv.Q1 FROM (SELECT * FROM produce) PIVOT(SUM(sales) FOR quarter IN (\'Q1\', \'Q2\')) piv',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM produce PIVOT(SUM(produce.sales) FOR quarter IN (\'Q1\', \'Q2\'))',
      {
        read: {
          snowflake: 'SELECT * FROM produce PIVOT (SUM(produce.sales) FOR produce.quarter IN (\'Q1\', \'Q2\'))',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM produce AS p PIVOT(SUM(p.sales) AS sales FOR quarter IN (\'Q1\' AS Q1, \'Q2\' AS Q1))',
      {
        read: {
          bigquery: 'SELECT * FROM produce AS p PIVOT(SUM(p.sales) AS sales FOR p.quarter IN (\'Q1\' AS Q1, \'Q2\' AS Q1))',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(MONTH, \'2020-01-01\', \'2020-03-05\')',
      {
        write: {
          databricks: 'SELECT DATEDIFF(MONTH, \'2020-01-01\', \'2020-03-05\')',
          hive: 'SELECT CAST(MONTHS_BETWEEN(\'2020-03-05\', \'2020-01-01\') AS INT)',
          presto: 'SELECT DATE_DIFF(\'MONTH\', CAST(CAST(\'2020-01-01\' AS TIMESTAMP) AS DATE), CAST(CAST(\'2020-03-05\' AS TIMESTAMP) AS DATE))',
          spark: 'SELECT DATEDIFF(MONTH, \'2020-01-01\', \'2020-03-05\')',
          spark2: 'SELECT CAST(MONTHS_BETWEEN(\'2020-03-05\', \'2020-01-01\') AS INT)',
          trino: 'SELECT DATE_DIFF(\'MONTH\', CAST(CAST(\'2020-01-01\' AS TIMESTAMP) AS DATE), CAST(CAST(\'2020-03-05\' AS TIMESTAMP) AS DATE))',
        },
      },
    );

    this.validateAll(
      'SELECT * FROM quarterly_sales PIVOT(SUM(amount) AS amount, \'dummy\' AS bar FOR quarter IN (\'2023_Q1\'))',
      {
        read: {
          spark: 'SELECT * FROM quarterly_sales PIVOT(SUM(amount) amount, \'dummy\' bar FOR quarter IN (\'2023_Q1\'))',
          databricks: 'SELECT * FROM quarterly_sales PIVOT(SUM(amount) amount, \'dummy\' bar FOR quarter IN (\'2023_Q1\'))',
        },
        write: {
          databricks: 'SELECT * FROM quarterly_sales PIVOT(SUM(amount) AS amount, \'dummy\' AS bar FOR quarter IN (\'2023_Q1\'))',
        },
      },
    );

    for (const dataType of [
      'BOOLEAN',
      'DATE',
      'DOUBLE',
      'FLOAT',
      'INT',
      'TIMESTAMP',
    ]) {
      this.validateAll(
        `${dataType}(x)`,
        {
          write: {
            '': `CAST(x AS ${dataType})`,
            'spark': `CAST(x AS ${dataType})`,
          },
        },
      );
    }

    for (const tsSuffix of ['NTZ', 'LTZ']) {
      this.validateAll(
        `TIMESTAMP_${tsSuffix}(x)`,
        {
          write: {
            '': `CAST(x AS TIMESTAMP${tsSuffix})`,
            'spark': `CAST(x AS TIMESTAMP_${tsSuffix})`,
          },
        },
      );
    }

    this.validateAll(
      'STRING(x)',
      {
        write: {
          '': 'CAST(x AS TEXT)',
          'spark': 'CAST(x AS STRING)',
        },
      },
    );

    this.validateAll(
      'CAST(x AS TIMESTAMP)',
      {
        read: {
          trino: 'CAST(x AS TIMESTAMP(6) WITH TIME ZONE)',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_ADD(my_date_column, 1)',
      {
        write: {
          spark: 'SELECT DATE_ADD(my_date_column, 1)',
          spark2: 'SELECT DATE_ADD(my_date_column, 1)',
          bigquery: 'SELECT DATE_ADD(CAST(CAST(my_date_column AS DATETIME) AS DATE), INTERVAL 1 DAY)',
        },
      },
    );
    this.validateAll(
      'AGGREGATE(my_arr, 0, (acc, x) -> acc + x, s -> s * 2)',
      {
        write: {
          trino: 'REDUCE(my_arr, 0, (acc, x) -> acc + x, s -> s * 2)',
          duckdb: 'REDUCE(my_arr, 0, (acc, x) -> acc + x, s -> s * 2)',
          hive: 'REDUCE(my_arr, 0, (acc, x) -> acc + x, s -> s * 2)',
          presto: 'REDUCE(my_arr, 0, (acc, x) -> acc + x, s -> s * 2)',
          spark: 'AGGREGATE(my_arr, 0, (acc, x) -> acc + x, s -> s * 2)',
        },
      },
    );
    this.validateAll('TRIM(\'SL\', \'SSparkSQLS\')', { write: { spark: 'TRIM(\'SL\' FROM \'SSparkSQLS\')' } });
    this.validateAll(
      'ARRAY_SORT(x, (left, right) -> -1)',
      {
        write: {
          duckdb: 'ARRAY_SORT(x)',
          presto: 'ARRAY_SORT(x, ("left", "right") -> -1)',
          hive: 'SORT_ARRAY(x)',
          spark: 'ARRAY_SORT(x, (left, right) -> -1)',
        },
      },
    );
    this.validateAll(
      'ARRAY(0, 1, 2)',
      {
        write: {
          bigquery: '[0, 1, 2]',
          duckdb: '[0, 1, 2]',
          presto: 'ARRAY[0, 1, 2]',
          hive: 'ARRAY(0, 1, 2)',
          spark: 'ARRAY(0, 1, 2)',
        },
      },
    );

    this.validateAll(
      'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname',
      {
        write: {
          clickhouse: 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname NULLS FIRST',
          duckdb: 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname NULLS FIRST',
          postgres: 'SELECT fname, lname, age FROM person ORDER BY age DESC, fname ASC, lname NULLS FIRST',
          presto: 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname NULLS FIRST',
          hive: 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname',
          spark: 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname',
          snowflake: 'SELECT fname, lname, age FROM person ORDER BY age DESC, fname ASC, lname NULLS FIRST',
        },
      },
    );
    this.validateAll(
      'SELECT APPROX_COUNT_DISTINCT(a) FROM foo',
      {
        write: {
          duckdb: 'SELECT APPROX_COUNT_DISTINCT(a) FROM foo',
          presto: 'SELECT APPROX_DISTINCT(a) FROM foo',
          hive: 'SELECT APPROX_COUNT_DISTINCT(a) FROM foo',
          spark: 'SELECT APPROX_COUNT_DISTINCT(a) FROM foo',
        },
      },
    );
    this.validateAll(
      'MONTH(\'2021-03-01\')',
      {
        write: {
          duckdb: 'MONTH(CAST(\'2021-03-01\' AS DATE))',
          presto: 'MONTH(CAST(CAST(\'2021-03-01\' AS TIMESTAMP) AS DATE))',
          hive: 'MONTH(\'2021-03-01\')',
          spark: 'MONTH(\'2021-03-01\')',
        },
      },
    );
    this.validateAll(
      'YEAR(\'2021-03-01\')',
      {
        write: {
          duckdb: 'YEAR(CAST(\'2021-03-01\' AS DATE))',
          presto: 'YEAR(CAST(CAST(\'2021-03-01\' AS TIMESTAMP) AS DATE))',
          hive: 'YEAR(\'2021-03-01\')',
          spark: 'YEAR(\'2021-03-01\')',
        },
      },
    );
    this.validateAll(
      '\'\u6bdb\'',
      {
        write: {
          duckdb: '\'毛\'',
          presto: '\'毛\'',
          hive: '\'毛\'',
          spark: '\'毛\'',
        },
      },
    );
    this.validateAll(
      'SELECT LEFT(x, 2), RIGHT(x, 2)',
      {
        write: {
          duckdb: 'SELECT LEFT(x, 2), RIGHT(x, 2)',
          presto: 'SELECT SUBSTRING(x, 1, 2), SUBSTRING(x, LENGTH(x) - (2 - 1))',
          hive: 'SELECT SUBSTRING(x, 1, 2), SUBSTRING(x, LENGTH(x) - (2 - 1))',
          spark: 'SELECT LEFT(x, 2), RIGHT(x, 2)',
        },
      },
    );
    this.validateIdentity('SELECT SUBSTR(\'Spark\' FROM 5 FOR 1)', 'SELECT SUBSTRING(\'Spark\', 5, 1)');
    this.validateIdentity('SELECT SUBSTR(\'Spark SQL\', 5)', 'SELECT SUBSTRING(\'Spark SQL\', 5)');
    this.validateIdentity(
      'SELECT SUBSTR(ENCODE(\'Spark SQL\', \'utf-8\'), 5)',
      'SELECT SUBSTRING(ENCODE(\'Spark SQL\', \'utf-8\'), 5)',
    );
    this.validateAll(
      'MAP_FROM_ARRAYS(ARRAY(1), c)',
      {
        write: {
          duckdb: 'MAP([1], c)',
          presto: 'MAP(ARRAY[1], c)',
          hive: 'MAP(ARRAY(1), c)',
          spark: 'MAP_FROM_ARRAYS(ARRAY(1), c)',
          snowflake: 'OBJECT_CONSTRUCT([1], c)',
        },
      },
    );
    this.validateAll(
      'SELECT ARRAY_SORT(x)',
      {
        write: {
          duckdb: 'SELECT ARRAY_SORT(x)',
          presto: 'SELECT ARRAY_SORT(x)',
          hive: 'SELECT SORT_ARRAY(x)',
          spark: 'SELECT ARRAY_SORT(x)',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_ADD(MONTH, 20, col)',
      {
        read: {
          spark: 'SELECT TIMESTAMPADD(MONTH, 20, col)',
        },
        write: {
          spark: 'SELECT DATE_ADD(MONTH, 20, col)',
          databricks: 'SELECT DATE_ADD(MONTH, 20, col)',
          presto: 'SELECT DATE_ADD(\'MONTH\', 20, col)',
          trino: 'SELECT DATE_ADD(\'MONTH\', 20, col)',
        },
      },
    );
    this.validateIdentity('DESCRIBE schema.test PARTITION(ds = \'2024-01-01\')');

    this.validateAll(
      'SELECT ANY_VALUE(col, true), FIRST(col, true), FIRST_VALUE(col, true) OVER ()',
      {
        write: {
          duckdb: 'SELECT ANY_VALUE(col), ANY_VALUE(col), FIRST_VALUE(col IGNORE NULLS) OVER ()',
        },
      },
    );

    this.validateAll(
      'SELECT STRUCT(1, 2)',
      {
        write: {
          spark: 'SELECT STRUCT(1 AS col1, 2 AS col2)',
          presto: 'SELECT CAST(ROW(1, 2) AS ROW(col1 INTEGER, col2 INTEGER))',
          duckdb: 'SELECT {\'col1\': 1, \'col2\': 2}',
        },
      },
    );
    this.validateAll(
      'SELECT STRUCT(x, 1, y AS col3, STRUCT(5)) FROM t',
      {
        write: {
          spark: 'SELECT STRUCT(x AS x, 1 AS col2, y AS col3, STRUCT(5 AS col1) AS col4) FROM t',
          duckdb: 'SELECT {\'x\': x, \'col2\': 1, \'col3\': y, \'col4\': {\'col1\': 5}} FROM t',
        },
      },
    );

    this.validateAll(
      'SELECT TIMESTAMPDIFF(MONTH, foo, bar)',
      {
        read: {
          databricks: 'SELECT TIMESTAMPDIFF(MONTH, foo, bar)',
        },
        write: {
          spark: 'SELECT TIMESTAMPDIFF(MONTH, foo, bar)',
          databricks: 'SELECT TIMESTAMPDIFF(MONTH, foo, bar)',
        },
      },
    );

    this.validateAll(
      'SELECT CAST(col AS TIMESTAMP)',
      {
        write: {
          spark2: 'SELECT CAST(col AS TIMESTAMP)',
          spark: 'SELECT CAST(col AS TIMESTAMP)',
          databricks: 'SELECT TRY_CAST(col AS TIMESTAMP)',
          duckdb: 'SELECT TRY_CAST(col AS TIMESTAMPTZ)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM {df}',
      {
        read: {
          databricks: 'SELECT * FROM {df}',
        },
        write: {
          spark: 'SELECT * FROM {df}',
          databricks: 'SELECT * FROM {df}',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM {df} WHERE id > :foo',
      {
        read: {
          databricks: 'SELECT * FROM {df} WHERE id > :foo',
        },
        write: {
          spark: 'SELECT * FROM {df} WHERE id > :foo',
          databricks: 'SELECT * FROM {df} WHERE id > :foo',
        },
      },
    );
    this.validateAll(
      'STRING_AGG(x, \', \')',
      {
        write: {
          'spark, version=3.0.0': 'ARRAY_JOIN(COLLECT_LIST(x), \', \')',
          'spark, version=4.0.0': 'LISTAGG(x, \', \')',
          'spark': 'LISTAGG(x, \', \')',
        },
      },
    );
    this.validateAll(
      'LISTAGG(x, \', \')',
      {
        write: {
          'spark, version=3.0.0': 'ARRAY_JOIN(COLLECT_LIST(x), \', \')',
          'spark, version=4.0.0': 'LISTAGG(x, \', \')',
          'spark': 'LISTAGG(x, \', \')',
        },
      },
    );
    this.validateAll(
      'LIKE(foo, \'pattern\')',
      {
        write: {
          spark: 'foo LIKE \'pattern\'',
          databricks: 'foo LIKE \'pattern\'',
        },
      },
    );
    this.validateAll(
      'LIKE(foo, \'pattern\', \'!\')',
      {
        write: {
          spark: 'foo LIKE \'pattern\' ESCAPE \'!\'',
          databricks: 'foo LIKE \'pattern\' ESCAPE \'!\'',
        },
      },
    );
    this.validateAll(
      'ILIKE(foo, \'pattern\')',
      {
        write: {
          spark: 'foo ILIKE \'pattern\'',
          databricks: 'foo ILIKE \'pattern\'',
        },
      },
    );
    this.validateAll(
      'ILIKE(foo, \'pattern\', \'!\')',
      {
        write: {
          spark: 'foo ILIKE \'pattern\' ESCAPE \'!\'',
          databricks: 'foo ILIKE \'pattern\' ESCAPE \'!\'',
        },
      },
    );
    this.validateIdentity('BITMAP_OR_AGG(x)');
    this.validateIdentity('SELECT ELT(2, \'foo\', \'bar\', \'baz\') AS Result');
    this.validateIdentity('SELECT MAKE_INTERVAL(100, 11, 12, 13, 14, 14, 15)');
    this.validateIdentity('SELECT name, GROUPING_ID() FROM customer GROUP BY ROLLUP (name)');
  }

  testBoolOr () {
    this.validateAll(
      'SELECT a, LOGICAL_OR(b) FROM table GROUP BY a',
      { write: { spark: 'SELECT a, BOOL_OR(b) FROM table GROUP BY a' } },
    );
  }

  testCurrentUser () {
    this.validateAll(
      'CURRENT_USER',
      { write: { spark: 'CURRENT_USER()' } },
    );
    this.validateAll(
      'CURRENT_USER()',
      { write: { spark: 'CURRENT_USER()' } },
    );
  }

  testTransformQuery () {
    this.validateIdentity('SELECT TRANSFORM(x) USING \'x\' AS (x INT) FROM t');
    this.validateIdentity(
      'SELECT TRANSFORM(zip_code, name, age) USING \'cat\' AS (a, b, c) FROM person WHERE zip_code > 94511',
    );
    this.validateIdentity(
      'SELECT TRANSFORM(zip_code, name, age) USING \'cat\' AS (a STRING, b STRING, c STRING) FROM person WHERE zip_code > 94511',
    );
    this.validateIdentity(
      'SELECT TRANSFORM(name, age) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' LINES TERMINATED BY \'\\n\' NULL DEFINED AS \'NULL\' USING \'cat\' AS (name_age STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY \'@\' LINES TERMINATED BY \'\\n\' NULL DEFINED AS \'NULL\' FROM person',
    );
    this.validateIdentity(
      'SELECT TRANSFORM(zip_code, name, age) ROW FORMAT SERDE \'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\' WITH SERDEPROPERTIES (\'field.delim\'=\'\\t\') USING \'cat\' AS (a STRING, b STRING, c STRING) ROW FORMAT SERDE \'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\' WITH SERDEPROPERTIES (\'field.delim\'=\'\\t\') FROM person WHERE zip_code > 94511',
    );
    this.validateIdentity(
      'SELECT TRANSFORM(zip_code, name, age) USING \'cat\' FROM person WHERE zip_code > 94500',
    );
  }

  testInsertCte () {
    this.validateAll(
      'INSERT OVERWRITE TABLE table WITH cte AS (SELECT cola FROM other_table) SELECT cola FROM cte',
      {
        write: {
          databricks: 'WITH cte AS (SELECT cola FROM other_table) INSERT OVERWRITE TABLE table SELECT cola FROM cte',
          hive: 'WITH cte AS (SELECT cola FROM other_table) INSERT OVERWRITE TABLE table SELECT cola FROM cte',
          spark: 'WITH cte AS (SELECT cola FROM other_table) INSERT OVERWRITE TABLE table SELECT cola FROM cte',
          spark2: 'WITH cte AS (SELECT cola FROM other_table) INSERT OVERWRITE TABLE table SELECT cola FROM cte',
        },
      },
    );
  }

  testExplodeProjectionToUnnest () {
    this.validateAll(
      'SELECT EXPLODE(x) FROM tbl',
      {
        write: {
          bigquery: 'SELECT IF(pos = pos_2, col, NULL) AS col FROM tbl CROSS JOIN UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH(x)) - 1)) AS pos CROSS JOIN UNNEST(x) AS col WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH(x) - 1) AND pos_2 = (ARRAY_LENGTH(x) - 1))',
          presto: 'SELECT IF(_u.pos = _u_2.pos_2, _u_2.col) AS col FROM tbl CROSS JOIN UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(x)))) AS _u(pos) CROSS JOIN UNNEST(x) WITH ORDINALITY AS _u_2(col, pos_2) WHERE _u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(x) AND _u_2.pos_2 = CARDINALITY(x))',
          spark: 'SELECT EXPLODE(x) FROM tbl',
        },
      },
    );
    this.validateAll(
      'SELECT EXPLODE(col) FROM _u',
      {
        write: {
          bigquery: 'SELECT IF(pos = pos_2, col_2, NULL) AS col_2 FROM _u CROSS JOIN UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH(col)) - 1)) AS pos CROSS JOIN UNNEST(col) AS col_2 WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH(col) - 1) AND pos_2 = (ARRAY_LENGTH(col) - 1))',
          presto: 'SELECT IF(_u_2.pos = _u_3.pos_2, _u_3.col_2) AS col_2 FROM _u CROSS JOIN UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(col)))) AS _u_2(pos) CROSS JOIN UNNEST(col) WITH ORDINALITY AS _u_3(col_2, pos_2) WHERE _u_2.pos = _u_3.pos_2 OR (_u_2.pos > CARDINALITY(col) AND _u_3.pos_2 = CARDINALITY(col))',
          spark: 'SELECT EXPLODE(col) FROM _u',
        },
      },
    );
    this.validateAll(
      'SELECT EXPLODE(col) AS exploded FROM schema.tbl',
      {
        write: {
          presto: 'SELECT IF(_u.pos = _u_2.pos_2, _u_2.exploded) AS exploded FROM schema.tbl CROSS JOIN UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(col)))) AS _u(pos) CROSS JOIN UNNEST(col) WITH ORDINALITY AS _u_2(exploded, pos_2) WHERE _u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(col) AND _u_2.pos_2 = CARDINALITY(col))',
        },
      },
    );
    this.validateAll(
      'SELECT EXPLODE(ARRAY(1, 2))',
      {
        write: {
          bigquery: 'SELECT IF(pos = pos_2, col, NULL) AS col FROM UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH([1, 2])) - 1)) AS pos CROSS JOIN UNNEST([1, 2]) AS col WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH([1, 2]) - 1) AND pos_2 = (ARRAY_LENGTH([1, 2]) - 1))',
          presto: 'SELECT IF(_u.pos = _u_2.pos_2, _u_2.col) AS col FROM UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(ARRAY[1, 2])))) AS _u(pos) CROSS JOIN UNNEST(ARRAY[1, 2]) WITH ORDINALITY AS _u_2(col, pos_2) WHERE _u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(ARRAY[1, 2]) AND _u_2.pos_2 = CARDINALITY(ARRAY[1, 2]))',
        },
      },
    );
    this.validateAll(
      'SELECT POSEXPLODE(ARRAY(2, 3)) AS x',
      {
        write: {
          bigquery: 'SELECT IF(pos = pos_2, x, NULL) AS x, IF(pos = pos_2, pos_2, NULL) AS pos_2 FROM UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH([2, 3])) - 1)) AS pos CROSS JOIN UNNEST([2, 3]) AS x WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH([2, 3]) - 1) AND pos_2 = (ARRAY_LENGTH([2, 3]) - 1))',
          presto: 'SELECT IF(_u.pos = _u_2.pos_2, _u_2.x) AS x, IF(_u.pos = _u_2.pos_2, _u_2.pos_2) AS pos_2 FROM UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(ARRAY[2, 3])))) AS _u(pos) CROSS JOIN UNNEST(ARRAY[2, 3]) WITH ORDINALITY AS _u_2(x, pos_2) WHERE _u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(ARRAY[2, 3]) AND _u_2.pos_2 = CARDINALITY(ARRAY[2, 3]))',
        },
      },
    );
    this.validateAll(
      'SELECT POSEXPLODE(ARRAY(\'a\'))',
      {
        write: {
          duckdb: 'SELECT GENERATE_SUBSCRIPTS([\'a\'], 1) - 1 AS pos, UNNEST([\'a\']) AS col',
          spark: 'SELECT POSEXPLODE(ARRAY(\'a\'))',
        },
      },
    );
    this.validateAll(
      'SELECT POSEXPLODE(x) AS (a, b)',
      {
        write: {
          presto: 'SELECT IF(_u.pos = _u_2.a, _u_2.b) AS b, IF(_u.pos = _u_2.a, _u_2.a) AS a FROM UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(x)))) AS _u(pos) CROSS JOIN UNNEST(x) WITH ORDINALITY AS _u_2(b, a) WHERE _u.pos = _u_2.a OR (_u.pos > CARDINALITY(x) AND _u_2.a = CARDINALITY(x))',
          duckdb: 'SELECT GENERATE_SUBSCRIPTS(x, 1) - 1 AS a, UNNEST(x) AS b',
          spark: 'SELECT POSEXPLODE(x) AS (a, b)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM POSEXPLODE(ARRAY(\'a\'))',
      {
        write: {
          duckdb: 'SELECT * FROM (SELECT GENERATE_SUBSCRIPTS([\'a\'], 1) - 1 AS pos, UNNEST([\'a\']) AS col)',
          spark: 'SELECT * FROM POSEXPLODE(ARRAY(\'a\'))',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM POSEXPLODE(ARRAY(\'a\')) AS (a, b)',
      {
        write: {
          duckdb: 'SELECT * FROM (SELECT GENERATE_SUBSCRIPTS([\'a\'], 1) - 1 AS a, UNNEST([\'a\']) AS b)',
          spark: 'SELECT * FROM POSEXPLODE(ARRAY(\'a\')) AS _t0(a, b)',
        },
      },
    );
    this.validateAll(
      'SELECT POSEXPLODE(ARRAY(2, 3)), EXPLODE(ARRAY(4, 5, 6)) FROM tbl',
      {
        write: {
          bigquery: 'SELECT IF(pos = pos_2, col, NULL) AS col, IF(pos = pos_2, pos_2, NULL) AS pos_2, IF(pos = pos_3, col_2, NULL) AS col_2 FROM tbl CROSS JOIN UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH([2, 3]), ARRAY_LENGTH([4, 5, 6])) - 1)) AS pos CROSS JOIN UNNEST([2, 3]) AS col WITH OFFSET AS pos_2 CROSS JOIN UNNEST([4, 5, 6]) AS col_2 WITH OFFSET AS pos_3 WHERE (pos = pos_2 OR (pos > (ARRAY_LENGTH([2, 3]) - 1) AND pos_2 = (ARRAY_LENGTH([2, 3]) - 1))) AND (pos = pos_3 OR (pos > (ARRAY_LENGTH([4, 5, 6]) - 1) AND pos_3 = (ARRAY_LENGTH([4, 5, 6]) - 1)))',
          presto: 'SELECT IF(_u.pos = _u_2.pos_2, _u_2.col) AS col, IF(_u.pos = _u_2.pos_2, _u_2.pos_2) AS pos_2, IF(_u.pos = _u_3.pos_3, _u_3.col_2) AS col_2 FROM tbl CROSS JOIN UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(ARRAY[2, 3]), CARDINALITY(ARRAY[4, 5, 6])))) AS _u(pos) CROSS JOIN UNNEST(ARRAY[2, 3]) WITH ORDINALITY AS _u_2(col, pos_2) CROSS JOIN UNNEST(ARRAY[4, 5, 6]) WITH ORDINALITY AS _u_3(col_2, pos_3) WHERE (_u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(ARRAY[2, 3]) AND _u_2.pos_2 = CARDINALITY(ARRAY[2, 3]))) AND (_u.pos = _u_3.pos_3 OR (_u.pos > CARDINALITY(ARRAY[4, 5, 6]) AND _u_3.pos_3 = CARDINALITY(ARRAY[4, 5, 6])))',
        },
      },
    );
    this.validateAll(
      'SELECT col, pos, POSEXPLODE(ARRAY(2, 3)) FROM _u',
      {
        write: {
          presto: 'SELECT col, pos, IF(_u_2.pos_2 = _u_3.pos_3, _u_3.col_2) AS col_2, IF(_u_2.pos_2 = _u_3.pos_3, _u_3.pos_3) AS pos_3 FROM _u CROSS JOIN UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(ARRAY[2, 3])))) AS _u_2(pos_2) CROSS JOIN UNNEST(ARRAY[2, 3]) WITH ORDINALITY AS _u_3(col_2, pos_3) WHERE _u_2.pos_2 = _u_3.pos_3 OR (_u_2.pos_2 > CARDINALITY(ARRAY[2, 3]) AND _u_3.pos_3 = CARDINALITY(ARRAY[2, 3]))',
        },
      },
    );
  }

  testStripModifiers () {
    const withoutModifiers = 'SELECT * FROM t';
    const withModifiers = `${withoutModifiers} CLUSTER BY y DISTRIBUTE BY x SORT BY z`;
    const query = this.parseOne(withModifiers);

    const dialectsWithModifiers = new Set([
      '',
      'databricks',
      'hive',
      'spark',
      'spark2',
    ]);
    const dialectNames = [
      '',
      'databricks',
      'hive',
      'spark',
      'spark2',
      'bigquery',
      'duckdb',
      'postgres',
      'presto',
      'snowflake',
      'trino',
      'tsql',
    ];
    for (const name of dialectNames) {
      if (dialectsWithModifiers.has(name)) {
        expect(query.sql({ dialect: name || undefined })).toBe(withModifiers);
      } else {
        expect(query.sql({ dialect: name || undefined })).toBe(withoutModifiers);
      }
    }
  }

  testSchemaBindingOptions () {
    for (const schemaBinding of [
      'BINDING',
      'COMPENSATION',
      'TYPE EVOLUTION',
      'EVOLUTION',
    ]) {
      this.validateIdentity(
        `CREATE VIEW emp_v WITH SCHEMA ${schemaBinding} AS SELECT * FROM emp`,
      );
    }
  }

  testMinus () {
    this.validateAll(
      'SELECT * FROM db.table1 MINUS SELECT * FROM db.table2',
      {
        write: {
          spark: 'SELECT * FROM db.table1 EXCEPT SELECT * FROM db.table2',
          databricks: 'SELECT * FROM db.table1 EXCEPT SELECT * FROM db.table2',
        },
      },
    );
  }

  testString () {
    for (const dialect of [
      'hive',
      'spark2',
      'spark',
      'databricks',
    ]) {
      const query = parseOne('STRING(a)', { read: dialect });
      expect(query.sql({ dialect })).toBe('CAST(a AS STRING)');
    }
  }

  testBinaryString () {
    for (const dialect of [
      'spark2',
      'spark',
      'databricks',
    ]) {
      const query1 = parseOne('X\'ab\'', { read: dialect });
      expect(query1.sql({ dialect })).toBe('X\'ab\'');

      const query2 = parseOne('X\'\'', { read: dialect });
      expect(query2.sql({ dialect })).toBe('X\'\'');
    }
  }

  testAnalyze () {
    this.validateIdentity('ANALYZE TABLE tbl COMPUTE STATISTICS NOSCAN');
    this.validateIdentity('ANALYZE TABLE tbl COMPUTE STATISTICS FOR ALL COLUMNS');
    this.validateIdentity('ANALYZE TABLE tbl COMPUTE STATISTICS FOR COLUMNS foo, bar');
    this.validateIdentity('ANALYZE TABLE ctlg.db.tbl COMPUTE STATISTICS NOSCAN');
    this.validateIdentity('ANALYZE TABLES COMPUTE STATISTICS NOSCAN');
    this.validateIdentity('ANALYZE TABLES FROM db COMPUTE STATISTICS');
    this.validateIdentity('ANALYZE TABLES IN db COMPUTE STATISTICS');
    this.validateIdentity(
      'ANALYZE TABLE ctlg.db.tbl PARTITION(foo = \'foo\', bar = \'bar\') COMPUTE STATISTICS NOSCAN',
    );
  }

  testApproxPercentile () {
    this.validateAll(
      'PERCENTILE_APPROX(DISTINCT col, 0.3)',
      {
        read: {
          spark: 'APPROX_PERCENTILE(DISTINCT col, 0.3)',
          databricks: 'APPROX_PERCENTILE(DISTINCT col, 0.3)',
        },
      },
    );
    this.validateAll(
      'PERCENTILE_APPROX(DISTINCT col, 0.3, 200)',
      {
        read: {
          spark: 'APPROX_PERCENTILE(DISTINCT col, 0.3, 200)',
          databricks: 'APPROX_PERCENTILE(DISTINCT col, 0.3, 200)',
        },
      },
    );

    const approxQuantileExpr1 = this.validateIdentity('PERCENTILE_APPROX(DISTINCT col, 0.3)');
    const narrowed1 = approxQuantileExpr1.assertIs(ApproxQuantileExpr);
    narrowInstanceOf(narrowed1?.args.this, DistinctExpr);
    expect(narrowed1?.getArgKey('quantile')).toBeInstanceOf(LiteralExpr);

    const approxQuantileExpr2 = this.validateIdentity('PERCENTILE_APPROX(DISTINCT col, 0.3, 200)');
    const narrowed2 = approxQuantileExpr2.assertIs(ApproxQuantileExpr);
    narrowInstanceOf(narrowed2?.args.this, DistinctExpr);
    expect(narrowed2?.getArgKey('quantile')).toBeInstanceOf(LiteralExpr);
  }
}

const t = new TestSpark();
describe('TestSpark', () => {
  test('testDdl', () => t.testDdl());
  test('testToDate', () => t.testToDate());
  test('testHint', () => t.testHint());
  test('testSpark', () => t.testSpark());
  test('testBoolOr', () => t.testBoolOr());
  test('testCurrentUser', () => t.testCurrentUser());
  test('testTransformQuery', () => t.testTransformQuery());
  test('testInsertCte', () => t.testInsertCte());
  test('testExplodeProjectionToUnnest', () => t.testExplodeProjectionToUnnest());
  test('testStripModifiers', () => t.testStripModifiers());
  test('testSchemaBindingOptions', () => t.testSchemaBindingOptions());
  test('testMinus', () => t.testMinus());
  test('testString', () => t.testString());
  test('testBinaryString', () => t.testBinaryString());
  test('testAnalyze', () => t.testAnalyze());
  test('testApproxPercentile', () => t.testApproxPercentile());
});
