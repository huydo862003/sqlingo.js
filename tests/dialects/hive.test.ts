import {
  describe, test,
} from 'vitest';
import {
  Expression,
  ColumnExpr, DistinctExpr, ExistsExpr, LiteralExpr, QuantileExpr, TimestampTruncExpr,
} from '../../src/expressions';
import { narrowInstanceOf } from '../../src/port_internals';
import { Validator } from './validator';

class TestHive extends Validator {
  override dialect = 'hive' as const;

  testBits () {
    this.validateAll(
      'x & 1',
      {
        read: {
          duckdb: 'x & 1',
          presto: 'BITWISE_AND(x, 1)',
          spark: 'x & 1',
        },
        write: {
          duckdb: 'x & 1',
          hive: 'x & 1',
          presto: 'BITWISE_AND(x, 1)',
          spark: 'x & 1',
        },
      },
    );
    this.validateAll(
      'x & 1 > 0',
      {
        read: {
          duckdb: 'x & 1 > 0',
          presto: 'BITWISE_AND(x, 1) > 0',
          spark: 'x & 1 > 0',
        },
        write: {
          duckdb: 'x & 1 > 0',
          presto: 'BITWISE_AND(x, 1) > 0',
          hive: 'x & 1 > 0',
          spark: 'x & 1 > 0',
        },
      },
    );
    this.validateAll(
      '~x',
      {
        read: {
          duckdb: '~x',
          presto: 'BITWISE_NOT(x)',
          spark: '~x',
        },
        write: {
          duckdb: '~x',
          hive: '~x',
          presto: 'BITWISE_NOT(x)',
          spark: '~x',
        },
      },
    );
    this.validateAll(
      'x | 1',
      {
        read: {
          duckdb: 'x | 1',
          presto: 'BITWISE_OR(x, 1)',
          spark: 'x | 1',
        },
        write: {
          duckdb: 'x | 1',
          hive: 'x | 1',
          presto: 'BITWISE_OR(x, 1)',
          spark: 'x | 1',
        },
      },
    );
    this.validateAll(
      'x << 1',
      {
        read: {
          spark: 'SHIFTLEFT(x, 1)',
        },
        write: {
          duckdb: 'x << 1',
          presto: 'BITWISE_ARITHMETIC_SHIFT_LEFT(x, 1)',
          hive: 'x << 1',
          spark: 'SHIFTLEFT(x, 1)',
        },
      },
    );
    this.validateAll(
      'x >> 1',
      {
        read: {
          spark: 'SHIFTRIGHT(x, 1)',
        },
        write: {
          duckdb: 'x >> 1',
          presto: 'BITWISE_ARITHMETIC_SHIFT_RIGHT(x, 1)',
          hive: 'x >> 1',
          spark: 'SHIFTRIGHT(x, 1)',
        },
      },
    );
  }

  testCast () {
    this.validateAll(
      '1s',
      {
        write: {
          duckdb: 'TRY_CAST(1 AS SMALLINT)',
          presto: 'TRY_CAST(1 AS SMALLINT)',
          hive: 'CAST(1 AS SMALLINT)',
          spark: 'CAST(1 AS SMALLINT)',
        },
      },
    );
    this.validateAll(
      '1S',
      {
        write: {
          duckdb: 'TRY_CAST(1 AS SMALLINT)',
          presto: 'TRY_CAST(1 AS SMALLINT)',
          hive: 'CAST(1 AS SMALLINT)',
          spark: 'CAST(1 AS SMALLINT)',
        },
      },
    );
    this.validateAll(
      '1Y',
      {
        write: {
          duckdb: 'TRY_CAST(1 AS TINYINT)',
          presto: 'TRY_CAST(1 AS TINYINT)',
          hive: 'CAST(1 AS TINYINT)',
          spark: 'CAST(1 AS TINYINT)',
        },
      },
    );
    this.validateAll(
      '1L',
      {
        write: {
          duckdb: 'TRY_CAST(1 AS BIGINT)',
          presto: 'TRY_CAST(1 AS BIGINT)',
          hive: 'CAST(1 AS BIGINT)',
          spark: 'CAST(1 AS BIGINT)',
        },
      },
    );
    this.validateAll(
      '1.0bd',
      {
        write: {
          duckdb: 'TRY_CAST(1.0 AS DECIMAL)',
          presto: 'TRY_CAST(1.0 AS DECIMAL)',
          hive: 'CAST(1.0 AS DECIMAL)',
          spark: 'CAST(1.0 AS DECIMAL)',
        },
      },
    );
    this.validateAll(
      'CAST(1 AS INT)',
      {
        read: {
          presto: 'TRY_CAST(1 AS INT)',
        },
        write: {
          duckdb: 'TRY_CAST(1 AS INT)',
          presto: 'TRY_CAST(1 AS INTEGER)',
          hive: 'CAST(1 AS INT)',
          spark: 'CAST(1 AS INT)',
        },
      },
    );
  }

  testDdl () {
    this.validateAll(
      'CREATE TABLE x (w STRING) PARTITIONED BY (y INT, z INT)',
      {
        write: {
          duckdb: 'CREATE TABLE x (w TEXT)',
          presto: 'CREATE TABLE x (w VARCHAR, y INTEGER, z INTEGER) WITH (PARTITIONED_BY=ARRAY[\'y\', \'z\'])',
          hive: 'CREATE TABLE x (w STRING) PARTITIONED BY (y INT, z INT)',
          spark: 'CREATE TABLE x (w STRING, y INT, z INT) PARTITIONED BY (y, z)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE test STORED AS parquet TBLPROPERTIES (\'x\'=\'1\', \'Z\'=\'2\') AS SELECT 1',
      {
        write: {
          duckdb: 'CREATE TABLE test AS SELECT 1',
          presto: 'CREATE TABLE test WITH (format=\'parquet\', x=\'1\', Z=\'2\') AS SELECT 1',
          hive: 'CREATE TABLE test STORED AS PARQUET TBLPROPERTIES (\'x\'=\'1\', \'Z\'=\'2\') AS SELECT 1',
          spark: 'CREATE TABLE test STORED AS PARQUET TBLPROPERTIES (\'x\'=\'1\', \'Z\'=\'2\') AS SELECT 1',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE test STORED AS INPUTFORMAT \'foo1\' OUTPUTFORMAT \'foo2\'',
      {
        write: {
          hive: 'CREATE TABLE test STORED AS INPUTFORMAT \'foo1\' OUTPUTFORMAT \'foo2\'',
          spark: 'CREATE TABLE test STORED AS INPUTFORMAT \'foo1\' OUTPUTFORMAT \'foo2\'',
          databricks: 'CREATE TABLE test STORED AS INPUTFORMAT \'foo1\' OUTPUTFORMAT \'foo2\'',
        },
      },
    );

    this.validateIdentity('ALTER TABLE x PARTITION(y = z) ADD COLUMN a VARCHAR(10)');
    this.validateIdentity(
      'ALTER TABLE x CHANGE a a VARCHAR(10)',
      'ALTER TABLE x CHANGE COLUMN a a VARCHAR(10)',
    );

    this.validateAll(
      'ALTER TABLE x CHANGE COLUMN a a VARCHAR(10)',
      {
        write: {
          hive: 'ALTER TABLE x CHANGE COLUMN a a VARCHAR(10)',
          spark: 'ALTER TABLE x ALTER COLUMN a TYPE VARCHAR(10)',
        },
      },
    );
    this.validateAll(
      'ALTER TABLE x CHANGE COLUMN a a VARCHAR(10) COMMENT \'comment\'',
      {
        write: {
          hive: 'ALTER TABLE x CHANGE COLUMN a a VARCHAR(10) COMMENT \'comment\'',
          spark: 'ALTER TABLE x ALTER COLUMN a COMMENT \'comment\'',
        },
      },
    );
    this.validateAll(
      'ALTER TABLE x CHANGE COLUMN a b VARCHAR(10)',
      {
        write: {
          hive: 'ALTER TABLE x CHANGE COLUMN a b VARCHAR(10)',
          spark: 'ALTER TABLE x RENAME COLUMN a TO b',
        },
      },
    );
    this.validateAll(
      'ALTER TABLE x CHANGE COLUMN a a VARCHAR(10) CASCADE',
      {
        write: {
          hive: 'ALTER TABLE x CHANGE COLUMN a a VARCHAR(10) CASCADE',
          spark: 'ALTER TABLE x ALTER COLUMN a TYPE VARCHAR(10)',
        },
      },
    );

    this.validateIdentity('ALTER TABLE X ADD COLUMNS (y INT, z STRING)');
    this.validateIdentity('ALTER TABLE X ADD COLUMNS (y INT, z STRING) CASCADE');

    this.validateIdentity(
      'CREATE EXTERNAL TABLE x (y INT) ROW FORMAT SERDE \'serde\' ROW FORMAT DELIMITED FIELDS TERMINATED BY \'1\' WITH SERDEPROPERTIES (\'input.regex\'=\'\')',
    );
    this.validateIdentity(
      'CREATE EXTERNAL TABLE `my_table` (`a7` ARRAY<DATE>) ROW FORMAT SERDE \'a\' STORED AS INPUTFORMAT \'b\' OUTPUTFORMAT \'c\' LOCATION \'d\' TBLPROPERTIES (\'e\'=\'f\')',
    );
    this.validateIdentity('CREATE EXTERNAL TABLE X (y INT) STORED BY \'x\'');
    this.validateIdentity('ALTER VIEW v1 AS SELECT x, UPPER(s) AS s FROM t2');
    this.validateIdentity('ALTER VIEW v1 (c1, c2) AS SELECT x, UPPER(s) AS s FROM t2');
    this.validateIdentity(
      'ALTER VIEW v7 (c1 COMMENT \'Comment for c1\', c2) AS SELECT t1.c1, t1.c2 FROM t1',
    );
    this.validateIdentity('ALTER VIEW db1.v1 RENAME TO db2.v2');
    this.validateIdentity('ALTER VIEW v1 SET TBLPROPERTIES (\'tblp1\'=\'1\', \'tblp2\'=\'2\')');
    this.validateIdentity(
      'ALTER VIEW v1 UNSET TBLPROPERTIES (\'tblp1\', \'tblp2\')',
      undefined,
      { checkCommandWarning: true },
    );
    this.validateIdentity('CREATE TABLE foo (col STRUCT<struct_col_a: VARCHAR((50))>)');

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
      'ALTER TABLE db.example_table ADD PARTITION(col_a = \'a\') LOCATION \'b\'',
      {
        read: {
          spark2: 'ALTER TABLE db.example_table ADD PARTITION(col_a = \'a\') LOCATION \'b\'',
          spark: 'ALTER TABLE db.example_table ADD PARTITION(col_a = \'a\') LOCATION \'b\'',
          databricks: 'ALTER TABLE db.example_table ADD PARTITION(col_a = \'a\') LOCATION \'b\'',
        },
        write: {
          hive: 'ALTER TABLE db.example_table ADD PARTITION(col_a = \'a\') LOCATION \'b\'',
          spark2: 'ALTER TABLE db.example_table ADD PARTITION(col_a = \'a\') LOCATION \'b\'',
          spark: 'ALTER TABLE db.example_table ADD PARTITION(col_a = \'a\') LOCATION \'b\'',
          databricks: 'ALTER TABLE db.example_table ADD PARTITION(col_a = \'a\') LOCATION \'b\'',
        },
      },
    );
  }

  testLateralView () {
    this.validateAll(
      'SELECT a, b FROM x LATERAL VIEW EXPLODE(y) t AS a LATERAL VIEW EXPLODE(z) u AS b',
      {
        write: {
          presto: 'SELECT a, b FROM x CROSS JOIN UNNEST(y) AS t(a) CROSS JOIN UNNEST(z) AS u(b)',
          duckdb: 'SELECT a, b FROM x CROSS JOIN UNNEST(y) AS t(a) CROSS JOIN UNNEST(z) AS u(b)',
          hive: 'SELECT a, b FROM x LATERAL VIEW EXPLODE(y) t AS a LATERAL VIEW EXPLODE(z) u AS b',
          spark: 'SELECT a, b FROM x LATERAL VIEW EXPLODE(y) t AS a LATERAL VIEW EXPLODE(z) u AS b',
        },
      },
    );
    this.validateAll(
      'SELECT a FROM x LATERAL VIEW EXPLODE(y) t AS a',
      {
        write: {
          presto: 'SELECT a FROM x CROSS JOIN UNNEST(y) AS t(a)',
          duckdb: 'SELECT a FROM x CROSS JOIN UNNEST(y) AS t(a)',
          hive: 'SELECT a FROM x LATERAL VIEW EXPLODE(y) t AS a',
          spark: 'SELECT a FROM x LATERAL VIEW EXPLODE(y) t AS a',
        },
      },
    );
    this.validateAll(
      'SELECT a FROM x LATERAL VIEW POSEXPLODE(y) t AS pos, col',
      {
        write: {
          presto: 'SELECT a FROM x CROSS JOIN LATERAL (SELECT pos - 1 AS pos, col FROM UNNEST(y) WITH ORDINALITY AS t(col, pos))',
          trino: 'SELECT a FROM x CROSS JOIN LATERAL (SELECT pos - 1 AS pos, col FROM UNNEST(y) WITH ORDINALITY AS t(col, pos))',
          duckdb: 'SELECT a FROM x CROSS JOIN LATERAL (SELECT pos - 1 AS pos, col FROM UNNEST(y) WITH ORDINALITY AS t(col, pos))',
          hive: 'SELECT a FROM x LATERAL VIEW POSEXPLODE(y) t AS pos, col',
          spark: 'SELECT a FROM x LATERAL VIEW POSEXPLODE(y) t AS pos, col',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM x LATERAL VIEW POSEXPLODE(MAP(col, \'val\')) t AS pos, key, value',
      {
        write: {
          presto: 'SELECT * FROM x CROSS JOIN LATERAL (SELECT pos - 1 AS pos, key, value FROM UNNEST(MAP(ARRAY[col], ARRAY[\'val\'])) WITH ORDINALITY AS t(key, value, pos))',
          trino: 'SELECT * FROM x CROSS JOIN LATERAL (SELECT pos - 1 AS pos, key, value FROM UNNEST(MAP(ARRAY[col], ARRAY[\'val\'])) WITH ORDINALITY AS t(key, value, pos))',
          hive: 'SELECT * FROM x LATERAL VIEW POSEXPLODE(MAP(col, \'val\')) t AS pos, key, value',
          spark: 'SELECT * FROM x LATERAL VIEW POSEXPLODE(MAP(col, \'val\')) t AS pos, key, value',
        },
      },
    );
    this.validateAll(
      'SELECT a FROM x LATERAL VIEW EXPLODE(ARRAY(y)) t AS a',
      {
        write: {
          presto: 'SELECT a FROM x CROSS JOIN UNNEST(ARRAY[y]) AS t(a)',
          duckdb: 'SELECT a FROM x CROSS JOIN UNNEST([y]) AS t(a)',
          hive: 'SELECT a FROM x LATERAL VIEW EXPLODE(ARRAY(y)) t AS a',
          spark: 'SELECT a FROM x LATERAL VIEW EXPLODE(ARRAY(y)) t AS a',
        },
      },
    );
  }

  testQuotes () {
    this.validateAll(
      '\'\\\'\'',
      {
        write: {
          duckdb: '\'\'\'\'',
          presto: '\'\'\'\'',
          hive: '\'\\\'\'',
          spark: '\'\\\'\'',
        },
      },
    );
    this.validateAll(
      '\'"x"\'',
      {
        write: {
          duckdb: '\'"x"\'',
          presto: '\'"x"\'',
          hive: '\'"x"\'',
          spark: '\'"x"\'',
        },
      },
    );
    this.validateAll(
      '"\'x\'"',
      {
        write: {
          duckdb: '\'\'\'x\'\'\'',
          presto: '\'\'\'x\'\'\'',
          hive: '\'\\\'x\\\'\'',
          spark: '\'\\\'x\\\'\'',
        },
      },
    );
    this.validateAll(
      '\'\\\\\\\\a\'',
      {
        read: {
          drill: '\'\\\\\\\\a\'',
          duckdb: '\'\\\\a\'',
          presto: '\'\\\\a\'',
        },
        write: {
          drill: '\'\\\\\\\\a\'',
          duckdb: '\'\\\\a\'',
          hive: '\'\\\\\\\\a\'',
          presto: '\'\\\\a\'',
          spark: '\'\\\\\\\\a\'',
        },
      },
    );
  }

  testRegex () {
    this.validateAll(
      'a RLIKE \'x\'',
      {
        write: {
          duckdb: 'REGEXP_MATCHES(a, \'x\')',
          presto: 'REGEXP_LIKE(a, \'x\')',
          hive: 'a RLIKE \'x\'',
          spark: 'a RLIKE \'x\'',
        },
      },
    );
    this.validateAll(
      'a REGEXP \'x\'',
      {
        write: {
          duckdb: 'REGEXP_MATCHES(a, \'x\')',
          presto: 'REGEXP_LIKE(a, \'x\')',
          hive: 'a RLIKE \'x\'',
          spark: 'a RLIKE \'x\'',
        },
      },
    );
  }

  testTime () {
    this.validateAll(
      '(UNIX_TIMESTAMP(y) - UNIX_TIMESTAMP(x)) * 1000',
      {
        read: {
          presto: 'DATE_DIFF(\'millisecond\', x, y)',
        },
      },
    );
    this.validateAll(
      'UNIX_TIMESTAMP(y) - UNIX_TIMESTAMP(x)',
      {
        read: {
          presto: 'DATE_DIFF(\'second\', x, y)',
        },
      },
    );
    this.validateAll(
      '(UNIX_TIMESTAMP(y) - UNIX_TIMESTAMP(x)) / 60',
      {
        read: {
          presto: 'DATE_DIFF(\'minute\', x, y)',
        },
      },
    );
    this.validateAll(
      '(UNIX_TIMESTAMP(y) - UNIX_TIMESTAMP(x)) / 3600',
      {
        read: {
          presto: 'DATE_DIFF(\'hour\', x, y)',
        },
      },
    );
    this.validateAll(
      'DATEDIFF(a, b)',
      {
        write: {
          'duckdb': 'DATE_DIFF(\'DAY\', CAST(b AS DATE), CAST(a AS DATE))',
          'presto': 'DATE_DIFF(\'DAY\', CAST(CAST(b AS TIMESTAMP) AS DATE), CAST(CAST(a AS TIMESTAMP) AS DATE))',
          'hive': 'DATEDIFF(a, b)',
          'spark': 'DATEDIFF(a, b)',
          '': 'DATEDIFF(CAST(a AS DATE), CAST(b AS DATE))',
        },
      },
    );
    this.validateAll(
      'from_unixtime(x, "yyyy-MM-dd\'T\'HH")',
      {
        write: {
          duckdb: 'STRFTIME(TO_TIMESTAMP(x), \'%Y-%m-%d\'\'T\'\'%H\')',
          presto: 'DATE_FORMAT(FROM_UNIXTIME(x), \'%Y-%m-%d\'\'T\'\'%H\')',
          hive: 'FROM_UNIXTIME(x, \'yyyy-MM-dd\\\'T\\\'HH\')',
          spark: 'FROM_UNIXTIME(x, \'yyyy-MM-dd\\\'T\\\'HH\')',
        },
      },
    );
    this.validateAll(
      'DATE_FORMAT(\'2020-01-01\', \'yyyy-MM-dd HH:mm:ss\')',
      {
        write: {
          bigquery: 'FORMAT_DATE(\'%F %T\', CAST(\'2020-01-01\' AS DATETIME))',
          duckdb: 'STRFTIME(CAST(\'2020-01-01\' AS TIMESTAMP), \'%Y-%m-%d %H:%M:%S\')',
          presto: 'DATE_FORMAT(CAST(\'2020-01-01\' AS TIMESTAMP), \'%Y-%m-%d %T\')',
          hive: 'DATE_FORMAT(\'2020-01-01\', \'yyyy-MM-dd HH:mm:ss\')',
          spark: 'DATE_FORMAT(\'2020-01-01\', \'yyyy-MM-dd HH:mm:ss\')',
        },
      },
    );
    this.validateAll(
      'DATE_ADD(\'2020-01-01\', 1)',
      {
        write: {
          '': 'TS_OR_DS_ADD(\'2020-01-01\', 1, DAY)',
          'bigquery': 'DATE_ADD(CAST(CAST(\'2020-01-01\' AS DATETIME) AS DATE), INTERVAL 1 DAY)',
          'duckdb': 'CAST(\'2020-01-01\' AS DATE) + INTERVAL 1 DAY',
          'hive': 'DATE_ADD(\'2020-01-01\', 1)',
          'presto': 'DATE_ADD(\'DAY\', 1, CAST(CAST(\'2020-01-01\' AS TIMESTAMP) AS DATE))',
          'redshift': 'DATEADD(DAY, 1, \'2020-01-01\')',
          'snowflake': 'DATEADD(DAY, 1, CAST(CAST(\'2020-01-01\' AS TIMESTAMP) AS DATE))',
          'spark': 'DATE_ADD(\'2020-01-01\', 1)',
          'tsql': 'DATEADD(DAY, 1, CAST(CAST(\'2020-01-01\' AS DATETIME2) AS DATE))',
        },
      },
    );
    this.validateAll(
      'DATE_SUB(\'2020-01-01\', 1)',
      {
        write: {
          '': 'TS_OR_DS_ADD(\'2020-01-01\', 1 * -1, DAY)',
          'bigquery': 'DATE_ADD(CAST(CAST(\'2020-01-01\' AS DATETIME) AS DATE), INTERVAL (1 * -1) DAY)',
          'duckdb': 'CAST(\'2020-01-01\' AS DATE) + INTERVAL (1 * -1) DAY',
          'hive': 'DATE_ADD(\'2020-01-01\', 1 * -1)',
          'presto': 'DATE_ADD(\'DAY\', 1 * -1, CAST(CAST(\'2020-01-01\' AS TIMESTAMP) AS DATE))',
          'redshift': 'DATEADD(DAY, 1 * -1, \'2020-01-01\')',
          'snowflake': 'DATEADD(DAY, 1 * -1, CAST(CAST(\'2020-01-01\' AS TIMESTAMP) AS DATE))',
          'spark': 'DATE_ADD(\'2020-01-01\', 1 * -1)',
          'tsql': 'DATEADD(DAY, 1 * -1, CAST(CAST(\'2020-01-01\' AS DATETIME2) AS DATE))',
        },
      },
    );
    this.validateAll('DATE_ADD(\'2020-01-01\', -1)', { read: { '': 'DATE_SUB(\'2020-01-01\', 1)' } });
    this.validateAll('DATE_ADD(a, b * -1)', { read: { '': 'DATE_SUB(a, b)' } });
    this.validateAll('ADD_MONTHS(\'2020-01-01\', -2)', { read: { '': 'DATE_SUB(\'2020-01-01\', 2, month)' } });
    this.validateAll(
      'DATEDIFF(TO_DATE(y), x)',
      {
        write: {
          'duckdb': 'DATE_DIFF(\'DAY\', CAST(x AS DATE), TRY_CAST(y AS DATE))',
          'presto': 'DATE_DIFF(\'DAY\', CAST(CAST(x AS TIMESTAMP) AS DATE), CAST(CAST(CAST(CAST(y AS TIMESTAMP) AS DATE) AS TIMESTAMP) AS DATE))',
          'hive': 'DATEDIFF(TO_DATE(y), x)',
          'spark': 'DATEDIFF(TO_DATE(y), x)',
          '': 'DATEDIFF(TRY_CAST(y AS DATE), CAST(x AS DATE))',
        },
      },
    );
    this.validateAll(
      'UNIX_TIMESTAMP(x)',
      {
        write: {
          'duckdb': 'EPOCH(STRPTIME(x, \'%Y-%m-%d %H:%M:%S\'))',
          'presto': 'TO_UNIXTIME(COALESCE(TRY(DATE_PARSE(CAST(x AS VARCHAR), \'%Y-%m-%d %T\')), PARSE_DATETIME(DATE_FORMAT(x, \'%Y-%m-%d %T\'), \'yyyy-MM-dd HH:mm:ss\')))',
          'hive': 'UNIX_TIMESTAMP(x)',
          'spark': 'UNIX_TIMESTAMP(x)',
          '': 'STR_TO_UNIX(x, \'%Y-%m-%d %H:%M:%S\')',
        },
      },
    );

    for (const unit of [
      'DAY',
      'MONTH',
      'YEAR',
    ]) {
      this.validateAll(
        `${unit}(x)`,
        {
          write: {
            duckdb: `${unit}(CAST(x AS DATE))`,
            presto: `${unit}(CAST(CAST(x AS TIMESTAMP) AS DATE))`,
            hive: `${unit}(x)`,
            spark: `${unit}(x)`,
          },
        },
      );
    }
  }

  testOrderBy () {
    this.validateAll(
      'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname',
      {
        write: {
          duckdb: 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname NULLS FIRST',
          presto: 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname NULLS FIRST',
          hive: 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname',
          spark: 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname',
        },
      },
    );
  }

  testHive () {
    this.validateIdentity('TO_DATE(TO_DATE(x))');
    this.validateIdentity('DAY(TO_DATE(x))');
    this.validateIdentity('SELECT * FROM t WHERE col IN (\'stream\')');
    this.validateIdentity('SET hiveconf:some_var = 5', undefined, { checkCommandWarning: true });
    this.validateIdentity('(VALUES (1 AS a, 2 AS b, 3))');
    this.validateIdentity('SELECT * FROM my_table TIMESTAMP AS OF DATE_ADD(CURRENT_DATE, -1)');
    this.validateIdentity('SELECT * FROM my_table VERSION AS OF DATE_ADD(CURRENT_DATE, -1)');
    this.validateIdentity(
      'SELECT WEEKOFYEAR(\'2024-05-22\'), DAYOFMONTH(\'2024-05-22\'), DAYOFWEEK(\'2024-05-22\')',
    );
    this.validateIdentity(
      'SELECT ROW() OVER (DISTRIBUTE BY x SORT BY y)',
      'SELECT ROW() OVER (PARTITION BY x ORDER BY y)',
    );
    this.validateIdentity('SELECT transform');
    this.validateIdentity('SELECT * FROM test DISTRIBUTE BY y SORT BY x DESC ORDER BY l');
    this.validateIdentity(
      'SELECT * FROM test WHERE RAND() <= 0.1 DISTRIBUTE BY RAND() SORT BY RAND()',
    );
    this.validateIdentity('(SELECT 1 UNION SELECT 2) DISTRIBUTE BY z');
    this.validateIdentity('(SELECT 1 UNION SELECT 2) DISTRIBUTE BY z SORT BY x');
    this.validateIdentity('(SELECT 1 UNION SELECT 2) CLUSTER BY y DESC');
    this.validateIdentity('SELECT * FROM test CLUSTER BY y');
    this.validateIdentity('(SELECT 1 UNION SELECT 2) SORT BY z');
    this.validateIdentity(
      'INSERT OVERWRITE TABLE zipcodes PARTITION(state = \'0\') VALUES (896, \'US\', \'TAMPA\', 33607)',
    );
    this.validateIdentity(
      'INSERT OVERWRITE TABLE zipcodes PARTITION(state = 0) VALUES (896, \'US\', \'TAMPA\', 33607)',
    );
    this.validateIdentity(
      'INSERT OVERWRITE DIRECTORY \'x\' ROW FORMAT DELIMITED FIELDS TERMINATED BY \'\x01\' COLLECTION ITEMS TERMINATED BY \',\' MAP KEYS TERMINATED BY \':\' LINES TERMINATED BY \'\' STORED AS TEXTFILE SELECT * FROM `a`.`b`',
    );
    this.validateIdentity(
      'SELECT a, b, SUM(c) FROM tabl AS t GROUP BY a, b, GROUPING SETS ((a, b), a)',
    );
    this.validateIdentity(
      'SELECT a, b, SUM(c) FROM tabl AS t GROUP BY a, b, GROUPING SETS ((t.a, b), a)',
    );
    this.validateIdentity(
      'SELECT a, b, SUM(c) FROM tabl AS t GROUP BY a, FOO(b), GROUPING SETS ((a, FOO(b)), a)',
    );
    this.validateIdentity(
      'SELECT key, value, GROUPING__ID, COUNT(*) FROM T1 GROUP BY key, value WITH CUBE',
    );
    this.validateIdentity(
      'SELECT key, value, GROUPING__ID, COUNT(*) FROM T1 GROUP BY key, value WITH ROLLUP',
    );
    this.validateIdentity(
      'TRUNCATE TABLE t1 PARTITION(age = 10, name = \'test\', address = \'abc\')',
    );
    this.validateIdentity(
      'SELECT * FROM t1, t2',
      'SELECT * FROM t1 CROSS JOIN t2',
    );

    this.validateAll(
      'SELECT ${hiveconf:some_var}',
      {
        write: {
          hive: 'SELECT ${hiveconf:some_var}',
          spark: 'SELECT ${hiveconf:some_var}',
        },
      },
    );
    this.validateAll(
      'SELECT A.1a AS b FROM test_a AS A',
      {
        write: {
          spark: 'SELECT A.1a AS b FROM test_a AS A',
        },
      },
    );
    this.validateAll(
      'SELECT 1_a AS a FROM test_table',
      {
        write: {
          spark: 'SELECT 1_a AS a FROM test_table',
          trino: 'SELECT "1_a" AS a FROM test_table',
        },
      },
    );
    this.validateAll(
      'SELECT a_b AS 1_a FROM test_table',
      {
        write: {
          spark: 'SELECT a_b AS 1_a FROM test_table',
        },
      },
    );
    this.validateAll(
      'SELECT 1a_1a FROM test_a',
      {
        write: {
          spark: 'SELECT 1a_1a FROM test_a',
        },
      },
    );
    this.validateAll(
      'SELECT 1a AS 1a_1a FROM test_a',
      {
        write: {
          spark: 'SELECT 1a AS 1a_1a FROM test_a',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE test_table (1a STRING)',
      {
        write: {
          spark: 'CREATE TABLE test_table (1a STRING)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE test_table2 (1a_1a STRING)',
      {
        write: {
          spark: 'CREATE TABLE test_table2 (1a_1a STRING)',
        },
      },
    );
    this.validateAll(
      'PERCENTILE_APPROX(x, 0.5)',
      {
        read: {
          hive: 'PERCENTILE_APPROX(x, 0.5)',
          presto: 'APPROX_PERCENTILE(x, 0.5)',
          duckdb: 'APPROX_QUANTILE(x, 0.5)',
          spark: 'PERCENTILE_APPROX(x, 0.5)',
        },
        write: {
          hive: 'PERCENTILE_APPROX(x, 0.5)',
          presto: 'APPROX_PERCENTILE(x, 0.5)',
          duckdb: 'APPROX_QUANTILE(x, 0.5)',
          spark: 'PERCENTILE_APPROX(x, 0.5)',
        },
      },
    );
    this.validateAll(
      'PERCENTILE_APPROX(x, 0.5)',
      {
        read: {
          hive: 'PERCENTILE_APPROX(ALL x, 0.5)',
          spark2: 'PERCENTILE_APPROX(ALL x, 0.5)',
          spark: 'PERCENTILE_APPROX(ALL x, 0.5)',
          databricks: 'PERCENTILE_APPROX(ALL x, 0.5)',
        },
      },
    );
    this.validateAll(
      'PERCENTILE_APPROX(x, 0.5, 200)',
      {
        read: {
          hive: 'PERCENTILE_APPROX(ALL x, 0.5, 200)',
          spark2: 'PERCENTILE_APPROX(ALL x, 0.5, 200)',
          spark: 'PERCENTILE_APPROX(ALL x, 0.5, 200)',
          databricks: 'PERCENTILE_APPROX(ALL x, 0.5, 200)',
        },
      },
    );
    this.validateAll(
      'APPROX_COUNT_DISTINCT(a)',
      {
        write: {
          bigquery: 'APPROX_COUNT_DISTINCT(a)',
          duckdb: 'APPROX_COUNT_DISTINCT(a)',
          presto: 'APPROX_DISTINCT(a)',
          hive: 'APPROX_COUNT_DISTINCT(a)',
          snowflake: 'APPROX_COUNT_DISTINCT(a)',
          spark: 'APPROX_COUNT_DISTINCT(a)',
        },
      },
    );
    this.validateAll(
      'ARRAY_CONTAINS(x, 1)',
      {
        read: {
          duckdb: 'LIST_HAS(x, 1)',
          snowflake: 'ARRAY_CONTAINS(1, x)',
        },
        write: {
          duckdb: 'ARRAY_CONTAINS(x, 1)',
          presto: 'CONTAINS(x, 1)',
          hive: 'ARRAY_CONTAINS(x, 1)',
          spark: 'ARRAY_CONTAINS(x, 1)',
          snowflake: 'ARRAY_CONTAINS(CAST(1 AS VARIANT), x)',
        },
      },
    );
    this.validateAll(
      'SIZE(x)',
      {
        write: {
          duckdb: 'ARRAY_LENGTH(x)',
          presto: 'CARDINALITY(x)',
          hive: 'SIZE(x)',
          spark: 'SIZE(x)',
        },
      },
    );
    this.validateAll(
      'LOCATE(\'a\', x)',
      {
        write: {
          duckdb: 'STRPOS(x, \'a\')',
          presto: 'STRPOS(x, \'a\')',
          hive: 'LOCATE(\'a\', x)',
          spark: 'LOCATE(\'a\', x)',
        },
      },
    );
    this.validateAll(
      'LOCATE(\'a\', x, 3)',
      {
        write: {
          duckdb: 'CASE WHEN STRPOS(SUBSTRING(x, 3), \'a\') = 0 THEN 0 ELSE STRPOS(SUBSTRING(x, 3), \'a\') + 3 - 1 END',
          presto: 'IF(STRPOS(SUBSTRING(x, 3), \'a\') = 0, 0, STRPOS(SUBSTRING(x, 3), \'a\') + 3 - 1)',
          hive: 'LOCATE(\'a\', x, 3)',
          spark: 'LOCATE(\'a\', x, 3)',
        },
      },
    );
    this.validateAll(
      'INITCAP(\'new york\')',
      {
        write: {
          hive: 'INITCAP(\'new york\')',
          spark: 'INITCAP(\'new york\')',
        },
      },
    );
    // assert_duckdb_sql is a Python-specific test helper - skipped
    this.validateAll(
      'SELECT * FROM x.z TABLESAMPLE(10 PERCENT) y',
      {
        write: {
          hive: 'SELECT * FROM x.z TABLESAMPLE (10 PERCENT) AS y',
          spark: 'SELECT * FROM x.z TABLESAMPLE (10 PERCENT) AS y',
        },
      },
    );
    this.validateAll(
      'SELECT SORT_ARRAY(x, FALSE)',
      {
        read: {
          duckdb: 'SELECT ARRAY_REVERSE_SORT(x)',
          spark: 'SELECT SORT_ARRAY(x, FALSE)',
        },
        write: {
          duckdb: 'SELECT ARRAY_REVERSE_SORT(x)',
          presto: 'SELECT ARRAY_SORT(x, (a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END)',
          hive: 'SELECT SORT_ARRAY(x, FALSE)',
          spark: 'SELECT SORT_ARRAY(x, FALSE)',
        },
      },
    );
    this.validateAll(
      'GET_JSON_OBJECT(x, \'$.name\')',
      {
        write: {
          presto: 'JSON_EXTRACT_SCALAR(x, \'$.name\')',
          hive: 'GET_JSON_OBJECT(x, \'$.name\')',
          spark: 'GET_JSON_OBJECT(x, \'$.name\')',
        },
      },
    );
    this.validateAll(
      'MAP(a, b, c, d)',
      {
        read: {
          '': 'VAR_MAP(a, b, c, d)',
          'clickhouse': 'map(a, b, c, d)',
          'duckdb': 'MAP([a, c], [b, d])',
          'hive': 'MAP(a, b, c, d)',
          'presto': 'MAP(ARRAY[a, c], ARRAY[b, d])',
          'spark': 'MAP(a, b, c, d)',
        },
        write: {
          '': 'MAP(ARRAY(a, c), ARRAY(b, d))',
          'clickhouse': 'map(a, b, c, d)',
          'duckdb': 'MAP([a, c], [b, d])',
          'presto': 'MAP(ARRAY[a, c], ARRAY[b, d])',
          'hive': 'MAP(a, b, c, d)',
          'spark': 'MAP(a, b, c, d)',
          'snowflake': 'OBJECT_CONSTRUCT(a, b, c, d)',
        },
      },
    );
    this.validateAll(
      'MAP(a, b)',
      {
        write: {
          duckdb: 'MAP([a], [b])',
          presto: 'MAP(ARRAY[a], ARRAY[b])',
          hive: 'MAP(a, b)',
          spark: 'MAP(a, b)',
          snowflake: 'OBJECT_CONSTRUCT(a, b)',
        },
      },
    );
    this.validateAll(
      'LOG(10)',
      {
        write: {
          duckdb: 'LN(10)',
          presto: 'LN(10)',
          hive: 'LN(10)',
          spark: 'LN(10)',
        },
      },
    );
    this.validateAll(
      'ds = "2020-01-01"',
      {
        write: {
          duckdb: 'ds = \'2020-01-01\'',
          presto: 'ds = \'2020-01-01\'',
          hive: 'ds = \'2020-01-01\'',
          spark: 'ds = \'2020-01-01\'',
        },
      },
    );
    this.validateAll(
      'ds = "1\'\'2"',
      {
        write: {
          duckdb: 'ds = \'1\'\'\'\'2\'',
          presto: 'ds = \'1\'\'\'\'2\'',
          hive: 'ds = \'1\\\'\\\'2\'',
          spark: 'ds = \'1\\\'\\\'2\'',
        },
      },
    );
    this.validateAll(
      'x == 1',
      {
        write: {
          duckdb: 'x = 1',
          presto: 'x = 1',
          hive: 'x = 1',
          spark: 'x = 1',
        },
      },
    );
    this.validateAll(
      'x DIV y',
      {
        read: {
          databricks: 'x DIV y',
          duckdb: 'x // y',
          hive: 'x DIV y',
          spark2: 'x DIV y',
          spark: 'x DIV y',
        },
        write: {
          duckdb: 'x // y',
          databricks: 'x DIV y',
          presto: 'CAST(CAST(x AS DOUBLE) / y AS INTEGER)',
          spark2: 'x DIV y',
          spark: 'x DIV y',
        },
      },
    );
    this.validateAll(
      'COLLECT_LIST(x)',
      {
        read: {
          presto: 'ARRAY_AGG(x)',
        },
        write: {
          duckdb: 'ARRAY_AGG(x) FILTER(WHERE x IS NOT NULL)',
          presto: 'ARRAY_AGG(x) FILTER(WHERE x IS NOT NULL)',
          hive: 'COLLECT_LIST(x)',
          spark: 'COLLECT_LIST(x)',
        },
      },
    );
    this.validateAll(
      'COLLECT_SET(x)',
      {
        read: {
          doris: 'COLLECT_SET(x)',
          presto: 'SET_AGG(x)',
          snowflake: 'ARRAY_UNIQUE_AGG(x)',
        },
        write: {
          doris: 'COLLECT_SET(x)',
          hive: 'COLLECT_SET(x)',
          presto: 'SET_AGG(x)',
          snowflake: 'ARRAY_UNIQUE_AGG(x)',
          spark: 'COLLECT_SET(x)',
          trino: 'ARRAY_AGG(DISTINCT x)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM x TABLESAMPLE (1 PERCENT) AS foo',
      {
        read: {
          presto: 'SELECT * FROM x AS foo TABLESAMPLE BERNOULLI (1)',
          snowflake: 'SELECT * FROM x AS foo TABLESAMPLE (1)',
        },
        write: {
          hive: 'SELECT * FROM x TABLESAMPLE (1 PERCENT) AS foo',
          snowflake: 'SELECT * FROM x AS foo TABLESAMPLE (1)',
          spark: 'SELECT * FROM x TABLESAMPLE (1 PERCENT) AS foo',
        },
      },
    );
    this.validateAll(
      'SELECT a, SUM(c) FROM t GROUP BY a, DATE_FORMAT(b, \'yyyy\'), GROUPING SETS ((a, DATE_FORMAT(b, \'yyyy\')), a)',
      {
        write: {
          hive: 'SELECT a, SUM(c) FROM t GROUP BY a, DATE_FORMAT(b, \'yyyy\'), GROUPING SETS ((a, DATE_FORMAT(b, \'yyyy\')), a)',
        },
      },
    );
    this.validateAll(
      'SELECT TRUNC(CAST(ds AS TIMESTAMP), \'MONTH\')',
      {
        read: {
          hive: 'SELECT TRUNC(CAST(ds AS TIMESTAMP), \'MONTH\')',
          presto: 'SELECT DATE_TRUNC(\'MONTH\', CAST(ds AS TIMESTAMP))',
        },
        write: {
          presto: 'SELECT DATE_TRUNC(\'MONTH\', TRY_CAST(ds AS TIMESTAMP))',
        },
      },
    );

    // Hive TRUNC is date-only, should parse to TimestampTrunc (not numeric Trunc)
    this.validateIdentity('TRUNC(date_col, \'MM\')').assertIs(TimestampTruncExpr);

    // Numeric TRUNC from other dialects - Hive has no native support, uses CAST to BIGINT
    this.validateAll(
      'CAST(3.14159 AS BIGINT)',
      { read: { postgres: 'TRUNC(3.14159, 2)' } },
    );

    this.validateAll(
      'REGEXP_EXTRACT(\'abc\', \'(a)(b)(c)\')',
      {
        read: {
          hive: 'REGEXP_EXTRACT(\'abc\', \'(a)(b)(c)\')',
          spark2: 'REGEXP_EXTRACT(\'abc\', \'(a)(b)(c)\')',
          spark: 'REGEXP_EXTRACT(\'abc\', \'(a)(b)(c)\')',
          databricks: 'REGEXP_EXTRACT(\'abc\', \'(a)(b)(c)\')',
        },
        write: {
          hive: 'REGEXP_EXTRACT(\'abc\', \'(a)(b)(c)\')',
          spark2: 'REGEXP_EXTRACT(\'abc\', \'(a)(b)(c)\')',
          spark: 'REGEXP_EXTRACT(\'abc\', \'(a)(b)(c)\')',
          databricks: 'REGEXP_EXTRACT(\'abc\', \'(a)(b)(c)\')',
          presto: 'REGEXP_EXTRACT(\'abc\', \'(a)(b)(c)\', 1)',
          trino: 'REGEXP_EXTRACT(\'abc\', \'(a)(b)(c)\', 1)',
          duckdb: 'REGEXP_EXTRACT(\'abc\', \'(a)(b)(c)\', 1)',
        },
      },
    );

    this.validateIdentity('EXISTS(col, x -> x % 2 = 0)').assertIs(ExistsExpr);

    this.validateAll(
      'SELECT EXISTS(ARRAY(2, 3), x -> x % 2 = 0)',
      {
        read: {
          hive: 'SELECT EXISTS(ARRAY(2, 3), x -> x % 2 = 0)',
          spark2: 'SELECT EXISTS(ARRAY(2, 3), x -> x % 2 = 0)',
          spark: 'SELECT EXISTS(ARRAY(2, 3), x -> x % 2 = 0)',
          databricks: 'SELECT EXISTS(ARRAY(2, 3), x -> x % 2 = 0)',
        },
        write: {
          spark2: 'SELECT EXISTS(ARRAY(2, 3), x -> x % 2 = 0)',
          spark: 'SELECT EXISTS(ARRAY(2, 3), x -> x % 2 = 0)',
          databricks: 'SELECT EXISTS(ARRAY(2, 3), x -> x % 2 = 0)',
        },
      },
    );

    this.validateIdentity('SELECT 1_2');

    this.validateAll(
      'SELECT MAP(*), STRUCT(*) FROM t',
      {
        read: {
          hive: 'SELECT MAP(*), STRUCT(*) FROM t',
          spark2: 'SELECT MAP(*), STRUCT(*) FROM t',
          spark: 'SELECT MAP(*), STRUCT(*) FROM t',
          databricks: 'SELECT MAP(*), STRUCT(*) FROM t',
        },
        write: {
          spark2: 'SELECT MAP(*), STRUCT(*) FROM t',
          spark: 'SELECT MAP(*), STRUCT(*) FROM t',
          databricks: 'SELECT MAP(*), STRUCT(*) FROM t',
        },
      },
    );

    this.validateAll(
      'SELECT FIRST(sample_col) IGNORE NULLS',
      {
        read: {
          hive: 'SELECT FIRST(sample_col, TRUE)',
          spark2: 'SELECT FIRST(sample_col, TRUE)',
          spark: 'SELECT FIRST(sample_col, TRUE)',
          databricks: 'SELECT FIRST(sample_col, TRUE)',
        },
        write: {
          duckdb: 'SELECT ANY_VALUE(sample_col)',
        },
      },
    );
    this.validateIdentity('DATE_SUB(CURRENT_DATE, 1 + 1)', 'DATE_ADD(CURRENT_DATE, (1 + 1) * -1)');
    this.validateIdentity('SELECT ELT(2, \'foo\', \'bar\', \'baz\') AS Result');
  }

  testEscapes () {
    this.validateIdentity('\'\n\'', '\'\\n\'');
    this.validateIdentity('\'\\n\'');
    this.validateIdentity('\'\\\n\'', '\'\\\\\\n\'');
    this.validateIdentity('\'\\\\n\'');
    this.validateIdentity('\'\'');
    this.validateIdentity('\'\\\\\'');
    this.validateIdentity('\'\\\\z\'');
  }

  testDataType () {
    this.validateAll(
      'CAST(a AS BIT)',
      {
        write: {
          hive: 'CAST(a AS BOOLEAN)',
        },
      },
    );
  }

  testJoinsWithoutOn () {
    for (const join of [
      'FULL OUTER',
      'LEFT',
      'RIGHT',
      'LEFT OUTER',
      'RIGHT OUTER',
      'INNER',
    ]) {
      this.validateAll(
        `SELECT * FROM t1 ${join} JOIN t2 ON TRUE`,
        {
          read: {
            hive: `SELECT * FROM t1 ${join} JOIN t2`,
            spark2: `SELECT * FROM t1 ${join} JOIN t2`,
            spark: `SELECT * FROM t1 ${join} JOIN t2`,
            databricks: `SELECT * FROM t1 ${join} JOIN t2`,
            sqlite: `SELECT * FROM t1 ${join} JOIN t2`,
          },
          write: {
            hive: `SELECT * FROM t1 ${join} JOIN t2 ON TRUE`,
            spark2: `SELECT * FROM t1 ${join} JOIN t2 ON TRUE`,
            spark: `SELECT * FROM t1 ${join} JOIN t2 ON TRUE`,
            databricks: `SELECT * FROM t1 ${join} JOIN t2 ON TRUE`,
            sqlite: `SELECT * FROM t1 ${join} JOIN t2 ON TRUE`,
            duckdb: `SELECT * FROM t1 ${join} JOIN t2 ON TRUE`,
          },
        },
      );
    }
  }

  testPercentile () {
    this.validateAll(
      'PERCENTILE(x, 0.5)',
      {
        write: {
          duckdb: 'QUANTILE(x, 0.5)',
          presto: 'APPROX_PERCENTILE(x, 0.5)',
          hive: 'PERCENTILE(x, 0.5)',
          spark2: 'PERCENTILE(x, 0.5)',
          spark: 'PERCENTILE(x, 0.5)',
          databricks: 'PERCENTILE(x, 0.5)',
        },
      },
    );

    this.validateAll(
      'PERCENTILE(DISTINCT x, 0.5)',
      {
        read: {
          hive: 'PERCENTILE(DISTINCT x, 0.5)',
          spark: 'PERCENTILE(DISTINCT x, 0.5)',
          databricks: 'PERCENTILE(DISTINCT x, 0.5)',
        },
        write: {
          spark: 'PERCENTILE(DISTINCT x, 0.5)',
          databricks: 'PERCENTILE(DISTINCT x, 0.5)',
        },
      },
    );

    this.validateAll(
      'PERCENTILE(x, 0.5)',
      {
        read: {
          hive: 'PERCENTILE(ALL x, 0.5)',
          spark2: 'PERCENTILE(ALL x, 0.5)',
          spark: 'PERCENTILE(ALL x, 0.5)',
          databricks: 'PERCENTILE(ALL x, 0.5)',
        },
      },
    );

    const quantileExpr1 = this.validateIdentity('PERCENTILE(DISTINCT x, 0.5)');
    quantileExpr1.assertIs(QuantileExpr);
    narrowInstanceOf(quantileExpr1.args.this, Expression)?.assertIs(DistinctExpr);
    (quantileExpr1.args['quantile'] as Expression).assertIs(LiteralExpr);

    const quantileExpr2 = this.validateIdentity('PERCENTILE(ALL x, 0.5)', 'PERCENTILE(x, 0.5)');
    quantileExpr2.assertIs(QuantileExpr);
    narrowInstanceOf(quantileExpr2.args.this, Expression)?.assertIs(ColumnExpr);
    (quantileExpr2.args['quantile'] as Expression).assertIs(LiteralExpr);
  }
}

const t = new TestHive();
describe('TestHive', () => {
  test('bits', () => t.testBits());
  test('cast', () => t.testCast());
  test('ddl', () => t.testDdl());
  test('testLateralView', () => t.testLateralView());
  test('quotes', () => t.testQuotes());
  test('regex', () => t.testRegex());
  test('time', () => t.testTime());
  test('testOrderBy', () => t.testOrderBy());
  test('hive', () => t.testHive());
  test('escapes', () => t.testEscapes());
  test('testDataType', () => t.testDataType());
  test('testJoinsWithoutOn', () => t.testJoinsWithoutOn());
  test('percentile', () => t.testPercentile());
});
