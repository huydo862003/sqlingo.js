import {
  describe, test, expect,
} from 'vitest';
import {
  parseOne, transpile, ParseError,
  UnsupportedError,
} from '../../../src/index';
import {
  DataTypeExpr, ToCharExpr, VarExpr,
} from '../../../src/expressions';
import {
  Validator,
} from './validator';

class TestDatabricks extends Validator {
  override dialect = 'databricks' as const;

  testDatabricks () {
    this.validateIdentity('SELECT COSH(1.5)');
    const nullType = DataTypeExpr.build('VOID', { dialect: 'databricks' });
    expect(nullType.sql()).toBe('NULL');
    expect(nullType.sql({ dialect: 'databricks' })).toBe('VOID');

    this.validateIdentity('DESCRIBE EXTENDED staging.onetrade_startb AS JSON');
    this.validateIdentity('SELECT BITMAP_BIT_POSITION(10)');
    this.validateIdentity('SELECT BITMAP_BUCKET_NUMBER(32769)');
    this.validateIdentity('SELECT BITMAP_CONSTRUCT_AGG(value)');
    this.validateIdentity('SELECT EXP(1)');
    this.validateIdentity('SELECT MODE(category)');
    this.validateIdentity('SELECT MODE(price, TRUE) AS deterministic_mode FROM products');
    this.validateIdentity('REGEXP_LIKE(x, y)');
    this.validateIdentity('SELECT CAST(NULL AS VOID)');
    this.validateIdentity('SELECT void FROM t');
    this.validateIdentity('SELECT * FROM stream');
    this.validateIdentity('SELECT * FROM STREAM t');
    this.validateIdentity('SELECT t.current_time FROM t');
    this.validateIdentity('ALTER TABLE labels ADD COLUMN label_score FLOAT');
    this.validateIdentity('DESCRIBE HISTORY a.b');
    this.validateIdentity('DESCRIBE history.tbl');
    this.validateIdentity('CREATE TABLE t (a STRUCT<c: MAP<STRING, STRING>>)');
    this.validateIdentity('CREATE TABLE t (c STRUCT<interval: DOUBLE COMMENT \'aaa\'>)');
    this.validateIdentity('CREATE TABLE my_table TBLPROPERTIES (a.b=15)');
    this.validateIdentity('CREATE TABLE my_table TBLPROPERTIES (\'a.b\'=15)');
    this.validateIdentity('SELECT CAST(\'11 23:4:0\' AS INTERVAL DAY TO HOUR)');
    this.validateIdentity('SELECT CAST(\'11 23:4:0\' AS INTERVAL DAY TO MINUTE)');
    this.validateIdentity('SELECT CAST(\'11 23:4:0\' AS INTERVAL DAY TO SECOND)');
    this.validateIdentity('SELECT CAST(\'23:00:00\' AS INTERVAL HOUR TO MINUTE)');
    this.validateIdentity('SELECT CAST(\'23:00:00\' AS INTERVAL HOUR TO SECOND)');
    this.validateIdentity('SELECT CAST(\'23:00:00\' AS INTERVAL MINUTE TO SECOND)');
    this.validateIdentity('CREATE TABLE target SHALLOW CLONE source');
    this.validateIdentity('INSERT INTO a REPLACE WHERE cond VALUES (1), (2)');
    this.validateIdentity('CREATE FUNCTION a.b(x INT) RETURNS INT RETURN x + 1');
    this.validateIdentity('CREATE FUNCTION a AS b');
    this.validateIdentity('SELECT ${x} FROM ${y} WHERE ${z} > 1');
    this.validateIdentity('CREATE TABLE foo (x DATE GENERATED ALWAYS AS (CAST(y AS DATE)))');
    this.validateIdentity('TRUNCATE TABLE t1 PARTITION(age = 10, name = \'test\', address)');
    this.validateIdentity('SELECT PARSE_JSON(\'{}\')');
    this.validateIdentity('SELECT RANDSTR(123)');
    this.validateIdentity('SELECT RANDSTR(123, 456)');

    this.validateIdentity('PARSE_URL(\'https://example.com/path\')');
    this.validateIdentity('PARSE_URL(\'https://example.com/path\', \'HOST\')');
    this.validateIdentity('PARSE_URL(\'https://example.com/path\', \'QUERY\', \'param\')');
    this.validateIdentity(
      'CREATE TABLE IF NOT EXISTS db.table (a TIMESTAMP, b BOOLEAN GENERATED ALWAYS AS (NOT a IS NULL)) USING DELTA',
    );
    this.validateIdentity(
      'SELECT * FROM sales UNPIVOT INCLUDE NULLS (sales FOR quarter IN (q1 AS `Jan-Mar`))',
    );
    this.validateIdentity(
      'SELECT * FROM sales UNPIVOT EXCLUDE NULLS (sales FOR quarter IN (q1 AS `Jan-Mar`))',
    );
    this.validateIdentity(
      'CREATE FUNCTION add_one(x INT) RETURNS INT LANGUAGE PYTHON AS $$def add_one(x):\n  return x+1$$',
    );
    this.validateIdentity(
      'CREATE FUNCTION add_one(x INT) RETURNS INT LANGUAGE PYTHON AS $FOO$def add_one(x):\n  return x+1$FOO$',
    );
    this.validateIdentity(
      'TRUNCATE TABLE t1 PARTITION(age = 10, name = \'test\', city LIKE \'LA\')',
    );
    this.validateIdentity(
      'COPY INTO target FROM `s3://link` FILEFORMAT = AVRO VALIDATE = ALL FILES = (\'file1\', \'file2\') FORMAT_OPTIONS (\'opt1\'=\'true\', \'opt2\'=\'test\') COPY_OPTIONS (\'mergeSchema\'=\'true\')',
    );
    this.validateIdentity(
      'SELECT * FROM t1, t2',
      'SELECT * FROM t1 CROSS JOIN t2',
    );
    this.validateIdentity(
      'SELECT TIMESTAMP \'2025-04-29 18.47.18\'::DATE',
      'SELECT CAST(CAST(\'2025-04-29 18.47.18\' AS DATE) AS TIMESTAMP)',
    );
    this.validateIdentity(
      'SELECT DATE_FORMAT(CAST(FROM_UTC_TIMESTAMP(foo, \'America/Los_Angeles\') AS TIMESTAMP), \'yyyy-MM-dd HH:mm:ss\') AS foo FROM t',
      'SELECT DATE_FORMAT(CAST(FROM_UTC_TIMESTAMP(CAST(foo AS TIMESTAMP), \'America/Los_Angeles\') AS TIMESTAMP), \'yyyy-MM-dd HH:mm:ss\') AS foo FROM t',
    );
    this.validateIdentity(
      'DATE_DIFF(day, created_at, current_date())',
      'DATEDIFF(DAY, created_at, CURRENT_DATE)',
    ).args['unit'].assertIs(VarExpr);
    this.validateIdentity(
      'SELECT r"\\\\foo.bar\\"',
      'SELECT \'\\\\\\\\foo.bar\\\\\'',
    );
    this.validateIdentity(
      'FROM_UTC_TIMESTAMP(x::TIMESTAMP, tz)',
      'FROM_UTC_TIMESTAMP(CAST(x AS TIMESTAMP), tz)',
    );

    this.validateIdentity('SELECT SUBSTRING_INDEX(str, delim, count)');
    this.validateIdentity('BITMAP_OR_AGG(x)');

    this.validateAll(
      'SELECT SUBSTRING_INDEX(\'a.b.c.d\', \'.\', 2)',
      {
        write: {
          databricks: 'SELECT SUBSTRING_INDEX(\'a.b.c.d\', \'.\', 2)',
          spark: 'SELECT SUBSTRING_INDEX(\'a.b.c.d\', \'.\', 2)',
          mysql: 'SELECT SUBSTRING_INDEX(\'a.b.c.d\', \'.\', 2)',
        },
      },
    );
    this.validateIdentity(
      'SELECT SUBSTR(\'Spark\' FROM 5 FOR 1)', 'SELECT SUBSTRING(\'Spark\', 5, 1)',
    );
    this.validateIdentity('SELECT SUBSTR(\'Spark SQL\', 5)', 'SELECT SUBSTRING(\'Spark SQL\', 5)');
    this.validateIdentity(
      'SELECT SUBSTR(ENCODE(\'Spark SQL\', \'utf-8\'), 5)',
      'SELECT SUBSTRING(ENCODE(\'Spark SQL\', \'utf-8\'), 5)',
    );
    this.validateAll(
      'SELECT TYPEOF(1)',
      {
        read: {
          databricks: 'SELECT TYPEOF(1)',
          snowflake: 'SELECT TYPEOF(1)',
          hive: 'SELECT TYPEOF(1)',
          clickhouse: 'SELECT toTypeName(1)',
        },
        write: {
          clickhouse: 'SELECT toTypeName(1)',
        },
      },
    );

    this.validateAll(
      'SELECT c1:item[1].price',
      {
        read: {
          spark: 'SELECT GET_JSON_OBJECT(c1, \'$.item[1].price\')',
        },
        write: {
          databricks: 'SELECT c1:item[1].price',
          spark: 'SELECT GET_JSON_OBJECT(c1, \'$.item[1].price\')',
        },
      },
    );

    this.validateAll(
      'SELECT GET_JSON_OBJECT(c1, \'$.item[1].price\')',
      {
        write: {
          databricks: 'SELECT c1:item[1].price',
          spark: 'SELECT GET_JSON_OBJECT(c1, \'$.item[1].price\')',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE foo (x INT GENERATED ALWAYS AS (YEAR(y)))',
      {
        write: {
          databricks: 'CREATE TABLE foo (x INT GENERATED ALWAYS AS (YEAR(y)))',
          tsql: 'CREATE TABLE foo (x AS YEAR(CAST(y AS DATE)))',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE t1 AS (SELECT c FROM t2)',
      {
        read: {
          teradata: 'CREATE TABLE t1 AS (SELECT c FROM t2) WITH DATA',
        },
      },
    );
    this.validateAll(
      'SELECT X\'1A2B\'',
      {
        read: {
          spark2: 'SELECT X\'1A2B\'',
          spark: 'SELECT X\'1A2B\'',
          databricks: 'SELECT x\'1A2B\'',
        },
        write: {
          spark2: 'SELECT X\'1A2B\'',
          spark: 'SELECT X\'1A2B\'',
          databricks: 'SELECT X\'1A2B\'',
        },
      },
    );

    expect(() => transpile(
      'CREATE FUNCTION add_one(x INT) RETURNS INT LANGUAGE PYTHON AS $foo$def add_one(x):\n  return x+1$$',
      { read: 'databricks' },
    )).toThrow(ParseError);

    expect(() => transpile(
      'CREATE FUNCTION add_one(x INT) RETURNS INT LANGUAGE PYTHON AS $foo bar$def add_one(x):\n  return x+1$foo bar$',
      { read: 'databricks' },
    )).toThrow(ParseError);

    this.validateAll(
      'CREATE OR REPLACE FUNCTION func(a BIGINT, b BIGINT) RETURNS TABLE (a INT) RETURN SELECT a',
      {
        write: {
          databricks: 'CREATE OR REPLACE FUNCTION func(a BIGINT, b BIGINT) RETURNS TABLE (a INT) RETURN SELECT a',
          duckdb: 'CREATE OR REPLACE FUNCTION func(a, b) AS TABLE SELECT a',
        },
      },
    );

    this.validateAll(
      'CREATE OR REPLACE FUNCTION func(a BIGINT, b BIGINT) RETURNS BIGINT RETURN a',
      {
        write: {
          databricks: 'CREATE OR REPLACE FUNCTION func(a BIGINT, b BIGINT) RETURNS BIGINT RETURN a',
          duckdb: 'CREATE OR REPLACE FUNCTION func(a, b) AS a',
        },
      },
    );

    this.validateAll(
      'SELECT ANY(col) FROM VALUES (TRUE), (FALSE) AS tab(col)',
      {
        read: {
          databricks: 'SELECT ANY(col) FROM VALUES (TRUE), (FALSE) AS tab(col)',
          spark: 'SELECT ANY(col) FROM VALUES (TRUE), (FALSE) AS tab(col)',
        },
        write: {
          spark: 'SELECT ANY(col) FROM VALUES (TRUE), (FALSE) AS tab(col)',
        },
      },
    );

    for (const option of ['', ' (foo)', ' MATCH FULL', ' NOT ENFORCED']) {
      this.validateIdentity(
        `CREATE TABLE t1 (foo BIGINT NOT NULL CONSTRAINT foo_c FOREIGN KEY REFERENCES t2${option})`,
      );
    }
    this.validateIdentity(
      'SELECT test, LISTAGG(email, \'\') AS Email FROM organizations GROUP BY test',
    );

    this.validateIdentity(
      'WITH t AS (VALUES (\'foo_val\') AS t(foo1)) SELECT foo1 FROM t',
      'WITH t AS (SELECT * FROM VALUES (\'foo_val\') AS t(foo1)) SELECT foo1 FROM t',
    );
    this.validateIdentity('NTILE() OVER (ORDER BY 1)');
    this.validateIdentity('CURRENT_VERSION()');
    this.validateAll(
      'UNIFORM(1, 10, 5)',
      {
        write: {
          snowflake: 'UNIFORM(1, 10, RANDOM(5))',
          databricks: 'UNIFORM(1, 10, 5)',
        },
      },
    );
    this.validateAll(
      'UNIFORM(1, 10)',
      {
        write: {
          databricks: 'UNIFORM(1, 10)',
          snowflake: 'UNIFORM(1, 10, RANDOM())',
        },
      },
    );
    this.validateIdentity('SELECT ELT(2, \'foo\', \'bar\', \'baz\') AS Result');
    this.validateIdentity('GETDATE()', 'CURRENT_TIMESTAMP()');
    this.validateIdentity('NOW()', 'CURRENT_TIMESTAMP()');
    this.validateIdentity('CURRENT_TIMEZONE()');
    this.validateIdentity('CURDATE()', 'CURRENT_DATE');
    this.validateIdentity('CURDATE', 'CURRENT_DATE');
    this.validateIdentity('SELECT MAKE_INTERVAL(100, 11, 12, 13, 14, 14, 15)');
    this.validateIdentity('SELECT name, GROUPING_ID() FROM customer GROUP BY ROLLUP (name)');
    this.validateIdentity('BIT_GET(11, 0)', 'GETBIT(11, 0)');
    this.validateIdentity('SELECT CURDATE()', 'SELECT CURRENT_DATE');
  }

  // https://docs.databricks.com/sql/language-manual/functions/colonsign.html
  testJson () {
    this.validateIdentity('SELECT c1:price, c1:price.foo, c1:price.bar[1]');
    this.validateIdentity('SELECT TRY_CAST(c1:price AS ARRAY<VARIANT>)');
    this.validateIdentity('SELECT TRY_CAST(c1:["foo bar"]["baz qux"] AS ARRAY<VARIANT>)');
    this.validateIdentity(
      'SELECT c1:item[1].price FROM VALUES (\'{ "item": [ { "model" : "basic", "price" : 6.12 }, { "model" : "medium", "price" : 9.24 } ] }\') AS T(c1)',
    );
    this.validateIdentity(
      'SELECT c1:item[*].price FROM VALUES (\'{ "item": [ { "model" : "basic", "price" : 6.12 }, { "model" : "medium", "price" : 9.24 } ] }\') AS T(c1)',
    );
    this.validateIdentity(
      'SELECT FROM_JSON(c1:item[*].price, \'ARRAY<DOUBLE>\')[0] FROM VALUES (\'{ "item": [ { "model" : "basic", "price" : 6.12 }, { "model" : "medium", "price" : 9.24 } ] }\') AS T(c1)',
    );
    this.validateIdentity(
      'SELECT INLINE(FROM_JSON(c1:item[*], \'ARRAY<STRUCT<model STRING, price DOUBLE>>\')) FROM VALUES (\'{ "item": [ { "model" : "basic", "price" : 6.12 }, { "model" : "medium", "price" : 9.24 } ] }\') AS T(c1)',
    );
    this.validateIdentity(
      'SELECT c1:[\'price\'] FROM VALUES (\'{ "price": 5 }\') AS T(c1)',
      'SELECT c1:price FROM VALUES (\'{ "price": 5 }\') AS T(c1)',
    );
    this.validateIdentity(
      'SELECT GET_JSON_OBJECT(c1, \'$.price\') FROM VALUES (\'{ "price": 5 }\') AS T(c1)',
      'SELECT c1:price FROM VALUES (\'{ "price": 5 }\') AS T(c1)',
    );
    this.validateIdentity(
      'SELECT raw:`zip code`, raw:`fb:testid`, raw:store[\'bicycle\'], raw:store["zip code"]',
      'SELECT raw:["zip code"], raw:["fb:testid"], raw:store.bicycle, raw:store["zip code"]',
    );
    this.validateAll(
      'SELECT col:`fr\'uit`',
      {
        write: {
          databricks: 'SELECT col:["fr\'uit"]',
          postgres: 'SELECT JSON_EXTRACT_PATH(col, \'fr\'\'uit\')',
        },
      },
    );
  }

  testDatediff () {
    this.validateAll(
      'SELECT DATEDIFF(year, \'start\', \'end\')',
      {
        write: {
          tsql: 'SELECT DATEDIFF(YEAR, \'start\', \'end\')',
          databricks: 'SELECT DATEDIFF(YEAR, \'start\', \'end\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(microsecond, \'start\', \'end\')',
      {
        write: {
          databricks: 'SELECT DATEDIFF(MICROSECOND, \'start\', \'end\')',
          postgres: 'SELECT CAST(EXTRACT(epoch FROM CAST(\'end\' AS TIMESTAMP) - CAST(\'start\' AS TIMESTAMP)) * 1000000 AS BIGINT)',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(millisecond, \'start\', \'end\')',
      {
        write: {
          databricks: 'SELECT DATEDIFF(MILLISECOND, \'start\', \'end\')',
          postgres: 'SELECT CAST(EXTRACT(epoch FROM CAST(\'end\' AS TIMESTAMP) - CAST(\'start\' AS TIMESTAMP)) * 1000 AS BIGINT)',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(second, \'start\', \'end\')',
      {
        write: {
          databricks: 'SELECT DATEDIFF(SECOND, \'start\', \'end\')',
          postgres: 'SELECT CAST(EXTRACT(epoch FROM CAST(\'end\' AS TIMESTAMP) - CAST(\'start\' AS TIMESTAMP)) AS BIGINT)',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(minute, \'start\', \'end\')',
      {
        write: {
          databricks: 'SELECT DATEDIFF(MINUTE, \'start\', \'end\')',
          postgres: 'SELECT CAST(EXTRACT(epoch FROM CAST(\'end\' AS TIMESTAMP) - CAST(\'start\' AS TIMESTAMP)) / 60 AS BIGINT)',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(hour, \'start\', \'end\')',
      {
        write: {
          databricks: 'SELECT DATEDIFF(HOUR, \'start\', \'end\')',
          postgres: 'SELECT CAST(EXTRACT(epoch FROM CAST(\'end\' AS TIMESTAMP) - CAST(\'start\' AS TIMESTAMP)) / 3600 AS BIGINT)',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(day, \'start\', \'end\')',
      {
        write: {
          databricks: 'SELECT DATEDIFF(DAY, \'start\', \'end\')',
          postgres: 'SELECT CAST(EXTRACT(epoch FROM CAST(\'end\' AS TIMESTAMP) - CAST(\'start\' AS TIMESTAMP)) / 86400 AS BIGINT)',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(week, \'start\', \'end\')',
      {
        write: {
          databricks: 'SELECT DATEDIFF(WEEK, \'start\', \'end\')',
          postgres: 'SELECT CAST(EXTRACT(days FROM (CAST(\'end\' AS TIMESTAMP) - CAST(\'start\' AS TIMESTAMP))) / 7 AS BIGINT)',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(month, \'start\', \'end\')',
      {
        write: {
          databricks: 'SELECT DATEDIFF(MONTH, \'start\', \'end\')',
          postgres: 'SELECT CAST(EXTRACT(year FROM AGE(CAST(\'end\' AS TIMESTAMP), CAST(\'start\' AS TIMESTAMP))) * 12 + EXTRACT(month FROM AGE(CAST(\'end\' AS TIMESTAMP), CAST(\'start\' AS TIMESTAMP))) AS BIGINT)',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(quarter, \'start\', \'end\')',
      {
        write: {
          databricks: 'SELECT DATEDIFF(QUARTER, \'start\', \'end\')',
          postgres: 'SELECT CAST(EXTRACT(year FROM AGE(CAST(\'end\' AS TIMESTAMP), CAST(\'start\' AS TIMESTAMP))) * 4 + EXTRACT(month FROM AGE(CAST(\'end\' AS TIMESTAMP), CAST(\'start\' AS TIMESTAMP))) / 3 AS BIGINT)',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(year, \'start\', \'end\')',
      {
        write: {
          databricks: 'SELECT DATEDIFF(YEAR, \'start\', \'end\')',
          postgres: 'SELECT CAST(EXTRACT(year FROM AGE(CAST(\'end\' AS TIMESTAMP), CAST(\'start\' AS TIMESTAMP))) AS BIGINT)',
        },
      },
    );
  }

  testAddDate () {
    this.validateAll(
      'SELECT DATEADD(year, 1, \'2020-01-01\')',
      {
        write: {
          tsql: 'SELECT DATEADD(YEAR, 1, \'2020-01-01\')',
          databricks: 'SELECT DATEADD(YEAR, 1, \'2020-01-01\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(\'end\', \'start\')',
      {
        write: { databricks: 'SELECT DATEDIFF(DAY, \'start\', \'end\')' },
      },
    );
    this.validateAll(
      'SELECT DATE_ADD(\'2020-01-01\', 1)',
      {
        write: {
          tsql: 'SELECT DATEADD(DAY, 1, \'2020-01-01\')',
          databricks: 'SELECT DATEADD(DAY, 1, \'2020-01-01\')',
        },
      },
    );
  }

  testWithoutAs () {
    this.validateAll(
      'CREATE TABLE x (SELECT 1)',
      {
        write: {
          databricks: 'CREATE TABLE x AS (SELECT 1)',
        },
      },
    );

    this.validateAll(
      'WITH x (select 1) SELECT * FROM x',
      {
        write: {
          databricks: 'WITH x AS (SELECT 1) SELECT * FROM x',
        },
      },
    );
  }

  testStreamingTables () {
    this.validateIdentity(
      'CREATE STREAMING TABLE raw_data AS SELECT * FROM STREAM READ_FILES(\'abfss://container@storageAccount.dfs.core.windows.net/base/path\')',
    );
    this.validateIdentity(
      'CREATE OR REFRESH STREAMING TABLE csv_data (id INT, ts TIMESTAMP, event STRING) AS SELECT * FROM STREAM READ_FILES(\'s3://bucket/path\', format => \'csv\', schema => \'id int, ts timestamp, event string\')',
    );
  }

  testGrant () {
    this.validateIdentity('GRANT CREATE ON SCHEMA my_schema TO `alf@melmak.et`');
    this.validateIdentity('GRANT SELECT ON TABLE sample_data TO `alf@melmak.et`');
    this.validateIdentity('GRANT ALL PRIVILEGES ON TABLE forecasts TO finance');
    this.validateIdentity('GRANT SELECT ON TABLE t TO `fab9e00e-ca35-11ec-9d64-0242ac120002`');
  }

  testRevoke () {
    this.validateIdentity('REVOKE CREATE ON SCHEMA my_schema FROM `alf@melmak.et`');
    this.validateIdentity('REVOKE SELECT ON TABLE sample_data FROM `alf@melmak.et`');
    this.validateIdentity('REVOKE ALL PRIVILEGES ON TABLE forecasts FROM finance');
    this.validateIdentity(
      'REVOKE SELECT ON TABLE t FROM `fab9e00e-ca35-11ec-9d64-0242ac120002`',
    );
  }

  testAnalyze () {
    this.validateIdentity('ANALYZE TABLE tbl COMPUTE DELTA STATISTICS NOSCAN');
    this.validateIdentity('ANALYZE TABLE tbl COMPUTE DELTA STATISTICS FOR ALL COLUMNS');
    this.validateIdentity('ANALYZE TABLE tbl COMPUTE DELTA STATISTICS FOR COLUMNS foo, bar');
    this.validateIdentity('ANALYZE TABLE ctlg.db.tbl COMPUTE DELTA STATISTICS NOSCAN');
    this.validateIdentity('ANALYZE TABLES COMPUTE STATISTICS NOSCAN');
    this.validateIdentity('ANALYZE TABLES FROM db COMPUTE STATISTICS');
    this.validateIdentity('ANALYZE TABLES IN db COMPUTE STATISTICS');
    this.validateIdentity(
      'ANALYZE TABLE ctlg.db.tbl PARTITION(foo = \'foo\', bar = \'bar\') COMPUTE STATISTICS NOSCAN',
    );
  }

  testUdfEnvironmentProperty () {
    this.validateIdentity(
      'CREATE FUNCTION a() ENVIRONMENT (dependencies = \'["foo1==1", "foo2==2"]\', environment_version = \'None\')',
    );
  }

  testToCharIsNumericTranspileToCast () {
    // The input SQL simulates a TO_CHAR with is_numeric flag set (from dremio dialect)
    const sql = 'SELECT TO_CHAR(12345, \'#\')';
    const expression = parseOne(sql, { read: 'dremio' });

    const toCharExp = expression.find(ToCharExpr);
    expect(toCharExp).not.toBeUndefined();
    expect(toCharExp?.args['isNumeric']).toBe(true);

    const result = transpile(sql, { read: 'dremio', write: 'databricks' })[0];
    expect(result).toContain('CAST(12345 AS STRING)');
  }

  testQdcolon () {
    this.validateIdentity('SELECT \'20\'?::INTEGER', 'SELECT TRY_CAST(\'20\' AS INTEGER)');
  }

  testOverlay () {
    this.validateIdentity(
      'SELECT OVERLAY(\'Spark SQL\', \'ANSI \', 7, 0)',
      'SELECT OVERLAY(\'Spark SQL\' PLACING \'ANSI \' FROM 7 FOR 0)',
    );
    this.validateIdentity(
      'SELECT OVERLAY(\'Spark SQL\' PLACING \'CORE\' FROM 7)',
    );
    this.validateIdentity(
      'SELECT OVERLAY(ENCODE(\'Spark SQL\', \'utf-8\') PLACING ENCODE(\'_\', \'utf-8\') FROM 6)',
    );
    this.validateIdentity(
      'SELECT OVERLAY(\'Spark SQL\' PLACING \'ANSI \' FROM 7 FOR 0)',
    );
  }

  testDeclare () {
    this.validateIdentity('DECLARE VAR x INT', 'DECLARE x INT');
    this.validateIdentity('DECLARE x INT');
    this.validateIdentity('DECLARE VARIABLE myvar INT DEFAULT 1', 'DECLARE myvar INT = 1');
    this.validateIdentity('DECLARE x, y, z INT DEFAULT 1', 'DECLARE x, y, z INT = 1');
    this.validateIdentity('DECLARE x INT = 1');
  }
}

const t = new TestDatabricks();

describe('TestDatabricks', () => {
  test('databricks', () => t.testDatabricks());
  test('json', () => t.testJson());
  test('datediff', () => t.testDatediff());
  test('addDate', () => t.testAddDate());
  test('withoutAs', () => t.testWithoutAs());
  test('streamingTables', () => t.testStreamingTables());
  test('grant', () => t.testGrant());
  test('revoke', () => t.testRevoke());
  test('analyze', () => t.testAnalyze());
  test('udfEnvironmentProperty', () => t.testUdfEnvironmentProperty());
  test('toCharIsNumericTranspileToCast', () => t.testToCharIsNumericTranspileToCast());
  test('qdcolon', () => t.testQdcolon());
  test('overlay', () => t.testOverlay());
  test('declare', () => t.testDeclare());
});
