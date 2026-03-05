import {
  describe, test,
} from 'vitest';
import { CommandExpr } from '../../src/expressions';
import { Validator } from './validator';

class TestTeradata extends Validator {
  override dialect = 'teradata' as const;

  testTeradata () {
    this.validateAll(
      'RANDOM(l, u)',
      {
        write: {
          '': '(u - l) * RAND() + l',
          'teradata': 'RANDOM(l, u)',
        },
      },
    );
    this.validateIdentity('TO_NUMBER(expr, fmt, nlsparam)');
    this.validateIdentity('SELECT TOP 10 * FROM tbl');
    this.validateIdentity('SELECT * FROM tbl SAMPLE 5');
    this.validateIdentity(
      'SELECT * FROM tbl SAMPLE 0.33, .25, .1',
      'SELECT * FROM tbl SAMPLE 0.33, 0.25, 0.1',
    );

    this.validateAll(
      'DATABASE tduser',
      {
        read: {
          databricks: 'USE tduser',
        },
        write: {
          databricks: 'USE tduser',
          teradata: 'DATABASE tduser',
        },
      },
    );

    this.validateIdentity('SELECT 0x1d', 'SELECT X\'1d\'');
    this.validateIdentity('SELECT X\'1D\'', 'SELECT X\'1D\'');
    this.validateIdentity('SELECT x\'1d\'', 'SELECT X\'1d\'');

    this.validateIdentity(
      'RENAME TABLE emp TO employee',
      undefined,
      { checkCommandWarning: true },
    ).assertIs(CommandExpr);
  }

  testTranslate () {
    this.validateIdentity('TRANSLATE(x USING LATIN_TO_UNICODE)');
    this.validateIdentity('TRANSLATE(x USING LATIN_TO_UNICODE WITH ERROR)');
  }

  testLocking () {
    this.validateIdentity('LOCKING ROW FOR ACCESS SELECT * FROM table1');
    this.validateIdentity('LOCKING TABLE table1 FOR ACCESS SELECT col1, col2 FROM table1');
    this.validateIdentity('LOCKING ROW FOR SHARE SELECT * FROM table1');
    this.validateIdentity('LOCKING DATABASE db1 FOR READ SELECT * FROM table1');
    this.validateIdentity('LOCKING ROW FOR EXCLUSIVE SELECT * FROM table1');
    this.validateIdentity('LOCKING VIEW view1 FOR ACCESS SELECT * FROM view1');

    this.validateIdentity(
      'LOCKING ROW FOR ACCESS SELECT col1, col2 FROM table1 WHERE col1 > 10',
    );
    this.validateIdentity(
      'LOCKING TABLE table1 FOR ACCESS SELECT * FROM table1 JOIN table2 ON table1.id = table2.id',
    );

    this.validateIdentity(
      'CREATE VIEW view_b AS LOCKING ROW FOR ACCESS SELECT COL1, COL2 FROM table_b',
    );
  }

  testUpdate () {
    this.validateAll(
      'UPDATE A FROM schema.tableA AS A, (SELECT col1 FROM schema.tableA GROUP BY col1) AS B SET col2 = \'\' WHERE A.col1 = B.col1',
      {
        write: {
          teradata: 'UPDATE A FROM schema.tableA AS A, (SELECT col1 FROM schema.tableA GROUP BY col1) AS B SET col2 = \'\' WHERE A.col1 = B.col1',
          mysql: 'UPDATE A JOIN `schema`.tableA AS A ON TRUE JOIN (SELECT col1 FROM `schema`.tableA GROUP BY col1) AS B ON TRUE SET A.col2 = \'\' WHERE A.col1 = B.col1',
        },
      },
    );
  }

  testStatistics () {
    this.validateIdentity('COLLECT STATISTICS ON tbl INDEX(col)', undefined, { checkCommandWarning: true });
    this.validateIdentity('COLLECT STATS ON tbl COLUMNS(col)', undefined, { checkCommandWarning: true });
    this.validateIdentity('COLLECT STATS COLUMNS(col) ON tbl', undefined, { checkCommandWarning: true });
    this.validateIdentity('HELP STATISTICS personel.employee', undefined, { checkCommandWarning: true });
    this.validateIdentity(
      'HELP STATISTICS personnel.employee FROM my_qcd',
      undefined,
      { checkCommandWarning: true },
    );
  }

  testCreate () {
    this.validateIdentity(
      'REPLACE VIEW view_b (COL1, COL2) AS LOCKING ROW FOR ACCESS SELECT COL1, COL2 FROM table_b',
      'CREATE OR REPLACE VIEW view_b (COL1, COL2) AS LOCKING ROW FOR ACCESS SELECT COL1, COL2 FROM table_b',
    );
    this.validateIdentity('CREATE TABLE x (y INT) PRIMARY INDEX (y) PARTITION BY y INDEX (y)');
    this.validateIdentity('CREATE TABLE x (y INT) PARTITION BY y INDEX (y)');
    this.validateIdentity(
      'CREATE MULTISET VOLATILE TABLE my_table (id INT) PRIMARY INDEX (id) ON COMMIT PRESERVE ROWS',
    );
    this.validateIdentity(
      'CREATE SET VOLATILE TABLE my_table (id INT) PRIMARY INDEX (id) ON COMMIT DELETE ROWS',
    );
    this.validateIdentity(
      'CREATE TABLE a (b INT) PRIMARY INDEX (y) PARTITION BY RANGE_N(b BETWEEN \'a\', \'b\' AND \'c\' EACH \'1\')',
    );
    this.validateIdentity(
      'CREATE TABLE a (b INT) PARTITION BY RANGE_N(b BETWEEN 0, 1 AND 2 EACH 1)',
    );
    this.validateIdentity(
      'CREATE TABLE a (b INT) PARTITION BY RANGE_N(b BETWEEN *, 1 AND * EACH b) INDEX (a)',
    );
    this.validateIdentity(
      'CREATE TABLE a, NO FALLBACK PROTECTION, NO LOG, NO JOURNAL, CHECKSUM=ON, NO MERGEBLOCKRATIO, BLOCKCOMPRESSION=ALWAYS (a INT)',
    );
    this.validateIdentity(
      'CREATE TABLE a, WITH JOURNAL TABLE=x.y.z, CHECKSUM=OFF, MERGEBLOCKRATIO=1, DATABLOCKSIZE=10 KBYTES (a INT)',
    );
    this.validateIdentity(
      'CREATE TABLE a, BEFORE JOURNAL, AFTER JOURNAL, FREESPACE=1, DEFAULT DATABLOCKSIZE, BLOCKCOMPRESSION=DEFAULT (a INT)',
    );
    this.validateIdentity(
      'CREATE TABLE a, DUAL JOURNAL, DUAL AFTER JOURNAL, MERGEBLOCKRATIO=1 PERCENT, DATABLOCKSIZE=10 KILOBYTES (a INT)',
    );
    this.validateIdentity(
      'CREATE TABLE a, DUAL BEFORE JOURNAL, LOCAL AFTER JOURNAL, MAXIMUM DATABLOCKSIZE, BLOCKCOMPRESSION=AUTOTEMP(c1 INT) (a INT)',
    );
    this.validateIdentity(
      'CREATE VOLATILE MULTISET TABLE a, NOT LOCAL AFTER JOURNAL, FREESPACE=1 PERCENT, DATABLOCKSIZE=10 BYTES, WITH NO CONCURRENT ISOLATED LOADING FOR ALL (a INT)',
    );
    this.validateIdentity(
      'CREATE VOLATILE SET TABLE example1 AS (SELECT col1, col2, col3 FROM table1) WITH DATA PRIMARY INDEX (col1) ON COMMIT PRESERVE ROWS',
    );
    this.validateIdentity(
      'CREATE SET GLOBAL TEMPORARY TABLE a, NO BEFORE JOURNAL, NO AFTER JOURNAL, MINIMUM DATABLOCKSIZE, BLOCKCOMPRESSION=NEVER (a INT)',
    );
    this.validateAll(
      `
            CREATE SET TABLE test, NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL,
            CHECKSUM = DEFAULT (x INT, y INT, z CHAR(30), a INT, b DATE, e INT)
            PRIMARY INDEX (a),
            INDEX(x, y)
            `,
      {
        write: {
          teradata: 'CREATE SET TABLE test, NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM=DEFAULT (x INT, y INT, z CHAR(30), a INT, b DATE, e INT) PRIMARY INDEX (a) INDEX (x, y)',
        },
      },
    );
    this.validateAll(
      'REPLACE VIEW a AS (SELECT b FROM c)',
      { write: { teradata: 'CREATE OR REPLACE VIEW a AS (SELECT b FROM c)' } },
    );
    this.validateAll(
      'CREATE VOLATILE TABLE a',
      {
        write: {
          teradata: 'CREATE VOLATILE TABLE a',
          bigquery: 'CREATE TABLE a',
          clickhouse: 'CREATE TABLE a',
          databricks: 'CREATE TABLE a',
          drill: 'CREATE TABLE a',
          duckdb: 'CREATE TABLE a',
          hive: 'CREATE TABLE a',
          mysql: 'CREATE TABLE a',
          oracle: 'CREATE TABLE a',
          postgres: 'CREATE TABLE a',
          presto: 'CREATE TABLE a',
          redshift: 'CREATE TABLE a',
          snowflake: 'CREATE TABLE a',
          spark: 'CREATE TABLE a',
          sqlite: 'CREATE TABLE a',
          starrocks: 'CREATE TABLE a',
          tableau: 'CREATE TABLE a',
          trino: 'CREATE TABLE a',
          tsql: 'CREATE TABLE a',
        },
      },
    );
    this.validateIdentity(
      'CREATE TABLE db.foo (id INT NOT NULL, valid_date DATE FORMAT \'YYYY-MM-DD\', measurement INT COMPRESS)',
    );
    this.validateIdentity(
      'CREATE TABLE db.foo (id INT NOT NULL, valid_date DATE FORMAT \'YYYY-MM-DD\', measurement INT COMPRESS (1, 2, 3))',
    );
    this.validateIdentity(
      'CREATE TABLE db.foo (id INT NOT NULL, valid_date DATE FORMAT \'YYYY-MM-DD\' COMPRESS (CAST(\'9999-09-09\' AS DATE)), measurement INT)',
    );
  }

  testInsert () {
    this.validateAll(
      'INS INTO x SELECT * FROM y',
      { write: { teradata: 'INSERT INTO x SELECT * FROM y' } },
    );
  }

  testMod () {
    this.validateAll('a MOD b', {
      write: {
        teradata: 'a MOD b',
        mysql: 'a % b',
      },
    });
  }

  testPower () {
    this.validateAll('a ** b', {
      write: {
        teradata: 'a ** b',
        mysql: 'POWER(a, b)',
      },
    });
  }

  testAbbrev () {
    this.validateIdentity('a LT b', 'a < b');
    this.validateIdentity('a LE b', 'a <= b');
    this.validateIdentity('a GT b', 'a > b');
    this.validateIdentity('a GE b', 'a >= b');
    this.validateIdentity('a ^= b', 'a <> b');
    this.validateIdentity('a NE b', 'a <> b');
    this.validateIdentity('a NOT= b', 'a <> b');
    this.validateIdentity('a EQ b', 'a = b');
    this.validateIdentity('SEL a FROM b', 'SELECT a FROM b');
    this.validateIdentity(
      'SELECT col1, col2 FROM dbc.table1 WHERE col1 EQ \'value1\' MINUS SELECT col1, col2 FROM dbc.table2',
      'SELECT col1, col2 FROM dbc.table1 WHERE col1 = \'value1\' EXCEPT SELECT col1, col2 FROM dbc.table2',
    );
    this.validateIdentity('UPD a SET b = 1', 'UPDATE a SET b = 1');
    this.validateIdentity('DEL FROM a', 'DELETE FROM a');
  }

  testDatatype () {
    this.validateAll(
      'CREATE TABLE z (a ST_GEOMETRY(1))',
      {
        write: {
          teradata: 'CREATE TABLE z (a ST_GEOMETRY(1))',
          redshift: 'CREATE TABLE z (a GEOMETRY(1))',
        },
      },
    );
    this.validateIdentity('CREATE TABLE z (a SYSUDTLIB.INT)');
  }

  testCast () {
    this.validateAll(
      'CAST(\'1992-01\' AS DATE FORMAT \'YYYY-DD\')',
      {
        read: {
          bigquery: 'CAST(\'1992-01\' AS DATE FORMAT \'YYYY-DD\')',
        },
        write: {
          'teradata': 'CAST(\'1992-01\' AS DATE FORMAT \'YYYY-DD\')',
          'bigquery': 'PARSE_DATE(\'%Y-%d\', \'1992-01\')',
          'databricks': 'TO_DATE(\'1992-01\', \'yyyy-dd\')',
          'mysql': 'STR_TO_DATE(\'1992-01\', \'%Y-%d\')',
          'spark': 'TO_DATE(\'1992-01\', \'yyyy-dd\')',
          '': 'STR_TO_DATE(\'1992-01\', \'%Y-%d\')',
        },
      },
    );
    this.validateIdentity('CAST(\'1992-01\' AS FORMAT \'YYYY-DD\')');

    this.validateAll(
      'TRYCAST(\'-2.5\' AS DECIMAL(5, 2))',
      {
        read: {
          snowflake: 'TRY_CAST(\'-2.5\' AS DECIMAL(5, 2))',
        },
        write: {
          snowflake: 'TRY_CAST(\'-2.5\' AS DECIMAL(5, 2))',
          teradata: 'TRYCAST(\'-2.5\' AS DECIMAL(5, 2))',
        },
      },
    );
  }

  testFormatOverride () {
    this.validateIdentity('SELECT (\'a\' || \'b\') (FORMAT \'...\')');
    this.validateIdentity('SELECT Col1 (FORMAT \'+9999\') FROM Test1');
    this.validateIdentity('SELECT date_col (FORMAT \'YYYY-MM-DD\') FROM t');
    this.validateIdentity(
      'SELECT CAST(Col1 AS INTEGER) FROM Test1',
      'SELECT CAST(Col1 AS INT) FROM Test1',
    );
  }

  testTime () {
    this.validateIdentity('CAST(CURRENT_TIMESTAMP(6) AS TIMESTAMP WITH TIME ZONE)');

    this.validateAll(
      'CURRENT_TIMESTAMP',
      {
        read: {
          teradata: 'CURRENT_TIMESTAMP',
          snowflake: 'CURRENT_TIMESTAMP()',
        },
      },
    );

    this.validateAll(
      'SELECT \'2023-01-01\' + INTERVAL \'5\' YEAR',
      {
        read: {
          teradata: 'SELECT \'2023-01-01\' + INTERVAL \'5\' YEAR',
          snowflake: 'SELECT DATEADD(YEAR, 5, \'2023-01-01\')',
        },
      },
    );
    this.validateAll(
      'SELECT \'2023-01-01\' - INTERVAL \'5\' YEAR',
      {
        read: {
          teradata: 'SELECT \'2023-01-01\' - INTERVAL \'5\' YEAR',
          snowflake: 'SELECT DATEADD(YEAR, -5, \'2023-01-01\')',
        },
      },
    );
    this.validateAll(
      'SELECT \'2023-01-01\' - INTERVAL \'5\' YEAR',
      {
        read: {
          teradata: 'SELECT \'2023-01-01\' - INTERVAL \'5\' YEAR',
          sqlite: 'SELECT DATE_SUB(\'2023-01-01\', 5, YEAR)',
        },
      },
    );
    this.validateAll(
      'SELECT \'2023-01-01\' + INTERVAL \'5\' YEAR',
      {
        read: {
          teradata: 'SELECT \'2023-01-01\' + INTERVAL \'5\' YEAR',
          sqlite: 'SELECT DATE_SUB(\'2023-01-01\', -5, YEAR)',
        },
      },
    );
    this.validateAll(
      'SELECT (90 * INTERVAL \'1\' DAY)',
      {
        read: {
          teradata: 'SELECT (90 * INTERVAL \'1\' DAY)',
          snowflake: 'SELECT INTERVAL \'1\' QUARTER',
        },
      },
    );
    this.validateAll(
      'SELECT (7 * INTERVAL \'1\' DAY)',
      {
        read: {
          teradata: 'SELECT (7 * INTERVAL \'1\' DAY)',
          snowflake: 'SELECT INTERVAL \'1\' WEEK',
        },
      },
    );
    this.validateAll(
      'SELECT \'2023-01-01\' + (90 * INTERVAL \'5\' DAY)',
      {
        read: {
          teradata: 'SELECT \'2023-01-01\' + (90 * INTERVAL \'5\' DAY)',
          snowflake: 'SELECT DATEADD(QUARTER, 5, \'2023-01-01\')',
        },
      },
    );
    this.validateAll(
      'SELECT \'2023-01-01\' + (7 * INTERVAL \'5\' DAY)',
      {
        read: {
          teradata: 'SELECT \'2023-01-01\' + (7 * INTERVAL \'5\' DAY)',
          snowflake: 'SELECT DATEADD(WEEK, 5, \'2023-01-01\')',
        },
      },
    );
    this.validateAll(
      'CAST(TO_CHAR(x, \'Q\') AS INT)',
      {
        read: {
          teradata: 'CAST(TO_CHAR(x, \'Q\') AS INT)',
          snowflake: 'DATE_PART(QUARTER, x)',
          bigquery: 'EXTRACT(QUARTER FROM x)',
        },
      },
    );
    this.validateAll(
      'EXTRACT(MONTH FROM x)',
      {
        read: {
          teradata: 'EXTRACT(MONTH FROM x)',
          snowflake: 'DATE_PART(MONTH, x)',
          bigquery: 'EXTRACT(MONTH FROM x)',
        },
      },
    );
    this.validateAll(
      'CAST(TO_CHAR(x, \'Q\') AS INT)',
      {
        read: {
          snowflake: 'quarter(x)',
          teradata: 'CAST(TO_CHAR(x, \'Q\') AS INT)',
        },
      },
    );
  }

  testQueryBand () {
    this.validateIdentity('SET QUERY_BAND = \'app=myapp;\' FOR SESSION');
    this.validateIdentity('SET QUERY_BAND = \'app=myapp;user=john;\' FOR TRANSACTION');
    this.validateIdentity('SET QUERY_BAND = \'priority=high;\' UPDATE FOR SESSION');
    this.validateIdentity('SET QUERY_BAND = \'workload=batch;\' UPDATE FOR TRANSACTION');
    this.validateIdentity('SET QUERY_BAND = \'org=Finance;report=Fin123;\' FOR SESSION');
    this.validateIdentity('SET QUERY_BAND = NONE FOR SESSION');
    this.validateIdentity('SET QUERY_BAND = NONE FOR SESSION VOLATILE');
    this.validateIdentity('SET QUERY_BAND = \'priority=high;\' UPDATE FOR SESSION VOLATILE');
    this.validateIdentity('SET QUERY_BAND = \'NONE\' FOR SESSION');
    this.validateIdentity('SET QUERY_BAND = \'\' FOR SESSION');
  }
}

const t = new TestTeradata();
describe('TestTeradata', () => {
  test('teradata', () => t.testTeradata());
  test('translate', () => t.testTranslate());
  test('locking', () => t.testLocking());
  test('update', () => t.testUpdate());
  test('statistics', () => t.testStatistics());
  test('create', () => t.testCreate());
  test('insert', () => t.testInsert());
  test('mod', () => t.testMod());
  test('power', () => t.testPower());
  test('abbrev', () => t.testAbbrev());
  test('datatype', () => t.testDatatype());
  test('cast', () => t.testCast());
  test('testFormatOverride', () => t.testFormatOverride());
  test('time', () => t.testTime());
  test('testQueryBand', () => t.testQueryBand());
});
