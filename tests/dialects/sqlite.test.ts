import {
  describe, test,
} from 'vitest';
import {
  TruncExpr,
} from '../../src/expressions';
import { Validator } from './validator';

class TestSQLite extends Validator {
  override dialect = 'sqlite' as const;

  testSqlite () {
    this.validateIdentity('SELECT * FROM t AS t INDEXED BY s.i');
    this.validateIdentity('SELECT * FROM t INDEXED BY s.i');
    this.validateIdentity('SELECT * FROM t INDEXED BY i');
    this.validateIdentity('SELECT * FROM t NOT INDEXED');
    this.validateIdentity('SELECT match FROM t');
    this.validateIdentity('SELECT rowid FROM t1 WHERE t1 MATCH \'lorem\'');
    this.validateIdentity('SELECT RANK() OVER (RANGE CURRENT ROW) FROM tbl');
    this.validateIdentity('UNHEX(a, b)');
    this.validateIdentity('SELECT DATE()');
    this.validateIdentity('SELECT DATE(\'now\', \'start of month\', \'+1 month\', \'-1 day\')');
    this.validateIdentity('SELECT DATETIME(1092941466, \'unixepoch\')');
    this.validateIdentity('SELECT DATETIME(1092941466, \'auto\')');
    this.validateIdentity('SELECT DATETIME(1092941466, \'unixepoch\', \'localtime\')');
    this.validateIdentity('SELECT UNIXEPOCH()');
    this.validateIdentity('SELECT JULIANDAY(\'now\') - JULIANDAY(\'1776-07-04\')');
    this.validateIdentity('SELECT UNIXEPOCH() - UNIXEPOCH(\'2004-01-01 02:34:56\')');
    this.validateIdentity('SELECT DATE(\'now\', \'start of year\', \'+9 months\', \'weekday 2\')');
    this.validateIdentity('SELECT (JULIANDAY(\'now\') - 2440587.5) * 86400.0');
    this.validateIdentity('SELECT UNIXEPOCH(\'now\', \'subsec\')');
    this.validateIdentity('SELECT TIMEDIFF(\'now\', \'1809-02-12\')');
    this.validateIdentity('SELECT * FROM GENERATE_SERIES(1, 5)');
    this.validateIdentity('SELECT INSTR(haystack, needle)');
    this.validateIdentity(
      'SELECT a, SUM(b) OVER (ORDER BY a ROWS BETWEEN -1 PRECEDING AND 1 FOLLOWING) FROM t1 ORDER BY 1',
    );
    this.validateIdentity(
      'SELECT JSON_EXTRACT(\'[10, 20, [30, 40]]\', \'$[2]\', \'$[0]\', \'$[1]\')',
    );
    this.validateIdentity(
      'SELECT item AS "item", some AS "some" FROM data WHERE (item = \'value_1\' COLLATE NOCASE) AND (some = \'t\' COLLATE NOCASE) ORDER BY item ASC LIMIT 1 OFFSET 0',
    );
    this.validateIdentity(
      'SELECT a FROM t1 WHERE a NOT NULL AND a NOT NULL ORDER BY a',
      'SELECT a FROM t1 WHERE NOT a IS NULL AND NOT a IS NULL ORDER BY a',
    );
    this.validateIdentity(
      'SELECT a, b FROM t1 WHERE b + a NOT NULL ORDER BY 1',
      'SELECT a, b FROM t1 WHERE NOT b + a IS NULL ORDER BY 1',
    );
    this.validateIdentity(
      'SELECT * FROM t1, t2',
      'SELECT * FROM t1 CROSS JOIN t2',
    );
    this.validateIdentity(
      'ALTER TABLE t RENAME a TO b',
      'ALTER TABLE t RENAME COLUMN a TO b',
    );

    this.validateAll('SELECT LIKE(y, x)', { write: { sqlite: 'SELECT x LIKE y' } });
    this.validateAll('SELECT GLOB(\'*y*\', \'xyz\')', { write: { sqlite: 'SELECT \'xyz\' GLOB \'*y*\'' } });
    this.validateAll(
      'SELECT LIKE(\'%y%\', \'xyz\', \'\')',
      { write: { sqlite: 'SELECT \'xyz\' LIKE \'%y%\' ESCAPE \'\'' } },
    );
    this.validateAll(
      'CURRENT_DATE',
      {
        read: {
          '': 'CURRENT_DATE',
          'snowflake': 'CURRENT_DATE()',
        },
      },
    );
    this.validateAll(
      'CURRENT_TIME',
      {
        read: {
          '': 'CURRENT_TIME',
        },
      },
    );
    this.validateAll(
      'CURRENT_TIMESTAMP',
      {
        read: {
          '': 'CURRENT_TIMESTAMP',
          'snowflake': 'CURRENT_TIMESTAMP()',
        },
      },
    );
    this.validateAll(
      'SELECT DATE(\'2020-01-01 16:03:05\')',
      {
        read: {
          snowflake: 'SELECT CAST(\'2020-01-01 16:03:05\' AS DATE)',
        },
      },
    );
    this.validateAll(
      'SELECT CAST([a].[b] AS SMALLINT) FROM foo',
      {
        write: {
          sqlite: 'SELECT CAST("a"."b" AS INTEGER) FROM foo',
          spark: 'SELECT CAST(`a`.`b` AS SMALLINT) FROM foo',
        },
      },
    );
    this.validateAll(
      'EDITDIST3(col1, col2)',
      {
        read: {
          sqlite: 'EDITDIST3(col1, col2)',
          spark: 'LEVENSHTEIN(col1, col2)',
        },
        write: {
          sqlite: 'EDITDIST3(col1, col2)',
          spark: 'LEVENSHTEIN(col1, col2)',
        },
      },
    );
    this.validateAll(
      'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname',
      {
        write: {
          spark: 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname',
          sqlite: 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname',
        },
      },
    );
    this.validateAll('x', { read: { snowflake: 'LEAST(x)' } });
    this.validateAll('MIN(x)', {
      read: { snowflake: 'MIN(x)' },
      write: { snowflake: 'MIN(x)' },
    });
    this.validateAll(
      'MIN(x, y, z)',
      {
        read: { snowflake: 'LEAST(x, y, z)' },
        write: { snowflake: 'LEAST(x, y, z)' },
      },
    );
    this.validateAll(
      'UNICODE(x)',
      {
        write: {
          '': 'UNICODE(x)',
          'mysql': 'ORD(CONVERT(x USING utf32))',
          'oracle': 'ASCII(UNISTR(x))',
          'postgres': 'ASCII(x)',
          'redshift': 'ASCII(x)',
          'spark': 'ASCII(x)',
        },
      },
    );
    this.validateIdentity(
      'SELECT * FROM station WHERE city IS NOT \'\'',
      'SELECT * FROM station WHERE NOT city IS \'\'',
    );
    this.validateIdentity('SELECT JSON_OBJECT(\'col1\', 1, \'col2\', \'1\')');
    this.validateIdentity(
      'CREATE TABLE "foo t" ("foo t id" TEXT NOT NULL, PRIMARY KEY ("foo t id"))',
      'CREATE TABLE "foo t" ("foo t id" TEXT NOT NULL PRIMARY KEY)',
    );
    this.validateIdentity('REPLACE INTO foo (x, y) VALUES (1, 2)', undefined, { checkCommandWarning: true });
    this.validateIdentity(
      'ATTACH DATABASE \'foo\' AS schema_name',
      'ATTACH \'foo\' AS schema_name',
    );
    this.validateIdentity(
      'ATTACH DATABASE NOT EXISTS(SELECT 1) AS schema_name',
      'ATTACH NOT EXISTS(SELECT 1) AS schema_name',
    );
    this.validateIdentity(
      'ATTACH DATABASE IIF(NOT EXISTS(SELECT 1), \'foo1\', \'foo2\') AS schema_name',
      'ATTACH IIF(NOT EXISTS(SELECT 1), \'foo1\', \'foo2\') AS schema_name',
    );
    this.validateIdentity(
      'ATTACH DATABASE \'foo\' || \'.foo2\' AS schema_name',
      'ATTACH \'foo\' || \'.foo2\' AS schema_name',
    );
    this.validateIdentity('DETACH DATABASE schema_name', 'DETACH schema_name');
    this.validateIdentity('SELECT * FROM t WHERE NULL IS y');
    this.validateIdentity(
      'SELECT * FROM t WHERE NULL IS NOT y',
      'SELECT * FROM t WHERE NOT NULL IS y',
    );
    this.validateIdentity('SELECT SQLITE_VERSION()');
  }

  testStrftime () {
    this.validateIdentity('SELECT STRFTIME(\'%Y/%m/%d\', \'now\')');
    this.validateIdentity('SELECT STRFTIME(\'%Y-%m-%d\', \'2016-10-16\', \'start of month\')');
    this.validateIdentity(
      'SELECT STRFTIME(\'%s\')',
      'SELECT STRFTIME(\'%s\', CURRENT_TIMESTAMP)',
    );

    this.validateAll(
      'SELECT STRFTIME(\'%Y-%m-%d\', \'2020-01-01 12:05:03\')',
      {
        write: {
          duckdb: 'SELECT STRFTIME(CAST(\'2020-01-01 12:05:03\' AS TIMESTAMP), \'%Y-%m-%d\')',
          sqlite: 'SELECT STRFTIME(\'%Y-%m-%d\', \'2020-01-01 12:05:03\')',
        },
      },
    );
    this.validateAll(
      'SELECT STRFTIME(\'%Y-%m-%d\', CURRENT_TIMESTAMP)',
      {
        write: {
          duckdb: 'SELECT STRFTIME(CAST(CURRENT_TIMESTAMP AS TIMESTAMP), \'%Y-%m-%d\')',
          sqlite: 'SELECT STRFTIME(\'%Y-%m-%d\', CURRENT_TIMESTAMP)',
        },
      },
    );
  }

  testDatediff () {
    this.validateAll(
      'DATEDIFF(a, b, \'day\')',
      { write: { sqlite: 'CAST((JULIANDAY(a) - JULIANDAY(b)) AS INTEGER)' } },
    );
    this.validateAll(
      'DATEDIFF(a, b, \'hour\')',
      { write: { sqlite: 'CAST((JULIANDAY(a) - JULIANDAY(b)) * 24.0 AS INTEGER)' } },
    );
    this.validateAll(
      'DATEDIFF(a, b, \'year\')',
      { write: { sqlite: 'CAST((JULIANDAY(a) - JULIANDAY(b)) / 365.0 AS INTEGER)' } },
    );
  }

  testHexadecimalLiteral () {
    this.validateAll(
      'SELECT 0XCC',
      {
        write: {
          sqlite: 'SELECT x\'CC\'',
          mysql: 'SELECT x\'CC\'',
        },
      },
    );
  }

  testWindowNullTreatment () {
    this.validateAll(
      'SELECT FIRST_VALUE(Name) OVER (PARTITION BY AlbumId ORDER BY Bytes DESC) AS LargestTrack FROM tracks',
      {
        write: {
          sqlite: 'SELECT FIRST_VALUE(Name) OVER (PARTITION BY AlbumId ORDER BY Bytes DESC) AS LargestTrack FROM tracks',
        },
      },
    );
  }

  testLongvarcharDtype () {
    this.validateAll(
      'CREATE TABLE foo (bar LONGVARCHAR)',
      { write: { sqlite: 'CREATE TABLE foo (bar TEXT)' } },
    );
  }

  testTrunc () {
    this.validateIdentity('TRUNC(3.14)').assertIs(TruncExpr);
  }

  testDdl () {
    for (const _conflictAction of [
      'ABORT',
      'FAIL',
      'IGNORE',
      'REPLACE',
      'ROLLBACK',
    ]) {
      this.validateIdentity('CREATE TABLE a (b, c, UNIQUE (b, c) ON CONFLICT IGNORE)');
    }

    this.validateIdentity('CREATE TABLE over (x, y)');
    this.validateIdentity('INSERT OR ABORT INTO foo (x, y) VALUES (1, 2)');
    this.validateIdentity('INSERT OR FAIL INTO foo (x, y) VALUES (1, 2)');
    this.validateIdentity('INSERT OR IGNORE INTO foo (x, y) VALUES (1, 2)');
    this.validateIdentity('INSERT OR REPLACE INTO foo (x, y) VALUES (1, 2)');
    this.validateIdentity('INSERT OR ROLLBACK INTO foo (x, y) VALUES (1, 2)');
    this.validateIdentity('CREATE TABLE foo (id INTEGER PRIMARY KEY ASC)');
    this.validateIdentity('CREATE TEMPORARY TABLE foo (id INTEGER)');

    this.validateAll(
      `
      CREATE TABLE "Track"
      (
          CONSTRAINT "PK_Track" FOREIGN KEY ("TrackId"),
          FOREIGN KEY ("AlbumId") REFERENCES "Album" (
              "AlbumId"
          ) ON DELETE NO ACTION ON UPDATE NO ACTION,
          FOREIGN KEY ("AlbumId") ON DELETE CASCADE ON UPDATE RESTRICT,
          FOREIGN KEY ("AlbumId") ON DELETE SET NULL ON UPDATE SET DEFAULT
      )
      `,
      {
        write: {
          sqlite: `CREATE TABLE "Track" (
  CONSTRAINT "PK_Track" FOREIGN KEY ("TrackId"),
  FOREIGN KEY ("AlbumId") REFERENCES "Album" (
    "AlbumId"
  ) ON DELETE NO ACTION ON UPDATE NO ACTION,
  FOREIGN KEY ("AlbumId") ON DELETE CASCADE ON UPDATE RESTRICT,
  FOREIGN KEY ("AlbumId") ON DELETE SET NULL ON UPDATE SET DEFAULT
)`,
        },
        pretty: true,
      },
    );
    this.validateAll(
      'CREATE TABLE z (a INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT)',
      {
        read: {
          mysql: 'CREATE TABLE z (a INT UNIQUE PRIMARY KEY AUTO_INCREMENT)',
          postgres: 'CREATE TABLE z (a INT GENERATED BY DEFAULT AS IDENTITY NOT NULL UNIQUE PRIMARY KEY)',
        },
        write: {
          sqlite: 'CREATE TABLE z (a INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT)',
          mysql: 'CREATE TABLE z (a INT UNIQUE PRIMARY KEY AUTO_INCREMENT)',
          postgres: 'CREATE TABLE z (a INT GENERATED BY DEFAULT AS IDENTITY NOT NULL UNIQUE PRIMARY KEY)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE "x" ("Name" NVARCHAR(200) NOT NULL)',
      {
        write: {
          sqlite: 'CREATE TABLE "x" ("Name" TEXT(200) NOT NULL)',
          mysql: 'CREATE TABLE `x` (`Name` VARCHAR(200) NOT NULL)',
        },
      },
    );

    this.validateIdentity(
      'CREATE TABLE store (store_id INTEGER PRIMARY KEY AUTOINCREMENT, mgr_id INTEGER NOT NULL UNIQUE REFERENCES staff ON UPDATE CASCADE)',
    );
  }

  testAnalyze () {
    this.validateIdentity('ANALYZE tbl');
    this.validateIdentity('ANALYZE schma.tbl');
  }
}

const t = new TestSQLite();
describe('TestSQLite', () => {
  test('sqlite', () => t.testSqlite());
  test('strftime', () => t.testStrftime());
  test('datediff', () => t.testDatediff());
  test('testHexadecimalLiteral', () => t.testHexadecimalLiteral());
  test('testWindowNullTreatment', () => t.testWindowNullTreatment());
  test('testLongvarcharDtype', () => t.testLongvarcharDtype());
  test('trunc', () => t.testTrunc());
  test('ddl', () => t.testDdl());
  test('analyze', () => t.testAnalyze());
});
