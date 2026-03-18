import {
  describe, test, expect,
} from 'vitest';
import { ToCharExpr } from '../../../src/expressions';
import {
  transpile, ErrorLevel,
} from '../../../src/index';
import {
  Validator, UnsupportedError,
} from './validator';

class TestDremio extends Validator {
  override dialect = 'dremio' as const;

  testTypeMappings () {
    this.validateIdentity('CAST(x AS SMALLINT)', 'CAST(x AS INT)');
    this.validateIdentity('CAST(x AS TINYINT)', 'CAST(x AS INT)');
    this.validateIdentity('CAST(x AS BINARY)', 'CAST(x AS VARBINARY)');
    this.validateIdentity('CAST(x AS TEXT)', 'CAST(x AS VARCHAR)');
    this.validateIdentity('CAST(x AS NCHAR)', 'CAST(x AS VARCHAR)');
    this.validateIdentity('CAST(x AS CHAR)', 'CAST(x AS VARCHAR)');
    this.validateIdentity('CAST(x AS TIMESTAMPNTZ)', 'CAST(x AS TIMESTAMP)');
    this.validateIdentity('CAST(x AS DATETIME)', 'CAST(x AS TIMESTAMP)');
    this.validateIdentity('CAST(x AS ARRAY)', 'CAST(x AS LIST)');
    this.validateIdentity('CAST(x AS BIT)', 'CAST(x AS BOOLEAN)');

    expect(() =>
      transpile('CAST(x AS TIMESTAMPTZ)', {
        read: 'oracle',
        write: 'dremio',
        unsupportedLevel: ErrorLevel.RAISE,
      })).toThrow(UnsupportedError);
    expect(() =>
      transpile('CAST(x AS TIMESTAMPLTZ)', {
        read: 'oracle',
        write: 'dremio',
        unsupportedLevel: ErrorLevel.RAISE,
      })).toThrow(UnsupportedError);
  }

  testConcatCoalesce () {
    this.validateAll(
      'SELECT CONCAT(\'a\', NULL)',
      {
        write: {
          'dremio': 'SELECT CONCAT(\'a\', NULL)',
          '': 'SELECT CONCAT(\'a\', COALESCE(NULL, \'\'))',
        },
      },
    );
  }

  testNullOrdering () {
    this.validateIdentity(
      'SELECT * FROM t ORDER BY a NULLS LAST',
      'SELECT * FROM t ORDER BY a',
    );
    this.validateIdentity(
      'SELECT * FROM t ORDER BY a DESC NULLS LAST',
      'SELECT * FROM t ORDER BY a DESC',
    );

    this.validateIdentity('SELECT * FROM t ORDER BY a NULLS FIRST');
    this.validateIdentity('SELECT * FROM t ORDER BY a DESC NULLS FIRST');
  }

  testConvertTimezone () {
    this.validateAll(
      'SELECT CONVERT_TIMEZONE(\'America/Chicago\', DateColumn)',
      {
        write: {
          'dremio': 'SELECT CONVERT_TIMEZONE(\'America/Chicago\', DateColumn)',
          '': 'SELECT DateColumn AT TIME ZONE \'America/Chicago\'',
        },
      },
    );
  }

  testIntervalPlural () {
    this.validateIdentity('INTERVAL \'7\' DAYS', 'INTERVAL \'7\' DAY');
  }

  testLimitOnlyLiterals () {
    this.validateIdentity('SELECT * FROM t LIMIT 1 + 1', 'SELECT * FROM t LIMIT 2');
  }

  testMultiArgDistinctUnsupported () {
    this.validateIdentity(
      'SELECT COUNT(DISTINCT a, b) FROM t',
      'SELECT COUNT(DISTINCT CASE WHEN a IS NULL THEN NULL WHEN b IS NULL THEN NULL ELSE (a, b) END) FROM t',
    );
  }

  testTimeMapping () {
    const ts = 'CAST(\'2025-06-24 12:34:56\' AS TIMESTAMP)';

    this.validateAll(
      `SELECT TO_CHAR(${ts}, 'yyyy-mm-dd hh24:mi:ss')`,
      {
        read: {
          dremio: `SELECT TO_CHAR(${ts}, 'yyyy-mm-dd hh24:mi:ss')`,
          postgres: `SELECT TO_CHAR(${ts}, 'YYYY-MM-DD HH24:MI:SS')`,
          oracle: `SELECT TO_CHAR(${ts}, 'YYYY-MM-DD HH24:MI:SS')`,
          duckdb: `SELECT STRFTIME(${ts}, '%Y-%m-%d %H:%M:%S')`,
        },
        write: {
          dremio: `SELECT TO_CHAR(${ts}, 'yyyy-mm-dd hh24:mi:ss')`,
          postgres: `SELECT TO_CHAR(${ts}, 'YYYY-MM-DD HH24:MI:SS')`,
          oracle: `SELECT TO_CHAR(${ts}, 'YYYY-MM-DD HH24:MI:SS')`,
          duckdb: `SELECT STRFTIME(${ts}, '%Y-%m-%d %H:%M:%S')`,
        },
      },
    );

    this.validateAll(
      `SELECT TO_CHAR(${ts}, 'yy-ddd hh24:mi:ss.fff tzd')`,
      {
        read: {
          dremio: `SELECT TO_CHAR(${ts}, 'yy-ddd hh24:mi:ss.fff tzd')`,
          postgres: `SELECT TO_CHAR(${ts}, 'YY-DDD HH24:MI:SS.US TZ')`,
          oracle: `SELECT TO_CHAR(${ts}, 'YY-DDD HH24:MI:SS.FF6 %Z')`,
          duckdb: `SELECT STRFTIME(${ts}, '%y-%j %H:%M:%S.%f %Z')`,
        },
        write: {
          dremio: `SELECT TO_CHAR(${ts}, 'yy-ddd hh24:mi:ss.fff tzd')`,
          postgres: `SELECT TO_CHAR(${ts}, 'YY-DDD HH24:MI:SS.US TZ')`,
          oracle: `SELECT TO_CHAR(${ts}, 'YY-DDD HH24:MI:SS.FF6 %Z')`,
          duckdb: `SELECT STRFTIME(${ts}, '%y-%j %H:%M:%S.%f %Z')`,
        },
      },
    );
  }

  testToCharSpecial () {
    const toChar1 = this.validateIdentity('TO_CHAR(5555, \'#\')').assertIs(ToCharExpr);
    expect(toChar1.args['isNumeric']).toBe(true);

    const toChar2 = this.validateIdentity('TO_CHAR(3.14, \'#.#\')').assertIs(ToCharExpr);
    expect(toChar2.args['isNumeric']).toBe(true);

    const toChar3 = this.validateIdentity('TO_CHAR(columnname, \'#.##\')').assertIs(ToCharExpr);
    expect(toChar3.args['isNumeric']).toBe(true);

    const toChar4 = this.validateIdentity('TO_CHAR(5555)').assertIs(ToCharExpr);
    expect(toChar4.args['isNumeric']).toBeFalsy();

    const toChar5 = this.validateIdentity('TO_CHAR(3.14, columnname)').assertIs(ToCharExpr);
    expect(toChar5.args['isNumeric']).toBeFalsy();

    const toChar6 = this.validateIdentity('TO_CHAR(123, \'abcd\')').assertIs(ToCharExpr);
    expect(toChar6.args['isNumeric']).toBeFalsy();

    const toChar7 = this.validateIdentity('TO_CHAR(3.14, UPPER(\'abcd\'))').assertIs(ToCharExpr);
    expect(toChar7.args['isNumeric']).toBeFalsy();
  }

  testDateAdd () {
    this.validateIdentity('SELECT DATE_ADD(col, 1)');
    this.validateIdentity('SELECT DATE_ADD(col, CAST(1 AS INTERVAL HOUR))');
    this.validateIdentity(
      'SELECT DATE_ADD(TIMESTAMP \'2022-01-01 12:00:00\', CAST(-1 AS INTERVAL HOUR))',
      'SELECT DATE_ADD(CAST(\'2022-01-01 12:00:00\' AS TIMESTAMP), CAST(-1 AS INTERVAL HOUR))',
    );
  }

  testDateSub () {
    this.validateIdentity('SELECT DATE_SUB(col, 1)');
    this.validateIdentity('SELECT DATE_SUB(col, CAST(1 AS INTERVAL HOUR))');
    this.validateIdentity(
      'SELECT DATE_SUB(TIMESTAMP \'2022-01-01 12:00:00\', CAST(-1 AS INTERVAL HOUR))',
      'SELECT DATE_SUB(CAST(\'2022-01-01 12:00:00\' AS TIMESTAMP), CAST(-1 AS INTERVAL HOUR))',
    );
  }

  testDatetimeParsing () {
    this.validateIdentity(
      'SELECT DATE_FORMAT(CAST(\'2025-08-18 15:30:00\' AS TIMESTAMP), \'yyyy-mm-dd\')',
      'SELECT TO_CHAR(CAST(\'2025-08-18 15:30:00\' AS TIMESTAMP), \'yyyy-mm-dd\')',
    );
  }

  testArrayGenerateRange () {
    this.validateAll(
      'ARRAY_GENERATE_RANGE(1, 4)',
      {
        read: { dremio: 'ARRAY_GENERATE_RANGE(1, 4)' },
        write: { duckdb: 'GENERATE_SERIES(1, 4)' },
      },
    );
  }

  testCurrentDateUtc () {
    this.validateIdentity('SELECT CURRENT_DATE_UTC');
    this.validateIdentity('SELECT CURRENT_DATE_UTC()', 'SELECT CURRENT_DATE_UTC');
  }

  testRepeatstr () {
    this.validateIdentity('SELECT REPEAT(x, 5)');
    this.validateIdentity('SELECT REPEATSTR(x, 5)', 'SELECT REPEAT(x, 5)');
  }

  testRegexpLike () {
    this.validateAll(
      'REGEXP_MATCHES(x, y)',
      {
        write: {
          dremio: 'REGEXP_LIKE(x, y)',
          duckdb: 'REGEXP_MATCHES(x, y)',
          presto: 'REGEXP_LIKE(x, y)',
          hive: 'x RLIKE y',
          spark: 'x RLIKE y',
        },
      },
    );
    this.validateIdentity('REGEXP_MATCHES(x, y)', 'REGEXP_LIKE(x, y)');
  }

  testDatePart () {
    this.validateIdentity(
      'SELECT DATE_PART(\'YEAR\', date \'2021-04-01\')',
      'SELECT EXTRACT(\'YEAR\' FROM CAST(\'2021-04-01\' AS DATE))',
    );
  }

  testDatetype () {
    this.validateIdentity('DATETYPE(2024,2,2)', 'DATE(\'2024-02-02\')');
    this.validateIdentity('DATETYPE(x,y,z)', 'CAST(CONCAT(x, \'-\', y, \'-\', z) AS DATE)');
  }

  testTryCast () {
    this.validateAll(
      'CAST(a AS FLOAT)',
      {
        read: {
          'dremio': 'CAST(a AS FLOAT)',
          '': 'TRY_CAST(a AS FLOAT)',
          'hive': 'CAST(a AS FLOAT)',
        },
      },
    );
  }
}

const t = new TestDremio();
describe('TestDremio', () => {
  test('testTypeMappings', () => t.testTypeMappings());
  test('testConcatCoalesce', () => t.testConcatCoalesce());
  test('testNullOrdering', () => t.testNullOrdering());
  test('testConvertTimezone', () => t.testConvertTimezone());
  test('testIntervalPlural', () => t.testIntervalPlural());
  test('testLimitOnlyLiterals', () => t.testLimitOnlyLiterals());
  test('testMultiArgDistinctUnsupported', () => t.testMultiArgDistinctUnsupported());
  test('testTimeMapping', () => t.testTimeMapping());
  test('testToCharSpecial', () => t.testToCharSpecial());
  test('testDateAdd', () => t.testDateAdd());
  test('testDateSub', () => t.testDateSub());
  test('testDatetimeParsing', () => t.testDatetimeParsing());
  test('testArrayGenerateRange', () => t.testArrayGenerateRange());
  test('testCurrentDateUtc', () => t.testCurrentDateUtc());
  test('testRepeatstr', () => t.testRepeatstr());
  test('testRegexpLike', () => t.testRegexpLike());
  test('testDatePart', () => t.testDatePart());
  test('testDatetype', () => t.testDatetype());
  test('testTryCast', () => t.testTryCast());
});
