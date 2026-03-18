import {
  describe, test, expect,
} from 'vitest';
import {
  transpile, parseOne, ParseError, ErrorLevel,
} from '../../src/index';
import { NormalizeFunctions } from '../../src/dialects/dialect';
import {
  loadSqlFixtures, loadSqlFixturePairs,
} from './helpers';

class TestTranspile {
  validate (sql: string, target: string) {
    expect(transpile(sql)[0]).toBe(target);
  }

  testWeirdChars () {
    expect(transpile('0Êß')[0]).toBe('0 AS Êß');
    expect(transpile('SELECT\u3000* FROM t WHERE c = 1')[0]).toBe('SELECT * FROM t WHERE c = 1');
  }

  testAlias () {
    expect(transpile('SELECT SUM(y) KEEP')[0]).toBe('SELECT SUM(y) AS KEEP');
    expect(transpile('SELECT 1 overwrite')[0]).toBe('SELECT 1 AS overwrite');
    expect(transpile('SELECT 1 is')[0]).toBe('SELECT 1 AS is');
    expect(transpile('SELECT 1 current_time')[0]).toBe('SELECT 1 AS current_time');
    expect(transpile('SELECT 1 current_timestamp')[0]).toBe('SELECT 1 AS current_timestamp');
    expect(transpile('SELECT 1 current_date')[0]).toBe('SELECT 1 AS current_date');
    expect(transpile('SELECT 1 current_datetime')[0]).toBe('SELECT 1 AS current_datetime');
    expect(transpile('SELECT 1 row')[0]).toBe('SELECT 1 AS row');

    expect(transpile('SELECT 1 FROM a.b.table1 t UNPIVOT((c3) FOR c4 IN (a, b))')[0]).toBe(
      'SELECT 1 FROM a.b.table1 AS t UNPIVOT((c3) FOR c4 IN (a, b))',
    );

    for (const key of [
      'union',
      'from',
      'join',
    ]) {
      expect(transpile(`SELECT x AS ${key}`)[0]).toBe(`SELECT x AS ${key}`);
      expect(transpile(`SELECT x "${key}"`)[0]).toBe(`SELECT x AS "${key}"`);
      expect(() => transpile(`SELECT x ${key}`)).toThrow(ParseError);
    }
  }

  testUnary () {
    expect(transpile('+++1')[0]).toBe('1');
    expect(transpile('+-1')[0]).toBe('-1');
    expect(transpile('+- - -1')[0]).toBe('- - -1');
  }

  testParen () {
    expect(() => transpile('1 + (2 + 3')).toThrow(ParseError);
  }

  testSome () {
    expect(transpile('SELECT * FROM x WHERE a = SOME (SELECT 1)')[0]).toBe(
      'SELECT * FROM x WHERE a = ANY(SELECT 1)',
    );
  }

  testLeadingComma () {
    expect(transpile('SELECT a, b, c FROM (SELECT a, b, c FROM t)', {
      identity: true,
      leadingComma: true,
      pretty: true,
      pad: 4,
      indent: 4,
    })[0]).toBe(
      'SELECT\n    a\n    , b\n    , c\nFROM (\n    SELECT\n        a\n        , b\n        , c\n    FROM t\n)',
    );
    expect(transpile('SELECT FOO, BAR, BAZ', {
      identity: true,
      leadingComma: true,
      pretty: true,
    })[0]).toBe(
      'SELECT\n  FOO\n  , BAR\n  , BAZ',
    );
    expect(transpile('SELECT FOO, BAR, BAZ', {
      identity: true,
      leadingComma: true,
    })[0]).toBe(
      'SELECT FOO, BAR, BAZ',
    );
  }

  testSpace () {
    expect(transpile('SELECT MIN(3)>MIN(2)')[0]).toBe('SELECT MIN(3) > MIN(2)');
    expect(transpile('SELECT MIN(3)>=MIN(2)')[0]).toBe('SELECT MIN(3) >= MIN(2)');
    expect(transpile('SELECT 1>0')[0]).toBe('SELECT 1 > 0');
    expect(transpile('SELECT 3>=3')[0]).toBe('SELECT 3 >= 3');
    expect(transpile('SELECT a\r\nFROM b')[0]).toBe('SELECT a FROM b');
  }

  testComments () {
    expect(transpile('select /* asfd /* asdf */ asdf */ 1')[0]).toBe('/* asfd /* asdf */ asdf */ SELECT 1');
    expect(transpile('SELECT c /* foo */ AS alias')[0]).toBe('SELECT c AS alias /* foo */');
    expect(transpile('SELECT * FROM t1\n/*x*/\nUNION ALL SELECT * FROM t2')[0]).toBe(
      'SELECT * FROM t1 /* x */ UNION ALL SELECT * FROM t2',
    );
    expect(transpile('SELECT 1 FROM foo -- comment')[0]).toBe('SELECT 1 FROM foo /* comment */');
  }

  testTypes () {
    expect(transpile('INT 1')[0]).toBe('CAST(1 AS INT)');
    expect(transpile('VARCHAR \'x\' y')[0]).toBe('CAST(\'x\' AS VARCHAR) AS y');
    expect(transpile('x::INT')[0]).toBe('CAST(x AS INT)');
    expect(transpile('x::INT::BOOLEAN')[0]).toBe('CAST(CAST(x AS INT) AS BOOLEAN)');
    expect(() => transpile('x::z', { dialect: 'clickhouse' })).toThrow(ParseError);
  }

  testNotRange () {
    expect(transpile('a NOT LIKE b')[0]).toBe('NOT a LIKE b');
    expect(transpile('a NOT BETWEEN b AND c')[0]).toBe('NOT a BETWEEN b AND c');
    expect(transpile('a NOT IN (1, 2)')[0]).toBe('NOT a IN (1, 2)');
    expect(transpile('a IS NOT NULL')[0]).toBe('NOT a IS NULL');
  }

  testExtract () {
    expect(transpile('EXTRACT(day FROM \'2020-01-01\'::TIMESTAMP)')[0]).toBe(
      'EXTRACT(DAY FROM CAST(\'2020-01-01\' AS TIMESTAMP))',
    );
    expect(transpile('extract(week from current_date + 2)')[0]).toBe(
      'EXTRACT(WEEK FROM CURRENT_DATE + 2)',
    );
  }

  testIf () {
    expect(transpile('SELECT IF(a > 1, 1, 0) FROM foo')[0]).toBe(
      'SELECT CASE WHEN a > 1 THEN 1 ELSE 0 END FROM foo',
    );
    expect(transpile('SELECT IF a > 1 THEN b END')[0]).toBe(
      'SELECT CASE WHEN a > 1 THEN b END',
    );
    expect(transpile('SELECT IF(a > 1, 1) FROM foo')[0]).toBe(
      'SELECT CASE WHEN a > 1 THEN 1 END FROM foo',
    );
  }

  testWith () {
    expect(transpile('WITH a AS (SELECT 1) WITH b AS (SELECT 2) SELECT *')[0]).toBe(
      'WITH a AS (SELECT 1), b AS (SELECT 2) SELECT *',
    );
    expect(transpile('WITH a AS (SELECT 1), WITH b AS (SELECT 2) SELECT *')[0]).toBe(
      'WITH a AS (SELECT 1), b AS (SELECT 2) SELECT *',
    );
  }

  testAlter () {
    expect(transpile('ALTER TABLE integers ADD k INTEGER')[0]).toBe(
      'ALTER TABLE integers ADD COLUMN k INT',
    );
    expect(transpile('ALTER TABLE integers ALTER i TYPE VARCHAR')[0]).toBe(
      'ALTER TABLE integers ALTER COLUMN i SET DATA TYPE VARCHAR',
    );
  }

  testTime () {
    expect(transpile('INTERVAL \'1 day\'')[0]).toBe('INTERVAL \'1\' DAY');
    expect(transpile('TIMESTAMP \'2020-01-01\'')[0]).toBe('CAST(\'2020-01-01\' AS TIMESTAMP)');
    expect(transpile('DATE \'2020-01-01\'')[0]).toBe('CAST(\'2020-01-01\' AS DATE)');
    expect(transpile('CREATE TEMPORARY TABLE test AS SELECT 1', { dialect: 'spark2' })[0]).toBe(
      'CREATE TEMPORARY VIEW test AS SELECT 1',
    );
  }

  testIdentifyLambda () {
    expect(transpile('x(y -> y)', { identify: true })[0]).toBe(
      'X("y" -> "y")',
    );
  }

  testIdentity () {
    expect(transpile('')[0]).toBe('');
    for (const sql of loadSqlFixtures('identity.sql')) {
      if (sql.trim()) {
        expect(transpile(sql)[0]).toBe(sql.trim());
      }
    }
  }

  testNormalizeName () {
    expect(transpile('cardinality(x)', {
      read: 'presto',
      write: 'presto',
      identity: true,
      normalizeFunctions: NormalizeFunctions.LOWER,
    })[0]).toBe(
      'cardinality(x)',
    );
  }

  testPartial () {
    for (const sql of loadSqlFixtures('partial.sql')) {
      if (sql.trim()) {
        expect(transpile(sql, {
          identity: true,
          errorLevel: ErrorLevel.IGNORE,
        })[0]).toBe(sql.trim());
      }
    }
  }

  testPretty () {
    for (const [
      , sql,
      pretty,
    ] of loadSqlFixturePairs('pretty.sql')) {
      if (sql.trim()) {
        const generated = transpile(sql, {
          identity: true,
          pretty: true,
        })[0];
        expect(generated).toBe(pretty);
        expect(parseOne(sql).equals(parseOne(pretty))).toBe(true);
      }
    }
  }

  testPrettyLineBreaks () {
    expect(transpile('SELECT \'1\n2\'', {
      identity: true,
      pretty: true,
    })[0]).toBe('SELECT\n  \'1\n2\'');
  }

  testRecursion () {
    const sql = ('1 AND 2 OR 3 AND ').repeat(1000) + '4';
    expect(parseOne(sql).sql().length).toBe(17001);
  }
}

const t = new TestTranspile();

describe('TestTranspile', () => {
  test('testWeirdChars', () => t.testWeirdChars());
  test('alias', () => t.testAlias());
  test('unary', () => t.testUnary());
  test('paren', () => t.testParen());
  test('some', () => t.testSome());
  test('testLeadingComma', () => t.testLeadingComma());
  test('space', () => t.testSpace());
  test('comments', () => t.testComments());
  test('types', () => t.testTypes());
  test('testNotRange', () => t.testNotRange());
  test('extract', () => t.testExtract());
  test('if', () => t.testIf());
  test('with', () => t.testWith());
  test('alter', () => t.testAlter());
  test('time', () => t.testTime());
  test('testIdentifyLambda', () => t.testIdentifyLambda());
  test('identity', () => t.testIdentity());
  test('testNormalizeName', () => t.testNormalizeName());
  test('partial', () => t.testPartial());
  test('pretty', () => t.testPretty());
  test('testPrettyLineBreaks', () => t.testPrettyLineBreaks());

  // parseOne-based tests — these can run
  test('recursion', () => t.testRecursion());
});
