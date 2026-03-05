import {
  describe, test,
} from 'vitest';
import { Validator } from './validator';

class TestFabric extends Validator {
  override dialect = 'fabric' as const;

  testTypeMappings () {
    this.validateIdentity('CAST(x AS BOOLEAN)', 'CAST(x AS BIT)');
    this.validateIdentity('CAST(x AS DATE)', 'CAST(x AS DATE)');
    this.validateIdentity('CAST(x AS DATETIME)', 'CAST(x AS DATETIME2(6))');
    this.validateIdentity('CAST(x AS DECIMAL)', 'CAST(x AS DECIMAL)');
    this.validateIdentity('CAST(x AS DOUBLE)', 'CAST(x AS FLOAT)');
    this.validateIdentity('CAST(x AS IMAGE)', 'CAST(x AS VARBINARY)');
    this.validateIdentity('CAST(x AS INT)', 'CAST(x AS INT)');
    this.validateIdentity('CAST(x AS JSON)', 'CAST(x AS VARCHAR)');
    this.validateIdentity('CAST(x AS MONEY)', 'CAST(x AS DECIMAL)');
    this.validateIdentity('CAST(x AS NCHAR)', 'CAST(x AS CHAR)');
    this.validateIdentity('CAST(x AS NVARCHAR)', 'CAST(x AS VARCHAR)');
    this.validateIdentity('CAST(x AS ROWVERSION)', 'CAST(x AS ROWVERSION)');
    this.validateIdentity('CAST(x AS SMALLDATETIME)', 'CAST(x AS DATETIME2(6))');
    this.validateIdentity('CAST(x AS SMALLMONEY)', 'CAST(x AS DECIMAL)');
    this.validateIdentity('CAST(x AS TEXT)', 'CAST(x AS VARCHAR(MAX))');
    this.validateIdentity('CAST(x AS TIMESTAMP)', 'CAST(x AS DATETIME2(6))');
    this.validateIdentity('CAST(x AS TIMESTAMPNTZ)', 'CAST(x AS DATETIME2(6))');
    this.validateIdentity('CAST(x AS TINYINT)', 'CAST(x AS SMALLINT)');
    this.validateIdentity('CAST(x AS UTINYINT)', 'CAST(x AS SMALLINT)');
    this.validateIdentity('CAST(x AS UUID)', 'CAST(x AS UNIQUEIDENTIFIER)');
    this.validateIdentity('CAST(x AS VARIANT)', 'CAST(x AS SQL_VARIANT)');
    this.validateIdentity('CAST(x AS XML)', 'CAST(x AS VARCHAR)');
  }

  testPrecisionCapping () {
    this.validateIdentity('CAST(x AS TIME)', 'CAST(x AS TIME(6))');
    this.validateIdentity('CAST(x AS DATETIME2)', 'CAST(x AS DATETIME2(6))');
    this.validateIdentity('CAST(x AS TIME(3))', 'CAST(x AS TIME(3))');
    this.validateIdentity('CAST(x AS DATETIME2(3))', 'CAST(x AS DATETIME2(3))');
    this.validateIdentity('CAST(x AS TIME(6))', 'CAST(x AS TIME(6))');
    this.validateIdentity('CAST(x AS DATETIME2(6))', 'CAST(x AS DATETIME2(6))');
    this.validateIdentity('CAST(x AS TIME(7))', 'CAST(x AS TIME(6))');
    this.validateIdentity('CAST(x AS DATETIME2(7))', 'CAST(x AS DATETIME2(6))');
    this.validateIdentity('CAST(x AS TIME(9))', 'CAST(x AS TIME(6))');
    this.validateIdentity('CAST(x AS DATETIME2(9))', 'CAST(x AS DATETIME2(6))');
  }

  testTimestamptzWithoutAtTimeZone () {
    this.validateIdentity('CAST(x AS TIMESTAMPTZ)', 'CAST(x AS DATETIME2(6))');
    this.validateIdentity('CAST(x AS TIMESTAMPTZ(3))', 'CAST(x AS DATETIME2(3))');
    this.validateIdentity('CAST(x AS TIMESTAMPTZ(6))', 'CAST(x AS DATETIME2(6))');
    this.validateIdentity('CAST(x AS TIMESTAMPTZ(9))', 'CAST(x AS DATETIME2(6))');
  }

  testTimestamptzWithAtTimeZone () {
    this.validateIdentity(
      'CAST(x AS TIMESTAMPTZ) AT TIME ZONE \'Pacific Standard Time\'',
      'CAST(CAST(x AS DATETIMEOFFSET(6)) AT TIME ZONE \'Pacific Standard Time\' AS DATETIME2(6))',
    );
    this.validateIdentity(
      'CAST(x AS TIMESTAMPTZ(3)) AT TIME ZONE \'Pacific Standard Time\'',
      'CAST(CAST(x AS DATETIMEOFFSET(3)) AT TIME ZONE \'Pacific Standard Time\' AS DATETIME2(3))',
    );
    this.validateIdentity(
      'CAST(x AS TIMESTAMPTZ(6)) AT TIME ZONE \'Pacific Standard Time\'',
      'CAST(CAST(x AS DATETIMEOFFSET(6)) AT TIME ZONE \'Pacific Standard Time\' AS DATETIME2(6))',
    );
    this.validateIdentity(
      'CAST(x AS TIMESTAMPTZ(9)) AT TIME ZONE \'Pacific Standard Time\'',
      'CAST(CAST(x AS DATETIMEOFFSET(6)) AT TIME ZONE \'Pacific Standard Time\' AS DATETIME2(6))',
    );
  }

  testUnixToTime () {
    this.validateIdentity(
      'UNIX_TO_TIME(column)',
      'DATEADD(MICROSECONDS, CAST(ROUND(column * 1e6, 0) AS BIGINT), CAST(\'1970-01-01\' AS DATETIME2(6)))',
    );
  }

  testVarcharPrecisionInference () {
    this.validateIdentity('CREATE TABLE t (col VARCHAR)', 'CREATE TABLE t (col VARCHAR(1))');
    this.validateIdentity('CREATE TABLE t (col VARCHAR(50))');
    this.validateIdentity('CREATE TABLE t (col CHAR)', 'CREATE TABLE t (col CHAR(1))');
    this.validateIdentity('CREATE TABLE t (col CHAR(10))');
    this.validateAll('CREATE TABLE t (col VARCHAR(MAX))', {
      read: {
        postgres: 'CREATE TABLE t (col VARCHAR)',
        tsql: 'CREATE TABLE t (col VARCHAR(MAX))',
      },
    });
    this.validateAll('CREATE TABLE t (col CHAR(MAX))', {
      read: {
        postgres: 'CREATE TABLE t (col CHAR)',
        tsql: 'CREATE TABLE t (col CHAR(MAX))',
      },
    });
  }
}

const t = new TestFabric();
describe('TestFabric', () => {
  test('testTypeMappings', () => t.testTypeMappings());
  test('testPrecisionCapping', () => t.testPrecisionCapping());
  test('testTimestamptzWithoutAtTimeZone', () => t.testTimestamptzWithoutAtTimeZone());
  test('testTimestamptzWithAtTimeZone', () => t.testTimestamptzWithAtTimeZone());
  test('testUnixToTime', () => t.testUnixToTime());
  test('testVarcharPrecisionInference', () => t.testVarcharPrecisionInference());
});
