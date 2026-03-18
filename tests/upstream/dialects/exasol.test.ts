import {
  describe, test,
} from 'vitest';
import {
  TruncExpr, AnonymousExpr, DateTruncExpr,
} from '../../../src/expressions';
import { Validator } from './validator';

class TestExasol extends Validator {
  override dialect = 'exasol' as const;

  testExasol () {
    this.validateIdentity(
      'SELECT 1 AS [x]',
      'SELECT 1 AS "x"',
    );
    this.validateIdentity('SYSTIMESTAMP', 'SYSTIMESTAMP()');
    this.validateIdentity('SELECT SYSTIMESTAMP()');
    this.validateIdentity('SELECT SYSTIMESTAMP(6)');
  }

  testQualifyUnscopedStar () {
    this.validateAll(
      'SELECT TEST.*, 1 FROM TEST',
      {
        read: {
          '': 'SELECT *, 1 FROM TEST',
        },
      },
    );
    this.validateIdentity('SELECT t.*, 1 FROM t');
    this.validateIdentity('SELECT t.* FROM t');
    this.validateIdentity('SELECT * FROM t');
    this.validateIdentity('WITH t AS (SELECT 1 AS x) SELECT t.*, 3 FROM t');
    this.validateAll(
      'WITH t1 AS (SELECT 1 AS c1), t2 AS (SELECT 2 AS c2) SELECT t1.*, t2.*, 3 FROM t1, t2',
      {
        read: {
          '': 'WITH t1 AS (SELECT 1 AS c1), t2 AS (SELECT 2 AS c2) SELECT *, 3 FROM t1, t2',
        },
      },
    );
    this.validateAll(
      'SELECT "A".*, "B".*, 3 FROM "A" JOIN "B" ON 1 = 1',
      {
        read: {
          '': 'SELECT *, 3 FROM "A" JOIN "B" ON 1=1',
        },
      },
    );
    this.validateAll(
      'SELECT s.*, q.*, 7 FROM (SELECT 1 AS x) AS s CROSS JOIN (SELECT 2 AS y) AS q',
      {
        read: {
          '': 'SELECT *, 7 FROM (SELECT 1 AS x) s CROSS JOIN (SELECT 2 AS y) q',
        },
      },
    );
  }

  testTypeMappings () {
    this.validateIdentity('CAST(x AS BLOB)', 'CAST(x AS VARCHAR)');
    this.validateIdentity('CAST(x AS LONGBLOB)', 'CAST(x AS VARCHAR)');
    this.validateIdentity('CAST(x AS LONGTEXT)', 'CAST(x AS VARCHAR)');
    this.validateIdentity('CAST(x AS MEDIUMBLOB)', 'CAST(x AS VARCHAR)');
    this.validateIdentity('CAST(x AS MEDIUMTEXT)', 'CAST(x AS VARCHAR)');
    this.validateIdentity('CAST(x AS TINYBLOB)', 'CAST(x AS VARCHAR)');
    this.validateIdentity('CAST(x AS TINYTEXT)', 'CAST(x AS VARCHAR)');
    this.validateIdentity('CAST(x AS TEXT)', 'CAST(x AS LONG VARCHAR)');
    this.validateIdentity(
      'SELECT CAST((CAST(202305 AS INT) - 100) AS LONG VARCHAR) AS CAL_YEAR_WEEK_ADJUSTED',
    );
    this.validateIdentity('CAST(x AS VARBINARY)', 'CAST(x AS VARCHAR)');
    this.validateIdentity('CAST(x AS VARCHAR)', 'CAST(x AS VARCHAR)');
    this.validateIdentity('CAST(x AS CHAR)', 'CAST(x AS CHAR)');
    this.validateIdentity('CAST(x AS TINYINT)', 'CAST(x AS SMALLINT)');
    this.validateIdentity('CAST(x AS SMALLINT)');
    this.validateIdentity('CAST(x AS INT)');
    this.validateIdentity('CAST(x AS MEDIUMINT)', 'CAST(x AS INT)');
    this.validateIdentity('CAST(x AS BIGINT)');
    this.validateIdentity('CAST(x AS FLOAT)');
    this.validateIdentity('CAST(x AS DOUBLE)');
    this.validateIdentity('CAST(x AS DECIMAL32)', 'CAST(x AS DECIMAL)');
    this.validateIdentity('CAST(x AS DECIMAL64)', 'CAST(x AS DECIMAL)');
    this.validateIdentity('CAST(x AS DECIMAL128)', 'CAST(x AS DECIMAL)');
    this.validateIdentity('CAST(x AS DECIMAL256)', 'CAST(x AS DECIMAL)');
    this.validateIdentity('CAST(x AS DATE)');
    this.validateIdentity('CAST(x AS DATETIME)', 'CAST(x AS TIMESTAMP)');
    this.validateIdentity('CAST(x AS TIMESTAMP)');
    this.validateAll(
      'CAST(x AS TIMESTAMP)',
      {
        read: { tsql: 'CAST(x AS DATETIME2)' },
        write: { exasol: 'CAST(x AS TIMESTAMP)' },
      },
    );
    this.validateAll(
      'CAST(x AS TIMESTAMP)',
      {
        read: { tsql: 'CAST(x AS SMALLDATETIME)' },
        write: { exasol: 'CAST(x AS TIMESTAMP)' },
      },
    );
    this.validateIdentity('CAST(x AS BOOLEAN)');
    this.validateIdentity(
      'CAST(x AS TIMESTAMPLTZ)',
      'CAST(x AS TIMESTAMP WITH LOCAL TIME ZONE)',
    );
    this.validateIdentity(
      'CAST(x AS TIMESTAMP(3) WITH LOCAL TIME ZONE)',
      'CAST(x AS TIMESTAMP WITH LOCAL TIME ZONE)',
    );
  }

  testMod () {
    this.validateAll(
      'SELECT MOD(x, 10)',
      {
        read: { exasol: 'SELECT MOD(x, 10)' },
        write: {
          teradata: 'SELECT x MOD 10',
          mysql: 'SELECT x % 10',
          exasol: 'SELECT MOD(x, 10)',
        },
      },
    );
  }

  testBits () {
    this.validateAll(
      'SELECT BIT_AND(x, 1)',
      {
        read: {
          exasol: 'SELECT BIT_AND(x, 1)',
          duckdb: 'SELECT x & 1',
          presto: 'SELECT BITWISE_AND(x, 1)',
          spark: 'SELECT x & 1',
        },
        write: {
          exasol: 'SELECT BIT_AND(x, 1)',
          duckdb: 'SELECT x & 1',
          hive: 'SELECT x & 1',
          presto: 'SELECT BITWISE_AND(x, 1)',
          spark: 'SELECT x & 1',
        },
      },
    );
    this.validateAll(
      'SELECT BIT_OR(x, 1)',
      {
        read: {
          exasol: 'SELECT BIT_OR(x, 1)',
          duckdb: 'SELECT x | 1',
          presto: 'SELECT BITWISE_OR(x, 1)',
          spark: 'SELECT x | 1',
        },
        write: {
          exasol: 'SELECT BIT_OR(x, 1)',
          duckdb: 'SELECT x | 1',
          hive: 'SELECT x | 1',
          presto: 'SELECT BITWISE_OR(x, 1)',
          spark: 'SELECT x | 1',
        },
      },
    );
    this.validateAll(
      'SELECT BIT_XOR(x, 1)',
      {
        read: {
          '': 'SELECT x ^ 1',
          'exasol': 'SELECT BIT_XOR(x, 1)',
          'bigquery': 'SELECT x ^ 1',
          'presto': 'SELECT BITWISE_XOR(x, 1)',
          'postgres': 'SELECT x # 1',
        },
        write: {
          '': 'SELECT x ^ 1',
          'exasol': 'SELECT BIT_XOR(x, 1)',
          'bigquery': 'SELECT x ^ 1',
          'duckdb': 'SELECT XOR(x, 1)',
          'presto': 'SELECT BITWISE_XOR(x, 1)',
          'postgres': 'SELECT x # 1',
        },
      },
    );
    this.validateAll(
      'SELECT BIT_NOT(x)',
      {
        read: {
          exasol: 'SELECT BIT_NOT(x)',
          duckdb: 'SELECT ~x',
          presto: 'SELECT BITWISE_NOT(x)',
          spark: 'SELECT ~x',
        },
        write: {
          exasol: 'SELECT BIT_NOT(x)',
          duckdb: 'SELECT ~x',
          hive: 'SELECT ~x',
          presto: 'SELECT BITWISE_NOT(x)',
          spark: 'SELECT ~x',
        },
      },
    );
    this.validateAll(
      'SELECT BIT_LSHIFT(x, 1)',
      {
        read: {
          exasol: 'SELECT BIT_LSHIFT(x, 1)',
          spark: 'SELECT SHIFTLEFT(x, 1)',
          duckdb: 'SELECT x << 1',
          hive: 'SELECT x << 1',
        },
        write: {
          exasol: 'SELECT BIT_LSHIFT(x, 1)',
          duckdb: 'SELECT x << 1',
          presto: 'SELECT BITWISE_ARITHMETIC_SHIFT_LEFT(x, 1)',
          hive: 'SELECT x << 1',
          spark: 'SELECT SHIFTLEFT(x, 1)',
        },
      },
    );
    this.validateAll(
      'SELECT BIT_RSHIFT(x, 1)',
      {
        read: {
          exasol: 'SELECT BIT_RSHIFT(x, 1)',
          spark: 'SELECT SHIFTRIGHT(x, 1)',
          duckdb: 'SELECT x >> 1',
          hive: 'SELECT x >> 1',
        },
        write: {
          exasol: 'SELECT BIT_RSHIFT(x, 1)',
          duckdb: 'SELECT x >> 1',
          presto: 'SELECT BITWISE_ARITHMETIC_SHIFT_RIGHT(x, 1)',
          hive: 'SELECT x >> 1',
          spark: 'SELECT SHIFTRIGHT(x, 1)',
        },
      },
    );
  }

  testAggregateFunctions () {
    this.validateAll(
      'SELECT department, EVERY(age >= 30) AS EVERY FROM employee_table GROUP BY department',
      {
        read: {
          exasol: 'SELECT department, EVERY(age >= 30) AS EVERY FROM employee_table GROUP BY department',
        },
        write: {
          exasol: 'SELECT department, EVERY(age >= 30) AS EVERY FROM employee_table GROUP BY department',
          duckdb: 'SELECT department, ALL (age >= 30) AS EVERY FROM employee_table GROUP BY department',
        },
      },
    );
    this.validateAll(
      'SELECT VAR_POP(current_salary)',
      {
        write: {
          exasol: 'SELECT VAR_POP(current_salary)',
          duckdb: 'SELECT VAR_POP(current_salary)',
          presto: 'SELECT VAR_POP(current_salary)',
        },
        read: {
          exasol: 'SELECT VAR_POP(current_salary)',
          duckdb: 'SELECT VAR_POP(current_salary)',
          presto: 'SELECT VAR_POP(current_salary)',
        },
      },
    );
    this.validateAll(
      'SELECT APPROXIMATE_COUNT_DISTINCT(y)',
      {
        read: {
          spark: 'SELECT APPROX_COUNT_DISTINCT(y)',
          exasol: 'SELECT APPROXIMATE_COUNT_DISTINCT(y)',
        },
        write: {
          redshift: 'SELECT APPROXIMATE COUNT(DISTINCT y)',
          spark: 'SELECT APPROX_COUNT_DISTINCT(y)',
          exasol: 'SELECT APPROXIMATE_COUNT_DISTINCT(y)',
        },
      },
    );
    this.validateAll(
      'SELECT a, b, rank(b) OVER (ORDER BY b) FROM (VALUES (\'A1\', 2), (\'A1\', 1), (\'A2\', 3), (\'A1\', 1)) AS tab(a, b)',
      {
        write: {
          exasol: 'SELECT a, b, RANK() OVER (ORDER BY b) FROM (VALUES (\'A1\', 2), (\'A1\', 1), (\'A2\', 3), (\'A1\', 1)) AS tab(a, b)',
          databricks: 'SELECT a, b, RANK(b) OVER (ORDER BY b NULLS LAST) FROM VALUES (\'A1\', 2), (\'A1\', 1), (\'A2\', 3), (\'A1\', 1) AS tab(a, b)',
          spark: 'SELECT a, b, RANK(b) OVER (ORDER BY b NULLS LAST) FROM VALUES (\'A1\', 2), (\'A1\', 1), (\'A2\', 3), (\'A1\', 1) AS tab(a, b)',
        },
      },
    );
  }

  testStringFunctions () {
    this.validateIdentity(
      'TO_CHAR(CAST(TO_DATE(date, \'YYYYMMDD\') AS TIMESTAMP), \'DY\') AS day_of_week',
    );
    this.validateIdentity('SELECT TO_CHAR(12345.67890, \'9999999.999999999\') AS TO_CHAR');
    this.validateIdentity(
      'SELECT TO_CHAR(DATE \'1999-12-31\') AS TO_CHAR',
      'SELECT TO_CHAR(CAST(\'1999-12-31\' AS DATE)) AS TO_CHAR',
    );
    this.validateIdentity(
      'SELECT TO_CHAR(TIMESTAMP \'1999-12-31 23:59:00\', \'HH24:MI:SS DD-MM-YYYY\') AS TO_CHAR',
      'SELECT TO_CHAR(CAST(\'1999-12-31 23:59:00\' AS TIMESTAMP), \'HH24:MI:SS DD-MM-YYYY\') AS TO_CHAR',
    );
    this.validateIdentity('SELECT TO_CHAR(12345.6789) AS TO_CHAR');
    this.validateIdentity('SELECT TO_CHAR(-12345.67890, \'000G000G000D000000MI\') AS TO_CHAR');

    this.validateIdentity(
      'SELECT id, department, hire_date, GROUP_CONCAT(id ORDER BY hire_date SEPARATOR \',\') OVER (PARTITION BY department rows between 1 preceding and 1 following) GROUP_CONCAT_RESULT from employee_table ORDER BY department, hire_date',
      'SELECT id, department, hire_date, LISTAGG(id, \',\') WITHIN GROUP (ORDER BY hire_date) OVER (PARTITION BY department rows BETWEEN 1 preceding AND 1 following) AS GROUP_CONCAT_RESULT FROM employee_table ORDER BY department, hire_date',
    );
    this.validateAll(
      'GROUP_CONCAT(DISTINCT x ORDER BY y DESC)',
      {
        write: {
          exasol: 'LISTAGG(DISTINCT x, \',\') WITHIN GROUP (ORDER BY y DESC)',
          mysql: 'GROUP_CONCAT(DISTINCT x ORDER BY y DESC SEPARATOR \',\')',
          tsql: 'STRING_AGG(x, \',\') WITHIN GROUP (ORDER BY y DESC)',
          databricks: 'LISTAGG(DISTINCT x, \',\') WITHIN GROUP (ORDER BY y DESC)',
        },
      },
    );
    this.validateAll(
      'EDIT_DISTANCE(col1, col2)',
      {
        read: {
          exasol: 'EDIT_DISTANCE(col1, col2)',
          bigquery: 'EDIT_DISTANCE(col1, col2)',
          clickhouse: 'editDistance(col1, col2)',
          drill: 'LEVENSHTEIN_DISTANCE(col1, col2)',
          duckdb: 'LEVENSHTEIN(col1, col2)',
          hive: 'LEVENSHTEIN(col1, col2)',
        },
        write: {
          exasol: 'EDIT_DISTANCE(col1, col2)',
          bigquery: 'EDIT_DISTANCE(col1, col2)',
          clickhouse: 'editDistance(col1, col2)',
          drill: 'LEVENSHTEIN_DISTANCE(col1, col2)',
          duckdb: 'LEVENSHTEIN(col1, col2)',
          hive: 'LEVENSHTEIN(col1, col2)',
        },
      },
    );
    this.validateAll(
      'REGEXP_REPLACE(subject, pattern, replacement, position, occurrence)',
      {
        write: {
          bigquery: 'REGEXP_REPLACE(subject, pattern, replacement)',
          exasol: 'REGEXP_REPLACE(subject, pattern, replacement, position, occurrence)',
          duckdb: 'REGEXP_REPLACE(subject, pattern, replacement)',
          hive: 'REGEXP_REPLACE(subject, pattern, replacement)',
          snowflake: 'REGEXP_REPLACE(subject, pattern, replacement, position, occurrence)',
          spark: 'REGEXP_REPLACE(subject, pattern, replacement, position)',
        },
        read: {
          exasol: 'REGEXP_REPLACE(subject, pattern, replacement, position, occurrence)',
          snowflake: 'REGEXP_REPLACE(subject, pattern, replacement, position, occurrence)',
          spark: 'REGEXP_REPLACE(subject, pattern, replacement, position, occurrence)',
        },
      },
    );
    this.validateAll(
      'SELECT TO_CHAR(CAST(\'1999-12-31\' AS DATE)) AS TO_CHAR',
      {
        write: {
          exasol: 'SELECT TO_CHAR(CAST(\'1999-12-31\' AS DATE)) AS TO_CHAR',
          presto: 'SELECT DATE_FORMAT(CAST(\'1999-12-31\' AS DATE)) AS TO_CHAR',
          oracle: 'SELECT TO_CHAR(CAST(\'1999-12-31\' AS DATE)) AS TO_CHAR',
          redshift: 'SELECT CAST(CAST(\'1999-12-31\' AS DATE) AS VARCHAR(MAX)) AS TO_CHAR',
          postgres: 'SELECT CAST(CAST(\'1999-12-31\' AS DATE) AS TEXT) AS TO_CHAR',
        },
        read: {
          exasol: 'SELECT TO_CHAR(DATE \'1999-12-31\') AS TO_CHAR',
        },
      },
    );
    this.validateAll(
      'STRPOS(haystack, needle)',
      {
        write: {
          exasol: 'INSTR(haystack, needle)',
          bigquery: 'INSTR(haystack, needle)',
          databricks: 'LOCATE(needle, haystack)',
          oracle: 'INSTR(haystack, needle)',
          presto: 'STRPOS(haystack, needle)',
        },
      },
    );
    this.validateAll(
      String.raw`SELECT REGEXP_SUBSTR('My mail address is my_mail@yahoo.com', '(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}') AS EMAIL`,
      {
        write: {
          exasol: String.raw`SELECT REGEXP_SUBSTR('My mail address is my_mail@yahoo.com', '(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}') AS EMAIL`,
          bigquery: String.raw`SELECT REGEXP_EXTRACT('My mail address is my_mail@yahoo.com', '(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,4}') AS EMAIL`,
          snowflake: String.raw`SELECT REGEXP_SUBSTR('My mail address is my_mail@yahoo.com', '(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,4}') AS EMAIL`,
          presto: String.raw`SELECT REGEXP_EXTRACT('My mail address is my_mail@yahoo.com', '(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}') AS EMAIL`,
        },
      },
    );
    this.validateAll(
      'SELECT SUBSTR(\'www.apache.org\', 1, NVL(NULLIF(INSTR(\'www.apache.org\', \'.\', 1, 2), 0) - 1, LENGTH(\'www.apache.org\')))',
      {
        read: {
          databricks: 'SELECT substring_index(\'www.apache.org\', \'.\', 2)',
        },
      },
    );
    this.validateAll(
      'SELECT SUBSTR(\'555A66A777\', 1, NVL(NULLIF(INSTR(\'555A66A777\', \'a\', 1, 2), 0) - 1, LENGTH(\'555A66A777\')))',
      {
        read: {
          databricks: 'SELECT substring_index(\'555A66A777\' COLLATE UTF8_BINARY, \'a\', 2)',
        },
      },
    );
    this.validateAll(
      'SELECT SUBSTR(\'555A66A777\', 1, NVL(NULLIF(INSTR(LOWER(\'555A66A777\'), \'a\', 1, 2), 0) - 1, LENGTH(\'555A66A777\')))',
      {
        read: {
          databricks: 'SELECT substring_index(\'555A66A777\' COLLATE UTF8_LCASE, \'a\', 2)',
        },
      },
    );
    this.validateAll(
      'SELECT SUBSTR(\'A|a|A\', 1, NVL(NULLIF(INSTR(LOWER(\'A|a|A\'), LOWER(\'A\'), 1, 2), 0) - 1, LENGTH(\'A|a|A\')))',
      {
        read: {
          databricks: 'SELECT substring_index(\'A|a|A\' COLLATE UTF8_LCASE, \'A\' COLLATE UTF8_LCASE, 2)',
        },
      },
    );
  }

  testDatetimeFunctions () {
    const formats: Record<string, string> = {
      HH12: 'hour_12',
      HH24: 'hour_24',
      ID: 'iso_weekday',
      IW: 'iso_week_number',
      uW: 'week_number_uW',
      VW: 'week_number_VW',
      IYYY: 'iso_year',
      MI: 'minutes',
      SS: 'seconds',
      DAY: 'day_full',
      DY: 'day_abbr',
    };

    this.validateIdentity(
      'SELECT TO_DATE(\'31-12-1999\', \'dd-mm-yyyy\') AS TO_DATE',
      'SELECT TO_DATE(\'31-12-1999\', \'DD-MM-YYYY\') AS TO_DATE',
    );
    this.validateIdentity(
      'SELECT TO_DATE(\'31-12-1999\', \'dd-mm-YY\') AS TO_DATE',
      'SELECT TO_DATE(\'31-12-1999\', \'DD-MM-YY\') AS TO_DATE',
    );
    this.validateIdentity('SELECT TO_DATE(\'31-DECEMBER-1999\', \'DD-MONTH-YYYY\') AS TO_DATE');
    this.validateIdentity('SELECT TO_DATE(\'31-DEC-1999\', \'DD-MON-YYYY\') AS TO_DATE');
    this.validateIdentity('SELECT WEEKOFYEAR(\'2024-05-22\')', 'SELECT WEEK(\'2024-05-22\')');

    for (const [fmt, alias] of Object.entries(formats)) {
      this.validateIdentity(
        `SELECT TO_CHAR(CAST('2024-07-08 13:45:00' AS TIMESTAMP), '${fmt}') AS ${alias}`,
      );
    }

    this.validateAll(
      'SELECT TO_CHAR(CAST(\'2024-07-08 13:45:00\' AS TIMESTAMP), \'DY\')',
      {
        write: {
          exasol: 'SELECT TO_CHAR(CAST(\'2024-07-08 13:45:00\' AS TIMESTAMP), \'DY\')',
          oracle: 'SELECT TO_CHAR(CAST(\'2024-07-08 13:45:00\' AS TIMESTAMP), \'DY\')',
          postgres: 'SELECT TO_CHAR(CAST(\'2024-07-08 13:45:00\' AS TIMESTAMP), \'TMDy\')',
          databricks: 'SELECT DATE_FORMAT(CAST(\'2024-07-08 13:45:00\' AS TIMESTAMP), \'EEE\')',
        },
      },
    );

    this.validateAll(
      'TO_DATE(x, \'YYYY-MM-DD\')',
      {
        write: {
          exasol: 'TO_DATE(x, \'YYYY-MM-DD\')',
          duckdb: 'CAST(x AS DATE)',
          hive: 'TO_DATE(x)',
          presto: 'CAST(CAST(x AS TIMESTAMP) AS DATE)',
          spark: 'TO_DATE(x)',
          snowflake: 'TO_DATE(x, \'yyyy-mm-DD\')',
          databricks: 'TO_DATE(x)',
        },
      },
    );
    this.validateAll(
      'TO_DATE(x, \'YYYY\')',
      {
        write: {
          exasol: 'TO_DATE(x, \'YYYY\')',
          duckdb: 'CAST(STRPTIME(x, \'%Y\') AS DATE)',
          hive: 'TO_DATE(x, \'yyyy\')',
          presto: 'CAST(DATE_PARSE(x, \'%Y\') AS DATE)',
          spark: 'TO_DATE(x, \'yyyy\')',
          snowflake: 'TO_DATE(x, \'yyyy\')',
          databricks: 'TO_DATE(x, \'yyyy\')',
        },
      },
    );
    this.validateIdentity(
      'SELECT CONVERT_TZ(CAST(\'2012-03-25 02:30:00\' AS TIMESTAMP), \'Europe/Berlin\', \'UTC\', \'INVALID REJECT AMBIGUOUS REJECT\') AS CONVERT_TZ',
    );
    this.validateAll(
      'SELECT CONVERT_TZ(\'2012-05-10 12:00:00\', \'Europe/Berlin\', \'America/New_York\')',
      {
        read: {
          exasol: 'SELECT CONVERT_TZ(\'2012-05-10 12:00:00\', \'Europe/Berlin\', \'America/New_York\')',
          mysql: 'SELECT CONVERT_TZ(\'2012-05-10 12:00:00\', \'Europe/Berlin\', \'America/New_York\')',
          databricks: 'SELECT CONVERT_TIMEZONE(\'Europe/Berlin\', \'America/New_York\', \'2012-05-10 12:00:00\')',
        },
        write: {
          exasol: 'SELECT CONVERT_TZ(\'2012-05-10 12:00:00\', \'Europe/Berlin\', \'America/New_York\')',
          mysql: 'SELECT CONVERT_TZ(\'2012-05-10 12:00:00\', \'Europe/Berlin\', \'America/New_York\')',
          databricks: 'SELECT CONVERT_TIMEZONE(\'Europe/Berlin\', \'America/New_York\', \'2012-05-10 12:00:00\')',
          snowflake: 'SELECT CONVERT_TIMEZONE(\'Europe/Berlin\', \'America/New_York\', \'2012-05-10 12:00:00\')',
          spark: 'SELECT CONVERT_TIMEZONE(\'Europe/Berlin\', \'America/New_York\', \'2012-05-10 12:00:00\')',
          redshift: 'SELECT CONVERT_TIMEZONE(\'Europe/Berlin\', \'America/New_York\', \'2012-05-10 12:00:00\')',
          duckdb: 'SELECT CAST(\'2012-05-10 12:00:00\' AS TIMESTAMP) AT TIME ZONE \'Europe/Berlin\' AT TIME ZONE \'America/New_York\'',
        },
      },
    );
    this.validateIdentity(
      'TIME_TO_STR(b, \'%Y-%m-%d %H:%M:%S\')',
      'TO_CHAR(b, \'YYYY-MM-DD HH:MI:SS\')',
    );
    this.validateIdentity(
      'SELECT TIME_TO_STR(CAST(STR_TO_TIME(date, \'%Y%m%d\') AS DATE), \'%a\') AS day_of_week',
      'SELECT TO_CHAR(CAST(TO_DATE(date, \'YYYYMMDD\') AS DATE), \'DY\') AS day_of_week',
    );
    this.validateIdentity(
      'SELECT CAST(CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) AT TIME ZONE \'CET\' AS DATE) - 1',
      'SELECT CAST(CONVERT_TZ(CAST(CURRENT_TIMESTAMP() AS TIMESTAMP), \'UTC\', \'CET\') AS DATE) - 1',
    );

    const units = [
      'MM',
      'QUARTER',
      'WEEK',
      'MINUTE',
      'YEAR',
    ];
    for (const unit of units) {
      this.validateAll(
        `SELECT TRUNC(CAST('2006-12-31' AS DATE), '${unit}') AS TRUNC`,
        {
          write: {
            exasol: `SELECT DATE_TRUNC('${unit}', DATE '2006-12-31') AS TRUNC`,
            presto: `SELECT DATE_TRUNC('${unit}', CAST('2006-12-31' AS DATE)) AS TRUNC`,
            databricks: `SELECT TRUNC(CAST('2006-12-31' AS DATE), '${unit}') AS TRUNC`,
          },
        },
      );

      this.validateAll(
        `SELECT DATE_TRUNC('${unit}', TIMESTAMP '2006-12-31T23:59:59') DATE_TRUNC`,
        {
          write: {
            exasol: `SELECT DATE_TRUNC('${unit}', TIMESTAMP '2006-12-31 23:59:59') AS DATE_TRUNC`,
            presto: `SELECT DATE_TRUNC('${unit}', CAST('2006-12-31T23:59:59' AS TIMESTAMP)) AS DATE_TRUNC`,
            databricks: `SELECT DATE_TRUNC('${unit}', CAST('2006-12-31T23:59:59' AS TIMESTAMP)) AS DATE_TRUNC`,
          },
        },
      );
      this.validateAll(
        `SELECT DATE_TRUNC('${unit}', CURRENT_TIMESTAMP) DATE_TRUNC`,
        {
          write: {
            exasol: `SELECT DATE_TRUNC('${unit}', CURRENT_TIMESTAMP()) AS DATE_TRUNC`,
            presto: `SELECT DATE_TRUNC('${unit}', CURRENT_TIMESTAMP) AS DATE_TRUNC`,
            databricks: `SELECT DATE_TRUNC('${unit}', CURRENT_TIMESTAMP()) AS DATE_TRUNC`,
          },
        },
      );
    }

    // DATE_UNITS from exasol dialect
    const DATE_UNITS = [
      'DAY',
      'MONTH',
      'YEAR',
      'HOUR',
      'MINUTE',
      'SECOND',
      'WEEK',
    ];
    for (const unit of DATE_UNITS) {
      this.validateAll(
        `SELECT ADD_${unit}S(DATE '2000-02-28', 1)`,
        {
          write: {
            exasol: `SELECT ADD_${unit}S(CAST('2000-02-28' AS DATE), 1)`,
            bigquery: `SELECT DATE_ADD(CAST('2000-02-28' AS DATE), INTERVAL 1 ${unit})`,
            duckdb: `SELECT CAST('2000-02-28' AS DATE) + INTERVAL 1 ${unit}`,
            presto: `SELECT DATE_ADD('${unit}', 1, CAST('2000-02-28' AS DATE))`,
            redshift: `SELECT DATEADD(${unit}, 1, CAST('2000-02-28' AS DATE))`,
            snowflake: `SELECT DATEADD(${unit}, 1, CAST('2000-02-28' AS DATE))`,
            tsql: `SELECT DATEADD(${unit}, 1, CAST('2000-02-28' AS DATE))`,
          },
        },
      );

      this.validateAll(
        `SELECT ADD_${unit}S('2000-02-28', -'1')`,
        {
          read: {
            sqlite: `SELECT DATE_SUB('2000-02-28', INTERVAL 1 ${unit})`,
            bigquery: `SELECT DATE_SUB('2000-02-28', INTERVAL 1 ${unit})`,
            presto: `SELECT DATE_SUB('2000-02-28', INTERVAL 1 ${unit})`,
            redshift: `SELECT DATE_SUB('2000-02-28', INTERVAL 1 ${unit})`,
            snowflake: `SELECT DATE_SUB('2000-02-28', INTERVAL 1 ${unit})`,
            tsql: `SELECT DATE_SUB('2000-02-28', INTERVAL 1 ${unit})`,
          },
        },
      );

      this.validateAll(
        'SELECT CAST(ADD_DAYS(ADD_MONTHS(DATE_TRUNC(\'MONTH\', DATE \'2008-11-25\'), 1), -1) AS DATE)',
        {
          read: {
            snowflake: 'SELECT LAST_DAY(CAST(\'2008-11-25\' AS DATE), MONTH)',
            databricks: 'SELECT LAST_DAY(\'2008-11-25\')',
            spark: 'SELECT LAST_DAY(CAST(\'2008-11-25\' AS DATE))',
            presto: 'SELECT LAST_DAY_OF_MONTH(CAST(\'2008-11-25\' AS DATE))',
          },
        },
      );

      this.validateAll(
        `SELECT ${unit}S_BETWEEN(TIMESTAMP '2000-02-28 00:00:00', CURRENT_TIMESTAMP)`,
        {
          write: {
            exasol: `SELECT ${unit}S_BETWEEN(CAST('2000-02-28 00:00:00' AS TIMESTAMP), CURRENT_TIMESTAMP())`,
            bigquery: `SELECT DATE_DIFF(CAST('2000-02-28 00:00:00' AS DATETIME), CURRENT_TIMESTAMP(), ${unit})`,
            duckdb: `SELECT DATE_DIFF('${unit}', CURRENT_TIMESTAMP, CAST('2000-02-28 00:00:00' AS TIMESTAMP))`,
            presto: `SELECT DATE_DIFF('${unit}', CURRENT_TIMESTAMP, CAST('2000-02-28 00:00:00' AS TIMESTAMP))`,
            redshift: `SELECT DATEDIFF(${unit}, GETDATE(), CAST('2000-02-28 00:00:00' AS TIMESTAMP))`,
            snowflake: `SELECT DATEDIFF(${unit}, CURRENT_TIMESTAMP(), CAST('2000-02-28 00:00:00' AS TIMESTAMP))`,
            tsql: `SELECT DATEDIFF(${unit}, GETDATE(), CAST('2000-02-28 00:00:00' AS DATETIME2))`,
          },
        },
      );
    }

    this.validateAll(
      'SELECT quarter(\'2016-08-31\')',
      {
        write: {
          exasol: 'SELECT CEIL(MONTH(TO_DATE(\'2016-08-31\'))/3)',
          databricks: 'SELECT QUARTER(\'2016-08-31\')',
        },
      },
    );
  }

  testNumberFunctions () {
    this.validateIdentity('SELECT TRUNC(123.456, 2) AS TRUNC');
    this.validateIdentity('SELECT DIV(1234, 2) AS DIV');

    this.validateIdentity('TRUNC(123.456, 2)').assertIs(TruncExpr);
    this.validateIdentity('TRUNC(3.14159)').assertIs(TruncExpr);

    this.parseOne('TRUNC(CAST(x AS DATE), \'MONTH\')').assertIs(DateTruncExpr);
    this.parseOne('TRUNC(CAST(x AS TIMESTAMP), \'MONTH\')').assertIs(DateTruncExpr);
    this.parseOne('TRUNC(CAST(x AS DATETIME), \'MONTH\')').assertIs(DateTruncExpr);

    this.validateIdentity('TRUNC(CAST(x AS DATE))').assertIs(AnonymousExpr);

    this.validateAll(
      'TRUNC(price, 2)',
      {
        write: {
          exasol: 'TRUNC(price, 2)',
          oracle: 'TRUNC(price, 2)',
          postgres: 'TRUNC(price, 2)',
          mysql: 'TRUNCATE(price, 2)',
          tsql: 'ROUND(price, 2, 1)',
        },
      },
    );

    for (const unit of [
      'YYYY',
      'MM',
      'DD',
      'HH',
      'MI',
      'SS',
      'WW',
    ]) {
      this.validateAll(
        `TRUNC(CAST(x AS TIMESTAMP), '${unit}')`,
        {
          write: {
            exasol: `DATE_TRUNC('${unit}', x)`,
            oracle: `TRUNC(CAST(x AS TIMESTAMP), '${unit}')`,
          },
        },
      );
    }

    this.validateAll(
      'TRUNC(CAST(x AS TIMESTAMP), \'Q\')',
      {
        write: {
          exasol: 'DATE_TRUNC(\'QUARTER\', x)',
          oracle: 'TRUNC(CAST(x AS TIMESTAMP), \'QUARTER\')',
        },
      },
    );
  }

  testScalar () {
    this.validateAll(
      'SELECT CURRENT_USER',
      {
        read: {
          exasol: 'SELECT USER',
          spark: 'SELECT CURRENT_USER()',
          trino: 'SELECT CURRENT_USER',
          snowflake: 'SELECT CURRENT_USER()',
        },
        write: {
          exasol: 'SELECT CURRENT_USER',
          spark: 'SELECT CURRENT_USER()',
          trino: 'SELECT CURRENT_USER',
          snowflake: 'SELECT CURRENT_USER()',
        },
      },
    );
    this.validateAll(
      'CREATE OR REPLACE VIEW "schema"."v" ("col" COMMENT IS \'desc\') AS SELECT "src_col" AS "col"',
      {
        write: {
          databricks: 'CREATE OR REPLACE VIEW `schema`.`v` (`col` COMMENT \'desc\') AS SELECT `src_col` AS `col`',
          exasol: 'CREATE OR REPLACE VIEW "schema"."v" ("col" COMMENT IS \'desc\') AS SELECT "src_col" AS "col"',
        },
      },
    );
    this.validateAll(
      'HASH_SHA(x)',
      {
        read: {
          clickhouse: 'SHA1(x)',
          exasol: 'HASH_SHA1(x)',
          presto: 'SHA1(x)',
          trino: 'SHA1(x)',
        },
        write: {
          'exasol': 'HASH_SHA(x)',
          'clickhouse': 'SHA1(x)',
          'bigquery': 'SHA1(x)',
          '': 'SHA(x)',
          'presto': 'SHA1(x)',
          'trino': 'SHA1(x)',
        },
      },
    );
    this.validateAll(
      'HASH_MD5(x)',
      {
        write: {
          'exasol': 'HASH_MD5(x)',
          '': 'MD5(x)',
          'bigquery': 'TO_HEX(MD5(x))',
          'clickhouse': 'LOWER(HEX(MD5(x)))',
          'hive': 'MD5(x)',
          'presto': 'LOWER(TO_HEX(MD5(x)))',
          'spark': 'MD5(x)',
          'trino': 'LOWER(TO_HEX(MD5(x)))',
        },
      },
    );
    this.validateAll(
      'HASHTYPE_MD5(x)',
      {
        write: {
          'exasol': 'HASHTYPE_MD5(x)',
          '': 'MD5_DIGEST(x)',
          'bigquery': 'MD5(x)',
          'clickhouse': 'MD5(x)',
          'hive': 'UNHEX(MD5(x))',
          'presto': 'MD5(x)',
          'spark': 'UNHEX(MD5(x))',
          'trino': 'MD5(x)',
        },
      },
    );
    this.validateAll(
      'HASH_SHA256(x)',
      {
        read: {
          clickhouse: 'SHA256(x)',
          presto: 'SHA256(x)',
          trino: 'SHA256(x)',
          postgres: 'SHA256(x)',
          duckdb: 'SHA256(x)',
        },
        write: {
          exasol: 'HASH_SHA256(x)',
          bigquery: 'SHA256(x)',
          spark2: 'SHA2(x, 256)',
          clickhouse: 'SHA256(x)',
          postgres: 'SHA256(x)',
          presto: 'SHA256(x)',
          redshift: 'SHA2(x, 256)',
          trino: 'SHA256(x)',
          duckdb: 'SHA256(x)',
          snowflake: 'SHA2(x, 256)',
        },
      },
    );
    this.validateAll(
      'HASH_SHA512(x)',
      {
        read: {
          clickhouse: 'SHA512(x)',
          presto: 'SHA512(x)',
          trino: 'SHA512(x)',
        },
        write: {
          exasol: 'HASH_SHA512(x)',
          clickhouse: 'SHA512(x)',
          bigquery: 'SHA512(x)',
          spark2: 'SHA2(x, 512)',
          presto: 'SHA512(x)',
          trino: 'SHA512(x)',
        },
      },
    );
    this.validateAll(
      'SELECT NULLIFZERO(1) NIZ1',
      {
        write: {
          exasol: 'SELECT IF 1 = 0 THEN NULL ELSE 1 ENDIF AS NIZ1',
          snowflake: 'SELECT IFF(1 = 0, NULL, 1) AS NIZ1',
          sqlite: 'SELECT IIF(1 = 0, NULL, 1) AS NIZ1',
          presto: 'SELECT IF(1 = 0, NULL, 1) AS NIZ1',
          spark: 'SELECT IF(1 = 0, NULL, 1) AS NIZ1',
          hive: 'SELECT IF(1 = 0, NULL, 1) AS NIZ1',
          duckdb: 'SELECT CASE WHEN 1 = 0 THEN NULL ELSE 1 END AS NIZ1',
        },
      },
    );
    this.validateAll(
      'SELECT ZEROIFNULL(NULL) NIZ1',
      {
        write: {
          exasol: 'SELECT IF NULL IS NULL THEN 0 ELSE NULL ENDIF AS NIZ1',
          snowflake: 'SELECT IFF(NULL IS NULL, 0, NULL) AS NIZ1',
          sqlite: 'SELECT IIF(NULL IS NULL, 0, NULL) AS NIZ1',
          presto: 'SELECT IF(NULL IS NULL, 0, NULL) AS NIZ1',
          spark: 'SELECT IF(NULL IS NULL, 0, NULL) AS NIZ1',
          hive: 'SELECT IF(NULL IS NULL, 0, NULL) AS NIZ1',
          duckdb: 'SELECT CASE WHEN NULL IS NULL THEN 0 ELSE NULL END AS NIZ1',
        },
      },
    );
    this.validateIdentity(
      'SELECT name, age, IF age < 18 THEN \'underaged\' ELSE \'adult\' ENDIF AS LEGALITY FROM persons',
    );
    this.validateIdentity('SELECT HASHTYPE_MD5(a, b, c, d)');
  }

  testOdbcDateLiterals () {
    this.validateIdentity('SELECT {d\'2024-01-01\'}', 'SELECT TO_DATE(\'2024-01-01\')');
    this.validateIdentity(
      'SELECT {ts\'2024-01-01 12:00:00\'}',
      'SELECT TO_TIMESTAMP(\'2024-01-01 12:00:00\')',
    );
  }

  testLocalPrefixForAlias () {
    this.validateIdentity(
      'SELECT ID FROM local WHERE "LOCAL".ID IS NULL',
      'SELECT ID FROM "LOCAL" WHERE "LOCAL".ID IS NULL',
    );
    this.validateIdentity(
      'SELECT YEAR(a_date) AS "a_year" FROM MY_SUMMARY_TABLE GROUP BY LOCAL."a_year"',
    );
    this.validateIdentity(
      'SELECT a_year AS a_year FROM "LOCAL" GROUP BY "LOCAL".a_year',
    );

    const testCases: Array<[string, string, string]> = [
      [
        'GROUP BY alias',
        'SELECT YEAR(a_date) AS a_year FROM my_table GROUP BY LOCAL.a_year',
        'SELECT YEAR(a_date) AS a_year FROM my_table GROUP BY a_year',
      ],
      [
        'HAVING alias',
        'SELECT SUM(amount) AS total FROM my_table HAVING LOCAL.total > 10000',
        'SELECT SUM(amount) AS total FROM my_table HAVING total > 10000',
      ],
      [
        'WHERE alias',
        'SELECT YEAR(a_date) AS a_year FROM my_table WHERE LOCAL.a_year > 2020',
        'SELECT YEAR(a_date) AS a_year FROM my_table WHERE a_year > 2020',
      ],
      [
        'Multiple aliases',
        'SELECT YEAR(a_date) AS a_year, MONTH(a_date) AS a_month FROM my_table WHERE LOCAL.a_year > 2020 AND LOCAL.a_month < 6',
        'SELECT YEAR(a_date) AS a_year, MONTH(a_date) AS a_month FROM my_table WHERE a_year > 2020 AND a_month < 6',
      ],
      [
        'Select list aliases',
        'SELECT YR AS THE_YEAR, ID AS YR, LOCAL.THE_YEAR + 1 AS NEXT_YEAR FROM my_table',
        'SELECT YR AS THE_YEAR, ID AS YR, THE_YEAR + 1 AS NEXT_YEAR FROM my_table',
      ],
      [
        'Select list aliases without Local keyword',
        'SELECT YEAR(CURRENT_DATE) AS current_year, LOCAL.current_year + 1 AS next_year',
        'SELECT YEAR(CURRENT_DATE) AS current_year, current_year + 1 AS next_year',
      ],
    ];

    for (const [
      , exasolSql,
      dbxSql,
    ] of testCases) {
      this.validateAll(exasolSql, {
        write: {
          exasol: exasolSql,
          databricks: dbxSql,
        },
      });
    }
  }
}

const t = new TestExasol();
describe('TestExasol', () => {
  test('testExasol', () => t.testExasol());
  test('testQualifyUnscopedStar', () => t.testQualifyUnscopedStar());
  test('testTypeMappings', () => t.testTypeMappings());
  test('testMod', () => t.testMod());
  test('testBits', () => t.testBits());
  test('test_aggregateFunctions', () => t.testAggregateFunctions());
  test('test_stringFunctions', () => t.testStringFunctions());
  test('testDatetimeFunctions', () => t.testDatetimeFunctions());
  test('testNumberFunctions', () => t.testNumberFunctions());
  test('testScalar', () => t.testScalar());
  test('testOdbcDateLiterals', () => t.testOdbcDateLiterals());
  test('testLocalPrefixForAlias', () => t.testLocalPrefixForAlias());
});
