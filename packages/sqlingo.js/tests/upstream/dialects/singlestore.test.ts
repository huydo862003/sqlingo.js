import {
  describe, test,
} from 'vitest';
import {
  JsonArrayExpr, JsonObjectExpr, MatchAgainstExpr, SelectExpr,
} from '../../../src/expressions';
import {
  Validator,
} from './validator';

class TestSingleStore extends Validator {
  override dialect = 'singlestore' as const;

  testSinglestore () {
    // Note: The qualify() test from Python is skipped as qualify is not yet ported.

    this.validateIdentity('SELECT 1');
    this.validateIdentity('SELECT * FROM `users` ORDER BY ALL');
    this.validateIdentity('SELECT ELT(2, \'foo\', \'bar\', \'baz\')');
    this.validateIdentity('SELECT CHARSET(CHAR(100 USING utf8))');
    this.validateIdentity('SELECT TO_JSON(ROW(1, 2) :> RECORD(a INT, b INT))');

    this.validateIdentity('JSON_KEYS(json_doc, \'a\', \'b\', \'c\', 2)');
    this.validateIdentity('SELECT VERSION()');
    this.validateIdentity('SELECT CURTIME()', 'SELECT CURRENT_TIME()');
  }

  testByteStrings () {
    this.validateIdentity('SELECT e\'text\'');
    this.validateIdentity('SELECT E\'text\'', 'SELECT e\'text\'');
  }

  testNationalStrings () {
    this.validateAll(
      'SELECT \'text\'', { read: { '': 'SELECT N\'text\'', singlestore: 'SELECT \'text\'' } },
    );
  }

  testRestrictedKeywords () {
    this.validateIdentity('SELECT * FROM abs', 'SELECT * FROM `abs`');
    this.validateIdentity('SELECT * FROM ABS', 'SELECT * FROM `ABS`');
    this.validateIdentity(
      'SELECT * FROM security_lists_intersect', 'SELECT * FROM `security_lists_intersect`',
    );
    this.validateIdentity('SELECT * FROM vacuum', 'SELECT * FROM `vacuum`');
  }

  testTimeFormatting () {
    this.validateIdentity('SELECT STR_TO_DATE(\'March 3rd, 2015\', \'%M %D, %Y\')');
    this.validateIdentity('SELECT DATE_FORMAT(NOW(), \'%Y-%m-%d %h:%i:%s\')');
    this.validateIdentity(
      'SELECT TO_DATE(\'03/01/2019\', \'MM/DD/YYYY\') AS `result`',
    );
    this.validateIdentity(
      'SELECT TO_TIMESTAMP(\'The date and time are 01/01/2018 2:30:15.123456\', \'The date and time are MM/DD/YYYY HH12:MI:SS.FF6\') AS `result`',
    );
    this.validateIdentity(
      'SELECT TO_CHAR(\'2018-03-01\', \'MM/DD\')',
    );
    this.validateIdentity(
      'SELECT TIME_FORMAT(\'12:05:47\', \'%s, %i, %h\')',
      'SELECT DATE_FORMAT(\'12:05:47\' :> TIME(6), \'%s, %i, %h\')',
    );
    this.validateIdentity('SELECT DATE(\'2019-01-01 05:06\')');
    this.validateAll(
      'SELECT DATE(\'2019-01-01 05:06\')',
      {
        read: {
          '': 'SELECT TS_OR_DS_TO_DATE(\'2019-01-01 05:06\')',
          singlestore: 'SELECT DATE(\'2019-01-01 05:06\')',
        },
      },
    );
  }

  testCast () {
    this.validateAll(
      'SELECT 1 :> INT',
      {
        read: {
          '': 'SELECT CAST(1 AS INT)',
        },
        write: {
          singlestore: 'SELECT 1 :> INT',
          '': 'SELECT CAST(1 AS INT)',
        },
      },
    );
    this.validateAll(
      'SELECT 1 !:> INT',
      {
        read: {
          '': 'SELECT TRY_CAST(1 AS INT)',
        },
        write: {
          singlestore: 'SELECT 1 !:> INT',
          '': 'SELECT TRY_CAST(1 AS INT)',
        },
      },
    );
    this.validateIdentity('SELECT \'{"a" : 1}\' :> JSON');
    this.validateIdentity('SELECT NOW() !:> TIMESTAMP(6)');
    this.validateIdentity('SELECT x :> GEOGRAPHYPOINT');
    this.validateAll(
      'SELECT age :> TEXT FROM `users`',
      {
        read: {
          '': 'SELECT CAST(age, \'TEXT\') FROM users',
          singlestore: 'SELECT age :> TEXT FROM `users`',
        },
      },
    );
  }

  testUnixFunctions () {
    this.validateIdentity('SELECT FROM_UNIXTIME(1234567890)');
    this.validateIdentity('SELECT FROM_UNIXTIME(1234567890, \'%M %D, %Y\')');
    this.validateIdentity('SELECT UNIX_TIMESTAMP()');
    this.validateIdentity('SELECT UNIX_TIMESTAMP(\'2009-02-13 23:31:30\') AS funday');

    this.validateAll(
      'SELECT UNIX_TIMESTAMP(\'2009-02-13 23:31:30\')',
      { read: { duckdb: 'SELECT EPOCH(\'2009-02-13 23:31:30\')' } },
    );
    this.validateAll(
      'SELECT UNIX_TIMESTAMP(\'2009-02-13 23:31:30\')',
      { read: { duckdb: 'SELECT TIME_STR_TO_UNIX(\'2009-02-13 23:31:30\')' } },
    );
    this.validateAll(
      'SELECT UNIX_TIMESTAMP(\'2009-02-13 23:31:30\')',
      { read: { '': 'SELECT TIME_STR_TO_UNIX(\'2009-02-13 23:31:30\')' } },
    );
    this.validateAll(
      'SELECT UNIX_TIMESTAMP(\'2009-02-13 23:31:30\')',
      { read: { '': 'SELECT UNIX_SECONDS(\'2009-02-13 23:31:30\')' } },
    );

    this.validateAll(
      'SELECT FROM_UNIXTIME(1234567890, \'%Y-%m-%d %T\')',
      { read: { hive: 'SELECT FROM_UNIXTIME(1234567890)' } },
    );
    this.validateAll(
      'SELECT FROM_UNIXTIME(1234567890) :> TEXT',
      { read: { '': 'SELECT UNIX_TO_TIME_STR(1234567890)' } },
    );
  }

  testJsonExtract () {
    this.validateIdentity('SELECT a::b FROM t', 'SELECT JSON_EXTRACT_JSON(a, \'b\') FROM t');
    this.validateIdentity('SELECT a::b FROM t', 'SELECT JSON_EXTRACT_JSON(a, \'b\') FROM t');
    this.validateIdentity('SELECT a::$b FROM t', 'SELECT JSON_EXTRACT_STRING(a, \'b\') FROM t');
    this.validateIdentity('SELECT a::%b FROM t', 'SELECT JSON_EXTRACT_DOUBLE(a, \'b\') FROM t');
    this.validateIdentity(
      'SELECT a::`b`::`2` FROM t',
      'SELECT JSON_EXTRACT_JSON(JSON_EXTRACT_JSON(a, \'b\'), \'2\') FROM t',
    );
    this.validateIdentity('SELECT a::2 FROM t', 'SELECT JSON_EXTRACT_JSON(a, \'2\') FROM t');

    this.validateAll(
      'SELECT JSON_EXTRACT_JSON(a, \'b\') FROM t',
      {
        read: {
          mysql: 'SELECT JSON_EXTRACT(a, \'$.b\') FROM t',
          singlestore: 'SELECT JSON_EXTRACT_JSON(a, \'b\') FROM t',
        },
        write: { mysql: 'SELECT JSON_EXTRACT(a, \'$.b\') FROM t' },
      },
    );
    this.validateAll(
      'SELECT JSON_EXTRACT_STRING(a, \'b\') FROM t',
      { write: { '': 'SELECT JSON_EXTRACT_SCALAR(a, \'$.b\', STRING) FROM t' } },
    );
    this.validateAll(
      'SELECT JSON_EXTRACT_DOUBLE(a, \'b\') FROM t',
      { write: { '': 'SELECT JSON_EXTRACT_SCALAR(a, \'$.b\', DOUBLE) FROM t' } },
    );
    this.validateAll(
      'SELECT JSON_EXTRACT_BIGINT(a, \'b\') FROM t',
      { write: { '': 'SELECT JSON_EXTRACT_SCALAR(a, \'$.b\', BIGINT) FROM t' } },
    );
    this.validateAll(
      'SELECT JSON_EXTRACT_BIGINT(a, \'b\') FROM t',
      { write: { '': 'SELECT JSON_EXTRACT_SCALAR(a, \'$.b\', BIGINT) FROM t' } },
    );
    this.validateAll(
      'SELECT JSON_EXTRACT_JSON(a, \'b\', \'2\') FROM t',
      {
        read: {
          mysql: 'SELECT JSON_EXTRACT(a, \'$.b[2]\') FROM t',
          singlestore: 'SELECT JSON_EXTRACT_JSON(a, \'b\', \'2\') FROM t',
        },
        write: { mysql: 'SELECT JSON_EXTRACT(a, \'$.b[2]\') FROM t' },
      },
    );
    this.validateAll(
      'SELECT JSON_EXTRACT_STRING(a, \'b\', 2) FROM t',
      { write: { '': 'SELECT JSON_EXTRACT_SCALAR(a, \'$.b[2]\', STRING) FROM t' } },
    );

    this.validateAll(
      'SELECT BSON_EXTRACT_BSON(a, \'b\') FROM t',
      {
        read: {
          mysql: 'SELECT JSONB_EXTRACT(a, \'b\') FROM t',
          singlestore: 'SELECT BSON_EXTRACT_BSON(a, \'b\') FROM t',
        },
        write: { mysql: 'SELECT JSONB_EXTRACT(a, \'$.b\') FROM t' },
      },
    );
    this.validateAll(
      'SELECT BSON_EXTRACT_STRING(a, \'b\') FROM t',
      { write: { '': 'SELECT JSONB_EXTRACT_SCALAR(a, \'$.b\', STRING) FROM t' } },
    );
    this.validateAll(
      'SELECT BSON_EXTRACT_DOUBLE(a, \'b\') FROM t',
      { write: { '': 'SELECT JSONB_EXTRACT_SCALAR(a, \'$.b\', DOUBLE) FROM t' } },
    );
    this.validateAll(
      'SELECT BSON_EXTRACT_BIGINT(a, \'b\') FROM t',
      { write: { '': 'SELECT JSONB_EXTRACT_SCALAR(a, \'$.b\', BIGINT) FROM t' } },
    );
    this.validateAll(
      'SELECT BSON_EXTRACT_BIGINT(a, \'b\') FROM t',
      { write: { '': 'SELECT JSONB_EXTRACT_SCALAR(a, \'$.b\', BIGINT) FROM t' } },
    );
    this.validateAll(
      'SELECT BSON_EXTRACT_BSON(a, \'b\', 2) FROM t',
      { write: { '': 'SELECT JSONB_EXTRACT(a, \'$.b[2]\') FROM t' } },
    );
    this.validateAll(
      'SELECT BSON_EXTRACT_STRING(a, \'b\', 2) FROM t',
      { write: { '': 'SELECT JSONB_EXTRACT_SCALAR(a, \'$.b[2]\', STRING) FROM t' } },
    );
    this.validateAll(
      'SELECT JSON_EXTRACT_STRING(\'{"item": "shoes", "price": "49.95"}\', \'price\') :> DECIMAL(4, 2)',
      {
        read: {
          mysql: 'SELECT JSON_VALUE(\'{"item": "shoes", "price": "49.95"}\', \'$.price\' RETURNING DECIMAL(4, 2))',
        },
      },
    );
  }

  testJson () {
    this.validateIdentity('SELECT JSON_ARRAY_CONTAINS_STRING(\'["a", "b"]\', \'b\')');
    this.validateIdentity('SELECT JSON_ARRAY_CONTAINS_DOUBLE(\'[1, 2]\', 1)');
    this.validateIdentity('SELECT JSON_ARRAY_CONTAINS_JSON(\'["{"a": 1}"]\', \'{"a":   1}\')');
    this.validateAll(
      'SELECT JSON_ARRAY_CONTAINS_JSON(\'["a"]\', TO_JSON(\'a\'))',
      {
        read: {
          mysql: 'SELECT \'a\' MEMBER OF (\'["a"]\')',
          singlestore: 'SELECT JSON_ARRAY_CONTAINS_JSON(\'["a"]\', TO_JSON(\'a\'))',
        },
      },
    );
    this.validateAll(
      'SELECT JSON_PRETTY(\'["G","alpha","20",10]\')',
      {
        read: {
          singlestore: 'SELECT JSON_PRETTY(\'["G","alpha","20",10]\')',
          '': 'SELECT JSON_FORMAT(\'["G","alpha","20",10]\')',
        },
      },
    );
    this.validateAll(
      'SELECT JSON_AGG(name ORDER BY id ASC NULLS LAST, name DESC NULLS FIRST) FROM t',
      {
        read: {
          singlestore: 'SELECT JSON_AGG(name ORDER BY id ASC NULLS LAST, name DESC NULLS FIRST) FROM t',
          oracle: 'SELECT JSON_ARRAYAGG(name ORDER BY id ASC, name DESC) FROM t',
        },
      },
    );
    this.validateIdentity('SELECT JSON_AGG(name) FROM t');
    this.validateIdentity('SELECT JSON_AGG(t.*) FROM t');
    this.validateAll(
      'SELECT JSON_BUILD_ARRAY(id, name) FROM t',
      {
        read: {
          singlestore: 'SELECT JSON_BUILD_ARRAY(id, name) FROM t',
          oracle: 'SELECT JSON_ARRAY(id, name) FROM t',
        },
      },
    );
    this.validateIdentity('JSON_BUILD_ARRAY(id, name)').assertIs(JsonArrayExpr);
    this.validateAll(
      'SELECT BSON_MATCH_ANY_EXISTS(\'{"x":true}\', \'x\')',
      {
        read: {
          singlestore: 'SELECT BSON_MATCH_ANY_EXISTS(\'{"x":true}\', \'x\')',
          '': 'SELECT JSONB_EXISTS(\'{"x":true}\', \'x\')',
        },
      },
    );
    this.validateAll(
      'SELECT JSON_MATCH_ANY_EXISTS(\'{"a":1}\', \'a\')',
      {
        read: {
          singlestore: 'SELECT JSON_MATCH_ANY_EXISTS(\'{"a":1}\', \'a\')',
          oracle: 'SELECT JSON_EXISTS(\'{"a":1}\', \'$.a\')',
        },
      },
    );
    this.validateAll(
      'SELECT JSON_BUILD_OBJECT(\'name\', name) FROM t',
      {
        read: {
          singlestore: 'SELECT JSON_BUILD_OBJECT(\'name\', name) FROM t',
          '': 'SELECT JSON_OBJECT(\'name\', name) FROM t',
        },
      },
    );
    this.validateIdentity('JSON_BUILD_OBJECT(\'name\', name)').assertIs(JsonObjectExpr);
  }

  testDatePartsFunctions () {
    this.validateIdentity(
      'SELECT DAYNAME(\'2014-04-18\')', 'SELECT DATE_FORMAT(\'2014-04-18\', \'%W\')',
    );
    this.validateIdentity(
      'SELECT HOUR(\'2009-02-13 23:31:30\')',
      'SELECT DATE_FORMAT(\'2009-02-13 23:31:30\' :> TIME(6), \'%k\') :> INT',
    );
    this.validateIdentity(
      'SELECT MICROSECOND(\'2009-02-13 23:31:30.123456\')',
      'SELECT DATE_FORMAT(\'2009-02-13 23:31:30.123456\' :> TIME(6), \'%f\') :> INT',
    );
    this.validateIdentity(
      'SELECT SECOND(\'2009-02-13 23:31:30.123456\')',
      'SELECT DATE_FORMAT(\'2009-02-13 23:31:30.123456\' :> TIME(6), \'%s\') :> INT',
    );
    this.validateIdentity(
      'SELECT MONTHNAME(\'2014-04-18\')', 'SELECT DATE_FORMAT(\'2014-04-18\', \'%M\')',
    );
    this.validateIdentity(
      'SELECT WEEKDAY(\'2014-04-18\')', 'SELECT (DAYOFWEEK(\'2014-04-18\') + 5) % 7',
    );
    this.validateIdentity(
      'SELECT MINUTE(\'2009-02-13 23:31:30.123456\')',
      'SELECT DATE_FORMAT(\'2009-02-13 23:31:30.123456\' :> TIME(6), \'%i\') :> INT',
    );
    this.validateAll(
      'SELECT ((DAYOFWEEK(\'2014-04-18\') % 7) + 1)',
      {
        read: {
          singlestore: 'SELECT ((DAYOFWEEK(\'2014-04-18\') % 7) + 1)',
          '': 'SELECT DAYOFWEEK_ISO(\'2014-04-18\')',
        },
      },
    );
    this.validateAll(
      'SELECT DAY(\'2014-04-18\')',
      {
        read: {
          singlestore: 'SELECT DAY(\'2014-04-18\')',
          '': 'SELECT DAY_OF_MONTH(\'2014-04-18\')',
        },
      },
    );
  }

  testMathFunctions () {
    this.validateAll(
      'SELECT APPROX_COUNT_DISTINCT(asset_id) AS approx_distinct_asset_id FROM acd_assets',
      {
        read: {
          singlestore: 'SELECT APPROX_COUNT_DISTINCT(asset_id) AS approx_distinct_asset_id FROM acd_assets',
          '': 'SELECT HLL(asset_id) AS approx_distinct_asset_id FROM acd_assets',
        },
      },
    );
    this.validateIdentity(
      'SELECT APPROX_COUNT_DISTINCT(asset_id1, asset_id2) AS approx_distinct_asset_id FROM acd_assets',
    );
    this.validateAll(
      'SELECT APPROX_COUNT_DISTINCT(asset_id) AS approx_distinct_asset_id FROM acd_assets',
      {
        read: {
          singlestore: 'SELECT APPROX_COUNT_DISTINCT(asset_id) AS approx_distinct_asset_id FROM acd_assets',
          '': 'SELECT APPROX_DISTINCT(asset_id) AS approx_distinct_asset_id FROM acd_assets',
        },
      },
    );
    this.validateAll(
      'SELECT SUM(CASE WHEN age > 18 THEN 1 ELSE 0 END) FROM `users`',
      {
        read: {
          singlestore: 'SELECT SUM(CASE WHEN age > 18 THEN 1 ELSE 0 END) FROM `users`',
          '': 'SELECT COUNT_IF(age > 18) FROM users',
        },
      },
    );
    this.validateAll(
      'SELECT MAX(ABS(age > 18)) FROM `users`',
      {
        read: {
          singlestore: 'SELECT MAX(ABS(age > 18)) FROM `users`',
          '': 'SELECT LOGICAL_OR(age > 18) FROM users',
        },
      },
    );
    this.validateAll(
      'SELECT MIN(ABS(age > 18)) FROM `users`',
      {
        read: {
          singlestore: 'SELECT MIN(ABS(age > 18)) FROM `users`',
          '': 'SELECT LOGICAL_AND(age > 18) FROM users',
        },
      },
    );
    this.validateIdentity(
      'SELECT `class`, student_id, test1, APPROX_PERCENTILE(test1, 0.3) OVER (PARTITION BY `class`) AS percentile FROM test_scores',
    );
    this.validateIdentity(
      'SELECT `class`, student_id, test1, APPROX_PERCENTILE(test1, 0.3, 0.4) OVER (PARTITION BY `class`) AS percentile FROM test_scores',
    );
    this.validateAll(
      'SELECT APPROX_PERCENTILE(test1, 0.3) FROM test_scores',
      {
        read: {
          singlestore: 'SELECT APPROX_PERCENTILE(test1, 0.3) FROM test_scores',
          // accuracy parameter is not supported in SingleStore, so it is ignored
          '': 'SELECT APPROX_QUANTILE(test1, 0.3, 0.4) FROM test_scores',
        },
      },
    );
    this.validateAll(
      'SELECT VAR_SAMP(yearly_total) FROM player_scores',
      {
        read: {
          singlestore: 'SELECT VAR_SAMP(yearly_total) FROM player_scores',
          '': 'SELECT VARIANCE(yearly_total) FROM player_scores',
        },
        write: {
          '': 'SELECT VARIANCE(yearly_total) FROM player_scores',
        },
      },
    );
    this.validateAll(
      'SELECT VAR_POP(yearly_total) FROM player_scores',
      {
        read: {
          singlestore: 'SELECT VARIANCE(yearly_total) FROM player_scores',
          '': 'SELECT VARIANCE_POP(yearly_total) FROM player_scores',
        },
        write: {
          '': 'SELECT VARIANCE_POP(yearly_total) FROM player_scores',
        },
      },
    );
    this.validateAll(
      'SELECT POWER(id, 1 / 3) FROM orders',
      {
        read: {
          '': 'SELECT CBRT(id) FROM orders',
          singlestore: 'SELECT POWER(id, 1 / 3) FROM orders',
        },
      },
    );
  }

  testLogical () {
    this.validateAll(
      'SELECT (TRUE AND (NOT FALSE)) OR ((NOT TRUE) AND FALSE)',
      {
        read: {
          mysql: 'SELECT TRUE XOR FALSE',
          singlestore: 'SELECT (TRUE AND (NOT FALSE)) OR ((NOT TRUE) AND FALSE)',
        },
      },
    );
  }

  testStringFunctions () {
    this.validateAll(
      'SELECT \'a\' RLIKE \'b\'',
      {
        read: {
          bigquery: 'SELECT REGEXP_CONTAINS(\'a\', \'b\')',
          singlestore: 'SELECT \'a\' RLIKE \'b\'',
        },
      },
    );
    this.validateIdentity('SELECT \'a\' REGEXP \'b\'', 'SELECT \'a\' RLIKE \'b\'');
    this.validateAll(
      'SELECT LPAD(\'\', LENGTH(\'a\') * 3, \'a\')',
      {
        read: {
          '': 'SELECT REPEAT(\'a\', 3)',
          singlestore: 'SELECT LPAD(\'\', LENGTH(\'a\') * 3, \'a\')',
        },
      },
    );
    this.validateAll(
      'SELECT REGEXP_SUBSTR(\'adog\', \'O\', 1, 1, \'c\')',
      {
        read: {
          // group parameter is not supported in SingleStore, so it is ignored
          '': 'SELECT REGEXP_EXTRACT(\'adog\', \'O\', 1, 1, \'c\', \'gr1\')',
          singlestore: 'SELECT REGEXP_SUBSTR(\'adog\', \'O\', 1, 1, \'c\')',
        },
      },
    );
    this.validateAll(
      `SELECT ('a' RLIKE '^[\x00-\x7f]*$')`,
      {
        read: { singlestore: `SELECT ('a' RLIKE '^[\x00-\x7f]*$')`, '': "SELECT IS_ASCII('a')" },
      },
    );
    this.validateAll(
      'SELECT UNHEX(MD5(\'data\'))',
      {
        read: {
          singlestore: 'SELECT UNHEX(MD5(\'data\'))',
          '': 'SELECT MD5_DIGEST(\'data\')',
        },
      },
    );
    this.validateAll(
      'SELECT CHAR(101)', { read: { '': 'SELECT CHR(101)', singlestore: 'SELECT CHAR(101)' } },
    );
    this.validateAll(
      'SELECT INSTR(\'ohai\', \'i\')',
      {
        read: {
          '': 'SELECT CONTAINS(\'ohai\', \'i\')',
          singlestore: 'SELECT INSTR(\'ohai\', \'i\')',
        },
      },
    );
    this.validateAll(
      'SELECT REGEXP_MATCH(\'adog\', \'O\', \'c\')',
      {
        read: {
          // group, position, occurrence parameters are not supported in SingleStore, so they are ignored
          '': 'SELECT REGEXP_EXTRACT_ALL(\'adog\', \'O\', 1, \'c\', 1, \'gr1\')',
          singlestore: 'SELECT REGEXP_MATCH(\'adog\', \'O\', \'c\')',
        },
      },
    );
    this.validateAll(
      'SELECT REGEXP_SUBSTR(\'adog\', \'O\', 1, 1, \'c\')',
      {
        read: {
          // group parameter is not supported in SingleStore, so it is ignored
          '': 'SELECT REGEXP_EXTRACT(\'adog\', \'O\', 1, 1, \'c\', \'gr1\')',
          singlestore: 'SELECT REGEXP_SUBSTR(\'adog\', \'O\', 1, 1, \'c\')',
        },
      },
    );
    this.validateAll(
      'SELECT REGEXP_INSTR(\'abcd\', CONCAT(\'^\', \'ab\'))',
      {
        read: {
          '': 'SELECT STARTS_WITH(\'abcd\', \'ab\')',
          singlestore: 'SELECT REGEXP_INSTR(\'abcd\', CONCAT(\'^\', \'ab\'))',
        },
      },
    );
    this.validateAll(
      'SELECT CONV(\'f\', 16, 10)',
      {
        read: {
          redshift: 'SELECT STRTOL(\'f\',16)',
          singlestore: 'SELECT CONV(\'f\', 16, 10)',
        },
      },
    );
    this.validateAll(
      'SELECT LOWER(\'ABC\') RLIKE LOWER(\'a.*\')',
      {
        read: {
          postgres: 'SELECT \'ABC\' ~* \'a.*\'',
          singlestore: 'SELECT LOWER(\'ABC\') RLIKE LOWER(\'a.*\')',
        },
      },
    );
    this.validateAll(
      'SELECT CONCAT(SUBSTRING(\'abcdef\', 1, 2 - 1), \'xyz\', SUBSTRING(\'abcdef\', 2 + 3))',
      {
        read: {
          singlestore: 'SELECT CONCAT(SUBSTRING(\'abcdef\', 1, 2 - 1), \'xyz\', SUBSTRING(\'abcdef\', 2 + 3))',
          '': 'SELECT STUFF(\'abcdef\', 2, 3, \'xyz\')',
        },
      },
    );
    this.validateAll(
      'SELECT SHA(email) FROM t',
      {
        read: {
          singlestore: 'SELECT SHA(email) FROM t',
          '': 'SELECT STANDARD_HASH(email) FROM t',
        },
      },
    );
    this.validateAll(
      'SELECT SHA(email) FROM t',
      {
        read: {
          singlestore: 'SELECT SHA(email) FROM t',
          '': 'SELECT STANDARD_HASH(email, \'sha\') FROM t',
        },
      },
    );
    this.validateAll(
      'SELECT MD5(email) FROM t',
      {
        read: {
          singlestore: 'SELECT MD5(email) FROM t',
          '': 'SELECT STANDARD_HASH(email, \'MD5\') FROM t',
        },
      },
    );
  }

  testReduceFunctions () {
    this.validateAll(
      'SELECT REDUCE(0, JSON_TO_ARRAY(\'[1,2,3,4]\'), REDUCE_ACC() + REDUCE_VALUE()) AS `Result`',
      {
        read: {
          // finish argument is not supported in SingleStore, so it is ignored
          '': 'SELECT REDUCE(JSON_TO_ARRAY(\'[1,2,3,4]\'), 0, REDUCE_ACC() + REDUCE_VALUE(), REDUCE_ACC() + REDUCE_VALUE()) AS Result',
          singlestore: 'SELECT REDUCE(0, JSON_TO_ARRAY(\'[1,2,3,4]\'), REDUCE_ACC() + REDUCE_VALUE()) AS `Result`',
        },
      },
    );
  }

  testTimeFunctions () {
    this.validateAll(
      'SELECT TIME_BUCKET(\'1d\', \'2019-03-14 06:04:12\', \'2019-03-13 03:00:00\')',
      {
        read: {
          // unit and zone parameters are not supported in SingleStore, so they are ignored
          '': 'SELECT DATE_BIN(\'1d\', \'2019-03-14 06:04:12\', DAY, \'UTC\', \'2019-03-13 03:00:00\')',
          singlestore: 'SELECT TIME_BUCKET(\'1d\', \'2019-03-14 06:04:12\', \'2019-03-13 03:00:00\')',
        },
      },
    );
    this.validateAll(
      'SELECT \'2019-03-14 06:04:12\' :> DATE',
      {
        read: {
          '': 'SELECT TIME_STR_TO_DATE(\'2019-03-14 06:04:12\')',
          singlestore: 'SELECT \'2019-03-14 06:04:12\' :> DATE',
        },
      },
    );
    this.validateAll(
      'SELECT CONVERT_TZ(NOW() :> TIMESTAMP, \'GMT\', \'UTC\')',
      {
        read: {
          spark2: 'SELECT TO_UTC_TIMESTAMP(NOW(), \'GMT\')',
          singlestore: 'SELECT CONVERT_TZ(NOW() :> TIMESTAMP, \'GMT\', \'UTC\')',
        },
      },
    );
    this.validateAll(
      'SELECT STR_TO_DATE(20190314, \'%Y%m%d\')',
      {
        read: {
          '': 'SELECT DI_TO_DATE(20190314)',
          singlestore: 'SELECT STR_TO_DATE(20190314, \'%Y%m%d\')',
        },
      },
    );
    this.validateAll(
      'SELECT (DATE_FORMAT(\'2019-03-14 06:04:12\', \'%Y%m%d\') :> INT)',
      {
        read: {
          singlestore: 'SELECT (DATE_FORMAT(\'2019-03-14 06:04:12\', \'%Y%m%d\') :> INT)',
          '': 'SELECT DATE_TO_DI(\'2019-03-14 06:04:12\')',
        },
      },
    );
    this.validateAll(
      'SELECT (DATE_FORMAT(\'2019-03-14 06:04:12\', \'%Y%m%d\') :> INT)',
      {
        read: {
          singlestore: 'SELECT (DATE_FORMAT(\'2019-03-14 06:04:12\', \'%Y%m%d\') :> INT)',
          '': 'SELECT TS_OR_DI_TO_DI(\'2019-03-14 06:04:12\')',
        },
      },
    );
    this.validateAll(
      'SELECT \'2019-03-14 06:04:12\' :> TIME',
      {
        read: {
          // zone parameter is not supported in SingleStore, so it is ignored
          bigquery: 'SELECT TIME(\'2019-03-14 06:04:12\', \'GMT\')',
          singlestore: 'SELECT \'2019-03-14 06:04:12\' :> TIME',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_ADD(NOW(), INTERVAL \'1\' MONTH)',
      {
        read: {
          bigquery: 'SELECT DATETIME_ADD(NOW(), INTERVAL 1 MONTH)',
          singlestore: 'SELECT DATE_ADD(NOW(), INTERVAL \'1\' MONTH)',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_TRUNC(\'MINUTE\', \'2016-08-08 12:05:31\')',
      {
        read: {
          bigquery: 'SELECT DATETIME_TRUNC(\'2016-08-08 12:05:31\', MINUTE)',
          singlestore: 'SELECT DATE_TRUNC(\'MINUTE\', \'2016-08-08 12:05:31\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_SUB(\'2010-04-02\', INTERVAL \'1\' WEEK)',
      {
        read: {
          bigquery: 'SELECT DATETIME_SUB(\'2010-04-02\', INTERVAL \'1\' WEEK)',
          singlestore: 'SELECT DATE_SUB(\'2010-04-02\', INTERVAL \'1\' WEEK)',
        },
      },
    );
    this.validateAll(
      'SELECT TIMESTAMPDIFF(QUARTER, \'2009-02-13\', \'2013-09-01\')',
      {
        read: {
          singlestore: 'SELECT TIMESTAMPDIFF(QUARTER, \'2009-02-13\', \'2013-09-01\')',
          '': 'SELECT DATETIME_DIFF(\'2013-09-01\', \'2009-02-13\', QUARTER)',
        },
      },
    );
    this.validateAll(
      'SELECT TIMESTAMPDIFF(QUARTER, \'2009-02-13\', \'2013-09-01\')',
      {
        read: {
          singlestore: 'SELECT TIMESTAMPDIFF(QUARTER, \'2009-02-13\', \'2013-09-01\')',
          bigquery: 'SELECT DATE_DIFF(\'2013-09-01\', \'2009-02-13\', QUARTER)',
          duckdb: 'SELECT DATE_DIFF(\'QUARTER\', \'2009-02-13\', \'2013-09-01\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(DATE(\'2013-09-01\'), DATE(\'2009-02-13\'))',
      {
        read: {
          hive: 'SELECT DATEDIFF(\'2013-09-01\', \'2009-02-13\')',
          singlestore: 'SELECT DATEDIFF(DATE(\'2013-09-01\'), DATE(\'2009-02-13\'))',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_TRUNC(\'MINUTE\', \'2016-08-08 12:05:31\')',
      {
        read: {
          '': 'SELECT TIMESTAMP_TRUNC(\'2016-08-08 12:05:31\', MINUTE)',
          singlestore: 'SELECT DATE_TRUNC(\'MINUTE\', \'2016-08-08 12:05:31\')',
        },
      },
    );
    this.validateAll(
      'SELECT TIMESTAMPDIFF(WEEK, \'2009-01-01\', \'2009-12-31\') AS numweeks',
      {
        read: {
          redshift: 'SELECT datediff(week,\'2009-01-01\',\'2009-12-31\') AS numweeks',
          singlestore: 'SELECT TIMESTAMPDIFF(WEEK, \'2009-01-01\', \'2009-12-31\') AS numweeks',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(\'2009-12-31\', \'2009-01-01\') AS numweeks',
      {
        read: {
          '': 'SELECT TS_OR_DS_DIFF(\'2009-12-31\', \'2009-01-01\') AS numweeks',
          singlestore: 'SELECT DATEDIFF(\'2009-12-31\', \'2009-01-01\') AS numweeks',
        },
      },
    );
    this.validateAll(
      'SELECT CURRENT_DATE()',
      {
        read: {
          '': 'SELECT CURRENT_DATE()',
          singlestore: 'SELECT CURRENT_DATE',
        },
      },
    );
    this.validateAll(
      'SELECT UTC_DATE()',
      {
        read: {
          '': 'SELECT CURRENT_DATE(\'UTC\')',
          singlestore: 'SELECT UTC_DATE',
        },
        write: { '': 'SELECT CURRENT_DATE(\'UTC\')' },
      },
    );
    this.validateAll(
      'SELECT CURRENT_TIME()',
      {
        read: {
          '': 'SELECT CURRENT_TIME()',
          singlestore: 'SELECT CURRENT_TIME',
        },
      },
    );
    this.validateIdentity('SELECT CURRENT_TIME(6)');
    this.validateAll(
      'SELECT UTC_TIME()',
      {
        read: {
          '': 'SELECT CURRENT_TIME(\'UTC\')',
          singlestore: 'SELECT UTC_TIME',
        },
        write: { '': 'SELECT CURRENT_TIME(\'UTC\')' },
      },
    );
    this.validateAll(
      'SELECT CURRENT_TIMESTAMP()',
      {
        read: {
          '': 'SELECT CURRENT_TIMESTAMP()',
          singlestore: 'SELECT CURRENT_TIMESTAMP',
        },
      },
    );
    this.validateIdentity('SELECT CURRENT_TIMESTAMP(6)');
    this.validateAll(
      'SELECT UTC_TIMESTAMP()',
      {
        read: {
          '': 'SELECT CURRENT_TIMESTAMP(\'UTC\')',
          singlestore: 'SELECT UTC_TIMESTAMP',
        },
        write: { '': 'SELECT CURRENT_TIMESTAMP(\'UTC\')' },
      },
    );
    this.validateAll(
      'SELECT CURRENT_TIMESTAMP(6) :> DATETIME(6)',
      {
        read: {
          bigquery: 'SELECT CURRENT_DATETIME()',
          singlestore: 'SELECT CURRENT_TIMESTAMP(6) :> DATETIME(6)',
        },
      },
    );
    this.validateIdentity('SELECT UTC_TIMESTAMP(6)');
    this.validateIdentity('SELECT UTC_TIME(6)');
  }

  testTypes () {
    this.validateAll(
      'CREATE TABLE testTypes (a DECIMAL(10, 20))',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a DECIMAL(10, 20))',
          bigquery: 'CREATE TABLE testTypes (a BIGDECIMAL(10, 20))',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a BOOLEAN)',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a BOOLEAN)',
          tsql: 'CREATE TABLE testTypes (a BIT)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a DATE)',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a DATE)',
          clickhouse: 'CREATE TABLE testTypes (a DATE32)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a DATETIME)',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a DATETIME)',
          clickhouse: 'CREATE TABLE testTypes (a DATETIME64)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a DECIMAL(9, 3))',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a DECIMAL(9, 3))',
          clickhouse: 'CREATE TABLE testTypes (a DECIMAL32(3))',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a DECIMAL(18, 3))',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a DECIMAL(18, 3))',
          clickhouse: 'CREATE TABLE testTypes (a DECIMAL64(3))',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a DECIMAL(38, 3))',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a DECIMAL(38, 3))',
          clickhouse: 'CREATE TABLE testTypes (a DECIMAL128(3))',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a DECIMAL(65, 3))',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a DECIMAL(65, 3))',
          clickhouse: 'CREATE TABLE testTypes (a DECIMAL256(3))',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a ENUM(\'a\'))',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a ENUM(\'a\'))',
          clickhouse: 'CREATE TABLE testTypes (a ENUM8(\'a\'))',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a ENUM(\'a\'))',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a ENUM(\'a\'))',
          clickhouse: 'CREATE TABLE testTypes (a ENUM16(\'a\'))',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a TEXT(2))',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a TEXT(2))',
          clickhouse: 'CREATE TABLE testTypes (a FIXEDSTRING(2))',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a GEOGRAPHY)',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a GEOGRAPHY)',
          snowflake: 'CREATE TABLE testTypes (a GEOMETRY)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a GEOGRAPHYPOINT)',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a GEOGRAPHYPOINT)',
          clickhouse: 'CREATE TABLE testTypes (a POINT)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a GEOGRAPHY)',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a GEOGRAPHY)',
          clickhouse: 'CREATE TABLE testTypes (a RING)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a GEOGRAPHY)',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a GEOGRAPHY)',
          clickhouse: 'CREATE TABLE testTypes (a LINESTRING)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a GEOGRAPHY)',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a GEOGRAPHY)',
          clickhouse: 'CREATE TABLE testTypes (a POLYGON)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a GEOGRAPHY)',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a GEOGRAPHY)',
          clickhouse: 'CREATE TABLE testTypes (a MULTIPOLYGON)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a BSON)',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a BSON)',
          postgres: 'CREATE TABLE testTypes (a JSONB)',
        },
      },
    );
    this.validateIdentity('CREATE TABLE testTypes (a TIMESTAMP(6))');
    this.validateAll(
      'CREATE TABLE testTypes (a TIMESTAMP)',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a TIMESTAMP)',
          duckdb: 'CREATE TABLE testTypes (a TIMESTAMP_S)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a TIMESTAMP(6))',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a TIMESTAMP(6))',
          duckdb: 'CREATE TABLE testTypes (a TIMESTAMP_MS)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE testTypes (a BLOB)',
      {
        read: {
          singlestore: 'CREATE TABLE testTypes (a BLOB)',
          '': 'CREATE TABLE testTypes (a VARBINARY)',
        },
      },
    );
  }

  testColumnWithTablename () {
    this.validateIdentity('SELECT `t0`.`name` FROM `t0`');
  }

  testUnicodestringSQL () {
    this.validateAll(
      'SELECT \'data\'',
      { read: { presto: 'SELECT U&\'d\\0061t\\0061\'', singlestore: 'SELECT \'data\'' } },
    );
  }

  testCollateSQL () {
    this.validateAll(
      'SELECT name :> LONGTEXT COLLATE \'utf8mb4_bin\' FROM `users`',
      {
        read: {
          '': 'SELECT name COLLATE \'utf8mb4_bin\' FROM users',
        },
      },
    );
    this.validateIdentity(
      'SELECT name :> LONGTEXT COLLATE \'utf8mb4_bin\' FROM `users`',
      'SELECT name :> LONGTEXT :> LONGTEXT COLLATE \'utf8mb4_bin\' FROM `users`',
    );
  }

  testMatchAgainst () {
    (this.validateIdentity(
      'SELECT MATCH(name) AGAINST(\'search term\') FROM products',
    ) as SelectExpr).selects[0].assertIs(MatchAgainstExpr);
    (this.validateIdentity(
      'SELECT MATCH(name, name) AGAINST(\'book\') FROM products',
    ) as SelectExpr).selects[0].assertIs(MatchAgainstExpr);
    (this.validateIdentity(
      'SELECT MATCH(TABLE products2) AGAINST(\'search term\') FROM products2',
    ) as SelectExpr).selects[0].assertIs(MatchAgainstExpr);
  }

  testShow () {
    this.validateIdentity('SHOW AGGREGATES FROM db1');
    this.validateIdentity('SHOW AGGREGATES LIKE \'multiply%\'');
    this.validateIdentity('SHOW CDC EXTRACTOR POOL');
    this.validateIdentity('SHOW CREATE AGGREGATE avg_udaf');
    this.validateIdentity('SHOW CREATE PIPELINE mypipeline');
    this.validateIdentity('SHOW CREATE PROJECTION lineitem_sort_shipdate FOR TABLE lineitem');
    this.validateIdentity('SHOW DATABASE STATUS');
    this.validateIdentity('SHOW DISTRIBUTED_PLANCACHE STATUS');
    this.validateIdentity('SHOW FULLTEXT SERVICE STATUS');
    this.validateIdentity('SHOW FULLTEXT SERVICE METRICS LOCAL');
    this.validateIdentity('SHOW FULLTEXT SERVICE METRICS FOR NODE 1');
    this.validateIdentity('SHOW FUNCTIONS FROM db LIKE \'a\'');
    this.validateIdentity('SHOW GROUPS');
    this.validateIdentity('SHOW GROUPS FOR ROLE \'role_name_0\'');
    this.validateIdentity('SHOW GROUPS FOR USER \'root\'');
    this.validateIdentity('SHOW INDEXES FROM mytbl', 'SHOW INDEX FROM mytbl');
    this.validateIdentity('SHOW KEYS FROM mytbl', 'SHOW INDEX FROM mytbl');
    this.validateIdentity('SHOW LINKS ON Orderdb');
    this.validateIdentity('SHOW LOAD ERRORS');
    this.validateIdentity('SHOW LOAD WARNINGS');
    this.validateIdentity('SHOW PARTITIONS ON memsql_demo');
    this.validateIdentity('SHOW PIPELINES');
    this.validateIdentity('SHOW PLAN JSON 25');
    this.validateIdentity('SHOW PLAN 25');
    this.validateIdentity('SHOW PLANCACHE');
    this.validateIdentity('SHOW PROCEDURES FROM dbExample');
    this.validateIdentity('SHOW PROCEDURES LIKE \'%sp%\'');
    this.validateIdentity('SHOW PROJECTIONS ON TABLE t');
    this.validateIdentity('SHOW PROJECTIONS');
    this.validateIdentity('SHOW REPLICATION STATUS');
    this.validateIdentity('SHOW REPRODUCTION');
    this.validateIdentity('SHOW REPRODUCTION INTO OUTFILE \'a\'');
    this.validateIdentity('SHOW RESOURCE POOLS');
    this.validateIdentity('SHOW ROLES LIKE \'xyz\'');
    this.validateIdentity('SHOW ROLES FOR GROUP \'group_0\'');
    this.validateIdentity('SHOW ROLES FOR USER \'root\'');
    this.validateIdentity('SHOW STATUS');
    this.validateIdentity('SHOW USERS');
    this.validateIdentity('SHOW USERS FOR GROUP \'group_name\'');
    this.validateIdentity('SHOW USERS FOR ROLE \'role_name\'');
  }

  testTruncate () {
    this.validateAll(
      'TRUNCATE t1; TRUNCATE t2',
      {
        read: {
          '': 'TRUNCATE TABLE t1, t2',
        },
      },
    );
  }

  testVector () {
    this.validateAll(
      'CREATE TABLE t (a VECTOR(10, I32))',
      {
        read: {
          snowflake: 'CREATE TABLE t (a VECTOR(INT, 10))',
          singlestore: 'CREATE TABLE t (a VECTOR(10, I32))',
        },
        write: {
          snowflake: 'CREATE TABLE t (a VECTOR(INT, 10))',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE t (a VECTOR(10))',
      {
        read: {
          snowflake: 'CREATE TABLE t (a VECTOR(10))',
          singlestore: 'CREATE TABLE t (a VECTOR(10))',
        },
        write: {
          snowflake: 'CREATE TABLE t (a VECTOR(10))',
        },
      },
    );
  }

  testAlter () {
    this.validateIdentity('ALTER TABLE t CHANGE middle_initial middle_name');
    this.validateIdentity('ALTER TABLE t MODIFY COLUMN name TEXT COLLATE \'binary\'');
  }

  testConstraints () {
    this.validateAll(
      'CREATE TABLE ComputedColumnConstraint (points INT, score AS (points * 2) PERSISTED AUTO NOT NULL)',
      {
        read: {
          '': 'CREATE TABLE ComputedColumnConstraint (points INT, score AS (points * 2) PERSISTED NOT NULL)',
          singlestore: 'CREATE TABLE ComputedColumnConstraint (points INT, score AS (points * 2) AUTO NOT NULL)',
        },
      },
    );
    this.validateIdentity(
      'CREATE TABLE ComputedColumnConstraint (points INT, score AS (points * 2) PERSISTED BIGINT NOT NULL)',
    );
  }

  testDcolonqmark () {
    this.validateIdentity('SELECT * FROM employee WHERE JSON_MATCH_ANY(payroll::?names)');
  }
}

const t = new TestSingleStore();

describe('TestSingleStore', () => {
  test('singlestore', () => t.testSinglestore());
  test('byteStrings', () => t.testByteStrings());
  test('nationalStrings', () => t.testNationalStrings());
  test('restrictedKeywords', () => t.testRestrictedKeywords());
  test('timeFormatting', () => t.testTimeFormatting());
  test('cast', () => t.testCast());
  test('unixFunctions', () => t.testUnixFunctions());
  test('jsonExtract', () => t.testJsonExtract());
  test('json', () => t.testJson());
  test('datePartsFunctions', () => t.testDatePartsFunctions());
  test('mathFunctions', () => t.testMathFunctions());
  test('logical', () => t.testLogical());
  test('stringFunctions', () => t.testStringFunctions());
  test('reduceFunctions', () => t.testReduceFunctions());
  test('timeFunctions', () => t.testTimeFunctions());
  test('types', () => t.testTypes());
  test('columnWithTablename', () => t.testColumnWithTablename());
  test('unicodestringSQL', () => t.testUnicodestringSQL());
  test('collateSQL', () => t.testCollateSQL());
  test('matchAgainst', () => t.testMatchAgainst());
  test('show', () => t.testShow());
  test('truncate', () => t.testTruncate());
  test('vector', () => t.testVector());
  test('alter', () => t.testAlter());
  test('constraints', () => t.testConstraints());
  test('dcolonqmark', () => t.testDcolonqmark());
});
