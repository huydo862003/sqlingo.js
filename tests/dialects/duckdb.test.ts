import {
  describe, test, expect,
} from 'vitest';
import { parseOne } from '../../src/index';
import {
  TruncExpr, ArrayOverlapsExpr,
  InstallExpr, ShowExpr,
  AnonymousExpr,
} from '../../src/expressions';
import {
  Validator, UnsupportedError,
} from './validator';

class TestDuckDB extends Validator {
  override dialect = 'duckdb' as const;

  testDuckdb () {
    this.validateIdentity('TRUNC(3.14)').assertIs(TruncExpr);
    this.validateIdentity('TRUNC(3.14, 2)', 'TRUNC(3.14)').assertIs(TruncExpr);
    this.validateAll('TRUNC(3.14159)', { read: { postgres: 'TRUNC(3.14159, 2)' } });

    this.validateIdentity('SELECT ([1,2,3])[:-:-1]', 'SELECT ([1, 2, 3])[:-1:-1]');
    this.validateIdentity('SELECT INTERVAL \'1 hour\'::VARCHAR', 'SELECT CAST(INTERVAL \'1\' HOUR AS TEXT)');
    this.validateIdentity(
      'PIVOT duckdb_functions() ON schema_name USING AVG(LENGTH(function_name))::INTEGER GROUP BY schema_name',
      'PIVOT DUCKDB_FUNCTIONS() ON schema_name USING CAST(AVG(LENGTH(function_name)) AS INT) GROUP BY schema_name',
    );
    this.validateIdentity('SELECT str[0:1]');
    this.validateIdentity('SELECT COSH(1.5)');
    this.validateIdentity('SELECT MODE(category)');
    this.validateIdentity('SELECT e\'\\n\'');
    this.validateIdentity('SELECT e\'\\t\'');
    this.validateIdentity(
      'SELECT e\'update table_name set a = \\\'foo\\\' where 1 = 0\' AS x FROM tab',
      'SELECT e\'update table_name set a = \'\'foo\'\' where 1 = 0\' AS x FROM tab',
    );

    expect(() => parseOne('1 //', { read: 'duckdb' })).toThrow();

    this.validateAll(
      '(c LIKE \'a\' OR c LIKE \'b\') AND other_cond',
      { read: { databricks: 'c LIKE ANY (\'a\', \'b\') AND other_cond' } },
    );
    this.validateAll(
      'SELECT FIRST_VALUE(c IGNORE NULLS) OVER (PARTITION BY gb ORDER BY ob) FROM t',
      {
        write: {
          duckdb: 'SELECT FIRST_VALUE(c IGNORE NULLS) OVER (PARTITION BY gb ORDER BY ob) FROM t',
          sqlite: UnsupportedError,
          mysql: UnsupportedError,
          postgres: UnsupportedError,
        },
      },
    );
    this.validateAll(
      'SELECT FIRST_VALUE(c RESPECT NULLS) OVER (PARTITION BY gb ORDER BY ob) FROM t',
      {
        write: {
          duckdb: 'SELECT FIRST_VALUE(c RESPECT NULLS) OVER (PARTITION BY gb ORDER BY ob) FROM t',
          sqlite: 'SELECT FIRST_VALUE(c) OVER (PARTITION BY gb ORDER BY ob NULLS LAST) FROM t',
          mysql: 'SELECT FIRST_VALUE(c) RESPECT NULLS OVER (PARTITION BY gb ORDER BY CASE WHEN ob IS NULL THEN 1 ELSE 0 END, ob) FROM t',
          postgres: UnsupportedError,
        },
      },
    );
    this.validateAll('CAST(x AS UUID)', {
      write: {
        bigquery: 'CAST(x AS STRING)',
        duckdb: 'CAST(x AS UUID)',
      },
    });
    this.validateAll('SELECT APPROX_TOP_K(category, 3) FROM t', {
      write: {
        snowflake: 'SELECT APPROX_TOP_K(category, 3) FROM t',
        duckdb: UnsupportedError,
      },
    });
    this.validateAll(
      'SELECT CASE WHEN JSON_VALID(\'{"x: 1}\') THEN \'{"x: 1}\' ELSE NULL END',
      {
        read: {
          duckdb: 'SELECT CASE WHEN JSON_VALID(\'{"x: 1}\') THEN \'{"x: 1}\' ELSE NULL END',
          snowflake: 'SELECT TRY_PARSE_JSON(\'{"x: 1}\')',
        },
      },
    );
    this.validateAll('SELECT straight_join', {
      write: {
        duckdb: 'SELECT straight_join',
        mysql: 'SELECT `straight_join`',
      },
    });
    this.validateAll('STRUCT_PACK("a b" := 1)', {
      write: {
        duckdb: '{\'a b\': 1}',
        spark: 'STRUCT(1 AS `a b`)',
        snowflake: 'OBJECT_CONSTRUCT(\'a b\', 1)',
      },
    });
    this.validateAll('ARRAY_TO_STRING(arr, delim)', {
      read: {
        bigquery: 'ARRAY_TO_STRING(arr, delim)',
        postgres: 'ARRAY_TO_STRING(arr, delim)',
        presto: 'ARRAY_JOIN(arr, delim)',
        snowflake: 'ARRAY_TO_STRING(arr, delim)',
        spark: 'ARRAY_JOIN(arr, delim)',
      },
      write: {
        bigquery: 'ARRAY_TO_STRING(arr, delim)',
        duckdb: 'ARRAY_TO_STRING(arr, delim)',
        postgres: 'ARRAY_TO_STRING(arr, delim)',
        presto: 'ARRAY_JOIN(arr, delim)',
        snowflake: 'ARRAY_TO_STRING(arr, delim)',
        spark: 'ARRAY_JOIN(arr, delim)',
        tsql: 'STRING_AGG(arr, delim)',
      },
    });
    this.validateAll('SELECT SUM(X) OVER (ORDER BY x)', {
      write: {
        bigquery: 'SELECT SUM(X) OVER (ORDER BY x)',
        duckdb: 'SELECT SUM(X) OVER (ORDER BY x)',
        mysql: 'SELECT SUM(X) OVER (ORDER BY CASE WHEN x IS NULL THEN 1 ELSE 0 END, x)',
      },
    });
    this.validateAll('SELECT SUM(X) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)', {
      write: {
        bigquery: 'SELECT SUM(X) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)',
        duckdb: 'SELECT SUM(X) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)',
        mysql: 'SELECT SUM(X) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)',
      },
    });
    this.validateAll('SELECT * FROM x ORDER BY 1 NULLS LAST', {
      write: {
        duckdb: 'SELECT * FROM x ORDER BY 1',
        mysql: 'SELECT * FROM x ORDER BY 1',
      },
    });
    this.validateAll('CREATE TEMPORARY FUNCTION f1(a, b) AS (a + b)', {
      read: { bigquery: 'CREATE TEMP FUNCTION f1(a INT64, b INT64) AS (a + b)' },
    });
    this.validateIdentity('SELECT GET_BIT(CAST(\'0110010\' AS BIT), 2)');
    this.validateIdentity('SELECT 1 WHERE x > $1');
    this.validateIdentity('SELECT 1 WHERE x > $name');
    this.validateIdentity('SELECT \'{"x": 1}\' -> c FROM t');

    this.validateAll('{\'a\': 1, \'b\': \'2\'}', {
      write: { presto: 'CAST(ROW(1, \'2\') AS ROW(a INTEGER, b VARCHAR))' },
    });
    this.validateAll('struct_pack(a := 1, b := 2)', {
      write: { presto: 'CAST(ROW(1, 2) AS ROW(a INTEGER, b INTEGER))' },
    });
    this.validateAll('struct_pack(a := 1, b := x)', {
      write: {
        duckdb: '{\'a\': 1, \'b\': x}',
        presto: UnsupportedError,
      },
    });

    for (const joinType of ['SEMI', 'ANTI'] as const) {
      const exists = joinType === 'SEMI' ? 'EXISTS' : 'NOT EXISTS';
      this.validateAll(`SELECT * FROM t1 ${joinType} JOIN t2 ON t1.x = t2.x`, {
        write: {
          bigquery: `SELECT * FROM t1 WHERE ${exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)`,
          clickhouse: `SELECT * FROM t1 ${joinType} JOIN t2 ON t1.x = t2.x`,
          databricks: `SELECT * FROM t1 ${joinType} JOIN t2 ON t1.x = t2.x`,
          doris: `SELECT * FROM t1 WHERE ${exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)`,
          drill: `SELECT * FROM t1 WHERE ${exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)`,
          duckdb: `SELECT * FROM t1 ${joinType} JOIN t2 ON t1.x = t2.x`,
          hive: `SELECT * FROM t1 ${joinType} JOIN t2 ON t1.x = t2.x`,
          mysql: `SELECT * FROM t1 WHERE ${exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)`,
          oracle: `SELECT * FROM t1 ${joinType} JOIN t2 ON t1.x = t2.x`,
          postgres: `SELECT * FROM t1 WHERE ${exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)`,
          presto: `SELECT * FROM t1 WHERE ${exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)`,
          redshift: `SELECT * FROM t1 WHERE ${exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)`,
          snowflake: `SELECT * FROM t1 WHERE ${exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)`,
          spark: `SELECT * FROM t1 ${joinType} JOIN t2 ON t1.x = t2.x`,
          sqlite: `SELECT * FROM t1 WHERE ${exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)`,
          starrocks: `SELECT * FROM t1 WHERE ${exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)`,
          teradata: `SELECT * FROM t1 WHERE ${exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)`,
          trino: `SELECT * FROM t1 WHERE ${exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)`,
          tsql: `SELECT * FROM t1 WHERE ${exists}(SELECT 1 FROM t2 WHERE t1.x = t2.x)`,
        },
      });
      this.validateAll(`SELECT * FROM t1 ${joinType} JOIN t2 ON t1.x = t2.x`, {
        read: {
          duckdb: `SELECT * FROM t1 ${joinType} JOIN t2 ON t1.x = t2.x`,
          spark: `SELECT * FROM t1 LEFT ${joinType} JOIN t2 ON t1.x = t2.x`,
        },
      });
    }

    this.validateIdentity('SELECT EXP(1)');
    this.validateIdentity('SELECT \'{"duck": [1, 2, 3]}\' -> \'$.duck[#-1]\'');

    this.validateAll('SELECT RANGE(1, 5)', {
      write: {
        duckdb: 'SELECT RANGE(1, 5)',
        spark: 'SELECT SEQUENCE(1, 4)',
      },
    });
    this.validateAll('SELECT RANGE(1, 5, 2)', {
      write: {
        duckdb: 'SELECT RANGE(1, 5, 2)',
        spark: 'SELECT SEQUENCE(1, 3, 2)',
      },
    });
    this.validateAll('SELECT RANGE(1, 1)', {
      write: {
        duckdb: 'SELECT RANGE(1, 1)',
        spark: 'SELECT ARRAY()',
      },
    });
    this.validateAll('SELECT RANGE(5, 1, -1)', {
      write: {
        duckdb: 'SELECT RANGE(5, 1, -1)',
        spark: 'SELECT SEQUENCE(5, 2, -1)',
      },
    });
    this.validateAll('SELECT RANGE(5, 1, 0)', {
      write: {
        duckdb: 'SELECT RANGE(5, 1, 0)',
        spark: 'SELECT ARRAY()',
      },
    });
    this.validateAll('WITH t AS (SELECT 5 AS c) SELECT RANGE(1, c) FROM t', {
      write: {
        duckdb: 'WITH t AS (SELECT 5 AS c) SELECT RANGE(1, c) FROM t',
        spark: 'WITH t AS (SELECT 5 AS c) SELECT IF((c - 1) <= 1, ARRAY(), SEQUENCE(1, (c - 1))) FROM t',
      },
    });
    this.validateAll('SELECT JSON_EXTRACT(\'{"duck": [1, 2, 3]}\', \'/duck/0\')', {
      write: {
        '': 'SELECT JSON_EXTRACT(\'{"duck": [1, 2, 3]}\', \'/duck/0\')',
        'duckdb': 'SELECT \'{"duck": [1, 2, 3]}\' -> \'/duck/0\'',
      },
    });
    this.validateAll('SELECT JSON(\'{"fruit":"banana"}\') -> \'fruit\'', {
      write: {
        duckdb: 'SELECT JSON(\'{"fruit":"banana"}\') -> \'$.fruit\'',
        snowflake: 'SELECT GET_PATH(PARSE_JSON(\'{"fruit":"banana"}\'), \'fruit\')',
      },
    });
    this.validateAll('SELECT JSON(\'{"fruit": {"foo": "banana"}}\') -> \'fruit\' -> \'foo\'', {
      write: {
        duckdb: 'SELECT JSON(\'{"fruit": {"foo": "banana"}}\') -> \'$.fruit\' -> \'$.foo\'',
        snowflake: 'SELECT GET_PATH(GET_PATH(PARSE_JSON(\'{"fruit": {"foo": "banana"}}\'), \'fruit\'), \'foo\')',
      },
    });
    this.validateAll(
      'SELECT {\'bla\': column1, \'foo\': column2, \'bar\': column3} AS data FROM source_table',
      {
        read: {
          bigquery: 'SELECT STRUCT(column1 AS bla, column2 AS foo, column3 AS bar) AS data FROM source_table',
          duckdb: 'SELECT {\'bla\': column1, \'foo\': column2, \'bar\': column3} AS data FROM source_table',
        },
        write: {
          bigquery: 'SELECT STRUCT(column1 AS bla, column2 AS foo, column3 AS bar) AS data FROM source_table',
        },
      },
    );
    this.validateAll(
      'WITH cte(x) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT AVG(x) FILTER (WHERE x > 1) FROM cte',
      {
        write: {
          duckdb: 'WITH cte(x) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT AVG(x) FILTER(WHERE x > 1) FROM cte',
          snowflake: 'WITH cte(x) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT AVG(IFF(x > 1, x, NULL)) FROM cte',
        },
      },
    );
    this.validateAll('SELECT AVG(x) FILTER (WHERE TRUE) FROM t', {
      write: {
        duckdb: 'SELECT AVG(x) FILTER(WHERE TRUE) FROM t',
        snowflake: 'SELECT AVG(IFF(TRUE, x, NULL)) FROM t',
      },
    });

    for (const joinType of [
      'LEFT',
      'LEFT OUTER',
      'INNER',
    ] as const) {
      this.validateAll(`SELECT * FROM x ${joinType} JOIN UNNEST(y) ON TRUE`, {
        read: { bigquery: `SELECT * FROM x ${joinType} JOIN UNNEST(y)` },
        write: {
          bigquery: `SELECT * FROM x ${joinType} JOIN UNNEST(y) ON TRUE`,
          duckdb: `SELECT * FROM x ${joinType} JOIN UNNEST(y) ON TRUE`,
        },
      });
    }

    this.validateIdentity(
      'SELECT \'{ "family": "anatidae", "species": [ "duck", "goose", "swan", null ] }\' ->> [\'$.family\', \'$.species\']',
    );
    this.validateIdentity('SELECT $🦆$foo$🦆$', 'SELECT \'foo\'');
    this.validateIdentity(
      'SELECT * FROM t PIVOT(FIRST(t) AS t, FOR quarter IN (\'Q1\', \'Q2\'))',
      'SELECT * FROM t PIVOT(FIRST(t) AS t FOR quarter IN (\'Q1\', \'Q2\'))',
    );
    this.validateIdentity(
      'SELECT JSON_EXTRACT_STRING(\'{ "family": "anatidae", "species": [ "duck", "goose", "swan", null ] }\', [\'$.family\', \'$.species\'])',
      'SELECT \'{ "family": "anatidae", "species": [ "duck", "goose", "swan", null ] }\' ->> [\'$.family\', \'$.species\']',
    );
    this.validateIdentity(
      'SELECT col FROM t WHERE JSON_EXTRACT_STRING(col, \'$.id\') NOT IN (\'b\')',
      'SELECT col FROM t WHERE NOT (col ->> \'$.id\') IN (\'b\')',
    );
    this.validateIdentity(
      'SELECT a, LOGICAL_OR(b) FROM foo GROUP BY a',
      'SELECT a, BOOL_OR(CAST(b AS BOOLEAN)) FROM foo GROUP BY a',
    );
    this.validateIdentity(
      'SELECT JSON_EXTRACT_STRING(c, \'$.k1\') = \'v1\'',
      'SELECT (c ->> \'$.k1\') = \'v1\'',
    );
    this.validateIdentity(
      'SELECT JSON_EXTRACT(c, \'$.k1\') = \'v1\'',
      'SELECT (c -> \'$.k1\') = \'v1\'',
    );
    this.validateIdentity(
      'SELECT JSON_EXTRACT(c, \'$[*].id\')[0:2]',
      'SELECT (c -> \'$[*].id\')[0:2]',
    );
    this.validateIdentity(
      'SELECT JSON_EXTRACT_STRING(c, \'$[*].id\')[0:2]',
      'SELECT (c ->> \'$[*].id\')[0:2]',
    );
    this.validateIdentity(
      'SELECT \'{"foo": [1, 2, 3]}\' -> \'foo\' -> 0',
      'SELECT \'{"foo": [1, 2, 3]}\' -> \'$.foo\' -> \'$[0]\'',
    );
    this.validateIdentity('SELECT ($$hello)\'world$$)', 'SELECT (\'hello)\'\'world\')');
    this.validateIdentity('SELECT $$foo$$', 'SELECT \'foo\'');
    this.validateIdentity('SELECT $tag$foo$tag$', 'SELECT \'foo\'');
    this.validateIdentity('JSON_EXTRACT(x, \'$.family\')', 'x -> \'$.family\'');
    this.validateIdentity('JSON_EXTRACT_PATH(x, \'$.family\')', 'x -> \'$.family\'');
    this.validateIdentity('JSON_EXTRACT_STRING(x, \'$.family\')', 'x ->> \'$.family\'');
    this.validateIdentity('JSON_EXTRACT_PATH_TEXT(x, \'$.family\')', 'x ->> \'$.family\'');
    this.validateAll('SELECT NOT (data -> \'$.value\')', {
      read: { snowflake: 'SELECT NOT data:value' },
    });
    this.validateAll('SELECT NOT (data -> \'$.value.nested\')', {
      read: { snowflake: 'SELECT NOT data:value:nested' },
    });
    this.validateAll('SELECT (data -> \'$.value\') = 1', {
      read: { snowflake: 'SELECT data:value = 1' },
    });
    this.validateIdentity('SELECT {\'yes\': \'duck\', \'maybe\': \'goose\', \'huh\': NULL, \'no\': \'heron\'}');
    this.validateIdentity('SELECT a[\'x space\'] FROM (SELECT {\'x space\': 1, \'y\': 2, \'z\': 3} AS a)');
    this.validateIdentity('PIVOT Cities ON Year IN (2000, 2010) USING SUM(Population) GROUP BY Country');
    this.validateIdentity('PIVOT Cities ON Year USING SUM(Population) AS total, MAX(Population) AS max GROUP BY Country');
    this.validateIdentity('WITH pivot_alias AS (PIVOT Cities ON Year USING SUM(Population) GROUP BY Country) SELECT * FROM pivot_alias');
    this.validateIdentity('SELECT * FROM (PIVOT Cities ON Year USING SUM(Population) GROUP BY Country) AS pivot_alias');
    this.validateIdentity('SELECT * FROM cities PIVOT(SUM(population) FOR year IN (2000, 2010, 2020) GROUP BY country)');
    this.validateIdentity(
      'SELECT schema_name, function_name, ROW_NUMBER() OVER my_window AS function_rank FROM DUCKDB_FUNCTIONS() WINDOW my_window AS (PARTITION BY schema_name ORDER BY function_name) QUALIFY ROW_NUMBER() OVER my_window < 3',
    );
    this.validateIdentity('DATE_SUB(\'YEAR\', col, \'2020-01-01\')').assertIs(AnonymousExpr);
    this.validateIdentity('DATESUB(\'YEAR\', col, \'2020-01-01\')').assertIs(AnonymousExpr);
    this.validateIdentity('SELECT SHA256(\'abc\')');

    this.validateAll('0b1010', { write: { '': '0 AS b1010' } });
    this.validateAll('0x1010', { write: { '': '0 AS x1010' } });
    this.validateIdentity('x ~ y', 'REGEXP_FULL_MATCH(x, y)');
    this.validateIdentity('x !~ y', 'NOT REGEXP_FULL_MATCH(x, y)');
    this.validateIdentity('REGEXP_FULL_MATCH(x, y, \'i\')');
    this.validateAll('SELECT * FROM \'x.y\'', { write: { duckdb: 'SELECT * FROM "x.y"' } });
    this.validateAll('SELECT LIST(DISTINCT sample_col) FROM sample_table', {
      read: {
        duckdb: 'SELECT LIST(DISTINCT sample_col) FROM sample_table',
        spark: 'SELECT COLLECT_SET(sample_col) FROM sample_table',
      },
    });
    this.validateAll(
      'SELECT LIST_TRANSFORM(STR_SPLIT_REGEX(\'abc , dfg \', \',\'), x -> TRIM(x))',
      {
        write: {
          duckdb: 'SELECT LIST_TRANSFORM(STR_SPLIT_REGEX(\'abc , dfg \', \',\'), x -> TRIM(x))',
          spark: 'SELECT TRANSFORM(SPLIT(\'abc , dfg \', \',\'), x -> TRIM(x))',
        },
      },
    );
    this.validateAll('SELECT LIST_FILTER([4, 5, 6], x -> x > 4)', {
      write: {
        duckdb: 'SELECT LIST_FILTER([4, 5, 6], x -> x > 4)',
        spark: 'SELECT FILTER(ARRAY(4, 5, 6), x -> x > 4)',
      },
    });
    this.validateAll('ARRAY_COMPACT([1, NULL, 2, NULL, 3])', {
      write: {
        duckdb: 'LIST_FILTER([1, NULL, 2, NULL, 3], _u -> NOT _u IS NULL)',
        snowflake: 'ARRAY_COMPACT([1, NULL, 2, NULL, 3])',
      },
    });
    this.validateAll('ARRAY_COMPACT(NULL)', {
      write: {
        duckdb: 'LIST_FILTER(NULL, _u -> NOT _u IS NULL)',
        snowflake: 'ARRAY_COMPACT(NULL)',
      },
    });
    this.validateAll('ARRAY_COMPACT([])', {
      write: {
        duckdb: 'LIST_FILTER([], _u -> NOT _u IS NULL)',
        snowflake: 'ARRAY_COMPACT([])',
      },
    });
    this.validateAll('ARRAY_COMPACT([\'a\', NULL, \'b\', NULL, \'c\'])', {
      write: {
        duckdb: 'LIST_FILTER([\'a\', NULL, \'b\', NULL, \'c\'], _u -> NOT _u IS NULL)',
        snowflake: 'ARRAY_COMPACT([\'a\', NULL, \'b\', NULL, \'c\'])',
      },
    });
    this.validateAll('ARRAY_COMPACT([[1, 2], NULL, [3, 4]])', {
      write: {
        duckdb: 'LIST_FILTER([[1, 2], NULL, [3, 4]], _u -> NOT _u IS NULL)',
        snowflake: 'ARRAY_COMPACT([[1, 2], NULL, [3, 4]])',
      },
    });
    this.validateAll('ARRAY_CONSTRUCT_COMPACT(1, 2, 3, 4, 5)', {
      write: {
        duckdb: 'LIST_FILTER([1, 2, 3, 4, 5], _u -> NOT _u IS NULL)',
        snowflake: 'ARRAY_CONSTRUCT_COMPACT(1, 2, 3, 4, 5)',
      },
    });
    this.validateAll('ARRAY_CONSTRUCT_COMPACT()', {
      write: {
        duckdb: 'LIST_FILTER([], _u -> NOT _u IS NULL)',
        snowflake: 'ARRAY_CONSTRUCT_COMPACT()',
      },
    });
    this.validateAll('ARRAY_CONSTRUCT_COMPACT(\'a\', NULL, \'b\', NULL, \'c\')', {
      write: {
        duckdb: 'LIST_FILTER([\'a\', NULL, \'b\', NULL, \'c\'], _u -> NOT _u IS NULL)',
        snowflake: 'ARRAY_CONSTRUCT_COMPACT(\'a\', NULL, \'b\', NULL, \'c\')',
      },
    });
    this.validateAll('SELECT ANY_VALUE(sample_column) FROM sample_table', {
      write: {
        duckdb: 'SELECT ANY_VALUE(sample_column) FROM sample_table',
        spark: 'SELECT ANY_VALUE(sample_column) IGNORE NULLS FROM sample_table',
      },
    });
    this.validateAll('COUNT_IF(x)', {
      write: {
        'duckdb': 'COUNT_IF(x)',
        'duckdb, version=1.0': 'SUM(CASE WHEN x THEN 1 ELSE 0 END)',
        'duckdb, version=1.2': 'COUNT_IF(x)',
      },
    });
    this.validateAll(
      'SELECT STRFTIME(CAST(\'2020-01-01\' AS TIMESTAMP), CONCAT(\'%Y\', \'%m\'))',
      {
        write: {
          duckdb: 'SELECT STRFTIME(CAST(\'2020-01-01\' AS TIMESTAMP), CONCAT(\'%Y\', \'%m\'))',
          spark: 'SELECT DATE_FORMAT(CAST(\'2020-01-01\' AS TIMESTAMP_NTZ), CONCAT(\'yyyy\', \'MM\'))',
          tsql: 'SELECT FORMAT(CAST(\'2020-01-01\' AS DATETIME2), CONCAT(\'yyyy\', \'MM\'))',
        },
      },
    );
    this.validateAll('SELECT CAST(\'{"x": 1}\' AS JSON)', {
      read: {
        duckdb: 'SELECT \'{"x": 1}\'::JSON',
        postgres: 'SELECT \'{"x": 1}\'::JSONB',
      },
    });
    this.validateAll('SELECT * FROM produce PIVOT(SUM(sales) FOR quarter IN (\'Q1\', \'Q2\'))', {
      read: {
        duckdb: 'SELECT * FROM produce PIVOT(SUM(sales) FOR quarter IN (\'Q1\', \'Q2\'))',
        snowflake: 'SELECT * FROM produce PIVOT(SUM(produce.sales) FOR produce.quarter IN (\'Q1\', \'Q2\'))',
      },
    });
    this.validateAll('VAR_POP(x)', {
      read: { '': 'VARIANCE_POP(x)' },
      write: {
        '': 'VARIANCE_POP(x)',
        'duckdb': 'VAR_POP(x)',
      },
    });
    this.validateAll('DATE_DIFF(\'DAY\', CAST(b AS DATE), CAST(a AS DATE))', {
      read: {
        duckdb: 'DATE_DIFF(\'day\', CAST(b AS DATE), CAST(a AS DATE))',
        hive: 'DATEDIFF(a, b)',
        spark: 'DATEDIFF(a, b)',
        spark2: 'DATEDIFF(a, b)',
      },
    });
    this.validateAll('XOR(a, b)', {
      read: {
        '': 'a ^ b',
        'bigquery': 'a ^ b',
        'presto': 'BITWISE_XOR(a, b)',
        'postgres': 'a # b',
      },
      write: {
        '': 'a ^ b',
        'bigquery': 'a ^ b',
        'duckdb': 'XOR(a, b)',
        'presto': 'BITWISE_XOR(a, b)',
        'postgres': 'a # b',
      },
    });
    this.validateAll('PIVOT_WIDER Cities ON Year USING SUM(Population)', {
      write: { duckdb: 'PIVOT Cities ON Year USING SUM(Population)' },
    });
    this.validateAll('WITH t AS (SELECT 1) FROM t', {
      write: { duckdb: 'WITH t AS (SELECT 1) SELECT * FROM t' },
    });
    this.validateAll('WITH t AS (SELECT 1) SELECT * FROM (FROM t)', {
      write: { duckdb: 'WITH t AS (SELECT 1) SELECT * FROM (SELECT * FROM t)' },
    });
    this.validateAll('SELECT DATEDIFF(\'day\', t1."A", t1."B") FROM "table" AS t1', {
      write: {
        duckdb: 'SELECT DATE_DIFF(\'DAY\', t1."A", t1."B") FROM "table" AS t1',
        trino: 'SELECT DATE_DIFF(\'DAY\', t1."A", t1."B") FROM "table" AS t1',
      },
    });
    this.validateAll('SELECT DATE_DIFF(\'day\', DATE \'2020-01-01\', DATE \'2020-01-05\')', {
      write: {
        duckdb: 'SELECT DATE_DIFF(\'DAY\', CAST(\'2020-01-01\' AS DATE), CAST(\'2020-01-05\' AS DATE))',
        trino: 'SELECT DATE_DIFF(\'DAY\', CAST(\'2020-01-01\' AS DATE), CAST(\'2020-01-05\' AS DATE))',
      },
    });
    this.validateAll('WITH \'x\' AS (SELECT 1) SELECT * FROM x', {
      write: { duckdb: 'WITH "x" AS (SELECT 1) SELECT * FROM x' },
    });
    this.validateAll(
      'CREATE TABLE IF NOT EXISTS t (cola INT, colb STRING) USING ICEBERG PARTITIONED BY (colb)',
      { write: { duckdb: 'CREATE TABLE IF NOT EXISTS t (cola INT, colb TEXT)' } },
    );
    this.validateAll(
      'CREATE TABLE IF NOT EXISTS t (cola INT COMMENT \'cola\', colb STRING) USING ICEBERG PARTITIONED BY (colb)',
      { write: { duckdb: 'CREATE TABLE IF NOT EXISTS t (cola INT, colb TEXT)' } },
    );
    this.validateAll('[0, 1, 2]', {
      read: { spark: 'ARRAY(0, 1, 2)' },
      write: {
        bigquery: '[0, 1, 2]',
        duckdb: '[0, 1, 2]',
        presto: 'ARRAY[0, 1, 2]',
        spark: 'ARRAY(0, 1, 2)',
      },
    });
    this.validateAll('SELECT ARRAY_LENGTH([0], 1) AS x', {
      write: { duckdb: 'SELECT ARRAY_LENGTH([0], 1) AS x' },
    });
    this.validateIdentity('REGEXP_REPLACE(this, pattern, replacement, modifiers)');
    this.validateIdentity(
      'SELECT NTH_VALUE(is_deleted, 2) OVER (PARTITION BY id) AS nth_is_deleted FROM my_table',
    );
    this.validateIdentity(
      'SELECT NTH_VALUE(is_deleted, 2 IGNORE NULLS) OVER (PARTITION BY id) AS nth_is_deleted FROM my_table',
    );
    this.validateAll('REGEXP_MATCHES(x, y)', {
      write: {
        duckdb: 'REGEXP_MATCHES(x, y)',
        presto: 'REGEXP_LIKE(x, y)',
        hive: 'x RLIKE y',
        spark: 'x RLIKE y',
      },
    });
    this.validateAll('STR_SPLIT(x, \'a\')', {
      write: {
        duckdb: 'STR_SPLIT(x, \'a\')',
        presto: 'SPLIT(x, \'a\')',
        hive: 'SPLIT(x, CONCAT(\'\\\\Q\', \'a\', \'\\\\E\'))',
        spark: 'SPLIT(x, CONCAT(\'\\\\Q\', \'a\', \'\\\\E\'))',
      },
    });
    this.validateAll('STRING_TO_ARRAY(x, \'a\')', {
      read: { snowflake: 'STRTOK_TO_ARRAY(x, \'a\')' },
      write: {
        duckdb: 'STR_SPLIT(x, \'a\')',
        presto: 'SPLIT(x, \'a\')',
        hive: 'SPLIT(x, CONCAT(\'\\\\Q\', \'a\', \'\\\\E\'))',
        spark: 'SPLIT(x, CONCAT(\'\\\\Q\', \'a\', \'\\\\E\'))',
      },
    });
    this.validateAll('STR_SPLIT_REGEX(x, \'a\')', {
      write: {
        duckdb: 'STR_SPLIT_REGEX(x, \'a\')',
        presto: 'REGEXP_SPLIT(x, \'a\')',
        hive: 'SPLIT(x, \'a\')',
        spark: 'SPLIT(x, \'a\')',
      },
    });
    this.validateAll('STRUCT_EXTRACT(x, \'abc\')', {
      write: {
        duckdb: 'STRUCT_EXTRACT(x, \'abc\')',
        presto: 'x.abc',
        hive: 'x.abc',
        postgres: 'x.abc',
        redshift: 'x.abc',
        spark: 'x.abc',
      },
    });
    this.validateAll('STRUCT_EXTRACT(STRUCT_EXTRACT(x, \'y\'), \'abc\')', {
      write: {
        duckdb: 'STRUCT_EXTRACT(STRUCT_EXTRACT(x, \'y\'), \'abc\')',
        presto: 'x.y.abc',
        hive: 'x.y.abc',
        spark: 'x.y.abc',
      },
    });
    this.validateAll('QUANTILE(x, 0.5)', {
      write: {
        duckdb: 'QUANTILE(x, 0.5)',
        presto: 'APPROX_PERCENTILE(x, 0.5)',
        hive: 'PERCENTILE(x, 0.5)',
        spark: 'PERCENTILE(x, 0.5)',
      },
    });
    this.validateAll('UNNEST(x)', {
      read: { spark: 'EXPLODE(x)' },
      write: {
        duckdb: 'UNNEST(x)',
        spark: 'EXPLODE(x)',
      },
    });
    this.validateAll('1d', {
      write: {
        duckdb: '1 AS d',
        spark: '1 AS d',
      },
    });
    this.validateAll('POWER(TRY_CAST(2 AS SMALLINT), 3)', {
      read: {
        hive: 'POW(2S, 3)',
        spark: 'POW(2S, 3)',
      },
    });
    this.validateAll('LIST_SUM([1, 2])', {
      read: { spark: 'ARRAY_SUM(ARRAY(1, 2))' },
    });
    this.validateAll('STRUCT_PACK(x := 1, y := \'2\')', {
      write: {
        bigquery: 'STRUCT(1 AS x, \'2\' AS y)',
        duckdb: '{\'x\': 1, \'y\': \'2\'}',
        spark: 'STRUCT(1 AS x, \'2\' AS y)',
      },
    });
    this.validateAll('STRUCT_PACK(key1 := \'value1\', key2 := 42)', {
      write: {
        bigquery: 'STRUCT(\'value1\' AS key1, 42 AS key2)',
        duckdb: '{\'key1\': \'value1\', \'key2\': 42}',
        spark: 'STRUCT(\'value1\' AS key1, 42 AS key2)',
      },
    });
    this.validateAll('ARRAY_REVERSE_SORT(x)', {
      write: {
        duckdb: 'ARRAY_REVERSE_SORT(x)',
        presto: 'ARRAY_SORT(x, (a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END)',
        hive: 'SORT_ARRAY(x, FALSE)',
        spark: 'SORT_ARRAY(x, FALSE)',
      },
    });
    this.validateAll('LIST_REVERSE_SORT(x)', {
      write: {
        duckdb: 'ARRAY_REVERSE_SORT(x)',
        presto: 'ARRAY_SORT(x, (a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END)',
        hive: 'SORT_ARRAY(x, FALSE)',
        spark: 'SORT_ARRAY(x, FALSE)',
      },
    });
    this.validateAll('LIST_SORT(x)', {
      write: {
        duckdb: 'ARRAY_SORT(x)',
        presto: 'ARRAY_SORT(x)',
        hive: 'SORT_ARRAY(x)',
        spark: 'SORT_ARRAY(x)',
      },
    });
    this.validateAll(
      'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname',
      {
        write: {
          '': 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname NULLS LAST',
          'duckdb': 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname',
        },
      },
    );
    this.validateAll('MONTH(\'2021-03-01\')', {
      write: {
        duckdb: 'MONTH(\'2021-03-01\')',
        presto: 'MONTH(\'2021-03-01\')',
        hive: 'MONTH(\'2021-03-01\')',
        spark: 'MONTH(\'2021-03-01\')',
      },
    });
    this.validateAll('LIST_CONCAT([1, 2], [3, 4])', {
      read: {
        bigquery: 'ARRAY_CONCAT([1, 2], [3, 4])',
        postgres: 'ARRAY_CAT(ARRAY[1, 2], ARRAY[3, 4])',
        snowflake: 'ARRAY_CAT([1, 2], [3, 4])',
      },
      write: {
        bigquery: 'ARRAY_CONCAT([1, 2], [3, 4])',
        duckdb: 'LIST_CONCAT([1, 2], [3, 4])',
        hive: 'CONCAT(ARRAY(1, 2), ARRAY(3, 4))',
        postgres: 'ARRAY_CAT(ARRAY[1, 2], ARRAY[3, 4])',
        presto: 'CONCAT(ARRAY[1, 2], ARRAY[3, 4])',
        snowflake: 'ARRAY_CAT([1, 2], [3, 4])',
        spark: 'CONCAT(ARRAY(1, 2), ARRAY(3, 4))',
      },
    });
    this.validateAll('SELECT CAST(TRY_CAST(x AS DATE) AS DATE) + INTERVAL 1 DAY', {
      read: { hive: 'SELECT DATE_ADD(TO_DATE(x), 1)' },
    });
    this.validateAll('SELECT CAST(\'2018-01-01 00:00:00\' AS DATE) + INTERVAL 3 DAY', {
      read: { hive: 'SELECT DATE_ADD(\'2018-01-01 00:00:00\', 3)' },
      write: {
        duckdb: 'SELECT CAST(\'2018-01-01 00:00:00\' AS DATE) + INTERVAL \'3\' DAY',
        hive: 'SELECT CAST(\'2018-01-01 00:00:00\' AS DATE) + INTERVAL \'3\' DAY',
      },
    });
    this.validateAll('SELECT CAST(\'2020-05-06\' AS DATE) - INTERVAL \'5\' DAY', {
      read: { bigquery: 'SELECT DATE_SUB(CAST(\'2020-05-06\' AS DATE), INTERVAL 5 DAY)' },
    });
    this.validateAll('SELECT CAST(\'2020-05-06\' AS DATE) + INTERVAL \'5\' DAY', {
      read: { bigquery: 'SELECT DATE_ADD(CAST(\'2020-05-06\' AS DATE), INTERVAL 5 DAY)' },
    });
    this.validateIdentity(
      'SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY y DESC) FROM t',
      'SELECT QUANTILE_CONT(y, 0.25 ORDER BY y DESC) FROM t',
    );
    this.validateIdentity(
      'SELECT PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY y DESC) FROM t',
      'SELECT QUANTILE_DISC(y, 0.25 ORDER BY y DESC) FROM t',
    );
    this.validateAll('SELECT QUANTILE_CONT(x, q) FROM t', {
      write: {
        duckdb: 'SELECT QUANTILE_CONT(x, q) FROM t',
        postgres: 'SELECT PERCENTILE_CONT(q) WITHIN GROUP (ORDER BY x) FROM t',
        snowflake: 'SELECT PERCENTILE_CONT(q) WITHIN GROUP (ORDER BY x) FROM t',
      },
    });
    this.validateAll('SELECT QUANTILE_DISC(x, q) FROM t', {
      write: {
        duckdb: 'SELECT QUANTILE_DISC(x, q) FROM t',
        postgres: 'SELECT PERCENTILE_DISC(q) WITHIN GROUP (ORDER BY x) FROM t',
        snowflake: 'SELECT PERCENTILE_DISC(q) WITHIN GROUP (ORDER BY x) FROM t',
      },
    });
    this.validateAll('SELECT REGEXP_EXTRACT(a, \'pattern\') FROM t', {
      read: {
        duckdb: 'SELECT REGEXP_EXTRACT(a, \'pattern\') FROM t',
        bigquery: 'SELECT REGEXP_EXTRACT(a, \'pattern\') FROM t',
        snowflake: 'SELECT REGEXP_SUBSTR(a, \'pattern\') FROM t',
      },
      write: {
        duckdb: 'SELECT REGEXP_EXTRACT(a, \'pattern\') FROM t',
        bigquery: 'SELECT REGEXP_EXTRACT(a, \'pattern\') FROM t',
        snowflake: 'SELECT REGEXP_SUBSTR(a, \'pattern\') FROM t',
      },
    });
    this.validateAll('SELECT REGEXP_EXTRACT(a, \'pattern\', 2, \'i\') FROM t', {
      read: { snowflake: 'SELECT REGEXP_SUBSTR(a, \'pattern\', 1, 1, \'i\', 2) FROM t' },
      write: {
        duckdb: 'SELECT REGEXP_EXTRACT(a, \'pattern\', 2, \'i\') FROM t',
        snowflake: 'SELECT REGEXP_SUBSTR(a, \'pattern\', 1, 1, \'i\', 2) FROM t',
      },
    });
    this.validateIdentity(
      'SELECT REGEXP_EXTRACT(a, \'pattern\', 0)',
      'SELECT REGEXP_EXTRACT(a, \'pattern\')',
    );
    this.validateIdentity('SELECT REGEXP_EXTRACT(a, \'pattern\', 0, \'i\')');
    this.validateIdentity('SELECT REGEXP_EXTRACT(a, \'pattern\', 1, \'i\')');
    this.validateIdentity('SELECT ISNAN(x)');
    this.validateAll('SELECT COUNT_IF(x)', {
      write: {
        duckdb: 'SELECT COUNT_IF(x)',
        bigquery: 'SELECT COUNTIF(x)',
      },
    });
    this.validateIdentity('SELECT * FROM RANGE(1, 5, 10)');
    this.validateIdentity('SELECT * FROM GENERATE_SERIES(2, 13, 4)');
    this.validateAll(
      'WITH t AS (SELECT i, i * i * i * i * i AS i5 FROM RANGE(1, 5) t(i)) SELECT * FROM t',
      {
        write: {
          duckdb: 'WITH t AS (SELECT i, i * i * i * i * i AS i5 FROM RANGE(1, 5) AS t(i)) SELECT * FROM t',
          sqlite: 'WITH t AS (SELECT i, i * i * i * i * i AS i5 FROM (SELECT value AS i FROM GENERATE_SERIES(1, 5)) AS t) SELECT * FROM t',
        },
      },
    );
    this.validateIdentity(
      'SELECT i FROM RANGE(5) AS _(i) ORDER BY i ASC',
      'SELECT i FROM RANGE(0, 5) AS _(i) ORDER BY i ASC',
    );
    this.validateIdentity(
      'SELECT i FROM GENERATE_SERIES(12) AS _(i) ORDER BY i ASC',
      'SELECT i FROM GENERATE_SERIES(0, 12) AS _(i) ORDER BY i ASC',
    );
    this.validateIdentity(
      'COPY lineitem FROM \'lineitem.ndjson\' WITH (FORMAT JSON, DELIMITER \',\', AUTO_DETECT TRUE, COMPRESSION SNAPPY, CODEC ZSTD, FORCE_NOT_NULL (col1, col2))',
    );
    this.validateIdentity(
      'COPY (SELECT 42 AS a, \'hello\' AS b) TO \'query.json\' WITH (FORMAT JSON, ARRAY TRUE)',
    );
    this.validateIdentity('COPY lineitem (l_orderkey) TO \'orderkey.tbl\' WITH (DELIMITER \'|\')');
    this.validateAll('VARIANCE(a)', {
      write: {
        duckdb: 'VARIANCE(a)',
        clickhouse: 'varSamp(a)',
      },
    });
    this.validateAll('STDDEV(a)', {
      write: {
        duckdb: 'STDDEV(a)',
        clickhouse: 'stddevSamp(a)',
      },
    });
    this.validateAll('DATE_TRUNC(\'DAY\', x)', {
      write: {
        duckdb: 'DATE_TRUNC(\'DAY\', x)',
        clickhouse: 'dateTrunc(\'DAY\', x)',
      },
    });
    this.validateIdentity('EDITDIST3(col1, col2)', 'LEVENSHTEIN(col1, col2)');
    this.validateIdentity('JARO_WINKLER_SIMILARITY(\'hello\', \'world\')');
    this.validateIdentity('SELECT LENGTH(foo)');
    this.validateIdentity('SELECT ARRAY[1, 2, 3]', 'SELECT [1, 2, 3]');
    this.validateIdentity('SELECT * FROM (DESCRIBE t)');
    this.validateIdentity('SELECT UNNEST([*COLUMNS(\'alias_.*\')]) AS column_name');
    this.validateIdentity(
      'SELECT COALESCE(*COLUMNS(*)) FROM (SELECT NULL, 2, 3) AS t(a, b, c)',
    );
    this.validateIdentity(
      'SELECT id, STRUCT_PACK(*COLUMNS(\'m\\d\')) AS measurements FROM many_measurements',
      'SELECT id, {\'_0\': *COLUMNS(\'m\\d\')} AS measurements FROM many_measurements',
    );
    this.validateIdentity('SELECT COLUMNS(c -> c LIKE \'%num%\') FROM numbers');
    this.validateIdentity(
      'SELECT MIN(COLUMNS(* REPLACE (number + id AS number))), COUNT(COLUMNS(* EXCLUDE (number))) FROM numbers',
    );
    this.validateIdentity('SELECT COLUMNS(*) + COLUMNS(*) FROM numbers');
    this.validateIdentity('SELECT COLUMNS(\'(id|numbers?)\') FROM numbers');
    this.validateIdentity(
      'SELECT COALESCE(COLUMNS([\'a\', \'b\', \'c\'])) AS result FROM (SELECT NULL AS a, 42 AS b, TRUE AS c)',
    );
    this.validateIdentity(
      'SELECT COALESCE(*COLUMNS([\'a\', \'b\', \'c\'])) AS result FROM (SELECT NULL AS a, 42 AS b, TRUE AS c)',
    );
    this.validateAll('SELECT UNNEST(foo) AS x', {
      write: { redshift: UnsupportedError },
    });
    this.validateIdentity('a ^ b', 'POWER(a, b)');
    this.validateIdentity('a ** b', 'POWER(a, b)');
    this.validateIdentity('a ~~~ b', 'a GLOB b');
    this.validateIdentity('a ~~ b', 'a LIKE b');
    this.validateIdentity('a @> b');
    this.validateIdentity('a <@ b', 'b @> a');
    this.validateIdentity('a && b').assertIs(ArrayOverlapsExpr);
    this.validateIdentity('a ^@ b', 'STARTS_WITH(a, b)');
    this.validateIdentity('a !~~ b', 'NOT a LIKE b');
    this.validateIdentity('a !~~* b', 'NOT a ILIKE b');
    this.validateAll('SELECT e\'Hello\nworld\'', {
      write: {
        duckdb: 'SELECT e\'Hello\\nworld\'',
        bigquery: 'SELECT CAST(b\'Hello\\nworld\' AS STRING)',
      },
    });
    this.validateAll('SELECT REGEXP_MATCHES(\'ThOmAs\', \'thomas\', \'i\')', {
      read: { postgres: 'SELECT \'ThOmAs\' ~* \'thomas\'' },
    });
    this.validateIdentity(
      'SELECT DATE_ADD(CAST(\'2020-01-01\' AS DATE), INTERVAL 1 DAY)',
      'SELECT CAST(\'2020-01-01\' AS DATE) + INTERVAL \'1\' DAY',
    );
    this.validateIdentity('ARRAY_SLICE(x, 1, 3, 2)');
    this.validateIdentity('SELECT #2, #1 FROM (VALUES (1, \'foo\'))');
    this.validateIdentity('SELECT #2 AS a, #1 AS b FROM (VALUES (1, \'foo\'))');
    this.validateAll('LIST_CONTAINS([1, 2, NULL], 1)', {
      write: {
        duckdb: 'ARRAY_CONTAINS([1, 2, NULL], 1)',
        postgres: 'CASE WHEN 1 IS NULL THEN NULL ELSE COALESCE(1 = ANY(ARRAY[1, 2, NULL]), FALSE) END',
      },
    });
    this.validateAll('LIST_CONTAINS([1, 2, NULL], NULL)', {
      write: {
        duckdb: 'ARRAY_CONTAINS([1, 2, NULL], NULL)',
        postgres: 'CASE WHEN NULL IS NULL THEN NULL ELSE COALESCE(NULL = ANY(ARRAY[1, 2, NULL]), FALSE) END',
      },
    });
    this.validateAll('LIST_HAS_ANY([1, 2, 3], [1,2])', {
      write: {
        duckdb: '[1, 2, 3] && [1, 2]',
        postgres: 'ARRAY[1, 2, 3] && ARRAY[1, 2]',
      },
    });
    this.validateIdentity('LISTAGG(x, \', \')');
    this.validateIdentity('STRING_AGG(x, \', \')', 'LISTAGG(x, \', \')');
    this.validateAll('SELECT CONCAT(foo)', {
      write: {
        duckdb: 'SELECT CONCAT(foo)',
        spark: 'SELECT CONCAT(COALESCE(foo, \'\'))',
      },
    });
    this.validateAll('SELECT CONCAT(COALESCE([\'abc\'], []), [\'bcg\'])', {
      write: {
        duckdb: 'SELECT CONCAT(COALESCE([\'abc\'], []), [\'bcg\'])',
        spark: 'SELECT CONCAT(COALESCE(ARRAY(\'abc\'), ARRAY()), ARRAY(\'bcg\'))',
      },
    });
    this.validateIdentity('SELECT CUME_DIST( ORDER BY foo) OVER (ORDER BY 1) FROM (SELECT 1 AS foo)');
    this.validateIdentity('SELECT NTILE(1 ORDER BY foo) OVER (ORDER BY 1) FROM (SELECT 1 AS foo)');
    this.validateIdentity('SELECT RANK( ORDER BY foo) OVER (ORDER BY 1) FROM (SELECT 1 AS foo)');
    this.validateIdentity('SELECT PERCENT_RANK( ORDER BY foo) OVER (ORDER BY 1) FROM (SELECT 1 AS foo)');
    this.validateIdentity('LIST_COSINE_DISTANCE(x, y)');
    this.validateIdentity('LIST_DISTANCE(x, y)');
    this.validateIdentity('SELECT * FROM t LIMIT 10 PERCENT');
    this.validateIdentity('SELECT * FROM t LIMIT 10%', 'SELECT * FROM t LIMIT 10 PERCENT');
    this.validateIdentity('SELECT * FROM t LIMIT 10 PERCENT OFFSET 1');
    this.validateIdentity('SELECT * FROM t LIMIT 10% OFFSET 1', 'SELECT * FROM t LIMIT 10 PERCENT OFFSET 1');
    this.validateIdentity(
      'SELECT CAST(ROW(1, 2) AS ROW(a INTEGER, b INTEGER))',
      'SELECT CAST(ROW(1, 2) AS STRUCT(a INT, b INT))',
    );
    this.validateIdentity('SELECT row');
    this.validateIdentity(
      'SELECT TRY_STRPTIME(\'2013-04-28T20:57:01.123456789+07:00\', \'%Y-%m-%dT%H:%M:%S.%n%z\')',
    );
    this.validateIdentity(
      'DELETE FROM t USING (VALUES (1)) AS t1(c), (VALUES (1), (2)) AS t2(c) WHERE t.c = t1.c AND t.c = t2.c',
    );
    this.validateIdentity(
      'FROM (FROM t1 UNION FROM t2)',
      'SELECT * FROM (SELECT * FROM t1 UNION SELECT * FROM t2)',
    );
    this.validateIdentity(
      'FROM (FROM (SELECT 1) AS t2(c), (SELECT t2.c AS c0))',
      'SELECT * FROM (SELECT * FROM (SELECT 1) AS t2(c), (SELECT t2.c AS c0))',
    );
    this.validateIdentity(
      'FROM (FROM (SELECT 2000 as amount) t GROUP BY amount HAVING SUM(amount) > 1000)',
      'SELECT * FROM (SELECT * FROM (SELECT 2000 AS amount) AS t GROUP BY amount HAVING SUM(amount) > 1000)',
    );
    this.validateIdentity(
      '(FROM (SELECT 1) t1(c) EXCEPT FROM (SELECT 2) t2(c)) UNION ALL (FROM (SELECT 3) t3(c) EXCEPT FROM (SELECT 4) t4(c))',
      '(SELECT * FROM (SELECT 1) AS t1(c) EXCEPT SELECT * FROM (SELECT 2) AS t2(c)) UNION ALL (SELECT * FROM (SELECT 3) AS t3(c) EXCEPT SELECT * FROM (SELECT 4) AS t4(c))',
    );

    for (const option of [
      'ORDER BY 1',
      'LIMIT 1',
      'OFFSET 1',
      'ORDER BY 1 LIMIT 1',
      'ORDER BY 1 OFFSET 1',
      'ORDER BY 1 LIMIT 1 OFFSET 1',
      'LIMIT 1 OFFSET 1',
    ]) {
      this.validateIdentity(
        `SELECT 1 FROM (SELECT 1) AS t(c) WHERE ((VALUES (1), (c) ${option}) INTERSECT (SELECT 1))`,
      );
    }

    this.validateIdentity('FORMAT(\'foo\')');
    this.validateIdentity('FORMAT(\'foo\', \'foo2\', \'foo3\')');
    this.validateAll('SELECT UUID()', {
      write: {
        duckdb: 'SELECT UUID()',
        bigquery: 'SELECT GENERATE_UUID()',
      },
    });
    this.validateIdentity('SELECT REPLACE(\'apple pie\', \'pie\', \'cobbler\') AS result');
    this.validateIdentity(
      'SELECT REPLACE(CAST(CAST(\'apple pie\' AS BLOB) AS TEXT), CAST(CAST(\'pie\' AS BLOB) AS TEXT), CAST(CAST(\'cobbler\' AS BLOB) AS TEXT)) AS result',
    );
    this.validateIdentity('SELECT TRIM(\'***apple***\', \'*\') AS result');
    this.validateIdentity(
      'SELECT CAST(TRIM(CAST(CAST(\'***apple***\' AS BLOB) AS TEXT), CAST(CAST(\'*\' AS BLOB) AS TEXT)) AS BLOB) AS result',
    );
    this.validateIdentity('SELECT GREATEST(1.0, 2.5, NULL, 3.7)');
    this.validateIdentity('FROM t1, t2 SELECT *', 'SELECT * FROM t1, t2');
    this.validateIdentity('ROUND(2.256, 1)');
    this.validateIdentity('SELECT MAKE_DATE(DATE_PART([\'year\', \'month\', \'day\'], TODAY()))');
    this.validateIdentity('SELECT * FROM t PIVOT(SUM(y) FOR foo IN y_enum)');
    this.validateIdentity('SELECT 20_000 AS literal');
    this.validateIdentity('SELECT 1_2E+1_0::FLOAT', 'SELECT CAST(1_2E+1_0 AS REAL)');

    this.validateAll(
      'CASE WHEN 2500 > 0 THEN ((2500 - 1) // 32768) + 1 ELSE 2500 // 32768 END',
      { read: { snowflake: 'BITMAP_BUCKET_NUMBER(2500)' } },
    );
    this.validateAll(
      'CASE WHEN 32768 > 0 THEN ((32768 - 1) // 32768) + 1 ELSE 32768 // 32768 END',
      { read: { snowflake: 'BITMAP_BUCKET_NUMBER(32768)' } },
    );
    this.validateAll(
      'CASE WHEN 32769 > 0 THEN ((32769 - 1) // 32768) + 1 ELSE 32769 // 32768 END',
      { read: { snowflake: 'BITMAP_BUCKET_NUMBER(32769)' } },
    );
    this.validateAll(
      'CASE WHEN -100 > 0 THEN ((-100 - 1) // 32768) + 1 ELSE -100 // 32768 END',
      { read: { snowflake: 'BITMAP_BUCKET_NUMBER(-100)' } },
    );
    this.validateAll(
      'CASE WHEN NULL > 0 THEN ((NULL - 1) // 32768) + 1 ELSE NULL // 32768 END',
      { read: { snowflake: 'BITMAP_BUCKET_NUMBER(NULL)' } },
    );

    this.validateIdentity('SELECT [1, 2, 3][1 + 1:LENGTH([1, 2, 3]) + -1]');
    this.validateIdentity('VERSION()');
  }

  testArrayIndex () {
    this.validateAll('SELECT some_arr[1] AS first FROM blah', {
      read: { bigquery: 'SELECT some_arr[0] AS first FROM blah' },
      write: {
        bigquery: 'SELECT some_arr[0] AS first FROM blah',
        duckdb: 'SELECT some_arr[1] AS first FROM blah',
        presto: 'SELECT some_arr[1] AS first FROM blah',
      },
    });
    this.validateIdentity('[x.STRING_SPLIT(\' \')[i] FOR x IN [\'1\', \'2\', 3] IF x.CONTAINS(\'1\')]');
    this.validateIdentity('SELECT [4, 5, 6] AS l, [x FOR x, i IN l IF i = 2] AS filtered');
    this.validateIdentity('SELECT LIST_VALUE(1)[i]', 'SELECT [1][i]');
    this.validateIdentity('{\'x\': LIST_VALUE(1)[i]}', '{\'x\': [1][i]}');
    this.validateIdentity(
      'SELECT LIST_APPLY(RANGE(1, 4), i -> {\'f1\': LIST_VALUE(1, 2, 3)[i], \'f2\': LIST_VALUE(1, 2, 3)[i]})',
      'SELECT LIST_APPLY(RANGE(1, 4), i -> {\'f1\': [1, 2, 3][i], \'f2\': [1, 2, 3][i]})',
    );
  }

  testArrayInsert () {
    this.validateAll(
      'CASE WHEN [1, 2, 3] IS NULL THEN NULL ELSE LIST_CONCAT([99], [1, 2, 3]) END',
      {
        read: {
          '': 'ARRAY_INSERT([1, 2, 3], 0, 99)',
          'snowflake': 'ARRAY_INSERT([1, 2, 3], 0, 99)',
          'spark': 'ARRAY_INSERT(ARRAY(1, 2, 3), 1, 99)',
        },
      },
    );
    this.validateAll(
      'CASE WHEN [1, 2, 3] IS NULL THEN NULL ELSE LIST_CONCAT([1, 2, 3][1:1], [99], [1, 2, 3][2:]) END',
      {
        read: {
          '': 'ARRAY_INSERT([1, 2, 3], 1, 99)',
          'snowflake': 'ARRAY_INSERT([1, 2, 3], 1, 99)',
          'spark': 'ARRAY_INSERT(ARRAY(1, 2, 3), 2, 99)',
        },
      },
    );
    this.validateAll(
      'CASE WHEN [1, 2, 3] IS NULL THEN NULL ELSE LIST_CONCAT([1, 2, 3][1:3], [99], [1, 2, 3][4:]) END',
      {
        read: {
          '': 'ARRAY_INSERT([1, 2, 3], 3, 99)',
          'snowflake': 'ARRAY_INSERT([1, 2, 3], 3, 99)',
          'spark': 'ARRAY_INSERT(ARRAY(1, 2, 3), 4, 99)',
        },
      },
    );
    this.validateAll(
      'CASE WHEN [1, 2, 3] IS NULL THEN NULL ELSE LIST_CONCAT([1, 2, 3][1:LENGTH([1, 2, 3]) + -1], [99], [1, 2, 3][LENGTH([1, 2, 3]) + -1 + 1:]) END',
      {
        read: {
          '': 'ARRAY_INSERT([1, 2, 3], -1, 99)',
          'snowflake': 'ARRAY_INSERT([1, 2, 3], -1, 99)',
          'spark': 'ARRAY_INSERT(ARRAY(1, 2, 3), -2, 99)',
        },
      },
    );
  }

  testArrayRemove () {
    this.validateAll(
      'CASE WHEN target IS NULL THEN NULL ELSE LIST_FILTER(the_array, _u -> _u <> target) END',
      { read: { snowflake: 'ARRAY_REMOVE(the_array, target)' } },
    );
    this.validateAll('LIST_FILTER([1, 2, 3], _u -> _u <> 2)', {
      read: { snowflake: 'ARRAY_REMOVE([1, 2, 3], 2)' },
    });
    this.validateAll(
      'CASE WHEN NULL IS NULL THEN NULL ELSE LIST_FILTER([1, 2, 3], _u -> _u <> NULL) END',
      { read: { snowflake: 'ARRAY_REMOVE([1, 2, 3], NULL)' } },
    );
  }

  testArrayRemoveAt () {
    this.validateAll('CASE WHEN [1, 2, 3] IS NULL THEN NULL ELSE [1, 2, 3][2:] END', {
      read: { snowflake: 'ARRAY_REMOVE_AT([1, 2, 3], 0)' },
    });
    this.validateAll(
      'CASE WHEN [1, 2, 3] IS NULL THEN NULL ELSE LIST_CONCAT([1, 2, 3][1:1], [1, 2, 3][3:]) END',
      { read: { snowflake: 'ARRAY_REMOVE_AT([1, 2, 3], 1)' } },
    );
    this.validateAll(
      'CASE WHEN [1, 2, 3] IS NULL THEN NULL ELSE LIST_CONCAT([1, 2, 3][1:2], [1, 2, 3][4:]) END',
      { read: { snowflake: 'ARRAY_REMOVE_AT([1, 2, 3], 2)' } },
    );
    this.validateAll(
      'CASE WHEN [1, 2, 3] IS NULL THEN NULL ELSE [1, 2, 3][1:LENGTH([1, 2, 3]) + -1] END',
      { read: { snowflake: 'ARRAY_REMOVE_AT([1, 2, 3], -1)' } },
    );
    this.validateAll(
      'CASE WHEN [1, 2, 3] IS NULL THEN NULL ELSE LIST_CONCAT([1, 2, 3][1:LENGTH([1, 2, 3]) + -2], [1, 2, 3][LENGTH([1, 2, 3]) + -2 + 2:]) END',
      { read: { snowflake: 'ARRAY_REMOVE_AT([1, 2, 3], -2)' } },
    );
    this.validateAll('CASE WHEN [99] IS NULL THEN NULL ELSE [99][2:] END', {
      read: { snowflake: 'ARRAY_REMOVE_AT([99], 0)' },
    });
    this.validateAll('CASE WHEN arr IS NULL THEN NULL ELSE arr[2:] END', {
      read: { snowflake: 'ARRAY_REMOVE_AT(arr, 0)' },
    });
    this.validateAll('ARRAY_REMOVE_AT([1, 2, 3], pos)', {
      read: { snowflake: 'ARRAY_REMOVE_AT([1, 2, 3], pos)' },
    });
  }

  testTime () {
    this.validateIdentity('SELECT CURRENT_DATE');
    this.validateIdentity('SELECT CURRENT_TIMESTAMP');
    this.validateAll('SELECT CAST(CURRENT_TIMESTAMP AT TIME ZONE \'UTC\' AS DATE)', {
      read: {
        bigquery: 'SELECT CURRENT_DATE(\'UTC\')',
        duckdb: 'SELECT CAST(CURRENT_TIMESTAMP AT TIME ZONE \'UTC\' AS DATE)',
      },
    });
    this.validateAll('SELECT MAKE_DATE(2016, 12, 25)', {
      read: { bigquery: 'SELECT DATE(2016, 12, 25)' },
      write: {
        bigquery: 'SELECT DATE(2016, 12, 25)',
        duckdb: 'SELECT MAKE_DATE(2016, 12, 25)',
      },
    });
    this.validateAll('SELECT CAST(CAST(\'2016-12-25 23:59:59\' AS TIMESTAMP) AS DATE)', {
      read: { bigquery: 'SELECT DATE(DATETIME \'2016-12-25 23:59:59\')' },
    });
    this.validateAll(
      'SELECT CAST(CAST(CAST(\'2016-12-25\' AS TIMESTAMPTZ) AS TIMESTAMP) AT TIME ZONE \'UTC\' AT TIME ZONE \'America/Los_Angeles\' AS DATE)',
      { read: { bigquery: 'SELECT DATE(TIMESTAMP \'2016-12-25\', \'America/Los_Angeles\')' } },
    );
    this.validateAll(
      'SELECT CAST(CAST(\'2024-01-15 23:30:00\' AS TIMESTAMP) AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Berlin\' AS DATE)',
      { read: { bigquery: 'SELECT DATE(\'2024-01-15 23:30:00\', \'Europe/Berlin\')' } },
    );
    this.validateAll(
      'SELECT CAST(CAST(STRPTIME(\'05/06/2020\', \'%m/%d/%Y\') AS DATE) AS DATE)',
      { read: { bigquery: 'SELECT DATE(PARSE_DATE(\'%m/%d/%Y\', \'05/06/2020\'))' } },
    );
    this.validateAll('SELECT CAST(\'2020-01-01\' AS DATE) + INTERVAL \'-1\' DAY', {
      read: { mysql: 'SELECT DATE \'2020-01-01\' + INTERVAL -1 DAY' },
    });
    this.validateAll('SELECT INTERVAL \'1 quarter\'', {
      write: { duckdb: 'SELECT INTERVAL \'1\' QUARTER' },
    });
    this.validateAll(
      'SELECT ((DATE_TRUNC(\'DAY\', CAST(CAST(DATE_TRUNC(\'DAY\', CURRENT_TIMESTAMP) AS DATE) AS TIMESTAMP) + INTERVAL (0 - ((ISODOW(CAST(CAST(DATE_TRUNC(\'DAY\', CURRENT_TIMESTAMP) AS DATE) AS TIMESTAMP)) % 7) - 1 + 7) % 7) DAY) + INTERVAL (-5) WEEK)) AS t1',
      {
        read: {
          presto: 'SELECT ((DATE_ADD(\'week\', -5, DATE_TRUNC(\'DAY\', DATE_ADD(\'day\', (0 - MOD((DAY_OF_WEEK(CAST(CAST(DATE_TRUNC(\'DAY\', NOW()) AS DATE) AS TIMESTAMP)) % 7) - 1 + 7, 7)), CAST(CAST(DATE_TRUNC(\'DAY\', NOW()) AS DATE) AS TIMESTAMP)))))) AS t1',
        },
      },
    );
    this.validateAll('EPOCH(x)', {
      read: { presto: 'TO_UNIXTIME(x)' },
      write: {
        bigquery: 'TIME_TO_UNIX(x)',
        duckdb: 'EPOCH(x)',
        presto: 'TO_UNIXTIME(x)',
        spark: 'UNIX_TIMESTAMP(x)',
      },
    });
    this.validateAll('EPOCH_MS(x)', {
      write: {
        bigquery: 'TIMESTAMP_MILLIS(x)',
        clickhouse: 'fromUnixTimestamp64Milli(CAST(x AS Nullable(Int64)))',
        duckdb: 'EPOCH_MS(x)',
        mysql: 'FROM_UNIXTIME(x / POWER(10, 3))',
        postgres: 'TO_TIMESTAMP(CAST(x AS DOUBLE PRECISION) / POWER(10, 3))',
        presto: 'FROM_UNIXTIME(CAST(x AS DOUBLE) / POW(10, 3))',
        spark: 'TIMESTAMP_MILLIS(x)',
      },
    });
    this.validateAll('STRFTIME(x, \'%y-%-m-%S\')', {
      write: {
        bigquery: 'FORMAT_DATE(\'%y-%-m-%S\', x)',
        duckdb: 'STRFTIME(x, \'%y-%-m-%S\')',
        postgres: 'TO_CHAR(x, \'YY-FMMM-SS\')',
        presto: 'DATE_FORMAT(x, \'%y-%c-%s\')',
        spark: 'DATE_FORMAT(x, \'yy-M-ss\')',
      },
    });
    this.validateAll('SHA1(x)', {
      write: {
        'duckdb': 'SHA1(x)',
        '': 'SHA(x)',
      },
    });
    this.validateAll('STRFTIME(x, \'%Y-%m-%d %H:%M:%S\')', {
      write: {
        bigquery: 'FORMAT_DATE(\'%F %T\', x)',
        duckdb: 'STRFTIME(x, \'%Y-%m-%d %H:%M:%S\')',
        presto: 'DATE_FORMAT(x, \'%Y-%m-%d %T\')',
        hive: 'DATE_FORMAT(x, \'yyyy-MM-dd HH:mm:ss\')',
      },
    });
    this.validateAll('STRPTIME(x, \'%y-%-m\')', {
      write: {
        bigquery: 'PARSE_TIMESTAMP(\'%y-%-m\', x)',
        duckdb: 'STRPTIME(x, \'%y-%-m\')',
        presto: 'DATE_PARSE(x, \'%y-%c\')',
        hive: 'CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, \'yy-M\')) AS TIMESTAMP)',
        spark: 'TO_TIMESTAMP(x, \'yy-M\')',
      },
    });
    this.validateAll('TO_TIMESTAMP(x)', {
      write: {
        bigquery: 'TIMESTAMP_SECONDS(x)',
        duckdb: 'TO_TIMESTAMP(x)',
        presto: 'FROM_UNIXTIME(x)',
        hive: 'FROM_UNIXTIME(x)',
      },
    });
    this.validateAll('STRPTIME(x, \'%-m/%-d/%y %-I:%M %p\')', {
      write: {
        bigquery: 'PARSE_TIMESTAMP(\'%-m/%e/%y %-I:%M %p\', x)',
        duckdb: 'STRPTIME(x, \'%-m/%-d/%y %-I:%M %p\')',
        presto: 'DATE_PARSE(x, \'%c/%e/%y %l:%i %p\')',
        hive: 'CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, \'M/d/yy h:mm a\')) AS TIMESTAMP)',
        spark: 'TO_TIMESTAMP(x, \'M/d/yy h:mm a\')',
      },
    });
    this.validateAll('CAST(start AS TIMESTAMPTZ) AT TIME ZONE \'America/New_York\'', {
      read: {
        snowflake: 'CONVERT_TIMEZONE(\'America/New_York\', CAST(start AS TIMESTAMPTZ))',
      },
      write: {
        bigquery: 'TIMESTAMP(DATETIME(CAST(start AS TIMESTAMP), \'America/New_York\'))',
        duckdb: 'CAST(start AS TIMESTAMPTZ) AT TIME ZONE \'America/New_York\'',
        snowflake: 'CONVERT_TIMEZONE(\'America/New_York\', CAST(start AS TIMESTAMPTZ))',
      },
    });
    this.validateAll('SELECT TIMESTAMP \'foo\'', {
      write: {
        duckdb: 'SELECT CAST(\'foo\' AS TIMESTAMP)',
        hive: 'SELECT CAST(\'foo\' AS TIMESTAMP)',
        spark2: 'SELECT CAST(\'foo\' AS TIMESTAMP)',
        spark: 'SELECT CAST(\'foo\' AS TIMESTAMP_NTZ)',
        postgres: 'SELECT CAST(\'foo\' AS TIMESTAMP)',
        mysql: 'SELECT CAST(\'foo\' AS DATETIME)',
        clickhouse: 'SELECT CAST(\'foo\' AS Nullable(DateTime))',
        databricks: 'SELECT CAST(\'foo\' AS TIMESTAMP_NTZ)',
        snowflake: 'SELECT CAST(\'foo\' AS TIMESTAMPNTZ)',
        redshift: 'SELECT CAST(\'foo\' AS TIMESTAMP)',
        tsql: 'SELECT CAST(\'foo\' AS DATETIME2)',
        presto: 'SELECT CAST(\'foo\' AS TIMESTAMP)',
        trino: 'SELECT CAST(\'foo\' AS TIMESTAMP)',
        oracle: 'SELECT CAST(\'foo\' AS TIMESTAMP)',
        bigquery: 'SELECT CAST(\'foo\' AS DATETIME)',
        starrocks: 'SELECT CAST(\'foo\' AS DATETIME)',
      },
    });
  }

  testSample () {
    this.validateIdentity(
      'SELECT * FROM tbl USING SAMPLE 5',
      'SELECT * FROM tbl USING SAMPLE RESERVOIR (5 ROWS)',
    );
    this.validateIdentity(
      'SELECT * FROM tbl USING SAMPLE 10%',
      'SELECT * FROM tbl USING SAMPLE SYSTEM (10 PERCENT)',
    );
    this.validateIdentity(
      'SELECT * FROM tbl USING SAMPLE 10 PERCENT (bernoulli)',
      'SELECT * FROM tbl USING SAMPLE BERNOULLI (10 PERCENT)',
    );
    this.validateIdentity(
      'SELECT * FROM tbl USING SAMPLE reservoir(50 ROWS) REPEATABLE (100)',
      'SELECT * FROM tbl USING SAMPLE RESERVOIR (50 ROWS) REPEATABLE (100)',
    );
    this.validateIdentity(
      'SELECT * FROM tbl USING SAMPLE 10% (system, 377)',
      'SELECT * FROM tbl USING SAMPLE SYSTEM (10 PERCENT) REPEATABLE (377)',
    );
    this.validateIdentity(
      'SELECT * FROM tbl TABLESAMPLE RESERVOIR(20%), tbl2 WHERE tbl.i=tbl2.i',
      'SELECT * FROM tbl TABLESAMPLE RESERVOIR (20 PERCENT), tbl2 WHERE tbl.i = tbl2.i',
    );
    this.validateIdentity(
      'SELECT * FROM tbl, tbl2 WHERE tbl.i=tbl2.i USING SAMPLE RESERVOIR(20%)',
      'SELECT * FROM tbl, tbl2 WHERE tbl.i = tbl2.i USING SAMPLE RESERVOIR (20 PERCENT)',
    );
    this.validateAll('SELECT * FROM example TABLESAMPLE RESERVOIR (3 ROWS) REPEATABLE (82)', {
      read: {
        duckdb: 'SELECT * FROM example TABLESAMPLE (3) REPEATABLE (82)',
        snowflake: 'SELECT * FROM example SAMPLE (3 ROWS) SEED (82)',
      },
      write: {
        duckdb: 'SELECT * FROM example TABLESAMPLE RESERVOIR (3 ROWS) REPEATABLE (82)',
      },
    });
    this.validateAll(
      'SELECT * FROM (SELECT * FROM t) AS t1 TABLESAMPLE (1 ROWS), (SELECT * FROM t) AS t2 TABLESAMPLE (2 ROWS)',
      {
        write: {
          duckdb: 'SELECT * FROM (SELECT * FROM t) AS t1 TABLESAMPLE RESERVOIR (1 ROWS), (SELECT * FROM t) AS t2 TABLESAMPLE RESERVOIR (2 ROWS)',
          spark: 'SELECT * FROM (SELECT * FROM t) TABLESAMPLE (1 ROWS) AS t1, (SELECT * FROM t) TABLESAMPLE (2 ROWS) AS t2',
        },
      },
    );
  }

  testArray () {
    this.validateIdentity('ARRAY(SELECT id FROM t)');
    this.validateIdentity('ARRAY((SELECT id FROM t))');
  }

  testCast () {
    this.validateIdentity('x::int[3]', 'CAST(x AS INT[3])');
    this.validateIdentity('CAST(x AS REAL)');
    this.validateIdentity('CAST(x AS UINTEGER)');
    this.validateIdentity('CAST(x AS UBIGINT)');
    this.validateIdentity('CAST(x AS USMALLINT)');
    this.validateIdentity('CAST(x AS UTINYINT)');
    this.validateIdentity('CAST(x AS TEXT)');
    this.validateIdentity('CAST(x AS INT128)');
    this.validateIdentity('CAST(x AS DOUBLE)');
    this.validateIdentity('CAST(x AS DECIMAL(15, 4))');
    this.validateIdentity('CAST(x AS STRUCT(number BIGINT))');
    this.validateIdentity('CAST(x AS INT64)', 'CAST(x AS BIGINT)');
    this.validateIdentity('CAST(x AS INT32)', 'CAST(x AS INT)');
    this.validateIdentity('CAST(x AS INT16)', 'CAST(x AS SMALLINT)');
    this.validateIdentity('CAST(x AS INT8)', 'CAST(x AS BIGINT)');
    this.validateIdentity('CAST(x AS NUMERIC(1, 2))', 'CAST(x AS DECIMAL(1, 2))');
    this.validateIdentity('CAST(x AS HUGEINT)', 'CAST(x AS INT128)');
    this.validateIdentity('CAST(x AS UHUGEINT)', 'CAST(x AS UINT128)');
    this.validateIdentity('CAST(x AS CHAR)', 'CAST(x AS TEXT)');
    this.validateIdentity('CAST(x AS BPCHAR)', 'CAST(x AS TEXT)');
    this.validateIdentity('CAST(x AS STRING)', 'CAST(x AS TEXT)');
    this.validateIdentity('CAST(x AS VARCHAR)', 'CAST(x AS TEXT)');
    this.validateIdentity('CAST(x AS INT1)', 'CAST(x AS TINYINT)');
    this.validateIdentity('CAST(x AS FLOAT4)', 'CAST(x AS REAL)');
    this.validateIdentity('CAST(x AS FLOAT)', 'CAST(x AS REAL)');
    this.validateIdentity('CAST(x AS INT4)', 'CAST(x AS INT)');
    this.validateIdentity('CAST(x AS INTEGER)', 'CAST(x AS INT)');
    this.validateIdentity('CAST(x AS SIGNED)', 'CAST(x AS INT)');
    this.validateIdentity('CAST(x AS BLOB)', 'CAST(x AS BLOB)');
    this.validateIdentity('CAST(x AS BYTEA)', 'CAST(x AS BLOB)');
    this.validateIdentity('CAST(x AS BINARY)', 'CAST(x AS BLOB)');
    this.validateIdentity('CAST(x AS VARBINARY)', 'CAST(x AS BLOB)');
    this.validateIdentity('CAST(x AS LOGICAL)', 'CAST(x AS BOOLEAN)');
    this.validateIdentity('CAST({\'i\': 1, \'s\': \'foo\'} AS STRUCT("s" TEXT, "i" INT))');
    this.validateIdentity(
      'CAST(ROW(1, ROW(1)) AS STRUCT(number BIGINT, row STRUCT(number BIGINT)))',
    );
    this.validateIdentity('123::CHARACTER VARYING', 'CAST(123 AS TEXT)');
    this.validateIdentity(
      'CAST([[STRUCT_PACK(a := 1)]] AS STRUCT(a BIGINT)[][])',
      'CAST([[{\'a\': 1}]] AS STRUCT(a BIGINT)[][])',
    );
    this.validateIdentity(
      'CAST([STRUCT_PACK(a := 1)] AS STRUCT(a BIGINT)[])',
      'CAST([{\'a\': 1}] AS STRUCT(a BIGINT)[])',
    );
    this.validateIdentity('STRUCT_PACK(a := \'b\')::json', 'CAST({\'a\': \'b\'} AS JSON)');
    this.validateIdentity('STRUCT_PACK(a := \'b\')::STRUCT(a TEXT)', 'CAST({\'a\': \'b\'} AS STRUCT(a TEXT))');
    this.validateAll('CAST(x AS TIME)', {
      read: {
        duckdb: 'CAST(x AS TIME)',
        presto: 'CAST(x AS TIME(6))',
      },
    });
    this.validateAll('SELECT CAST(\'2020-01-01 12:05:01\' AS TIMESTAMP)', {
      read: {
        duckdb: 'SELECT CAST(\'2020-01-01 12:05:01\' AS TIMESTAMP)',
        snowflake: 'SELECT CAST(\'2020-01-01 12:05:01\' AS TIMESTAMPNTZ)',
      },
    });
    this.validateAll('SELECT CAST(\'2020-01-01\' AS DATE) + INTERVAL (day_offset) DAY FROM t', {
      read: {
        duckdb: 'SELECT CAST(\'2020-01-01\' AS DATE) + INTERVAL (day_offset) DAY FROM t',
        mysql: 'SELECT DATE \'2020-01-01\' + INTERVAL day_offset DAY FROM t',
      },
    });
    this.validateAll('SELECT CAST(\'09:05:03\' AS TIME) + INTERVAL 2 HOUR', {
      read: { snowflake: 'SELECT TIMEADD(HOUR, 2, TO_TIME(\'09:05:03\'))' },
      write: {
        duckdb: 'SELECT CAST(\'09:05:03\' AS TIME) + INTERVAL \'2\' HOUR',
        snowflake: 'SELECT CAST(\'09:05:03\' AS TIME) + INTERVAL \'2 HOUR\'',
      },
    });
    this.validateAll('CAST(x AS VARCHAR(5))', {
      write: {
        duckdb: 'CAST(x AS TEXT)',
        postgres: 'CAST(x AS TEXT)',
      },
    });
    this.validateAll('CAST(x AS DECIMAL(38, 0))', {
      read: {
        snowflake: 'CAST(x AS NUMBER)',
        duckdb: 'CAST(x AS DECIMAL(38, 0))',
      },
      write: {
        snowflake: 'CAST(x AS DECIMAL(38, 0))',
      },
    });
    this.validateAll('CAST(x AS NUMERIC)', {
      write: {
        duckdb: 'CAST(x AS DECIMAL(18, 3))',
        postgres: 'CAST(x AS DECIMAL(18, 3))',
      },
    });
    this.validateAll('CAST(x AS DECIMAL)', {
      write: {
        duckdb: 'CAST(x AS DECIMAL(18, 3))',
        postgres: 'CAST(x AS DECIMAL(18, 3))',
      },
    });
    this.validateAll('CAST(x AS BIT)', {
      read: { duckdb: 'CAST(x AS BITSTRING)' },
      write: {
        duckdb: 'CAST(x AS BIT)',
        tsql: 'CAST(x AS BIT)',
      },
    });
    this.validateAll('cast([[1]] as int[][])', {
      write: {
        duckdb: 'CAST([[1]] AS INT[][])',
        spark: 'CAST(ARRAY(ARRAY(1)) AS ARRAY<ARRAY<INT>>)',
      },
    });
    this.validateAll('CAST(x AS DATE) + INTERVAL (7 * -1) DAY', {
      read: { spark: 'DATE_SUB(x, 7)' },
    });
    this.validateAll('TRY_CAST(1 AS DOUBLE)', {
      read: {
        hive: '1d',
        spark: '1d',
      },
    });
    this.validateAll('CAST(x AS DATE)', {
      write: {
        'duckdb': 'CAST(x AS DATE)',
        '': 'CAST(x AS DATE)',
      },
    });
    this.validateAll('COL::BIGINT[]', {
      write: {
        duckdb: 'CAST(COL AS BIGINT[])',
        presto: 'CAST(COL AS ARRAY(BIGINT))',
        hive: 'CAST(COL AS ARRAY<BIGINT>)',
        spark: 'CAST(COL AS ARRAY<BIGINT>)',
        postgres: 'CAST(COL AS BIGINT[])',
        snowflake: 'CAST(COL AS ARRAY(BIGINT))',
      },
    });
    this.validateIdentity('SELECT x::INT[3][3]', 'SELECT CAST(x AS INT[3][3])');
    this.validateIdentity(
      'SELECT ARRAY[[[1]]]::INT[1][1][1]',
      'SELECT CAST([[[1]]] AS INT[1][1][1])',
    );
  }

  testEncodeDecode () {
    this.validateAll('ENCODE(x)', {
      read: {
        spark: 'ENCODE(x, \'utf-8\')',
        presto: 'TO_UTF8(x)',
      },
      write: {
        duckdb: 'ENCODE(x)',
        spark: 'ENCODE(x, \'utf-8\')',
        presto: 'TO_UTF8(x)',
      },
    });
    this.validateAll('DECODE(x)', {
      read: {
        spark: 'DECODE(x, \'utf-8\')',
        presto: 'FROM_UTF8(x)',
      },
      write: {
        duckdb: 'DECODE(x)',
        spark: 'DECODE(x, \'utf-8\')',
        presto: 'FROM_UTF8(x)',
      },
    });
    this.validateAll('DECODE(x)', {
      read: { presto: 'FROM_UTF8(x, y)' },
    });
  }

  testSha () {
    this.validateIdentity('SHA1(\'foo\')');
    this.validateIdentity('SHA1(x)');
    this.validateIdentity('SHA256(\'foo\')');
    this.validateIdentity('SHA256(x)');
  }

  testRenameTable () {
    this.validateAll('ALTER TABLE db.t1 RENAME TO db.t2', {
      write: {
        snowflake: 'ALTER TABLE db.t1 RENAME TO db.t2',
        duckdb: 'ALTER TABLE db.t1 RENAME TO t2',
        tsql: 'EXEC sp_rename \'db.t1\', \'t2\'',
      },
    });
    this.validateAll('ALTER TABLE "db"."t1" RENAME TO "db"."t2"', {
      write: {
        snowflake: 'ALTER TABLE "db"."t1" RENAME TO "db"."t2"',
        duckdb: 'ALTER TABLE "db"."t1" RENAME TO "t2"',
        tsql: 'EXEC sp_rename \'[db].[t1]\', \'t2\'',
      },
    });
  }

  testTimestampsWithUnits () {
    this.validateAll('SELECT w::TIMESTAMP_S, x::TIMESTAMP_MS, y::TIMESTAMP_US, z::TIMESTAMP_NS', {
      write: {
        duckdb: 'SELECT CAST(w AS TIMESTAMP_S), CAST(x AS TIMESTAMP_MS), CAST(y AS TIMESTAMP), CAST(z AS TIMESTAMP_NS)',
      },
    });
  }

  testIsnan () {
    this.validateAll('ISNAN(x)', {
      read: { bigquery: 'IS_NAN(x)' },
      write: {
        bigquery: 'IS_NAN(x)',
        duckdb: 'ISNAN(x)',
      },
    });
  }

  testIsinf () {
    this.validateAll('ISINF(x)', {
      read: { bigquery: 'IS_INF(x)' },
      write: {
        bigquery: 'IS_INF(x)',
        duckdb: 'ISINF(x)',
      },
    });
  }

  testParameterToken () {
    this.validateAll('SELECT $foo', {
      read: { bigquery: 'SELECT @foo' },
      write: {
        bigquery: 'SELECT @foo',
        duckdb: 'SELECT $foo',
      },
    });
  }

  testAttachDetach () {
    this.validateIdentity('ATTACH \'file.db\'');
    this.validateIdentity('ATTACH \':memory:\' AS db_alias');
    this.validateIdentity('ATTACH IF NOT EXISTS \'file.db\' AS db_alias');
    this.validateIdentity('ATTACH \'file.db\' AS db_alias (READ_ONLY)');
    this.validateIdentity('ATTACH \'file.db\' (READ_ONLY FALSE, TYPE sqlite)');
    this.validateIdentity('ATTACH \'file.db\' (TYPE POSTGRES, SCHEMA \'public\')');
    this.validateIdentity('ATTACH DATABASE \'file.db\'', 'ATTACH \'file.db\'');
    this.validateIdentity('DETACH new_database');
    this.validateIdentity('DETACH IF EXISTS file', 'DETACH DATABASE IF EXISTS file');
    this.validateIdentity('DETACH DATABASE IF EXISTS file', 'DETACH DATABASE IF EXISTS file');
    this.validateIdentity('DETACH DATABASE db', 'DETACH db');
  }

  testSimplifiedPivotUnpivot () {
    this.validateIdentity('PIVOT Cities ON Year USING SUM(Population)');
    this.validateIdentity('PIVOT Cities ON Year USING FIRST(Population)');
    this.validateIdentity('PIVOT Cities ON Year USING SUM(Population) GROUP BY Country');
    this.validateIdentity('PIVOT Cities ON Country, Name USING SUM(Population)');
    this.validateIdentity('PIVOT Cities ON Country || \'_\' || Name USING SUM(Population)');
    this.validateIdentity('PIVOT Cities ON Year USING SUM(Population) GROUP BY Country, Name');
    this.validateIdentity('UNPIVOT (SELECT 1 AS col1, 2 AS col2) ON foo, bar');
    this.validateIdentity(
      'UNPIVOT monthly_sales ON jan, feb, mar, apr, may, jun INTO NAME month VALUE sales',
    );
    this.validateIdentity(
      'UNPIVOT monthly_sales ON COLUMNS(* EXCLUDE (empid, dept)) INTO NAME month VALUE sales',
    );
    this.validateIdentity(
      'UNPIVOT monthly_sales ON (jan, feb, mar) AS q1, (apr, may, jun) AS q2 INTO NAME quarter VALUE month_1_sales, month_2_sales, month_3_sales',
    );
    this.validateIdentity(
      'WITH unpivot_alias AS (UNPIVOT monthly_sales ON COLUMNS(* EXCLUDE (empid, dept)) INTO NAME month VALUE sales) SELECT * FROM unpivot_alias',
    );
    this.validateIdentity(
      'SELECT * FROM (UNPIVOT monthly_sales ON COLUMNS(* EXCLUDE (empid, dept)) INTO NAME month VALUE sales) AS unpivot_alias',
    );
    this.validateIdentity(
      'WITH cities(country, name, year, population) AS (SELECT \'NL\', \'Amsterdam\', 2000, 1005 UNION ALL SELECT \'US\', \'Seattle\', 2020, 738) PIVOT cities ON year USING SUM(population)',
    );
  }

  testFromFirstWithParentheses () {
    this.validateIdentity(
      'CREATE TABLE t1 AS (FROM t2 SELECT foo1, foo2)',
      'CREATE TABLE t1 AS (SELECT foo1, foo2 FROM t2)',
    );
    this.validateIdentity(
      'FROM (FROM t1 SELECT foo1, foo2)',
      'SELECT * FROM (SELECT foo1, foo2 FROM t1)',
    );
    this.validateIdentity(
      'WITH t1 AS (FROM (FROM t2 SELECT foo1, foo2)) FROM t1',
      'WITH t1 AS (SELECT * FROM (SELECT foo1, foo2 FROM t2)) SELECT * FROM t1',
    );
  }

  testAnalyze () {
    this.validateIdentity('ANALYZE');
  }

  testPrefixAliases () {
    this.validateIdentity('SELECT foo: 1', 'SELECT 1 AS foo');
    this.validateIdentity('SELECT foo: bar', 'SELECT bar AS foo');
    this.validateIdentity('SELECT foo: t.col FROM t', 'SELECT t.col AS foo FROM t');
    this.validateIdentity('SELECT "foo" /* bla */: 1', 'SELECT 1 AS "foo" /* bla */');
    this.validateIdentity('SELECT "foo": 1 /* bla */', 'SELECT 1 AS "foo" /* bla */');
    this.validateIdentity('SELECT "foo": /* bla */ 1', 'SELECT 1 AS "foo" /* bla */');
    this.validateIdentity(
      'SELECT "foo": /* bla */ 1 /* foo */',
      'SELECT 1 AS "foo" /* bla */ /* foo */',
    );
    this.validateIdentity('SELECT "foo": 1', 'SELECT 1 AS "foo"');
    this.validateIdentity('SELECT foo: 1, bar: 2, baz: 3', 'SELECT 1 AS foo, 2 AS bar, 3 AS baz');
    this.validateIdentity(
      'SELECT e: 1 + 2, f: len(\'asdf\'), s: (SELECT 42)',
      'SELECT 1 + 2 AS e, LENGTH(\'asdf\') AS f, (SELECT 42) AS s',
    );
    this.validateIdentity('SELECT * FROM foo: bar', 'SELECT * FROM bar AS foo');
    this.validateIdentity('SELECT * FROM foo: c.db.tbl', 'SELECT * FROM c.db.tbl AS foo');
    this.validateIdentity('SELECT * FROM foo /* bla */: bar', 'SELECT * FROM bar AS foo /* bla */');
    this.validateIdentity(
      'SELECT * FROM foo /* bla */: bar /* baz */',
      'SELECT * FROM bar AS foo /* bla */ /* baz */',
    );
    this.validateIdentity(
      'SELECT * FROM foo /* bla */: /* baz */ bar /* boo */',
      'SELECT * FROM bar AS foo /* bla */ /* baz */ /* boo */',
    );
    this.validateIdentity(
      'SELECT * FROM r: range(10), v: (VALUES (42)), s: (FROM range(10))',
      'SELECT * FROM RANGE(0, 10) AS r, (VALUES (42)) AS v, (SELECT * FROM RANGE(0, 10)) AS s',
    );
  }

  testAtSignToAbs () {
    this.validateIdentity('SELECT @col FROM t', 'SELECT ABS(col) FROM t');
    this.validateIdentity('SELECT @col + 1 FROM t', 'SELECT ABS(col + 1) FROM t');
    this.validateIdentity('SELECT (@col) + 1 FROM t', 'SELECT (ABS(col)) + 1 FROM t');
    this.validateIdentity('SELECT @(-1)', 'SELECT ABS((-1))');
    this.validateIdentity('SELECT @(-1) + 1', 'SELECT ABS((-1) + 1)');
    this.validateIdentity('SELECT (@-1) + 1', 'SELECT (ABS(-1)) + 1');
  }

  testShowTables () {
    this.validateIdentity('SHOW TABLES').assertIs(ShowExpr);
    this.validateIdentity('SHOW ALL TABLES').assertIs(ShowExpr);
  }

  testExtractDateParts () {
    for (const part of ['WEEK', 'WEEKOFYEAR']) {
      this.validateIdentity(`EXTRACT(${part} FROM foo)`, 'EXTRACT(WEEK FROM foo)');
    }
    for (const part of [
      'WEEKDAY',
      'ISOYEAR',
      'ISODOW',
      'YEARWEEK',
      'TIMEZONE_HOUR',
      'TIMEZONE_MINUTE',
    ]) {
      this.validateIdentity(`EXTRACT(${part} FROM foo)`);
    }
  }

  testSetItem () {
    this.validateIdentity('SET memory_limit = \'10GB\'');
    this.validateIdentity('SET SESSION default_collation = \'nocase\'');
    this.validateIdentity('SET GLOBAL sort_order = \'desc\'');
    this.validateIdentity('SET VARIABLE my_var = 30');
    this.validateIdentity('SET VARIABLE location_map = (SELECT foo FROM bar)');
    this.validateIdentity('SET VARIABLE my_var TO 30', 'SET VARIABLE my_var = 30');
    this.validateAll('SET VARIABLE a = 1', {
      write: {
        duckdb: 'SET VARIABLE a = 1',
        bigquery: 'SET a = 1',
        snowflake: 'SET a = 1',
      },
    });
  }

  testReset () {
    this.validateIdentity('RESET threads', undefined, { checkCommandWarning: true });
    this.validateIdentity('RESET memory_limit', undefined, { checkCommandWarning: true });
    this.validateIdentity('RESET default_collation', undefined, { checkCommandWarning: true });
    this.validateIdentity('RESET SESSION threads', undefined, { checkCommandWarning: true });
    this.validateIdentity('RESET GLOBAL memory_limit', undefined, { checkCommandWarning: true });
    this.validateIdentity('RESET LOCAL threads', undefined, { checkCommandWarning: true });
    this.validateIdentity('RESET SESSION default_collation', undefined, { checkCommandWarning: true });
  }

  testMapStruct () {
    this.validateIdentity('MAP {1: \'a\', 2: \'b\'}');
    this.validateIdentity('MAP {\'1\': \'a\', \'2\': \'b\'}');
    this.validateIdentity('MAP {[1, 2]: \'a\', [3, 4]: \'b\'}');
  }

  testCreateSequence () {
    this.validateIdentity('CREATE SEQUENCE serial START 101', 'CREATE SEQUENCE serial START WITH 101');
    this.validateIdentity('CREATE SEQUENCE serial START WITH 1 INCREMENT BY 2');
    this.validateIdentity('CREATE SEQUENCE serial START WITH 99 INCREMENT BY -1 MAXVALUE 99');
    this.validateIdentity('CREATE SEQUENCE serial START WITH 1 MAXVALUE 10 NO CYCLE');
    this.validateIdentity('CREATE SEQUENCE serial START WITH 1 MAXVALUE 10 CYCLE');
  }

  testInstall () {
    this.validateIdentity('INSTALL httpfs').assertIs(InstallExpr);
    this.validateIdentity('INSTALL httpfs FROM community');
    this.validateIdentity('INSTALL httpfs FROM \'https://extensions.duckdb.org\'');
    this.validateIdentity('FORCE INSTALL httpfs').assertIs(InstallExpr);
    this.validateIdentity('FORCE INSTALL httpfs FROM community');
    this.validateIdentity('FORCE INSTALL httpfs FROM \'https://extensions.duckdb.org\'');
    this.validateIdentity('FORCE CHECKPOINT db', undefined, { checkCommandWarning: true });
  }

  testCteUsingKey () {
    this.validateIdentity(
      'WITH RECURSIVE tbl(a, b) USING KEY (a) AS (SELECT a, b FROM (VALUES (1, 3), (2, 4)) AS t(a, b) UNION SELECT a + 1, b FROM tbl WHERE a < 3) SELECT * FROM tbl',
    );
    this.validateIdentity(
      'WITH RECURSIVE tbl(a, b) USING KEY (a, b) AS (SELECT a, b FROM (VALUES (1, 3), (2, 4)) AS t(a, b) UNION SELECT a + 1, b FROM tbl WHERE a < 3) SELECT * FROM tbl',
    );
  }

  testUdf () {
    for (const keyword of ['FUNCTION', 'MACRO']) {
      this.validateIdentity(`SELECT ${keyword}`);
      this.validateIdentity(`CREATE ${keyword} add(a, b) AS a + b`);
      this.validateIdentity(`CREATE ${keyword} ifelse(a, b, c) AS CASE WHEN a THEN b ELSE c END`);
    }
  }

  testBitwiseAgg () {
    this.validateAll('SELECT BIT_OR(int_value) FROM t', {
      read: {
        snowflake: 'SELECT BITOR_AGG(int_value) FROM t',
        duckdb: 'SELECT BIT_OR(int_value) FROM t',
      },
    });
    this.validateAll('SELECT BIT_AND(int_value) FROM t', {
      read: {
        snowflake: 'SELECT BITAND_AGG(int_value) FROM t',
        duckdb: 'SELECT BIT_AND(int_value) FROM t',
      },
    });
    this.validateAll('SELECT BIT_XOR(int_value) FROM t', {
      read: {
        snowflake: 'SELECT BITXOR_AGG(int_value) FROM t',
        duckdb: 'SELECT BIT_XOR(int_value) FROM t',
      },
    });
    this.validateAll('SELECT BIT_OR(CAST(val AS FLOAT)) FROM t', {
      write: {
        duckdb: 'SELECT BIT_OR(CAST(ROUND(CAST(val AS REAL)) AS INT)) FROM t',
        snowflake: 'SELECT BITORAGG(CAST(val AS FLOAT)) FROM t',
      },
    });
    this.validateAll('SELECT BIT_AND(CAST(val AS DOUBLE)) FROM t', {
      write: {
        duckdb: 'SELECT BIT_AND(CAST(ROUND(CAST(val AS DOUBLE)) AS INT)) FROM t',
        snowflake: 'SELECT BITANDAGG(CAST(val AS DOUBLE)) FROM t',
      },
    });
    this.validateAll('SELECT BIT_OR(CAST(val AS DECIMAL(10, 2))) FROM t', {
      write: {
        duckdb: 'SELECT BIT_OR(CAST(CAST(val AS DECIMAL(10, 2)) AS INT)) FROM t',
        snowflake: 'SELECT BITORAGG(CAST(val AS DECIMAL(10, 2))) FROM t',
      },
    });
    this.validateAll('SELECT BIT_XOR(CAST(val AS DECIMAL)) FROM t', {
      write: {
        duckdb: 'SELECT BIT_XOR(CAST(CAST(val AS DECIMAL(18, 3)) AS INT)) FROM t',
        snowflake: 'SELECT BITXORAGG(CAST(val AS DECIMAL(18, 3))) FROM t',
      },
    });
  }

  testApproxPercentile () {
    this.validateAll('SELECT APPROX_QUANTILE(a, 0.5) FROM t', {
      read: { snowflake: 'SELECT APPROX_PERCENTILE(a, 0.5) FROM t' },
      write: {
        duckdb: 'SELECT APPROX_QUANTILE(a, 0.5) FROM t',
        snowflake: 'SELECT APPROX_PERCENTILE(a, 0.5) FROM t',
      },
    });
  }

  testCurrentDatabase () {
    this.validateAll('SELECT CURRENT_DATABASE()', {
      read: { snowflake: 'SELECT CURRENT_DATABASE()' },
      write: {
        duckdb: 'SELECT CURRENT_DATABASE()',
        snowflake: 'SELECT CURRENT_DATABASE()',
      },
    });
  }

  testCurrentSchema () {
    this.validateAll('SELECT CURRENT_SCHEMA()', {
      read: { snowflake: 'SELECT CURRENT_SCHEMA()' },
      write: {
        duckdb: 'SELECT CURRENT_SCHEMA()',
        snowflake: 'SELECT CURRENT_SCHEMA()',
      },
    });
  }
}

const t = new TestDuckDB();
describe('TestDuckDB', () => {
  test('testDuckdb', () => t.testDuckdb());
  test('testArrayIndex', () => t.testArrayIndex());
  test('testArrayInsert', () => t.testArrayInsert());
  test('testArrayRemove', () => t.testArrayRemove());
  test('testArrayRemoveAt', () => t.testArrayRemoveAt());
  test('testTime', () => t.testTime());
  test('testSample', () => t.testSample());
  test('testArray', () => t.testArray());
  test('testCast', () => t.testCast());
  test('testEncodeDecode', () => t.testEncodeDecode());
  test('testSha', () => t.testSha());
  test('testRenameTable', () => t.testRenameTable());
  test('testTimestampsWithUnits', () => t.testTimestampsWithUnits());
  test('testIsnan', () => t.testIsnan());
  test('testIsinf', () => t.testIsinf());
  test('testParameterToken', () => t.testParameterToken());
  test('testAttachDetach', () => t.testAttachDetach());
  test('testSimplifiedPivotUnpivot', () => t.testSimplifiedPivotUnpivot());
  test('testFromFirstWithParentheses', () => t.testFromFirstWithParentheses());
  test('testAnalyze', () => t.testAnalyze());
  test('testPrefixAliases', () => t.testPrefixAliases());
  test('testAtSignToAbs', () => t.testAtSignToAbs());
  test('testShowTables', () => t.testShowTables());
  test('testExtractDateParts', () => t.testExtractDateParts());
  test('testSetItem', () => t.testSetItem());
  test('testReset', () => t.testReset());
  test('testMapStruct', () => t.testMapStruct());
  test('testCreateSequence', () => t.testCreateSequence());
  test('testInstall', () => t.testInstall());
  test('testCteUsingKey', () => t.testCteUsingKey());
  test('testUdf', () => t.testUdf());
  test('testBitwiseAgg', () => t.testBitwiseAgg());
  test('testApproxPercentile', () => t.testApproxPercentile());
  test('testCurrentDatabase', () => t.testCurrentDatabase());
  test('testCurrentSchema', () => t.testCurrentSchema());
});
