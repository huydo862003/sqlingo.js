import {
  DateTime,
} from 'luxon';
import {
  describe, test, expect, vi,
} from 'vitest';
import {
  parseOne, ErrorLevel,
} from '../../../src/index';
import type {
  Expression,
  SelectExpr,
} from '../../../src/expressions';
import {
  AliasExpr, AndExpr, AnonymousAggFuncExpr, AnonymousExpr, ArrayExpr,
  CombinedAggFuncExpr, CombinedParameterizedAggExpr, ColumnConstraintExpr,
  ColumnExpr, CreateExpr, DataTypeExpr, DateStrToDateExpr, IfExpr, LiteralExpr,
  OnClusterExpr, ParameterExpr, ParameterizedAggExpr, ParenExpr,
  PropertiesExpr, EnginePropertyExpr, SchemaCommentPropertyExpr,
  SubqueryExpr, TableExpr, TimeStrToTimeExpr, TruncExpr, TupleExpr,
  VarExpr,
  convert, select, table_, toIdentifier as toIdent, toTable, values, var_,
} from '../../../src/expressions';
import {
  CreateExprKind,
} from '../../../src/expressions/types';
import {
  ClickHouseGenerator,
} from '../../../src/dialects/clickhouse';
import {
  quoteIdentifiers,
} from '../../../src/optimizer/qualify_columns';
import {
  traverseScope,
} from '../../../src/optimizer/scope';
import {
  Validator,
} from './validator';

class TestClickHouse extends Validator {
  override dialect = 'clickhouse' as const;

  testClickhouse () {
    const expr = quoteIdentifiers(this.parseOne('{start_date:String}'), {
      dialect: 'clickhouse',
    });

    expect(expr.sql({
      dialect: 'clickhouse',
    })).toBe('{start_date: String}');

    for (const stringTypeEnum of ClickHouseGenerator.STRING_TYPE_MAPPING.keys()) {
      this.validateIdentity(`CAST(x AS ${stringTypeEnum.toUpperCase()})`, 'CAST(x AS String)');
    }

    // Arrays, maps and tuples can't be Nullable in ClickHouse
    for (const nonNullableType of [
      'ARRAY<INT>',
      'MAP<INT, INT>',
      'STRUCT(a: INT)',
    ]) {
      const tryCast = parseOne(`TRY_CAST(x AS ${nonNullableType})`);
      const targetType = (tryCast as any).to.sql({
        dialect: 'clickhouse',
      });

      expect(tryCast.sql({
        dialect: 'clickhouse',
      })).toBe(`CAST(x AS ${targetType})`);
    }

    for (const nullableType of [
      'INT',
      'UINT',
      'BIGINT',
      'FLOAT',
      'DOUBLE',
      'TEXT',
      'DATE',
      'UUID',
    ]) {
      const tryCast = parseOne(`TRY_CAST(x AS ${nullableType})`);
      const targetType = DataTypeExpr.build(nullableType, {
        dialect: 'clickhouse',
      })?.sql({
        dialect: 'clickhouse',
      });

      expect(tryCast.sql({
        dialect: 'clickhouse',
      })).toBe(`CAST(x AS Nullable(${targetType}))`);
    }

    const exprCount = parseOne('count(x)');

    expect(exprCount.sql({
      dialect: 'clickhouse',
    })).toBe('COUNT(x)');

    this.validateIdentity('SELECT DISTINCT ON ("id") * FROM t');
    this.validateIdentity('SELECT 1 OR (1 = 2)');
    this.validateIdentity('SELECT 1 AND (1 = 2)');
    this.validateIdentity('SELECT json.a.:Int64');
    this.validateIdentity('SELECT json.a.:JSON.b.:Int64');
    this.validateIdentity('WITH arrayJoin([(1, [2, 3])]) AS arr SELECT arr');
    this.validateIdentity('CAST(1 AS Bool)');
    this.validateIdentity('SELECT toString(CHAR(104.1, 101, 108.9, 108.9, 111, 32))');
    this.validateIdentity('@macro').assertIs(ParameterExpr).args.this?.assertIs(VarExpr);
    this.validateIdentity('SELECT toFloat(like)');
    this.validateIdentity('SELECT like');
    this.validateIdentity('SELECT STR_TO_DATE(str, fmt, tz)');
    this.validateIdentity('SELECT STR_TO_DATE(\'05 12 2000\', \'%d %m %Y\')');
    this.validateIdentity('SELECT EXTRACT(YEAR FROM toDateTime(\'2023-02-01\'))');
    this.validateIdentity('extract(haystack, pattern)');
    this.validateIdentity('SELECT * FROM x LIMIT 1 UNION ALL SELECT * FROM y');
    this.validateIdentity('SELECT CAST(x AS Tuple(String, Array(Nullable(Float64))))');
    this.validateIdentity('countIf(x, y)');
    this.validateIdentity('x = y');
    this.validateIdentity('x <> y');
    this.validateIdentity('SELECT * FROM (SELECT a FROM b SAMPLE 0.01)');
    this.validateIdentity('SELECT * FROM (SELECT a FROM b SAMPLE 1 / 10 OFFSET 1 / 2)');
    this.validateIdentity('SELECT sum(foo * bar) FROM bla SAMPLE 10000000');
    this.validateIdentity('CAST(x AS Nested(ID UInt32, Serial UInt32, EventTime DateTime))');
    this.validateIdentity('CAST(x AS Enum(\'hello\' = 1, \'world\' = 2))');
    this.validateIdentity('CAST(x AS Enum(\'hello\', \'world\'))');
    this.validateIdentity('CAST(x AS Enum(\'hello\' = 1, \'world\'))');
    this.validateIdentity('CAST(x AS Enum8(\'hello\' = -123, \'world\'))');
    this.validateIdentity('CAST(x AS FixedString(1))');
    this.validateIdentity('CAST(x AS LowCardinality(FixedString))');
    this.validateIdentity('SELECT isNaN(1.0)');
    this.validateIdentity('SELECT startsWith(\'Spider-Man\', \'Spi\')');
    this.validateIdentity('SELECT xor(TRUE, FALSE)');
    this.validateIdentity('CAST([\'hello\'], \'Array(Enum8(\'\'hello\'\' = 1))\')');
    this.validateIdentity('SELECT x, COUNT() FROM y GROUP BY x WITH TOTALS');
    this.validateIdentity('SELECT INTERVAL t.days DAY');
    this.validateIdentity('SELECT match(\'abc\', \'([a-z]+)\')');
    this.validateIdentity('dictGet(x, \'y\')');
    this.validateIdentity('WITH final AS (SELECT 1) SELECT * FROM final');
    this.validateIdentity('SELECT * FROM x FINAL');
    this.validateIdentity('SELECT * FROM x AS y FINAL');
    this.validateIdentity('\'a\' IN mapKeys(map(\'a\', 1, \'b\', 2))');
    this.validateIdentity('CAST((1, 2) AS Tuple(a Int8, b Int16))');
    this.validateIdentity('SELECT * FROM foo LEFT ANY JOIN bla');
    this.validateIdentity('SELECT * FROM foo LEFT ASOF JOIN bla');
    this.validateIdentity('SELECT * FROM foo ASOF JOIN bla');
    this.validateIdentity('SELECT * FROM foo ANY JOIN bla');
    this.validateIdentity('SELECT * FROM foo GLOBAL ANY JOIN bla');
    this.validateIdentity('SELECT * FROM foo GLOBAL LEFT ANY JOIN bla');
    this.validateIdentity('SELECT quantile(0.5)(a)');
    this.validateIdentity('SELECT quantiles(0.5)(a) AS x FROM t');
    this.validateIdentity('SELECT quantilesIf(0.5)(a, a > 1) AS x FROM t');
    this.validateIdentity('SELECT quantileState(0.5)(a) AS x FROM t');
    this.validateIdentity('SELECT deltaSumMerge(a) AS x FROM t');
    this.validateIdentity('SELECT quantiles(0.1, 0.2, 0.3)(a)');
    this.validateIdentity('SELECT quantileTiming(0.5)(RANGE(100))');
    this.validateIdentity('SELECT histogram(5)(a)');
    this.validateIdentity('SELECT groupUniqArray(2)(a)');
    this.validateIdentity('SELECT exponentialTimeDecayedAvg(60)(a, b)');
    this.validateIdentity('levenshteinDistance(col1, col2)', 'editDistance(col1, col2)');
    this.validateIdentity('SELECT * FROM foo WHERE x GLOBAL IN (SELECT * FROM bar)');
    this.validateIdentity('SELECT * FROM foo WHERE x GLOBAL NOT IN (SELECT * FROM bar)');
    this.validateIdentity('POSITION(haystack, needle)');
    this.validateIdentity('POSITION(haystack, needle, position)');
    this.validateIdentity('CAST(x AS DATETIME)', 'CAST(x AS DateTime)');
    this.validateIdentity('CAST(x AS TIMESTAMPTZ)', 'CAST(x AS DateTime)');
    this.validateIdentity('CAST(x as MEDIUMINT)', 'CAST(x AS Int32)');
    this.validateIdentity('CAST(x AS DECIMAL(38, 2))', 'CAST(x AS Decimal(38, 2))');
    this.validateIdentity('SELECT arrayJoin([1, 2, 3] AS src) AS dst, \'Hello\', src');
    this.validateIdentity('SELECT JSONExtractString(\'{"x": {"y": 1}}\', \'x\', \'y\')');
    this.validateIdentity('SELECT * FROM table LIMIT 1 BY a, b');
    this.validateIdentity('SELECT * FROM table LIMIT 2 OFFSET 1 BY a, b');
    this.validateIdentity('TRUNCATE TABLE t1 ON CLUSTER test_cluster');
    this.validateIdentity('TRUNCATE TABLE t1 ON CLUSTER \'{cluster}\'');
    this.validateIdentity('TRUNCATE DATABASE db');
    this.validateIdentity('TRUNCATE DATABASE db ON CLUSTER test_cluster');
    this.validateIdentity('TRUNCATE DATABASE db ON CLUSTER \'{cluster}\'');

    // Numeric trunc
    this.validateIdentity('trunc(3.14159, 2)').assertIs(TruncExpr);
    this.validateIdentity('trunc(3.14159)').assertIs(TruncExpr);
    this.validateAll(
      'trunc(3.14159, 2)',
      {
        read: {
          postgres: 'TRUNC(3.14159, 2)',
        },
      },
    );

    this.validateIdentity('EXCHANGE TABLES x.a AND y.b', undefined, {
      checkCommandWarning: true,
    });
    this.validateIdentity('CREATE TABLE test (id UInt8) ENGINE=Null()');
    this.validateIdentity(
      'SELECT * FROM foo ORDER BY bar OFFSET 0 ROWS FETCH NEXT 10 ROWS WITH TIES',
    );
    this.validateIdentity(
      'SELECT DATE_BIN(toDateTime(\'2023-01-01 14:45:00\'), INTERVAL \'1\' MINUTE, toDateTime(\'2023-01-01 14:35:30\'), \'UTC\')',
    );
    this.validateIdentity(
      'SELECT CAST(1730098800 AS DateTime64) AS DATETIME, \'test\' AS interp ORDER BY DATETIME WITH FILL FROM toDateTime64(1730098800, 3) - INTERVAL \'7\' HOUR TO toDateTime64(1730185140, 3) - INTERVAL \'7\' HOUR STEP toIntervalSecond(900) INTERPOLATE (interp)',
    );
    this.validateIdentity(
      'SELECT number, COUNT() OVER (PARTITION BY number % 3) AS partition_count FROM numbers(10) WINDOW window_name AS (PARTITION BY number) QUALIFY partition_count = 4 ORDER BY number',
    );
    this.validateIdentity(
      'SELECT id, quantileGK(100, 0.95)(reading) OVER (PARTITION BY id ORDER BY id RANGE BETWEEN 30000 PRECEDING AND CURRENT ROW) AS window FROM table',
    );
    this.validateIdentity(
      'SELECT * FROM table LIMIT 1 BY CONCAT(datalayerVariantNo, datalayerProductId, warehouse)',
    );
    this.validateIdentity(
      'SELECT JSONExtractString(\'{"a": "hello", "b": [-100, 200.0, 300]}\', \'a\')',
    );
    this.validateIdentity(
      'ATTACH DATABASE DEFAULT ENGINE = ORDINARY',
      undefined,
      {
        checkCommandWarning: true,
      },
    );
    this.validateIdentity(
      'SELECT n, source FROM (SELECT toFloat32(number % 10) AS n, \'original\' AS source FROM numbers(10) WHERE number % 3 = 1) ORDER BY n WITH FILL',
    );
    this.validateIdentity(
      'SELECT n, source FROM (SELECT toFloat32(number % 10) AS n, \'original\' AS source FROM numbers(10) WHERE number % 3 = 1) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5',
    );
    this.validateIdentity(
      'SELECT toDate((number * 10) * 86400) AS d1, toDate(number * 86400) AS d2, \'original\' AS source FROM numbers(10) WHERE (number % 3) = 1 ORDER BY d2 WITH FILL, d1 WITH FILL STEP 5',
    );
    this.validateIdentity(
      'SELECT n, source, inter FROM (SELECT toFloat32(number % 10) AS n, \'original\' AS source, number AS inter FROM numbers(10) WHERE number % 3 = 1) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5 INTERPOLATE (inter AS inter + 1)',
    );
    this.validateIdentity(
      'SELECT SUM(1) AS impressions, arrayJoin(cities) AS city, arrayJoin(browsers) AS browser FROM (SELECT [\'Istanbul\', \'Berlin\', \'Bobruisk\'] AS cities, [\'Firefox\', \'Chrome\', \'Chrome\'] AS browsers) GROUP BY 2, 3',
    );
    this.validateIdentity(
      'SELECT sum(1) AS impressions, (arrayJoin(arrayZip(cities, browsers)) AS t).1 AS city, t.2 AS browser FROM (SELECT [\'Istanbul\', \'Berlin\', \'Bobruisk\'] AS cities, [\'Firefox\', \'Chrome\', \'Chrome\'] AS browsers) GROUP BY 2, 3',
    );
    this.validateIdentity(
      'SELECT CAST(tuple(1 AS "a", 2 AS "b", 3.0 AS "c").2 AS Nullable(String))',
    );
    this.validateIdentity(
      'CREATE TABLE test (id UInt8) ENGINE=AggregatingMergeTree() ORDER BY tuple()',
    );
    this.validateIdentity(
      'CREATE TABLE test ON CLUSTER default (id UInt8) ENGINE=AggregatingMergeTree() ORDER BY tuple()',
    );
    this.validateIdentity(
      'CREATE TABLE test ON CLUSTER \'{cluster}\' (id UInt8) ENGINE=AggregatingMergeTree() ORDER BY tuple()',
    );
    this.validateIdentity(
      'CREATE MATERIALIZED VIEW test_view ON CLUSTER cl1 (id UInt8) ENGINE=AggregatingMergeTree() ORDER BY tuple() AS SELECT * FROM test_data',
    );
    this.validateIdentity(
      'CREATE MATERIALIZED VIEW test_view ON CLUSTER \'{cluster}\' (id UInt8) ENGINE=AggregatingMergeTree() ORDER BY tuple() AS SELECT * FROM test_data',
    );
    this.validateIdentity(
      'CREATE MATERIALIZED VIEW test_view ON CLUSTER cl1 TO table1 AS SELECT * FROM test_data',
    );
    this.validateIdentity(
      'CREATE MATERIALIZED VIEW test_view ON CLUSTER \'{cluster}\' TO table1 AS SELECT * FROM test_data',
    );
    this.validateIdentity(
      'CREATE MATERIALIZED VIEW test_view TO db.table1 (id UInt8) AS SELECT * FROM test_data',
    );
    this.validateIdentity(
      'CREATE TABLE t (foo String CODEC(LZ4HC(9), ZSTD, DELTA), size String ALIAS formatReadableSize(size_bytes), INDEX idx1 a TYPE bloom_filter(0.001) GRANULARITY 1, INDEX idx2 a TYPE set(100) GRANULARITY 2, INDEX idx3 a TYPE minmax GRANULARITY 3)',
    );
    this.validateIdentity(
      'SELECT generate_series FROM generate_series(0, 10) AS g(x)',
    );
    this.validateIdentity(
      'SELECT t.c FROM (SELECT arrayJoin([1,2,3,4,5]) AS c) AS t WHERE (t.c + 0) NOT IN (1,2)',
      'SELECT t.c FROM (SELECT arrayJoin([1, 2, 3, 4, 5]) AS c) AS t WHERE NOT ((t.c + 0) IN (1, 2))',
    );
    this.validateIdentity(
      'SELECT * FROM t1, t2',
      'SELECT * FROM t1 CROSS JOIN t2',
    );
    this.validateIdentity(
      'SELECT and(1, 2)',
      'SELECT 1 AND 2',
    );
    this.validateIdentity(
      'SELECT or(1, 2)',
      'SELECT 1 OR 2',
    );
    this.validateIdentity(
      'SELECT generate_series FROM generate_series(0, 10) AS g',
      'SELECT generate_series FROM generate_series(0, 10) AS g(generate_series)',
    );
    this.validateIdentity(
      'INSERT INTO tab VALUES ({\'key1\': 1, \'key2\': 10}), ({\'key1\': 2, \'key2\': 20}), ({\'key1\': 3, \'key2\': 30})',
      'INSERT INTO tab VALUES ((map(\'key1\', 1, \'key2\', 10))), ((map(\'key1\', 2, \'key2\', 20))), ((map(\'key1\', 3, \'key2\', 30)))',
    );
    this.validateIdentity(
      'SELECT (toUInt8(\'1\') + toUInt8(\'2\')) IS NOT NULL',
      'SELECT NOT ((toUInt8(\'1\') + toUInt8(\'2\')) IS NULL)',
    );
    this.validateIdentity(
      'SELECT $1$foo$1$',
      'SELECT \'foo\'',
    );
    this.validateIdentity(
      'SELECT * FROM table LIMIT 1, 2 BY a, b',
      'SELECT * FROM table LIMIT 2 OFFSET 1 BY a, b',
    );
    this.validateIdentity(
      'SELECT SUM(1) AS impressions FROM (SELECT [\'Istanbul\', \'Berlin\', \'Bobruisk\'] AS cities) WHERE arrayJoin(cities) IN [\'Istanbul\', \'Berlin\']',
      'SELECT SUM(1) AS impressions FROM (SELECT [\'Istanbul\', \'Berlin\', \'Bobruisk\'] AS cities) WHERE arrayJoin(cities) IN (\'Istanbul\', \'Berlin\')',
    );

    this.validateIdentity('SELECT SUBSTRING_INDEX(str, delim, count)');
    this.validateIdentity('SELECT SUBSTRING_INDEX(\'a.b.c.d\', \'.\', 2)');
    this.validateIdentity('SELECT SUBSTRING_INDEX(\'a.b.c.d\', \'.\', -2)');

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

    this.validateAll(
      'SELECT substringIndex(\'a.b.c.d\', \'.\', 2)',
      {
        write: {
          databricks: 'SELECT SUBSTRING_INDEX(\'a.b.c.d\', \'.\', 2)',
          spark: 'SELECT SUBSTRING_INDEX(\'a.b.c.d\', \'.\', 2)',
          mysql: 'SELECT SUBSTRING_INDEX(\'a.b.c.d\', \'.\', 2)',
          clickhouse: 'SELECT substringIndex(\'a.b.c.d\', \'.\', 2)',
        },
      },
    );

    this.validateAll(
      'SELECT CAST(STR_TO_DATE(SUBSTRING(a.eta, 1, 10), \'%Y-%m-%d\') AS Nullable(DATE))',
      {
        read: {
          clickhouse: 'SELECT CAST(STR_TO_DATE(SUBSTRING(a.eta, 1, 10), \'%Y-%m-%d\') AS Nullable(DATE))',
          oracle: 'SELECT to_date(substr(a.eta, 1,10), \'YYYY-MM-DD\')',
        },
      },
    );

    this.validateAll(
      'CHAR(67) || CHAR(65) || CHAR(84)',
      {
        read: {
          clickhouse: 'CHAR(67) || CHAR(65) || CHAR(84)',
          oracle: 'CHR(67) || CHR(65) || CHR(84)',
        },
      },
    );
    this.validateAll(
      'SELECT lagInFrame(salary, 1, 0) OVER (ORDER BY hire_date) AS prev_sal FROM employees',
      {
        read: {
          clickhouse: 'SELECT lagInFrame(salary, 1, 0) OVER (ORDER BY hire_date) AS prev_sal FROM employees',
          oracle: 'SELECT LAG(salary, 1, 0) OVER (ORDER BY hire_date) AS prev_sal FROM employees',
        },
      },
    );
    this.validateAll(
      'SELECT leadInFrame(salary, 1, 0) OVER (ORDER BY hire_date) AS prev_sal FROM employees',
      {
        read: {
          clickhouse: 'SELECT leadInFrame(salary, 1, 0) OVER (ORDER BY hire_date) AS prev_sal FROM employees',
          oracle: 'SELECT LEAD(salary, 1, 0) OVER (ORDER BY hire_date) AS prev_sal FROM employees',
        },
      },
    );
    this.validateAll(
      'SELECT CAST(STR_TO_DATE(\'05 12 2000\', \'%d %m %Y\') AS Nullable(DATE))',
      {
        read: {
          clickhouse: 'SELECT CAST(STR_TO_DATE(\'05 12 2000\', \'%d %m %Y\') AS Nullable(DATE))',
          postgres: 'SELECT TO_DATE(\'05 12 2000\', \'DD MM YYYY\')',
        },
        write: {
          clickhouse: 'SELECT CAST(STR_TO_DATE(\'05 12 2000\', \'%d %m %Y\') AS Nullable(DATE))',
          postgres: 'SELECT CAST(CAST(TO_DATE(\'05 12 2000\', \'DD MM YYYY\') AS TIMESTAMP) AS DATE)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM x PREWHERE y = 1 WHERE z = 2',
      {
        write: {
          '': 'SELECT * FROM x WHERE z = 2',
          clickhouse: 'SELECT * FROM x PREWHERE y = 1 WHERE z = 2',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM x AS prewhere',
      {
        read: {
          clickhouse: 'SELECT * FROM x AS prewhere',
          duckdb: 'SELECT * FROM x prewhere',
        },
      },
    );
    this.validateAll(
      'SELECT a, b FROM (SELECT * FROM x) AS t(a, b)',
      {
        read: {
          clickhouse: 'SELECT a, b FROM (SELECT * FROM x) AS t(a, b)',
          duckdb: 'SELECT a, b FROM (SELECT * FROM x) AS t(a, b)',
        },
      },
    );
    this.validateAll(
      'SELECT arrayJoin([1,2,3])',
      {
        write: {
          clickhouse: 'SELECT arrayJoin([1, 2, 3])',
          postgres: 'SELECT UNNEST(ARRAY[1, 2, 3])',
        },
      },
    );
    this.validateAll(
      'has([1], x)',
      {
        read: {
          postgres: 'x = any(array[1])',
        },
      },
    );
    this.validateAll(
      'NOT has([1], x)',
      {
        read: {
          postgres: 'any(array[1]) <> x',
        },
      },
    );
    this.validateAll(
      'has([1], x)',
      {
        read: {
          clickhouse: 'has([1], x)',
          presto: 'CONTAINS(ARRAY[1], x)',
          spark: 'ARRAY_CONTAINS(ARRAY(1), x)',
        },
        write: {
          presto: 'CONTAINS(ARRAY[1], x)',
          spark: 'ARRAY_CONTAINS(ARRAY(1), x)',
        },
      },
    );
    this.validateAll(
      'SELECT CAST(\'2020-01-01\' AS Nullable(DateTime)) + INTERVAL \'500\' MICROSECOND',
      {
        read: {
          duckdb: 'SELECT TIMESTAMP \'2020-01-01\' + INTERVAL \'500 us\'',
          postgres: 'SELECT TIMESTAMP \'2020-01-01\' + INTERVAL \'500 us\'',
        },
        write: {
          clickhouse: 'SELECT CAST(\'2020-01-01\' AS Nullable(DateTime)) + INTERVAL \'500\' MICROSECOND',
          duckdb: 'SELECT CAST(\'2020-01-01\' AS TIMESTAMP) + INTERVAL \'500\' MICROSECOND',
          postgres: 'SELECT CAST(\'2020-01-01\' AS TIMESTAMP) + INTERVAL \'500 MICROSECOND\'',
        },
      },
    );
    this.validateAll(
      'SELECT CURRENT_DATE()',
      {
        read: {
          clickhouse: 'SELECT CURRENT_DATE()',
          postgres: 'SELECT CURRENT_DATE',
        },
      },
    );
    this.validateAll(
      'SELECT CURRENT_TIMESTAMP()',
      {
        read: {
          clickhouse: 'SELECT CURRENT_TIMESTAMP()',
          postgres: 'SELECT CURRENT_TIMESTAMP',
        },
      },
    );
    this.validateAll(
      'SELECT match(\'ThOmAs\', CONCAT(\'(?i)\', \'thomas\'))',
      {
        read: {
          postgres: 'SELECT \'ThOmAs\' ~* \'thomas\'',
        },
      },
    );
    this.validateAll(
      'SELECT match(\'ThOmAs\', CONCAT(\'(?i)\', x)) FROM t',
      {
        read: {
          postgres: 'SELECT \'ThOmAs\' ~* x FROM t',
        },
      },
    );
    this.validateAll(
      'SELECT \'\\0\'',
      {
        read: {
          mysql: 'SELECT \'\0\'',
        },
        write: {
          clickhouse: 'SELECT \'\\0\'',
          mysql: 'SELECT \'\0\'',
        },
      },
    );
    this.validateAll(
      'DATE_ADD(DAY, 1, x)',
      {
        read: {
          clickhouse: 'dateAdd(DAY, 1, x)',
          presto: 'DATE_ADD(\'DAY\', 1, x)',
        },
        write: {
          clickhouse: 'DATE_ADD(DAY, 1, x)',
          presto: 'DATE_ADD(\'DAY\', 1, x)',
          '': 'DATE_ADD(x, 1, \'DAY\')',
        },
      },
    );
    this.validateAll(
      'DATE_DIFF(DAY, a, b)',
      {
        read: {
          clickhouse: 'dateDiff(DAY, a, b)',
          presto: 'DATE_DIFF(\'DAY\', a, b)',
        },
        write: {
          clickhouse: 'DATE_DIFF(DAY, a, b)',
          presto: 'DATE_DIFF(\'DAY\', a, b)',
          '': 'DATEDIFF(b, a, DAY)',
        },
      },
    );
    this.validateAll(
      'SELECT xor(1, 0)',
      {
        read: {
          clickhouse: 'SELECT xor(1, 0)',
          mysql: 'SELECT 1 XOR 0',
        },
        write: {
          mysql: 'SELECT 1 XOR 0',
        },
      },
    );
    this.validateAll(
      'SELECT xor(0, 1, xor(1, 0, 0))',
      {
        write: {
          clickhouse: 'SELECT xor(0, 1, xor(1, 0, 0))',
          mysql: 'SELECT 0 XOR 1 XOR 1 XOR 0 XOR 0',
        },
      },
    );
    this.validateAll(
      'SELECT xor(xor(1, 0), 1)',
      {
        read: {
          clickhouse: 'SELECT xor(xor(1, 0), 1)',
          mysql: 'SELECT 1 XOR 0 XOR 1',
        },
        write: {
          clickhouse: 'SELECT xor(xor(1, 0), 1)',
          mysql: 'SELECT 1 XOR 0 XOR 1',
        },
      },
    );
    this.validateIdentity('SELECT xor(0, 1, 1, 0)');
    this.validateAll(
      'CONCAT(a, b)',
      {
        read: {
          clickhouse: 'CONCAT(a, b)',
          mysql: 'CONCAT(a, b)',
        },
        write: {
          mysql: 'CONCAT(a, b)',
          postgres: 'a || b',
        },
      },
    );
    this.validateAll(
      '\'Enum8(\\\'Sunday\\\' = 0)\'',
      {
        write: {
          clickhouse: '\'Enum8(\'\'Sunday\'\' = 0)\'',
        },
      },
    );
    this.validateAll(
      'SELECT uniq(x) FROM (SELECT any(y) AS x FROM (SELECT 1 AS y))',
      {
        read: {
          bigquery: 'SELECT APPROX_COUNT_DISTINCT(x) FROM (SELECT ANY_VALUE(y) x FROM (SELECT 1 y))',
        },
        write: {
          bigquery: 'SELECT APPROX_COUNT_DISTINCT(x) FROM (SELECT ANY_VALUE(y) AS x FROM (SELECT 1 AS y))',
        },
      },
    );
    this.validateAll(
      'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname',
      {
        write: {
          clickhouse: 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname',
          spark: 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname NULLS LAST',
        },
      },
    );
    this.validateAll(
      'CAST(1 AS NULLABLE(Int64))',
      {
        write: {
          clickhouse: 'CAST(1 AS Nullable(Int64))',
        },
      },
    );
    this.validateAll(
      'CAST(1 AS Nullable(DateTime64(6, \'UTC\')))',
      {
        write: {
          clickhouse: 'CAST(1 AS Nullable(DateTime64(6, \'UTC\')))',
        },
      },
    );
    this.validateAll(
      'SELECT x #! comment',
      {
        write: {
          '': 'SELECT x /* comment */',
        },
      },
    );
    this.validateAll(
      'SELECT quantileIf(0.5)(a, true)',
      {
        write: {
          clickhouse: 'SELECT quantileIf(0.5)(a, TRUE)',
        },
      },
    );
    this.validateIdentity(
      'SELECT POSITION(needle IN haystack)',
      'SELECT POSITION(haystack, needle)',
    );
    this.validateIdentity(
      'SELECT * FROM x LIMIT 10 SETTINGS max_results = 100, result = \'break\'',
    );
    this.validateIdentity('SELECT * FROM x LIMIT 10 SETTINGS max_results = 100, result_');
    this.validateIdentity('SELECT * FROM x FORMAT PrettyCompact');
    this.validateIdentity(
      'SELECT * FROM x LIMIT 10 SETTINGS max_results = 100, result_ FORMAT PrettyCompact',
    );
    this.validateAll(
      'SELECT * FROM foo JOIN bar USING id, name',
      {
        write: {
          clickhouse: 'SELECT * FROM foo JOIN bar USING (id, name)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM foo ANY LEFT JOIN bla ON foo.c1 = bla.c2',
      {
        write: {
          clickhouse: 'SELECT * FROM foo LEFT ANY JOIN bla ON foo.c1 = bla.c2',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM foo GLOBAL ANY LEFT JOIN bla ON foo.c1 = bla.c2',
      {
        write: {
          clickhouse: 'SELECT * FROM foo GLOBAL LEFT ANY JOIN bla ON foo.c1 = bla.c2',
        },
      },
    );
    this.validateAll(
      `
            SELECT
                loyalty,
                count()
            FROM hits SEMI LEFT JOIN users USING (UserID)
            GROUP BY loyalty
            ORDER BY loyalty ASC
            `,
      {
        write: {
          clickhouse: 'SELECT loyalty, count() FROM hits LEFT SEMI JOIN users USING (UserID)'
            + ' GROUP BY loyalty ORDER BY loyalty ASC',
        },
      },
    );
    this.validateAll(
      'SELECT quantile(0.5)(a)',
      {
        read: {
          duckdb: 'SELECT quantile(a, 0.5)',
          clickhouse: 'SELECT median(a)',
        },
        write: {
          clickhouse: 'SELECT quantile(0.5)(a)',
        },
      },
    );
    this.validateAll(
      'SELECT quantiles(0.5, 0.4)(a)',
      {
        read: {
          duckdb: 'SELECT quantile(a, [0.5, 0.4])',
        },
        write: {
          clickhouse: 'SELECT quantiles(0.5, 0.4)(a)',
        },
      },
    );
    this.validateAll(
      'SELECT quantiles(0.5)(a)',
      {
        read: {
          duckdb: 'SELECT quantile(a, [0.5])',
        },
        write: {
          clickhouse: 'SELECT quantiles(0.5)(a)',
        },
      },
    );

    this.validateIdentity('SELECT isNaN(x)');
    this.validateAll(
      'SELECT IS_NAN(x), ISNAN(x)',
      {
        write: {
          clickhouse: 'SELECT isNaN(x), isNaN(x)',
        },
      },
    );

    this.validateIdentity('SELECT startsWith(\'a\', \'b\')');
    this.validateAll(
      'SELECT STARTS_WITH(\'a\', \'b\'), STARTSWITH(\'a\', \'b\')',
      {
        write: {
          clickhouse: 'SELECT startsWith(\'a\', \'b\'), startsWith(\'a\', \'b\')',
        },
      },
    );
    this.validateIdentity('SYSTEM STOP MERGES foo.bar', undefined, {
      checkCommandWarning: true,
    });

    this.validateIdentity(
      'INSERT INTO FUNCTION s3(\'url\', \'CSV\', \'name String, value UInt32\', \'gzip\') SELECT name, value FROM existing_table',
    );
    this.validateIdentity(
      'INSERT INTO FUNCTION remote(\'localhost\', default.simple_table) VALUES (100, \'inserted via remote()\')',
      'INSERT INTO FUNCTION remote(\'localhost\', default.simple_table) VALUES ((100), (\'inserted via remote()\'))',
    );
    this.validateIdentity(
      'INSERT INTO TABLE FUNCTION hdfs(\'hdfs://hdfs1:9000/test\', \'TSV\', \'name String, column2 UInt32, column3 UInt32\') VALUES (\'test\', 1, 2)',
      'INSERT INTO FUNCTION hdfs(\'hdfs://hdfs1:9000/test\', \'TSV\', \'name String, column2 UInt32, column3 UInt32\') VALUES ((\'test\'), (1), (2))',
    );

    this.validateIdentity('SELECT 1 FORMAT TabSeparated');
    this.validateIdentity('SELECT * FROM t FORMAT TabSeparated');
    this.validateIdentity('SELECT FORMAT');
    this.validateIdentity('1 AS FORMAT').assertIs(AliasExpr);

    this.validateIdentity('SELECT formatDateTime(NOW(), \'%Y-%m-%d\', \'%T\')');
    this.validateAll(
      'SELECT formatDateTime(NOW(), \'%Y-%m-%d\')',
      {
        read: {
          clickhouse: 'SELECT formatDateTime(NOW(), \'%Y-%m-%d\')',
          mysql: 'SELECT DATE_FORMAT(NOW(), \'%Y-%m-%d\')',
        },
        write: {
          clickhouse: 'SELECT formatDateTime(NOW(), \'%Y-%m-%d\')',
          mysql: 'SELECT DATE_FORMAT(NOW(), \'%Y-%m-%d\')',
        },
      },
    );

    this.validateIdentity('ALTER TABLE visits DROP PARTITION 201901');
    this.validateIdentity('ALTER TABLE visits DROP PARTITION ALL');
    this.validateIdentity(
      'ALTER TABLE visits DROP PARTITION tuple(toYYYYMM(toDate(\'2019-01-25\')))',
    );
    this.validateIdentity('ALTER TABLE visits DROP PARTITION ID \'201901\'');

    this.validateIdentity('ALTER TABLE visits REPLACE PARTITION 201901 FROM visits_tmp');
    this.validateIdentity('ALTER TABLE visits REPLACE PARTITION ALL FROM visits_tmp');
    this.validateIdentity(
      'ALTER TABLE visits REPLACE PARTITION tuple(toYYYYMM(toDate(\'2019-01-25\'))) FROM visits_tmp',
    );
    this.validateIdentity('ALTER TABLE visits REPLACE PARTITION ID \'201901\' FROM visits_tmp');
    this.validateIdentity('ALTER TABLE visits ON CLUSTER test_cluster DROP COLUMN col1');
    this.validateIdentity('ALTER TABLE visits ON CLUSTER \'{cluster}\' DROP COLUMN col1');
    this.validateIdentity('DELETE FROM tbl ON CLUSTER test_cluster WHERE date = \'2019-01-01\'');
    this.validateIdentity('DELETE FROM tbl ON CLUSTER \'{cluster}\' WHERE date = \'2019-01-01\'');

    expect(
      parseOne('Tuple(select Int64)', {
        into: DataTypeExpr,
        read: 'clickhouse',
      }),
    ).toBeInstanceOf(DataTypeExpr);

    this.validateIdentity(
      'INSERT INTO t (col1, col2) VALUES (\'abcd\', 1234)',
      'INSERT INTO t (col1, col2) VALUES ((\'abcd\'), (1234))',
    );
    this.validateAll(
      'INSERT INTO t (col1, col2) VALUES (\'abcd\', 1234)',
      {
        write: {
          clickhouse: 'INSERT INTO t (col1, col2) VALUES ((\'abcd\'), (1234))',
          postgres: 'INSERT INTO t (col1, col2) VALUES ((\'abcd\'), (1234))',
        },
      },
    );
    this.validateIdentity('SELECT TRIM(TRAILING \')\' FROM \'(   Hello, world!   )\')');
    this.validateIdentity('SELECT TRIM(LEADING \'(\' FROM \'(   Hello, world!   )\')');
    this.validateIdentity('current_timestamp').assertIs(ColumnExpr);

    this.validateIdentity('SELECT * APPLY(sum) FROM columns_transformers');
    this.validateIdentity('SELECT COLUMNS(\'[jk]\') APPLY(toString) FROM columns_transformers');
    this.validateIdentity(
      'SELECT COLUMNS(\'[jk]\') APPLY(toString) APPLY(length) APPLY(max) FROM columns_transformers',
    );
    this.validateIdentity('SELECT * APPLY(sum), COLUMNS(\'col\') APPLY(sum) APPLY(avg) FROM t');
    this.validateIdentity(
      'SELECT * FROM ABC WHERE hasAny(COLUMNS(\'.*field\') APPLY(toUInt64) APPLY(to), (SELECT groupUniqArray(toUInt64(field))))',
    );
    this.validateIdentity('SELECT col apply', 'SELECT col AS apply');
    this.validateIdentity(
      'SELECT name FROM data WHERE (SELECT DISTINCT name FROM data) IS NOT NULL',
      'SELECT name FROM data WHERE NOT ((SELECT DISTINCT name FROM data) IS NULL)',
    );

    this.validateIdentity('SELECT 1_2_3_4_5');
    this.validateIdentity('SELECT 1_b', 'SELECT 1_b');
    this.validateIdentity(
      'SELECT COUNT(1) FROM table SETTINGS additional_table_filters = {\'a\': \'b\', \'c\': \'d\'}',
    );
    this.validateIdentity('SELECT arrayConcat([1, 2], [3, 4])');

    this.validateIdentity('SELECT parseDateTime(\'2021-01-04+23:00:00\', \'%Y-%m-%d+%H:%i:%s\')');
    this.validateIdentity(
      'SELECT parseDateTime(\'2021-01-04+23:00:00\', \'%Y-%m-%d+%H:%i:%s\', \'Asia/Istanbul\')',
    );

    this.validateIdentity('farmFingerprint64(x1, x2, x3)');

    this.validateIdentity('cosineDistance(x, y)');
    this.validateIdentity('L2Distance(x, y)');
    this.validateIdentity('tuple(1 = 1, \'foo\' = \'foo\')');

    this.validateIdentity('SELECT LIKE(a, b)', 'SELECT a LIKE b');
    this.validateIdentity('SELECT notLike(a, b)', 'SELECT NOT a LIKE b');
    this.validateIdentity('SELECT ilike(a, b)', 'SELECT a ILIKE b');

    this.validateIdentity('currentDatabase()', 'CURRENT_DATABASE()');
    this.validateIdentity('currentSchemas(TRUE)', 'CURRENT_SCHEMAS(TRUE)');

    this.validateIdentity(
      'SELECT quantilesExactExclusive(0.25, 0.5, 0.75)(x) AS y FROM (SELECT number AS x FROM num)',
    );

    this.validateIdentity('SELECT or(0, 1, -2)', 'SELECT 0 OR 1 OR -2');
    this.validateIdentity('SELECT and(1, 2, 3)', 'SELECT 1 AND 2 AND 3');
    this.validateIdentity('SELECT or(and(3, 0), 5)', 'SELECT (3 AND 0) OR 5');
  }

  testClickhouseValues () {
    const ast = this.parseOne('SELECT * FROM VALUES (1, 2, 3)');

    expect(Array.from(ast.findAll(TupleExpr)).length).toBe(4);

    const valuesExpr = select('*').from(
      values([
        [
          1,
          2,
          3,
        ],
      ], {
        alias: 'subq',
        columns: [
          'a',
          'b',
          'c',
        ],
      }),
    );

    expect(valuesExpr.sql({
      dialect: 'clickhouse',
    })).toBe(
      'SELECT * FROM (SELECT 1 AS a, 2 AS b, 3 AS c) AS subq',
    );

    this.validateIdentity('SELECT * FROM VALUES ((1, 1), (2, 1), (3, 1), (4, 1))');
    this.validateIdentity(
      'SELECT type, id FROM VALUES (\'id Int, type Int\', (1, 1), (2, 1), (3, 1), (4, 1))',
    );

    this.validateIdentity(
      'INSERT INTO t (col1, col2) VALUES (\'abcd\', 1234)',
      'INSERT INTO t (col1, col2) VALUES ((\'abcd\'), (1234))',
    );
    this.validateIdentity(
      'INSERT INTO t (col1, col2) FORMAT Values(\'abcd\', 1234)',
      'INSERT INTO t (col1, col2) VALUES ((\'abcd\'), (1234))',
    );

    this.validateAll(
      'SELECT col FROM (SELECT 1 AS col) AS _t',
      {
        read: {
          duckdb: 'SELECT col FROM (VALUES (1)) AS _t(col)',
        },
      },
    );
    this.validateAll(
      'SELECT col1, col2 FROM (SELECT 1 AS col1, 2 AS col2 UNION ALL SELECT 3, 4) AS _t',
      {
        read: {
          duckdb: 'SELECT col1, col2 FROM (VALUES (1, 2), (3, 4)) AS _t(col1, col2)',
        },
      },
    );
  }

  testCte () {
    this.validateIdentity('WITH \'x\' AS foo SELECT foo');
    this.validateIdentity('WITH [\'c\'] AS field_names SELECT field_names');
    this.validateIdentity('WITH SUM(bytes) AS foo SELECT foo FROM system.parts');
    this.validateIdentity('WITH (SELECT foo) AS bar SELECT bar + 5');
    this.validateIdentity('WITH test1 AS (SELECT i + 1, j + 1 FROM test1) SELECT * FROM test1');

    const query = parseOne('WITH (SELECT 1) AS y SELECT * FROM y', {
      read: 'clickhouse',
    });
    const withExpr = query.getArgKey('with') as Expression;
    const cteExprs = withExpr.getArgKey('expressions') as Expression[];

    expect(cteExprs[0].args.this).toBeInstanceOf(SubqueryExpr);
    expect(cteExprs[0].alias).toBe('y');

    const querySql = 'WITH 1 AS var SELECT var';

    for (const errorLevel of [
      ErrorLevel.IGNORE,
      ErrorLevel.RAISE,
      ErrorLevel.IMMEDIATE,
    ]) {
      expect(
        this.parseOne(querySql, {
          errorLevel,
        }).sql({
          dialect: this.dialect,
        }),
      ).toBe(querySql);
    }

    this.validateIdentity('arraySlice(x, 1)');
  }

  testTernary () {
    this.validateAll(
      'x ? 1 : 2',
      {
        write: {
          clickhouse: 'CASE WHEN x THEN 1 ELSE 2 END',
        },
      },
    );
    this.validateAll(
      'IF(BAR(col), sign > 0 ? FOO() : 0, 1)',
      {
        write: {
          clickhouse: 'CASE WHEN BAR(col) THEN CASE WHEN sign > 0 THEN FOO() ELSE 0 END ELSE 1 END',
        },
      },
    );
    this.validateAll(
      'x AND FOO() > 3 + 2 ? 1 : 2',
      {
        write: {
          clickhouse: 'CASE WHEN x AND FOO() > 3 + 2 THEN 1 ELSE 2 END',
        },
      },
    );
    this.validateAll(
      'x ? (y ? 1 : 2) : 3',
      {
        write: {
          clickhouse: 'CASE WHEN x THEN (CASE WHEN y THEN 1 ELSE 2 END) ELSE 3 END',
        },
      },
    );
    this.validateAll(
      'x AND (foo() ? FALSE : TRUE) ? (y ? 1 : 2) : 3',
      {
        write: {
          clickhouse: 'CASE WHEN x AND (CASE WHEN foo() THEN FALSE ELSE TRUE END) THEN (CASE WHEN y THEN 1 ELSE 2 END) ELSE 3 END',
        },
      },
    );

    const ternary = parseOne('x ? (y ? 1 : 2) : 3', {
      read: 'clickhouse',
    });

    expect(ternary).toBeInstanceOf(IfExpr);
    expect(ternary.args.this).toBeInstanceOf(ColumnExpr);
    expect(ternary.getArgKey('true')).toBeInstanceOf(ParenExpr);
    expect(ternary.getArgKey('false')).toBeInstanceOf(LiteralExpr);

    const nestedTernary = (ternary.getArgKey('true') as Expression).args.this as Expression;

    expect(nestedTernary.args.this).toBeInstanceOf(ColumnExpr);
    expect(nestedTernary.getArgKey('true')).toBeInstanceOf(LiteralExpr);
    expect(nestedTernary.getArgKey('false')).toBeInstanceOf(LiteralExpr);

    (parseOne('a and b ? 1 : 2', {
      read: 'clickhouse',
    }).assertIs(IfExpr).args.this as Expression).assertIs(AndExpr);
  }

  testParameterization () {
    this.validateAll(
      'SELECT {abc: UInt32}, {b: String}, {c: DateTime},{d: Map(String, Array(UInt8))}, {e: Tuple(UInt8, String)}',
      {
        write: {
          clickhouse: 'SELECT {abc: UInt32}, {b: String}, {c: DateTime}, {d: Map(String, Array(UInt8))}, {e: Tuple(UInt8, String)}',
          '': 'SELECT :abc, :b, :c, :d, :e',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM {table: Identifier}',
      {
        write: {
          clickhouse: 'SELECT * FROM {table: Identifier}',
        },
      },
    );
  }

  testSignedAndUnsignedTypes () {
    const dataTypes = [
      'UInt8',
      'UInt16',
      'UInt32',
      'UInt64',
      'UInt128',
      'UInt256',
      'Int8',
      'Int16',
      'Int32',
      'Int64',
      'Int128',
      'Int256',
    ];

    for (const dataType of dataTypes) {
      this.validateAll(
        `pow(2, 32)::${dataType}`,
        {
          write: {
            clickhouse: `CAST(pow(2, 32) AS ${dataType})`,
          },
        },
      );
    }
  }

  testGeomTypes () {
    const dataTypes = [
      'Point',
      'Ring',
      'LineString',
      'MultiLineString',
      'Polygon',
      'MultiPolygon',
    ];

    for (const dataType of dataTypes) {
      this.validateIdentity(`SELECT CAST(val AS ${dataType})`);
    }
  }

  testNothingType () {
    const dataTypes = [
      'Nothing',
      'Nullable(Nothing)',
    ];

    for (const dataType of dataTypes) {
      this.validateIdentity(`SELECT CAST(val AS ${dataType})`);
    }
  }

  testAggregateFunctionColumnWithAnyKeyword () {
    // Regression test for https://github.com/tobymao/sqlglot/issues/4723
    this.validateAll(
      `
            CREATE TABLE my_db.my_table
            (
                someId UUID,
                aggregatedColumn AggregateFunction(any, String),
                aggregatedColumnWithParams AggregateFunction(any(somecolumn), String),
            )
            ENGINE = AggregatingMergeTree()
            ORDER BY (someId)
                    `,
      {
        write: {
          clickhouse: `CREATE TABLE my_db.my_table (
  someId UUID,
  aggregatedColumn AggregateFunction(any, String),
  aggregatedColumnWithParams AggregateFunction(any(somecolumn), String)
)
ENGINE=AggregatingMergeTree()
ORDER BY (
  someId
)`,
        },
        pretty: true,
      },
    );
  }

  testCreateTableAsAlias () {
    const ctasAlias = 'CREATE TABLE my_db.my_table AS another_db.another_table';

    const expected = new CreateExpr({
      this: toTable('my_db.my_table'),
      kind: CreateExprKind.TABLE,
      expression: toTable('another_db.another_table'),
    });

    expect(this.parseOne(ctasAlias)).toEqual(expected);
    this.validateIdentity(ctasAlias);
  }

  testDdl () {
    const dbTableExpr = new TableExpr({
      this: undefined,
      db: toIdent('foo'),
      catalog: undefined,
    });
    const createWithCluster = new CreateExpr({
      this: dbTableExpr,
      kind: CreateExprKind.DATABASE,
      properties: new PropertiesExpr({
        expressions: [
          new OnClusterExpr({
            this: toIdent('c'),
          }),
        ],
      }),
    });

    expect(createWithCluster.sql({
      dialect: 'clickhouse',
    })).toBe('CREATE DATABASE foo ON CLUSTER c');

    // Transpiled CREATE SCHEMA may have OnCluster property set
    const createSchemaWithCluster = new CreateExpr({
      this: dbTableExpr,
      kind: CreateExprKind.SCHEMA,
      properties: new PropertiesExpr({
        expressions: [
          new OnClusterExpr({
            this: toIdent('c'),
          }),
        ],
      }),
    });

    expect(createSchemaWithCluster.sql({
      dialect: 'clickhouse',
    })).toBe('CREATE DATABASE foo ON CLUSTER c');

    const ctasWithComment = new CreateExpr({
      this: table_('foo'),
      kind: CreateExprKind.TABLE,
      expression: select('*').from('db.other_table'),
      properties: new PropertiesExpr({
        expressions: [
          new EnginePropertyExpr({
            this: var_('Memory'),
          }),
          new SchemaCommentPropertyExpr({
            this: LiteralExpr.string('foo'),
          }),
        ],
      }),
    });

    expect(ctasWithComment.sql({
      dialect: 'clickhouse',
    })).toBe(
      'CREATE TABLE foo ENGINE=Memory AS (SELECT * FROM db.other_table) COMMENT \'foo\'',
    );

    this.validateIdentity('CREATE FUNCTION linear_equation AS (x, k, b) -> k * x + b');
    this.validateIdentity('CREATE MATERIALIZED VIEW a.b TO a.c (c Int32) AS SELECT * FROM a.d');
    this.validateIdentity('CREATE TABLE ip_data (ip4 IPv4, ip6 IPv6) ENGINE=TinyLog()');
    this.validateIdentity('CREATE TABLE dates (dt1 Date32) ENGINE=TinyLog()');
    this.validateIdentity('CREATE TABLE named_tuples (a Tuple(select String, i Int64))');
    this.validateIdentity('CREATE TABLE t (a String) EMPTY AS SELECT * FROM dummy');
    this.validateIdentity(
      'CREATE TABLE t1 (a String EPHEMERAL, b String EPHEMERAL func(), c String MATERIALIZED func(), d String ALIAS func()) ENGINE=TinyLog()',
    );
    this.validateIdentity(
      'CREATE TABLE t (a String, b String, c UInt64, PROJECTION p1 (SELECT a, sum(c) GROUP BY a, b), PROJECTION p2 (SELECT b, sum(c) GROUP BY b)) ENGINE=MergeTree()',
    );
    this.validateIdentity(
      'CREATE TABLE xyz (ts DateTime, data String) ENGINE=MergeTree() ORDER BY ts SETTINGS index_granularity = 8192 COMMENT \'{"key": "value"}\'',
    );
    this.validateIdentity(
      'INSERT INTO FUNCTION s3(\'a\', \'b\', \'c\', \'d\', \'e\') PARTITION BY CONCAT(s1, s2, s3, s4) SETTINGS set1 = 1, set2 = \'2\' SELECT * FROM some_table SETTINGS foo = 3',
    );
    this.validateIdentity(
      'CREATE TABLE data5 ("x" UInt32, "y" UInt32) ENGINE=MergeTree ORDER BY (round(y / 1000000000), cityHash64(x)) SAMPLE BY cityHash64(x)',
    );
    this.validateIdentity(
      'CREATE TABLE foo (x UInt32) TTL time_column + INTERVAL \'1\' MONTH DELETE WHERE column = \'value\'',
    );
    this.validateIdentity(
      'CREATE FUNCTION parity_str AS (n) -> IF(n % 2, \'odd\', \'even\')',
      'CREATE FUNCTION parity_str AS n -> CASE WHEN n % 2 THEN \'odd\' ELSE \'even\' END',
    );
    this.validateIdentity(
      'CREATE TABLE a ENGINE=Memory AS SELECT 1 AS c COMMENT \'foo\'',
      'CREATE TABLE a ENGINE=Memory AS (SELECT 1 AS c) COMMENT \'foo\'',
    );
    this.validateIdentity(
      'CREATE TABLE t1 ("x" UInt32, "y" Dynamic, "z" Dynamic(max_types = 10)) ENGINE=MergeTree ORDER BY x',
    );

    this.validateAll(
      'CREATE DATABASE x',
      {
        read: {
          duckdb: 'CREATE SCHEMA x',
        },
        write: {
          clickhouse: 'CREATE DATABASE x',
          duckdb: 'CREATE SCHEMA x',
        },
      },
    );
    this.validateAll(
      'DROP DATABASE x',
      {
        read: {
          duckdb: 'DROP SCHEMA x',
        },
        write: {
          clickhouse: 'DROP DATABASE x',
          duckdb: 'DROP SCHEMA x',
        },
      },
    );
    this.validateAll(
      `
            CREATE TABLE example1 (
               timestamp DateTime,
               x UInt32 TTL now() + INTERVAL 1 MONTH,
               y String TTL timestamp + INTERVAL 1 DAY,
               z String
            )
            ENGINE = MergeTree
            ORDER BY tuple()
            `,
      {
        write: {
          clickhouse: `CREATE TABLE example1 (
  timestamp DateTime,
  x UInt32 TTL now() + INTERVAL '1' MONTH,
  y String TTL timestamp + INTERVAL '1' DAY,
  z String
)
ENGINE=MergeTree
ORDER BY tuple()`,
        },
        pretty: true,
      },
    );
    this.validateAll(
      `
            CREATE TABLE test (id UInt64, timestamp DateTime64, data String, max_hits UInt64, sum_hits UInt64) ENGINE = MergeTree
            PRIMARY KEY (id, toStartOfDay(timestamp), timestamp)
            TTL timestamp + INTERVAL 1 DAY
            GROUP BY id, toStartOfDay(timestamp)
            SET
               max_hits = max(max_hits),
               sum_hits = sum(sum_hits)
            `,
      {
        write: {
          clickhouse: `CREATE TABLE test (
  id UInt64,
  timestamp DateTime64,
  data String,
  max_hits UInt64,
  sum_hits UInt64
)
ENGINE=MergeTree
PRIMARY KEY (id, dateTrunc('DAY', timestamp), timestamp)
TTL
  timestamp + INTERVAL '1' DAY
GROUP BY
  id,
  dateTrunc('DAY', timestamp)
SET
  max_hits = max(max_hits),
  sum_hits = sum(sum_hits)`,
        },
        pretty: true,
      },
    );
    this.validateAll(
      `
            CREATE TABLE test (id String, data String) ENGINE = AggregatingMergeTree()
                ORDER BY tuple()
            SETTINGS
                max_suspicious_broken_parts=500,
                parts_to_throw_insert=100
            `,
      {
        write: {
          clickhouse: `CREATE TABLE test (
  id String,
  data String
)
ENGINE=AggregatingMergeTree()
ORDER BY tuple()
SETTINGS
  max_suspicious_broken_parts = 500,
  parts_to_throw_insert = 100`,
        },
        pretty: true,
      },
    );
    this.validateAll(
      `
            CREATE TABLE example_table
            (
                d DateTime,
                a Int
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(d)
            ORDER BY d
            TTL d + INTERVAL 1 MONTH DELETE,
                d + INTERVAL 1 WEEK TO VOLUME 'aaa',
                d + INTERVAL 2 WEEK TO DISK 'bbb';
            `,
      {
        write: {
          clickhouse: `CREATE TABLE example_table (
  d DateTime,
  a Int32
)
ENGINE=MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL
  d + INTERVAL '1' MONTH DELETE,
  d + INTERVAL '1' WEEK TO VOLUME 'aaa',
  d + INTERVAL '2' WEEK TO DISK 'bbb'`,
        },
        pretty: true,
      },
    );
    this.validateAll(
      `
            CREATE TABLE table_with_where
            (
                d DateTime,
                a Int
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(d)
            ORDER BY d
            TTL d + INTERVAL 1 MONTH DELETE WHERE toDayOfWeek(d) = 1;
            `,
      {
        write: {
          clickhouse: `CREATE TABLE table_with_where (
  d DateTime,
  a Int32
)
ENGINE=MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL
  d + INTERVAL '1' MONTH DELETE
WHERE
  toDayOfWeek(d) = 1`,
        },
        pretty: true,
      },
    );
    this.validateAll(
      `
            CREATE TABLE table_for_recompression
            (
                d DateTime,
                key UInt64,
                value String
            ) ENGINE MergeTree()
            ORDER BY tuple()
            PARTITION BY key
            TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(17)), d + INTERVAL 1 YEAR RECOMPRESS CODEC(LZ4HC(10))
            SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
            `,
      {
        write: {
          clickhouse: `CREATE TABLE table_for_recompression (
  d DateTime,
  key UInt64,
  value String
)
ENGINE=MergeTree()
ORDER BY tuple()
PARTITION BY key
TTL
  d + INTERVAL '1' MONTH RECOMPRESS CODEC(ZSTD(17)),
  d + INTERVAL '1' YEAR RECOMPRESS CODEC(LZ4HC(10))
SETTINGS
  min_rows_for_wide_part = 0,
  min_bytes_for_wide_part = 0`,
        },
        pretty: true,
      },
    );
    this.validateAll(
      `
            CREATE TABLE table_for_aggregation
            (
                d DateTime,
                k1 Int,
                k2 Int,
                x Int,
                y Int
            )
            ENGINE = MergeTree
            ORDER BY (k1, k2)
            TTL d + INTERVAL 1 MONTH GROUP BY k1, k2 SET x = max(x), y = min(y);
            `,
      {
        write: {
          clickhouse: `CREATE TABLE table_for_aggregation (
  d DateTime,
  k1 Int32,
  k2 Int32,
  x Int32,
  y Int32
)
ENGINE=MergeTree
ORDER BY (k1, k2)
TTL
  d + INTERVAL '1' MONTH
GROUP BY
  k1,
  k2
SET
  x = max(x),
  y = min(y)`,
        },
        pretty: true,
      },
    );
    this.validateAll(
      `
            CREATE DICTIONARY discounts_dict (
                advertiser_id UInt64,
                discount_start_date Date,
                discount_end_date Date,
                amount Float64
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'discounts'))
            LIFETIME(MIN 1 MAX 1000)
            LAYOUT(RANGE_HASHED(range_lookup_strategy 'max'))
            RANGE(MIN discount_start_date MAX discount_end_date)
            `,
      {
        write: {
          clickhouse: `CREATE DICTIONARY discounts_dict (
  advertiser_id UInt64,
  discount_start_date DATE,
  discount_end_date DATE,
  amount Float64
)
PRIMARY KEY (id)
SOURCE(CLICKHOUSE(
  TABLE 'discounts'
))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED(
  range_lookup_strategy 'max'
))
RANGE(MIN discount_start_date MAX discount_end_date)`,
        },
        pretty: true,
      },
    );
    this.validateAll(
      `
            CREATE DICTIONARY my_ip_trie_dictionary (
                prefix String,
                asn UInt32,
                cca2 String DEFAULT '??'
            )
            PRIMARY KEY prefix
            SOURCE(CLICKHOUSE(TABLE 'my_ip_addresses'))
            LAYOUT(IP_TRIE)
            LIFETIME(3600);
            `,
      {
        write: {
          clickhouse: `CREATE DICTIONARY my_ip_trie_dictionary (
  prefix String,
  asn UInt32,
  cca2 String DEFAULT '??'
)
PRIMARY KEY (prefix)
SOURCE(CLICKHOUSE(
  TABLE 'my_ip_addresses'
))
LAYOUT(IP_TRIE())
LIFETIME(MIN 0 MAX 3600)`,
        },
        pretty: true,
      },
    );
    this.validateAll(
      `
            CREATE DICTIONARY polygons_test_dictionary
            (
                key Array(Array(Array(Tuple(Float64, Float64)))),
                name String
            )
            PRIMARY KEY key
            SOURCE(CLICKHOUSE(TABLE 'polygons_test_table'))
            LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
            LIFETIME(0);
            `,
      {
        write: {
          clickhouse: `CREATE DICTIONARY polygons_test_dictionary (
  key Array(Array(Array(Tuple(Float64, Float64)))),
  name String
)
PRIMARY KEY (key)
SOURCE(CLICKHOUSE(
  TABLE 'polygons_test_table'
))
LAYOUT(POLYGON(
  STORE_POLYGON_KEY_COLUMN 1
))
LIFETIME(MIN 0 MAX 0)`,
        },
        pretty: true,
      },
    );
    this.validateAll(
      `
            CREATE TABLE t (
                a AggregateFunction(quantiles(0.5, 0.9), UInt64),
                b AggregateFunction(quantiles, UInt64),
                c SimpleAggregateFunction(sum, Float64),
                d AggregateFunction(count)
            )`,
      {
        write: {
          clickhouse: `CREATE TABLE t (
  a AggregateFunction(quantiles(0.5, 0.9), UInt64),
  b AggregateFunction(quantiles, UInt64),
  c SimpleAggregateFunction(sum, Float64),
  d AggregateFunction(count)
)`,
        },
        pretty: true,
      },
    );

    expect(
      this.validateIdentity('CREATE TABLE t1 (a String MATERIALIZED func())').find(
        ColumnConstraintExpr,
      ),
    ).toBeDefined();

    this.validateAll(
      `
            CREATE TABLE session_log
            (
                UserID UInt64,
                SessionID UUID
            )
            ENGINE = MergeTree
            PARTITION BY sipHash64(UserID) % 16
            ORDER BY tuple();
            `,
      {
        pretty: true,
      },
    );

    this.validateAll(
      `
            CREATE TABLE visits
            (
                VisitDate Date,
                Hour UInt8,
                ClientID UUID
            )
            ENGINE = MergeTree()
            PARTITION BY (toYYYYMM(VisitDate), Hour)
            ORDER BY Hour;
            `,
      {
        pretty: true,
      },
    );
  }

  testAggFunctions () {
    const extractAggFunc = (querySql: string) => {
      return (parseOne(querySql, {
        read: 'clickhouse',
      }) as SelectExpr).selects[0].args.this;
    };

    expect(
      extractAggFunc('select quantileGK(100, 0.95) OVER (PARTITION BY id) FROM table'),
    ).toBeInstanceOf(AnonymousAggFuncExpr);
    expect(
      extractAggFunc('select quantileGK(100, 0.95)(reading) OVER (PARTITION BY id) FROM table'),
    ).toBeInstanceOf(ParameterizedAggExpr);
    expect(
      extractAggFunc('select quantileGKIf(100, 0.95) OVER (PARTITION BY id) FROM table'),
    ).toBeInstanceOf(CombinedAggFuncExpr);
    expect(
      extractAggFunc('select quantileGKIf(100, 0.95)(reading) OVER (PARTITION BY id) FROM table'),
    ).toBeInstanceOf(CombinedParameterizedAggExpr);

    parseOne('foobar(x)').assertIs(AnonymousExpr);

    (this.validateIdentity('SELECT approx_top_sum(column, weight) FROM t') as SelectExpr).selects[0].assertIs(
      AnonymousAggFuncExpr,
    );
    (this.validateIdentity('SELECT approx_top_sum(N)(column, weight) FROM t') as SelectExpr).selects[0].assertIs(
      ParameterizedAggExpr,
    );
    (this.validateIdentity('SELECT approx_top_sum(N, reserved)(column, weight) FROM t') as SelectExpr).selects[0].assertIs(
      ParameterizedAggExpr,
    );
  }

  testDropOnCluster () {
    for (const creatable of [
      'DATABASE',
      'TABLE',
      'VIEW',
      'DICTIONARY',
      'FUNCTION',
    ]) {
      this.validateIdentity(`DROP ${creatable} test ON CLUSTER test_cluster`);
      this.validateIdentity(`DROP ${creatable} test ON CLUSTER '{cluster}'`);
    }
  }

  testDatetimeFuncs () {
    // Each datetime func has an alias that is roundtripped to the original name e.g. (DATE_SUB, DATESUB) -> DATE_SUB
    const datetimeFuncs: [string, string][] = [
      [
        'DATE_SUB',
        'DATESUB',
      ],
      [
        'DATE_ADD',
        'DATEADD',
      ],
    ];

    // 2-arg functions of type <func>(date, unit)
    for (const func of [
      ...datetimeFuncs,
      [
        'TIMESTAMP_ADD',
        'TIMESTAMPADD',
      ] as [string, string],
    ]) {
      const funcName = func[0];

      for (const funcAlias of func) {
        this.validateIdentity(
          `SELECT ${funcAlias}(date, INTERVAL '3' YEAR)`,
          `SELECT ${funcName}(date, INTERVAL '3' YEAR)`,
        );
      }
    }

    // 3-arg functions of type <func>(unit, value, date)
    for (const func of [
      ...datetimeFuncs,
      [
        'DATE_DIFF',
        'DATEDIFF',
      ] as [string, string],
      [
        'TIMESTAMP_SUB',
        'TIMESTAMPSUB',
      ] as [string, string],
    ]) {
      const funcName = func[0];

      for (const funcAlias of func) {
        this.validateIdentity(
          `SELECT ${funcAlias}(SECOND, 1, bar)`,
          `SELECT ${funcName}(SECOND, 1, bar)`,
        );
      }
    }
    // 4-arg functions of type <func>(unit, value, date, timezone)
    for (const func of [
      [
        'DATE_DIFF',
        'DATEDIFF',
      ] as [string, string],
    ]) {
      const funcName = func[0];

      for (const funcAlias of func) {
        this.validateIdentity(
          `SELECT ${funcAlias}(SECOND, 1, bar, 'UTC')`,
          `SELECT ${funcName}(SECOND, 1, bar, 'UTC')`,
        );
      }
    }
  }

  testConvert () {
    // Python: convert(date(2020, 1, 1)) → DateStrToDate
    expect(
      new DateStrToDateExpr({ this: LiteralExpr.string('2020-01-01') }).sql({
        dialect: this.dialect,
      }),
    ).toBe('toDate(\'2020-01-01\')');

    // no fractional seconds
    expect(
      convert(DateTime.local(2020, 1, 1, 0, 0, 1)).sql({
        dialect: this.dialect,
      }),
    ).toBe('CAST(\'2020-01-01 00:00:01\' AS DateTime64(6))');

    expect(
      convert(DateTime.utc(2020, 1, 1, 0, 0, 1)).sql({
        dialect: this.dialect,
      }),
    ).toBe('CAST(\'2020-01-01 00:00:01\' AS DateTime64(6, \'UTC\'))');

    // with fractional seconds
    // Luxon only has millisecond precision (vs Python's microsecond)
    expect(
      convert(DateTime.local(2020, 1, 1, 0, 0, 1, 1)).sql({
        dialect: this.dialect,
      }),
    ).toBe('CAST(\'2020-01-01 00:00:01.001000\' AS DateTime64(6))');
    expect(
      convert(DateTime.utc(2020, 1, 1, 0, 0, 1, 1)).sql({
        dialect: this.dialect,
      }),
    ).toBe('CAST(\'2020-01-01 00:00:01.001000\' AS DateTime64(6, \'UTC\'))');
  }

  testTimestrToTime () {
    // no fractional seconds
    const timeStrings = [
      '2020-01-01 00:00:01',
      '2020-01-01 00:00:01+01:00',
      ' 2020-01-01 00:00:01-01:00 ',
      '2020-01-01T00:00:01+01:00',
    ];

    for (const timeString of timeStrings) {
      expect(
        new TimeStrToTimeExpr({
          this: LiteralExpr.string(timeString),
        }).sql(
          {
            dialect: this.dialect,
          },
        ),
      ).toBe(`CAST('${timeString}' AS DateTime64(6))`);
    }

    const timeStringsNoUtc = [
      '2020-01-01 00:00:01',
      '2020-01-01 00:00:01',
      '2020-01-01 00:00:01',
      '2020-01-01 00:00:01',
    ];

    for (let i = 0; i < timeStrings.length; i++) {
      const utc = timeStrings[i];
      const noUtc = timeStringsNoUtc[i];

      expect(
        new TimeStrToTimeExpr({
          this: LiteralExpr.string(utc),
          zone: LiteralExpr.string('UTC'),
        }).sql({
          dialect: this.dialect,
        }),
      ).toBe(`CAST('${noUtc}' AS DateTime64(6, 'UTC'))`);
    }

    // with fractional seconds
    const timeStringsFrac = [
      '2020-01-01 00:00:01.001',
      '2020-01-01 00:00:01.000001',
      '2020-01-01 00:00:01.001+00:00',
      '2020-01-01 00:00:01.000001-00:00',
      '2020-01-01 00:00:01.0001',
      '2020-01-01 00:00:01.1+00:00',
    ];

    for (const timeString of timeStringsFrac) {
      expect(
        new TimeStrToTimeExpr({
          this: LiteralExpr.string(timeString[0]),
        }).sql(
          {
            dialect: this.dialect,
          },
        ),
      ).toBe(`CAST('${timeString[0]}' AS DateTime64(6))`);
    }

    const timeStringsNoUtcFrac = [
      '2020-01-01 00:00:01.001000',
      '2020-01-01 00:00:01.000001',
      '2020-01-01 00:00:01.001000',
      '2020-01-01 00:00:01.000001',
      '2020-01-01 00:00:01.000100',
      '2020-01-01 00:00:01.100000',
    ];

    for (let i = 0; i < timeStringsFrac.length; i++) {
      const utc = timeStringsFrac[i];
      const noUtc = timeStringsNoUtcFrac[i];

      expect(
        new TimeStrToTimeExpr({
          this: LiteralExpr.string(utc),
          zone: LiteralExpr.string('UTC'),
        }).sql({
          dialect: this.dialect,
        }),
      ).toBe(`CAST('${noUtc}' AS DateTime64(6, 'UTC'))`);
    }
  }

  testGrant () {
    this.validateIdentity('GRANT SELECT(x, y) ON db.table TO john WITH GRANT OPTION');
    this.validateIdentity('GRANT INSERT(x, y) ON db.table TO john');
  }

  testRevoke () {
    this.validateIdentity('REVOKE SELECT(x, y) ON db.table FROM john');
    this.validateIdentity('REVOKE INSERT(x, y) ON db.table FROM john');
  }

  testArrayJoin () {
    const exprAj = this.validateIdentity(
      'SELECT * FROM arrays_test ARRAY JOIN arr1, arrays_test.arr2 AS foo, [\'a\', \'b\', \'c\'] AS elem',
    );
    const joins = exprAj.getArgKey('joins') as Expression[];

    expect(joins.length).toBe(1);

    const join = joins[0];

    expect((join as any).kind).toBe('ARRAY');
    expect(join.args.this).toBeInstanceOf(ColumnExpr);

    const joinExprs = join.getArgKey('expressions') as Expression[];

    expect(joinExprs.length).toBe(2);
    expect(joinExprs[0]).toBeInstanceOf(AliasExpr);
    expect(joinExprs[0].args.this).toBeInstanceOf(ColumnExpr);

    expect(joinExprs[1]).toBeInstanceOf(AliasExpr);
    expect(joinExprs[1].args.this).toBeInstanceOf(ArrayExpr);

    this.validateIdentity('SELECT s, arr FROM arrays_test ARRAY JOIN arr');
    this.validateIdentity('SELECT s, arr, a FROM arrays_test LEFT ARRAY JOIN arr AS a');
    this.validateIdentity(
      'SELECT s, arr_external FROM arrays_test ARRAY JOIN [1, 2, 3] AS arr_external',
    );
    this.validateIdentity(
      'SELECT * FROM arrays_test ARRAY JOIN [1, 2, 3] AS arr_external1, [\'a\', \'b\', \'c\'] AS arr_external2, splitByString(\',\', \'asd,qwerty,zxc\') AS arr_external3',
    );
  }

  testTraverseScope () {
    const sql = 'SELECT * FROM t FINAL';
    const scopes = traverseScope(parseOne(sql, {
      read: this.dialect,
    }));

    expect(scopes.length).toBe(1);
    expect(new Set(Object.keys(scopes[0].sources))).toEqual(new Set(['t']));
  }

  testWindowFunctions () {
    this.validateIdentity(
      'SELECT row_number(column1) OVER (PARTITION BY column2 ORDER BY column3) FROM table',
    );
    this.validateIdentity(
      'SELECT row_number() OVER (PARTITION BY column2 ORDER BY column3) FROM table',
    );
  }

  testFunctions () {
    this.validateIdentity('SELECT TRANSFORM(foo, [1, 2], [\'first\', \'second\']) FROM table');
    this.validateIdentity(
      'SELECT TRANSFORM(foo, [1, 2], [\'first\', \'second\'], \'default\') FROM table',
    );
  }

  testArrayOffset () {
    const infoSpy = vi.spyOn(console, 'info').mockImplementation(() => {});

    try {
      this.validateAll(
        'SELECT col[1]',
        {
          write: {
            bigquery: 'SELECT col[0]',
            duckdb: 'SELECT col[1]',
            hive: 'SELECT col[0]',
            clickhouse: 'SELECT col[1]',
            presto: 'SELECT col[1]',
          },
        },
      );

      const logMessages = infoSpy.mock.calls.map((args) => String(args[0]));

      expect(logMessages.filter((m) => m.includes('Applying array index offset'))).toEqual([
        'Applying array index offset (-1)',
        'Applying array index offset (1)',
        'Applying array index offset (1)',
        'Applying array index offset (1)',
      ]);
    } finally {
      infoSpy.mockRestore();
    }
  }

  testToStartOf () {
    for (const unit of [
      'SECOND',
      'DAY',
      'YEAR',
    ]) {
      this.validateAll(
        `toStartOf${unit}(x)`,
        {
          write: {
            databricks: `DATE_TRUNC('${unit}', x)`,
            duckdb: `DATE_TRUNC('${unit}', x)`,
            doris: `DATE_TRUNC(x, '${unit}')`,
            presto: `DATE_TRUNC('${unit}', x)`,
            spark: `DATE_TRUNC('${unit}', x)`,
          },
        },
      );
    }

    this.validateAll(
      'toMonday(x)',
      {
        write: {
          databricks: 'DATE_TRUNC(\'WEEK\', x)',
          duckdb: 'DATE_TRUNC(\'WEEK\', x)',
          doris: 'DATE_TRUNC(x, \'WEEK\')',
          presto: 'DATE_TRUNC(\'WEEK\', x)',
          spark: 'DATE_TRUNC(\'WEEK\', x)',
        },
      },
    );
  }

  testStringSplit () {
    this.validateAll(
      'splitByString(\'s\', x)',
      {
        read: {
          bigquery: 'SPLIT(x, \'s\')',
          duckdb: 'STRING_SPLIT(x, \'s\')',
        },
        write: {
          clickhouse: 'splitByString(\'s\', x)',
          doris: 'SPLIT_BY_STRING(x, \'s\')',
          duckdb: 'STR_SPLIT(x, \'s\')',
          hive: 'SPLIT(x, CONCAT(\'\\\\Q\', \'s\', \'\\\\E\'))',
        },
      },
    );
    this.validateAll(
      'splitByRegexp(\'\\\\d+\', x)',
      {
        read: {
          duckdb: 'STRING_SPLIT_REGEX(x, \'\\d+\')',
          hive: 'SPLIT(x, \'\\\\d+\')',
        },
        write: {
          clickhouse: 'splitByRegexp(\'\\\\d+\', x)',
          duckdb: 'STR_SPLIT_REGEX(x, \'\\d+\')',
          hive: 'SPLIT(x, \'\\\\d+\')',
        },
      },
    );
    this.validateIdentity('splitByChar(\'\', x)');
  }
}

const t = new TestClickHouse();

describe('TestClickHouse', () => {
  test('clickhouse', () => t.testClickhouse());
  test('clickhouseValues', () => t.testClickhouseValues());
  test('cte', () => t.testCte());
  test('ternary', () => t.testTernary());
  test('parameterization', () => t.testParameterization());
  test('signedAndUnsignedTypes', () => t.testSignedAndUnsignedTypes());
  test('geomTypes', () => t.testGeomTypes());
  test('nothingType', () => t.testNothingType());
  test('aggregateFunctionColumnWithAnyKeyword', () => t.testAggregateFunctionColumnWithAnyKeyword());
  test('createTableAsAlias', () => t.testCreateTableAsAlias());
  test('ddl', () => t.testDdl());
  test('aggFunctions', () => t.testAggFunctions());
  test('dropOnCluster', () => t.testDropOnCluster());
  test('datetimeFuncs', () => t.testDatetimeFuncs());
  test('convert', () => t.testConvert());
  test('timestrToTime', () => t.testTimestrToTime());
  test('grant', () => t.testGrant());
  test('revoke', () => t.testRevoke());
  test('arrayJoin', () => t.testArrayJoin());
  test('traverseScope', () => t.testTraverseScope());
  test('windowFunctions', () => t.testWindowFunctions());
  test('functions', () => t.testFunctions());
  test('arrayOffset', () => t.testArrayOffset());
  test('toStartOf', () => t.testToStartOf());
  test('stringSplit', () => t.testStringSplit());
});
