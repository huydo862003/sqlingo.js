import {
  describe, test, expect, vi,
} from 'vitest';
import {
  parseOne, parse, transpile,
  ErrorLevel, ParseError, TokenError, UnsupportedError,
} from '../../../src/index';
import type {
  Expression,
  SelectExpr,
} from '../../../src/expressions';
import {
  DataTypeExpr, IdentifierExpr, JsonPathExpr, ParameterExpr,
  PredictExpr, FeaturesAtTimeExpr, VectorSearchExpr,
  GenerateEmbeddingExpr, MlTranslateExpr, MlForecastExpr,
  SafeFuncExpr, SubstringExpr, TimestampExpr, StrToDateExpr,
  StrToTimeExpr, ParseDatetimeExpr, NetFuncExpr, HostExpr,
  RegDomainExpr, PseudocolumnExpr, UnixSecondsExpr, TranslateExpr,
  TableExpr, convert, toTable,
} from '../../../src/expressions';
import {
  annotateTypes, qualify,
} from '../../../src/optimizer';
import {
  Validator,
} from './validator';

class TestBigQuery extends Validator {
  override dialect = 'bigquery' as const;

  testBigquery () {
    for (const prefix of ['c.db.', 'db.', '']) {
      const table = this.parseOne(`\`${prefix}INFORMATION_SCHEMA.X\``, { into: TableExpr }) as any;
      const this_ = table.args.this;
      expect(this_).toBeInstanceOf(IdentifierExpr);
      expect(this_.args.quoted).toBe(true);
      expect(this_.name).toBe('INFORMATION_SCHEMA.X');
    }

    let table = this.parseOne('x-0._y.z', { into: TableExpr }) as any;
    expect(table.catalog).toBe('x-0');
    expect(table.db).toBe('_y');
    expect(table.name).toBe('z');

    table = this.parseOne('x-0._y', { into: TableExpr }) as any;
    expect(table.db).toBe('x-0');
    expect(table.name).toBe('_y');

    this.validateIdentity('SAFE.SOME_RANDOM_FUNC(a, b, c)').assertIs(SafeFuncExpr);
    (this.validateIdentity(
      'SAFE.SUBSTR(\'foo\', 0, -2)',
    ).assertIs(SafeFuncExpr) as any).args.this.assertIs(SubstringExpr);
    (this.validateIdentity('SAFE.TIMESTAMP(foo, zone)').assertIs(SafeFuncExpr) as any).args.this.assertIs(TimestampExpr);
    (this.validateIdentity(
      'SAFE.PARSE_DATE(\'%Y-%m-%d\', \'2024-01-15\')',
      'SAFE.PARSE_DATE(\'%F\', \'2024-01-15\')',
    ).assertIs(SafeFuncExpr) as any).args.this.assertIs(StrToDateExpr);
    (this.validateIdentity(
      'SAFE.PARSE_DATETIME(\'%Y-%m-%d %H:%M:%S\', \'2024-01-15 10:30:00\')',
      'SAFE.PARSE_DATETIME(\'%F %T\', \'2024-01-15 10:30:00\')',
    ).assertIs(SafeFuncExpr) as any).args.this.assertIs(ParseDatetimeExpr);
    (this.validateIdentity(
      'SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\', \'2024-01-15 10:30:00\')',
      'SAFE.PARSE_TIMESTAMP(\'%F %T\', \'2024-01-15 10:30:00\')',
    ).assertIs(SafeFuncExpr) as any).args.this.assertIs(StrToTimeExpr);

    this.validateIdentity('TIMESTAMP(foo, zone)').assertIs(TimestampExpr);
    this.validateIdentity('SELECT * FROM x-0.y');
    expect(toTable('`a.b`.`c.d`', { dialect: 'bigquery' }).sql()).toBe('"a"."b"."c"."d"');
    expect(toTable('`x`.`y.z`', { dialect: 'bigquery' }).sql()).toBe('"x"."y"."z"');
    expect(toTable('`x.y.z`', { dialect: 'bigquery' }).sql()).toBe('"x"."y"."z"');
    expect(toTable('`x.y.z`', { dialect: 'bigquery' }).sql({ dialect: 'bigquery' })).toBe('`x.y.z`');
    expect(toTable('`x`.`y`', { dialect: 'bigquery' }).sql({ dialect: 'bigquery' })).toBe('`x`.`y`');

    const column = (this.validateIdentity('SELECT `db.t`.`c` FROM `db.t`') as any).selects[0];
    expect(column.parts.length).toBe(3);

    const selectWithQuotedUdf = this.validateIdentity('SELECT `p.d.UdF`(data) FROM `p.d.t`') as any;
    expect(selectWithQuotedUdf.selects[0].name).toBe('p.d.UdF');

    this.validateIdentity('SELECT EXP(1)');
    (this.validateIdentity('NET.HOST(\'http://example.com\')').assertIs(NetFuncExpr) as any).args.this.assertIs(HostExpr);
    (this.validateIdentity('NET.REG_DOMAIN(\'http://example.com\')').assertIs(NetFuncExpr) as any).args.this.assertIs(RegDomainExpr);
    (this.validateIdentity('DATE_TRUNC(x, @foo)') as any).args.unit.assertIs(ParameterExpr);
    this.validateIdentity('ARRAY_CONCAT_AGG(x ORDER BY ARRAY_LENGTH(x) LIMIT 2)');
    this.validateIdentity('ARRAY_CONCAT_AGG(x LIMIT 2)');
    this.validateIdentity('ARRAY_CONCAT_AGG(x ORDER BY ARRAY_LENGTH(x))');
    this.validateIdentity('ARRAY_CONCAT_AGG(x)');
    this.validateIdentity('PARSE_TIMESTAMP(\'%FT%H:%M:%E*S%z\', x)');
    this.validateIdentity('SELECT ARRAY_CONCAT([1])');
    this.validateIdentity('SELECT * FROM READ_CSV(\'bla.csv\')');
    this.validateIdentity('CAST(x AS STRUCT<list ARRAY<INT64>>)');
    this.validateIdentity('assert.true(1 = 1)');
    this.validateIdentity('SELECT jsondoc[\'some_key\']');
    this.validateIdentity('SELECT `p.d.UdF`(data).* FROM `p.d.t`');
    this.validateIdentity('SELECT * FROM `my-project.my-dataset.my-table`');
    this.validateIdentity('CREATE OR REPLACE TABLE `a.b.c` CLONE `a.b.d`');
    this.validateIdentity('SELECT x, 1 AS y GROUP BY 1 ORDER BY 1');
    this.validateIdentity('SELECT * FROM x.*');
    this.validateIdentity('SELECT * FROM x.y*');
    this.validateIdentity('CASE A WHEN 90 THEN \'red\' WHEN 50 THEN \'blue\' ELSE \'green\' END');
    this.validateIdentity('CREATE SCHEMA x DEFAULT COLLATE \'en\'');
    this.validateIdentity('CREATE TABLE x (y INT64) DEFAULT COLLATE \'en\'');
    this.validateIdentity('PARSE_JSON(\'{}\', wide_number_mode => \'exact\')');
    this.validateIdentity('FOO(values)');
    this.validateIdentity('STRUCT(values AS value)');

    this.validateIdentity('SELECT SEARCH(data_to_search, \'search_query\')');
    this.validateIdentity(
      'SELECT SEARCH(data_to_search, \'search_query\', json_scope => \'JSON_KEYS_AND_VALUES\')',
    );
    this.validateIdentity(
      'SELECT SEARCH(data_to_search, \'search_query\', analyzer => \'PATTERN_ANALYZER\')',
    );
    this.validateIdentity(
      'SELECT SEARCH(data_to_search, \'search_query\', analyzer_options => \'analyzer_options_values\')',
    );
    this.validateIdentity(
      'SELECT SEARCH(data_to_search, \'search_query\', json_scope => \'JSON_VALUES\', analyzer => \'LOG_ANALYZER\')',
    );
    this.validateIdentity(
      'SELECT SEARCH(data_to_search, \'search_query\', analyzer => \'PATTERN_ANALYZER\', analyzer_options => \'options\')',
    );

    this.validateIdentity('ARRAY_AGG(x IGNORE NULLS LIMIT 1)');
    this.validateIdentity('ARRAY_AGG(x IGNORE NULLS ORDER BY x LIMIT 1)');
    this.validateIdentity('ARRAY_AGG(DISTINCT x IGNORE NULLS ORDER BY x LIMIT 1)');
    this.validateIdentity('ARRAY_AGG(x IGNORE NULLS)');
    this.validateIdentity('ARRAY_AGG(DISTINCT x IGNORE NULLS HAVING MAX x ORDER BY x LIMIT 1)');
    this.validateIdentity('SELECT * FROM dataset.my_table TABLESAMPLE SYSTEM (10 PERCENT)');
    this.validateIdentity('TIME(\'2008-12-25 15:30:00+08\')');
    this.validateIdentity('TIME(\'2008-12-25 15:30:00+08\', \'America/Los_Angeles\')');
    this.validateIdentity('SELECT \'\\n\\r\\a\\v\\f\\t\'');
    this.validateIdentity('SELECT * FROM tbl FOR SYSTEM_TIME AS OF z');
    this.validateIdentity('SELECT PARSE_TIMESTAMP(\'%c\', \'Thu Dec 25 07:30:00 2008\', \'UTC\')');
    this.validateIdentity('SELECT ANY_VALUE(fruit HAVING MAX sold) FROM fruits');
    this.validateIdentity('SELECT ANY_VALUE(fruit HAVING MIN sold) FROM fruits');
    this.validateAll(
      'SELECT ANY_VALUE(fruit HAVING MAX sold) FROM Store',
      {
        write: {
          'bigquery': 'SELECT ANY_VALUE(fruit HAVING MAX sold) FROM Store',
          'duckdb': 'SELECT ARG_MAX_NULL(fruit, sold) FROM Store',
        },
      },
    );
    this.validateAll(
      'SELECT ANY_VALUE(fruit HAVING MIN sold) FROM Store',
      {
        write: {
          'bigquery': 'SELECT ANY_VALUE(fruit HAVING MIN sold) FROM Store',
          'duckdb': 'SELECT ARG_MIN_NULL(fruit, sold) FROM Store',
        },
      },
    );
    this.validateAll(
      'SELECT category, ANY_VALUE(product HAVING MAX price), ANY_VALUE(product HAVING MIN cost), ANY_VALUE(supplier) FROM products GROUP BY category',
      {
        write: {
          'bigquery': 'SELECT category, ANY_VALUE(product HAVING MAX price), ANY_VALUE(product HAVING MIN cost), ANY_VALUE(supplier) FROM products GROUP BY category',
          'duckdb': 'SELECT category, ARG_MAX_NULL(product, price), ARG_MIN_NULL(product, cost), ANY_VALUE(supplier) FROM products GROUP BY category',
        },
      },
    );
    this.validateAll(
      'WITH data AS (SELECT \'A\' AS fruit, 20 AS sold UNION ALL SELECT NULL AS fruit, 25 AS sold) SELECT ANY_VALUE(fruit HAVING MAX sold) FROM data',
      {
        write: {
          'duckdb': 'WITH data AS (SELECT \'A\' AS fruit, 20 AS sold UNION ALL SELECT NULL AS fruit, 25 AS sold) SELECT ARG_MAX_NULL(fruit, sold) FROM data',
        },
      },
    );
    this.validateIdentity('SELECT `project-id`.udfs.func(call.dir)');
    this.validateIdentity('SELECT CAST(CURRENT_DATE AS STRING FORMAT \'DAY\') AS current_day');
    this.validateIdentity('SAFE_CAST(encrypted_value AS STRING FORMAT \'BASE64\')');
    this.validateIdentity('CAST(encrypted_value AS STRING FORMAT \'BASE64\')');
    this.validateIdentity('DATE(2016, 12, 25)');
    this.validateIdentity('DATE(CAST(\'2016-12-25 23:59:59\' AS DATETIME))');
    this.validateIdentity('SELECT foo IN UNNEST(bar) AS bla');
    this.validateIdentity('SELECT * FROM x-0.a');
    this.validateIdentity('SELECT * FROM pivot CROSS JOIN foo');
    this.validateIdentity('SAFE_CAST(x AS STRING)');
    this.validateIdentity('SELECT * FROM a-b-c.mydataset.mytable');
    this.validateIdentity('SELECT * FROM abc-def-ghi');
    this.validateIdentity('SELECT * FROM a-b-c');
    this.validateIdentity('SELECT * FROM my-table');
    this.validateIdentity('SELECT * FROM my-project.mydataset.mytable');
    this.validateIdentity('SELECT * FROM pro-ject_id.c.d CROSS JOIN foo-bar');
    this.validateIdentity('SELECT * FROM foo.bar.25', 'SELECT * FROM foo.bar.`25`');
    this.validateIdentity('SELECT * FROM foo.bar.25_', 'SELECT * FROM foo.bar.`25_`');
    this.validateIdentity('SELECT * FROM foo.bar.25x a', 'SELECT * FROM foo.bar.`25x` AS a');
    this.validateIdentity('SELECT * FROM foo.bar.25ab c', 'SELECT * FROM foo.bar.`25ab` AS c');
    this.validateIdentity('x <> \'\'');
    this.validateIdentity('DATE_TRUNC(col, WEEK(MONDAY))');
    this.validateIdentity('DATE_TRUNC(col, MONTH, \'UTC+8\')');
    this.validateIdentity('SELECT b\'abc\'');
    this.validateIdentity('SELECT AS STRUCT 1 AS a, 2 AS b');
    this.validateIdentity('SELECT DISTINCT AS STRUCT 1 AS a, 2 AS b');
    this.validateIdentity('SELECT AS VALUE STRUCT(1 AS a, 2 AS b)');
    this.validateIdentity('SELECT * FROM q UNPIVOT(values FOR quarter IN (b, c))');
    this.validateIdentity('CREATE TABLE x (a STRUCT<values ARRAY<INT64>>)');
    this.validateIdentity('CREATE TABLE x (a STRUCT<b STRING OPTIONS (description=\'b\')>)');
    this.validateIdentity('CAST(x AS TIMESTAMP)');
    this.validateIdentity('BEGIN DECLARE y INT64', undefined, { checkCommandWarning: true });
    this.validateIdentity('LOOP SET x = x + 1', undefined, { checkCommandWarning: true });
    this.validateIdentity('REPEAT SET x = x + 1', undefined, { checkCommandWarning: true });
    this.validateIdentity('SELECT MAKE_INTERVAL(100, 11, 1, 12, 30, 10)');
    this.validateIdentity(
      'WHILE i < ARRAY_LENGTH(batches) DO SET x = batches[OFFSET(i)]',
      undefined,
      { checkCommandWarning: true },
    );
    this.validateIdentity('BEGIN TRANSACTION');
    this.validateIdentity('COMMIT TRANSACTION');
    this.validateIdentity('ROLLBACK TRANSACTION');
    this.validateIdentity('CAST(x AS BIGNUMERIC)');
    this.validateIdentity('SELECT y + 1 FROM x GROUP BY y + 1 ORDER BY 1');
    this.validateIdentity('SELECT TIMESTAMP_SECONDS(2) AS t');
    this.validateIdentity('SELECT TIMESTAMP_MILLIS(2) AS t');
    this.validateIdentity('UPDATE x SET y = NULL');
    this.validateIdentity('LOG(n, b)');
    this.validateIdentity('SELECT COUNT(x RESPECT NULLS)');
    this.validateIdentity('SELECT LAST_VALUE(x IGNORE NULLS) OVER y AS x');
    this.validateIdentity('SELECT ARRAY((SELECT AS STRUCT 1 AS a, 2 AS b))');
    this.validateIdentity('SELECT ARRAY((SELECT AS STRUCT 1 AS a, 2 AS b) LIMIT 10)');
    this.validateIdentity('CAST(x AS CHAR)', 'CAST(x AS STRING)');
    this.validateIdentity('CAST(x AS NCHAR)', 'CAST(x AS STRING)');
    this.validateIdentity('CAST(x AS NVARCHAR)', 'CAST(x AS STRING)');
    this.validateIdentity('CAST(x AS TIMESTAMPTZ)', 'CAST(x AS TIMESTAMP)');
    this.validateIdentity('CAST(x AS RECORD)', 'CAST(x AS STRUCT)');
    this.validateIdentity('SELECT * FROM x WHERE x.y >= (SELECT MAX(a) FROM b-c) - 20');
    (this.validateIdentity(
      'WITH t AS (SELECT \'{"x-y": "z"}\' AS c) SELECT JSON_EXTRACT(c, \'$.x-y\') FROM t',
    ) as any).selects[0].args.expression.assertIs(JsonPathExpr);
    this.validateIdentity(
      'SELECT FORMAT_TIMESTAMP(\'%F %T\', CURRENT_TIMESTAMP(), \'Europe/Berlin\') AS ts',
    );
    this.validateIdentity(
      'SELECT cars, apples FROM some_table PIVOT(SUM(total_counts) FOR products IN (\'general.cars\' AS cars, \'food.apples\' AS apples))',
    );
    this.validateIdentity(
      'MERGE INTO dataset.NewArrivals USING (SELECT * FROM UNNEST([(\'microwave\', 10, \'warehouse #1\'), (\'dryer\', 30, \'warehouse #1\'), (\'oven\', 20, \'warehouse #2\')])) ON FALSE WHEN NOT MATCHED THEN INSERT ROW WHEN NOT MATCHED BY SOURCE THEN DELETE',
    );
    this.validateIdentity(
      'SELECT * FROM test QUALIFY a IS DISTINCT FROM b WINDOW c AS (PARTITION BY d)',
    );
    this.validateIdentity(
      'FOR record IN (SELECT word, word_count FROM bigquery-public-data.samples.shakespeare LIMIT 5) DO SELECT record.word, record.word_count',
    );
    this.validateIdentity(
      'DATE(CAST(\'2016-12-25 05:30:00+07\' AS DATETIME), \'America/Los_Angeles\')',
    );
    this.validateIdentity(
      'CREATE TABLE x (a STRING OPTIONS (description=\'x\')) OPTIONS (table_expiration_days=1)',
    );
    this.validateIdentity(
      'SELECT * FROM (SELECT * FROM `t`) AS a UNPIVOT((c) FOR c_name IN (v1, v2))',
    );
    this.validateIdentity(
      'CREATE TABLE IF NOT EXISTS foo AS SELECT * FROM bla EXCEPT DISTINCT (SELECT * FROM bar) LIMIT 0',
    );
    this.validateIdentity(
      'SELECT ROW() OVER (y ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM x WINDOW y AS (PARTITION BY CATEGORY)',
    );
    this.validateIdentity(
      'SELECT item, purchases, LAST_VALUE(item) OVER (item_window ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS most_popular FROM Produce WINDOW item_window AS (ORDER BY purchases)',
    );
    this.validateIdentity(
      'SELECT LAST_VALUE(a IGNORE NULLS) OVER y FROM x WINDOW y AS (PARTITION BY CATEGORY)',
    );
    this.validateIdentity(
      'CREATE OR REPLACE VIEW test (tenant_id OPTIONS (description=\'Test description on table creation\')) AS SELECT 1 AS tenant_id, 1 AS customer_id',
    );
    this.validateIdentity(
      'SELECT * FROM foo AS t0 FOR SYSTEM_TIME AS OF \'2026-02-12T23:22:21.743416+00:00\'',
    );
    this.validateIdentity(
      'SELECT b"\\x0a$\'x\'00"',
      'SELECT b\'\\x0a$\\\'x\\\'00\'',
    );
    this.validateIdentity(
      '--c\nARRAY_AGG(v IGNORE NULLS)',
      'ARRAY_AGG(v IGNORE NULLS) /* c */',
    );
    this.validateIdentity(
      'SELECT * FROM t1, t2',
      'SELECT * FROM t1 CROSS JOIN t2',
    );
    this.validateIdentity(
      'SELECT r"\\t"',
      'SELECT \'\\\\t\'',
    );
    this.validateIdentity(
      'ARRAY(SELECT AS STRUCT e.x AS y, e.z AS bla FROM UNNEST(bob))::ARRAY<STRUCT<y STRING, bro NUMERIC>>',
      'CAST(ARRAY(SELECT AS STRUCT e.x AS y, e.z AS bla FROM UNNEST(bob)) AS ARRAY<STRUCT<y STRING, bro NUMERIC>>)',
    );
    this.validateIdentity(
      'SELECT * FROM `proj.dataset.INFORMATION_SCHEMA.SOME_VIEW`',
      'SELECT * FROM `proj.dataset.INFORMATION_SCHEMA.SOME_VIEW` AS `proj.dataset.INFORMATION_SCHEMA.SOME_VIEW`',
    );
    this.validateIdentity(
      'SELECT * FROM region_or_dataset.INFORMATION_SCHEMA.TABLES',
      'SELECT * FROM region_or_dataset.`INFORMATION_SCHEMA.TABLES` AS TABLES',
    );
    this.validateIdentity(
      'SELECT * FROM region_or_dataset.INFORMATION_SCHEMA.TABLES AS some_name',
      'SELECT * FROM region_or_dataset.`INFORMATION_SCHEMA.TABLES` AS some_name',
    );
    this.validateIdentity(
      'SELECT * FROM proj.region_or_dataset.INFORMATION_SCHEMA.TABLES',
      'SELECT * FROM proj.region_or_dataset.`INFORMATION_SCHEMA.TABLES` AS TABLES',
    );
    this.validateIdentity(
      'CREATE VIEW `d.v` OPTIONS (expiration_timestamp=TIMESTAMP \'2020-01-02T04:05:06.007Z\') AS SELECT 1 AS c',
      'CREATE VIEW `d.v` OPTIONS (expiration_timestamp=CAST(\'2020-01-02T04:05:06.007Z\' AS TIMESTAMP)) AS SELECT 1 AS c',
    );
    this.validateIdentity(
      'SELECT ARRAY(SELECT AS STRUCT 1 a, 2 b)',
      'SELECT ARRAY(SELECT AS STRUCT 1 AS a, 2 AS b)',
    );
    this.validateIdentity(
      'select array_contains([1, 2, 3], 1)',
      'SELECT EXISTS(SELECT 1 FROM UNNEST([1, 2, 3]) AS _col WHERE _col = 1)',
    );
    this.validateIdentity(
      'SELECT SPLIT(foo)',
      'SELECT SPLIT(foo, \',\')',
    );
    this.validateIdentity(
      'SELECT 1 AS hash',
      'SELECT 1 AS `hash`',
    );
    this.validateIdentity(
      'SELECT 1 AS at',
      'SELECT 1 AS `at`',
    );
    this.validateIdentity(
      'x <> ""',
      'x <> \'\'',
    );
    this.validateIdentity(
      'x <> """"""',
      'x <> \'\'',
    );
    this.validateIdentity(
      'x <> \'\'\'\'\'\'',
      'x <> \'\'',
    );
    this.validateIdentity(
      'SELECT a overlaps',
      'SELECT a AS overlaps',
    );
    this.validateIdentity(
      'SELECT y + 1 z FROM x GROUP BY y + 1 ORDER BY z',
      'SELECT y + 1 AS z FROM x GROUP BY z ORDER BY z',
    );
    this.validateIdentity(
      'SELECT y + 1 z FROM x GROUP BY y + 1',
      'SELECT y + 1 AS z FROM x GROUP BY y + 1',
    );
    this.validateIdentity(
      'SELECT JSON \'"foo"\' AS json_data',
      'SELECT PARSE_JSON(\'"foo"\') AS json_data',
    );
    this.validateIdentity(
      'SELECT * FROM (SELECT a, b, c FROM test) PIVOT(SUM(b) d, COUNT(*) e FOR c IN (\'x\', \'y\'))',
      'SELECT * FROM (SELECT a, b, c FROM test) PIVOT(SUM(b) AS d, COUNT(*) AS e FOR c IN (\'x\', \'y\'))',
    );
    this.validateIdentity(
      'SELECT CAST(1 AS BYTEINT)',
      'SELECT CAST(1 AS INT64)',
    );
    this.validateIdentity(
      `CREATE TEMPORARY FUNCTION FOO()
RETURNS STRING
LANGUAGE js AS
'return "Hello world!"'`,
      undefined,
      { pretty: true },
    );
    this.validateIdentity(
      '[a, a(1, 2,3,4444444444444444, tttttaoeunthaoentuhaoentuheoantu, toheuntaoheutnahoeunteoahuntaoeh), b(3, 4,5), c, d, tttttttttttttttteeeeeeeeeeeeeett, 12312312312]',
      `[
  a,
  a(
    1,
    2,
    3,
    4444444444444444,
    tttttaoeunthaoentuhaoentuheoantu,
    toheuntaoheutnahoeunteoahuntaoeh
  ),
  b(3, 4, 5),
  c,
  d,
  tttttttttttttttteeeeeeeeeeeeeett,
  12312312312
]`,
      { pretty: true },
    );

    this.validateAll(
      'SELECT TRUE IS TRUE',
      {
        write: {
          'bigquery': 'SELECT TRUE IS TRUE',
          'snowflake': 'SELECT TRUE',
        },
      },
    );
    this.validateAll(
      'SELECT REPEAT(\' \', 2)',
      {
        read: {
          'hive': 'SELECT SPACE(2)',
          'spark': 'SELECT SPACE(2)',
          'databricks': 'SELECT SPACE(2)',
          'trino': 'SELECT REPEAT(\' \', 2)',
        },
      },
    );
    this.validateAll(
      'SELECT purchases, LAST_VALUE(item) OVER item_window AS most_popular FROM Produce WINDOW item_window AS (PARTITION BY purchases ORDER BY purchases ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)',
      {
        write: {
          'bigquery': 'SELECT purchases, LAST_VALUE(item) OVER item_window AS most_popular FROM Produce WINDOW item_window AS (PARTITION BY purchases ORDER BY purchases ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)',
          'clickhouse': 'SELECT purchases, LAST_VALUE(item) OVER item_window AS most_popular FROM Produce WINDOW item_window AS (PARTITION BY purchases ORDER BY purchases NULLS FIRST ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)',
          'databricks': 'SELECT purchases, LAST_VALUE(item) OVER item_window AS most_popular FROM Produce WINDOW item_window AS (PARTITION BY purchases ORDER BY purchases ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)',
          'duckdb': 'SELECT purchases, LAST_VALUE(item) OVER item_window AS most_popular FROM Produce WINDOW item_window AS (PARTITION BY purchases ORDER BY purchases NULLS FIRST ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)',
          'mysql': 'SELECT purchases, LAST_VALUE(item) OVER item_window AS most_popular FROM Produce WINDOW item_window AS (PARTITION BY purchases ORDER BY purchases ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)',
          'oracle': 'SELECT purchases, LAST_VALUE(item) OVER item_window AS most_popular FROM Produce WINDOW item_window AS (PARTITION BY purchases ORDER BY purchases NULLS FIRST ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)',
          'postgres': 'SELECT purchases, LAST_VALUE(item) OVER item_window AS most_popular FROM Produce WINDOW item_window AS (PARTITION BY purchases ORDER BY purchases NULLS FIRST ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)',
          'presto': 'SELECT purchases, LAST_VALUE(item) OVER (PARTITION BY purchases ORDER BY purchases NULLS FIRST ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS most_popular FROM Produce',
          'redshift': 'SELECT purchases, LAST_VALUE(item) OVER (PARTITION BY purchases ORDER BY purchases NULLS FIRST ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS most_popular FROM Produce',
          'snowflake': 'SELECT purchases, LAST_VALUE(item) OVER (PARTITION BY purchases ORDER BY purchases NULLS FIRST ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS most_popular FROM Produce',
          'spark': 'SELECT purchases, LAST_VALUE(item) OVER item_window AS most_popular FROM Produce WINDOW item_window AS (PARTITION BY purchases ORDER BY purchases ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)',
          'trino': 'SELECT purchases, LAST_VALUE(item) OVER item_window AS most_popular FROM Produce WINDOW item_window AS (PARTITION BY purchases ORDER BY purchases NULLS FIRST ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)',
          'tsql': 'SELECT purchases, LAST_VALUE(item) OVER item_window AS most_popular FROM Produce WINDOW item_window AS (PARTITION BY purchases ORDER BY purchases ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)',
        },
      },
    );
    this.validateAll(
      'SELECT DATE(2024, 1, 15)',
      {
        write: {
          'bigquery': 'SELECT DATE(2024, 1, 15)',
          'duckdb': 'SELECT MAKE_DATE(2024, 1, 15)',
        },
      },
    );
    this.validateAll(
      'EXTRACT(HOUR FROM DATETIME(2008, 12, 25, 15, 30, 00))',
      {
        write: {
          'bigquery': 'EXTRACT(HOUR FROM DATETIME(2008, 12, 25, 15, 30, 00))',
          'duckdb': 'EXTRACT(HOUR FROM MAKE_TIMESTAMP(2008, 12, 25, 15, 30, 00))',
          'snowflake': 'DATE_PART(HOUR, TIMESTAMP_FROM_PARTS(2008, 12, 25, 15, 30, 00))',
        },
      },
    );
    this.validateAll(
      'SELECT STRUCT(1, 2, 3), STRUCT(), STRUCT(\'abc\'), STRUCT(1, t.str_col), STRUCT(1 AS a, \'abc\' AS b), STRUCT(str_col AS abc)',
      {
        write: {
          'bigquery': 'SELECT STRUCT(1, 2, 3), STRUCT(), STRUCT(\'abc\'), STRUCT(1, t.str_col), STRUCT(1 AS a, \'abc\' AS b), STRUCT(str_col AS abc)',
          'duckdb': 'SELECT {\'_0\': 1, \'_1\': 2, \'_2\': 3}, {}, {\'_0\': \'abc\'}, {\'_0\': 1, \'str_col\': t.str_col}, {\'a\': 1, \'b\': \'abc\'}, {\'abc\': str_col}',
          'hive': 'SELECT STRUCT(1, 2, 3), STRUCT(), STRUCT(\'abc\'), STRUCT(1, t.str_col), STRUCT(1, \'abc\'), STRUCT(str_col)',
          'spark2': 'SELECT STRUCT(1, 2, 3), STRUCT(), STRUCT(\'abc\'), STRUCT(1, t.str_col), STRUCT(1 AS a, \'abc\' AS b), STRUCT(str_col AS abc)',
          'spark': 'SELECT STRUCT(1, 2, 3), STRUCT(), STRUCT(\'abc\'), STRUCT(1, t.str_col), STRUCT(1 AS a, \'abc\' AS b), STRUCT(str_col AS abc)',
          'snowflake': 'SELECT OBJECT_CONSTRUCT(\'_0\', 1, \'_1\', 2, \'_2\', 3), OBJECT_CONSTRUCT(), OBJECT_CONSTRUCT(\'_0\', \'abc\'), OBJECT_CONSTRUCT(\'_0\', 1, \'_1\', t.str_col), OBJECT_CONSTRUCT(\'a\', 1, \'b\', \'abc\'), OBJECT_CONSTRUCT(\'abc\', str_col)',
          'trino': 'SELECT ROW(1, 2, 3), ROW(), ROW(\'abc\'), ROW(1, t.str_col), CAST(ROW(1, \'abc\') AS ROW(a INTEGER, b VARCHAR)), ROW(str_col)',
        },
      },
    );
    this.validateAll(
      'PARSE_TIMESTAMP(\'%Y-%m-%dT%H:%M:%E6S%z\', x)',
      {
        write: {
          'bigquery': 'PARSE_TIMESTAMP(\'%FT%H:%M:%E6S%z\', x)',
          'duckdb': 'STRPTIME(x, \'%Y-%m-%dT%H:%M:%S.%f%z\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)',
      {
        write: {
          'bigquery': 'SELECT DATE_SUB(CURRENT_DATE, INTERVAL \'2\' DAY)',
          'databricks': 'SELECT DATE_ADD(CURRENT_DATE, -2)',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_SUB(DATE \'2008-12-25\', INTERVAL 5 DAY)',
      {
        write: {
          'bigquery': 'SELECT DATE_SUB(CAST(\'2008-12-25\' AS DATE), INTERVAL \'5\' DAY)',
          'duckdb': 'SELECT CAST(\'2008-12-25\' AS DATE) - INTERVAL \'5\' DAY',
          'snowflake': 'SELECT DATEADD(DAY, \'5\' * -1, CAST(\'2008-12-25\' AS DATE))',
        },
      },
    );
    this.validateAll(
      'EDIT_DISTANCE(col1, col2, max_distance => 3)',
      {
        write: {
          'bigquery': 'EDIT_DISTANCE(col1, col2, max_distance => 3)',
          'clickhouse': UnsupportedError,
          'databricks': UnsupportedError,
          'drill': UnsupportedError,
          'duckdb': 'CASE WHEN LEVENSHTEIN(col1, col2) IS NULL OR 3 IS NULL THEN NULL ELSE LEAST(LEVENSHTEIN(col1, col2), 3) END',
          'hive': UnsupportedError,
          'postgres': 'LEVENSHTEIN_LESS_EQUAL(col1, col2, 3)',
          'presto': UnsupportedError,
          'snowflake': 'EDITDISTANCE(col1, col2, 3)',
          'spark': UnsupportedError,
          'spark2': UnsupportedError,
          'sqlite': UnsupportedError,
        },
      },
    );
    this.validateAll(
      'EDIT_DISTANCE(a, b)',
      {
        write: {
          'bigquery': 'EDIT_DISTANCE(a, b)',
          'duckdb': 'LEVENSHTEIN(a, b)',
        },
      },
    );
    this.validateAll(
      'SAFE_CAST(some_date AS DATE FORMAT \'DD MONTH YYYY\')',
      {
        write: {
          'bigquery': 'SAFE_CAST(some_date AS DATE FORMAT \'DD MONTH YYYY\')',
          'duckdb': 'CAST(TRY_STRPTIME(some_date, \'%d %B %Y\') AS DATE)',
        },
      },
    );
    this.validateAll(
      'SAFE_CAST(some_date AS DATE FORMAT \'YYYY-MM-DD\') AS some_date',
      {
        write: {
          'bigquery': 'SAFE_CAST(some_date AS DATE FORMAT \'YYYY-MM-DD\') AS some_date',
          'duckdb': 'CAST(TRY_STRPTIME(some_date, \'%Y-%m-%d\') AS DATE) AS some_date',
        },
      },
    );
    this.validateAll(
      'SAFE_CAST(x AS TIMESTAMP)',
      {
        write: {
          'bigquery': 'SAFE_CAST(x AS TIMESTAMP)',
          'snowflake': 'CAST(x AS TIMESTAMPTZ)',
        },
      },
    );
    this.validateAll(
      'SELECT t.c1, h.c2, s.c3 FROM t1 AS t, UNNEST(t.t2) AS h, UNNEST(h.t3) AS s',
      {
        write: {
          'bigquery': 'SELECT t.c1, h.c2, s.c3 FROM t1 AS t CROSS JOIN UNNEST(t.t2) AS h CROSS JOIN UNNEST(h.t3) AS s',
          'duckdb': 'SELECT t.c1, h.c2, s.c3 FROM t1 AS t CROSS JOIN UNNEST(t.t2) AS _t0(h) CROSS JOIN UNNEST(h.t3) AS _t1(s)',
        },
      },
    );
    this.validateAll(
      'PARSE_TIMESTAMP(\'%Y-%m-%dT%H:%M:%E6S%z\', x)',
      {
        write: {
          'bigquery': 'PARSE_TIMESTAMP(\'%FT%H:%M:%E6S%z\', x)',
          'duckdb': 'STRPTIME(x, \'%Y-%m-%dT%H:%M:%S.%f%z\')',
        },
      },
    );
    this.validateAll(
      'SELECT results FROM Coordinates, Coordinates.position AS results',
      {
        write: {
          'bigquery': 'SELECT results FROM Coordinates CROSS JOIN UNNEST(Coordinates.position) AS results',
          'presto': 'SELECT results FROM Coordinates CROSS JOIN UNNEST(Coordinates.position) AS _t0(results)',
        },
      },
    );
    this.validateAll(
      'SELECT results FROM Coordinates, `Coordinates.position` AS results',
      {
        write: {
          'bigquery': 'SELECT results FROM Coordinates CROSS JOIN `Coordinates.position` AS results',
          'presto': 'SELECT results FROM Coordinates CROSS JOIN "Coordinates"."position" AS results',
        },
      },
    );
    this.validateAll(
      'SELECT results FROM Coordinates AS c, UNNEST(c.position) AS results',
      {
        read: {
          'presto': 'SELECT results FROM Coordinates AS c, UNNEST(c.position) AS _t(results)',
          'redshift': 'SELECT results FROM Coordinates AS c, c.position AS results',
        },
        write: {
          'bigquery': 'SELECT results FROM Coordinates AS c CROSS JOIN UNNEST(c.position) AS results',
          'presto': 'SELECT results FROM Coordinates AS c CROSS JOIN UNNEST(c.position) AS _t0(results)',
          'redshift': 'SELECT results FROM Coordinates AS c CROSS JOIN c.position AS results',
        },
      },
    );
    this.validateAll(
      'TIMESTAMP(x)',
      {
        write: {
          'bigquery': 'TIMESTAMP(x)',
          'duckdb': 'CAST(x AS TIMESTAMPTZ)',
          'snowflake': 'CAST(x AS TIMESTAMPTZ)',
          'presto': 'CAST(x AS TIMESTAMP WITH TIME ZONE)',
        },
      },
    );
    this.validateAll(
      'SELECT TIMESTAMP(\'2008-12-25 15:30:00\', \'America/Los_Angeles\')',
      {
        write: {
          'bigquery': 'SELECT TIMESTAMP(\'2008-12-25 15:30:00\', \'America/Los_Angeles\')',
          'duckdb': 'SELECT CAST(\'2008-12-25 15:30:00\' AS TIMESTAMP) AT TIME ZONE \'America/Los_Angeles\'',
          'snowflake': 'SELECT CONVERT_TIMEZONE(\'America/Los_Angeles\', CAST(\'2008-12-25 15:30:00\' AS TIMESTAMP))',
        },
      },
    );
    this.validateAll(
      'SELECT SUM(x IGNORE NULLS) AS x',
      {
        read: {
          'bigquery': 'SELECT SUM(x IGNORE NULLS) AS x',
          'duckdb': 'SELECT SUM(x IGNORE NULLS) AS x',
          'spark': 'SELECT SUM(x) IGNORE NULLS AS x',
          'snowflake': 'SELECT SUM(x) IGNORE NULLS AS x',
        },
        write: {
          'bigquery': 'SELECT SUM(x IGNORE NULLS) AS x',
          'duckdb': 'SELECT SUM(x) AS x',
          'postgres': UnsupportedError,
          'spark': 'SELECT SUM(x) IGNORE NULLS AS x',
          'snowflake': 'SELECT SUM(x) IGNORE NULLS AS x',
        },
      },
    );
    this.validateAll(
      'SELECT SUM(x RESPECT NULLS) AS x',
      {
        read: {
          'bigquery': 'SELECT SUM(x RESPECT NULLS) AS x',
          'spark': 'SELECT SUM(x) RESPECT NULLS AS x',
          'snowflake': 'SELECT SUM(x) RESPECT NULLS AS x',
        },
        write: {
          'bigquery': 'SELECT SUM(x RESPECT NULLS) AS x',
          'duckdb': 'SELECT SUM(x) AS x',
          'postgres': UnsupportedError,
          'spark': 'SELECT SUM(x) RESPECT NULLS AS x',
          'snowflake': 'SELECT SUM(x) RESPECT NULLS AS x',
        },
      },
    );
    this.validateAll(
      'SELECT PERCENTILE_CONT(x, 0.5 RESPECT NULLS) OVER ()',
      {
        write: {
          'bigquery': 'SELECT PERCENTILE_CONT(x, 0.5 RESPECT NULLS) OVER ()',
          'duckdb': 'SELECT QUANTILE_CONT(x, 0.5) OVER ()',
          'spark': 'SELECT PERCENTILE_CONT(x, 0.5) RESPECT NULLS OVER ()',
        },
      },
    );
    this.validateAll(
      'SELECT ARRAY_AGG(DISTINCT x IGNORE NULLS ORDER BY a, b DESC LIMIT 10) AS x',
      {
        write: {
          'bigquery': 'SELECT ARRAY_AGG(DISTINCT x IGNORE NULLS ORDER BY a, b DESC LIMIT 10) AS x',
          'duckdb': 'SELECT ARRAY_AGG(DISTINCT x ORDER BY a NULLS FIRST, b DESC LIMIT 10) AS x',
          'spark': 'SELECT COLLECT_LIST(DISTINCT x ORDER BY a, b DESC LIMIT 10) IGNORE NULLS AS x',
        },
      },
    );
    this.validateAll(
      'SELECT ARRAY_AGG(DISTINCT x IGNORE NULLS ORDER BY a, b DESC LIMIT 1, 10) AS x',
      {
        write: {
          'bigquery': 'SELECT ARRAY_AGG(DISTINCT x IGNORE NULLS ORDER BY a, b DESC LIMIT 1, 10) AS x',
          'duckdb': 'SELECT ARRAY_AGG(DISTINCT x ORDER BY a NULLS FIRST, b DESC LIMIT 1, 10) AS x',
          'spark': 'SELECT COLLECT_LIST(DISTINCT x ORDER BY a, b DESC LIMIT 1, 10) IGNORE NULLS AS x',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM Produce UNPIVOT((first_half_sales, second_half_sales) FOR semesters IN ((Q1, Q2) AS \'semester_1\', (Q3, Q4) AS \'semester_2\'))',
      {
        read: {
          'spark': 'SELECT * FROM Produce UNPIVOT((first_half_sales, second_half_sales) FOR semesters IN ((Q1, Q2) AS semester_1, (Q3, Q4) AS semester_2))',
        },
        write: {
          'bigquery': 'SELECT * FROM Produce UNPIVOT((first_half_sales, second_half_sales) FOR semesters IN ((Q1, Q2) AS \'semester_1\', (Q3, Q4) AS \'semester_2\'))',
          'spark': 'SELECT * FROM Produce UNPIVOT((first_half_sales, second_half_sales) FOR semesters IN ((Q1, Q2) AS semester_1, (Q3, Q4) AS semester_2))',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM Produce UNPIVOT((first_half_sales, second_half_sales) FOR semesters IN ((Q1, Q2) AS 1, (Q3, Q4) AS 2))',
      {
        write: {
          'bigquery': 'SELECT * FROM Produce UNPIVOT((first_half_sales, second_half_sales) FOR semesters IN ((Q1, Q2) AS 1, (Q3, Q4) AS 2))',
          'spark': 'SELECT * FROM Produce UNPIVOT((first_half_sales, second_half_sales) FOR semesters IN ((Q1, Q2) AS `1`, (Q3, Q4) AS `2`))',
        },
      },
    );
    this.validateAll(
      'SELECT UNIX_DATE(DATE \'2008-12-25\')',
      {
        write: {
          'bigquery': 'SELECT UNIX_DATE(CAST(\'2008-12-25\' AS DATE))',
          'duckdb': 'SELECT DATE_DIFF(\'DAY\', CAST(\'1970-01-01\' AS DATE), CAST(\'2008-12-25\' AS DATE))',
        },
      },
    );
    this.validateAll(
      'SELECT LAST_DAY(CAST(\'2008-11-25\' AS DATE), MONTH)',
      {
        read: {
          'snowflake': 'SELECT LAST_DAY(CAST(\'2008-11-25\' AS DATE), MONS)',
        },
        write: {
          'bigquery': 'SELECT LAST_DAY(CAST(\'2008-11-25\' AS DATE), MONTH)',
          'duckdb': 'SELECT LAST_DAY(CAST(\'2008-11-25\' AS DATE))',
          'clickhouse': 'SELECT LAST_DAY(CAST(\'2008-11-25\' AS Nullable(DATE)))',
          'mysql': 'SELECT LAST_DAY(CAST(\'2008-11-25\' AS DATE))',
          'oracle': 'SELECT LAST_DAY(CAST(\'2008-11-25\' AS DATE))',
          'postgres': 'SELECT CAST(DATE_TRUNC(\'MONTH\', CAST(\'2008-11-25\' AS DATE)) + INTERVAL \'1 MONTH\' - INTERVAL \'1 DAY\' AS DATE)',
          'presto': 'SELECT LAST_DAY_OF_MONTH(CAST(\'2008-11-25\' AS DATE))',
          'redshift': 'SELECT LAST_DAY(CAST(\'2008-11-25\' AS DATE))',
          'snowflake': 'SELECT LAST_DAY(CAST(\'2008-11-25\' AS DATE), MONTH)',
          'spark': 'SELECT LAST_DAY(CAST(\'2008-11-25\' AS DATE))',
          'tsql': 'SELECT EOMONTH(CAST(\'2008-11-25\' AS DATE))',
        },
      },
    );
    this.validateAll(
      'SELECT LAST_DAY(CAST(\'2008-11-25\' AS DATE), QUARTER)',
      {
        read: {
          'snowflake': 'SELECT LAST_DAY(CAST(\'2008-11-25\' AS DATE), QUARTER)',
        },
        write: {
          'bigquery': 'SELECT LAST_DAY(CAST(\'2008-11-25\' AS DATE), QUARTER)',
          'snowflake': 'SELECT LAST_DAY(CAST(\'2008-11-25\' AS DATE), QUARTER)',
        },
      },
    );
    this.validateAll(
      'CAST(x AS DATETIME)',
      {
        read: {
          '': 'x::timestamp',
        },
      },
    );
    this.validateAll(
      'SELECT TIME(15, 30, 00)',
      {
        read: {
          'duckdb': 'SELECT MAKE_TIME(15, 30, 00)',
          'mysql': 'SELECT MAKETIME(15, 30, 00)',
          'postgres': 'SELECT MAKE_TIME(15, 30, 00)',
          'snowflake': 'SELECT TIME_FROM_PARTS(15, 30, 00)',
        },
        write: {
          'bigquery': 'SELECT TIME(15, 30, 00)',
          'duckdb': 'SELECT MAKE_TIME(15, 30, 00)',
          'mysql': 'SELECT MAKETIME(15, 30, 00)',
          'postgres': 'SELECT MAKE_TIME(15, 30, 00)',
          'snowflake': 'SELECT TIME_FROM_PARTS(15, 30, 00)',
          'tsql': 'SELECT TIMEFROMPARTS(15, 30, 00, 0, 0)',
        },
      },
    );
    this.validateAll(
      'SELECT TIME(\'2008-12-25 15:30:00\')',
      {
        write: {
          'bigquery': 'SELECT TIME(\'2008-12-25 15:30:00\')',
          'duckdb': 'SELECT CAST(\'2008-12-25 15:30:00\' AS TIME)',
          'mysql': 'SELECT CAST(\'2008-12-25 15:30:00\' AS TIME)',
          'postgres': 'SELECT CAST(\'2008-12-25 15:30:00\' AS TIME)',
          'redshift': 'SELECT CAST(\'2008-12-25 15:30:00\' AS TIME)',
          'spark': 'SELECT CAST(\'2008-12-25 15:30:00\' AS TIMESTAMP)',
          'tsql': 'SELECT CAST(\'2008-12-25 15:30:00\' AS TIME)',
        },
      },
    );
    this.validateAll(
      'SELECT COUNTIF(x)',
      {
        read: {
          'clickhouse': 'SELECT countIf(x)',
          'duckdb': 'SELECT COUNT_IF(x)',
        },
        write: {
          'bigquery': 'SELECT COUNTIF(x)',
          'clickhouse': 'SELECT countIf(x)',
          'duckdb': 'SELECT COUNT_IF(x)',
        },
      },
    );
    this.validateAll(
      'SELECT TIMESTAMP_DIFF(TIMESTAMP_SECONDS(60), TIMESTAMP_SECONDS(0), minute)',
      {
        write: {
          'bigquery': 'SELECT TIMESTAMP_DIFF(TIMESTAMP_SECONDS(60), TIMESTAMP_SECONDS(0), MINUTE)',
          'databricks': 'SELECT TIMESTAMPDIFF(MINUTE, CAST(FROM_UNIXTIME(0) AS TIMESTAMP), CAST(FROM_UNIXTIME(60) AS TIMESTAMP))',
          'duckdb': 'SELECT DATE_DIFF(\'MINUTE\', TO_TIMESTAMP(0), TO_TIMESTAMP(60))',
          'snowflake': 'SELECT TIMESTAMPDIFF(MINUTE, TO_TIMESTAMP(0), TO_TIMESTAMP(60))',
        },
      },
    );
    this.validateAll(
      'TIMESTAMP_DIFF(a, b, MONTH)',
      {
        read: {
          'bigquery': 'TIMESTAMP_DIFF(a, b, month)',
          'databricks': 'TIMESTAMPDIFF(month, b, a)',
          'mysql': 'TIMESTAMPDIFF(month, b, a)',
        },
        write: {
          'databricks': 'TIMESTAMPDIFF(MONTH, b, a)',
          'mysql': 'TIMESTAMPDIFF(MONTH, b, a)',
          'snowflake': 'TIMESTAMPDIFF(MONTH, b, a)',
        },
      },
    );
    this.validateAll(
      'SELECT TIMESTAMP_MICROS(x)',
      {
        read: {
          'duckdb': 'SELECT MAKE_TIMESTAMP(x)',
          'spark': 'SELECT TIMESTAMP_MICROS(x)',
        },
        write: {
          'bigquery': 'SELECT TIMESTAMP_MICROS(x)',
          'duckdb': 'SELECT MAKE_TIMESTAMP(x)',
          'snowflake': 'SELECT TO_TIMESTAMP(x, 6)',
          'spark': 'SELECT TIMESTAMP_MICROS(x)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM t WHERE EXISTS(SELECT * FROM unnest(nums) AS x WHERE x > 1)',
      {
        write: {
          'bigquery': 'SELECT * FROM t WHERE EXISTS(SELECT * FROM UNNEST(nums) AS x WHERE x > 1)',
          'duckdb': 'SELECT * FROM t WHERE EXISTS(SELECT * FROM UNNEST(nums) AS _t0(x) WHERE x > 1)',
        },
      },
    );
    this.validateAll(
      'NULL',
      {
        read: {
          'duckdb': 'NULL = a',
          'postgres': 'a = NULL',
        },
      },
    );
    this.validateAll(
      'SELECT \'\\n\'',
      {
        read: {
          'bigquery': 'SELECT \'\'\'\n\'\'\'',
        },
        write: {
          'bigquery': 'SELECT \'\\n\'',
          'postgres': 'SELECT \'\n\'',
        },
      },
    );
    this.validateAll(
      'TRIM(item, \'*\')',
      {
        read: {
          'snowflake': 'TRIM(item, \'*\')',
          'spark': 'TRIM(\'*\', item)',
        },
        write: {
          'bigquery': 'TRIM(item, \'*\')',
          'snowflake': 'TRIM(item, \'*\')',
          'spark': 'TRIM(\'*\' FROM item)',
        },
      },
    );

    let expr = this.parseOne(
      'SELECT TRIM(CAST(\'***apple***\' AS BYTES), CAST(\'*\' AS BYTES)) AS result',
    );
    let annotated = annotateTypes(expr, { dialect: 'bigquery' });
    expect(
      annotated.sql({ dialect: 'duckdb' }),
    ).toBe('SELECT CAST(TRIM(CAST(CAST(\'***apple***\' AS BLOB) AS TEXT), CAST(CAST(\'*\' AS BLOB) AS TEXT)) AS BLOB) AS result');

    expr = this.parseOne('SELECT TRIM(\'***apple***\', \'*\') AS result');
    annotated = annotateTypes(expr, { dialect: 'bigquery' });
    expect(annotated.sql({ dialect: 'duckdb' })).toBe('SELECT TRIM(\'***apple***\', \'*\') AS result');

    this.validateAll(
      'CREATE OR REPLACE TABLE `a.b.c` COPY `a.b.d`',
      {
        write: {
          'bigquery': 'CREATE OR REPLACE TABLE `a.b.c` COPY `a.b.d`',
          'snowflake': 'CREATE OR REPLACE TABLE "a"."b"."c" CLONE "a"."b"."d"',
        },
      },
    );
    this.validateAll(
      'SELECT DATETIME_DIFF(\'2023-01-01T00:00:00\', \'2023-01-01T05:00:00\', MILLISECOND)',
      {
        write: {
          'bigquery': 'SELECT DATETIME_DIFF(\'2023-01-01T00:00:00\', \'2023-01-01T05:00:00\', MILLISECOND)',
          'databricks': 'SELECT TIMESTAMPDIFF(MILLISECOND, \'2023-01-01T05:00:00\', \'2023-01-01T00:00:00\')',
          'snowflake': 'SELECT TIMESTAMPDIFF(MILLISECOND, \'2023-01-01T05:00:00\', \'2023-01-01T00:00:00\')',
          'duckdb': 'SELECT DATE_DIFF(\'MILLISECOND\', CAST(\'2023-01-01T05:00:00\' AS TIMESTAMP), CAST(\'2023-01-01T00:00:00\' AS TIMESTAMP))',
        },
      },
    );
    this.validateAll(
      'SELECT DATETIME_ADD(\'2023-01-01T00:00:00\', INTERVAL 1 MILLISECOND)',
      {
        write: {
          'bigquery': 'SELECT DATETIME_ADD(\'2023-01-01T00:00:00\', INTERVAL \'1\' MILLISECOND)',
          'databricks': 'SELECT TIMESTAMPADD(MILLISECOND, \'1\', \'2023-01-01T00:00:00\')',
          'duckdb': 'SELECT CAST(\'2023-01-01T00:00:00\' AS TIMESTAMP) + INTERVAL \'1\' MILLISECOND',
          'snowflake': 'SELECT TIMESTAMPADD(MILLISECOND, \'1\', \'2023-01-01T00:00:00\')',
          'spark': 'SELECT \'2023-01-01T00:00:00\' + INTERVAL \'1\' MILLISECOND',
        },
      },
    );
    this.validateAll(
      'SELECT DATETIME_SUB(\'2023-01-01T00:00:00\', INTERVAL 1 MILLISECOND)',
      {
        write: {
          'bigquery': 'SELECT DATETIME_SUB(\'2023-01-01T00:00:00\', INTERVAL \'1\' MILLISECOND)',
          'databricks': 'SELECT TIMESTAMPADD(MILLISECOND, \'1\' * -1, \'2023-01-01T00:00:00\')',
          'duckdb': 'SELECT CAST(\'2023-01-01T00:00:00\' AS TIMESTAMP) - INTERVAL \'1\' MILLISECOND',
          'spark': 'SELECT \'2023-01-01T00:00:00\' - INTERVAL \'1\' MILLISECOND',
        },
      },
    );
    this.validateAll(
      'SELECT DATETIME_TRUNC(\'2023-01-01T01:01:01\', HOUR)',
      {
        write: {
          'bigquery': 'SELECT DATETIME_TRUNC(\'2023-01-01T01:01:01\', HOUR)',
          'databricks': 'SELECT DATE_TRUNC(\'HOUR\', \'2023-01-01T01:01:01\')',
          'duckdb': 'SELECT DATE_TRUNC(\'HOUR\', CAST(\'2023-01-01T01:01:01\' AS TIMESTAMP))',
        },
      },
    );
    this.validateAll('LEAST(x, y)', { read: { 'sqlite': 'MIN(x, y)' } });
    this.validateAll(
      'SELECT TIMESTAMP_ADD(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE)',
      {
        write: {
          'bigquery': 'SELECT TIMESTAMP_ADD(CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMP), INTERVAL \'10\' MINUTE)',
          'databricks': 'SELECT DATE_ADD(MINUTE, \'10\', CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMP))',
          'duckdb': 'SELECT CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMPTZ) + INTERVAL \'10\' MINUTE',
          'mysql': 'SELECT DATE_ADD(TIMESTAMP(\'2008-12-25 15:30:00+00\'), INTERVAL \'10\' MINUTE)',
          'spark': 'SELECT DATE_ADD(MINUTE, \'10\', CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMP))',
          'snowflake': 'SELECT TIMESTAMPADD(MINUTE, \'10\', CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMPTZ))',
        },
      },
    );
    this.validateAll(
      'SELECT TIMESTAMP_SUB(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE)',
      {
        write: {
          'bigquery': 'SELECT TIMESTAMP_SUB(CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMP), INTERVAL \'10\' MINUTE)',
          'duckdb': 'SELECT CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMPTZ) - INTERVAL \'10\' MINUTE',
          'mysql': 'SELECT DATE_SUB(TIMESTAMP(\'2008-12-25 15:30:00+00\'), INTERVAL \'10\' MINUTE)',
          'snowflake': 'SELECT TIMESTAMPADD(MINUTE, \'10\' * -1, CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMPTZ))',
          'spark': 'SELECT CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMP) - INTERVAL \'10\' MINUTE',
        },
      },
    );
    this.validateAll(
      'SELECT TIMESTAMP_SUB(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL col MINUTE)',
      {
        write: {
          'bigquery': 'SELECT TIMESTAMP_SUB(CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMP), INTERVAL col MINUTE)',
          'snowflake': 'SELECT TIMESTAMPADD(MINUTE, col * -1, CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMPTZ))',
        },
      },
    );
    this.validateAll(
      'SELECT TIME_ADD(CAST(\'09:05:03\' AS TIME), INTERVAL 2 HOUR)',
      {
        write: {
          'bigquery': 'SELECT TIME_ADD(CAST(\'09:05:03\' AS TIME), INTERVAL \'2\' HOUR)',
          'duckdb': 'SELECT CAST(\'09:05:03\' AS TIME) + INTERVAL \'2\' HOUR',
        },
      },
    );
    this.validateAll(
      'SELECT TIME_SUB(CAST(\'09:05:03\' AS TIME), INTERVAL 2 HOUR)',
      {
        write: {
          'bigquery': 'SELECT TIME_SUB(CAST(\'09:05:03\' AS TIME), INTERVAL \'2\' HOUR)',
          'duckdb': 'SELECT CAST(\'09:05:03\' AS TIME) - INTERVAL \'2\' HOUR',
        },
      },
    );

    expr = this.parseOne('LOWER(CAST(\'HELLO\' AS BYTES))');
    annotated = annotateTypes(expr, { dialect: 'bigquery' });
    expect(annotated.sql({ dialect: 'duckdb' })).toBe('CAST(LOWER(CAST(CAST(\'HELLO\' AS BLOB) AS TEXT)) AS BLOB)');

    expr = this.parseOne('LOWER(\'HELLO\')');
    annotated = annotateTypes(expr, { dialect: 'bigquery' });
    expect(annotated.sql({ dialect: 'duckdb' })).toBe('LOWER(\'HELLO\')');

    this.validateAll(
      'LOWER(TO_HEX(x))',
      {
        write: {
          '': 'LOWER(HEX(x))',
          'bigquery': 'TO_HEX(x)',
          'clickhouse': 'LOWER(HEX(x))',
          'duckdb': 'LOWER(HEX(x))',
          'hive': 'LOWER(HEX(x))',
          'mysql': 'LOWER(HEX(x))',
          'spark': 'LOWER(HEX(x))',
          'sqlite': 'LOWER(HEX(x))',
          'presto': 'LOWER(TO_HEX(x))',
          'trino': 'LOWER(TO_HEX(x))',
        },
      },
    );
    this.validateAll(
      'TO_HEX(x)',
      {
        read: {
          '': 'LOWER(HEX(x))',
          'clickhouse': 'LOWER(HEX(x))',
          'duckdb': 'LOWER(HEX(x))',
          'hive': 'LOWER(HEX(x))',
          'mysql': 'LOWER(HEX(x))',
          'spark': 'LOWER(HEX(x))',
          'sqlite': 'LOWER(HEX(x))',
          'presto': 'LOWER(TO_HEX(x))',
          'trino': 'LOWER(TO_HEX(x))',
        },
        write: {
          '': 'LOWER(HEX(x))',
          'bigquery': 'TO_HEX(x)',
          'clickhouse': 'LOWER(HEX(x))',
          'duckdb': 'LOWER(HEX(x))',
          'hive': 'LOWER(HEX(x))',
          'mysql': 'LOWER(HEX(x))',
          'presto': 'LOWER(TO_HEX(x))',
          'spark': 'LOWER(HEX(x))',
          'sqlite': 'LOWER(HEX(x))',
          'trino': 'LOWER(TO_HEX(x))',
        },
      },
    );

    expr = this.parseOne('UPPER(CAST(\'hello\' AS BYTES))');
    annotated = annotateTypes(expr, { dialect: 'bigquery' });
    expect(annotated.sql({ dialect: 'duckdb' })).toBe('CAST(UPPER(CAST(CAST(\'hello\' AS BLOB) AS TEXT)) AS BLOB)');

    expr = this.parseOne('UPPER(\'hello\')');
    annotated = annotateTypes(expr, { dialect: 'bigquery' });
    expect(annotated.sql({ dialect: 'duckdb' })).toBe('UPPER(\'hello\')');

    this.validateAll(
      'UPPER(TO_HEX(x))',
      {
        read: {
          '': 'HEX(x)',
          'clickhouse': 'HEX(x)',
          'duckdb': 'HEX(x)',
          'hive': 'HEX(x)',
          'mysql': 'HEX(x)',
          'presto': 'TO_HEX(x)',
          'spark': 'HEX(x)',
          'sqlite': 'HEX(x)',
          'trino': 'TO_HEX(x)',
        },
        write: {
          '': 'HEX(x)',
          'bigquery': 'UPPER(TO_HEX(x))',
          'clickhouse': 'HEX(x)',
          'duckdb': 'HEX(x)',
          'hive': 'HEX(x)',
          'mysql': 'HEX(x)',
          'presto': 'TO_HEX(x)',
          'spark': 'HEX(x)',
          'sqlite': 'HEX(x)',
          'trino': 'TO_HEX(x)',
        },
      },
    );
    this.validateAll(
      'MD5(x)',
      {
        read: {
          'clickhouse': 'MD5(x)',
          'presto': 'MD5(x)',
          'trino': 'MD5(x)',
        },
        write: {
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
      'SELECT TO_HEX(MD5(some_string))',
      {
        read: {
          'duckdb': 'SELECT MD5(some_string)',
          'spark': 'SELECT MD5(some_string)',
          'clickhouse': 'SELECT LOWER(HEX(MD5(some_string)))',
          'presto': 'SELECT LOWER(TO_HEX(MD5(some_string)))',
          'trino': 'SELECT LOWER(TO_HEX(MD5(some_string)))',
        },
        write: {
          '': 'SELECT MD5(some_string)',
          'bigquery': 'SELECT TO_HEX(MD5(some_string))',
          'duckdb': 'SELECT MD5(some_string)',
          'clickhouse': 'SELECT LOWER(HEX(MD5(some_string)))',
          'presto': 'SELECT LOWER(TO_HEX(MD5(some_string)))',
          'trino': 'SELECT LOWER(TO_HEX(MD5(some_string)))',
        },
      },
    );
    this.validateAll(
      'SHA1(x)',
      {
        read: {
          'bigquery': 'SHA1(x)',
          'clickhouse': 'SHA1(x)',
          'presto': 'SHA1(x)',
          'trino': 'SHA1(x)',
        },
        write: {
          'clickhouse': 'SHA1(x)',
          'bigquery': 'SHA1(x)',
          'presto': 'SHA1(x)',
          'trino': 'SHA1(x)',
          'duckdb': 'UNHEX(SHA1(x))',
        },
      },
    );
    this.validateAll(
      'SHA256(x)',
      {
        read: {
          'clickhouse': 'SHA256(x)',
          'presto': 'SHA256(x)',
          'trino': 'SHA256(x)',
          'postgres': 'SHA256(x)',
          'duckdb': 'SHA256(x)',
        },
        write: {
          'bigquery': 'SHA256(x)',
          'spark2': 'SHA2(x, 256)',
          'clickhouse': 'SHA256(x)',
          'postgres': 'SHA256(x)',
          'presto': 'SHA256(x)',
          'redshift': 'SHA2(x, 256)',
          'trino': 'SHA256(x)',
          'duckdb': 'UNHEX(SHA256(x))',
          'snowflake': 'SHA2_BINARY(x, 256)',
        },
      },
    );
    this.validateAll(
      'SHA512(x)',
      {
        read: {
          'clickhouse': 'SHA512(x)',
          'presto': 'SHA512(x)',
          'trino': 'SHA512(x)',
        },
        write: {
          'clickhouse': 'SHA512(x)',
          'bigquery': 'SHA512(x)',
          'spark2': 'SHA2(x, 512)',
          'presto': 'SHA512(x)',
          'trino': 'SHA512(x)',
        },
      },
    );
    this.validateAll(
      'SELECT CAST(\'20201225\' AS TIMESTAMP FORMAT \'YYYYMMDD\' AT TIME ZONE \'America/New_York\')',
      { write: { 'bigquery': 'SELECT PARSE_TIMESTAMP(\'%Y%m%d\', \'20201225\', \'America/New_York\')' } },
    );
    this.validateAll(
      'SELECT CAST(\'20201225\' AS TIMESTAMP FORMAT \'YYYYMMDD\')',
      { write: { 'bigquery': 'SELECT PARSE_TIMESTAMP(\'%Y%m%d\', \'20201225\')' } },
    );
    this.validateAll(
      'SELECT CAST(TIMESTAMP \'2008-12-25 00:00:00+00:00\' AS STRING FORMAT \'YYYY-MM-DD HH24:MI:SS TZH:TZM\') AS date_time_to_string',
      {
        write: {
          'bigquery': 'SELECT CAST(CAST(\'2008-12-25 00:00:00+00:00\' AS TIMESTAMP) AS STRING FORMAT \'YYYY-MM-DD HH24:MI:SS TZH:TZM\') AS date_time_to_string',
        },
      },
    );
    this.validateAll(
      'SELECT CAST(TIMESTAMP \'2008-12-25 00:00:00+00:00\' AS STRING FORMAT \'YYYY-MM-DD HH24:MI:SS TZH:TZM\' AT TIME ZONE \'Asia/Kolkata\') AS date_time_to_string',
      {
        write: {
          'bigquery': 'SELECT CAST(CAST(\'2008-12-25 00:00:00+00:00\' AS TIMESTAMP) AS STRING FORMAT \'YYYY-MM-DD HH24:MI:SS TZH:TZM\' AT TIME ZONE \'Asia/Kolkata\') AS date_time_to_string',
        },
      },
    );
    this.validateAll(
      'WITH cte AS (SELECT [1, 2, 3] AS arr) SELECT IF(pos = pos_2, col, NULL) AS col FROM cte CROSS JOIN UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH(arr)) - 1)) AS pos CROSS JOIN UNNEST(arr) AS col WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH(arr) - 1) AND pos_2 = (ARRAY_LENGTH(arr) - 1))',
      {
        read: {
          'spark': 'WITH cte AS (SELECT ARRAY(1, 2, 3) AS arr) SELECT EXPLODE(arr) FROM cte',
        },
      },
    );
    this.validateAll(
      'SELECT IF(pos = pos_2, col, NULL) AS col FROM UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH(IF(ARRAY_LENGTH(COALESCE([], [])) = 0, [[][SAFE_ORDINAL(0)]], []))) - 1)) AS pos CROSS JOIN UNNEST(IF(ARRAY_LENGTH(COALESCE([], [])) = 0, [[][SAFE_ORDINAL(0)]], [])) AS col WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH(IF(ARRAY_LENGTH(COALESCE([], [])) = 0, [[][SAFE_ORDINAL(0)]], [])) - 1) AND pos_2 = (ARRAY_LENGTH(IF(ARRAY_LENGTH(COALESCE([], [])) = 0, [[][SAFE_ORDINAL(0)]], [])) - 1))',
      { read: { 'spark': 'select explode_outer([])' } },
    );
    this.validateAll(
      'SELECT IF(pos = pos_2, col, NULL) AS col, IF(pos = pos_2, pos_2, NULL) AS pos_2 FROM UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH(IF(ARRAY_LENGTH(COALESCE([], [])) = 0, [[][SAFE_ORDINAL(0)]], []))) - 1)) AS pos CROSS JOIN UNNEST(IF(ARRAY_LENGTH(COALESCE([], [])) = 0, [[][SAFE_ORDINAL(0)]], [])) AS col WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH(IF(ARRAY_LENGTH(COALESCE([], [])) = 0, [[][SAFE_ORDINAL(0)]], [])) - 1) AND pos_2 = (ARRAY_LENGTH(IF(ARRAY_LENGTH(COALESCE([], [])) = 0, [[][SAFE_ORDINAL(0)]], [])) - 1))',
      { read: { 'spark': 'select posexplode_outer([])' } },
    );
    this.validateAll(
      'SELECT AS STRUCT ARRAY(SELECT AS STRUCT 1 AS b FROM x) AS y FROM z',
      {
        write: {
          '': 'SELECT AS STRUCT ARRAY(SELECT AS STRUCT 1 AS b FROM x) AS y FROM z',
          'bigquery': 'SELECT AS STRUCT ARRAY(SELECT AS STRUCT 1 AS b FROM x) AS y FROM z',
          'duckdb': 'SELECT {\'y\': ARRAY(SELECT {\'b\': 1} FROM x)} FROM z',
        },
      },
    );
    this.validateAll(
      'SELECT CAST(STRUCT(1) AS STRUCT<INT64>)',
      {
        write: {
          'bigquery': 'SELECT CAST(STRUCT(1) AS STRUCT<INT64>)',
          'snowflake': 'SELECT CAST(OBJECT_CONSTRUCT(\'_0\', 1) AS OBJECT)',
        },
      },
    );
    this.validateAll(
      'cast(x as date format \'MM/DD/YYYY\')',
      { write: { 'bigquery': 'PARSE_DATE(\'%m/%d/%Y\', x)' } },
    );
    this.validateAll(
      'cast(x as time format \'YYYY.MM.DD HH:MI:SSTZH\')',
      { write: { 'bigquery': 'PARSE_TIMESTAMP(\'%Y.%m.%d %I:%M:%S%z\', x)' } },
    );
    this.validateIdentity(
      'CREATE TEMP TABLE foo AS SELECT 1',
      'CREATE TEMPORARY TABLE foo AS SELECT 1',
    );
    this.validateAll(
      'REGEXP_CONTAINS(\'foo\', \'.*\')',
      {
        read: {
          'bigquery': 'REGEXP_CONTAINS(\'foo\', \'.*\')',
          'mysql': 'REGEXP_LIKE(\'foo\', \'.*\')',
          'starrocks': 'REGEXP(\'foo\', \'.*\')',
        },
        write: {
          'mysql': 'REGEXP_LIKE(\'foo\', \'.*\')',
          'starrocks': 'REGEXP(\'foo\', \'.*\')',
        },
      },
    );
    this.validateAll(
      '"""x"""',
      {
        write: {
          'bigquery': '\'x\'',
          'duckdb': '\'x\'',
          'presto': '\'x\'',
          'hive': '\'x\'',
          'spark': '\'x\'',
        },
      },
    );
    this.validateAll(
      '"""x\'"""',
      {
        write: {
          'bigquery': '\'x\\\'\'',
          'duckdb': '\'x\'\'\'',
          'presto': '\'x\'\'\'',
          'hive': '\'x\\\'\'',
          'spark': '\'x\\\'\'',
        },
      },
    );
    this.validateAll(
      'r\'x\\\'\'',
      {
        write: {
          'bigquery': '\'x\\\'\'',
          'hive': '\'x\\\'\'',
        },
      },
    );
    this.validateAll(
      'r\'x\\y\'',
      {
        write: {
          'bigquery': '\'x\\\\y\'',
          'hive': '\'x\\\\y\'',
        },
      },
    );
    this.validateAll(
      '\'\\\\\'',
      {
        write: {
          'bigquery': '\'\\\\\'',
          'duckdb': '\'\\\'',
          'presto': '\'\\\'',
          'hive': '\'\\\\\'',
        },
      },
    );
    this.validateAll(
      'r"""/\\*.*\\*/"""',
      {
        write: {
          'bigquery': '\'/\\\\*.*\\\\*/\'',
          'duckdb': '\'/\\*.*\\*/\'',
          'presto': '\'/\\*.*\\*/\'',
          'hive': '\'/\\\\*.*\\\\*/\'',
          'spark': '\'/\\\\*.*\\\\*/\'',
        },
      },
    );
    this.validateAll(
      'R"""/\\*.*\\*/"""',
      {
        write: {
          'bigquery': '\'/\\\\*.*\\\\*/\'',
          'duckdb': '\'/\\*.*\\*/\'',
          'presto': '\'/\\*.*\\*/\'',
          'hive': '\'/\\\\*.*\\\\*/\'',
          'spark': '\'/\\\\*.*\\\\*/\'',
        },
      },
    );
    this.validateAll(
      'r"""a\n"""',
      {
        write: {
          'bigquery': '\'a\\n\'',
          'duckdb': '\'a\n\'',
        },
      },
    );
    this.validateAll(
      '"""a\n"""',
      {
        write: {
          'bigquery': '\'a\\n\'',
          'duckdb': '\'a\n\'',
        },
      },
    );
    this.validateAll(
      'CAST(a AS INT64)',
      {
        write: {
          'bigquery': 'CAST(a AS INT64)',
          'duckdb': 'CAST(a AS BIGINT)',
          'presto': 'CAST(a AS BIGINT)',
          'hive': 'CAST(a AS BIGINT)',
          'spark': 'CAST(a AS BIGINT)',
        },
      },
    );
    this.validateAll(
      'CAST(a AS BYTES)',
      {
        write: {
          'bigquery': 'CAST(a AS BYTES)',
          'duckdb': 'CAST(a AS BLOB)',
          'presto': 'CAST(a AS VARBINARY)',
          'hive': 'CAST(a AS BINARY)',
          'spark': 'CAST(a AS BINARY)',
        },
      },
    );

    expr = this.parseOne('STARTS_WITH(CAST(\'foo\' AS BYTES), CAST(\'f\' AS BYTES))');
    annotated = annotateTypes(expr, { dialect: 'bigquery' });
    expect(annotated.sql({ dialect: 'duckdb' })).toBe(
      'STARTS_WITH(CAST(CAST(\'foo\' AS BLOB) AS TEXT), CAST(CAST(\'f\' AS BLOB) AS TEXT))',
    );

    expr = this.parseOne('STARTS_WITH(CAST(\'foo\' AS BYTES), b\'f\')');
    annotated = annotateTypes(expr, { dialect: 'bigquery' });
    expect(annotated.sql({ dialect: 'duckdb' })).toBe(
      'STARTS_WITH(CAST(CAST(\'foo\' AS BLOB) AS TEXT), CAST(CAST(e\'f\' AS BLOB) AS TEXT))',
    );

    this.validateAll(
      'CAST(a AS NUMERIC)',
      {
        write: {
          'bigquery': 'CAST(a AS NUMERIC)',
          'duckdb': 'CAST(a AS DECIMAL)',
          'presto': 'CAST(a AS DECIMAL)',
          'hive': 'CAST(a AS DECIMAL)',
          'spark': 'CAST(a AS DECIMAL)',
        },
      },
    );
    this.validateAll(
      '[1, 2, 3]',
      {
        read: {
          'duckdb': '[1, 2, 3]',
          'presto': 'ARRAY[1, 2, 3]',
          'hive': 'ARRAY(1, 2, 3)',
          'spark': 'ARRAY(1, 2, 3)',
        },
        write: {
          'bigquery': '[1, 2, 3]',
          'duckdb': '[1, 2, 3]',
          'presto': 'ARRAY[1, 2, 3]',
          'hive': 'ARRAY(1, 2, 3)',
          'spark': 'ARRAY(1, 2, 3)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM UNNEST([\'7\', \'14\']) AS x',
      {
        read: {
          'spark': 'SELECT * FROM UNNEST(ARRAY(\'7\', \'14\')) AS (x)',
        },
        write: {
          'bigquery': 'SELECT * FROM UNNEST([\'7\', \'14\']) AS x',
          'presto': 'SELECT * FROM UNNEST(ARRAY[\'7\', \'14\']) AS _t0(x)',
          'spark': 'SELECT * FROM EXPLODE(ARRAY(\'7\', \'14\')) AS _t0(x)',
        },
      },
    );
    this.validateAll(
      'SELECT ARRAY(SELECT x FROM UNNEST([0, 1]) AS x)',
      { write: { 'bigquery': 'SELECT ARRAY(SELECT x FROM UNNEST([0, 1]) AS x)' } },
    );
    this.validateAll(
      'SELECT ARRAY(SELECT DISTINCT x FROM UNNEST(some_numbers) AS x) AS unique_numbers',
      { write: { 'bigquery': 'SELECT ARRAY(SELECT DISTINCT x FROM UNNEST(some_numbers) AS x) AS unique_numbers' } },
    );
    this.validateAll(
      'SELECT ARRAY(SELECT * FROM foo JOIN bla ON x = y)',
      { write: { 'bigquery': 'SELECT ARRAY(SELECT * FROM foo JOIN bla ON x = y)' } },
    );
    this.validateAll(
      'CURRENT_TIMESTAMP()',
      {
        read: { 'tsql': 'GETDATE()' },
        write: { 'tsql': 'GETDATE()' },
      },
    );
    this.validateAll(
      'current_datetime',
      {
        write: {
          'bigquery': 'CURRENT_DATETIME()',
          'presto': 'CURRENT_DATETIME()',
          'hive': 'CURRENT_DATETIME()',
          'spark': 'CURRENT_DATETIME()',
        },
      },
    );
    this.validateAll(
      'current_time',
      {
        write: {
          'bigquery': 'CURRENT_TIME()',
          'duckdb': 'CURRENT_TIME',
          'presto': 'CURRENT_TIME',
          'trino': 'CURRENT_TIME',
          'hive': 'CURRENT_TIME()',
          'spark': 'CURRENT_TIME()',
        },
      },
    );
    this.validateAll(
      'CURRENT_TIMESTAMP',
      {
        write: {
          'bigquery': 'CURRENT_TIMESTAMP()',
          'duckdb': 'CURRENT_TIMESTAMP',
          'postgres': 'CURRENT_TIMESTAMP',
          'presto': 'CURRENT_TIMESTAMP',
          'hive': 'CURRENT_TIMESTAMP()',
          'spark': 'CURRENT_TIMESTAMP()',
        },
      },
    );
    this.validateAll(
      'CURRENT_TIMESTAMP()',
      {
        write: {
          'bigquery': 'CURRENT_TIMESTAMP()',
          'duckdb': 'CURRENT_TIMESTAMP',
          'postgres': 'CURRENT_TIMESTAMP',
          'presto': 'CURRENT_TIMESTAMP',
          'hive': 'CURRENT_TIMESTAMP()',
          'spark': 'CURRENT_TIMESTAMP()',
        },
      },
    );
    this.validateAll(
      'DIV(x, y)',
      {
        write: {
          'bigquery': 'DIV(x, y)',
          'duckdb': 'x // y',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE db.example_table (col_a struct<struct_col_a:int, struct_col_b:string>)',
      {
        write: {
          'bigquery': 'CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRING>)',
          'duckdb': 'CREATE TABLE db.example_table (col_a STRUCT(struct_col_a INT, struct_col_b TEXT))',
          'presto': 'CREATE TABLE db.example_table (col_a ROW(struct_col_a INTEGER, struct_col_b VARCHAR))',
          'hive': 'CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRING>)',
          'spark': 'CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: INT, struct_col_b: STRING>)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)',
      {
        write: {
          'bigquery': 'CREATE TABLE db.example_table (col_a STRUCT<struct_col_a INT64, struct_col_b STRUCT<nested_col_a STRING, nested_col_b STRING>>)',
          'duckdb': 'CREATE TABLE db.example_table (col_a STRUCT(struct_col_a BIGINT, struct_col_b STRUCT(nested_col_a TEXT, nested_col_b TEXT)))',
          'presto': 'CREATE TABLE db.example_table (col_a ROW(struct_col_a BIGINT, struct_col_b ROW(nested_col_a VARCHAR, nested_col_b VARCHAR)))',
          'hive': 'CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: BIGINT, struct_col_b: STRUCT<nested_col_a: STRING, nested_col_b: STRING>>)',
          'spark': 'CREATE TABLE db.example_table (col_a STRUCT<struct_col_a: BIGINT, struct_col_b: STRUCT<nested_col_a: STRING, nested_col_b: STRING>>)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE db.example_table (x int) PARTITION BY x cluster by x',
      { write: { 'bigquery': 'CREATE TABLE db.example_table (x INT64) PARTITION BY x CLUSTER BY x' } },
    );
    this.validateAll(
      'DELETE db.example_table WHERE x = 1',
      {
        write: {
          'bigquery': 'DELETE db.example_table WHERE x = 1',
          'presto': 'DELETE FROM db.example_table WHERE x = 1',
        },
      },
    );
    this.validateAll(
      'DELETE db.example_table tb WHERE tb.x = 1',
      {
        write: {
          'bigquery': 'DELETE db.example_table AS tb WHERE tb.x = 1',
          'presto': 'DELETE FROM db.example_table WHERE x = 1',
        },
      },
    );
    this.validateAll(
      'DELETE db.example_table AS tb WHERE tb.x = 1',
      {
        write: {
          'bigquery': 'DELETE db.example_table AS tb WHERE tb.x = 1',
          'presto': 'DELETE FROM db.example_table WHERE x = 1',
        },
      },
    );
    this.validateAll(
      'DELETE FROM db.example_table WHERE x = 1',
      {
        write: {
          'bigquery': 'DELETE FROM db.example_table WHERE x = 1',
          'presto': 'DELETE FROM db.example_table WHERE x = 1',
        },
      },
    );
    this.validateAll(
      'DELETE FROM db.example_table tb WHERE tb.x = 1',
      {
        write: {
          'bigquery': 'DELETE FROM db.example_table AS tb WHERE tb.x = 1',
          'presto': 'DELETE FROM db.example_table WHERE x = 1',
        },
      },
    );
    this.validateAll(
      'DELETE FROM db.example_table AS tb WHERE tb.x = 1',
      {
        write: {
          'bigquery': 'DELETE FROM db.example_table AS tb WHERE tb.x = 1',
          'presto': 'DELETE FROM db.example_table WHERE x = 1',
        },
      },
    );
    this.validateAll(
      'DELETE FROM db.example_table AS tb WHERE example_table.x = 1',
      {
        write: {
          'bigquery': 'DELETE FROM db.example_table AS tb WHERE example_table.x = 1',
          'presto': 'DELETE FROM db.example_table WHERE x = 1',
        },
      },
    );
    this.validateAll(
      'DELETE FROM db.example_table WHERE example_table.x = 1',
      {
        write: {
          'bigquery': 'DELETE FROM db.example_table WHERE example_table.x = 1',
          'presto': 'DELETE FROM db.example_table WHERE example_table.x = 1',
        },
      },
    );
    this.validateAll(
      'DELETE FROM db.t1 AS t1 WHERE NOT t1.c IN (SELECT db.t2.c FROM db.t2)',
      {
        write: {
          'bigquery': 'DELETE FROM db.t1 AS t1 WHERE NOT t1.c IN (SELECT db.t2.c FROM db.t2)',
          'presto': 'DELETE FROM db.t1 WHERE NOT c IN (SELECT c FROM db.t2)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM a WHERE b IN UNNEST([1, 2, 3])',
      {
        write: {
          'bigquery': 'SELECT * FROM a WHERE b IN UNNEST([1, 2, 3])',
          'presto': 'SELECT * FROM a WHERE b IN (SELECT UNNEST(ARRAY[1, 2, 3]))',
          'hive': 'SELECT * FROM a WHERE b IN (SELECT EXPLODE(ARRAY(1, 2, 3)))',
          'spark': 'SELECT * FROM a WHERE b IN (SELECT EXPLODE(ARRAY(1, 2, 3)))',
        },
      },
    );
    this.validateAll(
      'DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)',
      {
        write: {
          'postgres': 'CURRENT_DATE - INTERVAL \'1 DAY\'',
          'bigquery': 'DATE_SUB(CURRENT_DATE, INTERVAL \'1\' DAY)',
        },
      },
    );
    this.validateAll(
      'DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)',
      {
        write: {
          'bigquery': 'DATE_ADD(CURRENT_DATE, INTERVAL \'-1\' DAY)',
          'duckdb': 'CURRENT_DATE + INTERVAL \'-1\' DAY',
          'mysql': 'DATE_ADD(CURRENT_DATE, INTERVAL \'-1\' DAY)',
          'postgres': 'CURRENT_DATE + INTERVAL \'-1 DAY\'',
          'presto': 'DATE_ADD(\'DAY\', CAST(\'-1\' AS BIGINT), CURRENT_DATE)',
          'hive': 'DATE_ADD(CURRENT_DATE, -1)',
          'spark': 'DATE_ADD(CURRENT_DATE, -1)',
        },
      },
    );
    this.validateAll(
      'DATE_DIFF(DATE \'2010-07-07\', DATE \'2008-12-25\', DAY)',
      {
        write: {
          'bigquery': 'DATE_DIFF(CAST(\'2010-07-07\' AS DATE), CAST(\'2008-12-25\' AS DATE), DAY)',
          'mysql': 'DATEDIFF(CAST(\'2010-07-07\' AS DATE), CAST(\'2008-12-25\' AS DATE))',
          'starrocks': 'DATE_DIFF(\'DAY\', CAST(\'2010-07-07\' AS DATE), CAST(\'2008-12-25\' AS DATE))',
        },
      },
    );
    this.validateAll(
      'DATE_DIFF(CAST(\'2010-07-07\' AS DATE), CAST(\'2008-12-25\' AS DATE), DAY)',
      {
        read: {
          'mysql': 'DATEDIFF(CAST(\'2010-07-07\' AS DATE), CAST(\'2008-12-25\' AS DATE))',
          'starrocks': 'DATEDIFF(CAST(\'2010-07-07\' AS DATE), CAST(\'2008-12-25\' AS DATE))',
        },
      },
    );
    this.validateAll(
      'DATE_DIFF(DATE \'2010-07-07\', DATE \'2008-12-25\', MINUTE)',
      {
        write: {
          'bigquery': 'DATE_DIFF(CAST(\'2010-07-07\' AS DATE), CAST(\'2008-12-25\' AS DATE), MINUTE)',
          'starrocks': 'DATE_DIFF(\'MINUTE\', CAST(\'2010-07-07\' AS DATE), CAST(\'2008-12-25\' AS DATE))',
        },
      },
    );
    this.validateAll(
      'DATE_DIFF(\'2021-01-01\', \'2020-01-01\', DAY)',
      {
        write: {
          'bigquery': 'DATE_DIFF(\'2021-01-01\', \'2020-01-01\', DAY)',
          'duckdb': 'DATE_DIFF(\'DAY\', CAST(\'2020-01-01\' AS DATE), CAST(\'2021-01-01\' AS DATE))',
        },
      },
    );
    this.validateAll(
      'CURRENT_DATE(\'UTC\')',
      {
        write: {
          'bigquery': 'CURRENT_DATE(\'UTC\')',
          'duckdb': 'CAST(CURRENT_TIMESTAMP AT TIME ZONE \'UTC\' AS DATE)',
          'mysql': 'CURRENT_DATE AT TIME ZONE \'UTC\'',
          'postgres': 'CURRENT_DATE AT TIME ZONE \'UTC\'',
          'snowflake': 'CAST(CONVERT_TIMEZONE(\'UTC\', CURRENT_TIMESTAMP()) AS DATE)',
        },
      },
    );
    this.validateAll(
      'SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10',
      {
        write: {
          'bigquery': 'SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a LIMIT 10',
          'snowflake': 'SELECT a FROM test WHERE a = 1 GROUP BY a HAVING a = 2 QUALIFY z ORDER BY a NULLS FIRST LIMIT 10',
        },
      },
    );
    this.validateAll(
      'SELECT cola, colb FROM UNNEST([STRUCT(1 AS cola, \'test\' AS colb)]) AS tab',
      {
        read: {
          'bigquery': 'SELECT cola, colb FROM UNNEST([STRUCT(1 AS cola, \'test\' AS colb)]) as tab',
          'snowflake': 'SELECT cola, colb FROM (VALUES (1, \'test\')) AS tab(cola, colb)',
          'spark': 'SELECT cola, colb FROM VALUES (1, \'test\') AS tab(cola, colb)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM UNNEST([STRUCT(1 AS _c0)]) AS t1',
      {
        read: {
          'bigquery': 'SELECT * FROM UNNEST([STRUCT(1 AS _c0)]) AS t1',
          'postgres': 'SELECT * FROM (VALUES (1)) AS t1',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM UNNEST([STRUCT(1 AS id)]) AS t1 CROSS JOIN UNNEST([STRUCT(1 AS id)]) AS t2',
      {
        read: {
          'bigquery': 'SELECT * FROM UNNEST([STRUCT(1 AS id)]) AS t1 CROSS JOIN UNNEST([STRUCT(1 AS id)]) AS t2',
          'postgres': 'SELECT * FROM (VALUES (1)) AS t1(id) CROSS JOIN (VALUES (1)) AS t2(id)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM UNNEST([1]) WITH OFFSET',
      { write: { 'bigquery': 'SELECT * FROM UNNEST([1]) WITH OFFSET AS offset' } },
    );
    this.validateAll(
      'SELECT * FROM UNNEST([1]) WITH OFFSET y',
      { write: { 'bigquery': 'SELECT * FROM UNNEST([1]) WITH OFFSET AS y' } },
    );
    this.validateAll(
      'GENERATE_ARRAY(1, 4)',
      {
        read: { 'bigquery': 'GENERATE_ARRAY(1, 4)' },
        write: { 'duckdb': 'GENERATE_SERIES(1, 4)' },
      },
    );
    this.validateAll(
      'TO_JSON_STRING(x)',
      {
        read: { 'bigquery': 'TO_JSON_STRING(x)' },
        write: {
          'bigquery': 'TO_JSON_STRING(x)',
          'duckdb': 'CAST(TO_JSON(x) AS TEXT)',
          'presto': 'JSON_FORMAT(CAST(x AS JSON))',
          'spark': 'TO_JSON(x)',
        },
      },
    );
    this.validateAll(
      `SELECT
  \`u\`.\`user_email\` AS \`user_email\`,
  \`d\`.\`user_id\` AS \`user_id\`,
  \`account_id\` AS \`account_id\`
FROM \`analytics_staging\`.\`stg_mongodb__users\` AS \`u\`, UNNEST(\`u\`.\`cluster_details\`) AS \`d\`, UNNEST(\`d\`.\`account_ids\`) AS \`account_id\`
WHERE
  NOT \`account_id\` IS NULL`,
      {
        read: {
          '': `
                SELECT
                  "u"."user_email" AS "user_email",
                  "_q_0"."d"."user_id" AS "user_id",
                  "_q_1"."account_id" AS "account_id"
                FROM
                  "analytics_staging"."stg_mongodb__users" AS "u",
                  UNNEST("u"."cluster_details") AS "_q_0"("d"),
                  UNNEST("_q_0"."d"."account_ids") AS "_q_1"("account_id")
                WHERE
                  NOT "_q_1"."account_id" IS NULL
                `,
        },
        pretty: true,
      },
    );
    this.validateAll(
      'SELECT MOD(x, 10)',
      {
        read: { 'postgres': 'SELECT x % 10' },
        write: {
          'bigquery': 'SELECT MOD(x, 10)',
          'postgres': 'SELECT x % 10',
        },
      },
    );
    this.validateAll(
      'SELECT CAST(x AS DATETIME)',
      {
        write: {
          '': 'SELECT CAST(x AS TIMESTAMP)',
          'bigquery': 'SELECT CAST(x AS DATETIME)',
        },
      },
    );
    this.validateAll(
      'SELECT TIME(foo, \'America/Los_Angeles\')',
      {
        write: {
          'duckdb': 'SELECT CAST(CAST(foo AS TIMESTAMPTZ) AT TIME ZONE \'America/Los_Angeles\' AS TIME)',
          'bigquery': 'SELECT TIME(foo, \'America/Los_Angeles\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATETIME(\'2020-01-01\')',
      {
        write: {
          'duckdb': 'SELECT CAST(\'2020-01-01\' AS TIMESTAMP)',
          'bigquery': 'SELECT DATETIME(\'2020-01-01\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATETIME(\'2020-01-01\', TIME \'23:59:59\')',
      {
        write: {
          'duckdb': 'SELECT CAST(CAST(\'2020-01-01\' AS DATE) + CAST(\'23:59:59\' AS TIME) AS TIMESTAMP)',
          'bigquery': 'SELECT DATETIME(\'2020-01-01\', CAST(\'23:59:59\' AS TIME))',
        },
      },
    );
    this.validateAll(
      'SELECT DATETIME(\'2020-01-01\', \'America/Los_Angeles\')',
      {
        write: {
          'duckdb': 'SELECT CAST(CAST(\'2020-01-01\' AS TIMESTAMPTZ) AT TIME ZONE \'America/Los_Angeles\' AS TIMESTAMP)',
          'bigquery': 'SELECT DATETIME(\'2020-01-01\', \'America/Los_Angeles\')',
        },
      },
    );
    this.validateAll(
      'SELECT LENGTH(foo)',
      {
        read: {
          'bigquery': 'SELECT LENGTH(foo)',
          'snowflake': 'SELECT LENGTH(foo)',
        },
        write: {
          'duckdb': 'SELECT CASE TYPEOF(foo) WHEN \'BLOB\' THEN OCTET_LENGTH(CAST(foo AS BLOB)) ELSE LENGTH(CAST(foo AS TEXT)) END',
          'snowflake': 'SELECT LENGTH(foo)',
          '': 'SELECT LENGTH(foo)',
        },
      },
    );
    this.validateAll(
      'SELECT TIME_DIFF(\'12:00:00\', \'12:30:00\', MINUTE)',
      {
        write: {
          'duckdb': 'SELECT DATE_DIFF(\'MINUTE\', CAST(\'12:30:00\' AS TIME), CAST(\'12:00:00\' AS TIME))',
          'bigquery': 'SELECT TIME_DIFF(\'12:00:00\', \'12:30:00\', MINUTE)',
        },
      },
    );
    this.validateAll(
      'ARRAY_CONCAT([1, 2], [3, 4], [5, 6])',
      {
        write: {
          'bigquery': 'ARRAY_CONCAT([1, 2], [3, 4], [5, 6])',
          'duckdb': 'LIST_CONCAT([1, 2], [3, 4], [5, 6])',
          'postgres': 'ARRAY_CAT(ARRAY[1, 2], ARRAY_CAT(ARRAY[3, 4], ARRAY[5, 6]))',
          'redshift': 'ARRAY_CONCAT(ARRAY(1, 2), ARRAY_CONCAT(ARRAY(3, 4), ARRAY(5, 6)))',
          'snowflake': 'ARRAY_CAT([1, 2], ARRAY_CAT([3, 4], [5, 6]))',
          'hive': 'CONCAT(ARRAY(1, 2), ARRAY(3, 4), ARRAY(5, 6))',
          'spark2': 'CONCAT(ARRAY(1, 2), ARRAY(3, 4), ARRAY(5, 6))',
          'spark': 'CONCAT(ARRAY(1, 2), ARRAY(3, 4), ARRAY(5, 6))',
          'databricks': 'CONCAT(ARRAY(1, 2), ARRAY(3, 4), ARRAY(5, 6))',
          'presto': 'CONCAT(ARRAY[1, 2], ARRAY[3, 4], ARRAY[5, 6])',
          'trino': 'CONCAT(ARRAY[1, 2], ARRAY[3, 4], ARRAY[5, 6])',
        },
      },
    );
    this.validateAll(
      'SELECT GENERATE_TIMESTAMP_ARRAY(\'2016-10-05 00:00:00\', \'2016-10-07 00:00:00\', INTERVAL \'1\' DAY)',
      {
        write: {
          'duckdb': 'SELECT GENERATE_SERIES(CAST(\'2016-10-05 00:00:00\' AS TIMESTAMP), CAST(\'2016-10-07 00:00:00\' AS TIMESTAMP), INTERVAL \'1\' DAY)',
          'bigquery': 'SELECT GENERATE_TIMESTAMP_ARRAY(\'2016-10-05 00:00:00\', \'2016-10-07 00:00:00\', INTERVAL \'1\' DAY)',
        },
      },
    );
    this.validateAll(
      'SELECT PARSE_DATE(\'%A %b %e %Y\', \'Thursday Dec 25 2008\')',
      {
        write: {
          'bigquery': 'SELECT PARSE_DATE(\'%A %b %e %Y\', \'Thursday Dec 25 2008\')',
          'duckdb': 'SELECT CAST(STRPTIME(\'Thursday Dec 25 2008\', \'%A %b %-d %Y\') AS DATE)',
        },
      },
    );
    this.validateAll(
      'SELECT PARSE_DATE(\'%Y%m%d\', \'20081225\')',
      {
        write: {
          'bigquery': 'SELECT PARSE_DATE(\'%Y%m%d\', \'20081225\')',
          'duckdb': 'SELECT CAST(STRPTIME(\'20081225\', \'%Y%m%d\') AS DATE)',
          'snowflake': 'SELECT DATE(\'20081225\', \'yyyymmDD\')',
        },
      },
    );
    this.validateAll(
      'SELECT ARRAY_TO_STRING([\'cake\', \'pie\', NULL], \'--\') AS text',
      {
        write: {
          'bigquery': 'SELECT ARRAY_TO_STRING([\'cake\', \'pie\', NULL], \'--\') AS text',
          'duckdb': 'SELECT ARRAY_TO_STRING([\'cake\', \'pie\', NULL], \'--\') AS text',
        },
      },
    );
    this.validateAll(
      'SELECT ARRAY_TO_STRING([\'cake\', \'pie\', NULL], \'--\', \'MISSING\') AS text',
      {
        write: {
          'bigquery': 'SELECT ARRAY_TO_STRING([\'cake\', \'pie\', NULL], \'--\', \'MISSING\') AS text',
          'duckdb': 'SELECT ARRAY_TO_STRING(LIST_TRANSFORM([\'cake\', \'pie\', NULL], x -> COALESCE(x, \'MISSING\')), \'--\') AS text',
        },
      },
    );
    this.validateAll(
      'STRING(a)',
      {
        write: {
          'bigquery': 'STRING(a)',
          'snowflake': 'CAST(a AS VARCHAR)',
          'duckdb': 'CAST(a AS TEXT)',
        },
      },
    );
    this.validateAll(
      'STRING(\'2008-12-25 15:30:00\', \'America/New_York\')',
      {
        write: {
          'bigquery': 'STRING(\'2008-12-25 15:30:00\', \'America/New_York\')',
          'snowflake': 'CAST(CONVERT_TIMEZONE(\'UTC\', \'America/New_York\', \'2008-12-25 15:30:00\') AS VARCHAR)',
          'duckdb': 'CAST(CAST(\'2008-12-25 15:30:00\' AS TIMESTAMP) AT TIME ZONE \'UTC\' AT TIME ZONE \'America/New_York\' AS TEXT)',
        },
      },
    );

    this.validateIdentity('SELECT * FROM a-b c', 'SELECT * FROM a-b AS c');

    this.validateAll(
      'SAFE_DIVIDE(x, y)',
      {
        write: {
          'bigquery': 'SAFE_DIVIDE(x, y)',
          'duckdb': 'CASE WHEN y <> 0 THEN x / y ELSE NULL END',
          'presto': 'IF(y <> 0, CAST(x AS DOUBLE) / y, NULL)',
          'trino': 'IF(y <> 0, CAST(x AS DOUBLE) / y, NULL)',
          'hive': 'IF(y <> 0, x / y, NULL)',
          'spark2': 'IF(y <> 0, x / y, NULL)',
          'spark': 'IF(y <> 0, x / y, NULL)',
          'databricks': 'IF(y <> 0, x / y, NULL)',
          'snowflake': 'IFF(y <> 0, x / y, NULL)',
          'postgres': 'CASE WHEN y <> 0 THEN CAST(x AS DOUBLE PRECISION) / y ELSE NULL END',
        },
      },
    );
    this.validateAll(
      'SAFE_DIVIDE(x + 1, 2 * y)',
      {
        write: {
          'bigquery': 'SAFE_DIVIDE(x + 1, 2 * y)',
          'duckdb': 'CASE WHEN (2 * y) <> 0 THEN (x + 1) / (2 * y) ELSE NULL END',
          'presto': 'IF((2 * y) <> 0, CAST((x + 1) AS DOUBLE) / (2 * y), NULL)',
          'trino': 'IF((2 * y) <> 0, CAST((x + 1) AS DOUBLE) / (2 * y), NULL)',
          'hive': 'IF((2 * y) <> 0, (x + 1) / (2 * y), NULL)',
          'spark2': 'IF((2 * y) <> 0, (x + 1) / (2 * y), NULL)',
          'spark': 'IF((2 * y) <> 0, (x + 1) / (2 * y), NULL)',
          'databricks': 'IF((2 * y) <> 0, (x + 1) / (2 * y), NULL)',
          'snowflake': 'IFF((2 * y) <> 0, (x + 1) / (2 * y), NULL)',
          'postgres': 'CASE WHEN (2 * y) <> 0 THEN CAST((x + 1) AS DOUBLE PRECISION) / (2 * y) ELSE NULL END',
        },
      },
    );
    this.validateAll(
      'SELECT JSON_VALUE_ARRAY(\'{"arr": [1, "a"]}\', \'$.arr\')',
      {
        write: {
          'bigquery': 'SELECT JSON_VALUE_ARRAY(\'{"arr": [1, "a"]}\', \'$.arr\')',
          'duckdb': 'SELECT CAST(\'{"arr": [1, "a"]}\' -> \'$.arr\' AS TEXT[])',
          'snowflake': 'SELECT TRANSFORM(GET_PATH(PARSE_JSON(\'{"arr": [1, "a"]}\'), \'arr\'), x -> CAST(x AS VARCHAR))',
        },
      },
    );
    this.validateAll(
      'SELECT INSTR(\'foo@example.com\', \'@\')',
      {
        write: {
          'bigquery': 'SELECT INSTR(\'foo@example.com\', \'@\')',
          'duckdb': 'SELECT STRPOS(\'foo@example.com\', \'@\')',
          'snowflake': 'SELECT CHARINDEX(\'@\', \'foo@example.com\')',
        },
      },
    );
    this.validateAll(
      'SELECT ts + MAKE_INTERVAL(1, 2, minute => 5, day => 3)',
      {
        write: {
          'bigquery': 'SELECT ts + MAKE_INTERVAL(1, 2, day => 3, minute => 5)',
          'duckdb': 'SELECT ts + INTERVAL \'1 year 2 month 5 minute 3 day\'',
          'snowflake': 'SELECT ts + INTERVAL \'1 year, 2 month, 5 minute, 3 day\'',
        },
      },
    );
    this.validateAll(
      'SELECT INT64(JSON_QUERY(JSON \'{"key": 2000}\', \'$.key\'))',
      {
        write: {
          'bigquery': 'SELECT INT64(JSON_QUERY(PARSE_JSON(\'{"key": 2000}\'), \'$.key\'))',
          'duckdb': 'SELECT CAST(JSON(\'{"key": 2000}\') -> \'$.key\' AS BIGINT)',
          'snowflake': 'SELECT CAST(GET_PATH(PARSE_JSON(\'{"key": 2000}\'), \'key\') AS BIGINT)',
        },
      },
    );

    this.validateIdentity('CONTAINS_SUBSTR(a, b, json_scope => \'JSON_KEYS_AND_VALUES\')');

    this.validateAll(
      'CONTAINS_SUBSTR(a, b)',
      {
        read: {
          '': 'CONTAINS(a, b)',
          'spark': 'CONTAINS(a, b)',
          'databricks': 'CONTAINS(a, b)',
          'snowflake': 'CONTAINS(a, b)',
          'duckdb': 'CONTAINS(a, b)',
          'oracle': 'CONTAINS(a, b)',
        },
        write: {
          '': 'CONTAINS(LOWER(a), LOWER(b))',
          'spark': 'CONTAINS(LOWER(a), LOWER(b))',
          'databricks': 'CONTAINS(LOWER(a), LOWER(b))',
          'snowflake': 'CONTAINS(LOWER(a), LOWER(b))',
          'duckdb': 'CONTAINS(LOWER(a), LOWER(b))',
          'oracle': 'CONTAINS(LOWER(a), LOWER(b))',
          'bigquery': 'CONTAINS_SUBSTR(a, b)',
        },
      },
    );

    this.validateIdentity(
      'EXPORT DATA OPTIONS (URI=\'gs://path*.csv.gz\', FORMAT=\'CSV\') AS SELECT * FROM all_rows',
    );
    this.validateIdentity(
      'EXPORT DATA WITH CONNECTION myproject.us.myconnection OPTIONS (URI=\'gs://path*.csv.gz\', FORMAT=\'CSV\') AS SELECT * FROM all_rows',
    );

    this.validateAll(
      'SELECT * FROM t1, UNNEST(`t1`) AS `col`',
      {
        read: { 'duckdb': 'SELECT * FROM t1, UNNEST("t1") "t1" ("col")' },
        write: {
          'bigquery': 'SELECT * FROM t1 CROSS JOIN UNNEST(`t1`) AS `col`',
          'redshift': 'SELECT * FROM t1 CROSS JOIN "t1" AS "col"',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM t, UNNEST(`t2`.`t3`) AS `col`',
      {
        read: { 'duckdb': 'SELECT * FROM t, UNNEST("t1"."t2"."t3") "t1" ("col")' },
        write: {
          'bigquery': 'SELECT * FROM t CROSS JOIN UNNEST(`t2`.`t3`) AS `col`',
          'redshift': 'SELECT * FROM t CROSS JOIN "t2"."t3" AS "col"',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM t1, UNNEST(`t1`.`t2`.`t3`.`t4`) AS `col`',
      {
        read: { 'duckdb': 'SELECT * FROM t1, UNNEST("t1"."t2"."t3"."t4") "t3" ("col")' },
        write: {
          'bigquery': 'SELECT * FROM t1 CROSS JOIN UNNEST(`t1`.`t2`.`t3`.`t4`) AS `col`',
          'redshift': 'SELECT * FROM t1 CROSS JOIN "t1"."t2"."t3"."t4" AS "col"',
        },
      },
    );

    this.validateIdentity('ARRAY_FIRST([\'a\', \'b\'])');
    this.validateIdentity('ARRAY_LAST([\'a\', \'b\'])');
    this.validateIdentity('JSON_TYPE(PARSE_JSON(\'1\'))');

    this.validateAll(
      'SELECT CAST(col AS STRUCT<fld1 STRUCT<fld2 INT>>).fld1.fld2',
      {
        write: {
          'bigquery': 'SELECT CAST(col AS STRUCT<fld1 STRUCT<fld2 INT64>>).fld1.fld2',
          'snowflake': 'SELECT CAST(col AS OBJECT(fld1 OBJECT(fld2 INT))):fld1.fld2',
        },
      },
    );
    this.validateIdentity(
      'SELECT PARSE_DATETIME(\'%a %b %e %I:%M:%S %Y\', \'Thu Dec 25 07:30:00 2008\')',
    );
    this.validateIdentity('FORMAT_TIME(\'%R\', CAST(\'15:30:00\' AS TIME))');
    this.validateIdentity('PARSE_TIME(\'%I:%M:%S\', \'07:30:00\')');
    this.validateIdentity('BYTE_LENGTH(\'foo\')');
    this.validateIdentity('BYTE_LENGTH(b\'foo\')');
    this.validateIdentity('CODE_POINTS_TO_STRING([65, 255])');
    this.validateIdentity('APPROX_TOP_COUNT(col, 2)');
    this.validateIdentity('ARPOX_TOP_SUM(col, 1.5, 2)');
    this.validateIdentity('SAFE_CONVERT_BYTES_TO_STRING(b\'\\xc2\')');
    this.validateIdentity('FROM_HEX(\'foo\')');
    this.validateIdentity('TO_CODE_POINTS(\'foo\')');
    this.validateIdentity('CODE_POINTS_TO_BYTES([65, 98])');
    this.validateIdentity('PARSE_BIGNUMERIC(\'1.2\')');
    this.validateIdentity('PARSE_NUMERIC(\'1.2\')');
    this.validateIdentity('BOOL(PARSE_JSON(\'true\'))');
    this.validateIdentity('FLOAT64(PARSE_JSON(\'9.8\'))');
    this.validateIdentity('FLOAT64(PARSE_JSON(\'9.8\'), wide_number_mode => \'round\')');
    this.validateIdentity('FLOAT64(PARSE_JSON(\'9.8\'), wide_number_mode => \'exact\')');
    this.validateIdentity('NORMALIZE_AND_CASEFOLD(\'foo\')');
    this.validateIdentity('NORMALIZE_AND_CASEFOLD(\'foo\', NFKC)');
    this.validateIdentity('OCTET_LENGTH(\'foo\')', 'BYTE_LENGTH(\'foo\')');
    this.validateIdentity('OCTET_LENGTH(b\'foo\')', 'BYTE_LENGTH(b\'foo\')');
    this.validateIdentity(
      'JSON_ARRAY_APPEND(PARSE_JSON(\'["a", "b", "c"]\'), \'$\', [1, 2], append_each_element => FALSE)',
    );
    this.validateIdentity(
      'JSON_ARRAY_INSERT(PARSE_JSON(\'["a", "b", "c"]\'), \'$[1]\', [1, 2], insert_each_element => FALSE)',
    );
    this.validateIdentity('JSON_KEYS(PARSE_JSON(\'{"a": {"b":1}}\'))');
    this.validateIdentity('JSON_KEYS(PARSE_JSON(\'{"a": {"b":1}}\'), 1)');
    this.validateIdentity('JSON_KEYS(PARSE_JSON(\'{"a": {"b":1}}\'), 1, mode => \'lax\')');
    this.validateIdentity(
      'JSON_SET(PARSE_JSON(\'{"a": 1}\'), \'$.b\', 999, create_if_missing => FALSE)',
    );
    this.validateIdentity('JSON_STRIP_NULLS(PARSE_JSON(\'[1, null, 2, null, [null]]\'))');
    this.validateIdentity(
      'JSON_STRIP_NULLS(PARSE_JSON(\'[1, null, 2, null]\'), include_arrays => FALSE)',
    );
    this.validateIdentity(
      'JSON_STRIP_NULLS(PARSE_JSON(\'{"a": {"b": {"c": null}}, "d": [null], "e": [], "f": 1}\'), include_arrays => FALSE, remove_empty => TRUE)',
    );
    this.validateIdentity(
      'JSON_EXTRACT_STRING_ARRAY(PARSE_JSON(\'{"fruits": ["apples", "oranges", "grapes"]}\'), \'$.fruits\')',
      'JSON_VALUE_ARRAY(PARSE_JSON(\'{"fruits": ["apples", "oranges", "grapes"]}\'), \'$.fruits\')',
    );
    this.validateIdentity('TO_JSON(STRUCT(1 AS id, [10, 20] AS cords))');
    this.validateIdentity('TO_JSON(9999999999, stringify_wide_numbers => FALSE)');
    this.validateIdentity('RANGE_BUCKET(20, [0, 10, 20, 30, 40])');
    this.validateIdentity('SELECT TRANSLATE(MODEL, \'in\', \'t\') FROM (SELECT \'input\' AS MODEL)');
    this.validateIdentity('SELECT GRANT FROM (SELECT \'input\' AS GRANT)');

    this.validateAll(
      'SELECT 0xA',
      {
        write: {
          'bigquery': 'SELECT 0xA',
          'duckdb': 'SELECT 10',
        },
      },
    );
    this.validateAll(
      'SELECT ARRAY_CONCAT_AGG(1)',
      {
        write: {
          'snowflake': 'SELECT ARRAY_FLATTEN(ARRAY_AGG(1))',
          'bigquery': 'SELECT ARRAY_CONCAT_AGG(1)',
        },
      },
    );
    this.validateAll(
      'SELECT b\'\\x61\'',
      {
        write: {
          'bigquery': 'SELECT b\'\\x61\'',
          'duckdb': 'SELECT CAST(e\'\\x61\' AS BLOB)',
          'postgres': 'SELECT CAST(e\'\\x61\' AS BYTEA)',
        },
      },
    );
    this.validateAll(
      'SELECT b\'a\'',
      {
        write: {
          'bigquery': 'SELECT b\'a\'',
          'duckdb': 'SELECT CAST(e\'a\' AS BLOB)',
          'postgres': 'SELECT CAST(e\'a\' AS BYTEA)',
        },
      },
    );
    this.validateAll(
      'SELECT GENERATE_UUID()',
      {
        write: {
          'bigquery': 'SELECT GENERATE_UUID()',
          'duckdb': 'SELECT CAST(UUID() AS TEXT)',
          'spark2': 'SELECT CAST(UUID() AS STRING)',
          'spark': 'SELECT CAST(UUID() AS STRING)',
          'presto': 'SELECT CAST(UUID() AS VARCHAR)',
          'trino': 'SELECT CAST(UUID() AS VARCHAR)',
          'snowflake': 'SELECT UUID_STRING()',
        },
      },
    );
    this.validateAll(
      'SELECT REPLACE(\'apple pie\', \'pie\', \'cobbler\') AS result',
      {
        write: {
          'bigquery': 'SELECT REPLACE(\'apple pie\', \'pie\', \'cobbler\') AS result',
          'duckdb': 'SELECT REPLACE(\'apple pie\', \'pie\', \'cobbler\') AS result',
        },
      },
    );

    expr = this.parseOne(
      'SELECT REPLACE(CAST(\'apple pie\' AS BYTES), CAST(\'pie\' AS BYTES), CAST(\'cobbler\' AS BYTES)) AS result',
    );
    annotated = annotateTypes(expr, { dialect: 'bigquery' });
    expect(annotated.sql({ dialect: 'duckdb' })).toBe(
      'SELECT CAST(REPLACE(CAST(CAST(\'apple pie\' AS BLOB) AS TEXT), CAST(CAST(\'pie\' AS BLOB) AS TEXT), CAST(CAST(\'cobbler\' AS BLOB) AS TEXT)) AS BLOB) AS result',
    );

    expr = this.parseOne('REPLACE(\'apple pie\', \'pie\', \'cobbler\')');
    annotated = annotateTypes(expr, { dialect: 'bigquery' });
    expect(annotated.sql({ dialect: 'duckdb' })).toBe('REPLACE(\'apple pie\', \'pie\', \'cobbler\')');

    this.validateAll(
      'TIMESTAMP_TRUNC(TIMESTAMP \'2024-03-15 14:35:47.123456\', DAY, \'America/New_York\')',
      {
        write: {
          'bigquery': 'TIMESTAMP_TRUNC(CAST(\'2024-03-15 14:35:47.123456\' AS TIMESTAMP), DAY, \'America/New_York\')',
          'duckdb': 'DATE_TRUNC(\'DAY\', CAST(\'2024-03-15 14:35:47.123456\' AS TIMESTAMPTZ) AT TIME ZONE \'America/New_York\') AT TIME ZONE \'America/New_York\'',
        },
      },
    );
    this.validateAll(
      'TIMESTAMP_TRUNC(TIMESTAMP \'2024-03-15 14:35:00\', MINUTE, \'America/New_York\')',
      {
        write: {
          'bigquery': 'TIMESTAMP_TRUNC(CAST(\'2024-03-15 14:35:00\' AS TIMESTAMP), MINUTE, \'America/New_York\')',
          'duckdb': 'DATE_TRUNC(\'MINUTE\', CAST(\'2024-03-15 14:35:00\' AS TIMESTAMPTZ))',
        },
      },
    );
    this.validateAll(
      'TIMESTAMP_TRUNC(TIMESTAMP \'2024-03-15 14:35:47.123456\', DAY)',
      {
        write: {
          'bigquery': 'TIMESTAMP_TRUNC(CAST(\'2024-03-15 14:35:47.123456\' AS TIMESTAMP), DAY)',
          'duckdb': 'DATE_TRUNC(\'DAY\', CAST(\'2024-03-15 14:35:47.123456\' AS TIMESTAMPTZ))',
        },
      },
    );
    this.validateAll(
      'TIMESTAMP_TRUNC(TIMESTAMP \'2025-01-01 14:35:47.123456\', MINUTE)',
      {
        write: {
          'bigquery': 'TIMESTAMP_TRUNC(CAST(\'2025-01-01 14:35:47.123456\' AS TIMESTAMP), MINUTE)',
          'duckdb': 'DATE_TRUNC(\'MINUTE\', CAST(\'2025-01-01 14:35:47.123456\' AS TIMESTAMPTZ))',
        },
      },
    );
    this.validateAll(
      'WITH sample AS (SELECT * FROM UNNEST([TIMESTAMP \'2024-03-15 14:35:46\', TIMESTAMP \'2024-03-16 01:12:03\']) AS ts) SELECT ts, TIMESTAMP_TRUNC(ts, DAY, \'America/New_York\') AS truncated_ts FROM sample',
      {
        write: {
          'bigquery': 'WITH sample AS (SELECT * FROM UNNEST([CAST(\'2024-03-15 14:35:46\' AS TIMESTAMP), CAST(\'2024-03-16 01:12:03\' AS TIMESTAMP)]) AS ts) SELECT ts, TIMESTAMP_TRUNC(ts, DAY, \'America/New_York\') AS truncated_ts FROM sample',
          'duckdb': 'WITH sample AS (SELECT * FROM UNNEST([CAST(\'2024-03-15 14:35:46\' AS TIMESTAMPTZ), CAST(\'2024-03-16 01:12:03\' AS TIMESTAMPTZ)]) AS _t0(ts)) SELECT ts, DATE_TRUNC(\'DAY\', ts AT TIME ZONE \'America/New_York\') AT TIME ZONE \'America/New_York\' AS truncated_ts FROM sample',
        },
      },
    );
    this.validateAll(
      'WITH sample AS (SELECT ts FROM UNNEST([TIMESTAMP \'2024-03-15 14:35:46\', TIMESTAMP \'2024-03-16 01:12:03\']) AS ts) SELECT ts, TIMESTAMP_TRUNC(ts, DAY) AS truncated_ts FROM sample',
      {
        write: {
          'bigquery': 'WITH sample AS (SELECT ts FROM UNNEST([CAST(\'2024-03-15 14:35:46\' AS TIMESTAMP), CAST(\'2024-03-16 01:12:03\' AS TIMESTAMP)]) AS ts) SELECT ts, TIMESTAMP_TRUNC(ts, DAY) AS truncated_ts FROM sample',
          'duckdb': 'WITH sample AS (SELECT ts FROM UNNEST([CAST(\'2024-03-15 14:35:46\' AS TIMESTAMPTZ), CAST(\'2024-03-16 01:12:03\' AS TIMESTAMPTZ)]) AS _t0(ts)) SELECT ts, DATE_TRUNC(\'DAY\', ts) AS truncated_ts FROM sample',
        },
      },
    );
    this.validateAll(
      'WITH sample AS (SELECT * FROM UNNEST([TIMESTAMP \'2024-03-15 14:35:46\', TIMESTAMP \'2024-03-16 01:12:03\']) AS ts) SELECT ts, TIMESTAMP_TRUNC(ts, MINUTE, \'America/New_York\') AS truncated_ts FROM sample',
      {
        write: {
          'bigquery': 'WITH sample AS (SELECT * FROM UNNEST([CAST(\'2024-03-15 14:35:46\' AS TIMESTAMP), CAST(\'2024-03-16 01:12:03\' AS TIMESTAMP)]) AS ts) SELECT ts, TIMESTAMP_TRUNC(ts, MINUTE, \'America/New_York\') AS truncated_ts FROM sample',
          'duckdb': 'WITH sample AS (SELECT * FROM UNNEST([CAST(\'2024-03-15 14:35:46\' AS TIMESTAMPTZ), CAST(\'2024-03-16 01:12:03\' AS TIMESTAMPTZ)]) AS _t0(ts)) SELECT ts, DATE_TRUNC(\'MINUTE\', ts) AS truncated_ts FROM sample',
        },
      },
    );
    this.validateAll(
      'WITH sample AS (SELECT * FROM UNNEST([TIMESTAMP \'2024-03-15 14:35:46\', TIMESTAMP \'2024-03-16 01:12:03\']) AS ts) SELECT ts, TIMESTAMP_TRUNC(ts, MINUTE) AS truncated_ts FROM sample',
      {
        write: {
          'bigquery': 'WITH sample AS (SELECT * FROM UNNEST([CAST(\'2024-03-15 14:35:46\' AS TIMESTAMP), CAST(\'2024-03-16 01:12:03\' AS TIMESTAMP)]) AS ts) SELECT ts, TIMESTAMP_TRUNC(ts, MINUTE) AS truncated_ts FROM sample',
          'duckdb': 'WITH sample AS (SELECT * FROM UNNEST([CAST(\'2024-03-15 14:35:46\' AS TIMESTAMPTZ), CAST(\'2024-03-16 01:12:03\' AS TIMESTAMPTZ)]) AS _t0(ts)) SELECT ts, DATE_TRUNC(\'MINUTE\', ts) AS truncated_ts FROM sample',
        },
      },
    );
    this.validateAll(
      'SELECT GREATEST(1, NULL, 3)',
      {
        write: {
          'duckdb': 'SELECT CASE WHEN 1 IS NULL OR NULL IS NULL OR 3 IS NULL THEN NULL ELSE GREATEST(1, NULL, 3) END',
          'bigquery': 'SELECT GREATEST(1, NULL, 3)',
        },
      },
    );
    this.validateAll(
      'SELECT LEAST(1, NULL, 3)',
      {
        write: {
          'duckdb': 'SELECT CASE WHEN 1 IS NULL OR NULL IS NULL OR 3 IS NULL THEN NULL ELSE LEAST(1, NULL, 3) END',
          'bigquery': 'SELECT LEAST(1, NULL, 3)',
        },
      },
    );
  }

  testErrors () {
    expect(() => this.parseOne('SELECT * FROM a - b.c.d2')).toThrow(ParseError);

    expect(() => transpile('\'\\\'', { read: 'bigquery' })).toThrow(TokenError);

    expect(() => transpile(
      'SELECT * FROM a INTERSECT ALL SELECT * FROM b',
      { write: 'bigquery', unsupportedLevel: ErrorLevel.RAISE },
    )).toThrow(UnsupportedError);

    expect(() => transpile(
      'SELECT * FROM a EXCEPT ALL SELECT * FROM b',
      { write: 'bigquery', unsupportedLevel: ErrorLevel.RAISE },
    )).toThrow(UnsupportedError);

    expect(() => transpile('SELECT * FROM UNNEST(x) AS x(y)', { read: 'bigquery' })).toThrow(ParseError);

    expect(() => transpile('DATE_ADD(x, day)', { read: 'bigquery' })).toThrow(ParseError);
  }

  testWarnings () {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    try {
      this.validateIdentity(
        'WITH cte(c) AS (SELECT * FROM t) SELECT * FROM cte',
        'WITH cte AS (SELECT * FROM t) SELECT * FROM cte',
      );
    } finally {
      warnSpy.mockRestore();
    }

    const warnSpy2 = vi.spyOn(console, 'warn').mockImplementation(() => {});
    try {
      this.validateIdentity(
        'SELECT * FROM t AS t(c1, c2)',
        'SELECT * FROM t AS t',
      );
    } finally {
      warnSpy2.mockRestore();
    }

    const warnSpy3 = vi.spyOn(console, 'warn').mockImplementation(() => {});
    try {
      this.validateIdentity(
        'SELECT a[1], b[OFFSET(1)], c[ORDINAL(1)], d[SAFE_OFFSET(1)], e[SAFE_ORDINAL(1)]',
      );
      this.validateAll(
        'SELECT a[1], b[OFFSET(1)], c[ORDINAL(1)], d[SAFE_OFFSET(1)], e[SAFE_ORDINAL(1)]',
        {
          write: {
            'duckdb': 'SELECT a[2], b[2], c[1], d[2], e[1]',
            'bigquery': 'SELECT a[1], b[OFFSET(1)], c[ORDINAL(1)], d[SAFE_OFFSET(1)], e[SAFE_ORDINAL(1)]',
            'presto': 'SELECT a[2], b[2], c[1], ELEMENT_AT(d, 2), ELEMENT_AT(e, 1)',
          },
        },
      );
      this.validateAll(
        'a[0]',
        {
          read: {
            'bigquery': 'a[0]',
            'duckdb': 'a[1]',
            'presto': 'a[1]',
          },
        },
      );
    } finally {
      warnSpy3.mockRestore();
    }
  }

  testUserDefinedFunctions () {
    this.validateIdentity(
      'CREATE TEMPORARY FUNCTION a(x FLOAT64, y FLOAT64) RETURNS FLOAT64 NOT DETERMINISTIC LANGUAGE js AS \'return x*y;\'',
    );
    this.validateIdentity('CREATE TEMPORARY FUNCTION udf(x ANY TYPE) AS (x)');
    this.validateIdentity('CREATE TEMPORARY FUNCTION a(x FLOAT64, y FLOAT64) AS ((x + 4) / y)');
    this.validateIdentity(
      'CREATE TABLE FUNCTION a(x INT64) RETURNS TABLE <q STRING, r INT64> AS SELECT s, t',
    );
    this.validateIdentity(
      'CREATE TEMPORARY FUNCTION string_length_0(strings ARRAY<STRING>) RETURNS FLOAT64 LANGUAGE js OPTIONS (library=[\'gs://ibis-testing-libraries/lodash.min.js\']) AS \'\\\'use strict\\\'; function string_length(strings) { return _.sum(_.map(strings, ((x) => x.length))); } return string_length(strings);\'',
    );
  }

  testRemovePrecisionParameterizedTypes () {
    this.validateIdentity('CREATE TABLE test (a NUMERIC(10, 2))');
    this.validateIdentity(
      'INSERT INTO test (cola, colb) VALUES (CAST(7 AS STRING(10)), CAST(14 AS STRING(10)))',
      'INSERT INTO test (cola, colb) VALUES (CAST(7 AS STRING), CAST(14 AS STRING))',
    );
    this.validateIdentity(
      'SELECT CAST(1 AS NUMERIC(10, 2))',
      'SELECT CAST(1 AS NUMERIC)',
    );
    this.validateIdentity(
      'SELECT CAST(\'1\' AS STRING(10)) UNION ALL SELECT CAST(\'2\' AS STRING(10))',
      'SELECT CAST(\'1\' AS STRING) UNION ALL SELECT CAST(\'2\' AS STRING)',
    );
    this.validateIdentity(
      'SELECT cola FROM (SELECT CAST(\'1\' AS STRING(10)) AS cola UNION ALL SELECT CAST(\'2\' AS STRING(10)) AS cola)',
      'SELECT cola FROM (SELECT CAST(\'1\' AS STRING) AS cola UNION ALL SELECT CAST(\'2\' AS STRING) AS cola)',
    );
  }

  testGapFill () {
    this.validateIdentity(
      'SELECT * FROM GAP_FILL(TABLE device_data, ts_column => \'time\', bucket_width => INTERVAL \'1\' MINUTE, value_columns => [(\'signal\', \'locf\')]) ORDER BY time',
    );
    this.validateIdentity(
      'SELECT a, b, c, d, e FROM GAP_FILL(TABLE foo, ts_column => \'b\', partitioning_columns => [\'a\'], value_columns => [(\'c\', \'bar\'), (\'d\', \'baz\'), (\'e\', \'bla\')], bucket_width => INTERVAL \'1\' DAY)',
    );
    this.validateIdentity(
      'SELECT * FROM GAP_FILL(TABLE device_data, ts_column => \'time\', bucket_width => INTERVAL \'1\' MINUTE, value_columns => [(\'signal\', \'linear\')], ignore_null_values => FALSE) ORDER BY time',
    );
    this.validateIdentity(
      'SELECT * FROM GAP_FILL(TABLE device_data, ts_column => \'time\', bucket_width => INTERVAL \'1\' MINUTE) ORDER BY time',
    );
    this.validateIdentity(
      'SELECT * FROM GAP_FILL(TABLE device_data, ts_column => \'time\', bucket_width => INTERVAL \'1\' MINUTE, value_columns => [(\'signal\', \'null\')], origin => CAST(\'2023-11-01 09:30:01\' AS DATETIME)) ORDER BY time',
    );
    this.validateIdentity(
      'SELECT * FROM GAP_FILL(TABLE device_data, ts_column => \'time\', bucket_width => INTERVAL \'1\' MINUTE, value_columns => [(\'signal\', \'locf\')]) ORDER BY time',
    );
  }

  testModels () {
    this.validateIdentity(
      'CREATE OR REPLACE MODEL foo OPTIONS (model_type=\'linear_reg\') AS SELECT bla FROM foo WHERE cond',
    );
    this.validateIdentity(
      `CREATE OR REPLACE MODEL m
TRANSFORM(
  ML.FEATURE_CROSS(STRUCT(f1, f2)) AS cross_f,
  ML.QUANTILE_BUCKETIZE(f3) OVER () AS buckets,
  label_col
)
OPTIONS (
  model_type='linear_reg',
  input_label_cols=['label_col']
) AS
SELECT
  *
FROM t`,
      undefined,
      { pretty: true },
    );
    this.validateIdentity(
      `CREATE MODEL project_id.mydataset.mymodel
INPUT(
  f1 INT64,
  f2 FLOAT64,
  f3 STRING,
  f4 ARRAY<INT64>
)
OUTPUT(
  out1 INT64,
  out2 INT64
)
REMOTE WITH CONNECTION myproject.us.test_connection
OPTIONS (
  ENDPOINT='https://us-central1-aiplatform.googleapis.com/v1/projects/myproject/locations/us-central1/endpoints/1234'
)`,
      undefined,
      { pretty: true },
    );
  }

  testMlFunctions () {
    const ast = this.validateIdentity(
      'SELECT * FROM ML.PREDICT(MODEL mydataset.mymodel, (SELECT label, column1, column2 FROM mydataset.mytable))',
    );
    expect((ast as any).find(PredictExpr)).toBeTruthy();

    this.validateIdentity(
      'SELECT label, predicted_label1, predicted_label AS predicted_label2 FROM ML.PREDICT(MODEL mydataset.mymodel2, (SELECT * EXCEPT (predicted_label), predicted_label AS predicted_label1 FROM ML.PREDICT(MODEL mydataset.mymodel1, TABLE mydataset.mytable)))',
    );
    this.validateIdentity(
      'SELECT * FROM ML.PREDICT(MODEL mydataset.mymodel, (SELECT custom_label, column1, column2 FROM mydataset.mytable), STRUCT(0.55 AS threshold))',
    );
    this.validateIdentity('SELECT COSH(1.5)');
    this.validateIdentity(
      'SELECT * FROM ML.PREDICT(MODEL `my_project`.my_dataset.my_model, (SELECT * FROM input_data))',
    );
    this.validateIdentity(
      'SELECT * FROM ML.PREDICT(MODEL my_dataset.vision_model, (SELECT uri, ML.RESIZE_IMAGE(ML.DECODE_IMAGE(data), 480, 480, FALSE) AS input FROM my_dataset.object_table))',
    );
    this.validateIdentity(
      'SELECT * FROM ML.PREDICT(MODEL my_dataset.vision_model, (SELECT uri, ML.CONVERT_COLOR_SPACE(ML.RESIZE_IMAGE(ML.DECODE_IMAGE(data), 224, 280, TRUE), \'YIQ\') AS input FROM my_dataset.object_table WHERE content_type = \'image/jpeg\'))',
    );
    const ast2 = this.validateIdentity('SELECT * FROM ML.FEATURES_AT_TIME((SELECT 1), num_rows => 1)');
    expect((ast2 as any).find(FeaturesAtTimeExpr)).toBeTruthy();
    this.validateIdentity(
      'SELECT * FROM ML.FEATURES_AT_TIME(TABLE mydataset.feature_table, time => \'2022-06-11 10:00:00+00\', num_rows => 1, ignore_feature_nulls => TRUE)',
    );

    const ast3 = this.validateIdentity(
      'SELECT * FROM VECTOR_SEARCH(TABLE mydataset.base_table, \'column_to_search\', TABLE mydataset.query_table, \'query_column_to_search\', top_k => 2, distance_type => \'cosine\', options => \'{"fraction_lists_to_search":0.15}\')',
    );
    expect((ast3 as any).find(VectorSearchExpr)).toBeTruthy();
    this.validateIdentity(
      'SELECT * FROM VECTOR_SEARCH(TABLE mydataset.base_table, \'column_to_search\', TABLE mydataset.query_table, query_column_to_search => \'query_column_to_search\', top_k => 2, distance_type => \'cosine\', options => \'{"fraction_lists_to_search":0.15}\')',
    );
    this.validateIdentity(
      'SELECT * FROM VECTOR_SEARCH((SELECT * FROM mydataset.base_table), \'column_to_search\', (SELECT * FROM mydataset.query_table), \'query_column_to_search\')',
    );
    this.validateIdentity(
      'SELECT * FROM VECTOR_SEARCH(TABLE mydataset.base_table, \'column_to_search\', TABLE mydataset.query_table)',
    );
    this.validateIdentity(
      'SELECT * FROM ML.TRANSLATE(MODEL `mydataset.mytranslatemodel`, TABLE `mydataset.mybqtable`, STRUCT(\'translate_text\' AS translate_mode, \'zh-CN\' AS target_language_code))',
    );
    const mlTranslateAst = this.validateIdentity(
      'SELECT * FROM ML.TRANSLATE(MODEL `mydataset.mymodel`, (SELECT comment AS text_content FROM mydataset.mytable), STRUCT(\'translate_text\' AS translate_mode, \'en\' AS target_language_code))',
    );
    (mlTranslateAst as any).find(MlTranslateExpr).assertIs(MlTranslateExpr);
    this.validateIdentity('TRANSLATE(x, y, z)').assertIs(TranslateExpr);

    const ast4 = this.validateIdentity(
      'SELECT * FROM ML.FORECAST(MODEL `mydataset.mymodel`, STRUCT(2 AS horizon))',
    );
    expect((ast4 as any).find(MlForecastExpr)).toBeTruthy();
    this.validateIdentity(
      'SELECT * FROM ML.FORECAST(MODEL `mydataset.mymodel`, TABLE `mydataset.mybqtable`, STRUCT(2 AS horizon, 4 AS confidence_level))',
    );
    this.validateIdentity(
      'SELECT * FROM ML.FORECAST(MODEL `mydataset.mymodel`, (SELECT * FROM mydataset.query_table), STRUCT())',
    );

    for (const name of ['GENERATE_EMBEDDING', 'GENERATE_TEXT_EMBEDDING']) {
      const genAst = this.validateIdentity(
        `SELECT * FROM ML.${name}(MODEL mydataset.mymodel, (SELECT label, column1, column2 FROM mydataset.mytable))`,
      );
      this.validateIdentity(
        `SELECT * FROM ML.${name}(MODEL mydataset.mymodel, TABLE mydataset.mytable, STRUCT(TRUE AS flatten_json_output))`,
      );
      expect((genAst as any).find(GenerateEmbeddingExpr)).toBeTruthy();
    }
  }

  testMerge () {
    this.validateAll(
      `
            MERGE dataset.Inventory T
            USING dataset.NewArrivals S ON FALSE
            WHEN NOT MATCHED BY TARGET AND product LIKE '%a%'
            THEN DELETE
            WHEN NOT MATCHED BY SOURCE AND product LIKE '%b%'
            THEN DELETE`,
      {
        write: {
          'bigquery': 'MERGE INTO dataset.Inventory AS T USING dataset.NewArrivals AS S ON FALSE WHEN NOT MATCHED AND product LIKE \'%a%\' THEN DELETE WHEN NOT MATCHED BY SOURCE AND product LIKE \'%b%\' THEN DELETE',
          'snowflake': 'MERGE INTO dataset.Inventory AS T USING dataset.NewArrivals AS S ON FALSE WHEN NOT MATCHED AND product LIKE \'%a%\' THEN DELETE WHEN NOT MATCHED AND product LIKE \'%b%\' THEN DELETE',
        },
      },
    );
  }

  testRenameTable () {
    this.validateAll(
      'ALTER TABLE db.t1 RENAME TO db.t2',
      {
        write: {
          'snowflake': 'ALTER TABLE db.t1 RENAME TO db.t2',
          'bigquery': 'ALTER TABLE db.t1 RENAME TO t2',
        },
      },
    );
  }

  testPushdownCteColumnNames () {
    expect(() => transpile(
      'WITH cte(foo) AS (SELECT * FROM tbl) SELECT foo FROM cte',
      { read: 'spark', write: 'bigquery', unsupportedLevel: ErrorLevel.RAISE },
    )).toThrow(UnsupportedError);

    this.validateAll(
      'WITH cte AS (SELECT 1 AS foo) SELECT foo FROM cte',
      { read: { 'spark': 'WITH cte(foo) AS (SELECT 1) SELECT foo FROM cte' } },
    );
    this.validateAll(
      'WITH cte AS (SELECT 1 AS foo) SELECT foo FROM cte',
      { read: { 'spark': 'WITH cte(foo) AS (SELECT 1 AS bar) SELECT foo FROM cte' } },
    );
    this.validateAll(
      'WITH cte AS (SELECT 1 AS bar) SELECT bar FROM cte',
      { read: { 'spark': 'WITH cte AS (SELECT 1 AS bar) SELECT bar FROM cte' } },
    );
    this.validateAll(
      'WITH cte AS (SELECT 1 AS foo, 2) SELECT foo FROM cte',
      { read: { 'postgres': 'WITH cte(foo) AS (SELECT 1, 2) SELECT foo FROM cte' } },
    );
    this.validateAll(
      'WITH cte AS (SELECT 1 AS foo UNION ALL SELECT 2) SELECT foo FROM cte',
      { read: { 'postgres': 'WITH cte(foo) AS (SELECT 1 UNION ALL SELECT 2) SELECT foo FROM cte' } },
    );
  }

  testJsonObject () {
    this.validateIdentity('SELECT JSON_OBJECT() AS json_data');
    this.validateIdentity('SELECT JSON_OBJECT(\'foo\', 10, \'bar\', TRUE) AS json_data');
    this.validateIdentity('SELECT JSON_OBJECT(\'foo\', 10, \'bar\', [\'a\', \'b\']) AS json_data');
    this.validateIdentity('SELECT JSON_OBJECT(\'a\', 10, \'a\', \'foo\') AS json_data');
    this.validateIdentity(
      'SELECT JSON_OBJECT([\'a\', \'b\'], [10, NULL]) AS json_data',
      'SELECT JSON_OBJECT(\'a\', 10, \'b\', NULL) AS json_data',
    );
    this.validateIdentity(
      'SELECT JSON_OBJECT([\'a\', \'b\'], [JSON \'10\', JSON \'"foo"\']) AS json_data',
      'SELECT JSON_OBJECT(\'a\', PARSE_JSON(\'10\'), \'b\', PARSE_JSON(\'"foo"\')) AS json_data',
    );
    this.validateIdentity(
      'SELECT JSON_OBJECT([\'a\', \'b\'], [STRUCT(10 AS id, \'Red\' AS color), STRUCT(20 AS id, \'Blue\' AS color)]) AS json_data',
      'SELECT JSON_OBJECT(\'a\', STRUCT(10 AS id, \'Red\' AS color), \'b\', STRUCT(20 AS id, \'Blue\' AS color)) AS json_data',
    );
    this.validateIdentity(
      'SELECT JSON_OBJECT([\'a\', \'b\'], [TO_JSON(10), TO_JSON([\'foo\', \'bar\'])]) AS json_data',
      'SELECT JSON_OBJECT(\'a\', TO_JSON(10), \'b\', TO_JSON([\'foo\', \'bar\'])) AS json_data',
    );

    expect(() => transpile('SELECT JSON_OBJECT(\'a\', 1, \'b\') AS json_data', { read: 'bigquery' })).toThrow(ParseError);
  }

  testMod () {
    for (const sql of ['MOD(a, b)', 'MOD(\'a\', b)', 'MOD(5, 2)', 'MOD((a + 1) * 8, 5 - 1)']) {
      this.validateIdentity(sql);
    }

    this.validateIdentity('SELECT MOD((SELECT 1), 2)');
    this.validateIdentity('MOD((a + 1), b)', 'MOD(a + 1, b)');
  }

  testInlineConstructor () {
    this.validateIdentity(
      'SELECT STRUCT<ARRAY<STRING>>(["2023-01-17"])',
      'SELECT CAST(STRUCT([\'2023-01-17\']) AS STRUCT<ARRAY<STRING>>)',
    );
    this.validateIdentity(
      'SELECT STRUCT<STRING>((SELECT \'foo\')).*',
      'SELECT CAST(STRUCT((SELECT \'foo\')) AS STRUCT<STRING>).*',
    );

    this.validateAll(
      'SELECT ARRAY<FLOAT64>[1, 2, 3]',
      {
        write: {
          'bigquery': 'SELECT ARRAY<FLOAT64>[1, 2, 3]',
          'duckdb': 'SELECT CAST([1, 2, 3] AS DOUBLE[])',
        },
      },
    );
    this.validateAll(
      'CAST(STRUCT<a INT64>(1) AS STRUCT<a INT64>)',
      {
        write: {
          'bigquery': 'CAST(CAST(STRUCT(1) AS STRUCT<a INT64>) AS STRUCT<a INT64>)',
          'duckdb': 'CAST(CAST(ROW(1) AS STRUCT(a BIGINT)) AS STRUCT(a BIGINT))',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM UNNEST(ARRAY<STRUCT<x INT64>>[])',
      {
        write: {
          'bigquery': 'SELECT * FROM UNNEST(ARRAY<STRUCT<x INT64>>[])',
          'duckdb': 'SELECT * FROM (SELECT UNNEST(CAST([] AS STRUCT(x BIGINT)[]), max_depth => 2))',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM UNNEST(ARRAY<STRUCT<device_id INT64, time DATETIME, signal INT64, state STRING>>[STRUCT(1, DATETIME \'2023-11-01 09:34:01\', 74, \'INACTIVE\'),STRUCT(4, DATETIME \'2023-11-01 09:38:01\', 80, \'ACTIVE\')])',
      {
        write: {
          'bigquery': 'SELECT * FROM UNNEST(ARRAY<STRUCT<device_id INT64, time DATETIME, signal INT64, state STRING>>[STRUCT(1, CAST(\'2023-11-01 09:34:01\' AS DATETIME), 74, \'INACTIVE\'), STRUCT(4, CAST(\'2023-11-01 09:38:01\' AS DATETIME), 80, \'ACTIVE\')])',
          'duckdb': 'SELECT * FROM (SELECT UNNEST(CAST([ROW(1, CAST(\'2023-11-01 09:34:01\' AS TIMESTAMP), 74, \'INACTIVE\'), ROW(4, CAST(\'2023-11-01 09:38:01\' AS TIMESTAMP), 80, \'ACTIVE\')] AS STRUCT(device_id BIGINT, time TIMESTAMP, signal BIGINT, state TEXT)[]), max_depth => 2))',
        },
      },
    );
    this.validateAll(
      'SELECT STRUCT<a INT64, b STRUCT<c STRING>>(1, STRUCT(\'c_str\'))',
      {
        write: {
          'bigquery': 'SELECT CAST(STRUCT(1, STRUCT(\'c_str\')) AS STRUCT<a INT64, b STRUCT<c STRING>>)',
          'duckdb': 'SELECT CAST(ROW(1, ROW(\'c_str\')) AS STRUCT(a BIGINT, b STRUCT(c TEXT)))',
        },
      },
    );
    this.validateAll(
      'SELECT MAX_BY(name, score) FROM table1',
      {
        write: {
          'bigquery': 'SELECT MAX_BY(name, score) FROM table1',
          'duckdb': 'SELECT ARG_MAX(name, score) FROM table1',
        },
      },
    );
    this.validateAll(
      'SELECT MIN_BY(product, price) FROM table1',
      {
        write: {
          'bigquery': 'SELECT MIN_BY(product, price) FROM table1',
          'duckdb': 'SELECT ARG_MIN(product, price) FROM table1',
        },
      },
    );
  }

  testUnnest () {
    this.validateAll(
      'SELECT name, laps FROM UNNEST([STRUCT(\'Rudisha\' AS name, [23.4, 26.3, 26.4, 26.1] AS laps), STRUCT(\'Makhloufi\' AS name, [24.5, 25.4, 26.6, 26.1] AS laps)])',
      {
        write: {
          'bigquery': 'SELECT name, laps FROM UNNEST([STRUCT(\'Rudisha\' AS name, [23.4, 26.3, 26.4, 26.1] AS laps), STRUCT(\'Makhloufi\' AS name, [24.5, 25.4, 26.6, 26.1] AS laps)])',
          'duckdb': 'SELECT name, laps FROM (SELECT UNNEST([{\'name\': \'Rudisha\', \'laps\': [23.4, 26.3, 26.4, 26.1]}, {\'name\': \'Makhloufi\', \'laps\': [24.5, 25.4, 26.6, 26.1]}], max_depth => 2))',
        },
      },
    );
    this.validateAll(
      'WITH Races AS (SELECT \'800M\' AS race) SELECT race, name, laps FROM Races AS r CROSS JOIN UNNEST([STRUCT(\'Rudisha\' AS name, [23.4, 26.3, 26.4, 26.1] AS laps)])',
      {
        write: {
          'bigquery': 'WITH Races AS (SELECT \'800M\' AS race) SELECT race, name, laps FROM Races AS r CROSS JOIN UNNEST([STRUCT(\'Rudisha\' AS name, [23.4, 26.3, 26.4, 26.1] AS laps)])',
          'duckdb': 'WITH Races AS (SELECT \'800M\' AS race) SELECT race, name, laps FROM Races AS r CROSS JOIN (SELECT UNNEST([{\'name\': \'Rudisha\', \'laps\': [23.4, 26.3, 26.4, 26.1]}], max_depth => 2))',
        },
      },
    );
    this.validateAll(
      'SELECT participant FROM UNNEST([STRUCT(\'Rudisha\' AS name, [23.4, 26.3, 26.4, 26.1] AS laps)]) AS participant',
      {
        write: {
          'bigquery': 'SELECT participant FROM UNNEST([STRUCT(\'Rudisha\' AS name, [23.4, 26.3, 26.4, 26.1] AS laps)]) AS participant',
          'duckdb': 'SELECT participant FROM (SELECT UNNEST([{\'name\': \'Rudisha\', \'laps\': [23.4, 26.3, 26.4, 26.1]}], max_depth => 2)) AS participant',
        },
      },
    );
    this.validateAll(
      'WITH Races AS (SELECT \'800M\' AS race) SELECT race, participant FROM Races AS r CROSS JOIN UNNEST([STRUCT(\'Rudisha\' AS name, [23.4, 26.3, 26.4, 26.1] AS laps)]) AS participant',
      {
        write: {
          'bigquery': 'WITH Races AS (SELECT \'800M\' AS race) SELECT race, participant FROM Races AS r CROSS JOIN UNNEST([STRUCT(\'Rudisha\' AS name, [23.4, 26.3, 26.4, 26.1] AS laps)]) AS participant',
          'duckdb': 'WITH Races AS (SELECT \'800M\' AS race) SELECT race, participant FROM Races AS r CROSS JOIN (SELECT UNNEST([{\'name\': \'Rudisha\', \'laps\': [23.4, 26.3, 26.4, 26.1]}], max_depth => 2)) AS participant',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM UNNEST([STRUCT(\'Alice\' AS name, STRUCT(85 AS math, 90 AS english) AS scores), STRUCT(\'Bob\' AS name, STRUCT(92 AS math, 88 AS english) AS scores)])',
      {
        write: {
          'bigquery': 'SELECT * FROM UNNEST([STRUCT(\'Alice\' AS name, STRUCT(85 AS math, 90 AS english) AS scores), STRUCT(\'Bob\' AS name, STRUCT(92 AS math, 88 AS english) AS scores)])',
          'duckdb': 'SELECT * FROM (SELECT UNNEST([{\'name\': \'Alice\', \'scores\': {\'math\': 85, \'english\': 90}}, {\'name\': \'Bob\', \'scores\': {\'math\': 92, \'english\': 88}}], max_depth => 2))',
          'snowflake': 'SELECT * FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'name\', \'Alice\', \'scores\', OBJECT_CONSTRUCT(\'math\', 85, \'english\', 90)), OBJECT_CONSTRUCT(\'name\', \'Bob\', \'scores\', OBJECT_CONSTRUCT(\'math\', 92, \'english\', 88))])) AS _t0(seq, key, path, index, value, this)',
          'presto': 'SELECT * FROM UNNEST(ARRAY[CAST(ROW(\'Alice\', CAST(ROW(85, 90) AS ROW(math INTEGER, english INTEGER))) AS ROW(name VARCHAR, scores ROW(math INTEGER, english INTEGER))), CAST(ROW(\'Bob\', CAST(ROW(92, 88) AS ROW(math INTEGER, english INTEGER))) AS ROW(name VARCHAR, scores ROW(math INTEGER, english INTEGER)))])',
          'trino': 'SELECT * FROM UNNEST(ARRAY[CAST(ROW(\'Alice\', CAST(ROW(85, 90) AS ROW(math INTEGER, english INTEGER))) AS ROW(name VARCHAR, scores ROW(math INTEGER, english INTEGER))), CAST(ROW(\'Bob\', CAST(ROW(92, 88) AS ROW(math INTEGER, english INTEGER))) AS ROW(name VARCHAR, scores ROW(math INTEGER, english INTEGER)))])',
          'spark2': 'SELECT * FROM EXPLODE(ARRAY(STRUCT(\'Alice\' AS name, STRUCT(85 AS math, 90 AS english) AS scores), STRUCT(\'Bob\' AS name, STRUCT(92 AS math, 88 AS english) AS scores)))',
          'databricks': 'SELECT * FROM EXPLODE(ARRAY(STRUCT(\'Alice\' AS name, STRUCT(85 AS math, 90 AS english) AS scores), STRUCT(\'Bob\' AS name, STRUCT(92 AS math, 88 AS english) AS scores)))',
          'hive': 'SELECT * FROM EXPLODE(ARRAY(STRUCT(\'Alice\', STRUCT(85, 90)), STRUCT(\'Bob\', STRUCT(92, 88))))',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM UNNEST([STRUCT(\'Alice\' AS name, 85 AS score), STRUCT(\'Bob\', 92), STRUCT(\'Diana\', 95)])',
      {
        write: {
          'bigquery': 'SELECT * FROM UNNEST([STRUCT(\'Alice\' AS name, 85 AS score), STRUCT(\'Bob\', 92), STRUCT(\'Diana\', 95)])',
          'duckdb': 'SELECT * FROM (SELECT UNNEST([{\'name\': \'Alice\', \'score\': 85}, {\'name\': \'Bob\', \'score\': 92}, {\'name\': \'Diana\', \'score\': 95}], max_depth => 2))',
          'snowflake': 'SELECT * FROM TABLE(FLATTEN(INPUT => [OBJECT_CONSTRUCT(\'name\', \'Alice\', \'score\', 85), OBJECT_CONSTRUCT(\'name\', \'Bob\', \'score\', 92), OBJECT_CONSTRUCT(\'name\', \'Diana\', \'score\', 95)])) AS _t0(seq, key, path, index, value, this)',
          'presto': 'SELECT * FROM UNNEST(ARRAY[CAST(ROW(\'Alice\', 85) AS ROW(name VARCHAR, score INTEGER)), CAST(ROW(\'Bob\', 92) AS ROW(name VARCHAR, score INTEGER)), CAST(ROW(\'Diana\', 95) AS ROW(name VARCHAR, score INTEGER))])',
          'trino': 'SELECT * FROM UNNEST(ARRAY[CAST(ROW(\'Alice\', 85) AS ROW(name VARCHAR, score INTEGER)), CAST(ROW(\'Bob\', 92) AS ROW(name VARCHAR, score INTEGER)), CAST(ROW(\'Diana\', 95) AS ROW(name VARCHAR, score INTEGER))])',
          'spark2': 'SELECT * FROM EXPLODE(ARRAY(STRUCT(\'Alice\' AS name, 85 AS score), STRUCT(\'Bob\' AS name, 92 AS score), STRUCT(\'Diana\' AS name, 95 AS score)))',
          'databricks': 'SELECT * FROM EXPLODE(ARRAY(STRUCT(\'Alice\' AS name, 85 AS score), STRUCT(\'Bob\' AS name, 92 AS score), STRUCT(\'Diana\' AS name, 95 AS score)))',
          'hive': 'SELECT * FROM EXPLODE(ARRAY(STRUCT(\'Alice\', 85), STRUCT(\'Bob\', 92), STRUCT(\'Diana\', 95)))',
        },
      },
    );
  }

  testRangeType () {
    const cases: [string, string][] = [
      ['RANGE<DATE>', '\'[2020-01-01, 2020-12-31)\''],
      ['RANGE<DATE>', '\'[UNBOUNDED, 2020-12-31)\''],
      ['RANGE<DATETIME>', '\'[2020-01-01 12:00:00, 2020-12-31 12:00:00)\''],
      ['RANGE<TIMESTAMP>', '\'[2020-10-01 12:00:00+08, 2020-12-31 12:00:00+08)\''],
    ];
    for (const [type, value] of cases) {
      this.validateIdentity(`SELECT ${type} ${value}`, `SELECT CAST(${value} AS ${type})`);
      expect(this.parseOne(type)).toEqual(DataTypeExpr.build(type, { dialect: 'bigquery' }));
    }

    this.validateIdentity(
      'SELECT RANGE(CAST(\'2022-12-01\' AS DATE), CAST(\'2022-12-31\' AS DATE))',
    );
    this.validateIdentity('SELECT RANGE(NULL, CAST(\'2022-12-31\' AS DATE))');
    this.validateIdentity(
      'SELECT RANGE(CAST(\'2022-10-01 14:53:27\' AS DATETIME), CAST(\'2022-10-01 16:00:00\' AS DATETIME))',
    );
    this.validateIdentity(
      'SELECT RANGE(CAST(\'2022-10-01 14:53:27 America/Los_Angeles\' AS TIMESTAMP), CAST(\'2022-10-01 16:00:00 America/Los_Angeles\' AS TIMESTAMP))',
    );
  }

  testNullOrdering () {
    for (const [sortOrder, nullOrder] of [['ASC', 'NULLS LAST'], ['DESC', 'NULLS FIRST']] as const) {
      this.validateAll(
        `SELECT color, ARRAY_AGG(id ORDER BY id ${sortOrder}) AS ids FROM colors GROUP BY 1`,
        {
          read: {
            '': `SELECT color, ARRAY_AGG(id ORDER BY id ${sortOrder} ${nullOrder}) AS ids FROM colors GROUP BY 1`,
          },
          write: {
            'bigquery': `SELECT color, ARRAY_AGG(id ORDER BY id ${sortOrder}) AS ids FROM colors GROUP BY 1`,
          },
        },
      );
      this.validateAll(
        `SELECT SUM(f1) OVER (ORDER BY f2 ${sortOrder}) FROM t`,
        {
          read: {
            '': `SELECT SUM(f1) OVER (ORDER BY f2 ${sortOrder} ${nullOrder}) FROM t`,
          },
          write: {
            'bigquery': `SELECT SUM(f1) OVER (ORDER BY f2 ${sortOrder}) FROM t`,
          },
        },
      );
    }
  }

  testJsonExtract () {
    this.validateAll(
      'SELECT JSON_QUERY(\'{"class": {"students": []}}\', \'$.class\')',
      {
        write: {
          'bigquery': 'SELECT JSON_QUERY(\'{"class": {"students": []}}\', \'$.class\')',
          'duckdb': 'SELECT \'{"class": {"students": []}}\' -> \'$.class\'',
          'snowflake': 'SELECT GET_PATH(PARSE_JSON(\'{"class": {"students": []}}\'), \'class\')',
        },
      },
    );
    this.validateAll(
      'SELECT JSON_QUERY(foo, \'$.class\')',
      {
        write: {
          'bigquery': 'SELECT JSON_QUERY(foo, \'$.class\')',
          'snowflake': 'SELECT GET_PATH(PARSE_JSON(foo), \'class\')',
        },
      },
    );

    for (const func of ['JSON_EXTRACT_SCALAR', 'JSON_VALUE']) {
      this.validateAll(
        `SELECT ${func}('5')`,
        {
          write: {
            'bigquery': `SELECT ${func}('5', '$')`,
            'duckdb': 'SELECT JSON_VALUE(\'5\', \'$\') ->> \'$\'',
          },
        },
      );
      const sql = `SELECT ${func}('{"name": "Jakob", "age": "6"}', '$.age')`;
      this.validateAll(
        sql,
        {
          write: {
            'bigquery': sql,
            'duckdb': 'SELECT JSON_VALUE(\'{"name": "Jakob", "age": "6"}\', \'$.age\') ->> \'$\'',
            'snowflake': 'SELECT JSON_EXTRACT_PATH_TEXT(\'{"name": "Jakob", "age": "6"}\', \'age\')',
          },
        },
      );
    }

    for (const func of ['JSON_VALUE', 'JSON_QUERY', 'JSON_QUERY_ARRAY']) {
      this.validateIdentity(
        `${func}(doc, '$. a b c .d')`, `${func}(doc, '$." a b c ".d')`,
      );
    }
    for (const func of ['JSON_EXTRACT', 'JSON_EXTRACT_SCALAR', 'JSON_EXTRACT_ARRAY']) {
      this.validateIdentity(
        `${func}(doc, '$. a b c .d')`, `${func}(doc, '$[\\' a b c \\'].d')`,
      );
    }
  }

  testJsonExtractArray () {
    for (const func of ['JSON_QUERY_ARRAY', 'JSON_EXTRACT_ARRAY']) {
      const sql = `SELECT ${func}('{"fruits": [1, "oranges"]}', '$.fruits')`;
      this.validateAll(
        sql,
        {
          write: {
            'bigquery': sql,
            'duckdb': 'SELECT CAST(\'{"fruits": [1, "oranges"]}\' -> \'$.fruits\' AS JSON[])',
            'snowflake': 'SELECT TRANSFORM(GET_PATH(PARSE_JSON(\'{"fruits": [1, "oranges"]}\'), \'fruits\'), x -> PARSE_JSON(TO_JSON(x)))',
          },
        },
      );
    }
  }

  testUnixSeconds () {
    this.validateAll(
      'SELECT UNIX_SECONDS(\'2008-12-25 15:30:00+00\')',
      {
        read: {
          'bigquery': 'SELECT UNIX_SECONDS(\'2008-12-25 15:30:00+00\')',
          'spark': 'SELECT UNIX_SECONDS(\'2008-12-25 15:30:00+00\')',
          'databricks': 'SELECT UNIX_SECONDS(\'2008-12-25 15:30:00+00\')',
        },
        write: {
          'spark': 'SELECT UNIX_SECONDS(\'2008-12-25 15:30:00+00\')',
          'databricks': 'SELECT UNIX_SECONDS(\'2008-12-25 15:30:00+00\')',
          'duckdb': 'SELECT CAST(EPOCH(CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMPTZ)) AS BIGINT)',
          'snowflake': 'SELECT TIMESTAMPDIFF(SECONDS, CAST(\'1970-01-01 00:00:00+00\' AS TIMESTAMPTZ), \'2008-12-25 15:30:00+00\')',
        },
      },
    );

    for (const dialect of ['bigquery', 'spark', 'databricks'] as const) {
      parseOne('UNIX_SECONDS(col)', { read: dialect }).assertIs(UnixSecondsExpr);
    }
  }

  testUnixMicros () {
    this.validateAll(
      'SELECT UNIX_MICROS(\'2008-12-25 15:30:00+00\')',
      {
        write: {
          'bigquery': 'SELECT UNIX_MICROS(\'2008-12-25 15:30:00+00\')',
          'duckdb': 'SELECT EPOCH_US(CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMPTZ))',
        },
      },
    );
    this.validateAll(
      'SELECT UNIX_MICROS(TIMESTAMP \'2008-12-25 15:30:00+00\')',
      {
        write: {
          'bigquery': 'SELECT UNIX_MICROS(CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMP))',
          'duckdb': 'SELECT EPOCH_US(CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMPTZ))',
        },
      },
    );
  }

  testUnixMillis () {
    this.validateAll(
      'SELECT UNIX_MILLIS(\'2008-12-25 15:30:00+00\')',
      {
        write: {
          'bigquery': 'SELECT UNIX_MILLIS(\'2008-12-25 15:30:00+00\')',
          'duckdb': 'SELECT EPOCH_MS(CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMPTZ))',
        },
      },
    );
    this.validateAll(
      'SELECT UNIX_MILLIS(TIMESTAMP \'2008-12-25 15:30:00+00\')',
      {
        write: {
          'bigquery': 'SELECT UNIX_MILLIS(CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMP))',
          'duckdb': 'SELECT EPOCH_MS(CAST(\'2008-12-25 15:30:00+00\' AS TIMESTAMPTZ))',
        },
      },
    );
  }

  testRegexpExtract () {
    this.validateIdentity('REGEXP_EXTRACT(x, \'(?<)\')');
    this.validateIdentity('REGEXP_EXTRACT(`foo`, \'bar: (.+?)\', 1, 1)');
    this.validateIdentity(
      'REGEXP_EXTRACT(svc_plugin_output, r\'\\\\\\((.*)\')' ,
      'REGEXP_EXTRACT(svc_plugin_output, \'\\\\\\\\\\\\((.*)\')' ,
    );
    this.validateIdentity(
      'REGEXP_SUBSTR(value, pattern, position, occurrence)',
      'REGEXP_EXTRACT(value, pattern, position, occurrence)',
    );

    this.validateAll(
      'SELECT REGEXP_EXTRACT(abc, \'pattern(group)\') FROM table',
      {
        write: {
          'bigquery': 'SELECT REGEXP_EXTRACT(abc, \'pattern(group)\') FROM table',
          'duckdb': 'SELECT REGEXP_EXTRACT(abc, \'pattern(group)\', 1) FROM "table"',
        },
      },
    );
    this.validateAll(
      'SELECT REGEXP_EXTRACT(abc, \'pattern(group)\', 1) FROM table',
      {
        write: {
          'bigquery': 'SELECT REGEXP_EXTRACT(abc, \'pattern(group)\', 1) FROM table',
          'duckdb': 'SELECT REGEXP_EXTRACT(abc, \'pattern(group)\', 1) FROM "table"',
        },
      },
    );
    this.validateAll(
      'SELECT REGEXP_EXTRACT(abc, \'pattern(group)\', 2) FROM table',
      {
        write: {
          'bigquery': 'SELECT REGEXP_EXTRACT(abc, \'pattern(group)\', 2) FROM table',
          'duckdb': 'SELECT REGEXP_EXTRACT(NULLIF(SUBSTRING(abc, 2), \'\'), \'pattern(group)\', 1) FROM "table"',
        },
      },
    );
    this.validateAll(
      'SELECT REGEXP_EXTRACT(abc, \'pattern(group)\', 1, 1) FROM table',
      {
        write: {
          'bigquery': 'SELECT REGEXP_EXTRACT(abc, \'pattern(group)\', 1, 1) FROM table',
          'duckdb': 'SELECT REGEXP_EXTRACT(abc, \'pattern(group)\', 1) FROM "table"',
        },
      },
    );
    this.validateAll(
      'SELECT REGEXP_EXTRACT(abc, \'pattern(group)\', 2, 3) FROM table',
      {
        write: {
          'bigquery': 'SELECT REGEXP_EXTRACT(abc, \'pattern(group)\', 2, 3) FROM table',
          'duckdb': 'SELECT ARRAY_EXTRACT(REGEXP_EXTRACT_ALL(NULLIF(SUBSTRING(abc, 2), \'\'), \'pattern(group)\', 1), 3) FROM "table"',
        },
      },
    );
    this.validateAll(
      'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'a[0-9]\')',
      {
        read: {
          'bigquery': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'a[0-9]\')',
          'trino': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'a[0-9]\')',
          'presto': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'a[0-9]\')',
          'snowflake': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'a[0-9]\')',
          'spark': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'a[0-9]\', 0)',
          'databricks': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'a[0-9]\', 0)',
        },
        write: {
          'bigquery': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'a[0-9]\')',
          'trino': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'a[0-9]\')',
          'presto': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'a[0-9]\')',
          'snowflake': 'REGEXP_SUBSTR_ALL(\'a1_a2a3_a4A5a6\', \'a[0-9]\')',
          'duckdb': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'a[0-9]\')',
          'spark': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'a[0-9]\', 0)',
          'databricks': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'a[0-9]\', 0)',
        },
      },
    );
    this.validateAll(
      'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'(a)[0-9]\')',
      {
        write: {
          'bigquery': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'(a)[0-9]\')',
          'trino': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'(a)[0-9]\', 1)',
          'presto': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'(a)[0-9]\', 1)',
          'snowflake': 'REGEXP_SUBSTR_ALL(\'a1_a2a3_a4A5a6\', \'(a)[0-9]\', 1, 1, \'c\', 1)',
          'duckdb': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'(a)[0-9]\', 1)',
          'spark': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'(a)[0-9]\')',
          'databricks': 'REGEXP_EXTRACT_ALL(\'a1_a2a3_a4A5a6\', \'(a)[0-9]\')',
        },
      },
    );
  }

  testFormatTemporal () {
    this.validateAll(
      'SELECT FORMAT_DATE(\'%Y%m%d\', \'2023-12-25\')',
      {
        write: {
          'bigquery': 'SELECT FORMAT_DATE(\'%Y%m%d\', \'2023-12-25\')',
          'duckdb': 'SELECT STRFTIME(CAST(\'2023-12-25\' AS DATE), \'%Y%m%d\')',
        },
      },
    );
    this.validateAll(
      'SELECT FORMAT_DATETIME(\'%Y%m%d %H:%M:%S\', DATETIME \'2023-12-25 15:30:00\')',
      {
        write: {
          'bigquery': 'SELECT FORMAT_DATETIME(\'%Y%m%d %T\', CAST(\'2023-12-25 15:30:00\' AS DATETIME))',
          'duckdb': 'SELECT STRFTIME(CAST(\'2023-12-25 15:30:00\' AS TIMESTAMP), \'%Y%m%d %H:%M:%S\')',
        },
      },
    );
    this.validateAll(
      'SELECT FORMAT_DATETIME(\'%x\', \'2023-12-25 15:30:00\')',
      {
        write: {
          'bigquery': 'SELECT FORMAT_DATETIME(\'%D\', \'2023-12-25 15:30:00\')',
          'duckdb': 'SELECT STRFTIME(CAST(\'2023-12-25 15:30:00\' AS TIMESTAMP), \'%m/%d/%y\')',
        },
      },
    );
    this.validateAll(
      'SELECT FORMAT_DATETIME(\'%F %T\', DATETIME \'2023-10-15 14:30:45\')',
      {
        write: {
          'bigquery': 'SELECT FORMAT_DATETIME(\'%F %T\', CAST(\'2023-10-15 14:30:45\' AS DATETIME))',
          'duckdb': 'SELECT STRFTIME(CAST(\'2023-10-15 14:30:45\' AS TIMESTAMP), \'%Y-%m-%d %H:%M:%S\')',
        },
      },
    );
    this.validateAll(
      'SELECT FORMAT_DATETIME(\'%c\', DATETIME \'2008-12-25 15:30:00\')',
      {
        write: {
          'bigquery': 'SELECT FORMAT_DATETIME(\'%c\', CAST(\'2008-12-25 15:30:00\' AS DATETIME))',
          'duckdb': 'SELECT STRFTIME(CAST(\'2008-12-25 15:30:00\' AS TIMESTAMP), \'%a %b %-d %H:%M:%S %Y\')',
        },
      },
    );
    this.validateAll(
      'SELECT FORMAT_DATETIME(\'%Y-%m-%e\', DATETIME \'2020-09-09 10:15:30\')',
      {
        write: {
          'bigquery': 'SELECT FORMAT_DATETIME(\'%Y-%m-%e\', CAST(\'2020-09-09 10:15:30\' AS DATETIME))',
          'duckdb': 'SELECT STRFTIME(CAST(\'2020-09-09 10:15:30\' AS TIMESTAMP), \'%Y-%m-%-d\')',
        },
      },
    );
    this.validateAll(
      'SELECT FORMAT_TIMESTAMP("%b-%d-%Y", TIMESTAMP "2050-12-25 15:30:55+00")',
      {
        write: {
          'bigquery': 'SELECT FORMAT_TIMESTAMP(\'%b-%d-%Y\', CAST(\'2050-12-25 15:30:55+00\' AS TIMESTAMP))',
          'duckdb': 'SELECT STRFTIME(CAST(CAST(\'2050-12-25 15:30:55+00\' AS TIMESTAMPTZ) AS TIMESTAMP), \'%b-%d-%Y\')',
          'snowflake': 'SELECT TO_CHAR(CAST(CAST(\'2050-12-25 15:30:55+00\' AS TIMESTAMPTZ) AS TIMESTAMP), \'mon-DD-yyyy\')',
        },
      },
    );
  }

  testStringAgg () {
    this.validateIdentity('STRING_AGG(a, \' & \')');
    this.validateIdentity('STRING_AGG(DISTINCT a, \' & \')');
    this.validateIdentity('STRING_AGG(a, \' & \' ORDER BY LENGTH(a))');
    this.validateIdentity('STRING_AGG(foo, b\'|\' ORDER BY bar)');
    this.validateIdentity('STRING_AGG(a)');
    this.validateIdentity('STRING_AGG(DISTINCT v, sep LIMIT 3)');
    this.validateIdentity('STRING_AGG(DISTINCT a ORDER BY b DESC, c DESC LIMIT 10)');
    this.validateIdentity(
      'SELECT a, GROUP_CONCAT(b) FROM table GROUP BY a',
      'SELECT a, STRING_AGG(b) FROM table GROUP BY a',
    );
  }

  testAnnotateTimestamps () {
    const sql = `
        SELECT
          CURRENT_TIMESTAMP() AS curr_ts,
          TIMESTAMP_SECONDS(2) AS ts_seconds,
          PARSE_TIMESTAMP('%c', 'Thu Dec 25 07:30:00 2008', 'UTC') AS parsed_ts,
          TIMESTAMP_ADD(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE) AS ts_add,
          TIMESTAMP_SUB(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE) AS ts_sub
        `;

    const annotated = annotateTypes(this.parseOne(sql), { dialect: 'bigquery' }) as any;
    for (const select of annotated.selects) {
      expect(select.type.sql({ dialect: 'bigquery' })).toBe('TIMESTAMP');
    }
  }

  testSetOperations () {
    this.validateIdentity('SELECT 1 AS foo INNER UNION ALL SELECT 3 AS foo, 4 AS bar');

    for (const side of ['', ' LEFT', ' FULL']) {
      for (const kind of ['', ' OUTER']) {
        for (const name of ['', ' BY NAME', ' BY NAME ON (foo, bar)']) {
          this.validateIdentity(
            `SELECT 1 AS foo${side}${kind} UNION ALL${name} SELECT 3 AS foo, 4 AS bar`,
          );
        }
      }
    }

    this.validateIdentity(
      'SELECT 1 AS x UNION ALL CORRESPONDING SELECT 2 AS x',
      'SELECT 1 AS x INNER UNION ALL BY NAME SELECT 2 AS x',
    );
    this.validateIdentity(
      'SELECT 1 AS x UNION ALL CORRESPONDING BY (foo, bar) SELECT 2 AS x',
      'SELECT 1 AS x INNER UNION ALL BY NAME ON (foo, bar) SELECT 2 AS x',
    );
    this.validateIdentity(
      'SELECT 1 AS x LEFT UNION ALL CORRESPONDING SELECT 2 AS x',
      'SELECT 1 AS x LEFT UNION ALL BY NAME SELECT 2 AS x',
    );
    this.validateIdentity(
      'SELECT 1 AS x UNION ALL STRICT CORRESPONDING SELECT 2 AS x',
      'SELECT 1 AS x UNION ALL BY NAME SELECT 2 AS x',
    );
    this.validateIdentity(
      'SELECT 1 AS x UNION ALL STRICT CORRESPONDING BY (foo, bar) SELECT 2 AS x',
      'SELECT 1 AS x UNION ALL BY NAME ON (foo, bar) SELECT 2 AS x',
    );
  }

  testWithOffset () {
    this.validateIdentity(
      'SELECT * FROM UNNEST(x) WITH OFFSET EXCEPT DISTINCT SELECT * FROM UNNEST(y) WITH OFFSET',
      'SELECT * FROM UNNEST(x) WITH OFFSET AS offset EXCEPT DISTINCT SELECT * FROM UNNEST(y) WITH OFFSET AS offset',
    );

    for (const joinOps of ['LEFT', 'RIGHT', 'FULL', 'NATURAL', 'SEMI', 'ANTI']) {
      this.validateIdentity(
        `SELECT * FROM t1, UNNEST([1, 2]) AS hit WITH OFFSET ${joinOps} JOIN foo`,
        `SELECT * FROM t1 CROSS JOIN UNNEST([1, 2]) AS hit WITH OFFSET AS offset ${joinOps} JOIN foo`,
      );
    }
  }

  testIdentifierMeta () {
    const ast = parseOne(
      'SELECT a, b FROM test_schema.test_table_a UNION ALL SELECT c, d FROM test_catalog.test_schema.test_table_b',
      { read: 'bigquery' },
    ) as any;
    for (const identifier of ast.findAll(IdentifierExpr)) {
      expect(new Set(Object.keys(identifier.meta))).toEqual(new Set(['line', 'col', 'start', 'end']));
    }

    expect(ast.args.this.args.from.args.this.args.this.meta).toEqual(
      { line: 1, col: 41, start: 29, end: 40 },
    );
    expect(ast.args.this.args.from.args.this.args.db.meta).toEqual(
      { line: 1, col: 28, start: 17, end: 27 },
    );
    expect(ast.args.expression.args.from.args.this.args.this.meta).toEqual(
      { line: 1, col: 106, start: 94, end: 105 },
    );
    expect(ast.args.expression.args.from.args.this.args.db.meta).toEqual(
      { line: 1, col: 93, start: 82, end: 92 },
    );
    expect(ast.args.expression.args.from.args.this.args.catalog.meta).toEqual(
      { line: 1, col: 81, start: 69, end: 80 },
    );

    const informationSchemaSql = 'SELECT a, b FROM region.INFORMATION_SCHEMA.COLUMNS';
    const ast2 = parseOne(informationSchemaSql, { read: 'bigquery' }) as any;
    const meta = ast2.args.from.args.this.args.this.meta;
    expect(meta).toEqual({ line: 1, col: 50, start: 24, end: 49 });
    expect(informationSchemaSql.slice(meta.start, meta.end + 1)).toBe('INFORMATION_SCHEMA.COLUMNS');
  }

  testQuotedIdentifierMeta () {
    const sql = 'SELECT `a` FROM `test_schema`.`test_table_a`';
    const ast = parseOne(sql, { read: 'bigquery' }) as any;
    const dbMeta = ast.args.from.args.this.args.db.meta;
    expect(sql.slice(dbMeta.start, dbMeta.end + 1)).toBe('`test_schema`');
    const tableMeta = ast.args.from.args.this.args.this.meta;
    expect(sql.slice(tableMeta.start, tableMeta.end + 1)).toBe('`test_table_a`');

    const informationSchemaSql = 'SELECT a, b FROM `region.INFORMATION_SCHEMA.COLUMNS`';
    const ast2 = parseOne(informationSchemaSql, { read: 'bigquery' }) as any;
    const tableMeta2 = ast2.args.from.args.this.args.this.meta;
    expect(informationSchemaSql.slice(tableMeta2.start, tableMeta2.end + 1)).toBe('`region.INFORMATION_SCHEMA.COLUMNS`');
  }

  testArrayAgg () {
    for (const distinct of ['', 'DISTINCT ']) {
      this.validateAll(
        `SELECT ARRAY_AGG(${distinct}x ORDER BY x)`,
        {
          write: {
            'bigquery': `SELECT ARRAY_AGG(${distinct}x ORDER BY x)`,
            'snowflake': `SELECT ARRAY_AGG(${distinct}x) WITHIN GROUP (ORDER BY x NULLS FIRST)`,
          },
        },
      );
    }

    for (const nulls of ['', ' IGNORE NULLS', ' RESPECT NULLS']) {
      this.validateAll(
        `SELECT ARRAY_AGG(x${nulls} ORDER BY col1 ASC, col2 DESC)`,
        {
          write: {
            'bigquery': `SELECT ARRAY_AGG(x${nulls} ORDER BY col1 ASC, col2 DESC)`,
            'snowflake': 'SELECT ARRAY_AGG(x) WITHIN GROUP (ORDER BY col1 ASC NULLS FIRST, col2 DESC NULLS LAST)',
          },
        },
      );
    }
  }

  testArrayConcat () {
    this.validateAll(
      'WITH x AS ( SELECT 1 AS id), test_cte AS ( SELECT ARRAY_CONCAT(( SELECT id FROM x WHERE FALSE)) AS result ) SELECT * FROM test_cte;',
      {
        write: {
          'snowflake': 'WITH x AS (SELECT 1 AS id), test_cte AS (SELECT ARRAY_CAT((SELECT id FROM x WHERE FALSE), []) AS result) SELECT * FROM test_cte',
        },
      },
    );
  }

  testSelectAsStruct () {
    this.validateAll(
      'SELECT ARRAY(SELECT AS STRUCT x1 AS x1, x2 AS x2 FROM t) AS array_col',
      {
        write: {
          'bigquery': 'SELECT ARRAY(SELECT AS STRUCT x1 AS x1, x2 AS x2 FROM t) AS array_col',
          'snowflake': 'SELECT (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(\'x1\', x1, \'x2\', x2)) FROM t) AS array_col',
        },
      },
    );
    this.validateAll(
      'WITH t1 AS (SELECT ARRAY(SELECT AS STRUCT x1 AS alias_x1, x2 /* test */ FROM t2) AS array_col) SELECT array_col[0].alias_x1, array_col[0].x2 FROM t1',
      {
        write: {
          'bigquery': 'WITH t1 AS (SELECT ARRAY(SELECT AS STRUCT x1 AS alias_x1, x2 /* test */ FROM t2) AS array_col) SELECT array_col[0].alias_x1, array_col[0].x2 FROM t1',
          'snowflake': 'WITH t1 AS (SELECT (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(\'alias_x1\', x1, \'x2\', x2 /* test */)) FROM t2) AS array_col) SELECT array_col[0].alias_x1, array_col[0].x2 FROM t1',
        },
      },
    );
    this.validateAll(
      'WITH t1 AS (SELECT ARRAY(SELECT AS STRUCT 1 AS a, 2 AS b) AS array_col) SELECT array_col[0].a, array_col[0].b FROM t1',
      {
        write: {
          'bigquery': 'WITH t1 AS (SELECT ARRAY(SELECT AS STRUCT 1 AS a, 2 AS b) AS array_col) SELECT array_col[0].a, array_col[0].b FROM t1',
          'snowflake': 'WITH t1 AS (SELECT (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(\'a\', 1, \'b\', 2))) AS array_col) SELECT array_col[0].a, array_col[0].b FROM t1',
        },
      },
    );
    this.validateAll(
      'WITH t1 AS (SELECT ARRAY(SELECT AS STRUCT x1 AS alias_x1, x2 /* test */ FROM t2 WHERE x2 = 4) AS array_col) SELECT array_col[0].alias_x1, array_col[0].x2 FROM t1',
      {
        write: {
          'bigquery': 'WITH t1 AS (SELECT ARRAY(SELECT AS STRUCT x1 AS alias_x1, x2 /* test */ FROM t2 WHERE x2 = 4) AS array_col) SELECT array_col[0].alias_x1, array_col[0].x2 FROM t1',
          'snowflake': 'WITH t1 AS (SELECT (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(\'alias_x1\', x1, \'x2\', x2 /* test */)) FROM t2 WHERE x2 = 4) AS array_col) SELECT array_col[0].alias_x1, array_col[0].x2 FROM t1',
        },
      },
    );
  }

  testAvoidGeneratingNestedComment () {
    const sql = `
        select
            id,
            foo,
            -- bar, /* the thing */
        from facts
        `;
    const expected = 'SELECT\n  id,\n  foo\n/* bar, /* the thing * / */\nFROM facts';
    expect(this.parseOne(sql).sql({ dialect: 'bigquery', pretty: true })).toBe(expected);
  }

  testUnnestWithOffset () {
    for (const [offset, alias] of [['', 'offset'], ['AS pos', 'pos']] as const) {
      this.validateAll(
        `SELECT * FROM tbl CROSS JOIN UNNEST(col) AS ref WITH OFFSET ${offset}`,
        {
          write: {
            'bigquery': `SELECT * FROM tbl CROSS JOIN UNNEST(col) AS ref WITH OFFSET AS ${alias}`,
            'hive': `SELECT * FROM tbl LATERAL VIEW POSEXPLODE(col) AS ${alias}, ref`,
            'spark2': `SELECT * FROM tbl LATERAL VIEW POSEXPLODE(col) AS ${alias}, ref`,
            'spark': `SELECT * FROM tbl LATERAL VIEW POSEXPLODE(col) AS ${alias}, ref`,
            'databricks': `SELECT * FROM tbl LATERAL VIEW POSEXPLODE(col) AS ${alias}, ref`,
          },
        },
      );
    }
  }

  testGenerateDateArray () {
    this.validateAll(
      'SELECT GENERATE_DATE_ARRAY(\'2016-10-05\', \'2016-10-08\')',
      {
        write: {
          'bigquery': 'SELECT GENERATE_DATE_ARRAY(\'2016-10-05\', \'2016-10-08\', INTERVAL \'1\' DAY)',
          'duckdb': 'SELECT CAST(GENERATE_SERIES(CAST(\'2016-10-05\' AS DATE), CAST(\'2016-10-08\' AS DATE), INTERVAL \'1\' DAY) AS DATE[])',
        },
      },
    );
    this.validateAll(
      'SELECT GENERATE_DATE_ARRAY(\'2016-10-05\', \'2016-10-08\', INTERVAL \'1\' MONTH)',
      {
        write: {
          'bigquery': 'SELECT GENERATE_DATE_ARRAY(\'2016-10-05\', \'2016-10-08\', INTERVAL \'1\' MONTH)',
          'duckdb': 'SELECT CAST(GENERATE_SERIES(CAST(\'2016-10-05\' AS DATE), CAST(\'2016-10-08\' AS DATE), INTERVAL \'1\' MONTH) AS DATE[])',
        },
      },
    );
    this.validateAll(
      'SELECT id, mnth FROM t CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(start_month, DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL \'1\' MONTH)) AS mnth',
      {
        write: {
          'bigquery': 'SELECT id, mnth FROM t CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(start_month, DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL \'1\' MONTH)) AS mnth',
          'duckdb': 'SELECT id, mnth FROM t CROSS JOIN UNNEST(CAST(GENERATE_SERIES(start_month, DATE_TRUNC(\'MONTH\', CURRENT_DATE), INTERVAL \'1\' MONTH) AS DATE[])) AS _t0(mnth)',
          'snowflake': 'SELECT id, DATEADD(MONTH, CAST(mnth AS INT), CAST(start_month AS DATE)) AS mnth FROM t, LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, (DATEDIFF(MONTH, start_month, DATE_TRUNC(\'MONTH\', CURRENT_DATE)) + 1 - 1) + 1)) AS _t0(seq, key, path, index, mnth, this)',
        },
      },
    );
    this.validateAll(
      'SELECT id, mnth AS a_mnth FROM t CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(start_month, DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL \'1\' MONTH)) AS mnth',
      {
        write: {
          'bigquery': 'SELECT id, mnth AS a_mnth FROM t CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(start_month, DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL \'1\' MONTH)) AS mnth',
          'duckdb': 'SELECT id, mnth AS a_mnth FROM t CROSS JOIN UNNEST(CAST(GENERATE_SERIES(start_month, DATE_TRUNC(\'MONTH\', CURRENT_DATE), INTERVAL \'1\' MONTH) AS DATE[])) AS _t0(mnth)',
          'snowflake': 'SELECT id, DATEADD(MONTH, CAST(mnth AS INT), CAST(start_month AS DATE)) AS a_mnth FROM t, LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, (DATEDIFF(MONTH, start_month, DATE_TRUNC(\'MONTH\', CURRENT_DATE)) + 1 - 1) + 1)) AS _t0(seq, key, path, index, mnth, this)',
        },
      },
    );
    this.validateAll(
      'SELECT id, mnth + 1 AS a_mnth FROM t CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(start_month, DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL \'1\' MONTH)) AS mnth',
      {
        write: {
          'bigquery': 'SELECT id, mnth + 1 AS a_mnth FROM t CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(start_month, DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL \'1\' MONTH)) AS mnth',
          'duckdb': 'SELECT id, mnth + 1 AS a_mnth FROM t CROSS JOIN UNNEST(CAST(GENERATE_SERIES(start_month, DATE_TRUNC(\'MONTH\', CURRENT_DATE), INTERVAL \'1\' MONTH) AS DATE[])) AS _t0(mnth)',
          'snowflake': 'SELECT id, DATEADD(MONTH, CAST(mnth AS INT), CAST(start_month AS DATE)) + 1 AS a_mnth FROM t, LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, (DATEDIFF(MONTH, start_month, DATE_TRUNC(\'MONTH\', CURRENT_DATE)) + 1 - 1) + 1)) AS _t0(seq, key, path, index, mnth, this)',
        },
      },
    );
  }

  testJsonArray () {
    this.validateIdentity('JSON_ARRAY()');
    this.validateIdentity('JSON_ARRAY(10)');
    this.validateIdentity('JSON_ARRAY([])');
    this.validateIdentity('JSON_ARRAY(STRUCT(10 AS a, \'foo\' AS b))');
    this.validateIdentity('JSON_ARRAY(10, [\'foo\', \'bar\'], [20, 30])');
  }

  testDeclare () {
    this.validateIdentity('DECLARE X INT64');
    this.validateIdentity('DECLARE X INT64 DEFAULT 1');
    this.validateIdentity('DECLARE X FLOAT64 DEFAULT 0.9');
    this.validateIdentity('DECLARE X INT64 DEFAULT (SELECT MAX(col) FROM foo)');
    this.validateIdentity('DECLARE X, Y, Z INT64');
    this.validateIdentity('DECLARE X, Y, Z INT64 DEFAULT 42');
    this.validateIdentity('DECLARE X, Y, Z INT64 DEFAULT (SELECT 42)');
    this.validateIdentity('DECLARE START_DATE DATE DEFAULT CURRENT_DATE - 1');
    this.validateIdentity(
      'DECLARE TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP() - INTERVAL \'1\' HOUR',
    );
  }

  testWeek () {
    this.validateIdentity('DATE_TRUNC(date, WEEK(MONDAY))');
    this.validateIdentity(
      'LAST_DAY(DATETIME \'2008-11-10 15:30:00\', WEEK(SUNDAY))',
      'LAST_DAY(CAST(\'2008-11-10 15:30:00\' AS DATETIME), WEEK)',
    );
    this.validateIdentity('DATE_DIFF(\'2017-12-18\', \'2017-12-17\', WEEK(SATURDAY))');
    this.validateIdentity('DATETIME_DIFF(\'2017-12-18\', \'2017-12-17\', WEEK(MONDAY))');
    this.validateIdentity(
      'EXTRACT(WEEK(THURSDAY) FROM DATE \'2013-12-25\')',
      'EXTRACT(WEEK(THURSDAY) FROM CAST(\'2013-12-25\' AS DATE))',
    );

    this.validateAll(
      'SELECT DATE_DIFF(\'2024-06-15\', \'2024-01-08\', WEEK(MONDAY))',
      {
        write: {
          'bigquery': 'SELECT DATE_DIFF(\'2024-06-15\', \'2024-01-08\', WEEK(MONDAY))',
          'duckdb': 'SELECT DATE_DIFF(\'WEEK\', DATE_TRUNC(\'WEEK\', CAST(\'2024-01-08\' AS DATE)), DATE_TRUNC(\'WEEK\', CAST(\'2024-06-15\' AS DATE)))',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_DIFF(\'2026-01-15\', \'2024-01-08\', WEEK(SUNDAY))',
      {
        write: {
          'bigquery': 'SELECT DATE_DIFF(\'2026-01-15\', \'2024-01-08\', WEEK)',
          'duckdb': 'SELECT DATE_DIFF(\'WEEK\', DATE_TRUNC(\'WEEK\', CAST(\'2024-01-08\' AS DATE) + INTERVAL \'1\' DAY), DATE_TRUNC(\'WEEK\', CAST(\'2026-01-15\' AS DATE) + INTERVAL \'1\' DAY))',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_DIFF(\'2024-01-15\', \'2022-04-28\', WEEK(SATURDAY))',
      {
        write: {
          'bigquery': 'SELECT DATE_DIFF(\'2024-01-15\', \'2022-04-28\', WEEK(SATURDAY))',
          'duckdb': 'SELECT DATE_DIFF(\'WEEK\', DATE_TRUNC(\'WEEK\', CAST(\'2022-04-28\' AS DATE) + INTERVAL \'-5\' DAY), DATE_TRUNC(\'WEEK\', CAST(\'2024-01-15\' AS DATE) + INTERVAL \'-5\' DAY))',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_DIFF(\'2024-01-15\', \'2024-01-08\', WEEK)',
      {
        write: {
          'bigquery': 'SELECT DATE_DIFF(\'2024-01-15\', \'2024-01-08\', WEEK)',
          'duckdb': 'SELECT DATE_DIFF(\'WEEK\', DATE_TRUNC(\'WEEK\', CAST(\'2024-01-08\' AS DATE) + INTERVAL \'1\' DAY), DATE_TRUNC(\'WEEK\', CAST(\'2024-01-15\' AS DATE) + INTERVAL \'1\' DAY))',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_DIFF(\'2024-01-07\', \'2024-01-06\', WEEK)',
      {
        write: {
          'bigquery': 'SELECT DATE_DIFF(\'2024-01-07\', \'2024-01-06\', WEEK)',
          'duckdb': 'SELECT DATE_DIFF(\'WEEK\', DATE_TRUNC(\'WEEK\', CAST(\'2024-01-06\' AS DATE) + INTERVAL \'1\' DAY), DATE_TRUNC(\'WEEK\', CAST(\'2024-01-07\' AS DATE) + INTERVAL \'1\' DAY))',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_DIFF(\'2024-01-15\', \'2024-01-08\', ISOWEEK)',
      {
        write: {
          'bigquery': 'SELECT DATE_DIFF(\'2024-01-15\', \'2024-01-08\', ISOWEEK)',
          'duckdb': 'SELECT DATE_DIFF(\'WEEK\', DATE_TRUNC(\'WEEK\', CAST(\'2024-01-08\' AS DATE)), DATE_TRUNC(\'WEEK\', CAST(\'2024-01-15\' AS DATE)))',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_DIFF(DATE \'2024-09-15\', DATE \'2024-01-08\', WEEK(MONDAY))',
      {
        write: {
          'bigquery': 'SELECT DATE_DIFF(CAST(\'2024-09-15\' AS DATE), CAST(\'2024-01-08\' AS DATE), WEEK(MONDAY))',
          'duckdb': 'SELECT DATE_DIFF(\'WEEK\', DATE_TRUNC(\'WEEK\', CAST(\'2024-01-08\' AS DATE)), DATE_TRUNC(\'WEEK\', CAST(\'2024-09-15\' AS DATE)))',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_DIFF(DATE \'2024-01-01\', DATE \'2024-01-15\', WEEK(SUNDAY))',
      {
        write: {
          'bigquery': 'SELECT DATE_DIFF(CAST(\'2024-01-01\' AS DATE), CAST(\'2024-01-15\' AS DATE), WEEK)',
          'duckdb': 'SELECT DATE_DIFF(\'WEEK\', DATE_TRUNC(\'WEEK\', CAST(\'2024-01-15\' AS DATE) + INTERVAL \'1\' DAY), DATE_TRUNC(\'WEEK\', CAST(\'2024-01-01\' AS DATE) + INTERVAL \'1\' DAY))',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_DIFF(DATE \'2023-05-01\', DATE \'2024-01-15\', ISOWEEK)',
      {
        write: {
          'bigquery': 'SELECT DATE_DIFF(CAST(\'2023-05-01\' AS DATE), CAST(\'2024-01-15\' AS DATE), ISOWEEK)',
          'duckdb': 'SELECT DATE_DIFF(\'WEEK\', DATE_TRUNC(\'WEEK\', CAST(\'2024-01-15\' AS DATE)), DATE_TRUNC(\'WEEK\', CAST(\'2023-05-01\' AS DATE)))',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_DIFF(DATE \'2024-01-01\', DATE \'2024-01-15\', DAY)',
      {
        write: {
          'bigquery': 'SELECT DATE_DIFF(CAST(\'2024-01-01\' AS DATE), CAST(\'2024-01-15\' AS DATE), DAY)',
          'duckdb': 'SELECT DATE_DIFF(\'DAY\', CAST(\'2024-01-15\' AS DATE), CAST(\'2024-01-01\' AS DATE))',
        },
      },
    );
  }

  testApproxQuantiles () {
    this.validateIdentity('APPROX_QUANTILES(x, 2)');
    this.validateIdentity('APPROX_QUANTILES(FALSE OR TRUE, 2)');
    this.validateIdentity('APPROX_QUANTILES((SELECT 1 AS val), CAST(2.1 AS INT64))');
    this.validateIdentity('APPROX_QUANTILES(DISTINCT x, 2)');
    this.validateIdentity('APPROX_QUANTILES(x, 2 RESPECT NULLS)');
    this.validateIdentity('APPROX_QUANTILES(x, 2 IGNORE NULLS)');
    this.validateIdentity('APPROX_QUANTILES(DISTINCT x, 2 RESPECT NULLS)');
  }

  testApproxQuantilesToDuckdb () {
    this.validateAll('APPROX_QUANTILES(x, 1)', { write: { 'duckdb': 'APPROX_QUANTILE(x, [0, 1])' } });
    this.validateAll('APPROX_QUANTILES(x, 2)', { write: { 'duckdb': 'APPROX_QUANTILE(x, [0, 0.5, 1])' } });
    this.validateAll('APPROX_QUANTILES(x, 4)', { write: { 'duckdb': 'APPROX_QUANTILE(x, [0, 0.25, 0.5, 0.75, 1])' } });
    this.validateAll('APPROX_QUANTILES(DISTINCT x, 2)', { write: { 'duckdb': 'APPROX_QUANTILE(DISTINCT x, [0, 0.5, 1])' } });

    const result = this.parseOne('APPROX_QUANTILES(x, 100)').sql({ dialect: 'duckdb' });
    expect(result).toContain('APPROX_QUANTILE(');
    expect(result).toContain('0.01');
    expect(result).toContain('0.99');

    for (const expr of ['x + y', 'CASE WHEN x > 0 THEN x ELSE 0 END', 'ABS(x)']) {
      this.validateAll(
        `APPROX_QUANTILES(${expr}, 2)`,
        { write: { 'duckdb': `APPROX_QUANTILE(${expr}, [0, 0.5, 1])` } },
      );
    }

    expect(() => this.parseOne('APPROX_QUANTILES(x, bucket_count)').sql({
      dialect: 'duckdb', unsupportedLevel: ErrorLevel.RAISE,
    })).toThrow(UnsupportedError);

    for (const value of ['0', '-1', '2.5']) {
      expect(() => this.parseOne(`APPROX_QUANTILES(x, ${value})`).sql({
        dialect: 'duckdb', unsupportedLevel: ErrorLevel.RAISE,
      })).toThrow(UnsupportedError);
    }

    expect(() => this.parseOne('APPROX_QUANTILES(x, NULL)').sql({
      dialect: 'duckdb', unsupportedLevel: ErrorLevel.RAISE,
    })).toThrow(UnsupportedError);

    expect(() => this.parseOne('APPROX_QUANTILES(x)').sql({
      dialect: 'duckdb', unsupportedLevel: ErrorLevel.RAISE,
    })).toThrow(UnsupportedError);

    expect(() => this.parseOne('APPROX_QUANTILES(DISTINCT x)').sql({
      dialect: 'duckdb', unsupportedLevel: ErrorLevel.RAISE,
    })).toThrow(UnsupportedError);

    this.validateAll(
      'APPROX_QUANTILES(x, 2 IGNORE NULLS)',
      { write: { 'duckdb': 'APPROX_QUANTILE(x, [0, 0.5, 1])' } },
    );

    expect(() => this.parseOne('APPROX_QUANTILES(x, 2 RESPECT NULLS)').sql({
      dialect: 'duckdb', unsupportedLevel: ErrorLevel.RAISE,
    })).toThrow(UnsupportedError);
  }

  testJsonLax () {
    this.validateIdentity('LAX_BOOL(PARSE_JSON(\'true\'))');
    this.validateIdentity('LAX_FLOAT64(PARSE_JSON(\'9.8\'))');
    this.validateIdentity('LAX_INT64(PARSE_JSON(\'10\'))');
    this.validateIdentity('LAX_STRING(PARSE_JSON(\'"str"\'))');
  }

  testSafeMathFuncs () {
    this.validateIdentity('SAFE_NEGATE(x)');
    this.validateAll(
      'SAFE_ADD(x, y)',
      {
        read: {
          'bigquery': 'SAFE_ADD(x, y)',
          'spark': 'TRY_ADD(x, y)',
          'databricks': 'TRY_ADD(x, y)',
        },
        write: {
          'spark': 'TRY_ADD(x, y)',
          'databricks': 'TRY_ADD(x, y)',
        },
      },
    );
    this.validateAll(
      'SAFE_MULTIPLY(x, y)',
      {
        read: {
          'bigquery': 'SAFE_MULTIPLY(x, y)',
          'spark': 'TRY_MULTIPLY(x, y)',
          'databricks': 'TRY_MULTIPLY(x, y)',
        },
        write: {
          'spark': 'TRY_MULTIPLY(x, y)',
          'databricks': 'TRY_MULTIPLY(x, y)',
        },
      },
    );
    this.validateAll(
      'SAFE_SUBTRACT(x, y)',
      {
        read: {
          'bigquery': 'SAFE_SUBTRACT(x, y)',
          'spark': 'TRY_SUBTRACT(x, y)',
          'databricks': 'TRY_SUBTRACT(x, y)',
        },
        write: {
          'spark': 'TRY_SUBTRACT(x, y)',
          'databricks': 'TRY_SUBTRACT(x, y)',
        },
      },
    );
  }

  testBitwiseAnd () {
    this.validateAll(
      'SELECT 1 & 1',
      { write: { 'bigquery': 'SELECT 1 & 1', 'snowflake': 'SELECT BITAND(1, 1)' } },
    );
  }

  testBitwiseNot () {
    this.validateAll(
      'SELECT ~1',
      { write: { 'bigquery': 'SELECT ~1', 'snowflake': 'SELECT BITNOT(1)' } },
    );
  }

  testBitAggs () {
    this.validateAll(
      'BIT_AND(x)',
      {
        read: { 'bigquery': 'BIT_AND(x)', 'databricks': 'BIT_AND(x)', 'dremio': 'BIT_AND(x)', 'duckdb': 'BIT_AND(x)', 'mysql': 'BIT_AND(x)', 'postgres': 'BIT_AND(x)', 'spark': 'BIT_AND(x)' },
        write: { 'databricks': 'BIT_AND(x)', 'dremio': 'BIT_AND(x)', 'duckdb': 'BIT_AND(x)', 'mysql': 'BIT_AND(x)', 'postgres': 'BIT_AND(x)', 'spark': 'BIT_AND(x)' },
      },
    );
    this.validateAll(
      'BIT_OR(x)',
      {
        read: { 'bigquery': 'BIT_OR(x)', 'databricks': 'BIT_OR(x)', 'dremio': 'BIT_OR(x)', 'duckdb': 'BIT_OR(x)', 'mysql': 'BIT_OR(x)', 'postgres': 'BIT_OR(x)', 'spark': 'BIT_OR(x)' },
        write: { 'databricks': 'BIT_OR(x)', 'dremio': 'BIT_OR(x)', 'duckdb': 'BIT_OR(x)', 'mysql': 'BIT_OR(x)', 'postgres': 'BIT_OR(x)', 'spark': 'BIT_OR(x)' },
      },
    );
    this.validateAll(
      'BIT_XOR(x)',
      {
        read: { 'bigquery': 'BIT_XOR(x)', 'databricks': 'BIT_XOR(x)', 'duckdb': 'BIT_XOR(x)', 'mysql': 'BIT_XOR(x)', 'postgres': 'BIT_XOR(x)', 'spark': 'BIT_XOR(x)' },
        write: { 'databricks': 'BIT_XOR(x)', 'duckdb': 'BIT_XOR(x)', 'mysql': 'BIT_XOR(x)', 'postgres': 'BIT_XOR(x)', 'spark': 'BIT_XOR(x)' },
      },
    );
    this.validateAll(
      'BIT_COUNT(x)',
      {
        read: { 'bigquery': 'BIT_COUNT(x)', 'spark': 'BIT_COUNT(x)', 'databricks': 'BIT_COUNT(x)', 'mysql': 'BIT_COUNT(x)' },
        write: { 'spark': 'BIT_COUNT(x)', 'databricks': 'BIT_COUNT(x)', 'mysql': 'BIT_COUNT(x)' },
      },
    );
  }

  testToHex () {
    this.validateAll(
      'SELECT TO_HEX(SHA1(\'abc\'))',
      {
        write: {
          'bigquery': 'SELECT TO_HEX(SHA1(\'abc\'))',
          'snowflake': 'SELECT TO_CHAR(SHA1_BINARY(\'abc\'))',
        },
      },
    );
  }

  testMd5 () {
    this.validateAll(
      'SELECT MD5(\'abc\')',
      {
        write: {
          'bigquery': 'SELECT MD5(\'abc\')',
          'snowflake': 'SELECT MD5_BINARY(\'abc\')',
        },
      },
    );
  }

  testToJsonString () {
    this.validateAll(
      'SELECT TO_JSON_STRING(STRUCT(\'Alice\' AS name)) AS json_data',
      {
        write: {
          'bigquery': 'SELECT TO_JSON_STRING(STRUCT(\'Alice\' AS name)) AS json_data',
          'snowflake': 'SELECT TO_JSON(OBJECT_CONSTRUCT(\'name\', \'Alice\')) AS json_data',
        },
      },
    );
  }

  testConcat () {
    this.validateAll(
      'SELECT CONCAT(\'T.P.\', \' \', \'Bar\') AS author',
      {
        write: {
          'bigquery': 'SELECT CONCAT(\'T.P.\', \' \', \'Bar\') AS author',
          'duckdb': 'SELECT \'T.P.\' || \' \' || \'Bar\' AS author',
        },
      },
    );
  }

  testPseudocolumns () {
    const schema = {
      't': { 'col': 'INT', 'a': 'TIMESTAMP', 'b': 'TIMESTAMP' },
    };

    const ast = this.validateIdentity('SELECT col FROM t WHERE _PARTITIONTIME BETWEEN a AND b');
    expect((ast as any).find(PseudocolumnExpr)).toBeFalsy();

    const qualified = qualify((ast as any).copy(), { schema, dialect: 'bigquery' }) as any;
    expect(qualified.find(PseudocolumnExpr)).toBeTruthy();
    expect(qualified.sql({ dialect: 'bigquery' })).toBe(
      'SELECT `t`.`col` AS `col` FROM `t` AS `t` WHERE `_partitiontime` BETWEEN `t`.`a` AND `t`.`b`',
    );

    const ast2 = this.validateIdentity('SELECT _DBT_MAX_PARTITION FROM t');
    expect((ast2 as any).find(PseudocolumnExpr)).toBeFalsy();

    const qualified2 = qualify((ast2 as any).copy(), { schema, dialect: 'bigquery' }) as any;
    expect(qualified2.find(PseudocolumnExpr)).toBeTruthy();
  }

  testRound () {
    this.validateAll(
      'SELECT ROUND(2.25) AS value',
      { write: { 'bigquery': 'SELECT ROUND(2.25) AS value', 'duckdb': 'SELECT ROUND(2.25) AS value' } },
    );
    this.validateAll(
      'SELECT ROUND(2.25, 1) AS value',
      { write: { 'bigquery': 'SELECT ROUND(2.25, 1) AS value', 'duckdb': 'SELECT ROUND(2.25, 1) AS value' } },
    );
    this.validateAll(
      'SELECT ROUND(NUMERIC \'2.25\', 1, \'ROUND_HALF_AWAY_FROM_ZERO\') AS value',
      {
        write: {
          'bigquery': 'SELECT ROUND(CAST(\'2.25\' AS NUMERIC), 1, \'ROUND_HALF_AWAY_FROM_ZERO\') AS value',
          'duckdb': 'SELECT ROUND(CAST(\'2.25\' AS DECIMAL), 1) AS value',
        },
      },
    );
    this.validateAll(
      'SELECT ROUND(NUMERIC \'2.25\', 1, \'ROUND_HALF_EVEN\') AS value',
      {
        write: {
          'bigquery': 'SELECT ROUND(CAST(\'2.25\' AS NUMERIC), 1, \'ROUND_HALF_EVEN\') AS value',
          'duckdb': 'SELECT ROUND_EVEN(CAST(\'2.25\' AS DECIMAL), 1) AS value',
        },
      },
    );
  }

  testBignumeric () {
    for (const type of ['BIGNUMERIC', 'BIGDECIMAL']) {
      this.validateAll(
        `SELECT ${type} '1'`,
        {
          write: {
            'bigquery': 'SELECT CAST(\'1\' AS BIGNUMERIC)',
            'duckdb': 'SELECT CAST(\'1\' AS DECIMAL(38, 5))',
          },
        },
      );
      this.validateAll(
        `SELECT CAST(1 AS ${type})`,
        {
          write: {
            'bigquery': 'SELECT CAST(1 AS BIGNUMERIC)',
            'duckdb': 'SELECT CAST(1 AS DECIMAL(38, 5))',
          },
        },
      );
    }
  }

  testConvert () {
    // DateTime without timezone -> DATETIME
    expect(convert(new Date('2023-01-01T00:00:00')).sql({ dialect: this.dialect })).toContain('DATETIME');
  }

  testOverrideNormalizationStrategy () {
    const sql = 'SELECT * FROM p.d.t';
    const ast = this.parseOne(sql);
    const qualified = qualify((ast as any).copy(), { dialect: 'bigquery,normalization_strategy=uppercase' }) as any;
    expect(qualified.sql({ dialect: 'bigquery' })).toBe('SELECT * FROM `P`.`D`.`T` AS `T`');
  }

  testApproxQunatiles () {
    // Note: This mirrors the typo in the upstream Python test name
    this.validateIdentity('APPROX_QUANTILES(foo, 2)');
    this.validateIdentity('APPROX_QUANTILES(DISTINCT foo, 2 RESPECT NULLS)');
    this.validateIdentity('APPROX_QUANTILES(DISTINCT foo, 2 IGNORE NULLS)');
  }
}

const t = new TestBigQuery();

describe('TestBigQuery', () => {
  test('testBigquery', () => t.testBigquery());
  test('testErrors', () => t.testErrors());
  test('testWarnings', () => t.testWarnings());
  test('testUserDefinedFunctions', () => t.testUserDefinedFunctions());
  test('testRemovePrecisionParameterizedTypes', () => t.testRemovePrecisionParameterizedTypes());
  test('testGapFill', () => t.testGapFill());
  test('testModels', () => t.testModels());
  test('testMlFunctions', () => t.testMlFunctions());
  test('testMerge', () => t.testMerge());
  test('testRenameTable', () => t.testRenameTable());
  test('testPushdownCteColumnNames', () => t.testPushdownCteColumnNames());
  test('testJsonObject', () => t.testJsonObject());
  test('testMod', () => t.testMod());
  test('testInlineConstructor', () => t.testInlineConstructor());
  test('testConvert', () => t.testConvert());
  test('testUnnest', () => t.testUnnest());
  test('testRangeType', () => t.testRangeType());
  test('testNullOrdering', () => t.testNullOrdering());
  test('testJsonExtract', () => t.testJsonExtract());
  test('testJsonExtractArray', () => t.testJsonExtractArray());
  test('testUnixSeconds', () => t.testUnixSeconds());
  test('testUnixMicros', () => t.testUnixMicros());
  test('testUnixMillis', () => t.testUnixMillis());
  test('testRegexpExtract', () => t.testRegexpExtract());
  test('testFormatTemporal', () => t.testFormatTemporal());
  test('testStringAgg', () => t.testStringAgg());
  test('testAnnotateTimestamps', () => t.testAnnotateTimestamps());
  test('testSetOperations', () => t.testSetOperations());
  test('testWithOffset', () => t.testWithOffset());
  test('testIdentifierMeta', () => t.testIdentifierMeta());
  test('testQuotedIdentifierMeta', () => t.testQuotedIdentifierMeta());
  test('testOverrideNormalizationStrategy', () => t.testOverrideNormalizationStrategy());
  test('testArrayAgg', () => t.testArrayAgg());
  test('testArrayConcat', () => t.testArrayConcat());
  test('testSelectAsStruct', () => t.testSelectAsStruct());
  test('testAvoidGeneratingNestedComment', () => t.testAvoidGeneratingNestedComment());
  test('testUnnestWithOffset', () => t.testUnnestWithOffset());
  test('testGenerateDateArray', () => t.testGenerateDateArray());
  test('testJsonArray', () => t.testJsonArray());
  test('testDeclare', () => t.testDeclare());
  test('testWeek', () => t.testWeek());
  test('testApproxQunatiles', () => t.testApproxQunatiles());
  test('testApproxQuantiles', () => t.testApproxQuantiles());
  test('testApproxQuantilesToDuckdb', () => t.testApproxQuantilesToDuckdb());
  test('testJsonLax', () => t.testJsonLax());
  test('testSafeMathFuncs', () => t.testSafeMathFuncs());
  test('testBitwiseAnd', () => t.testBitwiseAnd());
  test('testBitwiseNot', () => t.testBitwiseNot());
  test('testBitAggs', () => t.testBitAggs());
  test('testToHex', () => t.testToHex());
  test('testMd5', () => t.testMd5());
  test('testToJsonString', () => t.testToJsonString());
  test('testConcat', () => t.testConcat());
  test('testPseudocolumns', () => t.testPseudocolumns());
  test('testRound', () => t.testRound());
  test('testBignumeric', () => t.testBignumeric());
});
