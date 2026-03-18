import {
  describe, test, expect,
} from 'vitest';
import type { WidthBucketExpr } from '../../../src/expressions';
import {
  Expression,

  UnnestExpr, AlterExpr, ArrayOverlapsExpr, JsonExtractScalarExpr,
  JsonExtractExpr, ObjectIdentifierExpr, DataTypeExpr,
  JsonbContainsAnyTopKeysExpr, JsonbContainsAllTopKeysExpr, JsonbDeleteAtPathExpr,
  TransactionExpr, BinaryExpr, CastExpr, ColumnDefExpr,
} from '../../../src/expressions';
import { narrowInstanceOf } from '../../../src/port_internals';
import {
  Validator, UnsupportedError,
} from './validator';

class TestPostgres extends Validator {
  override dialect = 'postgres' as const;

  testPostgres () {
    const expr = this.parseOne('SELECT * FROM r CROSS JOIN LATERAL UNNEST(ARRAY[1]) AS s(location)');
    const firstJoin = (expr.getArgKey('joins') as Expression[])[0];
    const unnest = (firstJoin.getArgKey('this') as Expression).getArgKey('this');
    expect(unnest).toBeInstanceOf(UnnestExpr);

    const alterTableOnly = 'ALTER TABLE ONLY "Album" ADD CONSTRAINT "FK_AlbumArtistId" FOREIGN KEY ("ArtistId") REFERENCES "Artist" ("ArtistId") ON DELETE NO ACTION ON UPDATE NO ACTION';
    const exprAlter = this.parseOne(alterTableOnly);
    expect(exprAlter).toBeInstanceOf(AlterExpr);
    expect(exprAlter.sql({ dialect: 'postgres' })).toBe(alterTableOnly);

    const sql = 'ARRAY[x' + ',x'.repeat(27) + ']';
    const expectedSql = 'ARRAY[\n  x' + ',\n  x'.repeat(27) + '\n]';
    this.validateIdentity(sql, expectedSql, { pretty: true });

    this.validateIdentity('SELECT GET_BIT(CAST(44 AS BIT(10)), 6)');
    this.validateIdentity('SELECT * FROM t GROUP BY ROLLUP (a || \'^\' || b)');
    this.validateIdentity('SELECT COSH(1.5)');
    this.validateIdentity('SELECT EXP(1)');
    this.validateIdentity('SELECT MODE() WITHIN GROUP (ORDER BY status DESC) AS most_common FROM orders');
    this.validateIdentity('SELECT ST_DISTANCE(gg1, gg2, FALSE) AS sphere_dist');
    this.validateIdentity('SHA384(x)');
    this.validateIdentity('1.x', '1. AS x');
    this.validateIdentity('|/ x', 'SQRT(x)');
    this.validateIdentity('||/ x', 'CBRT(x)');
    this.validateIdentity('SELECT EXTRACT(QUARTER FROM CAST(\'2025-04-26\' AS DATE))');
    this.validateIdentity('SELECT DATE_TRUNC(\'QUARTER\', CAST(\'2025-04-26\' AS DATE))');
    this.validateIdentity('STRING_TO_ARRAY(\'xx~^~yy~^~zz\', \'~^~\', \'yy\')');
    this.validateIdentity('SELECT x FROM t WHERE CAST($1 AS TEXT) = \'ok\'');
    this.validateIdentity('SELECT * FROM t TABLESAMPLE SYSTEM (50) REPEATABLE (55)');
    this.validateIdentity('x @@ y');
    this.validateIdentity('CAST(x AS MONEY)');
    this.validateIdentity('CAST(x AS INT4RANGE)');
    this.validateIdentity('CAST(x AS INT4MULTIRANGE)');
    this.validateIdentity('CAST(x AS INT8RANGE)');
    this.validateIdentity('CAST(x AS INT8MULTIRANGE)');
    this.validateIdentity('CAST(x AS NUMRANGE)');
    this.validateIdentity('CAST(x AS NUMMULTIRANGE)');
    this.validateIdentity('CAST(x AS TSRANGE)');
    this.validateIdentity('CAST(x AS TSMULTIRANGE)');
    this.validateIdentity('CAST(x AS TSTZRANGE)');
    this.validateIdentity('CAST(x AS TSTZMULTIRANGE)');
    this.validateIdentity('CAST(x AS DATERANGE)');
    this.validateIdentity('CAST(x AS DATEMULTIRANGE)');
    this.validateIdentity('x$');
    this.validateIdentity('LENGTH(x)');
    this.validateIdentity('LENGTH(x, utf8)');
    this.validateIdentity('CHAR_LENGTH(x)', 'LENGTH(x)');
    this.validateIdentity('CHARACTER_LENGTH(x)', 'LENGTH(x)');
    this.validateIdentity('SELECT ARRAY[1, 2, 3]');
    this.validateIdentity('SELECT ARRAY(SELECT 1)');
    this.validateIdentity('STRING_AGG(x, y)');
    this.validateIdentity('STRING_AGG(x, \',\' ORDER BY y)');
    this.validateIdentity('STRING_AGG(x, \',\' ORDER BY y DESC)');
    this.validateIdentity('STRING_AGG(DISTINCT x, \',\' ORDER BY y DESC)');
    this.validateIdentity('SELECT CASE WHEN SUBSTRING(\'abcdefg\') IN (\'ab\') THEN 1 ELSE 0 END');
    this.validateIdentity('COMMENT ON TABLE mytable IS \'this\'');
    this.validateIdentity('COMMENT ON MATERIALIZED VIEW my_view IS \'this\'');
    this.validateIdentity('SELECT e\'\\xDEADBEEF\'');
    this.validateIdentity('SELECT CAST(e\'\\176\' AS BYTEA)');
    this.validateIdentity('SELECT * FROM x WHERE SUBSTRING(\'Thomas\' FROM \'...$\') IN (\'mas\')');
    this.validateIdentity('SELECT TRIM(\' X\' FROM \' XXX \')');
    this.validateIdentity('SELECT TRIM(LEADING \'bla\' FROM \' XXX \' COLLATE utf8_bin)');
    this.validateIdentity('SELECT * FROM JSON_TO_RECORDSET(z) AS y("rank" INT)');
    this.validateIdentity('SELECT ~x');
    this.validateIdentity('x ~ \'y\'');
    this.validateIdentity('x ~* \'y\'');
    this.validateIdentity('SELECT * FROM r CROSS JOIN LATERAL UNNEST(ARRAY[1]) AS s(location)');
    this.validateIdentity('CAST(1 AS DECIMAL) / CAST(2 AS DECIMAL) * -100');
    this.validateIdentity('EXEC AS myfunc @id = 123', undefined, { checkCommandWarning: true });
    this.validateIdentity('SELECT CURRENT_SCHEMA');
    this.validateIdentity('SELECT CURRENT_USER');
    this.validateIdentity('SELECT CURRENT_ROLE');
    this.validateIdentity('SELECT VERSION()');
    this.validateIdentity('SELECT * FROM ONLY t1');
    this.validateIdentity('SELECT INTERVAL \'-1 MONTH\'');
    this.validateIdentity('SELECT INTERVAL \'4.1 DAY\'');
    this.validateIdentity('SELECT INTERVAL \'3.14159 HOUR\'');
    this.validateIdentity('SELECT INTERVAL \'2.5 MONTH\'');
    this.validateIdentity('SELECT INTERVAL \'-10.75 MINUTE\'');
    this.validateIdentity('SELECT INTERVAL \'0.123456789 SECOND\'');
    this.validateIdentity(
      'SELECT SUM(x) OVER (PARTITION BY y ORDER BY interval ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - SUM(x) OVER (PARTITION BY y ORDER BY interval ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total',
    );
    this.validateIdentity(
      'SELECT * FROM test_data, LATERAL JSONB_ARRAY_ELEMENTS(data) WITH ORDINALITY AS elem(value, ordinality)',
    );
    this.validateIdentity(
      'SELECT id, name FROM xml_data AS t, XMLTABLE(\'/root/user\' PASSING t.xml COLUMNS id INT PATH \'@id\', name TEXT PATH \'name/text()\') AS x',
    );
    this.validateIdentity(
      'SELECT id, value FROM xml_content AS t, XMLTABLE(XMLNAMESPACES(\'http://example.com/ns1\' AS ns1, \'http://example.com/ns2\' AS ns2), \'/root/data\' PASSING t.xml COLUMNS id INT PATH \'@ns1:id\', value TEXT PATH \'ns2:value/text()\') AS x',
    );
    this.validateIdentity(
      'SELECT * FROM t WHERE some_column >= CURRENT_DATE + INTERVAL \'1 day 1 hour\' AND some_another_column IS TRUE',
    );
    this.validateIdentity(
      'UPDATE "x" SET "y" = CAST(\'0 days 60.000000 seconds\' AS INTERVAL) WHERE "x"."id" IN (2, 3)',
    );
    this.validateIdentity(
      'WITH t1 AS MATERIALIZED (SELECT 1), t2 AS NOT MATERIALIZED (SELECT 2) SELECT * FROM t1, t2',
    );
    this.validateIdentity(
      'LAST_VALUE("col1") OVER (ORDER BY "col2" RANGE BETWEEN INTERVAL \'1 DAY\' PRECEDING AND \'1 month\' FOLLOWING)',
    );
    this.validateIdentity(
      'ALTER TABLE ONLY "Album" ADD CONSTRAINT "FK_AlbumArtistId" FOREIGN KEY ("ArtistId") REFERENCES "Artist" ("ArtistId") ON DELETE CASCADE',
    );
    this.validateIdentity(
      'ALTER TABLE ONLY "Album" ADD CONSTRAINT "FK_AlbumArtistId" FOREIGN KEY ("ArtistId") REFERENCES "Artist" ("ArtistId") ON DELETE RESTRICT',
    );
    this.validateIdentity(
      'SELECT * FROM JSON_ARRAY_ELEMENTS(\'[1,true, [2,false]]\') WITH ORDINALITY',
    );
    this.validateIdentity(
      'SELECT * FROM JSON_ARRAY_ELEMENTS(\'[1,true, [2,false]]\') WITH ORDINALITY AS kv_json',
    );
    this.validateIdentity(
      'SELECT * FROM JSON_ARRAY_ELEMENTS(\'[1,true, [2,false]]\') WITH ORDINALITY AS kv_json(a, b)',
    );
    this.validateIdentity(
      'SELECT SUM(x) OVER a, SUM(y) OVER b FROM c WINDOW a AS (PARTITION BY d), b AS (PARTITION BY e)',
    );
    this.validateIdentity(
      'SELECT CASE WHEN SUBSTRING(\'abcdefg\' FROM 1) IN (\'ab\') THEN 1 ELSE 0 END',
    );
    this.validateIdentity(
      'SELECT CASE WHEN SUBSTRING(\'abcdefg\' FROM 1 FOR 2) IN (\'ab\') THEN 1 ELSE 0 END',
    );
    this.validateIdentity(
      'SELECT * FROM "x" WHERE SUBSTRING("x"."foo" FROM 1 FOR 2) IN (\'mas\')',
    );
    this.validateIdentity(
      'SELECT * FROM x WHERE SUBSTRING(\'Thomas\' FROM \'%#"o_a#"_\' FOR \'#\') IN (\'mas\')',
    );
    this.validateIdentity(
      'SELECT SUBSTRING(\'bla\' + \'foo\' || \'bar\' FROM 3 - 1 + 5 FOR 4 + SOME_FUNC(arg1, arg2))',
    );
    this.validateIdentity(
      'SELECT TO_TIMESTAMP(1284352323.5), TO_TIMESTAMP(\'05 Dec 2000\', \'DD Mon YYYY\')',
    );
    this.validateIdentity(
      'SELECT TO_TIMESTAMP(\'05 Dec 2000 10:00 AM\', \'DD Mon YYYY HH:MI AM\')',
    );
    this.validateIdentity(
      'SELECT TO_TIMESTAMP(\'05 Dec 2000 10:00 PM\', \'DD Mon YYYY HH:MI PM\')',
    );
    this.validateIdentity(
      'SELECT * FROM foo, LATERAL (SELECT * FROM bar WHERE bar.id = foo.bar_id) AS ss',
    );
    this.validateIdentity(
      'SELECT c.oid, n.nspname, c.relname '
      + 'FROM pg_catalog.pg_class AS c '
      + 'LEFT JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace '
      + 'WHERE c.relname OPERATOR(pg_catalog.~) \'^(courses)$\' COLLATE pg_catalog.default AND '
      + 'pg_catalog.PG_TABLE_IS_VISIBLE(c.oid) '
      + 'ORDER BY 2, 3',
    );
    this.validateIdentity(
      'SELECT e\'foo \\\' bar\'',
      'SELECT e\'foo \'\' bar\'',
    );
    this.validateIdentity('SELECT e\'\\n\'');
    this.validateIdentity('SELECT e\'\\t\'');
    this.validateIdentity(
      'SELECT e\'update table_name set a = \\\'foo\\\' where 1 = 0\' AS x FROM tab',
      'SELECT e\'update table_name set a = \'\'foo\'\' where 1 = 0\' AS x FROM tab',
    );
    this.validateIdentity(
      'select count() OVER(partition by a order by a range offset preceding exclude current row)',
      'SELECT COUNT() OVER (PARTITION BY a ORDER BY a range BETWEEN offset preceding AND CURRENT ROW EXCLUDE CURRENT ROW)',
    );
    narrowInstanceOf(this.validateIdentity(
      String.raw`x::JSON -> 'duration' ->> -1`,
      String.raw`JSON_EXTRACT_PATH_TEXT(CAST(x AS JSON) -> 'duration', -1)`,
    ).assertIs(JsonExtractScalarExpr)?.args.this, Expression)?.assertIs(JsonExtractExpr);
    this.validateIdentity(
      'SELECT SUBSTRING(\'Thomas\' FOR 3 FROM 2)',
      'SELECT SUBSTRING(\'Thomas\' FROM 2 FOR 3)',
    );
    this.validateIdentity(
      'SELECT ARRAY[1, 2, 3] <@ ARRAY[1, 2]',
      'SELECT ARRAY[1, 2] @> ARRAY[1, 2, 3]',
    );
    this.validateIdentity(
      'SELECT DATE_PART(\'isodow\'::varchar(6), current_date)',
      'SELECT EXTRACT(CAST(\'isodow\' AS VARCHAR(6)) FROM CURRENT_DATE)',
    );
    this.validateIdentity(
      'END WORK AND NO CHAIN',
      'COMMIT AND NO CHAIN',
    );
    this.validateIdentity(
      'END AND CHAIN',
      'COMMIT AND CHAIN',
    );
    this.validateIdentity(
      'x ? \'x\'',
      'x ? \'x\'',
    );
    this.validateIdentity(
      'SELECT $$a$$',
      'SELECT \'a\'',
    );
    this.validateIdentity(
      'SELECT $$Dianne\'s horse$$',
      'SELECT \'Dianne\'\'s horse\'',
    );
    this.validateIdentity(
      'SELECT $$The price is $9.95$$ AS msg',
      'SELECT \'The price is $9.95\' AS msg',
    );
    this.validateIdentity('COMMENT ON TABLE mytable IS $$doc this$$', 'COMMENT ON TABLE mytable IS \'doc this\'');
    this.validateIdentity(
      'UPDATE MYTABLE T1 SET T1.COL = 13',
      'UPDATE MYTABLE AS T1 SET T1.COL = 13',
    );
    this.validateIdentity(
      'x !~ \'y\'',
      'NOT x ~ \'y\'',
    );
    this.validateIdentity(
      'x !~* \'y\'',
      'NOT x ~* \'y\'',
    );
    this.validateIdentity(
      'x ~~ \'y\'',
      'x LIKE \'y\'',
    );
    this.validateIdentity(
      'x ~~* \'y\'',
      'x ILIKE \'y\'',
    );
    this.validateIdentity(
      'x !~~ \'y\'',
      'NOT x LIKE \'y\'',
    );
    this.validateIdentity(
      'x !~~* \'y\'',
      'NOT x ILIKE \'y\'',
    );
    this.validateIdentity(
      '\'45 days\'::interval day',
      'CAST(\'45 days\' AS INTERVAL DAY)',
    );
    this.validateIdentity(
      '\'x\' \'y\' \'z\'',
      'CONCAT(\'x\', \'y\', \'z\')',
    );
    this.validateIdentity(
      'x::cstring',
      'CAST(x AS CSTRING)',
    );
    this.validateIdentity(
      'x::oid',
      'CAST(x AS OID)',
    );
    this.validateIdentity(
      'x::regclass',
      'CAST(x AS REGCLASS)',
    );
    this.validateIdentity(
      'x::regcollation',
      'CAST(x AS REGCOLLATION)',
    );
    this.validateIdentity(
      'x::regconfig',
      'CAST(x AS REGCONFIG)',
    );
    this.validateIdentity(
      'x::regdictionary',
      'CAST(x AS REGDICTIONARY)',
    );
    this.validateIdentity(
      'x::regnamespace',
      'CAST(x AS REGNAMESPACE)',
    );
    this.validateIdentity(
      'x::regoper',
      'CAST(x AS REGOPER)',
    );
    this.validateIdentity(
      'x::regoperator',
      'CAST(x AS REGOPERATOR)',
    );
    this.validateIdentity(
      'x::regproc',
      'CAST(x AS REGPROC)',
    );
    this.validateIdentity(
      'x::regprocedure',
      'CAST(x AS REGPROCEDURE)',
    );
    this.validateIdentity(
      'x::regrole',
      'CAST(x AS REGROLE)',
    );
    this.validateIdentity(
      'x::regtype',
      'CAST(x AS REGTYPE)',
    );
    this.validateIdentity(
      '123::CHARACTER VARYING',
      'CAST(123 AS VARCHAR)',
    );
    this.validateIdentity(
      'TO_TIMESTAMP(123::DOUBLE PRECISION)',
      'TO_TIMESTAMP(CAST(123 AS DOUBLE PRECISION))',
    );
    this.validateIdentity(
      'SELECT to_timestamp(123)::time without time zone',
      'SELECT CAST(TO_TIMESTAMP(123) AS TIME)',
    );
    this.validateIdentity(
      'SELECT SUM(x) OVER (PARTITION BY a ORDER BY d ROWS 1 PRECEDING)',
      'SELECT SUM(x) OVER (PARTITION BY a ORDER BY d ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)',
    );
    this.validateIdentity(
      'SELECT SUBSTRING(2022::CHAR(4) || LPAD(3::CHAR(2), 2, \'0\') FROM 3 FOR 4)',
      'SELECT SUBSTRING(CAST(2022 AS CHAR(4)) || LPAD(CAST(3 AS CHAR(2)), 2, \'0\') FROM 3 FOR 4)',
    );
    this.validateIdentity(
      'SELECT m.name FROM manufacturers AS m LEFT JOIN LATERAL GET_PRODUCT_NAMES(m.id) pname ON TRUE WHERE pname IS NULL',
      'SELECT m.name FROM manufacturers AS m LEFT JOIN LATERAL GET_PRODUCT_NAMES(m.id) AS pname ON TRUE WHERE pname IS NULL',
    );
    this.validateIdentity(
      'SELECT p1.id, p2.id, v1, v2 FROM polygons AS p1, polygons AS p2, LATERAL VERTICES(p1.poly) v1, LATERAL VERTICES(p2.poly) v2 WHERE (v1 <-> v2) < 10 AND p1.id <> p2.id',
      'SELECT p1.id, p2.id, v1, v2 FROM polygons AS p1, polygons AS p2, LATERAL VERTICES(p1.poly) AS v1, LATERAL VERTICES(p2.poly) AS v2 WHERE (v1 <-> v2) < 10 AND p1.id <> p2.id',
    );
    this.validateIdentity(
      'SELECT id, email, CAST(deleted AS TEXT) FROM users WHERE deleted NOTNULL',
      'SELECT id, email, CAST(deleted AS TEXT) FROM users WHERE NOT deleted IS NULL',
    );
    this.validateIdentity(
      'SELECT id, email, CAST(deleted AS TEXT) FROM users WHERE NOT deleted ISNULL',
      'SELECT id, email, CAST(deleted AS TEXT) FROM users WHERE NOT deleted IS NULL',
    );
    this.validateIdentity(
      '\'{"x": {"y": 1}}\'::json->\'x\'->\'y\'',
      'CAST(\'{"x": {"y": 1}}\' AS JSON) -> \'x\' -> \'y\'',
    );
    this.validateIdentity(
      '\'[1,2,3]\'::json->>2',
      'CAST(\'[1,2,3]\' AS JSON) ->> 2',
    );
    this.validateIdentity(
      '\'{"a":1,"b":2}\'::json->>\'b\'',
      'CAST(\'{"a":1,"b":2}\' AS JSON) ->> \'b\'',
    );
    this.validateIdentity(
      '\'{"a":[1,2,3],"b":[4,5,6]}\'::json#>\'{a,2}\'',
      'CAST(\'{"a":[1,2,3],"b":[4,5,6]}\' AS JSON) #> \'{a,2}\'',
    );
    this.validateIdentity(
      '\'{"a":[1,2,3],"b":[4,5,6]}\'::json#>>\'{a,2}\'',
      'CAST(\'{"a":[1,2,3],"b":[4,5,6]}\' AS JSON) #>> \'{a,2}\'',
    );
    this.validateIdentity(
      '\'[1,2,3]\'::json->2',
      'CAST(\'[1,2,3]\' AS JSON) -> 2',
    );
    this.validateIdentity(
      'SELECT JSON_ARRAY_ELEMENTS((foo->\'sections\')::JSON) AS sections',
      'SELECT JSON_ARRAY_ELEMENTS(CAST((foo -> \'sections\') AS JSON)) AS sections',
    );
    this.validateIdentity(
      'MERGE INTO x USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET x.a = y.b WHEN NOT MATCHED THEN INSERT (a, b) VALUES (y.a, y.b)',
      'MERGE INTO x USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET a = y.b WHEN NOT MATCHED THEN INSERT (a, b) VALUES (y.a, y.b)',
    );
    this.validateIdentity(
      'SELECT * FROM t1*',
      'SELECT * FROM t1',
    );
    this.validateIdentity(
      'SELECT SUBSTRING(\'afafa\' for 1)',
      'SELECT SUBSTRING(\'afafa\' FROM 1 FOR 1)',
    );
    this.validateIdentity(
      'CAST(x AS INT8)',
      'CAST(x AS BIGINT)',
    );
    this.validateIdentity(
      `
            WITH
              json_data AS (SELECT '{"field_id": [1, 2, 3]}'::JSON AS data),
              field_ids AS (SELECT 'field_id' AS field_id)

            SELECT
                JSON_ARRAY_ELEMENTS(json_data.data -> field_ids.field_id) AS element
            FROM json_data, field_ids
            `,
      `WITH json_data AS (
  SELECT
    CAST('{"field_id": [1, 2, 3]}' AS JSON) AS data
), field_ids AS (
  SELECT
    'field_id' AS field_id
)
SELECT
  JSON_ARRAY_ELEMENTS(JSON_EXTRACT_PATH(json_data.data, field_ids.field_id)) AS element
FROM json_data, field_ids`,
      { pretty: true },
    );

    this.validateAll(
      'x ? y',
      {
        write: {
          '': 'JSONB_CONTAINS(x, y)',
          'postgres': 'x ? y',
        },
      },
    );
    this.validateAll(
      'SELECT CURRENT_TIMESTAMP + INTERVAL \'-3 MONTH\'',
      {
        read: {
          mysql: 'SELECT DATE_ADD(CURRENT_TIMESTAMP, INTERVAL -1 QUARTER)',
          postgres: 'SELECT CURRENT_TIMESTAMP + INTERVAL \'-3 MONTH\'',
          tsql: 'SELECT DATEADD(QUARTER, -1, GETDATE())',
        },
      },
    );
    this.validateAll(
      'SELECT ARRAY[]::INT[] AS foo',
      {
        write: {
          postgres: 'SELECT CAST(ARRAY[] AS INT[]) AS foo',
          duckdb: 'SELECT CAST([] AS INT[]) AS foo',
        },
      },
    );
    this.validateAll(
      'STRING_TO_ARRAY(\'xx~^~yy~^~zz\', \'~^~\', \'yy\')',
      {
        read: {
          doris: 'SPLIT_BY_STRING(\'xx~^~yy~^~zz\', \'~^~\', \'yy\')',
        },
        write: {
          doris: 'SPLIT_BY_STRING(\'xx~^~yy~^~zz\', \'~^~\', \'yy\')',
          postgres: 'STRING_TO_ARRAY(\'xx~^~yy~^~zz\', \'~^~\', \'yy\')',
        },
      },
    );
    this.validateAll(
      'SELECT ARRAY[1, 2, 3] @> ARRAY[1, 2]',
      {
        read: {
          duckdb: 'SELECT [1, 2, 3] @> [1, 2]',
        },
        write: {
          duckdb: 'SELECT [1, 2, 3] @> [1, 2]',
          mysql: UnsupportedError,
          postgres: 'SELECT ARRAY[1, 2, 3] @> ARRAY[1, 2]',
        },
      },
    );
    this.validateAll(
      'SELECT REGEXP_REPLACE(\'mr .\', \'[^a-zA-Z]\', \'\', \'g\')',
      {
        write: {
          duckdb: 'SELECT REGEXP_REPLACE(\'mr .\', \'[^a-zA-Z]\', \'\', \'g\')',
          postgres: 'SELECT REGEXP_REPLACE(\'mr .\', \'[^a-zA-Z]\', \'\', \'g\')',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE t (c INT)',
      {
        read: {
          mysql: 'CREATE TABLE t (c INT COMMENT \'comment 1\') COMMENT = \'comment 2\'',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM "test_table" ORDER BY RANDOM() LIMIT 5',
      {
        write: {
          bigquery: 'SELECT * FROM `test_table` ORDER BY RAND() NULLS LAST LIMIT 5',
          duckdb: 'SELECT * FROM "test_table" ORDER BY RANDOM() LIMIT 5',
          postgres: 'SELECT * FROM "test_table" ORDER BY RANDOM() LIMIT 5',
          tsql: 'SELECT TOP 5 * FROM [test_table] ORDER BY RAND()',
        },
      },
    );
    this.validateAll(
      'SELECT (data -> \'en-US\') AS acat FROM my_table',
      {
        write: {
          duckdb: 'SELECT (data -> \'$."en-US"\') AS acat FROM my_table',
          postgres: 'SELECT (data -> \'en-US\') AS acat FROM my_table',
        },
      },
    );
    this.validateAll(
      'SELECT (data ->> \'en-US\') AS acat FROM my_table',
      {
        write: {
          duckdb: 'SELECT (data ->> \'$."en-US"\') AS acat FROM my_table',
          postgres: 'SELECT (data ->> \'en-US\') AS acat FROM my_table',
        },
      },
    );
    this.validateAll(
      'SELECT JSON_EXTRACT_PATH_TEXT(x, k1, k2, k3) FROM t',
      {
        read: {
          clickhouse: 'SELECT JSONExtractString(x, k1, k2, k3) FROM t',
          redshift: 'SELECT JSON_EXTRACT_PATH_TEXT(x, k1, k2, k3) FROM t',
        },
        write: {
          clickhouse: 'SELECT JSONExtractString(x, k1, k2, k3) FROM t',
          postgres: 'SELECT JSON_EXTRACT_PATH_TEXT(x, k1, k2, k3) FROM t',
          redshift: 'SELECT JSON_EXTRACT_PATH_TEXT(x, k1, k2, k3) FROM t',
        },
      },
    );
    this.validateAll(
      'x #> \'y\'',
      {
        read: {
          '': 'JSONB_EXTRACT(x, \'y\')',
        },
        write: {
          '': 'JSONB_EXTRACT(x, \'y\')',
          'postgres': 'x #> \'y\'',
        },
      },
    );
    this.validateAll(
      'x #>> \'y\'',
      {
        read: {
          '': 'JSONB_EXTRACT_SCALAR(x, \'y\')',
        },
        write: {
          '': 'JSONB_EXTRACT_SCALAR(x, \'y\')',
          'postgres': 'x #>> \'y\'',
        },
      },
    );
    this.validateAll(
      'x -> \'y\' -> 0 -> \'z\'',
      {
        write: {
          '': 'JSON_EXTRACT(JSON_EXTRACT(JSON_EXTRACT(x, \'$.y\'), \'$[0]\'), \'$.z\')',
          'postgres': 'x -> \'y\' -> 0 -> \'z\'',
        },
      },
    );
    this.validateAll(
      'JSON_EXTRACT_PATH(\'{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}\',\'f4\')',
      {
        write: {
          '': 'JSON_EXTRACT(\'{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}\', \'$.f4\')',
          'bigquery': 'JSON_EXTRACT(\'{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}\', \'$.f4\')',
          'duckdb': '\'{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}\' -> \'$.f4\'',
          'mysql': 'JSON_EXTRACT(\'{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}\', \'$.f4\')',
          'postgres': 'JSON_EXTRACT_PATH(\'{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}\', \'f4\')',
          'presto': 'JSON_EXTRACT(\'{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}\', \'$.f4\')',
          'redshift': 'JSON_EXTRACT_PATH_TEXT(\'{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}\', \'f4\')',
          'spark': 'GET_JSON_OBJECT(\'{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}\', \'$.f4\')',
          'sqlite': '\'{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}\' -> \'$.f4\'',
          'tsql': 'ISNULL(JSON_QUERY(\'{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}\', \'$.f4\'), JSON_VALUE(\'{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}\', \'$.f4\'))',
        },
      },
    );
    this.validateAll(
      'JSON_EXTRACT_PATH_TEXT(\'{"farm": ["a", "b", "c"]}\', \'farm\', \'0\')',
      {
        read: {
          duckdb: '\'{"farm": ["a", "b", "c"]}\' ->> \'$.farm[0]\'',
          redshift: 'JSON_EXTRACT_PATH_TEXT(\'{"farm": ["a", "b", "c"]}\', \'farm\', \'0\')',
        },
        write: {
          duckdb: '\'{"farm": ["a", "b", "c"]}\' ->> \'$.farm[0]\'',
          postgres: 'JSON_EXTRACT_PATH_TEXT(\'{"farm": ["a", "b", "c"]}\', \'farm\', \'0\')',
          redshift: 'JSON_EXTRACT_PATH_TEXT(\'{"farm": ["a", "b", "c"]}\', \'farm\', \'0\')',
        },
      },
    );
    this.validateAll(
      'JSON_EXTRACT_PATH(x, \'x\', \'y\', \'z\')',
      {
        read: {
          duckdb: 'x -> \'$.x.y.z\'',
          postgres: 'JSON_EXTRACT_PATH(x, \'x\', \'y\', \'z\')',
        },
        write: {
          duckdb: 'x -> \'$.x.y.z\'',
          redshift: 'JSON_EXTRACT_PATH_TEXT(x, \'x\', \'y\', \'z\')',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM t TABLESAMPLE SYSTEM (50)',
      {
        write: {
          postgres: 'SELECT * FROM t TABLESAMPLE SYSTEM (50)',
          redshift: UnsupportedError,
        },
      },
    );
    this.validateAll(
      'SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount)',
      {
        write: {
          databricks: 'SELECT PERCENTILE_APPROX(amount, 0.5)',
          postgres: 'SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount)',
          presto: 'SELECT APPROX_PERCENTILE(amount, 0.5)',
          spark: 'SELECT PERCENTILE_APPROX(amount, 0.5)',
          trino: 'SELECT APPROX_PERCENTILE(amount, 0.5)',
        },
      },
    );
    this.validateAll(
      'e\'x\'',
      {
        write: {
          mysql: 'x',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_PART(\'minute\', timestamp \'2023-01-04 04:05:06.789\')',
      {
        write: {
          postgres: 'SELECT EXTRACT(minute FROM CAST(\'2023-01-04 04:05:06.789\' AS TIMESTAMP))',
          redshift: 'SELECT EXTRACT(minute FROM CAST(\'2023-01-04 04:05:06.789\' AS TIMESTAMP))',
          snowflake: 'SELECT DATE_PART(minute, CAST(\'2023-01-04 04:05:06.789\' AS TIMESTAMP))',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_PART(\'month\', date \'20220502\')',
      {
        write: {
          postgres: 'SELECT EXTRACT(month FROM CAST(\'20220502\' AS DATE))',
          redshift: 'SELECT EXTRACT(month FROM CAST(\'20220502\' AS DATE))',
          snowflake: 'SELECT DATE_PART(month, CAST(\'20220502\' AS DATE))',
        },
      },
    );
    this.validateAll(
      'SELECT (DATE \'2016-01-10\', DATE \'2016-02-01\') OVERLAPS (DATE \'2016-01-20\', DATE \'2016-02-10\')',
      {
        write: {
          postgres: 'SELECT (CAST(\'2016-01-10\' AS DATE), CAST(\'2016-02-01\' AS DATE)) OVERLAPS (CAST(\'2016-01-20\' AS DATE), CAST(\'2016-02-10\' AS DATE))',
          tsql: 'SELECT (CAST(\'2016-01-10\' AS DATE), CAST(\'2016-02-01\' AS DATE)) OVERLAPS (CAST(\'2016-01-20\' AS DATE), CAST(\'2016-02-10\' AS DATE))',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_PART(\'epoch\', CAST(\'2023-01-04 04:05:06.789\' AS TIMESTAMP))',
      {
        read: {
          '': 'SELECT TIME_TO_UNIX(TIMESTAMP \'2023-01-04 04:05:06.789\')',
        },
      },
    );
    this.validateAll(
      'x ^ y',
      {
        write: {
          '': 'POWER(x, y)',
          'postgres': 'POWER(x, y)',
        },
      },
    );
    this.validateAll(
      'x # y',
      {
        write: {
          '': 'x ^ y',
          'postgres': 'x # y',
        },
      },
    );
    this.validateAll(
      'SELECT GENERATE_SERIES(1, 5)',
      {
        write: {
          bigquery: UnsupportedError,
          postgres: 'SELECT GENERATE_SERIES(1, 5)',
        },
      },
    );
    this.validateAll(
      'WITH dates AS (SELECT GENERATE_SERIES(\'2020-01-01\'::DATE, \'2024-01-01\'::DATE, \'1 day\'::INTERVAL) AS date), date_table AS (SELECT DISTINCT DATE_TRUNC(\'MONTH\', date) AS date FROM dates) SELECT * FROM date_table',
      {
        write: {
          duckdb: 'WITH dates AS (SELECT UNNEST(GENERATE_SERIES(CAST(\'2020-01-01\' AS DATE), CAST(\'2024-01-01\' AS DATE), CAST(\'1 day\' AS INTERVAL))) AS date), date_table AS (SELECT DISTINCT DATE_TRUNC(\'MONTH\', date) AS date FROM dates) SELECT * FROM date_table',
          postgres: 'WITH dates AS (SELECT GENERATE_SERIES(CAST(\'2020-01-01\' AS DATE), CAST(\'2024-01-01\' AS DATE), CAST(\'1 day\' AS INTERVAL)) AS date), date_table AS (SELECT DISTINCT DATE_TRUNC(\'MONTH\', date) AS date FROM dates) SELECT * FROM date_table',
        },
      },
    );
    this.validateAll(
      'GENERATE_SERIES(a, b, \'  2   days  \')',
      {
        write: {
          postgres: 'GENERATE_SERIES(a, b, INTERVAL \'2 DAYS\')',
          presto: 'UNNEST(SEQUENCE(a, b, INTERVAL \'2\' DAY))',
          trino: 'UNNEST(SEQUENCE(a, b, INTERVAL \'2\' DAY))',
        },
      },
    );
    this.validateAll(
      'GENERATE_SERIES(\'2019-01-01\'::TIMESTAMP, NOW(), \'1day\')',
      {
        write: {
          databricks: 'EXPLODE(SEQUENCE(CAST(\'2019-01-01\' AS TIMESTAMP), CAST(CURRENT_TIMESTAMP() AS TIMESTAMP), INTERVAL \'1\' DAY))',
          hive: 'EXPLODE(SEQUENCE(CAST(\'2019-01-01\' AS TIMESTAMP), CAST(CURRENT_TIMESTAMP() AS TIMESTAMP), INTERVAL \'1\' DAY))',
          postgres: 'GENERATE_SERIES(CAST(\'2019-01-01\' AS TIMESTAMP), CURRENT_TIMESTAMP, INTERVAL \'1 DAY\')',
          presto: 'UNNEST(SEQUENCE(CAST(\'2019-01-01\' AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP), INTERVAL \'1\' DAY))',
          spark: 'EXPLODE(SEQUENCE(CAST(\'2019-01-01\' AS TIMESTAMP), CAST(CURRENT_TIMESTAMP() AS TIMESTAMP), INTERVAL \'1\' DAY))',
          spark2: 'EXPLODE(SEQUENCE(CAST(\'2019-01-01\' AS TIMESTAMP), CAST(CURRENT_TIMESTAMP() AS TIMESTAMP), INTERVAL \'1\' DAY))',
          trino: 'UNNEST(SEQUENCE(CAST(\'2019-01-01\' AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP), INTERVAL \'1\' DAY))',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM GENERATE_SERIES(a, b)',
      {
        read: {
          tsql: 'SELECT * FROM GENERATE_SERIES(a, b)',
        },
        write: {
          databricks: 'SELECT * FROM EXPLODE(SEQUENCE(a, b))',
          hive: 'SELECT * FROM EXPLODE(SEQUENCE(a, b))',
          postgres: 'SELECT * FROM GENERATE_SERIES(a, b)',
          presto: 'SELECT * FROM UNNEST(SEQUENCE(a, b))',
          spark: 'SELECT * FROM EXPLODE(SEQUENCE(a, b))',
          spark2: 'SELECT * FROM EXPLODE(SEQUENCE(a, b))',
          trino: 'SELECT * FROM UNNEST(SEQUENCE(a, b))',
          tsql: 'SELECT * FROM GENERATE_SERIES(a, b)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM t CROSS JOIN GENERATE_SERIES(2, 4)',
      {
        write: {
          postgres: 'SELECT * FROM t CROSS JOIN GENERATE_SERIES(2, 4)',
          presto: 'SELECT * FROM t CROSS JOIN UNNEST(SEQUENCE(2, 4))',
          trino: 'SELECT * FROM t CROSS JOIN UNNEST(SEQUENCE(2, 4))',
          tsql: 'SELECT * FROM t CROSS JOIN GENERATE_SERIES(2, 4)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM t CROSS JOIN GENERATE_SERIES(2, 4) AS s',
      {
        write: {
          postgres: 'SELECT * FROM t CROSS JOIN GENERATE_SERIES(2, 4) AS s',
          presto: 'SELECT * FROM t CROSS JOIN UNNEST(SEQUENCE(2, 4)) AS _u(s)',
          trino: 'SELECT * FROM t CROSS JOIN UNNEST(SEQUENCE(2, 4)) AS _u(s)',
          tsql: 'SELECT * FROM t CROSS JOIN GENERATE_SERIES(2, 4) AS s',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM x FETCH 1 ROW',
      {
        write: {
          postgres: 'SELECT * FROM x FETCH FIRST 1 ROWS ONLY',
          presto: 'SELECT * FROM x FETCH FIRST 1 ROWS ONLY',
          hive: 'SELECT * FROM x LIMIT 1',
          spark: 'SELECT * FROM x LIMIT 1',
          sqlite: 'SELECT * FROM x LIMIT 1',
        },
      },
    );
    this.validateAll(
      'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname',
      {
        write: {
          postgres: 'SELECT fname, lname, age FROM person ORDER BY age DESC, fname ASC, lname',
          presto: 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC, lname',
          hive: 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname NULLS LAST',
          spark: 'SELECT fname, lname, age FROM person ORDER BY age DESC NULLS FIRST, fname ASC NULLS LAST, lname NULLS LAST',
        },
      },
    );
    this.validateAll(
      'SELECT CASE WHEN SUBSTRING(\'abcdefg\' FROM 1 FOR 2) IN (\'ab\') THEN 1 ELSE 0 END',
      {
        write: {
          hive: 'SELECT CASE WHEN SUBSTRING(\'abcdefg\', 1, 2) IN (\'ab\') THEN 1 ELSE 0 END',
          spark: 'SELECT CASE WHEN SUBSTRING(\'abcdefg\', 1, 2) IN (\'ab\') THEN 1 ELSE 0 END',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM x WHERE SUBSTRING(col1 FROM 3 + LENGTH(col1) - 10 FOR 10) IN (col2)',
      {
        write: {
          hive: 'SELECT * FROM x WHERE SUBSTRING(col1, 3 + LENGTH(col1) - 10, 10) IN (col2)',
          spark: 'SELECT * FROM x WHERE SUBSTRING(col1, 3 + LENGTH(col1) - 10, 10) IN (col2)',
        },
      },
    );
    this.validateAll(
      'SELECT TRIM(BOTH \' XXX \')',
      {
        write: {
          mysql: 'SELECT TRIM(\' XXX \')',
          postgres: 'SELECT TRIM(\' XXX \')',
          hive: 'SELECT TRIM(\' XXX \')',
        },
      },
    );
    this.validateAll(
      'TRIM(LEADING FROM \' XXX \')',
      {
        write: {
          mysql: 'LTRIM(\' XXX \')',
          postgres: 'LTRIM(\' XXX \')',
          hive: 'LTRIM(\' XXX \')',
          presto: 'LTRIM(\' XXX \')',
        },
      },
    );
    this.validateAll(
      'TRIM(TRAILING FROM \' XXX \')',
      {
        write: {
          mysql: 'RTRIM(\' XXX \')',
          postgres: 'RTRIM(\' XXX \')',
          hive: 'RTRIM(\' XXX \')',
          presto: 'RTRIM(\' XXX \')',
        },
      },
    );
    this.validateAll(
      'TRIM(BOTH \'as\' FROM \'as string as\')',
      {
        write: {
          postgres: 'TRIM(BOTH \'as\' FROM \'as string as\')',
          spark: 'TRIM(BOTH \'as\' FROM \'as string as\')',
        },
      },
    );
    this.validateIdentity(
      'SELECT TRIM(LEADING \' XXX \' COLLATE "de_DE")',
      'SELECT LTRIM(\' XXX \' COLLATE "de_DE")',
    );
    this.validateIdentity(
      'SELECT TRIM(TRAILING \' XXX \' COLLATE "de_DE")',
      'SELECT RTRIM(\' XXX \' COLLATE "de_DE")',
    );
    this.validateIdentity('LEVENSHTEIN(col1, col2)');
    this.validateIdentity('LEVENSHTEIN_LESS_EQUAL(col1, col2, 1)');
    this.validateIdentity('LEVENSHTEIN(col1, col2, 1, 2, 3)');
    this.validateIdentity('LEVENSHTEIN_LESS_EQUAL(col1, col2, 1, 2, 3, 4)');

    this.validateAll(
      '\'{"a":1,"b":2}\'::json->\'b\'',
      {
        write: {
          postgres: 'CAST(\'{"a":1,"b":2}\' AS JSON) -> \'b\'',
          redshift: 'JSON_EXTRACT_PATH_TEXT(\'{"a":1,"b":2}\', \'b\')',
        },
      },
    );
    this.validateAll(
      'merge into x as x using (select id) as y on a = b WHEN matched then update set X."A" = y.b',
      {
        write: {
          postgres: 'MERGE INTO x AS x USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET "A" = y.b',
          trino: 'MERGE INTO x AS x USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET "A" = y.b',
          snowflake: 'MERGE INTO x AS x USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET X."A" = y.b',
        },
      },
    );
    this.validateAll(
      'merge into x as z using (select id) as y on a = b WHEN matched then update set X.a = y.b',
      {
        write: {
          postgres: 'MERGE INTO x AS z USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET a = y.b',
          snowflake: 'MERGE INTO x AS z USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET X.a = y.b',
        },
      },
    );
    this.validateAll(
      'merge into x as z using (select id) as y on a = b WHEN matched then update set Z.a = y.b',
      {
        write: {
          postgres: 'MERGE INTO x AS z USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET a = y.b',
          snowflake: 'MERGE INTO x AS z USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET Z.a = y.b',
        },
      },
    );
    this.validateAll(
      'merge into x using (select id) as y on a = b WHEN matched then update set x.a = y.b',
      {
        write: {
          postgres: 'MERGE INTO x USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET a = y.b',
          snowflake: 'MERGE INTO x USING (SELECT id) AS y ON a = b WHEN MATCHED THEN UPDATE SET x.a = y.b',
        },
      },
    );
    this.validateAll(
      'x / y ^ z',
      {
        write: {
          '': 'x / POWER(y, z)',
          'postgres': 'x / POWER(y, z)',
        },
      },
    );
    this.validateAll(
      'CAST(x AS NAME)',
      {
        read: {
          redshift: 'CAST(x AS NAME)',
        },
        write: {
          postgres: 'CAST(x AS NAME)',
          redshift: 'CAST(x AS NAME)',
        },
      },
    );
    expect(this.parseOne('id::UUID')).toBeInstanceOf(CastExpr);

    this.validateIdentity('1::"int"', 'CAST(1 AS INT)');
    this.validateIdentity('1::"udt"', 'CAST(1 AS "udt")');
    this.validateIdentity(
      'COPY tbl (col1, col2) FROM \'file\' WITH (FORMAT format, HEADER MATCH, FREEZE TRUE)',
    );
    this.validateIdentity(
      'COPY tbl (col1, col2) TO \'file\' WITH (FORMAT format, HEADER MATCH, FREEZE TRUE)',
    );
    this.validateIdentity(
      'COPY (SELECT * FROM t) TO \'file\' WITH (FORMAT format, HEADER MATCH, FREEZE TRUE)',
    );
    this.validateIdentity('cast(a as FLOAT)', 'CAST(a AS DOUBLE PRECISION)');
    this.validateIdentity('cast(a as FLOAT8)', 'CAST(a AS DOUBLE PRECISION)');
    this.validateIdentity('cast(a as FLOAT4)', 'CAST(a AS REAL)');

    this.validateAll(
      '1 / DIV(4, 2)',
      {
        read: {
          postgres: '1 / DIV(4, 2)',
        },
        write: {
          sqlite: '1 / CAST(CAST(CAST(4 AS REAL) / 2 AS INTEGER) AS REAL)',
          duckdb: '1 / CAST(4 // 2 AS DECIMAL)',
          bigquery: '1 / CAST(DIV(4, 2) AS NUMERIC)',
        },
      },
    );
    this.validateAll(
      'CAST(DIV(4, 2) AS DECIMAL(5, 3))',
      {
        read: {
          duckdb: 'CAST(4 // 2 AS DECIMAL(5, 3))',
        },
        write: {
          duckdb: 'CAST(CAST(4 // 2 AS DECIMAL) AS DECIMAL(5, 3))',
          postgres: 'CAST(DIV(4, 2) AS DECIMAL(5, 3))',
        },
      },
    );

    this.validateAll(
      'SELECT TO_DATE(\'01/01/2000\', \'MM/DD/YYYY\')',
      {
        write: {
          duckdb: 'SELECT CAST(STRPTIME(\'01/01/2000\', \'%m/%d/%Y\') AS DATE)',
          postgres: 'SELECT TO_DATE(\'01/01/2000\', \'MM/DD/YYYY\')',
        },
      },
    );

    this.validateIdentity(
      'SELECT js, js IS JSON AS "json?", js IS JSON VALUE AS "scalar?", js IS JSON SCALAR AS "scalar?", js IS JSON OBJECT AS "object?", js IS JSON ARRAY AS "array?" FROM t',
    );
    this.validateIdentity(
      'SELECT js, js IS JSON ARRAY WITH UNIQUE KEYS AS "array w. UK?", js IS JSON ARRAY WITHOUT UNIQUE KEYS AS "array w/o UK?", js IS JSON ARRAY UNIQUE KEYS AS "array w UK 2?" FROM t',
    );
    this.validateIdentity(
      'MERGE INTO target_table USING source_table AS source ON target.id = source.id WHEN MATCHED THEN DO NOTHING WHEN NOT MATCHED THEN DO NOTHING RETURNING MERGE_ACTION(), *',
    );
    this.validateIdentity(
      'SELECT 1 FROM ((VALUES (1)) AS vals(id) LEFT OUTER JOIN tbl ON vals.id = tbl.id)',
    );
    this.validateIdentity('SELECT OVERLAY(a PLACING b FROM 1)');
    this.validateIdentity('SELECT OVERLAY(a PLACING b FROM 1 FOR 1)');
    this.validateIdentity('ARRAY[1, 2, 3] && ARRAY[1, 2]').assertIs(ArrayOverlapsExpr);

    this.validateAll(
      'SELECT JSONB_EXISTS(\'{"a": [1,2,3]}\', \'a\')',
      {
        write: {
          postgres: 'SELECT JSONB_EXISTS(\'{"a": [1,2,3]}\', \'a\')',
          duckdb: 'SELECT JSON_EXISTS(\'{"a": [1,2,3]}\', \'$.a\')',
        },
      },
    );
    this.validateAll(
      'WITH t AS (SELECT ARRAY[1, 2, 3] AS col) SELECT * FROM t WHERE 1 <= ANY(col) AND 2 = ANY(col)',
      {
        write: {
          postgres: 'WITH t AS (SELECT ARRAY[1, 2, 3] AS col) SELECT * FROM t WHERE 1 <= ANY(col) AND 2 = ANY(col)',
          hive: 'WITH t AS (SELECT ARRAY(1, 2, 3) AS col) SELECT * FROM t WHERE EXISTS(col, x -> 1 <= x) AND EXISTS(col, x -> 2 = x)',
          spark2: 'WITH t AS (SELECT ARRAY(1, 2, 3) AS col) SELECT * FROM t WHERE EXISTS(col, x -> 1 <= x) AND EXISTS(col, x -> 2 = x)',
          spark: 'WITH t AS (SELECT ARRAY(1, 2, 3) AS col) SELECT * FROM t WHERE EXISTS(col, x -> 1 <= x) AND EXISTS(col, x -> 2 = x)',
          databricks: 'WITH t AS (SELECT ARRAY(1, 2, 3) AS col) SELECT * FROM t WHERE EXISTS(col, x -> 1 <= x) AND EXISTS(col, x -> 2 = x)',
        },
      },
    );

    this.validateIdentity(
      '/*+ some comment*/ SELECT b.foo, b.bar FROM baz AS b',
      '/* + some comment */ SELECT b.foo, b.bar FROM baz AS b',
    );

    this.validateIdentity(
      'SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY a) FILTER(WHERE CAST(b AS BOOLEAN)) AS mean_value FROM (VALUES (0, \'t\')) AS fake_data(a, b)',
    );

    this.validateAll(
      'SELECT JSON_OBJECT_AGG(k, v) FROM t',
      {
        write: {
          postgres: 'SELECT JSON_OBJECT_AGG(k, v) FROM t',
          duckdb: 'SELECT JSON_GROUP_OBJECT(k, v) FROM t',
        },
      },
    );

    this.validateAll(
      'SELECT JSONB_OBJECT_AGG(k, v) FROM t',
      {
        write: {
          postgres: 'SELECT JSONB_OBJECT_AGG(k, v) FROM t',
          duckdb: 'SELECT JSON_GROUP_OBJECT(k, v) FROM t',
        },
      },
    );

    this.validateAll(
      'SELECT DATE_BIN(\'30 days\', timestamp_col, (SELECT MIN(TIMESTAMP) from table)) FROM table',
      {
        write: {
          postgres: 'SELECT DATE_BIN(\'30 days\', timestamp_col, (SELECT MIN(TIMESTAMP) FROM table)) FROM table',
          duckdb: 'SELECT TIME_BUCKET(\'30 days\', timestamp_col, (SELECT MIN(TIMESTAMP) FROM "table")) FROM "table"',
        },
      },
    );

    // Postgres introduced ANY_VALUE in version 16
    this.validateAll(
      'SELECT ANY_VALUE(1) AS col',
      {
        write: {
          'postgres': 'SELECT ANY_VALUE(1) AS col',
          'postgres, version=16': 'SELECT ANY_VALUE(1) AS col',
          'postgres, version=17.5': 'SELECT ANY_VALUE(1) AS col',
          'postgres, version=15': 'SELECT MAX(1) AS col',
          'postgres, version=13.9': 'SELECT MAX(1) AS col',
        },
      },
    );

    this.validateIdentity('SELECT * FROM foo WHERE id = %s');
    this.validateIdentity('SELECT * FROM foo WHERE id = %(id_param)s');
    this.validateIdentity('SELECT * FROM foo WHERE id = ?');

    this.validateIdentity('a ?| b').assertIs(JsonbContainsAnyTopKeysExpr);
    this.validateIdentity(
      'SELECT \'{"a":1, "b":2, "c":3}\'::jsonb ?| array[\'b\', \'c\']',
      'SELECT CAST(\'{"a":1, "b":2, "c":3}\' AS JSONB) ?| ARRAY[\'b\', \'c\']',
    );

    this.validateIdentity('a ?& b').assertIs(JsonbContainsAllTopKeysExpr);
    this.validateIdentity(
      'SELECT \'["a", "b"]\'::jsonb ?& array[\'a\', \'b\']',
      'SELECT CAST(\'["a", "b"]\' AS JSONB) ?& ARRAY[\'a\', \'b\']',
    );

    this.validateIdentity('a #- b').assertIs(JsonbDeleteAtPathExpr);
    this.validateIdentity(
      'SELECT \'["a", {"b":1}]\'::jsonb #- \'{1,b}\'',
      'SELECT CAST(\'["a", {"b":1}]\' AS JSONB) #- \'{1,b}\'',
    );

    this.validateIdentity('SELECT JSON_AGG(DISTINCT name) FROM users');
    this.validateIdentity(
      'SELECT JSON_AGG(c1 ORDER BY c1) FROM (VALUES (\'c\'), (\'b\'), (\'a\')) AS t(c1)',
    );
    this.validateIdentity(
      'SELECT JSON_AGG(DISTINCT c1 ORDER BY c1) FROM (VALUES (\'c\'), (\'b\'), (\'a\')) AS t(c1)',
    );
    this.validateAll(
      'SELECT REGEXP_REPLACE(\'aaa\', \'a\', \'b\')',
      {
        read: {
          postgres: 'SELECT REGEXP_REPLACE(\'aaa\', \'a\', \'b\')',
          duckdb: 'SELECT REGEXP_REPLACE(\'aaa\', \'a\', \'b\')',
        },
        write: {
          duckdb: 'SELECT REGEXP_REPLACE(\'aaa\', \'a\', \'b\')',
        },
      },
    );

    this.validateAll(
      'SELECT TO_CHAR(foo, bar)',
      {
        read: {
          redshift: 'SELECT TO_CHAR(foo, bar)',
        },
        write: {
          postgres: 'SELECT TO_CHAR(foo, bar)',
          redshift: 'SELECT TO_CHAR(foo, bar)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE table1 (a INT, b INT, PRIMARY KEY (a))',
      {
        read: {
          sqlite: 'CREATE TABLE table1 (a INT, b INT, PRIMARY KEY (a))',
          postgres: 'CREATE TABLE table1 (a INT, b INT, PRIMARY KEY (a))',
        },
      },
    );
    this.validateIdentity('SELECT NUMRANGE(1.1, 2.2) -|- NUMRANGE(2.2, 3.3)');
    this.validateIdentity(
      'SELECT SLOPE(point \'(4,4)\', point \'(0,0)\')',
      'SELECT SLOPE(CAST(\'(4,4)\' AS POINT), CAST(\'(0,0)\' AS POINT))',
    );

    const widthBucket1 = this.validateIdentity('WIDTH_BUCKET(10, ARRAY[5, 15])');
    expect((widthBucket1 as WidthBucketExpr).args.threshold).not.toBeNull();

    const widthBucket2 = this.validateIdentity('WIDTH_BUCKET(10, 5, 15, 25)');
    expect((widthBucket2 as WidthBucketExpr).getArgKey('threshold')).toBeUndefined();

    this.validateAll(
      'UPDATE foo SET a = bar.a, b = bar.b FROM bar WHERE foo.id = bar.id',
      {
        write: {
          postgres: 'UPDATE foo SET a = bar.a, b = bar.b FROM bar WHERE foo.id = bar.id',
          doris: 'UPDATE foo SET a = bar.a, b = bar.b FROM bar WHERE foo.id = bar.id',
          starrocks: 'UPDATE foo SET a = bar.a, b = bar.b FROM bar WHERE foo.id = bar.id',
          mysql: 'UPDATE foo JOIN bar ON TRUE SET foo.a = bar.a, foo.b = bar.b WHERE foo.id = bar.id',
          singlestore: 'UPDATE foo JOIN bar ON TRUE SET foo.a = bar.a, foo.b = bar.b WHERE foo.id = bar.id',
        },
      },
    );

    this.validateIdentity('SELECT MLEAST(VARIADIC ARRAY[10, -1, 5, 4.4])');
    this.validateIdentity(
      'SELECT MLEAST(VARIADIC ARRAY[]::numeric[])',
      'SELECT MLEAST(VARIADIC CAST(ARRAY[] AS DECIMAL[]))',
    );
    this.validateIdentity(
      'SELECT * FROM schema_name.table_name st WHERE JSON_EXTRACT_PATH_TEXT((st.data)::json, variadic array[\'test\'::text]) = \'test\'::text',
      'SELECT * FROM schema_name.table_name AS st WHERE JSON_EXTRACT_PATH_TEXT(CAST((st.data) AS JSON), VARIADIC ARRAY[CAST(\'test\' AS TEXT)]) = CAST(\'test\' AS TEXT)',
    );
  }

  testDdl () {
    // Checks that user-defined types are parsed into DataType instead of Identifier
    const createUdt = this.parseOne('CREATE TABLE t (a udt)');
    const schema = createUdt.getArgKey('this') as Expression;
    const colKind = (schema.getArgKey('expressions') as Expression[])[0].getArgKey('kind');
    expect(colKind).toBeInstanceOf(DataTypeExpr);

    // Checks that OID is parsed into a DataType (ObjectIdentifier)
    expect(
      this.parseOne('CREATE TABLE p.t (c oid)').find(ObjectIdentifierExpr),
    ).toBeInstanceOf(ObjectIdentifierExpr);

    const exprInterval = this.parseOne('CREATE TABLE t (x INTERVAL day)');
    const cdef = exprInterval.find(ColumnDefExpr);
    expect(cdef).toBeInstanceOf(ColumnDefExpr);
    expect((cdef as ColumnDefExpr).getArgKey('kind')).toBeInstanceOf(DataTypeExpr);
    expect(exprInterval.sql({ dialect: 'postgres' })).toBe('CREATE TABLE t (x INTERVAL DAY)');

    this.validateIdentity('ALTER INDEX "IX_Ratings_Column1" RENAME TO "IX_Ratings_Column2"');
    this.validateIdentity('CREATE TABLE x (a TEXT COLLATE "de_DE")');
    this.validateIdentity('CREATE TABLE x (a TEXT COLLATE pg_catalog."default")');
    this.validateIdentity('CREATE TABLE t (col INT[3][5])');
    this.validateIdentity('CREATE TABLE t (col INT[3])');
    this.validateIdentity('CREATE INDEX IF NOT EXISTS ON t(c)');
    this.validateIdentity('CREATE INDEX et_vid_idx ON et(vid) INCLUDE (fid)');
    this.validateIdentity('CREATE INDEX idx_x ON x USING BTREE(x, y) WHERE (NOT y IS NULL)');
    this.validateIdentity('CREATE TABLE test (elems JSONB[])');
    this.validateIdentity('CREATE TABLE public.y (x TSTZRANGE NOT NULL)');
    this.validateIdentity('CREATE TABLE test (foo HSTORE)');
    this.validateIdentity('CREATE TABLE test (foo JSONB)');
    this.validateIdentity('CREATE TABLE test (foo VARCHAR(64)[])');
    this.validateIdentity('CREATE TABLE test (foo INT) PARTITION BY HASH(foo)');
    this.validateIdentity('INSERT INTO x VALUES (1, \'a\', 2.0) RETURNING a');
    this.validateIdentity('INSERT INTO x VALUES (1, \'a\', 2.0) RETURNING a, b');
    this.validateIdentity('INSERT INTO x VALUES (1, \'a\', 2.0) RETURNING *');
    this.validateIdentity('UPDATE tbl_name SET foo = 123 RETURNING a');
    this.validateIdentity('CREATE TABLE cities_partdef PARTITION OF cities DEFAULT');
    this.validateIdentity('CREATE TABLE t (c CHAR(2) UNIQUE NOT NULL) INHERITS (t1)');
    this.validateIdentity('CREATE TABLE s.t (c CHAR(2) UNIQUE NOT NULL) INHERITS (s.t1, s.t2)');
    this.validateIdentity('CREATE FUNCTION x(INT) RETURNS INT SET search_path = \'public\'');
    this.validateIdentity('TRUNCATE TABLE t1 CONTINUE IDENTITY');
    this.validateIdentity('TRUNCATE TABLE t1 RESTART IDENTITY');
    this.validateIdentity('TRUNCATE TABLE t1 CASCADE');
    this.validateIdentity('TRUNCATE TABLE t1 RESTRICT');
    this.validateIdentity('TRUNCATE TABLE t1 CONTINUE IDENTITY CASCADE');
    this.validateIdentity('TRUNCATE TABLE t1 RESTART IDENTITY RESTRICT');
    this.validateIdentity('ALTER TABLE t1 SET LOGGED');
    this.validateIdentity('ALTER TABLE t1 SET UNLOGGED');
    this.validateIdentity('ALTER TABLE t1 SET WITHOUT CLUSTER');
    this.validateIdentity('ALTER TABLE t1 SET WITHOUT OIDS');
    this.validateIdentity('ALTER TABLE t1 SET ACCESS METHOD method');
    this.validateIdentity('ALTER TABLE t1 SET TABLESPACE tablespace');
    this.validateIdentity('ALTER TABLE t1 SET (fillfactor = 5, autovacuum_enabled = TRUE)');
    this.validateIdentity(
      'INSERT INTO book (isbn, title) VALUES ($1, $2) ON CONFLICT(isbn) WHERE deleted_at IS NULL DO UPDATE SET title = EXCLUDED.title RETURNING id, isbn',
    );
    this.validateIdentity(
      'INSERT INTO newtable AS t(a, b, c) VALUES (1, 2, 3) ON CONFLICT(c) DO UPDATE SET a = t.a + 1 WHERE t.a < 1',
    );
    this.validateIdentity(
      'ALTER TABLE tested_table ADD CONSTRAINT unique_example UNIQUE (column_name) NOT VALID',
    );
    this.validateIdentity(
      'CREATE FUNCTION pymax(a INT, b INT) RETURNS INT LANGUAGE plpython3u AS $$\n  if a > b:\n    return a\n  return b\n$$',
    );
    this.validateIdentity(
      'CREATE TABLE t (vid INT NOT NULL, CONSTRAINT ht_vid_nid_fid_idx EXCLUDE (INT4RANGE(vid, nid) WITH &&, INT4RANGE(fid, fid, \'[]\') WITH &&))',
    );
    this.validateIdentity('CREATE TABLE t (i INT, a TEXT, PRIMARY KEY (i) INCLUDE (a))');
    this.validateIdentity(
      'CREATE TABLE t (i INT, PRIMARY KEY (i), EXCLUDE USING gist(col varchar_pattern_ops DESC NULLS LAST WITH &&) WITH (sp1=1, sp2=2))',
    );
    this.validateIdentity(
      'CREATE TABLE t (i INT, EXCLUDE USING btree(INT4RANGE(vid, nid, \'[]\') ASC NULLS FIRST WITH &&) INCLUDE (col1, col2))',
    );
    this.validateIdentity(
      'CREATE TABLE t (i INT, EXCLUDE USING gin(col1 WITH &&, col2 WITH ||) USING INDEX TABLESPACE tablespace WHERE (id > 5))',
    );
    this.validateIdentity(
      'CREATE TABLE A (LIKE B INCLUDING CONSTRAINT INCLUDING COMPRESSION EXCLUDING COMMENTS)',
    );
    this.validateIdentity(
      'CREATE TABLE cust_part3 PARTITION OF customers FOR VALUES WITH (MODULUS 3, REMAINDER 2)',
    );
    this.validateIdentity(
      'CREATE TABLE measurement_y2016m07 PARTITION OF measurement (unitsales DEFAULT 0) FOR VALUES FROM (\'2016-07-01\') TO (\'2016-08-01\')',
    );
    this.validateIdentity(
      'CREATE TABLE measurement_ym_older PARTITION OF measurement_year_month FOR VALUES FROM (MINVALUE, MINVALUE) TO (2016, 11)',
    );
    this.validateIdentity(
      'CREATE TABLE measurement_ym_y2016m11 PARTITION OF measurement_year_month FOR VALUES FROM (2016, 11) TO (2016, 12)',
    );
    this.validateIdentity(
      'CREATE TABLE cities_ab PARTITION OF cities (CONSTRAINT city_id_nonzero CHECK (city_id <> 0)) FOR VALUES IN (\'a\', \'b\')',
    );
    this.validateIdentity(
      'CREATE TABLE cities_ab PARTITION OF cities (CONSTRAINT city_id_nonzero CHECK (city_id <> 0)) FOR VALUES IN (\'a\', \'b\') PARTITION BY RANGE(population)',
    );
    this.validateIdentity(
      'CREATE INDEX foo ON bar.baz USING btree(col1 varchar_pattern_ops ASC, col2)',
    );
    this.validateIdentity(
      'CREATE INDEX index_issues_on_title_trigram ON public.issues USING gin(title public.gin_trgm_ops)',
    );
    this.validateIdentity(
      'INSERT INTO x VALUES (1, \'a\', 2.0) ON CONFLICT(id) DO NOTHING RETURNING *',
    );
    this.validateIdentity(
      'INSERT INTO x VALUES (1, \'a\', 2.0) ON CONFLICT(id) DO UPDATE SET x.id = 1 RETURNING *',
    );
    this.validateIdentity(
      'INSERT INTO x VALUES (1, \'a\', 2.0) ON CONFLICT(id) DO UPDATE SET x.id = excluded.id RETURNING *',
    );
    this.validateIdentity(
      'INSERT INTO x VALUES (1, \'a\', 2.0) ON CONFLICT ON CONSTRAINT pkey DO NOTHING RETURNING *',
    );
    this.validateIdentity(
      'INSERT INTO x VALUES (1, \'a\', 2.0) ON CONFLICT ON CONSTRAINT pkey DO UPDATE SET x.id = 1 RETURNING *',
    );
    this.validateIdentity(
      'DELETE FROM event USING sales AS s WHERE event.eventid = s.eventid RETURNING a',
    );
    this.validateIdentity(
      'WITH t(c) AS (SELECT 1) SELECT * INTO UNLOGGED foo FROM (SELECT c AS c FROM t) AS temp',
    );
    this.validateIdentity(
      'CREATE TABLE test (x TIMESTAMP WITHOUT TIME ZONE[][])',
      'CREATE TABLE test (x TIMESTAMP[][])',
    );
    this.validateIdentity(
      'CREATE FUNCTION add(integer, integer) RETURNS INT LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT AS \'select $1 + $2;\'',
    );
    this.validateIdentity(
      'CREATE FUNCTION add(integer, integer) RETURNS INT LANGUAGE SQL IMMUTABLE STRICT AS \'select $1 + $2;\'',
    );
    this.validateIdentity(
      'CREATE FUNCTION add(INT, INT) RETURNS INT SET search_path TO \'public\' AS \'select $1 + $2;\' LANGUAGE SQL IMMUTABLE',
      undefined,
      { checkCommandWarning: true },
    );
    this.validateIdentity(
      'CREATE FUNCTION x(INT) RETURNS INT SET foo FROM CURRENT',
      undefined,
      { checkCommandWarning: true },
    );
    this.validateIdentity(
      'CREATE FUNCTION add(integer, integer) RETURNS integer AS \'select $1 + $2;\' LANGUAGE SQL IMMUTABLE CALLED ON NULL INPUT',
      undefined,
      { checkCommandWarning: true },
    );
    this.validateIdentity(
      'CREATE CONSTRAINT TRIGGER my_trigger AFTER INSERT OR DELETE OR UPDATE OF col_a, col_b ON public.my_table DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION do_sth()',
      undefined,
      { checkCommandWarning: true },
    );
    this.validateIdentity(
      'CREATE UNLOGGED TABLE foo AS WITH t(c) AS (SELECT 1) SELECT * FROM (SELECT c AS c FROM t) AS temp',
    );
    this.validateIdentity(
      'ALTER TABLE foo ADD COLUMN id BIGINT NOT NULL PRIMARY KEY DEFAULT 1, ADD CONSTRAINT fk_orders_user FOREIGN KEY (id) REFERENCES foo (id)',
    );
    this.validateIdentity(
      'CREATE TABLE t (col integer ARRAY[3])',
      'CREATE TABLE t (col INT[3])',
    );
    this.validateIdentity(
      'CREATE TABLE t (col integer ARRAY)',
      'CREATE TABLE t (col INT[])',
    );
    this.validateIdentity(
      'CREATE FUNCTION x(INT) RETURNS INT SET search_path TO \'public\'',
      'CREATE FUNCTION x(INT) RETURNS INT SET search_path = \'public\'',
    );
    this.validateIdentity(
      'CREATE TABLE test (x TIMESTAMP WITHOUT TIME ZONE[][])',
      'CREATE TABLE test (x TIMESTAMP[][])',
    );
    this.validateIdentity(
      'CREATE OR REPLACE FUNCTION function_name (input_a character varying DEFAULT NULL::character varying)',
      'CREATE OR REPLACE FUNCTION function_name(input_a VARCHAR DEFAULT CAST(NULL AS VARCHAR))',
    );

    // Function parameter modes
    this.validateIdentity('CREATE FUNCTION foo(a INT)');
    this.validateIdentity('CREATE FUNCTION foo(IN a INT)');
    this.validateIdentity('CREATE FUNCTION foo(OUT a INT)');
    this.validateIdentity('CREATE FUNCTION foo(INOUT a INT)');
    this.validateIdentity('CREATE FUNCTION foo(VARIADIC a INT[])');
    this.validateIdentity('CREATE FUNCTION foo(out INT)'); // "out" as identifier
    this.validateIdentity('CREATE FUNCTION foo(inout VARCHAR)'); // "inout" as identifier
    this.validateIdentity('CREATE FUNCTION foo(variadic INT[])'); // "variadic" as identifier
    this.validateIdentity(
      'CREATE FUNCTION foo(a INT, OUT b INT, INOUT c VARCHAR, VARIADIC d INT[])',
    );
    this.validateIdentity('CREATE OR REPLACE FUNCTION foo(INOUT id UUID)');
    this.validateIdentity(
      'CREATE OR REPLACE FUNCTION foo(id UUID, OUT created_at TIMESTAMPTZ)',
    );
    this.validateIdentity('CREATE FUNCTION foo(OUT x INT DEFAULT 5)');
    this.validateIdentity('CREATE FUNCTION foo(INOUT y VARCHAR DEFAULT \'test\')');
    this.validateIdentity('CREATE FUNCTION foo(IN a INT DEFAULT 0, OUT b INT)');
    this.validateAll(
      'CREATE FUNCTION foo(VARIADIC args INT[] DEFAULT ARRAY[]::INT[])',
      {
        write: {
          postgres: 'CREATE FUNCTION foo(VARIADIC args INT[] DEFAULT CAST(ARRAY[] AS INT[]))',
        },
      },
    );
    this.validateIdentity('CREATE FUNCTION foo(OUT result INT, IN input INT DEFAULT 10)');

    this.validateIdentity(
      'CREATE TABLE products (product_no INT UNIQUE, name TEXT, price DECIMAL)',
      'CREATE TABLE products (product_no INT UNIQUE, name TEXT, price DECIMAL)',
    );
    this.validateIdentity(
      'CREATE TABLE products (product_no INT CONSTRAINT must_be_different UNIQUE, name TEXT CONSTRAINT present NOT NULL, price DECIMAL)',
      'CREATE TABLE products (product_no INT CONSTRAINT must_be_different UNIQUE, name TEXT CONSTRAINT present NOT NULL, price DECIMAL)',
    );
    this.validateIdentity(
      'CREATE TABLE products (product_no INT, name TEXT, price DECIMAL, UNIQUE (product_no, name))',
      'CREATE TABLE products (product_no INT, name TEXT, price DECIMAL, UNIQUE (product_no, name))',
    );
    this.validateIdentity(
      'CREATE TABLE products (product_no INT UNIQUE, name TEXT, price DECIMAL CHECK (price > 0), discounted_price DECIMAL CONSTRAINT positive_discount CHECK (discounted_price > 0), CHECK (product_no > 1), CONSTRAINT valid_discount CHECK (price > discounted_price))',
    );
    this.validateIdentity(
      `
            CREATE INDEX index_ci_builds_on_commit_id_and_artifacts_expireatandidpartial
            ON public.ci_builds
            USING btree (commit_id, artifacts_expire_at, id)
            WHERE (
                ((type)::text = 'Ci::Build'::text)
                AND ((retried = false) OR (retried IS NULL))
                AND ((name)::text = ANY (ARRAY[
                    ('sast'::character varying)::text,
                    ('dependency_scanning'::character varying)::text,
                    ('sast:container'::character varying)::text,
                    ('container_scanning'::character varying)::text,
                    ('dast'::character varying)::text
                ]))
            )
            `,
      'CREATE INDEX index_ci_builds_on_commit_id_and_artifacts_expireatandidpartial ON public.ci_builds USING btree(commit_id, artifacts_expire_at, id) WHERE ((CAST((type) AS TEXT) = CAST(\'Ci::Build\' AS TEXT)) AND ((retried = FALSE) OR (retried IS NULL)) AND (CAST((name) AS TEXT) = ANY(ARRAY[CAST((CAST(\'sast\' AS VARCHAR)) AS TEXT), CAST((CAST(\'dependency_scanning\' AS VARCHAR)) AS TEXT), CAST((CAST(\'sast:container\' AS VARCHAR)) AS TEXT), CAST((CAST(\'container_scanning\' AS VARCHAR)) AS TEXT), CAST((CAST(\'dast\' AS VARCHAR)) AS TEXT)])))',
    );
    this.validateIdentity(
      'CREATE INDEX index_ci_pipelines_on_project_idandrefandiddesc ON public.ci_pipelines USING btree(project_id, ref, id DESC)',
    );
    this.validateIdentity(
      'TRUNCATE TABLE ONLY t1, t2*, ONLY t3, t4, t5* RESTART IDENTITY CASCADE',
      'TRUNCATE TABLE ONLY t1, t2, ONLY t3, t4, t5 RESTART IDENTITY CASCADE',
    );

    this.validateAll(
      'CREATE TABLE x (a UUID, b BYTEA)',
      {
        write: {
          duckdb: 'CREATE TABLE x (a UUID, b BLOB)',
          presto: 'CREATE TABLE x (a UUID, b VARBINARY)',
          hive: 'CREATE TABLE x (a UUID, b BINARY)',
          spark: 'CREATE TABLE x (a STRING, b BINARY)',
          tsql: 'CREATE TABLE x (a UNIQUEIDENTIFIER, b VARBINARY)',
        },
      },
    );

    this.validateIdentity('CREATE TABLE tbl (col INT UNIQUE NULLS NOT DISTINCT DEFAULT 9.99)');
    this.validateIdentity('CREATE TABLE tbl (col UUID UNIQUE DEFAULT GEN_RANDOM_UUID())');
    this.validateIdentity('CREATE TABLE tbl (col UUID, UNIQUE NULLS NOT DISTINCT (col))');
    this.validateIdentity('CREATE TABLE tbl (col_a INT GENERATED ALWAYS AS (1 + 2) STORED)');
    this.validateIdentity(
      'CREATE TABLE tbl (col_a INTERVAL GENERATED ALWAYS AS (a - b) STORED)',
    );

    this.validateIdentity('CREATE INDEX CONCURRENTLY ix_table_id ON tbl USING btree(id)');
    this.validateIdentity(
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_table_id ON tbl USING btree(id)',
    );
    this.validateIdentity('DROP INDEX ix_table_id');
    this.validateIdentity('DROP INDEX IF EXISTS ix_table_id');
    this.validateIdentity('DROP INDEX CONCURRENTLY ix_table_id');
    this.validateIdentity('DROP INDEX CONCURRENTLY IF EXISTS ix_table_id');

    this.validateIdentity(
      `
        CREATE TABLE IF NOT EXISTS public.rental
        (
            inventory_id INT NOT NULL,
            CONSTRAINT rental_customer_id_fkey FOREIGN KEY (customer_id)
                REFERENCES public.customer (customer_id) MATCH FULL
                ON UPDATE CASCADE
                ON DELETE RESTRICT,
            CONSTRAINT rental_inventory_id_fkey FOREIGN KEY (inventory_id)
                REFERENCES public.inventory (inventory_id) MATCH PARTIAL
                ON UPDATE CASCADE
                ON DELETE RESTRICT,
            CONSTRAINT rental_staff_id_fkey FOREIGN KEY (staff_id)
                REFERENCES public.staff (staff_id) MATCH SIMPLE
                ON UPDATE CASCADE
                ON DELETE RESTRICT,
            INITIALLY IMMEDIATE
        )
        `,
      'CREATE TABLE IF NOT EXISTS public.rental (inventory_id INT NOT NULL, CONSTRAINT rental_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.customer (customer_id) MATCH FULL ON UPDATE CASCADE ON DELETE RESTRICT, CONSTRAINT rental_inventory_id_fkey FOREIGN KEY (inventory_id) REFERENCES public.inventory (inventory_id) MATCH PARTIAL ON UPDATE CASCADE ON DELETE RESTRICT, CONSTRAINT rental_staff_id_fkey FOREIGN KEY (staff_id) REFERENCES public.staff (staff_id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE RESTRICT, INITIALLY IMMEDIATE)',
    );

    for (const op of [
      '=',
      '>=',
      '<=',
      '<',
      '>',
      '&&',
      '||',
      '@>',
      '<@',
    ]) {
      this.validateIdentity(
        `CREATE TABLE circles (c circle, EXCLUDE USING gist(c WITH ${op}))`,
      );
    }

    expect(() => this.parseOne('CREATE TABLE products (price DECIMAL CHECK price > 0)')).toThrow();
    expect(() => this.parseOne('CREATE TABLE products (price DECIMAL, CHECK price > 1)')).toThrow();
  }

  testUnnest () {
    this.validateIdentity(
      'SELECT * FROM UNNEST(ARRAY[1, 2], ARRAY[\'foo\', \'bar\', \'baz\']) AS x(a, b)',
    );

    this.validateAll(
      'SELECT UNNEST(c) FROM t',
      {
        write: {
          hive: 'SELECT EXPLODE(c) FROM t',
          postgres: 'SELECT UNNEST(c) FROM t',
          presto: 'SELECT IF(_u.pos = _u_2.pos_2, _u_2.col) AS col FROM t CROSS JOIN UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(c)))) AS _u(pos) CROSS JOIN UNNEST(c) WITH ORDINALITY AS _u_2(col, pos_2) WHERE _u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(c) AND _u_2.pos_2 = CARDINALITY(c))',
        },
      },
    );
    this.validateAll(
      'SELECT UNNEST(ARRAY[1])',
      {
        write: {
          hive: 'SELECT EXPLODE(ARRAY(1))',
          postgres: 'SELECT UNNEST(ARRAY[1])',
          presto: 'SELECT IF(_u.pos = _u_2.pos_2, _u_2.col) AS col FROM UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(ARRAY[1])))) AS _u(pos) CROSS JOIN UNNEST(ARRAY[1]) WITH ORDINALITY AS _u_2(col, pos_2) WHERE _u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(ARRAY[1]) AND _u_2.pos_2 = CARDINALITY(ARRAY[1]))',
        },
      },
    );
  }

  testArrayOffset () {
    // Note: array offset logging test - skip log assertions, just validate SQL
    this.validateAll(
      'SELECT col[1]',
      {
        write: {
          bigquery: 'SELECT col[0]',
          duckdb: 'SELECT col[1]',
          hive: 'SELECT col[0]',
          postgres: 'SELECT col[1]',
          presto: 'SELECT col[1]',
        },
      },
    );
  }

  testBoolOr () {
    this.validateIdentity(
      'SELECT a, LOGICAL_OR(b) FROM table GROUP BY a',
      'SELECT a, BOOL_OR(b) FROM table GROUP BY a',
    );
  }

  testStringConcat () {
    this.validateIdentity('SELECT CONCAT(\'abcde\', 2, NULL, 22)');

    this.validateAll(
      'CONCAT(a, b)',
      {
        write: {
          '': 'CONCAT(COALESCE(a, \'\'), COALESCE(b, \'\'))',
          'clickhouse': 'CONCAT(COALESCE(a, \'\'), COALESCE(b, \'\'))',
          'duckdb': 'CONCAT(a, b)',
          'postgres': 'CONCAT(a, b)',
          'presto': 'CONCAT(COALESCE(CAST(a AS VARCHAR), \'\'), COALESCE(CAST(b AS VARCHAR), \'\'))',
        },
      },
    );
    this.validateAll(
      'a || b',
      {
        write: {
          '': 'a || b',
          'clickhouse': 'a || b',
          'duckdb': 'a || b',
          'postgres': 'a || b',
          'presto': 'CONCAT(CAST(a AS VARCHAR), CAST(b AS VARCHAR))',
        },
      },
    );
  }

  testVariance () {
    this.validateIdentity(
      'VAR_SAMP(x)',
      'VAR_SAMP(x)',
    );
    this.validateIdentity(
      'VAR_POP(x)',
      'VAR_POP(x)',
    );
    this.validateIdentity(
      'VARIANCE(x)',
      'VAR_SAMP(x)',
    );

    this.validateAll(
      'VAR_POP(x)',
      {
        read: {
          '': 'VARIANCE_POP(x)',
        },
        write: {
          postgres: 'VAR_POP(x)',
        },
      },
    );
  }

  testCorr () {
    this.validateAll(
      'SELECT CORR(a, b)',
      {
        write: {
          duckdb: 'SELECT CORR(a, b)',
          postgres: 'SELECT CORR(a, b)',
        },
      },
    );
    this.validateAll(
      'SELECT CORR(a, b) OVER (PARTITION BY c)',
      {
        write: {
          duckdb: 'SELECT CORR(a, b) OVER (PARTITION BY c)',
          postgres: 'SELECT CORR(a, b) OVER (PARTITION BY c)',
        },
      },
    );
    this.validateAll(
      'SELECT CORR(a, b) FILTER(WHERE c > 0)',
      {
        write: {
          duckdb: 'SELECT CORR(a, b) FILTER(WHERE c > 0)',
          postgres: 'SELECT CORR(a, b) FILTER(WHERE c > 0)',
        },
      },
    );
    this.validateAll(
      'SELECT CORR(a, b) FILTER(WHERE c > 0) OVER (PARTITION BY d)',
      {
        write: {
          duckdb: 'SELECT CORR(a, b) FILTER(WHERE c > 0) OVER (PARTITION BY d)',
          postgres: 'SELECT CORR(a, b) FILTER(WHERE c > 0) OVER (PARTITION BY d)',
        },
      },
    );
  }

  testRegexpBinary () {
    // See https://github.com/tobymao/sqlglot/pull/2404 for details
    expect(this.parseOne('\'thomas\' ~ \'.*thomas.*\'')).toBeInstanceOf(BinaryExpr);
    expect(this.parseOne('\'thomas\' ~* \'.*thomas.*\'')).toBeInstanceOf(BinaryExpr);
  }

  testUnnestJsonArray () {
    const trinoInput = `
            WITH t(boxcrate) AS (
              SELECT JSON '[{"boxes": [{"name": "f1", "type": "plant", "color": "red"}]}]'
            )
            SELECT
              JSON_EXTRACT_SCALAR(boxes,'$.name')  AS name,
              JSON_EXTRACT_SCALAR(boxes,'$.type')  AS type,
              JSON_EXTRACT_SCALAR(boxes,'$.color') AS color
            FROM t
            CROSS JOIN UNNEST(CAST(boxcrate AS array(json))) AS x(tbox)
            CROSS JOIN UNNEST(CAST(json_extract(tbox, '$.boxes') AS array(json))) AS y(boxes)
        `;

    const expectedPostgres = `WITH t(boxcrate) AS (
  SELECT
    CAST('[{"boxes": [{"name": "f1", "type": "plant", "color": "red"}]}]' AS JSON)
)
SELECT
  JSON_EXTRACT_PATH_TEXT(boxes, 'name') AS name,
  JSON_EXTRACT_PATH_TEXT(boxes, 'type') AS type,
  JSON_EXTRACT_PATH_TEXT(boxes, 'color') AS color
FROM t
CROSS JOIN JSON_ARRAY_ELEMENTS(CAST(boxcrate AS JSON)) AS x(tbox)
CROSS JOIN JSON_ARRAY_ELEMENTS(CAST(JSON_EXTRACT_PATH(tbox, 'boxes') AS JSON)) AS y(boxes)`;

    this.validateAll(expectedPostgres, {
      read: { trino: trinoInput },
      pretty: true,
    });
  }

  testRowsFrom () {
    this.validateIdentity('SELECT * FROM ROWS FROM (FUNC1(col1, col2))');
    this.validateIdentity(
      'SELECT * FROM ROWS FROM (FUNC1(col1) AS alias1("col1" TEXT), FUNC2(col2) AS alias2("col2" INT)) WITH ORDINALITY',
    );
    this.validateIdentity(
      'SELECT * FROM table1, ROWS FROM (FUNC1(col1) AS alias1("col1" TEXT)) WITH ORDINALITY AS alias3("col3" INT, "col4" TEXT)',
    );
  }

  testArrayLength () {
    this.validateIdentity('SELECT ARRAY_LENGTH(ARRAY[1, 2, 3], 1)');

    this.validateAll(
      'ARRAY_LENGTH(arr, 1)',
      {
        read: {
          bigquery: 'ARRAY_LENGTH(arr)',
          duckdb: 'ARRAY_LENGTH(arr)',
          presto: 'CARDINALITY(arr)',
          drill: 'REPEATED_COUNT(arr)',
          teradata: 'CARDINALITY(arr)',
          hive: 'SIZE(arr)',
          spark2: 'SIZE(arr)',
          spark: 'SIZE(arr)',
          databricks: 'SIZE(arr)',
        },
        write: {
          duckdb: 'ARRAY_LENGTH(arr, 1)',
          presto: 'CARDINALITY(arr)',
          teradata: 'CARDINALITY(arr)',
          bigquery: 'ARRAY_LENGTH(arr)',
          drill: 'REPEATED_COUNT(arr)',
          clickhouse: 'LENGTH(arr)',
          hive: 'SIZE(arr)',
          spark2: 'SIZE(arr)',
          spark: 'SIZE(arr)',
          databricks: 'SIZE(arr)',
        },
      },
    );

    this.validateAll(
      'ARRAY_LENGTH(arr, foo)',
      {
        write: {
          duckdb: 'ARRAY_LENGTH(arr, foo)',
          hive: UnsupportedError,
          spark2: UnsupportedError,
          spark: UnsupportedError,
          databricks: UnsupportedError,
          presto: UnsupportedError,
          teradata: UnsupportedError,
          bigquery: UnsupportedError,
          drill: UnsupportedError,
          clickhouse: UnsupportedError,
        },
      },
    );
  }

  testXmlelement () {
    this.validateIdentity('SELECT XMLELEMENT(NAME foo)');
    this.validateIdentity('SELECT XMLELEMENT(NAME foo, XMLATTRIBUTES(\'xyz\' AS bar))');
    this.validateIdentity('SELECT XMLELEMENT(NAME test, XMLATTRIBUTES(a, b)) FROM test');
    this.validateIdentity(
      'SELECT XMLELEMENT(NAME foo, XMLATTRIBUTES(CURRENT_DATE AS bar), \'cont\', \'ent\')',
    );
    this.validateIdentity(
      'SELECT XMLELEMENT(NAME "foo$bar", XMLATTRIBUTES(\'xyz\' AS "a&b"))',
    );
    this.validateIdentity(
      'SELECT XMLELEMENT(NAME foo, XMLATTRIBUTES(\'xyz\' AS bar), XMLELEMENT(NAME abc), XMLCOMMENT(\'test\'), XMLELEMENT(NAME xyz))',
    );
  }

  testAnalyze () {
    this.validateIdentity('ANALYZE TBL');
    this.validateIdentity('ANALYZE TBL(col1, col2)');
    this.validateIdentity('ANALYZE VERBOSE SKIP_LOCKED TBL(col1, col2)');
    this.validateIdentity('ANALYZE BUFFER_USAGE_LIMIT 1337 TBL');
  }

  testRecursiveCte () {
    for (const kind of ['BREADTH', 'DEPTH']) {
      this.validateIdentity(
        `WITH RECURSIVE search_tree(id, link, data) AS (SELECT t.id, t.link, t.data FROM tree AS t UNION ALL SELECT t.id, t.link, t.data FROM tree AS t, search_tree AS st WHERE t.id = st.link) SEARCH ${kind} FIRST BY id SET ordercol SELECT * FROM search_tree ORDER BY ordercol`,
      );
    }

    this.validateIdentity(
      'WITH RECURSIVE search_graph(id, link, data, depth) AS (SELECT g.id, g.link, g.data, 1 FROM graph AS g UNION ALL SELECT g.id, g.link, g.data, sg.depth + 1 FROM graph AS g, search_graph AS sg WHERE g.id = sg.link) CYCLE id SET is_cycle USING path SELECT * FROM search_graph',
    );
  }

  testJsonExtract () {
    for (const arrowOp of ['->', '->>']) {
      // Ensure arrow_op operator roundtrips int values as subscripts
      this.validateAll(
        `SELECT foo ${arrowOp} 1`,
        {
          write: {
            postgres: `SELECT foo ${arrowOp} 1`,
            duckdb: `SELECT foo ${arrowOp} '$[1]'`,
          },
        },
      );

      // Ensure arrow_op operator roundtrips string values that represent integers as keys
      this.validateAll(
        `SELECT foo ${arrowOp} '12'`,
        {
          write: {
            postgres: `SELECT foo ${arrowOp} '12'`,
            clickhouse: 'SELECT JSONExtractString(foo, \'12\')',
          },
        },
      );
    }
  }

  testUdt () {
    const validateUdt = (sql: string) => {
      narrowInstanceOf(this.validateIdentity(sql).args.to, Expression)?.assertIs(DataTypeExpr);
    };

    validateUdt('CAST(5 AS MyType)');
    validateUdt('CAST(5 AS "MyType")');
    validateUdt('CAST(5 AS MySchema.MyType)');
    validateUdt('CAST(5 AS "MySchema"."MyType")');
    validateUdt('CAST(5 AS MySchema."MyType")');
    validateUdt('CAST(5 AS "MyCatalog"."MySchema"."MyType")');
  }

  testRound () {
    this.validateIdentity('ROUND(x)');
    this.validateIdentity('ROUND(x, y)');
    this.validateIdentity('ROUND(CAST(x AS DOUBLE PRECISION))');
    this.validateIdentity('ROUND(CAST(x AS DECIMAL), 4)');
    this.validateIdentity('ROUND(CAST(x AS INT), 4)');
    this.validateAll(
      'ROUND(CAST(CAST(x AS DOUBLE PRECISION) AS DECIMAL), 4)',
      {
        read: {
          postgres: 'ROUND(x::DOUBLE, 4)',
          hive: 'ROUND(x::DOUBLE, 4)',
          bigquery: 'ROUND(x::DOUBLE, 4)',
        },
      },
    );
    this.validateAll('ROUND(CAST(x AS DECIMAL(18, 3)), 4)', { read: { duckdb: 'ROUND(x::DECIMAL, 4)' } });
  }

  testDatatype () {
    this.validateIdentity('CREATE TABLE foo (data XML)');
  }

  testLocks () {
    for (const keyType of [
      'FOR SHARE',
      'FOR UPDATE',
      'FOR NO KEY UPDATE',
      'FOR KEY SHARE',
    ]) {
      this.validateIdentity(`SELECT 1 FROM foo AS x ${keyType} OF x`);
    }
  }

  testGrant () {
    const grantCmds = [
      'GRANT SELECT ON TABLE users TO role1',
      'GRANT INSERT, DELETE ON TABLE orders TO user1',
      'GRANT SELECT ON employees TO manager WITH GRANT OPTION',
      'GRANT USAGE ON SCHEMA finance TO user2',
      'GRANT ALL PRIVILEGES ON DATABASE mydb TO PUBLIC',
      'GRANT CREATE ON SCHEMA public TO developer',
      'GRANT CONNECT ON DATABASE testdb TO readonly_user',
      'GRANT TEMPORARY ON DATABASE testdb TO temp_user',
      'GRANT TRIGGER ON orders TO audit_role',
      'GRANT REFERENCES ON products TO foreign_key_user',
      'GRANT TRUNCATE ON logs TO admin_role',
      'GRANT UPDATE(salary) ON employees TO hr_manager',
      'GRANT SELECT(id, name), UPDATE(email) ON customers TO customer_service',
    ];

    for (const sql of grantCmds) {
      this.validateIdentity(sql);
    }

    this.validateIdentity(
      'GRANT EXECUTE ON FUNCTION calculate_bonus(integer) TO analyst',
      'GRANT EXECUTE ON FUNCTION CALCULATE_BONUS(integer) TO analyst',
    );

    const advancedGrants = [
      'GRANT INSERT, DELETE ON ALL TABLES IN SCHEMA myschema TO user1',
      'GRANT developer_role TO john',
      'GRANT admin_role TO mary WITH ADMIN OPTION',
    ];

    for (const sql of advancedGrants) {
      this.validateIdentity(sql, undefined, { checkCommandWarning: true });
    }
  }

  testRevoke () {
    const revokeCmds = [
      'REVOKE SELECT ON TABLE users FROM role1',
      'REVOKE INSERT, DELETE ON TABLE orders FROM user1',
      'REVOKE USAGE ON SCHEMA finance FROM user2',
      'REVOKE ALL PRIVILEGES ON DATABASE mydb FROM PUBLIC',
      'REVOKE CREATE ON SCHEMA public FROM developer',
      'REVOKE CONNECT ON DATABASE testdb FROM readonly_user',
      'REVOKE TEMPORARY ON DATABASE testdb FROM temp_user',
      'REVOKE TRIGGER ON orders FROM audit_role',
      'REVOKE REFERENCES ON products FROM foreign_key_user',
      'REVOKE TRUNCATE ON logs FROM admin_role',
      'REVOKE USAGE ON SCHEMA finance FROM user2 CASCADE',
      'REVOKE SELECT ON TABLE orders FROM user1 RESTRICT',
      'REVOKE GRANT OPTION FOR SELECT ON employees FROM manager',
      'REVOKE GRANT OPTION FOR SELECT ON employees FROM manager RESTRICT',
      'REVOKE UPDATE(salary) ON employees FROM hr_manager',
      'REVOKE SELECT(id, name), UPDATE(email) ON customers FROM customer_service',
    ];

    for (const sql of revokeCmds) {
      this.validateIdentity(sql);
    }

    this.validateIdentity(
      'REVOKE EXECUTE ON FUNCTION calculate_bonus(integer) FROM analyst',
      'REVOKE EXECUTE ON FUNCTION CALCULATE_BONUS(integer) FROM analyst',
    );

    const advancedRevokeCmds = [
      'REVOKE INSERT, DELETE ON ALL TABLES IN SCHEMA myschema FROM user1',
      'REVOKE developer_role FROM john',
      'REVOKE admin_role FROM mary',
    ];

    for (const sql of advancedRevokeCmds) {
      this.validateIdentity(sql, undefined, { checkCommandWarning: true });
    }
  }

  testBeginTransaction () {
    this.validateAll(
      'BEGIN',
      {
        write: {
          postgres: 'BEGIN',
          presto: 'START TRANSACTION',
          trino: 'START TRANSACTION',
        },
      },
    );

    for (const keyword of ['TRANSACTION', 'WORK']) {
      for (const level of [
        'ISOLATION LEVEL SERIALIZABLE',
        'ISOLATION LEVEL READ COMMITTED',
        'NOT DEFFERABLE',
        'READ WRITE',
        'DEFERRABLE',
      ]) {
        this.validateIdentity(`BEGIN ${keyword} ${level}, ${level}`, `BEGIN ${level}, ${level}`).assertIs(TransactionExpr);
      }
    }
  }

  testIntervalSpan () {
    for (const timeStr of [
      '1 01:',
      '1 01:00',
      '1.5 01:',
      '-0.25 01:',
    ]) {
      this.validateIdentity(`INTERVAL '${timeStr}'`);
    }

    for (const timeStr of [
      '1 01:01:',
      '1 01:01:',
      '1 01:01:01',
      '1 01:01:01.01',
      '1.5 01:01:',
      '-0.25 01:01:',
    ]) {
      this.validateIdentity(`INTERVAL '${timeStr}'`);
    }

    // Ensure AND is not consumed as a unit following an omitted-span interval
    const dayTimeStr = 'a > INTERVAL \'1 00:00\' AND TRUE';
    this.validateIdentity(dayTimeStr, 'a > INTERVAL \'1 00:00\' AND TRUE');
  }
}

const t = new TestPostgres();
describe('TestPostgres', () => {
  test('testPostgres', () => t.testPostgres());
  test('testDdl', () => t.testDdl());
  test('testUnnest', () => t.testUnnest());
  test('testArrayOffset', () => t.testArrayOffset());
  test('testBoolOr', () => t.testBoolOr());
  test('testStringConcat', () => t.testStringConcat());
  test('testVariance', () => t.testVariance());
  test('testCorr', () => t.testCorr());
  test('testRegexpBinary', () => t.testRegexpBinary());
  test('testUnnestJsonArray', () => t.testUnnestJsonArray());
  test('testRowsFrom', () => t.testRowsFrom());
  test('testArrayLength', () => t.testArrayLength());
  test('testXmlelement', () => t.testXmlelement());
  test('testAnalyze', () => t.testAnalyze());
  test('testRecursiveCte', () => t.testRecursiveCte());
  test('testJsonExtract', () => t.testJsonExtract());
  test('testUdt', () => t.testUdt());
  test('testRound', () => t.testRound());
  test('testDatatype', () => t.testDatatype());
  test('testLocks', () => t.testLocks());
  test('testGrant', () => t.testGrant());
  test('testRevoke', () => t.testRevoke());
  test('testBeginTransaction', () => t.testBeginTransaction());
  test('testIntervalSpan', () => t.testIntervalSpan());
});
