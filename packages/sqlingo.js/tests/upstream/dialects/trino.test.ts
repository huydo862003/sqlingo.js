import {
  describe, test,
} from 'vitest';
import {
  Validator,
} from './validator';

class TestTrino extends Validator {
  override dialect = 'trino' as const;

  testTrino () {
    this.validateIdentity('REFRESH MATERIALIZED VIEW mynamespace.test_view');
    this.validateIdentity('JSON_QUERY(m.properties, \'lax $.area\' OMIT QUOTES NULL ON ERROR)');
    this.validateIdentity('JSON_EXTRACT(content, json_path)');
    this.validateIdentity('JSON_QUERY(content, \'lax $.HY.*\')');
    this.validateIdentity('JSON_QUERY(content, \'strict $.HY.*\' WITH WRAPPER)');
    this.validateIdentity('JSON_QUERY(content, \'strict $.HY.*\' WITH ARRAY WRAPPER)');
    this.validateIdentity('JSON_QUERY(content, \'strict $.HY.*\' WITH UNCONDITIONAL WRAPPER)');
    this.validateIdentity('JSON_QUERY(content, \'strict $.HY.*\' WITHOUT CONDITIONAL WRAPPER)');
    this.validateIdentity('JSON_QUERY(description, \'strict $.comment\' KEEP QUOTES)');
    this.validateIdentity(
      'JSON_QUERY(description, \'strict $.comment\' OMIT QUOTES ON SCALAR STRING)',
    );
    this.validateIdentity(
      'JSON_QUERY(content, \'strict $.HY.*\' WITH UNCONDITIONAL WRAPPER KEEP QUOTES)',
    );
    this.validateIdentity(
      'SELECT TIMESTAMP \'2012-10-31 01:00 -2\'',
      'SELECT CAST(\'2012-10-31 01:00 -2\' AS TIMESTAMP WITH TIME ZONE)',
    );
    this.validateIdentity(
      'SELECT TIMESTAMP \'2012-10-31 01:00 +2\'',
      'SELECT CAST(\'2012-10-31 01:00 +2\' AS TIMESTAMP WITH TIME ZONE)',
    );

    this.validateAll(
      'SELECT TIMESTAMP \'2012-10-31 01:00:00 +02:00\'',
      {
        write: {
          duckdb: 'SELECT CAST(\'2012-10-31 01:00:00 +02:00\' AS TIMESTAMPTZ)',
          trino: 'SELECT CAST(\'2012-10-31 01:00:00 +02:00\' AS TIMESTAMP WITH TIME ZONE)',
        },
      },
    );
    this.validateAll(
      'SELECT FORMAT(\'%s\', 123)',
      {
        write: {
          duckdb: 'SELECT FORMAT(\'{}\', 123)',
          snowflake: 'SELECT TO_CHAR(123)',
          trino: 'SELECT FORMAT(\'%s\', 123)',
        },
      },
    );

    this.validateIdentity(
      'SELECT * FROM tbl MATCH_RECOGNIZE (PARTITION BY id ORDER BY col MEASURES FIRST(col, 2) AS col1, LAST(col, 2) AS col2 PATTERN (B* A) DEFINE A AS col = 1)',
    );

    this.validateIdentity('SELECT VERSION()');
  }

  testListagg () {
    this.validateIdentity(
      'SELECT LISTAGG(DISTINCT col, \',\') WITHIN GROUP (ORDER BY col ASC) FROM tbl',
    );
    this.validateIdentity(
      'SELECT LISTAGG(col, \'; \' ON OVERFLOW ERROR) WITHIN GROUP (ORDER BY col ASC) FROM tbl',
    );
    this.validateIdentity(
      'SELECT LISTAGG(col, \'; \' ON OVERFLOW TRUNCATE WITH COUNT) WITHIN GROUP (ORDER BY col ASC) FROM tbl',
    );
    this.validateIdentity(
      'SELECT LISTAGG(col, \'; \' ON OVERFLOW TRUNCATE WITHOUT COUNT) WITHIN GROUP (ORDER BY col ASC) FROM tbl',
    );
    this.validateIdentity(
      'SELECT LISTAGG(col, \'; \' ON OVERFLOW TRUNCATE \'...\' WITH COUNT) WITHIN GROUP (ORDER BY col ASC) FROM tbl',
    );
    this.validateIdentity(
      'SELECT LISTAGG(col, \'; \' ON OVERFLOW TRUNCATE \'...\' WITHOUT COUNT) WITHIN GROUP (ORDER BY col ASC) FROM tbl',
    );
    this.validateIdentity(
      'SELECT LISTAGG(col) WITHIN GROUP (ORDER BY col DESC) FROM tbl',
      'SELECT LISTAGG(col, \',\') WITHIN GROUP (ORDER BY col DESC) FROM tbl',
    );
  }

  testTrim () {
    this.validateIdentity('SELECT TRIM(\'!\' FROM \'!foo!\')');
    this.validateIdentity('SELECT TRIM(BOTH \'$\' FROM \'$var$\')');
    this.validateIdentity('SELECT TRIM(TRAILING \'ER\' FROM UPPER(\'worker\'))');
    this.validateIdentity(
      'SELECT TRIM(LEADING FROM \'  abcd\')',
      'SELECT LTRIM(\'  abcd\')',
    );
    this.validateIdentity(
      'SELECT TRIM(\'!foo!\', \'!\')',
      'SELECT TRIM(\'!\' FROM \'!foo!\')',
    );
  }

  testDdl () {
    this.validateIdentity('ALTER TABLE users RENAME TO people');
    this.validateIdentity('ALTER TABLE IF EXISTS users RENAME TO people');
    this.validateIdentity('ALTER TABLE users ADD COLUMN zip VARCHAR');
    this.validateIdentity('ALTER TABLE IF EXISTS users ADD COLUMN IF NOT EXISTS zip VARCHAR');
    this.validateIdentity('ALTER TABLE users DROP COLUMN zip');
    this.validateIdentity('ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS zip');
    this.validateIdentity('ALTER TABLE users RENAME COLUMN id TO user_id');
    this.validateIdentity('ALTER TABLE IF EXISTS users RENAME COLUMN IF EXISTS id TO user_id');
    this.validateIdentity('ALTER TABLE users ALTER COLUMN id SET DATA TYPE BIGINT');
    this.validateIdentity('ALTER TABLE users ALTER COLUMN id DROP NOT NULL');
    this.validateIdentity(
      'ALTER TABLE people SET AUTHORIZATION alice', undefined, { checkCommandWarning: true },
    );
    this.validateIdentity(
      'ALTER TABLE people SET AUTHORIZATION ROLE PUBLIC', undefined, { checkCommandWarning: true },
    );
    this.validateIdentity(
      'ALTER TABLE people SET PROPERTIES x = \'y\'', undefined, { checkCommandWarning: true },
    );
    this.validateIdentity(
      'ALTER TABLE people SET PROPERTIES foo = 123, \'foo bar\' = 456',
      undefined,
      { checkCommandWarning: true },
    );
    this.validateIdentity(
      'ALTER TABLE people SET PROPERTIES x = DEFAULT', undefined, { checkCommandWarning: true },
    );
    this.validateIdentity('ALTER VIEW people RENAME TO users');
    this.validateIdentity(
      'ALTER VIEW people SET AUTHORIZATION alice', undefined, { checkCommandWarning: true },
    );
    this.validateIdentity('CREATE SCHEMA foo WITH (LOCATION=\'s3://bucket/foo\')');
    this.validateIdentity(
      'CREATE TABLE foo.bar WITH (LOCATION=\'s3://bucket/foo/bar\') AS SELECT 1',
    );

    // Hive connector syntax (partitioned_by)
    this.validateIdentity(
      'CREATE TABLE foo (a VARCHAR, b INTEGER, c DATE) WITH (PARTITIONED_BY=ARRAY[\'a\', \'b\'])',
    );
    this.validateIdentity(
      'CREATE TABLE "foo" ("a" VARCHAR, "b" INTEGER, "c" DATE) WITH (PARTITIONED_BY=ARRAY[\'a\', \'b\'])',
      undefined,
      { identify: true },
    );

    // Iceberg connector syntax (partitioning, can contain Iceberg transform expressions)
    this.validateIdentity(
      'CREATE TABLE foo (a VARCHAR, b INTEGER, c DATE) WITH (PARTITIONING=ARRAY[\'a\', \'bucket(4, b)\', \'month(c)\'])',
    );
    this.validateIdentity(
      'CREATE TABLE "foo" ("a" VARCHAR, "b" INTEGER, "c" DATE) WITH (PARTITIONING=ARRAY[\'a\', \'bucket(4, b)\', \'month(c)\'])',
      undefined,
      { identify: true },
    );
  }

  testAnalyze () {
    this.validateIdentity('ANALYZE tbl');
    this.validateIdentity('ANALYZE tbl WITH (prop1=val1, prop2=val2)');
  }

  testJsonValue () {
    this.validateIdentity(
      'JSON_VALUE(jl.extra_attributes, \'lax $.amount_source\' RETURNING VARCHAR)',
    );

    const jsonDoc = '\'{"item": "shoes", "price": "49.95"}\'';
    this.validateIdentity(`SELECT JSON_VALUE(${jsonDoc}, 'strict $.price')`);
    this.validateIdentity(
      `SELECT JSON_VALUE(${jsonDoc}, 'lax $.price' RETURNING DECIMAL(4, 2))`,
    );

    for (const onOption of ['NULL', 'ERROR', 'DEFAULT 1']) {
      this.validateIdentity(
        `SELECT JSON_VALUE(${jsonDoc}, 'lax $.price' RETURNING DECIMAL(4, 2) ${onOption} ON EMPTY ${onOption} ON ERROR) AS price`,
      );
    }
  }
}

const t = new TestTrino();

describe('TestTrino', () => {
  test('trino', () => t.testTrino());
  test('listagg', () => t.testListagg());
  test('trim', () => t.testTrim());
  test('ddl', () => t.testDdl());
  test('analyze', () => t.testAnalyze());
  test('jsonValue', () => t.testJsonValue());
});
