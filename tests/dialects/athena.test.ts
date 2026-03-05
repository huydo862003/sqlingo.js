import {
  describe, test, expect,
} from 'vitest';
import {
  PartitionedByPropertyExpr, PartitionedByBucketExpr, PartitionByTruncateExpr, SchemaExpr,
} from '../../src/expressions';
import { Validator } from './validator';

class TestAthena extends Validator {
  override dialect = 'athena' as const;

  testAthena () {
    this.validateIdentity('SELECT \'\\d+\'');
    this.validateIdentity('SELECT \'foo\'\'bar\'');
    this.validateIdentity(
      'CREATE TABLE IF NOT EXISTS t (name STRING) LOCATION \'s3://bucket/tmp/mytable/\' TBLPROPERTIES (\'table_type\'=\'iceberg\', \'FORMAT\'=\'parquet\')',
    );
    this.validateIdentity(
      'UNLOAD (SELECT name1, address1, comment1, key1 FROM table1) '
      + 'TO \'s3://amzn-s3-demo-bucket/ partitioned/\' '
      + 'WITH (format = \'TEXTFILE\', partitioned_by = ARRAY[\'key1\'])',
      undefined,
      { checkCommandWarning: true },
    );
    this.validateIdentity(
      `USING EXTERNAL FUNCTION some_function(input VARBINARY)
            RETURNS VARCHAR
                LAMBDA 'some-name'
            SELECT
            some_function(1)`,
      undefined,
      { checkCommandWarning: true },
    );

    this.validateIdentity(
      '/* leading comment */CREATE SCHEMA foo',
      '/* leading comment */ CREATE SCHEMA `foo`',
      { identify: true },
    );
    this.validateIdentity(
      '/* leading comment */SELECT * FROM foo',
      '/* leading comment */ SELECT * FROM "foo"',
      { identify: true },
    );
  }

  testDdl () {
    this.validateIdentity('CREATE EXTERNAL TABLE foo (id INT) COMMENT \'test comment\'');
    this.validateIdentity(
      String.raw`CREATE EXTERNAL TABLE george.t (id INT COMMENT 'foo \\ bar') LOCATION 's3://my-bucket/'`,
    );
    this.validateIdentity(
      String.raw`CREATE EXTERNAL TABLE my_table (id BIGINT COMMENT 'this is the row\'s id') LOCATION 's3://my-s3-bucket'`,
    );
    this.validateIdentity(
      'CREATE EXTERNAL TABLE foo (id INT, val STRING) CLUSTERED BY (id, val) INTO 10 BUCKETS',
    );
    this.validateIdentity(
      'CREATE EXTERNAL TABLE foo (id INT, val STRING) STORED AS PARQUET LOCATION \'s3://foo\' TBLPROPERTIES (\'has_encryped_data\'=\'true\', \'classification\'=\'test\')',
    );
    this.validateIdentity(
      'CREATE EXTERNAL TABLE IF NOT EXISTS foo (a INT, b STRING) ROW FORMAT SERDE \'org.openx.data.jsonserde.JsonSerDe\' WITH SERDEPROPERTIES (\'case.insensitive\'=\'FALSE\') LOCATION \'s3://table/path\'',
    );
    this.validateIdentity(
      'CREATE EXTERNAL TABLE x (y INT) ROW FORMAT SERDE \'serde\' ROW FORMAT DELIMITED FIELDS TERMINATED BY \'1\' WITH SERDEPROPERTIES (\'input.regex\'=\'\')',
    );
    this.validateIdentity(
      'CREATE EXTERNAL TABLE `my_table` (`a7` ARRAY<DATE>) ROW FORMAT SERDE \'a\' STORED AS INPUTFORMAT \'b\' OUTPUTFORMAT \'c\' LOCATION \'d\' TBLPROPERTIES (\'e\'=\'f\')',
    );

    this.validateIdentity(
      'CREATE TABLE iceberg_table (`id` BIGINT, `data` STRING, category STRING) PARTITIONED BY (category, BUCKET(16, id)) LOCATION \'s3://amzn-s3-demo-bucket/your-folder/\' TBLPROPERTIES (\'table_type\'=\'ICEBERG\', \'write_compression\'=\'snappy\')',
    );
    this.validateIdentity(
      'CREATE OR REPLACE TABLE iceberg_table (`id` BIGINT, `data` STRING, category STRING) PARTITIONED BY (category, BUCKET(16, id)) LOCATION \'s3://amzn-s3-demo-bucket/your-folder/\' TBLPROPERTIES (\'table_type\'=\'ICEBERG\', \'write_compression\'=\'snappy\')',
    );

    this.validateIdentity(
      'CREATE TABLE foo WITH (table_type=\'ICEBERG\', location=\'s3://foo/\', format=\'orc\', partitioning=ARRAY[\'bucket(id, 5)\']) AS SELECT * FROM a',
    );
    this.validateIdentity(
      'CREATE TABLE foo WITH (table_type=\'HIVE\', external_location=\'s3://foo/\', format=\'parquet\', partitioned_by=ARRAY[\'ds\']) AS SELECT * FROM a',
    );
    this.validateIdentity(
      'CREATE TABLE foo AS WITH foo AS (SELECT a, b FROM bar) SELECT * FROM foo',
    );

    this.validateIdentity(
      'ALTER TABLE `foo`.`bar` ADD COLUMN `end_ts` BIGINT',
      'ALTER TABLE `foo`.`bar` ADD COLUMNS (`end_ts` BIGINT)',
    );
    this.validateIdentity('ALTER TABLE `foo` DROP COLUMN `id`');
  }

  testDml () {
    this.validateAll(
      'SELECT CAST(ds AS VARCHAR) AS ds FROM (VALUES (\'2022-01-01\')) AS t(ds)',
      {
        read: { '': 'SELECT CAST(ds AS STRING) AS ds FROM (VALUES (\'2022-01-01\')) AS t(ds)' },
        write: {
          hive: 'SELECT CAST(ds AS STRING) AS ds FROM (VALUES (\'2022-01-01\')) AS t(ds)',
          trino: 'SELECT CAST(ds AS VARCHAR) AS ds FROM (VALUES (\'2022-01-01\')) AS t(ds)',
          athena: 'SELECT CAST(ds AS VARCHAR) AS ds FROM (VALUES (\'2022-01-01\')) AS t(ds)',
        },
      },
    );
  }

  testDdlQuoting () {
    this.validateIdentity('CREATE SCHEMA `foo`');
    this.validateIdentity('CREATE SCHEMA foo');

    this.validateIdentity('CREATE EXTERNAL TABLE `foo` (`id` INT) LOCATION \'s3://foo/\'');
    this.validateIdentity('CREATE EXTERNAL TABLE foo (id INT) LOCATION \'s3://foo/\'');
    this.validateIdentity(
      'CREATE EXTERNAL TABLE foo (id INT) LOCATION \'s3://foo/\'',
      'CREATE EXTERNAL TABLE `foo` (`id` INT) LOCATION \'s3://foo/\'',
      { identify: true },
    );

    this.validateIdentity('CREATE TABLE foo AS SELECT * FROM a');
    this.validateIdentity('CREATE TABLE "foo" AS SELECT * FROM "a"');

    this.validateIdentity('DROP VIEW IF EXISTS "foo"."bar"');
    this.validateIdentity('CREATE VIEW "foo" AS SELECT "id" FROM "tbl"');
    this.validateIdentity(
      'CREATE VIEW foo AS SELECT id FROM tbl',
      'CREATE VIEW "foo" AS SELECT "id" FROM "tbl"',
      { identify: true },
    );

    this.validateIdentity('DROP TABLE `foo`');
    this.validateIdentity('DROP TABLE foo');
    this.validateIdentity(
      'DROP TABLE foo',
      'DROP TABLE `foo`',
      { identify: true },
    );

    this.validateIdentity('CREATE VIEW "foo" AS SELECT "id" FROM "tbl"');
    this.validateIdentity('CREATE VIEW foo AS SELECT id FROM tbl');
    this.validateIdentity(
      'CREATE VIEW foo AS SELECT id FROM tbl',
      'CREATE VIEW "foo" AS SELECT "id" FROM "tbl"',
      { identify: true },
    );

    this.validateIdentity('CREATE SCHEMA "foo"', 'CREATE SCHEMA `foo`');
    this.validateIdentity('DROP TABLE "foo"', 'DROP TABLE `foo`');
    this.validateIdentity(
      'DESCRIBE foo.bar',
      'DESCRIBE `foo`.`bar`',
      { identify: true },
    );
    this.validateIdentity(
      'CREATE TABLE "foo" AS WITH "foo" AS (SELECT "a", "b" FROM "bar") SELECT * FROM "foo"',
    );
  }

  testDmlQuoting () {
    this.validateIdentity('SELECT a AS foo FROM tbl');
    this.validateIdentity('SELECT "a" AS "foo" FROM "tbl"');

    this.validateIdentity('INSERT INTO foo (id) VALUES (1)');
    this.validateIdentity('INSERT INTO "foo" ("id") VALUES (1)');

    this.validateIdentity('UPDATE foo SET id = 3 WHERE id = 7');
    this.validateIdentity('UPDATE "foo" SET "id" = 3 WHERE "id" = 7');

    this.validateIdentity('DELETE FROM foo WHERE id > 10');
    this.validateIdentity('DELETE FROM "foo" WHERE "id" > 10');

    this.validateIdentity('WITH foo AS (SELECT a, b FROM bar) SELECT * FROM foo');
    this.validateIdentity(
      'WITH foo AS (SELECT a, b FROM bar) SELECT * FROM foo',
      'WITH "foo" AS (SELECT "a", "b" FROM "bar") SELECT * FROM "foo"',
      { identify: true },
    );
  }

  testParsePartitionedByReturnsIcebergTransforms () {
    const parsed = this.parseOne(
      '(a, bucket(4, b), truncate(3, c), month(d))',
      { into: 'PartitionedByProperty' },
    );

    expect(parsed).toBeInstanceOf(PartitionedByPropertyExpr);
    const prop = parsed as PartitionedByPropertyExpr;
    expect(prop.args.this).toBeInstanceOf(SchemaExpr);
    const schema = prop.args.this as SchemaExpr;
    expect(schema.args.expressions?.some((n) => n instanceof PartitionedByBucketExpr)).toBe(true);
    expect(schema.args.expressions?.some((n) => n instanceof PartitionByTruncateExpr)).toBe(true);
  }
}

const t = new TestAthena();
describe('TestAthena', () => {
  test('testAthena', () => t.testAthena());
  test('testDdl', () => t.testDdl());
  test('testDml', () => t.testDml());
  test('testDdlQuoting', () => t.testDdlQuoting());
  test('testDmlQuoting', () => t.testDmlQuoting());
  test('testParsePartitionedByReturnsIcebergTransforms', () => t.testParsePartitionedByReturnsIcebergTransforms());
});
