import {
  describe, test,
} from 'vitest';
import { Validator } from './validator';

class TestRisingWave extends Validator {
  override dialect = 'risingwave' as const;

  testRisingwave () {
    this.validateAll(
      'SELECT a FROM tbl',
      {
        read: {
          '': 'SELECT a FROM tbl FOR UPDATE',
        },
      },
    );
    this.validateIdentity(
      'CREATE SOURCE from_kafka (*, gen_i32_field INT AS int32_field + 2, gen_i64_field INT AS int64_field + 2, WATERMARK FOR time_col AS time_col - INTERVAL \'5 SECOND\') INCLUDE header foo VARCHAR AS myheader INCLUDE key AS mykey WITH (connector=\'kafka\', topic=\'my_topic\') FORMAT PLAIN ENCODE PROTOBUF (A=1, B=2) KEY ENCODE PROTOBUF (A=3, B=4)',
    );
    this.validateIdentity(
      'CREATE SINK my_sink AS SELECT * FROM A WITH (connector=\'kafka\', topic=\'my_topic\') FORMAT PLAIN ENCODE PROTOBUF (A=1, B=2) KEY ENCODE PROTOBUF (A=3, B=4)',
    );
    this.validateIdentity(
      'WITH t1 AS MATERIALIZED (SELECT 1), t2 AS NOT MATERIALIZED (SELECT 2) SELECT * FROM t1, t2',
    );
  }

  testDatatypes () {
    this.validateIdentity('SELECT CAST(NULL AS MAP(VARCHAR, INT)) AS map_column');
    this.validateIdentity(
      'SELECT NULL::MAP<VARCHAR, INT> AS map_column',
      'SELECT CAST(NULL AS MAP(VARCHAR, INT)) AS map_column',
    );
    this.validateIdentity('CREATE TABLE t (map_col MAP(VARCHAR, INT))');
    this.validateIdentity(
      'CREATE TABLE t (map_col MAP<VARCHAR, INT>)',
      'CREATE TABLE t (map_col MAP(VARCHAR, INT))',
    );
  }
}

const t = new TestRisingWave();
describe('TestRisingWave', () => {
  test('risingwave', () => t.testRisingwave());
  test('datatypes', () => t.testDatatypes());
});
