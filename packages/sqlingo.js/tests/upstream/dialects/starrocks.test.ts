import {
  describe, test, expect,
} from 'vitest';
import {
  UnsupportedError,
} from '../../../src/index';
import {
  BracketExpr, ClusterExpr, column, FlattenExpr, LiteralExpr,
} from '../../../src/expressions';
import type {
  SelectExpr,
} from '../../../src/expressions';
import {
  Validator,
} from './validator';

class TestStarrocks extends Validator {
  override dialect = 'starrocks' as const;

  testStarrocks () {
    const expr = this.validateIdentity('arr[1]');
    expect((expr as BracketExpr).args.expressions![0].toString()).toBe('0');

    this.validateIdentity('SELECT ARRAY_JOIN([1, 3, 5, NULL], \'_\', \'NULL\')');
    this.validateIdentity('SELECT ARRAY_JOIN([1, 3, 5, NULL], \'_\')');
    this.validateIdentity('ALTER TABLE a SWAP WITH b');
    this.validateIdentity('SELECT ARRAY_AGG(a) FROM x');
    this.validateIdentity('SELECT ST_POINT(10, 20)');
    this.validateIdentity('SELECT ST_DISTANCE_SPHERE(10.1, 20.2, 30.3, 40.4)');
    this.validateIdentity('ARRAY_FLATTEN(arr)').assertIs(FlattenExpr);

    this.validateAll(
      'SELECT * FROM t WHERE cond',
      {
        read: {
          '': 'SELECT * FROM t WHERE cond IS TRUE',
          starrocks: 'SELECT * FROM t WHERE cond',
        },
      },
    );

    this.validateIdentity('CURRENT_VERSION()');
  }

  testDdl () {
    this.validateIdentity('INSERT OVERWRITE my_table SELECT * FROM other_table');
    this.validateIdentity('CREATE TABLE t (c INT) COMMENT \'c\'');

    const ddlSqls = [
      'PARTITION BY (col1, col2)',
      'PARTITION BY DATE_TRUNC(\'DAY\', col2), col1',
      'PARTITION BY FROM_UNIXTIME(col2)',
      'DISTRIBUTED BY HASH (col1) BUCKETS 1',
      'DISTRIBUTED BY HASH (col1)',
      'DISTRIBUTED BY RANDOM BUCKETS 1',
      'DISTRIBUTED BY RANDOM',
      'DISTRIBUTED BY HASH (col1) ORDER BY (col1)',
      'DISTRIBUTED BY HASH (col1) PROPERTIES (\'replication_num\'=\'1\')',
      'PRIMARY KEY (col1) DISTRIBUTED BY HASH (col1)',
      'DUPLICATE KEY (col1, col2) DISTRIBUTED BY HASH (col1)',
      'UNIQUE KEY (col1, col2) PARTITION BY RANGE (col1) (START (\'2024-01-01\') END (\'2024-01-31\') EVERY (INTERVAL 1 DAY)) DISTRIBUTED BY HASH (col1)',
      'UNIQUE KEY (col1, col2) PARTITION BY RANGE (col1, col2) (START (\'1\') END (\'10\') EVERY (1), START (\'10\') END (\'100\') EVERY (10)) DISTRIBUTED BY HASH (col1)',
      'ORDER BY (col1, col2)',
      'DISTRIBUTED BY HASH (col1) ROLLUP (r1(event_day, siteid), r2(event_day, citycode), r3(event_day))',
      'DISTRIBUTED BY HASH (col1) ROLLUP (r1(col2))',
      'DISTRIBUTED BY HASH (col1) ROLLUP (`r1`(`col2`))',
      'DISTRIBUTED BY HASH (col1) ROLLUP (r1(col2) FROM base_index)',
      'DISTRIBUTED BY HASH (col1) ROLLUP (r1(col2) PROPERTIES (\'storage_type\'=\'column\'))',
      'DISTRIBUTED BY HASH (col1) ROLLUP (r1(col2) FROM base_index PROPERTIES (\'k\'=\'v\'))',
      'DISTRIBUTED BY HASH (col1) ROLLUP (r1(col2) PROPERTIES (\'k1\'=\'v1\', \'k2\'=\'v2\'))',
    ];

    for (const properties of ddlSqls) {
      this.validateIdentity(`CREATE TABLE foo (col1 BIGINT, col2 BIGINT) ${properties}`);
      this.validateIdentity(
        `CREATE TABLE foo (col1 BIGINT, col2 BIGINT) ENGINE=OLAP ${properties}`,
      );
    }

    // Test the different wider DECIMAL types
    this.validateIdentity(
      'CREATE TABLE foo (col0 DECIMAL(9, 1), col1 DECIMAL32(9, 1), col2 DECIMAL64(18, 10), col3 DECIMAL128(38, 10)) DISTRIBUTED BY HASH (col1) BUCKETS 1',
    );
    this.validateIdentity(
      'CREATE TABLE foo (col1 LARGEINT) DISTRIBUTED BY HASH (col1) BUCKETS 1',
    );
    this.validateIdentity(
      'CREATE VIEW foo (foo_col1) SECURITY NONE AS SELECT bar_col1 FROM bar',
    );

    // Test ROLLUP property
    this.validateAll(
      'CREATE TABLE foo (col1 BIGINT, col2 BIGINT) ROLLUP (r1(col1, col2), r2(col1))',
      {
        write: {
          starrocks: 'CREATE TABLE foo (col1 BIGINT, col2 BIGINT) ROLLUP (r1(col1, col2), r2(col1))',
          spark: 'CREATE TABLE foo (col1 BIGINT, col2 BIGINT)',
          duckdb: 'CREATE TABLE foo (col1 BIGINT, col2 BIGINT)',
          postgres: 'CREATE TABLE foo (col1 BIGINT, col2 BIGINT)',
        },
      },
    );

    const multiColumnCluster = new ClusterExpr({
      expressions: [
        column({ col: 'c' }),
        column({ col: 'd' }),
      ],
    });

    expect(multiColumnCluster.sql({ dialect: 'starrocks' })).toBe('ORDER BY (c, d)');

    const singleColumnCluster = new ClusterExpr({
      expressions: [column({ col: 'c' })],
    });

    expect(singleColumnCluster.sql({ dialect: 'starrocks' })).toBe('ORDER BY (c)');

    const mvProperties = [
      // partitioning in MV
      'PARTITION BY (DATE_FUNC(ts), region) REFRESH ASYNC',
      'PARTITION BY (DATE_TRUNC(\'DAY\', ts)) REFRESH ASYNC',
      'PARTITION BY (col1, col2) REFRESH ASYNC',
      // MV: Refresh trigger property
      'REFRESH ASYNC',
      'REFRESH IMMEDIATE',
      'REFRESH DEFERRED',
      'REFRESH DEFERRED ASYNC',
      'REFRESH IMMEDIATE ASYNC',
      'REFRESH DEFERRED MANUAL',
      'REFRESH IMMEDIATE MANUAL',
      'REFRESH IMMEDIATE START (\'2025-01-01 00:00:00\') EVERY (INTERVAL 5 MINUTE)',
      'REFRESH IMMEDIATE ASYNC EVERY (INTERVAL 5 MINUTE)',
      'REFRESH DEFERRED START (\'2025-01-01 00:00:00\') EVERY (INTERVAL 5 MINUTE)',
      'REFRESH DEFERRED ASYNC EVERY (INTERVAL 5 MINUTE)',
      'REFRESH ASYNC START (\'2025-01-01 00:00:00\') EVERY (INTERVAL 5 MINUTE)',
      'REFRESH ASYNC EVERY (INTERVAL 5 MINUTE)',
    ];

    for (const properties of mvProperties) {
      this.validateIdentity(`CREATE MATERIALIZED VIEW mv ${properties} AS SELECT 1`);
    }

    // RENAME table without TO keyword
    this.validateIdentity('ALTER TABLE t1 RENAME t2');
  }

  testIdentity () {
    this.validateIdentity('SELECT CAST(`a`.`b` AS INT) FROM foo');
    this.validateIdentity('SELECT APPROX_COUNT_DISTINCT(a) FROM x');
    this.validateIdentity('SELECT [1, 2, 3]');
    this.validateIdentity(
      'SELECT CAST(PARSE_JSON(fieldvalue) -> \'00000000-0000-0000-0000-00000000\' AS VARCHAR) AS `code` FROM (SELECT \'{"00000000-0000-0000-0000-00000000":"code01"}\') AS t(fieldvalue)',
    );
    this.validateIdentity(
      'SELECT text FROM example_table', 'SELECT `text` FROM example_table',
    );
  }

  testTime () {
    this.validateIdentity('TIMESTAMP(\'2022-01-01\')');
    this.validateIdentity(
      'SELECT DATE_DIFF(\'SECOND\', \'2010-11-30 23:59:59\', \'2010-11-30 20:58:59\')',
    );
    this.validateIdentity(
      'SELECT DATE_DIFF(\'MINUTE\', \'2010-11-30 23:59:59\', \'2010-11-30 20:58:59\')',
    );
  }

  testRegex () {
    this.validateAll(
      'SELECT REGEXP(abc, \'%foo%\')',
      {
        read: {
          mysql: 'SELECT REGEXP_LIKE(abc, \'%foo%\')',
          starrocks: 'SELECT REGEXP(abc, \'%foo%\')',
        },
        write: {
          mysql: 'SELECT REGEXP_LIKE(abc, \'%foo%\')',
        },
      },
    );
  }

  testUnnest () {
    this.validateIdentity(
      'SELECT student, score, t.unnest FROM tests CROSS JOIN LATERAL UNNEST(scores) AS t',
      'SELECT student, score, t.unnest FROM tests CROSS JOIN LATERAL UNNEST(scores) AS t(unnest)',
    );
    this.validateAll(
      'SELECT student, score, unnest FROM tests CROSS JOIN LATERAL UNNEST(scores)',
      {
        write: {
          spark: 'SELECT student, score, unnest FROM tests LATERAL VIEW EXPLODE(scores) unnest AS unnest',
          starrocks: 'SELECT student, score, unnest FROM tests CROSS JOIN LATERAL UNNEST(scores) AS unnest(unnest)',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM UNNEST(array[\'John\', \'Jane\', \'Jim\', \'Jamie\'], array[24, 25, 26, 27]) AS t(name, age)',
      {
        write: {
          postgres: 'SELECT * FROM UNNEST(ARRAY[\'John\', \'Jane\', \'Jim\', \'Jamie\'], ARRAY[24, 25, 26, 27]) AS t(name, age)',
          spark: 'SELECT * FROM INLINE(ARRAYS_ZIP(ARRAY(\'John\', \'Jane\', \'Jim\', \'Jamie\'), ARRAY(24, 25, 26, 27))) AS t(name, age)',
          starrocks: 'SELECT * FROM UNNEST([\'John\', \'Jane\', \'Jim\', \'Jamie\'], [24, 25, 26, 27]) AS t(name, age)',
        },
      },
    );

    // Use UNNEST to convert into multiple columns
    this.validateAll(
      'SELECT id, t.type, t.scores FROM example_table, unnest(split(type, ";"), scores) AS t(type,scores)',
      {
        write: {
          postgres: 'SELECT id, t.type, t.scores FROM example_table, UNNEST(SPLIT(type, \';\'), scores) AS t(type, scores)',
          spark: 'SELECT id, t.type, t.scores FROM example_table LATERAL VIEW INLINE(ARRAYS_ZIP(SPLIT(type, CONCAT(\'\\\\Q\', \';\', \'\\\\E\')), scores)) t AS type, scores',
          databricks: 'SELECT id, t.type, t.scores FROM example_table LATERAL VIEW INLINE(ARRAYS_ZIP(SPLIT(type, CONCAT(\'\\\\Q\', \';\', \'\\\\E\')), scores)) t AS type, scores',
          starrocks: 'SELECT id, t.type, t.scores FROM example_table, UNNEST(SPLIT(type, \';\'), scores) AS t(type, scores)',
          hive: UnsupportedError,
        },
      },
    );

    this.validateAll(
      'SELECT id, t.type, t.scores FROM example_table_2 CROSS JOIN LATERAL unnest(split(type, ";"), scores) AS t(type,scores)',
      {
        write: {
          spark: 'SELECT id, t.type, t.scores FROM example_table_2 LATERAL VIEW INLINE(ARRAYS_ZIP(SPLIT(type, CONCAT(\'\\\\Q\', \';\', \'\\\\E\')), scores)) t AS type, scores',
          starrocks: 'SELECT id, t.type, t.scores FROM example_table_2 CROSS JOIN LATERAL UNNEST(SPLIT(type, \';\'), scores) AS t(type, scores)',
          hive: UnsupportedError,
        },
      },
    );

    const lateralExplodeSqls = [
      'SELECT id, t.col FROM tbl, UNNEST(scores) AS t(col)',
      'SELECT id, t.col FROM tbl CROSS JOIN LATERAL UNNEST(scores) AS t(col)',
    ];

    for (const sql of lateralExplodeSqls) {
      this.validateAll(
        sql,
        {
          write: {
            starrocks: sql,
            spark: 'SELECT id, t.col FROM tbl LATERAL VIEW EXPLODE(scores) t AS col',
          },
        },
      );
    }
  }

  testAnalyze () {
    this.validateIdentity('ANALYZE TABLE TBL(c1, c2) PROPERTIES (\'prop1\'=val1)');
    this.validateIdentity('ANALYZE FULL TABLE TBL(c1, c2) PROPERTIES (\'prop1\'=val1)');
    this.validateIdentity('ANALYZE SAMPLE TABLE TBL(c1, c2) PROPERTIES (\'prop1\'=val1)');
    this.validateIdentity('ANALYZE TABLE TBL(c1, c2) WITH SYNC MODE PROPERTIES (\'prop1\'=val1)');
    this.validateIdentity(
      'ANALYZE TABLE TBL(c1, c2) WITH ASYNC MODE PROPERTIES (\'prop1\'=val1)',
    );
    this.validateIdentity(
      'ANALYZE TABLE TBL UPDATE HISTOGRAM ON c1, c2 PROPERTIES (\'prop1\'=val1)',
    );
    this.validateIdentity(
      'ANALYZE TABLE TBL UPDATE HISTOGRAM ON c1, c2 WITH 5 BUCKETS PROPERTIES (\'prop1\'=val1)',
    );
    this.validateIdentity(
      'ANALYZE TABLE TBL UPDATE HISTOGRAM ON c1, c2 WITH SYNC MODE WITH 5 BUCKETS PROPERTIES (\'prop1\'=val1)',
    );
    this.validateIdentity(
      'ANALYZE TABLE TBL UPDATE HISTOGRAM ON c1, c2 WITH ASYNC MODE WITH 5 BUCKETS PROPERTIES (\'prop1\'=val1)',
    );
  }

  testBetween () {
    this.validateAll(
      'SELECT * FROM t WHERE a BETWEEN 1 AND 5',
      {
        write: {
          starrocks: 'SELECT * FROM t WHERE a BETWEEN 1 AND 5',
          mysql: 'SELECT * FROM t WHERE a BETWEEN 1 AND 5',
        },
      },
    );
    this.validateIdentity('SELECT a BETWEEN 1 AND 5 FROM t');
    this.validateIdentity(
      'DELETE FROM t WHERE a BETWEEN b AND c',
      'DELETE FROM t WHERE a >= b AND a <= c',
    );
    this.validateIdentity(
      'DELETE FROM t WHERE a BETWEEN 1 AND 10 AND b BETWEEN 20 AND 30 OR c BETWEEN \'x\' AND \'z\'',
      'DELETE FROM t WHERE a >= 1 AND a <= 10 AND b >= 20 AND b <= 30 OR c >= \'x\' AND c <= \'z\'',
    );
  }

  testPartition () {
    // Column-based partitioning
    for (const cols of ['col1', 'col1, col2']) {
      this.validateIdentity(
        `CREATE TABLE test_table (col1 INT, col2 DATE) PARTITION BY (${cols})`,
      );
      this.validateIdentity(
        `CREATE TABLE test_table (col1 INT, col2 DATE) PARTITION BY ${cols}`,
        `CREATE TABLE test_table (col1 INT, col2 DATE) PARTITION BY (${cols})`,
      );
    }

    // Expression-based partitioning
    this.validateIdentity(
      'CREATE TABLE test_table (col2 DATE) PARTITION BY DATE_TRUNC(\'DAY\', col2)',
    );
    this.validateIdentity(
      'CREATE TABLE test_table (col2 BIGINT) PARTITION BY FROM_UNIXTIME(col2, \'%Y%m%d\')',
    );
    this.validateIdentity(
      'CREATE TABLE test_table (col1 STRING, col2 BIGINT) PARTITION BY FROM_UNIXTIME(col2, \'%Y%m%d\'), col1',
    );
    this.validateIdentity(
      'CREATE TABLE test_table (col1 BIGINT, col2 DATE) PARTITION BY FROM_UNIXTIME(col2, \'%Y%m%d\'), DATE_TRUNC(\'DAY\', col1)',
    );

    // LIST partitioning
    this.validateIdentity(
      'CREATE TABLE test_table (col1 STRING) PARTITION BY LIST (col1) (PARTITION pLos_Angeles VALUES IN (\'Los Angeles\'), PARTITION pSan_Francisco VALUES IN (\'San Francisco\'))',
    );

    // Multi-column LIST partitioning
    this.validateIdentity(
      'CREATE TABLE test_table (col1 DATE, col2 STRING) PARTITION BY LIST (col1, col2) (PARTITION p1 VALUES IN ((\'2022-04-01\', \'LA\'), (\'2022-04-01\', \'SF\')))',
    );

    // RANGE partitioning with explicit values
    this.validateIdentity(
      'CREATE TABLE test_table (col1 DATE) PARTITION BY RANGE (col1) (PARTITION p1 VALUES LESS THAN (\'2020-01-31\'), PARTITION p2 VALUES LESS THAN (\'2020-02-29\'), PARTITION p3 VALUES LESS THAN (\'2020-03-31\'))',
    );
    this.validateIdentity(
      'CREATE TABLE test_table (col1 STRING) PARTITION BY RANGE (STR2DATE(col1, \'%Y-%m-%d\')) (PARTITION p1 VALUES LESS THAN (\'2021-01-01\'), PARTITION p2 VALUES LESS THAN (\'2021-01-02\'), PARTITION p3 VALUES LESS THAN (\'2021-01-03\'))',
    );
    this.validateIdentity(
      'CREATE TABLE test_table (col1 DATE) PARTITION BY RANGE (col1) (PARTITION p1 VALUES LESS THAN (\'2020-01-31\'), PARTITION p_max VALUES LESS THAN (MAXVALUE))',
    );

    // RANGE partitioning with START/END/EVERY
    this.validateIdentity(
      'CREATE TABLE test_table (col1 BIGINT) PARTITION BY RANGE (col1) (START (\'1\') END (\'10\') EVERY (1), START (\'10\') END (\'100\') EVERY (10))',
    );
    this.validateIdentity(
      'CREATE TABLE test_table (col1 DATE) PARTITION BY RANGE (col1) (START (\'2019-01-01\') END (\'2021-01-01\') EVERY (INTERVAL 1 YEAR), START (\'2021-01-01\') END (\'2021-05-01\') EVERY (INTERVAL 1 MONTH), START (\'2021-05-01\') END (\'2021-05-04\') EVERY (INTERVAL 1 DAY))',
    );
  }
}

const t = new TestStarrocks();

describe('TestStarrocks', () => {
  test('starrocks', () => t.testStarrocks());
  test('ddl', () => t.testDdl());
  test('identity', () => t.testIdentity());
  test('time', () => t.testTime());
  test('regex', () => t.testRegex());
  test('unnest', () => t.testUnnest());
  test('analyze', () => t.testAnalyze());
  test('between', () => t.testBetween());
  test('partition', () => t.testPartition());
});
