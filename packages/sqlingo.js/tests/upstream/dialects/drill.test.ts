import {
  describe, test,
} from 'vitest';
import {
  Validator,
} from './validator';

class TestDrill extends Validator {
  override dialect = 'drill' as const;

  testDrill () {
    this.validateIdentity(
      'SELECT * FROM table(dfs.`test_data.xlsx`(type => \'excel\', sheetName => \'secondSheet\'))',
    );
    this.validateIdentity(
      'SELECT * FROM (SELECT * FROM t) PIVOT(avg(c1) AS ac1 FOR c2 IN (\'V\' AS v))',
    );

    this.validateAll(
      'SELECT \'2021-01-01\' + INTERVAL 1 MONTH',
      {
        write: {
          drill: 'SELECT \'2021-01-01\' + INTERVAL \'1\' MONTH',
          mysql: 'SELECT \'2021-01-01\' + INTERVAL \'1\' MONTH',
        },
      },
    );
  }

  testAnalyze () {
    this.validateIdentity('ANALYZE TABLE tbl COMPUTE STATISTICS');
    this.validateIdentity('ANALYZE TABLE tbl COMPUTE STATISTICS SAMPLE 5 PERCENT');
  }
}

const t = new TestDrill();

describe('TestDrill', () => {
  test('drill', () => t.testDrill());
  test('analyze', () => t.testAnalyze());
});
