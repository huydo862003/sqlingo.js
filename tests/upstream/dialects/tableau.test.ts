import {
  describe, test,
} from 'vitest';
import { Validator } from './validator';

class TestTableau extends Validator {
  override dialect = 'tableau' as const;

  testTableau () {
    this.validateAll(
      '[x]',
      {
        write: {
          hive: '`x`',
          tableau: '[x]',
        },
      },
    );
    this.validateAll(
      '"x"',
      {
        write: {
          hive: '\'x\'',
          tableau: '\'x\'',
        },
      },
    );
    this.validateAll(
      'IF x = \'a\' THEN y ELSE NULL END',
      {
        read: {
          presto: 'IF(x = \'a\', y, NULL)',
        },
        write: {
          presto: 'IF(x = \'a\', y, NULL)',
          hive: 'IF(x = \'a\', y, NULL)',
          tableau: 'IF x = \'a\' THEN y ELSE NULL END',
        },
      },
    );
    this.validateAll(
      'IFNULL(a, 0)',
      {
        read: {
          presto: 'COALESCE(a, 0)',
        },
        write: {
          presto: 'COALESCE(a, 0)',
          hive: 'COALESCE(a, 0)',
          tableau: 'IFNULL(a, 0)',
        },
      },
    );
    this.validateAll(
      'COUNTD(a)',
      {
        read: {
          presto: 'COUNT(DISTINCT a)',
        },
        write: {
          presto: 'COUNT(DISTINCT a)',
          hive: 'COUNT(DISTINCT a)',
          tableau: 'COUNTD(a)',
        },
      },
    );
    this.validateAll(
      'COUNTD((a))',
      {
        read: {
          presto: 'COUNT(DISTINCT(a))',
        },
        write: {
          presto: 'COUNT(DISTINCT (a))',
          hive: 'COUNT(DISTINCT (a))',
          tableau: 'COUNTD((a))',
        },
      },
    );
    this.validateAll(
      'COUNT(a)',
      {
        read: {
          presto: 'COUNT(a)',
        },
        write: {
          presto: 'COUNT(a)',
          hive: 'COUNT(a)',
          tableau: 'COUNT(a)',
        },
      },
    );
  }
}

const t = new TestTableau();
describe('TestTableau', () => {
  test('tableau', () => t.testTableau());
});
