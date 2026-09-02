import {
  describe, test,
} from 'vitest';
import {
  Dialects,
} from '../../../src/dialects/dialect';
import {
  Validator,
} from './validator';

class TestDruid extends Validator {
  override dialect = 'druid' as const;

  testDruid () {
    this.validateIdentity('SELECT MOD(1000, 60)');
    this.validateIdentity('SELECT CEIL(__time TO WEEK) FROM t');
    this.validateIdentity('SELECT CEIL(col) FROM t');
    this.validateIdentity('SELECT CEIL(price, 2) AS rounded_price FROM t');
    this.validateIdentity('SELECT FLOOR(__time TO WEEK) FROM t');
    this.validateIdentity('SELECT FLOOR(col) FROM t');
    this.validateIdentity('SELECT FLOOR(price, 2) AS rounded_price FROM t');
    this.validateIdentity('SELECT CURRENT_TIMESTAMP');
    this.validateIdentity('SELECT ARRAY[1, 2, 3]');

    // validate across all dialects
    const write: Record<string, string> = {};

    for (const dialect of Object.values(Dialects)) {
      write[dialect] = 'FLOOR(__time TO WEEK)';
    }

    this.validateAll(
      'FLOOR(__time TO WEEK)',
      { write },
    );
  }
}

const t = new TestDruid();

describe('TestDruid', () => {
  test('druid', () => t.testDruid());
});
