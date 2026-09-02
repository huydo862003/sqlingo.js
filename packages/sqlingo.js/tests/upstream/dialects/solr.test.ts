import {
  describe, test,
} from 'vitest';
import {
  OrExpr,
} from '../../../src/expressions';
import {
  Validator,
} from './validator';

class TestSolr extends Validator {
  override dialect = 'solr' as const;

  testSolr () {
    this.validateIdentity('SELECT `default`.column FROM t');
    this.validateIdentity('SELECT column FROM t WHERE column = \'val\'');
    this.validateIdentity('a || b', 'a OR b').assertIs(OrExpr);
  }
}

const t = new TestSolr();

describe('TestSolr', () => {
  test('solr', () => t.testSolr());
});
