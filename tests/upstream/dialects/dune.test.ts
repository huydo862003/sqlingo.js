import {
  describe, test,
} from 'vitest';
import { HexStringExpr } from '../../../src/expressions';
import { Validator } from './validator';

class TestDune extends Validator {
  override dialect = 'dune' as const;

  testDune () {
    this.validateIdentity('CAST(x AS INT256)');
    this.validateIdentity('CAST(x AS UINT256)');

    for (const hexLiteral of [
      'deadbeef',
      'deadbeefdead',
      'deadbeefdeadbeef',
      'deadbeefdeadbeefde',
      'deadbeefdeadbeefdead',
      'deadbeefdeadbeefdeadbeef',
      'deadbeefdeadbeefdeadbeefdeadbeef',
    ]) {
      this.parseOne(`0x${hexLiteral}`).assertIs(HexStringExpr);

      this.validateAll(
        `SELECT 0x${hexLiteral}`,
        {
          read: {
            dune: `SELECT X'${hexLiteral}'`,
            postgres: `SELECT x'${hexLiteral}'`,
            trino: `SELECT X'${hexLiteral}'`,
          },
          write: {
            dune: `SELECT 0x${hexLiteral}`,
            postgres: `SELECT x'${hexLiteral}'`,
            trino: `SELECT x'${hexLiteral}'`,
          },
        },
      );
    }
  }
}

const t = new TestDune();
describe('TestDune', () => {
  test('dune', () => t.testDune());
});
