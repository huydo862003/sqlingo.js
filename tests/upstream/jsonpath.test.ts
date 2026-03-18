import { readFileSync } from 'node:fs';
import {
  join, dirname,
} from 'path';
import { fileURLToPath } from 'node:url';
import {
  describe, test, expect,
} from 'vitest';
import {
  JsonPathExpr,
  JsonPathFilterExpr,
  JsonPathKeyExpr,
  JsonPathRootExpr,
  JsonPathScriptExpr,
  JsonPathSelectorExpr,
  JsonPathSliceExpr,
  JsonPathSubscriptExpr,
  JsonPathUnionExpr,
  JsonPathWildcardExpr,
} from '../../src/expressions';
import { parse } from '../../src/jsonpath';
import {
  ParseError, TokenError,
} from '../../src/errors';

const fileDir = dirname(fileURLToPath(import.meta.url));
const FIXTURES_DIR = join(fileDir, '..', '..', 'upstream', 'sqlglot', 'tests', 'fixtures');

class TestJsonpath {
  testJsonpath () {
    const expectedExpressions = [
      new JsonPathRootExpr({}),
      new JsonPathKeyExpr({ this: new JsonPathWildcardExpr({}) }),
      new JsonPathKeyExpr({ this: 'a' }),
      new JsonPathSubscriptExpr({ this: 0 }),
      new JsonPathKeyExpr({ this: 'x' }),
      new JsonPathUnionExpr({
        expressions: [
          new JsonPathWildcardExpr({}),
          'y',
          1,
        ],
      }),
      new JsonPathKeyExpr({ this: 'z' }),
      new JsonPathSelectorExpr({ this: new JsonPathFilterExpr({ this: '(@.a == \'b\'), 1:' }) }),
      new JsonPathSubscriptExpr({
        this: new JsonPathSliceExpr({
          start: 1,
          end: 5,
          step: undefined,
        }),
      }),
      new JsonPathUnionExpr({ expressions: [1, new JsonPathFilterExpr({ this: '@.a' })] }),
      new JsonPathSelectorExpr({ this: new JsonPathScriptExpr({ this: '@.x)' }) }),
    ];

    expect(
      parse('$.*.a[0][\'x\'][*, \'y\', 1].z[?(@.a == \'b\'), 1:][1:5][1,?@.a][(@.x)]'),
    ).toEqual(new JsonPathExpr({ expressions: expectedExpressions }));
  }

  testIdentity () {
    const cases: [string, string][] = [
      ['$.select', '$.select'],
      ['$[(@.length-1)]', '$[(@.length-1)]'],
      ['$[((@.length-1))]', '$[((@.length-1))]'],
    ];

    for (const [selector, expected] of cases) {
      expect(parse(selector).sql()).toBe(`'${expected}'`);
    }
  }

  testCtsFile () {
    const filePath = join(FIXTURES_DIR, 'jsonpath', 'cts.json');
    const fileContent = readFileSync(filePath, 'utf-8');
    const tests = JSON.parse(fileContent).tests as Array<{
      selector: string;
      name: string;
      invalid_selector?: boolean;
    }>;

    const overrides: Record<string, string> = {
      '$.☺': '$["☺"]',
      '$[\'a\',1]': '$["a",1]',
      '$[*,\'a\']': '$[*,"a"]',
      '$..[\'a\',\'d\']': '$..["a","d"]',
      '$[1, ?@.a==\'b\', 1:]': '$[1,?@.a==\'b\', 1:]',
      '$["a"]': '$.a',
      '$["c"]': '$.c',
      '$[\'a\']': '$.a',
      '$[\'c\']': '$.c',
      '$[\' \']': '$[" "]',
      '$[\'\\\'\']': '$["\'"]',
      '$[\'\\\\\']': '$["\\\\"]',
      '$[\'\\/\']': '$["\\/"]',
      '$[\'\\b\']': '$["\\b"]',
      '$[\'\\f\']': '$["\\f"]',
      '$[\'\\n\']': '$["\\n"]',
      '$[\'\\r\']': '$["\\r"]',
      '$[\'\\t\']': '$["\\t"]',
      '$[\'\\u263A\']': '$["\\u263A"]',
      '$[\'\\u263a\']': '$["\\u263a"]',
      '$[\'\\uD834\\uDD1E\']': '$["\\uD834\\uDD1E"]',
      '$[\'\\uD83D\\uDE00\']': '$["\\uD83D\\uDE00"]',
      '$[\'\']': '$[""]',
      '$[? @.a]': '$[?@.a]',
      '$[?\n@.a]': '$[?@.a]',
      '$[?\t@.a]': '$[?@.a]',
      '$[?\r@.a]': '$[?@.a]',
      '$[? (@.a)]': '$[?(@.a)]',
      '$[?\n(@.a)]': '$[?(@.a)]',
      '$[?\t(@.a)]': '$[?(@.a)]',
      '$[?\r(@.a)]': '$[?(@.a)]',
      '$[ ?@.a]': '$[?@.a]',
      '$[\n?@.a]': '$[?@.a]',
      '$[\t?@.a]': '$[?@.a]',
      '$[\r?@.a]': '$[?@.a]',
      '$ [\'a\']': '$.a',
      '$\n[\'a\']': '$.a',
      '$\t[\'a\']': '$.a',
      '$\r[\'a\']': '$.a',
      '$[\'a\'] [\'b\']': '$.a.b',
      '$[\'a\'] \n[\'b\']': '$.a.b',
      '$[\'a\'] \t[\'b\']': '$.a.b',
      '$[\'a\'] \r[\'b\']': '$.a.b',
      '$ .a': '$.a',
      '$\n.a': '$.a',
      '$\t.a': '$.a',
      '$\r.a': '$.a',
      '$[ \'a\']': '$.a',
      '$[\n\'a\']': '$.a',
      '$[\t\'a\']': '$.a',
      '$[\r\'a\']': '$.a',
      '$[\'a\' ]': '$.a',
      '$[\'a\'\n]': '$.a',
      '$[\'a\'\t]': '$.a',
      '$[\'a\'\r]': '$.a',
      '$[\'a\' ,\'b\']': '$["a","b"]',
      '$[\'a\'\n,\'b\']': '$["a","b"]',
      '$[\'a\'\t,\'b\']': '$["a","b"]',
      '$[\'a\'\r,\'b\']': '$["a","b"]',
      '$[\'a\', \'b\']': '$["a","b"]',
      '$[\'a\',\n\'b\']': '$["a","b"]',
      '$[\'a\',\t\'b\']': '$["a","b"]',
      '$[\'a\',\r\'b\']': '$["a","b"]',
      '$[1 :5:2]': '$[1:5:2]',
      '$[1\n:5:2]': '$[1:5:2]',
      '$[1\t:5:2]': '$[1:5:2]',
      '$[1\r:5:2]': '$[1:5:2]',
      '$[1: 5:2]': '$[1:5:2]',
      '$[1:\n5:2]': '$[1:5:2]',
      '$[1:\t5:2]': '$[1:5:2]',
      '$[1:\r5:2]': '$[1:5:2]',
      '$[1:5 :2]': '$[1:5:2]',
      '$[1:5\n:2]': '$[1:5:2]',
      '$[1:5\t:2]': '$[1:5:2]',
      '$[1:5\r:2]': '$[1:5:2]',
      '$[1:5: 2]': '$[1:5:2]',
      '$[1:5:\n2]': '$[1:5:2]',
      '$[1:5:\t2]': '$[1:5:2]',
      '$[1:5:\r2]': '$[1:5:2]',
    };

    for (const testCase of tests) {
      const selector = testCase.selector;
      if (testCase.invalid_selector) {
        try {
          parse(selector);
        } catch (e) {
          if (!(e instanceof ParseError) && !(e instanceof TokenError)) {
            throw e;
          }
        }
      } else {
        const path = parse(selector);
        const expectedSql = `'${overrides[selector] ?? selector}'`;
        expect(path.sql()).toBe(expectedSql);
      }
    }
  }
}

const t = new TestJsonpath();

describe('TestJsonpath', () => {
  test('jsonpath', () => t.testJsonpath());
  test('identity', () => t.testIdentity());
  test('testCtsFile', () => t.testCtsFile());
});
