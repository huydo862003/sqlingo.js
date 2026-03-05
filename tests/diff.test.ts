import {
  describe, test, expect,
} from 'vitest';
import {
  diff, Insert, Remove, Move, Update,
} from '../src/diff';
import {
  parseOne, toTable, toColumn, toIdentifier, alias, column,
} from '../src/index';
import {
  LiteralExpr, ConcatExpr, JoinExpr, ColumnExpr, OrderExpr, LambdaExpr, Expression,
  SelectExpr,
} from '../src/expressions';

function diffDeltaOnly (
  source: Expression,
  target: Expression,
  options: {
    matchings?: [Expression, Expression][];
    dialect?: string;
  } = {},
): ReturnType<typeof diff> {
  return diff(source, target, {
    ...options,
    deltaOnly: true,
  });
}

class TestDiff {
  private validateDeltaOnly (
    actualDelta: ReturnType<typeof diff>,
    expectedDelta: ReturnType<typeof diff>,
  ): void {
    const actualSet = new Set(actualDelta.map((e) => JSON.stringify(e)));
    const expectedSet = new Set(expectedDelta.map((e) => JSON.stringify(e)));
    expect(actualSet).toEqual(expectedSet);
  }

  testSimple (): void {
    this.validateDeltaOnly(
      diffDeltaOnly(parseOne('SELECT a + b', { into: SelectExpr }), parseOne('SELECT a - b', { into: SelectExpr })),
      [
        new Remove(parseOne('a + b')),
        new Insert(parseOne('a - b')),
        new Move(parseOne('a'), parseOne('a')),
        new Move(parseOne('b'), parseOne('b')),
      ],
    );

    this.validateDeltaOnly(
      diffDeltaOnly(parseOne('SELECT a, b, c', { into: SelectExpr }), parseOne('SELECT a, c', { into: SelectExpr })),
      [new Remove(parseOne('b'))],
    );

    this.validateDeltaOnly(
      diffDeltaOnly(parseOne('SELECT a, b', { into: SelectExpr }), parseOne('SELECT a, b, c', { into: SelectExpr })),
      [new Insert(parseOne('c'))],
    );

    this.validateDeltaOnly(
      diffDeltaOnly(
        parseOne('SELECT a FROM table_one', { into: SelectExpr }),
        parseOne('SELECT a FROM table_two', { into: SelectExpr }),
      ),
      [
        new Update(
          toTable('table_one', { quoted: false }),
          toTable('table_two', { quoted: false }),
        ),
      ],
    );
  }

  testLambda (): void {
    this.validateDeltaOnly(
      diffDeltaOnly(
        parseOne('SELECT a, b, c, x(a -> a)', { into: SelectExpr }),
        parseOne('SELECT a, b, c, x(b -> b)', { into: SelectExpr }),
      ),
      [
        new Update(
          new LambdaExpr({
            this: toIdentifier('a'),
            expressions: [toIdentifier('a')],
          }),
          new LambdaExpr({
            this: toIdentifier('b'),
            expressions: [toIdentifier('b')],
          }),
        ),
      ],
    );
  }

  testUdf (): void {
    this.validateDeltaOnly(
      diffDeltaOnly(
        parseOne('SELECT a, b, "my.udf1"()', { into: SelectExpr }),
        parseOne('SELECT a, b, "my.udf2"()', { into: SelectExpr }),
      ),
      [new Insert(parseOne('"my.udf2"()')), new Remove(parseOne('"my.udf1"()'))],
    );

    this.validateDeltaOnly(
      diffDeltaOnly(
        parseOne('SELECT a, b, "my.udf"(x, y, z)', { into: SelectExpr }),
        parseOne('SELECT a, b, "my.udf"(x, y, w)', { into: SelectExpr }),
      ),
      [new Insert(column({ col: 'w' })), new Remove(column({ col: 'z' }))],
    );
  }

  testNodePositionChanged (): void {
    let exprSrc = parseOne('SELECT a, b, c', { into: SelectExpr });
    let exprTgt = parseOne('SELECT c, a, b', { into: SelectExpr });

    this.validateDeltaOnly(
      diffDeltaOnly(exprSrc, exprTgt),
      [new Move(exprSrc.selects[2], exprTgt.selects[0])],
    );

    exprSrc = parseOne('SELECT a + b', { into: SelectExpr });
    exprTgt = parseOne('SELECT b + a', { into: SelectExpr });

    const srcAddLeft = exprSrc.selects[0].getArgKey('this');
    const tgtAddRight = exprTgt.selects[0].getArgKey('expression');

    if (srcAddLeft instanceof Expression && tgtAddRight instanceof Expression) {
      this.validateDeltaOnly(
        diffDeltaOnly(exprSrc, exprTgt),
        [new Move(srcAddLeft, tgtAddRight)],
      );
    }

    exprSrc = parseOne('SELECT aaaa AND bbbb', { into: SelectExpr });
    exprTgt = parseOne('SELECT bbbb AND aaaa', { into: SelectExpr });

    const srcAndLeft = exprSrc.selects[0].getArgKey('this');
    const tgtAndRight = exprTgt.selects[0].getArgKey('expression');

    if (srcAndLeft instanceof Expression && tgtAndRight instanceof Expression) {
      this.validateDeltaOnly(
        diffDeltaOnly(exprSrc, exprTgt),
        [new Move(srcAndLeft, tgtAndRight)],
      );
    }

    exprSrc = parseOne('SELECT aaaa OR bbbb OR cccc', { into: SelectExpr });
    exprTgt = parseOne('SELECT cccc OR bbbb OR aaaa', { into: SelectExpr });

    const srcOrTopLeft = exprSrc.selects[0].getArgKey('this');
    const srcOrLeftLeft = srcOrTopLeft instanceof Expression
      ? srcOrTopLeft.getArgKey('this')
      : undefined;
    const srcOrRight = exprSrc.selects[0].getArgKey('expression');
    const tgtOrTopLeft = exprTgt.selects[0].getArgKey('this');
    const tgtOrLeftLeft = tgtOrTopLeft instanceof Expression
      ? tgtOrTopLeft.getArgKey('this')
      : undefined;
    const tgtOrRight = exprTgt.selects[0].getArgKey('expression');

    if (
      srcOrLeftLeft instanceof Expression
      && tgtOrRight instanceof Expression
      && srcOrRight instanceof Expression
      && tgtOrLeftLeft instanceof Expression
    ) {
      this.validateDeltaOnly(
        diffDeltaOnly(exprSrc, exprTgt),
        [new Move(srcOrLeftLeft, tgtOrRight), new Move(srcOrRight, tgtOrLeftLeft)],
      );
    }

    exprSrc = parseOne('SELECT a, b FROM t WHERE CONCAT(\'a\', \'b\') = \'ab\'', { into: SelectExpr });
    exprTgt = parseOne('SELECT a FROM t WHERE CONCAT(\'a\', \'b\', b) = \'ab\'', { into: SelectExpr });

    const tgtConcat = exprTgt.find(ConcatExpr);
    if (tgtConcat) {
      const tgtConcatExprs = tgtConcat.getArgKey('expressions');
      const lastTgtConcatExpr = Array.isArray(tgtConcatExprs)
        ? tgtConcatExprs[tgtConcatExprs.length - 1]
        : undefined;
      if (lastTgtConcatExpr instanceof Expression) {
        this.validateDeltaOnly(
          diffDeltaOnly(exprSrc, exprTgt),
          [new Move(exprSrc.selects[1], lastTgtConcatExpr)],
        );
      }
    }

    exprSrc = parseOne('SELECT a as a, b as b FROM t WHERE CONCAT(\'a\', \'b\') = \'ab\'', { into: SelectExpr });
    exprTgt = parseOne('SELECT a as a FROM t WHERE CONCAT(\'a\', \'b\', b) = \'ab\'', { into: SelectExpr });

    const bAlias = exprSrc.selects[1];
    const tgtConcat2 = exprTgt.find(ConcatExpr);

    if (tgtConcat2) {
      const tgtConcatExprs2 = tgtConcat2.getArgKey('expressions');
      const lastTgtConcatExpr2 = Array.isArray(tgtConcatExprs2)
        ? tgtConcatExprs2[tgtConcatExprs2.length - 1]
        : undefined;
      const bAliasThis = bAlias.getArgKey('this');
      if (lastTgtConcatExpr2 instanceof Expression && bAliasThis instanceof Expression) {
        this.validateDeltaOnly(
          diffDeltaOnly(exprSrc, exprTgt),
          [new Remove(bAlias), new Move(bAliasThis, lastTgtConcatExpr2)],
        );
      }
    }
  }

  testCte (): void {
    const exprSrc = `
      WITH
          cte1 AS (SELECT a, b, LOWER(c) AS c FROM table_one WHERE d = 'filter'),
          cte2 AS (SELECT d, e, f FROM table_two)
      SELECT a, b, d, e FROM cte1 JOIN cte2 ON f = c
    `;
    const exprTgt = `
      WITH
          cte1 AS (SELECT a, b, c FROM table_one WHERE d = 'different_filter'),
          cte2 AS (SELECT d, e, f FROM table_two)
      SELECT a, b, d, e FROM cte1 JOIN cte2 ON f = c
    `;

    this.validateDeltaOnly(
      diffDeltaOnly(parseOne(exprSrc), parseOne(exprTgt)),
      [
        new Remove(parseOne('LOWER(c) AS c')),
        new Remove(parseOne('LOWER(c)')),
        new Remove(parseOne('\'filter\'')),
        new Insert(parseOne('\'different_filter\'')),
        new Move(parseOne('c'), parseOne('c')),
      ],
    );
  }

  testJoin (): void {
    const exprSrc = parseOne('SELECT a, b FROM t1 LEFT JOIN t2 ON t1.key = t2.key', { into: SelectExpr });
    const exprTgt = parseOne('SELECT a, b FROM t1 RIGHT JOIN t2 ON t1.key = t2.key', { into: SelectExpr });

    const srcJoin = exprSrc.find(JoinExpr);
    const tgtJoin = exprTgt.find(JoinExpr);

    if (srcJoin && tgtJoin) {
      const srcOn = srcJoin.getArgKey('on');
      const tgtOn = tgtJoin.getArgKey('on');
      if (srcOn instanceof Expression && tgtOn instanceof Expression) {
        this.validateDeltaOnly(
          diffDeltaOnly(exprSrc, exprTgt),
          [
            new Remove(srcJoin),
            new Insert(tgtJoin),
            new Move(toTable('t2'), toTable('t2')),
            new Move(srcOn, tgtOn),
          ],
        );
      }
    }

    const exprSrc2 = parseOne('SELECT a.x FROM a INNER JOIN b ON a.x = b.y LEFT JOIN c ON a.p = c.q', { into: SelectExpr });
    const exprTgt2 = parseOne('SELECT a.x FROM a inner JOIN b ON a.x = b.y left JOIN c ON a.p = c.q', { into: SelectExpr });

    this.validateDeltaOnly(diffDeltaOnly(exprSrc2, exprTgt2), []);
  }

  testWindowFunctions (): void {
    const exprSrc = parseOne('SELECT ROW_NUMBER() OVER (PARTITION BY a ORDER BY b)', { into: SelectExpr });
    const exprTgt = parseOne('SELECT RANK() OVER (PARTITION BY a ORDER BY b)', { into: SelectExpr });

    this.validateDeltaOnly(diffDeltaOnly(exprSrc, exprSrc), []);

    this.validateDeltaOnly(
      diffDeltaOnly(exprSrc, exprTgt),
      [
        new Remove(parseOne('ROW_NUMBER()')),
        new Insert(parseOne('RANK()')),
        new Update(exprSrc.selects[0], exprTgt.selects[0]),
      ],
    );

    const exprSrc2 = parseOne('SELECT MAX(x) OVER (ORDER BY y) FROM z', {
      into: SelectExpr,
      dialect: 'oracle',
    });
    const exprTgt2 = parseOne('SELECT MAX(x) KEEP (DENSE_RANK LAST ORDER BY y) FROM z', {
      into: SelectExpr,
      dialect: 'oracle',
    });

    this.validateDeltaOnly(
      diffDeltaOnly(exprSrc2, exprTgt2),
      [new Update(exprSrc2.selects[0], exprTgt2.selects[0])],
    );
  }

  testPreMatchings (): void {
    const exprSrc = parseOne('SELECT 1', { into: SelectExpr });
    const exprTgt = parseOne('SELECT 1, 2, 3, 4', { into: SelectExpr });

    this.validateDeltaOnly(
      diffDeltaOnly(exprSrc, exprTgt),
      [
        new Remove(exprSrc),
        new Insert(exprTgt),
        new Insert(LiteralExpr.number(2)),
        new Insert(LiteralExpr.number(3)),
        new Insert(LiteralExpr.number(4)),
        new Move(LiteralExpr.number(1), LiteralExpr.number(1)),
      ],
    );

    this.validateDeltaOnly(
      diffDeltaOnly(exprSrc, exprTgt, { matchings: [[exprSrc, exprTgt]] }),
      [
        new Insert(LiteralExpr.number(2)),
        new Insert(LiteralExpr.number(3)),
        new Insert(LiteralExpr.number(4)),
      ],
    );

    this.validateDeltaOnly(
      diffDeltaOnly(exprSrc, exprTgt, {
        matchings: [[exprSrc, exprTgt], [exprSrc, exprTgt]],
      }),
      [
        new Insert(LiteralExpr.number(2)),
        new Insert(LiteralExpr.number(3)),
        new Insert(LiteralExpr.number(4)),
      ],
    );

    const firstTgtSelect = exprTgt.selects[0];
    const srcSelect = exprSrc.selects[0];
    firstTgtSelect.replace(srcSelect);

    this.validateDeltaOnly(
      diffDeltaOnly(exprSrc, exprTgt, { matchings: [[exprSrc, exprTgt]] }),
      [
        new Insert(LiteralExpr.number(2)),
        new Insert(LiteralExpr.number(3)),
        new Insert(LiteralExpr.number(4)),
      ],
    );
  }

  testIdentifier (): void {
    const exprSrc = parseOne('SELECT a FROM tbl', { into: SelectExpr });
    const exprTgt = parseOne('SELECT a, tbl.b from tbl', { into: SelectExpr });

    this.validateDeltaOnly(
      diffDeltaOnly(exprSrc, exprTgt),
      [new Insert(toColumn('tbl.b'))],
    );

    const exprSrc2 = parseOne('SELECT 1 AS c1, 2 AS c2', { into: SelectExpr });
    const exprTgt2 = parseOne('SELECT 2 AS c1, 3 AS c2', { into: SelectExpr });

    this.validateDeltaOnly(
      diffDeltaOnly(exprSrc2, exprTgt2),
      [
        new Remove(alias(1, 'c1')),
        new Remove(LiteralExpr.number(1)),
        new Insert(alias(3, 'c2')),
        new Insert(LiteralExpr.number(3)),
        new Update(alias(2, 'c2'), alias(2, 'c1')),
      ],
    );
  }

  testNonExpressionLeafDelta (): void {
    const exprSrc = parseOne('SELECT a UNION SELECT b', { into: SelectExpr });
    const exprTgt = parseOne('SELECT a UNION ALL SELECT b', { into: SelectExpr });

    this.validateDeltaOnly(
      diffDeltaOnly(exprSrc, exprTgt),
      [new Update(exprSrc, exprTgt)],
    );

    const exprSrc2 = parseOne('SELECT a FROM t ORDER BY b ASC', { into: SelectExpr });
    const exprTgt2 = parseOne('SELECT a FROM t ORDER BY b DESC', { into: SelectExpr });

    const srcOrder = exprSrc2.find(OrderExpr);
    const tgtOrder = exprTgt2.find(OrderExpr);

    if (srcOrder && tgtOrder) {
      const srcOrderExprs = srcOrder.getArgKey('expressions');
      const tgtOrderExprs = tgtOrder.getArgKey('expressions');
      const srcFirstExpr = Array.isArray(srcOrderExprs) ? srcOrderExprs[0] : undefined;
      const tgtFirstExpr = Array.isArray(tgtOrderExprs) ? tgtOrderExprs[0] : undefined;

      if (srcFirstExpr instanceof Expression && tgtFirstExpr instanceof Expression) {
        this.validateDeltaOnly(
          diffDeltaOnly(exprSrc2, exprTgt2),
          [new Update(srcFirstExpr, tgtFirstExpr)],
        );
      }
    }

    const exprSrc3 = parseOne('SELECT a, b FROM t ORDER BY c ASC', { into: SelectExpr });
    const exprTgt3 = parseOne('SELECT b, a FROM t ORDER BY c DESC', { into: SelectExpr });

    const srcOrder3 = exprSrc3.find(OrderExpr);
    const tgtOrder3 = exprTgt3.find(OrderExpr);

    if (srcOrder3 && tgtOrder3) {
      const srcOrderExprs3 = srcOrder3.getArgKey('expressions');
      const tgtOrderExprs3 = tgtOrder3.getArgKey('expressions');
      const srcFirstExpr3 = Array.isArray(srcOrderExprs3) ? srcOrderExprs3[0] : undefined;
      const tgtFirstExpr3 = Array.isArray(tgtOrderExprs3) ? tgtOrderExprs3[0] : undefined;

      if (srcFirstExpr3 instanceof Expression && tgtFirstExpr3 instanceof Expression) {
        this.validateDeltaOnly(
          diffDeltaOnly(exprSrc3, exprTgt3),
          [new Update(srcFirstExpr3, tgtFirstExpr3), new Move(exprSrc3.selects[0], exprTgt3.selects[1])],
        );
      }
    }
  }

  testNoneArgsAreNotTreatedAsLeaves (): void {
    const exprSrc = parseOne('a.b');
    const exprTgt = new ColumnExpr({
      this: toIdentifier('b'),
      table: toIdentifier('a'),
    });

    expect(new Set(Object.keys(exprSrc.args))).toEqual(new Set([
      'this',
      'table',
      'db',
      'catalog',
    ]));
    expect(new Set(Object.keys(exprTgt.args))).toEqual(new Set(['this', 'table']));

    this.validateDeltaOnly(diffDeltaOnly(exprSrc, exprTgt), []);
  }

  testCommentsDoNotAffectDiff (): void {
    const exprSrc = parseOne('select a from tbl', { into: SelectExpr });
    const exprTgt = parseOne('select a from tbl -- this is comment', { into: SelectExpr });

    const fromArg = exprTgt.getArgKey('from');
    if (fromArg instanceof Expression) {
      const fromThis = fromArg.getArgKey('this');
      if (fromThis instanceof Expression) {
        expect(fromThis.comments).toEqual([' this is comment']);
      }
    }

    this.validateDeltaOnly(diffDeltaOnly(exprSrc, exprTgt), []);
  }
}

const t = new TestDiff();
describe('TestDiff', () => {
  test('simple', () => t.testSimple());
  test('lambda', () => t.testLambda());
  test('udf', () => t.testUdf());
  test('testNodePositionChanged', () => t.testNodePositionChanged());
  test('cte', () => t.testCte());
  test('join', () => t.testJoin());
  test('testWindowFunctions', () => t.testWindowFunctions());
  test('testPreMatchings', () => t.testPreMatchings());
  test('identifier', () => t.testIdentifier());
  test('testNonExpressionLeafDelta', () => t.testNonExpressionLeafDelta());
  test('testNoneArgsAreNotTreatedAsLeaves', () => t.testNoneArgsAreNotTreatedAsLeaves());
  test('testCommentsDoNotAffectDiff', () => t.testCommentsDoNotAffectDiff());
});
