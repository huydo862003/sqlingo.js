import {
  describe, it, expect,
} from 'vitest';
import type { Expression } from '../src/index';
import {
  alias,
  and,
  case_,
  condition,
  except,
  from,
  intersect,
  not,
  or,
  parseOne,
  select,
  union,
} from '../src/index';
import {
  cast,
  column, convert, delete_, func, insert, JoinExpr, JoinExprKind, LiteralExpr, merge, null_,
  renameColumn,
  SelectExpr,
  subquery,
  table,
  TableExpr,
  toIdentifier,
  TupleExpr,
  update,
  UpdateExpr,
  values,
  WhenExpr,
} from '../src/expressions';

describe('TestBuild', () => {
  it('test_build', () => {
    const x = condition('x');
    const xPlusOne = x.add(1);

    expect(x.parent).toBeUndefined();

    expect(xPlusOne.args.this === x).toBe(false);

    const testCases: Array<[() => Expression | undefined, string, string?]> = [
      [() => x.add(1), 'x + 1'],
      [() => LiteralExpr.number(1).add(x), '1 + x'],
      [() => x.sub(1), 'x - 1'],
      [() => LiteralExpr.number(1).sub(x), '1 - x'],
      [() => x.mul(1), 'x * 1'],
      [() => LiteralExpr.number(1).mul(x), '1 * x'],
      [() => x.div(1), 'x / 1'],
      [() => LiteralExpr.number(1).div(x), '1 / x'],
      [() => x.floorDiv(1), 'CAST(x / 1 AS INT)'],
      [() => LiteralExpr.number(1).floorDiv(x), 'CAST(1 / x AS INT)'],
      [() => x.mod(1), 'x % 1'],
      [() => LiteralExpr.number(1).mod(x), '1 % x'],
      [() => x.pow(1), 'POWER(x, 1)'],
      [() => LiteralExpr.number(1).pow(x), 'POWER(1, x)'],
      [() => x.and(1), 'x AND 1'],
      [() => LiteralExpr.number(1).and(x), '1 AND x'],
      [() => x.or(1), 'x OR 1'],
      [() => LiteralExpr.number(1).or(x), '1 OR x'],
      [() => x.lt(1), 'x < 1'],
      [() => LiteralExpr.number(1).lt(x), '1 < x'],
      [() => x.lte(1), 'x <= 1'],
      [() => LiteralExpr.number(1).lte(x), '1 <= x'],
      [() => x.gt(1), 'x > 1'],
      [() => LiteralExpr.number(1).gt(x), '1 > x'],
      [() => x.gte(1), 'x >= 1'],
      [() => LiteralExpr.number(1).gte(x), '1 >= x'],
      [() => x.eq(1), 'x = 1'],
      [() => x.neq(1), 'x <> 1'],
      [() => x.is(null_()), 'x IS NULL'],
      [() => x.as('y'), 'x AS y'],
      [() => x.in([1, '2']), 'x IN (1, \'2\')'],
      [() => x.in([], 'select 1'), 'x IN (SELECT 1)'],
      [() => x.in([], undefined, { unnest: 'x' }), 'x IN (SELECT UNNEST(x))'],
      [
        () => x.in([], undefined, { unnest: 'x' }),
        'x IN UNNEST(x)',
        'bigquery',
      ],
      [() => x.in([], undefined, { unnest: ['x', 'y'] }), 'x IN (SELECT UNNEST(x, y))'],
      [() => x.between(1, 2), 'x BETWEEN 1 AND 2'],
      [
        () => LiteralExpr.number(1).add(x)
          .add(2)
          .add(3),
        '1 + x + 2 + 3',
      ],
      [
        () => LiteralExpr.number(1).add(x.mul(2))
          .add(3),
        '1 + (x * 2) + 3',
      ],
      [
        () => x.mul(1).mul(2)
          .add(3),
        '(x * 1 * 2) + 3',
      ],
      [() => LiteralExpr.number(1).add(x.mul(2).div(3)), '1 + ((x * 2) / 3)'],
      [() => x.and('\'y\''), 'x AND \'y\''],
      [() => x.or('\'y\''), 'x OR \'y\''],
      [() => x.neg(), '-x'],
      [() => x.not(), 'NOT x'],
      [() => x.getItem(1), 'x[1]'],
      [() => x.getItem(1, 2), 'x[1, 2]'],
      [() => x.getItem('y').add(1), 'x[\'y\'] + 1'],
      [() => x.like('y'), 'x LIKE \'y\''],
      [() => x.ilike('y'), 'x ILIKE \'y\''],
      [() => x.rlike('y'), 'REGEXP_LIKE(x, \'y\')'],
      [
        () => case_().when('x = 1', 'x')
          .else('bar'),
        'CASE WHEN x = 1 THEN x ELSE bar END',
      ],
      [
        () => case_('x').when('1', 'x')
          .else('bar'),
        'CASE x WHEN 1 THEN x ELSE bar END',
      ],
      [() => func('COALESCE', 'x', 1), 'COALESCE(x, 1)'],
      [() => column({ col: 'x' }).desc(), 'x DESC'],
      [() => column({ col: 'x' }).desc({ nullsFirst: true }), 'x DESC NULLS FIRST'],
      [() => select('x'), 'SELECT x'],
      [() => select(['x', 'y']), 'SELECT x, y'],
      [() => select('x').from('tbl'), 'SELECT x FROM tbl'],
      [() => select(['x', 'y']).from('tbl'), 'SELECT x, y FROM tbl'],
      [
        () => select('x').select('y')
          .from('tbl'),
        'SELECT x, y FROM tbl',
      ],
      [() => select(['comment', 'begin']), 'SELECT comment, begin'],
      [
        () => select('x').select('y', { append: false })
          .from('tbl'),
        'SELECT y FROM tbl',
      ],
      [
        () => select('x').from('tbl')
          .from('tbl2'),
        'SELECT x FROM tbl2',
      ],
      [() => select('SUM(x) AS y'), 'SELECT SUM(x) AS y'],
      [
        () => select('x').from('tbl')
          .where('x > 0'),
        'SELECT x FROM tbl WHERE x > 0',
      ],
      [
        () => select('x').from('tbl')
          .where('x < 4 OR x > 5'),
        'SELECT x FROM tbl WHERE x < 4 OR x > 5',
      ],
      [
        () => select('x').from('tbl')
          .where('x > 0')
          .where('x < 9'),
        'SELECT x FROM tbl WHERE x > 0 AND x < 9',
      ],
      [
        () => select('x').from('tbl')
          .where(['x > 0', 'x < 9']),
        'SELECT x FROM tbl WHERE x > 0 AND x < 9',
      ],
      [
        () => select('x').from('tbl')
          .where(undefined)
          .where([false, '']),
        'SELECT x FROM tbl WHERE FALSE',
      ],
      [
        () => select('x').from('tbl')
          .where('x > 0')
          .where('x < 9', { append: false }),
        'SELECT x FROM tbl WHERE x < 9',
      ],
      [
        () => select('x').from('tbl')
          .where('x > 0')
          .lock(),
        'SELECT x FROM tbl WHERE x > 0 FOR UPDATE',
        'mysql',
      ],
      [
        () => select('x').from('tbl')
          .where('x > 0')
          .lock({ update: false }),
        'SELECT x FROM tbl WHERE x > 0 FOR SHARE',
        'postgres',
      ],
      [
        () => select('x').from('tbl')
          .hint('repartition(100)'),
        'SELECT /*+ REPARTITION(100) */ x FROM tbl',
        'spark',
      ],
      [
        () => select('x').from('tbl')
          .hint(['coalesce(3)', 'broadcast(x)']),
        'SELECT /*+ COALESCE(3), BROADCAST(x) */ x FROM tbl',
        'spark',
      ],
      [
        () => select(['x', 'y']).from('tbl')
          .groupBy('x'),
        'SELECT x, y FROM tbl GROUP BY x',
      ],
      [
        () => select(['x', 'y']).from('tbl')
          .groupBy('x, y'),
        'SELECT x, y FROM tbl GROUP BY x, y',
      ],
      [
        () => select([
          'x',
          'y',
          'z',
          'a',
        ]).from('tbl')
          .groupBy(['x, y', 'z'])
          .groupBy('a'),
        'SELECT x, y, z, a FROM tbl GROUP BY x, y, z, a',
      ],
      [
        () => select(1).from('tbl')
          .groupBy('x with cube'),
        'SELECT 1 FROM tbl GROUP BY x WITH CUBE',
      ],
      [
        () => select('x').distinct(['a', 'b'])
          .from('tbl'),
        'SELECT DISTINCT ON (a, b) x FROM tbl',
      ],
      [
        () => select('x').distinct([true])
          .from('tbl'),
        'SELECT DISTINCT x FROM tbl',
      ],
      [
        () => select('x').distinct([false])
          .from('tbl'),
        'SELECT x FROM tbl',
      ],
      [
        () => select('x').lateral('OUTER explode(y) tbl2 AS z')
          .from('tbl'),
        'SELECT x FROM tbl LATERAL VIEW OUTER EXPLODE(y) tbl2 AS z',
      ],
      [
        () => select('x').from('tbl')
          .join('tbl2 ON tbl.y = tbl2.y'),
        'SELECT x FROM tbl JOIN tbl2 ON tbl.y = tbl2.y',
      ],
      [
        () => select('x').from('tbl')
          .join('tbl2', { on: 'tbl.y = tbl2.y' }),
        'SELECT x FROM tbl JOIN tbl2 ON tbl.y = tbl2.y',
      ],
      [
        () => select('x').from('tbl')
          .join('tbl2', { on: ['tbl.y = tbl2.y', 'a = b'] }),
        'SELECT x FROM tbl JOIN tbl2 ON tbl.y = tbl2.y AND a = b',
      ],
      [
        () => select('x').from('tbl')
          .join('tbl2', { joinType: JoinExprKind.LEFT }),
        'SELECT x FROM tbl LEFT JOIN tbl2',
      ],
      [
        () => select('x').from('tbl')
          .join(new TableExpr({ this: 'tbl2' }), { joinType: JoinExprKind.LEFT }),
        'SELECT x FROM tbl LEFT JOIN tbl2',
      ],
      [
        () => select('x').from('tbl')
          .join(new TableExpr({ this: 'tbl2' }), {
            joinType: JoinExprKind.LEFT,
            joinAlias: 'foo',
          }),
        'SELECT x FROM tbl LEFT JOIN tbl2 AS foo',
      ],
      [
        () => select('x').from('tbl')
          .join(select('y').from('tbl2'), { joinType: JoinExprKind.LEFT }),
        'SELECT x FROM tbl LEFT JOIN (SELECT y FROM tbl2)',
      ],
      [
        () => select('x').from('tbl')
          .join(select('y').from('tbl2')
            .subquery('aliased'), { joinType: JoinExprKind.LEFT }),
        'SELECT x FROM tbl LEFT JOIN (SELECT y FROM tbl2) AS aliased',
      ],
      [
        () => select('x').from('tbl')
          .join(select('y').from('tbl2'), {
            joinType: JoinExprKind.LEFT,
            joinAlias: 'aliased',
          }),
        'SELECT x FROM tbl LEFT JOIN (SELECT y FROM tbl2) AS aliased',
      ],
      [
        () => select('x').from('tbl')
          .join(parseOne('left join x', { into: JoinExpr }), { on: 'a=b' }),
        'SELECT x FROM tbl LEFT JOIN x ON a = b',
      ],
      [
        () => select('x').from('tbl')
          .join('left join x', { on: 'a=b' }),
        'SELECT x FROM tbl LEFT JOIN x ON a = b',
      ],
      [
        () => select('x').from('tbl')
          .join('select b from tbl2', {
            on: 'a=b',
            joinType: JoinExprKind.LEFT,
          }),
        'SELECT x FROM tbl LEFT JOIN (SELECT b FROM tbl2) ON a = b',
      ],
      [
        () => select('x').from('tbl')
          .join('select b from tbl2', {
            on: 'a=b',
            joinType: JoinExprKind.LEFT,
            joinAlias: 'aliased',
          }),
        'SELECT x FROM tbl LEFT JOIN (SELECT b FROM tbl2) AS aliased ON a = b',
      ],
      [
        () => select([
          'x',
          'y',
          'z',
        ]).from('merged_df')
          .join('vte_diagnosis_df', { using: ['patient_id', 'encounter_id'] }),
        'SELECT x, y, z FROM merged_df JOIN vte_diagnosis_df USING (patient_id, encounter_id)',
      ],
      [
        () => select([
          'x',
          'y',
          'z',
        ]).from('merged_df')
          .join('vte_diagnosis_df', { using: [toIdentifier('patient_id'), toIdentifier('encounter_id')] }),
        'SELECT x, y, z FROM merged_df JOIN vte_diagnosis_df USING (patient_id, encounter_id)',
      ],
      [() => parseOne('JOIN x', { into: JoinExpr }).on(['y = 1', 'z = 1']), 'JOIN x ON y = 1 AND z = 1'],
      [() => parseOne('JOIN x', { into: JoinExpr }).on('y = 1'), 'JOIN x ON y = 1'],
      [() => parseOne('JOIN x', { into: JoinExpr }).using(['bar', 'bob']), 'JOIN x USING (bar, bob)'],
      [() => parseOne('JOIN x', { into: JoinExpr }).using('bar'), 'JOIN x USING (bar)'],
      [
        () => select('x').from('foo')
          .join('bla', { using: 'bob' }),
        'SELECT x FROM foo JOIN bla USING (bob)',
      ],
      [
        () => select(['x', 'COUNT(y)']).from('tbl')
          .groupBy('x')
          .having('COUNT(y) > 0'),
        'SELECT x, COUNT(y) FROM tbl GROUP BY x HAVING COUNT(y) > 0',
      ],
      [
        () => select('x').from('tbl')
          .orderBy('y'),
        'SELECT x FROM tbl ORDER BY y',
      ],
      [() => parseOne('select * from x union select * from y', { into: SelectExpr }).orderBy('y'), 'SELECT * FROM x UNION SELECT * FROM y ORDER BY y'],
      [
        () => select('x').from('tbl')
          .clusterBy('y'),
        'SELECT x FROM tbl CLUSTER BY y',
        'hive',
      ],
      [
        () => select('x').from('tbl')
          .sortBy('y'),
        'SELECT x FROM tbl SORT BY y',
        'hive',
      ],
      [
        () => select('x').from('tbl')
          .orderBy('x, y DESC'),
        'SELECT x FROM tbl ORDER BY x, y DESC',
      ],
      [
        () => select('x').from('tbl')
          .clusterBy('x, y DESC'),
        'SELECT x FROM tbl CLUSTER BY x, y DESC',
        'hive',
      ],
      [
        () => select('x').from('tbl')
          .sortBy('x, y DESC'),
        'SELECT x FROM tbl SORT BY x, y DESC',
        'hive',
      ],
      [
        () => select([
          'x',
          'y',
          'z',
          'a',
        ]).from('tbl')
          .orderBy(['x, y', 'z'])
          .orderBy('a'),
        'SELECT x, y, z, a FROM tbl ORDER BY x, y, z, a',
      ],
      [
        () => select([
          'x',
          'y',
          'z',
          'a',
        ]).from('tbl')
          .clusterBy(['x, y', 'z'])
          .clusterBy('a'),
        'SELECT x, y, z, a FROM tbl CLUSTER BY x, y, z, a',
        'hive',
      ],
      [
        () => select([
          'x',
          'y',
          'z',
          'a',
        ]).from('tbl')
          .sortBy(['x, y', 'z'])
          .sortBy('a'),
        'SELECT x, y, z, a FROM tbl SORT BY x, y, z, a',
        'hive',
      ],
      [
        () => select('x').from('tbl')
          .limit(10),
        'SELECT x FROM tbl LIMIT 10',
      ],
      [
        () => select('x').from('tbl')
          .offset(10),
        'SELECT x FROM tbl OFFSET 10',
      ],
      [
        () => select('x').from('tbl')
          .with('tbl', 'SELECT x FROM tbl2'),
        'WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl',
      ],
      [
        () => select('x').from('tbl')
          .with(
            'tbl',
            'SELECT x FROM tbl2',
            {
              materialized: true,
            },
          ),
        'WITH tbl AS MATERIALIZED (SELECT x FROM tbl2) SELECT x FROM tbl',
      ],
      [
        () => select('x').from('tbl')
          .with(
            'tbl',
            'SELECT x FROM tbl2',
            {
              materialized: false,
            },
          ),
        'WITH tbl AS NOT MATERIALIZED (SELECT x FROM tbl2) SELECT x FROM tbl',
      ],
      [
        () => select('x').from('tbl')
          .with(
            'tbl',
            'SELECT x FROM tbl2',
            {
              recursive: true,
            },
          ),
        'WITH RECURSIVE tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl',
      ],
      [
        () => select('x').from('tbl')
          .with(
            'tbl',
            select('x').from('tbl2'),
            {
              recursive: true,
              materialized: true,
            },
          ),
        'WITH RECURSIVE tbl AS MATERIALIZED (SELECT x FROM tbl2) SELECT x FROM tbl',
      ],
      [
        () => select('x').from('tbl')
          .with(
            'tbl',
            select('x').from('tbl2'),
            {
              recursive: true,
              materialized: false,
            },
          ),
        'WITH RECURSIVE tbl AS NOT MATERIALIZED (SELECT x FROM tbl2) SELECT x FROM tbl',
      ],
      [
        () => select('x').from('tbl')
          .with('tbl', select('x').from('tbl2')),
        'WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl',
      ],
      [
        () => select('x').from('tbl')
          .with('tbl (x, y)', select(['x', 'y']).from('tbl2')),
        'WITH tbl(x, y) AS (SELECT x, y FROM tbl2) SELECT x FROM tbl',
      ],
      [
        () => select('x').from('tbl')
          .with('tbl', select('x').from('tbl2'))
          .with('tbl2', select('x').from('tbl3')),
        'WITH tbl AS (SELECT x FROM tbl2), tbl2 AS (SELECT x FROM tbl3) SELECT x FROM tbl',
      ],
      [
        () => select('x').from('tbl')
          .with('tbl', select(['x', 'y']).from('tbl2'))
          .select('y'),
        'WITH tbl AS (SELECT x, y FROM tbl2) SELECT x, y FROM tbl',
      ],
      [
        () => select('x').with('tbl', select('x').from('tbl2'))
          .from('tbl'),
        'WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl',
      ],
      [
        () => select('x').with('tbl', select('x').from('tbl2'))
          .from('tbl')
          .groupBy('x'),
        'WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl GROUP BY x',
      ],
      [
        () => select('x').with('tbl', select('x').from('tbl2'))
          .from('tbl')
          .orderBy('x'),
        'WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl ORDER BY x',
      ],
      [
        () => select('x').with('tbl', select('x').from('tbl2'))
          .from('tbl')
          .limit(10),
        'WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl LIMIT 10',
      ],
      [
        () => select('x').with('tbl', select('x').from('tbl2'))
          .from('tbl')
          .offset(10),
        'WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl OFFSET 10',
      ],
      [
        () => select('x').with('tbl', select('x').from('tbl2'))
          .from('tbl')
          .join('tbl3'),
        'WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl, tbl3',
      ],
      [
        () => select('x').with('tbl', select('x').from('tbl2'))
          .from('tbl')
          .distinct(),
        'WITH tbl AS (SELECT x FROM tbl2) SELECT DISTINCT x FROM tbl',
      ],
      [
        () => select('x').with('tbl', select('x').from('tbl2'))
          .from('tbl')
          .where('x > 10'),
        'WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl WHERE x > 10',
      ],
      [
        () => select('x').with('tbl', select('x').from('tbl2'))
          .from('tbl')
          .having('x > 20'),
        'WITH tbl AS (SELECT x FROM tbl2) SELECT x FROM tbl HAVING x > 20',
      ],
      [
        () => select('x').from('tbl')
          .subquery(),
        '(SELECT x FROM tbl)',
      ],
      [
        () => select('x').from('tbl')
          .subquery('y'),
        '(SELECT x FROM tbl) AS y',
      ],
      [
        () => select('x').from(select('x').from('tbl')
          .subquery()),
        'SELECT x FROM (SELECT x FROM tbl)',
      ],
      [() => from('tbl').select('x'), 'SELECT x FROM tbl'],
      [
        () => parseOne('SELECT a FROM tbl').assertIs(SelectExpr)
          .select('b'),
        'SELECT a, b FROM tbl',
      ],
      [
        () => parseOne('SELECT * FROM y').assertIs(SelectExpr)
          .ctas('x'),
        'CREATE TABLE x AS SELECT * FROM y',
      ],
      [
        () => parseOne('SELECT * FROM y').assertIs(SelectExpr)
          .ctas('foo.x', {
            properties: {
              format: 'parquet',
              y: '2',
            },
          }),
        'CREATE TABLE foo.x STORED AS PARQUET TBLPROPERTIES (\'y\'=\'2\') AS SELECT * FROM y',
        'hive',
      ],
      [() => and(['x=1', 'y=1']), 'x = 1 AND y = 1'],
      [
        () => condition('x').and('y[\'a\']')
          .and('1'),
        '(x AND y[\'a\']) AND 1',
      ],
      [() => condition('x=1').and('y=1'), 'x = 1 AND y = 1'],
      [
        () => and([
          'x=1',
          'y=1',
          'z=1',
        ]),
        'x = 1 AND y = 1 AND z = 1',
      ],
      [() => condition('x=1').and(['y=1', 'z=1']), 'x = 1 AND y = 1 AND z = 1'],
      [() => and(['x=1', and(['y=1', 'z=1'])]), 'x = 1 AND (y = 1 AND z = 1)'],
      [
        () => condition('x=1').and('y=1')
          .and('z=1'),
        '(x = 1 AND y = 1) AND z = 1',
      ],
      [() => or([and(['x=1', 'y=1']), 'z=1']), '(x = 1 AND y = 1) OR z = 1'],
      [
        () => condition('x=1').and('y=1')
          .or('z=1'),
        '(x = 1 AND y = 1) OR z = 1',
      ],
      [() => or(['z=1', and(['x=1', 'y=1'])]), 'z = 1 OR (x = 1 AND y = 1)'],
      [() => or(['z=1 OR a=1', and(['x=1', 'y=1'])]), '(z = 1 OR a = 1) OR (x = 1 AND y = 1)'],
      [() => not('x=1'), 'NOT x = 1'],
      [() => condition('x=1').not(), 'NOT x = 1'],
      [
        () => condition('x=1').and('y=1')
          .not(),
        'NOT (x = 1 AND y = 1)',
      ],
      [
        () => select('*').from('x')
          .where(condition('y=1').and('z=1')),
        'SELECT * FROM x WHERE y = 1 AND z = 1',
      ],
      [
        () => subquery('select x from tbl', 'foo').select('x')
          .where('x > 0'),
        'SELECT x FROM (SELECT x FROM tbl) AS foo WHERE x > 0',
      ],
      [() => subquery('select x from tbl UNION select x from bar', 'unioned').select('x'), 'SELECT x FROM (SELECT x FROM tbl UNION SELECT x FROM bar) AS unioned'],
      [() => parseOne('(SELECT 1)').select('2'), '(SELECT 1, 2)'],
      [() => parseOne('(SELECT 1)').limit(1), '(SELECT 1) LIMIT 1'],
      [() => parseOne('WITH t AS (SELECT 1) (SELECT 1)').limit(1), 'WITH t AS (SELECT 1) SELECT 1 LIMIT 1'],
      [() => parseOne('(SELECT 1 LIMIT 2)').limit(1), '(SELECT 1 LIMIT 2) LIMIT 1'],
      [
        () => parseOne('SELECT 1 UNION SELECT 2', { into: SelectExpr }).limit(5)
          .offset(2),
        'SELECT 1 UNION SELECT 2 LIMIT 5 OFFSET 2',
      ],
      [() => parseOne('(SELECT 1)').subquery(), '((SELECT 1))'],
      [() => parseOne('(SELECT 1)').subquery('alias'), '((SELECT 1)) AS alias'],
      [() => parseOne('(select * from foo)').with('foo', 'select 1 as c'), 'WITH foo AS (SELECT 1 AS c) (SELECT * FROM foo)'],
      [
        () => update('tbl', {
          x: undefined,
          y: { x: 1 },
        }),
        'UPDATE tbl SET x = NULL, y = MAP(ARRAY(\'x\'), ARRAY(1))',
      ],
      [() => update('tbl', { x: 1 }, { where: 'y > 0' }), 'UPDATE tbl SET x = 1 WHERE y > 0'],
      [() => update('tbl', { x: 1 }, { where: condition('y > 0') }), 'UPDATE tbl SET x = 1 WHERE y > 0'],
      [() => update('tbl', { x: 1 }, { from: 'tbl2' }), 'UPDATE tbl SET x = 1 FROM tbl2'],
      [() => update('tbl', { x: 1 }, { from: 'tbl2 cross join tbl3' }), 'UPDATE tbl SET x = 1 FROM tbl2 CROSS JOIN tbl3'],
      [
        () => update('my_table', { x: 1 }, {
          from: 'baz',
          where: 'my_table.id = baz.id',
          with: { baz: 'SELECT id FROM foo UNION SELECT id FROM bar' },
        }),
        'WITH baz AS (SELECT id FROM foo UNION SELECT id FROM bar) UPDATE my_table SET x = 1 FROM baz WHERE my_table.id = baz.id',
      ],
      [() => update('my_table').set('x = 1'), 'UPDATE my_table SET x = 1'],
      [
        () => update('my_table').set('x = 1')
          .where('y = 2'),
        'UPDATE my_table SET x = 1 WHERE y = 2',
      ],
      [
        () => update('my_table').set('a = 1')
          .set('b = 2'),
        'UPDATE my_table SET a = 1, b = 2',
      ],
      [
        () => update('my_table').set('x = 1')
          .where('my_table.id = baz.id')
          ?.from('baz')
          .with('baz', 'SELECT id FROM foo'),
        'WITH baz AS (SELECT id FROM foo) UPDATE my_table SET x = 1 FROM baz WHERE my_table.id = baz.id',
      ],
      [() => union(['SELECT * FROM foo', 'SELECT * FROM bla']), 'SELECT * FROM foo UNION SELECT * FROM bla'],
      [() => parseOne('SELECT * FROM foo', { into: SelectExpr }).union('SELECT * FROM bla'), 'SELECT * FROM foo UNION SELECT * FROM bla'],
      [() => intersect(['SELECT * FROM foo', 'SELECT * FROM bla']), 'SELECT * FROM foo INTERSECT SELECT * FROM bla'],
      [() => parseOne('SELECT * FROM foo', { into: SelectExpr }).intersect('SELECT * FROM bla'), 'SELECT * FROM foo INTERSECT SELECT * FROM bla'],
      [() => except(['SELECT * FROM foo', 'SELECT * FROM bla']), 'SELECT * FROM foo EXCEPT SELECT * FROM bla'],
      [() => parseOne('SELECT * FROM foo', { into: SelectExpr }).except('SELECT * FROM bla'), 'SELECT * FROM foo EXCEPT SELECT * FROM bla'],
      [() => parseOne('(SELECT * FROM foo)').union('SELECT * FROM bla'), '(SELECT * FROM foo) UNION SELECT * FROM bla'],
      [() => parseOne('(SELECT * FROM foo)').union('SELECT * FROM bla', { distinct: false }), '(SELECT * FROM foo) UNION ALL SELECT * FROM bla'],
      [() => alias(parseOne('LAG(x) OVER (PARTITION BY y)'), 'a'), 'LAG(x) OVER (PARTITION BY y) AS a'],
      [() => alias(parseOne('LAG(x) OVER (ORDER BY z)'), 'a'), 'LAG(x) OVER (ORDER BY z) AS a'],
      [() => alias(parseOne('LAG(x) OVER (PARTITION BY y ORDER BY z)'), 'a'), 'LAG(x) OVER (PARTITION BY y ORDER BY z) AS a'],
      [() => alias(parseOne('LAG(x) OVER ()'), 'a'), 'LAG(x) OVER () AS a'],
      [() => values([['1', 2]]), 'VALUES (\'1\', 2)'],
      [() => values([['1', 2]], { alias: 'alias' }), '(VALUES (\'1\', 2)) AS alias'],
      [() => values([['1', 2], ['2', 3]]), 'VALUES (\'1\', 2), (\'2\', 3)'],
      [
        () => values([
          [
            '1',
            2,
            undefined,
          ],
          [
            '2',
            3,
            undefined,
          ],
        ], {
          alias: 'alias',
          columns: [
            'col1',
            'col2',
            'col3',
          ],
        }),
        '(VALUES (\'1\', 2, NULL), (\'2\', 3, NULL)) AS alias(col1, col2, col3)',
      ],
      [() => delete_('y', { where: 'x > 1' }), 'DELETE FROM y WHERE x > 1'],
      [() => delete_('y', { where: and('x > 1') }), 'DELETE FROM y WHERE x > 1'],
      [
        () => select('AVG(a) OVER b').from('table')
          .window('b AS (PARTITION BY c ORDER BY d)'),
        'SELECT AVG(a) OVER b FROM table WINDOW b AS (PARTITION BY c ORDER BY d)',
      ],
      [
        () => select(['AVG(a) OVER b', 'MIN(c) OVER d']).from('table')
          .window('b AS (PARTITION BY e ORDER BY f)')
          .window('d AS (PARTITION BY g ORDER BY h)'),
        'SELECT AVG(a) OVER b, MIN(c) OVER d FROM table WINDOW b AS (PARTITION BY e ORDER BY f), d AS (PARTITION BY g ORDER BY h)',
      ],
      [
        () => select('*').from('table')
          .qualify('row_number() OVER (PARTITION BY a ORDER BY b) = 1'),
        'SELECT * FROM table QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) = 1',
      ],
      [() => delete_('tbl1', { where: 'x = 1' }).delete('tbl2'), 'DELETE FROM tbl2 WHERE x = 1'],
      [() => delete_('tbl').where('x = 1'), 'DELETE FROM tbl WHERE x = 1'],
      [() => delete_(table('tbl')), 'DELETE FROM tbl'],
      [() => delete_('tbl', { where: 'x = 1' }).where('y = 2'), 'DELETE FROM tbl WHERE x = 1 AND y = 2'],
      [() => delete_('tbl', { where: 'x = 1' }).where(condition('y = 2').or('z = 3')), 'DELETE FROM tbl WHERE x = 1 AND (y = 2 OR z = 3)'],
      [
        () => delete_('tbl').where('x = 1')
          .returning('*', { dialect: 'postgres' }),
        'DELETE FROM tbl WHERE x = 1 RETURNING *',
        'postgres',
      ],
      [
        () => delete_('tbl', {
          where: 'x = 1',
          returning: '*',
          dialect: 'postgres',
        }),
        'DELETE FROM tbl WHERE x = 1 RETURNING *',
        'postgres',
      ],
      [() => insert('SELECT * FROM tbl2', 'tbl'), 'INSERT INTO tbl SELECT * FROM tbl2'],
      [() => insert('SELECT * FROM tbl2', 'tbl', { returning: '*' }), 'INSERT INTO tbl SELECT * FROM tbl2 RETURNING *'],
      [() => insert('SELECT * FROM tbl2', 'tbl', { overwrite: true }), 'INSERT OVERWRITE TABLE tbl SELECT * FROM tbl2'],
      [() => insert('VALUES (1, 2), (3, 4)', 'tbl', { columns: ['cola', 'colb'] }), 'INSERT INTO tbl (cola, colb) VALUES (1, 2), (3, 4)'],
      [() => insert('VALUES (1), (2)', 'tbl', { columns: ['col a'] }), 'INSERT INTO tbl ("col a") VALUES (1), (2)'],
      [() => insert('SELECT * FROM cte', 't').with('cte', 'SELECT x FROM tbl'), 'WITH cte AS (SELECT x FROM tbl) INSERT INTO t SELECT * FROM cte'],
      [
        () => insert('SELECT * FROM cte', 't').with('cte', 'SELECT x FROM tbl', {
          materialized: true,
        }),
        'WITH cte AS MATERIALIZED (SELECT x FROM tbl) INSERT INTO t SELECT * FROM cte',
      ],
      [
        () => insert('SELECT * FROM cte', 't').with('cte', 'SELECT x FROM tbl', {
          materialized: false,
        }),
        'WITH cte AS NOT MATERIALIZED (SELECT x FROM tbl) INSERT INTO t SELECT * FROM cte',
      ],
      [
        () => new TupleExpr({ expressions: [column({ col: 'x' }), column({ col: 'y' })] }).in([new TupleExpr({ expressions: [LiteralExpr.number(1), LiteralExpr.number(2)] }), new TupleExpr({ expressions: [LiteralExpr.number(3), LiteralExpr.number(4)] })]),
        '(x, y) IN ((1, 2), (3, 4))',
        'postgres',
      ],
      [() => cast('CAST(x AS INT)', 'int'), 'CAST(x AS INT)'],
      [() => cast('CAST(x AS TEXT)', 'int'), 'CAST(CAST(x AS TEXT) AS INT)'],
      [() => renameColumn('table1', 'c1', 'c2', { exists: true }), 'ALTER TABLE table1 RENAME COLUMN IF EXISTS c1 TO c2'],
      [() => renameColumn('table1', 'c1', 'c2', { exists: false }), 'ALTER TABLE table1 RENAME COLUMN c1 TO c2'],
      [() => renameColumn('table1', 'c1', 'c2'), 'ALTER TABLE table1 RENAME COLUMN c1 TO c2'],
      [
        () => merge(['WHEN MATCHED THEN UPDATE SET col1 = source.col1', 'WHEN NOT MATCHED THEN INSERT (col1) VALUES (source.col1)'], {
          into: 'target_table',
          using: 'source_table',
          on: 'target_table.id = source_table.id',
        }),
        'MERGE INTO target_table USING source_table ON target_table.id = source_table.id WHEN MATCHED THEN UPDATE SET col1 = source.col1 WHEN NOT MATCHED THEN INSERT (col1) VALUES (source.col1)',
      ],
      [
        () => merge([
          'WHEN MATCHED AND source.is_deleted = 1 THEN DELETE',
          'WHEN MATCHED THEN UPDATE SET val = source.val',
          'WHEN NOT MATCHED THEN INSERT (id, val) VALUES (source.id, source.val)',
        ], {
          into: 'target_table',
          using: 'source_table',
          on: 'target_table.id = source_table.id',
        }),
        'MERGE INTO target_table USING source_table ON target_table.id = source_table.id WHEN MATCHED AND source.is_deleted = 1 THEN DELETE WHEN MATCHED THEN UPDATE SET val = source.val WHEN NOT MATCHED THEN INSERT (id, val) VALUES (source.id, source.val)',
      ],
      [
        () => merge('WHEN MATCHED THEN UPDATE SET target.name = source.name', {
          into: table('target_table').as('target'),
          using: table('source_table').as('source'),
          on: 'target.id = source.id',
        }),
        'MERGE INTO target_table AS target USING source_table AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.name = source.name',
      ],
      [
        () => merge('WHEN MATCHED THEN UPDATE SET target.name = source.name', {
          into: table('target_table').as('target'),
          using: table('source_table').as('source'),
          on: 'target.id = source.id',
          returning: 'target.*',
        }),
        'MERGE INTO target_table AS target USING source_table AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.name = source.name RETURNING target.*',
      ],
      [
        () => merge(new WhenExpr({
          matched: true,
          then: new UpdateExpr({
            expressions: [
              column({
                col: 'name',
                table: 'target',
              }).eq(column({
                col: 'name',
                table: 'source',
              })),
            ],
          }),
        }), {
          into: table('target_table').as('target'),
          using: table('source_table').as('source'),
          on: 'target.id = source.id',
          returning: 'target.*',
        }),
        'MERGE INTO target_table AS target USING source_table AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.name = source.name RETURNING target.*',
      ],
      [
        () => union([
          'SELECT 1',
          'SELECT 2',
          'SELECT 3',
          'SELECT 4',
        ]),
        'SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4',
      ],
      [
        () => select('x').with('var1', select('x').from('tbl2')
          .subquery(), {
          scalar: true,
        })
          .from('tbl')
          .where('x > var1'),
        'WITH (SELECT x FROM tbl2) AS var1 SELECT x FROM tbl WHERE x > var1',
        'clickhouse',
      ],
      [
        () => select('x').with('var1', select('x').from('tbl2'), {
          scalar: true,
        })
          .from('tbl')
          .where('x > var1'),
        'WITH (SELECT x FROM tbl2) AS var1 SELECT x FROM tbl WHERE x > var1',
        'clickhouse',
      ],
    ];

    testCases.forEach(([
      expression,
      expectedSql,
      dialect,
    ]) => {
      expect(expression()?.sql({ dialect })).toBe(expectedSql);
    });
  });
});
