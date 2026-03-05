import {
  describe, test, expect,
} from 'vitest';
import { execute } from '../src/executor/index';
import { ExecuteError } from '../src/errors';
import {
  Table, ensureTables,
} from '../src/executor/table';
import { optimize } from '../src/optimizer/optimizer';
import { Plan } from '../src/planner';
import { parseOne } from '../src/parser';
import {
  select, alias,
  LiteralExpr,
  SelectExpr,
  table,
} from '../src/expressions';

// Helper to sort rows for set comparisons
function rowSet (rows: unknown[][]): Set<string> {
  return new Set(rows.map((r) => JSON.stringify(r)));
}

class TestExecutor {
  testExecuteCallable () {
    const tables: Record<string, Record<string, string>[]> = {
      x: [
        {
          a: 'a',
          b: 'd',
        },
        {
          a: 'b',
          b: 'e',
        },
        {
          a: 'c',
          b: 'f',
        },
      ],
      y: [
        {
          b: 'd',
          c: 'g',
        },
        {
          b: 'e',
          c: 'h',
        },
        {
          b: 'f',
          c: 'i',
        },
      ],
      z: [],
    };
    const schema = {
      x: {
        a: 'VARCHAR',
        b: 'VARCHAR',
      },
      y: {
        b: 'VARCHAR',
        c: 'VARCHAR',
      },
      z: { d: 'VARCHAR' },
    };

    const cases: [string, string[], unknown[][]][] = [
      [
        'SELECT * FROM x',
        ['a', 'b'],
        [
          ['a', 'd'],
          ['b', 'e'],
          ['c', 'f'],
        ],
      ],
      [
        'SELECT * FROM x JOIN y ON x.b = y.b',
        [
          'a',
          'b',
          'b',
          'c',
        ],
        [
          [
            'a',
            'd',
            'd',
            'g',
          ],
          [
            'b',
            'e',
            'e',
            'h',
          ],
          [
            'c',
            'f',
            'f',
            'i',
          ],
        ],
      ],
      [
        'SELECT j.c AS d FROM x AS i JOIN y AS j ON i.b = j.b',
        ['d'],
        [
          ['g'],
          ['h'],
          ['i'],
        ],
      ],
      [
        'SELECT CONCAT(x.a, y.c) FROM x JOIN y ON x.b = y.b WHERE y.b = \'e\'',
        ['_col_0'],
        [['bh']],
      ],
      [
        'SELECT * FROM x JOIN y ON x.b = y.b WHERE y.b = \'e\'',
        [
          'a',
          'b',
          'b',
          'c',
        ],
        [
          [
            'b',
            'e',
            'e',
            'h',
          ],
        ],
      ],
      [
        'SELECT * FROM z',
        ['d'],
        [],
      ],
      [
        'SELECT d FROM z ORDER BY d',
        ['d'],
        [],
      ],
      [
        'SELECT a FROM x WHERE x.a <> \'b\'',
        ['a'],
        [['a'], ['c']],
      ],
      [
        'SELECT a AS i FROM x ORDER BY a',
        ['i'],
        [
          ['a'],
          ['b'],
          ['c'],
        ],
      ],
      [
        'SELECT a AS i FROM x ORDER BY i',
        ['i'],
        [
          ['a'],
          ['b'],
          ['c'],
        ],
      ],
      [
        'SELECT 100 - ORD(a) AS a, a AS i FROM x ORDER BY a',
        ['a', 'i'],
        [
          [1, 'c'],
          [2, 'b'],
          [3, 'a'],
        ],
      ],
      [
        'SELECT a /* test */ FROM x LIMIT 1',
        ['a'],
        [['a']],
      ],
      [
        'SELECT DISTINCT a FROM (SELECT 1 AS a UNION ALL SELECT 1 AS a)',
        ['a'],
        [[1]],
      ],
      [
        'SELECT DISTINCT a, SUM(b) AS b FROM (SELECT \'a\' AS a, 1 AS b UNION ALL SELECT \'a\' AS a, 2 AS b UNION ALL SELECT \'b\' AS a, 1 AS b) GROUP BY a LIMIT 1',
        ['a', 'b'],
        [['a', 3]],
      ],
      [
        'SELECT COUNT(1) AS a FROM (SELECT 1)',
        ['a'],
        [[1]],
      ],
      [
        'SELECT COUNT(1) AS a FROM (SELECT 1) LIMIT 0',
        ['a'],
        [],
      ],
      [
        'SELECT a FROM x GROUP BY a LIMIT 0',
        ['a'],
        [],
      ],
      [
        'SELECT a FROM x LIMIT 0',
        ['a'],
        [],
      ],
    ];

    for (const [
      sql,
      cols,
      rows,
    ] of cases) {
      const result = execute(sql, schema, undefined, tables);
      expect(result.columns).toEqual(cols);
      expect(result.rows).toEqual(rows);
    }
  }

  testSetOperations () {
    const tables: Record<string, Record<string, string>[]> = {
      x: [
        { a: 'a' },
        { a: 'b' },
        { a: 'c' },
      ],
      y: [
        { a: 'b' },
        { a: 'c' },
        { a: 'd' },
      ],
    };
    const schema = {
      x: { a: 'VARCHAR' },
      y: { a: 'VARCHAR' },
    };

    const listCases: [string, string[], unknown[][]][] = [
      [
        'SELECT a FROM x UNION ALL SELECT a FROM y',
        ['a'],
        [
          ['a'],
          ['b'],
          ['c'],
          ['b'],
          ['c'],
          ['d'],
        ],
      ],
      [
        'SELECT a FROM x UNION SELECT a FROM y',
        ['a'],
        [
          ['a'],
          ['b'],
          ['c'],
          ['d'],
        ],
      ],
      [
        'SELECT a FROM x EXCEPT SELECT a FROM y',
        ['a'],
        [['a']],
      ],
      [
        '(SELECT a FROM x) EXCEPT (SELECT a FROM y)',
        ['a'],
        [['a']],
      ],
      [
        'SELECT a FROM x INTERSECT SELECT a FROM y',
        ['a'],
        [['b'], ['c']],
      ],
      [
        `SELECT i.a
        FROM (
          SELECT a FROM x UNION SELECT a FROM y
        ) AS i
        JOIN (
          SELECT a FROM x UNION SELECT a FROM y
        ) AS j
          ON i.a = j.a`,
        ['a'],
        [
          ['a'],
          ['b'],
          ['c'],
          ['d'],
        ],
      ],
      [
        'SELECT 1 AS a UNION SELECT 2 AS a UNION SELECT 3 AS a',
        ['a'],
        [
          [1],
          [2],
          [3],
        ],
      ],
      [
        'SELECT 1 / 2 AS a',
        ['a'],
        [[0.5]],
      ],
    ];

    for (const [
      sql,
      cols,
      rows,
    ] of listCases) {
      const result = execute(sql, schema, undefined, tables);
      expect(result.columns).toEqual(cols);
      expect(rowSet(result.rows)).toEqual(rowSet(rows));
    }

    // Test ZeroDivisionError
    expect(() => execute('SELECT 1 / 0 AS a', schema, undefined, tables)).toThrow(ExecuteError);

    // Test typed division (integer)
    const typedDiv = alias(LiteralExpr.number(1).div(LiteralExpr.number(2), { typed: true }), 'a');
    const typedResult = execute(select(typedDiv));
    expect(rowSet(typedResult.rows)).toEqual(rowSet([[0]]));

    // Test safe division (undefined on zero)
    const safeDiv = alias(LiteralExpr.number(1).div(LiteralExpr.number(0), { safe: true }), 'a');
    const safeResult = execute(select(safeDiv));
    expect(rowSet(safeResult.rows)).toEqual(rowSet([[undefined]]));

    // Test LIMIT on UNION ALL
    const limitResult = execute('SELECT a FROM x UNION ALL SELECT a FROM x LIMIT 1', schema, undefined, tables);
    expect(limitResult.columns).toEqual(['a']);
    expect(limitResult.rows).toEqual([['a']]);
  }

  testExecuteCatalogDbTable () {
    const tables = {
      catalog: {
        db: {
          x: [
            { a: 'a' },
            { a: 'b' },
            { a: 'c' },
          ],
        },
      },
    };
    const schema = {
      catalog: {
        db: {
          x: { a: 'VARCHAR' },
        },
      },
    };
    const result1 = execute('SELECT * FROM x', schema, undefined, tables);
    const result2 = execute('SELECT * FROM catalog.db.x', schema, undefined, tables);
    expect(result1.columns).toEqual(result2.columns);
    expect(result1.rows).toEqual(result2.rows);
  }

  testExecuteTables () {
    const tables: Record<string, Record<string, unknown>[]> = {
      sushi: [
        {
          id: 1,
          price: 1.0,
        },
        {
          id: 2,
          price: 2.0,
        },
        {
          id: 3,
          price: 3.0,
        },
      ],
      order_items: [
        {
          sushi_id: 1,
          order_id: 1,
        },
        {
          sushi_id: 1,
          order_id: 1,
        },
        {
          sushi_id: 2,
          order_id: 1,
        },
        {
          sushi_id: 3,
          order_id: 2,
        },
      ],
      orders: [
        {
          id: 1,
          user_id: 1,
        },
        {
          id: 2,
          user_id: 2,
        },
      ],
    };

    const result1 = execute(
      `SELECT
        o.user_id,
        SUM(s.price) AS price
      FROM orders o
      JOIN order_items i
        ON o.id = i.order_id
      JOIN sushi s
        ON i.sushi_id = s.id
      GROUP BY o.user_id`,
      undefined,
      undefined,
      tables,
    );
    expect(result1.rows).toEqual([[1, 4.0], [2, 3.0]]);

    const result2 = execute(
      `SELECT
        o.id, x.*
      FROM orders o
      LEFT JOIN (
          SELECT
            1 AS id, 'b' AS x
          UNION ALL
          SELECT
            3 AS id, 'c' AS x
      ) x
        ON o.id = x.id`,
      undefined,
      undefined,
      tables,
    );
    expect(result2.rows).toEqual([
      [
        1,
        1,
        'b',
      ],
      [
        2,
        undefined,
        undefined,
      ],
    ]);

    const result3 = execute(
      `SELECT
        o.id, x.*
      FROM orders o
      RIGHT JOIN (
          SELECT
            1 AS id,
            'b' AS x
          UNION ALL
          SELECT
            3 AS id, 'c' AS x
      ) x
        ON o.id = x.id`,
      undefined,
      undefined,
      tables,
    );
    expect(result3.rows).toEqual([
      [
        1,
        1,
        'b',
      ],
      [
        undefined,
        3,
        'c',
      ],
    ]);
  }

  testExecuteSubqueries () {
    const tables: Record<string, Record<string, number>[]> = {
      table: [
        {
          a: 1,
          b: 1,
        },
        {
          a: 2,
          b: 2,
        },
      ],
    };

    const result1 = execute(
      `SELECT *
      FROM table
      WHERE a = (SELECT MAX(a) FROM table)`,
      undefined,
      undefined,
      tables,
    );
    expect(result1.rows).toEqual([[2, 2]]);

    // Test CTE with subquery
    const table1View = parseOne('SELECT id, sub_type FROM table1', { into: SelectExpr }).subquery();
    const selectFromSubQuery = parseOne('SELECT id AS id_alias, sub_type FROM t', { into: SelectExpr }).from(table1View);
    const expression = parseOne('SELECT * FROM cte1', { into: SelectExpr }).with('cte1', selectFromSubQuery);

    const schema = {
      table1: {
        id: 'str',
        sub_type: 'str',
      },
    };
    const executed = execute(expression, schema, undefined, { table1: [] });

    expect(executed.rows).toEqual([]);
    expect(executed.columns).toEqual(['id_alias', 'sub_type']);
  }

  testCorrelatedCount () {
    const tables: Record<string, Record<string, number>[]> = {
      parts: [
        {
          pnum: 0,
          qoh: 1,
        },
      ],
      supplies: [],
    };
    const schema = {
      parts: {
        pnum: 'int',
        qoh: 'int',
      },
      supplies: {
        pnum: 'int',
        shipdate: 'int',
      },
    };

    const result = execute(
      `select *
      from parts
      where parts.qoh >= (
        select count(supplies.shipdate) + 1
        from supplies
        where supplies.pnum = parts.pnum and supplies.shipdate < 10
      )`,
      schema,
      undefined,
      tables,
    );
    expect(result.rows).toEqual([[0, 1]]);
  }

  testTableDepthMismatch () {
    const tables = { table: [] };
    const schema = { db: { table: { col: 'VARCHAR' } } };
    expect(() => execute('SELECT * FROM table', schema, undefined, tables)).toThrow(ExecuteError);
  }

  testTables () {
    const tables = ensureTables({
      catalog1: {
        db1: {
          t1: [{ a: 1 }],
          t2: [{ a: 1 }],
        },
        db2: {
          t3: [{ a: 1 }],
          t4: [{ a: 1 }],
        },
      },
      catalog2: {
        db3: {
          t5: new Table(['a'], [[1]]),
          t6: new Table(['a'], [[1]]),
        },
        db4: {
          t7: new Table(['a'], [[1]]),
          t8: new Table(['a'], [[1]]),
        },
      },
    });

    const t1 = tables.find(table('t1', 'db1', 'catalog1'));
    expect(typeof t1).toBe('object');
    expect(t1).toBeDefined();
    if (typeof t1 !== 'object' || t1 == undefined) {
      throw new Error('Unreachable');
    }
    expect(t1['columns']).toEqual(['a', undefined]);
    expect(t1['rows']).toEqual([1, undefined]);

    const t8 = tables.find(table('t8'));
    expect(typeof t8).toBe('object');
    expect(t8).toBeDefined();
    if (typeof t8 !== 'object' || t8 == undefined) {
      throw new Error('Unreachable');
    }
    expect(t8['columns']).toEqual(t1['columns']);
    expect(t8['rows']).toEqual(t1['rows']);
  }

  testStaticQueries () {
    const cases: [string, string[], unknown[][]][] = [
      [
        'SELECT 1',
        ['1'],
        [[1]],
      ],
      [
        'SELECT 1 + 2 AS x',
        ['x'],
        [[3]],
      ],
      [
        'SELECT CONCAT(\'a\', \'b\') AS x',
        ['x'],
        [['ab']],
      ],
      [
        'SELECT CONCAT(\'a\', 1) AS x',
        ['x'],
        [['a1']],
      ],
      [
        'SELECT 1 AS x, 2 AS y',
        ['x', 'y'],
        [[1, 2]],
      ],
      [
        'SELECT \'foo\' LIMIT 1',
        ['foo'],
        [['foo']],
      ],
      [
        'SELECT SUM(x), COUNT(x) FROM (SELECT 1 AS x WHERE FALSE)',
        ['_col_0', '_col_1'],
        [[undefined, 0]],
      ],
    ];

    for (const [
      sql,
      cols,
      rows,
    ] of cases) {
      const result = execute(sql);
      expect(result.columns).toEqual(cols);
      expect(result.rows).toEqual(rows);
    }
  }

  testAggregateWithoutGroupBy () {
    const result = execute('SELECT SUM(x) FROM t', undefined, undefined, { t: [{ x: 1 }, { x: 2 }] });
    expect(result.columns).toEqual(['_col_0']);
    expect(result.rows).toEqual([[3]]);
  }

  testScalarFunctions () {
    const now = new Date();

    const cases: [string, unknown][] = [
      ['CONCAT(\'a\', \'b\')', 'ab'],
      ['CONCAT(\'a\', NULL)', undefined],
      ['CONCAT_WS(\'_\', \'a\', \'b\')', 'a_b'],
      ['STR_POSITION(\'foobarbar\', \'bar\')', 4],
      ['STR_POSITION(\'foobarbar\', \'bar\', 5)', 7],
      ['STR_POSITION(\'foobarbar\', NULL)', undefined],
      ['STR_POSITION(NULL, \'bar\')', undefined],
      ['UPPER(\'foo\')', 'FOO'],
      ['UPPER(NULL)', undefined],
      ['LOWER(\'FOO\')', 'foo'],
      ['LOWER(NULL)', undefined],
      ['IFNULL(\'a\', \'b\')', 'a'],
      ['IFNULL(NULL, \'b\')', 'b'],
      ['IFNULL(NULL, NULL)', undefined],
      ['SUBSTRING(\'12345\')', '12345'],
      ['SUBSTRING(\'12345\', 3)', '345'],
      ['SUBSTRING(\'12345\', 3, 0)', ''],
      ['SUBSTRING(\'12345\', 3, 1)', '3'],
      ['SUBSTRING(\'12345\', 3, 2)', '34'],
      ['SUBSTRING(\'12345\', 3, 3)', '345'],
      ['SUBSTRING(\'12345\', 3, 4)', '345'],
      ['SUBSTRING(\'12345\', -3)', '345'],
      ['SUBSTRING(\'12345\', -3, 0)', ''],
      ['SUBSTRING(\'12345\', -3, 1)', '3'],
      ['SUBSTRING(\'12345\', -3, 2)', '34'],
      ['SUBSTRING(\'12345\', 0)', ''],
      ['SUBSTRING(\'12345\', 0, 1)', ''],
      ['SUBSTRING(NULL)', undefined],
      ['SUBSTRING(NULL, 1)', undefined],
      ['CAST(1 AS TEXT)', '1'],
      ['CAST(\'1\' AS LONG)', 1],
      ['CAST(\'1.1\' AS FLOAT)', 1.1],
      ['COALESCE(NULL)', undefined],
      ['COALESCE(NULL, NULL)', undefined],
      ['COALESCE(NULL, \'b\')', 'b'],
      ['COALESCE(\'a\', \'b\')', 'a'],
      ['1 << 1', 2],
      ['1 >> 1', 0],
      ['1 & 1', 1],
      ['1 | 1', 1],
      ['1 < 1', false],
      ['1 <= 1', true],
      ['1 > 1', false],
      ['1 >= 1', true],
      ['1 + NULL', undefined],
      ['IF(true, 1, 0)', 1],
      ['IF(false, 1, 0)', 0],
      ['CASE WHEN 0 = 1 THEN \'foo\' ELSE \'bar\' END', 'bar'],
      ['1 IN (1, 2, 3)', true],
      ['1 IN (2, 3)', false],
      ['1 IN (1)', true],
      ['NULL IS NULL', true],
      ['NULL IS NOT NULL', false],
      ['NULL = NULL', undefined],
      ['NULL <> NULL', undefined],
      ['YEAR(CURRENT_TIMESTAMP)', now.getFullYear()],
      ['MONTH(CURRENT_TIME)', now.getMonth() + 1],
      ['YEAR(CURRENT_TIMESTAMP) + 1', now.getFullYear() + 1],
      ['YEAR(CURRENT_TIMESTAMP) = (YEAR(CURRENT_TIMESTAMP))', true],
      ['YEAR(CURRENT_TIMESTAMP) <> (YEAR(CURRENT_TIMESTAMP))', false],
      ['LEFT(\'12345\', 3)', '123'],
      ['RIGHT(\'12345\', 3)', '345'],
      ['TRIM(\' foo \')', 'foo'],
      ['TRIM(\'afoob\', \'ab\')', 'foo'],
      ['ARRAY_JOIN([\'foo\', \'bar\'], \':\')', 'foo:bar'],
      ['STRUCT(\'foo\', \'bar\', undefined, undefined)', { foo: 'bar' }],
      ['ROUND(1.5)', 2],
      ['ROUND(1.2)', 1],
      ['ROUND(1.2345, 2)', 1.23],
      ['ROUND(NULL)', undefined],
    ];

    for (const [sql, expected] of cases) {
      const result = execute(`SELECT ${sql}`);
      expect(result.rows).toEqual([[expected]]);
    }

    // Oracle NVL test
    const oracleResult = execute(
      'WITH t AS (SELECT \'a\' AS c1, \'b\' AS c2) SELECT NVL(c1, c2) FROM t',
      undefined,
      'oracle',
    );
    expect(oracleResult.rows).toEqual([['a']]);
  }

  testCaseSensitivity () {
    const result1 = execute('SELECT A AS A FROM X', undefined, undefined, { x: [{ a: 1 }] });
    expect(result1.columns).toEqual(['a']);
    expect(result1.rows).toEqual([[1]]);

    const result2 = execute('SELECT A AS "A" FROM X', undefined, undefined, { x: [{ a: 1 }] });
    expect(result2.columns).toEqual(['A']);
    expect(result2.rows).toEqual([[1]]);
  }

  testNestedTableReference () {
    const tables = {
      some_catalog: {
        some_schema: {
          some_table: [
            {
              id: 1,
              price: 1.0,
            },
            {
              id: 2,
              price: 2.0,
            },
            {
              id: 3,
              price: 3.0,
            },
          ],
        },
      },
    };

    const result = execute('SELECT * FROM some_catalog.some_schema.some_table s', undefined, undefined, tables);
    expect(result.columns).toEqual(['id', 'price']);
    expect(result.rows).toEqual([
      [1, 1.0],
      [2, 2.0],
      [3, 3.0],
    ]);
  }

  testGroupBy () {
    const tables = {
      x: [
        {
          a: 1,
          b: 10,
        },
        {
          a: 2,
          b: 20,
        },
        {
          a: 3,
          b: 28,
        },
        {
          a: 2,
          b: 25,
        },
        {
          a: 1,
          b: 40,
        },
      ],
    };

    const cases: [string, unknown[][], [string, ...string[]]][] = [
      [
        'SELECT a, AVG(b) FROM x GROUP BY a ORDER BY AVG(b)',
        [
          [2, 22.5],
          [1, 25.0],
          [3, 28.0],
        ],
        ['a', '_col_1'],
      ],
      [
        'SELECT a, AVG(b) FROM x GROUP BY a having avg(b) > 23',
        [[1, 25.0], [3, 28.0]],
        ['a', '_col_1'],
      ],
      [
        'SELECT a, AVG(b) FROM x GROUP BY a having avg(b + 1) > 23',
        [
          [1, 25.0],
          [2, 22.5],
          [3, 28.0],
        ],
        ['a', '_col_1'],
      ],
      [
        'SELECT a, AVG(b) FROM x GROUP BY a having sum(b) + 5 > 50',
        [[1, 25.0]],
        ['a', '_col_1'],
      ],
      [
        'SELECT a + 1 AS a, AVG(b + 1) FROM x GROUP BY a + 1 having AVG(b + 1) > 26',
        [[4, 29.0]],
        ['a', '_col_1'],
      ],
      [
        'SELECT a, avg(b) FROM x GROUP BY a HAVING a = 1',
        [[1, 25.0]],
        ['a', '_col_1'],
      ],
      [
        'SELECT a + 1, avg(b) FROM x GROUP BY a + 1 HAVING a + 1 = 2',
        [[2, 25.0]],
        ['_col_0', '_col_1'],
      ],
      [
        'SELECT a FROM x GROUP BY a ORDER BY AVG(b)',
        [
          [2],
          [1],
          [3],
        ],
        ['a'],
      ],
      [
        'SELECT a, SUM(b) FROM x GROUP BY a ORDER BY COUNT(*)',
        [
          [3, 28],
          [1, 50],
          [2, 45],
        ],
        ['a', '_col_1'],
      ],
      [
        'SELECT a, SUM(b) FROM x GROUP BY a ORDER BY COUNT(*) DESC',
        [
          [1, 50],
          [2, 45],
          [3, 28],
        ],
        ['a', '_col_1'],
      ],
    ];

    for (const [
      sql,
      expected,
      columns,
    ] of cases) {
      const result = execute(sql, undefined, undefined, tables);
      expect(result.columns).toEqual(columns);
      expect(result.rows).toEqual(expected);
    }
  }

  testNestedValues () {
    const fooTables = {
      foo: [
        {
          raw: {
            name: 'Hello, World',
            a: [{ b: 1 }],
          },
        },
      ],
    };

    const r1 = execute('SELECT raw:name AS name FROM foo', undefined, 'snowflake', fooTables);
    expect(r1.columns).toEqual(['NAME']);
    expect(r1.rows).toEqual([['Hello, World']]);

    const r2 = execute('SELECT raw:a[0].b AS b FROM foo', undefined, 'snowflake', fooTables);
    expect(r2.columns).toEqual(['B']);
    expect(r2.rows).toEqual([[1]]);

    const r3 = execute('SELECT raw:a[1].b AS b FROM foo', undefined, 'snowflake', fooTables);
    expect(r3.columns).toEqual(['B']);
    expect(r3.rows).toEqual([[undefined]]);

    const r4 = execute('SELECT raw:a[0].c AS c FROM foo', undefined, 'snowflake', fooTables);
    expect(r4.columns).toEqual(['C']);
    expect(r4.rows).toEqual([[undefined]]);

    const itemTables = {
      '"ITEM"': [
        {
          id: 1,
          attributes: {
            flavor: 'cherry',
            taste: 'sweet',
          },
        },
        {
          id: 2,
          attributes: {
            flavor: 'lime',
            taste: 'sour',
          },
        },
        {
          id: 3,
          attributes: {
            flavor: 'apple',
            taste: undefined,
          },
        },
      ],
    };
    const r5 = execute('SELECT i.attributes.flavor FROM `ITEM` i', undefined, 'bigquery', itemTables);
    expect(r5.columns).toEqual(['flavor']);
    expect(r5.rows).toEqual([
      ['cherry'],
      ['lime'],
      ['apple'],
    ]);

    const arrayTables = {
      t: [
        {
          x: [
            1,
            2,
            3,
          ],
        },
      ],
    };
    const r6 = execute('SELECT x FROM t', undefined, 'duckdb', arrayTables);
    expect(r6.columns).toEqual(['x']);
    expect(r6.rows).toEqual([
      [
        [
          1,
          2,
          3,
        ],
      ],
    ]);
  }

  testAggOrder () {
    const plan = new Plan(
      optimize(`
        SELECT
          AVG(bill_length_mm) AS avg_bill_length,
          AVG(bill_depth_mm) AS avg_bill_depth
        FROM penguins
      `),
    );

    expect(plan.root?.aggregations.map((agg) => (agg as { alias?: string }).alias)).toEqual(['avg_bill_length', 'avg_bill_depth']);
  }

  testTableToJslist () {
    const columns = [
      'id',
      'product',
      'price',
    ];
    const rows: unknown[][] = [
      [
        1,
        'Shirt',
        20.0,
      ],
      [
        2,
        'Shoes',
        60.0,
      ],
    ];
    const table = new Table(columns, rows);
    const expected = [
      {
        id: 1,
        product: 'Shirt',
        price: 20.0,
      },
      {
        id: 2,
        product: 'Shoes',
        price: 60.0,
      },
    ];
    expect(table.toJslist()).toEqual(expected);
  }
}

const t = new TestExecutor();
describe('TestExecutor', () => {
  test('testExecuteCallable', () => t.testExecuteCallable());
  test('testSetOperations', () => t.testSetOperations());
  test('testExecuteCatalogDbTable', () => t.testExecuteCatalogDbTable());
  test('testExecuteTables', () => t.testExecuteTables());
  test('testExecuteSubqueries', () => t.testExecuteSubqueries());
  test('testCorrelatedCount', () => t.testCorrelatedCount());
  test('testTableDepthMismatch', () => t.testTableDepthMismatch());
  test('tables', () => t.testTables());
  test('testStaticQueries', () => t.testStaticQueries());
  test('testAggregateWithoutGroupBy', () => t.testAggregateWithoutGroupBy());
  test('testScalarFunctions', () => t.testScalarFunctions());
  test('testCaseSensitivity', () => t.testCaseSensitivity());
  test('testNestedTableReference', () => t.testNestedTableReference());
  test('testGroupBy', () => t.testGroupBy());
  test('testNestedValues', () => t.testNestedValues());
  test('testAggOrder', () => t.testAggOrder());
  test('testTableToJslist', () => t.testTableToJslist());
});
