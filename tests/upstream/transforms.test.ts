import {
  describe, test, expect,
} from 'vitest';
import type { Expression } from '../../src/expressions';
import {
  maybeParse,
  ArrayExpr,
} from '../../src/expressions';
import {
  eliminateDistinctOn,
  eliminateQualify,
  removePrecisionParameterizedTypes,
  eliminateJoinMarks,
  eliminateWindowClause,
  inheritStructFieldNames,
} from '../../src/transforms';

class TestTransforms {
  validate (
    transform: (expr: Expression) => Expression,
    sqlOrExpr: string | Expression,
    target: string,
    dialect?: string,
  ) {
    const expr = typeof sqlOrExpr === 'string'
      ? maybeParse(sqlOrExpr, { dialect })
      : sqlOrExpr;
    expect(expr.transform(transform).sql({ dialect })).toBe(target);
  }

  testEliminateDistinctOn () {
    this.validate(
      eliminateDistinctOn,
      'SELECT DISTINCT ON (a) a, b FROM x ORDER BY c DESC',
      'SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1',
    );
    this.validate(
      eliminateDistinctOn,
      'SELECT DISTINCT ON (a) a, b FROM x',
      'SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY a) AS _row_number FROM x) AS _t WHERE _row_number = 1',
    );
    this.validate(
      eliminateDistinctOn,
      'SELECT DISTINCT ON (a, b) a, b FROM x ORDER BY c DESC',
      'SELECT a, b FROM (SELECT a AS a, b AS b, ROW_NUMBER() OVER (PARTITION BY a, b ORDER BY c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1',
    );
    this.validate(
      eliminateDistinctOn,
      'SELECT DISTINCT a, b FROM x ORDER BY c DESC',
      'SELECT DISTINCT a, b FROM x ORDER BY c DESC',
    );
    this.validate(
      eliminateDistinctOn,
      'SELECT DISTINCT ON (_row_number) _row_number FROM x ORDER BY c DESC',
      'SELECT _row_number FROM (SELECT _row_number AS _row_number, ROW_NUMBER() OVER (PARTITION BY _row_number ORDER BY c DESC) AS _row_number_2 FROM x) AS _t WHERE _row_number_2 = 1',
    );
    this.validate(
      eliminateDistinctOn,
      'SELECT DISTINCT ON (x.a, x.b) x.a, x.b FROM x ORDER BY c DESC',
      'SELECT a, b FROM (SELECT x.a AS a, x.b AS b, ROW_NUMBER() OVER (PARTITION BY x.a, x.b ORDER BY c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1',
    );
    this.validate(
      eliminateDistinctOn,
      'SELECT DISTINCT ON (a) x.a, y.a FROM x CROSS JOIN y ORDER BY c DESC',
      'SELECT a, a_2 FROM (SELECT x.a AS a, y.a AS a_2, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) AS _row_number FROM x CROSS JOIN y) AS _t WHERE _row_number = 1',
    );
    this.validate(
      eliminateDistinctOn,
      'SELECT DISTINCT ON (a) a, a + b FROM x ORDER BY c DESC',
      'SELECT a, _col FROM (SELECT a AS a, a + b AS _col, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1',
    );
    this.validate(
      eliminateDistinctOn,
      'SELECT DISTINCT ON (a) * FROM x ORDER BY c DESC',
      'SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1',
    );
    this.validate(
      eliminateDistinctOn,
      'SELECT DISTINCT ON (a) a AS "A", b FROM x ORDER BY c DESC',
      'SELECT "A", b FROM (SELECT a AS "A", b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1',
    );
    this.validate(
      eliminateDistinctOn,
      'SELECT DISTINCT ON (a) "A", b FROM x ORDER BY c DESC',
      'SELECT "A", b FROM (SELECT "A" AS "A", b AS b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) AS _row_number FROM x) AS _t WHERE _row_number = 1',
    );
  }

  testEliminateQualify () {
    this.validate(
      eliminateQualify,
      'SELECT i, a + 1 FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p) = 1',
      'SELECT i, _c FROM (SELECT i, a + 1 AS _c, ROW_NUMBER() OVER (PARTITION BY p) AS _w, p FROM qt) AS _t WHERE _w = 1',
    );
    this.validate(
      eliminateQualify,
      'SELECT i FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1 AND p = 0',
      'SELECT i FROM (SELECT i, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS _w, p, o FROM qt) AS _t WHERE _w = 1 AND p = 0',
    );
    this.validate(
      eliminateQualify,
      'SELECT i, p, o FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1',
      'SELECT i, p, o FROM (SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS _w FROM qt) AS _t WHERE _w = 1',
    );
    this.validate(
      eliminateQualify,
      'SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS row_num FROM qt QUALIFY row_num = 1',
      'SELECT i, p, o, row_num FROM (SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS row_num FROM qt) AS _t WHERE row_num = 1',
    );
    this.validate(
      eliminateQualify,
      'SELECT * FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1',
      'SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS _w FROM qt) AS _t WHERE _w = 1',
    );
    this.validate(
      eliminateQualify,
      'SELECT c2, SUM(c3) OVER (PARTITION BY c2) AS r FROM t1 WHERE c3 < 4 GROUP BY c2, c3 HAVING SUM(c1) > 3 QUALIFY r IN (SELECT MIN(c1) FROM test GROUP BY c2 HAVING MIN(c1) > 3)',
      'SELECT c2, r FROM (SELECT c2, SUM(c3) OVER (PARTITION BY c2) AS r, c1 FROM t1 WHERE c3 < 4 GROUP BY c2, c3 HAVING SUM(c1) > 3) AS _t WHERE r IN (SELECT MIN(c1) FROM test GROUP BY c2 HAVING MIN(c1) > 3)',
    );
    this.validate(
      eliminateQualify,
      'SELECT x FROM y QUALIFY ROW_NUMBER() OVER (PARTITION BY p)',
      'SELECT x FROM (SELECT x, ROW_NUMBER() OVER (PARTITION BY p) AS _w, p FROM y) AS _t WHERE _w',
    );
    this.validate(
      eliminateQualify,
      'SELECT x AS z FROM y QUALIFY ROW_NUMBER() OVER (PARTITION BY z)',
      'SELECT z FROM (SELECT x AS z, ROW_NUMBER() OVER (PARTITION BY x) AS _w FROM y) AS _t WHERE _w',
    );
    this.validate(
      eliminateQualify,
      'SELECT SOME_UDF(x) AS z FROM y QUALIFY ROW_NUMBER() OVER (PARTITION BY x ORDER BY z)',
      'SELECT z FROM (SELECT SOME_UDF(x) AS z, ROW_NUMBER() OVER (PARTITION BY x ORDER BY SOME_UDF(x)) AS _w, x FROM y) AS _t WHERE _w',
    );
    this.validate(
      eliminateQualify,
      'SELECT x, t, x || t AS z FROM y QUALIFY ROW_NUMBER() OVER (PARTITION BY x ORDER BY z DESC)',
      'SELECT x, t, z FROM (SELECT x, t, x || t AS z, ROW_NUMBER() OVER (PARTITION BY x ORDER BY x || t DESC) AS _w FROM y) AS _t WHERE _w',
    );
    this.validate(
      eliminateQualify,
      'SELECT y.x AS x, y.t AS z FROM y QUALIFY ROW_NUMBER() OVER (PARTITION BY x ORDER BY x DESC, z)',
      'SELECT x, z FROM (SELECT y.x AS x, y.t AS z, ROW_NUMBER() OVER (PARTITION BY y.x ORDER BY y.x DESC, y.t) AS _w FROM y) AS _t WHERE _w',
    );
    this.validate(
      eliminateQualify,
      'select max(col) over (partition by col_id) as col, from some_table qualify row_number() over (partition by col_id order by col asc)=1',
      'SELECT col FROM (SELECT MAX(col) OVER (PARTITION BY col_id) AS col, ROW_NUMBER() OVER (PARTITION BY col_id ORDER BY MAX(col) OVER (PARTITION BY col_id) ASC) AS _w, col_id FROM some_table) AS _t WHERE _w = 1',
    );
  }

  testRemovePrecisionParameterizedTypes () {
    this.validate(
      removePrecisionParameterizedTypes,
      'SELECT CAST(1 AS DECIMAL(10, 2)), CAST(\'13\' AS VARCHAR(10))',
      'SELECT CAST(1 AS DECIMAL), CAST(\'13\' AS VARCHAR)',
    );
  }

  testEliminateJoinMarks () {
    for (const dialect of ['oracle', 'redshift']) {
      // No join marks => query remains unaffected
      this.validate(
        eliminateJoinMarks,
        'SELECT T1.d, T2.c FROM T1, T2 WHERE T1.x = T2.x (+) and T2.y (+) > 5',
        'SELECT T1.d, T2.c FROM T1 LEFT JOIN T2 ON T1.x = T2.x AND T2.y > 5',
        dialect,
      );
    }
  }

  testEliminateWindowClause () {
    this.validate(
      eliminateWindowClause,
      'SELECT purchases, LAST_VALUE(item) OVER (d) AS most_popular FROM Produce WINDOW a AS (PARTITION BY purchases), b AS (a ORDER BY purchases), c AS (b ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING), d AS (c)',
      'SELECT purchases, LAST_VALUE(item) OVER (PARTITION BY purchases ORDER BY purchases ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS most_popular FROM Produce',
    );
    this.validate(
      eliminateWindowClause,
      'SELECT LAST_VALUE(c) OVER (a) AS c2 FROM (SELECT LAST_VALUE(i) OVER (a) AS c FROM p WINDOW a AS (PARTITION BY x)) AS q(c) WINDOW a AS (PARTITION BY y)',
      'SELECT LAST_VALUE(c) OVER (PARTITION BY y) AS c2 FROM (SELECT LAST_VALUE(i) OVER (PARTITION BY x) AS c FROM p) AS q(c)',
    );
  }

  testInheritStructFieldNames () {
    function parseAndSetStructNameInheritance (sql: string): Expression {
      const ast = maybeParse(sql);
      for (const array of ast.findAll(ArrayExpr)) {
        array.setArgKey('structNameInheritance', true);
      }
      return ast;
    }

    // Basic case: field names inherited from first struct
    this.validate(
      inheritStructFieldNames,
      parseAndSetStructNameInheritance(
        'SELECT ARRAY(STRUCT(\'Alice\' AS name, 85 AS score), STRUCT(\'Bob\', 92), STRUCT(\'Diana\', 95))',
      ),
      'SELECT ARRAY(STRUCT(\'Alice\' AS name, 85 AS score), STRUCT(\'Bob\' AS name, 92 AS score), STRUCT(\'Diana\' AS name, 95 AS score))',
    );

    // Single struct in array: no inheritance needed
    this.validate(
      inheritStructFieldNames,
      parseAndSetStructNameInheritance(
        'SELECT ARRAY(STRUCT(\'Alice\' AS name, 85 AS score))',
      ),
      'SELECT ARRAY(STRUCT(\'Alice\' AS name, 85 AS score))',
    );

    // Empty array: no change
    this.validate(
      inheritStructFieldNames,
      parseAndSetStructNameInheritance('SELECT ARRAY()'),
      'SELECT ARRAY()',
    );

    // First struct has no field names: no inheritance
    this.validate(
      inheritStructFieldNames,
      parseAndSetStructNameInheritance(
        'SELECT ARRAY(STRUCT(\'Alice\', 85), STRUCT(\'Bob\', 92))',
      ),
      'SELECT ARRAY(STRUCT(\'Alice\', 85), STRUCT(\'Bob\', 92))',
    );

    // Mismatched field counts: skip inheritance
    this.validate(
      inheritStructFieldNames,
      parseAndSetStructNameInheritance(
        'SELECT ARRAY(STRUCT(\'Alice\' AS name, 85 AS score), STRUCT(\'Bob\'))',
      ),
      'SELECT ARRAY(STRUCT(\'Alice\' AS name, 85 AS score), STRUCT(\'Bob\'))',
    );

    // Struct already has field names: don't override
    this.validate(
      inheritStructFieldNames,
      parseAndSetStructNameInheritance(
        'SELECT ARRAY(STRUCT(\'Alice\' AS name, 85 AS score), STRUCT(\'Bob\' AS fullname, 92 AS points))',
      ),
      'SELECT ARRAY(STRUCT(\'Alice\' AS name, 85 AS score), STRUCT(\'Bob\' AS fullname, 92 AS points))',
    );

    // Mixed: some structs inherit, some already have names
    this.validate(
      inheritStructFieldNames,
      parseAndSetStructNameInheritance(
        'SELECT ARRAY(STRUCT(\'Alice\' AS name, 85 AS score), STRUCT(\'Bob\', 92), STRUCT(\'Carol\' AS name, 88 AS score), STRUCT(\'Diana\', 95))',
      ),
      'SELECT ARRAY(STRUCT(\'Alice\' AS name, 85 AS score), STRUCT(\'Bob\' AS name, 92 AS score), STRUCT(\'Carol\' AS name, 88 AS score), STRUCT(\'Diana\' AS name, 95 AS score))',
    );

    // Non-struct elements: no change
    this.validate(
      inheritStructFieldNames,
      parseAndSetStructNameInheritance('SELECT ARRAY(1, 2, 3)'),
      'SELECT ARRAY(1, 2, 3)',
    );

    // Multiple arrays: each processed independently
    this.validate(
      inheritStructFieldNames,
      parseAndSetStructNameInheritance(
        'SELECT ARRAY(STRUCT(\'Alice\' AS name, 85 AS score), STRUCT(\'Bob\', 92)), ARRAY(STRUCT(\'X\' AS col), STRUCT(\'Y\'))',
      ),
      'SELECT ARRAY(STRUCT(\'Alice\' AS name, 85 AS score), STRUCT(\'Bob\' AS name, 92 AS score)), ARRAY(STRUCT(\'X\' AS col), STRUCT(\'Y\' AS col))',
    );

    // Partial field names in first struct: inherit only the named ones
    this.validate(
      inheritStructFieldNames,
      parseAndSetStructNameInheritance(
        'SELECT ARRAY(STRUCT(\'Alice\' AS name, 85), STRUCT(\'Bob\', 92))',
      ),
      'SELECT ARRAY(STRUCT(\'Alice\' AS name, 85), STRUCT(\'Bob\', 92))',
    );
  }
}

const t = new TestTransforms();

describe('TestTransforms', () => {
  test('testEliminateDistinctOn', () => t.testEliminateDistinctOn());
  test('testEliminateQualify', () => t.testEliminateQualify());
  test('testEliminateWindowClause', () => t.testEliminateWindowClause());
  test('testInheritStructFieldNames', () => t.testInheritStructFieldNames());
  test('testRemovePrecisionParameterizedTypes', () => t.testRemovePrecisionParameterizedTypes());
  test('testEliminateJoinMarks', () => t.testEliminateJoinMarks());
});
