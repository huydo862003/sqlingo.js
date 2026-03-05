import {
  describe, test, expect,
} from 'vitest';
import { lineage } from '../src/lineage';
import { parseOne } from '../src/parser';
import { SqlglotError } from '../src/errors';
import type { SelectExpr } from '../src/expressions';

class TestLineage {
  testLineage () {
    const node = lineage(
      'a',
      'SELECT a FROM z',
      {
        schema: { x: { a: 'int' } },
        sources: {
          y: 'SELECT * FROM x',
          z: 'SELECT a FROM y',
        },
      },
    );
    expect(node.source.sql()).toBe(
      'SELECT z.a AS a FROM (SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y /* source: y */) AS z /* source: z */',
    );
    expect(node.sourceName).toBe('');

    let downstream = node.downstream[0];
    expect(downstream.source.sql()).toBe(
      'SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y /* source: y */',
    );
    expect(downstream.sourceName).toBe('z');

    downstream = downstream.downstream[0];
    expect(downstream.source.sql()).toBe('SELECT x.a AS a FROM x AS x');
    expect(downstream.sourceName).toBe('y');

    const graphHtml = node.toHtml();
    expect(graphHtml.toHtml().length).toBeGreaterThan(1000);

    for (const edge of graphHtml.edges) {
      expect(edge).toHaveProperty('from');
      expect(edge).toHaveProperty('to');
    }

    // test that sql is not modified
    const sql = 'SELECT a FROM x';
    const ast = parseOne(sql);
    lineage('a', ast);
    expect(ast.sql()).toBe(sql);

    // test that sources are not modified
    const ast2 = parseOne(sql);
    const sourceStr = 'SELECT a FROM y';
    const source = parseOne(sourceStr) as SelectExpr;
    lineage('a', ast2, { sources: { x: source } });
    expect(source.sql()).toBe(sourceStr);
  }

  testLineageSqlWithCte () {
    const node = lineage(
      'a',
      'WITH z AS (SELECT a FROM y) SELECT a FROM z',
      {
        schema: { x: { a: 'int' } },
        sources: { y: 'SELECT * FROM x' },
      },
    );
    expect(node.source.sql()).toBe(
      'WITH z AS (SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y /* source: y */) SELECT z.a AS a FROM z AS z',
    );
    expect(node.sourceName).toBe('');
    expect(node.referenceNodeName).toBe('');

    let downstream = node.downstream[0];
    expect(downstream.source.sql()).toBe(
      'SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y /* source: y */',
    );
    expect(downstream.sourceName).toBe('');
    expect(downstream.referenceNodeName).toBe('z');

    downstream = downstream.downstream[0];
    expect(downstream.source.sql()).toBe('SELECT x.a AS a FROM x AS x');
    expect(downstream.sourceName).toBe('y');
    expect(downstream.referenceNodeName).toBe('');
  }

  testLineageSourceWithCte () {
    const node = lineage(
      'a',
      'SELECT a FROM z',
      {
        schema: { x: { a: 'int' } },
        sources: { z: 'WITH y AS (SELECT * FROM x) SELECT a FROM y' },
      },
    );
    expect(node.source.sql()).toBe(
      'SELECT z.a AS a FROM (WITH y AS (SELECT x.a AS a FROM x AS x) SELECT y.a AS a FROM y AS y) AS z /* source: z */',
    );
    expect(node.sourceName).toBe('');
    expect(node.referenceNodeName).toBe('');

    let downstream = node.downstream[0];
    expect(downstream.source.sql()).toBe(
      'WITH y AS (SELECT x.a AS a FROM x AS x) SELECT y.a AS a FROM y AS y',
    );
    expect(downstream.sourceName).toBe('z');
    expect(downstream.referenceNodeName).toBe('');

    downstream = downstream.downstream[0];
    expect(downstream.source.sql()).toBe('SELECT x.a AS a FROM x AS x');
    expect(downstream.sourceName).toBe('z');
    expect(downstream.referenceNodeName).toBe('y');
  }

  testLineageSourceWithStar () {
    const node = lineage(
      'a',
      'WITH y AS (SELECT * FROM x) SELECT a FROM y',
    );
    expect(node.source.sql()).toBe(
      'WITH y AS (SELECT * FROM x AS x) SELECT y.a AS a FROM y AS y',
    );
    expect(node.sourceName).toBe('');
    expect(node.referenceNodeName).toBe('');

    const downstream = node.downstream[0];
    expect(downstream.source.sql()).toBe('SELECT * FROM x AS x');
    expect(downstream.sourceName).toBe('');
    expect(downstream.referenceNodeName).toBe('y');
  }

  testLineageJoinWithStar () {
    const node = lineage(
      '*',
      'SELECT * from x JOIN y USING (uid)',
    );
    expect(node.source.sql()).toBe(
      'SELECT * FROM x AS x JOIN y AS y ON x.uid = y.uid',
    );
    expect(node.sourceName).toBe('');
    expect(node.referenceNodeName).toBe('');
    expect(node.downstream.length).toBe(2);

    const downstream0 = node.downstream[0];
    expect(downstream0.expression.sql()).toBe('x AS x');
    expect(downstream0.name).toBe('*');

    const downstream1 = node.downstream[1];
    expect(downstream1.expression.sql()).toBe('y AS y');
    expect(downstream1.name).toBe('*');
  }

  testLineageJoinWithQualifiedStar () {
    const node = lineage(
      '*',
      'SELECT x.* from x JOIN y USING (uid)',
    );
    expect(node.source.sql()).toBe(
      'SELECT x.* FROM x AS x JOIN y AS y ON x.uid = y.uid',
    );
    expect(node.sourceName).toBe('');
    expect(node.referenceNodeName).toBe('');
    expect(node.downstream.length).toBe(1);

    const downstream = node.downstream[0];
    expect(downstream.expression.sql()).toBe('x AS x');
    expect(downstream.name).toBe('x.*');
  }

  testLineageExternalCol () {
    const node = lineage(
      'a',
      'WITH y AS (SELECT * FROM x) SELECT a FROM y JOIN z USING (uid)',
    );
    expect(node.source.sql()).toBe(
      'WITH y AS (SELECT * FROM x AS x) SELECT a AS a FROM y AS y JOIN z AS z ON y.uid = z.uid',
    );
    expect(node.sourceName).toBe('');
    expect(node.referenceNodeName).toBe('');

    const downstream = node.downstream[0];
    expect(downstream.source.sql()).toBe('?');
    expect(downstream.sourceName).toBe('');
    expect(downstream.referenceNodeName).toBe('');
  }

  testLineageValues () {
    const node = lineage(
      'a',
      'SELECT a FROM y',
      { sources: { y: 'SELECT a FROM (VALUES (1), (2)) AS t (a)' } },
    );
    expect(node.source.sql()).toBe(
      'SELECT y.a AS a FROM (SELECT t.a AS a FROM (VALUES (1), (2)) AS t(a)) AS y /* source: y */',
    );
    expect(node.sourceName).toBe('');

    let downstream = node.downstream[0];
    expect(downstream.source.sql()).toBe('SELECT t.a AS a FROM (VALUES (1), (2)) AS t(a)');
    expect(downstream.expression.sql()).toBe('t.a AS a');
    expect(downstream.sourceName).toBe('y');

    downstream = downstream.downstream[0];
    expect(downstream.source.sql()).toBe('(VALUES (1), (2)) AS t(a)');
    expect(downstream.expression.sql()).toBe('a');
    expect(downstream.sourceName).toBe('y');
  }

  testLineageCteNameAppearsInSchema () {
    const schema = {
      a: {
        b: {
          t1: { c1: 'int' },
          t2: { c2: 'int' },
        },
      },
    };

    const node = lineage(
      'c2',
      'WITH t1 AS (SELECT * FROM a.b.t2), inter AS (SELECT * FROM t1) SELECT * FROM inter',
      { schema },
    );

    expect(node.source.sql()).toBe(
      'WITH t1 AS (SELECT t2.c2 AS c2 FROM a.b.t2 AS t2), inter AS (SELECT t1.c2 AS c2 FROM t1 AS t1) SELECT inter.c2 AS c2 FROM inter AS inter',
    );
    expect(node.sourceName).toBe('');

    let downstream = node.downstream[0];
    expect(downstream.source.sql()).toBe('SELECT t1.c2 AS c2 FROM t1 AS t1');
    expect(downstream.expression.sql()).toBe('t1.c2 AS c2');
    expect(downstream.sourceName).toBe('');

    downstream = downstream.downstream[0];
    expect(downstream.source.sql()).toBe('SELECT t2.c2 AS c2 FROM a.b.t2 AS t2');
    expect(downstream.expression.sql()).toBe('t2.c2 AS c2');
    expect(downstream.sourceName).toBe('');

    downstream = downstream.downstream[0];
    expect(downstream.source.sql()).toBe('a.b.t2 AS t2');
    expect(downstream.expression.sql()).toBe('a.b.t2 AS t2');
    expect(downstream.sourceName).toBe('');

    expect(downstream.downstream).toEqual([]);
  }

  testLineageUnion () {
    let node = lineage(
      'x',
      'SELECT ax AS x FROM a UNION SELECT bx FROM b UNION SELECT cx FROM c',
    );
    expect(node.downstream.length).toBe(3);

    node = lineage(
      'x',
      'SELECT x FROM (SELECT ax AS x FROM a UNION SELECT bx FROM b UNION SELECT cx FROM c)',
    );
    expect(node.downstream.length).toBe(3);
  }

  testLineageLateralFlatten () {
    let node = lineage(
      'VALUE',
      'SELECT FLATTENED.VALUE FROM TEST_TABLE, LATERAL FLATTEN(INPUT => RESULT, OUTER => TRUE) FLATTENED',
      { dialect: 'snowflake' },
    );
    expect(node.name).toBe('VALUE');

    let downstream = node.downstream[0];
    expect(downstream.name).toBe('FLATTENED.VALUE');
    expect(downstream.source.sql({ dialect: 'snowflake' })).toBe(
      'LATERAL FLATTEN(INPUT => TEST_TABLE.RESULT, OUTER => TRUE) AS FLATTENED(SEQ, KEY, PATH, INDEX, VALUE, THIS)',
    );
    expect(downstream.expression.sql({ dialect: 'snowflake' })).toBe('VALUE');
    expect(downstream.downstream.length).toBe(1);

    downstream = downstream.downstream[0];
    expect(downstream.name).toBe('TEST_TABLE.RESULT');
    expect(downstream.source.sql({ dialect: 'snowflake' })).toBe('TEST_TABLE AS TEST_TABLE');

    node = lineage(
      'FIELD',
      'SELECT FLATTENED.VALUE:field::text AS FIELD FROM SNOWFLAKE.SCHEMA.MODEL AS MODEL_ALIAS, LATERAL FLATTEN(INPUT => MODEL_ALIAS.A) AS FLATTENED',
      {
        schema: { SNOWFLAKE: { SCHEMA: { TABLE: { A: 'integer' } } } },
        sources: { 'SNOWFLAKE.SCHEMA.MODEL': 'SELECT A FROM SNOWFLAKE.SCHEMA.TABLE' },
        dialect: 'snowflake',
      },
    );
    expect(node.name).toBe('FIELD');

    let downstream2 = node.downstream[0];
    expect(downstream2.name).toBe('FLATTENED.VALUE');
    expect(downstream2.source.sql({ dialect: 'snowflake' })).toBe(
      'LATERAL FLATTEN(INPUT => MODEL_ALIAS.A) AS FLATTENED(SEQ, KEY, PATH, INDEX, VALUE, THIS)',
    );
    expect(downstream2.expression.sql({ dialect: 'snowflake' })).toBe('VALUE');
    expect(downstream2.downstream.length).toBe(1);

    downstream2 = downstream2.downstream[0];
    expect(downstream2.name).toBe('MODEL_ALIAS.A');
    expect(downstream2.sourceName).toBe('SNOWFLAKE.SCHEMA.MODEL');
    expect(downstream2.source.sql({ dialect: 'snowflake' })).toBe(
      'SELECT TABLE.A AS A FROM SNOWFLAKE.SCHEMA.TABLE AS TABLE',
    );
    expect(downstream2.expression.sql({ dialect: 'snowflake' })).toBe('TABLE.A AS A');
    expect(downstream2.downstream.length).toBe(1);

    downstream2 = downstream2.downstream[0];
    expect(downstream2.name).toBe('TABLE.A');
    expect(downstream2.source.sql({ dialect: 'snowflake' })).toBe(
      'SNOWFLAKE.SCHEMA.TABLE AS TABLE',
    );
    expect(downstream2.expression.sql({ dialect: 'snowflake' })).toBe(
      'SNOWFLAKE.SCHEMA.TABLE AS TABLE',
    );
  }

  testSubquery () {
    let node = lineage(
      'output',
      'SELECT (SELECT max(t3.my_column) my_column FROM foo t3) AS output FROM table3',
    );
    expect(node.name).toBe('output');
    node = node.downstream[0];
    expect(node.name).toBe('my_column');
    node = node.downstream[0];
    expect(node.name).toBe('t3.my_column');
    expect(node.source.sql()).toBe('foo AS t3');

    const node2 = lineage(
      'y',
      'SELECT SUM((SELECT max(a) a from x) + (SELECT min(b) b from x) + c) AS y FROM x',
    );
    expect(node2.name).toBe('y');
    expect(node2.downstream.length).toBe(3);
    expect(node2.downstream[0].name).toBe('a');
    expect(node2.downstream[1].name).toBe('b');
    expect(node2.downstream[2].name).toBe('x.c');

    let node3 = lineage(
      'x',
      'WITH cte AS (SELECT a, b FROM z) SELECT sum(SELECT a FROM cte) AS x, (SELECT b FROM cte) as y FROM cte',
    );
    expect(node3.name).toBe('x');
    expect(node3.downstream.length).toBe(1);
    node3 = node3.downstream[0];
    expect(node3.name).toBe('a');
    node3 = node3.downstream[0];
    expect(node3.name).toBe('cte.a');
    expect(node3.referenceNodeName).toBe('cte');
    node3 = node3.downstream[0];
    expect(node3.name).toBe('z.a');

    const node4 = lineage(
      'a',
      `
      WITH foo AS (
        SELECT
          1 AS a
      ), bar AS (
        (
          SELECT
            a + 1 AS a
          FROM foo
        )
      )
      (
        SELECT
          a + b AS a
        FROM bar
        CROSS JOIN (
          SELECT
            2 AS b
        ) AS baz
      )
      `,
    );
    expect(node4.name).toBe('a');
    expect(node4.downstream.length).toBe(2);
    const sorted = [...node4.downstream].sort((x, y) => x.name.localeCompare(y.name));
    const [a, b] = sorted;
    expect(a.name).toBe('bar.a');
    expect(a.downstream.length).toBe(1);
    expect(b.name).toBe('baz.b');
    expect(b.downstream).toEqual([]);

    const nodeA = a.downstream[0];
    expect(nodeA.name).toBe('foo.a');

    // Select from derived table
    let node5 = lineage(
      'a',
      'SELECT a FROM (SELECT a FROM x) subquery',
    );
    expect(node5.name).toBe('a');
    expect(node5.downstream.length).toBe(1);
    node5 = node5.downstream[0];
    expect(node5.name).toBe('subquery.a');
    expect(node5.referenceNodeName).toBe('subquery');

    let node6 = lineage(
      'a',
      'SELECT a FROM (SELECT a FROM x)',
    );
    expect(node6.name).toBe('a');
    expect(node6.downstream.length).toBe(1);
    node6 = node6.downstream[0];
    expect(node6.name).toBe('_0.a');
    expect(node6.referenceNodeName).toBe('_0');
  }

  testLineageCteUnion () {
    const query = `
    WITH dataset AS (
        SELECT *
        FROM catalog.db.table_a

        UNION

        SELECT *
        FROM catalog.db.table_b
    )

    SELECT x, created_at FROM dataset;
    `;
    const node = lineage('x', query);

    expect(node.name).toBe('x');

    const downstreamA = node.downstream[0];
    expect(downstreamA.name).toBe('0');
    expect(downstreamA.source.sql()).toBe('SELECT * FROM catalog.db.table_a AS table_a');
    expect(downstreamA.referenceNodeName).toBe('dataset');
    const downstreamB = node.downstream[1];
    expect(downstreamB.name).toBe('0');
    expect(downstreamB.source.sql()).toBe('SELECT * FROM catalog.db.table_b AS table_b');
    expect(downstreamB.referenceNodeName).toBe('dataset');
  }

  testLineageSourceUnion () {
    const query = 'SELECT x, created_at FROM dataset;';
    const node = lineage(
      'x',
      query,
      {
        sources: {
          dataset: `
          SELECT *
          FROM catalog.db.table_a

          UNION

          SELECT *
          FROM catalog.db.table_b
          `,
        },
      },
    );

    expect(node.name).toBe('x');

    const downstreamA = node.downstream[0];
    expect(downstreamA.name).toBe('0');
    expect(downstreamA.sourceName).toBe('dataset');
    expect(downstreamA.source.sql()).toBe('SELECT * FROM catalog.db.table_a AS table_a');
    expect(downstreamA.referenceNodeName).toBe('');
    const downstreamB = node.downstream[1];
    expect(downstreamB.name).toBe('0');
    expect(downstreamB.sourceName).toBe('dataset');
    expect(downstreamB.source.sql()).toBe('SELECT * FROM catalog.db.table_b AS table_b');
    expect(downstreamB.referenceNodeName).toBe('');
  }

  testSelectStar () {
    const node = lineage('x', 'SELECT x from (SELECT * from table_a)');

    expect(node.name).toBe('x');

    let downstream = node.downstream[0];
    expect(downstream.name).toBe('_0.x');
    expect(downstream.source.sql()).toBe('SELECT * FROM table_a AS table_a');

    downstream = downstream.downstream[0];
    expect(downstream.name).toBe('*');
    expect(downstream.source.sql()).toBe('table_a AS table_a');
  }

  testUnnest () {
    const node = lineage(
      'b',
      'with _data as (select [struct(1 as a, 2 as b)] as col) select b from _data cross join unnest(col)',
    );
    expect(node.name).toBe('b');
  }

  testLineageNormalize () {
    const node = lineage('a', 'WITH x AS (SELECT 1 a) SELECT a FROM x', { dialect: 'snowflake' });
    expect(node.name).toBe('A');

    expect(() => lineage('"a"', 'WITH x AS (SELECT 1 a) SELECT a FROM x', { dialect: 'snowflake' })).toThrow(SqlglotError);
  }

  testDdlLineage () {
    const sql = `
    INSERT /*+ HINT1 */
    INTO target (x, y)
    SELECT subq.x, subq.y
    FROM (
      SELECT /*+ HINT2 */
        t.x AS x,
        TO_DATE('2023-12-19', 'YYYY-MM-DD') AS y
      FROM s.t t
      WHERE 1 = 1 AND y = TO_DATE('2023-12-19', 'YYYY-MM-DD')
    ) subq
    `;

    const node = lineage('y', sql, { dialect: 'oracle' });

    expect(node.name).toBe('Y');
    expect(node.expression.sql({ dialect: 'oracle' })).toBe('SUBQ.Y AS Y');

    const downstream = node.downstream[0];
    expect(downstream.name).toBe('SUBQ.Y');
    expect(downstream.expression.sql({ dialect: 'oracle' })).toBe(
      'TO_DATE(\'2023-12-19\', \'YYYY-MM-DD\') AS Y',
    );
  }

  testTrim () {
    const sql = `
        SELECT a, b, c
        FROM (select a, b, c from y) z
    `;

    const node = lineage('a', sql, { trimSelects: false });

    expect(node.name).toBe('a');
    expect(node.source.sql()).toBe(
      'SELECT z.a AS a, z.b AS b, z.c AS c FROM (SELECT y.a AS a, y.b AS b, y.c AS c FROM y AS y) AS z',
    );

    const downstream = node.downstream[0];
    expect(downstream.name).toBe('z.a');
    expect(downstream.source.sql()).toBe('SELECT y.a AS a, y.b AS b, y.c AS c FROM y AS y');
  }

  testNodeNameDoesntContainComment () {
    const sql = 'SELECT * FROM (SELECT x /* c */ FROM t1) AS t2';
    const node = lineage('x', sql);

    expect(node.downstream.length).toBe(1);
    expect(node.downstream[0].downstream.length).toBe(1);
    expect(node.downstream[0].downstream[0].name).toBe('t1.x');
  }

  testPivotWithoutAlias () {
    const sql = `
    SELECT
        a as other_a
    FROM (select value,category from sample_data)
    PIVOT (
        sum(value)
        FOR category IN ('a', 'b')
    );
    `;
    const node = lineage('other_a', sql);

    expect(node.downstream[0].name).toBe('_0.value');
    expect(node.downstream[0].downstream[0].name).toBe('sample_data.value');
  }

  testPivotWithAlias () {
    const sql = `
        SELECT
            cat_a_s as other_as
        FROM sample_data
        PIVOT (
            sum(value) as s, max(price)
            FOR category IN ('a' as cat_a, 'b')
        )
    `;
    const node = lineage('other_as', sql);

    expect(node.downstream.length).toBe(1);
    expect(node.downstream[0].name).toBe('sample_data.value');
  }

  testPivotWithCte () {
    const sql = `
    WITH t as (
        SELECT
            a as other_a
        FROM sample_data
        PIVOT (
            sum(value)
            FOR category IN ('a', 'b')
        )
    )
    select other_a from t
    `;
    const node = lineage('other_a', sql);

    expect(node.downstream[0].name).toBe('t.other_a');
    expect(node.downstream[0].referenceNodeName).toBe('t');
    expect(node.downstream[0].downstream[0].name).toBe('sample_data.value');
  }

  testPivotWithImplicitColumnOfPivotedSource () {
    const sql = `
    SELECT empid
    FROM quarterly_sales
        PIVOT(SUM(amount) FOR quarter IN (
        '2023_Q1',
        '2023_Q2',
        '2023_Q3'))
    ORDER BY empid;
    `;
    const node = lineage('empid', sql);

    expect(node.downstream[0].name).toBe('quarterly_sales.empid');
  }

  testPivotWithImplicitColumnOfPivotedSourceAndCte () {
    const sql = `
    WITH t as (
        SELECT empid
        FROM quarterly_sales
        PIVOT(SUM(amount) FOR quarter IN (
            '2023_Q1',
            '2023_Q2',
            '2023_Q3'))
    )
    select empid from t
    `;
    const node = lineage('empid', sql);

    expect(node.downstream[0].name).toBe('t.empid');
    expect(node.downstream[0].referenceNodeName).toBe('t');
    expect(node.downstream[0].downstream[0].name).toBe('quarterly_sales.empid');
  }

  testTableUdtfSnowflake () {
    const lateralFlatten = `
    SELECT f.value:external_id::string AS external_id
    FROM database_name.schema_name.table_name AS raw,
    LATERAL FLATTEN(events) AS f
    `;
    const tableFlatten = `
    SELECT f.value:external_id::string AS external_id
    FROM database_name.schema_name.table_name AS raw
    JOIN TABLE(FLATTEN(events)) AS f
    `;

    let lateralNode = lineage('external_id', lateralFlatten, { dialect: 'snowflake' });
    let tableNode = lineage('external_id', tableFlatten, { dialect: 'snowflake' });

    expect(lateralNode.name).toBe('EXTERNAL_ID');
    expect(tableNode.name).toBe('EXTERNAL_ID');

    lateralNode = lateralNode.downstream[0];
    tableNode = tableNode.downstream[0];

    expect(lateralNode.name).toBe('F.VALUE');
    expect(lateralNode.source.sql({ dialect: 'snowflake' })).toBe(
      'LATERAL FLATTEN(RAW.EVENTS) AS F(SEQ, KEY, PATH, INDEX, VALUE, THIS)',
    );

    expect(tableNode.name).toBe('F.VALUE');
    expect(tableNode.source.sql({ dialect: 'snowflake' })).toBe('TABLE(FLATTEN(RAW.EVENTS)) AS F');

    lateralNode = lateralNode.downstream[0];
    tableNode = tableNode.downstream[0];

    expect(lateralNode.name).toBe('RAW.EVENTS');
    expect(lateralNode.source.sql({ dialect: 'snowflake' })).toBe(
      'DATABASE_NAME.SCHEMA_NAME.TABLE_NAME AS RAW',
    );

    expect(tableNode.name).toBe('RAW.EVENTS');
    expect(tableNode.source.sql({ dialect: 'snowflake' })).toBe(
      'DATABASE_NAME.SCHEMA_NAME.TABLE_NAME AS RAW',
    );
  }

  testPivotWithSubquery () {
    const schema = {
      loan_ledger: {
        product_type: 'varchar',
        month: 'date',
        loan_id: 'int',
      },
    };

    const sql = `
    WITH cte AS (
        SELECT * FROM (
            SELECT product_type, month, loan_id
            FROM loan_ledger
        ) PIVOT (
            COUNT(loan_id) FOR month IN ('2024-10', '2024-11')
        )
    )
    SELECT
        cte.product_type AS product_type,
        cte."2024-10" AS "2024-10"
    FROM cte
    `;

    let node = lineage('product_type', sql, {
      dialect: 'duckdb',
      schema,
    });
    expect(node.downstream[0].name).toBe('cte.product_type');
    expect(node.downstream[0].downstream[0].name).toBe('_0.product_type');
    expect(node.downstream[0].downstream[0].downstream[0].name).toBe('loan_ledger.product_type');

    node = lineage('"2024-10"', sql, {
      dialect: 'duckdb',
      schema,
    });
    expect(node.downstream[0].name).toBe('cte.2024-10');
    expect(node.downstream[0].downstream[0].name).toBe('_0.loan_id');
    expect(node.downstream[0].downstream[0].downstream[0].name).toBe('loan_ledger.loan_id');
  }

  testCopyFlag () {
    const schema = { x: { a: 'int' } };

    let query = parseOne('SELECT a FROM z');
    let sources = {
      y: parseOne('SELECT * FROM x') as SelectExpr,
      z: parseOne('SELECT * FROM y') as SelectExpr,
    };

    lineage('a', query, {
      schema,
      sources,
      copy: false,
    });

    expect(sources['y'].sql()).toBe('SELECT * FROM x');
    expect(sources['z'].sql()).toBe('SELECT * FROM y');
    expect(query.sql()).toBe(
      'SELECT z.a AS a FROM (SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y /* source: y */) AS z /* source: z */',
    );

    query = parseOne('SELECT a FROM z');
    sources = {
      y: parseOne('SELECT * FROM x') as SelectExpr,
      z: parseOne('SELECT * FROM y') as SelectExpr,
    };

    lineage('a', query, {
      schema,
      sources,
      copy: true,
    });

    expect(sources['y'].sql()).toBe('SELECT * FROM x');
    expect(sources['z'].sql()).toBe('SELECT * FROM y');
    expect(query.sql()).toBe('SELECT a FROM z');

    query = parseOne('SELECT a FROM x');
    lineage('a', query, {
      schema,
      copy: false,
    });
    expect(query.sql()).toBe('SELECT x.a AS a FROM x AS x');

    query = parseOne('SELECT a FROM x');
    lineage('a', query, {
      schema,
      copy: true,
    });
    expect(query.sql()).toBe('SELECT a FROM x');
  }
}

const t = new TestLineage();
describe('TestLineage', () => {
  test('lineage', () => t.testLineage());
  test('testLineageSqlWithCte', () => t.testLineageSqlWithCte());
  test('testLineageSourceWithCte', () => t.testLineageSourceWithCte());
  test('testLineageSourceWithStar', () => t.testLineageSourceWithStar());
  test('testLineageJoinWithStar', () => t.testLineageJoinWithStar());
  test('testLineageJoinWithQualifiedStar', () => t.testLineageJoinWithQualifiedStar());
  test('testLineageExternalCol', () => t.testLineageExternalCol());
  test('testLineageValues', () => t.testLineageValues());
  test('testLineageCteNameAppearsInSchema', () => t.testLineageCteNameAppearsInSchema());
  test('testLineageUnion', () => t.testLineageUnion());
  test('testLineageLateralFlatten', () => t.testLineageLateralFlatten());
  test('subquery', () => t.testSubquery());
  test('testLineageCteUnion', () => t.testLineageCteUnion());
  test('testLineageSourceUnion', () => t.testLineageSourceUnion());
  test('testSelectStar', () => t.testSelectStar());
  test('unnest', () => t.testUnnest());
  test('testLineageNormalize', () => t.testLineageNormalize());
  test('testDdlLineage', () => t.testDdlLineage());
  test('trim', () => t.testTrim());
  test('testNodeNameDoesntContainComment', () => t.testNodeNameDoesntContainComment());
  test('testPivotWithoutAlias', () => t.testPivotWithoutAlias());
  test('testPivotWithAlias', () => t.testPivotWithAlias());
  test('testPivotWithCte', () => t.testPivotWithCte());
  test('testPivotWithImplicitColumnOfPivotedSource', () => t.testPivotWithImplicitColumnOfPivotedSource());
  test('testPivotWithImplicitColumnOfPivotedSourceAndCte', () => t.testPivotWithImplicitColumnOfPivotedSourceAndCte());
  test('testTableUdtfSnowflake', () => t.testTableUdtfSnowflake());
  test('testPivotWithSubquery', () => t.testPivotWithSubquery());
  test('testCopyFlag', () => t.testCopyFlag());
});
