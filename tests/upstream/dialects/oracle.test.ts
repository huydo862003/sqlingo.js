import {
  describe, test, expect,
} from 'vitest';
import {
  parseOne, ParseError,
} from '../../../src/index';
import {
  AlterExpr, AnonymousExpr, DateTruncExpr, RandExpr, SystimestampExpr, TruncExpr,
  UtcTimeExpr, UtcTimestampExpr,
} from '../../../src/expressions';
import {
  Validator, UnsupportedError,
} from './validator';

class TestOracle extends Validator {
  override dialect = 'oracle' as const;

  testOracle () {
    this.validateIdentity('1 /* /* */');
    this.validateAll(
      'SELECT CONNECT_BY_ROOT x y',
      {
        write: {
          '': 'SELECT CONNECT_BY_ROOT x AS y',
          'oracle': 'SELECT CONNECT_BY_ROOT x AS y',
        },
      },
    );
    this.parseOne('ALTER TABLE tbl_name DROP FOREIGN KEY fk_symbol').assertIs(AlterExpr);

    this.validateIdentity('XMLELEMENT(EVALNAME foo + bar)');
    this.validateIdentity('SELECT BITMAP_BUCKET_NUMBER(32769)');
    this.validateIdentity('SELECT BITMAP_CONSTRUCT_AGG(value)');
    this.validateIdentity('DBMS_RANDOM.NORMAL');
    this.validateIdentity('DBMS_RANDOM.VALUE(low, high)').assertIs(RandExpr);
    this.validateIdentity('DBMS_RANDOM.VALUE()').assertIs(RandExpr);
    this.validateIdentity('CAST(value AS NUMBER DEFAULT 0 ON CONVERSION ERROR)');
    this.validateIdentity('SYSDATE');
    this.validateIdentity('CREATE GLOBAL TEMPORARY TABLE t AS SELECT * FROM orders');
    this.validateIdentity('CREATE PRIVATE TEMPORARY TABLE t AS SELECT * FROM orders');
    this.validateIdentity('REGEXP_REPLACE(\'source\', \'search\')');
    this.validateIdentity('TIMESTAMP(3) WITH TIME ZONE');
    this.validateIdentity('SYSTIMESTAMP').assertIs(SystimestampExpr);
    this.validateIdentity('SELECT SYSTIMESTAMP AT TIME ZONE \'UTC\'');
    this.validateIdentity('CURRENT_TIMESTAMP(precision)');
    this.validateIdentity('ALTER TABLE tbl_name DROP FOREIGN KEY fk_symbol');
    this.validateIdentity('ALTER TABLE Payments ADD Stock NUMBER NOT NULL');
    this.validateIdentity('SELECT x FROM t WHERE cond FOR UPDATE');
    this.validateIdentity('SELECT JSON_OBJECT(k1: v1 FORMAT JSON, k2: v2 FORMAT JSON)');
    this.validateIdentity('SELECT JSON_OBJECT(\'name\': first_name || \' \' || last_name) FROM t');
    this.validateIdentity('COALESCE(c1, c2, c3)');
    this.validateIdentity('SELECT * FROM TABLE(foo)');
    this.validateIdentity('SELECT a$x#b');
    this.validateIdentity('SELECT :OBJECT');
    this.validateIdentity('SELECT * FROM t FOR UPDATE');
    this.validateIdentity('SELECT * FROM t FOR UPDATE WAIT 5');
    this.validateIdentity('SELECT * FROM t FOR UPDATE NOWAIT');
    this.validateIdentity('SELECT * FROM t FOR UPDATE SKIP LOCKED');
    this.validateIdentity('SELECT * FROM t FOR UPDATE OF s.t.c, s.t.v');
    this.validateIdentity('SELECT * FROM t FOR UPDATE OF s.t.c, s.t.v NOWAIT');
    this.validateIdentity('SELECT * FROM t FOR UPDATE OF s.t.c, s.t.v SKIP LOCKED');
    this.validateIdentity('SELECT STANDARD_HASH(\'hello\')');
    this.validateIdentity('SELECT STANDARD_HASH(\'hello\', \'MD5\')');
    this.validateIdentity('SELECT * FROM table_name@dblink_name.database_link_domain');
    this.validateIdentity('SELECT * FROM table_name SAMPLE (25) s');
    this.validateIdentity('SELECT COUNT(*) * 10 FROM orders SAMPLE (10) SEED (1)');
    this.validateIdentity('SELECT * FROM V$SESSION');
    this.validateIdentity('SELECT TO_DATE(\'January 15, 1989, 11:00 A.M.\')');
    this.validateIdentity('SELECT INSTR(haystack, needle)');
    this.validateIdentity(
      'SELECT (TIMESTAMP \'2025-12-30 20:00:00\' - TIMESTAMP \'2025-12-29 14:30:00\') DAY TO SECOND',
      'SELECT (TO_TIMESTAMP(\'2025-12-30 20:00:00\', \'YYYY-MM-DD HH24:MI:SS.FF6\') - TO_TIMESTAMP(\'2025-12-29 14:30:00\', \'YYYY-MM-DD HH24:MI:SS.FF6\')) DAY TO SECOND',
    );
    this.validateIdentity('SELECT (SYSTIMESTAMP - order_date) DAY(9) TO SECOND FROM orders');
    this.validateIdentity('SELECT (SYSTIMESTAMP - order_date) DAY(9) TO SECOND(3) FROM orders');
    this.validateIdentity(
      'SELECT * FROM consumer LEFT JOIN groceries ON consumer.groceries_id = consumer.id PIVOT(MAX(type_id) FOR consumer_type IN (1, 2, 3, 4))',
    );
    this.validateIdentity(
      'SELECT * FROM test UNPIVOT INCLUDE NULLS (value FOR Description IN (col AS \'PREFIX \' || CHR(38) || \' SUFFIX\'))',
    );
    this.validateIdentity(
      'SELECT last_name, employee_id, manager_id, LEVEL FROM employees START WITH employee_id = 100 CONNECT BY PRIOR employee_id = manager_id ORDER SIBLINGS BY last_name',
    );
    this.validateIdentity(
      'ALTER TABLE Payments ADD (Stock NUMBER NOT NULL, dropid VARCHAR2(500) NOT NULL)',
    );
    this.validateIdentity(
      'SELECT JSON_ARRAYAGG(JSON_OBJECT(\'RNK\': RNK, \'RATING_CODE\': RATING_CODE, \'DATE_VALUE\': DATE_VALUE, \'AGENT_ID\': AGENT_ID RETURNING CLOB) RETURNING CLOB) AS JSON_DATA FROM tablename',
    );
    this.validateIdentity(
      'SELECT JSON_ARRAY(FOO() FORMAT JSON, BAR() NULL ON NULL RETURNING CLOB STRICT)',
    );
    this.validateIdentity(
      'SELECT JSON_ARRAYAGG(FOO() FORMAT JSON ORDER BY bar NULL ON NULL RETURNING CLOB STRICT)',
    );
    this.validateIdentity(
      'SELECT COUNT(1) INTO V_Temp FROM TABLE(CAST(somelist AS data_list)) WHERE col LIKE \'%contact\'',
    );
    this.validateIdentity(
      'SELECT department_id INTO v_department_id FROM departments FETCH FIRST 1 ROWS ONLY',
    );
    this.validateIdentity(
      'SELECT department_id BULK COLLECT INTO v_department_ids FROM departments',
    );
    this.validateIdentity(
      'SELECT department_id, department_name BULK COLLECT INTO v_department_ids, v_department_names FROM departments',
    );
    this.validateIdentity(
      'SELECT MIN(column_name) KEEP (DENSE_RANK FIRST ORDER BY column_name DESC) FROM table_name',
    );
    this.validateIdentity(
      'XMLELEMENT("ImageID", image.id)',
      'XMLELEMENT(NAME "ImageID", image.id)',
    );
    this.validateIdentity(
      'SELECT CAST(\'January 15, 1989, 11:00 A.M.\' AS DATE DEFAULT NULL ON CONVERSION ERROR, \'Month dd, YYYY, HH:MI A.M.\') FROM DUAL',
      'SELECT TO_DATE(\'January 15, 1989, 11:00 A.M.\', \'Month dd, YYYY, HH12:MI A.M.\') FROM DUAL',
    );
    this.validateIdentity(
      'SELECT TRUNC(SYSDATE)',
      'SELECT TRUNC(SYSDATE, \'DD\')',
    );
    this.validateIdentity(
      'SELECT JSON_OBJECT(KEY \'key1\' IS emp.column1, KEY \'key2\' IS emp.column1) "emp_key" FROM emp',
      'SELECT JSON_OBJECT(\'key1\': emp.column1, \'key2\': emp.column1) AS "emp_key" FROM emp',
    );
    this.validateIdentity(
      'SELECT JSON_OBJECTAGG(KEY department_name VALUE department_id) FROM dep WHERE id <= 30',
      'SELECT JSON_OBJECTAGG(department_name: department_id) FROM dep WHERE id <= 30',
    );
    this.validateIdentity(
      'SELECT last_name, department_id, salary, MIN(salary) KEEP (DENSE_RANK FIRST ORDER BY commission_pct) '
      + 'OVER (PARTITION BY department_id) AS "Worst", MAX(salary) KEEP (DENSE_RANK LAST ORDER BY commission_pct) '
      + 'OVER (PARTITION BY department_id) AS "Best" FROM employees ORDER BY department_id, salary, last_name',
    );
    this.validateIdentity(
      'SELECT UNIQUE col1, col2 FROM table',
      'SELECT DISTINCT col1, col2 FROM table',
    );
    this.validateIdentity(
      'SELECT * FROM T ORDER BY I OFFSET NVL(:variable1, 10) ROWS FETCH NEXT NVL(:variable2, 10) ROWS ONLY',
    );
    this.validateIdentity(
      'SELECT * FROM t SAMPLE (.25)',
      'SELECT * FROM t SAMPLE (0.25)',
    );
    this.validateIdentity('SELECT TO_CHAR(-100, \'L99\', \'NL_CURRENCY = \'\' AusDollars \'\' \')');
    this.validateIdentity(
      'SELECT * FROM t START WITH col CONNECT BY NOCYCLE PRIOR col1 = col2',
    );

    this.validateAll(
      'SELECT DBMS_RANDOM.VALUE()',
      {
        read: {
          oracle: 'SELECT DBMS_RANDOM.VALUE',
          postgres: 'SELECT RANDOM()',
        },
        write: {
          oracle: 'SELECT DBMS_RANDOM.VALUE()',
          postgres: 'SELECT RANDOM()',
        },
      },
    );
    this.validateAll(
      'SELECT TRIM(\'|\' FROM \'||Hello ||| world||\')',
      {
        write: {
          clickhouse: 'SELECT TRIM(BOTH \'|\' FROM \'||Hello ||| world||\')',
          oracle: 'SELECT TRIM(\'|\' FROM \'||Hello ||| world||\')',
        },
      },
    );
    this.validateAll(
      'SELECT department_id, department_name INTO v_department_id, v_department_name FROM departments FETCH FIRST 1 ROWS ONLY',
      {
        write: {
          oracle: 'SELECT department_id, department_name INTO v_department_id, v_department_name FROM departments FETCH FIRST 1 ROWS ONLY',
          postgres: UnsupportedError,
          tsql: UnsupportedError,
        },
      },
    );
    this.validateAll(
      'SELECT * FROM test WHERE MOD(col1, 4) = 3',
      {
        read: {
          duckdb: 'SELECT * FROM test WHERE col1 % 4 = 3',
        },
        write: {
          duckdb: 'SELECT * FROM test WHERE col1 % 4 = 3',
          oracle: 'SELECT * FROM test WHERE MOD(col1, 4) = 3',
        },
      },
    );
    this.validateAll(
      'CURRENT_TIMESTAMP BETWEEN TO_DATE(f.C_SDATE, \'YYYY/MM/DD\') AND TO_DATE(f.C_EDATE, \'YYYY/MM/DD\')',
      {
        read: {
          postgres: 'CURRENT_TIMESTAMP BETWEEN TO_DATE(f.C_SDATE, \'yyyy/mm/dd\') AND TO_DATE(f.C_EDATE, \'yyyy/mm/dd\')',
        },
        write: {
          oracle: 'CURRENT_TIMESTAMP BETWEEN TO_DATE(f.C_SDATE, \'YYYY/MM/DD\') AND TO_DATE(f.C_EDATE, \'YYYY/MM/DD\')',
          postgres: 'CURRENT_TIMESTAMP BETWEEN TO_DATE(f.C_SDATE, \'YYYY/MM/DD\') AND TO_DATE(f.C_EDATE, \'YYYY/MM/DD\')',
        },
      },
    );
    this.validateAll(
      'TO_CHAR(x)',
      {
        write: {
          doris: 'CAST(x AS STRING)',
          oracle: 'TO_CHAR(x)',
        },
      },
    );
    this.validateAll(
      'TO_NUMBER(expr, fmt, nlsparam)',
      {
        read: {
          teradata: 'TO_NUMBER(expr, fmt, nlsparam)',
        },
        write: {
          oracle: 'TO_NUMBER(expr, fmt, nlsparam)',
          teradata: 'TO_NUMBER(expr, fmt, nlsparam)',
        },
      },
    );
    this.validateAll(
      'TO_NUMBER(x)',
      {
        write: {
          bigquery: 'CAST(x AS FLOAT64)',
          doris: 'CAST(x AS DOUBLE)',
          drill: 'CAST(x AS DOUBLE)',
          duckdb: 'CAST(x AS DOUBLE)',
          hive: 'CAST(x AS DOUBLE)',
          mysql: 'CAST(x AS DOUBLE)',
          oracle: 'TO_NUMBER(x)',
          postgres: 'CAST(x AS DOUBLE PRECISION)',
          presto: 'CAST(x AS DOUBLE)',
          redshift: 'CAST(x AS DOUBLE PRECISION)',
          snowflake: 'TO_NUMBER(x)',
          spark: 'CAST(x AS DOUBLE)',
          spark2: 'CAST(x AS DOUBLE)',
          starrocks: 'CAST(x AS DOUBLE)',
          tableau: 'CAST(x AS DOUBLE)',
          teradata: 'TO_NUMBER(x)',
        },
      },
    );
    this.validateAll(
      'TO_NUMBER(x, fmt)',
      {
        read: {
          databricks: 'TO_NUMBER(x, fmt)',
          drill: 'TO_NUMBER(x, fmt)',
          postgres: 'TO_NUMBER(x, fmt)',
          snowflake: 'TO_NUMBER(x, fmt)',
          spark: 'TO_NUMBER(x, fmt)',
          redshift: 'TO_NUMBER(x, fmt)',
          teradata: 'TO_NUMBER(x, fmt)',
        },
        write: {
          databricks: 'TO_NUMBER(x, fmt)',
          drill: 'TO_NUMBER(x, fmt)',
          oracle: 'TO_NUMBER(x, fmt)',
          postgres: 'TO_NUMBER(x, fmt)',
          snowflake: 'TO_NUMBER(x, fmt)',
          spark: 'TO_NUMBER(x, fmt)',
          redshift: 'TO_NUMBER(x, fmt)',
          teradata: 'TO_NUMBER(x, fmt)',
        },
      },
    );
    this.validateAll(
      'SELECT CAST(NULL AS VARCHAR2(2328 CHAR)) AS COL1',
      {
        write: {
          oracle: 'SELECT CAST(NULL AS VARCHAR2(2328 CHAR)) AS COL1',
          spark: 'SELECT CAST(NULL AS VARCHAR(2328)) AS COL1',
        },
      },
    );
    this.validateAll(
      'SELECT CAST(NULL AS VARCHAR2(2328 BYTE)) AS COL1',
      {
        write: {
          oracle: 'SELECT CAST(NULL AS VARCHAR2(2328 BYTE)) AS COL1',
          spark: 'SELECT CAST(NULL AS VARCHAR(2328)) AS COL1',
        },
      },
    );
    this.validateAll(
      'DATE \'2022-01-01\'',
      {
        write: {
          '': 'DATE_STR_TO_DATE(\'2022-01-01\')',
          'mysql': 'CAST(\'2022-01-01\' AS DATE)',
          'oracle': 'TO_DATE(\'2022-01-01\', \'YYYY-MM-DD\')',
          'postgres': 'CAST(\'2022-01-01\' AS DATE)',
        },
      },
    );
    this.validateAll(
      'x::binary_double',
      {
        write: {
          'oracle': 'CAST(x AS DOUBLE PRECISION)',
          '': 'CAST(x AS DOUBLE)',
        },
      },
    );
    this.validateAll(
      'x::binary_float',
      {
        write: {
          'oracle': 'CAST(x AS FLOAT)',
          '': 'CAST(x AS FLOAT)',
        },
      },
    );
    this.validateAll(
      'CAST(x AS sch.udt)',
      {
        read: {
          postgres: 'CAST(x AS sch.udt)',
        },
        write: {
          oracle: 'CAST(x AS sch.udt)',
          postgres: 'CAST(x AS sch.udt)',
        },
      },
    );
    this.validateAll(
      'SELECT TO_TIMESTAMP(\'2024-12-12 12:12:12.000000\', \'YYYY-MM-DD HH24:MI:SS.FF6\')',
      {
        write: {
          oracle: 'SELECT TO_TIMESTAMP(\'2024-12-12 12:12:12.000000\', \'YYYY-MM-DD HH24:MI:SS.FF6\')',
          duckdb: 'SELECT STRPTIME(\'2024-12-12 12:12:12.000000\', \'%Y-%m-%d %H:%M:%S.%f\')',
        },
      },
    );
    this.validateAll(
      'SELECT TO_DATE(\'2024-12-12\', \'YYYY-MM-DD\')',
      {
        write: {
          oracle: 'SELECT TO_DATE(\'2024-12-12\', \'YYYY-MM-DD\')',
          duckdb: 'SELECT CAST(STRPTIME(\'2024-12-12\', \'%Y-%m-%d\') AS DATE)',
        },
      },
    );
    this.validateIdentity(
      'SELECT * FROM t ORDER BY a ASC NULLS LAST, b ASC NULLS FIRST, c DESC NULLS LAST, d DESC NULLS FIRST',
      'SELECT * FROM t ORDER BY a ASC, b ASC NULLS FIRST, c DESC NULLS LAST, d DESC',
    );
    this.validateAll(
      'NVL(NULL, 1)',
      {
        write: {
          'oracle': 'NVL(NULL, 1)',
          '': 'COALESCE(NULL, 1)',
          'clickhouse': 'COALESCE(NULL, 1)',
        },
      },
    );
    this.validateAll(
      'TRIM(BOTH \'h\' FROM \'Hello World\')',
      {
        write: {
          oracle: 'TRIM(BOTH \'h\' FROM \'Hello World\')',
          clickhouse: 'TRIM(BOTH \'h\' FROM \'Hello World\')',
        },
      },
    );
    this.validateIdentity('SELECT /*+ ORDERED */* FROM tbl', 'SELECT /*+ ORDERED */ * FROM tbl');
    this.validateIdentity(
      'SELECT /* test */ /*+ ORDERED */* FROM tbl',
      '/* test */ SELECT /*+ ORDERED */ * FROM tbl',
    );
    this.validateIdentity(
      'SELECT /*+ ORDERED */*/* test */ FROM tbl',
      'SELECT /*+ ORDERED */ * /* test */ FROM tbl',
    );
    this.validateAll(
      'SELECT * FROM t FETCH FIRST 10 ROWS ONLY',
      {
        write: {
          oracle: 'SELECT * FROM t FETCH FIRST 10 ROWS ONLY',
          tsql: 'SELECT * FROM t ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH FIRST 10 ROWS ONLY',
        },
      },
    );
    this.validateIdentity('CREATE OR REPLACE FORCE VIEW foo1.foo2');
    this.validateIdentity('TO_TIMESTAMP(\'foo\')');
    this.validateIdentity('SELECT TO_TIMESTAMP(\'05 Dec 2000 10:00 AM\', \'DD Mon YYYY HH12:MI AM\')');
    this.validateIdentity('SELECT TO_TIMESTAMP(\'05 Dec 2000 10:00 PM\', \'DD Mon YYYY HH12:MI PM\')');
    this.validateIdentity('SELECT TO_TIMESTAMP(\'05 Dec 2000 10:00 A.M.\', \'DD Mon YYYY HH12:MI A.M.\')');
    this.validateIdentity('SELECT TO_TIMESTAMP(\'05 Dec 2000 10:00 P.M.\', \'DD Mon YYYY HH12:MI P.M.\')');
    this.validateIdentity(
      'SELECT CUME_DIST(15, 0.05) WITHIN GROUP (ORDER BY col1, col2) FROM t',
    );
    this.validateIdentity(
      'SELECT DENSE_RANK(15, 0.05) WITHIN GROUP (ORDER BY col1, col2) FROM t',
    );
    this.validateIdentity('SELECT RANK(15, 0.05) WITHIN GROUP (ORDER BY col1, col2) FROM t');
    this.validateIdentity(
      'SELECT PERCENT_RANK(15, 0.05) WITHIN GROUP (ORDER BY col1, col2) FROM t',
    );
    this.validateIdentity('L2_DISTANCE(x, y)');
    this.validateIdentity('BITMAP_OR_AGG(x)');
  }

  testJoinMarker () {
    this.validateIdentity('SELECT e1.x, e2.x FROM e e1, e e2 WHERE e1.y (+) = e2.y');

    this.validateAll(
      'SELECT e1.x, e2.x FROM e e1, e e2 WHERE e1.y = e2.y (+)',
      { write: { '': UnsupportedError } },
    );
    this.validateAll(
      'SELECT e1.x, e2.x FROM e e1, e e2 WHERE e1.y = e2.y (+)',
      {
        write: {
          '': 'SELECT e1.x, e2.x FROM e AS e1, e AS e2 WHERE e1.y = e2.y',
          'oracle': 'SELECT e1.x, e2.x FROM e e1, e e2 WHERE e1.y = e2.y (+)',
        },
      },
    );
  }

  testHints () {
    this.validateIdentity('SELECT /*+ USE_NL(A B) */ A.COL_TEST FROM TABLE_A A, TABLE_B B');
    this.validateIdentity(
      'SELECT /*+ INDEX(v.j jhist_employee_ix (employee_id start_date)) */ * FROM v',
    );
    this.validateIdentity(
      'SELECT /*+ USE_NL(A B C) */ A.COL_TEST FROM TABLE_A A, TABLE_B B, TABLE_C C',
    );
    this.validateIdentity(
      'SELECT /*+ NO_INDEX(employees emp_empid) */ employee_id FROM employees WHERE employee_id > 200',
    );
    this.validateIdentity(
      'SELECT /*+ NO_INDEX_FFS(items item_order_ix) */ order_id FROM order_items items',
    );
    this.validateIdentity(
      'SELECT /*+ LEADING(e j) */ * FROM employees e, departments d, job_history j WHERE e.department_id = d.department_id AND e.hire_date = j.start_date',
    );
    this.validateIdentity('INSERT /*+ APPEND */ INTO IAP_TBL (id, col1) VALUES (2, \'test2\')');
    this.validateIdentity('INSERT /*+ APPEND_VALUES */ INTO dest_table VALUES (i, \'Value\')');
    this.validateIdentity('INSERT /*+ APPEND(d) */ INTO dest d VALUES (i, \'Value\')');
    this.validateIdentity(
      'INSERT /*+ APPEND(d) */ INTO dest d (i, value) SELECT 1, \'value\' FROM dual',
    );
    this.validateIdentity(
      'SELECT /*+ LEADING(departments employees) USE_NL(employees) */ * FROM employees JOIN departments ON employees.department_id = departments.department_id',
      `SELECT /*+ LEADING(departments employees)
  USE_NL(employees) */
  *
FROM employees
JOIN departments
  ON employees.department_id = departments.department_id`,
      { pretty: true },
    );
    this.validateIdentity(
      'SELECT /*+ USE_NL(bbbbbbbbbbbbbbbbbbbbbbbb) LEADING(aaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbb cccccccccccccccccccccccc dddddddddddddddddddddddd) INDEX(cccccccccccccccccccccccc) */ * FROM aaaaaaaaaaaaaaaaaaaaaaaa JOIN bbbbbbbbbbbbbbbbbbbbbbbb ON aaaaaaaaaaaaaaaaaaaaaaaa.id = bbbbbbbbbbbbbbbbbbbbbbbb.a_id JOIN cccccccccccccccccccccccc ON bbbbbbbbbbbbbbbbbbbbbbbb.id = cccccccccccccccccccccccc.b_id JOIN dddddddddddddddddddddddd ON cccccccccccccccccccccccc.id = dddddddddddddddddddddddd.c_id',
    );
    this.validateIdentity(
      'SELECT /*+ USE_NL(bbbbbbbbbbbbbbbbbbbbbbbb) LEADING(aaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbb cccccccccccccccccccccccc dddddddddddddddddddddddd) INDEX(cccccccccccccccccccccccc) */ * FROM aaaaaaaaaaaaaaaaaaaaaaaa JOIN bbbbbbbbbbbbbbbbbbbbbbbb ON aaaaaaaaaaaaaaaaaaaaaaaa.id = bbbbbbbbbbbbbbbbbbbbbbbb.a_id JOIN cccccccccccccccccccccccc ON bbbbbbbbbbbbbbbbbbbbbbbb.id = cccccccccccccccccccccccc.b_id JOIN dddddddddddddddddddddddd ON cccccccccccccccccccccccc.id = dddddddddddddddddddddddd.c_id',
      `SELECT /*+ USE_NL(bbbbbbbbbbbbbbbbbbbbbbbb)
  LEADING(
    aaaaaaaaaaaaaaaaaaaaaaaa
    bbbbbbbbbbbbbbbbbbbbbbbb
    cccccccccccccccccccccccc
    dddddddddddddddddddddddd
  )
  INDEX(cccccccccccccccccccccccc) */
  *
FROM aaaaaaaaaaaaaaaaaaaaaaaa
JOIN bbbbbbbbbbbbbbbbbbbbbbbb
  ON aaaaaaaaaaaaaaaaaaaaaaaa.id = bbbbbbbbbbbbbbbbbbbbbbbb.a_id
JOIN cccccccccccccccccccccccc
  ON bbbbbbbbbbbbbbbbbbbbbbbb.id = cccccccccccccccccccccccc.b_id
JOIN dddddddddddddddddddddddd
  ON cccccccccccccccccccccccc.id = dddddddddddddddddddddddd.c_id`,
      { pretty: true },
    );
    // Test that parsing error with keywords like select where etc falls back
    this.validateIdentity(
      'SELECT /*+ LEADING(departments employees) USE_NL(employees) select where group by is order by */ * FROM employees JOIN departments ON employees.department_id = departments.department_id',
      `SELECT /*+ LEADING(departments employees) USE_NL(employees) select where group by is order by */
  *
FROM employees
JOIN departments
  ON employees.department_id = departments.department_id`,
      { pretty: true },
    );
    // Test that parsing error with , inside hint function falls back
    this.validateIdentity(
      'SELECT /*+ LEADING(departments, employees) */ * FROM employees JOIN departments ON employees.department_id = departments.department_id',
    );
    // Test that parsing error with keyword inside hint function falls back
    this.validateIdentity(
      'SELECT /*+ LEADING(departments select) */ * FROM employees JOIN departments ON employees.department_id = departments.department_id',
    );
  }

  testXmlTable () {
    this.validateIdentity('XMLTABLE(\'x\')');
    this.validateIdentity('XMLTABLE(\'x\' RETURNING SEQUENCE BY REF)');
    this.validateIdentity('XMLTABLE(\'x\' PASSING y)');
    this.validateIdentity('XMLTABLE(\'x\' PASSING y RETURNING SEQUENCE BY REF)');
    this.validateIdentity(
      'XMLTABLE(\'x\' RETURNING SEQUENCE BY REF COLUMNS a VARCHAR2, b FLOAT)',
    );
    this.validateIdentity(
      'SELECT x.* FROM example t, XMLTABLE(XMLNAMESPACES(DEFAULT \'http://example.com/default\', \'http://example.com/ns1\' AS "ns1"), \'/root/data\' PASSING t.xml COLUMNS id NUMBER PATH \'@id\', value VARCHAR2(100) PATH \'ns1:value/text()\') x',
    );

    this.validateAll(
      `SELECT warehouse_name warehouse,
   warehouse2."Water", warehouse2."Rail"
   FROM warehouses,
   XMLTABLE('/Warehouse'
      PASSING warehouses.warehouse_spec
      COLUMNS
         "Water" varchar2(6) PATH 'WaterAccess',
         "Rail" varchar2(6) PATH 'RailAccess')
      warehouse2`,
      {
        write: {
          oracle: `SELECT
  warehouse_name AS warehouse,
  warehouse2."Water",
  warehouse2."Rail"
FROM warehouses, XMLTABLE(
  '/Warehouse'
  PASSING
    warehouses.warehouse_spec
  COLUMNS
    "Water" VARCHAR2(6) PATH 'WaterAccess',
    "Rail" VARCHAR2(6) PATH 'RailAccess'
) warehouse2`,
        },
        pretty: true,
      },
    );

    this.validateAll(
      `SELECT table_name, column_name, data_default FROM xmltable('ROWSET/ROW'
    passing dbms_xmlgen.getxmltype('SELECT table_name, column_name, data_default FROM user_tab_columns')
    columns table_name      VARCHAR2(128)   PATH '*[1]'
            , column_name   VARCHAR2(128)   PATH '*[2]'
            , data_default  VARCHAR2(2000)  PATH '*[3]'
            );`,
      {
        write: {
          oracle: `SELECT
  table_name,
  column_name,
  data_default
FROM XMLTABLE(
  'ROWSET/ROW'
  PASSING
    dbms_xmlgen.getxmltype('SELECT table_name, column_name, data_default FROM user_tab_columns')
  COLUMNS
    table_name VARCHAR2(128) PATH '*[1]',
    column_name VARCHAR2(128) PATH '*[2]',
    data_default VARCHAR2(2000) PATH '*[3]'
)`,
        },
        pretty: true,
      },
    );
  }

  testMatchRecognize () {
    this.validateIdentity(
      `SELECT
  *
FROM sales_history
MATCH_RECOGNIZE (
  PARTITION BY product
  ORDER BY
    tstamp
  MEASURES
    STRT.tstamp AS start_tstamp,
    LAST(UP.tstamp) AS peak_tstamp,
    LAST(DOWN.tstamp) AS end_tstamp,
    MATCH_NUMBER() AS mno
  ONE ROW PER MATCH
  AFTER MATCH SKIP TO LAST DOWN
  PATTERN (STRT UP+ FLAT* DOWN+)
  DEFINE
    UP AS UP.units_sold > PREV(UP.units_sold),
    FLAT AS FLAT.units_sold = PREV(FLAT.units_sold),
    DOWN AS DOWN.units_sold < PREV(DOWN.units_sold)
) MR`,
      undefined,
      { pretty: true },
    );
  }

  testJsonTable () {
    this.validateIdentity(
      'SELECT * FROM JSON_TABLE(foo FORMAT JSON, \'bla\' ERROR ON ERROR NULL ON EMPTY COLUMNS(foo PATH \'bar\'))',
    );
    this.validateIdentity(
      'SELECT * FROM JSON_TABLE(foo FORMAT JSON, \'bla\' ERROR ON ERROR NULL ON EMPTY COLUMNS foo PATH \'bar\')',
      'SELECT * FROM JSON_TABLE(foo FORMAT JSON, \'bla\' ERROR ON ERROR NULL ON EMPTY COLUMNS(foo PATH \'bar\'))',
    );
    this.validateIdentity(
      `SELECT
  CASE WHEN DBMS_LOB.GETLENGTH(info) < 32000 THEN DBMS_LOB.SUBSTR(info) END AS info_txt,
  info AS info_clob
FROM schemaname.tablename ar
INNER JOIN JSON_TABLE(:emps, '$[*]' COLUMNS(empno NUMBER PATH '$')) jt
  ON ar.empno = jt.empno`,
      undefined,
      { pretty: true },
    );
    this.validateIdentity(
      `SELECT
  *
FROM JSON_TABLE(res, '$.info[*]' COLUMNS(
  tempid NUMBER PATH '$.tempid',
  NESTED PATH '$.calid[*]' COLUMNS(last_dt PATH '$.last_dt ')
)) src`,
      undefined,
      { pretty: true },
    );
    this.validateIdentity('CONVERT(\'foo\', \'dst\')');
    this.validateIdentity('CONVERT(\'foo\', \'dst\', \'src\')');
  }

  testConnectBy () {
    const start = 'START WITH last_name = \'King\'';
    const connect = 'CONNECT BY PRIOR employee_id = manager_id AND LEVEL <= 4';
    const body = `
            SELECT last_name "Employee",
            LEVEL, SYS_CONNECT_BY_PATH(last_name, '/')  "Path"
            FROM employees
            WHERE level <= 3 AND department_id = 80
        `;
    const pretty = `SELECT
  last_name AS "Employee",
  LEVEL,
  SYS_CONNECT_BY_PATH(last_name, '/') AS "Path"
FROM employees
WHERE
  level <= 3 AND department_id = 80
START WITH last_name = 'King'
CONNECT BY PRIOR employee_id = manager_id AND LEVEL <= 4`;

    for (const query of [`${body}${start}${connect}`, `${body}${connect}${start}`]) {
      this.validateIdentity(query, pretty, { pretty: true });
    }
  }

  testQueryRestrictions () {
    for (const restriction of ['READ ONLY', 'CHECK OPTION']) {
      for (const constraintName of [' CONSTRAINT name', '']) {
        this.validateIdentity(`SELECT * FROM tbl WITH ${restriction}${constraintName}`);
        this.validateIdentity(
          `CREATE VIEW view AS SELECT * FROM tbl WITH ${restriction}${constraintName}`,
        );
      }
    }
  }

  testMultitableInserts () {
    this.validateIdentity(
      'INSERT ALL '
      + 'INTO dest_tab1 (id, description) VALUES (id, description) '
      + 'INTO dest_tab2 (id, description) VALUES (id, description) '
      + 'INTO dest_tab3 (id, description) VALUES (id, description) '
      + 'SELECT id, description FROM source_tab',
    );
    this.validateIdentity(
      'INSERT ALL '
      + 'INTO pivot_dest (id, day, val) VALUES (id, \'mon\', mon_val) '
      + 'INTO pivot_dest (id, day, val) VALUES (id, \'tue\', tue_val) '
      + 'INTO pivot_dest (id, day, val) VALUES (id, \'wed\', wed_val) '
      + 'INTO pivot_dest (id, day, val) VALUES (id, \'thu\', thu_val) '
      + 'INTO pivot_dest (id, day, val) VALUES (id, \'fri\', fri_val) '
      + 'SELECT * '
      + 'FROM pivot_source',
    );
    this.validateIdentity(
      'INSERT ALL '
      + 'WHEN id <= 3 THEN '
      + 'INTO dest_tab1 (id, description) VALUES (id, description) '
      + 'WHEN id BETWEEN 4 AND 7 THEN '
      + 'INTO dest_tab2 (id, description) VALUES (id, description) '
      + 'WHEN id >= 8 THEN '
      + 'INTO dest_tab3 (id, description) VALUES (id, description) '
      + 'SELECT id, description '
      + 'FROM source_tab',
    );
    this.validateIdentity(
      'INSERT ALL '
      + 'WHEN id <= 3 THEN '
      + 'INTO dest_tab1 (id, description) VALUES (id, description) '
      + 'WHEN id BETWEEN 4 AND 7 THEN '
      + 'INTO dest_tab2 (id, description) VALUES (id, description) '
      + 'WHEN 1 = 1 THEN '
      + 'INTO dest_tab3 (id, description) VALUES (id, description) '
      + 'SELECT id, description '
      + 'FROM source_tab',
    );
    this.validateIdentity(
      'INSERT FIRST '
      + 'WHEN id <= 3 THEN '
      + 'INTO dest_tab1 (id, description) VALUES (id, description) '
      + 'WHEN id <= 5 THEN '
      + 'INTO dest_tab2 (id, description) VALUES (id, description) '
      + 'ELSE '
      + 'INTO dest_tab3 (id, description) VALUES (id, description) '
      + 'SELECT id, description '
      + 'FROM source_tab',
    );
    this.validateIdentity(
      'INSERT FIRST '
      + 'WHEN id <= 3 THEN '
      + 'INTO dest_tab1 (id, description) VALUES (id, description) '
      + 'ELSE '
      + 'INTO dest_tab2 (id, description) VALUES (id, description) '
      + 'INTO dest_tab3 (id, description) VALUES (id, description) '
      + 'SELECT id, description '
      + 'FROM source_tab',
    );
    this.validateIdentity(
      '/* COMMENT */ INSERT FIRST '
      + 'WHEN salary > 4000 THEN INTO emp2 '
      + 'WHEN salary > 5000 THEN INTO emp3 '
      + 'WHEN salary > 6000 THEN INTO emp4 '
      + 'SELECT salary FROM employees',
    );
  }

  testJsonFunctions () {
    for (const formatJson of ['', ' FORMAT JSON']) {
      for (const onCond of [
        '',
        ' TRUE ON ERROR',
        ' NULL ON EMPTY',
        ' DEFAULT 1 ON ERROR TRUE ON EMPTY',
      ]) {
        for (const passing of ['', ' PASSING \'name1\' AS "var1", \'name2\' AS "var2"']) {
          this.validateIdentity(
            `SELECT * FROM t WHERE JSON_EXISTS(name${formatJson}, '$[1].middle'${passing}${onCond})`,
          );
        }
      }
    }
  }

  testGrant () {
    const grantCmds = [
      'GRANT purchases_reader_role TO george, maria',
      'GRANT USAGE ON TYPE price TO finance_role',
      'GRANT USAGE ON DERBY AGGREGATE types.maxPrice TO sales_role',
    ];

    for (const sql of grantCmds) {
      this.validateIdentity(sql, undefined, { checkCommandWarning: true });
    }

    this.validateIdentity('GRANT SELECT ON TABLE t TO maria, harry');
    this.validateIdentity('GRANT SELECT ON TABLE s.v TO PUBLIC');
    this.validateIdentity('GRANT SELECT ON TABLE t TO purchases_reader_role');
    this.validateIdentity('GRANT UPDATE, TRIGGER ON TABLE t TO anita, zhi');
    this.validateIdentity('GRANT EXECUTE ON PROCEDURE p TO george');
    this.validateIdentity('GRANT USAGE ON SEQUENCE order_id TO sales_role');
  }

  testRevoke () {
    const revokeCmds = [
      'REVOKE purchases_reader_role FROM george, maria',
      'REVOKE USAGE ON TYPE price FROM finance_role',
      'REVOKE USAGE ON DERBY AGGREGATE types.maxPrice FROM sales_role',
    ];

    for (const sql of revokeCmds) {
      this.validateIdentity(sql, undefined, { checkCommandWarning: true });
    }

    this.validateIdentity('REVOKE SELECT ON TABLE t FROM maria, harry');
    this.validateIdentity('REVOKE SELECT ON TABLE s.v FROM PUBLIC');
    this.validateIdentity('REVOKE SELECT ON TABLE t FROM purchases_reader_role');
    this.validateIdentity('REVOKE UPDATE, TRIGGER ON TABLE t FROM anita, zhi');
    this.validateIdentity('REVOKE EXECUTE ON PROCEDURE p FROM george');
    this.validateIdentity('REVOKE USAGE ON SEQUENCE order_id FROM sales_role');
  }

  testDatetrunc () {
    this.validateAll(
      'TRUNC(SYSDATE, \'YEAR\')',
      {
        write: {
          clickhouse: 'DATE_TRUNC(\'YEAR\', CURRENT_TIMESTAMP())',
          oracle: 'TRUNC(SYSDATE, \'YEAR\')',
        },
      },
    );

    // Make sure units are not normalized e.g 'Q' -> 'QUARTER' and 'W' -> 'WEEK'
    for (const unit of ['\'Q\'', '\'W\'']) {
      this.validateIdentity(`TRUNC(x, ${unit})`);
    }
  }

  testTruncTypeInference () {
    // temporal + string: first arg typed as temporal
    this.parseOne('TRUNC(CAST(x AS DATE), \'MONTH\')').assertIs(DateTruncExpr);
    this.parseOne('TRUNC(SYSDATE, \'MONTH\')').assertIs(DateTruncExpr);

    // ? + string: untyped first arg, string second arg infers DateTrunc
    this.parseOne('TRUNC(col, \'MONTH\')').assertIs(DateTruncExpr);

    // numeric + int: first arg typed as numeric (literal infers type)
    this.validateIdentity('TRUNC(3.14159, 2)').assertIs(TruncExpr);

    // ? + int: untyped first arg, int second arg infers Trunc
    this.validateIdentity('TRUNC(price, 0)').assertIs(TruncExpr);

    // ? + ?: neither arg typed, fallback to Anonymous
    this.validateIdentity('TRUNC(foo, bar)').assertIs(AnonymousExpr);
  }

  testTrunc () {
    // Numeric truncation identity and transpilation
    this.validateIdentity('TRUNC(3.14159)').assertIs(TruncExpr);
    this.validateAll(
      'TRUNC(3.14159)',
      {
        write: {
          oracle: 'TRUNC(3.14159)',
          postgres: 'TRUNC(3.14159)',
          mysql: 'TRUNCATE(3.14159)',
          tsql: 'ROUND(3.14159, 0, 1)',
        },
      },
    );

    // Cross-dialect numeric truncation transpilation
    this.validateAll(
      'TRUNC(3.14159, 2)',
      {
        read: {
          mysql: 'TRUNCATE(3.14159, 2)',
          postgres: 'TRUNC(3.14159, 2)',
          snowflake: 'TRUNC(3.14159, 2)',
        },
        write: {
          oracle: 'TRUNC(3.14159, 2)',
          postgres: 'TRUNC(3.14159, 2)',
          mysql: 'TRUNCATE(3.14159, 2)',
          tsql: 'ROUND(3.14159, 2, 1)',
          snowflake: 'TRUNC(3.14159, 2)',
          bigquery: 'TRUNC(3.14159, 2)',
          duckdb: 'TRUNC(3.14159)',
          presto: 'TRUNCATE(3.14159, 2)',
          clickhouse: 'trunc(3.14159, 2)',
          spark: 'CAST(3.14159 AS BIGINT)',
        },
      },
    );

    // Date truncation with various units
    for (const unit of [
      'DAY',
      'WEEK',
      'MONTH',
      'QUARTER',
      'YEAR',
    ]) {
      this.validateAll(
        `TRUNC(CAST(x AS DATE), '${unit}')`,
        {
          write: {
            oracle: `TRUNC(CAST(x AS DATE), '${unit}')`,
            snowflake: `DATE_TRUNC('${unit}', CAST(x AS DATE))`,
            postgres: `DATE_TRUNC('${unit}', CAST(x AS DATE))`,
            bigquery: `DATE_TRUNC(CAST(x AS DATE), ${unit})`,
            duckdb: `DATE_TRUNC('${unit}', CAST(x AS DATE))`,
            tsql: `DATE_TRUNC('${unit}', CAST(x AS DATE))`,
            spark: `TRUNC(CAST(x AS DATE), '${unit}')`,
          },
        },
      );
    }

    // Timestamp truncation with various units
    for (const unit of [
      'HOUR',
      'MINUTE',
      'SECOND',
      'DAY',
      'MONTH',
      'YEAR',
    ]) {
      this.validateAll(
        `TRUNC(CAST(x AS TIMESTAMP), '${unit}')`,
        {
          write: {
            oracle: `TRUNC(CAST(x AS TIMESTAMP), '${unit}')`,
            snowflake: `DATE_TRUNC('${unit}', CAST(x AS TIMESTAMP))`,
            postgres: `DATE_TRUNC('${unit}', CAST(x AS TIMESTAMP))`,
            duckdb: `DATE_TRUNC('${unit}', CAST(x AS TIMESTAMP))`,
            tsql: `DATE_TRUNC('${unit}', CAST(x AS DATETIME2))`,
            spark: `TRUNC(CAST(x AS TIMESTAMP), '${unit}')`,
          },
        },
      );
    }
  }

  testAnalyze () {
    this.validateIdentity('ANALYZE TABLE tbl');
    this.validateIdentity('ANALYZE INDEX ndx');
    this.validateIdentity('ANALYZE TABLE db.tbl PARTITION(foo = \'foo\', bar = \'bar\')');
    this.validateIdentity('ANALYZE TABLE db.tbl SUBPARTITION(foo = \'foo\', bar = \'bar\')');
    this.validateIdentity('ANALYZE INDEX db.ndx PARTITION(foo = \'foo\', bar = \'bar\')');
    this.validateIdentity('ANALYZE INDEX db.ndx PARTITION(part1)');
    this.validateIdentity('ANALYZE CLUSTER db.cluster');
    this.validateIdentity('ANALYZE TABLE tbl VALIDATE REF UPDATE');
    this.validateIdentity('ANALYZE LIST CHAINED ROWS');
    this.validateIdentity('ANALYZE LIST CHAINED ROWS INTO tbl');
    this.validateIdentity('ANALYZE DELETE STATISTICS');
    this.validateIdentity('ANALYZE DELETE SYSTEM STATISTICS');
    this.validateIdentity('ANALYZE VALIDATE REF UPDATE');
    this.validateIdentity('ANALYZE VALIDATE REF UPDATE SET DANGLING TO NULL');
    this.validateIdentity('ANALYZE VALIDATE STRUCTURE');
    this.validateIdentity('ANALYZE VALIDATE STRUCTURE CASCADE FAST');
    this.validateIdentity(
      'ANALYZE TABLE tbl VALIDATE STRUCTURE CASCADE COMPLETE ONLINE INTO db.tbl',
    );
    this.validateIdentity(
      'ANALYZE TABLE tbl VALIDATE STRUCTURE CASCADE COMPLETE OFFLINE INTO db.tbl',
    );
  }

  testPrior () {
    this.validateIdentity(
      'SELECT id, PRIOR name AS parent_name, name FROM tree CONNECT BY NOCYCLE PRIOR id = parent_id',
    );

    expect(() => parseOne('PRIOR as foo', { read: 'oracle' })).toThrow(ParseError);
  }

  testUtcTime () {
    this.validateIdentity('UTC_TIME()').assertIs(UtcTimeExpr);
    this.validateIdentity('UTC_TIME(6)').assertIs(UtcTimeExpr);
    this.validateIdentity('UTC_TIMESTAMP()').assertIs(UtcTimestampExpr);
    this.validateIdentity('UTC_TIMESTAMP(6)').assertIs(UtcTimestampExpr);
  }

  testChr () {
    this.validateIdentity('SELECT CHR(187 USING NCHAR_CS)');
    this.validateIdentity('SELECT CHR(187)');
  }
}

const t = new TestOracle();
describe('TestOracle', () => {
  test('oracle', () => t.testOracle());
  test('testJoinMarker', () => t.testJoinMarker());
  test('hints', () => t.testHints());
  test('testXmlTable', () => t.testXmlTable());
  test('testMatchRecognize', () => t.testMatchRecognize());
  test('testJsonTable', () => t.testJsonTable());
  test('testConnectBy', () => t.testConnectBy());
  test('testQueryRestrictions', () => t.testQueryRestrictions());
  test('testMultitableInserts', () => t.testMultitableInserts());
  test('testJsonFunctions', () => t.testJsonFunctions());
  test('grant', () => t.testGrant());
  test('revoke', () => t.testRevoke());
  test('datetrunc', () => t.testDatetrunc());
  test('testTruncTypeInference', () => t.testTruncTypeInference());
  test('trunc', () => t.testTrunc());
  test('analyze', () => t.testAnalyze());
  test('prior', () => t.testPrior());
  test('testUtcTime', () => t.testUtcTime());
  test('chr', () => t.testChr());
});
