import {
  describe, test, expect,
} from 'vitest';
import {
  parseOne, parse,
} from '../../../src/index';
import {
  Expression,
  AliasExpr, IdentifierExpr, ColumnExpr, CommandExpr, AlterExpr, DropExpr,
  AddConstraintExpr, ConstraintExpr, QueryOptionExpr, ParameterExpr, VarExpr,
  TableExpr, InsertExpr, DateExpr, TimeExpr, TimestampExpr,
} from '../../../src/expressions';
import { narrowInstanceOf } from '../../../src/port_internals';
import {
  Validator, UnsupportedError,
} from './validator';

class TestTSQL extends Validator {
  override dialect = 'tsql' as const;

  testTsql () {
    this.validateAll(
      'WITH x AS (SELECT 1 AS [1]) SELECT TOP 0 * FROM (SELECT * FROM x UNION SELECT * FROM x) AS _l_0 ORDER BY 1',
      {
        read: {
          '': 'WITH x AS (SELECT 1) SELECT * FROM x UNION SELECT * FROM x ORDER BY 1 LIMIT 0',
        },
      },
    );

    this.validateIdentity('SELECT * FROM a..b');
    this.validateIdentity('SELECT ATN2(x, y)');
    this.validateIdentity('SELECT EXP(1)');
    this.validateIdentity('SELECT SYSDATETIMEOFFSET()');
    this.validateIdentity('SELECT COMPRESS(\'Hello World\')');
    this.validateIdentity('GO').assertIs(CommandExpr);
    this.validateIdentity('SELECT go').selects[0].assertIs(ColumnExpr);
    this.validateIdentity('CREATE view a.b.c', 'CREATE VIEW b.c');
    this.validateIdentity('DROP view a.b.c', 'DROP VIEW b.c');
    this.validateIdentity('ROUND(x, 1, 0)');
    this.validateIdentity('EXEC MyProc @id=7, @name=\'Lochristi\'', undefined, { checkCommandWarning: true });
    this.validateIdentity('SELECT TRIM(\'     test    \') AS Result');
    this.validateIdentity('SELECT TRIM(\'.,! \' FROM \'     #     test    .\') AS Result');
    this.validateIdentity('SELECT * FROM t TABLESAMPLE (10 PERCENT)');
    this.validateIdentity('SELECT * FROM t TABLESAMPLE (20 ROWS)');
    this.validateIdentity('SELECT * FROM t TABLESAMPLE (10 PERCENT) REPEATABLE (123)');
    this.validateIdentity('SELECT CONCAT(column1, column2)');
    this.validateIdentity('SELECT TestSpecialChar.Test# FROM TestSpecialChar');
    this.validateIdentity('SELECT TestSpecialChar.Test@ FROM TestSpecialChar');
    this.validateIdentity('SELECT TestSpecialChar.Test$ FROM TestSpecialChar');
    this.validateIdentity('SELECT TestSpecialChar.Test_ FROM TestSpecialChar');
    this.validateIdentity('SELECT TOP (2 + 1) 1');
    this.validateIdentity('SELECT * FROM t WHERE NOT c', 'SELECT * FROM t WHERE NOT c <> 0');
    this.validateIdentity('1 AND true', '1 <> 0 AND (1 = 1)');
    this.validateIdentity('CAST(x AS int) OR y', 'CAST(x AS INTEGER) <> 0 OR y <> 0');
    this.validateIdentity('TRUNCATE TABLE t1 WITH (PARTITIONS(1, 2 TO 5, 10 TO 20, 84))');
    this.validateIdentity(
      'WITH t1 AS (SELECT 1 AS a), t2 AS (SELECT 1 AS a) SELECT TOP 10 a FROM t1 UNION ALL SELECT TOP 10 a FROM t2',
    );
    this.validateIdentity(
      'SELECT TOP 10 s.RECORDID, n.c.VALUE(\'(/*:FORM_ROOT/*:SOME_TAG)[1]\', \'float\') AS SOME_TAG_VALUE FROM source_table.dbo.source_data AS s(nolock) CROSS APPLY FormContent.nodes(\'/*:FORM_ROOT\') AS N(C)',
    );
    this.validateIdentity(
      'CREATE CLUSTERED INDEX [IX_OfficeTagDetail_TagDetailID] ON [dbo].[OfficeTagDetail]([TagDetailID] ASC)',
    );
    this.validateIdentity(
      'CREATE INDEX [x] ON [y]([z] ASC) WITH (allow_page_locks=on) ON X([y])',
    );
    this.validateIdentity(
      'CREATE INDEX [x] ON [y]([z] ASC) WITH (allow_page_locks=on) ON PRIMARY',
    );
    this.validateIdentity(
      'COPY INTO test_1 FROM \'path\' WITH (FORMAT_NAME = test, FILE_TYPE = \'CSV\', CREDENTIAL = (IDENTITY=\'Shared Access Signature\', SECRET=\'token\'), FIELDTERMINATOR = \';\', ROWTERMINATOR = \'0X0A\', ENCODING = \'UTF8\', DATEFORMAT = \'ymd\', MAXERRORS = 10, ERRORFILE = \'errorsfolder\', IDENTITY_INSERT = \'ON\')',
    );
    this.validateIdentity(
      'WITH t1 AS (SELECT 1 AS a), t2 AS (SELECT 1 AS a) SELECT TOP 10 a FROM t1 UNION ALL SELECT TOP 10 a FROM t2 ORDER BY a DESC',
    );
    this.validateIdentity(
      'WITH t1 AS (SELECT 1 AS a), t2 AS (SELECT 1 AS a) SELECT COUNT(*) FROM (SELECT TOP 10 a FROM t1 UNION ALL SELECT TOP 10 a FROM t2 ORDER BY a DESC) AS t',
    );
    this.validateIdentity('SELECT 1 AS "[x]"', 'SELECT 1 AS [[x]]]');
    this.validateIdentity(
      'INSERT INTO foo.bar WITH cte AS (SELECT 1 AS one) SELECT * FROM cte',
      'WITH cte AS (SELECT 1 AS one) INSERT INTO foo.bar SELECT * FROM cte',
    );

    this.validateAll(
      'CREATE TABLE test_table([ID] [BIGINT] NOT NULL,[EffectiveFrom] [DATETIME2] (3) NOT NULL)',
      {
        write: {
          spark: 'CREATE TABLE test_table (`ID` BIGINT NOT NULL, `EffectiveFrom` TIMESTAMP NOT NULL)',
          tsql: 'CREATE TABLE test_table ([ID] BIGINT NOT NULL, [EffectiveFrom] DATETIME2(3) NOT NULL)',
        },
      },
    );
    this.validateAll('SELECT CONVERT(DATETIME, \'2006-04-25T15:50:59.997\', 126)', {
      write: {
        duckdb: 'SELECT STRPTIME(\'2006-04-25T15:50:59.997\', \'%Y-%m-%dT%H:%M:%S.%f\')',
        tsql: 'SELECT CONVERT(DATETIME, \'2006-04-25T15:50:59.997\', 126)',
      },
    });
    this.validateAll(
      'WITH A AS (SELECT 2 AS value), C AS (SELECT * FROM A) SELECT * INTO TEMP_NESTED_WITH FROM (SELECT * FROM C) AS temp',
      {
        read: {
          snowflake: 'CREATE TABLE TEMP_NESTED_WITH AS WITH C AS (WITH A AS (SELECT 2 AS value) SELECT * FROM A) SELECT * FROM C',
          tsql: 'WITH A AS (SELECT 2 AS value), C AS (SELECT * FROM A) SELECT * INTO TEMP_NESTED_WITH FROM (SELECT * FROM C) AS temp',
        },
        write: {
          snowflake: 'CREATE TABLE TEMP_NESTED_WITH AS WITH A AS (SELECT 2 AS value), C AS (SELECT * FROM A) SELECT * FROM (SELECT * FROM C) AS temp',
        },
      },
    );
    this.validateAll('SELECT IIF(cond <> 0, \'True\', \'False\')', {
      read: {
        spark: 'SELECT IF(cond, \'True\', \'False\')',
        sqlite: 'SELECT IIF(cond, \'True\', \'False\')',
        tsql: 'SELECT IIF(cond <> 0, \'True\', \'False\')',
      },
    });
    this.validateAll('SELECT TRIM(BOTH \'a\' FROM a)', {
      read: { mysql: 'SELECT TRIM(BOTH \'a\' FROM a)' },
      write: {
        mysql: 'SELECT TRIM(BOTH \'a\' FROM a)',
        tsql: 'SELECT TRIM(BOTH \'a\' FROM a)',
      },
    });
    this.validateAll('SELECT TIMEFROMPARTS(23, 59, 59, 0, 0)', {
      read: {
        duckdb: 'SELECT MAKE_TIME(23, 59, 59)',
        mysql: 'SELECT MAKETIME(23, 59, 59)',
        postgres: 'SELECT MAKE_TIME(23, 59, 59)',
        snowflake: 'SELECT TIME_FROM_PARTS(23, 59, 59)',
      },
      write: { tsql: 'SELECT TIMEFROMPARTS(23, 59, 59, 0, 0)' },
    });
    this.validateAll('SELECT DATETIMEFROMPARTS(2013, 4, 5, 12, 00, 00, 0)', {
      read: { snowflake: 'SELECT TIMESTAMP_FROM_PARTS(2013, 4, 5, 12, 00, 00, 987654321)' },
      write: {
        duckdb: 'SELECT MAKE_TIMESTAMP(2013, 4, 5, 12, 00, 00 + (0 / 1000.0))',
        snowflake: 'SELECT TIMESTAMP_FROM_PARTS(2013, 4, 5, 12, 00, 00, 0 * 1000000)',
        tsql: 'SELECT DATETIMEFROMPARTS(2013, 4, 5, 12, 00, 00, 0)',
      },
    });
    this.validateAll('SELECT TOP 1 * FROM (SELECT x FROM t1 UNION ALL SELECT x FROM t2) AS _l_0', {
      read: { '': 'SELECT x FROM t1 UNION ALL SELECT x FROM t2 LIMIT 1' },
    });
    this.validateAll('WITH t(c) AS (SELECT 1) SELECT * INTO foo FROM (SELECT c AS c FROM t) AS temp', {
      read: { duckdb: 'CREATE TABLE foo AS WITH t(c) AS (SELECT 1) SELECT c FROM t' },
    });
    this.validateAll('WITH t(c) AS (SELECT 1) SELECT * INTO foo FROM (SELECT c AS c FROM t) AS temp', {
      write: {
        duckdb: 'CREATE TABLE foo AS WITH t(c) AS (SELECT 1) SELECT * FROM (SELECT c AS c FROM t) AS temp',
        postgres: 'WITH t(c) AS (SELECT 1) SELECT * INTO foo FROM (SELECT c AS c FROM t) AS temp',
        oracle: 'WITH t(c) AS (SELECT 1) SELECT * INTO foo FROM (SELECT c AS c FROM t) temp',
      },
    });
    this.validateAll(
      'WITH t(c) AS (SELECT 1) SELECT * INTO UNLOGGED #foo FROM (SELECT c AS c FROM t) AS temp',
      {
        write: {
          duckdb: 'CREATE TEMPORARY TABLE foo AS WITH t(c) AS (SELECT 1) SELECT * FROM (SELECT c AS c FROM t) AS temp',
          postgres: 'WITH t(c) AS (SELECT 1) SELECT * INTO TEMPORARY foo FROM (SELECT c AS c FROM t) AS temp',
        },
      },
    );
    this.validateAll('WITH t(c) AS (SELECT 1) SELECT c INTO #foo FROM t', {
      read: {
        tsql: 'WITH t(c) AS (SELECT 1) SELECT c INTO #foo FROM t',
        postgres: 'WITH t(c) AS (SELECT 1) SELECT c INTO TEMPORARY foo FROM t',
      },
      write: {
        tsql: 'WITH t(c) AS (SELECT 1) SELECT c INTO #foo FROM t',
        postgres: 'WITH t(c) AS (SELECT 1) SELECT c INTO TEMPORARY foo FROM t',
        duckdb: 'CREATE TEMPORARY TABLE foo AS WITH t(c) AS (SELECT 1) SELECT c FROM t',
        snowflake: 'CREATE TEMPORARY TABLE foo AS WITH t(c) AS (SELECT 1) SELECT c FROM t',
      },
    });
    this.validateAll(
      'WITH t(c) AS (SELECT 1) SELECT * INTO UNLOGGED foo FROM (SELECT c AS c FROM t) AS temp',
      {
        write: {
          duckdb: 'CREATE TABLE foo AS WITH t(c) AS (SELECT 1) SELECT * FROM (SELECT c AS c FROM t) AS temp',
        },
      },
    );
    this.validateAll('WITH y AS (SELECT 2 AS c) INSERT INTO #t SELECT * FROM y', {
      write: {
        duckdb: 'WITH y AS (SELECT 2 AS c) INSERT INTO t SELECT * FROM y',
        postgres: 'WITH y AS (SELECT 2 AS c) INSERT INTO t SELECT * FROM y',
      },
    });
    this.validateAll('WITH y AS (SELECT 2 AS c) INSERT INTO t SELECT * FROM y', {
      read: { duckdb: 'WITH y AS (SELECT 2 AS c) INSERT INTO t SELECT * FROM y' },
    });
    this.validateAll('WITH t(c) AS (SELECT 1) SELECT 1 AS c UNION (SELECT c FROM t)', {
      read: { duckdb: 'SELECT 1 AS c UNION (WITH t(c) AS (SELECT 1) SELECT c FROM t)' },
    });
    this.validateAll(
      'WITH t(c) AS (SELECT 1) MERGE INTO x AS z USING (SELECT c AS c FROM t) AS y ON a = b WHEN MATCHED THEN UPDATE SET a = y.b',
      {
        read: {
          postgres: 'MERGE INTO x AS z USING (WITH t(c) AS (SELECT 1) SELECT c FROM t) AS y ON a = b WHEN MATCHED THEN UPDATE SET a = y.b',
        },
      },
    );
    this.validateAll(
      'WITH t(n) AS (SELECT 1 AS n UNION ALL SELECT n + 1 AS n FROM t WHERE n < 4) SELECT * FROM (SELECT SUM(n) AS s4 FROM t) AS subq',
      {
        read: {
          duckdb: 'SELECT * FROM (WITH RECURSIVE t(n) AS (SELECT 1 AS n UNION ALL SELECT n + 1 AS n FROM t WHERE n < 4) SELECT SUM(n) AS s4 FROM t) AS subq',
        },
      },
    );
    this.validateAll('CREATE TABLE #mytemptable (a INTEGER)', {
      read: { duckdb: 'CREATE TEMPORARY TABLE mytemptable (a INT)' },
      write: {
        tsql: 'CREATE TABLE #mytemptable (a INTEGER)',
        snowflake: 'CREATE TEMPORARY TABLE mytemptable (a INT)',
        duckdb: 'CREATE TEMPORARY TABLE mytemptable (a INT)',
        oracle: 'CREATE GLOBAL TEMPORARY TABLE mytemptable (a INT)',
        hive: 'CREATE TEMPORARY TABLE mytemptable (a INT)',
        spark2: 'CREATE TEMPORARY TABLE mytemptable (a INT) USING PARQUET',
        spark: 'CREATE TEMPORARY TABLE mytemptable (a INT) USING PARQUET',
        databricks: 'CREATE TEMPORARY TABLE mytemptable (a INT) USING PARQUET',
      },
    });
    this.validateAll(
      'CREATE TABLE #mytemp (a INTEGER, b CHAR(2), c TIME(4), d FLOAT(24))',
      {
        write: {
          spark: 'CREATE TEMPORARY TABLE mytemp (a INT, b CHAR(2), c TIMESTAMP, d FLOAT) USING PARQUET',
          tsql: 'CREATE TABLE #mytemp (a INTEGER, b CHAR(2), c TIME(4), d FLOAT(24))',
        },
      },
    );
    this.validateAll(
      `CREATE TABLE [dbo].[mytable](
                [email] [varchar](255) NOT NULL,
                CONSTRAINT [UN_t_mytable] UNIQUE NONCLUSTERED
                (
                    [email] ASC
                )
                )`,
      {
        write: {
          hive: 'CREATE TABLE `dbo`.`mytable` (`email` VARCHAR(255) NOT NULL)',
          spark2: 'CREATE TABLE `dbo`.`mytable` (`email` VARCHAR(255) NOT NULL)',
          spark: 'CREATE TABLE `dbo`.`mytable` (`email` VARCHAR(255) NOT NULL)',
          databricks: 'CREATE TABLE `dbo`.`mytable` (`email` VARCHAR(255) NOT NULL)',
        },
      },
    );
    this.validateAll('CREATE TABLE x ( A INTEGER NOT NULL, B INTEGER NULL )', {
      write: {
        tsql: 'CREATE TABLE x (A INTEGER NOT NULL, B INTEGER NULL)',
        hive: 'CREATE TABLE x (A INT NOT NULL, B INT)',
      },
    });
    this.validateIdentity(
      'CREATE TABLE x (CONSTRAINT "pk_mytable" UNIQUE NONCLUSTERED (a DESC)) ON b (c)',
      'CREATE TABLE x (CONSTRAINT [pk_mytable] UNIQUE NONCLUSTERED (a DESC)) ON b (c)',
    );
    this.validateAll(
      'CREATE TABLE x ([zip_cd] VARCHAR(5) NULL NOT FOR REPLICATION, [zip_cd_mkey] VARCHAR(5) NOT NULL, CONSTRAINT [pk_mytable] PRIMARY KEY CLUSTERED ([zip_cd_mkey] ASC) WITH (PAD_INDEX=ON, STATISTICS_NORECOMPUTE=OFF) ON [INDEX]) ON [SECONDARY]',
      {
        write: {
          tsql: 'CREATE TABLE x ([zip_cd] VARCHAR(5) NULL NOT FOR REPLICATION, [zip_cd_mkey] VARCHAR(5) NOT NULL, CONSTRAINT [pk_mytable] PRIMARY KEY CLUSTERED ([zip_cd_mkey] ASC) WITH (PAD_INDEX=ON, STATISTICS_NORECOMPUTE=OFF) ON [INDEX]) ON [SECONDARY]',
          spark2: 'CREATE TABLE x (`zip_cd` VARCHAR(5), `zip_cd_mkey` VARCHAR(5) NOT NULL, CONSTRAINT `pk_mytable` PRIMARY KEY (`zip_cd_mkey`))',
        },
      },
    );
    this.validateIdentity('CREATE TABLE x (A INTEGER NOT NULL, B INTEGER NULL)');
    this.validateAll('CREATE TABLE x ( A INTEGER NOT NULL, B INTEGER NULL )', {
      write: { hive: 'CREATE TABLE x (A INT NOT NULL, B INT)' },
    });
    this.validateIdentity(
      'CREATE TABLE tbl (a AS (x + 1) PERSISTED, b AS (y + 2), c AS (y / 3) PERSISTED NOT NULL)',
    );
    this.validateIdentity(
      'CREATE TABLE [db].[tbl]([a] [int])',
      'CREATE TABLE [db].[tbl] ([a] INTEGER)',
    );
    this.validateIdentity('SELECT a = 1', 'SELECT 1 AS a').selects[0].assertIs(AliasExpr)
      .args['alias'].assertIs(IdentifierExpr);

    this.validateAll(
      'IF OBJECT_ID(\'tempdb.dbo.#TempTableName\', \'U\') IS NOT NULL DROP TABLE #TempTableName',
      {
        write: {
          tsql: 'DROP TABLE IF EXISTS #TempTableName',
          spark: 'DROP TABLE IF EXISTS TempTableName',
        },
      },
    );
    this.validateIdentity(
      'MERGE INTO mytable WITH (HOLDLOCK) AS T USING mytable_merge AS S ON (T.user_id = S.user_id) WHEN NOT MATCHED THEN INSERT (c1, c2) VALUES (S.c1, S.c2)',
    );
    this.validateIdentity('UPDATE STATISTICS x', undefined, { checkCommandWarning: true });
    this.validateIdentity('UPDATE x SET y = 1 OUTPUT x.a, x.b INTO @y FROM y');
    this.validateIdentity('UPDATE x SET y = 1 OUTPUT x.a, x.b FROM y');
    this.validateIdentity('INSERT INTO x (y) OUTPUT x.a, x.b INTO l SELECT * FROM z');
    this.validateIdentity('INSERT INTO x (y) OUTPUT x.a, x.b SELECT * FROM z');
    this.validateIdentity('DELETE x OUTPUT x.a FROM z');
    this.validateIdentity('SELECT * FROM t WITH (TABLOCK, INDEX(myindex))');
    this.validateIdentity('SELECT * FROM t WITH (NOWAIT)');
    this.validateIdentity('SELECT CASE WHEN a > 1 THEN b END');
    this.validateIdentity('SELECT * FROM taxi ORDER BY 1 OFFSET 0 ROWS FETCH NEXT 3 ROWS ONLY');
    this.validateIdentity('END');
    this.validateIdentity('@x');
    this.validateIdentity('#x');
    this.validateIdentity('PRINT @TestVariable', undefined, { checkCommandWarning: true });
    this.validateIdentity('SELECT Employee_ID, Department_ID FROM @MyTableVar');
    this.validateIdentity('INSERT INTO @TestTable VALUES (1, \'Value1\', 12, 20)');
    this.validateIdentity('SELECT * FROM #foo');
    this.validateIdentity('SELECT * FROM ##foo');
    this.validateIdentity('SELECT a = 1', 'SELECT 1 AS a');
    this.validateIdentity('DECLARE @TestVariable AS VARCHAR(100) = \'Save Our Planet\'');
    this.validateIdentity('SELECT a = 1 UNION ALL SELECT a = b', 'SELECT 1 AS a UNION ALL SELECT b AS a');
    this.validateIdentity(
      'SELECT x FROM @MyTableVar AS m JOIN Employee ON m.EmployeeID = Employee.EmployeeID',
    );
    this.validateIdentity(
      'SELECT DISTINCT DepartmentName, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY BaseRate) OVER (PARTITION BY DepartmentName) AS MedianCont FROM dbo.DimEmployee',
    );
    this.validateIdentity('SELECT "x"."y" FROM foo', 'SELECT [x].[y] FROM foo');

    this.validateAll('SELECT * FROM t ORDER BY (SELECT NULL) OFFSET 2 ROWS', {
      read: { postgres: 'SELECT * FROM t OFFSET 2' },
      write: {
        postgres: 'SELECT * FROM t ORDER BY (SELECT NULL) NULLS FIRST OFFSET 2',
        tsql: 'SELECT * FROM t ORDER BY (SELECT NULL) OFFSET 2 ROWS',
      },
    });
    this.validateAll(
      'SELECT * FROM t ORDER BY (SELECT NULL) OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY',
      {
        read: {
          duckdb: 'SELECT * FROM t LIMIT 10 OFFSET 5',
          sqlite: 'SELECT * FROM t LIMIT 5, 10',
          tsql: 'SELECT * FROM t ORDER BY (SELECT NULL) OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY',
        },
        write: {
          duckdb: 'SELECT * FROM t ORDER BY (SELECT NULL) NULLS FIRST LIMIT 10 OFFSET 5',
          sqlite: 'SELECT * FROM t ORDER BY (SELECT NULL) LIMIT 10 OFFSET 5',
        },
      },
    );
    this.validateAll('SELECT CAST([a].[b] AS SMALLINT) FROM foo', {
      write: {
        tsql: 'SELECT CAST([a].[b] AS SMALLINT) FROM foo',
        spark: 'SELECT CAST(`a`.`b` AS SMALLINT) FROM foo',
      },
    });
    this.validateAll('CONVERT(INT, CONVERT(NUMERIC, \'444.75\'))', {
      write: {
        mysql: 'CAST(CAST(\'444.75\' AS DECIMAL) AS SIGNED)',
        tsql: 'CONVERT(INTEGER, CONVERT(NUMERIC, \'444.75\'))',
      },
    });
    this.validateAll('STRING_AGG(x, y) WITHIN GROUP (ORDER BY z DESC)', {
      write: {
        tsql: 'STRING_AGG(x, y) WITHIN GROUP (ORDER BY z DESC)',
        mysql: 'GROUP_CONCAT(x ORDER BY z DESC SEPARATOR y)',
        sqlite: 'GROUP_CONCAT(x, y)',
        postgres: 'STRING_AGG(x, y ORDER BY z DESC NULLS LAST)',
      },
    });
    this.validateAll('STRING_AGG(x, \'|\') WITHIN GROUP (ORDER BY z ASC)', {
      write: {
        tsql: 'STRING_AGG(x, \'|\') WITHIN GROUP (ORDER BY z ASC)',
        mysql: 'GROUP_CONCAT(x ORDER BY z ASC SEPARATOR \'|\')',
        sqlite: 'GROUP_CONCAT(x, \'|\')',
        postgres: 'STRING_AGG(x, \'|\' ORDER BY z ASC NULLS FIRST)',
      },
    });
    this.validateAll('STRING_AGG(x, \'|\')', {
      write: {
        tsql: 'STRING_AGG(x, \'|\')',
        mysql: 'GROUP_CONCAT(x SEPARATOR \'|\')',
        sqlite: 'GROUP_CONCAT(x, \'|\')',
        postgres: 'STRING_AGG(x, \'|\')',
      },
    });
    this.validateAll('HASHBYTES(\'SHA1\', x)', {
      read: {
        snowflake: 'SHA1(x)',
        spark: 'SHA(x)',
      },
      write: {
        snowflake: 'SHA1(x)',
        spark: 'SHA(x)',
        tsql: 'HASHBYTES(\'SHA1\', x)',
      },
    });
    this.validateAll('HASHBYTES(\'SHA2_256\', x)', {
      read: { spark: 'SHA2(x, 256)' },
      write: {
        tsql: 'HASHBYTES(\'SHA2_256\', x)',
        spark: 'SHA2(x, 256)',
      },
    });
    this.validateAll('HASHBYTES(\'SHA2_512\', x)', {
      read: { spark: 'SHA2(x, 512)' },
      write: {
        tsql: 'HASHBYTES(\'SHA2_512\', x)',
        spark: 'SHA2(x, 512)',
      },
    });
    this.validateAll('HASHBYTES(\'MD5\', \'x\')', {
      read: { spark: 'MD5(\'x\')' },
      write: {
        tsql: 'HASHBYTES(\'MD5\', \'x\')',
        spark: 'MD5(\'x\')',
      },
    });
    this.validateIdentity('HASHBYTES(\'MD2\', \'x\')');
    this.validateIdentity('LOG(n)');
    this.validateIdentity('LOG(n, b)');
    this.validateAll('STDEV(x)', {
      read: { '': 'STDDEV(x)' },
      write: {
        '': 'STDDEV(x)',
        'tsql': 'STDEV(x)',
      },
    });
    this.validateIdentity(
      'SELECT val FROM (VALUES ((TRUE), (FALSE), (NULL))) AS t(val)',
      'SELECT val FROM (VALUES ((1), (0), (NULL))) AS t(val)',
    );
    this.validateIdentity('\'a\' + \'b\'');
    this.validateIdentity('\'a\' || \'b\'', '\'a\' + \'b\'');
    this.validateIdentity('CREATE TABLE db.t1 (a INTEGER, b VARCHAR(50), CONSTRAINT c PRIMARY KEY (a DESC))');
    this.validateIdentity(
      'CREATE TABLE db.t1 (a INTEGER, b INTEGER, CONSTRAINT c PRIMARY KEY (a DESC, b))',
    );
    this.validateAll('SCHEMA_NAME(id)', {
      write: {
        sqlite: '\'main\'',
        mysql: 'SCHEMA()',
        postgres: 'CURRENT_SCHEMA',
        tsql: 'SCHEMA_NAME(id)',
      },
    });

    expect(() => parseOne('SELECT begin', { read: 'tsql' })).toThrow();

    this.validateIdentity('CREATE PROCEDURE test(@v1 INTEGER = 1, @v2 CHAR(1) = \'c\')');
    this.validateIdentity('DECLARE @v1 AS INTEGER = 1, @v2 AS CHAR(1) = \'c\'');

    for (const output of [
      'OUT',
      'OUTPUT',
      'READONLY',
    ]) {
      this.validateIdentity(`CREATE PROCEDURE test(@v1 INTEGER = 1 ${output}, @v2 CHAR(1) ${output})`);
    }

    this.validateIdentity(
      'CREATE PROCEDURE test(@v1 AS INTEGER = 1, @v2 AS CHAR(1) = \'c\')',
      'CREATE PROCEDURE test(@v1 INTEGER = 1, @v2 CHAR(1) = \'c\')',
    );

    for (const orderBy of ['', ' ORDER BY c']) {
      for (const jsonClause of [
        '',
        ' NULL ON NULL',
        ' ABSENT ON NULL',
      ]) {
        this.validateIdentity(`JSON_ARRAYAGG(c${orderBy}${jsonClause})`);
      }
    }

    this.validateAll('JSON_ARRAYAGG(c1 ORDER BY c1)', {
      write: {
        tsql: 'JSON_ARRAYAGG(c1 ORDER BY c1)',
        postgres: 'JSON_AGG(c1 ORDER BY c1 NULLS FIRST)',
      },
    });
    this.validateIdentity('CEILING(2)');
  }

  testOption () {
    const possibleOptions = [
      'HASH GROUP',
      'ORDER GROUP',
      'CONCAT UNION',
      'HASH UNION',
      'MERGE UNION',
      'LOOP JOIN',
      'MERGE JOIN',
      'HASH JOIN',
      'DISABLE_OPTIMIZED_PLAN_FORCING',
      'EXPAND VIEWS',
      'FAST 15',
      'FORCE ORDER',
      'FORCE EXTERNALPUSHDOWN',
      'DISABLE EXTERNALPUSHDOWN',
      'FORCE SCALEOUTEXECUTION',
      'DISABLE SCALEOUTEXECUTION',
      'IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX',
      'KEEP PLAN',
      'KEEPFIXED PLAN',
      'MAX_GRANT_PERCENT = 5',
      'MIN_GRANT_PERCENT = 10',
      'MAXDOP 13',
      'MAXRECURSION 8',
      'NO_PERFORMANCE_SPOOL',
      'OPTIMIZE FOR UNKNOWN',
      'PARAMETERIZATION SIMPLE',
      'PARAMETERIZATION FORCED',
      'QUERYTRACEON 99',
      'RECOMPILE',
      'ROBUST PLAN',
      'USE PLAN N\'<xml_plan>\'',
      'LABEL = \'MyLabel\'',
    ];
    const possibleStatements = [
      'SELECT * FROM Table1',
      'SELECT * FROM Table1 WHERE id = 2',
      'UPDATE t1 SET k = t2.k FROM t2',
    ];

    for (const statement of possibleStatements) {
      for (const option of possibleOptions) {
        const query = `${statement} OPTION(${option})`;
        const result = this.validateIdentity(query);
        const options = result.args['options'];
        expect(Array.isArray(options)).toBe(true);
        expect(options.every((o: unknown) => o instanceof QueryOptionExpr)).toBe(true);
      }
      this.validateIdentity(`${statement} OPTION(RECOMPILE, USE PLAN N'<xml_plan>', MAX_GRANT_PERCENT = 5)`);
    }

    const raisingQueries = [
      'SELECT * FROM Table1 OPTION HASH GROUP',
      'SELECT * FROM Table1 OPTION(KEEPFIXED)',
      'SELECT * FROM Table1 OPTION(HASH GROUP HASH GROUP)',
    ];
    for (const query of raisingQueries) {
      expect(() => this.parseOne(query)).toThrow();
    }

    this.validateAll('SELECT col FROM t OPTION(LABEL = \'foo\')', {
      write: {
        tsql: 'SELECT col FROM t OPTION(LABEL = \'foo\')',
        databricks: UnsupportedError,
      },
    });
  }

  testForXml () {
    const xmlPossibleOptions = [
      'RAW(\'ElementName\')',
      'RAW(\'ElementName\'), BINARY BASE64',
      'RAW(\'ElementName\'), TYPE',
      'RAW(\'ElementName\'), ROOT(\'RootName\')',
      'RAW(\'ElementName\'), BINARY BASE64, TYPE',
      'RAW(\'ElementName\'), BINARY BASE64, ROOT(\'RootName\')',
      'RAW(\'ElementName\'), TYPE, ROOT(\'RootName\')',
      'RAW(\'ElementName\'), BINARY BASE64, TYPE, ROOT(\'RootName\')',
      'RAW(\'ElementName\'), XMLDATA',
      'RAW(\'ElementName\'), XMLSCHEMA(\'TargetNameSpaceURI\')',
      'RAW(\'ElementName\'), XMLDATA, ELEMENTS XSINIL',
      'RAW(\'ElementName\'), XMLSCHEMA(\'TargetNameSpaceURI\'), ELEMENTS ABSENT',
      'RAW(\'ElementName\'), XMLDATA, ELEMENTS ABSENT',
      'RAW(\'ElementName\'), XMLSCHEMA(\'TargetNameSpaceURI\'), ELEMENTS XSINIL',
      'AUTO',
      'AUTO, BINARY BASE64',
      'AUTO, TYPE',
      'AUTO, ROOT(\'RootName\')',
      'AUTO, BINARY BASE64, TYPE',
      'AUTO, TYPE, ROOT(\'RootName\')',
      'AUTO, BINARY BASE64, TYPE, ROOT(\'RootName\')',
      'AUTO, XMLDATA',
      'AUTO, XMLSCHEMA(\'TargetNameSpaceURI\')',
      'AUTO, XMLDATA, ELEMENTS XSINIL',
      'AUTO, XMLSCHEMA(\'TargetNameSpaceURI\'), ELEMENTS ABSENT',
      'AUTO, XMLDATA, ELEMENTS ABSENT',
      'AUTO, XMLSCHEMA(\'TargetNameSpaceURI\'), ELEMENTS XSINIL',
      'EXPLICIT',
      'EXPLICIT, BINARY BASE64',
      'EXPLICIT, TYPE',
      'EXPLICIT, ROOT(\'RootName\')',
      'EXPLICIT, BINARY BASE64, TYPE',
      'EXPLICIT, TYPE, ROOT(\'RootName\')',
      'EXPLICIT, BINARY BASE64, TYPE, ROOT(\'RootName\')',
      'EXPLICIT, XMLDATA',
      'EXPLICIT, XMLDATA, BINARY BASE64',
      'EXPLICIT, XMLDATA, TYPE',
      'EXPLICIT, XMLDATA, ROOT(\'RootName\')',
      'EXPLICIT, XMLDATA, BINARY BASE64, TYPE',
      'EXPLICIT, XMLDATA, BINARY BASE64, TYPE, ROOT(\'RootName\')',
      'PATH(\'ElementName\')',
      'PATH(\'ElementName\'), BINARY BASE64',
      'PATH(\'ElementName\'), TYPE',
      'PATH(\'ElementName\'), ROOT(\'RootName\')',
      'PATH(\'ElementName\'), BINARY BASE64, TYPE',
      'PATH(\'ElementName\'), TYPE, ROOT(\'RootName\')',
      'PATH(\'ElementName\'), BINARY BASE64, TYPE, ROOT(\'RootName\')',
      'PATH(\'ElementName\'), ELEMENTS XSINIL',
      'PATH(\'ElementName\'), ELEMENTS ABSENT',
      'PATH(\'ElementName\'), BINARY BASE64, ELEMENTS XSINIL',
      'PATH(\'ElementName\'), TYPE, ELEMENTS ABSENT',
      'PATH(\'ElementName\'), ROOT(\'RootName\'), ELEMENTS XSINIL',
      'PATH(\'ElementName\'), BINARY BASE64, TYPE, ROOT(\'RootName\'), ELEMENTS ABSENT',
    ];

    for (const xmlOption of xmlPossibleOptions) {
      this.validateIdentity(`SELECT * FROM t FOR XML ${xmlOption}`);
    }

    this.validateIdentity(
      'SELECT * FROM t FOR XML PATH, BINARY BASE64, ELEMENTS XSINIL',
      `SELECT
  *
FROM t
FOR XML
  PATH,
  BINARY BASE64,
  ELEMENTS XSINIL`,
      { pretty: true },
    );
  }

  testTypes () {
    this.validateIdentity('CAST(x AS XML)');
    this.validateIdentity('CAST(x AS UNIQUEIDENTIFIER)');
    this.validateIdentity('CAST(x AS MONEY)');
    this.validateIdentity('CAST(x AS SMALLMONEY)');
    this.validateIdentity('CAST(x AS IMAGE)');
    this.validateIdentity('CAST(x AS SQL_VARIANT)');
    this.validateIdentity('CAST(x AS BIT)');
    this.validateAll('CAST(x AS DATETIME2(6))', {
      write: { hive: 'CAST(x AS TIMESTAMP)' },
    });
    this.validateAll('CAST(x AS ROWVERSION)', {
      read: { tsql: 'CAST(x AS TIMESTAMP)' },
      write: {
        tsql: 'CAST(x AS ROWVERSION)',
        hive: 'CAST(x AS BINARY)',
      },
    });
    for (const temporalType of [
      'SMALLDATETIME',
      'DATETIME',
      'DATETIME2',
    ]) {
      this.validateAll(`CAST(x AS ${temporalType})`, {
        read: { '': `CAST(x AS ${temporalType})` },
        write: {
          mysql: 'CAST(x AS DATETIME)',
          duckdb: 'CAST(x AS TIMESTAMP)',
          tsql: `CAST(x AS ${temporalType})`,
        },
      });
    }
  }

  testTypesInts () {
    this.validateAll('CAST(X AS INT)', {
      write: {
        hive: 'CAST(X AS INT)',
        spark2: 'CAST(X AS INT)',
        spark: 'CAST(X AS INT)',
        tsql: 'CAST(X AS INTEGER)',
      },
    });
    this.validateAll('CAST(X AS BIGINT)', {
      write: {
        hive: 'CAST(X AS BIGINT)',
        spark2: 'CAST(X AS BIGINT)',
        spark: 'CAST(X AS BIGINT)',
        tsql: 'CAST(X AS BIGINT)',
      },
    });
    this.validateAll('CAST(X AS SMALLINT)', {
      write: {
        hive: 'CAST(X AS SMALLINT)',
        spark2: 'CAST(X AS SMALLINT)',
        spark: 'CAST(X AS SMALLINT)',
        tsql: 'CAST(X AS SMALLINT)',
      },
    });
    this.validateAll('CAST(X AS TINYINT)', {
      read: { duckdb: 'CAST(X AS UTINYINT)' },
      write: {
        duckdb: 'CAST(X AS UTINYINT)',
        hive: 'CAST(X AS SMALLINT)',
        spark2: 'CAST(X AS SMALLINT)',
        spark: 'CAST(X AS SMALLINT)',
        tsql: 'CAST(X AS TINYINT)',
      },
    });
  }

  testTypesDecimals () {
    this.validateAll('CAST(x as FLOAT)', {
      write: {
        spark: 'CAST(x AS FLOAT)',
        tsql: 'CAST(x AS FLOAT)',
      },
    });
    this.validateAll('CAST(x as FLOAT(32))', {
      write: {
        tsql: 'CAST(x AS FLOAT(32))',
        hive: 'CAST(x AS FLOAT)',
      },
    });
    this.validateAll('CAST(x as FLOAT(64))', {
      write: {
        tsql: 'CAST(x AS FLOAT(64))',
        spark: 'CAST(x AS DOUBLE)',
      },
    });
    this.validateAll('CAST(x as FLOAT(6))', {
      write: {
        tsql: 'CAST(x AS FLOAT(6))',
        hive: 'CAST(x AS FLOAT)',
      },
    });
    this.validateAll('CAST(x as FLOAT(36))', {
      write: {
        tsql: 'CAST(x AS FLOAT(36))',
        hive: 'CAST(x AS DOUBLE)',
      },
    });
    this.validateAll('CAST(x as FLOAT(99))', {
      write: {
        tsql: 'CAST(x AS FLOAT(99))',
        hive: 'CAST(x AS DOUBLE)',
      },
    });
    this.validateAll('CAST(x as DOUBLE)', {
      write: {
        spark: 'CAST(x AS DOUBLE)',
        tsql: 'CAST(x AS FLOAT)',
      },
    });
    this.validateAll('CAST(x as DECIMAL(15, 4))', {
      write: {
        spark: 'CAST(x AS DECIMAL(15, 4))',
        tsql: 'CAST(x AS NUMERIC(15, 4))',
      },
    });
    this.validateAll('CAST(x as NUMERIC(13,3))', {
      write: {
        spark: 'CAST(x AS DECIMAL(13, 3))',
        tsql: 'CAST(x AS NUMERIC(13, 3))',
      },
    });
    this.validateAll('CAST(x as MONEY)', {
      write: {
        spark: 'CAST(x AS DECIMAL(15, 4))',
        tsql: 'CAST(x AS MONEY)',
      },
    });
    this.validateAll('CAST(x as SMALLMONEY)', {
      write: {
        spark: 'CAST(x AS DECIMAL(6, 4))',
        tsql: 'CAST(x AS SMALLMONEY)',
      },
    });
    this.validateAll('CAST(x as REAL)', {
      write: {
        spark: 'CAST(x AS FLOAT)',
        tsql: 'CAST(x AS FLOAT)',
      },
    });
  }

  testTypesString () {
    this.validateAll('CAST(x as CHAR(1))', {
      write: {
        spark: 'CAST(x AS CHAR(1))',
        tsql: 'CAST(x AS CHAR(1))',
      },
    });
    this.validateAll('CAST(x as VARCHAR(2))', {
      write: {
        spark: 'CAST(x AS VARCHAR(2))',
        tsql: 'CAST(x AS VARCHAR(2))',
      },
    });
    this.validateAll('CAST(x as NCHAR(1))', {
      write: {
        spark: 'CAST(x AS CHAR(1))',
        tsql: 'CAST(x AS NCHAR(1))',
      },
    });
    this.validateAll('CAST(x as NVARCHAR(2))', {
      write: {
        spark: 'CAST(x AS VARCHAR(2))',
        tsql: 'CAST(x AS NVARCHAR(2))',
      },
    });
    this.validateAll('CAST(x as UNIQUEIDENTIFIER)', {
      write: {
        spark: 'CAST(x AS STRING)',
        tsql: 'CAST(x AS UNIQUEIDENTIFIER)',
      },
    });
  }

  testTypesDate () {
    this.validateAll('CAST(x as DATE)', {
      write: {
        spark: 'CAST(x AS DATE)',
        tsql: 'CAST(x AS DATE)',
      },
    });
    this.validateAll('CAST(x as TIME(4))', {
      write: {
        spark: 'CAST(x AS TIMESTAMP)',
        tsql: 'CAST(x AS TIME(4))',
      },
    });
    this.validateAll('CAST(x as DATETIME2)', {
      write: {
        spark: 'CAST(x AS TIMESTAMP)',
        tsql: 'CAST(x AS DATETIME2)',
      },
    });
    this.validateAll('CAST(x as DATETIMEOFFSET)', {
      write: {
        spark: 'CAST(x AS TIMESTAMP)',
        tsql: 'CAST(x AS DATETIMEOFFSET)',
      },
    });
    this.validateAll('CREATE TABLE t (col1 DATETIME2(2))', {
      read: { snowflake: 'CREATE TABLE t (col1 TIMESTAMP_NTZ(2))' },
      write: { tsql: 'CREATE TABLE t (col1 DATETIME2(2))' },
    });
  }

  testTypesBin () {
    this.validateAll('CAST(x as BIT)', {
      write: {
        spark: 'CAST(x AS BOOLEAN)',
        tsql: 'CAST(x AS BIT)',
      },
    });
    this.validateAll('CAST(x as VARBINARY)', {
      write: {
        spark: 'CAST(x AS BINARY)',
        tsql: 'CAST(x AS VARBINARY)',
      },
    });
    this.validateAll('CAST(x AS BOOLEAN)', {
      write: { tsql: 'CAST(x AS BIT)' },
    });
    this.validateAll('a = TRUE', { write: { tsql: 'a = 1' } });
    this.validateAll('a != FALSE', { write: { tsql: 'a <> 0' } });
    this.validateAll('a IS TRUE', { write: { tsql: 'a = 1' } });
    this.validateAll('a IS NOT FALSE', { write: { tsql: 'NOT a = 0' } });
    this.validateAll('CASE WHEN a IN (TRUE) THEN \'y\' ELSE \'n\' END', {
      write: { tsql: 'CASE WHEN a IN (1) THEN \'y\' ELSE \'n\' END' },
    });
    this.validateAll('CASE WHEN a NOT IN (FALSE) THEN \'y\' ELSE \'n\' END', {
      write: { tsql: 'CASE WHEN NOT a IN (0) THEN \'y\' ELSE \'n\' END' },
    });
    this.validateAll('SELECT TRUE, FALSE', { write: { tsql: 'SELECT 1, 0' } });
    this.validateAll('SELECT TRUE AS a, FALSE AS b', { write: { tsql: 'SELECT 1 AS a, 0 AS b' } });
    this.validateAll('SELECT 1 FROM a WHERE TRUE', {
      write: { tsql: 'SELECT 1 FROM a WHERE (1 = 1)' },
    });
    this.validateAll('CASE WHEN TRUE THEN \'y\' WHEN FALSE THEN \'n\' ELSE NULL END', {
      write: { tsql: 'CASE WHEN (1 = 1) THEN \'y\' WHEN (1 = 0) THEN \'n\' ELSE NULL END' },
    });
  }

  testDdl () {
    for (const colstore of ['NONCLUSTERED COLUMNSTORE', 'CLUSTERED COLUMNSTORE']) {
      this.validateIdentity(`CREATE ${colstore} INDEX index_name ON foo.bar`);
    }
    for (const viewAttr of [
      'ENCRYPTION',
      'SCHEMABINDING',
      'VIEW_METADATA',
    ]) {
      this.validateIdentity(`CREATE VIEW a.b WITH ${viewAttr} AS SELECT * FROM x`);
    }
    this.validateIdentity('ALTER TABLE dbo.DocExe DROP CONSTRAINT FK_Column_B').assertIs(AlterExpr)
      .args['actions']?.[0].assertIs(DropExpr);

    for (const clusteredKeyword of ['CLUSTERED', 'NONCLUSTERED']) {
      this.validateIdentity(
        `CREATE TABLE "dbo"."benchmark" ("name" CHAR(7) NOT NULL, "internal_id" VARCHAR(10) NOT NULL, UNIQUE ${clusteredKeyword} ("internal_id" ASC))`,
        `CREATE TABLE [dbo].[benchmark] ([name] CHAR(7) NOT NULL, [internal_id] VARCHAR(10) NOT NULL, UNIQUE ${clusteredKeyword} ([internal_id] ASC))`,
      );
    }

    this.validateIdentity('CREATE SCHEMA testSchema');
    this.validateIdentity('CREATE VIEW t AS WITH cte AS (SELECT 1 AS c) SELECT c FROM cte');
    this.validateIdentity('ALTER TABLE tbl SET (SYSTEM_VERSIONING=OFF)');
    this.validateIdentity('ALTER TABLE tbl SET (FILESTREAM_ON = \'test\')');
    this.validateIdentity('ALTER TABLE tbl SET (DATA_DELETION=ON)');
    this.validateIdentity('ALTER TABLE tbl SET (DATA_DELETION=OFF)');
    this.validateIdentity(
      'ALTER TABLE t1 WITH CHECK ADD CONSTRAINT ctr FOREIGN KEY (c1) REFERENCES t2 (c2)',
    );
    this.validateIdentity(
      'ALTER TABLE tbl SET (SYSTEM_VERSIONING=ON(HISTORY_TABLE=db.tbl, DATA_CONSISTENCY_CHECK=OFF, HISTORY_RETENTION_PERIOD=5 DAYS))',
    );
    this.validateIdentity(
      'ALTER TABLE tbl SET (SYSTEM_VERSIONING=ON(HISTORY_TABLE=db.tbl, HISTORY_RETENTION_PERIOD=INFINITE))',
    );
    this.validateIdentity(
      'ALTER TABLE tbl SET (DATA_DELETION=ON(FILTER_COLUMN=col, RETENTION_PERIOD=5 MONTHS))',
    );
    this.validateIdentity('ALTER VIEW v AS SELECT a, b, c, d FROM foo');
    this.validateIdentity('ALTER VIEW v AS SELECT * FROM foo WHERE c > 100');
    this.validateIdentity(
      'ALTER VIEW v WITH SCHEMABINDING AS SELECT * FROM foo WHERE c > 100',
      undefined,
      { checkCommandWarning: true },
    );
    this.validateIdentity(
      'ALTER VIEW v WITH ENCRYPTION AS SELECT * FROM foo WHERE c > 100',
      undefined,
      { checkCommandWarning: true },
    );
    this.validateIdentity(
      'ALTER VIEW v WITH VIEW_METADATA AS SELECT * FROM foo WHERE c > 100',
      undefined,
      { checkCommandWarning: true },
    );
    this.validateIdentity(
      'CREATE COLUMNSTORE INDEX index_name ON foo.bar',
      'CREATE NONCLUSTERED COLUMNSTORE INDEX index_name ON foo.bar',
    );
    this.validateIdentity(
      'CREATE PROCEDURE foo AS BEGIN DELETE FROM bla WHERE foo < CURRENT_TIMESTAMP - 7 END',
      'CREATE PROCEDURE foo AS BEGIN DELETE FROM bla WHERE foo < GETDATE() - 7 END',
    );
    this.validateAll('CREATE TABLE [#temptest] (name INTEGER)', {
      read: {
        duckdb: 'CREATE TEMPORARY TABLE \'temptest\' (name INTEGER)',
        tsql: 'CREATE TABLE [#temptest] (name INTEGER)',
      },
    });
    this.validateAll('CREATE TABLE tbl (id INTEGER IDENTITY PRIMARY KEY)', {
      read: {
        mysql: 'CREATE TABLE tbl (id INT AUTO_INCREMENT PRIMARY KEY)',
        tsql: 'CREATE TABLE tbl (id INTEGER IDENTITY PRIMARY KEY)',
      },
    });
    this.validateAll('CREATE TABLE tbl (id INTEGER NOT NULL IDENTITY(10, 1) PRIMARY KEY)', {
      read: {
        postgres: 'CREATE TABLE tbl (id INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 10) PRIMARY KEY)',
        tsql: 'CREATE TABLE tbl (id INTEGER NOT NULL IDENTITY(10, 1) PRIMARY KEY)',
      },
      write: {
        databricks: 'CREATE TABLE tbl (id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 10 INCREMENT BY 1) PRIMARY KEY)',
        postgres: 'CREATE TABLE tbl (id INT NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 1) PRIMARY KEY)',
      },
    });
    this.validateAll('CREATE TABLE x (a UNIQUEIDENTIFIER, b VARBINARY)', {
      write: {
        duckdb: 'CREATE TABLE x (a UUID, b BLOB)',
        presto: 'CREATE TABLE x (a UUID, b VARBINARY)',
        spark: 'CREATE TABLE x (a STRING, b BINARY)',
        postgres: 'CREATE TABLE x (a UUID, b BYTEA)',
      },
    });
    this.validateAll('SELECT * INTO foo.bar.baz FROM (SELECT * FROM a.b.c) AS temp', {
      read: {
        '': 'CREATE TABLE foo.bar.baz AS SELECT * FROM a.b.c',
        'duckdb': 'CREATE TABLE foo.bar.baz AS (SELECT * FROM a.b.c)',
      },
    });
    this.validateAll(
      'IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = object_id(\'db.tbl\') AND name = \'idx\') EXEC(\'CREATE INDEX idx ON db.tbl\')',
      { read: { '': 'CREATE INDEX IF NOT EXISTS idx ON db.tbl' } },
    );
    this.validateAll(
      'IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = \'foo\') EXEC(\'CREATE SCHEMA foo\')',
      { read: { '': 'CREATE SCHEMA IF NOT EXISTS foo' } },
    );
    this.validateAll(
      'IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = \'baz\' AND TABLE_SCHEMA = \'bar\' AND TABLE_CATALOG = \'foo\') EXEC(\'CREATE TABLE foo.bar.baz (a INTEGER)\')',
      { read: { '': 'CREATE TABLE IF NOT EXISTS foo.bar.baz (a INTEGER)' } },
    );
    this.validateAll(
      'IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = \'baz\' AND TABLE_SCHEMA = \'bar\' AND TABLE_CATALOG = \'foo\') EXEC(\'SELECT * INTO foo.bar.baz FROM (SELECT \'\'2020\'\' AS z FROM a.b.c) AS temp\')',
      { read: { '': 'CREATE TABLE IF NOT EXISTS foo.bar.baz AS SELECT \'2020\' AS z FROM a.b.c' } },
    );
    this.validateAll('CREATE OR ALTER VIEW a.b AS SELECT 1', {
      read: { '': 'CREATE OR REPLACE VIEW a.b AS SELECT 1' },
      write: { tsql: 'CREATE OR ALTER VIEW a.b AS SELECT 1' },
    });
    this.validateAll('ALTER TABLE a ADD b INTEGER, c INTEGER', {
      read: { '': 'ALTER TABLE a ADD COLUMN b INT, ADD COLUMN c INT' },
      write: {
        '': 'ALTER TABLE a ADD COLUMN b INT, ADD COLUMN c INT',
        'tsql': 'ALTER TABLE a ADD b INTEGER, c INTEGER',
      },
    });
    this.validateAll('ALTER TABLE a ALTER COLUMN b INTEGER', {
      read: { '': 'ALTER TABLE a ALTER COLUMN b INT' },
      write: {
        '': 'ALTER TABLE a ALTER COLUMN b SET DATA TYPE INT',
        'tsql': 'ALTER TABLE a ALTER COLUMN b INTEGER',
      },
    });
    this.validateAll('CREATE TABLE #mytemp (a INTEGER, b CHAR(2), c TIME(4), d FLOAT(24))', {
      write: {
        spark: 'CREATE TEMPORARY TABLE mytemp (a INT, b CHAR(2), c TIMESTAMP, d FLOAT) USING PARQUET',
        tsql: 'CREATE TABLE #mytemp (a INTEGER, b CHAR(2), c TIME(4), d FLOAT(24))',
      },
    });

    const constraint = this.validateIdentity(
      'ALTER TABLE tbl ADD CONSTRAINT cnstr PRIMARY KEY CLUSTERED (ID), CONSTRAINT cnstr2 UNIQUE CLUSTERED (ID)',
    ).find(AddConstraintExpr);
    expect(constraint).toBeTruthy();
    expect([...constraint!.findAll(ConstraintExpr)].length).toBe(2);
  }

  testTransaction () {
    this.validateIdentity('BEGIN TRANSACTION');
    this.validateAll('BEGIN TRAN', { write: { tsql: 'BEGIN TRANSACTION' } });
    this.validateIdentity('BEGIN TRANSACTION transaction_name');
    this.validateIdentity('BEGIN TRANSACTION @tran_name_variable');
    this.validateIdentity('BEGIN TRANSACTION transaction_name WITH MARK \'description\'');
  }

  testCommit () {
    this.validateAll('COMMIT', { write: { tsql: 'COMMIT TRANSACTION' } });
    this.validateAll('COMMIT TRAN', { write: { tsql: 'COMMIT TRANSACTION' } });
    this.validateIdentity('COMMIT TRANSACTION');
    this.validateIdentity('COMMIT TRANSACTION transaction_name');
    this.validateIdentity('COMMIT TRANSACTION @tran_name_variable');
    this.validateIdentity('COMMIT TRANSACTION @tran_name_variable WITH (DELAYED_DURABILITY = ON)');
    this.validateIdentity('COMMIT TRANSACTION transaction_name WITH (DELAYED_DURABILITY = OFF)');
  }

  testRollback () {
    this.validateAll('ROLLBACK', { write: { tsql: 'ROLLBACK TRANSACTION' } });
    this.validateAll('ROLLBACK TRAN', { write: { tsql: 'ROLLBACK TRANSACTION' } });
    this.validateIdentity('ROLLBACK TRANSACTION');
    this.validateIdentity('ROLLBACK TRANSACTION transaction_name');
    this.validateIdentity('ROLLBACK TRANSACTION @tran_name_variable');
  }

  testUdf () {
    this.validateIdentity(
      'DECLARE @DWH_DateCreated AS DATETIME2 = CONVERT(DATETIME2, GETDATE(), 104)',
    );
    this.validateIdentity(
      'CREATE PROCEDURE foo @a INTEGER, @b INTEGER AS SELECT @a = SUM(bla) FROM baz AS bar',
    );
    this.validateIdentity(
      'CREATE PROC foo @ID INTEGER, @AGE INTEGER AS SELECT DB_NAME(@ID) AS ThatDB',
    );
    this.validateIdentity('CREATE PROC foo AS SELECT BAR() AS baz');
    this.validateIdentity('CREATE PROCEDURE foo AS SELECT BAR() AS baz');
    this.validateIdentity('CREATE PROCEDURE foo WITH ENCRYPTION AS SELECT 1');
    this.validateIdentity('CREATE PROCEDURE foo WITH RECOMPILE AS SELECT 1');
    this.validateIdentity('CREATE PROCEDURE foo WITH SCHEMABINDING AS SELECT 1');
    this.validateIdentity('CREATE PROCEDURE foo WITH NATIVE_COMPILATION AS SELECT 1');
    this.validateIdentity('CREATE PROCEDURE foo WITH EXECUTE AS OWNER AS SELECT 1');
    this.validateIdentity('CREATE PROCEDURE foo WITH EXECUTE AS \'username\' AS SELECT 1');
    this.validateIdentity(
      'CREATE PROCEDURE foo WITH EXECUTE AS OWNER, SCHEMABINDING, NATIVE_COMPILATION AS SELECT 1',
    );
    this.validateIdentity('CREATE FUNCTION foo(@bar INTEGER) RETURNS TABLE AS RETURN SELECT 1');
    this.validateIdentity('CREATE FUNCTION dbo.ISOweek(@DATE DATETIME2) RETURNS INTEGER');
    this.validateIdentity(
      'CREATE FUNCTION foo(@bar INTEGER) RETURNS @foo TABLE (x INTEGER, y NUMERIC) AS RETURN SELECT 1',
    );
    this.validateIdentity(
      'CREATE FUNCTION foo() RETURNS @contacts TABLE (first_name VARCHAR(50), phone VARCHAR(25)) AS SELECT @fname, @phone',
    );

    this.validateAll(
      `
            CREATE FUNCTION udfProductInYear (
                @model_year INT
            )
            RETURNS TABLE
            AS
            RETURN
                SELECT
                    product_name,
                    model_year,
                    list_price
                FROM
                    production.products
                WHERE
                    model_year = @model_year
            `,
      {
        write: {
          tsql: `CREATE FUNCTION udfProductInYear(
    @model_year INTEGER
)
RETURNS TABLE AS
RETURN SELECT
  product_name,
  model_year,
  list_price
FROM production.products
WHERE
  model_year = @model_year`,
        },
        pretty: true,
      },
    );
  }

  testProcedureKeywords () {
    this.validateIdentity('BEGIN');
    this.validateIdentity('END');
    this.validateIdentity('SET XACT_ABORT ON');
  }

  testFullproc () {
    const sql = `
            CREATE procedure [TRANSF].[SP_Merge_Sales_Real]
                @Loadid INTEGER
               ,@NumberOfRows INTEGER
            WITH EXECUTE AS OWNER, SCHEMABINDING, NATIVE_COMPILATION
            AS
            BEGIN
                SET XACT_ABORT ON;

                DECLARE @DWH_DateCreated AS DATETIME = CONVERT(DATETIME, getdate(), 104);
                DECLARE @DWH_DateModified DATETIME2 = CONVERT(DATETIME2, GETDATE(), 104);
                DECLARE @DWH_IdUserCreated INTEGER = SUSER_ID (CURRENT_USER());
                DECLARE @DWH_IdUserModified INTEGER = SUSER_ID (SYSTEM_USER);

                DECLARE @SalesAmountBefore float;
                SELECT @SalesAmountBefore=SUM(SalesAmount) FROM TRANSF.[Pre_Merge_Sales_Real] S;
            END
        `;

    const expectedSqls = [
      'CREATE PROCEDURE [TRANSF].[SP_Merge_Sales_Real] @Loadid INTEGER, @NumberOfRows INTEGER WITH EXECUTE AS OWNER, SCHEMABINDING, NATIVE_COMPILATION AS BEGIN SET XACT_ABORT ON',
      'DECLARE @DWH_DateCreated AS DATETIME = CONVERT(DATETIME, GETDATE(), 104)',
      'DECLARE @DWH_DateModified AS DATETIME2 = CONVERT(DATETIME2, GETDATE(), 104)',
      'DECLARE @DWH_IdUserCreated AS INTEGER = SUSER_ID(CURRENT_USER())',
      'DECLARE @DWH_IdUserModified AS INTEGER = SUSER_ID(CURRENT_USER())',
      'DECLARE @SalesAmountBefore AS FLOAT',
      'SELECT @SalesAmountBefore = SUM(SalesAmount) FROM TRANSF.[Pre_Merge_Sales_Real] AS S',
      'END',
    ];

    const exprs = parse(sql, { read: 'tsql' });
    for (let i = 0; i < exprs.length; i++) {
      expect(exprs[i]?.sql({ dialect: 'tsql' })).toBe(expectedSqls[i]);
    }

    const sql2 = `
            CREATE PROC [dbo].[transform_proc] AS

            DECLARE @CurrentDate VARCHAR(20);
            SET @CurrentDate = CONVERT(VARCHAR(20), GETDATE(), 120);

            CREATE TABLE [target_schema].[target_table]
            (a INTEGER)
            WITH (DISTRIBUTION = REPLICATE, HEAP);
        `;

    const expectedSqls2 = [
      'CREATE PROC [dbo].[transform_proc] AS DECLARE @CurrentDate AS VARCHAR(20)',
      'SET @CurrentDate = CONVERT(VARCHAR(20), GETDATE(), 120)',
      'CREATE TABLE [target_schema].[target_table] (a INTEGER) WITH (DISTRIBUTION=REPLICATE, HEAP)',
    ];

    const exprs2 = parse(sql2, { read: 'tsql' });
    for (let i = 0; i < exprs2.length; i++) {
      expect(exprs2[i]?.sql({ dialect: 'tsql' })).toBe(expectedSqls2[i]);
    }
  }

  testCharindex () {
    this.validateIdentity(
      'SELECT CAST(SUBSTRING(\'ABCD~1234\', CHARINDEX(\'~\', \'ABCD~1234\') + 1, LEN(\'ABCD~1234\')) AS BIGINT)',
    );
    this.validateAll('CHARINDEX(x, y, 9)', {
      read: { spark: 'LOCATE(x, y, 9)' },
      write: {
        spark: 'LOCATE(x, y, 9)',
        tsql: 'CHARINDEX(x, y, 9)',
      },
    });
    this.validateAll('CHARINDEX(x, y)', {
      read: { spark: 'LOCATE(x, y)' },
      write: {
        spark: 'LOCATE(x, y)',
        tsql: 'CHARINDEX(x, y)',
      },
    });
    this.validateAll('CHARINDEX(\'sub\', \'testsubstring\', 3)', {
      read: { spark: 'LOCATE(\'sub\', \'testsubstring\', 3)' },
      write: {
        spark: 'LOCATE(\'sub\', \'testsubstring\', 3)',
        tsql: 'CHARINDEX(\'sub\', \'testsubstring\', 3)',
      },
    });
    this.validateAll('CHARINDEX(\'sub\', \'testsubstring\')', {
      read: { spark: 'LOCATE(\'sub\', \'testsubstring\')' },
      write: {
        spark: 'LOCATE(\'sub\', \'testsubstring\')',
        tsql: 'CHARINDEX(\'sub\', \'testsubstring\')',
      },
    });
  }

  testLen () {
    this.validateAll('LEN(x)', {
      read: { '': 'LENGTH(x)' },
      write: { spark: 'LENGTH(CAST(x AS STRING))' },
    });
    this.validateAll('RIGHT(x, 1)', {
      read: { '': 'RIGHT(CAST(x AS STRING), 1)' },
      write: { spark: 'RIGHT(CAST(x AS STRING), 1)' },
    });
    this.validateAll('LEFT(x, 1)', {
      read: { '': 'LEFT(CAST(x AS STRING), 1)' },
      write: { spark: 'LEFT(CAST(x AS STRING), 1)' },
    });
    this.validateAll('LEN(1)', {
      write: {
        tsql: 'LEN(1)',
        spark: 'LENGTH(CAST(1 AS STRING))',
      },
    });
    this.validateAll('LEN(\'x\')', {
      write: {
        tsql: 'LEN(\'x\')',
        spark: 'LENGTH(\'x\')',
      },
    });
  }

  testReplicate () {
    this.validateAll('REPLICATE(\'x\', 2)', {
      write: {
        spark: 'REPEAT(\'x\', 2)',
        tsql: 'REPLICATE(\'x\', 2)',
      },
    });
  }

  testIsnull () {
    this.validateIdentity('ISNULL(x, y)');
    this.validateAll('ISNULL(x, y)', { write: { spark: 'COALESCE(x, y)' } });
  }

  testJson () {
    this.validateIdentity(
      'JSON_QUERY(REPLACE(REPLACE(x , \'\'\'\', \'"\'), \'""\', \'"\'))',
      'ISNULL(JSON_QUERY(REPLACE(REPLACE(x, \'\'\'\', \'"\'), \'""\', \'"\'), \'$\'), JSON_VALUE(REPLACE(REPLACE(x, \'\'\'\', \'"\'), \'""\', \'"\'), \'$\'))',
    );
    this.validateAll('JSON_QUERY(r.JSON, \'$.Attr_INT\')', {
      write: {
        spark: 'GET_JSON_OBJECT(r.JSON, \'$.Attr_INT\')',
        tsql: 'ISNULL(JSON_QUERY(r.JSON, \'$.Attr_INT\'), JSON_VALUE(r.JSON, \'$.Attr_INT\'))',
      },
    });
    this.validateAll('JSON_VALUE(r.JSON, \'$.Attr_INT\')', {
      write: {
        spark: 'GET_JSON_OBJECT(r.JSON, \'$.Attr_INT\')',
        tsql: 'ISNULL(JSON_QUERY(r.JSON, \'$.Attr_INT\'), JSON_VALUE(r.JSON, \'$.Attr_INT\'))',
      },
    });
  }

  testDatefromparts () {
    this.validateAll('SELECT DATEFROMPARTS(\'2020\', 10, 01)', {
      write: {
        spark: 'SELECT MAKE_DATE(\'2020\', 10, 01)',
        tsql: 'SELECT DATEFROMPARTS(\'2020\', 10, 01)',
      },
    });
  }

  testDatename () {
    this.validateAll('SELECT DATENAME(mm, \'1970-01-01\')', {
      write: {
        spark: 'SELECT DATE_FORMAT(CAST(\'1970-01-01\' AS TIMESTAMP), \'MMMM\')',
        tsql: 'SELECT FORMAT(CAST(\'1970-01-01\' AS DATETIME2), \'MMMM\')',
      },
    });
    this.validateAll('SELECT DATENAME(dw, \'1970-01-01\')', {
      write: {
        spark: 'SELECT DATE_FORMAT(CAST(\'1970-01-01\' AS TIMESTAMP), \'EEEE\')',
        tsql: 'SELECT FORMAT(CAST(\'1970-01-01\' AS DATETIME2), \'dddd\')',
      },
    });
  }

  testDatepart () {
    const datepartFormats: [string[], string][] = [
      [
        [
          'QUARTER',
          'qq',
          'q',
        ],
        'QUARTER',
      ],
      [
        [
          'YEAR',
          'yy',
          'yyyy',
        ],
        'YEAR',
      ],
      [['HOUR', 'hh'], 'HOUR'],
      [
        [
          'MINUTE',
          'mi',
          'n',
        ],
        'MINUTE',
      ],
      [
        [
          'SECOND',
          'ss',
          's',
        ],
        'SECOND',
      ],
      [['MILLISECOND', 'ms'], 'MILLISECOND'],
      [['MICROSECOND', 'mcs'], 'MICROSECOND'],
      [['NANOSECOND', 'ns'], 'NANOSECOND'],
      [['WEEKDAY', 'dw'], 'WEEKDAY'],
      [['TZOFFSET', 'tz'], 'TZOFFSET'],
      [
        [
          'MONTH',
          'mm',
          'm',
        ],
        'MONTH',
      ],
      [
        [
          'DAYOFYEAR',
          'dy',
          'y',
        ],
        'DAYOFYEAR',
      ],
      [
        [
          'DAY',
          'dd',
          'd',
        ],
        'DAY',
      ],
    ];

    for (const [formats, canonical] of datepartFormats) {
      for (const fmt of formats) {
        this.validateIdentity(`DATEPART(${fmt}, x)`, `DATEPART(${canonical}, x)`);
      }
    }

    const selectDatepartFormats: [string[], string][] = [
      [
        [
          'WEEK',
          'WW',
          'WK',
        ],
        'WEEK',
      ],
      [
        [
          'ISOWK',
          'ISOWW',
          'ISO_WEEK',
        ],
        'ISO_WEEK',
      ],
    ];

    for (const [formats, canonical] of selectDatepartFormats) {
      for (const fmt of formats) {
        this.validateIdentity(
          `SELECT DATEPART(${fmt}, '2024-11-21')`,
          `SELECT DATEPART(${canonical}, '2024-11-21')`,
        );
      }
    }

    this.validateAll('SELECT DATEPART(month,\'1970-01-01\')', {
      write: {
        spark: 'SELECT EXTRACT(month FROM \'1970-01-01\')',
        tsql: 'SELECT DATEPART(month, \'1970-01-01\')',
      },
    });
    this.validateAll('SELECT DATEPART(YEAR, CAST(\'2017-01-01\' AS DATE))', {
      read: { postgres: 'SELECT DATE_PART(\'YEAR\', \'2017-01-01\'::DATE)' },
      write: {
        postgres: 'SELECT EXTRACT(YEAR FROM CAST(\'2017-01-01\' AS DATE))',
        spark: 'SELECT EXTRACT(YEAR FROM CAST(\'2017-01-01\' AS DATE))',
        tsql: 'SELECT DATEPART(YEAR, CAST(\'2017-01-01\' AS DATE))',
      },
    });
    this.validateAll('SELECT DATEPART(month, CAST(\'2017-03-01\' AS DATE))', {
      read: { postgres: 'SELECT DATE_PART(\'month\', \'2017-03-01\'::DATE)' },
      write: {
        postgres: 'SELECT EXTRACT(month FROM CAST(\'2017-03-01\' AS DATE))',
        spark: 'SELECT EXTRACT(month FROM CAST(\'2017-03-01\' AS DATE))',
        tsql: 'SELECT DATEPART(month, CAST(\'2017-03-01\' AS DATE))',
      },
    });
    this.validateAll('SELECT DATEPART(day, CAST(\'2017-01-02\' AS DATE))', {
      read: { postgres: 'SELECT DATE_PART(\'day\', \'2017-01-02\'::DATE)' },
      write: {
        postgres: 'SELECT EXTRACT(day FROM CAST(\'2017-01-02\' AS DATE))',
        spark: 'SELECT EXTRACT(day FROM CAST(\'2017-01-02\' AS DATE))',
        tsql: 'SELECT DATEPART(day, CAST(\'2017-01-02\' AS DATE))',
      },
    });
    this.validateIdentity('SELECT DATEPART("dd", x)', 'SELECT DATEPART(DAY, x)');
  }

  testConvert () {
    this.validateAll('CONVERT(NVARCHAR(200), x)', {
      write: {
        spark: 'CAST(x AS VARCHAR(200))',
        tsql: 'CONVERT(NVARCHAR(200), x)',
      },
    });
    this.validateAll('CONVERT(NVARCHAR, x)', {
      write: {
        spark: 'CAST(x AS VARCHAR(30))',
        tsql: 'CONVERT(NVARCHAR, x)',
      },
    });
    this.validateAll('CONVERT(NVARCHAR(MAX), x)', {
      write: {
        spark: 'CAST(x AS STRING)',
        tsql: 'CONVERT(NVARCHAR(MAX), x)',
      },
    });
    this.validateAll('CONVERT(VARCHAR(200), x)', {
      write: {
        spark: 'CAST(x AS VARCHAR(200))',
        tsql: 'CONVERT(VARCHAR(200), x)',
      },
    });
    this.validateAll('CONVERT(VARCHAR, x)', {
      write: {
        spark: 'CAST(x AS VARCHAR(30))',
        tsql: 'CONVERT(VARCHAR, x)',
      },
    });
    this.validateAll('CONVERT(VARCHAR(MAX), x)', {
      write: {
        spark: 'CAST(x AS STRING)',
        tsql: 'CONVERT(VARCHAR(MAX), x)',
      },
    });
    this.validateAll('CONVERT(CHAR(40), x)', {
      write: {
        spark: 'CAST(x AS CHAR(40))',
        tsql: 'CONVERT(CHAR(40), x)',
      },
    });
    this.validateAll('CONVERT(CHAR, x)', {
      write: {
        spark: 'CAST(x AS CHAR(30))',
        tsql: 'CONVERT(CHAR, x)',
      },
    });
    this.validateAll('CONVERT(NCHAR(40), x)', {
      write: {
        spark: 'CAST(x AS CHAR(40))',
        tsql: 'CONVERT(NCHAR(40), x)',
      },
    });
    this.validateAll('CONVERT(NCHAR, x)', {
      write: {
        spark: 'CAST(x AS CHAR(30))',
        tsql: 'CONVERT(NCHAR, x)',
      },
    });
    this.validateAll('CONVERT(VARCHAR, x, 121)', {
      write: {
        spark: 'CAST(DATE_FORMAT(x, \'yyyy-MM-dd HH:mm:ss.SSSSSS\') AS VARCHAR(30))',
        tsql: 'CONVERT(VARCHAR, x, 121)',
      },
    });
    this.validateAll('CONVERT(VARCHAR(40), x, 121)', {
      write: {
        spark: 'CAST(DATE_FORMAT(x, \'yyyy-MM-dd HH:mm:ss.SSSSSS\') AS VARCHAR(40))',
        tsql: 'CONVERT(VARCHAR(40), x, 121)',
      },
    });
    this.validateAll('CONVERT(VARCHAR(MAX), x, 121)', {
      write: {
        spark: 'CAST(DATE_FORMAT(x, \'yyyy-MM-dd HH:mm:ss.SSSSSS\') AS STRING)',
        tsql: 'CONVERT(VARCHAR(MAX), x, 121)',
      },
    });
    this.validateAll('CONVERT(NVARCHAR, x, 121)', {
      write: {
        spark: 'CAST(DATE_FORMAT(x, \'yyyy-MM-dd HH:mm:ss.SSSSSS\') AS VARCHAR(30))',
        tsql: 'CONVERT(NVARCHAR, x, 121)',
      },
    });
    this.validateAll('CONVERT(NVARCHAR(40), x, 121)', {
      write: {
        spark: 'CAST(DATE_FORMAT(x, \'yyyy-MM-dd HH:mm:ss.SSSSSS\') AS VARCHAR(40))',
        tsql: 'CONVERT(NVARCHAR(40), x, 121)',
      },
    });
    this.validateAll('CONVERT(NVARCHAR(MAX), x, 121)', {
      write: {
        spark: 'CAST(DATE_FORMAT(x, \'yyyy-MM-dd HH:mm:ss.SSSSSS\') AS STRING)',
        tsql: 'CONVERT(NVARCHAR(MAX), x, 121)',
      },
    });
    this.validateAll('CONVERT(DATE, x, 121)', {
      write: {
        spark: 'TO_DATE(x, \'yyyy-MM-dd HH:mm:ss.SSSSSS\')',
        tsql: 'CONVERT(DATE, x, 121)',
      },
    });
    this.validateAll('CONVERT(DATETIME, x, 121)', {
      write: {
        spark: 'TO_TIMESTAMP(x, \'yyyy-MM-dd HH:mm:ss.SSSSSS\')',
        tsql: 'CONVERT(DATETIME, x, 121)',
      },
    });
    this.validateAll('CONVERT(DATETIME2, x, 121)', {
      write: {
        spark: 'TO_TIMESTAMP(x, \'yyyy-MM-dd HH:mm:ss.SSSSSS\')',
        tsql: 'CONVERT(DATETIME2, x, 121)',
      },
    });
    this.validateAll('CONVERT(INT, x)', {
      write: {
        spark: 'CAST(x AS INT)',
        tsql: 'CONVERT(INTEGER, x)',
      },
    });
    this.validateAll('CONVERT(INT, x, 121)', {
      write: {
        spark: 'CAST(x AS INT)',
        tsql: 'CONVERT(INTEGER, x, 121)',
      },
    });
    this.validateAll('TRY_CONVERT(NVARCHAR, x, 121)', {
      write: {
        spark: 'TRY_CAST(DATE_FORMAT(x, \'yyyy-MM-dd HH:mm:ss.SSSSSS\') AS VARCHAR(30))',
        tsql: 'TRY_CONVERT(NVARCHAR, x, 121)',
      },
    });
    this.validateAll('TRY_CONVERT(INT, x)', {
      write: {
        spark: 'TRY_CAST(x AS INT)',
        tsql: 'TRY_CONVERT(INTEGER, x)',
      },
    });
    this.validateAll('TRY_CAST(x AS INT)', {
      write: {
        spark: 'TRY_CAST(x AS INT)',
        tsql: 'TRY_CAST(x AS INTEGER)',
      },
    });
    this.validateAll('SELECT CONVERT(VARCHAR(10), testdb.dbo.test.x, 120) y FROM testdb.dbo.test', {
      write: {
        mysql: 'SELECT CAST(DATE_FORMAT(testdb.dbo.test.x, \'%Y-%m-%d %T\') AS CHAR(10)) AS y FROM testdb.dbo.test',
        spark: 'SELECT CAST(DATE_FORMAT(testdb.dbo.test.x, \'yyyy-MM-dd HH:mm:ss\') AS VARCHAR(10)) AS y FROM testdb.dbo.test',
        tsql: 'SELECT CONVERT(VARCHAR(10), testdb.dbo.test.x, 120) AS y FROM testdb.dbo.test',
      },
    });
    this.validateAll('SELECT CONVERT(VARCHAR(10), y.x) z FROM testdb.dbo.test y', {
      write: {
        mysql: 'SELECT CAST(y.x AS CHAR(10)) AS z FROM testdb.dbo.test AS y',
        spark: 'SELECT CAST(y.x AS VARCHAR(10)) AS z FROM testdb.dbo.test AS y',
        tsql: 'SELECT CONVERT(VARCHAR(10), y.x) AS z FROM testdb.dbo.test AS y',
      },
    });
    this.validateAll('SELECT CAST((SELECT x FROM y) AS VARCHAR) AS test', {
      write: {
        spark: 'SELECT CAST((SELECT x FROM y) AS STRING) AS test',
        tsql: 'SELECT CAST((SELECT x FROM y) AS VARCHAR) AS test',
      },
    });
  }

  testAddDate () {
    this.validateIdentity('SELECT DATEADD(YEAR, 1, \'2017/08/25\')');
    this.validateAll('DATEADD(year, 50, \'2006-07-31\')', {
      write: { bigquery: 'DATE_ADD(\'2006-07-31\', INTERVAL 50 YEAR)' },
    });
    this.validateAll('SELECT DATEADD(year, 1, \'2017/08/25\')', {
      write: { spark: 'SELECT ADD_MONTHS(\'2017/08/25\', 12)' },
    });
    this.validateAll('SELECT DATEADD(qq, 1, \'2017/08/25\')', {
      write: { spark: 'SELECT ADD_MONTHS(\'2017/08/25\', 3)' },
    });
    this.validateAll('SELECT DATEADD(wk, 1, \'2017/08/25\')', {
      write: {
        spark: 'SELECT DATE_ADD(\'2017/08/25\', 7)',
        databricks: 'SELECT DATEADD(WEEK, 1, \'2017/08/25\')',
      },
    });
  }

  testDateDiff () {
    this.validateIdentity('SELECT DATEDIFF(HOUR, 1.5, \'2021-01-01\')');
    this.validateIdentity('SELECT DATEDIFF_BIG(HOUR, 1.5, \'2021-01-01\')');

    for (const fnc of ['DATEDIFF', 'DATEDIFF_BIG']) {
      this.validateAll(`SELECT ${fnc}(quarter, 0, '2021-01-01')`, {
        write: {
          tsql: `SELECT ${fnc}(QUARTER, CAST('1900-01-01' AS DATETIME2), CAST('2021-01-01' AS DATETIME2))`,
          spark: 'SELECT DATEDIFF(QUARTER, CAST(\'1900-01-01\' AS TIMESTAMP), CAST(\'2021-01-01\' AS TIMESTAMP))',
          duckdb: 'SELECT DATE_DIFF(\'QUARTER\', CAST(\'1900-01-01\' AS TIMESTAMP), CAST(\'2021-01-01\' AS TIMESTAMP))',
        },
      });
      this.validateAll(`SELECT ${fnc}(day, 1, '2021-01-01')`, {
        write: {
          tsql: `SELECT ${fnc}(DAY, CAST('1900-01-02' AS DATETIME2), CAST('2021-01-01' AS DATETIME2))`,
          spark: 'SELECT DATEDIFF(DAY, CAST(\'1900-01-02\' AS TIMESTAMP), CAST(\'2021-01-01\' AS TIMESTAMP))',
          duckdb: 'SELECT DATE_DIFF(\'DAY\', CAST(\'1900-01-02\' AS TIMESTAMP), CAST(\'2021-01-01\' AS TIMESTAMP))',
        },
      });
      this.validateAll(`SELECT ${fnc}(year, '2020-01-01', '2021-01-01')`, {
        write: {
          tsql: `SELECT ${fnc}(YEAR, CAST('2020-01-01' AS DATETIME2), CAST('2021-01-01' AS DATETIME2))`,
          spark: 'SELECT DATEDIFF(YEAR, CAST(\'2020-01-01\' AS TIMESTAMP), CAST(\'2021-01-01\' AS TIMESTAMP))',
          spark2: 'SELECT CAST(MONTHS_BETWEEN(CAST(\'2021-01-01\' AS TIMESTAMP), CAST(\'2020-01-01\' AS TIMESTAMP)) / 12 AS INT)',
        },
      });
      this.validateAll(`SELECT ${fnc}(mm, 'start', 'end')`, {
        write: {
          databricks: 'SELECT DATEDIFF(MONTH, CAST(\'start\' AS TIMESTAMP), CAST(\'end\' AS TIMESTAMP))',
          spark2: 'SELECT CAST(MONTHS_BETWEEN(CAST(\'end\' AS TIMESTAMP), CAST(\'start\' AS TIMESTAMP)) AS INT)',
          tsql: `SELECT ${fnc}(MONTH, CAST('start' AS DATETIME2), CAST('end' AS DATETIME2))`,
        },
      });
      this.validateAll(`SELECT ${fnc}(quarter, 'start', 'end')`, {
        write: {
          databricks: 'SELECT DATEDIFF(QUARTER, CAST(\'start\' AS TIMESTAMP), CAST(\'end\' AS TIMESTAMP))',
          spark: 'SELECT DATEDIFF(QUARTER, CAST(\'start\' AS TIMESTAMP), CAST(\'end\' AS TIMESTAMP))',
          spark2: 'SELECT CAST(MONTHS_BETWEEN(CAST(\'end\' AS TIMESTAMP), CAST(\'start\' AS TIMESTAMP)) / 3 AS INT)',
          tsql: `SELECT ${fnc}(QUARTER, CAST('start' AS DATETIME2), CAST('end' AS DATETIME2))`,
        },
      });
      this.validateAll(`SELECT ${fnc}(DAY, CAST(a AS DATETIME2), CAST(b AS DATETIME2)) AS x FROM foo`, {
        write: {
          tsql: `SELECT ${fnc}(DAY, CAST(a AS DATETIME2), CAST(b AS DATETIME2)) AS x FROM foo`,
          clickhouse: 'SELECT DATE_DIFF(DAY, CAST(CAST(a AS Nullable(DateTime)) AS DateTime64(6)), CAST(CAST(b AS Nullable(DateTime)) AS DateTime64(6))) AS x FROM foo',
        },
      });
      this.validateIdentity(
        `SELECT DATEADD(DAY, ${fnc}(DAY, -3, GETDATE()), '08:00:00')`,
        `SELECT DATEADD(DAY, ${fnc}(DAY, CAST('1899-12-29' AS DATETIME2), CAST(GETDATE() AS DATETIME2)), '08:00:00')`,
      );
    }
  }

  testLateralSubquery () {
    this.validateAll('SELECT x.a, x.b, t.v, t.y FROM x CROSS APPLY (SELECT v, y FROM t) t(v, y)', {
      write: {
        spark: 'SELECT x.a, x.b, t.v, t.y FROM x INNER JOIN LATERAL (SELECT v, y FROM t) AS t(v, y)',
        postgres: 'SELECT x.a, x.b, t.v, t.y FROM x INNER JOIN LATERAL (SELECT v, y FROM t) AS t(v, y) ON TRUE',
        tsql: 'SELECT x.a, x.b, t.v, t.y FROM x CROSS APPLY (SELECT v, y FROM t) AS t(v, y)',
      },
    });
    this.validateAll('SELECT x.a, x.b, t.v, t.y FROM x OUTER APPLY (SELECT v, y FROM t) t(v, y)', {
      write: {
        spark: 'SELECT x.a, x.b, t.v, t.y FROM x LEFT JOIN LATERAL (SELECT v, y FROM t) AS t(v, y)',
        postgres: 'SELECT x.a, x.b, t.v, t.y FROM x LEFT JOIN LATERAL (SELECT v, y FROM t) AS t(v, y) ON TRUE',
        tsql: 'SELECT x.a, x.b, t.v, t.y FROM x OUTER APPLY (SELECT v, y FROM t) AS t(v, y)',
      },
    });
    this.validateAll(
      'SELECT x.a, x.b, t.v, t.y, s.v, s.y FROM x OUTER APPLY (SELECT v, y FROM t) t(v, y) OUTER APPLY (SELECT v, y FROM t) s(v, y) LEFT JOIN z ON z.id = s.id',
      {
        write: {
          spark: 'SELECT x.a, x.b, t.v, t.y, s.v, s.y FROM x LEFT JOIN LATERAL (SELECT v, y FROM t) AS t(v, y) LEFT JOIN LATERAL (SELECT v, y FROM t) AS s(v, y) LEFT JOIN z ON z.id = s.id',
          postgres: 'SELECT x.a, x.b, t.v, t.y, s.v, s.y FROM x LEFT JOIN LATERAL (SELECT v, y FROM t) AS t(v, y) ON TRUE LEFT JOIN LATERAL (SELECT v, y FROM t) AS s(v, y) ON TRUE LEFT JOIN z ON z.id = s.id',
          tsql: 'SELECT x.a, x.b, t.v, t.y, s.v, s.y FROM x OUTER APPLY (SELECT v, y FROM t) AS t(v, y) OUTER APPLY (SELECT v, y FROM t) AS s(v, y) LEFT JOIN z ON z.id = s.id',
        },
      },
    );
  }

  testLateralTableValuedFunction () {
    this.validateAll('SELECT t.x, y.z FROM x CROSS APPLY tvfTest(t.x) y(z)', {
      write: {
        spark: 'SELECT t.x, y.z FROM x INNER JOIN LATERAL TVFTEST(t.x) AS y(z)',
        postgres: 'SELECT t.x, y.z FROM x INNER JOIN LATERAL TVFTEST(t.x) AS y(z) ON TRUE',
        tsql: 'SELECT t.x, y.z FROM x CROSS APPLY TVFTEST(t.x) AS y(z)',
      },
    });
    this.validateAll('SELECT t.x, y.z FROM x OUTER APPLY tvfTest(t.x)y(z)', {
      write: {
        spark: 'SELECT t.x, y.z FROM x LEFT JOIN LATERAL TVFTEST(t.x) AS y(z)',
        postgres: 'SELECT t.x, y.z FROM x LEFT JOIN LATERAL TVFTEST(t.x) AS y(z) ON TRUE',
        tsql: 'SELECT t.x, y.z FROM x OUTER APPLY TVFTEST(t.x) AS y(z)',
      },
    });
    this.validateAll('SELECT t.x, y.z FROM x OUTER APPLY a.b.tvfTest(t.x)y(z)', {
      write: {
        spark: 'SELECT t.x, y.z FROM x LEFT JOIN LATERAL a.b.tvfTest(t.x) AS y(z)',
        postgres: 'SELECT t.x, y.z FROM x LEFT JOIN LATERAL a.b.tvfTest(t.x) AS y(z) ON TRUE',
        tsql: 'SELECT t.x, y.z FROM x OUTER APPLY a.b.tvfTest(t.x) AS y(z)',
      },
    });
  }

  testTop () {
    this.validateAll('SELECT DISTINCT TOP 3 * FROM A', {
      read: { spark: 'SELECT DISTINCT * FROM A LIMIT 3' },
      write: {
        spark: 'SELECT DISTINCT * FROM A LIMIT 3',
        teradata: 'SELECT DISTINCT TOP 3 * FROM A',
        tsql: 'SELECT DISTINCT TOP 3 * FROM A',
      },
    });
    this.validateAll('SELECT TOP (3) * FROM A', {
      write: { spark: 'SELECT * FROM A LIMIT 3' },
    });
    this.validateIdentity(
      'CREATE TABLE schema.table AS SELECT a, id FROM (SELECT a, (SELECT id FROM tb ORDER BY t DESC LIMIT 1) as id FROM tbl) AS _subquery',
      'SELECT * INTO schema.table FROM (SELECT a AS a, id AS id FROM (SELECT a AS a, (SELECT TOP 1 id FROM tb ORDER BY t DESC) AS id FROM tbl) AS _subquery) AS temp',
    );
    this.validateIdentity('SELECT TOP 10 PERCENT');
    this.validateIdentity('SELECT TOP 10 PERCENT WITH TIES');
  }

  testFormat () {
    this.validateIdentity('SELECT FORMAT(foo, \'dddd\', \'de-CH\')');
    this.validateIdentity('SELECT FORMAT(EndOfDayRate, \'N\', \'en-us\')');
    this.validateIdentity('SELECT FORMAT(\'01-01-1991\', \'d.mm.yyyy\')');
    this.validateIdentity('SELECT FORMAT(12345, \'###.###.###\')');
    this.validateIdentity('SELECT FORMAT(1234567, \'f\')');
    this.validateAll('SELECT FORMAT(1000000.01,\'###,###.###\')', {
      write: {
        spark: 'SELECT FORMAT_NUMBER(1000000.01, \'###,###.###\')',
        tsql: 'SELECT FORMAT(1000000.01, \'###,###.###\')',
      },
    });
    this.validateAll('SELECT FORMAT(1234567, \'f\')', {
      write: {
        spark: 'SELECT FORMAT_NUMBER(1234567, \'f\')',
        tsql: 'SELECT FORMAT(1234567, \'f\')',
      },
    });
    this.validateAll('SELECT FORMAT(\'01-01-1991\', \'dd.mm.yyyy\')', {
      write: {
        spark: 'SELECT DATE_FORMAT(\'01-01-1991\', \'dd.mm.yyyy\')',
        tsql: 'SELECT FORMAT(\'01-01-1991\', \'dd.mm.yyyy\')',
      },
    });
    this.validateAll('SELECT FORMAT(date_col, \'dd.mm.yyyy\')', {
      write: {
        spark: 'SELECT DATE_FORMAT(date_col, \'dd.mm.yyyy\')',
        tsql: 'SELECT FORMAT(date_col, \'dd.mm.yyyy\')',
      },
    });
    this.validateAll('SELECT FORMAT(date_col, \'m\')', {
      write: {
        spark: 'SELECT DATE_FORMAT(date_col, \'MMMM d\')',
        tsql: 'SELECT FORMAT(date_col, \'MMMM d\')',
      },
    });
    this.validateAll('SELECT FORMAT(num_col, \'c\')', {
      write: {
        spark: 'SELECT FORMAT_NUMBER(num_col, \'c\')',
        tsql: 'SELECT FORMAT(num_col, \'c\')',
      },
    });
  }

  testString () {
    this.validateAll('SELECT N\'test\'', { write: { spark: 'SELECT \'test\'' } });
    this.validateAll('SELECT n\'test\'', { write: { spark: 'SELECT \'test\'' } });
    this.validateAll('SELECT \'\'\'test\'\'\'', { write: { spark: String.raw`SELECT '\'test\''` } });
  }

  testEoMonth () {
    this.validateAll('EOMONTH(GETDATE())', {
      read: { spark: 'LAST_DAY(CURRENT_TIMESTAMP())' },
      write: {
        bigquery: 'LAST_DAY(CAST(CURRENT_TIMESTAMP() AS DATE))',
        clickhouse: 'LAST_DAY(CAST(CURRENT_TIMESTAMP() AS Nullable(DATE)))',
        duckdb: 'LAST_DAY(CAST(CURRENT_TIMESTAMP AS DATE))',
        mysql: 'LAST_DAY(DATE(CURRENT_TIMESTAMP()))',
        postgres: 'CAST(DATE_TRUNC(\'MONTH\', CAST(CURRENT_TIMESTAMP AS DATE)) + INTERVAL \'1 MONTH\' - INTERVAL \'1 DAY\' AS DATE)',
        presto: 'LAST_DAY_OF_MONTH(CAST(CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS DATE))',
        redshift: 'LAST_DAY(CAST(GETDATE() AS DATE))',
        snowflake: 'LAST_DAY(TO_DATE(CURRENT_TIMESTAMP()))',
        spark: 'LAST_DAY(TO_DATE(CURRENT_TIMESTAMP()))',
        tsql: 'EOMONTH(CAST(GETDATE() AS DATE))',
      },
    });
    this.validateAll('EOMONTH(GETDATE(), -1)', {
      write: {
        bigquery: 'LAST_DAY(DATE_ADD(CAST(CURRENT_TIMESTAMP() AS DATE), INTERVAL -1 MONTH))',
        clickhouse: 'LAST_DAY(DATE_ADD(MONTH, -1, CAST(CURRENT_TIMESTAMP() AS Nullable(DATE))))',
        duckdb: 'LAST_DAY(CAST(CURRENT_TIMESTAMP AS DATE) + INTERVAL (-1) MONTH)',
        mysql: 'LAST_DAY(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 MONTH))',
        postgres: 'CAST(DATE_TRUNC(\'MONTH\', CAST(CURRENT_TIMESTAMP AS DATE) + INTERVAL \'-1 MONTH\') + INTERVAL \'1 MONTH\' - INTERVAL \'1 DAY\' AS DATE)',
        presto: 'LAST_DAY_OF_MONTH(DATE_ADD(\'MONTH\', -1, CAST(CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS DATE)))',
        redshift: 'LAST_DAY(DATEADD(MONTH, -1, CAST(GETDATE() AS DATE)))',
        snowflake: 'LAST_DAY(DATEADD(MONTH, -1, TO_DATE(CURRENT_TIMESTAMP())))',
        spark: 'LAST_DAY(ADD_MONTHS(TO_DATE(CURRENT_TIMESTAMP()), -1))',
        tsql: 'EOMONTH(DATEADD(MONTH, -1, CAST(GETDATE() AS DATE)))',
      },
    });
  }

  testIdentifierPrefixes () {
    expect(
      narrowInstanceOf(this.validateIdentity('#x').assertIs(ColumnExpr)?.args.this, IdentifierExpr)?.args['temporary'],
    ).toBeTruthy();
    expect(
      narrowInstanceOf(this.validateIdentity('##x').assertIs(ColumnExpr)?.args.this, IdentifierExpr)?.args['global'],
    ).toBeTruthy();

    narrowInstanceOf(this.validateIdentity('@x').assertIs(ParameterExpr)?.args.this, VarExpr);
    narrowInstanceOf(narrowInstanceOf(narrowInstanceOf(narrowInstanceOf(this.validateIdentity('SELECT * FROM @x').args['from'], Expression)?.args.this, TableExpr)?.args.this, ParameterExpr)?.args.this, VarExpr);

    this.validateAll('SELECT @x', {
      write: {
        databricks: 'SELECT ${x}',
        hive: 'SELECT ${x}',
        spark: 'SELECT ${x}',
        tsql: 'SELECT @x',
      },
    });
    this.validateAll('SELECT * FROM #mytemptable', {
      write: {
        duckdb: 'SELECT * FROM mytemptable',
        spark: 'SELECT * FROM mytemptable',
        tsql: 'SELECT * FROM #mytemptable',
      },
    });
    this.validateAll('SELECT * FROM ##mytemptable', {
      write: {
        duckdb: 'SELECT * FROM mytemptable',
        spark: 'SELECT * FROM mytemptable',
        tsql: 'SELECT * FROM ##mytemptable',
      },
    });
  }

  testTemporalTable () {
    this.validateIdentity(
      'CREATE TABLE test ("data" CHAR(7), "valid_from" DATETIME2(2) GENERATED ALWAYS AS ROW START NOT NULL, "valid_to" DATETIME2(2) GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME ("valid_from", "valid_to")) WITH(SYSTEM_VERSIONING=ON)',
      'CREATE TABLE test ([data] CHAR(7), [valid_from] DATETIME2(2) GENERATED ALWAYS AS ROW START NOT NULL, [valid_to] DATETIME2(2) GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])) WITH(SYSTEM_VERSIONING=ON)',
    );
    this.validateIdentity(
      'CREATE TABLE test ([data] CHAR(7), [valid_from] DATETIME2(2) GENERATED ALWAYS AS ROW START HIDDEN NOT NULL, [valid_to] DATETIME2(2) GENERATED ALWAYS AS ROW END HIDDEN NOT NULL, PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])) WITH(SYSTEM_VERSIONING=ON(HISTORY_TABLE=[dbo].[benchmark_history], DATA_CONSISTENCY_CHECK=ON))',
    );
    this.validateIdentity(
      'CREATE TABLE test ([data] CHAR(7), [valid_from] DATETIME2(2) GENERATED ALWAYS AS ROW START NOT NULL, [valid_to] DATETIME2(2) GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])) WITH(SYSTEM_VERSIONING=ON(HISTORY_TABLE=[dbo].[benchmark_history], DATA_CONSISTENCY_CHECK=ON))',
    );
    this.validateIdentity(
      'CREATE TABLE test ([data] CHAR(7), [valid_from] DATETIME2(2) GENERATED ALWAYS AS ROW START NOT NULL, [valid_to] DATETIME2(2) GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])) WITH(SYSTEM_VERSIONING=ON(HISTORY_TABLE=[dbo].[benchmark_history], DATA_CONSISTENCY_CHECK=OFF))',
    );
    this.validateIdentity(
      'CREATE TABLE test ([data] CHAR(7), [valid_from] DATETIME2(2) GENERATED ALWAYS AS ROW START NOT NULL, [valid_to] DATETIME2(2) GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])) WITH(SYSTEM_VERSIONING=ON(HISTORY_TABLE=[dbo].[benchmark_history]))',
    );
  }

  testSystemTime () {
    this.validateIdentity('SELECT [x] FROM [a].[b] FOR SYSTEM_TIME AS OF \'foo\'');
    this.validateIdentity('SELECT [x] FROM [a].[b] FOR SYSTEM_TIME AS OF \'foo\' AS alias');
    this.validateIdentity('SELECT [x] FROM [a].[b] FOR SYSTEM_TIME FROM c TO d');
    this.validateIdentity('SELECT [x] FROM [a].[b] FOR SYSTEM_TIME BETWEEN c AND d');
    this.validateIdentity('SELECT [x] FROM [a].[b] FOR SYSTEM_TIME CONTAINED IN (c, d)');
    this.validateIdentity('SELECT [x] FROM [a].[b] FOR SYSTEM_TIME ALL AS alias');
  }

  testCurrentUser () {
    this.validateAll('SUSER_NAME()', { write: { spark: 'CURRENT_USER()' } });
    this.validateAll('SUSER_SNAME()', { write: { spark: 'CURRENT_USER()' } });
    this.validateAll('SYSTEM_USER()', { write: { spark: 'CURRENT_USER()' } });
    this.validateAll('SYSTEM_USER', { write: { spark: 'CURRENT_USER()' } });
  }

  testHints () {
    this.validateAll('SELECT x FROM a INNER HASH JOIN b ON b.id = a.id', {
      write: { spark: 'SELECT x FROM a INNER JOIN b ON b.id = a.id' },
    });
    this.validateAll('SELECT x FROM a INNER LOOP JOIN b ON b.id = a.id', {
      write: { spark: 'SELECT x FROM a INNER JOIN b ON b.id = a.id' },
    });
    this.validateAll('SELECT x FROM a INNER REMOTE JOIN b ON b.id = a.id', {
      write: { spark: 'SELECT x FROM a INNER JOIN b ON b.id = a.id' },
    });
    this.validateAll('SELECT x FROM a INNER MERGE JOIN b ON b.id = a.id', {
      write: { spark: 'SELECT x FROM a INNER JOIN b ON b.id = a.id' },
    });
    this.validateAll('SELECT x FROM a WITH (NOLOCK)', {
      write: {
        'spark': 'SELECT x FROM a',
        'tsql': 'SELECT x FROM a WITH (NOLOCK)',
        '': 'SELECT x FROM a WITH (NOLOCK)',
      },
    });
    this.validateIdentity('SELECT x FROM a INNER LOOP JOIN b ON b.id = a.id');
  }

  testOpenjson () {
    this.validateIdentity('SELECT * FROM OPENJSON(@json)');
    this.validateAll('SELECT [key], value FROM OPENJSON(@json,\'$.path.to."sub-object"\')', {
      write: {
        tsql: 'SELECT [key], value FROM OPENJSON(@json, \'$.path.to."sub-object"\')',
      },
    });
    this.validateAll(
      'SELECT * FROM OPENJSON(@array) WITH (month VARCHAR(3), temp int, month_id tinyint \'$.sql:identity()\') as months',
      {
        write: {
          tsql: 'SELECT * FROM OPENJSON(@array) WITH (month VARCHAR(3), temp INTEGER, month_id TINYINT \'$.sql:identity()\') AS months',
        },
      },
    );
    this.validateAll(
      `
            SELECT *
            FROM OPENJSON ( @json )
            WITH (
                          Number   VARCHAR(200)   '$.Order.Number',
                          Date     DATETIME       '$.Order.Date',
                          Customer VARCHAR(200)   '$.AccountNumber',
                          Quantity INT            '$.Item.Quantity',
                          [Order]  NVARCHAR(MAX)  AS JSON
             )
            `,
      {
        write: {
          tsql: `SELECT
  *
FROM OPENJSON(@json) WITH (
    Number VARCHAR(200) '$.Order.Number',
    Date DATETIME '$.Order.Date',
    Customer VARCHAR(200) '$.AccountNumber',
    Quantity INTEGER '$.Item.Quantity',
    [Order] NVARCHAR(MAX) AS JSON
)`,
        },
        pretty: true,
      },
    );
  }

  testSet () {
    this.validateAll('SET KEY VALUE', {
      write: {
        tsql: 'SET KEY VALUE',
        duckdb: 'SET KEY = VALUE',
        spark: 'SET KEY = VALUE',
      },
    });
    this.validateAll('SET @count = (SELECT COUNT(1) FROM x)', {
      write: {
        databricks: 'SET count = (SELECT COUNT(1) FROM x)',
        tsql: 'SET @count = (SELECT COUNT(1) FROM x)',
        spark: 'SET count = (SELECT COUNT(1) FROM x)',
      },
    });
  }

  testQualifyDerivedTableOutputs () {
    this.validateIdentity('WITH t AS (SELECT 1) SELECT * FROM t', 'WITH t AS (SELECT 1 AS [1]) SELECT * FROM t');
    this.validateIdentity('WITH t AS (SELECT "c") SELECT * FROM t', 'WITH t AS (SELECT [c] AS [c]) SELECT * FROM t');
    this.validateIdentity('SELECT * FROM (SELECT 1) AS subq', 'SELECT * FROM (SELECT 1 AS [1]) AS subq');
    this.validateIdentity('SELECT * FROM (SELECT "c") AS subq', 'SELECT * FROM (SELECT [c] AS [c]) AS subq');
    this.validateAll(
      'WITH t1(c) AS (SELECT 1), t2 AS (SELECT CAST(c AS INTEGER) AS c FROM t1) SELECT * FROM t2',
      {
        read: {
          duckdb: 'WITH t1(c) AS (SELECT 1), t2 AS (SELECT CAST(c AS INTEGER) FROM t1) SELECT * FROM t2',
        },
      },
    );
  }

  testDeclare () {
    this.validateIdentity('DECLARE @X INT', 'DECLARE @X AS INTEGER');
    this.validateIdentity('DECLARE @X INT = 1', 'DECLARE @X AS INTEGER = 1');
    this.validateIdentity('DECLARE @X INT, @Y VARCHAR(10)', 'DECLARE @X AS INTEGER, @Y AS VARCHAR(10)');
    this.validateIdentity(
      'declare @X int = (select col from table where id = 1)',
      'DECLARE @X AS INTEGER = (SELECT col FROM table WHERE id = 1)',
    );
    this.validateIdentity(
      'declare @X TABLE (Id INT NOT NULL, Name VARCHAR(100) NOT NULL)',
      'DECLARE @X AS TABLE (Id INTEGER NOT NULL, Name VARCHAR(100) NOT NULL)',
    );
    this.validateIdentity(
      'declare @X TABLE (Id INT NOT NULL, constraint PK_Id primary key (Id))',
      'DECLARE @X AS TABLE (Id INTEGER NOT NULL, CONSTRAINT PK_Id PRIMARY KEY (Id))',
    );
    this.validateIdentity('declare @X UserDefinedTableType', 'DECLARE @X AS UserDefinedTableType');
    this.validateIdentity(
      'DECLARE @MyTableVar TABLE (EmpID INT NOT NULL, PRIMARY KEY CLUSTERED (EmpID), UNIQUE NONCLUSTERED (EmpID), INDEX CustomNonClusteredIndex NONCLUSTERED (EmpID))',
      undefined,
      { checkCommandWarning: true },
    );
    this.validateIdentity(
      'DECLARE vendor_cursor CURSOR FOR SELECT VendorID, Name FROM Purchasing.Vendor WHERE PreferredVendorStatus = 1 ORDER BY VendorID',
      undefined,
      { checkCommandWarning: true },
    );
  }

  testScopeResolutionOp () {
    this.validateIdentity('x::int', 'CAST(x AS INTEGER)');
    this.validateIdentity('x::varchar', 'CAST(x AS VARCHAR)');
    this.validateIdentity('x::varchar(MAX)', 'CAST(x AS VARCHAR(MAX))');

    const pairs: [string, string][] = [
      ['', 'FOO(a, b)'],
      ['bar', 'baZ(1, 2)'],
      ['LOGIN', 'EricKurjan'],
      ['GEOGRAPHY', 'Point(latitude, longitude, 4326)'],
      ['GEOGRAPHY', 'STGeomFromText(\'POLYGON((-122.358 47.653 , -122.348 47.649, -122.348 47.658, -122.358 47.658, -122.358 47.653))\', 4326)'],
    ];

    for (const [lhs, rhs] of pairs) {
      const expr = this.validateIdentity(`${lhs}::${rhs}`);
      const baseSql = expr.sql();
      expect(baseSql).toBe(`SCOPE_RESOLUTION(${lhs ? lhs + ', ' : ''}${rhs})`);
      expect(parseOne(baseSql).sql({ dialect: 'tsql' })).toBe(`${lhs}::${rhs}`);
    }
  }

  testGrant () {
    this.validateIdentity('GRANT EXECUTE ON TestProc TO User2');
    this.validateIdentity('GRANT EXECUTE ON TestProc TO TesterRole WITH GRANT OPTION');
    this.validateIdentity(
      'GRANT EXECUTE ON TestProc TO User2 AS TesterRole',
      undefined,
      { checkCommandWarning: true },
    );
  }

  testRevoke () {
    this.validateIdentity('REVOKE EXECUTE ON TestProc FROM User2');
    this.validateIdentity('REVOKE EXECUTE ON TestProc FROM TesterRole');
  }

  testParsename () {
    for (let i = 0; i < 4; i++) {
      this.validateAll(`SELECT PARSENAME('1.2.3', ${i})`, {
        read: {
          spark: `SELECT SPLIT_PART('1.2.3', '.', ${4 - i})`,
          databricks: `SELECT SPLIT_PART('1.2.3', '.', ${4 - i})`,
        },
        write: {
          spark: `SELECT SPLIT_PART('1.2.3', '.', ${4 - i})`,
          databricks: `SELECT SPLIT_PART('1.2.3', '.', ${4 - i})`,
          tsql: `SELECT PARSENAME('1.2.3', ${i})`,
        },
      });
    }
    this.validateAll('SELECT SPLIT_PART(\'1,2,3\', \',\', 1)', {
      write: {
        spark: 'SELECT SPLIT_PART(\'1,2,3\', \',\', 1)',
        databricks: 'SELECT SPLIT_PART(\'1,2,3\', \',\', 1)',
        tsql: UnsupportedError,
      },
    });
    this.validateAll(
      'WITH t AS (SELECT \'a.b.c\' AS value, 1 AS idx) SELECT SPLIT_PART(value, \'.\', idx) FROM t',
      {
        write: {
          spark: 'WITH t AS (SELECT \'a.b.c\' AS value, 1 AS idx) SELECT SPLIT_PART(value, \'.\', idx) FROM t',
          databricks: 'WITH t AS (SELECT \'a.b.c\' AS value, 1 AS idx) SELECT SPLIT_PART(value, \'.\', idx) FROM t',
          tsql: UnsupportedError,
        },
      },
    );
  }

  testNextValueFor () {
    this.validateIdentity(
      'SELECT NEXT VALUE FOR db.schema.sequence_name OVER (ORDER BY foo), col',
    );
    this.validateAll('SELECT NEXT VALUE FOR db.schema.sequence_name', {
      read: {
        oracle: 'SELECT NEXT VALUE FOR db.schema.sequence_name',
        tsql: 'SELECT NEXT VALUE FOR db.schema.sequence_name',
      },
      write: {
        oracle: 'SELECT NEXT VALUE FOR db.schema.sequence_name',
      },
    });
  }

  testDatetrunc () {
    this.validateAll('SELECT DATETRUNC(month, \'foo\')', {
      write: {
        duckdb: 'SELECT DATE_TRUNC(\'MONTH\', CAST(\'foo\' AS TIMESTAMP))',
        tsql: 'SELECT DATETRUNC(MONTH, CAST(\'foo\' AS DATETIME2))',
      },
    });
    this.validateAll('SELECT DATETRUNC(month, foo)', {
      write: {
        duckdb: 'SELECT DATE_TRUNC(\'MONTH\', foo)',
        tsql: 'SELECT DATETRUNC(MONTH, foo)',
      },
    });
    this.validateAll('SELECT DATETRUNC(year, CAST(\'foo1\' AS date))', {
      write: {
        duckdb: 'SELECT DATE_TRUNC(\'YEAR\', CAST(\'foo1\' AS DATE))',
        tsql: 'SELECT DATETRUNC(YEAR, CAST(\'foo1\' AS DATE))',
      },
    });
  }

  testNumericTrunc () {
    this.validateAll('ROUND(3.14159, 2, 1)', {
      read: {
        oracle: 'TRUNC(3.14159, 2)',
        postgres: 'TRUNC(3.14159, 2)',
        mysql: 'TRUNCATE(3.14159, 2)',
      },
      write: { tsql: 'ROUND(3.14159, 2, 1)' },
    });
  }

  testCollationParse () {
    narrowInstanceOf(
      narrowInstanceOf(
        narrowInstanceOf(
          this.validateIdentity('ALTER TABLE a ALTER COLUMN b CHAR(10) COLLATE abc')
            .assertIs(AlterExpr)
            ?.args['actions']?.[0],
          Expression,
        )?.args['collate'],
        Expression,
      )?.args.this,
      VarExpr,
    );
  }

  testOdbcDateLiterals () {
    const cases: [string, typeof DateExpr | typeof TimeExpr | typeof TimestampExpr][] = [
      ['{d\'2024-01-01\'}', DateExpr],
      ['{t\'12:00:00\'}', TimeExpr],
      ['{ts\'2024-01-01 12:00:00\'}', TimestampExpr],
    ];
    for (const [value, cls] of cases) {
      const sql = `INSERT INTO tab(ds) VALUES (${value})`;
      const expr = this.parseOne(sql);
      expect(expr).toBeInstanceOf(InsertExpr);
      const insert = expr as InsertExpr;
      const literal = narrowInstanceOf(insert.args.expression?.args?.expressions?.[0], Expression)?.args.expressions?.[0];
      expect(literal).toBeInstanceOf(cls);
    }
  }
}

const t = new TestTSQL();
describe('TestTSQL', () => {
  test('testTsql', () => t.testTsql());
  test('testOption', () => t.testOption());
  test('testForXml', () => t.testForXml());
  test('testTypes', () => t.testTypes());
  test('testTypesInts', () => t.testTypesInts());
  test('testTypesDecimals', () => t.testTypesDecimals());
  test('testTypesString', () => t.testTypesString());
  test('testTypesDate', () => t.testTypesDate());
  test('testTypesBin', () => t.testTypesBin());
  test('testDdl', () => t.testDdl());
  test('testTransaction', () => t.testTransaction());
  test('testCommit', () => t.testCommit());
  test('testRollback', () => t.testRollback());
  test('testUdf', () => t.testUdf());
  test('testProcedureKeywords', () => t.testProcedureKeywords());
  test('testFullproc', () => t.testFullproc());
  test('testCharindex', () => t.testCharindex());
  test('testLen', () => t.testLen());
  test('testReplicate', () => t.testReplicate());
  test('testIsnull', () => t.testIsnull());
  test('testJson', () => t.testJson());
  test('testDatefromparts', () => t.testDatefromparts());
  test('testDatename', () => t.testDatename());
  test('testDatepart', () => t.testDatepart());
  test('testConvert', () => t.testConvert());
  test('testAddDate', () => t.testAddDate());
  test('testDateDiff', () => t.testDateDiff());
  test('testLateralSubquery', () => t.testLateralSubquery());
  test('testLateralTableValuedFunction', () => t.testLateralTableValuedFunction());
  test('testTop', () => t.testTop());
  test('testFormat', () => t.testFormat());
  test('testString', () => t.testString());
  test('testEomonth', () => t.testEoMonth());
  test('testIdentifierPrefixes', () => t.testIdentifierPrefixes());
  test('testTemporalTable', () => t.testTemporalTable());
  test('testSystemTime', () => t.testSystemTime());
  test('testCurrentUser', () => t.testCurrentUser());
  test('testHints', () => t.testHints());
  test('testOpenjson', () => t.testOpenjson());
  test('testSet', () => t.testSet());
  test('testQualifyDerivedTableOutputs', () => t.testQualifyDerivedTableOutputs());
  test('testDeclare', () => t.testDeclare());
  test('testScopeResolutionOp', () => t.testScopeResolutionOp());
  test('testGrant', () => t.testGrant());
  test('testRevoke', () => t.testRevoke());
  test('testParsename', () => t.testParsename());
  test('testNextValueFor', () => t.testNextValueFor());
  test('testDatetrunc', () => t.testDatetrunc());
  test('testNumericTrunc', () => t.testNumericTrunc());
  test('testCollationParse', () => t.testCollationParse());
  test('testOdbcDateLiterals', () => t.testOdbcDateLiterals());
});
