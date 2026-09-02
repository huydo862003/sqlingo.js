import {
  describe, test, expect, vi,
} from 'vitest';
import {
  parseOne, UnsupportedError,
} from '../../../src/index';
import type {
  Expression,
} from '../../../src/expressions';
import {
  DescribeExpr, EqExpr, LiteralExpr, ModExpr, SessionParameterExpr,
  ShowExpr, TruncExpr, UtcTimeExpr, UtcTimestampExpr,
  WhereExpr,
} from '../../../src/expressions';
import {
  MySQL,
} from '../../../src/dialects/mysql';
import {
  Validator,
} from './validator';

class TestMySQL extends Validator {
  override dialect = 'mysql' as const;

  testDdl () {
    for (const t of ['BIGINT', 'INT', 'MEDIUMINT', 'SMALLINT', 'TINYINT']) {
      this.validateIdentity(`CREATE TABLE t (id ${t} UNSIGNED)`);
      this.validateIdentity(`CREATE TABLE t (id ${t}(10) UNSIGNED)`);
    }

    this.validateIdentity('CREATE TABLE bar (abacate DOUBLE(10, 2) UNSIGNED)');
    this.validateIdentity('CREATE TABLE t (id DECIMAL(20, 4) UNSIGNED)');
    this.validateIdentity('CREATE TABLE foo (a BIGINT, UNIQUE (b) USING BTREE)');
    this.validateIdentity('CREATE TABLE foo (id BIGINT)');
    this.validateIdentity('CREATE TABLE 00f (1d BIGINT)');
    this.validateIdentity('CREATE TABLE temp (id SERIAL PRIMARY KEY)');
    this.validateIdentity('UPDATE items SET items.price = 0 WHERE items.id >= 5 LIMIT 10');
    this.validateIdentity('DELETE FROM t WHERE a <= 10 LIMIT 10');
    this.validateIdentity('DELETE FROM t FORCE INDEX (idx) WHERE a > 5 ORDER BY id');
    this.validateIdentity('CREATE TABLE foo (a BIGINT, INDEX USING BTREE (b))');
    this.validateIdentity('CREATE TABLE foo (a BIGINT, FULLTEXT INDEX (b))');
    this.validateIdentity('CREATE TABLE foo (a BIGINT, SPATIAL INDEX (b))');
    this.validateIdentity('CREATE TABLE foo (a INT UNSIGNED ZEROFILL)');
    this.validateIdentity('ALTER TABLE t1 ADD COLUMN x INT, ALGORITHM=INPLACE, LOCK=EXCLUSIVE');
    this.validateIdentity('ALTER TABLE t ADD INDEX `i` (`c`)');
    this.validateIdentity('ALTER TABLE t ADD UNIQUE `i` (`c`)');
    this.validateIdentity('ALTER TABLE test_table MODIFY COLUMN test_column LONGTEXT');
    this.validateIdentity('ALTER VIEW v AS SELECT a, b, c, d FROM foo');
    this.validateIdentity('ALTER VIEW v AS SELECT * FROM foo WHERE c > 100');
    this.validateIdentity(
      'ALTER ALGORITHM = MERGE VIEW v AS SELECT * FROM foo',
      undefined,
      { checkCommandWarning: true },
    );
    this.validateIdentity(
      'ALTER DEFINER = \'admin\'@\'localhost\' VIEW v AS SELECT * FROM foo',
      undefined,
      { checkCommandWarning: true },
    );
    this.validateIdentity(
      'CREATE SQL SECURITY INVOKER VIEW id_test (id, foo) AS SELECT 0, foo FROM test',
    );
    this.validateIdentity(
      'CREATE SQL SECURITY DEFINER VIEW id_test (id, foo) AS SELECT 0, foo FROM test',
    );
    this.validateIdentity(
      'ALTER SQL SECURITY = DEFINER VIEW v AS SELECT * FROM foo',
      undefined,
      { checkCommandWarning: true },
    );
    this.validateIdentity(
      'INSERT INTO things (a, b) VALUES (1, 2) AS new_data ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id), a = new_data.a, b = new_data.b',
    );
    this.validateIdentity(
      'CREATE TABLE `oauth_consumer` (`key` VARCHAR(32) NOT NULL, UNIQUE `OAUTH_CONSUMER_KEY` (`key`))',
    );
    this.validateIdentity(
      'CREATE TABLE `x` (`username` VARCHAR(200), PRIMARY KEY (`username`(16)))',
    );
    this.validateIdentity(
      'UPDATE items SET items.price = 0 WHERE items.id >= 5 ORDER BY items.id LIMIT 10',
    );
    this.validateIdentity(
      'CREATE TABLE foo (a BIGINT, INDEX b USING HASH (c) COMMENT \'d\' VISIBLE ENGINE_ATTRIBUTE = \'e\' WITH PARSER foo)',
    );
    this.validateIdentity(
      'DELETE t1 FROM t1 LEFT JOIN t2 ON t1.id = t2.id WHERE t2.id IS NULL',
    );
    this.validateIdentity(
      'DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id = t2.id AND t2.id = t3.id',
    );
    this.validateIdentity(
      'DELETE FROM t1, t2 USING t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id = t2.id AND t2.id = t3.id',
    );
    this.validateIdentity(
      'INSERT IGNORE INTO subscribers (email) VALUES (\'john.doe@gmail.com\'), (\'jane.smith@ibm.com\')',
    );
    this.validateIdentity(
      'INSERT INTO t1 (a, b, c) VALUES (1, 2, 3), (4, 5, 6) ON DUPLICATE KEY UPDATE c = VALUES(a) + VALUES(b)',
    );
    this.validateIdentity(
      'INSERT INTO t1 (a, b) SELECT c, d FROM t2 UNION SELECT e, f FROM t3 ON DUPLICATE KEY UPDATE b = b + c',
    );
    this.validateIdentity(
      'INSERT INTO t1 (a, b, c) VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE c = c + 1',
    );
    this.validateIdentity(
      'INSERT INTO x VALUES (1, \'a\', 2.0) ON DUPLICATE KEY UPDATE x.id = 1',
    );
    this.validateIdentity(
      'CREATE OR REPLACE VIEW my_view AS SELECT column1 AS `boo`, column2 AS `foo` FROM my_table WHERE column3 = \'some_value\' UNION SELECT q.* FROM fruits_table, JSON_TABLE(Fruits, \'$[*]\' COLUMNS(id VARCHAR(255) PATH \'$.$id\', value VARCHAR(255) PATH \'$.value\')) AS q',
    );
    this.validateIdentity(
      'CREATE TABLE test_table (id INT AUTO_INCREMENT, PRIMARY KEY (id) USING BTREE)',
    );
    this.validateIdentity(
      'CREATE TABLE test_table (id INT AUTO_INCREMENT, PRIMARY KEY (id) USING HASH)',
    );
    this.validateIdentity('CREATE TABLE test (id INT, PRIMARY KEY pk_name (id))');
    this.validateIdentity('CREATE TABLE test (id INT, PRIMARY KEY `pk_name` (id))');
    this.validateIdentity(
      'CREATE TABLE test (id INT, PRIMARY KEY "pk_name" (id))',
      'CREATE TABLE test (id INT, PRIMARY KEY `pk_name` (id))',
    );
    this.validateIdentity('CREATE TABLE test (id INT, CONSTRAINT pk_name PRIMARY KEY (id))');
    this.validateIdentity(
      'CREATE TABLE test (a INT, b INT GENERATED ALWAYS AS (a + a) STORED)',
    );
    this.validateIdentity(
      'CREATE TABLE test (a INT, b INT GENERATED ALWAYS AS (a + a) VIRTUAL)',
    );
    this.validateIdentity(
      'CREATE TABLE test (a INT, b INT AS (a + a) STORED)',
      'CREATE TABLE test (a INT, b INT GENERATED ALWAYS AS (a + a) STORED)',
    );
    this.validateIdentity(
      'CREATE TABLE test (a INT, b INT AS (a + a) VIRTUAL)',
      'CREATE TABLE test (a INT, b INT GENERATED ALWAYS AS (a + a) VIRTUAL)',
    );
    this.validateIdentity(
      '/*left*/ EXPLAIN SELECT /*hint*/ col FROM t1 /*right*/',
      '/* left */ DESCRIBE /* hint */ SELECT col FROM t1 /* right */',
    );
    this.validateIdentity(
      'CREATE TABLE t (name VARCHAR)',
      'CREATE TABLE t (name TEXT)',
    );
    this.validateIdentity(
      'ALTER TABLE t ADD KEY `i` (`c`)',
      'ALTER TABLE t ADD INDEX `i` (`c`)',
    );
    this.validateIdentity(
      'CREATE TABLE `foo` (`id` char(36) NOT NULL DEFAULT (uuid()), PRIMARY KEY (`id`), UNIQUE KEY `id` (`id`))',
      'CREATE TABLE `foo` (`id` CHAR(36) NOT NULL DEFAULT (UUID()), PRIMARY KEY (`id`), UNIQUE `id` (`id`))',
    );
    this.validateIdentity(
      'CREATE TABLE IF NOT EXISTS industry_info (a BIGINT(20) NOT NULL AUTO_INCREMENT, b BIGINT(20) NOT NULL, c VARCHAR(1000), PRIMARY KEY (a), UNIQUE KEY d (b), KEY e (b))',
      'CREATE TABLE IF NOT EXISTS industry_info (a BIGINT(20) NOT NULL AUTO_INCREMENT, b BIGINT(20) NOT NULL, c VARCHAR(1000), PRIMARY KEY (a), UNIQUE d (b), INDEX e (b))',
    );
    this.validateIdentity(
      'CREATE TABLE test (ts TIMESTAMP, ts_tz TIMESTAMPTZ, ts_ltz TIMESTAMPLTZ)',
      'CREATE TABLE test (ts TIMESTAMP, ts_tz TIMESTAMP, ts_ltz TIMESTAMP)',
    );
    this.validateIdentity(
      'ALTER TABLE test_table ALTER COLUMN test_column SET DATA TYPE LONGTEXT',
      'ALTER TABLE test_table MODIFY COLUMN test_column LONGTEXT',
    );
    this.validateIdentity(
      'CREATE TABLE t (c DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP) DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC',
      'CREATE TABLE t (c DATETIME DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP()) DEFAULT CHARACTER SET=utf8 ROW_FORMAT=DYNAMIC',
    );
    this.validateIdentity(
      'CREATE TABLE `foo` (a VARCHAR(10), KEY idx_a (a DESC))',
      'CREATE TABLE `foo` (a VARCHAR(10), INDEX idx_a (a DESC))',
    );
    this.validateIdentity(
      'CREATE TABLE `foo` (a VARCHAR(10), UNIQUE INDEX idx_a (a))',
      'CREATE TABLE `foo` (a VARCHAR(10), UNIQUE idx_a (a))',
    );

    this.validateAll(
      'insert into t(i) values (default)',
      {
        write: {
          duckdb: 'INSERT INTO t (i) VALUES (DEFAULT)',
          mysql: 'INSERT INTO t (i) VALUES (DEFAULT)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE t (id INT UNSIGNED)',
      {
        write: {
          duckdb: 'CREATE TABLE t (id UINTEGER)',
          mysql: 'CREATE TABLE t (id INT UNSIGNED)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE z (a INT) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT=\'x\'',
      {
        write: {
          duckdb: 'CREATE TABLE z (a INT)',
          mysql: 'CREATE TABLE z (a INT) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT=\'x\'',
          spark: 'CREATE TABLE z (a INT) COMMENT \'x\'',
          sqlite: 'CREATE TABLE z (a INTEGER)',
        },
      },
    );
    this.validateAll(
      'CREATE TABLE x (id int not null auto_increment, primary key (id))',
      {
        write: {
          mysql: 'CREATE TABLE x (id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (id))',
          sqlite: 'CREATE TABLE x (id INTEGER NOT NULL AUTOINCREMENT PRIMARY KEY)',
        },
      },
    );
    this.validateIdentity('ALTER TABLE t ALTER INDEX i INVISIBLE');
    this.validateIdentity('ALTER TABLE t ALTER INDEX i VISIBLE');
    this.validateIdentity('ALTER TABLE t ALTER COLUMN c SET INVISIBLE');
    this.validateIdentity('ALTER TABLE t ALTER COLUMN c SET VISIBLE');
    this.validateIdentity(
      'UPDATE foo JOIN bar ON TRUE SET foo.a = bar.a WHERE foo.id = bar.id',
    );

    // PARTITION BY RANGE - simple column
    this.validateIdentity(
      'CREATE TABLE t (id INT, created_at DATE) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN (MAXVALUE))',
    );
    this.validateIdentity(
      'CREATE TABLE t (id INT, name VARCHAR(50)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (100))',
    );
    // PARTITION BY RANGE - with expression
    this.validateIdentity(
      'CREATE TABLE orders (id INT, order_date DATE) PARTITION BY RANGE (YEAR(order_date)) (PARTITION p2023 VALUES LESS THAN (2024), PARTITION p2024 VALUES LESS THAN (2025), PARTITION pmax VALUES LESS THAN (MAXVALUE))',
    );
    this.validateIdentity(
      'CREATE TABLE sales (id INT, sale_date DATE) PARTITION BY RANGE (MONTH(sale_date)) (PARTITION q1 VALUES LESS THAN (4), PARTITION q2 VALUES LESS THAN (7), PARTITION q3 VALUES LESS THAN (10), PARTITION q4 VALUES LESS THAN (13))',
    );
    // PARTITION BY LIST - simple column
    this.validateIdentity(
      'CREATE TABLE t (id INT, region VARCHAR(10)) PARTITION BY LIST (id) (PARTITION p_east VALUES IN (1, 2, 3), PARTITION p_west VALUES IN (4, 5, 6))',
    );
    this.validateIdentity(
      'CREATE TABLE t (id INT) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1, 2))',
    );
    this.validateIdentity(
      'CREATE TABLE employees (id INT, store_id INT) PARTITION BY LIST (store_id) (PARTITION pNorth VALUES IN (3, 5, 6), PARTITION pSouth VALUES IN (1, 2, 10))',
    );
  }

  testIdentity () {
    this.validateIdentity('SELECT HIGH_PRIORITY STRAIGHT_JOIN SQL_CALC_FOUND_ROWS * FROM t');
    this.validateIdentity('SELECT CAST(COALESCE(`id`, \'NULL\') AS CHAR CHARACTER SET binary)');
    this.validateIdentity('SELECT e.* FROM e STRAIGHT_JOIN p ON e.x = p.y');
    this.validateIdentity('ALTER TABLE test_table ALTER COLUMN test_column SET DEFAULT 1');
    this.validateIdentity('SELECT DATE_FORMAT(NOW(), \'%Y-%m-%d %H:%i:00.0000\')');
    this.validateIdentity('SELECT @var1 := 1, @var2');
    this.validateIdentity('UNLOCK TABLES');
    this.validateIdentity('LOCK TABLES `app_fields` WRITE', undefined, { checkCommandWarning: true });
    this.validateIdentity('SELECT 1 XOR 0');
    this.validateIdentity('SELECT 1 && 0', 'SELECT 1 AND 0');
    this.validateIdentity('SELECT /*+ BKA(t1) NO_BKA(t2) */ * FROM t1 INNER JOIN t2');
    this.validateIdentity('SELECT /*+ MERGE(dt) */ * FROM (SELECT * FROM t1) AS dt');
    this.validateIdentity('SELECT /*+ INDEX(t, i) */ c1 FROM t WHERE c2 = \'value\'');
    this.validateIdentity('SELECT @a MEMBER OF(@c), @b MEMBER OF(@c)');
    this.validateIdentity('SELECT JSON_ARRAY(4, 5) MEMBER OF(\'[[3,4],[4,5]]\')');
    this.validateIdentity('SELECT CAST(\'[4,5]\' AS JSON) MEMBER OF(\'[[3,4],[4,5]]\')');
    this.validateIdentity('SELECT \'ab\' MEMBER OF(\'[23, "abc", 17, "ab", 10]\')');
    this.validateIdentity('SELECT * FROM foo WHERE \'ab\' MEMBER OF(content)');
    this.validateIdentity('SELECT CURRENT_TIMESTAMP(6)');
    this.validateIdentity('SELECT CURRENT_ROLE()');
    this.validateIdentity('SELECT CURTIME()', 'SELECT CURRENT_TIME()');
    this.validateIdentity('x ->> \'$.name\'');
    this.validateIdentity('SELECT CAST(`a`.`b` AS CHAR) FROM foo');
    this.validateIdentity('SELECT TRIM(LEADING \'bla\' FROM \' XXX \')');
    this.validateIdentity('SELECT TRIM(TRAILING \'bla\' FROM \' XXX \')');
    this.validateIdentity('SELECT TRIM(BOTH \'bla\' FROM \' XXX \')');
    this.validateIdentity('SELECT TRIM(\'bla\' FROM \' XXX \')');
    this.validateIdentity('@@GLOBAL.max_connections');
    this.validateIdentity('CREATE TABLE A LIKE B');
    this.validateIdentity('SELECT * FROM t1, t2 FOR SHARE OF t1, t2 SKIP LOCKED');
    this.validateIdentity('SELECT a || b', 'SELECT a OR b');
    this.validateIdentity(
      'SELECT * FROM source, JSON_TABLE(source.links, \'$.org[*]\' COLUMNS(row_id FOR ORDINALITY, link VARCHAR(255) PATH \'$.link\')) AS links',
    );
    this.validateIdentity(
      'SELECT * FROM x ORDER BY BINARY a', 'SELECT * FROM x ORDER BY CAST(a AS BINARY)',
    );
    this.validateIdentity(
      'SELECT * FROM foo WHERE 3 MEMBER OF(JSON_EXTRACT(info, \'$.value\'))',
    );
    this.validateIdentity(
      'SELECT * FROM t1, t2, t3 FOR SHARE OF t1 NOWAIT FOR UPDATE OF t2, t3 SKIP LOCKED',
    );
    this.validateIdentity(
      'REPLACE INTO table SELECT id FROM table2 WHERE cnt > 100',
      undefined,
      { checkCommandWarning: true },
    );
    this.validateIdentity(
      'CAST(x AS VARCHAR)',
      'CAST(x AS CHAR)',
    );
    this.validateIdentity(
      'SELECT * FROM foo WHERE 3 MEMBER OF(info->\'$.value\')',
      'SELECT * FROM foo WHERE 3 MEMBER OF(JSON_EXTRACT(info, \'$.value\'))',
    );
    this.validateIdentity(
      'SELECT 1 AS row',
      'SELECT 1 AS `row`',
    );

    // Index hints
    this.validateIdentity(
      'SELECT * FROM table1 USE INDEX (col1_index, col2_index) WHERE col1 = 1 AND col2 = 2 AND col3 = 3',
    );
    this.validateIdentity(
      'SELECT * FROM table1 IGNORE INDEX (col3_index) WHERE col1 = 1 AND col2 = 2 AND col3 = 3',
    );
    this.validateIdentity(
      'SELECT * FROM t1 USE INDEX (i1) IGNORE INDEX FOR ORDER BY (i2) ORDER BY a',
    );
    this.validateIdentity('SELECT * FROM t1 USE INDEX (i1) USE INDEX (i1, i1)');
    this.validateIdentity('SELECT * FROM t1 USE INDEX FOR JOIN (i1) FORCE INDEX FOR JOIN (i2)');
    this.validateIdentity(
      'SELECT * FROM t1 USE INDEX () IGNORE INDEX (i2) USE INDEX (i1) USE INDEX (i2)',
    );

    // SET Commands
    this.validateIdentity('SET @var_name = expr');
    this.validateIdentity('SET @name = 43');
    this.validateIdentity('SET @total_tax = (SELECT SUM(tax) FROM taxable_transactions)');
    this.validateIdentity('SET GLOBAL max_connections = 1000');
    this.validateIdentity('SET @@GLOBAL.max_connections = 1000');
    this.validateIdentity('SET SESSION sql_mode = \'TRADITIONAL\'');
    this.validateIdentity('SET LOCAL sql_mode = \'TRADITIONAL\'');
    this.validateIdentity('SET @@SESSION.sql_mode = \'TRADITIONAL\'');
    this.validateIdentity('SET @@LOCAL.sql_mode = \'TRADITIONAL\'');
    this.validateIdentity('SET @@sql_mode = \'TRADITIONAL\'');
    this.validateIdentity('SET sql_mode = \'TRADITIONAL\'');
    this.validateIdentity('SET PERSIST max_connections = 1000');
    this.validateIdentity('SET @@PERSIST.max_connections = 1000');
    this.validateIdentity('SET PERSIST_ONLY back_log = 100');
    this.validateIdentity('SET @@PERSIST_ONLY.back_log = 100');
    this.validateIdentity('SET @@SESSION.max_join_size = DEFAULT');
    this.validateIdentity('SET @@SESSION.max_join_size = @@GLOBAL.max_join_size');
    this.validateIdentity('SET @x = 1, SESSION sql_mode = \'\'');
    this.validateIdentity('SET GLOBAL max_connections = 1000, sort_buffer_size = 1000000');
    this.validateIdentity('SET @@GLOBAL.sort_buffer_size = 50000, sort_buffer_size = 1000000');
    this.validateIdentity('SET CHARACTER SET \'utf8\'');
    this.validateIdentity('SET CHARACTER SET utf8');
    this.validateIdentity('SET CHARACTER SET DEFAULT');
    this.validateIdentity('SET NAMES \'utf8\'');
    this.validateIdentity('SET NAMES DEFAULT');
    this.validateIdentity('SET NAMES \'utf8\' COLLATE \'utf8_unicode_ci\'');
    this.validateIdentity('SET NAMES utf8 COLLATE utf8_unicode_ci');
    this.validateIdentity('SET autocommit = ON');
    this.validateIdentity('SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE');
    this.validateIdentity('SET TRANSACTION READ ONLY');
    this.validateIdentity('SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ WRITE');
    this.validateIdentity('DATABASE()', 'SCHEMA()');
    this.validateIdentity(
      'SET GLOBAL sort_buffer_size = 1000000, SESSION sort_buffer_size = 1000000',
    );
    this.validateIdentity(
      'SET @@GLOBAL.sort_buffer_size = 1000000, @@LOCAL.sort_buffer_size = 1000000',
    );
    this.validateIdentity('INTERVAL \'1\' YEAR');
    this.validateIdentity('DATE_ADD(x, INTERVAL \'1\' YEAR)');
    this.validateIdentity('CHAR(0)');
    this.validateIdentity('CHAR(77, 121, 83, 81, \'76\')');
    this.validateIdentity('CHAR(77, 77.3, \'77.3\' USING utf8mb4)');
    this.validateIdentity('SELECT * FROM t1 PARTITION(p0)');
    this.validateIdentity('SELECT @var1 := 1, @var2');
    this.validateIdentity('SELECT @var1, @var2 := @var1');
    this.validateIdentity('SELECT @var1 := COUNT(*) FROM t1');
    this.validateIdentity('SET @var1 := 1', 'SET @var1 = 1');

    this.validateIdentity(
      'SELECT DISTINCTROW tbl.col FROM tbl', 'SELECT DISTINCT tbl.col FROM tbl',
    );
    this.validateIdentity('ATAN(y, x)');

    this.validateIdentity(
      'SELECT \'foo\' SOUNDS LIKE \'bar\'', 'SELECT SOUNDEX(\'foo\') = SOUNDEX(\'bar\')',
    );
    this.validateIdentity(
      'SELECT \'foo\' NOT SOUNDS LIKE \'bar\'', 'SELECT NOT SOUNDEX(\'foo\') = SOUNDEX(\'bar\')',
    );
    this.validateIdentity('SELECT SUBSTR(1 FROM 2 FOR 3)', 'SELECT SUBSTRING(1, 2, 3)');
    this.validateIdentity('SELECT ELT(2, \'foo\', \'bar\', \'baz\') AS Result');
    this.validateIdentity('SELECT CHARSET(CHAR(100 USING utf8))');
    this.validateIdentity('SELECT VERSION()');
  }

  testTypes () {
    for (const charType of Object.keys(MySQL.Generator.CHAR_CAST_MAPPING)) {
      this.validateIdentity(`CAST(x AS ${charType.toUpperCase()})`, 'CAST(x AS CHAR)');
    }

    for (const signedType of Object.keys(MySQL.Generator.SIGNED_CAST_MAPPING)) {
      this.validateIdentity(`CAST(x AS ${signedType.toUpperCase()})`, 'CAST(x AS SIGNED)');
    }

    this.validateIdentity('CAST(x AS ENUM(\'a\', \'b\'))');
    this.validateIdentity('CAST(x AS SET(\'a\', \'b\'))');
    this.validateIdentity(
      'CAST(x AS MEDIUMINT) + CAST(y AS YEAR(4))',
      'CAST(x AS SIGNED) + CAST(y AS YEAR(4))',
    );
    this.validateIdentity(
      'CAST(x AS TIMESTAMP)',
      'TIMESTAMP(x)',
    );
    this.validateIdentity(
      'CAST(x AS TIMESTAMPTZ)',
      'TIMESTAMP(x)',
    );
    this.validateIdentity(
      'CAST(x AS TIMESTAMPLTZ)',
      'TIMESTAMP(x)',
    );

    this.validateAll(
      'CAST(x AS MEDIUMTEXT) + CAST(y AS LONGTEXT) + CAST(z AS TINYTEXT)',
      {
        write: {
          mysql: 'CAST(x AS CHAR) + CAST(y AS CHAR) + CAST(z AS CHAR)',
          spark: 'CAST(x AS TEXT) + CAST(y AS TEXT) + CAST(z AS TEXT)',
        },
      },
    );
    this.validateAll(
      'CAST(x AS MEDIUMBLOB) + CAST(y AS LONGBLOB) + CAST(z AS TINYBLOB)',
      {
        write: {
          mysql: 'CAST(x AS CHAR) + CAST(y AS CHAR) + CAST(z AS CHAR)',
          spark: 'CAST(x AS BLOB) + CAST(y AS BLOB) + CAST(z AS BLOB)',
        },
      },
    );
  }

  testCanonicalFunctions () {
    this.validateIdentity('SELECT LEFT(\'str\', 2)', 'SELECT LEFT(\'str\', 2)');
    this.validateIdentity('SELECT INSTR(\'str\', \'substr\')', 'SELECT LOCATE(\'substr\', \'str\')');
    this.validateIdentity('SELECT UCASE(\'foo\')', 'SELECT UPPER(\'foo\')');
    this.validateIdentity('SELECT LCASE(\'foo\')', 'SELECT LOWER(\'foo\')');
    this.validateIdentity(
      'SELECT DAY_OF_MONTH(\'2023-01-01\')', 'SELECT DAYOFMONTH(\'2023-01-01\')',
    );
    this.validateIdentity('SELECT DAY_OF_WEEK(\'2023-01-01\')', 'SELECT DAYOFWEEK(\'2023-01-01\')');
    this.validateIdentity('SELECT DAY_OF_YEAR(\'2023-01-01\')', 'SELECT DAYOFYEAR(\'2023-01-01\')');
    this.validateIdentity(
      'SELECT WEEK_OF_YEAR(\'2023-01-01\')', 'SELECT WEEKOFYEAR(\'2023-01-01\')',
    );
    this.validateAll(
      'CHAR(10)',
      {
        write: {
          mysql: 'CHAR(10)',
          presto: 'CHR(10)',
          sqlite: 'CHAR(10)',
          tsql: 'CHAR(10)',
        },
      },
    );
    this.validateIdentity('CREATE TABLE t (foo VARBINARY(5))');
    this.validateAll(
      'CREATE TABLE t (foo BLOB)',
      {
        write: {
          mysql: 'CREATE TABLE t (foo BLOB)',
          oracle: 'CREATE TABLE t (foo BLOB)',
          postgres: 'CREATE TABLE t (foo BYTEA)',
          tsql: 'CREATE TABLE t (foo VARBINARY)',
          sqlite: 'CREATE TABLE t (foo BLOB)',
          duckdb: 'CREATE TABLE t (foo VARBINARY)',
          hive: 'CREATE TABLE t (foo BINARY)',
          bigquery: 'CREATE TABLE t (foo BYTES)',
          redshift: 'CREATE TABLE t (foo VARBYTE)',
          clickhouse: 'CREATE TABLE t (foo Nullable(String))',
        },
      },
    );
  }

  testEscape () {
    this.validateIdentity('\'"abc"\'');
    this.validateIdentity(
      '\'\\\'a\'',
      '\'\'\'a\'',
    );
    this.validateIdentity(
      '"\'abc\'"',
      '\'\'\'abc\'\'\'',
    );
    this.validateAll(
      '\'a \\\' b \'\' \'',
      {
        write: {
          mysql: '\'a \'\' b \'\' \'',
          spark: '\'a \\\' b \\\' \'',
        },
      },
    );

    this.validateIdentity(
      '\'\\"\'',
      '\'"\'',
    );
    this.validateIdentity('\'\\\\"a\'');
    this.validateIdentity(
      '\'\\t\'',
      '\'\\t\'',
    );
    this.validateIdentity(
      '\'\\j\'',
      '\'j\'',
    );
  }

  testIntroducers () {
    this.validateAll(
      '_utf8mb4 \'hola\'',
      {
        read: {
          mysql: '_utf8mb4\'hola\'',
        },
        write: {
          mysql: '_utf8mb4 \'hola\'',
        },
      },
    );
    this.validateAll(
      'N\'some text\'',
      {
        read: {
          mysql: 'n\'some text\'',
        },
        write: {
          mysql: 'N\'some text\'',
        },
      },
    );
    this.validateAll(
      '_latin1 x\'4D7953514C\'',
      {
        read: {
          mysql: '_latin1 X\'4D7953514C\'',
        },
        write: {
          mysql: '_latin1 x\'4D7953514C\'',
        },
      },
    );
  }

  testHexadecimalLiteral () {
    const writeCC: Record<string, string | typeof UnsupportedError> = {
      bigquery: 'SELECT FROM_HEX(\'CC\')',
      clickhouse: UnsupportedError,
      databricks: 'SELECT X\'CC\'',
      drill: 'SELECT 204',
      duckdb: 'SELECT UNHEX(\'CC\')',
      hive: 'SELECT 204',
      mysql: 'SELECT x\'CC\'',
      oracle: 'SELECT 204',
      postgres: 'SELECT x\'CC\'',
      presto: 'SELECT x\'CC\'',
      redshift: 'SELECT 204',
      snowflake: 'SELECT x\'CC\'',
      spark: 'SELECT X\'CC\'',
      sqlite: 'SELECT x\'CC\'',
      starrocks: 'SELECT x\'CC\'',
      tableau: 'SELECT 204',
      teradata: 'SELECT X\'CC\'',
      trino: 'SELECT x\'CC\'',
      tsql: 'SELECT 0xCC',
    };
    const writeCCWithLeadingZeros: Record<string, string | typeof UnsupportedError> = {
      bigquery: 'SELECT FROM_HEX(\'0000CC\')',
      clickhouse: UnsupportedError,
      databricks: 'SELECT X\'0000CC\'',
      drill: 'SELECT 204',
      duckdb: 'SELECT UNHEX(\'0000CC\')',
      hive: 'SELECT 204',
      mysql: 'SELECT x\'0000CC\'',
      oracle: 'SELECT 204',
      postgres: 'SELECT x\'0000CC\'',
      presto: 'SELECT x\'0000CC\'',
      redshift: 'SELECT 204',
      snowflake: 'SELECT x\'0000CC\'',
      spark: 'SELECT X\'0000CC\'',
      sqlite: 'SELECT x\'0000CC\'',
      starrocks: 'SELECT x\'0000CC\'',
      tableau: 'SELECT 204',
      teradata: 'SELECT X\'0000CC\'',
      trino: 'SELECT x\'0000CC\'',
      tsql: 'SELECT 0x0000CC',
    };

    this.validateAll('SELECT X\'1A\'', { write: { mysql: 'SELECT x\'1A\'' } });
    this.validateAll('SELECT 0xz', { write: { mysql: 'SELECT `0xz`' } });
    this.validateAll('SELECT 0xCC', { write: writeCC });
    this.validateAll('SELECT 0xCC ', { write: writeCC });
    this.validateAll('SELECT x\'CC\'', { write: writeCC });
    this.validateAll('SELECT 0x0000CC', { write: writeCCWithLeadingZeros });
    this.validateAll('SELECT x\'0000CC\'', { write: writeCCWithLeadingZeros });
  }

  testBitsLiteral () {
    const write1011: Record<string, string> = {
      bigquery: 'SELECT 11',
      clickhouse: 'SELECT 0b1011',
      databricks: 'SELECT 11',
      drill: 'SELECT 11',
      hive: 'SELECT 11',
      mysql: 'SELECT b\'1011\'',
      oracle: 'SELECT 11',
      postgres: 'SELECT b\'1011\'',
      presto: 'SELECT 11',
      redshift: 'SELECT 11',
      snowflake: 'SELECT 11',
      spark: 'SELECT 11',
      sqlite: 'SELECT 11',
      tableau: 'SELECT 11',
      teradata: 'SELECT 11',
      trino: 'SELECT 11',
      tsql: 'SELECT 11',
    };

    this.validateAll('SELECT 0b1011', { write: write1011 });
    this.validateAll('SELECT b\'1011\'', { write: write1011 });
  }

  testStringLiterals () {
    this.validateAll(
      'SELECT "2021-01-01" + INTERVAL 1 MONTH',
      {
        write: {
          mysql: 'SELECT \'2021-01-01\' + INTERVAL \'1\' MONTH',
        },
      },
    );
  }

  testConvert () {
    this.validateAll(
      'CONVERT(x USING latin1)',
      {
        write: {
          mysql: 'CAST(x AS CHAR CHARACTER SET latin1)',
        },
      },
    );
    this.validateAll(
      'CAST(x AS CHAR CHARACTER SET latin1)',
      {
        write: {
          mysql: 'CAST(x AS CHAR CHARACTER SET latin1)',
        },
      },
    );
    this.validateIdentity(
      'CONVERT(\'a\' USING binary)', 'CAST(\'a\' AS CHAR CHARACTER SET binary)',
    );
  }

  testMatchAgainst () {
    this.validateAll(
      'MATCH(col1, col2, col3) AGAINST(\'abc\')',
      {
        read: {
          '': 'MATCH(col1, col2, col3) AGAINST(\'abc\')',
          mysql: 'MATCH(col1, col2, col3) AGAINST(\'abc\')',
        },
        write: {
          '': 'MATCH(col1, col2, col3) AGAINST(\'abc\')',
          mysql: 'MATCH(col1, col2, col3) AGAINST(\'abc\')',
          postgres: '(col1 @@ \'abc\' OR col2 @@ \'abc\' OR col3 @@ \'abc\')',
        },
      },
    );
    this.validateAll(
      'MATCH(col1, col2) AGAINST(\'abc\' IN NATURAL LANGUAGE MODE)',
      { write: { mysql: 'MATCH(col1, col2) AGAINST(\'abc\' IN NATURAL LANGUAGE MODE)' } },
    );
    this.validateAll(
      'MATCH(col1, col2) AGAINST(\'abc\' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)',
      {
        write: {
          mysql: 'MATCH(col1, col2) AGAINST(\'abc\' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)',
        },
      },
    );
    this.validateAll(
      'MATCH(col1, col2) AGAINST(\'abc\' IN BOOLEAN MODE)',
      { write: { mysql: 'MATCH(col1, col2) AGAINST(\'abc\' IN BOOLEAN MODE)' } },
    );
    this.validateAll(
      'MATCH(col1, col2) AGAINST(\'abc\' WITH QUERY EXPANSION)',
      { write: { mysql: 'MATCH(col1, col2) AGAINST(\'abc\' WITH QUERY EXPANSION)' } },
    );
    this.validateAll(
      'MATCH(a.b) AGAINST(\'abc\')',
      { write: { mysql: 'MATCH(a.b) AGAINST(\'abc\')' } },
    );
  }

  testDateFormat () {
    this.validateAll(
      'SELECT DATE_FORMAT(\'2017-06-15\', \'%Y\')',
      {
        write: {
          mysql: 'SELECT DATE_FORMAT(\'2017-06-15\', \'%Y\')',
          snowflake: 'SELECT TO_CHAR(CAST(\'2017-06-15\' AS TIMESTAMP), \'yyyy\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_FORMAT(\'2017-06-15\', \'%m\')',
      {
        write: {
          mysql: 'SELECT DATE_FORMAT(\'2017-06-15\', \'%m\')',
          snowflake: 'SELECT TO_CHAR(CAST(\'2017-06-15\' AS TIMESTAMP), \'mm\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_FORMAT(\'2017-06-15\', \'%d\')',
      {
        write: {
          mysql: 'SELECT DATE_FORMAT(\'2017-06-15\', \'%d\')',
          snowflake: 'SELECT TO_CHAR(CAST(\'2017-06-15\' AS TIMESTAMP), \'DD\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_FORMAT(\'2017-06-15\', \'%Y-%m-%d\')',
      {
        write: {
          mysql: 'SELECT DATE_FORMAT(\'2017-06-15\', \'%Y-%m-%d\')',
          snowflake: 'SELECT TO_CHAR(CAST(\'2017-06-15\' AS TIMESTAMP), \'yyyy-mm-DD\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_FORMAT(\'2017-06-15 22:23:34\', \'%H\')',
      {
        write: {
          mysql: 'SELECT DATE_FORMAT(\'2017-06-15 22:23:34\', \'%H\')',
          snowflake: 'SELECT TO_CHAR(CAST(\'2017-06-15 22:23:34\' AS TIMESTAMP), \'hh24\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_FORMAT(\'2017-06-15\', \'%w\')',
      {
        write: {
          mysql: 'SELECT DATE_FORMAT(\'2017-06-15\', \'%w\')',
          snowflake: 'SELECT TO_CHAR(CAST(\'2017-06-15\' AS TIMESTAMP), \'dy\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_FORMAT(\'2024-08-22 14:53:12\', \'%a\')',
      {
        write: {
          mysql: 'SELECT DATE_FORMAT(\'2024-08-22 14:53:12\', \'%a\')',
          snowflake: 'SELECT TO_CHAR(CAST(\'2024-08-22 14:53:12\' AS TIMESTAMP), \'DY\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_FORMAT(\'2009-10-04 22:23:00\', \'%a %M %Y\')',
      {
        write: {
          mysql: 'SELECT DATE_FORMAT(\'2009-10-04 22:23:00\', \'%a %M %Y\')',
          snowflake: 'SELECT TO_CHAR(CAST(\'2009-10-04 22:23:00\' AS TIMESTAMP), \'DY mmmm yyyy\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_FORMAT(\'2007-10-04 22:23:00\', \'%H:%i:%s\')',
      {
        write: {
          mysql: 'SELECT DATE_FORMAT(\'2007-10-04 22:23:00\', \'%T\')',
          snowflake: 'SELECT TO_CHAR(CAST(\'2007-10-04 22:23:00\' AS TIMESTAMP), \'hh24:mi:ss\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_FORMAT(\'1900-10-04 22:23:00\', \'%d %y %a %d %m %b\')',
      {
        write: {
          mysql: 'SELECT DATE_FORMAT(\'1900-10-04 22:23:00\', \'%d %y %a %d %m %b\')',
          snowflake: 'SELECT TO_CHAR(CAST(\'1900-10-04 22:23:00\' AS TIMESTAMP), \'DD yy DY DD mm mon\')',
        },
      },
    );
  }

  testMysqlTime () {
    this.validateIdentity('TIME_STR_TO_UNIX(x)', 'UNIX_TIMESTAMP(x)');
    this.validateIdentity('SELECT FROM_UNIXTIME(1711366265, \'%Y %D %M\')');
    this.validateAll(
      'SELECT TO_DAYS(x)',
      {
        write: {
          mysql: 'SELECT (DATEDIFF(x, \'0000-01-01\') + 1)',
          presto: 'SELECT (DATE_DIFF(\'DAY\', CAST(CAST(\'0000-01-01\' AS TIMESTAMP) AS DATE), CAST(CAST(x AS TIMESTAMP) AS DATE)) + 1)',
        },
      },
    );
    this.validateAll(
      'SELECT DATEDIFF(x, y)',
      {
        read: {
          presto: 'SELECT DATE_DIFF(\'DAY\', y, x)',
          redshift: 'SELECT DATEDIFF(DAY, y, x)',
        },
        write: {
          mysql: 'SELECT DATEDIFF(x, y)',
          presto: 'SELECT DATE_DIFF(\'DAY\', y, x)',
          redshift: 'SELECT DATEDIFF(DAY, y, x)',
        },
      },
    );
    this.validateAll(
      'DAYOFYEAR(x)',
      {
        write: {
          mysql: 'DAYOFYEAR(x)',
          '': 'DAY_OF_YEAR(CAST(x AS DATE))',
        },
      },
    );
    this.validateAll(
      'DAYOFMONTH(x)',
      { write: { mysql: 'DAYOFMONTH(x)', '': 'DAY_OF_MONTH(CAST(x AS DATE))' } },
    );
    this.validateAll(
      'DAYOFWEEK(x)',
      { write: { mysql: 'DAYOFWEEK(x)', '': 'DAY_OF_WEEK(CAST(x AS DATE))' } },
    );
    this.validateAll(
      'WEEKOFYEAR(x)',
      { write: { mysql: 'WEEKOFYEAR(x)', '': 'WEEK_OF_YEAR(CAST(x AS DATE))' } },
    );
    this.validateAll(
      'DAY(x)',
      { write: { mysql: 'DAY(x)', '': 'DAY(CAST(x AS DATE))' } },
    );
    this.validateAll(
      'WEEK(x)',
      { write: { mysql: 'WEEK(x)', '': 'WEEK(CAST(x AS DATE))' } },
    );
    this.validateAll(
      'YEAR(x)',
      { write: { mysql: 'YEAR(x)', '': 'YEAR(CAST(x AS DATE))' } },
    );
    this.validateAll(
      'DATE(x)',
      { read: { '': 'TS_OR_DS_TO_DATE(x)' } },
    );
    this.validateAll(
      'STR_TO_DATE(x, \'%M\')',
      { read: { '': 'TS_OR_DS_TO_DATE(x, \'%B\')' } },
    );
    this.validateAll(
      'STR_TO_DATE(x, \'%Y-%m-%d\')',
      { write: { presto: 'CAST(DATE_PARSE(x, \'%Y-%m-%d\') AS DATE)' } },
    );
    this.validateAll(
      'STR_TO_DATE(x, \'%Y-%m-%dT%T\')',
      { write: { presto: 'DATE_PARSE(x, \'%Y-%m-%dT%T\')' } },
    );
    this.validateAll(
      'SELECT FROM_UNIXTIME(col)',
      {
        read: {
          postgres: 'SELECT TO_TIMESTAMP(col)',
        },
        write: {
          mysql: 'SELECT FROM_UNIXTIME(col)',
          postgres: 'SELECT TO_TIMESTAMP(col)',
          redshift: 'SELECT (TIMESTAMP \'epoch\' + col * INTERVAL \'1 SECOND\')',
        },
      },
    );

    // No timezone, make sure DATETIME captures the correct precision
    this.validateIdentity(
      'SELECT TIME_STR_TO_TIME(\'2023-01-01 13:14:15.123456+00:00\')',
      'SELECT CAST(\'2023-01-01 13:14:15.123456+00:00\' AS DATETIME(6))',
    );
    this.validateIdentity(
      'SELECT TIME_STR_TO_TIME(\'2023-01-01 13:14:15.123+00:00\')',
      'SELECT CAST(\'2023-01-01 13:14:15.123+00:00\' AS DATETIME(3))',
    );
    this.validateIdentity(
      'SELECT TIME_STR_TO_TIME(\'2023-01-01 13:14:15+00:00\')',
      'SELECT CAST(\'2023-01-01 13:14:15+00:00\' AS DATETIME)',
    );

    // With timezone, make sure the TIMESTAMP constructor is used
    // also TIMESTAMP doesnt have the subsecond precision truncation issue that DATETIME does so we dont need to TIMESTAMP(6)
    this.validateIdentity(
      'SELECT TIME_STR_TO_TIME(\'2023-01-01 13:14:15-08:00\', \'America/Los_Angeles\')',
      'SELECT TIMESTAMP(\'2023-01-01 13:14:15-08:00\')',
    );
    this.validateIdentity(
      'SELECT TIME_STR_TO_TIME(\'2023-01-01 13:14:15-08:00\', \'America/Los_Angeles\')',
      'SELECT TIMESTAMP(\'2023-01-01 13:14:15-08:00\')',
    );
  }

  testMysqlTimePython311 () {
    this.validateIdentity(
      'SELECT TIME_STR_TO_TIME(\'2023-01-01 13:14:15.12345+00:00\')',
      'SELECT CAST(\'2023-01-01 13:14:15.12345+00:00\' AS DATETIME(6))',
    );
    this.validateIdentity(
      'SELECT TIME_STR_TO_TIME(\'2023-01-01 13:14:15.1234+00:00\')',
      'SELECT CAST(\'2023-01-01 13:14:15.1234+00:00\' AS DATETIME(6))',
    );
    this.validateIdentity(
      'SELECT TIME_STR_TO_TIME(\'2023-01-01 13:14:15.12+00:00\')',
      'SELECT CAST(\'2023-01-01 13:14:15.12+00:00\' AS DATETIME(3))',
    );
    this.validateIdentity(
      'SELECT TIME_STR_TO_TIME(\'2023-01-01 13:14:15.1+00:00\')',
      'SELECT CAST(\'2023-01-01 13:14:15.1+00:00\' AS DATETIME(3))',
    );
  }

  testMysql () {
    for (const func of ['CHAR_LENGTH', 'CHARACTER_LENGTH']) {
      this.validateAll(
        `SELECT ${func}('foo')`,
        {
          write: {
            duckdb: 'SELECT LENGTH(\'foo\')',
            mysql: 'SELECT CHAR_LENGTH(\'foo\')',
            postgres: 'SELECT LENGTH(\'foo\')',
          },
        },
      );
    }

    this.validateAll(
      'CURDATE()',
      {
        write: {
          mysql: 'CURRENT_DATE',
          postgres: 'CURRENT_DATE',
        },
      },
    );
    this.validateAll(
      'SELECT CONCAT(\'11\', \'22\')',
      {
        read: {
          postgres: 'SELECT \'11\' || \'22\'',
        },
        write: {
          mysql: 'SELECT CONCAT(\'11\', \'22\')',
          postgres: 'SELECT \'11\' || \'22\'',
        },
      },
    );
    this.validateAll(
      'SELECT department, GROUP_CONCAT(name) AS employee_names FROM data GROUP BY department',
      {
        read: {
          postgres: 'SELECT department, array_agg(name) AS employee_names FROM data GROUP BY department',
        },
      },
    );
    this.validateAll(
      'SELECT UNIX_TIMESTAMP(CAST(\'2024-04-29 12:00:00\' AS DATETIME))',
      {
        read: {
          mysql: 'SELECT UNIX_TIMESTAMP(CAST(\'2024-04-29 12:00:00\' AS DATETIME))',
          postgres: 'SELECT EXTRACT(epoch FROM TIMESTAMP \'2024-04-29 12:00:00\')',
        },
      },
    );
    this.validateAll(
      'SELECT JSON_EXTRACT(\'[10, 20, [30, 40]]\', \'$[1]\')',
      {
        read: {
          sqlite: 'SELECT JSON_EXTRACT(\'[10, 20, [30, 40]]\', \'$[1]\')',
        },
        write: {
          mysql: 'SELECT JSON_EXTRACT(\'[10, 20, [30, 40]]\', \'$[1]\')',
          sqlite: 'SELECT \'[10, 20, [30, 40]]\' -> \'$[1]\'',
        },
      },
    );
    this.validateAll(
      'SELECT JSON_EXTRACT(\'[10, 20, [30, 40]]\', \'$[1]\', \'$[0]\')',
      {
        read: {
          sqlite: 'SELECT JSON_EXTRACT(\'[10, 20, [30, 40]]\', \'$[1]\', \'$[0]\')',
        },
        write: {
          mysql: 'SELECT JSON_EXTRACT(\'[10, 20, [30, 40]]\', \'$[1]\', \'$[0]\')',
          sqlite: 'SELECT JSON_EXTRACT(\'[10, 20, [30, 40]]\', \'$[1]\', \'$[0]\')',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM x LEFT JOIN y ON x.id = y.id UNION ALL SELECT * FROM x RIGHT JOIN y ON x.id = y.id WHERE NOT EXISTS(SELECT 1 FROM x WHERE x.id = y.id) ORDER BY 1 LIMIT 0',
      {
        read: {
          postgres: 'SELECT * FROM x FULL JOIN y ON x.id = y.id ORDER BY 1 LIMIT 0',
        },
      },
    );
    this.validateAll(
      // MySQL doesn't support FULL OUTER joins
      'SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.x = t2.x UNION ALL SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.x = t2.x WHERE NOT EXISTS(SELECT 1 FROM t1 WHERE t1.x = t2.x)',
      {
        read: {
          postgres: 'SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.x = t2.x',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM t1 LEFT OUTER JOIN t2 USING (x) UNION ALL SELECT * FROM t1 RIGHT OUTER JOIN t2 USING (x) WHERE NOT EXISTS(SELECT 1 FROM t1 WHERE t1.x = t2.x)',
      {
        read: {
          postgres: 'SELECT * FROM t1 FULL OUTER JOIN t2 USING (x) ',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM t1 LEFT OUTER JOIN t2 USING (x, y) UNION ALL SELECT * FROM t1 RIGHT OUTER JOIN t2 USING (x, y) WHERE NOT EXISTS(SELECT 1 FROM t1 WHERE t1.x = t2.x AND t1.y = t2.y)',
      {
        read: {
          postgres: 'SELECT * FROM t1 FULL OUTER JOIN t2 USING (x, y) ',
        },
      },
    );
    this.validateAll(
      'a XOR b',
      {
        read: {
          mysql: 'a XOR b',
          snowflake: 'BOOLXOR(a, b)',
        },
        write: {
          duckdb: '(a AND (NOT b)) OR ((NOT a) AND b)',
          mysql: 'a XOR b',
          postgres: '(a AND (NOT b)) OR ((NOT a) AND b)',
          snowflake: 'BOOLXOR(a, b)',
          trino: '(a AND (NOT b)) OR ((NOT a) AND b)',
        },
      },
    );

    this.validateAll(
      'SELECT * FROM test LIMIT 0 + 1, 0 + 1',
      {
        write: {
          mysql: 'SELECT * FROM test LIMIT 1 OFFSET 1',
          postgres: 'SELECT * FROM test LIMIT 0 + 1 OFFSET 0 + 1',
          presto: 'SELECT * FROM test OFFSET 1 LIMIT 1',
          snowflake: 'SELECT * FROM test LIMIT 1 OFFSET 1',
          trino: 'SELECT * FROM test OFFSET 1 LIMIT 1',
          bigquery: 'SELECT * FROM test LIMIT 1 OFFSET 1',
        },
      },
    );
    this.validateAll(
      'CAST(x AS TEXT)',
      {
        write: {
          mysql: 'CAST(x AS CHAR)',
          presto: 'CAST(x AS VARCHAR)',
          starrocks: 'CAST(x AS STRING)',
        },
      },
    );
    this.validateAll('CAST(x AS SIGNED)', { write: { mysql: 'CAST(x AS SIGNED)' } });
    this.validateAll('CAST(x AS SIGNED INTEGER)', { write: { mysql: 'CAST(x AS SIGNED)' } });
    this.validateAll('CAST(x AS UNSIGNED)', { write: { mysql: 'CAST(x AS UNSIGNED)' } });
    this.validateAll('CAST(x AS UNSIGNED INTEGER)', { write: { mysql: 'CAST(x AS UNSIGNED)' } });
    this.validateAll('TIME_STR_TO_TIME(x)', { write: { mysql: 'CAST(x AS DATETIME)' } });
    this.validateAll(
      'SELECT 17 MEMBER OF(\'[23, "abc", 17, "ab", 10]\')',
      {
        write: {
          '': 'SELECT JSON_ARRAY_CONTAINS(17, \'[23, "abc", 17, "ab", 10]\')',
          mysql: 'SELECT 17 MEMBER OF(\'[23, "abc", 17, "ab", 10]\')',
        },
      },
    );
    this.validateAll(
      'SELECT DATE_ADD(\'2023-06-23 12:00:00\', INTERVAL 2 * 2 MONTH) FROM foo',
      {
        write: {
          mysql: 'SELECT DATE_ADD(\'2023-06-23 12:00:00\', INTERVAL (2 * 2) MONTH) FROM foo',
        },
      },
    );
    this.validateAll(
      'SELECT * FROM t LOCK IN SHARE MODE',
      { write: { mysql: 'SELECT * FROM t FOR SHARE' } },
    );
    this.validateAll(
      'SELECT DATE(DATE_SUB(`dt`, INTERVAL DAYOFMONTH(`dt`) - 1 DAY)) AS __timestamp FROM tableT',
      {
        write: {
          mysql: 'SELECT DATE(DATE_SUB(`dt`, INTERVAL (DAYOFMONTH(`dt`) - 1) DAY)) AS __timestamp FROM tableT',
        },
      },
    );
    this.validateIdentity('SELECT name FROM temp WHERE name = ? FOR UPDATE');
    this.validateAll(
      'SELECT a FROM tbl FOR UPDATE',
      {
        write: {
          '': 'SELECT a FROM tbl',
          mysql: 'SELECT a FROM tbl FOR UPDATE',
          oracle: 'SELECT a FROM tbl FOR UPDATE',
          postgres: 'SELECT a FROM tbl FOR UPDATE',
          redshift: 'SELECT a FROM tbl',
          tsql: 'SELECT a FROM tbl',
        },
      },
    );
    this.validateAll(
      'SELECT a FROM tbl FOR SHARE',
      {
        write: {
          '': 'SELECT a FROM tbl',
          mysql: 'SELECT a FROM tbl FOR SHARE',
          oracle: 'SELECT a FROM tbl FOR SHARE',
          postgres: 'SELECT a FROM tbl FOR SHARE',
          tsql: 'SELECT a FROM tbl',
        },
      },
    );
    this.validateAll(
      'GROUP_CONCAT(DISTINCT x ORDER BY y DESC)',
      {
        write: {
          mysql: 'GROUP_CONCAT(DISTINCT x ORDER BY y DESC SEPARATOR \',\')',
          sqlite: 'GROUP_CONCAT(DISTINCT x)',
          tsql: 'STRING_AGG(x, \',\') WITHIN GROUP (ORDER BY y DESC)',
          databricks: 'LISTAGG(DISTINCT x, \',\') WITHIN GROUP (ORDER BY y DESC)',
          postgres: 'STRING_AGG(DISTINCT x, \',\' ORDER BY y DESC NULLS LAST)',
        },
      },
    );
    this.validateAll(
      'GROUP_CONCAT(x ORDER BY y SEPARATOR z)',
      {
        write: {
          mysql: 'GROUP_CONCAT(x ORDER BY y SEPARATOR z)',
          sqlite: 'GROUP_CONCAT(x, z)',
          tsql: 'STRING_AGG(x, z) WITHIN GROUP (ORDER BY y)',
          databricks: 'LISTAGG(x, z) WITHIN GROUP (ORDER BY y)',
          postgres: 'STRING_AGG(x, z ORDER BY y NULLS FIRST)',
        },
      },
    );
    this.validateAll(
      'GROUP_CONCAT(DISTINCT x ORDER BY y DESC SEPARATOR \'\')',
      {
        write: {
          mysql: 'GROUP_CONCAT(DISTINCT x ORDER BY y DESC SEPARATOR \'\')',
          sqlite: 'GROUP_CONCAT(DISTINCT x, \'\')',
          tsql: 'STRING_AGG(x, \'\') WITHIN GROUP (ORDER BY y DESC)',
          databricks: 'LISTAGG(DISTINCT x, \'\') WITHIN GROUP (ORDER BY y DESC)',
          postgres: 'STRING_AGG(DISTINCT x, \'\' ORDER BY y DESC NULLS LAST)',
        },
      },
    );
    this.validateAll(
      'GROUP_CONCAT(a, b, c SEPARATOR \',\')',
      {
        write: {
          mysql: 'GROUP_CONCAT(CONCAT(a, b, c) SEPARATOR \',\')',
          sqlite: 'GROUP_CONCAT(a || b || c, \',\')',
          tsql: 'STRING_AGG(a + b + c, \',\')',
          postgres: 'STRING_AGG(a || b || c, \',\')',
          databricks: 'LISTAGG(CONCAT(a, b, c), \',\')',
          presto: 'ARRAY_JOIN(ARRAY_AGG(CONCAT(CAST(a AS VARCHAR), CAST(b AS VARCHAR), CAST(c AS VARCHAR))), \',\')',
        },
      },
    );
    this.validateAll(
      'GROUP_CONCAT(a, b, c SEPARATOR \'\')',
      {
        write: {
          mysql: 'GROUP_CONCAT(CONCAT(a, b, c) SEPARATOR \'\')',
          sqlite: 'GROUP_CONCAT(a || b || c, \'\')',
          tsql: 'STRING_AGG(a + b + c, \'\')',
          databricks: 'LISTAGG(CONCAT(a, b, c), \'\')',
          postgres: 'STRING_AGG(a || b || c, \'\')',
        },
      },
    );
    this.validateAll(
      'GROUP_CONCAT(DISTINCT a, b, c SEPARATOR \'\')',
      {
        write: {
          mysql: 'GROUP_CONCAT(DISTINCT CONCAT(a, b, c) SEPARATOR \'\')',
          sqlite: 'GROUP_CONCAT(DISTINCT a || b || c, \'\')',
          tsql: 'STRING_AGG(a + b + c, \'\')',
          databricks: 'LISTAGG(DISTINCT CONCAT(a, b, c), \'\')',
          postgres: 'STRING_AGG(DISTINCT a || b || c, \'\')',
        },
      },
    );
    this.validateAll(
      'GROUP_CONCAT(a, b, c ORDER BY d SEPARATOR \'\')',
      {
        write: {
          mysql: 'GROUP_CONCAT(CONCAT(a, b, c) ORDER BY d SEPARATOR \'\')',
          sqlite: 'GROUP_CONCAT(a || b || c, \'\')',
          tsql: 'STRING_AGG(a + b + c, \'\') WITHIN GROUP (ORDER BY d)',
          databricks: 'LISTAGG(CONCAT(a, b, c), \'\') WITHIN GROUP (ORDER BY d)',
          postgres: 'STRING_AGG(a || b || c, \'\' ORDER BY d NULLS FIRST)',
        },
      },
    );
    this.validateAll(
      'GROUP_CONCAT(DISTINCT a, b, c ORDER BY d SEPARATOR \'\')',
      {
        write: {
          mysql: 'GROUP_CONCAT(DISTINCT CONCAT(a, b, c) ORDER BY d SEPARATOR \'\')',
          sqlite: 'GROUP_CONCAT(DISTINCT a || b || c, \'\')',
          tsql: 'STRING_AGG(a + b + c, \'\') WITHIN GROUP (ORDER BY d)',
          databricks: 'LISTAGG(DISTINCT CONCAT(a, b, c), \'\') WITHIN GROUP (ORDER BY d)',
          postgres: 'STRING_AGG(DISTINCT a || b || c, \'\' ORDER BY d NULLS FIRST)',
        },
      },
    );
    this.validateIdentity(
      'CREATE TABLE z (a INT) ENGINE=InnoDB AUTO_INCREMENT=1 CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT=\'x\'',
    );
    this.validateIdentity(
      'CREATE TABLE z (a INT) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT=\'x\'',
    );
    this.validateIdentity(
      'CREATE TABLE z (a INT DEFAULT NULL, PRIMARY KEY (a)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT=\'x\'',
    );

    this.validateAll(
      `CREATE TABLE \`t_customer_account\` (
  \`id\` int(11) NOT NULL AUTO_INCREMENT,
  \`customer_id\` int(11) DEFAULT NULL COMMENT '\u5BA2\u6237id',
  \`bank\` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '\u884C\u522B',
  \`account_no\` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '\u8D26\u53F7',
  PRIMARY KEY (\`id\`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8 COLLATE=utf8_bin COMMENT='\u5BA2\u6237\u8D26\u6237\u8868'`,
      {
        write: {
          mysql: `CREATE TABLE \`t_customer_account\` (
  \`id\` INT(11) NOT NULL AUTO_INCREMENT,
  \`customer_id\` INT(11) DEFAULT NULL COMMENT '\u5BA2\u6237id',
  \`bank\` VARCHAR(100) COLLATE utf8_bin DEFAULT NULL COMMENT '\u884C\u522B',
  \`account_no\` VARCHAR(100) COLLATE utf8_bin DEFAULT NULL COMMENT '\u8D26\u53F7',
  PRIMARY KEY (\`id\`)
)
ENGINE=InnoDB
AUTO_INCREMENT=1
DEFAULT CHARACTER SET=utf8
COLLATE=utf8_bin
COMMENT='\u5BA2\u6237\u8D26\u6237\u8868'`,
        },
        pretty: true,
      },
    );
  }

  testShowSimple () {
    for (const [key, writeKey] of [
      ['BINARY LOGS', 'BINARY LOGS'],
      ['MASTER LOGS', 'BINARY LOGS'],
      ['STORAGE ENGINES', 'ENGINES'],
      ['ENGINES', 'ENGINES'],
      ['EVENTS', 'EVENTS'],
      ['MASTER STATUS', 'MASTER STATUS'],
      ['PLUGINS', 'PLUGINS'],
      ['PRIVILEGES', 'PRIVILEGES'],
      ['PROFILES', 'PROFILES'],
      ['REPLICAS', 'REPLICAS'],
      ['SLAVE HOSTS', 'REPLICAS'],
    ] as const) {
      const show = this.validateIdentity(`SHOW ${key}`, `SHOW ${writeKey}`);
      expect(show).toBeInstanceOf(ShowExpr);
      expect(show.name).toBe(writeKey);
    }
  }

  testShowEvents () {
    for (const key of ['BINLOG', 'RELAYLOG']) {
      let show = this.validateIdentity(`SHOW ${key} EVENTS`);
      expect(show).toBeInstanceOf(ShowExpr);
      expect(show.name).toBe(`${key} EVENTS`);

      show = this.validateIdentity(`SHOW ${key} EVENTS IN 'log' FROM 1 LIMIT 2, 3`);
      expect(show.text('log')).toBe('log');
      expect(show.text('position')).toBe('1');
      expect(show.text('limit')).toBe('3');
      expect(show.text('offset')).toBe('2');

      show = this.validateIdentity(`SHOW ${key} EVENTS LIMIT 1`);
      expect(show.text('limit')).toBe('1');
      expect(show.getArgKey('offset')).toBeUndefined();
    }
  }

  testShowLikeOrWhere () {
    for (const [key, writeKey] of [
      ['CHARSET', 'CHARACTER SET'],
      ['CHARACTER SET', 'CHARACTER SET'],
      ['COLLATION', 'COLLATION'],
      ['DATABASES', 'DATABASES'],
      ['SCHEMAS', 'DATABASES'],
      ['FUNCTION STATUS', 'FUNCTION STATUS'],
      ['PROCEDURE STATUS', 'PROCEDURE STATUS'],
      ['GLOBAL STATUS', 'GLOBAL STATUS'],
      ['SESSION STATUS', 'STATUS'],
      ['STATUS', 'STATUS'],
      ['GLOBAL VARIABLES', 'GLOBAL VARIABLES'],
      ['SESSION VARIABLES', 'VARIABLES'],
      ['VARIABLES', 'VARIABLES'],
    ] as const) {
      const expectedName = writeKey.replace('GLOBAL', '').replace(/^\s+|\s+$/g, '').trim();
      let template = 'SHOW {}';
      let show = this.validateIdentity(template.replace('{}', key), template.replace('{}', writeKey));
      expect(show).toBeInstanceOf(ShowExpr);
      expect(show.name).toBe(expectedName);

      template = 'SHOW {} LIKE \'%foo%\'';
      show = this.validateIdentity(template.replace('{}', key), template.replace('{}', writeKey));
      expect(show).toBeInstanceOf(ShowExpr);
      expect(show.getArgKey('like')).toBeInstanceOf(LiteralExpr);
      expect(show.text('like')).toBe('%foo%');

      template = 'SHOW {} WHERE Column_name LIKE \'%foo%\'';
      show = this.validateIdentity(template.replace('{}', key), template.replace('{}', writeKey));
      expect(show).toBeInstanceOf(ShowExpr);
      expect(show.getArgKey('where')).toBeInstanceOf(WhereExpr);
      expect((show.getArgKey('where') as Expression).sql()).toBe('WHERE Column_name LIKE \'%foo%\'');
    }
  }

  testShowColumns () {
    let show = this.validateIdentity('SHOW COLUMNS FROM tbl_name');
    expect(show).toBeInstanceOf(ShowExpr);
    expect(show.name).toBe('COLUMNS');
    expect(show.text('target')).toBe('tbl_name');
    expect(show.getArgKey('full')).toBeFalsy();

    show = this.validateIdentity('SHOW FULL COLUMNS FROM tbl_name FROM db_name LIKE \'%foo%\'');
    expect(show).toBeInstanceOf(ShowExpr);
    expect(show.text('target')).toBe('tbl_name');
    expect(show.getArgKey('full')).toBeTruthy();
    expect(show.text('db')).toBe('db_name');
    expect(show.getArgKey('like')).toBeInstanceOf(LiteralExpr);
    expect(show.text('like')).toBe('%foo%');
  }

  testShowName () {
    for (const key of [
      'CREATE DATABASE',
      'CREATE EVENT',
      'CREATE FUNCTION',
      'CREATE PROCEDURE',
      'CREATE TABLE',
      'CREATE TRIGGER',
      'CREATE VIEW',
      'FUNCTION CODE',
      'PROCEDURE CODE',
    ]) {
      const show = this.validateIdentity(`SHOW ${key} foo`);
      expect(show).toBeInstanceOf(ShowExpr);
      expect(show.name).toBe(key);
      expect(show.text('target')).toBe('foo');
    }
  }

  testShowGrants () {
    const show = this.validateIdentity('SHOW GRANTS FOR foo');
    expect(show).toBeInstanceOf(ShowExpr);
    expect(show.name).toBe('GRANTS');
    expect(show.text('target')).toBe('foo');
  }

  testShowEngine () {
    let show = this.validateIdentity('SHOW ENGINE foo STATUS');
    expect(show).toBeInstanceOf(ShowExpr);
    expect(show.name).toBe('ENGINE');
    expect(show.text('target')).toBe('foo');
    expect(show.getArgKey('mutex')).toBeFalsy();

    show = this.validateIdentity('SHOW ENGINE foo MUTEX');
    expect(show.name).toBe('ENGINE');
    expect(show.text('target')).toBe('foo');
    expect(show.getArgKey('mutex')).toBeTruthy();
  }

  testShowErrors () {
    for (const key of ['ERRORS', 'WARNINGS']) {
      let show = this.validateIdentity(`SHOW ${key}`);
      expect(show).toBeInstanceOf(ShowExpr);
      expect(show.name).toBe(key);

      show = this.validateIdentity(`SHOW ${key} LIMIT 2, 3`);
      expect(show.text('limit')).toBe('3');
      expect(show.text('offset')).toBe('2');
    }
  }

  testShowIndex () {
    let show = this.validateIdentity('SHOW INDEX FROM foo');
    expect(show).toBeInstanceOf(ShowExpr);
    expect(show.name).toBe('INDEX');
    expect(show.text('target')).toBe('foo');

    show = this.validateIdentity('SHOW INDEX FROM foo FROM bar');
    expect(show.text('db')).toBe('bar');

    this.validateAll(
      'SHOW INDEX FROM bar.foo',
      { write: { mysql: 'SHOW INDEX FROM foo FROM bar' } },
    );
  }

  testShowDbLikeOrWhereSql () {
    for (const key of [
      'OPEN TABLES',
      'TABLE STATUS',
      'TRIGGERS',
    ]) {
      let show = this.validateIdentity(`SHOW ${key}`);
      expect(show).toBeInstanceOf(ShowExpr);
      expect(show.name).toBe(key);

      show = this.validateIdentity(`SHOW ${key} FROM db_name`);
      expect(show.name).toBe(key);
      expect(show.text('db')).toBe('db_name');

      show = this.validateIdentity(`SHOW ${key} LIKE '%foo%'`);
      expect(show.name).toBe(key);
      expect(show.getArgKey('like')).toBeInstanceOf(LiteralExpr);
      expect(show.text('like')).toBe('%foo%');

      show = this.validateIdentity(`SHOW ${key} WHERE Column_name LIKE '%foo%'`);
      expect(show.name).toBe(key);
      expect(show.getArgKey('where')).toBeInstanceOf(WhereExpr);
      expect((show.getArgKey('where') as Expression).sql()).toBe('WHERE Column_name LIKE \'%foo%\'');
    }
  }

  testShowProcesslist () {
    let show = this.validateIdentity('SHOW PROCESSLIST');
    expect(show).toBeInstanceOf(ShowExpr);
    expect(show.name).toBe('PROCESSLIST');
    expect(show.getArgKey('full')).toBeFalsy();

    show = this.validateIdentity('SHOW FULL PROCESSLIST');
    expect(show.name).toBe('PROCESSLIST');
    expect(show.getArgKey('full')).toBeTruthy();
  }

  testShowProfile () {
    let show = this.validateIdentity('SHOW PROFILE');
    expect(show).toBeInstanceOf(ShowExpr);
    expect(show.name).toBe('PROFILE');

    show = this.validateIdentity('SHOW PROFILE BLOCK IO');
    expect((show.getArgKey('types') as Expression[])[0].name).toBe('BLOCK IO');

    show = this.validateIdentity(
      'SHOW PROFILE BLOCK IO, PAGE FAULTS FOR QUERY 1 OFFSET 2 LIMIT 3',
    );
    expect((show.getArgKey('types') as Expression[])[0].name).toBe('BLOCK IO');
    expect((show.getArgKey('types') as Expression[])[1].name).toBe('PAGE FAULTS');
    expect(show.text('query')).toBe('1');
    expect(show.text('offset')).toBe('2');
    expect(show.text('limit')).toBe('3');
  }

  testShowReplicaStatus () {
    let show = this.validateIdentity('SHOW REPLICA STATUS');
    expect(show).toBeInstanceOf(ShowExpr);
    expect(show.name).toBe('REPLICA STATUS');

    show = this.validateIdentity('SHOW SLAVE STATUS', 'SHOW REPLICA STATUS');
    expect(show).toBeInstanceOf(ShowExpr);
    expect(show.name).toBe('REPLICA STATUS');

    show = this.validateIdentity('SHOW REPLICA STATUS FOR CHANNEL channel_name');
    expect(show.text('channel')).toBe('channel_name');
  }

  testShowTables () {
    let show = this.validateIdentity('SHOW TABLES');
    expect(show).toBeInstanceOf(ShowExpr);
    expect(show.name).toBe('TABLES');

    show = this.validateIdentity('SHOW FULL TABLES FROM db_name LIKE \'%foo%\'');
    expect(show.getArgKey('full')).toBeTruthy();
    expect(show.text('db')).toBe('db_name');
    expect(show.getArgKey('like')).toBeInstanceOf(LiteralExpr);
    expect(show.text('like')).toBe('%foo%');
  }

  testSetVariable () {
    let cmd = this.parseOne('SET SESSION x = 1');
    let item = (cmd.getArgKey('expressions') as Expression[])[0];
    expect(item.text('kind')).toBe('SESSION');
    expect(item.args.this).toBeInstanceOf(EqExpr);
    expect((item.args.this as EqExpr).left!.name).toBe('x');
    expect((item.args.this as EqExpr).right!.name).toBe('1');

    cmd = this.parseOne('SET @@GLOBAL.x = @@GLOBAL.y');
    item = (cmd.getArgKey('expressions') as Expression[])[0];
    expect(item.text('kind')).toBe('');
    expect(item.args.this).toBeInstanceOf(EqExpr);
    expect((item.args.this as EqExpr).left).toBeInstanceOf(SessionParameterExpr);
    expect((item.args.this as EqExpr).right).toBeInstanceOf(SessionParameterExpr);

    cmd = this.parseOne('SET NAMES \'charset_name\' COLLATE \'collation_name\'');
    item = (cmd.getArgKey('expressions') as Expression[])[0];
    expect(item.text('kind')).toBe('NAMES');
    expect(item.name).toBe('charset_name');
    expect(item.text('collate')).toBe('collation_name');

    cmd = this.parseOne('SET CHARSET DEFAULT');
    item = (cmd.getArgKey('expressions') as Expression[])[0];
    expect(item.text('kind')).toBe('CHARACTER SET');
    expect((item.args.this as Expression).name).toBe('DEFAULT');

    cmd = this.parseOne('SET x = 1, y = 2');
    expect((cmd.getArgKey('expressions') as Expression[]).length).toBe(2);
  }

  testJsonObject () {
    this.validateIdentity('SELECT JSON_OBJECT(\'id\', 87, \'name\', \'carrot\')');
  }

  testIsNull () {
    this.validateAll(
      'SELECT ISNULL(x)',
      { write: { '': 'SELECT (x IS NULL)', mysql: 'SELECT (x IS NULL)' } },
    );
  }

  testMonthname () {
    this.validateAll(
      'MONTHNAME(x)',
      {
        write: {
          '': 'TIME_TO_STR(CAST(x AS DATE), \'%B\')',
          mysql: 'DATE_FORMAT(x, \'%M\')',
        },
      },
    );
  }

  testSafeDiv () {
    this.validateAll(
      'a / b',
      {
        write: {
          bigquery: 'a / NULLIF(b, 0)',
          clickhouse: 'a / b',
          databricks: 'a / NULLIF(b, 0)',
          duckdb: 'a / b',
          hive: 'a / b',
          mysql: 'a / b',
          oracle: 'a / NULLIF(b, 0)',
          snowflake: 'a / NULLIF(b, 0)',
          spark: 'a / b',
          starrocks: 'a / b',
          drill: 'CAST(a AS DOUBLE) / NULLIF(b, 0)',
          postgres: 'CAST(a AS DOUBLE PRECISION) / NULLIF(b, 0)',
          presto: 'CAST(a AS DOUBLE) / NULLIF(b, 0)',
          redshift: 'CAST(a AS DOUBLE PRECISION) / NULLIF(b, 0)',
          sqlite: 'CAST(a AS REAL) / b',
          teradata: 'CAST(a AS DOUBLE PRECISION) / NULLIF(b, 0)',
          trino: 'CAST(a AS DOUBLE) / NULLIF(b, 0)',
          tsql: 'CAST(a AS FLOAT) / NULLIF(b, 0)',
        },
      },
    );
  }

  testTimestampTrunc () {
    const hiveDialects = ['spark', 'databricks'];
    for (const dialect of ['postgres', 'snowflake', ...hiveDialects]) {
      for (const unit of [
        'SECOND',
        'DAY',
        'MONTH',
        'YEAR',
      ]) {
        const cast = hiveDialects.includes(dialect)
          ? 'TIMESTAMP(\'2001-02-16 20:38:40\')'
          : 'CAST(\'2001-02-16 20:38:40\' AS DATETIME)';
        this.validateAll(
          `DATE_ADD('0000-01-01 00:00:00', INTERVAL (TIMESTAMPDIFF(${unit}, '0000-01-01 00:00:00', ${cast})) ${unit})`,
          {
            read: {
              [dialect]: `DATE_TRUNC(${unit}, TIMESTAMP '2001-02-16 20:38:40')`,
            },
            write: {
              mysql: `DATE_ADD('0000-01-01 00:00:00', INTERVAL (TIMESTAMPDIFF(${unit}, '0000-01-01 00:00:00', ${cast})) ${unit})`,
            },
          },
        );
      }
    }
  }

  testAtTimeZone () {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    try {
      // Check AT TIME ZONE doesnt discard the column name and also raises a warning
      this.validateIdentity(
        'SELECT foo AT TIME ZONE \'UTC\'',
        'SELECT foo',
      );
      const warned = warnSpy.mock.calls.some(
        (args) => String(args[0]).includes('AT TIME ZONE is not supported'),
      );
      expect(warned).toBe(true);
    } finally {
      warnSpy.mockRestore();
    }
  }

  testJsonValue () {
    const jsonDoc = '\'{"item": "shoes", "price": "49.95"}\'';
    this.validateIdentity(`SELECT JSON_VALUE(${jsonDoc}, '$.price')`);
    this.validateIdentity(
      `SELECT JSON_VALUE(${jsonDoc}, '$.price' RETURNING DECIMAL(4, 2))`,
    );

    for (const onOption of ['NULL', 'ERROR', 'DEFAULT 1']) {
      this.validateIdentity(
        `SELECT JSON_VALUE(${jsonDoc}, '$.price' RETURNING DECIMAL(4, 2) ${onOption} ON EMPTY ${onOption} ON ERROR) AS price`,
      );
    }
  }

  testGrant () {
    const grantCmds = [
      'GRANT \'role1\', \'role2\' TO \'user1\'@\'localhost\', \'user2\'@\'localhost\'',
      'GRANT SELECT ON world.* TO \'role3\'',
      'GRANT SELECT ON db2.invoice TO \'jeffrey\'@\'localhost\'',
      'GRANT INSERT ON `d%`.* TO u',
      'GRANT ALL ON test.* TO \'\'@\'localhost\'',
      'GRANT SELECT (col1), INSERT (col1, col2) ON mydb.mytbl TO \'someuser\'@\'somehost\'',
      'GRANT SELECT, INSERT, UPDATE ON *.* TO u2',
    ];

    for (const sql of grantCmds) {
      this.validateIdentity(sql, undefined, { checkCommandWarning: true });
    }
  }

  testRevoke () {
    const revokeCmds = [
      'REVOKE \'role1\', \'role2\' FROM \'user1\'@\'localhost\', \'user2\'@\'localhost\'',
      'REVOKE SELECT ON world.* FROM \'role3\'',
      'REVOKE SELECT ON db2.invoice FROM \'jeffrey\'@\'localhost\'',
      'REVOKE INSERT ON `d%`.* FROM u',
      'REVOKE ALL ON test.* FROM \'\'@\'localhost\'',
      'REVOKE SELECT (col1), INSERT (col1, col2) ON mydb.mytbl FROM \'someuser\'@\'somehost\'',
      'REVOKE SELECT, INSERT, UPDATE ON *.* FROM u2',
    ];

    for (const sql of revokeCmds) {
      this.validateIdentity(sql, undefined, { checkCommandWarning: true });
    }
  }

  testExplain () {
    this.validateIdentity(
      'EXPLAIN ANALYZE SELECT * FROM t', 'DESCRIBE ANALYZE SELECT * FROM t',
    );

    const expression = this.parseOne('EXPLAIN ANALYZE SELECT * FROM t');
    expect(expression).toBeInstanceOf(DescribeExpr);
    expect(expression.text('style')).toBe('ANALYZE');

    for (const format of ['JSON', 'TRADITIONAL', 'TREE']) {
      this.validateIdentity(`DESCRIBE FORMAT=${format} UPDATE test SET test_col = 'abc'`);
    }
  }

  testNumberFormat () {
    this.validateAll(
      'SELECT FORMAT(12332.123456, 4)',
      {
        write: {
          duckdb: 'SELECT FORMAT(\'{:,.4f}\', 12332.123456)',
          mysql: 'SELECT FORMAT(12332.123456, 4)',
        },
      },
    );
    this.validateAll(
      'SELECT FORMAT(12332.1, 4)',
      {
        write: {
          duckdb: 'SELECT FORMAT(\'{:,.4f}\', 12332.1)',
          mysql: 'SELECT FORMAT(12332.1, 4)',
        },
      },
    );
    this.validateAll(
      'SELECT FORMAT(12332.2, 0)',
      {
        write: {
          duckdb: 'SELECT FORMAT(\'{:,.0f}\', 12332.2)',
          mysql: 'SELECT FORMAT(12332.2, 0)',
        },
      },
    );
    this.validateAll(
      'SELECT FORMAT(12332.2, 2, \'de_DE\')',
      {
        write: {
          duckdb: UnsupportedError,
          mysql: 'SELECT FORMAT(12332.2, 2, \'de_DE\')',
        },
      },
    );
  }

  testAnalyze () {
    this.validateIdentity('ANALYZE LOCAL TABLE tbl');
    this.validateIdentity('ANALYZE NO_WRITE_TO_BINLOG TABLE tbl');
    this.validateIdentity('ANALYZE tbl UPDATE HISTOGRAM ON col1');
    this.validateIdentity('ANALYZE tbl UPDATE HISTOGRAM ON col1 USING DATA \'json_data\'');
    this.validateIdentity('ANALYZE tbl UPDATE HISTOGRAM ON col1 WITH 5 BUCKETS');
    this.validateIdentity('ANALYZE tbl UPDATE HISTOGRAM ON col1 WITH 5 BUCKETS AUTO UPDATE');
    this.validateIdentity('ANALYZE tbl UPDATE HISTOGRAM ON col1 WITH 5 BUCKETS MANUAL UPDATE');
    this.validateIdentity('ANALYZE tbl DROP HISTOGRAM ON col1');
  }

  testUtcTime () {
    this.validateIdentity('UTC_TIME()').assertIs(UtcTimeExpr);
    this.validateIdentity('UTC_TIME(6)').assertIs(UtcTimeExpr);
    this.validateIdentity('UTC_TIMESTAMP()').assertIs(UtcTimestampExpr);
    this.validateIdentity('UTC_TIMESTAMP(6)').assertIs(UtcTimestampExpr);
  }

  testMod () {
    this.validateIdentity('x % y').assertIs(ModExpr);
    this.validateIdentity('x MOD y', 'x % y').assertIs(ModExpr);
    this.validateIdentity('MOD(x, y)', 'x % y').assertIs(ModExpr);
  }

  testNumericTrunc () {
    // MySQL uses TRUNCATE for numeric truncation
    this.validateIdentity('TRUNCATE(3.14159, 2)').assertIs(TruncExpr);
    this.validateIdentity('TRUNCATE(price, 0)').assertIs(TruncExpr);

    // TRUNC alias normalizes to TRUNCATE in MySQL
    this.validateIdentity('TRUNC(3.14159, 2)', 'TRUNCATE(3.14159, 2)').assertIs(TruncExpr);

    // Cross-dialect numeric truncation transpilation
    this.validateAll(
      'TRUNCATE(3.14159, 2)',
      {
        write: {
          mysql: 'TRUNCATE(3.14159, 2)',
          oracle: 'TRUNC(3.14159, 2)',
          postgres: 'TRUNC(3.14159, 2)',
          snowflake: 'TRUNC(3.14159, 2)',
          tsql: 'ROUND(3.14159, 2, 1)',
        },
      },
    );
  }

  testValidIntervalUnits () {
    for (const unit of [
      'SECOND_MICROSECOND',
      'MINUTE_MICROSECOND',
      'MINUTE_SECOND',
      'HOUR_MICROSECOND',
      'HOUR_SECOND',
      'HOUR_MINUTE',
      'DAY_MICROSECOND',
      'DAY_SECOND',
      'DAY_MINUTE',
      'DAY_HOUR',
      'YEAR_MONTH',
    ]) {
      this.validateIdentity(`DATE_ADD(base_date, INTERVAL day_interval ${unit})`);
    }
  }

  testCreateTrigger () {
    this.validateIdentity(
      'CREATE TRIGGER check_age BEFORE INSERT ON users FOR EACH ROW BEGIN SET NEW.created_at = NOW() END',
      undefined,
      { checkCommandWarning: true },
    );

    this.validateIdentity(
      'CREATE TRIGGER audit_update AFTER UPDATE ON accounts FOR EACH ROW BEGIN INSERT INTO audit_log (user_id, old_balance, new_balance, changed_at) VALUES (OLD.user_id, OLD.balance, NEW.balance, NOW()) END',
      undefined,
      { checkCommandWarning: true },
    );

    this.validateIdentity(
      'CREATE TRIGGER track_deletes BEFORE DELETE ON orders FOR EACH ROW BEGIN UPDATE statistics SET delete_count = delete_count + 1 WHERE table_name = \'orders\' END',
      undefined,
      { checkCommandWarning: true },
    );
  }
}

const t = new TestMySQL();

describe('TestMySQL', () => {
  test('testDdl', () => t.testDdl());
  test('testIdentity', () => t.testIdentity());
  test('testTypes', () => t.testTypes());
  test('testCanonicalFunctions', () => t.testCanonicalFunctions());
  test('testEscape', () => t.testEscape());
  test('testIntroducers', () => t.testIntroducers());
  test('testHexadecimalLiteral', () => t.testHexadecimalLiteral());
  test('testBitsLiteral', () => t.testBitsLiteral());
  test('testStringLiterals', () => t.testStringLiterals());
  test('testConvert', () => t.testConvert());
  test('testMatchAgainst', () => t.testMatchAgainst());
  test('testDateFormat', () => t.testDateFormat());
  test('testMysqlTime', () => t.testMysqlTime());
  test('testMysqlTimePython311', () => t.testMysqlTimePython311());
  test('testMysql', () => t.testMysql());
  test('testShowSimple', () => t.testShowSimple());
  test('testShowEvents', () => t.testShowEvents());
  test('testShowLikeOrWhere', () => t.testShowLikeOrWhere());
  test('testShowColumns', () => t.testShowColumns());
  test('testShowName', () => t.testShowName());
  test('testShowGrants', () => t.testShowGrants());
  test('testShowEngine', () => t.testShowEngine());
  test('testShowErrors', () => t.testShowErrors());
  test('testShowIndex', () => t.testShowIndex());
  test('testShowDbLikeOrWhereSql', () => t.testShowDbLikeOrWhereSql());
  test('testShowProcesslist', () => t.testShowProcesslist());
  test('testShowProfile', () => t.testShowProfile());
  test('testShowReplicaStatus', () => t.testShowReplicaStatus());
  test('testShowTables', () => t.testShowTables());
  test('testSetVariable', () => t.testSetVariable());
  test('testJsonObject', () => t.testJsonObject());
  test('testIsNull', () => t.testIsNull());
  test('testMonthname', () => t.testMonthname());
  test('testSafeDiv', () => t.testSafeDiv());
  test('testTimestampTrunc', () => t.testTimestampTrunc());
  test('testAtTimeZone', () => t.testAtTimeZone());
  test('testJsonValue', () => t.testJsonValue());
  test('testGrant', () => t.testGrant());
  test('testRevoke', () => t.testRevoke());
  test('testExplain', () => t.testExplain());
  test('testNumberFormat', () => t.testNumberFormat());
  test('testAnalyze', () => t.testAnalyze());
  test('testUtcTime', () => t.testUtcTime());
  test('testMod', () => t.testMod());
  test('testNumericTrunc', () => t.testNumericTrunc());
  test('testValidIntervalUnits', () => t.testValidIntervalUnits());
  test('testCreateTrigger', () => t.testCreateTrigger());
});
