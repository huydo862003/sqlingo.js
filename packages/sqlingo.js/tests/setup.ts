import {
  Dialect,
} from '../src/dialects/dialect';
import {
  Athena,
} from '../src/dialects/athena';
import {
  BigQuery,
} from '../src/dialects/bigquery';
import {
  ClickHouse,
} from '../src/dialects/clickhouse';
import {
  Databricks,
} from '../src/dialects/databricks';
import {
  Doris,
} from '../src/dialects/doris';
import {
  Dremio,
} from '../src/dialects/dremio';
import {
  Drill,
} from '../src/dialects/drill';
import {
  Druid,
} from '../src/dialects/druid';
import {
  DuckDB,
} from '../src/dialects/duckdb';
import {
  Dune,
} from '../src/dialects/dune';
import {
  Exasol,
} from '../src/dialects/exasol';
import {
  Fabric,
} from '../src/dialects/fabric';
import {
  Hive,
} from '../src/dialects/hive';
import {
  Materialize,
} from '../src/dialects/materialize';
import {
  MySQL,
} from '../src/dialects/mysql';
import {
  Oracle,
} from '../src/dialects/oracle';
import {
  Postgres,
} from '../src/dialects/postgres';
import {
  Presto,
} from '../src/dialects/presto';
import {
  PRQL,
} from '../src/dialects/prql';
import {
  Redshift,
} from '../src/dialects/redshift';
import {
  RisingWave,
} from '../src/dialects/risingwave';
import {
  SingleStore,
} from '../src/dialects/singlestore';
import {
  Snowflake,
} from '../src/dialects/snowflake';
import {
  Solr,
} from '../src/dialects/solr';
import {
  Spark,
} from '../src/dialects/spark';
import {
  Spark2,
} from '../src/dialects/spark2';
import {
  SQLite,
} from '../src/dialects/sqlite';
import {
  StarRocks,
} from '../src/dialects/starrocks';
import {
  Tableau,
} from '../src/dialects/tableau';
import {
  Teradata,
} from '../src/dialects/teradata';
import {
  Trino,
} from '../src/dialects/trino';
import {
  TSQL,
} from '../src/dialects/tsql';

Dialect.register(
  Athena,
  BigQuery,
  ClickHouse,
  Databricks,
  Doris,
  Dremio,
  Drill,
  Druid,
  DuckDB,
  Dune,
  Exasol,
  Fabric,
  Hive,
  Materialize,
  MySQL,
  Oracle,
  Postgres,
  Presto,
  PRQL,
  Redshift,
  RisingWave,
  SingleStore,
  Snowflake,
  Solr,
  Spark,
  Spark2,
  SQLite,
  StarRocks,
  Tableau,
  Teradata,
  Trino,
  TSQL,
);
