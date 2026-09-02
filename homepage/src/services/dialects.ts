import {
  Dialect,
} from '@hdnax/sqlingo.js';
import {
  MySQL,
} from '@hdnax/sqlingo.js/mysql';
import {
  Postgres,
} from '@hdnax/sqlingo.js/postgres';
import {
  SQLite,
} from '@hdnax/sqlingo.js/sqlite';
import {
  DuckDB,
} from '@hdnax/sqlingo.js/duckdb';
import {
  BigQuery,
} from '@hdnax/sqlingo.js/bigquery';
import {
  Snowflake,
} from '@hdnax/sqlingo.js/snowflake';
import {
  Spark,
} from '@hdnax/sqlingo.js/spark';
import {
  Hive,
} from '@hdnax/sqlingo.js/hive';
import {
  Trino,
} from '@hdnax/sqlingo.js/trino';
import {
  TSQL,
} from '@hdnax/sqlingo.js/mssql';
import {
  Oracle,
} from '@hdnax/sqlingo.js/oracle';
import {
  Redshift,
} from '@hdnax/sqlingo.js/redshift';

Dialect.register(
  MySQL,
  Postgres,
  SQLite,
  DuckDB,
  BigQuery,
  Snowflake,
  Spark,
  Hive,
  Trino,
  TSQL,
  Oracle,
  Redshift,
);

export const DIALECTS = [
  {
    value: 'mysql',
    label: 'MySQL',
  },
  {
    value: 'postgres',
    label: 'Postgres',
  },
  {
    value: 'sqlite',
    label: 'SQLite',
  },
  {
    value: 'duckdb',
    label: 'DuckDB',
  },
  {
    value: 'bigquery',
    label: 'BigQuery',
  },
  {
    value: 'snowflake',
    label: 'Snowflake',
  },
  {
    value: 'spark',
    label: 'Spark',
  },
  {
    value: 'hive',
    label: 'Hive',
  },
  {
    value: 'trino',
    label: 'Trino',
  },
  {
    value: 'mssql',
    label: 'MSSQL',
  },
  {
    value: 'oracle',
    label: 'Oracle',
  },
  {
    value: 'redshift',
    label: 'Redshift',
  },
] as const;

export type DialectValue = typeof DIALECTS[number]['value'];

export function getDialectLabel (value: string): string {
  return DIALECTS.find((dialect) => dialect.value === value)?.label ?? value;
}
