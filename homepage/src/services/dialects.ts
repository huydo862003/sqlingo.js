// Side-effect imports: each file calls Dialect.register() on load
import '@hdnax/sqlingo.js/mysql';
import '@hdnax/sqlingo.js/postgres';
import '@hdnax/sqlingo.js/sqlite';
import '@hdnax/sqlingo.js/duckdb';
import '@hdnax/sqlingo.js/bigquery';
import '@hdnax/sqlingo.js/snowflake';
import '@hdnax/sqlingo.js/spark';
import '@hdnax/sqlingo.js/hive';
import '@hdnax/sqlingo.js/trino';
import '@hdnax/sqlingo.js/mssql';
import '@hdnax/sqlingo.js/oracle';
import '@hdnax/sqlingo.js/redshift';

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
  return DIALECTS.find((d) => d.value === value)?.label ?? value;
}
