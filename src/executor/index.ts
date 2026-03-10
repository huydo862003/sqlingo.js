import type { DialectType } from '../dialects/dialect';
import type { Schema } from '../schema';
import { ExecuteError } from '../errors';
import type { Expression } from '../expressions';
import { convert } from '../expressions';
import { objectDepth } from '../helper';
import { optimize } from '../optimizer/optimizer';
import { annotateTypes } from '../optimizer/annotate_types';
import { Plan } from '../planner';
import {
  ensureSchema, flattenSchema, nestedGet, nestedSet,
} from '../schema';
import { JavascriptExecutor } from './javascript';
import type { Table } from './table';
import { ensureTables } from './table';

/**
 * Run a sql query against data.
 *
 * @param sql - a sql statement or Expression.
 * @param schema - database schema. Can be a Schema instance or a mapping in one of:
 *   1. {table: {col: type}}
 *   2. {db: {table: {col: type}}}
 *   3. {catalog: {db: {table: {col: type}}}}
 * @param dialect - the SQL dialect to apply during parsing.
 * @param tables_ - additional tables to register.
 * @returns Simple columnar data structure.
 */
export function execute (
  sql: string | Expression,
  schema?: Record<string, unknown> | Schema,
  dialect?: DialectType,
  tables_?: Record<string, Table>,
): Table {
  const tables = ensureTables(tables_, dialect);

  let schemaArg: Record<string, unknown>;
  if (!schema) {
    schemaArg = {};
    const mapping = tables.mapping;
    const depth = objectDepth(mapping);
    const flattened = flattenSchema(mapping, depth);

    for (const keys of flattened) {
      const path: Array<[string, string]> = keys.map((k) => [k, k]);
      const table = nestedGet(mapping, path, { raiseOnMissing: false }) as Table | undefined;
      if (!table) continue;

      for (const column of table.columns) {
        const value = table.get(0)[column];
        const columnType =
          annotateTypes(convert(value), { dialect })?.type?.toString() ?? typeof value;
        nestedSet(schemaArg, [...keys, column], columnType);
      }
    }
  } else {
    schemaArg = schema as Record<string, unknown>;
  }

  const resolvedSchema = ensureSchema(schemaArg, { dialect });

  if (
    0 < tables.supportedTableArgs.length
    && tables.supportedTableArgs.join(',') !== resolvedSchema.supportedTableArgs.join(',')
  ) {
    throw new ExecuteError('Tables must support the same table args as schema');
  }

  const expression = optimize(sql, {
    schema: resolvedSchema,
    dialect,
    isolateTables: true,
  });

  const plan = new Plan(expression);

  return new JavascriptExecutor(undefined, tables.mapping as Record<string, Table>).execute(plan);
}
