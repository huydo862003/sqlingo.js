import '../dialects'; // register all dialects as side effect
import {
  parse,
  CommandExpr,
  CreateExpr,
  CreateExprKind,
  Expression,
  IndexExpr,
  InsertExpr,
} from '@hdnax/sqlingo.js';
import {
  DbmlSchema, DbmlTable,
  type DbmlRecord,
} from './types';
import {
  tableParts,
} from './utils/name';
import {
  buildTable,
} from './utils/table';
import {
  buildRecord,
} from './utils/record';
import {
  indexFromParams,
} from './utils/tableIndex';
import {
  schemaToDbml,
} from './utils/emit';

export interface ConversionResult {
  schema: DbmlSchema;
  dbml: string;
}

function recordKey (r: Pick<DbmlRecord, 'schema' | 'tableName'>): string {
  return new DbmlTable({
    schema: r.schema,
    name: r.tableName,
    columns: [],
  }).intern();
}

export function sqlToDbml (sql: string, dialect?: string): ConversionResult {
  const parsed = parse(sql, dialect
    ? {
      read: dialect,
    }
    : {});
  const bad = parsed.find((s) => s instanceof CommandExpr);
  if (bad) throw new Error('Unsupported SQL syntax');

  const schema = new DbmlSchema();

  const tableByKey = new Map<string, DbmlTable>();
  const recordByKey = new Map<string, DbmlRecord>();

  for (const stmt of parsed) {
    if (stmt instanceof InsertExpr) {
      const rec = buildRecord(stmt);
      if (!rec) continue;
      const key = recordKey(rec);
      const existing = recordByKey.get(key);
      if (existing) {
        existing.rows.push(...rec.rows);
      } else {
        recordByKey.set(key, rec);
        schema.records.push(rec);
      }
      continue;
    }

    if (!(stmt instanceof CreateExpr)) continue;

    if (stmt.kind === CreateExprKind.INDEX) {
      const idx = stmt.args.this;
      if (!(idx instanceof IndexExpr)) continue;
      const tableRef = idx.args.table;
      if (!(tableRef instanceof Expression)) continue;
      const tp = tableParts(tableRef);
      const lookup = new DbmlTable({
        schema: tp.schema,
        name: tp.name,
        columns: [],
      }).intern();
      const target = tableByKey.get(lookup);
      if (!target) continue;
      const built = indexFromParams(idx);
      if (!built) continue;
      if (stmt.args.unique) built.unique = true;
      target.indexes = target.indexes ?? [];
      target.indexes.push(built);
      continue;
    }

    if (stmt.kind !== CreateExprKind.TABLE) continue;
    const built = buildTable(stmt);
    if (!built) continue;
    schema.tables.push(built.table);
    tableByKey.set(built.table.intern(), built.table);
    schema.refs.push(...built.refs);
  }

  return {
    schema,
    dbml: schemaToDbml(schema),
  };
}
