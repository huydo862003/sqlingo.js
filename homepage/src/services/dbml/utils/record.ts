import type {
  InsertExpr,
} from '@hdnax/sqlingo.js';
import {
  Expression,
  SchemaExpr,
  TupleExpr,
} from '@hdnax/sqlingo.js';
import {
  DbmlRecord,
} from '../types';
import {
  nodeText, tableParts,
} from './name';

export function buildRecord (stmt: InsertExpr): DbmlRecord | undefined {
  const tableExpr = stmt.args.this;
  if (!(tableExpr instanceof SchemaExpr)) return undefined;
  const tp = tableParts(tableExpr.args.this as Expression | undefined);
  if (!tp.name) return undefined;
  const columns = (tableExpr.args.expressions ?? []).map(nodeText);
  const valuesExpr = stmt.args.expression;
  if (!valuesExpr) return undefined;
  const rows: string[][] = [];
  for (const tuple of valuesExpr.args.expressions ?? []) {
    if (!(tuple instanceof TupleExpr)) continue;
    rows.push((tuple.args.expressions ?? []).map((v) =>
      v instanceof Expression ? v.sql() : String(v)));
  }
  if (!rows.length) return undefined;
  return new DbmlRecord({
    schema: tp.schema,
    tableName: tp.name,
    columns,
    rows,
  });
}
