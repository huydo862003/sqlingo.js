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
} from '../../types';
import {
  extractNodeText, extractTableParts,
} from '../parse/ast';

// Build dbml model records from sqlingo.js AST
export function buildRecord (stmt: InsertExpr): DbmlRecord | undefined {
  const tableExpr = stmt.args.this;

  if (!(tableExpr instanceof SchemaExpr)) return undefined;
  const tableParts = extractTableParts(tableExpr.args.this as Expression | undefined);

  if (!tableParts.name) return undefined;
  const columns = (tableExpr.args.expressions ?? []).map(extractNodeText);
  const valuesExpr = stmt.args.expression;

  if (!valuesExpr) return undefined;
  const rows: string[][] = [];

  for (const tuple of valuesExpr.args.expressions ?? []) {
    if (!(tuple instanceof TupleExpr)) continue;
    rows.push((tuple.args.expressions ?? []).map((value) =>
      value instanceof Expression ? value.sql() : String(value)));
  }
  if (!rows.length) return undefined;

  return new DbmlRecord({
    schema: tableParts.schema,
    tableName: tableParts.name,
    columns,
    rows,
  });
}
