import {
  CheckColumnConstraintExpr,
  ColumnDefExpr,
  Expression,
  ForeignKeyExpr,
  IndexExpr,
  PrimaryKeyExpr,
  ReferenceExpr,
  SchemaExpr,
  type CreateExpr,
} from '@hdnax/sqlingo.js';
import type {
  DbmlColumn,
  DbmlIndex,
} from '../types';
import {
  DbmlCheck,
  DbmlRef,
  DbmlRelation,
  DbmlTable,
} from '../types';
import {
  nodeText, tableParts,
} from './name';
import {
  endpoint, parseActionExpr, referenceActions,
} from './ref';
import {
  buildColumn,
} from './column';
import {
  indexFromParams,
} from './tableIndex';

export interface BuiltTable {
  table: DbmlTable;
  refs: DbmlRef[];
}

export function buildTable (stmt: CreateExpr): BuiltTable | undefined {
  const schemaNode = stmt.args.this;
  if (!(schemaNode instanceof SchemaExpr)) return undefined;

  const tp = tableParts(schemaNode.args.this as Expression | undefined);
  const expressions = schemaNode.args.expressions ?? [];

  const columns: DbmlColumn[] = [];
  const tablePkCols = new Set<string>();
  const tableRefs: DbmlRef[] = [];
  const checks: DbmlCheck[] = [];
  const inlineIndexes: DbmlIndex[] = [];

  for (const expr of expressions) {
    if (expr instanceof ColumnDefExpr) {
      columns.push(buildColumn(expr));
    } else if (expr instanceof PrimaryKeyExpr) {
      for (const col of expr.args.expressions ?? []) {
        if (col instanceof Expression) tablePkCols.add(nodeText(col));
      }
    } else if (expr instanceof ForeignKeyExpr) {
      const fkCols = (expr.args.expressions ?? []).filter((e): e is Expression => e instanceof Expression);
      const ref = expr.args.reference;
      if (!(ref instanceof ReferenceExpr)) continue;
      const inner = ref.args.this;
      const tableExpr = inner instanceof SchemaExpr ? inner.args.this : inner;
      const cols = inner instanceof SchemaExpr
        ? (inner.args.expressions ?? [])
        : [];
      const target = tableParts(tableExpr as Expression | undefined);
      const refCols = cols.map((c) => c instanceof Expression ? nodeText(c) : String(c));
      if (target.name && fkCols.length) {
        const actions = referenceActions(ref);
        tableRefs.push(new DbmlRef({
          relation: DbmlRelation.MANY_TO_ONE,
          source: endpoint(tp.schema, tp.name, fkCols.map(nodeText)),
          target: endpoint(target.schema, target.name, refCols.length ? refCols : fkCols.map(nodeText)),
          onDelete: actions.onDelete ?? parseActionExpr(expr.args.delete),
          onUpdate: actions.onUpdate ?? parseActionExpr(expr.args.update),
        }));
      }
    } else if (expr instanceof CheckColumnConstraintExpr) {
      const e = expr.args.this;
      if (e instanceof Expression) checks.push(new DbmlCheck({
        expression: e.sql(),
      }));
    } else if (expr instanceof IndexExpr) {
      const built = indexFromParams(expr);
      if (built) inlineIndexes.push(built);
    }
  }

  for (const col of columns) {
    if (tablePkCols.has(col.name)) {
      col.pk = true;
      col.notNull = true;
    }
    for (const r of col.ref ?? []) {
      tableRefs.push(new DbmlRef({
        relation: r.relation,
        source: endpoint(tp.schema, tp.name, [col.name]),
        target: r.target,
        onDelete: r.onDelete,
        onUpdate: r.onUpdate,
      }));
    }
  }

  const table = new DbmlTable({
    schema: tp.schema,
    name: tp.name,
    columns,
    checks: checks.length ? checks : undefined,
    indexes: inlineIndexes.length ? inlineIndexes : undefined,
  });

  return {
    table,
    refs: tableRefs,
  };
}
