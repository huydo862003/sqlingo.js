import {
  CheckColumnConstraintExpr,
  CheckExpr,
  ColumnDefExpr,
  ConstraintExpr,
  Expression,
  ForeignKeyExpr,
  IndexExpr,
  PrimaryKeyExpr,
  ReferenceExpr,
  SchemaExpr,
  UniqueColumnConstraintExpr,
  type CreateExpr,
} from '@hdnax/sqlingo.js';
import type {
  DbmlColumn,
  DbmlIndex,
} from '../types';
import {
  DbmlCheck,
  DbmlIndexColumn,
  DbmlIndex as DbmlIndexClass,
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

interface TableCtx {
  schema?: string;
  name: string;
  columns: DbmlColumn[];
  tablePkCols: Set<string>;
  tableRefs: DbmlRef[];
  checks: DbmlCheck[];
  inlineIndexes: DbmlIndex[];
}

function handlePrimaryKey (expr: PrimaryKeyExpr, ctx: TableCtx, name: string | undefined): void {
  const cols = (expr.args.expressions ?? []).filter((e): e is Expression => e instanceof Expression);
  const colNames = cols.map(nodeText);
  if (1 === colNames.length) {
    ctx.tablePkCols.add(colNames[0]);
    return;
  }
  if (colNames.length) {
    ctx.inlineIndexes.push(new DbmlIndexClass({
      name,
      columns: colNames.map((c) => new DbmlIndexColumn({
        expression: c,
      })),
      pk: true,
    }));
  }
}

function handleForeignKey (expr: ForeignKeyExpr, ctx: TableCtx, name: string | undefined): void {
  const fkCols = (expr.args.expressions ?? []).filter((e): e is Expression => e instanceof Expression);
  const ref = expr.args.reference;
  if (!(ref instanceof ReferenceExpr)) return;
  const inner = ref.args.this;
  const tableExpr = inner instanceof SchemaExpr ? inner.args.this : inner;
  const cols = inner instanceof SchemaExpr ? (inner.args.expressions ?? []) : [];
  const target = tableParts(tableExpr as Expression | undefined);
  const refCols = cols.map((c) => c instanceof Expression ? nodeText(c) : String(c));
  if (target.name && fkCols.length) {
    const actions = referenceActions(ref);
    ctx.tableRefs.push(new DbmlRef({
      name,
      relation: DbmlRelation.MANY_TO_ONE,
      source: endpoint(ctx.schema, ctx.name, fkCols.map(nodeText)),
      target: endpoint(target.schema, target.name, refCols.length ? refCols : fkCols.map(nodeText)),
      onDelete: actions.onDelete ?? parseActionExpr(expr.args.delete),
      onUpdate: actions.onUpdate ?? parseActionExpr(expr.args.update),
    }));
  }
}

function handleUnique (expr: UniqueColumnConstraintExpr, ctx: TableCtx, name: string | undefined): void {
  const inner = expr.args.this;
  const cols = inner instanceof SchemaExpr
    ? (inner.args.expressions ?? []).filter((e): e is Expression => e instanceof Expression)
    : [];
  if (!cols.length) return;
  ctx.inlineIndexes.push(new DbmlIndexClass({
    name,
    columns: cols.map((c) => new DbmlIndexColumn({
      expression: nodeText(c),
    })),
    unique: true,
  }));
}

function handleCheck (inner: Expression, ctx: TableCtx, name: string | undefined): void {
  ctx.checks.push(new DbmlCheck({
    name,
    expression: inner.sql(),
  }));
}

function dispatchConstraint (expr: Expression, ctx: TableCtx, name: string | undefined): void {
  if (expr instanceof PrimaryKeyExpr) {
    handlePrimaryKey(expr, ctx, name);
  } else if (expr instanceof ForeignKeyExpr) {
    handleForeignKey(expr, ctx, name);
  } else if (expr instanceof UniqueColumnConstraintExpr) {
    handleUnique(expr, ctx, name);
  } else if (expr instanceof CheckColumnConstraintExpr) {
    const e = expr.args.this;
    if (e instanceof Expression) handleCheck(e, ctx, name);
  } else if (expr instanceof CheckExpr) {
    const e = expr.args.this;
    if (e instanceof Expression) handleCheck(e, ctx, name);
  } else if (expr instanceof IndexExpr) {
    const built = indexFromParams(expr);
    if (built) {
      if (name && !built.name) built.name = name;
      ctx.inlineIndexes.push(built);
    }
  }
}

export function buildTable (stmt: CreateExpr): BuiltTable | undefined {
  const schemaNode = stmt.args.this;
  if (!(schemaNode instanceof SchemaExpr)) return undefined;

  const tp = tableParts(schemaNode.args.this as Expression | undefined);
  const expressions = schemaNode.args.expressions ?? [];

  const ctx: TableCtx = {
    schema: tp.schema,
    name: tp.name,
    columns: [],
    tablePkCols: new Set<string>(),
    tableRefs: [],
    checks: [],
    inlineIndexes: [],
  };

  for (const expr of expressions) {
    if (expr instanceof ColumnDefExpr) {
      ctx.columns.push(buildColumn(expr));
    } else if (expr instanceof ConstraintExpr) {
      const name = expr.args.this instanceof Expression ? nodeText(expr.args.this) : undefined;
      for (const part of expr.args.expressions ?? []) {
        if (part instanceof Expression) dispatchConstraint(part, ctx, name);
      }
    } else {
      dispatchConstraint(expr, ctx, undefined);
    }
  }

  for (const col of ctx.columns) {
    if (ctx.tablePkCols.has(col.name)) {
      col.pk = true;
      col.notNull = true;
    }
    for (const r of col.ref ?? []) {
      ctx.tableRefs.push(new DbmlRef({
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
    columns: ctx.columns,
    checks: ctx.checks.length ? ctx.checks : undefined,
    indexes: ctx.inlineIndexes.length ? ctx.inlineIndexes : undefined,
  });

  return {
    table,
    refs: ctx.tableRefs,
  };
}
