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
} from '../../types';
import {
  DbmlCheck,
  DbmlIndex,
  DbmlIndexColumn,
  DbmlReference,
  DbmlRelation,
  DbmlTable,
} from '../../types';
import {
  extractNodeText, extractTableParts,
} from '../parse/ast';
import {
  buildEndpoint, parseActionExpr, extractReferenceActions,
} from './ref';
import {
  buildDbmlColumn,
} from './column';
import {
  indexFromParameters,
} from './indexes';

// Build dbml model tables from sqlingo.js AST

export interface BuiltTable {
  table: DbmlTable;
  refs: DbmlReference[];
}

export function buildTable (stmt: CreateExpr): BuiltTable | undefined {
  const schemaNode = stmt.args.this;
  if (!(schemaNode instanceof SchemaExpr)) return undefined;

  const tp = extractTableParts(schemaNode.args.this as Expression | undefined);
  const expressions = schemaNode.args.expressions ?? [];

  const context: TableContext = {
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
      context.columns.push(buildDbmlColumn(expr));
    } else if (expr instanceof ConstraintExpr) {
      const name = expr.args.this instanceof Expression ? extractNodeText(expr.args.this) : undefined;
      for (const part of expr.args.expressions ?? []) {
        if (part instanceof Expression) dispatchConstraint(part, context, name);
      }
    } else {
      dispatchConstraint(expr, context, undefined);
    }
  }

  for (const col of context.columns) {
    if (context.tablePkCols.has(col.name)) {
      col.pk = true;
      col.notNull = true;
    }
    for (const inlineReference of col.ref ?? []) {
      context.tableRefs.push(new DbmlReference({
        relation: inlineReference.relation,
        source: buildEndpoint(tp.schema, tp.name, [col.name]),
        target: inlineReference.target,
        onDelete: inlineReference.onDelete,
        onUpdate: inlineReference.onUpdate,
      }));
    }
  }

  const table = new DbmlTable({
    schema: tp.schema,
    name: tp.name,
    columns: context.columns,
    checks: context.checks.length ? context.checks : undefined,
    indexes: context.inlineIndexes.length ? context.inlineIndexes : undefined,
  });

  return {
    table,
    refs: context.tableRefs,
  };
}

interface TableContext {
  schema?: string;
  name: string;
  columns: DbmlColumn[];
  tablePkCols: Set<string>;
  tableRefs: DbmlReference[];
  checks: DbmlCheck[];
  inlineIndexes: DbmlIndex[];
}

function handlePrimaryKey (expr: PrimaryKeyExpr, context: TableContext, name: string | undefined): void {
  const cols = (expr.args.expressions ?? []).filter((element): element is Expression => element instanceof Expression);
  const colNames = cols.map(extractNodeText);
  if (1 === colNames.length) {
    context.tablePkCols.add(colNames[0]);
    return;
  }
  if (colNames.length) {
    context.inlineIndexes.push(new DbmlIndex({
      name,
      columns: colNames.map((col) => new DbmlIndexColumn({
        expression: col,
      })),
      pk: true,
    }));
  }
}

function handleForeignKey (expr: ForeignKeyExpr, context: TableContext, name: string | undefined): void {
  const fkCols = (expr.args.expressions ?? []).filter((element): element is Expression => element instanceof Expression);
  const ref = expr.args.reference;
  if (!(ref instanceof ReferenceExpr)) return;
  const inner = ref.args.this;
  const tableExpr = inner instanceof SchemaExpr ? inner.args.this : inner;
  const cols = inner instanceof SchemaExpr ? (inner.args.expressions ?? []) : [];
  const target = extractTableParts(tableExpr as Expression | undefined);
  const refCols = cols.map((col) => col instanceof Expression ? extractNodeText(col) : String(col));
  if (target.name && fkCols.length) {
    const actions = extractReferenceActions(ref);
    context.tableRefs.push(new DbmlReference({
      name,
      relation: DbmlRelation.MANY_TO_ONE,
      source: buildEndpoint(context.schema, context.name, fkCols.map(extractNodeText)),
      target: buildEndpoint(target.schema, target.name, refCols.length ? refCols : fkCols.map(extractNodeText)),
      onDelete: actions.onDelete ?? parseActionExpr(expr.args.delete),
      onUpdate: actions.onUpdate ?? parseActionExpr(expr.args.update),
    }));
  }
}

function handleUnique (expr: UniqueColumnConstraintExpr, context: TableContext, name: string | undefined): void {
  const inner = expr.args.this;
  const cols = inner instanceof SchemaExpr
    ? (inner.args.expressions ?? []).filter((element): element is Expression => element instanceof Expression)
    : [];
  if (!cols.length) return;
  context.inlineIndexes.push(new DbmlIndex({
    name,
    columns: cols.map((col) => new DbmlIndexColumn({
      expression: extractNodeText(col),
    })),
    unique: true,
  }));
}

function handleCheck (inner: Expression, context: TableContext, name: string | undefined): void {
  context.checks.push(new DbmlCheck({
    name,
    expression: inner.sql(),
  }));
}

function dispatchConstraint (expr: Expression, context: TableContext, name: string | undefined): void {
  if (expr instanceof PrimaryKeyExpr) {
    handlePrimaryKey(expr, context, name);
  } else if (expr instanceof ForeignKeyExpr) {
    handleForeignKey(expr, context, name);
  } else if (expr instanceof UniqueColumnConstraintExpr) {
    handleUnique(expr, context, name);
  } else if (expr instanceof CheckColumnConstraintExpr) {
    const checkInner = expr.args.this;
    if (checkInner instanceof Expression) handleCheck(checkInner, context, name);
  } else if (expr instanceof CheckExpr) {
    const checkInner = expr.args.this;
    if (checkInner instanceof Expression) handleCheck(checkInner, context, name);
  } else if (expr instanceof IndexExpr) {
    const built = indexFromParameters(expr);
    if (built) {
      if (name && !built.name) built.name = name;
      context.inlineIndexes.push(built);
    }
  }
}
