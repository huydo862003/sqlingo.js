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

  const tableParts = extractTableParts(schemaNode.args.this as Expression | undefined);
  const expressions = schemaNode.args.expressions ?? [];

  const context: TableContext = {
    schema: tableParts.schema,
    name: tableParts.name,
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

  for (const column of context.columns) {
    if (context.tablePkCols.has(column.name)) {
      column.pk = true;
      column.notNull = true;
    }
    for (const inlineReference of column.ref ?? []) {
      context.tableRefs.push(new DbmlReference({
        relation: inlineReference.relation,
        source: buildEndpoint(tableParts.schema, tableParts.name, [column.name]),
        target: inlineReference.target,
        onDelete: inlineReference.onDelete,
        onUpdate: inlineReference.onUpdate,
      }));
    }
  }

  const table = new DbmlTable({
    schema: tableParts.schema,
    name: tableParts.name,
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

function handleCheck (inner: Expression, context: TableContext, name: string | undefined): void {
  context.checks.push(new DbmlCheck({
    name,
    expression: inner.sql(),
  }));
}

function handleForeignKey (expr: ForeignKeyExpr, context: TableContext, name: string | undefined): void {
  const fkColumns = (expr.args.expressions ?? []).filter((element): element is Expression => element instanceof Expression);
  const ref = expr.args.reference;

  if (!(ref instanceof ReferenceExpr)) return;
  const inner = ref.args.this;
  const tableExpr = inner instanceof SchemaExpr ? inner.args.this : inner;
  const columns = inner instanceof SchemaExpr ? (inner.args.expressions ?? []) : [];
  const target = extractTableParts(tableExpr as Expression | undefined);
  const refColumns = columns.map((column) => column instanceof Expression ? extractNodeText(column) : String(column));

  if (target.name && fkColumns.length) {
    const actions = extractReferenceActions(ref);

    context.tableRefs.push(new DbmlReference({
      name,
      relation: DbmlRelation.MANY_TO_ONE,
      source: buildEndpoint(context.schema, context.name, fkColumns.map(extractNodeText)),
      target: buildEndpoint(target.schema, target.name, refColumns.length ? refColumns : fkColumns.map(extractNodeText)),
      onDelete: actions.onDelete ?? parseActionExpr(expr.args.delete),
      onUpdate: actions.onUpdate ?? parseActionExpr(expr.args.update),
    }));
  }
}

function handlePrimaryKey (expr: PrimaryKeyExpr, context: TableContext, name: string | undefined): void {
  const columns = (expr.args.expressions ?? []).filter((element): element is Expression => element instanceof Expression);
  const columnNames = columns.map(extractNodeText);

  if (1 === columnNames.length) {
    context.tablePkCols.add(columnNames[0]);

    return;
  }
  if (columnNames.length) {
    context.inlineIndexes.push(new DbmlIndex({
      name,
      columns: columnNames.map((column) => new DbmlIndexColumn({
        expression: column,
      })),
      pk: true,
    }));
  }
}

function handleUnique (expr: UniqueColumnConstraintExpr, context: TableContext, name: string | undefined): void {
  const inner = expr.args.this;
  const columns = inner instanceof SchemaExpr
    ? (inner.args.expressions ?? []).filter((element): element is Expression => element instanceof Expression)
    : [];

  if (!columns.length) return;
  context.inlineIndexes.push(new DbmlIndex({
    name,
    columns: columns.map((column) => new DbmlIndexColumn({
      expression: extractNodeText(column),
    })),
    unique: true,
  }));
}
