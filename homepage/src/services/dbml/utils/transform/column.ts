import {
  AutoIncrementColumnConstraintExpr,
  CheckColumnConstraintExpr,
  ColumnConstraintExpr,
  ComputedColumnConstraintExpr,
  CollateColumnConstraintExpr,
  CommentColumnConstraintExpr,
  DataTypeExpr,
  DefaultColumnConstraintExpr,
  Expression,
  GeneratedAsIdentityColumnConstraintExpr,
  NotNullColumnConstraintExpr,
  PrimaryKeyColumnConstraintExpr,
  ReferenceExpr,
  SchemaExpr,
  UniqueColumnConstraintExpr,
  type ColumnDefExpr,
} from '@hdnax/sqlingo.js';
import {
  DbmlCheck,
  DbmlColumn,
  DbmlColumnType,
  DbmlInlineReference,
  DbmlRelation,
} from '../../types';
import {
  extractNodeText, extractTableParts,
} from '../parse/ast';
import {
  buildEndpoint, extractReferenceActions,
} from './ref';
import {
  mapDataType,
} from './type';

// Build dbml model columns from sqlingo.js AST

export function buildDbmlColumn (expr: ColumnDefExpr): DbmlColumn {
  const name = extractNodeText(expr.args.this);
  const typeExpr = expr.args.kind;
  const type: DbmlColumnType = typeExpr instanceof DataTypeExpr
    ? mapDataType(typeExpr)
    : new DbmlColumnType({
      name: typeof typeExpr === 'string' ? typeExpr : 'varchar',
    });

  const column = new DbmlColumn({
    name,
    type,
  });
  const references: DbmlInlineReference[] = [];

  for (const constraintExpr of expr.constraints) {
    if (!(constraintExpr instanceof ColumnConstraintExpr)) continue;
    const kind = constraintExpr.args.kind;
    if (!kind) continue;

    if (kind instanceof PrimaryKeyColumnConstraintExpr) {
      column.pk = true;
      column.notNull = true;
    } else if (kind instanceof NotNullColumnConstraintExpr) {
      column.notNull = !kind.args.allowNull;
    } else if (kind instanceof UniqueColumnConstraintExpr) {
      column.unique = true;
    } else if (kind instanceof AutoIncrementColumnConstraintExpr) {
      column.increment = true;
    } else if (kind instanceof GeneratedAsIdentityColumnConstraintExpr) {
      column.increment = true;
    } else if (kind instanceof DefaultColumnConstraintExpr) {
      const inner = kind.args.this;
      if (inner instanceof Expression) column.default = extractNodeText(inner);
    } else if (kind instanceof CommentColumnConstraintExpr) {
      const inner = kind.args.this;
      if (inner instanceof Expression) column.note = extractNodeText(inner);
    } else if (kind instanceof CollateColumnConstraintExpr) {
      // collation not representable in DBML
    } else if (kind instanceof ComputedColumnConstraintExpr) {
      column.note = 'VIRTUAL column';
    } else if (kind instanceof CheckColumnConstraintExpr) {
      const checkExpr = kind.args.this;
      if (checkExpr instanceof Expression) column.check = new DbmlCheck({
        expression: checkExpr.sql(),
      });
    } else if (kind instanceof ReferenceExpr) {
      const inner = kind.args.this;
      const tableExpr = inner instanceof SchemaExpr ? inner.args.this : inner;
      const cols = inner instanceof SchemaExpr
        ? (inner.args.expressions ?? [])
        : [];
      const target = extractTableParts(tableExpr as Expression | undefined);
      const refCols = cols.map((col) => col instanceof Expression ? extractNodeText(col) : String(col));
      if (target.name) {
        const actions = extractReferenceActions(kind);
        references.push(new DbmlInlineReference({
          relation: DbmlRelation.MANY_TO_ONE,
          target: buildEndpoint(target.schema, target.name, refCols.length
            ? refCols
            : [name]),
          onDelete: actions.onDelete,
          onUpdate: actions.onUpdate,
        }));
      }
    }
  }

  if (references.length) column.ref = references;
  return column;
}
