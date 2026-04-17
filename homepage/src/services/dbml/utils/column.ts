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
  DbmlInlineRef,
  DbmlRelation,
} from '../types';
import {
  nodeText, tableParts,
} from './name';
import {
  endpoint, referenceActions,
} from './ref';
import {
  mapDataType,
} from './type';

export function buildColumn (expr: ColumnDefExpr): DbmlColumn {
  const name = nodeText(expr.args.this);
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
  const refs: DbmlInlineRef[] = [];

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
      const t = kind.args.this;
      if (t instanceof Expression) column.default = nodeText(t);
    } else if (kind instanceof CommentColumnConstraintExpr) {
      const t = kind.args.this;
      if (t instanceof Expression) column.note = nodeText(t);
    } else if (kind instanceof CollateColumnConstraintExpr) {
      // collation not representable in DBML
    } else if (kind instanceof ComputedColumnConstraintExpr) {
      column.note = 'VIRTUAL column';
    } else if (kind instanceof CheckColumnConstraintExpr) {
      const t = kind.args.this;
      if (t instanceof Expression) column.check = new DbmlCheck({
        expression: t.sql(),
      });
    } else if (kind instanceof ReferenceExpr) {
      const inner = kind.args.this;
      const tableExpr = inner instanceof SchemaExpr ? inner.args.this : inner;
      const cols = inner instanceof SchemaExpr
        ? (inner.args.expressions ?? [])
        : [];
      const target = tableParts(tableExpr as Expression | undefined);
      const refCols = cols.map((c) => c instanceof Expression ? nodeText(c) : String(c));
      if (target.name) {
        const actions = referenceActions(kind);
        refs.push(new DbmlInlineRef({
          relation: DbmlRelation.MANY_TO_ONE,
          target: endpoint(target.schema, target.name, refCols.length
            ? refCols
            : [name]),
          onDelete: actions.onDelete,
          onUpdate: actions.onUpdate,
        }));
      }
    }
  }

  if (refs.length) column.ref = refs;
  return column;
}
