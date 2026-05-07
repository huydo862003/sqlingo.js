import {
  DataTypeExprKind,
  DotExpr,
  Expression,
  IdentifierExpr,
  type DataTypeExpr,
} from '@hdnax/sqlingo.js';
import {
  DbmlColumnType,
} from '../../types/column';

// Map sqlingo.js data types to dbml model column types

export function mapDataType (dtype: DataTypeExpr): DbmlColumnType {
  const kind = dtype.args.this;
  const exprs = dtype.args.expressions ?? [];

  if (kind === DataTypeExprKind.ARRAY && 0 < exprs.length && exprs[0] instanceof Expression) {
    const inner = mapDataType(exprs[0] as DataTypeExpr);
    const nextArray: boolean | (number | undefined)[] = inner.array
      ? [
        ...(Array.isArray(inner.array)
          ? inner.array
          : [undefined]),
        undefined,
      ]
      : true;
    return new DbmlColumnType({
      schema: inner.schema,
      name: inner.name,
      args: inner.args,
      array: nextArray,
    });
  }

  if (typeof kind === 'string' && kind in ALIAS) {
    const name = ALIAS[kind as DataTypeExprKind]!;
    const arguments_ = exprs.map(extractExprText);
    return new DbmlColumnType({
      name,
      args: arguments_.length ? arguments_ : undefined,
    });
  }

  if (kind instanceof Expression) {
    const qualified = extractQualifiedName(kind);
    return new DbmlColumnType({
      schema: qualified.schema,
      name: qualified.name,
    });
  }

  if (typeof kind === 'string') {
    return new DbmlColumnType({
      name: kind,
    });
  }

  return new DbmlColumnType({
    name: 'varchar',
  });
}

const ALIAS: Partial<Record<DataTypeExprKind, string>> = {
  [DataTypeExprKind.INT]: 'int',
  [DataTypeExprKind.INT128]: 'int',
  [DataTypeExprKind.INT256]: 'int',
  [DataTypeExprKind.BIGINT]: 'bigint',
  [DataTypeExprKind.BIGSERIAL]: 'bigint',
  [DataTypeExprKind.SMALLINT]: 'smallint',
  [DataTypeExprKind.TINYINT]: 'tinyint',
  [DataTypeExprKind.MEDIUMINT]: 'mediumint',
  [DataTypeExprKind.FLOAT]: 'float',
  [DataTypeExprKind.DOUBLE]: 'double',
  [DataTypeExprKind.DECIMAL]: 'decimal',
  [DataTypeExprKind.BIGDECIMAL]: 'decimal',
  [DataTypeExprKind.BOOLEAN]: 'boolean',
  [DataTypeExprKind.CHAR]: 'char',
  [DataTypeExprKind.NCHAR]: 'nchar',
  [DataTypeExprKind.VARCHAR]: 'varchar',
  [DataTypeExprKind.NVARCHAR]: 'nvarchar',
  [DataTypeExprKind.TEXT]: 'text',
  [DataTypeExprKind.LONGTEXT]: 'text',
  [DataTypeExprKind.MEDIUMTEXT]: 'text',
  [DataTypeExprKind.TINYTEXT]: 'text',
  [DataTypeExprKind.BLOB]: 'blob',
  [DataTypeExprKind.LONGBLOB]: 'blob',
  [DataTypeExprKind.MEDIUMBLOB]: 'blob',
  [DataTypeExprKind.TINYBLOB]: 'blob',
  [DataTypeExprKind.DATE]: 'date',
  [DataTypeExprKind.DATETIME]: 'datetime',
  [DataTypeExprKind.DATETIME2]: 'datetime',
  [DataTypeExprKind.TIMESTAMP]: 'timestamp',
  [DataTypeExprKind.TIMESTAMPTZ]: 'timestamp',
  [DataTypeExprKind.TIMESTAMPLTZ]: 'timestamp',
  [DataTypeExprKind.TIMESTAMPNTZ]: 'timestamp',
  [DataTypeExprKind.TIME]: 'time',
  [DataTypeExprKind.TIMETZ]: 'time',
  [DataTypeExprKind.JSON]: 'json',
  [DataTypeExprKind.JSONB]: 'jsonb',
  [DataTypeExprKind.UUID]: 'uuid',
  [DataTypeExprKind.SERIAL]: 'serial',
  [DataTypeExprKind.SMALLSERIAL]: 'smallserial',
  [DataTypeExprKind.BINARY]: 'binary',
  [DataTypeExprKind.BIT]: 'binary',
  [DataTypeExprKind.ENUM]: 'enum',
};

function extractExprText (node: unknown): string {
  if (node instanceof Expression) return node.name || node.sql();
  return String(node);
}

function extractIdentName (expr: Expression): string {
  if (expr instanceof IdentifierExpr) {
    const inner = expr.args.this;
    return typeof inner === 'string' ? inner : inner instanceof Expression ? extractIdentName(inner) : expr.name;
  }
  return expr.name || expr.sql();
}

function extractQualifiedName (node: Expression): {
  schema?: string;
  name: string;
} {
  if (node instanceof DotExpr) {
    const parts: string[] = [];
    const walk = (n: Expression): void => {
      if (n instanceof DotExpr) {
        if (n.args.this instanceof Expression) walk(n.args.this);
        if (n.args.expression instanceof Expression) walk(n.args.expression);
      } else {
        parts.push(extractIdentName(n));
      }
    };
    walk(node);
    return {
      schema: 1 < parts.length ? parts.slice(0, -1).join('.') : undefined,
      name: parts[parts.length - 1],
    };
  }
  return {
    name: extractIdentName(node),
  };
}
