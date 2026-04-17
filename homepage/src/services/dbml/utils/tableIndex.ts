import {
  ColumnExpr,
  DotExpr,
  Expression,
  IndexParametersExpr,
  OrderedExpr,
  type IndexExpr,
} from '@hdnax/sqlingo.js';
import {
  DbmlIndex, DbmlIndexColumn,
} from '../types';
import {
  identName, nodeText,
} from './name';

function unwrapIndexColumn (e: Expression): {
  expr: Expression;
  isExpression: boolean;
} {
  const inner = e instanceof OrderedExpr && e.args.this instanceof Expression ? e.args.this : e;
  const isExpression = !(inner instanceof ColumnExpr || inner instanceof DotExpr);
  return {
    expr: inner,
    isExpression,
  };
}

export function indexFromParams (idx: IndexExpr): DbmlIndex | undefined {
  const params = idx.args.params;
  if (!(params instanceof IndexParametersExpr)) return undefined;
  const cols = params.args.columns ?? [];
  const usingRaw = params.args.using as unknown;
  const using = usingRaw instanceof Expression
    ? identName(usingRaw)
    : typeof usingRaw === 'string' ? usingRaw : undefined;

  const columns: DbmlIndexColumn[] = cols.map((c) => {
    const {
      expr, isExpression,
    } = unwrapIndexColumn(c);
    return new DbmlIndexColumn({
      expression: isExpression ? expr.sql() : nodeText(expr),
      isExpression: isExpression || undefined,
    });
  });
  const name = idx.args.this ? nodeText(idx.args.this as Expression) : undefined;
  return new DbmlIndex({
    name,
    columns,
    unique: idx.args.unique || undefined,
    pk: idx.args.primary || undefined,
    type: using,
  });
}
