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

function unwrapIndexColumn (node: Expression): {
  expr: Expression;
  isExpression: boolean;
} {
  const inner = node instanceof OrderedExpr && node.args.this instanceof Expression ? node.args.this : node;
  const isExpr = !(inner instanceof ColumnExpr || inner instanceof DotExpr);
  return {
    expr: inner,
    isExpression: isExpr,
  };
}

export function indexFromParameters (index: IndexExpr): DbmlIndex | undefined {
  const parameters = index.args.params;
  if (!(parameters instanceof IndexParametersExpr)) return undefined;
  const cols = parameters.args.columns ?? [];
  const usingRaw = parameters.args.using as unknown;
  const using = usingRaw instanceof Expression
    ? identName(usingRaw)
    : typeof usingRaw === 'string' ? usingRaw : undefined;

  const columns: DbmlIndexColumn[] = cols.map((col) => {
    const {
      expr, isExpression,
    } = unwrapIndexColumn(col);
    return new DbmlIndexColumn({
      expression: isExpression ? expr.sql() : nodeText(expr),
      isExpression: isExpression || undefined,
    });
  });
  const name = index.args.this ? nodeText(index.args.this as Expression) : undefined;
  return new DbmlIndex({
    name,
    columns,
    unique: index.args.unique || undefined,
    pk: index.args.primary || undefined,
    type: using,
  });
}
