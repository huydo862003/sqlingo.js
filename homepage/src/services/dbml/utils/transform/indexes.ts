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
} from '../../types';
import {
  extractIdentName, extractNodeText,
} from '../parse/ast';

// Build dbml model's indexes from sqlingo.js AST
export function indexFromParameters (index: IndexExpr): DbmlIndex | undefined {
  const parameters = index.args.params;

  if (!(parameters instanceof IndexParametersExpr)) return undefined;
  const columns_ = parameters.args.columns ?? [];
  const usingRaw = parameters.args.using as unknown;
  const using = usingRaw instanceof Expression
    ? extractIdentName(usingRaw)
    : typeof usingRaw === 'string' ? usingRaw : undefined;

  const columns: DbmlIndexColumn[] = columns_.map((column) => {
    const {
      expr, isExpression,
    } = unwrapIndexColumn(column);

    return new DbmlIndexColumn({
      expression: isExpression ? expr.sql() : extractNodeText(expr),
      isExpression: isExpression || undefined,
    });
  });
  const name = index.args.this ? extractNodeText(index.args.this as Expression) : undefined;

  return new DbmlIndex({
    name,
    columns,
    unique: index.args.unique || undefined,
    pk: index.args.primary || undefined,
    type: using,
  });
}

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
