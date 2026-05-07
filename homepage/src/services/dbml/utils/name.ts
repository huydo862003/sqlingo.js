import {
  DotExpr,
  Expression,
  IdentifierExpr,
  TableExpr,
} from '@hdnax/sqlingo.js';

export function nodeText (node: Expression | string | undefined): string {
  if (node === undefined) return '';
  if (typeof node === 'string') return node;
  return node.name || node.sql();
}

export function identName (expr: Expression): string {
  if (expr instanceof IdentifierExpr) {
    const inner = expr.args.this;
    return typeof inner === 'string' ? inner : inner instanceof Expression ? identName(inner) : expr.name;
  }
  return expr.name || expr.sql();
}

export function dotParts (expr: Expression): string[] {
  if (expr instanceof DotExpr) {
    const parts: string[] = [];
    if (expr.args.this instanceof Expression) parts.push(...dotParts(expr.args.this));
    if (expr.args.expression instanceof Expression) parts.push(...dotParts(expr.args.expression));
    return parts;
  }
  return [identName(expr)];
}

export interface QualifiedName {
  schema?: string;
  name: string;
}

export function tableParts (expr: Expression | undefined): QualifiedName {
  if (!expr) return {
    name: '',
  };
  if (expr instanceof TableExpr) {
    const thisArgument = expr.args.this;
    const name = thisArgument instanceof Expression ? identName(thisArgument) : typeof thisArgument === 'string' ? thisArgument : '';
    const database = expr.args.db instanceof Expression ? identName(expr.args.db) : undefined;
    const catalog = expr.args.catalog instanceof Expression ? identName(expr.args.catalog) : undefined;
    const schema = catalog && database ? `${catalog}.${database}` : database || catalog;
    return {
      ...(schema
        ? {
          schema,
        }
        : {}),
      name,
    };
  }
  if (expr instanceof DotExpr) {
    const parts = dotParts(expr);
    return {
      ...(1 < parts.length
        ? {
          schema: parts.slice(0, -1).join('.'),
        }
        : {}),
      name: parts[parts.length - 1],
    };
  }
  return {
    name: identName(expr),
  };
}
