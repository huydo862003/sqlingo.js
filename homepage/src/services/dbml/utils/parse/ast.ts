import {
  DotExpr,
  Expression,
  IdentifierExpr,
  TableExpr,
} from '@hdnax/sqlingo.js';

// Extract text and qualified names from sqlingo.js AST nodes

// Return the text content of a node
export function extractNodeText (node: Expression | string | undefined): string {
  if (node === undefined) return '';
  if (typeof node === 'string') return node;
  return node.name || node.sql();
}

// Return the name of an identifier
export function extractIdentName (expr: Expression): string {
  if (expr instanceof IdentifierExpr) {
    const inner = expr.args.this;
    return typeof inner === 'string' ? inner : inner instanceof Expression ? extractIdentName(inner) : expr.name;
  }
  return expr.name || expr.sql();
}

// Return the string parts of a dot expression
export function extractDotParts (expr: Expression): string[] {
  if (expr instanceof DotExpr) {
    const parts: string[] = [];
    if (expr.args.this instanceof Expression) parts.push(...extractDotParts(expr.args.this));
    if (expr.args.expression instanceof Expression) parts.push(...extractDotParts(expr.args.expression));
    return parts;
  }
  return [extractIdentName(expr)];
}

export interface QualifiedName {
  schema?: string;
  name: string;
}

// Return the schema and name of a table
export function extractTableParts (expr: Expression | undefined): QualifiedName {
  if (!expr) return {
    name: '',
  };
  if (expr instanceof TableExpr) {
    const thisArgument = expr.args.this;
    const name = thisArgument instanceof Expression ? extractIdentName(thisArgument) : typeof thisArgument === 'string' ? thisArgument : '';
    const database = expr.args.db instanceof Expression ? extractIdentName(expr.args.db) : undefined;
    const catalog = expr.args.catalog instanceof Expression ? extractIdentName(expr.args.catalog) : undefined;
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
    const parts = extractDotParts(expr);
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
    name: extractIdentName(expr),
  };
}
