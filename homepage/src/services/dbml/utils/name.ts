import {
  DotExpr,
  Expression,
  IdentifierExpr,
  TableExpr,
} from '@hdnax/sqlingo.js';

export function nodeText (e: Expression | string | undefined): string {
  if (e === undefined) return '';
  if (typeof e === 'string') return e;
  return e.name || e.sql();
}

export function identName (e: Expression): string {
  if (e instanceof IdentifierExpr) {
    const inner = e.args.this;
    return typeof inner === 'string' ? inner : inner instanceof Expression ? identName(inner) : e.name;
  }
  return e.name || e.sql();
}

export function dotParts (e: Expression): string[] {
  if (e instanceof DotExpr) {
    const parts: string[] = [];
    if (e.args.this instanceof Expression) parts.push(...dotParts(e.args.this));
    if (e.args.expression instanceof Expression) parts.push(...dotParts(e.args.expression));
    return parts;
  }
  return [identName(e)];
}

export interface QualifiedName {
  schema?: string;
  name: string;
}

export function tableParts (e: Expression | undefined): QualifiedName {
  if (!e) return {
    name: '',
  };
  if (e instanceof TableExpr) {
    const thisArg = e.args.this;
    const name = thisArg instanceof Expression ? identName(thisArg) : typeof thisArg === 'string' ? thisArg : '';
    const db = e.args.db instanceof Expression ? identName(e.args.db) : undefined;
    const catalog = e.args.catalog instanceof Expression ? identName(e.args.catalog) : undefined;
    const schema = catalog && db ? `${catalog}.${db}` : db || catalog;
    return {
      ...(schema
        ? {
          schema,
        }
        : {}),
      name,
    };
  }
  if (e instanceof DotExpr) {
    const parts = dotParts(e);
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
    name: identName(e),
  };
}
