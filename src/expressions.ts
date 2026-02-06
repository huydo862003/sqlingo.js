import type { Dialects } from './dialects';

export class Expression {
  // Base expression class
}

export function aliasExpr (expression: unknown, alias: string): Expression {
  throw new Error(`alias not implemented: ${expression}, ${alias}`);
}

export function andExpr (...conditions: unknown[]): Expression {
  throw new Error(`and not implemented: ${conditions.length} conditions`);
}

export function caseExpr (): Expression {
  throw new Error('case_ not implemented');
}

export function castExpr (expression: unknown, to: string): Expression {
  throw new Error(`cast not implemented: ${expression}, ${to}`);
}

export function columnExpr (name: string, table?: string): Expression {
  throw new Error(`column not implemented: ${name}, ${table}`);
}

export function conditionExpr (sql: string): Expression {
  throw new Error(`condition not implemented: ${sql}`);
}

export function deleteExpr (table: string): Expression {
  throw new Error(`delete_ not implemented: ${table}`);
}

export function exceptExpr (...selects: unknown[]): Expression {
  throw new Error(`except_ not implemented: ${selects.length} selects`);
}

export function fromExpr (table: string): Expression {
  throw new Error(`from_ not implemented: ${table}`);
}

export function funcExpr (name: string, ...args: unknown[]): Expression {
  throw new Error(`func not implemented: ${name}, ${args.length} args`);
}

export function insertExpr (table: string, ...columns: unknown[]): Expression {
  throw new Error(`insert not implemented: ${table}, ${columns.length} columns`);
}

export function intersectExpr (...selects: unknown[]): Expression {
  throw new Error(`intersect not implemented: ${selects.length} selects`);
}

export function maybeParseExpr (sql: string | Expression, dialect?: Dialects): Expression {
  throw new Error(`maybeParse not implemented: ${sql}, ${dialect}`);
}

export function mergeExpr (target: string): Expression {
  throw new Error(`merge not implemented: ${target}`);
}

export function notExpr (condition: unknown): Expression {
  throw new Error(`not_ not implemented: ${condition}`);
}

export function orExpr (...conditions: unknown[]): Expression {
  throw new Error(`or_ not implemented: ${conditions.length} conditions`);
}

export function selectExpr (...columns: unknown[]): Expression {
  throw new Error(`select not implemented: ${columns.length} columns`);
}

export function subqueryExpr (expression: unknown, alias?: string): Expression {
  throw new Error(`subquery not implemented: ${expression}, ${alias}`);
}

export function tableExpr (name: string, db?: string, catalog?: string): Expression {
  throw new Error(`table not implemented: ${name}, ${db}, ${catalog}`);
}

export function toColumn (sql: string | Expression, dialect?: Dialects): Expression {
  throw new Error(`toColumn not implemented: ${sql}, ${dialect}`);
}

export function toIdentifier (name: string, quoted?: boolean): Expression {
  throw new Error(`toIdentifier not implemented: ${name}, ${quoted}`);
}

export function toTable (sql: string | Expression, dialect?: Dialects): Expression {
  throw new Error(`toTable not implemented: ${sql}, ${dialect}`);
}

export function unionExpr (...selects: unknown[]): Expression {
  throw new Error(`union not implemented: ${selects.length} selects`);
}

export function findTables (expression: Expression): string[] {
  throw new Error(`findTables not implemented: ${expression}`);
}
