// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/normalize_identifiers.py

import type {
  Expression, IdentifierExpr,
} from '../expressions';
import {
  ColumnExpr, DotExpr, parseIdentifier,
} from '../expressions';
import {
  Dialect, type DialectType,
} from '../dialects/dialect';

/**
 * Normalize identifiers by converting them to either lower or upper case,
 * ensuring the semantics are preserved in each case (e.g. by respecting
 * case-sensitivity).
 *
 * This transformation reflects how identifiers would be resolved by the engine corresponding
 * to each SQL dialect, and plays a very important role in the standardization of the AST.
 *
 * It's possible to make this a no-op by adding a special comment next to the
 * identifier of interest:
 *
 *     SELECT a /* sqlglot.meta case_sensitive *\/ FROM table
 *
 * In this example, the identifier `a` will not be normalized.
 *
 * Note:
 *     Some dialects (e.g. DuckDB) treat all identifiers as case-insensitive even
 *     when they're quoted, so in these cases all identifiers are normalized.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { normalizeIdentifiers } from 'sqlglot/optimizer';
 *
 *     const expression = parseOne('SELECT Bar.A AS A FROM "Foo".Bar');
 *     normalizeIdentifiers(expression).sql();
 *     // 'SELECT bar.a AS a FROM "Foo".bar'
 *
 *     normalizeIdentifiers("foo", { dialect: "snowflake" }).sql({ dialect: "snowflake" });
 *     // 'FOO'
 *     ```
 *
 * @param expression - The expression to transform (Expression or string)
 * @param options - Normalization options
 * @param options.dialect - The dialect to use in order to decide how to normalize identifiers
 * @param options.storeOriginalColumnIdentifiers - Whether to store the original column identifiers
 *   in the meta data of the expression in case we want to undo the normalization at a later point
 * @returns The transformed expression
 */
export function normalizeIdentifiers (
  expression: string,
  options?: {
    dialect?: DialectType;
    storeOriginalColumnIdentifiers?: boolean;
  },
): IdentifierExpr;

export function normalizeIdentifiers<E extends Expression> (
  expression: E,
  options?: {
    dialect?: DialectType;
    storeOriginalColumnIdentifiers?: boolean;
  },
): E;

export function normalizeIdentifiers<E extends Expression> (
  expression: E | string,
  options?: {
    dialect?: DialectType;
    storeOriginalColumnIdentifiers?: boolean;
  },
): E | IdentifierExpr;

export function normalizeIdentifiers (
  expression: string | Expression,
  options: {
    dialect?: DialectType;
    storeOriginalColumnIdentifiers?: boolean;
  } = {},
): Expression | IdentifierExpr {
  const {
    dialect: dialectArg, storeOriginalColumnIdentifiers = false,
  } = options;
  const dialect = Dialect.getOrRaise(dialectArg);

  let expr: Expression;
  if (typeof expression === 'string') {
    expr = parseIdentifier(expression, { dialect });
  } else {
    expr = expression;
  }

  for (const node of expr.walk({ prune: (n) => Boolean(n.meta?.caseSensitive) })) {
    if (!node.meta?.caseSensitive) {
      if (storeOriginalColumnIdentifiers && node instanceof ColumnExpr) {
        // TODO: This does not handle non-column cases, e.g PARSE_JSON(...).key
        let parent: Expression = node;
        while (parent && parent.parent instanceof DotExpr) {
          parent = parent.parent;
        }

        if ('parts' in parent && Array.isArray(parent.parts))
          node.meta.dotParts = parent.parts.map((p) => p.name);
      }

      dialect.normalizeIdentifier(node);
    }
  }

  return expr;
}
