// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/normalize.py

import type { Expression } from '../expressions/expressions';
import {
  and as andExpr,
  AndExpr,
  ConnectorExpr,
  or as orExpr,
  OrExpr,
  replaceChildren,
} from '../expressions/expressions';
import { OptimizeError } from '../errors';
import { whileChanging } from '../helper';
import { findAllInScope } from './scope';
import {
  flatten, Simplifier,
} from './simplify';

/**
 * Rewrite sqlglot AST into conjunctive normal form or disjunctive normal form.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { normalize } from 'sqlglot/optimizer';
 *
 *     const expression = parseOne("(x AND y) OR z");
 *     normalize(expression, { dnf: false }).sql();
 *     // '(x OR z) AND (y OR z)'
 *     ```
 *
 * @param expression - Expression to normalize
 * @param options - Normalization options
 * @param options.dnf - Rewrite in disjunctive normal form instead of CNF (default: false)
 * @param options.maxDistance - Max estimated distance from CNF/DNF to attempt conversion (default: 128)
 * @returns Normalized expression
 */
export function normalize (
  expression: Expression,
  options: {
    dnf?: boolean;
    maxDistance?: number;
    [index: string]: unknown;
  } = {},
): Expression {
  const {
    dnf = false, maxDistance = 128,
  } = options;
  const simplifier = new Simplifier({ annotateNewExpressions: false });

  // Walk only top-level connectors — prune at each connector so nested ones
  // are handled by distributiveLaw's own recursion (mirrors Python's prune).
  const connectors: ConnectorExpr[] = [];
  for (const node of expression.walk({ prune: (e) => e instanceof ConnectorExpr })) {
    if (node instanceof ConnectorExpr) {
      connectors.push(node);
    }
  }

  for (let node of connectors) {
    if (normalized(node, { dnf })) {
      continue;
    }

    const root = node === expression;
    const original = node.copy();

    node.transform((expr) => simplifier.rewriteBetween(expr), { copy: false });

    const distance = normalizationDistance(node, {
      dnf,
      max: maxDistance,
    });

    if (maxDistance < distance) {
      return expression;
    }

    try {
      const newNode = whileChanging(node, (e: Expression) =>
        distributiveLaw(e, {
          dnf,
          maxDistance,
          simplifier,
        }));
      node = node.replace(newNode) as ConnectorExpr;
    } catch (e) {
      if (e instanceof OptimizeError) {
        node.replace(original);
        if (root) {
          return original;
        }
        return expression;
      }
      throw e;
    }

    if (root) {
      expression = node;
    }
  }

  return expression;
}

/**
 * Checks whether a given expression is in a normal form of interest.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { normalized } from 'sqlglot/optimizer';
 *
 *     normalized(parseOne("(a AND b) OR c OR (d AND e)"), true);  // DNF - true
 *     normalized(parseOne("(a OR b) AND c"));  // CNF - true
 *     normalized(parseOne("a AND (b OR c)"), true);  // DNF - false
 *     ```
 *
 * @param expression - The expression to check if it's normalized
 * @param dnf - Whether to check for DNF (default: false = check for CNF)
 * @returns True if normalized
 */
export function normalized (expression: Expression, options: { dnf?: boolean } = {}): boolean {
  const { dnf = false } = options;
  // For DNF: check that no And has an Or ancestor
  // For CNF: check that no Or has an And ancestor
  const [ancestor, root] = dnf ? [AndExpr, OrExpr] : [OrExpr, AndExpr];

  return !findAllInScope(expression, [root]).some(
    (connector) => connector.findAncestor(ancestor),
  );
}

/**
 * The difference in the number of predicates between a given expression and its normalized form.
 *
 * This is used as an estimate of the cost of the conversion which is exponential in complexity.
 *
 * Example:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { normalizationDistance } from 'sqlglot/optimizer';
 *
 *     const expression = parseOne("(a AND b) OR (c AND d)");
 *     normalizationDistance(expression);  // 4
 *     ```
 *
 * @param expression - The expression to compute the normalization distance for
 * @param dnf - Whether to check for DNF (default: false = check for CNF)
 * @param max - Stop early if count exceeds this (default: Infinity)
 * @returns The normalization distance
 */
export function normalizationDistance (
  expression: Expression,
  options: { dnf?: boolean;
    max?: number; } = {},
): number {
  const {
    dnf = false, max = Infinity,
  } = options;
  // Start with negative count of connectors
  let total = -1;
  for (const _ of expression.findAll(ConnectorExpr)) {
    total -= 1;
  }

  // Add the lengths of all predicates when expanded
  for (const length of predicateLengths(expression, {
    dnf,
    max,
  })) {
    total += length;
    if (max < total) {
      return total;
    }
  }

  return total;
}

/**
 * Returns predicate lengths when expanded to normalized form.
 *
 * (A AND B) OR C -> [2, 2] because len(A OR C), len(B OR C)
 */
function* predicateLengths (
  expression: Expression,
  options: { dnf: boolean;
    max?: number;
    depth?: number; },
): Generator<number> {
  const {
    dnf, max = Infinity, depth: depth0 = 0,
  } = options;
  if (max < depth0) {
    yield depth0;
    return;
  }

  const unnested = expression.unnest();

  if (!(unnested instanceof ConnectorExpr)) {
    yield 1;
    return;
  }

  const depth = depth0 + 1;

  const left = unnested.left;
  const right = unnested.right;

  if (!left || !right) {
    yield 1;
    return;
  }

  const expandType = dnf ? AndExpr : OrExpr;

  if (unnested instanceof expandType) {
    // This is the expanding operator - multiply lengths
    for (const a of predicateLengths(left, {
      dnf,
      max,
      depth,
    })) {
      for (const b of predicateLengths(right, {
        dnf,
        max,
        depth,
      })) {
        yield a + b;
      }
    }
  } else {
    // This is the other operator - just yield from both
    yield* predicateLengths(left, {
      dnf,
      max,
      depth,
    });
    yield* predicateLengths(right, {
      dnf,
      max,
      depth,
    });
  }
}

/**
 * Apply distributive law to normalize expression.
 *
 * x OR (y AND z) -> (x OR y) AND (x OR z)
 * (x AND y) OR (y AND z) -> (x OR y) AND (x OR z) AND (y OR y) AND (y OR z)
 */
function distributiveLaw (
  expression: Expression,
  options: { dnf: boolean;
    maxDistance: number;
    simplifier?: Simplifier; },
): Expression {
  const {
    dnf, maxDistance, simplifier,
  } = options;
  if (normalized(expression, { dnf })) {
    return expression;
  }

  const distance = normalizationDistance(expression, {
    dnf,
    max: maxDistance,
  });

  if (maxDistance < distance) {
    throw new OptimizeError(`Normalization distance ${distance} exceeds max ${maxDistance}`);
  }

  replaceChildren(expression, (e) => distributiveLaw(e, options));

  const [toExp, fromExp] = dnf ? [OrExpr, AndExpr] : [AndExpr, OrExpr];

  if (!(expression instanceof fromExp)) {
    return expression;
  }

  const [a, b] = expression.unnestOperands();
  if (!a || !b) {
    return expression;
  }

  const fromFunc = fromExp === AndExpr ? andExpr : orExpr;
  const toFunc = toExp === AndExpr ? andExpr : orExpr;

  const simplifierInstance = simplifier ?? new Simplifier({ annotateNewExpressions: false });

  if (a instanceof toExp && b instanceof toExp) {
    const aConnectorCount = Array.from(a.findAll(ConnectorExpr)).length;
    const bConnectorCount = Array.from(b.findAll(ConnectorExpr)).length;

    if (bConnectorCount < aConnectorCount) {
      return distribute(a, b, fromFunc, toFunc, simplifierInstance);
    }
    return distribute(b, a, fromFunc, toFunc, simplifierInstance);
  }

  if (a instanceof toExp) {
    return distribute(b, a, fromFunc, toFunc, simplifierInstance);
  }

  if (b instanceof toExp) {
    return distribute(a, b, fromFunc, toFunc, simplifierInstance);
  }

  return expression;
}

/**
 * Distribute a over b using the distributive law.
 */
function distribute (
  a: Expression,
  b: ConnectorExpr,
  fromFunc: typeof andExpr | typeof orExpr,
  toFunc: typeof andExpr | typeof orExpr,
  simplifier: Simplifier,
): Expression {
  const bLeft = b.left;
  const bRight = b.right;

  if (!bLeft || !bRight) {
    return a;
  }

  if (a instanceof ConnectorExpr) {
    replaceChildren(a, (c) => {
      const leftCombined = flatten(fromFunc([c, bLeft]));
      const rightCombined = flatten(fromFunc([c, bRight]));
      return toFunc([simplifier.uniqSort(leftCombined), simplifier.uniqSort(rightCombined)], { copy: false });
    });
    return a;
  }

  const leftCombined = flatten(fromFunc([a, bLeft]));
  const rightCombined = flatten(fromFunc([a, bRight]));
  return toFunc([simplifier.uniqSort(leftCombined), simplifier.uniqSort(rightCombined)], { copy: false });
}
