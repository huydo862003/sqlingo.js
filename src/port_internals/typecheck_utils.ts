/* eslint-disable @typescript-eslint/no-explicit-any */

type AnyConstructor<T = object> =
  | (new (...args: any[]) => T)
  | (abstract new (...args: any[]) => T);

/**
 * Typesafe `instanceof` check. Narrows `value` to `T` if it is an instance of `type`.
 *
 * @example
 * if (is(expr, DataTypeExpr)) {
 *   expr.args.kind; // safely typed as DataTypeExpr
 * }
 */
export function isInstanceOf<T> (value: unknown, type: AnyConstructor<T>): value is T {
  return value instanceof (type as any);
}

/**
 * Asserts that `value` is an instance of `type`, throwing a TypeError if not.
 *
 * @example
 * assertIs(expression, QueryExpr);
 * expression.selects; // safely typed after this
 */
export function assertIsInstanceOf<T> (
  value: unknown,
  type: AnyConstructor<T>,
  msg?: string,
): asserts value is T {
  if (!(value instanceof (type as any))) {
    const name = (type as any).name ?? String(type);
    throw new TypeError(msg ?? `Expected instance of ${name}, got ${typeof value}`);
  }
}

/**
 * Filters an iterable, keeping only instances of the given type.
 *
 * @example
 * const exprs = filterInstanceOf(args.expressions ?? [], Expression);
 * // exprs: Expression[]
 */
export function filterInstanceOf<T> (
  values: Iterable<unknown>,
  type: AnyConstructor<T>,
): T[] {
  return Array.from(values).filter((v): v is T => v instanceof (type as any));
}
