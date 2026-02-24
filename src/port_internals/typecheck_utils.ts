/* eslint-disable @typescript-eslint/no-explicit-any */

type AnyConstructor<T = object> =
  | (new (...args: any[]) => T)
  | (abstract new (...args: any[]) => T);

/** Maps `typeof` tag strings to their corresponding TypeScript types. */
export interface TypeofMap {
  string: string;
  number: number;
  boolean: boolean;
  bigint: bigint;
  symbol: symbol;
  undefined: undefined;
  function: (...args: any[]) => any;
  object: object | null;
}

type TypeofTag = keyof TypeofMap;

/** A single type checker: either a constructor (for `instanceof`) or a `typeof` tag string. */
type TypeChecker = AnyConstructor<any> | TypeofTag;

type InferType<C> =
  C extends AnyConstructor<infer T> ? T
  : C extends TypeofTag ? TypeofMap[C]
  : never;

/** Resolves the union of all types matched by a tuple of `TypeChecker`s. */
type InferUnion<Cs extends readonly TypeChecker[]> = InferType<Cs[number]>;

function checkOne (value: unknown, type: TypeChecker): boolean {
  return typeof type === 'string'
    ? typeof value === type || (type === 'object' && value === null)
    : value instanceof (type as any);
}

function checkerName (type: TypeChecker): string {
  return typeof type === 'string' ? `typeof === '${type}'` : ((type as any).name ?? String(type));
}

/**
 * Typesafe `instanceof` / `typeof` check. Narrows `value` to the union of all matched types.
 * Accepts any mix of constructors and `typeof` tag strings.
 *
 * @example
 * isInstanceOf(expr, DataTypeExpr, SelectExpr)  // value is DataTypeExpr | SelectExpr
 * isInstanceOf(v, 'string', 'number')            // value is string | number
 * isInstanceOf(v, 'string', MyClass)             // value is string | MyClass
 */
export function isInstanceOf<Cs extends readonly TypeChecker[]> (
  value: unknown,
  ...types: Cs
): value is InferUnion<Cs> {
  return types.some((t) => checkOne(value, t));
}

/**
 * Asserts that `value` matches one of the given type checkers, throwing a `TypeError` if not.
 *
 * @example
 * assertIsInstanceOf(expression, QueryExpr);
 * assertIsInstanceOf(val, 'string', 'number');
 */
export function assertIsInstanceOf<Cs extends readonly TypeChecker[]> (
  value: unknown,
  ...types: Cs
): asserts value is InferUnion<Cs> {
  if (!types.some((t) => checkOne(value, t))) {
    const expected = types.map(checkerName).join(' | ');
    throw new TypeError(`Expected ${expected}, got ${typeof value}`);
  }
}

/**
 * Narrows a singular value to the union of matched types, returning `undefined` if it doesn't match.
 *
 * @example
 * narrowInstanceOf(expr, Expression, 'string')  // Expression | string | undefined
 */
export function narrowInstanceOf<Cs extends readonly TypeChecker[]> (
  value: unknown,
  ...types: Cs
): InferUnion<Cs> | undefined {
  return types.some((t) => checkOne(value, t)) ? value as InferUnion<Cs> : undefined;
}

/**
 * Filters an iterable, keeping only values that match one of the given type checkers.
 *
 * @example
 * filterInstanceOf(args, Expression, 'string')  // (Expression | string)[]
 */
export function filterInstanceOf<Cs extends readonly TypeChecker[]> (
  values: Iterable<unknown>,
  ...types: Cs
): InferUnion<Cs>[] {
  const result: InferUnion<Cs>[] = [];
  for (const v of values) {
    if (types.some((t) => checkOne(v, t))) result.push(v as InferUnion<Cs>);
  }
  return result;
}
