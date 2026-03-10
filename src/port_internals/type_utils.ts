import type { PrimitiveExpressionValue } from '../expressions';

/** Type guard that checks if a value is iterable (has `Symbol.iterator`). Does NOT exclude strings. */
export function isIterable<T = unknown> (value: unknown): value is Iterable<T> {
  return value != null && typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] === 'function';
}

/** Merges T and U, with U overriding T. Properties set to `never` in U are removed. */
export type Override<T, U> = Omit<T, keyof U> & U;

/** Merges multiple types left-to-right, with later types taking priority. */
export type Merge<Types extends readonly unknown[]> = Types extends readonly [
  infer First,
  infer Second,
  ...infer Rest,
]
  ? Merge<[Override<First, Second>, ...Rest]>
  : Types extends readonly [infer Only]
    ? Only
    : Types extends readonly []
      // eslint-disable-next-line @typescript-eslint/no-empty-object-type
      ? {}
      : never;

export interface Pojo {
  [index: string]: Pojo | PrimitiveExpressionValue | undefined | (Pojo | PrimitiveExpressionValue | undefined)[];
}
