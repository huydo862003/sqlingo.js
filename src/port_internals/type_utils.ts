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
