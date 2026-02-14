/**
 * Maps object properties to boolean values based on whether they are required or optional.
 * - Non-optional keys are mapped to `true`.
 * - Optional keys are mapped to `boolean` (to allow overriding optionality in subclasses).
 * - Index signatures are filtered out.
 */
export type RequiredMap<T> = {
  [K in keyof T as string extends K
    ? never
    : number extends K
      ? never
      : symbol extends K
        ? never
        // eslint-disable-next-line @typescript-eslint/no-empty-object-type
        : K]: {} extends Pick<T, K> ? boolean : true;
};

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
