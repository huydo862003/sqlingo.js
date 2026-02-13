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

/** Removes properties with `never` values */
type OmitNever<T> = {
  [K in keyof T as T[K] extends never ? never : K]: T[K];
};

/** Sets all properties of T to `never` for removal via Merge */
export type RemoveAll<T> = { [K in keyof T]?: never };

/** Merges T and U, with U overriding T. Properties set to `never` in U are removed. */
export type Override<T, U> = OmitNever<Omit<T, keyof U> & U>;

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
