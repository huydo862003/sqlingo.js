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

/**
 * Merges two types where properties in the second type override properties in the first
 * Similar to object spreading: { ...T, ...U }
 */
export type Override<T, U> = Omit<T, keyof U> & U;

/**
 * Merges multiple types from left to right, with later types taking priority
 * Properties in later types override properties in earlier types.
 */
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
