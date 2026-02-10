/**
 * Maps object properties to boolean values based on whether they are required or optional.
 * - Non-optional keys are mapped to `true`.
 * - Optional keys are mapped to `false`.
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
        : K]: {} extends Pick<T, K> ? false : true;
};
