/**
 * Caches the result of a getter (static or instance) after the first call.
 *
 * Usage:
 *   @cache
 *   static get FOO(): SomeType { return { ... }; }
 *
 *   @cache
 *   get bar(): SomeType { return { ... }; }
 */
export function cache<T> (
  target: () => T,
  _context: ClassGetterDecoratorContext,
): () => T {
  const store = new WeakMap<object, T>();
  return function (this: object): T {
    if (!store.has(this)) {
      store.set(this, target.call(this));
    }
    return store.get(this) as T;
  };
}
