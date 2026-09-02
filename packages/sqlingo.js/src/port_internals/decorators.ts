/**
 * Caches the result of a getter (static or instance) after the first call.
 * Also installs a setter so the cached value can be overridden per-class/instance.
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
  context: ClassGetterDecoratorContext,
): () => T;
/**
 * Wraps a free function so it only evaluates once, then returns the cached result.
 *
 * Usage:
 *   const getFoo = lazy(() => [A, B, C]);
 */
export function cache<T> (
  fn: () => T,
): () => T;
export function cache<T> (
  target: () => T,
  context?: ClassGetterDecoratorContext,
): () => T {
  if (context) {
    const store = new WeakMap<object, T>();

    context.addInitializer(function (this: unknown) {
      Object.defineProperty(this, context.name, {
        get (): T {
          if (!store.has(this)) store.set(this, target.call(this));

          return store.get(this) as T;
        },
        set (value: T) {
          store.set(this, value);
        },
        configurable: true,
      });
    });

    return target;
  }

  let value: T;
  let called = false;

  return () => {
    if (!called) {
      value = target();
      called = true;
    }

    return value;
  };
}
