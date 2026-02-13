/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unsafe-function-type */

const registeredTargets = new WeakMap<Function, Set<Function>>();

/**
 * Creates a class that inherits from multiple base classes.
 * This enables proper multi-inheritance with:
 * - Instance methods and properties from all base classes
 * - Static methods and properties from all base classes
 * - Proper `instanceof` checks for all base classes
 *
 * @param Base - The primary base class (constructor will be called with its signature)
 * @param mixins - Additional classes to inherit from
 * @returns A new class that inherits from all provided classes
 *
 * @example
 * class A {
 *   methodA() { return 'A'; }
 *   static staticA = 'static A';
 * }
 * class B {
 *   methodB() { return 'B'; }
 *   static staticB = 'static B';
 * }
 * class C extends multiInherit(A, B) {
 *   methodC() { return 'C'; }
 * }
 * const c = new C();
 * c.methodA(); // works
 * c.methodB(); // works
 * c instanceof A; // true
 * c instanceof B; // true
 * C.staticA; // accessible
 * C.staticB; // accessible
 */
export function multiInherit<
  TBase extends Constructor,
  TMixins extends readonly Constructor[],
> (
  Base: TBase,
  ...mixins: TMixins
): MultiInheritResult<TBase, TMixins> {
  class MultiInheritClass extends Base {}

  const allBases = [Base, ...mixins];

  for (const BaseClass of allBases) {
    for (const name of Object.getOwnPropertyNames(BaseClass.prototype)) {
      if (name === 'constructor') continue;
      if (!Object.getOwnPropertyDescriptor(MultiInheritClass.prototype, name)) {
        Object.defineProperty(
          MultiInheritClass.prototype,
          name,
          Object.getOwnPropertyDescriptor(BaseClass.prototype, name)!,
        );
      }
    }

    for (const name of Object.getOwnPropertyNames(BaseClass)) {
      if ([
        'prototype',
        'length',
        'name',
      ].includes(name)) continue;
      if (!Object.getOwnPropertyDescriptor(MultiInheritClass, name)) {
        Object.defineProperty(
          MultiInheritClass,
          name,
          Object.getOwnPropertyDescriptor(BaseClass, name)!,
        );
      }
    }

    if (!registeredTargets.has(BaseClass)) {
      registeredTargets.set(BaseClass, new Set());
      Object.defineProperty(BaseClass, Symbol.hasInstance, {
        value (instance: unknown) {
          if (BaseClass.prototype.isPrototypeOf(Object(instance))) return true;
          const targets = registeredTargets.get(BaseClass);
          return targets?.has((instance as any)?.constructor) ?? false;
        },
        configurable: true,
      });
    }
    registeredTargets.get(BaseClass)!.add(MultiInheritClass);
  }

  return MultiInheritClass as any as MultiInheritResult<TBase, TMixins>;
}

/**
 * Extracts the constructor type from a class
 */
type Constructor<T = object> = new (...args: any[]) => T;

/**
 * Extracts the instance type from a constructor
 */
type InstanceType<T> = T extends Constructor<infer U> ? U : never;

/**
 * Merges multiple instance types into a single intersection type
 */
type MergeInstances<T extends readonly Constructor[]> = T extends readonly [
  infer First,
  ...infer Rest extends readonly Constructor[],
]
  ? InstanceType<First> & MergeInstances<Rest>
  : object;

/**
 * Merges static properties from multiple constructors
 * Uses UnionToIntersection to properly merge all static properties
 */
type MergeStatics<T extends readonly Constructor[]> = T extends readonly [
  infer First,
  ...infer Rest extends readonly Constructor[],
]
  ? Omit<First, 'prototype' | 'name' | 'length'> & MergeStatics<Rest>
  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  : {};

/**
 * The return type of the multiInherit function
 *
 * This creates a constructor that:
 * 1. Has the same constructor signature as TBase
 * 2. Creates instances that merge instance properties from all bases
 * 3. Has static properties from all bases merged together
 */
type MultiInheritResult<
  TBase extends Constructor,
  TMixins extends readonly Constructor[],
> = (abstract new (...args: ConstructorParameters<TBase>) => InstanceType<TBase> & MergeInstances<TMixins>)
  & Omit<TBase, 'prototype' | 'name' | 'length'>
  & MergeStatics<TMixins>;
