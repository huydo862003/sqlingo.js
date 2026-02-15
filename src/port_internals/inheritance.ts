/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unsafe-function-type */

const registeredTargets = new WeakMap<Function, Set<Function>>();

/**
 * Walks up the prototype chain and collects all classes in order
 * (simulates Python's MRO - Method Resolution Order)
 */
function getPrototypeChain (Class: Constructor): Constructor[] {
  const chain: Constructor[] = [];
  let current: any = Class;

  while (current && current !== Object && current.prototype) {
    chain.push(current);
    current = Object.getPrototypeOf(current);
  }

  return chain;
}

/**
 * Creates a class that inherits from multiple base classes.
 * This enables proper multi-inheritance with:
 * - Instance methods and properties from all base classes AND their ancestors
 * - Static methods and properties from all base classes AND their ancestors
 * - Proper `instanceof` checks for all base classes
 * - Respects Python's MRO (Method Resolution Order) - first class wins
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

  // Build MRO: [Base chain, Mixin1 chain, Mixin2 chain, ...]
  // This follows Python's MRO where earlier bases have higher priority
  const allBases = [Base, ...mixins];
  const mro: Constructor[] = [];
  const seen = new Set<Constructor>();

  for (const BaseClass of allBases) {
    const chain = getPrototypeChain(BaseClass);
    for (const cls of chain) {
      if (!seen.has(cls)) {
        seen.add(cls);
        mro.push(cls);
      }
    }
  }

  // Copy instance methods and properties following MRO
  // "First-wins" policy: if a property already exists, don't override
  for (const BaseClass of mro) {
    for (const name of Object.getOwnPropertyNames(BaseClass.prototype)) {
      if (name === 'constructor') continue;
      if (!Object.getOwnPropertyDescriptor(MultiInheritClass.prototype, name)) {
        const descriptor = Object.getOwnPropertyDescriptor(BaseClass.prototype, name);
        if (descriptor) {
          Object.defineProperty(MultiInheritClass.prototype, name, descriptor);
        }
      }
    }

    // Copy static methods and properties following MRO
    for (const name of Object.getOwnPropertyNames(BaseClass)) {
      if ([
        'prototype',
        'length',
        'name',
      ].includes(name)) continue;
      if (!Object.getOwnPropertyDescriptor(MultiInheritClass, name)) {
        const descriptor = Object.getOwnPropertyDescriptor(BaseClass, name);
        if (descriptor) {
          Object.defineProperty(MultiInheritClass, name, descriptor);
        }
      }
    }

    // Register for instanceof checks
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
