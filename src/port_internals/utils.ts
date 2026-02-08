/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unsafe-function-type */

const registeredTargets = new WeakMap<Function, Set<Function>>();

export function baseclass<T extends new (...args: any[]) => object> (Base: T) {
  return function <U extends new (...args: unknown[]) => object>(Target: U): U {
    for (const name of Object.getOwnPropertyNames(Base.prototype)) {
      if (name === 'constructor') continue;
      if (!Object.getOwnPropertyDescriptor(Target.prototype, name)) {
        Object.defineProperty(
          Target.prototype,
          name,
          Object.getOwnPropertyDescriptor(Base.prototype, name)!,
        );
      }
    }

    for (const name of Object.getOwnPropertyNames(Base)) {
      if (['prototype', 'length', 'name'].includes(name)) continue;
      if (!Object.getOwnPropertyDescriptor(Target, name)) {
        Object.defineProperty(
          Target,
          name,
          Object.getOwnPropertyDescriptor(Base, name)!,
        );
      }
    }

    if (!registeredTargets.has(Base)) {
      registeredTargets.set(Base, new Set());
      Object.defineProperty(Base, Symbol.hasInstance, {
        value (instance: unknown) {
          if (Base.prototype.isPrototypeOf(Object(instance))) return true;
          const targets = registeredTargets.get(Base);
          return targets?.has((instance as any)?.constructor) ?? false;
        },
        configurable: true,
      });
    }
    registeredTargets.get(Base)!.add(Target);

    return Target;
  };
}

