/**
 * Decorator to enable multiple inheritance in TypeScript.
 * Can be applied multiple times to merge properties and methods from multiple base classes.
 */
export function baseclass<T extends new (...args: unknown[]) => unknown> (Base: T) {
  return function <U extends new (...args: unknown[]) => unknown>(Target: U): U {
    // Copy static properties and methods from Base to Target
    Object.getOwnPropertyNames(Base).forEach((name) => {
      const descriptor = Object.getOwnPropertyDescriptor(Base, name);
      if (descriptor) {
        Object.defineProperty(Target, name, descriptor);
      }
    });

    Object.getOwnPropertyNames(Base.prototype).forEach((name) => {
      if (name !== 'constructor') {
        const descriptor = Object.getOwnPropertyDescriptor(Base.prototype, name);
        if (descriptor) {
          Object.defineProperty(Target.prototype, name, descriptor);
        }
      }
    });

    const descriptors = Object.getOwnPropertyDescriptors(Base.prototype);
    for (const [name, descriptor] of Object.entries(descriptors)) {
      if (name !== 'constructor') {
        Object.defineProperty(Target.prototype, name, descriptor);
      }
    }

    return Target;
  };
}
