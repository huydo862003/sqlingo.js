/**
 * Decorator to enable multiple inheritance in TypeScript.
 * Can be applied multiple times to merge properties and methods from multiple base classes.
 * Supports instanceof checks via Symbol.hasInstance.
 */
/* eslint-disable */
export function baseclass<T extends new (...args: any[]) => any> (Base: T) {
  return function <U extends new (...args: any[]) => any>(Target: U): any {
    // Copy static properties and methods from Base to Target
    Object.getOwnPropertyNames(Base).forEach((name) => {
      if (name !== 'prototype' && name !== 'length' && name !== 'name') {
        const descriptor = Object.getOwnPropertyDescriptor(Base, name);
        if (descriptor) {
          Object.defineProperty(Target, name, descriptor);
        }
      }
    });

    // Copy instance properties and methods from Base.prototype to Target.prototype
    Object.getOwnPropertyNames(Base.prototype).forEach((name) => {
      if (name !== 'constructor') {
        const descriptor = Object.getOwnPropertyDescriptor(Base.prototype, name);
        if (descriptor) {
          Object.defineProperty(Target.prototype, name, descriptor);
        }
      }
    });

    // Track mixed-in bases for instanceof support
    if (!Target.prototype._mixins) {
      Target.prototype._mixins = [];
    }
    Target.prototype._mixins.push(Base);

    // Override Symbol.hasInstance on Base to support instanceof checks
    if (!Object.prototype.hasOwnProperty.call(Base, Symbol.hasInstance)) {
      Object.defineProperty(Base, Symbol.hasInstance, {
        value: function (instance: any) {
          if (instance instanceof this) {
            return true;
          }
          return instance?._mixins?.includes(this);
        },
      });
    }

    return Target;
  };
}
