/**
 * Use this function when you want to allow user to specify a dynamic object key to set to an object
 * - Blocks prototype pollution
 */
export function requireSafeDynamicObjectKey (key: unknown) {
  switch (String(key)) {
    case '__proto__':
    case 'prototype':
    case 'constructor':
      throw new Error('Error: Block an attempty to pollute an object prototype');
    default:
      return;
  }
}
