declare const _brand: unique symbol;

export type Id = bigint & { readonly [_brand]: 'Id' };

let _counter = 0n;

const _objectIds = new WeakMap<object, Id>();
const _primitiveIds = new Map<unknown, Id>();

function nextId (): Id {
  return _counter++ as Id;
}

export function id (value: object): Id;
export function id (value: unknown): Id;
export function id (value: unknown): Id {
  if ((value !== null && typeof value === 'object') || typeof value === 'function') {
    const obj = value as object;
    let existing = _objectIds.get(obj);
    if (existing === undefined) {
      existing = nextId();
      _objectIds.set(obj, existing);
    }
    return existing;
  }

  let existing = _primitiveIds.get(value);
  if (existing === undefined) {
    existing = nextId();
    _primitiveIds.set(value, existing);
  }
  return existing;
}
