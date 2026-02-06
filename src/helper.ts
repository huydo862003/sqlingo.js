export function ensureList<T> (value: T | T[]): T[] {
  return Array.isArray(value) ? value : [value];
}

export function ensureCollection<T> (value: T | T[] | Set<T>): T[] {
  if (Array.isArray(value)) return value;
  if (value instanceof Set) return Array.from(value);
  return [value];
}
