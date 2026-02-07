// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py

// Not ported from Python: AutoName, classproperty, subclasses, apply_index_offset, object_to_dict, is_iterable, is_date_unit

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L22
const CAMEL_CASE_PATTERN = /(?<!^)(?=[A-Z])/;

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L48
export function suggestClosestMatchAndFail (
  kind: string,
  word: string,
  possibilities: string[],
): never {
  const closeMatches = getCloseMatches(word, possibilities, 1);

  const similar = seqGet(closeMatches, 0);
  const suggestion = similar ? ` Did you mean ${similar}?` : '';

  throw new Error(`Unknown ${kind} '${word}'.${suggestion}`);
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L62
export function seqGet<T> (seq: T[], index: number): T | undefined {
  return index >= 0 && index < seq.length ? seq[index] : undefined;
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L82
export function ensureList<T> (value?: T | T[]): T[] {
  if (value === undefined) {
    return [];
  }
  if (Array.isArray(value)) {
    return value;
  }
  return [value];
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L108
export function ensureCollection<T> (value: T | T[]): T[] {
  if (value === undefined) {
    return [];
  }
  if (Array.isArray(value)) {
    return value;
  }
  return [value];
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L125
export function csv (...args: string[]): string;
export function csv (sep: string, ...args: string[]): string;
export function csv (...args: string[]): string {
  const sep = args.length > 0 && args[0].length <= 3 && args[0].includes(',') ? args.shift() : ', ';
  return args.filter((arg) => arg).join(sep || ', ');
}

function getCloseMatches (
  word: string,
  possibilities: string[],
  n: number,
  cutoff: number = 0.6,
): string[] {
  const results: { match: string; ratio: number }[] = [];

  for (const possibility of possibilities) {
    const ratio = similarity(word, possibility);
    if (ratio >= cutoff) {
      results.push({ match: possibility, ratio });
    }
  }

  results.sort((a, b) => b.ratio - a.ratio);
  return results.slice(0, n).map((r) => r.match);
}

function similarity (a: string, b: string): number {
  const longer = a.length > b.length ? a : b;
  const shorter = a.length > b.length ? b : a;

  if (longer.length === 0) return 1.0;

  const editDistance = levenshteinDistance(shorter, longer);
  return (longer.length - editDistance) / longer.length;
}

function levenshteinDistance (a: string, b: string): number {
  const matrix: number[][] = [];

  for (let i = 0; i <= b.length; i++) {
    matrix[i] = [i];
  }

  for (let j = 0; j <= a.length; j++) {
    matrix[0][j] = j;
  }

  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      if (b.charAt(i - 1) === a.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1,
          matrix[i][j - 1] + 1,
          matrix[i - 1][j] + 1,
        );
      }
    }
  }

  return matrix[b.length][a.length];
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L212
export function camelToSnakeCase (name: string): string {
  return name.replace(CAMEL_CASE_PATTERN, '_').toUpperCase();
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L217
export function whileChanging<T> (expression: T, func: (expr: T) => T): T {
  let current = expression;
  while (true) {
    const startHash = hashObject(current);
    current = func(current);
    const endHash = hashObject(current);

    if (startHash === endHash) {
      break;
    }
  }
  return current;
}

function hashObject (obj: unknown): string {
  return JSON.stringify(obj);
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L240
export function tsort<T> (dag: Map<T, Set<T>>): T[] {
  const result: T[] = [];
  const dagCopy = new Map(dag);

  for (const [_node, deps] of Array.from(dag.entries())) {
    for (const dep of Array.from(deps)) {
      if (!dagCopy.has(dep)) {
        dagCopy.set(dep, new Set());
      }
    }
  }

  while (dagCopy.size > 0) {
    const current = new Set<T>();
    for (const [node, deps] of Array.from(dagCopy.entries())) {
      if (deps.size === 0) {
        current.add(node);
      }
    }

    if (current.size === 0) {
      throw new Error('Cycle error');
    }

    for (const node of Array.from(current)) {
      dagCopy.delete(node);
    }

    for (const deps of Array.from(dagCopy.values())) {
      for (const node of Array.from(current)) {
        deps.delete(node);
      }
    }

    result.push(...Array.from(current).sort());
  }

  return result;
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L274
// Modified: accepts string[] instead of Iterable<string>
export function findNewName (taken: string[], base: string): string {
  const takenSet = new Set(taken);
  if (!takenSet.has(base)) {
    return base;
  }

  let i = 2;
  let newName = `${base}_${i}`;
  while (takenSet.has(newName)) {
    i++;
    newName = `${base}_${i}`;
  }

  return newName;
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L297
export function isInt (text: string): boolean {
  return isType(text, (s) => {
    const num = Number(s);
    return Number.isInteger(num);
  });
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L301
export function isFloat (text: string): boolean {
  return isType(text, (s) => {
    const num = Number(s);
    return !isNaN(num) && isFinite(num);
  });
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L305
function isType (text: string, validator: (text: string) => boolean): boolean {
  try {
    return validator(text);
  } catch {
    return false;
  }
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L313
export function nameSequence (prefix: string): () => string {
  let counter = 0;
  return () => `${prefix}${counter++}`;
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L327
// Modified: simplified signature by using default parameters
export function splitNumWords (
  value: string,
  sep: string,
  minNumWords: number,
  fillFromStart: boolean = true,
): (string | undefined)[] {
  const words = value.split(sep);
  const padding = Math.max(0, minNumWords - words.length);

  if (fillFromStart) {
    return [...Array(padding).fill(undefined), ...words];
  }
  return [...words, ...Array(padding).fill(undefined)];
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L377
// Modified: returns array instead of generator, uses Array.isArray instead of is_iterable
export function flatten (values: unknown[]): unknown[] {
  const result: unknown[] = [];
  for (const value of values) {
    if (Array.isArray(value)) {
      result.push(...flatten(value));
    } else {
      result.push(value);
    }
  }
  return result;
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L401
// Modified: renamed from dictDepth to objectDepth
export function objectDepth (d: unknown): number {
  try {
    if (typeof d !== 'object' || d === null) {
      return 0;
    }

    const values = Object.values(d);
    if (values.length === 0) {
      return 1;
    }

    return 1 + objectDepth(values[0]);
  } catch {
    return 0;
  }
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L427
// Modified: accepts T[] instead of Iterable<T>
export function first<T> (it: T[]): T {
  if (it.length === 0) {
    throw new Error('Array is empty');
  }
  return it[0];
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L432
export function toBool (value?: string | boolean): string | boolean | undefined {
  if (typeof value === 'boolean' || value === undefined) {
    return value;
  }

  const valueLower = value.toLowerCase();
  if (valueLower === 'true' || valueLower === '1') {
    return true;
  }
  if (valueLower === 'false' || valueLower === '0') {
    return false;
  }

  return value;
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L446
export function mergeRanges<T> (ranges: [T, T][]): [T, T][] {
  if (ranges.length === 0) {
    return [];
  }

  const sorted = [...ranges].sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
  const merged: [T, T][] = [sorted[0]];

  for (let i = 1; i < sorted.length; i++) {
    const [start, end] = sorted[i];
    const [lastStart, lastEnd] = merged[merged.length - 1];

    if (start <= lastEnd) {
      merged[merged.length - 1] = [lastStart, end > lastEnd ? end : lastEnd];
    } else {
      merged.push([start, end]);
    }
  }

  return merged;
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L473
// Modified: removed unnecessary try-catch (Date constructor doesn't throw in JS)
export function isIsoDate (text: string): boolean {
  const date = new Date(text);
  return !isNaN(date.getTime()) && /^\d{4}-\d{2}-\d{2}$/.test(text);
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L481
// Modified: removed unnecessary try-catch (Date constructor doesn't throw in JS)
export function isIsoDatetime (text: string): boolean {
  const date = new Date(text);
  return !isNaN(date.getTime());
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L490
export const DATE_UNITS = new Set(['day', 'week', 'month', 'quarter', 'year', 'year_month']);

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L501
// Modified: constructor accepts K[] instead of Collection<K>, doesn't implement Map to avoid MapIterator requirements
// Map-like structure where multiple keys all return the same single value
// Memory optimization to avoid storing duplicate values when many keys share the same value
export class SingleValuedMapping<K, V> {
  private _keys: Set<K>;
  private _value: V;

  constructor (keys: K[], value: V) {
    this._keys = new Set(keys);
    this._value = value;
  }

  get (key: K): V | undefined {
    return this._keys.has(key) ? this._value : undefined;
  }

  has (key: K): boolean {
    return this._keys.has(key);
  }

  get size (): number {
    return this._keys.size;
  }

  * [Symbol.iterator] (): IterableIterator<[K, V]> {
    for (const key of Array.from(this._keys)) {
      yield [key, this._value] as [K, V];
    }
  }

  * entries (): IterableIterator<[K, V]> {
    for (const key of Array.from(this._keys)) {
      yield [key, this._value] as [K, V];
    }
  }

  keys (): IterableIterator<K> {
    return this._keys.values();
  }

  * values (): IterableIterator<V> {
    for (const _ of Array.from(this._keys)) {
      yield this._value;
    }
  }

  forEach (callbackfn: (value: V, key: K, map: SingleValuedMapping<K, V>) => void, thisArg?: unknown): void {
    for (const key of Array.from(this._keys)) {
      callbackfn.call(thisArg, this._value, key, this);
    }
  }

  clear (): void {
    throw new Error('Method not supported');
  }

  delete (_key: K): boolean {
    throw new Error('Method not supported');
  }

  set (_key: K, _value: V): this {
    throw new Error('Method not supported');
  }

  get [Symbol.toStringTag] (): string {
    return 'SingleValuedMapping';
  }
}
