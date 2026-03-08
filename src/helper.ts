// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py

import { DateTime } from 'luxon';
import type {
  Expression,
  ExpressionValue,
  ExpressionValueList,
  ExpressionOrString,
  ExpressionOrStringList,
} from './expressions/expressions';
import {
  DataTypeExpr, AddExpr, literal,
  DataTypeExprKind,
} from './expressions/expressions';
import { isIterable } from './port_internals';
import { annotateTypes } from './optimizer/annotate_types';
import { simplify } from './optimizer/simplify';
import type { Dialect } from './dialects';

// Not ported from Python: AutoName, classproperty, subclasses, object_to_dict, is_date_unit

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L22
const CAMEL_CASE_PATTERN = /(?<!^)(?=[A-Z])/;

/**
 * Suggests the closest match and throws an error.
 *
 * Uses Levenshtein distance to find similar strings and provide helpful error messages.
 *
 * @param kind - The type of thing being matched (e.g., "function", "table")
 * @param word - The unknown word that was provided
 * @param possibilities - Array of valid possibilities
 * @throws {Error} Always throws with a helpful suggestion if available
 *
 * @example
 * ```ts
 * suggestClosestMatchAndFail("function", "CONT", ["COUNT", "CONCAT", "COALESCE"]);
 * // Error: Unknown function 'CONT'. Did you mean COUNT?
 * ```
 *
 */
export function suggestClosestMatchAndFail (
  kind: string,
  word: string,
  possibilities: Iterable<string>,
): never {
  const closeMatches = getCloseMatches(word, possibilities, 1);

  const similar = seqGet(closeMatches, 0);
  const suggestion = similar
    ? ` Did you mean ${similar}?`
    : '';

  throw new Error(`Unknown ${kind} '${word}'.${suggestion}`);
}

/**
 * Safely gets an element from an array by index.
 *
 * @typeParam T - The array element type
 * @param seq - The array to get from
 * @param index - The index to retrieve
 * @returns The element at the index, or undefined if out of bounds
 *
 * @example
 * ```ts
 * seqGet([1, 2, 3], 1); // 2
 * seqGet([1, 2, 3], 10); // undefined
 * seqGet([1, 2, 3], -1); // undefined
 * ```
 *
 */
export function seqGet<T> (seq: Iterable<T>, index: number): T | undefined {
  if (Array.isArray(seq)) {
    return 0 <= index && index < seq.length ? seq[index] : undefined;
  }
  let i = 0;
  for (const item of seq) {
    if (i === index) return item;
    i++;
  }
  return undefined;
}

export function expressionValueToOrString<E extends Expression = Expression> (
  value: ExpressionValue<E>,
): ExpressionOrString<E> {
  if (typeof value === 'boolean' || typeof value === 'number') {
    return String(value);
  }
  return value;
}

export function expressionValueListToOrStringList<E extends Expression = Expression> (
  values: ExpressionValueList<E>,
): ExpressionOrStringList<E> {
  return values.map(expressionValueToOrString) as ExpressionOrStringList<E>;
}

/**
 * Ensures a value is iterable.
 *
 * @typeParam T - The element type
 * @param value - A single value, iterable, or undefined
 * @returns The iterable as-is, a single-element array, or an empty array if undefined
 *
 * @example
 * ```ts
 * ensureIterable(5); // [5]
 * ensureIterable([1, 2]); // [1, 2]
 * ensureIterable(new Set([1, 2])); // Set {1, 2}
 * ensureIterable(undefined); // []
 * ```
 *
 */
export function ensureIterable<T> (value?: T | Iterable<T>): Iterable<T> {
  if (value === undefined) {
    return [];
  }
  if (typeof value !== 'string' && isIterable<T>(value)) {
    return value;
  }
  return [value as T];
}

/**
 * Ensures a value is iterable.
 *
 * Similar to ensureIterable but doesn't handle undefined.
 *
 * @typeParam T - The element type
 * @param value - A single value or iterable
 * @returns The iterable as-is or a single-element array
 *
 * @example
 * ```ts
 * ensureCollection(5); // [5]
 * ensureCollection([1, 2]); // [1, 2]
 * ```
 *
 */
export function ensureCollection<T> (value: T | Iterable<T>): Iterable<T> {
  if (typeof value !== 'string' && isIterable<T>(value)) {
    return value;
  }
  return [value as T];
}

/**
 * Joins strings with a separator (comma-separated values).
 *
 * @example
 * ```ts
 * csv("a", "b", "c"); // "a, b, c"
 * csv("; ", "a", "b"); // "a; b"
 * csv("a", "", "b"); // "a, b" (empty strings filtered out)
 * ```
 *
 */
export function csv (args: Iterable<string>, options: { sep?: string } = {}): string {
  const { sep = ', ' } = options;
  const filtered: string[] = [];
  for (const arg of args) if (arg) filtered.push(arg);
  return filtered.join(sep ?? ', ');
}

function getCloseMatches (
  word: string,
  possibilities: Iterable<string>,
  n: number,
  cutoff: number = 0.6,
): string[] {
  const results: { match: string;
    ratio: number; }[] = [];

  for (const possibility of possibilities) {
    const ratio = similarity(word, possibility);
    if (cutoff <= ratio) {
      results.push({
        match: possibility,
        ratio,
      });
    }
  }

  results.sort((a, b) => b.ratio - a.ratio);
  return results.slice(0, n).map((r) => r.match);
}

function similarity (a: string, b: string): number {
  const longer = b.length < a.length
    ? a
    : b;
  const shorter = b.length < a.length
    ? b
    : a;

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

/**
 * Converts camelCase to SNAKE_CASE.
 *
 * @param name - The camelCase string
 * @returns The SNAKE_CASE string
 *
 * @example
 * ```ts
 * camelToSnakeCase("userId"); // "USER_ID"
 * camelToSnakeCase("getUserById"); // "GET_USER_BY_ID"
 * ```
 *
 */
export function camelToSnakeCase (name: string): string {
  return name.replace(CAMEL_CASE_PATTERN, '_').toUpperCase();
}

/**
 * Repeatedly applies a function until the result stops changing.
 *
 * Useful for applying optimization passes until a fixed point is reached.
 *
 * @typeParam T - The type being transformed
 * @param expression - The initial value
 * @param func - The transformation function
 * @returns The final value after no more changes occur
 *
 * @example
 * ```ts
 * const simplify = (x: number) => x > 10 ? x - 1 : x;
 * whileChanging(15, simplify); // 10
 * ```
 *
 */
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

/**
 * Topological sort of a directed acyclic graph (DAG).
 *
 * Takes a graph represented as a Map where keys are nodes and values are sets of dependencies.
 * Returns nodes in an order where dependencies come before dependents.
 *
 * @typeParam T - The node type
 * @param dag - A Map where each key depends on the nodes in its value Set
 * @returns Array of nodes in topological order
 * @throws {Error} If the graph contains a cycle
 *
 * @example
 * ```ts
 * const graph = new Map([
 *   ['c', new Set(['a', 'b'])],
 *   ['b', new Set(['a'])],
 *   ['a', new Set()],
 * ]);
 * tsort(graph); // ['a', 'b', 'c']
 * ```
 *
 */
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

  while (0 < dagCopy.size) {
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

/**
 * Finds a unique name by appending a number suffix if needed.
 *
 * If the base name is not taken, returns it as-is. Otherwise appends _2, _3, etc.
 * until finding an available name.
 *
 * @param taken - Array of names already in use
 * @param base - The desired base name
 * @returns A unique name not in the taken array
 *
 * @example
 * ```ts
 * findNewName(['foo', 'foo_2'], 'foo'); // "foo_3"
 * findNewName(['bar'], 'foo'); // "foo"
 * ```
 *
 */
export function findNewName (taken: Iterable<string>, base: string): string {
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

/**
 * Checks if a string represents an integer.
 *
 * @param text - The string to check
 * @returns True if the string can be parsed as an integer
 *
 * @example
 * ```ts
 * isInt("123"); // true
 * isInt("123.45"); // false
 * isInt("abc"); // false
 * ```
 *
 */
export function isInt (text: string): boolean {
  return isType(text, (s) => {
    const num = Number(s);
    return Number.isInteger(num);
  });
}

/**
 * Checks if a string represents a float.
 *
 * @param text - The string to check
 * @returns True if the string can be parsed as a finite number
 *
 * @example
 * ```ts
 * isFloat("123.45"); // true
 * isFloat("123"); // true
 * isFloat("abc"); // false
 * isFloat("Infinity"); // false
 * ```
 *
 */
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

/**
 * Creates a name generator function that produces sequential names.
 *
 * Returns a function that generates names with an incrementing counter starting at 0.
 *
 * @param prefix - The prefix for generated names
 * @returns A function that generates sequential names
 *
 * @example
 * ```ts
 * const genName = nameSequence('temp');
 * genName(); // "temp0"
 * genName(); // "temp1"
 * genName(); // "temp2"
 * ```
 *
 */
export function nameSequence (prefix: string): () => string {
  let counter = 0;
  return () => `${prefix}${counter++}`;
}

/**
 * Splits a string and pads the result to a minimum number of elements.
 *
 * @param value - The string to split
 * @param sep - The separator to split on
 * @param minNumWords - Minimum number of elements in the result
 * @param fillFromStart - If true, adds undefined at the start; otherwise at the end
 * @returns Array of strings (and undefined for padding)
 *
 * @example
 * ```ts
 * splitNumWords("a.b", ".", 4); // [undefined, undefined, "a", "b"]
 * splitNumWords("a.b", ".", 4, false); // ["a", "b", undefined, undefined]
 * splitNumWords("a.b.c", ".", 2); // ["a", "b", "c"]
 * ```
 *
 */
// Modified: simplified signature by using default parameters
export function splitNumWords (
  value: string,
  sep: string,
  minNumWords: number,
  options: { fillFromStart?: boolean } = {},
): (string | undefined)[] {
  const { fillFromStart = true } = options;
  const words = value.split(sep);
  const padding = Math.max(0, minNumWords - words.length);

  if (fillFromStart) {
    return [...Array(padding).fill(undefined), ...words];
  }
  return [...words, ...Array(padding).fill(undefined)];
}

/**
 * Recursively flattens a nested array structure.
 *
 * @param values - Array that may contain nested arrays
 * @returns A flat array with all nested arrays expanded
 *
 * @example
 * ```ts
 * flatten([1, [2, [3, 4]], 5]); // [1, 2, 3, 4, 5]
 * flatten([1, 2, 3]); // [1, 2, 3]
 * flatten([[[]]]); // []
 * ```
 *
 */
export function flatten (values: Iterable<unknown>): unknown[] {
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

/**
 * Calculates the nesting depth of an object.
 *
 * Returns the maximum depth of nested objects, counting each level.
 *
 * @param d - The value to measure depth of
 * @returns The depth (0 for non-objects, 1 for flat objects, etc.)
 *
 * @example
 * ```ts
 * objectDepth(5); // 0
 * objectDepth({}); // 1
 * objectDepth({a: 1}); // 1
 * objectDepth({a: {b: 1}}); // 2
 * objectDepth({a: {b: {c: 1}}}); // 3
 * ```
 *
 */
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

/**
 * Gets the first element of an array.
 *
 * @typeParam T - The array element type
 * @param it - The array
 * @returns The first element
 * @throws {Error} If the array is empty
 *
 * @example
 * ```ts
 * first([1, 2, 3]); // 1
 * first(["a"]); // "a"
 * first([]); // Error: Array is empty
 * ```
 *
 */
export function first<T> (it: Iterable<T>): T {
  for (const v of it) return v;
  throw new Error('Iterable is empty');
}

/**
 * Converts a string to a boolean if it represents one.
 *
 * Recognizes "true", "1" as true and "false", "0" as false (case insensitive).
 * Returns the original value if it's already a boolean, undefined, or not a recognized string.
 *
 * @param value - The value to convert
 * @returns The boolean representation, or the original value if not convertible
 *
 * @example
 * ```ts
 * toBool("true"); // true
 * toBool("FALSE"); // false
 * toBool("1"); // true
 * toBool("0"); // false
 * toBool("yes"); // "yes"
 * toBool(true); // true
 * toBool(undefined); // undefined
 * ```
 *
 */
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

/**
 * Merges overlapping ranges.
 *
 * Takes an array of [start, end] tuples and merges any that overlap.
 *
 * @typeParam T - The range boundary type (must be comparable)
 * @param ranges - Array of [start, end] tuples
 * @returns Array of merged ranges with no overlaps
 *
 * @example
 * ```ts
 * mergeRanges([[1, 3], [2, 5], [7, 9]]); // [[1, 5], [7, 9]]
 * mergeRanges([[1, 2], [3, 4]]); // [[1, 2], [3, 4]]
 * mergeRanges([]); // []
 * ```
 *
 */
export function mergeRanges<T> (ranges: Iterable<[T, T]>): [T, T][] {
  const arr = [...ranges];
  if (arr.length === 0) {
    return [];
  }

  const sorted = arr.sort((a, b) => (a[0] < b[0]
    ? -1
    : b[0] < a[0]
      ? 1
      : 0));
  const merged: [T, T][] = [sorted[0]];

  for (let i = 1; i < sorted.length; i++) {
    const [start, end] = sorted[i];
    const [lastStart, lastEnd] = merged[merged.length - 1];

    if (start <= lastEnd) {
      merged[merged.length - 1] = [
        lastStart,
        lastEnd < end
          ? end
          : lastEnd,
      ];
    } else {
      merged.push([start, end]);
    }
  }

  return merged;
}

/**
 * Checks if a string is in ISO date format (YYYY-MM-DD).
 *
 * @param text - The string to check
 * @returns True if the string is a valid ISO date
 *
 * @example
 * ```ts
 * isIsoDate("2024-01-15"); // true
 * isIsoDate("2024-1-5"); // false (not padded)
 * isIsoDate("01/15/2024"); // false
 * isIsoDate("2024-13-01"); // false (invalid month)
 * ```
 *
 */
export function isIsoDate (text: string): boolean {
  const dt = DateTime.fromISO(text);
  return dt.isValid && /^\d{4}-\d{2}-\d{2}$/.test(text);
}

/**
 * Checks if a string can be parsed as a datetime.
 *
 * @param text - The string to check
 * @returns True if the string represents a valid datetime
 *
 * @example
 * ```ts
 * isIsoDatetime("2024-01-15T10:30:00Z"); // true
 * isIsoDatetime("2024-01-15"); // true
 * isIsoDatetime("invalid"); // false
 * ```
 *
 */
export function isIsoDatetime (text: string): boolean {
  const dt = DateTime.fromISO(text);
  return dt.isValid;
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/helper.py#L490
export const DATE_UNITS = new Set([
  'day',
  'week',
  'month',
  'quarter',
  'year',
  'year_month',
]);

/**
 * Checks if an expression represents a date unit.
 *
 * @param expression - The expression to check (or undefined)
 * @returns True if the expression's name is a valid date unit
 *
 * @example
 * ```ts
 * import { parseOne } from 'sqlglot';
 * const expr = parseOne("'day'");
 * isDateUnit(expr); // true
 * ```
 */
export function isDateUnit (expression: { name: string } | undefined): boolean {
  return expression !== undefined && DATE_UNITS.has(expression.name.toLowerCase());
}

/**
 * Map-like structure where multiple keys all return the same single value.
 *
 * Memory optimization to avoid storing duplicate values when many keys share the same value.
 * Provides a Map-like interface but only stores one value for all keys.
 *
 * @typeParam K - The key type
 * @typeParam V - The value type
 *
 * @example
 * ```ts
 * const mapping = new SingleValuedMapping(['a', 'b', 'c'], 42);
 * mapping.get('a'); // 42
 * mapping.get('b'); // 42
 * mapping.get('d'); // undefined
 * mapping.has('c'); // true
 * mapping.size; // 3
 * ```
 *
 */
export class SingleValuedMapping<K, V> implements Map<K, V> {
  private _keys: Set<K>;
  private _value: V;

  constructor (keys: Iterable<K>, value: V) {
    this._keys = new Set(keys);
    this._value = value;
  }

  get (key: K): V | undefined {
    return this._keys.has(key)
      ? this._value
      : undefined;
  }

  has (key: K): boolean {
    return this._keys.has(key);
  }

  get size (): number {
    return this._keys.size;
  }

  * [Symbol.iterator] (): MapIterator<[K, V]> {
    for (const key of Array.from(this._keys)) {
      yield [key, this._value] as [K, V];
    }
  }

  * entries (): MapIterator<[K, V]> {
    for (const key of Array.from(this._keys)) {
      yield [key, this._value] as [K, V];
    }
  }

  keys (): MapIterator<K> {
    return this._keys.values();
  }

  * values (): MapIterator<V> {
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

/**
 * Applies an offset to a given integer literal expression.
 *
 * @param this_ - The target of the index
 * @param expressions - List of expressions to adjust
 * @param offset - The offset to apply
 * @param options - Options including dialect
 * @returns The adjusted expressions
 */
export function applyIndexOffset (
  this_: Expression,
  expressions: Iterable<Expression>,
  offset: number,
  options?: { dialect?: Dialect },
): Expression[] {
  const exprs = [...expressions];
  if (!offset || exprs.length !== 1) {
    return exprs;
  }

  const expression = exprs[0];

  // Annotate types if not already done
  if (!this_.type) {
    annotateTypes(this_, { dialect: options?.dialect });
  }

  // Check if this_ is an array type
  const thisType = this_.type as DataTypeExpr | undefined;
  if (
    thisType?.args.this !== DataTypeExprKind.UNKNOWN
    && thisType?.args.this !== DataTypeExprKind.ARRAY
  ) {
    return exprs;
  }

  // Annotate expression type if not already done
  if (!expression.type) {
    annotateTypes(expression, { dialect: options?.dialect });
  }

  // Check if expression is an integer type
  const exprType = expression.type as DataTypeExpr | undefined;
  if (exprType?.args.this && DataTypeExpr.INTEGER_TYPES?.has(exprType.args.this as DataTypeExprKind)) {
    // Apply offset: expression + offset
    const offsetExpr = new AddExpr({
      this: expression,
      expression: literal(offset),
    });
    return [simplify(offsetExpr)];
  }

  return exprs;
}
