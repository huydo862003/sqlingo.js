import {
  DateTime, Duration,
} from 'luxon';
import {
  DataTypeExpr, DataTypeExprKind,
} from '../expressions';
import { Generator } from '../generator';
import {
  isInt, seqGet,
} from '../helper';
import {
  add, and, eq, floorDiv, gt, gte, lt, lte, lshift, mod, mul, neq, or, pow, rshift, sub, truediv, xor,
} from '../port_internals/ops_utils';

/**
 * Makes a method return `undefined` if any argument is `undefined`.
 *
 * Can be used directly:
 *   @undefinedIfAny
 *   static foo(a, b) { ... }
 *
 * Or as a factory to restrict the check to specific argument indices:
 *   @undefinedIfAny(0, 1)
 *   static foo(a, b, c) { ... }  // only checks a and b
 */
function undefinedIfAny<This, Args extends unknown[], Return> (
  value: (this: This, ...args: Args) => Return,
  context: ClassMethodDecoratorContext<This, (this: This, ...args: Args) => Return>,
): (this: This, ...args: Args) => Return;
function undefinedIfAny (...indices: number[]): <This, Args extends unknown[], Return>(
  value: (this: This, ...args: Args) => Return,
  context: ClassMethodDecoratorContext<This, (this: This, ...args: Args) => Return>,
) => (this: This, ...args: Args) => Return;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function undefinedIfAny (...args: any[]): unknown {
  const isDirectDecorator =
    args.length === 2 // decorator receives exactly (value, context)
    && typeof args[0] === 'function' // value is the decorated method
    && args[1] !== undefined // context is present
    && typeof args[1] === 'object' // context is an object
    && args[1].kind === 'method'; // context.kind identifies a method decorator

  const wrap = (func: (...a: unknown[]) => unknown, indices: number[]) => {
    const predicate =
      0 < indices.length
        ? (...fnArgs: unknown[]) => indices.some((i) => fnArgs[i] === undefined)
        : (...fnArgs: unknown[]) => fnArgs.some((a) => a === undefined);

    return (...fnArgs: unknown[]) => (predicate(...fnArgs) ? undefined : func(...fnArgs));
  };

  if (isDirectDecorator) {
    return wrap(args[0] as (...a: unknown[]) => unknown, []);
  }
  const indices = args as number[];
  return (value: (...a: unknown[]) => unknown) => wrap(value, indices);
}

class ReverseKey<T> {
  constructor (public obj: T) {}

  eq (other: ReverseKey<T>): boolean {
    return other.obj === this.obj;
  }

  lt (other: ReverseKey<T>): boolean {
    return lt(other.obj, this.obj);
  }
}

function filterUndefineds<T, R> (func: (values: T[]) => R, emptyUndefined = true) {
  return (values: unknown): R | undefined => {
    // Convert generators/iterables to arrays
    const arr = Array.isArray(values)
      ? values
      : (values !== null && values !== undefined && typeof (values as Record<symbol, unknown>)[Symbol.iterator] === 'function')
        ? [...(values as Iterable<unknown>)]
        : [values];
    const filtered = arr.filter((v): v is T => v !== undefined);
    if (filtered.length === 0 && emptyUndefined) return undefined;
    return func(filtered);
  };
}

function fmean<T> (values: T[]): number {
  return truediv(values.reduce((a: unknown, b: T) => add(a, b), 0), values.length) as number;
}

export class ENV {
  // aggs
  static ARRAYAGG = <T>(values: T[]): T[] => [...values];
  static ARRAYUNIQUEAGG = filterUndefineds(<T>(acc: T[]): T[] => [...new Set(acc)]);
  static AVG = filterUndefineds((acc: number[]) => fmean(acc));
  static COUNT = filterUndefineds((acc: unknown[]) => acc.length, false);
  static MAX = filterUndefineds(<T>(acc: T[]): T => acc.reduce((a, b) => (lt(a, b) ? b : a)));
  static MIN = filterUndefineds(<T>(acc: T[]): T => acc.reduce((a, b) => (lt(b, a) ? b : a)));
  static SUM = filterUndefineds(<T>(acc: T[]) => acc.reduce((a: unknown, b: T) => add(a, b), 0));

  // scalar functions
  @undefinedIfAny
  static ABS<T extends number> (this_: T): number {
    return Math.abs(this_);
  }

  @undefinedIfAny
  static ADD<E, T> (e: E, this_: T) {
    return add(e, this_);
  }

  @undefinedIfAny
  static ARRAYANY<T> (arr: T[], func: (e: T) => boolean): boolean {
    return arr.some((e) => func(e));
  }

  @undefinedIfAny(0, 1)
  @undefinedIfAny(0, 1)
  static ARRAYTOSTRING<T> (this_: T[], expression: string, fallback?: T): string {
    return this_
      .map((x) => (x !== undefined ? x : fallback))
      .filter((x): x is T => x !== undefined)
      .join(expression);
  }

  @undefinedIfAny
  static BETWEEN<T> (this_: T, low: T, high: T): boolean {
    return lte(low, this_) && lte(this_, high);
  }

  @undefinedIfAny
  static BITWISEAND<T, E> (this_: T, e: E) {
    return and(this_, e);
  }

  @undefinedIfAny
  static BITWISELEFTSHIFT<T, E> (this_: T, e: E) {
    return lshift(this_, e);
  }

  @undefinedIfAny
  static BITWISEOR<T, E> (this_: T, e: E) {
    return or(this_, e);
  }

  @undefinedIfAny
  static BITWISERIGHTSHIFT<T, E> (this_: T, e: E) {
    return rshift(this_, e);
  }

  @undefinedIfAny
  static BITWISEXOR<T, E> (this_: T, e: E) {
    return xor(this_, e);
  }

  static CAST<T> (this_: T, to: DataTypeExprKind): unknown {
    if (this_ === undefined) return undefined;
    if (to === DataTypeExprKind.DATE) {
      if (this_ instanceof DateTime) return this_.startOf('day');
      if (typeof this_ === 'string') return DateTime.fromISO(this_).startOf('day');
    }
    if (to === DataTypeExprKind.TIME) {
      if (this_ instanceof DateTime) return this_;
      if (typeof this_ === 'string') return DateTime.fromISO(`1970-01-01T${this_}`);
    }
    if (to === DataTypeExprKind.DATETIME || to === DataTypeExprKind.TIMESTAMP) {
      if (this_ instanceof DateTime) return this_;
      if (typeof this_ === 'string') return DateTime.fromISO(this_);
    }
    if (to === DataTypeExprKind.BOOLEAN) return Boolean(this_);
    if (DataTypeExpr.TEXT_TYPES.has(to)) return String(this_);
    if (to === DataTypeExprKind.FLOAT || to === DataTypeExprKind.DOUBLE) return Number(this_);
    if (DataTypeExpr.NUMERIC_TYPES.has(to)) return Math.trunc(Number(this_));
    throw new Error(`Casting ${this_} to '${to}' not implemented.`);
  }

  static COALESCE = <T>(...args: (T | undefined)[]): T | undefined => args.find((a) => a !== undefined);

  @undefinedIfAny
  static CONCAT (...args: string[]): string {
    return args.join('');
  }

  @undefinedIfAny
  static SAFECONCAT (...args: unknown[]): string {
    return args.map(String).join('');
  }

  @undefinedIfAny
  static CONCATWS (this_: string, ...args: string[]): string {
    return args.join(this_);
  }

  @undefinedIfAny
  static DATEDIFF (this_: DateTime, expression: DateTime): number {
    return Math.floor(this_.diff(expression, 'days').days);
  }

  @undefinedIfAny
  static DATESTRTODATE (arg: string): DateTime {
    return DateTime.fromISO(arg).startOf('day');
  }

  @undefinedIfAny
  static DIV<E, T> (e: E, this_: T) {
    if (this_ === 0) throw new Error('division by zero');
    return truediv(e, this_);
  }

  static DOT<E extends Record<PropertyKey, unknown>, K extends PropertyKey> (e: E, this_: K): E[K] | undefined {
    if (e === undefined || this_ === undefined) return undefined;
    return e[this_];
  }

  @undefinedIfAny
  static EQ<T, E> (this_: T, e: E): boolean {
    return eq(this_, e);
  }

  @undefinedIfAny
  static EXTRACT (this_: string, e: DateTime): unknown {
    return (e as unknown as Record<string, unknown>)[this_];
  }

  @undefinedIfAny
  static GT<T, E> (this_: T, e: E): boolean {
    return gt(this_, e);
  }

  @undefinedIfAny
  static GTE<T, E> (this_: T, e: E): boolean {
    return gte(this_, e);
  }

  static IF = <T, F>(predicate: unknown, true_: T, false_: F): T | F => (predicate ? true_ : false_);

  @undefinedIfAny
  static INTDIV<E, T> (e: E, this_: T) {
    return floorDiv(e, this_);
  }

  @undefinedIfAny
  static INTERVAL<T> (this_: T, unit: string): number {
    const plural = unit + 'S';
    if (plural in Generator.TIME_PART_SINGULARS) unit = plural;
    return Duration.fromObject({ [unit.toLowerCase()]: parseFloat(this_ as string) }).toMillis();
  }

  @undefinedIfAny(0, 1)
  static JSONEXTRACT<T> (this_: T, expression: unknown[]): unknown {
    let current: unknown = this_;
    for (const pathSegment of expression) {
      if (current === undefined) break;
      if (typeof current === 'object' && !Array.isArray(current)) {
        current = (current as Record<PropertyKey, unknown>)[pathSegment as PropertyKey];
      } else if (Array.isArray(current) && isInt(String(pathSegment))) {
        current = seqGet(current, parseInt(pathSegment as string));
      } else {
        throw new Error(`Unable to extract value for ${current} at ${pathSegment}.`);
      }
    }
    return current;
  }

  @undefinedIfAny
  static LEFT (this_: string, e: number): string {
    return this_.slice(0, e);
  }

  @undefinedIfAny
  static LIKE (this_: string, e: string): boolean {
    return new RegExp('^' + e.replace(/_/g, '.').replace(/%/g, '.*') + '$').test(this_);
  }

  @undefinedIfAny
  static LOWER (arg: string): string {
    return arg.toLowerCase();
  }

  @undefinedIfAny
  static LT<T, E> (this_: T, e: E): boolean {
    return lt(this_, e);
  }

  @undefinedIfAny
  static LTE<T, E> (this_: T, e: E): boolean {
    return lte(this_, e);
  }

  static MAP<K extends PropertyKey, V> (keys: K[], values: V[]): Record<K, V> | undefined {
    if (keys === undefined || values === undefined) return undefined;
    return Object.fromEntries(keys.map((k, i) => [k, values[i]])) as Record<K, V>;
  }

  @undefinedIfAny
  static MOD<E, T> (e: E, this_: T) {
    return mod(e, this_);
  }

  @undefinedIfAny
  static MUL<E, T> (e: E, this_: T) {
    return mul(e, this_);
  }

  @undefinedIfAny
  static NEQ<T, E> (this_: T, e: E): boolean {
    return neq(this_, e);
  }

  @undefinedIfAny
  static ORD (c: string): number {
    return c.codePointAt(0)!;
  }

  static ORDERED<T> (this_: T, options: {
    desc: boolean;
    undefinedFirst: boolean;
  }): T | ReverseKey<T> {
    const { desc } = options;
    if (desc) return new ReverseKey(this_);
    return this_;
  }

  static POW = <A, B>(a: A, b: B) => pow(a, b);

  @undefinedIfAny
  static RIGHT (this_: string, e: number): string {
    return this_.slice(-e);
  }

  @undefinedIfAny
  static ROUND (this_: number, decimals?: number, _truncate?: unknown): number {
    return decimals === undefined ? Math.round(this_) : parseFloat(this_.toFixed(decimals));
  }

  static STRPOSITION (this_?: string, substr?: string, position?: number): number | undefined {
    if (this_ === undefined || substr === undefined) return undefined;
    const pos = position !== undefined ? position - 1 : 0;
    return this_.indexOf(substr, pos) + 1;
  }

  @undefinedIfAny
  static SUB<E, T> (e: E, this_: T) {
    return sub(e, this_);
  }

  static SUBSTRING (this_?: string, start?: number, length?: number): string | undefined {
    if (this_ === undefined) return undefined;
    if (start === undefined) return this_;
    if (start === 0) return '';
    const s = start < 0 ? this_.length + start : start - 1;
    const end = length === undefined ? undefined : s + length;
    return this_.slice(s, end);
  }

  @undefinedIfAny
  static TIMESTRTOTIME (arg: string): DateTime {
    return DateTime.fromISO(arg);
  }

  @undefinedIfAny
  static UPPER (arg: string): string {
    return arg.toUpperCase();
  }

  @undefinedIfAny
  static YEAR (arg: DateTime): number {
    return arg.year;
  }

  @undefinedIfAny
  static MONTH (arg: DateTime): number {
    return arg.month;
  }

  @undefinedIfAny
  static DAY (arg: DateTime): number {
    return arg.day;
  }

  static CURRENTDATETIME = (): DateTime => DateTime.now();
  static CURRENTTIMESTAMP = (): DateTime => DateTime.now();
  static CURRENTTIME = (): DateTime => DateTime.now();
  static CURRENTDATE = (): DateTime => DateTime.now().startOf('day');

  @undefinedIfAny
  static STRFTIME (fmt: string, arg: string): string {
    const d = DateTime.fromISO(arg);
    return fmt
      .replace('%Y', String(d.year))
      .replace('%m', String(d.month).padStart(2, '0'))
      .replace('%d', String(d.day).padStart(2, '0'))
      .replace('%H', String(d.hour).padStart(2, '0'))
      .replace('%M', String(d.minute).padStart(2, '0'))
      .replace('%S', String(d.second).padStart(2, '0'));
  }

  @undefinedIfAny
  static STRTOTIME (arg: string, format: string): DateTime {
    void format;
    return DateTime.fromISO(arg);
  }

  @undefinedIfAny
  static TRIM (this_: string, e?: string): string {
    return e === undefined ? this_.trim() : this_.replace(new RegExp(`^[${e}]+|[${e}]+$`, 'g'), '');
  }

  static STRUCT = <K extends string, V>(...args: (K | V)[]): Record<K, V> => {
    const result = {} as Record<K, V>;
    for (let x = 0; x < args.length; x += 2) {
      if (args[x] !== undefined && args[x + 1] !== undefined) {
        result[args[x] as K] = args[x + 1] as V;
      }
    }
    return result;
  };

  @undefinedIfAny
  static UNIXTOTIME (arg: number): DateTime {
    return DateTime.fromSeconds(arg);
  }

  // Make the static methods enumerable
  // Skip non-configurable properties
  // so that `...` spreading would work
  static {
    for (const key of Object.getOwnPropertyNames(this)) {
      const desc = Object.getOwnPropertyDescriptor(this, key);
      if (!desc?.configurable) {
        continue;
      }
      if (!desc.enumerable) {
        Object.defineProperty(this, key, {
          ...desc,
          enumerable: true,
        });
      }
    }
  }
}

export type Env = Record<string, unknown>;
