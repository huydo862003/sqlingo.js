/**
 * Simulates Python's arithmetic dunder methods (__add__, __sub__, __mul__, etc.)
 * and bitwise/set dunder methods (__or__, __and__, __xor__, __lshift__, __rshift__).
 * Each interface represents a single operation and can be independently implemented
 * by a class. Built-in types (number, string, Array, Set) are handled transparently
 * by the helper functions which fall back to native operators when the operand does
 * not implement the interface. Dispatch order matches Python:
 *   1. a.op(b)  (__op__)
 *   2. b.rop(a) (__rop__, reflected)
 *   3. native fallback
 */

export interface AddableObject<TOther = unknown, TReturn = unknown> {
  add(other: TOther): TReturn;
}
export type Addable = AddableObject | Array<unknown> | number | string;
export function isAddable (a: unknown): a is Addable {
  return typeof a === 'number' || typeof a === 'string' || Array.isArray(a) || isAddableObj(a);
}
export function isAddableObj (a: unknown): a is AddableObject {
  return a !== null && typeof a === 'object' && typeof (a as AddableObject).add === 'function';
}

export interface RaddableObject<TOther = unknown, TReturn = unknown> {
  radd(other: TOther): TReturn;
}
export type Raddable = RaddableObject | Array<unknown> | number | string;
export function isRaddable (a: unknown): a is Raddable {
  return typeof a === 'number' || typeof a === 'string' || Array.isArray(a) || isRaddableObj(a);
}
export function isRaddableObj (a: unknown): a is RaddableObject {
  return a !== null && typeof a === 'object' && typeof (a as RaddableObject).radd === 'function';
}

export function add<A extends AddableObject<B, R>, B, R> (a: A, b: B): R;
export function add<A, B extends RaddableObject<A, R>, R> (a: A, b: B): R;
export function add (a: unknown[], b: unknown[]): unknown[];
export function add (a: unknown, b: unknown): unknown;
export function add (a: unknown, b: unknown): unknown {
  if (isAddableObj(a)) return (a as AddableObject).add(b);
  if (isRaddableObj(b)) return (b as RaddableObject).radd(a);
  if (Array.isArray(a) && Array.isArray(b)) return [...a, ...(b as unknown[])];
  // @ts-expect-error "Fallback to primitive operation"
  return a + b;
}

export interface SubtractableObject<TOther = unknown, TReturn = unknown> {
  sub(other: TOther): TReturn;
}
export type Subtractable = SubtractableObject | Set<unknown> | number;
export function isSubtractable (a: unknown): a is Subtractable {
  return typeof a === 'number' || a instanceof Set || isSubtractableObj(a);
}
export function isSubtractableObj (a: unknown): a is SubtractableObject {
  return a !== null && typeof a === 'object' && typeof (a as SubtractableObject).sub === 'function';
}

export interface RsubtractableObject<TOther = unknown, TReturn = unknown> {
  rsub(other: TOther): TReturn;
}
export type Rsubtractable = RsubtractableObject | number;
export function isRsubtractable (a: unknown): a is Rsubtractable {
  return typeof a === 'number' || isRsubtractableObj(a);
}
export function isRsubtractableObj (a: unknown): a is RsubtractableObject {
  return a !== null && typeof a === 'object' && typeof (a as RsubtractableObject).rsub === 'function';
}

export function sub<A extends SubtractableObject<B, R>, B, R> (a: A, b: B): R;
export function sub<A, B extends RsubtractableObject<A, R>, R> (a: A, b: B): R;
export function sub (a: Set<unknown>, b: Set<unknown>): Set<unknown>;
export function sub (a: unknown, b: unknown): unknown;
export function sub (a: unknown, b: unknown): unknown {
  if (isSubtractableObj(a)) return (a as SubtractableObject).sub(b);
  if (isRsubtractableObj(b)) return (b as RsubtractableObject).rsub(a);
  if (a instanceof Set && b instanceof Set) return new Set([...(a as Set<unknown>)].filter((x) => !(b as Set<unknown>).has(x)));
  // @ts-expect-error "Fallback to primitive operation"
  return a - b;
}

export interface MultipliableObject<TOther = unknown, TReturn = unknown> {
  mul(other: TOther): TReturn;
}
export type Multipliable = MultipliableObject | number;
export function isMultipliable (a: unknown): a is Multipliable {
  return typeof a === 'number' || isMultipliableObj(a);
}
export function isMultipliableObj (a: unknown): a is MultipliableObject {
  return a !== null && typeof a === 'object' && typeof (a as MultipliableObject).mul === 'function';
}

export interface RmultipliableObject<TOther = unknown, TReturn = unknown> {
  rmul(other: TOther): TReturn;
}
export type Rmultipliable = RmultipliableObject | number;
export function isRmultipliable (a: unknown): a is Rmultipliable {
  return typeof a === 'number' || isRmultipliableObj(a);
}
export function isRmultipliableObj (a: unknown): a is RmultipliableObject {
  return a !== null && typeof a === 'object' && typeof (a as RmultipliableObject).rmul === 'function';
}

export function mul<A extends MultipliableObject<B, R>, B, R> (a: A, b: B): R;
export function mul<A, B extends RmultipliableObject<A, R>, R> (a: A, b: B): R;
export function mul (a: unknown, b: unknown): unknown;
export function mul (a: unknown, b: unknown): unknown {
  if (isMultipliableObj(a)) return (a as MultipliableObject).mul(b);
  if (isRmultipliableObj(b)) return (b as RmultipliableObject).rmul(a);
  // @ts-expect-error "Fallback to primitive operation"
  return a * b;
}

export interface TrueDivisibleObject<TOther = unknown, TReturn = unknown> {
  truediv(other: TOther): TReturn;
}
export type TrueDivisible = TrueDivisibleObject | number;
export function isTrueDivisible (a: unknown): a is TrueDivisible {
  return typeof a === 'number' || isTrueDivisibleObj(a);
}
export function isTrueDivisibleObj (a: unknown): a is TrueDivisibleObject {
  return a !== null && typeof a === 'object' && typeof (a as TrueDivisibleObject).truediv === 'function';
}

export interface RtrueDivisibleObject<TOther = unknown, TReturn = unknown> {
  rtruediv(other: TOther): TReturn;
}
export type RtrueDivisible = RtrueDivisibleObject | number;
export function isRtrueDivisible (a: unknown): a is RtrueDivisible {
  return typeof a === 'number' || isRtrueDivisibleObj(a);
}
export function isRtrueDivisibleObj (a: unknown): a is RtrueDivisibleObject {
  return a !== null && typeof a === 'object' && typeof (a as RtrueDivisibleObject).rtruediv === 'function';
}

export function truediv<A extends TrueDivisibleObject<B, R>, B, R> (a: A, b: B): R;
export function truediv<A, B extends RtrueDivisibleObject<A, R>, R> (a: A, b: B): R;
export function truediv (a: unknown, b: unknown): unknown;
export function truediv (a: unknown, b: unknown): unknown {
  if (isTrueDivisibleObj(a)) return (a as TrueDivisibleObject).truediv(b);
  if (isRtrueDivisibleObj(b)) return (b as RtrueDivisibleObject).rtruediv(a);
  // @ts-expect-error "Fallback to primitive operation"
  return a / b;
}

export interface FloorDivisibleObject<TOther = unknown, TReturn = unknown> {
  floorDiv(other: TOther): TReturn;
}
export type FloorDivisible = FloorDivisibleObject | number;
export function isFloorDivisible (a: unknown): a is FloorDivisible {
  return typeof a === 'number' || isFloorDivisibleObj(a);
}
export function isFloorDivisibleObj (a: unknown): a is FloorDivisibleObject {
  return a !== null && typeof a === 'object' && typeof (a as FloorDivisibleObject).floorDiv === 'function';
}

export interface RfloorDivisibleObject<TOther = unknown, TReturn = unknown> {
  rfloorDiv(other: TOther): TReturn;
}
export type RfloorDivisible = RfloorDivisibleObject | number;
export function isRfloorDivisible (a: unknown): a is RfloorDivisible {
  return typeof a === 'number' || isRfloorDivisibleObj(a);
}
export function isRfloorDivisibleObj (a: unknown): a is RfloorDivisibleObject {
  return a !== null && typeof a === 'object' && typeof (a as RfloorDivisibleObject).rfloorDiv === 'function';
}

export function floorDiv<A extends FloorDivisibleObject<B, R>, B, R> (a: A, b: B): R;
export function floorDiv<A, B extends RfloorDivisibleObject<A, R>, R> (a: A, b: B): R;
export function floorDiv (a: unknown, b: unknown): unknown;
export function floorDiv (a: unknown, b: unknown): unknown {
  if (isFloorDivisibleObj(a)) return (a as FloorDivisibleObject).floorDiv(b);
  if (isRfloorDivisibleObj(b)) return (b as RfloorDivisibleObject).rfloorDiv(a);
  // @ts-expect-error "Fallback to primitive operation"
  return Math.trunc(a / b);
}

export interface ModableObject<TOther = unknown, TReturn = unknown> {
  mod(other: TOther): TReturn;
}
export type Modable = ModableObject | number;
export function isModable (a: unknown): a is Modable {
  return typeof a === 'number' || isModableObj(a);
}
export function isModableObj (a: unknown): a is ModableObject {
  return a !== null && typeof a === 'object' && typeof (a as ModableObject).mod === 'function';
}

export interface RmodableObject<TOther = unknown, TReturn = unknown> {
  rmod(other: TOther): TReturn;
}
export type Rmodable = RmodableObject | number;
export function isRmodable (a: unknown): a is Rmodable {
  return typeof a === 'number' || isRmodableObj(a);
}
export function isRmodableObj (a: unknown): a is RmodableObject {
  return a !== null && typeof a === 'object' && typeof (a as RmodableObject).rmod === 'function';
}

export function mod<A extends ModableObject<B, R>, B, R> (a: A, b: B): R;
export function mod<A, B extends RmodableObject<A, R>, R> (a: A, b: B): R;
export function mod (a: unknown, b: unknown): unknown;
export function mod (a: unknown, b: unknown): unknown {
  if (isModableObj(a)) return (a as ModableObject).mod(b);
  if (isRmodableObj(b)) return (b as RmodableObject).rmod(a);
  // @ts-expect-error "Fallback to primitive operation"
  return a % b;
}

export interface PowableObject<TOther = unknown, TReturn = unknown> {
  pow(other: TOther): TReturn;
}
export type Powable = PowableObject | number;
export function isPowable (a: unknown): a is Powable {
  return typeof a === 'number' || isPowableObj(a);
}
export function isPowableObj (a: unknown): a is PowableObject {
  return a !== null && typeof a === 'object' && typeof (a as PowableObject).pow === 'function';
}

export interface RpowableObject<TOther = unknown, TReturn = unknown> {
  rpow(other: TOther): TReturn;
}
export type Rpowable = RpowableObject | number;
export function isRpowable (a: unknown): a is Rpowable {
  return typeof a === 'number' || isRpowableObj(a);
}
export function isRpowableObj (a: unknown): a is RpowableObject {
  return a !== null && typeof a === 'object' && typeof (a as RpowableObject).rpow === 'function';
}

export function pow<A extends PowableObject<B, R>, B, R> (a: A, b: B): R;
export function pow<A, B extends RpowableObject<A, R>, R> (a: A, b: B): R;
export function pow (a: unknown, b: unknown): unknown;
export function pow (a: unknown, b: unknown): unknown {
  if (isPowableObj(a)) return (a as PowableObject).pow(b);
  if (isRpowableObj(b)) return (b as RpowableObject).rpow(a);
  // @ts-expect-error "Fallback to primitive operation"
  return a ** b;
}

export interface NegatableObject<TReturn = unknown> {
  neg(): TReturn;
}
export type Negatable = NegatableObject | number;
export function isNegatable (a: unknown): a is Negatable {
  return typeof a === 'number' || isNegatableObj(a);
}
export function isNegatableObj (a: unknown): a is NegatableObject {
  return a !== null && typeof a === 'object' && typeof (a as NegatableObject).neg === 'function';
}

export function neg<A extends NegatableObject<R>, R> (a: A): R;
export function neg (a: unknown): unknown;
export function neg (a: unknown): unknown {
  if (isNegatableObj(a)) return (a as NegatableObject).neg();
  // @ts-expect-error "Fallback to primitive operation"
  return -a;
}

export interface LtComparableObject<TOther = unknown> {
  lt(other: TOther): boolean;
}
export type LtComparable = LtComparableObject | number | string;
export function isLtComparableObj (a: unknown): a is LtComparableObject {
  return a !== null && typeof a === 'object' && typeof (a as LtComparableObject).lt === 'function';
}

export function lt<A extends LtComparableObject<B>, B> (a: A, b: B): boolean;
export function lt<A, B extends GtComparableObject<A>> (a: A, b: B): boolean;
export function lt (a: unknown, b: unknown): boolean;
export function lt (a: unknown, b: unknown): boolean {
  if (isLtComparableObj(a)) return (a as LtComparableObject).lt(b);
  if (isGtComparableObj(b)) return (b as GtComparableObject).gt(a);
  // @ts-expect-error "Fallback to primitive operation"
  return a < b;
}

export interface LteComparableObject<TOther = unknown> {
  lte(other: TOther): boolean;
}
export type LteComparable = LteComparableObject | number | string;
export function isLteComparableObj (a: unknown): a is LteComparableObject {
  return a !== null && typeof a === 'object' && typeof (a as LteComparableObject).lte === 'function';
}

export function lte<A extends LteComparableObject<B>, B> (a: A, b: B): boolean;
export function lte<A, B extends GteComparableObject<A>> (a: A, b: B): boolean;
export function lte (a: unknown, b: unknown): boolean;
export function lte (a: unknown, b: unknown): boolean {
  if (isLteComparableObj(a)) return (a as LteComparableObject).lte(b);
  if (isGteComparableObj(b)) return (b as GteComparableObject).gte(a);
  // @ts-expect-error "Fallback to primitive operation"
  return a <= b;
}

export interface GtComparableObject<TOther = unknown> {
  gt(other: TOther): boolean;
}
export type GtComparable = GtComparableObject | number | string;
export function isGtComparableObj (a: unknown): a is GtComparableObject {
  return a !== null && typeof a === 'object' && typeof (a as GtComparableObject).gt === 'function';
}

export function gt<A extends GtComparableObject<B>, B> (a: A, b: B): boolean;
export function gt<A, B extends LtComparableObject<A>> (a: A, b: B): boolean;
export function gt (a: unknown, b: unknown): boolean;
export function gt (a: unknown, b: unknown): boolean {
  if (isGtComparableObj(a)) return (a as GtComparableObject).gt(b);
  if (isLtComparableObj(b)) return (b as LtComparableObject).lt(a);
  // @ts-expect-error "Fallback to primitive operation"
  return b < a;
}

export interface GteComparableObject<TOther = unknown> {
  gte(other: TOther): boolean;
}
export type GteComparable = GteComparableObject | number | string;
export function isGteComparableObj (a: unknown): a is GteComparableObject {
  return a !== null && typeof a === 'object' && typeof (a as GteComparableObject).gte === 'function';
}

export function gte<A extends GteComparableObject<B>, B> (a: A, b: B): boolean;
export function gte<A, B extends LteComparableObject<A>> (a: A, b: B): boolean;
export function gte (a: unknown, b: unknown): boolean;
export function gte (a: unknown, b: unknown): boolean {
  if (isGteComparableObj(a)) return (a as GteComparableObject).gte(b);
  if (isLteComparableObj(b)) return (b as LteComparableObject).lte(a);
  // @ts-expect-error "Fallback to primitive operation"
  return b <= a;
}

export interface EqComparableObject<TOther = unknown> {
  eq(other: TOther): boolean;
}
export type EqComparable = EqComparableObject | number | string | boolean;
export function isEqComparableObj (a: unknown): a is EqComparableObject {
  return a !== null && typeof a === 'object' && typeof (a as EqComparableObject).eq === 'function';
}

export function eq<A extends EqComparableObject<B>, B> (a: A, b: B): boolean;
export function eq<A, B extends EqComparableObject<A>> (a: A, b: B): boolean;
export function eq (a: unknown, b: unknown): boolean;
export function eq (a: unknown, b: unknown): boolean {
  if (isEqComparableObj(a)) return (a as EqComparableObject).eq(b);
  if (isEqComparableObj(b)) return (b as EqComparableObject).eq(a);
  return a === b;
}

export interface NeqComparableObject<TOther = unknown> {
  neq(other: TOther): boolean;
}
export type NeqComparable = NeqComparableObject | number | string | boolean;
export function isNeqComparableObj (a: unknown): a is NeqComparableObject {
  return a !== null && typeof a === 'object' && typeof (a as NeqComparableObject).neq === 'function';
}

export function neq<A extends NeqComparableObject<B>, B> (a: A, b: B): boolean;
export function neq<A, B extends NeqComparableObject<A>> (a: A, b: B): boolean;
export function neq (a: unknown, b: unknown): boolean;
export function neq (a: unknown, b: unknown): boolean {
  if (isNeqComparableObj(a)) return (a as NeqComparableObject).neq(b);
  if (isNeqComparableObj(b)) return (b as NeqComparableObject).neq(a);
  return !eq(a, b);
}

export interface InvertableObject<TReturn = unknown> {
  invert(): TReturn;
}
export type Invertable = InvertableObject | number;
export function isInvertable (a: unknown): a is Invertable {
  return typeof a === 'number' || isInvertableObj(a);
}
export function isInvertableObj (a: unknown): a is InvertableObject {
  return a !== null && typeof a === 'object' && typeof (a as InvertableObject).invert === 'function';
}

export function invert<A extends InvertableObject<R>, R> (a: A): R;
export function invert (a: unknown): unknown;
export function invert (a: unknown): unknown {
  if (isInvertableObj(a)) return (a as InvertableObject).invert();
  // @ts-expect-error "Fallback to primitive operation"
  return ~a;
}

export interface OrableObject<TOther = unknown, TReturn = unknown> {
  or(other: TOther): TReturn;
}
export type Orable = OrableObject | Set<unknown> | number;
export function isOrable (a: unknown): a is Orable {
  return typeof a === 'number' || a instanceof Set || isOrableObj(a);
}
export function isOrableObj (a: unknown): a is OrableObject {
  return a !== null && typeof a === 'object' && typeof (a as OrableObject).or === 'function';
}

export interface RorableObject<TOther = unknown, TReturn = unknown> {
  ror(other: TOther): TReturn;
}
export type Rorable = RorableObject | Set<unknown> | number;
export function isRorable (a: unknown): a is Rorable {
  return typeof a === 'number' || a instanceof Set || isRorableObj(a);
}
export function isRorableObj (a: unknown): a is RorableObject {
  return a !== null && typeof a === 'object' && typeof (a as RorableObject).ror === 'function';
}

export function or<A extends OrableObject<B, R>, B, R> (a: A, b: B): R;
export function or<A, B extends RorableObject<A, R>, R> (a: A, b: B): R;
export function or (a: Set<unknown>, b: Set<unknown>): Set<unknown>;
export function or (a: unknown, b: unknown): unknown;
export function or (a: unknown, b: unknown): unknown {
  if (isOrableObj(a)) return (a as OrableObject).or(b);
  if (isRorableObj(b)) return (b as RorableObject).ror(a);
  if (a instanceof Set && b instanceof Set) return new Set([...(a as Set<unknown>), ...(b as Set<unknown>)]);
  // @ts-expect-error "Fallback to primitive operation"
  return a | b;
}

export interface AndableObject<TOther = unknown, TReturn = unknown> {
  and(other: TOther): TReturn;
}
export type Andable = AndableObject | Set<unknown> | number;
export function isAndable (a: unknown): a is Andable {
  return typeof a === 'number' || a instanceof Set || isAndableObj(a);
}
export function isAndableObj (a: unknown): a is AndableObject {
  return a !== null && typeof a === 'object' && typeof (a as AndableObject).and === 'function';
}

export interface RandableObject<TOther = unknown, TReturn = unknown> {
  rand(other: TOther): TReturn;
}
export type Randable = RandableObject | Set<unknown> | number;
export function isRandable (a: unknown): a is Randable {
  return typeof a === 'number' || a instanceof Set || isRandableObj(a);
}
export function isRandableObj (a: unknown): a is RandableObject {
  return a !== null && typeof a === 'object' && typeof (a as RandableObject).rand === 'function';
}

export function and<A extends AndableObject<B, R>, B, R> (a: A, b: B): R;
export function and<A, B extends RandableObject<A, R>, R> (a: A, b: B): R;
export function and (a: Set<unknown>, b: Set<unknown>): Set<unknown>;
export function and (a: unknown, b: unknown): unknown;
export function and (a: unknown, b: unknown): unknown {
  if (isAndableObj(a)) return (a as AndableObject).and(b);
  if (isRandableObj(b)) return (b as RandableObject).rand(a);
  if (a instanceof Set && b instanceof Set) return new Set([...(a as Set<unknown>)].filter((x) => (b as Set<unknown>).has(x)));
  // @ts-expect-error "Fallback to primitive operation"
  return a & b;
}

export interface XorableObject<TOther = unknown, TReturn = unknown> {
  xor(other: TOther): TReturn;
}
export type Xorable = XorableObject | Set<unknown> | number;
export function isXorable (a: unknown): a is Xorable {
  return typeof a === 'number' || a instanceof Set || isXorableObj(a);
}
export function isXorableObj (a: unknown): a is XorableObject {
  return a !== null && typeof a === 'object' && typeof (a as XorableObject).xor === 'function';
}

export interface RxorableObject<TOther = unknown, TReturn = unknown> {
  rxor(other: TOther): TReturn;
}
export type Rxorable = RxorableObject | Set<unknown> | number;
export function isRxorable (a: unknown): a is Rxorable {
  return typeof a === 'number' || a instanceof Set || isRxorableObj(a);
}
export function isRxorableObj (a: unknown): a is RxorableObject {
  return a !== null && typeof a === 'object' && typeof (a as RxorableObject).rxor === 'function';
}

export function xor<A extends XorableObject<B, R>, B, R> (a: A, b: B): R;
export function xor<A, B extends RxorableObject<A, R>, R> (a: A, b: B): R;
export function xor (a: Set<unknown>, b: Set<unknown>): Set<unknown>;
export function xor (a: unknown, b: unknown): unknown;
export function xor (a: unknown, b: unknown): unknown {
  if (isXorableObj(a)) return (a as XorableObject).xor(b);
  if (isRxorableObj(b)) return (b as RxorableObject).rxor(a);
  if (a instanceof Set && b instanceof Set) {
    const sa = a as Set<unknown>, sb = b as Set<unknown>;
    return new Set([...[...sa].filter((x) => !sb.has(x)), ...[...sb].filter((x) => !sa.has(x))]);
  }
  // @ts-expect-error "Fallback to primitive operation"
  return a ^ b;
}

export interface LshiftableObject<TOther = unknown, TReturn = unknown> {
  lshift(other: TOther): TReturn;
}
export type Lshiftable = LshiftableObject | number;
export function isLshiftable (a: unknown): a is Lshiftable {
  return typeof a === 'number' || isLshiftableObj(a);
}
export function isLshiftableObj (a: unknown): a is LshiftableObject {
  return a !== null && typeof a === 'object' && typeof (a as LshiftableObject).lshift === 'function';
}

export interface RlshiftableObject<TOther = unknown, TReturn = unknown> {
  rlshift(other: TOther): TReturn;
}
export type Rlshiftable = RlshiftableObject | number;
export function isRlshiftable (a: unknown): a is Rlshiftable {
  return typeof a === 'number' || isRlshiftableObj(a);
}
export function isRlshiftableObj (a: unknown): a is RlshiftableObject {
  return a !== null && typeof a === 'object' && typeof (a as RlshiftableObject).rlshift === 'function';
}

export function lshift<A extends LshiftableObject<B, R>, B, R> (a: A, b: B): R;
export function lshift<A, B extends RlshiftableObject<A, R>, R> (a: A, b: B): R;
export function lshift (a: unknown, b: unknown): unknown;
export function lshift (a: unknown, b: unknown): unknown {
  if (isLshiftableObj(a)) return (a as LshiftableObject).lshift(b);
  if (isRlshiftableObj(b)) return (b as RlshiftableObject).rlshift(a);
  // @ts-expect-error "Fallback to primitive operation"
  return a << b;
}

export interface RshiftableObject<TOther = unknown, TReturn = unknown> {
  rshift(other: TOther): TReturn;
}
export type Rshiftable = RshiftableObject | number;
export function isRshiftable (a: unknown): a is Rshiftable {
  return typeof a === 'number' || isRshiftableObj(a);
}
export function isRshiftableObj (a: unknown): a is RshiftableObject {
  return a !== null && typeof a === 'object' && typeof (a as RshiftableObject).rshift === 'function';
}

export interface RrshiftableObject<TOther = unknown, TReturn = unknown> {
  rrshift(other: TOther): TReturn;
}
export type Rrshiftable = RrshiftableObject | number;
export function isRrshiftable (a: unknown): a is Rrshiftable {
  return typeof a === 'number' || isRrshiftableObj(a);
}
export function isRrshiftableObj (a: unknown): a is RrshiftableObject {
  return a !== null && typeof a === 'object' && typeof (a as RrshiftableObject).rrshift === 'function';
}

export function rshift<A extends RshiftableObject<B, R>, B, R> (a: A, b: B): R;
export function rshift<A, B extends RrshiftableObject<A, R>, R> (a: A, b: B): R;
export function rshift (a: unknown, b: unknown): unknown;
export function rshift (a: unknown, b: unknown): unknown {
  if (isRshiftableObj(a)) return (a as RshiftableObject).rshift(b);
  if (isRrshiftableObj(b)) return (b as RrshiftableObject).rrshift(a);
  // @ts-expect-error "Fallback to primitive operation"
  return a >> b;
}

export interface IndexableObject<TOther = unknown, TReturn = unknown> {
  getItem(other: TOther): TReturn;
}
export type Indexable = IndexableObject | Array<unknown>;
export function isIndexable (a: unknown): a is Indexable {
  return Array.isArray(a) || isIndexableObj(a);
}
export function isIndexableObj (a: unknown): a is IndexableObject {
  return a !== null && typeof a === 'object' && typeof (a as IndexableObject).getItem === 'function';
}

export function getitem<A extends IndexableObject<B, R>, B, R> (a: A, b: B): R;
export function getitem (a: unknown[], b: number): unknown;
export function getitem (a: unknown, b: unknown): unknown;
export function getitem (a: unknown, b: unknown): unknown {
  if (isIndexableObj(a)) return (a as IndexableObject).getItem(b);
  if (Array.isArray(a)) return (a as unknown[])[b as number];
  throw new TypeError(`'${typeof a}' object is not subscriptable`);
}
