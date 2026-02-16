/**
 * A two-level Map keyed by a binary tuple [FirstT, SecondT].
 */
export class MapBinaryTuple<KeyT extends [unknown, unknown], ValueT> {
  private readonly _map = new Map<KeyT[0], Map<KeyT[1], ValueT>>();

  get (first: KeyT[0], second: KeyT[1]): ValueT | undefined {
    return this._map.get(first)?.get(second);
  }

  set (first: KeyT[0], second: KeyT[1], value: ValueT): this {
    let inner = this._map.get(first);
    if (!inner) {
      inner = new Map<KeyT[1], ValueT>();
      this._map.set(first, inner);
    }
    inner.set(second, value);
    return this;
  }

  has (first: KeyT[0], second: KeyT[1]): boolean {
    return this._map.get(first)?.has(second) ?? false;
  }

  delete (first: KeyT[0], second: KeyT[1]): boolean {
    const inner = this._map.get(first);
    if (!inner) {
      return false;
    }
    const deleted = inner.delete(second);
    if (inner.size === 0) {
      this._map.delete(first);
    }
    return deleted;
  }

  clear (): void {
    this._map.clear();
  }

  get size (): number {
    let count = 0;
    for (const inner of this._map.values()) {
      count += inner.size;
    }
    return count;
  }

  * [Symbol.iterator] (): Iterator<[KeyT, ValueT]> {
    for (const [first, inner] of this._map) {
      for (const [second, value] of inner) {
        yield [[first, second] as KeyT, value];
      }
    }
  }

  * keys (): Generator<KeyT> {
    for (const [first, inner] of this._map) {
      for (const second of inner.keys()) {
        yield [first, second] as KeyT;
      }
    }
  }

  * values (): Generator<ValueT> {
    for (const inner of this._map.values()) {
      yield* inner.values();
    }
  }

  * entries (): Generator<[KeyT, ValueT]> {
    yield* this;
  }
}
