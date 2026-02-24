// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/trie.py

/**
 * Result of a trie lookup operation.
 *
 */
export const enum TrieResult {
  /** The key was not found and is not a prefix of any key */
  FAILED,
  /** The key is a prefix of one or more keys in the trie */
  PREFIX,
  /** The key exists exactly in the trie */
  EXISTS,
}

/**
 * A trie node for efficient prefix matching.
 *
 * Each node maps characters to child nodes, with a special key `0` marking complete words.
 */
export type TrieNode = { [key: string]: TrieNode } & { 0?: true };

/**
 * A key represented as an array of characters.
 */
export type Key = string[];

/**
 * Creates a new trie from an array of keywords.
 *
 * @param keywords - Array of keys (each key is an array of characters)
 * @param trie - Optional existing trie to add keywords to
 * @returns A trie structure for efficient prefix matching
 *
 * @example
 * ```ts
 * const trie = newTrie([
 *   ['S', 'E', 'L', 'E', 'C', 'T'],
 *   ['S', 'E', 'T']
 * ]);
 * ```
 *
 */
export function newTrie (keywords: Iterable<Key>, trie?: TrieNode): TrieNode {
  const result = trie ?? {};

  for (const key of keywords) {
    let current: TrieNode = result;
    for (const char of key) {
      if (!current[char]) {
        current[char] = {};
      }
      current = current[char];
    }

    current[0] = true;
  }

  return result;
}

/**
 * Checks if a key exists in the trie.
 *
 * @param trie - The trie to search
 * @param key - The key to look up (array of characters)
 * @returns A tuple of [result, node] where result indicates the lookup outcome and node is the deepest matching node
 *
 * @example
 * ```ts
 * const trie = newTrie([['S', 'E', 'T']]);
 * inTrie(trie, ['S', 'E', 'T']); // [TrieResult.EXISTS, ...]
 * inTrie(trie, ['S', 'E']); // [TrieResult.PREFIX, ...]
 * inTrie(trie, ['S', 'E', 'L']); // [TrieResult.FAILED, ...]
 * ```
 *
 */
export function inTrie (trie: TrieNode, key: Key): [TrieResult, TrieNode] {
  const keyArray = Array.from(key);

  if (!keyArray.length) {
    return [TrieResult.FAILED, trie];
  }

  let current = trie;
  for (const char of keyArray) {
    if (!current[char]) {
      return [TrieResult.FAILED, current];
    }
    current = current[char];
  }

  if (current[0]) {
    return [TrieResult.EXISTS, current];
  }

  return [TrieResult.PREFIX, current];
}
