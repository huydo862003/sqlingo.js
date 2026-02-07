// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/trie.py

export const enum TrieResult {
  FAILED,
  PREFIX,
  EXISTS,
}

export type TrieNode = { [key: string]: TrieNode } & { 0?: true };

export type Key = string[];

export function newTrie (keywords: Key[], trie?: TrieNode): TrieNode {
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
