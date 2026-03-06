import {
  Tokenizer, TokenType,
} from '../tokens';

export class JsonPathTokenizer extends Tokenizer {
  static SINGLE_TOKENS: Record<string, TokenType> = {
    '(': TokenType.L_PAREN,
    ')': TokenType.R_PAREN,
    '[': TokenType.L_BRACKET,
    ']': TokenType.R_BRACKET,
    ':': TokenType.COLON,
    ',': TokenType.COMMA,
    '-': TokenType.DASH,
    '.': TokenType.DOT,
    '?': TokenType.PLACEHOLDER,
    '@': TokenType.PARAMETER,
    '\'': TokenType.QUOTE,
    '"': TokenType.QUOTE,
    '$': TokenType.DOLLAR,
    '*': TokenType.STAR,
  };

  static #KEYWORDS: WeakMap<typeof JsonPathTokenizer, Record<string, TokenType>> = new WeakMap();
  static get KEYWORDS (): Record<string, TokenType> {
    if (this.#KEYWORDS.has(this)) {
      return this.#KEYWORDS.get(this)!;
    }
    const res = {
      ...super.KEYWORDS,
      '..': TokenType.DOT,
    };
    this.#KEYWORDS.set(this, res);
    return res;
  }

  static IDENTIFIER_ESCAPES: string[] = ['\\'];
  static STRING_ESCAPES: string[] = ['\\'];
  static VAR_TOKENS = new Set([TokenType.VAR]);
}
