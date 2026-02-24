import type { Token } from './tokens';
import {
  Tokenizer, TokenType,
} from './tokens';
import type {
  DataTypeExprKind, ExpressionOrString, JsonPathPartExpr,
} from './expressions';
import {
  ExpressionKey,
  JsonPathExpr,
  JsonPathFilterExpr,
  JsonPathKeyExpr,
  JsonPathRecursiveExpr,
  JsonPathRootExpr,
  JsonPathScriptExpr,
  JsonPathSelectorExpr,
  JsonPathSliceExpr,
  JsonPathSubscriptExpr,
  JsonPathUnionExpr,
  JsonPathWildcardExpr,
} from './expressions';
import { ParseError } from './errors';
import {
  Dialect,
  type DialectType,
} from './dialects/dialect';
import type { Generator } from './generator';

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

  private static jsonPathKeywordsCache: WeakMap<typeof JsonPathTokenizer, Record<string, TokenType>> = new WeakMap();
  static get KEYWORDS (): Record<string, TokenType> {
    if (this.jsonPathKeywordsCache.has(this)) {
      return this.jsonPathKeywordsCache.get(this)!;
    }
    const res = {
      ...super.KEYWORDS,
      '..': TokenType.DOT,
    };
    this.jsonPathKeywordsCache.set(this, res);
    return res;
  }

  static IDENTIFIER_ESCAPES: string[] = ['\\'];
  static STRING_ESCAPES: string[] = ['\\'];
  static VAR_TOKENS = new Set([TokenType.VAR]);
}

export interface ParseJsonPathOptions {
  into?: DataTypeExprKind;
  dialect?: DialectType;
  [key: string]: unknown;
}

export function parse (path: string, options?: ParseJsonPathOptions): JsonPathExpr {
  const jsonPathTokenizer = Dialect.getOrRaise(options?.dialect).jsonpathTokenizer();
  const tokens = jsonPathTokenizer.tokenize(path);
  const size = tokens.length;

  let i = 0;

  function _curr (): TokenType | undefined {
    return i < size ? tokens[i].tokenType : undefined;
  }

  function prev (): Token {
    return tokens[i - 1];
  }

  function _advance (): Token {
    i += 1;
    return prev();
  }

  function _error (msg: string): string {
    return `${msg} at index ${i}: ${path}`;
  }

  function match (tokenType: TokenType, raiseUnmatched: true): Token;
  function match (tokenType: TokenType, raiseUnmatched?: false): Token | undefined;
  function match (tokenType: TokenType, raiseUnmatched = false): Token | undefined {
    if (_curr() === tokenType) {
      return _advance();
    }
    if (raiseUnmatched) {
      throw new ParseError(_error(`Expected ${tokenType}`));
    }
    return undefined;
  }

  function matchSet (types: Set<TokenType>): Token | undefined {
    const curr = _curr();
    if (curr === undefined) return undefined;
    return types.has(curr) ? _advance() : undefined;
  }

  function _parseLiteral (): string | JsonPathWildcardExpr | JsonPathScriptExpr | JsonPathFilterExpr | number | false {
    const token = match(TokenType.STRING) || match(TokenType.IDENTIFIER);
    if (token) {
      return token.text;
    }
    if (match(TokenType.STAR)) {
      return new JsonPathWildcardExpr({});
    }
    if (match(TokenType.PLACEHOLDER) || match(TokenType.L_PAREN)) {
      const script = prev().text === '(';
      const start = i;

      while (true) {
        if (match(TokenType.L_BRACKET)) {
          parseBracket();
        }
        const curr = _curr();
        if (curr === TokenType.R_BRACKET || curr === undefined) {
          break;
        }
        _advance();
      }

      const ExprType = script ? JsonPathScriptExpr : JsonPathFilterExpr;
      const startPos = tokens[start].start ?? 0;
      const endPos = i < size ? tokens[i].end ?? path.length : path.length;
      return new ExprType({ this: path.slice(startPos, endPos) });
    }

    let number = match(TokenType.DASH) ? '-' : '';

    const numToken = match(TokenType.NUMBER);
    if (numToken) {
      number += numToken.text;
    }

    if (number) {
      return parseInt(number, 10);
    }

    return false;
  }

  function _parseSlice (): string | JsonPathWildcardExpr | JsonPathScriptExpr | JsonPathFilterExpr | JsonPathSliceExpr | number | false {
    const start = _parseLiteral();
    const end = match(TokenType.COLON) ? _parseLiteral() : undefined;
    const step = match(TokenType.COLON) ? _parseLiteral() : undefined;

    if (end === undefined && step === undefined) {
      return start;
    }

    return new JsonPathSliceExpr({
      start: start as ExpressionOrString | number | undefined,
      end: end as ExpressionOrString | number | undefined,
      step: step as ExpressionOrString | number | undefined,
    });
  }

  function parseBracket (): JsonPathPartExpr {
    const literal = _parseSlice();

    if (typeof literal === 'string' || literal !== false) {
      const indexes: (string | JsonPathWildcardExpr | JsonPathScriptExpr | JsonPathFilterExpr | JsonPathSliceExpr | number)[] = [literal];
      while (match(TokenType.COMMA)) {
        const nextLiteral = _parseSlice();
        if (nextLiteral) {
          indexes.push(nextLiteral);
        }
      }

      let node: JsonPathPartExpr;
      if (indexes.length === 1) {
        if (typeof literal === 'string') {
          node = new JsonPathKeyExpr({ this: indexes[0] });
        } else if (literal instanceof JsonPathScriptExpr || literal instanceof JsonPathFilterExpr) {
          node = new JsonPathSelectorExpr({ this: indexes[0] });
        } else {
          node = new JsonPathSubscriptExpr({ this: indexes[0] });
        }
      } else {
        node = new JsonPathUnionExpr({ expressions: indexes });
      }

      match(TokenType.R_BRACKET, true);
      return node;
    } else {
      throw new ParseError(_error('Cannot have empty segment'));
    }
  }

  function parseVarText (): string {
    const prevIndex = i - 2;

    while (matchSet(JsonPathTokenizer.VAR_TOKENS)) {}

    const start = prevIndex < 0 ? 0 : (tokens[prevIndex].end ?? 0) + 1;

    let text: string;
    if (tokens.length <= i) {
      text = path.slice(start);
    } else {
      text = path.slice(start, tokens[i].start ?? path.length);
    }

    return text;
  }

  // We canonicalize the JSON path AST so that it always starts with a
  // "root" element, so paths like "field" will be generated as "$.field"
  match(TokenType.DOLLAR);
  const expressions: JsonPathPartExpr[] = [new JsonPathRootExpr({})];

  while (_curr()) {
    if (match(TokenType.DOT) || match(TokenType.COLON)) {
      const recursive = prev().text === '..';

      let value: string | JsonPathWildcardExpr | undefined;
      if (matchSet(JsonPathTokenizer.VAR_TOKENS)) {
        value = parseVarText();
      } else if (match(TokenType.IDENTIFIER)) {
        value = prev().text;
      } else if (match(TokenType.STAR)) {
        value = new JsonPathWildcardExpr({});
      } else {
        value = undefined;
      }

      if (recursive) {
        expressions.push(new JsonPathRecursiveExpr({ this: value }));
      } else if (value) {
        expressions.push(new JsonPathKeyExpr({ this: value }));
      } else {
        throw new ParseError(_error('Expected key name or * after DOT'));
      }
    } else if (match(TokenType.L_BRACKET)) {
      expressions.push(parseBracket());
    } else if (matchSet(JsonPathTokenizer.VAR_TOKENS)) {
      expressions.push(new JsonPathKeyExpr({ this: parseVarText() }));
    } else if (match(TokenType.IDENTIFIER)) {
      expressions.push(new JsonPathKeyExpr({ this: prev().text }));
    } else if (match(TokenType.STAR)) {
      expressions.push(new JsonPathWildcardExpr({}));
    } else {
      throw new ParseError(_error(`Unexpected ${tokens[i].tokenType}`));
    }
  }

  return new JsonPathExpr({
    expressions,
  });
}

export const JSON_PATH_PART_TRANSFORMS = {
  [ExpressionKey.JSON_PATH_FILTER]: (_generator: Generator, e: JsonPathFilterExpr) => `?${e.args.this}`,
  [ExpressionKey.JSON_PATH_KEY]: (generator: Generator, e: JsonPathKeyExpr) => generator.jsonPathKeySql(e),
  [ExpressionKey.JSON_PATH_RECURSIVE]: (_generator: Generator, e: JsonPathRecursiveExpr) => `..${e.args.this || ''}`,
  [ExpressionKey.JSON_PATH_ROOT]: (_generator: Generator, _e: JsonPathRootExpr) => '$',
  [ExpressionKey.JSON_PATH_SCRIPT]: (_generator: Generator, e: JsonPathScriptExpr) => `(${e.args.this}`,
  [ExpressionKey.JSON_PATH_SELECTOR]: (generator: Generator, e: JsonPathSelectorExpr) => {
    return `[${generator.jsonPathPart(e.args.this)}]`;
  },
  [ExpressionKey.JSON_PATH_SLICE]: (generator: Generator, e: JsonPathSliceExpr) => {
    const parts = [
      e.args.start,
      e.args.end,
      e.args.step,
    ]
      .filter((p) => p !== undefined)
      .map((p) => p === undefined ? '' : generator.jsonPathPart(p));
    return parts.join(':');
  },
  [ExpressionKey.JSON_PATH_SUBSCRIPT]: (generator: Generator, e: JsonPathSubscriptExpr) => generator.jsonPathSubscriptSql(e),
  [ExpressionKey.JSON_PATH_UNION]: (generator: Generator, e: JsonPathUnionExpr) => {
    return `[${(e.args.expressions ?? []).map((p) => generator.jsonPathPart(p)).join(',')}]`;
  },
  [ExpressionKey.JSON_PATH_WILDCARD]: (_generator: Generator, _e: JsonPathWildcardExpr) => '*',
} as const;

export const ALL_JSON_PATH_PARTS = new Set([
  JsonPathFilterExpr,
  JsonPathKeyExpr,
  JsonPathRecursiveExpr,
  JsonPathRootExpr,
  JsonPathScriptExpr,
  JsonPathSelectorExpr,
  JsonPathSliceExpr,
  JsonPathSubscriptExpr,
  JsonPathUnionExpr,
  JsonPathWildcardExpr,
]);
