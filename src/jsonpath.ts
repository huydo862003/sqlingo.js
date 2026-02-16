import type { Token } from './tokens';
import {
  Tokenizer, TokenType,
} from './tokens';
import type {
  DataTypeExprKind, JSONPathPartExpr,
} from './expressions';
import {
  ExpressionKey,
  JSONPathExpr,
  JSONPathFilterExpr,
  JSONPathKeyExpr,
  JSONPathRecursiveExpr,
  JSONPathRootExpr,
  JSONPathScriptExpr,
  JSONPathSelectorExpr,
  JSONPathSliceExpr,
  JSONPathSubscriptExpr,
  JSONPathUnionExpr,
  JSONPathWildcardExpr,
} from './expressions';
import { ParseError } from './errors';
import {
  Dialect,
  type DialectType,
} from './dialects/dialect';
import type { Generator } from './generator';

export class JSONPathTokenizer extends Tokenizer {
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

  static KEYWORDS (): Record<string, TokenType> {
    return {
      ...super.KEYWORDS(),
      '..': TokenType.DOT,
    };
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

export function parse (path: string, options?: ParseJsonPathOptions): JSONPathExpr {
  const jsonPathTokenizer = Dialect.getOrRaise(options?.dialect).jsonpathTokenizer();
  const tokens = jsonPathTokenizer.tokenize(path);
  const size = tokens.length;

  let i = 0;

  function _curr (): TokenType | undefined {
    return i < size ? tokens[i].tokenType : undefined;
  }

  function _prev (): Token {
    return tokens[i - 1];
  }

  function _advance (): Token {
    i += 1;
    return _prev();
  }

  function _error (msg: string): string {
    return `${msg} at index ${i}: ${path}`;
  }

  function _match (tokenType: TokenType, raiseUnmatched: true): Token;
  function _match (tokenType: TokenType, raiseUnmatched?: false): Token | undefined;
  function _match (tokenType: TokenType, raiseUnmatched = false): Token | undefined {
    if (_curr() === tokenType) {
      return _advance();
    }
    if (raiseUnmatched) {
      throw new ParseError(_error(`Expected ${tokenType}`));
    }
    return undefined;
  }

  function _matchSet (types: Set<TokenType>): Token | undefined {
    const curr = _curr();
    if (curr === undefined) return undefined;
    return types.has(curr) ? _advance() : undefined;
  }

  function _parseLiteral (): string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | number | false {
    const token = _match(TokenType.STRING) || _match(TokenType.IDENTIFIER);
    if (token) {
      return token.text;
    }
    if (_match(TokenType.STAR)) {
      return new JSONPathWildcardExpr({});
    }
    if (_match(TokenType.PLACEHOLDER) || _match(TokenType.L_PAREN)) {
      const script = _prev().text === '(';
      const start = i;

      while (true) {
        if (_match(TokenType.L_BRACKET)) {
          _parseBracket();
        }
        const curr = _curr();
        if (curr === TokenType.R_BRACKET || curr === undefined) {
          break;
        }
        _advance();
      }

      const ExprType = script ? JSONPathScriptExpr : JSONPathFilterExpr;
      const startPos = tokens[start].start ?? 0;
      const endPos = i < size ? tokens[i].end ?? path.length : path.length;
      return new ExprType({ this: path.slice(startPos, endPos) });
    }

    let number = _match(TokenType.DASH) ? '-' : '';

    const numToken = _match(TokenType.NUMBER);
    if (numToken) {
      number += numToken.text;
    }

    if (number) {
      return parseInt(number, 10);
    }

    return false;
  }

  function _parseSlice (): string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | JSONPathSliceExpr | number | false {
    const start = _parseLiteral();
    const end = _match(TokenType.COLON) ? _parseLiteral() : undefined;
    const step = _match(TokenType.COLON) ? _parseLiteral() : undefined;

    if (end === undefined && step === undefined) {
      return start;
    }

    return new JSONPathSliceExpr({
      start,
      end,
      step,
    });
  }

  function _parseBracket (): JSONPathPartExpr {
    const literal = _parseSlice();

    if (typeof literal === 'string' || literal !== false) {
      const indexes: (string | JSONPathWildcardExpr | JSONPathScriptExpr | JSONPathFilterExpr | JSONPathSliceExpr | number | false)[] = [literal];
      while (_match(TokenType.COMMA)) {
        const nextLiteral = _parseSlice();
        if (nextLiteral) {
          indexes.push(nextLiteral);
        }
      }

      let node: JSONPathPartExpr;
      if (indexes.length === 1) {
        if (typeof literal === 'string') {
          node = new JSONPathKeyExpr({ this: indexes[0] });
        } else if (literal instanceof JSONPathScriptExpr || literal instanceof JSONPathFilterExpr) {
          node = new JSONPathSelectorExpr({ this: indexes[0] });
        } else {
          node = new JSONPathSubscriptExpr({ this: indexes[0] });
        }
      } else {
        node = new JSONPathUnionExpr({ expressions: indexes });
      }

      _match(TokenType.R_BRACKET, true);
      return node;
    } else {
      throw new ParseError(_error('Cannot have empty segment'));
    }
  }

  function _parseVarText (): string {
    const prevIndex = i - 2;

    while (_matchSet(JSONPathTokenizer.VAR_TOKENS)) {}

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
  _match(TokenType.DOLLAR);
  const expressions: JSONPathPartExpr[] = [new JSONPathRootExpr({})];

  while (_curr()) {
    if (_match(TokenType.DOT) || _match(TokenType.COLON)) {
      const recursive = _prev().text === '..';

      let value: string | JSONPathWildcardExpr | undefined;
      if (_matchSet(JSONPathTokenizer.VAR_TOKENS)) {
        value = _parseVarText();
      } else if (_match(TokenType.IDENTIFIER)) {
        value = _prev().text;
      } else if (_match(TokenType.STAR)) {
        value = new JSONPathWildcardExpr({});
      } else {
        value = undefined;
      }

      if (recursive) {
        expressions.push(new JSONPathRecursiveExpr({ this: value }));
      } else if (value) {
        expressions.push(new JSONPathKeyExpr({ this: value }));
      } else {
        throw new ParseError(_error('Expected key name or * after DOT'));
      }
    } else if (_match(TokenType.L_BRACKET)) {
      expressions.push(_parseBracket());
    } else if (_matchSet(JSONPathTokenizer.VAR_TOKENS)) {
      expressions.push(new JSONPathKeyExpr({ this: _parseVarText() }));
    } else if (_match(TokenType.IDENTIFIER)) {
      expressions.push(new JSONPathKeyExpr({ this: _prev().text }));
    } else if (_match(TokenType.STAR)) {
      expressions.push(new JSONPathWildcardExpr({}));
    } else {
      throw new ParseError(_error(`Unexpected ${tokens[i].tokenType}`));
    }
  }

  return new JSONPathExpr({
    expressions,
  });
}

export const JSON_PATH_PART_TRANSFORMS = {
  [ExpressionKey.JSON_PATH_FILTER]: (_generator: Generator, e: JSONPathFilterExpr) => `?${e.this}`,
  [ExpressionKey.JSON_PATH_KEY]: (generator: Generator, e: JSONPathKeyExpr) => generator.jsonPathKeySql(e),
  [ExpressionKey.JSON_PATH_RECURSIVE]: (_generator: Generator, e: JSONPathRecursiveExpr) => `..${e.this || ''}`,
  [ExpressionKey.JSON_PATH_ROOT]: (_generator: Generator, _e: JSONPathRootExpr) => '$',
  [ExpressionKey.JSON_PATH_SCRIPT]: (_generator: Generator, e: JSONPathScriptExpr) => `(${e.this}`,
  [ExpressionKey.JSON_PATH_SELECTOR]: (generator: Generator, e: JSONPathSelectorExpr) => {
    return `[${generator.jsonPathPart(e.this)}]`;
  },
  [ExpressionKey.JSON_PATH_SLICE]: (generator: Generator, e: JSONPathSliceExpr) => {
    const parts = [
      e.args.start,
      e.args.end,
      e.args.step,
    ]
      .filter((p) => p !== undefined)
      .map((p) => p === false ? '' : generator.jsonPathPart(p));
    return parts.join(':');
  },
  [ExpressionKey.JSON_PATH_SUBSCRIPT]: (generator: Generator, e: JSONPathSubscriptExpr) => generator.jsonPathSubscriptSql(e),
  [ExpressionKey.JSON_PATH_UNION]: (generator: Generator, e: JSONPathUnionExpr) => {
    return `[${e.expressions.map((p) => generator.jsonPathPart(p)).join(',')}]`;
  },
  [ExpressionKey.JSON_PATH_WILDCARD]: (_generator: Generator, _e: JSONPathWildcardExpr) => '*',
} as const;

export const ALL_JSON_PATH_PARTS = new Set([
  JSONPathFilterExpr,
  JSONPathKeyExpr,
  JSONPathRecursiveExpr,
  JSONPathRootExpr,
  JSONPathScriptExpr,
  JSONPathSelectorExpr,
  JSONPathSliceExpr,
  JSONPathSubscriptExpr,
  JSONPathUnionExpr,
  JSONPathWildcardExpr,
]);
