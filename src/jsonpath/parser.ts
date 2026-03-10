import type { Token } from '../tokens';
import { TokenType } from '../tokens';
import type {
  DataTypeExprKind, JsonPathPartExpr,
} from '../expressions';
import {
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
} from '../expressions';
import { ParseError } from '../errors';
import {
  Dialect,
  type DialectType,
} from '../dialects/dialect';
import { JsonPathTokenizer } from './tokenizer';

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

  function curr (): TokenType | undefined {
    return i < size ? tokens[i].tokenType : undefined;
  }

  function prev (): Token {
    return tokens[i - 1];
  }

  function advance (): Token {
    i += 1;
    return prev();
  }

  function error (msg: string): string {
    return `${msg} at index ${i}: ${path}`;
  }

  function match (tokenType: TokenType, options?: { raiseUnmatched?: true }): Token;
  function match (tokenType: TokenType, options: { raiseUnmatched: false }): Token | undefined;
  function match (tokenType: TokenType, options: { raiseUnmatched?: boolean } = {}): Token | undefined {
    const { raiseUnmatched = false } = options;
    if (curr() === tokenType) {
      return advance();
    }
    if (raiseUnmatched) {
      throw new ParseError(error(`Expected ${tokenType}`));
    }
    return undefined;
  }

  function matchSet (types: Set<TokenType>): Token | undefined {
    const currToken = curr();
    if (currToken === undefined) return undefined;
    return types.has(currToken) ? advance() : undefined;
  }

  function parseLiteral (): string | JsonPathWildcardExpr | JsonPathScriptExpr | JsonPathFilterExpr | number | false {
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
        const currToken = curr();
        if (currToken === TokenType.R_BRACKET || currToken === undefined) {
          break;
        }
        advance();
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

  function parseSlice (): string | JsonPathWildcardExpr | JsonPathScriptExpr | JsonPathFilterExpr | JsonPathSliceExpr | number | false {
    const start = parseLiteral();
    const hasFirstColon = !!match(TokenType.COLON);
    const end = hasFirstColon ? parseLiteral() : undefined;
    const hasSecondColon = !!match(TokenType.COLON);
    const step = hasSecondColon ? parseLiteral() : undefined;

    if (!hasFirstColon && !hasSecondColon) {
      return start;
    }

    return new JsonPathSliceExpr({
      start,
      end,
      step,
    });
  }

  function parseBracket (): JsonPathPartExpr {
    const literal = parseSlice();

    if (typeof literal === 'string' || literal !== false) {
      type JsonPathIndexValue = string | JsonPathWildcardExpr | JsonPathScriptExpr | JsonPathFilterExpr | JsonPathSliceExpr | number;
      const indexes: JsonPathIndexValue[] = [literal as JsonPathIndexValue];
      while (match(TokenType.COMMA)) {
        const nextLiteral = parseSlice();
        if (nextLiteral !== false) {
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

      match(TokenType.R_BRACKET, { raiseUnmatched: true });
      return node;
    } else {
      throw new ParseError(error('Cannot have empty segment'));
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

  while (curr()) {
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
        throw new ParseError(error('Expected key name or * after DOT'));
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
      throw new ParseError(error(`Unexpected ${tokens[i].tokenType}`));
    }
  }

  return new JsonPathExpr({
    expressions,
  });
}
