import { ExpressionKey } from '../expressions/types';
import {
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
import type { Generator } from '../generator';

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
