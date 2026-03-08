import { Parser } from '../parser';
import {
  Tokenizer, TokenType,
} from '../tokens';
import type { Expression } from '../expressions';
import { OrExpr } from '../expressions';
import { cache } from '../port_internals';
import {
  Dialect, NormalizationStrategy, Dialects,
} from './dialect';

export class SolrParser extends Parser {
  @cache
  static get DISJUNCTION (): Partial<Record<TokenType, typeof Expression>> {
    return {
      ...Parser.DISJUNCTION,
      [TokenType.DPIPE]: OrExpr,
    };
  }
}

export class SolrTokenizer extends Tokenizer {
  @cache
  static get QUOTES () {
    return ['\''] as const;
  }

  @cache
  static get IDENTIFIERS () {
    return ['`'] as const;
  }
}

export class Solr extends Dialect {
  static NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE;
  static DPIPE_IS_STRING_CONCAT = false;
  static SUPPORTS_SEMI_ANTI_JOIN = false;

  static Parser = SolrParser;
  static Tokenizer = SolrTokenizer;
}

Dialect.register(Dialects.SOLR, Solr);
