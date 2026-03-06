import { Parser } from '../parser';
import {
  Tokenizer, TokenType,
} from '../tokens';
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
  static QUOTES = ['\''];
  static IDENTIFIERS = ['`'];
}

export class Solr extends Dialect {
  static NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE;
  static DPIPE_IS_STRING_CONCAT = false;
  static SUPPORTS_SEMI_ANTI_JOIN = false;

  static Parser = SolrParser;
  static Tokenizer = SolrTokenizer;
}

Dialect.register(Dialects.SOLR, Solr);
