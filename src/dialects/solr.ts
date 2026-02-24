import { Parser } from '../parser';
import {
  Tokenizer, TokenType,
} from '../tokens';
import { OrExpr } from '../expressions';
import {
  Dialect, NormalizationStrategy, Dialects,
} from './dialect';

export class SolrParser extends Parser {
  public static DISJUNCTION = {
    ...Parser.DISJUNCTION,
    [TokenType.DPIPE]: OrExpr,
  };
}

export class SolrTokenizer extends Tokenizer {
  public static QUOTES = ['\''];
  public static IDENTIFIERS = ['`'];
}

export class Solr extends Dialect {
  public static NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE;
  public static DPIPE_IS_STRING_CONCAT = false;
  public static SUPPORTS_SEMI_ANTI_JOIN = false;

  public static Parser = SolrParser;
  public static Tokenizer = SolrTokenizer;
}

Dialect.register(Dialects.SOLR, Solr);
