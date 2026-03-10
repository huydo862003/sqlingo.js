import { Parser } from '../parser';
import {
  Tokenizer, TokenType,
} from '../tokens';
import type { Expression } from '../expressions';
import { OrExpr } from '../expressions';
import { cache } from '../port_internals';
import { Generator } from '../generator';
import {
  Dialect, NormalizationStrategy, Dialects,
} from './dialect';

export class SolrParser extends Parser {
  @cache
  static get ID_VAR_TOKENS (): Set<TokenType> {
    return new Set([
      ...Parser.ID_VAR_TOKENS,
      TokenType.SESSION_USER,
      TokenType.CURRENT_CATALOG,
      TokenType.STRAIGHT_JOIN,
    ]);
  }

  // port from _Dialect metaclass logic
  @cache
  static get NO_PAREN_FUNCTIONS () {
    const noParenFunctions = { ...Parser.NO_PAREN_FUNCTIONS };
    delete noParenFunctions[TokenType.LOCALTIME];
    delete noParenFunctions[TokenType.LOCALTIMESTAMP];
    return noParenFunctions;
  }

  @cache
  static get DISJUNCTION (): Partial<Record<TokenType, typeof Expression>> {
    return {
      ...Parser.DISJUNCTION,
      [TokenType.DPIPE]: OrExpr,
    };
  }

  // port from _Dialect metaclass logic
  @cache
  static get TABLE_ALIAS_TOKENS (): Set<TokenType> {
    return new Set([...Parser.TABLE_ALIAS_TOKENS, TokenType.STRAIGHT_JOIN]);
  }
}
export class SolrTokenizer extends Tokenizer {
  @cache
  static get QUOTES () {
    return ['\''];
  }

  @cache
  static get IDENTIFIERS () {
    return ['`'];
  }
}

export class SolrGenerator extends Generator {
  @cache
  static get AFTER_HAVING_MODIFIER_TRANSFORMS () {
    const modifiers = new Map(super.AFTER_HAVING_MODIFIER_TRANSFORMS);
    [
      'cluster',
      'distribute',
      'sort',
    ].forEach((m) => modifiers.delete(m));
    return modifiers;
  }

  // port from _Dialect metaclass logic
  static SUPPORTS_DECODE_CASE = false;
  // port from _Dialect metaclass logic
  static readonly SELECT_KINDS: string[] = [];
  // port from _Dialect metaclass logic
  static TRY_SUPPORTED = false;
  // port from _Dialect metaclass logic
  static SUPPORTS_UESCAPE = false;
}

export class Solr extends Dialect {
  static DIALECT_NAME = Dialects.SOLR;

  @cache
  static get NORMALIZATION_STRATEGY () {
    return NormalizationStrategy.CASE_INSENSITIVE;
  }

  static DPIPE_IS_STRING_CONCAT = false;
  static SUPPORTS_SEMI_ANTI_JOIN = false;

  static Parser = SolrParser;
  static Tokenizer = SolrTokenizer;
  static Generator = SolrGenerator;
}

Dialect.register(Dialects.SOLR, Solr);
