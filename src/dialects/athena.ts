import type { GeneratorOptions } from '../generator';
import {
  Generator,
} from '../generator';
import type { ParseOptions } from '../parser';
import { Parser } from '../parser';
import type { TokenizerOptions } from '../tokens';
import {
  Tokenizer, Token, TokenType,
} from '../tokens';
import type {
  Expression,
} from '../expressions';
import {
  CreateExpr,
  ExternalPropertyExpr,
  QueryExpr,
  AlterExpr,
  DropExpr,
  DescribeExpr,
  ShowExpr,
  PropertiesExpr,
  PropertyExpr,
  LocationPropertyExpr,
  PartitionedByPropertyExpr,
  SchemaExpr,
  ColumnDefExpr,
  PropertiesLocation,
  CreateExprKind,
  DropExprKind,
  AlterExprKind,
} from '../expressions';
import type { DialectOptions } from './dialect';
import {
  Dialect, Dialects,
} from './dialect';
import { Hive } from './hive';
import { Trino } from './trino';

function tokenizeAsHive (tokensList: Token[]): boolean {
  if (tokensList.length < 2) {
    return false;
  }

  const [
    first,
    second,
    ...rest
  ] = tokensList;

  const firstType = first.tokenType;
  const firstText = first.text.toUpperCase();
  const secondType = second.tokenType;
  const secondText = second.text.toUpperCase();

  if ([TokenType.DESCRIBE, TokenType.SHOW].includes(firstType) || firstText === 'MSCK REPAIR') {
    return true;
  }

  if ([
    TokenType.ALTER,
    TokenType.CREATE,
    TokenType.DROP,
  ].includes(firstType)) {
    if ([
      'DATABASE',
      'EXTERNAL',
      'SCHEMA',
    ].includes(secondText)) {
      return true;
    }
    if (secondType === TokenType.VIEW) {
      return false;
    }

    return rest.every((t) => t.tokenType !== TokenType.SELECT);
  }

  return false;
}

function generateAsHive (expression: Expression): boolean {
  if (expression instanceof CreateExpr) {
    if (expression.args.kind === CreateExprKind.TABLE) {
      const properties = expression.args.properties;

      if (properties && properties.find(ExternalPropertyExpr)) {
        return true;
      }

      if (!(expression.args.expression instanceof QueryExpr)) {
        return true;
      }
    } else {
      return expression.args.kind !== CreateExprKind.VIEW;
    }
  } else if (expression instanceof AlterExpr || expression instanceof DropExpr || expression instanceof DescribeExpr || expression instanceof ShowExpr) {
    if (expression instanceof DropExpr && expression.args.kind === DropExprKind.VIEW) {
      return false;
    }

    return true;
  }

  return false;
}

function isIcebergTable (properties: PropertiesExpr): boolean {
  for (const p of properties.args.expressions || []) {
    if (p instanceof PropertyExpr && p.args.this?.toString().toLowerCase() === 'table_type') {
      return p.args.value?.toString().toLowerCase() === 'iceberg';
    }
  }

  return false;
}

function locationPropertySql (self: Generator, e: LocationPropertyExpr): string {
  let propName = 'external_location';

  if (e.parent instanceof PropertiesExpr) {
    if (isIcebergTable(e.parent)) {
      propName = 'location';
    }
  }

  return `${propName}=${self.sql(e, 'this')}`;
}

function partitionedByPropertySql (self: Generator, e: PartitionedByPropertyExpr): string {
  let propName = 'partitioned_by';

  if (e.parent instanceof PropertiesExpr) {
    if (isIcebergTable(e.parent)) {
      propName = 'partitioning';
    }
  }

  return `${propName}=${self.sql(e, 'this')}`;
}

class HiveGeneratorExtension extends Hive.Generator {
  public alterSql (expression: AlterExpr): string {
    if (expression instanceof AlterExpr && expression.args.kind === AlterExprKind.TABLE) {
      if (expression.args.actions && expression.args.actions[0] instanceof ColumnDefExpr) {
        const newActions = new SchemaExpr({ expressions: expression.args.actions });
        expression.setArgKey('actions', [newActions]);
      }
    }

    return super.alterSql(expression);
  }
}

class TrinoTokenizerExtension extends Trino.Tokenizer {
  public static ORIGINAL_KEYWORDS: Record<string, TokenType> = {
    ...Trino.Tokenizer.ORIGINAL_KEYWORDS,
    UNLOAD: TokenType.COMMAND,
  };
}

class TrinoParserExtension extends Trino.Parser {
  public static STATEMENT_PARSERS: Record<string, (self: Parser) => Expression | undefined> = {
    ...Trino.Parser.STATEMENT_PARSERS,
    [TokenType.USING]: (self: Parser) => self.parseAsCommand((self as TrinoParserExtension).prev),
  };
}

class TrinoGeneratorExtension extends Trino.Generator {
  public static PROPERTIES_LOCATION: Map<typeof Expression, PropertiesLocation> = (() => {
    const m = new Map(Trino.Generator.PROPERTIES_LOCATION);
    m.set(LocationPropertyExpr, (PropertiesLocation).POST_WITH);
    return m;
  })();

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  public static ORIGINAL_TRANSFORMS: Map<typeof Expression, (self: Generator, e: any) => string> = (() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const m = new Map<typeof Expression, (self: Generator, e: any) => string>(Trino.Generator.ORIGINAL_TRANSFORMS);
    m.set(PartitionedByPropertyExpr, partitionedByPropertySql);
    m.set(LocationPropertyExpr, locationPropertySql);
    return m;
  })();
}

export class AthenaTokenizer extends Tokenizer {
  public static IDENTIFIERS = [...Trino.Tokenizer.IDENTIFIERS, ...Hive.Tokenizer.IDENTIFIERS];
  public static STRING_ESCAPES = [...Trino.Tokenizer.STRING_ESCAPES, ...Hive.Tokenizer.STRING_ESCAPES];
  public static HEX_STRINGS = [...Trino.Tokenizer.HEX_STRINGS, ...Hive.Tokenizer.HEX_STRINGS];
  public static UNICODE_STRINGS = [...Trino.Tokenizer.UNICODE_STRINGS, ...Hive.Tokenizer.UNICODE_STRINGS];

  public static NUMERIC_LITERALS = {
    ...Trino.Tokenizer.NUMERIC_LITERALS,
    ...Hive.Tokenizer.NUMERIC_LITERALS,
  };

  public static ORIGINAL_KEYWORDS: Record<string, TokenType> = {
    ...Hive.Tokenizer.ORIGINAL_KEYWORDS,
    ...Trino.Tokenizer.ORIGINAL_KEYWORDS,
    UNLOAD: TokenType.COMMAND,
  };

  private hiveTokenizer: InstanceType<typeof Hive.Tokenizer>;
  private trinoTokenizer: InstanceType<typeof Trino.Tokenizer>;

  constructor (options: TokenizerOptions & {
    hive?: Hive;
    trino?: Trino;
  } = {}) {
    const {
      hive = new Hive(),
      trino = new Trino(),
    } = options;

    super(options);
    this.hiveTokenizer = hive.tokenizer({
      ...options,
      dialect: hive,
    }) as InstanceType<typeof Hive.Tokenizer>;
    this.trinoTokenizer = new TrinoTokenizerExtension({
      ...options,
      dialect: trino,
    });
  }

  public tokenize (sql: string): Token[] {
    const tokensResult = super.tokenize(sql);

    if (tokenizeAsHive(tokensResult)) {
      return [new Token(TokenType.HIVE_TOKEN_STREAM, '')].concat(this.hiveTokenizer.tokenize(sql));
    }

    return this.trinoTokenizer.tokenize(sql);
  }
}

export class AthenaParser extends Parser {
  private hiveParser: InstanceType<typeof Hive.Parser>;
  private trinoParser: InstanceType<typeof Trino.Parser>;

  constructor (options: ParseOptions & {
    hive?: Hive;
    trino?: Trino;
  } = {}) {
    const {
      hive = new Hive(),
      trino = new Trino(),
    } = options;
    super(options);
    this.hiveParser = hive.parser({
      ...options,
      dialect: hive,
    }) as InstanceType<typeof Hive.Parser>;
    this.trinoParser = new TrinoParserExtension({
      ...options,
      dialect: trino,
    });
  }

  public parse (rawTokens: Token[], sql?: string): (Expression | undefined)[] {
    if (rawTokens && rawTokens[0].tokenType === (TokenType).HIVE_TOKEN_STREAM) {
      return this.hiveParser.parse(rawTokens.slice(1), sql);
    }

    return this.trinoParser.parse(rawTokens, sql);
  }

  public parseIntoTypes (expressionTypes: string | string[], rawTokens: Token[], sql?: string): (Expression | undefined)[] {
    if (rawTokens && rawTokens[0].tokenType === (TokenType).HIVE_TOKEN_STREAM) {
      return this.hiveParser.parseIntoTypes(expressionTypes, rawTokens.slice(1), sql);
    }

    return this.trinoParser.parseIntoTypes(expressionTypes, rawTokens, sql);
  }
}

export class AthenaGenerator extends Generator {
  private hiveGenerator: InstanceType<typeof Hive.Generator>;
  private trinoGenerator: InstanceType<typeof Trino.Generator>;

  constructor (options: GeneratorOptions & {
    hive?: Hive;
    trino?: Trino;
  } = {}) {
    const {
      hive = new Hive(),
      trino = new Trino(),
    } = options;
    super(options);
    this.hiveGenerator = new HiveGeneratorExtension({
      ...options,
      dialect: hive,
    });
    this.trinoGenerator = new TrinoGeneratorExtension({
      ...options,
      dialect: trino,
    });
  }

  public generate (expression: Expression, options: { copy?: boolean } = {}): string {
    const { copy = true } = options;
    const generatorInstance = generateAsHive(expression) ? this.hiveGenerator : this.trinoGenerator;
    return generatorInstance.generate(expression, { copy });
  }
}

export class Athena extends Dialect {
  private hive: Hive;
  private trino: Trino;

  constructor (options: DialectOptions = {}) {
    super(options);
    this.hive = new Hive(options);
    this.trino = new Trino(options);
  }

  public tokenize (sql: string, options: TokenizerOptions = {}): Token[] {
    options.hive = this.hive;
    options.trino = this.trino;
    return super.tokenize(sql, options);
  }

  public parse (sql: string, options: ParseOptions = {}): (Expression | undefined)[] {
    options.hive = this.hive;
    options.trino = this.trino;
    return super.parse(sql, options);
  }

  public parseIntoTypes (expressionType: string | string[], sql: string, options: ParseOptions = {}): (Expression | undefined)[] {
    options.hive = this.hive;
    options.trino = this.trino;
    return super.parseInto(expressionType, sql, options);
  }

  public generate (expression: Expression, options: GeneratorOptions = {}): string {
    options.hive = this.hive;
    options.trino = this.trino;
    return super.generate(expression, options);
  }

  public static Tokenizer = AthenaTokenizer;
  public static Parser = AthenaParser;
  public static Generator = AthenaGenerator;
}

Dialect.register(Dialects.ATHENA, Athena);
