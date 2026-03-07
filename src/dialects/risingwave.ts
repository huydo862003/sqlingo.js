import type {
  Generator,
} from '../generator';
import type {
  Expression,
  DataTypeExpr,
  ComputedColumnConstraintExpr,
} from '../expressions';
import {
  PropertiesExpr,
  DataTypeExprKind,
  PropertiesLocation,
  FileFormatPropertyExpr,
  EncodePropertyExpr,
  WatermarkColumnConstraintExpr,
  ColumnDefExpr,
  IncludePropertyExpr,
} from '../expressions';
import { TokenType } from '../tokens';
import type { Parser } from '../parser';
import { cache } from '../port_internals';
import {
  Dialect, Dialects,
} from './dialect';
import { Postgres } from './postgres';

class RisingWaveTokenizer extends Postgres.Tokenizer {
  @cache
  static get ORIGINAL_KEYWORDS () {
    return {
      ...Postgres.Tokenizer.KEYWORDS,
      SINK: TokenType.SINK,
      SOURCE: TokenType.SOURCE,
    };
  }
}

class RisingWaveParser extends Postgres.Parser {
  static WRAPPED_TRANSFORM_COLUMN_CONSTRAINT = false;

  @cache
  static get PROPERTY_PARSERS () {
    return {
      ...Postgres.Parser.PROPERTY_PARSERS,
      ENCODE: function (this: Parser) {
        return (this as RisingWaveParser).parseEncodeProperty();
      },
      INCLUDE: function (this: Parser) {
        return (this as RisingWaveParser).parseIncludeProperty();
      },
      KEY: function (this: Parser) {
        return (this as RisingWaveParser).parseEncodeProperty(true);
      },
    };
  }

  @cache
  static get CONSTRAINT_PARSERS () {
    return {
      ...Postgres.Parser.CONSTRAINT_PARSERS,
      WATERMARK: function (this: Parser) {
        return this.expression(WatermarkColumnConstraintExpr, {
          this: (this as RisingWaveParser).match(TokenType.FOR) && (this as RisingWaveParser).parseColumn(),
          expression: (this as RisingWaveParser).match(TokenType.ALIAS) && (this as RisingWaveParser).parseDisjunction(),
        });
      },
    };
  }

  @cache
  static get SCHEMA_UNNAMED_CONSTRAINTS () {
    return new Set([...Postgres.Parser.SCHEMA_UNNAMED_CONSTRAINTS, 'WATERMARK']);
  }

  parseTableHints (): Expression[] | undefined {
    // There is no hint in risingwave.
    // Do nothing here to avoid WITH keywords conflict in CREATE SINK statement.
    return undefined;
  }

  parseIncludeProperty (): Expression | undefined {
    let header: Expression | undefined = undefined;
    let coldef: Expression | undefined = undefined;

    const thisNode = this.parseVarOrString();

    if (!this.match(TokenType.ALIAS)) {
      header = this.parseField();
      if (header) {
        coldef = this.expression(ColumnDefExpr, {
          this: header,
          kind: this.parseTypes(),
        });
      }
    }

    this.match(TokenType.ALIAS);
    const alias = this.parseIdVar({ tokens: (this.constructor as typeof RisingWaveParser).ALIAS_TOKENS });

    return this.expression(IncludePropertyExpr, {
      this: thisNode,
      alias: alias,
      columnDef: coldef,
    });
  }

  parseEncodeProperty (key?: boolean | undefined): EncodePropertyExpr {
    this.matchTextSeq('ENCODE');
    const thisNode = this.parseVarOrString();

    let properties: PropertiesExpr | undefined = undefined;
    if (this.match(TokenType.L_PAREN, { advance: false })) {
      properties = this.expression(PropertiesExpr, {
        expressions: this.parseWrappedProperties(),
      });
    }

    return this.expression(EncodePropertyExpr, {
      this: thisNode,
      properties: properties,
      key: key,
    });
  }
}

class RisingWaveGenerator extends Postgres.Generator {
  static LOCKING_READS_SUPPORTED = false;
  static SUPPORTS_BETWEEN_FLAGS = false;

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (this: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (this: Generator, e: any) => string>([
      ...Postgres.Generator.TRANSFORMS,
      [
        FileFormatPropertyExpr,
        function (this: Generator, e: FileFormatPropertyExpr) {
          return `FORMAT ${this.sql(e.args.this)}`;
        },
      ],
    ]);
    return transforms;
  }

  @cache
  static get PROPERTIES_LOCATION () {
    return {
      ...Postgres.Generator.PROPERTIES_LOCATION,
      [FileFormatPropertyExpr.constructor.name]: PropertiesLocation.POST_EXPRESSION,
    };
  }

  @cache
  static get EXPRESSION_PRECEDES_PROPERTIES_CREATABLES () { return new Set(['SINK']); }

  computedColumnConstraintSql (expression: ComputedColumnConstraintExpr): string {
    return super.computedColumnConstraintSql(expression);
  }

  dataTypeSql (expression: DataTypeExpr): string {
    if (expression.isType(DataTypeExprKind.MAP) && expression.args.expressions?.length === 2) {
      const [keyType, valueType] = expression.args.expressions;
      return `MAP(${this.sql(keyType)}, ${this.sql(valueType)})`;
    }

    return super.dataTypeSql(expression);
  }
}

export class RisingWave extends Postgres {
  static REQUIRES_PARENTHESIZED_STRUCT_ACCESS = true;
  static SUPPORTS_STRUCT_STAR_EXPANSION = true;

  static Tokenizer = RisingWaveTokenizer;
  static Parser = RisingWaveParser;
  static Generator = RisingWaveGenerator;
}

Dialect.register(Dialects.RISINGWAVE, RisingWave);
