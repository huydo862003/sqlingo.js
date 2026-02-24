import type {
  Generator,
} from '../generator';
import {
  TokenType,
} from '../tokens';
import {
  type Expression,
  DataTypeExpr,
  DataTypeParamExpr,
  LiteralExpr,
  CreateExpr,
  CreateExprKind,
  type CommandExpr,
  SchemaExpr,
  ColumnDefExpr,
  DataTypeExprKind,
  AtTimeZoneExpr,
  SelectExpr,
  CastExpr,
  UnixToTimeExpr,
  DateAddExpr,
  var_,
  cast,
  func,
  literal,
} from '../expressions';
import { preprocess } from '../transforms';
import { TSQL } from './tsql';
import {
  Dialect, NormalizationStrategy, Dialects,
} from './dialect';

function capDataTypePrecision (expression: DataTypeExpr, maxPrecision: number = 6): DataTypeExpr {
  const precisionParam = expression.find(DataTypeParamExpr);

  let targetPrecision = maxPrecision;
  if (precisionParam && precisionParam.args.this instanceof LiteralExpr && precisionParam.args.this.isNumber) {
    const currentPrecision = Number(precisionParam.args.this.toString());
    targetPrecision = Math.min(currentPrecision, maxPrecision);
  }

  return new DataTypeExpr({
    this: expression.args.this,
    expressions: [new DataTypeParamExpr({ this: literal(targetPrecision) })],
  });
}

function addDefaultPrecisionToVarchar (expression: Expression): Expression {
  if (
    expression instanceof CreateExpr
    && expression.args.kind === CreateExprKind.TABLE
    && expression.args.this instanceof SchemaExpr
  ) {
    for (const columnDef of expression.args.this.args.expressions || []) {
      if (columnDef instanceof ColumnDefExpr) {
        const columnType = columnDef.args.kind;
        if (
          columnType instanceof DataTypeExpr
          && [DataTypeExprKind.VARCHAR, DataTypeExprKind.CHAR].includes(columnType.args.this as DataTypeExprKind)
          && (!columnType.args.expressions || columnType.args.expressions.length === 0)
        ) {
          columnType.setArgKey('expressions', [var_('MAX')]);
        }
      }
    }
  }

  return expression;
}

export class FabricTokenizer extends TSQL.Tokenizer {
  public static ORIGINAL_KEYWORDS: Record<string, TokenType> = {
    ...TSQL.Tokenizer.ORIGINAL_KEYWORDS,
    TIMESTAMP: TokenType.TIMESTAMP,
    UTINYINT: TokenType.UTINYINT,
  };
}

export class FabricParser extends TSQL.Parser {
  public parseCreate (): CreateExpr | CommandExpr {
    const create = super.parseCreate();

    if (create instanceof CreateExpr) {
      if (create.args.kind === CreateExprKind.TABLE && create.args.this instanceof SchemaExpr) {
        for (const columnDef of create.args.this.args.expressions || []) {
          if (columnDef instanceof ColumnDefExpr) {
            const columnType = columnDef.args.kind;
            if (
              columnType instanceof DataTypeExpr
              && [DataTypeExprKind.VARCHAR, DataTypeExprKind.CHAR].includes(columnType.args.this as DataTypeExprKind)
              && (!columnType.args.expressions || columnType.args.expressions.length === 0)
            ) {
              columnType.setArgKey('expressions', [literal(1)]);
            }
          }
        }
      }
    }

    return create;
  }
}

export class FabricGenerator extends TSQL.Generator {
  public static TYPE_MAPPING: Map<DataTypeExprKind, string> = (() => {
    const mapping = new Map(TSQL.Generator.TYPE_MAPPING);
    mapping.set(DataTypeExprKind.DATETIME, 'DATETIME2');
    mapping.set(DataTypeExprKind.DECIMAL, 'DECIMAL');
    mapping.set(DataTypeExprKind.IMAGE, 'VARBINARY');
    mapping.set(DataTypeExprKind.INT, 'INT');
    mapping.set(DataTypeExprKind.JSON, 'VARCHAR');
    mapping.set(DataTypeExprKind.MONEY, 'DECIMAL');
    mapping.set(DataTypeExprKind.NCHAR, 'CHAR');
    mapping.set(DataTypeExprKind.NVARCHAR, 'VARCHAR');
    mapping.set(DataTypeExprKind.ROWVERSION, 'ROWVERSION');
    mapping.set(DataTypeExprKind.SMALLDATETIME, 'DATETIME2');
    mapping.set(DataTypeExprKind.SMALLMONEY, 'DECIMAL');
    mapping.set(DataTypeExprKind.TIMESTAMP, 'DATETIME2');
    mapping.set(DataTypeExprKind.TIMESTAMPNTZ, 'DATETIME2');
    mapping.set(DataTypeExprKind.TIMESTAMPTZ, 'DATETIME2');
    mapping.set(DataTypeExprKind.TINYINT, 'SMALLINT');
    mapping.set(DataTypeExprKind.UTINYINT, 'SMALLINT');
    mapping.set(DataTypeExprKind.UUID, 'UNIQUEIDENTIFIER');
    mapping.set(DataTypeExprKind.XML, 'VARCHAR');
    return mapping;
  })();

  public static ORIGINAL_TRANSFORMS: Map<typeof Expression, (self: Generator, e: Expression) => string> = (() => {
    const m = new Map<typeof Expression, (self: Generator, e: Expression) => string>(TSQL.Generator.ORIGINAL_TRANSFORMS);
    m.set(CreateExpr, preprocess([addDefaultPrecisionToVarchar]));
    return m;
  })();

  public dataTypeSql (expression: DataTypeExpr): string {
    if (
      expression.isType(DataTypeExpr.TEMPORAL_TYPES)
      && expression.args.this !== DataTypeExprKind.DATE
    ) {
      expression = capDataTypePrecision(expression);
    }

    return super.dataTypeSql(expression);
  }

  public castSql (expression: CastExpr, options: { safePrefix?: string } = {}): string {
    const { safePrefix } = options;
    if (expression.isType(DataTypeExprKind.TIMESTAMPTZ)) {
      const atTimeZone = expression.findAncestor<AtTimeZoneExpr | SelectExpr>(AtTimeZoneExpr, SelectExpr);

      if (!(atTimeZone instanceof AtTimeZoneExpr)) {
        return super.castSql(expression, { safePrefix });
      }

      const cappedDataType = capDataTypePrecision(expression.args.to as DataTypeExpr, 6);
      const precisionParam = cappedDataType.find(DataTypeParamExpr);
      const precisionValue = (precisionParam && precisionParam.args.this instanceof LiteralExpr && precisionParam.args.this.isNumber)
        ? Number(precisionParam.args.this.toString())
        : 6;

      const datetimeoffset = `CAST(${this.sql(expression, 'this')} AS DATETIMEOFFSET(${precisionValue}))`;

      return datetimeoffset;
    }

    return super.castSql(expression, { safePrefix });
  }

  public atTimeZoneSql (expression: AtTimeZoneExpr): string {
    const timestamptzCast = expression.find(CastExpr);
    if (timestamptzCast && timestamptzCast.isType(DataTypeExprKind.TIMESTAMPTZ)) {
      const dataType = timestamptzCast.args.to as DataTypeExpr;
      const cappedDataType = capDataTypePrecision(dataType, 6);
      const precisionParam = cappedDataType.find(DataTypeParamExpr);
      const precision = (precisionParam && precisionParam.args.this instanceof LiteralExpr && precisionParam.args.this.isNumber)
        ? Number(precisionParam.args.this.toString())
        : 6;

      const atTimeZoneSqlResult = super.atTimeZoneSql(expression);

      return `CAST(${atTimeZoneSqlResult} AS DATETIME2(${precision}))`;
    }

    return super.atTimeZoneSql(expression);
  }

  public unixToTimeSql (expression: UnixToTimeExpr): string {
    const scale = expression.args.scale;
    const timestamp = expression.args.this;

    if (scale !== undefined && scale !== UnixToTimeExpr.SECONDS) {
      this.unsupported(`UnixToTime scale ${scale} is not supported by Fabric`);
      return '';
    }

    if (!timestamp) return '';
    const microseconds = timestamp.mul(literal(1e6));
    const rounded = func('round', microseconds, literal(0));
    const roundedMsAsBigint = cast(rounded, DataTypeExprKind.BIGINT);

    const epochStart = cast('\'1970-01-01\'', 'datetime2(6)');

    const dateadd = new DateAddExpr({
      this: epochStart,
      expression: roundedMsAsBigint,
      unit: literal('MICROSECONDS'),
    });
    return this.sql(dateadd);
  }
}

export class Fabric extends TSQL {
  public static NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE;

  public static Tokenizer = FabricTokenizer;
  public static Parser = FabricParser;
  public static Generator = FabricGenerator;
}

Dialect.register(Dialects.FABRIC, Fabric);
