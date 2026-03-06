import type { Generator } from '../generator';
import { TokenType } from '../tokens';
import type { UnnestExpr } from '../expressions';
import {
  Expression,
  BetweenExpr, GteExpr, LteExpr, AndExpr,
  LiteralExpr, DateDiffExpr, FlattenExpr, RegexpLikeExpr,
  UniqueKeyPropertyExpr, RollupPropertyExpr, RefreshTriggerPropertyExpr,
  RollupIndexExpr, PropertiesExpr, CreateExpr, CreateExprKind, SchemaExpr, TableAliasExpr, PartitionedByPropertyExpr,
  PartitionByRangePropertyExpr, PartitionByRangePropertyDynamicExpr,
  PropertyExpr, PrimaryKeyExpr, EnginePropertyExpr, ColumnExpr,
  IdentifierExpr, DeleteExpr, ArrayExpr, ArrayAggExpr,
  ArrayFilterExpr, ArrayToStringExpr, ApproxDistinctExpr,
  StrToUnixExpr, TimestampTruncExpr,
  TimeStrToDateExpr, UnixToStrExpr, UnixToTimeExpr, DateTruncExpr,
  DataTypeExprKind, PropertiesLocation,
  JsonExtractScalarExpr, JsonExtractExpr, CurrentVersionExpr,
  StDistanceExpr,
  SchemaCommentPropertyExpr,
  toIdentifier,
} from '../expressions';
import { seqGet } from '../helper';
import { narrowInstanceOf } from '../port_internals';
import { preprocess } from '../transforms';
import {
  renameFunc, arrowJsonExtractSql, buildTimestampTrunc, unitToStr,
  approxCountDistinctSql, inlineArraySql, propertySql,
  Dialect, Dialects,
} from './dialect';
import {
  MySQL,
} from './mysql';

/**
 * StarRocks doesn't support BETWEEN in DELETE statements.
 * This transforms BETWEEN expressions into explicit GTE and LTE comparisons.
 * Reference: https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/DELETE/#parameters
 */
export function eliminateBetweenInDelete (expression: Expression): Expression {
  const where = expression.getArgKey('where');

  if (where instanceof Expression) {
    for (const between of where.findAll(BetweenExpr)) {
      const low = between.args.low;
      const high = between.args.high;

      between.replace(
        new AndExpr({
          this: new GteExpr({
            this: between.args.this?.copy(),
            expression: low,
          }),
          expression: new LteExpr({
            this: between.args.this?.copy(),
            expression: high,
          }),
        }),
      );
    }
  }

  return expression;
}

/**
 * StarRocks ST_Distance_Sphere expects individual coordinates rather than point objects.
 * Reference: https://docs.starrocks.io/docs/sql-reference/sql-functions/spatial-functions/st_distance_sphere/
 */
export function stDistanceSphere (self: Generator, expression: StDistanceExpr): string {
  const point1 = expression.args.this;
  const point2 = expression.args.expression;

  const point1X = self.func('ST_X', [point1]);
  const point1Y = self.func('ST_Y', [point1]);
  const point2X = self.func('ST_X', [point2]);
  const point2Y = self.func('ST_Y', [point2]);

  return self.func('ST_Distance_Sphere', [
    point1X,
    point1Y,
    point2X,
    point2Y,
  ]);
}

class StarRocksTokenizer extends MySQL.Tokenizer {
  public static ORIGINAL_KEYWORDS: Record<string, TokenType> = {
    ...MySQL.Tokenizer.ORIGINAL_KEYWORDS,
    LARGEINT: TokenType.INT128,
  };
};

class StarRocksParser extends MySQL.Parser {
  static #FUNCTIONS: undefined = undefined;
  static get FUNCTIONS () {
    return StarRocksParser.#FUNCTIONS ??= {
      ...MySQL.Parser.FUNCTIONS,
      DATE_TRUNC: buildTimestampTrunc,
      DATEDIFF: (args: Expression[]): DateDiffExpr =>
        new DateDiffExpr({
          this: seqGet(args, 0),
          expression: seqGet(args, 1),
          unit: new LiteralExpr({
            this: 'DAY',
            isString: true,
          }),
        }),
      DATE_DIFF: (args: Expression[]): DateDiffExpr =>
        new DateDiffExpr({
          this: seqGet(args, 1),
          expression: seqGet(args, 2),
          unit: seqGet(args, 0),
        }),
      ARRAY_FLATTEN: FlattenExpr.fromArgList,
      REGEXP: RegexpLikeExpr.fromArgList,
    };
  }

  static #PROPERTY_PARSERS: undefined = undefined;
  static get PROPERTY_PARSERS () {
    return StarRocksParser.#PROPERTY_PARSERS ??= {
      ...MySQL.Parser.PROPERTY_PARSERS,
      PROPERTIES: (self: StarRocksParser): Expression[] => self.parseWrappedProperties(),
      UNIQUE: (self: StarRocksParser): Expression => self.parseCompositeKeyProperty(UniqueKeyPropertyExpr),
      ROLLUP: (self: StarRocksParser): RollupPropertyExpr => self.parseRollupProperty(),
      REFRESH: (self: StarRocksParser): Expression => self.parseRefreshProperty(),
    };
  }

  /**
     * ROLLUP (rollup_name (col1, col2) [FROM from_index] [PROPERTIES (...)], ...)
     */
  protected parseRollupProperty (): RollupPropertyExpr {
    const parseRollupIndex = (): RollupIndexExpr => {
      return this.expression(RollupIndexExpr, {
        this: this.parseIdVar(),
        expressions: this.parseWrappedIdVars(),
        fromIndex: this.matchTextSeq(['FROM']) ? this.parseIdVar() : undefined,
        properties: this.matchTextSeq(['PROPERTIES'])
          ? this.expression(PropertiesExpr, { expressions: this.parseWrappedProperties() })
          : undefined,
      });
    };

    return this.expression(RollupPropertyExpr, {
      expressions: this.parseWrappedCsv(parseRollupIndex),
    });
  }

  public parseCreate (): CreateExpr {
    const create = super.parseCreate();

    // StarRocks' primary key is defined outside of the schema in properties,
    // so we move it into the schema expressions for standard AST representation.
    if (create instanceof CreateExpr && create.args.this instanceof SchemaExpr) {
      const props = create.args.properties;
      if (props) {
        const primaryKey = props.find(PrimaryKeyExpr);
        if (primaryKey) {
          create.args.this.append('expressions', primaryKey.pop());
        }
      }
    }

    return create as CreateExpr;
  }

  public parseUnnest (options: { withAlias?: boolean } = {}): UnnestExpr | undefined {
    const unnest = super.parseUnnest(options);

    if (unnest) {
      let aliasObj = unnest.args.alias;

      if (!aliasObj) {
        // StarRocks defaults to naming the table alias as "unnest"
        aliasObj = new TableAliasExpr({
          this: toIdentifier('unnest'),
          columns: [toIdentifier('unnest')],
        });
        unnest.setArgKey('alias', aliasObj);
      } else if (!narrowInstanceOf(aliasObj, TableAliasExpr)?.args.columns) {
        // StarRocks defaults to naming the UNNEST column as "unnest" if unspecified
        aliasObj.setArgKey('columns', [toIdentifier('unnest')]);
      }
    }

    return unnest;
  }

  public parsePartitionedBy (): PartitionedByPropertyExpr {
    return this.expression(PartitionedByPropertyExpr, {
      this: new SchemaExpr({
        expressions: this.parseWrappedCsv(this.parseAssignment.bind(this), { optional: true }),
      }),
    });
  }

  protected parsePartitionProperty (): Expression | Expression[] | undefined {
    const expr = super.parsePartitionProperty();

    if (!expr) {
      return this.parsePartitionedBy() ?? undefined;
    }

    if (expr instanceof PropertyExpr) {
      return expr;
    }

    this.matchLParen();

    let createExpressions: Expression[] | undefined = undefined;
    if (this.matchTextSeq(['START'], { advance: false })) {
      createExpressions = this.parseCsv(this.parsePartitioningGranularityDynamic.bind(this));
    }

    this.matchRParen();

    return this.expression(PartitionByRangePropertyExpr, {
      partitionExpressions: expr,
      createExpressions: createExpressions,
    });
  }

  protected parsePartitioningGranularityDynamic (): PartitionByRangePropertyDynamicExpr {
    this.matchTextSeq(['START']);
    const start = this.parseWrapped(this.parseString.bind(this));
    this.matchTextSeq(['END']);
    const end = this.parseWrapped(this.parseString.bind(this));
    this.matchTextSeq(['EVERY']);
    const every = this.parseWrapped(() => this.parseInterval() || this.parseNumber());

    return this.expression(PartitionByRangePropertyDynamicExpr, {
      start: start,
      end: end,
      every: every,
    });
  }

  protected parseRefreshProperty (): RefreshTriggerPropertyExpr {
    const method = this.matchTexts(['DEFERRED', 'IMMEDIATE']) ? this.prev?.text?.toUpperCase() ?? undefined : undefined;
    const kind = this.matchTexts(['ASYNC', 'MANUAL']) ? this.prev?.text?.toUpperCase() ?? undefined : undefined;
    const start = this.matchTextSeq(['START']) ? this.parseWrapped(this.parseString.bind(this)) : undefined;

    let every: Expression | undefined = undefined;
    let unit: Expression | undefined = undefined;

    if (this.matchTextSeq(['EVERY'])) {
      this.matchLParen();
      this.matchTextSeq(['INTERVAL']);
      every = this.parseNumber() ?? undefined;
      unit = this.parseVar({ anyToken: true }) ?? undefined;
      this.matchRParen();
    }

    return this.expression(RefreshTriggerPropertyExpr, {
      method: method,
      kind: kind,
      starts: start,
      every: every,
      unit: unit,
    });
  }
};

class StarRocksGenerator extends MySQL.Generator {
  public static EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE: boolean = false;
  public static JSON_TYPE_REQUIRED_FOR_EXTRACTION: boolean = false;
  public static VARCHAR_REQUIRES_SIZE: boolean = false;
  public static PARSE_JSON_NAME: string | undefined = 'PARSE_JSON';
  public static WITH_PROPERTIES_PREFIX: string = 'PROPERTIES';
  public static UPDATE_STATEMENT_SUPPORTS_FROM: boolean = true;
  public static INSERT_OVERWRITE: string = ' OVERWRITE';

  // StarRocks doesn't support "IS TRUE/FALSE" syntax.
  public static IS_BOOL_ALLOWED: boolean = false;
  // StarRocks doesn't support renaming a table with a database.
  public static RENAME_TABLE_WITH_DB: boolean = false;

  public static CAST_MAPPING: Record<string, string> = {};

  public static TYPE_MAPPING: Map<DataTypeExprKind | string, string> = (() => {
    const m = new Map(MySQL.Generator.TYPE_MAPPING);
    m.set(DataTypeExprKind.INT128, 'LARGEINT');
    m.set(DataTypeExprKind.TEXT, 'STRING');
    m.set(DataTypeExprKind.TIMESTAMP, 'DATETIME');
    m.set(DataTypeExprKind.TIMESTAMPTZ, 'DATETIME');
    return m;
  })();

  public static PROPERTIES_LOCATION: Map<typeof Expression, PropertiesLocation> = (() => {
    const m = new Map(MySQL.Generator.PROPERTIES_LOCATION);
    m.set(PrimaryKeyExpr, PropertiesLocation.POST_SCHEMA);
    m.set(UniqueKeyPropertyExpr, PropertiesLocation.POST_SCHEMA);
    m.set(RollupPropertyExpr, PropertiesLocation.POST_SCHEMA);
    m.set(PartitionedByPropertyExpr, PropertiesLocation.POST_SCHEMA);
    return m;
  })();

  public static ORIGINAL_TRANSFORMS = (() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const m = new Map<typeof Expression, (self: Generator, e: any) => string>(MySQL.Generator.TRANSFORMS);
    m.set(ArrayExpr, inlineArraySql);
    m.set(ArrayAggExpr, renameFunc('ARRAY_AGG'));
    m.set(ArrayFilterExpr, renameFunc('ARRAY_FILTER'));
    m.set(ArrayToStringExpr, renameFunc('ARRAY_JOIN'));
    m.set(ApproxDistinctExpr, approxCountDistinctSql);
    m.set(CurrentVersionExpr, (): string => 'CURRENT_VERSION()');
    m.set(
      DateDiffExpr,
      (self: Generator, e: DateDiffExpr): string =>
        self.func('DATE_DIFF', [
          unitToStr(e),
          e.args.this,
          e.args.expression,
        ]),
    );
    m.set(DeleteExpr, preprocess([eliminateBetweenInDelete]));
    m.set(FlattenExpr, renameFunc('ARRAY_FLATTEN'));
    m.set(JsonExtractScalarExpr, arrowJsonExtractSql);
    m.set(JsonExtractExpr, arrowJsonExtractSql);
    m.set(PropertyExpr, propertySql);
    m.set(RegexpLikeExpr, renameFunc('REGEXP'));
    m.set(SchemaCommentPropertyExpr, (self: Generator, e: Expression): string => self.nakedProperty(e));
    m.set(StDistanceExpr, stDistanceSphere);
    m.set(
      StrToUnixExpr,
      (self: Generator, e: StrToUnixExpr): string =>
        self.func('UNIX_TIMESTAMP', [e.args.this, self.formatTime(e)]),
    );
    m.set(
      TimestampTruncExpr,
      (self: Generator, e: TimestampTruncExpr): string =>
        self.func('DATE_TRUNC', [unitToStr(e), e.args.this]),
    );
    m.set(TimeStrToDateExpr, renameFunc('TO_DATE'));
    m.set(
      UnixToStrExpr,
      (self: Generator, e: UnixToStrExpr): string =>
        self.func('FROM_UNIXTIME', [e.args.this, self.formatTime(e)]),
    );
    m.set(UnixToTimeExpr, renameFunc('FROM_UNIXTIME'));
    // StarRocks uses DATE_TRUNC instead of the MySQL simulation, so we remove the MySQL transform.
    m.delete(DateTruncExpr);
    return m;
  })();

  /**
     * Comprehensive list of StarRocks reserved keywords.
     * Reference: https://docs.starrocks.io/docs/sql-reference/sql-statements/keywords/#reserved-keywords
     */
  public static RESERVED_KEYWORDS: Set<string> = new Set([
    'add',
    'all',
    'alter',
    'analyze',
    'and',
    'array',
    'as',
    'asc',
    'between',
    'bigint',
    'bitmap',
    'both',
    'by',
    'case',
    'char',
    'character',
    'check',
    'collate',
    'column',
    'compaction',
    'convert',
    'create',
    'cross',
    'cube',
    'current_date',
    'current_role',
    'current_time',
    'current_timestamp',
    'current_user',
    'database',
    'databases',
    'decimal',
    'decimalv2',
    'decimal32',
    'decimal64',
    'decimal128',
    'default',
    'deferred',
    'delete',
    'dense_rank',
    'desc',
    'describe',
    'distinct',
    'double',
    'drop',
    'dual',
    'else',
    'except',
    'exists',
    'explain',
    'false',
    'first_value',
    'float',
    'for',
    'force',
    'from',
    'full',
    'function',
    'grant',
    'group',
    'grouping',
    'grouping_id',
    'groups',
    'having',
    'hll',
    'host',
    'if',
    'ignore',
    'immediate',
    'in',
    'index',
    'infile',
    'inner',
    'insert',
    'int',
    'integer',
    'intersect',
    'into',
    'is',
    'join',
    'json',
    'key',
    'keys',
    'kill',
    'lag',
    'largeint',
    'last_value',
    'lateral',
    'lead',
    'left',
    'like',
    'limit',
    'load',
    'localtime',
    'localtimestamp',
    'maxvalue',
    'minus',
    'mod',
    'not',
    'ntile',
    'undefined',
    'on',
    'or',
    'order',
    'outer',
    'outfile',
    'over',
    'partition',
    'percentile',
    'primary',
    'procedure',
    'qualify',
    'range',
    'rank',
    'read',
    'regexp',
    'release',
    'rename',
    'replace',
    'revoke',
    'right',
    'rlike',
    'row',
    'row_number',
    'rows',
    'schema',
    'schemas',
    'select',
    'set',
    'set_var',
    'show',
    'smallint',
    'system',
    'table',
    'terminated',
    'text',
    'then',
    'tinyint',
    'to',
    'true',
    'union',
    'unique',
    'unsigned',
    'update',
    'use',
    'using',
    'values',
    'varchar',
    'when',
    'where',
    'with',
  ]);

  /**
     * Overrides table creation to move Primary Keys from the schema into properties.
     * In StarRocks, PKs are defined as part of the table properties (often after the ENGINE).
     */
  public createSql (expression: CreateExpr): string {
    const schema = expression.args.this;

    if (schema instanceof SchemaExpr) {
      const primaryKey = schema.find(PrimaryKeyExpr);

      if (primaryKey) {
        let props = expression.args.properties;

        if (!props) {
          props = new PropertiesExpr({ expressions: [] });
          expression.setArgKey('properties', props);
        }

        // StarRocks typically wants the PK property right after the ENGINE property
        const engine = props.find(EnginePropertyExpr);
        const engineIndex = engine?.index !== undefined ? engine.index : -1;

        // Move the Primary Key from the schema to the properties list at the correct index
        props.setArgKey('expressions', [primaryKey.pop()], engineIndex + 1, { overwrite: false });
      }
    }

    return super.createSql(expression);
  }

  /**
     * Generates the PARTITION BY clause.
     * Handles specific parenthesis requirements for Materialized Views.
     */
  public partitionedByPropertySql (expression: PartitionedByPropertyExpr): string {
    const thisExpr = expression.args.this;

    if (thisExpr instanceof SchemaExpr) {
      // StarRocks requires outer parentheses for MVs or simple column lists.
      const create = expression.findAncestor(CreateExpr);

      let sqlResult = this.expressions(thisExpr, { flat: true });
      const isSimpleColumnList = thisExpr.args.expressions?.every(
        (col) => col instanceof ColumnExpr || col instanceof IdentifierExpr,
      );

      if ((create && create.args.kind === CreateExprKind.VIEW) || isSimpleColumnList) {
        sqlResult = `(${sqlResult})`;
      }

      return `PARTITION BY ${sqlResult}`;
    }

    return `PARTITION BY ${this.sql(thisExpr)}`;
  }

  /**
     * Generates StarRocks ORDER BY clause for clustering.
     */
  public clusterSql (expression: Expression): string {
    const expressions = this.expressions(expression, { flat: true });
    return expressions ? `ORDER BY (${expressions})` : '';
  }

  /**
     * Generates the REFRESH clause for materialized views.
     */
  public refreshTriggerPropertySql (expression: RefreshTriggerPropertyExpr): string {
    let method = this.sql(expression, 'method');
    method = method ? ` ${method}` : '';

    let kind = this.sql(expression, 'kind');
    kind = kind ? ` ${kind}` : '';

    let starts = this.sql(expression, 'starts');
    starts = starts ? ` START (${starts})` : '';

    const everyValue = this.sql(expression, 'every');
    const unitValue = this.sql(expression, 'unit');
    const every = (everyValue && unitValue) ? ` EVERY (INTERVAL ${everyValue} ${unitValue})` : '';

    return `REFRESH${method}${kind}${starts}${every}`;
  }
}

export class StarRocks extends MySQL {
  public static STRICT_JSON_PATH_SYNTAX = false;
  public static INDEX_OFFSET = 1;

  public static Tokenizer = StarRocksTokenizer;
  public static Parser = StarRocksParser;
  public static Generator = StarRocksGenerator;
}
Dialect.register(Dialects.STARROCKS, StarRocks);
