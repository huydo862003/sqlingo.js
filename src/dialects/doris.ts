import { cache } from '../port_internals';
import type {
  Expression, TableExpr,
} from '../expressions';
import {
  ApproxDistinctExpr,
  ArgMaxExpr,
  ArgMinExpr,
  ArrayAggExpr,
  ArrayToStringExpr,
  CurrentDateExpr,
  CurrentTimestampExpr,
  DateTruncExpr,
  GroupConcatExpr,
  LeadExpr,
  MapExpr,
  RegexpSplitExpr,
  SchemaCommentPropertyExpr,
  SplitExpr,
  TsOrDsAddExpr,
  BuildPropertyExpr,
  PartitionByRangePropertyDynamicExpr,
  PartitionExpr,
  PartitionRangeExpr,
  RefreshTriggerPropertyExpr,
  LagExpr,
  TimestampTruncExpr,
  LiteralExpr,
  null_,
  ArrayUniqueAggExpr,
  EuclideanDistanceExpr,
  AddMonthsExpr,
  RegexpLikeExpr,
  TsOrDsToDateExpr,
  UniqueKeyPropertyExpr,
  PropertyExpr,
  PartitionByRangePropertyExpr,
  IntervalExpr,
  DataTypeExprKind,
  PropertiesLocation,
  PartitionedByPropertyExpr,
  UnixToStrExpr,
  TimeToUnixExpr,
  UnixToTimeExpr,
  TimeStrToDateExpr,
  StringToArrayExpr,
  StrToUnixExpr,
  JsonExtractScalarExpr,
  SelectExpr,
  DeleteExpr,
  UpdateExpr,
  SchemaExpr,
  CreateExpr,
  MaterializedPropertyExpr,
} from '../expressions';
import { seqGet } from '../helper';
import type { Parser } from '../parser';
import { TokenType } from '../tokens';
import type { Generator } from '../generator';
import { MySQL } from './mysql';
import {
  approxCountDistinctSql,
  Dialect, Dialects,
  renameFunc,
  timeFormat,
  unitToStr,
} from './dialect';

function lagLeadSql (this: Generator, expression: LagExpr | LeadExpr): string {
  return this.func(
    expression instanceof LagExpr ? 'LAG' : 'LEAD',
    [
      expression.args.this,
      expression.args.offset ?? LiteralExpr.number(1),
      expression.args.default ?? null_(),
    ],
  );
}

/**
 * Accept both DATE_TRUNC(datetime, unit) and DATE_TRUNC(unit, datetime)
 */
function buildDateTrunc (args: Expression[]): TimestampTruncExpr {
  const a0 = seqGet(args, 0);
  const a1 = seqGet(args, 1);

  const isUnitLike = (e: Expression | undefined): boolean => {
    if (!(e instanceof LiteralExpr && e.isString)) {
      return false;
    }
    const text = e.args.this;
    // Doris units typically don't contain digits (e.g., 'year', 'month')
    return !/\d/.test(text ?? '');
  };

  // Determine which argument is the unit vs the timestamp
  const [unit, thisNode] = isUnitLike(a0) ? [a0, a1] : [a1, a0];

  return new TimestampTruncExpr({
    this: thisNode,
    unit: unit,
  });
}

export class DorisTokenizer extends MySQL.Tokenizer {}

class DorisParser extends MySQL.Parser {
  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> {
    return {
      ...MySQL.Parser.FUNCTIONS,
      COLLECT_SET: ArrayUniqueAggExpr.fromArgList,
      DATE_TRUNC: buildDateTrunc,
      L2_DISTANCE: EuclideanDistanceExpr.fromArgList,
      MONTHS_ADD: AddMonthsExpr.fromArgList,
      REGEXP: RegexpLikeExpr.fromArgList,
      TO_DATE: TsOrDsToDateExpr.fromArgList,
    };
  }

  @cache
  static get FUNCTION_PARSERS (): Partial<Record<string, (this: Parser) => Expression | undefined>> {
    return { ...MySQL.Parser.FUNCTION_PARSERS };
  }

  static {
    delete DorisParser.FUNCTION_PARSERS['GROUP_CONCAT'];
  }

  @cache
  static get NO_PAREN_FUNCTIONS (): Partial<Record<TokenType, typeof Expression>> {
    return { ...MySQL.Parser.NO_PAREN_FUNCTIONS };
  }

  static {
    delete DorisParser.NO_PAREN_FUNCTIONS[TokenType.CURRENT_DATE];
  }

  @cache
  static get PROPERTY_PARSERS (): Record<string, (this: Parser, ...args: unknown[]) => Expression | Expression[] | undefined> {
    return {
      ...MySQL.Parser.PROPERTY_PARSERS,
      PROPERTIES: function (this: Parser) {
        return this.parseWrappedProperties();
      },
      UNIQUE: function (this: Parser) {
        return this.parseCompositeKeyProperty(UniqueKeyPropertyExpr);
      },
      KEY: function (this: Parser) {
        return this.parseCompositeKeyProperty(UniqueKeyPropertyExpr);
      },
      BUILD: function (this: Parser) {
        return (this as DorisParser).parseBuildProperty();
      },
      REFRESH: function (this: Parser) {
        return (this as DorisParser).parseRefreshProperty();
      },
    };
  }

  parsePartitionProperty (): Expression | Expression[] | undefined {
    const expr = super.parsePartitionProperty();

    if (!expr) {
      return this.parsePartitionedBy();
    }

    if (expr instanceof PropertyExpr) {
      return expr;
    }

    this.matchLParen();

    let createExpressions: Expression[] | undefined = undefined;
    if (this.matchTextSeq('FROM', { advance: false })) {
      createExpressions = this.parseCsv(() => this.parsePartitioningGranularityDynamic());
    }

    this.matchRParen();

    return this.expression(PartitionByRangePropertyExpr, {
      partitionExpressions: expr,
      createExpressions: createExpressions,
    });
  }

  parsePartitioningGranularityDynamic (): PartitionByRangePropertyDynamicExpr {
    this.matchTextSeq('FROM');
    const start = this.parseWrapped(() => this.parseString());
    this.matchTextSeq('TO');
    const end = this.parseWrapped(() => this.parseString());
    this.matchTextSeq('INTERVAL');
    const number = this.parseNumber();
    const unit = this.parseVar({ anyToken: true });

    const every = this.expression(IntervalExpr, {
      this: number,
      unit: unit,
    });

    return this.expression(PartitionByRangePropertyDynamicExpr, {
      start: start,
      end: end,
      every: every,
    });
  }

  parsePartitionRangeValue (): PartitionExpr {
    const expr = super.parsePartitionRangeValue();

    if (expr instanceof PartitionExpr) {
      return expr;
    }

    this.matchTextSeq('VALUES');
    const name = expr;

    // Doris-specific bracket syntax: VALUES [(...), (...))
    this.match(TokenType.L_BRACKET);
    const values = this.parseWrappedCsv(() => this.parseExpression());

    this.match(TokenType.R_BRACKET);
    this.match(TokenType.R_PAREN);

    const partRange = this.expression(PartitionRangeExpr, {
      this: name,
      expressions: values,
    });

    return this.expression(PartitionExpr, { expressions: [partRange] });
  }

  parseBuildProperty (): BuildPropertyExpr {
    return this.expression(BuildPropertyExpr, {
      this: this.parseVar({ upper: true }),
    });
  }

  parseRefreshProperty (): RefreshTriggerPropertyExpr {
    const method = this.parseVar({ upper: true });

    this.match(TokenType.ON);

    const kind = this.matchTexts([
      'MANUAL',
      'COMMIT',
      'SCHEDULE',
    ])
      ? this.prev?.text.toUpperCase()
      : undefined;
    const every = this.matchTextSeq('EVERY') ? this.parseNumber() : undefined;
    const unit = every ? this.parseVar({ anyToken: true }) : undefined;
    const starts = this.matchTextSeq('STARTS') ? this.parseString() : undefined;

    return this.expression(RefreshTriggerPropertyExpr, {
      method: method,
      kind: kind,
      every: every,
      unit: unit,
      starts: starts,
    });
  }
}

class DorisGenerator extends MySQL.Generator {
  static LAST_DAY_SUPPORTS_DATE_PART = false;
  static VARCHAR_REQUIRES_SIZE = false;
  static WITH_PROPERTIES_PREFIX = 'PROPERTIES';
  static RENAME_TABLE_WITH_DB = false;
  static UPDATE_STATEMENT_SUPPORTS_FROM = true;

  @cache
  static get TYPE_MAPPING () {
    return {
      ...MySQL.Generator.TYPE_MAPPING,
      [DataTypeExprKind.TEXT]: 'STRING',
      [DataTypeExprKind.TIMESTAMP]: 'DATETIME',
      [DataTypeExprKind.TIMESTAMPTZ]: 'DATETIME',
    };
  }

  @cache
  static get PROPERTIES_LOCATION () {
    return new Map<typeof Expression, PropertiesLocation>([
      ...MySQL.Generator.PROPERTIES_LOCATION,
      [UniqueKeyPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [PartitionedByPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [BuildPropertyExpr, PropertiesLocation.POST_SCHEMA],
    ]);
  }

  @cache
  static get CAST_MAPPING () {
    return {};
  }

  @cache
  static get TIMESTAMP_FUNC_TYPES () {
    return new Set<DataTypeExprKind>();
  }

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (this: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (this: Generator, e: any) => string>([
      ...MySQL.Generator.TRANSFORMS,
      [AddMonthsExpr, renameFunc('MONTHS_ADD')],
      [ApproxDistinctExpr, approxCountDistinctSql],
      [ArgMaxExpr, renameFunc('MAX_BY')],
      [ArgMinExpr, renameFunc('MIN_BY')],
      [ArrayAggExpr, renameFunc('COLLECT_LIST')],
      [ArrayToStringExpr, renameFunc('ARRAY_JOIN')],
      [ArrayUniqueAggExpr, renameFunc('COLLECT_SET')],
      [
        CurrentDateExpr,
        function (this: Generator) {
          return this.func('CURRENT_DATE', []);
        },
      ],
      [
        CurrentTimestampExpr,
        function (this: Generator) {
          return this.func('NOW', []);
        },
      ],
      [
        DateTruncExpr,
        function (this: Generator, e: DateTruncExpr) {
          return this.func('DATE_TRUNC', [e.args.this, unitToStr(e)]);
        },
      ],
      [EuclideanDistanceExpr, renameFunc('L2_DISTANCE')],
      [
        GroupConcatExpr,
        function (this: Generator, e: GroupConcatExpr) {
          return this.func('GROUP_CONCAT', [e.args.this, e.args.separator ?? LiteralExpr.string(',')]);
        },
      ],
      [
        JsonExtractScalarExpr,
        function (this: Generator, e: JsonExtractScalarExpr) {
          return this.func('JSON_EXTRACT', [e.args.this, e.args.expression]);
        },
      ],
      [LagExpr, lagLeadSql],
      [LeadExpr, lagLeadSql],
      [MapExpr, renameFunc('ARRAY_MAP')],
      [
        PropertyExpr,
        function (this: Generator, e: PropertyExpr) {
          return this.propertySql(e);
        },
      ],
      [RegexpLikeExpr, renameFunc('REGEXP')],
      [RegexpSplitExpr, renameFunc('SPLIT_BY_STRING')],
      [
        SchemaCommentPropertyExpr,
        function (this: Generator, e: SchemaCommentPropertyExpr) {
          return this.nakedProperty(e);
        },
      ],
      [SplitExpr, renameFunc('SPLIT_BY_STRING')],
      [StringToArrayExpr, renameFunc('SPLIT_BY_STRING')],
      [
        StrToUnixExpr,
        function (this: Generator, e: StrToUnixExpr) {
          return this.func('UNIX_TIMESTAMP', [e.args.this, this.formatTime(e)]);
        },
      ],
      [TimeStrToDateExpr, renameFunc('TO_DATE')],
      [
        TsOrDsAddExpr,
        function (this: Generator, e: TsOrDsAddExpr) {
          return this.func('DATE_ADD', [e.args.this, e.args.expression]);
        },
      ],
      [
        TsOrDsToDateExpr,
        function (this: Generator, e: TsOrDsToDateExpr) {
          return this.func('TO_DATE', [e.args.this]);
        },
      ],
      [TimeToUnixExpr, renameFunc('UNIX_TIMESTAMP')],
      [
        TimestampTruncExpr,
        function (this: Generator, e: TimestampTruncExpr) {
          return this.func('DATE_TRUNC', [e.args.this, unitToStr(e)]);
        },
      ],
      [
        UnixToStrExpr,
        function (this: Generator, e: UnixToStrExpr) {
          return this.func('FROM_UNIXTIME', [e.args.this, timeFormat('doris').call(this, e)]);
        },
      ],
      [UnixToTimeExpr, renameFunc('FROM_UNIXTIME')],
    ]);

    return transforms;
  }

  static RESERVED_KEYWORDS = new Set([
    'account_lock',
    'account_unlock',
    'add',
    'adddate',
    'admin',
    'after',
    'agg_state',
    'aggregate',
    'alias',
    'all',
    'alter',
    'analyze',
    'analyzed',
    'and',
    'anti',
    'append',
    'array',
    'array_range',
    'as',
    'asc',
    'at',
    'authors',
    'auto',
    'auto_increment',
    'backend',
    'backends',
    'backup',
    'begin',
    'belong',
    'between',
    'bigint',
    'bin',
    'binary',
    'binlog',
    'bitand',
    'bitmap',
    'bitmap_union',
    'bitor',
    'bitxor',
    'blob',
    'boolean',
    'brief',
    'broker',
    'buckets',
    'build',
    'builtin',
    'bulk',
    'by',
    'cached',
    'call',
    'cancel',
    'case',
    'cast',
    'catalog',
    'catalogs',
    'chain',
    'char',
    'character',
    'charset',
    'check',
    'clean',
    'cluster',
    'clusters',
    'collate',
    'collation',
    'collect',
    'column',
    'columns',
    'comment',
    'commit',
    'committed',
    'compact',
    'complete',
    'config',
    'connection',
    'connection_id',
    'consistent',
    'constraint',
    'constraints',
    'convert',
    'copy',
    'count',
    'create',
    'creation',
    'cron',
    'cross',
    'cube',
    'current',
    'current_catalog',
    'current_date',
    'current_time',
    'current_timestamp',
    'current_user',
    'data',
    'database',
    'databases',
    'date',
    'date_add',
    'date_ceil',
    'date_diff',
    'date_floor',
    'date_sub',
    'dateadd',
    'datediff',
    'datetime',
    'datetimev2',
    'datev2',
    'datetimev1',
    'datev1',
    'day',
    'days_add',
    'days_sub',
    'decimal',
    'decimalv2',
    'decimalv3',
    'decommission',
    'default',
    'deferred',
    'delete',
    'demand',
    'desc',
    'describe',
    'diagnose',
    'disk',
    'distinct',
    'distinctpc',
    'distinctpcsa',
    'distributed',
    'distribution',
    'div',
    'do',
    'doris_internal_table_id',
    'double',
    'drop',
    'dropp',
    'dual',
    'duplicate',
    'dynamic',
    'else',
    'enable',
    'encryptkey',
    'encryptkeys',
    'end',
    'ends',
    'engine',
    'engines',
    'enter',
    'errors',
    'events',
    'every',
    'except',
    'exclude',
    'execute',
    'exists',
    'expired',
    'explain',
    'export',
    'extended',
    'external',
    'extract',
    'failed_login_attempts',
    'false',
    'fast',
    'feature',
    'fields',
    'file',
    'filter',
    'first',
    'float',
    'follower',
    'following',
    'for',
    'foreign',
    'force',
    'format',
    'free',
    'from',
    'frontend',
    'frontends',
    'full',
    'function',
    'functions',
    'generic',
    'global',
    'grant',
    'grants',
    'graph',
    'group',
    'grouping',
    'groups',
    'hash',
    'having',
    'hdfs',
    'help',
    'histogram',
    'hll',
    'hll_union',
    'hostname',
    'hour',
    'hub',
    'identified',
    'if',
    'ignore',
    'immediate',
    'in',
    'incremental',
    'index',
    'indexes',
    'infile',
    'inner',
    'insert',
    'install',
    'int',
    'integer',
    'intermediate',
    'intersect',
    'interval',
    'into',
    'inverted',
    'ipv4',
    'ipv6',
    'is',
    'is_not_null_pred',
    'is_null_pred',
    'isnull',
    'isolation',
    'job',
    'jobs',
    'join',
    'json',
    'jsonb',
    'key',
    'keys',
    'kill',
    'label',
    'largeint',
    'last',
    'lateral',
    'ldap',
    'ldap_admin_password',
    'left',
    'less',
    'level',
    'like',
    'limit',
    'lines',
    'link',
    'list',
    'load',
    'local',
    'localtime',
    'localtimestamp',
    'location',
    'lock',
    'logical',
    'low_priority',
    'manual',
    'map',
    'match',
    'match_all',
    'match_any',
    'match_phrase',
    'match_phrase_edge',
    'match_phrase_prefix',
    'match_regexp',
    'materialized',
    'max',
    'maxvalue',
    'memo',
    'merge',
    'migrate',
    'migrations',
    'min',
    'minus',
    'minute',
    'modify',
    'month',
    'mtmv',
    'name',
    'names',
    'natural',
    'negative',
    'never',
    'next',
    'ngram_bf',
    'no',
    'non_nullable',
    'not',
    'null',
    'nulls',
    'observer',
    'of',
    'offset',
    'on',
    'only',
    'open',
    'optimized',
    'or',
    'order',
    'outer',
    'outfile',
    'over',
    'overwrite',
    'parameter',
    'parsed',
    'partition',
    'partitions',
    'password',
    'password_expire',
    'password_history',
    'password_lock_time',
    'password_reuse',
    'path',
    'pause',
    'percent',
    'period',
    'permissive',
    'physical',
    'plan',
    'process',
    'plugin',
    'plugins',
    'policy',
    'preceding',
    'prepare',
    'primary',
    'proc',
    'procedure',
    'processlist',
    'profile',
    'properties',
    'property',
    'quantile_state',
    'quantile_union',
    'query',
    'quota',
    'random',
    'range',
    'read',
    'real',
    'rebalance',
    'recover',
    'recycle',
    'refresh',
    'references',
    'regexp',
    'release',
    'rename',
    'repair',
    'repeatable',
    'replace',
    'replace_if_not_null',
    'replica',
    'repositories',
    'repository',
    'resource',
    'resources',
    'restore',
    'restrictive',
    'resume',
    'returns',
    'revoke',
    'rewritten',
    'right',
    'rlike',
    'role',
    'roles',
    'rollback',
    'rollup',
    'routine',
    'row',
    'rows',
    's3',
    'sample',
    'schedule',
    'scheduler',
    'schema',
    'schemas',
    'second',
    'select',
    'semi',
    'sequence',
    'serializable',
    'session',
    'set',
    'sets',
    'shape',
    'show',
    'signed',
    'skew',
    'smallint',
    'snapshot',
    'soname',
    'split',
    'sql_block_rule',
    'start',
    'starts',
    'stats',
    'status',
    'stop',
    'storage',
    'stream',
    'streaming',
    'string',
    'struct',
    'subdate',
    'sum',
    'superuser',
    'switch',
    'sync',
    'system',
    'table',
    'tables',
    'tablesample',
    'tablet',
    'tablets',
    'task',
    'tasks',
    'temporary',
    'terminated',
    'text',
    'than',
    'then',
    'time',
    'timestamp',
    'timestampadd',
    'timestampdiff',
    'tinyint',
    'to',
    'transaction',
    'trash',
    'tree',
    'triggers',
    'trim',
    'true',
    'truncate',
    'type',
    'type_cast',
    'types',
    'unbounded',
    'uncommitted',
    'uninstall',
    'union',
    'unique',
    'unlock',
    'unsigned',
    'update',
    'use',
    'user',
    'using',
    'value',
    'values',
    'varchar',
    'variables',
    'variant',
    'vault',
    'verbose',
    'version',
    'view',
    'warnings',
    'week',
    'when',
    'where',
    'whitelist',
    'with',
    'work',
    'workload',
    'write',
    'xor',
    'year',
  ]);

  uniqueKeyPropertySql (expression: UniqueKeyPropertyExpr, options: { prefix?: string } = {}): string {
    let { prefix = 'UNIQUE KEY' } = options;
    const createStmt = expression.findAncestor(CreateExpr);

    if (
      createStmt
      && createStmt.args.properties?.find(MaterializedPropertyExpr)
    ) {
      prefix = 'KEY';
    }

    return super.uniqueKeyPropertySql(expression, { prefix });
  }

  partitionRangeSql (expression: PartitionRangeExpr): string {
    const name = this.sql(expression, 'this');
    const values = expression.args.expressions || [];

    if (values.length !== 1) {
    // Multiple values: use VALUES [ ... )
      let valuesSql: string;

      // Check if we have a nested array structure (list of lists)
      if (0 < values.length && Array.isArray(values[0])) {
        valuesSql = (values as Expression[][]).map((inner) =>
          `(${inner.map((v) => this.sql(v)).join(', ')})`).join(', ');
      } else {
        valuesSql = values.map((v) => `(${this.sql(v as Expression)})`).join(', ');
      }

      return `PARTITION ${name} VALUES [${valuesSql})`;
    }

    return `PARTITION ${name} VALUES LESS THAN (${this.sql((values as Expression[])[0])})`;
  }

  partitionByRangePropertyDynamicSql (expression: PartitionByRangePropertyDynamicExpr): string {
  // Generates: FROM ("start") TO ("end") INTERVAL N UNIT
    const start = this.sql(expression, 'start');
    const end = this.sql(expression, 'end');
    const every = expression.args.every;

    let interval = '';
    if (every) {
      const number = this.sql(every, 'this');
      const unit = this.sql(every, 'unit');
      interval = `INTERVAL ${number} ${unit}`;
    }

    return `FROM (${start}) TO (${end}) ${interval}`.trim();
  }

  partitionedByPropertySql (expression: PartitionedByPropertyExpr): string {
    const thisNode = expression.args.this;
    if (thisNode instanceof SchemaExpr) {
      return `PARTITION BY (${this.expressions(thisNode, { flat: true })})`;
    }
    return `PARTITION BY (${this.sql(thisNode)})`;
  }

  tableSql (expression: TableExpr, options: { sep?: string } = {}): string {
  /** Override table_sql to avoid AS keyword in UPDATE and DELETE statements. */
    let { sep = ' AS ' } = options;
    const ancestor = expression.findAncestor<UpdateExpr | DeleteExpr | SelectExpr>(UpdateExpr, DeleteExpr, SelectExpr);

    if (ancestor && !(ancestor instanceof SelectExpr)) {
      sep = ' ';
    }

    return super.tableSql(expression, { sep });
  }
}

export class Doris extends MySQL {
  static DATE_FORMAT = '\'yyyy-MM-dd\'';
  static DATEINT_FORMAT = '\'yyyyMMdd\'';
  static TIME_FORMAT = '\'yyyy-MM-dd HH:mm:ss\'';

  static Tokenizer = DorisTokenizer;
  static Parser = DorisParser;
  static Generator = DorisGenerator;
}

Dialect.register(Dialects.DORIS, Doris);
