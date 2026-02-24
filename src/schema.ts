import type { ColumnExpr } from './expressions';
import {
  AnonymousExpr,
  DataTypeExpr,
  DataTypeExprKind,
  DotExpr,
  IdentifierExpr,
  TABLE_PARTS,
  TableExpr,
  maybeParse,
  parseIdentifier,
} from './expressions';
import {
  Dialect, type DialectType,
} from './dialects/dialect';
import {
  objectDepth,
} from './helper';
import {
  multiInherit,
} from './port_internals/inheritance';
import {
  TrieResult, inTrie, newTrie, type TrieNode,
} from './trie';
import { SchemaError } from './errors';

export type ColumnMapping = string | Record<string, unknown> | string[];

export function flattenSchema (
  schema: Record<string, unknown>,
  depth?: number,
  keys: string[] = [],
): string[][] {
  const tables: string[][] = [];
  const d = depth ?? objectDepth(schema) - 1;

  for (const [k, v] of Object.entries(schema)) {
    if (d === 1 || typeof v !== 'object' || v === undefined) {
      tables.push([...keys, k]);
    } else if (2 <= d) {
      tables.push(...flattenSchema(v as Record<string, unknown>, d - 1, [...keys, k]));
    }
  }

  return tables;
}

export function nestedGet (
  d: Record<string, unknown>,
  path: Array<[string, string]>,
  options: { raiseOnMissing?: boolean } = {},
): unknown {
  const { raiseOnMissing = true } = options;
  let current: unknown = d;
  for (const [name, key] of path) {
    if (typeof current !== 'object' || current === undefined) {
      if (raiseOnMissing) {
        throw new SchemaError(`Unknown ${name === 'this' ? 'table' : name}: ${key}`);
      }
      return undefined;
    }
    const val = (current as Record<string, unknown>)[key];
    if (val === undefined || val === undefined) {
      if (raiseOnMissing) {
        throw new SchemaError(`Unknown ${name === 'this' ? 'table' : name}: ${key}`);
      }
      return undefined;
    }
    current = val;
  }
  return current;
}

export function nestedSet (d: Record<string, unknown>, keys: string[], value: unknown): Record<string, unknown> {
  if (!keys.length) return d;
  if (keys.length === 1) {
    d[keys[0]] = value;
    return d;
  }
  let current = d;
  for (let i = 0; i < keys.length - 1; i++) {
    if (!(keys[i] in current) || typeof current[keys[i]] !== 'object' || current[keys[i]] === undefined) {
      current[keys[i]] = {};
    }
    current = current[keys[i]] as Record<string, unknown>;
  }
  current[keys[keys.length - 1]] = value;
  return d;
}

export function ensureColumnMapping (mapping?: ColumnMapping): Record<string, unknown> {
  if (mapping == undefined) return {};
  if (typeof mapping === 'object' && !Array.isArray(mapping)) return mapping;
  if (typeof mapping === 'string') {
    return Object.fromEntries(
      mapping.split(',').map((part) => {
        const [name, type] = part.split(':').map((s) => s.trim());
        return [name, type];
      }),
    );
  }
  if (Array.isArray(mapping)) {
    return Object.fromEntries((mapping as string[]).map((col) => [col.trim(), undefined]));
  }
  throw new Error(`Invalid mapping provided: ${typeof mapping}`);
}

export function normalizeName (
  identifier: string | IdentifierExpr,
  options: { dialect?: DialectType;
    isTable?: boolean;
    normalize?: boolean; } = {},
): IdentifierExpr {
  const {
    dialect, isTable = false, normalize = true,
  } = options;
  const id = typeof identifier === 'string' ? parseIdentifier(identifier) : identifier;
  if (!normalize) return id;
  id.meta['isTable'] = isTable;
  return Dialect.getOrRaise(dialect).normalizeIdentifier(id) as IdentifierExpr;
}

/** Abstract base class for database schemas. */
export abstract class Schema {
  get dialect (): Dialect | undefined {
    return undefined;
  }

  abstract addTable (
    table: TableExpr | string,
    columnMapping?: ColumnMapping,
    options?: {
      dialect?: DialectType;
      normalize?: boolean;
      matchDepth?: boolean;
    },
  ): void;

  abstract columnNames (
    table: TableExpr | string,
    options?: {
      onlyVisible?: boolean;
      dialect?: DialectType;
      normalize?: boolean;
    },
  ): string[];

  abstract getColumnType (
    table: TableExpr | string,
    column: ColumnExpr | string,
    options?: {
      dialect?: DialectType;
      normalize?: boolean;
    },
  ): DataTypeExpr | undefined;

  abstract get supportedTableArgs (): readonly string[];

  get empty (): boolean {
    return true;
  }

  hasColumn (
    table: TableExpr | string,
    column: ColumnExpr | string,
    options?: {
      dialect?: DialectType;
      normalize?: boolean;
    },
  ): boolean {
    const name = typeof column === 'string' ? column : column.name;
    return this.columnNames(table, options).includes(name);
  }

  getUdfType (
    _udf: AnonymousExpr | string,
    _options?: {
      dialect?: DialectType;
      normalize?: boolean;
    },
  ): DataTypeExpr | undefined {
    return DataTypeExpr.build(DataTypeExprKind.UNKNOWN);
  }
}

/** Trie-based schema mixin providing `find`, `findUdf`, and `nestedGet`. */
export abstract class AbstractMappingSchema extends Schema {
  mapping: Record<string, unknown>;
  mappingTrie: TrieNode;
  udfMapping: Record<string, unknown>;
  udfTrie: TrieNode;

  protected _supportedTableArgs?: readonly string[];

  constructor (
    mapping?: Record<string, unknown>,
    udfMapping?: Record<string, unknown>,
  ) {
    super();
    this.mapping = mapping || {};
    this.udfMapping = udfMapping || {};
    this.mappingTrie = newTrie(
      flattenSchema(this.mapping, this.depth()).map((t) => [...t].reverse()),
    );
    this.udfTrie = newTrie(
      flattenSchema(this.udfMapping, this.udfDepth()).map((t) => [...t].reverse()),
    );
  }

  override get empty (): boolean {
    return Object.keys(this.mapping).length === 0;
  }

  depth (): number {
    return objectDepth(this.mapping);
  }

  udfDepth (): number {
    return objectDepth(this.udfMapping);
  }

  override get supportedTableArgs (): readonly string[] {
    if (!this._supportedTableArgs && !this.empty) {
      const d = this.depth();
      if (!d) {
        this._supportedTableArgs = [];
      } else if (1 <= d && d <= 3) {
        this._supportedTableArgs = TABLE_PARTS.slice(0, d);
      } else {
        throw new SchemaError(`Invalid mapping shape. Depth: ${d}`);
      }
    }
    return this._supportedTableArgs || [];
  }

  tableParts (table: TableExpr): string[] {
    return [...table.parts].reverse().map((p) => p.name);
  }

  udfParts (udf: AnonymousExpr): string[] {
    const parent = udf.parent;
    const parts = parent instanceof DotExpr
      ? [...parent.flatten()].map((p) => p.name)
      : [udf.name];
    return [...parts].reverse().slice(0, this.udfDepth());
  }

  protected findInTrie (
    parts: string[],
    trie: TrieNode,
    options: { raiseOnMissing: boolean },
  ): string[] | undefined {
    const { raiseOnMissing } = options;
    const [value, node] = inTrie(trie, parts);

    if (value === TrieResult.FAILED) {
      return undefined;
    }

    if (value === TrieResult.PREFIX) {
      const possibilities = flattenSchema(node as Record<string, unknown>);
      if (possibilities.length === 1) {
        parts.push(...possibilities[0]);
      } else {
        if (raiseOnMissing) {
          const joined = parts.join('.');
          const message = possibilities.map((p) => p.join('.')).join(', ');
          throw new SchemaError(`Ambiguous mapping for ${joined}: ${message}.`);
        }
        return undefined;
      }
    }

    return parts;
  }

  find (
    table: TableExpr,
    options: {
      raiseOnMissing?: boolean;
      ensureDataTypes?: boolean;
    } = {},
  ): unknown {
    const { raiseOnMissing = true } = options;
    const parts = this.tableParts(table).slice(0, this.supportedTableArgs.length);
    const resolvedParts = this.findInTrie(parts, this.mappingTrie, { raiseOnMissing });
    if (!resolvedParts) return undefined;
    return this.nestedGet(resolvedParts, undefined, { raiseOnMissing });
  }

  findUdf (udf: AnonymousExpr, options: { raiseOnMissing?: boolean } = {}): unknown {
    const { raiseOnMissing = false } = options;
    const parts = this.udfParts(udf);
    const resolvedParts = this.findInTrie(parts, this.udfTrie, { raiseOnMissing });
    if (!resolvedParts) return undefined;
    const reversed = [...resolvedParts].reverse();
    return nestedGet(
      this.udfMapping,
      resolvedParts.map((arg, i) => [arg, reversed[i]] as [string, string]),
      { raiseOnMissing },
    );
  }

  nestedGet (
    parts: string[],
    d?: Record<string, unknown>,
    options: {
      raiseOnMissing?: boolean;
    } = {},
  ): unknown {
    const { raiseOnMissing = true } = options;
    const reversed = [...parts].reverse();
    return nestedGet(
      d || this.mapping,
      this.supportedTableArgs.map((arg, i) => [arg, reversed[i]] as [string, string]),
      { raiseOnMissing },
    );
  }
}

/** Schema based on a nested mapping. */
export class MappingSchema extends multiInherit(AbstractMappingSchema, Schema) {
  visible: Record<string, unknown>;
  normalize: boolean;

  private _dialect: Dialect;
  private _typeCache: Record<string, DataTypeExpr | undefined> = {};
  private _depth: number = 0;

  constructor (
    schema?: Record<string, unknown>,
    options: {
      visible?: Record<string, unknown>;
      dialect?: DialectType;
      normalize?: boolean;
      udfMapping?: Record<string, unknown>;
    } = {},
  ) {
    const {
      visible, dialect, normalize = true, udfMapping,
    } = options;
    const d = dialect ? Dialect.getOrRaise(dialect) : new Dialect();
    const rawSchema = schema || {};
    const rawUdfs = udfMapping || {};
    super(
      normalize ? MappingSchema.normalizeSchemaStatic(rawSchema, d, { normalize }) : rawSchema,
      normalize ? MappingSchema.normalizeUdfsStatic(rawUdfs, d, { normalize }) : rawUdfs,
    );
    this.visible = visible || {};
    this.normalize = normalize;
    this._dialect = d;
  }

  static fromMappingSchema (other: MappingSchema): MappingSchema {
    return new MappingSchema(other.mapping, {
      visible: other.visible,
      dialect: other.dialect,
      normalize: other.normalize,
      udfMapping: other.udfMapping,
    });
  }

  override get dialect (): Dialect {
    return this._dialect;
  }

  override depth (): number {
    if (!this.empty && !this._depth) {
      this._depth = super.depth() - 1;
    }
    return this._depth;
  }

  copy (overrides: {
    schema?: Record<string, unknown>;
    visible?: Record<string, unknown>;
    dialect?: DialectType;
    normalize?: boolean;
    udfMapping?: Record<string, unknown>;
  } = {}): MappingSchema {
    return new MappingSchema(
      overrides.schema ?? { ...this.mapping },
      {
        visible: overrides.visible ?? { ...this.visible },
        dialect: overrides.dialect ?? this.dialect,
        normalize: overrides.normalize ?? this.normalize,
        udfMapping: overrides.udfMapping ?? { ...this.udfMapping },
      },
    );
  }

  override find (
    table: TableExpr,
    options: {
      raiseOnMissing?: boolean;
      ensureDataTypes?: boolean;
    } = {},
  ): unknown {
    const { ensureDataTypes = false } = options;
    const schema = super.find(table, options);
    if (!ensureDataTypes || typeof schema !== 'object' || schema === null) {
      return schema;
    }
    return Object.fromEntries(
      Object.entries(schema).map(([col, dtype]) => [col, typeof dtype === 'string' ? this.toDataType(dtype) : dtype]),
    );
  }

  override addTable (
    table: TableExpr | string,
    columnMapping?: ColumnMapping,
    options: { dialect?: DialectType;
      normalize?: boolean;
      matchDepth?: boolean; } = {},
  ): void {
    const {
      dialect, normalize, matchDepth = true,
    } = options;
    const normalizedTable = this.normalizeTable(table, dialect, normalize);

    if (matchDepth && !this.empty && normalizedTable.parts.length !== this.depth()) {
      throw new SchemaError(
        `Table ${normalizedTable.sql()} must match the schema's nesting level: ${this.depth()}.`,
      );
    }

    const normalizedColumnMapping = Object.fromEntries(
      Object.entries(ensureColumnMapping(columnMapping)).map(([k, v]) => [
        this.normalizeName(k, dialect, {
          isTable: false,
          normalize,
        }),
        v,
      ]),
    );

    const existing = this.find(normalizedTable, { raiseOnMissing: false });
    if (existing && !Object.keys(normalizedColumnMapping).length) return;

    const parts = this.tableParts(normalizedTable);
    nestedSet(this.mapping, [...parts].reverse(), normalizedColumnMapping);
    newTrie([parts], this.mappingTrie);
  }

  override columnNames (
    table: TableExpr | string,
    options: { onlyVisible?: boolean;
      dialect?: DialectType;
      normalize?: boolean; } = {},
  ): string[] {
    const {
      onlyVisible = false, dialect, normalize,
    } = options;
    const normalizedTable = this.normalizeTable(table, dialect, normalize);
    const schema = this.find(normalizedTable, { raiseOnMissing: false });
    if (!schema || typeof schema !== 'object') return [];

    const columns = Object.keys(schema as object);
    if (!onlyVisible || !Object.keys(this.visible).length) {
      return columns;
    }

    const visible = (this.nestedGet(this.tableParts(normalizedTable), this.visible, { raiseOnMissing: false }) || []) as string[];
    return columns.filter((col) => visible.includes(col));
  }

  override getColumnType (
    table: TableExpr | string,
    column: ColumnExpr | string,
    options: {
      dialect?: DialectType;
      normalize?: boolean;
    } = {},
  ): DataTypeExpr | undefined {
    const {
      dialect, normalize,
    } = options;
    const normalizedTable = this.normalizeTable(table, dialect, normalize);
    const colName = this.normalizeName(
      typeof column === 'string' ? column : column.args.this as IdentifierExpr,
      dialect,
      {
        isTable: false,
        normalize,
      },
    );

    const tableSchema = this.find(normalizedTable, { raiseOnMissing: false });
    if (tableSchema && typeof tableSchema === 'object') {
      const colType = (tableSchema as Record<string, unknown>)[colName];
      if (colType instanceof DataTypeExpr) return colType;
      if (typeof colType === 'string') return this.toDataType(colType, dialect);
    }

    return DataTypeExpr.build(DataTypeExprKind.UNKNOWN);
  }

  override hasColumn (
    table: TableExpr | string,
    column: ColumnExpr | string,
    options?: { dialect?: DialectType;
      normalize?: boolean; },
  ): boolean {
    const {
      dialect, normalize,
    } = options || {};
    const normalizedTable = this.normalizeTable(table, dialect, normalize);
    const normalizedColName = this.normalizeName(
      typeof column === 'string' ? column : column.args.this as IdentifierExpr,
      dialect,
      {
        isTable: false,
        normalize,
      },
    );
    const tableSchema = this.find(normalizedTable, { raiseOnMissing: false });
    return tableSchema ? normalizedColName in (tableSchema as object) : false;
  }

  override getUdfType (
    udf: AnonymousExpr | string,
    options: {
      dialect?: DialectType;
      normalize?: boolean;
    } = {},
  ): DataTypeExpr | undefined {
    const {
      dialect, normalize,
    } = options;
    const parts = this.normalizeUdf(udf, dialect, normalize);
    const resolvedParts = this.findInTrie(parts, this.udfTrie, { raiseOnMissing: false });
    if (!resolvedParts) return DataTypeExpr.build(DataTypeExprKind.UNKNOWN);

    const reversed = [...resolvedParts].reverse();
    const udfType = nestedGet(
      this.udfMapping,
      resolvedParts.map((arg, i) => [arg, reversed[i]] as [string, string]),
      { raiseOnMissing: false },
    );
    if (udfType instanceof DataTypeExpr) return udfType;
    if (typeof udfType === 'string') return this.toDataType(udfType, dialect);
    return DataTypeExpr.build(DataTypeExprKind.UNKNOWN);
  }

  private normalizeUdf (
    udf: AnonymousExpr | string,
    dialect?: DialectType,
    normalize?: boolean,
  ): string[] {
    const d = dialect ? Dialect.getOrRaise(dialect) : this._dialect;
    const shouldNormalize = normalize ?? this.normalize;

    let udfExpr: AnonymousExpr;
    if (typeof udf === 'string') {
      const parsed = maybeParse(udf, { dialect: d });
      if (parsed instanceof AnonymousExpr) {
        udfExpr = parsed;
      } else if (parsed instanceof DotExpr) {
        const parts = [...parsed.flatten()];
        const last = parts[parts.length - 1];
        if (!(last instanceof AnonymousExpr)) {
          throw new SchemaError(`Unable to parse UDF from: ${JSON.stringify(udf)}`);
        }
        udfExpr = last;
      } else {
        throw new SchemaError(`Unable to parse UDF from: ${JSON.stringify(udf)}`);
      }
    } else {
      udfExpr = udf;
    }

    const parts = this.udfParts(udfExpr);
    if (shouldNormalize) {
      return parts.map((part) => this.normalizeName(part, dialect, {
        isTable: true,
        normalize,
      }));
    }
    return parts;
  }

  private toDataType (schemaType: string, dialect?: DialectType): DataTypeExpr | undefined {
    if (!this._typeCache[schemaType]) {
      const d = dialect ? Dialect.getOrRaise(dialect) : this._dialect;
      this._typeCache[schemaType] = DataTypeExpr.build(schemaType, { dialect: d });
    }
    return this._typeCache[schemaType];
  }

  private normalizeTable (
    table: TableExpr | string,
    dialect?: DialectType,
    normalize?: boolean,
  ): TableExpr {
    const d = dialect ? Dialect.getOrRaise(dialect) : this._dialect;
    const shouldNormalize = normalize ?? this.normalize;
    const tableExpr = maybeParse<TableExpr>(table, {
      into: TableExpr,
      dialect: d,
    });
    if (shouldNormalize) {
      for (const part of tableExpr.parts) {
        if (part instanceof IdentifierExpr) {
          part.replace(normalizeName(part, {
            dialect: d,
            isTable: true,
            normalize: shouldNormalize,
          }));
        }
      }
    }
    return tableExpr;
  }

  private normalizeName (
    name: string | IdentifierExpr,
    dialect?: DialectType,
    options: { isTable?: boolean;
      normalize?: boolean; } = {},
  ): string {
    const {
      isTable = false, normalize,
    } = options;
    const d = dialect ? Dialect.getOrRaise(dialect) : this._dialect;
    return normalizeName(name, {
      dialect: d,
      isTable,
      normalize: normalize ?? this.normalize,
    }).name;
  }

  private static normalizeSchemaStatic (
    schema: Record<string, unknown>,
    dialect: Dialect,
    options: { normalize: boolean },
  ): Record<string, unknown> {
    const { normalize } = options;
    if (!normalize || !Object.keys(schema).length) return schema;

    const normalized: Record<string, unknown> = {};
    const flattened = flattenSchema(schema);
    if (!flattened.length) return schema;

    for (const keys of flattened) {
      const columns = nestedGet(
        schema,
        keys.map((k) => [k, k] as [string, string]),
        { raiseOnMissing: false },
      );
      if (typeof columns !== 'object' || columns === undefined) {
        throw new SchemaError(
          `Table ${keys.slice(0, -1).join('.')} must match the schema's nesting level: ${flattened[0].length}.`,
        );
      }
      const colEntries = Object.entries(columns as Record<string, unknown>);
      if (!colEntries.length) {
        throw new SchemaError(`Table ${keys.slice(0, -1).join('.')} must have at least one column`);
      }
      const firstVal = colEntries[0][1];
      if (typeof firstVal === 'object' && firstVal !== undefined) {
        const deeper = flattenSchema(columns as Record<string, unknown>);
        throw new SchemaError(
          `Table ${[...keys, ...(deeper[0] || [])].join('.')} must match the schema's nesting level: ${flattened[0].length}.`,
        );
      }
      const normalizedKeys = keys.map((k) =>
        normalizeName(k, {
          dialect,
          isTable: true,
          normalize,
        }).name);
      for (const [colName, colType] of colEntries) {
        nestedSet(
          normalized,
          [
            ...normalizedKeys,
            normalizeName(colName, {
              dialect,
              normalize,
            }).name,
          ],
          colType,
        );
      }
    }
    return normalized;
  }

  private static normalizeUdfsStatic (
    udfs: Record<string, unknown>,
    dialect: Dialect,
    options: { normalize: boolean },
  ): Record<string, unknown> {
    const { normalize } = options;
    if (!normalize || !Object.keys(udfs).length) return udfs;

    const normalized: Record<string, unknown> = {};
    for (const keys of flattenSchema(udfs, objectDepth(udfs))) {
      const udfType = nestedGet(
        udfs,
        keys.map((k) => [k, k] as [string, string]),
        { raiseOnMissing: false },
      );
      const normalizedKeys = keys.map((k) =>
        normalizeName(k, {
          dialect,
          isTable: true,
          normalize,
        }).name);
      nestedSet(normalized, normalizedKeys, udfType);
    }
    return normalized;
  }
}

export function ensureSchema (
  schema?: Record<string, unknown> | Schema,
  options: {
    dialect?: DialectType;
  } = {},
): Schema {
  if (schema instanceof Schema) {
    return schema;
  }
  return new MappingSchema(schema, options);
}
