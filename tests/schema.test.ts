import {
  describe, test, expect,
} from 'vitest';
import type { ColumnExpr } from '../src/expressions';
import {
  DataTypeExprKind,
  column,
  table_,
  toTable,
  DataTypeExpr,
  ColumnDefExpr,
} from '../src/expressions';
import { narrowInstanceOf } from '../src/port_internals';
import { parseOne } from '../src/parser';
import {
  MappingSchema,
  ensureSchema,
} from '../src/schema';
import { SchemaError } from '../src/errors';

class TestSchema {
  private assertColumnNames (
    schema: ReturnType<typeof ensureSchema>,
    ...tableResults: [string, string[]][]
  ) {
    for (const [table, result] of tableResults) {
      expect(schema.columnNames(toTable(table))).toEqual(result);
    }
  }

  private assertColumnNamesRaises (
    schema: ReturnType<typeof ensureSchema>,
    ...tables: string[]
  ) {
    for (const table of tables) {
      expect(() => schema.columnNames(toTable(table))).toThrow(SchemaError);
    }
  }

  private assertColumnNamesEmpty (
    schema: ReturnType<typeof ensureSchema>,
    ...tables: string[]
  ) {
    for (const table of tables) {
      expect(schema.columnNames(toTable(table))).toEqual([]);
    }
  }

  testSchema () {
    const schema = ensureSchema({
      x: { a: 'uint64' },
      y: {
        b: 'uint64',
        c: 'uint64',
      },
    });

    this.assertColumnNames(
      schema,
      ['x', ['a']],
      ['y', ['b', 'c']],
      ['z.x', ['a']],
      ['z.x.y', ['b', 'c']],
    );

    this.assertColumnNamesEmpty(schema, 'z', 'z.z', 'z.z.z');
  }

  testSchemaDb () {
    const schema = ensureSchema({
      d1: {
        x: { a: 'uint64' },
        y: { b: 'uint64' },
      },
      d2: {
        x: { c: 'uint64' },
      },
    });

    this.assertColumnNames(
      schema,
      ['d1.x', ['a']],
      ['d2.x', ['c']],
      ['y', ['b']],
      ['d1.y', ['b']],
      ['z.d1.y', ['b']],
    );

    this.assertColumnNamesRaises(schema, 'x');

    this.assertColumnNamesEmpty(schema, 'z.x', 'z.y');
  }

  testSchemaCatalog () {
    const schema = ensureSchema({
      c1: {
        d1: {
          x: { a: 'uint64' },
          y: { b: 'uint64' },
          z: { c: 'uint64' },
        },
      },
      c2: {
        d1: {
          y: { d: 'uint64' },
          z: { e: 'uint64' },
        },
        d2: {
          z: { f: 'uint64' },
        },
      },
    });

    this.assertColumnNames(
      schema,
      ['x', ['a']],
      ['d1.x', ['a']],
      ['c1.d1.x', ['a']],
      ['c1.d1.y', ['b']],
      ['c1.d1.z', ['c']],
      ['c2.d1.y', ['d']],
      ['c2.d1.z', ['e']],
      ['d2.z', ['f']],
      ['c2.d2.z', ['f']],
    );

    this.assertColumnNamesRaises(schema, 'y', 'z', 'd1.y', 'd1.z');

    this.assertColumnNamesEmpty(schema, 'q', 'd2.x', 'a.b.c');
  }

  testSchemaAddTableWithAndWithoutMapping () {
    const schema = new MappingSchema();
    schema.addTable('test');
    expect(schema.columnNames('test')).toEqual([]);
    schema.addTable('test', { x: 'string' });
    expect(schema.columnNames('test')).toEqual(['x']);
    schema.addTable('test', {
      x: 'string',
      y: 'int',
    });
    expect(schema.columnNames('test')).toEqual(['x', 'y']);
    schema.addTable('test');
    expect(schema.columnNames('test')).toEqual(['x', 'y']);
  }

  testSchemaGetColumnType () {
    const schema1 = new MappingSchema({ A: { b: 'varchar' } });
    expect(schema1.getColumnType('a', 'B')?.args.this).toBe(DataTypeExprKind.VARCHAR);
    expect(schema1.getColumnType(table_('a'), column({ col: 'b' }) as ColumnExpr)?.args.this).toBe(DataTypeExprKind.VARCHAR);
    expect(schema1.getColumnType('a', column({ col: 'b' }) as ColumnExpr)?.args.this).toBe(DataTypeExprKind.VARCHAR);
    expect(schema1.getColumnType(table_('a'), 'b')?.args.this).toBe(DataTypeExprKind.VARCHAR);

    const schema2 = new MappingSchema({ a: { b: { c: 'varchar' } } });
    expect(schema2.getColumnType(table_('b', { db: 'a' }), column({ col: 'c' }) as ColumnExpr)?.args.this).toBe(DataTypeExprKind.VARCHAR);
    expect(schema2.getColumnType(table_('b', { db: 'a' }), 'c')?.args.this).toBe(DataTypeExprKind.VARCHAR);

    const schema3 = new MappingSchema({ a: { b: { c: { d: 'varchar' } } } });
    expect(schema3.getColumnType(table_('c', {
      db: 'b',
      catalog: 'a',
    }), column({ col: 'd' }) as ColumnExpr)?.args.this).toBe(DataTypeExprKind.VARCHAR);
    expect(schema3.getColumnType(table_('c', {
      db: 'b',
      catalog: 'a',
    }), 'd')?.args.this).toBe(DataTypeExprKind.VARCHAR);

    const schema4 = new MappingSchema({ foo: { bar: parseOne('INT', { into: DataTypeExpr }) } });
    expect(schema4.getColumnType('foo', 'bar')?.args.this).toBe(DataTypeExprKind.INT);
  }

  testSchemaNormalization () {
    const schema1 = new MappingSchema(
      {
        x: {
          '`y`': {
            Z: {
              'a': 'INT',
              '`B`': 'VARCHAR',
            },
            w: { C: 'INT' },
          },
        },
      },
      { dialect: 'clickhouse' },
    );

    const tableZ = table_('Z', {
      db: 'y',
      catalog: 'x',
    });
    const tableW = table_('w', {
      db: 'y',
      catalog: 'x',
    });

    expect(schema1.columnNames(tableZ)).toEqual(['a', 'B']);
    expect(schema1.columnNames(tableW)).toEqual(['C']);

    const schema2 = new MappingSchema({ x: { '`y`': 'INT' } }, { dialect: 'clickhouse' });
    expect(schema2.columnNames(table_('x'))).toEqual(['y']);

    const schema3 = new MappingSchema();
    schema3.addTable('Foo', {
      'SomeColumn': 'INT',
      '"SomeColumn"': 'DOUBLE',
    });
    expect(schema3.columnNames(table_('fOO'))).toEqual(['somecolumn', 'SomeColumn']);

    const schema4 = new MappingSchema({
      x: {
        'foo': 'int',
        '"bLa"': 'int',
      },
    }, { dialect: 'snowflake' });
    expect(schema4.columnNames(table_('x'))).toEqual(['FOO', 'bLa']);

    const schema5 = new MappingSchema({ x: { foo: 'int' } }, {
      normalize: false,
      dialect: 'snowflake',
    });
    expect(schema5.columnNames(table_('x'))).toEqual(['foo']);

    const schema6 = new MappingSchema({ '[Fo]': { x: 'int' } }, { dialect: 'tsql' });
    expect(schema6.columnNames('[Fo]')).toEqual(schema6.columnNames('`fo`', { dialect: 'clickhouse' }));

    const schema7 = new MappingSchema({ Foo: { '`BaR`': 'int' } }, { dialect: 'bigquery' });
    expect(schema7.columnNames('Foo')).toEqual(['bar']);
    expect(schema7.columnNames('foo')).toEqual([]);

    const schema8 = new MappingSchema({ X: { y: 'int' } }, {
      normalize: false,
      dialect: 'snowflake',
    });
    expect(schema8.columnNames('x', { normalize: true })).toEqual(['y']);

    const schemaBqStruct = new MappingSchema({ t: { col: 'STRUCT<FooBar INT>' } }, { dialect: 'bigquery' });
    const colTypeBq = schemaBqStruct.getColumnType('t', 'col');
    expect(narrowInstanceOf(colTypeBq?.args.expressions?.[0], ColumnDefExpr)?.name).toBe('foobar');

    const schemaSnowStruct = new MappingSchema({ t: { col: 'STRUCT<FooBar INT>' } }, { dialect: 'snowflake' });
    const colTypeSnow = schemaSnowStruct.getColumnType('T', 'COL');
    expect(narrowInstanceOf(colTypeSnow?.args.expressions?.[0], ColumnDefExpr)?.name).toBe('FOOBAR');

    const schemaCkStruct = new MappingSchema({ t: { col: 'STRUCT<FooBar INT>' } }, { dialect: 'clickhouse' });
    const colTypeCk = schemaCkStruct.getColumnType('t', 'col');
    expect(narrowInstanceOf(colTypeCk?.args.expressions?.[0], ColumnDefExpr)?.name).toBe('FooBar');

    const schemaNestedStruct = new MappingSchema(
      { t: { col: 'STRUCT<Outer STRUCT<Inner INT>>' } },
      { dialect: 'bigquery' },
    );
    const colTypeNested = schemaNestedStruct.getColumnType('t', 'col');
    expect(narrowInstanceOf(colTypeNested?.args.expressions?.[0], ColumnDefExpr)?.name).toBe('outer');
    expect(narrowInstanceOf(narrowInstanceOf(narrowInstanceOf(colTypeNested?.args.expressions?.[0], ColumnDefExpr)?.kind, DataTypeExpr)?.args.expressions?.[0], ColumnDefExpr)?.name).toBe('inner');

    const schemaArrayStruct = new MappingSchema(
      { t: { col: 'ARRAY<STRUCT<FooBar INT>>' } },
      { dialect: 'bigquery' },
    );
    const colTypeArray = schemaArrayStruct.getColumnType('t', 'col');
    const structType = narrowInstanceOf(colTypeArray?.args.expressions?.[0], DataTypeExpr);
    expect(narrowInstanceOf(structType?.args.expressions?.[0], ColumnDefExpr)?.name).toBe('foobar');
  }

  testSameNumberOfQualifiers () {
    const schema1 = new MappingSchema({ x: { y: { c1: 'int' } } });
    expect(() => schema1.addTable('z', { c2: 'int' })).toThrow('Table z must match the schema\'s nesting level: 2.');

    const schema2 = new MappingSchema();
    schema2.addTable('x.y', { c1: 'int' });
    expect(() => schema2.addTable('z', { c2: 'int' })).toThrow('Table z must match the schema\'s nesting level: 2.');

    expect(() => new MappingSchema({
      x: { y: { c1: 'int' } },
      z: { c2: 'int' },
    }))
      .toThrow('Table z must match the schema\'s nesting level: 2.');

    expect(() => new MappingSchema({
      catalog: { db: { tbl: { col: 'a' } } },
      tbl2: { col: 'b' },
    })).toThrow('Table tbl2 must match the schema\'s nesting level: 3.');

    expect(() => new MappingSchema({
      tbl2: { col: 'b' },
      catalog: { db: { tbl: { col: 'a' } } },
    })).toThrow('Table catalog.db.tbl must match the schema\'s nesting level: 1.');
  }

  testHasColumn () {
    const schema = new MappingSchema({ x: { c: 'int' } });
    expect(schema.hasColumn('x', column({ col: 'c' }) as ColumnExpr)).toBe(true);
    expect(schema.hasColumn('x', column({ col: 'k' }) as ColumnExpr)).toBe(false);
  }

  testFind () {
    const schema = new MappingSchema({ x: { c: 'int' } });
    const found = schema.find(toTable('x') as ReturnType<typeof toTable>);
    expect(found).toEqual({ c: 'int' });
    const foundWithTypes = schema.find(toTable('x') as ReturnType<typeof toTable>, { ensureDataTypes: true });
    expect(foundWithTypes).toEqual({ c: DataTypeExpr.build('int') });
  }
}

const t = new TestSchema();

describe('TestSchema', () => {
  test('schema', () => t.testSchema());
  test('testSchemaDb', () => t.testSchemaDb());
  test('testSchemaCatalog', () => t.testSchemaCatalog());
  test('testSchemaAddTableWithAndWithoutMapping', () => t.testSchemaAddTableWithAndWithoutMapping());
  test('testSchemaGetColumnType', () => t.testSchemaGetColumnType());
  test('testSchemaNormalization', () => t.testSchemaNormalization());
  test('testSameNumberOfQualifiers', () => t.testSameNumberOfQualifiers());
  test('testHasColumn', () => t.testHasColumn());
  test('find', () => t.testFind());
});
