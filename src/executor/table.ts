import type { DialectType } from '../dialects/dialect';
import { objectDepth } from '../helper';
import {
  MappingSchema, normalizeName,
} from '../schema';

export class Table {
  public columns: string[];
  public columnRange?: ColumnRange;
  public reader: RowReader;
  public rows: unknown[][];
  public rangeReader: RangeReader;

  constructor (
    columns: Iterable<string>,
    rows: unknown[][] = [],
    columnRange?: ColumnRange,
  ) {
    this.columns = Array.from(columns);
    this.columnRange = columnRange;
    this.reader = new RowReader(this.columns, this.columnRange);
    this.rows = rows;

    if (0 < this.rows.length) {
      if (this.rows[0].length !== this.columns.length) {
        throw new Error('Row length does not match column length.');
      }
    }

    this.rangeReader = new RangeReader(this);
  }

  addColumns (...columns: string[]): void {
    this.columns.push(...columns);
    if (this.columnRange) {
      this.columnRange = {
        start: this.columnRange.start,
        stop: this.columnRange.stop + columns.length,
      };
    }
    this.reader = new RowReader(this.columns, this.columnRange);
  }

  append (row: unknown[]): void {
    if (row.length !== this.columns.length) {
      throw new Error('Row length does not match column length.');
    }
    this.rows.push(row);
  }

  pop (): void {
    this.rows.pop();
  }

  toJslist (): Record<string, unknown>[] {
    return this.rows.map((row) => {
      const obj: Record<string, unknown> = {};
      this.columns.forEach((col, i) => {
        obj[col] = row[i];
      });
      return obj;
    });
  }

  get width (): number {
    return this.columns.length;
  }

  get length (): number {
    return this.rows.length;
  }

  * [Symbol.iterator] (): Generator<RowReader> {
    for (let i = 0; i < this.rows.length; i++) {
      yield this.get(i);
    }
  }

  get (index: number): RowReader {
    this.reader.row = this.rows[index];
    return this.reader;
  }

  toString (): string {
    const cols = this.columns.filter(
      (_, i) => !this.columnRange || (this.columnRange.start <= i && i < this.columnRange.stop),
    );

    const widths: Record<string, number> = {};
    cols.forEach((col) => {
      widths[col] = col.length;
    });

    const lines = [cols.join(' ')];

    for (let i = 0; i < this.length; i++) {
      if (10 < i) break;

      const reader = this.get(i);
      const rowStr = cols
        .map((col) => {
          const valStr = String(reader.row[reader.columns[col]] ?? '');
          return valStr.padStart(widths[col], ' ').substring(0, widths[col]);
        })
        .join(' ');

      lines.push(rowStr);
    }

    return lines.join('\n');
  }
}

export class TableIter implements Iterator<RowReader> {
  public table: Table;
  public index: number = -1;

  constructor (table: Table) {
    this.table = table;
  }

  [Symbol.iterator] () {
    return this;
  }

  next (): IteratorResult<RowReader> {
    this.index++;
    if (this.index < this.table.length) {
      return {
        value: this.table.get(this.index),
        done: false,
      };
    }
    return {
      value: undefined,
      done: true,
    };
  }
}

export class Tables extends MappingSchema {}

export function ensureTables (d?: Record<string, unknown>, dialect?: DialectType): Tables {
  return new Tables(_ensureTables(d, dialect));
}

function _ensureTables (d?: unknown, dialect?: DialectType): Record<string, unknown> {
  if (typeof d !== 'object' || !d || Object.keys(d).length === 0) {
    return {};
  }

  const depth = objectDepth(d);
  if (1 < depth) {
    const nestedResult: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(d)) {
      const normalizedName = normalizeName(k, {
        dialect,
        isTable: true,
      }).name;
      nestedResult[normalizedName] = _ensureTables(v, dialect);
    }
    return nestedResult;
  }

  const result: Record<string, unknown> = {};
  for (const [rawTableName, tableData] of Object.entries(d)) {
    const tableName = normalizeName(rawTableName, { dialect }).name;

    if (tableData instanceof Table) {
      result[tableName] = tableData;
    } else {
      // Normalize raw object arrays into Table structures
      const normalizedTableData = tableData.map((row: Record<string, unknown>) => {
        const newRow: Record<string, unknown> = {};
        for (const [columnName, value] of Object.entries(row)) {
          newRow[normalizeName(columnName, { dialect }).name] = value;
        }
        return newRow;
      });

      const columnNames = 0 < normalizedTableData.length ? Object.keys(normalizedTableData[0]) : [];
      const rows = normalizedTableData.map((row: Record<string, unknown>) => columnNames.map((name) => row[name]));

      result[tableName] = new Table(columnNames, rows);
    }
  }

  return result;
}

export interface ColumnRange {
  start: number;
  stop: number;
}

/**
 * Reads a specific column for the current row.
 * Wrapped in a Proxy to allow dynamic column access like `reader.column_name`.
 */
export class RowReader {
  [column: string]: unknown;
  public columns: Record<string, number> = {};
  public row: unknown[] = [];

  constructor (columns: string[], columnRange?: ColumnRange) {
    columns.forEach((column, i) => {
      if (!columnRange || (columnRange.start <= i && i < columnRange.stop)) {
        this.columns[column] = i;
      }
    });

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return new Proxy(this as any, {
      get (target, prop: string | symbol) {
        if (prop in target) return target[prop];
        if (typeof prop === 'string' && prop in target.columns && target.row) {
          return target.row[target.columns[prop]];
        }
        return undefined;
      },
    });
  }
}

/**
 * Reads a range of rows for a specific column, returning an iterator.
 * Wrapped in a Proxy to allow dynamic column access like `rangeReader.column_name`.
 */
export class RangeReader {
  public table: Table;
  public range: ColumnRange = {
    start: 0,
    stop: 0,
  };

  constructor (table: Table) {
    this.table = table;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return new Proxy(this as any, {
      get (target, prop: string | symbol) {
        if (prop in target) return target[prop];
        if (typeof prop === 'string') {
          return (function* () {
            for (let i = target.range.start; i < target.range.stop; i++) {
              yield target.table.rows[i][target.table.columns.indexOf(prop)];
            }
          })();
        }
        return undefined;
      },
    });
  }

  get length (): number {
    return Math.max(0, this.range.stop - this.range.start);
  }
}
