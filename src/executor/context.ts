import { lt } from '../port_internals/ops_utils';
import type { Env } from './env';
import { ENV } from './env';
import type {
  RangeReader,
  RowReader, Table,
} from './table';

/**
 * Execution context for sql expressions.
 *
 * Context is used to hold relevant data tables which can then be queried on with eval.
 *
 * References to columns can either be scalar or vectors. When setRow is used, column references
 * evaluate to scalars while setRange evaluates to vectors. This allows convenient and efficient
 * evaluation of aggregation functions.
 */
export class Context {
  public tables: Map<string, Table>;
  public env: Env;
  public rangeReaders: Record<string, RangeReader>;
  public rowReaders: Record<string, RowReader>;
  private _table: Table | undefined = undefined;

  /**
   * @param tables Map of tables representing the scope of the current execution context.
   * @param env Dictionary of functions within the execution context.
   */
  constructor (tables: Map<string, Table>, env?: Env) {
    this.tables = tables;
    this.rangeReaders = {};
    this.rowReaders = {};

    for (const [name, table] of this.tables) {
      this.rangeReaders[name] = table.rangeReader;
      this.rowReaders[name] = table.reader;
    }

    this.env = {
      ...ENV,
      ...(env || {}),
      scope: this.rowReaders,
    };
  }

  /**
   * Evaluates dynamic JavaScript/TypeScript code string within the execution environment.
   */
  eval (code: string): unknown {
    const keys = Object.keys(this.env);
    const values = Object.values(this.env);
    const func = new Function(...keys, `return ${code};`);
    return func(...values);
  }

  evalTuple (codes: string[]): unknown[] {
    return codes.map((code) => this.eval(code));
  }

  get table (): Table {
    if (this._table === undefined) {
      const tablesArray = Array.from(this.tables.values());
      this._table = tablesArray[0];

      for (const other of tablesArray) {
        if (this._table.columns.join(',') !== other.columns.join(',')) {
          throw new Error('Columns are different.');
        }
        if (this._table.rows.length !== other.rows.length) {
          throw new Error('Rows are different.');
        }
      }
    }

    return this._table;
  }

  addColumns (...columns: string[]): void {
    for (const table of this.tables.values()) {
      table.addColumns(...columns);
    }
  }

  get columns (): string[] {
    return this.table.columns;
  }

  /**
   * Iterates through the tables row by row.
   */
  * [Symbol.iterator] (): Generator<[RowReader, Context]> {
    this.env['scope'] = this.rowReaders;

    for (let i = 0; i < this.table.rows.length; i++) {
      let reader: RowReader | undefined;
      for (const table of this.tables.values()) {
        reader = table.get(i);
      }
      yield [reader!, this];
    }
  }

  tableIter (table: string): Generator<RowReader> {
    this.env['scope'] = this.rowReaders;
    const targetTable = this.tables.get(table);
    if (!targetTable) throw new Error(`Table ${table} not found in context.`);

    return targetTable[Symbol.iterator]();
  }

  filter (condition: string): void {
    const rows: unknown[][] = [];

    for (const [reader] of this) {
      if (this.eval(condition)) {
        rows.push(reader.row);
      }
    }

    for (const table of this.tables.values()) {
      table.rows = rows;
    }
  }

  sort (keys: string[]): void {
    const mapped = this.table.rows.map((row) => {
      this.setRow(row);
      const evaluatedKeys = this.evalTuple(keys).map((t) => [t === undefined, t]);
      return {
        row,
        evaluatedKeys,
      };
    });

    mapped.sort((a, b) => {
      for (let i = 0; i < keys.length; i++) {
        const aVal = a.evaluatedKeys[i];
        const bVal = b.evaluatedKeys[i];

        if (aVal[0] !== bVal[0]) return aVal[0] ? 1 : -1;

        if (lt(aVal[1], bVal[1])) return -1;
        if (lt(bVal[1], aVal[1])) return 1;
      }
      return 0;
    });

    const sortedRows = mapped.map((m) => m.row);
    for (const table of this.tables.values()) {
      table.rows = sortedRows;
    }
  }

  setRow (row: unknown[]): void {
    for (const table of this.tables.values()) {
      table.reader.row = row;
    }
    this.env['scope'] = this.rowReaders;
  }

  setIndex (index: number): void {
    for (const table of this.tables.values()) {
      table.get(index);
    }
    this.env['scope'] = this.rowReaders;
  }

  setRange (start: number, end: number): void {
    for (const [name] of this.tables) {
      this.rangeReaders[name].range = {
        start,
        stop: end,
      };
    }
    this.env['scope'] = this.rangeReaders;
  }

  has (table: string): boolean {
    return this.tables.has(table);
  }
}
