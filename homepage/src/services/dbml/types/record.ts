import {
  DEFAULT_SCHEMA_NAME,
} from '../constants';
import {
  SchemaElement,
} from './element';
import {
  DbmlKind,
} from './kind';

export class DbmlRecord extends SchemaElement {
  readonly kind = DbmlKind.RECORD;
  schema?: string;
  tableName: string;
  columns: string[];
  rows: string[][];

  constructor (args: {
    schema?: string;
    tableName: string;
    columns: string[];
    rows: string[][];
  }) {
    super();
    this.schema = args.schema;
    this.tableName = args.tableName;
    this.columns = args.columns;
    this.rows = args.rows;
  }

  intern (): string {
    return `${this.kind}:${this.schema ?? DEFAULT_SCHEMA_NAME}.${this.tableName}`;
  }
}
