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

  constructor (arguments_: {
    schema?: string;
    tableName: string;
    columns: string[];
    rows: string[][];
  }) {
    super();
    this.schema = arguments_.schema;
    this.tableName = arguments_.tableName;
    this.columns = arguments_.columns;
    this.rows = arguments_.rows;
  }

  intern (): string {
    return `${this.kind}:${this.schema ?? DEFAULT_SCHEMA_NAME}.${this.tableName}`;
  }
}
