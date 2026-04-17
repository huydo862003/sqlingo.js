import {
  DEFAULT_SCHEMA_NAME,
} from '../constants';
import {
  SchemaElement,
} from './element';
import {
  DbmlKind,
} from './kind';

export class DbmlTableGroupField extends SchemaElement {
  readonly kind = DbmlKind.TABLE_GROUP_FIELD;
  schema: string;
  table: string;

  constructor (args: {
    schema?: string;
    table: string;
  }) {
    super();
    this.schema = args.schema ?? DEFAULT_SCHEMA_NAME;
    this.table = args.table;
  }

  intern (): string {
    return `${this.kind}:${this.schema}.${this.table}`;
  }
}

export class DbmlTableGroup extends SchemaElement {
  readonly kind = DbmlKind.TABLE_GROUP;
  name: string;
  tables: DbmlTableGroupField[];
  note?: string;

  constructor (args: {
    name: string;
    tables: DbmlTableGroupField[];
    note?: string;
  }) {
    super();
    this.name = args.name;
    this.tables = args.tables;
    this.note = args.note;
  }

  intern (): string {
    return `${this.kind}:${this.name}`;
  }
}
