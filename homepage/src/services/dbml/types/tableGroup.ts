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

  constructor (arguments_: {
    schema?: string;
    table: string;
  }) {
    super();
    this.schema = arguments_.schema ?? DEFAULT_SCHEMA_NAME;
    this.table = arguments_.table;
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

  constructor (arguments_: {
    name: string;
    tables: DbmlTableGroupField[];
    note?: string;
  }) {
    super();
    this.name = arguments_.name;
    this.tables = arguments_.tables;
    this.note = arguments_.note;
  }

  intern (): string {
    return `${this.kind}:${this.name}`;
  }
}
