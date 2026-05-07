import {
  DEFAULT_SCHEMA_NAME,
} from '../constants';
import type {
  DbmlColumn,
} from './column';
import {
  SchemaElement,
} from './element';
import {
  DbmlKind,
} from './kind';
import type {
  DbmlIndex,
} from './tableIndex';
import type {
  DbmlCheck,
} from './check';

export class DbmlTable extends SchemaElement {
  readonly kind = DbmlKind.TABLE;
  schema?: string;
  name: string;
  alias?: string;
  note?: string;
  headerColor?: string;
  columns: DbmlColumn[];
  indexes?: DbmlIndex[];
  checks?: DbmlCheck[];

  constructor (arguments_: {
    schema?: string;
    name: string;
    alias?: string;
    note?: string;
    headerColor?: string;
    columns: DbmlColumn[];
    indexes?: DbmlIndex[];
    checks?: DbmlCheck[];
  }) {
    super();
    this.schema = arguments_.schema;
    this.name = arguments_.name;
    this.alias = arguments_.alias;
    this.note = arguments_.note;
    this.headerColor = arguments_.headerColor;
    this.columns = arguments_.columns;
    this.indexes = arguments_.indexes;
    this.checks = arguments_.checks;
  }

  intern (): string {
    return `${this.kind}:${this.schema ?? DEFAULT_SCHEMA_NAME}.${this.name}`;
  }
}
