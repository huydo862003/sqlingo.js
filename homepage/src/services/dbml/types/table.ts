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

  constructor (args: {
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
    this.schema = args.schema;
    this.name = args.name;
    this.alias = args.alias;
    this.note = args.note;
    this.headerColor = args.headerColor;
    this.columns = args.columns;
    this.indexes = args.indexes;
    this.checks = args.checks;
  }

  intern (): string {
    return `${this.kind}:${this.schema ?? DEFAULT_SCHEMA_NAME}.${this.name}`;
  }
}
