import {
  DEFAULT_SCHEMA_NAME,
} from '../constants';
import {
  SchemaElement,
} from './element';
import {
  DbmlKind,
} from './kind';

export class DbmlEnumValue extends SchemaElement {
  readonly kind = DbmlKind.ENUM_VALUE;
  name: string;
  note?: string;

  constructor (args: {
    name: string;
    note?: string;
  }) {
    super();
    this.name = args.name;
    this.note = args.note;
  }

  intern (): string {
    return `${this.kind}:${this.name}`;
  }
}

export class DbmlEnum extends SchemaElement {
  readonly kind = DbmlKind.ENUM;
  schema?: string;
  name: string;
  values: DbmlEnumValue[];
  note?: string;

  constructor (args: {
    schema?: string;
    name: string;
    values: DbmlEnumValue[];
    note?: string;
  }) {
    super();
    this.schema = args.schema;
    this.name = args.name;
    this.values = args.values;
    this.note = args.note;
  }

  intern (): string {
    return `${this.kind}:${this.schema ?? DEFAULT_SCHEMA_NAME}.${this.name}`;
  }
}
