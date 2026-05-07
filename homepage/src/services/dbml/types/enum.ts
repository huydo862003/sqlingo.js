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

  constructor (arguments_: {
    name: string;
    note?: string;
  }) {
    super();
    this.name = arguments_.name;
    this.note = arguments_.note;
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

  constructor (arguments_: {
    schema?: string;
    name: string;
    values: DbmlEnumValue[];
    note?: string;
  }) {
    super();
    this.schema = arguments_.schema;
    this.name = arguments_.name;
    this.values = arguments_.values;
    this.note = arguments_.note;
  }

  intern (): string {
    return `${this.kind}:${this.schema ?? DEFAULT_SCHEMA_NAME}.${this.name}`;
  }
}
