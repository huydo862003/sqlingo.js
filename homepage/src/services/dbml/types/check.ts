import {
  SchemaElement,
} from './element';
import {
  DbmlKind,
} from './kind';

export class DbmlCheck extends SchemaElement {
  readonly kind = DbmlKind.CHECK;
  name?: string;
  expression: string;

  constructor (args: {
    name?: string;
    expression: string;
  }) {
    super();
    this.name = args.name;
    this.expression = args.expression;
  }

  intern (): string {
    return `${this.kind}:${this.name ?? ''}:${this.expression}`;
  }
}
