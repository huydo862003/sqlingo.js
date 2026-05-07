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

  constructor (arguments_: {
    name?: string;
    expression: string;
  }) {
    super();
    this.name = arguments_.name;
    this.expression = arguments_.expression;
  }

  intern (): string {
    return `${this.kind}:${this.name ?? ''}:${this.expression}`;
  }
}
