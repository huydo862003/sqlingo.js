import type {
  DbmlKind,
} from './kind';

export abstract class SchemaElement {
  abstract readonly kind: DbmlKind;
  abstract intern (): string;
}
