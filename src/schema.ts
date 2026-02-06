export class Schema {
  // Base schema class
}

export class MappingSchema extends Schema {
  constructor (_mapping?: Record<string, unknown>) {
    super();
    throw new Error('MappingSchema not implemented');
  }
}
