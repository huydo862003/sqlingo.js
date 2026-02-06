export type DataType = string;

export function inferType (value: unknown): DataType {
  throw new Error(`inferType not implemented: ${value}`);
}
