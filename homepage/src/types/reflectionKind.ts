// mirrors typedoc's ReflectionKind enum
export const ReflectionKind = {
  Project: 1,
  Module: 2,
  Namespace: 4,
  Enum: 8,
  EnumMember: 16,
  Variable: 32,
  Function: 64,
  Class: 128,
  Interface: 256,
  Constructor: 512,
  Property: 1024,
  Method: 2048,
  Accessor: 262144,
  TypeAlias: 2097152,
  TypeParameter: 131072,
} as const;

export type ReflectionKind = typeof ReflectionKind[keyof typeof ReflectionKind];
