/** Finds a matching enum value by case-insensitive string comparison. */
export function enumFromString<T extends string> (
  enumObj: Record<string, T>,
  value: string | undefined,
): T | undefined {
  const values = Object.values(enumObj);
  const lower = value?.toLowerCase();
  return values.find((v) => v === lower) ?? values.find((v) => v.toLowerCase() === lower);
}
