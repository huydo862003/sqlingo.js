/** Finds a matching enum value by case-insensitive string comparison. */
export function enumFromString<T extends string> (
  enumObj: Record<string, T>,
  value: string,
): T | undefined {
  const lower = value.toLowerCase();
  return Object.values(enumObj).find((v) => v.toLowerCase() === lower);
}
