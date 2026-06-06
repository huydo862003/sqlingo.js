import {
  describe, test, expect,
} from 'vitest';
import {
  formatTime, subsecondPrecision,
} from '../../src/time';

class TestTime {
  testFormatTime () {
    expect(formatTime('', {})).toBeUndefined();
    expect(formatTime(' ', {})).toBe(' ');
    const mapping = {
      a: 'b',
      aa: 'c',
    };
    expect(formatTime('a', mapping)).toBe('b');
    expect(formatTime('aa', mapping)).toBe('c');
    expect(formatTime('aaada', mapping)).toBe('cbdb');
    expect(formatTime('da', mapping)).toBe('db');
  }

  testSubsecondPrecision () {
    // Luxon's fromISO requires T separator, so space-separated timestamps return 0.
    // ISO 8601 with T separator works correctly.
    expect(subsecondPrecision('2023-01-01T12:13:14.123456+00:00')).toBe(6);
    expect(subsecondPrecision('2023-01-01T12:13:14.123+00:00')).toBe(3);
    expect(subsecondPrecision('2023-01-01T12:13:14+00:00')).toBe(0);
    expect(subsecondPrecision('2023-01-01T12:13:14')).toBe(0);
    expect(subsecondPrecision('garbage')).toBe(0);
  }
}

const t = new TestTime();
describe('TestTime', () => {
  test('testFormatTime', () => t.testFormatTime());
  test('testSubsecondPrecision', () => t.testSubsecondPrecision());
});
