import {
  defineConfig,
} from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    setupFiles: [
      './tests/setup.ts',
    ],
    include: [
      './tests/**/*.{test,spec}.?(c|m)[jt]s?(x)',
    ],
    coverage: {
      provider: 'v8',
      reporter: [
        'text',
        'json',
        'html',
      ],
    },
  },
});
