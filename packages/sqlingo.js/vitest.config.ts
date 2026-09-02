import {
  defineConfig,
} from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    testTimeout: 30000,
    setupFiles: ['./tests/setup.ts'],
    include: ['./tests/**/*.{test,spec}.?(c|m)[jt]s?(x)'],
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
