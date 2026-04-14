import nuclint from '@hdnax/nuclint';

export default [
  {
    ignores: [
      'upstream/**',
      'node_modules/**',
      'dist/**',
      'doc/**',
    ],
  },
  ...nuclint,
  {
    files: [
      '**/*.ts',
      '**/*.js',
    ],
    rules: {
      '@typescript-eslint/no-non-null-assertion': 'warn',
      '@typescript-eslint/no-use-before-define': 'off',
      'no-use-before-define': 'off',
      '@typescript-eslint/no-extraneous-class': 'off',
      '@typescript-eslint/no-useless-constructor': 'off',
      '@typescript-eslint/no-dynamic-delete': 'off',
      '@typescript-eslint/no-unused-expressions': 'off',
      '@typescript-eslint/unified-signatures': 'off',
    },
  },
  {
    files: [
      'homepage/src/**/*.ts',
      'homepage/src/**/*.vue',
      'homepage/vite.config.ts',
    ],
    rules: {
      'import/named': 'off',
    },
  },
];
