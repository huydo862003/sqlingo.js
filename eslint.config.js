import nuclint from '@hdnax/nuclint';

export default [
  {
    // Additional ignores specific to this project
    ignores: [
      'upstream/**',
      'node_modules',
      'dist',
      'doc',
    ],
  },
  ...nuclint,
  {
    // Override for this project - we're not using Vue
    files: [
      '**/*.ts',
      '**/*.js',
    ],
    rules: {
      // Add any project-specific rule overrides here
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
];
