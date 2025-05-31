import js from '@eslint/js'
import tseslint from '@typescript-eslint/eslint-plugin'
import parser from '@typescript-eslint/parser'
import prettierPlugin from 'eslint-plugin-prettier'
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const globals = require('globals');


export default [
  { ignores: ['**/dist/**', '**/node_modules/**'] },
  js.configs.recommended,
  {
    files: ['**/*.ts'],
    languageOptions: {
      parser,
      parserOptions: {
        project: './tsconfig.json', // or tsconfig.base.json
        tsconfigRootDir: process.cwd(),
      },
    },
    plugins: {
      '@typescript-eslint': tseslint,
      prettier: prettierPlugin,
    },
    rules: {
      ...tseslint.configs.recommended.rules,
      ...tseslint.configs['strict-type-checked'].rules,
      'prettier/prettier': 'warn',
    },
  },
  {
    files: ['examples/esm-cli/**/*.ts'],
    languageOptions: {
      globals: {
        ...globals.node
      }
    }
  }
]
