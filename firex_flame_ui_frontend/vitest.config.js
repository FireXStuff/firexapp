/// <reference types="vitest" />

import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import path from 'path';
import { fileURLToPath } from 'url';

const dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  plugins: [ vue() ],
  resolve: {
    alias: {
      // js-untar touches URL.createObjectURL at import time, which jsdom doesn't provide.
      'js-untar': path.resolve(dirname, '__mocks__/js-untar.js'),
    },
  },
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: [
      'vitest.setup.js',
    ],
  },
});
