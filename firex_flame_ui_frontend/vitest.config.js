/// <reference types="vitest" />

import { defineConfig } from 'vite'
import { createVuePlugin as Vue2 } from 'vite-plugin-vue2'

export default defineConfig({
  plugins: [ Vue2() ],
  // base: './',
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: [
      'vitest.setup.js',
    ],
  },
});
