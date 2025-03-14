import { defineConfig } from "vite";
import { createVuePlugin as vue } from "vite-plugin-vue2"; //vue 2
import path from 'path';
import { fileURLToPath } from 'url';

import { execSync } from 'child_process';
import { writeFileSync } from 'fs';

function createCommitHashFile() {
  return {
    name: 'commithash',
    apply: 'build',
    generateBundle() {
        writeFileSync(
          path.resolve(__dirname, 'dist', 'COMMITHASH'),
          execSync('git rev-parse HEAD').toString().trim());
    }
  };
}

const dirname = path.dirname(fileURLToPath(import.meta.url));

function createConfig(ctx) {
  const config = {
    define: {},
    plugins: [
      vue(),
      createCommitHashFile(),
    ],
    base: ctx.mode === 'dev-build' ? path.join(dirname, 'dist') : '/flame/',
  }
  return config;
}

// https://vitejs.dev/config/
export default defineConfig(createConfig);