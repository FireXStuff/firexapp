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
      const distDir = path.resolve(__dirname, 'dist');
        writeFileSync(
          path.resolve(distDir, 'COMMITHASH'),
          execSync('git rev-parse HEAD').toString().trim());
        // writeFileSync(
        //     path.resolve(distDir, 'VERSION'),
        //     execSync('git describe --tags --long --dirty --always').toString().trim());
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
    publicDir: './public',
  }
  return config;
}

// https://vitejs.dev/config/
export default defineConfig(createConfig);