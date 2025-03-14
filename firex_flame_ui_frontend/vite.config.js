import { defineConfig } from "vite";
import { createVuePlugin as vue } from "vite-plugin-vue2"; //vue 2
import GitRevision from 'vite-plugin-git-revision';
import path from 'path';
import { fileURLToPath } from 'url';

const dirname = path.dirname(fileURLToPath(import.meta.url));

function createConfig(ctx) {
  const config = {
    define: {},
    plugins: [ vue(), GitRevision() ],
    base: ctx.mode === 'dev-build' ? path.join(dirname, 'dist') : '/flame/',
  }
  return config;
}

// https://vitejs.dev/config/
export default defineConfig(createConfig);