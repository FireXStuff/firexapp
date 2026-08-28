import { defineConfig } from "vite";
import { createVuePlugin as vue } from "vite-plugin-vue2"; //vue 2
import path from 'path';
import { fileURLToPath } from 'url';
import { execSync } from 'child_process';
import fs from 'fs';

const dirname = path.dirname(fileURLToPath(import.meta.url));
const packagedUiDir = path.resolve(dirname, '..', 'firex_flame_ui');

function createCommitHashFile(isDev, outputDir) {
  return {
    name: 'commithash',
    apply: 'build',
    generateBundle() {
      if (!isDev) {
        fs.writeFileSync(
          path.resolve(outputDir, 'COMMITHASH'),
          execSync('git rev-parse HEAD', { cwd: dirname }).toString().trim());
        fs.writeFileSync(
          path.resolve(outputDir, 'VERSION'),
          execSync('git describe --tags --always', { cwd: dirname })
            .toString()
            .trim());
      }
    }
  };
}

function createConfig(ctx) {
  const isDev = ctx.mode === 'dev-build';
  // Production builds replace the checked-in compatibility assets consumed
  // by FireXApp's Python package. Development builds remain isolated under
  // this frontend tree.
  const outputDir = isDev ? path.resolve(dirname, 'dist') : packagedUiDir;
  const config = {
    define: {},
    plugins: [
      vue(),
      createCommitHashFile(isDev, outputDir),
    ],
    base: isDev ? path.join(dirname, 'dist/') : '/flame/',
    publicDir: './public',
    build: {
      outDir: outputDir,
      emptyOutDir: true,
    },
    server: {
      proxy: {
        '^/auto/firex-logs.*': {
          target: 'http://localhost:3000',
          configure: (proxy, options) => {
            proxy.on('proxyReq', (proxyReq, req, res) => {
              const filePath = req.url;
              if (fs.existsSync(filePath)) {
                res.writeHead(200);
                fs.createReadStream(filePath).pipe(res);
              } else {
                res.writeHead(404);
                res.end('File not found');
              }
            });
          },
        }
      }
    },
  }
  return config;
}

// https://vitejs.dev/config/
export default defineConfig(createConfig);
