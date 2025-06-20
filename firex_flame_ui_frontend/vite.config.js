import { defineConfig } from "vite";
import { createVuePlugin as vue } from "vite-plugin-vue2"; //vue 2
import path from 'path';
import { fileURLToPath } from 'url';
import { execSync } from 'child_process';
import fs from 'fs';

const dirname = path.dirname(fileURLToPath(import.meta.url));

function createCommitHashFile(isDev) {
  return {
    name: 'commithash',
    apply: 'build',
    generateBundle() {
      if (!isDev) {
        const distDir = path.resolve(dirname, 'dist');
          fs.writeFileSync(
            path.resolve(distDir, 'COMMITHASH'),
            execSync('git rev-parse HEAD').toString().trim());
          fs.writeFileSync(
              path.resolve(distDir, 'VERSION'),
              execSync('git describe --tags --always').toString().trim());
      }
    }
  };
}

function createConfig(ctx) {
  const isDev = ctx.mode === 'dev-build';
  const config = {
    define: {},
    plugins: [
      vue(),
      createCommitHashFile(isDev),
    ],
    base: isDev ? path.join(dirname, 'dist/') : '/flame/',
    publicDir: './public',
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