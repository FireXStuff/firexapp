import { defineConfig } from "vite";
import { createVuePlugin as vue } from "vite-plugin-vue2"; //vue 2
import path from 'path';
import { fileURLToPath } from 'url';
import { execFileSync } from 'child_process';
import fs from 'fs';

const dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(dirname, '..');
const packagedUiDir = path.resolve(repoRoot, 'firex_flame_ui');

function commandOutput(command, args) {
  return execFileSync(command, args, {
    cwd: repoRoot,
    encoding: 'utf8',
  }).trim();
}

function createBuildMetadataFiles(isDev, outputDir) {
  return {
    name: 'build-metadata',
    apply: 'build',
    generateBundle() {
      if (!isDev) {
        fs.writeFileSync(
          path.resolve(outputDir, 'COMMITHASH'),
          commandOutput('git', ['rev-parse', 'HEAD']));
        fs.writeFileSync(
          path.resolve(outputDir, 'VERSION'),
          commandOutput('uv', ['version', '--short']));
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
      createBuildMetadataFiles(isDev, outputDir),
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
