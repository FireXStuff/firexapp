import { defineConfig } from "vite";
import { createVuePlugin as vue } from "vite-plugin-vue2"; //vue 2
import fs from 'fs';
import { resolve } from 'path'

function createConfig(ctx) {
  const config = {
    plugins: [vue()],
    base: '/flame/',
  }
  return config;
}

// https://vitejs.dev/config/
export default defineConfig(createConfig);