// Stub for tests: the real js-untar dereferences URL.createObjectURL at import time,
// which jsdom does not provide. Wired up via the `js-untar` alias in vitest.config.js.
export default function untar() {
  return Promise.resolve([]);
}
