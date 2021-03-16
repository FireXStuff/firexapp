const GitRevisionPlugin = require('git-revision-webpack-plugin');

module.exports = {
  lintOnSave: process.env.NODE_ENV !== 'production',
  runtimeCompiler: undefined,
  publicPath: './',
  outputDir: undefined,
  assetsDir: undefined,
  productionSourceMap: undefined,
  parallel: undefined,
  css: undefined,
  configureWebpack: {
    plugins: [
      new GitRevisionPlugin(),
    ],
  },
  devServer: {
    proxy: {
      '^/auto': {
        target: 'http://firex-west.cisco.com',
      },
      '^/ws_proxy': {
        target: 'http://firex-west.cisco.com',
      },
      '^/runs': {
        target: 'http://www.firexflame.com',
      },
    },
  },
};
