const GitRevisionPlugin = require('git-revision-webpack-plugin');

const proxyAuthHeaders = {
  Authorization: 'Bearer PUT_TOKEN_HERE',
  scope: 'openid profile',
  'X-Xsrf-Header': 'cisco.com',
};

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
        target: 'https://firex-west.cisco.com',
        headers: proxyAuthHeaders,
      },
      '^/ws_proxy': {
        target: 'https://firex-west.cisco.com',
        headers: proxyAuthHeaders,
      },
      '^/runs': {
        target: 'https://www.firexflame.com',
      },
    },
  },
};
