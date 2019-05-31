var GitRevisionPlugin = require('git-revision-webpack-plugin');

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
};
