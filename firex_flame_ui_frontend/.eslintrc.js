
module.exports = {
  env: {
    node: true,
  },
  extends: [
    'eslint:recommended',
    'plugin:vue/essential',
  ],
  rules: {
    "no-param-reassign": [
      2,
      {
        "props": false
      }
    ],
    "prefer-destructuring": [
      "error",
      {
        "array": false
      }
    ]
  }
}