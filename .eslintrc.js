// SEE: http://eslint.org/docs/user-guide/configuring
module.exports = {
  root: true,
  env: {
    node: true
  },
  extends: ['standard', 'prettier', 'prettier/standard'],
  plugins: ['import', 'prettier', 'standard'],
  parserOptions: {
    sourceType: 'module'
  },
  rules: {
    'prettier/prettier': 'error'
  }
}
