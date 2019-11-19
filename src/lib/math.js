const { create, all } = require('mathjs')

module.exports = create(
  { all },
  {
    matrix: 'Array',
    number: 'BigNumber',
    precision: 15 // Hardcoded!
  }
)
