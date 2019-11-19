/**
 * Create a Feathers app with authentication for connecting to the web API.
 */

const feathers = require('@feathersjs/feathers')
const auth = require('@feathersjs/authentication-client')
const localStorage = require('localstorage-memory')
const restClient = require('@feathersjs/rest-client')
const axios = require('axios')
const murmurHash3 = require('murmurhash3js')

module.exports = {
  guard(m) {
    return !m.webConnectionError && !m.private.webConnection
  },

  execute(m, { logger }) {
    const cfg = m.$app.get('connections').web
    const connection = {}
    const storageKey = (connection.storageKey = murmurHash3.x86.hash128(
      `${m.key},${cfg.url}`
    ))
    const app = (connection.app = feathers()
      .configure(restClient(cfg.url).axios(axios))
      .configure(
        auth({
          storage: localStorage,
          storageKey
        })
      ))

    connection.authenticate = app.authenticate.bind(null, cfg.auth)

    return connection
  },

  assign(m, res, { logger }) {
    m.private.webConnection = res

    logger.info('Web connection ready')
  }
}
