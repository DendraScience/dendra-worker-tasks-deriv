/**
 * Create an InfluxDB client if not defined.
 */

const Influx = require('influx')

module.exports = {
  guard(m) {
    return !m.influxError && !m.private.influx
  },

  execute(m, { logger }) {
    const cfg = m.$app.get('clients').influx
    const influx = new Influx.InfluxDB(cfg)

    logger.info('Influx pinging hosts')

    return influx.ping(5000).then(hosts => ({ hosts, influx }))
  },

  assign(m, res, { logger }) {
    res.hosts.forEach(host => {
      const { url, online, rtt, version } = host
      if (online) {
        logger.info('Influx host is online', { url, rtt, version })
      } else {
        logger.warn('Influx host is OFFLINE', { url, rtt, version })
      }
    })

    m.private.influx = res.influx

    logger.info('Influx ready')
  }
}
