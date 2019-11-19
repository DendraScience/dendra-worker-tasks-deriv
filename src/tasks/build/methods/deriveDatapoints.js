const pick = require('lodash/pick')
const derivers = require('../../../lib/derivers')
const { getAuthUser } = require('../../../lib/helpers')

const SPEC_DEFAULTS = {}

async function deriveDatapoints(req, ctx) {
  const { datapointService, influx, logger } = ctx
  const spec = Object.assign({}, SPEC_DEFAULTS, req.spec)
  const {
    database,
    derivation_method: derivationMethod,
    measurement,
    start_time: startTime,
    until_time: untilTime
  } = spec

  if (!(database && derivationMethod && measurement))
    throw new Error('Spec incomplete')

  /*
    Authenticate and/or verify user credentials.
   */

  await getAuthUser(ctx)

  /*
    Ensure target database exists.
   */

  logger.info('Creating database', { database })

  await influx.createDatabase(database)

  /*
    Delete persisted dervied data.
   */

  const queryOptions = {
    database,
    precision: 'ms'
  }
  logger.info('Deleting measurement data', {
    measurement,
    queryOptions,
    startTime,
    untilTime
  })

  await influx.query(
    `DELETE FROM ${measurement} WHERE time >= ${startTime} AND TIME < ${untilTime}`,
    queryOptions
  )

  /*
    Create and run deriver instance.
   */

  logger.info('Running deriver', { derivationMethod })

  const createDeriver = derivers[derivationMethod]

  if (!createDeriver) throw new Error('Derivation method not supported')

  const deriver = createDeriver({ datapointService })
  const writeOptions = {
    database,
    precision: 'ms'
  }

  return deriver(
    spec,
    (timestamp, fields) => ({ timestamp, fields }),
    data => {
      logger.info(`Writing (${data.length}) point(s)`, {
        measurement,
        writeOptions
      })
      return influx.writeMeasurement(measurement, data, writeOptions)
    }
  )
}

module.exports = async (...args) => {
  try {
    return await deriveDatapoints(...args)
  } catch (err) {
    // Wrap errors, ensure they are written to the store
    return {
      error: pick(err, ['code', 'className', 'message', 'type'])
    }
  }
}
