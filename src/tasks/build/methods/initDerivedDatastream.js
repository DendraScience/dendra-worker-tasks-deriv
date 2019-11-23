/**
 * Method to initialize a dervied datastream's configuration and data.
 */

const pick = require('lodash/pick')
const derivers = require('../../../lib/derivers')
const { getAuthUser } = require('../../../lib/helpers')
const { idRandom } = require('../../../lib/utils')
const { DateTime } = require('luxon')

const DATE_TIME_OPTS = {
  zone: 'utc'
}

// Reasonable min and max dates to perform low-level querying
// NOTE: Didn't use min/max integer since db date conversion could choke
// NOTE: Revised to be within InfluxDB default dates
const MIN_TIME = Date.UTC(1800, 1, 2)
const MAX_TIME = Date.UTC(2200, 1, 2)
const MIN_DATE_TIME = DateTime.fromMillis(MIN_TIME, DATE_TIME_OPTS)
const MAX_DATE_TIME = DateTime.fromMillis(MAX_TIME, DATE_TIME_OPTS)

const SPEC_DEFAULTS = {}

async function initDerivedDatastream(req, ctx) {
  const {
    datapointService,
    datastreamService,
    derivedBuildService,
    influx,
    logger,
    stationService
  } = ctx
  const spec = Object.assign({}, SPEC_DEFAULTS, req.spec)
  const { change, datastream } = spec

  if (!datastream) throw new Error('Spec incomplete')

  /*
    Authenticate and/or verify user credentials.
   */

  await getAuthUser(ctx)

  /*
    Fetch source datastreams.
   */

  let query = {
    _id: {
      $in: datastream.derived_from_datastream_ids
    },
    is_enabled: true,
    source_type: 'sensor',
    $limit: 2000, // FIX: Implement unbounded find or pagination???
    $sort: {
      _id: 1 // ASC
    }
  }

  logger.info('Finding source datastreams', { query })

  const datastreamRes = await datastreamService.find({ query })

  /*
    Create and init deriver instance.
   */

  const derivationMethod = datastream.derivation_method

  logger.info('Initializing deriver', { derivationMethod })

  const createDeriver = derivers[derivationMethod]

  if (!createDeriver) throw new Error('Derivation method not supported')

  const deriver = createDeriver({ datapointService })
  const database = datastream.organization_id
    ? `derived_org_${datastream.organization_id}`
    : 'derived_default'
  const measurement = `derived_data_${datastream._id}`

  let deriverSpecs
  let patchRes

  if (change) {
    /*
      Determine update time based on the change extents. Init the deriver.
     */

    // Changes are in UTC, so fetch the station and do a quick convert
    logger.info('Getting station', { _id: datastream.station_id })
    const station = await stationService.get(datastream.station_id)

    deriverSpecs = await deriver({
      derived_datastream: datastream,
      source_datastreams: datastreamRes.data,
      update_time: change.timeMin + (station.utc_offset | 0) * 1000
    })

    logger.info('Deriver specs returned', { deriverSpecs })
  } else {
    /*
      Init the deriver.
     */

    deriverSpecs = await deriver({
      derived_datastream: datastream,
      source_datastreams: datastreamRes.data
    })

    logger.info('Deriver specs returned', { deriverSpecs })

    /*
      Patch the datastream with a built config.
     */

    const config = [
      {
        begins_at: MIN_DATE_TIME.toISO(),
        ends_before: MAX_DATE_TIME.toISO(),
        params: {
          query: {
            db: database,
            fc: measurement,
            coalesce: false,
            // NOTE: For now assume all dervied datastreams are in local time
            local: true
          }
        },
        path: '/influx/select'
      }
    ]
    query = {
      source_type: 'deriver'
    }

    logger.info('Patching derived datastream', { _id: datastream._id, query })

    patchRes = await datastreamService.patch(
      datastream._id,
      {
        $set: { datapoints_config: config, datapoints_config_built: config }
      },
      { query }
    )

    /*
      Delete all persisted dervied data.
     */

    const queryOptions = {
      database,
      precision: 'ms'
    }
    logger.info('Deleting all measurement data', { measurement, queryOptions })

    try {
      await influx.query(`DELETE FROM ${measurement}`, queryOptions)
    } catch (err) {
      if (err.message.includes('database not found')) logger.warn(err.message)
      else throw err
    }
  }

  /*
    Dispatch deriveDatapoints build requests.
   */

  for (const deriverSpec of deriverSpecs) {
    const method = 'deriveDatapoints'
    const now = new Date()
    const { _id: id } = datastream
    const buildId = `${method}-${id}-${now.getTime()}-${idRandom()}`
    const buildSpec = Object.assign(
      {
        database,
        measurement,
        derivation_method: derivationMethod
      },
      deriverSpec
    )

    logger.info('Dispatching build', { buildId })

    await derivedBuildService.create({
      _id: buildId,
      method,
      dispatch_at: now,
      dispatch_key: id,
      expires_at: new Date(now.getTime() + 86400000), // 24 hours from now
      spec: buildSpec
    })
  }

  return { change, patchRes }
}

module.exports = async (...args) => {
  try {
    return await initDerivedDatastream(...args)
  } catch (err) {
    // Wrap errors, ensure they are written to the store
    return {
      error: pick(err, ['code', 'className', 'message', 'type'])
    }
  }
}
