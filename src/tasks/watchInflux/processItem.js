/**
 * Process an individual message.
 */

const { getAuthUser } = require('../../lib/helpers')
const { idRandom } = require('../../lib/utils')

async function processItem({ data, dataObj, msgSeq }, ctx) {
  const { datastreamService, derivedBuildService, logger, subSubject } = ctx
  try {
    /*
      Validate inbound message data.
     */

    if (!dataObj.context) throw new Error('Missing context object')
    if (!dataObj.payload) throw new Error('Missing payload object')

    const { options, changes } = dataObj.payload

    if (typeof options !== 'object') throw new Error('Invalid payload.options')
    if (!Array.isArray(changes)) throw new Error('Invalid payload.changes')

    const { org_slug: orgSlug } = dataObj.context
    const { database } = options

    /*
      Authenticate and/or verify user credentials.
     */

    await getAuthUser(ctx)

    /*
      Process each change.
     */

    logger.info(`Processing (${changes.length}) changes`)

    for (let i = 0; i < changes.length; i++) {
      const change = changes[i]
      const changeId = `${change.msgSeq}-${i}`

      /*
        Fetch source datastreams.
       */

      // HACK: Makes assumptions about how the driver is configured
      // TODO: An index should be defined!
      const query = {
        'datapoints_config_built.params.query.api': orgSlug,
        'datapoints_config_built.params.query.db': database,
        'datapoints_config_built.params.query.fc': change.measurement,
        'datapoints_config_built.path': '/influx/select',
        is_enabled: true,
        source_type: 'sensor',
        $limit: 2000, // FIX: Implement unbounded find or pagination???
        $sort: {
          _id: 1 // ASC
        }
      }

      logger.info('Finding source datastreams', { query })

      const datastreamRes = await datastreamService.find({ query })
      const datastreams = datastreamRes.data || []
      const datastreamIds = datastreams.map(doc => doc._id)

      /*
        Dispatch a processDatastream build request.
       */

      if (datastreamIds.length) {
        const method = 'processDatastream'
        const now = new Date()
        const buildId = `${method}-${orgSlug}-${changeId}-${now.getTime()}-${idRandom()}`
        const buildSpec = {
          change,
          change_id: changeId,
          datastream_ids: datastreamIds
        }

        logger.info('Dispatching build', { buildId })

        await derivedBuildService.create({
          _id: buildId,
          method,
          dispatch_at: now,
          dispatch_key: orgSlug,
          expires_at: new Date(now.getTime() + 86400000), // 24 hours from now
          spec: buildSpec
        })
      }
    }

    logger.info('Changes processed', { msgSeq, subSubject })
  } catch (err) {
    logger.error('Processing error', { msgSeq, subSubject, err, dataObj })
  }
}

module.exports = processItem
