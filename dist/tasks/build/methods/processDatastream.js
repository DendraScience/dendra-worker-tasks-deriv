"use strict";

/**
 * Method to dispatch derived builds given source datastream(s).
 */
const pick = require('lodash/pick');

const {
  getAuthUser
} = require('../../../lib/helpers');

const {
  idRandom
} = require('../../../lib/utils');

const SPEC_DEFAULTS = {};

async function processDatastream(req, ctx) {
  const {
    datastreamService,
    derivedBuildService,
    logger
  } = ctx;
  const spec = Object.assign({}, SPEC_DEFAULTS, req.spec);
  const {
    change,
    change_id: changeId,
    datastream: sourceDatastream,
    datastream_ids: sourceDatastreamIds
  } = spec;
  if (!(sourceDatastream || sourceDatastreamIds)) throw new Error('Spec incomplete');
  /*
    Authenticate and/or verify user credentials.
   */

  await getAuthUser(ctx);
  /*
    Fetch derived datastreams that reference the source datastream.
   */

  const query = {
    derived_from_datastream_ids: {
      $in: sourceDatastream ? [sourceDatastream._id] : sourceDatastreamIds
    },
    is_enabled: true,
    source_type: 'deriver',
    $limit: 2000,
    // FIX: Implement unbounded find or pagination
    $sort: {
      _id: 1 // ASC

    }
  };
  logger.info('Finding derived datastreams', {
    query
  });
  const datastreamRes = await datastreamService.find({
    query
  });
  const datastreams = datastreamRes.data || [];
  const datastreamIds = datastreams.map(doc => doc._id);
  logger.info(`Processing (${datastreams.length}) dervied datastreams`);
  /*
    Dispatch initDerivedDatastream build requests.
   */

  for (const datastream of datastreams) {
    const method = 'initDerivedDatastream';
    const now = new Date();
    const {
      _id: id
    } = datastream;
    let buildId;
    let buildSpec;

    if (change) {
      buildId = `${method}-${id}-${changeId}-${now.getTime()}-${idRandom()}`;
      buildSpec = {
        change,
        datastream
      };
    } else {
      buildId = `${method}-${id}-${now.getTime()}-${idRandom()}`;
      buildSpec = {
        datastream
      };
    }

    logger.info('Dispatching build', {
      buildId
    });
    await derivedBuildService.create({
      _id: buildId,
      method,
      dispatch_at: now,
      dispatch_key: id,
      expires_at: new Date(now.getTime() + 86400000),
      // 24 hours from now
      spec: buildSpec
    });
  }

  return {
    datastreamIds
  };
}

module.exports = async (...args) => {
  try {
    return await processDatastream(...args);
  } catch (err) {
    // Wrap errors, ensure they are written to the store
    return {
      error: pick(err, ['code', 'className', 'message', 'type'])
    };
  }
};