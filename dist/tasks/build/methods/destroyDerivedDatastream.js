"use strict";

/**
 * Method to destroy a dervied datastream's data.
 */
const pick = require('lodash/pick');

const SPEC_DEFAULTS = {};

async function destroyDerivedDatastream(req, ctx) {
  const {
    influx,
    logger
  } = ctx;
  const spec = Object.assign({}, SPEC_DEFAULTS, req.spec);
  const {
    datastream
  } = spec;
  if (!datastream) throw new Error('Spec incomplete');
  /*
    Delete all persisted dervied data.
   */

  const database = datastream.organization_id ? `derived_org_${datastream.organization_id}` : 'derived_default';
  const measurement = `derived_data_${datastream._id}`;
  logger.info('Dropping measurement', {
    database,
    measurement
  });

  try {
    await influx.dropMeasurement(measurement, database);
  } catch (err) {// if (err.message.includes('database not found')) logger.warn(err.message)
    // else throw err
  }

  return {
    database,
    measurement
  };
}

module.exports = async (...args) => {
  try {
    return await destroyDerivedDatastream(...args);
  } catch (err) {
    // Wrap errors, ensure they are written to the store
    return {
      error: pick(err, ['code', 'className', 'message', 'type'])
    };
  }
};