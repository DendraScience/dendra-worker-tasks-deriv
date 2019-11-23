"use strict";

/**
 * Worker tasks for performing derived calculations.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module dendra-worker-tasks-deriv
 */
// Named exports for convenience
module.exports = {
  build: require('./build'),
  watchInflux: require('./watchInflux')
};