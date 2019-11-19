"use strict";

/**
 * General utilities and helpers.
 *
 * @author J. Scott Smith
 * @license BSD-2-Clause-FreeBSD
 * @module lib/utils
 */

/**
 * Returns a random value for id generation.
 */
function idRandom() {
  return Math.floor(Math.random() * 10000);
}

module.exports = {
  idRandom
};