"use strict";

/**
 * Close NATS if connected and new state is detected.
 */
module.exports = {
  guard(m) {
    return !m.stanCloseError && !m.stanCloseReady && m.private.stan && m.stanConnected && m.versionTs > m.stanTs;
  },

  execute(m, {
    logger
  }) {
    logger.info('NATS Streaming closing');
    m.private.stan.close();
    return true;
  }

};