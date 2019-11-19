"use strict";

/**
 * Trigger NATS reconnect if we're supposed to be connected and we're not.
 */
module.exports = {
  guard(m) {
    return !m.stanCheckError && !m.stanCheckReady && m.private.stan && !m.stanConnected;
  },

  execute(m) {
    return true;
  },

  assign(m, res, {
    logger
  }) {
    if (m.private.subscriptions) m.private.subscriptions.forEach(sub => sub.removeAllListeners());
    m.private.stan.removeAllListeners();
    delete m.private.subscriptions;
    delete m.subscriptionsTs;
    delete m.private.stan;
    delete m.stanConnected;
    delete m.stanTs;
    logger.error('NATS Streaming reset');
  }

};