"use strict";

/**
 * Create a NATS client if not defined and connected. Add event listeners to manage state.
 */
const STAN = require('node-nats-streaming');

module.exports = {
  guard(m) {
    return !m.stanError && !m.private.stan && !m.stanConnected;
  },

  execute(m, {
    logger
  }) {
    const cfg = m.$app.get('clients').stan;
    const client = cfg.client.replace(/{([.\w]+)}/g, (_, k) => m[k]);
    const stan = STAN.connect(cfg.cluster, client, cfg.opts || {});
    logger.info('NATS Streaming connecting', {
      client,
      cluster: cfg.cluster
    });
    return new Promise((resolve, reject) => {
      stan.once('connect', () => {
        stan.removeAllListeners();
        resolve(stan);
      });
      stan.once('error', err => {
        stan.removeAllListeners();
        reject(err);
      });
    }).catch(err => {
      logger.error('NATS Streaming connect error', err);
      throw err;
    });
  },

  assign(m, res, {
    logger
  }) {
    res.on('close', () => {
      logger.info('NATS Streaming closed');
      m.stanConnected = false;
    });
    res.on('error', err => {
      logger.error('NATS Streaming error', err);
    });
    m.private.stan = res;
    m.stanConnected = true;
    m.stanTs = m.versionTs;
    logger.info('NATS Streaming connected');
  }

};