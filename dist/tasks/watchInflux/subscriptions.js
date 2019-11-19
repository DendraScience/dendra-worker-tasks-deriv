"use strict";

/**
 * Subscribe to subjects after connected. Add an event listener for messages.
 */
const processItem = require('./processItem');

function handleMessage(msg) {
  const {
    logger,
    m,
    subSubject
  } = this;

  if (!msg) {
    logger.error('Message undefined');
    return;
  }

  const msgSeq = msg.getSequence();
  logger.info('Message received', {
    msgSeq,
    subSubject
  });

  if (m.subscriptionsTs !== m.versionTs) {
    logger.info('Message deferred', {
      msgSeq,
      subSubject
    });
    return;
  } // DEBUG: Memory usage
  // let heap1
  // let heap2


  try {
    const data = msg.getData();
    const dataObj = JSON.parse(data); // DEBUG: Memory usage
    // heap1 = process.memoryUsage().heapUsed

    processItem({
      data,
      dataObj,
      msgSeq
    }, this).then(() => msg.ack()).catch(err => {
      logger.error('Message processing error', {
        msgSeq,
        subSubject,
        err,
        dataObj
      });
    }); // DEBUG: Memory usage
    // }).finally(() => {
    //   global.gc(true)
    //   heap2 = process.memoryUsage().heapUsed
    //   console.log('HEAP1', heap1)
    //   console.log('HEAP2', heap2)
    //   console.log('HEAPD', heap2 - heap1)
    // })
  } catch (err) {
    logger.error('Message error', {
      msgSeq,
      subSubject,
      err
    });
  }
}

module.exports = {
  guard(m) {
    return !m.subscriptionsError && m.private.dispatchConnection && m.private.webConnection && m.private.stan && m.stanConnected && m.private.influx && m.sourcesTs === m.versionTs && m.subscriptionsTs !== m.versionTs && !m.private.subscriptions;
  },

  execute(m, {
    logger
  }) {
    const {
      stan,
      dispatchConnection,
      webConnection
    } = m.private;
    const {
      authenticate
    } = webConnection;
    const {
      passport
    } = webConnection.app;
    const datastreamService = webConnection.app.service('/datastreams');
    const derivedBuildService = dispatchConnection.app.service('/derived-builds');
    const userService = webConnection.app.service('/users');
    const subs = [];
    m.sourceKeys.forEach(sourceKey => {
      const source = m.sources[sourceKey];
      const {
        influx_api: influxAPI,
        sub_options: subOptions,
        sub_to_subject: subSubject
      } = source;

      try {
        const opts = stan.subscriptionOptions();
        opts.setManualAckMode(true);
        opts.setDeliverAllAvailable();
        opts.setMaxInFlight(1);

        if (subOptions) {
          if (typeof subOptions.ack_wait === 'number') opts.setAckWait(subOptions.ack_wait);
          if (typeof subOptions.durable_name === 'string') opts.setDurableName(subOptions.durable_name);
        }

        const sub = stan.subscribe(subSubject, opts);
        sub.on('message', handleMessage.bind({
          authenticate,
          datastreamService,
          derivedBuildService,
          influxAPI,
          logger,
          m,
          passport,
          stan,
          subSubject,
          userService
        }));
        subs.push(sub);
      } catch (err) {
        logger.error('Subscription error', {
          err,
          sourceKey,
          subSubject
        });
      }
    });
    return subs;
  },

  assign(m, res, {
    logger
  }) {
    m.private.subscriptions = res;
    m.subscriptionsTs = m.versionTs;
    logger.info('Subscriptions ready');
  }

};