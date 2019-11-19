"use strict";

module.exports = {
  influx: require('./tasks/influx'),
  sources: require('./tasks/sources'),
  stan: require('./tasks/stan'),
  stanCheck: require('./tasks/stanCheck'),
  stanClose: require('./tasks/stanClose'),
  subscriptions: require('./tasks/build/subscriptions'),
  versionTs: require('./tasks/versionTs'),
  webConnection: require('./tasks/webConnection')
};