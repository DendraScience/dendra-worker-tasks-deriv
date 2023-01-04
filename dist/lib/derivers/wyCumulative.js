"use strict";

/**
 * WY cumulative dervier.
 */
const math = require('../math');

const {
  DateTime
} = require('luxon');

const DATE_TIME_OPTS = {
  zone: 'utc'
};

function toWYDateTime(time) {
  return DateTime.fromMillis(time, DATE_TIME_OPTS).startOf('month').minus({
    months: 9
  }).startOf('year').plus({
    months: 9
  });
}

function defaultPointCb(timestamp, fields) {
  return {
    timestamp,
    fields
  };
}

async function init({
  datapointService
}, {
  derived_datastream: derivedDatastream,
  source_datastreams: sourceDatastreams,
  update_time: updateTime
}) {
  const specs = [];
  if (sourceDatastreams && sourceDatastreams.length !== 1) return specs;
  const derivedDatastreamId = derivedDatastream._id;
  const sourceDatastream = sourceDatastreams[0];
  const sourceDatastreamId = sourceDatastream._id;

  if (updateTime !== undefined) {
    /*
      Return a spec for the WY impacted by the given updateTime.
     */
    specs.push({
      derived_datastream_id: derivedDatastreamId,
      source_datastream_id: sourceDatastreamId,
      start_time: toWYDateTime(updateTime).toMillis(),
      until_time: toWYDateTime(updateTime).plus({
        year: 1
      }).toMillis(),
      update_time: updateTime
    });
    return specs;
  }
  /*
    Find the first and last datapoint, return a spec for each WY in-between.
   */


  const firstDatapoint = await datapointService.find({
    query: {
      datastream_id: sourceDatastreamId,
      t_int: true,
      t_local: true,
      $limit: 1,
      $sort: {
        time: 1 // ASC

      }
    }
  });
  if (!(firstDatapoint.data && firstDatapoint.data.length)) return specs;
  const lastDatapoint = await datapointService.find({
    query: {
      datastream_id: sourceDatastreamId,
      t_int: true,
      t_local: true,
      $limit: 1,
      $sort: {
        time: -1 // DESC

      }
    }
  });
  if (!(lastDatapoint.data && lastDatapoint.data.length)) return specs;
  const firstDateTime = toWYDateTime(firstDatapoint.data[0].lt);
  const lastDateTime = toWYDateTime(lastDatapoint.data[0].lt);
  let currentDateTime = firstDateTime;

  while (currentDateTime.toMillis() <= lastDateTime.toMillis()) {
    const nextDateTime = currentDateTime.plus({
      year: 1
    });
    specs.push({
      derived_datastream_id: derivedDatastreamId,
      source_datastream_id: sourceDatastreamId,
      start_time: currentDateTime.toMillis(),
      until_time: nextDateTime.toMillis()
    });
    currentDateTime = nextDateTime;
  }

  return specs;
}

async function run({
  datapointService
}, {
  derived_datastream_id: derivedDatastreamId,
  source_datastream_id: sourceDatastreamId,
  start_time: startTime,
  until_time: untilTime,
  update_time: updateTime
}, pointCb, dataCb) {
  let fromTime = startTime;
  let count = 0;
  let pages = 0;
  let vSum = math.bignumber(0);

  if (updateTime !== undefined) {
    /*
      Start with a previosuly derived datapoint if it exists.
     */
    const priorDatapoint = await datapointService.find({
      query: {
        datastream_id: derivedDatastreamId,
        t_int: true,
        t_local: true,
        time_local: true,
        time: {
          $gte: startTime,
          $lt: updateTime
        },
        $limit: 1,
        $sort: {
          time: -1 // DESC

        }
      }
    });

    if (priorDatapoint.data && priorDatapoint.data.length) {
      fromTime = updateTime;
      vSum = math.bignumber(priorDatapoint.data[0].v);
    }
  }
  /*
    Fetch source datapoints, derive and transform.
   */


  if (!pointCb) pointCb = defaultPointCb;

  while (true) {
    const datapoints = await datapointService.find({
      query: {
        datastream_id: sourceDatastreamId,
        t_int: true,
        t_local: true,
        time_local: true,
        time: {
          [pages > 0 ? '$gt' : '$gte']: fromTime,
          $lt: untilTime
        },
        $limit: 2016,
        $sort: {
          time: 1 // ASC

        }
      }
    });
    if (!(datapoints.data && datapoints.data.length)) break;
    const data = [];
    const length = datapoints.data.length;
    count += length;

    for (let i = 0; i < length; i++) {
      const point = datapoints.data[i];
      /* eslint-disable-next-line no-console */

      console.log('vSum, point.v', {
        vSum,
        v: point.v
      });
      vSum = math.add(vSum, math.bignumber(point.v));
      data.push(pointCb(point.lt, {
        utc_offset: point.o,
        value: math.number(vSum)
      }));
    }

    await dataCb(data);
    fromTime = datapoints.data[length - 1].lt;
    pages++;
  }

  return {
    count,
    pages,
    from_time: fromTime,
    start_time: startTime,
    until_time: untilTime,
    update_time: updateTime,
    v_sum: vSum
  };
}

module.exports = ctx => {
  return async (opts, pointCb, dataCb) => dataCb ? run(ctx, opts, pointCb, dataCb) : init(ctx, opts);
};