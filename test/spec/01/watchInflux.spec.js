/**
 * Tests for build tasks
 */

const feathers = require('@feathersjs/feathers')
const auth = require('@feathersjs/authentication-client')
const localStorage = require('localstorage-memory')
const restClient = require('@feathersjs/rest-client')
const axios = require('axios')
const murmurHash3 = require('murmurhash3js')

describe('watchInflux tasks', function() {
  this.timeout(180000)

  const now = new Date()
  const model = {
    props: {},
    state: {
      _id: 'taskMachine-watchInflux-current',
      source_defaults: {
        some_default: 'default'
      },
      sources: [
        {
          description:
            'Watch for Influx DB changes and dispatch derived updates',
          sub_options: {
            ack_wait: 3600000,
            durable_name: '20181223'
          },
          sub_to_subject: 'abcd.influxChange.v2.log'
        }
      ],
      created_at: now,
      updated_at: now
    }
  }

  // const requestSubject = 'dendra.derivedBuild.v2.req.0'
  const testName = 'dendra-worker-tasks-deriv UNIT_TEST'

  const id = {}
  const webConnection = {}

  const authWebConnection = async () => {
    const cfg = main.app.get('connections').web
    const storageKey = (webConnection.storageKey = murmurHash3.x86.hash128(
      `TEST,${cfg.url}`
    ))
    const app = (webConnection.app = feathers()
      .configure(restClient(cfg.url).axios(axios))
      .configure(
        auth({
          storage: localStorage,
          storageKey
        })
      ))

    await app.authenticate(cfg.auth)
  }
  const removeDocuments = async (path, query) => {
    const res = await webConnection.app.service(path).find({ query })

    for (const doc of res.data) {
      await webConnection.app.service(path).remove(doc._id)
    }
  }
  const cleanup = async () => {
    await removeDocuments('/datastreams', {
      description: testName
    })
    await removeDocuments('/stations', {
      name: testName
    })
    await removeDocuments('/organizations', {
      name: testName
    })
  }

  Object.defineProperty(model, '$app', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: main.app
  })
  Object.defineProperty(model, 'key', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: 'build'
  })
  Object.defineProperty(model, 'private', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: {}
  })

  let tasks
  let machine

  before(async function() {
    await authWebConnection()
    await cleanup()

    id.org = (
      await webConnection.app.service('/organizations').create({
        name: testName
      })
    )._id

    id.station = (
      await webConnection.app.service('/stations').create({
        is_active: true,
        is_enabled: true,
        is_stationary: true,
        name: testName,
        organization_id: id.org,
        station_type: 'weather',
        time_zone: 'PST',
        utc_offset: -28800
      })
    )._id

    id.datastream = (
      await webConnection.app.service('/datastreams').create({
        datapoints_config: [],
        datapoints_config_built: [
          {
            params: {
              query: {
                api: 'abcd',
                db: 'station_database',
                fc: 'source_measurement',
                sc: '"time", "value"',
                utc_offset: -28800,
                coalesce: false
              }
            },
            begins_at: '2018-04-25T00:00:00.000Z',
            ends_before: '2018-04-26T00:00:00.000Z',
            path: '/influx/select'
          }
        ],
        description: testName,
        is_enabled: true,
        name: testName,
        organization_id: id.org,
        source_type: 'sensor',
        station_id: id.station,
        terms: {}
      })
    )._id

    id.derivedDatastream = (
      await webConnection.app.service('/datastreams').create({
        derivation_method: 'wyCumulative',
        derived_from_datastream_ids: [id.datastream],
        description: testName,
        is_enabled: true,
        name: testName,
        organization_id: id.org,
        source_type: 'deriver',
        station_id: id.station,
        terms: {}
      })
    )._id
  })

  after(async function() {
    await cleanup()

    await Promise.all([
      model.private.stan
        ? new Promise((resolve, reject) => {
            model.private.stan.removeAllListeners()
            model.private.stan.once('close', resolve)
            model.private.stan.once('error', reject)
            model.private.stan.close()
          })
        : Promise.resolve()
    ])
  })

  it('should import', function() {
    tasks = require('../../../dist').build

    expect(tasks).to.have.property('sources')
  })

  it('should create machine', function() {
    machine = new tm.TaskMachine(model, tasks, {
      helpers: {
        logger: console
      },
      interval: 500
    })

    expect(machine).to.have.property('model')
  })

  it('should run', function() {
    model.scratch = {}

    return machine
      .clear()
      .start()
      .then(success => {
        /* eslint-disable-next-line no-unused-expressions */
        expect(success).to.be.true

        // Verify task state
        expect(model).to.have.property('influxReady', true)
        expect(model).to.have.property('sourcesReady', true)
        expect(model).to.have.property('stanCheckReady', false)
        expect(model).to.have.property('stanCloseReady', false)
        expect(model).to.have.property('stanReady', true)
        expect(model).to.have.property('subscriptionsReady', true)
        expect(model).to.have.property('versionTsReady', false)

        // Check for defaults
        expect(model).to.have.nested.property(
          'sources.sources.abcd_influxChange_v2_log.some_default',
          'default'
        )
      })
  })

  it('should process change log writes', function() {
    const msgStr = JSON.stringify({
      context: {
        org_slug: 'abcd'
      },
      payload: {
        options: {
          database: 'station_database',
          precision: 'ms'
        },
        writes: [
          {
            measurement: 'source_measurement',
            pointsCount: 2,
            timeMax: 1524614400000,
            timeMin: 1524614400000
          }
        ]
      }
    })

    return new Promise((resolve, reject) => {
      model.private.stan.publish(requestSubject, msgStr, (err, guid) =>
        err ? reject(err) : resolve(guid)
      )
    })
  })

  it('should wait for 5 seconds', function() {
    return new Promise(resolve => setTimeout(resolve, 5000))
  })

  it('should verify derived builds', function() {
    return main.app
      .get('connections')
      .dispatch.app.service('/derived-builds')
      .find()
      .then(res => {
        expect(res)
          .to.have.property('data')
          .lengthOf(1)

        expect(res).to.have.nested.property('data.0.method', 'deriveDatapoints')
        expect(res).to.have.nested.property(
          'data.0.dispatch_key',
          id.derivedDatastream
        )
        expect(res).to.have.nested.property(
          'data.0.spec.database',
          `derived_org_${id.org}`
        )
        expect(res).to.have.nested.property(
          'data.0.spec.measurement',
          `derived_data_${id.derivedDatastream}`
        )
        expect(res).to.have.nested.property(
          'data.0.spec.sourceDatastreamId',
          id.datastream
        )
        expect(res).to.have.nested.property(
          'data.0.spec.derivedDatastreamId',
          id.derivedDatastream
        )
      })
  })
})
