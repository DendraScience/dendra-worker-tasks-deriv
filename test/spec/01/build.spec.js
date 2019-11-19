/**
 * Tests for build tasks
 */

const feathers = require('@feathersjs/feathers')
const auth = require('@feathersjs/authentication-client')
const localStorage = require('localstorage-memory')
const restClient = require('@feathersjs/rest-client')
const axios = require('axios')
const murmurHash3 = require('murmurhash3js')

describe('build tasks', function() {
  this.timeout(180000)

  const now = new Date()
  const hostname = 'test-hostname-0'
  const hostParts = hostname.split('-')

  const model = {
    props: {},
    state: {
      _id: 'taskMachine-build-current',
      source_defaults: {
        some_default: 'default'
      },
      sources: [
        {
          description: 'Build derived datastreams based on a method',
          // NOTE: Deprecated in favor of consistent hashing
          // queue_group: 'dendra.derivedBuild.v2',
          sub_options: {
            ack_wait: 3600000,
            durable_name: '20181223'
          },
          sub_to_subject: 'dendra.derivedBuild.v2.req.{hostOrdinal}'
        }
      ],
      created_at: now,
      updated_at: now
    }
  }

  const requestSubject = 'dendra.derivedBuild.v2.req.0'
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
  Object.defineProperty(model, 'hostname', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: hostname
  })
  Object.defineProperty(model, 'hostOrdinal', {
    enumerable: false,
    configurable: false,
    writable: false,
    value: hostParts[hostParts.length - 1]
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
  let derivedDatastream
  let derivedBuild

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
        datapoints_config: [
          {
            begins_at: '2015-03-26T07:00:00.000Z',
            params: {
              query: {
                compact: true,
                datastream_id: 1785,
                time_adjust: -28800
              }
            },
            path: '/legacy/datavalues2',
            ends_before: '2018-04-25T00:00:00.000Z'
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
          'sources.dendra_derivedBuild_v2_req__hostOrdinal_.some_default',
          'default'
        )
      })
  })

  it('should touch dervied datastream using version_id', function() {
    return webConnection.app
      .service('/datastreams')
      .get(id.derivedDatastream)
      .then(doc => {
        return webConnection.app.service('/datastreams').patch(
          id.derivedDatastream,
          {
            $set: {
              source_type: 'deriver'
            }
          },
          {
            query: {
              version_id: doc.version_id
            }
          }
        )
      })
      .then(res => {
        expect(res).to.have.nested.property('_id', id.derivedDatastream)

        derivedDatastream = res
      })
  })

  it('should process initDerivedDatastream request', function() {
    const msgStr = JSON.stringify({
      _id: 'init-derived-datastream-1234',
      method: 'initDerivedDatastream',
      spec: {
        datastream: derivedDatastream
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

  it('should verify datastream patch after initDerivedDatastream', function() {
    return webConnection.app
      .service('/datastreams')
      .get(id.derivedDatastream)
      .then(doc => {
        expect(doc).to.have.property('_id', id.derivedDatastream)

        expect(doc).to.have.nested.property(
          'datapoints_config_built.0.begins_at',
          '1800-02-02T00:00:00.000Z'
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.0.ends_before',
          '2200-02-02T00:00:00.000Z'
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.0.params.query.db',
          `derived_org_${id.org}`
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.0.params.query.fc',
          `derived_data_${id.derivedDatastream}`
        )
        expect(doc).to.have.nested.property(
          'datapoints_config_built.0.path',
          '/influx/select'
        )
      })
  })

  it('should verify derived builds after initDerivedDatastream', function() {
    return main.app
      .get('connections')
      .dispatch.app.service('/derived-builds')
      .find()
      .then(res => {
        expect(res)
          .to.have.property('data')
          .lengthOf(4)

        expect(res).to.have.nested.property('data.0.method', 'deriveDatapoints')
        expect(res).to.have.nested.property(
          'data.0.dispatch_key',
          derivedDatastream._id
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

        derivedBuild = res.data[0]
      })
  })

  it('should process deriveDatapoints request', function() {
    if (!derivedBuild)
      return Promise.reject(new Error('Undefined derivedBuild'))

    const msgStr = JSON.stringify(derivedBuild)

    return new Promise((resolve, reject) => {
      model.private.stan.publish(requestSubject, msgStr, (err, guid) =>
        err ? reject(err) : resolve(guid)
      )
    })
  })

  it('should wait for 90 seconds', function() {
    return new Promise(resolve => setTimeout(resolve, 90000))
  })

  it('should find derived datapoints', function() {
    return webConnection.app
      .service('/datapoints')
      .find({
        query: {
          datastream_id: id.derivedDatastream,
          $limit: 100,
          $sort: {
            time: 1
          }
        }
      })
      .then(res => {
        /* eslint-disable-next-line no-console */
        console.log('>>>', res)

        // expect(res)
        //   .to.have.property('data')
        //   .lengthOf(2)
        //   .and.deep.include.ordered.members([
        //     { t: '2018-05-09T19:00:00.000Z', o: -28800, v: 17.25 },
        //     { t: '2018-05-09T19:10:00.000Z', o: -28800, v: 15.99 }
        //   ])
      })
  })
})
