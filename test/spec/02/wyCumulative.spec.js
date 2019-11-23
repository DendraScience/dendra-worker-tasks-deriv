/**
 * Tests for wyCumulative deriver
 */

const feathers = require('@feathersjs/feathers')
const auth = require('@feathersjs/authentication-client')
const localStorage = require('localstorage-memory')
const restClient = require('@feathersjs/rest-client')
const axios = require('axios')
const murmurHash3 = require('murmurhash3js')

describe('wyCumulative deriver', function() {
  this.timeout(60000)

  const testName = 'dendra-worker-tasks-deriv UNIT_TEST'

  const id = {}
  const date = {
    wy2014: '2014-10-01T00:00:00.000Z',
    wy2015: '2015-10-01T00:00:00.000Z',
    wy2016: '2016-10-01T00:00:00.000Z',
    wy2017: '2017-10-01T00:00:00.000Z',
    wy2018: '2018-10-01T00:00:00.000Z'
  }
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

  let createDeriver
  let deriver
  let deriverSpecs

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
  })

  after(async function() {
    await cleanup()
  })

  it('should import', function() {
    createDeriver = require('../../../dist/lib/derivers').wyCumulative

    expect(createDeriver).to.be.a('function')
  })

  it('should create', function() {
    const datapointService = webConnection.app.service('/datapoints')

    deriver = createDeriver({ datapointService })

    expect(deriver).to.be.a('function')
  })

  it('should init', function() {
    return webConnection.app
      .service('/datastreams')
      .find({ query: { _id: id.datastream } })
      .then(res => {
        expect(res)
          .to.have.property('data')
          .lengthOf(1)

        return deriver({
          derived_datastream: {
            _id: 'dummy'
          },
          source_datastreams: res.data
        })
      })
      .then(specs => {
        expect(specs).to.have.lengthOf(4)
        expect(specs).to.have.nested.property(
          '0.source_datastream_id',
          id.datastream
        )

        expect(specs).to.have.nested.property(
          '0.start_time',
          new Date(date.wy2014).getTime()
        )
        expect(specs).to.have.nested.property(
          '0.until_time',
          new Date(date.wy2015).getTime()
        )

        expect(specs).to.have.nested.property(
          '1.start_time',
          new Date(date.wy2015).getTime()
        )
        expect(specs).to.have.nested.property(
          '1.until_time',
          new Date(date.wy2016).getTime()
        )

        expect(specs).to.have.nested.property(
          '2.start_time',
          new Date(date.wy2016).getTime()
        )
        expect(specs).to.have.nested.property(
          '2.until_time',
          new Date(date.wy2017).getTime()
        )

        expect(specs).to.have.nested.property(
          '3.start_time',
          new Date(date.wy2017).getTime()
        )
        expect(specs).to.have.nested.property(
          '3.until_time',
          new Date(date.wy2018).getTime()
        )

        deriverSpecs = specs
      })
  })

  it('should run first spec', function() {
    return deriver(deriverSpecs[0], null, data => {
      expect(data).to.have.nested.property('0.timestamp')
      expect(data).to.have.nested.property('0.fields.utc_offset', -28800)
      expect(data).to.have.nested.property('0.fields.value')
    }).then(stats => {
      expect(stats).to.have.property('count', 54447)
      expect(stats).to.have.property('pages', 28)
      expect(stats).to.have.property(
        'start_time',
        new Date(date.wy2014).getTime()
      )
    })
  })
})
