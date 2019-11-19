/**
 * Tests for wyCumulative deriver
 */

const feathers = require('@feathersjs/feathers')
const auth = require('@feathersjs/authentication-client')
const localStorage = require('localstorage-memory')
const restClient = require('@feathersjs/rest-client')
const axios = require('axios')
const murmurHash3 = require('murmurhash3js')

describe.skip('wyCumulative deriver', function() {
  this.timeout(60000)

  const id = {
    datastream: '5ae87434fe27f47745102634'
  }
  const date = {
    wy2014: '2014-10-01T00:00:00.000Z',
    wy2015: '2015-10-01T00:00:00.000Z',
    wy2016: '2016-10-01T00:00:00.000Z',
    wy2017: '2017-10-01T00:00:00.000Z',
    wy2018: '2018-10-01T00:00:00.000Z',
    wy2019: '2019-10-01T00:00:00.000Z',
    wy2020: '2019-10-01T00:00:00.000Z'
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

  let createDeriver
  let deriver
  let deriverSpecs

  before(async function() {
    await authWebConnection()
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

        return deriver({ datastreams: res.data })
      })
      .then(specs => {
        expect(specs).to.have.lengthOf(6)
        expect(specs).to.have.nested.property('0.datastreamId', id.datastream)

        expect(specs).to.have.nested.property(
          '0.startTime',
          new Date(date.wy2014).getTime()
        )
        expect(specs).to.have.nested.property(
          '0.untilTime',
          new Date(date.wy2015).getTime()
        )

        expect(specs).to.have.nested.property(
          '1.startTime',
          new Date(date.wy2015).getTime()
        )
        expect(specs).to.have.nested.property(
          '1.untilTime',
          new Date(date.wy2016).getTime()
        )

        expect(specs).to.have.nested.property(
          '2.startTime',
          new Date(date.wy2016).getTime()
        )
        expect(specs).to.have.nested.property(
          '2.untilTime',
          new Date(date.wy2017).getTime()
        )

        expect(specs).to.have.nested.property(
          '3.startTime',
          new Date(date.wy2017).getTime()
        )
        expect(specs).to.have.nested.property(
          '3.untilTime',
          new Date(date.wy2018).getTime()
        )

        expect(specs).to.have.nested.property(
          '4.startTime',
          new Date(date.wy2018).getTime()
        )
        expect(specs).to.have.nested.property(
          '5.untilTime',
          new Date(date.wy2019).getTime()
        )

        expect(specs).to.have.nested.property(
          '5.startTime',
          new Date(date.wy2019).getTime()
        )
        expect(specs).to.have.nested.property(
          '5.untilTime',
          new Date(date.wy2020).getTime()
        )

        deriverSpecs = specs
      })
  })

  it('should run first spec', function() {
    return deriver(deriverSpecs[0], null, data => {
      expect(data).to.have.nested.property('0.lt')
      expect(data).to.have.nested.property('0.o', -28800)
      expect(data).to.have.nested.property('0.v')
      expect(data).to.have.nested.property('0.v_sum')
    }).then(stats => {
      expect(stats).to.have.property('count', 54447)
      expect(stats).to.have.property('pages', 28)
      expect(stats).to.have.property(
        'startTime',
        new Date(date.wy2014).getTime()
      )
    })
  })
})
