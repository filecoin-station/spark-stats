import http from 'node:http'
import { once } from 'node:events'
import assert from 'node:assert'
import createDebug from 'debug'
import { getPgPools } from '@filecoin-station/spark-stats-db'
import { givenDailyParticipants } from '@filecoin-station/spark-stats-db/test-helpers.js'

import { assertResponseStatus, getPort } from './test-helpers.js'
import { createApp } from '../lib/app.js'
import { today } from '../lib/request-helpers.js'

describe('HTTP request handler', () => {
  /** @type {import('@filecoin-station/spark-stats-db').PgPools} */
  let pgPools
  /** @type {import('fastify').FastifyInstance} */
  let app
  /** @type {string} */
  let baseUrl

  before(async () => {
    pgPools = await getPgPools()

    app = createApp({
      SPARK_API_BASE_URL: 'https://api.filspark.com/',
      pgPools,
      logger: false
    })

    baseUrl = await app.listen()
  })

  after(async () => {
    await app.close()
    await pgPools.end()
  })

  beforeEach(async () => {
    await pgPools.evaluate.query('DELETE FROM retrieval_stats')
    await pgPools.evaluate.query('DELETE FROM daily_participants')
    await pgPools.evaluate.query('DELETE FROM daily_deals')
    await pgPools.evaluate.query('DELETE FROM retrieval_timings')
    await pgPools.stats.query('DELETE FROM daily_scheduled_rewards')
    await pgPools.stats.query('DELETE FROM daily_reward_transfers')
    await pgPools.stats.query('DELETE FROM daily_retrieval_result_codes')
  })

  it('returns 200 for GET /', async () => {
    const res = await fetch(new URL('/', baseUrl))
    await assertResponseStatus(res, 200)
  })

  it('returns 404 for unknown routes', async () => {
    const res = await fetch(new URL('/unknown-path', baseUrl))
    await assertResponseStatus(res, 404)
  })

  it('returns 404 when the path starts with double slash', async () => {
    const res = await fetch(`${baseUrl}//path-not-found`)
    await assertResponseStatus(res, 404)
  })

  describe('GET /retrieval-success-rate', () => {
    beforeEach(async () => {
      await pgPools.evaluate.query('DELETE FROM retrieval_stats')
    })

    it('returns today stats for no query string', async () => {
      const day = today()
      await givenRetrievalStats(pgPools.evaluate, { day, total: 10, successful: 1, successfulHttp: 0 })
      const res = await fetch(new URL('/retrieval-success-rate', baseUrl), { redirect: 'follow' })
      await assertResponseStatus(res, 200)
      const stats = await res.json()
      assert.deepStrictEqual(stats, [
        { day, success_rate: 0.1, successful: '1', total: '10', successful_http: '0', success_rate_http: 0 }
      ])
    })

    it('applies from & to in YYYY-MM-DD format', async () => {
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-10', total: 10, successful: 1, successfulHttp: 1 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-11', total: 20, successful: 1, successfulHttp: 0 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-12', total: 30, successful: 3, successfulHttp: 3 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-13', total: 40, successful: 1, successfulHttp: 1 })

      const res = await fetch(
        new URL(
          '/retrieval-success-rate?from=2024-01-11&to=2024-01-12',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = await res.json()
      assert.deepStrictEqual(stats, [
        { day: '2024-01-11', success_rate: 0.05, successful: '1', total: '20', successful_http: '0', success_rate_http: 0 },
        { day: '2024-01-12', success_rate: 0.1, successful: '3', total: '30', successful_http: '3', success_rate_http: 0.1 }
      ])
    })

    it('redirects when from & to is in YYYY-MM-DDThh:mm:ss.sssZ format', async () => {
      const res = await fetch(
        new URL(
          '/retrieval-success-rate?from=2024-01-10T13:44:44.289Z&to=2024-01-15T09:44:44.289Z',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 301)
      assert.strictEqual(
        res.headers.get('location'),
        '/retrieval-success-rate?from=2024-01-10&to=2024-01-15'
      )
    })

    it('caches data including today for short time', async () => {
      const res = await fetch(
        new URL(`/retrieval-success-rate?from=2024-01-01&to=${today()}`, baseUrl),
        {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      assert.strictEqual(res.headers.get('cache-control'), 'public, max-age=600')
    })

    it('caches historical including for long time & marks them immutable', async () => {
      const res = await fetch(
        new URL('/retrieval-success-rate?from=2023-01-01&to=2023-12-31', baseUrl),
        {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      assert.strictEqual(res.headers.get('cache-control'), 'public, max-age=31536000, immutable')
    })

    it('sums daily retrievals from all miners', async () => {
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-10', minerId: 'f1one', total: 10, successful: 1, successfulHttp: 1 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-10', minerId: 'f1two', total: 100, successful: 50, successfulHttp: 35 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-11', minerId: 'f1one', total: 20, successful: 1, successfulHttp: 0 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-11', minerId: 'f1two', total: 200, successful: 60, successfulHttp: 50 })

      const res = await fetch(
        new URL(
          '/retrieval-success-rate?from=2024-01-10&to=2024-01-11',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)

      const stats = /** @type {{ day: string, success_rate: number }[]} */(
        await res.json()
      )
      assert.deepStrictEqual(stats, [
        { day: '2024-01-10', success_rate: 51 / 110, total: '110', successful: '51', successful_http: '36', success_rate_http: 36 / 110 },
        { day: '2024-01-11', success_rate: 61 / 220, total: '220', successful: '61', successful_http: '50', success_rate_http: 50 / 220 }
      ])
    })

    it('sorts items by date ascending', async () => {
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-20', total: 10, successful: 1, successfulHttp: 1 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-10', total: 10, successful: 5, successfulHttp: 3 })

      const res = await fetch(
        new URL(
          '/retrieval-success-rate?from=2024-01-01&to=2024-01-31',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = (/** @type {{ day: string, success_rate: number }[]} */
        await res.json()
      )
      assert.deepStrictEqual(stats, [
        { day: '2024-01-10', success_rate: 5 / 10, total: '10', successful: '5', successful_http: '3', success_rate_http: 3 / 10 },
        { day: '2024-01-20', success_rate: 1 / 10, total: '10', successful: '1', successful_http: '1', success_rate_http: 1 / 10 }
      ])
    })

    it('filters out miners with zero RSR when asked', async () => {
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-20', total: 10, successful: 1, minerId: 'f1one', successfulHttp: 1 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-20', total: 10, successful: 0, minerId: 'f1two', successfulHttp: 0 })

      const res = await fetch(
        new URL(
          '/retrieval-success-rate?from=2024-01-01&to=2024-01-31&nonZero=true',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = /** @type {{ day: string, success_rate: number }[]} */(
        await res.json()
      )
      assert.deepStrictEqual(stats, [
        { day: '2024-01-20', success_rate: 1 / 10, successful: '1', total: '10', successful_http: '1', success_rate_http: 1 / 10 }
      ])
    })

    it('preserves additional query string arguments when redirecting', async () => {
      const day = today()
      await givenRetrievalStats(pgPools.evaluate, { day, total: 10, successful: 1, minerId: 'f1one', successfulHttp: 1 })
      await givenRetrievalStats(pgPools.evaluate, { day, total: 10, successful: 0, minerId: 'f1two', successfulHttp: 0 })
      const res = await fetch(new URL('/retrieval-success-rate?nonZero=true', baseUrl), { redirect: 'follow' })
      await assertResponseStatus(res, 200)
      const stats = await res.json()
      assert.deepStrictEqual(stats, [
        { day, success_rate: 0.1, successful: '1', total: '10', successful_http: '1', success_rate_http: 0.1 }
      ])
    })
    it('handles successful_http values 0, null, undefined', async () => {
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-20', total: 10, successful: 1, successfulHttp: 0 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-21', total: 10, successful: 1, successfulHttp: undefined })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-22', total: 10, successful: 1, successfulHttp: null })

      const res = await fetch(
        new URL(
          '/retrieval-success-rate?from=2024-01-20&to=2024-01-22',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = await res.json()
      assert.deepStrictEqual(stats, [
        { day: '2024-01-20', success_rate: 0.1, successful: '1', total: '10', successful_http: '0', success_rate_http: 0 },
        { day: '2024-01-21', success_rate: 0.1, successful: '1', total: '10', successful_http: null, success_rate_http: null },
        { day: '2024-01-22', success_rate: 0.1, successful: '1', total: '10', successful_http: null, success_rate_http: null }
      ])
    })
  })

  describe('GET /participants/daily', () => {
    it('returns daily active participants for the given date range', async () => {
      await givenDailyParticipants(pgPools.evaluate, '2024-01-10', ['0x10', '0x20'])
      await givenDailyParticipants(pgPools.evaluate, '2024-01-11', ['0x10', '0x20', '0x30'])
      await givenDailyParticipants(pgPools.evaluate, '2024-01-12', ['0x10', '0x20', '0x40', '0x50'])
      await givenDailyParticipants(pgPools.evaluate, '2024-01-13', ['0x10'])

      const res = await fetch(
        new URL(
          '/participants/daily?from=2024-01-11&to=2024-01-12',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = await res.json()
      assert.deepStrictEqual(stats, [
        { day: '2024-01-11', participants: 3 },
        { day: '2024-01-12', participants: 4 }
      ])
    })
  })

  describe('GET /participants/monthly', () => {
    it('returns monthly active participants for the given date range ignoring the day number', async () => {
      // before the range
      await givenDailyParticipants(pgPools.evaluate, '2023-12-31', ['0x01', '0x02'])
      // in the range
      await givenDailyParticipants(pgPools.evaluate, '2024-01-10', ['0x10', '0x20'])
      await givenDailyParticipants(pgPools.evaluate, '2024-01-11', ['0x10', '0x20', '0x30'])
      await givenDailyParticipants(pgPools.evaluate, '2024-01-12', ['0x10', '0x20', '0x40', '0x50'])
      await givenDailyParticipants(pgPools.evaluate, '2024-02-13', ['0x10', '0x60'])
      // after the range
      await givenDailyParticipants(pgPools.evaluate, '2024-03-01', ['0x99'])

      const res = await fetch(
        new URL(
          '/participants/monthly?from=2024-01-12&to=2024-02-12',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = await res.json()
      assert.deepStrictEqual(stats, [
        { month: '2024-01-01', participants: 5 },
        { month: '2024-02-01', participants: 2 }
      ])
    })
  })

  describe('GET /participants/change-rates', () => {
    it('returns monthly change rates for the given date range ignoring the day number', async () => {
      // before the range
      await givenDailyParticipants(pgPools.evaluate, '2023-12-31', ['0x01', '0x02'])
      // the last month before the range
      await givenDailyParticipants(pgPools.evaluate, '2024-01-10', ['0x10', '0x20'])
      await givenDailyParticipants(pgPools.evaluate, '2024-01-11', ['0x10', '0x20', '0x30'])
      await givenDailyParticipants(pgPools.evaluate, '2024-01-12', ['0x10', '0x20', '0x40', '0x50'])
      // the first month in the range - 0x50 is gone
      await givenDailyParticipants(pgPools.evaluate, '2024-02-11', ['0x10', '0x20'])
      await givenDailyParticipants(pgPools.evaluate, '2024-02-13', ['0x20', '0x30', '0x40'])
      // the second month in the range - 0x30 and 0x40 is gone, new participant 0x60
      await givenDailyParticipants(pgPools.evaluate, '2024-03-11', ['0x10', '0x20'])
      await givenDailyParticipants(pgPools.evaluate, '2024-03-13', ['0x10', '0x60'])
      // after the range
      await givenDailyParticipants(pgPools.evaluate, '2024-04-01', ['0x99'])

      const res = await fetch(
        new URL(
          '/participants/change-rates?from=2024-02-28&to=2024-03-01',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = await res.json()
      assert.deepStrictEqual(stats, [
        // January: 5 participants
        // February: 1 participant lost, no new participants
        {
          month: '2024-02-01',
          // Churn: 1/5 = 20%
          churnRate: 0.2,
          // Growth: 0/5 = 20%
          growthRate: 0,
          // Retention: 4/5 = 80%
          retentionRate: 0.8
        },
        // February: 4 participants
        // March: 2 participants lost, 1 new participant
        {
          month: '2024-03-01',
          // Churn: 2/4 = 50%
          churnRate: 0.5,
          // Growth: 1/4 = 25%
          growthRate: 0.25,
          // Retention: 2/4 = 50%
          retentionRate: 0.5
        }
      ])
    })

    it('handles a single-month range', async () => {
      // the last month before the range
      await givenDailyParticipants(pgPools.evaluate, '2024-01-10', ['0x10', '0x20'])
      // the only month in the range - 0x20 is gone
      await givenDailyParticipants(pgPools.evaluate, '2024-02-11', ['0x10'])
      // after the range
      await givenDailyParticipants(pgPools.evaluate, '2024-03-01', ['0x99'])

      const res = await fetch(
        new URL(
          '/participants/change-rates?from=2024-02-11&to=2024-02-11',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = await res.json()
      assert.deepStrictEqual(stats, [{
        month: '2024-02-01',
        churnRate: 0.5,
        growthRate: 0,
        retentionRate: 0.5
      }])
    })
  })

  describe('GET /participant/:address/scheduled-rewards', () => {
    it('returns daily scheduled rewards for the given date range', async () => {
      await pgPools.stats.query(
        'INSERT INTO daily_scheduled_rewards (day, participant_address, scheduled_rewards) VALUES ($1, $2, $3)',
        ['2024-01-11', '0x20', '1']
      )

      const res = await fetch(
        new URL(
          '/participant/0x20/scheduled-rewards?from=2024-01-11&to=2024-01-12',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = await res.json()
      assert.deepStrictEqual(stats, [
        { day: '2024-01-11', scheduled_rewards: '1' }
      ])
    })
  })

  describe('GET /participant/:address/reward-transfers', () => {
    it('returns daily reward trainsfers for the given date range', async () => {
      await pgPools.stats.query(`
        INSERT INTO daily_reward_transfers
        (day, to_address, amount, last_checked_block)
        VALUES
        ($1, $2, $3, $4)
      `, ['2024-01-11', '0x00', '1', 0])

      const res = await fetch(
        new URL(
          '/participant/0x00/reward-transfers?from=2024-01-11&to=2024-01-12',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = await res.json()
      assert.deepStrictEqual(stats, [
        { day: '2024-01-11', amount: '1' }
      ])
    })
  })

  describe('GET /miners/retrieval-success-rate/summary', () => {
    it('returns a summary of miners RSR for the given date range', async () => {
      // before the range
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-10', minerId: 'f1one', total: 10, successful: 1, successfulHttp: 1 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-10', minerId: 'f1two', total: 100, successful: 20, successfulHttp: 10 })
      // in the range
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-11', minerId: 'f1one', total: 20, successful: 1, successfulHttp: 0 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-11', minerId: 'f1two', total: 200, successful: 150, successfulHttp: 100 })
      // after the range
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-12', minerId: 'f1one', total: 30, successful: 1, successfulHttp: 1 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-12', minerId: 'f1two', total: 300, successful: 60, successfulHttp: 60 })

      const res = await fetch(
        new URL(
          '/miners/retrieval-success-rate/summary?from=2024-01-11&to=2024-01-11',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = await res.json()
      assert.deepStrictEqual(stats, [
        { miner_id: 'f1one', success_rate: 0.05, total: '20', successful: '1', successful_http: '0', success_rate_http: 0 },
        { miner_id: 'f1two', success_rate: 0.75, total: '200', successful: '150', successful_http: '100', success_rate_http: 100 / 200 }
      ])
    })
    it('handles successful_http values 0, null, undefined', async () => {
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-20', minerId: 'f1one', total: 10, successful: 1, successfulHttp: 0 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-21', minerId: 'f1one', total: 10, successful: 1, successfulHttp: undefined })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-22', minerId: 'f1one', total: 10, successful: 1, successfulHttp: null })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-23', minerId: 'f2two', total: 10, successful: 1, successfulHttp: undefined })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-24', minerId: 'f3three', total: 20, successful: 2, successfulHttp: null })

      let res = await fetch(
        new URL(
          '/miners/retrieval-success-rate/summary?from=2024-01-20&to=2024-01-22',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      let stats = await res.json()
      assert.deepStrictEqual(stats, [
        // If there is a single number we expect any undefined or null values to be converted to 0 by Postgres
        { miner_id: 'f1one', total: '30', successful: '3', success_rate: 0.1, successful_http: '0', success_rate_http: 0 }
      ])

      res = await fetch(
        new URL(
          '/miners/retrieval-success-rate/summary?from=2024-01-23&to=2024-01-24',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      stats = await res.json()
      assert.deepStrictEqual(stats, [
        { miner_id: 'f2two', total: '10', successful: '1', success_rate: 0.1, successful_http: null, success_rate_http: null },
        { miner_id: 'f3three', total: '20', successful: '2', success_rate: 0.1, successful_http: null, success_rate_http: null }
      ]
      )
    })
  })

  describe('GET /retrieval-result-codes/daily', () => {
    it('returns daily retrieval result codes for the given date range', async () => {
      await pgPools.stats.query(`
        INSERT INTO daily_retrieval_result_codes
        (day, code, rate)
        VALUES
        ('2024-01-11', 'OK', 0.1),
        ('2024-01-11', 'CAR_TOO_LARGE', 0.9),
        ('2024-01-12', 'OK', 1),
        ('2024-01-13', 'OK', 0.5),
        ('2024-01-13', 'IPNI_500', 0.5)
      `)

      const res = await fetch(
        new URL(
          '/retrieval-result-codes/daily?from=2024-01-11&to=2024-01-13',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = await res.json()
      assert.deepStrictEqual(stats, [
        { day: '2024-01-11', rates: { OK: '0.1', CAR_TOO_LARGE: '0.9' } },
        { day: '2024-01-12', rates: { OK: '1' } },
        { day: '2024-01-13', rates: { OK: '0.5', IPNI_500: '0.5' } }
      ])
    })
  })

  describe('summary of eligible deals', () => {
    describe('GET /miner/{id}/deals/eligible/summary', () => {
      it('redirects to spark-api', async () => {
        const res = await fetch(new URL('/miner/f0230/deals/eligible/summary', baseUrl), { redirect: 'manual' })
        await assertResponseStatus(res, 302)
        assert.strictEqual(res.headers.get('cache-control'), 'max-age=21600')
        assert.strictEqual(res.headers.get('location'), 'https://api.filspark.com/miner/f0230/deals/eligible/summary')
      })
    })

    describe('GET /client/{id}/deals/eligible/summary', () => {
      it('redirects to spark-api', async () => {
        const res = await fetch(new URL('/client/f0800/deals/eligible/summary', baseUrl), { redirect: 'manual' })
        await assertResponseStatus(res, 302)
        assert.strictEqual(res.headers.get('cache-control'), 'max-age=21600')
        assert.strictEqual(res.headers.get('location'), 'https://api.filspark.com/client/f0800/deals/eligible/summary')
      })
    })

    describe('GET /allocator/{id}/deals/eligible/summary', () => {
      it('redirects to spark-api', async () => {
        const res = await fetch(new URL('/allocator/f0500/deals/eligible/summary', baseUrl), { redirect: 'manual' })
        await assertResponseStatus(res, 302)
        assert.strictEqual(res.headers.get('cache-control'), 'max-age=21600')
        assert.strictEqual(res.headers.get('location'), 'https://api.filspark.com/allocator/f0500/deals/eligible/summary')
      })
    })
  })

  describe('GET /deals/daily', () => {
    it('returns daily deal stats for the given date range', async () => {
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-01-10', tested: 10, indexed: 5, retrievable: 1 })
      await givenDailyDealStats(pgPools.evaluate, {
        day: '2024-01-11',
        tested: 20,
        indexMajorityFound: 10,
        indexed: 6,
        indexedHttp: 4,
        retrievalMajorityFound: 5,
        retrievable: 2
      })
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-01-12', tested: 30, indexed: 7, retrievable: 3 })
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-01-13', tested: 40, indexed: 8, retrievable: 4 })

      const res = await fetch(
        new URL(
          '/deals/daily?from=2024-01-11&to=2024-01-12',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = await res.json()
      assert.deepStrictEqual(stats, [
        {
          day: '2024-01-11',
          tested: '20',
          indexMajorityFound: '10',
          indexed: '6',
          indexedHttp: '4',
          retrievalMajorityFound: '5',
          retrievable: '2'
        },
        {
          day: '2024-01-12',
          tested: '30',
          indexMajorityFound: '7',
          indexed: '7',
          indexedHttp: '7',
          retrievalMajorityFound: '3',
          retrievable: '3'
        }
      ])
    })

    it('aggregates stats over miners', async () => {
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-01-11', minerId: 'f1aa', tested: 10 })
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-01-11', minerId: 'f1bb', tested: 20 })
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-01-12', minerId: 'f1aa', tested: 30 })
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-01-12', minerId: 'f1bb', tested: 40 })

      const res = await fetch(
        new URL(
          '/deals/daily?from=2024-01-11&to=2024-01-12',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = /** @type {any[]} */(await res.json())
      assert.deepStrictEqual(stats.map(({ day, tested }) => ({ day, tested })), [
        {
          day: '2024-01-11',
          tested: String(10 + 20)
        },
        {
          day: '2024-01-12',
          tested: String(30 + 40)
        }
      ])
    })

    it('aggregates stats over clients', async () => {
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-01-11', clientId: 'f1aa', tested: 10 })
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-01-11', clientId: 'f1bb', tested: 20 })
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-01-12', clientId: 'f1aa', tested: 30 })
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-01-12', minerId: 'f1bb', tested: 40 })

      const res = await fetch(
        new URL(
          '/deals/daily?from=2024-01-11&to=2024-01-12',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = /** @type {any[]} */(await res.json())
      assert.deepStrictEqual(stats.map(({ day, tested }) => ({ day, tested })), [
        {
          day: '2024-01-11',
          tested: String(10 + 20)
        },
        {
          day: '2024-01-12',
          tested: String(30 + 40)
        }
      ])
    })
  })

  describe('GET /deals/summary', () => {
    it('returns deal summary for the given date range (including the end day)', async () => {
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-03-12', tested: 200, indexed: 52, retrievable: 2 })
      // filter.to - 7 days -> should be excluded
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-03-23', tested: 300, indexed: 53, retrievable: 3 })
      // last 7 days
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-03-24', tested: 400, indexed: 54, retrievable: 4 })
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-03-29', tested: 500, indexed: 55, retrievable: 5 })
      // `filter.to` (e.g. today) - should be included
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-03-30', tested: 6000, indexed: 600, retrievable: 60 })
      // after the requested range
      await givenDailyDealStats(pgPools.evaluate, { day: '2024-03-31', tested: 70000, indexed: 7000, retrievable: 700 })

      const res = await fetch(
        new URL(
          '/deals/summary?from=2024-03-24&to=2024-03-30',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = await res.json()

      assert.deepStrictEqual(stats, {
        tested: '6900',
        indexMajorityFound: '709',
        indexed: '709',
        indexedHttp: '709',
        retrievalMajorityFound: '69',
        retrievable: '69'
      })
    })

    it('handles query for future date with no recorded stats', async () => {
      const res = await fetch(
        new URL(
          '/deals/summary?from=3024-04-24&to=3024-03-30',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)
      const stats = await res.json()

      assert.deepStrictEqual(stats, {
        indexMajorityFound: null,
        tested: null,
        indexed: null,
        indexedHttp: null,
        retrievalMajorityFound: null,
        retrievable: null
      })
    })
  })

  describe('CORS', () => {
    it('sets CORS headers for requests from Station Desktop in production', async () => {
      const res = await fetch(new URL('/', baseUrl), {
        headers: {
          origin: 'app://-'
        }
      })
      assert.strictEqual(res.headers.get('access-control-allow-origin'), 'app://-')
    })
    it('sets CORS headers for requests from Station Desktop in development', async () => {
      const res = await fetch(new URL('/', baseUrl), {
        headers: {
          origin: 'http://localhost:3000'
        }
      })
      assert.strictEqual(res.headers.get('access-control-allow-origin'), 'http://localhost:3000')
    })
  })

  describe('GET /miner/{id}/retrieval-success-rate/summary', () => {
    it('lists daily retrieval stats summary for specified miner in given date range', async () => {
      // before the range
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-09', minerId: 'f1one', total: 10, successful: 1, successfulHttp: 1 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-09', minerId: 'f1two', total: 100, successful: 20, successfulHttp: 10 })
      // in the range
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-20', minerId: 'f1one', total: 20, successful: 1, successfulHttp: 0 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-20', minerId: 'f1two', total: 200, successful: 60, successfulHttp: 50 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-10', minerId: 'f1one', total: 10, successful: 1, successfulHttp: 1 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-10', minerId: 'f1two', total: 100, successful: 50, successfulHttp: 35 })
      // after the range
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-21', minerId: 'f1one', total: 30, successful: 1, successfulHttp: 1 })
      await givenRetrievalStats(pgPools.evaluate, { day: '2024-01-21', minerId: 'f1two', total: 300, successful: 60, successfulHttp: 60 })

      const res = await fetch(
        new URL(
          '/miner/f1one/retrieval-success-rate/summary?from=2024-01-10&to=2024-01-20',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)

      const stats = /** @type {{ day: string, success_rate: number }[]} */(
        await res.json()
      )
      assert.deepStrictEqual(stats, [
        { day: '2024-01-10', success_rate: 1 / 10, total: '10', successful: '1', successful_http: '1', success_rate_http: 1 / 10 },
        { day: '2024-01-20', success_rate: 1 / 20, total: '20', successful: '1', successful_http: '0', success_rate_http: 0 }
      ])
    })
  })

  describe('miner retrieval timing stats', () => {
    beforeEach(async () => {
      // before the range
      await givenRetrievalTimings(pgPools.evaluate, { day: '2024-01-09', minerId: 'f1one', timeToFirstByteP50: [1000] })
      await givenRetrievalTimings(pgPools.evaluate, { day: '2024-01-09', minerId: 'f1two', timeToFirstByteP50: [1000] })
      // in the range
      await givenRetrievalTimings(pgPools.evaluate, { day: '2024-01-20', minerId: 'f1one', timeToFirstByteP50: [1000] })
      await givenRetrievalTimings(pgPools.evaluate, { day: '2024-01-20', minerId: 'f1two', timeToFirstByteP50: [1000] })

      await givenRetrievalTimings(pgPools.evaluate, { day: '2024-01-10', minerId: 'f1one', timeToFirstByteP50: [123, 345] })
      await givenRetrievalTimings(pgPools.evaluate, { day: '2024-01-10', minerId: 'f1two', timeToFirstByteP50: [654, 789] })
      // after the range
      await givenRetrievalTimings(pgPools.evaluate, { day: '2024-01-21', minerId: 'f1one', timeToFirstByteP50: [1000] })
      await givenRetrievalTimings(pgPools.evaluate, { day: '2024-01-21', minerId: 'f1two', timeToFirstByteP50: [1000] })
    })

    it('lists daily retrieval timings in given date range', async () => {
      const res = await fetch(
        new URL(
          '/retrieval-timings/daily?from=2024-01-10&to=2024-01-20',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)

      const stats = /** @type {{ day: string, success_rate: number }[]} */(
        await res.json()
      )
      assert.deepStrictEqual(stats, [
        { day: '2024-01-10', ttfb_ms: 500 },
        { day: '2024-01-20', ttfb_ms: 1000 }
      ])
    })

    it('lists daily retrieval timings summary for specified miner in given date range', async () => {
      const res = await fetch(
        new URL(
          '/miner/f1one/retrieval-timings/summary?from=2024-01-10&to=2024-01-20',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)

      const stats = /** @type {{ day: string, success_rate: number }[]} */(
        await res.json()
      )
      assert.deepStrictEqual(stats, [
        { day: '2024-01-10', miner_id: 'f1one', ttfb_ms: 234 },
        { day: '2024-01-20', miner_id: 'f1one', ttfb_ms: 1000 }
      ])
    })

    it('lists daily retrieval timings summary for all miners in given date range', async () => {
      const res = await fetch(
        new URL(
          '/miners/retrieval-timings/summary?from=2024-01-10&to=2024-01-20',
          baseUrl
        ), {
          redirect: 'manual'
        }
      )
      await assertResponseStatus(res, 200)

      const stats = /** @type {{ day: string, success_rate: number }[]} */(
        await res.json()
      )
      assert.deepStrictEqual(stats, [
        { miner_id: 'f1one', ttfb_ms: 345 },
        { miner_id: 'f1two', ttfb_ms: 789 }
      ])
    })
  })
})

/**
 *
 * @param {import('../lib/platform-stats-fetchers.js').Queryable} pgPool
 * @param {object} data
 * @param {string} data.day
 * @param {string} [data.minerId]
 * @param {number | bigint} data.total
 * @param {number | bigint } data.successful
 * @param {number | bigint} [data.successfulHttp]
 */
const givenRetrievalStats = async (pgPool, { day, minerId, total, successful, successfulHttp }) => {
  await pgPool.query(
    'INSERT INTO retrieval_stats (day, miner_id, total, successful, successful_http) VALUES ($1, $2, $3, $4, $5)',
    [day, minerId ?? 'f1test', total, successful, successfulHttp]
  )
}

/**
 *
 * @param {import('@filecoin-station/spark-stats-db').Queryable} pgPool
 * @param {{
 *  day: string;
 *  minerId?: string;
 *  clientId?: string;
 *  tested: number;
 *  indexMajorityFound?: number;
 *  indexed?: number;
 *  indexedHttp?: number;
 *  retrievalMajorityFound?: number;
 *  retrievable?: number;
 * }} stats
 */
const givenDailyDealStats = async (pgPool, {
  day,
  minerId,
  clientId,
  tested,
  indexMajorityFound,
  indexed,
  indexedHttp,
  retrievalMajorityFound,
  retrievable
}) => {
  indexed ??= tested
  indexedHttp ??= indexed
  indexMajorityFound ??= indexed

  retrievable ??= tested
  retrievalMajorityFound ??= retrievable

  await pgPool.query(`
    INSERT INTO daily_deals (
      day,
      miner_id,
      client_id,
      tested,
      index_majority_found,
      indexed,
      indexed_http,
      retrieval_majority_found,
      retrievable
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
  `, [
    day,
    minerId ?? 'f1miner',
    clientId ?? 'f1client',
    tested,
    indexMajorityFound,
    indexed,
    indexedHttp,
    retrievalMajorityFound,
    retrievable
  ])
}

/**
 *
 * @param {import('../lib/platform-stats-fetchers.js').Queryable} pgPool
 * @param {object} data
 * @param {string} data.day
 * @param {string} data.minerId
 * @param {number[]} data.timeToFirstByteP50
 */
const givenRetrievalTimings = async (pgPool, { day, minerId, timeToFirstByteP50 }) => {
  await pgPool.query(
    'INSERT INTO retrieval_timings (day, miner_id, ttfb_p50) VALUES ($1, $2, $3)',
    [day, minerId ?? 'f1test', timeToFirstByteP50]
  )
}
