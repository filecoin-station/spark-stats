import http from 'node:http'
import { once } from 'node:events'
import assert, { AssertionError } from 'node:assert'
import pg from 'pg'
import createDebug from 'debug'

import { createHandler, today } from '../lib/handler.js'
import { DATABASE_URL } from '../lib/config.js'

const debug = createDebug('test')

describe('HTTP request handler', () => {
  /** @type {pg.Pool} */
  let pgPool
  /** @type {http.Server} */
  let server
  /** @type {string} */
  let baseUrl

  before(async () => {
    pgPool = new pg.Pool({ connectionString: DATABASE_URL })

    const handler = createHandler({
      pgPool,
      logger: {
        info: debug,
        error: console.error,
        request: debug
      }
    })

    server = http.createServer(handler)
    server.listen()
    await once(server, 'listening')
    baseUrl = `http://127.0.0.1:${server.address().port}`
  })

  after(async () => {
    server.closeAllConnections()
    server.close()
    await pgPool.end()
  })

  it('returns 200 for GET /', async () => {
    const res = await fetch(new URL('/', baseUrl))
    await assertResponseStatus(res, 200)
  })

  it('returns 404 for unknown routes', async () => {
    const res = await fetch(new URL('/unknown-path', baseUrl))
    await assertResponseStatus(res, 404)
  })

  describe('GET /retrieval-success-rate', () => {
    beforeEach(async () => {
      await pgPool.query('DELETE FROM retrieval_stats')
    })

    it('returns today stats for no query string', async () => {
      const day = today()
      await givenRetrievalStats(pgPool, { day, total: 10, successful: 1 })
      const res = await fetch(new URL('/retrieval-success-rate', baseUrl), { redirect: 'follow' })
      await assertResponseStatus(res, 200)
      const stats = await res.json()
      assert.deepStrictEqual(stats, [
        { day, success_rate: 0.1 }
      ])
    })

    it('applies from & to in YYYY-MM-DD format', async () => {
      await givenRetrievalStats(pgPool, { day: '2024-01-10', total: 10, successful: 1 })
      await givenRetrievalStats(pgPool, { day: '2024-01-11', total: 20, successful: 1 })
      await givenRetrievalStats(pgPool, { day: '2024-01-12', total: 30, successful: 3 })
      await givenRetrievalStats(pgPool, { day: '2024-01-13', total: 40, successful: 1 })

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
        { day: '2024-01-11', success_rate: 0.05 },
        { day: '2024-01-12', success_rate: 0.1 }
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
  })
})

const assertResponseStatus = async (res, status) => {
  if (res.status !== status) {
    throw new AssertionError({
      actual: res.status,
      expected: status,
      message: await res.text()
    })
  }
}

const givenRetrievalStats = async (pgPool, { day, total, successful }) => {
  await pgPool.query(
    'INSERT INTO retrieval_stats (day, total, successful) VALUES ($1, $2, $3)',
    [day, total, successful]
  )
}
