import http from 'node:http'
import { once } from 'node:events'
import { AssertionError } from 'node:assert'
import pg from 'pg'
import createDebug from 'debug'

import { createHandler } from '../lib/handler.js'
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

    // server = http.createServer((req, res) => { console.log(req.method, req.url); res.end('hello') })
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

  it('returns 404 for unknown routes', async () => {
    const res = await fetch(new URL('/unknown-path', baseUrl))
    assertResponseStatus(res, 404)
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
