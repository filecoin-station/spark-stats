import '../lib/instrument.js'
import http from 'node:http'
import { once } from 'node:events'
import pg from 'pg'
import { createHandler } from '../lib/handler.js'
import { DATABASE_URL, EVALUATE_DB_URL } from '../lib/config.js'

const {
  PORT = 8080,
  HOST = '127.0.0.1',
  REQUEST_LOGGING = 'true'
} = process.env

const pgPoolConfig = {
  // allow the pool to close all connections and become empty
  min: 0,
  // this values should correlate with service concurrency hard_limit configured in fly.toml
  // and must take into account the connection limit of our PG server, see
  // https://fly.io/docs/postgres/managing/configuration-tuning/
  max: 100,
  // close connections that haven't been used for one second
  idleTimeoutMillis: 1000,
  // automatically close connections older than 60 seconds
  maxLifetimeSeconds: 60
}
const pgPoolErrFn = err => {
  // Prevent crashing the process on idle client errors, the pool will recover
  // itself. If all connections are lost, the process will still crash.
  // https://github.com/brianc/node-postgres/issues/1324#issuecomment-308778405
  console.error('An idle client has experienced an error', err.stack)
}

// Connect and set up the Evaluate DB
const pgPoolEvaluateDb = new pg.Pool({
  connectionString: EVALUATE_DB_URL,
  ...pgPoolConfig
})
pgPoolEvaluateDb.on('error', pgPoolErrFn)
// Check that we can talk to the database
await pgPoolEvaluateDb.query('SELECT 1')

// Connect and set up the Stats DB
const pgPoolStatsDb = new pg.Pool({
  connectionString: DATABASE_URL,
  ...pgPoolConfig
})
pgPoolStatsDb.on('error', pgPoolErrFn)
// Check that we can talk to the database
await pgPoolStatsDb.query('SELECT 1')

const logger = {
  error: console.error,
  info: console.info,
  request: ['1', 'true'].includes(REQUEST_LOGGING) ? console.info : () => {}
}

const handler = createHandler({
  pgPoolEvaluateDb,
  pgPoolStatsDb,
  logger
})
const server = http.createServer(handler)
console.log('Starting the http server on host %j port %s', HOST, PORT)
server.listen(PORT, HOST)
await once(server, 'listening')
console.log(`http://${HOST}:${PORT}`)
