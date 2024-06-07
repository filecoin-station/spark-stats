import { migrateWithPgClient } from '@filecoin-station/spark-stats-db-migrations'
import pg from 'pg'

import { DATABASE_URL } from '../lib/config.js'

export const getPgPool = async () => {
  const pgPool = new pg.Pool({
    connectionString: DATABASE_URL,
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
  })

  pgPool.on('error', err => {
    // Prevent crashing the process on idle client errors, the pool will recover
    // itself. If all connections are lost, the process will still crash.
    // https://github.com/brianc/node-postgres/issues/1324#issuecomment-308778405
    console.error('An idle client has experienced an error', err.stack)
  })

  await migrateWithPgClient(pgPool)

  // Check that we can talk to the database
  await pgPool.query('SELECT 1')

  return pgPool
}
