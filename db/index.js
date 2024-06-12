import { migrateWithPgClient as migrateEvaluateDB } from 'spark-evaluate/lib/migrate.js'
import { migrateWithPgClient as migrateStatsDB } from '@filecoin-station/spark-stats-db-migrations'
import pg from 'pg'

const {
  // DATABASE_URL points to `spark_stats` database managed by this monorepo
  DATABASE_URL = 'postgres://localhost:5432/spark_stats',

  // EVALUATE_DB_URL points to `spark_evaluate` database managed by spark-evaluate repo.
  // Eventually, we should move the code updating stats from spark-evaluate to this repo
  // and then we won't need two connection strings.
  EVALUATE_DB_URL = 'postgres://localhost:5432/spark_evaluate'
} = process.env

const poolConfig = {
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

const onError = err => {
  // Prevent crashing the process on idle client errors, the pool will recover
  // itself. If all connections are lost, the process will still crash.
  // https://github.com/brianc/node-postgres/issues/1324#issuecomment-308778405
  console.error('An idle client has experienced an error', err.stack)
}

export const getStats = async () => {
  const stats = new pg.Pool({
    ...poolConfig,
    connectionString: DATABASE_URL
  })
  stats.on('error', onError)
  await migrateStatsDB(stats)
  return stats
}

export const getEvaluate = async () => {
  const evaluate = new pg.Pool({
    ...poolConfig,
    connectionString: EVALUATE_DB_URL
  })
  evaluate.on('error', onError)
  await evaluate.query('SELECT 1')
  return evaluate
}

/**
 * @returns {Promise<import('./typings').pgPools>}
 */
export const getPgPools = async () => {
  const stats = await getStats()
  const evaluate = await getEvaluate()
  const end = () => Promise.all([stats.end(), evaluate.end()])

  return { stats, evaluate, end }
}

export const migrate = async () => {
  const pgPools = await getPgPools()

  console.log('Migrating spark_evaluate database')
  await migrateEvaluateDB(pgPools.evaluate)
  console.log('Migrating spark_stats database')
  await migrateStatsDB(pgPools.stats)

  await pgPools.end()
}
