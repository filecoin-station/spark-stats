import { migrateWithPgClient as migrateEvaluateDB } from 'spark-evaluate/lib/migrate.js'
import pg from 'pg'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import Postgrator from 'postgrator'

/** @typedef {import('./typings.js').PgPools} PgPools */
/** @typedef {import('./typings.js').EndablePgPools} EndablePgPools */

export { migrateEvaluateDB }

const {
  // DATABASE_URL points to `spark_stats` database managed by this monorepo
  DATABASE_URL = 'postgres://localhost:5432/spark_stats',

  // EVALUATE_DB_URL points to `spark_evaluate` database managed by spark-evaluate repo.
  // Eventually, we should move the code updating stats from spark-evaluate to this repo
  // and then we won't need two connection strings.
  EVALUATE_DB_URL = 'postgres://localhost:5432/spark_evaluate'
} = process.env

const migrationsDirectory = join(
  dirname(fileURLToPath(import.meta.url)),
  'migrations'
)

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

export const getStatsPgPool = async () => {
  const stats = new pg.Pool({
    ...poolConfig,
    connectionString: DATABASE_URL
  })
  stats.on('error', onError)
  await migrateStatsDB(stats)
  return stats
}

export const getEvaluatePgPool = async () => {
  const evaluate = new pg.Pool({
    ...poolConfig,
    connectionString: EVALUATE_DB_URL
  })
  evaluate.on('error', onError)
  await evaluate.query('SELECT 1')
  return evaluate
}

/**
 * @returns {Promise<EndablePgPools>}
 */
export const getPgPools = async () => {
  const stats = await getStatsPgPool()
  const evaluate = await getEvaluatePgPool()
  const end = async () => Promise.all([stats.end(), evaluate.end()])

  return { stats, evaluate, end }
}

/**
 * @param {pg.Client | pg.Pool} client
 */
export const migrateStatsDB = async (client) => {
  const postgrator = new Postgrator({
    migrationPattern: join(migrationsDirectory, '*'),
    driver: 'pg',
    execQuery: (query) => client.query(query)
  })
  console.log(
    'Migrating `spark-stats` DB schema from version %s to version %s',
    await postgrator.getDatabaseVersion(),
    await postgrator.getMaxVersion()
  )

  await postgrator.migrate()

  console.log('Migrated `spark-stats` DB schema to version', await postgrator.getDatabaseVersion())
}
