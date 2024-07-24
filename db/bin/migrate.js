import { migrateStatsDB, getStatsPgPool } from '../index.js'

const pgPool = await getStatsPgPool()
await migrateStatsDB(pgPool)
