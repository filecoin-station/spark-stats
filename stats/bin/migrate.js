import {
  getPgPools,
  migrateEvaluateDB,
  migrateStatsDB
} from '@filecoin-station/spark-stats-db'

const pgPools = await getPgPools()
await migrateStatsDB(pgPools.stats)
await migrateEvaluateDB(pgPools.evaluate)
