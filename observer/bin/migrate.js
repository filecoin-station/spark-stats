import { migrateStatsDB, migrateEvaluateDB, getPgPools } from '@filecoin-station/spark-stats-db'

const pgPools = await getPgPools()
await migrateStatsDB(pgPools.stats)
await migrateEvaluateDB(pgPools.evaluate)
