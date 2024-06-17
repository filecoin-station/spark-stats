import { migrateStatsDB, getPgPools } from '@filecoin-station/spark-stats-db'

const pgPools = await getPgPools()
await migrateStatsDB(pgPools.stats)
