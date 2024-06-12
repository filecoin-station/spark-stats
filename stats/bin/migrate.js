import { getPgPools } from '../../common/db.js'
import { migrateWithPgClient as migrateEvaluateDB } from 'spark-evaluate/lib/migrate.js'
import { migrateWithPgClient as migrateStatsDB } from '@filecoin-station/spark-stats-db-migrations'

const pgPools = await getPgPools()

console.log('Migrating spark_evaluate database')
await migrateEvaluateDB(pgPools.evaluate)

console.log('Migrating spark_stats database')
await migrateStatsDB(pgPools.stats)

await pgPools.end()
