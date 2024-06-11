import { getStats } from '../../common/db.js'
import { migrateWithPgClient as migrateStatsDB } from '@filecoin-station/spark-stats-db-migrations'

console.log('Migrating spark_stats database')
await migrateStatsDB(await getStats())
