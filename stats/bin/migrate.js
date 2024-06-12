import {
  DATABASE_URL
} from '../lib/config.js'
import { migrateWithPgConfig as migrateStatsDB } from '@filecoin-station/spark-stats-db-migrations'


console.log('Migrating spark_stats database', DATABASE_URL)
await migrateStatsDB({ connectionString: DATABASE_URL })
