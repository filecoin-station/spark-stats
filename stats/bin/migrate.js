import {
  DATABASE_URL,
  EVALUATE_DB_URL
} from '../lib/config.js'
import { migrateWithPgConfig as migrateEvaluateDB } from 'spark-evaluate/lib/migrate.js'
import { migrateWithPgConfig as migrateStatsDB } from '@filecoin-station/spark-stats-db-migrations'

console.log('Migrating spark_evaluate database')
await migrateEvaluateDB({ connectionString: EVALUATE_DB_URL })

console.log('Migrating spark_stats database')
await migrateStatsDB({ connectionString: DATABASE_URL })