import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'
import { migrateWithPgClient } from '@filecoin-station/spark-stats-db-migrations'
import { ethers } from 'ethers'
import pg from 'pg'
import { RPC_URL, DATABASE_URL, rpcHeaders } from '../lib/config.js'
import { updateDailyFilStats } from '../lib/platform-stats-generator.js'

// TODO: move this to a different file
const fetchRequest = new ethers.FetchRequest(RPC_URL)
fetchRequest.setHeader('Authorization', rpcHeaders.Authorization || '')
const provider = new ethers.JsonRpcProvider(
  fetchRequest,
  null,
  { polling: true }
)

const ieContract = new ethers.Contract(
  SparkImpactEvaluator.ADDRESS,
  SparkImpactEvaluator.ABI,
  provider
)

const pgPool = new pg.Pool({
  connectionString: DATABASE_URL,
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
})

pgPool.on('error', err => {
  // Prevent crashing the process on idle client errors, the pool will recover
  // itself. If all connections are lost, the process will still crash.
  // https://github.com/brianc/node-postgres/issues/1324#issuecomment-308778405
  console.error('An idle client has experienced an error', err.stack)
})

await migrateWithPgClient(pgPool)

// Check that we can talk to the database
await pgPool.query('SELECT 1')

console.log('Listening for impact evaluator events')

// Get the last block we checked. Even though there should be only one row, use MAX just to be safe
const lastCheckedBlock = await pgPool.query(
  'SELECT MAX(last_block) AS last_block FROM reward_transfer_last_block'
).then(res => res.rows[0].last_block)

// Listen for Transfer events from the IE contract
ieContract.queryFilter(ieContract.filters.Transfer(), lastCheckedBlock)
  .then(events => {
    for (const event of events) {
      console.log('%s FIL to %s at block %s', event.args.amount, event.args.to, event.blockNumber)
      updateDailyFilStats(
        pgPool,
        {
          to_address: event.args.to,
          amount: event.args.amount,
          blockNumber: event.blockNumber
        }
      )
    }
  })
