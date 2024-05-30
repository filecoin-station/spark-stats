import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'
import { migrateWithPgClient } from '@filecoin-station/spark-stats-db-migrations'
import { ethers } from 'ethers'
import pg from 'pg'

// TODO: move this to a config.js file
const {
  DATABASE_URL = 'postgres://localhost:5432/spark_stats',
  RPC_URLS = 'https://api.node.glif.io/rpc/v0',
  GLIF_TOKEN
} = process.env

const rpcUrls = RPC_URLS.split(',')
const RPC_URL = rpcUrls[Math.floor(Math.random() * rpcUrls.length)]
console.log(`Selected JSON-RPC endpoint ${RPC_URL}`)

const rpcHeaders = {}
if (RPC_URL.includes('glif')) {
  rpcHeaders.Authorization = `Bearer ${GLIF_TOKEN}`
}

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

ieContract.on('Transfer', (to, amount, ...args) => {
  /** @type {number} */
  const blockNumber = args.pop()
  console.log('Transfer %s FIL to %s at epoch %s', amount, to, blockNumber)
  // TODO: update the database
})
