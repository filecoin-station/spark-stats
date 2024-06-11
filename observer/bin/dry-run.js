import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'
import { ethers } from 'ethers'

import { RPC_URL, rpcHeaders } from '../lib/config.js'
import { observeTransferEvents } from '../lib/observer.js'
import { getPgPool } from '../lib/db.js'

/** @type {pg.Pool} */
const pgPool = await getPgPool()

const fetchRequest = new ethers.FetchRequest(RPC_URL)
fetchRequest.setHeader('Authorization', rpcHeaders.Authorization || '')
const provider = new ethers.JsonRpcProvider(fetchRequest, null, { polling: true })

const ieContract = new ethers.Contract(SparkImpactEvaluator.ADDRESS, SparkImpactEvaluator.ABI, provider)

await pgPool.query('DELETE FROM daily_reward_transfers')

await observeTransferEvents(pgPool, ieContract, provider)

// Do it a second time, without clearing the table.
// This should find 0 events, unless rewards are currently being released.
await observeTransferEvents(pgPool, ieContract, provider)

await pgPool.end()
