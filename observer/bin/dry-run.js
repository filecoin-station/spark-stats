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
await pgPool.query('DELETE FROM reward_transfer_last_block')
// Set the last block to -800 to simulate the observer starting from the beginning
await pgPool.query('INSERT INTO reward_transfer_last_block (last_block) VALUES (-800)')

await observeTransferEvents(pgPool, ieContract, provider)

await pgPool.end()
