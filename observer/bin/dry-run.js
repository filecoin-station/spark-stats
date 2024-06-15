import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'
import { ethers } from 'ethers'

import { RPC_URL, rpcHeaders } from '../lib/config.js'
import { observeTransferEvents, observeScheduledRewards } from '../index.js'
import { getPgPools } from '@filecoin-station/spark-stats-db'

const pgPools = await getPgPools()

const fetchRequest = new ethers.FetchRequest(RPC_URL)
fetchRequest.setHeader('Authorization', rpcHeaders.Authorization || '')
const provider = new ethers.JsonRpcProvider(fetchRequest, null, { polling: true })

const ieContract = new ethers.Contract(SparkImpactEvaluator.ADDRESS, SparkImpactEvaluator.ABI, provider)

await pgPools.stats.query('DELETE FROM daily_reward_transfers')

await Promise.all([
  observeTransferEvents(pgPools.stats, ieContract, provider),
  observeScheduledRewards(pgPools, ieContract)
])

await pgPools.stats.end()
