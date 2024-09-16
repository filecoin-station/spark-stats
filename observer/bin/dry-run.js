import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'
import * as SparkEvaluationsRecentParticipants from '@filecoin-station/spark-evaluations-recent-participants'
import { ethers } from 'ethers'

import { RPC_URL, rpcHeaders } from '../lib/config.js'
import { observeTransferEvents, observeScheduledRewards } from '../lib/observer.js'
import { getStatsPgPool } from '@filecoin-station/spark-stats-db'

const pgPoolStats = await getStatsPgPool()

const fetchRequest = new ethers.FetchRequest(RPC_URL)
fetchRequest.setHeader('Authorization', rpcHeaders.Authorization || '')
const provider = new ethers.JsonRpcProvider(fetchRequest, null, { polling: true })

const ieContract = new ethers.Contract(SparkImpactEvaluator.ADDRESS, SparkImpactEvaluator.ABI, provider)
const recentParticipantsContract = new ethers.Contract(
  SparkEvaluationsRecentParticipants.ADDRESS,
  SparkEvaluationsRecentParticipants.ABI,
  provider
)

await pgPoolStats.query('DELETE FROM daily_reward_transfers')

await Promise.all([
  observeTransferEvents(pgPoolStats, ieContract, provider),
  observeScheduledRewards(pgPoolStats, ieContract, recentParticipantsContract)
])

await pgPoolStats.end()
