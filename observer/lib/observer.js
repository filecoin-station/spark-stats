import { updateDailyTransferStats } from './platform-stats.js'
import * as Sentry from '@sentry/node'

/**
 * Observe the transfer events on the Filecoin blockchain
 * @param {import('@filecoin-station/spark-stats-db').Queryable} pgPoolStats
 * @param {import('ethers').Contract} ieContract
 * @param {import('ethers').Provider} provider
 */
export const observeTransferEvents = async (pgPoolStats, ieContract, provider) => {
  const { rows } = await pgPoolStats.query(
    'SELECT MAX(last_checked_block) AS last_checked_block FROM daily_reward_transfers'
  )
  let queryFromBlock = rows[0].last_checked_block + 1
  const currentBlockNumber = await provider.getBlockNumber()

  if (!queryFromBlock || queryFromBlock < currentBlockNumber - 1900) {
    queryFromBlock = currentBlockNumber - 1900
    console.log('Block number too old, GLIF only provides last 2000 blocks, querying from -1900')
  }

  console.log('Querying impact evaluator Transfer events after block', queryFromBlock)
  const events = await ieContract.queryFilter(ieContract.filters.Transfer(), queryFromBlock)

  console.log(`Found ${events.length} Transfer events`)
  for (const event of events.filter(isEventLog)) {
    const transferEvent = {
      toAddress: event.args.to,
      amount: event.args.amount
    }
    console.log('Transfer event:', transferEvent)
    await updateDailyTransferStats(pgPoolStats, transferEvent, currentBlockNumber)
  }

  return events.length
}

/**
 * Observe scheduled rewards on the Filecoin blockchain
 * @param {import('@filecoin-station/spark-stats-db').Queryable} pgPoolStats
 * @param {import('ethers').Contract} ieContract
 * @param {import('ethers').Contract} recentParticipantsContract
 */
export const observeScheduledRewards = async (pgPoolStats, ieContract, recentParticipantsContract) => {
  console.log('Querying scheduled rewards from impact evaluator')
  const participants = await recentParticipantsContract.get()
  const participantsSeen = new Map()
  for (const address of participants) {
    // participants contains duplicates
    if (participantsSeen.has(address)) {
      continue
    }
    participantsSeen.set(address, true)

    let scheduledRewards
    try {
      scheduledRewards = await ieContract.rewardsScheduledFor(address)
    } catch (err) {
      Sentry.captureException(err)
      console.error(
        'Error querying scheduled rewards for',
        address,
        { cause: err }
      )
      continue  
    }
    console.log('Scheduled rewards for', address, scheduledRewards)
    await pgPoolStats.query(`
      INSERT INTO daily_scheduled_rewards
      (day, participant_address, scheduled_rewards)
      VALUES (now(), $1, $2)
      ON CONFLICT (day, participant_address) DO UPDATE SET
      scheduled_rewards = EXCLUDED.scheduled_rewards
    `, [address, scheduledRewards])
  }
}

/**
 * @param {import('ethers').Log | import('ethers').EventLog} logOrEventLog
 * @returns {logOrEventLog is import('ethers').EventLog}
 */
function isEventLog (logOrEventLog) {
  return 'args' in logOrEventLog
}
