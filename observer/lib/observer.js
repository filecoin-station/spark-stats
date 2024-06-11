import { updateDailyTransferStats } from './platform-stats.js'
import * as Sentry from '@sentry/node'

/**
 * @param {import('../../common/typings').pgPools} pgPools
 * @param {import('ethers').Contract} ieContract
 * @param {import('ethers').Provider} provider
 */
export const observe = async (pgPools, ieContract, provider) => {
  await Promise.all([
    observeTransferEvents(pgPools.stats, ieContract, provider),
    observeScheduledRewards(pgPools, ieContract)
  ])
}

/**
 * Observe the transfer events on the Filecoin blockchain
 * @param {import('pg').Pool} pgPoolStats
 * @param {import('ethers').Contract} ieContract
 * @param {import('ethers').Provider} provider
 */
const observeTransferEvents = async (pgPoolStats, ieContract, provider) => {
  const { rows } = await pgPoolStats.query(
    'SELECT MAX(last_checked_block) FROM daily_reward_transfers'
  )
  const lastCheckedBlock = rows[0].last_checked_block

  console.log('Querying impact evaluator Transfer events after block', lastCheckedBlock)
  let events
  try {
    events = await ieContract.queryFilter(ieContract.filters.Transfer(), lastCheckedBlock)
  } catch (error) {
    console.error('Error querying impact evaluator Transfer events', error)
    if (error.message.includes('bad tipset height')) {
      console.log('Block number too old, GLIF only provides last 2000 blocks, querying from -1900')
      events = await ieContract.queryFilter(ieContract.filters.Transfer(), -1900)
    } else {
      throw error
    }
  }
  const currentBlockNumber = await provider.getBlockNumber()
  console.log('Current block number:', currentBlockNumber)
  console.log(`Found ${events.length} Transfer events`)
  for (const event of events) {
    const transferEvent = {
      to_address: event.args.to,
      amount: event.args.amount
    }
    console.log('Transfer event:', transferEvent)
    await updateDailyTransferStats(pgPoolStats, transferEvent, currentBlockNumber)
  }
}

/**
 * Observe scheduled rewards on the Filecoin blockchain
 * @param {import('../../common/typings').pgPools} pgPools
 * @param {import('ethers').Contract} ieContract
 */
const observeScheduledRewards = async (pgPools, ieContract) => {
  console.log('Querying scheduled rewards from impact evaluator')
  const rows = await pgPools.evaluate.query(`
    SELECT participant_address
    FROM participants
    JOIN daily_participants USING (participant_id)
    WHERE day >= now() - interval '3 days'
  `)
  for (const { participant_address: address } of rows) {
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
    await pgPools.stats.query(`
      INSERT INTO daily_scheduled_rewards
      (day, participant_address, scheduled_rewards)
      VALUES (now(), $1, $2)
      ON CONFLICT (day, id) DO UPDATE SET
      scheduled_rewards = EXCLUDED.scheduled_rewards
    `, [address, scheduledRewards])
  }
}
