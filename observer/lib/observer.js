import { updateDailyTransferStats } from './platform-stats.js'

/**
 * Observe the transfer events on the Filecoin blockchain
 * @param {import('pg').Pool} pgPool
 * @param {import('ethers').Contract} ieContract
 * @param {import('ethers').Provider} provider
 */
export const observeTransferEvents = async (pgPool, ieContract, provider) => {
  // Get the last checked block. Even though there should be only one row, use MAX just to be safe
  const lastCheckedBlock = await pgPool.query(
    'SELECT MAX(last_block) AS last_block FROM reward_transfer_last_block'
  ).then(res => res.rows[0].last_block)

  console.log('Querying impact evaluator Transfer events after block', lastCheckedBlock)
  let events
  try {
    events = await ieContract.queryFilter(ieContract.filters.Transfer(), lastCheckedBlock)
  } catch (error) {
    console.error('Error querying impact evaluator Transfer events', error)
    if (error.message.includes('bad tipset height')) {
      console.log('Block number too old, GLIF only provides last 2000 blocks, querying from there')
      events = await ieContract.queryFilter(ieContract.filters.Transfer(), -1999)
    } else {
      throw error
    }
  }
  console.log(`Found ${events.length} Transfer events`)
  for (const event of events) {
    const transferEvent = {
      to_address: event.args.to,
      amount: event.args.amount
    }
    console.log('Transfer event:', transferEvent)
    await updateDailyTransferStats(pgPool, transferEvent)
  }

  // Get the current block number and update the last_block in reward_transfer_last_block table
  // For safety, only update if the new block number is greater than the existing one
  const blockNumber = await provider.getBlockNumber()
  console.log('Current block number:', blockNumber)
  await pgPool.query(`
    UPDATE reward_transfer_last_block
    SET last_block = $1
    WHERE $1 > last_block
  `, [blockNumber])
}
