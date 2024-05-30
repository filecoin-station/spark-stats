/**
 * @param {import('pg').Client} pgClient
 * @param {Object} filEvent
 * @param {string} filEvent.to_address
 * @param {number} filEvent.amount
 * @param {number} filEvent.blockNumber
 */
export const updateDailyFilStats = async (pgClient, filEvent) => {
  await pgClient.query(`
    INSERT INTO daily_reward_transfers (day, to_address, amount)
    VALUES (now(), $1, $2)
    ON CONFLICT (day, to_address) DO UPDATE
    SET amount = daily_reward_transfers.amount + EXCLUDED.amount
  `, [filEvent.to_address, filEvent.amount])

  // Update the last_block in reward_transfer_last_block table
  // For safety, only update if the new block number is greater than the existing one
  await pgClient.query(`
    UPDATE reward_transfer_last_block
    SET last_block = $1
    WHERE $1 > last_block
  `, [filEvent.blockNumber])
}
