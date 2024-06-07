/**
 * @param {import('pg').Client} pgClient
 * @param {Object} participant
 * @param {string} participant.address
 * @param {number} participant.amount
 */
export const updateDailyScheduledRewardsStats = async (pgClient, participant) => {
  await pgClient.query(`
    INSERT INTO daily_scheduled_rewards (day, address, amount)
    VALUES (now(), $1, $2)
    ON CONFLICT (day, address) DO UPDATE SET
      amount = EXCLUDED.amount
  `, [participant.address, participant.amount])
}
