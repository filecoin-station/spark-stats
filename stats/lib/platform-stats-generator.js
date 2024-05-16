/**
 * @param {import('pg').Client} pgClient
 * @param {Object} filEvent
 */
export const updateDailyFilStats = async (pgClient, filEvent) => {
  console.log('Event:', filEvent)

  await pgClient.query(`
    INSERT INTO daily_fil (day, to_address, amount)
    VALUES (now(), $1, $2)
    ON CONFLICT (day, to_address) DO UPDATE
    SET amount = daily_fil.amount + EXCLUDED.amount
  `, [filEvent.to_address, filEvent.amount])
}
