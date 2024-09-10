import assert, { AssertionError } from 'node:assert'
import { mapParticipantsToIds } from 'spark-evaluate/lib/platform-stats.js'

export const assertResponseStatus = async (res, status) => {
  if (res.status !== status) {
    throw new AssertionError({
      actual: res.status,
      expected: status,
      message: await res.text()
    })
  }
}

/**
 * @param {import('http').Server} server
 */
export const getPort = (server) => {
  const address = server.address()
  assert(typeof address === 'object')
  return address.port
}

/**
 * @param {import('@filecoin-station/spark-stats-db').Queryable} pgPool
 * @param {string} day
 * @param {string[]} participantAddresses
 */
export const givenDailyParticipants = async (pgPool, day, participantAddresses) => {
  const ids = await mapParticipantsToIds(pgPool, new Set(participantAddresses))
  await pgPool.query(`
    INSERT INTO daily_participants (day, participant_id)
    SELECT $1 as day, UNNEST($2::INT[]) AS participant_id
    ON CONFLICT DO NOTHING
  `, [
    day,
    Array.from(ids.values())
  ])
}
