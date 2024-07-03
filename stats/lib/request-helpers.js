import assert from 'http-assert'
import { json } from 'http-responders'
import { URLSearchParams } from 'node:url'
import pg from 'pg'

/** @typedef {import('@filecoin-station/spark-stats-db').Queryable} Queryable */

export const getDayAsISOString = (d) => d.toISOString().split('T')[0]

export const today = () => getDayAsISOString(new Date())

/** @typedef {import('@filecoin-station/spark-stats-db').PgPools} PgPools */
/**
 * @template {import('./typings.js').DateRangeFilter} FilterType
 * @param {string} pathname
 * @param {object} pathParams
 * @param {URLSearchParams} searchParams
 * @param {import('node:http').ServerResponse} res
 * @param {PgPools} pgPools
 * @param {(pgPools: PgPools, filter: FilterType, pathParams: object) => Promise<object[]>} fetchStatsFn
 */
export const getStatsWithFilterAndCaching = async (pathname, pathParams, searchParams, res, pgPools, fetchStatsFn) => {
  const filter = Object.fromEntries(searchParams)
  let shouldRedirect = false

  // Provide default values for "from" and "to" when not specified

  if (!filter.to) {
    filter.to = today()
    shouldRedirect = true
  }
  if (!filter.from) {
    filter.from = filter.to
    shouldRedirect = true
  }
  if (shouldRedirect) {
    res.setHeader('cache-control', `public, max-age=${600 /* 10min */}`)
    res.setHeader('location', `${pathname}?${new URLSearchParams(Object.entries(filter))}`)
    res.writeHead(302) // Found
    res.end()
    return
  }

  // Trim time from date-time values that are typically provided by Grafana

  const matchFrom = filter.from.match(/^(\d{4}-\d{2}-\d{2})(T\d{2}:\d{2}:\d{2}\.\d{3}Z)?$/)
  assert(matchFrom, 400, '"from" must have format YYYY-MM-DD or YYYY-MM-DDThh:mm:ss.sssZ')
  if (matchFrom[2]) {
    filter.from = matchFrom[1]
    shouldRedirect = true
  }

  const matchTo = filter.to.match(/^(\d{4}-\d{2}-\d{2})(T\d{2}:\d{2}:\d{2}\.\d{3}Z)?$/)
  assert(matchTo, 400, '"to" must have format YYYY-MM-DD or YYYY-MM-DDThh:mm:ss.sssZ')
  if (matchTo[2]) {
    filter.to = matchTo[1]
    shouldRedirect = true
  }

  if (shouldRedirect) {
    res.setHeader('cache-control', `public, max-age=${24 * 3600 /* one day */}`)
    res.setHeader('location', `${pathname}?${new URLSearchParams(Object.entries(filter))}`)
    res.writeHead(301) // Found
    res.end()
    return
  }

  // We have well-formed from & to dates now, let's fetch the requested stats from the DB

  // Workaround for the following TypeScript error:
  // Argument of type '{ [k: string]: string; }' is not assignable to parameter
  //   of type 'FilterType'.
  // 'FilterType' could be instantiated with an arbitrary type which could be
  //   unrelated to '{ [k: string]: string; }'
  const typedFilter = /** @type {FilterType} */(/** @type {unknown} */(filter))
  const stats = await fetchStatsFn(pgPools, typedFilter, pathParams)
  setCacheControlForStatsResponse(res, typedFilter)
  json(res, stats)
}

/**
 * @param {import('node:http').ServerResponse} res
 * @param {import('./typings.js').DateRangeFilter} filter
 */
const setCacheControlForStatsResponse = (res, filter) => {
  // We cannot simply compare filter.to vs today() because there may be a delay in finalizing
  // stats for the previous day. Let's allow up to one hour for the finalization.
  const boundary = getDayAsISOString(new Date(Date.now() - 3600_000))

  if (filter.to >= boundary) {
    // response includes partial data for today, cache it for 10 minutes only
    res.setHeader('cache-control', 'public, max-age=600')
  } else {
    // historical data should never change, cache it for one year
    res.setHeader('cache-control', `public, max-age=${365 * 24 * 3600}, immutable`)
  }
}

/**
 * @param {object} args
 * @param {import('@filecoin-station/spark-stats-db').Queryable} args.pgPool
 * @param {string} args.table
 * @param {string} args.column
 * @param {import('./typings.js').DateRangeFilter} args.filter
 * @param {string} [args.asColumn]
 */
export const getDailyDistinctCount = async ({
  pgPool,
  table,
  column,
  filter,
  asColumn = null
}) => {
  if (!asColumn) asColumn = column + '_count'
  const safeTable = pg.escapeIdentifier(table)
  const safeColumn = pg.escapeIdentifier(column)
  const safeAsColumn = pg.escapeIdentifier(asColumn)

  // Fetch the "day" (DATE) as a string (TEXT) to prevent node-postgres from converting it into
  // a JavaScript Date with a timezone, as that could change the date one day forward or back.
  const { rows } = await pgPool.query(`
    SELECT day::TEXT, COUNT(DISTINCT ${safeColumn})::INT as ${safeAsColumn}
    FROM ${safeTable}
    WHERE day >= $1 AND day <= $2
    GROUP BY day
    ORDER BY day
  `, [filter.from, filter.to])
  return rows
}

/**
 * @param {object} args
 * @param {Queryable} args.pgPool
 * @param {string} args.table
 * @param {string} args.column
 * @param {import('./typings.js').DateRangeFilter} args.filter
 * @param {string} [args.asColumn]
 */
export const getMonthlyDistinctCount = async ({
  pgPool,
  table,
  column,
  filter,
  asColumn = null
}) => {
  if (!asColumn) asColumn = column + '_count'
  const safeTable = pg.escapeIdentifier(table)
  const safeColumn = pg.escapeIdentifier(column)
  const safeAsColumn = pg.escapeIdentifier(asColumn)

  // Fetch the "day" (DATE) as a string (TEXT) to prevent node-postgres from converting it into
  // a JavaScript Date with a timezone, as that could change the date one day forward or back.
  const { rows } = await pgPool.query(`
    SELECT
      date_trunc('month', day)::DATE::TEXT as month,
      COUNT(DISTINCT ${safeColumn})::INT as ${safeAsColumn}
    FROM ${safeTable}
    WHERE
      day >= date_trunc('month', $1::DATE)
      AND day < date_trunc('month', $2::DATE) + INTERVAL '1 month'
    GROUP BY month
    ORDER BY month
  `, [filter.from, filter.to]
  )
  return rows
}
