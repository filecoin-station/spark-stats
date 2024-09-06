import assert from 'http-assert'
import { json } from 'http-responders'
import { URLSearchParams } from 'node:url'

/** @typedef {import('@filecoin-station/spark-stats-db').Queryable} Queryable */

export const getDayAsISOString = (d) => d.toISOString().split('T')[0]
export const todayDate = () => new Date(Date.now() - new Date().getTimezoneOffset() * 60000)

export const today = () => getDayAsISOString(todayDate())
export const yesterday = () => getDayAsISOString(new Date(todayDate().getTime() - 24 * 60 * 60 * 1000))

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

  filter.from = handleDateKeyword(filter.from)
  filter.to = handleDateKeyword(filter.to)

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
  const boundary = getDayAsISOString(new Date(todayDate().getTime() - 3600_000))

  if (filter.to >= boundary) {
    // response includes partial data for today, cache it for 10 minutes only
    res.setHeader('cache-control', 'public, max-age=600')
  } else {
    // historical data should never change, cache it for one year
    res.setHeader('cache-control', `public, max-age=${365 * 24 * 3600}, immutable`)
  }
}

/**
 * @param {string} date
 * @returns {string}
 */
const handleDateKeyword = (date) => {
  switch (date) {
    case 'today':
      return today()
    case 'yesterday':
      return yesterday()
    default:
      return date
  }
}
