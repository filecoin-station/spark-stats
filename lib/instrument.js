import Sentry from '@sentry/node'

Sentry.init({
  dsn: 'https://47b65848a6171ecd8bf9f5395a782b3f@o1408530.ingest.sentry.io/4506576125427712',
  release: pkg.version,
  environment: SENTRY_ENVIRONMENT,
  tracesSampleRate: 0.1
})
