# spark-stats

API exposing public statistics about Spark

## Public API

Base URL: http://stats.filspark.com/

- `GET /retrieval-success-rate?from=2024-01-01&to=2024-01-31`

  http://stats.filspark.com/retrieval-success-rate

- `GET /retrieval-success-rate?from=2024-01-01&to=2024-01-31&nonZero=true`

  _Miners with no successful retrievals are excluded from the RSR calculation._

  http://stats.filspark.com/retrieval-success-rate?nonZero=true

- `GET /miners/retrieval-success-rate/summary?from=<day>&to=<day>`

  http://stats.filspark.com/miners/retrieval-success-rate/summary

- `GET /miner/:id/deals/eligible/summary`

  http://stats.filspark.com/miner/f0814049/deals/eligible/summary

- `GET /client/:id/deals/eligible/summary`

  http://stats.filspark.com/client/f0215074/deals/eligible/summary

- `GET /participants/daily?from=<day>&to=<day>`

  http://stats.filspark.com/participants/daily

- `GET /participants/monthly?from=<day>&to=<day>`

  http://stats.filspark.com/participants/monthly

- `GET /participants/change-rates?from=<day>&to=<day>`

  http://stats.filspark.com/participants/change-rates

- `GET /participants/top-earning`

  http://stats.filspark.com/participants/top-earning

- `GET /participants/top-measurements`

  http://stats.filspark.com/participants/top-measurements?from=yesterday&to=yesterday

- `GET /participant/:address/scheduled-rewards?address=<address>&from=<day>&to=<day>`

  http://stats.filspark.com/participant/0x000000000000000000000000000000000000dEaD/scheduled-rewards

- `GET /participant/:address/reward-transfers?from=<day>&to=<day>`

  http://stats.filspark.com/participant//0x000000000000000000000000000000000000dEaD/reward-transfers


- `GET /stations/daily?from=<day>&to=<day>`

  http://stats.filspark.com/stations/daily

- `GET /stations/monthly?from=<day>&to=<day>`

  http://stats.filspark.com/stations/monthly

- `GET /measurements/daily?from=<day>&to=<day>`

  http://stats.filspark.com/measurements/daily

- `GET /transfers/daily?from=<day>&to=<day>`

  http://stats.filspark.com/transfers/daily

- `GET /deals/daily?from=2024-01-01&to=2024-01-31`

  http://stats.filspark.com/deals/daily

- `GET /deals/summary?from=2024-01-01&to=2024-01-31`

  http://stats.filspark.com/deals/summary


## Development

### Database

Set up [PostgreSQL](https://www.postgresql.org/) with default settings:
 - Port: 5432
 - User: _your system user name_
 - Password: _blank_
 - Database: spark_stats

Alternatively, set the environment variable `$DATABASE_URL` with
`postgres://${USER}:${PASS}@${HOST}:${POST}/${DATABASE}`.

The Postgres user and database need to exist already, and the user
needs full management permissions for the database.

You can also run the following command to set up the PostgreSQL server via Docker:

```bash
docker run -d --name spark-db \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -e POSTGRES_USER=$USER \
  -e POSTGRES_DB=spark_stats \
  -p 5432:5432 \
  postgres
```

Next, you need to create `spark_evaluate` database.

```bash
psql postgres://localhost:5432/ -c "CREATE DATABASE spark_evaluate"
```

Finally, run database schema migration scripts.

```bash
npm run migrate
```

### Run the test suite

```sh
npm test
```

### Run the `spark-stats` service

```sh
npm start -w stats
```

You can also run the service against live data in Spark DB running on Fly.io.

1. Set up a proxy to forward connections to Spark DB Postgres. Connect to the reader replica running
  on port 5433 (not 5432).

  The command below will forward connections to local post 5455 to Spark DB's reader replica.

  ```
  fly proxy 5455:5433 -a spark-db
  ```

2. Start the service and configure the database connection string to use the proxied connection.
  Look up the user and the password in our shared 1Password vault.

  ```bash
  DATABASE_URL="postgres://user:password@localhost:5455/spark_stats" \
    EVALUATE_DB_URL="postgres://user:password@localhost:5455/spark_evaluate" \
    npm start -w stats
  ```

### Run the `spark-observer` service

```sh
npm start -w observer
```

## Deployment

```
git push
```
