export const {
  // DATABASE_URL points to `spark_stats` database managed by this monorepo
  DATABASE_URL = 'postgres://localhost:5432/spark_stats',

  // EVALUATE_DB_URL points to `spark_evaluate` database managed by spark-evaluate repo.
  // Eventually, we should move the code updating stats from spark-evaluate to this repo
  // and then we won't need two connection strings.
  EVALUATE_DB_URL = 'postgres://localhost:5432/spark_evaluate'
} = process.env
