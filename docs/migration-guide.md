# Migrating Talend jobs into this template

This is a guide for people, not a converter. Nothing here reads Talend's `.item` files or
generates code from them. What it gives you is a place for each thing a Talend job does, a
way to prove the new pipeline produces what the old job produced, and an order to do the
work in.

It is written for Talend because that is the migration I did. Most of it applies to any
scheduled ETL tool or folder of cron scripts.

## Where each Talend concept goes

| In Talend | In this template |
|---|---|
| A Job | A folder under `pipelines/`, holding a `Pipeline` definition and its SQL |
| A subjob | A task |
| `OnSubjobOk`, `OnComponentOk` triggers | `after=("task_name",)` on the next task |
| `Run if` trigger | A `QualityChecks` task that stops the run, or a condition inside the SQL |
| `tPrejob`, `tPostjob` | The first task (`create_tables`) and the last (`SetWatermark`) |
| Context variables, and context groups such as Dev and Prod | `config/<env>.yml`, chosen by `PLATFORM_ENV`, with values from environment variables |
| Repository metadata connections | Named connections: `warehouse`, `source`, `landing` |
| `tFileInputDelimited`, `tFileList` | `LoadCsv` with a path such as `loans/loans_{{ ds }}.csv` |
| `tS3Get`, `tS3Put` and their kin | The object store behind a named connection: a folder locally, a bucket in the cloud |
| `tDBInput` (`tPostgresqlInput`, `tRedshiftInput`, ...) | `CopyQuery`, which runs a query on the source and lands the result in staging |
| `tDBOutput` with "insert or update" | `Upsert` |
| `tRedshiftOutputBulkExec` | `RedshiftWarehouse.copy_from_store` |
| `tMap` with its joins, lookups, expressions and filters | A `.sql` file run by `RunSql` |
| `tFilterRow`, `tAggregateRow`, `tSortRow`, `tUniqRow`, `tJoin` | Clauses in that same SQL file |
| `tJavaRow`, routines | SQL expressions. A small task class in Python when SQL cannot express it |
| `tDBSCD` | `Scd2` |
| `tRunJob` (a parent job calling children) | Separate pipelines, with `upstream=("other_pipeline",)` on the one that depends |
| A context variable holding the last run's timestamp | A watermark, moved only after the load has been checked |
| `tLogCatcher`, `tStatCatcher`, `tFlowMeterCatcher` | The `platform.run_log` table, plus Airflow's own logs |
| `tDie`, `tWarn`, `tAssert` | An exception, which fails the task, or a `QualityChecks` task |
| `tSendMail` on failure | Airflow's notifications. Not set up in this template |
| A joblet | A task class of your own in `src/dataplatform/tasks/` |
| Scheduling in the Talend Administration Center | `schedule=` on the pipeline, which becomes the Airflow schedule |

The biggest change is the `tMap`. In Talend, rows are pulled out of the database, pushed
through Java, and written back. Here the data is loaded as it is and transformed where it
lives, in SQL. That is usually faster, and it puts the logic in a text file that can be
reviewed and diffed.

## The order to work in

### 1. Take an inventory

List every job: what it reads, what it writes, when it runs, what calls it, and who looks
at its output. Half the value of a migration is finding the jobs nobody needs.

### 2. Sort the jobs into the three shapes

Almost every job is one of these, and each example pipeline is one of them.

| Shape | Example here | Signs in the Talend job |
|---|---|---|
| File in, table out | `loans_daily` | `tFileInput*`, a `tMap`, a `tDBOutput` |
| Database to database, only what changed | `payments_incremental` | `tDBInput` with a date filter from a context variable |
| A dimension that keeps its history | `customers_history` | `tDBSCD`, or a `tMap` with a lookup against the target |

### 3. Migrate one job of each shape first

```bash
dp new-pipeline vendor_invoices
```

That creates the pipeline, its SQL, its DAG file and a test. Then:

1. Put the table definitions in `sql/create_tables.sql`.
2. Translate the `tMap` into `sql/transform.sql`. Do it expression by expression, and keep
   the Talend expression beside the SQL in a comment until the outputs match.
3. Replace the placeholder checks with ones that describe this data: its key, its
   required columns, its allowed values.
4. Write the test from a small sample of real input and the output the old job gave for it.
5. Run it: `dp run vendor_invoices --date 2026-01-05`.

### 4. Run old and new side by side

Do not switch over on the strength of a test. For a week or two, let the Talend job keep
writing the real tables and let the new pipeline write to separate schemas. Change
`schemas:` in the environment's config file, so nothing in the pipeline changes:

```yaml
schemas:
  staging: mig_staging
  core: mig_core
  marts: mig_marts
```

Each day, compare the two sets of tables: row counts, sums of the money columns, and a
row-by-row diff on the key. My [reconkit](https://github.com/linusdominic/reconkit)
project does that diff without downloading both tables. Every difference is either a bug
in the new pipeline or a bug in the old job that nobody had noticed. Write down which.

### 5. Switch over, one pipeline at a time

Point `schemas:` back at the real schemas, disable the job in Talend, and enable the DAG.
Keep the Talend job, disabled, until a month-end has passed.

### 6. Remove the old job

Only then delete it. A migration that leaves both systems running has doubled the work.

## A checklist for each job

- [ ] The pipeline has an owner and a one-sentence description
- [ ] Every table it writes is created by `create_tables.sql`, so it runs in an empty schema
- [ ] Running it twice for the same date leaves the tables identical
- [ ] Running an older date after a newer one is either safe or refused
- [ ] A missing or empty input fails loudly
- [ ] It has checks on its key, its required columns and its row count
- [ ] An incremental load reconciles against its source before the watermark moves
- [ ] No connection string, user name or password is in the code or the config file
- [ ] It has a test built from real input and the old job's real output
- [ ] It ran beside the old job, and every difference was explained

## What you will have to decide yourself

- **Deletes.** `tDBOutput` can delete rows. The example pipelines never do. If the source
  deletes rows, decide whether the warehouse should keep them, flag them or remove them.
- **Late data.** The examples treat the logical date as closed at midnight. If files arrive
  late, either re-run the date, which is safe, or widen the window.
- **Very large loads.** `LoadCsv` and `CopyQuery` move rows through Python, which is fine
  for the sizes most jobs handle and wrong for hundreds of millions of rows. For those,
  write the file to the object store and bulk load it (`COPY` on Redshift).
- **Java logic that SQL cannot express.** Write a task class for it. A task is any object
  with a `name`, an `after` and a `run(ctx)` method.
