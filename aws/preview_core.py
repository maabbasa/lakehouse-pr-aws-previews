"""Engine-level preview logic shared by EMR and the local integration test."""
import re
import hashlib

TABLES = ('orders', 'customers', 'order_totals')

def identifier(value):
    if not re.fullmatch(r'[a-z][a-z0-9_]{0,110}', value):
        raise ValueError(f'Unsafe SQL identifier: {value!r}')
    return value

def owned_run(value):
    if not re.fullmatch(r'pr_[1-9][0-9]*_[0-9a-f]{12}_r[1-9][0-9]*', value):
        raise ValueError('Expected pr_NUMBER_COMMIT12_rATTEMPT')
    return value

def snapshot(spark, table):
    return int(spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").first()[0])

def totals(spark, table):
    return [r.asDict() for r in spark.sql(f'SELECT region, orders, revenue_cents FROM {table} ORDER BY region').collect()]

def seed(spark, namespace):
    identifier(namespace)
    root = f'lake.{namespace}'
    # CREATE without IF NOT EXISTS deliberately refuses to reset existing data.
    spark.sql(f"CREATE TABLE {root}.customers USING iceberg TBLPROPERTIES ('format-version'='2') AS SELECT CAST(id AS BIGINT) customer_id, CASE WHEN id % 3 = 0 THEN 'EU' WHEN id % 3 = 1 THEN 'UK' ELSE 'US' END region FROM range(100)")
    spark.sql(f"CREATE TABLE {root}.orders USING iceberg TBLPROPERTIES ('format-version'='2') AS SELECT id order_id, id % 100 customer_id, CASE WHEN id % 10 = 0 THEN 'CANCELLED' ELSE 'COMPLETE' END status, CAST(1000 + (id % 100) * 25 AS BIGINT) amount_cents FROM range(10000)")
    spark.sql(f"CREATE TABLE {root}.order_totals USING iceberg TBLPROPERTIES ('format-version'='2') AS SELECT c.region, count(*) orders, sum(o.amount_cents) revenue_cents FROM {root}.orders o JOIN {root}.customers c ON o.customer_id=c.customer_id GROUP BY c.region")

def cleanup(spark, namespace, run):
    identifier(namespace); owned_run(run)
    root = f'lake.{namespace}'
    for table in ('orders', 'customers'):
        spark.sql(f'ALTER TABLE {root}.{table} DROP TAG IF EXISTS {run}_input')
    for suffix in ('base', 'candidate'):
        spark.sql(f'ALTER TABLE {root}.order_totals DROP BRANCH IF EXISTS {run}_{suffix}')

def preview(spark, namespace, run, baseline_sql, candidate_sql, persist=lambda x: None):
    identifier(namespace); owned_run(run)
    root = f'lake.{namespace}'
    pins = {t: snapshot(spark, f'{root}.{t}') for t in TABLES}
    manifest = {'run': run, 'namespace': namespace, 'snapshots': pins, 'status': 'captured',
                'sql_sha256': {'baseline': hashlib.sha256(baseline_sql.encode()).hexdigest(),
                               'candidate': hashlib.sha256(candidate_sql.encode()).hexdigest()}}
    persist(manifest)
    # A manifest is a set of per-table snapshots, not a cross-table transaction.
    for table in ('orders', 'customers'):
        spark.sql(f'ALTER TABLE {root}.{table} CREATE TAG {run}_input AS OF VERSION {pins[table]} RETAIN 1 DAYS')
        spark.sql(f'CREATE OR REPLACE TEMP VIEW input_{table} AS SELECT * FROM {root}.{table} VERSION AS OF {pins[table]}')
    for suffix, sql in (('base', baseline_sql), ('candidate', candidate_sql)):
        branch = f'{run}_{suffix}'
        spark.sql(f'ALTER TABLE {root}.order_totals CREATE BRANCH {branch} AS OF VERSION {pins["order_totals"]} RETAIN 1 DAYS')
        result = spark.sql(sql.strip().rstrip(';'))
        result.createOrReplaceTempView('proposed_output')
        spark.sql(f'INSERT OVERWRITE {root}.order_totals.branch_{branch} SELECT region, orders, revenue_cents FROM proposed_output')
    before = totals(spark, f'{root}.order_totals.branch_{run}_base')
    after = totals(spark, f'{root}.order_totals.branch_{run}_candidate')
    base = {r['region']: r for r in before}; candidate = {r['region']: r for r in after}
    diff = [{'region': r, 'orders_before': base.get(r, {}).get('orders', 0),
             'orders_after': candidate.get(r, {}).get('orders', 0),
             'revenue_delta_cents': candidate.get(r, {}).get('revenue_cents', 0)-base.get(r, {}).get('revenue_cents', 0)}
            for r in sorted(base.keys() | candidate.keys())]
    # Sandbox gate: a concurrent main write also fails rather than being misattributed.
    checks = {'main_snapshot_unchanged': snapshot(spark, f'{root}.order_totals') == pins['order_totals'],
              'candidate_nonempty': bool(after),
              'regions_unique': len(after) == len(candidate),
              'counts_nonnegative': all(r['orders'] >= 0 for r in after)}
    manifest.update(status='compared', baseline=before, candidate=after, diff=diff, checks=checks,
                    spark_version=spark.version)
    persist(manifest)
    if not all(checks.values()):
        raise AssertionError(f'Preview checks failed: {checks}')
    return manifest
