"""Real Spark + Iceberg integration. No AWS credentials; local Hadoop catalog."""
import json
import os
from pathlib import Path
import sys
import tempfile
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'aws'))
from pyspark.sql import SparkSession
from preview_core import seed, preview, cleanup, snapshot

root=Path(__file__).resolve().parents[2]
warehouse=tempfile.mkdtemp(prefix='iceberg-pr-')
spark=(SparkSession.builder.master('local[2]').appName('aws-preview-contract')
 .config('spark.jars',os.environ['ICEBERG_JAR'])
 .config('spark.sql.extensions','org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
 .config('spark.sql.catalog.lake','org.apache.iceberg.spark.SparkCatalog')
 .config('spark.sql.catalog.lake.type','hadoop')
 .config('spark.sql.catalog.lake.warehouse',warehouse)
 .config('spark.sql.shuffle.partitions','2').config('spark.ui.enabled','false').getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
spark.sql('CREATE NAMESPACE lake.lab')
seed(spark,'lab')
base=(root/'aws/baseline.sql').read_text();candidate=(root/'aws/candidate.sql').read_text()
run='pr_1_abcdef123456_r1'
main=snapshot(spark,'lake.lab.order_totals')
report=preview(spark,'lab',run,base,candidate)
assert sum(r['orders'] for r in report['baseline'])==10000
assert sum(r['orders'] for r in report['candidate'])==9000
assert sum(r['revenue_cents'] for r in report['candidate'])==20250000
assert all(report['checks'].values())
cleanup(spark,'lab',run);cleanup(spark,'lab',run)
assert snapshot(spark,'lake.lab.order_totals')==main
for table in ['orders','customers','order_totals']:
 assert spark.sql(f'SELECT * FROM lake.lab.{table}.refs').count()==1
# New arrivals after capture must not leak into either transformation.
def append_after_capture(manifest):
 if manifest['status']=='captured':
  spark.sql("INSERT INTO lake.lab.orders VALUES (10001,1,'COMPLETE',999999)")
second=preview(spark,'lab','pr_2_abcdef123456_r1',base,candidate,append_after_capture)
assert second['baseline']==report['baseline'] and second['candidate']==report['candidate']
cleanup(spark,'lab','pr_2_abcdef123456_r1')
# A broken candidate leaves refs that explicit recovery can remove.
try:
 preview(spark,'lab','pr_3_abcdef123456_r1',base,'SELECT missing_column FROM input_orders')
 raise AssertionError('Broken SQL unexpectedly succeeded')
except Exception as error:
 assert 'missing_column' in str(error)
cleanup(spark,'lab','pr_3_abcdef123456_r1')
assert snapshot(spark,'lake.lab.order_totals')==main
report['validation']={'backend':'local Hadoop catalog and local files','iceberg':'1.10.0',
 'cloud_deployment_tested':False,'scenarios':['baseline/candidate diff','main unchanged','new input after capture excluded','failed SQL recovery','idempotent cleanup']}
(root/'docs/results/aws-engine-validation.json').write_text(json.dumps(report,indent=2))
print('PASS: five real Spark/Iceberg integration scenarios')
spark.stop()
