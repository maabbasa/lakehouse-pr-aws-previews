"""Upload a commit-scoped bundle, submit EMR jobs, collect a review report."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import time
import uuid
import boto3
from preview_core import identifier, owned_run

HERE = Path(__file__).resolve().parent
TERMINAL = {'SUCCESS', 'FAILED', 'CANCELLED'}


def spark_parameters(bucket):
    # Use the EMR-distributed Iceberg runtime; do not mix a second Iceberg JAR in.
    values = {
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.lake': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.lake.catalog-impl': 'org.apache.iceberg.aws.glue.GlueCatalog',
        'spark.sql.catalog.lake.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
        'spark.sql.catalog.lake.warehouse': f's3://{bucket}/warehouse/',
        'spark.sql.shuffle.partitions': '2',
        'spark.driver.cores': '1', 'spark.driver.memory': '2g',
        'spark.executor.cores': '1', 'spark.executor.memory': '2g',
        'spark.dynamicAllocation.enabled': 'false', 'spark.executor.instances': '2',
    }
    return '--jars /usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar ' + ' '.join(f'--conf {k}={v}' for k,v in values.items())


def request(args, action):
    prefix = f'assets/{args.run}'
    return dict(applicationId=args.application, executionRoleArn=args.role,
        clientToken=(uuid.uuid4().hex if action == 'cleanup' else hashlib.sha256(f'{args.application}/{args.run}/{action}'.encode()).hexdigest()),
        name=f'{args.run}-{action}', executionTimeoutMinutes=20,
        jobDriver={'sparkSubmit': {
            'entryPoint': f's3://{args.bucket}/{prefix}/job.py',
            'entryPointArguments': ['--action', action, '--bucket', args.bucket, '--namespace', args.namespace,
                                    '--run', args.run, '--prefix', prefix],
            'sparkSubmitParameters': spark_parameters(args.bucket) + f' --py-files s3://{args.bucket}/{prefix}/preview_core.py'}},
        configurationOverrides={'monitoringConfiguration': {
            's3MonitoringConfiguration': {'logUri': f's3://{args.bucket}/logs/{args.run}/'}}})


def wait(emr, application, job, timeout=1500, interval=10):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = emr.get_job_run(applicationId=application, jobRunId=job)['jobRun']
        if result['state'] in TERMINAL:
            if result['state'] != 'SUCCESS':
                raise RuntimeError(f"EMR {job}: {result['state']}: {result.get('stateDetails', '')}")
            return result
        time.sleep(interval)
    emr.cancel_job_run(applicationId=application, jobRunId=job)
    # Never start cleanup while the old writer may still be running.
    cancel_end = time.monotonic() + 300
    while time.monotonic() < cancel_end:
        state = emr.get_job_run(applicationId=application, jobRunId=job)['jobRun']['state']
        if state in TERMINAL:
            raise TimeoutError('Job exceeded controller deadline; cancellation confirmed')
        time.sleep(interval)
    raise RuntimeError('Cancellation not confirmed; run cleanup only after the job is terminal')


def markdown(report):
    lines = ['# Output preview', '', f"Run: `{report['run']}`", '',
             '| Region | Orders before | Orders after | Revenue change (cents) |',
             '|---|---:|---:|---:|']
    for row in report['diff']:
        lines.append(f"| {row['region']} | {row['orders_before']} | {row['orders_after']} | {row['revenue_delta_cents']} |")
    lines += ['', 'Checks: ' + ', '.join(f'{k}={v}' for k,v in report['checks'].items()), '',
              'Input snapshot IDs and complete results are in report.json. Revenue uses integer cents.']
    return '\n'.join(lines) + '\n'


def main():
    p = argparse.ArgumentParser()
    for name in ('bucket', 'application', 'role', 'namespace', 'run'):
        p.add_argument('--' + name, required=True)
    p.add_argument('--action', choices=['seed', 'preview', 'cleanup'], default='preview')
    p.add_argument('--baseline', type=Path, default=HERE/'baseline.sql')
    args = p.parse_args()
    identifier(args.namespace); owned_run(args.run)
    # Bucket names cannot carry spaces or shell/Spark option injection.
    import re
    if not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', args.bucket):
        raise ValueError('Invalid S3 bucket')
    s3, emr = boto3.client('s3'), boto3.client('emr-serverless')
    prefix = f'assets/{args.run}'
    if args.action != 'cleanup':
        for name in ('job.py', 'preview_core.py', 'candidate.sql'):
            s3.upload_file(str(HERE/name), args.bucket, f'{prefix}/{name}')
        s3.upload_file(str(args.baseline), args.bucket, f'{prefix}/baseline.sql')
    if args.action == 'cleanup':
        previous = Path('artifacts/aws-job.json')
        if previous.exists():
            saved = json.loads(previous.read_text())
            if saved['run'] == args.run and saved['application'] == args.application:
                state = emr.get_job_run(applicationId=args.application, jobRunId=saved['job'])['jobRun']['state']
                if state not in TERMINAL:
                    raise RuntimeError('Writer is still active; cancel it and wait for a terminal state before cleanup')
    response = emr.start_job_run(**request(args, args.action))
    job = response['jobRunId']
    Path('artifacts').mkdir(exist_ok=True)
    Path('artifacts/aws-job.json').write_text(json.dumps({'application': args.application, 'job': job, 'run': args.run}))
    print(f'EMR job: {job}', flush=True)
    wait(emr, args.application, job)
    if args.action == 'preview':
        report = json.loads(s3.get_object(Bucket=args.bucket, Key=f'reports/{args.run}/report.json')['Body'].read())
        Path('artifacts/aws-report.json').write_text(json.dumps(report, indent=2))
        summary = markdown(report)
        Path('artifacts/aws-report.md').write_text(summary)
        if os.getenv('GITHUB_STEP_SUMMARY'):
            with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as f: f.write(summary)
        print(summary)

if __name__ == '__main__':
    main()
