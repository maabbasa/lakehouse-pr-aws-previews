"""EMR Serverless entry point; credentials come from the EMR execution role."""
import argparse
import json
import boto3
from pyspark.sql import SparkSession
from preview_core import seed, preview, cleanup, identifier, owned_run


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--action', choices=['seed', 'preview', 'cleanup'], required=True)
    p.add_argument('--bucket', required=True)
    p.add_argument('--namespace', required=True)
    p.add_argument('--run', required=True)
    p.add_argument('--prefix', required=True)
    args = p.parse_args()
    identifier(args.namespace); owned_run(args.run)
    spark = SparkSession.builder.appName(args.run + '-' + args.action).getOrCreate()
    s3 = boto3.client('s3')
    def persist(data):
        s3.put_object(Bucket=args.bucket, Key=f'reports/{args.run}/report.json',
                      Body=json.dumps(data, indent=2).encode(), ContentType='application/json')
    try:
        if args.action == 'seed':
            seed(spark, args.namespace)
        elif args.action == 'cleanup':
            cleanup(spark, args.namespace, args.run)
        else:
            sql = lambda name: s3.get_object(Bucket=args.bucket, Key=f'{args.prefix}/{name}.sql')['Body'].read().decode()
            preview(spark, args.namespace, args.run, sql('baseline'), sql('candidate'), persist)
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
