# Preview transformation output on AWS

EMR Serverless runs two versions of a Spark SQL transformation against pinned Iceberg input snapshots. Glue is the catalog and S3 stores the data. Each run uses two Iceberg output branches. The comparison is saved as JSON and a GitHub Actions summary. No Nessie, Docker cluster, Trino service or long-lived access keys are required for this AWS path.

## Prerequisites

- An AWS sandbox account and a Region supporting EMR Serverless 7.12.0.
- AWS CLI v2 authenticated locally (`aws configure sso`, then `aws sso login --profile YOUR_PROFILE`). Set `AWS_PROFILE` and `AWS_DEFAULT_REGION` for that session. Verify with `aws sts get-caller-identity`.
- Python 3.11+, Git and a GitHub repository containing this package.
- Deployment permissions for CloudFormation, S3, IAM, Glue and EMR Serverless, including creation of its service-linked role if this is your first application.
- This example uses IAM-authorized Glue tables. An account enforcing Lake Formation permissions may require corresponding grants; it does not configure Lake Formation governance.

## 1. Deploy the isolated sandbox

```bash
aws cloudformation deploy \
  --template-file aws/template.yaml \
  --stack-name iceberg-pr-preview \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides GitHubRepository=maabbasa/lakehouse-pr-aws-previews

aws cloudformation describe-stacks --stack-name iceberg-pr-preview \
  --query 'Stacks[0].Outputs' --output table
```

If your account already has the GitHub OIDC provider, pass `ExistingOidcProviderArn=arn:aws:iam::ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com` in the parameter overrides. IAM permits only one provider for that URL per account.

Copy the output values into your shell:

```bash
export BUCKET='BucketName output'
export APP='ApplicationId output'
export ROLE='ExecutionRoleArn output'
export DATABASE='DatabaseName output'
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r aws/requirements.txt
```

The template creates a new bucket, catalog database, EMR application and scoped roles. It does not attach to your production database. EMR has no pre-initialized workers, uses at most 8 vCPU/32 GB, and stops after five idle minutes. Jobs have a 20-minute execution limit. These controls limit resources; they are not a dollar-denominated spending cap.

## 2. Seed once

```bash
python aws/run.py --action seed --bucket "$BUCKET" --application "$APP" \
  --role "$ROLE" --namespace "$DATABASE" --run pr_1_000000000001_r1
```

This creates 10,000 synthetic orders, 100 customers and an initial output table. Re-seeding deliberately fails rather than overwriting existing tables. A partially completed seed requires inspecting the sandbox tables before retrying.

## 3. Run the included cancellation-filter example

```bash
python aws/run.py --bucket "$BUCKET" --application "$APP" \
  --role "$ROLE" --namespace "$DATABASE" --run pr_1_000000000002_r1
```

`aws/baseline.sql` includes all orders; `aws/candidate.sql` excludes cancelled orders. Expect 10,000 versus 9,000 orders and 22,375,000 versus 20,250,000 revenue cents. The runner writes `artifacts/aws-report.json` and `.md`. Inspect the JSON snapshot IDs and checks, as well as the business deltas.

```bash
python aws/run.py --action cleanup --bucket "$BUCKET" --application "$APP" \
  --role "$ROLE" --namespace "$DATABASE" --run pr_1_000000000002_r1
```

Always use a new run identifier for a new attempt. Do not reuse a completed run: EMR submission tokens are idempotent and branch creation deliberately refuses collisions.

## 4. Connect the GitHub pipeline

Create the GitHub environment **aws-preview**. Configure required reviewers and allow deployment only from your trusted default branch. Review the PR's exact head commit before approving a run. The workflow executes candidate Python as well as SQL under the sandbox role, so treat approval as permission to execute that code in AWS. Do not approve untrusted or fork code.

Set these environment variables under Settings → Environments → aws-preview:

| Variable | Value |
|---|---|
| AWS_REGION | Your deployed Region |
| AWS_PREVIEW_ROLE_ARN | GitHubRoleArn output |
| PREVIEW_BUCKET | BucketName output |
| EMR_APPLICATION_ID | ApplicationId output |
| EMR_EXECUTION_ROLE_ARN | ExecutionRoleArn output |
| GLUE_DATABASE | DatabaseName output |

Commit the package to the default branch first. Open a same-repository PR that changes `aws/candidate.sql`. From Actions → AWS output preview → Run workflow, use the default branch, enter the open PR number and the full head commit SHA you reviewed, then choose `preview`. The controller fetches `aws/candidate.sql` from the PR's base commit as the baseline, then checks out the immutable head commit. The report appears in the Actions summary and artifact. An unchanged transformation should produce zero delta.

The AWS job is intentionally approval-driven. The separate **Validate AWS preview package** workflow runs on pull requests without AWS credentials. Neither workflow posts comments or publishes content.

## Failures and recovery

The workflow runs cleanup after success or failure. Cleanup checks the saved writer job's state and refuses to run while it is still active. If the whole runner is killed, find the application/job ID in the EMR console or retained job artifact; cancel the writer and wait until SUCCESS, FAILED or CANCELLED before dispatching `cleanup` with the exact old run name. A manual cleanup on a new runner cannot discover an unknown active writer automatically; verify terminal state first.

The job persists a snapshot manifest before writing output and a complete comparison afterward. A failed candidate can leave only the initial manifest. Input tags and output branches request one-day retention, but retention is applied by snapshot maintenance; it is not a background timer. Cleanup removes references only. It never deletes S3 data files. Configure Iceberg-aware snapshot expiration and orphan-file maintenance separately if keeping the sandbox. Never apply a blanket S3 lifecycle expiry to `warehouse/`.

## Scope

- Branches are per Iceberg table. Input snapshot IDs are captured individually, not as a transaction across the database. Use an upstream batch manifest when orders and customers must represent the same ingestion batch.
- This example changes one SQL transformation and one output table. It does not automatically run arbitrary existing Glue scripts, EMR programs or their external side effects.
- Branches share a table schema and IAM permissions. They are not isolation for schema migrations or untrusted code.
- A matching row count does not establish correctness. Extend `checks` with domain expectations, key constraints, null thresholds and full row-level comparisons appropriate to your pipeline.
- No branch is merged into `main`. Production deployment remains a separate process.

## Tests

```bash
python -m pip install -r aws/requirements-test.txt
cfn-lint aws/template.yaml
python -m pytest tests/aws/test_core.py -q
curl --fail --location -o /tmp/iceberg.jar \
  https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.0/iceberg-spark-runtime-3.5_2.12-1.10.0.jar
ICEBERG_JAR=/tmp/iceberg.jar SPARK_LOCAL_IP=127.0.0.1 python tests/aws/integration.py
```

The integration test uses real Spark and Iceberg with local storage/Hadoop catalog. It tests branch writes, pinned reads, failed SQL and cleanup. It does not emulate EMR, Glue IAM or AWS networking. See `docs/validation.md` for recorded execution evidence.

## Tear down

After all jobs stop and reference cleanup completes:

```bash
aws cloudformation delete-stack --stack-name iceberg-pr-preview
aws cloudformation wait stack-delete-complete --stack-name iceberg-pr-preview
```

The bucket is deliberately retained. Inspect and delete the sandbox bucket separately when its reports and data are no longer needed. The stack's IAM OIDC provider is deleted only if this stack created it; do not reuse that provider in other stacks without managing its lifecycle separately.
