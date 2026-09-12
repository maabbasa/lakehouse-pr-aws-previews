# Validation record

Date: 2026-09-12

## Executed successfully

- 21 Python unit tests across the repository, including 9 AWS-controller/name-validation tests.
- `cfn-lint aws/template.yaml`: no errors or warnings.
- `actionlint` on both AWS workflows and the retained local-demo workflow: no errors.
- All seed/preview/cleanup StartJobRun payloads validated against the installed AWS SDK model.
- Python byte compilation and shell syntax check for the deployment helper.
- Real Spark 3.5.6 + Iceberg 1.10.0 integration with Java 17, a Hadoop catalog and local files.
- Baseline versus candidate aggregation: 10,000 / 9,000 orders; 22,375,000 / 20,250,000 revenue cents.
- Main output snapshot unchanged after both branch writes.
- Input arriving after snapshot capture excluded from both transformations.
- Failed candidate SQL followed by successful cleanup.
- Cleanup executed twice without error; only main references remain.
- Both diagram exports visually inspected and corrected for label/edge overlap.

The raw engine result is `docs/results/aws-engine-validation.json`. It explicitly records that cloud deployment was not tested.

## Not executed

No AWS credentials were available. CloudFormation deployment, EMR startup, Glue permissions, S3 execution-role access and GitHub-to-AWS OIDC remain account-level integration checks. Local tests do not establish those results.

The connected GitHub app returned 404 for the private repository. The authoring session did not push this AWS revision or execute its workflows on GitHub. The downloadable release includes everything needed to upload and run it.
