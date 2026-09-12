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

The public repository is https://github.com/maabbasa/lakehouse-pr-aws-previews. GitHub Actions run 34713817118 passed infrastructure validation, all 9 AWS unit tests and five real Spark/Iceberg integration scenarios. It did not deploy to AWS.

## Repository exposure review

Gitleaks 8.30.1 scanned both published commits through 24b46c74d66ff7d5254ca6a06ce1b2922e9e201b and the working tree on 2026-09-12. No secrets were detected. This scan covers the Git history available from the repository; it does not certify an AWS account or detect every possible secret format.
