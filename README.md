# Preview EMR output before you merge

Run base and candidate Spark SQL against the same Iceberg input snapshots, write isolated output branches, and review the differences in GitHub Actions.

The AWS implementation uses **Amazon EMR Serverless, AWS Glue Data Catalog, Amazon S3 and GitHub OIDC**. CloudFormation creates a dedicated sandbox. Nothing is published or promoted to production by these workflows.

- [Deploy and run the AWS example](aws/README.md)
- [Infrastructure](aws/template.yaml)
- [Spark preview logic](aws/preview_core.py)
- [AWS job controller](aws/run.py)
- [Article manuscript](docs/article.md)
- [Validation evidence](docs/validation.md)

Start with `aws/README.md`. The article's cancellation-filter example compares 10,000 baseline orders with 9,000 candidate orders. The GitHub workflow instead compares the actual PR base and head versions of `aws/candidate.sql`.

The AWS package is a reference implementation with locally executed Spark/Iceberg tests. AWS account deployment has not been executed in this authoring session. The validation record distinguishes those scopes.

