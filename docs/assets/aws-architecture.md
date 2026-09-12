# Preview Spark output before production

1. A trusted GitHub workflow assumes a scoped IAM role using OIDC and uploads a run bundle to S3.
2. It submits an EMR Serverless Spark job. Spark resolves Iceberg tables through the AWS Glue catalog.
3. Spark pins input snapshots, writes base and candidate branches, and reads both outputs.
4. The comparison is stored in S3 and retrieved by the runner for its summary and artifact.

The account sandbox and IAM grants provide access isolation. Iceberg branches provide version separation, not authorization. The catalog database is dedicated to synthetic data. No Nessie server, VPC, NAT gateway or persistent cluster is required.

The .drawio source uses verified official AWS stencil names (emr, glue, s3). The PNG/SVG companion uses official AWS icon images from awslabs/aws-icons-for-plantuml. Geometry is rendered from the editable XML by scripts/render_diagrams.py because the desktop exporter crashes in this environment.
