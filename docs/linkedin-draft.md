Have you ever changed a Glue or EMR job and wondered what its output would look like before it reached production?

That is usually where orders_test, orders_test_v2 and orders_test_final start appearing.

I built a preview workflow around EMR Serverless, the Glue Data Catalog and Iceberg branches. It reruns the base and candidate transformations against the same pinned input snapshots, then produces a comparison for review before the code is merged.

The detail that matters: comparing a new run with yesterday's output can mix code changes with changes in the data. Both versions need the same input.

The walkthrough covers the AWS infrastructure, Spark code, GitHub pipeline and cleanup. It also shows what branches do not isolate, including table schemas and IAM permissions.

In the included local Spark/Iceberg test, one filter removes 1,000 cancelled orders while leaving the main output snapshot unchanged. The article separates those verified engine results from the AWS deployment steps.

Read the full walkthrough on Build With Abbas.

#AWS #ApacheIceberg #DataEngineering #AmazonEMR
