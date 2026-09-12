#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
STACK_NAME="${STACK_NAME:-iceberg-pr-preview}"
REPOSITORY="${GITHUB_REPOSITORY:-maabbasa/lakehouse-pr-previews}"
parameters=("GitHubRepository=$REPOSITORY")
if [[ -n "${EXISTING_OIDC_PROVIDER_ARN:-}" ]]; then parameters+=("ExistingOidcProviderArn=$EXISTING_OIDC_PROVIDER_ARN"); fi
aws sts get-caller-identity
aws cloudformation deploy --template-file aws/template.yaml --stack-name "$STACK_NAME" \
  --capabilities CAPABILITY_IAM --parameter-overrides "${parameters[@]}"
mkdir -p artifacts
aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].Outputs' > artifacts/stack-outputs.json
python3 - <<'PY'
import json, shlex
outputs={x['OutputKey']:x['OutputValue'] for x in json.load(open('artifacts/stack-outputs.json'))}
keys={'BUCKET':'BucketName','APP':'ApplicationId','ROLE':'ExecutionRoleArn','DATABASE':'DatabaseName','AWS_PREVIEW_ROLE_ARN':'GitHubRoleArn'}
with open('artifacts/aws-env.sh','w') as f:
 for key,out in keys.items(): f.write('export '+key+'='+shlex.quote(outputs[out])+'\n')
PY
printf '%s\n' 'Deployment finished. Run: source artifacts/aws-env.sh'
