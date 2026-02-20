---
name: ec2-cluster-provision
description: uses the code present in this repository for provision an EC2 cluster for benchmarking purposes
---

This project uses a remote benchmarks EC2 cluster constructed with AWS CDK located at `benchmarks/cdk`.

There's a package.json file in `benchmarks/cdk/package.json` with relevant commands about deploying.

Authentication for this skill should be handled in this order of preference:
1. AWS SSO profile commands
2. Command prefix wrapper from `./claude/settings.local.json` key `aws-commands-prefix` (for example `aws-vault exec <profile> --`)
3. Explicit environment credentials (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`)

For method 2, define an `awscmd` shell function in your session that applies the chosen prefix.

Setup references:
- AWS CLI + IAM Identity Center (SSO): https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html
- AWS CLI profile/config files: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html
- AWS CLI environment variables: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html
- `aws-vault` project: https://github.com/99designs/aws-vault

Before running AWS/CDK commands, ensure auth is valid for the chosen method:

```shell
# Method 1: AWS SSO profile commands (preferred)
# How to get these values:
# - aws configure sso
# - aws configure list-profiles
# - aws configure get region --profile <profile>
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_SECURITY_TOKEN AWS_CREDENTIAL_EXPIRATION
export AWS_PROFILE=<profile>
export AWS_REGION=${AWS_REGION:-us-east-1}
export AWS_DEFAULT_REGION="$AWS_REGION"
export AWS_SDK_LOAD_CONFIG=1
aws sso login --profile "$AWS_PROFILE"
aws sts get-caller-identity --profile "$AWS_PROFILE" --region "$AWS_REGION"

# Method 2: Command prefix wrapper (example: aws-vault)
# How to get these values:
# - same <profile> discovery as method 1
# - aws-vault list
# - aws-vault exec <profile> -- aws sts get-caller-identity
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_SECURITY_TOKEN AWS_CREDENTIAL_EXPIRATION
export AWS_REGION=${AWS_REGION:-us-east-1}
export AWS_DEFAULT_REGION="$AWS_REGION"
awscmd() { aws-vault exec <profile> -- "$@"; }
awscmd aws sts get-caller-identity --region "$AWS_REGION"

# Method 3: Explicit environment credentials
# How to get these values:
# - https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html
# - include AWS_SESSION_TOKEN when using temporary credentials
unset AWS_PROFILE
export AWS_ACCESS_KEY_ID=<access-key-id>
export AWS_SECRET_ACCESS_KEY=<secret-access-key>
# export AWS_SESSION_TOKEN=<session-token> # when credentials are temporary
export AWS_REGION=${AWS_REGION:-us-east-1}
export AWS_DEFAULT_REGION="$AWS_REGION"
aws sts get-caller-identity --region "$AWS_REGION"
```

Bootstrap once per account/region:

```shell
# Method 1: AWS SSO profile commands
ACCOUNT_ID=$(aws sts get-caller-identity --profile "$AWS_PROFILE" --query Account --output text)
npm run bootstrap -- aws://$ACCOUNT_ID/$AWS_REGION

# Method 2: Command prefix wrapper (example: aws-vault)
ACCOUNT_ID=$(awscmd aws sts get-caller-identity --region "$AWS_REGION" --query Account --output text)
awscmd npm run bootstrap -- aws://$ACCOUNT_ID/$AWS_REGION

# Method 3: Explicit environment credentials
ACCOUNT_ID=$(aws sts get-caller-identity --region "$AWS_REGION" --query Account --output text)
npm run bootstrap -- aws://$ACCOUNT_ID/$AWS_REGION
```

Running `npm run deploy` will provision the cluster with the resources specified in `benchmarks/cdk/lib/`.
This takes a while typically (~5 mins). If the user data of the EC2 machines was changed, and you want those changes
to take effect you will need to prepend the deployment command with `USER_DATA_CAUSES_REPLACEMENT=true`.
Deployment writes `.cdk-outputs.json` used by benchmark scripts for bucket resolution.

Once the deployment is complete, the list of instance IDs will be printed to stdout.
Pick one instance for validation commands with:

```shell
# Method 1: AWS SSO profile commands
INSTANCE_ID=$(aws cloudformation describe-stacks \
  --stack-name DataFusionDistributedBenchmarks \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='WorkerInstanceIds'].OutputValue" \
  --output text | cut -d',' -f1)

# Method 2: Command prefix wrapper (example: aws-vault)
INSTANCE_ID=$(awscmd aws cloudformation describe-stacks \
  --stack-name DataFusionDistributedBenchmarks \
  --region "$AWS_REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='WorkerInstanceIds'].OutputValue" \
  --output text | cut -d',' -f1)

# Method 3: Explicit environment credentials
INSTANCE_ID=$(aws cloudformation describe-stacks \
  --stack-name DataFusionDistributedBenchmarks \
  --region "$AWS_REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='WorkerInstanceIds'].OutputValue" \
  --output text | cut -d',' -f1)
```

For all `aws` examples below, keep using the selected auth style:
- Method 1: `aws ... --profile "$AWS_PROFILE" --region "$AWS_REGION"`
- Method 2: `awscmd aws ... --region "$AWS_REGION"`
- Method 3: `aws ... --region "$AWS_REGION"`

It's usually necessary to verify that everything was deployed correctly, and it's running fine. For that
it's necessary to perform the following steps for the following engines:

## Distributed DataFusion

1. Port forward the 9000 port in a background terminal:
   `aws ssm start-session --target $INSTANCE_ID --document-name AWS-StartPortForwardingSession --parameters "portNumber=9000,localPortNumber=9000"`
2. Issue a command to /info to see what was deployed:
   `curl http://localhost:9000/info | jq .`
3. If everything's fine, you should see the list of listening workers and the build time

## Trino

1. Port forward the 8080 port in a background terminal:
   `aws ssm start-session --target $INSTANCE_ID --document-name AWS-StartPortForwardingSession --parameters "portNumber=8080,localPortNumber=8080"`
2. Issue a command to /v1/node to see what nodes are available for listening:
   `curl -s -H "X-Trino-User: admin" http://localhost:8080/v1/node | jq .`
   `curl -s -H "X-Trino-User: admin" http://localhost:8080/v1/info | jq .`
3. Make sure that the response of the above is consistent with what is supposed to be deployed by
   `benchmarks/cdk/lib/trino.ts`

## Spark

1. Port forward the 9003 port in a background terminal (this is a custom Python server `benchmarks/cdk/bin/spark_http.py`):
   `aws ssm start-session --target $INSTANCE_ID --document-name AWS-StartPortForwardingSession --parameters "portNumber=9003,localPortNumber=9003"`
2. You can issue curl queries to `http://localhost:9003/health` and `http://localhost:9003/query` to double-check that
   everything is consistent with what's expected from `benchmarks/cdk/lib/spark.ts`

## Ballista

1. Port forward the 9002 port in a background terminal:
   `aws ssm start-session --target $INSTANCE_ID --document-name AWS-StartPortForwardingSession --parameters "portNumber=9002,localPortNumber=9002"`
2. Verify that there are in fact the expected workers connected to the scheduler. For this, all workers need to
   have the appropriate instance IP configured in their launch command, which might not have been done by default,
   because this happens in a post cloud-init command that might not run because of timeout.
3. Verify with systemctl in each remote machine that all workers are running successfully. One thing that happens
   very often is that `--external-host` is incorrectly set to localhost rather than the actual scheduler EC2 private IP

Remember that for running port forward commands in the background, they take like 5 secs until the
"waiting for connections" message appears. Until then, the port is still not forwarded.

If at some point you need to run a command in all machines and get its output, you can do it
with `npm run send-command your custom command`
